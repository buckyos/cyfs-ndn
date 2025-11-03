use arrow::ipc::UnionMode;
use buckyos_kit::get_relative_path;
use name_lib::{decode_json_from_jwt_with_pk, decode_jwt_claim_without_verify, DID};
use name_client::resolve_auth_key;
use tokio::io::{AsyncRead,AsyncWrite,AsyncWriteExt,AsyncReadExt,AsyncSeekExt};
use tokio::sync::Mutex;
use url::Url;
use log::*;
use reqwest::{Body, Client, StatusCode};
use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio_util::io::StreamReader;
use futures_util::StreamExt;
use std::pin::Pin;
use tokio::io::{BufReader,BufWriter};
use std::collections::HashMap;
use futures::Future;
use futures::future::BoxFuture;
use rand::RngCore;

use crate::*;


pub enum ChunkWorkState {
    Idle,
    Downloading(u64,u64),//complete size / total size
    DownloadError(String),//error message
}

pub struct NdnGetChunkResult {
    pub chunk_id : ChunkId,
    pub chunk_size : u64,
    pub reader : ChunkReader,
}

use crate::{chunk, ChunkReader, ObjId, MAX_CHUNK_SIZE};
use crate::{ChunkId,NdnResult,NamedDataMgr,NdnError,ChunkReadSeek,ChunkHasher};
pub struct NdnClient {
    default_ndn_mgr_id:Option<String>,
    session_token:Option<String>,
    default_remote_url:Option<String>,
    enable_mutil_remote:bool,
    pub enable_remote_pull:bool,
    pub enable_zone_pull:bool,
    chunk_work_state:HashMap<ChunkId,ChunkWorkState>,//
    pub obj_id_in_host:bool,
    pub force_trust_remote:bool,//是否强制信任远程节点
}


pub enum ChunkWriterOpenMode {
    AlwaysNew,
    AutoResume,
    SpecifiedOffset(u64,SeekFrom),
} 

//ndn client的核心类似传统http的reqwest库，但增加了chunk的语义
impl NdnClient {
    pub fn new(default_remote_url:String,session_token:Option<String>,named_mgr_id:Option<String>)->Self {
        Self {
            default_ndn_mgr_id:named_mgr_id,
            session_token,
            default_remote_url:Some(default_remote_url),
            enable_mutil_remote:false,
            enable_remote_pull:false,
            enable_zone_pull:false,
            chunk_work_state:HashMap::new(),
            obj_id_in_host:false,
            force_trust_remote:false,
        }
    }

    pub fn gen_chunk_url(&self,chunk_id:&ChunkId,base_url:Option<String>)->String {
        let real_base_url;
        if base_url.is_some() {
            real_base_url = base_url.unwrap();
        } else {
            real_base_url = self.default_remote_url.as_ref().unwrap().clone();
        }
        let result;
        if self.obj_id_in_host {
            result = format!("{}.{}",chunk_id.to_base32(),real_base_url);
        } else {
            result = format!("{}/{}",real_base_url,chunk_id.to_base32());
        }
        //去掉多余的/
        //let result = result.replace("//", "/");
        result
    }

    pub fn gen_obj_url(&self,obj_id:&ObjId,base_url:Option<String>)->String {
        let real_base_url;
        if base_url.is_some() {
            real_base_url = base_url.unwrap();
        } else {
            real_base_url = self.default_remote_url.as_ref().unwrap().clone();
        }

        format!("{}/{}",real_base_url,obj_id.to_base32())
    }

    fn verify_inner_path_to_obj(resp_headers:&CYFSHttpRespHeaders,inner_path:&str)->NdnResult<()> {
        // //use crate::::{PathObjectMapItemProof, PathObjectMapProofVerifier, PathObjectMapProofVerifyResult};
        // use crate::hash::HashMethod;

        // // First get root_obj_id and obj_id from resp_headers, if any one is None, then we can not verify?
        // if resp_headers.root_obj_id.is_none() {
        //     let msg = format!("no root obj id in resp_headers, inner_path: {}", inner_path);
        //     warn!("{}",msg);
        //     return Err(NdnError::InvalidId(msg));
        // }

        // if resp_headers.obj_id.is_none() {
        //     let msg = format!("no obj id in resp_headers, inner_path: {}", inner_path);
        //     warn!("{}",msg);
        //     return Err(NdnError::InvalidId(msg));
        // }

        // let proof= PathObjectMapItemProof::decode_nodes(
        //     &resp_headers.mtree_path, 
        //     resp_headers.root_obj_id.as_ref().unwrap()
        // )?;
        
        // // FIXME: Should we allow multiple hash methods? or just use the default one?
        // let verifier = PathObjectMapProofVerifier::new(HashMethod::Keccak256);
        // let ret = verifier.verify_object(
        //     inner_path, 
        //     resp_headers.obj_id.as_ref().unwrap(),
        //     None, 
        //     &proof,
        // )?;
        
        // match ret {
        //     PathObjectMapProofVerifyResult::Ok => {
        //         return Ok(());
        //     },
        //     _ => {
        //         let msg = format!("verify inner path failed, url: {}, ret: {:?}", inner_path, ret);
        //         warn!("{}",msg);
        //         return Err(NdnError::VerifyError(msg));
        //     }
        // }
        error!("verify inner path to obj not implemented");
        return Ok(());
    }

    //return (chunk_state,already_uploaded_size)
    pub async fn query_chunk_state(&self,chunk_id:&ChunkId,target_url:Option<String>)->NdnResult<(ChunkState,u64)> {
        let chunk_url;
        if target_url.is_some() {
            chunk_url = target_url.unwrap();
        } else {
            chunk_url = self.gen_chunk_url(&chunk_id, None);
        }

        let mut client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NdnError::Internal(format!("Failed to create client: {}", e)))?;
        
        let head_res = client.head(&chunk_url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("query chunk state request by HEAD ({}) failed: {}", chunk_url, e)))?;
        debug!("SEND HEAD request, head_res:{}",head_res.status());
        let content_length = head_res.content_length();
        // 如果chunk已存在，则不需要再次上传
        match head_res.status() {
            StatusCode::OK => {
                if content_length.is_some() {
                    return Ok((ChunkState::Completed,content_length.unwrap()));
                }
                return Ok((ChunkState::Completed,0));
            },
            StatusCode::NOT_FOUND => {
                return Ok((ChunkState::NotExist,0));
            },
            StatusCode::PARTIAL_CONTENT => {
                if content_length.is_some() {
                    return Ok((ChunkState::Incompleted,content_length.unwrap()));
                }
                return Ok((ChunkState::Incompleted,0));
            },
            StatusCode::CREATED => {
                return Ok((ChunkState::New,0));
            },
            _ => {
                return Err(NdnError::RemoteError(format!("HEAD request failed: {}", head_res.status())));
            }
        }
    }       

    //zone内设备之间互相push chunk
    pub async fn push_chunk(&self,chunk_id:ChunkId,target_url:Option<String>)->NdnResult<()> {
        let chunk_url;
        if target_url.is_some() {
            chunk_url = target_url.unwrap();
        } else {
            chunk_url = self.gen_chunk_url(&chunk_id, None);
        }

        let (chunk_state,already_uploaded_size) = self.query_chunk_state(&chunk_id,Some(chunk_url.clone())).await?;
        match chunk_state {
            ChunkState::Completed => {
                info!("push_chunk:remote chunk already exists, skip");
                return Ok(());
            },
            ChunkState::LocalLink(_link_data) => {
                info!("push_chunk:remote chunk is a local link, skip");
                return Ok(());
            }
            _ => {}
        }

        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await
            .ok_or_else(|| NdnError::Internal("No named data manager available".to_string()))?;
        let real_named_mgr = named_mgr.lock().await;
        let (mut chunk_reader,len) = real_named_mgr.open_chunk_reader_impl(&chunk_id,already_uploaded_size,false).await?;
        debug!("push_chunk:local chunk_reader open success");
        drop(real_named_mgr);

        let mut client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NdnError::Internal(format!("Failed to create client: {}", e)))?;

        let stream: tokio_util::io::ReaderStream<Pin<Box<dyn AsyncRead + Send + Unpin>>> = tokio_util::io::ReaderStream::new(chunk_reader);
        info!("SEND PUT chunk request, chunk_url:{}",chunk_url);
        let res = client.put(chunk_url.clone())
            .header("Content-Type", "application/octet-stream")
            .header("cyfs-chunk-size", len.to_string())
            .header("cyfs-chunk-offset", already_uploaded_size.to_string())
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;

        if !res.status().is_success() {
            return Err(NdnError::RemoteError(
                format!("HTTP error: {} for {}", res.status(), chunk_url)
            ));
        }
        info!("# PUSH CHUNK {} => {} success.",chunk_id.to_string(),chunk_url);
        Ok(())
    }

    pub async fn get_obj_by_id(&self,obj_id:ObjId) -> NdnResult<serde_json::Value> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await
            .ok_or_else(|| NdnError::Internal("No named data manager available".to_string()))?;
        let real_named_mgr = named_mgr.lock().await;
        let obj_data = real_named_mgr.get_object_impl(&obj_id, None).await;
        drop(real_named_mgr);
        if obj_data.is_ok() {
            return obj_data;
        } else {
            info!("get_obj_by_id: get obj by id failed:{}, try to get obj from remote",obj_id.to_string());
            let obj_url = self.gen_obj_url(&obj_id, None);
            let (_,obj_data) = self.get_obj_by_url(&obj_url, Some(obj_id.clone())).await?;
            let obj_data_str = serde_json::to_string(&obj_data).unwrap();
            NamedDataMgr::put_object(self.default_ndn_mgr_id.as_deref(), &obj_id, &obj_data_str).await?;
            return Ok(obj_data);
        }
    }
    

    //helper function
    // 使用标准HTTP协议打开URL获取对象,返回obj_id和obj_str
    // 调用该函数一定会产生网络操作
    pub async fn get_obj_by_url(&self,url:&str,known_obj_id:Option<ObjId>) -> NdnResult<(ObjId,serde_json::Value)> {
        let mut obj_id_from_url: Option<ObjId> = None;
        let mut obj_inner_path: Option<String> = None;     
        // 尝试从URL中提取对象ID
        let url_obj_id_result = cyfs_get_obj_id_from_url(url);
        if let Ok((obj_id, inner_path)) = url_obj_id_result {
            obj_id_from_url = Some(obj_id);
            obj_inner_path = inner_path;
        }
        //info!("get_obj_by_url:obj_id_from_url:{:?},obj_inner_path:{:?}",obj_id_from_url,obj_inner_path);
        // 使用标准HTTP协议打开URL获取对象
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NdnError::Internal(format!("Failed to create client: {}", e)))?;
        
        let res = client.get(url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;
        
        if !res.status().is_success() {
            //cover res.status to NdnError
            let err = NdnError::from_http_status(res.status(),url.to_string());
            return Err(err);
        }
        
        let cyfs_resp_headers = get_cyfs_resp_headers(&res.headers())?;
        debug!("get_obj_by_url : cyfs_resp_headers {:?}",cyfs_resp_headers);
        // 获取响应内容
        let obj_str = res.text()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Failed to read response body: {}", e)))?;
        debug!("get_obj_by_url => RESP : {}",obj_str);
       
        if known_obj_id.is_some() {
            let known_obj_id = known_obj_id.unwrap();
            if obj_inner_path.is_none() {
                if obj_id_from_url.is_some() {
                    if obj_id_from_url.unwrap() != known_obj_id {
                        return Err(NdnError::InvalidId(format!("object id not match, known:{} remote:{}",known_obj_id.to_string(),cyfs_resp_headers.obj_id.unwrap().to_string())));
                    }
                }
            }
            let real_obj = verify_named_object_from_str(&known_obj_id,&obj_str)?;
            return Ok((known_obj_id,real_obj));
        }
        
        if obj_id_from_url.is_some() {
            //URL is a Object Link (CYFS O-Link)
            let obj_id = obj_id_from_url.unwrap();
            if obj_inner_path.is_none() {
                let real_obj = verify_named_object_from_str(&obj_id,&obj_str)?;
                return Ok((obj_id,real_obj));
            } else {
                if cyfs_resp_headers.obj_id.is_none() || cyfs_resp_headers.root_obj_id.is_none() {
                    return Err(NdnError::InvalidId("no obj id or root obj id".to_string()));
                }
                let real_obj_id = cyfs_resp_headers.obj_id.clone().unwrap();
                let root_obj_id = cyfs_resp_headers.root_obj_id.clone().unwrap();
                if root_obj_id != obj_id {
                    return Err(NdnError::InvalidId(format!("root obj id not match, known:{} remote:{}",root_obj_id.to_string(),obj_id.to_string())));
                }
                let real_obj = verify_named_object_from_str(&real_obj_id,&obj_str)?;
                let verify_result = NdnClient::verify_inner_path_to_obj(&cyfs_resp_headers,obj_inner_path.unwrap().as_str())?;
                return Ok((real_obj_id,real_obj));
            }
        } else {
            //URL is a Semantic Object Link (CYFS R-Link)
            if cyfs_resp_headers.obj_id.is_none() {
                return Err(NdnError::InvalidId("no obj id".to_string()));
            }
            
            let obj_id = cyfs_resp_headers.obj_id.clone().unwrap();
            let real_target_obj = verify_named_object_from_str(&obj_id,&obj_str)?;
            //let real_path = 

            if url.starts_with("http://127.0.0.1/") || url.starts_with("https://")  || self.force_trust_remote {
                return Ok((obj_id,real_target_obj));
            }

            if cyfs_resp_headers.path_obj.is_none() {
                return Err(NdnError::InvalidId("no path obj".to_string()));
            }
            let path_obj_jwt = cyfs_resp_headers.path_obj.clone().unwrap();
            let path_obj = decode_jwt_claim_without_verify(&path_obj_jwt)
                .map_err(|e|NdnError::InvalidId(format!("decode path obj failed:{}",e.to_string())))?;
            let path_obj : PathObject = serde_json::from_value(path_obj)
                .map_err(|e|NdnError::InvalidId(format!("decode path obj failed:{}",e.to_string())))?;

            //verify path obj

            let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await
                .ok_or_else(|| NdnError::Internal("No named data manager available".to_string()))?;
            let real_named_mgr = named_mgr.lock().await;
            let cache_path_obj;
            if cyfs_resp_headers.root_obj_id.is_some() {
                if path_obj.target != *cyfs_resp_headers.root_obj_id.as_ref().unwrap() {
                    return Err(NdnError::InvalidId(format!("path obj target obj id not match, known:{} remote:{}",path_obj.target.to_string(),cyfs_resp_headers.root_obj_id.as_ref().unwrap().to_string())));
                }

                cache_path_obj = real_named_mgr.get_cache_path_obj(url);
                obj_inner_path = Some(get_relative_path(&path_obj.path,url));
                NdnClient::verify_inner_path_to_obj(&cyfs_resp_headers,obj_inner_path.unwrap().as_str())?;
            } else {
                if path_obj.target != obj_id {
                    return Err(NdnError::InvalidId(format!("path obj target obj id not match, known:{} remote:{}",path_obj.target.to_string(),obj_id.to_string())));
                }
                cache_path_obj = real_named_mgr.get_cache_path_obj(url);
            }
            
            if cache_path_obj.is_some() {
                let cache_path_obj = cache_path_obj.unwrap();
                if cache_path_obj == path_obj {
                    return Ok((obj_id,real_target_obj));
                }

                if cache_path_obj.uptime > path_obj.uptime {
                    return Err(NdnError::InvalidId("cache path obj is newer than remote path obj".to_string()));
                }
            }
            let did = DID::from_str(url);
            if did.is_err() {
                return Err(NdnError::InvalidId("invalid did".to_string()));
            }
            let did = did.unwrap();
            let pk = resolve_auth_key(&did,None).await
                .map_err(|e|NdnError::InvalidId(format!("resolve auth key failed:{}",e.to_string())))?;
            //veirfy path_obj is signed by pk
            let path_obj_result = decode_json_from_jwt_with_pk(&path_obj_jwt,&pk);
            if path_obj_result.is_err() {
                return Err(NdnError::InvalidId("path obj is not signed by auth key".to_string()));
            }
            real_named_mgr.update_cache_path_obj(url,path_obj);
            return Ok((obj_id,real_target_obj));            
        }

    }

    pub async fn pull_chunk(&self, chunk_id:ChunkId,pull_mode:StoreMode)->NdnResult<u64> {
        let chunk_url = self.gen_chunk_url(&chunk_id,None);
        info!("will pull chunk {} by url:{}",chunk_id.to_string(),chunk_url);
        self.pull_chunk_by_url(chunk_url,chunk_id,pull_mode).await
    }

    //helper function 
    pub async fn open_chunk_reader_by_url(&self,chunk_url:&str,expect_chunk_id:Option<ChunkId>,range:Option<Range<u64>>)         
        ->NdnResult<(ChunkReader,CYFSHttpRespHeaders)> {
        let mut obj_id_from_url: Option<ObjId> = None;
        let mut obj_inner_path: Option<String> = None;     

        let url_obj_id_result = cyfs_get_obj_id_from_url(chunk_url);
        if let Ok((obj_id, inner_path)) = url_obj_id_result {
            obj_id_from_url = Some(obj_id);
            obj_inner_path = inner_path;
        }

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NdnError::Internal(format!("Failed to create client: {}", e)))?;
        
        let res = client.get(chunk_url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;
        
        if !res.status().is_success() {
            return Err(NdnError::from_http_status(res.status(),chunk_url.to_string()));
        }
        
        let cyfs_resp_headers = get_cyfs_resp_headers(&res.headers())?;
        let content_length = res.content_length();
        if content_length.is_none() {
            return Err(NdnError::RemoteError(format!("content length not found for {}", chunk_url)));
        }
        let content_length = content_length.unwrap();

        let stream = res.bytes_stream().map(|r| {
            r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Stream error: {}", e)))
        });
        let reader = StreamReader::new(stream);
        let reader = Box::pin(reader);

        //start verify
        if expect_chunk_id.is_some() {
            let known_obj_id = expect_chunk_id.unwrap();
            if obj_inner_path.is_none() {
                if obj_id_from_url.is_some() {
                    let chunk_id_from_url = ChunkId::from_obj_id(&obj_id_from_url.unwrap());
                    if chunk_id_from_url != known_obj_id {
                        return Err(NdnError::InvalidId(format!("chunk id not match, known:{} remote:{}",
                            chunk_id_from_url.to_string(),known_obj_id.to_string())));
                    }
                }
            }
            return Ok((reader,cyfs_resp_headers));
        }
        
        if obj_id_from_url.is_some() {
            //URL is a Object Link (CYFS O-Link)
            let obj_id = obj_id_from_url.unwrap();
            if obj_inner_path.is_none() {
                return Ok((reader,cyfs_resp_headers));
            } else {
                if cyfs_resp_headers.obj_id.is_none() || cyfs_resp_headers.root_obj_id.is_none() {
                    return Err(NdnError::InvalidId("no obj id or root obj id".to_string()));
                }
                let real_obj_id = cyfs_resp_headers.obj_id.clone().unwrap();
                let root_obj_id = cyfs_resp_headers.root_obj_id.clone().unwrap();
                if root_obj_id != obj_id {
                    return Err(NdnError::InvalidId(format!("root obj id not match, known:{} remote:{}",root_obj_id.to_string(),obj_id.to_string())));
                }
                let _verify_result = NdnClient::verify_inner_path_to_obj(&cyfs_resp_headers,obj_inner_path.unwrap().as_str())?;
                return Ok((reader,cyfs_resp_headers));
            }
        } else {
            //URL is a Semantic Object Link (CYFS R-Link)
            if cyfs_resp_headers.obj_id.is_none() {
                return Err(NdnError::InvalidId("no obj id".to_string()));
            }

            if chunk_url.starts_with("https://") || self.force_trust_remote {
                return Ok((reader,cyfs_resp_headers));
            }

            if cyfs_resp_headers.path_obj.is_none() {
                return Err(NdnError::InvalidId("no path obj".to_string()));
            }
            let path_obj_jwt = cyfs_resp_headers.path_obj.clone().unwrap();
            let path_obj = decode_jwt_claim_without_verify(&path_obj_jwt)
                .map_err(|e|NdnError::InvalidId(format!("decode path obj failed:{}",e.to_string())))?;
            let path_obj : PathObject = serde_json::from_value(path_obj)
                .map_err(|e|NdnError::InvalidId(format!("decode path obj failed:{}",e.to_string())))?;
            let obj_id = cyfs_resp_headers.obj_id.clone().unwrap();

            let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await
                .ok_or_else(|| NdnError::Internal("No named data manager available".to_string()))?;
            let real_named_mgr = named_mgr.lock().await;
            let cache_path_obj;
            if cyfs_resp_headers.root_obj_id.is_some() {
                cache_path_obj = real_named_mgr.get_cache_path_obj(chunk_url);
                obj_inner_path = Some(get_relative_path(&path_obj.path,chunk_url));
                NdnClient::verify_inner_path_to_obj(&cyfs_resp_headers,obj_inner_path.unwrap().as_str())?;
            } else {
                cache_path_obj = real_named_mgr.get_cache_path_obj(chunk_url);
            }
            
            if cache_path_obj.is_some() {
                let cache_path_obj = cache_path_obj.unwrap();
                if cache_path_obj == path_obj {
                    return Ok((reader,cyfs_resp_headers));
                }

                if cache_path_obj.uptime > path_obj.uptime {
                    return Err(NdnError::InvalidId("cache path obj is newer than remote path obj".to_string()));
                }
            }
            let did = DID::from_str(chunk_url);
            if did.is_err() {
                return Err(NdnError::InvalidId("invalid did".to_string()));
            }
            let did = did.unwrap();
            let pk = resolve_auth_key(&did,None).await
                .map_err(|e|NdnError::InvalidId(format!("resolve auth key failed:{}",e.to_string())))?;
            //veirfy path_obj is signed by pk
            let path_obj_result = decode_json_from_jwt_with_pk(&path_obj_jwt,&pk);
            if path_obj_result.is_err() {
                return Err(NdnError::InvalidId("path obj is not signed by auth key".to_string()));
            }
            real_named_mgr.update_cache_path_obj(chunk_url,path_obj);
            return Ok((reader,cyfs_resp_headers));            
        }        
    }

    // download chunk被pull_chunk替代
    //返回成功下载的chunk_id和chunk_size,下载成功后named mgr种chunk存在于cache中
    // pub async fn download_chunk(&self,chunk_url:&str,chunk_id:ChunkId,
    //     local_path:&PathBuf,offset:u64,no_verify:Option<bool>) -> NdnResult<(ChunkId,u64)> {
    //     // 首先从URL下载chunk到本地缓存
    //     let chunk_size = self.pull_chunk_by_url(chunk_url.to_string(), chunk_id.clone(), PullMode::default()).await?;
 
    //     // 确保目标目录存在
    //     if let Some(parent) = local_path.parent() {
    //         std::fs::create_dir_all(parent)
    //             .map_err(|e| NdnError::IoError(format!("Failed to create directory: {}", e)))?;
    //     }
        
    //     // 打开本地文件用于写入
    //     let mut file = tokio::fs::File::create(local_path)
    //         .await
    //         .map_err(|e| NdnError::IoError(format!("Failed to create file: {}", e)))?;

    //     file.seek(SeekFrom::Start(offset)).await
    //         .map_err(|e| NdnError::IoError(format!("Failed to seek file: {}", e)))?;
        
    //     // 从named-data-mgr中获取chunk reader，因为pull_chunk已经将chunk写入named-data-mgr
    //     let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await
    //         .ok_or_else(|| NdnError::Internal("No named data manager available".to_string()))?;
    //     let real_named_mgr = named_mgr.lock().await;
    //     let (mut chunk_reader, chunk_size) = real_named_mgr.open_chunk_reader_impl(&chunk_id, 0, true).await
    //         .map_err(|e| NdnError::Internal(format!("Failed to get chunk reader: {}", e)))?;    
    //     // 复制数据到本地文件（TODO：要验证 chunkid)
    //     drop(real_named_mgr);
    //     let hasher = ChunkHasher::new_with_hash_method(chunk_id.chunk_type.to_hash_method()?)?;
    //     copy_chunk(chunk_id.clone(), chunk_reader, &mut file, Some(hasher), None)
    //         .await
    //         .map_err(|e| NdnError::IoError(format!("Failed to copy data to file: {}", e)))?;
        
    //     // 返回chunk_id和大小
    //     Ok((chunk_id, chunk_size))
    // }


    pub async fn pull_file(&self,file_obj:FileObject,pull_mode:StoreMode,mut progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>) -> NdnResult<()>{
        let content_obj_id = ObjId::new(file_obj.content.as_str())?;
        if content_obj_id.is_chunk() {
            let chunk_id = ChunkId::new(file_obj.content.as_str())?;
            self.pull_chunk(chunk_id, StoreMode::default()).await?;
            //TODO: add link to local file?
            return Ok(());
        } else {
            if content_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
                self.pull_chunklist(&content_obj_id, pull_mode,progress_callback).await?;
                return Ok(());
            } else {
                return Err(NdnError::Internal(format!("Unsupported content obj type: {}", content_obj_id.obj_type)));
            }
        }
    }

    //准备dir_object,并列出所有需要下载的chunk
    // pub async fn prepare_dir_object(&self,dir_obj:&ObjId,last_dir_obj:Option<ObjId>) -> NdnResult<()>{
    //     unimplemented!()
    // }
    
    pub async fn pull_dir(&self,ndn_mgr_id:Option<&str>,dir_obj:DirObject,
        pull_mode:StoreMode, progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>) -> NdnResult<()>{
        //TODO: ndn_mgr需要有一个可信的，容器对象都存在的标签，来快速检查
        for (sub_name,sub_item) in dir_obj.iter() {
            let sub_item_type = sub_item.get_obj_type();
            match sub_item_type.as_str() {
                OBJ_TYPE_DIR => {
                    let (sub_item_obj_id,_) = sub_item.get_obj_id()?;
                    let sub_item_obj = self.get_obj_by_id(sub_item_obj_id).await?;
                    let sub_dir: DirObject = serde_json::from_value(sub_item_obj)
                        .map_err(|e| NdnError::Internal(format!("Failed to parse DirObject: {}", e)))?;
                    let sub_pull_mode = pull_mode.gen_sub_store_mode(sub_name);
                    
                    Box::pin(self.pull_dir(ndn_mgr_id,sub_dir,pull_mode.clone(),progress_callback.clone())).await?;
                }
                OBJ_TYPE_FILE => {
                    // placeholder for file handling
                    let sub_item_obj = sub_item.get_obj()?;
                    let (sub_item_obj_id, sub_item_obj_str) = sub_item.get_obj_id()?;
                
                    let sub_file: FileObject = serde_json::from_value(sub_item_obj)
                        .map_err(|e| NdnError::Internal(format!("Failed to parse FileObject: {}", e)))?;
                    let sub_file_size = sub_file.size;
                    NamedDataMgr::put_object(ndn_mgr_id, &sub_item_obj_id, &sub_item_obj_str).await?;
                    
                    let sub_pull_mode = pull_mode.gen_sub_store_mode(sub_name);
                    self.pull_file(sub_file,pull_mode.clone(),progress_callback.clone()).await?;
                    if let Some(progress_callback) = progress_callback.clone() {
                        let mut progress_callback = progress_callback.lock().await;
                        let callback_result = progress_callback(sub_name.clone(), NdnAction::FileOK(sub_item_obj_id.clone(),sub_file_size)).await?;
                        if !callback_result.is_continue() {
                            info!("pull_dir: stopped by progress callback");
                            return Ok(());
                        }
                    }
                    
                }
                _ => {
                    warn!("pull_dir: unknown sub item type:{}",sub_item_type);
                    continue;
                }
            }

        }
        Ok(())
    }

    //使用这种模式是发布方承诺用 R-Link发布FileObject,用O-Link发布chunk的模式
    //返回下载成功的FileObj和obj_id，下载成功后named mgr中chunk存在于cache中
    pub async fn download_fileobj(&self,fileobj_url:&str,local_path:&PathBuf,no_verify:Option<bool>) -> NdnResult<(ObjId,FileObject)> {
        let (obj_id, file_obj_json) = self.get_obj_by_url(fileobj_url, None).await?;
        
        // 解析FileObject
        let file_obj: FileObject = serde_json::from_value(file_obj_json.clone())
            .map_err(|e| NdnError::Internal(format!("Failed to parse FileObject: {}", e)))?;
        let file_obj_size = file_obj.size;

        self.pull_file(file_obj.clone(), StoreMode::LocalFile(local_path.clone(), 0..file_obj_size, false), None).await?;

        let local_file_obj_path = local_path.with_extension("fileobj");
            //dump file_obj to local_file_obj_path
        tokio::fs::write(local_file_obj_path, serde_json::to_string(&file_obj_json).unwrap()).await
            .map_err(|e| NdnError::IoError(format!("Failed to write fileobj to local file: {}", e)))?;

        Ok((obj_id, file_obj))
    }
    pub async fn remote_is_better(&self,url:&str,local_path:&PathBuf) -> NdnResult<bool> {
        // 1. 通过url下载fileojbect对象
        // 2. 计算本地文件的hash 
        // 3. 比较fileobj的hcontent和本地文件的hash

        if !local_path.exists() {
            warn!("remote_is_better: local file does not exist: {:?}", local_path);
            return Ok(true);
        }

        let mut file = tokio::fs::File::open(local_path).await
            .map_err(|e| NdnError::IoError(format!("Failed to open local file: {}", e)))?;
        let file_size = file.metadata().await
            .map_err(|e| NdnError::IoError(format!("Failed to get file metadata: {}", e)))?
            .len();

        debug!("start download remote fileobj!");
        let get_result = self.get_obj_by_url(url, None).await;
        if get_result.is_err() {
            debug!("get fileobj from remote failed:{}",get_result.err().unwrap().to_string());
            return Ok(false);
        }

        let (obj_id, file_obj_json) = get_result.unwrap();
        let file_obj: FileObject = serde_json::from_value(file_obj_json)
            .map_err(|e| NdnError::Internal(format!("Failed to parse FileObject: {}", e)))?;
        let content_chunk_id = ChunkId::new(file_obj.content.as_str())
            .map_err(|e| NdnError::Internal(format!("Failed to parse content chunk id: {}", e)))?;

        let local_fileobj_file = local_path.with_extension("fileobj");
        if local_fileobj_file.exists() {
            info!("remote_is_better: local fileobj file exists: {:?}", local_fileobj_file);
            let local_fileobj = tokio::fs::read_to_string(local_path.with_extension("fileobj")).await
                .map_err(|e| NdnError::IoError(format!("Failed to read local fileobj file: {}", e)))?;
            let local_fileobj: FileObject = serde_json::from_str(&local_fileobj)
                .map_err(|e| NdnError::Internal(format!("Failed to parse local fileobj file: {}", e)))?;
            if local_fileobj.create_time >= file_obj.create_time {
                return Ok(false);
            }
        }

        if file_size != file_obj.size {
            info!("remote_is_better: file size not match, remote:{} local:{}",file_obj.size,file_size);
            return Ok(true);
        }

        debug!("start calculate hash!");

        let mut hasher = ChunkHasher::new(None)
            .map_err(|e| NdnError::Internal(format!("Failed to create chunk hasher: {}", e)))?;
        let chunk_type = hasher.hash_method.clone();
        let (hash_result,_) = hasher.calc_from_reader(&mut file).await
            .map_err(|e| NdnError::Internal(format!("Failed to calculate hash: {}", e)))?;
        let file_chunk_id = ChunkId::from_mix_hash_result_by_hash_method(file_size, &hash_result, chunk_type)?;
 
        Ok(file_chunk_id != content_chunk_id)
    }

    pub async fn pull_chunklist(&self, chunk_list_id:&ObjId,pull_mode:StoreMode,mut progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>)->NdnResult<u64>{
        //在chunklist下载中，允许使用多源
        let chunk_list_obj_data = self.get_obj_by_id(chunk_list_id.clone()).await?;
        let chunk_list:Vec<ChunkId> = serde_json::from_value(chunk_list_obj_data)
            .map_err(|e| NdnError::Internal(format!("Failed to parse SimpleChunkList: {}", e)))?;
        let mut offset = 0;
        for chunk_id in chunk_list {
            let chunk_size = chunk_id.get_length();
            if chunk_size.is_none() {
                return Err(NdnError::Internal(format!("Failed to get chunk size: {}", chunk_id.to_string())));
            }
            let chunk_size = chunk_size.unwrap();
            let chunk_pull_mode:StoreMode;
            let the_pull_mode = pull_mode.clone();
            match the_pull_mode {
                StoreMode::LocalFile(local_path,range,need_pull_to_named_mgr) => {
                    chunk_pull_mode = StoreMode::LocalFile(local_path.clone(), 
                        range.start + offset..range.start + offset + chunk_size, need_pull_to_named_mgr);
                }
                _ => {
                    chunk_pull_mode = the_pull_mode;
                }
            }
            let copy_size = self.pull_chunk( chunk_id, chunk_pull_mode).await?;
            offset += chunk_size;
        }

        info!("pull_chunklist {} OK, total size:{}",chunk_list_id.to_string(),offset);
        return Ok(offset);
    }

    //TODO: support muilt remote source,when open remote reader failed, try next source
    //return download size
    pub async fn pull_chunk_by_url(&self, chunk_url:String,chunk_id:ChunkId,pull_mode:StoreMode)->NdnResult<u64> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref()).await;
        if named_mgr.is_none() {
            return Err(NdnError::Internal("no named data mgr".to_string()));
        }
        let named_mgr = named_mgr.unwrap();
        let mut real_named_mgr = named_mgr.lock().await;

        let mut chunk_size:u64 = 0;
        // query chunk state from named_mgr (if chunk is completed, return already exists)
        let (chunk_state,_chunk_size,progress) = real_named_mgr.query_chunk_state_impl(&chunk_id).await?;
        drop(real_named_mgr);
        
        //open remote reader
        let mut real_hash_state = None;
        let mut download_pos = 0;
        let mut remote_reader = None;
        match chunk_state {
            ChunkState::Completed  => {
                info!("pull_chunk: chunk {} already exists at named_mgr",chunk_id.to_string());
            },
            ChunkState::Disabled => {
                warn!("pull_chunk: chunk {} is disabled at named_mgr",chunk_id.to_string());
                return Err(NdnError::InvalidState(format!("{} is disable",chunk_id.to_string())));
            },
            ChunkState::LocalLink(link_data) => {
                warn!("pull_chunk: chunk {} is a local link at named_mgr",chunk_id.to_string());
            },
            ChunkState::NotExist | ChunkState::New => {
                //no progess info
                let open_result = self.open_chunk_reader_by_url(chunk_url.as_str(),Some(chunk_id.clone()),None).await;
                if open_result.is_err() {
                    warn!("pull_chunk: open chunk reader failed:{}",open_result.err().unwrap().to_string());
                    return Err(NdnError::NotFound(chunk_id.to_string()));
                }
                let (mut _reader,resp_headers) = open_result.unwrap();
                chunk_size = resp_headers.obj_size.unwrap();
                remote_reader = Some(_reader);
            },
            ChunkState::Incompleted => {
                chunk_size = _chunk_size;
                // use progress info to open reader send request with range to remote
                if progress.len() > 2 {
                    let json_value = serde_json::from_str::<serde_json::Value>(&progress);
                    if json_value.is_err() {
                        warn!("pull_chunk: invalid progress info:{}",progress);
                    } else {
                        let json_value = json_value.unwrap();
                        let hash_state = ChunkHasher::restore_from_state(json_value);
                        if hash_state.is_err() {
                            warn!("pull_chunk: invalid progress info:{}",progress);
                        } else {
                            
                            let hash_state = hash_state.unwrap();
                            download_pos = hash_state.get_pos();
                            real_hash_state  = Some(hash_state);
                            info!("pull_chunk load progress sucess!,pos:{}",download_pos);
                        }
                    }
                }
                let mut download_range = None;
                if real_hash_state.is_some() {
                    download_range = Some(download_pos.._chunk_size);
                }
                let open_result = self.open_chunk_reader_by_url(chunk_url.as_str(),Some(chunk_id.clone()),download_range).await;
                if open_result.is_err() {
                    warn!("pull_chunk: open chunk reader failed:{},url:{}",open_result.err().unwrap().to_string(),chunk_url);
                    return Err(NdnError::NotFound(chunk_id.to_string()));
                }
                let (mut _reader,resp_headers) = open_result.unwrap();
                remote_reader = Some(_reader);
                info!("pull_chunk: open chunk reader success,chunk_id:{},chunk_size:{},download_pos:{}",
                    chunk_id.to_string(),chunk_size,download_pos);
            },
        }
        
        //copy chunk from remote to named_mgr and local file
        if remote_reader.is_none() {
            if !pull_mode.is_store_to_local() {
                info!("pull_chunk OK. chunk already exists at named_mgr.");
                return Ok(0);
            } else {
                //local to local
                let mut real_named_mgr = named_mgr.lock().await;
                let (mut local_reader,_) = real_named_mgr.open_chunk_reader_impl(&chunk_id,0,true).await?;
                drop(real_named_mgr);
                let mut local_writer = pull_mode.open_local_writer().await?;
                debug!("pull_chunk: copy chunk from ndn_mgr => local_file ...");
                tokio::io::copy(&mut remote_reader.unwrap(), &mut local_writer).await?;
                info!("pull_chunk OK, copy chunk from ndn_mgr => local_file success");
                return Ok(chunk_size);
            }
        } else {
            let mut remote_reader = remote_reader.unwrap();
            if pull_mode.need_store_to_named_mgr() {
                // open chunk writer with progress info
                let real_named_mgr = named_mgr.lock().await;
                let (mut chunk_writer,progress_info) = real_named_mgr.open_chunk_writer_impl(&chunk_id,chunk_size,download_pos).await?;
                drop(real_named_mgr);
                let named_mgr2 = named_mgr.clone();
                let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
                let progress_callback = {
                    Some(Box::new(move |chunk_id: ChunkId, pos: u64, hasher: &Option<ChunkHasher>| {
                        let this_chunk_id = chunk_id.clone();
                        let mut json_progress_str = String::new();
                        if let Some(hasher) = hasher {
                            let state = hasher.save_state();
                            if state.is_err() {
                                warn!("pull_chunk: save state failed:{}",state.err().unwrap().to_string());                  
                            } else {
                                let state = state.unwrap();
                                json_progress_str = serde_json::to_string(&state).unwrap(); 
                            }
                        }
                        let counter = counter.clone();
                        let named_mgr2 = named_mgr2.clone();
                        
                        Box::pin(async move {
                            let count = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if count % 16 == 0 {
                                if !json_progress_str.is_empty() {
                                    let mut real_named_mgr = named_mgr2.lock().await;
                                    real_named_mgr.update_chunk_progress_impl(&this_chunk_id,json_progress_str).await?;
                                }
                            }
                            Ok(())
                        }) as Pin<Box<dyn Future<Output = NdnResult<()>> + Send>>
                    }) as Box<dyn FnMut(ChunkId, u64, &Option<ChunkHasher>) -> Pin<Box<dyn Future<Output = NdnResult<()>> + Send + 'static>> + Send>)
                };

                let copy_result = copy_chunk(chunk_id.clone(), &mut remote_reader, &mut chunk_writer, real_hash_state, progress_callback).await?;
                named_mgr.lock().await.complete_chunk_writer_impl(&chunk_id).await?;
                info!("pull_chunk OK, copy chunk from remote => named_mgr success");
                if pull_mode.is_store_to_local() {
                    let mut real_named_mgr = named_mgr.lock().await;
                    let (mut local_reader,_) = real_named_mgr.open_chunk_reader_impl(&chunk_id,0,true).await?;
                    drop(real_named_mgr);
                    let mut local_writer = pull_mode.open_local_writer().await?;
                    debug!("pull_chunk: copy chunk from ndn_mgr => local_file ...");
                    
                    tokio::io::copy(&mut remote_reader, &mut local_writer).await?;
                    info!("pull_chunk OK, copy chunk from ndn_mgr => local_file success");
                    return Ok(chunk_size);
                } else {
                    return Ok(copy_result);
                }
            } else {
                let mut local_writer = pull_mode.open_local_writer().await?;
                debug!("pull_chunk: copy chunk from remote => local_file ...");
                copy_chunk(chunk_id.clone(), &mut remote_reader, &mut local_writer, None, None).await?;
                info!("pull_chunk OK, copy chunk from remote => local_file success");
                return Ok(chunk_size);
            }
        }
    }

    
    
    //async fn open_chunk_writer_by_url(&self,chunk_url:String,open_mode:ChunkWriterOpenMode)->NdnResult<(ChunkWriter,Option<ChunkHasher>)> {
    //    unimplemented!()
    //}

}


// #[cfg(test)] 
// mod tests {
//     use super::*;
//     use buckyos_kit::*;
//     use crate::*;
//     use serde_json::json;
//     use cyfs_gateway_lib::*;
//     use cyfs_warp::*;
//     use rand::{rng, RngCore};

//     fn generate_random_bytes(size: u64) -> Vec<u8> {
//         let mut rng = rand::rng();
//         let mut buffer = vec![0u8; size as usize];
//         rng.fill_bytes(&mut buffer);
//         buffer
//     }

//     #[tokio::test]
//     async fn test_pull_chunk() {
//         init_logging("ndn_client_test",false);
//         let test_server_config = json!({
//             "tls_port":3243,
//             "http_port":3280,
//             "hosts": {
//               "*": {
//                 "enable_cors":true,
//                 "routes": {
//                   "/ndn/": {
//                     "named_mgr": {
//                         "named_data_mgr_id":"test",
//                         "read_only":true,
//                         "guest_access":true,
//                         "is_chunk_id_in_path":true,
//                         "enable_mgr_file_path":true
//                     }
//                   }
//                 } 
//               }
//             }
//           });  

//         let test_server_config:WarpServerConfig = serde_json::from_value(test_server_config).unwrap();

//         tokio::spawn(async move {
//             info!("start test ndn server(powered by cyfs-warp)...");
//             start_cyfs_warp_server(test_server_config).await;
//         });
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;

//         // Step 1: Initialize a new NamedDataMgr in a temporary directory and create a test object
//         let temp_dir = tempfile::tempdir().unwrap();
//         let config = NamedDataMgrConfig {
//             local_stores: vec![temp_dir.path().to_str().unwrap().to_string()],
//             local_cache: None,
//             mmap_cache_dir: None,
//         };
        
//         let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(Some("test")).await.unwrap();
//         let mut named_mgr = named_mgr.lock().await;
//         let chunk_a_size:u64 = 1024*1024 + 321;
//         let chunk_a = generate_random_bytes(chunk_a_size);
//         let mut hasher = ChunkHasher::new(None).unwrap();
//         let chunk_id_a = hasher.calc_mix_chunk_id_from_bytes(&chunk_a).unwrap();
//         info!("chunk_id_a:{}",chunk_id_a.to_string());
//         let (mut chunk_writer,progress_info) = named_mgr.open_chunk_writer_impl(&chunk_id_a, chunk_a_size, 0).await.unwrap();
//         chunk_writer.write_all(&chunk_a).await.unwrap();
//         named_mgr.complete_chunk_writer_impl(&chunk_id_a).await.unwrap();


//         let chunk_b_size:u64 = 1024*1024*3 + 321*71;
//         let chunk_b = generate_random_bytes(chunk_b_size);
//         let mut hasher = ChunkHasher::new(None).unwrap();
//         let chunk_id_b = hasher.calc_mix_chunk_id_from_bytes(&chunk_b).unwrap();
//         info!("chunk_id_b:{}",chunk_id_b.to_string());
//         let (mut chunk_writer,progress_info) = named_mgr.open_chunk_writer_impl(&chunk_id_b, chunk_b_size, 0).await.unwrap();
//         chunk_writer.write_all(&chunk_b).await.unwrap();
//         named_mgr.complete_chunk_writer_impl(&chunk_id_b).await.unwrap();

//         let chunk_c_size:u64 = 1024*1024*3 + 321*71;
//         let chunk_c = generate_random_bytes(chunk_c_size);
//         let mut hasher = ChunkHasher::new(None).unwrap();
//         let chunk_id_c = hasher.calc_mix_chunk_id_from_bytes(&chunk_c).unwrap();
//         info!("chunk_id_c:{}",chunk_id_c.to_string());
//         let (mut chunk_writer,progress_info) = named_mgr.open_chunk_writer_impl(&chunk_id_c, chunk_c_size, 0).await.unwrap();
//         chunk_writer.write_all(&chunk_c).await.unwrap();
//         drop(chunk_writer);
//         named_mgr.complete_chunk_writer_impl(&chunk_id_c).await.unwrap();
//         drop(named_mgr);
        
//         //info!("named_mgr [test] init OK!");
//         //NamedDataMgr::set_mgr_by_id(Some("test"),named_mgr).await.unwrap();

//         let temp_dir = tempfile::tempdir().unwrap();
//         let config = NamedDataMgrConfig {
//             local_stores: vec![temp_dir.path().to_str().unwrap().to_string()],
//             local_cache: None,
//             mmap_cache_dir: None,
//         };    
//         let named_mgr2 = NamedDataMgr::from_config(
//             Some("test_client".to_string()),
//             temp_dir.path().to_path_buf(),
//             config
//         ).await.unwrap();
//         info!("named_mgr [test_client] init OK!");
//         NamedDataMgr::set_mgr_by_id(Some("test_client"),named_mgr2).await.unwrap();
//         // Step 2: Start a cyfs-warp server based on the named_mgr and configure the ndn-router
//         let named_mgr_test = NamedDataMgr::get_named_data_mgr_by_id(Some("test_client")).await.unwrap();
//         info!("test get test_client named mgr  OK!");
//         drop(named_mgr_test);
    
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;

//         // // Step 3: Configure the ndn-client and set the cyfs-warp address (obj_id in path)
//         let client = NdnClient::new("http://localhost:3280/ndn/".to_string(),None,Some("test_client".to_string()));
//         client.pull_chunk(chunk_id_a.clone(),Some("test_client")).await.unwrap();

//         let named_mgr_client = NamedDataMgr::get_named_data_mgr_by_id(Some("test_client")).await.unwrap();
//         let real_named_mgr_client = named_mgr_client.lock().await;
//         let (mut reader,len) = real_named_mgr_client.open_chunk_reader_impl(&chunk_id_a,SeekFrom::Start(0),false).await.unwrap();
//         assert_eq!(len,chunk_a_size);
//         drop(real_named_mgr_client);
//         let mut buffer = vec![0u8;chunk_a_size as usize];
//         reader.read_exact(&mut buffer).await.unwrap();
//         assert_eq!(buffer,chunk_a);


//         //client.set_remote_url(format!("http://{}/{}", warp_addr, test_obj_id.to_base32()));

//         // // Step 4.1: Use the ndn-client's pull_chunk interface to retrieve the chunk
//         // let chunk_id = ChunkId::from_obj_id(&test_obj_id);
//         // let pull_result = client.pull_chunk(chunk_id.clone()).await;
//         // assert!(pull_result.is_ok(), "Failed to pull chunk");

//         // // Step 4.2: Use the ndn-client's get_obj_by_url interface to get the chunk/object
//         // let obj_result = client.get_obj_by_url(&format!("http://{}/{}", warp_addr, test_obj_id.to_base32())).await;
//         // assert!(obj_result.is_ok(), "Failed to get object by URL");

//         // // Step 4.3: Use the ndn-client's get_obj_by_url with a URL containing obj_json_path to get the corresponding value
//         // let json_path = "some_json_path";
//         // let json_result = client.get_obj_by_url(&format!("http://{}/{}/{}", warp_addr, test_obj_id.to_base32(), json_path)).await;
//         // assert!(json_result.is_ok(), "Failed to get JSON value by URL");

//         // http://remote_zone_id/ndn/repo/meta_index_db
//         // // Step 4.4: Use the ndn-client's get_obj_by_url to get a typical file_obj.content
//         // let file_content_result = client.get_obj_by_url(&format!("http://{}/file_obj.content", warp_addr)).await;
//         // assert!(file_content_result.is_ok(), "Failed to get file object content");

//         // // Clean up
//         // warp_server.stop().await.unwrap();
//         // temp_dir.close().unwrap();
//     }
// }



