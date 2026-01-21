use super::def::{ChunkItem, ObjectState, ChunkState};
use super::named_data_mgr_db::NamedDataMgrDB;
use crate::{
    ChunkHasher, ChunkId, ChunkListReader, ChunkLocalInfo, ChunkReadSeek, ChunkType, FileObject, LimitReader, NdnError, NdnResult, OBJ_TYPE_CHUNK_LIST_SIMPLE, ObjectLinkData, PathObject, SimpleChunkList, SimpleChunkListReader, StoreMode, build_named_object_by_json, caculate_qcid_from_file
};
use crate::{ChunkList, ChunkReader, ChunkWriter, ObjId, CHUNK_NORMAL_SIZE};
use buckyos_kit::get_buckyos_named_data_dir;
use buckyos_kit::{
    buckyos_get_unix_timestamp, get_buckyos_root_dir, get_by_json_path, get_relative_path,
};
use futures_util::stream;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::*;
use memmap::Mmap;
use name_lib::{decode_jwt_claim_without_verify, EncodedDocument};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io as std_io;
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::UNIX_EPOCH;
use std::{path::PathBuf, path::Path,pin::Pin};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};
use tokio_util::bytes::BytesMut;
use tokio_util::io::StreamReader;
use fs2::FileExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedDataMgrConfig {
    pub local_cache: Option<String>,
    pub mmap_cache_dir: Option<String>,
}

impl Default for NamedDataMgrConfig {
    fn default() -> Self {
        Self {
            local_cache: None,
            mmap_cache_dir: None,
        }
    }
}

pub struct NamedDataMgr {
    // Store configuration fields (merged from NamedDataStore)
    base_dir: PathBuf,
    read_only: bool,
    
    // Cache configuration
    local_cache_base_dir: Option<String>,   //Cache at local disk
    mmap_cache_dir: Option<String>,        //Cache at memory
    mgr_id: Option<String>,
    db: NamedDataMgrDB,
}

impl NamedDataMgr {
    pub(crate) fn db(&self) -> &NamedDataMgrDB {
        &self.db
    }

    pub fn get_mgr_id(&self) -> Option<String> {
        self.mgr_id.clone()
    }


    pub async fn set_mgr_by_id(
        named_data_mgr_id: Option<&str>,
        mgr: NamedDataMgr,
    ) -> NdnResult<()> {
        let named_data_mgr_key = named_data_mgr_id.unwrap_or("default").to_string();
        let mut named_data_mgr_map = NAMED_DATA_MGR_MAP.lock().await;
        named_data_mgr_map.insert(named_data_mgr_key, Arc::new(tokio::sync::Mutex::new(mgr)));
        Ok(())
    }

    pub async fn get_named_data_mgr_by_path(
        root_path: PathBuf,
    ) -> NdnResult<NamedDataMgr> {
        if !root_path.exists() {
            debug!("NamedDataMgr: create base dir:{}", root_path.to_string_lossy());
            fs::create_dir_all(root_path.clone()).await.unwrap();
        }
        //mgrid is the dir name of root_path
        let mgr_id = root_path.file_name().unwrap().to_str().unwrap().to_string();

        let mgr_config;
        let mgr_json_file = root_path.join("ndn_mgr.json");
        if !mgr_json_file.exists() {
            mgr_config = NamedDataMgrConfig {
                local_cache: None,
                mmap_cache_dir: None,
            };

            let mgr_json_str = serde_json::to_string(&mgr_config).unwrap();
            let mut mgr_json_file = File::create(mgr_json_file.clone()).await.unwrap();
            mgr_json_file
                .write_all(mgr_json_str.as_bytes())
                .await
                .unwrap();
        } else {
            let mgr_json_str = fs::read_to_string(mgr_json_file).await;
            if mgr_json_str.is_err() {
                warn!(
                    "NamedDataMgr: read mgr config failed! {}",
                    mgr_json_str.err().unwrap().to_string()
                );
                return Err(NdnError::NotFound(format!("named data mgr not found")));
            }
            let mgr_json_str = mgr_json_str.unwrap();
            let mgr_config_result = serde_json::from_str::<NamedDataMgrConfig>(&mgr_json_str);
            if mgr_config_result.is_err() {
                warn!(
                    "NamedDataMgr: parse mgr config failed! {}",
                    mgr_config_result.err().unwrap().to_string()
                );
                return Err(NdnError::NotFound(format!("named data mgr not found")));
            }
            mgr_config = mgr_config_result.unwrap();
        }

        Self::from_config(
            Some(mgr_id),
            root_path,
            mgr_config,
        )
        .await
    }

    pub async fn is_named_data_mgr_exist(
        named_data_mgr_id: Option<&str>,
    ) -> bool {
        let named_mgr_key = named_data_mgr_id.unwrap_or("default").to_string();
        let mut named_data_mgr_map = NAMED_DATA_MGR_MAP.lock().await;
        named_data_mgr_map.contains_key(&named_mgr_key)
    }

    pub async fn get_named_data_mgr_by_id(
        named_data_mgr_id: Option<&str>,
    ) -> Option<Arc<tokio::sync::Mutex<Self>>> {
        let named_mgr_key = named_data_mgr_id.unwrap_or("default").to_string();
        let mut named_data_mgr_map = NAMED_DATA_MGR_MAP.lock().await;

        let named_data_mgr = named_data_mgr_map.get(&named_mgr_key);
        if named_data_mgr.is_some() {
            //debug!("NamedDataMgr: get named data mgr by id:{},return existing mgr", named_mgr_key);
            return Some(named_data_mgr.unwrap().clone());
        }

        info!(
            "NamedDataMgr: auto create new named data mgr for mgr_id:{}",
            named_mgr_key
        );
        let root_path = get_buckyos_named_data_dir(named_mgr_key.as_str());
        //make sure the root path dir exists
  
        let result_mgr = Self::get_named_data_mgr_by_path(root_path).await;
    
        if result_mgr.is_err() {
            warn!(
                "NamedDataMgr: create mgr failed! {}",
                result_mgr.err().unwrap().to_string()
            );
            return None;
        }
        let result_mgr = Arc::new(tokio::sync::Mutex::new(result_mgr.unwrap()));
        named_data_mgr_map.insert(named_mgr_key, result_mgr.clone());
        return Some(result_mgr);
    }

    pub async fn from_config(
        mgr_id: Option<String>,
        root_path: PathBuf,
        config: NamedDataMgrConfig,
    ) -> NdnResult<Self> {
        let db_path = root_path.join("ndn_mgr.db").to_str().unwrap().to_string();
        let db = NamedDataMgrDB::new(db_path)?;

        
        // Create base directory if it doesn't exist
        tokio::fs::create_dir_all(&root_path).await.map_err(|e| {
            warn!("NamedDataMgr: create base dir failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;

        Ok(Self {
            base_dir: root_path,
            read_only: false,
            local_cache_base_dir: config.local_cache,
            mmap_cache_dir: config.mmap_cache_dir,
            mgr_id: mgr_id,
            db: db,
        })
    }

    fn get_chunk_path(&self, chunk_id: &ChunkId) -> PathBuf {
        //根据ChunkId的HashResult,产生一个三层的目录结构
        let hex_str = hex::encode(chunk_id.hash_result.clone());
        let len = hex_str.len();
        let dir1 = &hex_str[len-4..len-2];
        let dir2 = &hex_str[len-2..len];
        let file_name = format!("{}.{}",&hex_str,chunk_id.chunk_type.to_string().as_str());
        //let file_name = &hex_str[4..] + chunk_id.chunk_type.to_string().as_str();

        self.base_dir.join(dir1).join(dir2).join(file_name)

    }

    pub fn get_base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    fn get_cache_mmap_path(&self, chunk_id: &ChunkId) -> Option<String> {
        None
    }

    pub fn get_cache_path_obj(&self, url: &str) -> Option<PathObject> {
        None
    }

    pub fn update_cache_path_obj(&self, url: &str, path_obj: PathObject) -> NdnResult<()> {
        Ok(())
    }


    //return path_obj_jwt
    async fn get_path_obj(&self, path: &str) -> NdnResult<Option<String>> {
        let (_obj_id, path_obj_jwt) = self.db.get_path_target_objid(path)?;
        if path_obj_jwt.is_some() {
            return Ok(Some(path_obj_jwt.unwrap()));
        }
        Ok(None)
    }

    pub async fn get_obj_id_by_path_impl(&self, path: &str) -> NdnResult<(ObjId, Option<String>)> {
        let (obj_id, path_obj_jwt) = self.db.get_path_target_objid(path)?;
        //info!("get_obj_id_by_path_impl: path:{},obj_id:{}",path,obj_id.to_string());
        Ok((obj_id, path_obj_jwt))
    }

    pub async fn get_obj_id_by_path(
        mgr_id: Option<&str>,
        path: &str,
    ) -> NdnResult<(ObjId, Option<String>)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.get_obj_id_by_path_impl(path).await
    }

    //返回obj_id,path_obj_jwt和relative_path(如有)
    pub async fn select_obj_id_by_path_impl(
        &self,
        path: &str,
    ) -> NdnResult<(ObjId, Option<String>, Option<String>)> {
        let (root_path, obj_id, path_obj_jwt, relative_path) =
            self.db.find_longest_matching_path(path)?;
        Ok((obj_id, path_obj_jwt, relative_path))
    }

    pub async fn select_obj_id_by_path(
        mgr_id: Option<&str>,
        path: &str,
    ) -> NdnResult<(ObjId, Option<String>, Option<String>)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.select_obj_id_by_path_impl(path).await
    }

    pub async fn is_object_exist(&self, obj_id: &ObjId) -> NdnResult<bool> {
        let obj_state = self.query_object_by_id(obj_id).await?;
        match obj_state {
            ObjectState::NotExist => Ok(false),
            ObjectState::Link(_) => Ok(true),
            ObjectState::Object(_) => Ok(true),
            _ => Ok(false),
        }
    }

    pub async fn query_object_by_id(&self, obj_id: &ObjId) -> NdnResult<ObjectState> {
        let real_obj_result = self.db.get_object(obj_id);
        if real_obj_result.is_ok() {
            let (obj_type, obj_str) = real_obj_result.unwrap();
            return Ok(ObjectState::Object(obj_str));
        }

        let same_as_object_ids = self.db.get_same_as_object_by_target(obj_id);
        if same_as_object_ids.is_ok() {
            let same_as_object_ids = same_as_object_ids.unwrap();
            if same_as_object_ids.is_empty() {
                return Ok(ObjectState::NotExist);
            }
            return Ok(ObjectState::Link(ObjectLinkData::SameAs(same_as_object_ids[0].clone())));
        }

        return Ok(ObjectState::NotExist);
    }

    pub async fn get_real_object_impl(&self, obj_id: &ObjId) -> NdnResult<EncodedDocument> {
        let obj_state = self.query_object_by_id(obj_id).await?;
        match obj_state {
            ObjectState::Object(obj_str) => {
                let doc = EncodedDocument::from_str(obj_str).map_err(|e| {
                    warn!("get_object: decode object failed! {}", e.to_string());
                    NdnError::DecodeError(e.to_string())
                })?;
                Ok(doc)
            }
            ObjectState::Link(obj_link) => match obj_link {
                ObjectLinkData::SameAs(link_obj_id) => Box::pin(self.get_real_object_impl(&link_obj_id)).await,
                _ => Err(NdnError::InvalidLink(format!(
                    "object link not supported! {}",
                    obj_id.to_string()
                ))),
            },
            _ => Err(NdnError::NotFound(format!(
                "object not found! {}",
                obj_id.to_string()
            ))),
        }
    }

    pub async fn get_object(
        mgr_id: Option<&str>,
        obj_id: &ObjId,
        inner_obj_path: Option<String>,
    ) -> NdnResult<serde_json::Value> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.get_object_impl(obj_id, inner_obj_path).await
    }

    pub async fn get_object_impl(
        &self,
        obj_id: &ObjId,
        inner_obj_path: Option<String>,
    ) -> NdnResult<serde_json::Value> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let mut obj_body = None;
        // TODO: Add local cache support if needed
        // if self.local_cache_base_dir.is_some() {
        //     // Handle local cache
        // }

        let obj_result = self.get_real_object_impl(&obj_id).await;
        if obj_result.is_ok() {
            obj_body = Some(obj_result.unwrap());
        }
        
        if obj_body.is_some() {
            let obj_body = obj_body.unwrap();
            let obj_body = obj_body.to_json_value().map_err(|e| {                
                warn!("get_object: decode obj body failed! {}", e.to_string());
                NdnError::DecodeError(e.to_string())
            })?;

            if inner_obj_path.is_some() {
                let obj_path = inner_obj_path.unwrap();
                let obj_filed = get_by_json_path(&obj_body, &obj_path);
                if obj_filed.is_some() {
                    return Ok(obj_filed.unwrap());
                } else {
                    return Ok(serde_json::Value::Null);
                }
            } else {
                return Ok(obj_body);
            }
        }

        Err(NdnError::NotFound(obj_id.to_string()))
    }


    pub async fn put_object_impl(&self, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        self.db.set_object(obj_id, obj_id.obj_type.as_str(), obj_data)
    }
    //target obj is the same as obj_id
    pub async fn link_same_object(&self, obj_id: &ObjId, target_obj: &ObjId) -> NdnResult<()> {
        let link_data = ObjectLinkData::SameAs(target_obj.clone());
        self.db.set_object_link(obj_id, &link_data)
    }
    //target obj is a part of obj_id,the part is defined by range
    pub async fn link_part_of(&self, obj_id: &ObjId, target_obj: &ObjId, range: Range<u64>) -> NdnResult<()> {
        let link_data = ObjectLinkData::PartOf(target_obj.clone(), range);
        self.db.set_object_link(obj_id, &link_data)
    }

    pub async fn query_source_object_by_target(&self, target_obj: &ObjId) -> NdnResult<Option<ObjId>> {
        let same_objs = self.db.get_same_as_object_by_target(target_obj);
        if same_objs.is_ok() {
            let same_objs = same_objs.unwrap();
            if same_objs.len() > 0 {
                return Ok(Some(same_objs[0].clone()));
            }
        }
        Ok(None)
    }

    async fn get_chunk_item_impl(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        let chunk_item = self.db.get_chunk_item(chunk_id);
        if chunk_item.is_ok() {
            return chunk_item;
        }

        debug!("{} chunk_item not found,try to get same as object", chunk_id.to_string());
        let same_objs = self.db.get_same_as_object_by_target(&chunk_id.to_obj_id());
        if same_objs.is_ok() {
            let same_objs = same_objs.unwrap();
            if same_objs.len() > 0 {
                debug!("same as object:{}", same_objs[0].to_string());
                let result_item = Box::pin(self.get_chunk_item_impl(&ChunkId::from_obj_id(&same_objs[0]))).await;
                if result_item.is_ok() {
                    return result_item;
                }
            }
        }

        Err(NdnError::NotFound(format!(
            "chunk_item :{} not found!",
            chunk_id.to_string()
        )))
    }

    pub async fn put_object(mgr_id: Option<&str>, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.put_object_impl(obj_id, obj_data).await
    }

    pub async fn get_chunk_reader_by_path_impl(
        &self,
        path: &str,
        user_id: &str,
        app_id: &str,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64, ChunkId)> {
        let obj_id = self.db.get_path_target_objid(path)?;

        // Check if obj_id is a valid chunk id
        if !obj_id.0.is_chunk() {
            warn!(
                "get_chunk_reader_by_path: obj_id is not a chunk_id:{}",
                obj_id.0.to_string()
            );
            return Err(NdnError::InvalidParam(format!(
                "obj_id is not a chunk_id:{}",
                obj_id.0.to_string()
            )));
        }

        let chunk_id = ChunkId::from_obj_id(&obj_id.0);
        let (chunk_reader, chunk_size) = self
            .open_chunk_reader_impl(&chunk_id, offset, true)
            .await?;
        //let access_time = buckyos_get_unix_timestamp();
        //self.db.update_obj_access_time(&obj_id.0, access_time)?;
        Ok((chunk_reader, chunk_size, chunk_id))
    }

    pub async fn get_chunk_reader_by_path(
        mgr_id: Option<&str>,
        path: &str,
        user_id: &str,
        app_id: &str,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64, ChunkId)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr
            .get_chunk_reader_by_path_impl(path, user_id, app_id, offset)
            .await
    }

    pub async fn create_file_impl(
        &self,
        path: &str,
        obj_id: &ObjId,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<()> {
        self.db
            .create_path(path, obj_id, None, app_id, user_id)
            .map_err(|e| {
                warn!("create_file: create path failed! {}", e.to_string());
                e
            })?;
        info!("create ndn path:{} ==> {}", path, obj_id.to_string());
        Ok(())
    }

    pub async fn create_file(
        mgr_id: Option<&str>,
        path: &str,
        obj_id: &ObjId,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr
            .create_file_impl(path, obj_id, app_id, user_id)
            .await
    }

    pub async fn set_file_impl(
        &self,
        path: &str,
        new_obj_id: &ObjId,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<()> {
        let path_obj = PathObject::new(path.to_string(), new_obj_id.clone());
        let path_obj_str = serde_json::to_string(&path_obj).unwrap();
        self.db
            .set_path(path, &new_obj_id, None, path_obj_str, app_id, user_id)
            .map_err(|e| {
                warn!("update_file: update path failed! {}", e.to_string());
                e
            })?;
        info!("update ndn path:{} ==> {}", path, new_obj_id.to_string());
        Ok(())
    }

    pub async fn set_file(
        mgr_id: Option<&str>,
        path: &str,
        new_obj_id: &ObjId,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr
            .set_file_impl(path, new_obj_id, app_id, user_id)
            .await
    }

    pub async fn remove_file_impl(&self, path: &str) -> NdnResult<()> {
        self.db.remove_path(path, "", "").map_err(|e| {
            warn!("remove_file: remove path failed! {}", e.to_string());
            e
        })?;
        info!("remove ndn path:{}", path);
        Ok(())

        //TODO: 这里不立刻删除chunk,而是等统一的GC来删除
    }

    pub async fn remove_file(mgr_id: Option<&str>, path: &str) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.remove_file_impl(path).await
    }

    // pub async fn remove_dir_impl(&self, path: &str) -> NdnResult<()> {
    //     self.db.remove_dir_path(path).map_err(|e| {
    //         warn!("remove_dir: remove dir path failed! {}", e.to_string());
    //         e
    //     })?;
    //     info!("remove ndn dir path:{}", path);
    //     Ok(())
    // }

    // pub async fn remove_dir(mgr_id: Option<&str>, path: &str) -> NdnResult<()> {
    //     let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
    //     if named_mgr.is_none() {
    //         return Err(NdnError::NotFound(format!("named data mgr not found")));
    //     }
    //     let named_mgr = named_mgr.unwrap();
    //     let named_mgr = named_mgr.lock().await;
    //     named_mgr.remove_dir_impl(path).await
    // }

    //只有chunk完整准备好了，才是存在。写入到一半的chunk不会算存在
    //通过get_chunk_state可以得到更准确的chunk状态
    // pub async fn is_chunk_exist(
    //     &self,
    //     chunk_id: &ChunkId,
    //     is_auto_add: Option<bool>,
    // ) -> NdnResult<(bool, u64)> {
    //     let chunk_item = self.get_chunk_item_impl(chunk_id).await?;
    //     let (chunk_state, chunk_size,_progress) = chunk_state;
    //     match chunk_state {
    //         ChunkState::Completed => Ok((true, chunk_size)),
    //         ChunkState::LocalLink(local_chunk_info) => {
    //             //TODO: 这里需要检查local_chunk_info是否存在?
    //             return Ok((true, chunk_size));
    //         }
    //         _ => Ok((false, 0)),
    //     }
    // }


    pub async fn have_chunk(mgr_id: Option<&str>,chunk_id: &ChunkId) -> bool {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return false;
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        return named_mgr.have_chunk_impl(chunk_id).await;
    }

    pub async fn have_chunk_impl(&self, chunk_id: &ChunkId) -> bool {
        let query_result = self.query_chunk_state_impl(chunk_id).await;
        if query_result.is_err() {
            return false;
        }
        let (chunk_state, _chunk_size, _progress) = query_result.unwrap();
        //TODO:是否需要verify local file 保存的chunk的qcid?
        return chunk_state.can_open_reader();
    }

    //if chunk exist, return chunk size
    pub async fn check_chunk_exist_impl(&self, chunk_id: &ChunkId) -> NdnResult<u64> {
        unimplemented!()
    }

    pub async fn query_chunk_state_impl(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkState, u64, String)> {
        let chunk_item = self.get_chunk_item_impl(chunk_id).await;
        if chunk_item.is_err() {
            return Ok((ChunkState::NotExist, 0, "".to_string()));
        }
        let chunk_item = chunk_item.unwrap();
        let (chunk_state, chunk_size, progress) = (chunk_item.chunk_state, chunk_item.chunk_size, chunk_item.progress);
        return Ok((chunk_state, chunk_size, progress));
    }

    pub async fn query_chunk_state(
        mgr_id: Option<&str>,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkState, u64, String)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.query_chunk_state_impl(chunk_id).await
    }

    // pub async fn link_chunk_to_local_impl(&self, chunk_id: &ChunkId, local_file_path: &Path,range:Range<u64>,last_modify_time:u64,qcid:Option<String>) -> NdnResult<()> {
    //     let qcid = qcid.unwrap_or("_".to_string());
    //     let file_meta = local_file_path.metadata().map_err(|e| {
    //         warn!("link_chunk_to_local: get file metadata failed! {}", e.to_string());
    //         NdnError::IoError(e.to_string())
    //     })?;
    //     let file_size = file_meta.len();
    //     let link_data = LinkData::LocalFile(local_file_path.to_string_lossy().to_string(),range,last_modify_time,qcid);
    //     let obj_id = chunk_id.to_obj_id();
    //     self.link(&obj_id, &link_data).await?;
    //     Ok(())
    // }

    pub async fn open_store_chunk_reader_impl(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let (chunk_state, chunk_size,_progress) = self.query_chunk_state_impl(chunk_id).await?;
        let chunk_real_path;
        let chunk_range:Range<u64>;
        match chunk_state {
            ChunkState::Completed => {
                chunk_real_path = self.get_chunk_path(chunk_id);
                chunk_range = Range{start:0,end:chunk_size};
            }
            // ChunkState::Link(link_data) => {
            //     match link_data {
            //         LinkData::SameAs(link_obj_id) => {
            //             let link_chunk_id = ChunkId::from_obj_id(&link_obj_id);
            //             return Box::pin(self.open_store_chunk_reader_impl(&link_chunk_id, offset)).await;
            //         }
            //         LinkData::PartOf(link_obj_id,range) => {
            //             return Err(NdnError::Internal(("not supported".to_string())));
            //         }
            //         LinkData::LocalFile(file_path,range,_last_modify_time,_qcid) => {
            //             chunk_real_path = PathBuf::from(file_path);
            //             chunk_range = range;
            //         }
            //     }
            // }
            _ => {
                return Err(NdnError::InComplete(format!(
                    "chunk {} state not support open reader! state:{}",
                    chunk_id.to_string(),
                    chunk_state.to_str()
                )));
            }
        }

        let mut file = OpenOptions::new()
            .read(true) // 设置只读模式
            .open(&chunk_real_path)
            .await
            .map_err(|e| {
                warn!("open_chunk_reader: open file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
        
        if chunk_range.start != 0 {
            file.seek(SeekFrom::Start(chunk_range.start)).await.map_err(|e| {
                warn!("open_chunk_reader: seek file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
        }

        if offset > 0 {
            file.seek(SeekFrom::Current(offset as i64)).await.map_err(|e| {
                warn!("open_chunk_reader: seek file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
        }

        Ok((Box::pin(LimitReader::from_reader(Box::pin(file), chunk_range.end - chunk_range.start - offset)), chunk_size))
    }

    pub async fn open_chunk_reader(
        mgr_id: Option<&str>,
        chunk_id: &ChunkId,
        offset: u64,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!(
                "named data mgr {} not found",
                mgr_id.unwrap()
            )));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr
            .open_chunk_reader_impl(chunk_id, offset, auto_cache)
            .await
    }

    pub async fn open_chunk_reader_impl(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        // memroy cache ==> local disk cache ==> local store
        //at first ,do access control
        let mcache_file_path = self.get_cache_mmap_path(chunk_id);
        if mcache_file_path.is_some() {
            let mcache_file_path = mcache_file_path.unwrap();

            let mut file = OpenOptions::new()
                .read(true) // 设置只读模式
                .open(&mcache_file_path)
                .await;

            if file.is_ok() {
                let mut file = file.unwrap();
                let file_meta = file.metadata().await.unwrap();
                if offset > 0 {
                    file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                        warn!(
                            "get_chunk_reader: seek cache file failed! {}",
                            e.to_string()
                        );
                        NdnError::IoError(e.to_string())
                    })?;
                }
                //info!("get_chunk_reader:return tmpfs cache file:{}", mcache_file_path);
                return Ok((Box::pin(file), file_meta.len()));
            }
        }

        let chunk_item = self.get_chunk_item_impl(chunk_id).await?;
        let file = match chunk_item.chunk_state {
            ChunkState::Completed => {
                let chunk_real_path = self.get_chunk_path(&chunk_item.chunk_id);
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&chunk_real_path)
                    .await?;
                if offset > 0 {
                    file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                        warn!("open_chunk_reader: seek chunk file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                }
                file
            },
            ChunkState::LocalLink(local_chunk_info) => {
                let chunk_real_path = PathBuf::from(local_chunk_info.path);
                let mut real_offset = 0;
                if local_chunk_info.range.is_some() {
                    real_offset = local_chunk_info.range.unwrap().start;
                }
                if offset > 0 {
                    real_offset += offset;
                }
                let mut file = File::open(&chunk_real_path).await.map_err(|e| {
                    warn!("open_chunk_reader: open file failed! {}", e.to_string());
                    NdnError::IoError(e.to_string())
                })?;
                if real_offset > 0 {
                    file.seek(SeekFrom::Start(real_offset)).await.map_err(|e| {
                        warn!("open_chunk_reader: seek file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                }
                file
            },
            _ => {
                return Err(NdnError::Internal(format!(
                    "chunk {} state not support open reader! state:{}",
                    chunk_id.to_string(),
                    chunk_item.chunk_state.to_str()
                )));
            }
        };

        Ok((Box::pin(LimitReader::from_reader(Box::pin(file), chunk_item.chunk_size - offset)), chunk_item.chunk_size))
    }


    //return chunklist_reader,chunklist_size(total_size)
    pub async fn open_chunklist_reader(
        mgr_id: Option<&str>,
        chunklist_id: &ObjId,
        seek_from: SeekFrom,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        if chunklist_id.obj_type != OBJ_TYPE_CHUNK_LIST_SIMPLE {
            return Err(NdnError::InvalidParam(format!("chunklist_id is not OBJ_TYPE_CHUNK_LIST_SIMPLE id:{}", chunklist_id.to_string())));
        }

        // 1. Get named data manager by id
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id)
            .await
            .ok_or_else(|| {
                NdnError::NotFound(format!(
                    "named data mgr {} not found",
                    mgr_id.unwrap_or("default")
                ))
            })?;

        // 2. Get chunklist object
        let obj_data = {
            let mgr = named_mgr.lock().await;
            mgr.get_object_impl(chunklist_id, None).await?
        };

        let chunk_ids : Vec<ChunkId> = serde_json::from_value(obj_data).map_err(|e| {
            NdnError::InvalidData(format!("parse chunk_ids failed:{}", e.to_string()))
        })?;

        let chunk_list = SimpleChunkList::from_chunk_list(chunk_ids)?;
        let total_size = chunk_list.total_size;

        let reader = SimpleChunkListReader::new(named_mgr, chunk_list, seek_from, auto_cache).await?;

        Ok((Box::pin(reader), total_size))
    }

    //打开writer并允许writer已经存在
    pub async fn open_chunk_writer_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        offset: u64,
    ) -> NdnResult<(ChunkWriter, String)> {
        let mut real_chunk_size = chunk_size;
        let mut no_chunk_item = false;
        let mut chunk_state = ChunkState::NotExist;
        let mut progress = "".to_string();
        let chunk_item = self.get_chunk_item_impl(chunk_id).await;

        if chunk_item.is_err() {
            let chunk_error = chunk_item.err().unwrap();
            if !chunk_error.is_not_found() {
                return Err(chunk_error);
            };
        } else {
            let chunk_item = chunk_item.unwrap();
            chunk_state = chunk_item.chunk_state;
            if chunk_state == ChunkState::Completed {
                return Err(NdnError::AlreadyExists(format!("chunk {} already completed", chunk_id.to_string())));
            }
            progress = chunk_item.progress;
            if !chunk_state.can_open_writer() {
                return Err(NdnError::Internal(format!(
                    "chunk {} state not support open writer! {}",
                    chunk_id.to_string(),chunk_state.to_str()
                )));
            }
        }
       
        //let chunk_item = self.db.get_chunk_item(chunk_id).await;
        let chunk_path = self.get_chunk_path(chunk_id);
        if chunk_state == ChunkState::Incompleted {
            let file_meta = fs::metadata(&chunk_path).await.map_err(|e| {
                warn!("open_chunk_writer: get metadata failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            if offset <= file_meta.len() {
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(&chunk_path)
                    .await
                    .map_err(|e| {
                        warn!("open_chunk_writer: open file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;

                let std_file = file.into_std().await;
                let mut file_lock = std_file.try_lock_exclusive().map_err(|e| {
                    warn!("open_chunk_writer: lock file failed! {}", e.to_string());
                    NdnError::IoError(e.to_string())
                })?;
                let mut file = tokio::fs::File::from_std(std_file);

                if offset != 0 {
                    file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                        warn!("open_chunk_writer: seek file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                } else {
                    file.seek(SeekFrom::End(0)).await.map_err(|e| {
                        warn!("open_chunk_writer: seek file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                }

                if progress.len() < 2 {
                    let progress = json!({
                        "pos":file_meta.len(),
                    })
                    .to_string();
                    return Ok((Box::pin(file), progress));
                }
                return Ok((Box::pin(file), progress));
            } else {
                warn!(
                    "open_chunk_writer: offset too large! {}",
                    chunk_id.to_string()
                );
                return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
            }
        } else {
            if offset != 0 {
                warn!("open_chunk_writer: offset not 0! {}", chunk_id.to_string());
                return Err(NdnError::Internal("offset not 0".to_string()));
            }
            // Create parent directories if they don't exist
            if let Some(parent) = std::path::Path::new(&chunk_path).parent() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    warn!("open_chunk_writer: create dir failed! {}", e.to_string());
                    NdnError::IoError(e.to_string())
                })?;
            }

            let file = File::create(&chunk_path).await.map_err(|e| {
                warn!("open_chunk_writer: create file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            let std_file = file.into_std().await;
            let mut file_lock = std_file.try_lock_exclusive().map_err(|e| {
                warn!("open_chunk_writer: lock file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
            let file = tokio::fs::File::from_std(std_file);

            //创建chunk_item
            let chunk_item = ChunkItem::new(&chunk_id, real_chunk_size);
            self.db.set_chunk_item(&chunk_item)?;
            debug!("create chunk item {} to db success", chunk_id.to_string());

            return Ok((Box::pin(file), "".to_string()));
        }
    }

    //打开writer,不允许writer已经存在
    pub async fn open_new_chunk_writer_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
    ) -> NdnResult<ChunkWriter> {
        let mut chunk_state = ChunkState::NotExist;
        let mut progress = "".to_string();

        let chunk_item = self.get_chunk_item_impl(chunk_id).await;
        if chunk_item.is_err() {
            let chunk_error = chunk_item.err().unwrap();
            if !chunk_error.is_not_found() {
                return Err(chunk_error);
            };
        } else {
            let chunk_item = chunk_item.unwrap();
            chunk_state = chunk_item.chunk_state;
            progress = chunk_item.progress;
        }

        if !chunk_state.can_open_new_writer() {
            return Err(NdnError::Internal(format!(
                "chunk {} state not support open new writer! {}",
                chunk_id.to_string(),chunk_state.to_str()
            )));
        }
        let chunk_path = self.get_chunk_path(&chunk_id);

        // Create parent directories if they don't exist
        if let Some(parent) = std::path::Path::new(&chunk_path).parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                warn!(
                    "open_new_chunk_writer: create dir failed! {}",
                    e.to_string()
                );
                NdnError::IoError(e.to_string())
            })?;
        }

        //已独占写模式创建文件
        let file = File::create(&chunk_path).await.map_err(|e| {
            warn!("open_chunk_writer: create file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;

        let std_file = file.into_std().await;
        let mut file_lock = std_file.try_lock_exclusive().map_err(|e| {
            warn!("open_new_chunk_writer: lock file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        let file = tokio::fs::File::from_std(std_file);

        let chunk_item = ChunkItem::new(chunk_id, chunk_size);
        self.db.set_chunk_item(&chunk_item)?;
        return Ok(Box::pin(file));
    }

    //return chunk_id,progress_info


    pub async fn open_chunk_writer(
        mgr_id: Option<&str>,
        chunk_id: &ChunkId,
        chunk_size: u64,
        offset: u64,
    ) -> NdnResult<(ChunkWriter, String)> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr
            .open_chunk_writer_impl(chunk_id, chunk_size, offset)
            .await
    }


    pub async fn update_chunk_progress_impl(
        &self,
        chunk_id: &ChunkId,
        progress: String,
    ) -> NdnResult<()> {
        return self
            .db
            .update_chunk_progress(chunk_id, progress);
    }

    //writer已经写入完成，此时可以进行一次可选的hash校验
    pub async fn complete_chunk_writer_impl(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let mut chunk_item = self.db.get_chunk_item(chunk_id)?;
        chunk_item.chunk_state = ChunkState::Completed;
        chunk_item.progress = "".to_string();
        info!(
            "complete_chunk_writer: complete chunk {} success itemsize:{}",
            chunk_id.to_string(),
            chunk_item.chunk_size
        );
        self.db.set_chunk_item(&chunk_item)?;
        Ok(())
    }

    pub async fn complete_chunk_writer_and_rename_impl(&self, chunk_id: &ChunkId, new_chunk_id: &ChunkId) -> NdnResult<()> {
        unimplemented!();
    }


    pub async fn complete_chunk_writer(mgr_id: Option<&str>, chunk_id: &ChunkId) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.complete_chunk_writer_impl(chunk_id).await
    }

    pub async fn add_chunk_by_link_to_local_file_impl(&self, chunk_id: &ChunkId,chunk_size: u64, chunk_local_info: &ChunkLocalInfo) -> NdnResult<()> {
        let chunk_item = ChunkItem::new_local_file(chunk_id, chunk_size, chunk_local_info);
        return self.db.set_chunk_item(&chunk_item);
    }


    //=====================下面的都是helper函数了======================
    //针对小于1MB的 chunk,推荐直接返回内存
    pub async fn get_chunk_data(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
        let (mut chunk_reader, chunk_size) =
            self.open_store_chunk_reader_impl(chunk_id, 0).await?;
        let mut buffer = Vec::with_capacity(chunk_size as usize);
        chunk_reader.read_to_end(&mut buffer).await.map_err(|e| {
            warn!("get_chunk_data: read file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        Ok(buffer)
    }

    pub async fn get_chunk_piece(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        piece_size: u32,
    ) -> NdnResult<Vec<u8>> {
        let (mut reader, chunk_size) = self.open_store_chunk_reader_impl(chunk_id, 0).await?;
        let mut buffer = vec![0u8; piece_size as usize];
        reader.read_exact(&mut buffer).await.map_err(|e| {
            warn!("get_chunk_piece: read file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        Ok(buffer)
    }

    pub async fn put_chunk_by_reader_impl(&self, chunk_id: &ChunkId, chunk_size: u64, mut reader: &mut ChunkReader) -> NdnResult<()> {
        let mut chunk_writer = self.open_new_chunk_writer_impl(chunk_id, chunk_size).await?;
        // ensure we only copy exactly chunk_size bytes
        let mut limited = reader.take(chunk_size);
        let copy_bytes = tokio::io::copy(&mut limited,&mut chunk_writer).await?;
        if copy_bytes != chunk_size {
            return Err(NdnError::IoError(format!("copy chunk failed! expected:{} actual:{}", chunk_size, copy_bytes)));
        }
        self.complete_chunk_writer_impl(chunk_id).await?;
        Ok(())
    }

    pub async fn put_chunk_by_reader(
        mgr_id: Option<&str>,
        chunk_id: &ChunkId,
        chunk_size: u64,
        mut reader: &mut ChunkReader,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let real_named_mgr = named_mgr.lock().await;
        let mut chunk_writer = real_named_mgr.open_new_chunk_writer_impl(chunk_id, chunk_size).await?;
        drop(real_named_mgr);

        // ensure we only copy exactly chunk_size bytes
        let mut limited = reader.take(chunk_size);
        let copy_bytes = tokio::io::copy(&mut limited,&mut chunk_writer).await?;
        if copy_bytes != chunk_size {
            return Err(NdnError::IoError(format!("copy chunk failed! expected:{} actual:{}", chunk_size, copy_bytes)));
        }
        
        let real_named_mgr = named_mgr.lock().await;
        real_named_mgr.complete_chunk_writer_impl(chunk_id).await?;
        Ok(())
    }

    //写入一个在内存中的完整的chunk
    pub async fn put_chunk(
        &self,
        chunk_id: &ChunkId,
        chunk_data: &[u8],
        need_verify: bool,
    ) -> NdnResult<()> {
        if need_verify {
            let hash_method = chunk_id.chunk_type.to_hash_method()?;
            let mut chunk_hasher = ChunkHasher::new_with_hash_method(hash_method)?;
            let hash_bytes = chunk_hasher.calc_from_bytes(&chunk_data);
            if !chunk_id.equal(&hash_bytes) {
                warn!(
                    "put_chunk: chunk_id not equal hash_bytes! {}",
                    chunk_id.to_string()
                );
                return Err(NdnError::InvalidId(format!(
                    "chunk_id not equal hash_bytes! {}",
                    chunk_id.to_string()
                )));
            }
        }

        let mut chunk_writer = self
            .open_new_chunk_writer_impl(chunk_id, chunk_data.len() as u64)
            .await?;
        chunk_writer.write_all(chunk_data).await.map_err(|e| {
            warn!("put_chunk: write file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        self.complete_chunk_writer_impl(chunk_id).await?;

        Ok(())
    }


    //下面是一些helper函数
    pub async fn pub_object(
        mgr_id: Option<&str>,
        will_pub_obj: serde_json::Value,
        obj_type: &str,
        ndn_path: &str,
        user_id: &str,
        app_id: &str,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let (obj_id, obj_str) = build_named_object_by_json(obj_type, &will_pub_obj);
        let mut named_mgr = named_mgr.lock().await;
        named_mgr.put_object_impl(&obj_id, &obj_str).await?;
        named_mgr
            .create_file_impl(ndn_path, &obj_id, app_id, user_id)
            .await?;
        Ok(())
    }

    pub async fn sign_obj(
        mgr_id: Option<&str>,
        will_sign_obj_id: ObjId,
        obj_jwt: String,
        user_id: &str,
        app_id: &str,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let mut named_mgr = named_mgr.lock().await;
        named_mgr
            .put_object_impl(&will_sign_obj_id, &obj_jwt)
            .await?;
        Ok(())
    }


    pub async fn sigh_path_obj_impl(&self, path: &str, path_obj_jwt: &str) -> NdnResult<()> {
        let path_obj_json: serde_json::Value = decode_jwt_claim_without_verify(path_obj_jwt)
            .map_err(|e| {
                warn!(
                    "sigh_path_obj: decode path obj jwt failed! {}",
                    e.to_string()
                );
                NdnError::DecodeError(e.to_string())
            })?;

        let path_obj: PathObject = serde_json::from_value(path_obj_json).map_err(|e| {
            warn!("sigh_path_obj: parse path obj failed! {}", e.to_string());
            NdnError::DecodeError(e.to_string())
        })?;

        if path_obj.path != path {
            return Err(NdnError::InvalidParam(format!(
                "path_obj.path != path:{}",
                path
            )));
        }

        self.db.set_path_obj_jwt(path, path_obj_jwt).map_err(|e| {
            warn!("sigh_path_obj: set path obj jwt failed! {}", e.to_string());
            e
        })?;
        Ok(())
    }

    pub async fn sigh_path_obj(
        mgr_id: Option<&str>,
        path: &str,
        path_obj_jwt: &str,
    ) -> NdnResult<()> {
        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
        if named_mgr.is_none() {
            return Err(NdnError::NotFound(format!("named data mgr not found")));
        }
        let named_mgr = named_mgr.unwrap();
        let named_mgr = named_mgr.lock().await;
        named_mgr.sigh_path_obj_impl(path, path_obj_jwt).await
    }


    pub async fn gc_worker(db_path: &str) -> NdnResult<()> {
        let mut conn = Connection::open(&db_path).map_err(|e| {
            warn!("NamedDataMgrDB: open database failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "DELETE FROM obj_ref_update_queue WHERE ref_count = 0",
            [],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: delete obj ref update queue failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        loop {
            let tx = conn.transaction().map_err(|e| {
                warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

            //在obj_ref_update_queue中pop出一条最旧的记录
            let row = tx.query_row("SELECT obj_id, ref_count FROM obj_ref_update_queue ORDER BY update_time ASC LIMIT 1", [], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
            });
            if row.is_ok() {
                let (obj_id_str, ref_count) = row.unwrap();
                let obj_id = ObjId::new(obj_id_str.as_str());
                if obj_id.is_err() {
                    continue;;
                }

                if ref_count == 0 {
                    continue;;
                }
                let obj_id = obj_id.unwrap();
                //尝试获取obj_data,如果失败则创建一个占位的空记录
                let obj_row = tx.query_row("SELECT obj_data,ref_count FROM objects WHERE obj_id = ?1", [obj_id_str.clone()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
                });
                if obj_row.is_ok() {
                    let (obj_data, _ref_count) = obj_row.unwrap();
                    NamedDataMgrDB::update_obj_ref_count(&tx, &obj_id, Some(obj_data.as_str()), ref_count)?;
                } else {
                    debug!("NamedDataMgrDB: insert object without obj_data ! {}", obj_id_str.as_str());
                    tx.execute("INSERT INTO objects (obj_id, obj_type,obj_data, ref_count,create_time,last_access_time) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", 
                    [obj_id_str.clone(), obj_id.obj_type.clone(), "".to_string(), ref_count.to_string(), buckyos_get_unix_timestamp().to_string(), buckyos_get_unix_timestamp().to_string()]).map_err(|e| {
                        warn!("NamedDataMgrDB: insert objects failed! {}", e.to_string());
                        NdnError::DbError(e.to_string())
                    })?;
                }
                //Pop这条记录
                tx.execute("DELETE FROM obj_ref_update_queue WHERE obj_id = ?1", [obj_id_str]).map_err(|e| {
                    warn!("NamedDataMgrDB: delete obj ref update queue failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;

                tx.commit().map_err(|e| {
                    warn!("NamedDataMgrDB: commit transaction failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
            } else {
                break;
            }

        }       

        Ok(())
    }

    pub async fn gc_objects(db_path: &str) -> NdnResult<()> {
        let mut conn = Connection::open(&db_path).map_err(|e| {
            warn!("NamedDataMgrDB: open database failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        let mut remove_count = 0;

        loop {
            let now = buckyos_get_unix_timestamp();
            let last_keep_time = now - 60 * 60 * 24; //1天

            // 先查询要删除的chunk_id
            let chunk_ids_to_delete = {
                let mut stmt = conn.prepare_cached("SELECT chunk_id FROM chunk_items WHERE ref_count = 0 AND update_time < ?1 ORDER BY update_time ASC LIMIT 64").map_err(|e| {
                    warn!("NamedDataMgrDB: prepare statement failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                let mut stmt_iter = stmt.query([last_keep_time.to_string()]).map_err(|e| {
                    warn!("NamedDataMgrDB: query statement failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                
                let mut chunk_ids_to_delete = Vec::new();
                while let Ok(Some(row)) = stmt_iter.next() {
                    let chunk_id = row.get::<_, String>(0).map_err(|e| {
                        warn!("NamedDataMgrDB: get chunk id failed! {}", e.to_string());
                        NdnError::DbError(e.to_string())
                    })?;
                    chunk_ids_to_delete.push(chunk_id.clone());
                    debug!("gc_objects: will delete chunk_id: {}", chunk_id);
                }
                chunk_ids_to_delete
            };
            
            // 如果没有找到要删除的记录，退出循环
            if chunk_ids_to_delete.is_empty() {
                break;
            }
            
            // 在新的事务中删除找到的chunk记录
            let tx = conn.transaction().map_err(|e| {
                warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            
            for chunk_id in &chunk_ids_to_delete {
                //删除本地文件
                
                tx.execute(
                    "DELETE FROM chunk_items WHERE chunk_id = ?1",
                    params![chunk_id],
                ).map_err(|e| {
                    warn!("NamedDataMgrDB: delete chunk failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                info!("gc_objects: deleted chunk_id: {}", chunk_id);
                remove_count += 1;
            }
            
            // 提交事务
            tx.commit().map_err(|e| {
                warn!("NamedDataMgrDB: commit transaction failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            
            info!("gc_objects: deleted {} chunk records", chunk_ids_to_delete.len());
            
            //再删除objects中ref_count为0的记录
            if remove_count >= 1024 {
                break;
            }
        }

        let now = buckyos_get_unix_timestamp();
        let last_keep_time = now - 60 * 60 * 24; //1天
        let remove_obj_count = conn.execute("DELETE FROM objects WHERE ref_count = 0 AND last_access_time < ?1", [last_keep_time.to_string()]).map_err(|e| {
            warn!("NamedDataMgrDB: delete objects failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        info!("gc_objects: deleted {} object records", remove_obj_count);

        Ok(())

    }

    pub fn start_gc_thread(&self) -> NdnResult<()> {
        let db_path = self.db.db_path.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                let worker_result = Self::gc_worker(&db_path).await;
                if worker_result.is_ok() {
                    info!("gc_worker success,obj_ref_update_queue is clean!");
                    //此时可以尝试真正的删除对象了，总是从chunk开始
                    //Self::gc_object(&db_path).await;
                }
            }
        });
        Ok(())
    }
}

pub type NamedDataMgrRef = Arc<tokio::sync::Mutex<NamedDataMgr>>;

lazy_static! {
    pub static ref NAMED_DATA_MGR_MAP: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<NamedDataMgr>>>>> =
        { Arc::new(tokio::sync::Mutex::new(HashMap::new())) };
}
