use futures_util::StreamExt;
use log::{info, warn};
use reqwest::{header, Body, Client, StatusCode};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_util::io::{ReaderStream, StreamReader};

use ndm::{NamedDataMgr, ReadOptions};
use named_store::ChunkStoreState;
use ndn_lib::{
    copy_chunk, cyfs_get_obj_id_from_url, get_cyfs_resp_headers, verify_named_object_from_str,
    ChunkHasher, ChunkId, ChunkReader, CYFSHttpRespHeaders, DirObject, FileObject, NdnAction,
    NdnError, NdnProgressCallback, NdnResult, ObjId, StoreMode, OBJ_TYPE_CHUNK_LIST_SIMPLE,
    OBJ_TYPE_DIR, OBJ_TYPE_FILE,
};

#[derive(Debug, Clone)]
pub enum ChunkWorkState {
    Idle,
    Downloading(u64, u64),
    DownloadError(String),
}

pub struct NdnGetChunkResult {
    pub chunk_id: ChunkId,
    pub chunk_size: u64,
    pub reader: ChunkReader,
}

pub enum ChunkWriterOpenMode {
    AlwaysNew,
    AutoResume,
    SpecifiedOffset(u64, std::io::SeekFrom),
}

pub struct NdnClient {
    default_ndn_mgr_id: Option<String>,
    session_token: Option<String>,
    default_remote_url: Option<String>,
    pub enable_remote_pull: bool,
    pub enable_zone_pull: bool,
    pub obj_id_in_host: bool,
    pub force_trust_remote: bool,
}

impl NdnClient {
    pub fn new(default_remote_url: String, session_token: Option<String>, named_mgr_id: Option<String>) -> Self {
        Self {
            default_ndn_mgr_id: named_mgr_id,
            session_token,
            default_remote_url: Some(default_remote_url),
            enable_remote_pull: false,
            enable_zone_pull: false,
            obj_id_in_host: false,
            force_trust_remote: false,
        }
    }

    fn build_http_client() -> NdnResult<Client> {
        Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NdnError::Internal(format!("Failed to create client: {}", e)))
    }

    pub fn gen_chunk_url(&self, chunk_id: &ChunkId, base_url: Option<String>) -> String {
        let real_base = base_url
            .or_else(|| self.default_remote_url.clone())
            .unwrap_or_default();

        if self.obj_id_in_host {
            format!("{}.{}", chunk_id.to_base32(), real_base)
        } else {
            format!("{}/{}", real_base, chunk_id.to_base32())
        }
    }

    pub fn gen_obj_url(&self, obj_id: &ObjId, base_url: Option<String>) -> String {
        let real_base = base_url
            .or_else(|| self.default_remote_url.clone())
            .unwrap_or_default();
        format!("{}/{}", real_base, obj_id.to_base32())
    }

    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
        target_url: Option<String>,
    ) -> NdnResult<(ChunkStoreState, u64)> {
        let chunk_url = target_url.unwrap_or_else(|| self.gen_chunk_url(chunk_id, None));
        let client = Self::build_http_client()?;

        let head_res = client
            .head(&chunk_url)
            .send()
            .await
            .map_err(|e| {
                NdnError::RemoteError(format!(
                    "query chunk state request by HEAD ({}) failed: {}",
                    chunk_url, e
                ))
            })?;

        let content_length = head_res.content_length().unwrap_or(0);
        match head_res.status() {
            StatusCode::OK => Ok((ChunkStoreState::Completed, content_length)),
            StatusCode::NOT_FOUND => Ok((ChunkStoreState::NotExist, 0)),
            StatusCode::PARTIAL_CONTENT => Ok((ChunkStoreState::Incompleted, content_length)),
            StatusCode::CREATED => Ok((ChunkStoreState::New, 0)),
            s => Err(NdnError::RemoteError(format!("HEAD request failed: {}", s))),
        }
    }

    pub async fn push_chunk(&self, chunk_id: ChunkId, target_url: Option<String>) -> NdnResult<()> {
        let chunk_url = target_url.unwrap_or_else(|| self.gen_chunk_url(&chunk_id, None));
        let (chunk_state, already_uploaded_size) = self
            .query_chunk_state(&chunk_id, Some(chunk_url.clone()))
            .await?;

        if matches!(chunk_state, ChunkStoreState::Completed) {
            info!("push_chunk: remote chunk already exists, skip");
            return Ok(());
        }

        let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(self.default_ndn_mgr_id.as_deref())
            .await
            .ok_or_else(|| NdnError::NotFound("named data mgr not found".to_string()))?;
        let guard = named_mgr.lock().await;
        let (chunk_reader, len) = guard
            .open_chunk_reader(
                &chunk_id,
                already_uploaded_size,
                ReadOptions { auto_pull: false },
            )
            .await?;
        drop(guard);

        let client = Self::build_http_client()?;
        let stream = ReaderStream::new(chunk_reader);
        let mut req = client
            .put(chunk_url.clone())
            .header("Content-Type", "application/octet-stream")
            .header("cyfs-chunk-size", len.to_string())
            .header("cyfs-chunk-offset", already_uploaded_size.to_string());
        if let Some(token) = &self.session_token {
            req = req.bearer_auth(token);
        }

        let res = req
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;

        if !res.status().is_success() {
            return Err(NdnError::RemoteError(format!(
                "HTTP error: {} for {}",
                res.status(),
                chunk_url
            )));
        }

        info!("PUSH CHUNK {} => {} success", chunk_id.to_string(), chunk_url);
        Ok(())
    }

    pub async fn get_obj_by_id(&self, obj_id: ObjId) -> NdnResult<serde_json::Value> {
        let obj_url = self.gen_obj_url(&obj_id, None);
        let (_, obj_data) = self.get_obj_by_url(&obj_url, Some(obj_id)).await?;
        Ok(obj_data)
    }

    pub async fn get_obj_by_url(
        &self,
        url: &str,
        known_obj_id: Option<ObjId>,
    ) -> NdnResult<(ObjId, serde_json::Value)> {
        let client = Self::build_http_client()?;
        let mut req = client.get(url);
        if let Some(token) = &self.session_token {
            req = req.bearer_auth(token);
        }

        let res = req
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;

        if !res.status().is_success() {
            return Err(NdnError::from_http_status(res.status(), url.to_string()));
        }

        let cyfs_resp_headers = get_cyfs_resp_headers(res.headers())?;
        let obj_str = res
            .text()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Failed to read response body: {}", e)))?;

        if let Some(expect) = known_obj_id {
            let obj_json = verify_named_object_from_str(&expect, &obj_str)?;
            return Ok((expect, obj_json));
        }

        if let Some(obj_id) = cyfs_resp_headers.obj_id {
            let obj_json = verify_named_object_from_str(&obj_id, &obj_str)?;
            return Ok((obj_id, obj_json));
        }

        if let Ok((obj_id_from_url, _)) = cyfs_get_obj_id_from_url(url) {
            let obj_json = verify_named_object_from_str(&obj_id_from_url, &obj_str)?;
            return Ok((obj_id_from_url, obj_json));
        }

        Err(NdnError::InvalidId(format!(
            "no object id in headers/url for {}",
            url
        )))
    }

    pub async fn open_chunk_reader_by_url(
        &self,
        chunk_url: &str,
        expect_chunk_id: Option<ChunkId>,
        range: Option<Range<u64>>,
    ) -> NdnResult<(ChunkReader, CYFSHttpRespHeaders)> {
        let client = Self::build_http_client()?;
        let mut req = client.get(chunk_url);

        if let Some(range) = range {
            if range.end > range.start {
                req = req.header(header::RANGE, format!("bytes={}-{}", range.start, range.end - 1));
            }
        }

        if let Some(token) = &self.session_token {
            req = req.bearer_auth(token);
        }

        let res = req
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("Request failed: {}", e)))?;

        let status = res.status();
        if !(status.is_success() || status == StatusCode::PARTIAL_CONTENT) {
            return Err(NdnError::from_http_status(status, chunk_url.to_string()));
        }

        let mut cyfs_resp_headers = get_cyfs_resp_headers(res.headers())?;
        if cyfs_resp_headers.obj_size.is_none() {
            cyfs_resp_headers.obj_size = res.content_length();
        }

        if let Some(expect) = expect_chunk_id {
            if let Some(obj_id) = cyfs_resp_headers.obj_id.clone() {
                let chunk_id = ChunkId::from_obj_id(&obj_id);
                if chunk_id != expect {
                    return Err(NdnError::InvalidId(format!(
                        "chunk id not match, known:{} remote:{}",
                        expect.to_string(),
                        chunk_id.to_string()
                    )));
                }
            }
        }

        let stream = res.bytes_stream().map(|chunk| {
            chunk.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        });
        let reader: ChunkReader = Box::pin(StreamReader::new(stream));

        Ok((reader, cyfs_resp_headers))
    }

    pub async fn pull_chunk(&self, chunk_id: ChunkId, pull_mode: StoreMode) -> NdnResult<u64> {
        let chunk_url = self.gen_chunk_url(&chunk_id, None);
        self.pull_chunk_by_url(chunk_url, chunk_id, pull_mode).await
    }

    pub async fn pull_chunk_by_url(
        &self,
        chunk_url: String,
        chunk_id: ChunkId,
        pull_mode: StoreMode,
    ) -> NdnResult<u64> {
        if pull_mode.need_store_to_named_mgr() {
            warn!(
                "pull_chunk_by_url: store-to-named-mgr path is unavailable after refactor, url={}",
                chunk_url
            );
        }

        if !pull_mode.is_store_to_local() {
            return Err(NdnError::Unsupported(
                "pull_chunk currently only supports local-file mode".to_string(),
            ));
        }

        if let StoreMode::LocalFile(local_path, _, _) = &pull_mode {
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if !local_path.exists() {
                tokio::fs::File::create(local_path).await?;
            }
        }

        let (mut remote_reader, resp_headers) = self
            .open_chunk_reader_by_url(chunk_url.as_str(), Some(chunk_id.clone()), None)
            .await?;

        let mut local_writer = pull_mode.open_local_writer().await?;
        let copied = copy_chunk(chunk_id, &mut remote_reader, &mut local_writer, None, None).await?;
        local_writer.flush().await?;

        let size = resp_headers.obj_size.unwrap_or(copied);
        Ok(size)
    }

    pub async fn pull_chunklist(
        &self,
        chunk_list_id: &ObjId,
        pull_mode: StoreMode,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    ) -> NdnResult<u64> {
        let chunk_list_obj = self.get_obj_by_id(chunk_list_id.clone()).await?;
        let chunk_list = ndn_lib::SimpleChunkList::from_json_value(chunk_list_obj)?;

        let mut offset = 0u64;
        for chunk_id in chunk_list.body {
            let chunk_size = chunk_id.get_length().ok_or_else(|| {
                NdnError::InvalidData(format!(
                    "invalid chunk id without length: {}",
                    chunk_id.to_string()
                ))
            })?;

            let chunk_pull_mode = match &pull_mode {
                StoreMode::LocalFile(local_path, range, need_pull_to_named_mgr) => StoreMode::LocalFile(
                    local_path.clone(),
                    range.start + offset..range.start + offset + chunk_size,
                    *need_pull_to_named_mgr,
                ),
                _ => pull_mode.clone(),
            };

            self.pull_chunk(chunk_id.clone(), chunk_pull_mode).await?;
            offset += chunk_size;

            if let Some(cb) = progress_callback.as_ref() {
                let mut cb = cb.lock().await;
                let result = cb(
                    format!("chunk:{}", chunk_id.to_string()),
                    NdnAction::ChunkOK(chunk_id, chunk_size),
                )
                .await?;
                if !result.is_continue() {
                    break;
                }
            }
        }

        Ok(offset)
    }

    pub async fn pull_file(
        &self,
        file_obj: FileObject,
        pull_mode: StoreMode,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    ) -> NdnResult<()> {
        let content_obj_id = ObjId::new(file_obj.content.as_str())?;
        if content_obj_id.is_chunk() {
            let chunk_id = ChunkId::new(file_obj.content.as_str())?;
            self.pull_chunk(chunk_id, pull_mode).await?;
            return Ok(());
        }

        if content_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
            self.pull_chunklist(&content_obj_id, pull_mode, progress_callback)
                .await?;
            return Ok(());
        }

        Err(NdnError::Unsupported(format!(
            "unsupported file content obj type: {}",
            content_obj_id.obj_type
        )))
    }

    pub async fn pull_dir(
        &self,
        _ndn_mgr_id: Option<&str>,
        dir_obj: DirObject,
        pull_mode: StoreMode,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    ) -> NdnResult<()> {
        for (sub_name, sub_item) in dir_obj.iter() {
            let sub_item_type = sub_item.get_obj_type();
            match sub_item_type.as_str() {
                OBJ_TYPE_DIR => {
                    let sub_dir_obj = sub_item.get_obj().map_err(|_| {
                        NdnError::Unsupported(format!(
                            "pull_dir for ObjId-only directory entry is unavailable after refactor: {}",
                            sub_name
                        ))
                    })?;
                    let sub_dir: DirObject = serde_json::from_value(sub_dir_obj).map_err(|e| {
                        NdnError::InvalidData(format!("Failed to parse DirObject: {}", e))
                    })?;
                    let sub_pull_mode = pull_mode.gen_sub_store_mode(sub_name);
                    Box::pin(self.pull_dir(
                        None,
                        sub_dir,
                        sub_pull_mode,
                        progress_callback.clone(),
                    ))
                    .await?;
                }
                OBJ_TYPE_FILE => {
                    let sub_file_obj = sub_item.get_obj().map_err(|_| {
                        NdnError::Unsupported(format!(
                            "pull_dir for ObjId-only file entry is unavailable after refactor: {}",
                            sub_name
                        ))
                    })?;
                    let sub_file: FileObject =
                        serde_json::from_value(sub_file_obj).map_err(|e| {
                            NdnError::InvalidData(format!("Failed to parse FileObject: {}", e))
                        })?;
                    let sub_pull_mode = pull_mode.gen_sub_store_mode(sub_name);
                    self.pull_file(sub_file, sub_pull_mode, progress_callback.clone())
                        .await?;
                }
                _ => {
                    warn!("pull_dir: unknown sub item type: {}", sub_item_type);
                }
            }
        }

        Ok(())
    }

    pub async fn download_fileobj(
        &self,
        fileobj_url: &str,
        local_path: &PathBuf,
        _no_verify: Option<bool>,
    ) -> NdnResult<(ObjId, FileObject)> {
        let (obj_id, file_obj_json) = self.get_obj_by_url(fileobj_url, None).await?;
        let file_obj: FileObject = serde_json::from_value(file_obj_json.clone())
            .map_err(|e| NdnError::InvalidData(format!("Failed to parse FileObject: {}", e)))?;

        self.pull_file(
            file_obj.clone(),
            StoreMode::LocalFile(local_path.clone(), 0..file_obj.size, false),
            None,
        )
        .await?;

        let local_file_obj_path = local_path.with_extension("fileobj");
        tokio::fs::write(local_file_obj_path, serde_json::to_string(&file_obj_json).unwrap())
            .await?;

        Ok((obj_id, file_obj))
    }

    pub async fn remote_is_better(&self, url: &str, local_path: &PathBuf) -> NdnResult<bool> {
        if !local_path.exists() {
            return Ok(true);
        }

        let mut file = tokio::fs::File::open(local_path).await?;
        let file_size = file.metadata().await?.len();

        let (_obj_id, file_obj_json) = self.get_obj_by_url(url, None).await?;
        let file_obj: FileObject = serde_json::from_value(file_obj_json)
            .map_err(|e| NdnError::InvalidData(format!("Failed to parse FileObject: {}", e)))?;

        if file_size != file_obj.size {
            return Ok(true);
        }

        let hasher = ChunkHasher::new(None)?;
        let hash_method = hasher.hash_method;
        let (hash_result, _) = hasher.calc_from_reader(&mut file).await?;
        let local_chunk_id =
            ChunkId::from_mix_hash_result_by_hash_method(file_size, &hash_result, hash_method)?;

        let remote_chunk_id = ChunkId::new(file_obj.content.as_str()).map_err(|e| {
            NdnError::InvalidData(format!("Failed to parse remote file content chunk id: {}", e))
        })?;

        Ok(local_chunk_id != remote_chunk_id)
    }
}
