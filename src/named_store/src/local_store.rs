use crate::store_db::{ChunkItem, ChunkLocalInfo, ChunkStoreState, NamedLocalStoreDB};
use fs2::FileExt;
use log::{debug, warn};
use ndn_lib::{
    caculate_qcid_from_file, ChunkHasher, ChunkId, ChunkReader, ChunkWriter, NdnError, NdnResult,
    ObjId,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const CONFIG_FILE_NAME: &str = "named_store.json";
const DEFAULT_DB_FILE: &str = "named_store.db";
const CHUNK_DIR_NAME: &str = "chunks";
const CHUNK_FINAL_EXT: &str = "final";
const CHUNK_TMP_EXT: &str = "tmp";
static STORE_REGISTRY: Lazy<Mutex<HashMap<String, Arc<tokio::sync::Mutex<NamedLocalStore>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedLocalConfig {
    pub read_only: bool,
    pub db_path: Option<PathBuf>,
    pub chunk_dir: Option<PathBuf>,
    pub mmap_cache_dir: Option<PathBuf>,
}

impl Default for NamedLocalConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            db_path: None,
            chunk_dir: None,
            mmap_cache_dir: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ObjectState {
    NotExist,
    Object(String),
}

#[derive(Clone)]
pub struct NamedLocalStore {
    base_dir: PathBuf,
    read_only: bool,
    store_id: String,
    db: Arc<NamedLocalStoreDB>,
    chunk_dir: PathBuf,
    mmap_cache_dir: Option<PathBuf>,
}

impl NamedLocalStore {
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub async fn get_named_store_by_path(root_path: PathBuf) -> NdnResult<NamedLocalStore> {
        if !root_path.exists() {
            debug!(
                "NamedLocalStore: create base dir:{}",
                root_path.to_string_lossy()
            );
            fs::create_dir_all(root_path.clone())
                .await
                .map_err(|e| NdnError::IoError(format!("create base dir failed: {}", e)))?;
        }
        let mgr_id = root_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("default")
            .to_string();

        let mgr_json_file = root_path.join(CONFIG_FILE_NAME);
        let mgr_config = if !mgr_json_file.exists() {
            let config = NamedLocalConfig::default();
            let mgr_json_str =
                serde_json::to_string(&config).map_err(|e| NdnError::Internal(e.to_string()))?;
            let mut file = File::create(mgr_json_file.clone())
                .await
                .map_err(|e| NdnError::IoError(format!("create config failed: {}", e)))?;
            file.write_all(mgr_json_str.as_bytes())
                .await
                .map_err(|e| NdnError::IoError(format!("write config failed: {}", e)))?;
            config
        } else {
            let mgr_json_str = fs::read_to_string(&mgr_json_file).await.map_err(|e| {
                warn!("NamedLocalStore: read mgr config failed! {}", e);
                NdnError::NotFound("named store config not found".to_string())
            })?;
            serde_json::from_str::<NamedLocalConfig>(&mgr_json_str).map_err(|e| {
                warn!("NamedLocalStore: parse mgr config failed! {}", e);
                NdnError::InvalidData("named store config invalid".to_string())
            })?
        };

        Self::from_config(Some(mgr_id), root_path, mgr_config).await
    }

    pub async fn is_named_store_exist(named_store_id: Option<&str>) -> bool {
        let registry = STORE_REGISTRY.lock().unwrap();
        match named_store_id {
            Some(id) => registry.contains_key(id),
            None => !registry.is_empty(),
        }
    }

    pub async fn get_named_store_by_id(
        named_store_id: Option<&str>,
    ) -> Option<Arc<tokio::sync::Mutex<Self>>> {
        let registry = STORE_REGISTRY.lock().unwrap();
        named_store_id.and_then(|id| registry.get(id).cloned())
    }

    pub async fn from_config(
        store_id: Option<String>,
        root_path: PathBuf,
        config: NamedLocalConfig,
    ) -> NdnResult<Self> {
        let read_only = config.read_only;
        let db_path = config
            .db_path
            .clone()
            .unwrap_or_else(|| root_path.join(DEFAULT_DB_FILE));
        let chunk_dir = config
            .chunk_dir
            .clone()
            .unwrap_or_else(|| root_path.join(CHUNK_DIR_NAME));

        if !read_only {
            fs::create_dir_all(&chunk_dir)
                .await
                .map_err(|e| NdnError::IoError(format!("create chunk dir failed: {}", e)))?;
        }

        let db = Arc::new(NamedLocalStoreDB::new(
            db_path.to_string_lossy().to_string(),
        )?);

        let store_id = store_id
            .or_else(|| {
                root_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "default".to_string());

        let store = NamedLocalStore {
            base_dir: root_path,
            read_only,
            store_id: store_id.clone(),
            db,
            chunk_dir,
            mmap_cache_dir: config.mmap_cache_dir,
        };

        let mut registry = STORE_REGISTRY.lock().unwrap();
        registry
            .entry(store_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(store.clone())));

        Ok(store)
    }

    pub fn get_base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    pub async fn is_object_exist(&self, obj_id: &ObjId) -> NdnResult<bool> {
        let obj_state = self.query_object_by_id(obj_id).await?;
        Ok(!matches!(obj_state, ObjectState::NotExist))
    }

    pub async fn query_object_by_id(&self, obj_id: &ObjId) -> NdnResult<ObjectState> {
        if let Ok((_obj_type, obj_str)) = self.db.get_object(obj_id) {
            return Ok(ObjectState::Object(obj_str));
        }
        Ok(ObjectState::NotExist)
    }

    pub async fn get_object_impl(
        &self,
        obj_id: &ObjId,
        inner_obj_path: Option<String>,
    ) -> NdnResult<Value> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let (_obj_type, obj_str) = self.db.get_object(obj_id).map_err(|e| {
            if e.is_not_found() {
                NdnError::NotFound(obj_id.to_string())
            } else {
                e
            }
        })?;

        let json_value: Value =
            serde_json::from_str(&obj_str).map_err(|e| NdnError::DecodeError(e.to_string()))?;

        if let Some(inner_path) = inner_obj_path {
            self.extract_json_by_path(&json_value, inner_path.as_str())
        } else {
            Ok(json_value)
        }
    }

    pub async fn put_object_impl(&self, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        self.ensure_writable()?;
        self.db
            .set_object(obj_id, obj_id.obj_type.as_str(), obj_data)
    }

    pub async fn remove_object_impl(&self, obj_id: &ObjId) -> NdnResult<()> {
        self.ensure_writable()?;
        self.db.remove_object(obj_id)
    }

    async fn get_chunk_item_impl(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        self.db.get_chunk_item(chunk_id)
    }

    pub async fn have_chunk_impl(&self, chunk_id: &ChunkId) -> bool {
        let query_result = self.query_chunk_state_impl(chunk_id).await;
        if let Ok((chunk_state, _chunk_size, _progress)) = query_result {
            chunk_state.can_open_reader()
        } else {
            false
        }
    }

    pub async fn query_chunk_state_impl(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkStoreState, u64, String)> {
        let chunk_item = self.get_chunk_item_impl(chunk_id).await;
        if let Ok(chunk_item) = chunk_item {
            Ok((
                chunk_item.chunk_state,
                chunk_item.chunk_size,
                chunk_item.progress,
            ))
        } else {
            Ok((ChunkStoreState::NotExist, 0, String::new()))
        }
    }

    pub async fn open_store_chunk_reader_impl(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let (chunk_state, chunk_size, _progress) = self.query_chunk_state_impl(chunk_id).await?;
        if chunk_state != ChunkStoreState::Completed {
            return Err(NdnError::InComplete(format!(
                "chunk {} state not support open reader! state:{}",
                chunk_id.to_string(),
                chunk_state.to_str()
            )));
        }

        if offset > chunk_size {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }

        let chunk_real_path = self.get_chunk_final_path(chunk_id);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&chunk_real_path)
            .await
            .map_err(|e| {
                warn!("open_chunk_reader: open file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

        if offset > 0 {
            file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                warn!("open_chunk_reader: seek file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
        }

        let limited = file.take(chunk_size - offset);
        Ok((Box::pin(limited), chunk_size))
    }

    pub async fn open_chunk_reader_impl(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        _auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        if let Some(mcache_file_path) = self.get_cache_mmap_path(chunk_id) {
            if let Ok(mut file) = OpenOptions::new().read(true).open(&mcache_file_path).await {
                let file_meta = file
                    .metadata()
                    .await
                    .map_err(|e| NdnError::IoError(format!("read cache metadata failed: {}", e)))?;
                if offset > 0 {
                    file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                        warn!(
                            "get_chunk_reader: seek cache file failed! {}",
                            e.to_string()
                        );
                        NdnError::IoError(e.to_string())
                    })?;
                }
                return Ok((Box::pin(file), file_meta.len()));
            }
        }

        let chunk_item = self.get_chunk_item_impl(chunk_id).await?;
        match chunk_item.chunk_state {
            ChunkStoreState::Completed => {
                let chunk_real_path = self.get_chunk_final_path(&chunk_item.chunk_id);
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&chunk_real_path)
                    .await
                    .map_err(|e| {
                        warn!("open_chunk_reader: open file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                if offset > 0 {
                    if offset > chunk_item.chunk_size {
                        return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
                    }
                    file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                        warn!(
                            "open_chunk_reader: seek chunk file failed! {}",
                            e.to_string()
                        );
                        NdnError::IoError(e.to_string())
                    })?;
                }
                let limited = file.take(chunk_item.chunk_size - offset);
                Ok((Box::pin(limited), chunk_item.chunk_size))
            }
            ChunkStoreState::LocalLink(local_info) => {
                self.verify_local_link(chunk_id, &local_info).await?;

                let chunk_real_path = PathBuf::from(local_info.path);
                let mut real_offset = 0u64;
                let mut limit_len = chunk_item.chunk_size;

                if let Some(range) = local_info.range.clone() {
                    let range_len = range.end.saturating_sub(range.start);
                    if range_len != chunk_item.chunk_size {
                        return Err(NdnError::InvalidLink(format!(
                            "link range mismatch: expected {} got {}",
                            chunk_item.chunk_size, range_len
                        )));
                    }
                    real_offset = range.start;
                    limit_len = range_len;
                }

                if offset > limit_len {
                    return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
                }

                real_offset += offset;
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
                let limited = file.take(limit_len - offset);
                Ok((Box::pin(limited), chunk_item.chunk_size))
            }
            _ => Err(NdnError::Internal(format!(
                "chunk {} state not support open reader! state:{}",
                chunk_id.to_string(),
                chunk_item.chunk_state.to_str()
            ))),
        }
    }

    pub async fn open_chunk_writer_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        offset: u64,
    ) -> NdnResult<(ChunkWriter, String)> {
        self.ensure_writable()?;

        let chunk_item = self.db.get_chunk_item(chunk_id);
        let mut chunk_state = ChunkStoreState::NotExist;

        if let Ok(item) = chunk_item {
            chunk_state = item.chunk_state.clone();
            if chunk_state == ChunkStoreState::Completed {
                return Err(NdnError::AlreadyExists(format!(
                    "chunk {} already completed",
                    chunk_id.to_string()
                )));
            }
            if !chunk_state.can_open_writer() {
                return Err(NdnError::InvalidState(format!(
                    "chunk {} state not support open writer! {}",
                    chunk_id.to_string(),
                    chunk_state.to_str()
                )));
            }
        }

        let chunk_tmp_path = self.get_chunk_tmp_path(chunk_id);
        if let Some(parent) = chunk_tmp_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                warn!("open_chunk_writer: create dir failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
        }

        let file = if chunk_state == ChunkStoreState::Incompleted {
            let file_meta = fs::metadata(&chunk_tmp_path).await.map_err(|e| {
                warn!("open_chunk_writer: get metadata failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            if offset > file_meta.len() {
                warn!(
                    "open_chunk_writer: offset too large! {}",
                    chunk_id.to_string()
                );
                return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
            }

            let mut file = OpenOptions::new()
                .write(true)
                .open(&chunk_tmp_path)
                .await
                .map_err(|e| {
                    warn!("open_chunk_writer: open file failed! {}", e.to_string());
                    NdnError::IoError(e.to_string())
                })?;

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
            file
        } else {
            if offset != 0 {
                warn!("open_chunk_writer: offset not 0! {}", chunk_id.to_string());
                return Err(NdnError::InvalidParam("offset not 0".to_string()));
            }

            let file = File::create(&chunk_tmp_path).await.map_err(|e| {
                warn!("open_chunk_writer: create file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            let std_file = file.into_std().await;
            std_file.try_lock_exclusive().map_err(|e| {
                warn!("open_chunk_writer: lock file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
            tokio::fs::File::from_std(std_file)
        };

        let mut chunk_item = match self.db.get_chunk_item(chunk_id) {
            Ok(item) => item,
            Err(_) => ChunkItem::new_incompleted(chunk_id, chunk_size),
        };
        chunk_item.chunk_state = ChunkStoreState::Incompleted;
        if chunk_item.progress.is_empty() {
            chunk_item.progress = serde_json::json!({ "pos": offset }).to_string();
        }
        self.db.set_chunk_item(&chunk_item)?;
        Ok((Box::pin(file), chunk_item.progress.clone()))
    }

    pub async fn open_new_chunk_writer_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
    ) -> NdnResult<ChunkWriter> {
        self.ensure_writable()?;

        if self.db.get_chunk_item(chunk_id).is_ok() {
            return Err(NdnError::AlreadyExists(format!(
                "chunk {} already exists",
                chunk_id.to_string()
            )));
        }

        let chunk_tmp_path = self.get_chunk_tmp_path(chunk_id);
        if let Some(parent) = chunk_tmp_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                warn!(
                    "open_new_chunk_writer: create dir failed! {}",
                    e.to_string()
                );
                NdnError::IoError(e.to_string())
            })?;
        }

        let file = File::create(&chunk_tmp_path).await.map_err(|e| {
            warn!(
                "open_new_chunk_writer: create file failed! {}",
                e.to_string()
            );
            NdnError::IoError(e.to_string())
        })?;

        let std_file = file.into_std().await;
        std_file.try_lock_exclusive().map_err(|e| {
            warn!("open_new_chunk_writer: lock file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        let file = tokio::fs::File::from_std(std_file);

        let chunk_item = ChunkItem::new_incompleted(chunk_id, chunk_size);
        self.db.set_chunk_item(&chunk_item)?;
        Ok(Box::pin(file))
    }

    pub async fn update_chunk_progress_impl(
        &self,
        chunk_id: &ChunkId,
        progress: String,
    ) -> NdnResult<()> {
        self.db.update_chunk_progress(chunk_id, progress)
    }

    pub async fn complete_chunk_writer_impl(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        self.ensure_writable()?;

        let mut chunk_item = self.db.get_chunk_item(chunk_id)?;
        let tmp_path = self.get_chunk_tmp_path(chunk_id);
        let final_path = self.get_chunk_final_path(chunk_id);

        if !tmp_path.exists() {
            return Err(NdnError::NotFound(format!(
                "chunk tmp file not found: {}",
                tmp_path.to_string_lossy()
            )));
        }

        if final_path.exists() {
            return Err(NdnError::AlreadyExists(format!(
                "chunk final already exists: {}",
                final_path.to_string_lossy()
            )));
        }

        fs::rename(&tmp_path, &final_path).await.map_err(|e| {
            warn!("complete_chunk_writer: rename failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;

        chunk_item.chunk_state = ChunkStoreState::Completed;
        chunk_item.progress = String::new();
        chunk_item.update_time = current_unix_ts();
        self.db.set_chunk_item(&chunk_item)?;
        Ok(())
    }

    pub async fn remove_chunk_impl(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        self.ensure_writable()?;

        let final_path = self.get_chunk_final_path(chunk_id);
        if let Err(err) = fs::remove_file(&final_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(err.to_string()));
            }
        }

        let tmp_path = self.get_chunk_tmp_path(chunk_id);
        if let Err(err) = fs::remove_file(&tmp_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(err.to_string()));
            }
        }

        if let Some(cache_path) = self.get_cache_mmap_path(chunk_id) {
            if let Err(err) = fs::remove_file(&cache_path).await {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(NdnError::IoError(err.to_string()));
                }
            }
        }

        self.db.remove_chunk(chunk_id)
    }

    pub async fn add_chunk_by_link_to_local_file_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        chunk_local_info: &ChunkLocalInfo,
    ) -> NdnResult<()> {
        self.ensure_writable()?;
        if let Some(range) = &chunk_local_info.range {
            let range_len = range.end.saturating_sub(range.start);
            if range_len != chunk_size {
                return Err(NdnError::InvalidParam(format!(
                    "range size mismatch: expected {} got {}",
                    chunk_size, range_len
                )));
            }
        }
        let chunk_item = ChunkItem::new_local_file(chunk_id, chunk_size, chunk_local_info);
        self.db.set_chunk_item(&chunk_item)
    }

    pub async fn get_chunk_data(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
        let (mut chunk_reader, chunk_size) = self.open_store_chunk_reader_impl(chunk_id, 0).await?;
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
        if offset > chunk_size {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        let mut buffer = vec![0u8; piece_size as usize];
        reader.read_exact(&mut buffer).await.map_err(|e| {
            warn!("get_chunk_piece: read file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        Ok(buffer)
    }

    pub async fn put_chunk_by_reader_impl(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        reader: &mut ChunkReader,
    ) -> NdnResult<()> {
        let mut chunk_writer = self
            .open_new_chunk_writer_impl(chunk_id, chunk_size)
            .await?;
        let mut limited = reader.take(chunk_size);
        let copy_bytes = tokio::io::copy(&mut limited, &mut chunk_writer).await?;
        if copy_bytes != chunk_size {
            return Err(NdnError::IoError(format!(
                "copy chunk failed! expected:{} actual:{}",
                chunk_size, copy_bytes
            )));
        }
        self.complete_chunk_writer_impl(chunk_id).await?;
        Ok(())
    }

    pub async fn put_chunk(
        &self,
        chunk_id: &ChunkId,
        chunk_data: &[u8],
        need_verify: bool,
    ) -> NdnResult<()> {
        if need_verify {
            let hash_method = chunk_id.chunk_type.to_hash_method()?;
            let chunk_hasher = ChunkHasher::new_with_hash_method(hash_method)?;
            let hash_bytes = chunk_hasher.calc_from_bytes(chunk_data);
            let verify_id = if chunk_id.chunk_type.is_mix() {
                ChunkId::from_mix_hash_result(
                    chunk_data.len() as u64,
                    &hash_bytes,
                    chunk_id.chunk_type.clone(),
                )
            } else {
                ChunkId::from_hash_result(&hash_bytes, chunk_id.chunk_type.clone())
            };
            if verify_id != *chunk_id {
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

    fn ensure_writable(&self) -> NdnResult<()> {
        if self.read_only {
            Err(NdnError::PermissionDenied(
                "named store is read-only".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn extract_json_by_path(&self, value: &Value, path: &str) -> NdnResult<Value> {
        let mut current = value;
        for segment in path.split('/') {
            if segment.is_empty() {
                continue;
            }
            current = match current {
                Value::Object(map) => map
                    .get(segment)
                    .ok_or_else(|| NdnError::NotFound(format!("inner path not found: {}", path)))?,
                Value::Array(list) => {
                    let index: usize = segment.parse().map_err(|_| {
                        NdnError::InvalidParam(format!("invalid array index: {}", segment))
                    })?;
                    list.get(index).ok_or_else(|| {
                        NdnError::NotFound(format!("inner path not found: {}", path))
                    })?
                }
                _ => {
                    return Err(NdnError::NotFound(format!(
                        "inner path not found: {}",
                        path
                    )))
                }
            };
        }
        Ok(current.clone())
    }

    async fn verify_local_link(&self, chunk_id: &ChunkId, info: &ChunkLocalInfo) -> NdnResult<()> {
        if info.qcid.is_empty() {
            return Err(NdnError::InvalidLink(format!(
                "local link missing qcid for {}",
                chunk_id.to_string()
            )));
        }

        let path = Path::new(&info.path);
        let metadata = fs::metadata(path).await.map_err(|e| {
            warn!("verify_local_link: stat failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;

        if let Some(range) = &info.range {
            let file_len = metadata.len();
            if range.end > file_len {
                return Err(NdnError::InvalidLink(format!(
                    "link range exceeds file length: {}",
                    chunk_id.to_string()
                )));
            }
        }

        let qcid = caculate_qcid_from_file(path).await?;
        if qcid.to_string() != info.qcid {
            return Err(NdnError::VerifyError(format!(
                "qcid mismatch for {}",
                chunk_id.to_string()
            )));
        }

        Ok(())
    }

    fn chunk_file_name(&self, chunk_id: &ChunkId) -> String {
        chunk_id.to_obj_id().to_filename()
    }

    fn get_chunk_final_path(&self, chunk_id: &ChunkId) -> PathBuf {
        let file_name = format!("{}.{}", self.chunk_file_name(chunk_id), CHUNK_FINAL_EXT);
        let prefix = &file_name[0..2.min(file_name.len())];
        self.chunk_dir.join(prefix).join(file_name)
    }

    fn get_chunk_tmp_path(&self, chunk_id: &ChunkId) -> PathBuf {
        let file_name = format!("{}.{}", self.chunk_file_name(chunk_id), CHUNK_TMP_EXT);
        let prefix = &file_name[0..2.min(file_name.len())];
        self.chunk_dir.join(prefix).join(file_name)
    }

    fn get_cache_mmap_path(&self, chunk_id: &ChunkId) -> Option<PathBuf> {
        let mmap_dir = self.mmap_cache_dir.as_ref()?;
        let file_name = self.chunk_file_name(chunk_id);
        Some(mmap_dir.join(file_name))
    }
}

fn current_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndn_lib::{ChunkHasher, MIN_QCID_FILE_SIZE};
    use serde_json::json;
    use tempfile::TempDir;

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    #[tokio::test]
    async fn test_put_and_read_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedLocalStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = b"hello named store".to_vec();
        let chunk_id = calc_chunk_id(&data);

        store.put_chunk(&chunk_id, &data, true).await.unwrap();

        let (mut reader, size) = store
            .open_chunk_reader_impl(&chunk_id, 0, false)
            .await
            .unwrap();
        assert_eq!(size, data.len() as u64);
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_local_link_qcid_ok() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedLocalStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = vec![0x5Au8; MIN_QCID_FILE_SIZE as usize + 1024];
        let file_path = temp_dir.path().join("external.bin");
        fs::write(&file_path, &data).await.unwrap();

        let chunk_id = calc_chunk_id(&data);
        let qcid = caculate_qcid_from_file(&file_path).await.unwrap();
        let meta = fs::metadata(&file_path).await.unwrap();
        let mtime = meta
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let info = ChunkLocalInfo {
            path: file_path.to_string_lossy().to_string(),
            qcid: qcid.to_string(),
            last_modify_time: mtime,
            range: None,
        };

        store
            .add_chunk_by_link_to_local_file_impl(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let (mut reader, size) = store
            .open_chunk_reader_impl(&chunk_id, 0, false)
            .await
            .unwrap();
        assert_eq!(size, data.len() as u64);
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_local_link_qcid_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedLocalStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = vec![0xAAu8; MIN_QCID_FILE_SIZE as usize + 2048];
        let file_path = temp_dir.path().join("external.bin");
        fs::write(&file_path, &data).await.unwrap();

        let chunk_id = calc_chunk_id(&data);
        let qcid = caculate_qcid_from_file(&file_path).await.unwrap();

        // Modify file after qcid is recorded
        let new_data = vec![0xBBu8; data.len()];
        fs::write(&file_path, &new_data).await.unwrap();

        let info = ChunkLocalInfo {
            path: file_path.to_string_lossy().to_string(),
            qcid: qcid.to_string(),
            last_modify_time: 0,
            range: None,
        };

        store
            .add_chunk_by_link_to_local_file_impl(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let err = store
            .open_chunk_reader_impl(&chunk_id, 0, false)
            .await
            .err()
            .expect("expected verify error");
        match err {
            NdnError::VerifyError(_) | NdnError::InvalidLink(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_local_link_missing_qcid() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedLocalStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = vec![0x33u8; MIN_QCID_FILE_SIZE as usize + 1024];
        let file_path = temp_dir.path().join("external.bin");
        fs::write(&file_path, &data).await.unwrap();

        let chunk_id = calc_chunk_id(&data);
        let info = ChunkLocalInfo {
            path: file_path.to_string_lossy().to_string(),
            qcid: String::new(),
            last_modify_time: 0,
            range: None,
        };

        store
            .add_chunk_by_link_to_local_file_impl(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let err = store
            .open_chunk_reader_impl(&chunk_id, 0, false)
            .await
            .err()
            .expect("expected invalid link error");
        match err {
            NdnError::InvalidLink(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_write_large_chunk_stream() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedLocalStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data_size = 8 * 1024 * 1024 + 123;
        let mut data = vec![0u8; data_size];
        for (idx, byte) in data.iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }

        let chunk_id = calc_chunk_id(&data);
        let chunk_size = data.len() as u64;

        let (mut writer, _progress) = store
            .open_chunk_writer_impl(&chunk_id, chunk_size, 0)
            .await
            .unwrap();

        let mut pos = 0u64;
        for chunk in data.chunks(1024 * 1024) {
            writer.write_all(chunk).await.unwrap();
            pos += chunk.len() as u64;
            store
                .update_chunk_progress_impl(&chunk_id, json!({ "pos": pos }).to_string())
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
        store.complete_chunk_writer_impl(&chunk_id).await.unwrap();

        let (mut reader, size) = store
            .open_chunk_reader_impl(&chunk_id, 0, false)
            .await
            .unwrap();
        assert_eq!(size, chunk_size);

        let hasher = ChunkHasher::new(None).unwrap();
        let hash_method = hasher.hash_method.clone();
        let (hash_bytes, read_size) = hasher.calc_from_reader(&mut reader).await.unwrap();
        let read_chunk_id =
            ChunkId::from_mix_hash_result_by_hash_method(read_size, &hash_bytes, hash_method)
                .unwrap();
        assert_eq!(read_chunk_id, chunk_id);
    }
}
