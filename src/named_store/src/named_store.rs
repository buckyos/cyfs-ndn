use crate::backend::{ChunkWriteOutcome, NamedDataStoreBackend};
use crate::gc_types::*;
use crate::local_fs_backend::{LocalFsBackend, LocalFsBackendConfig};
use crate::lru_hot_table::LruHotTable;
use crate::store_db::{ChunkItem, ChunkLocalInfo, ChunkStoreState, NamedLocalStoreDB};
use log::{debug, warn};
use ndn_lib::{
    caculate_qcid_from_file, ChunkId, ChunkReader, CHUNK_DEFAULT_SIZE, NdnError, NdnResult,
    ObjId,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const CONFIG_FILE_NAME: &str = "named_store.json";
const DEFAULT_DB_FILE: &str = "named_store.db";
const CHUNK_DIR_NAME: &str = "chunks";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedLocalConfig {
    pub read_only: bool,
    pub db_path: Option<PathBuf>,
    pub chunk_dir: Option<PathBuf>,
}

impl Default for NamedLocalConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            db_path: None,
            chunk_dir: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ObjectState {
    NotExist,
    Object(String),
}

/// Default relatime threshold: 60 seconds between DB flushes for the same key.
const LRU_RELATIME_THRESHOLD_SECS: u64 = 60;

/// 通用 Named Store：使用 `NamedDataStoreBackend` 进行 chunk I/O，
/// 使用 `NamedLocalStoreDB` 管理元数据（chunk 状态、GC、pin、outbox 等）。
///
/// 对象存储在 DB 中（兼容已有行为），chunk 数据委托给 backend。
#[derive(Clone)]
pub struct NamedStore {
    base_dir: PathBuf,
    read_only: bool,
    store_id: String,
    db: Arc<NamedLocalStoreDB>,
    backend: Arc<dyn NamedDataStoreBackend>,
    lru_hot: Arc<LruHotTable>,
}

impl NamedStore {
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub fn backend(&self) -> &Arc<dyn NamedDataStoreBackend> {
        &self.backend
    }

    /// 从路径创建 NamedStore，自动使用 LocalFsBackend。
    pub async fn get_named_store_by_path(root_path: PathBuf) -> NdnResult<NamedStore> {
        if !root_path.exists() {
            debug!(
                "NamedStore: create base dir:{}",
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
            let mut file = tokio::fs::File::create(mgr_json_file.clone())
                .await
                .map_err(|e| NdnError::IoError(format!("create config failed: {}", e)))?;
            file.write_all(mgr_json_str.as_bytes())
                .await
                .map_err(|e| NdnError::IoError(format!("write config failed: {}", e)))?;
            config
        } else {
            let mgr_json_str = fs::read_to_string(&mgr_json_file).await.map_err(|e| {
                warn!("NamedStore: read mgr config failed! {}", e);
                NdnError::NotFound("named store config not found".to_string())
            })?;
            serde_json::from_str::<NamedLocalConfig>(&mgr_json_str).map_err(|e| {
                warn!("NamedStore: parse mgr config failed! {}", e);
                NdnError::InvalidData("named store config invalid".to_string())
            })?
        };

        Self::from_config(Some(mgr_id), root_path, mgr_config).await
    }

    /// 从配置创建 NamedStore，自动使用 LocalFsBackend。
    pub async fn from_config(
        store_id: Option<String>,
        root_path: PathBuf,
        config: NamedLocalConfig,
    ) -> NdnResult<Self> {
        let read_only = config.read_only;
        let chunk_dir = config
            .chunk_dir
            .clone()
            .unwrap_or_else(|| root_path.join(CHUNK_DIR_NAME));

        // 创建 LocalFsBackend，root 设为 chunk_dir 的父目录
        let backend_root = chunk_dir
            .parent()
            .unwrap_or(&root_path)
            .to_path_buf();
        let backend = Arc::new(
            LocalFsBackend::new(LocalFsBackendConfig {
                root: backend_root,
                read_only,
            })
            .await?,
        ) as Arc<dyn NamedDataStoreBackend>;

        Self::from_config_with_backend(store_id, root_path, config, backend).await
    }

    /// 使用自定义 backend 创建 NamedStore。
    pub async fn from_config_with_backend(
        store_id: Option<String>,
        root_path: PathBuf,
        config: NamedLocalConfig,
        backend: Arc<dyn NamedDataStoreBackend>,
    ) -> NdnResult<Self> {
        let read_only = config.read_only;
        let db_path = config
            .db_path
            .clone()
            .unwrap_or_else(|| root_path.join(DEFAULT_DB_FILE));

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

        let store = NamedStore {
            base_dir: root_path,
            read_only,
            store_id,
            db,
            backend,
            lru_hot: Arc::new(LruHotTable::new(LRU_RELATIME_THRESHOLD_SECS)),
        };

        Ok(store)
    }

    pub fn get_base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    // ================================================================
    // Object Operations (via DB)
    // ================================================================

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

    pub async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let (_obj_type, obj_str) = self.db.get_object(obj_id).map_err(|e| {
            match &e {
                NdnError::NotFound(_) => e,
                // rusqlite's QueryReturnedNoRows arrives as DbError — treat as NotFound.
                NdnError::DbError(msg) if msg.contains("no rows") => {
                    NdnError::NotFound(obj_id.to_string())
                }
                _ => e,
            }
        })?;

        Ok(obj_str)
    }

    pub async fn put_object(&self, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        self.ensure_writable()?;
        self.db
            .set_object(obj_id, obj_id.obj_type.as_str(), obj_data)
    }

    pub async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        self.ensure_writable()?;
        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            self.remove_chunk(&chunk_id).await
        } else {
            self.db.remove_object(obj_id)
        }
    }

    // ================================================================
    // Chunk Operations (via Backend + DB metadata)
    // ================================================================

    async fn get_chunk_item(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        self.db.get_chunk_item(chunk_id)
    }

    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> bool {
        let query_result = self.query_chunk_state(chunk_id).await;
        if let Ok((chunk_state, _chunk_size)) = query_result {
            chunk_state.can_open_reader()
        } else {
            false
        }
    }

    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkStoreState, u64)> {
        let chunk_item = self.get_chunk_item(chunk_id).await;
        if let Ok(chunk_item) = chunk_item {
            Ok((chunk_item.chunk_state, chunk_item.chunk_size))
        } else {
            Ok((ChunkStoreState::NotExist, 0))
        }
    }

    pub async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let chunk_item = self.get_chunk_item(chunk_id).await?;
        match chunk_item.chunk_state {
            ChunkStoreState::Completed => {
                // 委托给 backend
                self.backend.open_chunk_reader(chunk_id, offset).await
            }
            ChunkStoreState::LocalLink(ref local_info) => {
                // LocalLink: 直接读取外部文件
                self.open_local_link_reader(chunk_id, &chunk_item, local_info, offset)
                    .await
            }
            _ => Err(NdnError::Internal(format!(
                "chunk {} state not support open reader! state:{}",
                chunk_id.to_string(),
                chunk_item.chunk_state.to_str()
            ))),
        }
    }

    /// 打开 LocalLink 引用的外部文件读取器。
    async fn open_local_link_reader(
        &self,
        chunk_id: &ChunkId,
        chunk_item: &ChunkItem,
        local_info: &ChunkLocalInfo,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        self.verify_local_link(chunk_id, local_info).await?;

        let chunk_real_path = PathBuf::from(&local_info.path);
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
        let mut file = tokio::fs::File::open(&chunk_real_path).await.map_err(|e| {
            warn!("open_local_link_reader: open file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        if real_offset > 0 {
            file.seek(SeekFrom::Start(real_offset))
                .await
                .map_err(|e| {
                    warn!(
                        "open_local_link_reader: seek file failed! {}",
                        e.to_string()
                    );
                    NdnError::IoError(e.to_string())
                })?;
        }
        let limited = file.take(limit_len - offset);
        Ok((Box::pin(limited), chunk_item.chunk_size))
    }

    pub async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        self.ensure_writable()?;

        // 委托给 backend 删除文件
        self.backend.remove_chunk(chunk_id).await?;

        // 清理 DB 元数据
        self.db.remove_chunk(chunk_id)
    }

    pub async fn add_chunk_by_link_to_local_file(
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
        let (mut chunk_reader, chunk_size) = self.open_chunk_reader(chunk_id, 0).await?;
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
        let (mut reader, chunk_size) = self.open_chunk_reader(chunk_id, 0).await?;
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

    /// 原子写入 chunk：一次性从 reader 读取全部数据，backend 负责 hash 校验和原子提交。
    ///
    /// chunk_size 不能超过 CHUNK_DEFAULT_SIZE（32MB），大文件必须使用 SameAs ChunkList 模式。
    pub async fn put_chunk_by_reader(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        reader: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
        self.ensure_writable()?;
        if chunk_size > CHUNK_DEFAULT_SIZE {
            return Err(NdnError::InvalidParam(format!(
                "chunk {} size {} exceeds CHUNK_DEFAULT_SIZE ({}), use SameAs ChunkList for large data",
                chunk_id.to_string(),
                chunk_size,
                CHUNK_DEFAULT_SIZE
            )));
        }

        let outcome = self
            .backend
            .open_chunk_writer(chunk_id, chunk_size, reader)
            .await?;

        if outcome == ChunkWriteOutcome::Written {
            let chunk_item = ChunkItem::new_completed(chunk_id, chunk_size);
            self.db.set_chunk_item(&chunk_item)?;
        }

        Ok(outcome)
    }

    /// 原子写入 chunk 字节数据，backend 自动进行 hash 校验。
    ///
    /// chunk_data 长度不能超过 CHUNK_DEFAULT_SIZE（32MB），大文件必须使用 SameAs ChunkList 模式。
    pub async fn put_chunk(&self, chunk_id: &ChunkId, chunk_data: &[u8]) -> NdnResult<()> {
        let chunk_size = chunk_data.len() as u64;
        let reader: ChunkReader = Box::pin(std::io::Cursor::new(chunk_data.to_vec()));
        self.put_chunk_by_reader(chunk_id, chunk_size, reader).await?;
        Ok(())
    }

    /// 物化大 chunk 的 SameAs 关系：big_chunk_id 的内容等价于 chunk_list_id 指向的 ChunkList 拼接。
    ///
    /// 写入 state='present', owned_bytes=0, logical_size=big_chunk_size, SameAs(chunk_list_id)。
    /// 后续 GC 的 parse_obj_refs(big_chunk) 会返回 [chunk_list_id]。
    pub async fn add_chunk_by_same_as(
        &self,
        big_chunk_id: &ChunkId,
        big_chunk_size: u64,
        chunk_list_id: &ObjId,
    ) -> NdnResult<()> {
        self.ensure_writable()?;

        let chunk_item = ChunkItem {
            chunk_id: big_chunk_id.clone(),
            chunk_size: big_chunk_size,
            chunk_state: ChunkStoreState::SameAs(chunk_list_id.clone()),
            create_time: current_unix_ts(),
            update_time: current_unix_ts(),
        };
        self.db.set_chunk_item(&chunk_item)?;
        Ok(())
    }

    // ================================================================
    // GC Public API
    // ================================================================

    pub async fn pin(
        &self,
        obj_id: &ObjId,
        owner: &str,
        scope: PinScope,
        ttl: Option<Duration>,
    ) -> NdnResult<()> {
        self.db.pin(obj_id, owner, scope, ttl)
    }

    pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()> {
        self.db.unpin(obj_id, owner)
    }

    pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize> {
        self.db.unpin_owner(owner)
    }

    pub async fn fs_acquire(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        self.db.fs_acquire(obj_id, inode_id, field_tag)
    }

    pub async fn fs_release(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        self.db.fs_release(obj_id, inode_id, field_tag)
    }

    pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize> {
        self.db.fs_release_inode(inode_id)
    }

    pub async fn apply_edge(&self, msg: EdgeMsg) -> NdnResult<()> {
        self.db.apply_edge(&msg)
    }

    pub fn touch(&self, obj_id: &ObjId) {
        self.lru_hot.touch(&obj_id.to_string());
    }

    pub async fn flush_lru(&self) -> NdnResult<()> {
        let batch = self.lru_hot.collect_dirty_batch();
        if !batch.is_empty() {
            self.db.batch_touch_last_access_with_max(&batch)?;
        }
        Ok(())
    }

    pub async fn run_background_gc(&self) -> NdnResult<GcReport> {
        self.flush_lru().await?;
        let expired = self.db.expire_pins().unwrap_or(0);
        if expired > 0 {
            debug!("GC: expired {} pins", expired);
        }
        self.gc_round(0).await
    }

    pub async fn gc_round(&self, target_bytes: u64) -> NdnResult<GcReport> {
        let mut report = GcReport::default();
        let batch_size = 100;

        loop {
            let candidates = self.db.list_gc_candidates(batch_size)?;
            if candidates.is_empty() {
                break;
            }

            for (obj_id_str, _owned_bytes) in &candidates {
                let freed = self.db.try_evict_object(obj_id_str)?;
                if freed > 0 {
                    if let Ok(obj_id) = ObjId::new(obj_id_str) {
                        if obj_id.is_chunk() {
                            let chunk_id = ChunkId::from_obj_id(&obj_id);
                            let _ = self.backend.remove_chunk(&chunk_id).await;
                            report.evicted_chunks += 1;
                        } else {
                            report.evicted_objects += 1;
                        }
                    }
                    report.freed_bytes += freed;
                } else {
                    report.skipped_protected += 1;
                }

                if target_bytes > 0 && report.freed_bytes >= target_bytes {
                    return Ok(report);
                }
            }

            if target_bytes == 0 {
                break;
            }
        }

        Ok(report)
    }

    pub async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64> {
        let report = self.gc_round(target_bytes).await?;
        if report.freed_bytes >= target_bytes {
            Ok(report.freed_bytes)
        } else {
            Err(NdnError::IoError(format!(
                "ENOSPC: freed {} bytes but needed {}; no class-0 owned bytes left to evict",
                report.freed_bytes, target_bytes
            )))
        }
    }

    pub async fn await_cascade_idle(&self) -> NdnResult<()> {
        loop {
            let count = self.db.outbox_count()?;
            if count == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug> {
        self.db.debug_dump_expand_state(obj_id)
    }

    pub async fn anchor_state(
        &self,
        obj_id: &ObjId,
        owner: &str,
    ) -> NdnResult<CascadeStateP0> {
        self.db.anchor_state(obj_id, owner)
    }

    pub async fn fs_anchor_state(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<CascadeStateP0> {
        self.db.fs_anchor_state(obj_id, inode_id, field_tag)
    }

    pub async fn fetch_outbox_ready(&self, limit: usize) -> NdnResult<Vec<OutboxEntry>> {
        self.db.fetch_outbox_ready(limit)
    }

    pub async fn complete_outbox_entry(&self, seq: i64) -> NdnResult<()> {
        self.db.complete_outbox_entry(seq)
    }

    pub async fn retry_outbox_entry(&self, seq: i64) -> NdnResult<()> {
        self.db.retry_outbox_entry(seq)
    }

    pub async fn outbox_count(&self) -> NdnResult<u64> {
        self.db.outbox_count()
    }

    // ================================================================
    // Internal Helpers
    // ================================================================

    fn ensure_writable(&self) -> NdnResult<()> {
        if self.read_only {
            Err(NdnError::PermissionDenied(
                "named store is read-only".to_string(),
            ))
        } else {
            Ok(())
        }
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
}

/// 向后兼容的类型别名。
pub type NamedLocalStore = NamedStore;

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
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = b"hello named store".to_vec();
        let chunk_id = calc_chunk_id(&data);

        store.put_chunk(&chunk_id, &data).await.unwrap();

        let (mut reader, size) = store.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(size, data.len() as u64);
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_local_link_qcid_ok() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
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
            .add_chunk_by_link_to_local_file(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let (mut reader, size) = store.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(size, data.len() as u64);
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_local_link_qcid_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data = vec![0xAAu8; MIN_QCID_FILE_SIZE as usize + 2048];
        let file_path = temp_dir.path().join("external.bin");
        fs::write(&file_path, &data).await.unwrap();

        let chunk_id = calc_chunk_id(&data);
        let qcid = caculate_qcid_from_file(&file_path).await.unwrap();

        let new_data = vec![0xBBu8; data.len()];
        fs::write(&file_path, &new_data).await.unwrap();

        let info = ChunkLocalInfo {
            path: file_path.to_string_lossy().to_string(),
            qcid: qcid.to_string(),
            last_modify_time: 0,
            range: None,
        };

        store
            .add_chunk_by_link_to_local_file(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let err = store
            .open_chunk_reader(&chunk_id, 0)
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
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
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
            .add_chunk_by_link_to_local_file(&chunk_id, data.len() as u64, &info)
            .await
            .unwrap();

        let err = store
            .open_chunk_reader(&chunk_id, 0)
            .await
            .err()
            .expect("expected invalid link error");
        match err {
            NdnError::InvalidLink(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_put_chunk_by_reader() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let data_size = 8 * 1024 * 1024 + 123;
        let mut data = vec![0u8; data_size];
        for (idx, byte) in data.iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }

        let chunk_id = calc_chunk_id(&data);
        let chunk_size = data.len() as u64;

        let reader: ndn_lib::ChunkReader = Box::pin(std::io::Cursor::new(data.clone()));
        store
            .put_chunk_by_reader(&chunk_id, chunk_size, reader)
            .await
            .unwrap();

        let (mut reader, size) = store.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(size, chunk_size);
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_reject_oversized_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let store = NamedStore::get_named_store_by_path(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // 尝试写入超过 CHUNK_DEFAULT_SIZE 的 chunk，应该被拒绝
        let oversize = CHUNK_DEFAULT_SIZE + 1;
        let chunk_id = calc_chunk_id(&[0u8; 1]); // dummy id
        let reader: ndn_lib::ChunkReader = Box::pin(std::io::Cursor::new(vec![0u8; 1]));
        let err = store
            .put_chunk_by_reader(&chunk_id, oversize, reader)
            .await
            .unwrap_err();
        assert!(matches!(err, NdnError::InvalidParam(_)));
    }
}
