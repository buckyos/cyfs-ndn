use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ndn_lib::{ChunkId, NdnError, NdnResult, ObjId};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::buffer_db::LocalFileBufferDB;
use crate::fb_service::{FileBufferService, NdmPath, WriteLease};

static HANDLE_SEQ: AtomicU64 = AtomicU64::new(1);

const BUFFER_DIR_NAME: &str = "buffers";
const BUFFER_DB_FILE: &str = "file_buffer.db";

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FileBufferDiffState {
    pub chunk_indices: Vec<u64>,
    pub diff_chunk_sizes: Vec<u64>,
    pub base_chunk_sizes: Vec<u64>,
    pub merged_chunk_sizes: Vec<u64>,
    pub position: u64,
    pub total_size: u64,
    pub auto_cache: bool,
    pub local_mode: bool,
    pub fixed_chunk_size: Option<u64>,
    pub append_merge_last_chunk: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum FileBufferBaseReader {
    None,
    BaseChunkList(Vec<ChunkId>),
}

impl Default for FileBufferBaseReader {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileBufferRecordMeta {
    pub handle_id: String,
    pub file_inode_id: u64,
    pub base_reader: FileBufferBaseReader,
    pub read_only: bool,
    pub diff_file_path: PathBuf,
    pub diff_state: FileBufferDiffState,
}

pub struct FileBufferRecord {
    pub handle_id: String,
    pub file_inode_id: u64,
    pub base_reader: FileBufferBaseReader,
    pub read_only: bool,
    pub diff_file_path: PathBuf,
    pub diff_state: Arc<RwLock<FileBufferDiffState>>,
}

impl FileBufferRecord {
    pub fn to_meta(&self) -> FileBufferRecordMeta {
        FileBufferRecordMeta {
            handle_id: self.handle_id.clone(),
            file_inode_id: self.file_inode_id,
            base_reader: self.base_reader.clone(),
            read_only: self.read_only,
            diff_file_path: self.diff_file_path.clone(),
            diff_state: self
                .diff_state
                .read()
                .map(|state| state.clone())
                .unwrap_or_default(),
        }
    }

    pub fn from_meta(meta: FileBufferRecordMeta) -> Self {
        Self {
            handle_id: meta.handle_id,
            file_inode_id: meta.file_inode_id,
            base_reader: meta.base_reader,
            read_only: meta.read_only,
            diff_file_path: meta.diff_file_path,
            diff_state: Arc::new(RwLock::new(meta.diff_state)),
        }
    }

    fn clone_ref(&self) -> Self {
        Self {
            handle_id: self.handle_id.clone(),
            file_inode_id: self.file_inode_id,
            base_reader: self.base_reader.clone(),
            read_only: self.read_only,
            diff_file_path: self.diff_file_path.clone(),
            diff_state: self.diff_state.clone(),
        }
    }
}

pub struct LocalFileBufferService {
    base_dir: PathBuf,
    buffer_dir: PathBuf,
    size_limit: u64,
    size_used: RwLock<u64>,
    db: Arc<LocalFileBufferDB>,
    records: RwLock<HashMap<String, Arc<RwLock<FileBufferRecord>>>>,
}

impl LocalFileBufferService {
    pub fn new(base_dir: PathBuf, size_limit: u64) -> Self {
        let buffer_dir = base_dir.join(BUFFER_DIR_NAME);
        let db_path = base_dir.join(BUFFER_DB_FILE);
        let db = Arc::new(LocalFileBufferDB::new(db_path).unwrap());

        let records = match db.load_all() {
            Ok(loaded) => {
                let mut map = HashMap::new();
                for record in loaded {
                    let handle_id = record.handle_id.clone();
                    map.insert(handle_id, Arc::new(RwLock::new(record)));
                }
                RwLock::new(map)
            }
            Err(_) => RwLock::new(HashMap::new()),
        };

        Self {
            base_dir,
            buffer_dir,
            size_limit,
            size_used: RwLock::new(0),
            db,
            records,
        }
    }

    fn get_record(&self, handle_id: &str) -> NdnResult<Arc<RwLock<FileBufferRecord>>> {
        let records = self
            .records
            .read()
            .map_err(|_| NdnError::InvalidState("records index poisoned".to_string()))?;
        records
            .get(handle_id)
            .cloned()
            .ok_or_else(|| NdnError::NotFound(format!("buffer not found: {}", handle_id)))
    }

    fn insert_record(&self, record: FileBufferRecord) -> NdnResult<Arc<RwLock<FileBufferRecord>>> {
        let handle_id = record.handle_id.clone();
        let arc_record = Arc::new(RwLock::new(record));
        let mut records = self
            .records
            .write()
            .map_err(|_| NdnError::InvalidState("records index poisoned".to_string()))?;
        records.insert(handle_id, arc_record.clone());
        Ok(arc_record)
    }

    fn remove_record(&self, handle_id: &str) -> NdnResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| NdnError::InvalidState("records index poisoned".to_string()))?;
        records.remove(handle_id);
        Ok(())
    }

    fn next_handle_id() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let seq = HANDLE_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("fb-{}-{}", ts, seq)
    }

    fn buffer_prefix(handle_id: &str) -> String {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in handle_id.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("{:02x}", hash & 0xff)
    }

    fn buffer_path_by_id(&self, handle_id: &str) -> PathBuf {
        let file_name = format!("{}.buf", handle_id);
        let prefix = Self::buffer_prefix(handle_id);
        self.buffer_dir.join(prefix).join(file_name)
    }

    fn ensure_writable(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        if fb.read_only {
            return Err(NdnError::PermissionDenied(
                "file buffer is read-only".to_string(),
            ));
        }
        Ok(())
    }

    fn snapshot_record(
        &self,
        arc_record: &Arc<RwLock<FileBufferRecord>>,
    ) -> NdnResult<FileBufferRecord> {
        let guard = arc_record
            .read()
            .map_err(|_| NdnError::InvalidState("record lock poisoned".to_string()))?;
        Ok(guard.clone_ref())
    }
}

#[async_trait]
impl FileBufferService for LocalFileBufferService {
    async fn alloc_buffer(
        &self,
        _path: &NdmPath,
        file_inode_id: u64,
        base_chunk_list: Vec<ChunkId>,
        _lease: &WriteLease,
        expected_size: Option<u64>,
    ) -> NdnResult<FileBufferRecord> {
        if self.size_limit > 0 {
            if let Some(expect) = expected_size {
                let mut used = self.size_used.write().unwrap();
                if *used + expect > self.size_limit {
                    return Err(NdnError::InvalidState(
                        "buffer capacity exceeded".to_string(),
                    ));
                }
                *used += expect;
            }
        }

        fs::create_dir_all(&self.base_dir).await?;
        fs::create_dir_all(&self.buffer_dir).await?;

        let handle_id = Self::next_handle_id();
        let file_path = self.buffer_path_by_id(&handle_id);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .await?;

        let base_reader = if base_chunk_list.is_empty() {
            FileBufferBaseReader::None
        } else {
            FileBufferBaseReader::BaseChunkList(base_chunk_list)
        };

        let record = FileBufferRecord {
            handle_id: handle_id.clone(),
            file_inode_id,
            base_reader,
            read_only: false,
            diff_file_path: file_path,
            diff_state: Arc::new(RwLock::new(FileBufferDiffState::default())),
        };

        let db = self.db.clone();
        let record_meta = record.to_meta();
        tokio::task::spawn_blocking(move || {
            let record = FileBufferRecord::from_meta(record_meta);
            db.add_buffer(&record)
        })
        .await
        .map_err(|e| NdnError::IoError(format!("add buffer join error: {}", e)))??;

        self.insert_record(record)?;
        let arc_record = self.get_record(&handle_id)?;
        self.snapshot_record(&arc_record)
    }

    async fn get_buffer(&self, handle_id: &str) -> NdnResult<FileBufferRecord> {
        if let Ok(arc_record) = self.get_record(handle_id) {
            return self.snapshot_record(&arc_record);
        }

        let db = self.db.clone();
        let handle_id_owned = handle_id.to_string();
        let record = tokio::task::spawn_blocking(move || db.get_buffer(&handle_id_owned))
            .await
            .map_err(|e| NdnError::IoError(format!("get buffer join error: {}", e)))??;

        self.insert_record(record)?;
        let arc_record = self.get_record(handle_id)?;
        self.snapshot_record(&arc_record)
    }

    async fn flush(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fb.diff_file_path)
            .await
        {
            Ok(file) => file.sync_all().await?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(NdnError::IoError(format!("open diff file failed: {}", e))),
        }

        let meta = fb.to_meta();
        let handle_id = fb.handle_id.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.set_meta(&handle_id, &meta))
            .await
            .map_err(|e| NdnError::IoError(format!("persist meta join error: {}", e)))??;

        Ok(())
    }

    async fn close(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        self.flush(fb).await
    }

    async fn append(&self, fb: &FileBufferRecord, data: &[u8]) -> NdnResult<()> {
        self.ensure_writable(fb)?;
        if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            return Err(NdnError::Unsupported(
                "append with base chunk list is handled by DiffChunkListWriter".to_string(),
            ));
        }

        if let Some(parent) = fb.diff_file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&fb.diff_file_path)
            .await?;
        file.write_all(data).await?;
        file.sync_data().await?;

        let meta = fs::metadata(&fb.diff_file_path).await?;
        let mut state = fb
            .diff_state
            .write()
            .map_err(|_| NdnError::InvalidState("filebuffer diff_state poisoned".to_string()))?;
        state.total_size = meta.len();
        state.position = state.total_size;

        Ok(())
    }

    async fn cacl_name(&self, _fb: &FileBufferRecord) -> NdnResult<ObjId> {
        Err(NdnError::Unsupported(
            "cacl_name not implemented".to_string(),
        ))
    }

    async fn remove(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        self.remove_record(&fb.handle_id)?;

        if let Ok(meta) = fs::metadata(&fb.diff_file_path).await {
            let mut used = self.size_used.write().unwrap();
            *used = used.saturating_sub(meta.len());
        }

        if let Err(e) = fs::remove_file(&fb.diff_file_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(format!("remove file failed: {}", e)));
            }
        }

        let handle_id = fb.handle_id.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.remove(&handle_id))
            .await
            .map_err(|e| NdnError::IoError(format!("remove db entry join error: {}", e)))??;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_lease() -> WriteLease {
        WriteLease {
            session: crate::SessionId("s1".to_string()),
            session_seq: 1,
            expires_at: 0,
        }
    }

    #[tokio::test]
    async fn test_alloc_get_flush_reload_diff_state() {
        let dir = tempdir().unwrap();
        let base = dir.path().to_path_buf();

        let service = LocalFileBufferService::new(base.clone(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/a.txt".to_string()),
                100,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        {
            let mut state = fb.diff_state.write().unwrap();
            state.chunk_indices = vec![1, 3];
            state.diff_chunk_sizes = vec![128, 256];
            state.base_chunk_sizes = vec![1024, 1024, 1024, 1024];
            state.merged_chunk_sizes = vec![1024, 1024, 1024, 1024];
            state.total_size = 4096;
            state.position = 384;
            state.append_merge_last_chunk = true;
        }

        service.flush(&fb).await.unwrap();

        let reloaded_service = LocalFileBufferService::new(base, 0);
        let loaded = reloaded_service.get_buffer(&fb.handle_id).await.unwrap();
        let loaded_state = loaded.diff_state.read().unwrap().clone();
        assert_eq!(loaded_state.chunk_indices, vec![1, 3]);
        assert_eq!(loaded_state.diff_chunk_sizes, vec![128, 256]);
        assert_eq!(loaded_state.total_size, 4096);
        assert_eq!(loaded_state.position, 384);
        assert!(loaded_state.append_merge_last_chunk);
    }

    #[tokio::test]
    async fn test_append_for_none_base_reader() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/b.txt".to_string()),
                101,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        service.append(&fb, b"hello").await.unwrap();
        service.append(&fb, b" world").await.unwrap();
        service.flush(&fb).await.unwrap();

        let bytes = fs::read(&fb.diff_file_path).await.unwrap();
        assert_eq!(bytes, b"hello world");

        let state = fb.diff_state.read().unwrap().clone();
        assert_eq!(state.total_size, 11);
        assert_eq!(state.position, 11);
    }

    #[tokio::test]
    async fn test_remove_cleans_file_and_db() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/c.txt".to_string()),
                102,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        assert!(fb.diff_file_path.exists());
        service.remove(&fb).await.unwrap();
        assert!(!fb.diff_file_path.exists());
        assert!(service.get_buffer(&fb.handle_id).await.is_err());
    }
}
