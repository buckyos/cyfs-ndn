use std::collections::HashMap;
use std::fs::File as StdFile;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::FileExt as StdFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as StdFileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fs2::FileExt;
use log::warn;
use ndn_lib::{
    ChunkHasher, ChunkId, FileObject, NdnError, NdnResult, ObjId, SimpleChunkList,
    CHUNK_DEFAULT_SIZE,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf, SeekFrom};

use crate::buffer_db::LocalFileBufferDB;
use crate::fb_service::{
    FileBufferSeekReader, FileBufferSeekWriter, FileBufferService, NdmPath, WriteLease,
};

/// buffer 服务：单机 mmap / 多 BufferNode(GFS 模型) 都可落到这里

static HANDLE_SEQ: AtomicU64 = AtomicU64::new(1);

const BUFFER_DIR_NAME: &str = "buffers";
const BUFFER_DB_FILE: &str = "file_buffer.db";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DirtyChunkLayout {
    pub order: Vec<u32>,
    #[serde(skip)]
    index: HashMap<u32, u32>,
}

fn write_at_once(file: &StdFile, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    {
        StdFileExt::write_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        StdFileExt::seek_write(file, buf, offset)
    }
}

fn write_at_all(file: &StdFile, mut offset: u64, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = write_at_once(file, buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_at returned zero",
            ));
        }
        offset += n as u64;
        buf = &buf[n..];
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct ChunkPlan {
    index: u64,
    offset: u64,
    max_len: usize,
}

fn plan_chunk(pos: u64, remaining: usize, chunk_size: u64) -> ChunkPlan {
    let chunk_size = chunk_size.max(1);
    let index = pos / chunk_size;
    let offset = pos % chunk_size;
    let max_len = std::cmp::min(remaining as u64, chunk_size - offset) as usize;
    ChunkPlan {
        index,
        offset,
        max_len,
    }
}

fn start_seek_at<T: AsyncSeek>(target: Pin<&mut T>, pos: u64) -> std::io::Result<()> {
    target.start_seek(SeekFrom::Start(pos))
}

fn poll_complete_seek<T: AsyncSeek>(
    target: Pin<&mut T>,
    cx: &mut Context<'_>,
) -> Poll<std::io::Result<u64>> {
    target.poll_complete(cx)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum FileBufferBaseReader {
    None,
    BaseChunkList(Vec<ChunkId>),
    //FsNode(u64), // fs_meta里的NodeId,先不支持
}

impl Default for FileBufferBaseReader {
    fn default() -> Self {
        FileBufferBaseReader::None
    }
}

/// Serializable part of FileBufferRecord for persistence
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileBufferRecordMeta {
    pub handle_id: String,
    pub file_inode_id: u64,
    pub base_reader: FileBufferBaseReader,
    pub read_only: bool,
}

pub struct FileBufferRecord {
    pub handle_id: String,
    pub file_inode_id: u64,
    pub base_reader: FileBufferBaseReader,
    pub read_only: bool,
    //state: BufferStage,
    pub dirty_layout: Arc<RwLock<DirtyChunkLayout>>,
}

impl FileBufferRecord {
    /// Convert to serializable meta for persistence
    pub fn to_meta(&self) -> FileBufferRecordMeta {
        FileBufferRecordMeta {
            handle_id: self.handle_id.clone(),
            file_inode_id: self.file_inode_id,
            base_reader: self.base_reader.clone(),
            read_only: self.read_only,
        }
    }

    /// Create from meta with empty dirty layout
    pub fn from_meta(meta: FileBufferRecordMeta) -> Self {
        Self {
            handle_id: meta.handle_id,
            file_inode_id: meta.file_inode_id,
            base_reader: meta.base_reader,
            read_only: meta.read_only,
            dirty_layout: Arc::new(RwLock::new(DirtyChunkLayout::default())),
        }
    }

    /// Create from meta with specified dirty layout
    pub fn from_meta_with_layout(meta: FileBufferRecordMeta, order: Vec<u32>) -> Self {
        Self {
            handle_id: meta.handle_id,
            file_inode_id: meta.file_inode_id,
            base_reader: meta.base_reader,
            read_only: meta.read_only,
            dirty_layout: Arc::new(RwLock::new(DirtyChunkLayout::new(order))),
        }
    }
}

impl Default for DirtyChunkLayout {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            index: HashMap::new(),
        }
    }
}

impl DirtyChunkLayout {
    fn new(order: Vec<u32>) -> Self {
        let mut layout = Self {
            order,
            index: HashMap::new(),
        };
        layout.rebuild_index();
        layout
    }

    fn rebuild_index(&mut self) {
        self.index.clear();
        for (slot, chunk_id) in self.order.iter().enumerate() {
            self.index.insert(*chunk_id, slot as u32);
        }
    }

    fn slot_of(&self, chunk_id: u32) -> Option<u32> {
        self.index.get(&chunk_id).copied()
    }

    fn ensure_slot(&mut self, chunk_id: u32) -> (u32, bool) {
        if let Some(slot) = self.index.get(&chunk_id) {
            return (*slot, false);
        }
        let slot = self.order.len() as u32;
        self.order.push(chunk_id);
        self.index.insert(chunk_id, slot);
        (slot, true)
    }
}

pub struct LocalFileBufferService {
    // 管理本地 buffer 的数据结构
    base_dir: PathBuf,
    buffer_dir: PathBuf,
    size_limit: u64,
    size_used: RwLock<u64>,
    db: Arc<LocalFileBufferDB>,
    // In-memory index: handle_id -> FileBufferRecord
    records: RwLock<HashMap<String, Arc<RwLock<FileBufferRecord>>>>,
}

impl LocalFileBufferService {
    pub fn new(base_dir: PathBuf, size_limit: u64) -> Self {
        let buffer_dir = base_dir.join(BUFFER_DIR_NAME);
        let db_path = base_dir.join(BUFFER_DB_FILE);
        let db = Arc::new(LocalFileBufferDB::new(db_path).unwrap());

        // Load existing records from database into memory index
        let records = match db.load_all() {
            Ok(loaded) => {
                let mut map = HashMap::new();
                for record in loaded {
                    let handle_id = record.handle_id.clone();
                    map.insert(handle_id, Arc::new(RwLock::new(record)));
                }
                RwLock::new(map)
            }
            Err(e) => {
                warn!(
                    "LocalFileBufferService: failed to load records from db: {}",
                    e
                );
                RwLock::new(HashMap::new())
            }
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

    /// Get a record from the in-memory index
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

    /// Insert a record into the in-memory index
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

    /// Remove a record from the in-memory index
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

    fn buffer_path(&self, fb: &FileBufferRecord) -> PathBuf {
        self.buffer_path_by_id(&fb.handle_id)
    }

    fn persist_layout_async(&self, handle_id: String, order: Vec<u32>) {
        let db = self.db.clone();
        let handle_id_log = handle_id.clone();
        tokio::spawn(async move {
            let result =
                tokio::task::spawn_blocking(move || db.set_dirty_order(&handle_id, &order)).await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(
                    "LocalFileBuffer: persist layout failed for {}: {}",
                    handle_id_log, err
                ),
                Err(err) => warn!(
                    "LocalFileBuffer: persist layout join failed for {}: {}",
                    handle_id_log, err
                ),
            }
        });
    }

    async fn load_layout_if_needed(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        let need_load = fb
            .dirty_layout
            .read()
            .map(|layout| layout.order.is_empty())
            .map_err(|_| NdnError::InvalidState("dirty layout poisoned".to_string()))?;
        if !need_load {
            return Ok(());
        }

        let handle_id = fb.handle_id.clone();
        let db = self.db.clone();
        let order_opt = tokio::task::spawn_blocking(move || db.get_dirty_order(&handle_id))
            .await
            .map_err(|e| NdnError::IoError(format!("load layout join error: {}", e)))??;
        if let Some(order) = order_opt {
            let mut layout = fb
                .dirty_layout
                .write()
                .map_err(|_| NdnError::InvalidState("dirty layout poisoned".to_string()))?;
            if layout.order.is_empty() {
                *layout = DirtyChunkLayout::new(order);
            }
        }
        Ok(())
    }

    async fn open_exclusive_rw(&self, path: &Path, create: bool) -> NdnResult<File> {
        let path = path.to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let std_file = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            let mut opts = std::fs::OpenOptions::new();
            opts.read(true).write(true);
            if create {
                opts.create(true);
            }
            let file = opts.open(&path)?;
            match file.try_lock_exclusive() {
                Ok(()) => Ok(file),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Err(
                    std::io::Error::new(err.kind(), "file buffer already locked"),
                ),
                Err(err) => Err(err),
            }
        })
        .await
        .map_err(|e| NdnError::IoError(format!("open writer join error: {}", e)))??;

        Ok(File::from_std(std_file))
    }

    fn ensure_writable(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        if fb.read_only {
            return Err(NdnError::PermissionDenied(
                "file buffer is read-only".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl FileBufferService for LocalFileBufferService {
    //这个函数通常由fs-meta service调用
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
            dirty_layout: Arc::new(RwLock::new(DirtyChunkLayout::default())),
        };

        // Add to database
        let db = self.db.clone();
        let record_for_db = record.to_meta();
        let dirty_order: Vec<u32> = Vec::new();
        tokio::task::spawn_blocking(move || {
            let temp_record = FileBufferRecord::from_meta_with_layout(record_for_db, dirty_order);
            db.add_buffer(&temp_record)
        })
        .await
        .map_err(|e| NdnError::IoError(format!("add buffer join error: {}", e)))??;

        // Add to in-memory index
        self.insert_record(record)?;

        // Return a copy from the index
        let arc_record = self.get_record(&handle_id)?;
        let result = {
            let guard = arc_record
                .read()
                .map_err(|_| NdnError::InvalidState("record lock poisoned".to_string()))?;
            FileBufferRecord {
                handle_id: guard.handle_id.clone(),
                file_inode_id: guard.file_inode_id,
                base_reader: guard.base_reader.clone(),
                read_only: guard.read_only,
                dirty_layout: guard.dirty_layout.clone(),
            }
        };
        Ok(result)
    }

    async fn get_buffer(&self, handle_id: &str) -> NdnResult<FileBufferRecord> {
        // First try in-memory index
        if let Ok(arc_record) = self.get_record(handle_id) {
            let guard = arc_record
                .read()
                .map_err(|_| NdnError::InvalidState("record lock poisoned".to_string()))?;
            return Ok(FileBufferRecord {
                handle_id: guard.handle_id.clone(),
                file_inode_id: guard.file_inode_id,
                base_reader: guard.base_reader.clone(),
                read_only: guard.read_only,
                dirty_layout: guard.dirty_layout.clone(),
            });
        }

        // Fall back to database
        let db = self.db.clone();
        let handle_id_owned = handle_id.to_string();
        let record = tokio::task::spawn_blocking(move || db.get_buffer(&handle_id_owned))
            .await
            .map_err(|e| NdnError::IoError(format!("get buffer join error: {}", e)))??;

        // Insert into memory index
        self.insert_record(record)?;

        // Return from index
        let arc_record = self.get_record(handle_id)?;
        let guard = arc_record
            .read()
            .map_err(|_| NdnError::InvalidState("record lock poisoned".to_string()))?;
        Ok(FileBufferRecord {
            handle_id: guard.handle_id.clone(),
            file_inode_id: guard.file_inode_id,
            base_reader: guard.base_reader.clone(),
            read_only: guard.read_only,
            dirty_layout: guard.dirty_layout.clone(),
        })
    }

    async fn open_reader(
        &self,
        fb: &FileBufferRecord,
        seek_from: SeekFrom,
    ) -> NdnResult<FileBufferSeekReader> {
        self.load_layout_if_needed(fb).await?;
        if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            return Err(NdnError::Unsupported(
                "BaseChunkList overlay not implemented".to_string(),
            ));
        }
        //打开BaseReader
        //打开本地文件，如果有BaseReader,那么本地文件是一个“差异Chunk文件",只包含修改过的Chunk
        //根据BaseReader和本地文件，构造LocalFileBufferSeekReader

        let file_path = self.buffer_path(fb);
        let mut file = OpenOptions::new().read(true).open(file_path).await?;
        let pos = file.seek(seek_from).await?;
        let reader = LocalFileBufferSeekReader::new(
            file,
            None,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
        );
        Ok(Box::pin(reader))
    }

    async fn open_writer(
        &self,
        fb: &FileBufferRecord,
        seek_from: SeekFrom,
    ) -> NdnResult<FileBufferSeekWriter> {
        self.load_layout_if_needed(fb).await?;
        if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            return Err(NdnError::Unsupported(
                "BaseChunkList overlay not implemented".to_string(),
            ));
        }
        // 伪代码：
        // 打开BaseReader
        // 用只读模式打开本地文件，如果有BaseReader,那么本地文件是一个“空洞文件"
        // 根据BaseReader和本地文件，构造LocalFileBufferSeekWriter
        self.ensure_writable(fb)?;

        let file_path = self.buffer_path(fb);
        let mut file = self.open_exclusive_rw(&file_path, true).await?;
        let pos = file.seek(seek_from).await?;
        let std_clone = file.try_clone().await?;
        let inner_std = std_clone.into_std().await;
        let base_reader: Option<FileBufferSeekReader> = None;
        let compact_layout = base_reader.is_some();
        let writer = LocalFileBufferSeekWriter::new(
            file,
            inner_std,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
            fb.handle_id.clone(),
            self.db.clone(),
            compact_layout,
            base_reader,
        );
        Ok(Box::pin(writer))
    }

    async fn flush(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        // 伪代码：
        // 如果没有BaseReader，直接 fsync 本地文件
        // 如果有BaseReader，刷新 dirty chunk 到磁盘，并更新必要的元数据
        let file_path = self.buffer_path(fb);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .await?;
        file.sync_all().await?;
        if let Ok(layout) = fb.dirty_layout.read() {
            self.persist_layout_async(fb.handle_id.clone(), layout.order.clone());
        }
        Ok(())
    }

    async fn close(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        self.flush(fb).await?;
        let order = fb
            .dirty_layout
            .read()
            .map(|layout| layout.order.clone())
            .map_err(|_| NdnError::InvalidState("dirty layout poisoned".to_string()))?;
        let handle_id = fb.handle_id.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.set_dirty_order(&handle_id, &order))
            .await
            .map_err(|e| NdnError::IoError(format!("persist layout join error: {}", e)))??;
        Ok(())
    }

    async fn append(&self, fb: &FileBufferRecord, data: &[u8]) -> NdnResult<()> {
        self.ensure_writable(fb)?;
        if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            return Err(NdnError::Unsupported(
                "BaseChunkList overlay not implemented".to_string(),
            ));
        }
        let file_path = self.buffer_path(fb);
        let mut file = self.open_exclusive_rw(&file_path, true).await?;
        let pos = file.seek(SeekFrom::End(0)).await?;
        file.write_all(data).await?;
        if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            let mut layout = fb
                .dirty_layout
                .write()
                .map_err(|_| NdnError::InvalidState("dirty layout poisoned".to_string()))?;
            let mut new_added = false;
            let chunk_size = CHUNK_DEFAULT_SIZE.max(1);
            let end = pos.saturating_add(data.len() as u64).saturating_sub(1);
            let start_idx = pos / chunk_size;
            let end_idx = end / chunk_size;
            for idx in start_idx..=end_idx {
                let (_slot, is_new) = layout.ensure_slot(idx as u32);
                new_added |= is_new;
            }
            if new_added {
                let order = layout.order.clone();
                drop(layout);
                self.persist_layout_async(fb.handle_id.clone(), order);
            }
        }
        Ok(())
    }

    async fn cacl_name(&self, _fb: &FileBufferRecord) -> NdnResult<ObjId> {
        Err(NdnError::Unsupported(
            "cacl_name not implemented".to_string(),
        ))
    }

    // /// Finalize：把数据从 buffer node 推到 NamedStore internal（IO 密集型）
    // async fn move_to_store(&self, fb: &FileBufferHandle, store: &dyn NamedStore) -> NdnResult<()> {
    //     // 伪代码：
    //     // - 读取本地文件 / mmap 内存
    //     // - 写入 NamedStore internal
    //     let file_path = self.buffer_path(fb);
    //     let name_hint = fb.handle_id.as_str();
    //     let calc = self.calc_file_object(&file_path, name_hint).await?;

    //     let mut file = File::open(&file_path).await?;
    //     let mut offset = 0u64;
    //     for chunk_id in calc.chunk_ids.iter() {
    //         let chunk_size = chunk_id
    //             .get_length()
    //             .ok_or_else(|| NdnError::InvalidParam("chunk size missing".to_string()))?;
    //         file.seek(SeekFrom::Start(offset)).await?;
    //         {
    //             let mut limited = (&mut file).take(chunk_size);
    //             store
    //                 .put_chunk_by_reader(chunk_id, chunk_size, &mut limited)
    //                 .await?;
    //         }
    //         offset += chunk_size;
    //     }

    //     if let Some((chunk_list_id, chunk_list_str)) = calc.chunk_list {
    //         store
    //             .put_object(&chunk_list_id, chunk_list_str.as_bytes())
    //             .await?;
    //     }

    //     store
    //         .put_object(&calc.file_obj_id, calc.file_obj_str.as_bytes())
    //         .await?;
    //     Ok(())
    // }

    async fn remove(&self, fb: &FileBufferRecord) -> NdnResult<()> {
        // Remove from in-memory index first
        self.remove_record(&fb.handle_id)?;

        // Delete the local buffer file
        let file_path = self.buffer_path(fb);
        if let Ok(meta) = fs::metadata(&file_path).await {
            let mut used = self.size_used.write().unwrap();
            *used = used.saturating_sub(meta.len());
        }
        // Ignore file not found error
        if let Err(e) = fs::remove_file(&file_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(format!("remove file failed: {}", e)));
            }
        }

        // Remove from database
        let handle_id = fb.handle_id.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.remove(&handle_id))
            .await
            .map_err(|e| NdnError::IoError(format!("remove db entry join error: {}", e)))??;
        Ok(())
    }
}

// 基于 FileBufferHandle 实现 LocalFileBufferSeekReader,LocalFileBufferSeekWriter

pub struct LocalFileBufferSeekReader {
    inner: File,
    base_reader: Option<FileBufferSeekReader>,
    dirty_layout: Arc<RwLock<DirtyChunkLayout>>,
    chunk_size: u64,
    pos: u64,
    inner_pos: u64,
    base_pos: u64,
    read_state: ReadState,
}

impl LocalFileBufferSeekReader {
    fn new(
        inner: File,
        base_reader: Option<FileBufferSeekReader>,
        dirty_layout: Arc<RwLock<DirtyChunkLayout>>,
        chunk_size: u64,
        pos: u64,
    ) -> Self {
        Self {
            inner,
            base_reader,
            dirty_layout,
            chunk_size,
            pos,
            inner_pos: pos,
            base_pos: pos,
            read_state: ReadState::Idle,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReaderTarget {
    Inner,
    Base,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadState {
    Idle,
    Syncing(ReaderTarget),
}

#[derive(Clone, Copy, Debug)]
struct ReadPlan {
    target: ReaderTarget,
    desired_pos: u64,
    max_len: usize,
}

impl LocalFileBufferSeekReader {
    fn target_pos(&self, target: ReaderTarget) -> u64 {
        match target {
            ReaderTarget::Inner => self.inner_pos,
            ReaderTarget::Base => self.base_pos,
        }
    }

    fn set_target_pos(&mut self, target: ReaderTarget, pos: u64) {
        match target {
            ReaderTarget::Inner => self.inner_pos = pos,
            ReaderTarget::Base => self.base_pos = pos,
        }
    }

    fn base_reader_mut(&mut self) -> std::io::Result<&mut FileBufferSeekReader> {
        self.base_reader
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "base_reader missing"))
    }

    fn start_seek_target(&mut self, target: ReaderTarget, pos: u64) -> std::io::Result<()> {
        match target {
            ReaderTarget::Inner => start_seek_at(Pin::new(&mut self.inner), pos),
            ReaderTarget::Base => {
                let base = self.base_reader_mut()?;
                start_seek_at(Pin::new(base), pos)
            }
        }
    }

    fn poll_complete_target(
        &mut self,
        target: ReaderTarget,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        match target {
            ReaderTarget::Inner => poll_complete_seek(Pin::new(&mut self.inner), cx),
            ReaderTarget::Base => {
                let base = match self.base_reader_mut() {
                    Ok(base) => base,
                    Err(err) => return Poll::Ready(Err(err)),
                };
                poll_complete_seek(Pin::new(base), cx)
            }
        }
    }

    fn poll_read_target(
        &mut self,
        target: ReaderTarget,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match target {
            ReaderTarget::Inner => Pin::new(&mut self.inner).poll_read(cx, buf),
            ReaderTarget::Base => {
                let base = match self.base_reader_mut() {
                    Ok(base) => base,
                    Err(err) => return Poll::Ready(Err(err)),
                };
                Pin::new(base).poll_read(cx, buf)
            }
        }
    }

    fn read_plan(&self, remaining: usize) -> std::io::Result<ReadPlan> {
        let chunk_size = self.chunk_size.max(1);
        let chunk = plan_chunk(self.pos, remaining, chunk_size);
        let layout = self
            .dirty_layout
            .read()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "dirty layout poisoned"))?;
        if let Some(slot) = layout.slot_of(chunk.index as u32) {
            let local_offset = slot as u64 * chunk_size + chunk.offset;
            Ok(ReadPlan {
                target: ReaderTarget::Inner,
                desired_pos: local_offset,
                max_len: chunk.max_len,
            })
        } else {
            Ok(ReadPlan {
                target: ReaderTarget::Base,
                desired_pos: self.pos,
                max_len: chunk.max_len,
            })
        }
    }
}

impl AsyncRead for LocalFileBufferSeekReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.read_state {
                ReadState::Syncing(target) => {
                    let poll = this.poll_complete_target(target, cx);
                    match poll {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(pos)) => {
                            this.set_target_pos(target, pos);
                            this.read_state = ReadState::Idle;
                            continue;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    }
                }
                ReadState::Idle => {}
            }

            if this.base_reader.is_none() {
                let before = buf.filled().len();
                let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
                if let Poll::Ready(Ok(())) = &poll {
                    let read = buf.filled().len() - before;
                    this.pos += read as u64;
                    this.inner_pos = this.pos;
                }
                return poll;
            }

            let remaining = buf.remaining();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let plan = match this.read_plan(remaining) {
                Ok(plan) => plan,
                Err(err) => return Poll::Ready(Err(err)),
            };

            if this.target_pos(plan.target) != plan.desired_pos {
                if let Err(err) = this.start_seek_target(plan.target, plan.desired_pos) {
                    return Poll::Ready(Err(err));
                }
                this.read_state = ReadState::Syncing(plan.target);
                continue;
            }

            let mut temp = vec![0u8; plan.max_len];
            let mut temp_buf = ReadBuf::new(&mut temp);

            let poll = this.poll_read_target(plan.target, cx, &mut temp_buf);
            match poll {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let read = temp_buf.filled().len();
                    if read == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    buf.put_slice(&temp[..read]);
                    this.pos += read as u64;
                    match plan.target {
                        ReaderTarget::Inner => {
                            this.inner_pos = plan.desired_pos + read as u64;
                        }
                        ReaderTarget::Base => {
                            this.base_pos = this.pos;
                        }
                    }
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }
    }
}

impl AsyncSeek for LocalFileBufferSeekReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        this.read_state = ReadState::Idle;
        if this.base_reader.is_none() {
            Pin::new(&mut this.inner).start_seek(position)?;
        } else if let Some(base) = this.base_reader.as_mut() {
            Pin::new(base).start_seek(position)?;
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        if this.base_reader.is_none() {
            let inner_poll = poll_complete_seek(Pin::new(&mut this.inner), cx);
            match inner_poll {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(pos)) => {
                    this.pos = pos;
                    this.inner_pos = pos;
                    this.read_state = ReadState::Idle;
                    Poll::Ready(Ok(pos))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            }
        } else {
            let base_poll = if let Some(base) = this.base_reader.as_mut() {
                poll_complete_seek(Pin::new(base), cx)
            } else {
                Poll::Ready(Ok(this.pos))
            };
            match base_poll {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(pos)) => {
                    this.pos = pos;
                    this.base_pos = pos;
                    this.inner_pos = u64::MAX;
                    this.read_state = ReadState::Idle;
                    Poll::Ready(Ok(pos))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            }
        }
    }
}

pub struct LocalFileBufferSeekWriter {
    inner: File,
    inner_std: Arc<StdFile>,
    dirty_layout: Arc<RwLock<DirtyChunkLayout>>,
    base_reader: Option<FileBufferSeekReader>,
    chunk_size: u64,
    pos: u64,
    inner_pos: u64,
    handle_id: String,
    db: Arc<LocalFileBufferDB>,
    compact_layout: bool,
    prefill: Option<PrefillState>,
    write_state: WriteState,
}

impl LocalFileBufferSeekWriter {
    fn new(
        inner: File,
        inner_std: StdFile,
        dirty_layout: Arc<RwLock<DirtyChunkLayout>>,
        chunk_size: u64,
        pos: u64,
        handle_id: String,
        db: Arc<LocalFileBufferDB>,
        compact_layout: bool,
        base_reader: Option<FileBufferSeekReader>,
    ) -> Self {
        Self {
            inner,
            inner_std: Arc::new(inner_std),
            dirty_layout,
            base_reader,
            chunk_size,
            pos,
            inner_pos: pos,
            handle_id,
            db,
            compact_layout,
            prefill: None,
            write_state: WriteState::Idle,
        }
    }

    fn ensure_slots_and_persist(
        &self,
        start: u64,
        len: usize,
    ) -> std::io::Result<Vec<(u64, u32, bool)>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let chunk_size = self.chunk_size.max(1);
        let end = start.saturating_add(len as u64).saturating_sub(1);
        let start_idx = start / chunk_size;
        let end_idx = end / chunk_size;
        if end_idx > u32::MAX as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "dirty chunk index overflow",
            ));
        }

        let mut new_added = false;
        let mut slots = Vec::new();
        let mut layout = self
            .dirty_layout
            .write()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "dirty layout poisoned"))?;
        for idx in start_idx..=end_idx {
            let (slot, is_new) = layout.ensure_slot(idx as u32);
            new_added |= is_new;
            slots.push((idx as u64, slot, is_new));
        }
        if new_added {
            let order = layout.order.clone();
            let db = self.db.clone();
            let handle_id = self.handle_id.clone();
            let handle_id_log = handle_id.clone();
            tokio::spawn(async move {
                let result =
                    tokio::task::spawn_blocking(move || db.set_dirty_order(&handle_id, &order))
                        .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => warn!(
                        "LocalFileBuffer: persist layout failed for {}: {}",
                        handle_id_log, err
                    ),
                    Err(err) => warn!(
                        "LocalFileBuffer: persist layout join failed for {}: {}",
                        handle_id_log, err
                    ),
                }
            });
        }
        Ok(slots)
    }

    fn drive_prefill(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let Some(state) = &mut self.prefill else {
            return Poll::Ready(Ok(()));
        };
        let Some(base_reader) = self.base_reader.as_mut() else {
            self.prefill = None;
            return Poll::Ready(Ok(()));
        };

        loop {
            match state.phase {
                PrefillPhase::SeekBase => {
                    if !state.base_seek_pending {
                        if let Err(err) = Pin::new(&mut *base_reader)
                            .start_seek(SeekFrom::Start(state.chunk_start))
                        {
                            return Poll::Ready(Err(err));
                        }
                        state.base_seek_pending = true;
                    }
                    match Pin::new(&mut *base_reader).poll_complete(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(_)) => {
                            state.base_seek_pending = false;
                            state.phase = PrefillPhase::ReadBase;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    }
                }
                PrefillPhase::ReadBase => {
                    let remaining = state.buf.len().saturating_sub(state.read_len);
                    if remaining == 0 || state.eof {
                        state.phase = PrefillPhase::WriteInner;
                        continue;
                    }

                    let mut read_buf = ReadBuf::new(&mut state.buf[state.read_len..]);
                    let before = read_buf.filled().len();
                    match Pin::new(&mut *base_reader).poll_read(cx, &mut read_buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {
                            let read = read_buf.filled().len().saturating_sub(before);
                            if read == 0 {
                                state.eof = true;
                            } else {
                                state.read_len += read;
                            }
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    }
                }
                PrefillPhase::WriteInner => {
                    if state.write_handle.is_none() {
                        let buf = std::mem::take(&mut state.buf);
                        let slot_start = state.slot_start;
                        let write_len = buf.len();
                        let file = self.inner_std.clone();
                        state.write_len = write_len;
                        state.write_handle = Some(tokio::task::spawn_blocking(move || {
                            write_at_all(&file, slot_start, &buf)
                        }));
                    }

                    if let Some(handle) = state.write_handle.as_mut() {
                        match Pin::new(handle).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(join_result) => {
                                state.write_handle = None;
                                match join_result {
                                    Ok(Ok(())) => {
                                        self.inner_pos = state.slot_start + state.write_len as u64;
                                        self.prefill = None;
                                        return Poll::Ready(Ok(()));
                                    }
                                    Ok(Err(err)) => return Poll::Ready(Err(err)),
                                    Err(err) => {
                                        return Poll::Ready(Err(std::io::Error::new(
                                            std::io::ErrorKind::Other,
                                            format!("prefill join error: {}", err),
                                        )))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn poll_direct_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
        pending_pos: u64,
        max_len: Option<usize>,
        track_dirty: bool,
    ) -> Poll<std::io::Result<usize>> {
        let target_buf = if let Some(max_len) = max_len {
            &buf[..max_len]
        } else {
            buf
        };

        match Pin::new(&mut self.inner).poll_write(cx, target_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(n)) => {
                if track_dirty && self.base_reader.is_some() {
                    if let Err(err) = self.ensure_slots_and_persist(self.pos, n) {
                        return Poll::Ready(Err(err));
                    }
                }
                self.pos += n as u64;
                self.inner_pos = pending_pos + n as u64;
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    fn poll_pending_pwrite(
        &mut self,
        cx: &mut Context<'_>,
        pending: &mut PendingPwrite,
    ) -> Poll<std::io::Result<usize>> {
        let desired_pos = pending.desired_pos;
        match Pin::new(&mut pending.handle).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(join_result) => {
                let n = match join_result {
                    Ok(Ok(n)) => n,
                    Ok(Err(err)) => return Poll::Ready(Err(err)),
                    Err(err) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("pwrite join error: {}", err),
                        )))
                    }
                };
                self.pos += n as u64;
                self.inner_pos = desired_pos + n as u64;
                Poll::Ready(Ok(n))
            }
        }
    }
}

struct PrefillState {
    chunk_start: u64,
    slot_start: u64,
    buf: Vec<u8>,
    read_len: usize,
    eof: bool,
    phase: PrefillPhase,
    base_seek_pending: bool,
    write_handle: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    write_len: usize,
}

enum PrefillPhase {
    SeekBase,
    ReadBase,
    WriteInner,
}

#[derive(Debug)]
struct PendingPwrite {
    desired_pos: u64,
    handle: tokio::task::JoinHandle<std::io::Result<usize>>,
}

#[derive(Debug)]
enum WriteState {
    Idle,
    PendingDirect {
        pending_pos: u64,
        max_len: Option<usize>,
    },
    PendingPwrite(PendingPwrite),
}

impl AsyncWrite for LocalFileBufferSeekWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match std::mem::replace(&mut this.write_state, WriteState::Idle) {
            WriteState::PendingDirect {
                pending_pos,
                max_len,
            } => {
                let track_dirty = !this.compact_layout && this.base_reader.is_some();
                let poll = this.poll_direct_write(cx, buf, pending_pos, max_len, track_dirty);
                match poll {
                    Poll::Pending => {
                        this.write_state = WriteState::PendingDirect {
                            pending_pos,
                            max_len,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => return Poll::Ready(result),
                }
            }
            WriteState::PendingPwrite(mut pending) => {
                let poll = this.poll_pending_pwrite(cx, &mut pending);
                match poll {
                    Poll::Pending => {
                        this.write_state = WriteState::PendingPwrite(pending);
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => return Poll::Ready(result),
                }
            }
            WriteState::Idle => {}
        }

        match this.drive_prefill(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
        }

        if !this.compact_layout {
            let pending_pos = this.inner_pos;
            let track_dirty = this.base_reader.is_some();
            let poll = this.poll_direct_write(cx, buf, pending_pos, None, track_dirty);
            match poll {
                Poll::Pending => {
                    this.write_state = WriteState::PendingDirect {
                        pending_pos,
                        max_len: None,
                    };
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            }
        } else {
            let chunk_size = this.chunk_size.max(1);
            let chunk = plan_chunk(this.pos, buf.len(), chunk_size);
            let target_buf = &buf[..chunk.max_len];

            let slots = match this.ensure_slots_and_persist(this.pos, chunk.max_len) {
                Ok(slots) => slots,
                Err(err) => return Poll::Ready(Err(err)),
            };
            let current_chunk = chunk.index as u32;
            let slot = slots
                .iter()
                .find(|(idx, _, _)| *idx == current_chunk as u64)
                .map(|(_, slot, is_new)| (*slot, *is_new))
                .unwrap_or((0, false));
            let (slot, is_new) = slot;
            let desired_pos = slot as u64 * chunk_size + chunk.offset;

            if is_new {
                if let Some(_base) = this.base_reader.as_ref() {
                    this.prefill = Some(PrefillState {
                        chunk_start: current_chunk as u64 * chunk_size,
                        slot_start: slot as u64 * chunk_size,
                        buf: vec![0u8; chunk_size as usize],
                        read_len: 0,
                        eof: false,
                        phase: PrefillPhase::SeekBase,
                        base_seek_pending: false,
                        write_handle: None,
                        write_len: 0,
                    });
                    match this.drive_prefill(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    }
                }
            }

            let data = target_buf.to_vec();
            let file = this.inner_std.clone();
            let handle =
                tokio::task::spawn_blocking(move || write_at_once(&file, &data, desired_pos));
            let mut pending = PendingPwrite {
                desired_pos,
                handle,
            };
            let poll = this.poll_pending_pwrite(cx, &mut pending);
            match poll {
                Poll::Pending => {
                    this.write_state = WriteState::PendingPwrite(pending);
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl AsyncSeek for LocalFileBufferSeekWriter {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().inner).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        match poll_complete_seek(Pin::new(&mut this.inner), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(pos)) => {
                this.pos = pos;
                this.inner_pos = pos;
                Poll::Ready(Ok(pos))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
    use tokio::time::{sleep, Duration};

    fn dummy_path() -> NdmPath {
        NdmPath("ndm://test/path".to_string())
    }

    fn dummy_lease() -> WriteLease {
        WriteLease {
            session: crate::fb_service::SessionId("s1".to_string()),
            session_seq: 1,
            expires_at: 0,
        }
    }

    async fn write_filled(file: &mut File, value: u8, mut len: u64) -> std::io::Result<()> {
        const BUF_SIZE: usize = 1024 * 1024;
        let buf = vec![value; BUF_SIZE];
        while len > 0 {
            let write_len = std::cmp::min(len, BUF_SIZE as u64) as usize;
            file.write_all(&buf[..write_len]).await?;
            len -= write_len as u64;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_basic_write_read() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(&dummy_path(), 1, vec![], &dummy_lease(), None)
            .await
            .unwrap();

        let mut writer = service.open_writer(&fb, SeekFrom::Start(0)).await.unwrap();
        writer.write_all(b"hello").await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        let mut reader = service.open_reader(&fb, SeekFrom::Start(0)).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello");
    }

    #[tokio::test]
    async fn test_dirty_layout_persisted() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(&dummy_path(), 1, vec![], &dummy_lease(), None)
            .await
            .unwrap();

        // build a base reader with at least one full chunk
        let base_path = dir.path().join("base.bin");
        {
            let mut base_file = tokio::fs::File::create(&base_path).await.unwrap();
            base_file
                .write_all(&vec![0x5Au8; CHUNK_DEFAULT_SIZE as usize])
                .await
                .unwrap();
        }
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());

        let file_path = service.buffer_path(&fb);
        let mut file = service.open_exclusive_rw(&file_path, true).await.unwrap();
        let pos = file.seek(SeekFrom::Start(0)).await.unwrap();
        let std_clone = file.try_clone().await.unwrap();
        let inner_std = std_clone.into_std().await;
        let mut writer = LocalFileBufferSeekWriter::new(
            file,
            inner_std,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
            fb.handle_id.clone(),
            service.db.clone(),
            true,
            Some(base_reader),
        );
        let data = vec![0u8; (CHUNK_DEFAULT_SIZE as usize) + 10];
        tokio::io::AsyncWriteExt::write_all(&mut writer, &data)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut writer).await.unwrap();

        // wait for async persist
        for _ in 0..20u32 {
            if let Ok(Some(order)) = service.db.get_dirty_order(&fb.handle_id) {
                assert!(!order.is_empty());
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }

        panic!("dirty layout not persisted");
    }

    #[tokio::test]
    async fn test_compact_layout_write_read() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let mut fb = service
            .alloc_buffer(&dummy_path(), 1, vec![], &dummy_lease(), None)
            .await
            .unwrap();
        fb.base_reader = FileBufferBaseReader::BaseChunkList(vec![]);

        let base_path = dir.path().join("base2.bin");
        {
            let mut base_file = tokio::fs::File::create(&base_path).await.unwrap();
            base_file
                .write_all(&vec![b'x'; CHUNK_DEFAULT_SIZE as usize])
                .await
                .unwrap();
        }
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());

        let file_path = service.buffer_path(&fb);
        let mut file = service.open_exclusive_rw(&file_path, true).await.unwrap();
        let pos = file.seek(SeekFrom::Start(0)).await.unwrap();
        let std_clone = file.try_clone().await.unwrap();
        let inner_std = std_clone.into_std().await;
        let mut writer = LocalFileBufferSeekWriter::new(
            file,
            inner_std,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
            fb.handle_id.clone(),
            service.db.clone(),
            true,
            Some(base_reader),
        );
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"abc")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut writer).await.unwrap();

        // reopen reader and ensure content exists (BaseChunkList overlay not supported in service)
        let inner_file = tokio::fs::File::open(&file_path).await.unwrap();
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());
        let mut reader = LocalFileBufferSeekReader::new(
            inner_file,
            Some(base_reader),
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            0,
        );
        let mut buf = [0u8; 3];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abc");

        // ensure local file is compact (<= chunk_size)
        let file_path = service.buffer_path(&fb);
        let meta = tokio::fs::metadata(&file_path).await.unwrap();
        assert!(meta.len() <= CHUNK_DEFAULT_SIZE);
    }

    #[tokio::test]
    async fn test_dirty_then_clean_read_across_chunks() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let mut fb = service
            .alloc_buffer(&dummy_path(), 1, vec![], &dummy_lease(), None)
            .await
            .unwrap();
        fb.base_reader = FileBufferBaseReader::BaseChunkList(vec![]);

        let base_path = dir.path().join("base_dirty_clean.bin");
        {
            let mut base_file = tokio::fs::File::create(&base_path).await.unwrap();
            write_filled(&mut base_file, 0x00, CHUNK_DEFAULT_SIZE)
                .await
                .unwrap();
            write_filled(&mut base_file, 0x11, CHUNK_DEFAULT_SIZE)
                .await
                .unwrap();
            base_file.flush().await.unwrap();
        }
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());

        let file_path = service.buffer_path(&fb);
        let mut file = service.open_exclusive_rw(&file_path, true).await.unwrap();
        let pos = file.seek(SeekFrom::Start(0)).await.unwrap();
        let std_clone = file.try_clone().await.unwrap();
        let inner_std = std_clone.into_std().await;
        let mut writer = LocalFileBufferSeekWriter::new(
            file,
            inner_std,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
            fb.handle_id.clone(),
            service.db.clone(),
            true,
            Some(base_reader),
        );
        writer.seek(SeekFrom::Start(10)).await.unwrap();
        writer.write_all(b"DIRTY").await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        let inner_file = tokio::fs::File::open(&file_path).await.unwrap();
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());
        let mut reader = LocalFileBufferSeekReader::new(
            inner_file,
            Some(base_reader),
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            0,
        );

        reader
            .seek(SeekFrom::Start(CHUNK_DEFAULT_SIZE - 4))
            .await
            .unwrap();
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..4], &[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(&buf[4..], &[0x11, 0x11, 0x11, 0x11]);
    }

    //一个复杂的测试
    // 构造一个 16MB * 16 - 256的文件，有16个块，每个块的值填充当前编号
    // 基于该文件作为BaseReader，创建一个 LocalFileBufferSeekWriter，进行如下写入操作：
    // - 在块 5 的中间位置写入 "HELLO"
    // - 在块 2 的开头写入 "WORLD"
    // - 在尾部写入512字节的新数据（全部都是0xff)
    // 创建Reader，验证上面3个写入都可以正确读取
    #[tokio::test]
    async fn test_complex_overlay_write_read() {
        let dir = tempdir().unwrap();
        let dir_path = dir.into_path(); // 目录会保留
        print!("temp dir path: {:?}\n", dir_path);
        let service = LocalFileBufferService::new(dir_path.clone(), 0);
        let mut fb = service
            .alloc_buffer(&dummy_path(), 1, vec![], &dummy_lease(), None)
            .await
            .unwrap();
        fb.base_reader = FileBufferBaseReader::BaseChunkList(vec![]);

        let base_path = dir_path.join("base_large.bin");
        {
            let mut base_file = tokio::fs::File::create(&base_path).await.unwrap();
            for idx in 0..16u64 {
                let chunk_len = if idx == 15 {
                    CHUNK_DEFAULT_SIZE - 256
                } else {
                    CHUNK_DEFAULT_SIZE
                };
                write_filled(&mut base_file, idx as u8, chunk_len)
                    .await
                    .unwrap();
            }
            base_file.flush().await.unwrap();
        }
        let base_size = CHUNK_DEFAULT_SIZE * 16 - 256;

        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());
        let file_path = service.buffer_path(&fb);
        let mut file = service.open_exclusive_rw(&file_path, true).await.unwrap();
        let pos = file.seek(SeekFrom::Start(0)).await.unwrap();
        let std_clone = file.try_clone().await.unwrap();
        let inner_std = std_clone.into_std().await;
        let mut writer = LocalFileBufferSeekWriter::new(
            file,
            inner_std,
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            pos,
            fb.handle_id.clone(),
            service.db.clone(),
            true,
            Some(base_reader),
        );

        let chunk_middle = CHUNK_DEFAULT_SIZE / 2;
        writer
            .seek(SeekFrom::Start(5 * CHUNK_DEFAULT_SIZE + chunk_middle))
            .await
            .unwrap();
        writer.write_all(b"HELLO").await.unwrap();

        writer
            .seek(SeekFrom::Start(2 * CHUNK_DEFAULT_SIZE))
            .await
            .unwrap();
        writer.write_all(b"WORLD").await.unwrap();

        writer.seek(SeekFrom::Start(base_size)).await.unwrap();
        writer.write_all(&vec![0xffu8; 512]).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        println!("writer completed!");

        let inner_file = tokio::fs::File::open(&file_path).await.unwrap();
        let base_reader: FileBufferSeekReader =
            Box::pin(tokio::fs::File::open(&base_path).await.unwrap());
        let mut reader = LocalFileBufferSeekReader::new(
            inner_file,
            Some(base_reader),
            fb.dirty_layout.clone(),
            CHUNK_DEFAULT_SIZE,
            0,
        );
        println!("reader opened!");

        reader.seek(SeekFrom::Start(0)).await.unwrap();
        let mut buf = vec![0u8; 256];
        reader.read_exact(&mut buf).await.unwrap();
        assert!(buf.iter().all(|b| *b == 0));

        reader
            .seek(SeekFrom::Start(2 * CHUNK_DEFAULT_SIZE))
            .await
            .unwrap();
        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"WORLD");
        println!("read WORLD ok");

        reader
            .seek(SeekFrom::Start(5 * CHUNK_DEFAULT_SIZE + chunk_middle))
            .await
            .unwrap();
        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"HELLO");
        println!("read HELLO ok");

        reader.seek(SeekFrom::Start(base_size)).await.unwrap();
        let mut tail = vec![0u8; 512];
        reader.read_exact(&mut tail).await.unwrap();
        assert!(tail.iter().all(|b| *b == 0xff));
        println!("read tail 0xff ok");
    }
}
