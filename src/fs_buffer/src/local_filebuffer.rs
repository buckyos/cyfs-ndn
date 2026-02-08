use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use named_store::{
    ChunkLocalInfo, DiffChunkListDirtyChunk, DiffChunkListWriter, DiffChunkListWriterState,
    NamedStoreMgr,
};
use ndm_lib::{FileLinkedState, FinalizedObjState, FsMetaClient, NodeState};
use ndn_lib::{
    caculate_qcid_from_file, calculate_file_chunk_id, ChunkHasher, ChunkId, FileObject,
    NamedObject, NdnError, NdnResult, ObjId, SimpleChunkList, CHUNK_DEFAULT_SIZE,
    MIN_QCID_FILE_SIZE,
};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::buffer_db::LocalFileBufferDB;
use crate::fb_service::{FileBufferService, NdmPath, WriteLease};

static HANDLE_SEQ: AtomicU64 = AtomicU64::new(1);

const BUFFER_DIR_NAME: &str = "buffers";
const BUFFER_DB_FILE: &str = "file_buffer.db";
const DIRECT_FINALIZE_FILE_SIZE: u64 = CHUNK_DEFAULT_SIZE * 2;

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
    pub linked_obj_id: Option<ObjId>,
    pub linked_qcid: Option<ChunkId>,
    pub linked_at: Option<u64>,
    pub finalized_at: Option<u64>,
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

#[derive(Clone, Debug)]
struct ChunkSegment {
    chunk_id: ChunkId,
    offset: u64,
    size: u64,
}

#[derive(Clone, Debug)]
enum ContentLayout {
    Single {
        segment: ChunkSegment,
    },
    ChunkList {
        segments: Vec<ChunkSegment>,
        chunk_list_obj_id: ObjId,
        chunk_list_obj_str: String,
    },
}

impl ContentLayout {
    fn content_obj_id(&self) -> ObjId {
        match self {
            Self::Single { segment } => segment.chunk_id.to_obj_id(),
            Self::ChunkList {
                chunk_list_obj_id, ..
            } => chunk_list_obj_id.clone(),
        }
    }

    fn segments(&self) -> Vec<ChunkSegment> {
        match self {
            Self::Single { segment } => vec![segment.clone()],
            Self::ChunkList { segments, .. } => segments.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct FinalizeData {
    file_inode_id: u64,
    file_size: u64,
    diff_file_path: PathBuf,
    content_layout: ContentLayout,
    file_obj_id: ObjId,
    file_obj_str: String,
    qcid: ChunkId,
    linked_at: u64,
}

#[derive(Clone, Debug)]
struct OverlayDirtyChunkSegment {
    chunk_id: ChunkId,
    diff_file_offset: u64,
    size: u64,
}

#[derive(Clone, Debug)]
struct OverlayFinalizeData {
    finalize_data: FinalizeData,
    dirty_segments: Vec<OverlayDirtyChunkSegment>,
}

pub struct LocalFileBufferService {
    base_dir: PathBuf,
    buffer_dir: PathBuf,
    size_limit: u64,
    size_used: RwLock<u64>,
    db: Arc<LocalFileBufferDB>,
    records: RwLock<HashMap<String, Arc<RwLock<FileBufferRecord>>>>,
    named_store_mgr: RwLock<Option<Arc<NamedStoreMgr>>>,
    fsmeta_client: RwLock<Option<Arc<FsMetaClient>>>,
}

impl LocalFileBufferService {
    fn now_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

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
            named_store_mgr: RwLock::new(None),
            fsmeta_client: RwLock::new(None),
        }
    }

    pub fn with_named_store_mgr(mut self, named_store_mgr: Arc<NamedStoreMgr>) -> Self {
        self.named_store_mgr = RwLock::new(Some(named_store_mgr));
        self
    }

    pub fn with_fsmeta_client(mut self, fsmeta_client: Arc<FsMetaClient>) -> Self {
        self.fsmeta_client = RwLock::new(Some(fsmeta_client));
        self
    }

    pub fn set_named_store_mgr(&self, named_store_mgr: Arc<NamedStoreMgr>) -> NdnResult<()> {
        let mut guard = self
            .named_store_mgr
            .write()
            .map_err(|_| NdnError::InvalidState("named_store_mgr lock poisoned".to_string()))?;
        *guard = Some(named_store_mgr);
        Ok(())
    }

    pub fn set_fsmeta_client(&self, fsmeta_client: Arc<FsMetaClient>) -> NdnResult<()> {
        let mut guard = self
            .fsmeta_client
            .write()
            .map_err(|_| NdnError::InvalidState("fsmeta_client lock poisoned".to_string()))?;
        *guard = Some(fsmeta_client);
        Ok(())
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

    async fn persist_meta(&self, record: &FileBufferRecord) -> NdnResult<()> {
        let meta = record.to_meta();
        let handle_id = record.handle_id.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.set_meta(&handle_id, &meta))
            .await
            .map_err(|e| NdnError::IoError(format!("persist meta join error: {}", e)))??;
        Ok(())
    }

    async fn calculate_content_layout(
        &self,
        diff_file_path: &PathBuf,
        file_size: u64,
    ) -> NdnResult<ContentLayout> {
        if file_size <= CHUNK_DEFAULT_SIZE {
            let path_str = diff_file_path.to_string_lossy().to_string();
            let (chunk_id, _) =
                calculate_file_chunk_id(&path_str, ChunkId::default_chunk_type()).await?;
            return Ok(ContentLayout::Single {
                segment: ChunkSegment {
                    chunk_id,
                    offset: 0,
                    size: file_size,
                },
            });
        }

        let mut file = fs::File::open(diff_file_path).await.map_err(|e| {
            NdnError::IoError(format!("open diff file for chunk list failed: {}", e))
        })?;
        let mut segments = Vec::new();
        let mut chunk_ids = Vec::new();
        let mut remaining = file_size;
        let mut offset = 0u64;

        while remaining > 0 {
            let this_chunk_size = std::cmp::min(remaining, CHUNK_DEFAULT_SIZE) as usize;
            let mut buf = vec![0u8; this_chunk_size];
            file.read_exact(&mut buf).await.map_err(|e| {
                NdnError::IoError(format!("read diff file for chunk list failed: {}", e))
            })?;
            let chunk_id = ChunkHasher::new(None)?.calc_mix_chunk_id_from_bytes(&buf)?;
            segments.push(ChunkSegment {
                chunk_id: chunk_id.clone(),
                offset,
                size: this_chunk_size as u64,
            });
            chunk_ids.push(chunk_id);
            remaining -= this_chunk_size as u64;
            offset += this_chunk_size as u64;
        }

        if chunk_ids.is_empty() {
            return Err(NdnError::InvalidData(
                "chunk list calc failed: empty chunk list for non-empty file".to_string(),
            ));
        }

        let simple_chunk_list = SimpleChunkList::from_chunk_list(chunk_ids)?;
        let (chunk_list_obj_id, chunk_list_obj_str) = simple_chunk_list.gen_obj_id();
        Ok(ContentLayout::ChunkList {
            segments,
            chunk_list_obj_id,
            chunk_list_obj_str,
        })
    }

    fn simple_chunk_list_from_ids_with_sizes(
        chunk_ids: Vec<ChunkId>,
        chunk_sizes: &[u64],
    ) -> NdnResult<SimpleChunkList> {
        let total_size: u64 = chunk_sizes.iter().sum();
        match SimpleChunkList::from_chunk_list(chunk_ids.clone()) {
            Ok(list) => {
                if list.total_size != total_size {
                    return Err(NdnError::InvalidData(format!(
                        "chunk list total size mismatch, expect {} got {}",
                        total_size, list.total_size
                    )));
                }
                Ok(list)
            }
            Err(_) => Ok(SimpleChunkList {
                total_size,
                body: chunk_ids,
            }),
        }
    }

    fn overlay_chunk_sizes(
        &self,
        base_chunk_ids: &[ChunkId],
        diff_state: &FileBufferDiffState,
    ) -> NdnResult<Vec<u64>> {
        if !diff_state.merged_chunk_sizes.is_empty() {
            return Ok(diff_state.merged_chunk_sizes.clone());
        }
        if !diff_state.base_chunk_sizes.is_empty() {
            return Ok(diff_state.base_chunk_sizes.clone());
        }

        let mut sizes = Vec::with_capacity(base_chunk_ids.len());
        for chunk_id in base_chunk_ids {
            let size = chunk_id.get_length().ok_or_else(|| {
                NdnError::InvalidData(
                    "overlay chunk size missing in diff_state and base chunk id".to_string(),
                )
            })?;
            sizes.push(size);
        }
        Ok(sizes)
    }

    fn overlay_writer_state(
        &self,
        fb: &FileBufferRecord,
        base_chunk_ids: &[ChunkId],
        diff_state: &FileBufferDiffState,
    ) -> NdnResult<DiffChunkListWriterState> {
        let base_chunk_sizes = if diff_state.base_chunk_sizes.is_empty() {
            let mut sizes = Vec::with_capacity(base_chunk_ids.len());
            for chunk_id in base_chunk_ids {
                let size = chunk_id.get_length().ok_or_else(|| {
                    NdnError::InvalidData(
                        "overlay base chunk size missing in diff_state and base chunk id"
                            .to_string(),
                    )
                })?;
                sizes.push(size);
            }
            sizes
        } else {
            diff_state.base_chunk_sizes.clone()
        };
        let merged_chunk_sizes = self.overlay_chunk_sizes(base_chunk_ids, diff_state)?;
        let total_size = if diff_state.total_size > 0 {
            diff_state.total_size
        } else {
            merged_chunk_sizes.iter().sum()
        };

        let base_chunk_list = Self::simple_chunk_list_from_ids_with_sizes(
            base_chunk_ids.to_vec(),
            &base_chunk_sizes,
        )?;
        let (base_chunk_list_id, _) = base_chunk_list.gen_obj_id();

        Ok(DiffChunkListWriterState {
            base_chunk_list: base_chunk_list_id,
            diff_file_path: fb.diff_file_path.clone(),
            chunk_indices: diff_state.chunk_indices.clone(),
            diff_chunk_sizes: diff_state.diff_chunk_sizes.clone(),
            base_chunk_sizes,
            merged_chunk_sizes,
            position: diff_state.position.min(total_size),
            total_size,
            auto_cache: diff_state.auto_cache,
            local_mode: diff_state.local_mode,
            fixed_chunk_size: diff_state.fixed_chunk_size,
            append_merge_last_chunk: diff_state.append_merge_last_chunk,
        })
    }

    fn overlay_content_layout(
        &self,
        merged_chunk_list: SimpleChunkList,
        merged_chunk_sizes: &[u64],
    ) -> NdnResult<ContentLayout> {
        if merged_chunk_list.body.len() != merged_chunk_sizes.len() {
            return Err(NdnError::InvalidData(format!(
                "overlay merged chunk size count {} mismatch chunk id count {}",
                merged_chunk_sizes.len(),
                merged_chunk_list.body.len()
            )));
        }

        if merged_chunk_list.body.is_empty() {
            return Err(NdnError::InvalidData(
                "overlay merged chunk list is empty".to_string(),
            ));
        }

        let mut segments = Vec::with_capacity(merged_chunk_list.body.len());
        let mut offset = 0u64;
        for (chunk_id, size) in merged_chunk_list.body.iter().zip(merged_chunk_sizes.iter()) {
            segments.push(ChunkSegment {
                chunk_id: chunk_id.clone(),
                offset,
                size: *size,
            });
            offset = offset.saturating_add(*size);
        }

        if segments.len() == 1 {
            return Ok(ContentLayout::Single {
                segment: segments[0].clone(),
            });
        }

        let (chunk_list_obj_id, chunk_list_obj_str) = merged_chunk_list.gen_obj_id();
        Ok(ContentLayout::ChunkList {
            segments,
            chunk_list_obj_id,
            chunk_list_obj_str,
        })
    }

    fn qcid_from_content_layout(content_layout: &ContentLayout) -> NdnResult<ChunkId> {
        match content_layout {
            ContentLayout::Single { segment } => Ok(segment.chunk_id.clone()),
            ContentLayout::ChunkList { segments, .. } => segments
                .first()
                .map(|s| s.chunk_id.clone())
                .ok_or_else(|| NdnError::InvalidData("empty chunk list for qcid".to_string())),
        }
    }

    fn build_overlay_finalize_data(
        &self,
        file_inode_id: u64,
        diff_file_path: &PathBuf,
        merged_chunk_list: SimpleChunkList,
        merged_chunk_sizes: &[u64],
        existing_linked_at: Option<u64>,
    ) -> NdnResult<FinalizeData> {
        let content_layout = self.overlay_content_layout(merged_chunk_list, merged_chunk_sizes)?;
        let file_size: u64 = merged_chunk_sizes.iter().sum();
        let content_obj_id = content_layout.content_obj_id();
        let file_obj = FileObject::new(
            format!("inode-{}", file_inode_id),
            file_size,
            content_obj_id.to_string(),
        );
        let (file_obj_id, file_obj_str) = file_obj.gen_obj_id();
        let qcid = Self::qcid_from_content_layout(&content_layout)?;

        if file_size >= MIN_QCID_FILE_SIZE {
            log::warn!(
                "overlay finalize uses first chunk as qcid fallback, inode={}",
                file_inode_id
            );
        }

        Ok(FinalizeData {
            file_inode_id,
            file_size,
            diff_file_path: diff_file_path.clone(),
            content_layout,
            file_obj_id,
            file_obj_str,
            qcid,
            linked_at: existing_linked_at.unwrap_or_else(Self::now_unix_secs),
        })
    }

    async fn build_overlay_finalize_context(
        &self,
        fb: &FileBufferRecord,
        existing_linked_at: Option<u64>,
    ) -> NdnResult<OverlayFinalizeData> {
        let base_chunk_ids = match &fb.base_reader {
            FileBufferBaseReader::BaseChunkList(chunk_ids) => chunk_ids.clone(),
            FileBufferBaseReader::None => {
                return Err(NdnError::InvalidState(
                    "overlay finalize context requires base chunk list".to_string(),
                ))
            }
        };

        let diff_state = fb
            .diff_state
            .read()
            .map_err(|_| NdnError::InvalidState("filebuffer diff_state poisoned".to_string()))?
            .clone();
        let writer_state = self.overlay_writer_state(fb, &base_chunk_ids, &diff_state)?;
        let merged_state = DiffChunkListWriter::rebuild_merged_state_from_writer_state(
            &base_chunk_ids,
            &writer_state,
        )
        .await?;
        let finalize_data = self.build_overlay_finalize_data(
            fb.file_inode_id,
            &fb.diff_file_path,
            merged_state.merged_chunk_list,
            &merged_state.merged_chunk_sizes,
            existing_linked_at,
        )?;
        let dirty_segments = merged_state
            .dirty_chunks
            .into_iter()
            .map(|chunk: DiffChunkListDirtyChunk| OverlayDirtyChunkSegment {
                chunk_id: chunk.chunk_id,
                diff_file_offset: chunk.diff_file_offset,
                size: chunk.chunk_size,
            })
            .collect();
        Ok(OverlayFinalizeData {
            finalize_data,
            dirty_segments,
        })
    }

    fn get_named_store_mgr(&self) -> NdnResult<Option<Arc<NamedStoreMgr>>> {
        let guard = self
            .named_store_mgr
            .read()
            .map_err(|_| NdnError::InvalidState("named_store_mgr lock poisoned".to_string()))?;
        Ok(guard.clone())
    }

    fn get_fsmeta_client(&self) -> NdnResult<Option<Arc<FsMetaClient>>> {
        let guard = self
            .fsmeta_client
            .read()
            .map_err(|_| NdnError::InvalidState("fsmeta_client lock poisoned".to_string()))?;
        Ok(guard.clone())
    }

    async fn put_chunks_to_store(
        &self,
        named_store_mgr: &Arc<NamedStoreMgr>,
        diff_file_path: &PathBuf,
        segments: &[ChunkSegment],
    ) -> NdnResult<()> {
        let mut file = fs::File::open(diff_file_path).await.map_err(|e| {
            NdnError::IoError(format!("open diff file for store put failed: {}", e))
        })?;
        let mut cursor = 0u64;

        for segment in segments {
            if named_store_mgr.have_chunk(&segment.chunk_id).await {
                continue;
            }

            if cursor != segment.offset {
                file.seek(std::io::SeekFrom::Start(segment.offset))
                    .await
                    .map_err(|e| NdnError::IoError(format!("seek diff file failed: {}", e)))?;
                cursor = segment.offset;
            }

            let mut buf = vec![0u8; segment.size as usize];
            file.read_exact(&mut buf)
                .await
                .map_err(|e| NdnError::IoError(format!("read chunk bytes failed: {}", e)))?;
            named_store_mgr
                .put_chunk(&segment.chunk_id, &buf, true)
                .await?;
            cursor += segment.size;
        }

        Ok(())
    }

    async fn put_overlay_dirty_chunks_to_store(
        &self,
        named_store_mgr: &Arc<NamedStoreMgr>,
        diff_file_path: &PathBuf,
        dirty_segments: &[OverlayDirtyChunkSegment],
    ) -> NdnResult<()> {
        if dirty_segments.is_empty() {
            return Ok(());
        }

        let mut file = fs::File::open(diff_file_path).await.map_err(|e| {
            NdnError::IoError(format!(
                "open overlay diff file for store put failed: {}",
                e
            ))
        })?;
        let mut cursor = 0u64;

        for segment in dirty_segments {
            if named_store_mgr.have_chunk(&segment.chunk_id).await {
                continue;
            }

            if cursor != segment.diff_file_offset {
                file.seek(std::io::SeekFrom::Start(segment.diff_file_offset))
                    .await
                    .map_err(|e| {
                        NdnError::IoError(format!("seek overlay diff file failed: {}", e))
                    })?;
                cursor = segment.diff_file_offset;
            }

            let mut buf = vec![0u8; segment.size as usize];
            file.read_exact(&mut buf).await.map_err(|e| {
                NdnError::IoError(format!("read overlay dirty chunk bytes failed: {}", e))
            })?;
            named_store_mgr
                .put_chunk(&segment.chunk_id, &buf, true)
                .await?;
            cursor = cursor.saturating_add(segment.size);
        }

        Ok(())
    }

    async fn put_links_to_store(
        &self,
        named_store_mgr: &Arc<NamedStoreMgr>,
        diff_file_path: &PathBuf,
        segments: &[ChunkSegment],
        qcid: &ChunkId,
    ) -> NdnResult<()> {
        let local_path = diff_file_path.to_string_lossy().to_string();
        let now = Self::now_unix_secs();

        for segment in segments {
            let local_info = ChunkLocalInfo {
                path: local_path.clone(),
                qcid: qcid.to_string(),
                last_modify_time: now,
                range: Some(Range {
                    start: segment.offset,
                    end: segment.offset + segment.size,
                }),
            };
            named_store_mgr
                .add_chunk_by_link_to_local_file(&segment.chunk_id, segment.size, &local_info)
                .await?;
        }

        Ok(())
    }

    async fn put_overlay_dirty_links_to_store(
        &self,
        named_store_mgr: &Arc<NamedStoreMgr>,
        diff_file_path: &PathBuf,
        dirty_segments: &[OverlayDirtyChunkSegment],
        qcid: &ChunkId,
    ) -> NdnResult<()> {
        if dirty_segments.is_empty() {
            return Ok(());
        }

        let local_path = diff_file_path.to_string_lossy().to_string();
        let now = Self::now_unix_secs();

        for segment in dirty_segments {
            let local_info = ChunkLocalInfo {
                path: local_path.clone(),
                qcid: qcid.to_string(),
                last_modify_time: now,
                range: Some(Range {
                    start: segment.diff_file_offset,
                    end: segment.diff_file_offset + segment.size,
                }),
            };
            named_store_mgr
                .add_chunk_by_link_to_local_file(&segment.chunk_id, segment.size, &local_info)
                .await?;
        }

        Ok(())
    }

    async fn put_objects_to_store(
        &self,
        named_store_mgr: &Arc<NamedStoreMgr>,
        content_layout: &ContentLayout,
        file_obj_id: &ObjId,
        file_obj_str: &str,
    ) -> NdnResult<()> {
        if let ContentLayout::ChunkList {
            chunk_list_obj_id,
            chunk_list_obj_str,
            ..
        } = content_layout
        {
            named_store_mgr
                .put_object(chunk_list_obj_id, chunk_list_obj_str)
                .await?;
        }
        named_store_mgr.put_object(file_obj_id, file_obj_str).await
    }

    async fn build_finalize_data(
        &self,
        file_inode_id: u64,
        diff_file_path: &PathBuf,
        existing_linked_at: Option<u64>,
    ) -> NdnResult<FinalizeData> {
        let metadata = fs::metadata(diff_file_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                NdnError::NotFound(format!("diff file not found: {}", diff_file_path.display()))
            } else {
                NdnError::IoError(format!("stat diff file failed: {}", e))
            }
        })?;
        let file_size = metadata.len();
        let content_layout = self
            .calculate_content_layout(diff_file_path, file_size)
            .await?;
        let content_obj_id = content_layout.content_obj_id();
        let file_obj = FileObject::new(
            format!("inode-{}", file_inode_id),
            file_size,
            content_obj_id.to_string(),
        );
        let (file_obj_id, file_obj_str) = file_obj.gen_obj_id();
        let qcid = if file_size >= MIN_QCID_FILE_SIZE {
            caculate_qcid_from_file(diff_file_path).await?
        } else {
            match &content_layout {
                ContentLayout::Single { segment } => segment.chunk_id.clone(),
                ContentLayout::ChunkList { segments, .. } => segments
                    .first()
                    .map(|s| s.chunk_id.clone())
                    .ok_or_else(|| {
                        NdnError::InvalidData("empty chunk list for qcid fallback".to_string())
                    })?,
            }
        };

        Ok(FinalizeData {
            file_inode_id,
            file_size,
            diff_file_path: diff_file_path.clone(),
            content_layout,
            file_obj_id,
            file_obj_str,
            qcid,
            linked_at: existing_linked_at.unwrap_or_else(Self::now_unix_secs),
        })
    }

    async fn update_fsmeta_linked_state(
        &self,
        file_inode_id: u64,
        file_obj_id: ObjId,
        qcid: ChunkId,
        fb_handle: String,
        linked_at: u64,
    ) -> NdnResult<()> {
        let Some(client) = self.get_fsmeta_client()? else {
            return Ok(());
        };

        client
            .update_inode_state(
                file_inode_id,
                NodeState::Linked(FileLinkedState {
                    obj_id: file_obj_id,
                    qcid: qcid.to_obj_id(),
                    filebuffer_id: fb_handle,
                    linked_at,
                }),
                None,
            )
            .await
            .map_err(|e| NdnError::RemoteError(format!("fsmeta update linked state failed: {}", e)))
    }

    async fn update_fsmeta_finalized_state(
        &self,
        file_inode_id: u64,
        file_obj_id: ObjId,
        finalized_at: u64,
    ) -> NdnResult<()> {
        let Some(client) = self.get_fsmeta_client()? else {
            return Ok(());
        };

        client
            .update_inode_state(
                file_inode_id,
                NodeState::Finalized(FinalizedObjState {
                    obj_id: file_obj_id,
                    finalized_at,
                }),
                None,
            )
            .await
            .map_err(|e| {
                NdnError::RemoteError(format!("fsmeta update finalized state failed: {}", e))
            })
    }

    async fn do_finalize(
        &self,
        arc_record: &Arc<RwLock<FileBufferRecord>>,
        finalize_data: FinalizeData,
    ) -> NdnResult<()> {
        let snapshot = {
            let mut guard = arc_record
                .write()
                .map_err(|_| NdnError::InvalidState("record lock poisoned".to_string()))?;
            guard.read_only = true;
            {
                let mut state = guard.diff_state.write().map_err(|_| {
                    NdnError::InvalidState("filebuffer diff_state poisoned".to_string())
                })?;
                state.linked_obj_id = Some(finalize_data.file_obj_id.clone());
                state.linked_qcid = Some(finalize_data.qcid.clone());
                state.linked_at = Some(finalize_data.linked_at);
                state.finalized_at = Some(Self::now_unix_secs());
            }
            guard.clone_ref()
        };

        self.persist_meta(&snapshot).await?;

        let finalized_at = snapshot
            .diff_state
            .read()
            .map_err(|_| NdnError::InvalidState("filebuffer diff_state poisoned".to_string()))?
            .finalized_at
            .unwrap_or_else(Self::now_unix_secs);
        self.update_fsmeta_finalized_state(
            finalize_data.file_inode_id,
            finalize_data.file_obj_id,
            finalized_at,
        )
        .await?;

        if let Ok(meta) = fs::metadata(&finalize_data.diff_file_path).await {
            let mut used = self.size_used.write().unwrap();
            *used = used.saturating_sub(meta.len());
        }
        if let Err(e) = fs::remove_file(&finalize_data.diff_file_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(format!("remove file failed: {}", e)));
            }
        }

        Ok(())
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

    async fn cacl_name(&self, fb: &FileBufferRecord) -> NdnResult<bool> {
        self.close(fb).await?;

        {
            let state = fb.diff_state.read().map_err(|_| {
                NdnError::InvalidState("filebuffer diff_state poisoned".to_string())
            })?;
            if state.finalized_at.is_some() {
                return Ok(true);
            }
            if state.linked_obj_id.is_some() {
                return Ok(false);
            }
        }

        let overlay_context = if matches!(fb.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
            Some(self.build_overlay_finalize_context(fb, None).await?)
        } else {
            None
        };

        let finalize_data = if let Some(context) = &overlay_context {
            context.finalize_data.clone()
        } else {
            self.build_finalize_data(fb.file_inode_id, &fb.diff_file_path, None)
                .await?
        };
        let file_size = finalize_data.file_size;

        if file_size <= DIRECT_FINALIZE_FILE_SIZE {
            if let Some(named_store_mgr) = self.get_named_store_mgr()? {
                if let Some(context) = &overlay_context {
                    self.put_overlay_dirty_chunks_to_store(
                        &named_store_mgr,
                        &fb.diff_file_path,
                        &context.dirty_segments,
                    )
                    .await?;
                } else {
                    let segments = finalize_data.content_layout.segments();
                    self.put_chunks_to_store(&named_store_mgr, &fb.diff_file_path, &segments)
                        .await?;
                }
                self.put_objects_to_store(
                    &named_store_mgr,
                    &finalize_data.content_layout,
                    &finalize_data.file_obj_id,
                    &finalize_data.file_obj_str,
                )
                .await?;
            } else {
                log::warn!(
                    "cacl_name direct finalize without named_store_mgr configured, handle={}",
                    fb.handle_id
                );
            }
            let arc_record = self.get_record(&fb.handle_id)?;
            self.do_finalize(&arc_record, finalize_data).await?;
            return Ok(true);
        }

        if let Some(named_store_mgr) = self.get_named_store_mgr()? {
            if let Some(context) = &overlay_context {
                self.put_overlay_dirty_links_to_store(
                    &named_store_mgr,
                    &fb.diff_file_path,
                    &context.dirty_segments,
                    &finalize_data.qcid,
                )
                .await?;
            } else {
                let segments = finalize_data.content_layout.segments();
                self.put_links_to_store(
                    &named_store_mgr,
                    &fb.diff_file_path,
                    &segments,
                    &finalize_data.qcid,
                )
                .await?;
            }
            self.put_objects_to_store(
                &named_store_mgr,
                &finalize_data.content_layout,
                &finalize_data.file_obj_id,
                &finalize_data.file_obj_str,
            )
            .await?;
        } else {
            log::warn!(
                "cacl_name linked without named_store_mgr configured, handle={}",
                fb.handle_id
            );
        }

        {
            let mut state = fb.diff_state.write().map_err(|_| {
                NdnError::InvalidState("filebuffer diff_state poisoned".to_string())
            })?;
            state.linked_obj_id = Some(finalize_data.file_obj_id.clone());
            state.linked_qcid = Some(finalize_data.qcid.clone());
            state.linked_at = Some(finalize_data.linked_at);
            state.finalized_at = None;
        }
        self.persist_meta(fb).await?;

        self.update_fsmeta_linked_state(
            fb.file_inode_id,
            finalize_data.file_obj_id,
            finalize_data.qcid,
            fb.handle_id.clone(),
            finalize_data.linked_at,
        )
        .await?;

        Ok(false)
    }

    async fn finalize(&self, fb_id: String) -> NdnResult<()> {
        let _ = self.get_buffer(&fb_id).await?;
        let arc_record = self.get_record(&fb_id)?;
        let snapshot = self.snapshot_record(&arc_record)?;
        let state = snapshot
            .diff_state
            .read()
            .map_err(|_| NdnError::InvalidState("filebuffer diff_state poisoned".to_string()))?
            .clone();

        if state.finalized_at.is_some() {
            return Ok(());
        }

        let overlay_context =
            if matches!(snapshot.base_reader, FileBufferBaseReader::BaseChunkList(_)) {
                Some(
                    self.build_overlay_finalize_context(&snapshot, state.linked_at)
                        .await?,
                )
            } else {
                None
            };

        let finalize_data = if let Some(context) = &overlay_context {
            context.finalize_data.clone()
        } else {
            self.build_finalize_data(
                snapshot.file_inode_id,
                &snapshot.diff_file_path,
                state.linked_at,
            )
            .await?
        };

        if let Some(named_store_mgr) = self.get_named_store_mgr()? {
            if let Some(context) = &overlay_context {
                self.put_overlay_dirty_chunks_to_store(
                    &named_store_mgr,
                    &snapshot.diff_file_path,
                    &context.dirty_segments,
                )
                .await?;
            } else {
                let segments = finalize_data.content_layout.segments();
                self.put_chunks_to_store(&named_store_mgr, &snapshot.diff_file_path, &segments)
                    .await?;
            }
            self.put_objects_to_store(
                &named_store_mgr,
                &finalize_data.content_layout,
                &finalize_data.file_obj_id,
                &finalize_data.file_obj_str,
            )
            .await?;
        } else {
            log::warn!(
                "finalize without named_store_mgr configured, handle={}",
                fb_id
            );
        }

        if let Some(existing) = state.linked_obj_id {
            if existing != finalize_data.file_obj_id {
                log::warn!(
                    "finalize recalculated file obj differs from linked state, handle={}, old={}, new={}",
                    fb_id,
                    existing,
                    finalize_data.file_obj_id
                );
            }
        }
        if let Some(existing_qcid) = state.linked_qcid {
            if existing_qcid != finalize_data.qcid {
                log::warn!(
                    "finalize recalculated qcid differs from linked state, handle={}, old={}, new={}",
                    fb_id,
                    existing_qcid.to_string(),
                    finalize_data.qcid.to_string()
                );
            }
        }

        self.do_finalize(&arc_record, finalize_data).await
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
    use ndn_lib::{
        ChunkHasher, ChunkType, FileObject, NamedObject, SimpleChunkList, CHUNK_DEFAULT_SIZE,
    };
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

    #[tokio::test]
    async fn test_cacl_name_and_finalize_for_none_base_reader() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/d.txt".to_string()),
                103,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        service.append(&fb, b"linked-data").await.unwrap();
        service.close(&fb).await.unwrap();

        let finalized = service.cacl_name(&fb).await.unwrap();
        assert!(finalized);

        let record = service.get_buffer(&fb.handle_id).await.unwrap();
        let finalized_state = record.diff_state.read().unwrap().clone();
        assert!(finalized_state.linked_obj_id.is_some());
        assert!(finalized_state.linked_qcid.is_some());
        assert!(finalized_state.linked_at.is_some());
        assert!(finalized_state.finalized_at.is_some());
        assert!(record.read_only);
        assert!(!record.diff_file_path.exists());

        // idempotent
        service.finalize(fb.handle_id.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn test_cacl_name_supports_overlay_mode_with_dirty_diff() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let base_chunk = ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(b"base-chunk")
            .unwrap();
        let dirty_chunk_bytes = b"diff-chunk";
        let fb = service
            .alloc_buffer(
                &NdmPath("/e.txt".to_string()),
                104,
                vec![base_chunk.clone()],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        fs::write(&fb.diff_file_path, dirty_chunk_bytes)
            .await
            .unwrap();
        {
            let mut state = fb.diff_state.write().unwrap();
            state.chunk_indices = vec![0];
            state.diff_chunk_sizes = vec![dirty_chunk_bytes.len() as u64];
            state.base_chunk_sizes = vec![base_chunk.get_length().unwrap()];
            state.merged_chunk_sizes = vec![dirty_chunk_bytes.len() as u64];
            state.total_size = dirty_chunk_bytes.len() as u64;
            state.position = state.total_size;
        }
        service.flush(&fb).await.unwrap();

        let finalized = service.cacl_name(&fb).await.unwrap();
        assert!(finalized);

        let record = service.get_buffer(&fb.handle_id).await.unwrap();
        let state = record.diff_state.read().unwrap().clone();
        assert!(state.finalized_at.is_some());
        assert!(record.read_only);
        assert!(!record.diff_file_path.exists());

        let dirty_chunk = ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(dirty_chunk_bytes)
            .unwrap();
        let file_obj = FileObject::new(
            format!("inode-{}", fb.file_inode_id),
            dirty_chunk_bytes.len() as u64,
            dirty_chunk.to_obj_id().to_string(),
        );
        let (expected_file_obj_id, _) = file_obj.gen_obj_id();
        assert_eq!(state.linked_obj_id.unwrap(), expected_file_obj_id);
    }

    #[tokio::test]
    async fn test_overlay_cacl_name_linked_then_finalize() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let huge_size = DIRECT_FINALIZE_FILE_SIZE + 1;
        let base_chunk = ChunkId::from_mix_hash_result(huge_size, &[0x11; 32], ChunkType::Mix256);
        let fb = service
            .alloc_buffer(
                &NdmPath("/overlay-linked.txt".to_string()),
                107,
                vec![base_chunk.clone()],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        let finalized = service.cacl_name(&fb).await.unwrap();
        assert!(!finalized);

        let linked = service.get_buffer(&fb.handle_id).await.unwrap();
        let linked_state = linked.diff_state.read().unwrap().clone();
        assert!(linked_state.linked_obj_id.is_some());
        assert!(linked_state.linked_qcid.is_some());
        assert!(linked_state.linked_at.is_some());
        assert!(linked_state.finalized_at.is_none());
        assert!(linked.diff_file_path.exists());

        service.finalize(fb.handle_id.clone()).await.unwrap();
        let finalized_record = service.get_buffer(&fb.handle_id).await.unwrap();
        let finalized_state = finalized_record.diff_state.read().unwrap().clone();
        assert!(finalized_state.finalized_at.is_some());
        assert!(!finalized_record.diff_file_path.exists());
        assert!(finalized_record.read_only);
    }

    #[tokio::test]
    async fn test_cacl_name_uses_chunk_list_when_file_exceeds_default_chunk_size() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/big.txt".to_string()),
                105,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        let first = vec![0x11u8; CHUNK_DEFAULT_SIZE as usize];
        let second = vec![0x22u8; 19];
        service.append(&fb, &first).await.unwrap();
        service.append(&fb, &second).await.unwrap();
        service.close(&fb).await.unwrap();

        let finalized = service.cacl_name(&fb).await.unwrap();
        assert!(finalized);
        let linked = service.get_buffer(&fb.handle_id).await.unwrap();
        let linked_obj_id = linked
            .diff_state
            .read()
            .unwrap()
            .linked_obj_id
            .clone()
            .unwrap();
        assert!(linked.diff_state.read().unwrap().finalized_at.is_some());

        let chunk_1 = ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(&first)
            .unwrap();
        let chunk_2 = ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(&second)
            .unwrap();
        let simple_chunk_list = SimpleChunkList::from_chunk_list(vec![chunk_1, chunk_2]).unwrap();
        let (content_obj_id, _) = simple_chunk_list.gen_obj_id();
        assert!(content_obj_id.is_chunk_list());

        let file_obj = FileObject::new(
            format!("inode-{}", fb.file_inode_id),
            (first.len() + second.len()) as u64,
            content_obj_id.to_string(),
        );
        let (expected_file_obj_id, _) = file_obj.gen_obj_id();
        assert_eq!(linked_obj_id, expected_file_obj_id);
    }

    #[tokio::test]
    async fn test_cacl_name_enters_linked_state_when_file_exceeds_direct_finalize_limit() {
        let dir = tempdir().unwrap();
        let service = LocalFileBufferService::new(dir.path().to_path_buf(), 0);
        let fb = service
            .alloc_buffer(
                &NdmPath("/linked-big.txt".to_string()),
                106,
                vec![],
                &test_lease(),
                None,
            )
            .await
            .unwrap();

        let first = vec![0x31u8; CHUNK_DEFAULT_SIZE as usize];
        let second = vec![0x32u8; CHUNK_DEFAULT_SIZE as usize];
        let third = vec![0x33u8; 19];
        assert!((first.len() + second.len() + third.len()) as u64 > DIRECT_FINALIZE_FILE_SIZE);

        service.append(&fb, &first).await.unwrap();
        service.append(&fb, &second).await.unwrap();
        service.append(&fb, &third).await.unwrap();
        service.close(&fb).await.unwrap();

        let finalized = service.cacl_name(&fb).await.unwrap();
        assert!(!finalized);

        let linked = service.get_buffer(&fb.handle_id).await.unwrap();
        let linked_state = linked.diff_state.read().unwrap().clone();
        assert!(linked_state.linked_obj_id.is_some());
        assert!(linked_state.linked_qcid.is_some());
        assert!(linked_state.linked_at.is_some());
        assert!(linked_state.finalized_at.is_none());
        assert!(linked.diff_file_path.exists());
    }
}
