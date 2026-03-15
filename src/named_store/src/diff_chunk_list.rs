use crate::{chunk_list_reader::OpenChunkReader, NamedStoreMgr};
use ndn_lib::{ChunkHasher, ChunkId, ChunkReader, NdnError, NdnResult, ObjId, SimpleChunkList};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::future::Future;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, ReadBuf};

type LoadingFuture = Pin<Box<dyn Future<Output = std::io::Result<ChunkReader>> + Send + 'static>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffChunkList {
    pub base_chunk_list: ObjId,
    pub diff_file_path: PathBuf,
    pub chunk_indices: Vec<u64>,
    pub chunk_ids: Option<Vec<ChunkId>>,
}

impl DiffChunkList {
    pub fn validate(&self) -> NdnResult<()> {
        if let Some(chunk_ids) = &self.chunk_ids {
            if chunk_ids.len() != self.chunk_indices.len() {
                return Err(NdnError::InvalidParam(format!(
                    "chunk_ids length {} does not match chunk_indices length {}",
                    chunk_ids.len(),
                    self.chunk_indices.len()
                )));
            }
        }

        let mut seen = HashSet::new();
        for index in &self.chunk_indices {
            if !seen.insert(*index) {
                return Err(NdnError::InvalidParam(format!(
                    "duplicate diff chunk index {}",
                    index
                )));
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct DiffChunkListReaderOptions {
    pub auto_cache: bool,
    pub local_mode: bool,
    pub fixed_chunk_size: Option<u64>,
    pub base_chunk_sizes: Option<Vec<u64>>,
    pub diff_chunk_sizes: Option<Vec<u64>>,
    pub open_chunk_reader: Option<OpenChunkReader>,
}

impl DiffChunkListReaderOptions {
    pub fn with_local_mode(mut self, local_mode: bool) -> Self {
        self.local_mode = local_mode;
        self
    }

    pub fn with_fixed_chunk_size(mut self, fixed_chunk_size: u64) -> Self {
        self.fixed_chunk_size = Some(fixed_chunk_size);
        self
    }

    pub fn with_base_chunk_sizes(mut self, base_chunk_sizes: Vec<u64>) -> Self {
        self.base_chunk_sizes = Some(base_chunk_sizes);
        self
    }

    pub fn with_diff_chunk_sizes(mut self, diff_chunk_sizes: Vec<u64>) -> Self {
        self.diff_chunk_sizes = Some(diff_chunk_sizes);
        self
    }

    pub fn with_open_chunk_reader(mut self, open_chunk_reader: OpenChunkReader) -> Self {
        self.open_chunk_reader = Some(open_chunk_reader);
        self
    }
}

#[derive(Clone)]
struct DiffEntryMeta {
    chunk_index: usize,
    file_offset: u64,
    size: u64,
    chunk_id: Option<ChunkId>,
}

#[derive(Clone)]
enum MergedChunkSource {
    Base { chunk_id: ChunkId },
    Diff { diff_entry_index: usize },
}

#[derive(Clone)]
struct MergedChunkMeta {
    size: u64,
    start: u64,
    source: MergedChunkSource,
}

pub struct DiffChunkListReader {
    named_store_mgr: Arc<NamedStoreMgr>,
    auto_cache: bool,
    local_mode: bool,
    open_chunk_reader: Option<OpenChunkReader>,
    diff_file_path: PathBuf,

    diff_entries: Vec<DiffEntryMeta>,
    merged_chunks: Vec<MergedChunkMeta>,
    total_size: u64,
    position: u64,

    next_chunk_index: usize,
    next_chunk_offset: u64,
    active_chunk_index: Option<usize>,
    pending_seek: Option<u64>,

    loading_chunk_index: Option<usize>,
    loading_future: Option<LoadingFuture>,
    current_reader: Option<ChunkReader>,
}

impl DiffChunkListReader {
    pub async fn new(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list: SimpleChunkList,
        diff_chunk_list: DiffChunkList,
        seek_from: SeekFrom,
        auto_cache: bool,
    ) -> NdnResult<Self> {
        let options = DiffChunkListReaderOptions {
            auto_cache,
            ..Default::default()
        };
        Self::with_options(
            named_store_mgr,
            base_chunk_list,
            diff_chunk_list,
            seek_from,
            options,
        )
        .await
    }

    pub async fn with_options(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list: SimpleChunkList,
        diff_chunk_list: DiffChunkList,
        seek_from: SeekFrom,
        options: DiffChunkListReaderOptions,
    ) -> NdnResult<Self> {
        diff_chunk_list.validate()?;

        let base_chunk_sizes = resolve_chunk_sizes_for_list(
            &named_store_mgr,
            &base_chunk_list,
            options.local_mode,
            options.fixed_chunk_size,
            options.base_chunk_sizes.clone(),
        )
        .await?;

        let diff_file_size = if diff_chunk_list.diff_file_path.exists() {
            fs::metadata(&diff_chunk_list.diff_file_path)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?
                .len()
        } else {
            0
        };

        let diff_sizes = resolve_diff_chunk_sizes(
            &diff_chunk_list,
            &base_chunk_sizes,
            options.fixed_chunk_size,
            options.diff_chunk_sizes.clone(),
            diff_file_size,
        )?;
        let diff_entries = build_diff_entries(&diff_chunk_list, diff_sizes)?;
        validate_diff_file_len(&diff_entries, diff_file_size)?;

        let merged_chunks =
            build_merged_chunks(&base_chunk_list, &base_chunk_sizes, &diff_entries)?;
        let total_size = merged_chunks
            .last()
            .map(|chunk| chunk.start.saturating_add(chunk.size))
            .unwrap_or(0);

        let mut reader = Self {
            named_store_mgr,
            auto_cache: options.auto_cache,
            local_mode: options.local_mode,
            open_chunk_reader: options.open_chunk_reader,
            diff_file_path: diff_chunk_list.diff_file_path,
            diff_entries,
            merged_chunks,
            total_size,
            position: 0,
            next_chunk_index: 0,
            next_chunk_offset: 0,
            active_chunk_index: None,
            pending_seek: None,
            loading_chunk_index: None,
            loading_future: None,
            current_reader: None,
        };

        let target = reader.calc_seek_target(seek_from)?;
        reader.apply_seek_target(target);
        Ok(reader)
    }

    pub async fn from_writer_state(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list: SimpleChunkList,
        writer_state: &DiffChunkListWriterState,
        seek_from: SeekFrom,
        open_chunk_reader: Option<OpenChunkReader>,
    ) -> NdnResult<Self> {
        let diff_chunk_list = DiffChunkList {
            base_chunk_list: writer_state.base_chunk_list.clone(),
            diff_file_path: writer_state.diff_file_path.clone(),
            chunk_indices: writer_state.chunk_indices.clone(),
            chunk_ids: None,
        };

        let options = DiffChunkListReaderOptions {
            auto_cache: writer_state.auto_cache,
            local_mode: writer_state.local_mode,
            fixed_chunk_size: writer_state.fixed_chunk_size,
            base_chunk_sizes: Some(writer_state.base_chunk_sizes.clone()),
            diff_chunk_sizes: Some(writer_state.diff_chunk_sizes.clone()),
            open_chunk_reader: None,
        };

        let options = if let Some(open_chunk_reader) = open_chunk_reader {
            options.with_open_chunk_reader(open_chunk_reader)
        } else {
            options
        };

        Self::with_options(
            named_store_mgr,
            base_chunk_list,
            diff_chunk_list,
            seek_from,
            options,
        )
        .await
    }

    pub async fn from_writer_state_file(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list: SimpleChunkList,
        diff_file_path: impl AsRef<Path>,
        seek_from: SeekFrom,
        open_chunk_reader: Option<OpenChunkReader>,
    ) -> NdnResult<Self> {
        let state = DiffChunkListWriter::load_state(diff_file_path).await?;
        Self::from_writer_state(
            named_store_mgr,
            base_chunk_list,
            &state,
            seek_from,
            open_chunk_reader,
        )
        .await
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub async fn build_simple_chunk_list(
        &self,
        store_to_named_store_mgr: bool,
    ) -> NdnResult<SimpleChunkList> {
        let mut chunk_ids = Vec::with_capacity(self.merged_chunks.len());

        for merged_chunk in &self.merged_chunks {
            match &merged_chunk.source {
                MergedChunkSource::Base { chunk_id } => {
                    chunk_ids.push(chunk_id.clone());
                }
                MergedChunkSource::Diff { diff_entry_index } => {
                    let entry = &self.diff_entries[*diff_entry_index];
                    let diff_bytes = self.read_diff_chunk_bytes(entry).await?;
                    let chunk_id = if let Some(chunk_id) = &entry.chunk_id {
                        chunk_id.clone()
                    } else {
                        ChunkHasher::new(None)
                            .map_err(|e| NdnError::InvalidParam(e.to_string()))?
                            .calc_mix_chunk_id_from_bytes(&diff_bytes)?
                    };

                    if store_to_named_store_mgr {
                        self.named_store_mgr
                            .put_chunk(&chunk_id, &diff_bytes, false)
                            .await?;
                    }

                    chunk_ids.push(chunk_id);
                }
            }
        }

        SimpleChunkList::from_chunk_list(chunk_ids)
    }

    async fn read_diff_chunk_bytes(&self, entry: &DiffEntryMeta) -> NdnResult<Vec<u8>> {
        let mut file = File::open(&self.diff_file_path)
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;
        file.seek(SeekFrom::Start(entry.file_offset))
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;

        let mut data = vec![0u8; entry.size as usize];
        file.read_exact(&mut data)
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;
        Ok(data)
    }

    fn calc_seek_target(&self, seek_from: SeekFrom) -> NdnResult<u64> {
        let target = match seek_from {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::Current(delta) => self.position as i128 + delta as i128,
            SeekFrom::End(delta) => self.total_size as i128 + delta as i128,
        };

        if target < 0 || target > self.total_size as i128 {
            return Err(NdnError::OffsetTooLarge(format!(
                "seek target {} out of range [0, {}]",
                target, self.total_size
            )));
        }

        Ok(target as u64)
    }

    fn apply_seek_target(&mut self, position: u64) {
        self.position = position;
        let (chunk_index, chunk_offset) = self.locate_position(position);

        self.next_chunk_index = chunk_index;
        self.next_chunk_offset = chunk_offset;
        self.active_chunk_index = None;
        self.pending_seek = None;
        self.current_reader = None;
        self.loading_future = None;
        self.loading_chunk_index = None;
    }

    fn locate_position(&self, position: u64) -> (usize, u64) {
        if position >= self.total_size || self.merged_chunks.is_empty() {
            return (self.merged_chunks.len(), 0);
        }

        let mut left = 0usize;
        let mut right = self.merged_chunks.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let chunk = &self.merged_chunks[mid];
            let end = chunk.start.saturating_add(chunk.size);
            if end <= position {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        let index = left;
        let offset = position.saturating_sub(self.merged_chunks[index].start);
        (index, offset)
    }

    fn start_loading_current_chunk(&mut self) -> std::io::Result<()> {
        if self.next_chunk_index >= self.merged_chunks.len() {
            return Ok(());
        }

        let chunk = self.merged_chunks[self.next_chunk_index].clone();
        if self.next_chunk_offset > chunk.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "chunk offset {} exceeds chunk size {}",
                    self.next_chunk_offset, chunk.size
                ),
            ));
        }

        let named_store_mgr = self.named_store_mgr.clone();
        let diff_file_path = self.diff_file_path.clone();
        let auto_cache = self.auto_cache;
        let local_mode = self.local_mode;
        let open_chunk_reader = self.open_chunk_reader.clone();
        let read_offset = self.next_chunk_offset;
        let diff_entry = match &chunk.source {
            MergedChunkSource::Diff { diff_entry_index } => {
                Some(self.diff_entries[*diff_entry_index].clone())
            }
            _ => None,
        };

        self.loading_chunk_index = Some(self.next_chunk_index);
        self.loading_future = Some(Box::pin(async move {
            match chunk.source {
                MergedChunkSource::Base { chunk_id } => open_store_chunk_reader_with_fallback(
                    named_store_mgr,
                    chunk_id,
                    read_offset,
                    auto_cache,
                    local_mode,
                    open_chunk_reader,
                )
                .await
                .map_err(to_io_error),
                MergedChunkSource::Diff { .. } => {
                    let entry = diff_entry.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "diff entry missing for diff source",
                        )
                    })?;
                    open_diff_file_reader(
                        diff_file_path,
                        entry.file_offset,
                        read_offset,
                        entry.size,
                    )
                    .await
                }
            }
        }));

        Ok(())
    }

    fn advance_after_chunk_eof(&mut self) {
        if let Some(active_chunk_index) = self.active_chunk_index.take() {
            self.next_chunk_index = active_chunk_index.saturating_add(1);
            self.next_chunk_offset = 0;
        }
        self.current_reader = None;
    }
}

impl AsyncRead for DiffChunkListReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if this.pending_seek.is_some() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "seek in progress, call poll_complete before read",
            )));
        }

        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if let Some(reader) = this.current_reader.as_mut() {
                let before = buf.filled().len();
                match Pin::new(reader).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len().saturating_sub(before);
                        if bytes_read > 0 {
                            this.position = this.position.saturating_add(bytes_read as u64);
                            return Poll::Ready(Ok(()));
                        }

                        this.advance_after_chunk_eof();
                        continue;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if let Some(fut) = this.loading_future.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(reader)) => {
                        let Some(active_chunk_index) = this.loading_chunk_index.take() else {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "loading chunk index missing",
                            )));
                        };
                        this.loading_future = None;
                        this.active_chunk_index = Some(active_chunk_index);
                        this.current_reader = Some(reader);
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        this.loading_future = None;
                        this.loading_chunk_index = None;
                        return Poll::Ready(Err(err));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if this.position >= this.total_size || this.next_chunk_index >= this.merged_chunks.len()
            {
                return Poll::Ready(Ok(()));
            }

            if let Err(err) = this.start_loading_current_chunk() {
                return Poll::Ready(Err(err));
            }
        }
    }
}

impl AsyncSeek for DiffChunkListReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        let target = this.calc_seek_target(position).map_err(to_io_error)?;
        this.pending_seek = Some(target);
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        if let Some(target) = this.pending_seek.take() {
            this.apply_seek_target(target);
        }
        Poll::Ready(Ok(this.position))
    }
}

#[derive(Clone)]
pub struct DiffChunkListWriterOptions {
    pub auto_cache: bool,
    pub local_mode: bool,
    pub fixed_chunk_size: Option<u64>,
    pub base_chunk_sizes: Option<Vec<u64>>,
    pub open_chunk_reader: Option<OpenChunkReader>,
    pub append_merge_last_chunk: bool,
}

impl Default for DiffChunkListWriterOptions {
    fn default() -> Self {
        Self {
            auto_cache: false,
            local_mode: false,
            fixed_chunk_size: None,
            base_chunk_sizes: None,
            open_chunk_reader: None,
            append_merge_last_chunk: true,
        }
    }
}

impl DiffChunkListWriterOptions {
    pub fn with_local_mode(mut self, local_mode: bool) -> Self {
        self.local_mode = local_mode;
        self
    }

    pub fn with_fixed_chunk_size(mut self, fixed_chunk_size: u64) -> Self {
        self.fixed_chunk_size = Some(fixed_chunk_size);
        self
    }

    pub fn with_base_chunk_sizes(mut self, base_chunk_sizes: Vec<u64>) -> Self {
        self.base_chunk_sizes = Some(base_chunk_sizes);
        self
    }

    pub fn with_open_chunk_reader(mut self, open_chunk_reader: OpenChunkReader) -> Self {
        self.open_chunk_reader = Some(open_chunk_reader);
        self
    }

    pub fn with_append_merge_last_chunk(mut self, append_merge_last_chunk: bool) -> Self {
        self.append_merge_last_chunk = append_merge_last_chunk;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffChunkListWriterState {
    pub base_chunk_list: ObjId,
    pub diff_file_path: PathBuf,
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

#[derive(Debug, Clone)]
pub struct DiffChunkListDirtyChunk {
    pub chunk_index: u64,
    pub diff_file_offset: u64,
    pub chunk_size: u64,
    pub chunk_id: ChunkId,
}

pub struct DiffChunkListMergedState {
    pub merged_chunk_list: SimpleChunkList,
    pub merged_chunk_sizes: Vec<u64>,
    pub dirty_chunks: Vec<DiffChunkListDirtyChunk>,
}

struct PersistedDirtyChunks {
    chunk_indices: Vec<u64>,
    diff_chunk_sizes: Vec<u64>,
    dirty_chunk_id_map: HashMap<usize, ChunkId>,
    diff_chunk_ids: Vec<ChunkId>,
}

pub struct DiffChunkListWriter {
    named_store_mgr: Arc<NamedStoreMgr>,
    base_chunk_list_id: ObjId,
    base_chunk_ids: Vec<ChunkId>,
    base_chunk_sizes: Vec<u64>,

    auto_cache: bool,
    local_mode: bool,
    open_chunk_reader: Option<OpenChunkReader>,
    fixed_chunk_size: Option<u64>,
    append_merge_last_chunk: bool,

    diff_file_path: PathBuf,
    merged_chunk_sizes: Vec<u64>,
    dirty_chunks: BTreeMap<usize, Vec<u8>>,
    position: u64,
    total_size: u64,
}

impl DiffChunkListWriter {
    pub async fn new(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list_id: ObjId,
        base_chunk_list: SimpleChunkList,
        diff_file_path: impl AsRef<Path>,
        options: DiffChunkListWriterOptions,
    ) -> NdnResult<Self> {
        let base_chunk_sizes = resolve_chunk_sizes_for_list(
            &named_store_mgr,
            &base_chunk_list,
            options.local_mode,
            options.fixed_chunk_size,
            options.base_chunk_sizes.clone(),
        )
        .await?;

        let total_size = base_chunk_sizes.iter().sum();
        Ok(Self {
            named_store_mgr,
            base_chunk_list_id,
            base_chunk_ids: base_chunk_list.body,
            base_chunk_sizes: base_chunk_sizes.clone(),
            auto_cache: options.auto_cache,
            local_mode: options.local_mode,
            open_chunk_reader: options.open_chunk_reader,
            fixed_chunk_size: options.fixed_chunk_size,
            append_merge_last_chunk: options.append_merge_last_chunk,
            diff_file_path: diff_file_path.as_ref().to_path_buf(),
            merged_chunk_sizes: base_chunk_sizes,
            dirty_chunks: BTreeMap::new(),
            position: 0,
            total_size,
        })
    }

    pub fn state_file_path_for(diff_file_path: &Path) -> PathBuf {
        let mut file_name = diff_file_path
            .file_name()
            .map(|v| v.to_os_string())
            .unwrap_or_else(|| OsString::from("diff_chunk_list"));
        file_name.push(".state.json");
        diff_file_path.with_file_name(file_name)
    }

    pub fn state_file_path(&self) -> PathBuf {
        Self::state_file_path_for(&self.diff_file_path)
    }

    pub async fn load_state(
        diff_file_path: impl AsRef<Path>,
    ) -> NdnResult<DiffChunkListWriterState> {
        let state_path = Self::state_file_path_for(diff_file_path.as_ref());
        let state_bytes = fs::read(&state_path)
            .await
            .map_err(|e| NdnError::IoError(format!("read writer state failed: {}", e)))?;
        serde_json::from_slice::<DiffChunkListWriterState>(&state_bytes)
            .map_err(|e| NdnError::DecodeError(format!("decode writer state failed: {}", e)))
    }

    pub async fn open_from_state(
        named_store_mgr: Arc<NamedStoreMgr>,
        base_chunk_list: SimpleChunkList,
        state: DiffChunkListWriterState,
        open_chunk_reader: Option<OpenChunkReader>,
    ) -> NdnResult<Self> {
        if state.base_chunk_sizes.len() != base_chunk_list.body.len() {
            return Err(NdnError::InvalidData(format!(
                "base chunk size count {} mismatch base chunk id count {}",
                state.base_chunk_sizes.len(),
                base_chunk_list.body.len()
            )));
        }
        if state.position > state.total_size {
            return Err(NdnError::InvalidData(format!(
                "writer state position {} exceeds total_size {}",
                state.position, state.total_size
            )));
        }

        let merged_total: u64 = state.merged_chunk_sizes.iter().sum();
        if merged_total != state.total_size {
            return Err(NdnError::InvalidData(format!(
                "writer state total_size mismatch, expect {} got {}",
                merged_total, state.total_size
            )));
        }
        if state.chunk_indices.len() != state.diff_chunk_sizes.len() {
            return Err(NdnError::InvalidData(format!(
                "writer state diff size count {} mismatch index count {}",
                state.diff_chunk_sizes.len(),
                state.chunk_indices.len()
            )));
        }

        let mut file_data = if state.diff_file_path.exists() {
            fs::read(&state.diff_file_path)
                .await
                .map_err(|e| NdnError::IoError(format!("read diff file failed: {}", e)))?
        } else {
            Vec::new()
        };

        let expected_file_size: u64 = state.diff_chunk_sizes.iter().sum();
        if expected_file_size != file_data.len() as u64 {
            return Err(NdnError::InvalidData(format!(
                "diff file size mismatch, expect {} got {}",
                expected_file_size,
                file_data.len()
            )));
        }

        let mut dirty_chunks = BTreeMap::new();
        let mut cursor = 0usize;
        let mut seen = HashSet::new();
        for (chunk_index_u64, chunk_size) in state
            .chunk_indices
            .iter()
            .zip(state.diff_chunk_sizes.iter())
        {
            let chunk_index = *chunk_index_u64 as usize;
            if !seen.insert(chunk_index) {
                return Err(NdnError::InvalidData(format!(
                    "duplicate dirty chunk index {} in writer state",
                    chunk_index
                )));
            }
            let expected_size = state
                .merged_chunk_sizes
                .get(chunk_index)
                .copied()
                .ok_or_else(|| {
                    NdnError::InvalidData(format!(
                        "dirty chunk index {} out of merged chunk range {}",
                        chunk_index,
                        state.merged_chunk_sizes.len()
                    ))
                })?;
            if *chunk_size != expected_size {
                return Err(NdnError::InvalidData(format!(
                    "dirty chunk size mismatch for index {}, expect {} got {}",
                    chunk_index, expected_size, chunk_size
                )));
            }

            let end = cursor.saturating_add(*chunk_size as usize);
            dirty_chunks.insert(chunk_index, file_data[cursor..end].to_vec());
            cursor = end;
        }
        file_data.clear();

        Ok(Self {
            named_store_mgr,
            base_chunk_list_id: state.base_chunk_list,
            base_chunk_ids: base_chunk_list.body,
            base_chunk_sizes: state.base_chunk_sizes,
            auto_cache: state.auto_cache,
            local_mode: state.local_mode,
            open_chunk_reader,
            fixed_chunk_size: state.fixed_chunk_size,
            append_merge_last_chunk: state.append_merge_last_chunk,
            diff_file_path: state.diff_file_path,
            merged_chunk_sizes: state.merged_chunk_sizes,
            dirty_chunks,
            position: state.position,
            total_size: state.total_size,
        })
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn seek(&mut self, seek_from: SeekFrom) -> NdnResult<u64> {
        let target = match seek_from {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::Current(delta) => self.position as i128 + delta as i128,
            SeekFrom::End(delta) => self.total_size as i128 + delta as i128,
        };

        if target < 0 || target > self.total_size as i128 {
            return Err(NdnError::OffsetTooLarge(format!(
                "seek target {} out of range [0, {}]",
                target, self.total_size
            )));
        }

        self.position = target as u64;
        Ok(self.position)
    }

    pub async fn write(&mut self, buf: &[u8]) -> NdnResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut written = 0usize;
        while written < buf.len() {
            if self.position == self.total_size {
                self.extend_for_append((buf.len() - written) as u64)?;
            }

            let (chunk_index, chunk_offset) = self
                .locate_position(self.position)
                .ok_or_else(|| NdnError::Internal("failed to locate write position".to_string()))?;
            let chunk_size = self.merged_chunk_sizes[chunk_index];
            let writable = chunk_size.saturating_sub(chunk_offset) as usize;
            if writable == 0 {
                continue;
            }

            let write_len = writable.min(buf.len() - written);
            self.ensure_dirty_chunk(chunk_index).await?;
            let chunk = self
                .dirty_chunks
                .get_mut(&chunk_index)
                .ok_or_else(|| NdnError::Internal("dirty chunk missing".to_string()))?;
            let start = chunk_offset as usize;
            let end = start + write_len;
            chunk[start..end].copy_from_slice(&buf[written..written + write_len]);

            written += write_len;
            self.position = self.position.saturating_add(write_len as u64);
            if self.position > self.total_size {
                self.total_size = self.position;
            }
        }

        Ok(written)
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> NdnResult<()> {
        let written = self.write(buf).await?;
        if written != buf.len() {
            return Err(NdnError::IoError(format!(
                "write truncated, expect {} got {}",
                buf.len(),
                written
            )));
        }
        Ok(())
    }

    pub async fn close(&self) -> NdnResult<DiffChunkListWriterState> {
        let persisted = self.persist_dirty_chunks_to_diff_file(false).await?;
        let state = DiffChunkListWriterState {
            base_chunk_list: self.base_chunk_list_id.clone(),
            diff_file_path: self.diff_file_path.clone(),
            chunk_indices: persisted.chunk_indices,
            diff_chunk_sizes: persisted.diff_chunk_sizes,
            base_chunk_sizes: self.base_chunk_sizes.clone(),
            merged_chunk_sizes: self.merged_chunk_sizes.clone(),
            position: self.position,
            total_size: self.total_size,
            auto_cache: self.auto_cache,
            local_mode: self.local_mode,
            fixed_chunk_size: self.fixed_chunk_size,
            append_merge_last_chunk: self.append_merge_last_chunk,
        };

        let state_path = self.state_file_path();
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
        }
        let state_bytes = serde_json::to_vec(&state)
            .map_err(|e| NdnError::InvalidParam(format!("serialize writer state failed: {}", e)))?;
        fs::write(&state_path, state_bytes)
            .await
            .map_err(|e| NdnError::IoError(format!("write writer state failed: {}", e)))?;

        Ok(state)
    }

    pub async fn finalize(self, named_mode: bool) -> NdnResult<(DiffChunkList, SimpleChunkList)> {
        let persisted = self.persist_dirty_chunks_to_diff_file(named_mode).await?;

        let merged_chunk_ids = Self::build_merged_chunk_ids(
            &self.base_chunk_ids,
            &self.merged_chunk_sizes,
            &persisted.dirty_chunk_id_map,
        )?;
        let merged_chunk_list =
            build_simple_chunk_list_from_ids(merged_chunk_ids, &self.merged_chunk_sizes)?;
        let diff_chunk_list = DiffChunkList {
            base_chunk_list: self.base_chunk_list_id,
            diff_file_path: self.diff_file_path,
            chunk_indices: persisted.chunk_indices,
            chunk_ids: if named_mode {
                Some(persisted.diff_chunk_ids)
            } else {
                None
            },
        };

        let state_path = Self::state_file_path_for(&diff_chunk_list.diff_file_path);
        if state_path.exists() {
            let _ = fs::remove_file(state_path).await;
        }

        Ok((diff_chunk_list, merged_chunk_list))
    }

    pub async fn rebuild_merged_state_from_writer_state(
        base_chunk_ids: &[ChunkId],
        writer_state: &DiffChunkListWriterState,
    ) -> NdnResult<DiffChunkListMergedState> {
        if writer_state.position > writer_state.total_size {
            return Err(NdnError::InvalidData(format!(
                "writer state position {} exceeds total_size {}",
                writer_state.position, writer_state.total_size
            )));
        }

        let merged_total: u64 = writer_state.merged_chunk_sizes.iter().sum();
        if merged_total != writer_state.total_size {
            return Err(NdnError::InvalidData(format!(
                "writer state total_size mismatch, expect {} got {}",
                merged_total, writer_state.total_size
            )));
        }
        if writer_state.chunk_indices.len() != writer_state.diff_chunk_sizes.len() {
            return Err(NdnError::InvalidData(format!(
                "writer state diff size count {} mismatch index count {}",
                writer_state.diff_chunk_sizes.len(),
                writer_state.chunk_indices.len()
            )));
        }

        let expected_file_size: u64 = writer_state.diff_chunk_sizes.iter().sum();
        let diff_file_data = if writer_state.diff_file_path.exists() {
            fs::read(&writer_state.diff_file_path)
                .await
                .map_err(|e| NdnError::IoError(format!("read diff file failed: {}", e)))?
        } else if expected_file_size == 0 {
            Vec::new()
        } else {
            return Err(NdnError::NotFound(format!(
                "diff file not found: {}",
                writer_state.diff_file_path.display()
            )));
        };

        if expected_file_size != diff_file_data.len() as u64 {
            return Err(NdnError::InvalidData(format!(
                "diff file size mismatch, expect {} got {}",
                expected_file_size,
                diff_file_data.len()
            )));
        }

        let mut dirty_chunk_id_map = HashMap::new();
        let mut dirty_chunks = Vec::with_capacity(writer_state.chunk_indices.len());
        let mut cursor = 0usize;
        let mut seen = HashSet::new();
        for (chunk_index_u64, chunk_size) in writer_state
            .chunk_indices
            .iter()
            .zip(writer_state.diff_chunk_sizes.iter())
        {
            let chunk_index = usize::try_from(*chunk_index_u64).map_err(|_| {
                NdnError::InvalidData(format!(
                    "dirty chunk index {} overflow usize",
                    chunk_index_u64
                ))
            })?;
            if !seen.insert(chunk_index) {
                return Err(NdnError::InvalidData(format!(
                    "duplicate dirty chunk index {} in writer state",
                    chunk_index
                )));
            }
            let expected_size = writer_state
                .merged_chunk_sizes
                .get(chunk_index)
                .copied()
                .ok_or_else(|| {
                    NdnError::InvalidData(format!(
                        "dirty chunk index {} out of merged chunk range {}",
                        chunk_index,
                        writer_state.merged_chunk_sizes.len()
                    ))
                })?;
            if *chunk_size != expected_size {
                return Err(NdnError::InvalidData(format!(
                    "dirty chunk size mismatch for index {}, expect {} got {}",
                    chunk_index, expected_size, chunk_size
                )));
            }

            let chunk_size_usize = usize::try_from(*chunk_size).map_err(|_| {
                NdnError::InvalidData(format!("dirty chunk size too large: {}", chunk_size))
            })?;
            let end = cursor
                .checked_add(chunk_size_usize)
                .ok_or_else(|| NdnError::InvalidData("dirty chunk cursor overflow".to_string()))?;
            if end > diff_file_data.len() {
                return Err(NdnError::InvalidData(format!(
                    "dirty chunk bytes out of range, index {}, end {} > file_len {}",
                    chunk_index,
                    end,
                    diff_file_data.len()
                )));
            }

            let chunk_data = &diff_file_data[cursor..end];
            let chunk_id = ChunkHasher::new(None)
                .map_err(|e| NdnError::InvalidParam(e.to_string()))?
                .calc_mix_chunk_id_from_bytes(chunk_data)?;

            dirty_chunk_id_map.insert(chunk_index, chunk_id.clone());
            dirty_chunks.push(DiffChunkListDirtyChunk {
                chunk_index: *chunk_index_u64,
                diff_file_offset: cursor as u64,
                chunk_size: *chunk_size,
                chunk_id,
            });
            cursor = end;
        }

        if cursor != diff_file_data.len() {
            return Err(NdnError::InvalidData(format!(
                "diff chunk bytes not fully consumed, consumed {} total {}",
                cursor,
                diff_file_data.len()
            )));
        }

        let merged_chunk_ids = Self::build_merged_chunk_ids(
            base_chunk_ids,
            &writer_state.merged_chunk_sizes,
            &dirty_chunk_id_map,
        )?;
        let merged_chunk_list =
            build_simple_chunk_list_from_ids(merged_chunk_ids, &writer_state.merged_chunk_sizes)?;

        Ok(DiffChunkListMergedState {
            merged_chunk_list,
            merged_chunk_sizes: writer_state.merged_chunk_sizes.clone(),
            dirty_chunks,
        })
    }

    fn build_merged_chunk_ids(
        base_chunk_ids: &[ChunkId],
        merged_chunk_sizes: &[u64],
        dirty_chunk_id_map: &HashMap<usize, ChunkId>,
    ) -> NdnResult<Vec<ChunkId>> {
        let mut merged_chunk_ids = Vec::with_capacity(merged_chunk_sizes.len());
        for index in 0..merged_chunk_sizes.len() {
            if let Some(chunk_id) = dirty_chunk_id_map.get(&index) {
                merged_chunk_ids.push(chunk_id.clone());
                continue;
            }

            if let Some(base_chunk_id) = base_chunk_ids.get(index) {
                merged_chunk_ids.push(base_chunk_id.clone());
                continue;
            }

            return Err(NdnError::InvalidData(format!(
                "chunk index {} has no chunk id (not in base and not dirty)",
                index
            )));
        }
        Ok(merged_chunk_ids)
    }

    async fn persist_dirty_chunks_to_diff_file(
        &self,
        named_mode: bool,
    ) -> NdnResult<PersistedDirtyChunks> {
        if let Some(parent) = self.diff_file_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
        }

        let mut file = File::create(&self.diff_file_path)
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;

        let mut dirty_chunk_id_map = HashMap::new();
        let mut chunk_indices = Vec::new();
        let mut diff_chunk_sizes = Vec::new();
        let mut diff_chunk_ids = Vec::new();

        for (chunk_index, chunk_data) in self.dirty_chunks.iter() {
            let expected_size = self
                .merged_chunk_sizes
                .get(*chunk_index)
                .copied()
                .ok_or_else(|| {
                    NdnError::InvalidParam("dirty chunk index out of range".to_string())
                })?;
            if chunk_data.len() as u64 != expected_size {
                return Err(NdnError::InvalidData(format!(
                    "dirty chunk size mismatch for index {}, expect {} got {}",
                    chunk_index,
                    expected_size,
                    chunk_data.len()
                )));
            }

            file.write_all(chunk_data)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;

            let chunk_id = ChunkHasher::new(None)
                .map_err(|e| NdnError::InvalidParam(e.to_string()))?
                .calc_mix_chunk_id_from_bytes(chunk_data)?;
            if named_mode {
                self.named_store_mgr
                    .put_chunk(&chunk_id, chunk_data, false)
                    .await?;
                diff_chunk_ids.push(chunk_id.clone());
            }

            dirty_chunk_id_map.insert(*chunk_index, chunk_id);
            chunk_indices.push(*chunk_index as u64);
            diff_chunk_sizes.push(chunk_data.len() as u64);
        }

        file.flush()
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;

        Ok(PersistedDirtyChunks {
            chunk_indices,
            diff_chunk_sizes,
            dirty_chunk_id_map,
            diff_chunk_ids,
        })
    }

    fn preferred_chunk_size(&self, remaining: u64) -> u64 {
        if let Some(fixed_chunk_size) = self.fixed_chunk_size {
            if fixed_chunk_size > 0 {
                return fixed_chunk_size;
            }
        }

        if self.base_chunk_sizes.len() > 1 {
            let mut max_size = 0u64;
            for size in &self.base_chunk_sizes[..self.base_chunk_sizes.len() - 1] {
                max_size = max_size.max(*size);
            }
            if max_size > 0 {
                return max_size;
            }
        }

        self.base_chunk_sizes
            .last()
            .copied()
            .filter(|size| *size > 0)
            .unwrap_or_else(|| remaining.max(1))
    }

    fn extend_for_append(&mut self, remaining: u64) -> NdnResult<()> {
        if remaining == 0 {
            return Ok(());
        }

        let preferred_chunk_size = self.preferred_chunk_size(remaining);
        if self.merged_chunk_sizes.is_empty() {
            let added = remaining.min(preferred_chunk_size);
            self.merged_chunk_sizes.push(added);
            self.total_size = self.total_size.saturating_add(added);
            return Ok(());
        }

        let last_index = self.merged_chunk_sizes.len() - 1;
        let last_size = self.merged_chunk_sizes[last_index];
        if self.append_merge_last_chunk && last_size < preferred_chunk_size {
            let grow = (preferred_chunk_size - last_size).min(remaining);
            self.merged_chunk_sizes[last_index] = last_size.saturating_add(grow);
            self.total_size = self.total_size.saturating_add(grow);
            if let Some(chunk) = self.dirty_chunks.get_mut(&last_index) {
                chunk.resize(self.merged_chunk_sizes[last_index] as usize, 0);
            }
            return Ok(());
        }

        let added = remaining.min(preferred_chunk_size);
        self.merged_chunk_sizes.push(added);
        self.total_size = self.total_size.saturating_add(added);
        Ok(())
    }

    fn locate_position(&self, position: u64) -> Option<(usize, u64)> {
        if self.merged_chunk_sizes.is_empty() || position >= self.total_size {
            return None;
        }

        let mut start = 0u64;
        for (index, size) in self.merged_chunk_sizes.iter().enumerate() {
            let end = start.saturating_add(*size);
            if position < end {
                return Some((index, position - start));
            }
            start = end;
        }
        None
    }

    async fn ensure_dirty_chunk(&mut self, chunk_index: usize) -> NdnResult<()> {
        let expected_size = self
            .merged_chunk_sizes
            .get(chunk_index)
            .copied()
            .ok_or_else(|| {
                NdnError::InvalidParam(format!("invalid chunk index {}", chunk_index))
            })?;

        if let Some(chunk) = self.dirty_chunks.get_mut(&chunk_index) {
            if chunk.len() as u64 != expected_size {
                chunk.resize(expected_size as usize, 0);
            }
            return Ok(());
        }

        let mut chunk_data = vec![0u8; expected_size as usize];
        if let Some(base_chunk_id) = self.base_chunk_ids.get(chunk_index) {
            let mut reader = open_store_chunk_reader_with_fallback(
                self.named_store_mgr.clone(),
                base_chunk_id.clone(),
                0,
                self.auto_cache,
                self.local_mode,
                self.open_chunk_reader.clone(),
            )
            .await?;

            let mut base_data = Vec::new();
            reader
                .read_to_end(&mut base_data)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;

            let copy_len = base_data.len().min(chunk_data.len());
            chunk_data[..copy_len].copy_from_slice(&base_data[..copy_len]);
        }

        self.dirty_chunks.insert(chunk_index, chunk_data);
        Ok(())
    }
}

fn to_io_error(err: NdnError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
}

fn build_simple_chunk_list_from_ids(
    chunk_ids: Vec<ChunkId>,
    merged_chunk_sizes: &[u64],
) -> NdnResult<SimpleChunkList> {
    let merged_total: u64 = merged_chunk_sizes.iter().sum();
    match SimpleChunkList::from_chunk_list(chunk_ids.clone()) {
        Ok(list) => {
            if list.total_size != merged_total {
                return Err(NdnError::InvalidData(format!(
                    "merged chunk total size mismatch, expect {} got {}",
                    merged_total, list.total_size
                )));
            }
            Ok(list)
        }
        Err(_) => Ok(SimpleChunkList {
            total_size: merged_total,
            body: chunk_ids,
        }),
    }
}

async fn open_store_chunk_reader_with_fallback(
    named_store_mgr: Arc<NamedStoreMgr>,
    chunk_id: ChunkId,
    offset: u64,
    auto_cache: bool,
    local_mode: bool,
    open_chunk_reader: Option<OpenChunkReader>,
) -> NdnResult<ChunkReader> {
    match named_store_mgr.open_chunk_reader(&chunk_id, offset).await {
        Ok((reader, _)) => Ok(reader),
        Err(open_err) => {
            if local_mode {
                return Err(open_err);
            }

            let Some(custom_open_chunk_reader) = open_chunk_reader else {
                return Err(open_err);
            };

            custom_open_chunk_reader(chunk_id, offset, auto_cache)
                .await
                .map(|(reader, _)| reader)
        }
    }
}

async fn open_diff_file_reader(
    diff_file_path: PathBuf,
    diff_file_offset: u64,
    chunk_offset: u64,
    chunk_size: u64,
) -> std::io::Result<ChunkReader> {
    let mut file = File::open(&diff_file_path).await?;
    let start = diff_file_offset
        .checked_add(chunk_offset)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "offset overflow"))?;

    file.seek(SeekFrom::Start(start)).await?;
    let limited = file.take(chunk_size.saturating_sub(chunk_offset));
    Ok(Box::pin(limited))
}

fn build_diff_entries(
    diff_chunk_list: &DiffChunkList,
    sizes: Vec<u64>,
) -> NdnResult<Vec<DiffEntryMeta>> {
    if sizes.len() != diff_chunk_list.chunk_indices.len() {
        return Err(NdnError::InvalidParam(format!(
            "diff chunk size count {} does not match chunk index count {}",
            sizes.len(),
            diff_chunk_list.chunk_indices.len()
        )));
    }

    let mut file_offset = 0u64;
    let mut entries = Vec::with_capacity(diff_chunk_list.chunk_indices.len());
    for (i, chunk_index) in diff_chunk_list.chunk_indices.iter().enumerate() {
        let size = sizes[i];
        let chunk_id = diff_chunk_list
            .chunk_ids
            .as_ref()
            .and_then(|ids| ids.get(i))
            .cloned();
        entries.push(DiffEntryMeta {
            chunk_index: *chunk_index as usize,
            file_offset,
            size,
            chunk_id,
        });
        file_offset = file_offset
            .checked_add(size)
            .ok_or_else(|| NdnError::InvalidData("diff file size overflow".to_string()))?;
    }

    Ok(entries)
}

fn validate_diff_file_len(entries: &[DiffEntryMeta], file_size: u64) -> NdnResult<()> {
    let expected_size = entries
        .last()
        .map(|entry| entry.file_offset.saturating_add(entry.size))
        .unwrap_or(0);
    if expected_size != file_size {
        return Err(NdnError::InvalidData(format!(
            "diff file size mismatch, expect {} got {}",
            expected_size, file_size
        )));
    }
    Ok(())
}

fn build_merged_chunks(
    base_chunk_list: &SimpleChunkList,
    base_chunk_sizes: &[u64],
    diff_entries: &[DiffEntryMeta],
) -> NdnResult<Vec<MergedChunkMeta>> {
    if base_chunk_list.body.len() != base_chunk_sizes.len() {
        return Err(NdnError::InvalidParam(format!(
            "base chunk id count {} does not match chunk size count {}",
            base_chunk_list.body.len(),
            base_chunk_sizes.len()
        )));
    }

    let mut chunks = Vec::with_capacity(base_chunk_list.body.len().max(diff_entries.len()));
    for (chunk_id, chunk_size) in base_chunk_list.body.iter().zip(base_chunk_sizes.iter()) {
        chunks.push(MergedChunkMeta {
            size: *chunk_size,
            start: 0,
            source: MergedChunkSource::Base {
                chunk_id: chunk_id.clone(),
            },
        });
    }

    for (entry_index, entry) in diff_entries.iter().enumerate() {
        if entry.chunk_index > chunks.len() {
            return Err(NdnError::InvalidParam(format!(
                "diff chunk index {} leaves gap, current merged chunk count {}",
                entry.chunk_index,
                chunks.len()
            )));
        }

        let new_chunk = MergedChunkMeta {
            size: entry.size,
            start: 0,
            source: MergedChunkSource::Diff {
                diff_entry_index: entry_index,
            },
        };
        if entry.chunk_index == chunks.len() {
            chunks.push(new_chunk);
        } else {
            chunks[entry.chunk_index] = new_chunk;
        }
    }

    let mut start = 0u64;
    for chunk in chunks.iter_mut() {
        chunk.start = start;
        start = start
            .checked_add(chunk.size)
            .ok_or_else(|| NdnError::InvalidData("merged chunk size overflow".to_string()))?;
    }

    Ok(chunks)
}

fn resolve_diff_chunk_sizes(
    diff_chunk_list: &DiffChunkList,
    base_chunk_sizes: &[u64],
    fixed_chunk_size: Option<u64>,
    explicit_diff_sizes: Option<Vec<u64>>,
    diff_file_size: u64,
) -> NdnResult<Vec<u64>> {
    if let Some(chunk_ids) = &diff_chunk_list.chunk_ids {
        let mut sizes = Vec::with_capacity(chunk_ids.len());
        for chunk_id in chunk_ids {
            let Some(size) = chunk_id.get_length() else {
                return Err(NdnError::Unsupported(format!(
                    "chunk id {} has no embedded length, pass diff_chunk_sizes explicitly",
                    chunk_id.to_base32()
                )));
            };
            sizes.push(size);
        }
        return Ok(sizes);
    }

    if let Some(sizes) = explicit_diff_sizes {
        return Ok(sizes);
    }

    let diff_count = diff_chunk_list.chunk_indices.len();
    if diff_count == 0 {
        return Ok(Vec::new());
    }

    let mut sizes = Vec::with_capacity(diff_count);
    let mut consumed = 0u64;
    for (i, chunk_index) in diff_chunk_list.chunk_indices.iter().enumerate() {
        let is_last = i + 1 == diff_count;
        if is_last {
            let remaining = diff_file_size.saturating_sub(consumed);
            sizes.push(remaining);
            consumed = consumed.saturating_add(remaining);
            continue;
        }

        let size = if (*chunk_index as usize) < base_chunk_sizes.len() {
            base_chunk_sizes[*chunk_index as usize]
        } else if let Some(fixed_chunk_size) = fixed_chunk_size {
            fixed_chunk_size
        } else {
            return Err(NdnError::Unsupported(format!(
                "cannot infer size for appended diff chunk index {}, set chunk_ids/diff_chunk_sizes/fixed_chunk_size",
                chunk_index
            )));
        };

        sizes.push(size);
        consumed = consumed.saturating_add(size);
    }

    Ok(sizes)
}

async fn resolve_chunk_sizes_for_list(
    named_store_mgr: &Arc<NamedStoreMgr>,
    chunk_list: &SimpleChunkList,
    local_mode: bool,
    fixed_chunk_size: Option<u64>,
    explicit_chunk_sizes: Option<Vec<u64>>,
) -> NdnResult<Vec<u64>> {
    if let Some(chunk_sizes) = explicit_chunk_sizes {
        if chunk_sizes.len() != chunk_list.body.len() {
            return Err(NdnError::InvalidParam(format!(
                "chunk_sizes length mismatch, expect {} got {}",
                chunk_list.body.len(),
                chunk_sizes.len()
            )));
        }

        if local_mode {
            ensure_chunks_available_in_local(named_store_mgr, chunk_list, Some(&chunk_sizes))
                .await?;
        }
        return Ok(chunk_sizes);
    }

    if local_mode {
        return ensure_chunks_available_in_local(named_store_mgr, chunk_list, None).await;
    }

    if let Some(mix_sizes) = resolve_mix_chunk_sizes(chunk_list) {
        return Ok(mix_sizes);
    }

    if let Some(fixed_chunk_size) = fixed_chunk_size {
        return resolve_fixed_chunk_sizes(chunk_list, fixed_chunk_size);
    }

    Err(NdnError::Unsupported(
        "cannot resolve chunk sizes: need mix chunk id, explicit chunk_sizes, fixed_chunk_size, or local_mode"
            .to_string(),
    ))
}

fn resolve_mix_chunk_sizes(chunk_list: &SimpleChunkList) -> Option<Vec<u64>> {
    let mut sizes = Vec::with_capacity(chunk_list.body.len());
    for chunk_id in &chunk_list.body {
        let Some(chunk_size) = chunk_id.get_length() else {
            return None;
        };
        sizes.push(chunk_size);
    }
    Some(sizes)
}

fn resolve_fixed_chunk_sizes(
    chunk_list: &SimpleChunkList,
    fixed_chunk_size: u64,
) -> NdnResult<Vec<u64>> {
    if chunk_list.body.is_empty() {
        return Ok(Vec::new());
    }
    if fixed_chunk_size == 0 {
        return Err(NdnError::InvalidParam(
            "fixed_chunk_size cannot be zero".to_string(),
        ));
    }

    if chunk_list.total_size == 0 {
        return Ok(vec![fixed_chunk_size; chunk_list.body.len()]);
    }

    let mut sizes = Vec::with_capacity(chunk_list.body.len());
    let mut remaining = chunk_list.total_size;
    for index in 0..chunk_list.body.len() {
        if remaining == 0 {
            return Err(NdnError::InvalidParam(format!(
                "fixed chunk size {} cannot fit total size {}",
                fixed_chunk_size, chunk_list.total_size
            )));
        }
        let size = if index + 1 == chunk_list.body.len() {
            remaining
        } else {
            fixed_chunk_size.min(remaining)
        };
        sizes.push(size);
        remaining -= size;
    }
    if remaining != 0 {
        return Err(NdnError::InvalidParam(format!(
            "resolve fixed chunk size failed, remaining {}",
            remaining
        )));
    }
    Ok(sizes)
}

async fn ensure_chunks_available_in_local(
    named_store_mgr: &Arc<NamedStoreMgr>,
    chunk_list: &SimpleChunkList,
    expected_sizes: Option<&Vec<u64>>,
) -> NdnResult<Vec<u64>> {
    let mut chunk_sizes = Vec::with_capacity(chunk_list.body.len());
    for (index, chunk_id) in chunk_list.body.iter().enumerate() {
        let (state, chunk_size, _) = named_store_mgr.query_chunk_state(chunk_id).await?;
        if !state.can_open_reader() {
            return Err(NdnError::NotFound(format!(
                "chunk {} missing in NamedStoreMgr local mode, state={}",
                chunk_id.to_base32(),
                state.to_str()
            )));
        }

        if let Some(sizes) = expected_sizes {
            if sizes[index] != chunk_size {
                return Err(NdnError::InvalidData(format!(
                    "chunk size mismatch for {}, expected={} actual={}",
                    chunk_id.to_base32(),
                    sizes[index],
                    chunk_size
                )));
            }
        }
        chunk_sizes.push(chunk_size);
    }
    Ok(chunk_sizes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NamedLocalStore, StoreLayout, StoreTarget};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn calc_mix_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    fn clone_chunk_list(chunk_list: &SimpleChunkList) -> SimpleChunkList {
        SimpleChunkList {
            total_size: chunk_list.total_size,
            body: chunk_list.body.clone(),
        }
    }

    fn default_target(store_id: &str) -> StoreTarget {
        StoreTarget {
            store_id: store_id.to_string(),
            device_did: None,
            capacity: Some(1024 * 1024 * 1024),
            used: Some(0),
            readonly: false,
            enabled: true,
            weight: 1,
        }
    }

    async fn create_mgr_with_store(
        store_id: &str,
    ) -> (
        TempDir,
        Arc<NamedStoreMgr>,
        Arc<tokio::sync::Mutex<NamedLocalStore>>,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let store_root = temp_dir.path().join(store_id);
        tokio::fs::create_dir_all(&store_root).await.unwrap();

        let store = NamedLocalStore::get_named_store_by_path(store_root)
            .await
            .unwrap();
        let store = Arc::new(tokio::sync::Mutex::new(store));

        let store_mgr = Arc::new(NamedStoreMgr::new());
        store_mgr.register_store(store.clone()).await;
        let layout = StoreLayout::new(1, vec![default_target(store_id)], 0, 0);
        store_mgr.add_layout(layout).await;

        (temp_dir, store_mgr, store)
    }

    async fn setup_base_chunk_list(
        store: &Arc<tokio::sync::Mutex<NamedLocalStore>>,
        chunks: &[Vec<u8>],
    ) -> SimpleChunkList {
        let chunk_ids: Vec<ChunkId> = chunks
            .iter()
            .map(|chunk| calc_mix_chunk_id(chunk))
            .collect();

        {
            let store = store.lock().await;
            for (chunk_id, data) in chunk_ids.iter().zip(chunks.iter()) {
                store.put_chunk(chunk_id, data, true).await.unwrap();
            }
        }

        SimpleChunkList::from_chunk_list(chunk_ids).unwrap()
    }

    #[tokio::test]
    async fn test_diff_chunk_list_reader_merge_and_seek() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cc".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("diff.bin");
        fs::write(&diff_file_path, b"BBBB").await.unwrap();
        let diff_chunk_list = DiffChunkList {
            base_chunk_list: base_chunk_list_id,
            diff_file_path: diff_file_path.clone(),
            chunk_indices: vec![1],
            chunk_ids: None,
        };

        let mut reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        let mut merged = Vec::new();
        reader.read_to_end(&mut merged).await.unwrap();
        assert_eq!(merged, b"aaaaBBBBcc".to_vec());

        reader.seek(SeekFrom::Start(3)).await.unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, b"aBBBBcc".to_vec());
    }

    #[tokio::test]
    async fn test_diff_chunk_list_writer_cow_and_append_optimize() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"ABCD".to_vec(), b"E".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("writer-diff.bin");
        let options = DiffChunkListWriterOptions::default().with_fixed_chunk_size(4);
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            options,
        )
        .await
        .unwrap();

        writer.seek(SeekFrom::Start(1)).unwrap();
        writer.write_all(b"Z").await.unwrap();
        writer.seek(SeekFrom::End(0)).unwrap();
        writer.write_all(b"FGH").await.unwrap();

        let (diff_chunk_list, merged_chunk_list) = writer.finalize(false).await.unwrap();
        assert_eq!(merged_chunk_list.body.len(), 2);
        assert_eq!(diff_chunk_list.chunk_indices, vec![0, 1]);
        assert!(diff_chunk_list.chunk_ids.is_none());

        let mut reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        let mut merged = Vec::new();
        reader.read_to_end(&mut merged).await.unwrap();
        assert_eq!(merged, b"AZCDEFGH".to_vec());
    }

    #[tokio::test]
    async fn test_diff_chunk_list_writer_close_then_reader_before_finalize() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"ABCD".to_vec(), b"E".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("writer-close-reader.bin");
        let options = DiffChunkListWriterOptions::default().with_fixed_chunk_size(4);
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            options,
        )
        .await
        .unwrap();

        writer.seek(SeekFrom::Start(1)).unwrap();
        writer.write_all(b"Z").await.unwrap();
        writer.seek(SeekFrom::End(0)).unwrap();
        writer.write_all(b"FGH").await.unwrap();

        let state = writer.close().await.unwrap();
        assert_eq!(state.chunk_indices, vec![0, 1]);
        assert_eq!(state.diff_chunk_sizes, vec![4, 4]);

        let loaded_state = DiffChunkListWriter::load_state(&diff_file_path)
            .await
            .unwrap();
        assert_eq!(loaded_state.chunk_indices, state.chunk_indices);
        assert_eq!(loaded_state.diff_chunk_sizes, state.diff_chunk_sizes);

        let mut reader = DiffChunkListReader::from_writer_state(
            store_mgr.clone(),
            clone_chunk_list(&base_chunk_list),
            &state,
            SeekFrom::Start(0),
            None,
        )
        .await
        .unwrap();
        let mut merged = Vec::new();
        reader.read_to_end(&mut merged).await.unwrap();
        assert_eq!(merged, b"AZCDEFGH".to_vec());

        let mut reader_by_file = DiffChunkListReader::from_writer_state_file(
            store_mgr.clone(),
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            SeekFrom::Start(0),
            None,
        )
        .await
        .unwrap();
        let mut merged2 = Vec::new();
        reader_by_file.read_to_end(&mut merged2).await.unwrap();
        assert_eq!(merged2, b"AZCDEFGH".to_vec());

        let (_diff_chunk_list, _merged_chunk_list) = writer.finalize(false).await.unwrap();
        let state_path = DiffChunkListWriter::state_file_path_for(&diff_file_path);
        assert!(!state_path.exists());
    }

    #[tokio::test]
    async fn test_rebuild_merged_state_from_writer_state() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"ABCD".to_vec(), b"E".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("writer-rebuild-state.bin");
        let options = DiffChunkListWriterOptions::default().with_fixed_chunk_size(4);
        let mut writer = DiffChunkListWriter::new(
            store_mgr,
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            options,
        )
        .await
        .unwrap();

        writer.seek(SeekFrom::Start(1)).unwrap();
        writer.write_all(b"Z").await.unwrap();
        writer.seek(SeekFrom::End(0)).unwrap();
        writer.write_all(b"FGH").await.unwrap();

        let state = writer.close().await.unwrap();
        let rebuilt = DiffChunkListWriter::rebuild_merged_state_from_writer_state(
            &base_chunk_list.body,
            &state,
        )
        .await
        .unwrap();

        assert_eq!(rebuilt.merged_chunk_list.body.len(), 2);
        assert_eq!(
            rebuilt.merged_chunk_list.body[0],
            calc_mix_chunk_id(b"AZCD")
        );
        assert_eq!(
            rebuilt.merged_chunk_list.body[1],
            calc_mix_chunk_id(b"EFGH")
        );
        assert_eq!(rebuilt.merged_chunk_sizes, vec![4, 4]);
        assert_eq!(rebuilt.dirty_chunks.len(), 2);
        assert_eq!(rebuilt.dirty_chunks[0].chunk_index, 0);
        assert_eq!(rebuilt.dirty_chunks[0].diff_file_offset, 0);
        assert_eq!(rebuilt.dirty_chunks[0].chunk_size, 4);
        assert_eq!(rebuilt.dirty_chunks[1].chunk_index, 1);
        assert_eq!(rebuilt.dirty_chunks[1].diff_file_offset, 4);
        assert_eq!(rebuilt.dirty_chunks[1].chunk_size, 4);
    }

    #[tokio::test]
    async fn test_diff_chunk_list_writer_reopen_from_state_and_continue() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"ABCD".to_vec(), b"E".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("writer-reopen.bin");
        let options = DiffChunkListWriterOptions::default().with_fixed_chunk_size(4);
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            options,
        )
        .await
        .unwrap();

        writer.seek(SeekFrom::Start(1)).unwrap();
        writer.write_all(b"Z").await.unwrap();
        let _state1 = writer.close().await.unwrap();

        let state = DiffChunkListWriter::load_state(&diff_file_path)
            .await
            .unwrap();
        let mut reopened = DiffChunkListWriter::open_from_state(
            store_mgr.clone(),
            clone_chunk_list(&base_chunk_list),
            state,
            None,
        )
        .await
        .unwrap();

        reopened.seek(SeekFrom::End(0)).unwrap();
        reopened.write_all(b"FGH").await.unwrap();
        let state2 = reopened.close().await.unwrap();
        assert_eq!(state2.chunk_indices, vec![0, 1]);

        let mut reader = DiffChunkListReader::from_writer_state(
            store_mgr.clone(),
            clone_chunk_list(&base_chunk_list),
            &state2,
            SeekFrom::Start(0),
            None,
        )
        .await
        .unwrap();
        let mut merged = Vec::new();
        reader.read_to_end(&mut merged).await.unwrap();
        assert_eq!(merged, b"AZCDEFGH".to_vec());

        let (diff_chunk_list, _merged_chunk_list) = reopened.finalize(false).await.unwrap();
        let mut reader_after_finalize = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();
        let mut merged_after_finalize = Vec::new();
        reader_after_finalize
            .read_to_end(&mut merged_after_finalize)
            .await
            .unwrap();
        assert_eq!(merged_after_finalize, b"AZCDEFGH".to_vec());
    }

    #[tokio::test]
    async fn test_reader_build_simple_chunk_list() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let base_chunks = vec![b"1111".to_vec(), b"2222".to_vec(), b"3333".to_vec()];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("build-list.bin");
        fs::write(&diff_file_path, b"ABCD").await.unwrap();
        let diff_chunk_list = DiffChunkList {
            base_chunk_list: base_chunk_list_id,
            diff_file_path,
            chunk_indices: vec![1],
            chunk_ids: None,
        };

        let reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        let merged = reader.build_simple_chunk_list(false).await.unwrap();
        assert_eq!(merged.body.len(), 3);
        assert_eq!(merged.body[0], base_chunk_list.body[0]);
        assert_eq!(merged.body[2], base_chunk_list.body[2]);
        assert_eq!(merged.body[1], calc_mix_chunk_id(b"ABCD"));
    }

    #[tokio::test]
    async fn test_overlay_single_dirty_chunk_compact_like() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_size = 1024usize;
        let base_chunks = vec![vec![b'x'; chunk_size]];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("overlay-single-dirty.bin");
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            DiffChunkListWriterOptions::default(),
        )
        .await
        .unwrap();

        writer.write_all(b"abc").await.unwrap();
        let (diff_chunk_list, _merged_chunk_list) = writer.finalize(true).await.unwrap();

        assert_eq!(diff_chunk_list.chunk_indices, vec![0]);
        let meta = fs::metadata(&diff_file_path).await.unwrap();
        assert_eq!(meta.len(), chunk_size as u64);

        let mut reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        let mut head = [0u8; 3];
        reader.read_exact(&mut head).await.unwrap();
        assert_eq!(&head, b"abc");

        let mut untouched = [0u8; 3];
        reader.read_exact(&mut untouched).await.unwrap();
        assert_eq!(&untouched, b"xxx");
    }

    #[tokio::test]
    async fn test_overlay_dirty_then_clean_read_across_chunks() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_size = 1024u64;
        let base_chunks = vec![
            vec![0x00; chunk_size as usize],
            vec![0x11; chunk_size as usize],
        ];
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();

        let diff_file_path = temp_dir.path().join("overlay-dirty-clean.bin");
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            DiffChunkListWriterOptions::default(),
        )
        .await
        .unwrap();

        writer.seek(SeekFrom::Start(10)).unwrap();
        writer.write_all(b"DIRTY").await.unwrap();
        let (diff_chunk_list, _merged_chunk_list) = writer.finalize(true).await.unwrap();

        assert_eq!(diff_chunk_list.chunk_indices, vec![0]);

        let mut reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        reader.seek(SeekFrom::Start(chunk_size - 4)).await.unwrap();
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..4], &[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(&buf[4..], &[0x11, 0x11, 0x11, 0x11]);
    }

    #[tokio::test]
    async fn test_overlay_complex_write_read() {
        let (temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_size = 4096u64;
        let mut base_chunks = Vec::new();
        for idx in 0..16u64 {
            let len = if idx == 15 {
                chunk_size - 256
            } else {
                chunk_size
            };
            base_chunks.push(vec![idx as u8; len as usize]);
        }
        let base_chunk_list = setup_base_chunk_list(&store, &base_chunks).await;
        let (base_chunk_list_id, _) = clone_chunk_list(&base_chunk_list).gen_obj_id();
        let base_size = chunk_size * 16 - 256;

        let diff_file_path = temp_dir.path().join("diff-complex-overlay.bin");
        let mut writer = DiffChunkListWriter::new(
            store_mgr.clone(),
            base_chunk_list_id,
            clone_chunk_list(&base_chunk_list),
            &diff_file_path,
            DiffChunkListWriterOptions::default(),
        )
        .await
        .unwrap();

        let chunk_middle = chunk_size / 2;
        writer
            .seek(SeekFrom::Start(5 * chunk_size + chunk_middle))
            .unwrap();
        writer.write_all(b"HELLO").await.unwrap();

        writer.seek(SeekFrom::Start(2 * chunk_size)).unwrap();
        writer.write_all(b"WORLD").await.unwrap();

        writer.seek(SeekFrom::Start(base_size)).unwrap();
        writer.write_all(&vec![0xffu8; 512]).await.unwrap();

        let (diff_chunk_list, _merged_chunk_list) = writer.finalize(true).await.unwrap();
        assert_eq!(diff_chunk_list.chunk_indices, vec![2, 5, 15, 16]);

        let mut reader = DiffChunkListReader::new(
            store_mgr,
            clone_chunk_list(&base_chunk_list),
            diff_chunk_list,
            SeekFrom::Start(0),
            false,
        )
        .await
        .unwrap();

        let mut head = vec![0u8; 256];
        reader.read_exact(&mut head).await.unwrap();
        assert!(head.iter().all(|b| *b == 0));

        reader.seek(SeekFrom::Start(2 * chunk_size)).await.unwrap();
        let mut world = [0u8; 5];
        reader.read_exact(&mut world).await.unwrap();
        assert_eq!(&world, b"WORLD");

        reader
            .seek(SeekFrom::Start(5 * chunk_size + chunk_middle))
            .await
            .unwrap();
        let mut hello = [0u8; 5];
        reader.read_exact(&mut hello).await.unwrap();
        assert_eq!(&hello, b"HELLO");

        reader.seek(SeekFrom::Start(base_size)).await.unwrap();
        let mut tail = vec![0u8; 512];
        reader.read_exact(&mut tail).await.unwrap();
        assert!(tail.iter().all(|b| *b == 0xff));

        let _ = fs::remove_file(&diff_file_path).await;
    }
}
