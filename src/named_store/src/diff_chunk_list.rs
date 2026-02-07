use crate::{chunk_list_reader::OpenChunkReader, NamedStoreMgr};
use ndn_lib::{ChunkHasher, ChunkId, ChunkReader, NdnError, NdnResult, ObjId, SimpleChunkList};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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

    pub async fn finalize(self, named_mode: bool) -> NdnResult<(DiffChunkList, SimpleChunkList)> {
        if let Some(parent) = self.diff_file_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
        }

        let mut file = File::create(&self.diff_file_path)
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;

        let mut dirty_chunk_id_map: HashMap<usize, ChunkId> = HashMap::new();
        let mut chunk_indices = Vec::new();
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
        }

        file.flush()
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;

        let mut merged_chunk_ids = Vec::with_capacity(self.merged_chunk_sizes.len());
        for index in 0..self.merged_chunk_sizes.len() {
            if let Some(chunk_id) = dirty_chunk_id_map.get(&index) {
                merged_chunk_ids.push(chunk_id.clone());
                continue;
            }

            if let Some(base_chunk_id) = self.base_chunk_ids.get(index) {
                merged_chunk_ids.push(base_chunk_id.clone());
                continue;
            }

            return Err(NdnError::InvalidData(format!(
                "chunk index {} has no chunk id (not in base and not dirty)",
                index
            )));
        }

        let merged_chunk_list = SimpleChunkList::from_chunk_list(merged_chunk_ids)?;
        let diff_chunk_list = DiffChunkList {
            base_chunk_list: self.base_chunk_list_id,
            diff_file_path: self.diff_file_path,
            chunk_indices,
            chunk_ids: if named_mode {
                Some(diff_chunk_ids)
            } else {
                None
            },
        };

        Ok((diff_chunk_list, merged_chunk_list))
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

async fn open_store_chunk_reader_with_fallback(
    named_store_mgr: Arc<NamedStoreMgr>,
    chunk_id: ChunkId,
    offset: u64,
    auto_cache: bool,
    local_mode: bool,
    open_chunk_reader: Option<OpenChunkReader>,
) -> NdnResult<ChunkReader> {
    match named_store_mgr
        .open_chunk_reader(&chunk_id, offset)
        .await
    {
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
}
