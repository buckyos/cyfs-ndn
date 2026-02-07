use crate::NamedStoreMgr;
use log::warn;
use ndn_lib::{ChunkId, ChunkReader, NdnError, NdnResult, SimpleChunkList};
use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

type LoadingFuture = Pin<Box<dyn Future<Output = std::io::Result<ChunkReader>> + Send + 'static>>;

pub type OpenChunkReaderFuture =
    Pin<Box<dyn Future<Output = NdnResult<(ChunkReader, u64)>> + Send + 'static>>;

pub type OpenChunkReader =
    Arc<dyn Fn(ChunkId, u64, bool) -> OpenChunkReaderFuture + Send + Sync + 'static>;

#[derive(Default)]
pub struct ChunkListReaderOptions {
    pub local_mode: bool,
    pub fixed_chunk_size: Option<u64>,
    pub chunk_sizes: Option<Vec<u64>>,
    pub open_chunk_reader: Option<OpenChunkReader>,
}

impl ChunkListReaderOptions {
    pub fn with_local_mode(mut self, local_mode: bool) -> Self {
        self.local_mode = local_mode;
        self
    }

    pub fn with_fixed_chunk_size(mut self, fixed_chunk_size: u64) -> Self {
        self.fixed_chunk_size = Some(fixed_chunk_size);
        self
    }

    pub fn with_chunk_sizes(mut self, chunk_sizes: Vec<u64>) -> Self {
        self.chunk_sizes = Some(chunk_sizes);
        self
    }

    pub fn with_open_chunk_reader(mut self, open_chunk_reader: OpenChunkReader) -> Self {
        self.open_chunk_reader = Some(open_chunk_reader);
        self
    }
}

#[derive(Clone)]
struct ChunkMeta {
    chunk_id: ChunkId,
    size: u64,
    start: u64,
}

pub struct ChunkListReader {
    named_store_mgr: Arc<NamedStoreMgr>,
    local_mode: bool,
    open_chunk_reader: Option<OpenChunkReader>,

    chunks: Vec<ChunkMeta>,
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

pub type SimpleChunkListReader = ChunkListReader;

impl ChunkListReader {
    pub async fn new(
        named_store_mgr: Arc<NamedStoreMgr>,
        chunk_list: SimpleChunkList,
        seek_from: SeekFrom,
    ) -> NdnResult<Self> {
        let options = ChunkListReaderOptions {
            ..Default::default()
        };
        Self::with_options(named_store_mgr, chunk_list, seek_from, options).await
    }

    pub async fn with_options(
        named_store_mgr: Arc<NamedStoreMgr>,
        chunk_list: SimpleChunkList,
        seek_from: SeekFrom,
        options: ChunkListReaderOptions,
    ) -> NdnResult<Self> {
        let chunk_count = chunk_list.body.len();
        let chunk_sizes =
            Self::resolve_chunk_sizes(&named_store_mgr, &chunk_list, &options).await?;
        if chunk_sizes.len() != chunk_count {
            return Err(NdnError::InvalidParam(format!(
                "chunk size count mismatch, expect {} got {}",
                chunk_count,
                chunk_sizes.len()
            )));
        }

        let (chunks, total_size) = Self::build_chunks(chunk_list.body, chunk_sizes)?;
        if chunk_list.total_size != 0 && chunk_list.total_size != total_size {
            warn!(
                "SimpleChunkList total_size mismatch: declared={} resolved={}",
                chunk_list.total_size, total_size
            );
        }

        let mut reader = Self {
            named_store_mgr,
            local_mode: options.local_mode,
            open_chunk_reader: options.open_chunk_reader,
            chunks,
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

        let seek_target = reader.calc_seek_target(seek_from)?;
        reader.apply_seek_target(seek_target);
        Ok(reader)
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn position(&self) -> u64 {
        self.position
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
        let (next_chunk_index, next_chunk_offset) = self.locate_position(position);

        self.next_chunk_index = next_chunk_index;
        self.next_chunk_offset = next_chunk_offset;
        self.active_chunk_index = None;
        self.pending_seek = None;
        self.current_reader = None;
        self.loading_future = None;
        self.loading_chunk_index = None;
    }

    fn locate_position(&self, position: u64) -> (usize, u64) {
        if position >= self.total_size || self.chunks.is_empty() {
            return (self.chunks.len(), 0);
        }

        let mut left = 0usize;
        let mut right = self.chunks.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let chunk = &self.chunks[mid];
            let end = chunk.start.saturating_add(chunk.size);
            if end <= position {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        let index = left;
        let offset = position.saturating_sub(self.chunks[index].start);
        (index, offset)
    }

    fn start_loading_current_chunk(&mut self) -> std::io::Result<()> {
        if self.next_chunk_index >= self.chunks.len() {
            return Ok(());
        }

        let chunk = self.chunks[self.next_chunk_index].clone();
        if self.next_chunk_offset > chunk.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "chunk offset {} exceeds chunk size {} for {}",
                    self.next_chunk_offset,
                    chunk.size,
                    chunk.chunk_id.to_base32()
                ),
            ));
        }

        let named_store_mgr = self.named_store_mgr.clone();
        let chunk_id = chunk.chunk_id;
        let offset = self.next_chunk_offset;
        let local_mode = self.local_mode;
        let open_chunk_reader = self.open_chunk_reader.clone();

        self.loading_chunk_index = Some(self.next_chunk_index);
        self.loading_future = Some(Box::pin(async move {
            Self::load_chunk_reader(
                named_store_mgr,
                chunk_id,
                offset,
                local_mode,
                open_chunk_reader,
            )
            .await
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

    async fn load_chunk_reader(
        named_store_mgr: Arc<NamedStoreMgr>,
        chunk_id: ChunkId,
        offset: u64,
        local_mode: bool,
        open_chunk_reader: Option<OpenChunkReader>,
    ) -> std::io::Result<ChunkReader> {
        match named_store_mgr.open_chunk_reader(&chunk_id, offset).await {
            Ok((reader, _)) => Ok(reader),
            Err(open_err) => {
                if local_mode {
                    return Err(Self::to_io_error(open_err));
                }

                let Some(custom_open_chunk_reader) = open_chunk_reader else {
                    return Err(Self::to_io_error(open_err));
                };

                warn!(
                    "open chunk {} from NamedStoreMgr failed, fallback to custom reader: {}",
                    chunk_id.to_base32(),
                    open_err
                );

                custom_open_chunk_reader(chunk_id, offset, false)
                    .await
                    .map(|(reader, _)| reader)
                    .map_err(Self::to_io_error)
            }
        }
    }

    fn resolve_mix_chunk_sizes(chunk_list: &SimpleChunkList) -> Option<Vec<u64>> {
        let mut sizes = Vec::with_capacity(chunk_list.body.len());
        for chunk_id in chunk_list.body.iter() {
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
                    "fixed chunk size {} cannot fit list total size {}",
                    fixed_chunk_size, chunk_list.total_size
                )));
            }

            let chunk_size = if index + 1 == chunk_list.body.len() {
                remaining
            } else {
                fixed_chunk_size.min(remaining)
            };

            sizes.push(chunk_size);
            remaining -= chunk_size;
        }

        if remaining != 0 {
            return Err(NdnError::InvalidParam(format!(
                "resolved fixed chunk sizes do not cover total size {}, remaining={}",
                chunk_list.total_size, remaining
            )));
        }

        Ok(sizes)
    }

    async fn resolve_chunk_sizes(
        named_store_mgr: &Arc<NamedStoreMgr>,
        chunk_list: &SimpleChunkList,
        options: &ChunkListReaderOptions,
    ) -> NdnResult<Vec<u64>> {
        if let Some(chunk_sizes) = options.chunk_sizes.clone() {
            if chunk_sizes.len() != chunk_list.body.len() {
                return Err(NdnError::InvalidParam(format!(
                    "chunk_sizes length mismatch, expect {} got {}",
                    chunk_list.body.len(),
                    chunk_sizes.len()
                )));
            }
            if options.local_mode {
                Self::ensure_chunks_available_in_local(
                    named_store_mgr,
                    chunk_list,
                    Some(&chunk_sizes),
                )
                .await?;
            }
            return Ok(chunk_sizes);
        }

        if options.local_mode {
            return Self::ensure_chunks_available_in_local(named_store_mgr, chunk_list, None).await;
        }

        if let Some(chunk_sizes) = Self::resolve_mix_chunk_sizes(chunk_list) {
            return Ok(chunk_sizes);
        }

        if let Some(fixed_chunk_size) = options.fixed_chunk_size {
            return Self::resolve_fixed_chunk_sizes(chunk_list, fixed_chunk_size);
        }

        Err(NdnError::Unsupported(
            "cannot resolve chunk sizes for seek: need mix chunk id, fixed_chunk_size, chunk_sizes, or local_mode"
                .to_string(),
        ))
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
                    "chunk {} missing in local NamedStoreMgr, state={}",
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

    fn build_chunks(
        chunk_ids: Vec<ChunkId>,
        chunk_sizes: Vec<u64>,
    ) -> NdnResult<(Vec<ChunkMeta>, u64)> {
        if chunk_ids.len() != chunk_sizes.len() {
            return Err(NdnError::InvalidParam(format!(
                "chunk count {} does not match chunk size count {}",
                chunk_ids.len(),
                chunk_sizes.len()
            )));
        }

        let mut chunks = Vec::with_capacity(chunk_ids.len());
        let mut start = 0u64;

        for (chunk_id, size) in chunk_ids.into_iter().zip(chunk_sizes.into_iter()) {
            chunks.push(ChunkMeta {
                chunk_id,
                size,
                start,
            });
            start = start.checked_add(size).ok_or_else(|| {
                NdnError::InvalidData("chunk list total size overflow".to_string())
            })?;
        }

        Ok((chunks, start))
    }

    fn to_io_error(err: NdnError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

impl AsyncRead for ChunkListReader {
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

            if this.position >= this.total_size || this.next_chunk_index >= this.chunks.len() {
                return Poll::Ready(Ok(()));
            }

            if let Err(err) = this.start_loading_current_chunk() {
                return Poll::Ready(Err(err));
            }
        }
    }
}

impl AsyncSeek for ChunkListReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        let target = this.calc_seek_target(position).map_err(Self::to_io_error)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NamedLocalStore, StoreLayout, StoreTarget};
    use ndn_lib::{ChunkHasher, ChunkType};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn calc_mix_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_chunk_id_from_bytes(data)
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

    #[tokio::test]
    async fn test_mix_chunk_list_read_and_seek() {
        let (_temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_a = b"hello".to_vec();
        let chunk_b = b"-chunk".to_vec();
        let chunk_c = b"-reader".to_vec();

        let chunk_ids = vec![
            calc_mix_chunk_id(&chunk_a),
            calc_mix_chunk_id(&chunk_b),
            calc_mix_chunk_id(&chunk_c),
        ];

        {
            let store = store.lock().await;
            store
                .put_chunk(&chunk_ids[0], &chunk_a, true)
                .await
                .unwrap();
            store
                .put_chunk(&chunk_ids[1], &chunk_b, true)
                .await
                .unwrap();
            store
                .put_chunk(&chunk_ids[2], &chunk_c, true)
                .await
                .unwrap();
        }

        let chunk_list = SimpleChunkList::from_chunk_list(chunk_ids).unwrap();
        let mut reader = ChunkListReader::new(store_mgr, chunk_list, SeekFrom::Start(0))
            .await
            .unwrap();

        let all = [chunk_a.clone(), chunk_b.clone(), chunk_c.clone()].concat();

        let mut all_read = Vec::new();
        reader.read_to_end(&mut all_read).await.unwrap();
        assert_eq!(all_read, all);

        reader.seek(SeekFrom::Start(3)).await.unwrap();
        let mut from_3 = Vec::new();
        reader.read_to_end(&mut from_3).await.unwrap();
        assert_eq!(from_3, all[3..].to_vec());

        reader.seek(SeekFrom::End(-4)).await.unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, all[all.len() - 4..].to_vec());
    }

    #[tokio::test]
    async fn test_local_mode_requires_all_chunks_in_named_store_mgr() {
        let (_temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_a = b"only-one".to_vec();
        let missing_chunk = b"missing".to_vec();

        let chunk_a_id = calc_mix_chunk_id(&chunk_a);
        let missing_chunk_id = calc_mix_chunk_id(&missing_chunk);

        {
            let store = store.lock().await;
            store.put_chunk(&chunk_a_id, &chunk_a, true).await.unwrap();
        }

        let chunk_list =
            SimpleChunkList::from_chunk_list(vec![chunk_a_id, missing_chunk_id]).unwrap();
        let options = ChunkListReaderOptions::default().with_local_mode(true);

        let err = ChunkListReader::with_options(store_mgr, chunk_list, SeekFrom::Start(0), options)
            .await
            .err()
            .expect("expect local-mode init failure");
        assert!(matches!(err, NdnError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_custom_open_chunk_reader_fallback() {
        let (_temp_main, store_mgr, main_store) = create_mgr_with_store("store-main").await;

        let backup_temp_dir = TempDir::new().unwrap();
        let backup_store_root = backup_temp_dir.path().join("store-backup");
        tokio::fs::create_dir_all(&backup_store_root).await.unwrap();
        let backup_store = Arc::new(tokio::sync::Mutex::new(
            NamedLocalStore::get_named_store_by_path(backup_store_root)
                .await
                .unwrap(),
        ));

        let in_main = b"in-main".to_vec();
        let in_backup = b"in-backup".to_vec();
        let in_main_id = calc_mix_chunk_id(&in_main);
        let in_backup_id = calc_mix_chunk_id(&in_backup);

        {
            let store = main_store.lock().await;
            store.put_chunk(&in_main_id, &in_main, true).await.unwrap();
        }
        {
            let store = backup_store.lock().await;
            store
                .put_chunk(&in_backup_id, &in_backup, true)
                .await
                .unwrap();
        }

        let fallback_store = backup_store.clone();
        let options = ChunkListReaderOptions::default().with_open_chunk_reader(Arc::new(
            move |chunk_id: ChunkId, offset: u64, _auto_cache: bool| {
                let fallback_store = fallback_store.clone();
                Box::pin(async move {
                    let store = fallback_store.lock().await;
                    store.open_chunk_reader(&chunk_id, offset).await
                })
            },
        ));

        let chunk_list =
            SimpleChunkList::from_chunk_list(vec![in_main_id.clone(), in_backup_id.clone()])
                .unwrap();
        let mut reader =
            ChunkListReader::with_options(store_mgr, chunk_list, SeekFrom::Start(0), options)
                .await
                .unwrap();

        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, [in_main, in_backup].concat());
    }

    #[tokio::test]
    async fn test_fixed_chunk_size_for_non_mix_chunk_list() {
        let (_temp_dir, store_mgr, store) = create_mgr_with_store("store-main").await;

        let chunk_a = b"1234".to_vec();
        let chunk_b = b"5678".to_vec();
        let chunk_c = b"90".to_vec();
        let fixed_chunk_size = 4u64;

        let chunk_a_id = calc_chunk_id(&chunk_a);
        let chunk_b_id = calc_chunk_id(&chunk_b);
        let chunk_c_id = calc_chunk_id(&chunk_c);

        assert!(!matches!(chunk_a_id.chunk_type, ChunkType::Mix256));

        {
            let store = store.lock().await;
            store.put_chunk(&chunk_a_id, &chunk_a, true).await.unwrap();
            store.put_chunk(&chunk_b_id, &chunk_b, true).await.unwrap();
            store.put_chunk(&chunk_c_id, &chunk_c, true).await.unwrap();
        }

        let total_size = (chunk_a.len() + chunk_b.len() + chunk_c.len()) as u64;
        let chunk_list = SimpleChunkList {
            total_size,
            body: vec![chunk_a_id, chunk_b_id, chunk_c_id],
        };
        let options = ChunkListReaderOptions::default().with_fixed_chunk_size(fixed_chunk_size);

        let mut reader =
            ChunkListReader::with_options(store_mgr, chunk_list, SeekFrom::Start(5), options)
                .await
                .unwrap();

        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, b"67890".to_vec());
    }
}
