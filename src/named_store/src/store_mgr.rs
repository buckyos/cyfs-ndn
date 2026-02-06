/// NamedStoreMgr manages multiple versions of StoreLayout for seamless data migration
///
/// During layout changes (e.g., adding/removing stores), objects may still exist
/// in locations determined by older layouts. This manager maintains up to 3 versions:
/// - versions[0]: current layout (newest)
/// - versions[1]: previous layout  
/// - versions[2]: oldest layout being migrated from
///
/// When getting an object:
/// 1. Try current layout first
/// 2. If NotFound, try previous layouts
/// 3. Return the first successful result or final error
use crate::{
    ChunkLocalInfo, ChunkStoreState, LayoutVersion, NamedLocalStore, ObjectState, StoreLayout,
};
use ndn_lib::{
    ChunkId, ChunkReader, ChunkWriter, FileObject, NdnError, NdnResult, ObjId, SimpleChunkList,
    OBJ_TYPE_FILE,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock;
use tokio::io::{AsyncRead, ReadBuf};

struct ConcatChunkReader {
    readers: Vec<ChunkReader>,
    current: usize,
}

impl ConcatChunkReader {
    fn new(readers: Vec<ChunkReader>) -> Self {
        Self { readers, current: 0 }
    }
}

impl AsyncRead for ConcatChunkReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let this = self.as_mut().get_mut();
            if this.current >= this.readers.len() {
                return Poll::Ready(Ok(()));
            }

            let before = buf.filled().len();
            let reader = this.readers.get_mut(this.current).expect("reader index");
            let mut pinned = Pin::new(reader);
            match pinned.as_mut().poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let after = buf.filled().len();
                    if after > before {
                        return Poll::Ready(Ok(()));
                    }
                    this.current += 1;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct NamedStoreMgr {
    /// Store layouts ordered by epoch (newest first)
    /// Maximum 3 versions: [current, previous, oldest]
    versions: RwLock<Vec<LayoutVersion>>,

    /// Store instances keyed by store_id
    stores: RwLock<HashMap<String, Arc<tokio::sync::Mutex<NamedLocalStore>>>>,

    /// Maximum number of layout versions to keep
    max_versions: usize,
}

impl NamedStoreMgr {
    /// Create a new NamedStoreMgr
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(Vec::new()),
            stores: RwLock::new(HashMap::new()),
            max_versions: 3,
        }
    }

    /// Create with custom max versions
    pub fn with_max_versions(max_versions: usize) -> Self {
        Self {
            versions: RwLock::new(Vec::new()),
            stores: RwLock::new(HashMap::new()),
            max_versions: max_versions.max(1),
        }
    }

    /// Register a store instance
    pub async fn register_store(&self, store: Arc<tokio::sync::Mutex<NamedLocalStore>>) {
        let store_id = {
            let guard = store.lock().await;
            guard.store_id().to_string()
        };
        let mut stores = self.stores.write().await;
        stores.insert(store_id, store);
    }

    /// Unregister a store instance
    pub async fn unregister_store(&self, store_id: &str) {
        let mut stores = self.stores.write().await;
        stores.remove(store_id);
    }

    /// Add a new layout version
    /// If epoch is newer than current, it becomes the new current version
    /// Old versions are kept up to max_versions limit
    pub async fn add_layout(&self, layout: StoreLayout) {
        let epoch = layout.epoch;
        let version = LayoutVersion { epoch, layout };

        let mut versions = self.versions.write().await;

        // Find insertion position (maintain descending epoch order)
        let pos = versions
            .iter()
            .position(|v| v.epoch < epoch)
            .unwrap_or(versions.len());

        // Check if this epoch already exists
        if versions.iter().any(|v| v.epoch == epoch) {
            // Replace existing version with same epoch
            if let Some(idx) = versions.iter().position(|v| v.epoch == epoch) {
                versions[idx] = version;
            }
        } else {
            versions.insert(pos, version);
        }

        // Trim to max_versions
        while versions.len() > self.max_versions {
            versions.pop();
        }
    }

    /// Get current layout (newest version)
    pub async fn current_layout(&self) -> Option<StoreLayout> {
        let versions = self.versions.read().await;
        versions.first().map(|v| v.layout.clone())
    }

    /// Get layout by epoch
    pub async fn get_layout(&self, epoch: u64) -> Option<StoreLayout> {
        let versions = self.versions.read().await;
        versions
            .iter()
            .find(|v| v.epoch == epoch)
            .map(|v| v.layout.clone())
    }

    /// Get all layout versions (newest first)
    pub async fn all_versions(&self) -> Vec<LayoutVersion> {
        let versions = self.versions.read().await;
        versions.clone()
    }

    /// Get object from stores, trying layouts from newest to oldest
    ///
    /// Algorithm:
    /// 1. For each layout version (newest first):
    ///    a. Use layout.select_primary_target(obj_id) to find target store
    ///    b. Get the store instance by store_id
    ///    c. Try store.get_object_impl(obj_id)
    ///    d. If found, return success
    ///    e. If NotFound, continue to next layout version
    /// 2. If all layouts exhausted, return NotFound
    pub async fn get_object(&self, obj_id: &ObjId) -> NdnResult<serde_json::Value> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Err(NdnError::NotFound(
                "no layout versions available".to_string(),
            ));
        }

        let mut last_error: Option<NdnError> = None;
        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            // Select target store from this layout version
            let target = match version.layout.select_primary_target(obj_id) {
                Some(t) => t,
                None => continue, // No target in this layout, try next
            };

            // Skip if we already tried this store
            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            // Get store instance
            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => {
                    last_error = Some(NdnError::NotFound(format!(
                        "store {} not registered",
                        target.store_id
                    )));
                    continue;
                }
            };

            // Try to get object from this store
            let store_guard = store.lock().await;
            match store_guard.get_object(obj_id, None).await {
                Ok(obj) => return Ok(obj),
                Err(NdnError::NotFound(_)) => {
                    // NotFound in this store, try next layout version
                    last_error = Some(NdnError::NotFound(format!(
                        "object not found in store {}",
                        target.store_id
                    )));
                    continue;
                }
                Err(e) => {
                    // Other error, still try next layout but record this error
                    last_error = Some(e);
                    continue;
                }
            }
        }

        // All layouts exhausted
        Err(last_error.unwrap_or_else(|| {
            NdnError::NotFound(format!(
                "object {:?} not found in any layout version",
                obj_id
            ))
        }))
    }

    /// Get object with extended fallback - try all targets in each layout
    ///
    /// More aggressive search: for each layout, try all possible targets
    /// (not just primary) before moving to next layout version
    pub async fn get_object_exhaustive(&self, obj_id: &ObjId) -> NdnResult<serde_json::Value> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Err(NdnError::NotFound(
                "no layout versions available".to_string(),
            ));
        }

        let mut last_error: Option<NdnError> = None;
        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            // Get all possible targets from this layout
            let targets = version.layout.select_targets(obj_id);

            for target in targets {
                // Skip if we already tried this store
                if tried_stores.contains(&target.store_id) {
                    continue;
                }
                tried_stores.push(target.store_id.clone());

                // Get store instance
                let store = match stores.get(&target.store_id) {
                    Some(s) => s,
                    None => continue,
                };

                // Try to get object from this store
                let store_guard = store.lock().await;
                match store_guard.get_object(obj_id, None).await {
                    Ok(obj) => return Ok(obj),
                    Err(NdnError::NotFound(_)) => continue,
                    Err(e) => {
                        last_error = Some(e);
                        continue;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            NdnError::NotFound(format!("object {:?} not found in any store", obj_id))
        }))
    }

    /// Select primary store for a new object (uses current layout)
    pub async fn select_store_for_write(
        &self,
        obj_id: &ObjId,
    ) -> Option<Arc<tokio::sync::Mutex<NamedLocalStore>>> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        let current = versions.first()?;
        let target = current.layout.select_primary_target(obj_id)?;
        stores.get(&target.store_id).cloned()
    }

    /// Get number of active layout versions
    pub async fn version_count(&self) -> usize {
        let versions = self.versions.read().await;
        versions.len()
    }

    /// Get current epoch
    pub async fn current_epoch(&self) -> Option<u64> {
        let versions = self.versions.read().await;
        versions.first().map(|v| v.epoch)
    }

    /// Remove old layout versions, keeping only the newest one
    pub async fn compact(&self) {
        let mut versions = self.versions.write().await;
        if versions.len() > 1 {
            versions.truncate(1);
        }
    }

    // ==================== Object Operations ====================

    /// Check if object exists (tries all layout versions)
    pub async fn is_object_exist(&self, obj_id: &ObjId) -> NdnResult<bool> {
        let obj_state = self.query_object_by_id(obj_id).await?;
        Ok(!matches!(obj_state, ObjectState::NotExist))
    }

    /// Query object state by id (tries all layout versions)
    pub async fn query_object_by_id(&self, obj_id: &ObjId) -> NdnResult<ObjectState> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Ok(ObjectState::NotExist);
        }

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => continue,
            };

            let store_guard = store.lock().await;
            let state = store_guard.query_object_by_id(obj_id).await?;
            if !matches!(state, ObjectState::NotExist) {
                return Ok(state);
            }
        }

        Ok(ObjectState::NotExist)
    }

    /// Put object to the appropriate store (uses current layout)
    pub async fn put_object(&self, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard.put_object(obj_id, obj_data).await
    }

    /// Remove object from all possible layout targets (best-effort)
    pub async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Ok(());
        }

        let mut tried_stores: HashSet<String> = HashSet::new();
        for version in versions.iter() {
            let target = match version.layout.select_primary_target(obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.insert(target.store_id.clone());

            if let Some(store) = stores.get(&target.store_id) {
                let store_guard = store.lock().await;
                let _ = store_guard.remove_object(obj_id).await;
            }
        }

        Ok(())
    }

    // ==================== Chunk State Operations ====================

    /// Check if chunk exists (tries all layout versions)
    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> bool {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return false;
        }

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => continue,
            };

            let store_guard = store.lock().await;
            if store_guard.have_chunk(chunk_id).await {
                return true;
            }
        }

        false
    }

    /// Query chunk state (tries all layout versions)
    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkStoreState, u64, String)> {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Ok((ChunkStoreState::NotExist, 0, String::new()));
        }

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => continue,
            };

            let store_guard = store.lock().await;
            let (state, size, progress) = store_guard.query_chunk_state(chunk_id).await?;
            if state != ChunkStoreState::NotExist {
                return Ok((state, size, progress));
            }
        }

        Ok((ChunkStoreState::NotExist, 0, String::new()))
    }

    // ==================== Chunk Read Operations ====================

    /// Open chunk reader (tries all layout versions)
    pub async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Err(NdnError::NotFound(
                "no layout versions available".to_string(),
            ));
        }

        let mut last_error: Option<NdnError> = None;
        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => {
                    last_error = Some(NdnError::NotFound(format!(
                        "store {} not registered",
                        target.store_id
                    )));
                    continue;
                }
            };

            let store_guard = store.lock().await;
            match store_guard
                .open_chunk_reader(chunk_id, offset, auto_cache)
                .await
            {
                Ok(result) => return Ok(result),
                Err(NdnError::NotFound(_)) | Err(NdnError::InComplete(_)) => {
                    last_error = Some(NdnError::NotFound(format!(
                        "chunk not found in store {}",
                        target.store_id
                    )));
                    continue;
                }
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            NdnError::NotFound(format!(
                "chunk {} not found in any store",
                chunk_id.to_string()
            ))
        }))
    }

    /// Open chunklist reader by chunklist object id.
    pub async fn open_chunklist_reader(
        &self,
        chunklist_id: &ObjId,
        offset: u64,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        if !chunklist_id.is_chunk_list() {
            return Err(NdnError::InvalidObjType(format!(
                "{} is not chunklist",
                chunklist_id.to_string()
            )));
        }

        let chunklist_json = self.get_object(chunklist_id).await?;
        let chunk_list = SimpleChunkList::from_json_value(chunklist_json)
            .map_err(|e| NdnError::DecodeError(format!("parse chunklist failed: {}", e)))?;

        if offset > chunk_list.total_size {
            return Err(NdnError::OffsetTooLarge(format!(
                "{} offset {} > total {}",
                chunklist_id.to_string(),
                offset,
                chunk_list.total_size
            )));
        }

        let mut remain_offset = offset;
        let mut readers: Vec<ChunkReader> = Vec::new();
        for chunk_id in chunk_list.body.iter() {
            let chunk_size = chunk_id.get_length().ok_or_else(|| {
                NdnError::InvalidData(format!(
                    "chunk {} has no encoded length in chunklist {}",
                    chunk_id.to_string(),
                    chunklist_id.to_string()
                ))
            })?;

            if remain_offset >= chunk_size {
                remain_offset -= chunk_size;
                continue;
            }

            let chunk_offset = remain_offset;
            remain_offset = 0;
            let (reader, _) = self
                .open_chunk_reader(chunk_id, chunk_offset, auto_cache)
                .await?;
            readers.push(reader);
        }

        if readers.is_empty() {
            return Ok((Box::pin(tokio::io::empty()), chunk_list.total_size));
        }

        Ok((
            Box::pin(ConcatChunkReader::new(readers)),
            chunk_list.total_size,
        ))
    }

    /// Compatibility alias for typo-ed API name.
    pub async fn open_chunklist_rader(
        &self,
        chunklist_id: &ObjId,
        offset: u64,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        self.open_chunklist_reader(chunklist_id, offset, auto_cache)
            .await
    }

    /// Open a generic reader by object id and optional inner path.
    pub async fn open_reader(
        &self,
        obj_id: &ObjId,
        inner_obj_path: Option<String>,
        auto_cache: bool,
    ) -> NdnResult<(ChunkReader, u64)> {
        let mut current_obj_id = obj_id.clone();
        let mut current_inner_path = inner_obj_path;
        let mut size_override: Option<u64> = None;

        loop {
            if current_obj_id.is_chunk() {
                if current_inner_path.is_some() {
                    return Err(NdnError::InvalidObjType(format!(
                        "{} is chunk, no inner_obj_path",
                        current_obj_id.to_string()
                    )));
                }

                let chunk_id = ChunkId::from_obj_id(&current_obj_id);
                let (reader, size) = self.open_chunk_reader(&chunk_id, 0, auto_cache).await?;
                return Ok((reader, size_override.unwrap_or(size)));
            }

            if current_obj_id.is_chunk_list() {
                if current_inner_path.is_some() {
                    return Err(NdnError::InvalidObjType(format!(
                        "{} is chunklist, no inner_obj_path",
                        current_obj_id.to_string()
                    )));
                }

                let (reader, size) = self
                    .open_chunklist_reader(&current_obj_id, 0, auto_cache)
                    .await?;
                return Ok((reader, size_override.unwrap_or(size)));
            }

            if current_obj_id.obj_type == OBJ_TYPE_FILE && current_inner_path.is_none() {
                let obj_json = self.get_object(&current_obj_id).await?;
                let file_obj: FileObject = serde_json::from_value(obj_json)
                    .map_err(|e| NdnError::DecodeError(e.to_string()))?;
                if file_obj.content.is_empty() {
                    return Err(NdnError::InvalidData(format!(
                        "file {} content is empty",
                        current_obj_id.to_string()
                    )));
                }
                if file_obj.size > 0 {
                    size_override = Some(file_obj.size);
                }
                current_obj_id = ObjId::new(file_obj.content.as_str())?;
                continue;
            }

            let inner_path = current_inner_path.take().ok_or_else(|| {
                NdnError::InvalidObjType(format!(
                    "{} is object, can't open reader without inner_obj_path",
                    current_obj_id.to_string()
                ))
            })?;
            let obj_json = self.get_object(&current_obj_id).await?;
            let path_to = Self::extract_json_by_path(&obj_json, inner_path.as_str())?;
            current_obj_id = Self::value_to_obj_id(&path_to)?;
        }
    }

    /// Remove chunk from all possible layout targets (best-effort)
    pub async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Ok(());
        }

        let mut tried_stores: HashSet<String> = HashSet::new();
        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.insert(target.store_id.clone());

            if let Some(store) = stores.get(&target.store_id) {
                let store_guard = store.lock().await;
                let _ = store_guard.remove_chunk(chunk_id).await;
            }
        }

        Ok(())
    }

    /// Get chunk data (tries all layout versions)
    pub async fn get_chunk_data(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
        let (mut chunk_reader, chunk_size) = self.open_chunk_reader(chunk_id, 0, false).await?;
        let mut buffer = Vec::with_capacity(chunk_size as usize);
        use tokio::io::AsyncReadExt;
        chunk_reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| NdnError::IoError(format!("read chunk data failed: {}", e)))?;
        Ok(buffer)
    }

    /// Get chunk piece (tries all layout versions)
    pub async fn get_chunk_piece(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        piece_size: u32,
    ) -> NdnResult<Vec<u8>> {
        let (mut reader, chunk_size) = self.open_chunk_reader(chunk_id, offset, false).await?;
        if offset > chunk_size {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        let mut buffer = vec![0u8; piece_size as usize];
        use tokio::io::AsyncReadExt;
        reader
            .read_exact(&mut buffer)
            .await
            .map_err(|e| NdnError::IoError(format!("read chunk piece failed: {}", e)))?;
        Ok(buffer)
    }

    // ==================== Chunk Write Operations ====================

    /// Open chunk writer (uses current layout for write target)
    pub async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        offset: u64,
    ) -> NdnResult<(ChunkWriter, String)> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard
            .open_chunk_writer(chunk_id, chunk_size, offset)
            .await
    }

    /// Open new chunk writer (uses current layout for write target)
    pub async fn open_new_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
    ) -> NdnResult<ChunkWriter> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard
            .open_new_chunk_writer(chunk_id, chunk_size)
            .await
    }

    /// Update chunk progress (needs to find the correct store first)
    /// This operation finds the store where chunk is being written
    pub async fn update_chunk_progress(
        &self,
        chunk_id: &ChunkId,
        progress: String,
    ) -> NdnResult<()> {
        // First query chunk state to find which store has it
        let (state, _, _) = self.query_chunk_state(chunk_id).await?;
        if state == ChunkStoreState::NotExist {
            return Err(NdnError::NotFound(format!(
                "chunk {} not found",
                chunk_id.to_string()
            )));
        }

        // Get the store that has this chunk
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => continue,
            };

            let store_guard = store.lock().await;
            let (chunk_state, _, _) = store_guard.query_chunk_state(chunk_id).await?;
            if chunk_state == ChunkStoreState::Incompleted {
                return store_guard
                    .update_chunk_progress(chunk_id, progress)
                    .await;
            }
        }

        Err(NdnError::NotFound(format!(
            "chunk {} not in incompleted state",
            chunk_id.to_string()
        )))
    }

    /// Complete chunk writer (needs to find the correct store first)
    pub async fn complete_chunk_writer(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(&obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            let store = match stores.get(&target.store_id) {
                Some(s) => s,
                None => continue,
            };

            let store_guard = store.lock().await;
            let (chunk_state, _, _) = store_guard.query_chunk_state(chunk_id).await?;
            if chunk_state == ChunkStoreState::Incompleted {
                return store_guard.complete_chunk_writer(chunk_id).await;
            }
        }

        Err(NdnError::NotFound(format!(
            "chunk {} not in incompleted state",
            chunk_id.to_string()
        )))
    }

    /// Put chunk by reader (uses current layout for write target)
    pub async fn put_chunk_by_reader(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        reader: &mut ChunkReader,
    ) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard
            .put_chunk_by_reader(chunk_id, chunk_size, reader)
            .await
    }

    /// Put chunk data (uses current layout for write target)
    pub async fn put_chunk(
        &self,
        chunk_id: &ChunkId,
        chunk_data: &[u8],
        need_verify: bool,
    ) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard
            .put_chunk(chunk_id, chunk_data, need_verify)
            .await
    }

    /// Add chunk by link to local file (uses current layout for write target)
    pub async fn add_chunk_by_link_to_local_file(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        chunk_local_info: &ChunkLocalInfo,
    ) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard
            .add_chunk_by_link_to_local_file(chunk_id, chunk_size, chunk_local_info)
            .await
    }

    // ==================== Helper Methods ====================

    /// Get store by store_id
    pub async fn get_store(
        &self,
        store_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<NamedLocalStore>>> {
        let stores = self.stores.read().await;
        stores.get(store_id).cloned()
    }

    /// Get all registered store ids
    pub async fn get_store_ids(&self) -> Vec<String> {
        let stores = self.stores.read().await;
        stores.keys().cloned().collect()
    }

    /// Select store for an object (read operation - tries all layout versions)
    /// Returns the first store that has the object
    pub async fn select_store_for_read(
        &self,
        obj_id: &ObjId,
    ) -> Option<Arc<tokio::sync::Mutex<NamedLocalStore>>> {
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        let mut tried_stores: Vec<String> = Vec::new();

        for version in versions.iter() {
            let target = match version.layout.select_primary_target(obj_id) {
                Some(t) => t,
                None => continue,
            };

            if tried_stores.contains(&target.store_id) {
                continue;
            }
            tried_stores.push(target.store_id.clone());

            if let Some(store) = stores.get(&target.store_id) {
                let store_guard = store.lock().await;
                let state = store_guard.query_object_by_id(obj_id).await.ok()?;
                if !matches!(state, ObjectState::NotExist) {
                    drop(store_guard);
                    return Some(store.clone());
                }
            }
        }

        None
    }

    fn value_to_obj_id(value: &Value) -> NdnResult<ObjId> {
        match value {
            Value::String(v) => ObjId::new(v),
            Value::Object(map) => {
                if let Some(Value::String(v)) = map.get("obj_id") {
                    return ObjId::new(v);
                }

                if let Ok(obj_id) = serde_json::from_value::<ObjId>(value.clone()) {
                    return Ok(obj_id);
                }

                Err(NdnError::InvalidParam(format!(
                    "cannot convert object value to ObjId: {}",
                    value
                )))
            }
            _ => Err(NdnError::InvalidParam(format!(
                "cannot convert value to ObjId: {}",
                value
            ))),
        }
    }

    fn extract_json_by_path(value: &Value, path: &str) -> NdnResult<Value> {
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
                    list.get(index)
                        .ok_or_else(|| NdnError::NotFound(format!("inner path not found: {}", path)))?
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
}

impl Default for NamedStoreMgr {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreTarget;

    fn create_test_target(
        store_id: &str,
        weight: u32,
        enabled: bool,
        readonly: bool,
    ) -> StoreTarget {
        StoreTarget {
            store_id: store_id.to_string(),
            device_did: None,
            capacity: Some(1000),
            used: Some(100),
            readonly,
            enabled,
            weight,
        }
    }

    fn create_layout_with_epoch(epoch: u64, targets: Vec<StoreTarget>) -> StoreLayout {
        StoreLayout::new(epoch, targets, 10000, 1000)
    }

    #[tokio::test]
    async fn test_store_layout_mgr_basic() {
        let mgr = NamedStoreMgr::new();

        // Initially empty
        assert_eq!(mgr.version_count().await, 0);
        assert!(mgr.current_layout().await.is_none());
        assert!(mgr.current_epoch().await.is_none());
    }

    #[tokio::test]
    async fn test_store_layout_mgr_add_versions() {
        let mgr = NamedStoreMgr::new();

        let targets1 = vec![create_test_target("store1", 1, true, false)];
        let layout1 = create_layout_with_epoch(1, targets1);
        mgr.add_layout(layout1).await;

        assert_eq!(mgr.version_count().await, 1);
        assert_eq!(mgr.current_epoch().await, Some(1));

        // Add newer version
        let targets2 = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
        ];
        let layout2 = create_layout_with_epoch(2, targets2);
        mgr.add_layout(layout2).await;

        assert_eq!(mgr.version_count().await, 2);
        assert_eq!(mgr.current_epoch().await, Some(2));

        // Add even newer version
        let targets3 = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout3 = create_layout_with_epoch(3, targets3);
        mgr.add_layout(layout3).await;

        assert_eq!(mgr.version_count().await, 3);
        assert_eq!(mgr.current_epoch().await, Some(3));

        // Adding a 4th version should trim the oldest
        let targets4 = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
            create_test_target("store4", 1, true, false),
        ];
        let layout4 = create_layout_with_epoch(4, targets4);
        mgr.add_layout(layout4).await;

        assert_eq!(mgr.version_count().await, 3); // Still 3, oldest trimmed
        assert_eq!(mgr.current_epoch().await, Some(4));

        // Verify version 1 is gone
        assert!(mgr.get_layout(1).await.is_none());
        assert!(mgr.get_layout(2).await.is_some());
        assert!(mgr.get_layout(3).await.is_some());
        assert!(mgr.get_layout(4).await.is_some());
    }

    #[tokio::test]
    async fn test_store_layout_mgr_version_ordering() {
        let mgr = NamedStoreMgr::new();

        // Add versions out of order
        let targets2 = vec![create_test_target("store1", 1, true, false)];
        let layout2 = create_layout_with_epoch(2, targets2);
        mgr.add_layout(layout2).await;

        let targets1 = vec![create_test_target("store1", 1, true, false)];
        let layout1 = create_layout_with_epoch(1, targets1);
        mgr.add_layout(layout1).await;

        let targets3 = vec![create_test_target("store1", 1, true, false)];
        let layout3 = create_layout_with_epoch(3, targets3);
        mgr.add_layout(layout3).await;

        // Current should be the newest (epoch 3)
        assert_eq!(mgr.current_epoch().await, Some(3));

        // Versions should be ordered newest first
        let versions = mgr.all_versions().await;
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].epoch, 3);
        assert_eq!(versions[1].epoch, 2);
        assert_eq!(versions[2].epoch, 1);
    }

    #[tokio::test]
    async fn test_store_layout_mgr_compact() {
        let mgr = NamedStoreMgr::new();

        for epoch in 1..=3 {
            let targets = vec![create_test_target("store1", 1, true, false)];
            let layout = create_layout_with_epoch(epoch, targets);
            mgr.add_layout(layout).await;
        }

        assert_eq!(mgr.version_count().await, 3);

        mgr.compact().await;

        assert_eq!(mgr.version_count().await, 1);
        assert_eq!(mgr.current_epoch().await, Some(3));
    }

    #[tokio::test]
    async fn test_store_layout_mgr_custom_max_versions() {
        let mgr = NamedStoreMgr::with_max_versions(2);

        for epoch in 1..=5 {
            let targets = vec![create_test_target("store1", 1, true, false)];
            let layout = create_layout_with_epoch(epoch, targets);
            mgr.add_layout(layout).await;
        }

        // Should only keep 2 versions
        assert_eq!(mgr.version_count().await, 2);
        assert_eq!(mgr.current_epoch().await, Some(5));

        // Only epochs 4 and 5 should exist
        assert!(mgr.get_layout(3).await.is_none());
        assert!(mgr.get_layout(4).await.is_some());
        assert!(mgr.get_layout(5).await.is_some());
    }

    #[tokio::test]
    async fn test_store_layout_mgr_replace_same_epoch() {
        let mgr = NamedStoreMgr::new();

        let targets1 = vec![create_test_target("store1", 1, true, false)];
        let layout1 = create_layout_with_epoch(1, targets1);
        mgr.add_layout(layout1).await;

        assert_eq!(mgr.version_count().await, 1);

        // Add layout with same epoch should replace
        let targets1_updated = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
        ];
        let layout1_updated = create_layout_with_epoch(1, targets1_updated);
        mgr.add_layout(layout1_updated).await;

        // Should still be 1 version, not 2
        assert_eq!(mgr.version_count().await, 1);

        // The updated layout should have 2 targets
        let current = mgr.current_layout().await.unwrap();
        assert_eq!(current.targets.len(), 2);
    }
}
