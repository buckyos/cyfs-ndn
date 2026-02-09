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
    ChunkLocalInfo, ChunkStoreState, LayoutVersion, NamedLocalStore, ObjectState,
    SimpleChunkListReader, StoreLayout,
};
use ndn_lib::{
    extract_objid_by_path, load_named_obj, load_named_object_from_obj_str, ChunkId, ChunkReader,
    ChunkWriter, DirObject, FileObject, NdnError, NdnResult, ObjId, SimpleChunkList, SimpleMapItem,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

const DEFAULT_RESOLVE_NEXT_OBJ_CACHE_MAX_ENTRIES: usize = 10000;
type ResolveNextObjCacheEntryId = u64;

#[derive(Clone, Hash, Eq, PartialEq)]
struct ResolveNextObjCacheKey {
    obj_id: ObjId,
    path: String,
}

#[derive(Clone)]
struct ResolveNextObjCacheValue {
    next_obj_id: ObjId,
    next_path: Option<String>,
    next_obj_str: Option<String>,
}

struct ResolveNextObjCacheEntry {
    value: ResolveNextObjCacheValue,
    entry_id: ResolveNextObjCacheEntryId,
}

struct ResolveNextObjCache {
    entries: HashMap<ResolveNextObjCacheKey, ResolveNextObjCacheEntry>,
    lru_order: BTreeMap<ResolveNextObjCacheEntryId, ResolveNextObjCacheKey>,
    next_entry_id: ResolveNextObjCacheEntryId,
    max_entries: usize,
}

impl ResolveNextObjCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru_order: BTreeMap::new(),
            next_entry_id: 0,
            max_entries,
        }
    }

    fn get(&mut self, obj_id: &ObjId, path: &str) -> Option<ResolveNextObjCacheValue> {
        let key = ResolveNextObjCacheKey {
            obj_id: obj_id.clone(),
            path: path.to_string(),
        };

        let entry = self.entries.get_mut(&key)?;
        self.lru_order.remove(&entry.entry_id);

        let new_entry_id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.wrapping_add(1);
        self.lru_order.insert(new_entry_id, key);
        entry.entry_id = new_entry_id;

        Some(entry.value.clone())
    }

    fn put(&mut self, obj_id: &ObjId, path: &str, value: ResolveNextObjCacheValue) {
        if self.max_entries == 0 {
            return;
        }

        let key = ResolveNextObjCacheKey {
            obj_id: obj_id.clone(),
            path: path.to_string(),
        };

        if let Some(old) = self.entries.remove(&key) {
            self.lru_order.remove(&old.entry_id);
        }

        let entry_id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.wrapping_add(1);
        self.lru_order.insert(entry_id, key.clone());
        self.entries
            .insert(key, ResolveNextObjCacheEntry { value, entry_id });

        while self.entries.len() > self.max_entries {
            let Some((oldest_entry_id, oldest_key)) = self
                .lru_order
                .iter()
                .next()
                .map(|(id, key)| (*id, key.clone()))
            else {
                break;
            };

            self.lru_order.remove(&oldest_entry_id);
            self.entries.remove(&oldest_key);
        }
    }
}

#[derive(Clone)]
pub struct NamedStoreMgr {
    /// Store layouts ordered by epoch (newest first)
    /// Maximum 3 versions: [current, previous, oldest]
    versions: Arc<RwLock<Vec<LayoutVersion>>>,

    /// Store instances keyed by store_id
    stores: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<NamedLocalStore>>>>>,

    /// Maximum number of layout versions to keep
    max_versions: usize,

    /// Cache for resolve_next_obj results by (obj_id, inner_path).
    resolve_next_obj_cache: Arc<Mutex<ResolveNextObjCache>>,
}

impl NamedStoreMgr {
    /// Create a new NamedStoreMgr
    pub fn new() -> Self {
        Self {
            versions: Arc::new(RwLock::new(Vec::new())),
            stores: Arc::new(RwLock::new(HashMap::new())),
            max_versions: 3,
            resolve_next_obj_cache: Arc::new(Mutex::new(ResolveNextObjCache::new(
                DEFAULT_RESOLVE_NEXT_OBJ_CACHE_MAX_ENTRIES,
            ))),
        }
    }

    /// Create with custom max versions
    pub fn with_max_versions(max_versions: usize) -> Self {
        Self {
            versions: Arc::new(RwLock::new(Vec::new())),
            stores: Arc::new(RwLock::new(HashMap::new())),
            max_versions: max_versions.max(1),
            resolve_next_obj_cache: Arc::new(Mutex::new(ResolveNextObjCache::new(
                DEFAULT_RESOLVE_NEXT_OBJ_CACHE_MAX_ENTRIES,
            ))),
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
    pub async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
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
            match store_guard.get_object(obj_id).await {
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

    pub async fn open_object(
        &self,
        obj_id: &ObjId,
        inner_path: Option<String>,
    ) -> NdnResult<String> {
        let mut current_obj_id = obj_id.clone();
        let mut current_path = Self::normalize_inner_path(inner_path);
        let mut current_obj_str: Option<String> = None;

        loop {
            if current_obj_id.is_chunk() {
                return Err(NdnError::InvalidObjType(format!(
                    "{} is chunk",
                    current_obj_id.to_string()
                )));
            }

            let obj_str = match current_obj_str.take() {
                Some(obj_str) => obj_str,
                None => self.get_object(&current_obj_id).await?,
            };

            if current_path.is_none() {
                return Ok(obj_str);
            }

            let path = current_path.as_ref().unwrap().as_str();
            let (next_obj_id, next_path, next_obj_str) = self
                .resolve_next_obj(&current_obj_id, obj_str.as_str(), path)
                .await?;
            current_obj_id = next_obj_id;
            current_path = next_path;
            current_obj_str = next_obj_str;
        }
    }

    /// Resolve one child from a DirObject by name.
    ///
    /// For embedded child objects (Object/ObjectJwt), this method persists the
    /// generated object data into store so later lookups by `obj_id` can use
    /// regular `get_object` APIs.
    pub async fn get_dir_child(&self, dir_obj_id: &ObjId, item_name: &str) -> NdnResult<ObjId> {
        if !dir_obj_id.is_dir_object() {
            return Err(NdnError::InvalidObjType("must be dirobject".to_string()));
        }

        let dir_obj_str = self.get_object(dir_obj_id).await?;
        let dir_obj: DirObject = load_named_obj(dir_obj_str.as_str())?;
        let item = dir_obj.get(item_name).ok_or_else(|| {
            NdnError::NotFound(format!(
                "child {} not found in dir {}",
                item_name, dir_obj_id
            ))
        })?;
        let (obj_id, obj_str) = item.get_obj_id()?;
        if !obj_str.is_empty() {
            self.put_object(&obj_id, obj_str.as_str()).await?;
        }
        Ok(obj_id)
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
            match store_guard.open_chunk_reader(chunk_id, offset).await {
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
    ) -> NdnResult<(ChunkReader, u64)> {
        if !chunklist_id.is_chunk_list() {
            return Err(NdnError::InvalidObjType(format!(
                "{} is not chunklist",
                chunklist_id.to_string()
            )));
        }

        let chunklist_json = self.get_object(chunklist_id).await?;
        let vec_chunk_id: Vec<ChunkId> = load_named_obj(chunklist_json.as_str())?;
        let chunk_list = SimpleChunkList::from_chunk_list(vec_chunk_id)?;

        let total_size = chunk_list.total_size;
        let reader = SimpleChunkListReader::new(
            Arc::new(self.clone()),
            chunk_list,
            std::io::SeekFrom::Start(offset),
        )
        .await?;

        Ok((Box::pin(reader), total_size))
    }

    async fn open_chunklist_reader_by_obj_str(
        &self,
        chunklist_json: &str,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let vec_chunk_id: Vec<ChunkId> = load_named_obj(chunklist_json)?;
        let chunk_list = SimpleChunkList::from_chunk_list(vec_chunk_id)?;
        let total_size = chunk_list.total_size;
        let reader = SimpleChunkListReader::new(
            Arc::new(self.clone()),
            chunk_list,
            std::io::SeekFrom::Start(offset),
        )
        .await?;
        Ok((Box::pin(reader), total_size))
    }

    /// Open a generic reader by object id and optional inner path.
    pub async fn open_reader(
        &self,
        obj_id: &ObjId,
        inner_path: Option<String>,
    ) -> NdnResult<(ChunkReader, u64)> {
        let mut current_obj_id = obj_id.clone();
        let mut current_path = Self::normalize_inner_path(inner_path);
        let mut current_obj_str: Option<String> = None;

        loop {
            if current_path.is_none() {
                if current_obj_id.is_chunk() {
                    let chunk_id = ChunkId::from_obj_id(&current_obj_id);
                    return self.open_chunk_reader(&chunk_id, 0).await;
                }

                if current_obj_id.is_chunk_list() {
                    if let Some(obj_str) = current_obj_str.take() {
                        return self
                            .open_chunklist_reader_by_obj_str(obj_str.as_str(), 0)
                            .await;
                    }
                    return self.open_chunklist_reader(&current_obj_id, 0).await;
                }

                if current_obj_id.is_file_object() {
                    let obj_str = match current_obj_str.take() {
                        Some(obj_str) => obj_str,
                        None => self.get_object(&current_obj_id).await?,
                    };
                    let file_obj: FileObject = load_named_obj(obj_str.as_str())?;
                    let content_obj_id = ObjId::new(file_obj.content.as_str())?;
                    if content_obj_id.is_chunk() {
                        let chunk_id = ChunkId::from_obj_id(&content_obj_id);
                        return self.open_chunk_reader(&chunk_id, 0).await;
                    }
                    if content_obj_id.is_chunk_list() {
                        return self.open_chunklist_reader(&content_obj_id, 0).await;
                    }
                    return Err(NdnError::InvalidObjType(format!(
                        "file object content {} is not chunk or chunklist",
                        content_obj_id.to_string()
                    )));
                }

                return Err(NdnError::InvalidObjType(format!(
                    "{} does not support open_reader",
                    current_obj_id.to_string()
                )));
            }

            if current_obj_id.is_chunk() {
                return Err(NdnError::InvalidParam(format!(
                    "chunk {} does not support inner path",
                    current_obj_id.to_string()
                )));
            }

            let obj_str = match current_obj_str.take() {
                Some(obj_str) => obj_str,
                None => self.get_object(&current_obj_id).await?,
            };

            let path = current_path.as_ref().unwrap().as_str();
            let (next_obj_id, next_path, next_obj_str) = self
                .resolve_next_obj(&current_obj_id, obj_str.as_str(), path)
                .await?;
            current_obj_id = next_obj_id;
            current_path = next_path;
            current_obj_str = next_obj_str;
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
        let (mut chunk_reader, chunk_size) = self.open_chunk_reader(chunk_id, 0).await?;
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
        let (mut reader, chunk_size) = self.open_chunk_reader(chunk_id, offset).await?;
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

    /// TODO:考虑到chunk writer的连续性，应该在OpenWriter后，返回store_id,或则推荐用户用原始的select_writer语义实现。
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

    //TODO:ndn-lib里有通用函数？
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

    fn normalize_inner_path(inner_path: Option<String>) -> Option<String> {
        let path = match inner_path {
            Some(path) => path.trim().to_string(),
            None => return None,
        };
        if path.is_empty() || path == "/" {
            return None;
        }
        if path.starts_with('/') {
            Some(path)
        } else {
            Some(format!("/{}", path))
        }
    }

    fn split_first_segment(path: &str) -> NdnResult<(String, Option<String>)> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Err(NdnError::InvalidParam("empty inner path".to_string()));
        }
        let first = segments[0].to_string();
        let rest = if segments.len() > 1 {
            Some(format!("/{}", segments[1..].join("/")))
        } else {
            None
        };
        Ok((first, rest))
    }

    async fn resolve_next_obj(
        &self,
        obj_id: &ObjId,
        obj_str: &str,
        path: &str,
    ) -> NdnResult<(ObjId, Option<String>, Option<String>)> {
        let mut current_obj_id = obj_id.clone();
        let mut current_obj_str = obj_str.to_string();
        let mut current_path = path.to_string();
        let mut pending_cache_keys: Vec<ResolveNextObjCacheKey> = Vec::new();

        loop {
            if let Some(cached) = {
                let mut cache = self.resolve_next_obj_cache.lock().await;
                cache.get(&current_obj_id, current_path.as_str())
            } {
                let resolved = (cached.next_obj_id, cached.next_path, cached.next_obj_str);
                if !pending_cache_keys.is_empty() {
                    let mut cache = self.resolve_next_obj_cache.lock().await;
                    for key in pending_cache_keys {
                        cache.put(
                            &key.obj_id,
                            key.path.as_str(),
                            ResolveNextObjCacheValue {
                                next_obj_id: resolved.0.clone(),
                                next_path: resolved.1.clone(),
                                next_obj_str: resolved.2.clone(),
                            },
                        );
                    }
                }
                return Ok(resolved);
            }

            if current_obj_id.is_chunk() {
                return Err(NdnError::InvalidParam(format!(
                    "chunk {} does not support inner path",
                    current_obj_id.to_string()
                )));
            }

            pending_cache_keys.push(ResolveNextObjCacheKey {
                obj_id: current_obj_id.clone(),
                path: current_path.clone(),
            });

            let (next_obj_id, next_path, next_obj_str) =
                Self::resolve_next_obj_once(&current_obj_id, current_obj_str.as_str(), current_path.as_str())?;

            if let Some(rest_path) = next_path {
                let next_obj_str_for_next = match next_obj_str {
                    Some(next_obj_str) => next_obj_str,
                    None => self.get_object(&next_obj_id).await?,
                };
                current_obj_id = next_obj_id;
                current_obj_str = next_obj_str_for_next;
                current_path = rest_path;
                continue;
            }

            let resolved = (next_obj_id, None, next_obj_str);
            let mut cache = self.resolve_next_obj_cache.lock().await;
            for key in pending_cache_keys {
                cache.put(
                    &key.obj_id,
                    key.path.as_str(),
                    ResolveNextObjCacheValue {
                        next_obj_id: resolved.0.clone(),
                        next_path: resolved.1.clone(),
                        next_obj_str: resolved.2.clone(),
                    },
                );
            }
            return Ok(resolved);
        }
    }

    fn resolve_next_obj_once(
        obj_id: &ObjId,
        obj_str: &str,
        path: &str,
    ) -> NdnResult<(ObjId, Option<String>, Option<String>)> {
        if obj_id.is_dir_object() {
            let dir_obj: DirObject = load_named_obj(obj_str)?;
            let (segment, rest_path) = Self::split_first_segment(path)?;
            let item = dir_obj
                .get(&segment)
                .ok_or_else(|| NdnError::NotFound(format!("path not found: {}", segment)))?;
            match item {
                SimpleMapItem::ObjId(next_obj_id) => Ok((next_obj_id.clone(), rest_path, None)),
                SimpleMapItem::Object(_, _) | SimpleMapItem::ObjectJwt(_, _) => {
                    let (next_obj_id, next_obj_str) = item.get_obj_id()?;
                    Ok((next_obj_id, rest_path, Some(next_obj_str)))
                }
            }
        } else if obj_id.is_chunk_list() {
            //只消费1级
            unimplemented!()
        } else {
            let obj_json = load_named_object_from_obj_str(obj_str)?;
            let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            if segments.is_empty() {
                return Err(NdnError::InvalidParam("empty inner path".to_string()));
            }
            let mut last_err: Option<NdnError> = None;
            for i in (1..=segments.len()).rev() {
                let candidate = format!("/{}", segments[0..i].join("/"));
                match extract_objid_by_path(&obj_json, candidate.as_str()) {
                    Ok(next_obj_id) => {
                        let rest_path = if i < segments.len() {
                            Some(format!("/{}", segments[i..].join("/")))
                        } else {
                            None
                        };
                        return Ok((next_obj_id, rest_path, None));
                    }
                    Err(err) => last_err = Some(err),
                }
            }
            Err(last_err
                .unwrap_or_else(|| NdnError::NotFound(format!("objid path not found: {}", path))))
        }
    }
}

impl Default for NamedStoreMgr {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "test_store_mgr.rs"]
mod test_store_mgr;
