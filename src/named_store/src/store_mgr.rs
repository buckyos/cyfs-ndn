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
use crate::gc_types::{CascadeStateP0, EdgeMsg, ExpandDebug, PinScope};
use crate::{
    ChunkLocalInfo, ChunkStoreState, ChunkWriteOutcome, LayoutVersion, NamedLocalConfig,
    NamedStore, ObjectState, SimpleChunkListReader, StoreLayout, StoreTarget,
};
use log::warn;
use ndn_lib::{
    extract_objid_by_path, load_named_obj, load_named_object_from_obj_str, ChunkId, ChunkReader,
    DirObject, FileObject, NdnError, NdnResult, ObjId, SimpleChunkList, SimpleMapItem,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct StoreLayoutConfigFile {
    epoch: u64,
    #[serde(alias = "targets")]
    stores: Vec<StoreConfigEntry>,
    total_capacity: Option<u64>,
    total_used: Option<u64>,
}

impl Default for StoreLayoutConfigFile {
    fn default() -> Self {
        Self {
            epoch: 1,
            stores: Vec::new(),
            total_capacity: None,
            total_used: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct StoreConfigEntry {
    store_id: Option<String>,
    #[serde(alias = "base_dir", alias = "root_path", alias = "store_path")]
    path: PathBuf,
    capacity: Option<u64>,
    used: Option<u64>,
    readonly: bool,
    enabled: bool,
    weight: u32,
}

impl Default for StoreConfigEntry {
    fn default() -> Self {
        Self {
            store_id: None,
            path: PathBuf::new(),
            capacity: None,
            used: None,
            readonly: false,
            enabled: true,
            weight: 1,
        }
    }
}

fn read_json_config<T: serde::de::DeserializeOwned>(path: &Path) -> NdnResult<T> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| NdnError::IoError(format!("read {} failed: {}", path.display(), e)))?;
    serde_json::from_str::<T>(&content)
        .map_err(|e| NdnError::InvalidData(format!("parse {} failed: {}", path.display(), e)))
}

fn resolve_store_id(entry: &StoreConfigEntry, index: usize) -> String {
    if let Some(store_id) = entry.store_id.as_ref().filter(|v| !v.is_empty()) {
        return store_id.clone();
    }
    entry
        .path
        .file_name()
        .and_then(|v| v.to_str())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| format!("store-{}", index + 1))
}

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
    stores: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<NamedStore>>>>>,

    /// Maximum number of layout versions to keep
    max_versions: usize,

    /// Cache for resolve_next_obj results by (obj_id, inner_path).
    resolve_next_obj_cache: Arc<Mutex<ResolveNextObjCache>>,
}

impl NamedStoreMgr {
    pub async fn get_store_mgr(store_config_path: &Path) -> NdnResult<Self> {
        let store_config: StoreLayoutConfigFile = read_json_config(store_config_path)?;
        if store_config.stores.len() < 1 {
            return Err(NdnError::InvalidParam(format!(
                "store config {} must include at least 1 stores",
                store_config_path.display()
            )));
        }

        let store_mgr = Self::new();
        let mut targets = Vec::with_capacity(store_config.stores.len());
        let mut total_capacity = 0u64;
        let mut total_used = 0u64;
        let mut store_id_set = HashSet::new();

        let config_dir = store_config_path.parent().unwrap_or_else(|| Path::new("."));

        for (index, entry) in store_config.stores.iter().enumerate() {
            if entry.path.as_os_str().is_empty() {
                return Err(NdnError::InvalidParam(format!(
                    "store config {} has empty path at index {}",
                    store_config_path.display(),
                    index
                )));
            }

            let store_path = if entry.path.is_absolute() {
                entry.path.clone()
            } else {
                config_dir.join(&entry.path)
            };

            std::fs::create_dir_all(&store_path).map_err(|e| {
                NdnError::IoError(format!(
                    "create store dir {} failed: {}",
                    store_path.display(),
                    e
                ))
            })?;

            let store_id = resolve_store_id(entry, index);
            let config = NamedLocalConfig {
                read_only: entry.readonly,
                ..Default::default()
            };
            let store = NamedStore::from_config(Some(store_id.clone()), store_path, config).await?;

            let actual_store_id = store.store_id().to_string();
            if actual_store_id != store_id {
                warn!(
                    "store id mismatch, configured={}, actual={}, using actual",
                    store_id, actual_store_id
                );
            }
            if !store_id_set.insert(actual_store_id.clone()) {
                return Err(NdnError::InvalidParam(format!(
                    "duplicate store_id '{}' in {}",
                    actual_store_id,
                    store_config_path.display()
                )));
            }

            let store_ref = Arc::new(tokio::sync::Mutex::new(store));
            store_mgr.register_store(store_ref).await;

            total_capacity += entry.capacity.unwrap_or(0);
            total_used += entry.used.unwrap_or(0);
            targets.push(StoreTarget {
                store_id: actual_store_id,
                device_did: None,
                capacity: entry.capacity,
                used: entry.used,
                readonly: entry.readonly,
                enabled: entry.enabled,
                weight: entry.weight,
            });
        }

        let layout = StoreLayout::new(
            store_config.epoch.max(1),
            targets,
            store_config.total_capacity.unwrap_or(total_capacity),
            store_config.total_used.unwrap_or(total_used),
        );
        store_mgr.add_layout(layout).await;
        Ok(store_mgr)
    }

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
    pub async fn register_store(&self, store: Arc<tokio::sync::Mutex<NamedStore>>) {
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
    ) -> Option<Arc<tokio::sync::Mutex<NamedStore>>> {
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
    pub async fn is_object_stored(&self, obj_id: &ObjId, inner_path: Option<String>) -> NdnResult<bool> {
        // 判断对象和其所有的children是否都已经在store里了
        let mut current_obj_id = obj_id.clone();
        let mut current_path = Self::normalize_inner_path(inner_path);
        let mut current_obj_str: Option<String> = None;

        // Follow inner_path to resolve the target object
        loop {
            if current_path.is_none() {
                break;
            }

            if current_obj_id.is_chunk() {
                return Err(NdnError::InvalidParam(format!(
                    "chunk {} does not support inner path",
                    current_obj_id.to_string()
                )));
            }

            let obj_str = match current_obj_str.take() {
                Some(s) => s,
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

        // Now check if the resolved object and all its dependencies are stored
        self.is_object_fully_stored(&current_obj_id, current_obj_str).await
    }

    /// Recursively check if an object and all its strong-referenced children are stored.
    fn is_object_fully_stored<'a>(&'a self, obj_id: &'a ObjId, obj_str_hint: Option<String>) -> Pin<Box<dyn Future<Output = NdnResult<bool>> + Send + 'a>> {
        Box::pin(async move {
        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            return Ok(self.have_chunk(&chunk_id).await);
        }

        if obj_id.is_chunk_list() {
            let obj_str = match obj_str_hint {
                Some(s) => s,
                None => self.get_object(obj_id).await?,
            };
            let chunk_list = SimpleChunkList::from_json(obj_str.as_str())?;
            for chunk_id in chunk_list.body.iter() {
                if !self.have_chunk(chunk_id).await {
                    return Ok(false);
                }
            }
            return Ok(true);
        }

        // Object must exist in store
        let obj_str = match obj_str_hint {
            Some(s) => s,
            None => match self.query_object_by_id(obj_id).await? {
                ObjectState::Object(s) => s,
                ObjectState::NotExist => return Ok(false),
            },
        };

        if obj_id.is_file_object() {
            let file_obj: FileObject = load_named_obj(obj_str.as_str())?;
            if file_obj.content.is_empty() {
                return Ok(true);
            }
            let content_obj_id = ObjId::new(file_obj.content.as_str())?;
            return self.is_object_fully_stored(&content_obj_id, None).await;
        }

        if obj_id.is_dir_object() {
            let dir_obj: DirObject = load_named_obj(obj_str.as_str())?;
            for item in dir_obj.values() {
                let (child_obj_id, child_obj_str) = item.get_obj_id()?;
                let hint = if child_obj_str.is_empty() { None } else { Some(child_obj_str) };
                if !self.is_object_fully_stored(&child_obj_id, hint).await? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }

        // For other object types, just being present in store is sufficient
        Ok(true)
        })
    }

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
    pub async fn query_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<(ChunkStoreState, u64)> {
        let obj_id = chunk_id.to_obj_id();
        let versions = self.versions.read().await;
        let stores = self.stores.read().await;

        if versions.is_empty() {
            return Ok((ChunkStoreState::NotExist, 0));
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
            let (state, size) = store_guard.query_chunk_state(chunk_id).await?;
            if state != ChunkStoreState::NotExist {
                return Ok((state, size));
            }
        }

        Ok((ChunkStoreState::NotExist, 0))
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

    /// 原子写入 chunk：一次性从 reader 读取全部数据，backend 自动 hash 校验。
    /// chunk_size 不能超过 CHUNK_DEFAULT_SIZE（32MB）。
    pub async fn put_chunk_by_reader(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        reader: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
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

    /// 原子写入 chunk 字节数据。backend 自动进行 hash 校验。
    /// chunk_data 长度不能超过 CHUNK_DEFAULT_SIZE（32MB）。
    pub async fn put_chunk(&self, chunk_id: &ChunkId, chunk_data: &[u8]) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no available store for write".to_string()))?;

        let store_guard = store.lock().await;
        store_guard.put_chunk(chunk_id, chunk_data).await
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

    // ==================== GC / Edge Operations ====================

    /// Route an edge message to the bucket owning `msg.referee`.
    pub async fn apply_edge(&self, msg: EdgeMsg) -> NdnResult<()> {
        let store = self
            .select_store_for_write(&msg.referee)
            .await
            .ok_or_else(|| NdnError::NotFound(format!("no store for referee {}", msg.referee)))?;
        let store_guard = store.lock().await;
        store_guard.apply_edge(msg).await
    }

    /// Pin an object in the bucket owning `obj_id`.
    pub async fn pin(
        &self,
        obj_id: &ObjId,
        owner: &str,
        scope: PinScope,
        ttl: Option<Duration>,
    ) -> NdnResult<()> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for pin".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.pin(obj_id, owner, scope, ttl).await
    }

    /// Unpin an object in the bucket owning `obj_id`.
    pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for unpin".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.unpin(obj_id, owner).await
    }

    /// Unpin all objects owned by `owner` across all stores.
    pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize> {
        let stores = self.stores.read().await;
        let mut total = 0usize;
        for store in stores.values() {
            let store_guard = store.lock().await;
            total += store_guard.unpin_owner(owner).await?;
        }
        Ok(total)
    }

    /// Acquire an fs anchor in the bucket owning `obj_id`.
    pub async fn fs_acquire(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32) -> NdnResult<()> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for fs_acquire".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.fs_acquire(obj_id, inode_id, field_tag).await
    }

    /// Release an fs anchor in the bucket owning `obj_id`.
    pub async fn fs_release(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32) -> NdnResult<()> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for fs_release".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.fs_release(obj_id, inode_id, field_tag).await
    }

    /// Release all fs anchors for a given inode across all stores.
    pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize> {
        let stores = self.stores.read().await;
        let mut total = 0usize;
        for store in stores.values() {
            let store_guard = store.lock().await;
            total += store_guard.fs_release_inode(inode_id).await?;
        }
        Ok(total)
    }

    /// Query fs anchor state for (obj_id, inode_id, field_tag) — routes to owning bucket.
    pub async fn fs_anchor_state(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<CascadeStateP0> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for fs_anchor_state".to_string()))?;
        let store_guard = store.lock().await;
        store_guard
            .fs_anchor_state(obj_id, inode_id, field_tag)
            .await
    }

    /// Run forced GC across all stores until target_bytes freed.
    pub async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64> {
        let stores = self.stores.read().await;
        let mut total_freed = 0u64;
        for store in stores.values() {
            if total_freed >= target_bytes {
                break;
            }
            let store_guard = store.lock().await;
            let remaining = target_bytes - total_freed;
            match store_guard.forced_gc_until(remaining).await {
                Ok(freed) => total_freed += freed,
                Err(_) => {
                    // This store couldn't free enough, try next
                    if let Ok(report) = store_guard.gc_round(remaining).await {
                        total_freed += report.freed_bytes;
                    }
                }
            }
        }
        if total_freed >= target_bytes {
            Ok(total_freed)
        } else {
            Err(NdnError::IoError(format!(
                "ENOSPC: freed {} bytes but needed {}; no class-0 owned bytes left to evict",
                total_freed, target_bytes
            )))
        }
    }

    /// Register a SameAs relationship: big_chunk_id content equals chunk_list_id.
    pub async fn add_chunk_by_same_as(
        &self,
        big_chunk_id: &ChunkId,
        big_chunk_size: u64,
        chunk_list_id: &ObjId,
    ) -> NdnResult<()> {
        let obj_id = big_chunk_id.to_obj_id();
        let store = self
            .select_store_for_write(&obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for same_as".to_string()))?;
        let store_guard = store.lock().await;
        store_guard
            .add_chunk_by_same_as(big_chunk_id, big_chunk_size, chunk_list_id)
            .await
    }

    /// Sum of outbox counts across all stores.
    pub async fn outbox_count(&self) -> NdnResult<u64> {
        let stores = self.stores.read().await;
        let mut total = 0u64;
        for store in stores.values() {
            let store_guard = store.lock().await;
            total += store_guard.outbox_count().await?;
        }
        Ok(total)
    }

    /// Debug: dump expand state for an object (routes to owning bucket).
    pub async fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for expand_state".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.debug_dump_expand_state(obj_id).await
    }

    /// Query anchor state for (obj_id, owner) — routes to owning bucket.
    pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str) -> NdnResult<CascadeStateP0> {
        let store = self
            .select_store_for_write(obj_id)
            .await
            .ok_or_else(|| NdnError::NotFound("no store for anchor_state".to_string()))?;
        let store_guard = store.lock().await;
        store_guard.anchor_state(obj_id, owner).await
    }

    // ==================== Store Access ====================

    /// Get store by store_id
    pub async fn get_store(&self, store_id: &str) -> Option<Arc<tokio::sync::Mutex<NamedStore>>> {
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
    ) -> Option<Arc<tokio::sync::Mutex<NamedStore>>> {
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

            let (next_obj_id, next_path, next_obj_str) = Self::resolve_next_obj_once(
                &current_obj_id,
                current_obj_str.as_str(),
                current_path.as_str(),
            )?;

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
