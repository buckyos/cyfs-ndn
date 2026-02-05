//! Named Data Manager (NDM) - Core orchestration layer
//!
//! This module coordinates fs_meta, fs_buffer, and named_store to provide
//! a unified file/directory namespace with overlay semantics.

use std::collections::HashMap;
use std::sync::Arc;

use fs_buffer::{FileBufferService, SessionId};
use named_store::NamedLocalStore;
use ndn_lib::{ChunkId, NdnError, NdnResult, ObjId};

use crate::{
    DentryRecord, DentryTarget, FsMetaClient, IndexNodeId, NodeKind, NodeRecord, NodeState,
    ObjStat,
};

// ------------------------------
// Basic Types
// ------------------------------

/// Instance identifier for a NamedDataMgr
pub type NdmInstanceId = String;

/// NDM path representation
#[derive(Debug, Clone)]
pub struct NdmPath(pub String);

impl NdmPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split path into parent and name components
    pub fn split_parent_name(&self) -> Option<(NdmPath, String)> {
        let path = self.0.trim_end_matches('/');
        if path.is_empty() || path == "/" {
            return None;
        }
        let last_slash = path.rfind('/')?;
        let parent = if last_slash == 0 {
            "/".to_string()
        } else {
            path[..last_slash].to_string()
        };
        let name = path[last_slash + 1..].to_string();
        if name.is_empty() {
            None
        } else {
            Some((NdmPath(parent), name))
        }
    }

    pub fn is_root(&self) -> bool {
        let s = self.0.trim_end_matches('/');
        s.is_empty() || s == "/"
    }
}

/// Path statistics
#[derive(Debug, Clone)]
pub struct PathStat {
    pub kind: PathKind,
    pub size: Option<u64>,
    pub obj_id: Option<ObjId>,
    pub inode_id: Option<IndexNodeId>,
    pub state: Option<NodeState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathKind {
    File,
    Dir,
    NotFound,
}

/// Commit policy for file writes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitPolicy {
    /// Immediately finalize data to internal store
    Immediate,
    /// Stage data, allow background finalization
    Staged,
}

impl Default for CommitPolicy {
    fn default() -> Self {
        CommitPolicy::Staged
    }
}

/// Read options for open_reader
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub auto_pull: bool,
}

/// Inner path for accessing content within an object
pub type InnerPath = String;


// ------------------------------
// Move-related types
// ------------------------------

#[derive(Clone, Copy, Default)]
struct MoveOptions {
    /// Whether to overwrite destination if destination is visible in Upper layer.
    overwrite_upper: bool,
    /// If true: also check destination Base (merged view no-clobber).
    /// This requires Base materialized, otherwise NEED_PULL.
    strict_check_base: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    File,
    Dir,
    Unknown,
}

#[derive(Clone, Debug)]
enum MoveSource {
    /// Source resolved from Upper dentry (fast, no base needed)
    Upper {
        target: DentryTarget,
        kind_hint: ObjectKind,
    },
    /// Source resolved from Base (no upper dentry exists)
    Base {
        obj_id: ObjId,
        kind: ObjectKind,
        src_parent_rev0: u64,
        src_parent_base0: Option<ObjId>,
    },
}

#[derive(Clone, Debug)]
struct MovePlan {
    src_parent: IndexNodeId,
    src_name: String,
    dst_parent: IndexNodeId,
    dst_name: String,
    src_rev0: u64,
    dst_rev0: u64,
    source: MoveSource,
}

// ------------------------------
// Trait definitions for dependencies
// ------------------------------

/// Trait for NDN fetcher (pull operations)
#[async_trait::async_trait]
pub trait NdnFetcher: Send + Sync {
    async fn schedule_pull_obj(&self, obj_id: &ObjId) -> NdnResult<()>;
    async fn schedule_pull_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()>;
}

/// Pull context for fetch operations
#[derive(Debug, Clone, Default)]
pub struct PullContext {
    pub priority: u32,
}

// ------------------------------
// Store target for expansion
// ------------------------------

#[derive(Debug, Clone)]
pub struct StoreTarget {
    pub store_id: String,
    pub path: String,
}

// ------------------------------
// NamedDataMgr Implementation
// ------------------------------

pub struct NamedDataMgr {
    pub instance: NdmInstanceId,

    fsmeta: Arc<FsMetaClient>,
    store: Arc<tokio::sync::Mutex<NamedLocalStore>>,
    buffer: Arc<dyn FileBufferService>,
    fetcher: Option<Arc<dyn NdnFetcher>>,

    /// Default commit policy
    default_commit_policy: CommitPolicy,

    /// Background task manager
    bg: Arc<tokio::sync::Mutex<BackgroundMgr>>,
}

impl NamedDataMgr {
    pub fn new(
        instance: NdmInstanceId,
        fsmeta: Arc<FsMetaClient>,
        store: Arc<tokio::sync::Mutex<NamedLocalStore>>,
        buffer: Arc<dyn FileBufferService>,
        fetcher: Option<Arc<dyn NdnFetcher>>,
        default_commit_policy: CommitPolicy,
    ) -> Self {
        Self {
            instance,
            fsmeta,
            store,
            buffer,
            fetcher,
            default_commit_policy,
            bg: Arc::new(tokio::sync::Mutex::new(BackgroundMgr::new())),
        }
    }

    // ========== Path Resolution ==========

    /// Resolve a path to its inode, creating directory inodes as needed
    async fn resolve_path(&self, path: &NdmPath) -> NdnResult<Option<(IndexNodeId, NodeRecord)>> {
        if path.is_root() {
            let root_id = self.fsmeta.root_dir().await.map_err(|e| {
                NdnError::Internal(format!("failed to get root dir: {}", e))
            })?;
            let node = self.fsmeta.get_inode(root_id, None).await.map_err(|e| {
                NdnError::Internal(format!("failed to get root inode: {}", e))
            })?;
            return Ok(node.map(|n| (root_id, n)));
        }

        // Walk the path from root
        let components: Vec<&str> = path
            .as_str()
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        let root_id = self.fsmeta.root_dir().await.map_err(|e| {
            NdnError::Internal(format!("failed to get root dir: {}", e))
        })?;

        let mut current_id = root_id;

        for (i, component) in components.iter().enumerate() {
            let is_last = i == components.len() - 1;

            // Look up dentry
            let dentry = self
                .fsmeta
                .get_dentry(current_id, component.to_string(), None)
                .await
                .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

            match dentry {
                Some(d) => match d.target {
                    DentryTarget::IndexNodeId(id) => {
                        if is_last {
                            let node = self.fsmeta.get_inode(id, None).await.map_err(|e| {
                                NdnError::Internal(format!("failed to get inode: {}", e))
                            })?;
                            return Ok(node.map(|n| (id, n)));
                        }
                        current_id = id;
                    }
                    DentryTarget::ObjId(_obj_id) => {
                        // For now, ObjId targets need materialization for traversal
                        if is_last {
                            // Return info about ObjId binding
                            return Ok(None); // TODO: handle ObjId targets better
                        }
                        return Err(NdnError::NotFound(format!(
                            "path {} requires materialization",
                            path.as_str()
                        )));
                    }
                    DentryTarget::Tombstone => {
                        return Ok(None);
                    }
                },
                None => {
                    return Ok(None);
                }
            }
        }

        // Should not reach here
        Ok(None)
    }

    // ========== Basic Operations ==========

    pub async fn stat(&self, path: &NdmPath) -> NdnResult<PathStat> {
        let resolved = self.resolve_path(path).await?;
        match resolved {
            Some((inode_id, node)) => {
                let kind = match node.kind {
                    NodeKind::File => PathKind::File,
                    NodeKind::Dir => PathKind::Dir,
                    NodeKind::Object => PathKind::File, // Objects are file-like
                };
                Ok(PathStat {
                    kind,
                    size: None, // TODO: get size from state
                    obj_id: node.base_obj_id.clone(),
                    inode_id: Some(inode_id),
                    state: Some(node.state),
                })
            }
            None => Ok(PathStat {
                kind: PathKind::NotFound,
                size: None,
                obj_id: None,
                inode_id: None,
                state: None,
            }),
        }
    }

    pub async fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat> {
        let stat = self.fsmeta.obj_stat_get(obj_id.clone()).await.map_err(|e| {
            NdnError::Internal(format!("failed to get obj stat: {}", e))
        })?;

        stat.ok_or_else(|| NdnError::NotFound(format!("object {} not found", obj_id)))
    }

    // ========== Write Operations ==========

    pub async fn set_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let parent = self.ensure_dir_inode(&parent_path).await?;

        // Upsert dentry
        self.fsmeta
            .upsert_dentry(parent, name, DentryTarget::ObjId(obj_id.clone()), None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

        // Bump ref count
        let _ = self
            .fsmeta
            .obj_stat_bump(obj_id, 1, None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to bump ref count: {}", e)))?;

        Ok(())
    }

    pub async fn set_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let parent = self.ensure_dir_inode(&parent_path).await?;

        self.fsmeta
            .upsert_dentry(parent, name, DentryTarget::ObjId(dir_obj_id.clone()), None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

        // TODO: update ref_count for dir and its children

        Ok(())
    }

    pub async fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let resolved = self.resolve_path(&parent_path).await?;
        let (parent_id, _parent_node) =
            resolved.ok_or_else(|| NdnError::NotFound("parent not found".to_string()))?;

        // Set tombstone instead of deleting (overlay semantics)
        self.fsmeta
            .set_tombstone(parent_id, name, None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to set tombstone: {}", e)))?;

        Ok(())
    }

    pub async fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()> {
        self.move_path_with_opts(old_path, new_path, MoveOptions::default())
            .await
    }

    pub async fn copy_path(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        // Get source stat
        let src_stat = self.stat(src).await?;
        if src_stat.kind == PathKind::NotFound {
            return Err(NdnError::NotFound(format!("source {} not found", src.as_str())));
        }

        // If source has an ObjId, just bind it to target
        if let Some(obj_id) = src_stat.obj_id {
            if src_stat.kind == PathKind::File {
                self.set_file(target, obj_id).await?;
            } else {
                self.set_dir(target, obj_id).await?;
            }
            return Ok(());
        }

        // Source is a working file without committed ObjId
        Err(NdnError::InvalidState(
            "source must be committed to copy".to_string(),
        ))
    }

    // ========== Directory Operations ==========

    pub async fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let parent_id = self.ensure_dir_inode(&parent_path).await?;

        // Check if already exists
        let existing = self
            .fsmeta
            .get_dentry(parent_id, name.clone(), None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

        if let Some(d) = existing {
            if !matches!(d.target, DentryTarget::Tombstone) {
                return Err(NdnError::AlreadyExists(format!(
                    "path {} already exists",
                    path.as_str()
                )));
            }
        }

        // Create directory inode
        let txid = self
            .fsmeta
            .begin_txn()
            .await
            .map_err(|e| NdnError::Internal(format!("failed to begin txn: {}", e)))?;

        let new_node = NodeRecord {
            inode_id: 0, // Will be assigned by alloc
            kind: NodeKind::Dir,
            read_only: false,
            base_obj_id: None,
            state: NodeState::DirNormal,
            rev: Some(0),
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        let new_id = self
            .fsmeta
            .alloc_inode(new_node, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to alloc inode: {}", e)))?;

        self.fsmeta
            .upsert_dentry(
                parent_id,
                name,
                DentryTarget::IndexNodeId(new_id),
                Some(txid.clone()),
            )
            .await
            .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

        self.fsmeta
            .commit(Some(txid))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to commit: {}", e)))?;

        Ok(())
    }

    pub async fn list(
        &self,
        path: &NdmPath,
        _pos: u32,
        _page_size: u32,
    ) -> NdnResult<Vec<(String, PathStat)>> {
        let resolved = self.resolve_path(path).await?;
        let (dir_id, dir_node) =
            resolved.ok_or_else(|| NdnError::NotFound("path not found".to_string()))?;

        if dir_node.kind != NodeKind::Dir {
            return Err(NdnError::InvalidParam("path is not a directory".to_string()));
        }

        // List dentries from upper layer
        let dentries = self
            .fsmeta
            .list_dentries(dir_id, None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to list dentries: {}", e)))?;

        let mut results = Vec::new();

        for dentry in dentries {
            if matches!(dentry.target, DentryTarget::Tombstone) {
                continue;
            }

            let child_stat = match &dentry.target {
                DentryTarget::IndexNodeId(id) => {
                    let node = self
                        .fsmeta
                        .get_inode(*id, None)
                        .await
                        .map_err(|e| NdnError::Internal(format!("failed to get inode: {}", e)))?;

                    match node {
                        Some(n) => PathStat {
                            kind: match n.kind {
                                NodeKind::File => PathKind::File,
                                NodeKind::Dir => PathKind::Dir,
                                NodeKind::Object => PathKind::File,
                            },
                            size: None,
                            obj_id: n.base_obj_id,
                            inode_id: Some(*id),
                            state: Some(n.state),
                        },
                        None => continue,
                    }
                }
                DentryTarget::ObjId(obj_id) => {
                    // TODO: determine kind from object metadata
                    PathStat {
                        kind: PathKind::File, // Assume file for now
                        size: None,
                        obj_id: Some(obj_id.clone()),
                        inode_id: None,
                        state: None,
                    }
                }
                DentryTarget::Tombstone => continue,
            };

            results.push((dentry.name, child_stat));
        }

        // TODO: merge with base directory if present
        // TODO: implement pagination

        Ok(results)
    }

    // ========== Read Operations ==========

    pub async fn open_reader(
        &self,
        path: &NdmPath,
        _opts: ReadOptions,
    ) -> NdnResult<(Box<dyn tokio::io::AsyncRead + Send + Unpin>, u64)> {
        let stat = self.stat(path).await?;

        if stat.kind == PathKind::NotFound {
            return Err(NdnError::NotFound(format!("path {} not found", path.as_str())));
        }

        // If there's an inode with working state, use buffer
        if let Some(_inode_id) = stat.inode_id {
            if let Some(state) = &stat.state {
                match state {
                    NodeState::Working(ws) => {
                        // Open reader from buffer
                        let fb = self.buffer.get_buffer(&ws.fb_handle).await?;
                        let reader = self
                            .buffer
                            .open_reader(&fb, std::io::SeekFrom::Start(0))
                            .await?;
                        // Get file size from metadata or estimate
                        return Ok((Box::new(reader), 0)); // TODO: get actual size
                    }
                    NodeState::Cooling(cs) => {
                        let fb = self.buffer.get_buffer(&cs.fb_handle).await?;
                        let reader = self
                            .buffer
                            .open_reader(&fb, std::io::SeekFrom::Start(0))
                            .await?;
                        return Ok((Box::new(reader), 0));
                    }
                    _ => {}
                }
            }
        }

        // Fall back to committed object
        if let Some(obj_id) = stat.obj_id {
            return self.open_reader_by_id(&obj_id, None, ReadOptions::default()).await;
        }

        Err(NdnError::NotFound("no readable content".to_string()))
    }

    pub async fn open_reader_by_id(
        &self,
        obj_id: &ObjId,
        _inner_path: Option<&InnerPath>,
        _opts: ReadOptions,
    ) -> NdnResult<(Box<dyn tokio::io::AsyncRead + Send + Unpin>, u64)> {
        // Check if object is a chunk
        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            let store = self.store.lock().await;
            let (reader, size) = store.open_chunk_reader_impl(&chunk_id, 0, false).await?;
            return Ok((Box::new(reader), size));
        }

        // TODO: handle other object types (FileObject, DirObject, etc.)
        Err(NdnError::Unsupported(
            "non-chunk object reading not implemented".to_string(),
        ))
    }

    pub async fn get_object(&self, obj_id: &ObjId) -> NdnResult<Vec<u8>> {
        let store = self.store.lock().await;
        let obj = store.get_object_impl(obj_id, None).await?;
        Ok(serde_json::to_vec(&obj).map_err(|e| NdnError::Internal(e.to_string()))?)
    }

    pub async fn get_object_by_path(&self, path: &NdmPath) -> NdnResult<String> {
        let stat = self.stat(path).await?;
        if stat.kind == PathKind::NotFound {
            return Err(NdnError::NotFound(format!("path {} not found", path.as_str())));
        }

        let obj_id = stat
            .obj_id
            .ok_or_else(|| NdnError::NotFound("no object bound to path".to_string()))?;

        let store = self.store.lock().await;
        let obj = store.get_object_impl(&obj_id, None).await?;
        Ok(serde_json::to_string(&obj).map_err(|e| NdnError::Internal(e.to_string()))?)
    }

    // ========== Write Operations (File) ==========

    pub async fn open_file_writer(
        &self,
        path: &NdmPath,
        expected_size: Option<u64>,
    ) -> NdnResult<String> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let parent_id = self.ensure_dir_inode(&parent_path).await?;

        // Check parent is not read-only
        let parent_node = self
            .fsmeta
            .get_inode(parent_id, None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get parent inode: {}", e)))?
            .ok_or_else(|| NdnError::NotFound("parent not found".to_string()))?;

        if parent_node.read_only {
            return Err(NdnError::PermissionDenied("parent is read-only".to_string()));
        }

        let txid = self
            .fsmeta
            .begin_txn()
            .await
            .map_err(|e| NdnError::Internal(format!("failed to begin txn: {}", e)))?;

        // Check existing dentry
        let dentry = self
            .fsmeta
            .get_dentry(parent_id, name.clone(), Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

        let file_id = match dentry {
            None | Some(DentryRecord { target: DentryTarget::Tombstone, .. }) => {
                // New file
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: None,
                    state: NodeState::DirNormal, // Will be updated to Working
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };

                let fid = self
                    .fsmeta
                    .alloc_inode(new_node, Some(txid.clone()))
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to alloc inode: {}", e)))?;

                self.fsmeta
                    .upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                    )
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

                fid
            }
            Some(DentryRecord { target: DentryTarget::IndexNodeId(fid), .. }) => {
                // Existing inode - check state
                let node = self
                    .fsmeta
                    .get_inode(fid, Some(txid.clone()))
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get inode: {}", e)))?
                    .ok_or_else(|| NdnError::NotFound("inode not found".to_string()))?;

                if matches!(node.state, NodeState::Working(_)) {
                    self.fsmeta
                        .rollback(Some(txid))
                        .await
                        .map_err(|e| NdnError::Internal(format!("failed to rollback: {}", e)))?;
                    return Err(NdnError::InvalidState("file is already being written".to_string()));
                }

                fid
            }
            Some(DentryRecord { target: DentryTarget::ObjId(oid), .. }) => {
                // Materialize inode from ObjId
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: Some(oid),
                    state: NodeState::DirNormal,
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };

                let fid = self
                    .fsmeta
                    .alloc_inode(new_node, Some(txid.clone()))
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to alloc inode: {}", e)))?;

                self.fsmeta
                    .upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                    )
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

                fid
            }
        };

        // Acquire lease
        let session = SessionId(format!("{}:{}", self.instance, file_id));
        let lease_seq = self
            .fsmeta
            .acquire_file_lease(file_id, session.clone(), std::time::Duration::from_secs(300))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to acquire lease: {}", e)))?;

        // Allocate buffer
        let lease = fs_buffer::WriteLease {
            session: session.clone(),
            session_seq: lease_seq,
            expires_at: 0,
        };

        let fb = self
            .buffer
            .alloc_buffer(
                &fs_buffer::NdmPath(path.0.clone()),
                file_id,
                vec![],
                &lease,
                expected_size,
            )
            .await?;

        // Update inode state to Working
        let working_state = NodeState::Working(crate::FileWorkingState {
            fb_handle: fb.handle_id.clone(),
            last_write_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });

        self.fsmeta
            .update_inode_state(file_id, working_state, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to update inode state: {}", e)))?;

        self.fsmeta
            .commit(Some(txid))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to commit: {}", e)))?;

        Ok(fb.handle_id)
    }

    pub async fn append(&self, path: &NdmPath, data: &[u8]) -> NdnResult<()> {
        let handle_id = self.open_file_writer(path, Some(data.len() as u64)).await?;
        let fb = self.buffer.get_buffer(&handle_id).await?;
        self.buffer.append(&fb, data).await?;
        self.buffer.close(&fb).await?;
        Ok(())
    }

    pub async fn flush(&self, handle_id: &str) -> NdnResult<()> {
        let fb = self.buffer.get_buffer(handle_id).await?;
        self.buffer.flush(&fb).await
    }

    pub async fn close_file(&self, handle_id: &str) -> NdnResult<()> {
        let fb = self.buffer.get_buffer(handle_id).await?;
        self.buffer.flush(&fb).await?;

        // Update inode state to Cooling
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cooling_state = NodeState::Cooling(crate::FileCoolingState {
            fb_handle: handle_id.to_string(),
            closed_at: now,
        });

        self.fsmeta
            .update_inode_state(fb.file_inode_id, cooling_state, None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to update inode state: {}", e)))?;

        // Release lease
        let session = SessionId(format!("{}:{}", self.instance, fb.file_inode_id));
        let _ = self
            .fsmeta
            .release_file_lease(fb.file_inode_id, session, 0)
            .await;

        Ok(())
    }

    pub async fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        let src_stat = self.stat(src).await?;

        if src_stat.kind == PathKind::NotFound {
            return Err(NdnError::NotFound(format!("source {} not found", src.as_str())));
        }

        // Check not working
        if let Some(state) = &src_stat.state {
            if matches!(state, NodeState::Working(_) | NodeState::Cooling(_)) {
                return Err(NdnError::InvalidState(
                    "source must be committed to snapshot".to_string(),
                ));
            }
        }

        // Copy the binding
        self.copy_path(src, target).await
    }

    // ========== Pull Operations ==========

    pub async fn pull(&self, path: &NdmPath, ctx: PullContext) -> NdnResult<()> {
        let stat = self.stat(path).await?;
        if let Some(obj_id) = stat.obj_id {
            self.pull_by_objid(obj_id, ctx).await
        } else {
            Err(NdnError::NotFound("no object to pull".to_string()))
        }
    }

    pub async fn pull_by_objid(&self, obj_id: ObjId, _ctx: PullContext) -> NdnResult<()> {
        let fetcher = self
            .fetcher
            .as_ref()
            .ok_or_else(|| NdnError::InvalidState("fetcher not configured".to_string()))?;

        fetcher.schedule_pull_obj(&obj_id).await
    }

    pub async fn pull_chunk(&self, chunk_id: ChunkId, _ctx: PullContext) -> NdnResult<()> {
        let fetcher = self
            .fetcher
            .as_ref()
            .ok_or_else(|| NdnError::InvalidState("fetcher not configured".to_string()))?;

        fetcher.schedule_pull_chunk(&chunk_id).await
    }

    // ========== Eviction ==========

    pub async fn erase_obj_by_id(&self, obj_id: &ObjId) -> NdnResult<()> {
        // This only removes physical data, not the path binding
        // Implementation depends on store interface
        let _store = self.store.lock().await;
        // TODO: store.erase_object_data(obj_id)
        log::warn!("erase_obj_by_id not fully implemented for {}", obj_id);
        Ok(())
    }

    // ========== Chunk Operations (for ndn_router compatibility) ==========

    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool> {
        let store = self.store.lock().await;
        Ok(store.have_chunk_impl(chunk_id).await)
    }

    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(named_store::ChunkState, u64, String)> {
        let store = self.store.lock().await;
        store.query_chunk_state_impl(chunk_id).await
    }

    pub async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        _opts: ReadOptions,
    ) -> NdnResult<(ndn_lib::ChunkReader, u64)> {
        let store = self.store.lock().await;
        store.open_chunk_reader_impl(chunk_id, offset, false).await
    }

    // ========== Admin Operations ==========

    pub async fn expand_capacity(&self, _new_target: StoreTarget) -> NdnResult<u64> {
        // TODO: implement store layout expansion
        Err(NdnError::Unsupported(
            "expand_capacity not implemented".to_string(),
        ))
    }

    pub async fn publish_dir(&self, path: &NdmPath) -> NdnResult<ObjId> {
        let resolved = self.resolve_path(path).await?;
        let (dir_id, dir_node) =
            resolved.ok_or_else(|| NdnError::NotFound("path not found".to_string()))?;

        if dir_node.kind != NodeKind::Dir {
            return Err(NdnError::InvalidParam("path is not a directory".to_string()));
        }

        // If already committed, return existing ObjId
        if let Some(obj_id) = dir_node.base_obj_id {
            return Ok(obj_id);
        }

        // TODO: implement directory materialization (cacl_dir logic)
        Err(NdnError::Unsupported(
            "directory materialization not fully implemented".to_string(),
        ))
    }

    // ========== Helper Methods ==========

    /// Ensure directory inode exists at path, creating parent directories as needed.
    /// Uses iterative approach to avoid async recursion.
    async fn ensure_dir_inode(&self, path: &NdmPath) -> NdnResult<IndexNodeId> {
        if path.is_root() {
            return self.fsmeta.root_dir().await.map_err(|e| {
                NdnError::Internal(format!("failed to get root dir: {}", e))
            });
        }

        // Try to resolve existing path first
        if let Some((id, node)) = self.resolve_path(path).await? {
            if node.kind != NodeKind::Dir {
                return Err(NdnError::InvalidParam(format!(
                    "{} is not a directory",
                    path.as_str()
                )));
            }
            return Ok(id);
        }

        // Collect all missing ancestors
        let mut missing_paths = Vec::new();
        let mut current_path = path.clone();
        
        loop {
            if current_path.is_root() {
                break;
            }
            
            if let Some((_, node)) = self.resolve_path(&current_path).await? {
                if node.kind != NodeKind::Dir {
                    return Err(NdnError::InvalidParam(format!(
                        "{} is not a directory",
                        current_path.as_str()
                    )));
                }
                break; // Found an existing directory
            }
            
            missing_paths.push(current_path.clone());
            if let Some((parent, _)) = current_path.split_parent_name() {
                current_path = parent;
            } else {
                break;
            }
        }

        // Create directories from root to leaf
        for missing_path in missing_paths.into_iter().rev() {
            self.create_dir_internal(&missing_path).await?;
        }

        // Return the final directory inode
        let resolved = self.resolve_path(path).await?;
        resolved
            .map(|(id, _)| id)
            .ok_or_else(|| NdnError::Internal("failed to create directory".to_string()))
    }

    /// Internal directory creation that doesn't recursively call ensure_dir_inode.
    /// Assumes parent directory already exists.
    async fn create_dir_internal(&self, path: &NdmPath) -> NdnResult<()> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| NdnError::InvalidParam("invalid path".to_string()))?;

        let parent_id = if parent_path.is_root() {
            self.fsmeta.root_dir().await.map_err(|e| {
                NdnError::Internal(format!("failed to get root dir: {}", e))
            })?
        } else {
            let resolved = self.resolve_path(&parent_path).await?;
            resolved
                .map(|(id, _)| id)
                .ok_or_else(|| NdnError::Internal("parent directory not found".to_string()))?
        };

        // Check if already exists
        let existing = self
            .fsmeta
            .get_dentry(parent_id, name.clone(), None)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

        if let Some(d) = existing {
            if !matches!(d.target, DentryTarget::Tombstone) {
                // Already exists, nothing to do
                return Ok(());
            }
        }

        // Create directory inode
        let txid = self
            .fsmeta
            .begin_txn()
            .await
            .map_err(|e| NdnError::Internal(format!("failed to begin txn: {}", e)))?;

        let new_node = NodeRecord {
            inode_id: 0,
            kind: NodeKind::Dir,
            read_only: false,
            base_obj_id: None,
            state: NodeState::DirNormal,
            rev: None,
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        let new_id = self
            .fsmeta
            .alloc_inode(new_node, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to alloc inode: {}", e)))?;

        // Link dentry via upsert_dentry
        self.fsmeta
            .upsert_dentry(parent_id, name.clone(), DentryTarget::IndexNodeId(new_id), Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to upsert dentry: {}", e)))?;

        self.fsmeta
            .commit(Some(txid))
            .await
            .map_err(|e| NdnError::Internal(format!("failed to commit: {}", e)))?;

        Ok(())
    }

    async fn move_path_with_opts(
        &self,
        old_path: &NdmPath,
        new_path: &NdmPath,
        opts: MoveOptions,
    ) -> NdnResult<()> {
        // Normalize and validate
        if old_path.is_root() {
            return Err(NdnError::InvalidParam("invalid name".to_string()));
        }

        let old_normalized = old_path.as_str().trim_end_matches('/');
        let new_normalized = new_path.as_str().trim_end_matches('/');

        if old_normalized == new_normalized {
            return Ok(()); // No-op
        }

        // Parse paths
        let (src_parent_path, src_name) = old_path
            .split_parent_name()
            .ok_or(NdnError::InvalidParam("invalid name".to_string()))?;
        let (dst_parent_path, dst_name) = new_path
            .split_parent_name()
            .ok_or(NdnError::InvalidParam("invalid name".to_string()))?;

        // Validate names
        if src_name.is_empty() || dst_name.is_empty() {
            return Err(NdnError::InvalidParam("invalid name".to_string()));
        }

        // Ensure parent directories exist
        let src_parent = self.ensure_dir_inode(&src_parent_path).await?;
        let dst_parent = self.ensure_dir_inode(&dst_parent_path).await?;

        // Get parent nodes
        let src_dir = self
            .fsmeta
            .get_inode(src_parent, None)
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?
            .ok_or(NdnError::NotFound("not found".to_string()))?;
        let dst_dir = self
            .fsmeta
            .get_inode(dst_parent, None)
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?
            .ok_or(NdnError::NotFound("not found".to_string()))?;

        if src_dir.kind != NodeKind::Dir || dst_dir.kind != NodeKind::Dir {
            return Err(NdnError::NotFound("not found".to_string()));
        }

        if src_dir.read_only || dst_dir.read_only {
            return Err(NdnError::PermissionDenied("read only".to_string()));
        }

        let src_rev0 = src_dir.rev.unwrap_or(0);
        let dst_rev0 = dst_dir.rev.unwrap_or(0);

        // Build move plan
        let source = self
            .plan_move_source(src_parent, &src_name, &src_dir, src_rev0)
            .await?;

        // Cycle prevention for directories
        if is_dir_like(&source) {
            if is_descendant_path(new_path, old_path) {
                return Err(NdnError::InvalidParam("invalid name".to_string()));
            }
        }

        // Check destination conflict
        self.check_dest_conflict_pre(dst_parent, &dst_name, &dst_dir, opts)
            .await?;

        // Build plan
        let plan = MovePlan {
            src_parent,
            src_name: src_name.clone(),
            dst_parent,
            dst_name: dst_name.clone(),
            src_rev0,
            dst_rev0,
            source,
        };

        // Apply in transaction
        self.apply_move_plan_txn(plan, opts).await
    }

    async fn check_dest_conflict_pre(
        &self,
        dst_parent: IndexNodeId,
        dst_name: &str,
        dst_dir_node: &NodeRecord,
        opts: MoveOptions,
    ) -> Result<(), NdnError> {
        // Check upper layer
        let dentry = self
            .fsmeta
            .get_dentry(dst_parent, dst_name.to_string(), None)
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        if let Some(d) = dentry {
            match d.target {
                DentryTarget::Tombstone => { /* treat as not-exists */ }
                DentryTarget::IndexNodeId(_) | DentryTarget::ObjId(_) => {
                    if !opts.overwrite_upper {
                        return Err(NdnError::AlreadyExists("already exists".to_string()));
                    }
                }
            }
        }

        if !opts.strict_check_base {
            return Ok(());
        }

        // Check base if strict mode
        let base = match &dst_dir_node.base_obj_id {
            None => return Ok(()),
            Some(oid) => oid.clone(),
        };

        let store = self.store.lock().await;
        let is_materialized = store.is_object_exist(&base).await?;
        if !is_materialized {
            return Err(NdnError::NotFound("need pull".to_string()));
        }

        // TODO: check children in base DirObject
        // For now, allow the move
        Ok(())
    }

    async fn plan_move_source(
        &self,
        src_parent: IndexNodeId,
        src_name: &str,
        src_dir_node: &NodeRecord,
        src_rev0: u64,
    ) -> Result<MoveSource, NdnError> {
        // Check upper first
        let dentry = self
            .fsmeta
            .get_dentry(src_parent, src_name.to_string(), None)
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        if let Some(d) = dentry {
            return match d.target {
                DentryTarget::Tombstone => Err(NdnError::NotFound("not found".to_string())),
                DentryTarget::IndexNodeId(fid) => {
                    let inode = self
                        .fsmeta
                        .get_inode(fid, None)
                        .await
                        .map_err(|e| NdnError::Internal(e.to_string()))?
                        .ok_or(NdnError::NotFound("not found".to_string()))?;
                    let kind_hint = match inode.kind {
                        NodeKind::Dir => ObjectKind::Dir,
                        NodeKind::File => ObjectKind::File,
                        NodeKind::Object => ObjectKind::Unknown,
                    };
                    Ok(MoveSource::Upper {
                        target: DentryTarget::IndexNodeId(fid),
                        kind_hint,
                    })
                }
                DentryTarget::ObjId(oid) => Ok(MoveSource::Upper {
                    target: DentryTarget::ObjId(oid),
                    kind_hint: ObjectKind::Unknown,
                }),
            };
        }

        // Upper miss => check base
        let base0 = src_dir_node.base_obj_id.clone();
        let base_oid = base0.clone().ok_or(NdnError::NotFound("not found".to_string()))?;

        let store = self.store.lock().await;
        let is_materialized = store.is_object_exist(&base_oid).await?;
        if !is_materialized {
            return Err(NdnError::NotFound("need pull".to_string()));
        }

        // TODO: look up child in base DirObject
        // For now, return NotFound
        Err(NdnError::NotFound("not found".to_string()))
    }

    async fn apply_move_plan_txn(
        &self,
        plan: MovePlan,
        _opts: MoveOptions,
    ) -> NdnResult<()> {
        let txid = self
            .fsmeta
            .begin_txn()
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        // Verify revisions haven't changed (OCC)
        let src_dir = self
            .fsmeta
            .get_inode(plan.src_parent, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?
            .ok_or(NdnError::NotFound("not found".to_string()))?;

        let dst_dir = self
            .fsmeta
            .get_inode(plan.dst_parent, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?
            .ok_or(NdnError::NotFound("not found".to_string()))?;

        if src_dir.rev.unwrap_or(0) != plan.src_rev0
            || dst_dir.rev.unwrap_or(0) != plan.dst_rev0
        {
            let _ = self.fsmeta.rollback(Some(txid)).await;
            return Err(NdnError::InvalidState("conflict".to_string()));
        }

        // Set tombstone at source
        self.fsmeta
            .set_tombstone(plan.src_parent, plan.src_name, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        // Upsert dentry at destination
        let target = match plan.source {
            MoveSource::Upper { target, .. } => target,
            MoveSource::Base { obj_id, .. } => DentryTarget::ObjId(obj_id),
        };

        self.fsmeta
            .upsert_dentry(plan.dst_parent, plan.dst_name, target, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        // Bump revisions
        if plan.src_parent == plan.dst_parent {
            self.fsmeta
                .bump_dir_rev(plan.src_parent, plan.src_rev0, Some(txid.clone()))
                .await
                .map_err(|e| NdnError::Internal(e.to_string()))?;
        } else {
            self.fsmeta
                .bump_dir_rev(plan.src_parent, plan.src_rev0, Some(txid.clone()))
                .await
                .map_err(|e| NdnError::Internal(e.to_string()))?;
            self.fsmeta
                .bump_dir_rev(plan.dst_parent, plan.dst_rev0, Some(txid.clone()))
                .await
                .map_err(|e| NdnError::Internal(e.to_string()))?;
        }

        self.fsmeta
            .commit(Some(txid))
            .await
            .map_err(|e| NdnError::Internal(e.to_string()))?;

        Ok(())
    }
}

// ========== Helper Functions ==========

fn is_dir_like(src: &MoveSource) -> bool {
    match src {
        MoveSource::Upper { kind_hint, .. } => *kind_hint == ObjectKind::Dir,
        MoveSource::Base { kind, .. } => *kind == ObjectKind::Dir,
    }
}

fn is_descendant_path(potential_child: &NdmPath, potential_parent: &NdmPath) -> bool {
    let child = potential_child.as_str().trim_end_matches('/');
    let parent = potential_parent.as_str().trim_end_matches('/');

    if child.len() <= parent.len() {
        return false;
    }

    child.starts_with(parent)
        && (child.as_bytes().get(parent.len()) == Some(&b'/') || parent == "/")
}

// ========== Background Manager ==========

pub struct BackgroundMgr {
    // Placeholder for background task management
    _staged_queue: Vec<String>,
    _lazy_migration_queue: Vec<String>,
}

impl BackgroundMgr {
    pub fn new() -> Self {
        Self {
            _staged_queue: Vec::new(),
            _lazy_migration_queue: Vec::new(),
        }
    }
}

impl Default for BackgroundMgr {
    fn default() -> Self {
        Self::new()
    }
}

// ========== Global Manager Registry ==========

pub type NamedDataMgrRef = Arc<tokio::sync::Mutex<NamedDataMgr>>;

lazy_static::lazy_static! {
    pub static ref NAMED_DATA_MGR_MAP: Arc<tokio::sync::Mutex<HashMap<String, NamedDataMgrRef>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
}

impl NamedDataMgr {
    pub async fn get_named_data_mgr_by_id(mgr_id: Option<&str>) -> Option<NamedDataMgrRef> {
        let id = mgr_id.unwrap_or("default");
        let map = NAMED_DATA_MGR_MAP.lock().await;
        map.get(id).cloned()
    }

    pub async fn register_named_data_mgr(
        mgr_id: &str,
        mgr: NamedDataMgr,
    ) -> NdnResult<NamedDataMgrRef> {
        let mgr_ref = Arc::new(tokio::sync::Mutex::new(mgr));
        let mut map = NAMED_DATA_MGR_MAP.lock().await;
        map.insert(mgr_id.to_string(), mgr_ref.clone());
        Ok(mgr_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ndm_path_split() {
        let path = NdmPath::new("/foo/bar/baz");
        let (parent, name) = path.split_parent_name().unwrap();
        assert_eq!(parent.as_str(), "/foo/bar");
        assert_eq!(name, "baz");

        let root_child = NdmPath::new("/foo");
        let (parent, name) = root_child.split_parent_name().unwrap();
        assert_eq!(parent.as_str(), "/");
        assert_eq!(name, "foo");

        let root = NdmPath::new("/");
        assert!(root.split_parent_name().is_none());
        assert!(root.is_root());
    }

    #[test]
    fn test_is_descendant_path() {
        assert!(is_descendant_path(
            &NdmPath::new("/a/b/c"),
            &NdmPath::new("/a/b")
        ));
        assert!(is_descendant_path(
            &NdmPath::new("/a/b/c"),
            &NdmPath::new("/a")
        ));
        assert!(is_descendant_path(
            &NdmPath::new("/a/b"),
            &NdmPath::new("/")
        ));
        assert!(!is_descendant_path(
            &NdmPath::new("/a/b"),
            &NdmPath::new("/a/b")
        ));
        assert!(!is_descendant_path(
            &NdmPath::new("/a/bc"),
            &NdmPath::new("/a/b")
        ));
    }
}
