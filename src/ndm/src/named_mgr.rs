//! Named Data Manager (NDM) - Core orchestration layer
//!
//! This module coordinates fs_meta, fs_buffer, and named_store to provide
//! a unified file/directory namespace with overlay semantics.

use std::collections::HashMap;
use std::sync::Arc;

use fs_buffer::{FileBufferSeekWriter, FileBufferService};
use named_store::{ChunkStoreState, NamedStoreMgr};
use ndn_lib::{
    ChunkId, DirObject, NdmPath, NdnError, NdnResult, ObjId, SimpleMapItem, OBJ_TYPE_DIR,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    DentryRecord, DentryTarget, FsMetaClient, IndexNodeId, MoveOptions, NodeKind, NodeRecord,
    NodeState, ObjStat,
};

// ------------------------------
// Basic Types
// ------------------------------

/// Instance identifier for a NamedDataMgr
pub type NdmInstanceId = String;

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

//Open write flags
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenWriteFlag {
    /// Append to existing file (file must exist)
    /// Returns error if file not found
    Append,

    /// Continue previous write session (file must exist, state must be Cooling/Working)
    /// For resuming interrupted writes
    ContinueWrite,

    /// Create new file exclusively (fails if file exists)
    CreateExclusive,

    /// Create if not exist, truncate if exists
    CreateOrTruncate,

    /// Create if not exist, append if exists (useful for distributed logging)
    CreateOrAppend,
}

// ------------------------------
// NamedDataMgr Implementation
// NamedDataMgr是NDM的Client，一定会在进程内使用
// NamedDataMgr 整合来自各个组件的返回，构建FileSystem 数据视图
// ------------------------------

pub struct NamedDataMgr {
    pub instance: NdmInstanceId,

    fsmeta: Arc<FsMetaClient>,
    fsbuffer: Arc<dyn FileBufferService>,
    fetcher: Option<Arc<dyn NdnFetcher>>,

    /// Default commit policy
    default_commit_policy: CommitPolicy,

    /// Background task manager
    bg: Arc<tokio::sync::Mutex<BackgroundMgr>>,

    /// Optional store layout manager for multi-version store fallback
    /// When set, get_object operations will try multiple layout versions
    layout_mgr: Option<Arc<NamedStoreMgr>>,
}

impl NamedDataMgr {
    pub fn new(
        instance: NdmInstanceId,
        fsmeta: Arc<FsMetaClient>,
        buffer: Arc<dyn FileBufferService>,
        fetcher: Option<Arc<dyn NdnFetcher>>,
        default_commit_policy: CommitPolicy,
    ) -> Self {
        Self {
            instance,
            fsmeta,
            fsbuffer: buffer,
            fetcher,
            default_commit_policy,
            bg: Arc::new(tokio::sync::Mutex::new(BackgroundMgr::new())),
            layout_mgr: None,
        }
    }

    /// Create with store layout manager for multi-version store fallback
    pub fn with_layout_mgr(
        instance: NdmInstanceId,
        fsmeta: Arc<FsMetaClient>,
        buffer: Arc<dyn FileBufferService>,
        fetcher: Option<Arc<dyn NdnFetcher>>,
        default_commit_policy: CommitPolicy,
        layout_mgr: Arc<NamedStoreMgr>,
    ) -> Self {
        Self {
            instance,
            fsmeta,
            fsbuffer: buffer,
            fetcher,
            default_commit_policy,
            bg: Arc::new(tokio::sync::Mutex::new(BackgroundMgr::new())),
            layout_mgr: Some(layout_mgr),
        }
    }

    /// Set the store layout manager
    pub fn set_layout_mgr(&mut self, layout_mgr: Arc<NamedStoreMgr>) {
        self.layout_mgr = Some(layout_mgr);
    }

    /// Get the store layout manager if set
    pub fn layout_mgr(&self) -> Option<&Arc<NamedStoreMgr>> {
        self.layout_mgr.as_ref()
    }

    // ========== Basic Operations ==========

    pub async fn stat(&self, path: &NdmPath) -> NdnResult<PathStat> {
        let resolved = self.fsmeta.resolve_path(path).await?;
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
        let stat = self
            .fsmeta
            .obj_stat_get(obj_id.clone())
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get obj stat: {}", e)))?;

        stat.ok_or_else(|| NdnError::NotFound(format!("object {} not found", obj_id)))
    }

    // ========== Write Operations ==========

    pub async fn set_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()> {
        self.fsmeta.set_file(path, obj_id).await
    }

    pub async fn set_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()> {
        self.fsmeta.set_dir(path, dir_obj_id).await
    }

    pub async fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        self.fsmeta.delete(path).await
    }

    pub async fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()> {
        self.move_path_with_opts(old_path, new_path, MoveOptions::default())
            .await
    }

    //创建软链接，目标必须是文件或者目录，不能是符号链接
    //LINK: link_path -> target
    pub async fn make_link(&self, link_path: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        self.fsmeta.make_link(target, link_path).await
    }

    pub async fn copy_file(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        let _ = (src, target);
        Err(NdnError::Unsupported(
            "copy_file not implemented".to_string(),
        ))
    }

    pub async fn copy_dir(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        //这是一个复杂的函数，需要复制大量的inode，做成fs-meta的任务可能会导致性能问题，这里要考虑如何阶段性的实现：
        // fsmeta上有复制任务管理器
        // 先copy到一个目录然后rename到target?
        unimplemented!()
    }

    //快照的逻辑是copy_dir的特殊情况，复制后把target设置为readonly,并很快会触发物化流程冻结
    pub async fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        unimplemented!()
    }

    // ========== Directory Operations ==========
    pub async fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        self.fsmeta.create_dir(path).await
    }

    pub async fn list(
        &self,
        path: &NdmPath,
        _pos: u32,
        _page_size: u32,
    ) -> NdnResult<Vec<(String, PathStat)>> {
        let resolved = self.fsmeta.resolve_path(path).await?;
        let (dir_id, dir_node) =
            resolved.ok_or_else(|| NdnError::NotFound("path not found".to_string()))?;

        if dir_node.kind != NodeKind::Dir {
            return Err(NdnError::InvalidParam(
                "path is not a directory".to_string(),
            ));
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
                    let node =
                        self.fsmeta.get_inode(*id, None).await.map_err(|e| {
                            NdnError::Internal(format!("failed to get inode: {}", e))
                        })?;

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
            return Err(NdnError::NotFound(format!(
                "path {} not found",
                path.as_str()
            )));
        }

        // If there's an inode with working state, use buffer
        if let Some(_inode_id) = stat.inode_id {
            if let Some(state) = &stat.state {
                match state {
                    NodeState::Working(ws) => {
                        // Open reader from buffer
                        let fb = self.fsbuffer.get_buffer(&ws.fb_handle).await?;
                        let reader = self
                            .fsbuffer
                            .open_reader(&fb, std::io::SeekFrom::Start(0))
                            .await?;
                        // Get file size from metadata or estimate
                        return Ok((Box::new(reader), 0)); // TODO: get actual size
                    }
                    NodeState::Cooling(cs) => {
                        let fb = self.fsbuffer.get_buffer(&cs.fb_handle).await?;
                        let reader = self
                            .fsbuffer
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
            return self
                .open_reader_by_id(&obj_id, None, ReadOptions::default())
                .await;
        }

        Err(NdnError::NotFound("no readable content".to_string()))
    }

    //move to store_layout_mgr
    pub async fn open_reader_by_id(
        &self,
        obj_id: &ObjId,
        _inner_path: Option<&InnerPath>,
        _opts: ReadOptions,
    ) -> NdnResult<(Box<dyn tokio::io::AsyncRead + Send + Unpin>, u64)> {
        // Check if object is a chunk
        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            let layout_mgr = self.layout_mgr.as_ref().ok_or_else(|| {
                NdnError::NotFound("store layout manager not configured".to_string())
            })?;
            let (reader, size) = layout_mgr.open_chunk_reader(&chunk_id, 0, false).await?;
            return Ok((Box::new(reader), size));
        }

        // TODO: handle other object types (FileObject, DirObject, etc.)
        Err(NdnError::Unsupported(
            "non-chunk object reading not implemented".to_string(),
        ))
    }

    pub async fn get_object_by_path(&self, path: &NdmPath) -> NdnResult<String> {
        let stat = self.stat(path).await?;
        if stat.kind == PathKind::NotFound {
            return Err(NdnError::NotFound(format!(
                "path {} not found",
                path.as_str()
            )));
        }

        let obj_id = stat
            .obj_id
            .ok_or_else(|| NdnError::NotFound("no object bound to path".to_string()))?;

        let obj = self.get_object(&obj_id).await?;
        Ok(serde_json::to_string(&obj).map_err(|e| NdnError::Internal(e.to_string()))?)
    }

    /// Internal method to get object with multi-version layout fallback
    ///
    /// If layout_mgr is set:
    /// 1. Try current layout version first
    /// 2. If NotFound, try previous layout versions
    /// 3. Return the first successful result or final error
    ///
    /// If layout_mgr is not set:
    /// - Use the default store directly
    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<serde_json::Value> {
        // If layout manager is set, use multi-version fallback
        if let Some(layout_mgr) = &self.layout_mgr {
            return layout_mgr.get_object(obj_id).await;
        }

        Err(NdnError::NotFound(
            "store layout manager not configured".to_string(),
        ))
    }

    /// Load existing file chunks from base object for append operations
    async fn load_file_chunklist(&self, _obj_id: &ObjId) -> NdnResult<Vec<ChunkId>> {
        Ok(Vec::new())
    }

    // ========== Write Operations (File) ==========

    pub async fn open_file_writer(
        &self,
        path: &NdmPath,
        flag: OpenWriteFlag,
        expected_size: Option<u64>,
    ) -> NdnResult<(FileBufferSeekWriter, IndexNodeId)> {
        let file_handle_id = self
            .fsmeta
            .open_file_writer(path, flag, expected_size)
            .await?;
        let file_handle = self.fsbuffer.get_buffer(&file_handle_id).await?;

        let writer = self
            .fsbuffer
            .open_writer(&file_handle, std::io::SeekFrom::Start(0))
            .await?;
        Ok((writer, file_handle.file_inode_id))
    }

    pub async fn append(&self, path: &NdmPath, data: &[u8]) -> NdnResult<()> {
        let file_handle_id = self
            .fsmeta
            .open_file_writer(path, OpenWriteFlag::CreateOrAppend, None)
            .await?;
        let file_handle = self.fsbuffer.get_buffer(&file_handle_id).await?;
        let mut writer = self
            .fsbuffer
            .open_writer(&file_handle, std::io::SeekFrom::End(0))
            .await?;
        writer.write_all(data).await?;
        writer.flush().await?;
        self.fsmeta
            .close_file_writer(file_handle.file_inode_id)
            .await?;
        Ok(())
    }

    pub async fn close_file(&self, file_inode_id: IndexNodeId) -> NdnResult<()> {
        self.fsmeta.close_file_writer(file_inode_id).await
    }

    // ========== Pull Operations,need more think ==========

    // pub async fn pull(&self, path: &NdmPath, ctx: PullContext) -> NdnResult<()> {
    //     let stat = self.stat(path).await?;
    //     if let Some(obj_id) = stat.obj_id {
    //         self.pull_by_objid(obj_id, ctx).await
    //     } else {
    //         Err(NdnError::NotFound("no object to pull".to_string()))
    //     }
    // }

    // pub async fn pull_by_objid(&self, obj_id: ObjId, _ctx: PullContext) -> NdnResult<()> {
    //     let fetcher = self
    //         .fetcher
    //         .as_ref()
    //         .ok_or_else(|| NdnError::InvalidState("fetcher not configured".to_string()))?;

    //     fetcher.schedule_pull_obj(&obj_id).await
    // }

    // pub async fn pull_chunk(&self, chunk_id: ChunkId, _ctx: PullContext) -> NdnResult<()> {
    //     let fetcher = self
    //         .fetcher
    //         .as_ref()
    //         .ok_or_else(|| NdnError::InvalidState("fetcher not configured".to_string()))?;

    //     fetcher.schedule_pull_chunk(&chunk_id).await
    // }

    // ========== Eviction ==========

    pub async fn erase_obj_by_id(&self, obj_id: &ObjId) -> NdnResult<()> {
        let _ = obj_id;
        Err(NdnError::Unsupported(
            "erase_obj_by_id not implemented".to_string(),
        ))
    }

    // ========== Chunk Operations (for ndn_router , call to store_layout_mgr) ==========

    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;
        let (state, _, _) = layout_mgr.query_chunk_state(chunk_id).await?;
        Ok(state.can_open_reader())
    }

    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(named_store::ChunkStoreState, u64, String)> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;
        layout_mgr.query_chunk_state(chunk_id).await
    }

    pub async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        _opts: ReadOptions,
    ) -> NdnResult<(ndn_lib::ChunkReader, u64)> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;
        layout_mgr.open_chunk_reader(chunk_id, offset, false).await
    }

    // ========== Admin Operations ==========

    pub async fn expand_capacity(&self, _new_target: StoreTarget) -> NdnResult<u64> {
        // TODO: implement store layout expansion
        Err(NdnError::Unsupported(
            "expand_capacity not implemented".to_string(),
        ))
    }

    // ========== Helper Methods ==========
    // 手工物化一个目录，把目录的objid设置为path的objid，并设置为readonly
    pub async fn publish_dir(&self, path: &NdmPath) -> NdnResult<ObjId> {
        let _ = path;
        Err(NdnError::Unsupported(
            "publish_dir not implemented".to_string(),
        ))
    }

    //  move to store_layout_mgr
    /// Lookup a child entry in a DirObject by inner_path.
    /// Returns the ObjId if the child exists and is a directory.
    #[allow(dead_code)]
    async fn lookup_in_dir_object(
        &self,
        dir_obj_id: &ObjId,
        child_name: &str,
    ) -> NdnResult<Option<ObjId>> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;

        let obj_json = layout_mgr
            .get_object(dir_obj_id)
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get DirObject: {}", e)))?;

        // Parse as DirObject
        let dir_obj: DirObject = serde_json::from_value(obj_json)
            .map_err(|e| NdnError::Internal(format!("failed to parse DirObject: {}", e)))?;

        // Look up child in the object map
        match dir_obj.get(child_name) {
            Some(item) => {
                match item {
                    SimpleMapItem::ObjId(child_obj_id) => {
                        if child_obj_id.obj_type == OBJ_TYPE_DIR {
                            Ok(Some(child_obj_id.clone()))
                        } else {
                            // Child exists but is not a directory
                            Err(NdnError::InvalidParam(format!(
                                "{} in DirObject is not a directory",
                                child_name
                            )))
                        }
                    }
                    SimpleMapItem::Object(obj_type, _) | SimpleMapItem::ObjectJwt(obj_type, _) => {
                        if obj_type == OBJ_TYPE_DIR {
                            // Embedded directory object - need to compute its ObjId
                            let (child_obj_id, _) = item.get_obj_id().map_err(|e| {
                                NdnError::Internal(format!("failed to get obj_id: {}", e))
                            })?;
                            Ok(Some(child_obj_id))
                        } else {
                            Err(NdnError::InvalidParam(format!(
                                "{} in DirObject is not a directory",
                                child_name
                            )))
                        }
                    }
                }
            }
            None => Ok(None), // Child not found in DirObject
        }
    }

    async fn move_path_with_opts(
        &self,
        old_path: &NdmPath,
        new_path: &NdmPath,
        opts: MoveOptions,
    ) -> NdnResult<()> {
        self.fsmeta
            .move_path_with_opts(old_path, new_path, opts)
            .await
    }
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
