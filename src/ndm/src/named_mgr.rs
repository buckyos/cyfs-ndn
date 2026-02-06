//! Named Data Manager (NDM) - Core orchestration layer
//!
//! This module coordinates fs_meta, fs_buffer, and named_store to provide
//! a unified file/directory namespace with overlay semantics.

use std::collections::HashMap;
use std::sync::Arc;

use fs_buffer::{FileBufferSeekWriter, FileBufferService};
use named_store::{
    NamedLocalConfig, NamedLocalStore, NamedStoreMgr, StoreLayout,
    StoreTarget as LayoutStoreTarget,
};
use ndn_lib::{
    ChunkId, DirObject, FileObject, NdmPath, NdnError, NdnResult, ObjId, SimpleChunkList,
    SimpleMapItem, OBJ_TYPE_CHUNK_LIST_SIMPLE, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
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
        if let Some((inode_id, node)) = resolved {
            let kind = match node.kind {
                NodeKind::File => PathKind::File,
                NodeKind::Dir => PathKind::Dir,
                NodeKind::Object => PathKind::File,
            };

            let obj_id = Self::node_obj_id(&node).or_else(|| node.base_obj_id.clone());
            let size = if let Some(ref id) = obj_id {
                self.obj_size_from_obj_id(id).await
            } else {
                None
            };

            return Ok(PathStat {
                kind,
                size,
                obj_id,
                inode_id: Some(inode_id),
                state: Some(node.state),
            });
        }

        if path.is_root() {
            return Ok(PathStat {
                kind: PathKind::NotFound,
                size: None,
                obj_id: None,
                inode_id: None,
                state: None,
            });
        }

        if let Some((parent_path, name)) = path.split_parent_name() {
            if let Some((parent_id, _)) = self.fsmeta.resolve_path(&parent_path).await? {
                let dentry = self
                    .fsmeta
                    .get_dentry(parent_id, name, None)
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

                if let Some(dentry) = dentry {
                    match dentry.target {
                        DentryTarget::ObjId(obj_id) => return self.path_stat_from_obj_id(obj_id).await,
                        DentryTarget::IndexNodeId(id) => {
                            if let Some(node) = self
                                .fsmeta
                                .get_inode(id, None)
                                .await
                                .map_err(|e| {
                                    NdnError::Internal(format!("failed to get inode: {}", e))
                                })?
                            {
                                let kind = match node.kind {
                                    NodeKind::File => PathKind::File,
                                    NodeKind::Dir => PathKind::Dir,
                                    NodeKind::Object => PathKind::File,
                                };
                                let obj_id = Self::node_obj_id(&node);
                                let size = if let Some(ref id) = obj_id {
                                    self.obj_size_from_obj_id(id).await
                                } else {
                                    None
                                };
                                return Ok(PathStat {
                                    kind,
                                    size,
                                    obj_id,
                                    inode_id: Some(id),
                                    state: Some(node.state),
                                });
                            }
                        }
                        DentryTarget::Tombstone => {}
                    }
                }
            }
        }

        Ok(PathStat {
            kind: PathKind::NotFound,
            size: None,
            obj_id: None,
            inode_id: None,
            state: None,
        })
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
        self.fsmeta.make_link(link_path, target).await
    }

    pub async fn copy_file(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        let stat = self.stat(src).await?;
        if stat.kind != PathKind::File {
            return Err(NdnError::InvalidParam("source is not a file".to_string()));
        }
        let obj_id = stat
            .obj_id
            .ok_or_else(|| NdnError::InvalidState("source file not published".to_string()))?;
        self.set_file(target, obj_id).await
    }

    pub async fn copy_dir(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        let stat = self.stat(src).await?;
        if stat.kind != PathKind::Dir {
            return Err(NdnError::InvalidParam("source is not a directory".to_string()));
        }
        let obj_id = stat
            .obj_id
            .ok_or_else(|| NdnError::InvalidState("source directory not published".to_string()))?;
        self.set_dir(target, obj_id).await
    }

    //快照的逻辑是copy_dir的特殊情况，复制后把target设置为readonly,并很快会触发物化流程冻结
    pub async fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        self.copy_dir(src, target).await
    }

    // ========== Directory Operations ==========
    pub async fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        self.fsmeta.create_dir(path).await
    }

    pub async fn list(
        &self,
        path: &NdmPath,
        pos: u32,
        page_size: u32,
    ) -> NdnResult<Vec<(String, PathStat)>> {
        let resolved = self.fsmeta.resolve_path(path).await?;
        if let Some((dir_id, dir_node)) = resolved {
            if dir_node.kind != NodeKind::Dir {
                return Err(NdnError::InvalidParam(
                    "path is not a directory".to_string(),
                ));
            }

            let dentries = self
                .fsmeta
                .list_dentries(dir_id, None)
                .await
                .map_err(|e| NdnError::Internal(format!("failed to list dentries: {}", e)))?;

            let mut upper_map: HashMap<String, DentryRecord> = HashMap::new();
            for dentry in dentries {
                upper_map.insert(dentry.name.clone(), dentry);
            }

            let mut results: HashMap<String, PathStat> = HashMap::new();

            if let Some(base_obj_id) = dir_node.base_obj_id.clone() {
                let dir_obj = self.load_dir_object(&base_obj_id).await?;
                for (name, item) in dir_obj.iter() {
                    if let Some(upper) = upper_map.get(name) {
                        if matches!(upper.target, DentryTarget::Tombstone) {
                            continue;
                        }
                        continue;
                    }
                    let child_stat = self.path_stat_from_simple_map_item(item).await?;
                    results.insert(name.clone(), child_stat);
                }
            }

            for (name, dentry) in upper_map {
                if matches!(dentry.target, DentryTarget::Tombstone) {
                    continue;
                }

                let child_stat = match dentry.target {
                    DentryTarget::IndexNodeId(id) => {
                        let node = self
                            .fsmeta
                            .get_inode(id, None)
                            .await
                            .map_err(|e| NdnError::Internal(format!("failed to get inode: {}", e)))?;

                        match node {
                            Some(n) => {
                                let kind = match n.kind {
                                    NodeKind::File => PathKind::File,
                                    NodeKind::Dir => PathKind::Dir,
                                    NodeKind::Object => PathKind::File,
                                };
                                let obj_id = Self::node_obj_id(&n);
                                let size = if let Some(ref id) = obj_id {
                                    self.obj_size_from_obj_id(id).await
                                } else {
                                    None
                                };
                                PathStat {
                                    kind,
                                    size,
                                    obj_id,
                                    inode_id: Some(id),
                                    state: Some(n.state),
                                }
                            }
                            None => continue,
                        }
                    }
                    DentryTarget::ObjId(obj_id) => self.path_stat_from_obj_id(obj_id).await?,
                    DentryTarget::Tombstone => continue,
                };

                results.insert(name, child_stat);
            }

            let mut out: Vec<(String, PathStat)> = results.into_iter().collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));

            if page_size > 0 {
                let start = pos as usize;
                if start >= out.len() {
                    return Ok(Vec::new());
                }
                let end = (start + page_size as usize).min(out.len());
                return Ok(out[start..end].to_vec());
            }

            return Ok(out);
        }

        if let Some((parent_path, name)) = path.split_parent_name() {
            if let Some((parent_id, _)) = self.fsmeta.resolve_path(&parent_path).await? {
                let dentry = self
                    .fsmeta
                    .get_dentry(parent_id, name, None)
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?;

                if let Some(dentry) = dentry {
                    match dentry.target {
                        DentryTarget::ObjId(obj_id) => {
                            if obj_id.obj_type != OBJ_TYPE_DIR {
                                return Err(NdnError::InvalidParam(
                                    "path is not a directory".to_string(),
                                ));
                            }
                            let dir_obj = self.load_dir_object(&obj_id).await?;
                            let mut out = Vec::new();
                            for (entry_name, item) in dir_obj.iter() {
                                let child_stat = self.path_stat_from_simple_map_item(item).await?;
                                out.push((entry_name.clone(), child_stat));
                            }
                            out.sort_by(|a, b| a.0.cmp(&b.0));
                            if page_size > 0 {
                                let start = pos as usize;
                                if start >= out.len() {
                                    return Ok(Vec::new());
                                }
                                let end = (start + page_size as usize).min(out.len());
                                return Ok(out[start..end].to_vec());
                            }
                            return Ok(out);
                        }
                        DentryTarget::IndexNodeId(_) | DentryTarget::Tombstone => {}
                    }
                }
            }
        }

        Err(NdnError::NotFound("path not found".to_string()))
    }

    // ========== Read Operations ==========

    pub async fn open_reader(
        &self,
        path: &NdmPath,
        _opts: ReadOptions,
    ) -> NdnResult<(Box<dyn tokio::io::AsyncRead + Send + Unpin>, u64)> {
        let resp = self.fsmeta.open_file_reader(path).await?;
        match resp {
            crate::OpenFileReaderResp::FileBufferId(handle_id) => {
                let fb = self.fsbuffer.get_buffer(&handle_id).await?;
                let reader = self
                    .fsbuffer
                    .open_reader(&fb, std::io::SeekFrom::Start(0))
                    .await?;
                Ok((Box::new(reader), 0))
            }
            crate::OpenFileReaderResp::Object(obj_id, inner_path) => {
                let layout_mgr = self.layout_mgr.as_ref().ok_or_else(|| {
                    NdnError::NotFound("store layout manager not configured".to_string())
                })?;
                let (reader, size) = layout_mgr.open_reader(&obj_id, inner_path, false).await?;
                Ok((Box::new(reader), size))
            }
        }
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

    async fn load_dir_object(&self, obj_id: &ObjId) -> NdnResult<DirObject> {
        let obj_json = self.get_object(obj_id).await?;
        serde_json::from_value(obj_json)
            .map_err(|e| NdnError::Internal(format!("failed to parse DirObject: {}", e)))
    }

    async fn load_file_object(&self, obj_id: &ObjId) -> NdnResult<FileObject> {
        let obj_json = self.get_object(obj_id).await?;
        serde_json::from_value(obj_json)
            .map_err(|e| NdnError::Internal(format!("failed to parse FileObject: {}", e)))
    }

    async fn load_chunk_list(&self, obj_id: &ObjId) -> NdnResult<SimpleChunkList> {
        let obj_json = self.get_object(obj_id).await?;
        SimpleChunkList::from_json_value(obj_json)
            .map_err(|e| NdnError::Internal(format!("failed to parse chunk list: {}", e)))
    }

    fn obj_kind_from_obj_id(obj_id: &ObjId) -> ObjectKind {
        if obj_id.obj_type == OBJ_TYPE_DIR {
            ObjectKind::Dir
        } else if obj_id.obj_type == OBJ_TYPE_FILE {
            ObjectKind::File
        } else {
            ObjectKind::Unknown
        }
    }

    async fn obj_size_from_obj_id(&self, obj_id: &ObjId) -> Option<u64> {
        match Self::obj_kind_from_obj_id(obj_id) {
            ObjectKind::File => self.load_file_object(obj_id).await.ok().map(|f| f.size),
            ObjectKind::Dir => self.load_dir_object(obj_id).await.ok().map(|d| d.total_size),
            ObjectKind::Unknown => None,
        }
    }

    async fn path_stat_from_obj_id(&self, obj_id: ObjId) -> NdnResult<PathStat> {
        let kind = match Self::obj_kind_from_obj_id(&obj_id) {
            ObjectKind::Dir => PathKind::Dir,
            ObjectKind::File => PathKind::File,
            ObjectKind::Unknown => PathKind::File,
        };
        let size = self.obj_size_from_obj_id(&obj_id).await;
        Ok(PathStat {
            kind,
            size,
            obj_id: Some(obj_id),
            inode_id: None,
            state: None,
        })
    }

    async fn path_stat_from_simple_map_item(
        &self,
        item: &SimpleMapItem,
    ) -> NdnResult<PathStat> {
        let (obj_id, _) = item.get_obj_id()?;
        let kind = match Self::obj_kind_from_obj_id(&obj_id) {
            ObjectKind::Dir => PathKind::Dir,
            ObjectKind::File => PathKind::File,
            ObjectKind::Unknown => PathKind::File,
        };

        let size = match item {
            SimpleMapItem::Object(obj_type, _) => {
                if obj_type == OBJ_TYPE_FILE {
                    let file_obj: FileObject = serde_json::from_value(item.get_obj()?).map_err(|e| {
                        NdnError::Internal(format!("failed to parse FileObject: {}", e))
                    })?;
                    Some(file_obj.size)
                } else if obj_type == OBJ_TYPE_DIR {
                    let dir_obj: DirObject = serde_json::from_value(item.get_obj()?).map_err(|e| {
                        NdnError::Internal(format!("failed to parse DirObject: {}", e))
                    })?;
                    Some(dir_obj.total_size)
                } else {
                    None
                }
            }
            SimpleMapItem::ObjectJwt(obj_type, _) => {
                if obj_type == OBJ_TYPE_FILE {
                    let file_obj: FileObject = serde_json::from_value(item.get_obj()?).map_err(|e| {
                        NdnError::Internal(format!("failed to parse FileObject: {}", e))
                    })?;
                    Some(file_obj.size)
                } else if obj_type == OBJ_TYPE_DIR {
                    let dir_obj: DirObject = serde_json::from_value(item.get_obj()?).map_err(|e| {
                        NdnError::Internal(format!("failed to parse DirObject: {}", e))
                    })?;
                    Some(dir_obj.total_size)
                } else {
                    None
                }
            }
            SimpleMapItem::ObjId(_) => self.obj_size_from_obj_id(&obj_id).await,
        };

        Ok(PathStat {
            kind,
            size,
            obj_id: Some(obj_id),
            inode_id: None,
            state: None,
        })
    }

    fn node_obj_id(node: &NodeRecord) -> Option<ObjId> {
        match &node.state {
            NodeState::Linked(ls) => Some(ls.obj_id.clone()),
            NodeState::Finalized(fs) => Some(fs.obj_id.clone()),
            _ => node.base_obj_id.clone(),
        }
    }

    /// Load existing file chunks from base object for append operations
    async fn load_file_chunklist(&self, obj_id: &ObjId) -> NdnResult<Vec<ChunkId>> {
        let Some(_layout_mgr) = self.layout_mgr.as_ref() else {
            return Ok(Vec::new());
        };

        let file_obj = match self.load_file_object(obj_id).await {
            Ok(obj) => obj,
            Err(_) => return Ok(Vec::new()),
        };

        if file_obj.content.is_empty() {
            return Ok(Vec::new());
        }

        let content_id = match ObjId::new(file_obj.content.as_str()) {
            Ok(id) => id,
            Err(_) => return Ok(Vec::new()),
        };

        if content_id.is_chunk() {
            return Ok(vec![ChunkId::from_obj_id(&content_id)]);
        }

        if content_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
            let chunk_list = match self.load_chunk_list(&content_id).await {
                Ok(list) => list,
                Err(_) => return Ok(Vec::new()),
            };
            return Ok(chunk_list.body);
        }

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
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;

        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            layout_mgr.remove_chunk(&chunk_id).await?;
            return Ok(());
        }

        layout_mgr.remove_object(obj_id).await?;
        Ok(())
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
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;

        let store_path = std::path::PathBuf::from(_new_target.path.clone());
        let store = NamedLocalStore::from_config(
            Some(_new_target.store_id.clone()),
            store_path,
            NamedLocalConfig::default(),
        )
        .await?;
        let store_ref = Arc::new(tokio::sync::Mutex::new(store));
        layout_mgr.register_store(store_ref).await;

        let current = layout_mgr.current_layout().await;
        let mut targets = current.as_ref().map(|l| l.targets.clone()).unwrap_or_default();

        if !targets
            .iter()
            .any(|t| t.store_id == _new_target.store_id)
        {
            targets.push(LayoutStoreTarget {
                store_id: _new_target.store_id.clone(),
                device_did: None,
                capacity: None,
                used: None,
                readonly: false,
                enabled: true,
                weight: 1,
            });
        }

        let epoch = current.as_ref().map(|l| l.epoch + 1).unwrap_or(1);
        let total_capacity = current.as_ref().map(|l| l.total_capacity).unwrap_or(0);
        let total_used = current.as_ref().map(|l| l.total_used).unwrap_or(0);
        let layout = StoreLayout::new(epoch, targets, total_capacity, total_used);
        layout_mgr.add_layout(layout).await;
        Ok(epoch)
    }

    // ========== Helper Methods ==========
    // 手工物化一个目录，把目录的objid设置为path的objid，并设置为readonly
    pub async fn publish_dir(&self, path: &NdmPath) -> NdnResult<ObjId> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;

        let resolved = self.fsmeta.resolve_path(path).await?;
        let (dir_id, _) =
            resolved.ok_or_else(|| NdnError::NotFound("path not found".to_string()))?;

        let txid = self
            .fsmeta
            .begin_txn()
            .await
            .map_err(|e| NdnError::Internal(format!("begin_txn failed: {}", e)))?;

        let mut node = match self
            .fsmeta
            .get_inode(dir_id, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("get_inode failed: {}", e)))?
        {
            Some(node) => node,
            None => {
                let _ = self.fsmeta.rollback(Some(txid.clone())).await;
                return Err(NdnError::NotFound("inode not found".to_string()));
            }
        };

        if node.kind != NodeKind::Dir {
            let _ = self.fsmeta.rollback(Some(txid.clone())).await;
            return Err(NdnError::InvalidParam("path is not a directory".to_string()));
        }

        let rev0 = node.rev.unwrap_or(0);

        let upper_dentries = self
            .fsmeta
            .list_dentries(dir_id, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("list_dentries failed: {}", e)))?;

        let mut upper_map: HashMap<String, DentryRecord> = HashMap::new();
        for dentry in upper_dentries.iter() {
            upper_map.insert(dentry.name.clone(), dentry.clone());
        }

        let mut entries: HashMap<String, ObjId> = HashMap::new();

        if let Some(base_obj_id) = node.base_obj_id.clone() {
            let dir_obj = self.load_dir_object(&base_obj_id).await?;
            for (name, item) in dir_obj.iter() {
                if let Some(upper) = upper_map.get(name) {
                    if matches!(upper.target, DentryTarget::Tombstone) {
                        continue;
                    }
                    continue;
                }
                let (obj_id, _) = item.get_obj_id()?;
                entries.insert(name.clone(), obj_id);
            }
        }

        for (name, dentry) in upper_map {
            if matches!(dentry.target, DentryTarget::Tombstone) {
                continue;
            }
            let obj_id = match dentry.target {
                DentryTarget::ObjId(obj_id) => obj_id,
                DentryTarget::IndexNodeId(inode_id) => {
                    let child = self
                        .fsmeta
                        .get_inode(inode_id, Some(txid.clone()))
                        .await
                        .map_err(|e| NdnError::Internal(format!("get_inode failed: {}", e)))?;
                    let child = match child {
                        Some(c) => c,
                        None => {
                            let _ = self.fsmeta.rollback(Some(txid.clone())).await;
                            return Err(NdnError::NotFound("child inode not found".to_string()));
                        }
                    };
                    Self::node_obj_id(&child).ok_or_else(|| {
                        NdnError::InvalidState(format!(
                            "child {} not published",
                            dentry.name
                        ))
                    })?
                }
                DentryTarget::Tombstone => continue,
            };
            entries.insert(name, obj_id);
        }

        let mut dir_obj = DirObject::new(None);
        for (name, obj_id) in entries.iter() {
            let kind = Self::obj_kind_from_obj_id(obj_id);
            match kind {
                ObjectKind::File => {
                    let file_obj = self.load_file_object(obj_id).await?;
                    dir_obj.file_count += 1;
                    dir_obj.file_size += file_obj.size;
                    dir_obj.total_size += file_obj.size;
                }
                ObjectKind::Dir => {
                    let sub_dir = self.load_dir_object(obj_id).await?;
                    dir_obj.total_size += sub_dir.total_size;
                }
                ObjectKind::Unknown => {}
            }

            dir_obj
                .object_map
                .insert(name.clone(), SimpleMapItem::ObjId(obj_id.clone()));
        }

        let (dir_obj_id, dir_obj_str) = dir_obj.gen_obj_id()?;
        layout_mgr.put_object(&dir_obj_id, &dir_obj_str).await?;

        let new_rev = self
            .fsmeta
            .bump_dir_rev(dir_id, rev0, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("bump_dir_rev failed: {}", e)))?;

        node.base_obj_id = Some(dir_obj_id.clone());
        node.state = NodeState::DirOverlay;
        node.rev = Some(new_rev);

        self.fsmeta
            .set_inode(node, Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("set_inode failed: {}", e)))?;

        for dentry in upper_dentries.iter() {
            self.fsmeta
                .remove_dentry_row(dir_id, dentry.name.clone(), Some(txid.clone()))
                .await
                .map_err(|e| NdnError::Internal(format!("remove_dentry failed: {}", e)))?;
        }

        self.fsmeta
            .commit(Some(txid.clone()))
            .await
            .map_err(|e| NdnError::Internal(format!("commit failed: {}", e)))?;

        Ok(dir_obj_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fs_buffer::{
        FileBufferRecord, FileBufferSeekReader, FileBufferSeekWriter, FileBufferService, NdmPath,
        SessionId, WriteLease,
    };
    use krpc::{RPCContext, RPCErrors};
    use ndn_lib::{ChunkHasher, ObjId};
    use named_store::ObjectState;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::AsyncWriteExt;

    struct DummyFsMeta;

    #[async_trait]
    impl FsMetaHandler for DummyFsMeta {
        async fn handle_root_dir(&self, _ctx: RPCContext) -> Result<IndexNodeId, RPCErrors> {
            Ok(0)
        }

        async fn handle_resolve_path(
            &self,
            _path: &ndn_lib::NdmPath,
            _ctx: RPCContext,
        ) -> NdnResult<Option<(IndexNodeId, NodeRecord)>> {
            Ok(None)
        }

        async fn handle_begin_txn(&self, _ctx: RPCContext) -> Result<String, RPCErrors> {
            Ok("tx".to_string())
        }

        async fn handle_get_inode(
            &self,
            _id: IndexNodeId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Option<NodeRecord>, RPCErrors> {
            Ok(None)
        }

        async fn handle_set_inode(
            &self,
            _node: NodeRecord,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_update_inode_state(
            &self,
            _node_id: IndexNodeId,
            _new_state: NodeState,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_alloc_inode(
            &self,
            _node: NodeRecord,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<IndexNodeId, RPCErrors> {
            Ok(0)
        }

        async fn handle_get_dentry(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Option<DentryRecord>, RPCErrors> {
            Ok(None)
        }

        async fn handle_list_dentries(
            &self,
            _parent: IndexNodeId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Vec<DentryRecord>, RPCErrors> {
            Ok(Vec::new())
        }

        async fn handle_upsert_dentry(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _target: DentryTarget,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_remove_dentry_row(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_set_tombstone(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_bump_dir_rev(
            &self,
            _dir: IndexNodeId,
            _expected_rev: u64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            Ok(0)
        }

        async fn handle_commit(&self, _txid: Option<String>, _ctx: RPCContext) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_rollback(&self, _txid: Option<String>, _ctx: RPCContext) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_acquire_file_lease(
            &self,
            _node_id: IndexNodeId,
            _session: SessionId,
            _ttl: std::time::Duration,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            Ok(0)
        }

        async fn handle_renew_file_lease(
            &self,
            _node_id: IndexNodeId,
            _session: SessionId,
            _lease_seq: u64,
            _ttl: std::time::Duration,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_release_file_lease(
            &self,
            _node_id: IndexNodeId,
            _session: SessionId,
            _lease_seq: u64,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_obj_stat_get(
            &self,
            _obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<Option<ObjStat>, RPCErrors> {
            Ok(None)
        }

        async fn handle_obj_stat_bump(
            &self,
            _obj_id: ObjId,
            _delta: i64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            Ok(0)
        }

        async fn handle_obj_stat_list_zero(
            &self,
            _older_than_ts: u64,
            _limit: u32,
            _ctx: RPCContext,
        ) -> Result<Vec<ObjId>, RPCErrors> {
            Ok(Vec::new())
        }

        async fn handle_obj_stat_delete_if_zero(
            &self,
            _obj_id: ObjId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<bool, RPCErrors> {
            Ok(false)
        }

        async fn handle_set_file(
            &self,
            _path: &ndn_lib::NdmPath,
            _obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            Ok(String::new())
        }

        async fn handle_set_dir(
            &self,
            _path: &ndn_lib::NdmPath,
            _dir_obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            Ok(String::new())
        }

        async fn handle_delete(&self, _path: &ndn_lib::NdmPath, _ctx: RPCContext) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_move_path_with_opts(
            &self,
            _old_path: &ndn_lib::NdmPath,
            _new_path: &ndn_lib::NdmPath,
            _opts: MoveOptions,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_make_link(
            &self,
            _link_path: &ndn_lib::NdmPath,
            _target: &ndn_lib::NdmPath,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_create_dir(&self, _path: &ndn_lib::NdmPath, _ctx: RPCContext) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_open_file_writer(
            &self,
            _path: &ndn_lib::NdmPath,
            _flag: OpenWriteFlag,
            _expected_size: Option<u64>,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            Err(RPCErrors::ReasonError("not supported".to_string()))
        }

        async fn handle_close_file_writer(
            &self,
            _file_inode_id: IndexNodeId,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_open_file_reader(
            &self,
            _path: ndn_lib::NdmPath,
            _ctx: RPCContext,
        ) -> Result<OpenFileReaderResp, RPCErrors> {
            Err(RPCErrors::ReasonError("not supported".to_string()))
        }
    }

    struct DummyBuffer;

    #[async_trait]
    impl FileBufferService for DummyBuffer {
        async fn alloc_buffer(
            &self,
            _path: &NdmPath,
            _file_inode_id: u64,
            _base_chunk_list: Vec<ChunkId>,
            _lease: &WriteLease,
            _expected_size: Option<u64>,
        ) -> NdnResult<FileBufferRecord> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn get_buffer(&self, _handle_id: &str) -> NdnResult<FileBufferRecord> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn open_reader(
            &self,
            _fb: &FileBufferRecord,
            _seek_from: std::io::SeekFrom,
        ) -> NdnResult<FileBufferSeekReader> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn open_writer(
            &self,
            _fb: &FileBufferRecord,
            _seek_from: std::io::SeekFrom,
        ) -> NdnResult<FileBufferSeekWriter> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn flush(&self, _fb: &FileBufferRecord) -> NdnResult<()> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn close(&self, _fb: &FileBufferRecord) -> NdnResult<()> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn append(&self, _fb: &FileBufferRecord, _data: &[u8]) -> NdnResult<()> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn cacl_name(&self, _fb: &FileBufferRecord) -> NdnResult<ObjId> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }

        async fn remove(&self, _fb: &FileBufferRecord) -> NdnResult<()> {
            Err(NdnError::Unsupported("dummy".to_string()))
        }
    }

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("{}_{}", prefix, nanos))
    }

    #[tokio::test]
    async fn test_erase_obj_by_id_removes_object_and_chunk() {
        let store_dir = unique_temp_dir("ndm_erase_test");
        tokio::fs::create_dir_all(&store_dir).await.unwrap();

        let store = NamedLocalStore::from_config(
            Some("test-store".to_string()),
            store_dir.clone(),
            NamedLocalConfig::default(),
        )
        .await
        .unwrap();
        let store_ref = Arc::new(tokio::sync::Mutex::new(store));

        let layout_mgr = Arc::new(NamedStoreMgr::new());
        layout_mgr.register_store(store_ref).await;

        let layout = StoreLayout::new(
            1,
            vec![LayoutStoreTarget {
                store_id: "test-store".to_string(),
                device_did: None,
                capacity: None,
                used: None,
                readonly: false,
                enabled: true,
                weight: 1,
            }],
            0,
            0,
        );
        layout_mgr.add_layout(layout).await;

        let fsmeta = Arc::new(FsMetaClient::new_in_process(Box::new(DummyFsMeta)));
        let buffer = Arc::new(DummyBuffer);
        let ndm = NamedDataMgr::with_layout_mgr(
            "test".to_string(),
            fsmeta,
            buffer,
            None,
            CommitPolicy::default(),
            layout_mgr.clone(),
        );

        let file_obj = FileObject::new("file".to_string(), 3, "sha256:00".to_string());
        let (file_obj_id, file_obj_str) = file_obj.gen_obj_id();
        layout_mgr
            .put_object(&file_obj_id, &file_obj_str)
            .await
            .unwrap();

        let obj_state = layout_mgr.query_object_by_id(&file_obj_id).await.unwrap();
        assert!(matches!(obj_state, ObjectState::Object(_)));

        ndm.erase_obj_by_id(&file_obj_id).await.unwrap();
        let obj_state = layout_mgr.query_object_by_id(&file_obj_id).await.unwrap();
        assert!(matches!(obj_state, ObjectState::NotExist));

        let data = b"hello chunk";
        let chunk_id = ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap();
        let (mut writer, _) = layout_mgr
            .open_chunk_writer(&chunk_id, data.len() as u64, 0)
            .await
            .unwrap();
        writer.write_all(data).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);
        layout_mgr.complete_chunk_writer(&chunk_id).await.unwrap();

        let (state, _, _) = layout_mgr.query_chunk_state(&chunk_id).await.unwrap();
        assert!(state.can_open_reader());

        ndm.erase_obj_by_id(&chunk_id.to_obj_id()).await.unwrap();
        let (state, _, _) = layout_mgr.query_chunk_state(&chunk_id).await.unwrap();
        assert!(matches!(state, named_store::ChunkStoreState::NotExist));

        let _ = tokio::fs::remove_dir_all(&store_dir).await;
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
