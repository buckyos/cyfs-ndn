//! Named Data Manager (NDM) - Core orchestration layer
//!
//! This module coordinates fs_meta, fs_buffer, and named_store to provide
//! a unified file/directory namespace with overlay semantics.
//!

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fs_buffer::{FileBufferSeekWriter, FileBufferService};
use log::warn;
use named_store::{
    NamedLocalConfig, NamedLocalStore, NamedStoreMgr, StoreLayout, StoreTarget as LayoutStoreTarget,
};
use ndn_lib::{
    load_named_obj, ChunkId, DirObject, FileObject, NdmPath, NdnError, NdnResult, ObjId,
    SimpleChunkList, SimpleMapItem, OBJ_TYPE_CHUNK_LIST_SIMPLE, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    DentryRecord, DentryTarget, FsMetaClient, FsMetaListEntry, IndexNodeId, MoveOptions, NodeKind,
    NodeRecord, NodeState, ObjStat,
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
    pub obj_inner_path: Option<String>,
    pub inode_id: Option<IndexNodeId>,
    pub state: Option<NodeState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathKind {
    File,
    Dir,
    Object,
    SymLink,
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

struct NamedListSession {
    fsmeta_list_session_id: Option<u64>,
    entries: Option<BTreeMap<String, PathStat>>,
    cursor: Option<String>,
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

pub struct CopyOptions {
    pub is_target_readonly: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            is_target_readonly: false,
        }
    }
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

    /// Optional store layout manager for multi-version store fallback
    /// When set, get_object operations will try multiple layout versions
    layout_mgr: Option<Arc<NamedStoreMgr>>,

    list_session_seq: AtomicU64,
    list_sessions: Arc<tokio::sync::Mutex<HashMap<u64, NamedListSession>>>,
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
            layout_mgr: None,
            list_session_seq: AtomicU64::new(1),
            list_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
            layout_mgr: Some(layout_mgr),
            list_session_seq: AtomicU64::new(1),
            list_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
                obj_inner_path: None,
                inode_id: Some(inode_id),
                state: Some(node.state),
            });
        }

        if path.is_root() {
            return Ok(PathStat {
                kind: PathKind::NotFound,
                size: None,
                obj_id: None,
                obj_inner_path: None,
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
                        DentryTarget::ObjId(obj_id) => {
                            return self.path_stat_from_obj_id(obj_id).await
                        }
                        DentryTarget::SymLink(target_path) => {
                            return Ok(Self::path_stat_from_symlink(target_path));
                        }
                        DentryTarget::IndexNodeId(id) => {
                            if let Some(node) =
                                self.fsmeta.get_inode(id, None).await.map_err(|e| {
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
                                    obj_inner_path: None,
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
            obj_inner_path: None,
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

    pub async fn set_file_with_body(&self, path: &NdmPath, obj_data: String) -> NdnResult<()> {
        //gen obj_id by obj_data
        //store_mgr.put
        //self.set_file
        unimplemented!()
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

    //创建软链接(symlink)，目标保存为路径（可相对路径）
    //SYMLINK: link_path -> target_path
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

    pub async fn copy_dir(
        &self,
        src: &NdmPath,
        target: &NdmPath,
        copy_option: CopyOptions,
    ) -> NdnResult<()> {
        let stat = self.stat(src).await?;
        if stat.kind != PathKind::Dir {
            return Err(NdnError::InvalidParam(
                "source is not a directory".to_string(),
            ));
        }
        if stat.obj_id.is_some() {
            //1) src已经物化，非常简单的set_dir就可以了
            self.set_dir(target, stat.obj_id.unwrap()).await
        } else {
            //2) src没有物化，此时要把src所有children的inode都clone一份到dest(处于working状态的inode跳过)
            unimplemented!();
        }
    }

    //快照的逻辑是copy_dir的特殊情况，复制后把target设置为readonly,并很快会触发物化流程冻结
    pub async fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        let cp_option = CopyOptions {
            is_target_readonly: false,
        };
        self.copy_dir(src, target, cp_option).await
    }

    // ========== Directory Operations ==========
    pub async fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        self.fsmeta.create_dir(path).await
    }

    //return list_session_id
    pub async fn start_list(&self, path: &NdmPath) -> NdnResult<u64> {
        if let Some((dir_id, dir_node)) = self.fsmeta.resolve_path(path).await? {
            if dir_node.kind != NodeKind::Dir {
                warn!(
                    "NamedDataMgr::start_list: path is not a directory, path={}, kind={:?}",
                    path.as_str(),
                    dir_node.kind
                );
                return Err(NdnError::InvalidParam(
                    "path is not a directory".to_string(),
                ));
            }

            let fsmeta_list_session_id =
                self.fsmeta.start_list(dir_id, None).await.map_err(|e| {
                    warn!(
                        "NamedDataMgr::start_list: start_list failed, path={}, dir_id={}, err={}",
                        path.as_str(),
                        dir_id,
                        e
                    );
                    NdnError::Internal(format!("start_list failed: {}", e))
                })?;

            let list_session_id = self.list_session_seq.fetch_add(1, Ordering::SeqCst);
            let mut local_entries: Option<BTreeMap<String, PathStat>> = None;
            if dir_node.base_obj_id.is_some() {
                let upper_entries = match self.fsmeta.list_next(fsmeta_list_session_id, 0).await {
                    Ok(entries) => entries,
                    Err(e) => {
                        warn!(
                            "NamedDataMgr::start_list: list_next failed, path={}, dir_id={}, err={}",
                            path.as_str(),
                            dir_id,
                            e
                        );
                        if let Err(stop_err) = self.fsmeta.stop_list(fsmeta_list_session_id).await {
                            warn!(
                                "NamedDataMgr::start_list: stop_list failed after list_next error, session_id={}, err={}",
                                fsmeta_list_session_id,
                                stop_err
                            );
                        }
                        return Err(NdnError::Internal(format!("list_next failed: {}", e)));
                    }
                };

                let merged_entries = match self
                    .build_merged_dir_entries(&dir_node, upper_entries)
                    .await
                {
                    Ok(entries) => entries,
                    Err(e) => {
                        let _ = self.fsmeta.stop_list(fsmeta_list_session_id).await;
                        return Err(e);
                    }
                };
                local_entries = Some(merged_entries);
            }

            let mut sessions = self.list_sessions.lock().await;
            sessions.insert(
                list_session_id,
                NamedListSession {
                    fsmeta_list_session_id: Some(fsmeta_list_session_id),
                    entries: local_entries,
                    cursor: None,
                },
            );
            return Ok(list_session_id);
        }

        if let Some(entries) = self.load_obj_dir_entries(path).await? {
            let list_session_id = self.list_session_seq.fetch_add(1, Ordering::SeqCst);
            let mut sessions = self.list_sessions.lock().await;
            sessions.insert(
                list_session_id,
                NamedListSession {
                    fsmeta_list_session_id: None,
                    entries: Some(entries),
                    cursor: None,
                },
            );
            return Ok(list_session_id);
        }

        Err(NdnError::NotFound("path not found".to_string()))
    }

    pub async fn stop_list(&self, list_session_id: u64) -> NdnResult<()> {
        let session = {
            let mut sessions = self.list_sessions.lock().await;
            sessions.remove(&list_session_id)
        };
        let session = session.ok_or_else(|| {
            NdnError::NotFound(format!("list session {} not found", list_session_id))
        })?;

        if let Some(fsmeta_list_session_id) = session.fsmeta_list_session_id {
            self.fsmeta
                .stop_list(fsmeta_list_session_id)
                .await
                .map_err(|e| NdnError::Internal(format!("stop_list failed: {}", e)))?;
        }

        Ok(())
    }

    pub async fn list_next(
        &self,
        list_session_id: u64,
        page_size: u32,
    ) -> NdnResult<Vec<(String, PathStat)>> {
        let passthrough_fsmeta_session = {
            let mut sessions = self.list_sessions.lock().await;
            let session = match sessions.get_mut(&list_session_id) {
                Some(session) => session,
                None => {
                    warn!(
                        "NamedDataMgr::list_next: list session not found, list_session_id={}",
                        list_session_id
                    );
                    return Err(NdnError::NotFound(format!(
                        "list session {} not found",
                        list_session_id
                    )));
                }
            };

            if let Some(entries) = session.entries.as_ref() {
                let out = Self::page_from_ordered_map(entries, &mut session.cursor, page_size);
                return Ok(out);
            }

            session.fsmeta_list_session_id.ok_or_else(|| {
                warn!(
                    "NamedDataMgr::list_next: missing fsmeta session, list_session_id={}",
                    list_session_id
                );
                NdnError::InvalidState(format!(
                    "list session {} missing fsmeta session",
                    list_session_id
                ))
            })?
        };

        let fsmeta_page = self
            .fsmeta
            .list_next(passthrough_fsmeta_session, page_size)
            .await
            .map_err(|e| NdnError::Internal(format!("list_next failed: {}", e)))?;
        self.path_stats_from_fsmeta_entries(fsmeta_page).await
    }

    async fn build_merged_dir_entries(
        &self,
        dir_node: &NodeRecord,
        upper_entries: BTreeMap<String, FsMetaListEntry>,
    ) -> NdnResult<BTreeMap<String, PathStat>> {
        let mut merged: BTreeMap<String, PathStat> = BTreeMap::new();
        if let Some(base_obj_id) = dir_node.base_obj_id.clone() {
            let dir_obj = self.load_dir_object(&base_obj_id).await.map_err(|e| {
                warn!(
                    "NamedDataMgr::build_merged_dir_entries: load base dir object failed, base_obj_id={}, err={}",
                    base_obj_id.to_string(),
                    e
                );
                e
            })?;
            for (name, item) in dir_obj.iter() {
                if let Some(upper) = upper_entries.get(name) {
                    if matches!(upper.target, DentryTarget::Tombstone) {
                        continue;
                    }
                    continue;
                }
                let child_stat = self.path_stat_from_simple_map_item(item).await?;
                merged.insert(name.clone(), child_stat);
            }
        }

        for (name, entry) in upper_entries {
            let FsMetaListEntry {
                name: _,
                target,
                inode,
            } = entry;
            if matches!(target, DentryTarget::Tombstone) {
                continue;
            }

            let child_stat = match target {
                DentryTarget::IndexNodeId(id) => {
                    let node = match inode {
                        Some(node) => Some(node),
                        None => self.fsmeta.get_inode(id, None).await.map_err(|e| {
                            NdnError::Internal(format!("failed to get inode: {}", e))
                        })?,
                    };
                    let node = match node {
                        Some(node) => node,
                        None => continue,
                    };
                    self.path_stat_from_inode_node(id, node).await
                }
                DentryTarget::SymLink(target_path) => Self::path_stat_from_symlink(target_path),
                DentryTarget::ObjId(obj_id) => self.path_stat_from_obj_id(obj_id).await?,
                DentryTarget::Tombstone => continue,
            };

            merged.insert(name, child_stat);
        }

        Ok(merged)
    }

    async fn load_obj_dir_entries(
        &self,
        path: &NdmPath,
    ) -> NdnResult<Option<BTreeMap<String, PathStat>>> {
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
                            let mut out = BTreeMap::new();
                            for (entry_name, item) in dir_obj.iter() {
                                let child_stat = self.path_stat_from_simple_map_item(item).await?;
                                out.insert(entry_name.clone(), child_stat);
                            }
                            return Ok(Some(out));
                        }
                        DentryTarget::IndexNodeId(_)
                        | DentryTarget::SymLink(_)
                        | DentryTarget::Tombstone => {}
                    }
                }
            }
        }

        Ok(None)
    }

    fn page_from_ordered_map<T: Clone>(
        entries: &BTreeMap<String, T>,
        cursor: &mut Option<String>,
        page_size: u32,
    ) -> Vec<(String, T)> {
        let start_bound = match cursor.as_ref() {
            Some(c) => Bound::Excluded(c.clone()),
            None => Bound::Unbounded,
        };
        let limit = if page_size == 0 {
            usize::MAX
        } else {
            page_size as usize
        };

        let mut out: Vec<(String, T)> = Vec::new();
        for (name, value) in entries.range((start_bound, Bound::Unbounded)).take(limit) {
            out.push((name.clone(), value.clone()));
        }
        if let Some((last_name, _)) = out.last() {
            *cursor = Some(last_name.clone());
        }
        out
    }

    async fn path_stats_from_fsmeta_entries(
        &self,
        entries: BTreeMap<String, FsMetaListEntry>,
    ) -> NdnResult<Vec<(String, PathStat)>> {
        let mut out = Vec::with_capacity(entries.len());
        for (name, entry) in entries {
            let child_stat = match entry.target {
                DentryTarget::IndexNodeId(id) => {
                    let node = match entry.inode {
                        Some(node) => Some(node),
                        None => self.fsmeta.get_inode(id, None).await.map_err(|e| {
                            NdnError::Internal(format!("failed to get inode: {}", e))
                        })?,
                    };
                    let node = match node {
                        Some(node) => node,
                        None => continue,
                    };
                    self.path_stat_from_inode_node(id, node).await
                }
                DentryTarget::SymLink(target_path) => Self::path_stat_from_symlink(target_path),
                DentryTarget::ObjId(obj_id) => self.path_stat_from_obj_id(obj_id).await?,
                DentryTarget::Tombstone => continue,
            };
            out.push((name, child_stat));
        }
        Ok(out)
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
                //TODO 手工构造政府的fs buffer reader，要传入正确的chunklist base reader,或则提供成标准的DiffChunkListReader实现
                let reader = self
                    .fsbuffer
                    .open_reader(&fb, std::io::SeekFrom::Start(0))
                    .await?;
                Ok((Box::new(reader), 0))
            }
            crate::OpenFileReaderResp::Object(obj_id, inner_path) => {
                let layout_mgr = self.layout_mgr.as_ref().ok_or_else(|| {
                    warn!(
                        "NamedDataMgr::open_reader: store layout manager not configured, obj_id={}",
                        obj_id.to_string()
                    );
                    NdnError::NotFound("store layout manager not configured".to_string())
                })?;
                let (reader, size) = layout_mgr.open_reader(&obj_id, inner_path).await?;
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

        //get_object的返回，需要正确支持jwt格式
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
    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
        // If layout manager is set, use multi-version fallback
        if let Some(layout_mgr) = &self.layout_mgr {
            return layout_mgr.get_object(obj_id).await;
        }

        warn!(
            "NamedDataMgr::get_object: store layout manager not configured, obj_id={}",
            obj_id.to_string()
        );
        Err(NdnError::NotFound(
            "store layout manager not configured".to_string(),
        ))
    }

    async fn load_dir_object(&self, obj_id: &ObjId) -> NdnResult<DirObject> {
        if !obj_id.is_dir_object() {
            return Err(NdnError::InvalidObjType("must be dirobject".to_string()));
        }
        let obj_str = self.get_object(obj_id).await?;
        load_named_obj(obj_str.as_str())
    }

    async fn load_file_object(&self, obj_id: &ObjId) -> NdnResult<FileObject> {
        if !obj_id.is_file_object() {
            return Err(NdnError::InvalidObjType("must be fileobject".to_string()));
        }
        let obj_str = self.get_object(obj_id).await?;
        load_named_obj(obj_str.as_str())
    }

    async fn load_chunk_list(&self, obj_id: &ObjId) -> NdnResult<SimpleChunkList> {
        let obj_str = self.get_object(obj_id).await?;
        let chunk_list: Vec<ChunkId> = load_named_obj(obj_str.as_str())?;
        SimpleChunkList::from_chunk_list(chunk_list)
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
            ObjectKind::Dir => self
                .load_dir_object(obj_id)
                .await
                .ok()
                .map(|d| d.total_size),
            ObjectKind::Unknown => None,
        }
    }

    async fn path_stat_from_inode_node(&self, inode_id: IndexNodeId, node: NodeRecord) -> PathStat {
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

        PathStat {
            kind,
            size,
            obj_id,
            obj_inner_path: None,
            inode_id: Some(inode_id),
            state: Some(node.state),
        }
    }

    fn path_stat_from_symlink(target_path: String) -> PathStat {
        PathStat {
            kind: PathKind::SymLink,
            size: None,
            obj_id: None,
            obj_inner_path: Some(target_path),
            inode_id: None,
            state: None,
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
            obj_inner_path: None,
            inode_id: None,
            state: None,
        })
    }

    async fn path_stat_from_simple_map_item(&self, item: &SimpleMapItem) -> NdnResult<PathStat> {
        let (obj_id, _) = item.get_obj_id()?;
        let kind = match Self::obj_kind_from_obj_id(&obj_id) {
            ObjectKind::Dir => PathKind::Dir,
            ObjectKind::File => PathKind::File,
            ObjectKind::Unknown => PathKind::File,
        };

        let size = match item {
            SimpleMapItem::Object(obj_type, _) => {
                if obj_type == OBJ_TYPE_FILE {
                    let file_obj: FileObject =
                        serde_json::from_value(item.get_obj()?).map_err(|e| {
                            NdnError::Internal(format!("failed to parse FileObject: {}", e))
                        })?;
                    Some(file_obj.size)
                } else if obj_type == OBJ_TYPE_DIR {
                    let dir_obj: DirObject =
                        serde_json::from_value(item.get_obj()?).map_err(|e| {
                            NdnError::Internal(format!("failed to parse DirObject: {}", e))
                        })?;
                    Some(dir_obj.total_size)
                } else {
                    None
                }
            }
            SimpleMapItem::ObjectJwt(obj_type, _) => {
                if obj_type == OBJ_TYPE_FILE {
                    let file_obj: FileObject =
                        serde_json::from_value(item.get_obj()?).map_err(|e| {
                            NdnError::Internal(format!("failed to parse FileObject: {}", e))
                        })?;
                    Some(file_obj.size)
                } else if obj_type == OBJ_TYPE_DIR {
                    let dir_obj: DirObject =
                        serde_json::from_value(item.get_obj()?).map_err(|e| {
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
            obj_inner_path: None,
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

        layout_mgr.remove_object(obj_id).await?;
        Ok(())
    }

    // ========== Chunk Operations (for ndn_router , call to store_layout_mgr) ==========

    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool> {
        let layout_mgr = self
            .layout_mgr
            .as_ref()
            .ok_or_else(|| NdnError::NotFound("store layout manager not configured".to_string()))?;
        let have_chunk = layout_mgr.have_chunk(chunk_id).await;
        Ok(have_chunk)
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
        layout_mgr.open_chunk_reader(chunk_id, offset).await
    }

    // ========== Admin Operations ==========

    pub async fn expand_store(&self, _new_target: StoreTarget) -> NdnResult<u64> {
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
        let mut targets = current
            .as_ref()
            .map(|l| l.targets.clone())
            .unwrap_or_default();

        if !targets.iter().any(|t| t.store_id == _new_target.store_id) {
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
                warn!(
                    "NamedDataMgr::publish_dir: inode not found, path={}, dir_id={}",
                    path.as_str(),
                    dir_id
                );
                let _ = self.fsmeta.rollback(Some(txid.clone())).await;
                return Err(NdnError::NotFound("inode not found".to_string()));
            }
        };

        if node.kind != NodeKind::Dir {
            warn!(
                "NamedDataMgr::publish_dir: path is not a directory, path={}, kind={:?}",
                path.as_str(),
                node.kind
            );
            let _ = self.fsmeta.rollback(Some(txid.clone())).await;
            return Err(NdnError::InvalidParam(
                "path is not a directory".to_string(),
            ));
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
                            warn!(
                                "NamedDataMgr::publish_dir: child inode not found, name={}, inode_id={}",
                                dentry.name,
                                inode_id
                            );
                            let _ = self.fsmeta.rollback(Some(txid.clone())).await;
                            return Err(NdnError::NotFound("child inode not found".to_string()));
                        }
                    };
                    Self::node_obj_id(&child).ok_or_else(|| {
                        NdnError::InvalidState(format!("child {} not published", dentry.name))
                    })?
                }
                DentryTarget::SymLink(_) => continue,
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
