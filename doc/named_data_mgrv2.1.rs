// named_data_mgr.rs
//
// Architecture v2 (pseudocode-ish):
// - Overlay directories (Upper = fsmeta dentries, Base = DirObject children).
// - Inode/Dentry in fsmeta with Strategy B: dentry target can be IndexNodeId OR ObjId OR Tombstone.
// - list() has no pos/page_size; merge happens in NDM.
// - File write path uses staged commit with GFS-like FileBuffer nodes:
//   Writing -> Cooling -> Linked (ExternalLink in store) -> Finalized (internal chunk store).
//
// This file is intentionally "executable-ish pseudocode":
// - Interfaces are realistic Rust traits.
// - Implementations are filled with structured comments + todo!/unimplemented!().
//
// ------------------------------------------------------------

#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::{Read, Seek};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// ------------------------------
/// Common Types
/// ------------------------------

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ObjId(pub String); // e.g. sha256:...

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ChunkId(pub String); // e.g. sha256:...

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IndexNodeId(pub u64); // inode number (fsmeta internal)

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseFence(pub u64);

#[derive(Clone, Debug)]
pub struct NdmPath(pub String); // e.g. ndm://pool/ndm_id/...

impl NdmPath {
    pub fn components(&self) -> Vec<String> {
        // PSEUDOCODE:
        // - Parse ndm://pool/ndm_id/<path>
        // - Return path components excluding scheme/pool/ndm_id
        // NOTE: Keep name normalization rules consistent across the system.
        todo!("parse + normalize path")
    }

    pub fn join(&self, child: &str) -> NdmPath {
        let mut s = self.0.clone();
        if !s.ends_with('/') {
            s.push('/');
        }
        s.push_str(child);
        NdmPath(s)
    }
}

pub type NdnResult<T> = Result<T, NdmError>;

/// ------------------------------
/// Errors
/// ------------------------------

#[derive(Debug, Clone)]
pub enum NdmError {
    NotFound,
    AlreadyExists,
    InvalidName,
    NeedPull,              // object referenced but not materialized locally
    ReadOnly,              // operation forbidden under read_only mount
    PathBusy,              // strict single writer conflict
    LeaseConflict,         // lease fencing mismatch
    LinkInvalidOrChanged,  // ExternalLink changed (qcid mismatch)
    Conflict,              // optimistic dir rev mismatch or name conflict
    Io(String),
    Internal(String),
}

impl fmt::Display for NdmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// ------------------------------
/// Reader Abstraction
/// ------------------------------

/// A reader that supports Read + Seek and is Send.
/// Real implementation may provide async streams or range readers.
pub trait NdmReader: Read + Seek + Send {}
impl<T: Read + Seek + Send> NdmReader for T {}

/// ------------------------------
/// Object Model (minimal placeholders)
/// ------------------------------

#[derive(Clone, Debug)]
pub enum ObjectKind {
    File,
    Dir,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct NamedObjectMeta {
    pub obj_id: ObjId,
    pub kind: ObjectKind,
    pub size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct FileObject {
    pub obj_id: ObjId,
    pub chunks: Vec<ChunkId>, // chunklist
    pub size: u64,
    // ... other fields: content field name, ver, encoding, etc.
}

#[derive(Clone, Debug)]
pub struct DirObject {
    pub obj_id: ObjId,
    /// Children are (name -> child pointer). Pointer can be file or dir object id.
    /// Assumed to be sorted by name in canonical encoding.
    pub children: Vec<DirChild>,
}

#[derive(Clone, Debug)]
pub struct DirChild {
    pub name: String,
    pub kind: ObjectKind,
    pub obj_id: ObjId,
}

/// ------------------------------
/// Mount Mode (dir-level)
/// ------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountMode {
    Overlay,
    ReadOnly,
}

/// ------------------------------
/// FsMeta: Inode/Dentry Model (Strategy B)
/// ------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Dir,
}

#[derive(Clone, Debug)]
pub enum NodeState {
    /// Directory node (usually committed; delta lives in dentries)
    DirCommitted,
    /// File node: currently writable, bound to FileBuffer
    Writing(FileWritingState),
    /// File node: closed, waiting to stabilize (debounce)
    Cooling(FileCoolingState),
    /// File node: hashed & published via ExternalLink (content address stable)
    Linked(FileLinkedState),
    /// File node: data promoted into internal store (chunks finalized)
    Finalized(FileFinalizedState),
    /// File/Dir: committed base points to an immutable object id
    Committed { base_obj_id: ObjId },
}

#[derive(Clone, Debug)]
pub struct FileWritingState {
    pub session: SessionId,
    pub fence: LeaseFence,
    pub buffer_node: String,
    pub remote_path: String,
    pub last_write_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct FileCoolingState {
    pub buffer_node: String,
    pub remote_path: String,
    pub closed_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct FileLinkedState {
    pub obj_id: ObjId, // published FileObjectId (stable)
    pub buffer_node: String,
    pub remote_path: String,
    pub linked_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct FileFinalizedState {
    pub obj_id: ObjId,
    pub finalized_at: SystemTime,
    // Optionally: which store targets now host the data
}

#[derive(Clone, Debug)]
pub struct FileFinalizedState2 {
    pub obj_id: ObjId,
    pub finalized_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct NodeRecord {
    pub file_id: IndexNodeId,
    pub kind: NodeKind,
    pub mount_mode: Option<MountMode>, // only for dirs
    pub base_obj_id: Option<ObjId>,    // committed base snapshot (file or dir)
    pub state: NodeState,
    pub rev: Option<u64>,              // only for dirs
    // leases:
    pub lease_session: Option<SessionId>,
    pub lease_fence: Option<LeaseFence>,
    pub lease_expire_at: Option<SystemTime>,
}

/// Dentry target (Strategy B)
#[derive(Clone, Debug)]
pub enum DentryTarget {
    IndexNodeId(IndexNodeId),
    ObjId(ObjId),
    Tombstone,
}

#[derive(Clone, Debug)]
pub struct DentryRecord {
    pub parent: IndexNodeId,
    pub name: String,
    pub target: DentryTarget,
    pub mtime: Option<SystemTime>,
}

/// ------------------------------
/// FsMeta Service Traits
/// ------------------------------

/// Transaction object for fsmeta updates.
/// In a real implementation, this is backed by SQLite txn or Raft log txn.
pub trait FsMetaTxn: Send {
    fn get_node(&mut self, id: &IndexNodeId) -> NdnResult<Option<NodeRecord>>;
    fn put_node(&mut self, node: &NodeRecord) -> NdnResult<()>;

    fn get_dentry(&mut self, parent: &IndexNodeId, name: &str) -> NdnResult<Option<DentryRecord>>;
    fn list_dentries(&mut self, parent: &IndexNodeId) -> NdnResult<Vec<DentryRecord>>;

    fn upsert_dentry(&mut self, parent: &IndexNodeId, name: &str, target: DentryTarget) -> NdnResult<()>;
    fn remove_dentry_row(&mut self, parent: &IndexNodeId, name: &str) -> NdnResult<()>;

    /// Set a tombstone (whiteout). Must not "DELETE" in overlay mode.
    fn set_tombstone(&mut self, parent: &IndexNodeId, name: &str) -> NdnResult<()> {
        self.upsert_dentry(parent, name, DentryTarget::Tombstone)
    }

    /// Atomically bump directory rev (OCC).
    fn bump_dir_rev(&mut self, dir: &IndexNodeId, expected_rev: u64) -> NdnResult<u64>;

    fn commit(self: Box<Self>) -> NdnResult<()>;
    fn rollback(self: Box<Self>) -> NdnResult<()>;
}

/// FsMeta API (strong consistency domain)
pub trait FsMetaService: Send + Sync {
    fn begin_txn(&self) -> NdnResult<Box<dyn FsMetaTxn>>;

    fn root_dir(&self) -> NdnResult<IndexNodeId>;

    fn alloc_inode(&self, kind: NodeKind, mount_mode: Option<MountMode>) -> NdnResult<IndexNodeId>;

    /// Acquire strict single-writer lease for file inode.
    /// - Must return a fencing token (monotonic per file_id).
    /// - Subsequent writes must provide the same (session, fence).
    fn acquire_file_lease(
        &self,
        file_id: &IndexNodeId,
        session: &SessionId,
        ttl: Duration,
    ) -> NdnResult<LeaseFence>;

    fn renew_file_lease(
        &self,
        file_id: &IndexNodeId,
        session: &SessionId,
        fence: LeaseFence,
        ttl: Duration,
    ) -> NdnResult<()>;

    fn release_file_lease(&self, file_id: &IndexNodeId, session: &SessionId, fence: LeaseFence) -> NdnResult<()>;

    /// Read node record (non-transactional).
    fn get_node(&self, id: &IndexNodeId) -> NdnResult<Option<NodeRecord>>;

    /// Read dentry record (Upper layer only).
    fn get_dentry(&self, parent: &IndexNodeId, name: &str) -> NdnResult<Option<DentryRecord>>;

    /// List all upper-layer dentries for merge (usually sparse).
    fn list_dentries(&self, parent: &IndexNodeId) -> NdnResult<Vec<DentryRecord>>;

    /// For background lifecycle: scan file nodes in Cooling/Linked states.
    fn scan_files_by_state(&self, state: ScanFileState, limit: usize) -> NdnResult<Vec<NodeRecord>>;

    /// Update file node state (atomic check via lease fencing where needed).
    fn update_file_state(&self, file_id: &IndexNodeId, new_state: NodeState) -> NdnResult<()>;
}

#[derive(Clone, Copy, Debug)]
pub enum ScanFileState {
    Cooling,
    Linked,
}

/// ------------------------------
/// Buffer (GFS-like FileBuffer Nodes)
/// ------------------------------

#[derive(Clone, Debug)]
pub struct FileBufferHandle {
    pub file_id: IndexNodeId,
    pub session: SessionId,
    pub fence: LeaseFence,
    pub buffer_node: String,
    pub remote_path: String,
}

/// Chunk link information returned by BufferNode when hashing/sealing a file.
#[derive(Clone, Debug)]
pub struct ExternalChunkLink {
    pub chunk_id: ChunkId,
    pub url: String,              // e.g. gfs://node_id/path or file://...
    pub range: Option<(u64, u64)>, // (offset, len)
    pub qcid: Option<String>,      // quick change id for ExternalFile
}

/// Result of objectification on a BufferNode (no data transfer).
#[derive(Clone, Debug)]
pub struct BufferObjectification {
    pub file_obj_id: ObjId,
    pub file_size: u64,
    pub chunks: Vec<ExternalChunkLink>,
}

/// Client to a buffer node.
pub trait BufferNodeClient: Send + Sync {
    fn append(&self, remote_path: &str, data: &[u8]) -> NdnResult<()>;
    fn flush(&self, remote_path: &str) -> NdnResult<()>;

    /// Seal the buffer file so it becomes immutable for hashing/reading.
    fn seal(&self, remote_path: &str) -> NdnResult<()>;

    /// Compute chunklist + file object id; return chunk external links (no data copy).
    fn objectify(&self, remote_path: &str) -> NdnResult<BufferObjectification>;

    /// Copy data to named_store internal targets (expensive IO).
    fn push_to_store(&self, remote_path: &str, store: &dyn NamedStore) -> NdnResult<()>;

    fn delete_buffer_file(&self, remote_path: &str) -> NdnResult<()>;
}

/// Master that assigns buffer nodes (GFS master-like).
pub trait BufferMaster: Send + Sync {
    fn pick_node_for_write(&self, expect_size: Option<u64>) -> NdnResult<Arc<dyn BufferNodeClient>>;
    fn get_node(&self, node_id: &str) -> NdnResult<Arc<dyn BufferNodeClient>>;

    /// Allocate remote path on a node; returns (node_id, remote_path).
    fn create_remote_file(&self, node: &dyn BufferNodeClient, hint: &str) -> NdnResult<(String, String)>;
}

/// ------------------------------
/// NamedStore (CAS object store + chunk store + links)
/// ------------------------------

#[derive(Clone, Debug)]
pub enum LinkType {
    SameAs(ChunkId),
    PartOf { base: ChunkId, range: (u64, u64) },
    ExternalFile { url: String, range: Option<(u64, u64)>, qcid: Option<String> },
}

pub trait NamedStore: Send + Sync {
    /// Get object meta (kind, size, etc). Might require object header only.
    fn get_object_meta(&self, obj_id: &ObjId) -> NdnResult<Option<NamedObjectMeta>>;

    fn get_dir_object(&self, obj_id: &ObjId) -> NdnResult<DirObject>;
    fn get_file_object(&self, obj_id: &ObjId) -> NdnResult<FileObject>;

    /// Open reader by object id (object + inner_path resolution is out of scope here).
    fn open_reader_by_id(&self, obj_id: &ObjId) -> NdnResult<Box<dyn NdmReader>>;

    /// Ensure object body/chunks are present locally, or schedule pull.
    fn has_materialized(&self, obj_id: &ObjId) -> NdnResult<bool>;
    fn schedule_pull(&self, obj_id: &ObjId, remote: PullContext) -> NdnResult<()>;

    /// Put a (small) object metadata into object DB. For FileObject/DirObject, obj_id must match content hash.
    fn put_object_meta(&self, meta: &NamedObjectMeta) -> NdnResult<()>;
    fn put_dir_object(&self, dir: &DirObject) -> NdnResult<()>;
    fn put_file_object(&self, file: &FileObject) -> NdnResult<()>;

    /// Register link mapping for a chunk (ExternalFile/SameAs/PartOf).
    fn upsert_chunk_link(&self, chunk_id: &ChunkId, link: LinkType) -> NdnResult<()>;

    /// Promote chunk(s) from external links to internal storage: remove link or mark COMPLETE.
    fn promote_obj_to_internal(&self, obj_id: &ObjId) -> NdnResult<()>;

    /// Physical erase (evict local materialized body/chunks). Keep meta/index as needed.
    fn erase_obj_data(&self, obj_id: &ObjId) -> NdnResult<()>;
}

/// ------------------------------
/// Pull Context (placeholder)
/// ------------------------------
#[derive(Clone, Debug)]
pub struct PullContext {
    pub remote: String, // e.g. ndn://..., http(s)://..., peer-id, etc.
    // auth, timeout, policy, ...
}

/// ------------------------------
/// NDM Public API Types
/// ------------------------------

#[derive(Clone, Debug)]
pub struct PathStat {
    pub path: NdmPath,
    pub kind: ObjectKind,
    pub resolved: ResolvedRef,
    pub materialized: bool,
    pub state_hint: Option<NodeState>, // if resolved via inode
}

#[derive(Clone, Debug)]
pub enum ResolvedRef {
    Inode(IndexNodeId),
    ObjId(ObjId),
}

#[derive(Clone, Debug)]
pub struct DirEntryView {
    pub name: String,
    pub kind: ObjectKind,
    pub target: ResolvedRef,
}

/// ------------------------------
/// NamedDataMgr Config
/// ------------------------------

#[derive(Clone, Debug)]
pub struct NdmConfig {
    pub file_lease_ttl: Duration,
    pub cooling_grace: Duration,   // debounce window after close
    pub frozen_grace: Duration,    // time in Linked before promote to internal
    pub background_scan_limit: usize,
}

impl Default for NdmConfig {
    fn default() -> Self {
        Self {
            file_lease_ttl: Duration::from_secs(60),
            cooling_grace: Duration::from_secs(10),
            frozen_grace: Duration::from_secs(3600),
            background_scan_limit: 64,
        }
    }
}

/// ------------------------------
/// NamedDataMgr
/// ------------------------------

pub struct NamedDataMgr {
    pub pool_id: String,
    pub ndm_id: String,

    fsmeta: Arc<dyn FsMetaService>,
    store: Arc<dyn NamedStore>,
    buffer_master: Arc<dyn BufferMaster>,

    cfg: NdmConfig,

    // Very light background control. A real implementation would use a task runtime.
    bg_running: Arc<Mutex<bool>>,
}

impl NamedDataMgr {
    pub fn new(
        pool_id: String,
        ndm_id: String,
        fsmeta: Arc<dyn FsMetaService>,
        store: Arc<dyn NamedStore>,
        buffer_master: Arc<dyn BufferMaster>,
        cfg: NdmConfig,
    ) -> Self {
        Self {
            pool_id,
            ndm_id,
            fsmeta,
            store,
            buffer_master,
            cfg,
            bg_running: Arc::new(Mutex::new(false)),
        }
    }

    /// Start background lifecycle worker (Cooling->Linked->Finalized).
    /// PSEUDOCODE: spawn a thread / async task loop.
    pub fn start_background_workers(&self) {
        // In production:
        // - use a task executor
        // - ensure only one worker per NDM instance
        let mut running = self.bg_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        // PSEUDOCODE: spawn thread
        // std::thread::spawn(move || loop { self.bg_tick(); sleep(1s); });
        todo!("spawn background worker(s)")
    }

    /// One tick of background lifecycle processing.
    pub fn bg_tick(&self) -> NdnResult<()> {
        // 1) process Cooling -> Linked
        let cooling = self.fsmeta.scan_files_by_state(ScanFileState::Cooling, self.cfg.background_scan_limit)?;
        for node in cooling {
            self.try_cooling_to_linked(&node)?;
        }

        // 2) process Linked -> Finalized
        let linked = self.fsmeta.scan_files_by_state(ScanFileState::Linked, self.cfg.background_scan_limit)?;
        for node in linked {
            self.try_linked_to_finalized(&node)?;
        }

        Ok(())
    }

    /// ------------------------------
    /// Path Resolution (Overlay + Strategy B)
    /// ------------------------------

    /// Resolve a path to a Ref (inode or obj).
    /// - Traversal uses Upper dentries first.
    /// - On miss, consult Base DirObject (NDM reads store).
    pub fn resolve_path(&self, path: &NdmPath) -> NdnResult<ResolvedRef> {
        let root = self.fsmeta.root_dir()?;
        let mut cur_dir = root;

        let comps = path.components();
        if comps.is_empty() {
            return Ok(ResolvedRef::Inode(root));
        }

        for (i, name) in comps.iter().enumerate() {
            let is_last = i + 1 == comps.len();

            let child = self.lookup_one_in_dir(cur_dir.clone(), name)?;
            match child {
                LookupOne::Tombstone => return Err(NdmError::NotFound),
                LookupOne::UpperIndexNodeId(fid) => {
                    if is_last {
                        return Ok(ResolvedRef::Inode(fid));
                    }
                    // Must be dir to continue
                    let node = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                    if node.kind != NodeKind::Dir {
                        return Err(NdmError::NotFound);
                    }
                    cur_dir = fid;
                }
                LookupOne::UpperObjId(obj_id) | LookupOne::BaseObjId(obj_id) => {
                    if is_last {
                        return Ok(ResolvedRef::ObjId(obj_id));
                    }
                    // Need to step into a directory object. We do not have an inode.
                    // For read traversal, we can keep stepping in DirObject directly.
                    // For write operations, the caller should "ensure_dir_inode" (materialize).
                    let meta = self.store.get_object_meta(&obj_id)?.ok_or(NdmError::NotFound)?;
                    if meta.kind != ObjectKind::Dir {
                        return Err(NdmError::NotFound);
                    }
                    // Enter base dir object: represent as "virtual dir" with base obj id.
                    // We keep a special sentinel: cur_dir remains the same inode (no),
                    // so we need a different traversal method. For simplicity, we fail here
                    // and require directories to be inodes.
                    //
                    // In v2 design, directories are typically materialized as inodes because
                    // they need mount_mode/rev/dentries.
                    return Err(NdmError::Internal(
                        "cannot traverse into ObjId dir without inode; materialize dir inode first".into(),
                    ));
                }
            }
        }

        Err(NdmError::NotFound)
    }

    /// Lookup a single name under a directory inode, applying overlay rules:
    /// - If upper dentry exists: IndexNodeId/ObjId/Tombstone
    /// - Else consult base DirObject (if any)
    fn lookup_one_in_dir(&self, parent_dir: IndexNodeId, name: &str) -> NdnResult<LookupOne> {
        if !is_valid_name(name) {
            return Err(NdmError::InvalidName);
        }

        if let Some(d) = self.fsmeta.get_dentry(&parent_dir, name)? {
            return Ok(match d.target {
                DentryTarget::IndexNodeId(fid) => LookupOne::UpperIndexNodeId(fid),
                DentryTarget::ObjId(oid) => LookupOne::UpperObjId(oid),
                DentryTarget::Tombstone => LookupOne::Tombstone,
            });
        }

        // Upper miss -> consult base
        let dir_node = self.fsmeta.get_node(&parent_dir)?.ok_or(NdmError::NotFound)?;
        let base = match &dir_node.base_obj_id {
            None => return Ok(LookupOne::NotFound),
            Some(oid) => oid.clone(),
        };

        // If base not materialized: choose either NEED_PULL or empty; we choose NEED_PULL to avoid surprise.
        if !self.store.has_materialized(&base)? {
            return Err(NdmError::NeedPull);
        }

        // Load DirObject and search child (streaming is preferred; simplified here).
        let dir_obj = self.store.get_dir_object(&base)?;
        for child in dir_obj.children.iter() {
            if child.name == name {
                return Ok(LookupOne::BaseObjId(child.obj_id.clone()));
            }
        }

        Ok(LookupOne::NotFound)
    }

    /// Ensure a directory is represented as an inode (required for any mutable operation under it).
    /// If the path resolves to an ObjId(DirObject), materialize:
    /// - alloc inode kind=dir, set base_obj_id=that DirObject, mount_mode=Overlay
    /// - update parent dentry to point to file_id
    pub fn ensure_dir_inode(&self, path: &NdmPath) -> NdnResult<IndexNodeId> {
        // PSEUDOCODE:
        // 1) Resolve parent + name (need parent dir inode)
        // 2) lookup_one_in_dir(parent, name)
        // 3) if already file_id -> return
        // 4) if obj_id -> verify it's a dir object
        // 5) txn: alloc inode, set node.base_obj_id, mount_mode, insert dentry -> file_id
        todo!("materialize dir inode on demand")
    }

    /// ------------------------------
    /// Read APIs
    /// ------------------------------

    pub fn open_reader_by_id(&self, obj_id: &ObjId) -> NdnResult<Box<dyn NdmReader>> {
        // Read committed object by ObjId.
        // If not materialized: return NEED_PULL (or schedule pull depending on policy).
        if !self.store.has_materialized(obj_id)? {
            return Err(NdmError::NeedPull);
        }
        self.store.open_reader_by_id(obj_id)
    }

    pub fn open_reader(&self, path: &NdmPath) -> NdnResult<Box<dyn NdmReader>> {
        // Resolve to inode or objid
        match self.resolve_path(path)? {
            ResolvedRef::ObjId(oid) => self.open_reader_by_id(&oid),
            ResolvedRef::Inode(fid) => {
                let node = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                match node.state.clone() {
                    NodeState::Writing(_) => {
                        // Non-POSIX read semantics: default deny reading working buffer (except same session optional).
                        Err(NdmError::PathBusy)
                    }
                    NodeState::Cooling(_) => {
                        // Cooling: treat as busy or read-last-committed.
                        // We choose: read last committed base if present, else PATH_BUSY.
                        if let Some(oid) = node.base_obj_id {
                            self.open_reader_by_id(&oid)
                        } else {
                            Err(NdmError::PathBusy)
                        }
                    }
                    NodeState::Linked(s) => self.open_reader_by_id(&s.obj_id),
                    NodeState::Finalized(s) => self.open_reader_by_id(&s.obj_id),
                    NodeState::Committed { base_obj_id } => self.open_reader_by_id(&base_obj_id),
                    NodeState::DirCommitted => Err(NdmError::Internal("open_reader on dir inode".into())),
                }
            }
        }
    }

    pub fn get_object_meta(&self, obj_id: &ObjId) -> NdnResult<NamedObjectMeta> {
        self.store.get_object_meta(obj_id)?.ok_or(NdmError::NotFound)
    }

    /// ------------------------------
    /// Pull APIs
    /// ------------------------------

    pub fn pull(&self, path: &NdmPath, remote: PullContext) -> NdnResult<()> {
        // Resolve path to obj id. If inode, read its committed/linked obj id.
        let obj_id = self.resolve_objid_for_read(path)?;
        self.store.schedule_pull(&obj_id, remote)
    }

    pub fn pull_by_objid(&self, obj_id: &ObjId, remote: PullContext) -> NdnResult<()> {
        self.store.schedule_pull(obj_id, remote)
    }

    fn resolve_objid_for_read(&self, path: &NdmPath) -> NdnResult<ObjId> {
        match self.resolve_path(path)? {
            ResolvedRef::ObjId(oid) => Ok(oid),
            ResolvedRef::Inode(fid) => {
                let node = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                match node.state {
                    NodeState::Linked(s) => Ok(s.obj_id),
                    NodeState::Finalized(s) => Ok(s.obj_id),
                    NodeState::Committed { base_obj_id } => Ok(base_obj_id),
                    _ => Err(NdmError::NeedPull),
                }
            }
        }
    }

    /// ------------------------------
    /// Stat APIs
    /// ------------------------------

    pub fn stat(&self, path: &NdmPath) -> NdnResult<PathStat> {
        match self.resolve_path(path)? {
            ResolvedRef::ObjId(oid) => {
                let meta = self.store.get_object_meta(&oid)?.ok_or(NdmError::NotFound)?;
                let materialized = self.store.has_materialized(&oid)?;
                Ok(PathStat {
                    path: path.clone(),
                    kind: meta.kind,
                    resolved: ResolvedRef::ObjId(oid),
                    materialized,
                    state_hint: None,
                })
            }
            ResolvedRef::Inode(fid) => {
                let node = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                let (kind, resolved, materialized) = match node.kind {
                    NodeKind::Dir => {
                        let base = node.base_obj_id.clone();
                        let materialized = match &base {
                            None => true,
                            Some(oid) => self.store.has_materialized(oid)?,
                        };
                        (ObjectKind::Dir, ResolvedRef::Inode(fid.clone()), materialized)
                    }
                    NodeKind::File => {
                        // Determine obj_id if committed/linked/finalized
                        let obj_opt = match &node.state {
                            NodeState::Linked(s) => Some(s.obj_id.clone()),
                            NodeState::Finalized(s) => Some(s.obj_id.clone()),
                            NodeState::Committed { base_obj_id } => Some(base_obj_id.clone()),
                            _ => node.base_obj_id.clone(),
                        };
                        let materialized = match &obj_opt {
                            None => true,
                            Some(oid) => self.store.has_materialized(oid)?,
                        };
                        (ObjectKind::File, ResolvedRef::Inode(fid.clone()), materialized)
                    }
                };
                Ok(PathStat {
                    path: path.clone(),
                    kind,
                    resolved,
                    materialized,
                    state_hint: Some(node.state),
                })
            }
        }
    }

    /// ------------------------------
    /// Directory APIs (Overlay merge in NDM)
    /// ------------------------------

    pub fn list(&self, dir_path: &NdmPath) -> NdnResult<Vec<DirEntryView>> {
        // Resolve dir_path to a directory inode (recommended) or an ObjId dir.
        let resolved = self.resolve_path(dir_path)?;

        match resolved {
            ResolvedRef::ObjId(dir_oid) => {
                // Read-only view over a DirObject (no upper dentries).
                let dir_obj = self.store.get_dir_object(&dir_oid)?;
                let mut out = Vec::new();
                for c in dir_obj.children {
                    out.push(DirEntryView {
                        name: c.name,
                        kind: c.kind,
                        target: ResolvedRef::ObjId(c.obj_id),
                    });
                }
                Ok(out)
            }
            ResolvedRef::Inode(dir_id) => {
                let node = self.fsmeta.get_node(&dir_id)?.ok_or(NdmError::NotFound)?;
                if node.kind != NodeKind::Dir {
                    return Err(NdmError::NotFound);
                }

                // Upper layer
                let upper = self.fsmeta.list_dentries(&dir_id)?;
                let mut upper_map: HashMap<String, DentryTarget> = HashMap::new();
                for d in upper {
                    upper_map.insert(d.name.clone(), d.target.clone());
                }

                // Base layer (DirObject children)
                let base_children = match &node.base_obj_id {
                    None => Vec::new(),
                    Some(oid) => {
                        if !self.store.has_materialized(oid)? {
                            return Err(NdmError::NeedPull);
                        }
                        self.store.get_dir_object(oid)?.children
                    }
                };

                // Merge:
                // - start from base
                // - apply upper overrides
                let mut result: BTreeMap<String, DirEntryView> = BTreeMap::new();

                for c in base_children {
                    result.insert(
                        c.name.clone(),
                        DirEntryView {
                            name: c.name,
                            kind: c.kind,
                            target: ResolvedRef::ObjId(c.obj_id),
                        },
                    );
                }

                for (name, t) in upper_map {
                    match t {
                        DentryTarget::Tombstone => {
                            result.remove(&name);
                        }
                        DentryTarget::IndexNodeId(fid) => {
                            // Need kind. We can read inode kind.
                            let inode = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                            let kind = match inode.kind {
                                NodeKind::Dir => ObjectKind::Dir,
                                NodeKind::File => ObjectKind::File,
                            };
                            result.insert(
                                name.clone(),
                                DirEntryView {
                                    name,
                                    kind,
                                    target: ResolvedRef::Inode(fid),
                                },
                            );
                        }
                        DentryTarget::ObjId(oid) => {
                            let meta = self.store.get_object_meta(&oid)?.unwrap_or(NamedObjectMeta {
                                obj_id: oid.clone(),
                                kind: ObjectKind::Unknown,
                                size: None,
                            });
                            result.insert(
                                name.clone(),
                                DirEntryView {
                                    name,
                                    kind: meta.kind,
                                    target: ResolvedRef::ObjId(oid),
                                },
                            );
                        }
                    }
                }

                Ok(result.into_values().collect())
            }
        }
    }

    /// Create an empty directory at path (mutable).
    pub fn create_dir(&self, dir_path: &NdmPath, mount_mode: MountMode) -> NdnResult<()> {
        // PSEUDOCODE:
        // 1) Resolve parent dir inode; ensure it is inode (materialize if needed).
        // 2) Check parent mount_mode != ReadOnly.
        // 3) alloc inode kind=dir, set mount_mode, base_obj_id = empty DirObject or None.
        // 4) In parent dentries: upsert name -> IndexNodeId(new_dir)
        todo!("create_dir")
    }

    /// Bind an existing DirObjectId at path.
    /// - overlay: allow upper dentries modifications
    /// - read_only: forbid modifications
    pub fn add_dir(&self, path: &NdmPath, dir_obj_id: ObjId, mount_mode: MountMode) -> NdnResult<()> {
        // PSEUDOCODE:
        // Always create a dir inode so we can store mount_mode/rev/base_obj_id.
        // 1) resolve parent dir inode
        // 2) alloc inode kind=dir (mount_mode)
        // 3) set node.base_obj_id = dir_obj_id, node.rev = 0
        // 4) parent dentries: name -> IndexNodeId(new_inode)
        todo!("add_dir")
    }

    /// Bind an existing FileObjectId at path (O(1) meta).
    /// Strategy B allows either:
    /// - Create inode and use dentry->IndexNodeId (preferred if future writes expected)
    /// - Or set dentry->ObjId directly (fast, read-only unless later materialized)
    pub fn add_file(&self, path: &NdmPath, file_obj_id: ObjId) -> NdnResult<()> {
        // PSEUDOCODE:
        // 1) resolve parent dir inode
        // 2) parent mount_mode must allow
        // 3) upsert dentry(parent, name, ObjId(file_obj_id))
        todo!("add_file")
    }

    /// Delete a name from directory view.
    /// Overlay semantics: write tombstone so base won't "reappear".
    pub fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        // PSEUDOCODE:
        // 1) resolve parent dir inode + name
        // 2) check mount_mode != ReadOnly
        // 3) txn: set tombstone(parent, name)
        todo!("delete")
    }

    /// Rename/move a path.
    /// Overlay semantics:
    /// - If entry is upper: move/rename dentry row.
    /// - If entry is base-only: add new name with ObjId + tombstone old name.
    pub fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()> {
        // PSEUDOCODE:
        // 1) parse old_parent, old_name; new_parent, new_name
        // 2) ensure both parents are dir inodes
        // 3) read mount_mode of both dirs (deny if ReadOnly)
        // 4) Determine source entry by overlay lookup:
        //    - if upper has IndexNodeId/ObjId: we can "move" that dentry row
        //    - if base-only: we need base child obj_id to create new dentry ObjId and tombstone old
        // 5) Transaction:
        //    - Check new_name not conflicting (unless overwrite allowed)
        //    - Apply updates atomically across both dirs (order by file_id to avoid deadlock)
        todo!("move_path/rename")
    }

    /// Objectify directory delta into a new DirObject (staged commit for dirs).
    /// - Merge base children + upper dentries (tombstones applied)
    /// - Build new DirObject (sorted)
    /// - Put into store
    /// - Commit in fsmeta: set base_obj_id=new, clear dentries, bump rev
    pub fn cacl_dir(&self, dir_path: &NdmPath) -> NdnResult<ObjId> {
        // PSEUDOCODE:
        // 1) resolve to dir inode
        // 2) read dir node: base_obj_id + rev0 + mount_mode
        // 3) deny if read_only
        // 4) fetch upper dentries snapshot
        // 5) stream base children (if any) and merge with upper
        // 6) build new DirObject; compute obj_id (canonical encoding)
        // 7) store.put_dir_object(new)
        // 8) fsmeta txn: check rev==rev0, update base_obj_id, clear dentries, bump rev
        todo!("cacl_dir")
    }

    /// ------------------------------
    /// File Write APIs (Strict Single Writer + staged commit)
    /// ------------------------------

    /// Create or open a writable file (strict single writer).
    /// - Allocates / reuses inode (IndexNodeId)
    /// - Acquires file lease (session+fence)
    /// - Allocates a FileBuffer on a BufferNode (GFS-like)
    pub fn create_file(&self, path: &NdmPath, expect_size: Option<u64>, session: SessionId) -> NdnResult<FileBufferHandle> {
        // PSEUDOCODE:
        // 1) resolve parent dir inode + name; check mount_mode != ReadOnly
        // 2) ensure dentry -> IndexNodeId (materialize inode if currently ObjId)
        // 3) acquire lease on that file inode (session, ttl) => fence
        // 4) pick buffer node and create remote file path
        // 5) fsmeta.update_file_state(file_id, Writing{...})
        todo!("create_file")
    }

    pub fn append(&self, fb: &FileBufferHandle, data: &[u8]) -> NdnResult<()> {
        // 1) verify lease (session+fence) by reading inode state or relying on buffer node fencing
        // 2) rpc append to buffer node
        // 3) update last_write_at (optional batched)
        let node = self.buffer_master.get_node(&fb.buffer_node)?;
        node.append(&fb.remote_path, data)
    }

    pub fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        let node = self.buffer_master.get_node(&fb.buffer_node)?;
        node.flush(&fb.remote_path)
    }

    /// Close the file: end the write session.
    /// IMPORTANT: close does NOT immediately hash/commit. It enters Cooling.
    pub fn close_file(&self, fb: FileBufferHandle) -> NdnResult<()> {
        // PSEUDOCODE:
        // 1) flush
        // 2) mark inode state: Writing -> Cooling {node, path, closed_at}
        // 3) release lease (or shorten ttl)
        // 4) background will later objectify (Cooling->Linked)
        todo!("close_file")
    }

    /// Force objectification now (Cooling->Linked), useful for "publish now".
    pub fn force_link(&self, path: &NdmPath) -> NdnResult<ObjId> {
        // PSEUDOCODE:
        // 1) resolve to file inode
        // 2) if Writing: require close or deny
        // 3) if Cooling: run objectify and update to Linked, return obj_id
        // 4) if Linked/Finalized/Committed: return existing obj_id
        todo!("force_link")
    }

    /// Force finalization now (Linked->Finalized): move data into internal store.
    pub fn force_finalize(&self, path: &NdmPath) -> NdnResult<ObjId> {
        // PSEUDOCODE:
        // 1) resolve to file inode
        // 2) if Linked: push_to_store and promote_obj_to_internal; update state Finalized
        // 3) else if already Finalized: return
        todo!("force_finalize")
    }

    /// Snapshot semantics: copy committed version (no data copy).
    pub fn snapshot(&self, src: &NdmPath, dst: &NdmPath) -> NdnResult<()> {
        // PSEUDOCODE:
        // - If src is working (Writing/Cooling) => PATH_BUSY
        // - Otherwise bind dst to same ObjId (dentry ObjId or inode base snapshot)
        todo!("snapshot")
    }

    /// Erase local materialized data by ObjId. Does not change namespace bindings.
    pub fn erase_obj_by_id(&self, obj_id: &ObjId) -> NdnResult<()> {
        self.store.erase_obj_data(obj_id)
    }

    /// ------------------------------
    /// Internal helpers for file lifecycle
    /// ------------------------------

    fn try_cooling_to_linked(&self, node: &NodeRecord) -> NdnResult<()> {
        // Only if enough time passed since closed_at
        let cooling = match &node.state {
            NodeState::Cooling(s) => s.clone(),
            _ => return Ok(()),
        };

        if SystemTime::now().duration_since(cooling.closed_at).unwrap_or(Duration::from_secs(0)) < self.cfg.cooling_grace {
            return Ok(()); // not ready
        }

        // Seal + objectify on buffer node (no data copy)
        let bn = self.buffer_master.get_node(&cooling.buffer_node)?;
        bn.seal(&cooling.remote_path)?;
        let obj = bn.objectify(&cooling.remote_path)?;

        // Register chunk links in store (ExternalFile) and put FileObject meta
        for link in obj.chunks.iter() {
            self.store.upsert_chunk_link(
                &link.chunk_id,
                LinkType::ExternalFile {
                    url: link.url.clone(),
                    range: link.range,
                    qcid: link.qcid.clone(),
                },
            )?;
        }

        // Put file object metadata (and the file object itself if needed)
        // NOTE: obj.file_obj_id should match canonical encoding hash.
        let file_obj = FileObject {
            obj_id: obj.file_obj_id.clone(),
            chunks: obj.chunks.iter().map(|x| x.chunk_id.clone()).collect(),
            size: obj.file_size,
        };
        self.store.put_file_object(&file_obj)?;
        self.store.put_object_meta(&NamedObjectMeta {
            obj_id: obj.file_obj_id.clone(),
            kind: ObjectKind::File,
            size: Some(obj.file_size),
        })?;

        // Update inode state -> Linked, publish base_obj_id
        self.fsmeta.update_file_state(
            &node.file_id,
            NodeState::Linked(FileLinkedState {
                obj_id: obj.file_obj_id.clone(),
                buffer_node: cooling.buffer_node.clone(),
                remote_path: cooling.remote_path.clone(),
                linked_at: SystemTime::now(),
            }),
        )?;

        Ok(())
    }

    fn try_linked_to_finalized(&self, node: &NodeRecord) -> NdnResult<()> {
        let linked = match &node.state {
            NodeState::Linked(s) => s.clone(),
            _ => return Ok(()),
        };

        if SystemTime::now().duration_since(linked.linked_at).unwrap_or(Duration::from_secs(0)) < self.cfg.frozen_grace {
            return Ok(());
        }

        // Move data into internal store
        let bn = self.buffer_master.get_node(&linked.buffer_node)?;
        bn.push_to_store(&linked.remote_path, self.store.as_ref())?;

        // Promote in store: chunks become internal COMPLETE; remove/ignore ExternalFile links
        self.store.promote_obj_to_internal(&linked.obj_id)?;

        // Update inode state
        self.fsmeta.update_file_state(
            &node.file_id,
            NodeState::Finalized(FileFinalizedState {
                obj_id: linked.obj_id.clone(),
                finalized_at: SystemTime::now(),
            }),
        )?;

        // Cleanup buffer file (best effort)
        let _ = bn.delete_buffer_file(&linked.remote_path);

        Ok(())
    }
}

/// ------------------------------
/// Lookup helper enum
/// ------------------------------
#[derive(Clone, Debug)]
enum LookupOne {
    UpperIndexNodeId(IndexNodeId),
    UpperObjId(ObjId),
    BaseObjId(ObjId),
    Tombstone,
    NotFound,
}

/// ------------------------------
/// Utility helpers
/// ------------------------------

fn is_valid_name(name: &str) -> bool {
    // PSEUDOCODE:
    // - forbid empty
    // - forbid "/" and null
    // - define normalization rules (case sensitivity, unicode NFC, etc.)
    !name.is_empty() && !name.contains('/')
}

/// ------------------------------
/// Notes for implementors
/// ------------------------------
///
/// 1) Directories:
///    - In normal overlay mode, directories SHOULD be materialized as inodes.
///    - read_only mount can be represented as inode too, but should deny structure changes.
///    - DentryTarget::ObjId is still useful for:
///       a) base-only file entries
///       b) renamed base entries without materializing inode
///       c) "fast bind" semantics where future writes are not expected
///
/// 2) Tombstones:
///    - delete/rename of base entries MUST write tombstones; never "DELETE row" in overlay mode.
///    - tombstones should be cleaned when cacl_dir commits a new DirObject snapshot.
///
/// 3) Directory concurrency:
///    - For short operations, a SQLite txn may be enough.
///    - For distributed fsmeta, use dir.rev OCC (expected_rev) in bump_dir_rev.
///    - For cross-dir move, lock/txn ordering by IndexNodeId to avoid deadlocks.
///
/// 4) File staged commit:
///    - close_file should be fast: only transition to Cooling and release lease.
///    - objectify runs after debounce (cooling_grace) and publishes ObjId via ExternalLink.
///    - promote runs after frozen_grace (or by force_finalize).
///
/// 5) Reliability / home environment:
///    - Before promotion, data lives on buffer node(s). For availability, buffer nodes may replicate (GFS style).
///    - For "zero-op", reliability may initially rely on periodic backup; later you can enable async mirror/replication.
///
