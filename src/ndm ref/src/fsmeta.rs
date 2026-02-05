
/// ------------------------------
/// FsMeta: Inode/Dentry Model (Strategy B)
/// ------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Dir,
    Object,
}

#[derive(Clone, Debug)]
pub struct FileWorkingState {
    pub fb_handle: FileBufferHandle,
    pub last_write_at: u64,
}

#[derive(Clone, Debug)]
pub struct FileCoolingState {
    pub fb_handle: FileBufferHandle,
    pub closed_at: u64,
}

#[derive(Clone, Debug)]
pub struct FileLinkedState {
    pub obj_id: ObjId, 
    pub qcid: ObjId,
    pub filebuffer_id: String,
    pub linked_at: u64,
}

#[derive(Clone, Debug)]
pub struct FinalizedObjState {
    pub obj_id: ObjId,
    pub finalized_at: u64,
}

pub type IndexNodeId = u64;

#[derive(Clone, Debug)]
pub enum NodeState {
    /// Directory node (usually committed; delta lives in dentries)
    DirNormal,
    DirOverlay, // overlay mode (upper layer only)
    /// File node: currently writable, bound to FileBuffer
    Working(FileWorkingState),
    /// File node: closed, waiting to stabilize (debounce)
    Cooling(FileCoolingState),
    /// File node: hashed & published via ExternalLink (content address stable)
    Linked(FileLinkedState),
    /// File & Object node: data promoted into internal store (chunks finalized)
    Finalized(FinalizedObjState),
}

#[derive(Clone, Debug)]
pub struct NodeRecord {
    pub inode_id: IndexNodeId,
    pub kind: NodeKind,
    pub read_only: bool,
    pub base_obj_id: Option<ObjId>,    // committed base snapshot (file or dir)
    pub state: NodeState,
    pub rev: Option<u64>,              // only for dirs
    // metas:
    pub meta:Option<Value>, 

    // leases:
    pub lease_client_session: Option<ClientSessionId>,
    pub lease_seq: Option<u64>,
    pub lease_expire_at: Option<u64>,
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
    pub mtime: Option<u64>,
}

/// ------------------------------
/// FsMeta Service Traits
/// ------------------------------
pub trait FsMetaService: Send + Sync {
    fn root_dir(&self) -> NdnResult<IndexNodeId>;

    fn begin_txn(&self) -> NdnResult<String>;
    fn get_inode(&mut self, id: &IndexNodeId,txid:Option<String>) -> NdnResult<Option<NodeRecord>>;
    fn set_inode(&mut self, node: &NodeRecord,txid:Option<String>) -> NdnResult<()>;
    /// Update inode state (atomic check via lease fencing where needed).
    fn update_inode_state(&self, node_id: &IndexNodeId, new_state: NodeState,txid:Option<String>) -> NdnResult<()>;
    fn alloc_inode(&self, node: &NodeRecord,txid:Option<String>) -> NdnResult<IndexNodeId>;

    fn get_dentry(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<Option<DentryRecord>>;
    fn list_dentries(&mut self, parent: &IndexNodeId,txid:Option<String>) -> NdnResult<Vec<DentryRecord>>;

    fn upsert_dentry(&mut self, parent: &IndexNodeId, name: &str, target: DentryTarget,txid:Option<String>) -> NdnResult<()>;
    fn remove_dentry_row(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<()>;

    /// Set a tombstone (whiteout). Must not "DELETE" in overlay mode.
    fn set_tombstone(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<()> {
        self.upsert_dentry(parent, name, DentryTarget::Tombstone,txid)
    }

    /// Atomically bump directory rev (OCC).
    fn bump_dir_rev(&mut self, dir: &IndexNodeId, expected_rev: u64,txid:Option<String>) -> NdnResult<u64>;
    fn commit(self: Box<Self>,txid:Option<String>) -> NdnResult<()>;
    fn rollback(self: Box<Self>,txid:Option<String>) -> NdnResult<()>;

    /// Acquire strict single-writer lease for file inode.
    /// - Must return a fencing token (monotonic per file_id).
    /// - Subsequent writes must provide the same (session, fence).
    fn acquire_file_lease(
        &self,
        node_id: &IndexNodeId,
        session: &SessionId,
        ttl: Duration,
    ) -> NdnResult<u64>;
    fn renew_file_lease(
        &self,
        node_id: &IndexNodeId,
        session: &SessionId,
        lease_seq: u64,
        ttl: Duration,
    ) -> NdnResult<()>;
    fn release_file_lease(&self, node_id: &IndexNodeId, session: &SessionId, lease_seq: u64) -> NdnResult<()>;
}
