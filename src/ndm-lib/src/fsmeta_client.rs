/// ------------------------------
/// FsMeta: Inode/Dentry Model (Strategy B)
/// ------------------------------
use crate::{OpenWriteFlag, SessionId};
use krpc::{kRPC, RPCContext, RPCErrors, RPCHandler, RPCRequest, RPCResponse, RPCResult};
use ndn_lib::{NdmPath, NdnError, NdnResult, ObjId, OBJ_TYPE_DIR};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ClientSessionId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    File,   //File,Can finalized to FileObject
    Dir,    //Dir,Can finalized to DirObject
    Object, //Other Object,immutable object
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FileWorkingState {
    pub fb_handle: String,
    pub last_write_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FileCoolingState {
    pub fb_handle: String,
    pub closed_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FileLinkedState {
    pub obj_id: ObjId,
    pub qcid: ObjId,
    pub filebuffer_id: String,
    pub linked_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FinalizedObjState {
    pub obj_id: ObjId,
    pub finalized_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjStat {
    pub obj_id: ObjId,
    pub ref_count: u64,
    pub zero_since: Option<u64>,
    pub updated_at: u64,
}

pub type IndexNodeId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NodeState {
    /// Directory node (usually committed; delta lives in dentries)
    DirNormal,
    DirOverlay, // overlay mode (upper layer only)
    /// File node: idle state (not currently opened for write)
    FileNormal,
    /// File node: currently writable, bound to FileBuffer
    Working(FileWorkingState),
    /// File node: closed, waiting to stabilize (debounce)
    Cooling(FileCoolingState),
    /// File node: hashed & published via ExternalLink (content address stable)
    Linked(FileLinkedState),
    /// File & Object node: data promoted into internal store (chunks finalized)
    Finalized(FinalizedObjState),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub inode_id: IndexNodeId,
    pub ref_by: Option<u64>,
    pub state: NodeState,
    pub read_only: bool,
    pub base_obj_id: Option<ObjId>, // committed base snapshot (file or dir)
    pub rev: Option<u64>,           // only for dirs
    // metas:
    pub meta: Option<Value>,

    // leases:
    pub lease_client_session: Option<ClientSessionId>,
    pub lease_seq: Option<u64>,
    pub lease_expire_at: Option<u64>,
}

impl NodeRecord {
    pub fn get_node_kind(&self) -> NodeKind {
        match &self.state {
            NodeState::DirNormal | NodeState::DirOverlay => NodeKind::Dir,
            NodeState::FileNormal | NodeState::Working(_) | NodeState::Cooling(_) => NodeKind::File,
            NodeState::Linked(s) => {
                if s.obj_id.is_dir_object() {
                    NodeKind::Dir
                } else if s.obj_id.is_file_object() {
                    NodeKind::File
                } else {
                    NodeKind::Object
                }
            }
            NodeState::Finalized(s) => {
                if s.obj_id.is_dir_object() {
                    NodeKind::Dir
                } else if s.obj_id.is_file_object() {
                    NodeKind::File
                } else {
                    NodeKind::Object
                }
            }
        }
    }
}

/// Dentry target (Strategy B)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DentryTarget {
    IndexNodeId(IndexNodeId),
    SymLink(String),
    ObjId(ObjId),
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DentryRecord {
    pub id: u64,
    pub parent: IndexNodeId,
    pub name: String,
    pub target: DentryTarget,
    pub mtime: Option<u64>,
}

/// Materialized list entry from fsmeta list cache.
/// For `IndexNodeId` targets, `inode` is prefetched in start_list.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsMetaListEntry {
    pub name: String,
    pub target: DentryTarget,
    pub inode: Option<NodeRecord>,
}

// /// ------------------------------
// /// FsMeta Service original Traits
// /// ------------------------------
// pub trait FsMetaService: Send + Sync {
//     fn root_dir(&self) -> NdnResult<IndexNodeId>;

//     fn begin_txn(&self) -> NdnResult<String>;
//     fn get_inode(&mut self, id: &IndexNodeId,txid:Option<String>) -> NdnResult<Option<NodeRecord>>;
//     fn set_inode(&mut self, node: &NodeRecord,txid:Option<String>) -> NdnResult<()>;
//     /// Update inode state (atomic check via lease fencing where needed).
//     fn update_inode_state(&self, node_id: &IndexNodeId, new_state: NodeState,txid:Option<String>) -> NdnResult<()>;
//     fn alloc_inode(&self, node: &NodeRecord,txid:Option<String>) -> NdnResult<IndexNodeId>;

//     fn get_dentry(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<Option<DentryRecord>>;
//     fn list_dentries(&mut self, parent: &IndexNodeId,txid:Option<String>) -> NdnResult<Vec<DentryRecord>>;

//     fn upsert_dentry(&mut self, parent: &IndexNodeId, name: &str, target: DentryTarget,txid:Option<String>) -> NdnResult<()>;
//     fn remove_dentry_row(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<()>;

//     /// Set a tombstone (whiteout). Must not "DELETE" in overlay mode.
//     fn set_tombstone(&mut self, parent: &IndexNodeId, name: &str,txid:Option<String>) -> NdnResult<()> {
//         self.upsert_dentry(parent, name, DentryTarget::Tombstone,txid)
//     }

//     /// Atomically bump directory rev (OCC).
//     fn bump_dir_rev(&mut self, dir: &IndexNodeId, expected_rev: u64,txid:Option<String>) -> NdnResult<u64>;
//     fn commit(self: Box<Self>,txid:Option<String>) -> NdnResult<()>;
//     fn rollback(self: Box<Self>,txid:Option<String>) -> NdnResult<()>;

//     /// Acquire strict single-writer lease for file inode.
//     /// - Must return a fencing token (monotonic per file_id).
//     /// - Subsequent writes must provide the same (session, fence).
//     fn acquire_file_lease(
//         &self,
//         node_id: &IndexNodeId,
//         session: &SessionId,
//         ttl: Duration,
//     ) -> NdnResult<u64>;
//     fn renew_file_lease(
//         &self,
//         node_id: &IndexNodeId,
//         session: &SessionId,
//         lease_seq: u64,
//         ttl: Duration,
//     ) -> NdnResult<()>;
//     fn release_file_lease(&self, node_id: &IndexNodeId, session: &SessionId, lease_seq: u64) -> NdnResult<()>;
// }

/// ------------------------------
/// FsMeta kRPC Protocol
/// ------------------------------
pub enum OpenFileReaderResp {
    Object(ObjId, Option<String>),
    FileBufferId(String),
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
pub struct MoveOptions {
    /// Whether to overwrite destination if destination is visible in Upper layer.
    pub overwrite_upper: bool,
    /// If true: also check destination Base (merged view no-clobber).
    /// This requires Base materialized, otherwise NEED_PULL.
    pub strict_check_base: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaRootDirReq;

impl FsMetaRootDirReq {
    pub fn new() -> Self {
        Self
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaRootDirReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaResolvePathExReq {
    pub path: String,
    pub sym_count: u32,
}

impl FsMetaResolvePathExReq {
    pub fn new(path: String, sym_count: u32) -> Self {
        Self { path, sym_count }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaResolvePathExReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FsMetaResolvePathItem {
    Inode {
        inode_id: IndexNodeId,
        inode: NodeRecord,
    },
    ObjId(ObjId),
    SymLink(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaResolvePathResp {
    pub item: FsMetaResolvePathItem,
    pub inner_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaBeginTxnReq;

impl FsMetaBeginTxnReq {
    pub fn new() -> Self {
        Self
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaBeginTxnReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaGetInodeReq {
    pub id: IndexNodeId,
    pub txid: Option<String>,
}

impl FsMetaGetInodeReq {
    pub fn new(id: IndexNodeId, txid: Option<String>) -> Self {
        Self { id, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaGetInodeReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaSetInodeReq {
    pub node: NodeRecord,
    pub txid: Option<String>,
}

impl FsMetaSetInodeReq {
    pub fn new(node: NodeRecord, txid: Option<String>) -> Self {
        Self { node, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaSetInodeReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaUpdateInodeStateReq {
    pub node_id: IndexNodeId,
    pub new_state: NodeState,
    pub old_state: NodeState,
    pub txid: Option<String>,
}

impl FsMetaUpdateInodeStateReq {
    pub fn new(
        node_id: IndexNodeId,
        new_state: NodeState,
        old_state: NodeState,
        txid: Option<String>,
    ) -> Self {
        Self {
            node_id,
            new_state,
            old_state,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!(
                "Failed to parse FsMetaUpdateInodeStateReq: {}",
                e
            ))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaAllocInodeReq {
    pub node: NodeRecord,
    pub txid: Option<String>,
}

impl FsMetaAllocInodeReq {
    pub fn new(node: NodeRecord, txid: Option<String>) -> Self {
        Self { node, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaAllocInodeReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaGetDentryReq {
    pub parent: IndexNodeId,
    pub name: String,
    pub txid: Option<String>,
}

impl FsMetaGetDentryReq {
    pub fn new(parent: IndexNodeId, name: String, txid: Option<String>) -> Self {
        Self { parent, name, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaGetDentryReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaListDentriesReq {
    pub parent: IndexNodeId,
    pub txid: Option<String>,
}

impl FsMetaListDentriesReq {
    pub fn new(parent: IndexNodeId, txid: Option<String>) -> Self {
        Self { parent, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaListDentriesReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaStartListReq {
    pub parent: IndexNodeId,
    pub txid: Option<String>,
}

impl FsMetaStartListReq {
    pub fn new(parent: IndexNodeId, txid: Option<String>) -> Self {
        Self { parent, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaStartListReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaListNextReq {
    pub list_session_id: u64,
    pub page_size: u32,
}

impl FsMetaListNextReq {
    pub fn new(list_session_id: u64, page_size: u32) -> Self {
        Self {
            list_session_id,
            page_size,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaListNextReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaStopListReq {
    pub list_session_id: u64,
}

impl FsMetaStopListReq {
    pub fn new(list_session_id: u64) -> Self {
        Self { list_session_id }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaStopListReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaCreateDentryReq {
    pub parent: IndexNodeId,
    pub name: String,
    pub target: DentryTarget,
    pub expected_parent_rev: u64,
    pub txid: Option<String>,
}

impl FsMetaCreateDentryReq {
    pub fn new(
        parent: IndexNodeId,
        name: String,
        target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Self {
        Self {
            parent,
            name,
            target,
            expected_parent_rev,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaCreateDentryReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaDeleteDentryReq {
    pub parent: IndexNodeId,
    pub name: String,
    pub expected_parent_rev: u64,
    pub txid: Option<String>,
}

impl FsMetaDeleteDentryReq {
    pub fn new(
        parent: IndexNodeId,
        name: String,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Self {
        Self {
            parent,
            name,
            expected_parent_rev,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaDeleteDentryReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaReplaceTargetReq {
    pub parent: IndexNodeId,
    pub name: String,
    pub expected_old_target: DentryTarget,
    pub new_target: DentryTarget,
    pub expected_parent_rev: u64,
    pub txid: Option<String>,
}

impl FsMetaReplaceTargetReq {
    pub fn new(
        parent: IndexNodeId,
        name: String,
        expected_old_target: DentryTarget,
        new_target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Self {
        Self {
            parent,
            name,
            expected_old_target,
            new_target,
            expected_parent_rev,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaReplaceTargetReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaBumpDirRevReq {
    pub dir: IndexNodeId,
    pub expected_rev: u64,
    pub txid: Option<String>,
}

impl FsMetaBumpDirRevReq {
    pub fn new(dir: IndexNodeId, expected_rev: u64, txid: Option<String>) -> Self {
        Self {
            dir,
            expected_rev,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaBumpDirRevReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaCommitReq {
    pub txid: Option<String>,
}

impl FsMetaCommitReq {
    pub fn new(txid: Option<String>) -> Self {
        Self { txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaCommitReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaRollbackReq {
    pub txid: Option<String>,
}

impl FsMetaRollbackReq {
    pub fn new(txid: Option<String>) -> Self {
        Self { txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaRollbackReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaAcquireFileLeaseReq {
    pub node_id: IndexNodeId,
    pub session: SessionId,
    pub ttl: Duration,
}

impl FsMetaAcquireFileLeaseReq {
    pub fn new(node_id: IndexNodeId, session: SessionId, ttl: Duration) -> Self {
        Self {
            node_id,
            session,
            ttl,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!(
                "Failed to parse FsMetaAcquireFileLeaseReq: {}",
                e
            ))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaRenewFileLeaseReq {
    pub node_id: IndexNodeId,
    pub session: SessionId,
    pub lease_seq: u64,
    pub ttl: Duration,
}

impl FsMetaRenewFileLeaseReq {
    pub fn new(node_id: IndexNodeId, session: SessionId, lease_seq: u64, ttl: Duration) -> Self {
        Self {
            node_id,
            session,
            lease_seq,
            ttl,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaRenewFileLeaseReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaReleaseFileLeaseReq {
    pub node_id: IndexNodeId,
    pub session: SessionId,
    pub lease_seq: u64,
}

impl FsMetaReleaseFileLeaseReq {
    pub fn new(node_id: IndexNodeId, session: SessionId, lease_seq: u64) -> Self {
        Self {
            node_id,
            session,
            lease_seq,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!(
                "Failed to parse FsMetaReleaseFileLeaseReq: {}",
                e
            ))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaObjStatGetReq {
    pub obj_id: ObjId,
}

impl FsMetaObjStatGetReq {
    pub fn new(obj_id: ObjId) -> Self {
        Self { obj_id }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaObjStatGetReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaObjStatBumpReq {
    pub obj_id: ObjId,
    pub delta: i64,
    pub txid: Option<String>,
}

impl FsMetaObjStatBumpReq {
    pub fn new(obj_id: ObjId, delta: i64, txid: Option<String>) -> Self {
        Self {
            obj_id,
            delta,
            txid,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaObjStatBumpReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaObjStatListZeroReq {
    pub older_than_ts: u64,
    pub limit: u32,
}

impl FsMetaObjStatListZeroReq {
    pub fn new(older_than_ts: u64, limit: u32) -> Self {
        Self {
            older_than_ts,
            limit,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaObjStatListZeroReq: {}", e))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaObjStatDeleteIfZeroReq {
    pub obj_id: ObjId,
    pub txid: Option<String>,
}

impl FsMetaObjStatDeleteIfZeroReq {
    pub fn new(obj_id: ObjId, txid: Option<String>) -> Self {
        Self { obj_id, txid }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!(
                "Failed to parse FsMetaObjStatDeleteIfZeroReq: {}",
                e
            ))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaOpenFileWriterReq {
    pub parent: IndexNodeId,
    pub name: String,
    pub flag: OpenWriteFlag,
    pub expected_size: Option<u64>,
}

impl FsMetaOpenFileWriterReq {
    pub fn new(
        parent: IndexNodeId,
        name: String,
        flag: OpenWriteFlag,
        expected_size: Option<u64>,
    ) -> Self {
        Self {
            parent,
            name,
            flag,
            expected_size,
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, RPCErrors> {
        serde_json::from_value(value).map_err(|e| {
            RPCErrors::ParseRequestError(format!("Failed to parse FsMetaOpenFileWriterReq: {}", e))
        })
    }
}

pub enum FsMetaClient {
    InProcess(Box<dyn FsMetaHandler>),
    KRPC(Box<kRPC>),
}

impl FsMetaClient {
    const DEFAULT_CLIENT_SYMLINK_COUNT: u32 = 40;
    const ENSURE_DIR_RETRY_LIMIT: usize = 8;

    pub fn new_in_process(handler: Box<dyn FsMetaHandler>) -> Self {
        Self::InProcess(handler)
    }

    pub fn new_krpc(client: Box<kRPC>) -> Self {
        Self::KRPC(client)
    }

    pub async fn set_context(&self, _context: RPCContext) {
        // TODO: kRPC client does not support context propagation yet.
    }

    fn is_retryable_conflict(err: &RPCErrors) -> bool {
        match err {
            RPCErrors::ReasonError(msg) => {
                msg.contains("rev mismatch")
                    || msg.contains("already exists")
                    || msg.contains("conflict")
            }
            _ => false,
        }
    }

    fn join_child_path(parent: &NdmPath, name: &str) -> NdmPath {
        let parent_str = parent.as_str().trim_end_matches('/');
        if parent_str.is_empty() || parent_str == "/" {
            NdmPath::new(format!("/{}", name))
        } else {
            NdmPath::new(format!("{}/{}", parent_str, name))
        }
    }

    fn is_descendant_path(path: &NdmPath, ancestor: &NdmPath) -> bool {
        let path_str = path.as_str().trim_end_matches('/');
        let ancestor_str = ancestor.as_str().trim_end_matches('/');
        if ancestor_str.is_empty() {
            return false;
        }
        if path_str == ancestor_str {
            return false;
        }
        if !path_str.starts_with(ancestor_str) {
            return false;
        }
        let rest = &path_str[ancestor_str.len()..];
        rest.starts_with('/')
    }

    async fn parent_and_name_for_path(&self, path: &NdmPath) -> Result<(IndexNodeId, String), RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;
        if name.is_empty() {
            return Err(RPCErrors::ReasonError("invalid path".to_string()));
        }
        let parent_id = self.ensure_dir_inode(&parent_path).await?;
        Ok((parent_id, name))
    }

    async fn create_dir_under_parent(
        &self,
        parent_id: IndexNodeId,
        name: &str,
    ) -> Result<IndexNodeId, RPCErrors> {
        for _ in 0..Self::ENSURE_DIR_RETRY_LIMIT {
            let txid = self.begin_txn().await?;
            let txid_opt = Some(txid.clone());
            let result: Result<IndexNodeId, RPCErrors> = async {
                let parent_node = self
                    .get_inode(parent_id, txid_opt.clone())
                    .await?
                    .ok_or_else(|| RPCErrors::ReasonError("parent directory not found".to_string()))?;
                if parent_node.get_node_kind() != NodeKind::Dir {
                    return Err(RPCErrors::ReasonError("parent is not a directory".to_string()));
                }
                if parent_node.read_only {
                    return Err(RPCErrors::ReasonError("parent is read-only".to_string()));
                }

                let parent_rev = parent_node.rev.unwrap_or(0);
                let current = self
                    .get_dentry(parent_id, name.to_string(), txid_opt.clone())
                    .await?;

                match current {
                    Some(DentryRecord {
                        target: DentryTarget::IndexNodeId(id),
                        ..
                    }) => {
                        let node = self
                            .get_inode(id, txid_opt.clone())
                            .await?
                            .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;
                        if node.get_node_kind() != NodeKind::Dir {
                            return Err(RPCErrors::ReasonError("path is not a directory".to_string()));
                        }
                        Ok(id)
                    }
                    Some(DentryRecord {
                        target: DentryTarget::ObjId(obj_id),
                        ..
                    }) => {
                        if obj_id.obj_type != OBJ_TYPE_DIR {
                            return Err(RPCErrors::ReasonError("path is not a directory".to_string()));
                        }
                        let new_node = NodeRecord {
                            inode_id: 0,
                            ref_by: None,
                            read_only: false,
                            base_obj_id: Some(obj_id.clone()),
                            state: NodeState::DirOverlay,
                            rev: Some(0),
                            meta: None,
                            lease_client_session: None,
                            lease_seq: None,
                            lease_expire_at: None,
                        };
                        let new_id = self.alloc_inode(new_node, txid_opt.clone()).await?;
                        self.replace_target(
                            parent_id,
                            name.to_string(),
                            DentryTarget::ObjId(obj_id),
                            DentryTarget::IndexNodeId(new_id),
                            parent_rev,
                            txid_opt.clone(),
                        )
                        .await?;
                        Ok(new_id)
                    }
                    Some(DentryRecord {
                        target: DentryTarget::Tombstone,
                        ..
                    }) => {
                        let new_node = NodeRecord {
                            inode_id: 0,
                            ref_by: None,
                            read_only: false,
                            base_obj_id: None,
                            state: NodeState::DirNormal,
                            rev: Some(0),
                            meta: None,
                            lease_client_session: None,
                            lease_seq: None,
                            lease_expire_at: None,
                        };
                        let new_id = self.alloc_inode(new_node, txid_opt.clone()).await?;
                        self.replace_target(
                            parent_id,
                            name.to_string(),
                            DentryTarget::Tombstone,
                            DentryTarget::IndexNodeId(new_id),
                            parent_rev,
                            txid_opt.clone(),
                        )
                        .await?;
                        Ok(new_id)
                    }
                    Some(_) => Err(RPCErrors::ReasonError("path is not a directory".to_string())),
                    None => {
                        let new_node = NodeRecord {
                            inode_id: 0,
                            ref_by: None,
                            read_only: false,
                            base_obj_id: None,
                            state: NodeState::DirNormal,
                            rev: Some(0),
                            meta: None,
                            lease_client_session: None,
                            lease_seq: None,
                            lease_expire_at: None,
                        };
                        let new_id = self.alloc_inode(new_node, txid_opt.clone()).await?;
                        self.create_dentry(
                            parent_id,
                            name.to_string(),
                            DentryTarget::IndexNodeId(new_id),
                            parent_rev,
                            txid_opt,
                        )
                        .await?;
                        Ok(new_id)
                    }
                }
            }
            .await;

            match result {
                Ok(dir_id) => {
                    self.commit(Some(txid)).await?;
                    return Ok(dir_id);
                }
                Err(e) => {
                    let _ = self.rollback(Some(txid)).await;
                    if Self::is_retryable_conflict(&e) {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(RPCErrors::ReasonError(
            "conflict while creating directory".to_string(),
        ))
    }

    async fn materialize_dir_from_obj(
        &self,
        parent_id: IndexNodeId,
        name: &str,
        base_obj_id: &ObjId,
    ) -> Result<IndexNodeId, RPCErrors> {
        if base_obj_id.obj_type != OBJ_TYPE_DIR {
            return Err(RPCErrors::ReasonError("path is not a directory".to_string()));
        }

        for _ in 0..Self::ENSURE_DIR_RETRY_LIMIT {
            let txid = self.begin_txn().await?;
            let txid_opt = Some(txid.clone());
            let result: Result<IndexNodeId, RPCErrors> = async {
                let parent_node = self
                    .get_inode(parent_id, txid_opt.clone())
                    .await?
                    .ok_or_else(|| RPCErrors::ReasonError("parent directory not found".to_string()))?;
                if parent_node.get_node_kind() != NodeKind::Dir {
                    return Err(RPCErrors::ReasonError("parent is not a directory".to_string()));
                }
                if parent_node.read_only {
                    return Err(RPCErrors::ReasonError("parent is read-only".to_string()));
                }

                let parent_rev = parent_node.rev.unwrap_or(0);
                let current = self
                    .get_dentry(parent_id, name.to_string(), txid_opt.clone())
                    .await?;

                match current {
                    Some(DentryRecord {
                        target: DentryTarget::IndexNodeId(id),
                        ..
                    }) => {
                        let node = self
                            .get_inode(id, txid_opt.clone())
                            .await?
                            .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;
                        if node.get_node_kind() != NodeKind::Dir {
                            return Err(RPCErrors::ReasonError("path is not a directory".to_string()));
                        }
                        Ok(id)
                    }
                    Some(DentryRecord {
                        target: DentryTarget::ObjId(existing_obj),
                        ..
                    }) => {
                        if existing_obj.obj_type != OBJ_TYPE_DIR {
                            return Err(RPCErrors::ReasonError("path is not a directory".to_string()));
                        }
                        let new_node = NodeRecord {
                            inode_id: 0,
                            ref_by: None,
                            read_only: false,
                            base_obj_id: Some(existing_obj.clone()),
                            state: NodeState::DirOverlay,
                            rev: Some(0),
                            meta: None,
                            lease_client_session: None,
                            lease_seq: None,
                            lease_expire_at: None,
                        };
                        let new_id = self.alloc_inode(new_node, txid_opt.clone()).await?;
                        self.replace_target(
                            parent_id,
                            name.to_string(),
                            DentryTarget::ObjId(existing_obj),
                            DentryTarget::IndexNodeId(new_id),
                            parent_rev,
                            txid_opt.clone(),
                        )
                        .await?;
                        Ok(new_id)
                    }
                    Some(DentryRecord {
                        target: DentryTarget::Tombstone,
                        ..
                    }) => Err(RPCErrors::ReasonError("path not found".to_string())),
                    Some(_) => Err(RPCErrors::ReasonError("path is not a directory".to_string())),
                    None => {
                        let new_node = NodeRecord {
                            inode_id: 0,
                            ref_by: None,
                            read_only: false,
                            base_obj_id: Some(base_obj_id.clone()),
                            state: NodeState::DirOverlay,
                            rev: Some(0),
                            meta: None,
                            lease_client_session: None,
                            lease_seq: None,
                            lease_expire_at: None,
                        };
                        let new_id = self.alloc_inode(new_node, txid_opt.clone()).await?;
                        self.create_dentry(
                            parent_id,
                            name.to_string(),
                            DentryTarget::IndexNodeId(new_id),
                            parent_rev,
                            txid_opt,
                        )
                        .await?;
                        Ok(new_id)
                    }
                }
            }
            .await;

            match result {
                Ok(dir_id) => {
                    self.commit(Some(txid)).await?;
                    return Ok(dir_id);
                }
                Err(e) => {
                    let _ = self.rollback(Some(txid)).await;
                    if Self::is_retryable_conflict(&e) {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(RPCErrors::ReasonError(
            "conflict while materializing directory".to_string(),
        ))
    }

    pub async fn ensure_dir_inode(&self, path: &NdmPath) -> Result<IndexNodeId, RPCErrors> {
        if path.is_root() {
            return self.root_dir().await;
        }

        let components = path.components();
        if components.is_empty() {
            return self.root_dir().await;
        }

        let mut current_id = self.root_dir().await?;
        let mut current_path = NdmPath::new("/".to_string());

        for component in components {
            let child_path = Self::join_child_path(&current_path, &component);
            let dentry = self
                .get_dentry(current_id, component.to_string(), None)
                .await?;

            let next_id = match dentry {
                Some(DentryRecord {
                    target: DentryTarget::IndexNodeId(id),
                    ..
                }) => {
                    let inode = self
                        .get_inode(id, None)
                        .await?
                        .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;
                    if inode.get_node_kind() != NodeKind::Dir {
                        return Err(RPCErrors::ReasonError(format!(
                            "{} is not a directory",
                            child_path.as_str()
                        )));
                    }
                    id
                }
                Some(DentryRecord {
                    target: DentryTarget::ObjId(obj_id),
                    ..
                }) => {
                    if obj_id.obj_type != OBJ_TYPE_DIR {
                        return Err(RPCErrors::ReasonError(format!(
                            "{} is not a directory",
                            child_path.as_str()
                        )));
                    }
                    self.materialize_dir_from_obj(current_id, component, &obj_id)
                        .await?
                }
                Some(DentryRecord {
                    target: DentryTarget::Tombstone,
                    ..
                }) => self.create_dir_under_parent(current_id, component).await?,
                Some(DentryRecord {
                    target: DentryTarget::SymLink(_),
                    ..
                }) => {
                    return Err(RPCErrors::ReasonError(format!(
                        "{} is not a directory",
                        child_path.as_str()
                    )));
                }
                None => {
                    let resolved = self
                        .resolve_path_ex(&child_path, Self::DEFAULT_CLIENT_SYMLINK_COUNT)
                        .await
                        .map_err(|e| RPCErrors::ReasonError(e.to_string()))?;

                    match resolved {
                        Some(FsMetaResolvePathResp {
                            item:
                                FsMetaResolvePathItem::Inode {
                                    inode_id,
                                    inode,
                                },
                            inner_path: _,
                        }) => {
                            if inode.get_node_kind() != NodeKind::Dir {
                                return Err(RPCErrors::ReasonError(format!(
                                    "{} is not a directory",
                                    child_path.as_str()
                                )));
                            }
                            inode_id
                        }
                        Some(FsMetaResolvePathResp {
                            item: FsMetaResolvePathItem::ObjId(obj_id),
                            inner_path,
                        }) => {
                            if obj_id.obj_type != OBJ_TYPE_DIR {
                                return Err(RPCErrors::ReasonError(format!(
                                    "{} is not a directory",
                                    child_path.as_str()
                                )));
                            }
                            if inner_path.is_some() {
                                return Err(RPCErrors::ReasonError(format!(
                                    "{} is not a directory inode",
                                    child_path.as_str()
                                )));
                            }
                            self.materialize_dir_from_obj(current_id, component, &obj_id)
                                .await?
                        }
                        Some(FsMetaResolvePathResp {
                            item: FsMetaResolvePathItem::SymLink(_),
                            inner_path: _,
                        }) => {
                            return Err(RPCErrors::ReasonError(format!(
                                "{} is not a directory",
                                child_path.as_str()
                            )));
                        }
                        None => self.create_dir_under_parent(current_id, component).await?,
                    }
                }
            };

            current_id = next_id;
            current_path = child_path;
        }

        Ok(current_id)
    }

    pub async fn root_dir(&self) -> Result<IndexNodeId, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_root_dir(ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaRootDirReq::new();
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("root_dir", req_json).await?;
                result.as_u64().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected u64 result".to_string())
                })
            }
        }
    }

    pub async fn resolve_path_ex(
        &self,
        path: &NdmPath,
        sym_count: u32,
    ) -> NdnResult<Option<FsMetaResolvePathResp>> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_resolve_path_ex(path, sym_count, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaResolvePathExReq::new(path.as_str().to_string(), sym_count);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    NdnError::Internal(format!("Failed to serialize request: {}", e))
                })?;

                let result = client
                    .call("resolve_path_ex", req_json)
                    .await
                    .map_err(|e| {
                        NdnError::Internal(format!("resolve_path_ex rpc failed: {}", e))
                    })?;
                serde_json::from_value(result).map_err(|e| {
                    NdnError::Internal(format!(
                        "Expected Option<FsMetaResolvePathResp> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn begin_txn(&self) -> Result<String, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_begin_txn(ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaBeginTxnReq::new();
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("begin_txn", req_json).await?;
                result.as_str().map(|v| v.to_string()).ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected String result".to_string())
                })
            }
        }
    }

    pub async fn get_inode(
        &self,
        id: IndexNodeId,
        txid: Option<String>,
    ) -> Result<Option<NodeRecord>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_get_inode(id, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaGetInodeReq::new(id, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("get_inode", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!(
                        "Expected Option<NodeRecord> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn set_inode(&self, node: NodeRecord, txid: Option<String>) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_set_inode(node, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaSetInodeReq::new(node, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("set_inode", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn update_inode_state(
        &self,
        node_id: IndexNodeId,
        new_state: NodeState,
        old_state: NodeState,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_update_inode_state(node_id, new_state, old_state, txid, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaUpdateInodeStateReq::new(node_id, new_state, old_state, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("update_inode_state", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn alloc_inode(
        &self,
        node: NodeRecord,
        txid: Option<String>,
    ) -> Result<IndexNodeId, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_alloc_inode(node, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaAllocInodeReq::new(node, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("alloc_inode", req_json).await?;
                result.as_u64().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected u64 result".to_string())
                })
            }
        }
    }

    pub async fn get_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        txid: Option<String>,
    ) -> Result<Option<DentryRecord>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_get_dentry(parent, name, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaGetDentryReq::new(parent, name, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("get_dentry", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!(
                        "Expected Option<DentryRecord> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn list_dentries(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
    ) -> Result<Vec<DentryRecord>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_list_dentries(parent, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaListDentriesReq::new(parent, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("list_dentries", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!(
                        "Expected Vec<DentryRecord> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn start_list(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
    ) -> Result<u64, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_start_list(parent, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaStartListReq::new(parent, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("start_list", req_json).await?;
                result.as_u64().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected u64 result".to_string())
                })
            }
        }
    }

    pub async fn list_next(
        &self,
        list_session_id: u64,
        page_size: u32,
    ) -> Result<BTreeMap<String, FsMetaListEntry>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_list_next(list_session_id, page_size, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaListNextReq::new(list_session_id, page_size);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("list_next", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!(
                        "Expected BTreeMap<String, FsMetaListEntry> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn stop_list(&self, list_session_id: u64) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_stop_list(list_session_id, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaStopListReq::new(list_session_id);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("stop_list", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn create_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_create_dentry(parent, name, target, expected_parent_rev, txid, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req =
                    FsMetaCreateDentryReq::new(parent, name, target, expected_parent_rev, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("create_dentry", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn delete_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_delete_dentry(parent, name, expected_parent_rev, txid, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaDeleteDentryReq::new(parent, name, expected_parent_rev, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("delete_dentry", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn replace_target(
        &self,
        parent: IndexNodeId,
        name: String,
        expected_old_target: DentryTarget,
        new_target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_replace_target(
                        parent,
                        name,
                        expected_old_target,
                        new_target,
                        expected_parent_rev,
                        txid,
                        ctx,
                    )
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaReplaceTargetReq::new(
                    parent,
                    name,
                    expected_old_target,
                    new_target,
                    expected_parent_rev,
                    txid,
                );
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("replace_target", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn bump_dir_rev(
        &self,
        dir: IndexNodeId,
        expected_rev: u64,
        txid: Option<String>,
    ) -> Result<u64, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_bump_dir_rev(dir, expected_rev, txid, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaBumpDirRevReq::new(dir, expected_rev, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("bump_dir_rev", req_json).await?;
                result.as_u64().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected u64 result".to_string())
                })
            }
        }
    }

    pub async fn commit(&self, txid: Option<String>) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_commit(txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaCommitReq::new(txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("commit", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn rollback(&self, txid: Option<String>) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_rollback(txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaRollbackReq::new(txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("rollback", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn acquire_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        ttl: Duration,
    ) -> Result<u64, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_acquire_file_lease(node_id, session, ttl, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaAcquireFileLeaseReq::new(node_id, session, ttl);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("acquire_file_lease", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected LeaseFence result: {}", e))
                })
            }
        }
    }

    pub async fn renew_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        ttl: Duration,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_renew_file_lease(node_id, session, lease_seq, ttl, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaRenewFileLeaseReq::new(node_id, session, lease_seq, ttl);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("renew_file_lease", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn release_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
    ) -> Result<(), RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_release_file_lease(node_id, session, lease_seq, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaReleaseFileLeaseReq::new(node_id, session, lease_seq);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("release_file_lease", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected () result: {}", e))
                })
            }
        }
    }

    pub async fn obj_stat_get(&self, obj_id: ObjId) -> Result<Option<ObjStat>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_obj_stat_get(obj_id, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaObjStatGetReq::new(obj_id);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("obj_stat_get", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!(
                        "Expected Option<ObjStat> result: {}",
                        e
                    ))
                })
            }
        }
    }

    pub async fn obj_stat_bump(
        &self,
        obj_id: ObjId,
        delta: i64,
        txid: Option<String>,
    ) -> Result<u64, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler.handle_obj_stat_bump(obj_id, delta, txid, ctx).await
            }
            Self::KRPC(client) => {
                let req = FsMetaObjStatBumpReq::new(obj_id, delta, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("obj_stat_bump", req_json).await?;
                result.as_u64().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected u64 result".to_string())
                })
            }
        }
    }

    pub async fn obj_stat_list_zero(
        &self,
        older_than_ts: u64,
        limit: u32,
    ) -> Result<Vec<ObjId>, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_obj_stat_list_zero(older_than_ts, limit, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaObjStatListZeroReq::new(older_than_ts, limit);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("obj_stat_list_zero", req_json).await?;
                serde_json::from_value(result).map_err(|e| {
                    RPCErrors::ParserResponseError(format!("Expected Vec<ObjId> result: {}", e))
                })
            }
        }
    }

    pub async fn obj_stat_delete_if_zero(
        &self,
        obj_id: ObjId,
        txid: Option<String>,
    ) -> Result<bool, RPCErrors> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_obj_stat_delete_if_zero(obj_id, txid, ctx)
                    .await
            }
            Self::KRPC(client) => {
                let req = FsMetaObjStatDeleteIfZeroReq::new(obj_id, txid);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    RPCErrors::ReasonError(format!("Failed to serialize request: {}", e))
                })?;

                let result = client.call("obj_stat_delete_if_zero", req_json).await?;
                result.as_bool().ok_or_else(|| {
                    RPCErrors::ParserResponseError("Expected bool result".to_string())
                })
            }
        }
    }

    pub async fn open_file_writer(
        &self,
        path: &NdmPath,
        flag: OpenWriteFlag,
        expected_size: Option<u64>,
    ) -> NdnResult<String> {
        let (parent, name) = self
            .parent_and_name_for_path(path)
            .await
            .map_err(|e| NdnError::Internal(format!("open_file_writer failed: {}", e)))?;

        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_open_file_writer(parent, name.clone(), flag, expected_size, ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("open_file_writer failed: {}", e)))
            }
            Self::KRPC(client) => {
                let req = FsMetaOpenFileWriterReq::new(parent, name, flag, expected_size);
                let req_json = serde_json::to_value(&req).map_err(|e| {
                    NdnError::Internal(format!("Failed to serialize request: {}", e))
                })?;

                let result = client
                    .call("open_file_writer", req_json)
                    .await
                    .map_err(|e| {
                        NdnError::Internal(format!("open_file_writer rpc failed: {}", e))
                    })?;
                result
                    .as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| NdnError::Internal("Expected String result".to_string()))
            }
        }
    }

    pub async fn close_file_writer(&self, file_inode_id: IndexNodeId) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_close_file_writer(file_inode_id, ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("close_file_writer failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "close_file_writer is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn open_file_reader(&self, path: &NdmPath) -> NdnResult<OpenFileReaderResp> {
        match self {
            Self::InProcess(handler) => {
                let ctx = RPCContext::default();
                handler
                    .handle_open_file_reader(path.clone(), ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("open_file_reader failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "open_file_reader is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn set_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (parent, name) = self
                    .parent_and_name_for_path(path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("set_file failed: {}", e)))?;
                let ctx = RPCContext::default();
                handler
                    .handle_set_file(parent, name, obj_id, ctx)
                    .await
                    .map(|_| ())
                    .map_err(|e| NdnError::Internal(format!("set_file failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "set_file is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn set_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (parent, name) = self
                    .parent_and_name_for_path(path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("set_dir failed: {}", e)))?;
                let ctx = RPCContext::default();
                handler
                    .handle_set_dir(parent, name, dir_obj_id, ctx)
                    .await
                    .map(|_| ())
                    .map_err(|e| NdnError::Internal(format!("set_dir failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "set_dir is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (parent, name) = self
                    .parent_and_name_for_path(path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("delete failed: {}", e)))?;
                let ctx = RPCContext::default();
                handler
                    .handle_delete(parent, name, ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("delete failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "delete is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn move_path_with_opts(
        &self,
        old_path: &NdmPath,
        new_path: &NdmPath,
        opts: MoveOptions,
    ) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (src_parent_path, src_name) = old_path
                    .split_parent_name()
                    .ok_or_else(|| NdnError::InvalidParam("invalid source path".to_string()))?;
                let (dst_parent_path, dst_name) = new_path.split_parent_name().ok_or_else(|| {
                    NdnError::InvalidParam("invalid destination path".to_string())
                })?;
                if src_name.is_empty() || dst_name.is_empty() {
                    return Err(NdnError::InvalidParam("invalid path".to_string()));
                }
                let src_parent = self
                    .ensure_dir_inode(&src_parent_path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("move_path failed: {}", e)))?;
                let dst_parent = self
                    .ensure_dir_inode(&dst_parent_path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("move_path failed: {}", e)))?;
                if Self::is_descendant_path(new_path, old_path) {
                    return Err(NdnError::InvalidParam("invalid name".to_string()));
                }
                let ctx = RPCContext::default();
                handler
                    .handle_move_path_with_opts(
                        src_parent, src_name, dst_parent, dst_name, opts, ctx,
                    )
                    .await
                    .map_err(|e| NdnError::Internal(format!("move_path failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "move_path is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn symlink(&self, link_path: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (link_parent, link_name) = self
                    .parent_and_name_for_path(link_path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("symlink failed: {}", e)))?;
                let ctx = RPCContext::default();
                handler
                    .handle_symlink(link_parent, link_name, target.as_str().to_string(), ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("symlink failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "symlink is not supported in kRPC client".to_string(),
            )),
        }
    }

    pub async fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        match self {
            Self::InProcess(handler) => {
                let (parent, name) = self
                    .parent_and_name_for_path(path)
                    .await
                    .map_err(|e| NdnError::Internal(format!("create_dir failed: {}", e)))?;
                let ctx = RPCContext::default();
                handler
                    .handle_create_dir(parent, name, ctx)
                    .await
                    .map_err(|e| NdnError::Internal(format!("create_dir failed: {}", e)))
            }
            Self::KRPC(_) => Err(NdnError::Unsupported(
                "create_dir is not supported in kRPC client".to_string(),
            )),
        }
    }
}

// ========== Kernel : FsMetaHandler ==========
#[async_trait::async_trait]
pub trait FsMetaHandler: Send + Sync {
    //这里是区分不同namespace的起点
    //对大部分的MetaClient来说，这个值都是可以缓存下来用的
    async fn handle_root_dir(&self, ctx: RPCContext) -> Result<IndexNodeId, RPCErrors>;

    async fn handle_begin_txn(&self, ctx: RPCContext) -> Result<String, RPCErrors>;
    async fn handle_get_inode(
        &self,
        id: IndexNodeId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<Option<NodeRecord>, RPCErrors>;
    async fn handle_set_inode(
        &self,
        node: NodeRecord,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;
    async fn handle_update_inode_state(
        &self,
        node_id: IndexNodeId,
        new_state: NodeState,
        old_state: NodeState,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_alloc_inode(
        &self,
        node: NodeRecord,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<IndexNodeId, RPCErrors>;

    async fn handle_get_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<Option<DentryRecord>, RPCErrors>;

    async fn handle_list_dentries(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<Vec<DentryRecord>, RPCErrors>;

    async fn handle_start_list(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<u64, RPCErrors>;

    async fn handle_list_next(
        &self,
        list_session_id: u64,
        page_size: u32,
        ctx: RPCContext,
    ) -> Result<BTreeMap<String, FsMetaListEntry>, RPCErrors>;

    async fn handle_stop_list(
        &self,
        list_session_id: u64,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_create_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_delete_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        expected_parent_rev: u64,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_replace_target(
        &self,
        parent: IndexNodeId,
        name: String,
        expected_old_target: DentryTarget,
        new_target: DentryTarget,
        expected_parent_rev: u64,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_bump_dir_rev(
        &self,
        dir: IndexNodeId,
        expected_rev: u64,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<u64, RPCErrors>;

    async fn handle_commit(&self, txid: Option<String>, ctx: RPCContext) -> Result<(), RPCErrors>;
    async fn handle_rollback(&self, txid: Option<String>, ctx: RPCContext)
        -> Result<(), RPCErrors>;

    async fn handle_acquire_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        ttl: Duration,
        ctx: RPCContext,
    ) -> Result<u64, RPCErrors>;

    async fn handle_renew_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        ttl: Duration,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_release_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_obj_stat_get(
        &self,
        obj_id: ObjId,
        ctx: RPCContext,
    ) -> Result<Option<ObjStat>, RPCErrors>;

    async fn handle_obj_stat_bump(
        &self,
        obj_id: ObjId,
        delta: i64,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<u64, RPCErrors>;

    async fn handle_obj_stat_list_zero(
        &self,
        older_than_ts: u64,
        limit: u32,
        ctx: RPCContext,
    ) -> Result<Vec<ObjId>, RPCErrors>;

    async fn handle_obj_stat_delete_if_zero(
        &self,
        obj_id: ObjId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<bool, RPCErrors>;

    //------下面向是业务的高阶接口(减少调用fsmeta rpc的次数)------

    async fn handle_resolve_path_ex(
        &self,
        path: &NdmPath,
        sym_count: u32,
        ctx: RPCContext,
    ) -> NdnResult<Option<FsMetaResolvePathResp>>;

    async fn handle_set_file(
        &self,
        parent: IndexNodeId,
        name: String,
        obj_id: ObjId,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors>;

    async fn handle_set_dir(
        &self,
        parent: IndexNodeId,
        name: String,
        dir_obj_id: ObjId,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors>;

    async fn handle_delete(
        &self,
        parent: IndexNodeId,
        name: String,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_move_path_with_opts(
        &self,
        src_parent: IndexNodeId,
        src_name: String,
        dst_parent: IndexNodeId,
        dst_name: String,
        opts: MoveOptions,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_symlink(
        &self,
        link_parent: IndexNodeId,
        link_name: String,
        target: String,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    async fn handle_create_dir(
        &self,
        parent: IndexNodeId,
        name: String,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    //return FileHandleId,which is a unique identifier for the file buffer
    async fn handle_open_file_writer(
        &self,
        parent: IndexNodeId,
        name: String,
        flag: OpenWriteFlag,
        expected_size: Option<u64>,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors>;

    async fn handle_close_file_writer(
        &self,
        file_inode_id: IndexNodeId,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors>;

    //return ObjectId + innerpath or file_buffer_handle_id
    async fn handle_open_file_reader(
        &self,
        path: NdmPath,
        ctx: RPCContext,
    ) -> Result<OpenFileReaderResp, RPCErrors>;
}

pub struct FsMetaServerHandler<T: FsMetaHandler>(pub T);

impl<T: FsMetaHandler> FsMetaServerHandler<T> {
    pub fn new(handler: T) -> Self {
        Self(handler)
    }
}

#[async_trait::async_trait]
impl<T: FsMetaHandler> RPCHandler for FsMetaServerHandler<T> {
    async fn handle_rpc_call(
        &self,
        req: RPCRequest,
        ip_from: std::net::IpAddr,
    ) -> Result<RPCResponse, RPCErrors> {
        let seq = req.seq;
        let trace_id = req.trace_id.clone();
        let ctx = RPCContext::from_request(&req, ip_from);

        let result = match req.method.as_str() {
            "root_dir" => {
                let _req = FsMetaRootDirReq::from_json(req.params)?;
                let result = self.0.handle_root_dir(ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "resolve_path_ex" => {
                let req = FsMetaResolvePathExReq::from_json(req.params)?;
                let path = NdmPath::new(req.path);
                let result = self
                    .0
                    .handle_resolve_path_ex(&path, req.sym_count, ctx)
                    .await
                    .map_err(|e| {
                        RPCErrors::ReasonError(format!("resolve_path_ex failed: {}", e))
                    })?;
                RPCResult::Success(serde_json::json!(result))
            }
            "begin_txn" => {
                let _req = FsMetaBeginTxnReq::from_json(req.params)?;
                let result = self.0.handle_begin_txn(ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "get_inode" => {
                let req = FsMetaGetInodeReq::from_json(req.params)?;
                let result = self.0.handle_get_inode(req.id, req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "set_inode" => {
                let req = FsMetaSetInodeReq::from_json(req.params)?;
                let result = self.0.handle_set_inode(req.node, req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "update_inode_state" => {
                let req = FsMetaUpdateInodeStateReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_update_inode_state(
                        req.node_id,
                        req.new_state,
                        req.old_state,
                        req.txid,
                        ctx,
                    )
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "alloc_inode" => {
                let req = FsMetaAllocInodeReq::from_json(req.params)?;
                let result = self.0.handle_alloc_inode(req.node, req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "get_dentry" => {
                let req = FsMetaGetDentryReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_get_dentry(req.parent, req.name, req.txid, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "list_dentries" => {
                let req = FsMetaListDentriesReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_list_dentries(req.parent, req.txid, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "start_list" => {
                let req = FsMetaStartListReq::from_json(req.params)?;
                let result = self.0.handle_start_list(req.parent, req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "list_next" => {
                let req = FsMetaListNextReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_list_next(req.list_session_id, req.page_size, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "stop_list" => {
                let req = FsMetaStopListReq::from_json(req.params)?;
                let result = self.0.handle_stop_list(req.list_session_id, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "create_dentry" => {
                let req = FsMetaCreateDentryReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_create_dentry(
                        req.parent,
                        req.name,
                        req.target,
                        req.expected_parent_rev,
                        req.txid,
                        ctx,
                    )
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "delete_dentry" => {
                let req = FsMetaDeleteDentryReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_delete_dentry(
                        req.parent,
                        req.name,
                        req.expected_parent_rev,
                        req.txid,
                        ctx,
                    )
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "replace_target" => {
                let req = FsMetaReplaceTargetReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_replace_target(
                        req.parent,
                        req.name,
                        req.expected_old_target,
                        req.new_target,
                        req.expected_parent_rev,
                        req.txid,
                        ctx,
                    )
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "bump_dir_rev" => {
                let req = FsMetaBumpDirRevReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_bump_dir_rev(req.dir, req.expected_rev, req.txid, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "commit" => {
                let req = FsMetaCommitReq::from_json(req.params)?;
                let result = self.0.handle_commit(req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "rollback" => {
                let req = FsMetaRollbackReq::from_json(req.params)?;
                let result = self.0.handle_rollback(req.txid, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "acquire_file_lease" => {
                let req = FsMetaAcquireFileLeaseReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_acquire_file_lease(req.node_id, req.session, req.ttl, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "renew_file_lease" => {
                let req = FsMetaRenewFileLeaseReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_renew_file_lease(req.node_id, req.session, req.lease_seq, req.ttl, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "release_file_lease" => {
                let req = FsMetaReleaseFileLeaseReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_release_file_lease(req.node_id, req.session, req.lease_seq, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "obj_stat_get" => {
                let req = FsMetaObjStatGetReq::from_json(req.params)?;
                let result = self.0.handle_obj_stat_get(req.obj_id, ctx).await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "obj_stat_bump" => {
                let req = FsMetaObjStatBumpReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_obj_stat_bump(req.obj_id, req.delta, req.txid, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "obj_stat_list_zero" => {
                let req = FsMetaObjStatListZeroReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_obj_stat_list_zero(req.older_than_ts, req.limit, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "obj_stat_delete_if_zero" => {
                let req = FsMetaObjStatDeleteIfZeroReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_obj_stat_delete_if_zero(req.obj_id, req.txid, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            "open_file_writer" => {
                let req = FsMetaOpenFileWriterReq::from_json(req.params)?;
                let result = self
                    .0
                    .handle_open_file_writer(req.parent, req.name, req.flag, req.expected_size, ctx)
                    .await?;
                RPCResult::Success(serde_json::json!(result))
            }
            _ => {
                return Err(RPCErrors::UnknownMethod(req.method.clone()));
            }
        };

        Ok(RPCResponse {
            result,
            seq,
            trace_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};
    use std::io::{Read, Write};
    use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[derive(Clone)]
    struct MockHandler {
        state: Arc<Mutex<MockState>>,
    }

    struct MockState {
        root_dir: IndexNodeId,
        next_txn: u64,
        next_inode: IndexNodeId,
        next_dentry: u64,
        inodes: HashMap<IndexNodeId, NodeRecord>,
        dentries: HashMap<(IndexNodeId, String), DentryRecord>,
        next_list_session: u64,
        list_sessions: HashMap<u64, (BTreeMap<String, FsMetaListEntry>, Option<String>)>,
        dir_rev: HashMap<IndexNodeId, u64>,
        lease_seq: HashMap<IndexNodeId, u64>,
        obj_stats: HashMap<ObjId, ObjStat>,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState {
                    root_dir: 1,
                    next_txn: 0,
                    next_inode: 1,
                    next_dentry: 0,
                    inodes: HashMap::new(),
                    dentries: HashMap::new(),
                    next_list_session: 0,
                    list_sessions: HashMap::new(),
                    dir_rev: HashMap::new(),
                    lease_seq: HashMap::new(),
                    obj_stats: HashMap::new(),
                })),
            }
        }
    }

    #[async_trait::async_trait]
    impl FsMetaHandler for MockHandler {
        async fn handle_root_dir(&self, _ctx: RPCContext) -> Result<IndexNodeId, RPCErrors> {
            Ok(self.state.lock().unwrap().root_dir)
        }

        async fn handle_resolve_path_ex(
            &self,
            path: &NdmPath,
            sym_count: u32,
            _ctx: RPCContext,
        ) -> NdnResult<Option<FsMetaResolvePathResp>> {
            let state = self.state.lock().unwrap();

            let root_id = state.root_dir;
            if path.is_root() {
                let node = state.inodes.get(&root_id).cloned();
                return Ok(node.map(|inode| FsMetaResolvePathResp {
                    item: FsMetaResolvePathItem::Inode {
                        inode_id: root_id,
                        inode,
                    },
                    inner_path: None,
                }));
            }

            let components = path.components();

            let mut current_id = root_id;

            for (i, component) in components.iter().enumerate() {
                let is_last = i == components.len() - 1;

                let dentry = state
                    .dentries
                    .get(&(current_id, component.to_string()))
                    .cloned();

                match dentry {
                    Some(d) => match d.target {
                        DentryTarget::IndexNodeId(id) => {
                            if is_last {
                                let node = state.inodes.get(&id).cloned();
                                return Ok(node.map(|inode| FsMetaResolvePathResp {
                                    item: FsMetaResolvePathItem::Inode {
                                        inode_id: id,
                                        inode,
                                    },
                                    inner_path: None,
                                }));
                            }
                            current_id = id;
                        }
                        DentryTarget::SymLink(target_path) => {
                            let tail = if i + 1 >= components.len() {
                                None
                            } else {
                                Some(format!("/{}", components[i + 1..].join("/")))
                            };
                            if sym_count == 0 {
                                return Ok(Some(FsMetaResolvePathResp {
                                    item: FsMetaResolvePathItem::SymLink(target_path),
                                    inner_path: tail,
                                }));
                            }
                            return Err(NdnError::Unsupported(
                                "mock handler does not support symbolic link expansion".to_string(),
                            ));
                        }
                        DentryTarget::ObjId(obj_id) => {
                            let tail = if i + 1 >= components.len() {
                                None
                            } else {
                                Some(format!("/{}", components[i + 1..].join("/")))
                            };
                            return Ok(Some(FsMetaResolvePathResp {
                                item: FsMetaResolvePathItem::ObjId(obj_id),
                                inner_path: tail,
                            }));
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

            Ok(None)
        }

        async fn handle_begin_txn(&self, _ctx: RPCContext) -> Result<String, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            state.next_txn += 1;
            Ok(format!("tx-{}", state.next_txn))
        }

        async fn handle_get_inode(
            &self,
            id: IndexNodeId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Option<NodeRecord>, RPCErrors> {
            Ok(self.state.lock().unwrap().inodes.get(&id).cloned())
        }

        async fn handle_set_inode(
            &self,
            node: NodeRecord,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            self.state
                .lock()
                .unwrap()
                .inodes
                .insert(node.inode_id, node);
            Ok(())
        }

        async fn handle_update_inode_state(
            &self,
            node_id: IndexNodeId,
            new_state: NodeState,
            old_state: NodeState,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            let mut state = self.state.lock().unwrap();
            match state.inodes.get_mut(&node_id) {
                Some(node) => {
                    if node.state != old_state {
                        return Err(RPCErrors::ReasonError("inode state conflict".to_string()));
                    }
                    node.state = new_state;
                    Ok(())
                }
                None => Err(RPCErrors::ReasonError("inode not found".to_string())),
            }
        }

        async fn handle_alloc_inode(
            &self,
            mut node: NodeRecord,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<IndexNodeId, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            state.next_inode += 1;
            node.inode_id = state.next_inode;
            state.inodes.insert(node.inode_id, node);
            Ok(state.next_inode)
        }

        async fn handle_get_dentry(
            &self,
            parent: IndexNodeId,
            name: String,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Option<DentryRecord>, RPCErrors> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .dentries
                .get(&(parent, name))
                .cloned())
        }

        async fn handle_list_dentries(
            &self,
            parent: IndexNodeId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<Vec<DentryRecord>, RPCErrors> {
            let state = self.state.lock().unwrap();
            let mut out = Vec::new();
            for ((p, _), dentry) in state.dentries.iter() {
                if *p == parent {
                    out.push(dentry.clone());
                }
            }
            Ok(out)
        }

        async fn handle_start_list(
            &self,
            parent: IndexNodeId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let mut entries = BTreeMap::new();
            for ((p, _), dentry) in state.dentries.iter() {
                if *p != parent {
                    continue;
                }
                let target = dentry.target.clone();
                let inode = match &target {
                    DentryTarget::IndexNodeId(id) => state.inodes.get(id).cloned(),
                    _ => None,
                };
                entries.insert(
                    dentry.name.clone(),
                    FsMetaListEntry {
                        name: dentry.name.clone(),
                        target,
                        inode,
                    },
                );
            }

            state.next_list_session += 1;
            let list_session_id = state.next_list_session;
            state.list_sessions.insert(list_session_id, (entries, None));
            Ok(list_session_id)
        }

        async fn handle_list_next(
            &self,
            list_session_id: u64,
            page_size: u32,
            _ctx: RPCContext,
        ) -> Result<BTreeMap<String, FsMetaListEntry>, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let (entries, cursor) = state
                .list_sessions
                .get_mut(&list_session_id)
                .ok_or_else(|| RPCErrors::ReasonError("list session not found".to_string()))?;

            let start_bound = match cursor.as_ref() {
                Some(c) => std::ops::Bound::Excluded(c.clone()),
                None => std::ops::Bound::Unbounded,
            };
            let limit = if page_size == 0 {
                usize::MAX
            } else {
                page_size as usize
            };

            let mut out = BTreeMap::new();
            for (name, entry) in entries
                .range((start_bound, std::ops::Bound::Unbounded))
                .take(limit)
            {
                out.insert(name.clone(), entry.clone());
            }
            if let Some((last_name, _)) = out.iter().next_back() {
                *cursor = Some(last_name.clone());
            }

            Ok(out)
        }

        async fn handle_stop_list(
            &self,
            list_session_id: u64,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            self.state
                .lock()
                .unwrap()
                .list_sessions
                .remove(&list_session_id);
            Ok(())
        }

        async fn handle_create_dentry(
            &self,
            parent: IndexNodeId,
            name: String,
            target: DentryTarget,
            expected_parent_rev: u64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let parent_rev = state
                .inodes
                .get(&parent)
                .ok_or_else(|| RPCErrors::ReasonError("parent inode not found".to_string()))?
                .rev
                .unwrap_or(0);
            if parent_rev != expected_parent_rev {
                return Err(RPCErrors::ReasonError("rev mismatch".to_string()));
            }
            if state.dentries.contains_key(&(parent, name.clone())) {
                return Err(RPCErrors::ReasonError("dentry already exists".to_string()));
            }

            state.next_dentry += 1;
            let dentry_id = state.next_dentry;
            let record = DentryRecord {
                id: dentry_id,
                parent,
                name: name.clone(),
                target: target.clone(),
                mtime: None,
            };
            state.dentries.insert((parent, name), record);
            if let DentryTarget::IndexNodeId(new_inode_id) = target {
                let node = state
                    .inodes
                    .get_mut(&new_inode_id)
                    .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;
                if let Some(existing) = node.ref_by {
                    if existing != dentry_id {
                        return Err(RPCErrors::ReasonError(
                            "inode already referenced by another dentry".to_string(),
                        ));
                    }
                }
                node.ref_by = Some(dentry_id);
            }
            if let Some(parent_node) = state.inodes.get_mut(&parent) {
                parent_node.rev = Some(parent_rev + 1);
            }
            Ok(())
        }

        async fn handle_delete_dentry(
            &self,
            parent: IndexNodeId,
            name: String,
            expected_parent_rev: u64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let parent_rev = state
                .inodes
                .get(&parent)
                .ok_or_else(|| RPCErrors::ReasonError("parent inode not found".to_string()))?
                .rev
                .unwrap_or(0);
            if parent_rev != expected_parent_rev {
                return Err(RPCErrors::ReasonError("rev mismatch".to_string()));
            }

            if let Some(record) = state.dentries.remove(&(parent, name)) {
                if let DentryTarget::IndexNodeId(inode_id) = record.target {
                    if let Some(node) = state.inodes.get_mut(&inode_id) {
                        if node.ref_by == Some(record.id) {
                            node.ref_by = None;
                        }
                    }
                }
                if let Some(parent_node) = state.inodes.get_mut(&parent) {
                    parent_node.rev = Some(parent_rev + 1);
                }
            }
            Ok(())
        }

        async fn handle_replace_target(
            &self,
            parent: IndexNodeId,
            name: String,
            expected_old_target: DentryTarget,
            new_target: DentryTarget,
            expected_parent_rev: u64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let parent_rev = state
                .inodes
                .get(&parent)
                .ok_or_else(|| RPCErrors::ReasonError("parent inode not found".to_string()))?
                .rev
                .unwrap_or(0);
            if parent_rev != expected_parent_rev {
                return Err(RPCErrors::ReasonError("rev mismatch".to_string()));
            }

            let key = (parent, name.clone());
            let mut record = state
                .dentries
                .get(&key)
                .cloned()
                .ok_or_else(|| RPCErrors::ReasonError("dentry not found".to_string()))?;
            if record.target != expected_old_target {
                return Err(RPCErrors::ReasonError("dentry target mismatch".to_string()));
            }

            if record.target == new_target {
                return Ok(());
            }

            let old_target = record.target.clone();
            record.target = new_target.clone();
            state.dentries.insert(key, record.clone());

            if let DentryTarget::IndexNodeId(old_inode_id) = old_target {
                if let Some(node) = state.inodes.get_mut(&old_inode_id) {
                    if node.ref_by == Some(record.id) {
                        node.ref_by = None;
                    }
                }
            }
            if let DentryTarget::IndexNodeId(new_inode_id) = new_target {
                let node = state
                    .inodes
                    .get_mut(&new_inode_id)
                    .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;
                if let Some(existing) = node.ref_by {
                    if existing != record.id {
                        return Err(RPCErrors::ReasonError(
                            "inode already referenced by another dentry".to_string(),
                        ));
                    }
                }
                node.ref_by = Some(record.id);
            }
            if let Some(parent_node) = state.inodes.get_mut(&parent) {
                parent_node.rev = Some(parent_rev + 1);
            }
            Ok(())
        }

        async fn handle_bump_dir_rev(
            &self,
            dir: IndexNodeId,
            expected_rev: u64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let current = state.dir_rev.entry(dir).or_insert(0);
            if *current != expected_rev {
                return Err(RPCErrors::ReasonError("rev mismatch".to_string()));
            }
            *current += 1;
            Ok(*current)
        }

        async fn handle_commit(
            &self,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_rollback(
            &self,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_acquire_file_lease(
            &self,
            node_id: IndexNodeId,
            _session: SessionId,
            _ttl: Duration,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let seq = state.lease_seq.entry(node_id).or_insert(0);
            *seq += 1;
            Ok(*seq)
        }

        async fn handle_renew_file_lease(
            &self,
            node_id: IndexNodeId,
            _session: SessionId,
            lease_seq: u64,
            _ttl: Duration,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            let state = self.state.lock().unwrap();
            let current = state.lease_seq.get(&node_id).copied().unwrap_or(0);
            if current != lease_seq {
                return Err(RPCErrors::ReasonError("lease mismatch".to_string()));
            }
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
            obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<Option<ObjStat>, RPCErrors> {
            Ok(self.state.lock().unwrap().obj_stats.get(&obj_id).cloned())
        }

        async fn handle_obj_stat_bump(
            &self,
            obj_id: ObjId,
            delta: i64,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<u64, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let now = 0u64;
            let entry = state.obj_stats.entry(obj_id.clone()).or_insert(ObjStat {
                obj_id,
                ref_count: 0,
                zero_since: None,
                updated_at: now,
            });
            let next = entry.ref_count as i64 + delta;
            if next < 0 {
                return Err(RPCErrors::ReasonError(
                    "ref_count would be negative".to_string(),
                ));
            }
            entry.ref_count = next as u64;
            entry.updated_at = now;
            entry.zero_since = if entry.ref_count == 0 {
                Some(now)
            } else {
                None
            };
            Ok(entry.ref_count)
        }

        async fn handle_obj_stat_list_zero(
            &self,
            older_than_ts: u64,
            limit: u32,
            _ctx: RPCContext,
        ) -> Result<Vec<ObjId>, RPCErrors> {
            let state = self.state.lock().unwrap();
            let mut list = state
                .obj_stats
                .values()
                .filter(|s| s.ref_count == 0)
                .filter(|s| s.zero_since.unwrap_or(0) <= older_than_ts)
                .map(|s| (s.zero_since.unwrap_or(0), s.obj_id.clone()))
                .collect::<Vec<_>>();
            list.sort_by_key(|(ts, _)| *ts);
            Ok(list
                .into_iter()
                .take(limit as usize)
                .map(|(_, id)| id)
                .collect())
        }

        async fn handle_obj_stat_delete_if_zero(
            &self,
            obj_id: ObjId,
            _txid: Option<String>,
            _ctx: RPCContext,
        ) -> Result<bool, RPCErrors> {
            let mut state = self.state.lock().unwrap();
            let should_delete = state
                .obj_stats
                .get(&obj_id)
                .map(|s| s.ref_count == 0)
                .unwrap_or(false);
            if should_delete {
                state.obj_stats.remove(&obj_id);
            }
            Ok(should_delete)
        }

        async fn handle_set_file(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            Ok(String::new())
        }

        async fn handle_set_dir(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _dir_obj_id: ObjId,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            Ok(String::new())
        }

        async fn handle_delete(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_move_path_with_opts(
            &self,
            _src_parent: IndexNodeId,
            _src_name: String,
            _dst_parent: IndexNodeId,
            _dst_name: String,
            _opts: MoveOptions,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_symlink(
            &self,
            _link_parent: IndexNodeId,
            _link_name: String,
            _target: String,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_create_dir(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _ctx: RPCContext,
        ) -> Result<(), RPCErrors> {
            Ok(())
        }

        async fn handle_open_file_writer(
            &self,
            _parent: IndexNodeId,
            _name: String,
            _flag: OpenWriteFlag,
            _expected_size: Option<u64>,
            _ctx: RPCContext,
        ) -> Result<String, RPCErrors> {
            // Mock implementation - just return a dummy handle
            Ok("mock-file-handle".to_string())
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
            _path: NdmPath,
            _ctx: RPCContext,
        ) -> Result<OpenFileReaderResp, RPCErrors> {
            Err(RPCErrors::ReasonError("not implemented".to_string()))
        }
    }

    struct MockServer {
        url: String,
        shutdown: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
        addr: SocketAddr,
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn start_mock_server(handler: FsMetaServerHandler<MockHandler>) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("server addr");
        listener.set_nonblocking(true).expect("set nonblocking");
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_flag = shutdown.clone();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let _ = ready_tx.send(());
            while !shutdown_flag.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, peer)) => {
                        if let Ok(body) = read_http_body(&mut stream) {
                            if let Ok(req) = serde_json::from_slice::<RPCRequest>(&body) {
                                let ip_from = match peer {
                                    SocketAddr::V4(v4) => IpAddr::V4(*v4.ip()),
                                    SocketAddr::V6(v6) => IpAddr::V6(*v6.ip()),
                                };
                                let resp = rt
                                    .block_on(handler.handle_rpc_call(req, ip_from))
                                    .unwrap_or_else(|err| RPCResponse {
                                        result: RPCResult::Failed(err.to_string()),
                                        seq: 0,
                                        trace_id: None,
                                    });
                                let resp_body = serde_json::to_vec(&resp).unwrap();
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    resp_body.len()
                                );
                                let _ = stream.write_all(header.as_bytes());
                                let _ = stream.write_all(&resp_body);
                                let _ = stream.flush();
                            }
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        // Keep serving despite transient accept failures.
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                }
            }
        });
        let _ = ready_rx.recv_timeout(Duration::from_secs(1));
        MockServer {
            url: format!("http://{}", addr),
            shutdown,
            handle: Some(handle),
            addr,
        }
    }

    fn read_http_body(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        // Keep this generous to reduce flaky timeout failures on loaded CI machines.
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let mut buf = Vec::new();
        let mut temp = [0u8; 4096];
        let mut header_end = None;
        let mut content_len = 0usize;

        loop {
            let n = stream.read(&mut temp)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&temp[..n]);
            if header_end.is_none() {
                if let Some(pos) = find_double_crlf(&buf) {
                    header_end = Some(pos + 4);
                    content_len = parse_content_length(&buf[..pos + 4]);
                }
            }
            if let Some(end) = header_end {
                if buf.len() >= end + content_len {
                    break;
                }
            }
        }

        let end =
            header_end.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "bad http"))?;
        Ok(buf[end..end + content_len].to_vec())
    }

    fn find_double_crlf(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn parse_content_length(header: &[u8]) -> usize {
        let header_str = String::from_utf8_lossy(header);
        for line in header_str.lines().skip(1) {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("content-length") {
                    if let Ok(len) = v.trim().parse::<usize>() {
                        return len;
                    }
                }
            }
        }
        0
    }

    fn sample_node(inode_id: IndexNodeId) -> NodeRecord {
        NodeRecord {
            inode_id,
            ref_by: None,
            read_only: false,
            base_obj_id: None,
            state: NodeState::FileNormal,
            rev: None,
            meta: Some(json!({"k":"v"})),
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        }
    }

    #[tokio::test]
    async fn test_krpc_root_and_begin_txn() {
        let handler = MockHandler::new();
        let server = start_mock_server(FsMetaServerHandler::new(handler));
        let client = FsMetaClient::new_krpc(Box::new(kRPC::new(&server.url, None)));

        let root = client.root_dir().await.unwrap();
        assert_eq!(root, 1);

        let txid = client.begin_txn().await.unwrap();
        assert_eq!(txid, "tx-1");
    }

    #[tokio::test]
    async fn test_krpc_inode_and_dentry_flow() {
        let handler = MockHandler::new();
        let server = start_mock_server(FsMetaServerHandler::new(handler));
        let client = FsMetaClient::new_krpc(Box::new(kRPC::new(&server.url, None)));

        let node = sample_node(7);
        client.set_inode(node.clone(), None).await.unwrap();
        let got = client.get_inode(7, None).await.unwrap().unwrap();
        assert_eq!(got.inode_id, 7);

        client
            .create_dentry(
                7,
                "hello".to_string(),
                DentryTarget::IndexNodeId(7),
                0,
                None,
            )
            .await
            .unwrap();
        let dent = client
            .get_dentry(7, "hello".to_string(), None)
            .await
            .unwrap()
            .unwrap();
        match dent.target {
            DentryTarget::IndexNodeId(id) => assert_eq!(id, 7),
            _ => panic!("unexpected dentry target"),
        }

        let list = client.list_dentries(7, None).await.unwrap();
        assert_eq!(list.len(), 1);

        let list_session_id = client.start_list(7, None).await.unwrap();
        let page1 = client.list_next(list_session_id, 1).await.unwrap();
        assert_eq!(page1.len(), 1);
        assert_eq!(page1.keys().next().unwrap(), "hello");
        client.stop_list(list_session_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_krpc_lease_flow() {
        let handler = MockHandler::new();
        let server = start_mock_server(FsMetaServerHandler::new(handler));
        let client = FsMetaClient::new_krpc(Box::new(kRPC::new(&server.url, None)));

        let fence = client
            .acquire_file_lease(9, SessionId("s1".to_string()), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(fence, 1);

        client
            .renew_file_lease(
                9,
                SessionId("s1".to_string()),
                fence,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        client
            .release_file_lease(9, SessionId("s1".to_string()), fence)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_krpc_obj_stat_flow() {
        let handler = MockHandler::new();
        let server = start_mock_server(FsMetaServerHandler::new(handler));
        let client = FsMetaClient::new_krpc(Box::new(kRPC::new(&server.url, None)));

        let obj_id = ObjId::new("sha256:00").unwrap();
        let count = client.obj_stat_bump(obj_id.clone(), 1, None).await.unwrap();
        assert_eq!(count, 1);

        let stat = client.obj_stat_get(obj_id.clone()).await.unwrap().unwrap();
        assert_eq!(stat.ref_count, 1);

        let count = client
            .obj_stat_bump(obj_id.clone(), -1, None)
            .await
            .unwrap();
        assert_eq!(count, 0);

        let zeros = client.obj_stat_list_zero(u64::MAX, 10).await.unwrap();
        assert!(zeros.contains(&obj_id));

        let deleted = client
            .obj_stat_delete_if_zero(obj_id.clone(), None)
            .await
            .unwrap();
        assert!(deleted);
    }
}
