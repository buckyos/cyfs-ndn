use fs_buffer::{FileBufferService, SessionId};
use krpc::{RPCContext, RPCErrors};
use log::{info, warn};
use named_store::NamedStoreMgr;
use ndm::{
    ClientSessionId, DentryRecord, DentryTarget, FsMetaHandler, FsMetaListEntry,
    FsMetaResolvePathItem, FsMetaResolvePathResp, IndexNodeId, MoveOptions, NdmInstanceId,
    NodeKind, NodeRecord, NodeState, ObjStat, OpenFileReaderResp, OpenWriteFlag,
};
use ndn_lib::{
    load_named_obj, ChunkId, DirObject, NdmPath, NdnError, NdnResult, ObjId, SimpleMapItem,
    OBJ_TYPE_DIR,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::background::BackgroundMgr;
use crate::list_cache::ListCache;
use crate::path_resolve_cache::{PathResolveCache, PathResolveTerminalValue};

const NODE_STATE_DIR_NORMAL: i64 = 0;
const NODE_STATE_DIR_OVERLAY: i64 = 1;
const NODE_STATE_WORKING: i64 = 2;
const NODE_STATE_COOLING: i64 = 3;
const NODE_STATE_LINKED: i64 = 4;
const NODE_STATE_FINALIZED: i64 = 5;

const DENTRY_TARGET_INODE: i64 = 0;
const DENTRY_TARGET_OBJ: i64 = 1;
const DENTRY_TARGET_TOMBSTONE: i64 = 2;
const DENTRY_TARGET_SYMLINK: i64 = 3;

const DEFAULT_SYMLINK_COUNT: u32 = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectKind {
    File,
    Dir,
    Unknown,
}

#[derive(Clone, Debug)]
enum MoveSource {
    Upper {
        target: DentryTarget,
        kind_hint: ObjectKind,
    },
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

#[derive(Clone, Debug)]
enum BaseChildLookup {
    Missing,
    DirObj(ObjId),
    NonDirObj(ObjId),
}

const ROOT_KEY: &str = "root_dir";

/// Default transaction timeout: 5 minutes
const TXN_TIMEOUT_SECS: u64 = 300;
/// Interval for cleaning up stale transactions: 30 seconds
const TXN_CLEANUP_INTERVAL_SECS: u64 = 30;
/// TTL for write leases created by open_file_writer: 5 minutes
const WRITE_LEASE_TTL_SECS: u64 = 300;
/// Interval for cleaning up expired write leases.
#[cfg(not(test))]
const WRITE_LEASE_CLEANUP_INTERVAL_SECS: u64 = 30;
#[cfg(test)]
const WRITE_LEASE_CLEANUP_INTERVAL_SECS: u64 = 1;

#[derive(Clone)]
struct TxnEntry {
    conn: Arc<Mutex<Connection>>,
    /// When this transaction was created (monotonic time)
    created_at: Instant,
    /// Last time this transaction was used (monotonic time)
    last_used_at: Arc<Mutex<Instant>>,
    /// Whether this transaction is being closed (commit/rollback in progress)
    closing: Arc<AtomicBool>,
    /// Number of in-flight operations using this transaction
    in_flight: Arc<AtomicU64>,

    /// Dentry edges touched in this transaction (for cache invalidation on commit)
    touched_edges: Arc<Mutex<HashSet<(IndexNodeId, String)>>>,
}

pub struct FSMetaService {
    db_path: String,
    conn: Arc<Mutex<Connection>>,
    txns: Arc<Mutex<HashMap<String, TxnEntry>>>,
    root_inode: IndexNodeId,
    txn_seq: AtomicU64,
    /// Transaction timeout in seconds
    txn_timeout_secs: u64,
    /// Handle to stop the transaction cleanup task on drop
    txn_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    /// Handle to stop the write lease cleanup task on drop
    lease_cleanup_handle: Option<tokio::task::JoinHandle<()>>,

    resolve_path_cache: Arc<RwLock<PathResolveCache>>,
    list_cache: Arc<Mutex<ListCache>>,

    /// Instance identifier for lease session naming (required for high-level file operations)
    instance: Option<NdmInstanceId>,
    /// File buffer service for managing write buffers (required for high-level file operations)
    buffer: Option<Arc<dyn FileBufferService>>,
    /// Named store manager for resolving base DirObject children
    store_mgr: Option<Arc<NamedStoreMgr>>,
    /// Background task manager for deferred operations (finalize/lazy migration)
    background_mgr: Arc<Mutex<BackgroundMgr>>,
}

impl FSMetaService {
    pub fn new(db_path: impl Into<String>) -> Result<Self, RPCErrors> {
        Self::new_with_timeout(db_path, TXN_TIMEOUT_SECS)
    }

    pub fn new_with_timeout(
        db_path: impl Into<String>,
        txn_timeout_secs: u64,
    ) -> Result<Self, RPCErrors> {
        let db_path = db_path.into();
        let conn = Connection::open_with_flags(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .map_err(|e| RPCErrors::ReasonError(format!("open db failed: {}", e)))?;
        Self::init_connection(&conn)?;
        Self::create_schema(&conn)?;
        Self::migrate_schema(&conn)?;
        let root_inode = Self::ensure_root_dir(&conn)?;
        let conn = Arc::new(Mutex::new(conn));

        let txns = Arc::new(Mutex::new(HashMap::new()));

        // Start background cleanup task
        let txn_cleanup_handle = Self::start_cleanup_task(txns.clone(), txn_timeout_secs);
        let lease_cleanup_handle = Self::start_write_lease_cleanup_task(conn.clone());

        Ok(Self {
            db_path,
            conn,
            txns,
            root_inode,
            txn_seq: AtomicU64::new(1),
            txn_timeout_secs,
            txn_cleanup_handle: Some(txn_cleanup_handle),
            lease_cleanup_handle: Some(lease_cleanup_handle),
            resolve_path_cache: Arc::new(RwLock::new(PathResolveCache::default())),
            list_cache: Arc::new(Mutex::new(ListCache::default())),
            instance: None,
            buffer: None,
            store_mgr: None,
            background_mgr: Arc::new(Mutex::new(BackgroundMgr::default())),
        })
    }

    /// Set instance and buffer for high-level file operations (open_file_writer, etc.)
    pub fn with_buffer(
        mut self,
        instance: NdmInstanceId,
        buffer: Arc<dyn FileBufferService>,
    ) -> Self {
        self.instance = Some(instance);
        self.buffer = Some(buffer);
        self
    }

    /// Set named store manager for DirObject child resolution.
    pub fn with_named_store(mut self, store_mgr: Arc<NamedStoreMgr>) -> Self {
        self.store_mgr = Some(store_mgr);
        self
    }

    /// Set background manager for deferred tasks.
    pub fn with_background_mgr(mut self, background_mgr: Arc<Mutex<BackgroundMgr>>) -> Self {
        self.background_mgr = background_mgr;
        self
    }

    /// Start a background task that periodically cleans up stale transactions
    fn start_cleanup_task(
        txns: Arc<Mutex<HashMap<String, TxnEntry>>>,
        timeout_secs: u64,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(TXN_CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;

                // Collect stale transactions
                let stale_txids: Vec<(String, TxnEntry)> = {
                    let mut txns_guard = match txns.lock() {
                        Ok(g) => g,
                        Err(e) => {
                            warn!("txn cleanup: failed to lock txns map: {}", e);
                            continue;
                        }
                    };

                    let timeout = Duration::from_secs(timeout_secs);
                    let now = Instant::now();
                    let stale: Vec<String> = txns_guard
                        .iter()
                        .filter(|(_, entry)| {
                            let last_used = entry
                                .last_used_at
                                .lock()
                                .map(|g| *g)
                                .unwrap_or(entry.created_at);
                            now.duration_since(last_used) > timeout
                        })
                        .map(|(txid, _)| txid.clone())
                        .collect();

                    stale
                        .into_iter()
                        .filter_map(|txid| txns_guard.remove(&txid).map(|entry| (txid, entry)))
                        .collect()
                };

                // Rollback stale transactions outside the lock
                for (txid, entry) in stale_txids {
                    // Mark as closing to prevent new operations
                    entry.closing.store(true, Ordering::SeqCst);

                    // Wait for in-flight operations to complete (with timeout)
                    let wait_start = Instant::now();
                    while entry.in_flight.load(Ordering::SeqCst) > 0 {
                        if wait_start.elapsed() > Duration::from_secs(5) {
                            warn!(
                                "txn cleanup: timeout waiting for in-flight ops for txid={}",
                                txid
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }

                    // Rollback the transaction
                    let txid_clone = txid.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        if let Ok(conn) = entry.conn.lock() {
                            let _ = conn.execute_batch("ROLLBACK");
                        }
                    })
                    .await;

                    if let Err(e) = result {
                        warn!(
                            "txn cleanup: rollback task failed for txid={}: {}",
                            txid_clone, e
                        );
                    } else {
                        info!(
                            "txn cleanup: rolled back stale transaction txid={}",
                            txid_clone
                        );
                    }
                }
            }
        })
    }

    /// Start a background task that periodically reclaims expired write leases.
    /// For timed-out leases on Working files, this also transitions node state to Cooling.
    fn start_write_lease_cleanup_task(conn: Arc<Mutex<Connection>>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(WRITE_LEASE_CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;

                let conn = conn.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let now = unix_timestamp() as i64;
                    let conn_guard = conn.lock().map_err(|e| {
                        RPCErrors::ReasonError(format!("conn lock poisoned: {}", e))
                    })?;
                    Self::reclaim_expired_write_leases(&conn_guard, now)
                })
                .await;

                match result {
                    Ok(Ok(reclaimed)) if reclaimed > 0 => {
                        info!(
                            "write lease cleanup: reclaimed {} expired leases",
                            reclaimed
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => warn!("write lease cleanup failed: {}", e),
                    Err(e) => warn!("write lease cleanup join failed: {}", e),
                }
            }
        })
    }

    fn reclaim_expired_write_leases(conn: &Connection, now: i64) -> Result<usize, RPCErrors> {
        let reclaimed = conn
            .execute(
                "UPDATE nodes
                 SET
                    lease_client_session = NULL,
                    lease_expire_at = NULL,
                    state = CASE WHEN state = ?2 THEN ?3 ELSE state END,
                    closed_at = CASE WHEN state = ?2 THEN ?1 ELSE closed_at END,
                    updated_at = ?1
                 WHERE
                    lease_expire_at IS NOT NULL
                    AND lease_expire_at <= ?1",
                params![now, NODE_STATE_WORKING, NODE_STATE_COOLING],
            )
            .map_err(map_db_err)?;
        Ok(reclaimed)
    }

    fn init_connection(conn: &Connection) -> Result<(), RPCErrors> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|e| RPCErrors::ReasonError(format!("pragma failed: {}", e)))?;
        Ok(())
    }

    fn create_schema(conn: &Connection) -> Result<(), RPCErrors> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                k TEXT PRIMARY KEY,
                v_int INTEGER,
                v_blob BLOB,
                v_text TEXT
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS nodes (
                inode_id INTEGER PRIMARY KEY,
                kind INTEGER NOT NULL,
                read_only INTEGER NOT NULL DEFAULT 0,
                base_obj_id BLOB,
                state INTEGER NOT NULL,
                rev INTEGER,
                meta_json TEXT,
                lease_client_session TEXT,
                lease_seq INTEGER,
                lease_expire_at INTEGER,
                fb_handle TEXT,
                last_write_at INTEGER,
                closed_at INTEGER,
                linked_obj_id BLOB,
                linked_qcid BLOB,
                linked_filebuffer_id TEXT,
                linked_at INTEGER,
                finalized_obj_id BLOB,
                finalized_at INTEGER,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_kind_state ON nodes(kind, state);
            CREATE INDEX IF NOT EXISTS idx_nodes_cooling ON nodes(state, closed_at);
            CREATE INDEX IF NOT EXISTS idx_nodes_linked ON nodes(state, linked_at);

            CREATE TABLE IF NOT EXISTS dentries (
                parent_inode_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                target_type INTEGER NOT NULL,
                target_inode_id INTEGER,
                target_obj_id BLOB,
                mtime INTEGER,
                PRIMARY KEY (parent_inode_id, name),
                CHECK (
                    (target_type = 0 AND target_inode_id IS NOT NULL AND target_obj_id IS NULL) OR
                    (target_type = 1 AND target_inode_id IS NULL AND target_obj_id IS NOT NULL) OR
                    (target_type = 2 AND target_inode_id IS NULL AND target_obj_id IS NULL) OR
                    (target_type = 3 AND target_inode_id IS NULL AND target_obj_id IS NOT NULL)
                )
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_dentries_target_inode ON dentries(target_inode_id);
            CREATE INDEX IF NOT EXISTS idx_dentries_target_obj ON dentries(target_obj_id);
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_inode_target
                ON dentries(target_inode_id)
                WHERE target_type = 0;

            CREATE TABLE IF NOT EXISTS obj_stat (
                obj_id BLOB PRIMARY KEY,
                ref_count INTEGER NOT NULL,
                zero_since INTEGER,
                updated_at INTEGER NOT NULL,
                CHECK (ref_count >= 0)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_obj_stat_gc ON obj_stat(ref_count, zero_since);",
        )
        .map_err(|e| RPCErrors::ReasonError(format!("create schema failed: {}", e)))?;
        Ok(())
    }

    fn migrate_schema(conn: &Connection) -> Result<(), RPCErrors> {
        let dentry_ddl = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'dentries'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| RPCErrors::ReasonError(format!("query schema failed: {}", e)))?;

        let Some(ddl) = dentry_ddl else {
            return Ok(());
        };

        if ddl.contains("target_type = 3 AND target_inode_id IS NULL AND target_obj_id IS NOT NULL")
        {
            return Ok(());
        }

        conn.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE dentries RENAME TO dentries_old;
             CREATE TABLE dentries (
                parent_inode_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                target_type INTEGER NOT NULL,
                target_inode_id INTEGER,
                target_obj_id BLOB,
                mtime INTEGER,
                PRIMARY KEY (parent_inode_id, name),
                CHECK (
                    (target_type = 0 AND target_inode_id IS NOT NULL AND target_obj_id IS NULL) OR
                    (target_type = 1 AND target_inode_id IS NULL AND target_obj_id IS NOT NULL) OR
                    (target_type = 2 AND target_inode_id IS NULL AND target_obj_id IS NULL) OR
                    (target_type = 3 AND target_inode_id IS NULL AND target_obj_id IS NOT NULL)
                )
             ) WITHOUT ROWID;
             INSERT INTO dentries(parent_inode_id, name, target_type, target_inode_id, target_obj_id, mtime)
                SELECT
                    parent_inode_id,
                    name,
                    target_type,
                    CASE
                        WHEN target_type = 3 THEN NULL
                        ELSE target_inode_id
                    END AS target_inode_id,
                    CASE
                        WHEN target_type = 3 AND target_obj_id IS NULL AND target_inode_id IS NOT NULL
                            THEN CAST(printf('legacy-inode:%lld', target_inode_id) AS BLOB)
                        WHEN target_type = 3
                            THEN target_obj_id
                        ELSE target_obj_id
                    END AS target_obj_id,
                    mtime
                FROM dentries_old;
             DROP TABLE dentries_old;
             CREATE INDEX IF NOT EXISTS idx_dentries_target_inode ON dentries(target_inode_id);
             CREATE INDEX IF NOT EXISTS idx_dentries_target_obj ON dentries(target_obj_id);
             CREATE UNIQUE INDEX IF NOT EXISTS uniq_inode_target
                ON dentries(target_inode_id)
                WHERE target_type = 0;
             COMMIT;",
        )
        .map_err(|e| RPCErrors::ReasonError(format!("migrate schema failed: {}", e)))?;

        Ok(())
    }

    fn ensure_root_dir(conn: &Connection) -> Result<IndexNodeId, RPCErrors> {
        let root = conn
            .query_row(
                "SELECT v_int FROM kv WHERE k = ?1",
                params![ROOT_KEY],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| RPCErrors::ReasonError(format!("query root failed: {}", e)))?;
        if let Some(id) = root {
            return Ok(id as IndexNodeId);
        }

        let now = unix_timestamp();
        conn.execute(
            "INSERT INTO nodes (
                inode_id, kind, read_only, base_obj_id, state, rev, meta_json,
                lease_client_session, lease_seq, lease_expire_at,
                fb_handle, last_write_at, closed_at,
                linked_obj_id, linked_qcid, linked_filebuffer_id, linked_at,
                finalized_obj_id, finalized_at, updated_at
            ) VALUES (?1, ?2, 0, NULL, ?3, ?4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?5)",
            params![
                1i64,
                node_kind_to_int(NodeKind::Dir),
                NODE_STATE_DIR_NORMAL,
                0i64,
                now as i64
            ],
        )
        .map_err(|e| RPCErrors::ReasonError(format!("init root node failed: {}", e)))?;

        conn.execute(
            "INSERT INTO kv (k, v_int) VALUES (?1, ?2)",
            params![ROOT_KEY, 1i64],
        )
        .map_err(|e| RPCErrors::ReasonError(format!("init root kv failed: {}", e)))?;

        Ok(1)
    }

    /// Get connection and entry for a transaction
    fn get_txn_entry(&self, txid: &str) -> Result<TxnEntry, RPCErrors> {
        let txns = self
            .txns
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?;
        let entry = txns
            .get(txid)
            .ok_or_else(|| RPCErrors::ReasonError("txid not found".to_string()))?;

        // Check if transaction is being closed
        if entry.closing.load(Ordering::SeqCst) {
            return Err(RPCErrors::ReasonError("transaction is closing".to_string()));
        }

        Ok(entry.clone())
    }

    async fn with_conn<T, F>(&self, txid: Option<&str>, f: F) -> Result<T, RPCErrors>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, RPCErrors> + Send + 'static,
    {
        if let Some(txid) = txid {
            let entry = self.get_txn_entry(txid)?;

            // Increment in-flight counter
            entry.in_flight.fetch_add(1, Ordering::SeqCst);

            // Update last_used_at
            if let Ok(mut last_used) = entry.last_used_at.lock() {
                *last_used = Instant::now();
            }

            let conn = entry.conn.clone();
            let in_flight = entry.in_flight.clone();

            let result = tokio::task::spawn_blocking(move || {
                let conn_guard = conn
                    .lock()
                    .map_err(|e| RPCErrors::ReasonError(format!("conn lock poisoned: {}", e)))?;
                let result = f(&conn_guard);
                drop(conn_guard);
                // Decrement in-flight counter
                in_flight.fetch_sub(1, Ordering::SeqCst);
                result
            })
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("db task join failed: {}", e)))?;

            result
        } else {
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                let conn_guard = conn
                    .lock()
                    .map_err(|e| RPCErrors::ReasonError(format!("conn lock poisoned: {}", e)))?;
                f(&conn_guard)
            })
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("db task join failed: {}", e)))?
        }
    }

    fn open_txn_connection(db_path: &str) -> Result<Connection, RPCErrors> {
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .map_err(|e| RPCErrors::ReasonError(format!("open txn db failed: {}", e)))?;
        Self::init_connection(&conn)?;
        // Use BEGIN IMMEDIATE to acquire write lock early, avoiding mid-transaction SQLITE_BUSY
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| RPCErrors::ReasonError(format!("begin txn failed: {}", e)))?;
        Ok(conn)
    }

    fn node_state_cols(state: &NodeState) -> NodeStateCols {
        match state {
            NodeState::DirNormal => NodeStateCols {
                state: NODE_STATE_DIR_NORMAL,
                ..NodeStateCols::default()
            },
            NodeState::DirOverlay => NodeStateCols {
                state: NODE_STATE_DIR_OVERLAY,
                ..NodeStateCols::default()
            },
            NodeState::Working(s) => NodeStateCols {
                state: NODE_STATE_WORKING,
                fb_handle: Some(s.fb_handle.clone()),
                last_write_at: Some(s.last_write_at),
                ..NodeStateCols::default()
            },
            NodeState::Cooling(s) => NodeStateCols {
                state: NODE_STATE_COOLING,
                fb_handle: Some(s.fb_handle.clone()),
                closed_at: Some(s.closed_at),
                ..NodeStateCols::default()
            },
            NodeState::Linked(s) => NodeStateCols {
                state: NODE_STATE_LINKED,
                linked_obj_id: Some(obj_id_to_blob(&s.obj_id)),
                linked_qcid: Some(obj_id_to_blob(&s.qcid)),
                linked_filebuffer_id: Some(s.filebuffer_id.clone()),
                linked_at: Some(s.linked_at),
                ..NodeStateCols::default()
            },
            NodeState::Finalized(s) => NodeStateCols {
                state: NODE_STATE_FINALIZED,
                finalized_obj_id: Some(obj_id_to_blob(&s.obj_id)),
                finalized_at: Some(s.finalized_at),
                ..NodeStateCols::default()
            },
        }
    }

    fn parse_node_state(row: &rusqlite::Row<'_>) -> Result<NodeState, RPCErrors> {
        let state = row.get::<_, i64>(4).map_err(map_db_err)?;
        match state {
            NODE_STATE_DIR_NORMAL => Ok(NodeState::DirNormal),
            NODE_STATE_DIR_OVERLAY => Ok(NodeState::DirOverlay),
            NODE_STATE_WORKING => {
                let fb_handle: String = row.get(10).map_err(map_db_err)?;
                let last_write_at: i64 = row.get(11).map_err(map_db_err)?;
                Ok(NodeState::Working(ndm::FileWorkingState {
                    fb_handle,
                    last_write_at: last_write_at as u64,
                }))
            }
            NODE_STATE_COOLING => {
                let fb_handle: String = row.get(10).map_err(map_db_err)?;
                let closed_at: i64 = row.get(12).map_err(map_db_err)?;
                Ok(NodeState::Cooling(ndm::FileCoolingState {
                    fb_handle,
                    closed_at: closed_at as u64,
                }))
            }
            NODE_STATE_LINKED => {
                let obj_blob: Vec<u8> = row.get(13).map_err(map_db_err)?;
                let qcid_blob: Vec<u8> = row.get(14).map_err(map_db_err)?;
                let filebuffer_id: String = row.get(15).map_err(map_db_err)?;
                let linked_at: i64 = row.get(16).map_err(map_db_err)?;
                Ok(NodeState::Linked(ndm::FileLinkedState {
                    obj_id: obj_id_from_blob(obj_blob)?,
                    qcid: obj_id_from_blob(qcid_blob)?,
                    filebuffer_id,
                    linked_at: linked_at as u64,
                }))
            }
            NODE_STATE_FINALIZED => {
                let obj_blob: Vec<u8> = row.get(17).map_err(map_db_err)?;
                let finalized_at: i64 = row.get(18).map_err(map_db_err)?;
                Ok(NodeState::Finalized(ndm::FinalizedObjState {
                    obj_id: obj_id_from_blob(obj_blob)?,
                    finalized_at: finalized_at as u64,
                }))
            }
            _ => Err(RPCErrors::ReasonError("invalid node state".to_string())),
        }
    }

    fn parse_node(row: &rusqlite::Row<'_>) -> Result<NodeRecord, RPCErrors> {
        let inode_id: i64 = row.get(0).map_err(map_db_err)?;
        let kind: i64 = row.get(1).map_err(map_db_err)?;
        let read_only: i64 = row.get(2).map_err(map_db_err)?;
        let base_obj_id: Option<Vec<u8>> = row.get(3).map_err(map_db_err)?;
        let rev: Option<i64> = row.get(5).map_err(map_db_err)?;
        let meta_json: Option<String> = row.get(6).map_err(map_db_err)?;
        let lease_client_session: Option<String> = row.get(7).map_err(map_db_err)?;
        let lease_seq: Option<i64> = row.get(8).map_err(map_db_err)?;
        let lease_expire_at: Option<i64> = row.get(9).map_err(map_db_err)?;

        let state = Self::parse_node_state(row)?;
        let base_obj_id = match base_obj_id {
            Some(blob) => Some(obj_id_from_blob(blob)?),
            None => None,
        };
        let meta = match meta_json {
            Some(text) => Some(
                serde_json::from_str::<Value>(&text)
                    .map_err(|e| RPCErrors::ReasonError(format!("invalid meta json: {}", e)))?,
            ),
            None => None,
        };

        Ok(NodeRecord {
            inode_id: inode_id as IndexNodeId,
            kind: node_kind_from_int(kind)?,
            read_only: read_only != 0,
            base_obj_id,
            state,
            rev: rev.map(|v| v as u64),
            meta,
            lease_client_session: lease_client_session.map(ClientSessionId),
            lease_seq: lease_seq.map(|v| v as u64),
            lease_expire_at: lease_expire_at.map(|v| v as u64),
        })
    }

    /// Internal directory creation that doesn't recursively call ensure_dir_inode.
    /// Assumes parent directory already exists.
    async fn create_dir_internal(&self, path: &NdmPath) -> Result<(), RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;

        let ctx = RPCContext::default();
        let parent_id = if parent_path.is_root() {
            self.root_inode
        } else {
            let resolved = self
                .handle_resolve_path_ex(&parent_path, 0, ctx.clone())
                .await
                .map_err(|e| RPCErrors::ReasonError(e.to_string()))?;
            match resolved {
                Some(FsMetaResolvePathResp {
                    item:
                        FsMetaResolvePathItem::Inode {
                            inode_id,
                            inode: parent_node,
                        },
                    inner_path: _,
                }) => {
                    if parent_node.kind != NodeKind::Dir {
                        return Err(RPCErrors::ReasonError(
                            "parent is not a directory".to_string(),
                        ));
                    }
                    inode_id
                }
                _ => {
                    return Err(RPCErrors::ReasonError(
                        "parent directory not found".to_string(),
                    ));
                }
            }
        };

        // Check if already exists
        let existing = self
            .handle_get_dentry(parent_id, name.clone(), None, ctx.clone())
            .await?;

        if let Some(d) = existing {
            if !matches!(d.target, DentryTarget::Tombstone) {
                // Already exists, nothing to do
                return Ok(());
            }
        }

        // Create directory inode
        let txid = self.handle_begin_txn(ctx.clone()).await?;

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
            .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
            .await?;

        // Link dentry via upsert_dentry
        self.handle_upsert_dentry(
            parent_id,
            name.clone(),
            DentryTarget::IndexNodeId(new_id),
            Some(txid.clone()),
            ctx.clone(),
        )
        .await?;

        self.handle_commit(Some(txid), ctx).await?;

        Ok(())
    }

    fn join_child_path(parent: &NdmPath, name: &str) -> NdmPath {
        let parent_str = parent.as_str().trim_end_matches('/');
        if parent_str.is_empty() || parent_str == "/" {
            NdmPath::new(format!("/{}", name))
        } else {
            NdmPath::new(format!("{}/{}", parent_str, name))
        }
    }

    async fn resolve_symlink_child_as_obj(
        &self,
        child_path: &NdmPath,
        ctx: RPCContext,
    ) -> Result<Option<ObjId>, RPCErrors> {
        let resolved = self
            .handle_resolve_path_ex(child_path, DEFAULT_SYMLINK_COUNT, ctx)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("resolve symlink child failed: {}", e)))?;

        let Some(resp) = resolved else {
            return Ok(None);
        };

        if resp.inner_path.is_some() {
            return Ok(None);
        }

        match resp.item {
            FsMetaResolvePathItem::ObjId(obj_id) => {
                warn!(
                    "finalize_dir: symlink child {} resolved to ObjId={}, this may be surprising to users",
                    child_path.as_str(),
                    obj_id
                );
                Ok(Some(obj_id))
            }
            _ => Ok(None),
        }
    }

    fn enqueue_finalize_tasks(&self, pending_paths: Vec<String>) -> Result<(), RPCErrors> {
        let mut mgr = self
            .background_mgr
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("background mgr lock poisoned: {}", e)))?;
        for path in pending_paths {
            mgr.add_finalize_dir_task(path);
        }
        Ok(())
    }

    //如果finalize_dir成功，则返回true,如果dir不满足finalzie条件，需要先finalzie child,返回false
    async fn try_finalize_dir(&self, dir_path: &NdmPath) -> Result<bool, RPCErrors> {
        /*
        finalize_dir是系统减少元数据的重要方法。
        检查children是不是都finalize了，如果都finalize了，可以立刻完成并返回true
        - 计算DirObject，并设置
        - 删除目录的iNode，
        - 更新DentryItem指向的DirObject?
        - 删除chilren DentryItem和children iNodes
          inode一般很少删除，这里要判断风险：我们的系统没有hardlink,按道理是严格树结构的（一个inode只会被一个dentryItem引用）

        如果child没有finalize,则把children放入”待处理队列，处理类型是finalize"
        最后把当前目录放入”待处理队列“
        等待后台管理器处理finalzie
         */
        if dir_path.is_root() {
            return Err(RPCErrors::ReasonError(
                "finalize root directory is not supported".to_string(),
            ));
        }

        let store_mgr = self.store_mgr.as_ref().ok_or_else(|| {
            RPCErrors::ReasonError("named store manager not configured".to_string())
        })?;
        let ctx = RPCContext::default();

        let (parent_path, dir_name) = dir_path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;
        let parent_id = self.ensure_dir_inode(&parent_path).await?;
        let parent_node = self
            .handle_get_inode(parent_id, None, ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("parent directory not found".to_string()))?;
        if parent_node.kind != NodeKind::Dir {
            return Err(RPCErrors::ReasonError(
                "parent is not a directory".to_string(),
            ));
        }
        let parent_rev0 = parent_node.rev.unwrap_or(0);

        let dir_dentry = self
            .handle_get_dentry(parent_id, dir_name.clone(), None, ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("directory not found".to_string()))?;

        let dir_inode_id = match dir_dentry.target {
            DentryTarget::ObjId(obj_id) => {
                if obj_id.obj_type != OBJ_TYPE_DIR {
                    return Err(RPCErrors::ReasonError(
                        "path is not a directory".to_string(),
                    ));
                }
                return Ok(true);
            }
            DentryTarget::IndexNodeId(id) => id,
            DentryTarget::SymLink(_) => {
                return Err(RPCErrors::ReasonError(
                    "path is a symbolic link".to_string(),
                ))
            }
            DentryTarget::Tombstone => {
                return Err(RPCErrors::ReasonError("directory not found".to_string()))
            }
        };

        let dir_node = self
            .handle_get_inode(dir_inode_id, None, ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("directory inode not found".to_string()))?;
        if dir_node.kind != NodeKind::Dir {
            return Err(RPCErrors::ReasonError(
                "path is not a directory".to_string(),
            ));
        }
        let dir_rev0 = dir_node.rev.unwrap_or(0);

        let mut merged_children: BTreeMap<String, ObjId> = BTreeMap::new();
        if let Some(base_obj_id) = dir_node.base_obj_id.as_ref() {
            if base_obj_id.obj_type != OBJ_TYPE_DIR {
                return Err(RPCErrors::ReasonError(format!(
                    "invalid base_obj_id type for directory inode: {}",
                    base_obj_id.obj_type
                )));
            }
            let base_obj_str = store_mgr.get_object(base_obj_id).await.map_err(|e| {
                RPCErrors::ReasonError(format!("failed to load base DirObject: {}", e))
            })?;
            let base_dir: DirObject = load_named_obj(base_obj_str.as_str()).map_err(|e| {
                RPCErrors::ReasonError(format!("failed to parse base DirObject: {}", e))
            })?;
            for (base_name, item) in base_dir.iter() {
                let (obj_id, obj_str) = item
                    .get_obj_id()
                    .map_err(|e| RPCErrors::ReasonError(format!("invalid base child: {}", e)))?;
                if !obj_str.is_empty() {
                    store_mgr
                        .put_object(&obj_id, obj_str.as_str())
                        .await
                        .map_err(|e| {
                            RPCErrors::ReasonError(format!(
                                "failed to cache embedded base child object: {}",
                                e
                            ))
                        })?;
                }
                merged_children.insert(base_name.clone(), obj_id);
            }
        }

        let upper_children = self
            .handle_list_dentries(dir_inode_id, None, ctx.clone())
            .await?;
        let mut pending_finalize_paths: Vec<String> = Vec::new();
        for child in upper_children.iter() {
            match &child.target {
                DentryTarget::Tombstone => {
                    merged_children.remove(&child.name);
                }
                DentryTarget::ObjId(obj_id) => {
                    merged_children.insert(child.name.clone(), obj_id.clone());
                }
                DentryTarget::IndexNodeId(child_inode_id) => {
                    let child_inode = self
                        .handle_get_inode(*child_inode_id, None, ctx.clone())
                        .await?
                        .ok_or_else(|| {
                            RPCErrors::ReasonError(format!(
                                "child inode {} not found",
                                child_inode_id
                            ))
                        })?;
                    match child_inode.kind {
                        NodeKind::Dir => {
                            let child_path = Self::join_child_path(dir_path, &child.name);
                            pending_finalize_paths.push(child_path.as_str().to_string());
                        }
                        NodeKind::File | NodeKind::Object => match child_inode.state {
                            NodeState::Finalized(ref fs) => {
                                merged_children.insert(child.name.clone(), fs.obj_id.clone());
                            }
                            _ => {
                                let child_path = Self::join_child_path(dir_path, &child.name);
                                pending_finalize_paths.push(child_path.as_str().to_string());
                            }
                        },
                    }
                }
                DentryTarget::SymLink(_) => {
                    //TODO：应该把SymbLink当成一个纯粹的DirItem来看
                    let child_path = Self::join_child_path(dir_path, &child.name);
                    if let Some(obj_id) = self
                        .resolve_symlink_child_as_obj(&child_path, ctx.clone())
                        .await?
                    {
                        merged_children.insert(child.name.clone(), obj_id);
                    } else {
                        pending_finalize_paths.push(child_path.as_str().to_string());
                    }
                }
            }
        }

        if !pending_finalize_paths.is_empty() {
            let mut unique = HashSet::new();
            pending_finalize_paths.retain(|p| unique.insert(p.clone()));
            pending_finalize_paths.push(dir_path.as_str().to_string());
            self.enqueue_finalize_tasks(pending_finalize_paths)?;
            return Ok(false);
        }

        let mut new_dir_obj = DirObject::new(Some(dir_name.clone()));
        for (child_name, child_obj_id) in merged_children.iter() {
            new_dir_obj.object_map.insert(
                child_name.clone(),
                SimpleMapItem::ObjId(child_obj_id.clone()),
            );
        }
        let (new_dir_obj_id, new_dir_obj_str) = new_dir_obj
            .gen_obj_id()
            .map_err(|e| RPCErrors::ReasonError(format!("build dir object failed: {}", e)))?;
        store_mgr
            .put_object(&new_dir_obj_id, new_dir_obj_str.as_str())
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("store dir object failed: {}", e)))?;

        let txid = self.handle_begin_txn(ctx.clone()).await?;
        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_now = match self
            .handle_get_inode(parent_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "parent inode missing during finalize".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if parent_now.rev.unwrap_or(0) != parent_rev0 {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent rev changed during finalize".to_string()
            ));
        }

        let current_dentry = match self
            .handle_get_dentry(parent_id, dir_name.clone(), Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(d)) => d,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "directory dentry missing during finalize".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        match current_dentry.target {
            DentryTarget::ObjId(obj_id) => {
                if obj_id.obj_type == OBJ_TYPE_DIR {
                    let _ = self.handle_rollback(Some(txid), ctx).await;
                    return Ok(true);
                }
                rollback_and_err!(RPCErrors::ReasonError(
                    "path is not a directory".to_string()
                ));
            }
            DentryTarget::IndexNodeId(id) => {
                if id != dir_inode_id {
                    rollback_and_err!(RPCErrors::ReasonError(
                        "directory target changed during finalize".to_string()
                    ));
                }
            }
            DentryTarget::SymLink(_) | DentryTarget::Tombstone => rollback_and_err!(
                RPCErrors::ReasonError("directory target changed during finalize".to_string())
            ),
        }

        let dir_now = match self
            .handle_get_inode(dir_inode_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "directory inode missing during finalize".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if dir_now.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "path is not a directory".to_string()
            ));
        }
        if dir_now.rev.unwrap_or(0) != dir_rev0 {
            rollback_and_err!(RPCErrors::ReasonError(
                "directory rev changed during finalize".to_string()
            ));
        }

        if let Err(e) = self
            .handle_upsert_dentry(
                parent_id,
                dir_name,
                DentryTarget::ObjId(new_dir_obj_id),
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        let children_in_tx = match self
            .handle_list_dentries(dir_inode_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(items) => items,
            Err(e) => rollback_and_err!(e),
        };
        for child in children_in_tx {
            if let DentryTarget::IndexNodeId(child_inode_id) = child.target {
                if let Err(e) = self
                    .handle_remove_inode(child_inode_id, Some(txid.clone()), ctx.clone())
                    .await
                {
                    rollback_and_err!(e);
                }
            }
            if let Err(e) = self
                .handle_remove_dentry_row(dir_inode_id, child.name, Some(txid.clone()), ctx.clone())
                .await
            {
                rollback_and_err!(e);
            }
        }

        if let Err(e) = self
            .handle_remove_inode(dir_inode_id, Some(txid.clone()), ctx.clone())
            .await
        {
            rollback_and_err!(e);
        }

        self.handle_commit(Some(txid), ctx).await?;
        Ok(true)
    }

    /// Materialize a directory from a DirObject.
    /// Creates a new inode with base_obj_id pointing to the DirObject,
    /// and updates the dentry to point to the new inode.
    async fn materialize_dir_from_obj(
        &self,
        parent_id: IndexNodeId,
        name: String,
        dir_obj_id: &ObjId,
    ) -> Result<IndexNodeId, RPCErrors> {
        let ctx = RPCContext::default();
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        // Helper macro for rollback on error (similar to open_file_writer)
        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        // Create new directory inode with base_obj_id pointing to the DirObject
        let new_node = NodeRecord {
            inode_id: 0,
            kind: NodeKind::Dir,
            read_only: false,
            base_obj_id: Some(dir_obj_id.clone()),
            state: NodeState::DirOverlay, // Overlay mode: upper layer changes on top of base DirObject
            rev: Some(0),
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        let new_id = match self
            .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(id) => id,
            Err(e) => {
                rollback_and_err!(RPCErrors::ReasonError(format!(
                    "failed to alloc inode: {}",
                    e
                )));
            }
        };

        // Update dentry to point to the new inode
        if let Err(e) = self
            .handle_upsert_dentry(
                parent_id,
                name,
                DentryTarget::IndexNodeId(new_id),
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(RPCErrors::ReasonError(format!(
                "failed to upsert dentry: {}",
                e
            )));
        }

        self.handle_commit(Some(txid), ctx)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("failed to commit: {}", e)))?;

        Ok(new_id)
    }

    async fn create_dir_under_parent(
        &self,
        parent_id: IndexNodeId,
        name: &str,
        ctx: RPCContext,
    ) -> Result<IndexNodeId, RPCErrors> {
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let existing = self
            .handle_get_dentry(parent_id, name.to_string(), Some(txid.clone()), ctx.clone())
            .await?;
        if let Some(d) = existing {
            match d.target {
                DentryTarget::IndexNodeId(id) => {
                    let node = self
                        .handle_get_inode(id, Some(txid.clone()), ctx.clone())
                        .await?
                        .ok_or_else(|| {
                            RPCErrors::ReasonError("existing inode not found".to_string())
                        })?;
                    if node.kind != NodeKind::Dir {
                        rollback_and_err!(RPCErrors::ReasonError(format!(
                            "{} exists and is not a directory",
                            name
                        )));
                    }
                    self.handle_commit(Some(txid), ctx).await?;
                    return Ok(id);
                }
                DentryTarget::Tombstone => {}
                DentryTarget::ObjId(_) | DentryTarget::SymLink(_) => {
                    rollback_and_err!(RPCErrors::ReasonError(format!(
                        "{} exists and is not a directory",
                        name
                    )));
                }
            }
        }

        let node = NodeRecord {
            inode_id: 0,
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
            .handle_alloc_inode(node, Some(txid.clone()), ctx.clone())
            .await?;
        self.handle_upsert_dentry(
            parent_id,
            name.to_string(),
            DentryTarget::IndexNodeId(new_id),
            Some(txid.clone()),
            ctx.clone(),
        )
        .await?;
        self.handle_commit(Some(txid), ctx).await?;
        Ok(new_id)
    }

    async fn lookup_base_child(
        &self,
        dir_node: &NodeRecord,
        name: &str,
    ) -> Result<BaseChildLookup, RPCErrors> {
        let Some(base_obj_id) = dir_node.base_obj_id.as_ref() else {
            return Ok(BaseChildLookup::Missing);
        };
        if base_obj_id.obj_type != OBJ_TYPE_DIR {
            return Err(RPCErrors::ReasonError(format!(
                "invalid base_obj_id type for directory inode: {}",
                base_obj_id.obj_type
            )));
        }

        let store_mgr = self.store_mgr.as_ref().ok_or_else(|| {
            RPCErrors::ReasonError(
                "named store manager not configured for DirObject traversal".to_string(),
            )
        })?;

        let base_obj_str = store_mgr
            .get_object(base_obj_id)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("failed to load base DirObject: {}", e)))?;
        let base_dir: DirObject = load_named_obj(base_obj_str.as_str()).map_err(|e| {
            RPCErrors::ReasonError(format!("failed to parse base DirObject: {}", e))
        })?;

        let Some(item) = base_dir.get(name) else {
            return Ok(BaseChildLookup::Missing);
        };

        let (child_obj_id, child_obj_str) = item
            .get_obj_id()
            .map_err(|e| RPCErrors::ReasonError(format!("invalid base child entry: {}", e)))?;

        // Persist embedded object forms (Object/ObjectJwt) so later lookups by obj_id can reuse store APIs.
        if !child_obj_str.is_empty() {
            store_mgr
                .put_object(&child_obj_id, child_obj_str.as_str())
                .await
                .map_err(|e| {
                    RPCErrors::ReasonError(format!("failed to cache embedded child object: {}", e))
                })?;
        }

        if child_obj_id.obj_type == OBJ_TYPE_DIR {
            Ok(BaseChildLookup::DirObj(child_obj_id))
        } else {
            Ok(BaseChildLookup::NonDirObj(child_obj_id))
        }
    }

    async fn load_base_dir_children(
        &self,
        dir_node: &NodeRecord,
    ) -> Result<BTreeMap<String, ObjId>, RPCErrors> {
        let mut out = BTreeMap::new();
        let Some(base_obj_id) = dir_node.base_obj_id.as_ref() else {
            return Ok(out);
        };
        if base_obj_id.obj_type != OBJ_TYPE_DIR {
            return Err(RPCErrors::ReasonError(format!(
                "invalid base_obj_id type for directory inode: {}",
                base_obj_id.obj_type
            )));
        }

        let store_mgr = self.store_mgr.as_ref().ok_or_else(|| {
            RPCErrors::ReasonError(
                "named store manager not configured for DirObject traversal".to_string(),
            )
        })?;
        let base_obj_str = store_mgr
            .get_object(base_obj_id)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("failed to load base DirObject: {}", e)))?;
        let base_dir: DirObject = load_named_obj(base_obj_str.as_str())
            .map_err(|e| RPCErrors::ReasonError(format!("failed to parse base DirObject: {}", e)))?;
        for (name, item) in base_dir.iter() {
            let (obj_id, obj_str) = item
                .get_obj_id()
                .map_err(|e| RPCErrors::ReasonError(format!("invalid base child entry: {}", e)))?;
            if !obj_str.is_empty() {
                store_mgr
                    .put_object(&obj_id, obj_str.as_str())
                    .await
                    .map_err(|e| {
                        RPCErrors::ReasonError(format!(
                            "failed to cache embedded child object: {}",
                            e
                        ))
                    })?;
            }
            out.insert(name.clone(), obj_id);
        }
        Ok(out)
    }

    async fn list_merged_dentries(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<Vec<DentryRecord>, RPCErrors> {
        let parent_node = self
            .handle_get_inode(parent, txid.clone(), ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("parent inode not found".to_string()))?;
        if parent_node.kind != NodeKind::Dir {
            return Err(RPCErrors::ReasonError(
                "parent is not a directory".to_string(),
            ));
        }

        let mut merged = BTreeMap::<String, DentryRecord>::new();
        for (name, obj_id) in self.load_base_dir_children(&parent_node).await? {
            merged.insert(
                name.clone(),
                DentryRecord {
                    parent,
                    name,
                    target: DentryTarget::ObjId(obj_id),
                    mtime: None,
                },
            );
        }

        for d in self
            .handle_list_dentries(parent, txid.clone(), ctx.clone())
            .await?
        {
            match d.target {
                DentryTarget::Tombstone => {
                    merged.remove(&d.name);
                }
                _ => {
                    merged.insert(d.name.clone(), d);
                }
            }
        }

        Ok(merged.into_values().collect())
    }

    async fn ensure_name_absent_for_create(
        &self,
        parent_id: IndexNodeId,
        parent_node: &NodeRecord,
        name: &str,
        path: &NdmPath,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let upper = self
            .handle_get_dentry(parent_id, name.to_string(), txid, ctx.clone())
            .await?;

        match upper {
            Some(DentryRecord {
                target: DentryTarget::Tombstone,
                ..
            }) => {
                // Tombstone in upper hides base item, so it's safe to create.
                return Ok(());
            }
            Some(_) => {
                return Err(RPCErrors::ReasonError(format!(
                    "path {} already exists",
                    path.as_str()
                )));
            }
            None => {}
        }

        match self.lookup_base_child(parent_node, name).await? {
            BaseChildLookup::Missing => Ok(()),
            BaseChildLookup::DirObj(_) | BaseChildLookup::NonDirObj(_) => Err(
                RPCErrors::ReasonError(format!("path {} already exists", path.as_str())),
            ),
        }
    }

    /// Load existing file chunks from base object for append operations.
    /// Note: This is a simplified implementation that returns empty chunks.
    /// For full append support with existing file content, the store would need to be accessed.
    async fn load_file_chunklist(&self, _obj_id: &ObjId) -> Result<Vec<ChunkId>, RPCErrors> {
        // TODO: To fully support appending to existing files with content,
        // we would need access to the object store to read the file object's chunk list.
        // For now, we return an empty list, which means append operations will start fresh.
        Ok(Vec::new())
    }

    async fn check_dest_conflict_pre(
        &self,
        dst_parent: IndexNodeId,
        dst_name: &str,
        dst_dir_node: &NodeRecord,
        opts: MoveOptions,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        // Check upper layer
        let dentry = self
            .handle_get_dentry(dst_parent, dst_name.to_string(), None, ctx)
            .await?;

        if let Some(d) = dentry {
            match d.target {
                DentryTarget::Tombstone => { /* treat as not-exists */ }
                DentryTarget::IndexNodeId(_)
                | DentryTarget::SymLink(_)
                | DentryTarget::ObjId(_) => {
                    if !opts.overwrite_upper {
                        return Err(RPCErrors::ReasonError("already exists".to_string()));
                    }
                }
            }
        }

        if !opts.strict_check_base {
            return Ok(());
        }

        // Check base if strict mode
        if dst_dir_node.base_obj_id.is_some() {
            // TODO: check children in base DirObject if store access is available
        }

        Ok(())
    }
    /// Ensure directory inode exists at path, creating parent directories as needed.
    /// Uses iterative approach to avoid async recursion.
    ///
    /// This method handles the case where the path passes through a DirObject:
    /// - When a dentry points to ObjId (DirObject) instead of IndexNodeId
    /// - It materializes the directory by creating inode with base_obj_id pointing to the DirObject
    async fn ensure_dir_inode(&self, path: &NdmPath) -> Result<IndexNodeId, RPCErrors> {
        if path.is_root() {
            return Ok(self.root_inode);
        }

        let ctx = RPCContext::default();

        // Try to resolve existing path first
        match self
            .handle_resolve_path_ex(path, DEFAULT_SYMLINK_COUNT, ctx.clone())
            .await
            .map_err(|e| RPCErrors::ReasonError(e.to_string()))?
        {
            Some(FsMetaResolvePathResp {
                item:
                    FsMetaResolvePathItem::Inode {
                        inode_id: id,
                        inode: node,
                    },
                inner_path: _,
            }) => {
                if node.kind != NodeKind::Dir {
                    return Err(RPCErrors::ReasonError(format!(
                        "{} is not a directory",
                        path.as_str()
                    )));
                }
                return Ok(id);
            }
            Some(FsMetaResolvePathResp {
                item: FsMetaResolvePathItem::SymLink(_),
                inner_path: _,
            }) => {
                return Err(RPCErrors::ReasonError(format!(
                    "{} is not a directory",
                    path.as_str()
                )));
            }
            Some(FsMetaResolvePathResp {
                item: FsMetaResolvePathItem::ObjId(obj_id),
                inner_path: _,
            }) => {
                if obj_id.obj_type != OBJ_TYPE_DIR {
                    return Err(RPCErrors::ReasonError(format!(
                        "{} is not a directory",
                        path.as_str()
                    )));
                }
                // DirObject path exists but may need materialization; continue walking below.
            }
            None => {
                // Path doesn't exist, will create it below
            }
        }

        // Walk through path components:
        // 1) honor upper dentries first
        // 2) if upper miss and parent has base DirObject, consult base child
        // 3) materialize DirObject child as Overlay inode when needed
        // 4) otherwise create directory inode (mkdir -p semantics)
        let components = path.components();
        let mut current_id = self.root_inode;

        for (i, component) in components.iter().enumerate() {
            let dentry = self
                .handle_get_dentry(current_id, component.to_string(), None, ctx.clone())
                .await?;

            let next_id = match dentry {
                Some(d) => match d.target {
                    DentryTarget::IndexNodeId(id) => {
                        let node = self
                            .handle_get_inode(id, None, ctx.clone())
                            .await?
                            .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;

                        if node.kind != NodeKind::Dir {
                            return Err(RPCErrors::ReasonError(format!(
                                "{} is not a directory",
                                components[..=i].join("/")
                            )));
                        }
                        id
                    }
                    DentryTarget::ObjId(obj_id) => {
                        if obj_id.obj_type != OBJ_TYPE_DIR {
                            return Err(RPCErrors::ReasonError(format!(
                                "path component {} points to non-directory object",
                                component
                            )));
                        }

                        self.materialize_dir_from_obj(current_id, component.to_string(), &obj_id)
                            .await?
                    }
                    DentryTarget::SymLink(_) => {
                        return Err(RPCErrors::ReasonError(format!(
                            "{} is not a directory",
                            components[..=i].join("/")
                        )));
                    }
                    DentryTarget::Tombstone => {
                        self.create_dir_under_parent(current_id, component, ctx.clone())
                            .await?
                    }
                },
                None => {
                    let parent_node = self
                        .handle_get_inode(current_id, None, ctx.clone())
                        .await?
                        .ok_or_else(|| {
                            RPCErrors::ReasonError("parent inode not found".to_string())
                        })?;
                    if parent_node.kind != NodeKind::Dir {
                        return Err(RPCErrors::ReasonError(format!(
                            "{} is not a directory",
                            components[..i].join("/")
                        )));
                    }

                    match self.lookup_base_child(&parent_node, component).await? {
                        BaseChildLookup::DirObj(child_dir_obj) => {
                            self.materialize_dir_from_obj(
                                current_id,
                                component.to_string(),
                                &child_dir_obj,
                            )
                            .await?
                        }
                        BaseChildLookup::NonDirObj(_) => {
                            return Err(RPCErrors::ReasonError(format!(
                                "{} is not a directory",
                                components[..=i].join("/")
                            )));
                        }
                        BaseChildLookup::Missing => {
                            self.create_dir_under_parent(current_id, component, ctx.clone())
                                .await?
                        }
                    }
                }
            };

            current_id = next_id;
        }

        Ok(current_id)
    }

    fn is_dir_like(src: &MoveSource) -> bool {
        match src {
            MoveSource::Upper { kind_hint, .. } => *kind_hint == ObjectKind::Dir,
            MoveSource::Base { kind, .. } => *kind == ObjectKind::Dir,
        }
    }

    async fn apply_move_plan_txn(
        &self,
        plan: MovePlan,
        _opts: MoveOptions,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        // Verify revisions haven't changed (OCC)
        let src_dir = self
            .handle_get_inode(plan.src_parent, Some(txid.clone()), ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;

        let dst_dir = self
            .handle_get_inode(plan.dst_parent, Some(txid.clone()), ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;

        if src_dir.rev.unwrap_or(0) != plan.src_rev0 || dst_dir.rev.unwrap_or(0) != plan.dst_rev0 {
            let _ = self.handle_rollback(Some(txid), ctx.clone()).await;
            return Err(RPCErrors::ReasonError("conflict".to_string()));
        }

        // Set tombstone at source
        self.handle_set_tombstone(
            plan.src_parent,
            plan.src_name,
            Some(txid.clone()),
            ctx.clone(),
        )
        .await?;

        // Upsert dentry at destination
        let target = match plan.source {
            MoveSource::Upper { target, .. } => target,
            MoveSource::Base { obj_id, .. } => DentryTarget::ObjId(obj_id),
        };

        self.handle_upsert_dentry(
            plan.dst_parent,
            plan.dst_name,
            target,
            Some(txid.clone()),
            ctx.clone(),
        )
        .await?;

        self.handle_commit(Some(txid), ctx).await?;

        Ok(())
    }

    async fn plan_move_source(
        &self,
        src_parent: IndexNodeId,
        src_name: &str,
        src_dir_node: &NodeRecord,
        src_rev0: u64,
        ctx: RPCContext,
    ) -> Result<MoveSource, RPCErrors> {
        // Check upper first
        let dentry = self
            .handle_get_dentry(src_parent, src_name.to_string(), None, ctx.clone())
            .await?;

        if let Some(d) = dentry {
            return match d.target {
                DentryTarget::Tombstone => Err(RPCErrors::ReasonError("not found".to_string())),
                DentryTarget::IndexNodeId(fid) => {
                    let inode = self
                        .handle_get_inode(fid, None, ctx.clone())
                        .await?
                        .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;
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
                DentryTarget::SymLink(target_path) => Ok(MoveSource::Upper {
                    target: DentryTarget::SymLink(target_path),
                    kind_hint: ObjectKind::Unknown,
                }),
                DentryTarget::ObjId(oid) => Ok(MoveSource::Upper {
                    target: DentryTarget::ObjId(oid),
                    kind_hint: ObjectKind::Unknown,
                }),
            };
        }

        // Upper miss => check base
        let base0 = src_dir_node.base_obj_id.clone();
        let _base_oid = base0
            .clone()
            .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;

        // TODO: look up child in base DirObject
        // For now, return NotFound
        Err(RPCErrors::ReasonError("not found".to_string()))
    }

    async fn acquire_file_lease_inner(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        ttl: Duration,
        txid: Option<String>,
    ) -> Result<u64, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let now = unix_timestamp() as i64;
            let expire_at = now.saturating_add(ttl.as_secs() as i64);

            let renewed = conn
                .execute(
                    "UPDATE nodes SET lease_expire_at = ?1 
                     WHERE inode_id = ?2 
                       AND lease_client_session = ?3 
                       AND lease_expire_at > ?4",
                    params![expire_at, node_id as i64, &session.0, now],
                )
                .map_err(map_db_err)?;

            if renewed > 0 {
                let seq: i64 = conn
                    .query_row(
                        "SELECT COALESCE(lease_seq, 0) FROM nodes WHERE inode_id = ?1",
                        params![node_id as i64],
                        |row| row.get(0),
                    )
                    .map_err(map_db_err)?;
                return Ok(seq as u64);
            }

            let acquired = conn
                .execute(
                    "UPDATE nodes SET 
                        lease_client_session = ?1, 
                        lease_seq = COALESCE(lease_seq, 0) + 1, 
                        lease_expire_at = ?2
                     WHERE inode_id = ?3 
                       AND (lease_expire_at IS NULL OR lease_expire_at <= ?4)",
                    params![&session.0, expire_at, node_id as i64, now],
                )
                .map_err(map_db_err)?;

            if acquired > 0 {
                let seq: i64 = conn
                    .query_row(
                        "SELECT lease_seq FROM nodes WHERE inode_id = ?1",
                        params![node_id as i64],
                        |row| row.get(0),
                    )
                    .map_err(map_db_err)?;
                return Ok(seq as u64);
            }

            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM nodes WHERE inode_id = ?1",
                    params![node_id as i64],
                    |_| Ok(true),
                )
                .optional()
                .map_err(map_db_err)?
                .unwrap_or(false);

            if !exists {
                return Err(RPCErrors::ReasonError("inode not found".to_string()));
            }

            Err(RPCErrors::ReasonError("lease conflict".to_string()))
        })
        .await
    }

    async fn renew_file_lease_inner(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        ttl: Duration,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let now = unix_timestamp() as i64;
            let expire_at = now.saturating_add(ttl.as_secs() as i64);

            let updated = conn
                .execute(
                    "UPDATE nodes SET lease_expire_at = ?1 
                     WHERE inode_id = ?2 
                       AND lease_client_session = ?3 
                       AND lease_seq = ?4",
                    params![expire_at, node_id as i64, &session.0, lease_seq as i64],
                )
                .map_err(map_db_err)?;

            if updated == 0 {
                let exists: bool = conn
                    .query_row(
                        "SELECT 1 FROM nodes WHERE inode_id = ?1",
                        params![node_id as i64],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(map_db_err)?
                    .unwrap_or(false);

                if !exists {
                    return Err(RPCErrors::ReasonError("inode not found".to_string()));
                }
                return Err(RPCErrors::ReasonError("lease mismatch".to_string()));
            }
            Ok(())
        })
        .await
    }

    async fn release_file_lease_inner(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        txid: Option<String>,
    ) -> Result<(), RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let updated = conn
                .execute(
                    "UPDATE nodes SET 
                        lease_client_session = NULL, 
                        lease_expire_at = NULL
                     WHERE inode_id = ?1 
                       AND lease_client_session = ?2 
                       AND lease_seq = ?3",
                    params![node_id as i64, &session.0, lease_seq as i64],
                )
                .map_err(map_db_err)?;

            if updated == 0 {
                let exists: bool = conn
                    .query_row(
                        "SELECT 1 FROM nodes WHERE inode_id = ?1",
                        params![node_id as i64],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(map_db_err)?
                    .unwrap_or(false);

                if !exists {
                    return Err(RPCErrors::ReasonError("inode not found".to_string()));
                }
                return Err(RPCErrors::ReasonError("lease mismatch".to_string()));
            }
            Ok(())
        })
        .await
    }

    async fn handle_remove_inode(
        &self,
        id: IndexNodeId,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        if id == self.root_inode {
            return Err(RPCErrors::ReasonError(
                "cannot remove root inode".to_string(),
            ));
        }

        self.with_conn(txid.as_deref(), move |conn| {
            let removed = conn
                .execute("DELETE FROM nodes WHERE inode_id = ?1", params![id as i64])
                .map_err(map_db_err)?;
            if removed == 0 {
                return Err(RPCErrors::ReasonError("inode not found".to_string()));
            }
            Ok(())
        })
        .await
    }
}

impl Drop for FSMetaService {
    fn drop(&mut self) {
        if let Some(handle) = self.txn_cleanup_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.lease_cleanup_handle.take() {
            handle.abort();
        }
    }
}

#[async_trait::async_trait]
impl ndm::FsMetaHandler for FSMetaService {
    async fn handle_root_dir(&self, _ctx: RPCContext) -> Result<IndexNodeId, RPCErrors> {
        Ok(self.root_inode)
    }

    async fn handle_resolve_path_ex(
        &self,
        path: &NdmPath,
        mut sym_count: u32,
        ctx: RPCContext,
    ) -> NdnResult<Option<FsMetaResolvePathResp>> {
        let root_id = self
            .handle_root_dir(ctx.clone())
            .await
            .map_err(|e| NdnError::Internal(format!("failed to get root dir: {}", e)))?;

        let mut components: Vec<String> = path
            .components()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        loop {
            if components.is_empty() {
                let node = self
                    .handle_get_inode(root_id, None, ctx.clone())
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get root inode: {}", e)))?;
                return Ok(node.map(|inode| FsMetaResolvePathResp {
                    item: FsMetaResolvePathItem::Inode {
                        inode_id: root_id,
                        inode,
                    },
                    inner_path: None,
                }));
            }

            let comp_refs: Vec<&str> = components.iter().map(|s| s.as_str()).collect();
            if let Some(hit) = self
                .resolve_path_cache
                .write()
                .ok()
                .and_then(|mut cache| cache.get_terminal_for_path(&comp_refs))
            {
                let tail = components[hit.matched_len..].to_vec();
                match hit.value {
                    PathResolveTerminalValue::ObjId(obj_id) => {
                        return Ok(Some(FsMetaResolvePathResp {
                            item: FsMetaResolvePathItem::ObjId(obj_id),
                            inner_path: join_components(&tail),
                        }));
                    }
                    PathResolveTerminalValue::SymLink(target_path) => {
                        if sym_count == 0 {
                            return Ok(Some(FsMetaResolvePathResp {
                                item: FsMetaResolvePathItem::SymLink(target_path),
                                inner_path: join_components(&tail),
                            }));
                        }

                        sym_count -= 1;
                        let parent_depth = hit.matched_len.saturating_sub(1);
                        let parent_prefix = components[..parent_depth].to_vec();
                        components =
                            normalize_symlink_target_path(&parent_prefix, &target_path, &tail);
                        continue;
                    }
                }
            }

            let cached_prefix_ids = self
                .resolve_path_cache
                .write()
                .ok()
                .and_then(|mut cache| cache.get_longest_prefix_ids_for_path(&comp_refs));

            let (mut ids, mut current_id, start_idx) = if let Some(prefix_ids) = cached_prefix_ids {
                let matched_len = prefix_ids.len().saturating_sub(1);
                let inode_id = *prefix_ids.last().unwrap_or(&root_id);
                (prefix_ids, inode_id, matched_len)
            } else {
                (vec![root_id], root_id, 0usize)
            };

            if start_idx == components.len() {
                let node = self
                    .handle_get_inode(current_id, None, ctx.clone())
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get inode: {}", e)))?;
                return Ok(node.map(|inode| FsMetaResolvePathResp {
                    item: FsMetaResolvePathItem::Inode {
                        inode_id: current_id,
                        inode,
                    },
                    inner_path: None,
                }));
            }

            let mut restart_with: Option<Vec<String>> = None;
            for i in start_idx..components.len() {
                let name = components[i].clone();
                let is_last = i == components.len() - 1;

                let target = match self
                    .handle_get_dentry(current_id, name.clone(), None, ctx.clone())
                    .await
                    .map_err(|e| NdnError::Internal(format!("failed to get dentry: {}", e)))?
                {
                    Some(dentry) => dentry.target,
                    None => {
                        let parent_node = self
                            .handle_get_inode(current_id, None, ctx.clone())
                            .await
                            .map_err(|e| {
                                NdnError::Internal(format!("failed to get inode: {}", e))
                            })?
                            .ok_or_else(|| {
                                NdnError::Internal("parent inode not found".to_string())
                            })?;
                        if parent_node.kind != NodeKind::Dir {
                            return Ok(None);
                        }

                        match self.lookup_base_child(&parent_node, &name).await.map_err(|e| {
                            NdnError::Internal(format!("failed to lookup base child: {}", e))
                        })? {
                            BaseChildLookup::Missing => return Ok(None),
                            BaseChildLookup::DirObj(obj_id)
                            | BaseChildLookup::NonDirObj(obj_id) => DentryTarget::ObjId(obj_id),
                        }
                    }
                };

                match target {
                    DentryTarget::Tombstone => return Ok(None),
                    DentryTarget::IndexNodeId(id) => {
                        ids.push(id);
                        current_id = id;
                        if is_last {
                            let node = self.handle_get_inode(id, None, ctx.clone()).await.map_err(
                                |e| NdnError::Internal(format!("failed to get inode: {}", e)),
                            )?;
                            if let Some(inode) = node {
                                if let Ok(mut cache) = self.resolve_path_cache.write() {
                                    cache.put_ids_for_path(components.clone(), ids.clone());
                                }
                                return Ok(Some(FsMetaResolvePathResp {
                                    item: FsMetaResolvePathItem::Inode {
                                        inode_id: id,
                                        inode,
                                    },
                                    inner_path: None,
                                }));
                            }
                            return Ok(None);
                        }
                    }
                    DentryTarget::ObjId(obj_id) => {
                        if let Ok(mut cache) = self.resolve_path_cache.write() {
                            cache.put_terminal_for_path(
                                components[..=i].to_vec(),
                                ids.clone(),
                                PathResolveTerminalValue::ObjId(obj_id.clone()),
                            );
                        }
                        return Ok(Some(FsMetaResolvePathResp {
                            item: FsMetaResolvePathItem::ObjId(obj_id),
                            inner_path: join_components(&components[i + 1..]),
                        }));
                    }
                    DentryTarget::SymLink(target_path) => {
                        if let Ok(mut cache) = self.resolve_path_cache.write() {
                            cache.put_terminal_for_path(
                                components[..=i].to_vec(),
                                ids.clone(),
                                PathResolveTerminalValue::SymLink(target_path.clone()),
                            );
                        }
                        let tail = components[i + 1..].to_vec();
                        if sym_count == 0 {
                            return Ok(Some(FsMetaResolvePathResp {
                                item: FsMetaResolvePathItem::SymLink(target_path),
                                inner_path: join_components(&tail),
                            }));
                        }
                        sym_count -= 1;
                        let parent_prefix = components[..i].to_vec();
                        restart_with = Some(normalize_symlink_target_path(
                            &parent_prefix,
                            &target_path,
                            &tail,
                        ));
                        break;
                    }
                }
            }

            if let Some(next_components) = restart_with {
                components = next_components;
                continue;
            }

            return Ok(None);
        }
    }

    async fn handle_begin_txn(&self, _ctx: RPCContext) -> Result<String, RPCErrors> {
        let seq = self.txn_seq.fetch_add(1, Ordering::SeqCst);
        let txid = format!("tx-{}-{}", unix_timestamp(), seq);
        let db_path = self.db_path.clone();
        let conn = tokio::task::spawn_blocking(move || Self::open_txn_connection(&db_path))
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("open txn task failed: {}", e)))??;

        let now = Instant::now();
        let entry = TxnEntry {
            conn: Arc::new(Mutex::new(conn)),
            created_at: now,
            last_used_at: Arc::new(Mutex::new(now)),
            closing: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicU64::new(0)),
            touched_edges: Arc::new(Mutex::new(HashSet::new())),
        };

        self.txns
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?
            .insert(txid.clone(), entry);
        Ok(txid)
    }

    async fn handle_get_inode(
        &self,
        id: IndexNodeId,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<Option<NodeRecord>, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT inode_id, kind, read_only, base_obj_id, state, rev, meta_json,
                            lease_client_session, lease_seq, lease_expire_at,
                            fb_handle, last_write_at, closed_at,
                            linked_obj_id, linked_qcid, linked_filebuffer_id, linked_at,
                            finalized_obj_id, finalized_at
                     FROM nodes WHERE inode_id = ?1",
                )
                .map_err(map_db_err)?;
            let mut rows = stmt.query(params![id as i64]).map_err(map_db_err)?;
            match rows.next().map_err(map_db_err)? {
                Some(row) => Ok(Some(Self::parse_node(row)?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn handle_set_inode(
        &self,
        node: NodeRecord,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let cols = Self::node_state_cols(&node.state);
            let now = unix_timestamp() as i64;
            let meta_json = node
                .meta
                .as_ref()
                .map(|v| serde_json::to_string(v))
                .transpose()
                .map_err(|e| RPCErrors::ReasonError(format!("serialize meta failed: {}", e)))?;

            conn.execute(
                "INSERT INTO nodes (
                    inode_id, kind, read_only, base_obj_id, state, rev, meta_json,
                    lease_client_session, lease_seq, lease_expire_at,
                    fb_handle, last_write_at, closed_at,
                    linked_obj_id, linked_qcid, linked_filebuffer_id, linked_at,
                    finalized_obj_id, finalized_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
                ON CONFLICT(inode_id) DO UPDATE SET
                    kind = excluded.kind,
                    read_only = excluded.read_only,
                    base_obj_id = excluded.base_obj_id,
                    state = excluded.state,
                    rev = excluded.rev,
                    meta_json = excluded.meta_json,
                    lease_client_session = excluded.lease_client_session,
                    lease_seq = excluded.lease_seq,
                    lease_expire_at = excluded.lease_expire_at,
                    fb_handle = excluded.fb_handle,
                    last_write_at = excluded.last_write_at,
                    closed_at = excluded.closed_at,
                    linked_obj_id = excluded.linked_obj_id,
                    linked_qcid = excluded.linked_qcid,
                    linked_filebuffer_id = excluded.linked_filebuffer_id,
                    linked_at = excluded.linked_at,
                    finalized_obj_id = excluded.finalized_obj_id,
                    finalized_at = excluded.finalized_at,
                    updated_at = excluded.updated_at",
                params![
                    node.inode_id as i64,
                    node_kind_to_int(node.kind),
                    if node.read_only { 1i64 } else { 0i64 },
                    node.base_obj_id.as_ref().map(obj_id_to_blob),
                    cols.state,
                    node.rev.map(|v| v as i64),
                    meta_json,
                    node.lease_client_session.as_ref().map(|s| s.0.clone()),
                    node.lease_seq.map(|v| v as i64),
                    node.lease_expire_at.map(|v| v as i64),
                    cols.fb_handle,
                    cols.last_write_at.map(|v| v as i64),
                    cols.closed_at.map(|v| v as i64),
                    cols.linked_obj_id,
                    cols.linked_qcid,
                    cols.linked_filebuffer_id,
                    cols.linked_at.map(|v| v as i64),
                    cols.finalized_obj_id,
                    cols.finalized_at.map(|v| v as i64),
                    now,
                ],
            )
            .map_err(map_db_err)?;
            Ok(())
        })
        .await
    }

    async fn handle_update_inode_state(
        &self,
        node_id: IndexNodeId,
        new_state: NodeState,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let cols = Self::node_state_cols(&new_state);
            let now = unix_timestamp() as i64;
            let updated = conn
                .execute(
                    "UPDATE nodes SET
                        state = ?1,
                        fb_handle = ?2,
                        last_write_at = ?3,
                        closed_at = ?4,
                        linked_obj_id = ?5,
                        linked_qcid = ?6,
                        linked_filebuffer_id = ?7,
                        linked_at = ?8,
                        finalized_obj_id = ?9,
                        finalized_at = ?10,
                        updated_at = ?11
                     WHERE inode_id = ?12",
                    params![
                        cols.state,
                        cols.fb_handle,
                        cols.last_write_at.map(|v| v as i64),
                        cols.closed_at.map(|v| v as i64),
                        cols.linked_obj_id,
                        cols.linked_qcid,
                        cols.linked_filebuffer_id,
                        cols.linked_at.map(|v| v as i64),
                        cols.finalized_obj_id,
                        cols.finalized_at.map(|v| v as i64),
                        now,
                        node_id as i64,
                    ],
                )
                .map_err(map_db_err)?;
            if updated == 0 {
                return Err(RPCErrors::ReasonError("inode not found".to_string()));
            }
            Ok(())
        })
        .await
    }

    async fn handle_alloc_inode(
        &self,
        node: NodeRecord,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<IndexNodeId, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let cols = Self::node_state_cols(&node.state);
            let now = unix_timestamp() as i64;
            let meta_json = node
                .meta
                .as_ref()
                .map(|v| serde_json::to_string(v))
                .transpose()
                .map_err(|e| RPCErrors::ReasonError(format!("serialize meta failed: {}", e)))?;

            if node.inode_id == 0 {
                conn.execute(
                    "INSERT INTO nodes (
                        kind, read_only, base_obj_id, state, rev, meta_json,
                        lease_client_session, lease_seq, lease_expire_at,
                        fb_handle, last_write_at, closed_at,
                        linked_obj_id, linked_qcid, linked_filebuffer_id, linked_at,
                        finalized_obj_id, finalized_at, updated_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
                    params![
                        node_kind_to_int(node.kind),
                        if node.read_only { 1i64 } else { 0i64 },
                        node.base_obj_id.as_ref().map(obj_id_to_blob),
                        cols.state,
                        node.rev.map(|v| v as i64),
                        meta_json,
                        node.lease_client_session.as_ref().map(|s| s.0.clone()),
                        node.lease_seq.map(|v| v as i64),
                        node.lease_expire_at.map(|v| v as i64),
                        cols.fb_handle,
                        cols.last_write_at.map(|v| v as i64),
                        cols.closed_at.map(|v| v as i64),
                        cols.linked_obj_id,
                        cols.linked_qcid,
                        cols.linked_filebuffer_id,
                        cols.linked_at.map(|v| v as i64),
                        cols.finalized_obj_id,
                        cols.finalized_at.map(|v| v as i64),
                        now,
                    ],
                )
                .map_err(map_db_err)?;
                let id = conn.last_insert_rowid() as u64;
                Ok(id)
            } else {
                conn.execute(
                    "INSERT INTO nodes (
                        inode_id, kind, read_only, base_obj_id, state, rev, meta_json,
                        lease_client_session, lease_seq, lease_expire_at,
                        fb_handle, last_write_at, closed_at,
                        linked_obj_id, linked_qcid, linked_filebuffer_id, linked_at,
                        finalized_obj_id, finalized_at, updated_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
                    params![
                        node.inode_id as i64,
                        node_kind_to_int(node.kind),
                        if node.read_only { 1i64 } else { 0i64 },
                        node.base_obj_id.as_ref().map(obj_id_to_blob),
                        cols.state,
                        node.rev.map(|v| v as i64),
                        meta_json,
                        node.lease_client_session.as_ref().map(|s| s.0.clone()),
                        node.lease_seq.map(|v| v as i64),
                        node.lease_expire_at.map(|v| v as i64),
                        cols.fb_handle,
                        cols.last_write_at.map(|v| v as i64),
                        cols.closed_at.map(|v| v as i64),
                        cols.linked_obj_id,
                        cols.linked_qcid,
                        cols.linked_filebuffer_id,
                        cols.linked_at.map(|v| v as i64),
                        cols.finalized_obj_id,
                        cols.finalized_at.map(|v| v as i64),
                        now,
                    ],
                )
                .map_err(map_db_err)?;
                Ok(node.inode_id)
            }
        })
        .await
    }

    async fn handle_get_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<Option<DentryRecord>, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT parent_inode_id, name, target_type, target_inode_id, target_obj_id, mtime
                     FROM dentries WHERE parent_inode_id = ?1 AND name = ?2",
                )
                .map_err(map_db_err)?;
            let mut rows = stmt.query(params![parent as i64, name]).map_err(map_db_err)?;
            match rows.next().map_err(map_db_err)? {
                Some(row) => Ok(Some(parse_dentry(row)?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn handle_list_dentries(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<Vec<DentryRecord>, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT parent_inode_id, name, target_type, target_inode_id, target_obj_id, mtime
                     FROM dentries WHERE parent_inode_id = ?1",
                )
                .map_err(map_db_err)?;
            let mut rows = stmt.query(params![parent as i64]).map_err(map_db_err)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(map_db_err)? {
                out.push(parse_dentry(row)?);
            }
            Ok(out)
        })
        .await
    }

    async fn handle_start_list(
        &self,
        parent: IndexNodeId,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<u64, RPCErrors> {
        let dentries = self.list_merged_dentries(parent, txid.clone(), ctx.clone()).await?;
        let mut entries = BTreeMap::new();
        for dentry in dentries {
            let target = dentry.target.clone();
            let inode = match &target {
                DentryTarget::IndexNodeId(id) => {
                    self.handle_get_inode(*id, txid.clone(), ctx.clone())
                        .await?
                }
                _ => None,
            };
            entries.insert(
                dentry.name.clone(),
                FsMetaListEntry {
                    name: dentry.name,
                    target,
                    inode,
                },
            );
        }

        let mut cache = self
            .list_cache
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("list cache lock poisoned: {}", e)))?;
        Ok(cache.start_session(entries))
    }

    async fn handle_list_next(
        &self,
        list_session_id: u64,
        page_size: u32,
        _ctx: RPCContext,
    ) -> Result<BTreeMap<String, FsMetaListEntry>, RPCErrors> {
        let mut cache = self
            .list_cache
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("list cache lock poisoned: {}", e)))?;
        cache
            .list_next(list_session_id, page_size)
            .ok_or_else(|| RPCErrors::ReasonError("list session not found".to_string()))
    }

    async fn handle_stop_list(
        &self,
        list_session_id: u64,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let mut cache = self
            .list_cache
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("list cache lock poisoned: {}", e)))?;
        cache.stop_session(list_session_id);
        Ok(())
    }

    async fn handle_upsert_dentry(
        &self,
        parent: IndexNodeId,
        name: String,
        target: DentryTarget,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let name_for_invalidate = name.clone();
        let changed = self.with_conn(txid.as_deref(), move |conn| {
            let now = unix_timestamp() as i64;
            let (target_type, target_inode_id, target_obj_id) = dentry_target_cols(&target)?;
            let updated = conn
                .execute(
                "INSERT INTO dentries (parent_inode_id, name, target_type, target_inode_id, target_obj_id, mtime)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(parent_inode_id, name) DO UPDATE SET
                    target_type = excluded.target_type,
                    target_inode_id = excluded.target_inode_id,
                    target_obj_id = excluded.target_obj_id,
                    mtime = excluded.mtime
                 WHERE
                    dentries.target_type IS NOT excluded.target_type OR
                    dentries.target_inode_id IS NOT excluded.target_inode_id OR
                    dentries.target_obj_id IS NOT excluded.target_obj_id",
                params![
                    parent as i64,
                    name,
                    target_type,
                    target_inode_id,
                    target_obj_id,
                    now,
                ],
            )
            .map_err(map_db_err)?;
            if updated > 0 {
                // Any effective child-item change bumps parent directory rev.
                conn.execute(
                    "UPDATE nodes
                     SET rev = COALESCE(rev, 0) + 1, updated_at = ?2
                     WHERE inode_id = ?1",
                    params![parent as i64, now],
                )
                .map_err(map_db_err)?;
            }
            Ok(updated > 0)
        })
        .await?;

        if changed {
            if let Some(txid) = txid {
                let entry = self.get_txn_entry(&txid)?;
                entry
                    .touched_edges
                    .lock()
                    .map_err(|e| {
                        RPCErrors::ReasonError(format!("touched_edges lock poisoned: {}", e))
                    })?
                    .insert((parent, name_for_invalidate));
            } else if let Ok(mut cache) = self.resolve_path_cache.write() {
                cache.invalidate_by_edge(parent, &name_for_invalidate);
            }
        }

        Ok(())
    }

    async fn handle_remove_dentry_row(
        &self,
        parent: IndexNodeId,
        name: String,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let name_for_invalidate = name.clone();
        let changed = self
            .with_conn(txid.as_deref(), move |conn| {
                let now = unix_timestamp() as i64;
                let removed = conn
                    .execute(
                        "DELETE FROM dentries WHERE parent_inode_id = ?1 AND name = ?2",
                        params![parent as i64, name],
                    )
                    .map_err(map_db_err)?;
                if removed > 0 {
                    // Any effective child-item change bumps parent directory rev.
                    conn.execute(
                        "UPDATE nodes
                         SET rev = COALESCE(rev, 0) + 1, updated_at = ?2
                         WHERE inode_id = ?1",
                        params![parent as i64, now],
                    )
                    .map_err(map_db_err)?;
                }
                Ok(removed > 0)
            })
            .await?;

        if changed {
            if let Some(txid) = txid {
                let entry = self.get_txn_entry(&txid)?;
                entry
                    .touched_edges
                    .lock()
                    .map_err(|e| {
                        RPCErrors::ReasonError(format!("touched_edges lock poisoned: {}", e))
                    })?
                    .insert((parent, name_for_invalidate));
            } else if let Ok(mut cache) = self.resolve_path_cache.write() {
                cache.invalidate_by_edge(parent, &name_for_invalidate);
            }
        }

        Ok(())
    }

    async fn handle_set_tombstone(
        &self,
        parent: IndexNodeId,
        name: String,
        txid: Option<String>,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        self.handle_upsert_dentry(parent, name, DentryTarget::Tombstone, txid, ctx)
            .await
    }

    async fn handle_bump_dir_rev(
        &self,
        dir: IndexNodeId,
        expected_rev: u64,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<u64, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let now = unix_timestamp() as i64;
            // Use COALESCE to handle NULL rev (treat as 0), also update updated_at
            let updated = conn
                .execute(
                    "UPDATE nodes SET rev = COALESCE(rev, 0) + 1, updated_at = ?3 
                     WHERE inode_id = ?1 AND COALESCE(rev, 0) = ?2",
                    params![dir as i64, expected_rev as i64, now],
                )
                .map_err(map_db_err)?;
            if updated == 0 {
                return Err(RPCErrors::ReasonError(
                    "rev mismatch or inode not found".to_string(),
                ));
            }
            Ok(expected_rev + 1)
        })
        .await
    }

    async fn handle_commit(&self, txid: Option<String>, _ctx: RPCContext) -> Result<(), RPCErrors> {
        let Some(txid) = txid else {
            return Ok(());
        };

        // Step 1: Mark as closing (but don't remove yet)
        let entry = {
            let txns = self
                .txns
                .lock()
                .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?;
            let entry = txns
                .get(&txid)
                .ok_or_else(|| RPCErrors::ReasonError("txid not found".to_string()))?;

            // Mark as closing - new operations will be rejected
            if entry.closing.swap(true, Ordering::SeqCst) {
                return Err(RPCErrors::ReasonError(
                    "transaction already closing".to_string(),
                ));
            }
            entry.clone()
        };

        // Step 2: Wait for in-flight operations to complete
        let wait_start = Instant::now();
        while entry.in_flight.load(Ordering::SeqCst) > 0 {
            if wait_start.elapsed() > Duration::from_secs(30) {
                // Timeout - still try to commit but warn
                warn!(
                    "commit: timeout waiting for in-flight ops for txid={}",
                    txid
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Step 3: Execute COMMIT
        let conn = entry.conn.clone();
        let commit_result = tokio::task::spawn_blocking(move || {
            let conn_guard = conn
                .lock()
                .map_err(|e| RPCErrors::ReasonError(format!("conn lock poisoned: {}", e)))?;
            conn_guard
                .execute_batch("COMMIT")
                .map_err(|e| RPCErrors::ReasonError(format!("commit failed: {}", e)))
        })
        .await
        .map_err(|e| RPCErrors::ReasonError(format!("commit join failed: {}", e)))??;

        // Step 4: Remove from map only after successful commit
        self.txns
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?
            .remove(&txid);

        // Step 5: Invalidate resolve_path cache entries affected by this txn
        let edges: Vec<(IndexNodeId, String)> = entry
            .touched_edges
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("touched_edges lock poisoned: {}", e)))?
            .iter()
            .cloned()
            .collect();
        if !edges.is_empty() {
            if let Ok(mut cache) = self.resolve_path_cache.write() {
                for (parent, name) in edges {
                    cache.invalidate_by_edge(parent, &name);
                }
            }
        }

        Ok(commit_result)
    }

    async fn handle_rollback(
        &self,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let Some(txid) = txid else {
            return Ok(());
        };

        // Step 1: Mark as closing (but don't remove yet)
        let entry = {
            let txns = self
                .txns
                .lock()
                .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?;
            let entry = txns
                .get(&txid)
                .ok_or_else(|| RPCErrors::ReasonError("txid not found".to_string()))?;

            // Mark as closing - new operations will be rejected
            if entry.closing.swap(true, Ordering::SeqCst) {
                return Err(RPCErrors::ReasonError(
                    "transaction already closing".to_string(),
                ));
            }
            entry.clone()
        };

        // Step 2: Wait for in-flight operations to complete
        let wait_start = Instant::now();
        while entry.in_flight.load(Ordering::SeqCst) > 0 {
            if wait_start.elapsed() > Duration::from_secs(30) {
                warn!(
                    "rollback: timeout waiting for in-flight ops for txid={}",
                    txid
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Step 3: Execute ROLLBACK
        let conn = entry.conn.clone();
        let rollback_result = tokio::task::spawn_blocking(move || {
            let conn_guard = conn
                .lock()
                .map_err(|e| RPCErrors::ReasonError(format!("conn lock poisoned: {}", e)))?;
            conn_guard
                .execute_batch("ROLLBACK")
                .map_err(|e| RPCErrors::ReasonError(format!("rollback failed: {}", e)))
        })
        .await
        .map_err(|e| RPCErrors::ReasonError(format!("rollback join failed: {}", e)))??;

        // Step 4: Remove from map only after successful rollback
        self.txns
            .lock()
            .map_err(|e| RPCErrors::ReasonError(format!("txns lock poisoned: {}", e)))?
            .remove(&txid);

        Ok(rollback_result)
    }

    async fn handle_acquire_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        ttl: Duration,
        _ctx: RPCContext,
    ) -> Result<u64, RPCErrors> {
        self.acquire_file_lease_inner(node_id, session, ttl, None).await
    }

    async fn handle_renew_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        ttl: Duration,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        self.renew_file_lease_inner(node_id, session, lease_seq, ttl, None)
            .await
    }

    async fn handle_release_file_lease(
        &self,
        node_id: IndexNodeId,
        session: SessionId,
        lease_seq: u64,
        _ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        self.release_file_lease_inner(node_id, session, lease_seq, None)
            .await
    }

    async fn handle_obj_stat_get(
        &self,
        obj_id: ObjId,
        _ctx: RPCContext,
    ) -> Result<Option<ObjStat>, RPCErrors> {
        self.with_conn(None, move |conn| {
            let obj_blob = obj_id_to_blob(&obj_id);
            let row = conn
                .query_row(
                    "SELECT ref_count, zero_since, updated_at FROM obj_stat WHERE obj_id = ?1",
                    params![obj_blob],
                    |row| {
                        let ref_count: i64 = row.get(0)?;
                        let zero_since: Option<i64> = row.get(1)?;
                        let updated_at: i64 = row.get(2)?;
                        Ok(ObjStat {
                            obj_id,
                            ref_count: ref_count as u64,
                            zero_since: zero_since.map(|v| v as u64),
                            updated_at: updated_at as u64,
                        })
                    },
                )
                .optional()
                .map_err(map_db_err)?;
            Ok(row)
        })
        .await
    }

    async fn handle_obj_stat_bump(
        &self,
        obj_id: ObjId,
        delta: i64,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<u64, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let now = unix_timestamp() as i64;
            let obj_blob = obj_id_to_blob(&obj_id);

            if delta > 0 {
                // Positive delta: use atomic INSERT ... ON CONFLICT DO UPDATE
                // This handles both new records and existing records atomically
                conn.execute(
                    "INSERT INTO obj_stat (obj_id, ref_count, zero_since, updated_at) 
                     VALUES (?1, ?2, NULL, ?3)
                     ON CONFLICT(obj_id) DO UPDATE SET 
                        ref_count = ref_count + excluded.ref_count,
                        zero_since = NULL,
                        updated_at = excluded.updated_at",
                    params![obj_blob.clone(), delta, now],
                )
                .map_err(map_db_err)?;
            } else {
                // Negative or zero delta: use atomic conditional UPDATE
                // Only update if the result would be non-negative
                let updated = conn
                    .execute(
                        "UPDATE obj_stat SET 
                            ref_count = ref_count + ?2,
                            zero_since = CASE 
                                WHEN ref_count + ?2 = 0 THEN COALESCE(zero_since, ?3)
                                ELSE NULL 
                            END,
                            updated_at = ?3
                         WHERE obj_id = ?1 AND ref_count + ?2 >= 0",
                        params![obj_blob.clone(), delta, now],
                    )
                    .map_err(map_db_err)?;

                if updated == 0 {
                    // Either record doesn't exist or would become negative
                    return Err(RPCErrors::ReasonError(
                        "ref_count would be negative".to_string(),
                    ));
                }
            }

            // Get the new ref_count
            let new_ref_count: i64 = conn
                .query_row(
                    "SELECT ref_count FROM obj_stat WHERE obj_id = ?1",
                    params![obj_blob],
                    |row| row.get(0),
                )
                .map_err(map_db_err)?;

            Ok(new_ref_count as u64)
        })
        .await
    }

    async fn handle_obj_stat_list_zero(
        &self,
        older_than_ts: u64,
        limit: u32,
        _ctx: RPCContext,
    ) -> Result<Vec<ObjId>, RPCErrors> {
        self.with_conn(None, move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT obj_id FROM obj_stat
                     WHERE ref_count = 0 AND zero_since IS NOT NULL AND zero_since <= ?1
                     ORDER BY zero_since ASC LIMIT ?2",
                )
                .map_err(map_db_err)?;
            let mut rows = stmt
                .query(params![older_than_ts as i64, limit as i64])
                .map_err(map_db_err)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(map_db_err)? {
                let obj_blob: Vec<u8> = row.get(0).map_err(map_db_err)?;
                out.push(obj_id_from_blob(obj_blob)?);
            }
            Ok(out)
        })
        .await
    }

    async fn handle_obj_stat_delete_if_zero(
        &self,
        obj_id: ObjId,
        txid: Option<String>,
        _ctx: RPCContext,
    ) -> Result<bool, RPCErrors> {
        self.with_conn(txid.as_deref(), move |conn| {
            let obj_blob = obj_id_to_blob(&obj_id);
            let removed = conn
                .execute(
                    "DELETE FROM obj_stat WHERE obj_id = ?1 AND ref_count = 0",
                    params![obj_blob],
                )
                .map_err(map_db_err)?;
            Ok(removed > 0)
        })
        .await
    }

    //----------------------------------高阶业务操作----------------------------------

    async fn handle_set_file(
        &self,
        path: &NdmPath,
        obj_id: ObjId,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;

        let parent = self.ensure_dir_inode(&parent_path).await?;
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_node = match self
            .handle_get_inode(parent, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "parent directory not found".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if parent_node.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent is not a directory".to_string()
            ));
        }
        if parent_node.read_only {
            rollback_and_err!(RPCErrors::ReasonError("parent is read-only".to_string()));
        }
        if let Err(e) = self
            .ensure_name_absent_for_create(
                parent,
                &parent_node,
                &name,
                path,
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        if let Err(e) = self
            .handle_upsert_dentry(
                parent,
                name,
                DentryTarget::ObjId(obj_id.clone()),
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        if let Err(e) = self
            .handle_obj_stat_bump(obj_id, 1, Some(txid.clone()), ctx.clone())
            .await
        {
            rollback_and_err!(e);
        }

        self.handle_commit(Some(txid), ctx).await?;

        Ok(String::new())
    }

    async fn handle_set_dir(
        &self,
        path: &NdmPath,
        dir_obj_id: ObjId,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;

        if dir_obj_id.obj_type != OBJ_TYPE_DIR {
            return Err(RPCErrors::ReasonError(
                "set_dir requires a DirObject id".to_string(),
            ));
        }

        let parent = self.ensure_dir_inode(&parent_path).await?;
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_node = match self
            .handle_get_inode(parent, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "parent directory not found".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if parent_node.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent is not a directory".to_string()
            ));
        }
        if parent_node.read_only {
            rollback_and_err!(RPCErrors::ReasonError("parent is read-only".to_string()));
        }
        if let Err(e) = self
            .ensure_name_absent_for_create(
                parent,
                &parent_node,
                &name,
                path,
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        if let Err(e) = self
            .handle_upsert_dentry(
                parent,
                name,
                DentryTarget::ObjId(dir_obj_id.clone()),
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        self.handle_commit(Some(txid), ctx).await?;

        // TODO: update ref_count for dir and its children

        Ok(String::new())
    }

    async fn handle_delete(&self, path: &NdmPath, ctx: RPCContext) -> Result<(), RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;

        let parent_id = self.ensure_dir_inode(&parent_path).await?;

        // Set tombstone instead of deleting (overlay semantics)
        self.handle_set_tombstone(parent_id, name, None, ctx)
            .await?;

        Ok(())
    }

    async fn handle_move_path_with_opts(
        &self,
        old_path: &NdmPath,
        new_path: &NdmPath,
        opts: MoveOptions,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        // Normalize and validate
        if old_path.is_root() {
            return Err(RPCErrors::ReasonError("invalid name".to_string()));
        }

        let old_normalized = old_path.as_str().trim_end_matches('/');
        let new_normalized = new_path.as_str().trim_end_matches('/');

        if old_normalized == new_normalized {
            return Ok(()); // No-op
        }

        // Parse paths
        let (src_parent_path, src_name) = old_path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid name".to_string()))?;
        let (dst_parent_path, dst_name) = new_path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid name".to_string()))?;

        // Validate names
        if src_name.is_empty() || dst_name.is_empty() {
            return Err(RPCErrors::ReasonError("invalid name".to_string()));
        }

        // Ensure parent directories exist
        let src_parent = self.ensure_dir_inode(&src_parent_path).await?;
        let dst_parent = self.ensure_dir_inode(&dst_parent_path).await?;

        // Get parent nodes
        let src_dir = self
            .handle_get_inode(src_parent, None, ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;
        let dst_dir = self
            .handle_get_inode(dst_parent, None, ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("not found".to_string()))?;

        if src_dir.kind != NodeKind::Dir || dst_dir.kind != NodeKind::Dir {
            return Err(RPCErrors::ReasonError("not found".to_string()));
        }

        if src_dir.read_only || dst_dir.read_only {
            return Err(RPCErrors::ReasonError("read only".to_string()));
        }

        let src_rev0 = src_dir.rev.unwrap_or(0);
        let dst_rev0 = dst_dir.rev.unwrap_or(0);

        // Build move plan
        let source = self
            .plan_move_source(src_parent, &src_name, &src_dir, src_rev0, ctx.clone())
            .await?;

        // Cycle prevention for directories
        if Self::is_dir_like(&source) {
            if is_descendant_path(new_path, old_path) {
                return Err(RPCErrors::ReasonError("invalid name".to_string()));
            }
        }

        // Check destination conflict
        self.check_dest_conflict_pre(dst_parent, &dst_name, &dst_dir, opts, ctx.clone())
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
        self.apply_move_plan_txn(plan, opts, ctx).await
    }

    // 创建软链接(symlink)，目标保存为路径（可相对路径）
    // SYMLINK: link_path -> target_path
    async fn handle_make_link(
        &self,
        link_path: &NdmPath,
        target: &NdmPath,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        if link_path.is_root() {
            return Err(RPCErrors::ReasonError("invalid link path".to_string()));
        }

        if target.as_str().is_empty() {
            return Err(RPCErrors::ReasonError("invalid target path".to_string()));
        }
        let link_target = DentryTarget::SymLink(target.as_str().to_string());

        let (link_parent_path, link_name) = link_path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid link path".to_string()))?;

        let link_parent_id = self.ensure_dir_inode(&link_parent_path).await?;
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_node = match self
            .handle_get_inode(link_parent_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "parent directory not found".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if parent_node.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent is not a directory".to_string()
            ));
        }
        if parent_node.read_only {
            rollback_and_err!(RPCErrors::ReasonError("parent is read-only".to_string()));
        }

        if let Err(e) = self
            .ensure_name_absent_for_create(
                link_parent_id,
                &parent_node,
                &link_name,
                link_path,
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        if let Err(e) = self
            .handle_upsert_dentry(
                link_parent_id,
                link_name,
                link_target,
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        self.handle_commit(Some(txid), ctx).await?;

        Ok(())
    }

    async fn handle_create_dir(&self, path: &NdmPath, ctx: RPCContext) -> Result<(), RPCErrors> {
        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;

        let parent_id = self.ensure_dir_inode(&parent_path).await?;

        // Create directory inode
        let txid = self.handle_begin_txn(ctx.clone()).await?;

        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_node = match self
            .handle_get_inode(parent_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                "parent directory not found".to_string()
            )),
            Err(e) => rollback_and_err!(e),
        };
        if parent_node.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent is not a directory".to_string()
            ));
        }
        if parent_node.read_only {
            rollback_and_err!(RPCErrors::ReasonError("parent is read-only".to_string()));
        }
        if let Err(e) = self
            .ensure_name_absent_for_create(
                parent_id,
                &parent_node,
                &name,
                path,
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

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

        let new_id = match self
            .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => rollback_and_err!(e),
        };

        if let Err(e) = self
            .handle_upsert_dentry(
                parent_id,
                name,
                DentryTarget::IndexNodeId(new_id),
                Some(txid.clone()),
                ctx.clone(),
            )
            .await
        {
            rollback_and_err!(e);
        }

        self.handle_commit(Some(txid), ctx).await?;

        Ok(())
    }

    async fn handle_open_file_writer(
        &self,
        path: &NdmPath,
        flag: OpenWriteFlag,
        expected_size: Option<u64>,
        ctx: RPCContext,
    ) -> Result<String, RPCErrors> {
        let buffer = self
            .buffer
            .as_ref()
            .ok_or_else(|| RPCErrors::ReasonError("buffer service not configured".to_string()))?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| RPCErrors::ReasonError("instance not configured".to_string()))?;

        let (parent_path, name) = path
            .split_parent_name()
            .ok_or_else(|| RPCErrors::ReasonError("invalid path".to_string()))?;
        let parent_id: u64 = self.ensure_dir_inode(&parent_path).await?;

        let txid = self.handle_begin_txn(ctx.clone()).await?;
        macro_rules! rollback_and_err {
            ($err:expr) => {{
                let _ = self.handle_rollback(Some(txid.clone()), ctx.clone()).await;
                return Err($err);
            }};
        }

        let parent_node = match self
            .handle_get_inode(parent_id, Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(Some(node)) => node,
            Ok(None) => rollback_and_err!(RPCErrors::ReasonError("parent not found".to_string())),
            Err(e) => rollback_and_err!(e),
        };
        if parent_node.kind != NodeKind::Dir {
            rollback_and_err!(RPCErrors::ReasonError(
                "parent is not a directory".to_string()
            ));
        }
        if parent_node.read_only {
            rollback_and_err!(RPCErrors::ReasonError("parent is read-only".to_string()));
        }

        let dentry = match self
            .handle_get_dentry(parent_id, name.clone(), Some(txid.clone()), ctx.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => rollback_and_err!(e),
        };

        let mut visible_base_obj: Option<ObjId> = None;
        if dentry.is_none() {
            match self.lookup_base_child(&parent_node, &name).await {
                Ok(BaseChildLookup::Missing) => {}
                Ok(BaseChildLookup::DirObj(_)) => {
                    rollback_and_err!(RPCErrors::ReasonError("path is a directory".to_string()));
                }
                Ok(BaseChildLookup::NonDirObj(obj_id)) => {
                    visible_base_obj = Some(obj_id);
                }
                Err(e) => rollback_and_err!(e),
            }
        }

        let file_exists = visible_base_obj.is_some()
            || matches!(
                &dentry,
                Some(DentryRecord {
                    target: DentryTarget::IndexNodeId(_),
                    ..
                }) | Some(DentryRecord {
                    target: DentryTarget::SymLink(_),
                    ..
                }) | Some(DentryRecord {
                    target: DentryTarget::ObjId(_),
                    ..
                })
            );
        match flag {
            OpenWriteFlag::Append | OpenWriteFlag::ContinueWrite => {
                if !file_exists {
                    rollback_and_err!(RPCErrors::ReasonError(format!(
                        "file not found for {:?}: {}",
                        flag, path.0
                    )));
                }
            }
            OpenWriteFlag::CreateExclusive => {
                if file_exists {
                    rollback_and_err!(RPCErrors::ReasonError(format!(
                        "file already exists: {}",
                        path.0
                    )));
                }
            }
            OpenWriteFlag::CreateOrTruncate | OpenWriteFlag::CreateOrAppend => {}
        }

        let (file_id, existing_base_obj, existing_state) = match dentry {
            None if visible_base_obj.is_none() => {
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: None,
                    state: NodeState::DirNormal,
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };
                let fid = match self
                    .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
                    .await
                {
                    Ok(v) => v,
                    Err(e) => rollback_and_err!(e),
                };
                if let Err(e) = self
                    .handle_upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                        ctx.clone(),
                    )
                    .await
                {
                    rollback_and_err!(e);
                }
                (fid, None, NodeState::DirNormal)
            }
            None => {
                let base_oid = match visible_base_obj.take() {
                    Some(v) => v,
                    None => rollback_and_err!(RPCErrors::ReasonError(
                        "base object missing".to_string()
                    )),
                };
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: Some(base_oid.clone()),
                    state: NodeState::DirNormal,
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };
                let fid = match self
                    .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
                    .await
                {
                    Ok(v) => v,
                    Err(e) => rollback_and_err!(e),
                };
                if let Err(e) = self
                    .handle_upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                        ctx.clone(),
                    )
                    .await
                {
                    rollback_and_err!(e);
                }
                (fid, Some(base_oid), NodeState::DirNormal)
            }
            Some(DentryRecord {
                target: DentryTarget::Tombstone,
                ..
            }) => {
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: None,
                    state: NodeState::DirNormal,
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };
                let fid = match self
                    .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
                    .await
                {
                    Ok(v) => v,
                    Err(e) => rollback_and_err!(e),
                };
                if let Err(e) = self
                    .handle_upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                        ctx.clone(),
                    )
                    .await
                {
                    rollback_and_err!(e);
                }
                (fid, None, NodeState::DirNormal)
            }
            Some(DentryRecord {
                target: DentryTarget::IndexNodeId(fid),
                ..
            }) => {
                let node = match self
                    .handle_get_inode(fid, Some(txid.clone()), ctx.clone())
                    .await
                {
                    Ok(Some(node)) => node,
                    Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                        "inode not found".to_string()
                    )),
                    Err(e) => rollback_and_err!(e),
                };

                if node.kind == NodeKind::Dir {
                    rollback_and_err!(RPCErrors::ReasonError("path is a directory".to_string()));
                }
                if node.kind != NodeKind::File {
                    rollback_and_err!(RPCErrors::ReasonError("path is not a file".to_string()));
                }
                match (&flag, &node.state) {
                    (OpenWriteFlag::ContinueWrite, NodeState::Working(_))
                    | (OpenWriteFlag::ContinueWrite, NodeState::Cooling(_)) => {}
                    (_, NodeState::Working(_)) => {
                        rollback_and_err!(RPCErrors::ReasonError(
                            "file is already being written".to_string()
                        ));
                    }
                    (OpenWriteFlag::ContinueWrite, _) => {
                        rollback_and_err!(RPCErrors::ReasonError(format!(
                            "ContinueWrite requires Working or Cooling state, current: {:?}",
                            node.state
                        )));
                    }
                    _ => {}
                }

                (fid, node.base_obj_id.clone(), node.state.clone())
            }
            Some(DentryRecord {
                target: DentryTarget::ObjId(oid),
                ..
            }) => {
                if oid.obj_type == OBJ_TYPE_DIR {
                    rollback_and_err!(RPCErrors::ReasonError("path is a directory".to_string()));
                }
                let new_node = NodeRecord {
                    inode_id: 0,
                    kind: NodeKind::File,
                    read_only: false,
                    base_obj_id: Some(oid.clone()),
                    state: NodeState::DirNormal,
                    rev: None,
                    meta: None,
                    lease_client_session: None,
                    lease_seq: None,
                    lease_expire_at: None,
                };
                let fid = match self
                    .handle_alloc_inode(new_node, Some(txid.clone()), ctx.clone())
                    .await
                {
                    Ok(v) => v,
                    Err(e) => rollback_and_err!(e),
                };
                if let Err(e) = self
                    .handle_upsert_dentry(
                        parent_id,
                        name.clone(),
                        DentryTarget::IndexNodeId(fid),
                        Some(txid.clone()),
                        ctx.clone(),
                    )
                    .await
                {
                    rollback_and_err!(e);
                }
                (fid, Some(oid), NodeState::DirNormal)
            }
            Some(DentryRecord {
                target: DentryTarget::SymLink(_),
                ..
            }) => {
                rollback_and_err!(RPCErrors::ReasonError(
                    "path is a symbolic link".to_string()
                ));
            }
        };

        let (should_truncate, existing_chunks, resume_handle) = match flag {
            OpenWriteFlag::CreateExclusive | OpenWriteFlag::CreateOrTruncate => {
                (true, vec![], None)
            }
            OpenWriteFlag::Append | OpenWriteFlag::CreateOrAppend => {
                let chunks = if let Some(ref base_oid) = existing_base_obj {
                    self.load_file_chunklist(base_oid).await.unwrap_or_default()
                } else {
                    vec![]
                };
                (false, chunks, None)
            }
            OpenWriteFlag::ContinueWrite => match existing_state {
                NodeState::Working(ws) => (false, vec![], Some(ws.fb_handle)),
                NodeState::Cooling(cs) => (false, vec![], Some(cs.fb_handle)),
                _ => {
                    rollback_and_err!(RPCErrors::ReasonError(
                        "unexpected state for ContinueWrite".to_string()
                    ));
                }
            },
        };

        if let Some(handle) = resume_handle {
            if let Err(_e) = buffer.get_buffer(&handle).await {
                rollback_and_err!(RPCErrors::ReasonError(format!(
                    "buffer {} not found for ContinueWrite, may have been cleaned up",
                    handle
                )));
            }

            let session = SessionId(format!("{}:{}", instance, file_id));
            let lease_seq = match self
                .acquire_file_lease_inner(
                    file_id,
                    session.clone(),
                    Duration::from_secs(WRITE_LEASE_TTL_SECS),
                    Some(txid.clone()),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => rollback_and_err!(e),
            };

            let working_state = NodeState::Working(ndm::FileWorkingState {
                fb_handle: handle.clone(),
                last_write_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });
            if let Err(e) = self
                .handle_update_inode_state(file_id, working_state, Some(txid.clone()), ctx.clone())
                .await
            {
                rollback_and_err!(e);
            }
            if let Err(e) = self.handle_commit(Some(txid.clone()), ctx.clone()).await {
                rollback_and_err!(e);
            }
            let _ = (session, lease_seq);
            return Ok(handle);
        }

        let session = SessionId(format!("{}:{}", instance, file_id));
        let lease_seq = match self
            .acquire_file_lease_inner(
                file_id,
                session.clone(),
                Duration::from_secs(WRITE_LEASE_TTL_SECS),
                Some(txid.clone()),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => rollback_and_err!(e),
        };

        let lease = fs_buffer::WriteLease {
            session: session.clone(),
            session_seq: lease_seq,
            expires_at: 0,
        };
        let fb = match buffer
            .alloc_buffer(
                &fs_buffer::NdmPath(path.0.clone()),
                file_id,
                existing_chunks,
                &lease,
                expected_size,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => rollback_and_err!(RPCErrors::ReasonError(format!(
                "failed to alloc buffer: {}",
                e
            ))),
        };

        let working_state = NodeState::Working(ndm::FileWorkingState {
            fb_handle: fb.handle_id.clone(),
            last_write_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });
        if should_truncate && existing_base_obj.is_some() {
            let mut node = match self
                .handle_get_inode(file_id, Some(txid.clone()), ctx.clone())
                .await
            {
                Ok(Some(node)) => node,
                Ok(None) => rollback_and_err!(RPCErrors::ReasonError(
                    "inode not found for truncate".to_string()
                )),
                Err(e) => rollback_and_err!(e),
            };
            node.base_obj_id = None;
            if let Err(e) = self
                .handle_set_inode(node, Some(txid.clone()), ctx.clone())
                .await
            {
                rollback_and_err!(e);
            }
        }

        if let Err(e) = self
            .handle_update_inode_state(file_id, working_state, Some(txid.clone()), ctx.clone())
            .await
        {
            rollback_and_err!(e);
        }
        if let Err(e) = self.handle_commit(Some(txid.clone()), ctx.clone()).await {
            rollback_and_err!(e);
        }

        Ok(fb.handle_id)
    }

    async fn handle_close_file_writer(
        &self,
        file_inode_id: IndexNodeId,
        ctx: RPCContext,
    ) -> Result<(), RPCErrors> {
        let buffer = self
            .buffer
            .as_ref()
            .ok_or_else(|| RPCErrors::ReasonError("buffer service not configured".to_string()))?;

        let txid = self.handle_begin_txn(ctx.clone()).await?;

        let node = self
            .handle_get_inode(file_inode_id, Some(txid.clone()), ctx.clone())
            .await?
            .ok_or_else(|| RPCErrors::ReasonError("inode not found".to_string()))?;

        if node.kind != NodeKind::File {
            let _ = self.handle_rollback(Some(txid), ctx.clone()).await;
            return Err(RPCErrors::ReasonError("not a file".to_string()));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let fb_handle = match &node.state {
            NodeState::Working(ws) => ws.fb_handle.clone(),
            NodeState::Cooling(cs) => cs.fb_handle.clone(),
            _ => {
                let _ = self.handle_rollback(Some(txid), ctx.clone()).await;
                return Err(RPCErrors::ReasonError("file is not writable".to_string()));
            }
        };

        let fb = buffer
            .get_buffer(&fb_handle)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("failed to get buffer: {}", e)))?;
        buffer
            .close(&fb)
            .await
            .map_err(|e| RPCErrors::ReasonError(format!("failed to close buffer: {}", e)))?;

        let cooling_state = NodeState::Cooling(ndm::FileCoolingState {
            fb_handle: fb_handle.clone(),
            closed_at: now,
        });

        self.handle_update_inode_state(
            file_inode_id,
            cooling_state,
            Some(txid.clone()),
            ctx.clone(),
        )
        .await?;

        self.handle_commit(Some(txid), ctx.clone()).await?;

        if let (Some(session), Some(seq)) = (node.lease_client_session, node.lease_seq) {
            let session = SessionId(session.0);
            self.handle_release_file_lease(file_inode_id, session, seq, ctx)
                .await?;
        }

        Ok(())
    }

    //return ObjectId + innerpath or file_buffer_handle_id
    async fn handle_open_file_reader(
        &self,
        path: NdmPath,
        ctx: RPCContext,
    ) -> Result<OpenFileReaderResp, RPCErrors> {
        if path.is_root() {
            return Err(RPCErrors::ReasonError("path is a directory".to_string()));
        }

        let resolved = self
            .handle_resolve_path_ex(&path, 0, ctx.clone())
            .await
            .map_err(|e| RPCErrors::ReasonError(e.to_string()))?
            .ok_or_else(|| RPCErrors::ReasonError("path not found".to_string()))?;

        match resolved.item {
            FsMetaResolvePathItem::SymLink(_) => Err(RPCErrors::ReasonError(
                "path is a symbolic link".to_string(),
            )),
            FsMetaResolvePathItem::ObjId(obj_id) => {
                Ok(OpenFileReaderResp::Object(obj_id, resolved.inner_path))
            }
            FsMetaResolvePathItem::Inode { inode, .. } => {
                if inode.kind == NodeKind::Dir {
                    return Err(RPCErrors::ReasonError("path is a directory".to_string()));
                }

                match inode.state {
                    NodeState::Working(ws) => Ok(OpenFileReaderResp::FileBufferId(ws.fb_handle)),
                    NodeState::Cooling(cs) => Ok(OpenFileReaderResp::FileBufferId(cs.fb_handle)),
                    NodeState::Linked(ls) => Ok(OpenFileReaderResp::Object(ls.obj_id, None)),
                    NodeState::Finalized(fs) => Ok(OpenFileReaderResp::Object(fs.obj_id, None)),
                    NodeState::DirNormal | NodeState::DirOverlay => {
                        if let Some(obj_id) = inode.base_obj_id {
                            Ok(OpenFileReaderResp::Object(obj_id, None))
                        } else {
                            Err(RPCErrors::ReasonError("no readable content".to_string()))
                        }
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct NodeStateCols {
    state: i64,
    fb_handle: Option<String>,
    last_write_at: Option<u64>,
    closed_at: Option<u64>,
    linked_obj_id: Option<Vec<u8>>,
    linked_qcid: Option<Vec<u8>>,
    linked_filebuffer_id: Option<String>,
    linked_at: Option<u64>,
    finalized_obj_id: Option<Vec<u8>>,
    finalized_at: Option<u64>,
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

fn join_components(components: &[String]) -> Option<String> {
    if components.is_empty() {
        None
    } else {
        Some(format!("/{}", components.join("/")))
    }
}

fn normalize_symlink_target_path(
    parent_prefix: &[String],
    target_path: &str,
    tail: &[String],
) -> Vec<String> {
    let mut out = if target_path.starts_with('/') {
        Vec::new()
    } else {
        parent_prefix.to_vec()
    };

    for segment in target_path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                out.pop();
            }
            _ => out.push(segment.to_string()),
        }
    }

    out.extend(tail.iter().cloned());
    out
}

fn obj_id_to_blob(obj_id: &ObjId) -> Vec<u8> {
    obj_id.to_bytes()
}

fn obj_id_from_blob(blob: Vec<u8>) -> Result<ObjId, RPCErrors> {
    ObjId::from_bytes(&blob)
        .map_err(|e| RPCErrors::ReasonError(format!("invalid obj_id bytes: {}", e)))
}

fn symlink_path_to_blob(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

fn symlink_path_from_blob(blob: Vec<u8>) -> Result<String, RPCErrors> {
    String::from_utf8(blob)
        .map_err(|e| RPCErrors::ReasonError(format!("invalid symlink path bytes: {}", e)))
}

fn node_kind_to_int(kind: NodeKind) -> i64 {
    match kind {
        NodeKind::File => 0,
        NodeKind::Dir => 1,
        NodeKind::Object => 2,
    }
}

fn node_kind_from_int(value: i64) -> Result<NodeKind, RPCErrors> {
    match value {
        0 => Ok(NodeKind::File),
        1 => Ok(NodeKind::Dir),
        2 => Ok(NodeKind::Object),
        _ => Err(RPCErrors::ReasonError("invalid node kind".to_string())),
    }
}

fn dentry_target_cols(
    target: &DentryTarget,
) -> Result<(i64, Option<i64>, Option<Vec<u8>>), RPCErrors> {
    match target {
        DentryTarget::IndexNodeId(id) => Ok((DENTRY_TARGET_INODE, Some(*id as i64), None)),
        DentryTarget::SymLink(path) => Ok((
            DENTRY_TARGET_SYMLINK,
            None,
            Some(symlink_path_to_blob(path)),
        )),
        DentryTarget::ObjId(obj_id) => Ok((DENTRY_TARGET_OBJ, None, Some(obj_id_to_blob(obj_id)))),
        DentryTarget::Tombstone => Ok((DENTRY_TARGET_TOMBSTONE, None, None)),
    }
}

fn parse_dentry(row: &rusqlite::Row<'_>) -> Result<DentryRecord, RPCErrors> {
    let parent: i64 = row.get(0).map_err(map_db_err)?;
    let name: String = row.get(1).map_err(map_db_err)?;
    let target_type: i64 = row.get(2).map_err(map_db_err)?;
    let target_inode_id: Option<i64> = row.get(3).map_err(map_db_err)?;
    let target_obj_id: Option<Vec<u8>> = row.get(4).map_err(map_db_err)?;
    let mtime: Option<i64> = row.get(5).map_err(map_db_err)?;
    let target = match target_type {
        DENTRY_TARGET_INODE => DentryTarget::IndexNodeId(
            target_inode_id
                .ok_or_else(|| RPCErrors::ReasonError("missing target_inode_id".to_string()))?
                as IndexNodeId,
        ),
        DENTRY_TARGET_OBJ => {
            DentryTarget::ObjId(obj_id_from_blob(target_obj_id.ok_or_else(|| {
                RPCErrors::ReasonError("missing target_obj_id".to_string())
            })?)?)
        }
        DENTRY_TARGET_TOMBSTONE => DentryTarget::Tombstone,
        DENTRY_TARGET_SYMLINK => {
            if let Some(blob) = target_obj_id {
                DentryTarget::SymLink(symlink_path_from_blob(blob)?)
            } else if let Some(id) = target_inode_id {
                // Legacy fallback for early symlink-as-inode format.
                DentryTarget::SymLink(format!("legacy-inode:{}", id))
            } else {
                return Err(RPCErrors::ReasonError(
                    "missing symlink target path".to_string(),
                ));
            }
        }
        _ => return Err(RPCErrors::ReasonError("invalid dentry target".to_string())),
    };
    Ok(DentryRecord {
        parent: parent as IndexNodeId,
        name,
        target,
        mtime: mtime.map(|v| v as u64),
    })
}

fn map_db_err(err: rusqlite::Error) -> RPCErrors {
    RPCErrors::ReasonError(format!("db error: {}", err))
}
