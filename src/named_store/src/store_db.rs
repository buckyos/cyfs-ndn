use crate::gc_types::*;
use log::{debug, warn};
use ndn_lib::{
    ChunkId, KnownStandardObject, NdnError, NdnResult, ObjId,
};
use rusqlite::types::{FromSql, ToSql, ValueRef};
use rusqlite::{params, Connection, Transaction};
use std::ops::Range;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ────────────────────────────────────────────────────────────────
// ChunkLocalInfo / ChunkStoreState (unchanged public API)
// ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChunkLocalInfo {
    #[serde(skip_serializing, default)]
    pub path: String,
    pub qcid: String,
    pub last_modify_time: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub range: Option<Range<u64>>,
}

impl Default for ChunkLocalInfo {
    fn default() -> Self {
        Self {
            path: String::new(),
            qcid: String::new(),
            last_modify_time: 0,
            range: None,
        }
    }
}

impl ChunkLocalInfo {
    pub fn create_by_info_str(path: String, info_str: &str) -> NdnResult<Self> {
        let mut local_info: ChunkLocalInfo =
            serde_json::from_str(info_str).map_err(|e| NdnError::InvalidParam(e.to_string()))?;
        local_info.path = path;
        Ok(local_info)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStoreState {
    New,
    Completed,
    Disabled,
    NotExist,
    LocalLink(ChunkLocalInfo),
    SameAs(ObjId),
}

impl ChunkStoreState {
    pub fn from_str(s: &str) -> Self {
        match s {
            "new" => ChunkStoreState::New,
            "completed" => ChunkStoreState::Completed,
            // 向后兼容：旧数据库中的 incompleted 按 New 处理
            "incompleted" => ChunkStoreState::New,
            "disabled" => ChunkStoreState::Disabled,
            "not_exist" => ChunkStoreState::NotExist,
            "local_link" => ChunkStoreState::LocalLink(ChunkLocalInfo::default()),
            // SameAs target will be filled from local_info column when reading
            "same_as" => ChunkStoreState::SameAs(ObjId::new_by_raw(String::new(), Vec::new())),
            _ => ChunkStoreState::NotExist,
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            ChunkStoreState::New => "new".to_string(),
            ChunkStoreState::Completed => "completed".to_string(),
            ChunkStoreState::Disabled => "disabled".to_string(),
            ChunkStoreState::NotExist => "not_exist".to_string(),
            ChunkStoreState::LocalLink(_) => "local_link".to_string(),
            ChunkStoreState::SameAs(_) => "same_as".to_string(),
        }
    }

    pub fn can_open_reader(&self) -> bool {
        matches!(
            self,
            ChunkStoreState::Completed | ChunkStoreState::LocalLink(_) | ChunkStoreState::SameAs(_)
        )
    }

    pub fn can_open_writer(&self) -> bool {
        matches!(
            self,
            ChunkStoreState::New | ChunkStoreState::NotExist
        )
    }

    pub fn is_local_link(&self) -> bool {
        matches!(self, ChunkStoreState::LocalLink(_))
    }

    pub fn is_same_as(&self) -> bool {
        matches!(self, ChunkStoreState::SameAs(_))
    }
}

impl ToSql for ChunkStoreState {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = match self {
            ChunkStoreState::New => "new",
            ChunkStoreState::Completed => "completed",
            ChunkStoreState::Disabled => "disabled",
            ChunkStoreState::NotExist => "not_exist",
            ChunkStoreState::LocalLink(_) => "local_link",
            ChunkStoreState::SameAs(_) => "same_as",
        };
        Ok(s.into())
    }
}

impl FromSql for ChunkStoreState {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str().unwrap_or("not_exist");
        Ok(ChunkStoreState::from_str(s))
    }
}

// ────────────────────────────────────────────────────────────────
// ChunkItem
// ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ChunkItem {
    pub chunk_id: ChunkId,
    pub chunk_size: u64,
    pub chunk_state: ChunkStoreState,
    pub create_time: u64,
    pub update_time: u64,
}

impl ChunkItem {
    pub fn new(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let now_time = unix_timestamp();
        Self {
            chunk_id: chunk_id.clone(),
            chunk_size,
            chunk_state: ChunkStoreState::New,
            create_time: now_time,
            update_time: now_time,
        }
    }

    pub fn new_completed(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkStoreState::Completed;
        result
    }

    pub fn new_local_file(
        chunk_id: &ChunkId,
        chunk_size: u64,
        chunk_local_info: &ChunkLocalInfo,
    ) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkStoreState::LocalLink(chunk_local_info.clone());
        result
    }
}

// ────────────────────────────────────────────────────────────────
// NamedLocalStoreDB
// ────────────────────────────────────────────────────────────────

pub struct NamedLocalStoreDB {
    pub db_path: String,
    conn: Mutex<Connection>,
}

impl NamedLocalStoreDB {
    pub fn new(db_path: String) -> NdnResult<Self> {
        debug!("NamedLocalStoreDB: new db path: {}", db_path);
        let conn = Connection::open(&db_path).map_err(|e| {
            warn!("NamedLocalStoreDB: open db failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        // Enable WAL for better concurrent read performance
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let db = Self {
            db_path,
            conn: Mutex::new(conn),
        };
        db.init_tables()?;
        Ok(db)
    }

    fn init_tables(&self) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            -- Objects table (extended with GC columns)
            CREATE TABLE IF NOT EXISTS objects (
                obj_id             TEXT PRIMARY KEY,
                obj_type           TEXT NOT NULL,
                obj_data           TEXT,
                create_time        INTEGER NOT NULL,
                last_access_time   INTEGER NOT NULL,
                state              TEXT    NOT NULL DEFAULT 'present',
                eviction_class     INTEGER NOT NULL DEFAULT 0,
                fs_anchor_count    INTEGER NOT NULL DEFAULT 0,
                logical_size       INTEGER NOT NULL DEFAULT 0,
                owned_bytes        INTEGER NOT NULL DEFAULT 0,
                children_expanded  INTEGER NOT NULL DEFAULT 0,
                home_epoch         INTEGER
            );

            -- Chunk items table (extended with GC columns)
            CREATE TABLE IF NOT EXISTS chunk_items (
                chunk_id           TEXT PRIMARY KEY,
                chunk_size         INTEGER NOT NULL,
                chunk_state        TEXT NOT NULL,
                local_path         TEXT,
                local_info         TEXT,
                progress           TEXT,
                create_time        INTEGER NOT NULL,
                update_time        INTEGER NOT NULL,
                state              TEXT    NOT NULL DEFAULT 'present',
                last_access_time   INTEGER NOT NULL DEFAULT 0,
                eviction_class     INTEGER NOT NULL DEFAULT 0,
                fs_anchor_count    INTEGER NOT NULL DEFAULT 0,
                logical_size       INTEGER NOT NULL DEFAULT 0,
                owned_bytes        INTEGER NOT NULL DEFAULT 0,
                children_expanded  INTEGER NOT NULL DEFAULT 0,
                home_epoch         INTEGER
            );

            -- Incoming references: only written by anchored cascade
            CREATE TABLE IF NOT EXISTS incoming_refs (
                referee        TEXT NOT NULL,
                referrer       TEXT NOT NULL,
                declared_epoch INTEGER NOT NULL,
                created_at     INTEGER NOT NULL,
                PRIMARY KEY (referee, referrer)
            );

            -- Edge outbox: durable cross-bucket side effects
            CREATE TABLE IF NOT EXISTS edge_outbox (
                seq          INTEGER PRIMARY KEY AUTOINCREMENT,
                op           TEXT NOT NULL,
                referee      TEXT NOT NULL,
                referrer     TEXT NOT NULL,
                target_epoch INTEGER NOT NULL,
                created_at   INTEGER NOT NULL,
                attempts     INTEGER NOT NULL DEFAULT 0,
                next_try_at  INTEGER NOT NULL DEFAULT 0
            );

            -- Pins
            CREATE TABLE IF NOT EXISTS pins (
                obj_id         TEXT NOT NULL,
                owner          TEXT NOT NULL,
                scope          TEXT NOT NULL,
                cascade_state  TEXT NOT NULL DEFAULT 'Pending',
                created_at     INTEGER NOT NULL,
                expires_at     INTEGER,
                PRIMARY KEY (obj_id, owner)
            );

            -- FS anchors
            CREATE TABLE IF NOT EXISTS fs_anchors (
                obj_id         TEXT    NOT NULL,
                inode_id       INTEGER NOT NULL,
                field_tag      INTEGER NOT NULL,
                cascade_state  TEXT    NOT NULL DEFAULT 'Pending',
                created_at     INTEGER NOT NULL,
                PRIMARY KEY (obj_id, inode_id, field_tag)
            );

            -- Indexes
            CREATE INDEX IF NOT EXISTS objects_lru_present
                ON objects(eviction_class, state, last_access_time);
            CREATE INDEX IF NOT EXISTS chunk_items_lru_present
                ON chunk_items(eviction_class, state, last_access_time);
            CREATE INDEX IF NOT EXISTS incoming_refs_by_referee
                ON incoming_refs(referee);
            CREATE INDEX IF NOT EXISTS edge_outbox_ready
                ON edge_outbox(next_try_at);
            CREATE INDEX IF NOT EXISTS pins_by_owner
                ON pins(owner);
            CREATE INDEX IF NOT EXISTS pins_by_expire
                ON pins(expires_at) WHERE expires_at IS NOT NULL;
            CREATE INDEX IF NOT EXISTS pins_recursive_by_obj
                ON pins(obj_id) WHERE scope = 'recursive';
            CREATE INDEX IF NOT EXISTS pins_skeleton_by_obj
                ON pins(obj_id) WHERE scope = 'skeleton';
            CREATE INDEX IF NOT EXISTS fs_anchors_by_obj
                ON fs_anchors(obj_id);
            ",
        )
        .map_err(|e| {
            warn!("NamedLocalStoreDB: init tables failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    // ================================================================
    // Original chunk / object CRUD (kept for backward compat)
    // ================================================================

    pub fn set_chunk_item(&self, chunk_item: &ChunkItem) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();

        match &chunk_item.chunk_state {
            ChunkStoreState::LocalLink(local_info) => {
                let local_info_str = serde_json::to_string(local_info).unwrap();
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items
                    (chunk_id, chunk_size, chunk_state, local_path, local_info, progress,
                     create_time, update_time, state, owned_bytes, logical_size)
                    VALUES (?1, ?2, ?3, ?4, ?5, '', ?6, ?7, 'present', 0, ?2)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size as i64,
                        chunk_item.chunk_state,
                        local_info.path,
                        local_info_str,
                        chunk_item.create_time as i64,
                        chunk_item.update_time as i64,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedLocalStoreDB: insert chunk failed! {}", e);
                    NdnError::DbError(e.to_string())
                })?;
            }
            ChunkStoreState::SameAs(chunk_list_id) => {
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items
                    (chunk_id, chunk_size, chunk_state, local_info, progress,
                     create_time, update_time, state, owned_bytes, logical_size)
                    VALUES (?1, ?2, ?3, ?4, '', ?5, ?6, 'present', 0, ?2)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size as i64,
                        chunk_item.chunk_state,
                        chunk_list_id.to_string(),
                        chunk_item.create_time as i64,
                        chunk_item.update_time as i64,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedLocalStoreDB: insert chunk failed! {}", e);
                    NdnError::DbError(e.to_string())
                })?;
            }
            _ => {
                let state_str = if chunk_item.chunk_state == ChunkStoreState::Completed {
                    "present"
                } else {
                    "new"
                };
                let owned = if chunk_item.chunk_state == ChunkStoreState::Completed {
                    chunk_item.chunk_size as i64
                } else {
                    0i64
                };
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items
                    (chunk_id, chunk_size, chunk_state, progress,
                     create_time, update_time, state, owned_bytes, logical_size)
                    VALUES (?1, ?2, ?3, '', ?4, ?5, ?6, ?7, ?2)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size as i64,
                        chunk_item.chunk_state,
                        chunk_item.create_time as i64,
                        chunk_item.update_time as i64,
                        state_str,
                        owned,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedLocalStoreDB: insert chunk failed! {}", e);
                    NdnError::DbError(e.to_string())
                })?;
            }
        }

        Ok(())
    }

    pub fn get_chunk_item(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        let conn = self.conn.lock().unwrap();
        Self::get_chunk_item_with_conn(&conn, chunk_id)
    }

    fn get_chunk_item_with_conn(conn: &Connection, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        let mut stmt = conn
            .prepare(
                "SELECT chunk_size, chunk_state, create_time, update_time, local_path, local_info
                 FROM chunk_items WHERE chunk_id = ?1",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let chunk = stmt
            .query_row(params![chunk_id.to_string()], |row| {
                let mut chunk_state: ChunkStoreState = row.get(1)?;
                if chunk_state.is_local_link() {
                    let local_path: String = row.get(4)?;
                    let local_info_str: String = row.get(5)?;
                    let local_info =
                        ChunkLocalInfo::create_by_info_str(local_path, local_info_str.as_str())
                            .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?;
                    chunk_state = ChunkStoreState::LocalLink(local_info);
                } else if chunk_state.is_same_as() {
                    let target_str: String = row.get(5)?;
                    let target_id = ObjId::new(&target_str)
                        .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?;
                    chunk_state = ChunkStoreState::SameAs(target_id);
                }

                Ok(ChunkItem {
                    chunk_id: chunk_id.clone(),
                    chunk_size: row.get::<_, i64>(0)? as u64,
                    chunk_state,
                    create_time: row.get::<_, i64>(2)? as u64,
                    update_time: row.get::<_, i64>(3)? as u64,
                })
            })
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    NdnError::NotFound(format!("chunk not found: {}", chunk_id.to_string()))
                }
                _ => {
                    warn!("NamedLocalStoreDB: get chunk failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                }
            })?;

        Ok(chunk)
    }

    pub fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        tx.execute(
            "DELETE FROM chunk_items WHERE chunk_id = ?1",
            params![chunk_id.to_string()],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        // Also clean up any incoming_refs / outbox referencing this chunk
        tx.execute(
            "DELETE FROM incoming_refs WHERE referee = ?1",
            params![chunk_id.to_string()],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn set_object(&self, obj_id: &ObjId, obj_type: &str, obj_str: &str) -> NdnResult<()> {
        let now_time = unix_timestamp();
        let logical_size = obj_str.len() as i64;
        let owned_bytes = obj_str.len() as i64;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        Self::put_object_in_tx(&tx, obj_id, obj_type, obj_str, now_time, logical_size, owned_bytes)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Core put_object logic inside a transaction, handling shadow→present promotion.
    fn put_object_in_tx(
        tx: &Transaction,
        obj_id: &ObjId,
        obj_type: &str,
        obj_str: &str,
        now_time: u64,
        logical_size: i64,
        owned_bytes: i64,
    ) -> NdnResult<bool> {
        // Check existing state
        let existing_state: Option<String> = tx
            .query_row(
                "SELECT state FROM objects WHERE obj_id = ?1",
                params![obj_id.to_string()],
                |row| row.get(0),
            )
            .ok();

        match existing_state.as_deref() {
            Some("present") => {
                // Already present, just touch LRU
                tx.execute(
                    "UPDATE objects SET last_access_time = MAX(last_access_time, ?1)
                     WHERE obj_id = ?2",
                    params![now_time as i64, obj_id.to_string()],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
                return Ok(false); // no state change
            }
            Some("shadow") => {
                // shadow → present: fill in content
                tx.execute(
                    "UPDATE objects SET obj_type = ?1, obj_data = ?2, state = 'present',
                     last_access_time = ?3, logical_size = ?4, owned_bytes = ?5
                     WHERE obj_id = ?6",
                    params![
                        obj_type,
                        obj_str,
                        now_time as i64,
                        logical_size,
                        owned_bytes,
                        obj_id.to_string()
                    ],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
                // Promote cascade_state for anchors pointing at this obj
                Self::promote_anchor_cascade_state_on_present_tx(tx, obj_id)?;
                // Recompute + reconcile since state changed
                Self::recompute_eviction_class_tx(tx, &obj_id.to_string())?;
                Self::reconcile_expand_state_tx(tx, obj_id)?;
                return Ok(true); // state changed
            }
            Some(_) | None => {
                // New insert as present
                tx.execute(
                    "INSERT OR REPLACE INTO objects
                     (obj_id, obj_type, obj_data, create_time, last_access_time,
                      state, logical_size, owned_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'present', ?6, ?7)",
                    params![
                        obj_id.to_string(),
                        obj_type,
                        obj_str,
                        now_time as i64,
                        now_time as i64,
                        logical_size,
                        owned_bytes,
                    ],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
                // For new inserts, no anchors/pins exist yet, class stays 0
                return Ok(true);
            }
        }
    }

    pub fn get_object(&self, obj_id: &ObjId) -> NdnResult<(String, String)> {
        let conn = self.conn.lock().unwrap();
        Self::get_object_with_conn(&conn, obj_id)
    }

    fn get_object_with_conn(conn: &Connection, obj_id: &ObjId) -> NdnResult<(String, String)> {
        let mut stmt = conn
            .prepare("SELECT obj_type, obj_data, state FROM objects WHERE obj_id = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let result = stmt
            .query_row(params![obj_id.to_string()], |row| {
                let state: String = row.get(2)?;
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, state))
            })
            .map_err(|e| {
                warn!("NamedLocalStoreDB: query object failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if result.2 == "shadow" {
            return Err(NdnError::NotFound(format!(
                "object is shadow: {}",
                obj_id.to_string()
            )));
        }

        Ok((result.0, result.1))
    }

    pub fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM objects WHERE obj_id = ?1",
            params![obj_id.to_string()],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    // ================================================================
    // GC Core: shadow, eviction_class, reconcile
    // ================================================================

    /// Ensure a shadow row exists for the given obj_id. No-op if already present.
    fn upsert_shadow_if_absent_tx(tx: &Transaction, obj_id: &str) -> NdnResult<()> {
        let now = unix_timestamp() as i64;
        tx.execute(
            "INSERT OR IGNORE INTO objects
             (obj_id, obj_type, obj_data, create_time, last_access_time, state,
              eviction_class, fs_anchor_count, logical_size, owned_bytes, children_expanded)
             VALUES (?1, '', '', ?2, ?2, 'shadow', 0, 0, 0, 0, 0)",
            params![obj_id, now],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        // Also check chunk_items if this looks like a chunk
        // We unify: objects table holds all items for GC purposes.
        // Chunks also get shadow rows in the objects table for GC tracking.
        Ok(())
    }

    /// Check if obj_id has any active (non-expired) pin.
    fn has_active_pin_tx(tx: &Transaction, obj_id: &str) -> NdnResult<bool> {
        let now = unix_timestamp() as i64;
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id, now],
                |row| row.get(0),
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count > 0)
    }

    fn has_recursive_pin_tx(tx: &Transaction, obj_id: &str) -> NdnResult<bool> {
        let now = unix_timestamp() as i64;
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1 AND scope = 'recursive'
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id, now],
                |row| row.get(0),
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count > 0)
    }

    fn has_skeleton_pin_tx(tx: &Transaction, obj_id: &str) -> NdnResult<bool> {
        let now = unix_timestamp() as i64;
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1 AND scope = 'skeleton'
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id, now],
                |row| row.get(0),
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count > 0)
    }

    fn has_incoming_tx(tx: &Transaction, obj_id: &str) -> NdnResult<bool> {
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM incoming_refs WHERE referee = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count > 0)
    }

    fn get_fs_anchor_count_tx(tx: &Transaction, obj_id: &str) -> NdnResult<i64> {
        let count: i64 = tx
            .query_row(
                "SELECT fs_anchor_count FROM objects WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(count)
    }

    fn get_item_state_tx(tx: &Transaction, obj_id: &str) -> NdnResult<Option<String>> {
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM objects WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .ok();
        Ok(state)
    }

    fn get_children_expanded_tx(tx: &Transaction, obj_id: &str) -> NdnResult<bool> {
        let val: i64 = tx
            .query_row(
                "SELECT children_expanded FROM objects WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(val != 0)
    }

    /// Recompute eviction_class from current state.
    fn recompute_eviction_class_tx(tx: &Transaction, obj_id: &str) -> NdnResult<()> {
        let has_pin = Self::has_active_pin_tx(tx, obj_id)?;
        let fs_count = Self::get_fs_anchor_count_tx(tx, obj_id)?;
        let has_incoming = Self::has_incoming_tx(tx, obj_id)?;

        let class = if has_pin || fs_count > 0 {
            2
        } else if has_incoming {
            1
        } else {
            0
        };

        tx.execute(
            "UPDATE objects SET eviction_class = ?1 WHERE obj_id = ?2",
            params![class, obj_id],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Parse children references from an object's stored content.
    fn parse_obj_refs_tx(tx: &Transaction, obj_id: &ObjId) -> NdnResult<Vec<ObjId>> {
        // Read obj_type and obj_data
        let result: Option<(String, String)> = tx
            .query_row(
                "SELECT obj_type, obj_data FROM objects WHERE obj_id = ?1 AND state = 'present'",
                params![obj_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .ok();

        let (obj_type, obj_data) = match result {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };

        if obj_type.is_empty() || obj_data.is_empty() {
            return Ok(Vec::new());
        }

        // Use KnownStandardObject to extract children
        match KnownStandardObject::from_obj_data(obj_id, &obj_data) {
            Ok(known) => {
                let children = known.get_child_objs().unwrap_or_default();
                Ok(children.into_iter().map(|(id, _)| id).collect())
            }
            Err(_) => {
                // Not a known container type; no children
                Ok(Vec::new())
            }
        }
    }

    /// The central reconcile: compare should_expand vs children_expanded,
    /// enqueue outbox add/remove as needed.
    fn reconcile_expand_state_tx(tx: &Transaction, obj_id: &ObjId) -> NdnResult<()> {
        let obj_id_str = obj_id.to_string();

        let state = Self::get_item_state_tx(tx, &obj_id_str)?;
        let is_present = state.as_deref() == Some("present");

        let want = if !is_present {
            false
        } else {
            let has_skeleton = Self::has_skeleton_pin_tx(tx, &obj_id_str)?;
            if has_skeleton {
                false
            } else {
                let has_recursive = Self::has_recursive_pin_tx(tx, &obj_id_str)?;
                let fs_count = Self::get_fs_anchor_count_tx(tx, &obj_id_str)?;
                let has_incoming = Self::has_incoming_tx(tx, &obj_id_str)?;
                has_recursive || fs_count > 0 || has_incoming
            }
        };

        let have = Self::get_children_expanded_tx(tx, &obj_id_str)?;

        if want && !have {
            tx.execute(
                "UPDATE objects SET children_expanded = 1 WHERE obj_id = ?1",
                params![obj_id_str],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

            let children = Self::parse_obj_refs_tx(tx, obj_id)?;
            let now = unix_timestamp() as i64;
            let epoch = 1i64; // P0: single epoch
            for child in &children {
                tx.execute(
                    "INSERT INTO edge_outbox (op, referee, referrer, target_epoch, created_at)
                     VALUES ('add', ?1, ?2, ?3, ?4)",
                    params![child.to_string(), obj_id_str, epoch, now],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
            }
        }

        if !want && have {
            tx.execute(
                "UPDATE objects SET children_expanded = 0 WHERE obj_id = ?1",
                params![obj_id_str],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

            let children = Self::parse_obj_refs_tx(tx, obj_id)?;
            let now = unix_timestamp() as i64;
            let epoch = 1i64;
            for child in &children {
                tx.execute(
                    "INSERT INTO edge_outbox (op, referee, referrer, target_epoch, created_at)
                     VALUES ('remove', ?1, ?2, ?3, ?4)",
                    params![child.to_string(), obj_id_str, epoch, now],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Promote cascade_state from Pending to Materializing for all anchors/recursive pins
    /// that point at this obj_id. Called in the shadow→present transaction.
    fn promote_anchor_cascade_state_on_present_tx(
        tx: &Transaction,
        obj_id: &ObjId,
    ) -> NdnResult<()> {
        let obj_id_str = obj_id.to_string();
        tx.execute(
            "UPDATE pins SET cascade_state = 'Materializing'
             WHERE obj_id = ?1 AND scope = 'recursive' AND cascade_state = 'Pending'",
            params![obj_id_str],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.execute(
            "UPDATE fs_anchors SET cascade_state = 'Materializing'
             WHERE obj_id = ?1 AND cascade_state = 'Pending'",
            params![obj_id_str],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    // ================================================================
    // Pin operations
    // ================================================================

    pub fn pin(
        &self,
        obj_id: &ObjId,
        owner: &str,
        scope: PinScope,
        ttl: Option<std::time::Duration>,
    ) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let obj_id_str = obj_id.to_string();
        Self::upsert_shadow_if_absent_tx(&tx, &obj_id_str)?;

        let now = unix_timestamp();
        let expires_at: Option<i64> = ttl.map(|d| (now + d.as_secs()) as i64);
        let is_present = Self::get_item_state_tx(&tx, &obj_id_str)?.as_deref() == Some("present");

        let cascade_state = if scope != PinScope::Recursive || is_present {
            "Materializing"
        } else {
            "Pending"
        };

        tx.execute(
            "INSERT INTO pins (obj_id, owner, scope, cascade_state, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(obj_id, owner) DO UPDATE SET
                scope = excluded.scope,
                cascade_state = excluded.cascade_state,
                expires_at = excluded.expires_at",
            params![
                obj_id_str,
                owner,
                scope.as_str(),
                cascade_state,
                now as i64,
                expires_at,
            ],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        Self::recompute_eviction_class_tx(&tx, &obj_id_str)?;
        Self::reconcile_expand_state_tx(&tx, obj_id)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let obj_id_str = obj_id.to_string();
        let deleted = tx
            .execute(
                "DELETE FROM pins WHERE obj_id = ?1 AND owner = ?2",
                params![obj_id_str, owner],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if deleted > 0 {
            Self::recompute_eviction_class_tx(&tx, &obj_id_str)?;
            Self::reconcile_expand_state_tx(&tx, obj_id)?;
        }

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Unpin all entries for an owner. Returns number of affected objects.
    pub fn unpin_owner(&self, owner: &str) -> NdnResult<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        // Collect affected obj_ids first
        let mut stmt = tx
            .prepare("SELECT DISTINCT obj_id FROM pins WHERE owner = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        let obj_ids: Vec<String> = stmt
            .query_map(params![owner], |row| row.get(0))
            .map_err(|e| NdnError::DbError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        tx.execute("DELETE FROM pins WHERE owner = ?1", params![owner])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for oid_str in &obj_ids {
            Self::recompute_eviction_class_tx(&tx, oid_str)?;
            if let Ok(oid) = ObjId::new(oid_str) {
                Self::reconcile_expand_state_tx(&tx, &oid)?;
            }
        }

        let count = obj_ids.len();
        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count)
    }

    // ================================================================
    // fs_anchor operations
    // ================================================================

    pub fn fs_acquire(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let obj_id_str = obj_id.to_string();
        Self::upsert_shadow_if_absent_tx(&tx, &obj_id_str)?;

        let now = unix_timestamp() as i64;
        let is_present = Self::get_item_state_tx(&tx, &obj_id_str)?.as_deref() == Some("present");
        let cascade_state = if is_present {
            "Materializing"
        } else {
            "Pending"
        };

        let inserted = tx
            .execute(
                "INSERT OR IGNORE INTO fs_anchors (obj_id, inode_id, field_tag, cascade_state, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![obj_id_str, inode_id as i64, field_tag as i64, cascade_state, now],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if inserted > 0 {
            tx.execute(
                "UPDATE objects SET fs_anchor_count = fs_anchor_count + 1 WHERE obj_id = ?1",
                params![obj_id_str],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        Self::recompute_eviction_class_tx(&tx, &obj_id_str)?;
        Self::reconcile_expand_state_tx(&tx, obj_id)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn fs_release(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let obj_id_str = obj_id.to_string();
        let deleted = tx
            .execute(
                "DELETE FROM fs_anchors WHERE obj_id = ?1 AND inode_id = ?2 AND field_tag = ?3",
                params![obj_id_str, inode_id as i64, field_tag as i64],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if deleted > 0 {
            tx.execute(
                "UPDATE objects SET fs_anchor_count = MAX(0, fs_anchor_count - 1) WHERE obj_id = ?1",
                params![obj_id_str],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        Self::recompute_eviction_class_tx(&tx, &obj_id_str)?;
        Self::reconcile_expand_state_tx(&tx, obj_id)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Release all fs_anchors for a given inode. Returns number of affected objects.
    pub fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let mut stmt = tx
            .prepare("SELECT DISTINCT obj_id FROM fs_anchors WHERE inode_id = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        let obj_ids: Vec<String> = stmt
            .query_map(params![inode_id as i64], |row| row.get(0))
            .map_err(|e| NdnError::DbError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        // For each affected object, count how many anchors will be removed
        for oid_str in &obj_ids {
            let anchor_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM fs_anchors WHERE obj_id = ?1 AND inode_id = ?2",
                    params![oid_str, inode_id as i64],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            tx.execute(
                "UPDATE objects SET fs_anchor_count = MAX(0, fs_anchor_count - ?1) WHERE obj_id = ?2",
                params![anchor_count, oid_str],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        tx.execute(
            "DELETE FROM fs_anchors WHERE inode_id = ?1",
            params![inode_id as i64],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        for oid_str in &obj_ids {
            Self::recompute_eviction_class_tx(&tx, oid_str)?;
            if let Ok(oid) = ObjId::new(oid_str) {
                Self::reconcile_expand_state_tx(&tx, &oid)?;
            }
        }

        let count = obj_ids.len();
        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count)
    }

    // ================================================================
    // apply_edge
    // ================================================================

    pub fn apply_edge(&self, msg: &EdgeMsg) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let referee_str = msg.referee.to_string();
        Self::upsert_shadow_if_absent_tx(&tx, &referee_str)?;

        match msg.op {
            EdgeOp::Add => {
                let now = unix_timestamp() as i64;
                tx.execute(
                    "INSERT OR IGNORE INTO incoming_refs (referee, referrer, declared_epoch, created_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![referee_str, msg.referrer.to_string(), msg.target_epoch as i64, now],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
            }
            EdgeOp::Remove => {
                tx.execute(
                    "DELETE FROM incoming_refs WHERE referee = ?1 AND referrer = ?2",
                    params![referee_str, msg.referrer.to_string()],
                )
                .map_err(|e| NdnError::DbError(e.to_string()))?;
            }
        }

        Self::recompute_eviction_class_tx(&tx, &referee_str)?;
        Self::reconcile_expand_state_tx(&tx, &msg.referee)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    // ================================================================
    // Edge outbox
    // ================================================================

    /// Fetch ready outbox entries (next_try_at <= now), up to limit.
    pub fn fetch_outbox_ready(&self, limit: usize) -> NdnResult<Vec<OutboxEntry>> {
        let conn = self.conn.lock().unwrap();
        let now = unix_timestamp() as i64;
        let mut stmt = conn
            .prepare(
                "SELECT seq, op, referee, referrer, target_epoch, created_at, attempts, next_try_at
                 FROM edge_outbox WHERE next_try_at <= ?1
                 ORDER BY seq ASC LIMIT ?2",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let entries = stmt
            .query_map(params![now, limit as i64], |row| {
                let op_str: String = row.get(1)?;
                let referee_str: String = row.get(2)?;
                let referrer_str: String = row.get(3)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    op_str,
                    referee_str,
                    referrer_str,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })
            .map_err(|e| NdnError::DbError(e.to_string()))?
            .filter_map(|r| r.ok())
            .filter_map(
                |(seq, op_str, referee_str, referrer_str, epoch, created, attempts, next_try)| {
                    let op = EdgeOp::from_str(&op_str)?;
                    let referee = ObjId::new(&referee_str).ok()?;
                    let referrer = ObjId::new(&referrer_str).ok()?;
                    Some(OutboxEntry {
                        seq,
                        msg: EdgeMsg {
                            op,
                            referee,
                            referrer,
                            target_epoch: epoch as u64,
                        },
                        attempts: attempts as u32,
                        next_try_at: next_try as u64,
                        created_at: created as u64,
                    })
                },
            )
            .collect();

        Ok(entries)
    }

    /// Mark outbox entry as completed (delete it).
    pub fn complete_outbox_entry(&self, seq: i64) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM edge_outbox WHERE seq = ?1", params![seq])
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Bump retry for outbox entry with exponential backoff.
    pub fn retry_outbox_entry(&self, seq: i64) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        let now = unix_timestamp() as i64;
        // Simple backoff: 2^attempts seconds, capped at 300s
        conn.execute(
            "UPDATE edge_outbox SET attempts = attempts + 1,
             next_try_at = ?1 + MIN(300, (1 << MIN(attempts, 8)))
             WHERE seq = ?2",
            params![now, seq],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Count pending outbox entries.
    pub fn outbox_count(&self) -> NdnResult<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM edge_outbox", [], |row| row.get(0))
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count as u64)
    }

    // ================================================================
    // GC: LRU candidates & eviction
    // ================================================================

    /// List class-0, present, owned_bytes > 0 objects ordered by LRU.
    pub fn list_gc_candidates(&self, limit: usize) -> NdnResult<Vec<(String, u64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT obj_id, owned_bytes FROM objects
                 WHERE eviction_class = 0 AND state = 'present' AND owned_bytes > 0
                 ORDER BY last_access_time ASC
                 LIMIT ?1",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let results = stmt
            .query_map(params![limit as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| NdnError::DbError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(results)
    }

    /// Try to evict a single object. Returns owned_bytes freed, or 0 if protected.
    pub fn try_evict_object(&self, obj_id_str: &str) -> NdnResult<u64> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        // Double-check: still class 0 with no protection?
        let has_pin = Self::has_active_pin_tx(&tx, obj_id_str)?;
        let has_incoming = Self::has_incoming_tx(&tx, obj_id_str)?;
        let fs_count = Self::get_fs_anchor_count_tx(&tx, obj_id_str)?;

        if has_pin || has_incoming || fs_count > 0 {
            // Drift correction: recompute class
            Self::recompute_eviction_class_tx(&tx, obj_id_str)?;
            tx.commit()
                .map_err(|e| NdnError::DbError(e.to_string()))?;
            return Ok(0);
        }

        let owned: i64 = tx
            .query_row(
                "SELECT owned_bytes FROM objects WHERE obj_id = ?1",
                params![obj_id_str],
                |row| row.get(0),
            )
            .unwrap_or(0);

        tx.execute(
            "DELETE FROM objects WHERE obj_id = ?1",
            params![obj_id_str],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        // Also remove from chunk_items if it's a chunk
        tx.execute(
            "DELETE FROM chunk_items WHERE chunk_id = ?1",
            params![obj_id_str],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(owned as u64)
    }

    /// Batch update last_access_time using MAX(old, new).
    pub fn batch_touch_last_access_with_max(&self, items: &[(String, u64)]) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for (obj_id, ts) in items {
            tx.execute(
                "UPDATE objects SET last_access_time = MAX(last_access_time, ?1)
                 WHERE obj_id = ?2",
                params![*ts as i64, obj_id],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    // ================================================================
    // Per-anchor completeness queries
    // ================================================================

    pub fn anchor_state(&self, obj_id: &ObjId, owner: &str) -> NdnResult<CascadeStateP0> {
        let conn = self.conn.lock().unwrap();
        let state_str: String = conn
            .query_row(
                "SELECT cascade_state FROM pins WHERE obj_id = ?1 AND owner = ?2",
                params![obj_id.to_string(), owner],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    NdnError::NotFound(format!("pin not found: {} / {}", obj_id.to_string(), owner))
                }
                _ => NdnError::DbError(e.to_string()),
            })?;

        CascadeStateP0::from_str(&state_str)
            .ok_or_else(|| NdnError::InvalidData(format!("unknown cascade_state: {}", state_str)))
    }

    pub fn fs_anchor_state(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<CascadeStateP0> {
        let conn = self.conn.lock().unwrap();
        let state_str: String = conn
            .query_row(
                "SELECT cascade_state FROM fs_anchors
                 WHERE obj_id = ?1 AND inode_id = ?2 AND field_tag = ?3",
                params![obj_id.to_string(), inode_id as i64, field_tag as i64],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => NdnError::NotFound(format!(
                    "fs_anchor not found: {} / {} / {}",
                    obj_id.to_string(),
                    inode_id,
                    field_tag
                )),
                _ => NdnError::DbError(e.to_string()),
            })?;

        CascadeStateP0::from_str(&state_str)
            .ok_or_else(|| NdnError::InvalidData(format!("unknown cascade_state: {}", state_str)))
    }

    // ================================================================
    // Debug / diagnostic queries
    // ================================================================

    pub fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug> {
        let conn = self.conn.lock().unwrap();
        let obj_id_str = obj_id.to_string();

        let row = conn
            .query_row(
                "SELECT state, eviction_class, children_expanded, fs_anchor_count,
                        owned_bytes, logical_size, last_access_time
                 FROM objects WHERE obj_id = ?1",
                params![obj_id_str],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    NdnError::NotFound(format!("object not found: {}", obj_id_str))
                }
                _ => NdnError::DbError(e.to_string()),
            })?;

        let incoming_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM incoming_refs WHERE referee = ?1",
                params![obj_id_str],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let now = unix_timestamp() as i64;
        let has_recursive: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1 AND scope = 'recursive'
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id_str, now],
                |row| Ok(row.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        let has_skeleton: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1 AND scope = 'skeleton'
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id_str, now],
                |row| Ok(row.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        let has_lease: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pins WHERE obj_id = ?1 AND scope = 'lease'
                 AND (expires_at IS NULL OR expires_at > ?2)",
                params![obj_id_str, now],
                |row| Ok(row.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        Ok(ExpandDebug {
            obj_id: obj_id.clone(),
            state: ItemState::from_str(&row.0),
            eviction_class: row.1 as u32,
            children_expanded: row.2 != 0,
            fs_anchor_count: row.3 as u32,
            incoming_refs_count: incoming_count as u32,
            has_recursive_pin: has_recursive,
            has_skeleton_pin: has_skeleton,
            has_lease_pin: has_lease,
            owned_bytes: row.4 as u64,
            logical_size: row.5 as u64,
            last_access_time: row.6 as u64,
        })
    }

    /// Expire pins past their TTL. Returns number of expired pins.
    pub fn expire_pins(&self) -> NdnResult<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        let now = unix_timestamp() as i64;

        let mut stmt = tx
            .prepare(
                "SELECT DISTINCT obj_id FROM pins
                 WHERE expires_at IS NOT NULL AND expires_at <= ?1",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        let obj_ids: Vec<String> = stmt
            .query_map(params![now], |row| row.get(0))
            .map_err(|e| NdnError::DbError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        tx.execute(
            "DELETE FROM pins WHERE expires_at IS NOT NULL AND expires_at <= ?1",
            params![now],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        for oid_str in &obj_ids {
            Self::recompute_eviction_class_tx(&tx, oid_str)?;
            if let Ok(oid) = ObjId::new(oid_str) {
                Self::reconcile_expand_state_tx(&tx, &oid)?;
            }
        }

        let count = obj_ids.len();
        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(count)
    }

    /// Check if an obj_id row exists in the objects table (any state).
    pub fn has_object_row(&self, obj_id: &str) -> NdnResult<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(count > 0)
    }

    /// Get the object's state string.
    pub fn get_object_state(&self, obj_id: &str) -> NdnResult<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let state: Option<String> = conn
            .query_row(
                "SELECT state FROM objects WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .ok();
        Ok(state)
    }

    /// Put object with full GC awareness (called from NamedLocalStore).
    /// Handles shadow→present and reconcile.
    pub fn put_object_gc_aware(
        &self,
        obj_id: &ObjId,
        obj_type: &str,
        obj_str: &str,
    ) -> NdnResult<()> {
        let now = unix_timestamp();
        let logical_size = obj_str.len() as i64;
        let owned_bytes = obj_str.len() as i64;

        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        Self::put_object_in_tx(&tx, obj_id, obj_type, obj_str, now, logical_size, owned_bytes)?;

        tx.commit()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "test_gc.rs"]
mod test_gc;
