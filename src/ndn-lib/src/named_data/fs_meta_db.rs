use crate::{NdnError, NdnResult, ObjId};
use buckyos_kit::buckyos_get_unix_timestamp;
use log::*;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::Value;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathObjType {
    File = 1,
    Dir = 2,
}

impl PathObjType {
    fn from_i64(value: i64) -> NdnResult<Self> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::Dir),
            _ => Err(NdnError::InvalidData(format!(
                "invalid path obj_type: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    Empty = 0,
    Working = 1,
    Committed = 2,
}

impl PathState {
    fn from_i64(value: i64) -> NdnResult<Self> {
        match value {
            0 => Ok(Self::Empty),
            1 => Ok(Self::Working),
            2 => Ok(Self::Committed),
            _ => Err(NdnError::InvalidData(format!(
                "invalid path state: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkingState {
    Writing = 0,
    Closed = 1,
}

impl WorkingState {
    fn from_i64(value: i64) -> NdnResult<Self> {
        match value {
            0 => Ok(Self::Writing),
            1 => Ok(Self::Closed),
            _ => Err(NdnError::InvalidData(format!(
                "invalid working state: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriteLease {
    pub session_id: String,
    pub fence: i64,
    pub lease_ttl: i64,
}

#[derive(Debug, Clone)]
pub struct WorkingEntry {
    pub path: String,
    pub session_id: String,
    pub fence: i64,
    pub lease_ttl: i64,
    pub buffer_id: String,
    pub expect_size: Option<u64>,
    pub state: WorkingState,
    pub create_time: i64,
    pub update_time: i64,
}

#[derive(Debug, Clone)]
pub struct PathStat {
    pub path: String,
    pub state: PathState,
    pub obj_id: Option<ObjId>,
    pub obj_type: Option<PathObjType>,
    pub version: i64,
    pub ctime: i64,
    pub mtime: i64,
    pub working: Option<WorkingEntry>,
    pub materialize_state: ObjMaterializeState,
    pub materialize_time: i64,
    pub materialize_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjMaterializeState {
    NotPulled = 0,
    Pulling = 1,
    Ready = 2,
    Failed = 3,
}

impl ObjMaterializeState {
    fn from_i64(value: i64) -> NdnResult<Self> {
        match value {
            0 => Ok(Self::NotPulled),
            1 => Ok(Self::Pulling),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Failed),
            _ => Err(NdnError::InvalidData(format!(
                "invalid materialize state: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjStat {
    pub obj_id: ObjId,
    pub materialize_state: ObjMaterializeState,
    pub materialize_time: i64,
    pub materialize_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListEntry {
    pub name: String,
    pub path: String,
    pub obj_id: ObjId,
    pub obj_type: PathObjType,
    pub state: PathState,
}

#[derive(Debug, Clone)]
pub struct ListResult {
    pub entries: Vec<ListEntry>,
    pub next_pos: u32,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct PullContext {
    pub remote: String,
}

impl Default for PullContext {
    fn default() -> Self {
        Self {
            remote: "".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReaderOptions {
    pub session_id: Option<String>,
    pub allow_working: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            session_id: None,
            allow_working: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FsMetaReader {
    pub path: Option<String>,
    pub obj_id: ObjId,
    pub obj_type: Option<PathObjType>,
}

#[derive(Debug, Clone)]
pub struct FileBufferHandle {
    pub path: String,
    pub buffer_id: String,
    pub lease: WriteLease,
}

pub struct FsMetaDb {
    pub db_path: String,
    conn: Mutex<Connection>,
}

impl FsMetaDb {
    pub fn new(db_path: String) -> NdnResult<Self> {
        let conn = Connection::open(&db_path).map_err(|e| {
            warn!("FsMetaDb: open db failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS path_entry (
                path TEXT PRIMARY KEY,
                obj_id TEXT,
                obj_type INTEGER,
                state INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                ctime INTEGER NOT NULL,
                version INTEGER NOT NULL,
                meta BLOB
            )",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create path_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS working_entry (
                path TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                fence INTEGER NOT NULL,
                lease_ttl INTEGER NOT NULL,
                buffer_id TEXT NOT NULL,
                expect_size INTEGER,
                state INTEGER NOT NULL,
                create_time INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create working_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS lease_entry (
                path TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                fence INTEGER NOT NULL,
                lease_ttl INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create lease_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS path_mount_index (
                parent_path TEXT NOT NULL,
                child_prefix TEXT NOT NULL,
                PRIMARY KEY (parent_path, child_prefix)
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "FsMetaDb: create path_mount_index failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS obj_materialize (
                obj_id TEXT PRIMARY KEY,
                state INTEGER NOT NULL,
                update_time INTEGER NOT NULL,
                last_error TEXT
            )",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create obj_materialize failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS pull_task (
                obj_id TEXT PRIMARY KEY,
                remote TEXT NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create pull_task failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_path_entry_state ON path_entry(state)",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create index failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_working_entry_session ON working_entry(session_id)",
            [],
        )
        .map_err(|e| {
            warn!("FsMetaDb: create index failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(Self {
            db_path,
            conn: Mutex::new(conn),
        })
    }

    fn now_ts() -> i64 {
        buckyos_get_unix_timestamp() as i64
    }

    fn ensure_parent_dir_not_mounted(tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let conflict_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM path_entry WHERE obj_type = ?1 AND ?2 LIKE (path || '/%')",
                params![PathObjType::Dir as i64, path],
                |row| row.get(0),
            )
            .map_err(|e| {
                warn!(
                    "FsMetaDb: check parent mount conflict failed! {}",
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        if conflict_count > 0 {
            return Err(NdnError::InvalidState("MOUNT_CONFLICT".to_string()));
        }

        Ok(())
    }

    fn ensure_no_child_binding(tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let like_path = format!("{}/%", path);
        let child_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM path_entry WHERE path LIKE ?1",
                params![like_path],
                |row| row.get(0),
            )
            .map_err(|e| {
                warn!(
                    "FsMetaDb: check child mount conflict failed! {}",
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        if child_count > 0 {
            return Err(NdnError::InvalidState("MOUNT_CONFLICT".to_string()));
        }

        Ok(())
    }

    fn get_path_state_tx(tx: &Transaction<'_>, path: &str) -> NdnResult<Option<(PathState, i64)>> {
        let row = tx
            .query_row(
                "SELECT state, version FROM path_entry WHERE path = ?1",
                params![path],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: query path state failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        match row {
            Some((state, version)) => Ok(Some((PathState::from_i64(state)?, version))),
            None => Ok(None),
        }
    }

    fn lease_expired(update_time: i64, lease_ttl: i64, now: i64) -> bool {
        update_time + lease_ttl <= now
    }

    fn check_lease_tx(tx: &Transaction<'_>, path: &str, lease: &WriteLease) -> NdnResult<()> {
        let row = tx
            .query_row(
                "SELECT session_id, fence, lease_ttl, update_time FROM lease_entry WHERE path = ?1",
                params![path],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: query lease failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if row.is_none() {
            return Err(NdnError::InvalidState("LEASE_CONFLICT".to_string()));
        }

        let (session_id, fence, lease_ttl, update_time) = row.unwrap();
        let now = Self::now_ts();
        if Self::lease_expired(update_time, lease_ttl, now) {
            return Err(NdnError::InvalidState("LEASE_CONFLICT".to_string()));
        }

        if session_id != lease.session_id || fence != lease.fence {
            return Err(NdnError::InvalidState("LEASE_CONFLICT".to_string()));
        }

        Ok(())
    }

    fn get_obj_materialize_state_tx(
        tx: &Transaction<'_>,
        obj_id: &ObjId,
    ) -> NdnResult<ObjMaterializeState> {
        let row = tx
            .query_row(
                "SELECT state FROM obj_materialize WHERE obj_id = ?1",
                params![obj_id.to_string()],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: query obj_materialize failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        match row {
            Some(state) => ObjMaterializeState::from_i64(state),
            None => Ok(ObjMaterializeState::NotPulled),
        }
    }

    pub fn set_obj_materialize_state(
        &self,
        obj_id: &ObjId,
        state: ObjMaterializeState,
        last_error: Option<String>,
    ) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        let now = Self::now_ts();
        conn.execute(
            "INSERT OR REPLACE INTO obj_materialize (obj_id, state, update_time, last_error) VALUES (?1, ?2, ?3, ?4)",
            params![obj_id.to_string(), state as i64, now, last_error],
        )
        .map_err(|e| {
            warn!("FsMetaDb: set_obj_materialize_state failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn add_file(&self, path: &str, obj_id: &ObjId) -> NdnResult<()> {
        self.add_committed_path(path, obj_id, PathObjType::File)
    }

    pub fn add_dir(&self, path: &str, obj_id: &ObjId) -> NdnResult<()> {
        self.add_committed_path(path, obj_id, PathObjType::Dir)
    }

    pub fn add_committed_path(
        &self,
        path: &str,
        obj_id: &ObjId,
        obj_type: PathObjType,
    ) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: add path tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Self::ensure_parent_dir_not_mounted(&tx, path)?;
        if obj_type == PathObjType::Dir {
            Self::ensure_no_child_binding(&tx, path)?;
        }

        let now = Self::now_ts();
        let existing = Self::get_path_state_tx(&tx, path)?;
        if let Some((state, version)) = existing {
            if state == PathState::Working {
                return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
            }
            tx.execute(
                "UPDATE path_entry SET obj_id = ?1, obj_type = ?2, state = ?3, mtime = ?4, version = ?5 WHERE path = ?6",
                params![
                    obj_id.to_string(),
                    obj_type as i64,
                    PathState::Committed as i64,
                    now,
                    version + 1,
                    path
                ],
            )
            .map_err(|e| {
                warn!("FsMetaDb: update path failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
        } else {
            tx.execute(
                "INSERT INTO path_entry (path, obj_id, obj_type, state, mtime, ctime, version, meta) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    path,
                    obj_id.to_string(),
                    obj_type as i64,
                    PathState::Committed as i64,
                    now,
                    now,
                    1i64,
                    Option::<Vec<u8>>::None
                ],
            )
            .map_err(|e| {
                warn!("FsMetaDb: insert path failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
        }

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: add path commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn create_file(
        &self,
        path: &str,
        lease: WriteLease,
        expect_size: Option<u64>,
    ) -> NdnResult<FileBufferHandle> {
        let now = Self::now_ts();
        let buffer_id = format!("fb:{}:{}:{}", lease.session_id, lease.fence, now);
        self.create_file_with_buffer(path, lease.clone(), &buffer_id, expect_size)?;
        Ok(FileBufferHandle {
            path: path.to_string(),
            buffer_id,
            lease,
        })
    }

    pub fn create_file_with_buffer(
        &self,
        path: &str,
        lease: WriteLease,
        buffer_id: &str,
        expect_size: Option<u64>,
    ) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: create file tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Self::ensure_parent_dir_not_mounted(&tx, path)?;

        if let Some((state, _version)) = Self::get_path_state_tx(&tx, path)? {
            if state == PathState::Working {
                return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
            }
            return Err(NdnError::AlreadyExists(path.to_string()));
        }

        let now = Self::now_ts();

        tx.execute(
            "INSERT INTO path_entry (path, obj_id, obj_type, state, mtime, ctime, version, meta) VALUES (?1, NULL, NULL, ?2, ?3, ?4, ?5, ?6)",
            params![path, PathState::Working as i64, now, now, 1i64, Option::<Vec<u8>>::None],
        )
        .map_err(|e| {
            warn!("FsMetaDb: insert path_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "INSERT INTO working_entry (path, session_id, fence, lease_ttl, buffer_id, expect_size, state, create_time, update_time) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                path,
                lease.session_id,
                lease.fence,
                lease.lease_ttl,
                buffer_id,
                expect_size.map(|v| v as i64),
                WorkingState::Writing as i64,
                now,
                now
            ],
        )
        .map_err(|e| {
            warn!("FsMetaDb: insert working_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "INSERT INTO lease_entry (path, session_id, fence, lease_ttl, update_time) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![path, lease.session_id, lease.fence, lease.lease_ttl, now],
        )
        .map_err(|e| {
            warn!("FsMetaDb: insert lease_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: create file commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn create_dir(&self, path: &str, dir_obj_id: &ObjId) -> NdnResult<()> {
        self.add_committed_path(path, dir_obj_id, PathObjType::Dir)
    }

    pub fn append(&self, path: &str, lease: WriteLease, data: &[u8]) -> NdnResult<()> {
        let _ = data.len();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: append tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Self::check_lease_tx(&tx, path, &lease)?;

        let state = tx
            .query_row(
                "SELECT state FROM working_entry WHERE path = ?1",
                params![path],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: append query failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if state.is_none() {
            return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
        }

        let state = WorkingState::from_i64(state.unwrap())?;
        if state != WorkingState::Writing {
            return Err(NdnError::InvalidState("INVALID_STATE".to_string()));
        }

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: append commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn flush(&self, _fb: &FileBufferHandle) -> NdnResult<()> {
        Ok(())
    }

    pub fn close_file(&self, path: &str, lease: WriteLease) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: close file tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Self::check_lease_tx(&tx, path, &lease)?;

        let now = Self::now_ts();
        tx.execute(
            "UPDATE working_entry SET state = ?1, update_time = ?2 WHERE path = ?3",
            params![WorkingState::Closed as i64, now, path],
        )
        .map_err(|e| {
            warn!("FsMetaDb: update working_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "UPDATE lease_entry SET update_time = ?1 WHERE path = ?2",
            params![now, path],
        )
        .map_err(|e| {
            warn!("FsMetaDb: update lease_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: close file commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn close_file_by_buffer(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        self.close_file(&fb.path, fb.lease.clone())
    }

    pub fn cacl_name(
        &self,
        path: &str,
        obj_id: &ObjId,
        obj_type: PathObjType,
        lease: WriteLease,
    ) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: cacl_name tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Self::check_lease_tx(&tx, path, &lease)?;

        let row = tx
            .query_row(
                "SELECT state FROM working_entry WHERE path = ?1",
                params![path],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: query working_entry failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if row.is_none() {
            return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
        }

        let state = WorkingState::from_i64(row.unwrap())?;
        if state != WorkingState::Closed {
            return Err(NdnError::InvalidState("INVALID_STATE".to_string()));
        }

        let now = Self::now_ts();
        let version: i64 = tx
            .query_row(
                "SELECT version FROM path_entry WHERE path = ?1",
                params![path],
                |r| r.get(0),
            )
            .map_err(|e| {
                warn!("FsMetaDb: query path_entry failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        tx.execute(
            "UPDATE path_entry SET obj_id = ?1, obj_type = ?2, state = ?3, mtime = ?4, version = ?5 WHERE path = ?6",
            params![
                obj_id.to_string(),
                obj_type as i64,
                PathState::Committed as i64,
                now,
                version + 1,
                path
            ],
        )
        .map_err(|e| {
            warn!("FsMetaDb: update path_entry failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute("DELETE FROM working_entry WHERE path = ?1", params![path])
            .map_err(|e| {
                warn!("FsMetaDb: delete working_entry failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        tx.execute("DELETE FROM lease_entry WHERE path = ?1", params![path])
            .map_err(|e| {
                warn!("FsMetaDb: delete lease_entry failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: cacl_name commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    fn get_obj_materialize_info(
        &self,
        obj_id: &ObjId,
    ) -> NdnResult<(ObjMaterializeState, i64, Option<String>)> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT state, update_time, last_error FROM obj_materialize WHERE obj_id = ?1",
                params![obj_id.to_string()],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                warn!(
                    "FsMetaDb: get_obj_materialize_info failed! {}",
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        match row {
            Some((state, update_time, last_error)) => Ok((
                ObjMaterializeState::from_i64(state)?,
                update_time,
                last_error,
            )),
            None => Ok((ObjMaterializeState::NotPulled, 0, None)),
        }
    }

    pub fn list(&self, path: &str, pos: u32, page_size: u32) -> NdnResult<ListResult> {
        let conn = self.conn.lock().unwrap();
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        let mut stmt = conn
            .prepare(
                "SELECT path, obj_id, obj_type, state FROM path_entry WHERE path LIKE ?1 AND state = ?2 ORDER BY path",
            )
            .map_err(|e| {
                warn!("FsMetaDb: list prepare failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        let rows = stmt
            .query_map(
                params![format!("{}%", prefix), PathState::Committed as i64],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(|e| {
                warn!("FsMetaDb: list query failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        let mut entries = Vec::new();
        for row in rows {
            let (full_path, obj_id_str, obj_type, state) = row.map_err(|e| {
                warn!("FsMetaDb: list row failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            if !full_path.starts_with(&prefix) {
                continue;
            }
            let suffix = &full_path[prefix.len()..];
            if suffix.is_empty() || suffix.contains('/') {
                continue;
            }
            let obj_id = ObjId::new(&obj_id_str)?;
            let obj_type = PathObjType::from_i64(obj_type)?;
            let state = PathState::from_i64(state)?;
            entries.push(ListEntry {
                name: suffix.to_string(),
                path: full_path,
                obj_id,
                obj_type,
                state,
            });
        }

        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let start = pos as usize;
        let end = (start + page_size as usize).min(entries.len());
        let page_entries = if start < entries.len() {
            entries[start..end].to_vec()
        } else {
            Vec::new()
        };
        let has_more = end < entries.len();

        Ok(ListResult {
            entries: page_entries,
            next_pos: end as u32,
            has_more,
        })
    }

    pub fn pull(&self, path: &str, remote: PullContext) -> NdnResult<()> {
        let stat = self.stat(path)?;
        let obj_id = stat
            .obj_id
            .ok_or_else(|| NdnError::InvalidState("INVALID_STATE".to_string()))?;
        self.pull_by_objid(&obj_id, remote)
    }

    pub fn pull_by_objid(&self, obj_id: &ObjId, remote: PullContext) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        let now = Self::now_ts();
        conn.execute(
            "INSERT OR REPLACE INTO pull_task (obj_id, remote, update_time) VALUES (?1, ?2, ?3)",
            params![obj_id.to_string(), remote.remote, now],
        )
        .map_err(|e| {
            warn!("FsMetaDb: pull insert failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        self.set_obj_materialize_state(obj_id, ObjMaterializeState::Pulling, None)
    }

    pub fn stat(&self, path: &str) -> NdnResult<PathStat> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT obj_id, obj_type, state, version, ctime, mtime FROM path_entry WHERE path = ?1",
                params![path],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: stat failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if row.is_none() {
            return Err(NdnError::NotFound(path.to_string()));
        }

        let (obj_id_str, obj_type, state, version, ctime, mtime) = row.unwrap();
        let obj_id = match obj_id_str {
            Some(v) => Some(ObjId::new(&v)?),
            None => None,
        };
        let obj_type = match obj_type {
            Some(v) => Some(PathObjType::from_i64(v)?),
            None => None,
        };

        let working = if PathState::from_i64(state)? == PathState::Working {
            let wrow = conn
                .query_row(
                    "SELECT session_id, fence, lease_ttl, buffer_id, expect_size, state, create_time, update_time FROM working_entry WHERE path = ?1",
                    params![path],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, i64>(5)?,
                            r.get::<_, i64>(6)?,
                            r.get::<_, i64>(7)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    warn!("FsMetaDb: stat working_entry failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;

            wrow.map(|v| WorkingEntry {
                path: path.to_string(),
                session_id: v.0,
                fence: v.1,
                lease_ttl: v.2,
                buffer_id: v.3,
                expect_size: v.4.map(|s| s as u64),
                state: WorkingState::from_i64(v.5).unwrap_or(WorkingState::Writing),
                create_time: v.6,
                update_time: v.7,
            })
        } else {
            None
        };

        let (materialize_state, materialize_time, materialize_error) = match obj_id.as_ref() {
            Some(obj_id) => self.get_obj_materialize_info(obj_id)?,
            None => (ObjMaterializeState::NotPulled, 0, None),
        };

        Ok(PathStat {
            path: path.to_string(),
            state: PathState::from_i64(state)?,
            obj_id,
            obj_type,
            version,
            ctime,
            mtime,
            working,
            materialize_state,
            materialize_time,
            materialize_error,
        })
    }

    pub fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat> {
        let (materialize_state, materialize_time, materialize_error) =
            self.get_obj_materialize_info(obj_id)?;
        Ok(ObjStat {
            obj_id: obj_id.clone(),
            materialize_state,
            materialize_time,
            materialize_error,
        })
    }

    pub fn open_reader(&self, path: &str, opt: ReaderOptions) -> NdnResult<FsMetaReader> {
        let stat = self.stat(path)?;
        match stat.state {
            PathState::Committed => {
                let obj_id = stat
                    .obj_id
                    .ok_or_else(|| NdnError::InvalidState("INVALID_STATE".to_string()))?;
                if stat.materialize_state != ObjMaterializeState::Ready {
                    return Err(NdnError::InvalidState("NEED_PULL".to_string()));
                }
                Ok(FsMetaReader {
                    path: Some(path.to_string()),
                    obj_id,
                    obj_type: stat.obj_type,
                })
            }
            PathState::Working => {
                if !opt.allow_working {
                    return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
                }
                if let Some(working) = stat.working {
                    if let Some(session_id) = opt.session_id {
                        if session_id != working.session_id {
                            return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
                        }
                    }
                }
                Err(NdnError::InvalidState("PATH_BUSY".to_string()))
            }
            PathState::Empty => Err(NdnError::NotFound(path.to_string())),
        }
    }

    pub fn open_reader_by_id(
        &self,
        obj_id: &ObjId,
        _opt: ReaderOptions,
    ) -> NdnResult<FsMetaReader> {
        let (state, _, _) = self.get_obj_materialize_info(obj_id)?;
        if state != ObjMaterializeState::Ready {
            return Err(NdnError::InvalidState("NEED_PULL".to_string()));
        }
        Ok(FsMetaReader {
            path: None,
            obj_id: obj_id.clone(),
            obj_type: None,
        })
    }

    pub fn close_reader(&self, _reader: FsMetaReader) -> NdnResult<()> {
        Ok(())
    }

    pub fn get_object(&self, _obj_id: &ObjId) -> NdnResult<Value> {
        Err(NdnError::Unsupported(
            "get_object not implemented".to_string(),
        ))
    }

    pub fn delete(&self, path: &str) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: delete tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        if let Some((state, _)) = Self::get_path_state_tx(&tx, path)? {
            if state == PathState::Working {
                return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
            }
        } else {
            return Err(NdnError::NotFound(path.to_string()));
        }

        tx.execute("DELETE FROM path_entry WHERE path = ?1", params![path])
            .map_err(|e| {
                warn!("FsMetaDb: delete path_entry failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: delete commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn move_path(&self, old_path: &str, new_path: &str) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: move_path tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        if let Some((state, _)) = Self::get_path_state_tx(&tx, old_path)? {
            if state == PathState::Working {
                return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
            }
        } else {
            return Err(NdnError::NotFound(old_path.to_string()));
        }

        Self::ensure_parent_dir_not_mounted(&tx, new_path)?;

        let now = Self::now_ts();
        tx.execute(
            "UPDATE path_entry SET path = ?1, mtime = ?2, version = version + 1 WHERE path = ?3",
            params![new_path, now, old_path],
        )
        .map_err(|e| {
            warn!("FsMetaDb: move_path update failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: move_path commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn copy_path(&self, src: &str, dst: &str) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("FsMetaDb: copy_path tx failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        let row = tx
            .query_row(
                "SELECT obj_id, obj_type, state FROM path_entry WHERE path = ?1",
                params![src],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: copy_path query failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if row.is_none() {
            return Err(NdnError::NotFound(src.to_string()));
        }

        let (obj_id_str, obj_type, state) = row.unwrap();
        let state = PathState::from_i64(state)?;
        if state == PathState::Working {
            return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
        }

        Self::ensure_parent_dir_not_mounted(&tx, dst)?;

        let obj_id =
            obj_id_str.ok_or_else(|| NdnError::InvalidState("INVALID_STATE".to_string()))?;
        let obj_type =
            obj_type.ok_or_else(|| NdnError::InvalidState("INVALID_STATE".to_string()))?;
        let now = Self::now_ts();

        tx.execute(
            "INSERT INTO path_entry (path, obj_id, obj_type, state, mtime, ctime, version, meta) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![dst, obj_id, obj_type, PathState::Committed as i64, now, now, 1i64, Option::<Vec<u8>>::None],
        )
        .map_err(|e| {
            warn!("FsMetaDb: copy_path insert failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("FsMetaDb: copy_path commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }

    pub fn snapshot(&self, src: &str, dst: &str) -> NdnResult<()> {
        self.copy_path(src, dst)
    }

    pub fn erase_obj_by_id(&self, obj_id: &ObjId) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM pull_task WHERE obj_id = ?1",
            params![obj_id.to_string()],
        )
        .map_err(|e| {
            warn!(
                "FsMetaDb: erase_obj_by_id pull_task failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        self.set_obj_materialize_state(obj_id, ObjMaterializeState::NotPulled, None)
    }

    pub fn set_meta(&self, path: &str, meta: Vec<u8>) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE path_entry SET meta = ?1 WHERE path = ?2",
            params![meta, path],
        )
        .map_err(|e| {
            warn!("FsMetaDb: set_meta failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn get_meta(&self, path: &str) -> NdnResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let meta = conn
            .query_row(
                "SELECT meta FROM path_entry WHERE path = ?1",
                params![path],
                |r| r.get::<_, Option<Vec<u8>>>(0),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: get_meta failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        if meta.is_none() {
            return Err(NdnError::NotFound(path.to_string()));
        }

        Ok(meta.unwrap())
    }
}
