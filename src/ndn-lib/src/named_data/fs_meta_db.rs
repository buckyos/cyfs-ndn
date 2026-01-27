use crate::{NdnError, NdnResult, ObjId};
use buckyos_kit::buckyos_get_unix_timestamp;
use log::*;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
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

        Ok(PathStat {
            path: path.to_string(),
            state: PathState::from_i64(state)?,
            obj_id,
            obj_type,
            version,
            ctime,
            mtime,
            working,
        })
    }

    pub fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<PathStat> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT path FROM path_entry WHERE obj_id = ?1 LIMIT 1",
                params![obj_id.to_string()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| {
                warn!("FsMetaDb: stat_by_objid failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        match row {
            Some(path) => self.stat(&path),
            None => Err(NdnError::NotFound(obj_id.to_string())),
        }
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
