use crate::{NdnError, NdnResult, ObjId};
use buckyos_kit::buckyos_get_unix_timestamp;
use rand::random;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub trait ObjectGraphResolver: Send + Sync {
    fn collect_reachable(&self, root: &ObjId) -> NdnResult<Vec<ObjId>>;
}

pub trait ObjectDeleter: Send + Sync {
    fn erase_object(&self, obj_id: &ObjId) -> NdnResult<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathKind {
    File,
    Dir,
}

impl PathKind {
    fn to_i32(&self) -> i32 {
        match self {
            PathKind::File => 0,
            PathKind::Dir => 1,
        }
    }

    fn from_i32(v: i32) -> NdnResult<Self> {
        match v {
            0 => Ok(PathKind::File),
            1 => Ok(PathKind::Dir),
            _ => Err(NdnError::InvalidData(format!("invalid path kind: {}", v))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathState {
    Bound,
    Writing,
}

impl PathState {
    fn to_i32(&self) -> i32 {
        match self {
            PathState::Bound => 0,
            PathState::Writing => 1,
        }
    }

    fn from_i32(v: i32) -> NdnResult<Self> {
        match v {
            0 => Ok(PathState::Bound),
            1 => Ok(PathState::Writing),
            _ => Err(NdnError::InvalidData(format!("invalid path state: {}", v))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WriteLease {
    pub session_id: String,
    pub fence: u64,
    pub lease_expire_at: i64,
}

#[derive(Clone, Debug)]
pub struct FileBufferMeta {
    pub path: String,
    pub buffer_id: String,
    pub expect_size: u64,
    pub lease: WriteLease,
}

#[derive(Clone, Debug)]
pub struct PathStatus {
    pub path: String,
    pub kind: PathKind,
    pub state: PathState,
    pub obj_id: Option<ObjId>,
    pub write_open: Option<bool>,
    pub lease: Option<WriteLease>,
}

#[derive(Clone, Debug)]
pub struct PathEntry {
    pub path: String,
    pub kind: PathKind,
    pub state: PathState,
    pub obj_id: Option<ObjId>,
}

pub struct FsMetaDb {
    pub db_path: String,
    conn: Mutex<Connection>,
    resolver: Arc<dyn ObjectGraphResolver>,
}

impl FsMetaDb {
    pub fn new(db_path: String, resolver: Arc<dyn ObjectGraphResolver>) -> NdnResult<Self> {
        let conn = Connection::open(&db_path).map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS paths (
                path TEXT PRIMARY KEY,
                obj_id TEXT,
                path_type INTEGER NOT NULL,
                state INTEGER NOT NULL,
                write_open INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER,
                size INTEGER
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS writing (
                path TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                fence INTEGER NOT NULL,
                lease_expire_at INTEGER NOT NULL,
                buffer_id TEXT NOT NULL,
                expect_size INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS path_meta (
                path TEXT PRIMARY KEY,
                meta_json TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS obj_refcount (
                obj_id TEXT PRIMARY KEY,
                ref_count INTEGER NOT NULL,
                last_update INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS path_refset (
                path TEXT NOT NULL,
                obj_id TEXT NOT NULL,
                kind INTEGER NOT NULL,
                PRIMARY KEY(path, obj_id)
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_path_refset_obj ON path_refset(obj_id)",
            [],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        Ok(Self {
            db_path,
            conn: Mutex::new(conn),
            resolver,
        })
    }

    pub fn bind_file(&self, path: &str, obj_id: &ObjId) -> NdnResult<()> {
        self.bind_object(path, obj_id, PathKind::File)
    }

    pub fn bind_dir(&self, path: &str, obj_id: &ObjId) -> NdnResult<()> {
        self.bind_object(path, obj_id, PathKind::Dir)
    }

    pub fn unbind_path(&self, path: &str) -> NdnResult<()> {
        let path = normalize_path(path)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.ensure_path_exists(&tx, &path)?;
        self.update_refcounts_for_unbind(&tx, &path)?;

        tx.execute("DELETE FROM paths WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        tx.execute("DELETE FROM writing WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn rename_path(&self, old_path: &str, new_path: &str) -> NdnResult<()> {
        let old_path = normalize_path(old_path)?;
        let new_path = normalize_path(new_path)?;
        if old_path == new_path {
            return Ok(());
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.ensure_path_exists(&tx, &old_path)?;
        self.ensure_path_not_exists(&tx, &new_path)?;
        self.check_mount_exclusivity(&tx, &new_path, false)?;
        self.ensure_no_descendants(&tx, &new_path)?;

        tx.execute(
            "UPDATE paths SET path = ?1 WHERE path = ?2",
            params![new_path, old_path],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        tx.execute(
            "UPDATE writing SET path = ?1 WHERE path = ?2",
            params![new_path, old_path],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;
        tx.execute(
            "UPDATE path_refset SET path = ?1 WHERE path = ?2",
            params![new_path, old_path],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn copy_path(&self, src: &str, target: &str) -> NdnResult<()> {
        let src = normalize_path(src)?;
        let target = normalize_path(target)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let (obj_id_str, kind, state) = self.load_path_binding(&tx, &src)?;
        if state != PathState::Bound {
            return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
        }

        self.ensure_path_not_exists(&tx, &target)?;
        self.check_mount_exclusivity(&tx, &target, kind == PathKind::Dir)?;
        self.ensure_no_descendants(&tx, &target)?;

        let now = buckyos_get_unix_timestamp() as i64;
        tx.execute(
            "INSERT INTO paths (path, obj_id, path_type, state, write_open, mtime) VALUES (?1, ?2, ?3, ?4, 0, ?5)",
            params![target, obj_id_str, kind.to_i32(), state.to_i32(), now],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.copy_refset_and_inc_refcount(&tx, &src, &target)?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn list_children(&self, path: &str, pos: u32, page_size: u32) -> NdnResult<Vec<PathEntry>> {
        let path = normalize_path(path)?;
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let mut stmt = conn
            .prepare("SELECT path, obj_id, path_type, state FROM paths WHERE path LIKE ?1 ORDER BY path LIMIT ?2 OFFSET ?3")
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let rows = stmt
            .query_map(
                params![format!("{}%", prefix), page_size as i64, pos as i64],
                |row| {
                    let path: String = row.get(0)?;
                    let obj_id: Option<String> = row.get(1)?;
                    let kind: i32 = row.get(2)?;
                    let state: i32 = row.get(3)?;
                    Ok((path, obj_id, kind, state))
                },
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let mut items = Vec::new();
        for row in rows {
            let (child_path, obj_id, kind, state) =
                row.map_err(|e| NdnError::DbError(e.to_string()))?;
            if is_immediate_child(&path, &child_path) {
                let kind = PathKind::from_i32(kind)?;
                let state = PathState::from_i32(state)?;
                let obj_id = match obj_id {
                    Some(s) => Some(ObjId::new(&s)?),
                    None => None,
                };
                items.push(PathEntry {
                    path: child_path,
                    kind,
                    state,
                    obj_id,
                });
            }
        }

        Ok(items)
    }

    pub fn get_path_status(&self, path: &str) -> NdnResult<PathStatus> {
        let path = normalize_path(path)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let mut stmt = conn
            .prepare("SELECT path, obj_id, path_type, state, write_open FROM paths WHERE path = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let row = stmt
            .query_row(params![path], |row| {
                let path: String = row.get(0)?;
                let obj_id: Option<String> = row.get(1)?;
                let kind: i32 = row.get(2)?;
                let state: i32 = row.get(3)?;
                let write_open: i32 = row.get(4)?;
                Ok((path, obj_id, kind, state, write_open))
            })
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let (path, obj_id, kind, state, write_open) = match row {
            Some(v) => v,
            None => return Err(NdnError::NotFound("PATH_NOT_FOUND".to_string())),
        };
        let kind = PathKind::from_i32(kind)?;
        let state = PathState::from_i32(state)?;

        let lease = if state == PathState::Writing {
            self.load_write_lease(&conn, &path)?
        } else {
            None
        };

        Ok(PathStatus {
            path,
            kind,
            state,
            obj_id: obj_id.map(|s| ObjId::new(&s)).transpose()?,
            write_open: Some(write_open != 0),
            lease,
        })
    }

    pub fn create_dir(&self, path: &str, obj_id: &ObjId) -> NdnResult<()> {
        self.bind_object(path, obj_id, PathKind::Dir)
    }

    pub fn reserve_write(
        &self,
        path: &str,
        buffer_id: &str,
        expect_size: u64,
        lease_ttl_secs: i64,
    ) -> NdnResult<FileBufferMeta> {
        let path = normalize_path(path)?;
        let now = buckyos_get_unix_timestamp() as i64;
        let lease = WriteLease {
            session_id: format!("{}-{}", now, random::<u64>()),
            fence: now as u64,
            lease_expire_at: now + lease_ttl_secs,
        };

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.ensure_path_not_exists(&tx, &path)?;
        self.check_mount_exclusivity(&tx, &path, false)?;

        tx.execute(
            "INSERT INTO paths (path, obj_id, path_type, state, write_open, mtime) VALUES (?1, NULL, ?2, ?3, 1, ?4)",
            params![path, PathKind::File.to_i32(), PathState::Writing.to_i32(), now],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.execute(
            "INSERT INTO writing (path, session_id, fence, lease_expire_at, buffer_id, expect_size) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                path,
                lease.session_id,
                lease.fence as i64,
                lease.lease_expire_at,
                buffer_id,
                expect_size as i64
            ],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;

        Ok(FileBufferMeta {
            path,
            buffer_id: buffer_id.to_string(),
            expect_size,
            lease,
        })
    }

    pub fn seal_write(&self, path: &str, session_id: &str, fence: u64) -> NdnResult<()> {
        let path = normalize_path(path)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.ensure_writing_session(&tx, &path, session_id, fence)?;

        tx.execute(
            "UPDATE paths SET write_open = 0 WHERE path = ?1",
            params![path],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn commit_binding(
        &self,
        path: &str,
        obj_id: &ObjId,
        session_id: &str,
        fence: u64,
    ) -> NdnResult<()> {
        let path = normalize_path(path)?;
        let reachable = self.collect_refset(obj_id)?;

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        self.ensure_writing_session(&tx, &path, session_id, fence)?;
        self.ensure_write_sealed(&tx, &path)?;

        self.update_refcounts_for_bind(&tx, &path, obj_id, &reachable)?;

        let now = buckyos_get_unix_timestamp() as i64;
        tx.execute(
            "UPDATE paths SET obj_id = ?1, state = ?2, write_open = 0, mtime = ?3 WHERE path = ?4",
            params![obj_id.to_string(), PathState::Bound.to_i32(), now, path],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.execute("DELETE FROM writing WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    pub fn gc_list_zero_refs(&self, limit: usize) -> NdnResult<Vec<ObjId>> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT obj_id FROM obj_refcount 
                 WHERE ref_count = 0 AND NOT EXISTS (
                     SELECT 1 FROM path_refset WHERE path_refset.obj_id = obj_refcount.obj_id
                 )
                 LIMIT ?1",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let obj_id: String = row.get(0)?;
                Ok(obj_id)
            })
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let mut items = Vec::new();
        for row in rows {
            let obj_id = row.map_err(|e| NdnError::DbError(e.to_string()))?;
            items.push(ObjId::new(&obj_id)?);
        }

        Ok(items)
    }

    pub fn gc_sweep_zero_refs(
        &self,
        deleter: &dyn ObjectDeleter,
        limit: usize,
    ) -> NdnResult<Vec<ObjId>> {
        let targets = self.gc_list_zero_refs(limit)?;
        if targets.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for obj_id in &targets {
            deleter.erase_object(obj_id)?;
            tx.execute(
                "DELETE FROM obj_refcount WHERE obj_id = ?1",
                params![obj_id.to_string()],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(targets)
    }

    fn bind_object(&self, path: &str, obj_id: &ObjId, kind: PathKind) -> NdnResult<()> {
        let path = normalize_path(path)?;
        let reachable = self.collect_refset(obj_id)?;

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| NdnError::DbError("db lock poisoned".to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if self.path_exists(&tx, &path)? {
            self.ensure_not_writing(&tx, &path)?;
        }
        self.check_mount_exclusivity(&tx, &path, kind == PathKind::Dir)?;
        self.ensure_no_descendants(&tx, &path)?;

        self.update_refcounts_for_bind(&tx, &path, obj_id, &reachable)?;

        let now = buckyos_get_unix_timestamp() as i64;
        tx.execute(
            "INSERT INTO paths (path, obj_id, path_type, state, write_open, mtime) 
             VALUES (?1, ?2, ?3, ?4, 0, ?5)
             ON CONFLICT(path) DO UPDATE SET obj_id = excluded.obj_id, path_type = excluded.path_type, state = excluded.state, write_open = 0, mtime = excluded.mtime",
            params![path, obj_id.to_string(), kind.to_i32(), PathState::Bound.to_i32(), now],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.execute("DELETE FROM writing WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        tx.commit().map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(())
    }

    fn collect_refset(&self, obj_id: &ObjId) -> NdnResult<Vec<(String, i32)>> {
        let mut reachable = self.resolver.collect_reachable(obj_id)?;
        reachable.push(obj_id.clone());

        let mut seen = HashSet::new();
        let mut items = Vec::new();
        for item in reachable {
            let id = item.to_string();
            if !seen.insert(id.clone()) {
                continue;
            }

            let kind = if item == *obj_id { 0 } else { 1 };
            items.push((id, kind));
        }

        Ok(items)
    }

    fn update_refcounts_for_bind(
        &self,
        tx: &Transaction<'_>,
        path: &str,
        obj_id: &ObjId,
        new_set: &[(String, i32)],
    ) -> NdnResult<()> {
        let old_set = self.load_path_refset(tx, path)?;
        let new_ids: HashSet<String> = new_set.iter().map(|(id, _)| id.clone()).collect();

        let to_dec: Vec<String> = old_set.difference(&new_ids).cloned().collect();
        let to_inc: Vec<String> = new_ids.difference(&old_set).cloned().collect();

        tx.execute("DELETE FROM path_refset WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for (id, kind) in new_set {
            tx.execute(
                "INSERT INTO path_refset (path, obj_id, kind) VALUES (?1, ?2, ?3)",
                params![path, id, kind],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        }

        for id in to_dec {
            self.adjust_refcount(tx, &id, -1)?;
        }
        for id in to_inc {
            self.adjust_refcount(tx, &id, 1)?;
        }

        Ok(())
    }

    fn update_refcounts_for_unbind(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let old_set = self.load_path_refset(tx, path)?;
        tx.execute("DELETE FROM path_refset WHERE path = ?1", params![path])
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for id in old_set {
            self.adjust_refcount(tx, &id, -1)?;
        }

        Ok(())
    }

    fn copy_refset_and_inc_refcount(
        &self,
        tx: &Transaction<'_>,
        src: &str,
        target: &str,
    ) -> NdnResult<()> {
        let mut stmt = tx
            .prepare("SELECT obj_id, kind FROM path_refset WHERE path = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let rows = stmt
            .query_map(params![src], |row| {
                let obj_id: String = row.get(0)?;
                let kind: i32 = row.get(1)?;
                Ok((obj_id, kind))
            })
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        for row in rows {
            let (obj_id, kind) = row.map_err(|e| NdnError::DbError(e.to_string()))?;
            tx.execute(
                "INSERT INTO path_refset (path, obj_id, kind) VALUES (?1, ?2, ?3)",
                params![target, obj_id, kind],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
            self.adjust_refcount(tx, &obj_id, 1)?;
        }

        Ok(())
    }

    fn adjust_refcount(&self, tx: &Transaction<'_>, obj_id: &str, delta: i64) -> NdnResult<()> {
        if delta == 0 {
            return Ok(());
        }

        let now = buckyos_get_unix_timestamp() as i64;
        if delta > 0 {
            tx.execute(
                "INSERT INTO obj_refcount (obj_id, ref_count, last_update) VALUES (?1, ?2, ?3)
                 ON CONFLICT(obj_id) DO UPDATE SET ref_count = ref_count + ?2, last_update = ?3",
                params![obj_id, delta, now],
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;
            return Ok(());
        }

        let current: Option<i64> = tx
            .query_row(
                "SELECT ref_count FROM obj_refcount WHERE obj_id = ?1",
                params![obj_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let current = current
            .ok_or_else(|| NdnError::InvalidState(format!("ref_count missing for {}", obj_id)))?;
        let new_count = current + delta;
        if new_count < 0 {
            return Err(NdnError::InvalidState(format!(
                "ref_count underflow for {}",
                obj_id
            )));
        }

        tx.execute(
            "UPDATE obj_refcount SET ref_count = ?1, last_update = ?2 WHERE obj_id = ?3",
            params![new_count, now, obj_id],
        )
        .map_err(|e| NdnError::DbError(e.to_string()))?;

        Ok(())
    }

    fn path_exists(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<bool> {
        let exists: Option<i32> = tx
            .query_row(
                "SELECT 1 FROM paths WHERE path = ?1",
                params![path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        Ok(exists.is_some())
    }

    fn ensure_path_exists(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        if !self.path_exists(tx, path)? {
            return Err(NdnError::NotFound("PATH_NOT_FOUND".to_string()));
        }
        Ok(())
    }

    fn ensure_path_not_exists(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        if self.path_exists(tx, path)? {
            return Err(NdnError::AlreadyExists("PATH_ALREADY_EXISTS".to_string()));
        }
        Ok(())
    }

    fn ensure_not_writing(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let state: Option<i32> = tx
            .query_row(
                "SELECT state FROM paths WHERE path = ?1",
                params![path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if let Some(state) = state {
            if PathState::from_i32(state)? == PathState::Writing {
                return Err(NdnError::InvalidState("PATH_BUSY".to_string()));
            }
        }
        Ok(())
    }

    fn ensure_writing_session(
        &self,
        tx: &Transaction<'_>,
        path: &str,
        session_id: &str,
        fence: u64,
    ) -> NdnResult<()> {
        let row: Option<(String, i64)> = tx
            .query_row(
                "SELECT session_id, fence FROM writing WHERE path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let (db_session, db_fence) =
            row.ok_or_else(|| NdnError::InvalidState("PATH_BUSY".to_string()))?;
        if db_session != session_id || db_fence as u64 != fence {
            return Err(NdnError::InvalidState("LEASE_CONFLICT".to_string()));
        }
        Ok(())
    }

    fn ensure_write_sealed(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let write_open: Option<i32> = tx
            .query_row(
                "SELECT write_open FROM paths WHERE path = ?1",
                params![path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        match write_open {
            Some(0) => Ok(()),
            Some(_) => Err(NdnError::InvalidState("WRITE_NOT_SEALED".to_string())),
            None => Err(NdnError::NotFound("PATH_NOT_FOUND".to_string())),
        }
    }

    fn load_write_lease(&self, conn: &Connection, path: &str) -> NdnResult<Option<WriteLease>> {
        let row: Option<(String, i64, i64)> = conn
            .query_row(
                "SELECT session_id, fence, lease_expire_at FROM writing WHERE path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        Ok(row.map(|(session_id, fence, lease_expire_at)| WriteLease {
            session_id,
            fence: fence as u64,
            lease_expire_at,
        }))
    }

    fn load_path_binding(
        &self,
        tx: &Transaction<'_>,
        path: &str,
    ) -> NdnResult<(String, PathKind, PathState)> {
        let row: Option<(String, i32, i32)> = tx
            .query_row(
                "SELECT obj_id, path_type, state FROM paths WHERE path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let (obj_id, kind, state) =
            row.ok_or_else(|| NdnError::NotFound("PATH_NOT_FOUND".to_string()))?;
        Ok((
            obj_id,
            PathKind::from_i32(kind)?,
            PathState::from_i32(state)?,
        ))
    }

    fn load_path_refset(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<HashSet<String>> {
        let mut stmt = tx
            .prepare("SELECT obj_id FROM path_refset WHERE path = ?1")
            .map_err(|e| NdnError::DbError(e.to_string()))?;
        let rows = stmt
            .query_map(params![path], |row| {
                let obj_id: String = row.get(0)?;
                Ok(obj_id)
            })
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let mut set = HashSet::new();
        for row in rows {
            set.insert(row.map_err(|e| NdnError::DbError(e.to_string()))?);
        }
        Ok(set)
    }

    fn check_mount_exclusivity(
        &self,
        tx: &Transaction<'_>,
        path: &str,
        is_dir_binding: bool,
    ) -> NdnResult<()> {
        self.ensure_no_ancestor_dir(tx, path)?;
        if is_dir_binding {
            self.ensure_no_descendants(tx, path)?;
        }
        Ok(())
    }

    fn ensure_no_ancestor_dir(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        for parent in parent_paths(path) {
            let row: Option<i32> = tx
                .query_row(
                    "SELECT path_type FROM paths WHERE path = ?1",
                    params![parent],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| NdnError::DbError(e.to_string()))?;

            if let Some(kind) = row {
                if PathKind::from_i32(kind)? == PathKind::Dir {
                    return Err(NdnError::InvalidState("MOUNT_CONFLICT".to_string()));
                }
            }
        }
        Ok(())
    }

    fn ensure_no_descendants(&self, tx: &Transaction<'_>, path: &str) -> NdnResult<()> {
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        let row: Option<i32> = tx
            .query_row(
                "SELECT 1 FROM paths WHERE path LIKE ?1 LIMIT 1",
                params![format!("{}%", prefix)],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        if row.is_some() {
            return Err(NdnError::InvalidState("MOUNT_CONFLICT".to_string()));
        }
        Ok(())
    }
}

fn normalize_path(path: &str) -> NdnResult<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(NdnError::InvalidParam("empty path".to_string()));
    }

    let mut parts = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() {
            continue;
        }
        if part == "." || part == ".." {
            return Err(NdnError::InvalidParam("invalid path segment".to_string()));
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

fn parent_paths(path: &str) -> Vec<String> {
    if path == "/" {
        return Vec::new();
    }

    let mut parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let mut parents = Vec::new();
    while !parts.is_empty() {
        parts.pop();
        if parts.is_empty() {
            parents.push("/".to_string());
            break;
        }
        parents.push(format!("/{}", parts.join("/")));
    }
    parents
}

fn is_immediate_child(parent: &str, child: &str) -> bool {
    if parent == "/" {
        let parts: Vec<&str> = child.trim_start_matches('/').split('/').collect();
        return parts.len() == 1;
    }

    if !child.starts_with(parent) {
        return false;
    }
    let remainder = &child[parent.len()..];
    if !remainder.starts_with('/') {
        return false;
    }

    let tail = remainder.trim_start_matches('/');
    !tail.is_empty() && !tail.contains('/')
}
