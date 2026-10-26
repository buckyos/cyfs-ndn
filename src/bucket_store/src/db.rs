//! The `bucket-index` SQLite instance (§13). One process (the host) holds it;
//! every write runs in one transaction under the connection mutex.

use crate::error::{BucketError, BucketResult};
use crate::types::*;
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS buckets (
  bucket_id        TEXT PRIMARY KEY,
  kind             TEXT NOT NULL,
  data_policy      TEXT NOT NULL,
  scope            TEXT NOT NULL,
  appid            TEXT NOT NULL,
  owner            TEXT NOT NULL,
  name             TEXT NOT NULL,
  versioning       INTEGER NOT NULL DEFAULT 0,
  quota_bytes      INTEGER,
  max_objects      INTEGER,
  default_ttl_secs INTEGER,
  orphaned_at      INTEGER,
  lifecycle_json   TEXT NOT NULL DEFAULT '[]',
  version_seq      INTEGER NOT NULL DEFAULT 0,
  unique_bytes     INTEGER NOT NULL DEFAULT 0,
  key_bytes        INTEGER NOT NULL DEFAULT 0,
  key_count        INTEGER NOT NULL DEFAULT 0,
  version_count    INTEGER NOT NULL DEFAULT 0,
  snapshot_only_bytes INTEGER NOT NULL DEFAULT 0,
  last_warned_pct  INTEGER NOT NULL DEFAULT 0,
  created_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS buckets_owner ON buckets(owner, appid);
CREATE INDEX IF NOT EXISTS buckets_app ON buckets(appid, owner);

CREATE TABLE IF NOT EXISTS bindings (
  bucket_id        TEXT NOT NULL,
  key              TEXT NOT NULL,
  version          INTEGER NOT NULL,
  is_current       INTEGER NOT NULL,
  is_delete_marker INTEGER NOT NULL DEFAULT 0,
  obj_id           TEXT,
  obj_type         TEXT,
  size             INTEGER NOT NULL DEFAULT 0,
  etag_md5         TEXT,
  content_type     TEXT,
  metadata_json    TEXT,
  tags_json        TEXT,
  http_headers_json TEXT,
  expires_at       INTEGER,
  origin_json      TEXT,
  last_access_at   INTEGER,
  noncurrent_at    INTEGER,
  created_at       INTEGER NOT NULL,
  created_by       TEXT NOT NULL,
  PRIMARY KEY (bucket_id, key, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS bindings_current ON bindings(bucket_id, key) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS bindings_prefix ON bindings(bucket_id, is_current, key);
CREATE INDEX IF NOT EXISTS bindings_expire ON bindings(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS bindings_obj ON bindings(bucket_id, obj_id);
CREATE INDEX IF NOT EXISTS bindings_by_obj ON bindings(obj_id);

CREATE TABLE IF NOT EXISTS bucket_pins (
  bucket_id  TEXT NOT NULL,
  obj_id     TEXT NOT NULL,
  ref_count  INTEGER NOT NULL,
  state      TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket_id, obj_id)
);
CREATE INDEX IF NOT EXISTS bucket_pins_state ON bucket_pins(state, updated_at);

CREATE TABLE IF NOT EXISTS bucket_grants (
  grant_id     TEXT PRIMARY KEY,
  bucket_id    TEXT NOT NULL,
  grantee_kind TEXT NOT NULL,
  grantee_id   TEXT NOT NULL,
  prefix       TEXT NOT NULL DEFAULT '',
  actions      TEXT NOT NULL,
  expires_at   INTEGER,
  granted_by   TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS bucket_grants_lookup ON bucket_grants(bucket_id, grantee_kind, grantee_id);

CREATE TABLE IF NOT EXISTS bucket_access_tokens (
  token_id     TEXT PRIMARY KEY,
  token_hash   TEXT NOT NULL,
  bucket_id    TEXT NOT NULL,
  prefix       TEXT,
  key          TEXT,
  actions      TEXT NOT NULL,
  expires_at   INTEGER NOT NULL,
  max_uses     INTEGER,
  uses         INTEGER NOT NULL DEFAULT 0,
  bind_user_id TEXT,
  bind_app_id  TEXT,
  created_by   TEXT NOT NULL,
  created_at   INTEGER NOT NULL,
  revoked_at   INTEGER
);
CREATE INDEX IF NOT EXISTS bucket_access_tokens_bucket ON bucket_access_tokens(bucket_id, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS bucket_access_tokens_hash ON bucket_access_tokens(token_hash);

CREATE TABLE IF NOT EXISTS bucket_quotas (
  bucket_id        TEXT PRIMARY KEY,
  max_bytes        INTEGER,
  soft_bytes       INTEGER,
  max_objects      INTEGER,
  max_object_bytes INTEGER,
  warn_percent     TEXT NOT NULL DEFAULT '[80,95]',
  set_by           TEXT NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_quotas (
  pool_kind          TEXT NOT NULL,
  pool_id            TEXT NOT NULL,
  declared_max_bytes INTEGER,
  max_bytes          INTEGER,
  last_warned_pct    INTEGER NOT NULL DEFAULT 0,
  set_by             TEXT NOT NULL,
  updated_at         INTEGER NOT NULL,
  PRIMARY KEY (pool_kind, pool_id)
);

CREATE TABLE IF NOT EXISTS bucket_usage_daily (
  bucket_id     TEXT NOT NULL,
  day           INTEGER NOT NULL,
  added_bytes   INTEGER NOT NULL DEFAULT 0,
  removed_bytes INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_id, day)
);

CREATE TABLE IF NOT EXISTS bucket_snapshots (
  snapshot_id     TEXT PRIMARY KEY,
  bucket_id       TEXT NOT NULL,
  label           TEXT,
  root_obj_id     TEXT NOT NULL,
  manifest_obj_id TEXT NOT NULL,
  tree_obj_id     TEXT NOT NULL,
  key_count       INTEGER NOT NULL,
  unique_bytes    INTEGER NOT NULL,
  created_at      INTEGER NOT NULL,
  created_by      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bucket_snapshots_bucket ON bucket_snapshots(bucket_id, created_at);

-- Derived from the manifests: which objects each snapshot retains (snapshot_only_bytes).
CREATE TABLE IF NOT EXISTS bucket_snapshot_objects (
  snapshot_id TEXT NOT NULL,
  bucket_id   TEXT NOT NULL,
  obj_id      TEXT NOT NULL,
  size        INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, obj_id)
);
CREATE INDEX IF NOT EXISTS bucket_snapshot_objects_obj ON bucket_snapshot_objects(bucket_id, obj_id);

CREATE TABLE IF NOT EXISTS bucket_issues (
  bucket_id TEXT NOT NULL,
  subject   TEXT NOT NULL,
  kind      TEXT NOT NULL,
  detail    TEXT NOT NULL,
  at        INTEGER NOT NULL,
  PRIMARY KEY (bucket_id, subject, kind)
);

CREATE TABLE IF NOT EXISTS s3_access_keys (
  access_key_id TEXT PRIMARY KEY,
  secret        TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  appid         TEXT NOT NULL,
  scope_json    TEXT,
  expires_at    INTEGER,
  revoked_at    INTEGER,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS s3_access_keys_user ON s3_access_keys(user_id, appid);

CREATE TABLE IF NOT EXISTS s3_multipart_uploads (
  upload_id     TEXT PRIMARY KEY,
  bucket_id     TEXT NOT NULL,
  key           TEXT NOT NULL,
  metadata_json TEXT,
  created_by    TEXT NOT NULL,
  created_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS s3_multipart_parts (
  upload_id      TEXT NOT NULL,
  part_number    INTEGER NOT NULL,
  chunk_ids_json TEXT NOT NULL,
  size           INTEGER NOT NULL,
  md5            TEXT NOT NULL,
  PRIMARY KEY (upload_id, part_number)
);
"#;

pub struct IndexDb {
    conn: Mutex<Connection>,
}

impl IndexDb {
    pub fn open(path: &Path) -> BucketResult<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| BucketError::Db(format!("create {}: {e}", dir.display())))?;
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> BucketResult<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> BucketResult<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;
        Ok(IndexDb { conn: Mutex::new(conn) })
    }

    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> BucketResult<T>) -> BucketResult<T> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        f(&conn)
    }

    pub fn write<T>(&self, f: impl FnOnce(&Transaction) -> BucketResult<T>) -> BucketResult<T> {
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }
}

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

fn json_map(s: Option<String>) -> BTreeMap<String, String> {
    s.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
}

fn opt_json_map(m: &BTreeMap<String, String>) -> Option<String> {
    (!m.is_empty()).then(|| to_json(m))
}

pub(crate) const BUCKET_COLS: &str = "bucket_id, kind, data_policy, scope, appid, owner, name, versioning, quota_bytes, max_objects, \
     default_ttl_secs, orphaned_at, lifecycle_json, unique_bytes, key_bytes, key_count, version_count, snapshot_only_bytes, created_at";

pub(crate) fn bucket_from_row(row: &Row) -> rusqlite::Result<BucketInfo> {
    let parse_err = |i: usize, e: BucketError| rusqlite::Error::FromSqlConversionFailure(i, rusqlite::types::Type::Text, Box::new(e));
    let id: String = row.get(0)?;
    let bucket_id: BucketId = id.parse().map_err(|e| parse_err(0, e))?;
    let kind: String = row.get(1)?;
    let policy: String = row.get(2)?;
    let scope: String = row.get(3)?;
    let lifecycle: String = row.get(12)?;
    Ok(BucketInfo {
        bucket_id,
        kind: kind.parse().map_err(|e| parse_err(1, e))?,
        data_policy: policy.parse().map_err(|e| parse_err(2, e))?,
        scope: scope.parse().map_err(|e| parse_err(3, e))?,
        appid: row.get(4)?,
        owner: row.get(5)?,
        name: row.get(6)?,
        versioning: row.get::<_, i64>(7)? != 0,
        quota_bytes: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
        max_objects: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        default_ttl_secs: row.get::<_, Option<i64>>(10)?.map(|v| v as u64),
        orphaned_at: row.get(11)?,
        lifecycle: serde_json::from_str(&lifecycle).unwrap_or_default(),
        unique_bytes: row.get::<_, i64>(13)? as u64,
        key_bytes: row.get::<_, i64>(14)? as u64,
        key_count: row.get::<_, i64>(15)? as u64,
        version_count: row.get::<_, i64>(16)? as u64,
        snapshot_only_bytes: row.get::<_, i64>(17)? as u64,
        created_at: row.get(18)?,
    })
}

pub(crate) fn load_bucket(conn: &Connection, id: &BucketId) -> BucketResult<BucketInfo> {
    conn.query_row(&format!("SELECT {BUCKET_COLS} FROM buckets WHERE bucket_id = ?1"), params![id.to_string()], bucket_from_row)
        .optional()?
        .ok_or_else(|| BucketError::BucketNotFound(id.to_string()))
}

pub(crate) const BINDING_COLS: &str = "bucket_id, key, version, is_current, is_delete_marker, obj_id, obj_type, size, etag_md5, content_type, \
     metadata_json, tags_json, http_headers_json, expires_at, origin_json, last_access_at, created_at, created_by";

pub(crate) fn binding_from_row(row: &Row) -> rusqlite::Result<Binding> {
    let id: String = row.get(0)?;
    let bucket_id: BucketId = id
        .parse()
        .map_err(|e: BucketError| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
    let obj_id: Option<String> = row.get(5)?;
    let origin: Option<String> = row.get(14)?;
    Ok(Binding {
            bucket_id,
            key: row.get(1)?,
            version: row.get::<_, i64>(2)? as u64,
            is_current: row.get::<_, i64>(3)? != 0,
            is_delete_marker: row.get::<_, i64>(4)? != 0,
            etag: obj_id.clone().unwrap_or_default(),
            obj_id,
            obj_type: row.get(6)?,
            size: row.get::<_, i64>(7)? as u64,
            etag_md5: row.get(8)?,
            content_type: row.get(9)?,
            metadata: json_map(row.get(10)?),
            tags: json_map(row.get(11)?),
            http_headers: json_map(row.get(12)?),
            expires_at: row.get(13)?,
            origin: origin.and_then(|s| serde_json::from_str(&s).ok()),
            last_access_at: row.get(15)?,
            created_at: row.get(16)?,
            created_by: row.get(17)?,
    })
}

pub(crate) fn current_binding(conn: &Connection, id: &BucketId, key: &str) -> BucketResult<Option<Binding>> {
    Ok(conn
        .query_row(
            &format!("SELECT {BINDING_COLS} FROM bindings WHERE bucket_id = ?1 AND key = ?2 AND is_current = 1"),
            params![id.to_string(), key],
            binding_from_row,
        )
        .optional()?)
}

pub(crate) fn version_binding(conn: &Connection, id: &BucketId, key: &str, version: u64) -> BucketResult<Option<Binding>> {
    Ok(conn
        .query_row(
            &format!("SELECT {BINDING_COLS} FROM bindings WHERE bucket_id = ?1 AND key = ?2 AND version = ?3"),
            params![id.to_string(), key, version as i64],
            binding_from_row,
        )
        .optional()?)
}

pub(crate) fn insert_binding(tx: &Transaction, b: &Binding) -> BucketResult<()> {
    tx.execute(
        "INSERT INTO bindings (bucket_id, key, version, is_current, is_delete_marker, obj_id, obj_type, size, etag_md5,
            content_type, metadata_json, tags_json, http_headers_json, expires_at, origin_json, last_access_at, created_at, created_by)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
        params![
            b.bucket_id.to_string(),
            b.key,
            b.version as i64,
            b.is_current as i64,
            b.is_delete_marker as i64,
            b.obj_id,
            b.obj_type,
            b.size as i64,
            b.etag_md5,
            b.content_type,
            opt_json_map(&b.metadata),
            opt_json_map(&b.tags),
            opt_json_map(&b.http_headers),
            b.expires_at,
            b.origin.as_ref().map(to_json),
            b.last_access_at,
            b.created_at,
            b.created_by,
        ],
    )?;
    Ok(())
}

pub(crate) fn update_binding_meta(tx: &Transaction, b: &Binding) -> BucketResult<()> {
    tx.execute(
        "UPDATE bindings SET content_type = ?4, metadata_json = ?5, tags_json = ?6, http_headers_json = ?7
         WHERE bucket_id = ?1 AND key = ?2 AND version = ?3",
        params![
            b.bucket_id.to_string(),
            b.key,
            b.version as i64,
            b.content_type,
            opt_json_map(&b.metadata),
            opt_json_map(&b.tags),
            opt_json_map(&b.http_headers),
        ],
    )?;
    Ok(())
}

pub(crate) fn exec(tx: &Transaction, sql: &str, p: impl rusqlite::Params) -> BucketResult<usize> {
    Ok(tx.execute(sql, p)?)
}
