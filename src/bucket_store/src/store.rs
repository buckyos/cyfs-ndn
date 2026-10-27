//! `BucketStore`: bindings, versions, conditional writes, retention (pins) and
//! accounting (§3, §5, §6, §7.2, §8.4–§8.5).
//!
//! A write holds the bucket's lock, pins new objects in the named store first,
//! then commits the index transaction; unpins happen after the commit. A crash
//! in between leaves only an extra pin, which reconcile removes.

use crate::access::*;
use crate::db::*;
use crate::error::*;
use crate::events::{BucketEvent, EventSink, ObjectEventKind};
use crate::types::*;
use named_store::NamedDataMgr;
use ndn_lib::{load_named_obj, DirObject, FileObject, ObjId, OBJ_TYPE_DIR, OBJ_TYPE_FILE};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub struct BucketStore {
    pub(crate) db: IndexDb,
    pub(crate) ndm: NamedDataMgr,
    locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    events: std::sync::RwLock<Option<Arc<dyn EventSink>>>,
}

/// What a bound object is: type and logical size.
#[derive(Debug, Clone)]
pub(crate) struct ObjectInfo {
    pub obj_id: ObjId,
    pub obj_type: String,
    pub size: u64,
    pub mime: Option<String>,
}

/// A put that passed access, policy and content checks.
#[derive(Debug, Clone)]
pub(crate) struct Prepared {
    pub req: PutRequest,
    pub obj: ObjectInfo,
    pub decision: Decision,
}

pub(crate) enum Op {
    Put(Prepared),
    Delete { key: String, version: Option<u64>, if_match: Option<String>, decision: Decision },
}

/// Side effects collected inside a transaction, applied after commit.
#[derive(Default)]
pub(crate) struct Effects {
    pub unpin: Vec<String>,
    pub events: Vec<BucketEvent>,
}

#[derive(Default)]
struct Delta {
    unique: i64,
    key_bytes: i64,
    keys: i64,
    versions: i64,
    snapshot_only: i64,
}

impl BucketStore {
    pub fn new(db: IndexDb, ndm: NamedDataMgr) -> Arc<Self> {
        Arc::new(BucketStore { db, ndm, locks: Default::default(), events: Default::default() })
    }

    pub fn ndm(&self) -> &NamedDataMgr {
        &self.ndm
    }

    pub fn set_event_sink(&self, sink: Arc<dyn EventSink>) {
        *self.events.write().unwrap() = Some(sink);
    }

    pub(crate) fn emit(&self, events: Vec<BucketEvent>) {
        if events.is_empty() {
            return;
        }
        if let Some(sink) = self.events.read().unwrap().clone() {
            for e in events {
                sink.emit(e);
            }
        }
    }

    pub(crate) fn access(&self) -> Access<'_> {
        Access { db: &self.db }
    }

    pub(crate) async fn lock(&self, id: &BucketId) -> tokio::sync::OwnedMutexGuard<()> {
        let m = self.locks.lock().unwrap().entry(id.to_string()).or_default().clone();
        m.lock_owned().await
    }

    // ================= buckets =================

    pub fn bucket(&self, id: &BucketId) -> BucketResult<BucketInfo> {
        self.db.read(|c| load_bucket(c, id))
    }

    /// Creates a declared bucket, or brings the mutable declared fields of an
    /// existing one up to date. Kind, data policy and scope never change (§4.3).
    pub fn ensure_bucket(&self, id: &BucketId, decl: &BucketDecl) -> BucketResult<BucketInfo> {
        decl.validate(&id.name)?;
        if decl.scope != id.scope {
            return Err(BucketError::InvalidArgument(format!("{id} is declared with scope {}", decl.scope)));
        }
        let now = now_ms();
        let versioning = decl.versioning.unwrap_or(decl.data_policy.default_versioning());
        let quota = decl.quota_mib.map(|m| (m * MIB) as i64);
        let ttl = decl.default_ttl_days.map(|d| d as i64 * 86_400);
        let lifecycle = to_json(&decl.lifecycle);
        self.db.write(|tx| {
            match load_bucket(tx, id) {
                Ok(existing) => {
                    if existing.kind != decl.kind || existing.data_policy != decl.data_policy {
                        log::warn!(
                            "bucket {id} keeps {}/{}; the declaration now says {}/{} (declare a new bucket to change it)",
                            existing.kind, existing.data_policy, decl.kind, decl.data_policy
                        );
                        return Ok(existing);
                    }
                    exec(
                        tx,
                        "UPDATE buckets SET versioning = ?2, quota_bytes = ?3, max_objects = ?4, default_ttl_secs = ?5,
                            lifecycle_json = ?6, orphaned_at = NULL WHERE bucket_id = ?1",
                        params![id.to_string(), versioning as i64, quota, decl.max_objects.map(|v| v as i64), ttl, lifecycle],
                    )?;
                }
                Err(BucketError::BucketNotFound(_)) => {
                    exec(
                        tx,
                        "INSERT INTO buckets (bucket_id, kind, data_policy, scope, appid, owner, name, versioning, quota_bytes,
                            max_objects, default_ttl_secs, lifecycle_json, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                        params![
                            id.to_string(),
                            decl.kind.as_str(),
                            decl.data_policy.as_str(),
                            id.scope.as_str(),
                            id.appid,
                            id.owner,
                            id.name,
                            versioning as i64,
                            quota,
                            decl.max_objects.map(|v| v as i64),
                            ttl,
                            lifecycle,
                            now
                        ],
                    )?;
                    log::info!("bucket {id} created ({} {})", decl.kind, decl.data_policy);
                }
                Err(e) => return Err(e),
            }
            load_bucket(tx, id)
        })
    }

    /// The App pool limit declared by the app's `storage_quota_mib` (D17).
    pub fn set_declared_pool_quota(&self, appid: &str, owner: &str, max_bytes: Option<u64>) -> BucketResult<()> {
        let pool_id = format!("{appid}@{owner}");
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO pool_quotas (pool_kind, pool_id, declared_max_bytes, set_by, updated_at) VALUES ('app', ?1, ?2, 'declaration', ?3)
                 ON CONFLICT(pool_kind, pool_id) DO UPDATE SET declared_max_bytes = excluded.declared_max_bytes, updated_at = excluded.updated_at",
                params![pool_id, max_bytes.map(|v| v as i64), now_ms()],
            )?;
            Ok(())
        })
    }

    /// The app no longer declares the bucket (uninstalled, or dropped in an upgrade).
    pub fn mark_orphaned(&self, id: &BucketId) -> BucketResult<()> {
        self.db.write(|tx| {
            exec(tx, "UPDATE buckets SET orphaned_at = ?2 WHERE bucket_id = ?1 AND orphaned_at IS NULL", params![id.to_string(), now_ms()])?;
            Ok(())
        })
    }

    pub fn list_buckets(&self, owner: Option<&str>, appid: Option<&str>, scope: Option<BucketScope>) -> BucketResult<Vec<BucketInfo>> {
        self.db.read(|c| {
            let mut stmt = c.prepare(&format!(
                "SELECT {BUCKET_COLS} FROM buckets WHERE (?1 IS NULL OR owner = ?1) AND (?2 IS NULL OR appid = ?2)
                   AND (?3 IS NULL OR scope = ?3) ORDER BY bucket_id"
            ))?;
            let rows = stmt.query_map(params![owner, appid, scope.map(|s| s.as_str())], bucket_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// `bucket.stat` (needs `list`).
    pub async fn stat(&self, id: &BucketId, caller: &Caller) -> BucketResult<BucketInfo> {
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Prefix("")).await?;
        Ok(bucket)
    }

    pub async fn set_versioning(&self, id: &BucketId, caller: &Caller, on: bool) -> BucketResult<BucketInfo> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        if on && !bucket.data_policy.versioning_settable() {
            return Err(BucketError::PolicyViolation(format!("a {} bucket cannot be versioned", bucket.data_policy)));
        }
        self.db.write(|tx| {
            exec(tx, "UPDATE buckets SET versioning = ?2 WHERE bucket_id = ?1", params![id.to_string(), on as i64])?;
            load_bucket(tx, id)
        })
    }

    /// Unbinds everything and drops the bucket (§12.1). A documents/collected
    /// bucket goes only by its user in a sudo session.
    pub async fn delete_bucket(&self, id: &BucketId, caller: &Caller) -> BucketResult<()> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        if bucket.data_policy.user_owned() && !caller.sudo {
            return Err(BucketError::PermissionDenied("deleting user documents needs a sudo session".into()));
        }
        let _guard = self.lock(id).await;
        self.drop_bucket(&bucket).await
    }

    pub(crate) async fn drop_bucket(&self, bucket: &BucketInfo) -> BucketResult<()> {
        let id = bucket.bucket_id.to_string();
        let snapshots: Vec<String> = self.db.read(|c| {
            let mut stmt = c.prepare("SELECT snapshot_id FROM bucket_snapshots WHERE bucket_id = ?1")?;
            let rows = stmt.query_map(params![id], |r| r.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
        })?;
        self.ndm.unpin_owner(&bucket.bucket_id.pin_owner()).await.map_err(store_err)?;
        for s in &snapshots {
            self.ndm.unpin_owner(&format!("bucket-snapshot:{s}")).await.map_err(store_err)?;
        }
        self.db.write(|tx| {
            for table in [
                "bindings",
                "bucket_pins",
                "bucket_grants",
                "bucket_access_tokens",
                "bucket_quotas",
                "bucket_usage_daily",
                "bucket_snapshots",
                "bucket_snapshot_objects",
                "bucket_issues",
                "s3_multipart_uploads",
            ] {
                exec(tx, &format!("DELETE FROM {table} WHERE bucket_id = ?1"), params![id])?;
            }
            exec(tx, "DELETE FROM buckets WHERE bucket_id = ?1", params![id])?;
            Ok(())
        })?;
        log::info!("bucket {id} deleted");
        Ok(())
    }

    // ================= reads =================

    /// `bucket.get`: the binding (current, or a given version), never the content.
    pub async fn get(&self, id: &BucketId, caller: &Caller, key: &str, version: Option<u64>) -> BucketResult<Binding> {
        validate_key(key)?;
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::Read, Target::Key(key)).await?;
        self.lookup(&bucket, key, version)
    }

    pub(crate) fn lookup(&self, bucket: &BucketInfo, key: &str, version: Option<u64>) -> BucketResult<Binding> {
        let b = self.db.read(|c| match version {
            Some(v) => version_binding(c, &bucket.bucket_id, key, v),
            None => current_binding(c, &bucket.bucket_id, key),
        })?;
        match b {
            Some(b) if !b.is_delete_marker => Ok(b),
            _ => Err(BucketError::NoSuchKey(key.to_string())),
        }
    }

    /// `bucket.head`: the binding plus where its content is (§12.1).
    pub async fn head(&self, id: &BucketId, caller: &Caller, key: &str, version: Option<u64>) -> BucketResult<(Binding, Availability)> {
        let b = self.get(id, caller, key, version).await?;
        let bucket = self.bucket(id)?;
        Ok((b.clone(), self.availability(&bucket, &b).await))
    }

    pub(crate) async fn availability(&self, bucket: &BucketInfo, b: &Binding) -> Availability {
        let Some(obj) = b.obj_id.as_deref().and_then(|o| ObjId::new(o).ok()) else { return Availability::Local };
        if self.ndm.is_object_stored(&obj, None).await.unwrap_or(false) {
            return Availability::Local;
        }
        match bucket.data_policy {
            DataPolicy::Ambient => Availability::Remote,
            _ => Availability::Evicted,
        }
    }

    pub async fn list_objects(&self, id: &BucketId, caller: &Caller, req: &ListRequest) -> BucketResult<ListResult> {
        if !req.prefix.is_empty() && req.prefix.len() > MAX_KEY_BYTES {
            return Err(BucketError::InvalidKey("prefix too long".into()));
        }
        let bucket = self.bucket(id)?;
        let d = self.access().authorize(&bucket, caller, Action::List, Target::Prefix(&req.prefix)).await?;
        let limit = req.limit.unwrap_or(MAX_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT);
        let range = d.narrow_prefix.clone().unwrap_or_else(|| req.prefix.clone());
        let base = req.prefix.as_str();
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
        let upper = format!("{range}\u{10FFFF}");
        let mut cursor = req.start_after.clone().unwrap_or_default();
        if let Some(dl) = delimiter {
            if cursor.ends_with(dl) && cursor.starts_with(base) {
                cursor.push('\u{10FFFF}');
            }
        }
        if cursor.as_str() < range.as_str() {
            cursor = String::new();
        }
        let mut out = ListResult { items: Vec::new(), common_prefixes: Vec::new(), next_start_after: None };
        let mut last = String::new();
        'scan: loop {
            let rows: Vec<Binding> = self.db.read(|c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT {BINDING_COLS} FROM bindings WHERE bucket_id = ?1 AND is_current = 1 AND is_delete_marker = 0
                       AND key > ?2 AND key >= ?3 AND key < ?4 ORDER BY key LIMIT ?5"
                ))?;
                let rows = stmt.query_map(params![id.to_string(), cursor, range, upper, (limit + 1) as i64], binding_from_row)?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })?;
            if rows.is_empty() {
                break;
            }
            for b in rows {
                let common = delimiter.and_then(|dl| {
                    b.key.get(base.len()..).and_then(|rest| rest.find(dl)).map(|pos| b.key[..base.len() + pos + dl.len()].to_string())
                });
                if out.items.len() + out.common_prefixes.len() == limit {
                    out.next_start_after = Some(last);
                    break 'scan;
                }
                match common {
                    Some(cp) => {
                        last = cp.clone();
                        cursor = format!("{cp}\u{10FFFF}");
                        out.common_prefixes.push(cp);
                        continue 'scan;
                    }
                    None => {
                        last = b.key.clone();
                        cursor = b.key.clone();
                        out.items.push(b);
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn list_versions(&self, id: &BucketId, caller: &Caller, key: &str) -> BucketResult<Vec<Binding>> {
        validate_key(key)?;
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Key(key)).await?;
        self.db.read(|c| {
            let mut stmt = c.prepare(&format!("SELECT {BINDING_COLS} FROM bindings WHERE bucket_id = ?1 AND key = ?2 ORDER BY version DESC"))?;
            let rows = stmt.query_map(params![id.to_string(), key], binding_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Buckets and keys currently binding `obj_id` (holding proof (b), §6.4).
    pub(crate) fn bindings_of(&self, obj_id: &str, limit: usize) -> BucketResult<Vec<(BucketId, String)>> {
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT bucket_id, key FROM bindings WHERE obj_id = ?1 AND is_current = 1 LIMIT ?2")?;
            let rows = stmt.query_map(params![obj_id, limit as i64], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            let mut out = Vec::new();
            for row in rows {
                let (b, k) = row?;
                out.push((b.parse()?, k));
            }
            Ok(out)
        })
    }

    pub async fn check_access(&self, id: &BucketId, caller: &Caller, action: Action, key: Option<&str>, prefix: Option<&str>) -> BucketResult<AccessCheck> {
        let bucket = self.bucket(id)?;
        let target = match (key, prefix) {
            (Some(k), _) => Target::Key(k),
            (None, p) => Target::Prefix(p.unwrap_or("")),
        };
        let mut decision = self.access().authorize_any(&bucket, caller, &[action], target, false).await;
        if action == Action::Manage {
            decision = decision.and_then(|d| manage_rule(&bucket, caller).map(|_| d));
        }
        Ok(match decision {
            Ok(d) => AccessCheck { allowed: true, via: Some(d.via) },
            Err(_) => AccessCheck { allowed: false, via: None },
        })
    }

    // ================= writes =================

    pub(crate) async fn describe(&self, obj_id: &ObjId) -> BucketResult<ObjectInfo> {
        if obj_id.is_chunk() || obj_id.is_chunk_list() {
            return Err(BucketError::KindMismatch(format!("{} is raw content; bind a FileObject", obj_id.to_string())));
        }
        let body = self
            .ndm
            .get_object(obj_id)
            .await
            .map_err(|_| BucketError::ContentNotAvailable(obj_id.to_string()))?;
        let (size, mime) = match obj_id.obj_type.as_str() {
            OBJ_TYPE_FILE => {
                let f: FileObject = load_named_obj(&body).map_err(store_err)?;
                (f.size, f.meta.get("mime").and_then(|v| v.as_str()).map(String::from))
            }
            OBJ_TYPE_DIR => {
                let d: DirObject = load_named_obj(&body).map_err(store_err)?;
                (d.total_size, None)
            }
            _ => (body.len() as u64, None),
        };
        Ok(ObjectInfo { obj_id: obj_id.clone(), obj_type: obj_id.obj_type.clone(), size, mime })
    }

    fn check_kind(bucket: &BucketInfo, obj: &ObjectInfo) -> BucketResult<()> {
        let file_like = obj.obj_type == OBJ_TYPE_FILE || obj.obj_type == OBJ_TYPE_DIR;
        match (bucket.kind, file_like) {
            (BucketKind::Data, true) | (BucketKind::Object, false) => Ok(()),
            (BucketKind::Data, false) => Err(BucketError::KindMismatch(format!("a data bucket binds cyfile/cydir, not {}", obj.obj_type))),
            (BucketKind::Object, true) => Err(BucketError::KindMismatch(format!("an object bucket does not bind {}", obj.obj_type))),
        }
    }

    pub(crate) async fn content_ready(&self, bucket: &BucketInfo, obj: &ObjectInfo) -> BucketResult<()> {
        if bucket.data_policy == DataPolicy::Ambient {
            return Ok(());
        }
        if self.ndm.is_object_stored(&obj.obj_id, None).await.unwrap_or(false) {
            Ok(())
        } else {
            Err(BucketError::ContentNotAvailable(obj.obj_id.to_string()))
        }
    }

    /// §6.4: the caller holds the object, may read every object, or may read a key binding it.
    async fn check_holding(&self, caller: &Caller, obj_id: &str) -> BucketResult<()> {
        if caller.identity.holds(obj_id).await {
            return Ok(());
        }
        for (bucket_id, key) in self.bindings_of(obj_id, 16)? {
            let Ok(bucket) = self.bucket(&bucket_id) else { continue };
            if self.access().authorize_any(&bucket, caller, &[Action::Read], Target::Key(&key), false).await.is_ok() {
                return Ok(());
            }
        }
        Err(BucketError::NotHeld(obj_id.to_string()))
    }

    fn effective_max_object_bytes(&self, bucket: &BucketInfo) -> BucketResult<Option<u64>> {
        let ovr: Option<i64> = self.db.read(|c| {
            Ok(c.query_row("SELECT max_object_bytes FROM bucket_quotas WHERE bucket_id = ?1", params![bucket.bucket_id.to_string()], |r| r.get(0))
                .optional()?
                .flatten())
        })?;
        Ok(ovr.map(|v| v as u64).or(bucket.data_policy.default_max_object_bytes()))
    }

    /// Access, data-policy and content checks of one put (§4.5.3, §6.1).
    pub(crate) async fn prepare_put(&self, bucket: &BucketInfo, caller: &Caller, mut req: PutRequest, holding_checked: bool) -> BucketResult<Prepared> {
        validate_key(&req.key)?;
        let decision = self.access().authorize_any(bucket, caller, &[Action::Write, Action::Create], Target::Key(&req.key), true).await?;
        if decision.action == Action::Create {
            req.conditions = Conditions::create_only();
        }
        let obj_id = ObjId::new(&req.obj_id).map_err(|e| BucketError::InvalidArgument(format!("obj_id: {e}")))?;
        req.obj_id = obj_id.to_string();
        let obj = self.describe(&obj_id).await?;
        Self::check_kind(bucket, &obj)?;
        let meta_bytes: usize = req.metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
        if meta_bytes > MAX_METADATA_BYTES {
            return Err(BucketError::InvalidArgument(format!("user metadata is at most {MAX_METADATA_BYTES} bytes")));
        }
        if req.tags.len() > MAX_TAGS {
            return Err(BucketError::InvalidArgument(format!("at most {MAX_TAGS} tags")));
        }
        let policy = bucket.data_policy;
        let now = now_ms();
        if req.expires_at.is_some() && !policy.object_expiry_allowed() {
            return Err(BucketError::PolicyViolation(format!("objects in a {policy} bucket never expire")));
        }
        if req.expires_at.is_none() {
            req.expires_at = bucket.default_ttl_secs.map(|s| now + s as i64 * 1000);
        }
        if policy == DataPolicy::Temp {
            let max = now + TEMP_MAX_TTL_DAYS as i64 * DAY_MS;
            match req.expires_at {
                None => return Err(BucketError::PolicyViolation("a temp object needs expires_at".into())),
                Some(t) if t > max => return Err(BucketError::PolicyViolation(format!("a temp object expires within {TEMP_MAX_TTL_DAYS} days"))),
                _ => {}
            }
        }
        match &req.origin {
            Some(o) => o.validate()?,
            None if policy.origin_required() => return Err(BucketError::OriginRequired(format!("a {policy} binding records where it came from"))),
            None => {}
        }
        if let Some(limit) = self.effective_max_object_bytes(bucket)? {
            if obj.size > limit {
                return Err(BucketError::ObjectTooLarge { size: obj.size, limit });
            }
        }
        if !holding_checked {
            self.check_holding(caller, &req.obj_id).await?;
        }
        self.content_ready(bucket, &obj).await?;
        Ok(Prepared { req, obj, decision })
    }

    pub async fn put(&self, id: &BucketId, caller: &Caller, req: PutRequest) -> BucketResult<PutResult> {
        let bucket = self.bucket(id)?;
        let p = self.prepare_put(&bucket, caller, req, false).await?;
        match self.apply(&bucket, caller, vec![Op::Put(p)]).await?.pop() {
            Some(BatchResult::Put(r)) => Ok(r),
            _ => Err(BucketError::Internal("put produced no result".into())),
        }
    }

    pub(crate) async fn put_trusted(&self, bucket: &BucketInfo, caller: &Caller, req: PutRequest) -> BucketResult<PutResult> {
        let p = self.prepare_put(bucket, caller, req, true).await?;
        match self.apply(bucket, caller, vec![Op::Put(p)]).await?.pop() {
            Some(BatchResult::Put(r)) => Ok(r),
            _ => Err(BucketError::Internal("put produced no result".into())),
        }
    }

    pub async fn delete(&self, id: &BucketId, caller: &Caller, key: &str, version: Option<u64>, if_match: Option<String>) -> BucketResult<DeleteResult> {
        validate_key(key)?;
        let bucket = self.bucket(id)?;
        let decision = self.access().authorize(&bucket, caller, Action::Delete, Target::Key(key)).await?;
        match self.apply(&bucket, caller, vec![Op::Delete { key: key.into(), version, if_match, decision }]).await?.pop() {
            Some(BatchResult::Delete(r)) => Ok(r),
            _ => Err(BucketError::Internal("delete produced no result".into())),
        }
    }

    /// Deletes on behalf of the system (lifecycle, evicted cache entries).
    pub(crate) async fn delete_system(&self, bucket: &BucketInfo, key: &str, version: Option<u64>, kind: ObjectEventKind) -> BucketResult<DeleteResult> {
        let decision = Decision { action: Action::Delete, via: Via::Identity, narrow_prefix: None };
        let caller = Caller::system();
        let mut out = self.apply_with(bucket, &caller, vec![Op::Delete { key: key.into(), version, if_match: None, decision }], kind).await?;
        match out.pop() {
            Some(BatchResult::Delete(r)) => Ok(r),
            _ => Err(BucketError::Internal("delete produced no result".into())),
        }
    }

    pub async fn set_metadata(
        &self,
        id: &BucketId,
        caller: &Caller,
        key: &str,
        content_type: Option<String>,
        metadata: Option<BTreeMap<String, String>>,
        tags: Option<BTreeMap<String, String>>,
    ) -> BucketResult<Binding> {
        validate_key(key)?;
        let bucket = self.bucket(id)?;
        let decision = self.access().authorize(&bucket, caller, Action::Write, Target::Key(key)).await?;
        let _guard = self.lock(id).await;
        let mut b = self.lookup(&bucket, key, None)?;
        existing_rule(&bucket, caller, &decision, &b)?;
        if let Some(ct) = content_type {
            b.content_type = Some(ct);
        }
        if let Some(m) = metadata {
            if m.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > MAX_METADATA_BYTES {
                return Err(BucketError::InvalidArgument(format!("user metadata is at most {MAX_METADATA_BYTES} bytes")));
            }
            b.metadata = m;
        }
        if let Some(t) = tags {
            if t.len() > MAX_TAGS {
                return Err(BucketError::InvalidArgument(format!("at most {MAX_TAGS} tags")));
            }
            b.tags = t;
        }
        self.db.write(|tx| update_binding_meta(tx, &b))?;
        Ok(b)
    }

    /// `bucket.copy`: binds the source's object under the destination key (§6.3).
    pub async fn copy(&self, caller: &Caller, req: CopyRequest) -> BucketResult<PutResult> {
        let src_id: BucketId = req.src_bucket_id.parse()?;
        let dst_id: BucketId = req.dst_bucket_id.parse()?;
        validate_key(&req.src_key)?;
        let src_bucket = self.bucket(&src_id)?;
        self.access().authorize(&src_bucket, caller, Action::Read, Target::Key(&req.src_key)).await?;
        let src = self.lookup(&src_bucket, &req.src_key, req.src_version)?;
        let dst_bucket = self.bucket(&dst_id)?;
        let (content_type, metadata, tags) = match req.metadata_directive {
            MetadataDirective::Copy => (src.content_type.clone(), src.metadata.clone(), src.tags.clone()),
            MetadataDirective::Replace => (req.content_type.clone(), req.metadata.clone(), req.tags.clone()),
        };
        let put = PutRequest {
            key: req.dst_key.clone(),
            obj_id: src.obj_id.clone().unwrap_or_default(),
            content_type,
            metadata,
            tags,
            http_headers: src.http_headers.clone(),
            expires_at: None,
            origin: req.origin.clone().or_else(|| src.origin.clone()),
            etag_md5: src.etag_md5.clone(),
            conditions: req.conditions.clone(),
        };
        self.put_trusted(&dst_bucket, caller, put).await
    }

    /// `bucket.batch`: up to 100 puts/deletes in one bucket, all or nothing (§15.2).
    pub async fn batch(&self, id: &BucketId, caller: &Caller, ops: Vec<BatchOp>) -> BucketResult<Vec<BatchResult>> {
        if ops.is_empty() || ops.len() > MAX_BATCH_OPS {
            return Err(BucketError::InvalidArgument(format!("a batch has 1 to {MAX_BATCH_OPS} operations")));
        }
        let bucket = self.bucket(id)?;
        let mut prepared = Vec::with_capacity(ops.len());
        for op in ops {
            prepared.push(match op {
                BatchOp::Put(req) => Op::Put(self.prepare_put(&bucket, caller, req, false).await?),
                BatchOp::Delete { key, version, if_match } => {
                    validate_key(&key)?;
                    let decision = self.access().authorize(&bucket, caller, Action::Delete, Target::Key(&key)).await?;
                    Op::Delete { key, version, if_match, decision }
                }
            });
        }
        self.apply(&bucket, caller, prepared).await
    }

    pub(crate) async fn apply(&self, bucket: &BucketInfo, caller: &Caller, ops: Vec<Op>) -> BucketResult<Vec<BatchResult>> {
        self.apply_with(bucket, caller, ops, ObjectEventKind::ObjectRemoved).await
    }

    async fn apply_with(&self, bucket: &BucketInfo, caller: &Caller, ops: Vec<Op>, removal: ObjectEventKind) -> BucketResult<Vec<BatchResult>> {
        let _guard = self.lock(&bucket.bucket_id).await;
        // Re-read under the lock: versioning or quota may have changed.
        let bucket = self.bucket(&bucket.bucket_id)?;
        let owner = bucket.bucket_id.pin_owner();
        let mut pinned = Vec::new();
        if let Some(scope) = bucket.data_policy.pin_scope() {
            for op in &ops {
                let Op::Put(p) = op else { continue };
                if self.pin_held(&bucket.bucket_id, &p.req.obj_id)? || pinned.contains(&p.obj.obj_id) {
                    continue;
                }
                self.ndm
                    .pin(&p.obj.obj_id, &owner, scope, None)
                    .await
                    .map_err(|e| BucketError::Store(format!("pin {}: {e}", p.req.obj_id)))?;
                pinned.push(p.obj.obj_id.clone());
            }
        }
        let now = now_ms();
        let actor = caller.principal();
        let outcome = self.db.write(|tx| {
            let mut fx = Effects::default();
            let mut results = Vec::with_capacity(ops.len());
            for op in &ops {
                results.push(match op {
                    Op::Put(p) => BatchResult::Put(txn_put(tx, &bucket, caller, &actor, p, now, &mut fx)?),
                    Op::Delete { key, version, if_match, decision } => BatchResult::Delete(txn_delete(
                        tx, &bucket, caller, &actor, key, *version, if_match.as_deref(), decision, now, removal, &mut fx,
                    )?),
                });
            }
            Ok((results, fx))
        });
        let (results, fx) = match outcome {
            Ok(v) => v,
            Err(e) => {
                for obj in &pinned {
                    self.release_if_unreferenced(&bucket.bucket_id, obj).await;
                }
                return Err(e);
            }
        };
        for (op, result) in ops.iter().zip(&results) {
            match (op, result) {
                (Op::Put(p), BatchResult::Put(r)) => {
                    log::info!("bucket bind {} key={} obj_id={} version={} by {actor}", bucket.bucket_id, p.req.key, p.req.obj_id, r.version)
                }
                (Op::Delete { key, .. }, BatchResult::Delete(r)) => log::info!(
                    "bucket unbind {} key={key} version={:?} deleted={} marker={} by {actor}",
                    bucket.bucket_id,
                    r.version,
                    r.deleted,
                    r.delete_marker
                ),
                _ => {}
            }
        }
        self.finish_unpins(&bucket.bucket_id, &fx.unpin).await;
        self.emit(fx.events);
        self.check_quota_warnings(&bucket);
        Ok(results)
    }

    fn pin_held(&self, id: &BucketId, obj_id: &str) -> BucketResult<bool> {
        self.db.read(|c| {
            Ok(c.query_row(
                "SELECT 1 FROM bucket_pins WHERE bucket_id = ?1 AND obj_id = ?2 AND state = 'held'",
                params![id.to_string(), obj_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
        })
    }

    /// After a failed transaction: drop a pin this bucket no longer needs.
    async fn release_if_unreferenced(&self, id: &BucketId, obj: &ObjId) {
        let referenced = self.db.read(|c| count_refs(c, id, &obj.to_string())).unwrap_or(1);
        if referenced == 0 && !self.pin_held(id, &obj.to_string()).unwrap_or(true) {
            if let Err(e) = self.ndm.unpin(obj, &id.pin_owner()).await {
                log::warn!("release pin {} of {id}: {e}", obj.to_string());
            }
        }
    }

    /// Unpins objects whose last binding went away; rows stay `releasing` if the
    /// store call fails, and reconcile retries them.
    pub(crate) async fn finish_unpins(&self, id: &BucketId, objs: &[String]) {
        for obj in objs {
            let releasing = self
                .db
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT 1 FROM bucket_pins WHERE bucket_id = ?1 AND obj_id = ?2 AND state = 'releasing'",
                        params![id.to_string(), obj],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some())
                })
                .unwrap_or(false);
            if !releasing {
                continue;
            }
            let Ok(obj_id) = ObjId::new(obj) else { continue };
            match self.ndm.unpin(&obj_id, &id.pin_owner()).await {
                Ok(()) => {
                    let _ = self.db.write(|tx| {
                        exec(tx, "DELETE FROM bucket_pins WHERE bucket_id = ?1 AND obj_id = ?2 AND state = 'releasing'", params![id.to_string(), obj])
                    });
                }
                Err(e) => log::warn!("unpin {obj} of {id} failed, reconcile will retry: {e}"),
            }
        }
    }
}

pub(crate) fn count_refs(c: &Connection, id: &BucketId, obj_id: &str) -> BucketResult<u64> {
    Ok(c.query_row("SELECT COUNT(*) FROM bindings WHERE bucket_id = ?1 AND obj_id = ?2", params![id.to_string(), obj_id], |r| r.get::<_, i64>(0))? as u64)
}

fn in_snapshot(c: &Connection, id: &BucketId, obj_id: &str) -> BucketResult<bool> {
    Ok(c.query_row("SELECT 1 FROM bucket_snapshot_objects WHERE bucket_id = ?1 AND obj_id = ?2 LIMIT 1", params![id.to_string(), obj_id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn day_of(ms: i64) -> i64 {
    ms.div_euclid(DAY_MS)
}

fn add_daily(tx: &Transaction, id: &BucketId, now: i64, added: u64, removed: u64) -> BucketResult<()> {
    exec(
        tx,
        "INSERT INTO bucket_usage_daily (bucket_id, day, added_bytes, removed_bytes) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bucket_id, day) DO UPDATE SET added_bytes = added_bytes + excluded.added_bytes, removed_bytes = removed_bytes + excluded.removed_bytes",
        params![id.to_string(), day_of(now), added as i64, removed as i64],
    )?;
    Ok(())
}

/// The bucket starts (refs_before == 0) or keeps referencing `obj_id`.
fn add_ref(tx: &Transaction, bucket: &BucketInfo, obj_id: &str, size: u64, refs_before: u64, now: i64, delta: &mut Delta) -> BucketResult<()> {
    let id = &bucket.bucket_id;
    if refs_before == 0 {
        delta.unique += size as i64;
        add_daily(tx, id, now, size, 0)?;
        if in_snapshot(tx, id, obj_id)? {
            delta.snapshot_only -= size as i64;
        }
    }
    if bucket.data_policy.pin_scope().is_some() {
        exec(
            tx,
            "INSERT INTO bucket_pins (bucket_id, obj_id, ref_count, state, updated_at) VALUES (?1, ?2, ?3, 'held', ?4)
             ON CONFLICT(bucket_id, obj_id) DO UPDATE SET ref_count = excluded.ref_count, state = 'held', updated_at = excluded.updated_at",
            params![id.to_string(), obj_id, (refs_before + 1) as i64, now],
        )?;
    }
    Ok(())
}

/// A row referencing `obj_id` was removed; release when it was the last.
fn drop_ref(tx: &Transaction, bucket: &BucketInfo, obj_id: &str, size: u64, now: i64, delta: &mut Delta, fx: &mut Effects) -> BucketResult<()> {
    let id = &bucket.bucket_id;
    let refs = count_refs(tx, id, obj_id)?;
    let pinned = bucket.data_policy.pin_scope().is_some();
    if refs == 0 {
        delta.unique -= size as i64;
        add_daily(tx, id, now, 0, size)?;
        if in_snapshot(tx, id, obj_id)? {
            delta.snapshot_only += size as i64;
        }
        if pinned {
            exec(
                tx,
                "UPDATE bucket_pins SET ref_count = 0, state = 'releasing', updated_at = ?3 WHERE bucket_id = ?1 AND obj_id = ?2",
                params![id.to_string(), obj_id, now],
            )?;
            fx.unpin.push(obj_id.to_string());
        }
    } else if pinned {
        exec(tx, "UPDATE bucket_pins SET ref_count = ?3 WHERE bucket_id = ?1 AND obj_id = ?2", params![id.to_string(), obj_id, refs as i64])?;
    }
    Ok(())
}

fn apply_delta(tx: &Transaction, id: &BucketId, d: &Delta) -> BucketResult<()> {
    exec(
        tx,
        "UPDATE buckets SET unique_bytes = unique_bytes + ?2, key_bytes = key_bytes + ?3, key_count = key_count + ?4,
            version_count = version_count + ?5, snapshot_only_bytes = snapshot_only_bytes + ?6 WHERE bucket_id = ?1",
        params![id.to_string(), d.unique, d.key_bytes, d.keys, d.versions, d.snapshot_only],
    )?;
    Ok(())
}

fn next_version(tx: &Transaction, id: &BucketId) -> BucketResult<u64> {
    exec(tx, "UPDATE buckets SET version_seq = version_seq + 1 WHERE bucket_id = ?1", params![id.to_string()])?;
    Ok(tx.query_row("SELECT version_seq FROM buckets WHERE bucket_id = ?1", params![id.to_string()], |r| r.get::<_, i64>(0))? as u64)
}

fn etag_matches(b: &Binding, want: &str) -> bool {
    let want = want.trim().trim_start_matches("W/").trim_matches('"');
    if want == "*" {
        return true;
    }
    let normalized = ObjId::new(want).map(|o| o.to_string()).unwrap_or_else(|_| want.to_string());
    b.obj_id.as_deref() == Some(normalized.as_str()) || b.etag_md5.as_deref() == Some(want)
}

pub(crate) fn check_conditions(c: &Conditions, cur: Option<&Binding>) -> BucketResult<()> {
    let live = cur.filter(|b| !b.is_delete_marker);
    if let Some(v) = &c.if_none_match {
        if live.is_some_and(|b| etag_matches(b, v)) {
            return Err(BucketError::PreconditionFailed("the key already exists".into()));
        }
    }
    if let Some(m) = &c.if_match {
        if !live.is_some_and(|b| etag_matches(b, m)) {
            return Err(BucketError::PreconditionFailed("the current etag differs".into()));
        }
    }
    if let Some(v) = c.if_version {
        let ok = match live {
            Some(b) => b.version == v,
            None => v == 0,
        };
        if !ok {
            return Err(BucketError::PreconditionFailed("the current version differs".into()));
        }
    }
    Ok(())
}

pub(crate) struct Limits {
    pub max_bytes: Option<u64>,
    pub max_objects: u64,
}

pub(crate) fn bucket_limits(c: &Connection, bucket: &BucketInfo) -> BucketResult<Limits> {
    let ovr: Option<(Option<i64>, Option<i64>)> = c
        .query_row("SELECT max_bytes, max_objects FROM bucket_quotas WHERE bucket_id = ?1", params![bucket.bucket_id.to_string()], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()?;
    let (ovr_bytes, ovr_objects) = ovr.unwrap_or((None, None));
    Ok(Limits {
        max_bytes: ovr_bytes.map(|v| v as u64).or(bucket.quota_bytes),
        max_objects: ovr_objects.map(|v| v as u64).or(bucket.max_objects).unwrap_or(DEFAULT_MAX_OBJECTS),
    })
}

/// Effective limit of an App pool (`<appid>@<owner>`): admin override, else the declaration.
pub(crate) fn pool_limit(c: &Connection, kind: PoolKind, pool_id: &str) -> BucketResult<Option<u64>> {
    let row: Option<(Option<i64>, Option<i64>)> = c
        .query_row("SELECT max_bytes, declared_max_bytes FROM pool_quotas WHERE pool_kind = ?1 AND pool_id = ?2", params![kind.as_str(), pool_id], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()?;
    Ok(row.and_then(|(m, d)| m.or(d)).map(|v| v as u64))
}

/// Bytes a pool holds: bindings plus snapshot-only bytes of its buckets.
pub(crate) fn pool_usage(c: &Connection, kind: PoolKind, pool_id: &str) -> BucketResult<u64> {
    let policies = match kind {
        PoolKind::User => "('documents','collected')",
        PoolKind::App => "('app_data','temp')",
    };
    let v: i64 = match kind {
        PoolKind::User => c.query_row(
            &format!("SELECT COALESCE(SUM(unique_bytes + snapshot_only_bytes), 0) FROM buckets WHERE owner = ?1 AND data_policy IN {policies}"),
            params![pool_id],
            |r| r.get(0),
        )?,
        PoolKind::App => {
            let (appid, owner) = pool_id.rsplit_once('@').unwrap_or((pool_id, ZONE_OWNER));
            c.query_row(
                &format!("SELECT COALESCE(SUM(unique_bytes + snapshot_only_bytes), 0) FROM buckets WHERE appid = ?1 AND owner = ?2 AND data_policy IN {policies}"),
                params![appid, owner],
                |r| r.get(0),
            )?
        }
    };
    Ok(v as u64)
}

/// §8.4: a new distinct object must fit the bucket and the App pool. The user
/// pool only counts until res_pool carries a storage item (D14).
fn check_quota(tx: &Transaction, bucket: &BucketInfo, size: u64) -> BucketResult<()> {
    let row = load_bucket(tx, &bucket.bucket_id)?;
    let limits = bucket_limits(tx, bucket)?;
    if !bucket.data_policy.soft_quota() {
        if let Some(max) = limits.max_bytes {
            if row.unique_bytes + size > max {
                return Err(BucketError::QuotaExceeded(QuotaExceeded { level: QuotaLevel::Bucket, what: "bytes".into(), limit: max, usage: row.unique_bytes }));
            }
        }
    }
    if bucket.data_policy.pool() == Some(PoolKind::App) {
        let pool_id = bucket.bucket_id.pool_id(PoolKind::App);
        if let Some(max) = pool_limit(tx, PoolKind::App, &pool_id)? {
            let usage = pool_usage(tx, PoolKind::App, &pool_id)?;
            if usage + size > max {
                return Err(BucketError::QuotaExceeded(QuotaExceeded { level: QuotaLevel::Pool, what: "bytes".into(), limit: max, usage }));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn txn_put(tx: &Transaction, bucket: &BucketInfo, caller: &Caller, actor: &str, p: &Prepared, now: i64, fx: &mut Effects) -> BucketResult<PutResult> {
    let id = &bucket.bucket_id;
    let key = p.req.key.as_str();
    let cur = current_binding(tx, id, key)?;
    check_conditions(&p.req.conditions, cur.as_ref())?;
    if let Some(c) = cur.as_ref().filter(|c| !c.is_delete_marker) {
        existing_rule(bucket, caller, &p.decision, c)?;
    }
    let obj_id = p.req.obj_id.as_str();
    let refs = count_refs(tx, id, obj_id)?;
    let replaces_row = cur.is_some() && !bucket.versioning;
    if refs == 0 {
        check_quota(tx, bucket, p.obj.size)?;
    }
    if !replaces_row {
        let row = load_bucket(tx, id)?;
        let limits = bucket_limits(tx, bucket)?;
        if row.version_count + 1 > limits.max_objects {
            return Err(BucketError::QuotaExceeded(QuotaExceeded {
                level: QuotaLevel::Bucket,
                what: "objects".into(),
                limit: limits.max_objects,
                usage: row.version_count,
            }));
        }
    }
    let version = next_version(tx, id)?;
    let mut delta = Delta::default();
    if let Some(c) = &cur {
        exec(
            tx,
            "UPDATE bindings SET is_current = 0, noncurrent_at = ?4 WHERE bucket_id = ?1 AND key = ?2 AND version = ?3",
            params![id.to_string(), key, c.version as i64, now],
        )?;
        if !c.is_delete_marker {
            delta.keys -= 1;
            delta.key_bytes -= c.size as i64;
        }
    }
    add_ref(tx, bucket, obj_id, p.obj.size, refs, now, &mut delta)?;
    let content_type = p.req.content_type.clone().or_else(|| p.obj.mime.clone());
    let b = Binding {
        bucket_id: id.clone(),
        key: key.to_string(),
        version,
        is_current: true,
        is_delete_marker: false,
        obj_id: Some(obj_id.to_string()),
        obj_type: Some(p.obj.obj_type.clone()),
        size: p.obj.size,
        etag: obj_id.to_string(),
        etag_md5: p.req.etag_md5.clone(),
        content_type,
        metadata: p.req.metadata.clone(),
        tags: p.req.tags.clone(),
        http_headers: p.req.http_headers.clone(),
        expires_at: p.req.expires_at,
        origin: p.req.origin.clone(),
        last_access_at: matches!(bucket.data_policy, DataPolicy::Cache | DataPolicy::Ambient).then_some(now),
        created_at: now,
        created_by: actor.to_string(),
    };
    insert_binding(tx, &b)?;
    delta.keys += 1;
    delta.key_bytes += p.obj.size as i64;
    delta.versions += 1;
    if replaces_row {
        let c = cur.as_ref().unwrap();
        exec(tx, "DELETE FROM bindings WHERE bucket_id = ?1 AND key = ?2 AND version = ?3", params![id.to_string(), key, c.version as i64])?;
        delta.versions -= 1;
        if let Some(old) = &c.obj_id {
            drop_ref(tx, bucket, old, c.size, now, &mut delta, fx)?;
        }
    }
    apply_delta(tx, id, &delta)?;
    fx.events.push(BucketEvent::object(ObjectEventKind::ObjectCreated, &b, actor));
    Ok(PutResult { version, etag: obj_id.to_string(), created: cur.map_or(true, |c| c.is_delete_marker) })
}

#[allow(clippy::too_many_arguments)]
fn txn_delete(
    tx: &Transaction,
    bucket: &BucketInfo,
    caller: &Caller,
    actor: &str,
    key: &str,
    version: Option<u64>,
    if_match: Option<&str>,
    decision: &Decision,
    now: i64,
    removal: ObjectEventKind,
    fx: &mut Effects,
) -> BucketResult<DeleteResult> {
    let id = &bucket.bucket_id;
    let mut delta = Delta::default();
    let result = match version {
        None => {
            let Some(cur) = current_binding(tx, id, key)? else {
                return Ok(DeleteResult { deleted: false, version: None, delete_marker: false });
            };
            if let Some(m) = if_match {
                check_conditions(&Conditions { if_match: Some(m.into()), ..Default::default() }, Some(&cur))?;
            }
            if cur.is_delete_marker {
                return Ok(DeleteResult { deleted: false, version: None, delete_marker: false });
            }
            existing_rule(bucket, caller, decision, &cur)?;
            delta.keys -= 1;
            delta.key_bytes -= cur.size as i64;
            if bucket.versioning {
                exec(
                    tx,
                    "UPDATE bindings SET is_current = 0, noncurrent_at = ?4 WHERE bucket_id = ?1 AND key = ?2 AND version = ?3",
                    params![id.to_string(), key, cur.version as i64, now],
                )?;
                let v = next_version(tx, id)?;
                let marker = Binding {
                    bucket_id: id.clone(),
                    key: key.to_string(),
                    version: v,
                    is_current: true,
                    is_delete_marker: true,
                    obj_id: None,
                    obj_type: None,
                    size: 0,
                    etag: String::new(),
                    etag_md5: None,
                    content_type: None,
                    metadata: Default::default(),
                    tags: Default::default(),
                    http_headers: Default::default(),
                    expires_at: None,
                    origin: None,
                    last_access_at: None,
                    created_at: now,
                    created_by: actor.to_string(),
                };
                insert_binding(tx, &marker)?;
                delta.versions += 1;
                fx.events.push(BucketEvent::object(removal, &cur, actor));
                DeleteResult { deleted: true, version: Some(v), delete_marker: true }
            } else {
                exec(tx, "DELETE FROM bindings WHERE bucket_id = ?1 AND key = ?2 AND version = ?3", params![id.to_string(), key, cur.version as i64])?;
                delta.versions -= 1;
                if let Some(obj) = &cur.obj_id {
                    drop_ref(tx, bucket, obj, cur.size, now, &mut delta, fx)?;
                }
                fx.events.push(BucketEvent::object(removal, &cur, actor));
                DeleteResult { deleted: true, version: Some(cur.version), delete_marker: false }
            }
        }
        Some(v) => {
            let Some(row) = version_binding(tx, id, key, v)? else {
                return Err(BucketError::NoSuchKey(format!("{key} version {v}")));
            };
            if let Some(m) = if_match {
                check_conditions(&Conditions { if_match: Some(m.into()), ..Default::default() }, Some(&row))?;
            }
            if !row.is_delete_marker {
                existing_rule(bucket, caller, decision, &row)?;
            }
            exec(tx, "DELETE FROM bindings WHERE bucket_id = ?1 AND key = ?2 AND version = ?3", params![id.to_string(), key, v as i64])?;
            delta.versions -= 1;
            if row.is_current {
                if !row.is_delete_marker {
                    delta.keys -= 1;
                    delta.key_bytes -= row.size as i64;
                }
                let promoted: Option<(i64, i64, i64)> = tx
                    .query_row(
                        "SELECT version, is_delete_marker, size FROM bindings WHERE bucket_id = ?1 AND key = ?2 ORDER BY version DESC LIMIT 1",
                        params![id.to_string(), key],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                if let Some((pv, marker, size)) = promoted {
                    exec(
                        tx,
                        "UPDATE bindings SET is_current = 1, noncurrent_at = NULL WHERE bucket_id = ?1 AND key = ?2 AND version = ?3",
                        params![id.to_string(), key, pv],
                    )?;
                    if marker == 0 {
                        delta.keys += 1;
                        delta.key_bytes += size;
                    }
                }
            }
            if let Some(obj) = &row.obj_id {
                drop_ref(tx, bucket, obj, row.size, now, &mut delta, fx)?;
                fx.events.push(BucketEvent::object(removal, &row, actor));
            }
            DeleteResult { deleted: true, version: Some(v), delete_marker: row.is_delete_marker }
        }
    };
    apply_delta(tx, id, &delta)?;
    Ok(result)
}
