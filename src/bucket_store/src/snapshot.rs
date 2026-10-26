//! §10: snapshots (manifest = authority, tree = derived view), restore,
//! reference verification. A snapshot root is a two-entry DirObject
//! `{manifest, tree}` pinned as `bucket-snapshot:<snapshot_id>`.

use crate::access::*;
use crate::content::{load_file, read_file, store_bytes};
use crate::db::*;
use crate::error::*;
use crate::store::{count_refs, BucketStore};
use crate::types::*;
use named_store::{NamedDataMgr, PinScope};
use ndn_lib::{load_named_obj, DirObject, ObjId, SimpleMapItem, OBJ_TYPE_DIR, OBJ_TYPE_FILE};
use rand::RngCore;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const MANIFEST_FORMAT: &str = "bucket-manifest/1";
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub key: String,
    pub version: u64,
    pub is_current: bool,
    pub obj_id: String,
    pub obj_type: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag_md5: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub http_headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    pub created_at: i64,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: String,
    pub bucket_id: BucketId,
    pub kind: BucketKind,
    pub data_policy: DataPolicy,
    pub created_at: i64,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub bucket_id: BucketId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub root_obj_id: String,
    pub manifest_obj_id: String,
    pub tree_obj_id: String,
    pub key_count: u64,
    pub unique_bytes: u64,
    pub snapshot_only_bytes: u64,
    pub created_at: i64,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reference {
    Key {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    Object {
        obj_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissingEntry {
    pub key: String,
    pub version: u64,
    pub obj_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreReport {
    pub restored: u64,
    pub missing: Vec<MissingEntry>,
}

fn snapshot_owner(snapshot_id: &str) -> String {
    format!("bucket-snapshot:{snapshot_id}")
}

/// One directory level of the derived tree.
#[derive(Default)]
struct TreeNode {
    leaves: BTreeMap<String, (ObjId, u64)>,
    dirs: BTreeMap<String, TreeNode>,
}

impl TreeNode {
    fn insert(&mut self, key: &str, obj: ObjId, size: u64) {
        let segs: Vec<&str> = key.split('/').map(|s| if s.is_empty() { "~" } else { s }).collect();
        let (leaf, dirs) = segs.split_last().unwrap();
        let mut node = self;
        for d in dirs {
            node = node.dirs.entry(d.to_string()).or_default();
        }
        node.leaves.insert(leaf.to_string(), (obj, size));
    }
}

/// Stores the tree bottom-up: a key `a` next to a prefix `a/` becomes `a~file`,
/// later clashes get `~1`, `~2`… (§10.1). Returns the DirObject id and total size.
fn store_tree<'a>(
    ndm: &'a NamedDataMgr,
    name: &'a str,
    node: &'a TreeNode,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = BucketResult<(ObjId, u64)>> + Send + 'a>> {
    Box::pin(async move {
        let mut dir = DirObject::new(Some(name.to_string()));
        let mut used: BTreeSet<String> = BTreeSet::new();
        for (sub_name, sub) in &node.dirs {
            let (id, size) = store_tree(ndm, sub_name, sub).await?;
            dir.add_directory(sub_name.clone(), id, size).map_err(store_err)?;
            used.insert(sub_name.clone());
        }
        for (leaf, (obj, size)) in &node.leaves {
            let mut entry = if used.contains(leaf) { format!("{leaf}~file") } else { leaf.clone() };
            let mut n = 1;
            while used.contains(&entry) || (node.leaves.contains_key(&entry) && entry != *leaf) {
                entry = format!("{leaf}~{n}");
                n += 1;
            }
            used.insert(entry.clone());
            if obj.obj_type == OBJ_TYPE_DIR {
                dir.add_directory(entry, obj.clone(), *size).map_err(store_err)?;
            } else {
                if obj.obj_type == OBJ_TYPE_FILE {
                    dir.file_count += 1;
                    dir.file_size += size;
                }
                dir.total_size += size;
                dir.object_map.insert(entry, SimpleMapItem::ObjId(obj.clone()));
            }
        }
        let (id, _) = dir.gen_obj_id().map_err(store_err)?;
        let body = serde_json::to_string(&dir)?;
        ndm.put_object(&id, &body).await.map_err(store_err)?;
        Ok((id, dir.total_size))
    })
}

fn random_id() -> String {
    let mut buf = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("s_{}", hex::encode(buf))
}

impl BucketStore {
    fn manifest_entries(&self, id: &BucketId, include_noncurrent: bool) -> BucketResult<Vec<ManifestEntry>> {
        self.db.read(|c| {
            let mut stmt = c.prepare(&format!(
                "SELECT {BINDING_COLS} FROM bindings WHERE bucket_id = ?1 AND is_delete_marker = 0 AND (is_current = 1 OR ?2)
                 ORDER BY key, version"
            ))?;
            let rows = stmt.query_map(params![id.to_string(), include_noncurrent], binding_from_row)?;
            let mut out = Vec::new();
            for row in rows {
                let b = row?;
                out.push(ManifestEntry {
                    key: b.key,
                    version: b.version,
                    is_current: b.is_current,
                    obj_id: b.obj_id.unwrap_or_default(),
                    obj_type: b.obj_type.unwrap_or_default(),
                    size: b.size,
                    etag_md5: b.etag_md5,
                    content_type: b.content_type,
                    metadata: b.metadata,
                    tags: b.tags,
                    http_headers: b.http_headers,
                    expires_at: b.expires_at,
                    origin: b.origin,
                    created_at: b.created_at,
                    created_by: b.created_by,
                });
            }
            Ok(out)
        })
    }

    /// `bucket.snapshot` (§10.1).
    pub async fn snapshot(&self, id: &BucketId, caller: &Caller, label: Option<String>, include_noncurrent: bool) -> BucketResult<SnapshotInfo> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        let entries = {
            let _guard = self.lock(id).await;
            self.manifest_entries(id, include_noncurrent)?
        };
        let now = now_ms();
        let manifest = Manifest {
            format: MANIFEST_FORMAT.into(),
            bucket_id: id.clone(),
            kind: bucket.kind,
            data_policy: bucket.data_policy,
            created_at: now,
            entries,
        };
        let bytes = serde_json::to_vec(&manifest)?;
        let (manifest_file, manifest_id) = store_bytes(&self.ndm, "manifest.json", &bytes, Some("application/json")).await?;
        let mut tree = TreeNode::default();
        let mut objects: HashMap<String, u64> = HashMap::new();
        for e in &manifest.entries {
            let obj = ObjId::new(&e.obj_id).map_err(store_err)?;
            if e.is_current {
                tree.insert(&e.key, obj, e.size);
            }
            objects.insert(e.obj_id.clone(), e.size);
        }
        let (tree_id, tree_size) = store_tree(&self.ndm, "tree", &tree).await?;
        let mut root = DirObject::new(Some(format!("{}@{}", id.name, now)));
        root.add_file("manifest".into(), serde_json::to_value(&manifest_file)?, manifest_file.size).map_err(store_err)?;
        root.add_directory("tree".into(), tree_id.clone(), tree_size).map_err(store_err)?;
        let (root_id, _) = root.gen_obj_id().map_err(store_err)?;
        self.ndm.put_object(&root_id, &serde_json::to_string(&root)?).await.map_err(store_err)?;
        let snapshot_id = random_id();
        self.ndm
            .pin(&root_id, &snapshot_owner(&snapshot_id), PinScope::Recursive, None)
            .await
            .map_err(|e| BucketError::Store(format!("pin snapshot root: {e}")))?;
        let key_count = manifest.entries.iter().filter(|e| e.is_current).count() as u64;
        let unique: u64 = objects.values().sum();
        let creator = caller.principal();
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO bucket_snapshots (snapshot_id, bucket_id, label, root_obj_id, manifest_obj_id, tree_obj_id, key_count, unique_bytes, created_at, created_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    snapshot_id,
                    id.to_string(),
                    label,
                    root_id.to_string(),
                    manifest_id.to_string(),
                    tree_id.to_string(),
                    key_count as i64,
                    unique as i64,
                    now,
                    creator
                ],
            )?;
            for (obj, size) in &objects {
                exec(
                    tx,
                    "INSERT OR IGNORE INTO bucket_snapshot_objects (snapshot_id, bucket_id, obj_id, size) VALUES (?1, ?2, ?3, ?4)",
                    params![snapshot_id, id.to_string(), obj, *size as i64],
                )?;
            }
            Ok(())
        })?;
        // Objects unbound between reading the entries and here are snapshot-only now.
        self.recompute_usage(id)?;
        log::info!("bucket {id} snapshot {snapshot_id} root {} ({key_count} keys)", root_id.to_string());
        self.snapshot_info(&snapshot_id)
    }

    fn snapshot_info(&self, snapshot_id: &str) -> BucketResult<SnapshotInfo> {
        let mut list = self.snapshots_where("snapshot_id = ?1", snapshot_id)?;
        list.pop().ok_or_else(|| BucketError::InvalidArgument(format!("no snapshot {snapshot_id}")))
    }

    fn snapshots_where(&self, cond: &str, arg: &str) -> BucketResult<Vec<SnapshotInfo>> {
        self.db.read(|c| {
            let mut stmt = c.prepare(&format!(
                "SELECT s.snapshot_id, s.bucket_id, s.label, s.root_obj_id, s.manifest_obj_id, s.tree_obj_id, s.key_count, s.unique_bytes,
                        s.created_at, s.created_by,
                        (SELECT COALESCE(SUM(o.size), 0) FROM bucket_snapshot_objects o WHERE o.snapshot_id = s.snapshot_id
                           AND o.obj_id NOT IN (SELECT obj_id FROM bindings b WHERE b.bucket_id = s.bucket_id AND b.obj_id IS NOT NULL))
                 FROM bucket_snapshots s WHERE {cond} ORDER BY s.created_at"
            ))?;
            let rows = stmt.query_map(params![arg], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, String>(9)?,
                    r.get::<_, i64>(10)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (snapshot_id, bucket_id, label, root, manifest, tree, keys, unique, created_at, created_by, only) = row?;
                out.push(SnapshotInfo {
                    snapshot_id,
                    bucket_id: bucket_id.parse()?,
                    label,
                    root_obj_id: root,
                    manifest_obj_id: manifest,
                    tree_obj_id: tree,
                    key_count: keys as u64,
                    unique_bytes: unique as u64,
                    snapshot_only_bytes: only as u64,
                    created_at,
                    created_by,
                });
            }
            Ok(out)
        })
    }

    pub async fn list_snapshots(&self, id: &BucketId, caller: &Caller) -> BucketResult<Vec<SnapshotInfo>> {
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Prefix("")).await?;
        self.snapshots_where("s.bucket_id = ?1", &id.to_string())
    }

    /// The snapshot of a bucket, for callers that already authorized (export).
    pub async fn snapshot_of(&self, id: &BucketId, caller: &Caller, snapshot_id: &str) -> BucketResult<SnapshotInfo> {
        let info = self.snapshot_info(snapshot_id)?;
        if info.bucket_id != *id {
            return Err(BucketError::InvalidArgument(format!("snapshot {snapshot_id} is not of {id}")));
        }
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::Read, Target::Prefix("")).await?;
        Ok(info)
    }

    pub async fn delete_snapshot(&self, id: &BucketId, caller: &Caller, snapshot_id: &str) -> BucketResult<()> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        let info = self.snapshot_info(snapshot_id)?;
        if info.bucket_id != *id {
            return Err(BucketError::InvalidArgument(format!("snapshot {snapshot_id} is not of {id}")));
        }
        self.ndm.unpin_owner(&snapshot_owner(snapshot_id)).await.map_err(store_err)?;
        let _guard = self.lock(id).await;
        self.db.write(|tx| {
            exec(tx, "DELETE FROM bucket_snapshot_objects WHERE snapshot_id = ?1", params![snapshot_id])?;
            exec(tx, "DELETE FROM bucket_snapshots WHERE snapshot_id = ?1", params![snapshot_id])?;
            Ok(())
        })?;
        self.recompute_usage(id)?;
        log::info!("bucket {id} snapshot {snapshot_id} deleted by {}", caller.principal());
        Ok(())
    }

    /// Reads the manifest of a snapshot root (§10.2: the authority for restore).
    pub async fn read_manifest(&self, root_obj_id: &ObjId) -> BucketResult<Manifest> {
        let body = self.ndm.get_object(root_obj_id).await.map_err(|e| BucketError::ContentNotAvailable(e.to_string()))?;
        let root: DirObject = load_named_obj(&body).map_err(store_err)?;
        let item = root.get("manifest").ok_or_else(|| BucketError::InvalidArgument("snapshot root has no manifest".into()))?;
        let (manifest_id, inline) = item.get_obj_id().map_err(store_err)?;
        let file = if inline.is_empty() { load_file(&self.ndm, &manifest_id).await? } else { load_named_obj(&inline).map_err(store_err)? };
        let bytes = read_file(&self.ndm, &file, MAX_MANIFEST_BYTES).await?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        if manifest.format != MANIFEST_FORMAT {
            return Err(BucketError::InvalidArgument(format!("unknown manifest format {}", manifest.format)));
        }
        Ok(manifest)
    }

    /// `bucket.restore_from_snapshot`: rebuilds the bindings of an empty bucket from
    /// a manifest; entries whose content is not in this Zone are reported, not bound.
    pub async fn restore_from_snapshot(&self, id: &BucketId, caller: &Caller, root_obj_id: &str) -> BucketResult<RestoreReport> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        let root = ObjId::new(root_obj_id).map_err(|e| BucketError::InvalidArgument(e.to_string()))?;
        let manifest = self.read_manifest(&root).await?;
        if manifest.kind != bucket.kind {
            return Err(BucketError::KindMismatch(format!("the snapshot is of a {} bucket", manifest.kind)));
        }
        let _guard = self.lock(id).await;
        if self.bucket(id)?.version_count > 0 {
            return Err(BucketError::Conflict(format!("{id} is not empty; restore into an empty bucket")));
        }
        let mut missing = Vec::new();
        let mut ready = Vec::new();
        let mut checked: HashMap<String, bool> = HashMap::new();
        for e in manifest.entries {
            let obj = ObjId::new(&e.obj_id).map_err(store_err)?;
            let ok = match checked.get(&e.obj_id) {
                Some(ok) => *ok,
                None => {
                    let ok = if bucket.data_policy == DataPolicy::Ambient {
                        self.ndm.is_object_exist(&obj).await.unwrap_or(false)
                    } else {
                        self.ndm.is_object_stored(&obj, None).await.unwrap_or(false)
                    };
                    checked.insert(e.obj_id.clone(), ok);
                    ok
                }
            };
            if ok {
                ready.push((obj, e));
            } else {
                missing.push(MissingEntry { key: e.key, version: e.version, obj_id: e.obj_id });
            }
        }
        let owner = id.pin_owner();
        if let Some(scope) = bucket.data_policy.pin_scope() {
            let mut done = BTreeSet::new();
            for (obj, _) in &ready {
                if done.insert(obj.to_string()) {
                    self.ndm.pin(obj, &owner, scope, None).await.map_err(|e| BucketError::Store(format!("pin {}: {e}", obj.to_string())))?;
                }
            }
        }
        // A key keeps at most one current row: the newest restored one.
        let mut current: HashMap<String, u64> = HashMap::new();
        for (_, e) in &ready {
            if e.is_current {
                let v = current.entry(e.key.clone()).or_insert(e.version);
                *v = (*v).max(e.version);
            }
        }
        let now = now_ms();
        let pinned = bucket.data_policy.pin_scope().is_some();
        let restored = ready.len() as u64;
        self.db.write(|tx| {
            let mut max_version = 0u64;
            for (_, e) in &ready {
                let is_current = current.get(&e.key) == Some(&e.version);
                let b = Binding {
                    bucket_id: id.clone(),
                    key: e.key.clone(),
                    version: e.version,
                    is_current,
                    is_delete_marker: false,
                    obj_id: Some(e.obj_id.clone()),
                    obj_type: Some(e.obj_type.clone()),
                    size: e.size,
                    etag: e.obj_id.clone(),
                    etag_md5: e.etag_md5.clone(),
                    content_type: e.content_type.clone(),
                    metadata: e.metadata.clone(),
                    tags: e.tags.clone(),
                    http_headers: e.http_headers.clone(),
                    expires_at: e.expires_at,
                    origin: e.origin.clone(),
                    last_access_at: None,
                    created_at: e.created_at,
                    created_by: e.created_by.clone(),
                };
                insert_binding(tx, &b)?;
                if !is_current {
                    exec(tx, "UPDATE bindings SET noncurrent_at = ?4 WHERE bucket_id = ?1 AND key = ?2 AND version = ?3", params![id.to_string(), e.key, e.version as i64, now])?;
                }
                max_version = max_version.max(e.version);
            }
            if pinned {
                let mut stmt = tx.prepare("SELECT DISTINCT obj_id FROM bindings WHERE bucket_id = ?1 AND obj_id IS NOT NULL")?;
                let objs: Vec<String> = stmt.query_map(params![id.to_string()], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
                drop(stmt);
                for obj in objs {
                    let refs = count_refs(tx, id, &obj)?;
                    exec(
                        tx,
                        "INSERT INTO bucket_pins (bucket_id, obj_id, ref_count, state, updated_at) VALUES (?1, ?2, ?3, 'held', ?4)
                         ON CONFLICT(bucket_id, obj_id) DO UPDATE SET ref_count = excluded.ref_count, state = 'held'",
                        params![id.to_string(), obj, refs as i64, now],
                    )?;
                }
            }
            exec(tx, "UPDATE buckets SET version_seq = MAX(version_seq, ?2) WHERE bucket_id = ?1", params![id.to_string(), max_version as i64])?;
            Ok(())
        })?;
        self.recompute_usage(id)?;
        if !missing.is_empty() {
            log::warn!("restore of {id} from {root_obj_id}: {} entries missing content", missing.len());
        }
        Ok(RestoreReport { restored, missing })
    }

    /// `bucket.verify_references`: which references the bucket cannot serve (§10.2).
    pub async fn verify_references(&self, id: &BucketId, caller: &Caller, refs: Vec<Reference>) -> BucketResult<Vec<Reference>> {
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Prefix("")).await?;
        let mut missing = Vec::new();
        for r in refs {
            let obj = match &r {
                Reference::Key { key, version } => match self.lookup(&bucket, key, *version) {
                    Ok(b) => b.obj_id,
                    Err(_) => None,
                },
                Reference::Object { obj_id } => {
                    let normalized = ObjId::new(obj_id).map(|o| o.to_string()).unwrap_or_default();
                    let bound = self.db.read(|c| count_refs(c, id, &normalized))? > 0;
                    bound.then_some(normalized)
                }
            };
            let present = match obj.as_deref().and_then(|o| ObjId::new(o).ok()) {
                Some(o) if bucket.data_policy == DataPolicy::Ambient => self.ndm.is_object_exist(&o).await.unwrap_or(false),
                Some(o) => self.ndm.is_object_stored(&o, None).await.unwrap_or(false),
                None => false,
            };
            if !present {
                missing.push(r);
            }
        }
        Ok(missing)
    }

    /// Snapshot roots of a bucket and their pin owner, for reconcile.
    pub(crate) fn snapshot_roots(&self) -> BucketResult<Vec<(String, String)>> {
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT snapshot_id, root_obj_id FROM bucket_snapshots")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub(crate) fn snapshot_exists(&self, snapshot_id: &str) -> BucketResult<bool> {
        self.db.read(|c| {
            Ok(c.query_row("SELECT 1 FROM bucket_snapshots WHERE snapshot_id = ?1", params![snapshot_id], |_| Ok(()))
                .optional()?
                .is_some())
        })
    }
}
