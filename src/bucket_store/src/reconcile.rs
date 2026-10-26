//! §7.3: keeps the named store's `bucket:` / `bucket-snapshot:` pins equal to
//! the index (B1). The index is the authority; pins are its materialization.

use crate::db::*;
use crate::error::*;
use crate::store::{count_refs, BucketStore};
use crate::types::*;
use named_store::{PinInfo, PinScope};
use ndn_lib::ObjId;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const PAGE: usize = 1000;
const RELEASE_GRACE_MS: i64 = 60_000;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub released: u64,
    pub repinned: u64,
    pub stray_unpinned: u64,
    pub snapshot_repinned: u64,
    pub snapshot_stray_unpinned: u64,
    pub missing_content: u64,
    pub usage_drift: u64,
}

impl BucketStore {
    async fn store_pins(&self, prefix: &str) -> BucketResult<Vec<PinInfo>> {
        let mut out = Vec::new();
        let mut cursor: Option<(String, String)> = None;
        loop {
            let after = cursor.as_ref().map(|(o, i)| (o.as_str(), i.as_str()));
            let page = self.ndm.list_pins_by_owner(prefix, after, PAGE).await.map_err(store_err)?;
            let Some(last) = page.last() else { break };
            cursor = Some((last.owner.clone(), last.obj_id.to_string()));
            let full = page.len() == PAGE;
            out.extend(page);
            if !full {
                break;
            }
        }
        Ok(out)
    }

    /// One reconcile round; `deep` also recomputes usage and checks that held
    /// content is still readable (daily).
    pub async fn reconcile(&self, deep: bool) -> BucketResult<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let now = now_ms();

        // 1. releasing rows whose unpin never finished
        let stale: Vec<(String, String)> = self.db.read(|c| {
            let mut stmt = c.prepare("SELECT bucket_id, obj_id FROM bucket_pins WHERE state = 'releasing' AND updated_at <= ?1")?;
            let rows = stmt.query_map(params![now - RELEASE_GRACE_MS], |r| Ok((r.get(0)?, r.get(1)?)))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        let mut by_bucket: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (b, o) in stale {
            by_bucket.entry(b).or_default().push(o);
        }
        for (b, objs) in by_bucket {
            let Ok(id) = b.parse::<BucketId>() else { continue };
            let _guard = self.lock(&id).await;
            self.finish_unpins(&id, &objs).await;
            report.released += objs.len() as u64;
        }

        // 2. store pins vs index
        let buckets: HashMap<String, BucketInfo> = self.list_buckets(None, None, None)?.into_iter().map(|b| (b.bucket_id.to_string(), b)).collect();
        let mut pinned: HashMap<String, HashMap<String, PinScope>> = HashMap::new();
        let mut strays: BTreeMap<String, Vec<ObjId>> = BTreeMap::new();
        for pin in self.store_pins("bucket:").await? {
            let Some(bucket_id) = pin.owner.strip_prefix("bucket:") else { continue };
            let obj = pin.obj_id.to_string();
            let wanted = buckets.get(bucket_id).and_then(|b| b.data_policy.pin_scope());
            let referenced = wanted.is_some() && self.db.read(|c| count_refs(c, &bucket_id.parse()?, &obj))? > 0;
            if referenced {
                pinned.entry(bucket_id.to_string()).or_default().insert(obj, pin.scope);
            } else {
                strays.entry(bucket_id.to_string()).or_default().push(pin.obj_id);
            }
        }
        for (bucket_id, objs) in strays {
            let owner = format!("bucket:{bucket_id}");
            let id = bucket_id.parse::<BucketId>().ok();
            let _guard = match &id {
                Some(id) => Some(self.lock(id).await),
                None => None,
            };
            for obj in objs {
                // Re-check under the lock: a put may have bound it meanwhile.
                if let Some(id) = &id {
                    if buckets.get(&bucket_id).and_then(|b| b.data_policy.pin_scope()).is_some()
                        && self.db.read(|c| count_refs(c, id, &obj.to_string()))? > 0
                    {
                        continue;
                    }
                }
                match self.ndm.unpin(&obj, &owner).await {
                    Ok(()) => report.stray_unpinned += 1,
                    Err(e) => log::warn!("unpin stray {} of {bucket_id}: {e}", obj.to_string()),
                }
            }
        }
        for (key, bucket) in &buckets {
            let Some(scope) = bucket.data_policy.pin_scope() else { continue };
            let have = pinned.remove(key).unwrap_or_default();
            let objs: Vec<String> = self.db.read(|c| {
                let mut stmt = c.prepare("SELECT DISTINCT obj_id FROM bindings WHERE bucket_id = ?1 AND obj_id IS NOT NULL")?;
                let rows = stmt.query_map(params![key], |r| r.get(0))?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })?;
            let missing: Vec<String> = objs.into_iter().filter(|o| have.get(o) != Some(&scope)).collect();
            if missing.is_empty() {
                continue;
            }
            let _guard = self.lock(&bucket.bucket_id).await;
            for obj in missing {
                let Ok(obj_id) = ObjId::new(&obj) else { continue };
                if self.db.read(|c| count_refs(c, &bucket.bucket_id, &obj))? == 0 {
                    continue;
                }
                match self.ndm.pin(&obj_id, &bucket.bucket_id.pin_owner(), scope, None).await {
                    Ok(()) => {
                        report.repinned += 1;
                        let refs = self.db.read(|c| count_refs(c, &bucket.bucket_id, &obj))?;
                        self.db.write(|tx| {
                            exec(
                                tx,
                                "INSERT INTO bucket_pins (bucket_id, obj_id, ref_count, state, updated_at) VALUES (?1, ?2, ?3, 'held', ?4)
                                 ON CONFLICT(bucket_id, obj_id) DO UPDATE SET ref_count = excluded.ref_count, state = 'held', updated_at = excluded.updated_at",
                                params![key, obj, refs as i64, now],
                            )
                        })?;
                    }
                    Err(e) => log::warn!("repin {obj} of {key}: {e}"),
                }
            }
        }

        // 3. snapshot pins
        let roots: HashMap<String, String> = self.snapshot_roots()?.into_iter().collect();
        let mut seen = HashSet::new();
        for pin in self.store_pins("bucket-snapshot:").await? {
            let Some(sid) = pin.owner.strip_prefix("bucket-snapshot:") else { continue };
            if roots.get(sid) == Some(&pin.obj_id.to_string()) && self.snapshot_exists(sid)? {
                seen.insert(sid.to_string());
            } else if self.ndm.unpin(&pin.obj_id, &pin.owner).await.is_ok() {
                report.snapshot_stray_unpinned += 1;
            }
        }
        for (sid, root) in roots {
            if seen.contains(&sid) {
                continue;
            }
            let Ok(root_id) = ObjId::new(&root) else { continue };
            if self.ndm.pin(&root_id, &format!("bucket-snapshot:{sid}"), PinScope::Recursive, None).await.is_ok() {
                report.snapshot_repinned += 1;
            }
        }

        if deep {
            for (key, bucket) in &buckets {
                let _guard = self.lock(&bucket.bucket_id).await;
                if self.recompute_usage(&bucket.bucket_id)?.drifted {
                    report.usage_drift += 1;
                }
                if bucket.data_policy.pin_scope() != Some(PinScope::Recursive) {
                    continue;
                }
                let objs: Vec<String> = self.db.read(|c| {
                    let mut stmt = c.prepare("SELECT obj_id FROM bucket_pins WHERE bucket_id = ?1 AND state = 'held'")?;
                    let rows = stmt.query_map(params![key], |r| r.get(0))?;
                    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
                })?;
                for obj in objs {
                    let Ok(obj_id) = ObjId::new(&obj) else { continue };
                    let stored = self.ndm.is_object_stored(&obj_id, None).await.unwrap_or(false);
                    self.db.write(|tx| {
                        if stored {
                            exec(tx, "DELETE FROM bucket_issues WHERE bucket_id = ?1 AND subject = ?2 AND kind = 'missing_content'", params![key, obj])?;
                        } else {
                            exec(
                                tx,
                                "INSERT INTO bucket_issues (bucket_id, subject, kind, detail, at) VALUES (?1, ?2, 'missing_content', 'content not readable', ?3)
                                 ON CONFLICT(bucket_id, subject, kind) DO UPDATE SET at = excluded.at",
                                params![key, obj, now],
                            )?;
                        }
                        Ok(())
                    })?;
                    if !stored {
                        report.missing_content += 1;
                    }
                }
            }
        }
        if report != ReconcileReport::default() {
            log::info!("bucket reconcile: {report:?}");
        }
        Ok(report)
    }

    /// Open issues (missing content, usage drift) of a bucket.
    pub fn issues(&self, id: &BucketId) -> BucketResult<Vec<serde_json::Value>> {
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT subject, kind, detail, at FROM bucket_issues WHERE bucket_id = ?1 ORDER BY at")?;
            let rows = stmt.query_map(params![id.to_string()], |r| {
                Ok(serde_json::json!({"subject": r.get::<_, String>(0)?, "kind": r.get::<_, String>(1)?, "detail": r.get::<_, String>(2)?, "at": r.get::<_, i64>(3)?}))
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }
}
