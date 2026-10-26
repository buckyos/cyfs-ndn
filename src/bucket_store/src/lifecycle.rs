//! §9 lifecycle rules, object TTL (§5.6) and the cache tier's soft quota (§4.5.2).
//! Every action only unbinds; reclaiming bytes is the named store's business.

use crate::access::*;
use crate::db::*;
use crate::error::*;
use crate::events::ObjectEventKind;
use crate::store::{bucket_limits, BucketStore};
use crate::types::*;
use rusqlite::params;
use serde::{Deserialize, Serialize};

const BATCH: i64 = 500;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LifecycleReport {
    pub expired_objects: u64,
    pub expired_current: u64,
    pub expired_noncurrent: u64,
    pub removed_markers: u64,
    pub cache_evicted: u64,
}

impl BucketStore {
    pub async fn get_lifecycle(&self, id: &BucketId, caller: &Caller) -> BucketResult<Vec<LifecycleRule>> {
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Prefix("")).await?;
        Ok(bucket.lifecycle)
    }

    pub async fn set_lifecycle(&self, id: &BucketId, caller: &Caller, rules: Vec<LifecycleRule>) -> BucketResult<Vec<LifecycleRule>> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        bucket.data_policy.check_lifecycle(&rules)?;
        for rule in &rules {
            if let LifecycleRule::Expire { prefix, days } = rule {
                if *days == 0 || (!prefix.is_empty() && validate_key(prefix).is_err()) {
                    return Err(BucketError::InvalidArgument("an expire rule needs days > 0 and a valid prefix".into()));
                }
            }
        }
        self.db.write(|tx| {
            exec(tx, "UPDATE buckets SET lifecycle_json = ?2 WHERE bucket_id = ?1", params![id.to_string(), to_json(&rules)])?;
            Ok(())
        })?;
        Ok(rules)
    }

    fn rows(&self, sql: &str, p: impl rusqlite::Params) -> BucketResult<Vec<(String, u64, bool)>> {
        self.db.read(|c| {
            let mut stmt = c.prepare(sql)?;
            let rows = stmt.query_map(p, |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? != 0)))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Unbinds one row; a current version in a versioned bucket first gets a delete marker.
    async fn expire_row(&self, bucket: &BucketInfo, key: &str, version: u64, current: bool) -> BucketResult<()> {
        if current {
            self.delete_system(bucket, key, None, ObjectEventKind::ObjectExpired).await?;
            if !bucket.versioning {
                return Ok(());
            }
        }
        match self.delete_system(bucket, key, Some(version), ObjectEventKind::ObjectExpired).await {
            Ok(_) | Err(BucketError::NoSuchKey(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// One evaluation round over every bucket (hourly by the host; `now` for tests).
    pub async fn run_lifecycle(&self, now: i64) -> BucketResult<LifecycleReport> {
        let mut report = LifecycleReport::default();
        for bucket in self.list_buckets(None, None, None)? {
            if let Err(e) = self.lifecycle_bucket(&bucket, now, &mut report).await {
                log::warn!("lifecycle of {} failed: {e}", bucket.bucket_id);
            }
        }
        if report != LifecycleReport::default() {
            log::info!("bucket lifecycle round: {report:?}");
        }
        Ok(report)
    }

    async fn lifecycle_bucket(&self, bucket: &BucketInfo, now: i64, report: &mut LifecycleReport) -> BucketResult<()> {
        let id = bucket.bucket_id.to_string();
        let rows = self.rows(
            "SELECT key, version, is_current FROM bindings WHERE bucket_id = ?1 AND expires_at IS NOT NULL AND expires_at <= ?2
             AND is_delete_marker = 0 LIMIT ?3",
            params![id, now, BATCH],
        )?;
        for (key, version, current) in rows {
            self.expire_row(bucket, &key, version, current).await?;
            report.expired_objects += 1;
        }
        for rule in &bucket.lifecycle {
            match rule {
                LifecycleRule::Expire { prefix, days } => {
                    let before = now - *days as i64 * DAY_MS;
                    let upper = format!("{prefix}\u{10FFFF}");
                    let rows = self.rows(
                        "SELECT key, version, is_current FROM bindings WHERE bucket_id = ?1 AND is_current = 1 AND is_delete_marker = 0
                         AND key >= ?2 AND key < ?3 AND created_at <= ?4 LIMIT ?5",
                        params![id, prefix, upper, before, BATCH],
                    )?;
                    for (key, _, _) in rows {
                        self.delete_system(bucket, &key, None, ObjectEventKind::ObjectExpired).await?;
                        report.expired_current += 1;
                    }
                }
                LifecycleRule::NoncurrentExpire { days } => {
                    let before = now - *days as i64 * DAY_MS;
                    let rows = self.rows(
                        "SELECT key, version, is_current FROM bindings WHERE bucket_id = ?1 AND is_current = 0 AND noncurrent_at <= ?2 LIMIT ?3",
                        params![id, before, BATCH],
                    )?;
                    for (key, version, _) in rows {
                        match self.delete_system(bucket, &key, Some(version), ObjectEventKind::ObjectExpired).await {
                            Ok(_) | Err(BucketError::NoSuchKey(_)) => report.expired_noncurrent += 1,
                            Err(e) => return Err(e),
                        }
                    }
                }
                LifecycleRule::ExpiredDeleteMarker => {
                    let rows = self.rows(
                        "SELECT b.key, b.version, b.is_current FROM bindings b WHERE b.bucket_id = ?1 AND b.is_current = 1 AND b.is_delete_marker = 1
                         AND NOT EXISTS (SELECT 1 FROM bindings o WHERE o.bucket_id = b.bucket_id AND o.key = b.key AND o.version != b.version)
                         LIMIT ?2",
                        params![id, BATCH],
                    )?;
                    for (key, version, _) in rows {
                        self.delete_system(bucket, &key, Some(version), ObjectEventKind::ObjectExpired).await?;
                        report.removed_markers += 1;
                    }
                }
            }
        }
        if bucket.data_policy == DataPolicy::Cache {
            report.cache_evicted += self.evict_cache(bucket).await?;
        }
        Ok(())
    }

    /// Cache soft quota: unbind least recently read keys until back under the limit.
    pub(crate) async fn evict_cache(&self, bucket: &BucketInfo) -> BucketResult<u64> {
        let Some(max) = self.db.read(|c| bucket_limits(c, bucket))?.max_bytes else { return Ok(0) };
        let mut evicted = 0;
        loop {
            let current = self.bucket(&bucket.bucket_id)?;
            if current.unique_bytes <= max {
                return Ok(evicted);
            }
            let victims = self.rows(
                "SELECT key, version, is_current FROM bindings WHERE bucket_id = ?1 AND is_current = 1 AND is_delete_marker = 0
                 ORDER BY COALESCE(last_access_at, created_at) ASC LIMIT 16",
                params![bucket.bucket_id.to_string()],
            )?;
            if victims.is_empty() {
                return Ok(evicted);
            }
            for (key, _, _) in victims {
                self.delete_system(bucket, &key, None, ObjectEventKind::ObjectExpired).await?;
                evicted += 1;
                if self.bucket(&bucket.bucket_id)?.unique_bytes <= max {
                    return Ok(evicted);
                }
            }
        }
    }

    /// Reads of cache / ambient keys feed their LRU.
    pub(crate) fn touch_binding(&self, b: &Binding) {
        let now = now_ms();
        if b.last_access_at.is_some_and(|t| now - t < 60_000) {
            return;
        }
        let _ = self.db.write(|tx| {
            exec(
                tx,
                "UPDATE bindings SET last_access_at = ?4 WHERE bucket_id = ?1 AND key = ?2 AND version = ?3",
                params![b.bucket_id.to_string(), b.key, b.version as i64, now],
            )
        });
    }
}
