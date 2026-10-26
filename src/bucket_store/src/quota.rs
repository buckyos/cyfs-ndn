//! §8: bucket and pool quotas, usage report, recomputation and warnings.

use crate::access::*;
use crate::db::*;
use crate::error::*;
use crate::events::BucketEvent;
use crate::store::{bucket_limits, pool_limit, pool_usage, BucketStore};
use crate::types::*;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const DEFAULT_WARN: [u64; 2] = [80, 95];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    pub unique_bytes: u64,
    pub key_bytes: u64,
    pub key_count: u64,
    pub version_count: u64,
    pub snapshot_only_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketQuota {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_bytes: Option<u64>,
    /// ambient / cache: exceeding evicts instead of rejecting.
    pub soft_only: bool,
    pub max_objects: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_object_bytes: Option<u64>,
    pub warn_percent: Vec<u64>,
    pub usage: Usage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolBucketUsage {
    pub bucket_id: BucketId,
    pub data_policy: DataPolicy,
    pub unique_bytes: u64,
    pub snapshot_only_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolQuota {
    pub kind: PoolKind,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_max_bytes: Option<u64>,
    /// The user pool only counts until res_pool has a storage item (D14).
    pub enforced: bool,
    pub usage: u64,
    pub buckets: Vec<PoolBucketUsage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaView {
    pub bucket: BucketQuota,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolQuota>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaUpdate {
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub soft_bytes: Option<u64>,
    #[serde(default)]
    pub max_objects: Option<u64>,
    #[serde(default)]
    pub max_object_bytes: Option<u64>,
    #[serde(default)]
    pub warn_percent: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyUsage {
    pub day: i64,
    pub added_bytes: u64,
    pub removed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageReport {
    pub buckets: Vec<BucketInfo>,
    /// `unique_bytes + snapshot_only_bytes` per data policy.
    pub by_policy: BTreeMap<String, u64>,
    pub daily: Vec<DailyUsage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recomputed {
    pub before: Usage,
    pub after: Usage,
    pub drifted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportScope {
    User,
    App,
    Zone,
}

fn usage_of(b: &BucketInfo) -> Usage {
    Usage {
        unique_bytes: b.unique_bytes,
        key_bytes: b.key_bytes,
        key_count: b.key_count,
        version_count: b.version_count,
        snapshot_only_bytes: b.snapshot_only_bytes,
    }
}

fn warn_list(c: &Connection, id: &BucketId) -> BucketResult<Vec<u64>> {
    let s: Option<String> = c
        .query_row("SELECT warn_percent FROM bucket_quotas WHERE bucket_id = ?1", params![id.to_string()], |r| r.get(0))
        .optional()?;
    Ok(s.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_else(|| DEFAULT_WARN.to_vec()))
}

/// Highest threshold reached; 0 below all of them.
fn reached(usage: u64, limit: u64, thresholds: &[u64]) -> (u64, u64) {
    if limit == 0 {
        return (0, 0);
    }
    let percent = usage.saturating_mul(100) / limit;
    let level = thresholds.iter().copied().filter(|t| percent >= *t).max().unwrap_or(0);
    (percent, level)
}

impl BucketStore {
    fn bucket_quota(&self, c: &Connection, bucket: &BucketInfo) -> BucketResult<BucketQuota> {
        let limits = bucket_limits(c, bucket)?;
        let soft: Option<i64> = c
            .query_row("SELECT soft_bytes FROM bucket_quotas WHERE bucket_id = ?1", params![bucket.bucket_id.to_string()], |r| r.get(0))
            .optional()?
            .flatten();
        let max_object: Option<i64> = c
            .query_row("SELECT max_object_bytes FROM bucket_quotas WHERE bucket_id = ?1", params![bucket.bucket_id.to_string()], |r| r.get(0))
            .optional()?
            .flatten();
        Ok(BucketQuota {
            max_bytes: limits.max_bytes,
            soft_bytes: soft.map(|v| v as u64).or(limits.max_bytes.map(|m| m * 8 / 10)),
            soft_only: bucket.data_policy.soft_quota(),
            max_objects: limits.max_objects,
            max_object_bytes: max_object.map(|v| v as u64).or(bucket.data_policy.default_max_object_bytes()),
            warn_percent: warn_list(c, &bucket.bucket_id)?,
            usage: usage_of(bucket),
        })
    }

    pub(crate) fn pool_quota_of(&self, c: &Connection, kind: PoolKind, pool_id: &str) -> BucketResult<PoolQuota> {
        let row: Option<(Option<i64>, Option<i64>)> = c
            .query_row(
                "SELECT max_bytes, declared_max_bytes FROM pool_quotas WHERE pool_kind = ?1 AND pool_id = ?2",
                params![kind.as_str(), pool_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (max, declared) = row.unwrap_or((None, None));
        let buckets: Vec<BucketInfo> = {
            let (sql, a, b) = match kind {
                PoolKind::User => (
                    format!("SELECT {BUCKET_COLS} FROM buckets WHERE owner = ?1 AND ?2 = ?2 AND data_policy IN ('documents','collected') ORDER BY bucket_id"),
                    pool_id.to_string(),
                    String::new(),
                ),
                PoolKind::App => {
                    let (appid, owner) = pool_id.rsplit_once('@').unwrap_or((pool_id, ZONE_OWNER));
                    (
                        format!("SELECT {BUCKET_COLS} FROM buckets WHERE appid = ?1 AND owner = ?2 AND data_policy IN ('app_data','temp') ORDER BY bucket_id"),
                        appid.to_string(),
                        owner.to_string(),
                    )
                }
            };
            let mut stmt = c.prepare(&sql)?;
            let rows = stmt.query_map(params![a, b], bucket_from_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(PoolQuota {
            kind,
            id: pool_id.to_string(),
            max_bytes: max.or(declared).map(|v| v as u64),
            declared_max_bytes: declared.map(|v| v as u64),
            enforced: kind == PoolKind::App,
            usage: pool_usage(c, kind, pool_id)?,
            buckets: buckets
                .into_iter()
                .map(|b| PoolBucketUsage { bucket_id: b.bucket_id, data_policy: b.data_policy, unique_bytes: b.unique_bytes, snapshot_only_bytes: b.snapshot_only_bytes })
                .collect(),
        })
    }

    pub async fn get_quota(&self, id: &BucketId, caller: &Caller) -> BucketResult<QuotaView> {
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::List, Target::Prefix("")).await?;
        self.db.read(|c| {
            let pool = match bucket.data_policy.pool() {
                Some(kind) => Some(self.pool_quota_of(c, kind, &bucket.bucket_id.pool_id(kind))?),
                None => None,
            };
            Ok(QuotaView { bucket: self.bucket_quota(c, &bucket)?, pool })
        })
    }

    /// `bucket.set_quota`. Without `admin`, an app may only lower its limits to
    /// at most the declared values.
    pub async fn set_quota(&self, id: &BucketId, caller: &Caller, update: QuotaUpdate, admin: bool) -> BucketResult<QuotaView> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        if update.max_bytes.is_some() && !bucket.data_policy.quota_settable() {
            return Err(BucketError::PolicyViolation(format!("a {} bucket uses the user's quota", bucket.data_policy)));
        }
        if !admin {
            let declared_bytes = bucket.quota_bytes;
            let declared_objects = bucket.max_objects.unwrap_or(DEFAULT_MAX_OBJECTS);
            if update.max_bytes.is_some_and(|m| declared_bytes.is_some_and(|d| m > d)) {
                return Err(BucketError::PermissionDenied("only an admin raises a bucket above its declared quota".into()));
            }
            if update.max_objects.is_some_and(|m| m > declared_objects) {
                return Err(BucketError::PermissionDenied("only an admin raises max_objects above the declared value".into()));
            }
        }
        if let Some(w) = &update.warn_percent {
            if w.iter().any(|p| *p == 0 || *p > 100) {
                return Err(BucketError::InvalidArgument("warn_percent values are 1..=100".into()));
            }
        }
        let set_by = caller.principal();
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO bucket_quotas (bucket_id, max_bytes, soft_bytes, max_objects, max_object_bytes, warn_percent, set_by, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(?6, '[80,95]'), ?7, ?8)
                 ON CONFLICT(bucket_id) DO UPDATE SET
                    max_bytes = COALESCE(excluded.max_bytes, bucket_quotas.max_bytes),
                    soft_bytes = COALESCE(excluded.soft_bytes, bucket_quotas.soft_bytes),
                    max_objects = COALESCE(excluded.max_objects, bucket_quotas.max_objects),
                    max_object_bytes = COALESCE(excluded.max_object_bytes, bucket_quotas.max_object_bytes),
                    warn_percent = COALESCE(?6, bucket_quotas.warn_percent),
                    set_by = excluded.set_by, updated_at = excluded.updated_at",
                params![
                    id.to_string(),
                    update.max_bytes.map(|v| v as i64),
                    update.soft_bytes.map(|v| v as i64),
                    update.max_objects.map(|v| v as i64),
                    update.max_object_bytes.map(|v| v as i64),
                    update.warn_percent.as_ref().map(to_json),
                    set_by,
                    now_ms()
                ],
            )?;
            Ok(())
        })?;
        log::info!("bucket {id} quota set by {set_by}: {update:?}");
        self.get_quota(id, &Caller::system()).await
    }

    /// `bucket.clear_quota` (admin): back to the declared values.
    pub fn clear_quota(&self, id: &BucketId) -> BucketResult<()> {
        self.bucket(id)?;
        self.db.write(|tx| {
            exec(tx, "DELETE FROM bucket_quotas WHERE bucket_id = ?1", params![id.to_string()])?;
            Ok(())
        })
    }

    pub fn get_pool_quota(&self, kind: PoolKind, pool_id: &str) -> BucketResult<PoolQuota> {
        self.db.read(|c| self.pool_quota_of(c, kind, pool_id))
    }

    /// `bucket.set_pool_quota` (admin). `None` clears the override.
    pub fn set_pool_quota(&self, kind: PoolKind, pool_id: &str, max_bytes: Option<u64>, set_by: &str) -> BucketResult<PoolQuota> {
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO pool_quotas (pool_kind, pool_id, max_bytes, set_by, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(pool_kind, pool_id) DO UPDATE SET max_bytes = excluded.max_bytes, set_by = excluded.set_by, updated_at = excluded.updated_at",
                params![kind.as_str(), pool_id, max_bytes.map(|v| v as i64), set_by, now_ms()],
            )?;
            Ok(())
        })?;
        self.get_pool_quota(kind, pool_id)
    }

    /// `bucket.usage_report`: the storage page's source (§8.6).
    pub fn usage_report(&self, scope: ReportScope, id: Option<&str>, days: u32) -> BucketResult<UsageReport> {
        let buckets = match scope {
            ReportScope::User => self.list_buckets(id, None, None)?,
            ReportScope::App => self.list_buckets(None, id, None)?,
            ReportScope::Zone => self.list_buckets(None, None, None)?,
        };
        let mut by_policy = BTreeMap::new();
        for b in &buckets {
            *by_policy.entry(b.data_policy.as_str().to_string()).or_insert(0) += b.unique_bytes + b.snapshot_only_bytes;
        }
        let since = now_ms().div_euclid(DAY_MS) - days.max(1) as i64 + 1;
        let ids: Vec<String> = buckets.iter().map(|b| b.bucket_id.to_string()).collect();
        let mut series: BTreeMap<i64, (u64, u64)> = BTreeMap::new();
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT day, added_bytes, removed_bytes FROM bucket_usage_daily WHERE bucket_id = ?1 AND day >= ?2")?;
            for id in &ids {
                let rows = stmt.query_map(params![id, since], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)))?;
                for row in rows {
                    let (day, a, r) = row?;
                    let e = series.entry(day).or_default();
                    e.0 += a as u64;
                    e.1 += r as u64;
                }
            }
            Ok(())
        })?;
        Ok(UsageReport {
            buckets,
            by_policy,
            daily: series.into_iter().map(|(day, (a, r))| DailyUsage { day, added_bytes: a, removed_bytes: r }).collect(),
        })
    }

    /// Recomputes the counters from the bindings (§8.5); drift is recorded as an issue.
    pub fn recompute_usage(&self, id: &BucketId) -> BucketResult<Recomputed> {
        let key = id.to_string();
        self.db.write(|tx| {
            let before = usage_of(&load_bucket(tx, id)?);
            let unique: i64 = tx.query_row(
                "SELECT COALESCE(SUM(size), 0) FROM (SELECT obj_id, MAX(size) AS size FROM bindings WHERE bucket_id = ?1 AND obj_id IS NOT NULL GROUP BY obj_id)",
                params![key],
                |r| r.get(0),
            )?;
            let (key_bytes, key_count): (i64, i64) = tx.query_row(
                "SELECT COALESCE(SUM(size), 0), COUNT(*) FROM bindings WHERE bucket_id = ?1 AND is_current = 1 AND is_delete_marker = 0",
                params![key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let versions: i64 = tx.query_row("SELECT COUNT(*) FROM bindings WHERE bucket_id = ?1", params![key], |r| r.get(0))?;
            let snapshot_only: i64 = tx.query_row(
                "SELECT COALESCE(SUM(size), 0) FROM (SELECT obj_id, MAX(size) AS size FROM bucket_snapshot_objects WHERE bucket_id = ?1
                   AND obj_id NOT IN (SELECT obj_id FROM bindings WHERE bucket_id = ?1 AND obj_id IS NOT NULL) GROUP BY obj_id)",
                params![key],
                |r| r.get(0),
            )?;
            exec(
                tx,
                "UPDATE buckets SET unique_bytes = ?2, key_bytes = ?3, key_count = ?4, version_count = ?5, snapshot_only_bytes = ?6 WHERE bucket_id = ?1",
                params![key, unique, key_bytes, key_count, versions, snapshot_only],
            )?;
            let after = Usage {
                unique_bytes: unique as u64,
                key_bytes: key_bytes as u64,
                key_count: key_count as u64,
                version_count: versions as u64,
                snapshot_only_bytes: snapshot_only as u64,
            };
            let drifted = before != after;
            if drifted {
                log::warn!("bucket {id} usage drifted: {before:?} -> {after:?}");
                exec(
                    tx,
                    "INSERT INTO bucket_issues (bucket_id, subject, kind, detail, at) VALUES (?1, '', 'usage_drift', ?2, ?3)
                     ON CONFLICT(bucket_id, subject, kind) DO UPDATE SET detail = excluded.detail, at = excluded.at",
                    params![key, to_json(&(&before, &after)), now_ms()],
                )?;
            }
            Ok(Recomputed { before, after, drifted })
        })
    }

    pub async fn recompute_usage_checked(&self, id: &BucketId, caller: &Caller) -> BucketResult<Recomputed> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        let _guard = self.lock(id).await;
        self.recompute_usage(id)
    }

    /// Emits `quota_warning` when usage crosses a `warn_percent` threshold upwards.
    pub(crate) fn check_quota_warnings(&self, bucket: &BucketInfo) {
        let result = self.db.write(|tx| {
            let mut events = Vec::new();
            let row = load_bucket(tx, &bucket.bucket_id)?;
            let limits = bucket_limits(tx, &row)?;
            let thresholds = warn_list(tx, &row.bucket_id)?;
            if let Some(max) = limits.max_bytes {
                let usage = row.unique_bytes + row.snapshot_only_bytes;
                let (percent, level) = reached(usage, max, &thresholds);
                let last: i64 = tx.query_row("SELECT last_warned_pct FROM buckets WHERE bucket_id = ?1", params![row.bucket_id.to_string()], |r| r.get(0))?;
                if level as i64 != last && (level as i64 > last || level == 0) {
                    exec(tx, "UPDATE buckets SET last_warned_pct = ?2 WHERE bucket_id = ?1", params![row.bucket_id.to_string(), level as i64])?;
                    if level as i64 > last {
                        events.push(BucketEvent::QuotaWarning {
                            bucket_id: Some(row.bucket_id.clone()),
                            pool_kind: None,
                            pool_id: None,
                            percent,
                            usage,
                            limit: max,
                        });
                    }
                }
            }
            if let Some(kind) = row.data_policy.pool() {
                let pool_id = row.bucket_id.pool_id(kind);
                if let Some(max) = pool_limit(tx, kind, &pool_id)? {
                    let usage = pool_usage(tx, kind, &pool_id)?;
                    let (percent, level) = reached(usage, max, &DEFAULT_WARN);
                    let last: i64 = tx
                        .query_row(
                            "SELECT last_warned_pct FROM pool_quotas WHERE pool_kind = ?1 AND pool_id = ?2",
                            params![kind.as_str(), pool_id],
                            |r| r.get(0),
                        )
                        .optional()?
                        .unwrap_or(0);
                    if level as i64 != last && (level as i64 > last || level == 0) {
                        exec(
                            tx,
                            "UPDATE pool_quotas SET last_warned_pct = ?3 WHERE pool_kind = ?1 AND pool_id = ?2",
                            params![kind.as_str(), pool_id, level as i64],
                        )?;
                        if level as i64 > last {
                            events.push(BucketEvent::QuotaWarning {
                                bucket_id: None,
                                pool_kind: Some(kind),
                                pool_id: Some(pool_id),
                                percent,
                                usage,
                                limit: max,
                            });
                        }
                    }
                }
            }
            Ok(events)
        });
        match result {
            Ok(events) => {
                for e in &events {
                    log::info!("bucket quota warning: {e:?}");
                }
                self.emit(events)
            }
            Err(e) => log::warn!("quota warning check of {} failed: {e}", bucket.bucket_id),
        }
    }
}
