//! Application object buckets (buckyos `doc/arch/应用对象桶 Bucket 设计.md`):
//! a strongly consistent `key -> ObjId` index whose bindings are retention
//! promises (pins) in the named store.
//!
//! The lib knows no users, apps or agents: the host authenticates callers,
//! answers the identity policy (RBAC, first layer) through [`IdentityPolicy`],
//! and passes the caller's opaque identities in [`Caller`]. The lib decides
//! grants, capability tokens and the data-policy rules, keeps the index and
//! the pins consistent, and serves the HTTP data plane.

pub mod access;
pub mod content;
pub mod db;
pub mod error;
pub mod events;
pub mod http;
pub mod lifecycle;
pub mod quota;
pub mod reconcile;
pub mod s3;
pub mod snapshot;
pub mod store;
pub mod types;

#[cfg(test)]
mod tests;

pub use access::{
    AccessCheck, AccessToken, Action, ActionSet, AllowAll, Caller, DenyAll, Grant, GrantRequest, Grantee, GranteeKind,
    IdentityPolicy, IssuedToken, TokenRequest, Via,
};
pub use db::IndexDb;
pub use error::{BucketError, BucketResult, QuotaExceeded, QuotaLevel};
pub use events::{BucketEvent, EventSink, ObjectEventKind};
pub use lifecycle::LifecycleReport;
pub use quota::{PoolQuota, QuotaUpdate, QuotaView, Recomputed, ReportScope, UsageReport};
pub use reconcile::ReconcileReport;
pub use s3::{MultipartPart, MultipartUpload, S3Key, S3KeyScope};
pub use snapshot::{Manifest, MissingEntry, Reference, RestoreReport, SnapshotInfo};
pub use store::BucketStore;
pub use types::*;
