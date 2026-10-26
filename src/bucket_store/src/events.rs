//! §11: object and quota events. Delivery is the host's business (kmsg topic
//! `bucket.<bucket_id>`); events are best effort, never part of a transaction.

use crate::types::{Binding, BucketId, PoolKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectEventKind {
    ObjectCreated,
    ObjectRemoved,
    ObjectExpired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BucketEvent {
    Object {
        bucket_id: BucketId,
        event: ObjectEventKind,
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        obj_id: Option<String>,
        version: u64,
        etag: String,
        size: u64,
        actor: String,
    },
    QuotaWarning {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bucket_id: Option<BucketId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pool_kind: Option<PoolKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pool_id: Option<String>,
        percent: u64,
        usage: u64,
        limit: u64,
    },
}

impl BucketEvent {
    pub(crate) fn object(event: ObjectEventKind, b: &Binding, actor: &str) -> Self {
        BucketEvent::Object {
            bucket_id: b.bucket_id.clone(),
            event,
            key: b.key.clone(),
            obj_id: b.obj_id.clone(),
            version: b.version,
            etag: b.etag.clone(),
            size: b.size,
            actor: actor.to_string(),
        }
    }

    /// The kmsg topic of this event's bucket, if it has one.
    pub fn bucket_id(&self) -> Option<&BucketId> {
        match self {
            BucketEvent::Object { bucket_id, .. } => Some(bucket_id),
            BucketEvent::QuotaWarning { bucket_id, .. } => bucket_id.as_ref(),
        }
    }
}

pub trait EventSink: Send + Sync {
    fn emit(&self, event: BucketEvent);
}
