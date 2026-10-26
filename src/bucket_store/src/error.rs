use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaLevel {
    Bucket,
    Pool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaExceeded {
    pub level: QuotaLevel,
    /// `bytes` or `objects`.
    pub what: String,
    pub limit: u64,
    pub usage: u64,
}

/// Errors carry their kind as the variant; callers branch on the variant or
/// on [`BucketError::code`], never on the message.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BucketError {
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("no such key: {0}")]
    NoSuchKey(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("quota exceeded: {0:?}")]
    QuotaExceeded(QuotaExceeded),
    #[error("object too large: {size} > {limit}")]
    ObjectTooLarge { size: u64, limit: u64 },
    #[error("content not available: {0}")]
    ContentNotAvailable(String),
    #[error("content evicted: {0}")]
    ContentEvicted(String),
    #[error("origin unavailable: {0}")]
    OriginUnavailable(String),
    #[error("object type does not fit the bucket: {0}")]
    KindMismatch(String),
    #[error("not allowed by the bucket's data policy: {0}")]
    PolicyViolation(String),
    #[error("origin is required: {0}")]
    OriginRequired(String),
    #[error("the caller neither holds nor may read {0}")]
    NotHeld(String),
    #[error("invalid access token")]
    TokenInvalid,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("named store: {0}")]
    Store(String),
    #[error("index db: {0}")]
    Db(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type BucketResult<T> = Result<T, BucketError>;

impl BucketError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BucketNotFound(_) => "bucket_not_found",
            Self::NoSuchKey(_) => "no_such_key",
            Self::InvalidKey(_) => "invalid_key",
            Self::InvalidArgument(_) => "invalid_argument",
            Self::PreconditionFailed(_) => "precondition_failed",
            Self::PermissionDenied(_) => "permission_denied",
            Self::QuotaExceeded(_) => "quota_exceeded",
            Self::ObjectTooLarge { .. } => "object_too_large",
            Self::ContentNotAvailable(_) => "content_not_available",
            Self::ContentEvicted(_) => "content_evicted",
            Self::OriginUnavailable(_) => "origin_unavailable",
            Self::KindMismatch(_) => "kind_mismatch",
            Self::PolicyViolation(_) => "policy_violation",
            Self::OriginRequired(_) => "origin_required",
            Self::NotHeld(_) => "not_held",
            Self::TokenInvalid => "token_invalid",
            Self::Conflict(_) => "conflict",
            Self::Store(_) => "store_error",
            Self::Db(_) => "db_error",
            Self::Internal(_) => "internal_error",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::BucketNotFound(_) | Self::NoSuchKey(_) | Self::ContentEvicted(_) | Self::OriginUnavailable(_) => 404,
            Self::InvalidKey(_)
            | Self::InvalidArgument(_)
            | Self::KindMismatch(_)
            | Self::PolicyViolation(_)
            | Self::OriginRequired(_) => 400,
            Self::PreconditionFailed(_) => 412,
            Self::PermissionDenied(_) | Self::NotHeld(_) => 403,
            Self::TokenInvalid => 401,
            Self::QuotaExceeded(_) => 507,
            Self::ObjectTooLarge { .. } => 413,
            Self::ContentNotAvailable(_) | Self::Conflict(_) => 409,
            Self::Store(_) | Self::Db(_) | Self::Internal(_) => 500,
        }
    }

    /// Structured details for wire errors (quota level/limit/usage).
    pub fn data(&self) -> Option<serde_json::Value> {
        match self {
            Self::QuotaExceeded(q) => serde_json::to_value(q).ok(),
            Self::ObjectTooLarge { size, limit } => Some(serde_json::json!({"size": size, "limit": limit})),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for BucketError {
    fn from(e: rusqlite::Error) -> Self {
        BucketError::Db(e.to_string())
    }
}

impl From<serde_json::Error> for BucketError {
    fn from(e: serde_json::Error) -> Self {
        BucketError::Internal(format!("json: {e}"))
    }
}

pub(crate) fn store_err(e: ndn_lib::NdnError) -> BucketError {
    BucketError::Store(e.to_string())
}
