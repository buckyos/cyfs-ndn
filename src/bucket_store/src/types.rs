use crate::error::{BucketError, BucketResult};
use named_store::PinScope;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_METADATA_BYTES: usize = 2048;
pub const MAX_TAGS: usize = 10;
pub const MAX_BATCH_OPS: usize = 100;
pub const MAX_LIST_LIMIT: usize = 1000;
pub const DEFAULT_MAX_OBJECTS: u64 = 1_000_000;
pub const TEMP_MAX_TTL_DAYS: u32 = 30;
pub const MAX_TOKEN_TTL_MS: i64 = 7 * DAY_MS;
pub const DAY_MS: i64 = 86_400_000;
pub const MIB: u64 = 1024 * 1024;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketKind {
    /// FileObject / DirObject bindings: byte streams, exposed to S3.
    Data,
    /// Any other ObjType (messages, posts, app objects), served as object JSON.
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataPolicy {
    Documents,
    AppData,
    Collected,
    Ambient,
    Temp,
    Cache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketScope {
    User,
    Zone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolKind {
    /// All documents + collected buckets of one user.
    User,
    /// All app_data + temp buckets of one app instance (`<appid>@<owner>`).
    App,
}

macro_rules! str_enum {
    ($ty:ty { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(&self) -> &'static str {
                match self { $(Self::$variant => $s),+ }
            }
        }
        impl FromStr for $ty {
            type Err = BucketError;
            fn from_str(s: &str) -> BucketResult<Self> {
                match s {
                    $($s => Ok(Self::$variant),)+
                    other => Err(BucketError::InvalidArgument(format!("unknown {} '{}'", stringify!($ty), other))),
                }
            }
        }
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
    };
}

str_enum!(BucketKind { Data => "data", Object => "object" });
str_enum!(DataPolicy {
    Documents => "documents",
    AppData => "app_data",
    Collected => "collected",
    Ambient => "ambient",
    Temp => "temp",
    Cache => "cache",
});
str_enum!(BucketScope { User => "user", Zone => "zone" });
str_enum!(PoolKind { User => "user", App => "app" });

impl DataPolicy {
    /// How a binding retains its object (§7.1); cache bindings are not pinned.
    pub fn pin_scope(&self) -> Option<PinScope> {
        match self {
            Self::Documents | Self::AppData | Self::Collected | Self::Temp => Some(PinScope::Recursive),
            Self::Ambient => Some(PinScope::Skeleton),
            Self::Cache => None,
        }
    }

    /// The data belongs to the user, not to the app writing it.
    pub fn user_owned(&self) -> bool {
        matches!(self, Self::Documents | Self::Collected)
    }

    pub fn pool(&self) -> Option<PoolKind> {
        match self {
            Self::Documents | Self::Collected => Some(PoolKind::User),
            Self::AppData | Self::Temp => Some(PoolKind::App),
            Self::Ambient | Self::Cache => None,
        }
    }

    pub fn default_versioning(&self) -> bool {
        matches!(self, Self::Documents | Self::Collected)
    }

    pub fn versioning_settable(&self) -> bool {
        matches!(self, Self::Documents | Self::AppData | Self::Collected)
    }

    /// `quota_mib` is a hard limit here, a soft one for ambient/cache, absent for user-pool tiers.
    pub fn quota_settable(&self) -> bool {
        !self.user_owned()
    }

    pub fn soft_quota(&self) -> bool {
        matches!(self, Self::Ambient | Self::Cache)
    }

    pub fn origin_required(&self) -> bool {
        matches!(self, Self::Collected | Self::Ambient)
    }

    pub fn object_expiry_allowed(&self) -> bool {
        !self.user_owned()
    }

    pub fn default_ttl_settable(&self) -> bool {
        matches!(self, Self::Ambient | Self::Temp | Self::Cache)
    }

    pub fn default_max_object_bytes(&self) -> Option<u64> {
        match self {
            Self::Temp => Some(2 * 1024 * MIB),
            Self::Cache => Some(256 * MIB),
            _ => None,
        }
    }

    /// Lifecycle rules a bucket of this tier accepts (§4.5.3).
    pub fn check_lifecycle(&self, rules: &[LifecycleRule]) -> BucketResult<()> {
        for rule in rules {
            let ok = match self {
                Self::Documents | Self::Collected => matches!(rule, LifecycleRule::NoncurrentExpire { .. }),
                Self::Temp => false,
                _ => true,
            };
            if !ok {
                return Err(BucketError::PolicyViolation(format!(
                    "a {} bucket does not accept the lifecycle rule {:?}",
                    self, rule
                )));
            }
        }
        Ok(())
    }

    /// Included in backups and user migration (§10.2); ambient carries descriptors only.
    pub fn backed_up(&self) -> bool {
        !matches!(self, Self::Temp | Self::Cache)
    }
}

/// `<scope>/<appid>/<owner>/<name>`; zone buckets use the owner `_`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BucketId {
    pub scope: BucketScope,
    pub appid: String,
    pub owner: String,
    pub name: String,
}

pub const ZONE_OWNER: &str = "_";

fn valid_component(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

impl BucketId {
    pub fn new(scope: BucketScope, appid: &str, owner: &str, name: &str) -> BucketResult<Self> {
        let id = BucketId { scope, appid: appid.into(), owner: owner.into(), name: name.into() };
        id.validate()?;
        Ok(id)
    }

    pub fn user(appid: &str, owner: &str, name: &str) -> BucketResult<Self> {
        Self::new(BucketScope::User, appid, owner, name)
    }

    pub fn zone(appid: &str, name: &str) -> BucketResult<Self> {
        Self::new(BucketScope::Zone, appid, ZONE_OWNER, name)
    }

    fn validate(&self) -> BucketResult<()> {
        for (what, v) in [("appid", &self.appid), ("owner", &self.owner), ("name", &self.name)] {
            if !valid_component(v) {
                return Err(BucketError::InvalidArgument(format!("invalid bucket {what} '{v}'")));
            }
        }
        match self.scope {
            BucketScope::Zone if self.owner != ZONE_OWNER => {
                Err(BucketError::InvalidArgument("a zone bucket's owner is '_'".into()))
            }
            BucketScope::User if self.owner == ZONE_OWNER => {
                Err(BucketError::InvalidArgument("a user bucket needs a user owner".into()))
            }
            _ => Ok(()),
        }
    }

    /// Splits `<scope>/<appid>/<owner>/<name>/<key...>` into the bucket and the rest.
    pub fn split_path(path: &str) -> BucketResult<(BucketId, &str)> {
        let mut parts = path.splitn(5, '/');
        let (Some(scope), Some(appid), Some(owner), Some(name)) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return Err(BucketError::InvalidArgument(format!("'{path}' does not start with a bucket id")));
        };
        let id = BucketId::new(scope.parse()?, appid, owner, name)?;
        Ok((id, parts.next().unwrap_or("")))
    }

    pub fn pin_owner(&self) -> String {
        format!("bucket:{self}")
    }

    /// RBAC resource of a key (or prefix) in this bucket.
    pub fn resource(&self, key: &str) -> String {
        format!("obj://bucket/{self}/{key}")
    }

    /// The pool this bucket's bytes count against, if any.
    pub fn pool_id(&self, kind: PoolKind) -> String {
        match kind {
            PoolKind::User => self.owner.clone(),
            PoolKind::App => format!("{}@{}", self.appid, self.owner),
        }
    }
}

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}/{}", self.scope, self.appid, self.owner, self.name)
    }
}

impl FromStr for BucketId {
    type Err = BucketError;
    fn from_str(s: &str) -> BucketResult<Self> {
        match BucketId::split_path(s)? {
            (id, "") => Ok(id),
            _ => Err(BucketError::InvalidArgument(format!("'{s}' is not a bucket id"))),
        }
    }
}

impl Serialize for BucketId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BucketId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// §5.1: 1..=1024 UTF-8 bytes, no leading `/`, no NUL, no `..` segment.
pub fn validate_key(key: &str) -> BucketResult<()> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(BucketError::InvalidKey(format!("a key has 1 to {MAX_KEY_BYTES} bytes")));
    }
    if key.starts_with('/') || key.contains('\0') || key.split('/').any(|seg| seg == "..") {
        return Err(BucketError::InvalidKey(format!("invalid key '{key}'")));
    }
    Ok(())
}

/// Declaration of a bucket in an app's install config (`install_config.buckets.<name>`, §4.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketDecl {
    pub kind: BucketKind,
    pub data_policy: DataPolicy,
    #[serde(default = "default_scope", skip_serializing_if = "is_default_scope")]
    pub scope: BucketScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versioning: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_mib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_ttl_days: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle: Vec<LifecycleRule>,
}

fn default_scope() -> BucketScope {
    BucketScope::User
}

/// Defaults stay out of the serialized form, so a declaration round-trips to the
/// bytes an AppDoc was signed with.
fn is_default_scope(scope: &BucketScope) -> bool {
    *scope == BucketScope::User
}

impl BucketDecl {
    /// Tier rules of §4.5.3 that a declaration must satisfy.
    pub fn validate(&self, name: &str) -> BucketResult<()> {
        if !valid_component(name) {
            return Err(BucketError::InvalidArgument(format!("invalid bucket name '{name}'")));
        }
        let p = self.data_policy;
        if self.quota_mib.is_some() && !p.quota_settable() {
            return Err(BucketError::PolicyViolation(format!("a {p} bucket uses the user's quota; quota_mib is not allowed")));
        }
        if self.versioning == Some(true) && !p.versioning_settable() {
            return Err(BucketError::PolicyViolation(format!("a {p} bucket cannot be versioned")));
        }
        match (p, self.default_ttl_days) {
            (DataPolicy::Temp, None) => {
                return Err(BucketError::PolicyViolation("a temp bucket must declare default_ttl_days".into()))
            }
            (DataPolicy::Temp, Some(days)) if days == 0 || days > TEMP_MAX_TTL_DAYS => {
                return Err(BucketError::PolicyViolation(format!("default_ttl_days of a temp bucket is 1..={TEMP_MAX_TTL_DAYS}")))
            }
            (_, Some(_)) if !p.default_ttl_settable() => {
                return Err(BucketError::PolicyViolation(format!("a {p} bucket cannot declare default_ttl_days")))
            }
            _ => {}
        }
        p.check_lifecycle(&self.lifecycle)
    }

    /// Whether the app owning this bucket must declare `storage_quota_mib` (D17).
    pub fn needs_app_pool(&self) -> bool {
        self.data_policy.pool() == Some(PoolKind::App)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "rule")]
pub enum LifecycleRule {
    /// Unbind current versions older than `days` under `prefix`.
    Expire {
        #[serde(default)]
        prefix: String,
        days: u32,
    },
    /// Unbind noncurrent versions `days` after they became noncurrent.
    NoncurrentExpire { days: u32 },
    /// Drop delete markers that no longer hide any version.
    ExpiredDeleteMarker,
}

/// Where a collected / ambient object came from (§4.5.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    pub author: String,
    pub source: String,
    pub collected_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<String>,
}

impl Origin {
    /// Only the shape is checked; signatures are the reader's business.
    pub fn validate(&self) -> BucketResult<()> {
        if name_lib::DID::from_str(&self.author).is_err() {
            return Err(BucketError::InvalidArgument(format!("origin.author '{}' is not a DID", self.author)));
        }
        if !["https://", "cyfs://", "obj://", "did:"].iter().any(|prefix| self.source.starts_with(prefix)) {
            return Err(BucketError::InvalidArgument("origin.source is an https://, cyfs://, obj:// address or a Zone DID".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conditions {
    /// `"*"`: write only when the key does not exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_none_match: Option<String>,
    /// The current etag (ObjId, or the S3 MD5 form) must equal this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_match: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_version: Option<u64>,
}

impl Conditions {
    pub fn create_only() -> Self {
        Conditions { if_none_match: Some("*".into()), ..Default::default() }
    }

    pub fn is_empty(&self) -> bool {
        self.if_none_match.is_none() && self.if_match.is_none() && self.if_version.is_none()
    }
}

/// A binding row as clients see it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub bucket_id: BucketId,
    pub key: String,
    pub version: u64,
    pub is_current: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_delete_marker: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obj_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obj_type: Option<String>,
    pub size: u64,
    /// Native etag: the ObjId.
    pub etag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag_md5: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    /// HTTP caching headers stored and returned verbatim (`Cache-Control`, `Expires`, ...).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub http_headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_access_at: Option<i64>,
    pub created_at: i64,
    pub created_by: String,
}

/// What `bucket.put` binds (§12.1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PutRequest {
    pub key: String,
    #[serde(default)]
    pub obj_id: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub http_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub origin: Option<Origin>,
    #[serde(default)]
    pub etag_md5: Option<String>,
    #[serde(default, flatten)]
    pub conditions: Conditions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PutResult {
    pub version: u64,
    pub etag: String,
    /// The key did not exist before.
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteResult {
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub delete_marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Local,
    Evicted,
    Remote,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListRequest {
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub start_after: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListResult {
    pub items: Vec<Binding>,
    pub common_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_start_after: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataDirective {
    #[default]
    Copy,
    Replace,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CopyRequest {
    pub src_bucket_id: String,
    pub src_key: String,
    #[serde(default)]
    pub src_version: Option<u64>,
    pub dst_bucket_id: String,
    pub dst_key: String,
    #[serde(default)]
    pub metadata_directive: MetadataDirective,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub origin: Option<Origin>,
    #[serde(default, flatten)]
    pub conditions: Conditions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BatchOp {
    Put(PutRequest),
    Delete {
        key: String,
        #[serde(default)]
        version: Option<u64>,
        #[serde(default)]
        if_match: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BatchResult {
    Put(PutResult),
    Delete(DeleteResult),
}

/// A bucket row with its statistics (`bucket.stat`, `bucket.list`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketInfo {
    pub bucket_id: BucketId,
    pub kind: BucketKind,
    pub data_policy: DataPolicy,
    pub scope: BucketScope,
    pub appid: String,
    pub owner: String,
    pub name: String,
    pub versioning: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_ttl_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle: Vec<LifecycleRule>,
    pub unique_bytes: u64,
    pub key_bytes: u64,
    pub key_count: u64,
    pub version_count: u64,
    pub snapshot_only_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orphaned_at: Option<i64>,
    pub created_at: i64,
}

impl BucketInfo {
    pub fn decl(&self) -> BucketDecl {
        BucketDecl {
            kind: self.kind,
            data_policy: self.data_policy,
            scope: self.scope,
            versioning: Some(self.versioning),
            quota_mib: self.quota_bytes.map(|b| b / MIB),
            max_objects: self.max_objects,
            default_ttl_days: self.default_ttl_secs.map(|s| (s / 86_400) as u32),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_serialize_without_defaults() {
        for text in [
            r#"{"kind":"data","data_policy":"app_data"}"#,
            r#"{"kind":"object","data_policy":"documents","scope":"zone","versioning":false}"#,
        ] {
            let decl: BucketDecl = serde_json::from_str(text).unwrap();
            assert_eq!(serde_json::to_string(&decl).unwrap(), text);
        }
    }

    #[test]
    fn bucket_id_round_trip() {
        let id: BucketId = "user/photos.app.bns.did/alice/library".parse().unwrap();
        assert_eq!(id.appid, "photos.app.bns.did");
        assert_eq!(id.to_string(), "user/photos.app.bns.did/alice/library");
        assert_eq!(id.pin_owner(), "bucket:user/photos.app.bns.did/alice/library");
        let (zone, key) = BucketId::split_path("zone/homestation/_/published/a/b.png").unwrap();
        assert_eq!(zone.scope, BucketScope::Zone);
        assert_eq!(key, "a/b.png");
        assert!("zone/x/alice/n".parse::<BucketId>().is_err());
        assert!("user/x/_/n".parse::<BucketId>().is_err());
        assert!("user/x/a".parse::<BucketId>().is_err());
        assert!("user/x/a/b/c".parse::<BucketId>().is_err());
    }

    #[test]
    fn keys() {
        assert!(validate_key("a/b/c.txt").is_ok());
        assert!(validate_key("").is_err());
        assert!(validate_key("/a").is_err());
        assert!(validate_key("a/../b").is_err());
        assert!(validate_key("a\0b").is_err());
        assert!(validate_key(&"x".repeat(1025)).is_err());
    }

    #[test]
    fn decl_rules() {
        let decl = |policy, extra: serde_json::Value| {
            let mut v = serde_json::json!({"kind": "data", "data_policy": policy});
            v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            serde_json::from_value::<BucketDecl>(v).unwrap()
        };
        assert!(decl("documents", serde_json::json!({})).validate("lib").is_ok());
        assert!(decl("documents", serde_json::json!({"quota_mib": 1})).validate("lib").is_err());
        assert!(decl("temp", serde_json::json!({})).validate("t").is_err());
        assert!(decl("temp", serde_json::json!({"default_ttl_days": 31})).validate("t").is_err());
        assert!(decl("temp", serde_json::json!({"default_ttl_days": 7})).validate("t").is_ok());
        assert!(decl("cache", serde_json::json!({"versioning": true})).validate("c").is_err());
        assert!(decl("documents", serde_json::json!({"lifecycle": [{"rule": "expire", "days": 3}]})).validate("d").is_err());
        assert!(decl("documents", serde_json::json!({"lifecycle": [{"rule": "noncurrent_expire", "days": 3}]})).validate("d").is_ok());
        assert_eq!(decl("app_data", serde_json::json!({})).scope, BucketScope::User);
        assert!(decl("app_data", serde_json::json!({})).needs_app_pool());
    }
}
