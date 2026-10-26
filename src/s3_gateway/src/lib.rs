//! S3 compatible face of application object buckets (buckyos
//! `doc/arch/应用对象桶 Bucket 设计.md` §12.3): SigV4 with access keys bound
//! to a `(user, app)`, the first-phase operation set, multipart uploads whose
//! parts are chunks of the named store. Semantics stay those of `bucket_store`;
//! only `kind = data` buckets of the key's user and app are visible.

pub mod chunked;
pub mod error;
mod ops;
pub mod sigv4;
pub mod writer;
pub mod xml;

#[cfg(test)]
mod tests;

pub use error::{S3Error, S3Result};

use bucket_store::http::HttpBody;
use bucket_store::{Action, BucketInfo, BucketKind, BucketScope, BucketStore, Caller, IdentityPolicy, S3Key, S3KeyScope};
use buckyos_http_server::ServerError;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use rand::RngCore;
use sigv4::SigningContext;
use std::sync::Arc;

pub const DEFAULT_REGION: &str = "us-east-1";
/// Single-request PUT and part size limit (as on S3).
pub const MAX_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;
pub const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
pub const MAX_PARTS: u32 = 10_000;
/// Uploaded parts are held this long; abandoned uploads are aborted after it.
pub const UPLOAD_HOLD_SECS: u64 = 7 * 24 * 3600;

/// Request bodies stream (`Send`, not `Sync`: axum's body is not).
pub type RequestBody = http_body_util::combinators::UnsyncBoxBody<Bytes, ServerError>;
pub type S3Request = http::Request<RequestBody>;

/// What the host (nfs-server) provides: the caller of a key's `(user, app)`
/// with its identity policy, and lazily created declared buckets.
#[async_trait::async_trait]
pub trait S3Host: Send + Sync {
    async fn caller(&self, key: &S3Key) -> Caller;
    async fn sync_buckets(&self, appid: &str, user: &str);
}

pub struct S3Gateway {
    pub store: Arc<BucketStore>,
    pub region: String,
}

/// A key's scope narrows the identity layer (§4.4.8).
struct ScopedPolicy {
    inner: Arc<dyn IdentityPolicy>,
    scope: S3KeyScope,
}

#[async_trait::async_trait]
impl IdentityPolicy for ScopedPolicy {
    async fn allowed(&self, bucket: &BucketInfo, key: &str, action: Action) -> bool {
        let id = bucket.bucket_id.to_string();
        if !self.scope.buckets.is_empty() && !self.scope.buckets.contains(&id) {
            return false;
        }
        if let Some(p) = &self.scope.prefix {
            if !(key.starts_with(p.as_str()) || (action == Action::List && p.starts_with(key))) {
                return false;
            }
        }
        if self.scope.actions.is_some_and(|a| !a.contains(action)) {
            return false;
        }
        self.inner.allowed(bucket, key, action).await
    }

    async fn holds(&self, obj_id: &str) -> bool {
        self.inner.holds(obj_id).await
    }
}

/// `<appid>-<name>`, lower case, `[a-z0-9-]` only (S3 bucket naming).
pub fn s3_bucket_name(b: &BucketInfo) -> String {
    let slug = |s: &str| -> String { s.chars().map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' }).collect() };
    let mut name = format!("{}-{}", slug(&b.appid), slug(&b.name));
    name.truncate(63);
    name.trim_matches('-').to_string()
}

/// The S3 form of a binding's etag: the MD5 it was stored with, else a value
/// with a `-1` suffix so clients do not take it for an MD5 (§5.3).
pub fn s3_etag(b: &bucket_store::Binding) -> String {
    match &b.etag_md5 {
        Some(m) => m.clone(),
        None => {
            let id = b.obj_id.as_deref().unwrap_or_default();
            let hash = id.split_once(':').map(|(_, h)| h).unwrap_or(id);
            format!("{}-1", &hash[..hash.len().min(32)])
        }
    }
}

pub(crate) struct Ctx<'a> {
    pub key: S3Key,
    pub caller: Caller,
    pub signing: SigningContext,
    pub payload_hash: String,
    pub query: Vec<(String, String)>,
    pub headers: HeaderMap,
    pub method: Method,
    pub host: &'a dyn S3Host,
}

impl Ctx<'_> {
    pub fn q(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    pub fn has(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty())
    }
}

pub(crate) fn body_of(bytes: impl Into<Bytes>) -> HttpBody {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

pub(crate) fn xml_response(status: StatusCode, xml: String) -> Response<HttpBody> {
    let mut r = Response::new(body_of(xml));
    *r.status_mut() = status;
    r.headers_mut().insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    r
}

pub(crate) fn empty(status: StatusCode) -> Response<HttpBody> {
    let mut r = Response::new(body_of(Bytes::new()));
    *r.status_mut() = status;
    r
}

fn request_id() -> String {
    let mut b = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode_upper(b)
}

pub fn error_response(e: &S3Error, resource: &str, request_id: &str) -> Response<HttpBody> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>{}</RequestId></Error>",
        e.code,
        xml::escape(&e.message),
        xml::escape(resource),
        request_id
    );
    xml_response(e.status, body)
}

impl S3Gateway {
    pub fn new(store: Arc<BucketStore>) -> Self {
        S3Gateway { store, region: DEFAULT_REGION.into() }
    }

    /// Serves one S3 request; `prefix` is the path the endpoint is mounted at (`/s3`, or `` on `s3.<zone>`).
    pub async fn handle(&self, host: &dyn S3Host, req: S3Request, prefix: &str) -> Response<HttpBody> {
        let id = request_id();
        let resource = req.uri().path().to_string();
        let head = req.method() == Method::HEAD;
        let mut resp = match self.serve(host, req, prefix).await {
            Ok(r) => r,
            Err(e) => {
                if e.status.is_server_error() {
                    log::warn!("s3 {resource}: {} {}", e.code, e.message);
                } else {
                    log::debug!("s3 {resource}: {} {}", e.code, e.message);
                }
                let mut r = error_response(&e, &resource, &id);
                if head {
                    *r.body_mut() = body_of(Bytes::new());
                }
                r
            }
        };
        if let Ok(v) = HeaderValue::from_str(&id) {
            resp.headers_mut().insert("x-amz-request-id", v);
        }
        resp
    }

    async fn serve(&self, host: &dyn S3Host, req: S3Request, prefix: &str) -> S3Result<Response<HttpBody>> {
        let (parts, body) = req.into_parts();
        let raw_path = parts.uri.path().to_string();
        let query = sigv4::parse_query(parts.uri.query().unwrap_or(""));
        let signed = sigv4::parse(&query, &parts.headers)?
            .ok_or_else(|| S3Error::auth("AccessDenied", "anonymous access is not allowed; sign the request with an access key"))?;
        let key = self
            .store
            .s3_key(&signed.credential.access_key_id)?
            .ok_or_else(|| S3Error::auth("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records."))?;
        let now = chrono::Utc::now().timestamp();
        let signing = sigv4::verify(&signed, &key.secret, parts.method.as_str(), &raw_path, &query, &parts.headers, now)?;
        let mut caller = host.caller(&key).await;
        if let Some(scope) = &key.scope {
            caller.identity = Arc::new(ScopedPolicy { inner: caller.identity.clone(), scope: scope.clone() });
        }
        let rest = raw_path.strip_prefix(prefix).unwrap_or(&raw_path).trim_start_matches('/');
        let (bucket, object) = match rest.split_once('/') {
            Some((b, k)) => (sigv4::percent_decode(b), sigv4::percent_decode(k)),
            None => (sigv4::percent_decode(rest), String::new()),
        };
        let ctx = Ctx {
            key,
            caller,
            signing,
            payload_hash: signed.payload_hash.clone(),
            query,
            headers: parts.headers,
            method: parts.method,
            host,
        };
        if bucket.is_empty() {
            return match ctx.method {
                Method::GET => self.list_buckets(&ctx).await,
                _ => Err(S3Error::new(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "only ListBuckets is served at the root")),
            };
        }
        if object.is_empty() {
            return self.bucket_op(&ctx, &bucket, body).await;
        }
        self.object_op(&ctx, &bucket, &object, body).await
    }

    pub(crate) async fn buckets(&self, ctx: &Ctx<'_>) -> S3Result<Vec<BucketInfo>> {
        ctx.host.sync_buckets(&ctx.key.appid, &ctx.key.user_id).await;
        Ok(self
            .store
            .list_buckets(Some(&ctx.key.user_id), Some(&ctx.key.appid), Some(BucketScope::User))?
            .into_iter()
            .filter(|b| b.kind == BucketKind::Data)
            .filter(|b| ctx.key.scope.as_ref().map_or(true, |s| s.buckets.is_empty() || s.buckets.contains(&b.bucket_id.to_string())))
            .collect())
    }

    pub(crate) async fn resolve(&self, ctx: &Ctx<'_>, name: &str) -> S3Result<BucketInfo> {
        self.buckets(ctx).await?.into_iter().find(|b| s3_bucket_name(b) == name).ok_or_else(|| S3Error::no_such_bucket(name))
    }

    /// Aborts multipart uploads older than the part hold (daily by the host).
    pub async fn abort_stale_uploads(&self) -> usize {
        let Ok(ids) = self.store.stale_multipart(UPLOAD_HOLD_SECS as i64 * 1000) else { return 0 };
        for id in &ids {
            self.drop_upload(id).await;
        }
        ids.len()
    }

    pub(crate) async fn drop_upload(&self, upload_id: &str) {
        if let Err(e) = self.store.ndm().unpin_owner(&format!("s3-upload:{upload_id}")).await {
            log::warn!("release parts of upload {upload_id}: {e}");
        }
        if let Err(e) = self.store.delete_multipart(upload_id) {
            log::warn!("drop upload {upload_id}: {e}");
        }
    }
}
