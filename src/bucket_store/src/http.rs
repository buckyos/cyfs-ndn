//! §6.2 / §12.2 data plane: `GET | HEAD | PUT /bucket/v1/<bucket_id>/<key>`.
//! The host authenticates the caller and routes here; access is decided on the
//! bucket and key, never on the object.

use crate::access::*;
use crate::content::store_bytes;
use crate::error::*;
use crate::events::ObjectEventKind;
use crate::store::BucketStore;
use crate::types::*;
use buckyos_http_server::ServerError;
use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use ndn_lib::{load_named_obj, ChunkId, FileObject, ObjId, CHUNK_DEFAULT_SIZE, OBJ_TYPE_FILE};

pub type HttpBody = BoxBody<Bytes, ServerError>;

pub const PATH_PREFIX: &str = "/bucket/v1/";
/// Single-request PUT limit; larger files go through tus + `auto_bind`.
pub const MAX_PUT_BYTES: u64 = CHUNK_DEFAULT_SIZE;
pub const OBJECT_CONTENT_TYPE: &str = "application/cyfs+json";

/// Stored and returned verbatim (S3 semantics, §5.6).
const PASS_HEADERS: [&str; 5] = ["cache-control", "expires", "content-disposition", "content-language", "content-encoding"];
const META_PREFIX: &str = "x-bucky-meta-";

fn body(bytes: impl Into<Bytes>) -> HttpBody {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

pub fn json_response(status: StatusCode, value: &serde_json::Value) -> Response<HttpBody> {
    let mut r = Response::new(body(value.to_string()));
    *r.status_mut() = status;
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

pub fn error_response(e: &BucketError) -> Response<HttpBody> {
    let mut v = serde_json::json!({"error": e.code(), "message": e.to_string()});
    if let Some(data) = e.data() {
        v["data"] = data;
    }
    json_response(StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), &v)
}

pub fn http_date(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// `bytes=a-b` / `a-` / `-n`; multi-range requests get the whole body.
pub fn parse_range(h: Option<&str>, len: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(h) = h else { return Ok(None) };
    let spec = h.strip_prefix("bytes=").ok_or(())?;
    if spec.contains(',') {
        return Ok(None);
    }
    let (a, b) = spec.split_once('-').ok_or(())?;
    if len == 0 {
        return Err(());
    }
    match (a.trim(), b.trim()) {
        ("", n) => {
            let n: u64 = n.parse().map_err(|_| ())?;
            if n == 0 {
                return Err(());
            }
            Ok(Some((len.saturating_sub(n), len - 1)))
        }
        (s, "") => {
            let s: u64 = s.parse().map_err(|_| ())?;
            if s >= len {
                return Err(());
            }
            Ok(Some((s, len - 1)))
        }
        (s, e) => {
            let (s, e): (u64, u64) = (s.parse().map_err(|_| ())?, e.parse().map_err(|_| ())?);
            if s > e || s >= len {
                return Err(());
            }
            Ok(Some((s, e.min(len - 1))))
        }
    }
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty())
}

fn none_match(h: &HeaderMap, etag: &str) -> bool {
    header_str(h, "if-none-match").is_some_and(|v| v.split(',').any(|t| {
        let t = t.trim().trim_start_matches("W/").trim_matches('"');
        t == "*" || t == etag
    }))
}

/// Parses a deletion time: integer milliseconds or RFC 3339.
pub fn parse_expires_at(v: &str) -> BucketResult<i64> {
    if let Ok(ms) = v.parse::<i64>() {
        return Ok(ms);
    }
    chrono::DateTime::parse_from_rfc3339(v)
        .map(|t| t.timestamp_millis())
        .map_err(|_| BucketError::InvalidArgument(format!("x-bucky-expires-at '{v}' is neither milliseconds nor RFC 3339")))
}

/// The binding fields a PUT carries in headers.
pub fn put_request_from_headers(key: &str, headers: &HeaderMap) -> BucketResult<PutRequest> {
    let mut req = PutRequest { key: key.to_string(), ..Default::default() };
    req.content_type = header_str(headers, "content-type").map(String::from);
    for (name, value) in headers {
        let Ok(value) = value.to_str() else { continue };
        let name = name.as_str();
        if let Some(meta) = name.strip_prefix(META_PREFIX) {
            req.metadata.insert(meta.to_string(), value.to_string());
        } else if PASS_HEADERS.contains(&name) {
            req.http_headers.insert(name.to_string(), value.to_string());
        }
    }
    if let Some(v) = header_str(headers, "x-bucky-expires-at") {
        req.expires_at = Some(parse_expires_at(v)?);
    }
    if let Some(v) = header_str(headers, "x-bucky-origin") {
        req.origin = Some(serde_json::from_str(v).map_err(|e| BucketError::InvalidArgument(format!("x-bucky-origin: {e}")))?);
    }
    if let Some(v) = header_str(headers, "x-bucky-tags") {
        req.tags = serde_json::from_str(v).map_err(|e| BucketError::InvalidArgument(format!("x-bucky-tags: {e}")))?;
    }
    req.conditions.if_none_match = header_str(headers, "if-none-match").map(String::from);
    req.conditions.if_match = header_str(headers, "if-match").map(String::from);
    req.conditions.if_version = header_str(headers, "x-bucky-if-version")
        .map(|v| v.parse().map_err(|_| BucketError::InvalidArgument("x-bucky-if-version".into())))
        .transpose()?;
    Ok(req)
}

fn file_name_of(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

impl BucketStore {
    /// Read access and the binding to serve.
    pub async fn open_for_read(&self, id: &BucketId, caller: &Caller, key: &str, version: Option<u64>) -> BucketResult<(BucketInfo, Binding)> {
        validate_key(key)?;
        let bucket = self.bucket(id)?;
        self.access().authorize(&bucket, caller, Action::Read, Target::Key(key)).await?;
        let b = self.lookup(&bucket, key, version)?;
        Ok((bucket, b))
    }

    /// Content of an unavailable binding: cache misses unbind lazily (§4.5.2).
    async fn missing(&self, bucket: &BucketInfo, b: &Binding) -> BucketError {
        match bucket.data_policy {
            DataPolicy::Cache => {
                if let Err(e) = self.delete_system(bucket, &b.key, None, ObjectEventKind::ObjectExpired).await {
                    log::warn!("drop evicted {}/{}: {e}", bucket.bucket_id, b.key);
                }
                BucketError::ContentEvicted(b.key.clone())
            }
            DataPolicy::Ambient => BucketError::OriginUnavailable(b.key.clone()),
            _ => BucketError::ContentNotAvailable(b.key.clone()),
        }
    }

    /// Serves a binding's content: FileObject bytes with Range, other objects as JSON.
    pub async fn serve_read(&self, bucket: &BucketInfo, b: &Binding, req_headers: &HeaderMap, head: bool) -> Response<HttpBody> {
        match self.serve_inner(bucket, b, req_headers, head).await {
            Ok(r) => r,
            Err(e) => error_response(&e),
        }
    }

    async fn serve_inner(&self, bucket: &BucketInfo, b: &Binding, req_headers: &HeaderMap, head: bool) -> BucketResult<Response<HttpBody>> {
        let obj_id = ObjId::new(b.obj_id.as_deref().unwrap_or_default()).map_err(store_err)?;
        if matches!(bucket.data_policy, DataPolicy::Cache | DataPolicy::Ambient) {
            self.touch_binding(b);
        }
        self.ndm.touch(&obj_id).await;
        let etag = format!("\"{}\"", b.etag);
        let mut r = Response::new(body(Bytes::new()));
        let h = r.headers_mut();
        let set = |h: &mut HeaderMap, k: header::HeaderName, v: &str| {
            if let Ok(v) = HeaderValue::from_str(v) {
                h.insert(k, v);
            }
        };
        set(h, header::ETAG, &etag);
        set(h, header::LAST_MODIFIED, &http_date(b.created_at));
        set(h, header::CACHE_CONTROL, "private, no-cache");
        for (k, v) in &b.http_headers {
            if let (Ok(k), Ok(v)) = (header::HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
                h.insert(k, v);
            }
        }
        set(h, header::HeaderName::from_static("x-bucky-version"), &b.version.to_string());
        set(h, header::HeaderName::from_static("x-bucky-obj-id"), &b.etag);
        for (k, v) in &b.metadata {
            if let Ok(name) = header::HeaderName::from_bytes(format!("{META_PREFIX}{k}").as_bytes()) {
                set(h, name, v);
            }
        }
        if none_match(req_headers, &b.etag) {
            *r.status_mut() = StatusCode::NOT_MODIFIED;
            return Ok(r);
        }
        if obj_id.obj_type != OBJ_TYPE_FILE {
            let Ok(text) = self.ndm.get_object(&obj_id).await else { return Err(self.missing(bucket, b).await) };
            set(r.headers_mut(), header::CONTENT_TYPE, b.content_type.as_deref().unwrap_or(OBJECT_CONTENT_TYPE));
            set(r.headers_mut(), header::CONTENT_LENGTH, &text.len().to_string());
            if !head {
                *r.body_mut() = body(text);
            }
            return Ok(r);
        }
        let Ok(text) = self.ndm.get_object(&obj_id).await else { return Err(self.missing(bucket, b).await) };
        let file: FileObject = load_named_obj(&text).map_err(store_err)?;
        let mime = b
            .content_type
            .clone()
            .or_else(|| file.meta.get("mime").and_then(|v| v.as_str()).map(String::from))
            .unwrap_or_else(|| "application/octet-stream".into());
        set(r.headers_mut(), header::CONTENT_TYPE, &mime);
        set(r.headers_mut(), header::ACCEPT_RANGES, "bytes");
        let len = file.size;
        let range = match parse_range(header_str(req_headers, "range"), len) {
            Ok(r) => r,
            Err(()) => {
                *r.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                set(r.headers_mut(), header::CONTENT_RANGE, &format!("bytes */{len}"));
                return Ok(r);
            }
        };
        let (start, end) = range.unwrap_or((0, len.saturating_sub(1)));
        let count = if len == 0 { 0 } else { end - start + 1 };
        if let Some((s, e)) = range {
            *r.status_mut() = StatusCode::PARTIAL_CONTENT;
            set(r.headers_mut(), header::CONTENT_RANGE, &format!("bytes {s}-{e}/{len}"));
        }
        set(r.headers_mut(), header::CONTENT_LENGTH, &count.to_string());
        if head || count == 0 {
            return Ok(r);
        }
        let content = ObjId::new(&file.content).map_err(store_err)?;
        let opened = if content.is_chunk_list() {
            self.ndm.open_chunklist_reader(&content, start).await
        } else {
            self.ndm.open_chunk_reader(&ChunkId::from_obj_id(&content), start).await
        };
        let Ok((reader, _)) = opened else { return Err(self.missing(bucket, b).await) };
        *r.body_mut() = named_store::chunk_reader_to_body(reader, count);
        Ok(r)
    }

    /// `PUT /bucket/v1/...`: stores up to 32 MiB as a FileObject and binds it.
    pub async fn put_bytes(&self, id: &BucketId, caller: &Caller, key: &str, bytes: Bytes, headers: &HeaderMap) -> BucketResult<PutResult> {
        validate_key(key)?;
        if bytes.len() as u64 > MAX_PUT_BYTES {
            return Err(BucketError::ObjectTooLarge { size: bytes.len() as u64, limit: MAX_PUT_BYTES });
        }
        let bucket = self.bucket(id)?;
        if bucket.kind != BucketKind::Data {
            return Err(BucketError::KindMismatch("PUT of bytes needs a data bucket; bind objects with bucket.put".into()));
        }
        let mut req = put_request_from_headers(key, headers)?;
        // Fail before storing bytes the caller may not bind.
        self.access().authorize_any(&bucket, caller, &[Action::Write, Action::Create], Target::Key(key), false).await?;
        let mime = req.content_type.clone();
        let (_, obj_id) = store_bytes(&self.ndm, file_name_of(key), &bytes, mime.as_deref()).await?;
        req.obj_id = obj_id.to_string();
        self.put_trusted(&bucket, caller, req).await
    }
}
