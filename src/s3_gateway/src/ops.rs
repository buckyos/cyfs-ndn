//! The first-phase S3 operation set (§12.3), on top of `BucketStore`.

use crate::chunked::ChunkedDecoder;
use crate::sigv4::{percent_decode, STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER, UNSIGNED_PAYLOAD};
use crate::writer::{ObjectWriter, Written};
use crate::xml::{self, document, iso8601};
use crate::*;
use base64::Engine;
use bucket_store::http::{http_date, parse_expires_at};
use bucket_store::{Action, BucketError, Conditions, CopyRequest, ListRequest, MetadataDirective, MultipartPart, PutRequest};
use http::header;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_SMALL_BODY: usize = 2 * 1024 * 1024;
const MAX_DELETE_OBJECTS: usize = 1000;
const PASS_HEADERS: [&str; 5] = ["cache-control", "expires", "content-disposition", "content-language", "content-encoding"];

enum Payload {
    Raw(Option<String>),
    Chunked(ChunkedDecoder),
}

struct BodyReader {
    body: RequestBody,
    payload: Payload,
    sha: Sha256,
}

impl BodyReader {
    fn new(ctx: &Ctx<'_>, body: RequestBody) -> S3Result<Self> {
        let payload = match ctx.payload_hash.as_str() {
            STREAMING_SIGNED | STREAMING_SIGNED_TRAILER => Payload::Chunked(ChunkedDecoder::new(Some(ctx.signing.clone()))),
            STREAMING_UNSIGNED_TRAILER => Payload::Chunked(ChunkedDecoder::new(None)),
            UNSIGNED_PAYLOAD => Payload::Raw(None),
            h if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) => Payload::Raw(Some(h.to_ascii_lowercase())),
            other => return Err(S3Error::invalid(format!("x-amz-content-sha256 '{other}' is not supported"))),
        };
        Ok(BodyReader { body, payload, sha: Sha256::new() })
    }

    /// The next decoded piece of the payload; `None` once it ended and checked out.
    async fn next(&mut self) -> S3Result<Option<Vec<u8>>> {
        loop {
            let Some(frame) = self.body.frame().await else {
                self.finish()?;
                return Ok(None);
            };
            let frame = frame.map_err(|e| S3Error::new(StatusCode::BAD_REQUEST, "IncompleteBody", e.to_string()))?;
            let Ok(data) = frame.into_data() else { continue };
            match &mut self.payload {
                Payload::Raw(_) => {
                    sha2::Digest::update(&mut self.sha, &data);
                    return Ok(Some(data.to_vec()));
                }
                Payload::Chunked(d) => {
                    let mut out = Vec::new();
                    d.feed(&data, &mut out)?;
                    if !out.is_empty() {
                        return Ok(Some(out));
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> S3Result<()> {
        match &mut self.payload {
            Payload::Raw(Some(want)) => {
                let got = hex::encode(sha2::Digest::finalize_reset(&mut self.sha));
                if &got != want {
                    return Err(S3Error::new(StatusCode::BAD_REQUEST, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed."));
                }
                Ok(())
            }
            Payload::Raw(None) => Ok(()),
            Payload::Chunked(d) => d.finish(),
        }
    }

    async fn read_all(mut self, limit: usize) -> S3Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(piece) = self.next().await? {
            out.extend_from_slice(&piece);
            if out.len() > limit {
                return Err(S3Error::new(StatusCode::BAD_REQUEST, "MaxMessageLengthExceeded", "request body too large"));
            }
        }
        Ok(out)
    }

    async fn write_into(mut self, mut writer: ObjectWriter) -> S3Result<Written> {
        while let Some(piece) = self.next().await? {
            writer.write(&piece).await?;
        }
        Ok(writer.finish().await?)
    }
}

fn check_content_md5(ctx: &Ctx<'_>, written: &Written) -> S3Result<()> {
    let Some(v) = ctx.header("content-md5") else { return Ok(()) };
    let want = base64::engine::general_purpose::STANDARD
        .decode(v)
        .map_err(|_| S3Error::new(StatusCode::BAD_REQUEST, "InvalidDigest", "The Content-MD5 you specified was invalid."))?;
    if want.as_slice() != written.md5 {
        return Err(S3Error::new(StatusCode::BAD_REQUEST, "BadDigest", "The Content-MD5 you specified did not match what we received."));
    }
    Ok(())
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn set(r: &mut Response<HttpBody>, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (header::HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
        r.headers_mut().insert(n, v);
    }
}

/// The binding fields an object PUT / CreateMultipartUpload carries in headers.
fn put_template(ctx: &Ctx<'_>, key: &str) -> S3Result<PutRequest> {
    let mut req = PutRequest { key: key.to_string(), ..Default::default() };
    req.content_type = ctx.header("content-type").map(String::from);
    for (name, value) in &ctx.headers {
        let Ok(value) = value.to_str() else { continue };
        if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-") {
            req.metadata.insert(meta.to_string(), value.to_string());
        } else if PASS_HEADERS.contains(&name.as_str()) && !(name == "content-encoding" && value.contains("aws-chunked")) {
            req.http_headers.insert(name.as_str().to_string(), value.to_string());
        }
    }
    if let Some(v) = ctx.header("x-bucky-expires-at") {
        req.expires_at = Some(parse_expires_at(v)?);
    }
    if let Some(v) = ctx.header("x-amz-tagging") {
        req.tags = sigv4::parse_query(v).into_iter().collect();
    }
    req.conditions = Conditions {
        if_none_match: ctx.header("if-none-match").map(String::from),
        if_match: ctx.header("if-match").map(|v| v.trim_matches('"').to_string()),
        if_version: None,
    };
    Ok(req)
}

impl S3Gateway {
    fn writer(&self, owner: String, limit: u64) -> ObjectWriter {
        ObjectWriter::new(self.store.ndm().clone(), owner, Duration::from_secs(UPLOAD_HOLD_SECS), limit)
    }

    pub(crate) async fn list_buckets(&self, ctx: &Ctx<'_>) -> S3Result<Response<HttpBody>> {
        let buckets = self.buckets(ctx).await?;
        let user = ctx.key.user_id.clone();
        let doc = document("ListAllMyBucketsResult", |x| {
            x.open("Owner").leaf("ID", &user).leaf("DisplayName", &user).close("Owner").open("Buckets");
            for b in &buckets {
                x.open("Bucket").leaf("Name", &s3_bucket_name(b)).leaf("CreationDate", &iso8601(b.created_at)).close("Bucket");
            }
            x.close("Buckets");
        });
        Ok(xml_response(StatusCode::OK, doc))
    }

    pub(crate) async fn bucket_op(&self, ctx: &Ctx<'_>, name: &str, body: RequestBody) -> S3Result<Response<HttpBody>> {
        if ctx.method == Method::PUT && !ctx.has("versioning") && !ctx.has("tagging") && !ctx.has("acl") {
            return Err(S3Error::new(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "buckets are declared by apps (install_config.buckets), not created through S3"));
        }
        let bucket = self.resolve(ctx, name).await?;
        let id = bucket.bucket_id.clone();
        match ctx.method {
            Method::HEAD => {
                let info = self.store.stat(&id, &ctx.caller).await?;
                let quota = self.store.get_quota(&id, &ctx.caller).await?;
                let mut r = empty(StatusCode::OK);
                set(&mut r, "x-amz-bucket-region", &self.region);
                set(&mut r, "x-bucky-usage-bytes", &info.unique_bytes.to_string());
                if let Some(max) = quota.bucket.max_bytes.or(quota.pool.and_then(|p| p.max_bytes)) {
                    set(&mut r, "x-bucky-quota-bytes", &max.to_string());
                }
                Ok(r)
            }
            Method::GET if ctx.has("location") => {
                self.store.stat(&id, &ctx.caller).await?;
                let region = if self.region == DEFAULT_REGION { String::new() } else { self.region.clone() };
                Ok(xml_response(StatusCode::OK, document("LocationConstraint", |x| {
                    x.raw(&xml::escape(&region));
                })))
            }
            Method::GET if ctx.has("versioning") => {
                let info = self.store.stat(&id, &ctx.caller).await?;
                Ok(xml_response(StatusCode::OK, document("VersioningConfiguration", |x| {
                    if info.versioning {
                        x.leaf("Status", "Enabled");
                    }
                })))
            }
            Method::GET if ctx.has("acl") => {
                self.store.stat(&id, &ctx.caller).await?;
                Ok(self.owner_acl(ctx))
            }
            Method::GET if ctx.has("uploads") => self.list_uploads(ctx, &bucket, name).await,
            Method::GET if ctx.has("tagging") || ctx.has("policy") || ctx.has("cors") || ctx.has("lifecycle") || ctx.has("website") => {
                Err(S3Error::not_implemented("this bucket sub-resource"))
            }
            Method::GET => self.list_objects(ctx, &bucket, name).await,
            Method::POST if ctx.has("delete") => self.delete_objects(ctx, &bucket, body).await,
            Method::DELETE if ctx.query.is_empty() => {
                Err(S3Error::new(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "buckets are removed with the app or in the storage page"))
            }
            Method::PUT if ctx.has("acl") => Err(S3Error::new(StatusCode::BAD_REQUEST, "AccessControlListNotSupported", "The bucket does not allow ACLs")),
            _ => Err(S3Error::not_implemented("this bucket operation")),
        }
    }

    fn owner_acl(&self, ctx: &Ctx<'_>) -> Response<HttpBody> {
        let user = ctx.key.user_id.clone();
        xml_response(StatusCode::OK, document("AccessControlPolicy", |x| {
            x.open("Owner").leaf("ID", &user).leaf("DisplayName", &user).close("Owner");
            x.open("AccessControlList").open("Grant");
            x.raw("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">");
            x.leaf("ID", &user).leaf("DisplayName", &user).close("Grantee");
            x.leaf("Permission", "FULL_CONTROL").close("Grant").close("AccessControlList");
        }))
    }

    async fn list_objects(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, name: &str) -> S3Result<Response<HttpBody>> {
        let v2 = ctx.q("list-type") == Some("2");
        let max_keys: usize = match ctx.q("max-keys") {
            Some(v) => v.parse().map_err(|_| S3Error::invalid("max-keys"))?,
            None => 1000,
        };
        let prefix = ctx.q("prefix").unwrap_or("").to_string();
        let delimiter = ctx.q("delimiter").filter(|d| !d.is_empty()).map(String::from);
        let url_encode = ctx.q("encoding-type") == Some("url");
        let token = ctx.q("continuation-token").map(|t| String::from_utf8(hex::decode(t).unwrap_or_default()).unwrap_or_default());
        let start_after = if v2 { token.clone().or_else(|| ctx.q("start-after").map(String::from)) } else { ctx.q("marker").map(String::from) };
        let page = if max_keys == 0 {
            bucket_store::ListResult { items: Vec::new(), common_prefixes: Vec::new(), next_start_after: None }
        } else {
            self.store
                .list_objects(
                    &bucket.bucket_id,
                    &ctx.caller,
                    &ListRequest { prefix: prefix.clone(), delimiter: delimiter.clone(), start_after: start_after.clone(), limit: Some(max_keys.min(1000)) },
                )
                .await?
        };
        let enc = |s: &str| if url_encode { sigv4::uri_encode(s, false) } else { s.to_string() };
        let truncated = page.next_start_after.is_some();
        let owner = ctx.key.user_id.clone();
        let fetch_owner = !v2 || ctx.q("fetch-owner") == Some("true");
        let doc = document("ListBucketResult", |x| {
            x.leaf("Name", name).leaf("Prefix", &enc(&prefix));
            if let Some(d) = &delimiter {
                x.leaf("Delimiter", &enc(d));
            }
            x.leaf("MaxKeys", &max_keys.to_string());
            if url_encode {
                x.leaf("EncodingType", "url");
            }
            x.leaf("IsTruncated", if truncated { "true" } else { "false" });
            if v2 {
                x.leaf("KeyCount", &(page.items.len() + page.common_prefixes.len()).to_string());
                if let Some(t) = ctx.q("continuation-token") {
                    x.leaf("ContinuationToken", t);
                }
                if let Some(next) = &page.next_start_after {
                    x.leaf("NextContinuationToken", &hex::encode(next));
                }
                if let Some(s) = ctx.q("start-after") {
                    x.leaf("StartAfter", &enc(s));
                }
            } else {
                x.leaf("Marker", &enc(ctx.q("marker").unwrap_or("")));
                if let (Some(next), true) = (&page.next_start_after, delimiter.is_some()) {
                    x.leaf("NextMarker", &enc(next));
                }
            }
            for b in &page.items {
                x.open("Contents")
                    .leaf("Key", &enc(&b.key))
                    .leaf("LastModified", &iso8601(b.created_at))
                    .leaf("ETag", &quoted(&s3_etag(b)))
                    .leaf("Size", &b.size.to_string())
                    .leaf("StorageClass", "STANDARD");
                if fetch_owner {
                    x.open("Owner").leaf("ID", &owner).leaf("DisplayName", &owner).close("Owner");
                }
                x.close("Contents");
            }
            for p in &page.common_prefixes {
                x.open("CommonPrefixes").leaf("Prefix", &enc(p)).close("CommonPrefixes");
            }
        });
        Ok(xml_response(StatusCode::OK, doc))
    }

    async fn delete_objects(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, body: RequestBody) -> S3Result<Response<HttpBody>> {
        let text = String::from_utf8(BodyReader::new(ctx, body)?.read_all(MAX_SMALL_BODY).await?).map_err(|_| S3Error::malformed_xml())?;
        let quiet = xml::child(&text, "Quiet").as_deref() == Some("true");
        let objects = xml::elements(&text, "Object");
        if objects.is_empty() || objects.len() > MAX_DELETE_OBJECTS {
            return Err(S3Error::malformed_xml());
        }
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for o in objects {
            let Some(key) = xml::child(o, "Key") else { return Err(S3Error::malformed_xml()) };
            let version = xml::child(o, "VersionId").and_then(|v| v.parse::<u64>().ok());
            match self.store.delete(&bucket.bucket_id, &ctx.caller, &key, version, None).await {
                Ok(r) => deleted.push((key, r)),
                Err(e) => errors.push((key, S3Error::from(e))),
            }
        }
        let doc = document("DeleteResult", |x| {
            if !quiet {
                for (key, r) in &deleted {
                    x.open("Deleted").leaf("Key", key);
                    if r.delete_marker {
                        x.leaf("DeleteMarker", "true");
                        if let Some(v) = r.version {
                            x.leaf("DeleteMarkerVersionId", &v.to_string());
                        }
                    }
                    x.close("Deleted");
                }
            }
            for (key, e) in &errors {
                x.open("Error").leaf("Key", key).leaf("Code", e.code).leaf("Message", &e.message).close("Error");
            }
        });
        Ok(xml_response(StatusCode::OK, doc))
    }

    pub(crate) async fn object_op(&self, ctx: &Ctx<'_>, name: &str, key: &str, body: RequestBody) -> S3Result<Response<HttpBody>> {
        let bucket = self.resolve(ctx, name).await?;
        match ctx.method {
            Method::GET if ctx.has("uploadId") => self.list_parts(ctx, &bucket, name, key).await,
            Method::GET if ctx.has("tagging") => {
                let b = self.store.get(&bucket.bucket_id, &ctx.caller, key, None).await?;
                Ok(xml_response(StatusCode::OK, document("Tagging", |x| {
                    x.open("TagSet");
                    for (k, v) in &b.tags {
                        x.open("Tag").leaf("Key", k).leaf("Value", v).close("Tag");
                    }
                    x.close("TagSet");
                })))
            }
            Method::GET if ctx.has("acl") => {
                self.store.get(&bucket.bucket_id, &ctx.caller, key, None).await?;
                Ok(self.owner_acl(ctx))
            }
            Method::GET | Method::HEAD => self.get_object(ctx, &bucket, key).await,
            Method::PUT if ctx.has("partNumber") && ctx.has("uploadId") => {
                if ctx.header("x-amz-copy-source").is_some() {
                    return Err(S3Error::not_implemented("UploadPartCopy"));
                }
                self.upload_part(ctx, &bucket, key, body).await
            }
            Method::PUT if ctx.has("tagging") => {
                let text = String::from_utf8(BodyReader::new(ctx, body)?.read_all(MAX_SMALL_BODY).await?).map_err(|_| S3Error::malformed_xml())?;
                let mut tags = BTreeMap::new();
                for t in xml::elements(&text, "Tag") {
                    let (Some(k), Some(v)) = (xml::child(t, "Key"), xml::child(t, "Value")) else { return Err(S3Error::malformed_xml()) };
                    tags.insert(k, v);
                }
                self.store.set_metadata(&bucket.bucket_id, &ctx.caller, key, None, None, Some(tags)).await?;
                Ok(empty(StatusCode::OK))
            }
            Method::PUT if ctx.has("acl") => Err(S3Error::new(StatusCode::BAD_REQUEST, "AccessControlListNotSupported", "The bucket does not allow ACLs")),
            Method::PUT if ctx.header("x-amz-copy-source").is_some() => self.copy_object(ctx, &bucket, key).await,
            Method::PUT => self.put_object(ctx, &bucket, key, body).await,
            Method::POST if ctx.has("uploads") => self.create_upload(ctx, &bucket, name, key).await,
            Method::POST if ctx.has("uploadId") => self.complete_upload(ctx, &bucket, name, key, body).await,
            Method::DELETE if ctx.has("uploadId") => {
                let upload = self.upload_of(ctx, &bucket, key)?;
                self.drop_upload(&upload.upload_id).await;
                Ok(empty(StatusCode::NO_CONTENT))
            }
            Method::DELETE if ctx.has("tagging") => {
                self.store.set_metadata(&bucket.bucket_id, &ctx.caller, key, None, None, Some(BTreeMap::new())).await?;
                Ok(empty(StatusCode::NO_CONTENT))
            }
            Method::DELETE => {
                let version = ctx.q("versionId").map(|v| v.parse::<u64>().map_err(|_| S3Error::invalid("versionId"))).transpose()?;
                let r = self.store.delete(&bucket.bucket_id, &ctx.caller, key, version, ctx.header("if-match").map(|v| v.trim_matches('"').to_string())).await?;
                let mut resp = empty(StatusCode::NO_CONTENT);
                if r.delete_marker {
                    set(&mut resp, "x-amz-delete-marker", "true");
                }
                if let Some(v) = r.version.filter(|_| bucket.versioning) {
                    set(&mut resp, "x-amz-version-id", &v.to_string());
                }
                Ok(resp)
            }
            _ => Err(S3Error::not_implemented("this object operation")),
        }
    }

    async fn get_object(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str) -> S3Result<Response<HttpBody>> {
        let version = ctx.q("versionId").map(|v| v.parse::<u64>().map_err(|_| S3Error::invalid("versionId"))).transpose()?;
        let (bucket_now, b) = self.store.open_for_read(&bucket.bucket_id, &ctx.caller, key, version).await?;
        let etag = s3_etag(&b);
        let modified_secs = b.created_at / 1000;
        let since = |h: &str| ctx.header(h).and_then(|v| chrono::DateTime::parse_from_rfc2822(v).ok()).map(|t| t.timestamp());
        let matches = |v: &str| v.split(',').any(|t| {
            let t = t.trim().trim_start_matches("W/").trim_matches('"');
            t == "*" || t == etag
        });
        if let Some(v) = ctx.header("if-match") {
            if !matches(v) {
                return Err(S3Error::new(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold"));
            }
        } else if since("if-unmodified-since").is_some_and(|t| modified_secs > t) {
            return Err(S3Error::new(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold"));
        }
        let not_modified = match ctx.header("if-none-match") {
            Some(v) => matches(v),
            None => since("if-modified-since").is_some_and(|t| modified_secs <= t),
        };
        let mut headers = HeaderMap::new();
        if let Some(r) = ctx.headers.get(header::RANGE) {
            headers.insert(header::RANGE, r.clone());
        }
        let mut resp = if not_modified {
            empty(StatusCode::NOT_MODIFIED)
        } else {
            self.store.serve_read(&bucket_now, &b, &headers, ctx.method == Method::HEAD).await
        };
        if resp.status().is_client_error() || resp.status().is_server_error() {
            let status = resp.status();
            let body = resp.into_body().collect().await.map(|c| c.to_bytes()).unwrap_or_default();
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            return Err(match v["error"].as_str() {
                Some("content_evicted") | Some("origin_unavailable") | Some("no_such_key") => S3Error::new(StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist."),
                _ if status == StatusCode::RANGE_NOT_SATISFIABLE => S3Error::new(status, "InvalidRange", "The requested range is not satisfiable"),
                _ => S3Error::internal(String::from_utf8_lossy(&body).into_owned()),
            });
        }
        let h = resp.headers_mut();
        let renamed: Vec<(String, HeaderValue)> = h
            .iter()
            .filter_map(|(k, v)| k.as_str().strip_prefix("x-bucky-meta-").map(|m| (format!("x-amz-meta-{m}"), v.clone())))
            .collect();
        for (k, v) in renamed {
            if let Ok(n) = header::HeaderName::from_bytes(k.as_bytes()) {
                h.insert(n, v);
            }
        }
        set(&mut resp, "etag", &quoted(&etag));
        set(&mut resp, "last-modified", &http_date(b.created_at));
        if !b.tags.is_empty() {
            set(&mut resp, "x-amz-tagging-count", &b.tags.len().to_string());
        }
        if bucket_now.versioning {
            set(&mut resp, "x-amz-version-id", &b.version.to_string());
        }
        for (q, h) in [
            ("response-content-type", "content-type"),
            ("response-content-disposition", "content-disposition"),
            ("response-cache-control", "cache-control"),
            ("response-content-language", "content-language"),
            ("response-expires", "expires"),
            ("response-content-encoding", "content-encoding"),
        ] {
            if let Some(v) = ctx.q(q) {
                set(&mut resp, h, v);
            }
        }
        Ok(resp)
    }

    async fn require_write(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str) -> S3Result<()> {
        bucket_store::validate_key(key)?;
        for action in [Action::Write, Action::Create] {
            if self.store.check_access(&bucket.bucket_id, &ctx.caller, action, Some(key), None).await?.allowed {
                return Ok(());
            }
        }
        Err(BucketError::PermissionDenied(format!("write on {}", bucket.bucket_id)).into())
    }

    async fn put_object(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str, body: RequestBody) -> S3Result<Response<HttpBody>> {
        self.require_write(ctx, bucket, key).await?;
        let mut req = put_template(ctx, key)?;
        let writer = self.writer(format!("s3-hold:{}", ctx.key.access_key_id), MAX_PUT_BYTES);
        let written = BodyReader::new(ctx, body)?.write_into(writer).await?;
        check_content_md5(ctx, &written)?;
        req.etag_md5 = Some(hex::encode(written.md5));
        let r = self.store.put_chunks(&bucket.bucket_id, &ctx.caller, written.chunks, written.size, req.clone()).await?;
        let mut resp = empty(StatusCode::OK);
        set(&mut resp, "etag", &quoted(req.etag_md5.as_deref().unwrap_or_default()));
        if bucket.versioning {
            set(&mut resp, "x-amz-version-id", &r.version.to_string());
        }
        Ok(resp)
    }

    async fn copy_object(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str) -> S3Result<Response<HttpBody>> {
        let source = percent_decode(ctx.header("x-amz-copy-source").unwrap_or_default());
        let (path, version) = match source.split_once("?versionId=") {
            Some((p, v)) => (p.to_string(), Some(v.parse::<u64>().map_err(|_| S3Error::invalid("x-amz-copy-source versionId"))?)),
            None => (source.clone(), None),
        };
        let (src_name, src_key) = path.trim_start_matches('/').split_once('/').ok_or_else(|| S3Error::invalid("x-amz-copy-source is /bucket/key"))?;
        let src = self.resolve(ctx, src_name).await?;
        let replace = ctx.header("x-amz-metadata-directive").is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"));
        let template = put_template(ctx, key)?;
        let req = CopyRequest {
            src_bucket_id: src.bucket_id.to_string(),
            src_key: src_key.to_string(),
            src_version: version,
            dst_bucket_id: bucket.bucket_id.to_string(),
            dst_key: key.to_string(),
            metadata_directive: if replace { MetadataDirective::Replace } else { MetadataDirective::Copy },
            content_type: template.content_type,
            metadata: template.metadata,
            tags: template.tags,
            origin: None,
            conditions: template.conditions,
        };
        self.store.copy(&ctx.caller, req).await?;
        let b = self.store.get(&bucket.bucket_id, &ctx.caller, key, None).await?;
        let mut resp = xml_response(StatusCode::OK, document("CopyObjectResult", |x| {
            x.leaf("LastModified", &iso8601(b.created_at)).leaf("ETag", &quoted(&s3_etag(&b)));
        }));
        if bucket.versioning {
            set(&mut resp, "x-amz-version-id", &b.version.to_string());
        }
        Ok(resp)
    }

    // ---------- multipart ----------

    fn upload_of(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str) -> S3Result<bucket_store::MultipartUpload> {
        let id = ctx.q("uploadId").unwrap_or_default();
        let upload = self.store.multipart(id)?.ok_or_else(S3Error::no_such_upload)?;
        if upload.bucket_id != bucket.bucket_id || upload.key != key || upload.created_by != ctx.caller.principal() {
            return Err(S3Error::no_such_upload());
        }
        Ok(upload)
    }

    async fn create_upload(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, name: &str, key: &str) -> S3Result<Response<HttpBody>> {
        self.require_write(ctx, bucket, key).await?;
        let mut template = put_template(ctx, key)?;
        template.conditions = Conditions::default();
        let upload = self.store.create_multipart(&bucket.bucket_id, &template, &ctx.caller.principal())?;
        Ok(xml_response(StatusCode::OK, document("InitiateMultipartUploadResult", |x| {
            x.leaf("Bucket", name).leaf("Key", key).leaf("UploadId", &upload.upload_id);
        })))
    }

    async fn upload_part(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, key: &str, body: RequestBody) -> S3Result<Response<HttpBody>> {
        let upload = self.upload_of(ctx, bucket, key)?;
        let number: u32 = ctx.q("partNumber").and_then(|v| v.parse().ok()).filter(|n| (1..=MAX_PARTS).contains(n)).ok_or_else(|| S3Error::invalid("partNumber is 1..=10000"))?;
        let writer = self.writer(format!("s3-upload:{}", upload.upload_id), MAX_PUT_BYTES);
        let written = BodyReader::new(ctx, body)?.write_into(writer).await?;
        check_content_md5(ctx, &written)?;
        let md5 = hex::encode(written.md5);
        self.store.put_multipart_part(
            &upload.upload_id,
            &MultipartPart { part_number: number, chunk_ids: written.chunks.iter().map(|c| c.to_string()).collect(), size: written.size, md5: md5.clone() },
        )?;
        let mut resp = empty(StatusCode::OK);
        set(&mut resp, "etag", &quoted(&md5));
        Ok(resp)
    }

    async fn complete_upload(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, name: &str, key: &str, body: RequestBody) -> S3Result<Response<HttpBody>> {
        let upload = self.upload_of(ctx, bucket, key)?;
        let text = String::from_utf8(BodyReader::new(ctx, body)?.read_all(MAX_SMALL_BODY).await?).map_err(|_| S3Error::malformed_xml())?;
        let stored: BTreeMap<u32, MultipartPart> = self.store.multipart_parts(&upload.upload_id)?.into_iter().map(|p| (p.part_number, p)).collect();
        let requested = xml::elements(&text, "Part");
        if requested.is_empty() {
            return Err(S3Error::malformed_xml());
        }
        let invalid_part = || S3Error::new(StatusCode::BAD_REQUEST, "InvalidPart", "One or more of the specified parts could not be found or its entity tag did not match.");
        let mut parts = Vec::with_capacity(requested.len());
        let mut last = 0u32;
        for p in requested {
            let number: u32 = xml::child(p, "PartNumber").and_then(|v| v.parse().ok()).ok_or_else(S3Error::malformed_xml)?;
            if number <= last {
                return Err(S3Error::new(StatusCode::BAD_REQUEST, "InvalidPartOrder", "The list of parts was not in ascending order."));
            }
            last = number;
            let etag = xml::child(p, "ETag").unwrap_or_default();
            let part = stored.get(&number).ok_or_else(invalid_part)?;
            if etag.trim_matches('"') != part.md5 {
                return Err(invalid_part());
            }
            parts.push(part.clone());
        }
        let n = parts.len();
        if parts[..n - 1].iter().any(|p| p.size < MIN_PART_BYTES) {
            return Err(S3Error::new(StatusCode::BAD_REQUEST, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size."));
        }
        let mut md5s = Md5::new();
        let mut chunks = Vec::new();
        let mut size = 0u64;
        for p in &parts {
            md5s.update(hex::decode(&p.md5).unwrap_or_default());
            for c in &p.chunk_ids {
                chunks.push(ndn_lib::ChunkId::new(c).map_err(|e| S3Error::internal(e.to_string()))?);
            }
            size += p.size;
        }
        let etag = format!("{}-{n}", hex::encode(md5s.finalize()));
        let mut req = upload.template.clone();
        req.key = key.to_string();
        req.etag_md5 = Some(etag.clone());
        req.conditions = Conditions {
            if_none_match: ctx.header("if-none-match").map(String::from),
            if_match: ctx.header("if-match").map(|v| v.trim_matches('"').to_string()),
            if_version: None,
        };
        let r = self.store.put_chunks(&bucket.bucket_id, &ctx.caller, chunks, size, req).await?;
        // Parts stay leased until UPLOAD_HOLD_SECS: the binding's cascade reaches them asynchronously.
        self.store.delete_multipart(&upload.upload_id)?;
        let mut resp = xml_response(StatusCode::OK, document("CompleteMultipartUploadResult", |x| {
            x.leaf("Location", &format!("/{name}/{key}")).leaf("Bucket", name).leaf("Key", key).leaf("ETag", &quoted(&etag));
        }));
        if bucket.versioning {
            set(&mut resp, "x-amz-version-id", &r.version.to_string());
        }
        Ok(resp)
    }

    async fn list_parts(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, name: &str, key: &str) -> S3Result<Response<HttpBody>> {
        let upload = self.upload_of(ctx, bucket, key)?;
        let marker: u32 = ctx.q("part-number-marker").and_then(|v| v.parse().ok()).unwrap_or(0);
        let max: usize = ctx.q("max-parts").and_then(|v| v.parse().ok()).unwrap_or(1000).min(1000);
        let parts: Vec<MultipartPart> = self.store.multipart_parts(&upload.upload_id)?.into_iter().filter(|p| p.part_number > marker).collect();
        let truncated = parts.len() > max;
        let shown = &parts[..parts.len().min(max)];
        let user = ctx.key.user_id.clone();
        Ok(xml_response(StatusCode::OK, document("ListPartsResult", |x| {
            x.leaf("Bucket", name).leaf("Key", key).leaf("UploadId", &upload.upload_id);
            x.open("Initiator").leaf("ID", &user).leaf("DisplayName", &user).close("Initiator");
            x.open("Owner").leaf("ID", &user).leaf("DisplayName", &user).close("Owner");
            x.leaf("StorageClass", "STANDARD").leaf("PartNumberMarker", &marker.to_string());
            if let Some(last) = shown.last() {
                x.leaf("NextPartNumberMarker", &last.part_number.to_string());
            }
            x.leaf("MaxParts", &max.to_string()).leaf("IsTruncated", if truncated { "true" } else { "false" });
            for p in shown {
                x.open("Part")
                    .leaf("PartNumber", &p.part_number.to_string())
                    .leaf("LastModified", &iso8601(upload.created_at))
                    .leaf("ETag", &quoted(&p.md5))
                    .leaf("Size", &p.size.to_string())
                    .close("Part");
            }
        })))
    }

    async fn list_uploads(&self, ctx: &Ctx<'_>, bucket: &BucketInfo, name: &str) -> S3Result<Response<HttpBody>> {
        self.store.stat(&bucket.bucket_id, &ctx.caller).await?;
        let principal = ctx.caller.principal();
        let prefix = ctx.q("prefix").unwrap_or("").to_string();
        let uploads: Vec<_> = self
            .store
            .list_multipart(&bucket.bucket_id)?
            .into_iter()
            .filter(|u| u.created_by == principal && u.key.starts_with(&prefix))
            .collect();
        let user = ctx.key.user_id.clone();
        Ok(xml_response(StatusCode::OK, document("ListMultipartUploadsResult", |x| {
            x.leaf("Bucket", name).leaf("Prefix", &prefix).leaf("MaxUploads", "1000").leaf("IsTruncated", "false");
            for u in &uploads {
                x.open("Upload").leaf("Key", &u.key).leaf("UploadId", &u.upload_id);
                x.open("Initiator").leaf("ID", &user).leaf("DisplayName", &user).close("Initiator");
                x.open("Owner").leaf("ID", &user).leaf("DisplayName", &user).close("Owner");
                x.leaf("StorageClass", "STANDARD").leaf("Initiated", &iso8601(u.created_at)).close("Upload");
            }
        })))
    }
}
