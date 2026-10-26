use crate::sigv4::{self, sha256_hex, SigningContext, UNSIGNED_PAYLOAD};
use crate::*;
use base64::Engine;
use bucket_store::{BucketDecl, BucketId, IndexDb, S3KeyScope};
use md5::{Digest, Md5};
use named_store::{NamedDataMgr, NamedLocalStore, StoreLayout, StoreTarget};

struct OwnBuckets {
    user: String,
}

#[async_trait::async_trait]
impl IdentityPolicy for OwnBuckets {
    async fn allowed(&self, bucket: &BucketInfo, _key: &str, _action: Action) -> bool {
        bucket.owner == self.user
    }
    async fn holds(&self, _obj: &str) -> bool {
        false
    }
}

struct Host;

#[async_trait::async_trait]
impl S3Host for Host {
    async fn caller(&self, key: &S3Key) -> Caller {
        let mut c = Caller::anonymous(None);
        c.user = Some(key.user_id.clone());
        c.app = Some(key.appid.clone());
        c.zone_user = true;
        c.identity = Arc::new(OwnBuckets { user: key.user_id.clone() });
        c
    }
    async fn sync_buckets(&self, _appid: &str, _user: &str) {}
}

struct Env {
    _dir: tempfile::TempDir,
    gw: S3Gateway,
    key: S3Key,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let store = NamedLocalStore::get_named_store_by_path(dir.path().join("store")).await.unwrap();
    let store_id = store.store_id().to_string();
    let ndm = NamedDataMgr::new();
    ndm.register_store(Arc::new(tokio::sync::Mutex::new(store))).await;
    ndm.add_layout(StoreLayout::new(
        1,
        vec![StoreTarget { store_id, device_did: String::new(), capacity: None, used: None, readonly: false, enabled: true, weight: 1 }],
        0,
        0,
    ))
    .await;
    let buckets = BucketStore::new(IndexDb::open(&dir.path().join("index.db")).unwrap(), ndm);
    let decl = |v: serde_json::Value| serde_json::from_value::<BucketDecl>(v).unwrap();
    buckets.ensure_bucket(&BucketId::user("photos", "alice", "library").unwrap(), &decl(serde_json::json!({"kind": "data", "data_policy": "documents"}))).unwrap();
    buckets.ensure_bucket(&BucketId::user("photos", "alice", "index").unwrap(), &decl(serde_json::json!({"kind": "data", "data_policy": "app_data"}))).unwrap();
    buckets.ensure_bucket(&BucketId::user("photos", "alice", "notes").unwrap(), &decl(serde_json::json!({"kind": "object", "data_policy": "app_data"}))).unwrap();
    buckets.ensure_bucket(&BucketId::user("photos", "bob", "library").unwrap(), &decl(serde_json::json!({"kind": "data", "data_policy": "documents"}))).unwrap();
    let key = buckets.create_s3_key("alice", "photos", None, None).unwrap();
    Env { _dir: dir, gw: S3Gateway::new(buckets), key }
}

fn amz_now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

struct Req<'a> {
    method: &'a str,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    payload_hash: String,
}

impl<'a> Req<'a> {
    fn new(method: &'a str, path: &str) -> Self {
        Req { method, path: path.into(), query: String::new(), headers: Vec::new(), body: Vec::new(), payload_hash: UNSIGNED_PAYLOAD.into() }
    }
    fn query(mut self, q: &str) -> Self {
        self.query = q.into();
        self
    }
    fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.into(), v.into()));
        self
    }
    fn body(mut self, b: &[u8]) -> Self {
        self.body = b.to_vec();
        self
    }
    fn hash(mut self, h: &str) -> Self {
        self.payload_hash = h.into();
        self
    }

    fn signed(self, key: &S3Key) -> S3Request {
        self.signed_as(key, &key.secret)
    }

    fn signed_as(self, key: &S3Key, secret: &str) -> S3Request {
        let date = amz_now();
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.test.zone".parse().unwrap());
        headers.insert("x-amz-date", date.parse().unwrap());
        headers.insert("x-amz-content-sha256", self.payload_hash.parse().unwrap());
        for (k, v) in &self.headers {
            headers.insert(http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
        }
        let signed: Vec<String> = ["host", "x-amz-content-sha256", "x-amz-date"].iter().map(|s| s.to_string()).collect();
        let query = sigv4::parse_query(&self.query);
        let sig = sigv4::sign(secret, &key.access_key_id, "us-east-1", &date, self.method, &self.path, &query, &headers, &signed, &self.payload_hash);
        headers.insert(
            http::header::AUTHORIZATION,
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={sig}",
                key.access_key_id,
                &date[..8],
                signed.join(";")
            )
            .parse()
            .unwrap(),
        );
        build(self.method, &self.path, &self.query, headers, self.body)
    }
}

fn build(method: &str, path: &str, query: &str, headers: HeaderMap, body: Vec<u8>) -> S3Request {
    let uri = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
    let mut req = http::Request::builder().method(method).uri(uri).body(Full::new(Bytes::from(body)).map_err(|n| match n {}).boxed_unsync()).unwrap();
    *req.headers_mut() = headers;
    req
}

async fn send(env: &Env, req: S3Request) -> (StatusCode, HeaderMap, Vec<u8>) {
    let resp = env.gw.handle(&Host, req, "/s3").await;
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.map(|c| c.to_bytes().to_vec()).unwrap_or_default();
    (status, headers, body)
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn md5_hex(b: &[u8]) -> String {
    hex::encode(Md5::digest(b))
}

#[tokio::test]
async fn objects_round_trip_with_s3_semantics() {
    let e = env().await;
    let (s, _, body) = send(&e, Req::new("GET", "/s3/").signed(&e.key)).await;
    assert_eq!(s, 200);
    let listing = text(&body);
    assert!(listing.contains("<Name>photos-library</Name>") && listing.contains("<Name>photos-index</Name>"), "{listing}");
    assert!(!listing.contains("photos-notes"), "object buckets are not S3 buckets");

    let data = b"hello s3 world";
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(data));
    let (s, h, body) = send(
        &e,
        Req::new("PUT", "/s3/photos-library/2026/a%20b.txt")
            .header("content-type", "text/plain")
            .header("content-md5", &md5_b64)
            .header("x-amz-meta-album", "trip")
            .header("cache-control", "max-age=60")
            .body(data)
            .signed(&e.key),
    )
    .await;
    assert_eq!(s, 200, "{}", text(&body));
    assert_eq!(h["etag"], format!("\"{}\"", md5_hex(data)));
    assert!(h.contains_key("x-amz-version-id"), "documents buckets are versioned");

    let (s, h, body) = send(&e, Req::new("GET", "/s3/photos-library/2026/a%20b.txt").signed(&e.key)).await;
    assert_eq!(s, 200);
    assert_eq!(body, data);
    assert_eq!(h["etag"], format!("\"{}\"", md5_hex(data)));
    assert_eq!(h["x-amz-meta-album"], "trip");
    assert_eq!(h["cache-control"], "max-age=60");
    assert_eq!(h["content-type"], "text/plain");
    let (s, _, body) = send(&e, Req::new("GET", "/s3/photos-library/2026/a%20b.txt").header("range", "bytes=6-7").signed(&e.key)).await;
    assert_eq!((s, body.as_slice()), (StatusCode::PARTIAL_CONTENT, &b"s3"[..]));
    let etag = format!("\"{}\"", md5_hex(data));
    let (s, _, _) = send(&e, Req::new("GET", "/s3/photos-library/2026/a%20b.txt").header("if-none-match", &etag).signed(&e.key)).await;
    assert_eq!(s, 304);
    let (s, h, body) = send(&e, Req::new("HEAD", "/s3/photos-library/2026/a%20b.txt").signed(&e.key)).await;
    assert_eq!((s, h["content-length"].to_str().unwrap(), body.len()), (StatusCode::OK, "14", 0));

    // A wrong Content-MD5 or payload hash is refused before anything is bound.
    let (s, _, body) = send(&e, Req::new("PUT", "/s3/photos-library/bad").header("content-md5", &md5_b64).body(b"other").signed(&e.key)).await;
    assert_eq!(s, 400);
    assert!(text(&body).contains("BadDigest"));
    let (s, _, body) = send(&e, Req::new("PUT", "/s3/photos-library/bad").body(b"other").hash(&sha256_hex(b"different")).signed(&e.key)).await;
    assert!(s == 400 && text(&body).contains("XAmzContentSHA256Mismatch"), "{}", text(&body));
    let (s, _, _) = send(&e, Req::new("PUT", "/s3/photos-library/good").body(b"other").hash(&sha256_hex(b"other")).signed(&e.key)).await;
    assert_eq!(s, 200);

    // Conditional create.
    let (s, _, _) = send(&e, Req::new("PUT", "/s3/photos-library/good").header("if-none-match", "*").body(b"x").signed(&e.key)).await;
    assert_eq!(s, 412);

    // List v2 with a delimiter and continuation.
    for k in ["2026/b.txt", "2026/c.txt", "2027/d.txt"] {
        send(&e, Req::new("PUT", &format!("/s3/photos-index/{k}")).body(k.as_bytes()).signed(&e.key)).await;
    }
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index").query("list-type=2&delimiter=%2F").signed(&e.key)).await;
    let b = text(&body);
    assert!(b.contains("<Prefix>2026/</Prefix>") && b.contains("<Prefix>2027/</Prefix>") && b.contains("<KeyCount>2</KeyCount>"), "{b}");
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index").query("list-type=2&prefix=2026%2F&max-keys=1").signed(&e.key)).await;
    let b = text(&body);
    assert!(b.contains("<Key>2026/b.txt</Key>") && b.contains("<IsTruncated>true</IsTruncated>"), "{b}");
    let token = xml::child(&b, "NextContinuationToken").unwrap();
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index").query(&format!("list-type=2&prefix=2026%2F&continuation-token={token}")).signed(&e.key)).await;
    let b = text(&body);
    assert!(b.contains("<Key>2026/c.txt</Key>") && b.contains("<IsTruncated>false</IsTruncated>"), "{b}");
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index").query("prefix=2026%2F").signed(&e.key)).await;
    assert!(text(&body).contains("<Marker></Marker>"), "list v1");

    // Copy keeps the content and its ETag; tagging; delete objects.
    let (s, _, body) = send(&e, Req::new("PUT", "/s3/photos-index/copy.txt").header("x-amz-copy-source", "/photos-library/2026/a%20b.txt").signed(&e.key)).await;
    assert_eq!(s, 200, "{}", text(&body));
    assert!(text(&body).contains(&md5_hex(data)));
    let tagging = r#"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"#;
    let (s, _, _) = send(&e, Req::new("PUT", "/s3/photos-index/copy.txt").query("tagging").body(tagging.as_bytes()).signed(&e.key)).await;
    assert_eq!(s, 200);
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index/copy.txt").query("tagging").signed(&e.key)).await;
    assert!(text(&body).contains("<Key>k</Key><Value>v</Value>"));
    let delete = r#"<Delete><Object><Key>2026/b.txt</Key></Object><Object><Key>missing</Key></Object></Delete>"#;
    let (s, _, body) = send(&e, Req::new("POST", "/s3/photos-index").query("delete").body(delete.as_bytes()).signed(&e.key)).await;
    assert_eq!(s, 200);
    assert!(text(&body).contains("<Deleted><Key>2026/b.txt</Key></Deleted>"), "{}", text(&body));
    let (s, _, body) = send(&e, Req::new("GET", "/s3/photos-index/2026/b.txt").signed(&e.key)).await;
    assert!(s == 404 && text(&body).contains("NoSuchKey"));
    let (s, _, _) = send(&e, Req::new("DELETE", "/s3/photos-index/2026/c.txt").signed(&e.key)).await;
    assert_eq!(s, 204);

    // Buckets come from declarations, not CreateBucket; others' buckets do not exist here.
    let (s, _, body) = send(&e, Req::new("PUT", "/s3/new-bucket").signed(&e.key)).await;
    assert!(s == 405 && text(&body).contains("MethodNotAllowed"));
    let (s, h, _) = send(&e, Req::new("HEAD", "/s3/photos-library").signed(&e.key)).await;
    assert_eq!(s, 200);
    assert!(h.contains_key("x-bucky-usage-bytes"));
}

#[tokio::test]
async fn authentication_failures_and_presigned_urls() {
    let e = env().await;
    let (s, _, body) = send(&e, Req::new("GET", "/s3/").signed_as(&e.key, "wrong")).await;
    assert!(s == 403 && text(&body).contains("SignatureDoesNotMatch"));
    let mut ghost = e.key.clone();
    ghost.access_key_id = "BKNOSUCHKEY0000000000".into();
    let (s, _, body) = send(&e, Req::new("GET", "/s3/").signed(&ghost)).await;
    assert!(s == 403 && text(&body).contains("InvalidAccessKeyId"));
    let (s, _, body) = send(&e, build("GET", "/s3/", "", HeaderMap::new(), Vec::new())).await;
    assert!(s == 403 && text(&body).contains("AccessDenied"));

    send(&e, Req::new("PUT", "/s3/photos-library/p.txt").body(b"presigned").signed(&e.key)).await;
    let presign = |expires: i64, date: &str| {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.test.zone".parse().unwrap());
        let mut q = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}%2F{}%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date={date}&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host",
            e.key.access_key_id,
            &date[..8]
        );
        let sig = sigv4::sign(&e.key.secret, &e.key.access_key_id, "us-east-1", date, "GET", "/s3/photos-library/p.txt", &sigv4::parse_query(&q), &headers, &["host".to_string()], UNSIGNED_PAYLOAD);
        q.push_str(&format!("&X-Amz-Signature={sig}"));
        build("GET", "/s3/photos-library/p.txt", &q, headers, Vec::new())
    };
    let (s, _, body) = send(&e, presign(300, &amz_now())).await;
    assert_eq!((s, body.as_slice()), (StatusCode::OK, &b"presigned"[..]));
    let old = (chrono::Utc::now() - chrono::Duration::hours(2)).format("%Y%m%dT%H%M%SZ").to_string();
    let (s, _, body) = send(&e, presign(60, &old)).await;
    assert!(s == 403 && text(&body).contains("expired"));

    // A revoked key stops working; a scoped key only reaches its prefix.
    let scoped = e.gw.store.create_s3_key("alice", "photos", Some(S3KeyScope { buckets: vec![], prefix: Some("pub/".into()), actions: None }), None).unwrap();
    let (s, _, _) = send(&e, Req::new("PUT", "/s3/photos-library/pub/x").body(b"x").signed(&scoped)).await;
    assert_eq!(s, 200);
    let (s, _, _) = send(&e, Req::new("GET", "/s3/photos-library/p.txt").signed(&scoped)).await;
    assert_eq!(s, 403);
    e.gw.store.revoke_s3_key("alice", &scoped.access_key_id).unwrap();
    let (s, _, _) = send(&e, Req::new("GET", "/s3/photos-library/pub/x").signed(&scoped)).await;
    assert_eq!(s, 403);
}

#[tokio::test]
async fn aws_chunked_signed_upload() {
    let e = env().await;
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    // Sign the request the way SDKs do for streaming uploads, then frame the body with chunk signatures.
    let date = amz_now();
    let mut headers = HeaderMap::new();
    headers.insert("host", "s3.test.zone".parse().unwrap());
    headers.insert("x-amz-date", date.parse().unwrap());
    headers.insert("x-amz-content-sha256", sigv4::STREAMING_SIGNED.parse().unwrap());
    headers.insert("content-encoding", "aws-chunked".parse().unwrap());
    headers.insert("x-amz-decoded-content-length", data.len().to_string().parse().unwrap());
    let signed: Vec<String> = ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"].iter().map(|s| s.to_string()).collect();
    let path = "/s3/photos-library/stream.bin";
    let seed = sigv4::sign(&e.key.secret, &e.key.access_key_id, "us-east-1", &date, "PUT", path, &[], &headers, &signed, sigv4::STREAMING_SIGNED);
    headers.insert(
        http::header::AUTHORIZATION,
        format!("AWS4-HMAC-SHA256 Credential={}/{}/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={seed}", e.key.access_key_id, &date[..8], signed.join(";"))
            .parse()
            .unwrap(),
    );
    let ctx = SigningContext {
        key: sigv4::signing_key(&e.key.secret, &date[..8], "us-east-1", "s3"),
        amz_date: date.clone(),
        scope: format!("{}/us-east-1/s3/aws4_request", &date[..8]),
        seed_signature: seed.clone(),
    };
    let mut body = Vec::new();
    let mut prev = seed;
    for piece in data.chunks(65536).chain(std::iter::once(&[][..])) {
        let sig = ctx.chunk_signature(&prev, &sha256_hex(piece));
        body.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", piece.len()).as_bytes());
        body.extend_from_slice(piece);
        body.extend_from_slice(b"\r\n");
        prev = sig;
    }
    let (s, h, resp) = send(&e, build("PUT", path, "", headers, body)).await;
    assert_eq!(s, 200, "{}", text(&resp));
    assert_eq!(h["etag"], format!("\"{}\"", md5_hex(&data)));
    let (_, h, got) = send(&e, Req::new("GET", path).signed(&e.key)).await;
    assert_eq!(got, data);
    assert!(!h.contains_key("content-encoding"), "aws-chunked is a transfer encoding, not stored");
}

#[tokio::test]
async fn multipart_upload_complete_and_abort() {
    let e = env().await;
    let (s, _, body) = send(&e, Req::new("POST", "/s3/photos-index/big.bin").query("uploads").header("content-type", "application/x-test").signed(&e.key)).await;
    assert_eq!(s, 200);
    let upload = xml::child(&text(&body), "UploadId").unwrap();
    let part1: Vec<u8> = (0..MIN_PART_BYTES as u32).map(|i| (i % 7) as u8).collect();
    let part2 = b"tail of the object".to_vec();
    let mut etags = Vec::new();
    for (n, part) in [(1, &part1), (2, &part2)] {
        let (s, h, body) = send(&e, Req::new("PUT", "/s3/photos-index/big.bin").query(&format!("partNumber={n}&uploadId={upload}")).body(part).signed(&e.key)).await;
        assert_eq!(s, 200, "{}", text(&body));
        etags.push(h["etag"].to_str().unwrap().to_string());
    }
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index/big.bin").query(&format!("uploadId={upload}")).signed(&e.key)).await;
    assert_eq!(xml::elements(&text(&body), "Part").len(), 2);
    let (_, _, body) = send(&e, Req::new("GET", "/s3/photos-index").query("uploads").signed(&e.key)).await;
    assert!(text(&body).contains(&upload));

    let bad = format!("<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etags[1], etags[0]);
    let (s, _, body) = send(&e, Req::new("POST", "/s3/photos-index/big.bin").query(&format!("uploadId={upload}")).body(bad.as_bytes()).signed(&e.key)).await;
    assert!(s == 400 && text(&body).contains("InvalidPartOrder"));
    let complete = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etags[0], etags[1]);
    let (s, _, body) = send(&e, Req::new("POST", "/s3/photos-index/big.bin").query(&format!("uploadId={upload}")).body(complete.as_bytes()).signed(&e.key)).await;
    assert_eq!(s, 200, "{}", text(&body));
    let mut combined = Md5::new();
    combined.update(Md5::digest(&part1));
    combined.update(Md5::digest(&part2));
    let want = format!("\"{}-2\"", hex::encode(combined.finalize()));
    assert!(text(&body).contains(&want.replace('"', "&quot;")), "{}", text(&body));
    let (s, h, got) = send(&e, Req::new("GET", "/s3/photos-index/big.bin").signed(&e.key)).await;
    assert_eq!(s, 200);
    assert_eq!(h["etag"], want);
    assert_eq!(h["content-type"], "application/x-test");
    assert_eq!(got.len(), part1.len() + part2.len());
    assert_eq!(&got[..part1.len()], part1.as_slice());
    assert_eq!(&got[part1.len()..], part2.as_slice());
    let (_, _, got) = send(&e, Req::new("GET", "/s3/photos-index/big.bin").header("range", &format!("bytes={}-", part1.len() - 2)).signed(&e.key)).await;
    assert_eq!(&got[2..], part2.as_slice(), "ranges cross part boundaries");
    let (s, _, _) = send(&e, Req::new("POST", "/s3/photos-index/big.bin").query(&format!("uploadId={upload}")).body(complete.as_bytes()).signed(&e.key)).await;
    assert_eq!(s, 404, "a completed upload is gone");

    let (_, _, body) = send(&e, Req::new("POST", "/s3/photos-index/gone.bin").query("uploads").signed(&e.key)).await;
    let other = xml::child(&text(&body), "UploadId").unwrap();
    send(&e, Req::new("PUT", "/s3/photos-index/gone.bin").query(&format!("partNumber=1&uploadId={other}")).body(b"x").signed(&e.key)).await;
    let (s, _, _) = send(&e, Req::new("DELETE", "/s3/photos-index/gone.bin").query(&format!("uploadId={other}")).signed(&e.key)).await;
    assert_eq!(s, 204);
    assert!(e.gw.store.multipart(&other).unwrap().is_none());
    assert!(e.gw.store.ndm().list_pins_by_owner(&format!("s3-upload:{other}"), None, 10).await.unwrap().is_empty());
}
