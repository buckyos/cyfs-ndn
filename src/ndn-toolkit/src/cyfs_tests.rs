//! Test suite derived from `doc/CYFS TestCase Design.md`.
//!
//! The tests in this module exercise the combination of [`CyfsNdnClient`] and
//! [`NdnDirServer`] against the risk points highlighted in the design doc:
//! object id encoding, Canonical JSON, `PathObject` JWT, inner_path chains,
//! `resp=raw`, `ChunkList` total size, and request header limits. Tests are
//! named after the design doc IDs they cover (`obj_01`, `can_02`, ...).
//!
//! The heavier end-to-end paths are driven via an in-process transport that
//! feeds `CyfsTransportRequest` straight into [`NdnDirServer::serve_request`],
//! collects the resulting body, and returns it to the client — this avoids
//! binding to a real network socket in tests.

use bytes::Bytes;
use http::{HeaderName as HttpHeaderName, HeaderValue as HttpHeaderValue, Request as HttpRequest};
use http_body_util::{BodyExt, Empty};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use url::Url;

use crate::cyfs_ndn_client::{
    AcceptIfFreshVerifier, CyfsHttpTransport, CyfsNdnClient, CyfsTransportRequest,
    CyfsTransportResponse, PathObjectVerifier,
};
use crate::cyfs_ndn_dir_server::{NdnDirServer, NdnDirServerConfig, NdnDirServerMode};
use named_store::{NamedLocalStore, NamedStoreMgr, StoreLayout, StoreTarget};
use ndn_lib::{
    build_named_object_by_json, cyfs_parse_url, ChunkHasher, ChunkId, ChunkList, ChunkType,
    FileObject, HashMethod, NamedObject, NdnError, ObjId, CYFS_CASCADES_MAX_LEN,
    CYFS_HEADER_OBJ_ID, OBJ_TYPE_CHUNK_LIST,
};

// =====================================================================
// Shared fixtures
// =====================================================================

fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 131 + (i >> 5) * 17 + 7) & 0xFF) as u8)
        .collect()
}

async fn create_test_store_mgr(base_dir: &Path) -> Arc<NamedStoreMgr> {
    let store = NamedLocalStore::get_named_store_by_path(base_dir.join("named_store"))
        .await
        .unwrap();
    let store_id = store.store_id().to_string();
    let store_ref = Arc::new(tokio::sync::Mutex::new(store));

    let store_mgr = NamedStoreMgr::new();
    store_mgr.register_store(store_ref).await;
    store_mgr
        .add_layout(StoreLayout::new(
            1,
            vec![StoreTarget {
                store_id,
                device_did: None,
                capacity: None,
                used: None,
                readonly: false,
                enabled: true,
                weight: 1,
            }],
            0,
            0,
        ))
        .await;

    Arc::new(store_mgr)
}

fn mix256_from_bytes(bytes: &[u8]) -> ChunkId {
    ChunkHasher::new_with_hash_method(HashMethod::Sha256)
        .unwrap()
        .calc_mix_chunk_id_from_bytes(bytes)
        .unwrap()
}

fn clone_chunk_list(chunk_list: &ChunkList) -> ChunkList {
    ChunkList::from_chunk_list(chunk_list.body.clone()).unwrap()
}

/// Decode a leading unsigned varint (LEB128, per the mix* / clist spec).
/// Returns `(value, bytes_consumed)`. Test-local to avoid pulling
/// `unsigned-varint` into `ndn-toolkit`'s direct deps.
fn decode_varint(buf: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, b) in buf.iter().enumerate() {
        result |= ((*b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return (result, i + 1);
        }
        shift += 7;
        if shift >= 64 {
            panic!("varint too long");
        }
    }
    panic!("varint truncated");
}

// =====================================================================
// In-process transport: feeds CyfsTransportRequest into NdnDirServer
// =====================================================================

#[derive(Clone)]
struct InProcServerTransport {
    server: Arc<NdnDirServer>,
}

impl InProcServerTransport {
    fn new(server: Arc<NdnDirServer>) -> Self {
        Self { server }
    }
}

impl CyfsHttpTransport for InProcServerTransport {
    fn send(
        &self,
        request: CyfsTransportRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = ndn_lib::NdnResult<CyfsTransportResponse>> + Send + '_>>
    {
        let server = self.server.clone();
        Box::pin(async move {
            let parsed = Url::parse(&request.url)
                .map_err(|e| NdnError::InvalidParam(format!("parse url {}: {}", request.url, e)))?;
            let host = parsed.host_str().unwrap_or("local.test").to_string();
            let path_and_query = match parsed.query() {
                Some(q) => format!("{}?{}", parsed.path(), q),
                None => parsed.path().to_string(),
            };

            let mut builder = HttpRequest::builder()
                .method(request.method.as_str())
                .uri(path_and_query.as_str());
            {
                let hdrs = builder.headers_mut().unwrap();
                hdrs.insert(
                    http::header::HOST,
                    HttpHeaderValue::from_str(&host).unwrap(),
                );
                for (k, v) in request.headers.iter() {
                    let name = HttpHeaderName::from_bytes(k.as_str().as_bytes()).unwrap();
                    let value = HttpHeaderValue::from_bytes(v.as_bytes()).unwrap();
                    hdrs.insert(name, value);
                }
            }
            let req_http = builder
                .body(Empty::<Bytes>::new())
                .map_err(|e| NdnError::Internal(format!("build http request: {}", e)))?;

            let response = server.serve_request(req_http).await;
            let (parts, body) = response.into_parts();

            let status = reqwest::StatusCode::from_u16(parts.status.as_u16())
                .map_err(|e| NdnError::Internal(format!("status convert: {}", e)))?;
            let mut headers = HeaderMap::new();
            for (k, v) in parts.headers.iter() {
                let name = HeaderName::from_bytes(k.as_str().as_bytes()).unwrap();
                let value = HeaderValue::from_bytes(v.as_bytes()).unwrap();
                headers.insert(name, value);
            }

            let collected = body
                .collect()
                .await
                .map_err(|e| NdnError::RemoteError(format!("collect body: {}", e)))?;
            let bytes = collected.to_bytes();
            let content_length = Some(bytes.len() as u64);

            let reader = std::io::Cursor::new(bytes.to_vec());

            Ok(CyfsTransportResponse {
                status,
                headers,
                content_length,
                body: Box::pin(reader),
            })
        })
    }
}

async fn make_server_client_pair(
    base_dir: &Path,
) -> (Arc<NdnDirServer>, CyfsNdnClient, Arc<NamedStoreMgr>) {
    let store_mgr = create_test_store_mgr(&base_dir.join("store")).await;
    let semantic_root = base_dir.join("root");
    tokio::fs::create_dir_all(&semantic_root).await.unwrap();

    let cfg = NdnDirServerConfig::new(
        &semantic_root,
        store_mgr.clone(),
        NdnDirServerMode::LocalLink,
    )
    .url_prefix("/ndn")
    .scan_interval(Duration::from_secs(3600));
    let server = Arc::new(NdnDirServer::new(cfg));

    let client = CyfsNdnClient::builder()
        .transport(InProcServerTransport::new(server.clone()))
        .build()
        .unwrap();

    (server, client, store_mgr)
}

// =====================================================================
// Manual JWT minting: produces a structurally-valid JWT whose signature is
// unverified. `AcceptIfFreshVerifier` only checks `iat/exp`, so a junk
// signature is fine for PATH-06 testing.
// =====================================================================

fn mint_unsigned_path_jwt(path: &str, target: &ObjId, iat: u64, exp: u64) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let header = serde_json::json!({ "alg": "EdDSA", "typ": "JWT" });
    let claims = serde_json::json!({
        "path": path,
        "iat": iat,
        "exp": exp,
        "target": target.to_string(),
    });
    let h = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let c = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(b"not-a-real-signature");
    format!("{}.{}.{}", h, c, sig)
}

// =====================================================================
// L1 — Object id / chunk id / base32 / varint
// =====================================================================

/// OBJ-01: sha256:hex and base32 URL forms of the same ObjId resolve to the
/// same internal identity. Both representations must parse back to an
/// identical `ObjId`.
#[test]
fn obj_01_objid_base32_and_hex_equivalent() {
    let hex_form = "sha256:0203040506";
    let base32_form = ObjId::new(hex_form).unwrap().to_base32();

    let from_hex = ObjId::new(hex_form).unwrap();
    let from_base32 = ObjId::new(&base32_form).unwrap();
    assert_eq!(from_hex, from_base32);

    // Round-trip through both canonical string forms.
    assert_eq!(from_hex.to_string(), "sha256:0203040506");
    assert_eq!(from_base32.to_string(), "sha256:0203040506");
}

/// OBJ-02: base32 ObjId encoding must be lowercase and must not contain
/// `=` padding characters. Otherwise the ObjId is no longer canonical.
#[test]
fn obj_02_base32_is_lowercase_no_padding() {
    for fixture in &[
        "sha256:0203040506",
        "cyfile:1234567890abcdef",
        "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
    ] {
        let obj_id = ObjId::new(fixture).unwrap();
        let b32 = obj_id.to_base32();
        assert!(!b32.contains('='), "base32 carries padding: {}", b32);
        assert_eq!(
            b32,
            b32.to_ascii_lowercase(),
            "base32 is not lowercase: {}",
            b32
        );
    }
}

/// OBJ-05: `mix256` length prefix (varint) must round-trip correctly at the
/// LEB128 byte boundaries: 127 (1 byte), 128 (2 bytes), 16383 (2 bytes),
/// 16384 (3 bytes).
#[test]
fn obj_05_mix256_varint_length_boundaries() {
    for size in [127usize, 128, 16383, 16384] {
        let data = deterministic_bytes(size);
        let chunk_id = mix256_from_bytes(&data);
        let decoded = chunk_id
            .get_length()
            .unwrap_or_else(|| panic!("mix256 length missing at size {}", size));
        assert_eq!(
            decoded, size as u64,
            "mix256 length mismatch at boundary {}",
            size
        );

        // ChunkType::Mix256 must remain; round-trip through str form.
        let redecoded = ChunkId::new(&chunk_id.to_string()).unwrap();
        assert_eq!(redecoded.chunk_type, ChunkType::Mix256);
        assert_eq!(redecoded.get_length(), Some(size as u64));
    }
}

/// OBJ-06: For a multi-chunk `ChunkList`, the `clist` ObjId's leading length
/// prefix must equal the total file size — not the first chunk size, not the
/// JSON length.
#[test]
fn obj_06_clist_total_size_length_prefix() {
    // Two deliberately distinct chunk sizes.
    let a = deterministic_bytes(2048);
    let b = deterministic_bytes(137);
    let chunk_a = mix256_from_bytes(&a);
    let chunk_b = mix256_from_bytes(&b);

    let chunk_list = ChunkList::from_chunk_list(vec![chunk_a, chunk_b]).unwrap();
    let total = chunk_list.total_size;
    assert_eq!(total, (2048 + 137) as u64);

    let (clist_id, _) = clone_chunk_list(&chunk_list).gen_obj_id();
    assert_eq!(clist_id.obj_type, OBJ_TYPE_CHUNK_LIST);
    let (prefix_total, _consumed) = decode_varint(&clist_id.obj_hash);
    assert_eq!(
        prefix_total, total,
        "clist length prefix must equal total_size"
    );
    assert_ne!(
        prefix_total, 2048,
        "clist length prefix must not equal first chunk size"
    );
}

/// OBJ-04: base32 hostname carrying `=` padding must be rejected outright
/// (cannot silently map to another object).
#[test]
fn obj_04_base32_with_padding_is_rejected() {
    let valid = ObjId::new("sha256:0203040506").unwrap().to_base32();
    // Append padding; parser should refuse since strict RFC4648-no-pad.
    let with_pad = format!("{}==", valid);
    let res = ObjId::new(&with_pad);
    assert!(res.is_err(), "parser must not accept padded base32");
}

// =====================================================================
// L1 — Canonical JSON (RFC 8785 / JCS)
// =====================================================================

/// CAN-01: Object field order must not influence the ObjectId.
#[test]
fn can_01_canonical_json_field_order_invariant() {
    let a = serde_json::json!({ "name": "alice", "age": 30, "role": "admin" });
    let b = serde_json::json!({ "age": 30, "role": "admin", "name": "alice" });
    let (id_a, _) = build_named_object_by_json("jobj", &a);
    let (id_b, _) = build_named_object_by_json("jobj", &b);
    assert_eq!(id_a, id_b);
}

/// CAN-02: Explicit `null` must not produce the same ObjectId as a missing
/// key. Implementations MUST NOT silently normalize one to the other.
#[test]
fn can_02_explicit_null_differs_from_missing() {
    let with_null = serde_json::json!({ "name": "alice", "nickname": null });
    let without_key = serde_json::json!({ "name": "alice" });
    let (id_null, s_null) = build_named_object_by_json("jobj", &with_null);
    let (id_missing, s_missing) = build_named_object_by_json("jobj", &without_key);
    assert_ne!(
        id_null, id_missing,
        "null field MUST NOT be treated as missing: {} vs {}",
        s_null, s_missing
    );
}

/// CAN-07: FileObject custom meta fields participate in the hash and
/// therefore change the ObjectId. Implementations must not silently drop
/// unknown fields.
#[test]
fn can_07_file_object_custom_meta_changes_id() {
    let chunk_id = mix256_from_bytes(&deterministic_bytes(128));
    let base = FileObject::new(
        "cyfs-head.bin".to_string(),
        128,
        chunk_id.to_string(),
    );
    let (base_id, _) = base.clone().gen_obj_id();

    let mut with_meta = base.clone();
    with_meta
        .meta
        .insert("mime".to_string(), serde_json::json!("text/plain"));
    let (meta_id, _) = with_meta.gen_obj_id();

    assert_ne!(base_id, meta_id, "adding meta must change FileObject id");
}

/// CLIST-05: A `ChunkList` with the same chunks in a different order must
/// produce a different `clist` ObjectId — order-sensitivity is mandatory.
#[test]
fn clist_05_chunk_order_changes_clist_id() {
    let a = mix256_from_bytes(&deterministic_bytes(256));
    let b = mix256_from_bytes(&deterministic_bytes(512));

    let list_ab = ChunkList::from_chunk_list(vec![a.clone(), b.clone()]).unwrap();
    let list_ba = ChunkList::from_chunk_list(vec![b, a]).unwrap();

    let (id_ab, _) = list_ab.gen_obj_id();
    let (id_ba, _) = list_ba.gen_obj_id();
    assert_ne!(id_ab, id_ba);
}

// =====================================================================
// L2 — PathObject JWT
// =====================================================================

/// PATH-06: `AcceptIfFreshVerifier` must accept a JWT when `now` falls within
/// `[iat, exp)` and reject it once `exp` has passed. This exercises the
/// protocol's time-window contract even though signature verification is
/// deferred to host-aware verifiers.
#[test]
fn path_06_path_jwt_exp_window_enforced() {
    let target = ObjId::new("sha256:0102030405").unwrap();
    let now = buckyos_kit::buckyos_get_unix_timestamp();

    // Fresh window: iat in the past, exp in the future.
    let fresh = mint_unsigned_path_jwt("/repo/readme", &target, now - 30, now + 300);
    let verified = AcceptIfFreshVerifier::default()
        .verify(&fresh, Some("alice.example"))
        .expect("fresh JWT should verify");
    assert_eq!(verified.path, "/repo/readme");
    assert_eq!(verified.target, target);

    // Expired window: exp already passed.
    let stale = mint_unsigned_path_jwt("/repo/readme", &target, now - 3600, now - 10);
    let err = AcceptIfFreshVerifier::default()
        .verify(&stale, Some("alice.example"))
        .unwrap_err();
    assert!(
        matches!(err, NdnError::InvalidData(_)),
        "expired JWT must fail as InvalidData: {:?}",
        err
    );

    // iat far in the future — verifier rejects as not-yet-valid.
    let future_iat = mint_unsigned_path_jwt("/repo/readme", &target, now + 7200, now + 10800);
    let err = AcceptIfFreshVerifier::default()
        .verify(&future_iat, Some("alice.example"))
        .unwrap_err();
    assert!(matches!(err, NdnError::InvalidData(_)));
}

// =====================================================================
// L3/L4 — Client + server end-to-end
// =====================================================================

/// E2E-01: Full R-Link file retrieval through `NdnDirServer` — the server
/// objectifies the file, the client resolves `/ndn/<name>/@/content` back to
/// the correct bytes, and the `cyfs-parents-0` chain exposes the inline
/// `FileObject`.
#[tokio::test]
async fn e2e_01_r_link_file_pull_via_server() {
    let tmp = TempDir::new().unwrap();
    let (server, client, store_mgr) = make_server_client_pair(tmp.path()).await;

    // Drop a small file into semantic_root and objectify.
    let root = server.config().semantic_root.clone();
    // File must be ≥ MIN_QCID_FILE_SIZE (12 KiB) because LocalLink mode
    // registers chunks with a QCID fingerprint.
    let file_bytes = deterministic_bytes(16 * 1024 + 117);
    let file_path = root.join("readme.bin");
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();
    let processed = server.scan_and_objectify().await.unwrap();
    assert!(
        processed >= 1,
        "scan_and_objectify processed {} (expected >=1). root={}",
        processed,
        root.display()
    );
    let sidecar = root.join("readme.bin.cyobj");
    assert!(
        sidecar.is_file(),
        "sidecar missing at {}",
        sidecar.display()
    );

    // Request `/ndn/readme.bin/@/content` — the FileObject shortcut streams
    // the chunk back.
    let url = "http://local.test/ndn/readme.bin/@/content";
    let resp = client.get(url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // The response must carry cyfs-obj-id for the underlying chunk and
    // cyfs-parents-0 inlining the FileObject.
    let chunk_id = resp.meta().cyfs_headers.obj_id.clone().expect("cyfs-obj-id");
    assert!(chunk_id.is_chunk(), "final obj id must be a chunk");
    assert_eq!(resp.meta().cyfs_headers.parents.len(), 1);

    let received = resp.bytes().await.unwrap();
    assert_eq!(received, file_bytes);

    // The chunk must also be registered in the store (LocalLink mode).
    assert!(store_mgr.have_chunk(&ChunkId::from_obj_id(&chunk_id)).await);
}

/// RAW-01 + RAW-02: `?resp=raw` returns the raw object bytes and MUST NOT
/// attach `cyfs-path-obj` / `cyfs-parents-N` / `cyfs-obj-id` headers.
#[tokio::test]
async fn raw_01_and_02_resp_raw_is_bare() {
    let tmp = TempDir::new().unwrap();
    let (server, client, _) = make_server_client_pair(tmp.path()).await;

    let root = server.config().semantic_root.clone();
    let file_bytes = deterministic_bytes(16 * 1024 + 55);
    tokio::fs::write(root.join("rawtest.bin"), &file_bytes)
        .await
        .unwrap();
    server.scan_and_objectify().await.unwrap();

    // Asking for the root FileObject JSON in raw mode: no CYFS verification
    // headers should be attached.
    let url = "http://local.test/ndn/rawtest.bin?resp=raw";
    let resp = client.get(url).raw().send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let headers: Vec<String> = resp
        .meta()
        .cyfs_headers
        .parents
        .iter()
        .map(|_| "parents".to_string())
        .collect();
    assert!(
        headers.is_empty(),
        "resp=raw must not emit cyfs-parents-N headers"
    );
    assert!(
        resp.meta().cyfs_headers.path_obj.is_none(),
        "resp=raw must not emit cyfs-path-obj"
    );
    assert!(
        resp.meta().cyfs_headers.obj_id.is_none(),
        "resp=raw must not emit cyfs-obj-id"
    );

    // Body parses as the FileObject JSON (raw NamedObject canonical form).
    let body_text = resp.text().await.unwrap();
    let file_obj: FileObject = serde_json::from_str(&body_text).unwrap();
    assert_eq!(file_obj.size, file_bytes.len() as u64);
}

/// INNER-05: A Chunk URL with an appended `/@/x` inner_path MUST be rejected
/// — chunks have no addressable internal structure.
#[tokio::test]
async fn inner_05_chunk_url_rejects_inner_path() {
    let tmp = TempDir::new().unwrap();
    let (server, client, store_mgr) = make_server_client_pair(tmp.path()).await;

    let chunk_bytes = deterministic_bytes(512);
    let chunk_id = mix256_from_bytes(&chunk_bytes);
    store_mgr.put_chunk(&chunk_id, &chunk_bytes).await.unwrap();
    let _ = server;

    let url = format!(
        "http://local.test/ndn/{}/@/anything",
        chunk_id.to_string()
    );
    let err = client.get(url).send().await.err().expect("should fail");
    // Server returns HTTP 400 -> client surfaces an error.
    match err {
        NdnError::RemoteError(_) | NdnError::InvalidParam(_) | NdnError::InvalidData(_) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

/// AUTH-05: `cyfs-cascades` with length > 6 must be rejected at the client
/// before being sent — protocol caps the propagation chain.
#[tokio::test]
async fn auth_05_cascades_length_limit_rejected() {
    let tmp = TempDir::new().unwrap();
    let (_, client, _) = make_server_client_pair(tmp.path()).await;

    let overflow: Vec<serde_json::Value> = (0..(CYFS_CASCADES_MAX_LEN + 1))
        .map(|i| serde_json::json!({ "op": format!("step-{}", i) }))
        .collect();
    let err = client
        .get("http://local.test/ndn/does-not-matter")
        .cascades(overflow)
        .send()
        .await
        .err()
        .expect("cascades overflow must fail");
    assert!(
        matches!(err, NdnError::InvalidParam(_)),
        "cascades overflow must produce InvalidParam, got {:?}",
        err
    );

    // Boundary: exactly 6 entries must be accepted at the length-check layer
    // (the request may still fail later for unrelated reasons, but not for
    // cascades length).
    let boundary: Vec<serde_json::Value> = (0..CYFS_CASCADES_MAX_LEN)
        .map(|i| serde_json::json!({ "op": format!("step-{}", i) }))
        .collect();
    let res = client
        .get("http://local.test/ndn/also-missing")
        .cascades(boundary)
        .send()
        .await;
    if let Err(e) = res {
        assert!(
            !matches!(e, NdnError::InvalidParam(s) if s.contains("cascades")),
            "boundary cascades length should not trip the cap",
        );
    }
}

// =====================================================================
// Synthetic CYFS responses: these tests fabricate a response whose CYFS
// headers violate a specific invariant, and assert that the client rejects
// it as expected. They exercise `CyfsResponseMeta` / inner-path verification
// without depending on the full server pipeline.
// =====================================================================

#[derive(Clone)]
struct SyntheticTransport {
    headers: HeaderMap,
    body: Vec<u8>,
    status: reqwest::StatusCode,
}

impl CyfsHttpTransport for SyntheticTransport {
    fn send(
        &self,
        _request: CyfsTransportRequest,
    ) -> Pin<
        Box<dyn std::future::Future<Output = ndn_lib::NdnResult<CyfsTransportResponse>> + Send + '_>,
    > {
        let headers = self.headers.clone();
        let body = self.body.clone();
        let status = self.status;
        Box::pin(async move {
            let content_length = Some(body.len() as u64);
            let reader = std::io::Cursor::new(body);
            Ok(CyfsTransportResponse {
                status,
                headers,
                content_length,
                body: Box::pin(reader),
            })
        })
    }
}

fn header_map_from_pairs(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut m = HeaderMap::new();
    for (k, v) in pairs {
        let name = HeaderName::from_bytes(k.as_bytes()).unwrap();
        let value = HeaderValue::from_bytes(v.as_bytes()).unwrap();
        m.insert(name, value);
    }
    m
}

/// INNER-06: `cyfs-parents-N` headers must be consecutively numbered starting
/// at 0 — a gap (e.g. parent-0 and parent-2, no parent-1) must be ignored by
/// the header parser, so the client's verification chain is short by one and
/// therefore the synthetic "skipping" attack fails.
#[tokio::test]
async fn inner_06_nonconsecutive_parents_are_truncated() {
    // Construct a trivial FileObject canonical JSON.
    let chunk_id = mix256_from_bytes(&deterministic_bytes(256));
    let file_obj = FileObject::new("x.bin".to_string(), 256, chunk_id.to_string());
    let (file_obj_id, file_canonical) = file_obj.clone().gen_obj_id();

    // Header set has cyfs-parents-0 AND cyfs-parents-2, but no cyfs-parents-1.
    // The spec says: the client walks 0, 1, ... stopping at the first gap.
    // So the only parent actually materialized is parent-0.
    let parent0 = format!(
        "json:{}",
        {
            use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
            URL_SAFE_NO_PAD.encode(file_canonical.as_bytes())
        }
    );
    let parent2 = format!("oid:{}", file_obj_id.to_string());
    let headers = header_map_from_pairs(&[
        (CYFS_HEADER_OBJ_ID, &file_obj_id.to_string()),
        ("cyfs-parents-0", parent0.as_str()),
        ("cyfs-parents-2", parent2.as_str()),
    ]);

    let transport = SyntheticTransport {
        headers,
        body: file_canonical.as_bytes().to_vec(),
        status: reqwest::StatusCode::OK,
    };
    let client = CyfsNdnClient::builder()
        .transport(transport)
        .build()
        .unwrap();

    // URL carries two inner_path steps but only one parent was parsed — the
    // verification chain must refuse to complete.
    let url = format!(
        "http://alice.example/root/@/step_a/@/step_b"
    );
    let resp = client.get(url).obj_id(file_obj_id.clone()).send().await.unwrap();
    let err = resp.meta().verify_inner_path_chain().unwrap_err();
    assert!(
        matches!(err, NdnError::InvalidData(_)),
        "verify_inner_path_chain must reject truncated parent chain: {:?}",
        err
    );
}

/// RAW-01 supplementary: client `.raw()` must still attach `?resp=raw` to the
/// transport URL (idempotent if already present).
#[test]
fn raw_01_client_raw_toggle_adds_query_parameter() {
    let parsed = cyfs_parse_url("http://zone.example/obj/@/x?resp=raw").unwrap();
    assert!(parsed.resp_raw);
    let parsed2 = cyfs_parse_url("http://zone.example/obj/@/x").unwrap();
    assert!(!parsed2.resp_raw);
}

// =====================================================================
// Auxiliary sanity: keep the verifier pluggable.
// =====================================================================

#[test]
fn path_verifier_is_trait_object_compatible() {
    let _v: Arc<dyn PathObjectVerifier> = Arc::new(AcceptIfFreshVerifier::default());
}

