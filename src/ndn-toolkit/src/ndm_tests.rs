//! NamedDataMgr Proxy 协议集成测试。
//!
//! 一端是 `named_store::NamedDataMgrNodeGateway`（服务端），
//! 另一端是 `ndn-toolkit::NdmClient`（客户端），
//! 通过真实 HTTP/1.1 loopback 连接串联起来，
//! 覆盖 `doc/ndm_proxy_testcases.md` 中的首批回归子集与端到端闭环。

use bytes::Bytes;
use cyfs_gateway_lib::{HttpServer, ServerError, ServerErrorCode, StreamInfo};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use named_store::{
    ChunkStoreState, NamedDataMgr, NamedDataMgrNodeGateway, NamedLocalStore, NdmNodeGatewayConfig,
    ObjectState, StoreLayout, StoreTarget,
};
use crate::{NdmClient, NdmClientConfig};
use ndn_lib::{
    ChunkHasher, ChunkId, ChunkList, FileObject, HashMethod, NamedObject, NdnError, ObjId,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

// ==================== Test harness ====================

/// Build a single-store `NamedDataMgr` rooted at `base_dir` (same layout as the
/// toolkit's own tests).
async fn create_test_store_mgr(base_dir: &Path) -> Arc<NamedDataMgr> {
    let store = NamedLocalStore::get_named_store_by_path(base_dir.join("named_store"))
        .await
        .unwrap();
    let store_id = store.store_id().to_string();
    let store_ref = Arc::new(tokio::sync::Mutex::new(store));

    let store_mgr = NamedDataMgr::new();
    store_mgr.register_store(store_ref).await;
    store_mgr
        .add_layout(StoreLayout::new(
            1,
            vec![StoreTarget {
                store_id,
                device_did: String::new(),
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

struct TestServer {
    base_url: String,
    store_mgr: Arc<NamedDataMgr>,
    _temp_dir: TempDir,
}

impl TestServer {
    async fn start(restricted: bool) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let store_mgr = create_test_store_mgr(temp_dir.path()).await;
        let gateway = Arc::new(NamedDataMgrNodeGateway::new(
            store_mgr.clone(),
            NdmNodeGatewayConfig {
                restricted_enabled: restricted,
            },
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let gw = gateway.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let gw = gw.clone();
                        async move {
                            let gw_req = req.map(|body| {
                                body.map_err(|e| {
                                    ServerError::new(
                                        ServerErrorCode::StreamError,
                                        format!("incoming body error: {e}"),
                                    )
                                })
                                .boxed()
                            });
                            let resp = gw
                                .serve_request(gw_req, StreamInfo::default())
                                .await
                                .unwrap_or_else(|e| {
                                    http::Response::builder()
                                        .status(500)
                                        .body(
                                            Full::new(Bytes::from(format!("gateway error: {e}")))
                                                .map_err(|never| match never {})
                                                .boxed(),
                                        )
                                        .unwrap()
                                });
                            Ok::<_, std::io::Error>(resp)
                        }
                    });
                    let mut builder = http1::Builder::new();
                    builder.half_close(true);
                    let _ = builder
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        Self {
            base_url,
            store_mgr,
            _temp_dir: temp_dir,
        }
    }

    fn client(&self) -> NdmClient {
        NdmClient::new(NdmClientConfig {
            base_url: self.base_url.clone(),
        })
    }
}

fn calc_chunk_id(data: &[u8]) -> ChunkId {
    ChunkHasher::new_with_hash_method(HashMethod::Sha256)
        .unwrap()
        .calc_mix_chunk_id_from_bytes(data)
        .unwrap()
}

fn deterministic_bytes(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| ((idx * 31 + idx / 7 + seed) % 251) as u8)
        .collect()
}

// ==================== GEN: 通用协议 ====================

/// GEN-03: 路由存在但 method 错误 → 405 / unsupported。
#[tokio::test]
async fn gen_03_method_mismatch_returns_405() {
    let srv = TestServer::start(false).await;

    // /rpc/get_object 只接受 POST，发 GET。
    let resp = reqwest::Client::new()
        .get(format!("{}/ndm/proxy/v1/rpc/get_object", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "unsupported");

    // /write/chunk/{id} 只接受 PUT，发 POST。
    let chunk_id = calc_chunk_id(b"x");
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/ndm/proxy/v1/write/chunk/{}",
            srv.base_url,
            chunk_id.to_string()
        ))
        .body("hi")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);
}

/// GEN-02: 未知路由 → 404 not_found。
#[tokio::test]
async fn gen_02_unknown_route_returns_404() {
    let srv = TestServer::start(false).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/rpc/not_exists", srv.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "not_found");
    assert!(body["message"].is_string());
}

/// GEN-07: ID 字段格式非法 → 400 invalid_id。
#[tokio::test]
async fn gen_07_invalid_id_returns_invalid_id() {
    let srv = TestServer::start(false).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/rpc/get_object", srv.base_url))
        .json(&serde_json::json!({"obj_id": "this-is-not-a-valid-id"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_id");
}

/// GEN-06: 必填字段缺失 → 400 invalid_param。
/// 按协议文档固定错误码，避免 invalid_data 之类的回归被放过。
#[tokio::test]
async fn gen_06_missing_field_returns_invalid_param() {
    let srv = TestServer::start(false).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/rpc/get_object", srv.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap().trim().to_string()),
        Some("application/json".to_string())
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"], "invalid_param",
        "missing required field must map to invalid_param per protocol doc"
    );
    assert!(body["message"].is_string());
}

// ==================== OBJ: 对象类 RPC ====================

/// OBJ-02: 读取不存在对象返回 404。
#[tokio::test]
async fn obj_02_get_missing_object_returns_not_found() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let missing_id = ObjId::new(
        "cyfile:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    )
    .unwrap();
    let err = client.get_object(&missing_id).await.unwrap_err();
    assert!(matches!(err, NdnError::NotFound(_)), "got: {err}");
}

/// OBJ-03: `open_object` 的 inner_path 规范化：null / "" / "/" 行为一致。
#[tokio::test]
async fn obj_03_open_object_inner_path_normalization() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 准备一个 file object。inner_path=None 时等价于 get_object。
    let chunk = deterministic_bytes(64, 0);
    let chunk_id = calc_chunk_id(&chunk);
    srv.store_mgr.put_chunk(&chunk_id, &chunk).await.unwrap();

    let file_obj = FileObject::new("a.bin".to_string(), chunk.len() as u64, chunk_id.to_string());
    let (obj_id, obj_str) = file_obj.gen_obj_id();
    srv.store_mgr.put_object(&obj_id, &obj_str).await.unwrap();

    let a = client.open_object(&obj_id, None).await.unwrap();
    let b = client.open_object(&obj_id, Some("".into())).await.unwrap();
    let c = client.open_object(&obj_id, Some("/".into())).await.unwrap();
    assert_eq!(a, b);
    assert_eq!(a, c);
}

/// OBJ-13: put_object 不接受 chunk id。
#[tokio::test]
async fn obj_13_put_object_rejects_chunk_id() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let chunk_id = calc_chunk_id(b"abc");
    let chunk_obj_id = chunk_id.to_obj_id();

    // 客户端侧有前置校验。
    let err = client
        .put_object(&chunk_obj_id, "dummy")
        .await
        .unwrap_err();
    assert!(matches!(err, NdnError::InvalidObjType(_)), "got: {err}");

    // 绕过客户端校验，直接打到 HTTP 端点，确认服务端也会拒绝。
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/rpc/put_object", srv.base_url))
        .json(&serde_json::json!({
            "obj_id": chunk_obj_id.to_string(),
            "obj_data": "dummy",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let code = body["error"].as_str().unwrap();
    assert!(
        code == "invalid_param" || code == "invalid_obj_type",
        "expected 400 class, got {code}"
    );
}

/// OBJ-15: remove_object 不接受 chunk id。
/// 先走客户端校验，再绕过客户端直接打到 /rpc/remove_object 验证服务端也会拒绝。
#[tokio::test]
async fn obj_15_remove_object_rejects_chunk_id() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 先放一个真实的 chunk，确保即使服务端误把请求当 chunk 来处理也能观察到“未被删除”。
    let data = b"zzz-keep-me".to_vec();
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();
    let chunk_obj_id = chunk_id.to_obj_id();

    // 客户端前置校验。
    let err = client.remove_object(&chunk_obj_id).await.unwrap_err();
    assert!(matches!(err, NdnError::InvalidObjType(_)), "got: {err}");

    // 绕过客户端，直接打 /rpc/remove_object：服务端必须自己拒绝。
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/rpc/remove_object", srv.base_url))
        .json(&serde_json::json!({ "obj_id": chunk_obj_id.to_string() }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let code = body["error"].as_str().unwrap();
    assert!(
        code == "invalid_param" || code == "invalid_obj_type",
        "expected 400 class rejection, got {code}"
    );

    // chunk 不得被误删。
    assert!(srv.store_mgr.have_chunk(&chunk_id).await);
}

// ==================== CHK: Chunk 元数据 ====================

/// CHK-01 / CHK-02 / CHK-03 / CHK-04: 基础存在性 + state shape。
#[tokio::test]
async fn chk_have_and_query_state_basic() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    let data = b"chunk body contents".to_vec();
    let chunk_id = calc_chunk_id(&data);

    // 不存在 → have_chunk=false，query_chunk_state=NotExist。
    assert!(!client.have_chunk(&chunk_id).await);
    let (state, _) = client.query_chunk_state(&chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::NotExist);

    // 写入后 → have_chunk=true，query_chunk_state=Completed 且 chunk_size 正确。
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();
    assert!(client.have_chunk(&chunk_id).await);
    let (state, size) = client.query_chunk_state(&chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::Completed);
    assert_eq!(size, data.len() as u64);
}

/// CHK-05: query_chunk_state 对 same_as chunk 返回 `state=same_as` 且含 `same_as` 字段。
#[tokio::test]
async fn chk_05_same_as_state_shape() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 1. 准备一个 chunklist（作为 same_as 的 target），包含 1 个小 chunk。
    let inner = deterministic_bytes(256, 7);
    let inner_cid = calc_chunk_id(&inner);
    srv.store_mgr.put_chunk(&inner_cid, &inner).await.unwrap();
    let chunk_list = ChunkList::from_chunk_list(vec![inner_cid.clone()]).unwrap();
    let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
    srv.store_mgr
        .put_object(&chunk_list_id, &chunk_list_str)
        .await
        .unwrap();

    // 2. 构造一个 "大 chunk"：只要与 chunklist 有匹配的 hash 就行；直接拿
    //    chunklist 的 qcid 作为 chunk_id 即可 —— `add_chunk_by_same_as` 要求 qcid 一致。
    let big_chunk_id = inner_cid.clone();
    // same_as 要求 big_chunk_size >= chunklist.total_size。
    srv.store_mgr.remove_chunk(&big_chunk_id).await.unwrap();
    client
        .add_chunk_by_same_as(&big_chunk_id, inner.len() as u64, &chunk_list_id)
        .await
        .unwrap();

    let (state, size) = client.query_chunk_state(&big_chunk_id).await.unwrap();
    match state {
        ChunkStoreState::SameAs(target) => {
            assert_eq!(target, chunk_list_id);
        }
        other => panic!("expected SameAs, got {other:?}"),
    }
    assert_eq!(size, inner.len() as u64);
}

// ==================== READ: 流式读 ====================

/// READ-04 / READ-09: chunk/open 的 offset 边界。
#[tokio::test]
async fn read_04_open_chunk_offset_boundary() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    let data = deterministic_bytes(1024, 3);
    let n = data.len() as u64;
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    // offset=0 → 读完整内容
    let (mut reader, total) = client.open_chunk_reader(&chunk_id, 0).await.unwrap();
    assert_eq!(total, n);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data);

    // offset=N-1 → 读到 1 字节
    let (mut reader, total) = client.open_chunk_reader(&chunk_id, n - 1).await.unwrap();
    assert_eq!(total, n);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data[n as usize - 1..]);

    // offset=N → 允许空流（总长度仍然是 N）
    let (mut reader, total) = client.open_chunk_reader(&chunk_id, n).await.unwrap();
    assert_eq!(total, n);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert!(buf.is_empty());

    // offset=N+1 → offset_too_large
    let err = match client.open_chunk_reader(&chunk_id, n + 1).await {
        Ok(_) => panic!("expected OffsetTooLarge for offset=N+1"),
        Err(e) => e,
    };
    assert!(matches!(err, NdnError::OffsetTooLarge(_)), "got {err}");
}

/// READ-05: offset>0 时，返回 `NDM-Offset` 头。
#[tokio::test]
async fn read_05_open_chunk_returns_offset_header() {
    let srv = TestServer::start(false).await;
    let data = deterministic_bytes(512, 9);
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    let resp = reqwest::Client::new()
        .post(format!(
            "{}/ndm/proxy/v1/read/chunk/open",
            srv.base_url
        ))
        .json(&serde_json::json!({
            "chunk_id": chunk_id.to_string(),
            "offset": 10,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("ndm-offset")
            .and_then(|v| v.to_str().ok()),
        Some("10")
    );
    assert_eq!(
        resp.headers()
            .get("ndm-total-size")
            .and_then(|v| v.to_str().ok()),
        Some(&*data.len().to_string())
    );
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
}

/// READ-06: chunk/data 等价于 chunk/open(offset=0)。
#[tokio::test]
async fn read_06_chunk_data_equals_chunk_open_zero() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let data = deterministic_bytes(1000, 5);
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    let via_data = client.get_chunk_data(&chunk_id).await.unwrap();
    assert_eq!(via_data, data);

    let (mut reader, total) = client.open_chunk_reader(&chunk_id, 0).await.unwrap();
    let mut via_open = Vec::new();
    reader.read_to_end(&mut via_open).await.unwrap();
    assert_eq!(total, data.len() as u64);
    assert_eq!(via_open, data);
}

/// READ-07: chunk/piece 正常定长读取。
#[tokio::test]
async fn read_07_chunk_piece_fixed_length() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let data = deterministic_bytes(2048, 11);
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    let piece_size = 512u32;
    let offset = 100u64;
    let piece = client
        .get_chunk_piece(&chunk_id, offset, piece_size)
        .await
        .unwrap();
    assert_eq!(piece.len(), piece_size as usize);
    assert_eq!(
        piece,
        data[offset as usize..offset as usize + piece_size as usize]
    );
}

/// READ-08: chunk/piece 短读必须在 wire 层表现为错误 —— 直接打
/// `/read/chunk/piece`，断言 HTTP status、header、body 全部不是“短读成功”。
#[tokio::test]
async fn read_08_chunk_piece_short_read_is_error() {
    let srv = TestServer::start(false).await;
    let data = deterministic_bytes(16, 2);
    let n = data.len() as u64;
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    // offset=N-1, piece_size=2：只剩 1 字节 < piece_size，服务端绝不允许返回 200。
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/read/chunk/piece", srv.base_url))
        .json(&serde_json::json!({
            "chunk_id": chunk_id.to_string(),
            "offset": n - 1,
            "piece_size": 2,
        }))
        .send()
        .await
        .unwrap();

    let status = resp.status();
    assert!(
        status.as_u16() >= 400,
        "short read must not be 2xx; got status={status}"
    );

    // 错误体必须是 JSON，且 error 字段存在 —— 不接受 200 + 短 body 这种“成功但数据少”的形态。
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/json"),
        "error response must be JSON, got content-type={ct}"
    );
    // 错误响应不得带 NDM-Reader-Kind / NDM-Total-Size 这类流式头（避免被误判为读流）。
    assert!(
        resp.headers().get("ndm-reader-kind").is_none(),
        "error response should not carry streaming header ndm-reader-kind"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"].is_string(),
        "missing error field in response: {body}"
    );
    assert!(body["message"].is_string());
}

/// READ-09: chunk/piece 越界 → offset_too_large。
#[tokio::test]
async fn read_09_chunk_piece_offset_too_large() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let data = b"bar".to_vec();
    let chunk_id = calc_chunk_id(&data);
    srv.store_mgr.put_chunk(&chunk_id, &data).await.unwrap();

    let err = client
        .get_chunk_piece(&chunk_id, data.len() as u64 + 1, 1)
        .await
        .unwrap_err();
    assert!(matches!(err, NdnError::OffsetTooLarge(_)), "got {err}");
}

/// READ-12 / READ-11: chunklist/open 的 NDM-Total-Size 必须等于拼接总长度，
/// 且 NDM-Reader-Kind=chunklist。
#[tokio::test]
async fn read_12_chunklist_open_total_size_and_kind() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 构造 3 个 chunk + chunklist
    let parts: Vec<Vec<u8>> = vec![
        deterministic_bytes(500, 1),
        deterministic_bytes(700, 2),
        deterministic_bytes(300, 3),
    ];
    let total_len: u64 = parts.iter().map(|p| p.len() as u64).sum();
    let mut chunk_ids = Vec::new();
    for p in &parts {
        let cid = calc_chunk_id(p);
        srv.store_mgr.put_chunk(&cid, p).await.unwrap();
        chunk_ids.push(cid);
    }
    let chunk_list = ChunkList::from_chunk_list(chunk_ids).unwrap();
    let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
    srv.store_mgr
        .put_object(&chunk_list_id, &chunk_list_str)
        .await
        .unwrap();

    // 低层 reqwest 校验 headers。
    let resp = reqwest::Client::new()
        .post(format!("{}/ndm/proxy/v1/read/chunklist/open", srv.base_url))
        .json(&serde_json::json!({
            "chunk_list_id": chunk_list_id.to_string(),
            "offset": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("ndm-total-size")
            .and_then(|v| v.to_str().ok()),
        Some(&*total_len.to_string())
    );
    assert_eq!(
        resp.headers()
            .get("ndm-reader-kind")
            .and_then(|v| v.to_str().ok()),
        Some("chunklist")
    );

    // 客户端 API：reader 必须读出完整拼接内容。
    let (mut reader, total) = client
        .open_chunklist_reader(&chunk_list_id, 0)
        .await
        .unwrap();
    assert_eq!(total, total_len);
    let mut body = Vec::new();
    reader.read_to_end(&mut body).await.unwrap();
    let mut expected = Vec::new();
    for p in &parts {
        expected.extend_from_slice(p);
    }
    assert_eq!(body, expected);
}

/// READ-13: object/open 的 inner_path 规范化：null / "" / "/" 行为一致。
#[tokio::test]
async fn read_13_object_open_inner_path_normalization() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 构造一个 file object -> chunk
    let chunk = deterministic_bytes(256, 4);
    let chunk_id = calc_chunk_id(&chunk);
    srv.store_mgr.put_chunk(&chunk_id, &chunk).await.unwrap();
    let file_obj = FileObject::new(
        "inner.bin".to_string(),
        chunk.len() as u64,
        chunk_id.to_string(),
    );
    let (file_id, file_str) = file_obj.gen_obj_id();
    srv.store_mgr.put_object(&file_id, &file_str).await.unwrap();

    let read_all = |p: Option<String>| {
        let client = client.clone();
        let file_id = file_id.clone();
        async move {
            let (mut reader, _) = client.open_reader(&file_id, p).await.unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            buf
        }
    };

    let a = read_all(None).await;
    let b = read_all(Some("".into())).await;
    let c = read_all(Some("/".into())).await;
    assert_eq!(a, b);
    assert_eq!(a, c);
    assert_eq!(a, chunk);
}

/// READ-17: 读不存在对象 → 404 not_found。
#[tokio::test]
async fn read_17_not_found() {
    let srv = TestServer::start(false).await;
    let client = srv.client();
    let missing_chunk = calc_chunk_id(b"never-stored");
    let err = client.get_chunk_data(&missing_chunk).await.unwrap_err();
    assert!(matches!(err, NdnError::NotFound(_)), "got {err}");
}

// ==================== WRITE: 流式写 ====================

/// WRITE-02: Content-Type 必须是 application/octet-stream，否则 400。
#[tokio::test]
async fn write_02_wrong_content_type_is_rejected() {
    let srv = TestServer::start(false).await;
    let data = b"hello-write-ct";
    let chunk_id = calc_chunk_id(data);

    let resp = reqwest::Client::new()
        .put(format!(
            "{}/ndm/proxy/v1/write/chunk/{}",
            srv.base_url,
            chunk_id.to_string()
        ))
        .header("content-type", "text/plain")
        .header("ndm-chunk-size", data.len())
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(!srv.store_mgr.have_chunk(&chunk_id).await);
}

/// WRITE-04: 缺失 NDM-Chunk-Size 头 → 400。
#[tokio::test]
async fn write_04_missing_chunk_size_header() {
    let srv = TestServer::start(false).await;
    let data = b"missing-size";
    let chunk_id = calc_chunk_id(data);
    let resp = reqwest::Client::new()
        .put(format!(
            "{}/ndm/proxy/v1/write/chunk/{}",
            srv.base_url,
            chunk_id.to_string()
        ))
        .header("content-type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(!srv.store_mgr.have_chunk(&chunk_id).await);
}

/// WRITE-05: Content-Length 与 NDM-Chunk-Size 不一致 → 400 invalid_param。
#[tokio::test]
async fn write_05_content_length_mismatch() {
    let srv = TestServer::start(false).await;
    let data = vec![7u8; 32];
    let chunk_id = calc_chunk_id(&data);

    // Content-Length 会由 reqwest 根据 body 自动设为 32；我们把 ndm-chunk-size 设为 64。
    let resp = reqwest::Client::new()
        .put(format!(
            "{}/ndm/proxy/v1/write/chunk/{}",
            srv.base_url,
            chunk_id.to_string()
        ))
        .header("content-type", "application/octet-stream")
        .header("ndm-chunk-size", 64)
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_param");
    assert!(!srv.store_mgr.have_chunk(&chunk_id).await);
}

/// WRITE-07 & WRITE-11: body 与路径上的 chunk_id 不匹配 →
///   - HTTP 409 + `error=verify_error`（协议 §12.3 固定映射）
///   - 响应体为 JSON 错误体，而不是空或 HTML
///   - 失败写入不得留下任何可见 chunk（原子写入）
/// 直连 HTTP 端点断言，避免 NdmClient 把状态码归一化到 NdnError 后丢失协议细节。
#[tokio::test]
async fn write_07_content_mismatch_fails_atomically() {
    let srv = TestServer::start(false).await;

    let real_data = b"original-content".to_vec();
    let bogus_data = b"tampered-content".to_vec();
    assert_ne!(real_data, bogus_data);
    let chunk_id = calc_chunk_id(&real_data); // path = id of real_data

    let resp = reqwest::Client::new()
        .put(format!(
            "{}/ndm/proxy/v1/write/chunk/{}",
            srv.base_url,
            chunk_id.to_string()
        ))
        .header("content-type", "application/octet-stream")
        .header("ndm-chunk-size", bogus_data.len())
        .body(bogus_data.clone())
        .send()
        .await
        .unwrap();

    // 协议层契约：409 Conflict + error=verify_error。
    assert_eq!(
        resp.status().as_u16(),
        409,
        "content/hash mismatch must map to HTTP 409, got {}",
        resp.status()
    );
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/json"),
        "error response must be JSON, got content-type={ct}"
    );
    // 不得泄露写入成功类响应头（避免客户端误判写入成功）。
    assert!(
        resp.headers().get("ndm-chunk-write-outcome").is_none(),
        "failed write must not return NDM-Chunk-Write-Outcome"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"], "verify_error",
        "content/hash mismatch must map to error=verify_error"
    );
    assert!(body["message"].is_string());

    // 原子语义：失败后 chunk 仍为 NotExist，而不是部分可读。
    let client = srv.client();
    assert!(!client.have_chunk(&chunk_id).await);
    let (state, _) = client.query_chunk_state(&chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::NotExist);
}

/// WRITE-08 / WRITE-09 / WRITE-12: 第一次 written，重复 already_exists，响应头完整。
#[tokio::test]
async fn write_08_written_then_already_exists() {
    let srv = TestServer::start(false).await;
    let data = deterministic_bytes(1024, 13);
    let chunk_id = calc_chunk_id(&data);

    let url = format!(
        "{}/ndm/proxy/v1/write/chunk/{}",
        srv.base_url,
        chunk_id.to_string()
    );

    let first = reqwest::Client::new()
        .put(&url)
        .header("content-type", "application/octet-stream")
        .header("ndm-chunk-size", data.len())
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);
    assert_eq!(
        first
            .headers()
            .get("ndm-chunk-write-outcome")
            .and_then(|v| v.to_str().ok()),
        Some("written")
    );
    assert_eq!(
        first
            .headers()
            .get("ndm-chunk-size")
            .and_then(|v| v.to_str().ok()),
        Some(&*data.len().to_string())
    );
    assert!(first
        .headers()
        .get("ndm-chunk-object-id")
        .and_then(|v| v.to_str().ok())
        .is_some());

    // 重复写入相同内容 → already_exists。
    let second = reqwest::Client::new()
        .put(&url)
        .header("content-type", "application/octet-stream")
        .header("ndm-chunk-size", data.len())
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(
        second
            .headers()
            .get("ndm-chunk-write-outcome")
            .and_then(|v| v.to_str().ok()),
        Some("already_exists")
    );
}

// ==================== AUTH: 权限模型 ====================

/// AUTH-01: 默认关闭受限能力，受限接口返回 403 permission_denied。
#[tokio::test]
async fn auth_01_restricted_disabled_by_default() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // outbox_count 属于受限接口。
    let err = client.outbox_count().await.unwrap_err();
    assert!(matches!(err, NdnError::PermissionDenied(_)), "got {err}");

    // forced_gc_until 也受限。
    let err = client.forced_gc_until(0).await.unwrap_err();
    assert!(matches!(err, NdnError::PermissionDenied(_)), "got {err}");

    // unpin_owner 也受限。
    let err = client.unpin_owner("anyone").await.unwrap_err();
    assert!(matches!(err, NdnError::PermissionDenied(_)), "got {err}");
}

/// AUTH-02 / AUTH-05: 显式开启后 outbox_count 可调用。
#[tokio::test]
async fn auth_02_restricted_enabled_allows_outbox_count() {
    let srv = TestServer::start(true).await;
    let client = srv.client();
    let n = client.outbox_count().await.unwrap();
    // 新建的 store 里，outbox 通常是 0（但不必死锁定，只要不报 403 即可）。
    let _ = n;
}

// ==================== E2E: 端到端 ====================

/// E2E-01: put_object -> get_object -> remove_object 闭环。
#[tokio::test]
async fn e2e_01_object_roundtrip() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 构造一个非 chunk 的对象。
    let chunk = deterministic_bytes(32, 42);
    let chunk_id = calc_chunk_id(&chunk);
    let file_obj = FileObject::new(
        "roundtrip.bin".to_string(),
        chunk.len() as u64,
        chunk_id.to_string(),
    );
    let (obj_id, obj_str) = file_obj.gen_obj_id();

    assert!(!client.is_object_exist(&obj_id).await.unwrap());

    client.put_object(&obj_id, &obj_str).await.unwrap();
    assert!(client.is_object_exist(&obj_id).await.unwrap());

    let got = client.get_object(&obj_id).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&got).unwrap(),
        serde_json::from_str::<serde_json::Value>(&obj_str).unwrap()
    );

    // query_object_by_id 语义一致
    let state = client.query_object_by_id(&obj_id).await.unwrap();
    match state {
        ObjectState::Object(data) => {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&data).unwrap(),
                serde_json::from_str::<serde_json::Value>(&obj_str).unwrap()
            );
        }
        other => panic!("expected Object state, got {other:?}"),
    }

    client.remove_object(&obj_id).await.unwrap();
    assert!(!client.is_object_exist(&obj_id).await.unwrap());
    let state = client.query_object_by_id(&obj_id).await.unwrap();
    assert_eq!(state, ObjectState::NotExist);
}

/// E2E-02: write/chunk -> have_chunk -> query_chunk_state -> read/chunk/data 闭环。
#[tokio::test]
async fn e2e_02_chunk_write_then_read() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    let data = deterministic_bytes(4096 + 17, 17);
    let chunk_id = calc_chunk_id(&data);

    // 走完整的 PUT /write/chunk 路径。
    client.put_chunk(&chunk_id, &data).await.unwrap();
    assert!(client.have_chunk(&chunk_id).await);

    let (state, size) = client.query_chunk_state(&chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::Completed);
    assert_eq!(size, data.len() as u64);

    let back = client.get_chunk_data(&chunk_id).await.unwrap();
    assert_eq!(back, data);

    // remove 后状态一致。
    client.remove_chunk(&chunk_id).await.unwrap();
    assert!(!client.have_chunk(&chunk_id).await);
    let (state, _) = client.query_chunk_state(&chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::NotExist);
}

/// E2E-03: file object -> chunk，object/open 与 chunk/data 一致。
#[tokio::test]
async fn e2e_03_object_open_matches_chunk_data() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    let chunk = deterministic_bytes(2048, 23);
    let chunk_id = calc_chunk_id(&chunk);
    client.put_chunk(&chunk_id, &chunk).await.unwrap();

    let file_obj = FileObject::new(
        "e2e3.bin".to_string(),
        chunk.len() as u64,
        chunk_id.to_string(),
    );
    let (file_id, file_str) = file_obj.gen_obj_id();
    client.put_object(&file_id, &file_str).await.unwrap();

    let via_chunk = client.get_chunk_data(&chunk_id).await.unwrap();

    let (mut reader, total) = client.open_reader(&file_id, None).await.unwrap();
    assert_eq!(total, chunk.len() as u64);
    let mut via_object = Vec::new();
    reader.read_to_end(&mut via_object).await.unwrap();

    assert_eq!(via_chunk, chunk);
    assert_eq!(via_object, chunk);
}

/// E2E-04: file object -> chunklist，object/open 与 chunklist/open 一致。
#[tokio::test]
async fn e2e_04_object_open_matches_chunklist_open() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    let parts = vec![
        deterministic_bytes(1500, 31),
        deterministic_bytes(600, 32),
        deterministic_bytes(900, 33),
    ];
    let total: u64 = parts.iter().map(|p| p.len() as u64).sum();
    let mut chunk_ids = Vec::new();
    for p in &parts {
        let cid = calc_chunk_id(p);
        client.put_chunk(&cid, p).await.unwrap();
        chunk_ids.push(cid);
    }
    let chunk_list = ChunkList::from_chunk_list(chunk_ids).unwrap();
    let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
    client
        .put_object(&chunk_list_id, &chunk_list_str)
        .await
        .unwrap();

    let file_obj = FileObject::new(
        "e2e4.bin".to_string(),
        total,
        chunk_list_id.to_string(),
    );
    let (file_id, file_str) = file_obj.gen_obj_id();
    client.put_object(&file_id, &file_str).await.unwrap();

    // 通过 chunklist 直接读取
    let (mut r1, t1) = client
        .open_chunklist_reader(&chunk_list_id, 0)
        .await
        .unwrap();
    assert_eq!(t1, total);
    let mut via_list = Vec::new();
    r1.read_to_end(&mut via_list).await.unwrap();

    // 通过 file object 解析读取
    let (mut r2, t2) = client.open_reader(&file_id, None).await.unwrap();
    assert_eq!(t2, total);
    let mut via_file = Vec::new();
    r2.read_to_end(&mut via_file).await.unwrap();

    let mut expected = Vec::new();
    for p in &parts {
        expected.extend_from_slice(p);
    }
    assert_eq!(via_list, expected);
    assert_eq!(via_file, expected);
}

/// E2E-07: 错误请求不污染后续合法请求的状态。
#[tokio::test]
async fn e2e_07_error_path_does_not_pollute_state() {
    let srv = TestServer::start(false).await;
    let client = srv.client();

    // 发送若干非法请求。
    let missing = calc_chunk_id(b"missing-chunk");
    let _ = client.get_chunk_data(&missing).await.err();
    let _ = client
        .open_chunk_reader(&missing, 9999)
        .await
        .err();

    // 合法请求仍可正常完成。
    let data = b"still-works".to_vec();
    let chunk_id = calc_chunk_id(&data);
    client.put_chunk(&chunk_id, &data).await.unwrap();
    let back = client.get_chunk_data(&chunk_id).await.unwrap();
    assert_eq!(back, data);
}
