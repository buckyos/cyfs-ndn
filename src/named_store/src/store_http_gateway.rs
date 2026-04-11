//! `NamedStoreMgrHttpGateway` —— named-data-http-store 协议的服务端实现。
//!
//! 一个机器上只有一个实例，通过 `NamedStoreMgr` 管理多个 store 桶。
//! 协议详情见 `doc/named-data-http-store-protocol.md`。
//!
//! 资源类型由 `obj_id.is_chunk()` 自动判定，不需要额外 header。

use async_trait::async_trait;
use bytes::Bytes;
use cyfs_gateway_lib::{HttpServer, ServerError, ServerResult, StreamInfo};
use http::{Method, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use log::{info, warn};
use ndn_lib::{ChunkId, NdnError, ObjId};
use std::sync::Arc;

use crate::gc_types::{EdgeMsg, PinRequest};
use crate::store_mgr::NamedStoreMgr;

// Custom headers
const H_CHUNK_STATE: &str = "x-cyfs-chunk-state";
const H_CHUNK_SIZE: &str = "x-cyfs-chunk-size";
const H_OBJ_ID: &str = "x-cyfs-obj-id";
const H_CHUNK_ALREADY: &str = "x-cyfs-chunk-already";

const CONTENT_TYPE_OBJECT: &str = "application/cyfs-object";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";

/// 从 ChunkReader (AsyncRead) 逐块产出 Frame 的流适配器。
const STREAM_BUF_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct NamedStoreMgrHttpGateway {
    store_mgr: Arc<NamedStoreMgr>,
    /// When `false`, all `/_gc/*` endpoints return `gc_disabled` error.
    /// Large-scale deployments can set this to `false` to run in Data-only mode.
    gc_enabled: bool,
}

impl NamedStoreMgrHttpGateway {
    pub fn new(store_mgr: Arc<NamedStoreMgr>) -> Self {
        Self {
            store_mgr,
            gc_enabled: true,
        }
    }

    pub fn new_data_only(store_mgr: Arc<NamedStoreMgr>) -> Self {
        Self {
            store_mgr,
            gc_enabled: false,
        }
    }

    pub fn with_gc_enabled(mut self, enabled: bool) -> Self {
        self.gc_enabled = enabled;
        self
    }
}

#[async_trait]
impl HttpServer for NamedStoreMgrHttpGateway {
    async fn serve_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
        _info: StreamInfo,
    ) -> ServerResult<http::Response<BoxBody<Bytes, ServerError>>> {
        let result = self.route_request(req).await;
        match result {
            Ok(resp) => {
                info!("served request {}", resp.status());
                Ok(resp)
            }
            Err(e) => {
                let (status, error_code) = ndn_error_to_status(&e);
                warn!("request failed: {} -> {}", status, e);
                Ok(build_error_response(status, &error_code, &e.to_string()))
            }
        }
    }

    fn id(&self) -> String {
        "named-store-mgr".to_string()
    }

    fn http_version(&self) -> Version {
        Version::HTTP_11
    }

    fn http3_port(&self) -> Option<u16> {
        None
    }
}

impl NamedStoreMgrHttpGateway {
    /// Parse obj_id from the request URI path.
    /// Takes the last non-empty path segment as obj_id (format: `type:hex`).
    fn parse_obj_id_from_path(path: &str) -> Result<ObjId, NdnError> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Err(NdnError::InvalidParam("empty path".to_string()));
        }
        let last = segments.last().unwrap();
        ObjId::new(last)
    }

    async fn route_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
            .to_string();

        // ---- GC control-plane ----
        if let Some(gc_sub) = Self::strip_gc_prefix(&path) {
            return self.route_gc_request(gc_sub, req).await;
        }

        // ---- Data-plane ----
        let obj_id = Self::parse_obj_id_from_path(&path)?;
        let is_chunk = obj_id.is_chunk();
        let method = req.method().clone();

        match (&method, is_chunk) {
            // ---- Object ----
            (&Method::GET, false) => self.handle_get_object(&obj_id).await,
            (&Method::HEAD, false) => self.handle_head_object(&obj_id).await,
            (&Method::PUT, false) => self.handle_put_object(&obj_id, req).await,
            (&Method::DELETE, false) => self.handle_delete_object(&obj_id).await,
            // ---- Chunk ----
            (&Method::HEAD, true) => self.handle_head_chunk(&obj_id).await,
            (&Method::GET, true) => self.handle_get_chunk(&obj_id, &req).await,
            (&Method::PUT, true) => self.handle_put_chunk(&obj_id, req).await,
            (&Method::DELETE, true) => self.handle_delete_chunk(&obj_id).await,
            _ => Err(NdnError::Unsupported(format!(
                "{} on {}",
                method,
                if is_chunk { "chunk" } else { "object" }
            ))),
        }
    }

    // ======================== Object handlers ========================

    async fn handle_get_object(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let obj_str = self.store_mgr.get_object(obj_id).await?;
        let body_bytes = Bytes::from(obj_str);
        let len = body_bytes.len();
        ok_response_builder()
            .header("content-type", CONTENT_TYPE_OBJECT)
            .header("content-length", len)
            .header(H_OBJ_ID, obj_id.to_string())
            .body(full_body(body_bytes))
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    async fn handle_head_object(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let obj_str = self.store_mgr.get_object(obj_id).await?;
        ok_response_builder()
            .header("content-type", CONTENT_TYPE_OBJECT)
            .header("content-length", obj_str.len())
            .header(H_OBJ_ID, obj_id.to_string())
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    async fn handle_put_object(
        &self,
        obj_id: &ObjId,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let obj_str = String::from_utf8(body)
            .map_err(|e| NdnError::InvalidData(format!("invalid utf8: {e}")))?;
        self.store_mgr.put_object(obj_id, &obj_str).await?;

        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    async fn handle_delete_object(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        self.store_mgr.remove_object(obj_id).await?;
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    // ======================== Chunk handlers ========================

    async fn handle_head_chunk(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let chunk_id = ChunkId::from_obj_id(obj_id);
        let have = self.store_mgr.have_chunk(&chunk_id).await;
        if !have {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(H_CHUNK_STATE, "not_exist")
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}")));
        }

        let (_, chunk_size) = self.store_mgr.query_chunk_state(&chunk_id).await?;
        ok_response_builder()
            .header("content-length", chunk_size)
            .header(H_CHUNK_STATE, "completed")
            .header(H_CHUNK_SIZE, chunk_size)
            .header("accept-ranges", "bytes")
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    async fn handle_get_chunk(
        &self,
        obj_id: &ObjId,
        req: &http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let chunk_id = ChunkId::from_obj_id(obj_id);
        let offset = parse_range_offset(req);

        let (reader, total_size) = self.store_mgr.open_chunk_reader(&chunk_id, offset).await?;

        let remaining = total_size - offset;

        // 流式响应：把 ChunkReader (AsyncRead) 转为 http body stream，
        // 逐块读取，不全量缓冲。
        let stream =
            tokio_stream::wrappers::ReceiverStream::new(chunk_reader_to_channel(reader, remaining));
        let stream_body = StreamBody::new(stream);
        let boxed_body: BoxBody<Bytes, ServerError> = BodyExt::boxed(stream_body);

        if offset == 0 {
            ok_response_builder()
                .header("content-type", CONTENT_TYPE_OCTET)
                .header("content-length", remaining)
                .header(H_CHUNK_SIZE, total_size)
                .header("accept-ranges", "bytes")
                .body(boxed_body)
                .map_err(|e| NdnError::Internal(format!("build response: {e}")))
        } else {
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-type", CONTENT_TYPE_OCTET)
                .header(
                    "content-range",
                    format!("bytes {}-{}/{}", offset, total_size - 1, total_size),
                )
                .header("content-length", remaining)
                .header(H_CHUNK_SIZE, total_size)
                .body(boxed_body)
                .map_err(|e| NdnError::Internal(format!("build response: {e}")))
        }
    }

    async fn handle_put_chunk(
        &self,
        obj_id: &ObjId,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let chunk_id = ChunkId::from_obj_id(obj_id);

        // Reject Range header on PUT
        if req.headers().contains_key("range") || req.headers().contains_key("content-range") {
            return Err(NdnError::InvalidParam(
                "Range/Content-Range not allowed on PUT chunk".to_string(),
            ));
        }

        // Parse chunk size from X-CYFS-Chunk-Size or Content-Length
        let chunk_size = parse_chunk_size(&req)?;

        if self.store_mgr.have_chunk(&chunk_id).await {
            return ok_response_builder()
                .header(H_CHUNK_ALREADY, "1")
                .header(H_CHUNK_SIZE, chunk_size)
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}")));
        }

        let outcome = self
            .store_mgr
            .put_chunk_by_reader(&chunk_id, chunk_size, request_body_into_chunk_reader(req))
            .await?;

        match outcome {
            crate::ChunkWriteOutcome::Written => Response::builder()
                .status(StatusCode::CREATED)
                .header(H_CHUNK_SIZE, chunk_size)
                .header(H_OBJ_ID, obj_id.to_string())
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}"))),
            crate::ChunkWriteOutcome::AlreadyExists => ok_response_builder()
                .header(H_CHUNK_ALREADY, "1")
                .header(H_CHUNK_SIZE, chunk_size)
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}"))),
        }
    }

    async fn handle_delete_chunk(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let chunk_id = ChunkId::from_obj_id(obj_id);
        self.store_mgr.remove_chunk(&chunk_id).await?;
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    // ======================== GC control-plane ========================

    /// Strip the `/_gc/` prefix from a path. Returns the sub-path after `/_gc/`
    /// (e.g. `"edge"`, `"pin"`, `"outbox_count"`), or `None` if not a GC path.
    fn strip_gc_prefix(path: &str) -> Option<String> {
        // Normalize: find "/_gc/" in the path (may have a prefix like "/ndn/_gc/")
        let idx = path.find("/_gc/")?;
        let sub = &path[idx + 5..]; // skip "/_gc/"
        if sub.is_empty() {
            None
        } else {
            Some(sub.to_string())
        }
    }

    async fn route_gc_request(
        &self,
        sub_path: String,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        if !self.gc_enabled {
            return Err(NdnError::Unsupported(
                "gc control-plane is disabled on this deployment".to_string(),
            ));
        }

        let method = req.method().clone();

        match (method, sub_path.as_str()) {
            (Method::POST, "edge") => self.handle_gc_apply_edge(req).await,
            (Method::POST, "pin") => self.handle_gc_pin(req).await,
            (Method::POST, "unpin") => self.handle_gc_unpin(req).await,
            (Method::POST, "unpin_owner") => self.handle_gc_unpin_owner(req).await,
            (Method::POST, "fs_acquire") => self.handle_gc_fs_acquire(req).await,
            (Method::POST, "fs_release") => self.handle_gc_fs_release(req).await,
            (Method::POST, "fs_release_inode") => self.handle_gc_fs_release_inode(req).await,
            (Method::POST, "same_as") => self.handle_gc_same_as(req).await,
            (Method::POST, "forced_gc") => self.handle_gc_forced_gc(req).await,
            (Method::GET, "outbox_count") => self.handle_gc_outbox_count().await,
            (m, sub) => {
                // GET /_gc/expand_state/{obj_id}
                if m == Method::GET && sub.starts_with("expand_state/") {
                    let obj_id_str = &sub["expand_state/".len()..];
                    let obj_id = ObjId::new(obj_id_str)?;
                    return self.handle_gc_expand_state(&obj_id).await;
                }
                // GET /_gc/anchor_state/{obj_id}?owner=...
                if m == Method::GET && sub.starts_with("anchor_state/") {
                    return self.handle_gc_anchor_state(sub, &req).await;
                }
                // GET /_gc/fs_anchor_state/{obj_id}?inode_id=N&field_tag=N
                if m == Method::GET && sub.starts_with("fs_anchor_state/") {
                    return self.handle_gc_fs_anchor_state(sub, &req).await;
                }
                Err(NdnError::Unsupported(format!("{} /_gc/{}", m, sub)))
            }
        }
    }

    async fn handle_gc_apply_edge(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let msg: EdgeMsg = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid EdgeMsg JSON: {e}")))?;
        self.store_mgr.apply_edge(msg).await?;
        no_content_response()
    }

    async fn handle_gc_pin(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let pin_req: PinRequest = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid PinRequest JSON: {e}")))?;
        self.store_mgr
            .pin(
                &pin_req.obj_id,
                &pin_req.owner,
                pin_req.scope,
                pin_req.ttl(),
            )
            .await?;
        no_content_response()
    }

    async fn handle_gc_unpin(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let obj_id_str = v["obj_id"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidParam("missing obj_id".to_string()))?;
        let owner = v["owner"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidParam("missing owner".to_string()))?;
        let obj_id = ObjId::new(obj_id_str)?;
        self.store_mgr.unpin(&obj_id, owner).await?;
        no_content_response()
    }

    async fn handle_gc_unpin_owner(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let owner = v["owner"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidParam("missing owner".to_string()))?;
        let count = self.store_mgr.unpin_owner(owner).await?;
        json_response(&serde_json::json!({ "count": count }))
    }

    async fn handle_gc_fs_acquire(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let obj_id = ObjId::new(
            v["obj_id"]
                .as_str()
                .ok_or_else(|| NdnError::InvalidParam("missing obj_id".to_string()))?,
        )?;
        let inode_id = v["inode_id"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing inode_id".to_string()))?;
        let field_tag = v["field_tag"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing field_tag".to_string()))?
            as u32;
        self.store_mgr
            .fs_acquire(&obj_id, inode_id, field_tag)
            .await?;
        no_content_response()
    }

    async fn handle_gc_fs_release(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let obj_id = ObjId::new(
            v["obj_id"]
                .as_str()
                .ok_or_else(|| NdnError::InvalidParam("missing obj_id".to_string()))?,
        )?;
        let inode_id = v["inode_id"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing inode_id".to_string()))?;
        let field_tag = v["field_tag"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing field_tag".to_string()))?
            as u32;
        self.store_mgr
            .fs_release(&obj_id, inode_id, field_tag)
            .await?;
        no_content_response()
    }

    async fn handle_gc_outbox_count(
        &self,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let count = self.store_mgr.outbox_count().await?;
        json_response(&serde_json::json!({ "count": count }))
    }

    async fn handle_gc_expand_state(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let state = self.store_mgr.debug_dump_expand_state(obj_id).await?;
        let body_str = serde_json::to_string(&state)
            .map_err(|e| NdnError::Internal(format!("serialize ExpandDebug: {e}")))?;
        ok_response_builder()
            .header("content-type", "application/json; charset=utf-8")
            .body(full_body(Bytes::from(body_str)))
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    async fn handle_gc_anchor_state(
        &self,
        sub_path: &str,
        req: &http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        // sub_path = "anchor_state/{obj_id}" or "anchor_state/{obj_id}?owner=..."
        let after_prefix = &sub_path["anchor_state/".len()..];
        let (obj_id_str, query_str) = match after_prefix.find('?') {
            Some(i) => (&after_prefix[..i], &after_prefix[i + 1..]),
            None => {
                // Check URI query string
                let q = req.uri().query().unwrap_or("");
                (after_prefix, q)
            }
        };
        let obj_id = ObjId::new(obj_id_str)?;
        let owner = query_str
            .split('&')
            .find_map(|kv| {
                let mut parts = kv.splitn(2, '=');
                if parts.next()? == "owner" {
                    parts.next()
                } else {
                    None
                }
            })
            .ok_or_else(|| NdnError::InvalidParam("missing owner query param".to_string()))?;
        let state = self.store_mgr.anchor_state(&obj_id, owner).await?;
        json_response(&serde_json::json!({ "state": state }))
    }

    async fn handle_gc_fs_anchor_state(
        &self,
        sub_path: &str,
        req: &http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        // sub_path = "fs_anchor_state/{obj_id}" or "fs_anchor_state/{obj_id}?inode_id=N&field_tag=N"
        let after_prefix = &sub_path["fs_anchor_state/".len()..];
        let (obj_id_str, query_str) = match after_prefix.find('?') {
            Some(i) => (&after_prefix[..i], &after_prefix[i + 1..]),
            None => (after_prefix, req.uri().query().unwrap_or("")),
        };
        let obj_id = ObjId::new(obj_id_str)?;

        let parse_query_param = |key: &str| -> Option<u64> {
            query_str.split('&').find_map(|kv| {
                let mut parts = kv.splitn(2, '=');
                if parts.next()? == key {
                    parts.next()?.parse().ok()
                } else {
                    None
                }
            })
        };

        let inode_id = parse_query_param("inode_id")
            .ok_or_else(|| NdnError::InvalidParam("missing inode_id query param".to_string()))?;
        let field_tag = parse_query_param("field_tag")
            .ok_or_else(|| NdnError::InvalidParam("missing field_tag query param".to_string()))?
            as u32;

        let state = self
            .store_mgr
            .fs_anchor_state(&obj_id, inode_id, field_tag)
            .await?;
        json_response(&serde_json::json!({ "state": state }))
    }

    async fn handle_gc_fs_release_inode(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let inode_id = v["inode_id"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing inode_id".to_string()))?;
        let count = self.store_mgr.fs_release_inode(inode_id).await?;
        json_response(&serde_json::json!({ "count": count }))
    }

    async fn handle_gc_same_as(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let big_chunk_id_str = v["big_chunk_id"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidParam("missing big_chunk_id".to_string()))?;
        let chunk_list_id_str = v["chunk_list_id"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidParam("missing chunk_list_id".to_string()))?;

        let big_chunk_obj_id = ObjId::new(big_chunk_id_str)?;
        let big_chunk_id = ChunkId::from_obj_id(&big_chunk_obj_id);
        let chunk_list_id = ObjId::new(chunk_list_id_str)?;

        // Validate chunk_list_id exists as an object in the store
        let chunk_list_str = self.store_mgr.get_object(&chunk_list_id).await?;

        // Parse chunk list to compute big_chunk_size
        let obj_json = ndn_lib::load_named_object_from_obj_str(&chunk_list_str)?;
        let chunk_list = ndn_lib::SimpleChunkList::from_json_value(obj_json)
            .map_err(|e| NdnError::InvalidData(format!("invalid chunk list: {e}")))?;
        let big_chunk_size = chunk_list.total_size;

        self.store_mgr
            .add_chunk_by_same_as(&big_chunk_id, big_chunk_size, &chunk_list_id)
            .await?;
        no_content_response()
    }

    async fn handle_gc_forced_gc(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let body = collect_body(req).await?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
        let target_bytes = v["target_bytes"]
            .as_u64()
            .ok_or_else(|| NdnError::InvalidParam("missing target_bytes".to_string()))?;
        let freed_bytes = self.store_mgr.forced_gc_until(target_bytes).await?;
        json_response(&serde_json::json!({ "freed_bytes": freed_bytes }))
    }
}

// ======================== Streaming helpers ========================

/// 把 ChunkReader (AsyncRead) 转成一个 tokio mpsc channel，
/// 后台 task 逐块读取并发送 Frame<Bytes>，HTTP body 消费端零拷贝流式输出。
fn chunk_reader_to_channel(
    mut reader: ndn_lib::ChunkReader,
    total: u64,
) -> tokio::sync::mpsc::Receiver<Result<http_body::Frame<Bytes>, ServerError>> {
    use http_body::Frame;
    use tokio::io::AsyncReadExt;

    // channel buffer 2 帧，保证读和发之间有一点并行度但不堆积太多内存。
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, ServerError>>(2);

    tokio::spawn(async move {
        let mut sent: u64 = 0;
        loop {
            let to_read = std::cmp::min(STREAM_BUF_SIZE as u64, total - sent) as usize;
            if to_read == 0 {
                break;
            }
            let mut buf = vec![0u8; to_read];
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.truncate(n);
                    sent += n as u64;
                    if tx.send(Ok(Frame::data(Bytes::from(buf)))).await.is_err() {
                        break; // receiver dropped
                    }
                }
                Err(_) => break,
            }
        }
        // tx drops here → stream ends
    });

    rx
}

// ======================== Helpers ========================

fn ok_response_builder() -> http::response::Builder {
    Response::builder().status(StatusCode::OK)
}

fn empty_body() -> BoxBody<Bytes, ServerError> {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(data: Bytes) -> BoxBody<Bytes, ServerError> {
    Full::new(data).map_err(|never| match never {}).boxed()
}

async fn collect_body(
    req: http::Request<BoxBody<Bytes, ServerError>>,
) -> Result<Vec<u8>, NdnError> {
    let collected = req
        .into_body()
        .collect()
        .await
        .map_err(|e| NdnError::IoError(format!("read request body: {e}")))?;
    Ok(collected.to_bytes().to_vec())
}

fn request_body_into_chunk_reader(
    req: http::Request<BoxBody<Bytes, ServerError>>,
) -> ndn_lib::ChunkReader {
    use futures_util::StreamExt;

    let stream = req
        .into_body()
        .into_data_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let reader = tokio_util::io::StreamReader::new(stream);
    Box::pin(reader)
}

fn parse_chunk_size(req: &http::Request<BoxBody<Bytes, ServerError>>) -> Result<u64, NdnError> {
    if let Some(val) = req.headers().get(H_CHUNK_SIZE) {
        return val
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| NdnError::InvalidParam("invalid X-CYFS-Chunk-Size".to_string()));
    }
    if let Some(val) = req.headers().get("content-length") {
        return val
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| NdnError::InvalidParam("invalid content-length".to_string()));
    }
    Err(NdnError::InvalidParam(
        "missing X-CYFS-Chunk-Size and Content-Length".to_string(),
    ))
}

/// Parse `Range: bytes=N-` header to extract offset. Returns 0 if absent.
fn parse_range_offset(req: &http::Request<BoxBody<Bytes, ServerError>>) -> u64 {
    let Some(val) = req.headers().get("range") else {
        return 0;
    };
    let Ok(s) = val.to_str() else {
        return 0;
    };
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("bytes=") {
        if let Some(start_str) = rest.split('-').next() {
            if let Ok(n) = start_str.parse::<u64>() {
                return n;
            }
        }
    }
    0
}

fn ndn_error_to_status(e: &NdnError) -> (StatusCode, String) {
    match e {
        NdnError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found".to_string()),
        NdnError::InvalidObjType(_) => (StatusCode::BAD_REQUEST, "invalid_obj_type".to_string()),
        NdnError::InvalidParam(_) => (StatusCode::BAD_REQUEST, "invalid_param".to_string()),
        NdnError::InvalidData(_) => (StatusCode::BAD_REQUEST, "invalid_data".to_string()),
        NdnError::InvalidId(_) => (StatusCode::BAD_REQUEST, "invalid_id".to_string()),
        NdnError::VerifyError(_) => (StatusCode::CONFLICT, "verify_failed".to_string()),
        NdnError::PermissionDenied(_) => (StatusCode::FORBIDDEN, "permission_denied".to_string()),
        NdnError::AlreadyExists(_) => (StatusCode::CONFLICT, "already_exists".to_string()),
        NdnError::OffsetTooLarge(_) => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            "offset_too_large".to_string(),
        ),
        NdnError::Unsupported(_) => (StatusCode::METHOD_NOT_ALLOWED, "unsupported".to_string()),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error".to_string(),
        ),
    }
}

fn no_content_response() -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(empty_body())
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

fn json_response(
    value: &serde_json::Value,
) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    let body_str = serde_json::to_string(value)
        .map_err(|e| NdnError::Internal(format!("serialize JSON: {e}")))?;
    ok_response_builder()
        .header("content-type", "application/json; charset=utf-8")
        .body(full_body(Bytes::from(body_str)))
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

fn build_error_response(
    status: StatusCode,
    error_code: &str,
    message: &str,
) -> http::Response<BoxBody<Bytes, ServerError>> {
    let body = serde_json::json!({
        "error": error_code,
        "message": message,
    })
    .to_string();

    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(empty_body())
                .unwrap()
        })
}
