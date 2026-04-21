//! `NamedDataMgrNodeGateway` —— NDM Proxy 协议的服务端实现。
//!
//! 协议详情见 `doc/NDM Protocol/named-data-mgr-proxy-protocol.md`。
//!
//! 典型调用链：`Zone App -> ndm_client -> NDM Proxy -> NamedDataMgr`。
//!
//! 本 gateway 负责把 HTTP 请求转换为对 `NamedDataMgr` 的本地调用：
//! - `/ndm/proxy/v1/rpc/{method}` —— JSON RPC 控制面
//! - `/ndm/proxy/v1/read/*` —— 二进制流式读取
//! - `/ndm/proxy/v1/write/*` —— 原子一次写入

use async_trait::async_trait;
use bytes::Bytes;
use cyfs_gateway_lib::{server_err, HttpServer, ServerError, ServerErrorCode, ServerResult, StreamInfo};
use futures_util::StreamExt;
use http::{Method, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use log::{info, warn};
use ndn_lib::{ChunkId, ChunkReader, NdnError, ObjId};
use serde::Deserialize;
use std::sync::Arc;

use crate::gc_types::{EdgeMsg, PinRequest};
use crate::named_store::ObjectState;
use crate::ndm::NamedDataMgr;
use crate::store_db::ChunkStoreState;
use crate::ChunkWriteOutcome;

// ======================== Protocol Constants ========================

const PATH_PREFIX: &str = "/ndm/proxy/v1";
const RPC_PREFIX: &str = "/ndm/proxy/v1/rpc/";
const READ_PREFIX: &str = "/ndm/proxy/v1/read/";
const WRITE_CHUNK_PREFIX: &str = "/ndm/proxy/v1/write/chunk/";

const CONTENT_TYPE_JSON: &str = "application/json; charset=utf-8";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";

const H_TOTAL_SIZE: &str = "ndm-total-size";
const H_OFFSET: &str = "ndm-offset";
const H_READER_KIND: &str = "ndm-reader-kind";
const H_RESOLVED_OBJ_ID: &str = "ndm-resolved-object-id";
const H_CHUNK_SIZE: &str = "ndm-chunk-size";
const H_WRITE_OUTCOME: &str = "ndm-chunk-write-outcome";
const H_CHUNK_OBJ_ID: &str = "ndm-chunk-object-id";

const STREAM_BUF_SIZE: usize = 64 * 1024;

// ======================== Config ========================

#[derive(Debug, Clone)]
pub struct NdmNodeGatewayConfig {
    /// 受限能力开关（apply_edge / pin / unpin* / fs_* / forced_gc_until /
    /// outbox_count / debug_dump_expand_state / anchor_state）。
    ///
    /// 默认关闭，按协议 §13 所述"受限能力需要显式配置开启"。
    pub restricted_enabled: bool,
}

impl Default for NdmNodeGatewayConfig {
    fn default() -> Self {
        Self {
            restricted_enabled: false,
        }
    }
}

// ======================== Gateway ========================

#[derive(Clone)]
pub struct NamedDataMgrNodeGateway {
    store_mgr: Arc<NamedDataMgr>,
    config: NdmNodeGatewayConfig,
}

impl NamedDataMgrNodeGateway {
    pub fn new(store_mgr: Arc<NamedDataMgr>, config: NdmNodeGatewayConfig) -> Self {
        Self { store_mgr, config }
    }

    pub fn with_restricted(store_mgr: Arc<NamedDataMgr>) -> Self {
        Self::new(
            store_mgr,
            NdmNodeGatewayConfig {
                restricted_enabled: true,
            },
        )
    }
}

#[async_trait]
impl HttpServer for NamedDataMgrNodeGateway {
    async fn serve_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
        _info: StreamInfo,
    ) -> ServerResult<http::Response<BoxBody<Bytes, ServerError>>> {
        let result = self.route_request(req).await;
        match result {
            Ok(resp) => {
                info!("ndm-node-gateway served request {}", resp.status());
                Ok(resp)
            }
            Err(e) => {
                let (status, error_code) = ndm_error_to_status(&e);
                warn!("ndm-node-gateway request failed: {} -> {}", status, e);
                Ok(build_error_response(status, &error_code, &e.to_string()))
            }
        }
    }

    fn id(&self) -> String {
        "ndm-node-gateway".to_string()
    }

    fn http_version(&self) -> Version {
        Version::HTTP_11
    }

    fn http3_port(&self) -> Option<u16> {
        None
    }
}

// ======================== Routing ========================

impl NamedDataMgrNodeGateway {
    async fn route_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
            .split('?')
            .next()
            .unwrap_or("/")
            .to_string();
        let method = req.method().clone();

        // ---- JSON RPC ----
        if let Some(rpc_method) = path.strip_prefix(RPC_PREFIX) {
            if rpc_method.is_empty() {
                return Err(NdnError::NotFound("missing rpc method".to_string()));
            }
            if method != Method::POST {
                return Err(NdnError::Unsupported(format!(
                    "rpc only accepts POST, got {}",
                    method
                )));
            }
            return self.handle_rpc(rpc_method, req).await;
        }

        // ---- Streaming read ----
        if let Some(sub) = path.strip_prefix(READ_PREFIX) {
            if method != Method::POST {
                return Err(NdnError::Unsupported(format!(
                    "read endpoints only accept POST, got {}",
                    method
                )));
            }
            return self.handle_read(sub, req).await;
        }

        // ---- Streaming write: /write/chunk/{chunk_id} ----
        if let Some(chunk_id_str) = path.strip_prefix(WRITE_CHUNK_PREFIX) {
            if method != Method::PUT {
                return Err(NdnError::Unsupported(format!(
                    "write/chunk only accepts PUT, got {}",
                    method
                )));
            }
            return self.handle_write_chunk(chunk_id_str, req).await;
        }

        // 已知的协议前缀但没有匹配到具体路由
        if path.starts_with(PATH_PREFIX) {
            return Err(NdnError::NotFound(format!(
                "unknown ndm proxy route: {} {}",
                method, path
            )));
        }

        Err(NdnError::NotFound(format!(
            "unknown route: {} {}",
            method, path
        )))
    }
}

// ======================== JSON RPC ========================

impl NamedDataMgrNodeGateway {
    async fn handle_rpc(
        &self,
        method_name: &str,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        match method_name {
            // ---- Object ----
            "get_object" => {
                let r: ObjIdRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let obj_data = self.store_mgr.get_object(&obj_id).await?;
                json_response(&serde_json::json!({
                    "obj_id": obj_id.to_string(),
                    "obj_data": obj_data,
                }))
            }

            "open_object" => {
                let r: ObjIdWithInnerPathRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let inner_path = normalize_inner_path(r.inner_path);
                let obj_data = self.store_mgr.open_object(&obj_id, inner_path).await?;
                json_response(&serde_json::json!({ "obj_data": obj_data }))
            }

            "get_dir_child" => {
                let r: GetDirChildRequest = parse_json_body(req).await?;
                let dir_obj_id = ObjId::new(&r.dir_obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid dir_obj_id: {e}")))?;
                let child = self
                    .store_mgr
                    .get_dir_child(&dir_obj_id, &r.item_name)
                    .await?;
                json_response(&serde_json::json!({ "obj_id": child.to_string() }))
            }

            "is_object_stored" => {
                let r: ObjIdWithInnerPathRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let inner_path = normalize_inner_path(r.inner_path);
                let stored = self.store_mgr.is_object_stored(&obj_id, inner_path).await?;
                json_response(&serde_json::json!({ "stored": stored }))
            }

            "is_object_exist" => {
                let r: ObjIdRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let exists = self.store_mgr.is_object_exist(&obj_id).await?;
                json_response(&serde_json::json!({ "exists": exists }))
            }

            "query_object_by_id" => {
                let r: ObjIdRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let state = self.store_mgr.query_object_by_id(&obj_id).await?;
                json_response(&object_state_to_json(state))
            }

            "put_object" => {
                let r: PutObjectRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                if obj_id.is_chunk() {
                    return Err(NdnError::InvalidParam(
                        "put_object does not accept chunk ids; use write/chunk instead".to_string(),
                    ));
                }
                self.store_mgr.put_object(&obj_id, &r.obj_data).await?;
                no_content_response()
            }

            "remove_object" => {
                let r: ObjIdRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                if obj_id.is_chunk() {
                    return Err(NdnError::InvalidParam(
                        "remove_object does not accept chunk ids; use remove_chunk instead"
                            .to_string(),
                    ));
                }
                self.store_mgr.remove_object(&obj_id).await?;
                no_content_response()
            }

            // ---- Chunk metadata ----
            "have_chunk" => {
                let r: ChunkIdRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let exists = self.store_mgr.have_chunk(&chunk_id).await;
                json_response(&serde_json::json!({ "exists": exists }))
            }

            "query_chunk_state" => {
                let r: ChunkIdRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let (state, size) = self.store_mgr.query_chunk_state(&chunk_id).await?;
                json_response(&chunk_store_state_to_json(state, size))
            }

            "remove_chunk" => {
                let r: ChunkIdRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                self.store_mgr.remove_chunk(&chunk_id).await?;
                no_content_response()
            }

            "add_chunk_by_same_as" => {
                let r: AddChunkBySameAsRequest = parse_json_body(req).await?;
                let big_chunk_id = ChunkId::new(&r.big_chunk_id)?;
                let chunk_list_id = ObjId::new(&r.chunk_list_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid chunk_list_id: {e}")))?;
                self.store_mgr
                    .add_chunk_by_same_as(&big_chunk_id, r.big_chunk_size, &chunk_list_id)
                    .await?;
                no_content_response()
            }

            // ---- GC / Anchor / Debug (restricted) ----
            "apply_edge" => {
                self.ensure_restricted(method_name)?;
                let msg: EdgeMsg = parse_json_body(req).await?;
                self.store_mgr.apply_edge(msg).await?;
                no_content_response()
            }

            "pin" => {
                self.ensure_restricted(method_name)?;
                let pin_req: PinRequest = parse_json_body(req).await?;
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

            "unpin" => {
                self.ensure_restricted(method_name)?;
                let r: UnpinRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                self.store_mgr.unpin(&obj_id, &r.owner).await?;
                no_content_response()
            }

            "unpin_owner" => {
                self.ensure_restricted(method_name)?;
                let r: OwnerRequest = parse_json_body(req).await?;
                let count = self.store_mgr.unpin_owner(&r.owner).await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "fs_acquire" => {
                self.ensure_restricted(method_name)?;
                let r: FsAnchorRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                self.store_mgr
                    .fs_acquire(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                no_content_response()
            }

            "fs_release" => {
                self.ensure_restricted(method_name)?;
                let r: FsAnchorRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                self.store_mgr
                    .fs_release(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                no_content_response()
            }

            "fs_release_inode" => {
                self.ensure_restricted(method_name)?;
                let r: InodeRequest = parse_json_body(req).await?;
                let count = self.store_mgr.fs_release_inode(r.inode_id).await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "fs_anchor_state" => {
                self.ensure_restricted(method_name)?;
                let r: FsAnchorRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let state = self
                    .store_mgr
                    .fs_anchor_state(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                json_response(&serde_json::json!({ "state": state.as_str() }))
            }

            "forced_gc_until" => {
                self.ensure_restricted(method_name)?;
                let r: ForcedGcRequest = parse_json_body(req).await?;
                let freed_bytes = self.store_mgr.forced_gc_until(r.target_bytes).await?;
                json_response(&serde_json::json!({ "freed_bytes": freed_bytes }))
            }

            "outbox_count" => {
                self.ensure_restricted(method_name)?;
                drain_body(req).await?;
                let count = self.store_mgr.outbox_count().await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "debug_dump_expand_state" => {
                self.ensure_restricted(method_name)?;
                let r: ObjIdRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let debug = self.store_mgr.debug_dump_expand_state(&obj_id).await?;
                let v = serde_json::to_value(&debug)
                    .map_err(|e| NdnError::Internal(format!("serialize ExpandDebug: {e}")))?;
                json_response(&v)
            }

            "anchor_state" => {
                self.ensure_restricted(method_name)?;
                let r: AnchorStateRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let state = self.store_mgr.anchor_state(&obj_id, &r.owner).await?;
                json_response(&serde_json::json!({ "state": state.as_str() }))
            }

            _ => Err(NdnError::NotFound(format!(
                "unknown rpc method: {}",
                method_name
            ))),
        }
    }

    fn ensure_restricted(&self, method_name: &str) -> Result<(), NdnError> {
        if self.config.restricted_enabled {
            Ok(())
        } else {
            Err(NdnError::PermissionDenied(format!(
                "restricted operation '{}' is disabled on this gateway",
                method_name
            )))
        }
    }
}

// ======================== Streaming read ========================

impl NamedDataMgrNodeGateway {
    async fn handle_read(
        &self,
        sub: &str,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        match sub {
            "chunk/open" => {
                let r: ChunkOpenRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let offset = r.offset.unwrap_or(0);
                let (reader, total_size) =
                    self.store_mgr.open_chunk_reader(&chunk_id, offset).await?;
                if offset > total_size {
                    return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
                }
                let remaining = total_size - offset;
                let body = chunk_reader_to_body(reader, remaining);
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", CONTENT_TYPE_OCTET)
                    .header("content-length", remaining)
                    .header(H_TOTAL_SIZE, total_size)
                    .header(H_READER_KIND, "chunk");
                if offset > 0 {
                    builder = builder.header(H_OFFSET, offset);
                }
                builder
                    .body(body)
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }

            "chunk/data" => {
                let r: ChunkIdRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let (reader, total_size) = self.store_mgr.open_chunk_reader(&chunk_id, 0).await?;
                let body = chunk_reader_to_body(reader, total_size);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", CONTENT_TYPE_OCTET)
                    .header("content-length", total_size)
                    .header(H_TOTAL_SIZE, total_size)
                    .header(H_READER_KIND, "chunk")
                    .body(body)
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }

            "chunk/piece" => {
                let r: ChunkPieceRequest = parse_json_body(req).await?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                // 协议 §11.1 要求所有读接口返回 NDM-Total-Size，先取一次状态拿到逻辑总长度。
                let (state, total_size) = self.store_mgr.query_chunk_state(&chunk_id).await?;
                if matches!(state, ChunkStoreState::NotExist) {
                    return Err(NdnError::NotFound(format!(
                        "chunk {} not found",
                        chunk_id.to_string()
                    )));
                }
                let piece = self
                    .store_mgr
                    .get_chunk_piece(&chunk_id, r.offset, r.piece_size)
                    .await?;
                let piece_len = piece.len() as u64;
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", CONTENT_TYPE_OCTET)
                    .header("content-length", piece_len)
                    .header(H_TOTAL_SIZE, total_size)
                    .header(H_READER_KIND, "chunk");
                if r.offset > 0 {
                    builder = builder.header(H_OFFSET, r.offset);
                }
                builder
                    .body(full_body(Bytes::from(piece)))
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }

            "chunklist/open" => {
                let r: ChunkListOpenRequest = parse_json_body(req).await?;
                let chunk_list_id = ObjId::new(&r.chunk_list_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid chunk_list_id: {e}")))?;
                let offset = r.offset.unwrap_or(0);
                let (reader, total_size) = self
                    .store_mgr
                    .open_chunklist_reader(&chunk_list_id, offset)
                    .await?;
                if offset > total_size {
                    return Err(NdnError::OffsetTooLarge(chunk_list_id.to_string()));
                }
                let remaining = total_size - offset;
                let body = chunk_reader_to_body(reader, remaining);
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", CONTENT_TYPE_OCTET)
                    .header("content-length", remaining)
                    .header(H_TOTAL_SIZE, total_size)
                    .header(H_READER_KIND, "chunklist")
                    .header(H_RESOLVED_OBJ_ID, chunk_list_id.to_string());
                if offset > 0 {
                    builder = builder.header(H_OFFSET, offset);
                }
                builder
                    .body(body)
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }

            "object/open" => {
                let r: ObjIdWithInnerPathRequest = parse_json_body(req).await?;
                let obj_id = parse_obj_id(&r.obj_id)?;
                let inner_path = normalize_inner_path(r.inner_path);
                // 协议 §11.6：NDM-Resolved-Object-ID 必须是最终落地 reader 的对象 ID
                // （chunk 或 chunklist），而不是输入的入口 obj_id。
                let (reader, total_size, resolved_obj_id) = self
                    .store_mgr
                    .open_reader_with_resolved(&obj_id, inner_path)
                    .await?;
                let kind = if resolved_obj_id.is_chunk() {
                    "chunk"
                } else if resolved_obj_id.is_chunk_list() {
                    "chunklist"
                } else {
                    "object"
                };
                let body = chunk_reader_to_body(reader, total_size);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", CONTENT_TYPE_OCTET)
                    .header("content-length", total_size)
                    .header(H_TOTAL_SIZE, total_size)
                    .header(H_READER_KIND, kind)
                    .header(H_RESOLVED_OBJ_ID, resolved_obj_id.to_string())
                    .body(body)
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }

            _ => Err(NdnError::NotFound(format!("unknown read route: {}", sub))),
        }
    }
}

// ======================== Streaming write ========================

impl NamedDataMgrNodeGateway {
    async fn handle_write_chunk(
        &self,
        chunk_id_str: &str,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let chunk_id_str = chunk_id_str.trim_end_matches('/');
        if chunk_id_str.is_empty() {
            return Err(NdnError::InvalidParam(
                "missing chunk_id in path".to_string(),
            ));
        }
        let chunk_id = ChunkId::new(chunk_id_str)?;

        // 禁止 Range 语义 —— 这是原子一次写入，不是断点续传
        if req.headers().contains_key("range") || req.headers().contains_key("content-range") {
            return Err(NdnError::InvalidParam(
                "Range/Content-Range not allowed on write/chunk".to_string(),
            ));
        }

        // 协议 §12.1：Content-Type、Content-Length、NDM-Chunk-Size 都是必填，
        // 且 Content-Length 必须与 chunk_size 一致。
        let content_type = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                NdnError::InvalidParam(
                    "missing Content-Type header (required: application/octet-stream)".to_string(),
                )
            })?;
        // 严格匹配：不接受 text/plain、multipart 等；允许后续可能的参数（如 charset）但主类型必须一致。
        let main_type = content_type.split(';').next().unwrap_or("").trim();
        if main_type != CONTENT_TYPE_OCTET {
            return Err(NdnError::InvalidParam(format!(
                "write/chunk requires Content-Type: {}, got: {}",
                CONTENT_TYPE_OCTET, content_type
            )));
        }

        let content_length = parse_required_u64_header(&req, "content-length")?;
        let ndm_chunk_size = parse_required_u64_header(&req, H_CHUNK_SIZE)?;

        if content_length != ndm_chunk_size {
            return Err(NdnError::InvalidParam(format!(
                "Content-Length ({}) does not match {} ({})",
                content_length, H_CHUNK_SIZE, ndm_chunk_size
            )));
        }
        let chunk_size = ndm_chunk_size;

        // 若 chunk 已存在，短路返回 already_exists，避免重复读取请求体
        if self.store_mgr.have_chunk(&chunk_id).await {
            return Response::builder()
                .status(StatusCode::OK)
                .header(H_CHUNK_SIZE, chunk_size)
                .header(H_WRITE_OUTCOME, "already_exists")
                .header(H_CHUNK_OBJ_ID, chunk_id.to_obj_id().to_string())
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}")));
        }

        let body_reader = request_body_into_chunk_reader(req);
        let outcome = self
            .store_mgr
            .put_chunk_by_reader(&chunk_id, chunk_size, body_reader)
            .await?;

        match outcome {
            ChunkWriteOutcome::Written => Response::builder()
                .status(StatusCode::CREATED)
                .header(H_CHUNK_SIZE, chunk_size)
                .header(H_WRITE_OUTCOME, "written")
                .header(H_CHUNK_OBJ_ID, chunk_id.to_obj_id().to_string())
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}"))),
            ChunkWriteOutcome::AlreadyExists => Response::builder()
                .status(StatusCode::OK)
                .header(H_CHUNK_SIZE, chunk_size)
                .header(H_WRITE_OUTCOME, "already_exists")
                .header(H_CHUNK_OBJ_ID, chunk_id.to_obj_id().to_string())
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}"))),
        }
    }
}

// ======================== Request bodies ========================

#[derive(Deserialize)]
struct ObjIdRequest {
    obj_id: String,
}

#[derive(Deserialize)]
struct ObjIdWithInnerPathRequest {
    obj_id: String,
    #[serde(default)]
    inner_path: Option<String>,
}

#[derive(Deserialize)]
struct GetDirChildRequest {
    dir_obj_id: String,
    item_name: String,
}

#[derive(Deserialize)]
struct PutObjectRequest {
    obj_id: String,
    obj_data: String,
}

#[derive(Deserialize)]
struct ChunkIdRequest {
    chunk_id: String,
}

#[derive(Deserialize)]
struct ChunkOpenRequest {
    chunk_id: String,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Deserialize)]
struct ChunkPieceRequest {
    chunk_id: String,
    #[serde(default)]
    offset: u64,
    piece_size: u32,
}

#[derive(Deserialize)]
struct ChunkListOpenRequest {
    chunk_list_id: String,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Deserialize)]
struct AddChunkBySameAsRequest {
    big_chunk_id: String,
    chunk_list_id: String,
    big_chunk_size: u64,
}

#[derive(Deserialize)]
struct UnpinRequest {
    obj_id: String,
    owner: String,
}

#[derive(Deserialize)]
struct OwnerRequest {
    owner: String,
}

#[derive(Deserialize)]
struct FsAnchorRequest {
    obj_id: String,
    inode_id: u64,
    field_tag: u32,
}

#[derive(Deserialize)]
struct InodeRequest {
    inode_id: u64,
}

#[derive(Deserialize)]
struct ForcedGcRequest {
    target_bytes: u64,
}

#[derive(Deserialize)]
struct AnchorStateRequest {
    obj_id: String,
    owner: String,
}

// ======================== JSON encode helpers ========================

fn normalize_inner_path(p: Option<String>) -> Option<String> {
    match p.as_deref() {
        None | Some("") | Some("/") => None,
        _ => p,
    }
}

fn parse_obj_id(s: &str) -> Result<ObjId, NdnError> {
    ObjId::new(s).map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))
}

fn object_state_to_json(state: ObjectState) -> serde_json::Value {
    match state {
        ObjectState::NotExist => serde_json::json!({ "state": "not_exist" }),
        ObjectState::Object(data) => serde_json::json!({ "state": "object", "obj_data": data }),
    }
}

fn chunk_store_state_to_json(state: ChunkStoreState, chunk_size: u64) -> serde_json::Value {
    match state {
        ChunkStoreState::New => serde_json::json!({
            "state": "new",
            "chunk_size": chunk_size,
        }),
        ChunkStoreState::Completed => serde_json::json!({
            "state": "completed",
            "chunk_size": chunk_size,
        }),
        ChunkStoreState::Disabled => serde_json::json!({
            "state": "disabled",
            "chunk_size": chunk_size,
        }),
        ChunkStoreState::NotExist => serde_json::json!({
            "state": "not_exist",
            "chunk_size": chunk_size,
        }),
        ChunkStoreState::LocalLink(info) => {
            let mut v = serde_json::json!({
                "state": "local_link",
                "chunk_size": chunk_size,
                "local_info": {
                    "qcid": info.qcid,
                    "last_modify_time": info.last_modify_time,
                },
            });
            if let Some(range) = info.range {
                v["local_info"]["range"] = serde_json::json!({
                    "start": range.start,
                    "end": range.end,
                });
            }
            v
        }
        ChunkStoreState::SameAs(obj_id) => serde_json::json!({
            "state": "same_as",
            "chunk_size": chunk_size,
            "same_as": obj_id.to_string(),
        }),
    }
}

// ======================== HTTP body helpers ========================

fn empty_body() -> BoxBody<Bytes, ServerError> {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(data: Bytes) -> BoxBody<Bytes, ServerError> {
    Full::new(data).map_err(|never| match never {}).boxed()
}

async fn parse_json_body<T: serde::de::DeserializeOwned>(
    req: http::Request<BoxBody<Bytes, ServerError>>,
) -> Result<T, NdnError> {
    let body = collect_body(req).await?;
    if body.is_empty() {
        // 允许空 body（如 outbox_count 的 `{}`），这里把空 body 当作 `null` 处理
        // —— 不过普通调用方都会发 JSON，所以这分支很少触发。
        return serde_json::from_slice(b"{}")
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")));
    }
    serde_json::from_slice(&body).map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))
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

async fn drain_body(req: http::Request<BoxBody<Bytes, ServerError>>) -> Result<(), NdnError> {
    let _ = collect_body(req).await?;
    Ok(())
}

fn json_response(
    value: &serde_json::Value,
) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    let body_str = serde_json::to_string(value)
        .map_err(|e| NdnError::Internal(format!("serialize JSON: {e}")))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", CONTENT_TYPE_JSON)
        .body(full_body(Bytes::from(body_str)))
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

fn no_content_response() -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(empty_body())
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

fn parse_required_u64_header(
    req: &http::Request<BoxBody<Bytes, ServerError>>,
    name: &str,
) -> Result<u64, NdnError> {
    let value = req
        .headers()
        .get(name)
        .ok_or_else(|| NdnError::InvalidParam(format!("missing {} header", name)))?;
    value
        .to_str()
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| NdnError::InvalidParam(format!("invalid {} header value", name)))
}

// ======================== Stream adapters ========================

fn request_body_into_chunk_reader(
    req: http::Request<BoxBody<Bytes, ServerError>>,
) -> ChunkReader {
    let stream = req
        .into_body()
        .into_data_stream()
        .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    Box::pin(tokio_util::io::StreamReader::new(stream))
}

fn chunk_reader_to_body(reader: ChunkReader, total: u64) -> BoxBody<Bytes, ServerError> {
    let rx = chunk_reader_to_channel(reader, total);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    BodyExt::boxed(StreamBody::new(stream))
}

fn chunk_reader_to_channel(
    mut reader: ChunkReader,
    total: u64,
) -> tokio::sync::mpsc::Receiver<Result<http_body::Frame<Bytes>, ServerError>> {
    use http_body::Frame;
    use tokio::io::AsyncReadExt;

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, ServerError>>(2);

    tokio::spawn(async move {
        let mut sent: u64 = 0;
        while sent < total {
            let to_read = std::cmp::min(STREAM_BUF_SIZE as u64, total - sent) as usize;
            let mut buf = vec![0u8; to_read];
            match reader.read(&mut buf).await {
                Ok(0) => {
                    // 提前 EOF —— 实际读到的字节数小于声明的 total。
                    // 不能当成成功结束流：下游会拿到一个 Content-Length 与实际 body 不一致的
                    // 截断 200 OK。把它作为 body error 抛出，hyper 会中断响应连接。
                    let err = server_err!(
                        ServerErrorCode::IOError,
                        "chunk reader unexpected EOF: sent={}, expected={}",
                        sent,
                        total
                    );
                    let _ = tx.send(Err(err)).await;
                    return;
                }
                Ok(n) => {
                    buf.truncate(n);
                    sent += n as u64;
                    if tx.send(Ok(Frame::data(Bytes::from(buf)))).await.is_err() {
                        // 客户端主动断开：直接退出，不需要再报错。
                        return;
                    }
                }
                Err(e) => {
                    // 把底层 IO 错误透传给 body sink。如果发送也失败了说明客户端已经断开，
                    // 这种情况下静默退出即可。
                    let err = server_err!(
                        ServerErrorCode::IOError,
                        "chunk reader read failed at offset={}: {}",
                        sent,
                        e
                    );
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            }
        }
    });

    rx
}

// ======================== Error mapping ========================

fn ndm_error_to_status(e: &NdnError) -> (StatusCode, String) {
    match e {
        NdnError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found".to_string()),
        NdnError::InvalidParam(_) => (StatusCode::BAD_REQUEST, "invalid_param".to_string()),
        NdnError::InvalidData(_) => (StatusCode::BAD_REQUEST, "invalid_data".to_string()),
        NdnError::InvalidId(_) => (StatusCode::BAD_REQUEST, "invalid_id".to_string()),
        NdnError::InvalidObjType(_) => {
            (StatusCode::BAD_REQUEST, "invalid_obj_type".to_string())
        }
        NdnError::VerifyError(_) => (StatusCode::CONFLICT, "verify_error".to_string()),
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
        .header("content-type", CONTENT_TYPE_JSON)
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(empty_body())
                .unwrap()
        })
}
