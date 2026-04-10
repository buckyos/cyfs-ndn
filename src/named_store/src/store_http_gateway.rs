//! `NamedStoreMgrHttpGateway` —— named-data-http-store 协议的服务端实现。
//!
//! 一个机器上只有一个实例，通过 `NamedStoreMgr` 管理多个 store 桶。
//! 协议详情见 `doc/named-data-http-store-protocol.md`。

use async_trait::async_trait;
use bytes::Bytes;
use cyfs_gateway_lib::{HttpServer, ServerError, ServerResult, StreamInfo};
use http::{Method, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use log::{info, warn};
use ndn_lib::{ChunkId, NdnError, ObjId};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::store_mgr::NamedStoreMgr;

// Custom headers
const H_RESOURCE_KIND: &str = "x-cyfs-resource-kind";
const H_CHUNK_STATE: &str = "x-cyfs-chunk-state";
const H_CHUNK_SIZE: &str = "x-cyfs-chunk-size";
const H_OBJ_ID: &str = "x-cyfs-obj-id";
const H_CHUNK_ALREADY: &str = "x-cyfs-chunk-already";

const CONTENT_TYPE_OBJECT: &str = "application/cyfs-object";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";

#[derive(Clone)]
pub struct NamedStoreMgrHttpGateway {
    store_mgr: Arc<NamedStoreMgr>,
}

impl NamedStoreMgrHttpGateway {
    pub fn new(store_mgr: Arc<NamedStoreMgr>) -> Self {
        Self { store_mgr }
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
    /// Parse obj_id from the request URI path (strip the gateway prefix).
    /// Expects path like `/{prefix}/{obj_id}` — we take the last non-empty segment(s)
    /// that form a valid obj_id string like `sha256:abcd...` or `mix256:abcd...`.
    fn parse_obj_id_from_path(path: &str) -> Result<ObjId, NdnError> {
        // Find the obj_id portion: everything after the last `/` that contains a `:`
        // or use the full remaining path after stripping leading segments.
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Err(NdnError::InvalidParam("empty path".to_string()));
        }
        // The obj_id is the last segment (could be `type:hex`)
        // But obj_id format is `type:hex` which doesn't contain `/`, so last segment works.
        // However the path prefix may vary. Try from last segment backwards.
        let last = segments.last().unwrap();
        ObjId::new(last)
    }

    fn get_resource_kind(
        req: &http::Request<BoxBody<Bytes, ServerError>>,
        obj_id: &ObjId,
    ) -> ResourceKind {
        if let Some(val) = req.headers().get(H_RESOURCE_KIND) {
            if let Ok(s) = val.to_str() {
                match s.to_lowercase().as_str() {
                    "chunk" => return ResourceKind::Chunk,
                    "object" => return ResourceKind::Object,
                    _ => {}
                }
            }
        }
        // Fallback: infer from obj_id type
        if obj_id.is_chunk() {
            ResourceKind::Chunk
        } else {
            ResourceKind::Object
        }
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

        let obj_id = Self::parse_obj_id_from_path(&path)?;
        let kind = Self::get_resource_kind(&req, &obj_id);
        let method = req.method().clone();

        match (&method, kind) {
            (&Method::GET, ResourceKind::Object) => self.handle_get_object(&obj_id).await,
            (&Method::PUT, ResourceKind::Object) => self.handle_put_object(&obj_id, req).await,
            (&Method::DELETE, ResourceKind::Object) => self.handle_delete_object(&obj_id).await,
            (&Method::HEAD, ResourceKind::Chunk) => self.handle_head_chunk(&obj_id).await,
            (&Method::GET, ResourceKind::Chunk) => self.handle_get_chunk(&obj_id, &req).await,
            (&Method::PUT, ResourceKind::Chunk) => self.handle_put_chunk(&obj_id, req).await,
            (&Method::DELETE, ResourceKind::Chunk) => self.handle_delete_chunk(&obj_id).await,
            (&Method::HEAD, ResourceKind::Object) => {
                // HEAD on object: check existence
                let obj_str = self.store_mgr.get_object(&obj_id).await?;
                let body_bytes = obj_str.into_bytes();
                ok_response_builder()
                    .header("content-type", CONTENT_TYPE_OBJECT)
                    .header("content-length", body_bytes.len())
                    .header(H_OBJ_ID, obj_id.to_string())
                    .body(empty_body())
                    .map_err(|e| NdnError::Internal(format!("build response: {e}")))
            }
            _ => Err(NdnError::Unsupported(format!(
                "{} on {:?}",
                method,
                kind
            ))),
        }
    }

    // ======================== Object handlers ========================

    async fn handle_get_object(
        &self,
        obj_id: &ObjId,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(
                "use chunk endpoint for chunk ids".to_string(),
            ));
        }
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

    async fn handle_put_object(
        &self,
        obj_id: &ObjId,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(
                "use chunk endpoint for chunk ids".to_string(),
            ));
        }
        let body = collect_body(req).await?;
        let obj_str =
            String::from_utf8(body).map_err(|e| NdnError::InvalidData(format!("invalid utf8: {e}")))?;
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

        let (mut reader, total_size) =
            self.store_mgr.open_chunk_reader(&chunk_id, offset).await?;

        // Read the chunk data into memory for response.
        // For large chunks a streaming approach would be better, but the current
        // NamedStoreMgr API returns a ChunkReader (Pin<Box<dyn AsyncRead>>), which
        // we consume here. The protocol limits chunks to DEFAULT_CHUNK_SIZE anyway.
        let remaining = total_size - offset;
        let mut buf = Vec::with_capacity(remaining as usize);
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| NdnError::IoError(format!("read chunk: {e}")))?;

        let body_bytes = Bytes::from(buf);

        if offset == 0 {
            ok_response_builder()
                .header("content-type", CONTENT_TYPE_OCTET)
                .header("content-length", remaining)
                .header(H_CHUNK_SIZE, total_size)
                .header("accept-ranges", "bytes")
                .body(full_body(body_bytes))
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
                .body(full_body(body_bytes))
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

        // Parse chunk size from header or content-length
        let chunk_size = if let Some(val) = req.headers().get(H_CHUNK_SIZE) {
            val.to_str()
                .map_err(|_| NdnError::InvalidParam("invalid X-CYFS-Chunk-Size".to_string()))?
                .parse::<u64>()
                .map_err(|_| NdnError::InvalidParam("invalid X-CYFS-Chunk-Size".to_string()))?
        } else if let Some(val) = req.headers().get("content-length") {
            val.to_str()
                .map_err(|_| NdnError::InvalidParam("missing chunk size".to_string()))?
                .parse::<u64>()
                .map_err(|_| NdnError::InvalidParam("invalid content-length".to_string()))?
        } else {
            return Err(NdnError::InvalidParam(
                "missing X-CYFS-Chunk-Size and Content-Length".to_string(),
            ));
        };

        let body_data = collect_body(req).await?;
        if body_data.len() as u64 != chunk_size {
            return Err(NdnError::InvalidParam(format!(
                "body size {} != declared chunk_size {}",
                body_data.len(),
                chunk_size
            )));
        }

        // Check if chunk already exists (idempotent PUT)
        let already_exists = self.store_mgr.have_chunk(&chunk_id).await;
        if already_exists {
            return ok_response_builder()
                .header(H_CHUNK_ALREADY, "1")
                .header(H_CHUNK_SIZE, chunk_size)
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}")));
        }

        self.store_mgr
            .put_chunk_by_reader(
                &chunk_id,
                chunk_size,
                Box::pin(std::io::Cursor::new(body_data)),
            )
            .await?;

        Response::builder()
            .status(StatusCode::CREATED)
            .header(H_CHUNK_SIZE, chunk_size)
            .header(H_OBJ_ID, obj_id.to_string())
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
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
}

// ======================== Helpers ========================

#[derive(Debug, Clone, Copy)]
enum ResourceKind {
    Object,
    Chunk,
}

fn ok_response_builder() -> http::response::Builder {
    Response::builder().status(StatusCode::OK)
}

fn empty_body() -> BoxBody<Bytes, ServerError> {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(data: Bytes) -> BoxBody<Bytes, ServerError> {
    Full::new(data)
        .map_err(|never| match never {})
        .boxed()
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

/// Parse `Range: bytes=N-` header to extract offset. Returns 0 if absent.
fn parse_range_offset(req: &http::Request<BoxBody<Bytes, ServerError>>) -> u64 {
    let Some(val) = req.headers().get("range") else {
        return 0;
    };
    let Ok(s) = val.to_str() else {
        return 0;
    };
    // Expecting "bytes=N-" or "bytes=N-M"
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
