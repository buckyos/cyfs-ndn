//! `NamedStoreMgrZoneGateway` —— NDM Gateway 上传协议的服务端实现。
//!
//! 实现 `doc/ndm_gateway.md` 中定义的上传协议，包括：
//! - 对象存在性查询（quick hash 查重）
//! - 基于 chunk 的上传会话创建、断点续传、完成与持久化
//! - Gateway 侧缓存、配额、TTL/LRU 清理与状态管理
//! - 并发上传同一 chunk 时的互斥与幂等
//! - 结构化 store 控制面 API 的协议设计见 `doc/ndm_zone_gateway_structured_api.md`
//!
//! CYFS get/download 支持在下个迭代导入旧实现，当前仅做占位。

use async_trait::async_trait;
use bytes::Bytes;
use cyfs_gateway_lib::{HttpServer, ServerError, ServerResult, StreamInfo};
use http::{Method, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use log::{info, warn};
use ndn_lib::{ChunkHasher, ChunkId, NdnError, ObjId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use base64::Engine as _;

use serde::Deserialize;

use crate::gc_types::{EdgeMsg, PinRequest};
use crate::named_store::ObjectState;
use crate::store_db::ChunkStoreState;
use crate::ndm::NamedDataMgr;

// ======================== Constants ========================

/// 单个 chunk 最大 32 MiB
const MAX_CHUNK_SIZE: u64 = 32 * 1024 * 1024;

/// 默认会话 TTL: 1 小时
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(3600);

/// 默认每 App 缓存配额: 512 MiB
const DEFAULT_APP_QUOTA: u64 = 512 * 1024 * 1024;

/// GC 扫描间隔: 60 秒
const GC_INTERVAL: Duration = Duration::from_secs(60);

// ======================== TUS Protocol Constants ========================

const TUS_RESUMABLE: &str = "1.0.0";
const H_TUS_RESUMABLE: &str = "tus-resumable";
const H_TUS_VERSION: &str = "tus-version";
const H_TUS_EXTENSION: &str = "tus-extension";
const H_TUS_MAX_SIZE: &str = "tus-max-size";

// ======================== Custom Headers ========================

const H_UPLOAD_OFFSET: &str = "upload-offset";
const H_UPLOAD_LENGTH: &str = "upload-length";
const H_UPLOAD_METADATA: &str = "upload-metadata";
const H_UPLOAD_EXPIRES: &str = "upload-expires";
#[allow(dead_code)]
const H_UPLOAD_CHECKSUM: &str = "upload-checksum";
const H_NDM_UPLOAD_ID: &str = "ndm-upload-id";
const H_NDM_CHUNK_STATUS: &str = "ndm-chunk-status";
const H_NDM_CHUNK_OBJECT_ID: &str = "ndm-chunk-object-id";

fn remove_temp_cache_file(
    path: &Path,
    reason: &str,
    session_id: &str,
    app_id: &str,
    logical_path: &str,
    chunk_index: u32,
    cached_bytes: u64,
) {
    if path.as_os_str().is_empty() {
        return;
    }

    info!(
        "removing upload cache file: reason={}, session={}, app={}, path={}, chunk={}, cached_bytes={}, temp_file={}",
        reason,
        session_id,
        app_id,
        logical_path,
        chunk_index,
        cached_bytes,
        path.display()
    );

    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(
                "upload cache file already absent: reason={}, session={}, temp_file={}",
                reason,
                session_id,
                path.display()
            );
        }
        Err(e) => {
            warn!(
                "failed to remove upload cache file: reason={}, session={}, app={}, path={}, chunk={}, temp_file={}, err={}",
                reason,
                session_id,
                app_id,
                logical_path,
                chunk_index,
                path.display(),
                e
            );
        }
    }
}

async fn remove_temp_cache_file_async(
    path: &Path,
    reason: &str,
    session_id: &str,
    app_id: &str,
    logical_path: &str,
    chunk_index: u32,
    cached_bytes: u64,
) {
    if path.as_os_str().is_empty() {
        return;
    }

    info!(
        "removing upload cache file: reason={}, session={}, app={}, path={}, chunk={}, cached_bytes={}, temp_file={}",
        reason,
        session_id,
        app_id,
        logical_path,
        chunk_index,
        cached_bytes,
        path.display()
    );

    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(
                "upload cache file already absent: reason={}, session={}, temp_file={}",
                reason,
                session_id,
                path.display()
            );
        }
        Err(e) => {
            warn!(
                "failed to remove upload cache file: reason={}, session={}, app={}, path={}, chunk={}, temp_file={}, err={}",
                reason,
                session_id,
                app_id,
                logical_path,
                chunk_index,
                path.display(),
                e
            );
        }
    }
}

// ======================== Upload Session Types ========================

/// chunk 上传状态机
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ChunkUploadStatus {
    /// 会话已创建，尚未收到数据
    Pending,
    /// 已接收部分数据，offset > 0 且未完成
    Uploading,
    /// chunk 已写入对象存储
    Completed,
    /// chunk 已通过去重命中，无需上传
    Skipped,
    /// 未完成 chunk 的缓存已过期或被淘汰
    Expired,
}

impl ChunkUploadStatus {
    fn as_str(&self) -> &'static str {
        match self {
            ChunkUploadStatus::Pending => "pending",
            ChunkUploadStatus::Uploading => "uploading",
            ChunkUploadStatus::Completed => "completed",
            ChunkUploadStatus::Skipped => "skipped",
            ChunkUploadStatus::Expired => "expired",
        }
    }
}

/// 上传会话元数据，从 Upload-Metadata header 解析
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UploadMetadata {
    pub app_id: String,
    pub logical_path: String,
    pub file_name: Option<String>,
    pub file_size: Option<u64>,
    pub file_hash: Option<String>,
    pub quick_hash: Option<String>,
    pub chunk_index: u32,
    pub chunk_hash: Option<String>,
    pub mime_type: Option<String>,
}

/// chunk 上传会话
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UploadSession {
    pub session_id: String,
    pub canonical_upload_id: String,
    pub app_id: String,
    pub logical_path: String,
    pub file_hash: Option<String>,
    pub chunk_index: u32,
    pub chunk_hash: Option<String>,
    pub chunk_size: u64,
    pub offset: u64,
    pub status: ChunkUploadStatus,
    pub temp_file_path: PathBuf,
    pub created_at: Instant,
    pub updated_at: Instant,
    /// 完成后返回的 object id
    pub chunk_object_id: Option<String>,
    /// 原始 Upload-Metadata header 值，用于 HEAD 回显
    pub raw_metadata: String,
}

/// 每 App 缓存使用统计
#[derive(Debug, Clone)]
struct AppCacheUsage {
    bytes_in_use: u64,
    quota_bytes: u64,
}

/// 上传会话的唯一键：(app_id, logical_path, file_hash, chunk_index)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SessionKey {
    app_id: String,
    logical_path: String,
    file_hash: String,
    chunk_index: u32,
}

/// Gateway 上传状态管理器
struct UploadStateManager {
    /// session_id -> UploadSession
    sessions: HashMap<String, UploadSession>,
    /// SessionKey -> session_id，用于幂等查找
    key_index: HashMap<SessionKey, String>,
    /// app_id -> AppCacheUsage
    app_usage: HashMap<String, AppCacheUsage>,
    /// 自增 session 计数器
    next_session_seq: u64,
}

impl UploadStateManager {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            key_index: HashMap::new(),
            app_usage: HashMap::new(),
            next_session_seq: 1,
        }
    }

    fn generate_session_id(&mut self) -> String {
        let seq = self.next_session_seq;
        self.next_session_seq += 1;
        format!(
            "us_{:016x}_{:08x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            seq
        )
    }

    fn get_or_create_app_usage(&mut self, app_id: &str) -> &mut AppCacheUsage {
        self.app_usage
            .entry(app_id.to_string())
            .or_insert(AppCacheUsage {
                bytes_in_use: 0,
                quota_bytes: DEFAULT_APP_QUOTA,
            })
    }

    /// LRU 淘汰：按 updated_at 从最旧开始淘汰，直到释放出 needed 字节
    fn evict_lru_for_app(&mut self, app_id: &str, needed: u64, _cache_dir: &Path) -> u64 {
        // 收集该 app 的所有未完成 session，按 updated_at 排序
        let mut candidates: Vec<(String, Instant, u64)> = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                s.app_id == app_id
                    && matches!(
                        s.status,
                        ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
                    )
            })
            .map(|(id, s)| (id.clone(), s.updated_at, s.offset))
            .collect();
        candidates.sort_by_key(|(_, t, _)| *t);

        let mut freed: u64 = 0;
        for (sid, _, offset_bytes) in &candidates {
            if freed >= needed {
                break;
            }
            if let Some(session) = self.sessions.remove(sid) {
                // 删除 key_index
                let key = SessionKey {
                    app_id: session.app_id.clone(),
                    logical_path: session.logical_path.clone(),
                    file_hash: session.file_hash.clone().unwrap_or_default(),
                    chunk_index: session.chunk_index,
                };
                self.key_index.remove(&key);
                // 删除临时文件
                remove_temp_cache_file(
                    &session.temp_file_path,
                    "lru_evict",
                    &session.session_id,
                    &session.app_id,
                    &session.logical_path,
                    session.chunk_index,
                    session.offset,
                );
                freed += offset_bytes;
            }
        }

        // 更新 app usage
        if let Some(usage) = self.app_usage.get_mut(app_id) {
            usage.bytes_in_use = usage.bytes_in_use.saturating_sub(freed);
        }
        freed
    }

    /// TTL 过期清理
    fn expire_sessions(&mut self, ttl: Duration) {
        let now = Instant::now();
        let expired_ids: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                matches!(
                    s.status,
                    ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
                ) && now.duration_since(s.updated_at) > ttl
            })
            .map(|(id, _)| id.clone())
            .collect();

        for sid in expired_ids {
            if let Some(session) = self.sessions.remove(&sid) {
                let key = SessionKey {
                    app_id: session.app_id.clone(),
                    logical_path: session.logical_path.clone(),
                    file_hash: session.file_hash.clone().unwrap_or_default(),
                    chunk_index: session.chunk_index,
                };
                self.key_index.remove(&key);
                // 更新 app usage
                if let Some(usage) = self.app_usage.get_mut(&session.app_id) {
                    usage.bytes_in_use = usage.bytes_in_use.saturating_sub(session.offset);
                }
                // 删除临时文件
                remove_temp_cache_file(
                    &session.temp_file_path,
                    "ttl_expired",
                    &session.session_id,
                    &session.app_id,
                    &session.logical_path,
                    session.chunk_index,
                    session.offset,
                );
                info!(
                    "expired upload session {} (app={}, path={}, chunk={})",
                    sid, session.app_id, session.logical_path, session.chunk_index
                );
            }
        }
    }
}

// ======================== NDM Zone Gateway Config ========================

#[derive(Debug, Clone)]
pub struct NdmZoneGatewayConfig {
    /// 缓存临时文件目录
    pub cache_dir: PathBuf,
    /// 会话 TTL
    pub session_ttl: Duration,
    /// 默认每 App 配额
    pub default_app_quota: u64,
}

impl Default for NdmZoneGatewayConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/tmp/ndm_upload_cache"),
            session_ttl: DEFAULT_SESSION_TTL,
            default_app_quota: DEFAULT_APP_QUOTA,
        }
    }
}

// ======================== Gateway ========================

#[derive(Clone)]
pub struct NamedStoreMgrZoneGateway {
    store_mgr: Arc<NamedDataMgr>,
    state: Arc<RwLock<UploadStateManager>>,
    config: NdmZoneGatewayConfig,
}

impl NamedStoreMgrZoneGateway {
    pub fn new(store_mgr: Arc<NamedDataMgr>, config: NdmZoneGatewayConfig) -> Self {
        let gw = Self {
            store_mgr,
            state: Arc::new(RwLock::new(UploadStateManager::new())),
            config,
        };
        // 确保缓存目录存在
        let _ = std::fs::create_dir_all(&gw.config.cache_dir);
        // 启动后台 GC 任务
        gw.start_gc_task();
        gw
    }

    fn start_gc_task(&self) {
        let state = self.state.clone();
        let ttl = self.config.session_ttl;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(GC_INTERVAL).await;
                let mut mgr = state.write().await;
                mgr.expire_sessions(ttl);
            }
        });
    }
}

#[async_trait]
impl HttpServer for NamedStoreMgrZoneGateway {
    async fn serve_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
        _info: StreamInfo,
    ) -> ServerResult<http::Response<BoxBody<Bytes, ServerError>>> {
        let is_tus_path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
            .starts_with("/ndm/v1/uploads");

        let result = self.route_request(req).await;
        match result {
            Ok(resp) => {
                info!("ndm-zone-gateway served request {}", resp.status());
                Ok(resp)
            }
            Err(e) => {
                let (status, error_code) = ndm_error_to_status(&e);
                warn!("ndm-zone-gateway request failed: {} -> {}", status, e);
                let is_version_mismatch =
                    matches!(&e, NdnError::VerifyError(msg) if msg.contains("Tus-Resumable"));
                Ok(build_error_response(
                    status,
                    &error_code,
                    &e.to_string(),
                    is_tus_path,
                    is_version_mismatch,
                ))
            }
        }
    }

    fn id(&self) -> String {
        "ndm-zone-gateway".to_string()
    }

    fn http_version(&self) -> Version {
        Version::HTTP_11
    }

    fn http3_port(&self) -> Option<u16> {
        None
    }
}

// ======================== Request Routing ========================

impl NamedStoreMgrZoneGateway {
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
        // tus 1.0.0: honor X-HTTP-Method-Override so restricted environments
        // can tunnel PATCH/DELETE over POST
        let method = req
            .headers()
            .get("x-http-method-override")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<Method>().ok())
            .unwrap_or_else(|| req.method().clone());

        // ---- NDM upload protocol ----
        // GET /ndm/v1/objects/lookup?scope=...&quick_hash=...
        if path.starts_with("/ndm/v1/objects/lookup") && method == Method::GET {
            return self.handle_object_lookup(&path).await;
        }

        // OPTIONS /ndm/v1/uploads — TUS 能力发现
        if path.starts_with("/ndm/v1/uploads") && method == Method::OPTIONS {
            return self.handle_options();
        }

        // TUS Resumable 版本校验：POST/HEAD/PATCH 必须携带 Tus-Resumable: 1.0.0
        if path.starts_with("/ndm/v1/uploads") && matches!(method, Method::POST | Method::PATCH) {
            Self::validate_tus_resumable(req.headers())?;
        }
        // HEAD 也要校验（读取也需要版本协商）
        if let Some(_) = strip_prefix_segment(&path, "/ndm/v1/uploads/") {
            if method == Method::HEAD {
                Self::validate_tus_resumable(req.headers())?;
            }
        }

        // POST /ndm/v1/uploads — 创建 chunk 上传会话
        if path == "/ndm/v1/uploads" && method == Method::POST {
            return self.handle_create_upload(req).await;
        }

        // HEAD /ndm/v1/uploads/{session_id} — 查询上传状态
        if let Some(session_id) = strip_prefix_segment(&path, "/ndm/v1/uploads/") {
            match method {
                Method::HEAD => return self.handle_head_upload(&session_id).await,
                Method::PATCH => return self.handle_patch_upload(&session_id, req).await,
                _ => {
                    return Err(NdnError::Unsupported(format!(
                        "{} on /ndm/v1/uploads/{{session_id}}",
                        method
                    )))
                }
            }
        }

        // ---- Structured store control-plane API ----
        if let Some(method_name) = strip_prefix_segment(&path, "/ndm/v1/store/") {
            if method == Method::POST {
                return self.handle_store_rpc(&method_name, req).await;
            }
            return Err(NdnError::Unsupported(format!(
                "store API only accepts POST, got {}",
                method
            )));
        }

        // ---- CYFS get/download 占位（下个迭代导入旧实现） ----
        if path.starts_with("/cyfs/") || path.starts_with("/ndn/") {
            return Err(NdnError::Unsupported(
                "CYFS get/download not yet implemented in zone gateway, coming in next iteration"
                    .to_string(),
            ));
        }

        Err(NdnError::NotFound(format!(
            "unknown route: {} {}",
            method, path
        )))
    }

    /// TUS 版本校验：要求 Tus-Resumable: 1.0.0，否则返回 412 Precondition Failed
    fn validate_tus_resumable(headers: &http::HeaderMap) -> Result<(), NdnError> {
        match headers.get(H_TUS_RESUMABLE).and_then(|v| v.to_str().ok()) {
            Some(v) if v == TUS_RESUMABLE => Ok(()),
            Some(v) => Err(NdnError::VerifyError(format!(
                "unsupported Tus-Resumable version: {}, server supports {}",
                v, TUS_RESUMABLE
            ))),
            None => Err(NdnError::VerifyError(
                "missing Tus-Resumable header, required for tus protocol".to_string(),
            )),
        }
    }

    /// OPTIONS 响应：返回 TUS 能力信息
    fn handle_options(&self) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(H_TUS_RESUMABLE, TUS_RESUMABLE)
            .header(H_TUS_VERSION, TUS_RESUMABLE)
            .header(H_TUS_EXTENSION, "creation,expiration")
            .header(H_TUS_MAX_SIZE, MAX_CHUNK_SIZE)
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    // ======================== Object Lookup (6.1) ========================

    async fn handle_object_lookup(
        &self,
        path: &str,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let query_str = path.split_once('?').map(|(_, q)| q).unwrap_or("");
        let params = parse_query_params(query_str);

        let scope = params
            .get("scope")
            .ok_or_else(|| NdnError::InvalidParam("missing scope parameter".to_string()))?;
        let quick_hash = params
            .get("quick_hash")
            .ok_or_else(|| NdnError::InvalidParam("missing quick_hash parameter".to_string()))?;

        match scope.as_str() {
            "app" | "global" => {
                // 用 quick_hash 作为 ObjId 尝试在 store 中查找
                let obj_id = match ObjId::new(quick_hash) {
                    Ok(id) => id,
                    Err(_) => {
                        return Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header("content-type", "application/json; charset=utf-8")
                            .body(full_body(Bytes::from(
                                r#"{"error":"not_found","message":"object not found"}"#,
                            )))
                            .map_err(|e| NdnError::Internal(format!("build response: {e}")));
                    }
                };

                // chunk 类型：走 query_chunk_state，返回与 store RPC 对齐的状态
                if obj_id.is_chunk() {
                    let chunk_id = ChunkId::from_obj_id(&obj_id);
                    let (state, size) = self.store_mgr.query_chunk_state(&chunk_id).await?;
                    let mut resp = chunk_store_state_to_json(state, size);
                    resp["object_id"] = serde_json::json!(obj_id.to_string());
                    resp["scope"] = serde_json::json!(scope);
                    return json_response(&resp);
                }

                // 非 chunk 对象：��� is_object_stored
                let inner_path = params.get("inner_path").cloned();

                match self.store_mgr.is_object_stored(&obj_id, inner_path).await {
                    Ok(true) => json_response(&serde_json::json!({
                        "object_id": obj_id.to_string(),
                        "scope": scope,
                        "exists": true,
                    })),
                    Ok(false) | Err(NdnError::NotFound(_)) => Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("content-type", "application/json; charset=utf-8")
                        .body(full_body(Bytes::from(
                            r#"{"error":"not_found","message":"object not found"}"#,
                        )))
                        .map_err(|e| NdnError::Internal(format!("build response: {e}"))),
                    Err(e) => Err(e),
                }
            }
            _ => Err(NdnError::InvalidParam(format!(
                "invalid scope: {}, expected 'app' or 'global'",
                scope
            ))),
        }
    }

    // ======================== Create Upload Session (6.4.1) ========================

    async fn handle_create_upload(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        // 解析 Upload-Length
        let chunk_size = parse_header_u64(req.headers(), H_UPLOAD_LENGTH)
            .ok_or_else(|| NdnError::InvalidParam("missing Upload-Length header".to_string()))?;

        if chunk_size > MAX_CHUNK_SIZE {
            return Ok(build_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                &format!(
                    "chunk size {} exceeds max {} (32 MiB)",
                    chunk_size, MAX_CHUNK_SIZE
                ),
                true,
                false,
            ));
        }

        // 保存原始 Upload-Metadata 值用于 HEAD 回显
        let raw_metadata = req
            .headers()
            .get(H_UPLOAD_METADATA)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        // 解析 Upload-Metadata
        let metadata = parse_upload_metadata(req.headers())?;

        // 验证逻辑路径安全性
        validate_logical_path(&metadata.logical_path)?;

        // 生成 canonical upload id
        let canonical_upload_id = if let Some(ref custom_id) = req
            .headers()
            .get(H_NDM_UPLOAD_ID)
            .and_then(|v| v.to_str().ok().map(String::from))
        {
            custom_id.clone()
        } else {
            format!("path:{}/{}", metadata.app_id, metadata.logical_path)
        };

        let file_hash = metadata.file_hash.clone().unwrap_or_default();
        let session_key = SessionKey {
            app_id: metadata.app_id.clone(),
            logical_path: metadata.logical_path.clone(),
            file_hash: file_hash.clone(),
            chunk_index: metadata.chunk_index,
        };

        let mut state = self.state.write().await;

        // 检查同一路径是否有旧的 file_hash 不同的未完成会话，若有则清理
        self.invalidate_stale_sessions(
            &mut state,
            &metadata.app_id,
            &metadata.logical_path,
            &file_hash,
        );

        // 幂等检查：相同 key 是否已有会话
        if let Some(existing_sid) = state.key_index.get(&session_key).cloned() {
            if let Some(existing) = state.sessions.get(&existing_sid) {
                match existing.status {
                    ChunkUploadStatus::Completed => {
                        // chunk 已完成
                        return self.build_session_response(StatusCode::OK, existing);
                    }
                    ChunkUploadStatus::Skipped => {
                        return self.build_session_response(StatusCode::OK, existing);
                    }
                    ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading => {
                        // 返回已有会话
                        return self.build_session_response(StatusCode::OK, existing);
                    }
                    ChunkUploadStatus::Expired => {
                        // 清理过期会话，重新创建
                        remove_temp_cache_file(
                            &existing.temp_file_path,
                            "recreate_expired_session",
                            &existing.session_id,
                            &existing.app_id,
                            &existing.logical_path,
                            existing.chunk_index,
                            existing.offset,
                        );
                        state.sessions.remove(&existing_sid);
                        state.key_index.remove(&session_key);
                    }
                }
            }
        }

        // chunk hash 去重：如果提供了 chunk_hash 且在对象存储中已存在
        if let Some(ref chunk_hash) = metadata.chunk_hash {
            if let Ok(chunk_obj_id) = ObjId::new(chunk_hash) {
                if chunk_obj_id.is_chunk() {
                    let cid = ChunkId::from_obj_id(&chunk_obj_id);
                    if self.store_mgr.have_chunk(&cid).await {
                        // chunk 已存在，返回 skipped
                        let session_id = state.generate_session_id();
                        let session = UploadSession {
                            session_id: session_id.clone(),
                            canonical_upload_id: canonical_upload_id.clone(),
                            app_id: metadata.app_id.clone(),
                            logical_path: metadata.logical_path.clone(),
                            file_hash: metadata.file_hash.clone(),
                            chunk_index: metadata.chunk_index,
                            chunk_hash: metadata.chunk_hash.clone(),
                            chunk_size,
                            offset: chunk_size,
                            status: ChunkUploadStatus::Skipped,
                            temp_file_path: PathBuf::new(),
                            created_at: Instant::now(),
                            updated_at: Instant::now(),
                            chunk_object_id: Some(chunk_obj_id.to_string()),
                            raw_metadata: raw_metadata.clone(),
                        };
                        let resp = self.build_session_response(StatusCode::OK, &session);
                        state.sessions.insert(session_id.clone(), session);
                        state.key_index.insert(session_key, session_id);
                        return resp;
                    }
                }
            }
        }

        // 检查 App 配额
        {
            let usage = state.get_or_create_app_usage(&metadata.app_id);
            if usage.bytes_in_use + chunk_size > usage.quota_bytes {
                // 尝试 LRU 淘汰
                let needed = (usage.bytes_in_use + chunk_size) - usage.quota_bytes;
                let freed =
                    state.evict_lru_for_app(&metadata.app_id, needed, &self.config.cache_dir);
                let usage = state.get_or_create_app_usage(&metadata.app_id);
                if usage.bytes_in_use + chunk_size > usage.quota_bytes {
                    return Err(NdnError::IoError(format!(
                        "app {} cache quota exceeded: in_use={}, needed={}, quota={}, freed={}",
                        metadata.app_id, usage.bytes_in_use, chunk_size, usage.quota_bytes, freed
                    )));
                }
            }
        }

        // 分配临时文件
        let session_id = state.generate_session_id();
        let temp_dir = self.config.cache_dir.join(&metadata.app_id);
        let _ = std::fs::create_dir_all(&temp_dir);
        let temp_file_path = temp_dir.join(format!("{}_{}.tmp", session_id, metadata.chunk_index));

        // 创建空临时文件
        tokio::fs::File::create(&temp_file_path)
            .await
            .map_err(|e| NdnError::IoError(format!("create temp file: {e}")))?;

        let now = Instant::now();

        // Upload-Length: 0 — 零长度上传，创建即完成
        if chunk_size == 0 {
            let session = UploadSession {
                session_id: session_id.clone(),
                canonical_upload_id,
                app_id: metadata.app_id.clone(),
                logical_path: metadata.logical_path.clone(),
                file_hash: metadata.file_hash.clone(),
                chunk_index: metadata.chunk_index,
                chunk_hash: metadata.chunk_hash.clone(),
                chunk_size: 0,
                offset: 0,
                status: ChunkUploadStatus::Completed,
                temp_file_path: PathBuf::new(),
                created_at: now,
                updated_at: now,
                chunk_object_id: None,
                raw_metadata: raw_metadata.clone(),
            };
            // 删除临时文件（已创建但不需要）
            remove_temp_cache_file_async(
                &temp_file_path,
                "zero_length_upload",
                &session_id,
                &metadata.app_id,
                &metadata.logical_path,
                metadata.chunk_index,
                0,
            )
            .await;
            let resp = self.build_session_response(StatusCode::CREATED, &session);
            state.sessions.insert(session_id.clone(), session);
            state.key_index.insert(session_key, session_id);
            return resp;
        }

        let session = UploadSession {
            session_id: session_id.clone(),
            canonical_upload_id,
            app_id: metadata.app_id.clone(),
            logical_path: metadata.logical_path.clone(),
            file_hash: metadata.file_hash.clone(),
            chunk_index: metadata.chunk_index,
            chunk_hash: metadata.chunk_hash.clone(),
            chunk_size,
            offset: 0,
            status: ChunkUploadStatus::Pending,
            temp_file_path,
            created_at: now,
            updated_at: now,
            chunk_object_id: None,
            raw_metadata,
        };

        let resp = self.build_session_response(StatusCode::CREATED, &session);
        state.sessions.insert(session_id.clone(), session);
        state.key_index.insert(session_key, session_id);

        resp
    }

    // ======================== Head Upload (6.4.2) ========================

    async fn handle_head_upload(
        &self,
        session_id: &str,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let state = self.state.read().await;
        let session = state
            .sessions
            .get(session_id)
            .ok_or_else(|| NdnError::NotFound(format!("session {} not found", session_id)))?;

        // 检查是否过期
        if matches!(
            session.status,
            ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
        ) && Instant::now().duration_since(session.updated_at) > self.config.session_ttl
        {
            return Err(NdnError::NotFound(format!(
                "session {} expired (410 Gone)",
                session_id
            )));
        }

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(H_TUS_RESUMABLE, TUS_RESUMABLE)
            .header(H_UPLOAD_OFFSET, session.offset)
            .header(H_UPLOAD_LENGTH, session.chunk_size)
            .header(H_NDM_CHUNK_STATUS, session.status.as_str())
            .header(H_NDM_UPLOAD_ID, &session.canonical_upload_id)
            .header("cache-control", "no-store");

        // 回显原始 Upload-Metadata
        if !session.raw_metadata.is_empty() {
            builder = builder.header(H_UPLOAD_METADATA, &session.raw_metadata);
        }

        // 返回 Upload-Expires（活跃会话的过期时间）
        if matches!(
            session.status,
            ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
        ) {
            let expires_at = session.updated_at + self.config.session_ttl;
            let remaining = expires_at.saturating_duration_since(Instant::now());
            let expire_time = std::time::SystemTime::now() + remaining;
            builder = builder.header(H_UPLOAD_EXPIRES, format_http_date(expire_time));
        }

        if let Some(ref oid) = session.chunk_object_id {
            builder = builder.header(H_NDM_CHUNK_OBJECT_ID, oid.as_str());
        }

        builder
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    // ======================== Patch Upload (6.4.3) ========================

    async fn handle_patch_upload(
        &self,
        session_id: &str,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        // TUS 要求 PATCH Content-Type 必须是 application/offset+octet-stream
        let content_type = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if content_type != "application/offset+octet-stream" {
            return Err(NdnError::InvalidParam(format!(
                "PATCH requires Content-Type: application/offset+octet-stream, got: {}",
                content_type
            )));
        }

        // 解析 Upload-Offset
        let client_offset = parse_header_u64(req.headers(), H_UPLOAD_OFFSET)
            .ok_or_else(|| NdnError::InvalidParam("missing Upload-Offset header".to_string()))?;

        let completed_response = {
            let state = self.state.read().await;
            let session = state
                .sessions
                .get(session_id)
                .ok_or_else(|| NdnError::NotFound(format!("session {} not found", session_id)))?;

            match session.status {
                ChunkUploadStatus::Completed | ChunkUploadStatus::Skipped => {
                    Some(self.build_session_response(StatusCode::NO_CONTENT, session)?)
                }
                ChunkUploadStatus::Pending
                | ChunkUploadStatus::Uploading
                | ChunkUploadStatus::Expired => None,
            }
        };

        if let Some(resp) = completed_response {
            // A browser/proxy may retry PATCH after the chunk is already marked
            // completed. Drain the body before replying so the TCP connection is
            // not reset while the client is still sending bytes.
            let _ = collect_body(req).await?;
            return Ok(resp);
        }

        // 先读状态检查
        {
            let state = self.state.read().await;
            let session = state
                .sessions
                .get(session_id)
                .ok_or_else(|| NdnError::NotFound(format!("session {} not found", session_id)))?;
            // 检查状态
            match session.status {
                ChunkUploadStatus::Expired => {
                    return Err(NdnError::NotReady(format!(
                        "session {} expired, cache evicted (410 Gone)",
                        session_id
                    )));
                }
                ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading => {}
                ChunkUploadStatus::Completed | ChunkUploadStatus::Skipped => {
                    unreachable!("completed/skipped sessions are handled before validation")
                }
            }

            // 检查过期
            if Instant::now().duration_since(session.updated_at) > self.config.session_ttl {
                return Err(NdnError::NotReady(format!(
                    "session {} expired (410 Gone)",
                    session_id
                )));
            }

            // 校验 offset
            if client_offset != session.offset {
                return Err(NdnError::VerifyError(format!(
                    "offset mismatch: client={}, server={} (409 Conflict)",
                    client_offset, session.offset
                )));
            }
        }

        // 读取请求体
        let body_data = collect_body(req).await?;
        let body_len = body_data.len() as u64;

        if body_len == 0 {
            return Err(NdnError::InvalidParam("empty body".to_string()));
        }

        // 写状态 + 写文件（需要写锁）
        let mut state = self.state.write().await;

        // 提取 session 信息并校验
        let (
            temp_file_path,
            chunk_size,
            chunk_hash,
            app_id,
            logical_path,
            chunk_index,
            new_offset,
            is_complete,
        ) = {
            let session = state
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| NdnError::NotFound(format!("session {} not found", session_id)))?;

            // 再次校验 offset（可能在获取写锁期间被其他请求修改）
            if client_offset != session.offset {
                return Err(NdnError::VerifyError(format!(
                    "offset mismatch after lock: client={}, server={}",
                    client_offset, session.offset
                )));
            }

            // 校验不超出 chunk_size
            if session.offset + body_len > session.chunk_size {
                return Err(NdnError::InvalidParam(format!(
                    "data exceeds chunk size: offset={} + body_len={} > chunk_size={}",
                    session.offset, body_len, session.chunk_size
                )));
            }

            // 追加写入临时文件
            {
                let mut file = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&session.temp_file_path)
                    .await
                    .map_err(|e| NdnError::IoError(format!("open temp file for append: {e}")))?;
                file.write_all(&body_data)
                    .await
                    .map_err(|e| NdnError::IoError(format!("write temp file: {e}")))?;
                file.flush()
                    .await
                    .map_err(|e| NdnError::IoError(format!("flush temp file: {e}")))?;
            }

            session.offset += body_len;
            session.updated_at = Instant::now();
            session.status = ChunkUploadStatus::Uploading;

            let temp_path = session.temp_file_path.clone();
            let chunk_size = session.chunk_size;
            let chunk_hash = session.chunk_hash.clone();
            let app_id = session.app_id.clone();
            let logical_path = session.logical_path.clone();
            let chunk_index = session.chunk_index;
            let new_offset = session.offset;
            let is_complete = session.offset == session.chunk_size;

            (
                temp_path,
                chunk_size,
                chunk_hash,
                app_id,
                logical_path,
                chunk_index,
                new_offset,
                is_complete,
            )
        };

        // 如果已写满，持久化到对象存储
        if is_complete {
            // 确定 ChunkId
            let chunk_id = if let Some(ref hash) = chunk_hash {
                let oid = ObjId::new(hash).map_err(|_| {
                    NdnError::InvalidParam("chunk_hash is not a valid object id".to_string())
                })?;
                if !oid.is_chunk() {
                    return Err(NdnError::InvalidParam(
                        "chunk_hash is not a valid chunk id format".to_string(),
                    ));
                }
                ChunkId::from_obj_id(&oid)
            } else {
                // 从文件内容计算 ChunkId
                let content = tokio::fs::read(&temp_file_path)
                    .await
                    .map_err(|e| NdnError::IoError(format!("read temp file for hash: {e}")))?;
                let hasher = ChunkHasher::new(None)?;
                hasher.calc_mix_chunk_id_from_bytes(&content)?
            };

            // 打开临时文件作为 reader 并写入对象存储
            let file = tokio::fs::File::open(&temp_file_path)
                .await
                .map_err(|e| NdnError::IoError(format!("open temp file for persist: {e}")))?;
            let reader: ndn_lib::ChunkReader = Box::pin(file);

            let outcome = self
                .store_mgr
                .put_chunk_by_reader(&chunk_id, chunk_size, reader)
                .await?;
            let chunk_obj_id = chunk_id.to_obj_id().to_string();
            info!(
                "stored uploaded chunk into store: session={}, app={}, path={}, chunk={}, chunk_obj_id={}, chunk_size={}, outcome={:?}",
                session_id,
                app_id,
                logical_path,
                chunk_index,
                chunk_obj_id,
                chunk_size,
                outcome
            );

            // 更新 session 状态
            if let Some(session) = state.sessions.get_mut(session_id) {
                session.status = ChunkUploadStatus::Completed;
                session.chunk_object_id = Some(chunk_obj_id.clone());
            }

            // 更新 app usage：释放缓存
            if let Some(usage) = state.app_usage.get_mut(&app_id) {
                usage.bytes_in_use = usage.bytes_in_use.saturating_sub(chunk_size);
            }

            // 删除临时文件
            remove_temp_cache_file_async(
                &temp_file_path,
                "chunk_persisted",
                session_id,
                &app_id,
                &logical_path,
                chunk_index,
                chunk_size,
            )
            .await;

            // 构建响应
            if let Some(session) = state.sessions.get(session_id) {
                return self.build_session_response(StatusCode::NO_CONTENT, session);
            }
            // fallback: session somehow disappeared
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(H_TUS_RESUMABLE, TUS_RESUMABLE)
                .header(H_NDM_CHUNK_STATUS, "completed")
                .header(H_NDM_CHUNK_OBJECT_ID, chunk_obj_id)
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response: {e}")));
        }

        // 更新 app usage: 记录新写入的字节
        if let Some(usage) = state.app_usage.get_mut(&app_id) {
            usage.bytes_in_use += body_len;
        }

        // 未写满，返回当前状态
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(H_TUS_RESUMABLE, TUS_RESUMABLE)
            .header(H_UPLOAD_OFFSET, new_offset)
            .header(H_NDM_CHUNK_STATUS, "uploading")
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }

    // ======================== Session helpers ========================

    /// 清理同一 (app_id, logical_path) 下 file_hash 不同的旧未完成会话
    fn invalidate_stale_sessions(
        &self,
        state: &mut UploadStateManager,
        app_id: &str,
        logical_path: &str,
        new_file_hash: &str,
    ) {
        let stale_sids: Vec<String> = state
            .sessions
            .iter()
            .filter(|(_, s)| {
                s.app_id == app_id
                    && s.logical_path == logical_path
                    && s.file_hash.as_deref().unwrap_or("") != new_file_hash
                    && matches!(
                        s.status,
                        ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
                    )
            })
            .map(|(id, _)| id.clone())
            .collect();

        for sid in stale_sids {
            if let Some(session) = state.sessions.remove(&sid) {
                let key = SessionKey {
                    app_id: session.app_id.clone(),
                    logical_path: session.logical_path.clone(),
                    file_hash: session.file_hash.clone().unwrap_or_default(),
                    chunk_index: session.chunk_index,
                };
                state.key_index.remove(&key);
                if let Some(usage) = state.app_usage.get_mut(app_id) {
                    usage.bytes_in_use = usage.bytes_in_use.saturating_sub(session.offset);
                }
                remove_temp_cache_file(
                    &session.temp_file_path,
                    "invalidate_stale_session",
                    &session.session_id,
                    &session.app_id,
                    &session.logical_path,
                    session.chunk_index,
                    session.offset,
                );
                info!(
                    "invalidated stale session {} (file_hash changed, app={}, path={})",
                    sid, app_id, logical_path
                );
            }
        }
    }

    /// 构造上传会话响应
    fn build_session_response(
        &self,
        status: StatusCode,
        session: &UploadSession,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        let mut builder = Response::builder()
            .status(status)
            .header(H_TUS_RESUMABLE, TUS_RESUMABLE)
            .header(
                "location",
                format!("/ndm/v1/uploads/{}", session.session_id),
            )
            .header(H_NDM_UPLOAD_ID, &session.canonical_upload_id)
            .header(H_UPLOAD_OFFSET, session.offset)
            .header(H_UPLOAD_LENGTH, session.chunk_size)
            .header(H_NDM_CHUNK_STATUS, session.status.as_str());

        // 返回 Upload-Expires（活跃会话）
        if matches!(
            session.status,
            ChunkUploadStatus::Pending | ChunkUploadStatus::Uploading
        ) {
            let expires_at = session.updated_at + self.config.session_ttl;
            let remaining = expires_at.saturating_duration_since(Instant::now());
            let expire_time = std::time::SystemTime::now() + remaining;
            builder = builder.header(H_UPLOAD_EXPIRES, format_http_date(expire_time));
        }

        if let Some(ref oid) = session.chunk_object_id {
            builder = builder.header(H_NDM_CHUNK_OBJECT_ID, oid.as_str());
        }

        builder
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response: {e}")))
    }
}

// ======================== Structured Store RPC ========================

/// Request body for endpoints that take only an obj_id.
#[derive(Deserialize)]
struct ObjIdRequest {
    obj_id: String,
}

/// Request body for open_object / is_object_stored.
#[derive(Deserialize)]
struct ObjIdWithInnerPathRequest {
    obj_id: String,
    #[serde(default)]
    inner_path: Option<String>,
}

/// Request body for get_dir_child.
#[derive(Deserialize)]
struct GetDirChildRequest {
    dir_obj_id: String,
    item_name: String,
}

/// Request body for put_object.
#[derive(Deserialize)]
struct PutObjectRequest {
    obj_id: String,
    obj_data: String,
}

/// Request body for chunk endpoints that take only a chunk_id.
#[derive(Deserialize)]
struct ChunkIdRequest {
    chunk_id: String,
}

/// Request body for add_chunk_by_same_as.
#[derive(Deserialize)]
struct AddChunkBySameAsRequest {
    big_chunk_id: String,
    chunk_list_id: String,
    big_chunk_size: u64,
}

/// Request body for unpin.
#[derive(Deserialize)]
struct UnpinRequest {
    obj_id: String,
    owner: String,
}

/// Request body for unpin_owner.
#[derive(Deserialize)]
struct OwnerRequest {
    owner: String,
}

/// Request body for fs_acquire / fs_release / fs_anchor_state.
#[derive(Deserialize)]
struct FsAnchorRequest {
    obj_id: String,
    inode_id: u64,
    field_tag: u32,
}

/// Request body for fs_release_inode.
#[derive(Deserialize)]
struct InodeRequest {
    inode_id: u64,
}

/// Request body for forced_gc_until.
#[derive(Deserialize)]
struct ForcedGcRequest {
    target_bytes: u64,
}

/// Request body for anchor_state.
#[derive(Deserialize)]
struct AnchorStateRequest {
    obj_id: String,
    owner: String,
}

/// Normalize inner_path: empty / "/" → None.
fn normalize_inner_path(p: Option<String>) -> Option<String> {
    match p.as_deref() {
        None | Some("") | Some("/") => None,
        _ => p,
    }
}

/// Serialize ObjectState to JSON value.
fn object_state_to_json(state: ObjectState) -> serde_json::Value {
    match state {
        ObjectState::NotExist => serde_json::json!({ "state": "not_exist" }),
        ObjectState::Object(data) => serde_json::json!({ "state": "object", "obj_data": data }),
    }
}

/// Serialize ChunkStoreState + chunk_size to JSON value.
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

fn no_content_response() -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(empty_body())
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

impl NamedStoreMgrZoneGateway {
    async fn handle_store_rpc(
        &self,
        method_name: &str,
        req: http::Request<BoxBody<Bytes, ServerError>>,
    ) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
        match method_name {
            // ---- Object interfaces ----
            "get_object" => {
                let body = collect_body(req).await?;
                let r: ObjIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let obj_data = self.store_mgr.get_object(&obj_id).await?;
                json_response(&serde_json::json!({
                    "obj_id": obj_id.to_string(),
                    "obj_data": obj_data,
                }))
            }

            "open_object" => {
                let body = collect_body(req).await?;
                let r: ObjIdWithInnerPathRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let inner_path = normalize_inner_path(r.inner_path);
                let obj_data = self.store_mgr.open_object(&obj_id, inner_path).await?;
                json_response(&serde_json::json!({
                    "obj_data": obj_data,
                }))
            }

            "get_dir_child" => {
                let body = collect_body(req).await?;
                let r: GetDirChildRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let dir_obj_id = ObjId::new(&r.dir_obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid dir_obj_id: {e}")))?;
                let child_id = self
                    .store_mgr
                    .get_dir_child(&dir_obj_id, &r.item_name)
                    .await?;
                json_response(&serde_json::json!({
                    "obj_id": child_id.to_string(),
                }))
            }

            "is_object_stored" => {
                let body = collect_body(req).await?;
                let r: ObjIdWithInnerPathRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let inner_path = normalize_inner_path(r.inner_path);
                let stored = self.store_mgr.is_object_stored(&obj_id, inner_path).await?;
                json_response(&serde_json::json!({ "stored": stored }))
            }

            "is_object_exist" => {
                let body = collect_body(req).await?;
                let r: ObjIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let exists = self.store_mgr.is_object_exist(&obj_id).await?;
                json_response(&serde_json::json!({ "exists": exists }))
            }

            "query_object_by_id" => {
                let body = collect_body(req).await?;
                let r: ObjIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let state = self.store_mgr.query_object_by_id(&obj_id).await?;
                json_response(&object_state_to_json(state))
            }

            "put_object" => {
                let body = collect_body(req).await?;
                let r: PutObjectRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                if obj_id.is_chunk() {
                    return Err(NdnError::InvalidParam(
                        "put_object does not accept chunk ids; use chunk upload protocol instead"
                            .to_string(),
                    ));
                }
                self.store_mgr.put_object(&obj_id, &r.obj_data).await?;
                no_content_response()
            }

            "remove_object" => {
                let body = collect_body(req).await?;
                let r: ObjIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                if obj_id.is_chunk() {
                    return Err(NdnError::InvalidParam(
                        "remove_object does not accept chunk ids; use remove_chunk instead"
                            .to_string(),
                    ));
                }
                self.store_mgr.remove_object(&obj_id).await?;
                no_content_response()
            }

            // ---- Chunk metadata interfaces ----
            "have_chunk" => {
                let body = collect_body(req).await?;
                let r: ChunkIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let exists = self.store_mgr.have_chunk(&chunk_id).await;
                json_response(&serde_json::json!({ "exists": exists }))
            }

            "query_chunk_state" => {
                let body = collect_body(req).await?;
                let r: ChunkIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                let (state, size) = self.store_mgr.query_chunk_state(&chunk_id).await?;
                json_response(&chunk_store_state_to_json(state, size))
            }

            "remove_chunk" => {
                let body = collect_body(req).await?;
                let r: ChunkIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let chunk_id = ChunkId::new(&r.chunk_id)?;
                self.store_mgr.remove_chunk(&chunk_id).await?;
                no_content_response()
            }

            "add_chunk_by_same_as" => {
                let body = collect_body(req).await?;
                let r: AddChunkBySameAsRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let big_chunk_id = ChunkId::new(&r.big_chunk_id)?;
                let chunk_list_id = ObjId::new(&r.chunk_list_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid chunk_list_id: {e}")))?;
                self.store_mgr
                    .add_chunk_by_same_as(&big_chunk_id, r.big_chunk_size, &chunk_list_id)
                    .await?;
                no_content_response()
            }

            // ---- GC / Anchor / Debug interfaces ----
            "apply_edge" => {
                let body = collect_body(req).await?;
                let msg: EdgeMsg = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid EdgeMsg JSON: {e}")))?;
                self.store_mgr.apply_edge(msg).await?;
                no_content_response()
            }

            "pin" => {
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

            "unpin" => {
                let body = collect_body(req).await?;
                let r: UnpinRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                self.store_mgr.unpin(&obj_id, &r.owner).await?;
                no_content_response()
            }

            "unpin_owner" => {
                let body = collect_body(req).await?;
                let r: OwnerRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let count = self.store_mgr.unpin_owner(&r.owner).await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "fs_acquire" => {
                let body = collect_body(req).await?;
                let r: FsAnchorRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                self.store_mgr
                    .fs_acquire(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                no_content_response()
            }

            "fs_release" => {
                let body = collect_body(req).await?;
                let r: FsAnchorRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                self.store_mgr
                    .fs_release(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                no_content_response()
            }

            "fs_release_inode" => {
                let body = collect_body(req).await?;
                let r: InodeRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let count = self.store_mgr.fs_release_inode(r.inode_id).await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "fs_anchor_state" => {
                let body = collect_body(req).await?;
                let r: FsAnchorRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let state = self
                    .store_mgr
                    .fs_anchor_state(&obj_id, r.inode_id, r.field_tag)
                    .await?;
                json_response(&serde_json::json!({ "state": state.as_str() }))
            }

            "forced_gc_until" => {
                let body = collect_body(req).await?;
                let r: ForcedGcRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let freed_bytes = self.store_mgr.forced_gc_until(r.target_bytes).await?;
                json_response(&serde_json::json!({ "freed_bytes": freed_bytes }))
            }

            "outbox_count" => {
                let _body = collect_body(req).await?;
                let count = self.store_mgr.outbox_count().await?;
                json_response(&serde_json::json!({ "count": count }))
            }

            "debug_dump_expand_state" => {
                let body = collect_body(req).await?;
                let r: ObjIdRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let debug = self.store_mgr.debug_dump_expand_state(&obj_id).await?;
                let v = serde_json::to_value(&debug)
                    .map_err(|e| NdnError::Internal(format!("serialize ExpandDebug: {e}")))?;
                json_response(&v)
            }

            "anchor_state" => {
                let body = collect_body(req).await?;
                let r: AnchorStateRequest = serde_json::from_slice(&body)
                    .map_err(|e| NdnError::InvalidData(format!("invalid JSON: {e}")))?;
                let obj_id = ObjId::new(&r.obj_id)
                    .map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))?;
                let state = self.store_mgr.anchor_state(&obj_id, &r.owner).await?;
                json_response(&serde_json::json!({ "state": state.as_str() }))
            }

            _ => Err(NdnError::NotFound(format!(
                "unknown store method: {}",
                method_name
            ))),
        }
    }
}

// ======================== Upload-Metadata 解析 ========================

/// 解析 Upload-Metadata header。
/// 格式: key1 base64val1,key2 base64val2,...
/// 也支持简化的 key=value 格式（非 base64）
fn parse_upload_metadata(headers: &http::HeaderMap) -> Result<UploadMetadata, NdnError> {
    let raw = headers
        .get(H_UPLOAD_METADATA)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| NdnError::InvalidParam("missing Upload-Metadata header".to_string()))?;

    let mut fields: HashMap<String, String> = HashMap::new();

    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        // 尝试 "key base64val" 格式（tus 标准）
        if let Some((key, encoded)) = pair.split_once(' ') {
            let decoded =
                base64_decode(encoded.trim()).unwrap_or_else(|_| encoded.trim().to_string());
            fields.insert(key.trim().to_string(), decoded);
        } else if let Some((key, val)) = pair.split_once('=') {
            // 简化的 key=value 格式
            fields.insert(key.trim().to_string(), val.trim().to_string());
        }
    }

    let app_id = fields
        .get("app_id")
        .cloned()
        .ok_or_else(|| NdnError::InvalidParam("Upload-Metadata missing app_id".to_string()))?;

    let logical_path = fields.get("logical_path").cloned().ok_or_else(|| {
        NdnError::InvalidParam("Upload-Metadata missing logical_path".to_string())
    })?;

    let chunk_index = fields
        .get("chunk_index")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    Ok(UploadMetadata {
        app_id,
        logical_path,
        file_name: fields.get("file_name").cloned(),
        file_size: fields.get("file_size").and_then(|v| v.parse().ok()),
        file_hash: fields.get("file_hash").cloned(),
        quick_hash: fields.get("quick_hash").cloned(),
        chunk_index,
        chunk_hash: fields.get("chunk_hash").cloned(),
        mime_type: fields.get("mime_type").cloned(),
    })
}

fn base64_decode(s: &str) -> Result<String, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| format!("base64 decode error: {e}"))?;
    String::from_utf8(bytes).map_err(|e| format!("base64 decoded value is not valid UTF-8: {e}"))
}

// ======================== Path Validation ========================

fn validate_logical_path(path: &str) -> Result<(), NdnError> {
    if path.is_empty() {
        return Err(NdnError::InvalidParam(
            "logical_path cannot be empty".to_string(),
        ));
    }
    if path.contains("..") {
        return Err(NdnError::InvalidParam(
            "logical_path must not contain '..' (directory traversal)".to_string(),
        ));
    }
    if path.starts_with('/') {
        return Err(NdnError::InvalidParam(
            "logical_path must not start with '/' (must be relative)".to_string(),
        ));
    }
    // 允许常见文件名字符，但拒绝控制字符和 Windows 路径分隔符。
    for ch in path.chars() {
        if !ch.is_ascii() || ch.is_ascii_control() || ch == '\\' {
            return Err(NdnError::InvalidParam(format!(
                "logical_path contains unsafe character: '{}'",
                ch
            )));
        }
    }
    Ok(())
}

// ======================== Helpers ========================

/// 格式化 SystemTime 为 HTTP 日期格式（RFC 7231）
fn format_http_date(time: std::time::SystemTime) -> String {
    let dur = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // 手动计算 HTTP-date（避免引入 chrono/httpdate 依赖）
    const DAYS_PER_MONTH: [[u64; 12]; 2] = [
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    ];
    const WDAY: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MON: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let sec = (secs % 60) as u32;
    let min = ((secs / 60) % 60) as u32;
    let hour = ((secs / 3600) % 24) as u32;
    let mut days = secs / 86400;
    let wday = (days % 7) as usize;

    let mut year: u64 = 1970;
    loop {
        let ydays = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
        if days < ydays {
            break;
        }
        days -= ydays;
        year += 1;
    }
    let leap = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        1
    } else {
        0
    };
    let mut mon = 0usize;
    while mon < 11 && days >= DAYS_PER_MONTH[leap][mon] {
        days -= DAYS_PER_MONTH[leap][mon];
        mon += 1;
    }
    let mday = days + 1;

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        WDAY[wday], mday, MON[mon], year, hour, min, sec
    )
}

fn strip_prefix_segment(path: &str, prefix: &str) -> Option<String> {
    let rest = path.strip_prefix(prefix)?;
    // 去掉可能的 query string
    let segment = rest.split('?').next().unwrap_or(rest);
    if segment.is_empty() {
        None
    } else {
        Some(segment.to_string())
    }
}

fn parse_query_params(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

fn parse_header_u64(headers: &http::HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
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

fn json_response(
    value: &serde_json::Value,
) -> Result<http::Response<BoxBody<Bytes, ServerError>>, NdnError> {
    let body_str = serde_json::to_string(value)
        .map_err(|e| NdnError::Internal(format!("serialize JSON: {e}")))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json; charset=utf-8")
        .body(full_body(Bytes::from(body_str)))
        .map_err(|e| NdnError::Internal(format!("build response: {e}")))
}

fn build_error_response(
    status: StatusCode,
    error_code: &str,
    message: &str,
    is_tus: bool,
    is_version_mismatch: bool,
) -> http::Response<BoxBody<Bytes, ServerError>> {
    let body = serde_json::json!({
        "error": error_code,
        "message": message,
    })
    .to_string();

    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8");

    // tus protocol: every non-OPTIONS response must carry Tus-Resumable
    if is_tus {
        builder = builder.header(H_TUS_RESUMABLE, TUS_RESUMABLE);
    }
    // tus protocol: 412 version mismatch must also include Tus-Version
    if is_version_mismatch {
        builder = builder.header(H_TUS_VERSION, TUS_RESUMABLE);
    }

    builder
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(empty_body())
                .unwrap()
        })
}

fn ndm_error_to_status(e: &NdnError) -> (StatusCode, String) {
    match e {
        NdnError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found".to_string()),
        NdnError::NotReady(_) => (StatusCode::GONE, "session_expired".to_string()),
        NdnError::InvalidParam(_) => (StatusCode::BAD_REQUEST, "invalid_param".to_string()),
        NdnError::InvalidData(_) => (StatusCode::BAD_REQUEST, "invalid_data".to_string()),
        NdnError::InvalidId(_) => (StatusCode::BAD_REQUEST, "invalid_id".to_string()),
        NdnError::VerifyError(msg) if msg.contains("Tus-Resumable") => (
            StatusCode::PRECONDITION_FAILED,
            "precondition_failed".to_string(),
        ),
        NdnError::VerifyError(_) => (StatusCode::CONFLICT, "offset_conflict".to_string()),
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
