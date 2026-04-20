//! `NdnDirServer` — a static-dir-style HTTP server that serves `cyfs://`-style
//! R-Link and O-Link requests against a semantic root directory backed by a
//! [`NamedStoreMgr`].
//!
//! Design overview (see `doc/ndn_dir_router 需求.md` for the source spec):
//! - **O-Link**: requests whose hostname label or first path segment parses as
//!   an [`ObjId`] are resolved directly from the underlying `NamedStoreMgr`.
//! - **R-Link**: requests are resolved against `semantic_root`. Objectified
//!   entries carry a sidecar `<name>.cyobj` record that binds the semantic
//!   path to a `FileObject` / `DirObject` via an optional `PathObject` JWT.
//! - **Auto-objectification**: the scanner walks `semantic_root`, produces
//!   `<name>.cyobj` sidecars for newly added files, and pushes the chunk into
//!   `NamedStoreMgr` either by local link (`LocalLink` mode) or by stream
//!   upload (`InStore` mode, original file deleted after success).
//!
//! This module is deliberately self-contained: it does not pull in
//! `cyfs_gateway_lib` or any zone-gateway plumbing, so the HTTP body type is
//! the plain `BoxBody<Bytes, std::io::Error>`. Embedders are expected to adapt
//! their outer body error to `std::io::Error` at the boundary.

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use jsonwebtoken::EncodingKey;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

use ndn_lib::{
    apply_cyfs_resp_headers, build_named_object_by_json, caculate_qcid_from_file,
    calculate_file_chunk_id, named_obj_to_jwt, CYFSHttpRespHeaders, ChunkId, ChunkReader,
    ChunkType, CyfsParent, FileObject, NdnError, NdnResult, ObjId, PathObject, OBJ_TYPE_DIR,
    OBJ_TYPE_FILE,
};
use named_store::{ChunkLocalInfo, NamedStoreMgr};

const SIDECAR_SUFFIX: &str = ".cyobj";
const DIROBJ_META_FILE: &str = "dirobj.meta";
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);
const STREAM_BUF_SIZE: usize = 64 * 1024;
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";
const CONTENT_TYPE_CYFS_OBJECT: &str = "application/cyfs-object";

type ServerBody = BoxBody<Bytes, std::io::Error>;

// =====================================================================
// Config / mode
// =====================================================================

/// Persistence mode for auto-objectified files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdnDirServerMode {
    /// Keep the original file on disk and register it in the store as a
    /// local-link chunk. The on-disk file remains the single source of truth
    /// for the chunk bytes.
    LocalLink,
    /// Stream the file into the store and delete the original afterwards.
    /// Only the `<name>.cyobj` sidecar remains on disk.
    InStore,
}

/// Builder-style configuration for [`NdnDirServer`].
#[derive(Clone)]
pub struct NdnDirServerConfig {
    pub semantic_root: PathBuf,
    pub store_mgr: Arc<NamedStoreMgr>,
    pub mode: NdnDirServerMode,
    /// URL prefix to strip before resolving against `semantic_root`, e.g.
    /// `"/ndn"` — requests to `/ndn/readme.txt` then resolve `/readme.txt`.
    /// Leading and trailing slashes are normalized.
    pub url_prefix: String,
    /// When `true`, the leading hostname label is inspected for a base32 ObjId
    /// before falling back to path-based O-Link lookup.
    pub obj_id_in_host: bool,
    /// Optional private key used to mint `PathObject` JWTs during auto-
    /// objectification. Without a key, sidecars are still produced but omit
    /// the `path_obj_jwt` field; R-Link responses will lack `cyfs-path-obj`.
    pub signing_key: Option<EncodingKey>,
    pub signing_kid: Option<String>,
    /// Interval at which [`NdnDirServer::spawn_scanner`] wakes up.
    pub scan_interval: Duration,
}

impl NdnDirServerConfig {
    pub fn new(
        semantic_root: impl Into<PathBuf>,
        store_mgr: Arc<NamedStoreMgr>,
        mode: NdnDirServerMode,
    ) -> Self {
        Self {
            semantic_root: semantic_root.into(),
            store_mgr,
            mode,
            url_prefix: String::new(),
            obj_id_in_host: false,
            signing_key: None,
            signing_kid: None,
            scan_interval: DEFAULT_SCAN_INTERVAL,
        }
    }

    pub fn url_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.url_prefix = prefix.into();
        self
    }

    pub fn obj_id_in_host(mut self, enabled: bool) -> Self {
        self.obj_id_in_host = enabled;
        self
    }

    pub fn signing_key(mut self, key: EncodingKey, kid: Option<String>) -> Self {
        self.signing_key = Some(key);
        self.signing_kid = kid;
        self
    }

    pub fn scan_interval(mut self, interval: Duration) -> Self {
        self.scan_interval = interval;
        self
    }
}

// =====================================================================
// Sidecar record
// =====================================================================

/// On-disk representation of a `<name>.cyobj` sidecar.
///
/// A sidecar is authoritative for the semantic-path binding: the server
/// reads it to build CYFS response headers and never recomputes the object
/// id at request time. The scanner refreshes it when the originating file's
/// quick-hash (QCID) changes.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SidecarRecord {
    /// ObjType of the embedded object (e.g. `cyfile`, `cydir`).
    pub obj_type: String,
    /// Canonical ObjId of the embedded object.
    pub obj_id: String,
    /// Canonical JSON of the NamedObject.
    pub obj_json: serde_json::Value,
    /// Signed `PathObject` JWT binding the semantic path to `obj_id`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub path_obj_jwt: Option<String>,
    /// Quick-hash of the source file at the time of objectification. Used by
    /// the scanner to decide whether the sidecar is still current. Absent for
    /// sidecars produced from templates / directory objects.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_qcid: Option<String>,
    /// Last modification timestamp of the source file, in unix seconds.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_mtime: Option<u64>,
    /// Size of the source file in bytes.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_size: Option<u64>,
}

impl SidecarRecord {
    fn read_from(path: &Path) -> NdnResult<Self> {
        let bytes = std::fs::read(path).map_err(|e| {
            NdnError::IoError(format!("read sidecar {} failed: {}", path.display(), e))
        })?;
        serde_json::from_slice(&bytes).map_err(|e| {
            NdnError::DecodeError(format!("parse sidecar {} failed: {}", path.display(), e))
        })
    }

    fn write_to(&self, path: &Path) -> NdnResult<()> {
        let bytes = serde_json::to_vec_pretty(self).map_err(|e| {
            NdnError::Internal(format!("serialize sidecar failed: {}", e))
        })?;
        std::fs::write(path, bytes).map_err(|e| {
            NdnError::IoError(format!("write sidecar {} failed: {}", path.display(), e))
        })
    }
}

// =====================================================================
// Server
// =====================================================================

#[derive(Clone)]
pub struct NdnDirServer {
    config: Arc<NdnDirServerConfig>,
}

impl NdnDirServer {
    pub fn new(config: NdnDirServerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    pub fn config(&self) -> &NdnDirServerConfig {
        &self.config
    }

    pub fn store_mgr(&self) -> &Arc<NamedStoreMgr> {
        &self.config.store_mgr
    }

    /// Core HTTP entry point. Converts any internal error into a JSON error
    /// response so the outer transport never has to translate `NdnError`.
    pub async fn serve_request<B>(&self, request: Request<B>) -> Response<ServerBody>
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: std::fmt::Display,
    {
        match self.route_request(request).await {
            Ok(resp) => resp,
            Err(e) => {
                let status = ndn_error_to_status(&e);
                warn!("ndn_dir_server: {} -> {}", status, e);
                build_error_response(status, &e.to_string())
            }
        }
    }

    async fn route_request<B>(&self, request: Request<B>) -> NdnResult<Response<ServerBody>>
    where
        B: http_body::Body + Send + 'static,
    {
        if request.method() != Method::GET && request.method() != Method::HEAD {
            return Err(NdnError::Unsupported(format!(
                "method {} is not supported",
                request.method()
            )));
        }

        let head_only = request.method() == Method::HEAD;
        let host = request
            .headers()
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(':').next().unwrap_or(s).to_string());
        let uri_path = request.uri().path().to_string();
        let range_header = request
            .headers()
            .get(http::header::RANGE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // 1. O-Link via hostname label (obj_id_in_host mode).
        if self.config.obj_id_in_host {
            if let Some(host) = host.as_deref() {
                if let Some(label) = host.split('.').next() {
                    if let Ok(obj_id) = ObjId::from_hostname(label) {
                        return self
                            .serve_obj(&obj_id, head_only, range_header.as_deref())
                            .await;
                    }
                }
            }
        }

        // 2. Strip URL prefix and decode the semantic path.
        let rel_path = self.strip_url_prefix(&uri_path);
        let segments: Vec<String> = rel_path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| decode_url_segment(s))
            .collect();

        // 3. O-Link via first path segment that parses as an ObjId.
        if let Some(first) = segments.first() {
            if let Ok(obj_id) = ObjId::new(first) {
                return self
                    .serve_obj(&obj_id, head_only, range_header.as_deref())
                    .await;
            }
        }

        // 4. R-Link: resolve against semantic root.
        self.serve_r_link(&segments, head_only, range_header.as_deref())
            .await
    }

    /// Normalize URL prefix comparison: both sides are matched as `/segment/`.
    fn strip_url_prefix<'a>(&self, uri_path: &'a str) -> &'a str {
        let prefix = self.config.url_prefix.trim_matches('/');
        if prefix.is_empty() {
            return uri_path;
        }
        let with_slashes = format!("/{}", prefix);
        if let Some(rest) = uri_path.strip_prefix(&with_slashes) {
            if rest.is_empty() {
                return "/";
            }
            if rest.starts_with('/') {
                return rest;
            }
        }
        uri_path
    }

    // ---------------- O-Link ----------------

    async fn serve_obj(
        &self,
        obj_id: &ObjId,
        head_only: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        if obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(obj_id);
            return self.serve_chunk(chunk_id, head_only, range_header).await;
        }

        let obj_str = self.config.store_mgr.get_object(obj_id).await?;
        let mut cyfs_headers = CYFSHttpRespHeaders::default();
        cyfs_headers.obj_id = Some(obj_id.clone());

        let body_bytes = Bytes::from(obj_str);
        let len = body_bytes.len();
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_CYFS_OBJECT)
            .header(http::header::CONTENT_LENGTH, len);
        apply_cyfs_headers(&mut builder, &cyfs_headers)?;

        let body = if head_only {
            empty_body()
        } else {
            full_body(body_bytes)
        };
        builder
            .body(body)
            .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
    }

    async fn serve_chunk(
        &self,
        chunk_id: ChunkId,
        head_only: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        let (_, total_size) = self.config.store_mgr.query_chunk_state(&chunk_id).await?;
        let offset = parse_range_offset(range_header).unwrap_or(0);
        if offset > total_size {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        let remaining = total_size - offset;

        let mut cyfs_headers = CYFSHttpRespHeaders::default();
        cyfs_headers.obj_id = Some(chunk_id.to_obj_id());
        cyfs_headers.chunk_size = Some(total_size);

        let mut builder = Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_OCTET)
            .header(http::header::ACCEPT_RANGES, "bytes")
            .header(http::header::CONTENT_LENGTH, remaining);
        if offset == 0 {
            builder = builder.status(StatusCode::OK);
        } else {
            builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
                http::header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", offset, total_size - 1, total_size),
            );
        }
        apply_cyfs_headers(&mut builder, &cyfs_headers)?;

        if head_only {
            return builder
                .body(empty_body())
                .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)));
        }

        let (reader, _) = self
            .config
            .store_mgr
            .open_chunk_reader(&chunk_id, offset)
            .await?;
        let body = chunk_reader_to_body(reader, remaining);
        builder
            .body(body)
            .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
    }

    // ---------------- R-Link ----------------

    async fn serve_r_link(
        &self,
        segments: &[String],
        head_only: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        let fs_path = safe_resolve_path(&self.config.semantic_root, segments)?;
        let sidecar_path = append_extension(&fs_path, SIDECAR_SUFFIX);

        // Directly requested sidecar: return the raw JSON so clients can
        // inspect metadata. This is useful for debugging / template flows.
        if fs_path.extension().and_then(|s| s.to_str()) == Some("cyobj")
            && fs_path.is_file()
        {
            return serve_local_file_bytes(&fs_path, head_only, range_header).await;
        }

        if sidecar_path.is_file() {
            let record = SidecarRecord::read_from(&sidecar_path)?;
            return self
                .serve_sidecar(&record, &fs_path, head_only, range_header)
                .await;
        }

        // No sidecar yet — fall back to the raw file if it exists. Responses
        // in this path carry no CYFS headers; the caller must treat them as
        // untrusted until the scanner produces a sidecar.
        if fs_path.is_file() {
            debug!(
                "ndn_dir_server: serving unobjectified file {}",
                fs_path.display()
            );
            return serve_local_file_bytes(&fs_path, head_only, range_header).await;
        }

        Err(NdnError::NotFound(format!(
            "no object or file bound to /{}",
            segments.join("/")
        )))
    }

    async fn serve_sidecar(
        &self,
        record: &SidecarRecord,
        fs_path: &Path,
        head_only: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        match record.obj_type.as_str() {
            OBJ_TYPE_FILE => {
                let file_obj: FileObject =
                    serde_json::from_value(record.obj_json.clone()).map_err(|e| {
                        NdnError::InvalidData(format!("parse FileObject from sidecar: {}", e))
                    })?;
                self.serve_file_object_sidecar(record, file_obj, fs_path, head_only, range_header)
                    .await
            }
            OBJ_TYPE_DIR => {
                // Directory objects serve their canonical JSON; streaming a
                // directory walk is out-of-scope for v1.
                let body_bytes = Bytes::from(serde_json::to_vec(&record.obj_json).map_err(|e| {
                    NdnError::Internal(format!("serialize dir object failed: {}", e))
                })?);
                let len = body_bytes.len();
                let mut headers = CYFSHttpRespHeaders::default();
                headers.obj_id = Some(ObjId::new(&record.obj_id)?);
                if let Some(jwt) = record.path_obj_jwt.clone() {
                    headers.path_obj = Some(jwt);
                }
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, CONTENT_TYPE_CYFS_OBJECT)
                    .header(http::header::CONTENT_LENGTH, len);
                apply_cyfs_headers(&mut builder, &headers)?;
                let body = if head_only { empty_body() } else { full_body(body_bytes) };
                builder
                    .body(body)
                    .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
            }
            other => Err(NdnError::Unsupported(format!(
                "sidecar obj_type {} is not supported yet",
                other
            ))),
        }
    }

    async fn serve_file_object_sidecar(
        &self,
        record: &SidecarRecord,
        file_obj: FileObject,
        fs_path: &Path,
        head_only: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        let content_obj_id = ObjId::new(file_obj.content.as_str())?;

        let mut cyfs_headers = CYFSHttpRespHeaders::default();
        cyfs_headers.obj_id = Some(content_obj_id.clone());
        cyfs_headers.chunk_size = Some(file_obj.size);
        if let Some(jwt) = record.path_obj_jwt.clone() {
            cyfs_headers.path_obj = Some(jwt);
        }
        // Inline the FileObject itself via cyfs-parents-0 so the client can
        // short-circuit to raw-chunk pulling without an extra round-trip.
        cyfs_headers.parents.push(CyfsParent::Json(
            serde_json::to_string(&record.obj_json).map_err(|e| {
                NdnError::Internal(format!("serialize FileObject failed: {}", e))
            })?,
        ));

        if !content_obj_id.is_chunk() {
            // ChunkList fan-out isn't materialized inline yet — fall back to
            // serving the FileObject JSON itself.
            let body_bytes = Bytes::from(serde_json::to_vec(&record.obj_json).map_err(|e| {
                NdnError::Internal(format!("serialize FileObject failed: {}", e))
            })?);
            let len = body_bytes.len();
            cyfs_headers.obj_id = Some(ObjId::new(&record.obj_id)?);
            cyfs_headers.chunk_size = None;
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, CONTENT_TYPE_CYFS_OBJECT)
                .header(http::header::CONTENT_LENGTH, len);
            apply_cyfs_headers(&mut builder, &cyfs_headers)?;
            let body = if head_only { empty_body() } else { full_body(body_bytes) };
            return builder
                .body(body)
                .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)));
        }

        let chunk_id = ChunkId::from_obj_id(&content_obj_id);

        // Prefer the store: in LocalLink mode the store will re-open the
        // original file, in InStore mode it holds the only copy.
        let (reader, total_size) = match self.config.store_mgr.open_chunk_reader(&chunk_id, 0).await
        {
            Ok(v) => v,
            Err(NdnError::NotFound(_)) if fs_path.is_file() => {
                // Stale store entry or scanner hasn't finalized yet — serve
                // the raw file instead of returning 404.
                let meta = tokio::fs::metadata(fs_path).await.map_err(|e| {
                    NdnError::IoError(format!(
                        "stat {} failed: {}",
                        fs_path.display(),
                        e
                    ))
                })?;
                let file = tokio::fs::File::open(fs_path).await.map_err(|e| {
                    NdnError::IoError(format!("open {} failed: {}", fs_path.display(), e))
                })?;
                let reader: ChunkReader = Box::pin(file);
                (reader, meta.len())
            }
            Err(e) => return Err(e),
        };

        let offset = parse_range_offset(range_header).unwrap_or(0);
        if offset > total_size {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        let reader = if offset == 0 {
            reader
        } else {
            // Re-open at offset; the store supports random access, but the
            // local-file fallback does not — skip manually in that case.
            drop(reader);
            let (r, _) = self
                .config
                .store_mgr
                .open_chunk_reader(&chunk_id, offset)
                .await?;
            r
        };
        let remaining = total_size - offset;

        let mut builder = Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_OCTET)
            .header(http::header::ACCEPT_RANGES, "bytes")
            .header(http::header::CONTENT_LENGTH, remaining);
        if offset == 0 {
            builder = builder.status(StatusCode::OK);
        } else {
            builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
                http::header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", offset, total_size - 1, total_size),
            );
        }
        apply_cyfs_headers(&mut builder, &cyfs_headers)?;

        let body = if head_only {
            empty_body()
        } else {
            chunk_reader_to_body(reader, remaining)
        };
        builder
            .body(body)
            .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
    }

    // ---------------- Auto-objectification ----------------

    /// Walk `semantic_root` and (re)build sidecars for every regular file that
    /// is missing or out-of-date. Returns the number of files objectified in
    /// this pass.
    pub async fn scan_and_objectify(&self) -> NdnResult<usize> {
        let root = self.config.semantic_root.clone();
        if !root.is_dir() {
            return Err(NdnError::InvalidParam(format!(
                "semantic root {} is not a directory",
                root.display()
            )));
        }

        let mut files = Vec::new();
        collect_files_recursive(&root, &mut files).map_err(|e| {
            NdnError::IoError(format!("scan {} failed: {}", root.display(), e))
        })?;

        let mut processed = 0usize;
        for entry in files {
            match self.objectify_one(&entry).await {
                Ok(true) => processed += 1,
                Ok(false) => {}
                Err(e) => {
                    warn!(
                        "ndn_dir_server: objectify {} failed: {}",
                        entry.display(),
                        e
                    );
                }
            }
        }
        Ok(processed)
    }

    /// Spawn a background task that calls [`scan_and_objectify`] on each tick.
    /// The task exits silently when the underlying store is dropped; callers
    /// that need explicit control should keep the returned handle.
    pub fn spawn_scanner(&self) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        let interval = this.config.scan_interval;
        tokio::spawn(async move {
            // Run an immediate pass, then on a fixed interval.
            loop {
                if let Err(e) = this.scan_and_objectify().await {
                    warn!("ndn_dir_server: scan pass failed: {}", e);
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Objectify a single file. Returns `true` when a new sidecar was written
    /// (or an existing one was refreshed), `false` when the file is already
    /// current and requires no action.
    async fn objectify_one(&self, file_path: &Path) -> NdnResult<bool> {
        let file_name = match file_path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n.to_string(),
            None => return Ok(false),
        };

        // Skip sidecar files, directory markers, and hidden files.
        if file_name.ends_with(SIDECAR_SUFFIX) || file_name == DIROBJ_META_FILE {
            return Ok(false);
        }
        if file_name.starts_with('.') {
            return Ok(false);
        }

        let sidecar_path = append_extension(file_path, SIDECAR_SUFFIX);
        let meta = tokio::fs::metadata(file_path).await.map_err(|e| {
            NdnError::IoError(format!("stat {} failed: {}", file_path.display(), e))
        })?;
        if !meta.is_file() {
            return Ok(false);
        }
        let size = meta.len();
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Decide whether an existing sidecar is already current. For
        // performance we only recompute QCID when the cheap mtime/size check
        // indicates a possible change.
        if sidecar_path.is_file() {
            if let Ok(existing) = SidecarRecord::read_from(&sidecar_path) {
                if existing.source_size == Some(size)
                    && existing.source_mtime == Some(mtime)
                {
                    return Ok(false);
                }
                // mtime / size drifted — confirm via QCID before recomputing.
                let qcid = caculate_qcid_from_file(file_path).await?;
                if existing.source_qcid.as_deref() == Some(qcid.to_string().as_str()) {
                    return Ok(false);
                }
            }
        }

        info!(
            "ndn_dir_server: objectifying {} ({} bytes)",
            file_path.display(),
            size
        );

        let (chunk_id, chunk_size) =
            calculate_file_chunk_id(file_path.to_string_lossy().as_ref(), ChunkType::Mix256)
                .await?;
        if chunk_size != size {
            return Err(NdnError::InvalidData(format!(
                "size drift on {}: stat {} vs read {}",
                file_path.display(),
                size,
                chunk_size
            )));
        }

        let file_obj = FileObject::new(file_name.clone(), chunk_size, chunk_id.to_string());
        let file_json = serde_json::to_value(&file_obj).map_err(|e| {
            NdnError::Internal(format!("serialize FileObject failed: {}", e))
        })?;
        let (file_obj_id, _) = build_named_object_by_json(OBJ_TYPE_FILE, &file_json);

        // Register the chunk with the store according to mode. We do this
        // before writing the sidecar so a crash leaves the store consistent
        // and the sidecar absent — the next scan pass will retry.
        self.register_chunk_in_store(file_path, &chunk_id, chunk_size)
            .await?;

        // Mint a PathObject JWT for the semantic binding if we have a key.
        let path_obj_jwt = match self.config.signing_key.as_ref() {
            Some(key) => {
                let semantic_path = self.semantic_path_for(file_path);
                let path_obj = PathObject::new(semantic_path, file_obj_id.clone());
                let path_json = serde_json::to_value(&path_obj).map_err(|e| {
                    NdnError::Internal(format!("serialize PathObject failed: {}", e))
                })?;
                Some(named_obj_to_jwt(
                    &path_json,
                    key,
                    self.config.signing_kid.clone(),
                )?)
            }
            None => None,
        };

        let qcid = caculate_qcid_from_file(file_path).await.ok();
        let record = SidecarRecord {
            obj_type: OBJ_TYPE_FILE.to_string(),
            obj_id: file_obj_id.to_string(),
            obj_json: file_json,
            path_obj_jwt,
            source_qcid: qcid.map(|c| c.to_string()),
            source_mtime: Some(mtime),
            source_size: Some(size),
        };
        record.write_to(&sidecar_path)?;

        // Only now is it safe to delete the original (InStore mode).
        if matches!(self.config.mode, NdnDirServerMode::InStore) {
            if let Err(e) = tokio::fs::remove_file(file_path).await {
                warn!(
                    "ndn_dir_server: failed to remove source {} after in-store upload: {}",
                    file_path.display(),
                    e
                );
            }
        }

        Ok(true)
    }

    async fn register_chunk_in_store(
        &self,
        file_path: &Path,
        chunk_id: &ChunkId,
        chunk_size: u64,
    ) -> NdnResult<()> {
        if self.config.store_mgr.have_chunk(chunk_id).await {
            return Ok(());
        }

        match self.config.mode {
            NdnDirServerMode::LocalLink => {
                let qcid = caculate_qcid_from_file(file_path).await?;
                let meta = tokio::fs::metadata(file_path).await.map_err(|e| {
                    NdnError::IoError(format!("stat {} failed: {}", file_path.display(), e))
                })?;
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                self.config
                    .store_mgr
                    .add_chunk_by_link_to_local_file(
                        chunk_id,
                        chunk_size,
                        &ChunkLocalInfo {
                            path: file_path.to_string_lossy().to_string(),
                            qcid: qcid.to_string(),
                            last_modify_time: mtime,
                            range: None,
                        },
                    )
                    .await?;
            }
            NdnDirServerMode::InStore => {
                let file = tokio::fs::File::open(file_path).await.map_err(|e| {
                    NdnError::IoError(format!("open {} failed: {}", file_path.display(), e))
                })?;
                let reader: ChunkReader = Box::pin(file);
                self.config
                    .store_mgr
                    .put_chunk_by_reader(chunk_id, chunk_size, reader)
                    .await?;
            }
        }
        Ok(())
    }

    fn semantic_path_for(&self, file_path: &Path) -> String {
        let rel = file_path
            .strip_prefix(&self.config.semantic_root)
            .unwrap_or(file_path);
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        if rel_str.starts_with('/') {
            rel_str
        } else {
            format!("/{}", rel_str)
        }
    }
}

// =====================================================================
// Helpers
// =====================================================================

fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_files_recursive(&path, out)?;
        } else if file_type.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

/// Resolve `segments` against `root` while rejecting `..` and absolute paths.
fn safe_resolve_path(root: &Path, segments: &[String]) -> NdnResult<PathBuf> {
    let mut out = root.to_path_buf();
    for seg in segments {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." || seg.contains('/') || seg.contains('\\') {
            return Err(NdnError::InvalidParam(format!(
                "illegal path segment: {}",
                seg
            )));
        }
        out.push(seg);
    }
    Ok(out)
}

fn append_extension(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

fn decode_url_segment(seg: &str) -> String {
    // Lightweight percent-decoding — avoids pulling a new dependency.
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = from_hex(bytes[i + 1]);
            let lo = from_hex(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| seg.to_string())
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_range_offset(raw: Option<&str>) -> Option<u64> {
    let raw = raw?.trim();
    let rest = raw.strip_prefix("bytes=")?;
    let start_str = rest.split('-').next()?;
    start_str.parse::<u64>().ok()
}

fn apply_cyfs_headers(
    builder: &mut http::response::Builder,
    headers: &CYFSHttpRespHeaders,
) -> NdnResult<()> {
    let mut map: reqwest::header::HeaderMap = reqwest::header::HeaderMap::new();
    apply_cyfs_resp_headers(headers, &mut map)?;
    let response_headers = builder.headers_mut().ok_or_else(|| {
        NdnError::Internal("response builder has no headers".to_string())
    })?;
    for (k, v) in map.iter() {
        let name = http::header::HeaderName::from_bytes(k.as_ref()).map_err(|e| {
            NdnError::Internal(format!("invalid header name {}: {}", k, e))
        })?;
        let value = HeaderValue::from_bytes(v.as_bytes()).map_err(|e| {
            NdnError::Internal(format!("invalid header value: {}", e))
        })?;
        response_headers.append(name, value);
    }
    Ok(())
}

fn empty_body() -> ServerBody {
    Full::<Bytes>::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(data: Bytes) -> ServerBody {
    Full::new(data).map_err(|never| match never {}).boxed()
}

fn chunk_reader_to_body(reader: ChunkReader, total: u64) -> ServerBody {
    let rx = chunk_reader_to_channel(reader, total);
    ReceiverBody { rx }.boxed()
}

struct ReceiverBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, std::io::Error>>,
}

impl http_body::Body for ReceiverBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
    }
}

fn chunk_reader_to_channel(
    mut reader: ChunkReader,
    total: u64,
) -> mpsc::Receiver<Result<Frame<Bytes>, std::io::Error>> {
    let (tx, rx) = mpsc::channel(2);
    tokio::spawn(async move {
        let mut sent: u64 = 0;
        while sent < total {
            let to_read = std::cmp::min(STREAM_BUF_SIZE as u64, total - sent) as usize;
            let mut buf = vec![0u8; to_read];
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.truncate(n);
                    sent += n as u64;
                    if tx.send(Ok(Frame::data(Bytes::from(buf)))).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });
    rx
}

async fn serve_local_file_bytes(
    path: &Path,
    head_only: bool,
    range_header: Option<&str>,
) -> NdnResult<Response<ServerBody>> {
    let meta = tokio::fs::metadata(path).await.map_err(|e| {
        NdnError::IoError(format!("stat {} failed: {}", path.display(), e))
    })?;
    let total = meta.len();
    let offset = parse_range_offset(range_header).unwrap_or(0);
    if offset > total {
        return Err(NdnError::OffsetTooLarge(format!(
            "range offset {} > file size {}",
            offset, total
        )));
    }
    let remaining = total - offset;

    let mut builder = Response::builder()
        .header(http::header::CONTENT_TYPE, CONTENT_TYPE_OCTET)
        .header(http::header::ACCEPT_RANGES, "bytes")
        .header(http::header::CONTENT_LENGTH, remaining);
    if offset == 0 {
        builder = builder.status(StatusCode::OK);
    } else {
        builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
            http::header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", offset, total - 1, total),
        );
    }

    if head_only {
        return builder
            .body(empty_body())
            .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)));
    }

    let mut file = tokio::fs::File::open(path).await.map_err(|e| {
        NdnError::IoError(format!("open {} failed: {}", path.display(), e))
    })?;
    if offset > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|e| {
            NdnError::IoError(format!("seek {} failed: {}", path.display(), e))
        })?;
    }
    let reader: ChunkReader = Box::pin(file);
    let body = chunk_reader_to_body(reader, remaining);
    builder
        .body(body)
        .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
}

fn ndn_error_to_status(e: &NdnError) -> StatusCode {
    match e {
        NdnError::NotFound(_) => StatusCode::NOT_FOUND,
        NdnError::InvalidParam(_)
        | NdnError::InvalidData(_)
        | NdnError::InvalidId(_)
        | NdnError::InvalidObjType(_) => StatusCode::BAD_REQUEST,
        NdnError::VerifyError(_) => StatusCode::CONFLICT,
        NdnError::PermissionDenied(_) => StatusCode::FORBIDDEN,
        NdnError::AlreadyExists(_) => StatusCode::CONFLICT,
        NdnError::OffsetTooLarge(_) => StatusCode::RANGE_NOT_SATISFIABLE,
        NdnError::Unsupported(_) => StatusCode::METHOD_NOT_ALLOWED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn build_error_response(status: StatusCode, message: &str) -> Response<ServerBody> {
    let body = serde_json::json!({ "error": message }).to_string();
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(empty_body())
                .unwrap()
        })
}

