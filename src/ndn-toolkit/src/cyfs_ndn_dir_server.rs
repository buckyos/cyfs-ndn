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
    ChunkType, CyfsParent, DirObject, FileObject, NdnError, NdnResult, ObjId, PathObject,
    OBJ_TYPE_CHUNK_LIST, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
};
use named_store::{ChunkLocalInfo, NamedStoreMgr};

const INNER_PATH_DELIMITER: &str = "/@/";

const SIDECAR_SUFFIX: &str = ".cyobj";
const DIROBJ_META_FILE: &str = "dirobj.meta";
const OBJECT_TEMPLATE_FILE: &str = "object.template";
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);
const STREAM_BUF_SIZE: usize = 64 * 1024;
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";
const CONTENT_TYPE_CYFS_OBJECT: &str = "application/cyfs-object";

type ServerBody = BoxBody<Bytes, std::io::Error>;

/// Parsed `object.template` — a map from obj-type to an arbitrary JSON value
/// whose `meta` field (if any) provides per-type metadata defaults.
type ObjectTemplate = serde_json::Map<String, serde_json::Value>;

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
        let uri = request.uri();
        let uri_path = uri.path().to_string();
        let resp_raw = query_has_resp_raw(uri.query());
        let range_header = request
            .headers()
            .get(http::header::RANGE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // 1. Hostname O-Link (obj_id_in_host mode): the entire path after the
        //    host is inner_path applied to the hostname-encoded object.
        if self.config.obj_id_in_host {
            if let Some(host) = host.as_deref() {
                if let Some(label) = host.split('.').next() {
                    if let Ok(obj_id) = ObjId::from_hostname(label) {
                        let inner_steps = split_inner_path_whole(&uri_path);
                        let root = self.load_root_from_store(&obj_id).await?;
                        return self
                            .serve_resolved(
                                root,
                                inner_steps,
                                head_only,
                                resp_raw,
                                range_header.as_deref(),
                            )
                            .await;
                    }
                }
            }
        }

        // 2. Strip URL prefix, then split on "/@/" — the part before the first
        //    delimiter is the root locator, each subsequent part is a step.
        let rel_path = self.strip_url_prefix(&uri_path);
        let (root_part, inner_steps) = split_inner_path_with_root(rel_path);
        let root_segments: Vec<String> = root_part
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| decode_url_segment(s))
            .collect();

        // 3. O-Link via first path segment that parses as an ObjId. Any
        //    remaining root-part segments become fields of the first step.
        if let Some(first) = root_segments.first() {
            if let Ok(obj_id) = ObjId::new(first) {
                let mut steps = inner_steps;
                if root_segments.len() > 1 {
                    let first_fields: Vec<String> = root_segments[1..].to_vec();
                    steps.insert(0, first_fields);
                }
                let root = self.load_root_from_store(&obj_id).await?;
                return self
                    .serve_resolved(root, steps, head_only, resp_raw, range_header.as_deref())
                    .await;
            }
        }

        // 4. R-Link: resolve against semantic root.
        let root = self.load_root_from_semantic(&root_segments).await?;
        self.serve_resolved(root, inner_steps, head_only, resp_raw, range_header.as_deref())
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

    // ---------------- Root resolution ----------------

    /// Resolve an O-Link target to a [`RootState`]. Chunks stay as `Chunk`;
    /// NamedObjects are fetched from the store and parsed.
    async fn load_root_from_store(&self, obj_id: &ObjId) -> NdnResult<RootState> {
        if obj_id.is_chunk() {
            return Ok(RootState::Chunk(ChunkId::from_obj_id(obj_id)));
        }
        let obj_str = self.config.store_mgr.get_object(obj_id).await?;
        let obj_json: serde_json::Value = serde_json::from_str(&obj_str).map_err(|e| {
            NdnError::DecodeError(format!("parse store object {}: {}", obj_id, e))
        })?;
        Ok(RootState::NamedObj {
            obj_id: obj_id.clone(),
            obj_type: obj_id.obj_type.clone(),
            obj_json,
            path_obj_jwt: None,
            fs_path: None,
        })
    }

    /// Resolve an R-Link semantic path to a [`RootState`].
    async fn load_root_from_semantic(&self, segments: &[String]) -> NdnResult<RootState> {
        let fs_path = safe_resolve_path(&self.config.semantic_root, segments)?;
        let sidecar_path = append_extension(&fs_path, SIDECAR_SUFFIX);

        // Directly requested sidecar: return the raw JSON so clients can
        // inspect metadata. Useful for debugging / template flows.
        if fs_path.extension().and_then(|s| s.to_str()) == Some("cyobj")
            && fs_path.is_file()
        {
            return Ok(RootState::LocalObjFile(fs_path));
        }

        if sidecar_path.is_file() {
            let record = SidecarRecord::read_from(&sidecar_path)?;
            return Ok(RootState::NamedObj {
                obj_id: ObjId::new(&record.obj_id)?,
                obj_type: record.obj_type.clone(),
                obj_json: record.obj_json.clone(),
                path_obj_jwt: record.path_obj_jwt.clone(),
                fs_path: Some(fs_path),
            });
        }

        // No sidecar — fall back to raw file (untrusted).
        if fs_path.is_file() {
            debug!(
                "ndn_dir_server: serving unobjectified file {}",
                fs_path.display()
            );
            return Ok(RootState::RawFile(fs_path));
        }

        Err(NdnError::NotFound(format!(
            "no object or file bound to /{}",
            segments.join("/")
        )))
    }

    // ---------------- Unified response pipeline ----------------

    async fn serve_resolved(
        &self,
        root: RootState,
        steps: Vec<Vec<String>>,
        head_only: bool,
        resp_raw: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        match root {
            RootState::Chunk(chunk_id) => {
                if !steps.is_empty() {
                    return Err(NdnError::InvalidParam(
                        "inner_path is not applicable to a Chunk root".to_string(),
                    ));
                }
                let cyfs_headers = if resp_raw {
                    None
                } else {
                    let mut h = CYFSHttpRespHeaders::default();
                    h.obj_id = Some(chunk_id.to_obj_id());
                    Some(h)
                };
                self.build_chunk_response(&chunk_id, head_only, range_header, cyfs_headers, None)
                    .await
            }
            RootState::RawFile(path) | RootState::LocalObjFile(path) => {
                if !steps.is_empty() {
                    return Err(NdnError::InvalidParam(
                        "inner_path is not applicable to a raw-file root".to_string(),
                    ));
                }
                serve_local_file_bytes(&path, head_only, range_header).await
            }
            RootState::NamedObj {
                obj_id,
                obj_type,
                obj_json,
                path_obj_jwt,
                fs_path,
            } => {
                if steps.is_empty() {
                    return self
                        .serve_named_obj_root(
                            obj_id,
                            obj_type,
                            obj_json,
                            path_obj_jwt,
                            fs_path,
                            head_only,
                            resp_raw,
                            range_header,
                        )
                        .await;
                }
                let (final_value, parents) = self
                    .walk_inner_path(obj_type, obj_json, &steps)
                    .await?;
                self.serve_inner_path_final(
                    final_value,
                    parents,
                    path_obj_jwt,
                    head_only,
                    resp_raw,
                    range_header,
                )
                .await
            }
        }
    }

    /// Handle a NamedObject root without any inner_path. FileObject gets the
    /// `/@/content` convenience shortcut (chunk body + FileObject in
    /// `cyfs-parents-0`). All other types return canonical JSON.
    async fn serve_named_obj_root(
        &self,
        obj_id: ObjId,
        obj_type: String,
        obj_json: serde_json::Value,
        path_obj_jwt: Option<String>,
        fs_path: Option<PathBuf>,
        head_only: bool,
        resp_raw: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        // resp=raw always returns the raw NamedObject JSON with no CYFS
        // headers — the shortcut is disabled in this mode.
        if resp_raw {
            let (_, canonical) = build_named_object_by_json(&obj_type, &obj_json);
            return serve_raw_bytes(
                Bytes::from(canonical.into_bytes()),
                CONTENT_TYPE_CYFS_OBJECT,
                head_only,
            );
        }

        // ChunkList root: stream the concatenated chunk bytes directly,
        // mirroring the legacy `open_chunklist_reader` behavior.
        if obj_type == OBJ_TYPE_CHUNK_LIST {
            let mut cyfs_headers = CYFSHttpRespHeaders::default();
            cyfs_headers.obj_id = Some(obj_id.clone());
            cyfs_headers.path_obj = path_obj_jwt;
            return self
                .build_chunklist_response(
                    &obj_id,
                    head_only,
                    range_header,
                    Some(cyfs_headers),
                )
                .await;
        }

        // FileObject shortcut: stream the referenced chunk (or chunk list),
        // inline the FileObject as `cyfs-parents-0`.
        if obj_type == OBJ_TYPE_FILE {
            if let Ok(file_obj) = serde_json::from_value::<FileObject>(obj_json.clone()) {
                let content_obj_id = ObjId::new(file_obj.content.as_str())?;
                if content_obj_id.is_chunk() {
                    let (_, file_canonical) = build_named_object_by_json(OBJ_TYPE_FILE, &obj_json);
                    let mut cyfs_headers = CYFSHttpRespHeaders::default();
                    cyfs_headers.obj_id = Some(content_obj_id.clone());
                    cyfs_headers.chunk_size = Some(file_obj.size);
                    cyfs_headers.path_obj = path_obj_jwt;
                    cyfs_headers.parents.push(CyfsParent::Json(file_canonical));
                    let chunk_id = ChunkId::from_obj_id(&content_obj_id);
                    return self
                        .build_chunk_response(
                            &chunk_id,
                            head_only,
                            range_header,
                            Some(cyfs_headers),
                            fs_path.as_deref(),
                        )
                        .await;
                }
                if content_obj_id.is_chunk_list() {
                    let (_, file_canonical) = build_named_object_by_json(OBJ_TYPE_FILE, &obj_json);
                    let mut cyfs_headers = CYFSHttpRespHeaders::default();
                    cyfs_headers.obj_id = Some(content_obj_id.clone());
                    cyfs_headers.chunk_size = Some(file_obj.size);
                    cyfs_headers.path_obj = path_obj_jwt;
                    cyfs_headers.parents.push(CyfsParent::Json(file_canonical));
                    return self
                        .build_chunklist_response(
                            &content_obj_id,
                            head_only,
                            range_header,
                            Some(cyfs_headers),
                        )
                        .await;
                }
                // FileObject with nested non-chunk content — fall through to
                // the generic JSON response.
            }
        }

        // Generic NamedObject response.
        let (_, canonical) = build_named_object_by_json(&obj_type, &obj_json);
        let body_bytes = Bytes::from(canonical.into_bytes());
        let mut headers = CYFSHttpRespHeaders::default();
        headers.obj_id = Some(obj_id);
        headers.path_obj = path_obj_jwt;
        build_named_object_response(body_bytes, headers, head_only)
    }

    /// Walk `steps` against `root_obj_json`, following inner_path rules.
    /// Returns the final value and the parents chain (canonical JSON of root
    /// and each cross-segment dereferenced object, in order).
    async fn walk_inner_path(
        &self,
        root_obj_type: String,
        root_obj_json: serde_json::Value,
        steps: &[Vec<String>],
    ) -> NdnResult<(serde_json::Value, Vec<String>)> {
        let (_, root_canonical) = build_named_object_by_json(&root_obj_type, &root_obj_json);
        let mut parents: Vec<String> = vec![root_canonical];
        let mut cur_json = root_obj_json;

        for (i, fields) in steps.iter().enumerate() {
            let is_last = i + 1 == steps.len();
            let result = self.walk_segment(&cur_json, fields).await?;
            if is_last {
                return Ok((result, parents));
            }
            // Cross-segment boundary: must be an ObjectId.
            let next_id = ObjId::from_value(&result).map_err(|e| {
                NdnError::InvalidParam(format!(
                    "segment {} did not end with ObjectId: {}",
                    i, e
                ))
            })?;
            let json_str = self.config.store_mgr.get_object(&next_id).await?;
            let next_json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                NdnError::DecodeError(format!("parse intermediate object JSON: {}", e))
            })?;
            let (_, next_canonical) =
                build_named_object_by_json(next_id.obj_type.as_str(), &next_json);
            parents.push(next_canonical);
            cur_json = next_json;
        }
        unreachable!("steps is non-empty; last step returns")
    }

    /// Walk a single `/@/` segment: apply each field in turn, auto-deref any
    /// intermediate ObjectId (rule 4 of the protocol).
    async fn walk_segment(
        &self,
        start: &serde_json::Value,
        fields: &[String],
    ) -> NdnResult<serde_json::Value> {
        if fields.is_empty() {
            return Ok(start.clone());
        }
        let mut cur = start.clone();
        for (i, field) in fields.iter().enumerate() {
            let next = if let Ok(idx) = field.parse::<usize>() {
                cur.get(idx).cloned().ok_or_else(|| {
                    NdnError::NotFound(format!("inner_path index {} not found", idx))
                })?
            } else {
                cur.get(field.as_str()).cloned().ok_or_else(|| {
                    NdnError::NotFound(format!("inner_path field '{}' not found", field))
                })?
            };
            let has_more = i + 1 < fields.len();
            if has_more {
                if let Ok(obj_id) = ObjId::from_value(&next) {
                    let json_str = self.config.store_mgr.get_object(&obj_id).await?;
                    cur = serde_json::from_str(&json_str).map_err(|e| {
                        NdnError::DecodeError(format!(
                            "parse object JSON mid-segment: {}",
                            e
                        ))
                    })?;
                    continue;
                }
            }
            cur = next;
        }
        Ok(cur)
    }

    async fn serve_inner_path_final(
        &self,
        final_value: serde_json::Value,
        parents: Vec<String>,
        root_path_obj_jwt: Option<String>,
        head_only: bool,
        resp_raw: bool,
        range_header: Option<&str>,
    ) -> NdnResult<Response<ServerBody>> {
        if let Ok(obj_id) = ObjId::from_value(&final_value) {
            if resp_raw {
                // Do not dereference — return the ObjectId as a JSON string.
                let body = serde_json::to_string(&final_value).map_err(|e| {
                    NdnError::Internal(format!("serialize final value: {}", e))
                })?;
                return serve_raw_bytes(
                    Bytes::from(body.into_bytes()),
                    "application/json; charset=utf-8",
                    head_only,
                );
            }
            if obj_id.is_chunk() {
                let chunk_id = ChunkId::from_obj_id(&obj_id);
                let (_, total_size) =
                    self.config.store_mgr.query_chunk_state(&chunk_id).await?;
                let mut cyfs_headers = CYFSHttpRespHeaders::default();
                cyfs_headers.obj_id = Some(chunk_id.to_obj_id());
                cyfs_headers.chunk_size = Some(total_size);
                cyfs_headers.path_obj = root_path_obj_jwt;
                cyfs_headers.parents =
                    parents.into_iter().map(CyfsParent::Json).collect();
                return self
                    .build_chunk_response(
                        &chunk_id,
                        head_only,
                        range_header,
                        Some(cyfs_headers),
                        None,
                    )
                    .await;
            }
            // NamedObject deref: body = its canonical JSON.
            let json_str = self.config.store_mgr.get_object(&obj_id).await?;
            let obj_value: serde_json::Value =
                serde_json::from_str(&json_str).map_err(|e| {
                    NdnError::DecodeError(format!("parse final named object: {}", e))
                })?;
            let (_, canonical) =
                build_named_object_by_json(obj_id.obj_type.as_str(), &obj_value);
            let body_bytes = Bytes::from(canonical.into_bytes());
            let mut cyfs_headers = CYFSHttpRespHeaders::default();
            cyfs_headers.obj_id = Some(obj_id);
            cyfs_headers.path_obj = root_path_obj_jwt;
            cyfs_headers.parents =
                parents.into_iter().map(CyfsParent::Json).collect();
            return build_named_object_response(body_bytes, cyfs_headers, head_only);
        }

        // Non-ObjectId final value: return the JSON value itself. There is no
        // object id at the leaf, so `cyfs-obj-id` is omitted; the parents
        // chain alone backs the verification of the surrounding path.
        let body = serde_json::to_string(&final_value).map_err(|e| {
            NdnError::Internal(format!("serialize final value: {}", e))
        })?;
        if resp_raw {
            return serve_raw_bytes(
                Bytes::from(body.into_bytes()),
                "application/json; charset=utf-8",
                head_only,
            );
        }
        let body_bytes = Bytes::from(body.into_bytes());
        let mut cyfs_headers = CYFSHttpRespHeaders::default();
        cyfs_headers.path_obj = root_path_obj_jwt;
        cyfs_headers.parents = parents.into_iter().map(CyfsParent::Json).collect();
        build_json_value_response(body_bytes, cyfs_headers, head_only)
    }

    /// Unified chunk-streaming responder. When `cyfs_headers` is `None`, no
    /// `cyfs-*` headers are emitted (the `resp=raw` / O-Link-raw mode). When
    /// `fs_fallback` is `Some`, a missing store entry falls back to streaming
    /// the on-disk file directly (used for R-Link FileObject whose sidecar
    /// predates the store registration finalizing).
    async fn build_chunk_response(
        &self,
        chunk_id: &ChunkId,
        head_only: bool,
        range_header: Option<&str>,
        cyfs_headers: Option<CYFSHttpRespHeaders>,
        fs_fallback: Option<&Path>,
    ) -> NdnResult<Response<ServerBody>> {
        let offset = parse_range_offset(range_header).unwrap_or(0);

        let (reader, total_size) = match self
            .config
            .store_mgr
            .open_chunk_reader(chunk_id, offset)
            .await
        {
            Ok(v) => v,
            Err(NdnError::NotFound(_)) if fs_fallback.map(|p| p.is_file()).unwrap_or(false) => {
                let fs_path = fs_fallback.unwrap();
                let meta = tokio::fs::metadata(fs_path).await.map_err(|e| {
                    NdnError::IoError(format!("stat {} failed: {}", fs_path.display(), e))
                })?;
                let total = meta.len();
                if offset > total {
                    return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
                }
                let mut file = tokio::fs::File::open(fs_path).await.map_err(|e| {
                    NdnError::IoError(format!("open {} failed: {}", fs_path.display(), e))
                })?;
                if offset > 0 {
                    use tokio::io::AsyncSeekExt;
                    file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|e| {
                        NdnError::IoError(format!(
                            "seek {} failed: {}",
                            fs_path.display(),
                            e
                        ))
                    })?;
                }
                let reader: ChunkReader = Box::pin(file);
                (reader, total)
            }
            Err(e) => return Err(e),
        };
        finalize_stream_response(
            reader,
            total_size,
            offset,
            head_only,
            cyfs_headers,
            &chunk_id.to_string(),
        )
    }

    /// Stream a ChunkList object's concatenated bytes. Mirrors the behavior
    /// of [`NamedStoreMgr::open_chunklist_reader`] used by the legacy server.
    async fn build_chunklist_response(
        &self,
        chunklist_id: &ObjId,
        head_only: bool,
        range_header: Option<&str>,
        cyfs_headers: Option<CYFSHttpRespHeaders>,
    ) -> NdnResult<Response<ServerBody>> {
        let offset = parse_range_offset(range_header).unwrap_or(0);
        let (reader, total_size) = self
            .config
            .store_mgr
            .open_chunklist_reader(chunklist_id, offset)
            .await?;
        finalize_stream_response(
            reader,
            total_size,
            offset,
            head_only,
            cyfs_headers,
            &chunklist_id.to_string(),
        )
    }

    // ---------------- Auto-objectification ----------------

    /// Walk `semantic_root` and (re)build sidecars for every regular file and
    /// `dirobj.meta`-marked directory that is missing or out-of-date. Returns
    /// the number of files/directories objectified in this pass.
    pub async fn scan_and_objectify(&self) -> NdnResult<usize> {
        let root = self.config.semantic_root.clone();
        if !root.is_dir() {
            return Err(NdnError::InvalidParam(format!(
                "semantic root {} is not a directory",
                root.display()
            )));
        }

        let template = load_object_template(&root);
        let mut processed = 0usize;
        self.walk_and_objectify(&root, template.as_ref(), &mut processed)
            .await?;
        Ok(processed)
    }

    #[async_recursion::async_recursion]
    async fn walk_and_objectify(
        &self,
        dir: &Path,
        template: Option<&'async_recursion ObjectTemplate>,
        processed: &mut usize,
    ) -> NdnResult<()> {
        let read = std::fs::read_dir(dir).map_err(|e| {
            NdnError::IoError(format!("scan {} failed: {}", dir.display(), e))
        })?;
        for entry in read {
            let entry = entry.map_err(|e| {
                NdnError::IoError(format!("read_dir entry failed: {}", e))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| {
                NdnError::IoError(format!("file_type failed: {}", e))
            })?;
            let name = match path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if name.starts_with('.') {
                continue;
            }

            if file_type.is_dir() {
                let dirobj_meta = path.join(DIROBJ_META_FILE);
                if dirobj_meta.is_file() {
                    match self.objectify_directory(&path, template).await {
                        Ok(true) => *processed += 1,
                        Ok(false) => {}
                        Err(e) => warn!(
                            "ndn_dir_server: objectify dir {} failed: {}",
                            path.display(),
                            e
                        ),
                    }
                } else {
                    self.walk_and_objectify(&path, template, processed).await?;
                }
            } else if file_type.is_file() {
                if name == DIROBJ_META_FILE || name == OBJECT_TEMPLATE_FILE {
                    continue;
                }
                if name.ends_with(SIDECAR_SUFFIX) {
                    continue;
                }
                match self.objectify_file(&path, template).await {
                    Ok(true) => *processed += 1,
                    Ok(false) => {}
                    Err(e) => warn!(
                        "ndn_dir_server: objectify {} failed: {}",
                        path.display(),
                        e
                    ),
                }
            }
        }
        Ok(())
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
    async fn objectify_file(
        &self,
        file_path: &Path,
        template: Option<&ObjectTemplate>,
    ) -> NdnResult<bool> {
        let file_name = match file_path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n.to_string(),
            None => return Ok(false),
        };

        // Skip sidecar files, directory markers, templates, and hidden files.
        if file_name.ends_with(SIDECAR_SUFFIX)
            || file_name == DIROBJ_META_FILE
            || file_name == OBJECT_TEMPLATE_FILE
        {
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

        let mut file_obj =
            FileObject::new(file_name.clone(), chunk_size, chunk_id.to_string());
        apply_template_to_file(&mut file_obj, template);
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
        let path_obj_jwt = self.mint_path_jwt(file_path, &file_obj_id)?;

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

    /// Objectify a directory marked with `dirobj.meta`. Recursively walks the
    /// subtree, builds a single [`DirObject`] whose body inlines file objects
    /// and references sub-directory objects by id, writes a `<dir>.cyobj`
    /// sidecar at the parent level, and (in InStore mode) removes the source
    /// directory.
    async fn objectify_directory(
        &self,
        dir_path: &Path,
        template: Option<&ObjectTemplate>,
    ) -> NdnResult<bool> {
        let sidecar_path = append_extension(dir_path, SIDECAR_SUFFIX);
        let signature = compute_dir_signature(dir_path)?;

        if sidecar_path.is_file() {
            if let Ok(existing) = SidecarRecord::read_from(&sidecar_path) {
                if existing.obj_type == OBJ_TYPE_DIR
                    && existing.source_qcid.as_deref() == Some(signature.as_str())
                {
                    return Ok(false);
                }
            }
        }

        info!("ndn_dir_server: objectifying dir {}", dir_path.display());

        let dir_obj = self.build_dir_object_tree(dir_path, template).await?;
        let total_size = dir_obj.total_size;
        let (dir_obj_id, dir_obj_str) = dir_obj.gen_obj_id()?;

        // Ensure the DirObject itself is queryable via O-Link.
        self.config
            .store_mgr
            .put_object(&dir_obj_id, &dir_obj_str)
            .await?;

        // Sidecar stores the canonical form so the JSON round-trips to the
        // same `dir_obj_id` and inner_path verification holds for R-Link.
        let dir_obj_json: serde_json::Value =
            serde_json::from_str(&dir_obj_str).map_err(|e| {
                NdnError::Internal(format!("reparse canonical DirObject JSON: {}", e))
            })?;

        let path_obj_jwt = self.mint_path_jwt(dir_path, &dir_obj_id)?;

        let record = SidecarRecord {
            obj_type: OBJ_TYPE_DIR.to_string(),
            obj_id: dir_obj_id.to_string(),
            obj_json: dir_obj_json,
            path_obj_jwt,
            source_qcid: Some(signature),
            source_mtime: None,
            source_size: Some(total_size),
        };
        record.write_to(&sidecar_path)?;

        if matches!(self.config.mode, NdnDirServerMode::InStore) {
            if let Err(e) = tokio::fs::remove_dir_all(dir_path).await {
                warn!(
                    "ndn_dir_server: failed to remove dir {} after in-store upload: {}",
                    dir_path.display(),
                    e
                );
            }
        }

        Ok(true)
    }

    /// Recursively materialize a [`DirObject`] for `dir_path`. Files are
    /// inlined as FileObject JSON, sub-directories are built recursively and
    /// referenced by ObjId (their DirObject is written into the store so the
    /// reference resolves).
    #[async_recursion::async_recursion]
    async fn build_dir_object_tree(
        &self,
        dir_path: &Path,
        template: Option<&'async_recursion ObjectTemplate>,
    ) -> NdnResult<DirObject> {
        let name = dir_path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string());
        let mut dir_obj = DirObject::new(name);

        // Template defaults apply first so explicit `dirobj.meta` overrides win.
        apply_template_to_dir(&mut dir_obj, template);
        apply_dirobj_meta_overrides(&mut dir_obj, dir_path);

        let read = std::fs::read_dir(dir_path).map_err(|e| {
            NdnError::IoError(format!("read_dir {} failed: {}", dir_path.display(), e))
        })?;
        for entry in read {
            let entry = entry.map_err(|e| {
                NdnError::IoError(format!("read_dir entry failed: {}", e))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| {
                NdnError::IoError(format!("file_type failed: {}", e))
            })?;
            let fname = match path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if fname == DIROBJ_META_FILE || fname == OBJECT_TEMPLATE_FILE {
                continue;
            }
            if fname.ends_with(SIDECAR_SUFFIX) || fname.starts_with('.') {
                continue;
            }

            if file_type.is_file() {
                let (file_json, file_size) =
                    self.build_inline_file_object(&path, template).await?;
                dir_obj.add_file(fname, file_json, file_size)?;
            } else if file_type.is_dir() {
                let sub = self.build_dir_object_tree(&path, template).await?;
                let sub_size = sub.total_size;
                let (sub_id, sub_str) = sub.gen_obj_id()?;
                self.config
                    .store_mgr
                    .put_object(&sub_id, &sub_str)
                    .await?;
                dir_obj.add_directory(fname, sub_id, sub_size)?;
            }
        }

        Ok(dir_obj)
    }

    /// Build a FileObject for `file_path`, register its chunk in the store,
    /// and return the inlinable JSON + size. Used when folding files into a
    /// parent DirObject — no per-file sidecar is written on this path.
    async fn build_inline_file_object(
        &self,
        file_path: &Path,
        template: Option<&ObjectTemplate>,
    ) -> NdnResult<(serde_json::Value, u64)> {
        let file_name = file_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                NdnError::InvalidParam(format!("bad filename {}", file_path.display()))
            })?
            .to_string();

        let (chunk_id, chunk_size) =
            calculate_file_chunk_id(file_path.to_string_lossy().as_ref(), ChunkType::Mix256)
                .await?;
        self.register_chunk_in_store(file_path, &chunk_id, chunk_size)
            .await?;

        let mut file_obj =
            FileObject::new(file_name, chunk_size, chunk_id.to_string());
        apply_template_to_file(&mut file_obj, template);
        let file_json = serde_json::to_value(&file_obj).map_err(|e| {
            NdnError::Internal(format!("serialize FileObject failed: {}", e))
        })?;
        Ok((file_json, chunk_size))
    }

    fn mint_path_jwt(
        &self,
        source_path: &Path,
        target: &ObjId,
    ) -> NdnResult<Option<String>> {
        let Some(key) = self.config.signing_key.as_ref() else {
            return Ok(None);
        };
        let semantic_path = self.semantic_path_for(source_path);
        let path_obj = PathObject::new(semantic_path, target.clone());
        let path_json = serde_json::to_value(&path_obj).map_err(|e| {
            NdnError::Internal(format!("serialize PathObject failed: {}", e))
        })?;
        Ok(Some(named_obj_to_jwt(
            &path_json,
            key,
            self.config.signing_kid.clone(),
        )?))
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

/// Intermediate resolution result shared across O-Link / R-Link / hostname
/// paths. Consumed by [`NdnDirServer::serve_resolved`].
enum RootState {
    Chunk(ChunkId),
    NamedObj {
        obj_id: ObjId,
        obj_type: String,
        obj_json: serde_json::Value,
        path_obj_jwt: Option<String>,
        /// R-Link only: on-disk source for FileObject chunk-stream fallback.
        fs_path: Option<PathBuf>,
    },
    /// R-Link file with no sidecar yet — returned as raw bytes without CYFS
    /// verification headers.
    RawFile(PathBuf),
    /// A `.cyobj` path requested directly — returned as raw bytes so the
    /// client can inspect the sidecar record.
    LocalObjFile(PathBuf),
}

/// Emit a response body with no CYFS verification headers. Used for
/// `resp=raw` and for unobjectified content.
fn serve_raw_bytes(
    body_bytes: Bytes,
    content_type: &'static str,
    head_only: bool,
) -> NdnResult<Response<ServerBody>> {
    let len = body_bytes.len();
    let builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::CONTENT_LENGTH, len);
    let body = if head_only { empty_body() } else { full_body(body_bytes) };
    builder
        .body(body)
        .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
}

fn build_named_object_response(
    body_bytes: Bytes,
    cyfs_headers: CYFSHttpRespHeaders,
    head_only: bool,
) -> NdnResult<Response<ServerBody>> {
    let len = body_bytes.len();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, CONTENT_TYPE_CYFS_OBJECT)
        .header(http::header::CONTENT_LENGTH, len);
    apply_cyfs_headers(&mut builder, &cyfs_headers)?;
    let body = if head_only { empty_body() } else { full_body(body_bytes) };
    builder
        .body(body)
        .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
}

fn build_json_value_response(
    body_bytes: Bytes,
    cyfs_headers: CYFSHttpRespHeaders,
    head_only: bool,
) -> NdnResult<Response<ServerBody>> {
    let len = body_bytes.len();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json; charset=utf-8")
        .header(http::header::CONTENT_LENGTH, len);
    apply_cyfs_headers(&mut builder, &cyfs_headers)?;
    let body = if head_only { empty_body() } else { full_body(body_bytes) };
    builder
        .body(body)
        .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
}

/// Return `true` if the URL query contains `resp=raw` (the only documented
/// value today, but we tolerate other parameters alongside it).
fn query_has_resp_raw(query: Option<&str>) -> bool {
    let Some(q) = query else { return false };
    q.split('&').any(|pair| {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        k == "resp" && v == "raw"
    })
}

/// Split `/a/b/@/c/@/d` into (`"/a/b"`, `[["c"], ["d"]]`). Empty root is
/// preserved (returned as `""`). Each step's fields are percent-decoded.
fn split_inner_path_with_root(path: &str) -> (&str, Vec<Vec<String>>) {
    let mut parts = path.split(INNER_PATH_DELIMITER);
    let root = parts.next().unwrap_or("");
    let steps: Vec<Vec<String>> = parts.map(parse_step_fields).collect();
    (root, steps)
}

/// Same as [`split_inner_path_with_root`] but drops the root entirely: every
/// part of the URL path after the host contributes to the inner_path chain
/// (hostname O-Link case, where the object was encoded into the hostname).
fn split_inner_path_whole(path: &str) -> Vec<Vec<String>> {
    // For hostname O-Link, the URL path carries only inner_path steps. A
    // leading `/@/` is therefore purely syntactic — drop any empty segments
    // produced by splitting.
    path.split(INNER_PATH_DELIMITER)
        .filter_map(|chunk| {
            let fields = parse_step_fields(chunk);
            if fields.is_empty() {
                None
            } else {
                Some(fields)
            }
        })
        .collect()
}

fn parse_step_fields(step: &str) -> Vec<String> {
    step.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| decode_url_segment(s))
        .collect()
}

fn load_object_template(root: &Path) -> Option<ObjectTemplate> {
    let path = root.join(OBJECT_TEMPLATE_FILE);
    if !path.is_file() {
        return None;
    }
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                "ndn_dir_server: read {} failed: {}",
                path.display(),
                e
            );
            return None;
        }
    };
    match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(v) => v.as_object().cloned(),
        Err(e) => {
            warn!(
                "ndn_dir_server: parse {} failed: {}",
                path.display(),
                e
            );
            None
        }
    }
}

fn template_meta_for<'a>(
    template: Option<&'a ObjectTemplate>,
    obj_type: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    template
        .and_then(|t| t.get(obj_type))
        .and_then(|v| v.get("meta"))
        .and_then(|v| v.as_object())
}

fn apply_template_to_file(file_obj: &mut FileObject, template: Option<&ObjectTemplate>) {
    if let Some(meta) = template_meta_for(template, OBJ_TYPE_FILE) {
        for (k, v) in meta {
            file_obj.meta.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

fn apply_template_to_dir(dir_obj: &mut DirObject, template: Option<&ObjectTemplate>) {
    if let Some(meta) = template_meta_for(template, OBJ_TYPE_DIR) {
        for (k, v) in meta {
            dir_obj.meta.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

/// Merge values read from a directory's `dirobj.meta` into `dir_obj`. The file
/// is treated as optional JSON with shape `{ "name": "...", "meta": {...} }`;
/// unknown shape is logged and ignored rather than aborting objectification.
fn apply_dirobj_meta_overrides(dir_obj: &mut DirObject, dir_path: &Path) {
    let meta_path = dir_path.join(DIROBJ_META_FILE);
    let bytes = match std::fs::read(&meta_path) {
        Ok(b) if !b.is_empty() => b,
        _ => return,
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "ndn_dir_server: parse {} failed: {}",
                meta_path.display(),
                e
            );
            return;
        }
    };
    let Some(obj) = value.as_object() else { return };
    if let Some(n) = obj.get("name").and_then(|v| v.as_str()) {
        dir_obj.content_obj.name = n.to_string();
    }
    if let Some(m) = obj.get("meta").and_then(|v| v.as_object()) {
        for (k, v) in m {
            dir_obj.meta.insert(k.clone(), v.clone());
        }
    }
}

/// Produce a stable, cheap signature of a directory subtree for sidecar
/// freshness checks. Not cryptographic — only compared against sidecars we
/// wrote ourselves, so a non-cryptographic hash of (relative path, size,
/// mtime) tuples plus any `dirobj.meta` contents is sufficient.
fn compute_dir_signature(dir_path: &Path) -> NdnResult<String> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut entries: Vec<(String, u64, u64)> = Vec::new();
    let mut meta_bytes: Vec<u8> = Vec::new();
    collect_dir_signature_entries(dir_path, dir_path, &mut entries, &mut meta_bytes)
        .map_err(|e| {
            NdnError::IoError(format!(
                "signature scan {} failed: {}",
                dir_path.display(),
                e
            ))
        })?;
    entries.sort();

    let mut hasher = DefaultHasher::new();
    for (rel, size, mtime) in &entries {
        rel.hash(&mut hasher);
        size.hash(&mut hasher);
        mtime.hash(&mut hasher);
    }
    meta_bytes.hash(&mut hasher);
    Ok(format!("dirsig:{:016x}", hasher.finish()))
}

fn collect_dir_signature_entries(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(String, u64, u64)>,
    meta_bytes: &mut Vec<u8>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        if name.ends_with(SIDECAR_SUFFIX) {
            // Sidecars are outputs of objectification, not inputs — exclude
            // them so rewriting one doesn't look like a source change.
            continue;
        }
        if file_type.is_dir() {
            collect_dir_signature_entries(root, &path, out, meta_bytes)?;
        } else if file_type.is_file() {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let meta = entry.metadata()?;
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if name == DIROBJ_META_FILE || name == OBJECT_TEMPLATE_FILE {
                // Fold meta/template contents into the signature so editing
                // them forces a rebuild even when file stats are unchanged.
                if let Ok(bytes) = std::fs::read(&path) {
                    meta_bytes.extend_from_slice(rel.as_bytes());
                    meta_bytes.push(0);
                    meta_bytes.extend_from_slice(&bytes);
                    meta_bytes.push(0);
                }
                continue;
            }
            out.push((rel, size, mtime));
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

fn finalize_stream_response(
    reader: ChunkReader,
    total_size: u64,
    offset: u64,
    head_only: bool,
    cyfs_headers: Option<CYFSHttpRespHeaders>,
    id_for_error: &str,
) -> NdnResult<Response<ServerBody>> {
    if offset > total_size {
        return Err(NdnError::OffsetTooLarge(id_for_error.to_string()));
    }
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
    let cyfs_headers = cyfs_headers.map(|mut h| {
        if h.chunk_size.is_none() {
            h.chunk_size = Some(total_size);
        }
        h
    });
    if let Some(h) = cyfs_headers.as_ref() {
        apply_cyfs_headers(&mut builder, h)?;
    }

    let body = if head_only {
        empty_body()
    } else {
        chunk_reader_to_body(reader, remaining)
    };
    builder
        .body(body)
        .map_err(|e| NdnError::Internal(format!("build response failed: {}", e)))
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

#[cfg(test)]
mod inner_path_tests {
    use super::*;

    #[test]
    fn split_inner_path_with_root_no_delimiter() {
        let (root, steps) = split_inner_path_with_root("/readme.txt");
        assert_eq!(root, "/readme.txt");
        assert!(steps.is_empty());
    }

    #[test]
    fn split_inner_path_with_root_single_step() {
        let (root, steps) = split_inner_path_with_root("/all_images/@/readme");
        assert_eq!(root, "/all_images");
        assert_eq!(steps, vec![vec!["readme".to_string()]]);
    }

    #[test]
    fn split_inner_path_with_root_two_steps_multi_field() {
        let (root, steps) =
            split_inner_path_with_root("/all_images/@/readme/@/content");
        assert_eq!(root, "/all_images");
        assert_eq!(
            steps,
            vec![vec!["readme".to_string()], vec!["content".to_string()]]
        );
    }

    #[test]
    fn split_inner_path_with_root_multi_field_within_segment() {
        let (root, steps) = split_inner_path_with_root("/a/@/b/c/@/d");
        assert_eq!(root, "/a");
        assert_eq!(
            steps,
            vec![
                vec!["b".to_string(), "c".to_string()],
                vec!["d".to_string()],
            ]
        );
    }

    #[test]
    fn split_inner_path_whole_ignores_empty_leading_segment() {
        // Hostname O-Link: path "/@/content" has no root portion, just an
        // inner-path step whose single field is "content".
        let steps = split_inner_path_whole("/@/content");
        assert_eq!(steps, vec![vec!["content".to_string()]]);
    }

    #[test]
    fn split_inner_path_whole_single_segment_multi_field() {
        let steps = split_inner_path_whole("/foo/bar");
        assert_eq!(steps, vec![vec!["foo".to_string(), "bar".to_string()]]);
    }

    #[test]
    fn split_inner_path_whole_multiple_steps() {
        let steps = split_inner_path_whole("/readme/@/content");
        assert_eq!(
            steps,
            vec![vec!["readme".to_string()], vec!["content".to_string()]]
        );
    }

    #[test]
    fn query_resp_raw_detection() {
        assert!(query_has_resp_raw(Some("resp=raw")));
        assert!(query_has_resp_raw(Some("foo=1&resp=raw")));
        assert!(query_has_resp_raw(Some("resp=raw&bar=2")));
        assert!(!query_has_resp_raw(Some("resp=other")));
        assert!(!query_has_resp_raw(Some("respraw")));
        assert!(!query_has_resp_raw(None));
        assert!(!query_has_resp_raw(Some("")));
    }
}

