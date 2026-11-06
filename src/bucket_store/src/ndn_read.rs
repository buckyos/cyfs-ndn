//! §4.6.5: `GET | HEAD /ndn/<obj_id>[/@/<inner_path>][?hint=<hint>]`, the read
//! side of the protocol. The host decides whether the request may read its
//! authorization root (a public bucket hit or [`BucketStore::ndn_token_covers`]);
//! this module parses the request, walks from that root along containment only,
//! and answers in plain HTTP so curl, ffmpeg and `<video>` read it directly.
//!
//! The walk is typed on purpose: `File.content`, `ChunkList[i]` and `Dir` entries
//! are the only edges. Following any field that parses as an ObjId would let a
//! public object name a private one and read it.

use crate::error::BucketResult;
use crate::http::{body, parse_range, HttpBody};
use crate::store::BucketStore;
use crate::types::*;
use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Response, StatusCode};
use ndn_lib::{
    apply_content_policy, apply_cyfs_resp_headers, apply_security_headers, build_named_object_by_jwt, inline_mime_from_name, load_named_obj,
    try_build_named_object_by_json, ChunkId, ChunkList, CyfsParent, CYFSHttpRespHeaders, DirObject, FileObject, ObjId, SimpleMapItem,
    NdnHint, NdnHintError, CYFS_INNER_PATH_DELIMITER, NDN_PATH_PREFIX, OBJ_TYPE_CHUNK_LIST, OBJ_TYPE_DIR, OBJ_TYPE_FILE, OCTET_STREAM, SANDBOX_CSP,
};
use std::fmt;

pub const PATH_PREFIX: &str = NDN_PATH_PREFIX;
/// NamedObject JSON on the CYFS protocol.
pub const OBJECT_JSON_TYPE: &str = "application/cyfs-object";

const PUBLIC_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const PRIVATE_CACHE_CONTROL: &str = "private";
/// JSON bytes the `cyfs-parents-N` headers of one response may carry; beyond it
/// (large chunk lists, long walks) a parent travels as its id, so the headers
/// stay under proxy limits.
const MAX_PARENT_JSON_BYTES: usize = 4096;
/// Deepest walk of one request.
const MAX_STEPS: usize = 64;

/// A request that is wrong whatever it asks for (HTTP 400).
pub type BadRequest = NdnHintError;

/// How a request was allowed; decides what shared caches may do with the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdnAccess {
    Public,
    Token,
}

/// One parsed `/ndn/` request.
#[derive(Clone)]
pub struct NdnRequest {
    /// Where authorization is judged.
    pub root: ObjId,
    /// Containment walk from `root` to the target.
    pub steps: Vec<String>,
    /// With `hint.path`, the walk must end at the URL's object.
    pub expect: Option<ObjId>,
    pub token: Option<String>,
    pub resp_raw: bool,
}

impl fmt::Debug for NdnRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NdnRequest")
            .field("root", &self.root.to_string())
            .field("steps", &self.steps)
            .field("expect", &self.expect.as_ref().map(|e| e.to_string()))
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("resp_raw", &self.resp_raw)
            .finish()
    }
}

fn percent_decode_query(raw: &str) -> Option<String> {
    percent_decode(&raw.replace('+', " "))
}

fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn query_value(query: &str, name: &str) -> Result<Option<String>, BadRequest> {
    let mut found = None;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == name {
            if found.is_some() {
                return Err(NdnHintError("a query parameter is repeated"));
            }
            found = Some(percent_decode_query(v).ok_or(NdnHintError("a query parameter is not valid UTF-8"))?);
        }
    }
    Ok(found)
}

/// `a/@/b/c` → `[a, b, c]`: `/@/` and `/` both descend one edge, names are percent-decoded after splitting.
fn parse_steps(raw: &str, decode: bool) -> Result<Vec<String>, BadRequest> {
    let mut steps = Vec::new();
    for part in raw.split(CYFS_INNER_PATH_DELIMITER) {
        for segment in part.split('/') {
            let step = if decode { percent_decode(segment).ok_or(NdnHintError("inner path is not valid UTF-8"))? } else { segment.to_string() };
            if step.is_empty() || step == "." || step == ".." {
                return Err(NdnHintError("invalid inner path"));
            }
            steps.push(step);
        }
    }
    if steps.len() > MAX_STEPS {
        return Err(NdnHintError("inner path is too deep"));
    }
    Ok(steps)
}

impl NdnRequest {
    /// `path` is the request path including `/ndn/`, as received (not decoded).
    pub fn parse(path: &str, query: Option<&str>) -> Result<NdnRequest, BadRequest> {
        let rest = path.strip_prefix(PATH_PREFIX).ok_or(NdnHintError("not a /ndn/ path"))?;
        let (id, inner) = match rest.split_once(CYFS_INNER_PATH_DELIMITER) {
            Some((id, inner)) => (id, Some(inner)),
            None => (rest.trim_end_matches('/'), None),
        };
        let url_id = percent_decode(id).and_then(|id| ObjId::new(&id).ok()).ok_or(NdnHintError("invalid object id"))?;
        let query = query.unwrap_or("");
        let hint = query_value(query, "hint")?.map(|raw| NdnHint::parse(&raw)).transpose()?.unwrap_or_default();
        let resp_raw = query_value(query, "resp")?.as_deref() == Some("raw");
        let (root, steps, expect) = match (hint.path, inner) {
            (Some(_), Some(_)) => return Err(NdnHintError("hint.path and an inner path cannot be combined")),
            (Some(path), None) => {
                let (root, walk) = match path.split_once(CYFS_INNER_PATH_DELIMITER) {
                    Some((root, walk)) => (root, Some(walk)),
                    None => (path.as_str(), None),
                };
                let root = ObjId::new(root).map_err(|_| NdnHintError("hint.path does not start with an object id"))?;
                let steps = walk.map(|w| parse_steps(w, false)).transpose()?.unwrap_or_default();
                (root, steps, Some(url_id))
            }
            (None, Some(inner)) => (url_id, parse_steps(inner, true)?, None),
            (None, None) => (url_id, Vec::new(), None),
        };
        Ok(NdnRequest { root, steps, expect, token: hint.token, resp_raw })
    }
}

/// One object on the walk, known only by its id's type.
enum Node {
    Chunk(ChunkId),
    List { id: ObjId, list: ChunkList, json: String },
    File { id: ObjId, file: FileObject, json: String },
    Dir { id: ObjId, dir: DirObject, json: String },
    Other { id: ObjId, json: String },
}

impl Node {
    fn id(&self) -> ObjId {
        match self {
            Node::Chunk(c) => c.to_obj_id(),
            Node::List { id, .. } | Node::File { id, .. } | Node::Dir { id, .. } | Node::Other { id, .. } => id.clone(),
        }
    }

    fn json(&self) -> Option<&str> {
        match self {
            Node::Chunk(_) => None,
            Node::List { json, .. } | Node::File { json, .. } | Node::Dir { json, .. } | Node::Other { json, .. } => Some(json),
        }
    }
}

/// `cyfs-parents-N` of a walk, root first.
fn parent_chain<'a>(nodes: impl Iterator<Item = &'a Node>) -> Vec<CyfsParent> {
    let mut budget = MAX_PARENT_JSON_BYTES;
    nodes
        .filter_map(|node| {
            let json = node.json()?;
            Some(if json.len() <= budget {
                budget -= json.len();
                CyfsParent::Json(json.to_string())
            } else {
                CyfsParent::ObjId(node.id())
            })
        })
        .collect()
}

/// The canonical form of a stored object: what its id was computed over.
fn canonical(id: &ObjId, stored: &str) -> String {
    serde_json::from_str::<serde_json::Value>(stored)
        .ok()
        .and_then(|v| try_build_named_object_by_json(&id.obj_type, &v).ok())
        .map(|(_, json)| json)
        .unwrap_or_else(|| stored.to_string())
}

fn node_of(id: ObjId, stored: &str) -> Option<Node> {
    let json = canonical(&id, stored);
    Some(match id.obj_type.as_str() {
        OBJ_TYPE_FILE => Node::File { file: load_named_obj(stored).ok()?, id, json },
        OBJ_TYPE_DIR => Node::Dir { dir: load_named_obj(stored).ok()?, id, json },
        OBJ_TYPE_CHUNK_LIST => Node::List { list: ChunkList::from_json(stored).ok()?, id, json },
        _ => Node::Other { id, json },
    })
}

impl BucketStore {
    async fn ndn_load(&self, id: &ObjId) -> Option<Node> {
        if id.is_chunk() {
            return Some(Node::Chunk(ChunkId::from_obj_id(id)));
        }
        let stored = self.ndm.get_object(id).await.ok()?;
        node_of(id.clone(), &stored)
    }

    async fn ndn_step(&self, node: &Node, step: &str) -> Option<Node> {
        match node {
            Node::File { file, .. } if step == "content" => {
                let content = ObjId::new(&file.content).ok()?;
                (content.is_chunk() || content.is_chunk_list()).then_some(())?;
                self.ndn_load(&content).await
            }
            Node::List { list, .. } => {
                if step.is_empty() || !step.bytes().all(|b| b.is_ascii_digit()) || (step.len() > 1 && step.starts_with('0')) {
                    return None;
                }
                let chunk = list.body.get(step.parse::<usize>().ok()?)?;
                Some(Node::Chunk(chunk.clone()))
            }
            Node::Dir { dir, .. } => match dir.get(step)? {
                SimpleMapItem::ObjId(id) => self.ndn_load(id).await,
                SimpleMapItem::Object(obj_type, value) => {
                    let (id, json) = try_build_named_object_by_json(obj_type, value).ok()?;
                    node_of(id, &json)
                }
                SimpleMapItem::ObjectJwt(obj_type, jwt) => {
                    let (id, json) = build_named_object_by_jwt(obj_type, jwt).ok()?;
                    node_of(id, &json)
                }
            },
            _ => None,
        }
    }

    /// The target and the objects walked to reach it; `None` when the path does
    /// not exist, leaves containment, or ends elsewhere than the request says.
    async fn ndn_walk(&self, req: &NdnRequest) -> Option<(Node, Vec<Node>)> {
        let mut node = self.ndn_load(&req.root).await?;
        let mut parents = Vec::new();
        for step in &req.steps {
            let next = self.ndn_step(&node, step).await?;
            parents.push(std::mem::replace(&mut node, next));
        }
        if req.expect.as_ref().is_some_and(|expect| *expect != node.id()) {
            return None;
        }
        Some((node, parents))
    }

    /// Bytes of a chunk or chunk list: its real length and a reader from `start`.
    async fn ndn_content_len(&self, content: &Node) -> Option<u64> {
        match content {
            Node::Chunk(chunk) => match chunk.get_length() {
                Some(len) => Some(len),
                None => self.ndm.query_chunk_state(chunk).await.ok().map(|(_, size)| size),
            },
            Node::List { list, .. } => Some(list.total_size),
            _ => None,
        }
    }

    async fn ndn_open(&self, content: &Node, start: u64) -> Option<ndn_lib::ChunkReader> {
        let opened = match content {
            Node::Chunk(chunk) => self.ndm.open_chunk_reader(chunk, start).await,
            Node::List { id, .. } => self.ndm.open_chunklist_reader(id, start).await,
            _ => return None,
        };
        opened.ok().map(|(reader, _)| reader)
    }

    /// Serves an authorized request. Anything unreadable from the root is the
    /// same 404 the host sends for a refused one.
    pub async fn serve_ndn(&self, req: &NdnRequest, access: NdnAccess, headers: &HeaderMap, head: bool) -> Response<HttpBody> {
        let Some((target, parents)) = self.ndn_walk(req).await else { return ndn_not_found() };
        let id = target.id();
        self.ndm.touch(&id).await;
        let mut base = Response::new(body(Bytes::new()));
        let h = base.headers_mut();
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static(if access == NdnAccess::Public { PUBLIC_CACHE_CONTROL } else { PRIVATE_CACHE_CONTROL }));
        if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", id.to_string())) {
            h.insert(header::ETAG, v);
        }
        apply_security_headers(h, SANDBOX_CSP);
        let etag = id.to_string();
        if none_match(headers, &etag) {
            *base.status_mut() = StatusCode::NOT_MODIFIED;
            return base;
        }
        if req.resp_raw {
            if let Some(json) = target.json() {
                set_json(&mut base, json.to_string(), head);
                return base;
            }
        }
        let mut cyfs = CYFSHttpRespHeaders::default();
        match &target {
            Node::File { file, .. } => {
                let name = file.content_obj.name.as_str();
                let declared = file.meta.get("mime").or_else(|| file.meta.get("mime_type")).and_then(|v| v.as_str());
                let mime = declared.or_else(|| inline_mime_from_name(name)).unwrap_or(OCTET_STREAM);
                apply_content_policy(base.headers_mut(), mime, Some(name), false);
                cyfs.parents = parent_chain(parents.iter().chain(std::iter::once(&target)));
                if file.content.is_empty() {
                    if file.size != 0 {
                        return ndn_not_found();
                    }
                    base.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from(0u64));
                    base.headers_mut().insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                    cyfs.obj_id = Some(id);
                    let _ = apply_cyfs_resp_headers(&cyfs, base.headers_mut());
                    return base;
                }
                let content = match ObjId::new(&file.content) {
                    Ok(content) if content.is_chunk() || content.is_chunk_list() => self.ndn_load(&content).await,
                    _ => None,
                };
                let Some(content) = content else { return ndn_not_found() };
                cyfs.obj_id = Some(content.id());
                self.ndn_stream(req, base, cyfs, &content, &etag, headers, head).await
            }
            Node::Chunk(_) | Node::List { .. } => {
                base.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(OCTET_STREAM));
                base.headers_mut().insert(header::CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
                cyfs.parents = parent_chain(parents.iter());
                cyfs.obj_id = Some(id);
                self.ndn_stream(req, base, cyfs, &target, &etag, headers, head).await
            }
            Node::Dir { json, .. } | Node::Other { json, .. } => {
                set_json(&mut base, json.clone(), head);
                cyfs.parents = parent_chain(parents.iter());
                cyfs.obj_id = Some(id);
                let _ = apply_cyfs_resp_headers(&cyfs, base.headers_mut());
                base
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn ndn_stream(
        &self,
        req: &NdnRequest,
        mut r: Response<HttpBody>,
        mut cyfs: CYFSHttpRespHeaders,
        content: &Node,
        etag: &str,
        headers: &HeaderMap,
        head: bool,
    ) -> Response<HttpBody> {
        let Some(len) = self.ndn_content_len(content).await else { return ndn_not_found() };
        let h = r.headers_mut();
        h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        cyfs.chunk_size = Some(len);
        if !req.resp_raw {
            let _ = apply_cyfs_resp_headers(&cyfs, h);
        }
        let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok()).filter(|_| if_range_holds(headers, etag));
        let range = if len == 0 {
            None
        } else {
            match parse_range(range_header, len) {
                Ok(range) => range,
                Err(()) => {
                    *r.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{len}")) {
                        r.headers_mut().insert(header::CONTENT_RANGE, v);
                    }
                    r.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from(0u64));
                    return r;
                }
            }
        };
        let (start, count) = match range {
            Some((s, e)) => (s, e - s + 1),
            None => (0, len),
        };
        if let Some((s, e)) = range {
            *r.status_mut() = StatusCode::PARTIAL_CONTENT;
            if let Ok(v) = HeaderValue::from_str(&format!("bytes {s}-{e}/{len}")) {
                r.headers_mut().insert(header::CONTENT_RANGE, v);
            }
        }
        r.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from(count));
        if count == 0 {
            return r;
        }
        // HEAD also opens: an evicted object must not answer 200.
        let Some(reader) = self.ndn_open(content, start).await else { return ndn_not_found() };
        if !head {
            *r.body_mut() = named_store::chunk_reader_to_body(reader, count);
        }
        r
    }

    /// Is `obj_id` the current, live binding of some key of `bucket`? A hit feeds a cache key's LRU.
    pub fn ndn_bound_in(&self, bucket: &BucketId, obj_id: &ObjId) -> BucketResult<bool> {
        let Some(binding) = self.db.read(|c| crate::db::live_binding_of(c, bucket, &obj_id.to_string(), now_ms()))? else { return Ok(false) };
        if self.bucket(bucket).is_ok_and(|info| matches!(info.data_policy, DataPolicy::Cache)) {
            self.touch_binding(&binding);
        }
        Ok(true)
    }

    /// §4.6.5 step 3: the token is a read-only, unbound, single-key token (N7)
    /// and that key currently binds `root`. Every failure reads as no access.
    pub fn ndn_token_covers(&self, token: &str, root: &ObjId) -> bool {
        let Ok(Some((bucket_id, key))) = self.access().ndn_read_token(token) else { return false };
        let Ok(bucket) = self.bucket(&bucket_id) else { return false };
        let Ok(binding) = self.lookup(&bucket, &key, None) else { return false };
        if binding.expires_at.is_some_and(|t| t <= now_ms()) {
            return false;
        }
        let bound = binding.obj_id.as_deref().and_then(|o| ObjId::new(o).ok());
        if bound.as_ref() != Some(root) {
            return false;
        }
        if matches!(bucket.data_policy, DataPolicy::Cache | DataPolicy::Ambient) {
            self.touch_binding(&binding);
        }
        true
    }
}

fn set_json(r: &mut Response<HttpBody>, json: String, head: bool) {
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(OBJECT_JSON_TYPE));
    r.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from(json.len() as u64));
    if !head {
        *r.body_mut() = body(json);
    }
}

fn none_match(headers: &HeaderMap, etag: &str) -> bool {
    headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()).is_some_and(|v| {
        v.split(',').any(|t| {
            let t = t.trim().trim_start_matches("W/").trim_matches('"');
            t == "*" || t == etag
        })
    })
}

/// `If-Range` holds only for this exact ETag; a date or another validator reads the whole object.
fn if_range_holds(headers: &HeaderMap, etag: &str) -> bool {
    match headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok()) {
        Some(v) => v.trim().trim_matches('"') == etag,
        None => true,
    }
}

/// The one refusal of `/ndn/`: refused, missing, evicted and out of path look alike.
pub fn ndn_not_found() -> Response<HttpBody> {
    ndn_error(StatusCode::NOT_FOUND, "not_found")
}

pub fn ndn_bad_request(e: BadRequest) -> Response<HttpBody> {
    ndn_error(StatusCode::BAD_REQUEST, e.0)
}

pub fn ndn_error(status: StatusCode, message: &str) -> Response<HttpBody> {
    let mut r = Response::new(body(serde_json::json!({ "error": message }).to_string()));
    *r.status_mut() = status;
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    apply_security_headers(h, SANDBOX_CSP);
    r
}
