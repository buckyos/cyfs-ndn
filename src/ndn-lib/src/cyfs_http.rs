use crate::{NdnError, NdnResult, ObjId};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use url::{form_urlencoded, Url};

// =============================================================
// URL helpers
// =============================================================

pub const CYFS_INNER_PATH_DELIMITER: &str = "/@/";

pub enum CYFSUrlMode {
    PathMode,     // ObjId embedded in URL path
    HostnameMode, // ObjId in URL hostname (base32 form)
}

/// Parsed CYFS URL per spec:
///   http://$zone_id/<root_locator>(/@/<path_step>)*?query
#[derive(Debug, Clone)]
pub struct CyfsParsedUrl {
    pub host: Option<String>,
    /// ObjId located in hostname or path (if any).
    pub url_obj_id: Option<ObjId>,
    /// Raw semantic path from "/..." up to the first "/@/" (or end).
    /// For an O-Link, this will be the path segment that contains `url_obj_id`.
    pub root_locator: String,
    /// Ordered inner_path steps (each without the leading "/").
    pub inner_path_steps: Vec<String>,
    /// Raw query string (without leading `?`).
    pub query: Option<String>,
    /// Whether `?resp=raw` is present.
    pub resp_raw: bool,
}

impl CyfsParsedUrl {
    /// Full inner_path chain, e.g. `/readme/@/content`.
    pub fn inner_path_chain(&self) -> Option<String> {
        if self.inner_path_steps.is_empty() {
            None
        } else {
            Some(format!(
                "/{}",
                self.inner_path_steps.join(CYFS_INNER_PATH_DELIMITER)
            ))
        }
    }
}

/// Legacy helper – returns (ObjId, rest_path_after_objid).
/// Keep compatible with existing callers in the codebase.
pub fn cyfs_get_obj_id_from_url(cyfs_url: &str) -> NdnResult<(ObjId, Option<String>)> {
    let url = Url::parse(cyfs_url)
        .map_err(|e| NdnError::InvalidId(format!("parse cyfs url failed:{}", e)))?;
    let host = url
        .host_str()
        .ok_or_else(|| NdnError::InvalidId(format!("cyfs url host not found:{}", cyfs_url)))?;

    if let Ok(obj_id) = ObjId::from_hostname(host) {
        let obj_path = url.path();
        if obj_path.is_empty() {
            return Ok((obj_id, None));
        }
        return Ok((obj_id, Some(obj_path.to_string())));
    }

    ObjId::from_path(url.path())
}

/// Parse a CYFS URL per spec, splitting root_locator from the
/// `"/@/"`-delimited `inner_path` chain. This does NOT resolve
/// semantic paths to ObjIds – that is the caller's responsibility.
pub fn cyfs_parse_url(cyfs_url: &str) -> NdnResult<CyfsParsedUrl> {
    let url = Url::parse(cyfs_url)
        .map_err(|e| NdnError::InvalidId(format!("parse cyfs url failed:{}", e)))?;

    let host = url.host_str().map(|s| s.to_string());
    let host_obj_id = host.as_deref().and_then(|h| ObjId::from_hostname(h).ok());

    // Split path by the literal "/@/" delimiter.
    let path = url.path();
    let segments: Vec<&str> = path.split(CYFS_INNER_PATH_DELIMITER).collect();
    let root_locator = segments[0].to_string();
    let inner_path_steps: Vec<String> = segments[1..]
        .iter()
        .map(|s| s.trim_start_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Locate ObjId: prefer hostname, else scan root_locator.
    let mut url_obj_id = host_obj_id;
    if url_obj_id.is_none() {
        if let Ok((oid, _)) = ObjId::from_path(&root_locator) {
            url_obj_id = Some(oid);
        }
    }

    let query = url.query().map(|q| q.to_string());
    let resp_raw = url.query_pairs().any(|(k, v)| k == "resp" && v == "raw");

    Ok(CyfsParsedUrl {
        host,
        url_obj_id,
        root_locator,
        inner_path_steps,
        query,
        resp_raw,
    })
}

// =============================================================
// Dispatch and semantic-list helpers
// =============================================================

pub const CYFS_CONTENT_TYPE_NAMED_OBJECT_JSON: &str = "application/cyfs-named-object+json";

pub const CYFS_HEADER_DISPATCH_ERROR: &str = "cyfs-dispatch-error";
pub const CYFS_DISPATCH_ERROR_NO_HANDLER: &str = "no-handler";

pub const CYFS_HEADER_LIST_MODE: &str = "cyfs-list-mode";
pub const CYFS_HEADER_LIST_TRUNCATED: &str = "cyfs-list-truncated";

/// Maximum child ObjectIds in one loose semantic-list response.
pub const CYFS_SEMANTIC_LIST_MAX_CHILDREN: usize = 4096;

/// Validate the URL shape for protocol-level `dispatch`.
///
/// Dispatch writes to a semantic path only; it MUST NOT target an inner_path
/// because NamedObjects are immutable.
pub fn validate_cyfs_dispatch_url(parsed: &CyfsParsedUrl) -> NdnResult<()> {
    if !parsed.inner_path_steps.is_empty() {
        return Err(NdnError::InvalidParam(
            "dispatch URL MUST NOT contain inner_path segments".to_string(),
        ));
    }
    if parsed.root_locator.is_empty() || parsed.root_locator == "/" {
        return Err(NdnError::InvalidParam(
            "dispatch URL MUST contain a semantic path".to_string(),
        ));
    }
    Ok(())
}

/// Check `Content-Type` for a dispatch body. Parameters such as `charset=utf-8`
/// are accepted.
pub fn is_cyfs_named_object_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .map(|v| {
            v.trim()
                .eq_ignore_ascii_case(CYFS_CONTENT_TYPE_NAMED_OBJECT_JSON)
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CyfsSemanticListMode {
    Strict,
    Loose,
}

impl CyfsSemanticListMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Loose => "loose",
        }
    }

    pub fn parse(raw: &str) -> NdnResult<Self> {
        match raw {
            "strict" => Ok(Self::Strict),
            "loose" => Ok(Self::Loose),
            _ => Err(NdnError::InvalidParam(format!(
                "unsupported semantic list mode: {}",
                raw
            ))),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CyfsSemanticListQuery {
    /// `list=loose|strict`; absent means the host may use its default shape.
    pub mode: Option<CyfsSemanticListMode>,
    /// Opaque cursor accepted by the host: ObjectId, timestamp, or app-defined token.
    pub before: Option<String>,
    /// Opaque cursor accepted by the host: ObjectId, timestamp, or app-defined token.
    pub after: Option<String>,
    /// Requested page size. Values above the protocol maximum are rejected.
    pub limit: Option<usize>,
}

pub fn parse_cyfs_semantic_list_query(parsed: &CyfsParsedUrl) -> NdnResult<CyfsSemanticListQuery> {
    let Some(query) = &parsed.query else {
        return Ok(CyfsSemanticListQuery::default());
    };

    let mut out = CyfsSemanticListQuery::default();
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "list" => {
                out.mode = Some(CyfsSemanticListMode::parse(value.as_ref())?);
            }
            "before" => {
                out.before = Some(value.into_owned());
            }
            "after" => {
                out.after = Some(value.into_owned());
            }
            "limit" => {
                let limit = value.parse::<usize>().map_err(|e| {
                    NdnError::InvalidParam(format!("invalid semantic list limit: {}", e))
                })?;
                if limit > CYFS_SEMANTIC_LIST_MAX_CHILDREN {
                    return Err(NdnError::InvalidParam(format!(
                        "semantic list limit {} exceeds max {}",
                        limit, CYFS_SEMANTIC_LIST_MAX_CHILDREN
                    )));
                }
                out.limit = Some(limit);
            }
            _ => {}
        }
    }

    Ok(out)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CyfsSemanticListRespHeaders {
    /// Actual list shape used by the host for this response.
    pub mode: Option<CyfsSemanticListMode>,
    /// Whether the host truncated the child list and the client should page.
    pub truncated: Option<bool>,
}

pub fn get_cyfs_semantic_list_resp_headers(
    headers: &HeaderMap,
) -> NdnResult<CyfsSemanticListRespHeaders> {
    let mode = header_get_str(headers, CYFS_HEADER_LIST_MODE)?
        .map(|raw| CyfsSemanticListMode::parse(raw.as_str()))
        .transpose()?;
    let truncated = header_get_str(headers, CYFS_HEADER_LIST_TRUNCATED)?
        .map(|raw| parse_header_bool(CYFS_HEADER_LIST_TRUNCATED, raw.as_str()))
        .transpose()?;

    Ok(CyfsSemanticListRespHeaders { mode, truncated })
}

pub fn apply_cyfs_semantic_list_resp_headers(
    resp: &CyfsSemanticListRespHeaders,
    headers: &mut HeaderMap,
) -> NdnResult<()> {
    if let Some(mode) = resp.mode {
        insert_header(headers, CYFS_HEADER_LIST_MODE, mode.as_str())?;
    }
    if let Some(truncated) = resp.truncated {
        insert_header(
            headers,
            CYFS_HEADER_LIST_TRUNCATED,
            if truncated { "true" } else { "false" },
        )?;
    }
    Ok(())
}

/// Parse a loose semantic-list body. The protocol shape is only `ObjectId[]`;
/// full object inlining is intentionally left out of this helper.
pub fn parse_cyfs_loose_list_body(body: &str) -> NdnResult<Vec<ObjId>> {
    let value: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        NdnError::InvalidData(format!("parse loose semantic list body failed: {}", e))
    })?;
    let arr = value.as_array().ok_or_else(|| {
        NdnError::InvalidData("loose semantic list body MUST be an ObjectId array".to_string())
    })?;
    if arr.len() > CYFS_SEMANTIC_LIST_MAX_CHILDREN {
        return Err(NdnError::InvalidData(format!(
            "loose semantic list child count {} exceeds max {}",
            arr.len(),
            CYFS_SEMANTIC_LIST_MAX_CHILDREN
        )));
    }

    let mut result = Vec::with_capacity(arr.len());
    for item in arr {
        result.push(ObjId::from_value(item)?);
    }
    Ok(result)
}

// =============================================================
// Header name constants
// =============================================================

pub const CYFS_HEADER_OBJ_ID: &str = "cyfs-obj-id";
pub const CYFS_HEADER_PATH_OBJ: &str = "cyfs-path-obj";
pub const CYFS_HEADER_PARENTS_PREFIX: &str = "cyfs-parents-";
pub const CYFS_HEADER_INNER_PROOF: &str = "cyfs-inner-proof";
pub const CYFS_HEADER_CHUNK_SIZE: &str = "cyfs-chunk-size";

pub const CYFS_HEADER_ORIGINAL_USER: &str = "cyfs-original-user";
pub const CYFS_HEADER_CASCADES: &str = "cyfs-cascades";
pub const CYFS_HEADER_PROOFS: &str = "cyfs-proofs";
pub const CYFS_HEADER_ACCESS_CODE: &str = "cyfs-access-code";

/// `cyfs-cascades` 上限（协议规定不超过 6 项）。
pub const CYFS_CASCADES_MAX_LEN: usize = 6;

// =============================================================
// cyfs-parents-N items
// =============================================================

/// A single entry from the `cyfs-parents-N` Header chain.
///
/// Per CYFS Protocol spec, each parent entry is encoded as:
///   - `oid:$objid`  – only the ObjectId is carried.
///   - `json:$base64url_canonical_json` – the full canonical JSON
///     of the NamedObject (base64url, no padding).
#[derive(Debug, Clone)]
pub enum CyfsParent {
    ObjId(ObjId),
    /// Canonical JSON bytes of the NamedObject (decoded from base64url).
    Json(String),
}

impl CyfsParent {
    /// If the entry carries a full NamedObject JSON, return it.
    pub fn as_json(&self) -> Option<&str> {
        match self {
            CyfsParent::Json(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// If the entry is an ObjId-only pointer, return it.
    pub fn as_obj_id(&self) -> Option<&ObjId> {
        match self {
            CyfsParent::ObjId(id) => Some(id),
            _ => None,
        }
    }

    pub fn encode_header_value(&self) -> String {
        match self {
            CyfsParent::ObjId(id) => format!("oid:{}", id.to_string()),
            CyfsParent::Json(json) => {
                let enc = URL_SAFE_NO_PAD.encode(json.as_bytes());
                format!("json:{}", enc)
            }
        }
    }

    pub fn decode_header_value(raw: &str) -> NdnResult<Self> {
        if let Some(rest) = raw.strip_prefix("oid:") {
            let id = ObjId::new(rest)?;
            return Ok(CyfsParent::ObjId(id));
        }
        if let Some(rest) = raw.strip_prefix("json:") {
            let bytes = URL_SAFE_NO_PAD.decode(rest.as_bytes()).map_err(|e| {
                NdnError::DecodeError(format!("decode cyfs-parents base64url failed: {}", e))
            })?;
            let json = String::from_utf8(bytes).map_err(|e| {
                NdnError::DecodeError(format!("decode cyfs-parents utf8 failed: {}", e))
            })?;
            return Ok(CyfsParent::Json(json));
        }
        Err(NdnError::DecodeError(format!(
            "unsupported cyfs-parents-N value: {}",
            raw
        )))
    }
}

// =============================================================
// Response headers
// =============================================================

#[derive(Debug, Clone, Default)]
pub struct CYFSHttpRespHeaders {
    /// `cyfs-obj-id` – ObjectId of the response body (if it is a
    /// NamedObject or Chunk / Range of Chunk).
    pub obj_id: Option<ObjId>,

    /// `cyfs-chunk-size` – the full chunk size (regardless of any
    /// HTTP Range trimming).
    pub chunk_size: Option<u64>,

    /// `cyfs-path-obj` – JWT binding a semantic path to a root ObjectId.
    pub path_obj: Option<String>,

    /// `cyfs-parents-N` – ordered parent objects along the inner_path
    /// chain, index starting at 0 with no gaps.
    pub parents: Vec<CyfsParent>,

    /// `cyfs-inner-proof` – inner_path proof (typically merkle proof
    /// for large-container access), encoded as a JSON array.
    pub inner_proofs: Vec<serde_json::Value>,
}

pub fn get_cyfs_resp_headers(headers: &HeaderMap) -> NdnResult<CYFSHttpRespHeaders> {
    debug!("get_cyfs_resp_headers: headers:{:?}", headers);

    let obj_id = header_get_str(headers, CYFS_HEADER_OBJ_ID)?
        .map(|s| ObjId::new(&s))
        .transpose()?;

    let chunk_size = header_get_str(headers, CYFS_HEADER_CHUNK_SIZE)?
        .map(|s| {
            s.parse::<u64>().map_err(|e| {
                NdnError::DecodeError(format!("parse {} failed: {}", CYFS_HEADER_CHUNK_SIZE, e))
            })
        })
        .transpose()?;

    let path_obj = header_get_str(headers, CYFS_HEADER_PATH_OBJ)?;

    // Walk cyfs-parents-0, cyfs-parents-1, ... – MUST be consecutive.
    let mut parents = Vec::new();
    let mut idx = 0usize;
    loop {
        let name = format!("{}{}", CYFS_HEADER_PARENTS_PREFIX, idx);
        let Some(raw) = header_get_str(headers, &name)? else {
            break;
        };
        parents.push(CyfsParent::decode_header_value(&raw)?);
        idx += 1;
    }

    let inner_proofs = match header_get_str(headers, CYFS_HEADER_INNER_PROOF)? {
        None => Vec::new(),
        Some(raw) => {
            let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                NdnError::DecodeError(format!(
                    "parse {} json failed: {}",
                    CYFS_HEADER_INNER_PROOF, e
                ))
            })?;
            match value {
                serde_json::Value::Array(arr) => arr,
                other => vec![other],
            }
        }
    };

    Ok(CYFSHttpRespHeaders {
        obj_id,
        chunk_size,
        path_obj,
        parents,
        inner_proofs,
    })
}

/// Serialize the response headers back into an HTTP HeaderMap.
pub fn apply_cyfs_resp_headers(
    resp: &CYFSHttpRespHeaders,
    headers: &mut HeaderMap,
) -> NdnResult<()> {
    if let Some(obj_id) = &resp.obj_id {
        insert_header(headers, CYFS_HEADER_OBJ_ID, &obj_id.to_string())?;
    }
    if let Some(size) = resp.chunk_size {
        insert_header(headers, CYFS_HEADER_CHUNK_SIZE, &size.to_string())?;
    }
    if let Some(path_obj) = &resp.path_obj {
        insert_header(headers, CYFS_HEADER_PATH_OBJ, path_obj)?;
    }
    for (i, parent) in resp.parents.iter().enumerate() {
        let name = format!("{}{}", CYFS_HEADER_PARENTS_PREFIX, i);
        insert_header(headers, &name, &parent.encode_header_value())?;
    }
    if !resp.inner_proofs.is_empty() {
        let raw = serde_json::to_string(&resp.inner_proofs).map_err(|e| {
            NdnError::Internal(format!(
                "serialize {} failed: {}",
                CYFS_HEADER_INNER_PROOF, e
            ))
        })?;
        insert_header(headers, CYFS_HEADER_INNER_PROOF, &raw)?;
    }
    Ok(())
}

// =============================================================
// Request headers
// =============================================================

#[derive(Debug, Clone, Default)]
pub struct CYFSHttpReqHeaders {
    /// `cyfs-original-user` – DID of the user who initiated this request.
    pub original_user: Option<String>,
    /// `cyfs-cascades` – upstream ActionObject chain (length <= 6).
    pub cascades: Option<Vec<serde_json::Value>>,
    /// `cyfs-proofs` – behaviour proofs (JWTs), e.g. purchase receipts.
    pub proofs: Option<Vec<String>>,
    /// `cyfs-access-code` – opaque access code or JWT.
    pub access_code: Option<String>,
}

pub fn get_cyfs_req_headers(headers: &HeaderMap) -> NdnResult<CYFSHttpReqHeaders> {
    let original_user = header_get_str(headers, CYFS_HEADER_ORIGINAL_USER)?;

    let cascades = match header_get_str(headers, CYFS_HEADER_CASCADES)? {
        None => None,
        Some(raw) => {
            let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                NdnError::DecodeError(format!("parse {} json failed: {}", CYFS_HEADER_CASCADES, e))
            })?;
            match value {
                serde_json::Value::Array(arr) => {
                    if arr.len() > CYFS_CASCADES_MAX_LEN {
                        return Err(NdnError::InvalidData(format!(
                            "{} length {} exceeds max {}",
                            CYFS_HEADER_CASCADES,
                            arr.len(),
                            CYFS_CASCADES_MAX_LEN
                        )));
                    }
                    Some(arr)
                }
                _ => {
                    return Err(NdnError::InvalidData(format!(
                        "{} must be a JSON array",
                        CYFS_HEADER_CASCADES
                    )));
                }
            }
        }
    };

    let proofs = match header_get_str(headers, CYFS_HEADER_PROOFS)? {
        None => None,
        Some(raw) => {
            let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                NdnError::DecodeError(format!("parse {} json failed: {}", CYFS_HEADER_PROOFS, e))
            })?;
            match value {
                serde_json::Value::Array(arr) => {
                    let mut out = Vec::with_capacity(arr.len());
                    for item in arr {
                        let s = item.as_str().ok_or_else(|| {
                            NdnError::InvalidData(format!(
                                "{} array items must be JWT strings",
                                CYFS_HEADER_PROOFS
                            ))
                        })?;
                        out.push(s.to_string());
                    }
                    Some(out)
                }
                _ => {
                    return Err(NdnError::InvalidData(format!(
                        "{} must be a JSON array",
                        CYFS_HEADER_PROOFS
                    )));
                }
            }
        }
    };

    let access_code = header_get_str(headers, CYFS_HEADER_ACCESS_CODE)?;

    Ok(CYFSHttpReqHeaders {
        original_user,
        cascades,
        proofs,
        access_code,
    })
}

pub fn apply_cyfs_req_headers(req: &CYFSHttpReqHeaders, headers: &mut HeaderMap) -> NdnResult<()> {
    if let Some(user) = &req.original_user {
        insert_header(headers, CYFS_HEADER_ORIGINAL_USER, user)?;
    }
    if let Some(cascades) = &req.cascades {
        if cascades.len() > CYFS_CASCADES_MAX_LEN {
            return Err(NdnError::InvalidData(format!(
                "{} length {} exceeds max {}",
                CYFS_HEADER_CASCADES,
                cascades.len(),
                CYFS_CASCADES_MAX_LEN
            )));
        }
        let raw = serde_json::to_string(cascades).map_err(|e| {
            NdnError::Internal(format!("serialize {} failed: {}", CYFS_HEADER_CASCADES, e))
        })?;
        insert_header(headers, CYFS_HEADER_CASCADES, &raw)?;
    }
    if let Some(proofs) = &req.proofs {
        let raw = serde_json::to_string(proofs).map_err(|e| {
            NdnError::Internal(format!("serialize {} failed: {}", CYFS_HEADER_PROOFS, e))
        })?;
        insert_header(headers, CYFS_HEADER_PROOFS, &raw)?;
    }
    if let Some(code) = &req.access_code {
        insert_header(headers, CYFS_HEADER_ACCESS_CODE, code)?;
    }
    Ok(())
}

// =============================================================
// Internal helpers
// =============================================================

fn header_get_str(headers: &HeaderMap, name: &str) -> NdnResult<Option<String>> {
    match headers.get(name) {
        None => Ok(None),
        Some(v) => {
            let s = v.to_str().map_err(|e| {
                NdnError::DecodeError(format!("decode header {} failed: {}", name, e))
            })?;
            Ok(Some(s.to_string()))
        }
    }
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> NdnResult<()> {
    let hname = HeaderName::from_str(name)
        .map_err(|e| NdnError::Internal(format!("invalid header name {}: {}", name, e)))?;
    let hval = HeaderValue::from_str(value)
        .map_err(|e| NdnError::Internal(format!("invalid header value for {}: {}", name, e)))?;
    headers.insert(hname, hval);
    Ok(())
}

fn parse_header_bool(name: &str, raw: &str) -> NdnResult<bool> {
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(NdnError::DecodeError(format!(
            "parse {} bool failed: {}",
            name, raw
        ))),
    }
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_inner_path() {
        let parsed =
            cyfs_parse_url("http://zone.example/all_images/@/readme/@/content?resp=raw").unwrap();
        assert_eq!(parsed.root_locator, "/all_images");
        assert_eq!(parsed.inner_path_steps, vec!["readme", "content"]);
        assert!(parsed.resp_raw);
    }

    #[test]
    fn test_dispatch_url_validation() {
        let parsed = cyfs_parse_url("http://zone.example/did:bns:alice/inbox").unwrap();
        validate_cyfs_dispatch_url(&parsed).unwrap();

        let with_inner =
            cyfs_parse_url("http://zone.example/did:bns:alice/inbox/@/content").unwrap();
        assert!(validate_cyfs_dispatch_url(&with_inner).is_err());

        let root_only = cyfs_parse_url("http://zone.example/").unwrap();
        assert!(validate_cyfs_dispatch_url(&root_only).is_err());
    }

    #[test]
    fn test_named_object_content_type() {
        assert!(is_cyfs_named_object_content_type(
            "application/cyfs-named-object+json"
        ));
        assert!(is_cyfs_named_object_content_type(
            "Application/CYFS-Named-Object+JSON; charset=utf-8"
        ));
        assert!(!is_cyfs_named_object_content_type("application/json"));
    }

    #[test]
    fn test_semantic_list_query_parse() {
        let parsed =
            cyfs_parse_url("http://zone.example/did:bns:group/inbox?list=loose&after=123&limit=10")
                .unwrap();
        let query = parse_cyfs_semantic_list_query(&parsed).unwrap();
        assert_eq!(query.mode, Some(CyfsSemanticListMode::Loose));
        assert_eq!(query.after.as_deref(), Some("123"));
        assert_eq!(query.limit, Some(10));

        let too_large = cyfs_parse_url(&format!(
            "http://zone.example/inbox?list=loose&limit={}",
            CYFS_SEMANTIC_LIST_MAX_CHILDREN + 1
        ))
        .unwrap();
        assert!(parse_cyfs_semantic_list_query(&too_large).is_err());
    }

    #[test]
    fn test_semantic_list_resp_headers_roundtrip() {
        let src = CyfsSemanticListRespHeaders {
            mode: Some(CyfsSemanticListMode::Loose),
            truncated: Some(true),
        };
        let mut headers = HeaderMap::new();
        apply_cyfs_semantic_list_resp_headers(&src, &mut headers).unwrap();
        let back = get_cyfs_semantic_list_resp_headers(&headers).unwrap();
        assert_eq!(back.mode, Some(CyfsSemanticListMode::Loose));
        assert_eq!(back.truncated, Some(true));
    }

    #[test]
    fn test_loose_list_body_parse() {
        let body = r#"["sha256:0203040506","cyobj:01020304"]"#;
        let ids = parse_cyfs_loose_list_body(body).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].to_string(), "sha256:0203040506");
        assert_eq!(ids[1].to_string(), "cyobj:01020304");

        assert!(parse_cyfs_loose_list_body(r#"{"id":"sha256:0203040506"}"#).is_err());
        assert!(parse_cyfs_loose_list_body(r#"[{"id":"sha256:0203040506"}]"#).is_err());
    }

    #[test]
    fn test_parents_roundtrip_oid() {
        let id = ObjId::new("sha256:0203040506").unwrap();
        let raw = CyfsParent::ObjId(id.clone()).encode_header_value();
        assert!(raw.starts_with("oid:"));
        let back = CyfsParent::decode_header_value(&raw).unwrap();
        match back {
            CyfsParent::ObjId(id2) => assert_eq!(id2.to_string(), "sha256:0203040506"),
            _ => panic!("expected ObjId"),
        }
    }

    #[test]
    fn test_parents_roundtrip_json() {
        let json = r#"{"a":1,"b":"hello"}"#;
        let raw = CyfsParent::Json(json.to_string()).encode_header_value();
        assert!(raw.starts_with("json:"));
        let back = CyfsParent::decode_header_value(&raw).unwrap();
        match back {
            CyfsParent::Json(s) => assert_eq!(s, json),
            _ => panic!("expected Json"),
        }
    }

    #[test]
    fn test_resp_headers_roundtrip() {
        let id = ObjId::new("sha256:0203040506").unwrap();
        let src = CYFSHttpRespHeaders {
            obj_id: Some(id.clone()),
            chunk_size: Some(1234),
            path_obj: Some("eyJhbGciOi...jwt".to_string()),
            parents: vec![CyfsParent::ObjId(id.clone())],
            inner_proofs: vec![serde_json::json!({"leaf_index": 12000})],
        };
        let mut headers = HeaderMap::new();
        apply_cyfs_resp_headers(&src, &mut headers).unwrap();
        let back = get_cyfs_resp_headers(&headers).unwrap();
        assert_eq!(back.obj_id.unwrap().to_string(), id.to_string());
        assert_eq!(back.chunk_size, Some(1234));
        assert_eq!(back.path_obj.as_deref(), Some("eyJhbGciOi...jwt"));
        assert_eq!(back.parents.len(), 1);
        assert_eq!(back.inner_proofs.len(), 1);
    }

    #[test]
    fn test_req_headers_roundtrip() {
        let src = CYFSHttpReqHeaders {
            original_user: Some("did:bns:alice".to_string()),
            cascades: Some(vec![serde_json::json!({"op": "view"})]),
            proofs: Some(vec!["jwt1".to_string(), "jwt2".to_string()]),
            access_code: Some("code-xyz".to_string()),
        };
        let mut headers = HeaderMap::new();
        apply_cyfs_req_headers(&src, &mut headers).unwrap();
        let back = get_cyfs_req_headers(&headers).unwrap();
        assert_eq!(back.original_user.as_deref(), Some("did:bns:alice"));
        assert_eq!(back.cascades.as_ref().unwrap().len(), 1);
        assert_eq!(back.proofs.as_ref().unwrap().len(), 2);
        assert_eq!(back.access_code.as_deref(), Some("code-xyz"));
    }
}
