use crate::{build_obj_id, NdnError, NdnResult, ObjId};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use url::Url;

pub const CYFS_HEADER_DISPATCH_STATUS: &str = "cyfs-dispatch-status";
pub const CYFS_DISPATCH_ERROR_UNKNOWN: &str = "unknown-dispatch";
pub const CYFS_DISPATCH_ERROR_OUTCOME_UNKNOWN: &str = "upstream-outcome-unknown";

fn invalid(message: impl Into<String>) -> NdnError {
    NdnError::InvalidData(message.into())
}

/// Canonical delivery identity. Percent-encoded unreserved bytes are decoded;
/// reserved bytes remain encoded (uppercase hex), preserving path boundaries.
pub fn normalize_cyfs_dispatch_target(zone: &str, path: &str) -> NdnResult<String> {
    if zone.is_empty()
        || zone.trim() != zone
        || zone.bytes().any(|b| b.is_ascii_control())
        || zone.contains(['/', '?', '#', '@', '\\', '%'])
        || (zone.contains(':') && !(zone.starts_with('[') && zone.ends_with(']')))
    {
        return Err(invalid("invalid dispatch zone"));
    }
    let authority =
        Url::parse(&format!("http://{zone}/")).map_err(|_| invalid("invalid dispatch zone"))?;
    if authority.host_str().is_none() || authority.port().is_some() || zone.ends_with(':') {
        return Err(invalid("dispatch zone must be a host without a port"));
    }
    if !path.starts_with('/') || path == "/" || path.contains(['?', '#', '\\']) {
        return Err(invalid("dispatch requires a semantic path"));
    }
    let mut normalized = String::new();
    let bytes = path.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let pair = bytes
                .get(i + 1..i + 3)
                .ok_or_else(|| invalid("invalid path escape"))?;
            let pair = std::str::from_utf8(pair).map_err(|_| invalid("invalid path escape"))?;
            let decoded =
                u8::from_str_radix(pair, 16).map_err(|_| invalid("invalid path escape"))?;
            if decoded.is_ascii_alphanumeric() || b"-._~".contains(&decoded) {
                normalized.push(decoded as char);
            } else if b"/@\\".contains(&decoded) || decoded.is_ascii_control() {
                return Err(invalid(
                    "escaped path separator or inner_path is not allowed",
                ));
            } else {
                normalized.push_str(&format!("%{decoded:02X}"));
            }
            i += 3;
        } else {
            if !b.is_ascii() || b.is_ascii_control() || b == b' ' {
                return Err(invalid("path must be URL encoded"));
            }
            normalized.push(b as char);
            i += 1;
        }
    }
    if normalized
        .split('/')
        .skip(1)
        .any(|s| s.is_empty() || matches!(s, "." | ".." | "@"))
    {
        return Err(invalid("ambiguous path or inner_path is not allowed"));
    }
    let host = authority
        .host_str()
        .unwrap()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host.is_empty() {
        return Err(invalid("invalid dispatch zone"));
    }
    Ok(format!("cyfs://{host}{normalized}"))
}

/// A type is not inferable from arbitrary NamedObject business fields. A typed
/// object supplies cyfs-obj-id; an untyped JSON object uses the jobj namespace.
pub fn validate_cyfs_dispatch_object(body: &[u8], claimed: Option<&str>) -> NdnResult<ObjId> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| invalid("invalid NamedObject JSON"))?;
    if !value.is_object() {
        return Err(invalid("dispatch body must be a JSON object"));
    }
    let canonical = serde_jcs::to_vec(&value).map_err(|_| invalid("invalid canonical JSON"))?;
    if canonical != body {
        return Err(invalid("dispatch body must be canonical JSON"));
    }
    let claimed = claimed.map(ObjId::new).transpose()?;
    let ty = claimed
        .as_ref()
        .map(|id| id.obj_type.as_str())
        .unwrap_or("jobj");
    if ty.is_empty() || ty.len() > 64 || !ty.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(invalid("invalid object type"));
    }
    let id = build_obj_id(
        ty,
        std::str::from_utf8(body).map_err(|_| invalid("invalid UTF-8"))?,
    );
    if id.is_chunk() || ty == "pack" || claimed.as_ref().is_some_and(|v| *v != id) {
        return Err(invalid("dispatch ObjectId does not match NamedObject"));
    }
    Ok(id)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CyfsDispatchStatus {
    Accepted,
    Cached,
    Rejected,
}

impl CyfsDispatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Cached => "cached",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CyfsDispatchSource {
    Upstream,
    Cache,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CyfsDispatchResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub obj_id: Option<ObjId>,
    pub target: String,
    pub status: CyfsDispatchStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<CyfsDispatchSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

impl CyfsDispatchResult {
    pub fn new(obj_id: Option<ObjId>, target: String, status: CyfsDispatchStatus) -> Self {
        Self {
            obj_id,
            target,
            status,
            reason: None,
            retryable: None,
            source: None,
            expires_at_ms: None,
        }
    }

    pub fn rejected(
        obj_id: Option<ObjId>,
        target: String,
        reason: impl Into<String>,
        retryable: bool,
    ) -> Self {
        let mut result = Self::new(obj_id, target, CyfsDispatchStatus::Rejected);
        result.reason = Some(reason.into());
        result.retryable = Some(retryable);
        result
    }

    pub fn apply_headers(&self, headers: &mut HeaderMap) {
        headers.insert(
            CYFS_HEADER_DISPATCH_STATUS,
            self.status.as_str().parse().unwrap(),
        );
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("cache-control", "no-store".parse().unwrap());
    }
}

pub fn parse_cyfs_dispatch_result(
    http_status: u16,
    headers: &HeaderMap,
    body: &[u8],
    expected_id: &ObjId,
    expected_target: &str,
    query: bool,
) -> NdnResult<CyfsDispatchResult> {
    let result: CyfsDispatchResult =
        serde_json::from_slice(body).map_err(|_| invalid("invalid dispatch status body"))?;
    let header = headers
        .get(CYFS_HEADER_DISPATCH_STATUS)
        .and_then(|v| v.to_str().ok());
    let json = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/json")
        });
    let no_store = headers
        .get_all("cache-control")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| {
            v.split(',')
                .any(|v| v.trim().eq_ignore_ascii_case("no-store"))
        });
    if !json
        || !no_store
        || header != Some(result.status.as_str())
        || result.target != expected_target
    {
        return Err(invalid("dispatch response headers or target do not match"));
    }
    if result.obj_id.as_ref().is_some_and(|id| id != expected_id)
        || (result.status != CyfsDispatchStatus::Rejected
            && result.obj_id.as_ref() != Some(expected_id))
    {
        return Err(invalid("dispatch response ObjectId does not match"));
    }
    if result.status == CyfsDispatchStatus::Rejected
        && (result.reason.as_deref().is_none_or(str::is_empty) || result.retryable.is_none())
    {
        return Err(invalid("rejected dispatch requires reason and retryable"));
    }
    let valid_status = if query {
        http_status == 200 && result.source.is_some()
    } else {
        match result.status {
            CyfsDispatchStatus::Accepted => matches!(http_status, 200 | 201),
            CyfsDispatchStatus::Cached => http_status == 202,
            CyfsDispatchStatus::Rejected => (400..600).contains(&http_status),
        }
    };
    if !valid_status {
        return Err(invalid("HTTP status disagrees with dispatch status"));
    }
    Ok(result)
}

pub fn parse_cyfs_dispatch_status_query(query: &str) -> NdnResult<ObjId> {
    let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
    if pairs.len() != 1 || pairs[0].0 != "dispatch-status" {
        return Err(invalid(
            "expected exactly one dispatch-status query parameter",
        ));
    }
    let id = ObjId::new(&pairs[0].1)?;
    if id.obj_type.is_empty() || id.obj_hash.len() != 32 || id.is_chunk() {
        return Err(invalid("invalid dispatch status ObjectId"));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_target_and_object_identity() {
        assert_eq!(
            normalize_cyfs_dispatch_target("ALICE.example.", "/%69nbox").unwrap(),
            "cyfs://alice.example/inbox"
        );
        for path in [
            "/", "/a/../b", "/a/%2e/b", "/a/@/b", "/a/%40/b", "/a%2fb", "/a//b", "/a?b", "/a/%",
        ] {
            assert!(
                normalize_cyfs_dispatch_target("alice.example", path).is_err(),
                "{path}"
            );
        }
        let body = br#"{"a":1}"#;
        let id = validate_cyfs_dispatch_object(body, None).unwrap();
        assert_eq!(
            validate_cyfs_dispatch_object(body, Some(&id.to_base32())).unwrap(),
            id
        );
        for bad in [
            br#"{ "a":1}"#.as_slice(),
            br#"{"a":1,"a":1}"#,
            b"[]",
            br#"{"a":1.0}"#,
        ] {
            assert!(validate_cyfs_dispatch_object(bad, None).is_err());
        }
        assert!(validate_cyfs_dispatch_object(body, Some("jobj:00")).is_err());
    }

    #[test]
    fn dispatch_results_require_protocol_confirmation() {
        let id = validate_cyfs_dispatch_object(b"{}", None).unwrap();
        let target = "cyfs://alice.example/inbox";
        for (status, code) in [
            (CyfsDispatchStatus::Accepted, 201),
            (CyfsDispatchStatus::Cached, 202),
            (CyfsDispatchStatus::Rejected, 403),
        ] {
            let mut result = CyfsDispatchResult::new(Some(id.clone()), target.into(), status);
            if status == CyfsDispatchStatus::Rejected {
                result.reason = Some("denied".into());
                result.retryable = Some(false);
            }
            let mut headers = HeaderMap::new();
            result.apply_headers(&mut headers);
            let body = serde_json::to_vec(&result).unwrap();
            assert_eq!(
                parse_cyfs_dispatch_result(code, &headers, &body, &id, target, false).unwrap(),
                result
            );
            assert!(parse_cyfs_dispatch_result(204, &headers, &body, &id, target, false).is_err());
            assert!(parse_cyfs_dispatch_result(
                code,
                &headers,
                &body,
                &id,
                "cyfs://alice.example/other",
                false
            )
            .is_err());
            headers.remove(CYFS_HEADER_DISPATCH_STATUS);
            assert!(parse_cyfs_dispatch_result(code, &headers, &body, &id, target, false).is_err());
        }
        let query = format!("dispatch-status={}", id.to_base32());
        assert_eq!(parse_cyfs_dispatch_status_query(&query).unwrap(), id);
        assert!(parse_cyfs_dispatch_status_query(&format!("{query}&{query}")).is_err());
    }
}
