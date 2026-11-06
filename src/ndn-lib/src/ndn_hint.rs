//! The `hint` of `/ndn/<obj_id>` (buckyos bucket design §4.6.5): canonical JSON
//! carrying the authorization root's path and a read token, shared by the
//! server, the pull client and URL builders so there is one spelling.

use crate::{ObjId, CYFS_INNER_PATH_DELIMITER};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const NDN_PATH_PREFIX: &str = "/ndn/";
pub const NDN_MAX_HINT_BYTES: usize = 2048;

/// A hint that is wrong whatever object it names (HTTP 400); says nothing about any object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NdnHintError(pub &'static str);

impl fmt::Display for NdnHintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NdnHint {
    /// `<root_obj_id>/@/<step>/@/<step>`; the walk must end at the URL's object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `bkt_` read token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// Tokens stay out of logs even when a hint is debug-printed.
impl fmt::Debug for NdnHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NdnHint").field("path", &self.path).field("token", &self.token.as_ref().map(|_| "***")).finish()
    }
}

impl NdnHint {
    pub fn is_empty(&self) -> bool {
        self.path.is_none() && self.token.is_none()
    }

    /// The query value before URL encoding: sorted keys, no whitespace.
    pub fn to_canonical(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// `hint` has one spelling: URLs key caches.
    pub fn parse(raw: &str) -> Result<NdnHint, NdnHintError> {
        if raw.len() > NDN_MAX_HINT_BYTES {
            return Err(NdnHintError("hint is longer than 2 KiB"));
        }
        let hint: NdnHint = serde_json::from_str(raw).map_err(|_| NdnHintError("hint is not valid JSON"))?;
        if hint.to_canonical() != raw {
            return Err(NdnHintError("hint is not canonical JSON"));
        }
        Ok(hint)
    }

    /// `hint=<url encoded canonical JSON>`, nothing when the hint is empty.
    pub fn to_query(&self) -> Option<String> {
        (!self.is_empty()).then(|| format!("hint={}", percent_encode_component(&self.to_canonical())))
    }
}

/// `encodeURIComponent`: what a URL builder on any platform must produce for the hint.
pub fn percent_encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `/ndn/<obj_id>[?hint=...]`.
pub fn ndn_read_path(obj_id: &ObjId, hint: Option<&NdnHint>) -> String {
    let path = format!("{NDN_PATH_PREFIX}{}", obj_id.to_string());
    match hint.and_then(NdnHint::to_query) {
        Some(query) => format!("{path}?{query}"),
        None => path,
    }
}

/// `<base>/@/<walk>`.
pub fn ndn_join_path(base: &str, walk: &str) -> String {
    format!("{base}{CYFS_INNER_PATH_DELIMITER}{walk}")
}

/// A URL without its query: what errors and logs may show of an `/ndn/` URL.
pub fn redact_url_query(url: &str) -> String {
    match url.split_once('?') {
        Some((head, _)) => format!("{head}?<redacted>"),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_form_is_the_only_spelling() {
        let hint = NdnHint { path: Some("cyfile:ab/@/content".into()), token: Some("bkt_x".into()) };
        let raw = hint.to_canonical();
        assert_eq!(raw, r#"{"path":"cyfile:ab/@/content","token":"bkt_x"}"#);
        assert_eq!(NdnHint::parse(&raw).unwrap(), hint);
        assert!(NdnHint::parse(r#"{"token":"bkt_x","path":"cyfile:ab/@/content"}"#).is_err());
        assert!(NdnHint::parse(r#"{"token": "x"}"#).is_err());
        assert!(NdnHint::parse(r#"{"other":"x"}"#).is_err());
        assert!(NdnHint::parse(r#"{"path":1}"#).is_err());
        assert!(NdnHint::parse("nope").is_err());
        assert!(NdnHint::parse(&format!(r#"{{"token":"{}"}}"#, "x".repeat(NDN_MAX_HINT_BYTES))).is_err());
    }

    #[test]
    fn queries_and_redaction() {
        let hint = NdnHint { path: None, token: Some("bkt_x y".into()) };
        assert_eq!(hint.to_query().unwrap(), "hint=%7B%22token%22%3A%22bkt_x%20y%22%7D");
        assert_eq!(NdnHint::default().to_query(), None);
        assert!(!format!("{hint:?}").contains("bkt_x"));
        assert_eq!(redact_url_query("https://z/ndn/a?hint=%7B"), "https://z/ndn/a?<redacted>");
        assert_eq!(redact_url_query("https://z/ndn/a"), "https://z/ndn/a");
    }
}
