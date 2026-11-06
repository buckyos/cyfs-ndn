//! What a response may claim about user content (buckyos bucket design D24).
//!
//! User content is served from origins that hold SSO credentials, so it must
//! never run there. Only types a browser shows without running content keep
//! their declared type and may render inline; anything else is
//! `application/octet-stream` and forced to download. Every response carries
//! `nosniff`, `no-referrer` and a CSP whose `sandbox` also covers types that
//! look passive but are not (`image/svg+xml`).

use reqwest::header::{self, HeaderMap, HeaderValue};

pub const OCTET_STREAM: &str = "application/octet-stream";
pub const SANDBOX_CSP: &str = "sandbox; default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'";
/// PDFs get no `sandbox`. Chromium 156 shows a PDF under a sandboxing CSP header (it
/// refuses only in `<iframe sandbox>`); other browsers have not been tried, so the
/// exception stays until they are. PDF script runs in the viewer, not in the origin.
pub const PDF_CSP: &str = "default-src 'none'; object-src 'self'";

/// Lowercase media type without parameters.
pub fn mime_essence(mime: &str) -> String {
    mime.split(';').next().unwrap_or("").trim().to_ascii_lowercase()
}

/// Types a browser renders without running the content.
pub fn is_inline_type(mime: &str) -> bool {
    let essence = mime_essence(mime);
    let major = essence.split('/').next().unwrap_or("");
    matches!(major, "image" | "video" | "audio") || essence == "application/pdf" || essence == "text/plain"
}

/// The inline type a file name suggests, for objects that declare none.
pub fn inline_mime_from_name(name: &str) -> Option<&'static str> {
    let ext = name.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "avif" => "image/avif",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "svg" => "image/svg+xml",
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" | "oga" => "audio/ogg",
        "pdf" => "application/pdf",
        "txt" | "log" => "text/plain",
        _ => return None,
    })
}

/// The headers one piece of user content gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentPolicy {
    pub content_type: String,
    pub inline: bool,
    pub csp: &'static str,
}

pub fn content_policy(mime: &str) -> ContentPolicy {
    let essence = mime_essence(mime);
    let inline = is_inline_type(&essence);
    let declared = mime.trim();
    let content_type = if !inline || HeaderValue::from_str(declared).is_err() {
        OCTET_STREAM.to_string()
    } else if essence == "text/plain" && !declared.to_ascii_lowercase().contains("charset=") {
        "text/plain; charset=utf-8".to_string()
    } else {
        declared.to_string()
    };
    let csp = if essence == "application/pdf" { PDF_CSP } else { SANDBOX_CSP };
    ContentPolicy { inline: inline && content_type != OCTET_STREAM, content_type, csp }
}

/// `inline` / `attachment` with an RFC 5987 file name.
pub fn content_disposition(kind: &str, file_name: Option<&str>) -> String {
    let name = file_name.map(str::trim).filter(|n| !n.is_empty());
    let Some(name) = name else { return kind.to_string() };
    let mut encoded = String::with_capacity(name.len());
    for byte in name.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => encoded.push(*byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    format!("{kind}; filename*=UTF-8''{encoded}")
}

/// `nosniff`, `no-referrer` and the CSP.
pub fn apply_security_headers(headers: &mut HeaderMap, csp: &'static str) {
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(csp));
}

/// The content headers of one response; replaces whatever is already there.
pub fn apply_content_policy(headers: &mut HeaderMap, mime: &str, file_name: Option<&str>, want_download: bool) -> ContentPolicy {
    let policy = content_policy(mime);
    if let Ok(v) = HeaderValue::from_str(&policy.content_type) {
        headers.insert(header::CONTENT_TYPE, v);
    }
    let kind = if policy.inline && !want_download { "inline" } else { "attachment" };
    if let Ok(v) = HeaderValue::from_str(&content_disposition(kind, file_name)) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    apply_security_headers(headers, policy.csp);
    policy
}

/// `Cache-Control` of a response allowed by identity or token: a stored header
/// may tune freshness but cannot admit shared caches (bucket design G5).
pub fn private_cache_control(stored: Option<&str>) -> String {
    const DEFAULT: &str = "private, no-cache";
    let Some(stored) = stored else { return DEFAULT.to_string() };
    let kept: Vec<&str> = stored
        .split(',')
        .map(str::trim)
        .filter(|d| {
            let name = d.split('=').next().unwrap_or("").trim().to_ascii_lowercase();
            !name.is_empty() && !matches!(name.as_str(), "public" | "s-maxage" | "proxy-revalidate" | "private")
        })
        .collect();
    if kept.is_empty() {
        return DEFAULT.to_string();
    }
    if kept.iter().any(|d| d.eq_ignore_ascii_case("no-store")) {
        return "no-store".to_string();
    }
    format!("private, {}", kept.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_passive_types_keep_their_type_and_render() {
        for mime in ["image/png", "IMAGE/PNG", "video/mp4", "audio/mpeg", "application/pdf"] {
            let p = content_policy(mime);
            assert!(p.inline, "{mime}");
            assert_eq!(p.content_type, mime.trim());
        }
        assert_eq!(content_policy("text/plain").content_type, "text/plain; charset=utf-8");
        assert_eq!(content_policy("text/plain; charset=gbk").content_type, "text/plain; charset=gbk");
        for mime in ["text/html", "text/HTML; charset=utf-8", "application/xhtml+xml", "text/javascript", "application/zip", "application/json", "text/xml", "", "garbage"] {
            let p = content_policy(mime);
            assert!(!p.inline, "{mime}");
            assert_eq!(p.content_type, OCTET_STREAM, "{mime}");
            assert_eq!(p.csp, SANDBOX_CSP);
        }
        assert_eq!(content_policy("application/pdf").csp, PDF_CSP);
    }

    #[test]
    fn svg_renders_only_in_a_sandbox() {
        let p = content_policy("image/svg+xml");
        assert!(p.inline);
        assert!(p.csp.starts_with("sandbox"));
    }

    #[test]
    fn a_header_breaking_type_is_opaque() {
        let p = content_policy("image/png\r\nset-cookie: a=b");
        assert_eq!(p.content_type, OCTET_STREAM);
        assert!(!p.inline);
    }

    #[test]
    fn apply_replaces_and_forces_download() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html"));
        h.insert(header::CONTENT_DISPOSITION, HeaderValue::from_static("inline"));
        apply_content_policy(&mut h, "text/html", Some("a b.html"), false);
        assert_eq!(h[header::CONTENT_TYPE], OCTET_STREAM);
        assert_eq!(h[header::CONTENT_DISPOSITION], "attachment; filename*=UTF-8''a%20b.html");
        assert_eq!(h[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(h[header::REFERRER_POLICY], "no-referrer");
        assert!(h[header::CONTENT_SECURITY_POLICY].to_str().unwrap().starts_with("sandbox"));

        apply_content_policy(&mut h, "image/png", None, false);
        assert_eq!(h[header::CONTENT_DISPOSITION], "inline");
        apply_content_policy(&mut h, "image/png", None, true);
        assert_eq!(h[header::CONTENT_DISPOSITION], "attachment");
    }

    #[test]
    fn stored_cache_control_cannot_admit_shared_caches() {
        assert_eq!(private_cache_control(None), "private, no-cache");
        assert_eq!(private_cache_control(Some("public, max-age=600")), "private, max-age=600");
        assert_eq!(private_cache_control(Some("PUBLIC, s-maxage=9, max-age=1")), "private, max-age=1");
        assert_eq!(private_cache_control(Some("public")), "private, no-cache");
        assert_eq!(private_cache_control(Some("private, max-age=5")), "private, max-age=5");
        assert_eq!(private_cache_control(Some("no-store")), "no-store");
    }
}
