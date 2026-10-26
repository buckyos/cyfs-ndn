//! AWS Signature Version 4 for S3: the `Authorization` header and presigned
//! query forms, verified against a given secret.

use crate::error::S3Error;
use hmac::{Hmac, Mac};
use http::HeaderMap;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
/// Presigned URLs live at most 7 days, as on S3.
pub const MAX_PRESIGN_SECS: i64 = 7 * 24 * 3600;
const MAX_SKEW_SECS: i64 = 15 * 60;

pub fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// AWS `UriEncode`: unreserved characters stay, everything else is `%XX` (upper case).
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |b: u8| (b as char).to_digit(16);
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Query pairs, percent-decoded, in request order.
pub fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(&v.replace('+', "%20"))),
            None => (percent_decode(p), String::new()),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub access_key_id: String,
    pub date: String,
    pub region: String,
    pub service: String,
}

impl Credential {
    fn parse(v: &str) -> Result<Self, S3Error> {
        let parts: Vec<&str> = v.split('/').collect();
        if parts.len() != 5 || parts[4] != "aws4_request" {
            return Err(S3Error::auth("AuthorizationHeaderMalformed", "the credential scope is malformed"));
        }
        Ok(Credential { access_key_id: parts[0].into(), date: parts[1].into(), region: parts[2].into(), service: parts[3].into() })
    }

    pub fn scope(&self) -> String {
        format!("{}/{}/{}/aws4_request", self.date, self.region, self.service)
    }
}

#[derive(Debug, Clone)]
pub struct SignedRequest {
    pub credential: Credential,
    pub amz_date: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
    /// Presigned query form: seconds it stays valid.
    pub presigned_expires: Option<i64>,
    /// `x-amz-content-sha256` (or `UNSIGNED-PAYLOAD` for presigned URLs).
    pub payload_hash: String,
}

/// The key and scope that signed a request; aws-chunked bodies chain their chunk signatures from it.
#[derive(Debug, Clone)]
pub struct SigningContext {
    pub key: [u8; 32],
    pub amz_date: String,
    pub scope: String,
    pub seed_signature: String,
}

impl SigningContext {
    pub fn chunk_signature(&self, previous: &str, chunk_sha256_hex: &str) -> String {
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.amz_date, self.scope, previous, EMPTY_SHA256, chunk_sha256_hex
        );
        hex::encode(hmac(&self.key, to_sign.as_bytes()))
    }
}

pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let values: Vec<String> = headers.get_all(name).iter().filter_map(|v| v.to_str().ok()).map(collapse).collect();
    if values.is_empty() {
        if name == "host" {
            return headers.get("x-forwarded-host").and_then(|v| v.to_str().ok()).map(collapse);
        }
        return None;
    }
    Some(values.join(","))
}

fn collapse(v: &str) -> String {
    v.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The signature parts of a request; `None` for an anonymous request.
pub fn parse(query: &[(String, String)], headers: &HeaderMap) -> Result<Option<SignedRequest>, S3Error> {
    let q = |name: &str| query.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    if let Some(algorithm) = q("X-Amz-Algorithm") {
        if algorithm != ALGORITHM {
            return Err(S3Error::auth("AuthorizationQueryParametersError", "only AWS4-HMAC-SHA256 is supported"));
        }
        let missing = || S3Error::auth("AuthorizationQueryParametersError", "presigned query parameters are incomplete");
        let expires: i64 = q("X-Amz-Expires").and_then(|v| v.parse().ok()).ok_or_else(missing)?;
        if !(1..=MAX_PRESIGN_SECS).contains(&expires) {
            return Err(S3Error::auth("AuthorizationQueryParametersError", "X-Amz-Expires must be 1..=604800"));
        }
        return Ok(Some(SignedRequest {
            credential: Credential::parse(&q("X-Amz-Credential").ok_or_else(missing)?)?,
            amz_date: q("X-Amz-Date").ok_or_else(missing)?,
            signed_headers: q("X-Amz-SignedHeaders").ok_or_else(missing)?.split(';').map(str::to_string).collect(),
            signature: q("X-Amz-Signature").ok_or_else(missing)?,
            presigned_expires: Some(expires),
            payload_hash: header_value(headers, "x-amz-content-sha256").unwrap_or_else(|| UNSIGNED_PAYLOAD.into()),
        }));
    }
    let Some(auth) = headers.get(http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else { return Ok(None) };
    let rest = auth
        .strip_prefix(ALGORITHM)
        .ok_or_else(|| S3Error::auth("AuthorizationHeaderMalformed", "only AWS4-HMAC-SHA256 is supported"))?;
    let (mut credential, mut signed, mut signature) = (None, None, None);
    for part in rest.split(',') {
        let Some((k, v)) = part.trim().split_once('=') else { continue };
        match k.trim() {
            "Credential" => credential = Some(Credential::parse(v.trim())?),
            "SignedHeaders" => signed = Some(v.trim().split(';').map(str::to_string).collect::<Vec<_>>()),
            "Signature" => signature = Some(v.trim().to_string()),
            _ => {}
        }
    }
    let malformed = || S3Error::auth("AuthorizationHeaderMalformed", "the Authorization header is malformed");
    let amz_date = header_value(headers, "x-amz-date").ok_or_else(|| S3Error::auth("AccessDenied", "x-amz-date is required"))?;
    Ok(Some(SignedRequest {
        credential: credential.ok_or_else(malformed)?,
        amz_date,
        signed_headers: signed.ok_or_else(malformed)?,
        signature: signature.ok_or_else(malformed)?,
        presigned_expires: None,
        payload_hash: header_value(headers, "x-amz-content-sha256").unwrap_or_else(|| UNSIGNED_PAYLOAD.into()),
    }))
}

pub fn canonical_request(method: &str, raw_path: &str, query: &[(String, String)], headers: &HeaderMap, signed_headers: &[String], payload_hash: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| k != "X-Amz-Signature")
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    let canonical_query = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let canonical_headers: String =
        signed_headers.iter().map(|h| format!("{}:{}\n", h, header_value(headers, h).unwrap_or_default())).collect();
    let path = if raw_path.is_empty() { "/" } else { raw_path };
    format!("{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{}\n{payload_hash}", signed_headers.join(";"))
}

fn parse_amz_date(v: &str) -> Result<i64, S3Error> {
    chrono::NaiveDateTime::parse_from_str(v, "%Y%m%dT%H%M%SZ")
        .map(|t| t.and_utc().timestamp())
        .map_err(|_| S3Error::auth("AccessDenied", "x-amz-date is malformed"))
}

fn constant_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Checks the signature with `secret` at time `now` (unix seconds).
pub fn verify(
    req: &SignedRequest,
    secret: &str,
    method: &str,
    raw_path: &str,
    query: &[(String, String)],
    headers: &HeaderMap,
    now: i64,
) -> Result<SigningContext, S3Error> {
    let signed_at = parse_amz_date(&req.amz_date)?;
    if !req.amz_date.starts_with(&req.credential.date) {
        return Err(S3Error::auth("AuthorizationHeaderMalformed", "the credential date differs from x-amz-date"));
    }
    match req.presigned_expires {
        Some(expires) => {
            if now > signed_at + expires {
                return Err(S3Error::auth("AccessDenied", "Request has expired"));
            }
            if signed_at > now + MAX_SKEW_SECS {
                return Err(S3Error::auth("AccessDenied", "Request is not yet valid"));
            }
        }
        None if (now - signed_at).abs() > MAX_SKEW_SECS => {
            return Err(S3Error::auth("RequestTimeTooSkewed", "the difference between the request time and the server's time is too large"));
        }
        None => {}
    }
    if !req.signed_headers.iter().any(|h| h == "host") {
        return Err(S3Error::auth("AccessDenied", "the host header must be signed"));
    }
    let payload_hash = if req.presigned_expires.is_some() { UNSIGNED_PAYLOAD } else { req.payload_hash.as_str() };
    let canonical = canonical_request(method, raw_path, query, headers, &req.signed_headers, payload_hash);
    let scope = req.credential.scope();
    let to_sign = format!("{ALGORITHM}\n{}\n{}\n{}", req.amz_date, scope, sha256_hex(canonical.as_bytes()));
    let key = signing_key(secret, &req.credential.date, &req.credential.region, &req.credential.service);
    let expected = hex::encode(hmac(&key, to_sign.as_bytes()));
    if !constant_eq(&expected, &req.signature) {
        log::debug!("s3 signature mismatch; canonical request:\n{canonical}");
        return Err(S3Error::auth("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided"));
    }
    Ok(SigningContext { key, amz_date: req.amz_date.clone(), scope, seed_signature: req.signature.clone() })
}

/// Signs a request the way an S3 client does (tests, and the host's own presigned URLs).
pub fn sign(
    secret: &str,
    access_key_id: &str,
    region: &str,
    amz_date: &str,
    method: &str,
    raw_path: &str,
    query: &[(String, String)],
    headers: &HeaderMap,
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    let date = &amz_date[..8];
    let credential = Credential { access_key_id: access_key_id.into(), date: date.into(), region: region.into(), service: "s3".into() };
    let canonical = canonical_request(method, raw_path, query, headers, signed_headers, payload_hash);
    let to_sign = format!("{ALGORITHM}\n{amz_date}\n{}\n{}", credential.scope(), sha256_hex(canonical.as_bytes()));
    hex::encode(hmac(&signing_key(secret, date, region, "s3"), to_sign.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The GET Object example of the AWS SigV4 documentation.
    #[test]
    fn aws_documented_example() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert("range", "bytes=0-9".parse().unwrap());
        headers.insert("x-amz-content-sha256", EMPTY_SHA256.parse().unwrap());
        headers.insert("x-amz-date", "20130524T000000Z".parse().unwrap());
        let signed: Vec<String> = ["host", "range", "x-amz-content-sha256", "x-amz-date"].iter().map(|s| s.to_string()).collect();
        let sig = sign(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "AKIAIOSFODNN7EXAMPLE",
            "us-east-1",
            "20130524T000000Z",
            "GET",
            "/test.txt",
            &[],
            &headers,
            &signed,
            EMPTY_SHA256,
        );
        assert_eq!(sig, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
        headers.insert(
            http::header::AUTHORIZATION,
            format!(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature={sig}"
            )
            .parse()
            .unwrap(),
        );
        let req = parse(&[], &headers).unwrap().unwrap();
        let now = 1369353600;
        let ctx = verify(&req, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "GET", "/test.txt", &[], &headers, now).unwrap();
        assert_eq!(ctx.scope, "20130524/us-east-1/s3/aws4_request");
        assert!(verify(&req, "wrong", "GET", "/test.txt", &[], &headers, now).is_err());
        assert!(verify(&req, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "GET", "/test.txt", &[], &headers, now + 3600).is_err());
    }

    /// The first chunk of the AWS streaming (aws-chunked) PUT example.
    #[test]
    fn aws_documented_chunk_signature() {
        let key = signing_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3");
        let ctx = SigningContext {
            key,
            amz_date: "20130524T000000Z".into(),
            scope: "20130524/us-east-1/s3/aws4_request".into(),
            seed_signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9".into(),
        };
        let chunk = vec![b'a'; 65536];
        assert_eq!(
            ctx.chunk_signature(&ctx.seed_signature, &sha256_hex(&chunk)),
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        );
    }

    #[test]
    fn encoding() {
        assert_eq!(uri_encode("a b/c~", true), "a%20b%2Fc~");
        assert_eq!(uri_encode("a b/c", false), "a%20b/c");
        assert_eq!(percent_decode("a%20b%2fc%zz"), "a b/c%zz");
        assert_eq!(parse_query("list-type=2&prefix=a%2Fb&delete"), vec![
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), "a/b".to_string()),
            ("delete".to_string(), String::new()),
        ]);
    }
}
