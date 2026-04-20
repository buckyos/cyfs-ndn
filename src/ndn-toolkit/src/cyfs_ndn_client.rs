use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use name_lib::decode_jwt_claim_without_verify;
use named_store::{ChunkLocalInfo, NamedDataMgr};
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::Value;
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::io::StreamReader;

use ndn_lib::{
    apply_cyfs_req_headers, build_named_object_by_json, caculate_qcid_from_file, copy_chunk,
    cyfs_parse_url, get_cyfs_resp_headers, verify_named_object_from_str, CYFSHttpReqHeaders,
    CYFSHttpRespHeaders, ChunkHasher, ChunkId, ChunkList, ChunkReader, CyfsParent, CyfsParsedUrl,
    FileObject, NdnAction, NdnError, NdnProgressCallback, NdnResult, ObjId, PathObject,
    ProgressCallbackResult, StoreMode, CYFS_CASCADES_MAX_LEN, OBJ_TYPE_CHUNK_LIST, OBJ_TYPE_DIR,
    OBJ_TYPE_FILE, OBJ_TYPE_PATH,
};

// =====================================================================
// Path object verification (pluggable)
// =====================================================================

/// Result of verifying a `cyfs-path-obj` JWT.
#[derive(Debug, Clone)]
pub struct VerifiedPathObject {
    pub path: String,
    pub target: ObjId,
    pub iat: u64,
    pub exp: u64,
}

/// Verifier for `cyfs-path-obj` JWTs.
///
/// Production deployments should supply an implementation that resolves the
/// trusted public key for the request hostname (via DID Document / BNS / DNS)
/// and validates the JWT signature + `alg` + `kid`. The default verifier only
/// checks `iat`/`exp` freshness; signature verification is intentionally
/// deferred to host-aware implementations.
pub trait PathObjectVerifier: Send + Sync {
    fn verify(&self, jwt: &str, host: Option<&str>) -> NdnResult<VerifiedPathObject>;
}

/// Default verifier: decode JWT claims without signature check, enforce
/// `iat <= now < exp`. Intended for tests and sandbox use.
#[derive(Debug, Default, Clone)]
pub struct AcceptIfFreshVerifier;

impl PathObjectVerifier for AcceptIfFreshVerifier {
    fn verify(&self, jwt: &str, _host: Option<&str>) -> NdnResult<VerifiedPathObject> {
        let claims = decode_jwt_claim_without_verify(jwt).map_err(|e| {
            NdnError::DecodeError(format!("decode cyfs-path-obj jwt failed: {}", e))
        })?;

        let path_obj: PathObject = serde_json::from_value(claims).map_err(|e| {
            NdnError::InvalidData(format!("parse cyfs-path-obj jwt claims failed: {}", e))
        })?;

        let now = buckyos_kit::buckyos_get_unix_timestamp();
        if path_obj.iat > now + 60 {
            return Err(NdnError::InvalidData(format!(
                "cyfs-path-obj iat {} is in the future (now={})",
                path_obj.iat, now
            )));
        }
        if path_obj.exp <= now {
            return Err(NdnError::InvalidData(format!(
                "cyfs-path-obj expired at {} (now={})",
                path_obj.exp, now
            )));
        }

        Ok(VerifiedPathObject {
            path: path_obj.path,
            target: path_obj.target,
            iat: path_obj.iat,
            exp: path_obj.exp,
        })
    }
}

// =====================================================================
// Transport abstraction
// =====================================================================

pub struct CyfsTransportRequest {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
}

pub struct CyfsTransportResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub content_length: Option<u64>,
    pub body: ChunkReader,
}

type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = NdnResult<CyfsTransportResponse>> + Send + 'a>>;

pub trait CyfsHttpTransport: Send + Sync {
    fn send(&self, request: CyfsTransportRequest) -> TransportFuture<'_>;
}

#[derive(Clone)]
pub struct ReqwestCyfsTransport {
    client: Client,
}

impl ReqwestCyfsTransport {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[derive(Clone)]
pub struct HyperConnectorTransport<C> {
    client: HyperClient<C, Empty<Bytes>>,
}

impl<C> HyperConnectorTransport<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    pub fn new(connector: C) -> Self {
        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }
}

impl CyfsHttpTransport for ReqwestCyfsTransport {
    fn send(&self, request: CyfsTransportRequest) -> TransportFuture<'_> {
        Box::pin(async move {
            let mut req = self.client.request(request.method, request.url);
            for (name, value) in request.headers.iter() {
                req = req.header(name, value);
            }

            let response = req
                .send()
                .await
                .map_err(|e| NdnError::RemoteError(format!("request failed: {}", e)))?;
            let status = response.status();
            let headers = response.headers().clone();
            let content_length = response.content_length();
            let stream = response.bytes_stream().map(|result| {
                result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            });

            Ok(CyfsTransportResponse {
                status,
                headers,
                content_length,
                body: Box::pin(StreamReader::new(stream)),
            })
        })
    }
}

impl<C> CyfsHttpTransport for HyperConnectorTransport<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn send(&self, request: CyfsTransportRequest) -> TransportFuture<'_> {
        Box::pin(async move {
            let mut builder = Request::builder()
                .method(request.method.clone())
                .uri(request.url.as_str());
            if let Some(headers) = builder.headers_mut() {
                headers.extend(request.headers.clone());
            }

            let request = builder
                .body(Empty::<Bytes>::new())
                .map_err(|e| NdnError::Internal(format!("build hyper request failed: {}", e)))?;
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| NdnError::RemoteError(format!("request failed: {}", e)))?;

            let status = response.status();
            let headers = response.headers().clone();
            let content_length = headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            let stream = response.into_body().into_data_stream().map(|result| {
                result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            });

            Ok(CyfsTransportResponse {
                status,
                headers,
                content_length,
                body: Box::pin(StreamReader::new(stream)),
            })
        })
    }
}

// =====================================================================
// Client / builder
// =====================================================================

#[derive(Clone)]
pub struct CyfsNdnClient {
    transport: Arc<dyn CyfsHttpTransport>,
    default_store_mgr: Option<NamedDataMgr>,
    default_remote_url: Option<String>,
    session_token: Option<String>,
    obj_id_in_host: bool,
    path_verifier: Arc<dyn PathObjectVerifier>,
}

pub struct CyfsNdnClientBuilder {
    http_builder: reqwest::ClientBuilder,
    transport: Option<Arc<dyn CyfsHttpTransport>>,
    default_store_mgr: Option<NamedDataMgr>,
    default_remote_url: Option<String>,
    session_token: Option<String>,
    obj_id_in_host: bool,
    path_verifier: Option<Arc<dyn PathObjectVerifier>>,
}

impl CyfsNdnClientBuilder {
    pub fn new() -> Self {
        Self {
            http_builder: Client::builder().timeout(std::time::Duration::from_secs(30)),
            transport: None,
            default_store_mgr: None,
            default_remote_url: None,
            session_token: None,
            obj_id_in_host: false,
            path_verifier: None,
        }
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.http_builder = self.http_builder.timeout(timeout);
        self
    }

    pub fn default_remote_url(mut self, url: impl Into<String>) -> Self {
        self.default_remote_url = Some(url.into());
        self
    }

    pub fn session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    pub fn default_store_mgr(mut self, store_mgr: NamedDataMgr) -> Self {
        self.default_store_mgr = Some(store_mgr);
        self
    }

    pub fn transport<T>(mut self, transport: T) -> Self
    where
        T: CyfsHttpTransport + 'static,
    {
        self.transport = Some(Arc::new(transport));
        self
    }

    pub fn connector<C>(self, connector: C) -> Self
    where
        C: Connect + Clone + Send + Sync + 'static,
    {
        self.transport(HyperConnectorTransport::new(connector))
    }

    pub fn obj_id_in_host(mut self, enabled: bool) -> Self {
        self.obj_id_in_host = enabled;
        self
    }

    pub fn path_verifier<V>(mut self, verifier: V) -> Self
    where
        V: PathObjectVerifier + 'static,
    {
        self.path_verifier = Some(Arc::new(verifier));
        self
    }

    pub fn build(self) -> NdnResult<CyfsNdnClient> {
        let transport = match self.transport {
            Some(transport) => transport,
            None => {
                let client = self.http_builder.build().map_err(|e| {
                    NdnError::Internal(format!("build reqwest client failed: {}", e))
                })?;
                Arc::new(ReqwestCyfsTransport::new(client))
            }
        };

        Ok(CyfsNdnClient {
            transport,
            default_store_mgr: self.default_store_mgr,
            default_remote_url: self.default_remote_url,
            session_token: self.session_token,
            obj_id_in_host: self.obj_id_in_host,
            path_verifier: self
                .path_verifier
                .unwrap_or_else(|| Arc::new(AcceptIfFreshVerifier::default())),
        })
    }
}

impl Default for CyfsNdnClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CyfsNdnClient {
    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("default cyfs ndn client builder must succeed")
    }

    pub fn builder() -> CyfsNdnClientBuilder {
        CyfsNdnClientBuilder::new()
    }

    pub fn get(&self, url: impl Into<String>) -> CyfsNdnRequestBuilder {
        self.request(Method::GET, url)
    }

    pub fn request(&self, method: Method, url: impl Into<String>) -> CyfsNdnRequestBuilder {
        CyfsNdnRequestBuilder {
            client: self.clone(),
            method,
            url: url.into(),
            known_obj_id: None,
            range: None,
            progress_callback: None,
            req_headers: CYFSHttpReqHeaders::default(),
            resp_raw: false,
        }
    }

    fn gen_obj_url(&self, obj_id: &ObjId) -> NdnResult<String> {
        let base = self.default_remote_url.as_ref().ok_or_else(|| {
            NdnError::InvalidParam("default_remote_url is required to generate obj url".to_string())
        })?;

        if self.obj_id_in_host {
            let url = Url::parse(base).map_err(|e| {
                NdnError::InvalidParam(format!("parse default remote url failed: {}", e))
            })?;
            let host = url
                .host_str()
                .ok_or_else(|| NdnError::InvalidParam(format!("missing host in {}", base)))?;
            let mut with_host = url.clone();
            with_host
                .set_host(Some(format!("{}.{}", obj_id.to_base32(), host).as_str()))
                .map_err(|_| {
                    NdnError::InvalidParam(format!("replace host obj id failed for {}", base))
                })?;
            Ok(with_host.to_string())
        } else {
            Ok(format!(
                "{}/{}",
                base.trim_end_matches('/'),
                obj_id.to_string()
            ))
        }
    }

    fn resolve_related_url(&self, base: &ResolvedUrl, obj_id: &ObjId) -> NdnResult<String> {
        if let Ok(url) = base.replace_obj_id(obj_id) {
            return Ok(url);
        }
        self.gen_obj_url(obj_id)
    }

    async fn fetch_verified_object_by_id(
        &self,
        base: &ResolvedUrl,
        obj_id: &ObjId,
    ) -> NdnResult<VerifiedObject> {
        let url = self.resolve_related_url(base, obj_id)?;
        self.get(url)
            .obj_id(obj_id.clone())
            .send()
            .await?
            .into_verified_object()
            .await
    }
}

impl Default for CyfsNdnClient {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// Request builder
// =====================================================================

#[derive(Clone)]
pub struct CyfsNdnRequestBuilder {
    client: CyfsNdnClient,
    method: Method,
    url: String,
    known_obj_id: Option<ObjId>,
    range: Option<Range<u64>>,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    req_headers: CYFSHttpReqHeaders,
    resp_raw: bool,
}

impl CyfsNdnRequestBuilder {
    pub fn obj_id(mut self, obj_id: ObjId) -> Self {
        self.known_obj_id = Some(obj_id);
        self
    }

    pub fn range(mut self, range: Range<u64>) -> Self {
        self.range = Some(range);
        self
    }

    pub fn progress_callback(mut self, callback: Arc<Mutex<NdnProgressCallback>>) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    fn progress_callback_opt(mut self, callback: Option<Arc<Mutex<NdnProgressCallback>>>) -> Self {
        self.progress_callback = callback;
        self
    }

    pub fn original_user(mut self, user: impl Into<String>) -> Self {
        self.req_headers.original_user = Some(user.into());
        self
    }

    pub fn cascades(mut self, cascades: Vec<Value>) -> Self {
        self.req_headers.cascades = Some(cascades);
        self
    }

    pub fn proofs(mut self, proofs: Vec<String>) -> Self {
        self.req_headers.proofs = Some(proofs);
        self
    }

    pub fn access_code(mut self, code: impl Into<String>) -> Self {
        self.req_headers.access_code = Some(code.into());
        self
    }

    /// Add `?resp=raw` to the transport URL. Per protocol the server MUST then
    /// return the raw JSON/bytes without attaching any CYFS verification
    /// headers. The client will skip header-driven verification; it must
    /// reconcile the response against a known ObjId (either from the URL or
    /// supplied via [`obj_id`]).
    pub fn raw(mut self) -> Self {
        self.resp_raw = true;
        self
    }

    pub async fn send(self) -> NdnResult<CyfsNdnResponse> {
        if self
            .req_headers
            .cascades
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0)
            > CYFS_CASCADES_MAX_LEN
        {
            return Err(NdnError::InvalidParam(format!(
                "cyfs-cascades max length is {}",
                CYFS_CASCADES_MAX_LEN
            )));
        }

        let resolved_url = ResolvedUrl::parse(&self.url, self.resp_raw)?;
        let mut headers = HeaderMap::new();

        if let Some(range) = self.range.as_ref() {
            if range.end > range.start {
                headers.insert(
                    header::RANGE,
                    format!("bytes={}-{}", range.start, range.end - 1)
                        .parse()
                        .map_err(|e| {
                            NdnError::InvalidParam(format!("invalid range header value: {}", e))
                        })?,
                );
            }
        }

        if let Some(token) = self.client.session_token.as_ref() {
            headers.insert(
                header::AUTHORIZATION,
                format!("Bearer {}", token).parse().map_err(|e| {
                    NdnError::InvalidParam(format!("invalid authorization header: {}", e))
                })?,
            );
        }

        apply_cyfs_req_headers(&self.req_headers, &mut headers)?;

        let response = self
            .client
            .transport
            .send(CyfsTransportRequest {
                method: self.method.clone(),
                url: resolved_url.transport_url.clone(),
                headers,
            })
            .await?;

        let status = response.status;
        if !(status.is_success() || status == StatusCode::PARTIAL_CONTENT) {
            return Err(NdnError::from_http_status(
                status,
                resolved_url.transport_url.clone(),
            ));
        }

        let meta = CyfsResponseMeta::from_response(
            &resolved_url,
            self.known_obj_id.clone(),
            self.resp_raw || resolved_url.parsed.resp_raw,
            &response.headers,
            response.content_length,
            self.client.path_verifier.as_ref(),
        )?;

        Ok(CyfsNdnResponse {
            client: self.client,
            request: RequestContext {
                resolved_url,
                progress_callback: self.progress_callback,
                range: self.range,
            },
            meta,
            response,
        })
    }

    pub async fn pull_to_local_file(
        self,
        local_path: impl Into<PathBuf>,
    ) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(StoreMode::LocalFile(local_path.into(), 0..0, false), None)
            .await
    }

    pub async fn pull_to_local_file_with_named_store(
        self,
        local_path: impl Into<PathBuf>,
        store_mgr: &NamedDataMgr,
    ) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(
                StoreMode::LocalFile(local_path.into(), 0..0, true),
                Some(store_mgr.clone()),
            )
            .await
    }

    pub async fn pull_to_named_store(self, store_mgr: &NamedDataMgr) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(StoreMode::StoreInNamedMgr, Some(store_mgr.clone()))
            .await
    }
}

// =====================================================================
// Response metadata / URL parsing
// =====================================================================

#[derive(Clone)]
struct RequestContext {
    resolved_url: ResolvedUrl,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    range: Option<Range<u64>>,
}

/// URL parsed with full knowledge of CYFS semantics.
#[derive(Debug, Clone)]
struct ResolvedUrl {
    original_url: String,
    transport_url: String,
    parsed_full: Url,
    parsed: CyfsParsedUrl,
    locator: Option<ObjLocator>,
}

#[derive(Debug, Clone)]
enum ObjLocator {
    HostFirstLabel,
    PathSegment(usize),
}

impl ResolvedUrl {
    fn parse(url: &str, force_raw: bool) -> NdnResult<Self> {
        let parsed_full = Url::parse(url)
            .map_err(|e| NdnError::InvalidParam(format!("parse url {} failed: {}", url, e)))?;

        let mut parsed = cyfs_parse_url(url)?;
        let mut transport_url = url.to_string();
        if force_raw && !parsed.resp_raw {
            transport_url = append_query(&parsed_full, "resp", "raw")?;
            parsed.resp_raw = true;
        }

        let locator = if let Some(obj_id) = parsed.url_obj_id.as_ref() {
            if parsed_full
                .host_str()
                .and_then(|h| ObjId::from_hostname(h).ok())
                .as_ref()
                == Some(obj_id)
            {
                Some(ObjLocator::HostFirstLabel)
            } else {
                parsed_full.path_segments().and_then(|segments| {
                    segments
                        .enumerate()
                        .find(|(_, seg)| ObjId::new(seg).ok().as_ref() == Some(obj_id))
                        .map(|(i, _)| ObjLocator::PathSegment(i))
                })
            }
        } else {
            None
        };

        Ok(Self {
            original_url: url.to_string(),
            transport_url,
            parsed_full,
            parsed,
            locator,
        })
    }

    fn url_obj_id(&self) -> Option<&ObjId> {
        self.parsed.url_obj_id.as_ref()
    }

    fn inner_path_steps(&self) -> &[String] {
        &self.parsed.inner_path_steps
    }

    fn host(&self) -> Option<&str> {
        self.parsed.host.as_deref()
    }

    fn resp_raw(&self) -> bool {
        self.parsed.resp_raw
    }

    fn semantic_path(&self) -> Option<String> {
        if self.url_obj_id().is_some() {
            return None;
        }
        let root = self.parsed.root_locator.trim_end_matches('/');
        if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        }
    }

    fn replace_obj_id(&self, obj_id: &ObjId) -> NdnResult<String> {
        let mut url = self.parsed_full.clone();
        match self.locator.as_ref() {
            Some(ObjLocator::HostFirstLabel) => {
                let host = url.host_str().ok_or_else(|| {
                    NdnError::InvalidParam(format!("missing host in {}", self.original_url))
                })?;
                let mut host_parts = host.split('.').map(String::from).collect::<Vec<_>>();
                if host_parts.is_empty() {
                    return Err(NdnError::InvalidParam(format!(
                        "invalid host {} in {}",
                        host, self.original_url
                    )));
                }
                host_parts[0] = obj_id.to_base32();
                let replaced_host = host_parts.join(".");
                url.set_host(Some(replaced_host.as_str())).map_err(|_| {
                    NdnError::InvalidParam(format!("replace host failed for {}", self.original_url))
                })?;
            }
            Some(ObjLocator::PathSegment(index)) => {
                let segments = match url.path_segments() {
                    Some(segments) => segments.collect::<Vec<_>>(),
                    None => Vec::new(),
                };
                let mut new_segments = segments.iter().map(|s| s.to_string()).collect::<Vec<_>>();
                if *index >= new_segments.len() {
                    return Err(NdnError::InvalidParam(format!(
                        "path segment {} out of range for {}",
                        index, self.original_url
                    )));
                }
                new_segments[*index] = obj_id.to_string();
                url.set_path(&format!("/{}", new_segments.join("/")));
            }
            None => {
                return Err(NdnError::InvalidParam(format!(
                    "cannot replace obj id for {}",
                    self.original_url
                )));
            }
        }
        // Strip any inner_path / query so we can fetch the parent object directly.
        url.set_query(None);
        Ok(url.to_string())
    }
}

fn append_query(url: &Url, key: &str, value: &str) -> NdnResult<String> {
    let mut out = url.clone();
    let pairs = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect::<Vec<_>>();
    {
        let mut qp = out.query_pairs_mut();
        qp.clear();
        for (k, v) in pairs.iter() {
            if k == key {
                continue;
            }
            qp.append_pair(k, v);
        }
        qp.append_pair(key, value);
    }
    Ok(out.to_string())
}

pub struct CyfsResponseMeta {
    pub requested_url: String,
    pub transport_url: String,
    pub known_obj_id: Option<ObjId>,
    pub url_obj_id: Option<ObjId>,
    pub url_inner_path_steps: Vec<String>,
    pub resp_raw: bool,
    pub cyfs_headers: CYFSHttpRespHeaders,
    /// Verified path_object if `cyfs-path-obj` was present and a verifier
    /// accepted it. On success the `target` field binds the semantic path
    /// to a root ObjectId.
    pub path_object: Option<VerifiedPathObject>,
    /// Parents materialized from `cyfs-parents-N`. Each entry carries the
    /// parent ObjId (always) plus the canonical JSON + parsed JSON when the
    /// server chose to inline the full object.
    pub parents: Vec<ResolvedParent>,
}

#[derive(Debug, Clone)]
pub struct ResolvedParent {
    pub obj_id: ObjId,
    pub obj_str: Option<String>,
    pub obj_json: Option<Value>,
}

impl CyfsResponseMeta {
    fn from_response(
        resolved_url: &ResolvedUrl,
        known_obj_id: Option<ObjId>,
        resp_raw: bool,
        headers: &HeaderMap,
        content_length: Option<u64>,
        path_verifier: &dyn PathObjectVerifier,
    ) -> NdnResult<Self> {
        let mut cyfs_headers = get_cyfs_resp_headers(headers)?;
        if cyfs_headers.chunk_size.is_none() {
            cyfs_headers.chunk_size = content_length;
        }

        // Verify path-obj JWT when we have a semantic URL.
        let path_object = match (&cyfs_headers.path_obj, resolved_url.semantic_path()) {
            (Some(jwt), Some(_)) if !resp_raw => {
                let verified = path_verifier.verify(jwt, resolved_url.host())?;
                Some(verified)
            }
            (Some(jwt), None) if !resp_raw => {
                // Still verify freshness / signature if present even if URL is an O-Link.
                Some(path_verifier.verify(jwt, resolved_url.host())?)
            }
            _ => None,
        };

        // Materialize parents: for `json:` variants, re-hash canonical JSON and
        // ensure it matches the declared ObjId (cyfs-parents-N `oid:` form if
        // both exist, otherwise self-consistent via RFC 8785 hashing).
        let mut parents = Vec::with_capacity(cyfs_headers.parents.len());
        for parent in cyfs_headers.parents.iter() {
            parents.push(ResolvedParent::from_header(parent)?);
        }

        Ok(Self {
            requested_url: resolved_url.original_url.clone(),
            transport_url: resolved_url.transport_url.clone(),
            known_obj_id,
            url_obj_id: resolved_url.url_obj_id().cloned(),
            url_inner_path_steps: resolved_url.inner_path_steps().to_vec(),
            resp_raw,
            cyfs_headers,
            path_object,
            parents,
        })
    }

    /// The effective ObjectId of the response body, taking all signals into
    /// account. Priority: explicit `obj_id(...)` call > `cyfs-obj-id` header >
    /// last parent's inner_path resolution > URL ObjId.
    pub fn effective_obj_id(&self) -> Option<ObjId> {
        if let Some(obj_id) = self.known_obj_id.clone() {
            return Some(obj_id);
        }
        if let Some(obj_id) = self.cyfs_headers.obj_id.clone() {
            return Some(obj_id);
        }
        if let Some(path) = self.path_object.as_ref() {
            if self.url_inner_path_steps.is_empty() && self.parents.is_empty() {
                return Some(path.target.clone());
            }
        }
        self.url_obj_id.clone()
    }

    /// Verify the inner_path chain: for each `/@/` segment in the URL, ensure
    /// the parent object (from `cyfs-parents-N`) resolves the expected field
    /// and the result matches either the next parent's ObjId or the final
    /// `cyfs-obj-id`. Only applied when steps + parents are both present.
    pub fn verify_inner_path_chain(&self) -> NdnResult<Option<ObjId>> {
        if self.url_inner_path_steps.is_empty() || self.parents.is_empty() {
            return Ok(None);
        }
        if self.parents.len() < self.url_inner_path_steps.len() {
            return Err(NdnError::InvalidData(format!(
                "cyfs-parents-N count {} is less than inner_path steps {}",
                self.parents.len(),
                self.url_inner_path_steps.len()
            )));
        }

        // If we have a verified path-obj, it binds the first parent's ObjId.
        if let Some(path) = self.path_object.as_ref() {
            let first_parent = &self.parents[0];
            if first_parent.obj_id != path.target {
                return Err(NdnError::InvalidData(format!(
                    "cyfs-path-obj.target {} does not match cyfs-parents-0 {}",
                    path.target, first_parent.obj_id
                )));
            }
        }

        let mut last_resolved: Option<ObjId> = None;
        for (step_idx, step) in self.url_inner_path_steps.iter().enumerate() {
            let parent = &self.parents[step_idx];
            let parent_json = parent.obj_json.as_ref().ok_or_else(|| {
                NdnError::InvalidData(format!(
                    "cyfs-parents-{} missing json payload for inner_path verification",
                    step_idx
                ))
            })?;

            let resolved = resolve_inner_path_step(parent_json, step)?;
            let expected_next: ObjId = if step_idx + 1 < self.url_inner_path_steps.len() {
                self.parents[step_idx + 1].obj_id.clone()
            } else {
                match self.cyfs_headers.obj_id.as_ref() {
                    Some(id) => id.clone(),
                    None => ObjId::from_value(&resolved).map_err(|_| {
                        NdnError::InvalidData(format!(
                            "final inner_path step {} did not resolve to an ObjId and cyfs-obj-id is missing",
                            step
                        ))
                    })?,
                }
            };

            let resolved_obj_id = ObjId::from_value(&resolved).map_err(|_| {
                NdnError::InvalidData(format!(
                    "inner_path step {} result is not an ObjId (value: {})",
                    step, resolved
                ))
            })?;
            if resolved_obj_id != expected_next {
                return Err(NdnError::InvalidData(format!(
                    "inner_path step {} resolves to {} but expected {}",
                    step, resolved_obj_id, expected_next
                )));
            }
            last_resolved = Some(resolved_obj_id);
        }
        Ok(last_resolved)
    }

    /// Locate a parent by its ObjId (useful for looking up an embedded
    /// FileObject without re-fetching it).
    pub fn find_parent(&self, obj_id: &ObjId) -> Option<&ResolvedParent> {
        self.parents.iter().find(|p| &p.obj_id == obj_id)
    }
}

impl ResolvedParent {
    fn from_header(parent: &CyfsParent) -> NdnResult<Self> {
        match parent {
            CyfsParent::ObjId(id) => Ok(Self {
                obj_id: id.clone(),
                obj_str: None,
                obj_json: None,
            }),
            CyfsParent::Json(obj_str) => {
                let obj_json: Value = serde_json::from_str(obj_str).map_err(|e| {
                    NdnError::DecodeError(format!("parse cyfs-parents-N json failed: {}", e))
                })?;
                let obj_id = recompute_parent_obj_id(&obj_json)?;
                Ok(Self {
                    obj_id,
                    obj_str: Some(obj_str.clone()),
                    obj_json: Some(obj_json),
                })
            }
        }
    }
}

fn recompute_parent_obj_id(obj_json: &Value) -> NdnResult<ObjId> {
    // FileObject / DirObject / generic NamedObject paths reuse the RFC 8785
    // Hash. ChunkList needs length-prefix encoding. We sniff the object type
    // by looking at unique fields; fallback is to reject.
    if obj_json.is_array() {
        let chunk_list = ChunkList::from_json_value(obj_json.clone())?;
        let (id, _) = chunk_list.gen_obj_id();
        return Ok(id);
    }
    let obj_type = if obj_json.get("content").is_some() && obj_json.get("size").is_some() {
        OBJ_TYPE_FILE
    } else if obj_json.get("total_size").is_some() || obj_json.get("file_count").is_some() {
        OBJ_TYPE_DIR
    } else if obj_json.get("path").is_some() && obj_json.get("target").is_some() {
        OBJ_TYPE_PATH
    } else {
        return Err(NdnError::InvalidData(
            "unable to infer cyfs-parents-N object type from JSON payload".to_string(),
        ));
    };

    let (obj_id, _) = build_named_object_by_json(obj_type, obj_json);
    Ok(obj_id)
}

/// Resolve one `/@/` inner_path segment as a JSON path. Supports multi-field
/// steps delimited by `/`; returns the JSON value at the end of the step.
fn resolve_inner_path_step(obj_json: &Value, step: &str) -> NdnResult<Value> {
    let mut current = obj_json.clone();
    for field in step.split('/').filter(|s| !s.is_empty()) {
        current = if let Ok(idx) = field.parse::<usize>() {
            current
                .get(idx)
                .cloned()
                .ok_or_else(|| NdnError::NotFound(format!("inner_path index {} not found", idx)))?
        } else {
            current.get(field).cloned().ok_or_else(|| {
                NdnError::NotFound(format!("inner_path field '{}' not found", field))
            })?
        };
    }
    Ok(current)
}

// =====================================================================
// Response
// =====================================================================

pub struct CyfsNdnResponse {
    client: CyfsNdnClient,
    request: RequestContext,
    meta: CyfsResponseMeta,
    response: CyfsTransportResponse,
}

struct VerifiedObject {
    obj_id: ObjId,
    obj_json: Value,
    obj_str: String,
}

#[derive(Debug, Clone, Default)]
pub struct CyfsPullResult {
    pub obj_id: Option<ObjId>,
    pub total_size: u64,
    pub chunk_count: usize,
    pub stored_objects: Vec<ObjId>,
}

#[derive(Debug, Clone)]
struct KnownObjectToStore {
    obj_id: ObjId,
    obj_str: String,
}

#[derive(Debug, Clone)]
struct LocalChunkLink {
    chunk_id: ChunkId,
    range: Range<u64>,
}

enum PullDescriptor {
    Chunk {
        chunk_id: ChunkId,
        metadata_to_store: Vec<KnownObjectToStore>,
        file_action: Option<NdnAction>,
    },
    ChunkList {
        chunk_list: ChunkList,
        metadata_to_store: Vec<KnownObjectToStore>,
        result_obj_id: Option<ObjId>,
        file_action: Option<NdnAction>,
        file_size: u64,
    },
}

impl CyfsNdnResponse {
    pub fn status(&self) -> StatusCode {
        self.response.status
    }

    pub fn meta(&self) -> &CyfsResponseMeta {
        &self.meta
    }

    pub async fn object(self) -> NdnResult<(ObjId, Value)> {
        let verified = self.into_verified_object().await?;
        Ok((verified.obj_id, verified.obj_json))
    }

    pub async fn object_string(self) -> NdnResult<(ObjId, String)> {
        let verified = self.into_verified_object().await?;
        Ok((verified.obj_id, verified.obj_str))
    }

    pub async fn text(self) -> NdnResult<String> {
        let mut reader = self.reader();
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw).await?;
        String::from_utf8(raw)
            .map_err(|e| NdnError::DecodeError(format!("response body is not utf-8: {}", e)))
    }

    pub async fn bytes(self) -> NdnResult<Vec<u8>> {
        let mut reader = self.reader();
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw).await?;
        Ok(raw)
    }

    pub fn reader(self) -> ChunkReader {
        self.response.body
    }

    async fn into_verified_object(self) -> NdnResult<VerifiedObject> {
        // If the URL had an inner_path chain, verify it up-front. We don't
        // require it for raw responses.
        if !self.meta.resp_raw {
            self.meta.verify_inner_path_chain()?;
        }

        let effective_obj_id = self.meta.effective_obj_id();
        let transport_url = self.meta.transport_url.clone();
        let mut reader = self.reader();
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw).await?;
        let obj_str = String::from_utf8(raw)
            .map_err(|e| NdnError::DecodeError(format!("response body is not utf-8: {}", e)))?;

        let effective_obj_id = effective_obj_id.ok_or_else(|| {
            NdnError::InvalidId(format!(
                "no obj id found in request url / headers for {}",
                transport_url
            ))
        })?;

        let obj_json = if effective_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST {
            let chunk_list = ChunkList::from_json(obj_str.as_str())?;
            let (real_obj_id, _) = clone_chunk_list(&chunk_list)?.gen_obj_id();
            if real_obj_id != effective_obj_id {
                return Err(NdnError::InvalidId(format!(
                    "verify chunklist object failed, expect:{} actual:{}",
                    effective_obj_id.to_string(),
                    real_obj_id.to_string()
                )));
            }
            serde_json::from_str::<Value>(obj_str.as_str())
                .map_err(|e| NdnError::InvalidData(format!("parse chunklist json failed: {}", e)))?
        } else {
            verify_named_object_from_str(&effective_obj_id, obj_str.as_str())?
        };
        Ok(VerifiedObject {
            obj_id: effective_obj_id,
            obj_json,
            obj_str,
        })
    }

    async fn into_verified_chunk_bytes(self, chunk_id: ChunkId) -> NdnResult<Vec<u8>> {
        let mut reader = self.reader();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        verify_chunk_bytes(&chunk_id, bytes.as_slice())?;
        Ok(bytes)
    }

    pub async fn pull_to_local_file(
        self,
        local_path: impl Into<PathBuf>,
    ) -> NdnResult<CyfsPullResult> {
        self.pull(StoreMode::LocalFile(local_path.into(), 0..0, false), None)
            .await
    }

    pub async fn pull_to_local_file_with_named_store(
        self,
        local_path: impl Into<PathBuf>,
        store_mgr: &NamedDataMgr,
    ) -> NdnResult<CyfsPullResult> {
        self.pull(
            StoreMode::LocalFile(local_path.into(), 0..0, true),
            Some(store_mgr.clone()),
        )
        .await
    }

    pub async fn pull_to_named_store(self, store_mgr: &NamedDataMgr) -> NdnResult<CyfsPullResult> {
        self.pull(StoreMode::StoreInNamedMgr, Some(store_mgr.clone()))
            .await
    }

    pub async fn pull(
        self,
        pull_mode: StoreMode,
        store_mgr: Option<NamedDataMgr>,
    ) -> NdnResult<CyfsPullResult> {
        let target_store_mgr =
            resolve_target_store_mgr(&pull_mode, store_mgr, self.client.default_store_mgr.clone())?;

        if let Some(descriptor) = self.describe_raw_pull().await? {
            return match descriptor {
                PullDescriptor::Chunk {
                    chunk_id,
                    metadata_to_store,
                    file_action,
                } => {
                    self.pull_raw_chunk(
                        chunk_id,
                        pull_mode,
                        target_store_mgr,
                        metadata_to_store,
                        file_action,
                    )
                    .await
                }
                PullDescriptor::ChunkList {
                    chunk_list,
                    metadata_to_store,
                    result_obj_id,
                    file_action,
                    file_size,
                } => {
                    let mut result = self
                        .pull_raw_chunk_list(
                            chunk_list,
                            pull_mode,
                            target_store_mgr,
                            metadata_to_store,
                            file_action,
                            file_size,
                        )
                        .await?;
                    if result.obj_id.is_none() {
                        result.obj_id = result_obj_id;
                    }
                    Ok(result)
                }
            };
        }

        let effective_obj_id = self.meta.effective_obj_id().ok_or_else(|| {
            NdnError::InvalidId(format!(
                "cannot infer obj id for {}",
                self.meta.transport_url
            ))
        })?;

        if effective_obj_id.obj_type == OBJ_TYPE_FILE {
            let request = self.request.clone();
            let client = self.client.clone();
            let verified_obj = self.into_verified_object().await?;
            let file_obj: FileObject = serde_json::from_value(verified_obj.obj_json)
                .map_err(|e| NdnError::InvalidData(format!("parse file object failed: {}", e)))?;
            return client
                .pull_file_object(
                    &request.resolved_url,
                    KnownObjectToStore {
                        obj_id: verified_obj.obj_id,
                        obj_str: verified_obj.obj_str,
                    },
                    file_obj,
                    pull_mode,
                    target_store_mgr,
                    request.progress_callback,
                )
                .await;
        }

        if effective_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST {
            let request = self.request.clone();
            let client = self.client.clone();
            let verified_obj = self.into_verified_object().await?;
            let chunk_list = ChunkList::from_json_value(verified_obj.obj_json)?;
            return client
                .pull_chunk_list_by_object(
                    &request.resolved_url,
                    pull_mode,
                    target_store_mgr,
                    request.progress_callback,
                    KnownObjectToStore {
                        obj_id: verified_obj.obj_id.clone(),
                        obj_str: verified_obj.obj_str.clone(),
                    },
                    chunk_list,
                    None,
                    0,
                )
                .await;
        }

        Err(NdnError::Unsupported(format!(
            "pull is unsupported for object type {}",
            effective_obj_id.obj_type
        )))
    }

    /// Decide up-front whether the body is a raw Chunk/ChunkList stream based
    /// on CYFS response headers + URL signals. Returns `None` when the body is
    /// a NamedObject JSON that needs deserialization before we know what to do.
    async fn describe_raw_pull(&self) -> NdnResult<Option<PullDescriptor>> {
        // Verified inner_path chain gives us the canonical ObjectId of the
        // final value. Protocol allows servers to inline the FileObject /
        // ChunkList via `cyfs-parents-N`, in which case we can start pulling
        // raw chunks immediately without round-tripping for the metadata.
        if !self.meta.resp_raw {
            if let Some(desc) = self.describe_from_inner_path_chain()? {
                return Ok(Some(desc));
            }
        }

        let effective_obj_id = self.meta.effective_obj_id().ok_or_else(|| {
            NdnError::InvalidId(format!(
                "cannot infer obj id for {}",
                self.meta.transport_url
            ))
        })?;

        if effective_obj_id.is_chunk() {
            return Ok(Some(PullDescriptor::Chunk {
                chunk_id: ChunkId::from_obj_id(&effective_obj_id),
                metadata_to_store: Vec::new(),
                file_action: None,
            }));
        }

        Ok(None)
    }

    fn describe_from_inner_path_chain(&self) -> NdnResult<Option<PullDescriptor>> {
        if self.meta.parents.is_empty() {
            return Ok(None);
        }

        // A common shape: `.../readme/@/content` where the last parent is a
        // FileObject and cyfs-obj-id is its content chunk(list). Use that to
        // short-circuit into the raw-chunk fast path.
        let last_parent = self.meta.parents.last().unwrap();
        let last_json = match last_parent.obj_json.as_ref() {
            Some(j) => j,
            None => return Ok(None),
        };

        if last_parent.obj_id.is_file_object() {
            let file_obj: FileObject = serde_json::from_value(last_json.clone()).map_err(|e| {
                NdnError::InvalidData(format!(
                    "parse FileObject from cyfs-parents-N failed: {}",
                    e
                ))
            })?;
            let content_obj_id = ObjId::new(file_obj.content.as_str())?;
            let file_store = KnownObjectToStore {
                obj_id: last_parent.obj_id.clone(),
                obj_str: last_parent
                    .obj_str
                    .clone()
                    .unwrap_or_else(|| last_json.to_string()),
            };
            if content_obj_id.is_chunk() {
                return Ok(Some(PullDescriptor::Chunk {
                    chunk_id: ChunkId::from_obj_id(&content_obj_id),
                    metadata_to_store: vec![file_store],
                    file_action: Some(NdnAction::FileOK(last_parent.obj_id.clone(), file_obj.size)),
                }));
            }
            // Chunk-list inline body isn't a case the protocol explicitly
            // requires; defer to object-mode pull.
        }
        Ok(None)
    }

    async fn pull_raw_chunk(
        self,
        chunk_id: ChunkId,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedDataMgr>,
        metadata_to_store: Vec<KnownObjectToStore>,
        file_action: Option<NdnAction>,
    ) -> NdnResult<CyfsPullResult> {
        let effective_obj_id = self.meta.effective_obj_id();
        let header_chunk_size = self.meta.cyfs_headers.chunk_size;
        let is_range_request = self.request.range.is_some();
        let progress_callback = self.request.progress_callback.clone();
        let original_url = self.request.resolved_url.original_url.clone();
        let mut reader = self.reader();

        // For a Range request, the server returns a sub-slice, so full
        // chunk-id verification is not feasible in a single shot; we trust
        // cyfs-chunk-size + the outer caller to re-verify across all ranges.
        let chunk_size = chunk_id
            .get_length()
            .unwrap_or_else(|| header_chunk_size.unwrap_or_default());

        let mut stored_objects = Vec::new();
        if let Some(store_mgr) = target_store_mgr.as_ref() {
            for item in metadata_to_store.iter() {
                store_mgr.put_object(&item.obj_id, &item.obj_str).await?;
                stored_objects.push(item.obj_id.clone());
            }
        }

        match &pull_mode {
            StoreMode::StoreInNamedMgr => {
                let store_mgr = target_store_mgr
                    .as_ref()
                    .ok_or_else(|| NdnError::NotFound("named store mgr is required".to_string()))?;
                if is_range_request {
                    return Err(NdnError::Unsupported(
                        "range pull into named store is not supported".to_string(),
                    ));
                }
                store_mgr
                    .put_chunk_by_reader(&chunk_id, chunk_size, reader)
                    .await?;

                call_progress_callback(
                    &progress_callback,
                    format!("chunk:{}", chunk_id.to_string()),
                    NdnAction::ChunkOK(chunk_id.clone(), chunk_size),
                )
                .await?;

                if let Some(file_action) = file_action {
                    call_progress_callback(&progress_callback, original_url.clone(), file_action)
                        .await?;
                }

                return Ok(CyfsPullResult {
                    obj_id: effective_obj_id,
                    total_size: chunk_size,
                    chunk_count: 1,
                    stored_objects,
                });
            }
            StoreMode::LocalFile(path, range, _) => {
                ensure_local_file_exists(path).await?;
                let mut writer = pull_mode.open_local_writer().await?;
                let total_size = if is_range_request {
                    // Range response: copy raw bytes only; skip chunk-id match.
                    let written = tokio::io::copy(&mut reader, &mut writer).await?;
                    written
                } else {
                    let hasher = Some(ChunkHasher::new_with_hash_method(
                        chunk_id.chunk_type.to_hash_method()?,
                    )?);
                    copy_chunk(chunk_id.clone(), &mut reader, &mut writer, hasher, None).await?
                };
                writer.flush().await?;

                if !is_range_request && pull_mode.need_store_to_named_mgr() {
                    let store_mgr = target_store_mgr.as_ref().ok_or_else(|| {
                        NdnError::NotFound("named store mgr is required".to_string())
                    })?;
                    let qcid = caculate_qcid_from_file(path).await?;
                    let last_modify_time = file_last_modify_time(path).await?;
                    store_mgr
                        .add_chunk_by_link_to_local_file(
                            &chunk_id,
                            total_size,
                            &ChunkLocalInfo {
                                path: path.to_string_lossy().to_string(),
                                qcid: qcid.to_string(),
                                last_modify_time,
                                range: Some(range.start..range.start + total_size),
                            },
                        )
                        .await?;
                }

                call_progress_callback(
                    &progress_callback,
                    format!("chunk:{}", chunk_id.to_string()),
                    NdnAction::ChunkOK(chunk_id.clone(), total_size),
                )
                .await?;

                if let Some(file_action) = file_action {
                    call_progress_callback(&progress_callback, original_url.clone(), file_action)
                        .await?;
                }

                return Ok(CyfsPullResult {
                    obj_id: effective_obj_id,
                    total_size,
                    chunk_count: 1,
                    stored_objects,
                });
            }
            StoreMode::NoStore => {
                let mut sink = tokio::io::sink();
                let total_size = if is_range_request {
                    tokio::io::copy(&mut reader, &mut sink).await?
                } else {
                    let hasher = Some(ChunkHasher::new_with_hash_method(
                        chunk_id.chunk_type.to_hash_method()?,
                    )?);
                    copy_chunk(chunk_id.clone(), &mut reader, &mut sink, hasher, None).await?
                };
                call_progress_callback(
                    &progress_callback,
                    format!("chunk:{}", chunk_id.to_string()),
                    NdnAction::ChunkOK(chunk_id, total_size),
                )
                .await?;
                Ok(CyfsPullResult {
                    obj_id: effective_obj_id,
                    total_size,
                    chunk_count: 1,
                    stored_objects,
                })
            }
        }
    }

    async fn pull_raw_chunk_list(
        self,
        chunk_list: ChunkList,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedDataMgr>,
        metadata_to_store: Vec<KnownObjectToStore>,
        file_action: Option<NdnAction>,
        file_size: u64,
    ) -> NdnResult<CyfsPullResult> {
        let effective_obj_id = self.meta.effective_obj_id();
        let progress_callback = self.request.progress_callback.clone();
        let original_url = self.request.resolved_url.original_url.clone();
        let mut reader = self.reader();
        let mut local_writer = open_local_writer_if_needed(&pull_mode).await?;
        let mut pending_links = Vec::new();
        let mut total_size = 0u64;
        let mut stored_objects = Vec::new();

        if let Some(store_mgr) = target_store_mgr.as_ref() {
            for item in metadata_to_store.iter() {
                store_mgr.put_object(&item.obj_id, &item.obj_str).await?;
                stored_objects.push(item.obj_id.clone());
            }
        }

        for chunk_id in chunk_list.body.iter() {
            let chunk_size = chunk_id.get_length().ok_or_else(|| {
                NdnError::InvalidData(format!(
                    "chunk {} does not include length",
                    chunk_id.to_string()
                ))
            })?;
            let mut chunk_bytes = vec![0u8; chunk_size as usize];
            reader.read_exact(chunk_bytes.as_mut_slice()).await?;
            verify_chunk_bytes(chunk_id, chunk_bytes.as_slice())?;

            if let Some(writer) = local_writer.as_mut() {
                writer.write_all(chunk_bytes.as_slice()).await?;
            }

            if matches!(pull_mode, StoreMode::StoreInNamedMgr) {
                let store_mgr = target_store_mgr
                    .as_ref()
                    .ok_or_else(|| NdnError::NotFound("named store mgr is required".to_string()))?;
                store_mgr
                    .put_chunk(chunk_id, chunk_bytes.as_slice())
                    .await?;
            } else if pull_mode.need_store_to_named_mgr() {
                pending_links.push(LocalChunkLink {
                    chunk_id: chunk_id.clone(),
                    range: total_size..total_size + chunk_size,
                });
            }

            total_size += chunk_size;
            call_progress_callback(
                &progress_callback,
                format!("chunk:{}", chunk_id.to_string()),
                NdnAction::ChunkOK(chunk_id.clone(), chunk_size),
            )
            .await?;
        }

        if let Some(writer) = local_writer.as_mut() {
            writer.flush().await?;
        }

        finalize_local_links(
            &pull_mode,
            target_store_mgr.as_ref(),
            &pending_links,
            local_file_path(&pull_mode),
        )
        .await?;

        if let Some(file_action) = file_action {
            call_progress_callback(&progress_callback, original_url, file_action).await?;
        }

        Ok(CyfsPullResult {
            obj_id: effective_obj_id,
            total_size: file_size.max(total_size),
            chunk_count: chunk_list.body.len(),
            stored_objects,
        })
    }
}

// =====================================================================
// Client-side pull helpers for FileObject / ChunkList metadata flows
// =====================================================================

impl CyfsNdnClient {
    async fn pull_file_object(
        &self,
        base: &ResolvedUrl,
        file_obj_store: KnownObjectToStore,
        file_obj: FileObject,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedDataMgr>,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    ) -> NdnResult<CyfsPullResult> {
        let content_obj_id = ObjId::new(file_obj.content.as_str())?;

        if content_obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(&content_obj_id);
            let url = self.resolve_related_url(base, &chunk_id.to_obj_id())?;
            return self
                .get(url)
                .obj_id(chunk_id.to_obj_id())
                .progress_callback_opt(progress_callback)
                .send()
                .await?
                .pull_raw_chunk(
                    chunk_id,
                    pull_mode,
                    target_store_mgr,
                    vec![file_obj_store.clone()],
                    Some(NdnAction::FileOK(
                        file_obj_store.obj_id.clone(),
                        file_obj.size,
                    )),
                )
                .await;
        }

        if content_obj_id.obj_type != OBJ_TYPE_CHUNK_LIST {
            return Err(NdnError::Unsupported(format!(
                "unsupported file content obj type: {}",
                content_obj_id.obj_type
            )));
        }

        let chunk_list_obj = self
            .fetch_verified_object_by_id(base, &content_obj_id)
            .await?;
        let chunk_list = ChunkList::from_json_value(chunk_list_obj.obj_json)?;
        let chunklist_store = KnownObjectToStore {
            obj_id: chunk_list_obj.obj_id.clone(),
            obj_str: chunk_list_obj.obj_str.clone(),
        };

        let mut result = self
            .pull_chunk_list_by_object(
                base,
                pull_mode,
                target_store_mgr.clone(),
                progress_callback.clone(),
                chunklist_store.clone(),
                chunk_list,
                Some(file_obj_store.clone()),
                file_obj.size,
            )
            .await?;

        if let Some(store_mgr) = target_store_mgr.as_ref() {
            store_mgr
                .put_object(&file_obj_store.obj_id, &file_obj_store.obj_str)
                .await?;
            if !result.stored_objects.contains(&file_obj_store.obj_id) {
                result.stored_objects.push(file_obj_store.obj_id.clone());
            }
        }

        call_progress_callback(
            &progress_callback,
            base.original_url.clone(),
            NdnAction::FileOK(file_obj_store.obj_id.clone(), file_obj.size),
        )
        .await?;

        result.obj_id = Some(file_obj_store.obj_id);
        result.total_size = file_obj.size;
        Ok(result)
    }

    async fn pull_chunk_list_by_object(
        &self,
        base: &ResolvedUrl,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedDataMgr>,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
        chunklist_store: KnownObjectToStore,
        chunk_list: ChunkList,
        file_obj: Option<KnownObjectToStore>,
        file_size: u64,
    ) -> NdnResult<CyfsPullResult> {
        let mut local_writer = open_local_writer_if_needed(&pull_mode).await?;
        let mut pending_links = Vec::new();
        let mut total_size = 0u64;

        if let Some(store_mgr) = target_store_mgr.as_ref() {
            store_mgr
                .put_object(&chunklist_store.obj_id, &chunklist_store.obj_str)
                .await?;
        }

        for chunk_id in chunk_list.body.iter() {
            let chunk_size = chunk_id.get_length().ok_or_else(|| {
                NdnError::InvalidData(format!(
                    "chunk {} does not include length",
                    chunk_id.to_string()
                ))
            })?;
            let chunk_url = self.resolve_related_url(base, &chunk_id.to_obj_id())?;
            let chunk_resp = self
                .get(chunk_url)
                .obj_id(chunk_id.to_obj_id())
                .progress_callback_opt(progress_callback.clone())
                .send()
                .await?;
            let chunk_bytes = chunk_resp
                .into_verified_chunk_bytes(chunk_id.clone())
                .await?;

            if let Some(writer) = local_writer.as_mut() {
                writer.write_all(chunk_bytes.as_slice()).await?;
            }

            if matches!(pull_mode, StoreMode::StoreInNamedMgr) {
                let store_mgr = target_store_mgr
                    .as_ref()
                    .ok_or_else(|| NdnError::NotFound("named store mgr is required".to_string()))?;
                store_mgr
                    .put_chunk(chunk_id, chunk_bytes.as_slice())
                    .await?;
            } else if pull_mode.need_store_to_named_mgr() {
                pending_links.push(LocalChunkLink {
                    chunk_id: chunk_id.clone(),
                    range: total_size..total_size + chunk_size,
                });
            }

            total_size += chunk_size;
            call_progress_callback(
                &progress_callback,
                format!("chunk:{}", chunk_id.to_string()),
                NdnAction::ChunkOK(chunk_id.clone(), chunk_size),
            )
            .await?;
        }

        if let Some(writer) = local_writer.as_mut() {
            writer.flush().await?;
        }

        finalize_local_links(
            &pull_mode,
            target_store_mgr.as_ref(),
            &pending_links,
            local_file_path(&pull_mode),
        )
        .await?;

        let mut result = CyfsPullResult {
            obj_id: Some(
                file_obj
                    .as_ref()
                    .map(|v| v.obj_id.clone())
                    .unwrap_or_else(|| chunklist_store.obj_id.clone()),
            ),
            total_size,
            chunk_count: chunk_list.body.len(),
            stored_objects: Vec::new(),
        };

        if target_store_mgr.is_some() {
            result.stored_objects.push(chunklist_store.obj_id);
            if let Some(file_obj) = file_obj {
                result.stored_objects.push(file_obj.obj_id);
            }
        }

        if file_size > 0 {
            result.total_size = file_size;
        }

        Ok(result)
    }
}

// =====================================================================
// Utilities
// =====================================================================

fn resolve_target_store_mgr(
    pull_mode: &StoreMode,
    explicit_store_mgr: Option<NamedDataMgr>,
    default_store_mgr: Option<NamedDataMgr>,
) -> NdnResult<Option<NamedDataMgr>> {
    if matches!(pull_mode, StoreMode::StoreInNamedMgr) || pull_mode.need_store_to_named_mgr() {
        return explicit_store_mgr
            .or(default_store_mgr)
            .map(Some)
            .ok_or_else(|| {
                NdnError::NotFound(
                    "named store mgr is required for current pull target".to_string(),
                )
            });
    }
    Ok(explicit_store_mgr.or(default_store_mgr))
}

fn local_file_path(pull_mode: &StoreMode) -> Option<&PathBuf> {
    match pull_mode {
        StoreMode::LocalFile(path, _, _) => Some(path),
        _ => None,
    }
}

async fn ensure_local_file_exists(path: &Path) -> NdnResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if !path.exists() {
        tokio::fs::File::create(path).await?;
    }
    Ok(())
}

async fn open_local_writer_if_needed(
    pull_mode: &StoreMode,
) -> NdnResult<Option<ndn_lib::ChunkWriter>> {
    if let StoreMode::LocalFile(path, _, _) = pull_mode {
        ensure_local_file_exists(path).await?;
        return Ok(Some(pull_mode.open_local_writer().await?));
    }
    Ok(None)
}

async fn file_last_modify_time(path: &Path) -> NdnResult<u64> {
    let meta = tokio::fs::metadata(path).await?;
    let modified = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    Ok(modified)
}

async fn finalize_local_links(
    pull_mode: &StoreMode,
    store_mgr: Option<&NamedDataMgr>,
    links: &[LocalChunkLink],
    local_path: Option<&PathBuf>,
) -> NdnResult<()> {
    if !pull_mode.need_store_to_named_mgr() || links.is_empty() {
        return Ok(());
    }

    let store_mgr = store_mgr.ok_or_else(|| {
        NdnError::NotFound("named store mgr is required for local-link pull".to_string())
    })?;
    let local_path = local_path.ok_or_else(|| {
        NdnError::InvalidParam("local file path is required for local-link pull".to_string())
    })?;

    let qcid = caculate_qcid_from_file(local_path).await?;
    let last_modify_time = file_last_modify_time(local_path).await?;
    let start_offset = match pull_mode {
        StoreMode::LocalFile(_, range, _) => range.start,
        _ => 0,
    };

    for link in links.iter() {
        store_mgr
            .add_chunk_by_link_to_local_file(
                &link.chunk_id,
                link.range.end - link.range.start,
                &ChunkLocalInfo {
                    path: local_path.to_string_lossy().to_string(),
                    qcid: qcid.to_string(),
                    last_modify_time,
                    range: Some(start_offset + link.range.start..start_offset + link.range.end),
                },
            )
            .await?;
    }

    Ok(())
}

async fn call_progress_callback(
    progress_callback: &Option<Arc<Mutex<NdnProgressCallback>>>,
    inner_path: String,
    action: NdnAction,
) -> NdnResult<()> {
    if let Some(callback) = progress_callback {
        let mut callback = callback.lock().await;
        let result = callback(inner_path, action).await?;
        if !matches!(
            result,
            ProgressCallbackResult::Continue | ProgressCallbackResult::Skip
        ) {
            return Err(NdnError::InvalidState("break by user".to_string()));
        }
    }
    Ok(())
}

fn clone_chunk_list(chunk_list: &ChunkList) -> NdnResult<ChunkList> {
    ChunkList::from_chunk_list(chunk_list.body.clone())
}

fn verify_chunk_bytes(chunk_id: &ChunkId, chunk_bytes: &[u8]) -> NdnResult<()> {
    let hasher = ChunkHasher::new_with_hash_method(chunk_id.chunk_type.to_hash_method()?)?;
    let calc_chunk_id = if chunk_id.chunk_type.is_mix() {
        hasher.calc_mix_chunk_id_from_bytes(chunk_bytes)?
    } else {
        hasher.calc_chunk_id_from_bytes(chunk_bytes)
    };
    if calc_chunk_id != *chunk_id {
        return Err(NdnError::VerifyError(format!(
            "chunk verify failed, expect:{} actual:{}",
            chunk_id.to_string(),
            calc_chunk_id.to_string()
        )));
    }
    Ok(())
}

// Kept for source compatibility; the transport uses HeaderMap natively.
#[allow(dead_code)]
fn header_name(name: &str) -> NdnResult<HeaderName> {
    HeaderName::try_from(name)
        .map_err(|e| NdnError::Internal(format!("invalid header name {}: {}", name, e)))
}

#[allow(dead_code)]
fn header_value(value: &str) -> NdnResult<HeaderValue> {
    HeaderValue::from_str(value)
        .map_err(|e| NdnError::Internal(format!("invalid header value: {}", e)))
}
