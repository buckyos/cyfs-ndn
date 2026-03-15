use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use named_store::{ChunkLocalInfo, NamedStoreMgr};
use reqwest::header::{self, HeaderMap};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::io::StreamReader;

use ndn_lib::{
    caculate_qcid_from_file, copy_chunk, cyfs_get_obj_id_from_url, get_cyfs_resp_headers,
    verify_named_object_from_str, CYFSHttpRespHeaders, ChunkHasher, ChunkId, ChunkReader,
    DirObject, FileObject, NamedObject, NdnAction, NdnError, NdnProgressCallback, NdnResult,
    ObjId, ProgressCallbackResult, SimpleChunkList, StoreMode, OBJ_TYPE_CHUNK_LIST_SIMPLE,
    OBJ_TYPE_FILE,
};

#[derive(Clone)]
pub struct CyfsNdnClient {
    transport: Arc<dyn CyfsHttpTransport>,
    default_store_mgr: Option<NamedStoreMgr>,
    default_remote_url: Option<String>,
    session_token: Option<String>,
    obj_id_in_host: bool,
}

pub struct CyfsNdnClientBuilder {
    http_builder: reqwest::ClientBuilder,
    transport: Option<Arc<dyn CyfsHttpTransport>>,
    default_store_mgr: Option<NamedStoreMgr>,
    default_remote_url: Option<String>,
    session_token: Option<String>,
    obj_id_in_host: bool,
}

#[derive(Clone)]
pub struct CyfsNdnRequestBuilder {
    client: CyfsNdnClient,
    method: Method,
    url: String,
    known_obj_id: Option<ObjId>,
    range: Option<Range<u64>>,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
}

pub struct CyfsNdnResponse {
    client: CyfsNdnClient,
    request: RequestContext,
    meta: CyfsResponseMeta,
    response: CyfsTransportResponse,
}

pub struct CyfsResponseMeta {
    pub requested_url: String,
    pub transport_url: String,
    pub known_obj_id: Option<ObjId>,
    pub url_obj_id: Option<ObjId>,
    pub url_inner_path: Option<String>,
    pub cyfs_headers: CYFSHttpRespHeaders,
    pub cyfs_head: Option<CyfsHead>,
}

pub enum CyfsHead {
    ObjId(ObjId),
    RespHeaders(CYFSHttpRespHeaders),
    FileObject {
        obj_id: ObjId,
        obj: FileObject,
        obj_str: String,
    },
    DirObject {
        obj_id: ObjId,
        obj: DirObject,
        obj_str: String,
    },
    ChunkList {
        obj_id: ObjId,
        chunk_list: SimpleChunkList,
        obj_str: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct CyfsPullResult {
    pub obj_id: Option<ObjId>,
    pub total_size: u64,
    pub chunk_count: usize,
    pub stored_objects: Vec<ObjId>,
}

#[derive(Clone)]
struct RequestContext {
    resolved_url: ResolvedUrl,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
}

#[derive(Debug, Clone)]
struct ResolvedUrl {
    original_url: String,
    transport_url: String,
    parsed_original: Url,
    locator: Option<ObjLocator>,
    url_obj_id: Option<ObjId>,
    inner_path: Option<String>,
}

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

#[derive(Clone)]
pub struct HyperConnectorTransport<C> {
    client: HyperClient<C, Empty<Bytes>>,
}

#[derive(Debug, Clone)]
enum ObjLocator {
    HostFirstLabel,
    PathSegment(usize),
}

struct VerifiedObject {
    obj_id: ObjId,
    obj_json: Value,
    obj_str: String,
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
        chunk_list: SimpleChunkList,
        metadata_to_store: Vec<KnownObjectToStore>,
        result_obj_id: Option<ObjId>,
        file_action: Option<NdnAction>,
        file_size: u64,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CyfsHeadEnvelope {
    obj_id: Option<String>,
    obj_size: Option<u64>,
    root_obj_id: Option<String>,
    path_obj: Option<String>,
    mtree_path: Option<String>,
    embed_objs: HashMap<String, String>,
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

    pub fn default_store_mgr(mut self, store_mgr: NamedStoreMgr) -> Self {
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
        })
    }
}

impl Default for CyfsNdnClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestCyfsTransport {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
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
            let stream = response
                .into_body()
                .into_data_stream()
                .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));

            Ok(CyfsTransportResponse {
                status,
                headers,
                content_length,
                body: Box::pin(StreamReader::new(stream)),
            })
        })
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
            Ok(format!("{}/{}", base.trim_end_matches('/'), obj_id.to_string()))
        }
    }

    fn resolve_related_url(&self, base: &ResolvedUrl, obj_id: &ObjId) -> NdnResult<String> {
        if let Ok(url) = base.replace_obj_id(obj_id) {
            return Ok(url);
        }

        self.gen_obj_url(obj_id)
    }

    async fn get_verified_object_by_id(
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

    async fn pull_file_object(
        &self,
        base: &ResolvedUrl,
        file_obj_store: KnownObjectToStore,
        file_obj: FileObject,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedStoreMgr>,
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
                    Some(NdnAction::FileOK(file_obj_store.obj_id.clone(), file_obj.size)),
                )
                .await;
        }

        if content_obj_id.obj_type != OBJ_TYPE_CHUNK_LIST_SIMPLE {
            return Err(NdnError::Unsupported(format!(
                "unsupported file content obj type: {}",
                content_obj_id.obj_type
            )));
        }

        let chunk_list_obj = self.get_verified_object_by_id(base, &content_obj_id).await?;
        let chunk_list = SimpleChunkList::from_json_value(chunk_list_obj.obj_json)?;
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
        target_store_mgr: Option<NamedStoreMgr>,
        progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
        chunklist_store: KnownObjectToStore,
        chunk_list: SimpleChunkList,
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
            let chunk_bytes = chunk_resp.into_verified_chunk_bytes(chunk_id.clone()).await?;

            if let Some(writer) = local_writer.as_mut() {
                writer.write_all(chunk_bytes.as_slice()).await?;
            }

            if matches!(pull_mode, StoreMode::StoreInNamedMgr) {
                let store_mgr = target_store_mgr.as_ref().ok_or_else(|| {
                    NdnError::NotFound("named store mgr is required".to_string())
                })?;
                store_mgr.put_chunk(chunk_id, chunk_bytes.as_slice(), true).await?;
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

impl Default for CyfsNdnClient {
    fn default() -> Self {
        Self::new()
    }
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

    fn progress_callback_opt(
        mut self,
        callback: Option<Arc<Mutex<NdnProgressCallback>>>,
    ) -> Self {
        self.progress_callback = callback;
        self
    }

    pub async fn send(self) -> NdnResult<CyfsNdnResponse> {
        let resolved_url = ResolvedUrl::parse(&self.url)?;
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
            resolved_url.original_url.clone(),
            resolved_url.transport_url.clone(),
            self.known_obj_id.clone(),
            resolved_url.url_obj_id.clone(),
            resolved_url.inner_path.clone(),
            &response.headers,
            response.content_length,
        )?;

        Ok(CyfsNdnResponse {
            client: self.client,
            request: RequestContext {
                resolved_url,
                progress_callback: self.progress_callback,
            },
            meta,
            response,
        })
    }

    pub async fn pull_to_local_file(self, local_path: impl Into<PathBuf>) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(StoreMode::LocalFile(local_path.into(), 0..0, false), None)
            .await
    }

    pub async fn pull_to_local_file_with_named_store(
        self,
        local_path: impl Into<PathBuf>,
        store_mgr: &NamedStoreMgr,
    ) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(
                StoreMode::LocalFile(local_path.into(), 0..0, true),
                Some(store_mgr.clone()),
            )
            .await
    }

    pub async fn pull_to_named_store(self, store_mgr: &NamedStoreMgr) -> NdnResult<CyfsPullResult> {
        self.send()
            .await?
            .pull(StoreMode::StoreInNamedMgr, Some(store_mgr.clone()))
            .await
    }
}

impl CyfsResponseMeta {
    fn from_response(
        requested_url: String,
        transport_url: String,
        known_obj_id: Option<ObjId>,
        url_obj_id: Option<ObjId>,
        url_inner_path: Option<String>,
        headers: &HeaderMap,
        content_length: Option<u64>,
    ) -> NdnResult<Self> {
        let mut cyfs_headers = get_cyfs_resp_headers(headers)?;
        if cyfs_headers.obj_size.is_none() {
            cyfs_headers.obj_size = content_length;
        }

        let cyfs_head = parse_cyfs_head(headers)?;
        merge_cyfs_head_into_headers(&mut cyfs_headers, cyfs_head.as_ref());

        Ok(Self {
            requested_url,
            transport_url,
            known_obj_id,
            url_obj_id,
            url_inner_path,
            cyfs_headers,
            cyfs_head,
        })
    }

    pub fn effective_obj_id(&self) -> Option<ObjId> {
        if let Some(obj_id) = self.known_obj_id.clone() {
            return Some(obj_id);
        }
        if let Some(obj_id) = self.cyfs_headers.obj_id.clone() {
            return Some(obj_id);
        }
        if let Some(obj_id) = self.cyfs_head.as_ref().and_then(|head| head.obj_id()) {
            return Some(obj_id);
        }
        self.url_obj_id.clone()
    }
}

impl CyfsHead {
    fn obj_id(&self) -> Option<ObjId> {
        match self {
            CyfsHead::ObjId(obj_id) => Some(obj_id.clone()),
            CyfsHead::RespHeaders(headers) => headers.obj_id.clone(),
            CyfsHead::FileObject { obj_id, .. } => Some(obj_id.clone()),
            CyfsHead::DirObject { obj_id, .. } => Some(obj_id.clone()),
            CyfsHead::ChunkList { obj_id, .. } => Some(obj_id.clone()),
        }
    }
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
        let effective_obj_id = self.meta.effective_obj_id();
        let transport_url = self.meta.transport_url.clone();
        let mut reader = self.reader();
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw).await?;
        let obj_str = String::from_utf8(raw)
            .map_err(|e| NdnError::DecodeError(format!("response body is not utf-8: {}", e)))?;

        let effective_obj_id = effective_obj_id.ok_or_else(|| {
            NdnError::InvalidId(format!(
                "no obj id found in request url / headers / cyfs-head for {}",
                transport_url
            ))
        })?;

        let obj_json = if effective_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
            let chunk_list = SimpleChunkList::from_json(obj_str.as_str())?;
            let (real_obj_id, _) = clone_chunk_list(&chunk_list)?.gen_obj_id();
            if real_obj_id != effective_obj_id {
                return Err(NdnError::InvalidId(format!(
                    "verify chunklist object failed, expect:{} actual:{}",
                    effective_obj_id.to_string(),
                    real_obj_id.to_string()
                )));
            }
            serde_json::from_str::<Value>(obj_str.as_str()).map_err(|e| {
                NdnError::InvalidData(format!("parse chunklist json failed: {}", e))
            })?
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

    pub async fn pull_to_local_file(self, local_path: impl Into<PathBuf>) -> NdnResult<CyfsPullResult> {
        self.pull(StoreMode::LocalFile(local_path.into(), 0..0, false), None)
            .await
    }

    pub async fn pull_to_local_file_with_named_store(
        self,
        local_path: impl Into<PathBuf>,
        store_mgr: &NamedStoreMgr,
    ) -> NdnResult<CyfsPullResult> {
        self.pull(
            StoreMode::LocalFile(local_path.into(), 0..0, true),
            Some(store_mgr.clone()),
        )
        .await
    }

    pub async fn pull_to_named_store(self, store_mgr: &NamedStoreMgr) -> NdnResult<CyfsPullResult> {
        self.pull(StoreMode::StoreInNamedMgr, Some(store_mgr.clone()))
            .await
    }

    pub async fn pull(
        self,
        pull_mode: StoreMode,
        store_mgr: Option<NamedStoreMgr>,
    ) -> NdnResult<CyfsPullResult> {
        let target_store_mgr = resolve_target_store_mgr(
            &pull_mode,
            store_mgr,
            self.client.default_store_mgr.clone(),
        )?;
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
            NdnError::InvalidId(format!("cannot infer obj id for {}", self.meta.transport_url))
        })?;

        if effective_obj_id.obj_type == OBJ_TYPE_FILE {
            let request = self.request.clone();
            let client = self.client.clone();
            let verified_obj = self.into_verified_object().await?;
            let file_obj: FileObject = serde_json::from_value(verified_obj.obj_json).map_err(|e| {
                NdnError::InvalidData(format!("parse file object failed: {}", e))
            })?;
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

        if effective_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
            let request = self.request.clone();
            let client = self.client.clone();
            let verified_obj = self.into_verified_object().await?;
            let chunk_list = SimpleChunkList::from_json_value(verified_obj.obj_json)?;
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

    async fn describe_raw_pull(&self) -> NdnResult<Option<PullDescriptor>> {
        if let Some(head) = self.meta.cyfs_head.as_ref() {
            match head {
                CyfsHead::ChunkList {
                    obj_id,
                    obj_str,
                    chunk_list,
                } => {
                    return Ok(Some(PullDescriptor::ChunkList {
                        chunk_list: clone_chunk_list(chunk_list)?,
                        metadata_to_store: vec![KnownObjectToStore {
                            obj_id: obj_id.clone(),
                            obj_str: obj_str.clone(),
                        }],
                        result_obj_id: Some(obj_id.clone()),
                        file_action: None,
                        file_size: chunk_list.total_size,
                    }))
                }
                CyfsHead::FileObject {
                    obj_id,
                    obj,
                    obj_str,
                } => {
                    let content_obj_id = ObjId::new(obj.content.as_str())?;
                    let file_obj_store = KnownObjectToStore {
                        obj_id: obj_id.clone(),
                        obj_str: obj_str.clone(),
                    };
                    if content_obj_id.is_chunk() {
                        return Ok(Some(PullDescriptor::Chunk {
                            chunk_id: ChunkId::from_obj_id(&content_obj_id),
                            metadata_to_store: vec![file_obj_store],
                            file_action: Some(NdnAction::FileOK(obj_id.clone(), obj.size)),
                        }));
                    }
                    if content_obj_id.obj_type == OBJ_TYPE_CHUNK_LIST_SIMPLE {
                        let chunk_list_obj = self
                            .client
                            .get_verified_object_by_id(&self.request.resolved_url, &content_obj_id)
                            .await?;
                        let chunk_list = SimpleChunkList::from_json_value(chunk_list_obj.obj_json)?;
                        return Ok(Some(PullDescriptor::ChunkList {
                            chunk_list,
                            metadata_to_store: vec![
                                KnownObjectToStore {
                                    obj_id: chunk_list_obj.obj_id,
                                    obj_str: chunk_list_obj.obj_str,
                                },
                                file_obj_store,
                            ],
                            result_obj_id: Some(obj_id.clone()),
                            file_action: Some(NdnAction::FileOK(obj_id.clone(), obj.size)),
                            file_size: obj.size,
                        }));
                    }
                    return Err(NdnError::Unsupported(format!(
                        "unsupported file content obj type: {}",
                        content_obj_id.obj_type
                    )));
                }
                _ => {}
            }
        }

        let effective_obj_id = self.meta.effective_obj_id().ok_or_else(|| {
            NdnError::InvalidId(format!("cannot infer obj id for {}", self.meta.transport_url))
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

    async fn pull_raw_chunk(
        self,
        chunk_id: ChunkId,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedStoreMgr>,
        metadata_to_store: Vec<KnownObjectToStore>,
        file_action: Option<NdnAction>,
    ) -> NdnResult<CyfsPullResult> {
        let effective_obj_id = self.meta.effective_obj_id();
        let obj_size_hint = self.meta.cyfs_headers.obj_size;
        let progress_callback = self.request.progress_callback.clone();
        let original_url = self.request.resolved_url.original_url.clone();
        let mut reader = self.reader();
        let chunk_size = chunk_id
            .get_length()
            .unwrap_or_else(|| obj_size_hint.unwrap_or_default());

        let mut stored_objects = Vec::new();
        if let Some(store_mgr) = target_store_mgr.as_ref() {
            for item in metadata_to_store.iter() {
                store_mgr.put_object(&item.obj_id, &item.obj_str).await?;
                stored_objects.push(item.obj_id.clone());
            }
        }

        match &pull_mode {
            StoreMode::StoreInNamedMgr => {
                let store_mgr = target_store_mgr.as_ref().ok_or_else(|| {
                    NdnError::NotFound("named store mgr is required".to_string())
                })?;
                let mut writer = store_mgr.open_new_chunk_writer(&chunk_id, chunk_size).await?;
                let hasher = Some(ChunkHasher::new_with_hash_method(
                    chunk_id.chunk_type.to_hash_method()?,
                )?);
                let total_size = copy_chunk(chunk_id.clone(), &mut reader, &mut writer, hasher, None).await?;
                writer.flush().await?;

                call_progress_callback(
                    &progress_callback,
                    format!("chunk:{}", chunk_id.to_string()),
                    NdnAction::ChunkOK(chunk_id.clone(), total_size),
                )
                .await?;

                if let Some(file_action) = file_action {
                    call_progress_callback(
                        &progress_callback,
                        original_url.clone(),
                        file_action,
                    )
                    .await?;
                }

                return Ok(CyfsPullResult {
                    obj_id: effective_obj_id,
                    total_size,
                    chunk_count: 1,
                    stored_objects,
                });
            }
            StoreMode::LocalFile(path, range, _) => {
                ensure_local_file_exists(path).await?;
                let mut writer = pull_mode.open_local_writer().await?;
                let hasher = Some(ChunkHasher::new_with_hash_method(
                    chunk_id.chunk_type.to_hash_method()?,
                )?);
                let total_size = copy_chunk(chunk_id.clone(), &mut reader, &mut writer, hasher, None).await?;
                writer.flush().await?;

                if pull_mode.need_store_to_named_mgr() {
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
                    call_progress_callback(
                        &progress_callback,
                        original_url.clone(),
                        file_action,
                    )
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
                let hasher = Some(ChunkHasher::new_with_hash_method(
                    chunk_id.chunk_type.to_hash_method()?,
                )?);
                let total_size = copy_chunk(chunk_id.clone(), &mut reader, &mut sink, hasher, None).await?;
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
        chunk_list: SimpleChunkList,
        pull_mode: StoreMode,
        target_store_mgr: Option<NamedStoreMgr>,
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
                let store_mgr = target_store_mgr.as_ref().ok_or_else(|| {
                    NdnError::NotFound("named store mgr is required".to_string())
                })?;
                store_mgr.put_chunk(chunk_id, chunk_bytes.as_slice(), true).await?;
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
            call_progress_callback(
                &progress_callback,
                original_url,
                file_action,
            )
            .await?;
        }

        Ok(CyfsPullResult {
            obj_id: effective_obj_id,
            total_size: file_size.max(total_size),
            chunk_count: chunk_list.body.len(),
            stored_objects,
        })
    }
}

impl ResolvedUrl {
    fn parse(url: &str) -> NdnResult<Self> {
        let parsed_original = Url::parse(url)
            .map_err(|e| NdnError::InvalidParam(format!("parse url {} failed: {}", url, e)))?;
        if let Ok((obj_id, inner_path)) = cyfs_get_obj_id_from_url(url) {
            let locator = if parsed_original
                .host_str()
                .and_then(|host| ObjId::from_hostname(host).ok())
                .as_ref()
                == Some(&obj_id)
            {
                Some(ObjLocator::HostFirstLabel)
            } else {
                parsed_original
                    .path_segments()
                    .and_then(|segments| {
                        segments
                            .enumerate()
                            .find(|(_, segment)| ObjId::new(segment).ok().as_ref() == Some(&obj_id))
                            .map(|(index, _)| ObjLocator::PathSegment(index))
                    })
            };

            return Ok(Self {
                original_url: url.to_string(),
                transport_url: url.to_string(),
                parsed_original,
                locator,
                url_obj_id: Some(obj_id),
                inner_path,
            });
        }

        Ok(Self {
            original_url: url.to_string(),
            transport_url: url.to_string(),
            parsed_original,
            locator: None,
            url_obj_id: None,
            inner_path: None,
        })
    }

    fn replace_obj_id(&self, obj_id: &ObjId) -> NdnResult<String> {
        let mut url = self.parsed_original.clone();
        match self.locator.as_ref() {
            Some(ObjLocator::HostFirstLabel) => {
                let host = url.host_str().ok_or_else(|| {
                    NdnError::InvalidParam(format!("missing host in {}", self.original_url))
                })?;
                let mut host_parts = host
                    .split('.')
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>();
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
                let mut new_segments = segments
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>();
                if *index >= new_segments.len() {
                    return Err(NdnError::InvalidParam(format!(
                        "path segment {} out of range for {}",
                        index, self.original_url
                    )));
                }
                new_segments[*index] = obj_id.to_string();
                let new_path = format!("/{}", new_segments.join("/"));
                url.set_path(new_path.as_str());
            }
            None => {
                return Err(NdnError::InvalidParam(format!(
                    "cannot replace obj id for {}",
                    self.original_url
                )));
            }
        }

        Ok(url.to_string())
    }
}

fn resolve_target_store_mgr(
    pull_mode: &StoreMode,
    explicit_store_mgr: Option<NamedStoreMgr>,
    default_store_mgr: Option<NamedStoreMgr>,
) -> NdnResult<Option<NamedStoreMgr>> {
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

async fn open_local_writer_if_needed(pull_mode: &StoreMode) -> NdnResult<Option<ndn_lib::ChunkWriter>> {
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
    store_mgr: Option<&NamedStoreMgr>,
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
        if !matches!(result, ProgressCallbackResult::Continue | ProgressCallbackResult::Skip) {
            return Err(NdnError::InvalidState("break by user".to_string()));
        }
    }
    Ok(())
}

fn merge_cyfs_head_into_headers(headers: &mut CYFSHttpRespHeaders, cyfs_head: Option<&CyfsHead>) {
    let Some(cyfs_head) = cyfs_head else {
        return;
    };

    match cyfs_head {
        CyfsHead::ObjId(obj_id) => {
            if headers.obj_id.is_none() {
                headers.obj_id = Some(obj_id.clone());
            }
        }
        CyfsHead::RespHeaders(head_headers) => {
            if headers.obj_id.is_none() {
                headers.obj_id = head_headers.obj_id.clone();
            }
            if headers.obj_size.is_none() {
                headers.obj_size = head_headers.obj_size;
            }
            if headers.path_obj.is_none() {
                headers.path_obj = head_headers.path_obj.clone();
            }
            if headers.root_obj_id.is_none() {
                headers.root_obj_id = head_headers.root_obj_id.clone();
            }
            if headers.mtree_path.is_empty() {
                headers.mtree_path = head_headers.mtree_path.clone();
            }
            if headers.embed_objs.is_none() {
                headers.embed_objs = head_headers.embed_objs.clone();
            }
        }
        CyfsHead::FileObject { obj_id, .. }
        | CyfsHead::DirObject { obj_id, .. }
        | CyfsHead::ChunkList { obj_id, .. } => {
            if headers.obj_id.is_none() {
                headers.obj_id = Some(obj_id.clone());
            }
        }
    }
}

fn parse_cyfs_head(headers: &HeaderMap) -> NdnResult<Option<CyfsHead>> {
    let Some(raw) = headers.get("cyfs-head") else {
        return Ok(None);
    };
    let raw = raw.to_str().map_err(|e| {
        NdnError::DecodeError(format!("decode cyfs-head header failed: {}", e))
    })?;
    Ok(Some(parse_cyfs_head_value(raw)?))
}

fn parse_cyfs_head_value(raw: &str) -> NdnResult<CyfsHead> {
    if let Ok(obj_id) = ObjId::new(raw) {
        return Ok(CyfsHead::ObjId(obj_id));
    }

    let value: Value = serde_json::from_str(raw)
        .map_err(|e| NdnError::DecodeError(format!("parse cyfs-head json failed: {}", e)))?;

    if let Ok(envelope) = serde_json::from_value::<CyfsHeadEnvelope>(value.clone()) {
        if envelope.obj_id.is_some()
            || envelope.obj_size.is_some()
            || envelope.root_obj_id.is_some()
            || envelope.path_obj.is_some()
            || envelope
                .mtree_path
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false)
            || !envelope.embed_objs.is_empty()
        {
            return Ok(CyfsHead::RespHeaders(envelope.into_headers()?));
        }
    }

    if let Ok(chunk_list) = SimpleChunkList::from_json_value(value.clone()) {
        let (obj_id, obj_str) = clone_chunk_list(&chunk_list)?.gen_obj_id();
        return Ok(CyfsHead::ChunkList {
            obj_id,
            obj_str,
            chunk_list,
        });
    }

    if let Ok(file_obj) = serde_json::from_value::<FileObject>(value.clone()) {
        let (obj_id, obj_str) = file_obj.clone().gen_obj_id();
        return Ok(CyfsHead::FileObject {
            obj_id,
            obj: file_obj,
            obj_str,
        });
    }

    if let Ok(dir_obj) = serde_json::from_value::<DirObject>(value) {
        let (obj_id, obj_str) = dir_obj.clone().gen_obj_id()?;
        return Ok(CyfsHead::DirObject {
            obj_id,
            obj: dir_obj,
            obj_str,
        });
    }

    Err(NdnError::DecodeError(
        "unsupported cyfs-head payload".to_string(),
    ))
}

impl CyfsHeadEnvelope {
    fn into_headers(self) -> NdnResult<CYFSHttpRespHeaders> {
        let obj_id = self
            .obj_id
            .as_ref()
            .map(|v| ObjId::new(v))
            .transpose()?;
        let root_obj_id = self
            .root_obj_id
            .as_ref()
            .map(|v| ObjId::new(v))
            .transpose()?;
        let embed_objs = if self.embed_objs.is_empty() {
            None
        } else {
            let mut mapped = HashMap::new();
            for (key, value) in self.embed_objs {
                mapped.insert(ObjId::new(key.as_str())?, value);
            }
            Some(mapped)
        };

        Ok(CYFSHttpRespHeaders {
            obj_id,
            obj_size: self.obj_size,
            path_obj: self.path_obj,
            root_obj_id,
            mtree_path: self.mtree_path.unwrap_or_default(),
            embed_objs,
        })
    }
}

fn clone_chunk_list(chunk_list: &SimpleChunkList) -> NdnResult<SimpleChunkList> {
    SimpleChunkList::from_chunk_list(chunk_list.body.clone())
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
