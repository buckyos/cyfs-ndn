//! `HttpBackend` —— `NamedDataStoreBackend` 的 HTTP 客户端实现。
//!
//! 通过 HTTP 协议访问远端（或本机）的 `NamedStoreMgrHttpGateway`。
//! 协议详情见 `doc/named-data-http-store-protocol.md`。
//!
//! 资源类型由 URL 中的 obj_id 自动判定（`obj_id.is_chunk()`），不需要额外 header。

use async_trait::async_trait;
use bytes::Bytes;
use http::Request;
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use log::debug;
use ndn_lib::{ChunkId, ChunkReader, NdnError, NdnResult, ObjId};
use reqwest::Body;
use reqwest::{Client, StatusCode};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::backend::{ChunkStateInfo, ChunkWriteOutcome, NamedDataStoreBackend};

const H_CHUNK_SIZE: &str = "x-cyfs-chunk-size";
const H_OBJ_ID: &str = "x-cyfs-obj-id";
const H_CHUNK_ALREADY: &str = "x-cyfs-chunk-already";

const CONTENT_TYPE_OBJECT: &str = "application/cyfs-object";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";
const STREAM_BUF_SIZE: usize = 64 * 1024;

/// HTTP backend configuration.
#[derive(Debug, Clone)]
pub struct HttpBackendConfig {
    /// Base URL of the remote store, e.g. `http://127.0.0.1:3180/ndn`.
    /// obj_id will be appended as `{base_url}/{obj_id}`.
    pub base_url: String,
}

/// HTTP client backend for `NamedDataStoreBackend`.
pub struct NamedStoreHttpBackend {
    config: HttpBackendConfig,
    client: Client,
}

impl NamedStoreHttpBackend {
    pub fn new(config: HttpBackendConfig) -> Self {
        let client = Client::new();
        Self { config, client }
    }

    pub fn with_client(config: HttpBackendConfig, client: Client) -> Self {
        Self { config, client }
    }

    fn url_for(&self, obj_id: &ObjId) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!("{}/{}", base, obj_id.to_string())
    }
}

#[async_trait]
impl NamedDataStoreBackend for NamedStoreHttpBackend {
    // ======================== Object ========================

    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let url = self.url_for(obj_id);
        debug!("HttpBackend::get_object GET {}", url);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(NdnError::NotFound(obj_id.to_string()));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &body));
        }

        resp.text()
            .await
            .map_err(|e| NdnError::IoError(format!("read response body: {e}")))
    }

    async fn put_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<()> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let url = self.url_for(obj_id);
        debug!("HttpBackend::put_object PUT {}", url);

        let resp = self
            .client
            .put(&url)
            .header(H_OBJ_ID, obj_id.to_string())
            .header("content-type", CONTENT_TYPE_OBJECT)
            .body(obj_str.to_owned())
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NO_CONTENT || status.is_success() {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &body))
    }

    // ======================== Chunk ========================

    async fn get_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStateInfo> {
        let obj_id = chunk_id.to_obj_id();
        let url = self.url_for(&obj_id);
        debug!("HttpBackend::get_chunk_state HEAD {}", url);

        let resp = self
            .client
            .head(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("HEAD {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(ChunkStateInfo::not_exist());
        }
        if !status.is_success() {
            return Err(map_http_error(status, ""));
        }

        let chunk_size = resp
            .headers()
            .get(H_CHUNK_SIZE)
            .or_else(|| resp.headers().get("content-length"))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(ChunkStateInfo::completed(chunk_size))
    }

    async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let obj_id = chunk_id.to_obj_id();
        let url = self.url_for(&obj_id);
        debug!(
            "HttpBackend::open_chunk_reader GET {} offset={}",
            url, offset
        );

        let mut req = self.client.get(&url);
        if offset > 0 {
            req = req.header("range", format!("bytes={}-", offset));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(NdnError::NotFound(chunk_id.to_string()));
        }
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &body));
        }

        // total_size from X-CYFS-Chunk-Size (preferred) or Content-Length / Content-Range
        let total_size = resp
            .headers()
            .get(H_CHUNK_SIZE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| {
                if offset == 0 {
                    resp.content_length().unwrap_or(0)
                } else {
                    // Content-Range: bytes N-M/total
                    resp.headers()
                        .get("content-range")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.rsplit('/').next())
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or(0)
                }
            });

        // 流式：把 reqwest 的 byte stream 转成 AsyncRead，不全量缓冲
        let byte_stream = resp.bytes_stream();
        use futures_util::StreamExt;
        let mapped = byte_stream
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
        let stream_reader = tokio_util::io::StreamReader::new(mapped);
        let reader: ChunkReader = Box::pin(stream_reader);

        Ok((reader, total_size))
    }

    async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        source: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
        let obj_id = chunk_id.to_obj_id();
        let url = self.url_for(&obj_id);
        debug!(
            "HttpBackend::open_chunk_writer PUT {} size={}",
            url, chunk_size
        );

        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| NdnError::InvalidParam(format!("invalid PUT url {url}: {e}")))?;

        if parsed.scheme() == "http" {
            let resp = self
                .stream_put_chunk_http1(&parsed, chunk_size, source)
                .await?;
            let status = resp.status();
            if status.is_success() {
                let already = resp
                    .headers()
                    .get(H_CHUNK_ALREADY)
                    .and_then(|v| v.to_str().ok())
                    == Some("1");
                if already {
                    return Ok(ChunkWriteOutcome::AlreadyExists);
                }
                return Ok(ChunkWriteOutcome::Written);
            }

            let body = resp.text();
            return Err(map_http_error(status, &body));
        }

        let resp = self
            .stream_put_chunk_reqwest(&url, chunk_size, source)
            .await?;
        let status = resp.status();
        if status.is_success() {
            let already = resp
                .headers()
                .get(H_CHUNK_ALREADY)
                .and_then(|v| v.to_str().ok())
                == Some("1");
            if already {
                return Ok(ChunkWriteOutcome::AlreadyExists);
            }
            return Ok(ChunkWriteOutcome::Written);
        }

        let body = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &body))
    }

    // ======================== Delete ========================

    async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        let url = self.url_for(obj_id);
        debug!("HttpBackend::remove_object DELETE {}", url);

        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("DELETE {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_FOUND
            || status.is_success()
        {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &body))
    }

    async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let obj_id = chunk_id.to_obj_id();
        let url = self.url_for(&obj_id);
        debug!("HttpBackend::remove_chunk DELETE {}", url);

        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("DELETE {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_FOUND
            || status.is_success()
        {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &body))
    }
}

impl NamedStoreHttpBackend {
    async fn stream_put_chunk_reqwest(
        &self,
        url: &str,
        chunk_size: u64,
        source: ChunkReader,
    ) -> NdnResult<reqwest::Response> {
        let body = Body::wrap_stream(tokio_util::io::ReaderStream::new(source));
        self.client
            .put(url)
            .header(H_CHUNK_SIZE, chunk_size)
            .header("content-type", CONTENT_TYPE_OCTET)
            .header("content-length", chunk_size)
            .header("expect", "100-continue")
            .body(body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))
    }

    async fn stream_put_chunk_http1(
        &self,
        url: &reqwest::Url,
        chunk_size: u64,
        source: ChunkReader,
    ) -> NdnResult<SimpleHttpResponse> {
        let host = url
            .host_str()
            .ok_or_else(|| NdnError::InvalidParam(format!("missing host in {}", url)))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| NdnError::InvalidParam(format!("missing port in {}", url)))?;
        let authority = match url.port() {
            Some(port) => format!("{}:{}", host, port),
            None => host.to_string(),
        };
        let path_and_query = match url.query() {
            Some(query) => format!("{}?{}", url.path(), query),
            None => url.path().to_string(),
        };

        let stream = TcpStream::connect((host, port))
            .await
            .map_err(|e| NdnError::RemoteError(format!("connect {}: {}: {e}", host, port)))?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) = http1::handshake(io)
            .await
            .map_err(|e| NdnError::RemoteError(format!("handshake {}: {e}", url)))?;
        let conn_task = tokio::spawn(async move { conn.await });

        let (body, pump_task) = chunk_reader_to_http_body(source, chunk_size);
        let req = Request::builder()
            .method("PUT")
            .uri(path_and_query)
            .header("host", authority)
            .header(H_CHUNK_SIZE, chunk_size)
            .header("content-type", CONTENT_TYPE_OCTET)
            .header("content-length", chunk_size)
            .header("expect", "100-continue")
            .body(body)
            .map_err(|e| NdnError::Internal(format!("build PUT request: {e}")))?;

        let resp = sender
            .send_request(req)
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))?;

        pump_task.abort();
        let result = collect_simple_response(resp)
            .await
            .map_err(|e| NdnError::RemoteError(format!("read PUT response {url}: {e}")))?;

        drop(sender);
        let _ = conn_task.await;

        Ok(result)
    }
}

#[derive(Debug)]
struct SimpleHttpResponse {
    status: StatusCode,
    headers: http::HeaderMap,
    body: String,
}

impl SimpleHttpResponse {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    fn text(self) -> String {
        self.body
    }
}

async fn collect_simple_response(
    resp: http::Response<hyper::body::Incoming>,
) -> Result<SimpleHttpResponse, hyper::Error> {
    let (parts, body) = resp.into_parts();
    let collected = body.collect().await?;
    Ok(SimpleHttpResponse {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
    })
}

fn chunk_reader_to_http_body(
    mut reader: ChunkReader,
    total: u64,
) -> (BoxBody<Bytes, std::io::Error>, JoinHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(2);

    let task = tokio::spawn(async move {
        let mut sent = 0u64;
        while sent < total {
            let to_read = std::cmp::min(STREAM_BUF_SIZE as u64, total - sent) as usize;
            let mut buf = vec![0u8; to_read];
            match reader.read(&mut buf).await {
                Ok(0) => {
                    let err = std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("source size mismatch: expected {} got {}", total, sent),
                    );
                    let _ = tx.send(Err(err)).await;
                    return;
                }
                Ok(n) => {
                    buf.truncate(n);
                    sent += n as u64;
                    if tx.send(Ok(Frame::data(Bytes::from(buf)))).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
    });

    let body = BodyExt::boxed(StreamBody::new(ReceiverStream::new(rx)));
    (body, task)
}

/// Map HTTP error status to NdnError by trying to parse JSON body first.
/// Public so that `HttpGcClient` can reuse it.
pub(crate) fn map_http_error_public(status: StatusCode, body: &str) -> NdnError {
    map_http_error(status, body)
}

fn map_http_error(status: StatusCode, body: &str) -> NdnError {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        let error_code = json.get("error").and_then(|v| v.as_str()).unwrap_or("");
        let message = json.get("message").and_then(|v| v.as_str()).unwrap_or(body);

        return match error_code {
            "not_found" => NdnError::NotFound(message.to_string()),
            "verify_failed" => NdnError::VerifyError(message.to_string()),
            "permission_denied" => NdnError::PermissionDenied(message.to_string()),
            "invalid_obj_type" => NdnError::InvalidObjType(message.to_string()),
            "invalid_param" => NdnError::InvalidParam(message.to_string()),
            "invalid_data" => NdnError::InvalidData(message.to_string()),
            "invalid_id" => NdnError::InvalidId(message.to_string()),
            "offset_too_large" => NdnError::OffsetTooLarge(message.to_string()),
            "already_exists" => NdnError::AlreadyExists(message.to_string()),
            _ => NdnError::RemoteError(format!("HTTP {}: {}", status, message)),
        };
    }

    match status {
        StatusCode::NOT_FOUND => NdnError::NotFound(body.to_string()),
        StatusCode::BAD_REQUEST => NdnError::InvalidParam(body.to_string()),
        StatusCode::CONFLICT => NdnError::VerifyError(body.to_string()),
        StatusCode::FORBIDDEN => NdnError::PermissionDenied(body.to_string()),
        StatusCode::RANGE_NOT_SATISFIABLE => NdnError::OffsetTooLarge(body.to_string()),
        _ => NdnError::RemoteError(format!("HTTP {}: {}", status, body)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_construction() {
        let config = HttpBackendConfig {
            base_url: "http://127.0.0.1:3180/ndn".to_string(),
        };
        let backend = NamedStoreHttpBackend::new(config);
        let obj_id =
            ObjId::new("sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
                .unwrap();
        let url = backend.url_for(&obj_id);
        assert!(url.starts_with("http://127.0.0.1:3180/ndn/"));
        assert!(url.contains("sha256:"));
    }

    #[test]
    fn test_url_trailing_slash() {
        let config = HttpBackendConfig {
            base_url: "http://127.0.0.1:3180/ndn/".to_string(),
        };
        let backend = NamedStoreHttpBackend::new(config);
        let obj_id =
            ObjId::new("sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
                .unwrap();
        let url = backend.url_for(&obj_id);
        assert!(!url.contains("//ndn//"));
    }

    #[test]
    fn test_map_http_error_json() {
        let body = r#"{"error":"verify_failed","message":"chunk hash mismatch"}"#;
        let err = map_http_error(StatusCode::CONFLICT, body);
        assert!(matches!(err, NdnError::VerifyError(_)));
    }

    #[test]
    fn test_map_http_error_fallback() {
        let err = map_http_error(StatusCode::NOT_FOUND, "not here");
        assert!(matches!(err, NdnError::NotFound(_)));
    }
}
