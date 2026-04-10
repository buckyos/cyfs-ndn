//! `HttpBackend` —— `NamedDataStoreBackend` 的 HTTP 客户端实现。
//!
//! 通过 HTTP 协议访问远端（或本机）的 `NamedStoreMgrHttpGateway`。
//! 协议详情见 `doc/named-data-http-store-protocol.md`。
//!
//! 资源类型由 URL 中的 obj_id 自动判定（`obj_id.is_chunk()`），不需要额外 header。

use async_trait::async_trait;
use log::debug;
use ndn_lib::{ChunkId, ChunkReader, NdnError, NdnResult, ObjId};
use reqwest::{Client, StatusCode};
use tokio::io::AsyncReadExt;

use crate::backend::{ChunkStateInfo, ChunkWriteOutcome, NamedDataStoreBackend};

const H_CHUNK_SIZE: &str = "x-cyfs-chunk-size";
const H_OBJ_ID: &str = "x-cyfs-obj-id";
const H_CHUNK_ALREADY: &str = "x-cyfs-chunk-already";

const CONTENT_TYPE_OBJECT: &str = "application/cyfs-object";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";

/// HTTP backend configuration.
#[derive(Debug, Clone)]
pub struct HttpBackendConfig {
    /// Base URL of the remote store, e.g. `http://127.0.0.1:3180/ndn`.
    /// obj_id will be appended as `{base_url}/{obj_id}`.
    pub base_url: String,
}

/// HTTP client backend for `NamedDataStoreBackend`.
pub struct HttpBackend {
    config: HttpBackendConfig,
    client: Client,
}

impl HttpBackend {
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
impl NamedDataStoreBackend for HttpBackend {
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
        debug!("HttpBackend::open_chunk_reader GET {} offset={}", url, offset);

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
        let mapped = byte_stream.map(|result| {
            result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        });
        let stream_reader = tokio_util::io::StreamReader::new(mapped);
        let reader: ChunkReader = Box::pin(stream_reader);

        Ok((reader, total_size))
    }

    async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        mut source: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
        // 必须先读完 source 再发 HTTP 请求（reqwest 的 body 需要完整数据或 Stream）
        let mut buf = Vec::with_capacity(chunk_size as usize);
        let n = source
            .read_to_end(&mut buf)
            .await
            .map_err(|e| NdnError::IoError(format!("read source: {e}")))?;
        if n as u64 != chunk_size {
            return Err(NdnError::IoError(format!(
                "source size mismatch: expected {} got {}",
                chunk_size, n
            )));
        }

        let obj_id = chunk_id.to_obj_id();
        let url = self.url_for(&obj_id);
        debug!("HttpBackend::open_chunk_writer PUT {} size={}", url, chunk_size);

        let resp = self
            .client
            .put(&url)
            .header(H_CHUNK_SIZE, chunk_size)
            .header("content-type", CONTENT_TYPE_OCTET)
            .body(buf)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))?;

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
        if status == StatusCode::NO_CONTENT || status == StatusCode::NOT_FOUND || status.is_success()
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
        if status == StatusCode::NO_CONTENT || status == StatusCode::NOT_FOUND || status.is_success()
        {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &body))
    }
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
        let backend = HttpBackend::new(config);
        let obj_id = ObjId::new("sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789").unwrap();
        let url = backend.url_for(&obj_id);
        assert!(url.starts_with("http://127.0.0.1:3180/ndn/"));
        assert!(url.contains("sha256:"));
    }

    #[test]
    fn test_url_trailing_slash() {
        let config = HttpBackendConfig {
            base_url: "http://127.0.0.1:3180/ndn/".to_string(),
        };
        let backend = HttpBackend::new(config);
        let obj_id = ObjId::new("sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789").unwrap();
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
