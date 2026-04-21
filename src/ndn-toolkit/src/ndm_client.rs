//! `ndm_client` —— NamedDataMgr Proxy 协议客户端。
//!
//! 协议详情见 `doc/NDM Protocol/named-data-mgr-proxy-protocol.md`。
//!
//! 典型调用链：`Zone App -> NdmClient -> NDM Proxy -> NamedDataMgr`。
//!
//! 设计目标：在不直接链接服务端内存对象的前提下，提供与 `NamedDataMgr`
//! 接近的调用体验。API 命名刻意与 `named_store::NamedDataMgr` 对齐，
//! 但只暴露协议中纳入代理的能力——layout/register_store 等装配类能力
//! 不走该协议。

use futures_util::StreamExt;
use log::debug;
use ndn_lib::{ChunkId, ChunkReader, NdnError, NdnResult, ObjId};
use named_store::{
    CascadeStateP0, ChunkLocalInfo, ChunkStoreState, ChunkWriteOutcome, EdgeMsg, ExpandDebug,
    ObjectState, PinRequest, PinScope,
};
use reqwest::{Body, Client, Response, StatusCode};
use serde_json::{json, Value};
use std::ops::Range;
use std::time::Duration;

const DEFAULT_PROXY_PATH_PREFIX: &str = "/ndm/proxy/v1";
const H_TOTAL_SIZE: &str = "ndm-total-size";
const H_CHUNK_SIZE: &str = "ndm-chunk-size";
const H_WRITE_OUTCOME: &str = "ndm-chunk-write-outcome";
const CONTENT_TYPE_OCTET: &str = "application/octet-stream";

/// Client configuration.
#[derive(Debug, Clone)]
pub struct NdmClientConfig {
    /// NDM Proxy 入口，例如 `http://127.0.0.1:3180`。
    /// 协议前缀 `/ndm/proxy/v1` 会在内部自动拼接。
    pub base_url: String,
}

/// NamedDataMgr Proxy 客户端。
///
/// 内部持有 `reqwest::Client`，可以跨多个调用复用连接池。克隆代价低。
#[derive(Clone)]
pub struct NdmClient {
    client: Client,
    root: String,
}

impl NdmClient {
    pub fn new(config: NdmClientConfig) -> Self {
        let client = Client::new();
        Self::with_client(config, client)
    }

    pub fn with_client(config: NdmClientConfig, client: Client) -> Self {
        let base = config.base_url.trim_end_matches('/');
        let root = format!("{}{}", base, DEFAULT_PROXY_PATH_PREFIX);
        Self { client, root }
    }

    fn rpc_url(&self, method: &str) -> String {
        format!("{}/rpc/{}", self.root, method)
    }

    fn read_url(&self, path: &str) -> String {
        format!("{}/read/{}", self.root, path.trim_start_matches('/'))
    }

    fn write_chunk_url(&self, chunk_id: &ChunkId) -> String {
        format!("{}/write/chunk/{}", self.root, chunk_id.to_string())
    }

    // ==================== Object ====================

    /// 对应 `NamedDataMgr::get_object`.
    pub async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
        let resp = self
            .rpc_call("get_object", &json!({ "obj_id": obj_id.to_string() }))
            .await?;
        take_string(&resp, "obj_data")
    }

    /// 对应 `NamedDataMgr::open_object`.
    pub async fn open_object(
        &self,
        obj_id: &ObjId,
        inner_path: Option<String>,
    ) -> NdnResult<String> {
        let body = build_obj_with_path(obj_id, inner_path);
        let resp = self.rpc_call("open_object", &body).await?;
        take_string(&resp, "obj_data")
    }

    /// 对应 `NamedDataMgr::get_dir_child`.
    pub async fn get_dir_child(&self, dir_obj_id: &ObjId, item_name: &str) -> NdnResult<ObjId> {
        let resp = self
            .rpc_call(
                "get_dir_child",
                &json!({
                    "dir_obj_id": dir_obj_id.to_string(),
                    "item_name": item_name,
                }),
            )
            .await?;
        let id_str = take_str(&resp, "obj_id")?;
        ObjId::new(id_str).map_err(|e| NdnError::InvalidId(format!("invalid obj_id: {e}")))
    }

    /// 对应 `NamedDataMgr::is_object_stored`.
    pub async fn is_object_stored(
        &self,
        obj_id: &ObjId,
        inner_path: Option<String>,
    ) -> NdnResult<bool> {
        let body = build_obj_with_path(obj_id, inner_path);
        let resp = self.rpc_call("is_object_stored", &body).await?;
        Ok(resp.get("stored").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// 对应 `NamedDataMgr::is_object_exist`.
    pub async fn is_object_exist(&self, obj_id: &ObjId) -> NdnResult<bool> {
        let resp = self
            .rpc_call(
                "is_object_exist",
                &json!({ "obj_id": obj_id.to_string() }),
            )
            .await?;
        Ok(resp.get("exists").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// 对应 `NamedDataMgr::query_object_by_id`.
    pub async fn query_object_by_id(&self, obj_id: &ObjId) -> NdnResult<ObjectState> {
        let resp = self
            .rpc_call(
                "query_object_by_id",
                &json!({ "obj_id": obj_id.to_string() }),
            )
            .await?;
        parse_object_state(&resp)
    }

    /// 对应 `NamedDataMgr::put_object`. 不接受 chunk id。
    pub async fn put_object(&self, obj_id: &ObjId, obj_data: &str) -> NdnResult<()> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(format!(
                "{} is chunk; use put_chunk instead",
                obj_id.to_string()
            )));
        }
        self.rpc_call_no_content(
            "put_object",
            &json!({
                "obj_id": obj_id.to_string(),
                "obj_data": obj_data,
            }),
        )
        .await
    }

    /// 对应 `NamedDataMgr::remove_object`. 不接受 chunk id。
    pub async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(format!(
                "{} is chunk; use remove_chunk instead",
                obj_id.to_string()
            )));
        }
        self.rpc_call_no_content(
            "remove_object",
            &json!({ "obj_id": obj_id.to_string() }),
        )
        .await
    }

    // ==================== Chunk metadata ====================

    /// 对应 `NamedDataMgr::have_chunk`.
    pub async fn have_chunk(&self, chunk_id: &ChunkId) -> bool {
        self.rpc_call(
            "have_chunk",
            &json!({ "chunk_id": chunk_id.to_string() }),
        )
        .await
        .ok()
        .and_then(|v| v.get("exists").and_then(|b| b.as_bool()))
        .unwrap_or(false)
    }

    /// 对应 `NamedDataMgr::query_chunk_state`.
    pub async fn query_chunk_state(
        &self,
        chunk_id: &ChunkId,
    ) -> NdnResult<(ChunkStoreState, u64)> {
        let resp = self
            .rpc_call(
                "query_chunk_state",
                &json!({ "chunk_id": chunk_id.to_string() }),
            )
            .await?;
        parse_chunk_store_state(&resp)
    }

    /// 对应 `NamedDataMgr::remove_chunk`.
    pub async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        self.rpc_call_no_content(
            "remove_chunk",
            &json!({ "chunk_id": chunk_id.to_string() }),
        )
        .await
    }

    /// 对应 `NamedDataMgr::add_chunk_by_same_as`.
    pub async fn add_chunk_by_same_as(
        &self,
        big_chunk_id: &ChunkId,
        big_chunk_size: u64,
        chunk_list_id: &ObjId,
    ) -> NdnResult<()> {
        self.rpc_call_no_content(
            "add_chunk_by_same_as",
            &json!({
                "big_chunk_id": big_chunk_id.to_string(),
                "chunk_list_id": chunk_list_id.to_string(),
                "big_chunk_size": big_chunk_size,
            }),
        )
        .await
    }

    // ==================== GC / Anchor / Debug (受限) ====================

    pub async fn apply_edge(&self, msg: EdgeMsg) -> NdnResult<()> {
        self.rpc_call_no_content("apply_edge", &msg).await
    }

    pub async fn pin(
        &self,
        obj_id: &ObjId,
        owner: &str,
        scope: PinScope,
        ttl: Option<Duration>,
    ) -> NdnResult<()> {
        let req = PinRequest {
            obj_id: obj_id.clone(),
            owner: owner.to_string(),
            scope,
            ttl_secs: ttl.map(|d| d.as_secs()),
        };
        self.rpc_call_no_content("pin", &req).await
    }

    pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()> {
        self.rpc_call_no_content(
            "unpin",
            &json!({
                "obj_id": obj_id.to_string(),
                "owner": owner,
            }),
        )
        .await
    }

    pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize> {
        let resp = self
            .rpc_call("unpin_owner", &json!({ "owner": owner }))
            .await?;
        Ok(resp
            .get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
    }

    pub async fn fs_acquire(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        self.rpc_call_no_content(
            "fs_acquire",
            &json!({
                "obj_id": obj_id.to_string(),
                "inode_id": inode_id,
                "field_tag": field_tag,
            }),
        )
        .await
    }

    pub async fn fs_release(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        self.rpc_call_no_content(
            "fs_release",
            &json!({
                "obj_id": obj_id.to_string(),
                "inode_id": inode_id,
                "field_tag": field_tag,
            }),
        )
        .await
    }

    pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize> {
        let resp = self
            .rpc_call("fs_release_inode", &json!({ "inode_id": inode_id }))
            .await?;
        Ok(resp
            .get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
    }

    pub async fn fs_anchor_state(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<CascadeStateP0> {
        let resp = self
            .rpc_call(
                "fs_anchor_state",
                &json!({
                    "obj_id": obj_id.to_string(),
                    "inode_id": inode_id,
                    "field_tag": field_tag,
                }),
            )
            .await?;
        parse_cascade_state(&resp)
    }

    pub async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64> {
        let resp = self
            .rpc_call(
                "forced_gc_until",
                &json!({ "target_bytes": target_bytes }),
            )
            .await?;
        Ok(resp
            .get("freed_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    pub async fn outbox_count(&self) -> NdnResult<u64> {
        let resp = self.rpc_call("outbox_count", &json!({})).await?;
        Ok(resp.get("count").and_then(|v| v.as_u64()).unwrap_or(0))
    }

    pub async fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug> {
        let resp = self
            .rpc_call(
                "debug_dump_expand_state",
                &json!({ "obj_id": obj_id.to_string() }),
            )
            .await?;
        serde_json::from_value(resp)
            .map_err(|e| NdnError::InvalidData(format!("invalid ExpandDebug: {e}")))
    }

    pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str) -> NdnResult<CascadeStateP0> {
        let resp = self
            .rpc_call(
                "anchor_state",
                &json!({
                    "obj_id": obj_id.to_string(),
                    "owner": owner,
                }),
            )
            .await?;
        parse_cascade_state(&resp)
    }

    // ==================== Stream Read ====================

    /// 对应 `NamedDataMgr::open_chunk_reader`.
    ///
    /// 返回 `(reader, total_chunk_size)`，reader 从 `offset` 处开始。
    pub async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let resp = self
            .read_request(
                "chunk/open",
                &json!({
                    "chunk_id": chunk_id.to_string(),
                    "offset": offset,
                }),
            )
            .await?;
        let total = header_u64(&resp, H_TOTAL_SIZE)
            .or_else(|| header_u64(&resp, H_CHUNK_SIZE))
            .unwrap_or(0);
        Ok((into_stream_reader(resp), total))
    }

    /// 对应 `NamedDataMgr::open_chunklist_reader`.
    pub async fn open_chunklist_reader(
        &self,
        chunk_list_id: &ObjId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let resp = self
            .read_request(
                "chunklist/open",
                &json!({
                    "chunk_list_id": chunk_list_id.to_string(),
                    "offset": offset,
                }),
            )
            .await?;
        let total = header_u64(&resp, H_TOTAL_SIZE).unwrap_or(0);
        Ok((into_stream_reader(resp), total))
    }

    /// 对应 `NamedDataMgr::open_reader`.
    pub async fn open_reader(
        &self,
        obj_id: &ObjId,
        inner_path: Option<String>,
    ) -> NdnResult<(ChunkReader, u64)> {
        let body = build_obj_with_path(obj_id, inner_path);
        let resp = self.read_request("object/open", &body).await?;
        let total = header_u64(&resp, H_TOTAL_SIZE).unwrap_or(0);
        Ok((into_stream_reader(resp), total))
    }

    /// 对应 `NamedDataMgr::get_chunk_data`. 整块读取。
    pub async fn get_chunk_data(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
        let resp = self
            .read_request(
                "chunk/data",
                &json!({ "chunk_id": chunk_id.to_string() }),
            )
            .await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| NdnError::IoError(format!("read chunk data: {e}")))?;
        Ok(bytes.to_vec())
    }

    /// 对应 `NamedDataMgr::get_chunk_piece`. 定长读取：短读视为错误。
    pub async fn get_chunk_piece(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        piece_size: u32,
    ) -> NdnResult<Vec<u8>> {
        let resp = self
            .read_request(
                "chunk/piece",
                &json!({
                    "chunk_id": chunk_id.to_string(),
                    "offset": offset,
                    "piece_size": piece_size,
                }),
            )
            .await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| NdnError::IoError(format!("read chunk piece: {e}")))?;
        if bytes.len() != piece_size as usize {
            return Err(NdnError::IoError(format!(
                "short read: expected {} got {}",
                piece_size,
                bytes.len()
            )));
        }
        Ok(bytes.to_vec())
    }

    // ==================== Stream Write ====================

    /// 对应 `NamedDataMgr::put_chunk_by_reader`. 原子一次写入。
    pub async fn put_chunk_by_reader(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        reader: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
        let url = self.write_chunk_url(chunk_id);
        debug!("NdmClient::put_chunk_by_reader PUT {} size={}", url, chunk_size);

        let body = Body::wrap_stream(tokio_util::io::ReaderStream::new(reader));
        let resp = self
            .client
            .put(&url)
            .header("content-type", CONTENT_TYPE_OCTET)
            .header("content-length", chunk_size)
            .header(H_CHUNK_SIZE, chunk_size)
            .body(body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &body));
        }

        let outcome = resp
            .headers()
            .get(H_WRITE_OUTCOME)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("written");
        Ok(match outcome {
            "already_exists" => ChunkWriteOutcome::AlreadyExists,
            _ => ChunkWriteOutcome::Written,
        })
    }

    /// 对应 `NamedDataMgr::put_chunk`. 便利方法：手里已有 `Vec<u8>` 时走同一写入接口。
    pub async fn put_chunk(&self, chunk_id: &ChunkId, chunk_data: &[u8]) -> NdnResult<()> {
        let url = self.write_chunk_url(chunk_id);
        let chunk_size = chunk_data.len() as u64;
        debug!("NdmClient::put_chunk PUT {} size={}", url, chunk_size);

        let resp = self
            .client
            .put(&url)
            .header("content-type", CONTENT_TYPE_OCTET)
            .header("content-length", chunk_size)
            .header(H_CHUNK_SIZE, chunk_size)
            .body(chunk_data.to_vec())
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("PUT {url}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &body));
        }
        Ok(())
    }

    // ==================== Internal helpers ====================

    /// Send a JSON RPC request and return the decoded JSON body.
    async fn rpc_call(&self, method: &str, body: &impl serde::Serialize) -> NdnResult<Value> {
        let url = self.rpc_url(method);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;

        let status = resp.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let text = resp
            .text()
            .await
            .map_err(|e| NdnError::IoError(format!("read response body: {e}")))?;
        if !status.is_success() {
            return Err(map_http_error(status, &text));
        }
        if text.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|e| NdnError::InvalidData(format!("invalid JSON response from {method}: {e}")))
    }

    /// Send a JSON RPC request that expects 204 / no response body.
    async fn rpc_call_no_content(
        &self,
        method: &str,
        body: &impl serde::Serialize,
    ) -> NdnResult<()> {
        let url = self.rpc_url(method);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let text = resp.text().await.unwrap_or_default();
        Err(map_http_error(status, &text))
    }

    /// Send a /read/* request and return the raw streaming response.
    async fn read_request(&self, path: &str, body: &impl serde::Serialize) -> NdnResult<Response> {
        let url = self.read_url(path);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &text));
        }
        Ok(resp)
    }
}

// ==================== Helpers ====================

fn build_obj_with_path(obj_id: &ObjId, inner_path: Option<String>) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("obj_id".to_string(), Value::String(obj_id.to_string()));
    if let Some(path) = normalize_inner_path(inner_path) {
        map.insert("inner_path".to_string(), Value::String(path));
    }
    Value::Object(map)
}

fn normalize_inner_path(path: Option<String>) -> Option<String> {
    match path.as_deref() {
        None | Some("") | Some("/") => None,
        _ => path,
    }
}

fn take_string(value: &Value, field: &str) -> NdnResult<String> {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| NdnError::InvalidData(format!("missing field '{field}' in response")))
}

fn take_str<'a>(value: &'a Value, field: &str) -> NdnResult<&'a str> {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| NdnError::InvalidData(format!("missing field '{field}' in response")))
}

fn header_u64(resp: &Response, name: &str) -> Option<u64> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}

fn into_stream_reader(resp: Response) -> ChunkReader {
    let stream = resp
        .bytes_stream()
        .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    Box::pin(tokio_util::io::StreamReader::new(stream))
}

fn parse_object_state(v: &Value) -> NdnResult<ObjectState> {
    let state = take_str(v, "state")?;
    match state {
        "not_exist" => Ok(ObjectState::NotExist),
        "object" => {
            let data = take_string(v, "obj_data")?;
            Ok(ObjectState::Object(data))
        }
        other => Err(NdnError::InvalidData(format!(
            "unknown object state: {other}"
        ))),
    }
}

fn parse_chunk_store_state(v: &Value) -> NdnResult<(ChunkStoreState, u64)> {
    let state = take_str(v, "state")?;
    let size = v
        .get("chunk_size")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let state = match state {
        "new" => ChunkStoreState::New,
        "completed" => ChunkStoreState::Completed,
        "disabled" => ChunkStoreState::Disabled,
        "not_exist" => ChunkStoreState::NotExist,
        "local_link" => {
            let info = v
                .get("local_info")
                .ok_or_else(|| NdnError::InvalidData("missing local_info".to_string()))?;
            ChunkStoreState::LocalLink(parse_local_info(info)?)
        }
        "same_as" => {
            let id_str = take_str(v, "same_as")?;
            let obj_id = ObjId::new(id_str)
                .map_err(|e| NdnError::InvalidId(format!("invalid same_as obj_id: {e}")))?;
            ChunkStoreState::SameAs(obj_id)
        }
        other => {
            return Err(NdnError::InvalidData(format!(
                "unknown chunk state: {other}"
            )))
        }
    };
    Ok((state, size))
}

fn parse_local_info(v: &Value) -> NdnResult<ChunkLocalInfo> {
    let qcid = take_string(v, "qcid")?;
    let last_modify_time = v
        .get("last_modify_time")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let range = v
        .get("range")
        .and_then(|r| match (r.get("start").and_then(|x| x.as_u64()), r.get("end").and_then(|x| x.as_u64())) {
            (Some(start), Some(end)) => Some(Range { start, end }),
            _ => None,
        });
    Ok(ChunkLocalInfo {
        path: String::new(),
        qcid,
        last_modify_time,
        range,
    })
}

fn parse_cascade_state(v: &Value) -> NdnResult<CascadeStateP0> {
    let state = take_str(v, "state")?;
    CascadeStateP0::from_str(state)
        .ok_or_else(|| NdnError::InvalidData(format!("unknown cascade state: {state}")))
}

/// Map proxy-protocol HTTP error to `NdnError`. 优先解析 JSON body `{error, message}`。
fn map_http_error(status: StatusCode, body: &str) -> NdnError {
    if let Ok(json) = serde_json::from_str::<Value>(body) {
        let code = json.get("error").and_then(|v| v.as_str()).unwrap_or("");
        let message = json.get("message").and_then(|v| v.as_str()).unwrap_or(body);
        return match code {
            "not_found" => NdnError::NotFound(message.to_string()),
            "invalid_param" => NdnError::InvalidParam(message.to_string()),
            "invalid_data" => NdnError::InvalidData(message.to_string()),
            "invalid_id" => NdnError::InvalidId(message.to_string()),
            "invalid_obj_type" => NdnError::InvalidObjType(message.to_string()),
            "verify_error" | "verify_failed" => NdnError::VerifyError(message.to_string()),
            "permission_denied" => NdnError::PermissionDenied(message.to_string()),
            "already_exists" => NdnError::AlreadyExists(message.to_string()),
            "offset_too_large" => NdnError::OffsetTooLarge(message.to_string()),
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
        let c = NdmClient::new(NdmClientConfig {
            base_url: "http://127.0.0.1:3180".to_string(),
        });
        assert_eq!(
            c.rpc_url("get_object"),
            "http://127.0.0.1:3180/ndm/proxy/v1/rpc/get_object"
        );
        assert_eq!(
            c.read_url("chunk/open"),
            "http://127.0.0.1:3180/ndm/proxy/v1/read/chunk/open"
        );
    }

    #[test]
    fn test_url_trailing_slash() {
        let c = NdmClient::new(NdmClientConfig {
            base_url: "http://127.0.0.1:3180/".to_string(),
        });
        assert_eq!(
            c.rpc_url("ping"),
            "http://127.0.0.1:3180/ndm/proxy/v1/rpc/ping"
        );
    }

    #[test]
    fn test_normalize_inner_path() {
        assert_eq!(normalize_inner_path(None), None);
        assert_eq!(normalize_inner_path(Some("".into())), None);
        assert_eq!(normalize_inner_path(Some("/".into())), None);
        assert_eq!(
            normalize_inner_path(Some("/a/b".into())),
            Some("/a/b".to_string())
        );
    }

    #[test]
    fn test_parse_object_state() {
        let v = serde_json::json!({ "state": "not_exist" });
        assert_eq!(parse_object_state(&v).unwrap(), ObjectState::NotExist);

        let v = serde_json::json!({ "state": "object", "obj_data": "hello" });
        assert_eq!(
            parse_object_state(&v).unwrap(),
            ObjectState::Object("hello".to_string())
        );
    }

    #[test]
    fn test_parse_chunk_store_state_basic() {
        let v = serde_json::json!({ "state": "completed", "chunk_size": 1024 });
        let (st, sz) = parse_chunk_store_state(&v).unwrap();
        assert_eq!(st, ChunkStoreState::Completed);
        assert_eq!(sz, 1024);

        let v = serde_json::json!({ "state": "not_exist", "chunk_size": 0 });
        let (st, sz) = parse_chunk_store_state(&v).unwrap();
        assert_eq!(st, ChunkStoreState::NotExist);
        assert_eq!(sz, 0);
    }

    #[test]
    fn test_map_http_error_json() {
        let body = r#"{"error":"not_found","message":"object missing"}"#;
        let err = map_http_error(StatusCode::NOT_FOUND, body);
        assert!(matches!(err, NdnError::NotFound(_)));
    }
}
