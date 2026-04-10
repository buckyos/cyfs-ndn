//! `HttpGcClient` — HTTP client for GC control-plane operations.
//!
//! Sends requests to the `/_gc/` endpoints on a remote `NamedStoreMgrHttpGateway`.

use crate::gc_types::{CascadeStateP0, EdgeMsg, ExpandDebug, PinRequest, PinScope};
use ndn_lib::{NdnError, NdnResult, ObjId};
use reqwest::{Client, StatusCode};
use std::time::Duration;

/// Configuration for `HttpGcClient`.
#[derive(Debug, Clone)]
pub struct HttpGcClientConfig {
    /// Base URL of the remote gateway, e.g. `http://127.0.0.1:3180/ndn`.
    /// GC endpoints are at `{base_url}/_gc/...`.
    pub base_url: String,
}

/// HTTP client for remote GC control-plane operations.
pub struct HttpGcClient {
    config: HttpGcClientConfig,
    client: Client,
}

impl HttpGcClient {
    pub fn new(config: HttpGcClientConfig) -> Self {
        let client = Client::new();
        Self { config, client }
    }

    pub fn with_client(config: HttpGcClientConfig, client: Client) -> Self {
        Self { config, client }
    }

    fn gc_url(&self, sub: &str) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!("{}/_gc/{}", base, sub)
    }

    // ======================== Edge ========================

    /// Deliver an edge message (add/remove) to the remote bucket.
    pub async fn apply_edge(&self, msg: &EdgeMsg) -> NdnResult<()> {
        let url = self.gc_url("edge");
        let resp = self
            .client
            .post(&url)
            .json(msg)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    // ======================== Pin ========================

    /// Pin an object on the remote bucket.
    pub async fn pin(
        &self,
        obj_id: &ObjId,
        owner: &str,
        scope: PinScope,
        ttl: Option<Duration>,
    ) -> NdnResult<()> {
        let url = self.gc_url("pin");
        let req = PinRequest {
            obj_id: obj_id.clone(),
            owner: owner.to_string(),
            scope,
            ttl_secs: ttl.map(|d| d.as_secs()),
        };
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    /// Unpin an object on the remote bucket.
    pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()> {
        let url = self.gc_url("unpin");
        let body = serde_json::json!({
            "obj_id": obj_id.to_string(),
            "owner": owner,
        });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    /// Unpin all objects owned by `owner`. Returns the count of removed pins.
    pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize> {
        let url = self.gc_url("unpin_owner");
        let body = serde_json::json!({ "owner": owner });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        Ok(v["count"].as_u64().unwrap_or(0) as usize)
    }

    // ======================== fs_acquire / fs_release ========================

    pub async fn fs_acquire(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        let url = self.gc_url("fs_acquire");
        let body = serde_json::json!({
            "obj_id": obj_id.to_string(),
            "inode_id": inode_id,
            "field_tag": field_tag,
        });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    pub async fn fs_release(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<()> {
        let url = self.gc_url("fs_release");
        let body = serde_json::json!({
            "obj_id": obj_id.to_string(),
            "inode_id": inode_id,
            "field_tag": field_tag,
        });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    /// Release all fs anchors for a given inode. Returns count of released anchors.
    pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize> {
        let url = self.gc_url("fs_release_inode");
        let body = serde_json::json!({ "inode_id": inode_id });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        Ok(v["count"].as_u64().unwrap_or(0) as usize)
    }

    // ======================== same_as ========================

    /// Register a SameAs relationship for a big chunk.
    pub async fn same_as(
        &self,
        big_chunk_id: &ObjId,
        chunk_list_id: &ObjId,
    ) -> NdnResult<()> {
        let url = self.gc_url("same_as");
        let body = serde_json::json!({
            "big_chunk_id": big_chunk_id.to_string(),
            "chunk_list_id": chunk_list_id.to_string(),
        });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        check_no_content(resp, &url).await
    }

    // ======================== forced_gc ========================

    /// Trigger forced GC to free at least target_bytes. Returns freed bytes.
    pub async fn forced_gc(&self, target_bytes: u64) -> NdnResult<u64> {
        let url = self.gc_url("forced_gc");
        let body = serde_json::json!({ "target_bytes": target_bytes });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("POST {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        Ok(v["freed_bytes"].as_u64().unwrap_or(0))
    }

    // ======================== Observation ========================

    /// Query total outbox count on the remote node.
    pub async fn outbox_count(&self) -> NdnResult<u64> {
        let url = self.gc_url("outbox_count");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        Ok(v["count"].as_u64().unwrap_or(0))
    }

    /// Debug: dump expand state for an object.
    pub async fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug> {
        let url = self.gc_url(&format!("expand_state/{}", obj_id));
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        resp.json::<ExpandDebug>()
            .await
            .map_err(|e| NdnError::IoError(format!("parse ExpandDebug: {e}")))
    }

    /// Query fs anchor state for (obj_id, inode_id, field_tag).
    pub async fn fs_anchor_state(
        &self,
        obj_id: &ObjId,
        inode_id: u64,
        field_tag: u32,
    ) -> NdnResult<CascadeStateP0> {
        let url = format!(
            "{}?inode_id={}&field_tag={}",
            self.gc_url(&format!("fs_anchor_state/{}", obj_id)),
            inode_id,
            field_tag
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        let state_str = v["state"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidData("missing state field".to_string()))?;
        CascadeStateP0::from_str(state_str)
            .ok_or_else(|| NdnError::InvalidData(format!("unknown cascade state: {state_str}")))
    }

    /// Query anchor state for (obj_id, owner).
    pub async fn anchor_state(
        &self,
        obj_id: &ObjId,
        owner: &str,
    ) -> NdnResult<CascadeStateP0> {
        let url = format!(
            "{}?owner={}",
            self.gc_url(&format!("anchor_state/{}", obj_id)),
            owner
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NdnError::RemoteError(format!("GET {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_gc_error(resp).await);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NdnError::IoError(format!("parse response: {e}")))?;
        let state_str = v["state"]
            .as_str()
            .ok_or_else(|| NdnError::InvalidData("missing state field".to_string()))?;
        CascadeStateP0::from_str(state_str)
            .ok_or_else(|| NdnError::InvalidData(format!("unknown cascade state: {state_str}")))
    }
}

// ======================== Helpers ========================

async fn check_no_content(resp: reqwest::Response, _url: &str) -> NdnResult<()> {
    let status = resp.status();
    if status == StatusCode::NO_CONTENT || status.is_success() {
        return Ok(());
    }
    Err(map_gc_error(resp).await)
}

async fn map_gc_error(resp: reqwest::Response) -> NdnError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    crate::http_backend::map_http_error_public(status, &body)
}
