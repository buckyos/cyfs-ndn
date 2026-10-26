//! Streams an object body into 32 MiB chunks of the named store while hashing
//! it (MD5 for the ETag, SHA-256 for `x-amz-content-sha256`). Each stored
//! chunk is held by a lease until a binding (or the multipart upload) retains it.

use bucket_store::content::chunk_id_of;
use bucket_store::{BucketError, BucketResult};
use md5::{Digest as _, Md5};
use named_store::{NamedDataMgr, PinScope};
use ndn_lib::{ChunkId, CHUNK_DEFAULT_SIZE};
use sha2::Sha256;
use std::time::Duration;

pub struct ObjectWriter {
    ndm: NamedDataMgr,
    lease_owner: String,
    lease: Duration,
    buf: Vec<u8>,
    pub chunks: Vec<ChunkId>,
    pub size: u64,
    md5: Md5,
    sha256: Sha256,
    limit: u64,
}

pub struct Written {
    pub chunks: Vec<ChunkId>,
    pub size: u64,
    pub md5: [u8; 16],
    pub sha256_hex: String,
}

impl ObjectWriter {
    pub fn new(ndm: NamedDataMgr, lease_owner: String, lease: Duration, limit: u64) -> Self {
        ObjectWriter {
            ndm,
            lease_owner,
            lease,
            buf: Vec::new(),
            chunks: Vec::new(),
            size: 0,
            md5: Md5::new(),
            sha256: Sha256::new(),
            limit,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> BucketResult<()> {
        self.size += data.len() as u64;
        if self.size > self.limit {
            return Err(BucketError::ObjectTooLarge { size: self.size, limit: self.limit });
        }
        self.md5.update(data);
        sha2::Digest::update(&mut self.sha256, data);
        self.buf.extend_from_slice(data);
        while self.buf.len() as u64 >= CHUNK_DEFAULT_SIZE {
            let piece: Vec<u8> = self.buf.drain(..CHUNK_DEFAULT_SIZE as usize).collect();
            self.store(piece).await?;
        }
        Ok(())
    }

    async fn store(&mut self, piece: Vec<u8>) -> BucketResult<()> {
        let (id, piece) = tokio::task::spawn_blocking(move || chunk_id_of(&piece).map(|id| (id, piece)))
            .await
            .map_err(|e| BucketError::Internal(format!("hash task: {e}")))??;
        if !self.ndm.have_chunk(&id).await {
            self.ndm.put_chunk(&id, &piece).await.map_err(|e| BucketError::Store(e.to_string()))?;
        }
        if let Err(e) = self.ndm.pin(&id.to_obj_id(), &self.lease_owner, PinScope::Lease, Some(self.lease)).await {
            log::warn!("lease of s3 chunk {} failed: {e}", id.to_string());
        }
        self.chunks.push(id);
        Ok(())
    }

    pub async fn finish(mut self) -> BucketResult<Written> {
        if !self.buf.is_empty() {
            let piece = std::mem::take(&mut self.buf);
            self.store(piece).await?;
        }
        Ok(Written {
            chunks: self.chunks,
            size: self.size,
            md5: self.md5.finalize().into(),
            sha256_hex: hex::encode(sha2::Digest::finalize(self.sha256)),
        })
    }
}
