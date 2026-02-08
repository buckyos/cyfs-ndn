use async_trait::async_trait;
use ndn_lib::{ChunkId, NdnResult, ObjId};
use serde::{Deserialize, Serialize};

use crate::local_filebuffer::FileBufferRecord;

#[derive(Debug, Clone)]
pub struct NdmPath(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

#[derive(Debug, Clone)]
pub struct WriteLease {
    pub session: SessionId,
    pub session_seq: u64,
    pub expires_at: u64,
}

#[async_trait]
pub trait FileBufferService: Send + Sync {
    // 这个函数通常由 fs-meta service 调用
    async fn alloc_buffer(
        &self,
        path: &NdmPath,
        file_inode_id: u64,
        base_chunk_list: Vec<ChunkId>,
        lease: &WriteLease,
        expected_size: Option<u64>,
    ) -> NdnResult<FileBufferRecord>;

    async fn get_buffer(&self, handle_id: &str) -> NdnResult<FileBufferRecord>;

    async fn flush(&self, fb: &FileBufferRecord) -> NdnResult<()>;

    async fn close(&self, fb: &FileBufferRecord) -> NdnResult<()>;

    async fn append(&self, fb: &FileBufferRecord, data: &[u8]) -> NdnResult<()>;

    // 计算文件 buffer 的 objid，让 inode 处于 linked 状态
    async fn cacl_name(&self, fb: &FileBufferRecord) -> NdnResult<ObjId>;

    async fn remove(&self, fb: &FileBufferRecord) -> NdnResult<()>;
}
