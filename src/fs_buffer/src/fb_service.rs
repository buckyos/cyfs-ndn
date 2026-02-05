use std::io::SeekFrom;
use std::pin::Pin;

use async_trait::async_trait;
use ndn_lib::{ChunkId, NdnResult, ObjId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::local_filebuffer::FileBufferHandle;

pub trait FileBufferRead: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek + ?Sized> FileBufferRead for T {}

pub trait FileBufferWrite: AsyncWrite + AsyncSeek {}
impl<T: AsyncWrite + AsyncSeek + ?Sized> FileBufferWrite for T {}

pub type FileBufferSeekReader = Pin<Box<dyn FileBufferRead + Unpin + Send>>;
pub type FileBufferSeekWriter = Pin<Box<dyn FileBufferWrite + Unpin + Send>>;

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
    //这个函数通常由fs-meta service调用
    async fn alloc_buffer(&self, path: &NdmPath, lease: &WriteLease, expected_size: Option<u64>) -> NdnResult<FileBufferHandle>;
    
    async fn open_reader(&self, fb: &FileBufferHandle, seek_from: SeekFrom) -> NdnResult<FileBufferSeekReader>;
    async fn open_writer(&self, fb: &FileBufferHandle, seek_from: SeekFrom) -> NdnResult<FileBufferSeekWriter>;
    async fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()>;
    async fn close(&self, fb: &FileBufferHandle) -> NdnResult<()>;
    async fn append(&self, fb: &FileBufferHandle, data: &[u8]) -> NdnResult<()>;
    // 计算文件buffer的objid，让inode处于linked状态
    async fn cacl_name(&self, fb: &FileBufferHandle) -> NdnResult<ObjId>;

    /// Finalize：把数据从 buffer node 推到 NamedStore internal（IO 密集型）
    //async fn move_to_store(&self, fb: &FileBufferHandle, store: &dyn NamedStore) -> NdnResult<()>;
    async fn remove(&self, fb: &FileBufferHandle) -> NdnResult<()>;
}
