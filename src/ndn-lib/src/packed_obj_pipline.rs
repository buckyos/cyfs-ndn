use async_trait::async_trait;
use crate::object::ObjId;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::chunk::ChunkId;

pub struct PackedObjItem {
    pub obj_id: ObjId,
    pub sub_item_count: usize,//后面的sub item数量
}

#[async_trait]
pub trait PackedObjPiplineWriter: Send + Sync {
    async fn push(&self, item: PackedObjItem);
}

#[async_trait]
pub trait PackedObjPiplineReader: Send + Sync {
    async fn pop(&self) -> Option<PackedObjItem>;
}


#[async_trait]
pub trait ChunkChannelSourceWriter: Send + Sync {
    async fn put_chunk(&self, chunk_id: ChunkId, chunk_size: u64);
}

#[async_trait]
pub trait ChunkChannelSourceReader: Send + Sync {
    async fn get_chunk_reader(&self, chunk_id: ChunkId) -> Option<Vec<u8>>;
    async fn complete_chunk(&self, chunk_id: ChunkId);
}



pub struct SimplePackedObjPipline {
    pub max_item_num: usize,
}

#[async_trait]
impl PackedObjPiplineWriter for SimplePackedObjPipline {
    async fn push(&self, item: PackedObjItem) {
        unimplemented!()
    }
}

#[async_trait]
impl PackedObjPiplineReader for SimplePackedObjPipline {
    async fn pop(&self) -> Option<PackedObjItem> {
        unimplemented!()
    }
}

