mod buffer_db;
mod fb_service;
mod local_filebuffer;

pub use fb_service::{FileBufferService, NdmPath, WriteLease};
pub use local_filebuffer::{
    FileBufferBaseReader, FileBufferDiffState, FileBufferRecord, LocalFileBufferService,
};
pub use ndm_lib::SessionId;
use ndn_lib::ChunkId;

pub struct FileBufferId {
    pub handle_id: String,
    pub base_chunk_list: Vec<ChunkId>,
    pub size: Option<u64>,
}
