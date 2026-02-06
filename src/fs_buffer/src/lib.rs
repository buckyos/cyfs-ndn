mod buffer_db;
mod fb_service;
mod local_filebuffer;

pub use fb_service::{
    FileBufferRead, FileBufferSeekReader, FileBufferSeekWriter, FileBufferService, FileBufferWrite,
    NdmPath, SessionId, WriteLease,
};
pub use local_filebuffer::{FileBufferRecord, LocalFileBufferService};
use ndn_lib::ChunkId;

pub struct FileBufferId {
    pub handle_id: String,
    pub base_chunk_list: Vec<ChunkId>,
    pub size: Option<u64>,
}
