mod fb_service;
mod local_filebuffer;
mod buffer_db;

pub use fb_service::SessionId;
use ndn_lib::ChunkId;

pub struct FileBufferId {
    pub handle_id : String,
    pub base_chunk_list : Vec<ChunkId>,
    pub size:Option<u64>
}