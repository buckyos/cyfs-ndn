// simple chunk list的设计
// 通过simple chunk list,总是能一次获得所有的chunkid,并且chunkid的格式是mix256(支持变长)
// chunklist id 的设计和mix256一致，包含总大小（所有chunk大小之和）

use std::io::SeekFrom;

use std::{
    pin::Pin,
    task::{Context, Poll},
};
use pin_project::pin_project;
use futures::{future::BoxFuture, FutureExt};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::build_named_object_by_json;
use crate::ChunkId;
use crate::ChunkListId;
use crate::ChunkReader;
use crate::NamedDataMgrRef;
use crate::{NdnResult,NdnError};
use crate::ObjId;
use crate::OBJ_TYPE_CHUNK_LIST_SIMPLE;

pub struct SimpleChunkList {
    pub total_size:u64,
    pub body:Vec<ChunkId>,
}


impl SimpleChunkList {
    pub fn new() -> Self {
        Self { total_size:0 , body:Vec::new() }
    }

    pub fn from_chunk_list(chunk_list: Vec<ChunkId>) -> NdnResult<Self> {
        let mut total_size = 0;
        for chunk_id in chunk_list.iter() {
            let chunk_size = chunk_id.get_length();
            if chunk_size.is_none() {
                return Err(NdnError::InvalidParam("get chunk length from chunkid failed".to_string()));
            }
            total_size += chunk_size.unwrap();
        }
        Ok(Self { total_size, body:chunk_list })
    }

    pub fn append_chunk(&mut self,chunk_id:ChunkId) -> NdnResult<()> {
        let chunk_size = chunk_id.get_length();
        if chunk_size.is_none() {
            return Err(NdnError::InvalidParam("get chunk length from chunkid failed".to_string()));
        }

        self.body.push(chunk_id);
        self.total_size += chunk_size.unwrap();
        Ok(())
    }
    //TODO:这种特殊的obj-id可能会对obj-id的验证产生影响
    pub fn gen_obj_id(self) -> (ObjId, String) {
        let (obj_id, obj_str) = build_named_object_by_json(OBJ_TYPE_CHUNK_LIST_SIMPLE, &serde_json::to_value(self.body.clone()).unwrap());
        let chunk_list_id_raw =ChunkId::mix_length_and_hash_result(self.total_size, &obj_id.obj_hash);
        let result_id = ObjId::new_by_raw(OBJ_TYPE_CHUNK_LIST_SIMPLE.to_string(), chunk_list_id_raw);
        (result_id, obj_str)
    }

    pub fn from_json(obj_str: &str) -> NdnResult<Self> {
        let chunk_list:Vec<ChunkId> = serde_json::from_str(obj_str).map_err(|e| {
            NdnError::InvalidParam(format!("parse chunk list from json failed: {}", e.to_string()))
        })?;
        Self::from_chunk_list(chunk_list)
    }

    pub fn from_json_value(obj_value: serde_json::Value) -> NdnResult<Self> {
        let chunk_list:Vec<ChunkId> = serde_json::from_value(obj_value).map_err(|e| {
            NdnError::InvalidParam(format!("parse chunk list from json failed: {}", e.to_string()))
        })?;
        Self::from_chunk_list(chunk_list)
    }

    //return (chunk_index,chunk_offset)
    pub fn get_chunk_index_by_offset(&self, seek_from: SeekFrom) -> NdnResult<(usize, u64)> {
        unimplemented!()
    }
}

struct SimpleChunkInfo {
    chunk_id: ChunkId,
    offset: u64,
}

#[pin_project]
pub struct SimpleChunkListReader {
    named_data_mgr: NamedDataMgrRef,
    auto_cache: bool,

    #[pin]
    loading_future: Option<BoxFuture<'static, std::io::Result<ChunkReader>>>,
    current_reader: Option<ChunkReader>,
    first_chunk: Option<SimpleChunkInfo>,
    remaining_chunks: std::iter::Skip<std::vec::IntoIter<ChunkId>>,
}


impl SimpleChunkListReader {
    pub async fn new(
        named_data_mgr: NamedDataMgrRef,
        chunk_list: SimpleChunkList,
        seek_from: SeekFrom,
        auto_cache: bool,
    ) -> NdnResult<Self> {
        

        // Calculate the first chunk index and offset based on the seek_from position
        let (chunk_index, chunk_offset) = chunk_list.get_chunk_index_by_offset(seek_from)?;

        // Get the first chunk ID from the chunk list
        let first_chunk_id = chunk_list.body.get(chunk_index as usize);
        if first_chunk_id.is_none() {
            let msg = format!(
                "chunk index {} not found in chunk list ",
                chunk_index
            );
            warn!("{}", msg);
            return Err(NdnError::NotFound(msg));
        }

        let first_chunk_id = first_chunk_id.unwrap().clone();

        let remaining_chunks = chunk_list.body.into_iter().skip(chunk_index as usize + 1);

        Ok(Self {
            named_data_mgr,
            auto_cache,
            current_reader: None,
            loading_future: None,
            first_chunk: Some(SimpleChunkInfo {
                chunk_id: first_chunk_id.clone(),
                offset: chunk_offset,
            }),
            remaining_chunks: remaining_chunks,
        })
    }

    async fn load_chunk_reader(
        named_data_mgr: NamedDataMgrRef,
        chunk_id: ChunkId,
        offset: u64,
        auto_cache: bool,
    ) -> std::io::Result<ChunkReader> {
        let mut mgr = named_data_mgr.lock().await;
        let (reader, _) = mgr
            .open_chunk_reader_impl(&chunk_id, offset, auto_cache)
            .await
            .map_err(|e| {
                warn!(
                    "Failed to open chunk reader for {}: {}",
                    chunk_id.to_base32(),
                    e
                );
                std::io::Error::new(std::io::ErrorKind::Other, e)
            })?;
        Ok(reader)
    }
}

impl AsyncRead for SimpleChunkListReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.get_mut();

        loop {
            // First check if we have a current reader
            if let Some(reader) = this.current_reader.as_mut() {
                let current_len = buf.filled().len();
                match Pin::new(reader).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - current_len;
                        if bytes_read > 0 {
                            break Poll::Ready(Ok(()));
                        } else {
                            this.current_reader = None; // Clear current reader if no bytes read
                        }
                    }
                    Poll::Ready(Err(e)) => break Poll::Ready(Err(e)),
                    Poll::Pending => break Poll::Pending,
                }
            }

            // If no current reader, try to load next chunk reader
            if let Some(fut) = this.loading_future.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(new_reader)) => {
                        this.current_reader = Some(new_reader);
                        this.loading_future = None; // Clear the loading future
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                let (next_chunk, chunk_offset) = if let Some(info) = this.first_chunk.take() {
                    (Some(info.chunk_id), info.offset)
                } else {
                    (this.remaining_chunks.next(), 0)
                };
                
                if let Some(chunk_id) = next_chunk {
                    // Load the next chunk reader
                    this.loading_future = Some(
                        Self::load_chunk_reader(
                            this.named_data_mgr.clone(),
                            chunk_id, 
                            chunk_offset,
                            this.auto_cache,
                        ).boxed(),
                    );
                } else {
                    // No more chunks to read, return EOF
                    break Poll::Ready(Ok(()));
                }

            }
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn test_simple_chunk_list() {
        let mut simple_chunk_list = SimpleChunkList::new();
        simple_chunk_list.append_chunk(ChunkId::new("mix256:1234567890").unwrap()).unwrap();
        simple_chunk_list.append_chunk(ChunkId::new("mix256:1234567890").unwrap()).unwrap();
        let (obj_id, obj_str) = simple_chunk_list.gen_obj_id();
        println!("obj_str:{}",obj_str);
        println!("obj_id:{}",obj_id.to_string());
    }
}
