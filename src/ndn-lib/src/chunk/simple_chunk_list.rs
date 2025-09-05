// simple chunk list的设计
// 通过simple chunk list,总是能一次获得所有的chunkid,并且chunkid的格式是mix256(支持变长)
// chunklist id 的设计和mix256一致，包含总大小（所有chunk大小之和）
// 总是通过chunk listid来引用chunk？
// 为了简化传输，可以使用embed的方式，在Fileobject中嵌入chunklist的全部内容？


use crate::build_named_object_by_json;
use crate::ChunkId;
use crate::ChunkListId;
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

    pub fn gen_obj_id(self) -> (ObjId, String) {
        let (obj_id, obj_str) = build_named_object_by_json(OBJ_TYPE_CHUNK_LIST_SIMPLE, &serde_json::to_value(self.body.clone()).unwrap());
        let chunk_list_id_raw =ChunkId::mix_length_and_hash_result(self.total_size, &obj_id.obj_hash);
        let result_id = ObjId::new_by_raw(OBJ_TYPE_CHUNK_LIST_SIMPLE.to_string(), chunk_list_id_raw);
        (result_id, obj_str)
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
