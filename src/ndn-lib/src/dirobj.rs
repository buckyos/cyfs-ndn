use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::object::ObjId;
use crate::{BaseContentObject, FileObject, NdnError, NdnResult, OBJ_TYPE_DIR, OBJ_TYPE_FILE};
use crate::object_map::{SimpleObjectMap, SimpleMapItem};
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirObject {

    #[serde(flatten)]
    pub content_obj:BaseContentObject,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    #[serde(flatten)]
    pub meta:HashMap<String,serde_json::Value>,
   
    pub total_size: u64,//包含所有子文件夹和当前文件夹下文件的总大小
    pub file_count: u64,
    pub file_size: u64,//不包含子文件，只计算当前文件夹下文件的总大小

    #[serde(flatten)]
    pub object_map: SimpleObjectMap, // 保存真正的sub items
}


impl Deref for DirObject {
    type Target = BaseContentObject;
    fn deref(&self) -> &Self::Target {
        &self.content_obj
    }
}

impl DerefMut for DirObject {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.content_obj
    }
}


impl DirObject {
    pub fn new(name: Option<String>) -> Self {
        let content_obj = match name {
            Some(name) => BaseContentObject::new(name),
            None => BaseContentObject::default(),
        };
        Self {
            content_obj,
            meta: HashMap::new(),
            total_size: 0,
            file_count: 0,
            file_size: 0,
            object_map: SimpleObjectMap::new(),
        }
    }

    //gen_obj_id会消耗self,防止构造id后潜在的修改
    pub fn gen_obj_id(&self) -> NdnResult<(ObjId, String)> {
        // 性能优化：避免先序列化整个 DirObject（会包含很大的 body），再 remove("body")。
        // 这里仅序列化 content_obj（flatten 的基础字段）并补齐目录统计字段，然后让
        // SimpleObjectMap::gen_obj_id_with_real_obj 负责把 body(子项) 转成 ObjId 映射并写回。
        let mut this_obj = serde_json::to_value(&self.content_obj)
            .map_err(|e| NdnError::InvalidData(format!("serialize BaseContentObject failed: {}", e)))?;
        let obj = this_obj
            .as_object_mut()
            .ok_or_else(|| NdnError::InvalidData("BaseContentObject must serialize to JSON object".to_string()))?;
        obj.insert(
            "total_size".to_string(),
            Value::Number(serde_json::Number::from(self.total_size)),
        );
        obj.insert(
            "file_count".to_string(),
            Value::Number(serde_json::Number::from(self.file_count)),
        );
        obj.insert(
            "file_size".to_string(),
            Value::Number(serde_json::Number::from(self.file_size)),
        );

        self.object_map.gen_obj_id_with_real_obj(OBJ_TYPE_DIR, &mut this_obj)
    }

    // 委托方法到 SimpleObjectMap
    pub fn len(&self) -> usize {
        self.object_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.object_map.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&SimpleMapItem> {
        self.object_map.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<SimpleMapItem> {
        self.object_map.remove(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.object_map.contains_key(key)
    }

    pub fn keys(&self) -> std::collections::hash_map::Keys<'_,String, super::SimpleMapItem> {
        self.object_map.keys()
    }

    pub fn values(&self) -> std::collections::hash_map::Values<'_,String, super::SimpleMapItem> {
        self.object_map.values()
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<'_,String, SimpleMapItem> {
        self.object_map.iter()
    }

    // 目录特有的方法
    pub fn add_file(&mut self, name: String, file_obj: Value,file_size: u64) -> NdnResult<()>{
        self.file_size += file_size;
        self.file_count += 1;
        self.total_size += file_size;
        self.object_map.insert(name, SimpleMapItem::Object(OBJ_TYPE_FILE.to_string(), file_obj));
        Ok(())
    }

    pub fn add_directory(&mut self, name: String, dir_obj_id: ObjId,dir_size: u64) -> NdnResult<()> {
        if dir_obj_id.obj_type != OBJ_TYPE_DIR {
            warn!("add_directory: dir_obj_id is not a directory");
            return Err(NdnError::InvalidParam("dir_obj_id is not a directory".to_string()));
        }

        self.total_size += dir_size;
        self.object_map.insert(name, SimpleMapItem::ObjId(dir_obj_id));
        Ok(())
    }

    pub fn list_entries(&self) -> Vec<String> {
        self.object_map.keys().cloned().collect()
    }

    pub fn is_file(&self, name: &str) -> bool {
        let item = self.object_map.get(name);
        if item.is_some() {
            let item = item.unwrap();
            match item {
                SimpleMapItem::Object(obj_type, _) => obj_type == OBJ_TYPE_FILE,
                _ => false,
            }
        } else {
            false
        }
    }

    pub fn is_directory(&self, name: &str) -> bool {
        let item = self.object_map.get(name);
        if item.is_some() {
            let item = item.unwrap();
            match item {
                SimpleMapItem::ObjId(obj_id) => obj_id.obj_type == OBJ_TYPE_DIR,
                SimpleMapItem::ObjectJwt(obj_type, _) => obj_type == OBJ_TYPE_DIR,
                _ => false,
            }
        } else {
            false
        }
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjId;

    #[test]
    fn test_dir_object_simple_test() {
        let mut dir_root = DirObject::new(Some("root".to_string()));
        let file1 = FileObject::new("file1".to_string(), 1024, "sha256:1234567890".to_string());
        let file2 = FileObject::new("file2".to_string(), 1024, "sha256:1234567890AB".to_string());
        let file3 = FileObject::new("file3".to_string(), 1024, "sha256:1234567890ABCD".to_string());

        let file1_obj = serde_json::to_value(file1).unwrap();
        let file2_obj = serde_json::to_value(file2).unwrap();
        let file3_obj = serde_json::to_value(file3).unwrap();
        dir_root.add_file("file1".to_string(), file1_obj, 1024).unwrap();
        dir_root.add_file("file2".to_string(), file2_obj, 1024).unwrap();
        dir_root.add_file("file3".to_string(), file3_obj, 1024).unwrap();

        let mut dir_sub = DirObject::new(None);
        let file5 = FileObject::new("file5".to_string(), 2048, "sha256:1234567890ABCD".to_string());
        let file5_obj = serde_json::to_value(file5).unwrap();
        dir_sub.add_file("file5".to_string(), file5_obj, 1024).unwrap();

        let file4 = FileObject::new("file4".to_string(), 2048, "sha256:1234567890ABCD".to_string());
        let file4_obj = serde_json::to_value(file4).unwrap();
        dir_sub.add_file("file4".to_string(), file4_obj, 1024).unwrap();

        let sub_total_size = dir_sub.total_size;
        let (sub_obj_id, json_str) = dir_sub.gen_obj_id().unwrap();
        println!("sub_dir_id: {}", sub_obj_id.to_string());
        assert_eq!(sub_obj_id.obj_type, OBJ_TYPE_DIR);

        dir_root.add_directory("sub".to_string(), sub_obj_id, sub_total_size).unwrap();

        let json_str = serde_json::to_string_pretty(&dir_root).unwrap();
        println!("json_str_for_dir_root: {}", json_str);

        let (obj_id, json_str) = dir_root.gen_obj_id().unwrap();
        println!("dir_root_obj_id: {}", obj_id.to_string());
        println!("json_str_for_dir_root_gen_obj_id: {}", json_str);
        assert_eq!(obj_id.obj_type, OBJ_TYPE_DIR);
    }

}
