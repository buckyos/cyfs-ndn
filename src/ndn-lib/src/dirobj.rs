use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::object::ObjId;
use crate::{build_named_object_by_json, FileObject, NdnError, NdnResult, OBJ_TYPE_DIR, OBJ_TYPE_FILE};
use crate::object_map::{SimpleObjectMap, SimpleMapItem};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub total_size: u64,//包含所有子文件夹
    pub file_count: u64,
    pub file_size: u64,//不包含子文件内文件
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,


    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub exp: u64,
    #[serde(flatten)]
    pub extra_info: HashMap<String, Value>,
    #[serde(flatten)]
    pub object_map: SimpleObjectMap,
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
                                                               t == &T::default()
}

impl DirObject {
    pub fn new(name: Option<String>) -> Self {
        
        Self {
            name,
            total_size: 0,
            file_count: 0,
            file_size: 0,
            owner: None,
            exp: 0,
            extra_info: HashMap::new(),
            object_map: SimpleObjectMap::new(),
        }
    }

    //gen_obj_id会消耗self,防止构造id后潜在的修改
    pub fn gen_obj_id(self) -> NdnResult<(ObjId, String)> {
        let mut real_extra_info:HashMap<String, Value> = HashMap::new(); 
        for (key, value) in &self.extra_info {
            real_extra_info.insert(key.clone(), value.clone());
        }
        if self.name.is_some() {
            real_extra_info.insert("name".to_string(), Value::String(self.name.clone().unwrap()));
        }
        real_extra_info.insert("total_size".to_string(), Value::Number(self.total_size.into()));
        real_extra_info.insert("file_count".to_string(), Value::Number(self.file_count.into()));
        real_extra_info.insert("file_size".to_string(), Value::Number(self.file_size.into()));
        if self.owner.is_some() {
            real_extra_info.insert("owner".to_string(), Value::String(self.owner.clone().unwrap()));
        }

        if self.exp > 0 {
            real_extra_info.insert("exp".to_string(), Value::Number(self.exp.into()));
        }

        SimpleObjectMap::gen_obj_id_with_extra_info(OBJ_TYPE_DIR,&self.object_map.body, &real_extra_info)
    }

    // 委托方法到 SimpleObjectMap
    pub fn len(&self) -> usize {
        self.object_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.object_map.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&super::SimpleMapItem> {
        self.object_map.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<super::SimpleMapItem> {
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

    pub fn iter(&self) -> std::collections::hash_map::Iter<'_,String, super::SimpleMapItem> {
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
        self.object_map.insert(name, super::SimpleMapItem::ObjId(dir_obj_id));
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
        
        let file6 = FileObject::new("file6".to_string(), 2048, "sha256:1234567890ABCD".to_string());
        let sub_total_size = dir_sub.total_size;
        let (sub_obj_id, json_str) = dir_sub.gen_obj_id().unwrap();
        println!("sub_dir_id: {}", sub_obj_id.to_string());
        assert!(sub_obj_id.to_string() == "cydir:71ebdfa188af679d3853288e750d9995151f5850b8c9d7dfde3d1a8ed8a58ae7");

        dir_root.add_directory("sub".to_string(), sub_obj_id, sub_total_size).unwrap();

        let json_str = serde_json::to_string(&dir_root).unwrap();
        println!("json_str_for_dir_root: {}", json_str);

        dir_root.extra_info.insert("test".to_string(), Value::String("test extra info \nsecond line".to_string()));

        let (obj_id, json_str) = dir_root.gen_obj_id().unwrap();
        println!("root_dir_id: {}", obj_id.to_string());
        println!("json_str_for_root_gen_obj_id: {}", json_str);
        assert!(obj_id.to_string() == "cydir:118fc282dfb2fc0181e670020cf57730c6aab56b7b5a8b30d858ce0a76690e1c");

    }

}
