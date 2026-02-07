use crate::{BaseContentObject, NamedObject, ObjId, OBJ_TYPE_FILE, OBJ_TYPE_PATH};
use buckyos_kit::buckyos_get_unix_timestamp;
use buckyos_kit::is_zero;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

//TODO：NDN如何提供一种通用机制，检查FileObject在本地是 完全存在的 ？ 在这里的逻辑是FileObject的Content(存在)
// 思路：Object如果引用了另一个Object,要区分这个引用是强引用(依赖）还是弱引用，
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FileObject {
    #[serde(flatten)]
    pub content_obj: BaseContentObject,
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub size: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub content: String, //chunkid or chunklistid
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    #[serde(flatten)]
    pub meta: HashMap<String, serde_json::Value>,
}

impl Default for FileObject {
    fn default() -> Self {
        Self {
            content_obj: BaseContentObject::default(),
            size: 0,
            content: String::new(),
            meta: HashMap::new(),
        }
    }
}

impl Deref for FileObject {
    type Target = BaseContentObject;
    fn deref(&self) -> &Self::Target {
        &self.content_obj
    }
}

impl DerefMut for FileObject {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.content_obj
    }
}

impl FileObject {
    //content can be chunkid or chunklistid
    pub fn new(name: String, size: u64, content: String) -> Self {
        Self {
            content_obj: BaseContentObject::new(name),
            size,
            content,
            meta: HashMap::new(),
        }
    }
}

impl NamedObject for FileObject {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_FILE
    }
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PathObject {
    pub path: String,
    pub uptime: u64,
    pub target: ObjId,
    pub exp: u64,
}

impl PathObject {
    pub fn new(path: String, target: ObjId) -> Self {
        Self {
            path,
            uptime: buckyos_get_unix_timestamp(),
            target,
            exp: buckyos_get_unix_timestamp() + 3600 * 24 * 365 * 3,
        }
    }
}

impl NamedObject for PathObject {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_PATH
    }
}

#[cfg(test)]
mod tests {
    use crate::build_named_object_by_json;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_file_object() {
        let file_object = FileObject::new(
            "test.data".to_string(),
            100,
            "sha256:1234567890".to_string(),
        );
        let file_object_str = serde_json::to_string(&file_object).unwrap();
        println!("file_object_str {}", file_object_str);

        let (objid, obj_str) = file_object.gen_obj_id();
        println!("fileobj id {}", objid.to_string());
        println!("fileobj str {}", obj_str);
    }

    #[test]
    fn test_file_object_with_custom_meta() {
        let mut file_object = FileObject::new(
            "test-with-meta.data".to_string(),
            2048,
            "sha256:1234567890ABCDEF".to_string(),
        );
        file_object
            .meta
            .insert("app_version".to_string(), json!("1.2.3"));
        file_object.meta.insert("priority".to_string(), json!(7));

        let file_object_str = serde_json::to_string(&file_object).unwrap();
        let file_object_json: serde_json::Value = serde_json::from_str(&file_object_str).unwrap();
        assert_eq!(file_object_json["app_version"], json!("1.2.3"));
        assert_eq!(file_object_json["priority"], json!(7));

        let file_object2: FileObject = serde_json::from_str(&file_object_str).unwrap();
        assert_eq!(file_object2.meta.get("app_version"), Some(&json!("1.2.3")));
        assert_eq!(file_object2.meta.get("priority"), Some(&json!(7)));

        let (obj_id, _obj_str) = file_object.gen_obj_id();
        let (obj_id2, _obj_str2) =
            build_named_object_by_json(OBJ_TYPE_FILE, &serde_json::to_value(&file_object).unwrap());
        assert_eq!(obj_id, obj_id2);
    }

    #[test]
    fn test_path_object() {
        let path_object = PathObject::new(
            "/repo/pub_meta_index.db".to_string(),
            ObjId::new("sha256:1234567890").unwrap(),
        );
        let path_object_str = serde_json::to_string(&path_object).unwrap();
        println!("path_object_str {}", path_object_str);

        let (objid, obj_str) = path_object.gen_obj_id();
        println!("pathobj id {}", objid.to_string());
        println!("pathobj str {}", obj_str);
    }
}
