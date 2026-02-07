use ndn_lib::{FileObject, NamedObject, OBJ_TYPE_PKG};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use crate::{PackageId, PkgError, PkgResult};
use name_lib::{DID, EncodedDocument};

fn is_zero(value:&u64)->bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PackageMeta {
    #[serde(flatten)]
    pub _base : FileObject,

    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub version_tag: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    //key = pkg_name,value = version_req_str,like ">1.0.0-alpha"
    pub deps: HashMap<String, String>,     

}

impl Deref for PackageMeta {
    type Target = FileObject;
    fn deref(&self) -> &Self::Target {
        &self._base
    }
}

impl DerefMut for PackageMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self._base
    }
}

impl PackageMeta {

    pub fn new(pkg_name: &str, version: &str, author: &str, owner: &DID, tag: Option<&str>) -> Self {
        let now = buckyos_kit::buckyos_get_unix_timestamp();
        let exp = now + 3600 * 24 * 30;
        let mut base = FileObject::new(pkg_name.to_string(), 0, String::new());
        base.author = author.to_string();
        base.owner = owner.clone();
        base.create_time = now;
        base.last_update_time = now;
        base.exp = exp;

        Self {
            _base: base,
            version: version.to_string(),
            version_tag: tag.map(|s| s.to_string()),
            deps: HashMap::new(),
        }
    }
    pub fn from_str(meta_str: &str) -> PkgResult<Self> {
        let pkg_meta_doc = EncodedDocument::from_str(meta_str.to_string())
            .map_err(|e| PkgError::ParseError(meta_str.to_string(), e.to_string()))?;

        let pkg_json = pkg_meta_doc.to_json_value()
            .map_err(|e| PkgError::ParseError(meta_str.to_string(), e.to_string()))?;

        let meta: PackageMeta = serde_json::from_value(pkg_json)
            .map_err(|e| PkgError::ParseError(meta_str.to_string(), e.to_string()))?;
        Ok(meta)
    }

    pub fn get_package_id(&self) -> PackageId {
        if self.version_tag.is_some() {
            let package_id_str = format!(
                "{}#{}:{}",
                self._base.content_obj.name,
                self.version,
                self.version_tag.as_ref().unwrap()
            );
            PackageId::parse(&package_id_str).unwrap()
        } else {
            let package_id_str = format!("{}#{}", self._base.content_obj.name, self.version);
            PackageId::parse(&package_id_str).unwrap()
        }
    }

}

impl NamedObject for PackageMeta {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_PKG
    }
}

pub struct PackageMetaNode {
    pub meta_jwt:String,
    pub pkg_name:String,
    pub version:String,
    pub tag:Option<String>,
    pub author:String,
    pub author_pk:String,
}


mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct MyPackageMeta {
        #[serde(flatten)]
        pub _base : PackageMeta,
        pub sub_deps: HashMap<String, String>,
    }

    impl Deref for MyPackageMeta {
        type Target = PackageMeta;
        fn deref(&self) -> &Self::Target {
            &self._base
        }
    }
    impl DerefMut for MyPackageMeta {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self._base
        }
    }


    #[test]
    fn test_package_meta_serde_and_fileobj_compat() {
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let mut meta = PackageMeta::new("test", "1.0.0", "test", &owner, Some("test"));
        meta._base.size = 123;
        meta._base.content = "mix256:deadbeef".to_string();
        meta.deps.insert("dep1".to_string(), ">=1.0.0".to_string());

        // PackageMeta -> JSON -> PackageMeta
        let json_str = serde_json::to_string_pretty(&meta).unwrap();
        println!("json_str: {}", json_str);
        let meta2: PackageMeta = serde_json::from_str(&json_str).unwrap();
        assert_eq!(meta2._base.content_obj.name, "test");
        assert_eq!(meta2.version, "1.0.0");
        assert_eq!(meta2.version_tag.as_deref(), Some("test"));
        assert_eq!(meta2._base.size, 123);
        assert_eq!(meta2._base.content, "mix256:deadbeef");
        assert_eq!(meta2.deps.get("dep1").map(|s| s.as_str()), Some(">=1.0.0"));


        // 同一份 JSON 也应当能反序列化成 FileObject（PackageMeta 的额外字段会被忽略）
        let file_obj: FileObject = serde_json::from_str(&json_str).unwrap();
        assert_eq!(file_obj.content_obj.name, "test");
        assert_eq!(file_obj.size, 123);
        assert_eq!(file_obj.content, "mix256:deadbeef");

        let file_obj_str = serde_json::to_string_pretty(&file_obj).unwrap();
        println!("file_obj_str: {}", file_obj_str);

        let meta3 = PackageMeta::from_str(&json_str).unwrap();
        assert_eq!(meta3, meta2);

        let meta4_str = r#"
{
  "name": "test",
  "author": "test",
  "owner": "did:bns:buckyos.ai",
  "create_time": 1767754917,
  "last_update_time": 1767754917,
  "exp": 1770346917,
  "size": 123,
  "content": "mix256:deadbeef",
  "deps": {
    "dep1": ">=1.0.0"
  },
  "sub_deps": {
    "dep2": ">=2.0.0"
  },
  "version_tag": "test",
  "version": "1.0.0"
}
        "#;
        let meta4: PackageMeta = serde_json::from_str(meta4_str).unwrap();

        let dep2 = meta4
            .meta
            .get("sub_deps")
            .and_then(|v| v.as_object())
            .and_then(|m| m.get("dep2"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "meta.sub_deps.dep2 missing or not a string").unwrap();
        assert_eq!(dep2, ">=2.0.0");
    
        let meta4_str = serde_json::to_string_pretty(&meta4).unwrap();
        //println!("meta4_str: {}", meta4_str);
        
        let file_obj4: FileObject = serde_json::from_str(meta4_str.as_str()).unwrap();
        let dep2 = file_obj4.meta.get("sub_deps").and_then(|v| v.as_object()).and_then(|m| m.get("dep2")).and_then(|v| v.as_str()).ok_or_else(|| "meta.sub_deps.dep2 missing or not a string").unwrap();
        assert_eq!(dep2, ">=2.0.0");
        let file_obj4_str = serde_json::to_string_pretty(&file_obj4).unwrap();
        println!("file_obj4_str: {}", file_obj4_str);

        let my_meta4: MyPackageMeta = serde_json::from_str(meta4_str.as_str()).unwrap();
        assert_eq!(my_meta4.sub_deps.get("dep2").unwrap(), ">=2.0.0");
        let my_meta4_str = serde_json::to_string_pretty(&my_meta4).unwrap();
        println!("my_meta4_str: {}", my_meta4_str);
        let my_meta4_file_obj: FileObject = serde_json::from_str(my_meta4_str.as_str()).unwrap();
        let dep2 = my_meta4_file_obj.meta.get("sub_deps").and_then(|v| v.as_object()).and_then(|m| m.get("dep2")).and_then(|v| v.as_str()).ok_or_else(|| "meta.sub_deps.dep2 missing or not a string").unwrap();
        assert_eq!(dep2, ">=2.0.0");
        let my_meta4_file_obj_str = serde_json::to_string_pretty(&my_meta4_file_obj).unwrap();
        println!("my_meta4_file_obj_str: {}", my_meta4_file_obj_str);
        let my_meta4_meta: PackageMeta = serde_json::from_str(my_meta4_str.as_str()).unwrap();
        assert_eq!(my_meta4_meta, meta4);

        //assert_eq!(file_obj_str, json_str);
    }
}
