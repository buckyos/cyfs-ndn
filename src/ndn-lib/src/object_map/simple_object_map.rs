use name_lib::{decode_json_from_jwt_with_pk, decode_jwt_claim_without_verify};
use serde_json::Value;
use crate::{build_named_object_by_json, build_named_object_by_jwt, object::ObjId, NdnError, NdnResult, OBJ_TYPE_OBJMAP_SIMPLE};
use std::collections::HashMap;
use serde::{Deserialize, Serialize, Serializer, Deserializer};

/*注意,SimpleObjectMap是一个"虚“对象，不应该直接使用。而是被嵌入到一个ReallyObject中使用
struct ReallyObject {
    pub other_filed:Value,
    ...
    #[serde(flatten)]
    pub children: SimpleObjectMap,
}

*/

#[derive(Debug, Clone)]
pub enum SimpleMapItem {
    //obj type,obj value
    Object(String,Value),
    ObjId(ObjId),
    //obj type,obj value jwt
    ObjectJwt(String,String),
}

impl SimpleMapItem {
    pub fn get_obj_type(&self) -> String {
        match self {
            SimpleMapItem::Object(obj_type, _) => obj_type.clone(),
            SimpleMapItem::ObjId(obj_id) => obj_id.obj_type.clone(),
            SimpleMapItem::ObjectJwt(obj_type, _) => obj_type.clone(),
        }
    }

    pub fn get_obj(&self) -> NdnResult<Value> {
        match self {
            SimpleMapItem::Object(_, obj_value) => Ok(obj_value.clone()),
            SimpleMapItem::ObjId(_) => Err(NdnError::InvalidParam("ObjId is not a object".to_string())),
            SimpleMapItem::ObjectJwt(_, obj_value) => {
                let result = decode_jwt_claim_without_verify(obj_value.as_str())
                    .map_err(|e| NdnError::InvalidParam(format!("decode jwt failed:{}", e.to_string())))?;
                return Ok(result);
            }
        }
    }

    pub fn get_obj_id(&self) -> NdnResult<(ObjId,String)> {
        match self {
            SimpleMapItem::ObjId(obj_id) => Ok((obj_id.clone(),"".to_string())),
            SimpleMapItem::Object(obj_type, obj_value) => {
                let (obj_id, obj_str) = build_named_object_by_json(obj_type, obj_value);
                Ok((obj_id, obj_str))
            },
            SimpleMapItem::ObjectJwt(obj_type, obj_value) => {
                let (obj_id, obj_str) = build_named_object_by_jwt(obj_type, obj_value)?;
                Ok((obj_id, obj_str))
            }
        }
    }
}

impl Serialize for SimpleMapItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            SimpleMapItem::ObjId(obj_id) => {
                // 直接序列化为字符串
                serializer.serialize_str(&obj_id.to_string())
            }
            SimpleMapItem::Object(obj_type, obj_value) => {
                // 序列化为包含 obj_type 和 body 的对象
                let mut map = serde_json::Map::new();
                map.insert("obj_type".to_string(), serde_json::Value::String(obj_type.clone()));
                map.insert("body".to_string(), obj_value.clone());
                serde_json::Value::Object(map).serialize(serializer)
            }
            SimpleMapItem::ObjectJwt(obj_type, jwt) => {
                // 序列化为包含 obj_type 和 jwt 的对象
                let mut map = serde_json::Map::new();
                map.insert("obj_type".to_string(), serde_json::Value::String(obj_type.clone()));
                map.insert("jwt".to_string(), serde_json::Value::String(jwt.clone()));
                serde_json::Value::Object(map).serialize(serializer)
            }
        }
    }
}

impl<'a> Deserialize<'a> for SimpleMapItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        
        match value {
            serde_json::Value::String(s) => {
                // 如果是字符串，尝试解析为 ObjId
                match ObjId::new(&s) {
                    Ok(obj_id) => Ok(SimpleMapItem::ObjId(obj_id)),
                    Err(_) => Err(serde::de::Error::custom("Invalid ObjId format")),
                }
            }
            serde_json::Value::Object(mut map) => {
                if let Some(obj_type) = map.remove("obj_type") {
                    if let serde_json::Value::String(obj_type_str) = obj_type {
                        if let Some(jwt) = map.remove("jwt") {
                            if let serde_json::Value::String(jwt_str) = jwt {
                                // 包含 jwt 字段，解析为 ObjectJwt
                                Ok(SimpleMapItem::ObjectJwt(obj_type_str, jwt_str))
                            } else {
                                Err(serde::de::Error::custom("jwt field must be a string"))
                            }
                        } else if let Some(body) = map.remove("body") {
                            // 包含 body 字段，解析为 Object
                            Ok(SimpleMapItem::Object(obj_type_str, body))
                        } else {
                            Err(serde::de::Error::custom("Object must have either body or jwt field"))
                        }
                    } else {
                        Err(serde::de::Error::custom("obj_type field must be a string"))
                    }
                } else {
                    Err(serde::de::Error::custom("Object must have obj_type field"))
                }
            }
            _ => Err(serde::de::Error::custom("Unknown object item type (must be string or object)")),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimpleObjectMap {
    // #[serde(flatten)]
    // pub extra_info: HashMap<String, Value>,
    pub body: HashMap<String, SimpleMapItem>,
}

impl SimpleObjectMap {
    pub fn new() -> Self {
        Self {
            //extra_info: HashMap::new(),
            body: HashMap::new(),
        }
    }

    pub fn gen_obj_id_with_real_obj(&self,result_obj_type:&str, real_obj:&mut Value) -> NdnResult<(ObjId, String)> {
        let mut real_map = HashMap::new();
        for (key, value) in self.body.iter() {
            match value {
                SimpleMapItem::Object(obj_type,obj_value) => {
                    let (sub_obj_id, _json_str) = build_named_object_by_json(obj_type, obj_value);
                    real_map.insert(key.clone(), sub_obj_id.to_string());
                }
                SimpleMapItem::ObjId(v) => {
                    real_map.insert(key.clone(), v.to_string());
                }
                SimpleMapItem::ObjectJwt(obj_type,obj_jwt) => {
                    let (sub_obj_id, _json_str) = build_named_object_by_jwt(obj_type, obj_jwt)?;
                    real_map.insert(key.clone(), sub_obj_id.to_string());
                }
            }
        }
        
        let body = serde_json::to_value(real_map).expect("Failed to serialize SimpleObjectMap");
        real_obj.as_object_mut().unwrap().insert("body".to_string(), body);
        let real_obj = serde_json::to_value(real_obj).expect("Failed to serialize SimpleObjectMap");
        let (id, json_str) = build_named_object_by_json(result_obj_type, &real_obj);
        Ok((id, json_str))
    }
    
    //gen_obj_id会消耗self,防止构造id后潜在的修改
    // pub fn gen_obj_id(self) -> NdnResult<(ObjId, String)> {
    //     Self::gen_obj_id_with_extra_info(OBJ_TYPE_OBJMAP_SIMPLE, &self.body, &self.extra_info)
    // }

    pub fn len(&self) -> usize {
        self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&SimpleMapItem> {
        self.body.get(key)
    }

    pub fn insert(&mut self, key: String, value: SimpleMapItem) -> Option<SimpleMapItem> {
        let old_value = self.body.insert(key.clone(), value);
        old_value
    }

    pub fn remove(&mut self, key: &str) -> Option<SimpleMapItem> {
        let removed = self.body.remove(key);
        removed
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.body.contains_key(key)
    }

    pub fn keys(&self) -> std::collections::hash_map::Keys<'_, String, SimpleMapItem> {
        self.body.keys()
    }

    pub fn values(&self) -> std::collections::hash_map::Values<'_, String, SimpleMapItem> {
        self.body.values()
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<'_, String, SimpleMapItem> {
        self.body.iter()
    }
}

impl Default for SimpleObjectMap {
    fn default() -> Self {
        Self::new()
    }
}


mod test {
    use jsonwebtoken::EncodingKey;
    use serde_json::json;
    use crate::*;
    use super::*;

    #[test]
    fn test_simple_object_map() {
        let file1 = FileObject::new("file1".to_string(), 1024, "sha256:1234567890".to_string());
        let file2 = FileObject::new("file2".to_string(), 1024, "sha256:1234567890AB".to_string());
        let file3 = FileObject::new("file3".to_string(), 1024, "sha256:1234567890ABCD".to_string());
        let file1_obj_id = file1.gen_obj_id().0.to_string();
        let file2_obj = serde_json::to_value(file2).unwrap();
        let file3_obj = serde_json::to_value(file3).unwrap();
            let private_key_pem = r#"
        -----BEGIN PRIVATE KEY-----
        MC4CAQAwBQYDK2VwBCIEIJBRONAzbwpIOwm0ugIQNyZJrDXxZF7HoPWAZesMedOr
        -----END PRIVATE KEY-----
        "#;
        let jwk = json!(
            {
                "kty": "OKP",
                "crv": "Ed25519",
                "x": "T4Quc1L6Ogu4N2tTKOvneV1yYnBcmhP89B_RsuFsJZ8"
            }
        );
        let public_key_jwk: jsonwebtoken::jwk::Jwk = serde_json::from_value(jwk).unwrap();
        let private_key: EncodingKey =
            EncodingKey::from_ed_pem(private_key_pem.as_bytes()).unwrap();
        let file3_jwt = named_obj_to_jwt(&file3_obj, &private_key, None).unwrap();
        
        let test_map1 = json!({
            "body": {
                "file1": file1_obj_id,
                "file2": {
                    "obj_type": "cyfile",
                    "body": file2_obj
                },
                "file3": {
                    "obj_type": "cyfile",
                    "jwt": file3_jwt
                }
            }
        });

        let mut real_obj =  json!({
            "total_size": 302323,
            "item_count": 3,
        });

        let simple_map1 = serde_json::from_value::<SimpleObjectMap>(test_map1.clone()).unwrap();
       
        assert_eq!(simple_map1.len(), 3);

        let (simple_map1_obj_id, simple_map1_obj_str) = simple_map1.gen_obj_id_with_real_obj(OBJ_TYPE_OBJMAP_SIMPLE,  &mut real_obj).unwrap();
        println!("simple_map1_obj_id: {}", simple_map1_obj_id.to_string());
        println!("simple_map1_obj_str: {}", simple_map1_obj_str);

    }
}