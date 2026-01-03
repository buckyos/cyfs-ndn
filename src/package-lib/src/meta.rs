use ndn_lib::{build_named_object_by_json, ObjId, OBJ_TYPE_PKG};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::EncodeUtf16};
use serde_json::{Value,json};
use name_lib::*;
use buckyos_kit::buckyos_get_unix_timestamp;
use crate::{PkgResult, PkgError,PackageId};

fn is_zero(value:&u64)->bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackageMeta {
    pub pkg_name: String,
    pub version: String,
    pub meta: Value,//description

    pub author: String,
    pub owner:DID,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>, //pkg的分类,app,pkg,agent等
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    //key = pkg_name,value = version_req_str,like ">1.0.0-alpha"
    pub deps: HashMap<String, String>,     

    pub create_time: u64,
    #[serde(default)]
    pub exp:u64,

    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub size:u64,//如果content是null,则为0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>, //有些pkg不需要下载
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_url: Option<String>, //发布时的URL,可以不写

    #[serde(flatten)]
    pub extra_info: HashMap<String, Value>,

}

impl PackageMeta {

    pub fn new(pkg_name: &str, version: &str, author: &str, owner: &DID, tag: Option<&str>) -> Self {
        let now = buckyos_kit::buckyos_get_unix_timestamp();
        let exp = now + 3600 * 24 * 30;
        Self {
            pkg_name: pkg_name.to_string(),
            version: version.to_string(),
            author: author.to_string(),
            tag: tag.map(|s| s.to_string()),
            category: None,
            owner: owner.clone(),

            content: None,
            size: 0,
            chunk_url: None,
            create_time: now,
            exp: exp,
            deps: HashMap::new(),
            extra_info: HashMap::new(),
            meta: json!({}),
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
        if self.tag.is_some() {
            let package_id_str = format!("{}#{}:{}",self.pkg_name,self.version,self.tag.as_ref().unwrap());
            PackageId::parse(&package_id_str).unwrap()
        } else {
            let package_id_str = format!("{}#{}",self.pkg_name,self.version);
            PackageId::parse(&package_id_str).unwrap()
        }
    }

    pub fn gen_obj_id(&self) -> (ObjId, String) {
        let json_value = serde_json::to_value(self).unwrap();
        build_named_object_by_json(OBJ_TYPE_PKG, &json_value)  
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