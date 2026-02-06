use crate::{
    build_named_object_by_json, ChunkId, NdnError, NdnResult, ObjId, OBJ_TYPE_RELATION,
    RELATION_TYPE_PART_OF, RELATION_TYPE_SAME,
};
use buckyos_kit::buckyos_get_unix_timestamp;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Range};

/*

实现去中心的“内容评论”(强实体关系)

 - 内容的评论，本质上是基于一个特定的Content Object二次创作的NameObject
 - 该NamedObject的传播，也立足于“Onwer、收录者、传播者“的三元结构
 - 当创作一个评论时，自己时评论的Owner，原内容的Owner/收录者 都可以是该评论的收录者
 - 如何获得一个内容的所有评论？ 基于内容的“Onwer、收录者、传播者“，分别查询是否有基于该内容的评论，从这些渠道获得评论object后先去重，再进行本地展示
 - 当得到一个评论列表后，又可以基于评论列表里所有评论的作者，进一步获得更多的评论
 - 本地LLM可以很大的对海量的不同来源的评论进行筛选

 本文件中的RelationObject，是一种弱实体关系
 允许在两个对象的作者都不知情的情况下，被关联在一起
 创建RelationObject的作者，通常是一种纯粹的观察（洞察）视角

  通过一个cyfs:// url来引用obj,可以同时包含目标对象的 语义路径 + 引用时的objid
  这种引用方式，可以允许被引用的objid发生变化,而语义路径不变（比如可以简单的筛选针对一个软件所有版本的评论和针对特定版本的评论）
*/

////////////////////////////////////////////////////
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ObjectLinkData {
    SameAs(ObjId),             //Same ， src object is same as target object
    PartOf(ObjId, Range<u64>), //Object Id + Range
}

#[derive(Serialize, Deserialize)]
pub struct RelationObject {
    pub source: ObjId,
    pub relation: String,
    pub target: ObjId,
    #[serde(flatten)]
    pub body: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub iat: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exp: Option<u64>,
}

impl RelationObject {
    pub fn create_by_link_data(source: ObjId, link_data: ObjectLinkData) -> Self {
        match link_data {
            ObjectLinkData::SameAs(target) => {
                return Self {
                    source,
                    relation: RELATION_TYPE_SAME.to_string(),
                    target,
                    body: HashMap::new(),
                    iat: None,
                    exp: None,
                };
            }
            ObjectLinkData::PartOf(chunk_id, range) => {
                let mut body = HashMap::new();

                let range_value = serde_json::json!({
                    "start": range.start,
                    "end": range.end,
                });
                body.insert("range".to_string(), range_value);

                return Self {
                    source,
                    relation: RELATION_TYPE_PART_OF.to_string(),
                    target: chunk_id,
                    body: body,
                    iat: None,
                    exp: None,
                };
            }
        }
    }

    pub fn get_link_data(self) -> NdnResult<ObjectLinkData> {
        match self.relation.as_str() {
            RELATION_TYPE_SAME => {
                return Ok(ObjectLinkData::SameAs(self.target));
            }
            RELATION_TYPE_PART_OF => {
                let range = self.body.get("range");
                let range = range.unwrap();
                let start = range.get("start");
                let end = range.get("end");
                if start.is_none() || end.is_none() {
                    return Err(NdnError::InvalidLink(format!(
                        "invalid range:{}",
                        range.to_string()
                    )));
                }
                let start = start.unwrap().as_u64();
                let end = end.unwrap().as_u64();
                if start.is_none() || end.is_none() {
                    return Err(NdnError::InvalidLink(format!(
                        "invalid range:{}",
                        range.to_string()
                    )));
                }
                return Ok(ObjectLinkData::PartOf(
                    self.target,
                    Range {
                        start: start.unwrap(),
                        end: end.unwrap(),
                    },
                ));
            }
            _ => {
                return Err(NdnError::InvalidLink(format!(
                    "invalid relation:{}",
                    self.relation
                )))
            }
        }
    }

    pub fn gen_obj_id(&self) -> (ObjId, String) {
        let obj_value = serde_json::to_value(self).unwrap();
        return build_named_object_by_json(OBJ_TYPE_RELATION, &obj_value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_object() {
        let link_data1 = ObjectLinkData::SameAs(ObjId::new("test:1234").unwrap());
        let relation_object = RelationObject::create_by_link_data(
            ObjId::new("test:1234").unwrap(),
            link_data1.clone(),
        );
        let (obj_id, obj_str) = relation_object.gen_obj_id();
        println!("robj_id {}", obj_id.to_string());
        assert_eq!(
            obj_id.to_string(),
            "cyrel:e0ad5f3b656a883de323e4c6e7999207ce3026d3fa5dcb47518d2caeb4d92aa0"
        );
        println!("robj_str {}", obj_str);
        let link_data = relation_object.get_link_data().unwrap();
        println!("link_data {:?}", &link_data);
        assert_eq!(link_data, link_data1);

        let link_data2 = ObjectLinkData::PartOf(
            ObjId::new("test:1234").unwrap(),
            Range { start: 0, end: 100 },
        );
        let relation_object2 = RelationObject::create_by_link_data(
            ObjId::new("test:1234").unwrap(),
            link_data2.clone(),
        );
        let (obj_id2, obj_str2) = relation_object2.gen_obj_id();
        println!("robj_id2 {}", obj_id2.to_string());
        assert_eq!(
            obj_id2.to_string(),
            "cyrel:cf2ff05aaa9165e9c7fb2bc642cea5a02b730d9f0415f907fc9d4a6bad66bca9"
        );
        println!("robj_str2 {}", obj_str2);
        let link_data3 = relation_object2.get_link_data().unwrap();
        println!("link_data3 {:?}", &link_data2);
        assert_eq!(link_data3, link_data2);
    }

    // #[test]
    // fn test_link_data() {
    //     let link_data = LinkData::SameAs(ObjId::new("test:1234").unwrap());
    //     let link_str = link_data.to_string();
    //     println!("link_str {}",link_str);
    //     let link_data2 = LinkData::from_string(&link_str).unwrap();
    //     assert_eq!(link_data,link_data2);

    //     let chunk_id = ChunkId::new("sha256:1234567890").unwrap();
    //     let link_data = LinkData::PartOf(chunk_id,Range{start:0,end:100});
    //     let link_str = link_data.to_string();
    //     println!("link_str {}",link_str);
    //     let link_data2 = LinkData::from_string(&link_str).unwrap();
    //     assert_eq!(link_data,link_data2);

    //     let chunk_id = ChunkId::new("sha256:1234567890AE").unwrap();
    //     let link_data = LinkData::LocalFile("/Users/liuzhicong/Downloads/te  st.txt".to_string(),Range{start:0,end:1024},1717862400,"_".to_string());
    //     let link_str = link_data.to_string();
    //     println!("link_str {}",link_str);
    //     let link_data2 = LinkData::from_string(&link_str).unwrap();
    //     assert_eq!(link_data,link_data2);
    // }
}
