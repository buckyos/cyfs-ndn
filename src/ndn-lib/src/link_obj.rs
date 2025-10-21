
use std::{collections::HashMap, ops::Range};
use buckyos_kit::buckyos_get_unix_timestamp;
use serde::{Serialize,Deserialize};
use crate::{build_named_object_by_json, ChunkId, NdnError, NdnResult, ObjId, OBJ_TYPE_RELATION, RELATION_TYPE_PART_OF, RELATION_TYPE_SAME};


// #[derive(Debug, Clone,Eq, PartialEq)]
// pub enum LinkData {
//     SameAs(ObjId),//Same ChunkId
//     LocalFile(String,Range<u64>,u64,String),//Local File Path, Range in file, file last modify time, qcid
//     //ComposedBy(ChunkId,ObjMapId),// Base ChunkId + Diff Action Items
//     PartOf(ChunkId,Range<u64>), //Object Id + Range
//     //IndexOf(ObjId,u64),//Object Id + Index
// }

// impl Serialize for LinkData {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         serializer.serialize_str(&self.to_string())
//     }
// }

// impl<'a> Deserialize<'a> for LinkData {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'a>,
//     {
//         let s = String::deserialize(deserializer)?;
//         LinkData::from_string(&s).map_err(serde::de::Error::custom)
//     }
// }

// impl LinkData {
//     pub fn to_string(&self)->String {
//         match self {
//             LinkData::SameAs(obj_id) => format!("same->{}",obj_id.to_string()),
//             LinkData::PartOf(chunk_id,range) => {
//                 let range_str = format!("{}..{}",range.start,range.end);
//                 format!("part_of->{}@{}",range_str,chunk_id.to_string())
//             }
//             LinkData::LocalFile(file_path,range,last_modify_time,qcid) =>  {
//                 let range_str = format!("{}..{}",range.start,range.end);
//                 format!("file->{}@{}@{}@{}",range_str,file_path,last_modify_time,qcid)
//             }
        
//         }
//     }

//     pub fn from_string(link_str:&str)->NdnResult<Self> {
//         let parts = link_str.split("->").collect::<Vec<&str>>();
//         if parts.len() != 2 {
//             return Err(NdnError::InvalidLink(format!("invalid link string:{}",link_str)));
//         }
//         let link_type = parts[0];
//         let link_data = parts[1];

//         match link_type {
//             "same" => Ok(LinkData::SameAs(ObjId::new(link_data)?)),
//             "part_of" => {
//                 let parts = link_data.split("@").collect::<Vec<&str>>();
//                 if parts.len() != 2 {
//                     return Err(NdnError::InvalidLink(format!("invalid link string:{}",link_str)));
//                 }
//                 let range = parts[0].split("..").collect::<Vec<&str>>();
//                 if range.len() != 2 {
//                     return Err(NdnError::InvalidLink(format!("invalid range string:{}",parts[1])));
//                 }
//                 let start = range[0].parse::<u64>().unwrap();
//                 let end = range[1].parse::<u64>().unwrap();
//                 Ok(LinkData::PartOf(ChunkId::new(parts[1])?,Range{start,end}))
//             }
//             "file" => {
//                 let parts = link_data.split("@").collect::<Vec<&str>>();
//                 if parts.len() != 4 {
//                     return Err(NdnError::InvalidLink(format!("invalid link string:{}",link_str)));
//                 }
//                 let range = parts[0].split("..").collect::<Vec<&str>>();
//                 if range.len() != 2 {
//                     return Err(NdnError::InvalidLink(format!("invalid range string:{}",parts[1])));
//                 }
//                 let start = range[0].parse::<u64>().unwrap();
//                 let end = range[1].parse::<u64>().unwrap();
//                 Ok(LinkData::LocalFile(parts[1].to_string(),Range{start,end},parts[2].parse::<u64>().unwrap(),parts[3].to_string()))
//             }
//             _ => Err(NdnError::InvalidLink(format!("invalid link type:{}",link_type))),
//         }
//     }
// }


////////////////////////////////////////////////////
#[derive(Debug, Clone,Eq, PartialEq)]
pub enum ObjectLinkData {
    SameAs(ObjId),//Same ， src object is same as target object
    PartOf(ObjId,Range<u64>), //Object Id + Range
}



#[derive(Serialize, Deserialize)]
pub struct RelationObject {
    pub source: ObjId,
    pub relation: String,
    pub target: ObjId,
    #[serde(flatten)]
    pub body: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none",default)]
    pub iat: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none",default)]
    pub exp: Option<u64>   
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
            ObjectLinkData::PartOf(chunk_id,range) => 
            {
                let mut body = HashMap::new();

                let range_value = serde_json::json!({
                    "start": range.start,   
                    "end": range.end,
                });
                body.insert("range".to_string(),range_value);

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

    pub fn get_link_data(self)->NdnResult<ObjectLinkData> {
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
                    return Err(NdnError::InvalidLink(format!("invalid range:{}",range.to_string())));
                }
                let start = start.unwrap().as_u64();
                let end = end.unwrap().as_u64();
                if start.is_none() || end.is_none() {
                    return Err(NdnError::InvalidLink(format!("invalid range:{}",range.to_string())));
                }
                return Ok(ObjectLinkData::PartOf(self.target,Range{start:start.unwrap(),end:end.unwrap()}));
                
            },
            _ => return Err(NdnError::InvalidLink(format!("invalid relation:{}",self.relation))),
        }
    }

    pub fn gen_obj_id(&self)->(ObjId,String) {
        let obj_value = serde_json::to_value(self).unwrap();
        return build_named_object_by_json(OBJ_TYPE_RELATION,&obj_value);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_object() {
        let link_data1 = ObjectLinkData::SameAs(ObjId::new("test:1234").unwrap());
        let relation_object = RelationObject::create_by_link_data(ObjId::new("test:1234").unwrap(),link_data1.clone());
        let (obj_id,obj_str) = relation_object.gen_obj_id();
        println!("robj_id {}",obj_id.to_string());
        assert_eq!(obj_id.to_string(),"cyrel:e0ad5f3b656a883de323e4c6e7999207ce3026d3fa5dcb47518d2caeb4d92aa0");
        println!("robj_str {}",obj_str);
        let link_data = relation_object.get_link_data().unwrap();
        println!("link_data {:?}",&link_data);
        assert_eq!(link_data,link_data1);

        let link_data2 = ObjectLinkData::PartOf(ObjId::new("test:1234").unwrap(),Range{start:0,end:100});
        let relation_object2 = RelationObject::create_by_link_data(ObjId::new("test:1234").unwrap(),link_data2.clone());
        let (obj_id2,obj_str2) = relation_object2.gen_obj_id();
        println!("robj_id2 {}",obj_id2.to_string());
        assert_eq!(obj_id2.to_string(),"cyrel:cf2ff05aaa9165e9c7fb2bc642cea5a02b730d9f0415f907fc9d4a6bad66bca9");
        println!("robj_str2 {}",obj_str2);
        let link_data3 = relation_object2.get_link_data().unwrap();
        println!("link_data3 {:?}",&link_data2);
        assert_eq!(link_data3,link_data2);
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