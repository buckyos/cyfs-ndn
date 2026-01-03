use buckyos_kit::{buckyos_get_unix_timestamp,is_default};
use serde::{Serialize,Deserialize};

use crate::{ChunkId};
use std::collections::HashMap;
use crate::{OBJ_TYPE_FILE,OBJ_TYPE_PATH,build_named_object_by_json,ObjId};
use serde_json::Value;

//针对任何FileObject(NamedObject)的传播，都有网络性的3个核心橘色
//1.版权所有:
// author:String,作者，可以为空，为空时作者等于owner
// owner:DID,所有者，为空则是copyleft(属于所有人)
// author和owner都为空时，别认为是一个匿名且无版权的对象
//2. 收录者:DID
// 不保存在Object内的，称作外部收录者。针对不同的context,可以有不同的获得收录者的逻辑
//   - DirObject就是一种标准的收录Context
//   - pkg-source 是一种基于sqlite数据库的Context
// 系统提供多种形式，允许收录者证明“自己收录了该作品”，原则上收录者收录作品，不需要得到作品作者的认可
// 包存在Object内的，有Owner签名的，被称作“作者认可的收录者”，这种是双向收录，
// 单向收录和双向收录在涉及到利益分成时有所区别
// 有的大牌收录者，可能只会传播双向收录的Object
//3. 传播者:DID
// 传播者的DID一定不在Object内
// 传播者通常是基于Context得到的（比如Alice给Bob分享一个文件，Alice就是传播者
// 有的场景，也允许传播者构造签名证明其传播过这个文件。但通常这个签名只需要传播者的设备私钥就能构造。



//TODO：NDN如何提供一种通用机制，检查FileObject在本地是 完全存在的 ？ 在这里的逻辑是FileObject的Content(存在)
// 思路：Object如果引用了另一个Object,要区分这个引用是强引用(依赖）还是弱引用，
#[derive(Serialize,Deserialize,Clone)]
pub struct FileObject {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name:String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta:Option<serde_json::Value>,//description

    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner:Option<String>,

    pub size:u64,
    pub content:String,//chunkid or chunklistid
    #[serde(default)]

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime:Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time:Option<u64>,
    #[serde(skip_serializing_if = "is_default")]
    pub exp:u64,

    #[serde(flatten)]
    pub extra_info: HashMap<String, Value>,
}

impl Default for FileObject {
    fn default() -> Self {
        Self {name:String::new(),size:0,content:String::new(),meta:None,mime:None,owner:None,exp:0,
            create_time:None,extra_info:HashMap::new()}
    }
}

impl FileObject {
    //content can be chunkid or chunklistid
    pub fn new(name:String,size:u64,content:String)->Self {
        Self {name,size,content,meta:None,mime:None,owner:None,exp:0,
            create_time:None,extra_info:HashMap::new()}
    }

    pub fn gen_obj_id(&self)->(ObjId, String) {
        let json_value = serde_json::to_value(self).unwrap();
        build_named_object_by_json(OBJ_TYPE_FILE, &json_value)
    }
}

#[derive(Serialize,Deserialize,Clone,Eq,PartialEq)]
pub struct PathObject {
    pub path:String,
    pub uptime:u64,
    pub target:ObjId,
    pub exp:u64,
}

impl PathObject {
    pub fn new(path:String,target:ObjId)->Self {
        Self {
            path,
            uptime:buckyos_get_unix_timestamp(),
            target,
            exp:buckyos_get_unix_timestamp() + 3600*24*365*3,
        }
    }

    pub fn gen_obj_id(&self)->(ObjId, String) {
        let json_value = serde_json::to_value(self).unwrap();
        build_named_object_by_json(OBJ_TYPE_PATH, &json_value)
    }
}

#[cfg(test)]
mod tests {
    use crate::build_named_object_by_json;

    use super::*;

    #[test]
    fn test_file_object() {
        let file_object = FileObject::new("test.data".to_string(),100,"sha256:1234567890".to_string());
        let file_object_str = serde_json::to_string(&file_object).unwrap();
        println!("file_object_str {}",file_object_str);

        let (objid,obj_str) = file_object.gen_obj_id();
        println!("fileobj id {}",objid.to_string());
        println!("fileobj str {}",obj_str);
    }

    #[test]
    fn test_path_object() {
        let path_object = PathObject::new("/repo/pub_meta_index.db".to_string(),ObjId::new("sha256:1234567890").unwrap());
        let path_object_str = serde_json::to_string(&path_object).unwrap();
        println!("path_object_str {}",path_object_str);

        let (objid,obj_str) = path_object.gen_obj_id();
        println!("pathobj id {}",objid.to_string());
        println!("pathobj str {}",obj_str);
    }
}