use crate::{NamedObject, ObjId, OBJ_TYPE_INCLUSION_PROOF};
use buckyos_kit::buckyos_get_unix_timestamp;
use name_lib::DID;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

fn is_zero(v: &u64) -> bool {
    *v == 0
}

fn is_owner_invalid(owner: &DID) -> bool {
    !owner.is_valid()
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Curator {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Reference {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BaseContentObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub did: Option<DID>,
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub name: String, //friendly name,如果被保存在文件系统里应该用的名字，通常会是did的一部分
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub meta:Option<serde_json::Value>,//description
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub author: String,
    #[serde(skip_serializing_if = "is_owner_invalid")]
    #[serde(default)]
    pub owner: DID,
    pub create_time: u64,
    pub last_update_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub copyright: Option<String>, //copyright info
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub tags: Vec<String>, //tags
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub categories: Vec<String>, //categories，非常重要，说明这是什么类型的内容。不同类型的内容有不同的`五维评级`图
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub base_on: Option<ObjId>, //this content is based on another content id
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub directory: HashMap<String, Curator>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub references: HashMap<String, Reference>,
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub exp: u64,
}

impl Default for BaseContentObject {
    fn default() -> Self {
        let now = buckyos_get_unix_timestamp();
        Self {
            did: None,
            name: String::new(),
            author: String::new(),
            owner: DID::undefined(),
            create_time: 0,
            last_update_time: 0,
            copyright: None,
            tags: Vec::new(),
            categories: Vec::new(),
            base_on: None,
            directory: HashMap::new(),
            references: HashMap::new(),
            exp: 0,
        }
    }
}

impl BaseContentObject {
    pub fn new(name: String) -> Self {
        let mut obj = Self::default();
        obj.name = name;
        obj
    }

    pub fn new_with_create_time(name: String, create_time: u64) -> Self {
        let mut result = Self::new(name);
        result.create_time = create_time;
        result.last_update_time = create_time;
        result
    }
}

//定义 已收录证明 的结构

/// 收录者颁发给内容创建者的“已收录证明”。
///
/// 建议将本结构序列化后的 JSON 作为 JWT claims，并使用收录者的 DID 私钥（EdDSA）签名后分发；
/// 验签逻辑在上层根据 `curator` 对应的公钥完成。
#[derive(Serialize, Deserialize, Clone)]
pub struct InclusionProof {
    /// 被收录的内容 ObjId,必须和content_obj一致
    pub content_id: String,
    pub content_obj: serde_json::Value,

    /// 收录者身份（推荐 DID 字符串）
    pub curator: DID,
    pub editor: Vec<String>, //editor is the editor of the curator organization
    pub meta: Option<serde_json::Value>, //extra meta info
    pub rank: i64,           //rank of the content ,1-100

    /// 收录到哪个“收录空间/目录/集合”里
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub collection: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub review_url: Option<String>, //review url of the content

    pub iat: u64,
    pub exp: u64,
}

impl InclusionProof {
    pub fn new(
        content_id: ObjId,
        content_obj: serde_json::Value,
        curator: DID,
        rank: i64,
        collection: Vec<String>,
    ) -> Self {
        let now = buckyos_get_unix_timestamp();
        Self {
            content_id: content_id.to_string(),
            content_obj: content_obj,
            curator,
            editor: Vec::new(),
            rank,
            collection,
            review_url: None,
            iat: now,
            exp: now + 3600 * 24 * 30 * 12, //12 months
            meta: None,
        }
    }

}

impl NamedObject for InclusionProof {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_INCLUSION_PROOF
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inclusion_proof() {
        let content_id = ObjId::new("cyfile:1234").unwrap();
        let content_obj = serde_json::json!({
            "name": "test_app",
        });
        let curator = DID::new("web", "gitpot.ai");
        let rank = 1;
        let mut inclusion_proof = InclusionProof::new(
            content_id,
            content_obj,
            curator,
            rank,
            vec!["apps".to_string()],
        );
        inclusion_proof.editor = vec!["did:web:wcy.gitpot.ai".to_string()];
        inclusion_proof.review_url = Some("https://gitpot.ai/reviews/apps/test_app".to_string());
        let (obj_id, obj_str) = inclusion_proof.gen_obj_id();

        println!(
            "inclusion_proof: {}",
            serde_json::to_string_pretty(&inclusion_proof).unwrap()
        );
    }
}
