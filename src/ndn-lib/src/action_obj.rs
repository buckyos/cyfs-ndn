use crate::{NamedObject, ObjId, OBJ_TYPE_ACTION};
use serde::{Deserialize, Serialize};

pub const ACTION_TYPE_VIEWED: &str = "viewed";
pub const ACTION_TYPE_DOWNLOAD: &str = "download";
pub const ACTION_TYPE_INSTALLED: &str = "installed";
pub const ACTION_TYPE_SHARED: &str = "shared";
pub const ACTION_TYPE_LIKED: &str = "liked";
pub const ACTION_TYPE_UNLIKED: &str = "unliked";
pub const ACTION_TYPE_PURCHASED: &str = "purchased";

// subject does the action on target
#[derive(Serialize, Deserialize, Clone)]
pub struct ActionObject {
    pub subject: ObjId,
    pub action: String,
    pub target: ObjId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub base_on: Option<ObjId>, //the action is based on another action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    pub iat: u64,
    pub exp: u64,
}

impl NamedObject for ActionObject {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_ACTION
    }
}
