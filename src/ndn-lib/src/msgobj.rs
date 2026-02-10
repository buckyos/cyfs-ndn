//! Message object definitions.

use crate::{NamedObject, ObjId, OBJ_TYPE_MSG, OBJ_TYPE_MSG_RECE};
use buckyos_kit::buckyos_get_unix_timestamp;
use name_lib::DID;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// A URI-like helper for display or transport hints.
pub type Uri = String;

/// Message semantic kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsgObjKind {
    Info,
    Event,
    Accept,
    Decline,
    Deliver,
    Progress,
    Error,
    Cancel,
}

/// Human content format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgContentFormat {
    TextPlain,
    TextMarkdown,
    TextHtml,
    ImagePng,
    ImageJpeg,
    ApplicationJson,
    Unknown(String),
}

impl Serialize for MsgContentFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            MsgContentFormat::TextPlain => "text/plain",
            MsgContentFormat::TextMarkdown => "text/markdown",
            MsgContentFormat::TextHtml => "text/html",
            MsgContentFormat::ImagePng => "image/png",
            MsgContentFormat::ImageJpeg => "image/jpeg",
            MsgContentFormat::ApplicationJson => "application/json",
            MsgContentFormat::Unknown(v) => v.as_str(),
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for MsgContentFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "text/plain" => MsgContentFormat::TextPlain,
            "text/markdown" => MsgContentFormat::TextMarkdown,
            "text/html" => MsgContentFormat::TextHtml,
            "image/png" => MsgContentFormat::ImagePng,
            "image/jpeg" => MsgContentFormat::ImageJpeg,
            "application/json" => MsgContentFormat::ApplicationJson,
            _ => MsgContentFormat::Unknown(s),
        })
    }
}

/// Threading/correlation metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Thread {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reply_to: Option<ObjId>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tunnel_id: Option<String>,
}

/// Canonical machine value for structured payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CanonValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<CanonValue>),
    Object(BTreeMap<String, CanonValue>),
}

impl Default for CanonValue {
    fn default() -> Self {
        Self::Null
    }
}

/// Machine-facing payload lane.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MachineContent {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub intent: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub data: BTreeMap<String, CanonValue>,
}

/// Two reference kinds: data object and service DID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RefTarget {
    DataObj {
        obj_id: ObjId,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        uri_hint: Option<Uri>,
    },
    ServiceDid {
        did: DID,
    },
}

/// Reference role for indexing/policy hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefRole {
    Context,
    Input,
    Output,
    Evidence,
    Control,
}

/// A structured reference entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefItem {
    pub role: RefRole,
    pub target: RefTarget,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub label: Option<String>,
}

/// Fixed payload shape for message content.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MsgContent {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub format: Option<MsgContentFormat>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub machine: Option<MachineContent>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub refs: Vec<RefItem>,
}

/// Immutable message object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsgObject {
    pub from: DID,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source: Option<DID>,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    #[serde(skip_serializing_if = "Thread::is_empty", default)]
    pub thread: Thread,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub workspace: Option<DID>,
    #[serde(skip_serializing_if = "is_zero", default)]
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expires_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub nonce: Option<u64>,
    pub content: MsgContent,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub proof: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default, flatten)]
    pub meta: BTreeMap<String, serde_json::Value>,
}

impl Thread {
    pub fn is_empty(&self) -> bool {
        self.topic.is_none()
            && self.reply_to.is_none()
            && self.correlation_id.is_none()
            && self.tunnel_id.is_none()
    }
}

impl Default for MsgObject {
    fn default() -> Self {
        Self {
            from: DID::undefined(),
            source: None,
            to: Vec::new(),
            kind: MsgObjKind::Info,
            thread: Thread::default(),
            workspace: None,
            created_at_ms: 0,
            expires_at_ms: None,
            nonce: None,
            content: MsgContent::default(),
            proof: None,
            meta: BTreeMap::new(),
        }
    }
}

impl MsgObject {
    pub fn new(from: DID, to: Vec<DID>, kind: MsgObjKind, content: MsgContent) -> Self {
        Self {
            from,
            to,
            kind,
            content,
            created_at_ms: buckyos_get_unix_timestamp() * 1000,
            ..Self::default()
        }
    }
}

impl NamedObject for MsgObject {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_MSG
    }
}

/// Inbox/outbox index entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoxEntry {
    pub msg_id: ObjId,
    pub added_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    Accepted,
    Rejected,
    Quarantined,
}

/// Optional immutable delivery receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsgReceiptObj {
    pub msg_id: ObjId,
    pub iss:DID,
    pub accepted_at_ms: u64,
    pub status: ReceiptStatus,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
}

impl NamedObject for MsgReceiptObj {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_MSG_RECE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_named_object_by_json;
    use serde_json::json;

    fn did_web(host: &str) -> DID {
        DID::new("web", host)
    }

    fn assert_msg_roundtrip_consistency(original: &MsgObject) -> MsgObject {
        let s1 = serde_json::to_string(original).unwrap();
        let d1: MsgObject = serde_json::from_str(&s1).unwrap();
        let s2 = serde_json::to_string(&d1).unwrap();
        let d2: MsgObject = serde_json::from_str(&s2).unwrap();

        assert_eq!(d1, d2);

        let v1: serde_json::Value = serde_json::from_str(&s1).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&s2).unwrap();
        assert_eq!(v1, v2);

        let (id0, _) = original.gen_obj_id();
        let (id1, _) = d1.gen_obj_id();
        let (id2, _) = d2.gen_obj_id();
        assert_eq!(id0, id1);
        assert_eq!(id1, id2);
        assert_eq!(id2.obj_type, OBJ_TYPE_MSG);

        let (id3, _) = build_named_object_by_json(OBJ_TYPE_MSG, &serde_json::to_value(&d2).unwrap());
        assert_eq!(id2, id3);

        d2
    }

    fn print_msg_json(case_name: &str, msg: &MsgObject) {
        println!(
            "{} json: {}",
            case_name,
            serde_json::to_string_pretty(msg).unwrap()
        );
    }

    fn assert_receipt_roundtrip_consistency(original: &MsgReceiptObj) -> MsgReceiptObj {
        let s1 = serde_json::to_string(original).unwrap();
        let d1: MsgReceiptObj = serde_json::from_str(&s1).unwrap();
        let s2 = serde_json::to_string(&d1).unwrap();
        let d2: MsgReceiptObj = serde_json::from_str(&s2).unwrap();

        assert_eq!(d1, d2);

        let v1: serde_json::Value = serde_json::from_str(&s1).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&s2).unwrap();
        assert_eq!(v1, v2);

        let (id0, _) = original.gen_obj_id();
        let (id1, _) = d1.gen_obj_id();
        let (id2, _) = d2.gen_obj_id();
        assert_eq!(id0, id1);
        assert_eq!(id1, id2);
        assert_eq!(id2.obj_type, OBJ_TYPE_MSG_RECE);

        let (id3, _) = build_named_object_by_json(
            OBJ_TYPE_MSG_RECE,
            &serde_json::to_value(&d2).unwrap(),
        );
        assert_eq!(id2, id3);

        d2
    }

    fn print_receipt_json(case_name: &str, receipt: &MsgReceiptObj) {
        println!(
            "{} json: {}",
            case_name,
            serde_json::to_string_pretty(receipt).unwrap()
        );
    }

    #[test]
    fn test_msg_case_1_standard_plain_text_message() {
        let mut machine_data = BTreeMap::new();
        machine_data.insert("mime".to_string(), CanonValue::String("text/plain".to_string()));
        machine_data.insert("lang".to_string(), CanonValue::String("zh-CN".to_string()));
        machine_data.insert("channel".to_string(), CanonValue::String("dm".to_string()));

        let mut msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Info,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                ..Thread::default()
            },
            created_at_ms: 1735689600000,
            content: MsgContent {
                title: Some("Greeting".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "Meeting at 3 PM, please confirm.".to_string(),
                machine: Some(MachineContent {
                    intent: Some("chat_text".to_string()),
                    data: machine_data,
                }),
                refs: Vec::new(),
            },
            ..MsgObject::default()
        };
        msg.meta.insert("client".to_string(), json!("desktop"));
        print_msg_json("case_1_plain_text", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.kind, MsgObjKind::Info);
        assert_eq!(normalized.content.format, Some(MsgContentFormat::TextPlain));
        assert_eq!(normalized.content.refs.len(), 0);
    }

    #[test]
    fn test_msg_case_2_standard_image_message() {
        let image_obj_id = ObjId::new("sha256:1234567890abcdef").unwrap();

        let mut machine_data = BTreeMap::new();
        machine_data.insert("mime".to_string(), CanonValue::String("image/png".to_string()));
        machine_data.insert("width".to_string(), CanonValue::U64(1280));
        machine_data.insert("height".to_string(), CanonValue::U64(720));
        machine_data.insert("size".to_string(), CanonValue::U64(376218));

        let msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                ..Thread::default()
            },
            created_at_ms: 1735689615000,
            content: MsgContent {
                title: Some("Image".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "[image]".to_string(),
                machine: Some(MachineContent {
                    intent: Some("chat_image".to_string()),
                    data: machine_data,
                }),
                refs: vec![RefItem {
                    role: RefRole::Output,
                    target: RefTarget::DataObj {
                        obj_id: image_obj_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", image_obj_id.to_string())),
                    },
                    label: Some("image/png".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_2_image", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.kind, MsgObjKind::Deliver);
        assert_eq!(normalized.content.refs.len(), 1);
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.intent.as_ref())
                .map(String::as_str),
            Some("chat_image")
        );
    }

    #[test]
    fn test_msg_case_3_reply_to_standard_image_message() {
        let base_image_msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                ..Thread::default()
            },
            created_at_ms: 1735689615000,
            content: MsgContent {
                title: Some("Image".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "[image]".to_string(),
                machine: None,
                refs: vec![RefItem {
                    role: RefRole::Output,
                    target: RefTarget::DataObj {
                        obj_id: ObjId::new("sha256:1234567890abcdef").unwrap(),
                        uri_hint: None,
                    },
                    label: Some("image".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_3_base_image", &base_image_msg);
        let (image_msg_id, _) = base_image_msg.gen_obj_id();

        let reply_msg = MsgObject {
            from: did_web("bob.example.com"),
            to: vec![did_web("alice.example.com")],
            kind: MsgObjKind::Info,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                reply_to: Some(image_msg_id.clone()),
                correlation_id: Some("reply-image-1".to_string()),
                tunnel_id: None,
            },
            created_at_ms: 1735689622000,
            content: MsgContent {
                title: Some("Reply Image".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "Received, image is clear.".to_string(),
                machine: None,
                refs: vec![RefItem {
                    role: RefRole::Context,
                    target: RefTarget::DataObj {
                        obj_id: image_msg_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", image_msg_id.to_string())),
                    },
                    label: Some("reply_to_msg".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_3_reply_image", &reply_msg);

        let normalized = assert_msg_roundtrip_consistency(&reply_msg);
        assert_eq!(normalized.thread.reply_to, Some(image_msg_id));
        assert_eq!(normalized.content.machine, None);
    }

    #[test]
    fn test_msg_case_4_message_built_by_referencing_plain_text_message() {
        let quoted_text_msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Info,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                ..Thread::default()
            },
            created_at_ms: 1735689600000,
            content: MsgContent {
                title: Some("Original Message".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "Release version v1.2.0 tonight".to_string(),
                machine: None,
                refs: Vec::new(),
            },
            ..MsgObject::default()
        };
        print_msg_json("case_4_quoted_text", &quoted_text_msg);
        let (quoted_msg_id, _) = quoted_text_msg.gen_obj_id();

        let quote_msg = MsgObject {
            from: did_web("bob.example.com"),
            to: vec![did_web("alice.example.com")],
            kind: MsgObjKind::Event,
            thread: Thread {
                topic: Some("dm-alice-bob".to_string()),
                reply_to: Some(quoted_msg_id.clone()),
                correlation_id: Some("quote-1".to_string()),
                tunnel_id: None,
            },
            created_at_ms: 1735689630000,
            content: MsgContent {
                title: Some("Quoted Reply".to_string()),
                format: Some(MsgContentFormat::TextPlain),
                content: "Quote and confirm test plan".to_string(),
                machine: None,
                refs: vec![RefItem {
                    role: RefRole::Context,
                    target: RefTarget::DataObj {
                        obj_id: quoted_msg_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", quoted_msg_id.to_string())),
                    },
                    label: Some("quoted_msg".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_4_quote_message", &quote_msg);

        let normalized = assert_msg_roundtrip_consistency(&quote_msg);
        assert_eq!(normalized.thread.reply_to, Some(quoted_msg_id));
        assert_eq!(normalized.content.machine, None);
    }

    #[test]
    fn test_msg_case_5_group_chat_message() {
        let group_did = did_web("dev-team.chat.example.com");

        let mut machine_data = BTreeMap::new();
        machine_data.insert("chat_type".to_string(), CanonValue::String("group".to_string()));
        machine_data.insert("member_count".to_string(), CanonValue::U64(3));
        machine_data.insert(
            "mentions".to_string(),
            CanonValue::Array(vec![
                CanonValue::String("did:web:bob.example.com".to_string()),
                CanonValue::String("did:web:carol.example.com".to_string()),
            ]),
        );

        let mut msg = MsgObject {
            from: did_web("workgroup.example.com"),
            to: vec![group_did.clone()],
            source: Some(did_web("alice.example.com")),
            kind: MsgObjKind::Info,
            thread: Thread {
                topic: Some("grp-release".to_string()),
                correlation_id: None,
                tunnel_id: Some("im/slack".to_string()),
                reply_to: None,
            },
            workspace: Some(did_web("project.example.com")),
            created_at_ms: 1735689640000,
            content: MsgContent {
                title: None,
                format: Some(MsgContentFormat::TextPlain),
                content: "@bob @carol release is out, please watch metrics.".to_string(),
                machine: Some(MachineContent {
                    intent: Some("group_chat_text".to_string()),
                    data: machine_data,
                }),
                refs: vec![RefItem {
                    role: RefRole::Control,
                    target: RefTarget::ServiceDid { did: group_did },
                    label: Some("group_inbox".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        msg.meta.insert("room".to_string(), json!("release-war-room"));
        print_msg_json("case_5_group_chat", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.to.len(), 1);
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.intent.as_ref())
                .map(String::as_str),
            Some("group_chat_text")
        );
        assert_eq!(normalized.meta.get("room"), Some(&json!("release-war-room")));
    }

    #[test]
    fn test_msg_case_6_minimal_all_optional_none() {
        let msg = MsgObject {
            from: did_web("a.example.com"),
            source: None,
            to: vec![did_web("b.example.com")],
            kind: MsgObjKind::Info,
            thread: Thread::default(),
            workspace: None,
            created_at_ms: 0,
            expires_at_ms: None,
            nonce: None,
            content: MsgContent {
                title: None,
                format: None,
                content: "ok!".to_string(),
                machine: None,
                refs: Vec::new(),
            },
            proof: None,
            meta: BTreeMap::new(),
        };
        print_msg_json("case_6_minimal_none", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.source, None);
        assert_eq!(normalized.workspace, None);
        assert_eq!(normalized.expires_at_ms, None);
        assert_eq!(normalized.nonce, None);
        assert_eq!(normalized.proof, None);
        assert_eq!(normalized.content.title, None);
        assert_eq!(normalized.content.format, None);
        assert_eq!(normalized.content.machine, None);
        assert_eq!(normalized.content.refs.len(), 0);
        assert!(normalized.meta.is_empty());
    }

    #[test]
    fn test_msg_content_format_unknown_fallback() {
        let raw = json!({
            "from": "did:web:a.example.com",
            "to": ["did:web:b.example.com"],
            "kind": "info",
            "content": {
                "content": "hello",
                "format": "text/x-custom"
            }
        });

        // Deserialize from JSON string.
        let raw_str = serde_json::to_string(&raw).unwrap();
        let msg: MsgObject = serde_json::from_str(&raw_str).unwrap();
        assert_eq!(
            msg.content.format,
            Some(MsgContentFormat::Unknown("text/x-custom".to_string()))
        );

        // Deserialize from JSON value.
        let msg_from_value: MsgObject = serde_json::from_value(raw).unwrap();
        assert_eq!(
            msg_from_value.content.format,
            Some(MsgContentFormat::Unknown("text/x-custom".to_string()))
        );

        // Serialize and deserialize again to ensure fallback value is stable.
        let value = serde_json::to_value(&msg).unwrap();
        assert_eq!(value["content"]["format"], json!("text/x-custom"));

        let encoded = serde_json::to_string(&msg).unwrap();
        let msg2: MsgObject = serde_json::from_str(&encoded).unwrap();
        assert_eq!(msg, msg2);
        assert_eq!(
            msg2.content.format,
            Some(MsgContentFormat::Unknown("text/x-custom".to_string()))
        );
    }

    #[test]
    fn test_msg_receipt_obj_minimal() {
        let receipt = MsgReceiptObj {
            msg_id: ObjId::new("cymsg:1234567890abcdef").unwrap(),
            iss: did_web("msg-receipt.example.com"),
            accepted_at_ms: 1735689700000,
            status: ReceiptStatus::Accepted,
            reason: None,
        };
        print_receipt_json("receipt_minimal", &receipt);

        let value = serde_json::to_value(&receipt).unwrap();
        assert!(value.get("reason").is_none());
        assert_eq!(value["iss"], json!("did:web:msg-receipt.example.com"));

        let normalized = assert_receipt_roundtrip_consistency(&receipt);
        assert_eq!(normalized.iss, did_web("msg-receipt.example.com"));
        assert_eq!(normalized.status, ReceiptStatus::Accepted);
        assert_eq!(normalized.reason, None);
    }

    #[test]
    fn test_msg_receipt_obj_with_issuer_and_reason() {
        let receipt = MsgReceiptObj {
            msg_id: ObjId::new("cymsg:abcdef1234567890").unwrap(),
            iss: did_web("inbox-router.example.com"),
            accepted_at_ms: 1735689710000,
            status: ReceiptStatus::Rejected,
            reason: Some("policy_denied".to_string()),
        };
        print_receipt_json("receipt_with_issuer_reason", &receipt);

        let normalized = assert_receipt_roundtrip_consistency(&receipt);
        assert_eq!(normalized.iss, did_web("inbox-router.example.com"));
        assert_eq!(normalized.status, ReceiptStatus::Rejected);
        assert_eq!(normalized.reason, Some("policy_denied".to_string()));
    }

    #[test]
    fn test_msg_receipt_obj_from_json_and_obj_id_consistency() {
        let raw = json!({
            "msg_id": "cymsg:00112233445566778899aabbccddeeff",
            "iss": "did:web:inbox.example.com",
            "accepted_at_ms": 1735689720000u64,
            "status": "quarantined",
            "reason": "needs_manual_review"
        });

        let receipt: MsgReceiptObj = serde_json::from_value(raw).unwrap();
        assert_eq!(receipt.iss, did_web("inbox.example.com"));
        assert_eq!(receipt.status, ReceiptStatus::Quarantined);
        assert_eq!(receipt.reason, Some("needs_manual_review".to_string()));
        print_receipt_json("receipt_from_json", &receipt);

        let (obj_id, _) = receipt.gen_obj_id();
        assert_eq!(obj_id.obj_type, OBJ_TYPE_MSG_RECE);

        let (obj_id2, _) = build_named_object_by_json(
            OBJ_TYPE_MSG_RECE,
            &serde_json::to_value(&receipt).unwrap(),
        );
        assert_eq!(obj_id, obj_id2);
    }
}
