//! Message object definitions.

use crate::{
    named_obj_to_jwt, try_build_named_object_by_json, NamedObject, NdnError, NdnResult, ObjId,
    OBJ_TYPE_MSG, OBJ_TYPE_RECEIPT,
};
use buckyos_kit::buckyos_get_unix_timestamp;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation};
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
    Chat,
    GroupMsg,
    Deliver,
    Notify,
    Event,
    Operation,
}

impl Default for MsgObjKind {
    fn default() -> Self {
        Self::Chat
    }
}

/// Human content format (MIME-type based).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgContentFormat {
    // Text
    TextPlain,
    TextMarkdown,
    TextHtml,
    TextCss,
    TextXml,
    // Image
    ImagePng,
    ImageJpeg,
    ImageGif,
    ImageWebp,
    ImageSvg,
    ImageBmp,
    // Video
    VideoMp4,
    VideoWebm,
    VideoOgg,
    VideoQuicktime,
    VideoAvi,
    // Audio
    AudioMpeg,
    AudioWav,
    AudioOgg,
    AudioWebm,
    AudioAac,
    AudioFlac,
    // Document / Application
    ApplicationJson,
    ApplicationXml,
    ApplicationPdf,
    ApplicationZip,
    ApplicationOctetStream,
    // Fallback for unlisted MIME types
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
            MsgContentFormat::TextCss => "text/css",
            MsgContentFormat::TextXml => "text/xml",
            MsgContentFormat::ImagePng => "image/png",
            MsgContentFormat::ImageJpeg => "image/jpeg",
            MsgContentFormat::ImageGif => "image/gif",
            MsgContentFormat::ImageWebp => "image/webp",
            MsgContentFormat::ImageSvg => "image/svg+xml",
            MsgContentFormat::ImageBmp => "image/bmp",
            MsgContentFormat::VideoMp4 => "video/mp4",
            MsgContentFormat::VideoWebm => "video/webm",
            MsgContentFormat::VideoOgg => "video/ogg",
            MsgContentFormat::VideoQuicktime => "video/quicktime",
            MsgContentFormat::VideoAvi => "video/x-msvideo",
            MsgContentFormat::AudioMpeg => "audio/mpeg",
            MsgContentFormat::AudioWav => "audio/wav",
            MsgContentFormat::AudioOgg => "audio/ogg",
            MsgContentFormat::AudioWebm => "audio/webm",
            MsgContentFormat::AudioAac => "audio/aac",
            MsgContentFormat::AudioFlac => "audio/flac",
            MsgContentFormat::ApplicationJson => "application/json",
            MsgContentFormat::ApplicationXml => "application/xml",
            MsgContentFormat::ApplicationPdf => "application/pdf",
            MsgContentFormat::ApplicationZip => "application/zip",
            MsgContentFormat::ApplicationOctetStream => "application/octet-stream",
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
            "text/css" => MsgContentFormat::TextCss,
            "text/xml" => MsgContentFormat::TextXml,
            "image/png" => MsgContentFormat::ImagePng,
            "image/jpeg" | "image/jpg" => MsgContentFormat::ImageJpeg,
            "image/gif" => MsgContentFormat::ImageGif,
            "image/webp" => MsgContentFormat::ImageWebp,
            "image/svg+xml" | "image/svg" => MsgContentFormat::ImageSvg,
            "image/bmp" => MsgContentFormat::ImageBmp,
            "video/mp4" => MsgContentFormat::VideoMp4,
            "video/webm" => MsgContentFormat::VideoWebm,
            "video/ogg" => MsgContentFormat::VideoOgg,
            "video/quicktime" => MsgContentFormat::VideoQuicktime,
            "video/x-msvideo" | "video/avi" => MsgContentFormat::VideoAvi,
            "audio/mpeg" | "audio/mp3" => MsgContentFormat::AudioMpeg,
            "audio/wav" | "audio/x-wav" => MsgContentFormat::AudioWav,
            "audio/ogg" => MsgContentFormat::AudioOgg,
            "audio/webm" => MsgContentFormat::AudioWebm,
            "audio/aac" => MsgContentFormat::AudioAac,
            "audio/flac" => MsgContentFormat::AudioFlac,
            "application/json" => MsgContentFormat::ApplicationJson,
            "application/xml" => MsgContentFormat::ApplicationXml,
            "application/pdf" => MsgContentFormat::ApplicationPdf,
            "application/zip" => MsgContentFormat::ApplicationZip,
            "application/octet-stream" => MsgContentFormat::ApplicationOctetStream,
            _ => MsgContentFormat::Unknown(s),
        })
    }
}

/// Threading/correlation metadata. Pure message semantics: transport
/// information (which tunnel/hub carried the message) belongs to the delivery
/// layer, never to the immutable message object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicThread {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reply_to: Option<ObjId>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation_id: Option<String>,
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

/// Relation kind of a relation message (`CYFS 标准对象` §16.3).
///
/// Unknown values are kept verbatim so a receiver can still store and display
/// the message without applying any relation semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgRelType {
    Edit,
    Redact,
    Reaction,
    Thread,
    Unknown(String),
}

impl MsgRelType {
    pub fn as_str(&self) -> &str {
        match self {
            MsgRelType::Edit => "edit",
            MsgRelType::Redact => "redact",
            MsgRelType::Reaction => "reaction",
            MsgRelType::Thread => "thread",
            MsgRelType::Unknown(v) => v.as_str(),
        }
    }

    pub fn is_known(&self) -> bool {
        !matches!(self, MsgRelType::Unknown(_))
    }
}

impl From<&str> for MsgRelType {
    fn from(value: &str) -> Self {
        match value {
            "edit" => MsgRelType::Edit,
            "redact" => MsgRelType::Redact,
            "reaction" => MsgRelType::Reaction,
            "thread" => MsgRelType::Thread,
            other => MsgRelType::Unknown(other.to_string()),
        }
    }
}

impl Serialize for MsgRelType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MsgRelType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(MsgRelType::from(s.as_str()))
    }
}

/// Max byte length of a reaction key.
pub const MSG_REACTION_KEY_MAX_BYTES: usize = 64;

/// `relates_to`: this message edits / redacts / reacts to / joins the thread
/// of another `cymsg`. The original message is never modified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsgRelation {
    pub rel: MsgRelType,
    pub target: ObjId,
    /// Only for `reaction`: the reaction content, e.g. one emoji.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,
}

impl MsgRelation {
    pub fn new(rel: MsgRelType, target: ObjId) -> Self {
        Self {
            rel,
            target,
            key: None,
        }
    }

    pub fn reaction(target: ObjId, key: impl Into<String>) -> Self {
        Self {
            rel: MsgRelType::Reaction,
            target,
            key: Some(key.into()),
        }
    }
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Structured mentions. Notification semantics come only from this field,
/// never from `@` text in the body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsgMentions {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub dids: Vec<DID>,
    /// Mentions every participant of the target session.
    #[serde(skip_serializing_if = "is_false", default)]
    pub all: bool,
}

impl MsgMentions {
    pub fn is_empty(&self) -> bool {
        self.dids.is_empty() && !self.all
    }

    fn is_none_or_empty(value: &Option<Self>) -> bool {
        value.as_ref().map_or(true, Self::is_empty)
    }
}

// 注意MsgObject的构造:
//   单聊: from是发起者, to是接受者
//   群聊: from是发起者, to是群组, to_session是群内的具名会话(省略为默认会话)

/// Immutable message object (MsgObject v2, `CYFS 标准对象` §16).
///
/// There is no signature field: a signed message travels as a JWT whose claims
/// are this object (see [`MsgObject::to_jwt`] / [`verify_msg_object_jwt`]);
/// the ObjId is computed from the claims only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsgObject {
    pub from: DID,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    /// Named session under the single target entity (`to[0]/to_session`).
    /// `None` is the default session. Never used together with several `to`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to_session: Option<String>,
    /// Semantic hints only; never a routing key.
    #[serde(skip_serializing_if = "TopicThread::is_empty", default)]
    pub thread: TopicThread,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub relates_to: Option<MsgRelation>,
    #[serde(skip_serializing_if = "MsgMentions::is_none_or_empty", default)]
    pub mentions: Option<MsgMentions>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub workspace: Option<DID>,
    /// Sender-declared creation time, for display only.
    #[serde(skip_serializing_if = "is_zero", default)]
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expires_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub nonce: Option<u64>,
    pub content: MsgContent,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default, flatten)]
    pub meta: BTreeMap<String, serde_json::Value>,
}

impl TopicThread {
    pub fn is_empty(&self) -> bool {
        self.topic.is_none() && self.reply_to.is_none() && self.correlation_id.is_none()
    }
}

impl Default for MsgObject {
    fn default() -> Self {
        Self {
            from: DID::undefined(),
            to: Vec::new(),
            kind: MsgObjKind::default(),
            to_session: None,
            thread: TopicThread::default(),
            relates_to: None,
            mentions: None,
            workspace: None,
            created_at_ms: 0,
            expires_at_ms: None,
            nonce: None,
            content: MsgContent::default(),
            meta: BTreeMap::new(),
        }
    }
}

/// Top-level keys that `meta` must not use: the declared fields, plus `proof`
/// which is reserved since v2 removed it.
pub const MSG_OBJECT_RESERVED_KEYS: &[&str] = &[
    "from",
    "to",
    "kind",
    "to_session",
    "thread",
    "relates_to",
    "mentions",
    "workspace",
    "created_at_ms",
    "expires_at_ms",
    "nonce",
    "content",
    "proof",
];

/// Max char length of `to_session`, same as the MailboxAddress session part.
pub const MSG_SESSION_ID_MAX_CHARS: usize = 200;

/// `to_session` value rules, identical to the MailboxAddress session part:
/// 1-200 chars, no surrounding whitespace, no control chars, not `.`/`..`.
pub fn validate_msg_session_id(session: &str) -> NdnResult<()> {
    if session.is_empty()
        || session.chars().count() > MSG_SESSION_ID_MAX_CHARS
        || session == "."
        || session == ".."
        || session.trim() != session
        || session.chars().any(char::is_control)
    {
        return Err(NdnError::InvalidData(format!(
            "invalid msg to_session: {:?}",
            session
        )));
    }
    Ok(())
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

    /// Object-level validity rules of §16. Deserialization stays lenient (so
    /// stored records always load); receivers call this at ingress and reject
    /// the object on error.
    pub fn validate(&self) -> NdnResult<()> {
        if let Some(session) = self.to_session.as_deref() {
            validate_msg_session_id(session)?;
            if self.to.len() != 1 {
                return Err(NdnError::InvalidData(
                    "msg.to_session requires exactly one msg.to".to_string(),
                ));
            }
        }
        if let Some(mentions) = self.mentions.as_ref() {
            if mentions.is_empty() {
                return Err(NdnError::InvalidData(
                    "msg.mentions must be omitted when empty".to_string(),
                ));
            }
        }
        if let Some(relation) = self.relates_to.as_ref() {
            if relation.target.obj_type != OBJ_TYPE_MSG {
                return Err(NdnError::InvalidData(format!(
                    "msg.relates_to.target must be a {} object",
                    OBJ_TYPE_MSG
                )));
            }
            match (&relation.rel, relation.key.as_deref()) {
                (MsgRelType::Reaction, Some(key)) => {
                    if key.is_empty() || key.len() > MSG_REACTION_KEY_MAX_BYTES {
                        return Err(NdnError::InvalidData(format!(
                            "msg.relates_to.key must be 1-{} bytes",
                            MSG_REACTION_KEY_MAX_BYTES
                        )));
                    }
                }
                (MsgRelType::Reaction, None) => {
                    return Err(NdnError::InvalidData(
                        "reaction requires msg.relates_to.key".to_string(),
                    ));
                }
                (MsgRelType::Unknown(_), _) => {}
                (_, Some(_)) => {
                    return Err(NdnError::InvalidData(
                        "msg.relates_to.key is only allowed for reaction".to_string(),
                    ));
                }
                (_, None) => {}
            }
        }
        if let Some(key) = self
            .meta
            .keys()
            .find(|key| MSG_OBJECT_RESERVED_KEYS.contains(&key.as_str()))
        {
            return Err(NdnError::InvalidData(format!(
                "msg meta key '{}' is reserved",
                key
            )));
        }
        Ok(())
    }

    /// Parse a received MsgObject JSON (or JWT claims), validate it and make
    /// sure it re-serializes to the same canonical JSON. Returns the ObjId
    /// computed from `value` itself.
    pub fn from_json_value_checked(value: serde_json::Value) -> NdnResult<(Self, ObjId)> {
        let (obj_id, _) = try_build_named_object_by_json(OBJ_TYPE_MSG, &value)?;
        let msg: MsgObject = serde_json::from_value(value)
            .map_err(|e| NdnError::DecodeError(format!("invalid MsgObject: {}", e)))?;
        msg.validate()?;
        let normalized = serde_json::to_value(&msg)
            .map_err(|e| NdnError::InvalidData(format!("serialize MsgObject failed: {}", e)))?;
        let (normalized_id, _) = try_build_named_object_by_json(OBJ_TYPE_MSG, &normalized)?;
        if normalized_id != obj_id {
            return Err(NdnError::InvalidData(
                "MsgObject JSON is not in canonical form".to_string(),
            ));
        }
        Ok((msg, obj_id))
    }

    /// Sign as a JWT (`alg = EdDSA`, `kid` = DID URL of a key owned by `from`).
    pub fn to_jwt(&self, key: &EncodingKey, kid: &str) -> NdnResult<String> {
        self.validate()?;
        if kid.is_empty() {
            return Err(NdnError::InvalidParam("msg jwt kid is empty".to_string()));
        }
        let claims = serde_json::to_value(self)
            .map_err(|e| NdnError::Internal(format!("serialize MsgObject failed: {}", e)))?;
        named_obj_to_jwt(&claims, key, Some(kid.to_string()))
    }
}

/// A MsgObject received in JWT form, decoded but not yet signature-checked.
#[derive(Debug, Clone, PartialEq)]
pub struct MsgObjectJwt {
    pub msg: MsgObject,
    pub obj_id: ObjId,
    /// `kid` of the JWT header: DID URL of the signing key.
    pub kid: String,
}

impl MsgObjectJwt {
    /// DID part of `kid` (before `#`). It must equal `msg.from`, unless the
    /// receiver resolves it as a device/agent key authorized by `from`'s DID
    /// Document.
    pub fn kid_did(&self) -> Option<DID> {
        msg_jwt_kid_did(&self.kid)
    }
}

pub fn msg_jwt_kid_did(kid: &str) -> Option<DID> {
    let did = kid.split('#').next()?;
    DID::from_str(did).ok()
}

fn decode_msg_jwt_header(jwt: &str) -> NdnResult<String> {
    let header = jsonwebtoken::decode_header(jwt)
        .map_err(|e| NdnError::DecodeError(format!("decode msg jwt header failed: {}", e)))?;
    if header.alg != Algorithm::EdDSA {
        return Err(NdnError::InvalidData(format!(
            "msg jwt alg must be EdDSA, got {:?}",
            header.alg
        )));
    }
    header
        .kid
        .filter(|kid| !kid.is_empty())
        .ok_or_else(|| NdnError::InvalidData("msg jwt header requires kid".to_string()))
}

/// Decode a MsgObject JWT without checking the signature. Use only where the
/// signature is not relied upon (e.g. display); see [`verify_msg_object_jwt`].
pub fn decode_msg_object_jwt(jwt: &str) -> NdnResult<MsgObjectJwt> {
    let kid = decode_msg_jwt_header(jwt)?;
    let claims = name_lib::decode_jwt_claim_without_verify(jwt)
        .map_err(|e| NdnError::DecodeError(format!("decode msg jwt claims failed: {}", e)))?;
    let (msg, obj_id) = MsgObject::from_json_value_checked(claims)?;
    Ok(MsgObjectJwt { msg, obj_id, kid })
}

/// Verify the JWT signature with the public key the receiver resolved for
/// `kid`, then decode it. The caller still has to make sure that key belongs
/// to `msg.from` (see [`MsgObjectJwt::kid_did`]).
pub fn verify_msg_object_jwt(jwt: &str, public_key: &DecodingKey) -> NdnResult<MsgObjectJwt> {
    let kid = decode_msg_jwt_header(jwt)?;
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    let token = jsonwebtoken::decode::<serde_json::Value>(jwt, public_key, &validation)
        .map_err(|e| NdnError::VerifyError(format!("verify msg jwt failed: {}", e)))?;
    let (msg, obj_id) = MsgObject::from_json_value_checked(token.claims)?;
    Ok(MsgObjectJwt { msg, obj_id, kid })
}

impl NamedObject for MsgObject {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_MSG
    }
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
pub struct ReceiptObj {
    pub obj_id: ObjId,
    pub iss: DID,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub channel: Option<String>,
    pub iat: u64,
    pub status: ReceiptStatus,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
}

impl NamedObject for ReceiptObj {
    fn get_obj_type() -> &'static str {
        OBJ_TYPE_RECEIPT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_named_object_by_json, build_named_object_by_jwt};
    use base64::Engine;
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

        let (id3, _) =
            build_named_object_by_json(OBJ_TYPE_MSG, &serde_json::to_value(&d2).unwrap());
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

    fn assert_receipt_roundtrip_consistency(original: &ReceiptObj) -> ReceiptObj {
        let s1 = serde_json::to_string(original).unwrap();
        let d1: ReceiptObj = serde_json::from_str(&s1).unwrap();
        let s2 = serde_json::to_string(&d1).unwrap();
        let d2: ReceiptObj = serde_json::from_str(&s2).unwrap();

        assert_eq!(d1, d2);

        let v1: serde_json::Value = serde_json::from_str(&s1).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&s2).unwrap();
        assert_eq!(v1, v2);

        let (id0, _) = original.gen_obj_id();
        let (id1, _) = d1.gen_obj_id();
        let (id2, _) = d2.gen_obj_id();
        assert_eq!(id0, id1);
        assert_eq!(id1, id2);
        assert_eq!(id2.obj_type, OBJ_TYPE_RECEIPT);

        let (id3, _) =
            build_named_object_by_json(OBJ_TYPE_RECEIPT, &serde_json::to_value(&d2).unwrap());
        assert_eq!(id2, id3);

        d2
    }

    fn print_receipt_json(case_name: &str, receipt: &ReceiptObj) {
        println!(
            "{} json: {}",
            case_name,
            serde_json::to_string_pretty(receipt).unwrap()
        );
    }

    #[test]
    fn test_msg_case_1_standard_plain_text_message() {
        let mut machine_data = BTreeMap::new();
        machine_data.insert(
            "mime".to_string(),
            CanonValue::String("text/plain".to_string()),
        );
        machine_data.insert("lang".to_string(), CanonValue::String("zh-CN".to_string()));
        machine_data.insert("channel".to_string(), CanonValue::String("dm".to_string()));

        let mut msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::default(),
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
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
        assert_eq!(normalized.kind, MsgObjKind::default());
        assert_eq!(normalized.content.format, Some(MsgContentFormat::TextPlain));
        assert_eq!(normalized.content.refs.len(), 0);
    }

    #[test]
    fn test_msg_case_2_standard_image_message() {
        let image_obj_id = ObjId::new("sha256:1234567890abcdef").unwrap();

        let mut machine_data = BTreeMap::new();
        machine_data.insert(
            "mime".to_string(),
            CanonValue::String("image/png".to_string()),
        );
        machine_data.insert("width".to_string(), CanonValue::U64(1280));
        machine_data.insert("height".to_string(), CanonValue::U64(720));
        machine_data.insert("size".to_string(), CanonValue::U64(376218));

        let msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
            },
            created_at_ms: 1735689615000,
            content: MsgContent {
                title: Some("Image".to_string()),
                format: Some(MsgContentFormat::ImagePng),
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
        assert_eq!(normalized.content.format, Some(MsgContentFormat::ImagePng));
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
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
            },
            created_at_ms: 1735689615000,
            content: MsgContent {
                title: Some("Image".to_string()),
                format: Some(MsgContentFormat::ImagePng),
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
            kind: MsgObjKind::default(),
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                reply_to: Some(image_msg_id.clone()),
                correlation_id: Some("reply-image-1".to_string()),
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
            kind: MsgObjKind::default(),
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
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
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                reply_to: Some(quoted_msg_id.clone()),
                correlation_id: Some("quote-1".to_string()),
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
        machine_data.insert(
            "chat_type".to_string(),
            CanonValue::String("group".to_string()),
        );
        machine_data.insert("member_count".to_string(), CanonValue::U64(3));

        let mut msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![group_did.clone()],
            kind: MsgObjKind::GroupMsg,
            to_session: Some("release".to_string()),
            thread: TopicThread {
                topic: Some("grp-release".to_string()),
                correlation_id: None,
                reply_to: None,
            },
            mentions: Some(MsgMentions {
                dids: vec![did_web("bob.example.com"), did_web("carol.example.com")],
                all: false,
            }),
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
        msg.meta
            .insert("room".to_string(), json!("release-war-room"));
        print_msg_json("case_5_group_chat", &msg);

        msg.validate().unwrap();
        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.to.len(), 1);
        assert_eq!(normalized.to_session.as_deref(), Some("release"));
        assert_eq!(normalized.mentions.as_ref().unwrap().dids.len(), 2);
        let value = serde_json::to_value(&normalized).unwrap();
        assert_eq!(
            value["mentions"],
            json!({"dids": ["did:web:bob.example.com", "did:web:carol.example.com"]})
        );
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.intent.as_ref())
                .map(String::as_str),
            Some("group_chat_text")
        );
        assert_eq!(
            normalized.meta.get("room"),
            Some(&json!("release-war-room"))
        );
    }

    #[test]
    fn test_msg_case_6_minimal_all_optional_none() {
        let msg = MsgObject {
            from: did_web("a.example.com"),
            to: vec![did_web("b.example.com")],
            kind: MsgObjKind::default(),
            to_session: None,
            thread: TopicThread::default(),
            relates_to: None,
            mentions: None,
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
            meta: BTreeMap::new(),
        };
        print_msg_json("case_6_minimal_none", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.workspace, None);
        assert_eq!(normalized.expires_at_ms, None);
        assert_eq!(normalized.nonce, None);
        assert_eq!(normalized.to_session, None);
        assert_eq!(normalized.relates_to, None);
        assert_eq!(normalized.mentions, None);
        assert_eq!(normalized.content.title, None);
        assert_eq!(normalized.content.format, None);
        assert_eq!(normalized.content.machine, None);
        assert_eq!(normalized.content.refs.len(), 0);
        assert!(normalized.meta.is_empty());
    }

    #[test]
    fn test_msg_case_7_voice_message() {
        let voice_obj_id = ObjId::new("sha256:a1b2c3d4e5f6789012345678abcdef").unwrap();

        let mut machine_data = BTreeMap::new();
        machine_data.insert(
            "mime".to_string(),
            CanonValue::String("audio/mpeg".to_string()),
        );
        machine_data.insert("duration_sec".to_string(), CanonValue::U64(15));
        machine_data.insert("size".to_string(), CanonValue::U64(245760));

        let msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
            },
            created_at_ms: 1735689650000,
            content: MsgContent {
                title: Some("Voice Message".to_string()),
                format: Some(MsgContentFormat::AudioMpeg),
                content: "[voice]".to_string(),
                machine: Some(MachineContent {
                    intent: Some("chat_voice".to_string()),
                    data: machine_data,
                }),
                refs: vec![RefItem {
                    role: RefRole::Output,
                    target: RefTarget::DataObj {
                        obj_id: voice_obj_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", voice_obj_id.to_string())),
                    },
                    label: Some("audio/mpeg".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_7_voice", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(normalized.content.format, Some(MsgContentFormat::AudioMpeg));
        assert_eq!(normalized.content.refs.len(), 1);
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.intent.as_ref())
                .map(String::as_str),
            Some("chat_voice")
        );
    }

    #[test]
    fn test_msg_case_8_downloadable_file() {
        let file_obj_id = ObjId::new("sha256:f1e2a3b4c5d6789012345678abcdef").unwrap();

        let mut machine_data = BTreeMap::new();
        machine_data.insert(
            "mime".to_string(),
            CanonValue::String("application/octet-stream".to_string()),
        );
        machine_data.insert(
            "filename".to_string(),
            CanonValue::String("report_2024.xlsx".to_string()),
        );
        machine_data.insert("size".to_string(), CanonValue::U64(1024000));

        let msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
            },
            created_at_ms: 1735689660000,
            content: MsgContent {
                title: Some("Report File".to_string()),
                format: Some(MsgContentFormat::ApplicationOctetStream),
                content: "[file] report_2024.xlsx".to_string(),
                machine: Some(MachineContent {
                    intent: Some("chat_file".to_string()),
                    data: machine_data,
                }),
                refs: vec![RefItem {
                    role: RefRole::Output,
                    target: RefTarget::DataObj {
                        obj_id: file_obj_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", file_obj_id.to_string())),
                    },
                    label: Some("application/octet-stream".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_8_downloadable_file", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(
            normalized.content.format,
            Some(MsgContentFormat::ApplicationOctetStream)
        );
        assert_eq!(normalized.content.refs.len(), 1);
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.data.get("filename"))
                .and_then(|v| match v {
                    CanonValue::String(s) => Some(s.as_str()),
                    _ => None,
                }),
            Some("report_2024.xlsx")
        );
    }

    #[test]
    fn test_msg_case_9_pdf_message() {
        let pdf_obj_id = ObjId::new("sha256:abcdef1234567890abcdef12345678").unwrap();

        let mut machine_data = BTreeMap::new();
        machine_data.insert(
            "mime".to_string(),
            CanonValue::String("application/pdf".to_string()),
        );
        machine_data.insert(
            "filename".to_string(),
            CanonValue::String("design_spec.pdf".to_string()),
        );
        machine_data.insert("size".to_string(), CanonValue::U64(524288));
        machine_data.insert("page_count".to_string(), CanonValue::U64(12));

        let msg = MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("bob.example.com")],
            kind: MsgObjKind::Deliver,
            thread: TopicThread {
                topic: Some("dm-alice-bob".to_string()),
                ..TopicThread::default()
            },
            created_at_ms: 1735689670000,
            content: MsgContent {
                title: Some("Design Spec".to_string()),
                format: Some(MsgContentFormat::ApplicationPdf),
                content: "[pdf] design_spec.pdf".to_string(),
                machine: Some(MachineContent {
                    intent: Some("chat_document".to_string()),
                    data: machine_data,
                }),
                refs: vec![RefItem {
                    role: RefRole::Output,
                    target: RefTarget::DataObj {
                        obj_id: pdf_obj_id.clone(),
                        uri_hint: Some(format!("cyfs://{}", pdf_obj_id.to_string())),
                    },
                    label: Some("application/pdf".to_string()),
                }],
            },
            ..MsgObject::default()
        };
        print_msg_json("case_9_pdf", &msg);

        let normalized = assert_msg_roundtrip_consistency(&msg);
        assert_eq!(
            normalized.content.format,
            Some(MsgContentFormat::ApplicationPdf)
        );
        assert_eq!(normalized.content.refs.len(), 1);
        assert_eq!(
            normalized
                .content
                .machine
                .as_ref()
                .and_then(|m| m.intent.as_ref())
                .map(String::as_str),
            Some("chat_document")
        );
    }

    #[test]
    fn test_msg_content_format_unknown_fallback() {
        let raw = json!({
            "from": "did:web:a.example.com",
            "to": ["did:web:b.example.com"],
            "kind": "chat",
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
        let receipt = ReceiptObj {
            obj_id: ObjId::new("cymsg:1234567890abcdef").unwrap(),
            iss: did_web("msg-receipt.example.com"),
            channel: None,
            iat: 1735689700000,
            status: ReceiptStatus::Accepted,
            reason: None,
        };
        print_receipt_json("receipt_minimal", &receipt);

        let value = serde_json::to_value(&receipt).unwrap();
        assert!(value.get("reason").is_none());
        assert_eq!(value["iss"], json!("did:web:msg-receipt.example.com"));
        assert!(value.get("channel").is_none());

        let normalized = assert_receipt_roundtrip_consistency(&receipt);
        assert_eq!(normalized.iss, did_web("msg-receipt.example.com"));
        assert_eq!(normalized.channel, None);
        assert_eq!(normalized.iat, 1735689700000);
        assert_eq!(normalized.status, ReceiptStatus::Accepted);
        assert_eq!(normalized.reason, None);
    }

    #[test]
    fn test_msg_receipt_obj_with_issuer_and_reason() {
        let receipt = ReceiptObj {
            obj_id: ObjId::new("cymsg:abcdef1234567890").unwrap(),
            iss: did_web("inbox-router.example.com"),
            channel: Some("group".to_string()),
            iat: 1735689710000,
            status: ReceiptStatus::Rejected,
            reason: Some("policy_denied".to_string()),
        };
        print_receipt_json("receipt_with_issuer_reason", &receipt);

        let normalized = assert_receipt_roundtrip_consistency(&receipt);
        assert_eq!(normalized.iss, did_web("inbox-router.example.com"));
        assert_eq!(normalized.channel, Some("group".to_string()));
        assert_eq!(normalized.iat, 1735689710000);
        assert_eq!(normalized.status, ReceiptStatus::Rejected);
        assert_eq!(normalized.reason, Some("policy_denied".to_string()));
    }

    #[test]
    fn test_msg_receipt_obj_from_json_and_obj_id_consistency() {
        let raw = json!({
            "obj_id": "cymsg:00112233445566778899aabbccddeeff",
            "iss": "did:web:inbox.example.com",
            "channel": "group",
            "iat": 1735689720000u64,
            "status": "quarantined",
            "reason": "needs_manual_review"
        });

        let receipt: ReceiptObj = serde_json::from_value(raw).unwrap();
        assert_eq!(receipt.iss, did_web("inbox.example.com"));
        assert_eq!(receipt.channel, Some("group".to_string()));
        assert_eq!(receipt.iat, 1735689720000u64);
        assert_eq!(receipt.status, ReceiptStatus::Quarantined);
        assert_eq!(receipt.reason, Some("needs_manual_review".to_string()));
        print_receipt_json("receipt_from_json", &receipt);

        let (obj_id, _) = receipt.gen_obj_id();
        assert_eq!(obj_id.obj_type, OBJ_TYPE_RECEIPT);

        let (obj_id2, _) =
            build_named_object_by_json(OBJ_TYPE_RECEIPT, &serde_json::to_value(&receipt).unwrap());
        assert_eq!(obj_id, obj_id2);
    }

    fn base_group_msg() -> MsgObject {
        MsgObject {
            from: did_web("alice.example.com"),
            to: vec![did_web("team.example.com")],
            kind: MsgObjKind::GroupMsg,
            to_session: Some("release".to_string()),
            created_at_ms: 1700000000000,
            nonce: Some(1),
            content: MsgContent {
                content: "hello".to_string(),
                ..MsgContent::default()
            },
            ..MsgObject::default()
        }
    }

    #[test]
    fn test_msg_checked_rejects_canonicalization_errors() {
        let mut raw = serde_json::to_value(base_group_msg()).unwrap();
        raw["extension"] = serde_json::from_str(r#"{"n":1e400}"#).unwrap();
        assert!(matches!(
            MsgObject::from_json_value_checked(raw),
            Err(NdnError::InvalidData(_))
        ));
    }

    #[test]
    fn test_msg_v2_to_session_rules() {
        let msg = base_group_msg();
        msg.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&msg).unwrap()["to_session"],
            json!("release")
        );

        let mut multi = msg.clone();
        multi.to.push(did_web("other.example.com"));
        assert!(multi.validate().is_err());

        for bad in ["", " release", "release ", ".", "..", "a\u{7}b"] {
            let mut bad_msg = msg.clone();
            bad_msg.to_session = Some(bad.to_string());
            assert!(bad_msg.validate().is_err(), "{:?}", bad);
        }
        let mut long = msg.clone();
        long.to_session = Some("会".repeat(MSG_SESSION_ID_MAX_CHARS));
        long.validate().unwrap();
        long.to_session = Some("会".repeat(MSG_SESSION_ID_MAX_CHARS + 1));
        assert!(long.validate().is_err());

        // to_session is part of the ObjId.
        let mut default_session = msg.clone();
        default_session.to_session = None;
        assert_ne!(msg.gen_obj_id().0, default_session.gen_obj_id().0);
    }

    #[test]
    fn test_msg_v2_relations() {
        let target = base_group_msg().gen_obj_id().0;

        let mut reaction = base_group_msg();
        reaction.from = did_web("bob.example.com");
        reaction.content = MsgContent::default();
        reaction.relates_to = Some(MsgRelation::reaction(target.clone(), "👍"));
        reaction.validate().unwrap();
        let value = serde_json::to_value(&reaction).unwrap();
        assert_eq!(value["relates_to"]["rel"], json!("reaction"));
        assert_eq!(value["relates_to"]["key"], json!("👍"));
        assert_eq!(value["content"], json!({}));
        assert_msg_roundtrip_consistency(&reaction);

        let mut no_key = reaction.clone();
        no_key.relates_to.as_mut().unwrap().key = None;
        assert!(no_key.validate().is_err());
        let mut long_key = reaction.clone();
        long_key.relates_to.as_mut().unwrap().key =
            Some("x".repeat(MSG_REACTION_KEY_MAX_BYTES + 1));
        assert!(long_key.validate().is_err());

        let mut redact = base_group_msg();
        redact.relates_to = Some(MsgRelation::new(MsgRelType::Redact, target.clone()));
        redact.validate().unwrap();
        let mut redact_with_key = redact.clone();
        redact_with_key.relates_to.as_mut().unwrap().key = Some("x".to_string());
        assert!(redact_with_key.validate().is_err());

        let mut wrong_target = redact.clone();
        wrong_target.relates_to.as_mut().unwrap().target =
            ObjId::new("cyfile:1234567890abcdef").unwrap();
        assert!(wrong_target.validate().is_err());

        // Unknown rel values are preserved and still valid objects.
        let mut raw = serde_json::to_value(&redact).unwrap();
        raw["relates_to"]["rel"] = json!("pin");
        let (unknown, id) = MsgObject::from_json_value_checked(raw.clone()).unwrap();
        assert_eq!(
            unknown.relates_to.as_ref().unwrap().rel,
            MsgRelType::Unknown("pin".to_string())
        );
        assert_eq!(id, build_named_object_by_json(OBJ_TYPE_MSG, &raw).0);
        assert_eq!(serde_json::to_value(&unknown).unwrap(), raw);
    }

    #[test]
    fn test_msg_v2_mentions_canonical_form() {
        let mut msg = base_group_msg();
        msg.mentions = Some(MsgMentions::default());
        // Empty mentions never serialize, but such an object is invalid.
        assert!(serde_json::to_value(&msg)
            .unwrap()
            .get("mentions")
            .is_none());
        assert!(msg.validate().is_err());

        msg.mentions = Some(MsgMentions {
            dids: Vec::new(),
            all: true,
        });
        msg.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&msg).unwrap()["mentions"],
            json!({"all": true})
        );

        let mut raw = serde_json::to_value(base_group_msg()).unwrap();
        raw["mentions"] = json!({});
        assert!(MsgObject::from_json_value_checked(raw).is_err());

        let mut raw = serde_json::to_value(base_group_msg()).unwrap();
        raw["mentions"] = json!({"dids": [], "all": false});
        assert!(MsgObject::from_json_value_checked(raw).is_err());
    }

    #[test]
    fn test_msg_v2_proof_is_reserved() {
        let mut raw = serde_json::to_value(base_group_msg()).unwrap();
        raw["proof"] = json!("proof-001");
        // Lenient decode keeps it in meta, but the object is rejected.
        let msg: MsgObject = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(msg.meta.get("proof"), Some(&json!("proof-001")));
        assert!(msg.validate().is_err());
        assert!(MsgObject::from_json_value_checked(raw).is_err());

        let mut msg = base_group_msg();
        msg.meta.insert("to_session".to_string(), json!("x"));
        assert!(msg.validate().is_err());
    }

    #[test]
    fn test_msg_v2_without_new_fields_keeps_v1_obj_id() {
        let raw = json!({
            "from": "did:web:alice.example.com",
            "to": ["did:web:bob.example.com"],
            "kind": "chat",
            "thread": {"topic": "release", "correlation_id": "corr-001"},
            "created_at_ms": 1700000000000u64,
            "nonce": 7,
            "content": {"content": "hi"},
            "lang": "zh-CN"
        });
        let (msg, id) = MsgObject::from_json_value_checked(raw.clone()).unwrap();
        assert_eq!(msg.gen_obj_id().0, id);
        assert_eq!(build_named_object_by_json(OBJ_TYPE_MSG, &raw).0, id);
    }

    #[test]
    fn test_msg_v2_non_canonical_json_is_rejected() {
        let mut raw = serde_json::to_value(base_group_msg()).unwrap();
        raw["workspace"] = serde_json::Value::Null;
        assert!(MsgObject::from_json_value_checked(raw).is_err());
    }

    const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIJBRONAzbwpIOwm0ugIQNyZJrDXxZF7HoPWAZesMedOr\n-----END PRIVATE KEY-----\n";
    const TEST_PUBLIC_KEY_X: &str = "T4Quc1L6Ogu4N2tTKOvneV1yYnBcmhP89B_RsuFsJZ8";

    fn test_keys() -> (EncodingKey, DecodingKey) {
        let private_key = EncodingKey::from_ed_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).unwrap();
        let jwk: jsonwebtoken::jwk::Jwk = serde_json::from_value(json!({
            "kty": "OKP",
            "crv": "Ed25519",
            "x": TEST_PUBLIC_KEY_X
        }))
        .unwrap();
        (private_key, DecodingKey::from_jwk(&jwk).unwrap())
    }

    #[test]
    fn test_msg_v2_jwt_form() {
        let (private_key, public_key) = test_keys();
        let mut msg = base_group_msg();
        msg.mentions = Some(MsgMentions {
            dids: vec![did_web("bob.example.com")],
            all: false,
        });
        let kid = "did:web:alice.example.com#key-1";
        let jwt = msg.to_jwt(&private_key, kid).unwrap();

        // ObjId is computed from the claims only: JSON form and JWT form agree.
        let (json_id, _) = msg.gen_obj_id();
        assert_eq!(
            build_named_object_by_jwt(OBJ_TYPE_MSG, &jwt).unwrap().0,
            json_id
        );

        let verified = verify_msg_object_jwt(&jwt, &public_key).unwrap();
        assert_eq!(verified.msg, msg);
        assert_eq!(verified.obj_id, json_id);
        assert_eq!(verified.kid, kid);
        assert_eq!(verified.kid_did(), Some(did_web("alice.example.com")));
        assert_eq!(decode_msg_object_jwt(&jwt).unwrap(), verified);

        // Tampered claims fail verification.
        let parts: Vec<&str> = jwt.split('.').collect();
        let mut tampered = msg.clone();
        tampered.to_session = Some("other".to_string());
        let tampered_claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&tampered).unwrap());
        let tampered_jwt = format!("{}.{}.{}", parts[0], tampered_claims, parts[2]);
        assert!(verify_msg_object_jwt(&tampered_jwt, &public_key).is_err());

        // kid is mandatory.
        let no_kid =
            named_obj_to_jwt(&serde_json::to_value(&msg).unwrap(), &private_key, None).unwrap();
        assert!(verify_msg_object_jwt(&no_kid, &public_key).is_err());
        assert!(decode_msg_object_jwt(&no_kid).is_err());

        // Invalid objects are not signed.
        let mut invalid = msg.clone();
        invalid.to.push(did_web("x.example.com"));
        assert!(invalid.to_jwt(&private_key, kid).is_err());
    }
}
