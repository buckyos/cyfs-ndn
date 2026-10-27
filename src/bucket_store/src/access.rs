//! §4.4: actions, the host's identity policy (RBAC, first layer), explicit
//! grants (second layer), capability tokens, and the data-policy rules the
//! bucket applies after either layer allowed a request.

use crate::db::IndexDb;
use crate::error::{BucketError, BucketResult};
use crate::types::*;
use rand::RngCore;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    List,
    Read,
    Create,
    Write,
    Delete,
    Manage,
}

impl Action {
    pub const ALL: [Action; 6] = [Action::List, Action::Read, Action::Create, Action::Write, Action::Delete, Action::Manage];

    pub fn as_str(&self) -> &'static str {
        match self {
            Action::List => "list",
            Action::Read => "read",
            Action::Create => "create",
            Action::Write => "write",
            Action::Delete => "delete",
            Action::Manage => "manage",
        }
    }

    fn bit(&self) -> u8 {
        1 << (*self as u8)
    }
}

impl FromStr for Action {
    type Err = BucketError;
    fn from_str(s: &str) -> BucketResult<Self> {
        Action::ALL
            .into_iter()
            .find(|a| a.as_str() == s)
            .ok_or_else(|| BucketError::InvalidArgument(format!("unknown action '{s}'")))
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A set of actions; `write` implies `create`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionSet(u8);

impl ActionSet {
    pub fn all() -> Self {
        Action::ALL.iter().copied().collect()
    }

    pub fn contains(&self, a: Action) -> bool {
        self.0 & a.bit() != 0 || (a == Action::Create && self.0 & Action::Write.bit() != 0)
    }

    pub fn insert(&mut self, a: Action) {
        self.0 |= a.bit();
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = Action> + '_ {
        Action::ALL.into_iter().filter(move |a| self.0 & a.bit() != 0)
    }

    pub fn is_subset(&self, other: &ActionSet) -> bool {
        self.iter().all(|a| other.contains(a))
    }

    pub fn to_csv(&self) -> String {
        self.iter().map(|a| a.as_str()).collect::<Vec<_>>().join(",")
    }

    pub fn parse_csv(s: &str) -> BucketResult<Self> {
        s.split(',').filter(|p| !p.is_empty()).map(|p| p.trim().parse::<Action>()).collect()
    }
}

impl FromIterator<Action> for ActionSet {
    fn from_iter<I: IntoIterator<Item = Action>>(iter: I) -> Self {
        let mut set = ActionSet::default();
        for a in iter {
            set.insert(a);
        }
        set
    }
}

impl<'a> FromIterator<&'a Action> for ActionSet {
    fn from_iter<I: IntoIterator<Item = &'a Action>>(iter: I) -> Self {
        iter.into_iter().copied().collect()
    }
}

impl Serialize for ActionSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.iter().collect::<Vec<_>>().serialize(s)
    }
}

impl<'de> Deserialize<'de> for ActionSet {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Vec::<Action>::deserialize(d)?.into_iter().collect())
    }
}

/// The host's identity policy (RBAC). `key` is the key or prefix the request touches.
#[async_trait::async_trait]
pub trait IdentityPolicy: Send + Sync {
    async fn allowed(&self, bucket: &BucketInfo, key: &str, action: Action) -> bool;

    /// §6.4 (a)/(c): the caller uploaded or registered `obj_id`, or may read any object.
    async fn holds(&self, obj_id: &str) -> bool;
}

/// A policy that allows everything (trusted internal callers, tests).
pub struct AllowAll;

#[async_trait::async_trait]
impl IdentityPolicy for AllowAll {
    async fn allowed(&self, _: &BucketInfo, _: &str, _: Action) -> bool {
        true
    }
    async fn holds(&self, _: &str) -> bool {
        true
    }
}

/// A policy that allows nothing (anonymous, token-only callers).
pub struct DenyAll;

#[async_trait::async_trait]
impl IdentityPolicy for DenyAll {
    async fn allowed(&self, _: &BucketInfo, _: &str, _: Action) -> bool {
        false
    }
    async fn holds(&self, _: &str) -> bool {
        false
    }
}

/// Who is calling. The lib does not know users, apps or agents; it only matches
/// these strings against grants, tokens and binding creators.
#[derive(Clone)]
pub struct Caller {
    pub user: Option<String>,
    pub app: Option<String>,
    pub agent: Option<String>,
    pub groups: Vec<String>,
    /// An authenticated user of this Zone (matches `zone_users` grants).
    pub zone_user: bool,
    /// The user acting through their own system UI rather than an App.
    pub user_direct: bool,
    /// A sudo session (required to delete a documents bucket).
    pub sudo: bool,
    /// `bkt_` capability token presented with the request.
    pub token: Option<String>,
    pub identity: Arc<dyn IdentityPolicy>,
    /// The bucket service itself (lifecycle, reconcile): no data-policy rules apply.
    pub internal: bool,
}

impl fmt::Debug for Caller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Caller")
            .field("user", &self.user)
            .field("app", &self.app)
            .field("agent", &self.agent)
            .field("user_direct", &self.user_direct)
            .finish()
    }
}

impl Caller {
    /// Internal maintenance (lifecycle, reconcile, restore).
    pub fn system() -> Caller {
        Caller {
            user: None,
            app: Some("system".into()),
            agent: None,
            groups: Vec::new(),
            zone_user: false,
            user_direct: true,
            sudo: true,
            token: None,
            identity: Arc::new(AllowAll),
            internal: true,
        }
    }

    pub fn anonymous(token: Option<String>) -> Caller {
        Caller {
            user: None,
            app: None,
            agent: None,
            groups: Vec::new(),
            zone_user: false,
            user_direct: false,
            sudo: false,
            token,
            identity: Arc::new(DenyAll),
            internal: false,
        }
    }

    /// `created_by` / `granted_by` value: `<user>|<app or agent>`.
    pub fn principal(&self) -> String {
        format!(
            "{}|{}",
            self.user.as_deref().unwrap_or("-"),
            self.agent.as_deref().or(self.app.as_deref()).unwrap_or("-")
        )
    }

    fn grantees(&self, bucket: &BucketInfo) -> Vec<(GranteeKind, String)> {
        let mut out = Vec::new();
        if let Some(user) = &self.user {
            out.push((GranteeKind::User, user.clone()));
        }
        // An app grant on a user's bucket lets that app act for this user only.
        if let Some(app) = &self.app {
            if bucket.scope == BucketScope::Zone || self.user.as_deref() == Some(bucket.owner.as_str()) {
                out.push((GranteeKind::App, app.clone()));
            }
        }
        if let Some(agent) = &self.agent {
            out.push((GranteeKind::Agent, agent.clone()));
        }
        for g in &self.groups {
            out.push((GranteeKind::Group, g.clone()));
        }
        if self.zone_user {
            out.push((GranteeKind::ZoneUsers, String::new()));
        }
        out
    }
}

fn principal_parts(p: &str) -> (&str, &str) {
    p.split_once('|').unwrap_or((p, "-"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GranteeKind {
    App,
    User,
    Group,
    ZoneUsers,
    Agent,
}

impl GranteeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::App => "app",
            Self::User => "user",
            Self::Group => "group",
            Self::ZoneUsers => "zone_users",
            Self::Agent => "agent",
        }
    }
}

impl FromStr for GranteeKind {
    type Err = BucketError;
    fn from_str(s: &str) -> BucketResult<Self> {
        Ok(match s {
            "app" => Self::App,
            "user" => Self::User,
            "group" => Self::Group,
            "zone_users" => Self::ZoneUsers,
            "agent" => Self::Agent,
            other => return Err(BucketError::InvalidArgument(format!("unknown grantee kind '{other}'"))),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grantee {
    pub kind: GranteeKind,
    #[serde(default)]
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantRequest {
    pub grantee: Grantee,
    #[serde(default)]
    pub prefix: String,
    pub actions: ActionSet,
    #[serde(default)]
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub grant_id: String,
    pub bucket_id: BucketId,
    pub grantee: Grantee,
    pub prefix: String,
    pub actions: ActionSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub granted_by: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRequest {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    pub actions: ActionSet,
    pub expires_at: i64,
    #[serde(default)]
    pub max_uses: Option<u64>,
    #[serde(default)]
    pub bind_user_id: Option<String>,
    #[serde(default)]
    pub bind_app_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessToken {
    pub token_id: String,
    pub bucket_id: BucketId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub actions: ActionSet,
    pub expires_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u64>,
    pub uses: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_app_id: Option<String>,
    pub created_by: String,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedToken {
    pub token_id: String,
    /// `bkt_…`; shown once, only its hash is kept.
    pub token: String,
    /// Data-plane path (`/bucket/v1/<bucket_id>/<key or prefix>?token=…`).
    pub path: String,
    pub expires_at: i64,
}

/// How a request was allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Via {
    Identity,
    Grant(String),
    Token(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub action: Action,
    pub via: Via,
    /// For `list` allowed by a prefix grant or token: the listing is narrowed to it.
    pub narrow_prefix: Option<String>,
}

/// What a request touches: one key, or the keys under a prefix (`list`).
#[derive(Debug, Clone, Copy)]
pub enum Target<'a> {
    Key(&'a str),
    Prefix(&'a str),
}

impl Target<'_> {
    fn path(&self) -> &str {
        match self {
            Target::Key(k) | Target::Prefix(k) => k,
        }
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

pub(crate) fn token_hash(token: &str) -> String {
    name_lib::bearer_token::digest(token)
}

pub const ACCESS_TOKEN_PREFIX: &str = "bkt";

/// Does a grant/token scope (`prefix`) cover the target? Returns the narrowing prefix for lists.
fn covers(scope_prefix: &str, target: Target, action: Action) -> Option<Option<String>> {
    match target {
        Target::Key(k) => k.starts_with(scope_prefix).then_some(None),
        Target::Prefix(p) if p.starts_with(scope_prefix) => Some(None),
        Target::Prefix(p) if action == Action::List && scope_prefix.starts_with(p) => Some(Some(scope_prefix.to_string())),
        Target::Prefix(_) => None,
    }
}

pub(crate) struct Access<'a> {
    pub db: &'a IndexDb,
}

impl Access<'_> {
    /// First of `actions` the caller may perform on `target`, and how.
    pub async fn authorize_any(
        &self,
        bucket: &BucketInfo,
        caller: &Caller,
        actions: &[Action],
        target: Target<'_>,
        consume_token: bool,
    ) -> BucketResult<Decision> {
        for &action in actions {
            if caller.identity.allowed(bucket, target.path(), action).await {
                return Ok(Decision { action, via: Via::Identity, narrow_prefix: None });
            }
        }
        if let Some(d) = self.by_grant(bucket, caller, actions, target)? {
            return Ok(d);
        }
        if let Some(token) = &caller.token {
            if let Some(d) = self.by_token(bucket, caller, token, actions, target, consume_token)? {
                return Ok(d);
            }
        }
        if consume_token && actions.iter().any(|a| !matches!(a, Action::List | Action::Read)) {
            log::info!(
                "bucket write denied: {} on {}/{} for {}",
                actions.iter().map(|a| a.as_str()).collect::<Vec<_>>().join("|"),
                bucket.bucket_id,
                target.path(),
                caller.principal()
            );
        }
        Err(BucketError::PermissionDenied(format!(
            "{} on {}",
            actions.iter().map(|a| a.as_str()).collect::<Vec<_>>().join("|"),
            bucket.bucket_id
        )))
    }

    pub async fn authorize(&self, bucket: &BucketInfo, caller: &Caller, action: Action, target: Target<'_>) -> BucketResult<Decision> {
        self.authorize_any(bucket, caller, &[action], target, true).await
    }

    fn by_grant(&self, bucket: &BucketInfo, caller: &Caller, actions: &[Action], target: Target) -> BucketResult<Option<Decision>> {
        let now = now_ms();
        let grantees = caller.grantees(bucket);
        if grantees.is_empty() {
            return Ok(None);
        }
        let grants = self.list_grants(&bucket.bucket_id)?;
        for &action in actions {
            for g in &grants {
                if g.expires_at.is_some_and(|t| t <= now) || !g.actions.contains(action) {
                    continue;
                }
                if !grantees.iter().any(|(k, id)| *k == g.grantee.kind && (*k == GranteeKind::ZoneUsers || *id == g.grantee.id)) {
                    continue;
                }
                if let Some(narrow) = covers(&g.prefix, target, action) {
                    return Ok(Some(Decision { action, via: Via::Grant(g.grant_id.clone()), narrow_prefix: narrow }));
                }
            }
        }
        Ok(None)
    }

    fn by_token(
        &self,
        bucket: &BucketInfo,
        caller: &Caller,
        token: &str,
        actions: &[Action],
        target: Target,
        consume: bool,
    ) -> BucketResult<Option<Decision>> {
        let Some(t) = self.find_token(token)? else { return Err(BucketError::TokenInvalid) };
        let now = now_ms();
        if t.revoked_at.is_some() || t.expires_at <= now || t.max_uses.is_some_and(|m| t.uses >= m) {
            return Err(BucketError::TokenInvalid);
        }
        if t.bucket_id != bucket.bucket_id {
            return Ok(None);
        }
        if t.bind_user_id.as_ref().is_some_and(|u| caller.user.as_ref() != Some(u))
            || t.bind_app_id.as_ref().is_some_and(|a| caller.app.as_ref() != Some(a))
        {
            return Ok(None);
        }
        for &action in actions {
            if !t.actions.contains(action) {
                continue;
            }
            let narrow = match (&t.key, &t.prefix) {
                (Some(k), _) => match target {
                    Target::Key(tk) if tk == k => Some(None),
                    _ => None,
                },
                (None, Some(p)) => covers(p, target, action),
                (None, None) => None,
            };
            if let Some(narrow) = narrow {
                if consume {
                    self.db.write(|tx| {
                        tx.execute("UPDATE bucket_access_tokens SET uses = uses + 1 WHERE token_id = ?1", params![t.token_id])?;
                        Ok(())
                    })?;
                }
                return Ok(Some(Decision { action, via: Via::Token(t.token_id.clone()), narrow_prefix: narrow }));
            }
        }
        Ok(None)
    }

    // ---------- grants ----------

    pub fn list_grants(&self, bucket_id: &BucketId) -> BucketResult<Vec<Grant>> {
        self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT grant_id, grantee_kind, grantee_id, prefix, actions, expires_at, granted_by, created_at
                 FROM bucket_grants WHERE bucket_id = ?1 ORDER BY created_at",
            )?;
            let rows = stmt.query_map(params![bucket_id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (grant_id, kind, id, prefix, actions, expires_at, granted_by, created_at) = row?;
                out.push(Grant {
                    grant_id,
                    bucket_id: bucket_id.clone(),
                    grantee: Grantee { kind: kind.parse()?, id },
                    prefix,
                    actions: ActionSet::parse_csv(&actions)?,
                    expires_at,
                    granted_by,
                    created_at,
                });
            }
            Ok(out)
        })
    }

    pub async fn grant(&self, bucket: &BucketInfo, caller: &Caller, req: GrantRequest) -> BucketResult<Grant> {
        self.require_manage(bucket, caller).await?;
        if req.actions.is_empty() || req.actions.contains_exact(Action::Manage) {
            return Err(BucketError::InvalidArgument("a grant gives some of list/read/create/write/delete, never manage".into()));
        }
        if bucket.data_policy.user_owned() && req.grantee.kind == GranteeKind::App && req.actions.contains_exact(Action::Delete) {
            return Err(BucketError::PolicyViolation("user documents cannot be deleted by another app".into()));
        }
        if req.grantee.kind != GranteeKind::ZoneUsers && req.grantee.id.is_empty() {
            return Err(BucketError::InvalidArgument("grantee.id is required".into()));
        }
        if !req.prefix.is_empty() {
            validate_key(&req.prefix)?;
        }
        for action in req.actions.iter() {
            if !matches!(self.authorize_any(bucket, caller, &[action], Target::Prefix(&req.prefix), false).await, Ok(Decision { via: Via::Identity, .. })) {
                return Err(BucketError::PermissionDenied(format!("cannot grant '{action}' that the granter does not hold")));
            }
        }
        let grant = Grant {
            grant_id: format!("g_{}", random_hex(12)),
            bucket_id: bucket.bucket_id.clone(),
            grantee: Grantee {
                id: if req.grantee.kind == GranteeKind::ZoneUsers { String::new() } else { req.grantee.id.clone() },
                kind: req.grantee.kind,
            },
            prefix: req.prefix,
            actions: req.actions,
            expires_at: req.expires_at,
            granted_by: caller.principal(),
            created_at: now_ms(),
        };
        self.db.write(|tx| {
            tx.execute(
                "INSERT INTO bucket_grants (grant_id, bucket_id, grantee_kind, grantee_id, prefix, actions, expires_at, granted_by, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    grant.grant_id,
                    grant.bucket_id.to_string(),
                    grant.grantee.kind.as_str(),
                    grant.grantee.id,
                    grant.prefix,
                    grant.actions.to_csv(),
                    grant.expires_at,
                    grant.granted_by,
                    grant.created_at
                ],
            )?;
            Ok(())
        })?;
        log::info!("bucket grant {} on {} to {:?} ({}) by {}", grant.grant_id, grant.bucket_id, grant.grantee, grant.actions.to_csv(), grant.granted_by);
        Ok(grant)
    }

    pub fn grant_bucket(&self, grant_id: &str) -> BucketResult<BucketId> {
        let id: Option<String> = self.db.read(|conn| {
            Ok(conn.query_row("SELECT bucket_id FROM bucket_grants WHERE grant_id = ?1", params![grant_id], |r| r.get(0)).optional()?)
        })?;
        id.ok_or_else(|| BucketError::InvalidArgument(format!("no grant {grant_id}")))?.parse()
    }

    pub async fn revoke(&self, bucket: &BucketInfo, caller: &Caller, grant_id: &str) -> BucketResult<()> {
        self.require_manage(bucket, caller).await?;
        self.db.write(|tx| {
            tx.execute("DELETE FROM bucket_grants WHERE grant_id = ?1 AND bucket_id = ?2", params![grant_id, bucket.bucket_id.to_string()])?;
            Ok(())
        })?;
        log::info!("bucket grant {} on {} revoked by {}", grant_id, bucket.bucket_id, caller.principal());
        Ok(())
    }

    // ---------- tokens ----------

    fn find_token(&self, token: &str) -> BucketResult<Option<AccessToken>> {
        if !name_lib::bearer_token::has_form(ACCESS_TOKEN_PREFIX, token) {
            return Ok(None);
        }
        let hash = token_hash(token);
        self.db.read(|conn| {
            conn.query_row(&format!("SELECT {TOKEN_COLS} FROM bucket_access_tokens WHERE token_hash = ?1"), params![hash], token_from_row)
                .optional()?
                .transpose()
        })
    }

    pub fn token_bucket(&self, token_id: &str) -> BucketResult<BucketId> {
        let id: Option<String> = self.db.read(|conn| {
            Ok(conn
                .query_row("SELECT bucket_id FROM bucket_access_tokens WHERE token_id = ?1", params![token_id], |r| r.get(0))
                .optional()?)
        })?;
        id.ok_or_else(|| BucketError::InvalidArgument(format!("no token {token_id}")))?.parse()
    }

    /// The bucket a presented token is for (data-plane routing of token-only requests).
    pub fn token_target(&self, token: &str) -> BucketResult<AccessToken> {
        self.find_token(token)?.ok_or(BucketError::TokenInvalid)
    }

    pub async fn create_token(&self, bucket: &BucketInfo, caller: &Caller, req: TokenRequest) -> BucketResult<IssuedToken> {
        let now = now_ms();
        let target = match (&req.key, &req.prefix) {
            (Some(k), None) => {
                validate_key(k)?;
                Target::Key(k)
            }
            (None, Some(p)) => {
                if !p.is_empty() {
                    validate_key(p)?;
                }
                Target::Prefix(p)
            }
            _ => return Err(BucketError::InvalidArgument("a token is for exactly one of key or prefix".into())),
        };
        if req.actions.is_empty() || req.actions.contains_exact(Action::Manage) || req.actions.contains_exact(Action::Delete) {
            return Err(BucketError::InvalidArgument("a token carries list/read/create/write only".into()));
        }
        if req.actions.contains_exact(Action::List) && req.key.is_some() {
            return Err(BucketError::InvalidArgument("list needs a prefix token".into()));
        }
        if req.expires_at <= now || req.expires_at > now + MAX_TOKEN_TTL_MS {
            return Err(BucketError::InvalidArgument("a token expires within 7 days".into()));
        }
        let manage = self.require_manage(bucket, caller).await.is_ok();
        if !manage {
            for action in req.actions.iter() {
                match self.authorize_any(bucket, caller, &[action], target, false).await {
                    Ok(d) if !matches!(d.via, Via::Token(_)) => {}
                    _ => return Err(BucketError::PermissionDenied(format!("cannot issue '{action}' that the issuer does not hold"))),
                }
            }
        }
        let token_id = format!("t_{}", random_hex(10));
        let token = name_lib::bearer_token::issue(ACCESS_TOKEN_PREFIX);
        self.db.write(|tx| {
            tx.execute(
                "INSERT INTO bucket_access_tokens (token_id, token_hash, bucket_id, prefix, key, actions, expires_at, max_uses,
                    bind_user_id, bind_app_id, created_by, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    token_id,
                    token_hash(&token),
                    bucket.bucket_id.to_string(),
                    req.prefix,
                    req.key,
                    req.actions.to_csv(),
                    req.expires_at,
                    req.max_uses.map(|v| v as i64),
                    req.bind_user_id,
                    req.bind_app_id,
                    caller.principal(),
                    now
                ],
            )?;
            Ok(())
        })?;
        log::info!("bucket token {} on {} ({}) issued by {}", token_id, bucket.bucket_id, req.actions.to_csv(), caller.principal());
        Ok(IssuedToken {
            path: format!("/bucket/v1/{}/{}?token={}", bucket.bucket_id, target.path(), token),
            token_id,
            token,
            expires_at: req.expires_at,
        })
    }

    pub fn list_tokens(&self, bucket_id: &BucketId) -> BucketResult<Vec<AccessToken>> {
        self.db.read(|conn| {
            let mut stmt = conn.prepare(&format!("SELECT {TOKEN_COLS} FROM bucket_access_tokens WHERE bucket_id = ?1 ORDER BY created_at"))?;
            let rows = stmt.query_map(params![bucket_id.to_string()], token_from_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row??);
            }
            Ok(out)
        })
    }

    pub async fn revoke_token(&self, bucket: &BucketInfo, caller: &Caller, token_id: &str) -> BucketResult<()> {
        let token = self.list_tokens(&bucket.bucket_id)?.into_iter().find(|t| t.token_id == token_id);
        let Some(token) = token else { return Err(BucketError::InvalidArgument(format!("no token {token_id}"))) };
        if token.created_by != caller.principal() {
            self.require_manage(bucket, caller).await?;
        }
        self.db.write(|tx| {
            tx.execute("UPDATE bucket_access_tokens SET revoked_at = ?2 WHERE token_id = ?1", params![token_id, now_ms()])?;
            Ok(())
        })?;
        log::info!("bucket token {} on {} revoked by {}", token_id, bucket.bucket_id, caller.principal());
        Ok(())
    }

    // ---------- data-policy rules (after RBAC / grant / token) ----------

    pub async fn require_manage(&self, bucket: &BucketInfo, caller: &Caller) -> BucketResult<Decision> {
        let d = self.authorize_any(bucket, caller, &[Action::Manage], Target::Prefix(""), false).await?;
        if matches!(d.via, Via::Token(_)) {
            return Err(BucketError::PermissionDenied("tokens never carry manage".into()));
        }
        manage_rule(bucket, caller)?;
        Ok(d)
    }
}

const TOKEN_COLS: &str = "token_id, bucket_id, prefix, key, actions, expires_at, max_uses, uses, bind_user_id, bind_app_id, created_by, created_at, revoked_at";

fn token_from_row(row: &rusqlite::Row) -> rusqlite::Result<BucketResult<AccessToken>> {
    let bucket: String = row.get(1)?;
    let actions: String = row.get(4)?;
    let token = (|| -> BucketResult<AccessToken> {
        Ok(AccessToken {
            token_id: row.get(0)?,
            bucket_id: bucket.parse()?,
            prefix: row.get(2)?,
            key: row.get(3)?,
            actions: ActionSet::parse_csv(&actions)?,
            expires_at: row.get(5)?,
            max_uses: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
            uses: row.get::<_, i64>(7)? as u64,
            bind_user_id: row.get(8)?,
            bind_app_id: row.get(9)?,
            created_by: row.get(10)?,
            created_at: row.get(11)?,
            revoked_at: row.get(12)?,
        })
    })();
    Ok(token)
}

impl ActionSet {
    /// Membership without the write ⇒ create implication.
    pub fn contains_exact(&self, a: Action) -> bool {
        self.0 & a.bit() != 0
    }
}

/// `documents` / `collected` buckets are the user's: only the user, through
/// their own UI, manages them (§4.4.3).
pub(crate) fn manage_rule(bucket: &BucketInfo, caller: &Caller) -> BucketResult<()> {
    if bucket.data_policy.user_owned()
        && !caller.internal && !(caller.user_direct && caller.user.as_deref() == Some(bucket.owner.as_str())) {
        return Err(BucketError::PermissionDenied(format!(
            "only the user manages a {} bucket",
            bucket.data_policy
        )));
    }
    Ok(())
}

/// Overwrite / delete rules on an existing binding (§4.4.3, §4.4.6).
pub(crate) fn existing_rule(bucket: &BucketInfo, caller: &Caller, decision: &Decision, existing: &Binding) -> BucketResult<()> {
    if caller.internal {
        return Ok(());
    }
    let (creator_user, creator_app) = principal_parts(&existing.created_by);
    let caller_principal = caller.principal();
    let (caller_user, caller_app) = principal_parts(&caller_principal);
    if bucket.scope == BucketScope::Zone && matches!(decision.via, Via::Grant(_)) && creator_user != caller_user {
        return Err(BucketError::PermissionDenied(format!("'{}' was created by another user", existing.key)));
    }
    let owner_direct = caller.user_direct && caller.user.as_deref() == Some(bucket.owner.as_str());
    if bucket.data_policy.user_owned()
        && decision.action == Action::Delete
        && matches!(decision.via, Via::Identity)
        && !owner_direct
        && caller.app.is_some()
        && creator_app != caller_app
    {
        return Err(BucketError::PermissionDenied(format!(
            "an app deletes only the user documents it wrote ('{}')",
            existing.key
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessCheck {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<Via>,
}

impl crate::store::BucketStore {
    /// `bucket.grant` (§4.4.4).
    pub async fn grant(&self, id: &BucketId, caller: &Caller, req: GrantRequest) -> BucketResult<Grant> {
        let bucket = self.bucket(id)?;
        self.access().grant(&bucket, caller, req).await
    }

    /// `bucket.revoke`: the grant's bucket is looked up from the grant.
    pub async fn revoke(&self, caller: &Caller, grant_id: &str) -> BucketResult<()> {
        let bucket = self.bucket(&self.access().grant_bucket(grant_id)?)?;
        self.access().revoke(&bucket, caller, grant_id).await
    }

    pub async fn list_grants(&self, id: &BucketId, caller: &Caller) -> BucketResult<Vec<Grant>> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        self.access().list_grants(id)
    }

    /// `bucket.create_access_token` (§4.4.5).
    pub async fn create_access_token(&self, id: &BucketId, caller: &Caller, req: TokenRequest) -> BucketResult<IssuedToken> {
        let bucket = self.bucket(id)?;
        self.access().create_token(&bucket, caller, req).await
    }

    pub async fn revoke_access_token(&self, caller: &Caller, token_id: &str) -> BucketResult<()> {
        let bucket = self.bucket(&self.access().token_bucket(token_id)?)?;
        self.access().revoke_token(&bucket, caller, token_id).await
    }

    pub async fn list_access_tokens(&self, id: &BucketId, caller: &Caller) -> BucketResult<Vec<AccessToken>> {
        let bucket = self.bucket(id)?;
        self.access().require_manage(&bucket, caller).await?;
        self.access().list_tokens(id)
    }

    /// What a presented `bkt_` token is for (routing token-only data-plane requests).
    pub fn access_token_target(&self, token: &str) -> BucketResult<AccessToken> {
        self.access().token_target(token)
    }
}
