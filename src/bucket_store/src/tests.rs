use crate::access::*;
use crate::content::store_bytes;
use crate::*;
use ::http::HeaderMap;
use http_body_util::BodyExt;
use named_store::{NamedDataMgr, NamedLocalStore, PinScope, StoreLayout, StoreTarget};
use ndn_lib::{build_named_object_by_json, ObjId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

async fn open_ndm(dir: &std::path::Path) -> NamedDataMgr {
    let store = NamedLocalStore::get_named_store_by_path(dir.to_path_buf()).await.unwrap();
    let store_id = store.store_id().to_string();
    let mgr = NamedDataMgr::new();
    mgr.register_store(Arc::new(tokio::sync::Mutex::new(store))).await;
    mgr.add_layout(StoreLayout::new(
        1,
        vec![StoreTarget { store_id, device_did: String::new(), capacity: None, used: None, readonly: false, enabled: true, weight: 1 }],
        0,
        0,
    ))
    .await;
    mgr
}

struct Env {
    _dir: tempfile::TempDir,
    store: Arc<BucketStore>,
    events: Arc<Mutex<Vec<BucketEvent>>>,
}

struct Recorder(Arc<Mutex<Vec<BucketEvent>>>);
impl EventSink for Recorder {
    fn emit(&self, e: BucketEvent) {
        self.0.lock().unwrap().push(e);
    }
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let ndm = open_ndm(&dir.path().join("store")).await;
    let db = IndexDb::open(&dir.path().join("bucket-index.db")).unwrap();
    let store = BucketStore::new(db, ndm);
    let events = Arc::new(Mutex::new(Vec::new()));
    store.set_event_sink(Arc::new(Recorder(events.clone())));
    Env { _dir: dir, store, events }
}

/// Identity policy of a test: actions per bucket id, and whether the caller holds every object.
#[derive(Default)]
struct Policy {
    allow: HashMap<String, ActionSet>,
    holds: bool,
}

#[async_trait::async_trait]
impl IdentityPolicy for Policy {
    async fn allowed(&self, bucket: &BucketInfo, _key: &str, action: Action) -> bool {
        self.allow.get(&bucket.bucket_id.to_string()).is_some_and(|s| s.contains(action))
    }
    async fn holds(&self, _obj: &str) -> bool {
        self.holds
    }
}

fn caller(user: &str, app: &str, allow: &[(&BucketId, &[Action])]) -> Caller {
    let policy = Policy {
        allow: allow.iter().map(|(b, a)| (b.to_string(), a.iter().copied().collect())).collect(),
        holds: true,
    };
    Caller {
        user: Some(user.into()),
        app: Some(app.into()),
        agent: None,
        groups: vec!["users".into()],
        zone_user: true,
        user_direct: false,
        sudo: false,
        token: None,
        identity: Arc::new(policy),
        internal: false,
    }
}

fn owner_ui(user: &str, bucket: &BucketId) -> Caller {
    let mut c = caller(user, "control-panel", &[(bucket, &Action::ALL)]);
    c.user_direct = true;
    c
}

const ALL: &[Action] = &Action::ALL;
const RW: &[Action] = &[Action::List, Action::Read, Action::Write, Action::Delete];

fn decl(kind: &str, policy: &str, extra: serde_json::Value) -> BucketDecl {
    let mut v = serde_json::json!({"kind": kind, "data_policy": policy});
    v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    serde_json::from_value(v).unwrap()
}

async fn file(store: &BucketStore, name: &str, bytes: &[u8]) -> String {
    store_bytes(store.ndm(), name, bytes, Some("text/plain")).await.unwrap().1.to_string()
}

fn put(key: &str, obj: &str) -> PutRequest {
    PutRequest { key: key.into(), obj_id: obj.into(), ..Default::default() }
}

async fn pins(store: &BucketStore, id: &BucketId) -> Vec<String> {
    store.ndm().list_pins_by_owner(&id.pin_owner(), None, 1000).await.unwrap().into_iter().map(|p| p.obj_id.to_string()).collect()
}

fn user_bucket(app: &str, owner: &str, name: &str) -> BucketId {
    BucketId::user(app, owner, name).unwrap()
}

#[tokio::test]
async fn put_get_list_delete_keep_pins_symmetric() {
    let e = env().await;
    let id = user_bucket("photos", "alice", "index");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "photos", &[(&id, RW)]);
    let a = file(&e.store, "a.txt", b"hello").await;
    let b = file(&e.store, "b.txt", b"world!").await;

    let r = e.store.put(&id, &c, put("x/a.txt", &a)).await.unwrap();
    assert!(r.created);
    assert_eq!(r.etag, a);
    e.store.put(&id, &c, put("x/b.txt", &b)).await.unwrap();
    e.store.put(&id, &c, put("y/dup.txt", &a)).await.unwrap();
    let info = e.store.bucket(&id).unwrap();
    assert_eq!((info.key_count, info.unique_bytes, info.key_bytes), (3, 11, 16));
    let mut p = pins(&e.store, &id).await;
    p.sort();
    let mut want = vec![a.clone(), b.clone()];
    want.sort();
    assert_eq!(p, want, "one pin per distinct object");

    let got = e.store.get(&id, &c, "x/a.txt", None).await.unwrap();
    assert_eq!(got.obj_id.as_deref(), Some(a.as_str()));
    assert_eq!(got.content_type.as_deref(), Some("text/plain"));

    let list = e.store.list_objects(&id, &c, &ListRequest { delimiter: Some("/".into()), ..Default::default() }).await.unwrap();
    assert_eq!(list.common_prefixes, vec!["x/", "y/"]);
    assert!(list.items.is_empty());
    let page = e.store.list_objects(&id, &c, &ListRequest { prefix: "x/".into(), limit: Some(1), ..Default::default() }).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.next_start_after.as_deref(), Some("x/a.txt"));
    let next = e.store
        .list_objects(&id, &c, &ListRequest { prefix: "x/".into(), start_after: page.next_start_after, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(next.items.iter().map(|b| b.key.as_str()).collect::<Vec<_>>(), vec!["x/b.txt"]);
    assert!(next.next_start_after.is_none());

    e.store.delete(&id, &c, "x/a.txt", None, None).await.unwrap();
    assert_eq!(pins(&e.store, &id).await.len(), 2, "a is still bound by y/dup.txt");
    e.store.delete(&id, &c, "y/dup.txt", None, None).await.unwrap();
    assert_eq!(pins(&e.store, &id).await, vec![b.clone()]);
    let info = e.store.bucket(&id).unwrap();
    assert_eq!((info.key_count, info.unique_bytes, info.version_count), (1, 6, 1));
    assert!(matches!(e.store.get(&id, &c, "x/a.txt", None).await, Err(BucketError::NoSuchKey(_))));
    assert!(!e.store.delete(&id, &c, "x/a.txt", None, None).await.unwrap().deleted);

    let kinds: Vec<_> = e.events.lock().unwrap().iter().filter_map(|ev| match ev {
        BucketEvent::Object { event, .. } => Some(*event),
        _ => None,
    }).collect();
    assert_eq!(kinds.iter().filter(|k| **k == ObjectEventKind::ObjectCreated).count(), 3);
    assert_eq!(kinds.iter().filter(|k| **k == ObjectEventKind::ObjectRemoved).count(), 2);
}

#[tokio::test]
async fn conditional_writes() {
    let e = env().await;
    let id = user_bucket("app", "alice", "d");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let a = file(&e.store, "a", b"1").await;
    let b = file(&e.store, "b", b"2").await;
    let mut req = put("k", &a);
    req.conditions = Conditions::create_only();
    let v1 = e.store.put(&id, &c, req.clone()).await.unwrap();
    assert!(matches!(e.store.put(&id, &c, req).await, Err(BucketError::PreconditionFailed(_))));
    let mut cas = put("k", &b);
    cas.conditions.if_match = Some(format!("\"{b}\""));
    assert!(matches!(e.store.put(&id, &c, cas.clone()).await, Err(BucketError::PreconditionFailed(_))));
    cas.conditions.if_match = Some(a.clone());
    let v2 = e.store.put(&id, &c, cas).await.unwrap();
    assert!(v2.version > v1.version && !v2.created);
    let mut gen = put("k", &a);
    gen.conditions.if_version = Some(v1.version);
    assert!(matches!(e.store.put(&id, &c, gen.clone()).await, Err(BucketError::PreconditionFailed(_))));
    gen.conditions.if_version = Some(v2.version);
    e.store.put(&id, &c, gen).await.unwrap();
    assert!(matches!(e.store.delete(&id, &c, "k", None, Some(b.clone())).await, Err(BucketError::PreconditionFailed(_))));
    // the failed CAS left no stray pin for b
    e.store.reconcile(false).await.unwrap();
    assert_eq!(pins(&e.store, &id).await, vec![a]);
}

#[tokio::test]
async fn versioning_markers_and_promotion() {
    let e = env().await;
    let id = user_bucket("notes", "alice", "docs");
    let info = e.store.ensure_bucket(&id, &decl("data", "documents", serde_json::json!({}))).unwrap();
    assert!(info.versioning, "documents are versioned by default (D12)");
    let c = caller("alice", "notes", &[(&id, RW)]);
    let a = file(&e.store, "a", b"v1").await;
    let b = file(&e.store, "b", b"v22").await;
    let v1 = e.store.put(&id, &c, put("doc", &a)).await.unwrap().version;
    let v2 = e.store.put(&id, &c, put("doc", &b)).await.unwrap().version;
    assert_eq!(e.store.get(&id, &c, "doc", Some(v1)).await.unwrap().obj_id.as_deref(), Some(a.as_str()));
    let info = e.store.bucket(&id).unwrap();
    assert_eq!((info.key_count, info.version_count, info.unique_bytes), (1, 2, 5));
    let del = e.store.delete(&id, &c, "doc", None, None).await.unwrap();
    assert!(del.delete_marker);
    assert!(e.store.get(&id, &c, "doc", None).await.is_err());
    assert_eq!(pins(&e.store, &id).await.len(), 2, "old versions stay retained");
    let versions = e.store.list_versions(&id, &c, "doc").await.unwrap();
    assert_eq!(versions.len(), 3);
    assert!(versions[0].is_delete_marker && versions[0].is_current);
    // removing the marker brings v2 back
    e.store.delete(&id, &c, "doc", del.version, None).await.unwrap();
    assert_eq!(e.store.get(&id, &c, "doc", None).await.unwrap().version, v2);
    // removing v2 promotes v1 and releases b
    e.store.delete(&id, &c, "doc", Some(v2), None).await.unwrap();
    assert_eq!(e.store.get(&id, &c, "doc", None).await.unwrap().version, v1);
    assert_eq!(pins(&e.store, &id).await, vec![a]);
    assert_eq!(e.store.recompute_usage(&id).unwrap().drifted, false);
}

#[tokio::test]
async fn kinds_and_data_policies() {
    let e = env().await;
    let data = user_bucket("app", "alice", "files");
    let objs = user_bucket("app", "alice", "objs");
    let docs = user_bucket("app", "alice", "docs");
    let saved = user_bucket("app", "alice", "saved");
    let scratch = user_bucket("app", "alice", "scratch");
    let thumbs = user_bucket("app", "alice", "thumbs");
    let related = user_bucket("app", "alice", "related");
    e.store.ensure_bucket(&data, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    e.store.ensure_bucket(&objs, &decl("object", "app_data", serde_json::json!({}))).unwrap();
    e.store.ensure_bucket(&docs, &decl("data", "documents", serde_json::json!({}))).unwrap();
    e.store.ensure_bucket(&saved, &decl("data", "collected", serde_json::json!({}))).unwrap();
    e.store.ensure_bucket(&scratch, &decl("data", "temp", serde_json::json!({"default_ttl_days": 7}))).unwrap();
    e.store.ensure_bucket(&thumbs, &decl("data", "cache", serde_json::json!({}))).unwrap();
    e.store.ensure_bucket(&related, &decl("object", "ambient", serde_json::json!({}))).unwrap();
    let all: Vec<(&BucketId, &[Action])> = vec![(&data, RW), (&objs, RW), (&docs, RW), (&saved, RW), (&scratch, RW), (&thumbs, RW), (&related, RW)];
    let c = caller("alice", "app", &all);
    let f = file(&e.store, "f", b"content").await;
    let (note, note_body) = build_named_object_by_json("app_note", &serde_json::json!({"text": "hi"}));
    e.store.ndm().put_object(&note, &note_body).await.unwrap();
    let note = note.to_string();

    assert!(matches!(e.store.put(&objs, &c, put("f", &f)).await, Err(BucketError::KindMismatch(_))));
    assert!(matches!(e.store.put(&data, &c, put("n", &note)).await, Err(BucketError::KindMismatch(_))));
    e.store.put(&objs, &c, put("n", &note)).await.unwrap();

    let mut exp = put("f", &f);
    exp.expires_at = Some(now_ms() + 1000);
    assert!(matches!(e.store.put(&docs, &c, exp.clone()).await, Err(BucketError::PolicyViolation(_))));
    assert!(matches!(e.store.put(&saved, &c, put("f", &f)).await, Err(BucketError::OriginRequired(_))));
    let mut origin = put("f", &f);
    origin.origin = Some(Origin { author: "did:web:bob.example.com".into(), source: "cyfs://bob.example.com/x".into(), collected_at: 1, proof: None });
    e.store.put(&saved, &c, origin.clone()).await.unwrap();

    e.store.put(&scratch, &c, put("f", &f)).await.unwrap();
    let t = e.store.get(&scratch, &c, "f", None).await.unwrap();
    assert!(t.expires_at.unwrap() > now_ms() + 6 * DAY_MS, "temp takes the default ttl");
    let mut far = put("g", &f);
    far.expires_at = Some(now_ms() + 31 * DAY_MS);
    assert!(matches!(e.store.put(&scratch, &c, far).await, Err(BucketError::PolicyViolation(_))));

    e.store.put(&thumbs, &c, put("f", &f)).await.unwrap();
    assert!(pins(&e.store, &thumbs).await.is_empty(), "cache bindings are not pinned (D13)");

    let mut amb = put("n", &note);
    amb.origin = origin.origin.clone();
    e.store.put(&related, &c, amb).await.unwrap();
    let ambient_pins = e.store.ndm().list_pins_by_owner(&related.pin_owner(), None, 10).await.unwrap();
    assert_eq!(ambient_pins[0].scope, PinScope::Skeleton);
    let doc_pins = e.store.ndm().list_pins_by_owner(&saved.pin_owner(), None, 10).await.unwrap();
    assert_eq!(doc_pins[0].scope, PinScope::Recursive);

    // content must be in the store before binding
    let ghost = ObjId::new("cyfile:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff").unwrap().to_string();
    assert!(matches!(e.store.put(&data, &c, put("ghost", &ghost)).await, Err(BucketError::ContentNotAvailable(_))));
}

#[tokio::test]
async fn quotas_reject_new_bytes_only() {
    let e = env().await;
    let id = user_bucket("app", "alice", "small");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({"quota_mib": 1, "max_objects": 3}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let big = file(&e.store, "big", &vec![7u8; 700 * 1024]).await;
    let big2 = file(&e.store, "big2", &vec![8u8; 700 * 1024]).await;
    e.store.put(&id, &c, put("a", &big)).await.unwrap();
    match e.store.put(&id, &c, put("b", &big2)).await {
        Err(BucketError::QuotaExceeded(q)) => assert_eq!((q.level, q.limit), (QuotaLevel::Bucket, 1024 * 1024)),
        other => panic!("expected bucket quota, got {other:?}"),
    }
    e.store.put(&id, &c, put("b", &big)).await.expect("a dedupe hit is never rejected");
    e.store.put(&id, &c, put("c", &big)).await.unwrap();
    match e.store.put(&id, &c, put("d", &big)).await {
        Err(BucketError::QuotaExceeded(q)) => assert_eq!(q.what, "objects"),
        other => panic!("expected max_objects, got {other:?}"),
    }
    e.store.delete(&id, &c, "c", None, None).await.expect("deletes ignore quota");

    // App pool: two app_data buckets share the app's storage_quota_mib
    let other = user_bucket("app", "alice", "other");
    e.store.ensure_bucket(&other, &decl("data", "temp", serde_json::json!({"default_ttl_days": 1}))).unwrap();
    e.store.set_declared_pool_quota("app", "alice", Some(1024 * 1024)).unwrap();
    let c2 = caller("alice", "app", &[(&other, RW)]);
    match e.store.put(&other, &c2, put("x", &big2)).await {
        Err(BucketError::QuotaExceeded(q)) => assert_eq!(q.level, QuotaLevel::Pool),
        other => panic!("expected pool quota, got {other:?}"),
    }
    let pool = e.store.get_pool_quota(PoolKind::App, "app@alice").unwrap();
    assert_eq!(pool.usage, 700 * 1024);
    assert!(pool.enforced);

    // warnings: 700K of 1M is 68%; lowering the bucket limit crosses 80% and 95%
    let owner = caller("alice", "app", &[(&id, ALL)]);
    e.store.set_quota(&id, &owner, QuotaUpdate { max_bytes: Some(720 * 1024), ..Default::default() }, false).await.unwrap();
    assert!(matches!(
        e.store.set_quota(&id, &owner, QuotaUpdate { max_bytes: Some(4 * 1024 * 1024), ..Default::default() }, false).await,
        Err(BucketError::PermissionDenied(_))
    ));
    let small = file(&e.store, "s", b"x").await;
    e.store.put(&id, &c, put("e", &small)).await.unwrap();
    let warned = e.events.lock().unwrap().iter().any(|ev| matches!(ev, BucketEvent::QuotaWarning { percent, .. } if *percent >= 95));
    assert!(warned);
}

#[tokio::test]
async fn grants_tokens_and_ownership_rules() {
    let e = env().await;
    // Zone public library: the service grants list/read/create to all zone users.
    let lib = BucketId::zone("library-svc", "library").unwrap();
    e.store.ensure_bucket(&lib, &decl("data", "app_data", serde_json::json!({"scope": "zone"}))).unwrap();
    let svc = caller("library-svc", "library-svc", &[(&lib, ALL)]);
    let mut admin_req = GrantRequest {
        grantee: Grantee { kind: GranteeKind::ZoneUsers, id: String::new() },
        prefix: String::new(),
        actions: [Action::List, Action::Read, Action::Create].into_iter().collect(),
        expires_at: None,
    };
    let g = e.store.grant(&lib, &svc, admin_req.clone()).await.unwrap();
    admin_req.actions = [Action::Manage].into_iter().collect();
    assert!(e.store.grant(&lib, &svc, admin_req).await.is_err(), "manage is never granted");

    let alice = caller("alice", "files", &[]);
    let bob = caller("bob", "files", &[]);
    let fa = file(&e.store, "a", b"alice").await;
    let fb = file(&e.store, "b", b"bob").await;
    e.store.put(&lib, &alice, put("shared/a", &fa)).await.unwrap();
    // create-only: overwriting an existing key is refused even for the creator
    assert!(matches!(e.store.put(&lib, &alice, put("shared/a", &fb)).await, Err(BucketError::PreconditionFailed(_))));
    assert!(e.store.delete(&lib, &bob, "shared/a", None, None).await.is_err());
    e.store.put(&lib, &bob, put("shared/b", &fb)).await.unwrap();
    assert_eq!(e.store.list_objects(&lib, &bob, &ListRequest::default()).await.unwrap().items.len(), 2);
    let check = e.store.check_access(&lib, &bob, Action::Read, Some("shared/a"), None).await.unwrap();
    assert_eq!(check.via, Some(Via::Grant(g.grant_id.clone())));
    e.store.revoke(&svc, &g.grant_id).await.unwrap();
    assert!(e.store.get(&lib, &bob, "shared/a", None).await.is_err());

    // A family album: user bucket, read grant to zone users under a prefix.
    let album = user_bucket("photos", "alice", "album");
    e.store.ensure_bucket(&album, &decl("data", "documents", serde_json::json!({}))).unwrap();
    let alice_ui = owner_ui("alice", &album);
    let photos_app = caller("alice", "photos", &[(&album, RW)]);
    e.store.put(&album, &photos_app, put("2026/a.jpg", &fa)).await.unwrap();
    e.store.put(&album, &photos_app, put("private/b.jpg", &fb)).await.unwrap();
    let mut req = GrantRequest {
        grantee: Grantee { kind: GranteeKind::ZoneUsers, id: String::new() },
        prefix: "2026/".into(),
        actions: [Action::List, Action::Read].into_iter().collect(),
        expires_at: None,
    };
    assert!(e.store.grant(&album, &photos_app, req.clone()).await.is_err(), "an app does not manage user documents");
    e.store.grant(&album, &alice_ui, req.clone()).await.unwrap();
    let seen = e.store.list_objects(&album, &bob, &ListRequest::default()).await.unwrap();
    assert_eq!(seen.items.iter().map(|b| b.key.as_str()).collect::<Vec<_>>(), vec!["2026/a.jpg"], "listing narrows to the grant");
    assert!(e.store.get(&album, &bob, "private/b.jpg", None).await.is_err());
    req.grantee = Grantee { kind: GranteeKind::App, id: "chat".into() };
    req.actions = [Action::Delete].into_iter().collect();
    assert!(matches!(e.store.grant(&album, &alice_ui, req).await, Err(BucketError::PolicyViolation(_))));

    // An app deletes only the user documents it wrote.
    let other_app = caller("alice", "editor", &[(&album, RW)]);
    assert!(e.store.delete(&album, &other_app, "2026/a.jpg", None, None).await.is_err());
    e.store.delete(&album, &photos_app, "private/b.jpg", None, None).await.unwrap();

    // Capability tokens: one key, read only, two uses.
    let tok = e.store
        .create_access_token(
            &album,
            &alice_ui,
            TokenRequest {
                key: Some("2026/a.jpg".into()),
                prefix: None,
                actions: [Action::Read].into_iter().collect(),
                expires_at: now_ms() + 3600_000,
                max_uses: Some(2),
                bind_user_id: None,
                bind_app_id: None,
            },
        )
        .await
        .unwrap();
    assert!(tok.path.starts_with("/bucket/v1/user/photos/alice/album/2026/a.jpg?token=bkt_"));
    let anon = Caller::anonymous(Some(tok.token.clone()));
    e.store.get(&album, &anon, "2026/a.jpg", None).await.unwrap();
    assert!(e.store.list_objects(&album, &anon, &ListRequest::default()).await.is_err());
    e.store.get(&album, &anon, "2026/a.jpg", None).await.unwrap();
    assert!(matches!(e.store.get(&album, &anon, "2026/a.jpg", None).await, Err(BucketError::TokenInvalid)));
    let toks = e.store.list_access_tokens(&album, &alice_ui).await.unwrap();
    assert_eq!(toks[0].uses, 2);
    assert!(matches!(e.store.get(&album, &Caller::anonymous(Some("bkt_nope".into())), "2026/a.jpg", None).await, Err(BucketError::TokenInvalid)));
    let too_long = TokenRequest {
        key: None,
        prefix: Some("up/".into()),
        actions: [Action::Write].into_iter().collect(),
        expires_at: now_ms() + 8 * DAY_MS,
        max_uses: None,
        bind_user_id: None,
        bind_app_id: None,
    };
    assert!(e.store.create_access_token(&album, &alice_ui, too_long).await.is_err());

    // Deleting user documents needs the user in a sudo session.
    assert!(e.store.delete_bucket(&album, &alice_ui).await.is_err());
    let mut sudo = alice_ui.clone();
    sudo.sudo = true;
    e.store.delete_bucket(&album, &sudo).await.unwrap();
    assert!(pins(&e.store, &album).await.is_empty());
}

#[tokio::test]
async fn copy_and_holding_proof() {
    let e = env().await;
    let inbox = BucketId::zone("msg-center", "attachments").unwrap();
    let saved = user_bucket("messagehub", "alice", "saved");
    e.store.ensure_bucket(&inbox, &decl("data", "app_data", serde_json::json!({"scope": "zone"}))).unwrap();
    e.store.ensure_bucket(&saved, &decl("data", "collected", serde_json::json!({}))).unwrap();
    let f = file(&e.store, "att", b"attachment").await;
    let center = caller("msg-center", "msg-center", &[(&inbox, ALL)]);
    e.store.put(&inbox, &center, put("s1/m1/0", &f)).await.unwrap();

    // Knowing the ObjId is not enough to bind it.
    let mut stranger = caller("alice", "messagehub", &[(&saved, RW)]);
    stranger.identity = Arc::new(Policy { allow: [(saved.to_string(), RW.iter().copied().collect())].into(), holds: false });
    let mut req = put("from-bob/att", &f);
    req.origin = Some(Origin { author: "did:web:bob.example.com".into(), source: "obj://bucket/zone/msg-center/_/attachments/s1/m1/0".into(), collected_at: 1, proof: None });
    assert!(matches!(e.store.put(&saved, &stranger, req.clone()).await, Err(BucketError::NotHeld(_))));

    // With read on the source key (here a single-key token from msg-center), copy works.
    let tok = e.store
        .create_access_token(
            &inbox,
            &center,
            TokenRequest {
                key: Some("s1/m1/0".into()),
                prefix: None,
                actions: [Action::Read].into_iter().collect(),
                expires_at: now_ms() + 60_000,
                max_uses: None,
                bind_user_id: Some("alice".into()),
                bind_app_id: None,
            },
        )
        .await
        .unwrap();
    stranger.token = Some(tok.token);
    let copy = CopyRequest {
        src_bucket_id: inbox.to_string(),
        src_key: "s1/m1/0".into(),
        dst_bucket_id: saved.to_string(),
        dst_key: "from-bob/att".into(),
        origin: req.origin.clone(),
        ..Default::default()
    };
    e.store.copy(&stranger, copy.clone()).await.unwrap();
    // The source is withdrawn; the collected copy stays retained.
    e.store.delete(&inbox, &center, "s1/m1/0", None, None).await.unwrap();
    assert_eq!(pins(&e.store, &saved).await, vec![f.clone()]);
    assert!(pins(&e.store, &inbox).await.is_empty());
    let mut no_origin = copy;
    no_origin.origin = None;
    no_origin.dst_key = "x".into();
    e.store.put(&inbox, &center, put("s1/m1/0", &f)).await.unwrap();
    assert!(matches!(e.store.copy(&stranger, no_origin).await, Err(BucketError::OriginRequired(_))));
}

#[tokio::test]
async fn batch_is_all_or_nothing() {
    let e = env().await;
    let id = user_bucket("app", "alice", "b");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let a = file(&e.store, "a", b"a").await;
    let b = file(&e.store, "b", b"bb").await;
    e.store.put(&id, &c, put("exists", &a)).await.unwrap();
    e.store.put(&id, &c, put("keep", &a)).await.unwrap();
    let mut conflict = put("keep", &b);
    conflict.conditions = Conditions::create_only();
    let ops = vec![BatchOp::Put(put("new", &b)), BatchOp::Delete { key: "exists".into(), version: None, if_match: None }, BatchOp::Put(conflict)];
    assert!(e.store.batch(&id, &c, ops).await.is_err());
    assert!(e.store.get(&id, &c, "new", None).await.is_err());
    assert!(e.store.get(&id, &c, "exists", None).await.is_ok());
    assert_eq!(pins(&e.store, &id).await, vec![a.clone()], "the pin taken for the failed batch is released");
    let ok = vec![BatchOp::Put(put("new", &b)), BatchOp::Delete { key: "exists".into(), version: None, if_match: None }];
    let res = e.store.batch(&id, &c, ok).await.unwrap();
    assert_eq!(res.len(), 2);
    let mut p = pins(&e.store, &id).await;
    p.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(p, want);
}

#[tokio::test]
async fn lifecycle_ttl_noncurrent_and_cache_lru() {
    let e = env().await;
    let scratch = user_bucket("app", "alice", "scratch");
    let notes = user_bucket("app", "alice", "notes");
    let cache = user_bucket("app", "alice", "thumbs");
    e.store.ensure_bucket(&scratch, &decl("data", "temp", serde_json::json!({"default_ttl_days": 1}))).unwrap();
    e.store
        .ensure_bucket(&notes, &decl("data", "documents", serde_json::json!({"lifecycle": [{"rule": "noncurrent_expire", "days": 3}]})))
        .unwrap();
    e.store.ensure_bucket(&cache, &decl("data", "cache", serde_json::json!({"quota_mib": 1}))).unwrap();
    let c = caller("alice", "app", &[(&scratch, RW), (&notes, RW), (&cache, RW)]);
    let a = file(&e.store, "a", b"a").await;
    let b = file(&e.store, "b", b"bb").await;
    e.store.put(&scratch, &c, put("tmp", &a)).await.unwrap();
    e.store.put(&notes, &c, put("n", &a)).await.unwrap();
    e.store.put(&notes, &c, put("n", &b)).await.unwrap();
    assert_eq!(e.store.run_lifecycle(now_ms()).await.unwrap(), LifecycleReport::default());
    let report = e.store.run_lifecycle(now_ms() + 4 * DAY_MS).await.unwrap();
    assert_eq!((report.expired_objects, report.expired_noncurrent), (1, 1));
    assert!(e.store.get(&scratch, &c, "tmp", None).await.is_err());
    assert_eq!(e.store.list_versions(&notes, &c, "n").await.unwrap().len(), 1);
    assert!(e.events.lock().unwrap().iter().any(|ev| matches!(ev, BucketEvent::Object { event: ObjectEventKind::ObjectExpired, .. })));

    // cache: over the soft quota the least recently read keys go first; writes are never refused
    for i in 0..3u8 {
        let f = file(&e.store, &format!("t{i}"), &vec![i; 400 * 1024]).await;
        e.store.put(&cache, &c, put(&format!("t{i}"), &f)).await.unwrap();
    }
    let report = e.store.run_lifecycle(now_ms()).await.unwrap();
    assert_eq!(report.cache_evicted, 1);
    assert!(e.store.get(&cache, &c, "t0", None).await.is_err());
}

#[tokio::test]
async fn reconcile_repairs_broken_pins() {
    let e = env().await;
    let id = user_bucket("app", "alice", "r");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let a = file(&e.store, "a", b"a").await;
    let stray = file(&e.store, "s", b"stray").await;
    e.store.put(&id, &c, put("a", &a)).await.unwrap();
    let a_id = ObjId::new(&a).unwrap();
    // someone dropped the bucket's pin, and a crash left a pin without a binding
    e.store.ndm().unpin(&a_id, &id.pin_owner()).await.unwrap();
    e.store.ndm().pin(&ObjId::new(&stray).unwrap(), &id.pin_owner(), PinScope::Recursive, None).await.unwrap();
    e.store.ndm().pin(&a_id, "bucket:user/gone/alice/x", PinScope::Recursive, None).await.unwrap();
    let report = e.store.reconcile(true).await.unwrap();
    assert_eq!((report.repinned, report.stray_unpinned), (1, 2));
    assert_eq!(pins(&e.store, &id).await, vec![a.clone()]);
    assert_eq!(e.store.ndm().list_pins_by_owner("bucket:user/gone/", None, 10).await.unwrap().len(), 0);
    assert_eq!(e.store.reconcile(true).await.unwrap(), ReconcileReport::default());
}

#[tokio::test]
async fn snapshot_restore_and_verify() {
    let e = env().await;
    let id = user_bucket("app", "alice", "docs");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let owner = caller("alice", "app", &[(&id, ALL)]);
    let a = file(&e.store, "a", b"aaaa").await;
    let b = file(&e.store, "b", b"bb").await;
    e.store.put(&id, &owner, put("a", &a)).await.unwrap();
    e.store.put(&id, &owner, put("a/b", &b)).await.unwrap();
    let snap = e.store.snapshot(&id, &owner, Some("before".into()), false).await.unwrap();
    assert_eq!((snap.key_count, snap.unique_bytes, snap.snapshot_only_bytes), (2, 6, 0));
    let root = ObjId::new(&snap.root_obj_id).unwrap();
    // the tree renames the key `a` that clashes with the prefix `a/`
    let tree = e.store.ndm().get_object(&ObjId::new(&snap.tree_obj_id).unwrap()).await.unwrap();
    let tree: ndn_lib::DirObject = ndn_lib::load_named_obj(&tree).unwrap();
    let mut names = tree.list_entries();
    names.sort();
    assert_eq!(names, vec!["a", "a~file"]);

    e.store.delete(&id, &owner, "a", None, None).await.unwrap();
    let info = e.store.bucket(&id).unwrap();
    assert_eq!((info.unique_bytes, info.snapshot_only_bytes), (2, 4), "deleted bytes stay charged while a snapshot holds them");

    let missing = e.store
        .verify_references(&id, &owner, vec![Reference::Key { key: "a".into(), version: None }, Reference::Object { obj_id: b.clone() }])
        .await
        .unwrap();
    assert_eq!(missing, vec![Reference::Key { key: "a".into(), version: None }]);

    let fresh = user_bucket("app", "alice", "restored");
    e.store.ensure_bucket(&fresh, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c2 = caller("alice", "app", &[(&fresh, ALL)]);
    let report = e.store.restore_from_snapshot(&fresh, &c2, &root.to_string()).await.unwrap();
    assert_eq!((report.restored, report.missing.len()), (2, 0));
    assert_eq!(e.store.get(&fresh, &c2, "a", None).await.unwrap().obj_id.as_deref(), Some(a.as_str()));
    assert!(e.store.restore_from_snapshot(&fresh, &c2, &root.to_string()).await.is_err(), "only into an empty bucket");
    let mut p = pins(&e.store, &fresh).await;
    p.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(p, want);

    e.store.delete_snapshot(&id, &owner, &snap.snapshot_id).await.unwrap();
    assert_eq!(e.store.bucket(&id).unwrap().snapshot_only_bytes, 0);
    assert!(e.store.ndm().list_pins_by_owner("bucket-snapshot:", None, 10).await.unwrap().is_empty());
}

async fn body_bytes(r: ::http::Response<crate::http::HttpBody>) -> Vec<u8> {
    r.into_body().collect().await.unwrap().to_bytes().to_vec()
}

#[tokio::test]
async fn data_plane_put_get_range_head_304() {
    let e = env().await;
    let id = user_bucket("app", "alice", "web");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let mut h = HeaderMap::new();
    h.insert("content-type", "image/png".parse().unwrap());
    h.insert("x-bucky-meta-camera", "x100".parse().unwrap());
    h.insert("cache-control", "max-age=60".parse().unwrap());
    let r = e.store.put_bytes(&id, &c, "img/logo.png", bytes::Bytes::from_static(b"0123456789"), &h).await.unwrap();
    let (bucket, b) = e.store.open_for_read(&id, &c, "img/logo.png", None).await.unwrap();
    assert_eq!(b.metadata.get("camera").map(String::as_str), Some("x100"));
    let resp = e.store.serve_read(&bucket, &b, &HeaderMap::new(), false).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-type"], "image/png");
    assert_eq!(resp.headers()["cache-control"], "max-age=60");
    assert_eq!(resp.headers()["etag"], format!("\"{}\"", r.etag));
    assert_eq!(body_bytes(resp).await, b"0123456789");
    let mut range = HeaderMap::new();
    range.insert("range", "bytes=2-4".parse().unwrap());
    let resp = e.store.serve_read(&bucket, &b, &range, false).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.headers()["content-range"], "bytes 2-4/10");
    assert_eq!(body_bytes(resp).await, b"234");
    let mut inm = HeaderMap::new();
    inm.insert("if-none-match", format!("\"{}\"", r.etag).parse().unwrap());
    assert_eq!(e.store.serve_read(&bucket, &b, &inm, false).await.status(), 304);
    let head = e.store.serve_read(&bucket, &b, &HeaderMap::new(), true).await;
    assert_eq!(head.headers()["content-length"], "10");
    assert!(body_bytes(head).await.is_empty());
    let mut bad = HeaderMap::new();
    bad.insert("range", "bytes=20-".parse().unwrap());
    assert_eq!(e.store.serve_read(&bucket, &b, &bad, false).await.status(), 416);
    let mut cond = HeaderMap::new();
    cond.insert("if-none-match", "*".parse().unwrap());
    assert!(matches!(
        e.store.put_bytes(&id, &c, "img/logo.png", bytes::Bytes::from_static(b"other"), &cond).await,
        Err(BucketError::PreconditionFailed(_))
    ));
    let err = crate::http::error_response(&BucketError::NoSuchKey("x".into()));
    assert_eq!(err.status(), 404);
}

#[tokio::test]
async fn maintenance_round_materializes_cascade() {
    let e = env().await;
    let id = user_bucket("app", "alice", "m");
    e.store.ensure_bucket(&id, &decl("data", "app_data", serde_json::json!({}))).unwrap();
    let c = caller("alice", "app", &[(&id, RW)]);
    let (fo, f) = store_bytes(e.store.ndm(), "f", b"cascade me", None).await.unwrap();
    e.store.put(&id, &c, put("f", &f.to_string())).await.unwrap();
    let chunk = ObjId::new(&fo.content).unwrap();
    let report = e.store.ndm().maintenance_round(false).await.unwrap();
    assert!(report.edges_delivered >= 1);
    let st = e.store.ndm().debug_dump_expand_state(&chunk).await.unwrap();
    assert!(st.eviction_class >= 1, "the bound file's chunk is protected by the cascade");
    e.store.delete(&id, &c, "f", None, None).await.unwrap();
    e.store.ndm().maintenance_round(false).await.unwrap();
    assert_eq!(e.store.ndm().debug_dump_expand_state(&chunk).await.unwrap().eviction_class, 0, "released after unbinding");
}

#[allow(dead_code)]
fn futures_are_send(store: Arc<BucketStore>, c: Caller, id: BucketId) {
    fn send<T: Send>(_: T) {}
    send(store.run_lifecycle(0));
    send(store.reconcile(true));
    send(store.put(&id, &c, PutRequest::default()));
    send(store.delete(&id, &c, "k", None, None));
    send(store.batch(&id, &c, Vec::new()));
    send(store.copy(&c, CopyRequest::default()));
    send(store.snapshot(&id, &c, None, false));
    send(store.restore_from_snapshot(&id, &c, ""));
    send(store.list_objects(&id, &c, &ListRequest::default()));
    send(store.put_bytes(&id, &c, "k", bytes::Bytes::new(), &HeaderMap::new()));
    send(store.grant(&id, &c, GrantRequest { grantee: Grantee { kind: GranteeKind::User, id: String::new() }, prefix: String::new(), actions: ActionSet::default(), expires_at: None }));
    send(store.set_quota(&id, &c, QuotaUpdate::default(), false));
}
