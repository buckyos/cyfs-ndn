//! `CyfsNdnClient` pulling from the `/ndn/` read protocol of `bucket_store`
//! (bucket design §4.6.5, §21 N16): every part is reached by a walk from the
//! authorization root, with the token, and only on the same host.

use crate::cyfs_ndn_client::{CyfsHttpTransport, CyfsNdnClient, CyfsTransportRequest, CyfsTransportResponse};
use bucket_store::{BucketDecl, BucketId, BucketStore, Caller, IndexDb, NdnAccess, NdnRequest, PutRequest, TokenRequest};
use http_body_util::BodyExt;
use named_store::{NamedDataMgr, NamedLocalStore, StoreLayout, StoreTarget};
use ndn_lib::{ChunkHasher, ChunkId, ChunkList, FileObject, NamedObject, NdnError, NdnHint, ObjId};
use reqwest::header::HeaderMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

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

fn chunk_id(bytes: &[u8]) -> ChunkId {
    let hasher = ChunkHasher::new(None).unwrap();
    let method = hasher.hash_method;
    let raw = hasher.calc_from_bytes(bytes);
    ChunkId::from_mix_hash_result_by_hash_method(bytes.len() as u64, &raw, method).unwrap()
}

/// Serves `/ndn/` from a bucket store: the root must be bound in `public`, or
/// the token must cover it. Records what was requested.
struct NdnTransport {
    store: Arc<BucketStore>,
    public: BucketId,
    requests: Mutex<Vec<(String, HeaderMap)>>,
    drop_chunks: bool,
}

impl CyfsHttpTransport for NdnTransport {
    fn send(&self, request: CyfsTransportRequest) -> Pin<Box<dyn std::future::Future<Output = ndn_lib::NdnResult<CyfsTransportResponse>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push((request.url.clone(), request.headers.clone()));
            let url = url::Url::parse(&request.url).unwrap();
            let parsed = NdnRequest::parse(url.path(), url.query()).map_err(|e| NdnError::InvalidParam(e.to_string()))?;
            let access = if self.store.ndn_bound_in(&self.public, &parsed.root).unwrap() {
                Some(NdnAccess::Public)
            } else if parsed.token.as_deref().is_some_and(|t| self.store.ndn_token_covers(t, &parsed.root)) {
                Some(NdnAccess::Token)
            } else {
                None
            };
            let response = match access {
                Some(_) if self.drop_chunks && ObjId::new(url.path().trim_start_matches("/ndn/")).is_ok_and(|id| id.is_chunk()) => bucket_store::ndn_read::ndn_not_found(),
                Some(access) => self.store.serve_ndn(&parsed, access, &request.headers, false).await,
                None => bucket_store::ndn_read::ndn_not_found(),
            };
            let (parts, body) = response.into_parts();
            let bytes = body.collect().await.unwrap().to_bytes();
            let content_length = parts.headers.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
            Ok(CyfsTransportResponse {
                status: parts.status,
                headers: parts.headers,
                content_length,
                body: Box::pin(std::io::Cursor::new(bytes.to_vec())),
            })
        })
    }
}

struct Env {
    _dir: TempDir,
    source: Arc<BucketStore>,
    public: BucketId,
    private: BucketId,
    target: NamedDataMgr,
}

async fn env() -> Env {
    let dir = TempDir::new().unwrap();
    let ndm = open_ndm(&dir.path().join("source")).await;
    let source = BucketStore::new(IndexDb::open(&dir.path().join("index.db")).unwrap(), ndm);
    let public = BucketId::zone("nfs-server", "public-objects").unwrap();
    let private = BucketId::zone("msg-center", "attachments").unwrap();
    let decl: BucketDecl = serde_json::from_value(serde_json::json!({"kind": "data", "data_policy": "app_data", "scope": "zone"})).unwrap();
    source.ensure_bucket(&public, &decl).unwrap();
    source.ensure_bucket(&private, &decl).unwrap();
    let target = open_ndm(&dir.path().join("target")).await;
    Env { _dir: dir, source, public, private, target }
}

async fn multi_chunk_file(store: &BucketStore) -> (ObjId, ObjId, Vec<ChunkId>) {
    let mut chunks = Vec::new();
    for piece in [&b"first-"[..], b"second-", b"third"] {
        let id = chunk_id(piece);
        store.ndm().put_chunk(&id, piece).await.unwrap();
        chunks.push(id);
    }
    let (list, list_str) = ChunkList::from_chunk_list(chunks.clone()).unwrap().gen_obj_id();
    store.ndm().put_object(&list, &list_str).await.unwrap();
    let (file, body) = FileObject::new("a.bin".into(), 18, list.to_string()).gen_obj_id();
    store.ndm().put_object(&file, &body).await.unwrap();
    (file, list, chunks)
}

fn client(store: Arc<BucketStore>, public: BucketId, drop_chunks: bool) -> (CyfsNdnClient, Arc<NdnTransport>) {
    let transport = Arc::new(NdnTransport { store, public, requests: Mutex::new(Vec::new()), drop_chunks });
    let client = CyfsNdnClient::builder().transport(ArcTransport(transport.clone())).build().unwrap();
    (client, transport)
}

struct ArcTransport(Arc<NdnTransport>);
impl CyfsHttpTransport for ArcTransport {
    fn send(&self, request: CyfsTransportRequest) -> Pin<Box<dyn std::future::Future<Output = ndn_lib::NdnResult<CyfsTransportResponse>> + Send + '_>> {
        self.0.send(request)
    }
}

async fn read_all(ndm: &NamedDataMgr, id: &ObjId) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let (mut reader, _) = ndm.open_reader(id, None).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn pulls_a_public_file_by_walking_from_its_root() {
    let e = env().await;
    let (file, list, chunks) = multi_chunk_file(&e.source).await;
    let req = PutRequest { key: "hs/entry/x".into(), obj_id: file.to_string(), ..Default::default() };
    e.source.put(&e.public, &Caller::system(), req).await.unwrap();

    let (client, transport) = client(e.source.clone(), e.public.clone(), false);
    let result = client.get(format!("https://zone-a.example/ndn/{}", file.to_string())).pull_to_named_store(&e.target).await.unwrap();
    assert_eq!(result.obj_id, Some(file.clone()));
    assert_eq!(read_all(&e.target, &file).await, b"first-second-third");
    assert!(e.target.is_object_stored(&file, None).await.unwrap());

    let urls: Vec<String> = transport.requests.lock().unwrap().iter().map(|(u, _)| u.clone()).collect();
    assert_eq!(urls.len(), 5, "file, list, three chunks: {urls:?}");
    assert!(urls[1].contains(&list.to_string()) && urls[1].contains("resp=raw"));
    for (i, chunk) in chunks.iter().enumerate() {
        let url = url::Url::parse(&urls[2 + i]).unwrap();
        assert_eq!(url.path(), format!("/ndn/{}", chunk.to_string()));
        let hint = url.query_pairs().find(|(k, _)| k == "hint").unwrap().1.into_owned();
        assert_eq!(NdnHint::parse(&hint).unwrap().path, Some(format!("{}/@/content/@/{i}", file.to_string())));
        assert_eq!(url.host_str(), Some("zone-a.example"));
    }
}

#[tokio::test]
async fn a_single_chunk_file_and_an_empty_file_pull_too() {
    let e = env().await;
    let id = chunk_id(b"only");
    e.source.ndm().put_chunk(&id, b"only").await.unwrap();
    let (file, body) = FileObject::new("o.bin".into(), 4, id.to_string()).gen_obj_id();
    e.source.ndm().put_object(&file, &body).await.unwrap();
    let (empty, body) = FileObject::new("e.bin".into(), 0, String::new()).gen_obj_id();
    e.source.ndm().put_object(&empty, &body).await.unwrap();
    for (key, id) in [("a/o", &file), ("a/e", &empty)] {
        let req = PutRequest { key: key.into(), obj_id: id.to_string(), ..Default::default() };
        e.source.put(&e.public, &Caller::system(), req).await.unwrap();
    }
    let (client, _) = client(e.source.clone(), e.public.clone(), false);
    client.get(format!("https://zone-a.example/ndn/{}", file.to_string())).pull_to_named_store(&e.target).await.unwrap();
    assert_eq!(read_all(&e.target, &file).await, b"only");
    let r = client.get(format!("https://zone-a.example/ndn/{}", empty.to_string())).pull_to_named_store(&e.target).await.unwrap();
    assert_eq!(r.total_size, 0);
    assert!(e.target.is_object_stored(&empty, None).await.unwrap());
}

#[tokio::test]
async fn the_token_travels_with_every_part_and_nothing_works_without_it() {
    let e = env().await;
    let (file, _, _) = multi_chunk_file(&e.source).await;
    let system = Caller::system();
    let req = PutRequest { key: "s/m/0".into(), obj_id: file.to_string(), ..Default::default() };
    e.source.put(&e.private, &system, req).await.unwrap();
    let token = e.source
        .create_access_token(&e.private, &system, TokenRequest {
            key: Some("s/m/0".into()),
            prefix: None,
            actions: [bucket_store::Action::Read].into_iter().collect(),
            expires_at: bucket_store::now_ms() + 60_000,
            max_uses: None,
            bind_user_id: None,
            bind_app_id: None,
        })
        .await
        .unwrap();

    let (client_a, transport) = client(e.source.clone(), e.public.clone(), false);
    let denied = client_a.get(format!("https://zone-a.example/ndn/{}", file.to_string())).pull_to_named_store(&e.target).await;
    assert!(matches!(denied, Err(NdnError::NotFound(_))), "no token");
    assert!(!e.target.is_object_stored(&file, None).await.unwrap());
    transport.requests.lock().unwrap().clear();

    let query = NdnHint { path: None, token: Some(token.token.clone()) }.to_query().unwrap();
    client_a.get(format!("https://zone-a.example/ndn/{}?{query}", file.to_string())).pull_to_named_store(&e.target).await.unwrap();
    assert_eq!(read_all(&e.target, &file).await, b"first-second-third");
    let urls: Vec<String> = transport.requests.lock().unwrap().iter().map(|(u, _)| u.clone()).collect();
    assert!(urls.len() >= 5);
    for url in &urls {
        let parsed = url::Url::parse(url).unwrap();
        let hint = parsed.query_pairs().find(|(k, _)| k == "hint").unwrap().1.into_owned();
        assert_eq!(NdnHint::parse(&hint).unwrap().token.as_deref(), Some(token.token.as_str()), "{}", parsed.path());
        assert_eq!(parsed.host_str(), Some("zone-a.example"));
    }
}

#[tokio::test]
async fn a_source_without_chunks_never_counts_as_complete_even_with_local_copies() {
    let e = env().await;
    let (file, _, chunks) = multi_chunk_file(&e.source).await;
    let req = PutRequest { key: "hs/entry/x".into(), obj_id: file.to_string(), ..Default::default() };
    e.source.put(&e.public, &Caller::system(), req).await.unwrap();
    // The target already has the bytes; a source that cannot serve them must still fail.
    for (chunk, piece) in chunks.iter().zip([&b"first-"[..], b"second-", b"third"]) {
        e.target.put_chunk(chunk, piece).await.unwrap();
    }
    let (client, _) = client(e.source.clone(), e.public.clone(), true);
    let r = client.get(format!("https://zone-a.example/ndn/{}", file.to_string())).pull_to_named_store(&e.target).await;
    assert!(r.is_err());
}

#[tokio::test]
async fn sub_requests_inherit_cyfs_headers_only_on_the_same_host() {
    let e = env().await;
    let (file, _, _) = multi_chunk_file(&e.source).await;
    let req = PutRequest { key: "a/b".into(), obj_id: file.to_string(), ..Default::default() };
    e.source.put(&e.public, &Caller::system(), req).await.unwrap();
    let (client, transport) = client(e.source.clone(), e.public.clone(), false);
    client
        .get(format!("https://zone-a.example/ndn/{}", file.to_string()))
        .access_code("secret-code")
        .pull_to_named_store(&e.target)
        .await
        .unwrap();
    for (url, headers) in transport.requests.lock().unwrap().iter() {
        assert_eq!(headers.get("cyfs-access-code").and_then(|v| v.to_str().ok()), Some("secret-code"), "{url}");
    }
}

#[tokio::test]
async fn errors_never_show_the_token() {
    let e = env().await;
    let (client, _) = client(e.source.clone(), e.public.clone(), false);
    let (file, _, _) = multi_chunk_file(&e.source).await;
    let query = NdnHint { path: None, token: Some("bkt_topsecret".into()) }.to_query().unwrap();
    let err = client.get(format!("https://zone-a.example/ndn/{}?{query}", file.to_string())).pull_to_named_store(&e.target).await.err().unwrap();
    assert!(!err.to_string().contains("bkt_topsecret"), "{err}");
}

/// One-response-per-connection HTTP server: `redirect_to` answers 302, else 200.
async fn tiny_server(redirect_to: Option<String>, hits: Arc<std::sync::atomic::AtomicUsize>) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let response = match &redirect_to {
                Some(location) => format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
                None => "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            };
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    addr
}

/// A real socket: a redirect from a URL with a hint is not followed.
#[tokio::test]
async fn a_hint_url_never_follows_a_redirect() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let hits = Arc::new(AtomicUsize::new(0));
    let target = tiny_server(None, hits.clone()).await;
    let addr = tiny_server(Some(format!("http://{target}/elsewhere")), Arc::new(AtomicUsize::new(0))).await;

    let id = chunk_id(b"x");
    let client = CyfsNdnClient::new();
    let query = NdnHint { path: None, token: Some("bkt_secret".into()) }.to_query().unwrap();
    assert!(client.get(format!("http://{addr}/ndn/{}?{query}", id.to_string())).send().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 0, "the redirect target saw nothing");

    // Without a hint the client keeps following, as before.
    let _ = client.get(format!("http://{addr}/ndn/{}", id.to_string())).send().await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}
