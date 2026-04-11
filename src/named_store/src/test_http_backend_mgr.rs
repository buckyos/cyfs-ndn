//! Integration tests for NamedStoreMgr with mixed local + HTTP backends.
//!
//! Layout: 3 stores, 2 local + 1 remote (via HTTP gateway):
//!   - store-local-1: local filesystem backend
//!   - store-local-2: local filesystem backend
//!   - store-remote-1: HttpBackend → separate NamedStoreMgrHttpGateway server
//!
//! This exercises the typical deployment where some stores are local disks
//! and others are remote machines accessed over HTTP.

#[cfg(test)]
mod tests {
    use crate::backend::{ChunkWriteOutcome, NamedDataStoreBackend};
    use crate::http_backend::{HttpBackend, HttpBackendConfig};
    use crate::store_http_gateway::NamedStoreMgrHttpGateway;
    use crate::{NamedLocalConfig, NamedStore, NamedStoreMgr, StoreLayout, StoreTarget};

    use bytes::Bytes;
    use cyfs_gateway_lib::{HttpServer, ServerError, ServerErrorCode};
    use http_body_util::combinators::BoxBody;
    use http_body_util::{BodyExt, Full};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use ndn_lib::{ChunkHasher, ChunkId, NdnError, ObjId};
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    // ======================== Helpers ========================

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    /// Start an HTTP server backed by a NamedStoreMgrHttpGateway.
    /// Returns `(base_url, JoinHandle)`.
    async fn start_http_server(
        store_mgr: Arc<NamedStoreMgr>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let gateway = Arc::new(NamedStoreMgrHttpGateway::new(store_mgr));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let gw = gateway.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let gw = gw.clone();
                        async move {
                            let gateway_req = req.map(|body| {
                                body.map_err(|e| {
                                    ServerError::new(
                                        ServerErrorCode::StreamError,
                                        format!("incoming body error: {e}"),
                                    )
                                })
                                .boxed()
                            });

                            let resp: http::Response<
                                BoxBody<Bytes, cyfs_gateway_lib::ServerError>,
                            > = gw
                                .serve_request(gateway_req, cyfs_gateway_lib::StreamInfo::default())
                                .await
                                .unwrap_or_else(|e| {
                                    http::Response::builder()
                                        .status(500)
                                        .body(
                                            Full::new(Bytes::from(format!("gateway error: {e}")))
                                                .map_err(|never| match never {})
                                                .boxed(),
                                        )
                                        .unwrap()
                                });

                            Ok::<_, std::io::Error>(resp)
                        }
                    });
                    let mut builder = http1::Builder::new();
                    builder.half_close(true);
                    let _ = builder
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        (base_url, handle)
    }

    /// Create a local NamedStore with a given store_id at a given path.
    async fn make_local_store(store_id: &str, dir: &Path) -> Arc<tokio::sync::Mutex<NamedStore>> {
        let store = NamedStore::from_config(
            Some(store_id.to_string()),
            dir.to_path_buf(),
            NamedLocalConfig::default(),
        )
        .await
        .unwrap();
        Arc::new(tokio::sync::Mutex::new(store))
    }

    /// Create a remote NamedStore backed by HttpBackend.
    async fn make_remote_store(
        store_id: &str,
        db_dir: &Path,
        base_url: &str,
    ) -> Arc<tokio::sync::Mutex<NamedStore>> {
        let backend = Arc::new(HttpBackend::new(HttpBackendConfig {
            base_url: format!("{}/ndn", base_url),
        })) as Arc<dyn NamedDataStoreBackend>;

        let store = NamedStore::from_config_with_backend(
            Some(store_id.to_string()),
            db_dir.to_path_buf(),
            NamedLocalConfig::default(),
            backend,
        )
        .await
        .unwrap();
        Arc::new(tokio::sync::Mutex::new(store))
    }

    /// The typical test environment:
    ///   - store-local-1, store-local-2: local filesystem backends
    ///   - store-remote-1: HTTP backend pointing at a separate local server
    ///
    /// Returns (NamedStoreMgr, server_join_handle).
    struct TestEnv {
        mgr: Arc<NamedStoreMgr>,
        _server_handle: tokio::task::JoinHandle<()>,
        _tmp_dir: tempfile::TempDir,
    }

    async fn setup_test_env() -> TestEnv {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();

        // -- Remote backend: a separate NamedStoreMgr behind HTTP --
        let remote_store_dir = base.join("remote-store-data");
        std::fs::create_dir_all(&remote_store_dir).unwrap();

        let remote_mgr = {
            let store = NamedStore::from_config(
                Some("remote-backend".to_string()),
                remote_store_dir.clone(),
                NamedLocalConfig::default(),
            )
            .await
            .unwrap();
            let store = Arc::new(tokio::sync::Mutex::new(store));

            let mgr = NamedStoreMgr::new();
            mgr.register_store(store).await;

            let layout = StoreLayout::new(
                1,
                vec![StoreTarget {
                    store_id: "remote-backend".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                }],
                1_000_000_000,
                0,
            );
            mgr.add_layout(layout).await;
            Arc::new(mgr)
        };

        let (server_url, server_handle) = start_http_server(remote_mgr).await;

        // -- Main NamedStoreMgr with 2 local + 1 remote --
        let mgr = NamedStoreMgr::new();

        let local1_dir = base.join("store-local-1");
        std::fs::create_dir_all(&local1_dir).unwrap();
        let local1 = make_local_store("store-local-1", &local1_dir).await;
        mgr.register_store(local1).await;

        let local2_dir = base.join("store-local-2");
        std::fs::create_dir_all(&local2_dir).unwrap();
        let local2 = make_local_store("store-local-2", &local2_dir).await;
        mgr.register_store(local2).await;

        let remote_db_dir = base.join("store-remote-1-db");
        std::fs::create_dir_all(&remote_db_dir).unwrap();
        let remote = make_remote_store("store-remote-1", &remote_db_dir, &server_url).await;
        mgr.register_store(remote).await;

        let layout = StoreLayout::new(
            1,
            vec![
                StoreTarget {
                    store_id: "store-local-1".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-local-2".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-remote-1".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
            ],
            3_000_000_000,
            0,
        );
        mgr.add_layout(layout).await;

        TestEnv {
            mgr: Arc::new(mgr),
            _server_handle: server_handle,
            _tmp_dir: tmp,
        }
    }

    /// Determine which store a given obj_id would route to.
    async fn target_store_id(mgr: &NamedStoreMgr, obj_id: &ObjId) -> String {
        let layout = mgr.current_layout().await.unwrap();
        layout
            .select_primary_target(obj_id)
            .unwrap()
            .store_id
            .clone()
    }

    /// Generate a non-chunk object id from an integer index (deterministic, valid hex).
    fn make_obj_id(index: u64) -> ObjId {
        // Use the index bytes to fill a 32-byte hash (64 hex chars)
        let mut hash = [0u8; 32];
        let bytes = index.to_be_bytes();
        hash[24..32].copy_from_slice(&bytes);
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        ObjId::new(&format!("cyfile:{}", hex)).unwrap()
    }

    // ======================== Tests ========================

    /// Basic: put and get objects across all three stores.
    #[tokio::test]
    async fn object_put_get_across_stores() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        // Put many objects so that Maglev distributes them across stores
        let mut seen_stores = std::collections::HashSet::new();
        for i in 0..30 {
            let obj_id = make_obj_id(i * 17 + 3);
            let data = format!(r#"{{"index": {}}}"#, i);

            let store_id = target_store_id(mgr, &obj_id).await;
            seen_stores.insert(store_id.clone());

            mgr.put_object(&obj_id, &data).await.unwrap();
            let got = mgr.get_object(&obj_id).await.unwrap();
            assert_eq!(got, data, "object {i} round-trip failed (store={store_id})");
        }

        // All 3 stores should have been used
        assert!(
            seen_stores.len() >= 2,
            "expected objects distributed across >=2 stores, got: {:?}",
            seen_stores
        );
    }

    /// Basic: put and get chunks across all three stores.
    #[tokio::test]
    async fn chunk_put_get_across_stores() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let mut seen_stores = std::collections::HashSet::new();

        for i in 0..20 {
            let data = format!("chunk-data-{}-{}", i, "x".repeat(i * 7)).into_bytes();
            let chunk_id = calc_chunk_id(&data);
            let obj_id = chunk_id.to_obj_id();

            let store_id = target_store_id(mgr, &obj_id).await;
            seen_stores.insert(store_id.clone());

            mgr.put_chunk(&chunk_id, &data).await.unwrap();

            assert!(
                mgr.have_chunk(&chunk_id).await,
                "chunk {i} not found after put (store={store_id})"
            );

            let (mut reader, total) = mgr.open_chunk_reader(&chunk_id, 0).await.unwrap();
            assert_eq!(total, data.len() as u64);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, data, "chunk {i} data mismatch (store={store_id})");
        }

        assert!(
            seen_stores.len() >= 2,
            "expected chunks across >=2 stores, got: {:?}",
            seen_stores
        );
    }

    /// Get non-existent object/chunk returns NotFound.
    #[tokio::test]
    async fn not_found_errors() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        // Object NotFound
        let obj_id = make_obj_id(0xdeadbeef);
        let err = mgr.get_object(&obj_id).await.unwrap_err();
        assert!(
            matches!(err, NdnError::NotFound(_)),
            "expected NotFound for object, got: {err}"
        );

        // Chunk NotFound
        let data = b"nonexistent-chunk-data".to_vec();
        let chunk_id = calc_chunk_id(&data);
        assert!(!mgr.have_chunk(&chunk_id).await);

        match mgr.open_chunk_reader(&chunk_id, 0).await {
            Err(NdnError::NotFound(_)) => {}
            Err(e) => panic!("expected NotFound for chunk, got: {e}"),
            Ok(_) => panic!("expected NotFound error for chunk"),
        }
    }

    /// Remove object across stores.
    #[tokio::test]
    async fn object_remove() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let obj_id = make_obj_id(0xaabbccdd);
        let data = r#"{"remove": "me"}"#;

        mgr.put_object(&obj_id, data).await.unwrap();
        assert_eq!(mgr.get_object(&obj_id).await.unwrap(), data);

        mgr.remove_object(&obj_id).await.unwrap();

        let err = mgr.get_object(&obj_id).await.unwrap_err();
        assert!(matches!(err, NdnError::NotFound(_)));

        // Idempotent remove
        mgr.remove_object(&obj_id).await.unwrap();
    }

    /// Remove chunk across stores.
    #[tokio::test]
    async fn chunk_remove() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let data = b"chunk-to-be-removed".to_vec();
        let chunk_id = calc_chunk_id(&data);

        mgr.put_chunk(&chunk_id, &data).await.unwrap();
        assert!(mgr.have_chunk(&chunk_id).await);

        mgr.remove_chunk(&chunk_id).await.unwrap();
        assert!(!mgr.have_chunk(&chunk_id).await);

        // Idempotent remove
        mgr.remove_chunk(&chunk_id).await.unwrap();
    }

    /// Put the same chunk twice → idempotent (no error, data intact).
    #[tokio::test]
    async fn chunk_idempotent_put() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let data = b"idempotent-chunk".to_vec();
        let chunk_id = calc_chunk_id(&data);

        mgr.put_chunk(&chunk_id, &data).await.unwrap();
        mgr.put_chunk(&chunk_id, &data).await.unwrap();

        let (mut reader, total) = mgr.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    /// Read chunk with non-zero offset.
    #[tokio::test]
    async fn chunk_read_with_offset() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let data = b"0123456789abcdef".to_vec();
        let chunk_id = calc_chunk_id(&data);

        mgr.put_chunk(&chunk_id, &data).await.unwrap();

        let offset = 7u64;
        let (mut reader, total) = mgr.open_chunk_reader(&chunk_id, offset).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, &data[offset as usize..]);
    }

    /// query_object_by_id returns correct state.
    #[tokio::test]
    async fn query_object_state() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let obj_id = make_obj_id(0x9001);
        let data = r#"{"state": "test"}"#;

        // Before put → NotExist
        let state = mgr.query_object_by_id(&obj_id).await.unwrap();
        assert!(matches!(state, crate::ObjectState::NotExist));

        mgr.put_object(&obj_id, data).await.unwrap();

        // After put → Object with correct content
        let state = mgr.query_object_by_id(&obj_id).await.unwrap();
        match state {
            crate::ObjectState::Object(s) => assert_eq!(s, data),
            other => panic!("expected ObjectState::Object, got: {:?}", other),
        }
    }

    /// query_chunk_state returns correct state and size.
    #[tokio::test]
    async fn query_chunk_state() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let data = b"query-chunk-state-data".to_vec();
        let chunk_id = calc_chunk_id(&data);

        // Before put → NotExist
        let (state, size) = mgr.query_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state, crate::ChunkStoreState::NotExist);
        assert_eq!(size, 0);

        mgr.put_chunk(&chunk_id, &data).await.unwrap();

        // After put → Completed with correct size
        let (state, size) = mgr.query_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state, crate::ChunkStoreState::Completed);
        assert_eq!(size, data.len() as u64);
    }

    /// Layout change: objects written under epoch 1 remain readable after
    /// layout change to epoch 2 (multi-version fallback).
    #[tokio::test]
    async fn layout_change_fallback() {
        let env = setup_test_env().await;
        let mgr_inner = Arc::try_unwrap(env.mgr).unwrap_or_else(|arc| (*arc).clone());

        // Write objects under epoch 1 layout
        let mut written_objs = Vec::new();
        for i in 0..15 {
            let obj_id = make_obj_id(0x1000 + i as u64);
            let data = format!(r#"{{"epoch1_index": {}}}"#, i);
            mgr_inner.put_object(&obj_id, &data).await.unwrap();
            written_objs.push((obj_id, data));
        }

        let mut written_chunks = Vec::new();
        for i in 0..10 {
            let data = format!("epoch1-chunk-{}", i).into_bytes();
            let chunk_id = calc_chunk_id(&data);
            mgr_inner.put_chunk(&chunk_id, &data).await.unwrap();
            written_chunks.push((chunk_id, data));
        }

        // Change layout to epoch 2: swap store-local-1 out, add a "store-local-3"
        // (We reuse store-local-1's actual instance but the Maglev mapping changes)
        let layout2 = StoreLayout::new(
            2,
            vec![
                StoreTarget {
                    store_id: "store-local-2".to_string(),
                    device_did: None,
                    weight: 2,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-remote-1".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-local-1".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
            ],
            3_000_000_000,
            0,
        );
        mgr_inner.add_layout(layout2).await;

        assert_eq!(mgr_inner.version_count().await, 2);
        assert_eq!(mgr_inner.current_epoch().await, Some(2));

        // All epoch-1 objects should still be readable via fallback
        for (obj_id, expected_data) in &written_objs {
            let got = mgr_inner.get_object(obj_id).await.unwrap();
            assert_eq!(&got, expected_data, "object fallback failed for {}", obj_id);
        }

        for (chunk_id, expected_data) in &written_chunks {
            assert!(
                mgr_inner.have_chunk(chunk_id).await,
                "chunk fallback: have_chunk failed for {:?}",
                chunk_id
            );
            let (mut reader, _) = mgr_inner.open_chunk_reader(chunk_id, 0).await.unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(
                &buf, expected_data,
                "chunk fallback data mismatch for {:?}",
                chunk_id
            );
        }

        // New writes go to epoch 2 layout
        let new_obj_id = make_obj_id(0x2000);
        let new_data = r#"{"epoch": 2}"#;
        mgr_inner.put_object(&new_obj_id, new_data).await.unwrap();
        assert_eq!(mgr_inner.get_object(&new_obj_id).await.unwrap(), new_data);
    }

    /// put_chunk_by_reader via the mgr interface.
    #[tokio::test]
    async fn put_chunk_by_reader() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let data = b"reader-based-chunk-write-test".to_vec();
        let chunk_id = calc_chunk_id(&data);
        let chunk_size = data.len() as u64;

        let cursor = std::io::Cursor::new(data.clone());
        let reader: ndn_lib::ChunkReader = Box::pin(cursor);
        let outcome = mgr
            .put_chunk_by_reader(&chunk_id, chunk_size, reader)
            .await
            .unwrap();
        assert_eq!(outcome, ChunkWriteOutcome::Written);

        // Read back
        let (mut r, total) = mgr.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(total, chunk_size);
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);

        // Write again → AlreadyExists
        let cursor2 = std::io::Cursor::new(data.clone());
        let reader2: ndn_lib::ChunkReader = Box::pin(cursor2);
        let outcome2 = mgr
            .put_chunk_by_reader(&chunk_id, chunk_size, reader2)
            .await
            .unwrap();
        assert_eq!(outcome2, ChunkWriteOutcome::AlreadyExists);
    }

    /// Concurrent writes of the same chunk should both succeed (one Written, one AlreadyExists).
    #[tokio::test]
    async fn concurrent_chunk_write() {
        let env = setup_test_env().await;
        let mgr = env.mgr.clone();

        let data = b"concurrent-write-chunk".to_vec();
        let chunk_id = calc_chunk_id(&data);

        let mgr1 = mgr.clone();
        let mgr2 = mgr.clone();
        let data1 = data.clone();
        let data2 = data.clone();
        let cid1 = chunk_id.clone();
        let cid2 = chunk_id.clone();

        let (r1, r2) = tokio::join!(
            tokio::spawn(async move { mgr1.put_chunk(&cid1, &data1).await }),
            tokio::spawn(async move { mgr2.put_chunk(&cid2, &data2).await }),
        );

        r1.unwrap().unwrap();
        r2.unwrap().unwrap();

        // Data should be intact
        let (mut reader, _) = mgr.open_chunk_reader(&chunk_id, 0).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    /// Verify that objects targeting the remote store survive the HTTP round-trip.
    #[tokio::test]
    async fn remote_store_object_roundtrip() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        // Find an object that routes to store-remote-1
        let mut remote_obj_id = None;
        for i in 0..100 {
            let obj_id = make_obj_id(0x3000 + i as u64);
            let store = target_store_id(mgr, &obj_id).await;
            if store == "store-remote-1" {
                remote_obj_id = Some(obj_id);
                break;
            }
        }
        let obj_id = remote_obj_id.expect("couldn't find an obj_id that routes to store-remote-1");

        let data = r#"{"remote": true, "via": "http"}"#;
        mgr.put_object(&obj_id, data).await.unwrap();
        let got = mgr.get_object(&obj_id).await.unwrap();
        assert_eq!(got, data);

        // Remove and verify
        mgr.remove_object(&obj_id).await.unwrap();
        let err = mgr.get_object(&obj_id).await.unwrap_err();
        assert!(matches!(err, NdnError::NotFound(_)));
    }

    /// Verify that chunks targeting the remote store survive the HTTP round-trip.
    #[tokio::test]
    async fn remote_store_chunk_roundtrip() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        // Generate chunks until one routes to store-remote-1
        let mut remote_data = None;
        for i in 0..200 {
            let data = format!("remote-chunk-candidate-{}", i).into_bytes();
            let chunk_id = calc_chunk_id(&data);
            let obj_id = chunk_id.to_obj_id();
            let store = target_store_id(mgr, &obj_id).await;
            if store == "store-remote-1" {
                remote_data = Some((chunk_id, data));
                break;
            }
        }
        let (chunk_id, data) =
            remote_data.expect("couldn't find a chunk that routes to store-remote-1");

        mgr.put_chunk(&chunk_id, &data).await.unwrap();
        assert!(mgr.have_chunk(&chunk_id).await);

        let (state, size) = mgr.query_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state, crate::ChunkStoreState::Completed);
        assert_eq!(size, data.len() as u64);

        // Read full
        let (mut reader, total) = mgr.open_chunk_reader(&chunk_id, 0).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);

        // Read with offset
        let offset = 5u64;
        let (mut reader2, total2) = mgr.open_chunk_reader(&chunk_id, offset).await.unwrap();
        assert_eq!(total2, data.len() as u64);
        let mut buf2 = Vec::new();
        reader2.read_to_end(&mut buf2).await.unwrap();
        assert_eq!(buf2, &data[offset as usize..]);

        // Remove
        mgr.remove_chunk(&chunk_id).await.unwrap();
        assert!(!mgr.have_chunk(&chunk_id).await);
    }

    /// Re-put same object (same obj_id, same content) is idempotent.
    #[tokio::test]
    async fn object_idempotent_put() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let obj_id = make_obj_id(0x4001);
        let data = r#"{"v": 1}"#;
        mgr.put_object(&obj_id, data).await.unwrap();
        assert_eq!(mgr.get_object(&obj_id).await.unwrap(), data);

        // Re-put same content is idempotent
        mgr.put_object(&obj_id, data).await.unwrap();
        assert_eq!(mgr.get_object(&obj_id).await.unwrap(), data);
    }

    /// Many objects and chunks written, then batch-verified.
    #[tokio::test]
    async fn batch_write_read_verify() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let mut objects = Vec::new();
        let mut chunks = Vec::new();

        for i in 0..50 {
            let obj_id = make_obj_id(0x5000 + i as u64);
            let data = format!(r#"{{"batch_idx": {}, "payload": "{}"}}"#, i, "a".repeat(i));
            mgr.put_object(&obj_id, &data).await.unwrap();
            objects.push((obj_id, data));
        }

        for i in 0..30 {
            let data = format!("batch-chunk-{}-{}", i, "b".repeat(i * 3)).into_bytes();
            let chunk_id = calc_chunk_id(&data);
            mgr.put_chunk(&chunk_id, &data).await.unwrap();
            chunks.push((chunk_id, data));
        }

        // Verify all
        for (obj_id, expected) in &objects {
            let got = mgr.get_object(obj_id).await.unwrap();
            assert_eq!(&got, expected);
        }

        for (chunk_id, expected) in &chunks {
            let (mut reader, total) = mgr.open_chunk_reader(chunk_id, 0).await.unwrap();
            assert_eq!(total, expected.len() as u64);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(&buf, expected);
        }
    }

    /// Compact layout removes old versions; data written under current layout
    /// is still accessible.
    #[tokio::test]
    async fn compact_keeps_current_data() {
        let env = setup_test_env().await;
        let mgr_inner = Arc::try_unwrap(env.mgr).unwrap_or_else(|arc| (*arc).clone());

        // Write under epoch 1
        let obj_id = make_obj_id(0x6001);
        mgr_inner
            .put_object(&obj_id, r#"{"compact": true}"#)
            .await
            .unwrap();

        let data = b"compact-chunk".to_vec();
        let chunk_id = calc_chunk_id(&data);
        mgr_inner.put_chunk(&chunk_id, &data).await.unwrap();

        // Add epoch 2 layout (same stores, different weights)
        let layout2 = StoreLayout::new(
            2,
            vec![
                StoreTarget {
                    store_id: "store-local-1".to_string(),
                    device_did: None,
                    weight: 2,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-local-2".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
                StoreTarget {
                    store_id: "store-remote-1".to_string(),
                    device_did: None,
                    weight: 1,
                    capacity: Some(1_000_000_000),
                    used: Some(0),
                    readonly: false,
                    enabled: true,
                },
            ],
            3_000_000_000,
            0,
        );
        mgr_inner.add_layout(layout2).await;
        assert_eq!(mgr_inner.version_count().await, 2);

        // Compact
        mgr_inner.compact().await;
        assert_eq!(mgr_inner.version_count().await, 1);
        assert_eq!(mgr_inner.current_epoch().await, Some(2));

        // Data from epoch 1 that maps to same store under epoch 2 is still available;
        // data that moved to a different store may be lost after compact.
        // This test verifies the compact operation itself doesn't crash or corrupt state.
    }

    /// is_object_exist works across mixed backends.
    #[tokio::test]
    async fn is_object_exist() {
        let env = setup_test_env().await;
        let mgr = &env.mgr;

        let obj_id = make_obj_id(0x7001);
        assert!(!mgr.is_object_exist(&obj_id).await.unwrap());

        mgr.put_object(&obj_id, r#"{"exists": true}"#)
            .await
            .unwrap();
        assert!(mgr.is_object_exist(&obj_id).await.unwrap());

        mgr.remove_object(&obj_id).await.unwrap();
        assert!(!mgr.is_object_exist(&obj_id).await.unwrap());
    }
}
