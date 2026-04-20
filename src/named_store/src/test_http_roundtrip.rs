//! Integration test: HttpBackend ↔ NamedStoreMgrHttpGateway roundtrip.
//!
//! Spins up a real HTTP/1.1 server backed by a temp-dir NamedStoreMgr,
//! then exercises every protocol operation through `HttpBackend`.

#[cfg(test)]
mod tests {
    use crate::backend::{
        ChunkPresence, ChunkWriteOutcome, NamedDataStoreBackend, NamedDataStoreBackendExt,
    };
    use crate::http_backend::{HttpBackendConfig, NamedStoreHttpBackend};
    use crate::store_http_gateway::NamedStoreMgrHttpGateway;
    use crate::{NamedDataMgr, NamedLocalConfig, NamedStore, StoreLayout, StoreTarget};

    use bytes::Bytes;
    use cyfs_gateway_lib::{HttpServer, ServerError, ServerErrorCode, StreamInfo};
    use http_body_util::{BodyExt, Full};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use ndn_lib::{ChunkHasher, ChunkId, NdnError, ObjId};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Duration};

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    /// Start a test HTTP server backed by `NamedStoreMgrHttpGateway`.
    /// Returns `(base_url, JoinHandle)`.
    async fn start_test_server(
        store_mgr: Arc<NamedDataMgr>,
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

                            let resp = gw
                                .serve_request(gateway_req, StreamInfo::default())
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

    /// Create a temp NamedStoreMgr with one store.
    async fn make_temp_store_mgr(dir: &std::path::Path) -> Arc<NamedDataMgr> {
        let store = NamedStore::from_config(
            Some("test-store".to_string()),
            dir.to_path_buf(),
            NamedLocalConfig::default(),
        )
        .await
        .unwrap();
        let store = Arc::new(tokio::sync::Mutex::new(store));

        let mgr = NamedDataMgr::new();
        mgr.register_store(store).await;

        let layout = StoreLayout::new(
            1,
            vec![StoreTarget {
                store_id: "test-store".to_string(),
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
    }

    #[tokio::test]
    async fn object_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = make_temp_store_mgr(tmp.path()).await;
        let (base_url, _handle) = start_test_server(mgr).await;

        let backend = NamedStoreHttpBackend::new(HttpBackendConfig {
            base_url: format!("{}/ndn", base_url),
        });

        // Use a non-chunk object type (cyfile is a file object type)
        let obj_id =
            ObjId::new("cyfile:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
                .unwrap();
        assert!(!obj_id.is_chunk(), "test obj_id must not be a chunk type");
        let obj_str = r#"{"test": "hello world"}"#;

        // get_object on non-existent → NotFound
        let err = backend.get_object(&obj_id).await.unwrap_err();
        assert!(
            matches!(err, NdnError::NotFound(_)),
            "expected NotFound, got: {err}"
        );

        // put_object
        backend.put_object(&obj_id, obj_str).await.unwrap();

        // get_object → round-trips
        let got = backend.get_object(&obj_id).await.unwrap();
        assert_eq!(got, obj_str);

        // put_object again (idempotent)
        backend.put_object(&obj_id, obj_str).await.unwrap();

        // remove_object
        backend.remove_object(&obj_id).await.unwrap();
        let err = backend.get_object(&obj_id).await.unwrap_err();
        assert!(matches!(err, NdnError::NotFound(_)));

        // remove again (idempotent)
        backend.remove_object(&obj_id).await.unwrap();
    }

    #[tokio::test]
    async fn chunk_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = make_temp_store_mgr(tmp.path()).await;
        let (base_url, _handle) = start_test_server(mgr).await;

        let backend = NamedStoreHttpBackend::new(HttpBackendConfig {
            base_url: format!("{}/ndn", base_url),
        });

        let data = b"hello named-data-http-store via HTTP".to_vec();
        let chunk_id = calc_chunk_id(&data);

        // get_chunk_state → NotExist
        let state = backend.get_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state.presence, ChunkPresence::NotExist);

        // open_chunk_reader on non-existent → NotFound
        match backend.open_chunk_reader(&chunk_id, 0).await {
            Err(NdnError::NotFound(_)) => {}
            Err(e) => panic!("expected NotFound, got: {e}"),
            Ok(_) => panic!("expected NotFound error"),
        }

        // write chunk
        let outcome = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome, ChunkWriteOutcome::Written);

        // get_chunk_state → Completed
        let state = backend.get_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state.presence, ChunkPresence::Completed);
        assert_eq!(state.chunk_size, data.len() as u64);

        // write again → AlreadyExists
        let outcome2 = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome2, ChunkWriteOutcome::AlreadyExists);

        // read full chunk
        let read_back = backend.get_chunk_data(&chunk_id).await.unwrap();
        assert_eq!(read_back, data);

        // read with offset
        let (mut reader, total) = backend.open_chunk_reader(&chunk_id, 6).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, &data[6..]);

        // delete chunk
        backend.remove_chunk(&chunk_id).await.unwrap();
        let state = backend.get_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(state.presence, ChunkPresence::NotExist);

        // delete again (idempotent)
        backend.remove_chunk(&chunk_id).await.unwrap();
    }

    #[tokio::test]
    async fn chunk_offset_too_large() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = make_temp_store_mgr(tmp.path()).await;
        let (base_url, _handle) = start_test_server(mgr).await;

        let backend = NamedStoreHttpBackend::new(HttpBackendConfig {
            base_url: format!("{}/ndn", base_url),
        });

        let data = b"short".to_vec();
        let chunk_id = calc_chunk_id(&data);
        backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();

        // offset beyond chunk size
        match backend
            .open_chunk_reader(&chunk_id, data.len() as u64 + 1)
            .await
        {
            Err(NdnError::OffsetTooLarge(_)) => {}
            Err(e) => panic!("expected OffsetTooLarge, got: {e}"),
            Ok(_) => panic!("expected OffsetTooLarge error"),
        }
    }

    #[tokio::test]
    async fn chunk_already_exists_returns_before_full_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = make_temp_store_mgr(tmp.path()).await;
        let (base_url, _handle) = start_test_server(mgr).await;

        let backend = NamedStoreHttpBackend::new(HttpBackendConfig {
            base_url: format!("{}/ndn", base_url),
        });

        let data = vec![0x5au8; 2 * 1024 * 1024];
        let chunk_id = calc_chunk_id(&data);
        backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();

        let written = Arc::new(AtomicUsize::new(0));
        let (mut writer, reader) = tokio::io::duplex(64 * 1024);
        let writer_data = data.clone();
        let written_clone = written.clone();
        let writer_task = tokio::spawn(async move {
            let step = 16 * 1024;
            let mut offset = 0usize;
            while offset < writer_data.len() {
                let end = std::cmp::min(offset + step, writer_data.len());
                writer.write_all(&writer_data[offset..end]).await?;
                written_clone.store(end, Ordering::SeqCst);
                offset = end;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            writer.shutdown().await
        });

        let outcome = timeout(
            Duration::from_secs(2),
            backend.open_chunk_writer(&chunk_id, data.len() as u64, Box::pin(reader)),
        )
        .await
        .expect("already-exists PUT should finish early")
        .unwrap();
        assert_eq!(outcome, ChunkWriteOutcome::AlreadyExists);

        let _ = writer_task.await;
        assert!(
            written.load(Ordering::SeqCst) < data.len(),
            "expected client upload to be interrupted before sending full body"
        );
    }
}
