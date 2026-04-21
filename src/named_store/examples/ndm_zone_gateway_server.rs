//! Standalone NDM Zone Gateway test server.
//!
//! Starts a `NamedStoreMgrZoneGateway` HTTP server on 127.0.0.1 with a random
//! port, prints `PORT:<port>` to stdout so that external test runners (e.g.
//! Deno) can discover the address.
//!
//! Usage:
//!   cargo run --example ndm_zone_gateway_server

use std::sync::Arc;

use bytes::Bytes;
use cyfs_gateway_lib::{HttpServer, ServerError, ServerErrorCode, StreamInfo};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use named_store::*;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    env_logger::init();

    // ---------- temp directories ----------
    let base_dir = std::env::temp_dir().join(format!("ndm_gw_test_{}", std::process::id()));
    let store_dir = base_dir.join("store");
    let cache_dir = base_dir.join("cache");
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");

    // ---------- NamedStoreMgr ----------
    let store = NamedStore::from_config(
        Some("test-store".to_string()),
        store_dir,
        NamedLocalConfig::default(),
    )
    .await
    .expect("create NamedStore");
    let store = Arc::new(tokio::sync::Mutex::new(store));

    let mgr = NamedDataMgr::new();
    mgr.register_store(store).await;

    let layout = StoreLayout::new(
        1,
        vec![StoreTarget {
            store_id: "test-store".to_string(),
            device_did: String::new(),
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
    let mgr = Arc::new(mgr);

    // ---------- Zone Gateway ----------
    let config = NdmZoneGatewayConfig {
        cache_dir: cache_dir.clone(),
        ..Default::default()
    };
    let gateway = Arc::new(NamedStoreMgrZoneGateway::new(mgr, config));

    // ---------- HTTP server ----------
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let port = listener.local_addr().unwrap().port();

    // Signal port to external test runner.
    println!("PORT:{}", port);

    // Cleanup on Ctrl-C
    let base_dir_clone = base_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = std::fs::remove_dir_all(&base_dir_clone);
        std::process::exit(0);
    });

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

    // cleanup
    let _ = std::fs::remove_dir_all(&base_dir);
}
