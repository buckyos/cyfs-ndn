use super::*;
use ndn_lib::ChunkHasher;
use serde_json::json;
use std::collections::HashMap;
use tempfile::tempdir;

fn calc_chunk_id(data: &[u8]) -> ChunkId {
    ChunkHasher::new(None)
        .unwrap()
        .calc_mix_chunk_id_from_bytes(data)
        .unwrap()
}

#[tokio::test]
async fn get_store_mgr_uses_local_backend_when_device_is_not_in_link_map() {
    let temp_dir = tempdir().unwrap();
    let store_path = temp_dir.path().join("store-a");
    let config_path = temp_dir.path().join("store_layout.json");
    let current_node = "did:web:node-a.example.com".to_string();

    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "epoch": 1,
            "stores": [{
                "store_id": "store-a",
                "path": "store-a",
                "device_did": current_node,
                "weight": 1,
                "enabled": true,
                "readonly": false
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let mgr = NamedDataMgr::get_store_mgr(&config_path, &HashMap::new())
        .await
        .unwrap();

    let data = b"local backend path".to_vec();
    let chunk_id = calc_chunk_id(&data);
    mgr.put_chunk(&chunk_id, &data).await.unwrap();

    let chunk_file_exists = std::fs::read_dir(store_path.join("chunks"))
        .unwrap()
        .flatten()
        .next()
        .is_some();
    assert!(
        chunk_file_exists,
        "local backend should persist chunk files"
    );
}

#[tokio::test]
async fn get_store_mgr_uses_http_backend_when_device_is_in_link_map() {
    let temp_dir = tempdir().unwrap();
    let config_path = temp_dir.path().join("store_layout.json");
    let remote_node = "did:web:node-b.example.com".to_string();

    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "epoch": 1,
            "stores": [{
                "store_id": "store-b",
                "path": "store-b",
                "device_did": remote_node,
                "weight": 1,
                "enabled": true,
                "readonly": false
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let mgr = NamedDataMgr::get_store_mgr(
        &config_path,
        &HashMap::from([(
            "did:web:node-b.example.com".to_string(),
            "http://127.0.0.1:9/ndn".to_string(),
        )]),
    )
    .await
    .unwrap();

    let data = b"remote backend path".to_vec();
    let chunk_id = calc_chunk_id(&data);
    let err = mgr.put_chunk(&chunk_id, &data).await.unwrap_err();

    assert!(
        matches!(err, NdnError::RemoteError(_)),
        "expected remote backend transport error, got {err}"
    );
}

#[test]
fn remote_store_base_url_uses_link_map() {
    let entry = StoreConfigEntry {
        device_did: "did:bns:store-node".to_string(),
        ..Default::default()
    };

    let links = HashMap::from([(
        "did:bns:store-node".to_string(),
        "http://store-node.bns.did/ndn".to_string(),
    )]);
    let base_url = resolve_remote_store_base_url(&entry, &links).unwrap();
    assert_eq!(base_url, "http://store-node.bns.did/ndn");
}
