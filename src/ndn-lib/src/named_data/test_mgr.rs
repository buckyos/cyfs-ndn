use super::NamedDataMgr;
use crate::{
    build_named_object_by_json, ChunkHasher, NdnError, NdnResult,
};
use buckyos_kit::buckyos_get_unix_timestamp;
use rusqlite::Connection;
use serde_json::json;
use tempfile::TempDir;

async fn create_mgr() -> NdnResult<(TempDir, NamedDataMgr)> {
    let temp_dir = tempfile::tempdir().unwrap();
    let mgr_root = temp_dir.path().join("named_data_mgr");
    let mgr = NamedDataMgr::get_named_data_mgr_by_path(mgr_root).await?;
    Ok((temp_dir, mgr))
}

#[tokio::test]
async fn test_named_data_mgr_put_get_object() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "unit-test",
        "count": 1
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);

    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();
    let got = mgr.get_object_impl(&obj_id, None).await.unwrap();
    assert_eq!(got, obj_value);
}

#[tokio::test]
async fn test_named_data_mgr_get_missing_object() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "missing"
    });
    let (obj_id, _obj_str) = build_named_object_by_json("jobj", &obj_value);

    let err = mgr.get_object_impl(&obj_id, None).await.unwrap_err();
    assert!(matches!(err, NdnError::NotFound(_)));
}

#[tokio::test]
async fn test_named_data_mgr_path_management() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "path-target"
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);
    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();

    let path = "/ndn/path-test";
    mgr.create_file_impl(path, &obj_id, "app", "user")
        .await
        .unwrap();

    let (stored_obj_id, _path_obj_jwt) = mgr.get_obj_id_by_path_impl(path).await.unwrap();
    assert_eq!(stored_obj_id.to_string(), obj_id.to_string());

    let new_obj_value = json!({
        "name": "path-target-v2"
    });
    let (new_obj_id, new_obj_str) = build_named_object_by_json("jobj", &new_obj_value);
    mgr.put_object_impl(&new_obj_id, &new_obj_str).await.unwrap();
    mgr.set_file_impl(path, &new_obj_id, "app", "user")
        .await
        .unwrap();

    let (updated_obj_id, _path_obj_jwt) = mgr.get_obj_id_by_path_impl(path).await.unwrap();
    assert_eq!(updated_obj_id.to_string(), new_obj_id.to_string());

    mgr.remove_file_impl(path).await.unwrap();
    assert!(mgr.get_obj_id_by_path_impl(path).await.is_err());
}

#[tokio::test]
async fn test_named_data_mgr_put_get_chunk() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let chunk_data = b"named-data-chunk";
    let chunk_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(chunk_data);

    mgr.put_chunk(&chunk_id, chunk_data, true).await.unwrap();
    let got = mgr.get_chunk_data(&chunk_id).await.unwrap();
    assert_eq!(got, chunk_data);
}

#[tokio::test]
async fn test_named_data_mgr_gc_objects() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();

    let chunk_data = b"gc-chunk";
    let chunk_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(chunk_data);
    mgr.put_chunk(&chunk_id, chunk_data, true).await.unwrap();

    let obj_value = json!({
        "name": "gc-object"
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);
    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();

    let db_path = mgr.db().db_path.clone();
    let conn = Connection::open(&db_path).unwrap();
    let old_time = buckyos_get_unix_timestamp() as i64 - 60 * 60 * 24 - 10;
    conn.execute(
        "UPDATE chunk_items SET update_time = ?1, ref_count = 0 WHERE chunk_id = ?2",
        (old_time, chunk_id.to_string()),
    )
    .unwrap();
    conn.execute(
        "UPDATE objects SET last_access_time = ?1, ref_count = 0 WHERE obj_id = ?2",
        (old_time, obj_id.to_string()),
    )
    .unwrap();

    drop(conn);
    NamedDataMgr::gc_objects(&db_path).await.unwrap();

    let conn = Connection::open(&db_path).unwrap();
    let chunk_left: i64 = conn
        .query_row(
            "SELECT COUNT(1) FROM chunk_items WHERE chunk_id = ?1",
            [chunk_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let obj_left: i64 = conn
        .query_row(
            "SELECT COUNT(1) FROM objects WHERE obj_id = ?1",
            [obj_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(chunk_left, 0);
    assert_eq!(obj_left, 0);
}
