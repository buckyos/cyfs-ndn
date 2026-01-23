use super::NamedDataMgr;
use crate::{
    build_named_object_by_json, ChunkHasher, ChunkReader, NdnError, NdnResult, ObjectState,
};
use buckyos_kit::buckyos_get_unix_timestamp;
use rusqlite::Connection;
use serde_json::json;
use std::io::Cursor;
use std::sync::Once;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;

static INIT_LOGGER: Once = Once::new();

fn init_logging() {
    INIT_LOGGER.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

async fn create_mgr() -> NdnResult<(TempDir, NamedDataMgr)> {
    init_logging();
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
async fn test_named_data_mgr_object_exist_and_invalid_type() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "exist-check"
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);

    let exists_before = mgr.is_object_exist(&obj_id).await.unwrap();
    assert!(!exists_before);

    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();
    let exists_after = mgr.is_object_exist(&obj_id).await.unwrap();
    assert!(exists_after);

    let chunk_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(b"invalid-type");
    let err = mgr
        .get_object_impl(&chunk_id.to_obj_id(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, NdnError::InvalidObjType(_)));
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
async fn test_named_data_mgr_select_obj_id_by_path_longest_match() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let root_value = json!({
        "name": "root-path"
    });
    let (root_id, root_str) = build_named_object_by_json("jobj", &root_value);
    mgr.put_object_impl(&root_id, &root_str).await.unwrap();

    let child_value = json!({
        "name": "child-path"
    });
    let (child_id, child_str) = build_named_object_by_json("jobj", &child_value);
    mgr.put_object_impl(&child_id, &child_str).await.unwrap();

    mgr.create_file_impl("/a", &root_id, "app", "user")
        .await
        .unwrap();
    mgr.create_file_impl("/a/b", &child_id, "app", "user")
        .await
        .unwrap();

    let (obj_id, _path_obj, relative_path) =
        mgr.select_obj_id_by_path_impl("/a/b/c").await.unwrap();
    assert_eq!(obj_id.to_string(), child_id.to_string());
    assert_eq!(relative_path, Some("/c".to_string()));
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
async fn test_named_data_mgr_get_object_inner_path() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "inner-path",
        "info": {
            "count": 7,
            "labels": ["a", "b"]
        }
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);

    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();
    let got = mgr
        .get_object_impl(&obj_id, Some("info/count".to_string()))
        .await
        .unwrap();
    assert_eq!(got, json!(7));
}

#[tokio::test]
async fn test_named_data_mgr_link_same_object() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "link-target"
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);
    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();

    let alias_value = json!({
        "name": "link-alias"
    });
    let (alias_id, _alias_str) = build_named_object_by_json("jobj2", &alias_value);

    mgr.link_same_object(&obj_id,&alias_id).await.unwrap();
    let got = mgr.get_object_impl(&alias_id, None).await.unwrap();
    assert_eq!(got, obj_value);

    let source = mgr.query_source_object_by_target(&alias_id).await.unwrap();
    assert_eq!(source.unwrap().to_string(), obj_id.to_string());
}

#[tokio::test]
async fn test_named_data_mgr_link_part_of_invalid() {
    //TODO: implement this test ,part of is for chunk
    assert!(true);
}

#[tokio::test]
async fn test_named_data_mgr_query_object_by_id_link_state() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let obj_value = json!({
        "name": "query-link"
    });
    let (obj_id, obj_str) = build_named_object_by_json("jobj", &obj_value);
    mgr.put_object_impl(&obj_id, &obj_str).await.unwrap();

    let alias_value = json!({
        "name": "query-alias"
    });
    let (alias_id, _alias_str) = build_named_object_by_json("jobj", &alias_value);
    mgr.link_same_object(&obj_id,&alias_id).await.unwrap();

    let state = mgr.query_object_by_id(&alias_id).await.unwrap();
    assert!(matches!(state, ObjectState::Link(_)));
}

#[tokio::test]
async fn test_named_data_mgr_put_chunk_invalid_id() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let chunk_data = b"named-data-chunk";
    let wrong_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(b"other-data");

    let err = mgr.put_chunk(&wrong_id, chunk_data, true).await.unwrap_err();
    assert!(matches!(err, NdnError::InvalidId(_)));
}

#[tokio::test]
async fn test_named_data_mgr_put_chunk_by_reader_impl() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let chunk_data = b"named-data-reader";
    let chunk_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(chunk_data);

    let mut reader: ChunkReader = Box::pin(Cursor::new(chunk_data.to_vec()));
    mgr.put_chunk_by_reader_impl(&chunk_id, chunk_data.len() as u64, &mut reader)
        .await
        .unwrap();

    let got = mgr.get_chunk_data(&chunk_id).await.unwrap();
    assert_eq!(got, chunk_data);
}

#[tokio::test]
async fn test_named_data_mgr_chunk_reader_by_path() {
    let (_temp_dir, mgr) = create_mgr().await.unwrap();
    let chunk_data = b"path-chunk-data";
    let chunk_id = ChunkHasher::new(None)
        .unwrap()
        .calc_chunk_id_from_bytes(chunk_data);

    mgr.put_chunk(&chunk_id, chunk_data, true).await.unwrap();
    let chunk_obj_id = chunk_id.to_obj_id();
    let path = "/ndn/chunk-path";
    mgr.create_file_impl(path, &chunk_obj_id, "app", "user")
        .await
        .unwrap();

    let (mut reader, chunk_size, read_chunk_id) =
        mgr.get_chunk_reader_by_path_impl(path, "user", "app", 0)
            .await
            .unwrap();
    assert_eq!(chunk_size, chunk_data.len() as u64);
    assert_eq!(read_chunk_id.to_string(), chunk_id.to_string());

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, chunk_data);
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
