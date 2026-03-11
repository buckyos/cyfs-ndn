use crate::{cacl_dir_object, cacl_file_object, register_named_store_mgr, CheckMode};
use named_store::{
    ChunkListReader, ChunkLocalInfo, ChunkStoreState, NamedLocalStore, NamedStoreMgr, StoreLayout,
    StoreTarget,
};
use ndn_lib::{
    ChunkHasher, ChunkId, DirObject, FileObject, HashMethod, NamedObject, ObjId, SimpleChunkList,
    SimpleMapItem, StoreMode, CHUNK_DEFAULT_SIZE,
};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;

fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| ((idx * 31 + idx / 7) % 251) as u8)
        .collect()
}

async fn create_test_store_mgr(base_dir: &Path) -> Arc<NamedStoreMgr> {
    let store = NamedLocalStore::get_named_store_by_path(base_dir.join("named_store"))
        .await
        .unwrap();
    let store_id = store.store_id().to_string();
    let store_ref = Arc::new(tokio::sync::Mutex::new(store));

    let store_mgr = Arc::new(NamedStoreMgr::new());
    store_mgr.register_store(store_ref).await;
    store_mgr
        .add_layout(StoreLayout::new(
            1,
            vec![StoreTarget {
                store_id,
                device_did: None,
                capacity: None,
                used: None,
                readonly: false,
                enabled: true,
                weight: 1,
            }],
            0,
            0,
        ))
        .await;

    store_mgr
}

async fn create_test_named_mgr(base_dir: &Path) -> (String, Arc<NamedStoreMgr>) {
    let store_mgr = create_test_store_mgr(base_dir).await;
    let instance_id = format!(
        "test-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    register_named_store_mgr(&instance_id, store_mgr.clone()).await;

    (instance_id, store_mgr)
}

fn clone_chunk_list(chunk_list: &SimpleChunkList) -> SimpleChunkList {
    SimpleChunkList::from_chunk_list(chunk_list.body.clone()).unwrap()
}

fn small_file_size() -> usize {
    16 * 1024 + 73
}

async fn write_small_test_file(path: &Path) -> Vec<u8> {
    let file_bytes = deterministic_bytes(small_file_size());
    tokio::fs::write(path, &file_bytes).await.unwrap();
    file_bytes
}

async fn create_dir_fixture(root: &Path) {
    let nested_dir = root.join("nested");
    tokio::fs::create_dir_all(&nested_dir).await.unwrap();
    tokio::fs::write(root.join("root.bin"), deterministic_bytes(16 * 1024))
        .await
        .unwrap();
    tokio::fs::write(
        nested_dir.join("child.bin"),
        deterministic_bytes(16 * 1024 + 19),
    )
    .await
    .unwrap();
}

fn assert_dir_shape(dir_obj: &DirObject) {
    assert_eq!(dir_obj.len(), 2);
    assert_eq!(dir_obj.file_count, 1);
    assert_eq!(dir_obj.file_size, 16 * 1024);
    assert_eq!(dir_obj.total_size, 16 * 1024 + 16 * 1024 + 19);
    assert!(dir_obj.is_file("root.bin"));
    assert!(dir_obj.is_directory("nested"));

    let root_item = dir_obj.get("root.bin").unwrap();
    let root_file: FileObject = serde_json::from_value(root_item.get_obj().unwrap()).unwrap();
    assert_eq!(root_file.size, (16 * 1024) as u64);

    let nested_item = dir_obj.get("nested").unwrap();
    match nested_item {
        SimpleMapItem::ObjId(obj_id) => assert!(obj_id.is_dir_object()),
        _ => panic!("nested item should be a dir object id"),
    }
}

async fn build_chunk_list_from_file(
    file_path: &Path,
    chunk_size: usize,
    store_mgr: &Arc<NamedStoreMgr>,
) -> SimpleChunkList {
    let file_bytes = tokio::fs::read(file_path).await.unwrap();
    let mut chunk_ids = Vec::new();

    for chunk_data in file_bytes.chunks(chunk_size) {
        let chunk_id = ChunkHasher::new_with_hash_method(HashMethod::Sha256)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(chunk_data)
            .unwrap();
        store_mgr
            .put_chunk(&chunk_id, chunk_data, true)
            .await
            .unwrap();
        chunk_ids.push(chunk_id);
    }

    SimpleChunkList::from_chunk_list(chunk_ids).unwrap()
}

async fn assert_chunk_list_matches_file(
    file_path: &Path,
    store_mgr: Arc<NamedStoreMgr>,
    chunk_list: &SimpleChunkList,
    offset: u64,
) {
    let file_bytes = tokio::fs::read(file_path).await.unwrap();
    let mut reader = ChunkListReader::new(
        store_mgr,
        clone_chunk_list(chunk_list),
        SeekFrom::Start(offset),
    )
    .await
    .unwrap();

    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).await.unwrap();

    assert_eq!(read_back, file_bytes[offset as usize..].to_vec());
}

async fn assert_object_stored(store_mgr: &Arc<NamedStoreMgr>, obj_id: &ObjId, expected: &str) {
    let stored = store_mgr.get_object(obj_id).await.unwrap();
    match (
        serde_json::from_str::<serde_json::Value>(&stored),
        serde_json::from_str::<serde_json::Value>(expected),
    ) {
        (Ok(stored_json), Ok(expected_json)) => assert_eq!(stored_json, expected_json),
        _ => assert_eq!(stored, expected),
    }
}

async fn assert_completed_chunk(
    store_mgr: &Arc<NamedStoreMgr>,
    chunk_id: &ChunkId,
    expected: &[u8],
) {
    let (state, size, _) = store_mgr.query_chunk_state(chunk_id).await.unwrap();
    assert_eq!(state, ChunkStoreState::Completed);
    assert_eq!(size, expected.len() as u64);

    let (mut reader, _) = store_mgr.open_chunk_reader(chunk_id, 0).await.unwrap();
    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).await.unwrap();
    assert_eq!(read_back, expected);
}

async fn assert_local_link_chunk(
    store_mgr: &Arc<NamedStoreMgr>,
    chunk_id: &ChunkId,
    expected_path: &Path,
    expected_range: Option<std::ops::Range<u64>>,
    expected: &[u8],
) {
    let (state, size, _) = store_mgr.query_chunk_state(chunk_id).await.unwrap();
    assert_eq!(size, expected.len() as u64);

    match state {
        ChunkStoreState::LocalLink(ChunkLocalInfo {
            path,
            qcid,
            last_modify_time,
            range,
        }) => {
            assert_eq!(path, expected_path.to_string_lossy().to_string());
            assert_eq!(range, expected_range);
            assert!(!qcid.is_empty());
            assert!(last_modify_time > 0);
        }
        other => panic!("expect LocalLink, got {:?}", other),
    }

    let (mut reader, _) = store_mgr.open_chunk_reader(chunk_id, 0).await.unwrap();
    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).await.unwrap();
    assert_eq!(read_back, expected);
}

fn embedded_file_object(dir_obj: &DirObject, name: &str) -> (ObjId, FileObject, String) {
    let item = dir_obj.get(name).unwrap();
    let (obj_id, obj_str) = item.get_obj_id().unwrap();
    let file_obj: FileObject = serde_json::from_value(item.get_obj().unwrap()).unwrap();
    (obj_id, file_obj, obj_str)
}

#[tokio::test]
async fn test_chunk_list_main() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_file.bin");
    let store_mgr = create_test_store_mgr(temp_dir.path()).await;

    let chunk_size = CHUNK_DEFAULT_SIZE as usize;
    let file_size = chunk_size + 137;
    let file_bytes = deterministic_bytes(file_size);
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    let chunk_list = build_chunk_list_from_file(&file_path, chunk_size, &store_mgr).await;
    assert_eq!(chunk_list.total_size, file_size as u64);
    assert_eq!(chunk_list.body.len(), file_size.div_ceil(chunk_size));

    let (chunk_list_id, chunk_list_str) = clone_chunk_list(&chunk_list).gen_obj_id();
    let parsed_chunk_list = SimpleChunkList::from_json(&chunk_list_str).unwrap();
    assert_eq!(parsed_chunk_list.total_size, chunk_list.total_size);
    assert_eq!(parsed_chunk_list.body, chunk_list.body);

    let file_template = FileObject::default();
    let (file_obj, file_obj_id, file_obj_str) = cacl_file_object(
        None,
        &file_path,
        &file_template,
        true,
        &CheckMode::ByFullHash,
        StoreMode::NoStore,
        None,
    )
    .await
    .unwrap();

    assert_eq!(file_obj.size, file_size as u64);
    assert_eq!(file_obj.content, chunk_list_id.to_string());

    let (expected_file_obj_id, expected_file_obj_str) = file_obj.gen_obj_id();
    assert_eq!(file_obj_id, expected_file_obj_id);
    assert_eq!(file_obj_str, expected_file_obj_str);

    assert_chunk_list_matches_file(&file_path, store_mgr.clone(), &chunk_list, 0).await;
    assert_chunk_list_matches_file(&file_path, store_mgr.clone(), &chunk_list, 5 * 1024 + 17).await;
    assert_chunk_list_matches_file(&file_path, store_mgr, &chunk_list, file_size as u64).await;
}

#[tokio::test]
async fn test_cacl_file_object_store_modes() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("single_chunk.bin");
    let file_bytes = write_small_test_file(&file_path).await;

    let file_template = FileObject::default();
    let (expected_obj, expected_id, expected_str) = cacl_file_object(
        None,
        &file_path,
        &file_template,
        true,
        &CheckMode::ByFullHash,
        StoreMode::NoStore,
        None,
    )
    .await
    .unwrap();

    assert_eq!(expected_obj.size, file_bytes.len() as u64);
    assert!(expected_obj.content.starts_with("mix256:"));

    let completed_base = temp_dir.path().join("named-mgr-completed");
    tokio::fs::create_dir_all(&completed_base).await.unwrap();
    let (completed_mgr_id, completed_store_mgr) = create_test_named_mgr(&completed_base).await;

    let (store_mode_obj, store_mode_id, store_mode_str) = cacl_file_object(
        Some(completed_mgr_id.as_str()),
        &file_path,
        &file_template,
        true,
        &CheckMode::ByFullHash,
        StoreMode::StoreInNamedMgr,
        None,
    )
    .await
    .unwrap();
    assert_eq!(store_mode_obj.content, expected_obj.content);
    assert_eq!(store_mode_id, expected_id);
    assert_eq!(store_mode_str, expected_str);
    assert_object_stored(&completed_store_mgr, &store_mode_id, &store_mode_str).await;

    let completed_chunk_id = ChunkId::new(&store_mode_obj.content).unwrap();
    assert_completed_chunk(&completed_store_mgr, &completed_chunk_id, &file_bytes).await;

    let linked_base = temp_dir.path().join("named-mgr-linked");
    tokio::fs::create_dir_all(&linked_base).await.unwrap();
    let (linked_mgr_id, linked_store_mgr) = create_test_named_mgr(&linked_base).await;

    let placeholder = temp_dir.path().join("ignored-placeholder.bin");
    let (local_obj, local_id, local_str) = cacl_file_object(
        Some(linked_mgr_id.as_str()),
        &file_path,
        &file_template,
        true,
        &CheckMode::ByFullHash,
        StoreMode::LocalFile(placeholder, 0..file_bytes.len() as u64, false),
        None,
    )
    .await
    .unwrap();

    assert_eq!(local_obj.content, expected_obj.content);
    assert_eq!(local_id, expected_id);
    assert_eq!(local_str, expected_str);
    assert_object_stored(&linked_store_mgr, &local_id, &local_str).await;

    let linked_chunk_id = ChunkId::new(&local_obj.content).unwrap();
    assert_local_link_chunk(
        &linked_store_mgr,
        &linked_chunk_id,
        &file_path,
        Some(0..file_bytes.len() as u64),
        &file_bytes,
    )
    .await;
}

#[tokio::test]
async fn test_cacl_dir_object_store_modes() {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source-dir");
    tokio::fs::create_dir_all(&source_dir).await.unwrap();
    create_dir_fixture(&source_dir).await;

    let file_template = FileObject::default();
    let (expected_dir_obj, expected_dir_id, expected_dir_str) = cacl_dir_object(
        None,
        &source_dir,
        &file_template,
        &CheckMode::ByFullHash,
        StoreMode::NoStore,
        None,
    )
    .await
    .unwrap();

    assert_dir_shape(&expected_dir_obj);
    let (recalc_id, recalc_str) = expected_dir_obj.gen_obj_id().unwrap();
    assert_eq!(expected_dir_id, recalc_id);
    assert_eq!(expected_dir_str, recalc_str);
    let expected_dir_store_str = serde_json::to_string(&expected_dir_obj).unwrap();

    let nested_dir = source_dir.join("nested");
    let (nested_dir_obj, nested_dir_id, _nested_dir_str) = cacl_dir_object(
        None,
        &nested_dir,
        &file_template,
        &CheckMode::ByFullHash,
        StoreMode::NoStore,
        None,
    )
    .await
    .unwrap();
    assert_eq!(nested_dir_obj.total_size, 16 * 1024 + 19);
    let nested_dir_store_str = serde_json::to_string(&nested_dir_obj).unwrap();
    match expected_dir_obj.get("nested").unwrap() {
        SimpleMapItem::ObjId(obj_id) => assert_eq!(obj_id, &nested_dir_id),
        _ => panic!("nested item should be a dir object id"),
    }

    let (root_file_obj_id, root_file_obj, root_file_obj_str) =
        embedded_file_object(&expected_dir_obj, "root.bin");
    let (child_file_obj_id, child_file_obj, child_file_obj_str) =
        embedded_file_object(&nested_dir_obj, "child.bin");
    let root_chunk_id = ChunkId::new(&root_file_obj.content).unwrap();
    let child_chunk_id = ChunkId::new(&child_file_obj.content).unwrap();
    let root_bytes = tokio::fs::read(source_dir.join("root.bin")).await.unwrap();
    let child_bytes = tokio::fs::read(nested_dir.join("child.bin")).await.unwrap();

    let completed_base = temp_dir.path().join("dir-store-completed");
    tokio::fs::create_dir_all(&completed_base).await.unwrap();
    let (completed_mgr_id, completed_store_mgr) = create_test_named_mgr(&completed_base).await;

    let (store_mode_dir_obj, store_mode_dir_id, store_mode_dir_str) = cacl_dir_object(
        Some(completed_mgr_id.as_str()),
        &source_dir,
        &file_template,
        &CheckMode::ByFullHash,
        StoreMode::StoreInNamedMgr,
        None,
    )
    .await
    .unwrap();
    assert_eq!(store_mode_dir_id, expected_dir_id);
    assert_eq!(store_mode_dir_str, expected_dir_str);
    assert_eq!(store_mode_dir_obj.total_size, expected_dir_obj.total_size);
    assert_object_stored(
        &completed_store_mgr,
        &store_mode_dir_id,
        &expected_dir_store_str,
    )
    .await;
    assert_object_stored(&completed_store_mgr, &nested_dir_id, &nested_dir_store_str).await;
    assert_object_stored(&completed_store_mgr, &root_file_obj_id, &root_file_obj_str).await;
    assert_object_stored(
        &completed_store_mgr,
        &child_file_obj_id,
        &child_file_obj_str,
    )
    .await;
    assert_completed_chunk(&completed_store_mgr, &root_chunk_id, &root_bytes).await;
    assert_completed_chunk(&completed_store_mgr, &child_chunk_id, &child_bytes).await;

    let linked_base = temp_dir.path().join("dir-store-linked");
    tokio::fs::create_dir_all(&linked_base).await.unwrap();
    let (linked_mgr_id, linked_store_mgr) = create_test_named_mgr(&linked_base).await;

    let placeholder = temp_dir.path().join("ignored-dir-placeholder");
    let (local_dir_obj, local_dir_id, local_dir_str) = cacl_dir_object(
        Some(linked_mgr_id.as_str()),
        &source_dir,
        &file_template,
        &CheckMode::ByFullHash,
        StoreMode::LocalFile(placeholder, 0..0, false),
        None,
    )
    .await
    .unwrap();

    assert_dir_shape(&local_dir_obj);
    assert_eq!(local_dir_id, expected_dir_id);
    assert_eq!(local_dir_str, expected_dir_str);
    assert_object_stored(&linked_store_mgr, &local_dir_id, &expected_dir_store_str).await;
    assert_object_stored(&linked_store_mgr, &nested_dir_id, &nested_dir_store_str).await;
    assert_object_stored(&linked_store_mgr, &root_file_obj_id, &root_file_obj_str).await;
    assert_object_stored(&linked_store_mgr, &child_file_obj_id, &child_file_obj_str).await;
    assert_local_link_chunk(
        &linked_store_mgr,
        &root_chunk_id,
        &source_dir.join("root.bin"),
        Some(0..root_bytes.len() as u64),
        &root_bytes,
    )
    .await;
    assert_local_link_chunk(
        &linked_store_mgr,
        &child_chunk_id,
        &nested_dir.join("child.bin"),
        Some(0..child_bytes.len() as u64),
        &child_bytes,
    )
    .await;
}
