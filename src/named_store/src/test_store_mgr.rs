use super::*;
use crate::StoreTarget;

fn create_test_obj_id(obj_type: &str, hash_hex: &str) -> ObjId {
    ObjId::new(&format!("{}:{}", obj_type, hash_hex)).unwrap()
}

fn create_test_target(store_id: &str, weight: u32, enabled: bool, readonly: bool) -> StoreTarget {
    StoreTarget {
        store_id: store_id.to_string(),
        device_did: None,
        capacity: Some(1000),
        used: Some(100),
        readonly,
        enabled,
        weight,
    }
}

fn create_layout_with_epoch(epoch: u64, targets: Vec<StoreTarget>) -> StoreLayout {
    StoreLayout::new(epoch, targets, 10000, 1000)
}

#[test]
fn test_resolve_next_obj_cache_put_get() {
    let mut cache = ResolveNextObjCache::new(4);
    let src_obj_id = create_test_obj_id("cydir", "01");
    let next_obj_id = create_test_obj_id("cyfile", "02");

    cache.put(
        &src_obj_id,
        "/a/b",
        ResolveNextObjCacheValue {
            next_obj_id: next_obj_id.clone(),
            next_path: Some("/c".to_string()),
            next_obj_str: Some("{\"k\":\"v\"}".to_string()),
        },
    );

    let cached = cache.get(&src_obj_id, "/a/b").unwrap();
    assert_eq!(cached.next_obj_id, next_obj_id);
    assert_eq!(cached.next_path, Some("/c".to_string()));
    assert_eq!(cached.next_obj_str, Some("{\"k\":\"v\"}".to_string()));
}

#[test]
fn test_resolve_next_obj_cache_lru() {
    let mut cache = ResolveNextObjCache::new(2);
    let obj_a = create_test_obj_id("cydir", "0a");
    let obj_b = create_test_obj_id("cydir", "0b");
    let obj_c = create_test_obj_id("cydir", "0c");

    cache.put(
        &obj_a,
        "/a",
        ResolveNextObjCacheValue {
            next_obj_id: create_test_obj_id("cyfile", "10"),
            next_path: None,
            next_obj_str: None,
        },
    );
    cache.put(
        &obj_b,
        "/b",
        ResolveNextObjCacheValue {
            next_obj_id: create_test_obj_id("cyfile", "11"),
            next_path: None,
            next_obj_str: None,
        },
    );

    // Refresh obj_a so obj_b becomes LRU.
    assert!(cache.get(&obj_a, "/a").is_some());

    cache.put(
        &obj_c,
        "/c",
        ResolveNextObjCacheValue {
            next_obj_id: create_test_obj_id("cyfile", "12"),
            next_path: None,
            next_obj_str: None,
        },
    );

    assert!(cache.get(&obj_a, "/a").is_some());
    assert!(cache.get(&obj_b, "/b").is_none());
    assert!(cache.get(&obj_c, "/c").is_some());
}

#[tokio::test]
async fn test_resolve_next_obj_reuse_cached_subpath() {
    let mgr = NamedStoreMgr::new();

    let dir1 = DirObject::new(Some("dir1".to_string()));
    let (dir1_obj_id, dir1_obj_str) = dir1.gen_obj_id().unwrap();
    let final_obj_id = create_test_obj_id("cyfile", "13");

    {
        let mut cache = mgr.resolve_next_obj_cache.lock().await;
        cache.put(
            &dir1_obj_id,
            "/dir3/filename",
            ResolveNextObjCacheValue {
                next_obj_id: final_obj_id.clone(),
                next_path: None,
                next_obj_str: None,
            },
        );
    }

    let mut dir2 = DirObject::new(Some("dir2".to_string()));
    dir2.object_map.insert(
        "dir1".to_string(),
        SimpleMapItem::Object(
            "cydir".to_string(),
            serde_json::from_str(dir1_obj_str.as_str()).unwrap(),
        ),
    );
    let dir2_obj_id = create_test_obj_id("cydir", "21");
    let dir2_obj_str = serde_json::to_string(&dir2).unwrap();

    let resolved = mgr
        .resolve_next_obj(&dir2_obj_id, dir2_obj_str.as_str(), "/dir1/dir3/filename")
        .await
        .unwrap();

    assert_eq!(resolved.0, final_obj_id);
    assert_eq!(resolved.1, None);
    assert_eq!(resolved.2, None);

    let nested_hit = {
        let mut cache = mgr.resolve_next_obj_cache.lock().await;
        cache.get(&dir2_obj_id, "/dir1/dir3/filename")
    };
    assert!(nested_hit.is_some());
}

#[tokio::test]
async fn test_store_layout_mgr_basic() {
    let mgr = NamedStoreMgr::new();

    // Initially empty
    assert_eq!(mgr.version_count().await, 0);
    assert!(mgr.current_layout().await.is_none());
    assert!(mgr.current_epoch().await.is_none());
}

#[tokio::test]
async fn test_store_layout_mgr_add_versions() {
    let mgr = NamedStoreMgr::new();

    let targets1 = vec![create_test_target("store1", 1, true, false)];
    let layout1 = create_layout_with_epoch(1, targets1);
    mgr.add_layout(layout1).await;

    assert_eq!(mgr.version_count().await, 1);
    assert_eq!(mgr.current_epoch().await, Some(1));

    // Add newer version
    let targets2 = vec![
        create_test_target("store1", 1, true, false),
        create_test_target("store2", 1, true, false),
    ];
    let layout2 = create_layout_with_epoch(2, targets2);
    mgr.add_layout(layout2).await;

    assert_eq!(mgr.version_count().await, 2);
    assert_eq!(mgr.current_epoch().await, Some(2));

    // Add even newer version
    let targets3 = vec![
        create_test_target("store1", 1, true, false),
        create_test_target("store2", 1, true, false),
        create_test_target("store3", 1, true, false),
    ];
    let layout3 = create_layout_with_epoch(3, targets3);
    mgr.add_layout(layout3).await;

    assert_eq!(mgr.version_count().await, 3);
    assert_eq!(mgr.current_epoch().await, Some(3));

    // Adding a 4th version should trim the oldest
    let targets4 = vec![
        create_test_target("store1", 1, true, false),
        create_test_target("store2", 1, true, false),
        create_test_target("store3", 1, true, false),
        create_test_target("store4", 1, true, false),
    ];
    let layout4 = create_layout_with_epoch(4, targets4);
    mgr.add_layout(layout4).await;

    assert_eq!(mgr.version_count().await, 3); // Still 3, oldest trimmed
    assert_eq!(mgr.current_epoch().await, Some(4));

    // Verify version 1 is gone
    assert!(mgr.get_layout(1).await.is_none());
    assert!(mgr.get_layout(2).await.is_some());
    assert!(mgr.get_layout(3).await.is_some());
    assert!(mgr.get_layout(4).await.is_some());
}

#[tokio::test]
async fn test_store_layout_mgr_version_ordering() {
    let mgr = NamedStoreMgr::new();

    // Add versions out of order
    let targets2 = vec![create_test_target("store1", 1, true, false)];
    let layout2 = create_layout_with_epoch(2, targets2);
    mgr.add_layout(layout2).await;

    let targets1 = vec![create_test_target("store1", 1, true, false)];
    let layout1 = create_layout_with_epoch(1, targets1);
    mgr.add_layout(layout1).await;

    let targets3 = vec![create_test_target("store1", 1, true, false)];
    let layout3 = create_layout_with_epoch(3, targets3);
    mgr.add_layout(layout3).await;

    // Current should be the newest (epoch 3)
    assert_eq!(mgr.current_epoch().await, Some(3));

    // Versions should be ordered newest first
    let versions = mgr.all_versions().await;
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0].epoch, 3);
    assert_eq!(versions[1].epoch, 2);
    assert_eq!(versions[2].epoch, 1);
}

#[tokio::test]
async fn test_store_layout_mgr_compact() {
    let mgr = NamedStoreMgr::new();

    for epoch in 1..=3 {
        let targets = vec![create_test_target("store1", 1, true, false)];
        let layout = create_layout_with_epoch(epoch, targets);
        mgr.add_layout(layout).await;
    }

    assert_eq!(mgr.version_count().await, 3);

    mgr.compact().await;

    assert_eq!(mgr.version_count().await, 1);
    assert_eq!(mgr.current_epoch().await, Some(3));
}

#[tokio::test]
async fn test_store_layout_mgr_custom_max_versions() {
    let mgr = NamedStoreMgr::with_max_versions(2);

    for epoch in 1..=5 {
        let targets = vec![create_test_target("store1", 1, true, false)];
        let layout = create_layout_with_epoch(epoch, targets);
        mgr.add_layout(layout).await;
    }

    // Should only keep 2 versions
    assert_eq!(mgr.version_count().await, 2);
    assert_eq!(mgr.current_epoch().await, Some(5));

    // Only epochs 4 and 5 should exist
    assert!(mgr.get_layout(3).await.is_none());
    assert!(mgr.get_layout(4).await.is_some());
    assert!(mgr.get_layout(5).await.is_some());
}

#[tokio::test]
async fn test_store_layout_mgr_replace_same_epoch() {
    let mgr = NamedStoreMgr::new();

    let targets1 = vec![create_test_target("store1", 1, true, false)];
    let layout1 = create_layout_with_epoch(1, targets1);
    mgr.add_layout(layout1).await;

    assert_eq!(mgr.version_count().await, 1);

    // Add layout with same epoch should replace
    let targets1_updated = vec![
        create_test_target("store1", 1, true, false),
        create_test_target("store2", 1, true, false),
    ];
    let layout1_updated = create_layout_with_epoch(1, targets1_updated);
    mgr.add_layout(layout1_updated).await;

    // Should still be 1 version, not 2
    assert_eq!(mgr.version_count().await, 1);

    // The updated layout should have 2 targets
    let current = mgr.current_layout().await.unwrap();
    assert_eq!(current.targets.len(), 2);
}
