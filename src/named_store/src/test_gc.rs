//! GC integration tests — single-bucket ("当前桶") perspective.
//!
//! These tests exercise pin / unpin / fs_acquire / fs_release / apply_edge
//! and verify:
//!   - eviction_class, children_expanded, state, fs_anchor_count
//!   - outbox entries (add / remove, correct referee / referrer)
//!   - GC correctly deletes class-0 objects and refuses to touch class 1/2

use super::*;
use ndn_lib::ObjId;
use std::collections::HashSet;

// ───────────────────── helpers ─────────────────────

/// Create a deterministic hex hash from a tag string.
fn tag_to_hex(tag: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    tag.hash(&mut h);
    let v = h.finish();
    format!("{:016x}{:016x}", v, v ^ 0xdeadbeef12345678)
}

/// Create an ObjId for a dir object with a fake hash.
fn dir_id(tag: &str) -> ObjId {
    ObjId::new(&format!("cydir:{}", tag_to_hex(tag))).unwrap()
}

/// Create an ObjId for a file object with a fake hash.
fn file_id(tag: &str) -> ObjId {
    ObjId::new(&format!("cyfile:{}", tag_to_hex(tag))).unwrap()
}

/// Build a DirObject JSON containing references to the given child ObjIds.
/// Each child is stored as a SimpleMapItem::ObjId (string form).
fn make_dir_json(children: &[(&str, &ObjId)]) -> String {
    use serde_json::json;
    let mut body = serde_json::Map::new();
    for (name, obj_id) in children {
        body.insert(name.to_string(), json!(obj_id.to_string()));
    }
    let obj = json!({
        "create_time": 1000,
        "last_update_time": 1000,
        "total_size": 0,
        "file_count": children.len(),
        "file_size": 0,
        "body": body,
    });
    serde_json::to_string(&obj).unwrap()
}

/// Build a FileObject JSON referencing a single content chunk / obj.
fn make_file_json(content_id: &ObjId) -> String {
    use serde_json::json;
    let obj = json!({
        "create_time": 1000,
        "last_update_time": 1000,
        "size": 1024,
        "content": content_id.to_string(),
    });
    serde_json::to_string(&obj).unwrap()
}

/// Create a fresh in-memory DB for testing.
fn test_db() -> NamedLocalStoreDB {
    NamedLocalStoreDB::new(":memory:".to_string()).unwrap()
}

/// Drain *all* outbox entries by applying them back to the same DB
/// (simulating single-bucket loopback router). Returns the drained entries
/// so tests can inspect them.
fn drain_outbox(db: &NamedLocalStoreDB) -> Vec<OutboxEntry> {
    let mut all = Vec::new();
    loop {
        let batch = db.fetch_outbox_ready(100).unwrap();
        if batch.is_empty() {
            break;
        }
        for entry in &batch {
            db.apply_edge(&entry.msg).unwrap();
            db.complete_outbox_entry(entry.seq).unwrap();
        }
        all.extend(batch);
    }
    all
}

/// Collect outbox entries (op, referee, referrer) without draining.
fn peek_outbox(db: &NamedLocalStoreDB) -> Vec<(EdgeOp, String, String)> {
    db.fetch_outbox_ready(1000)
        .unwrap()
        .into_iter()
        .map(|e| (e.msg.op, e.msg.referee.to_string(), e.msg.referrer.to_string()))
        .collect()
}

/// Assert an object's expand debug matches expectations.
fn assert_expand(
    db: &NamedLocalStoreDB,
    obj_id: &ObjId,
    expected_class: u32,
    expected_expanded: bool,
    expected_state: ItemState,
) {
    let d = db.debug_dump_expand_state(obj_id).unwrap();
    assert_eq!(d.eviction_class, expected_class,
        "obj {} class mismatch: got {}, want {}", obj_id, d.eviction_class, expected_class);
    assert_eq!(d.children_expanded, expected_expanded,
        "obj {} children_expanded mismatch: got {}, want {}", obj_id, d.children_expanded, expected_expanded);
    assert_eq!(d.state, expected_state,
        "obj {} state mismatch: got {:?}, want {:?}", obj_id, d.state, expected_state);
}

// ═══════════════════════════════════════════════════
// Test cases
// ═══════════════════════════════════════════════════

// ──────────── 1. Basic put: class 0, no outbox ────────────

#[test]
fn test_plain_put_is_class0_no_outbox() {
    let db = test_db();
    let a = dir_id("aaa1");
    let b = file_id("bbb1");
    let dir_json = make_dir_json(&[("child", &b)]);

    db.set_object(&a, "cydir", &dir_json).unwrap();
    db.set_object(&b, "cyfile", &make_file_json(&dir_id("dummy_content"))).unwrap();

    // Both are class 0, not expanded, present
    assert_expand(&db, &a, 0, false, ItemState::Present);
    assert_expand(&db, &b, 0, false, ItemState::Present);

    // No outbox
    assert!(peek_outbox(&db).is_empty());
}

// ──────────── 2. Recursive pin on root ────────────

#[test]
fn test_recursive_pin_expands_children_outbox() {
    let db = test_db();

    // Tree: root_dir -> child_file -> content_chunk
    let content = dir_id("chunk01");
    let child = file_id("file01");
    let root = dir_id("root01");

    // Put objects bottom-up
    db.set_object(&content, "cydir", "{}").unwrap();
    db.set_object(&child, "cyfile", &make_file_json(&content)).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("f", &child)])).unwrap();

    // Pin root recursively
    db.pin(&root, "user1", PinScope::Recursive, None).unwrap();

    // Root: class 2, expanded
    assert_expand(&db, &root, 2, true, ItemState::Present);

    // Outbox should have add(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Add);
    assert_eq!(outbox[0].1, child.to_string()); // referee = child
    assert_eq!(outbox[0].2, root.to_string());  // referrer = root

    // Drain outbox (loopback): child gets incoming_ref, becomes class 1, expands
    let drained = drain_outbox(&db);
    assert!(drained.len() >= 1);

    assert_expand(&db, &child, 1, true, ItemState::Present);

    // After child expands, its child (content) should get an outbox entry too
    // (the drain_outbox loop already applied it)
    // content has incoming_ref so should_expand=true → children_expanded=true
    // (even though it has no parseable children, the flag reflects "expansion attempted")
    assert_expand(&db, &content, 1, true, ItemState::Present);
}

// ──────────── 3. Unpin root => cascade remove ────────────

#[test]
fn test_unpin_recursive_cascades_remove() {
    let db = test_db();

    let child = file_id("c01");
    let root = dir_id("r01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Pin + drain
    db.pin(&root, "owner1", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &root, 2, true, ItemState::Present);
    assert_expand(&db, &child, 1, true, ItemState::Present);

    // Unpin
    db.unpin(&root, "owner1").unwrap();

    // Root: class 0, children_expanded = false (reconcile flipped it)
    assert_expand(&db, &root, 0, false, ItemState::Present);

    // Outbox should have remove(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Remove);
    assert_eq!(outbox[0].1, child.to_string());

    // Drain: child loses incoming, becomes class 0
    drain_outbox(&db);
    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 4. GC evicts class 0 only ────────────

#[test]
fn test_gc_evicts_class0_preserves_class1_class2() {
    let db = test_db();

    let free_obj = dir_id("free01");
    let pinned_obj = dir_id("pinned01");
    let referenced_obj = file_id("ref01");

    // All present
    db.set_object(&free_obj, "cydir", "{}").unwrap();
    db.set_object(&pinned_obj, "cydir", &make_dir_json(&[("r", &referenced_obj)])).unwrap();
    db.set_object(&referenced_obj, "cyfile", &make_file_json(&dir_id("x"))).unwrap();

    // Pin pinned_obj -> referenced_obj becomes class 1 via cascade
    db.pin(&pinned_obj, "o1", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &free_obj, 0, false, ItemState::Present);
    assert_expand(&db, &pinned_obj, 2, true, ItemState::Present);
    assert_expand(&db, &referenced_obj, 1, true, ItemState::Present);

    // GC: try to evict everything
    let candidates = db.list_gc_candidates(100).unwrap();
    // Only free_obj should be a candidate
    let candidate_ids: Vec<&str> = candidates.iter().map(|(id, _)| id.as_str()).collect();
    assert!(candidate_ids.contains(&free_obj.to_string().as_str()));
    assert!(!candidate_ids.contains(&pinned_obj.to_string().as_str()));
    assert!(!candidate_ids.contains(&referenced_obj.to_string().as_str()));

    // Actually evict
    let freed = db.try_evict_object(&free_obj.to_string()).unwrap();
    assert!(freed > 0);

    // Verify free_obj is gone
    assert!(!db.has_object_row(&free_obj.to_string()).unwrap());

    // Verify pinned & referenced are still there
    assert!(db.has_object_row(&pinned_obj.to_string()).unwrap());
    assert!(db.has_object_row(&referenced_obj.to_string()).unwrap());
}

// ──────────── 5. GC: full cycle pin→unpin→evict ────────────

#[test]
fn test_gc_full_cycle_pin_unpin_evict() {
    let db = test_db();

    let child = file_id("fc01");
    let root = dir_id("fr01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Pin recursive
    db.pin(&root, "user", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    // Both protected
    assert!(db.try_evict_object(&root.to_string()).unwrap() == 0);
    assert!(db.try_evict_object(&child.to_string()).unwrap() == 0);

    // Unpin
    db.unpin(&root, "user").unwrap();
    drain_outbox(&db);

    // Now both class 0
    assert_expand(&db, &root, 0, false, ItemState::Present);
    assert_expand(&db, &child, 0, false, ItemState::Present);

    // GC should succeed on both
    let freed_root = db.try_evict_object(&root.to_string()).unwrap();
    assert!(freed_root > 0);
    let freed_child = db.try_evict_object(&child.to_string()).unwrap();
    assert!(freed_child > 0);

    assert!(!db.has_object_row(&root.to_string()).unwrap());
    assert!(!db.has_object_row(&child.to_string()).unwrap());
}

// ──────────── 6. fs_acquire / fs_release ────────────

#[test]
fn test_fs_acquire_release_lifecycle() {
    let db = test_db();

    let child = file_id("fsc01");
    let root = dir_id("fsr01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // fs_acquire
    db.fs_acquire(&root, 100, 1).unwrap();

    assert_expand(&db, &root, 2, true, ItemState::Present);
    let d = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d.fs_anchor_count, 1);

    // Outbox: add(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Add);
    drain_outbox(&db);

    assert_expand(&db, &child, 1, true, ItemState::Present);

    // fs_release
    db.fs_release(&root, 100, 1).unwrap();

    assert_expand(&db, &root, 0, false, ItemState::Present);
    let d2 = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d2.fs_anchor_count, 0);

    // Outbox: remove(child <- root)
    let outbox2 = peek_outbox(&db);
    assert_eq!(outbox2.len(), 1);
    assert_eq!(outbox2[0].0, EdgeOp::Remove);
    drain_outbox(&db);

    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 7. Shadow → present triggers expand ────────────

#[test]
fn test_shadow_to_present_triggers_expand() {
    let db = test_db();

    let root = dir_id("sr01");
    let child = file_id("sc01");

    // Pin before object content arrives (creates shadow)
    db.pin(&root, "user", PinScope::Recursive, None).unwrap();

    // Root is shadow, class 2 but NOT expanded (shadow can't expand)
    assert_expand(&db, &root, 2, false, ItemState::Shadow);

    // cascade_state should be Pending
    let cs = db.anchor_state(&root, "user").unwrap();
    assert_eq!(cs, CascadeStateP0::Pending);

    // No outbox yet (shadow can't parse children)
    assert!(peek_outbox(&db).is_empty());

    // Now put the actual object content
    let dir_json = make_dir_json(&[("c", &child)]);
    db.put_object_gc_aware(&root, "cydir", &dir_json).unwrap();

    // Root: still class 2, now expanded, present
    assert_expand(&db, &root, 2, true, ItemState::Present);

    // cascade_state promoted to Materializing
    let cs2 = db.anchor_state(&root, "user").unwrap();
    assert_eq!(cs2, CascadeStateP0::Materializing);

    // Outbox: add(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Add);
    assert_eq!(outbox[0].1, child.to_string());
}

// ──────────── 8. Skeleton pin blocks expansion ────────────

#[test]
fn test_skeleton_pin_blocks_expansion() {
    let db = test_db();

    let child = file_id("skc01");
    let root = dir_id("skr01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Skeleton pin: class 2 but should NOT expand
    db.pin(&root, "skel_owner", PinScope::Skeleton, None).unwrap();

    assert_expand(&db, &root, 2, false, ItemState::Present);
    assert!(peek_outbox(&db).is_empty());

    // Child stays class 0
    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 9. Skeleton added after expansion → tear-down ────────────

#[test]
fn test_skeleton_after_expansion_tears_down() {
    let db = test_db();

    let child = file_id("sk2c01");
    let root = dir_id("sk2r01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Recursive pin → expand
    db.pin(&root, "rec_owner", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &root, 2, true, ItemState::Present);
    assert_expand(&db, &child, 1, true, ItemState::Present);

    // Now add skeleton pin → should tear down children
    db.pin(&root, "skel_owner", PinScope::Skeleton, None).unwrap();

    // Root: class 2, NOT expanded (skeleton blocks)
    assert_expand(&db, &root, 2, false, ItemState::Present);

    // Outbox: remove(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Remove);
    drain_outbox(&db);

    // Child: class 0 (lost incoming)
    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 10. Remove skeleton → auto re-expand ────────────

#[test]
fn test_remove_skeleton_restores_expansion() {
    let db = test_db();

    let child = file_id("sk3c01");
    let root = dir_id("sk3r01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Recursive + Skeleton
    db.pin(&root, "rec_owner", PinScope::Recursive, None).unwrap();
    db.pin(&root, "skel_owner", PinScope::Skeleton, None).unwrap();
    drain_outbox(&db);

    // Blocked by skeleton
    assert_expand(&db, &root, 2, false, ItemState::Present);

    // Remove skeleton → recursive should re-expand
    db.unpin(&root, "skel_owner").unwrap();

    assert_expand(&db, &root, 2, true, ItemState::Present);

    // Outbox: add(child <- root)
    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, EdgeOp::Add);
    drain_outbox(&db);

    assert_expand(&db, &child, 1, true, ItemState::Present);
}

// ──────────── 11. Lease pin: protects self only ────────────

#[test]
fn test_lease_pin_no_expand() {
    let db = test_db();

    let child = file_id("lc01");
    let root = dir_id("lr01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    db.pin(&root, "lease_owner", PinScope::Lease, None).unwrap();

    // Root: class 2 (has pin), but NOT expanded (lease is not expand root)
    assert_expand(&db, &root, 2, false, ItemState::Present);
    assert!(peek_outbox(&db).is_empty());

    // Child: class 0
    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 12. Multi-parent shared DAG ────────────

#[test]
fn test_shared_dag_multi_parent() {
    let db = test_db();

    // D is shared: P1 -> D, P2 -> D
    let d = file_id("shared_d");
    let p1 = dir_id("parent1");
    let p2 = dir_id("parent2");

    db.set_object(&d, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&p1, "cydir", &make_dir_json(&[("d", &d)])).unwrap();
    db.set_object(&p2, "cydir", &make_dir_json(&[("d", &d)])).unwrap();

    // Pin both parents
    db.pin(&p1, "owner1", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);
    db.pin(&p2, "owner2", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    // D: class 1, 2 incoming refs
    let d_info = db.debug_dump_expand_state(&d).unwrap();
    assert_eq!(d_info.eviction_class, 1);
    assert_eq!(d_info.incoming_refs_count, 2);

    // Unpin P1: D should still be class 1 (P2 still holds)
    db.unpin(&p1, "owner1").unwrap();
    drain_outbox(&db);

    let d_info2 = db.debug_dump_expand_state(&d).unwrap();
    assert_eq!(d_info2.eviction_class, 1);
    assert_eq!(d_info2.incoming_refs_count, 1);

    // Unpin P2: D falls to class 0
    db.unpin(&p2, "owner2").unwrap();
    drain_outbox(&db);

    assert_expand(&db, &d, 0, false, ItemState::Present);
}

// ──────────── 13. Duplicate fs_acquire is idempotent ────────────

#[test]
fn test_fs_acquire_idempotent() {
    let db = test_db();

    let root = dir_id("idem01");
    db.set_object(&root, "cydir", "{}").unwrap();

    db.fs_acquire(&root, 1, 0).unwrap();
    db.fs_acquire(&root, 1, 0).unwrap(); // duplicate

    let d = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d.fs_anchor_count, 1); // not 2
    assert_eq!(d.eviction_class, 2);
}

// ──────────── 14. Multiple fs_anchors: count tracks correctly ────────────

#[test]
fn test_multiple_fs_anchors() {
    let db = test_db();

    let child = file_id("mfc01");
    let root = dir_id("mfr01");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Two different anchors
    db.fs_acquire(&root, 1, 0).unwrap();
    db.fs_acquire(&root, 1, 1).unwrap();

    let d = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d.fs_anchor_count, 2);
    assert_eq!(d.eviction_class, 2);
    assert!(d.children_expanded);

    // Release one: still anchored
    db.fs_release(&root, 1, 0).unwrap();

    let d2 = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d2.fs_anchor_count, 1);
    assert_eq!(d2.eviction_class, 2);
    assert!(d2.children_expanded);

    // No remove outbox yet (should_expand still true)
    // (there might be the original add still pending, so just check no remove)
    let outbox = peek_outbox(&db);
    let removes: Vec<_> = outbox.iter().filter(|e| e.0 == EdgeOp::Remove).collect();
    assert!(removes.is_empty());

    // Release last anchor
    db.fs_release(&root, 1, 1).unwrap();

    let d3 = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d3.fs_anchor_count, 0);
    assert_eq!(d3.eviction_class, 0);
    assert!(!d3.children_expanded);

    // Now there should be a remove in outbox
    let outbox2 = peek_outbox(&db);
    let removes2: Vec<_> = outbox2.iter().filter(|e| e.0 == EdgeOp::Remove).collect();
    assert!(!removes2.is_empty());
}

// ──────────── 15. GC delete with single bucket loopback ────────────
//
// End-to-end: put objects → pin → drain outbox → unpin → drain outbox → GC deletes all

#[test]
fn test_gc_single_bucket_end_to_end() {
    let db = test_db();

    // 3-level tree: root -> mid -> leaf
    let leaf = dir_id("leaf01");
    let mid = dir_id("mid01");
    let root = dir_id("root_e2e");

    db.set_object(&leaf, "cydir", "{}").unwrap();
    db.set_object(&mid, "cydir", &make_dir_json(&[("l", &leaf)])).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("m", &mid)])).unwrap();

    // Pin recursive
    db.pin(&root, "user", PinScope::Recursive, None).unwrap();

    // Drain outbox iteratively (multi-level cascade)
    drain_outbox(&db);

    // Verify: root=class2, mid=class1, leaf=class1
    assert_expand(&db, &root, 2, true, ItemState::Present);
    assert_expand(&db, &mid, 1, true, ItemState::Present);
    assert_expand(&db, &leaf, 1, true, ItemState::Present); // children_expanded=true (expansion attempted, no parseable children)

    // None should be GC-able
    assert_eq!(db.try_evict_object(&root.to_string()).unwrap(), 0);
    assert_eq!(db.try_evict_object(&mid.to_string()).unwrap(), 0);
    assert_eq!(db.try_evict_object(&leaf.to_string()).unwrap(), 0);

    // Unpin
    db.unpin(&root, "user").unwrap();

    // Drain remove cascade
    drain_outbox(&db);

    // All class 0 now
    assert_expand(&db, &root, 0, false, ItemState::Present);
    assert_expand(&db, &mid, 0, false, ItemState::Present);
    assert_expand(&db, &leaf, 0, false, ItemState::Present);

    // GC all
    let f1 = db.try_evict_object(&leaf.to_string()).unwrap();
    let f2 = db.try_evict_object(&mid.to_string()).unwrap();
    let f3 = db.try_evict_object(&root.to_string()).unwrap();
    assert!(f1 > 0);
    assert!(f2 > 0);
    assert!(f3 > 0);

    assert!(!db.has_object_row(&root.to_string()).unwrap());
    assert!(!db.has_object_row(&mid.to_string()).unwrap());
    assert!(!db.has_object_row(&leaf.to_string()).unwrap());
}

// ──────────── 16. Outbox correctness: multi-child ────────────

#[test]
fn test_outbox_multi_child() {
    let db = test_db();

    let c1 = file_id("mc1");
    let c2 = file_id("mc2");
    let c3 = file_id("mc3");
    let root = dir_id("mcroot");

    db.set_object(&c1, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&c2, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&c3, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[
        ("a", &c1), ("b", &c2), ("c", &c3),
    ])).unwrap();

    db.pin(&root, "u", PinScope::Recursive, None).unwrap();

    let outbox = peek_outbox(&db);
    assert_eq!(outbox.len(), 3);

    let referees: HashSet<String> = outbox.iter().map(|e| e.1.clone()).collect();
    assert!(referees.contains(&c1.to_string()));
    assert!(referees.contains(&c2.to_string()));
    assert!(referees.contains(&c3.to_string()));

    // All should have referrer = root
    for entry in &outbox {
        assert_eq!(entry.0, EdgeOp::Add);
        assert_eq!(entry.2, root.to_string());
    }
}

// ──────────── 17. apply_edge add/remove idempotency ────────────

#[test]
fn test_apply_edge_idempotent() {
    let db = test_db();

    let child = dir_id("ae_child");
    db.set_object(&child, "cydir", "{}").unwrap();

    let parent = dir_id("ae_parent");

    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: child.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };

    // Apply add twice
    db.apply_edge(&add_msg).unwrap();
    db.apply_edge(&add_msg).unwrap();

    let d = db.debug_dump_expand_state(&child).unwrap();
    assert_eq!(d.incoming_refs_count, 1); // not 2
    assert_eq!(d.eviction_class, 1);

    // Apply remove twice
    let rm_msg = EdgeMsg {
        op: EdgeOp::Remove,
        referee: child.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&rm_msg).unwrap();
    db.apply_edge(&rm_msg).unwrap();

    let d2 = db.debug_dump_expand_state(&child).unwrap();
    assert_eq!(d2.incoming_refs_count, 0);
    assert_eq!(d2.eviction_class, 0);
}

// ──────────── 18. fs_anchor on shadow → Pending, then present → Materializing ────────────

#[test]
fn test_fs_anchor_shadow_then_present() {
    let db = test_db();

    let root = dir_id("fss01");

    // fs_acquire before content → shadow + Pending
    db.fs_acquire(&root, 10, 0).unwrap();

    assert_expand(&db, &root, 2, false, ItemState::Shadow);
    let cs = db.fs_anchor_state(&root, 10, 0).unwrap();
    assert_eq!(cs, CascadeStateP0::Pending);

    // Put content
    db.put_object_gc_aware(&root, "cydir", "{}").unwrap();

    assert_expand(&db, &root, 2, true, ItemState::Present);
    let cs2 = db.fs_anchor_state(&root, 10, 0).unwrap();
    assert_eq!(cs2, CascadeStateP0::Materializing);
}

// ──────────── 19. GC refuses to evict class 1/2 even if listed ────────────

#[test]
fn test_gc_double_check_protects() {
    let db = test_db();

    let obj = dir_id("dc01");
    db.set_object(&obj, "cydir", "{}").unwrap();

    // Starts as class 0
    assert_expand(&db, &obj, 0, false, ItemState::Present);

    // list candidates
    let cands = db.list_gc_candidates(100).unwrap();
    assert!(!cands.is_empty());

    // Now pin it (simulating race: candidate listed but protection added before evict)
    db.pin(&obj, "o", PinScope::Lease, None).unwrap();
    assert_expand(&db, &obj, 2, false, ItemState::Present);

    // try_evict should return 0 (double-check catches it)
    let freed = db.try_evict_object(&obj.to_string()).unwrap();
    assert_eq!(freed, 0);
    assert!(db.has_object_row(&obj.to_string()).unwrap());
}

// ───────────────────── additional helpers ─────────────────────

/// Assert outbox contains exactly the expected entries (unordered).
fn assert_outbox_exact(
    db: &NamedLocalStoreDB,
    expected: &[(EdgeOp, &ObjId, &ObjId)], // (op, referee, referrer)
) {
    let outbox = peek_outbox(db);
    assert_eq!(
        outbox.len(),
        expected.len(),
        "outbox length mismatch: got {:?}",
        outbox
    );
    let mut expected_set: HashSet<(String, String, String)> = HashSet::new();
    for &(ref op, referee, referrer) in expected {
        expected_set.insert((
            format!("{:?}", op),
            ObjId::to_string(referee),
            ObjId::to_string(referrer),
        ));
    }
    let mut actual_set: HashSet<(String, String, String)> = HashSet::new();
    for (op, referee, referrer) in &outbox {
        actual_set.insert((format!("{:?}", op), referee.clone(), referrer.clone()));
    }
    assert_eq!(actual_set, expected_set, "outbox content mismatch");
}

/// Drain outbox and assert it is empty afterwards.
fn drain_and_assert_empty(db: &NamedLocalStoreDB) {
    drain_outbox(db);
    assert!(
        peek_outbox(db).is_empty(),
        "outbox not empty after drain"
    );
}

// ═══════════════════════════════════════════════════
// High-priority tests (20–25)
// ═══════════════════════════════════════════════════

// ──────────── 20. Shared DAG: remove does not retract downstream ────────────
// P1 -> D -> X,  P2 -> D
// Unpin P1: D still expanded via P2's incoming, no remove(X <- D).

#[test]
fn test_shared_dag_remove_does_not_retract_downstream() {
    let db = test_db();

    let x = file_id("dag_x");
    let d = dir_id("dag_d");
    let p1 = dir_id("dag_p1");
    let p2 = dir_id("dag_p2");

    db.set_object(&x, "cyfile", &make_file_json(&dir_id("dummy"))).unwrap();
    db.set_object(&d, "cydir", &make_dir_json(&[("x", &x)])).unwrap();
    db.set_object(&p1, "cydir", &make_dir_json(&[("d", &d)])).unwrap();
    db.set_object(&p2, "cydir", &make_dir_json(&[("d", &d)])).unwrap();

    // Pin both parents recursive
    db.pin(&p1, "o1", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);
    db.pin(&p2, "o2", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    // D: class 1, expanded, 2 incoming refs; X: class 1
    assert_expand(&db, &d, 1, true, ItemState::Present);
    let d_info = db.debug_dump_expand_state(&d).unwrap();
    assert_eq!(d_info.incoming_refs_count, 2);
    assert_expand(&db, &x, 1, true, ItemState::Present);

    // Unpin P1
    db.unpin(&p1, "o1").unwrap();

    // P1 teardown emits remove(D <- P1), drain it
    drain_outbox(&db);

    // D still has 1 incoming from P2 → class 1, children_expanded=true
    assert_expand(&db, &d, 1, true, ItemState::Present);

    // X should NOT have received a remove — still class 1
    assert_expand(&db, &x, 1, true, ItemState::Present);

    // Outbox must be empty — no spurious remove(X <- D)
    assert!(peek_outbox(&db).is_empty());
}

// ──────────── 21. Incoming shadow then present auto-expands ────────────

#[test]
fn test_incoming_shadow_then_present_auto_expands() {
    let db = test_db();

    let x = file_id("is_x");
    let s = dir_id("is_s");
    let d = dir_id("is_d");

    // D exists, S does not yet
    db.set_object(&d, "cydir", "{}").unwrap();

    // apply_edge(add, S <- D): S becomes shadow with class 1
    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: s.clone(),
        referrer: d.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&add_msg).unwrap();

    assert_expand(&db, &s, 1, false, ItemState::Shadow);

    // No outbox (shadow can't expand)
    assert!(peek_outbox(&db).is_empty());

    // Now put S with children
    db.set_object(&x, "cyfile", &make_file_json(&dir_id("dummy"))).unwrap();
    let s_json = make_dir_json(&[("x", &x)]);
    db.put_object_gc_aware(&s, "cydir", &s_json).unwrap();

    // S: class 1, present, expanded
    assert_expand(&db, &s, 1, true, ItemState::Present);

    // Outbox: add(X <- S)
    assert_outbox_exact(&db, &[(EdgeOp::Add, &x, &s)]);

    drain_outbox(&db);
    assert_expand(&db, &x, 1, true, ItemState::Present);
}

// ──────────── 22. fs_anchor transition outbox exactly once ────────────

#[test]
fn test_fs_anchor_transition_outbox_exactly_once() {
    let db = test_db();

    let child = file_id("fat_c");
    let root = dir_id("fat_r");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // 0 -> 1: should emit add(child <- root)
    db.fs_acquire(&root, 1, 0).unwrap();
    assert_outbox_exact(&db, &[(EdgeOp::Add, &child, &root)]);
    drain_and_assert_empty(&db);

    assert_expand(&db, &child, 1, true, ItemState::Present);

    // 1 -> 2: no new add
    db.fs_acquire(&root, 1, 1).unwrap();
    assert!(peek_outbox(&db).is_empty());
    let d = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d.fs_anchor_count, 2);

    // 2 -> 1: no remove
    db.fs_release(&root, 1, 1).unwrap();
    assert!(peek_outbox(&db).is_empty());
    let d2 = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(d2.fs_anchor_count, 1);
    assert!(d2.children_expanded);

    // 1 -> 0: should emit remove(child <- root)
    db.fs_release(&root, 1, 0).unwrap();
    assert_outbox_exact(&db, &[(EdgeOp::Remove, &child, &root)]);
    drain_and_assert_empty(&db);

    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 23. Recursive pin transition outbox exactly once ────────────

#[test]
fn test_recursive_pin_transition_outbox_exactly_once() {
    let db = test_db();

    let child = file_id("rpt_c");
    let root = dir_id("rpt_r");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // First owner pin: add(child <- root)
    db.pin(&root, "owner1", PinScope::Recursive, None).unwrap();
    assert_outbox_exact(&db, &[(EdgeOp::Add, &child, &root)]);
    drain_and_assert_empty(&db);

    assert_expand(&db, &child, 1, true, ItemState::Present);

    // Second owner pin: no duplicate add
    db.pin(&root, "owner2", PinScope::Recursive, None).unwrap();
    assert!(peek_outbox(&db).is_empty());

    // Remove first owner: no remove yet (second still holds)
    db.unpin(&root, "owner1").unwrap();
    assert!(peek_outbox(&db).is_empty());
    assert_expand(&db, &root, 2, true, ItemState::Present);

    // Remove last owner: remove(child <- root)
    db.unpin(&root, "owner2").unwrap();
    assert_outbox_exact(&db, &[(EdgeOp::Remove, &child, &root)]);
    drain_and_assert_empty(&db);

    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 24. Skeleton on shared node blocks without wrong teardown ────────────

#[test]
fn test_skeleton_on_shared_node_blocks_without_wrong_teardown() {
    let db = test_db();

    // P -> D -> X; D has incoming from P
    let x = file_id("sks_x");
    let d = dir_id("sks_d");
    let p = dir_id("sks_p");

    db.set_object(&x, "cyfile", &make_file_json(&dir_id("dummy"))).unwrap();
    db.set_object(&d, "cydir", &make_dir_json(&[("x", &x)])).unwrap();
    db.set_object(&p, "cydir", &make_dir_json(&[("d", &d)])).unwrap();

    // Pin P recursive → D gets incoming, D expands to X
    db.pin(&p, "owner", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &d, 1, true, ItemState::Present);
    assert_expand(&db, &x, 1, true, ItemState::Present);

    // Add Skeleton pin on D → should tear down D -> X
    db.pin(&d, "skel", PinScope::Skeleton, None).unwrap();

    // D: class 2 (has pin), NOT expanded (skeleton blocks)
    assert_expand(&db, &d, 2, false, ItemState::Present);

    // Outbox: remove(X <- D)
    assert_outbox_exact(&db, &[(EdgeOp::Remove, &x, &d)]);
    drain_and_assert_empty(&db);

    // X: class 0 (lost incoming)
    assert_expand(&db, &x, 0, false, ItemState::Present);

    // Remove Skeleton → should auto re-expand since D has incoming_ref
    db.unpin(&d, "skel").unwrap();

    // D: class 1 (incoming from P), children_expanded=true
    assert_expand(&db, &d, 1, true, ItemState::Present);

    // Outbox: add(X <- D)
    assert_outbox_exact(&db, &[(EdgeOp::Add, &x, &d)]);
    drain_and_assert_empty(&db);

    assert_expand(&db, &x, 1, true, ItemState::Present);
}

// ═══════════════════════════════════════════════════
// Medium-priority tests (25–31)
// ═══════════════════════════════════════════════════

// ──────────── 25. GC ignores zero owned_bytes rows ────────────

#[test]
fn test_gc_ignores_zero_owned_bytes_rows() {
    let db = test_db();

    // Shadow has owned_bytes=0
    let shadow_obj = dir_id("zb_shadow");
    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: shadow_obj.clone(),
        referrer: dir_id("zb_parent"),
        target_epoch: 1,
    };
    db.apply_edge(&add_msg).unwrap();
    // Remove incoming so it's class 0 but still shadow
    let rm_msg = EdgeMsg {
        op: EdgeOp::Remove,
        referee: shadow_obj.clone(),
        referrer: dir_id("zb_parent"),
        target_epoch: 1,
    };
    db.apply_edge(&rm_msg).unwrap();
    drain_outbox(&db);

    // Shadow with class 0 should NOT appear in candidates
    let candidates = db.list_gc_candidates(100).unwrap();
    let ids: Vec<&str> = candidates.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        !ids.contains(&shadow_obj.to_string().as_str()),
        "shadow with owned_bytes=0 should not be a GC candidate"
    );

    // Also verify with a present object that has real bytes
    let real_obj = dir_id("zb_real");
    db.set_object(&real_obj, "cydir", "{\"data\": true}").unwrap();
    let candidates2 = db.list_gc_candidates(100).unwrap();
    let ids2: Vec<&str> = candidates2.iter().map(|(id, _)| id.as_str()).collect();
    assert!(ids2.contains(&real_obj.to_string().as_str()));
}

// ──────────── 26. Expire pins reconciles expand state ────────────

#[test]
fn test_expire_pins_reconciles_expand_state() {
    let db = test_db();

    let child = file_id("ep_c");
    let root = dir_id("ep_r");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Pin with 1-second TTL
    db.pin(&root, "expiring", PinScope::Recursive, Some(std::time::Duration::from_secs(1))).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &root, 2, true, ItemState::Present);
    assert_expand(&db, &child, 1, true, ItemState::Present);

    // Wait for the TTL to expire
    std::thread::sleep(std::time::Duration::from_millis(1500));

    // Expire pins
    let expired = db.expire_pins().unwrap();
    assert!(expired > 0, "should have expired at least one pin");

    // Root: class 0, not expanded
    assert_expand(&db, &root, 0, false, ItemState::Present);

    // Outbox: remove(child <- root)
    let outbox = peek_outbox(&db);
    let removes: Vec<_> = outbox.iter().filter(|e| e.0 == EdgeOp::Remove).collect();
    assert!(!removes.is_empty());

    drain_outbox(&db);
    assert_expand(&db, &child, 0, false, ItemState::Present);
}

// ──────────── 27. unpin_owner reconciles all affected objects ────────────

#[test]
fn test_unpin_owner_reconciles_all_affected_objects() {
    let db = test_db();

    let c1 = file_id("uo_c1");
    let c2 = file_id("uo_c2");
    let r1 = dir_id("uo_r1");
    let r2 = dir_id("uo_r2");

    db.set_object(&c1, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&c2, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&r1, "cydir", &make_dir_json(&[("c", &c1)])).unwrap();
    db.set_object(&r2, "cydir", &make_dir_json(&[("c", &c2)])).unwrap();

    // Same owner pins both roots
    db.pin(&r1, "shared_owner", PinScope::Recursive, None).unwrap();
    db.pin(&r2, "shared_owner", PinScope::Recursive, None).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &r1, 2, true, ItemState::Present);
    assert_expand(&db, &r2, 2, true, ItemState::Present);
    assert_expand(&db, &c1, 1, true, ItemState::Present);
    assert_expand(&db, &c2, 1, true, ItemState::Present);

    // unpin_owner
    let affected = db.unpin_owner("shared_owner").unwrap();
    assert_eq!(affected, 2);

    // Both roots: class 0, not expanded
    assert_expand(&db, &r1, 0, false, ItemState::Present);
    assert_expand(&db, &r2, 0, false, ItemState::Present);

    // Outbox should have remove entries for both children
    let outbox = peek_outbox(&db);
    let remove_referees: HashSet<String> = outbox
        .iter()
        .filter(|e| e.0 == EdgeOp::Remove)
        .map(|e| e.1.clone())
        .collect();
    assert!(remove_referees.contains(&c1.to_string()));
    assert!(remove_referees.contains(&c2.to_string()));

    drain_outbox(&db);
    assert_expand(&db, &c1, 0, false, ItemState::Present);
    assert_expand(&db, &c2, 0, false, ItemState::Present);
}

// ──────────── 28. fs_release_inode reconciles all affected objects ────────────

#[test]
fn test_fs_release_inode_reconciles_all_affected_objects() {
    let db = test_db();

    let c1 = file_id("fri_c1");
    let c2 = file_id("fri_c2");
    let r1 = dir_id("fri_r1");
    let r2 = dir_id("fri_r2");

    db.set_object(&c1, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&c2, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&r1, "cydir", &make_dir_json(&[("c", &c1)])).unwrap();
    db.set_object(&r2, "cydir", &make_dir_json(&[("c", &c2)])).unwrap();

    // Same inode anchors both objects with different field_tags
    let inode = 42u64;
    db.fs_acquire(&r1, inode, 0).unwrap();
    db.fs_acquire(&r2, inode, 1).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &r1, 2, true, ItemState::Present);
    assert_expand(&db, &r2, 2, true, ItemState::Present);
    assert_expand(&db, &c1, 1, true, ItemState::Present);
    assert_expand(&db, &c2, 1, true, ItemState::Present);

    // Release all anchors for inode
    let affected = db.fs_release_inode(inode).unwrap();
    assert_eq!(affected, 2);

    // Both roots: class 0, fs_anchor_count=0
    let d1 = db.debug_dump_expand_state(&r1).unwrap();
    assert_eq!(d1.fs_anchor_count, 0);
    assert_eq!(d1.eviction_class, 0);
    assert!(!d1.children_expanded);

    let d2 = db.debug_dump_expand_state(&r2).unwrap();
    assert_eq!(d2.fs_anchor_count, 0);
    assert_eq!(d2.eviction_class, 0);
    assert!(!d2.children_expanded);

    // Outbox: remove entries for children
    let outbox = peek_outbox(&db);
    let remove_referees: HashSet<String> = outbox
        .iter()
        .filter(|e| e.0 == EdgeOp::Remove)
        .map(|e| e.1.clone())
        .collect();
    assert!(remove_referees.contains(&c1.to_string()));
    assert!(remove_referees.contains(&c2.to_string()));

    drain_outbox(&db);
    assert_expand(&db, &c1, 0, false, ItemState::Present);
    assert_expand(&db, &c2, 0, false, ItemState::Present);
}

// ──────────── 29. apply_edge remove before add is stable ────────────

#[test]
fn test_apply_edge_remove_before_add_is_stable() {
    let db = test_db();

    let child = dir_id("rba_child");
    db.set_object(&child, "cydir", "{}").unwrap();

    let parent = dir_id("rba_parent");

    // Remove first (no-op, nothing to remove)
    let rm_msg = EdgeMsg {
        op: EdgeOp::Remove,
        referee: child.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&rm_msg).unwrap(); // should not panic

    assert_expand(&db, &child, 0, false, ItemState::Present);

    // Then add
    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: child.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&add_msg).unwrap();

    // Final state: has incoming ref
    let d = db.debug_dump_expand_state(&child).unwrap();
    assert_eq!(d.incoming_refs_count, 1);
    assert_eq!(d.eviction_class, 1);
}

// ──────────── 30. apply_edge add on shadow with skeleton stays blocked ────────────

#[test]
fn test_apply_edge_add_on_shadow_with_skeleton_stays_blocked() {
    let db = test_db();

    let x = file_id("aes_x");
    let d = dir_id("aes_d");
    let upstream = dir_id("aes_up");

    // D exists with child X, add skeleton on D
    db.set_object(&x, "cyfile", &make_file_json(&dir_id("dummy"))).unwrap();
    db.set_object(&d, "cydir", &make_dir_json(&[("x", &x)])).unwrap();
    db.pin(&d, "skel", PinScope::Skeleton, None).unwrap();

    assert_expand(&db, &d, 2, false, ItemState::Present);
    assert!(peek_outbox(&db).is_empty());

    // Receive incoming edge: D gets class 2 (max of pin=2, incoming=1), still skeleton-blocked
    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: d.clone(),
        referrer: upstream.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&add_msg).unwrap();
    drain_outbox(&db);

    // D: class 2, NOT expanded (skeleton blocks)
    assert_expand(&db, &d, 2, false, ItemState::Present);

    // X: class 0 (no incoming from D)
    assert_expand(&db, &x, 0, false, ItemState::Present);
}

// ═══════════════════════════════════════════════════
// Low-priority tests (31–34)
// ═══════════════════════════════════════════════════

// ──────────── 31. cascade_state for non-recursive pins ────────────

#[test]
fn test_cascade_state_for_non_recursive_pins() {
    let db = test_db();

    let root = dir_id("cs_nr");
    db.set_object(&root, "cydir", "{}").unwrap();

    // Skeleton pin → cascade_state = Materializing (present object)
    db.pin(&root, "skel_owner", PinScope::Skeleton, None).unwrap();
    let cs = db.anchor_state(&root, "skel_owner").unwrap();
    assert_eq!(cs, CascadeStateP0::Materializing);

    // Lease pin → cascade_state = Materializing
    db.pin(&root, "lease_owner", PinScope::Lease, None).unwrap();
    let cs2 = db.anchor_state(&root, "lease_owner").unwrap();
    assert_eq!(cs2, CascadeStateP0::Materializing);

    // Shadow object with skeleton → still Materializing (non-recursive never Pending)
    let shadow = dir_id("cs_nr_shadow");
    db.pin(&shadow, "skel2", PinScope::Skeleton, None).unwrap();
    let cs3 = db.anchor_state(&shadow, "skel2").unwrap();
    assert_eq!(cs3, CascadeStateP0::Materializing);

    db.pin(&shadow, "lease2", PinScope::Lease, None).unwrap();
    let cs4 = db.anchor_state(&shadow, "lease2").unwrap();
    assert_eq!(cs4, CascadeStateP0::Materializing);
}

// ──────────── 32. Duplicate apply_edge does not duplicate outbox side effects ────────────

#[test]
fn test_duplicate_apply_edge_no_duplicate_outbox() {
    let db = test_db();

    let x = file_id("dae_x");
    let d = dir_id("dae_d");
    let parent = dir_id("dae_parent");

    db.set_object(&x, "cyfile", &make_file_json(&dir_id("dummy"))).unwrap();
    db.set_object(&d, "cydir", &make_dir_json(&[("x", &x)])).unwrap();

    let add_msg = EdgeMsg {
        op: EdgeOp::Add,
        referee: d.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };

    // First add → should expand D, emit add(X <- D)
    db.apply_edge(&add_msg).unwrap();
    let outbox1 = peek_outbox(&db);
    let add_count_1 = outbox1.iter().filter(|e| e.0 == EdgeOp::Add).count();
    assert_eq!(add_count_1, 1, "first apply_edge should emit exactly one add");
    drain_and_assert_empty(&db);

    // Duplicate add → should NOT emit another add(X <- D) since already expanded
    db.apply_edge(&add_msg).unwrap();
    assert!(
        peek_outbox(&db).is_empty(),
        "duplicate apply_edge(add) should not produce outbox entries"
    );

    // Remove
    let rm_msg = EdgeMsg {
        op: EdgeOp::Remove,
        referee: d.clone(),
        referrer: parent.clone(),
        target_epoch: 1,
    };
    db.apply_edge(&rm_msg).unwrap();
    let outbox2 = peek_outbox(&db);
    let rm_count = outbox2.iter().filter(|e| e.0 == EdgeOp::Remove).count();
    assert_eq!(rm_count, 1, "first apply_edge(remove) should emit exactly one remove");
    drain_and_assert_empty(&db);

    // Duplicate remove → no outbox
    db.apply_edge(&rm_msg).unwrap();
    assert!(
        peek_outbox(&db).is_empty(),
        "duplicate apply_edge(remove) should not produce outbox entries"
    );
}

// ──────────── 33. fs_release_inode only removes for last expand reason ────────────

#[test]
fn test_fs_release_inode_only_removes_last_expand_reason() {
    let db = test_db();

    let child = file_id("frl_c");
    let root = dir_id("frl_r");

    db.set_object(&child, "cyfile", &make_file_json(&dir_id("x"))).unwrap();
    db.set_object(&root, "cydir", &make_dir_json(&[("c", &child)])).unwrap();

    // Root has both recursive pin AND fs_anchor
    db.pin(&root, "owner", PinScope::Recursive, None).unwrap();
    db.fs_acquire(&root, 99, 0).unwrap();
    drain_outbox(&db);

    assert_expand(&db, &root, 2, true, ItemState::Present);
    assert_expand(&db, &child, 1, true, ItemState::Present);

    // Release fs inode → root still expanded (recursive pin holds)
    db.fs_release_inode(99).unwrap();
    assert_expand(&db, &root, 2, true, ItemState::Present);

    // No remove for child
    assert!(peek_outbox(&db).is_empty());
    assert_expand(&db, &child, 1, true, ItemState::Present);
}
