//! Issue #41: independent connections/processes must serialize read-before-write
//! transactions without losing idempotent writes or GC protection.

use named_store::{ChunkItem, ChunkStoreState, ItemState, NamedLocalStoreDB, PinScope};
use ndn_lib::{ChunkHasher, ChunkId, DirObject, HashMethod, NdnError, ObjId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const WRITERS: usize = 4;
const WRITES_PER_CONNECTION: usize = 1000;
const BODY: &str = r#"{"body":{},"create_time":0,"file_count":0,"file_size":0,"last_update_time":0,"total_size":0}"#;

fn shared_id() -> ObjId {
    ObjId::new("cydir:68fa31e4de063f4824b58022cbd15223edc71f59a4a7109ac3f94f5688e47ff9").unwrap()
}

fn open(path: &Path) -> NamedLocalStoreDB {
    NamedLocalStoreDB::new(path.to_str().unwrap().to_owned()).unwrap()
}

fn assert_contents(db: &NamedLocalStoreDB, id: &ObjId, body: &str) {
    assert_eq!(db.get_object(id).unwrap(), ("cydir".into(), body.into()));
    let state = db.debug_dump_expand_state(id).unwrap();
    assert_eq!(state.state, ItemState::Present);
    assert_eq!(state.logical_size, body.len() as u64);
    assert_eq!(state.owned_bytes, body.len() as u64);
}

fn write_idempotently(db: NamedLocalStoreDB) -> (usize, BTreeMap<String, usize>) {
    let id = shared_id();
    let mut passed = 0;
    let mut errors = BTreeMap::new();
    for _ in 0..WRITES_PER_CONNECTION {
        match db.put_object_gc_aware(&id, "cydir", BODY) {
            Ok(()) => passed += 1,
            Err(error) => *errors.entry(format!("{error:?}")).or_default() += 1,
        }
    }
    (passed, errors)
}

#[test]
fn independent_connections_complete_all_idempotent_puts() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("store.db");
    // Initialize serially, just like the report: exercise write contention,
    // independently of concurrent schema initialization.
    let stores: Vec<_> = (0..WRITERS).map(|_| open(&path)).collect();
    stores[0]
        .put_object_gc_aware(&shared_id(), "cydir", BODY)
        .unwrap();
    let gate = Arc::new(Barrier::new(WRITERS));
    let workers: Vec<_> = stores
        .into_iter()
        .map(|db| {
            let gate = gate.clone();
            thread::spawn(move || {
                gate.wait();
                write_idempotently(db)
            })
        })
        .collect();
    let mut passed = 0;
    let mut errors = BTreeMap::new();
    for worker in workers {
        let (successes, failures) = worker.join().unwrap();
        passed += successes;
        for (error, count) in failures {
            *errors.entry(error).or_insert(0) += count;
        }
    }
    assert_eq!(passed, WRITERS * WRITES_PER_CONNECTION, "{errors:?}");
    assert!(errors.is_empty());
    assert_contents(&open(&path), &shared_id(), BODY);
}

#[test]
fn concurrent_distinct_objects_and_conflicting_content_preserve_originals() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("store.db");
    let stores: Vec<_> = (0..WRITERS).map(|_| open(&path)).collect();
    stores[0].set_object(&shared_id(), "cydir", BODY).unwrap();
    let gate = Arc::new(Barrier::new(WRITERS));
    let workers: Vec<_> = stores
        .into_iter()
        .enumerate()
        .map(|(worker_id, db)| {
            let gate = gate.clone();
            thread::spawn(move || {
                let mut written = Vec::new();
                gate.wait();
                for sequence in 0..100 {
                    let (id, body) = DirObject::new(Some(format!("{worker_id}-{sequence}")))
                        .gen_obj_id()
                        .unwrap();
                    db.set_object(&id, "cydir", &body).unwrap();
                    db.put_object_gc_aware(&id, "cydir", &body).unwrap();
                    db.set_object(&shared_id(), "cydir", BODY).unwrap();
                    let conflict = if sequence % 2 == 0 {
                        db.set_object(&shared_id(), "cydir", "{}")
                    } else {
                        db.put_object_gc_aware(&shared_id(), "cydir", "{}")
                    };
                    assert!(
                        matches!(conflict, Err(NdnError::AlreadyExists(_))),
                        "{conflict:?}"
                    );
                    written.push((id, body));
                }
                written
            })
        })
        .collect();
    let written: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    let db = open(&path);
    assert_contents(&db, &shared_id(), BODY);
    assert_eq!(written.len(), WRITERS * 100);
    for (id, body) in written {
        assert_contents(&db, &id, &body);
    }
}

// Kill and reap children even if a worker exits early or an assertion fails.
struct Children(Vec<Child>);

impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn independent_processes_complete_all_idempotent_puts() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("store.db");
    open(&path)
        .put_object_gc_aware(&shared_id(), "cydir", BODY)
        .unwrap();
    let mut children = Children(Vec::new());
    for worker in 0..WRITERS {
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process_writer", "--ignored", "--nocapture"])
                .env("NAMED_STORE_CONCURRENCY_SCRATCH", scratch.path())
                .env("NAMED_STORE_CONCURRENCY_WORKER", worker.to_string())
                .spawn()
                .unwrap(),
        );
        // Finish each connection's initialization before spawning the next.
        wait_for(&scratch.path().join(format!("ready-{worker}")));
    }
    std::fs::write(scratch.path().join("go"), b"").unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    for child in &mut children.0 {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "writer process failed: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "writer process did not finish");
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert_contents(&open(&path), &shared_id(), BODY);
}

#[test]
#[ignore = "worker invoked by independent_processes_complete_all_idempotent_puts"]
fn process_writer() {
    let Some(scratch) = std::env::var_os("NAMED_STORE_CONCURRENCY_SCRATCH") else {
        return;
    };
    let scratch = PathBuf::from(scratch);
    let worker = std::env::var("NAMED_STORE_CONCURRENCY_WORKER").unwrap();
    let db = open(&scratch.join("store.db"));
    std::fs::write(scratch.join(format!("ready-{worker}")), b"").unwrap();
    wait_for(&scratch.join("go"));
    let (passed, errors) = write_idempotently(db);
    assert_eq!(passed, WRITES_PER_CONNECTION, "worker={worker} {errors:?}");
    assert!(errors.is_empty());
}

fn drain_edges(db: &NamedLocalStoreDB) {
    loop {
        let batch = db.fetch_outbox_ready(100).unwrap();
        if batch.is_empty() {
            break;
        }
        for entry in batch {
            db.apply_edge(&entry.msg).unwrap();
            db.complete_outbox_entry(entry.seq).unwrap();
        }
    }
}

fn completed_chunk(body: &[u8]) -> ChunkItem {
    let hash = ChunkHasher::new(None).unwrap().calc_from_bytes(body);
    let id =
        ChunkId::from_mix_hash_result_by_hash_method(body.len() as u64, &hash, HashMethod::Sha256)
            .unwrap();
    ChunkItem::new_completed(&id, body.len() as u64)
}

#[test]
fn concurrent_writes_and_maintenance_preserve_pins_references_and_gc() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("store.db");
    let stores: Vec<_> = (0..WRITERS).map(|_| open(&path)).collect();
    let (child, child_body) = DirObject::new(Some("child".into())).gen_obj_id().unwrap();
    let mut root_dir = DirObject::new(Some("root".into()));
    root_dir
        .add_directory("child".into(), child.clone(), 0)
        .unwrap();
    let (root, root_body) = root_dir.gen_obj_id().unwrap();
    stores[0]
        .put_object_gc_aware(&root, "cydir", &root_body)
        .unwrap();
    stores[0]
        .put_object_gc_aware(&child, "cydir", &child_body)
        .unwrap();
    stores[0]
        .pin(&root, "permanent", PinScope::Recursive, None)
        .unwrap();
    let pinned_chunk = completed_chunk(b"pinned chunk bytes");
    stores[0].set_chunk_item(&pinned_chunk).unwrap();
    stores[0]
        .pin(
            &pinned_chunk.chunk_id.to_obj_id(),
            "permanent-chunk",
            PinScope::Lease,
            None,
        )
        .unwrap();
    drain_edges(&stores[0]);
    let gate = Arc::new(Barrier::new(WRITERS));
    let workers: Vec<_> = stores
        .into_iter()
        .enumerate()
        .map(|(worker, db)| {
            let gate = gate.clone();
            let root = root.clone();
            let root_body = root_body.clone();
            let child = child.clone();
            let child_body = child_body.clone();
            let pinned_chunk = pinned_chunk.clone();
            thread::spawn(move || {
                gate.wait();
                for step in 0..100 {
                    match worker {
                        0 => {
                            db.put_object_gc_aware(&root, "cydir", &root_body).unwrap();
                            db.put_object_gc_aware(&child, "cydir", &child_body)
                                .unwrap();
                            db.set_chunk_item(&pinned_chunk).unwrap();
                        }
                        1 => {
                            db.pin(&root, "transient", PinScope::Recursive, None)
                                .unwrap();
                            assert_eq!(db.unpin_owner("transient").unwrap(), 1);
                            db.fs_acquire(&root, 42, 0).unwrap();
                            assert_eq!(db.fs_release_inode(42).unwrap(), 1);
                        }
                        2 => {
                            db.pin(&root, "expired", PinScope::Lease, Some(Duration::ZERO))
                                .unwrap();
                            db.expire_pins().unwrap();
                            db.batch_touch_last_access_with_max(&[(root.to_string(), step)])
                                .unwrap();
                        }
                        3 => {
                            assert_eq!(db.try_evict_object(&root.to_string()).unwrap(), 0);
                            assert_eq!(db.try_evict_object(&child.to_string()).unwrap(), 0);
                            assert_eq!(
                                db.try_evict_chunk(&pinned_chunk.chunk_id.to_string())
                                    .unwrap(),
                                0
                            );
                            let chunk = completed_chunk(format!("garbage chunk {step}").as_bytes());
                            db.set_chunk_item(&chunk).unwrap();
                            let stored = db.get_chunk_item(&chunk.chunk_id).unwrap();
                            assert_eq!(stored.chunk_size, chunk.chunk_size);
                            assert_eq!(stored.chunk_state, ChunkStoreState::Completed);
                            assert_eq!(
                                db.try_evict_chunk(&chunk.chunk_id.to_string()).unwrap(),
                                chunk.chunk_size
                            );
                            assert!(db.get_chunk_item(&chunk.chunk_id).is_err());
                            let (garbage, body) = DirObject::new(Some(format!("garbage-{step}")))
                                .gen_obj_id()
                                .unwrap();
                            db.set_object(&garbage, "cydir", &body).unwrap();
                            let candidates = db.list_gc_candidates(100).unwrap();
                            assert!(candidates.iter().any(|(id, _)| id == &garbage.to_string()));
                            assert!(
                                !candidates
                                    .iter()
                                    .any(|(id, _)| id == &root.to_string()
                                        || id == &child.to_string())
                            );
                            assert_eq!(
                                db.try_evict_object(&garbage.to_string()).unwrap(),
                                body.len() as u64
                            );
                            assert!(!db.has_object_row(&garbage.to_string()).unwrap());
                        }
                        _ => unreachable!(),
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let db = open(&path);
    drain_edges(&db);
    assert_contents(&db, &root, &root_body);
    assert_contents(&db, &child, &child_body);
    let root_state = db.debug_dump_expand_state(&root).unwrap();
    assert_eq!(root_state.eviction_class, 2);
    assert!(root_state.has_recursive_pin);
    assert!(root_state.children_expanded);
    assert_eq!(root_state.fs_anchor_count, 0);
    let child_state = db.debug_dump_expand_state(&child).unwrap();
    assert_eq!(child_state.eviction_class, 1);
    assert_eq!(child_state.incoming_refs_count, 1);
    assert_eq!(db.list_pins_by_owner("", None, 100).unwrap().len(), 2);
    let stored = db.get_chunk_item(&pinned_chunk.chunk_id).unwrap();
    assert_eq!(stored.chunk_size, pinned_chunk.chunk_size);
    assert_eq!(stored.chunk_state, ChunkStoreState::Completed);
    db.unpin(&pinned_chunk.chunk_id.to_obj_id(), "permanent-chunk")
        .unwrap();
    assert_eq!(
        db.try_evict_chunk(&pinned_chunk.chunk_id.to_string())
            .unwrap(),
        pinned_chunk.chunk_size
    );
    assert!(db.get_chunk_item(&pinned_chunk.chunk_id).is_err());
    assert!(!db
        .has_object_row(&pinned_chunk.chunk_id.to_string())
        .unwrap());
    db.unpin(&root, "permanent").unwrap();
    drain_edges(&db);
    assert_eq!(
        db.debug_dump_expand_state(&child)
            .unwrap()
            .incoming_refs_count,
        0
    );
    assert_eq!(
        db.try_evict_object(&root.to_string()).unwrap(),
        root_body.len() as u64
    );
    assert_eq!(
        db.try_evict_object(&child.to_string()).unwrap(),
        child_body.len() as u64
    );
    assert_eq!(db.outbox_count().unwrap(), 0);
}
