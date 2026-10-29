//! Lock admission and error propagation regressions for NamedLocalStoreDB.

use named_store::NamedLocalStoreDB;
use ndn_lib::{NdnError, ObjId};
use rusqlite::{params, Connection};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const BODY: &str = r#"{"body":{},"create_time":0,"file_count":0,"file_size":0,"last_update_time":0,"total_size":0}"#;

fn object_id() -> ObjId {
    ObjId::new("cydir:68fa31e4de063f4824b58022cbd15223edc71f59a4a7109ac3f94f5688e47ff9").unwrap()
}

fn setup() -> (tempfile::TempDir, NamedLocalStoreDB, Connection) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store.sqlite");
    let store = NamedLocalStoreDB::new(path.to_str().unwrap().to_owned()).unwrap();
    let other = Connection::open(path).unwrap();
    (root, store, other)
}

#[test]
fn writer_waits_for_another_connection_and_then_commits() {
    let (_root, store, other) = setup();
    other.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx
            .send(store.put_object_gc_aware(&object_id(), "cydir", BODY))
            .unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let before_release = result_rx.recv_timeout(Duration::from_millis(150));
    other.execute_batch("COMMIT").unwrap();
    assert!(
        matches!(before_release, Err(mpsc::RecvTimeoutError::Timeout)),
        "a competing writer must wait instead of failing immediately: {before_release:?}"
    );
    result_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();

    let stored: String = other
        .query_row(
            "SELECT obj_data FROM objects WHERE obj_id = ?1",
            params![object_id().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, BODY);
}

#[test]
fn sustained_writer_contention_returns_structured_busy_without_partial_write() {
    let (_root, store, other) = setup();
    other.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    let result = store.put_object_gc_aware(&object_id(), "cydir", BODY);
    let elapsed = started.elapsed();
    other.execute_batch("COMMIT").unwrap();

    let error = result.unwrap_err();
    assert!(
        matches!(
            error,
            NdnError::SqliteError {
                code: 5,
                extended_code: 5,
                ..
            }
        ),
        "expected SQLITE_BUSY with its original codes, got {error:?}"
    );
    // SQLite's timeout budgets lock-wait sleeps, not all scheduler/CPU time.
    assert!(elapsed >= Duration::from_millis(4500), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(20), "{elapsed:?}");
    assert!(!store.has_object_row(&object_id().to_string()).unwrap());

    store
        .put_object_gc_aware(&object_id(), "cydir", BODY)
        .unwrap();
    assert_eq!(store.get_object(&object_id()).unwrap().1, BODY);
}

#[test]
fn constraint_error_keeps_extended_code_and_rolls_back() {
    let (_root, store, other) = setup();
    other
        .execute_batch(
            "CREATE TRIGGER reject_object BEFORE INSERT ON objects
             BEGIN SELECT RAISE(ABORT, 'injected object constraint'); END;",
        )
        .unwrap();

    let error = store
        .put_object_gc_aware(&object_id(), "cydir", BODY)
        .unwrap_err();
    match error {
        NdnError::SqliteError {
            code,
            extended_code,
            message,
        } => {
            assert_eq!(code, 19); // SQLITE_CONSTRAINT
            assert_eq!(extended_code, 1811); // SQLITE_CONSTRAINT_TRIGGER
            assert!(message.contains("injected object constraint"));
        }
        error => panic!("expected structured SQLite constraint error, got {error:?}"),
    }
    assert!(!store.has_object_row(&object_id().to_string()).unwrap());

    other.execute_batch("DROP TRIGGER reject_object").unwrap();
    store
        .put_object_gc_aware(&object_id(), "cydir", BODY)
        .unwrap();
    assert_eq!(store.get_object(&object_id()).unwrap().1, BODY);
}

#[test]
fn unreadable_object_state_is_not_treated_as_a_missing_object() {
    let (_root, store, other) = setup();
    store
        .put_object_gc_aware(&object_id(), "cydir", BODY)
        .unwrap();
    // SQLite allows a BLOB in a TEXT-affinity column. Reading it as String
    // fails; swallowing that failure would incorrectly INSERT OR REPLACE.
    other
        .execute(
            "UPDATE objects SET state = X'00FF' WHERE obj_id = ?1",
            params![object_id().to_string()],
        )
        .unwrap();

    let error = store
        .put_object_gc_aware(&object_id(), "cydir", "replacement must not be written")
        .unwrap_err();
    assert!(matches!(error, NdnError::DbError(_)), "{error:?}");
    let (state, body): (Vec<u8>, String) = other
        .query_row(
            "SELECT state, obj_data FROM objects WHERE obj_id = ?1",
            params![object_id().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, vec![0, 255]);
    assert_eq!(body, BODY);
}
