use super::*;
use tempfile::TempDir;

fn create_test_daemon() -> (FsDaemon, TempDir) {
    let tmp = TempDir::new().expect("create temp dir");
    let runtime = Runtime::new().expect("create runtime");
    let named_mgr = init_named_mgr(&runtime, "test", tmp.path(), "test").expect("init named mgr");
    (FsDaemon::new(runtime, named_mgr), tmp)
}

#[test]
#[ignore]
fn test_mkdir_and_lookup() {
    let (daemon, _tmp) = create_test_daemon();
    let attr = daemon.mkdir_path(1, "alpha").expect("mkdir alpha");
    assert_eq!(attr.kind, FileType::Directory);
    let (_ino, lookup_attr) = daemon.lookup_entry(1, "alpha").expect("lookup alpha");
    assert_eq!(lookup_attr.kind, FileType::Directory);
}

#[test]
#[ignore]
fn test_create_write_read_file() {
    let (daemon, _tmp) = create_test_daemon();
    let (attr, fh) = daemon
        .create_file(1, "file.txt", libc::O_CREAT | libc::O_RDWR)
        .expect("create file");
    daemon.write_handle(fh, 0, b"hello").expect("write");
    daemon.release_handle(fh).expect("release");
    let data = daemon.read_path(attr.ino, 0, 5).expect("read");
    assert_eq!(data, b"hello");
}

#[test]
#[ignore]
fn test_rename_file() {
    let (daemon, _tmp) = create_test_daemon();
    let (_attr, fh) = daemon
        .create_file(1, "old.txt", libc::O_CREAT | libc::O_RDWR)
        .expect("create old");
    daemon.release_handle(fh).expect("release");

    daemon
        .rename_path(1, "old.txt", 1, "new.txt")
        .expect("rename");
    assert!(daemon.lookup_entry(1, "old.txt").is_err());
    assert!(daemon.lookup_entry(1, "new.txt").is_ok());
}

#[test]
#[ignore]
fn test_unlink_file() {
    let (daemon, _tmp) = create_test_daemon();
    let (_attr, fh) = daemon
        .create_file(1, "delete.txt", libc::O_CREAT | libc::O_RDWR)
        .expect("create delete");
    daemon.release_handle(fh).expect("release");
    daemon.unlink_path(1, "delete.txt").expect("unlink");
    assert!(daemon.lookup_entry(1, "delete.txt").is_err());
}

#[test]
#[ignore]
fn test_readdir_contains_entries() {
    let (daemon, _tmp) = create_test_daemon();
    daemon.mkdir_path(1, "dir").expect("mkdir dir");
    let (_attr, fh) = daemon
        .create_file(1, "file", libc::O_CREAT | libc::O_RDWR)
        .expect("create file");
    daemon.release_handle(fh).expect("release");

    let entries = daemon.readdir_entries(1, 0).expect("readdir");
    let names: Vec<String> = entries.into_iter().map(|e| e.2).collect();
    assert!(names.contains(&"dir".to_string()));
    assert!(names.contains(&"file".to_string()));
}
