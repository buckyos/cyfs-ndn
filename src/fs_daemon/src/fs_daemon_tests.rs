use crate::fs_daemon::{init_named_mgr, FsDaemon};
use fuser::FileType;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_test_configs(tmp: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let stores_root = tmp.path().join("stores");
    let store_a = stores_root.join("store-a");
    let store_b = stores_root.join("store-b");
    let store_c = stores_root.join("store-c");
    std::fs::create_dir_all(&store_a).expect("create store-a");
    std::fs::create_dir_all(&store_b).expect("create store-b");
    std::fs::create_dir_all(&store_c).expect("create store-c");

    let store_config = tmp.path().join("store_layout.json");
    let store_config_json = serde_json::json!({
        "epoch": 1,
        "stores": [
            { "store_id": "store-a", "path": store_a, "weight": 1 },
            { "store_id": "store-b", "path": store_b, "weight": 1 },
            { "store_id": "store-c", "path": store_c, "weight": 1 }
        ]
    });
    std::fs::write(
        &store_config,
        serde_json::to_string_pretty(&store_config_json).expect("serialize store config"),
    )
    .expect("write store config");

    let service_config = tmp.path().join("fs_daemon.json");
    let service_config_json = serde_json::json!({
        "instance_id": "test",
        "fs_buffer_dir": tmp.path().join("fs_buffer"),
        "fs_meta_db_path": tmp.path().join("fs_meta").join("fs_meta.db"),
        "fs_buffer_size_limit": 0
    });
    std::fs::write(
        &service_config,
        serde_json::to_string_pretty(&service_config_json).expect("serialize service config"),
    )
    .expect("write service config");

    (store_config, service_config)
}

fn create_test_daemon() -> (FsDaemon, TempDir) {
    let tmp = TempDir::new().expect("create temp dir");
    let runtime = Runtime::new().expect("create runtime");
    let (store_config, service_config) = create_test_configs(&tmp);
    let named_mgr =
        init_named_mgr(&runtime, &store_config, &service_config).expect("init named mgr");
    (FsDaemon::new(runtime, named_mgr), tmp)
}

#[test]
fn test_mkdir_and_lookup() {
    let (daemon, _tmp) = create_test_daemon();
    let attr = daemon.mkdir_path(1, "alpha").expect("mkdir alpha");
    assert_eq!(attr.kind, FileType::Directory);
    let (_ino, lookup_attr) = daemon.lookup_entry(1, "alpha").expect("lookup alpha");
    assert_eq!(lookup_attr.kind, FileType::Directory);
}

#[test]
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

#[test]
fn test_rename_keeps_inode_mapping() {
    let (daemon, _tmp) = create_test_daemon();
    let (_attr, fh) = daemon
        .create_file(1, "temp.bin", libc::O_CREAT | libc::O_RDWR)
        .expect("create temp.bin");
    daemon.write_handle(fh, 0, b"abc").expect("write");
    daemon.release_handle(fh).expect("release");

    let (old_ino, _) = daemon.lookup_entry(1, "temp.bin").expect("lookup old");
    daemon
        .rename_path(1, "temp.bin", 1, "final.bin")
        .expect("rename");

    let (new_ino, _) = daemon.lookup_entry(1, "final.bin").expect("lookup new");
    assert_eq!(new_ino, old_ino);
    assert!(daemon.getattr_entry(old_ino).is_ok());
}

#[test]
fn test_rename_dir_keeps_child_visible() {
    let (daemon, _tmp) = create_test_daemon();
    let src_attr = daemon.mkdir_path(1, "src").expect("mkdir src");
    daemon
        .mkdir_path(src_attr.ino, "nested")
        .expect("mkdir nested");

    daemon.rename_path(1, "src", 1, "dst").expect("rename dir");
    let (dst_ino, _) = daemon.lookup_entry(1, "dst").expect("lookup dst");
    let (_child_ino, child_attr) = daemon
        .lookup_entry(dst_ino, "nested")
        .expect("lookup child");
    assert_eq!(child_attr.kind, FileType::Directory);
}
