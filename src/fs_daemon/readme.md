FileSystem的守护进程，实现下面功能

- 实现FUSE要求的接口，让系统可以将基于 NDM 的 CYFS 文件系统抽象挂载到本地文件系统上
- 在实现内部，主要是转调named_mgr的接口，尽量不直接使用store_mgr,绝对不使用fs_daemon和fs_buffer
- CYFS 在单机模式下初始化 named_mgr，包括其内部使用的fs_buffer,fs_meta（通过kRPC都支持进程内模式）。

Usage:

1) Build and run:

```bash
cargo run -p fs_daemon -- <mountpoint> [--store-config <path>] [--service-config <path>]
```

2) Example:

```bash
cargo run -p fs_daemon -- /mnt/cyfs \
  --store-config /opt/buckyos/etc/store_layout.json \
  --service-config /opt/buckyos/etc/fs_daemon.json
```

Mount note (one line):

- macOS: install macFUSE, then `mkdir -p /Volumes/cyfs && cargo run -p fs_daemon -- /Volumes/cyfs`.
- Linux: install FUSE, then `mkdir -p /mnt/cyfs && cargo run -p fs_daemon -- /mnt/cyfs`.

Notes:

- The daemon initializes `named_mgr` in single-machine mode (in-process fs_meta + fs_buffer + named_store).
- FUSE operations route through `NamedFileMgr` APIs; the daemon avoids direct store_mgr usage in the request path.

Testing:

- Run `cargo test -p fs_daemon` to execute the unit tests.
- Run CLI+FUSE e2e (service stays alive after verification for manual inspection):

```bash
cd src
./fs_daemon/tests/fuse_cli_e2e.sh
```

- The e2e script will:
  - start `fs_daemon` via command line,
  - mount to a temp directory,
  - run `fs_daemon/tests/fuse_ops_verify.sh` to do shell operations against mountpoint,
  - print PID/mount/log path and keep process running.
