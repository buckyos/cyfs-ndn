FileSystem的守护进程，实现下面功能

- 实现FUSE要求的接口，让系统可以将NDM提供的FileSystem抽象挂载到本地文件系统上
- 在实现内部，主要是转调named_mgr的接口，尽量不直接使用store_mgr,绝对不使用fs_daemon和fs_buffer
- named_mgr用单机模式初始化，包括其内部使用的fs_buffer,fs_meta（通过kRPC都支持进程内模式）。

Usage:

1) Build and run:

```bash
cargo run -p fs_daemon -- <mountpoint> <data_dir> [mgr_id] [instance_id]
```

2) Example:

```bash
cargo run -p fs_daemon -- /mnt/ndn /var/lib/ndnfs default default
```

Mount note (one line):

- macOS: install macFUSE, then `mkdir -p /Volumes/ndn && cargo run -p fs_daemon -- /Volumes/ndn /var/lib/ndnfs`.
- Linux: install FUSE, then `mkdir -p /mnt/ndn && cargo run -p fs_daemon -- /mnt/ndn /var/lib/ndnfs`.

Notes:

- The daemon initializes `named_mgr` in single-machine mode (in-process fs_meta + fs_buffer + named_store).
- FUSE operations route through `NamedDataMgr` APIs; the daemon avoids direct store_mgr usage in the request path.
