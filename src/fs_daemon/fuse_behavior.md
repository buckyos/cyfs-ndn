# FUSE Behavior Requirements (Draft)

This document captures intended FUSE behavior for `fs_daemon`. It is a working draft with TODOs and open questions.

## Core Principles

- The FUSE layer is a thin adapter over `NamedDataMgr` (NDM).
- Avoid direct use of `store_mgr` / `fs_buffer` from the FUSE request path.
- Any data persistence or linking is managed by NDM semantics, not local filesystem semantics.

## Path & Inode Semantics

- Inodes are derived from NDM inode IDs when available; otherwise a stable synthetic inode is created.
- Path resolution is delegated to NDM (`resolve_path`, `stat`, `list_*`).
- TODO: Define cache invalidation strategy for inode/path maps.

## Supported Operations (Target)

### Lookup / Getattr

- Map `lookup` and `getattr` to `NamedDataMgr::stat`.
- Use NDM inode ID when present.
- TODO: Define UID/GID/permissions mapping rules (currently fixed 755/644).

### Readdir

- Use `start_list` + `list_next` + `stop_list`.
- TODO: Define paging behavior and stable offsets for large directories.

### Read

- Use `open_reader` with `ReadOptions`.
- TODO: Decide whether to reuse readers per file handle or open per read.
- TODO: Define read caching behavior and readahead policy.

### Write / Create

- Use `open_file_writer` with `OpenWriteFlag` derived from flags.
- Use `close_file` on release.
- TODO: Define `O_TRUNC` and `O_APPEND` mapping for NDM write state.
- TODO: Define `fsync` / `flush` expectations and commit policy.

### Mkdir / Unlink / Rename

- Use `create_dir`, `delete`, `move_path`.
- TODO: Define behavior for non-empty directory removal.
- TODO: Define atomicity expectations for `rename`.

## Link Semantics

- Local filesystem hardlink/symlink creation must be rejected.
- NDM can create internal links (e.g., `symlink`), which may be exposed via a dedicated command or extended API.
- TODO: Decide if/when to map FUSE `link`/`symlink` to NDM `symlink`.

## Unsupported Operations (Current)

- `setattr`, `symlink`, `link`, `mknod`, `readlink`, `xattr`.
- TODO: Revisit unsupported operations list after NDM capability review.

## Error Mapping

- Map `NdnError` to standard `errno` values (ENOENT, EEXIST, EINVAL, EPERM, ENOSYS, EIO).
- TODO: Add explicit mapping for permission/auth errors from future NDM policies.

## Security & Permissions

- TODO: Define ACL/permission model and enforcement mapping.
- TODO: Consider user namespace / uid/gid translation rules.

## Mount & Runtime

- TODO: Define mount options (caching, allow_other, default_permissions).
- TODO: Define logging, metrics, and tracing requirements.
