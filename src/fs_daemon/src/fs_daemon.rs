use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request,
};
use libc::{EAGAIN, EBADF, EINVAL, EIO, EISDIR, ENOENT, ENOSYS, EPERM};
use log::{debug, info, warn};
use named_store::{NamedLocalConfig, NamedLocalStore, NamedStoreMgr, StoreLayout, StoreTarget};
use ndm::{
    CommitPolicy, NamedDataMgr, NamedDataMgrRef, NdmFileWriter, OpenWriteFlag, PathKind,
    ReadOptions,
};
use ndn_lib::{NdmPath, NdnError, NdnResult};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

use fs_buffer::LocalFileBufferService;
use fs_meta::fs_meta_service::FSMetaService;

const TTL: Duration = Duration::from_secs(1);
pub const DEFAULT_STORE_LAYOUT_CONFIG_PATH: &str = "/opt/buckyos/etc/store_layout.json";
pub const DEFAULT_FS_DAEMON_CONFIG_PATH: &str = "/opt/buckyos/etc/fs_daemon.json";
#[cfg(target_os = "macos")]
const XATTR_NOT_FOUND: i32 = libc::ENOATTR;
#[cfg(not(target_os = "macos"))]
const XATTR_NOT_FOUND: i32 = libc::ENODATA;
#[cfg(target_os = "macos")]
const XATTR_CREATE_FLAG: i32 = libc::XATTR_CREATE as i32;
#[cfg(target_os = "macos")]
const XATTR_REPLACE_FLAG: i32 = libc::XATTR_REPLACE as i32;
#[cfg(not(target_os = "macos"))]
const XATTR_CREATE_FLAG: i32 = libc::XATTR_CREATE;
#[cfg(not(target_os = "macos"))]
const XATTR_REPLACE_FLAG: i32 = libc::XATTR_REPLACE;

#[derive(Debug, Clone)]
pub struct FsDaemonRunOptions {
    pub mountpoint: PathBuf,
    pub store_config_path: PathBuf,
    pub service_config_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct StoreLayoutConfigFile {
    epoch: u64,
    #[serde(alias = "targets")]
    stores: Vec<StoreConfigEntry>,
    total_capacity: Option<u64>,
    total_used: Option<u64>,
}

impl Default for StoreLayoutConfigFile {
    fn default() -> Self {
        Self {
            epoch: 1,
            stores: Vec::new(),
            total_capacity: None,
            total_used: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct StoreConfigEntry {
    store_id: Option<String>,
    #[serde(alias = "base_dir", alias = "root_path", alias = "store_path")]
    path: PathBuf,
    capacity: Option<u64>,
    used: Option<u64>,
    readonly: bool,
    enabled: bool,
    weight: u32,
}

impl Default for StoreConfigEntry {
    fn default() -> Self {
        Self {
            store_id: None,
            path: PathBuf::new(),
            capacity: None,
            used: None,
            readonly: false,
            enabled: true,
            weight: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct FsDaemonServiceConfig {
    #[serde(alias = "instance", alias = "ndm_instance_id")]
    instance_id: String,
    #[serde(alias = "buffer_dir", alias = "fs_buffer_path")]
    fs_buffer_dir: PathBuf,
    #[serde(alias = "meta_db_path", alias = "fs_meta_path")]
    fs_meta_db_path: PathBuf,
    #[serde(alias = "buffer_size_limit")]
    fs_buffer_size_limit: u64,
}

impl Default for FsDaemonServiceConfig {
    fn default() -> Self {
        Self {
            instance_id: "default".to_string(),
            fs_buffer_dir: PathBuf::from("/opt/buckyos/var/fs_buffer"),
            fs_meta_db_path: PathBuf::from("/opt/buckyos/var/fs_meta/fs_meta.db"),
            fs_buffer_size_limit: 0,
        }
    }
}

struct FsMetaServiceRunner {
    service: Option<FSMetaService>,
}

impl FsMetaServiceRunner {
    fn new(service: FSMetaService) -> Self {
        Self {
            service: Some(service),
        }
    }

    fn start_in_process(mut self) -> NdnResult<Arc<ndm::FsMetaClient>> {
        let service = self
            .service
            .take()
            .ok_or_else(|| NdnError::InvalidState("fs_meta runner already started".to_string()))?;
        Ok(Arc::new(ndm::FsMetaClient::new_in_process(Box::new(
            service,
        ))))
    }
}

struct InodeTable {
    next_inode: AtomicU64,
    inode_to_path: RwLock<HashMap<u64, String>>,
    path_to_inode: RwLock<HashMap<String, u64>>,
}

impl InodeTable {
    fn new() -> Self {
        let mut inode_to_path = HashMap::new();
        let mut path_to_inode = HashMap::new();
        inode_to_path.insert(1, "/".to_string());
        path_to_inode.insert("/".to_string(), 1);
        Self {
            next_inode: AtomicU64::new(2_000_000),
            inode_to_path: RwLock::new(inode_to_path),
            path_to_inode: RwLock::new(path_to_inode),
        }
    }

    fn get_path(&self, inode: u64) -> Option<String> {
        self.inode_to_path.read().ok()?.get(&inode).cloned()
    }

    fn remember(&self, inode: u64, path: String) {
        if let Ok(mut map) = self.inode_to_path.write() {
            map.insert(inode, path.clone());
        }
        if let Ok(mut map) = self.path_to_inode.write() {
            map.insert(path, inode);
        }
    }

    fn get_or_create(&self, inode_hint: Option<u64>, path: &str) -> u64 {
        if let Some(inode) = inode_hint {
            self.remember(inode, path.to_string());
            return inode;
        }
        if let Ok(map) = self.path_to_inode.read() {
            if let Some(inode) = map.get(path) {
                return *inode;
            }
        }
        let inode = self.next_inode.fetch_add(1, Ordering::SeqCst);
        self.remember(inode, path.to_string());
        inode
    }

    fn remove_path_recursive(&self, path: &str) -> Vec<u64> {
        let mut removed_inodes = Vec::new();
        let mut inode_to_path = match self.inode_to_path.write() {
            Ok(v) => v,
            Err(_) => return removed_inodes,
        };
        let mut path_to_inode = match self.path_to_inode.write() {
            Ok(v) => v,
            Err(_) => return removed_inodes,
        };

        let prefix = format!("{}/", path);
        let paths: Vec<String> = path_to_inode
            .keys()
            .filter(|candidate| *candidate == path || candidate.starts_with(&prefix))
            .cloned()
            .collect();

        for removed_path in paths {
            if let Some(inode) = path_to_inode.remove(&removed_path) {
                inode_to_path.remove(&inode);
                removed_inodes.push(inode);
            }
        }

        removed_inodes
    }

    fn rename_path_recursive(&self, old_path: &str, new_path: &str) {
        let mut inode_to_path = match self.inode_to_path.write() {
            Ok(v) => v,
            Err(_) => return,
        };
        let mut path_to_inode = match self.path_to_inode.write() {
            Ok(v) => v,
            Err(_) => return,
        };

        let old_prefix = format!("{}/", old_path);
        let mut moved: Vec<(String, u64)> = path_to_inode
            .iter()
            .filter_map(|(path, inode)| {
                if path == old_path || path.starts_with(&old_prefix) {
                    Some((path.clone(), *inode))
                } else {
                    None
                }
            })
            .collect();
        if moved.is_empty() {
            return;
        }

        let new_prefix = format!("{}/", new_path);
        let replaced_paths: Vec<String> = path_to_inode
            .keys()
            .filter(|path| *path == new_path || path.starts_with(&new_prefix))
            .cloned()
            .collect();
        for replaced in replaced_paths {
            if let Some(inode) = path_to_inode.remove(&replaced) {
                inode_to_path.remove(&inode);
            }
        }

        moved.sort_by_key(|(path, _)| path.len());
        for (old, inode) in moved {
            path_to_inode.remove(&old);
            let suffix = old.strip_prefix(old_path).unwrap_or("");
            let new_full_path = format!("{}{}", new_path, suffix);
            path_to_inode.insert(new_full_path.clone(), inode);
            inode_to_path.insert(inode, new_full_path);
        }
    }
}

struct OpenHandle {
    writer: NdmFileWriter,
    inode_id: u64,
}

#[derive(Debug, Clone)]
struct InodeMeta {
    perm: u16,
    uid: u32,
    gid: u32,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
    crtime: SystemTime,
    flags: u32,
}

struct HandleTable {
    next_fh: AtomicU64,
    handles: Mutex<HashMap<u64, OpenHandle>>,
}

impl HandleTable {
    fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: Mutex::new(HashMap::new()),
        }
    }

    fn insert(&self, handle: OpenHandle) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut map) = self.handles.lock() {
            map.insert(fh, handle);
        }
        fh
    }

    fn with_handle_mut<F, T>(&self, fh: u64, f: F) -> Result<T, i32>
    where
        F: FnOnce(&mut OpenHandle) -> Result<T, i32>,
    {
        let mut map = self.handles.lock().map_err(|_| EIO)?;
        let handle = map.get_mut(&fh).ok_or(EBADF)?;
        f(handle)
    }

    fn remove(&self, fh: u64) -> Option<OpenHandle> {
        self.handles.lock().ok()?.remove(&fh)
    }
}

pub struct FsDaemon {
    runtime: Runtime,
    named_mgr: NamedDataMgrRef,
    inode_table: InodeTable,
    handle_table: HandleTable,
    xattrs: Mutex<HashMap<u64, HashMap<Vec<u8>, Vec<u8>>>>,
    inode_meta: Mutex<HashMap<u64, InodeMeta>>,
}

impl FsDaemon {
    pub fn new(runtime: Runtime, named_mgr: NamedDataMgrRef) -> Self {
        Self {
            runtime,
            named_mgr,
            inode_table: InodeTable::new(),
            handle_table: HandleTable::new(),
            xattrs: Mutex::new(HashMap::new()),
            inode_meta: Mutex::new(HashMap::new()),
        }
    }

    fn default_meta(default_perm: u16) -> InodeMeta {
        let now = SystemTime::now();
        InodeMeta {
            perm: default_perm,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            flags: 0,
        }
    }

    fn get_or_init_meta(&self, inode: u64, default_perm: u16) -> InodeMeta {
        let mut map = match self.inode_meta.lock() {
            Ok(v) => v,
            Err(_) => return Self::default_meta(default_perm),
        };
        map.entry(inode)
            .or_insert_with(|| Self::default_meta(default_perm))
            .clone()
    }

    fn update_meta<F>(&self, inode: u64, default_perm: u16, f: F) -> Result<(), i32>
    where
        F: FnOnce(&mut InodeMeta),
    {
        let mut map = self.inode_meta.lock().map_err(|_| EIO)?;
        let meta = map
            .entry(inode)
            .or_insert_with(|| Self::default_meta(default_perm));
        f(meta);
        Ok(())
    }

    fn path_from_parent(&self, parent: u64, name: &str) -> Option<String> {
        let parent_path = self.inode_table.get_path(parent)?;
        if parent_path == "/" {
            Some(format!("/{}", name))
        } else {
            Some(format!("{}/{}", parent_path, name))
        }
    }

    pub(crate) fn lookup_entry(&self, parent: u64, name: &str) -> Result<(u64, FileAttr), i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        let stat = self.stat_path(&path)?;
        Self::ensure_exists(&stat)?;
        let inode = self.inode_table.get_or_create(None, &path);
        let attr = self.build_attr(inode, &path, &stat);
        Ok((inode, attr))
    }

    pub(crate) fn getattr_entry(&self, ino: u64) -> Result<(u64, FileAttr), i32> {
        let path = self.inode_table.get_path(ino).ok_or(ENOENT)?;
        let stat = self.stat_path(&path)?;
        Self::ensure_exists(&stat)?;
        let inode = ino;
        let attr = self.build_attr(inode, &path, &stat);
        Ok((inode, attr))
    }

    fn ensure_exists(stat: &ndm::PathStat) -> Result<(), i32> {
        if matches!(stat.kind, PathKind::NotFound) {
            return Err(ENOENT);
        }
        Ok(())
    }

    pub(crate) fn readdir_entries(
        &self,
        ino: u64,
        offset: i64,
    ) -> Result<Vec<(u64, FileType, String, i64)>, i32> {
        let path = self.inode_table.get_path(ino).ok_or(ENOENT)?;
        let entries = self
            .runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                let session = mgr.start_list(&NdmPath::new(path.clone())).await?;
                let list = mgr.list_next(session, 0).await?;
                mgr.stop_list(session).await?;
                Ok::<_, NdnError>(list)
            })
            .map_err(map_ndn_err)?;

        let mut out = Vec::new();
        let mut idx: i64 = offset;
        if offset == 0 {
            out.push((ino, FileType::Directory, ".".to_string(), 1));
            out.push((ino, FileType::Directory, "..".to_string(), 2));
            idx = 2;
        }

        for (name, stat) in entries.into_iter().skip((idx - 2).max(0) as usize) {
            let child_path = if path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", path, name)
            };
            let inode = self.inode_table.get_or_create(None, &child_path);
            let file_type = match stat.kind {
                PathKind::Dir => FileType::Directory,
                _ => FileType::RegularFile,
            };
            idx += 1;
            out.push((inode, file_type, name, idx));
        }
        Ok(out)
    }

    fn stat_path(&self, path: &str) -> Result<ndm::PathStat, i32> {
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.stat(&NdmPath::new(path.to_string())).await
            })
            .map_err(map_ndn_err)
    }

    fn resolve_attr_size(&self, path: &str, stat: &ndm::PathStat) -> u64 {
        if let Some(size) = stat.size {
            return size;
        }
        if !matches!(stat.kind, PathKind::File | PathKind::Object) {
            return 0;
        }

        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                let (mut reader, _) = mgr
                    .open_reader(&NdmPath::new(path.to_string()), ReadOptions::default())
                    .await?;
                let mut total = 0u64;
                let mut buffer = [0u8; 8192];
                loop {
                    let n = reader.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    total += n as u64;
                }
                Ok::<u64, NdnError>(total)
            })
            .unwrap_or(0)
    }

    fn build_attr(&self, inode: u64, path: &str, stat: &ndm::PathStat) -> FileAttr {
        let size = self.resolve_attr_size(path, stat);
        let (kind, perm, nlink) = match stat.kind {
            PathKind::Dir => (FileType::Directory, 0o755, 2),
            _ => (FileType::RegularFile, 0o644, 1),
        };
        let meta = self.get_or_init_meta(inode, perm);
        FileAttr {
            ino: inode,
            size,
            blocks: 1,
            atime: meta.atime,
            mtime: meta.mtime,
            ctime: meta.ctime,
            crtime: meta.crtime,
            kind,
            perm: meta.perm,
            nlink,
            uid: meta.uid,
            gid: meta.gid,
            rdev: 0,
            flags: meta.flags,
            blksize: 4096,
        }
    }

    fn open_writer(&self, path: &str, flag: OpenWriteFlag) -> Result<u64, i32> {
        let (writer, inode_id) = self
            .runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.open_file_writer(&NdmPath::new(path.to_string()), flag, None)
                    .await
            })
            .map_err(map_ndn_err)?;
        let fh = self.handle_table.insert(OpenHandle { writer, inode_id });
        Ok(fh)
    }

    fn open_file(&self, ino: u64, flags: i32) -> Result<u64, i32> {
        let accmode = flags & libc::O_ACCMODE;
        let write = accmode == libc::O_WRONLY || accmode == libc::O_RDWR;
        if !write {
            return Ok(0);
        }
        let path = self.inode_table.get_path(ino).ok_or(ENOENT)?;
        let flag = if (flags & libc::O_TRUNC) != 0 {
            OpenWriteFlag::CreateOrTruncate
        } else if (flags & libc::O_APPEND) != 0 {
            OpenWriteFlag::Append
        } else {
            OpenWriteFlag::CreateOrAppend
        };
        self.open_writer(&path, flag)
    }

    pub(crate) fn create_file(
        &self,
        parent: u64,
        name: &str,
        flags: i32,
    ) -> Result<(FileAttr, u64), i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        let flag = if (flags & libc::O_EXCL) != 0 {
            OpenWriteFlag::CreateExclusive
        } else if (flags & libc::O_TRUNC) != 0 {
            OpenWriteFlag::CreateOrTruncate
        } else {
            OpenWriteFlag::CreateOrAppend
        };
        let fh = self.open_writer(&path, flag)?;
        let stat = self.stat_path(&path)?;
        let inode = self.inode_table.get_or_create(None, &path);
        let attr = self.build_attr(inode, &path, &stat);
        Ok((attr, fh))
    }

    pub(crate) fn mkdir_path(&self, parent: u64, name: &str) -> Result<FileAttr, i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.create_dir(&NdmPath::new(path.clone())).await
            })
            .map_err(map_ndn_err)?;
        let stat = self.stat_path(&path)?;
        let inode = self.inode_table.get_or_create(None, &path);
        Ok(self.build_attr(inode, &path, &stat))
    }

    pub(crate) fn unlink_path(&self, parent: u64, name: &str) -> Result<(), i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.delete(&NdmPath::new(path.clone())).await
            })
            .map_err(map_ndn_err)?;

        let removed_inodes = self.inode_table.remove_path_recursive(&path);
        if let Ok(mut all_xattrs) = self.xattrs.lock() {
            for inode in &removed_inodes {
                all_xattrs.remove(inode);
            }
        }
        if let Ok(mut all_meta) = self.inode_meta.lock() {
            for inode in &removed_inodes {
                all_meta.remove(inode);
            }
        }
        Ok(())
    }

    pub(crate) fn rename_path(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
    ) -> Result<(), i32> {
        let old_path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        let new_path = self.path_from_parent(newparent, newname).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.move_path(
                    &NdmPath::new(old_path.clone()),
                    &NdmPath::new(new_path.clone()),
                )
                .await
            })
            .map_err(map_ndn_err)?;

        self.inode_table.rename_path_recursive(&old_path, &new_path);
        Ok(())
    }

    pub(crate) fn read_path(&self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, i32> {
        let path = self.inode_table.get_path(ino).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                let (mut reader, _) = mgr
                    .open_reader(&NdmPath::new(path.clone()), ReadOptions::default())
                    .await?;
                if offset > 0 {
                    let mut remaining = offset as u64;
                    let mut buffer = [0u8; 8192];
                    while remaining > 0 {
                        let read_len = std::cmp::min(remaining as usize, buffer.len());
                        let n = reader.read(&mut buffer[..read_len]).await?;
                        if n == 0 {
                            break;
                        }
                        remaining -= n as u64;
                    }
                }
                let mut buffer = vec![0u8; size as usize];
                let mut read_total = 0usize;
                loop {
                    let n = reader.read(&mut buffer[read_total..]).await?;
                    if n == 0 {
                        break;
                    }
                    read_total += n;
                    if read_total == buffer.len() {
                        break;
                    }
                }
                buffer.truncate(read_total);
                Ok::<_, NdnError>(buffer)
            })
            .map_err(map_ndn_err)
    }

    pub(crate) fn write_handle(&self, fh: u64, offset: i64, data: &[u8]) -> Result<usize, i32> {
        self.handle_table.with_handle_mut(fh, |handle| {
            let written = self.runtime.block_on(async {
                handle
                    .writer
                    .seek(std::io::SeekFrom::Start(offset as u64))
                    .await?;
                handle.writer.write_all(data).await?;
                Ok::<usize, NdnError>(data.len())
            });
            match written {
                Ok(n) => Ok(n),
                Err(err) => Err(map_ndn_err(err)),
            }
        })
    }

    pub(crate) fn release_handle(&self, fh: u64) -> Result<(), i32> {
        if let Some(mut handle) = self.handle_table.remove(fh) {
            self.runtime
                .block_on(async {
                    handle.writer.flush().await?;
                    let mgr = self.named_mgr.lock().await;
                    mgr.close_file(handle.inode_id).await
                })
                .map_err(map_ndn_err)?;
        }
        Ok(())
    }
}

impl Filesystem for FsDaemon {
    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.lookup_entry(parent, name) {
            Ok((_ino, attr)) => reply.entry(&TTL, &attr, 0),
            Err(code) => reply.error(code),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.getattr_entry(ino) {
            Ok((_ino, attr)) => reply.attr(&TTL, &attr),
            Err(code) => reply.error(code),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.readdir_entries(ino, offset) {
            Ok(entries) => {
                for (inode, file_type, name, next_offset) in entries {
                    if reply.add(inode, next_offset, file_type, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(code) => reply.error(code),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.open_file(ino, flags) {
            Ok(fh) => reply.opened(fh, 0),
            Err(code) => reply.error(code),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.create_file(parent, name, flags) {
            Ok((attr, fh)) => reply.created(&TTL, &attr, 0, fh, 0),
            Err(code) => reply.error(code),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read_path(ino, offset, size) {
            Ok(data) => reply.data(&data),
            Err(code) => reply.error(code),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write_handle(fh, offset, data) {
            Ok(n) => reply.written(n as u32),
            Err(code) => reply.error(code),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.release_handle(fh) {
            Ok(_) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // Data is persisted on write/release in current implementation.
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.mkdir_path(parent, name) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(code) => reply.error(code),
        }
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.unlink_path(parent, name) {
            Ok(_) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn rmdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        self.unlink(_req, parent, name, reply);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        let newname = match newname.to_str() {
            Some(v) => v,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.rename_path(parent, name, newparent, newname) {
            Ok(_) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let create = (flags & XATTR_CREATE_FLAG) != 0;
        let replace = (flags & XATTR_REPLACE_FLAG) != 0;
        if create && replace {
            reply.error(EINVAL);
            return;
        }

        let mut all_xattrs = match self.xattrs.lock() {
            Ok(v) => v,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let entry = all_xattrs.entry(ino).or_default();
        let key = name.as_bytes().to_vec();
        let exists = entry.contains_key(&key);
        if create && exists {
            reply.error(libc::EEXIST);
            return;
        }
        if replace && !exists {
            reply.error(XATTR_NOT_FOUND);
            return;
        }

        if position == 0 {
            entry.insert(key, value.to_vec());
        } else {
            let pos = position as usize;
            let stored = entry.entry(key).or_default();
            if pos > stored.len() {
                stored.resize(pos, 0);
            }
            let end = pos + value.len();
            if end > stored.len() {
                stored.resize(end, 0);
            }
            stored[pos..end].copy_from_slice(value);
        }
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let value = self.xattrs.lock().ok().and_then(|all_xattrs| {
            all_xattrs
                .get(&ino)
                .and_then(|attrs| attrs.get(name.as_bytes()).cloned())
        });
        let value = match value {
            Some(v) => v,
            None => {
                reply.error(XATTR_NOT_FOUND);
                return;
            }
        };

        if size == 0 {
            reply.size(value.len() as u32);
            return;
        }
        if size < value.len() as u32 {
            reply.error(libc::ERANGE);
            return;
        }
        reply.data(&value);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        let listing = self
            .xattrs
            .lock()
            .ok()
            .and_then(|all_xattrs| {
                all_xattrs.get(&ino).map(|attrs| {
                    let mut data = Vec::<u8>::new();
                    for name in attrs.keys() {
                        data.extend_from_slice(name);
                        data.push(0);
                    }
                    data
                })
            })
            .unwrap_or_default();

        if size == 0 {
            reply.size(listing.len() as u32);
            return;
        }
        if size < listing.len() as u32 {
            reply.error(libc::ERANGE);
            return;
        }
        reply.data(&listing);
    }

    fn removexattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        if let Ok(mut all_xattrs) = self.xattrs.lock() {
            if let Some(attrs) = all_xattrs.get_mut(&ino) {
                attrs.remove(name.as_bytes());
                if attrs.is_empty() {
                    all_xattrs.remove(&ino);
                }
            }
        }
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(
            "setattr request ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, fh={:?}, flags={:?}",
            ino, mode, uid, gid, size, fh, flags
        );
        let (_, current_attr) = match self.getattr_entry(ino) {
            Ok(v) => v,
            Err(code) => {
                reply.error(code);
                return;
            }
        };

        if let Some(new_size) = size {
            if current_attr.kind == FileType::Directory {
                reply.error(EISDIR);
                return;
            }
            if new_size != current_attr.size {
                if new_size == 0 {
                    let path = match self.inode_table.get_path(ino) {
                        Some(p) => p,
                        None => {
                            reply.error(ENOENT);
                            return;
                        }
                    };
                    let truncate_result = self.runtime.block_on(async {
                        let mgr = self.named_mgr.lock().await;
                        let (mut writer, inode_id) = mgr
                            .open_file_writer(
                                &NdmPath::new(path),
                                OpenWriteFlag::CreateOrTruncate,
                                Some(0),
                            )
                            .await?;
                        writer.flush().await?;
                        mgr.close_file(inode_id).await
                    });
                    if let Err(err) = truncate_result {
                        reply.error(map_ndn_err(err));
                        return;
                    }
                } else if fh.is_none() {
                    reply.error(EINVAL);
                    return;
                }
            }
        }

        let now = SystemTime::now();
        let atime_new = atime.map(|v| match v {
            fuser::TimeOrNow::SpecificTime(t) => t,
            fuser::TimeOrNow::Now => now,
        });
        let mtime_new = mtime.map(|v| match v {
            fuser::TimeOrNow::SpecificTime(t) => t,
            fuser::TimeOrNow::Now => now,
        });
        if let Err(code) = self.update_meta(ino, current_attr.perm, |meta| {
            if let Some(mode) = mode {
                meta.perm = (mode & 0o7777) as u16;
            }
            if let Some(uid) = uid {
                meta.uid = uid;
            }
            if let Some(gid) = gid {
                meta.gid = gid;
            }
            if let Some(atime) = atime_new {
                meta.atime = atime;
            }
            if let Some(mtime) = mtime_new {
                meta.mtime = mtime;
            }
            if let Some(ctime) = ctime {
                meta.ctime = ctime;
            } else {
                meta.ctime = now;
            }
            if let Some(crtime) = crtime {
                meta.crtime = crtime;
            }
            if let Some(flags) = flags {
                meta.flags = flags;
            }
        }) {
            reply.error(code);
            return;
        }

        match self.getattr_entry(ino) {
            Ok((_, attr)) => {
                debug!(
                    "setattr response ino={}, perm={:o}, uid={}, gid={}, size={}",
                    ino, attr.perm, attr.uid, attr.gid, attr.size
                );
                reply.attr(&TTL, &attr)
            }
            Err(code) => reply.error(code),
        }
    }
}

fn map_ndn_err(err: NdnError) -> i32 {
    match err {
        NdnError::NotFound(_) => ENOENT,
        NdnError::NotReady(_) => EAGAIN,
        NdnError::AlreadyExists(_) => libc::EEXIST,
        NdnError::InvalidParam(_) => EINVAL,
        NdnError::InvalidState(_) => EIO,
        NdnError::PermissionDenied(_) => EPERM,
        NdnError::Unsupported(_) => ENOSYS,
        NdnError::IoError(_) => EIO,
        NdnError::DbError(_) => EIO,
        NdnError::OffsetTooLarge(_) => EINVAL,
        NdnError::InvalidObjType(_) => EINVAL,
        NdnError::InvalidData(_) => EINVAL,
        NdnError::Internal(_) => EIO,
        NdnError::InvalidId(_) => EINVAL,
        NdnError::InvalidLink(_) => EINVAL,
        NdnError::VerifyError(_) => EIO,
        NdnError::InComplete(_) => EIO,
        NdnError::RemoteError(_) => EIO,
        NdnError::DecodeError(_) => EIO,
    }
}

fn read_json_config<T: DeserializeOwned>(path: &Path) -> NdnResult<T> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| NdnError::IoError(format!("read {} failed: {}", path.display(), e)))?;
    serde_json::from_str::<T>(&content)
        .map_err(|e| NdnError::InvalidData(format!("parse {} failed: {}", path.display(), e)))
}

fn resolve_store_id(entry: &StoreConfigEntry, index: usize) -> String {
    if let Some(store_id) = entry.store_id.as_ref().filter(|v| !v.is_empty()) {
        return store_id.clone();
    }
    entry
        .path
        .file_name()
        .and_then(|v| v.to_str())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| format!("store-{}", index + 1))
}

fn init_store_mgr(runtime: &Runtime, store_config_path: &Path) -> NdnResult<Arc<NamedStoreMgr>> {
    let store_config: StoreLayoutConfigFile = read_json_config(store_config_path)?;
    if store_config.stores.len() < 3 {
        return Err(NdnError::InvalidParam(format!(
            "store config {} must include at least 3 stores",
            store_config_path.display()
        )));
    }

    let store_mgr = Arc::new(NamedStoreMgr::new());
    let mut targets = Vec::with_capacity(store_config.stores.len());
    let mut total_capacity = 0u64;
    let mut total_used = 0u64;
    let mut store_id_set = HashSet::new();

    for (index, entry) in store_config.stores.iter().enumerate() {
        if entry.path.as_os_str().is_empty() {
            return Err(NdnError::InvalidParam(format!(
                "store config {} has empty path at index {}",
                store_config_path.display(),
                index
            )));
        }

        std::fs::create_dir_all(&entry.path).map_err(|e| {
            NdnError::IoError(format!(
                "create store dir {} failed: {}",
                entry.path.display(),
                e
            ))
        })?;

        let store_id = resolve_store_id(entry, index);
        let store_config = NamedLocalConfig {
            read_only: entry.readonly,
            ..Default::default()
        };
        let store = runtime.block_on(async {
            NamedLocalStore::from_config(Some(store_id.clone()), entry.path.clone(), store_config)
                .await
        })?;
        let actual_store_id = store.store_id().to_string();
        if actual_store_id != store_id {
            warn!(
                "store id mismatch, configured={}, actual={}, using actual",
                store_id, actual_store_id
            );
        }
        if !store_id_set.insert(actual_store_id.clone()) {
            return Err(NdnError::InvalidParam(format!(
                "duplicate store_id '{}' in {}",
                actual_store_id,
                store_config_path.display()
            )));
        }

        let store_ref = Arc::new(tokio::sync::Mutex::new(store));
        runtime.block_on(async { store_mgr.register_store(store_ref).await });

        total_capacity += entry.capacity.unwrap_or(0);
        total_used += entry.used.unwrap_or(0);
        targets.push(StoreTarget {
            store_id: actual_store_id,
            device_did: None,
            capacity: entry.capacity,
            used: entry.used,
            readonly: entry.readonly,
            enabled: entry.enabled,
            weight: entry.weight,
        });
    }

    let layout = StoreLayout::new(
        store_config.epoch.max(1),
        targets,
        store_config.total_capacity.unwrap_or(total_capacity),
        store_config.total_used.unwrap_or(total_used),
    );
    runtime.block_on(async { store_mgr.add_layout(layout).await });
    Ok(store_mgr)
}

pub fn init_named_mgr(
    runtime: &Runtime,
    store_config_path: &Path,
    service_config_path: &Path,
) -> NdnResult<NamedDataMgrRef> {
    // 1. load store_layout config, construct store_layout + store_mgr
    let store_mgr = init_store_mgr(runtime, store_config_path)?;

    // 2. load fs_daemon service config, init fs_buffer and fs_meta service runner
    let service_config: FsDaemonServiceConfig = read_json_config(service_config_path)?;
    std::fs::create_dir_all(&service_config.fs_buffer_dir).map_err(|e| {
        NdnError::IoError(format!(
            "create fs buffer dir {} failed: {}",
            service_config.fs_buffer_dir.display(),
            e
        ))
    })?;
    if let Some(parent) = service_config.fs_meta_db_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            NdnError::IoError(format!(
                "create fs_meta db dir {} failed: {}",
                parent.display(),
                e
            ))
        })?;
    }

    let buffer_service = Arc::new(
        LocalFileBufferService::new(
            service_config.fs_buffer_dir.clone(),
            service_config.fs_buffer_size_limit,
        )
        .with_named_store_mgr(store_mgr.clone()),
    );

    let instance_id = service_config.instance_id.clone();
    let fs_meta_service = runtime
        .block_on(async {
            FSMetaService::new(service_config.fs_meta_db_path.to_string_lossy().to_string())
        })
        .map_err(|e| NdnError::Internal(format!("create fs_meta service failed: {}", e)))?
        .with_buffer(instance_id.clone(), buffer_service.clone())
        .with_named_store(store_mgr.clone());
    let runner = FsMetaServiceRunner::new(fs_meta_service);
    let fs_meta_client = runner.start_in_process()?;
    buffer_service.set_fsmeta_client(fs_meta_client.clone())?;

    // 3. construct named_mgr, register with default id (None -> "default")
    let named_mgr = NamedDataMgr::with_layout_mgr(
        instance_id,
        fs_meta_client,
        buffer_service,
        None,
        CommitPolicy::default(),
        store_mgr,
    );
    runtime.block_on(async {
        NamedDataMgr::register_named_data_mgr("default", named_mgr).await?;
        NamedDataMgr::get_named_data_mgr_by_id(None)
            .await
            .ok_or_else(|| {
                NdnError::NotFound("default named data manager is not registered".to_string())
            })
    })
}

pub fn run_fs_daemon(options: FsDaemonRunOptions) -> NdnResult<()> {
    let runtime = Runtime::new().map_err(|e| NdnError::Internal(e.to_string()))?;
    let named_mgr = init_named_mgr(
        &runtime,
        &options.store_config_path,
        &options.service_config_path,
    )?;

    std::fs::create_dir_all(&options.mountpoint).map_err(|e| {
        NdnError::IoError(format!(
            "create mountpoint {} failed: {}",
            options.mountpoint.display(),
            e
        ))
    })?;

    // 4. mount FUSE with the named_mgr resolved from default registry
    let filesystem = FsDaemon::new(runtime, named_mgr);
    let mount_options = vec![
        MountOption::FSName("ndnfs".to_string()),
        MountOption::DefaultPermissions,
    ];
    #[cfg(not(target_os = "macos"))]
    let mut mount_options = mount_options;
    #[cfg(not(target_os = "macos"))]
    mount_options.push(MountOption::AutoUnmount);
    info!(
        "mounting fs_daemon at {:?}, store_config={}, service_config={}",
        options.mountpoint,
        options.store_config_path.display(),
        options.service_config_path.display()
    );
    match fuser::spawn_mount2(filesystem, &options.mountpoint, &mount_options) {
        Ok(session) => {
            println!("fs_daemon mounted at {:?}", options.mountpoint);
            session.join();
            Ok(())
        }
        Err(err) => Err(NdnError::IoError(format!("mount failed: {}", err))),
    }
}
