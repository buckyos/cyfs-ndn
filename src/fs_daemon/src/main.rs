use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EBADF, EIO, EINVAL, ENOENT, ENOSYS, EPERM};
use log::{error, info};
use named_store::{NamedLocalStore, NamedStoreMgr, StoreLayout, StoreTarget};
use ndm::{CommitPolicy, NamedDataMgr, NamedDataMgrRef, OpenWriteFlag, PathKind, ReadOptions};
use ndn_lib::{NdmPath, NdnError, NdnResult};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::runtime::Runtime;

use fs_buffer::LocalFileBufferService;
use fs_meta::fs_meta_service::FSMetaService;

const TTL: Duration = Duration::from_secs(1);

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

}

struct OpenHandle {
    writer: fs_buffer::FileBufferSeekWriter,
    inode_id: u64,
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

struct FsDaemon {
    runtime: Runtime,
    named_mgr: NamedDataMgrRef,
    inode_table: InodeTable,
    handle_table: HandleTable,
}

impl FsDaemon {
    fn new(runtime: Runtime, named_mgr: NamedDataMgrRef) -> Self {
        Self {
            runtime,
            named_mgr,
            inode_table: InodeTable::new(),
            handle_table: HandleTable::new(),
        }
    }

    fn path_from_parent(&self, parent: u64, name: &str) -> Option<String> {
        let parent_path = self.inode_table.get_path(parent)?;
        if parent_path == "/" {
            Some(format!("/{}", name))
        } else {
            Some(format!("{}/{}", parent_path, name))
        }
    }

    fn lookup_entry(&self, parent: u64, name: &str) -> Result<(u64, FileAttr), i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        let stat = self.stat_path(&path)?;
        let inode = self.inode_table.get_or_create(stat.inode_id, &path);
        let attr = self.build_attr(inode, &stat);
        Ok((inode, attr))
    }

    fn getattr_entry(&self, ino: u64) -> Result<(u64, FileAttr), i32> {
        let path = self.inode_table.get_path(ino).ok_or(ENOENT)?;
        let stat = self.stat_path(&path)?;
        let inode = self.inode_table.get_or_create(stat.inode_id, &path);
        let attr = self.build_attr(inode, &stat);
        Ok((inode, attr))
    }

    fn readdir_entries(&self, ino: u64, offset: i64) -> Result<Vec<(u64, FileType, String, i64)>, i32> {
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
            let inode = self.inode_table.get_or_create(stat.inode_id, &child_path);
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

    fn build_attr(&self, inode: u64, stat: &ndm::PathStat) -> FileAttr {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let (kind, perm, nlink) = match stat.kind {
            PathKind::Dir => (FileType::Directory, 0o755, 2),
            _ => (FileType::RegularFile, 0o644, 1),
        };
        FileAttr {
            ino: inode,
            size: stat.size.unwrap_or(0),
            blocks: 1,
            atime: UNIX_EPOCH + Duration::from_secs(now),
            mtime: UNIX_EPOCH + Duration::from_secs(now),
            ctime: UNIX_EPOCH + Duration::from_secs(now),
            crtime: UNIX_EPOCH + Duration::from_secs(now),
            kind,
            perm,
            nlink,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
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

    fn create_file(&self, parent: u64, name: &str, flags: i32) -> Result<(FileAttr, u64), i32> {
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
        let inode = self.inode_table.get_or_create(stat.inode_id, &path);
        let attr = self.build_attr(inode, &stat);
        Ok((attr, fh))
    }

    fn mkdir_path(&self, parent: u64, name: &str) -> Result<FileAttr, i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.create_dir(&NdmPath::new(path.clone())).await
            })
            .map_err(map_ndn_err)?;
        let stat = self.stat_path(&path)?;
        let inode = self.inode_table.get_or_create(stat.inode_id, &path);
        Ok(self.build_attr(inode, &stat))
    }

    fn unlink_path(&self, parent: u64, name: &str) -> Result<(), i32> {
        let path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.delete(&NdmPath::new(path)).await
            })
            .map_err(map_ndn_err)
    }

    fn rename_path(&self, parent: u64, name: &str, newparent: u64, newname: &str) -> Result<(), i32> {
        let old_path = self.path_from_parent(parent, name).ok_or(ENOENT)?;
        let new_path = self.path_from_parent(newparent, newname).ok_or(ENOENT)?;
        self.runtime
            .block_on(async {
                let mgr = self.named_mgr.lock().await;
                mgr.move_path(&NdmPath::new(old_path), &NdmPath::new(new_path))
                    .await
            })
            .map_err(map_ndn_err)
    }

    fn read_path(&self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, i32> {
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

    fn write_handle(&self, fh: u64, offset: i64, data: &[u8]) -> Result<usize, i32> {
        self.handle_table.with_handle_mut(fh, |handle| {
            let written = self.runtime.block_on(async {
                handle
                    .writer
                    .seek(std::io::SeekFrom::Start(offset as u64))
                    .await
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                handle
                    .writer
                    .write_all(data)
                    .await
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                handle
                    .writer
                    .flush()
                    .await
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok::<usize, NdnError>(data.len())
            });
            match written {
                Ok(n) => Ok(n),
                Err(err) => Err(map_ndn_err(err)),
            }
        })
    }

    fn release_handle(&self, fh: u64) -> Result<(), i32> {
        if let Some(handle) = self.handle_table.remove(fh) {
            self.runtime
                .block_on(async {
                    let mgr = self.named_mgr.lock().await;
                    mgr.close_file(handle.inode_id).await
                })
                .map_err(map_ndn_err)?;
        }
        Ok(())
    }
}

impl Filesystem for FsDaemon {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEntry) {
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

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(ENOSYS);
    }
}

fn map_ndn_err(err: NdnError) -> i32 {
    match err {
        NdnError::NotFound(_) => ENOENT,
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

fn build_layout(store_id: &str) -> StoreLayout {
    let target = StoreTarget {
        store_id: store_id.to_string(),
        device_did: None,
        capacity: None,
        used: None,
        readonly: false,
        enabled: true,
        weight: 1,
    };
    StoreLayout::new(1, vec![target], 0, 0)
}

fn init_named_mgr(runtime: &Runtime, mgr_id: &str, base_dir: &Path, instance_id: &str) -> NdnResult<NamedDataMgrRef> {
    let fs_meta_dir = base_dir.join("fs_meta");
    let fs_buffer_dir = base_dir.join("fs_buffer");
    let store_dir = base_dir.join("named_store");
    std::fs::create_dir_all(&fs_meta_dir).map_err(|e| NdnError::IoError(e.to_string()))?;
    std::fs::create_dir_all(&fs_buffer_dir).map_err(|e| NdnError::IoError(e.to_string()))?;
    std::fs::create_dir_all(&store_dir).map_err(|e| NdnError::IoError(e.to_string()))?;

    let db_path = fs_meta_dir.join("fs_meta.db");
    let buffer_service = Arc::new(LocalFileBufferService::new(fs_buffer_dir, 0));
    let fs_meta_service = FSMetaService::new(db_path.to_string_lossy().to_string())
        .map_err(|e| NdnError::Internal(e.to_string()))
        .map(|svc| svc.with_buffer(instance_id.to_string(), buffer_service.clone()))?;
    let fs_meta_client = Arc::new(ndm::FsMetaClient::new_in_process(Box::new(fs_meta_service)));

    let store_mgr = Arc::new(NamedStoreMgr::new());
    let store = runtime.block_on(async {
        NamedLocalStore::get_named_store_by_path(store_dir.clone()).await
    })?;
    let store_id = store.store_id().to_string();
    let store_ref = Arc::new(tokio::sync::Mutex::new(store));
    runtime.block_on(async {
        store_mgr.register_store(store_ref).await;
        store_mgr.add_layout(build_layout(&store_id)).await;
    });

    let named_mgr = NamedDataMgr::with_layout_mgr(
        instance_id.to_string(),
        fs_meta_client,
        buffer_service,
        None,
        CommitPolicy::default(),
        store_mgr,
    );
    runtime.block_on(async { NamedDataMgr::register_named_data_mgr(mgr_id, named_mgr).await })
}

fn parse_args() -> Result<(PathBuf, PathBuf, String, String), String> {
    let mut args = env::args().skip(1).collect::<Vec<String>>();
    if args.len() < 2 {
        return Err("usage: fs_daemon <mountpoint> <data_dir> [mgr_id] [instance_id]".to_string());
    }
    let mountpoint = PathBuf::from(args.remove(0));
    let data_dir = PathBuf::from(args.remove(0));
    let mgr_id = args.get(0).cloned().unwrap_or_else(|| "default".to_string());
    let instance_id = args.get(1).cloned().unwrap_or_else(|| "default".to_string());
    Ok((mountpoint, data_dir, mgr_id, instance_id))
}

fn main() {
    env_logger::init();
    let (mountpoint, data_dir, mgr_id, instance_id) = match parse_args() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{}", msg);
            std::process::exit(1);
        }
    };

    let runtime = Runtime::new().expect("create tokio runtime");
    let named_mgr = match init_named_mgr(&runtime, &mgr_id, &data_dir, &instance_id) {
        Ok(mgr) => mgr,
        Err(err) => {
            error!("init named mgr failed: {}", err);
            std::process::exit(1);
        }
    };

    let filesystem = FsDaemon::new(runtime, named_mgr);
    let options = vec![
        MountOption::FSName("ndnfs".to_string()),
        MountOption::DefaultPermissions,
        MountOption::AutoUnmount,
    ];
    info!("mounting fs_daemon at {:?}", mountpoint);
    match fuser::spawn_mount2(filesystem, &mountpoint, &options) {
        Ok(session) => {
            println!("fs_daemon mounted at {:?}", mountpoint);
            session.join();
        }
        Err(err) => {
            error!("mount failed: {}", err);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod fs_daemon_tests;
