//! `LocalFsBackend` —— `NamedDataStoreBackend` 的本地文件系统实现。
//!
//! 这是 `named-data-http-store` 协议的 loopback 实现：不走网络，直接操作本地文件。
//! 它承担了以前 `local_store.rs` 里所有 `tokio::fs::*` 读写职责，但对外严格遵循
//! trait 层的"两态 / 原子 / 幂等 / 失败即回滚"语义。
//!
//! 布局：
//! ```text
//! root/
//!   chunks/
//!     ab/
//!       <hex>.<obj_type>.chunk           # 已完成的 chunk
//!       <hex>.<obj_type>.chunk.tmp.<u>   # 某个 writer 的私有临时文件
//!   objects/
//!     ab/
//!       <hex>.<obj_type>.obj             # 已完成的 object
//! ```
//!
//! 元数据刻意不落 DB：chunk 是否存在 == 最终文件是否存在。这样
//! "两态" 从文件系统层面天然成立，`get_chunk_state` 不需要查询任何副表。
//!
//! 并发写同一个 chunk 的策略：
//! - 每个 writer 先检查 final 是否已存在；存在则直接 `AlreadyExists`。
//! - 否则用一个**全局唯一**的 tmp 文件名（pid + 纳秒时间戳 + 原子计数器）写入；
//!   两个并发 writer 不会互相踩对方的 tmp。
//! - 写完 + hash 校验成功后再次检查 final；如果此时 final 已被别人创建，
//!   丢弃自己的 tmp，返回 `AlreadyExists`。
//! - 否则 `rename(tmp, final)`。POSIX rename 是原子的；即便极端竞态下两个
//!   writer 都走到 rename，它们的字节内容也必然相同（hash 校验过），
//!   覆盖安全。

use crate::backend::{ChunkPresence, ChunkStateInfo, ChunkWriteOutcome, NamedDataStoreBackend};
use async_trait::async_trait;
use log::warn;
use ndn_lib::{ChunkHasher, ChunkId, ChunkReader, NdnError, NdnResult, ObjId};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const CHUNK_DIR: &str = "chunks";
const OBJECT_DIR: &str = "objects";
const CHUNK_TMP_TAG: &str = "tmp";
const COPY_BUF_SIZE: usize = 64 * 1024;

/// 全局 tmp 文件名计数器，避免同进程内 ns 冲突。
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct LocalFsBackendConfig {
    pub root: PathBuf,
    pub read_only: bool,
}

pub struct LocalFsBackend {
    chunk_dir: PathBuf,
    object_dir: PathBuf,
    read_only: bool,
}

impl LocalFsBackend {
    pub async fn new(config: LocalFsBackendConfig) -> NdnResult<Self> {
        let chunk_dir = config.root.join(CHUNK_DIR);
        let object_dir = config.root.join(OBJECT_DIR);

        if !config.read_only {
            fs::create_dir_all(&chunk_dir)
                .await
                .map_err(|e| NdnError::IoError(format!("create chunk dir failed: {}", e)))?;
            fs::create_dir_all(&object_dir)
                .await
                .map_err(|e| NdnError::IoError(format!("create object dir failed: {}", e)))?;
        }

        Ok(Self {
            chunk_dir,
            object_dir,
            read_only: config.read_only,
        })
    }

    pub fn chunk_dir(&self) -> &Path {
        &self.chunk_dir
    }

    pub fn object_dir(&self) -> &Path {
        &self.object_dir
    }

    fn ensure_writable(&self) -> NdnResult<()> {
        if self.read_only {
            Err(NdnError::PermissionDenied(
                "local fs backend is read-only".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn chunk_final_path(&self, chunk_id: &ChunkId) -> PathBuf {
        let file_name = chunk_id.to_obj_id().to_filename();
        let prefix = &file_name[0..2.min(file_name.len())];
        self.chunk_dir.join(prefix).join(file_name)
    }

    /// 为每次写入生成一个全局唯一的 tmp 路径。即便进程内有大量并发 writer
    /// 或不同进程共享同一个 chunk_dir，都不会撞车。
    fn chunk_tmp_path(&self, chunk_id: &ChunkId) -> PathBuf {
        let file_name = chunk_id.to_obj_id().to_filename();
        let prefix = &file_name[0..2.min(file_name.len())];
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = process::id();
        self.chunk_dir.join(prefix).join(format!(
            "{}.{}.{}.{}.{}",
            file_name, CHUNK_TMP_TAG, pid, ts, counter
        ))
    }

    fn object_path(&self, obj_id: &ObjId) -> PathBuf {
        let file_name = obj_id.to_filename();
        let prefix = &file_name[0..2.min(file_name.len())];
        self.object_dir.join(prefix).join(file_name)
    }

    async fn ensure_parent(path: &Path) -> NdnResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| NdnError::IoError(format!("create dir failed: {}", e)))?;
        }
        Ok(())
    }

    /// 流式地把 `source` 的前 `chunk_size` 字节拷贝到 `tmp_file`，并在拷贝过程中
    /// 更新 `hasher`。返回实际写入的字节数。严格要求 `source` 正好吐出 `chunk_size`
    /// 字节：多一个或少一个都算失败。
    async fn copy_and_hash(
        source: &mut ChunkReader,
        tmp_file: &mut File,
        hasher: &mut ChunkHasher,
        chunk_size: u64,
    ) -> NdnResult<()> {
        let mut remaining = chunk_size;
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let n = source
                .read(&mut buf[..want])
                .await
                .map_err(|e| NdnError::IoError(format!("read source failed: {}", e)))?;
            if n == 0 {
                return Err(NdnError::IoError(format!(
                    "source EOF before {} bytes: short {} bytes",
                    chunk_size, remaining
                )));
            }
            hasher.update_from_bytes(&buf[..n]);
            tmp_file
                .write_all(&buf[..n])
                .await
                .map_err(|e| NdnError::IoError(format!("write tmp file failed: {}", e)))?;
            remaining -= n as u64;
        }

        // 校验 source 确实只有 chunk_size 字节，多一个字节都不行。
        let mut overflow = [0u8; 1];
        let extra = source
            .read(&mut overflow)
            .await
            .map_err(|e| NdnError::IoError(format!("probe source tail failed: {}", e)))?;
        if extra != 0 {
            return Err(NdnError::IoError(format!(
                "source longer than declared chunk_size {}",
                chunk_size
            )));
        }

        tmp_file
            .flush()
            .await
            .map_err(|e| NdnError::IoError(format!("flush tmp file failed: {}", e)))?;
        tmp_file
            .sync_all()
            .await
            .map_err(|e| NdnError::IoError(format!("sync tmp file failed: {}", e)))?;
        Ok(())
    }

    fn verify_hash(hasher: ChunkHasher, chunk_id: &ChunkId) -> NdnResult<()> {
        let got = if chunk_id.chunk_type.is_mix() {
            hasher.finalize_mix_chunk_id()?
        } else {
            hasher.finalize_chunk_id()
        };
        if got != *chunk_id {
            return Err(NdnError::VerifyError(format!(
                "chunk hash mismatch: expected {} got {}",
                chunk_id.to_string(),
                got.to_string()
            )));
        }
        Ok(())
    }

    /// 静默地删除 tmp 文件，错误只打日志不上抛 —— 这是清理路径。
    async fn cleanup_tmp(tmp_path: &Path) {
        if let Err(e) = fs::remove_file(tmp_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "LocalFsBackend: remove tmp failed: {} ({})",
                    tmp_path.display(),
                    e
                );
            }
        }
    }
}

#[async_trait]
impl NamedDataStoreBackend for LocalFsBackend {
    // ----- Object -----

    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }
        let path = self.object_path(obj_id);
        match fs::read_to_string(&path).await {
            Ok(s) => Ok(s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(NdnError::NotFound(obj_id.to_string()))
            }
            Err(e) => Err(NdnError::IoError(format!(
                "read object {} failed: {}",
                obj_id.to_string(),
                e
            ))),
        }
    }

    async fn put_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<()> {
        self.ensure_writable()?;
        if obj_id.is_chunk() {
            return Err(NdnError::InvalidObjType(obj_id.to_string()));
        }

        let final_path = self.object_path(obj_id);
        Self::ensure_parent(&final_path).await?;

        // 和 chunk 一样用 tmp+rename 保证原子：其它 reader 绝不会看到半写的 object 文件。
        let tmp_path = final_path.with_extension(format!(
            "tmp.{}.{}",
            process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        {
            let mut f = File::create(&tmp_path)
                .await
                .map_err(|e| NdnError::IoError(format!("create object tmp failed: {}", e)))?;
            f.write_all(obj_str.as_bytes())
                .await
                .map_err(|e| NdnError::IoError(format!("write object tmp failed: {}", e)))?;
            f.flush().await.ok();
            f.sync_all()
                .await
                .map_err(|e| NdnError::IoError(format!("sync object tmp failed: {}", e)))?;
        }

        fs::rename(&tmp_path, &final_path).await.map_err(|e| {
            // rename 失败时尽量清理 tmp
            let tmp_path = tmp_path.clone();
            tokio::spawn(async move {
                let _ = fs::remove_file(&tmp_path).await;
            });
            NdnError::IoError(format!("rename object tmp failed: {}", e))
        })?;

        Ok(())
    }

    async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        self.ensure_writable()?;
        let path = self.object_path(obj_id);
        match fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(NdnError::IoError(format!(
                "remove object {} failed: {}",
                obj_id.to_string(),
                e
            ))),
        }
    }

    // ----- Chunk -----

    async fn get_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStateInfo> {
        let final_path = self.chunk_final_path(chunk_id);
        match fs::metadata(&final_path).await {
            Ok(meta) => Ok(ChunkStateInfo {
                presence: ChunkPresence::Completed,
                chunk_size: meta.len(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ChunkStateInfo::not_exist()),
            Err(e) => Err(NdnError::IoError(format!(
                "stat chunk {} failed: {}",
                chunk_id.to_string(),
                e
            ))),
        }
    }

    async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)> {
        let final_path = self.chunk_final_path(chunk_id);
        let mut file = match OpenOptions::new().read(true).open(&final_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(NdnError::NotFound(chunk_id.to_string()));
            }
            Err(e) => {
                return Err(NdnError::IoError(format!(
                    "open chunk {} failed: {}",
                    chunk_id.to_string(),
                    e
                )));
            }
        };

        let meta = file
            .metadata()
            .await
            .map_err(|e| NdnError::IoError(format!("stat chunk file failed: {}", e)))?;
        let total = meta.len();

        if offset > total {
            return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
        }
        if offset > 0 {
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| NdnError::IoError(format!("seek chunk file failed: {}", e)))?;
        }
        let limited = file.take(total - offset);
        Ok((Box::pin(limited), total))
    }

    async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        mut source: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome> {
        self.ensure_writable()?;

        let final_path = self.chunk_final_path(chunk_id);

        // 快速短路：已存在就不读 source。对应协议中 `PUT` 的 X-CYFS-Chunk-Already 语义。
        if fs::metadata(&final_path).await.is_ok() {
            return Ok(ChunkWriteOutcome::AlreadyExists);
        }

        Self::ensure_parent(&final_path).await?;
        let tmp_path = self.chunk_tmp_path(chunk_id);

        // 打开 tmp 文件。用 create_new 防止极端情况下撞上其它 writer 的 tmp（虽然名字已经足够唯一）。
        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .await
            .map_err(|e| {
                NdnError::IoError(format!(
                    "create chunk tmp {} failed: {}",
                    tmp_path.display(),
                    e
                ))
            })?;

        // 流式 copy + 增量 hash
        let hash_method = chunk_id.chunk_type.to_hash_method()?;
        let mut hasher = ChunkHasher::new_with_hash_method(hash_method)?;

        if let Err(e) =
            Self::copy_and_hash(&mut source, &mut tmp_file, &mut hasher, chunk_size).await
        {
            drop(tmp_file);
            Self::cleanup_tmp(&tmp_path).await;
            return Err(e);
        }
        drop(tmp_file);

        // hash 校验
        if let Err(e) = Self::verify_hash(hasher, chunk_id) {
            Self::cleanup_tmp(&tmp_path).await;
            return Err(e);
        }

        // 再次检查：写 hash 过程中可能别的 writer 已经把 final 放进去了。
        if fs::metadata(&final_path).await.is_ok() {
            Self::cleanup_tmp(&tmp_path).await;
            return Ok(ChunkWriteOutcome::AlreadyExists);
        }

        // 原子提交
        if let Err(e) = fs::rename(&tmp_path, &final_path).await {
            Self::cleanup_tmp(&tmp_path).await;
            return Err(NdnError::IoError(format!(
                "rename chunk tmp to final failed: {}",
                e
            )));
        }

        Ok(ChunkWriteOutcome::Written)
    }

    async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        self.ensure_writable()?;
        let final_path = self.chunk_final_path(chunk_id);
        if let Err(e) = fs::remove_file(&final_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(NdnError::IoError(format!(
                    "remove chunk {} failed: {}",
                    chunk_id.to_string(),
                    e
                )));
            }
        }
        Ok(())
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::NamedDataStoreBackendExt;
    use ndn_lib::{build_named_object_by_json, ChunkHasher};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    async fn new_backend() -> (TempDir, LocalFsBackend) {
        let dir = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(LocalFsBackendConfig {
            root: dir.path().to_path_buf(),
            read_only: false,
        })
        .await
        .unwrap();
        (dir, backend)
    }

    #[tokio::test]
    async fn put_get_chunk_roundtrip() {
        let (_dir, backend) = new_backend().await;
        let data = b"local fs backend hello".to_vec();
        let chunk_id = calc_chunk_id(&data);

        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );

        let outcome = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome, ChunkWriteOutcome::Written);

        let st = backend.get_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(st.presence, ChunkPresence::Completed);
        assert_eq!(st.chunk_size, data.len() as u64);

        let read_back = backend.get_chunk_data(&chunk_id).await.unwrap();
        assert_eq!(read_back, data);

        // Idempotent
        let outcome2 = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome2, ChunkWriteOutcome::AlreadyExists);
    }

    #[tokio::test]
    async fn chunk_reader_with_offset() {
        let (_dir, backend) = new_backend().await;
        let data: Vec<u8> = (0..200u8).collect();
        let chunk_id = calc_chunk_id(&data);
        backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();

        let (mut reader, total) = backend.open_chunk_reader(&chunk_id, 50).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, &data[50..]);

        // offset == total → OK，EOF 立刻触发
        let (mut reader, total) = backend
            .open_chunk_reader(&chunk_id, data.len() as u64)
            .await
            .unwrap();
        assert_eq!(total, data.len() as u64);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());

        // offset > total → OffsetTooLarge
        match backend
            .open_chunk_reader(&chunk_id, (data.len() as u64) + 1)
            .await
        {
            Err(NdnError::OffsetTooLarge(_)) => {}
            other => panic!("expected OffsetTooLarge, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn chunk_writer_size_mismatch_rolls_back() {
        let (_dir, backend) = new_backend().await;
        let data = b"abcdefghij".to_vec();
        let chunk_id = calc_chunk_id(&data);

        // 声明 100 字节但只给 10 字节 → 失败
        let cursor = std::io::Cursor::new(data.clone());
        let reader: ChunkReader = Box::pin(cursor);
        let err = match backend.open_chunk_writer(&chunk_id, 100, reader).await {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(err, NdnError::IoError(_)));
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );

        // 目录下不应残留任何 tmp 文件
        let prefix = &chunk_id.to_obj_id().to_filename()[0..2];
        let mut entries = fs::read_dir(backend.chunk_dir().join(prefix))
            .await
            .unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().into_string().unwrap();
            assert!(!name.contains(".tmp."), "tmp file leaked: {}", name);
        }
    }

    #[tokio::test]
    async fn chunk_writer_hash_mismatch_rolls_back() {
        let (_dir, backend) = new_backend().await;
        let real_data = b"real payload".to_vec();
        let wrong_data = b"fake payload".to_vec();
        let chunk_id = calc_chunk_id(&real_data);

        let cursor = std::io::Cursor::new(wrong_data.clone());
        let reader: ChunkReader = Box::pin(cursor);
        let err = match backend
            .open_chunk_writer(&chunk_id, wrong_data.len() as u64, reader)
            .await
        {
            Ok(_) => panic!("expected verify error"),
            Err(e) => e,
        };
        assert!(matches!(err, NdnError::VerifyError(_)));
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );
    }

    #[tokio::test]
    async fn chunk_writer_source_too_long_fails() {
        let (_dir, backend) = new_backend().await;
        let data = b"abcdefghij".to_vec();
        let chunk_id = calc_chunk_id(&data);
        let longer = b"abcdefghijEXTRA".to_vec();

        let cursor = std::io::Cursor::new(longer);
        let reader: ChunkReader = Box::pin(cursor);
        let err = match backend
            .open_chunk_writer(&chunk_id, data.len() as u64, reader)
            .await
        {
            Ok(_) => panic!("expected io error"),
            Err(e) => e,
        };
        assert!(matches!(err, NdnError::IoError(_)));
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );
    }

    #[tokio::test]
    async fn object_roundtrip() {
        let (_dir, backend) = new_backend().await;
        let (obj_id, obj_str) = build_named_object_by_json(
            "non-chunk",
            &json!({
                "name": "hello",
                "kind": "test"
            }),
        );
        // get before put
        match backend.get_object(&obj_id).await {
            Err(NdnError::NotFound(_)) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }

        backend.put_object(&obj_id, &obj_str).await.unwrap();
        let got = backend.get_object(&obj_id).await.unwrap();
        assert_eq!(got, obj_str);

        // idempotent
        backend.put_object(&obj_id, &obj_str).await.unwrap();

        backend.remove_object(&obj_id).await.unwrap();
        match backend.get_object(&obj_id).await {
            Err(NdnError::NotFound(_)) => {}
            other => panic!("expected NotFound after remove, got {:?}", other),
        }
        // idempotent remove
        backend.remove_object(&obj_id).await.unwrap();
    }

    #[tokio::test]
    async fn read_only_backend_rejects_writes() {
        let dir = TempDir::new().unwrap();
        // 先用可写后端造一点内容
        {
            let b = LocalFsBackend::new(LocalFsBackendConfig {
                root: dir.path().to_path_buf(),
                read_only: false,
            })
            .await
            .unwrap();
            let data = b"existing".to_vec();
            let cid = calc_chunk_id(&data);
            b.put_chunk_bytes(&cid, data).await.unwrap();
        }

        let backend = LocalFsBackend::new(LocalFsBackendConfig {
            root: dir.path().to_path_buf(),
            read_only: true,
        })
        .await
        .unwrap();

        let data = b"new".to_vec();
        let cid = calc_chunk_id(&data);
        let err = match backend.put_chunk_bytes(&cid, data).await {
            Ok(_) => panic!("expected permission denied"),
            Err(e) => e,
        };
        assert!(matches!(err, NdnError::PermissionDenied(_)));
        assert!(backend.is_read_only());
    }
}
