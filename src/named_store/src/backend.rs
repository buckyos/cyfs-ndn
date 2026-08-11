//! `NamedDataStoreBackend` —— `named-data-http-store` 协议的抽象层。
//!
//! 参考 `doc/named-data-http-store-protocol.md`。
//!
//! 这个 trait 是未来 `NamedLocalStore` / `NamedRemoteStore` 共用的存储后端接口：
//! - 本地实现（`LocalFsBackend`，待实现）直接走文件系统 + 元数据 DB；
//! - 远程实现（`HttpBackend`，待实现）按协议发起 HTTP 请求；
//! - 调用方只需要持有一个 `Arc<dyn NamedDataStoreBackend>`。
//!
//! 与 `local_store.rs` 原有接口相比，本 trait 做了两项关键简化：
//! 1. `open_chunk_writer` 把"返回一个 ChunkWriter + progress"改成"传入一个 ChunkReader 一次写完"。
//!    调用方如果手里只有一个 writer-style 的数据源，可以用 `tokio::io::duplex` 把它转成 reader。
//! 2. 对外暴露的 chunk 状态只有 `NotExist` 与 `Completed` 两种。`Incompleted`/`progress`
//!    退化为实现细节：写入过程中本地后端可以用 tmp 文件，但外部观察者看不到中间态。

use async_trait::async_trait;
use ndn_lib::{ChunkId, ChunkReader, NdnError, NdnResult, ObjId};

/// 对外可见的 chunk 状态。协议语义下只有两态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkPresence {
    /// Chunk 不存在（包括"正在写入但尚未 commit"的内部状态）。
    NotExist,
    /// Chunk 已完整落盘、hash 校验通过、可供读取。
    Completed,
}

impl ChunkPresence {
    pub fn exists(self) -> bool {
        matches!(self, ChunkPresence::Completed)
    }
}

/// `get_chunk_state` 的返回值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkStateInfo {
    pub presence: ChunkPresence,
    /// `Completed` 时为 chunk 字节数；`NotExist` 时为 0。
    pub chunk_size: u64,
}

impl ChunkStateInfo {
    pub fn not_exist() -> Self {
        Self {
            presence: ChunkPresence::NotExist,
            chunk_size: 0,
        }
    }

    pub fn completed(chunk_size: u64) -> Self {
        Self {
            presence: ChunkPresence::Completed,
            chunk_size,
        }
    }
}

/// `open_chunk_writer` 的返回值：告诉调用方后端实际有没有吞下这次写入。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkWriteOutcome {
    /// 本次写入把 chunk 从 `NotExist` 搬到了 `Completed`。
    Written,
    /// 调用前 chunk 就已经是 `Completed`；后端直接复用，没有消费 `source`。
    AlreadyExists,
}

/// `named-data-http-store` 协议的后端抽象。
///
/// 所有实现必须满足以下一致性约束：
///
/// * **两态可见性**：`get_chunk_state` 在任意时刻只会返回 `NotExist` 或 `Completed`，
///   绝不会暴露"正在写入"的中间态。
/// * **原子提交**：`open_chunk_writer` 成功返回 `Written` 当且仅当 chunk 整体可见且 hash 正确；
///   任何失败分支（传输中断、校验失败、IO 错误等）必须把 chunk 还原到 `NotExist`。
/// * **幂等**：`put_object` 与 `open_chunk_writer` 对同一 `obj_id`/`chunk_id` 可以重复调用；
///   对 chunk 的重复写入必须返回 `AlreadyExists` 而非报错。
/// * **并发安全**：同一 chunk 的两个并发 writer 必须串行化最终可见性，任何一方成功后，
///   另一方要么返回 `AlreadyExists`，要么返回 `Written`（取决于谁先落地），而不是数据损坏。
#[async_trait]
pub trait NamedDataStoreBackend: Send + Sync {
    // ---------- Object ----------

    /// 直接寻址获取 object 内容。返回 `String` 是为了兼容 JWT。
    ///
    /// `obj_id` 必须是非 chunk 类型，否则返回 `NdnError::InvalidObjType`。
    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String>;

    /// 幂等写入一个 object。
    ///
    /// `obj_id` 必须是非 chunk 类型。若后端能校验 `obj_id` 与 `obj_str` 的一致性，
    /// 校验失败时应返回 `NdnError::VerifyError`。
    async fn put_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<()>;

    // ---------- Chunk ----------

    /// 查询 chunk 状态。永远不会返回 `NdnError::NotFound`：不存在直接返回
    /// `ChunkStateInfo::not_exist()`。
    async fn get_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStateInfo>;

    /// 从 `offset` 处打开 chunk 的只读流。
    ///
    /// 返回 `(reader, total_chunk_size)`。`total_chunk_size` 是 chunk 的总字节数，
    /// 而不是 reader 剩余字节数；reader 自身在读完 `total_chunk_size - offset` 字节后 EOF。
    ///
    /// 错误：
    /// - `NdnError::NotFound` —— chunk 不存在；
    /// - `NdnError::OffsetTooLarge` —— `offset > total_chunk_size`；
    /// - 其他 IO 错误照常向上透传。
    async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64)>;

    /// 一次性写入 chunk。调用方负责提供能产出恰好 `chunk_size` 字节的 `source`。
    ///
    /// 语义：
    /// - 后端从 `source` 读满 `chunk_size` 字节，做 hash 校验，并原子地让它对外可见；
    /// - 成功返回 `ChunkWriteOutcome::Written`；
    /// - 若调用前 chunk 已经 `Completed`，后端**不**消费 `source`，直接返回
    ///   `ChunkWriteOutcome::AlreadyExists`；
    /// - 失败必须保证 chunk 回到 `NotExist`。
    ///
    /// 不支持断点续传。想要分片/重试的调用方应在更高层重算 `chunk_id` 并重试整次写入。
    async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        source: ChunkReader,
    ) -> NdnResult<ChunkWriteOutcome>;

    // ---------- 可选但实用 ----------

    /// 删除 object。幂等：不存在时也返回 `Ok(())`。
    async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()>;

    /// 删除 chunk。幂等：不存在时也返回 `Ok(())`。
    async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()>;

    /// 后端是否只读。只读后端在任何写操作上都会返回 `NdnError::PermissionDenied`。
    fn is_read_only(&self) -> bool {
        false
    }
}

// ---------- 便捷方法 ----------

/// `NamedDataStoreBackend` 的便利扩展：把常见的 "读全部 / 写字节串" 操作封装成一行。
///
/// 这些方法对 trait 对象自动生效（`dyn NamedDataStoreBackend`），无需各后端重复实现。
#[async_trait]
pub trait NamedDataStoreBackendExt: NamedDataStoreBackend {
    /// 读取整个 chunk 到内存。仅建议对小 chunk 使用。
    async fn get_chunk_data(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
        use tokio::io::AsyncReadExt;

        let (mut reader, total) = self.open_chunk_reader(chunk_id, 0).await?;
        let mut buf = Vec::with_capacity(total as usize);
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| ndn_lib::NdnError::IoError(e.to_string()))?;
        Ok(buf)
    }

    /// 用一段内存中的字节写入 chunk。内部包装成一个 reader 后调用 `open_chunk_writer`。
    async fn put_chunk_bytes(
        &self,
        chunk_id: &ChunkId,
        data: Vec<u8>,
    ) -> NdnResult<ChunkWriteOutcome> {
        let chunk_size = data.len() as u64;
        let cursor = std::io::Cursor::new(data);
        let reader: ChunkReader = Box::pin(cursor);
        self.open_chunk_writer(chunk_id, chunk_size, reader).await
    }

    /// 查询 chunk 是否存在（相当于 `get_chunk_state(...).presence.exists()`）。
    async fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool> {
        Ok(self.get_chunk_state(chunk_id).await?.presence.exists())
    }
}

impl<T: NamedDataStoreBackend + ?Sized> NamedDataStoreBackendExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use ndn_lib::NdnError;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::io::AsyncReadExt;

    /// 一个纯内存的后端，用来验证 trait 自身的约束是否自洽、便捷方法是否工作。
    /// 它**不是**生产后端，生产后端要依靠 `LocalFsBackend` / `HttpBackend`。
    struct MemBackend {
        objects: Mutex<HashMap<String, String>>,
        chunks: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MemBackend {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
                chunks: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl NamedDataStoreBackend for MemBackend {
        async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String> {
            if obj_id.is_chunk() {
                return Err(NdnError::InvalidObjType(obj_id.to_string()));
            }
            self.objects
                .lock()
                .unwrap()
                .get(&obj_id.to_string())
                .cloned()
                .ok_or_else(|| NdnError::NotFound(obj_id.to_string()))
        }

        async fn put_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<()> {
            if obj_id.is_chunk() {
                return Err(NdnError::InvalidObjType(obj_id.to_string()));
            }
            self.objects
                .lock()
                .unwrap()
                .insert(obj_id.to_string(), obj_str.to_string());
            Ok(())
        }

        async fn get_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStateInfo> {
            match self.chunks.lock().unwrap().get(&chunk_id.to_string()) {
                Some(data) => Ok(ChunkStateInfo::completed(data.len() as u64)),
                None => Ok(ChunkStateInfo::not_exist()),
            }
        }

        async fn open_chunk_reader(
            &self,
            chunk_id: &ChunkId,
            offset: u64,
        ) -> NdnResult<(ChunkReader, u64)> {
            let data = self
                .chunks
                .lock()
                .unwrap()
                .get(&chunk_id.to_string())
                .cloned()
                .ok_or_else(|| NdnError::NotFound(chunk_id.to_string()))?;
            let total = data.len() as u64;
            if offset > total {
                return Err(NdnError::OffsetTooLarge(chunk_id.to_string()));
            }
            let sliced = data[offset as usize..].to_vec();
            let reader: ChunkReader = Box::pin(std::io::Cursor::new(sliced));
            Ok((reader, total))
        }

        async fn open_chunk_writer(
            &self,
            chunk_id: &ChunkId,
            chunk_size: u64,
            mut source: ChunkReader,
        ) -> NdnResult<ChunkWriteOutcome> {
            // 幂等：已存在直接短路，不读 source。
            if self
                .chunks
                .lock()
                .unwrap()
                .contains_key(&chunk_id.to_string())
            {
                return Ok(ChunkWriteOutcome::AlreadyExists);
            }

            // 一次性读满 chunk_size 字节；不够或多余都算失败。
            let mut buf = Vec::with_capacity(chunk_size as usize);
            let read_bytes = source
                .read_to_end(&mut buf)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
            if read_bytes as u64 != chunk_size {
                return Err(NdnError::IoError(format!(
                    "chunk {} size mismatch: expected {} got {}",
                    chunk_id.to_string(),
                    chunk_size,
                    read_bytes
                )));
            }

            // 原子可见：拿到锁之后再插入，二次检查幂等。
            let mut chunks = self.chunks.lock().unwrap();
            if chunks.contains_key(&chunk_id.to_string()) {
                return Ok(ChunkWriteOutcome::AlreadyExists);
            }
            chunks.insert(chunk_id.to_string(), buf);
            Ok(ChunkWriteOutcome::Written)
        }

        async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
            self.objects.lock().unwrap().remove(&obj_id.to_string());
            Ok(())
        }

        async fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
            self.chunks.lock().unwrap().remove(&chunk_id.to_string());
            Ok(())
        }
    }

    fn calc_chunk_id(data: &[u8]) -> ChunkId {
        ndn_lib::ChunkHasher::new(None)
            .unwrap()
            .calc_mix_chunk_id_from_bytes(data)
            .unwrap()
    }

    #[tokio::test]
    async fn mem_backend_chunk_roundtrip() {
        let backend = MemBackend::new();
        let data = b"hello named-data-http-store".to_vec();
        let chunk_id = calc_chunk_id(&data);

        // NotExist 初态
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );

        // 首次写入 → Written
        let outcome = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome, ChunkWriteOutcome::Written);

        // 状态变成 Completed
        let st = backend.get_chunk_state(&chunk_id).await.unwrap();
        assert_eq!(st.presence, ChunkPresence::Completed);
        assert_eq!(st.chunk_size, data.len() as u64);

        // 再次写入 → AlreadyExists（幂等）
        let outcome2 = backend
            .put_chunk_bytes(&chunk_id, data.clone())
            .await
            .unwrap();
        assert_eq!(outcome2, ChunkWriteOutcome::AlreadyExists);

        // 全量读回
        let read_back = backend.get_chunk_data(&chunk_id).await.unwrap();
        assert_eq!(read_back, data);

        // 带 offset 的读
        let (mut reader, total) = backend.open_chunk_reader(&chunk_id, 6).await.unwrap();
        assert_eq!(total, data.len() as u64);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, &data[6..]);

        // offset 超界
        let err = match backend
            .open_chunk_reader(&chunk_id, (data.len() as u64) + 1)
            .await
        {
            Ok(_) => panic!("expected OffsetTooLarge"),
            Err(e) => e,
        };
        assert!(matches!(err, NdnError::OffsetTooLarge(_)));

        // 删除后回到 NotExist
        backend.remove_chunk(&chunk_id).await.unwrap();
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );
        // 重复删除依然幂等
        backend.remove_chunk(&chunk_id).await.unwrap();
    }

    #[tokio::test]
    async fn mem_backend_writer_size_mismatch_rolls_back() {
        let backend = MemBackend::new();
        let data = b"abcdef".to_vec();
        let chunk_id = calc_chunk_id(&data);

        // 声明 chunk_size=10，但 source 只有 6 字节 → 失败
        let cursor = std::io::Cursor::new(data.clone());
        let reader: ChunkReader = Box::pin(cursor);
        let err = backend
            .open_chunk_writer(&chunk_id, 10, reader)
            .await
            .unwrap_err();
        assert!(matches!(err, NdnError::IoError(_)));

        // 失败后 chunk 必须仍是 NotExist
        assert_eq!(
            backend.get_chunk_state(&chunk_id).await.unwrap().presence,
            ChunkPresence::NotExist
        );
    }
}
