//! named_data_mgr_v2.rs
//!
//! 目标：在保留旧版 NamedDataMgr “对象库 + chunk store + 一部分 path 操作”的可用性基础上，
//! 将行为严格对齐文档契约：
//! - Path 语义非 POSIX（由 FsMetaDb 定义）
//! - Strict Single Writer（由 FsMetaDb lease/fence 保证）
//! - 内容寻址：Chunk/Object 不可变，写入通过 .tmp -> .final 原子提交
//! - Placement：epoch backoff <= 2 + lazy migration
//! - Link：SameAs / PartOf / ExternalFile(qcid)；Reader 允许走 link，Writer 永远只写 internal
//! - erase_obj_by_id：驱逐本地物化数据（不删 namespace 绑定）


use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    path::{PathBuf},
    pin::Pin,
    sync::{Arc, RwLock},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncSeek, SeekFrom};

/// =======================
/// 1) 基础类型与错误
/// =======================

pub type NdnResult<T> = Result<T, NdnError>;

#[derive(Debug, Clone)]
pub enum NdnError {
    NotFound(String),

    /// 文档语义：对象元信息/绑定可能存在，但 body/chunk 未物化，需要 pull
    NeedPull(String),

    /// Strict Single Writer 冲突（path 已被他人写会话占用）
    PathBusy(String),

    /// 租约/栅栏冲突（同 session 续写失败或 fence 落后）
    LeaseConflict(String),

    /// Link 指向的外部文件被篡改或不满足 qcid/校验要求
    LinkInvalid(String),

    InvalidParam(String),
    InvalidData(String),

    AlreadyExists(String),
    OffsetTooLarge(String),

    IoError(String),
    DbError(String),

    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NdmId(pub String);

/// 一个 NamedDataMgr 实例的命名空间隔离单元：ndm://pool/ndm_id/...
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NdmInstanceId {
    pub pool: PoolId,
    pub ndm: NdmId,
}

/// 对外暴露的“逻辑路径”（避免与 std::path::Path 混淆）
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NdmPath(pub String);

/// 典型 inner_path：actions/MI6/content/4
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InnerPath(pub Vec<String>);

impl InnerPath {
    pub fn parse(s: &str) -> Self {
        // 伪代码：
        // - split('/')
        // - 过滤空段
        // - 返回 Vec<String>
        todo!()
    }
}

/// 这些 ObjId/ChunkId 假定来自 cyfs 体系；这里做最小占位
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkId(pub String);

/// Reader/Writer 抽象：尽量兼容旧版 open_chunk_reader/open_chunk_writer 的 async 语义
pub type Reader = Pin<Box<dyn AsyncRead + AsyncSeek + Send + Sync>>;
pub type Writer = Pin<Box<dyn AsyncWrite + Send + Sync>>;


/// =======================
/// 2) Stat / 状态枚举
/// =======================

#[derive(Debug, Clone)]
pub enum MaterialState {
    /// 仅有元信息（或仅有 path->objid 绑定），本地未物化
    NotPulled,

    /// 部分物化（对象体有，但引用 chunk 缺；或 chunk 部分缺）
    PartiallyPulled {
        missing_chunks: Vec<ChunkId>,
    },

    /// 已完全物化（对象体 + 引用 chunk 满足读）
    Materialized,
}

#[derive(Debug, Clone)]
pub enum PathState {
    NotFound,

    /// 已提交并绑定到某个对象（内容可更新：通过重新绑定新的 objid 实现 COW）
    Committed {
        obj_id: ObjId,
        material: MaterialState,
    },

    /// 写入中/分级提交中（Strict Single Writer）
    Working {
        session: SessionId,
        fence: FenceToken,
        buffer: FileBufferHandle,
        stage: BufferStage,
    },
}

#[derive(Debug, Clone)]
pub struct PathStat {
    pub path: NdmPath,
    pub state: PathState,
    pub meta: HashMap<String, String>, // 可选：PathMetaObject / 自定义 meta
}

#[derive(Debug, Clone)]
pub struct ObjStat {
    pub obj_id: ObjId,
    pub material: MaterialState,
    pub size_hint: Option<u64>,
    pub last_access_time: Option<u64>,
}

/// =======================
/// 3) Lease / Session（Strict Single Writer）
/// =======================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FenceToken(pub u64);

#[derive(Debug, Clone)]
pub struct WriteLease {
    pub session: SessionId,
    pub fence: FenceToken,
    pub expires_at: u64, // unix ts
}

/// =======================
/// 4) Link / Chunk 状态（Reader 走 link，Writer 只写 internal）
/// =======================

#[derive(Debug, Clone)]
pub enum LinkType {
    /// chunk 的别名（去重/重定向）
    SameAs(ChunkId),

    /// chunk 的切片引用：逻辑 chunk = base[range]
    PartOf { base: ChunkId, range: Range<u64> },

    /// 外部文件引用（“原地纳管”）：读前 qcid 快检；可选升级 sha256 强校验
    ExternalFile {
        path: PathBuf,
        range: Option<Range<u64>>,
        qcid: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum InternalChunkState {
    Absent,

    /// 写入中（只针对 internal tmp），不算“存在”
    Writing {
        tmp_path: PathBuf,
        expect_size: Option<u64>,
        written: u64,
    },

    /// 写入完成且已 .final
    Complete {
        chunk_size: u64,
        /// 记录 chunk 当前已落在哪些 target（可选：多副本）
        locations: Vec<ChunkLocation>,
    },
}

#[derive(Debug, Clone)]
pub struct ChunkLocation {
    pub store_id: StoreId,
    pub epoch: u64,
    pub relative_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreId(pub String);

#[derive(Debug, Clone)]
pub struct ChunkStat {
    pub chunk_id: ChunkId,
    pub internal: InternalChunkState,
    pub link: Option<LinkType>,
    pub progress: Option<String>, // 兼容旧版 progress 字段（断点续写/同步进度）
}

/// =======================
/// 5) Placement / Layout（epoch + backoff <= 2）
/// =======================

pub const MAX_EPOCH_BACKOFF: u64 = 2;

#[derive(Debug, Clone)]
pub enum TargetStatus {
    Active,
    ReadOnly,
    Offline,
}

#[derive(Debug, Clone)]
pub struct StoreTarget {
    pub store_id: StoreId,
    pub base_path: PathBuf,
    pub capacity: Option<u64>,
    pub status: TargetStatus,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct StoreLayout {
    pub epoch: u64,
    pub targets: Vec<StoreTarget>,

    // 伪代码：一致性哈希/CRUSH ring 等 placement 结构
    // pub ring: HashRing,
}

impl StoreLayout {
    pub fn select_targets(&self, chunk_id: &ChunkId, epoch: u64) -> Vec<StoreTarget> {
        // 伪代码：
        // - 用 epoch 对 ring 做确定性映射
        // - 选择 1..N 个 target（按可靠性策略/副本数/纠删码策略）
        // - 过滤 Offline/ReadOnly（写路径只选 Active；读路径可选 ReadOnly）
        todo!()
    }
}

/// =======================
/// 6) Pull / 远端上下文（异步 materialization）
/// =======================

#[derive(Debug, Clone)]
pub struct PullContext {
    pub source_url: String,        // e.g. cyfs://... 或 http(s)://...
    pub verify: bool,              // 是否开启校验
    pub timeout_ms: Option<u64>,
    pub headers: HashMap<String, String>,
}


/// =======================
/// 8) 依赖抽象：FsMetaDb / NamedStore / FileBufferService / NdnFetcher
/// =======================

/// Path 层：定义非 POSIX 的 namespace 事务、挂载互斥、Strict Single Writer lease
pub trait FsMetaDb: Send + Sync {
    fn check_mount_conflict(&self, path: &NdmPath) -> NdnResult<()>;

    fn stat(&self, path: &NdmPath) -> NdnResult<PathStat>;
    fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat>;

    /// O(1) 绑定：只写元数据，不代表本地已 pull
    fn add_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()>;
    fn add_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()>;

    fn delete(&self, path: &NdmPath) -> NdnResult<()>;
    fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()>;
    fn copy_path(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()>;

    fn list(&self, path: &NdmPath, pos: u32, page_size: u32) -> NdnResult<Vec<(String, PathStat)>>;

    /// 获取 committed objid（若 path 非 committed，返回错误）
    fn get_committed_objid(&self, path: &NdmPath) -> NdnResult<ObjId>;

    /// Strict Single Writer：申请写租约（事务）
    fn acquire_write_lease(&self, path: &NdmPath, session: &SessionId) -> NdnResult<WriteLease>;

    /// 更新 working 状态（例如写入中 -> cooling / linked / finalized）
    fn update_working_state(&self, path: &NdmPath, lease: &WriteLease, stage: BufferStage, fb: &FileBufferHandle) -> NdnResult<()>;

    /// 提交：Working -> Committed(objid) 并释放 lease（事务）
    fn commit_path(&self, path: &NdmPath, lease: &WriteLease, obj_id: ObjId) -> NdnResult<()>;

    /// 放弃写会话（事务）：释放 lease，必要时清理 working 记录
    fn abort_write(&self, path: &NdmPath, lease: &WriteLease) -> NdnResult<()>;
}

/// 内容层：对象库 + chunk store + link + placement
pub trait NamedStore: Send + Sync {
    /// ---- Layout/Admin ----
    fn get_layout(&self) -> StoreLayout;
    fn update_layout(&self, new_layout: StoreLayout) -> NdnResult<()>;

    /// ---- Object ----
    fn put_object(&self, obj_id: &ObjId, obj_bytes: &[u8]) -> NdnResult<()>;
    fn get_object(&self, obj_id: &ObjId) -> NdnResult<Vec<u8>>;

    /// 对象是否“可读”（body + 所需 chunk 是否具备）
    fn stat_object_material(&self, obj_id: &ObjId) -> NdnResult<MaterialState>;

    /// 物理驱逐：删除本地 object body 与关联 chunks（但不删 path 绑定）
    fn erase_object_data(&self, obj_id: &ObjId) -> NdnResult<()>;

    /// ---- Chunk ----
    fn query_chunk(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStat>;
    fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool>;

    fn open_chunk_reader(&self, chunk_id: &ChunkId, offset: u64, opts: &ReadOptions) -> NdnResult<(Reader, u64)>;

    fn open_chunk_writer(&self, chunk_id: &ChunkId, expect_size: Option<u64>, offset: u64) -> NdnResult<(Writer, ChunkWriteTicket)>;

    fn update_chunk_progress(&self, chunk_id: &ChunkId, progress: String) -> NdnResult<()>;
    fn complete_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()>;
    fn abort_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()>;

    /// ---- Link ----
    fn set_chunk_link(&self, chunk_id: &ChunkId, link: LinkType) -> NdnResult<()>;
    fn get_chunk_link(&self, chunk_id: &ChunkId) -> NdnResult<Option<LinkType>>;

    /// 反查：哪些 chunk 引用某个 external file（运维/清理/诊断）
    fn list_chunks_by_external_file(&self, file_path: &PathBuf) -> NdnResult<Vec<ChunkId>>;

    /// ---- 高层 helper ----
    fn open_chunklist_reader(&self, chunklist_obj_id: &ObjId, seek_from: SeekFrom, opts: &ReadOptions) -> NdnResult<(Reader, u64)>;

    /// 从 FileBuffer 导入为对象（切 chunk、hash、写入 store、构造 FileObject/ChunkList 等）
    fn import_buffer_as_object(&self, fb: &FileBufferHandle, policy: CommitPolicy) -> NdnResult<ImportResult>;
}

/// buffer 服务：单机 mmap / 多 BufferNode(GFS 模型) 都可落到这里
pub trait FileBufferService: Send + Sync {
    fn create_buffer(&self, path: &NdmPath, lease: &WriteLease, expected_size: Option<u64>) -> NdnResult<FileBufferHandle>;

    fn append(&self, fb: &FileBufferHandle, data: &[u8]) -> NdnResult<()>;
    fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()>;
    fn close(&self, fb: &FileBufferHandle) -> NdnResult<()>;

    fn open_reader(&self, fb: &FileBufferHandle, seek_from: SeekFrom) -> NdnResult<Reader>;

    /// Staged 模式：让 buffer node 计算 hash（避免把数据搬回本地再算）
    fn calc_obj_id(&self, fb: &FileBufferHandle) -> NdnResult<ObjId>;

    /// Finalize：把数据从 buffer node 推到 NamedStore internal（IO 密集型）
    fn push_to_store(&self, fb: &FileBufferHandle, store: &dyn NamedStore) -> NdnResult<()>;

    fn remove(&self, fb: &FileBufferHandle) -> NdnResult<()>;
}

/// 拉取器：从远端把对象/块 materialize 到本地 store
pub trait NdnFetcher: Send + Sync {
    fn schedule_pull_obj(&self, store: Arc<dyn NamedStore>, obj_id: ObjId, ctx: PullContext) -> NdnResult<()>;
    fn schedule_pull_chunk(&self, store: Arc<dyn NamedStore>, chunk_id: ChunkId, ctx: PullContext) -> NdnResult<()>;
}

/// Reader 行为选项
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub auto_cache: bool,
    pub max_link_depth: usize,

    /// ExternalFile 是否允许读取
    pub allow_external: bool,

    /// 是否在 ExternalFile 上做 qcid 快检/或升级 sha256 校验
    pub external_verify: ExternalVerifyMode,
}

#[derive(Debug, Clone)]
pub enum ExternalVerifyMode {
    None,
    QcidOnly,
    QcidThenSha256, // 慢但最安全
}

/// import 结果（把 buffer 对象化后的产物）
#[derive(Debug, Clone)]
pub struct ImportResult {
    pub obj_id: ObjId,
    pub referenced_chunks: Vec<ChunkId>,

    /// staged 模式下可能先产出 ExternalLink（warm），后续再 promote 到 internal（cold）
    pub staged: bool,
}

#[derive(Debug, Clone)]
pub struct ChunkWriteTicket {
    pub chunk_id: ChunkId,
    pub tmp_paths: Vec<PathBuf>, // 可能多 target
    pub expect_size: Option<u64>,
    pub epoch: u64,
}


/// =======================
/// 9) NamedDataMgr：对外统一入口（融合旧版风格）
/// =======================

pub struct NamedDataMgr {
    pub instance: NdmInstanceId,

    meta_db: Arc<dyn FsMetaDb>,
    store: Arc<dyn NamedStore>,
    buffer: Arc<dyn FileBufferService>,
    fetcher: Option<Arc<dyn NdnFetcher>>,

    /// 默认 commit 策略（可配置：Immediate / Staged）
    default_commit_policy: CommitPolicy,

    /// 用于后台任务：staged 状态机推进、scrubber、lazy migration 等
    bg: Arc<tokio::sync::Mutex<BackgroundMgr>>,
}

impl NamedDataMgr {
    pub fn new(
        instance: NdmInstanceId,
        meta_db: Arc<dyn FsMetaDb>,
        store: Arc<dyn NamedStore>,
        buffer: Arc<dyn FileBufferService>,
        fetcher: Option<Arc<dyn NdnFetcher>>,
        default_commit_policy: CommitPolicy,
    ) -> Self {
        // 伪代码：保存依赖；初始化后台管理器
        todo!()
    }

    /// ========== 9.1 Path/Namespace 接口（非 POSIX） ==========

    pub fn stat(&self, path: &NdmPath) -> NdnResult<PathStat> {
        // 伪代码：直接委托 meta_db.stat
        todo!()
    }

    pub fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat> {
        // 伪代码：
        // - meta_db.stat_by_objid（namespace 维度）可能只知道绑定/元信息
        // - store.stat_object_material 负责判断本地是否 materialized
        todo!()
    }

    pub fn add_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()> {
        // 伪代码：
        // 1) meta_db.check_mount_conflict(path)
        // 2) meta_db.add_file(path, obj_id)
        // 注意：O(1) 绑定，不保证已 pull
        todo!()
    }

    pub fn add_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()> {
        // 同 add_file
        todo!()
    }

    pub fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        // 伪代码：仅删除 path 绑定，不触发业务数据删除
        todo!()
    }

    pub fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()> {
        // 伪代码：namespace 事务，O(1) 改指针
        todo!()
    }

    pub fn copy_path(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        // 伪代码：复制绑定（语义上“快照”），仍是 O(1)
        todo!()
    }

    pub fn list(&self, path: &NdmPath, pos: u32, page_size: u32) -> NdnResult<Vec<(String, PathStat)>> {
        // 伪代码：委托 meta_db.list
        todo!()
    }

    /// ========== 9.2 读接口：open_reader / open_reader_by_id ==========

    pub async fn open_reader(&self, path: &NdmPath, opts: ReadOptions) -> NdnResult<(Reader, u64)> {
        // 伪代码：
        // 1) stat(path)
        // 2) 若 Working：默认返回 PathBusy（可选：同 session 允许读 buffer）
        // 3) 若 Committed(objid)：
        //    - 先判断 obj material 状态：store.stat_object_material
        //    - 若 NotPulled/Partial：返回 NeedPull
        //    - 否则 open_reader_by_id(objid, None, opts)
        todo!()
    }

    pub async fn open_reader_by_id(
        &self,
        obj_id: &ObjId,
        inner_path: Option<&InnerPath>,
        opts: ReadOptions,
    ) -> NdnResult<(Reader, u64)> {
        // 伪代码（多级 inner_path 解析）：
        // 1) 从 store.get_object(obj_id) 取对象体（jwt/json/bytes）
        // 2) 若对象体缺失：NeedPull
        // 3) 解析对象类型：
        //    - DirObject：inner_path 第一段作为 child name，递归拿 child objid
        //    - FileObject：inner_path 若走到 content/idx：
        //        a) content -> chunklist objid
        //        b) open_chunklist_reader(chunklist, seek_from, opts)
        //        c) 若还要 content/<idx> 精确 chunk：定位 chunk_id 再 open_chunk_reader
        // 4) 返回 Reader + size（能算则算，否则 size_hint）
        todo!()
    }

    pub fn get_object(&self, obj_id: &ObjId) -> NdnResult<Vec<u8>> {
        // 伪代码：store.get_object
        todo!()
    }

    /// ========== 9.3 写接口：Strict Single Writer（FileBuffer） ==========

    pub fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        // 伪代码：
        // - 构造空 DirObject
        // - store.put_object(dir_obj_id, dir_obj_bytes)
        // - meta_db.add_dir(path, dir_obj_id)
        todo!()
    }

    pub fn create_file(&self, path: &NdmPath, expected_size: Option<u64>) -> NdnResult<FileBufferHandle> {
        // 伪代码：
        // 1) meta_db.check_mount_conflict(path)
        // 2) lease = meta_db.acquire_write_lease(path, current_session)
        // 3) fb = buffer.create_buffer(path, &lease, expected_size)
        // 4) meta_db.update_working_state(path, &lease, BufferStage::Writing, &fb)
        // 5) 返回 fb
        todo!()
    }

    pub fn open_file_writer(&self, path: &NdmPath, session: &SessionId) -> NdnResult<FileBufferHandle> {
        // 伪代码：
        // 1) stat(path)
        // 2) 若非 Working 或 session 不符：PathBusy
        // 3) 返回 buffer handle
        todo!()
    }

    pub fn append(&self, fb: &FileBufferHandle, data: &[u8]) -> NdnResult<()> {
        // 伪代码：
        // - buffer.append(fb, data)
        // - 可选：更新 last_write、续租 lease
        todo!()
    }

    pub fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        // 伪代码：buffer.flush(fb)
        todo!()
    }

    pub fn close_file(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        // 伪代码：
        // 1) buffer.close(fb)
        // 2) meta_db.update_working_state(path, lease, BufferStage::Cooling{closed_at}, fb)
        // 3) 投递后台：推进 staged 状态机（cooling -> linked -> finalized）
        todo!()
    }

    /// cacl_name：把 buffer 对象化为 ObjId，并提交 path（Working -> Committed）
    pub fn cacl_name(&self, fb: &FileBufferHandle, policy: Option<CommitPolicy>) -> NdnResult<ObjId> {
        // 伪代码（对应文档 write/commit 流程）：
        // 1) buffer.flush(fb)
        // 2) use policy = policy.unwrap_or(self.default_commit_policy)
        // 3) import = store.import_buffer_as_object(fb, policy)
        //    - Immediate：切 chunk、hash、写 internal（.tmp->.final）
        //    - Staged：可能先算 objid 并写 ExternalLink，随后后台 finalize
        // 4) meta_db.commit_path(fb.path, lease, import.obj_id)
        // 5) buffer.remove(fb)
        // 6) return import.obj_id
        todo!()
    }

    pub fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        // 伪代码：
        // 1) stat(src)
        // 2) 若 src Working：PathBusy（要求先 close + cacl）
        // 3) 若 src Committed(objid)：copy_path(src, target)
        todo!()
    }

    /// ========== 9.4 Pull / Materialization（异步） ==========

    pub fn pull(&self, path: &NdmPath, ctx: PullContext) -> NdnResult<()> {
        // 伪代码（异步）：
        // 1) objid = meta_db.get_committed_objid(path)
        // 2) pull_by_objid(objid, ctx)
        todo!()
    }

    pub fn pull_by_objid(&self, obj_id: ObjId, ctx: PullContext) -> NdnResult<()> {
        // 伪代码：
        // 1) 若无 fetcher：返回 InvalidParam("fetcher not configured")
        // 2) fetcher.schedule_pull_obj(self.store.clone(), obj_id, ctx)
        // 3) 立刻返回；进度通过 stat/stat_by_objid 可观测
        todo!()
    }

    pub fn pull_chunk(&self, chunk_id: ChunkId, ctx: PullContext) -> NdnResult<()> {
        // 伪代码：fetcher.schedule_pull_chunk(store, chunk_id, ctx)
        todo!()
    }

    /// ========== 9.5 物理驱逐 / 本地删除（不改变绑定） ==========

    pub fn erase_obj_by_id(&self, obj_id: &ObjId) -> NdnResult<()> {
        // 伪代码（文档语义：驱逐=变 NotPulled）：
        // 1) store.erase_object_data(obj_id)
        // 2) 不触碰 meta_db 的 path->objid 绑定
        // 3) 后续 open_reader/open_reader_by_id 应返回 NeedPull
        todo!()
    }

    /// ======================
    /// 9.6 给 ndn_router / push_chunk 用的低层 chunk API（兼容旧版 NamedDataMgr）
    /// ======================

    pub fn have_chunk(&self, chunk_id: &ChunkId) -> NdnResult<bool> {
        // 伪代码：store.have_chunk(chunk_id)
        todo!()
    }

    pub fn query_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStat> {
        // 伪代码：store.query_chunk(chunk_id)
        todo!()
    }

    pub async fn open_chunk_reader(&self, chunk_id: &ChunkId, offset: u64, opts: ReadOptions) -> NdnResult<(Reader, u64)> {
        // 伪代码：store.open_chunk_reader
        todo!()
    }

    pub async fn open_chunk_writer(&self, chunk_id: &ChunkId, expect_size: Option<u64>, offset: u64) -> NdnResult<(Writer, ChunkWriteTicket)> {
        // 伪代码（只写 internal；忽略 link）：
        // - store.open_chunk_writer(chunk_id, expect_size, offset)
        // - writer 写入 .tmp
        // - complete_chunk_writer(ticket) 负责校验 hash/size + rename .tmp->.final + 更新 db
        todo!()
    }

    pub fn complete_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()> {
        // 伪代码：store.complete_chunk_writer(ticket)
        todo!()
    }

    pub fn abort_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()> {
        // 伪代码：store.abort_chunk_writer(ticket)
        todo!()
    }

    pub async fn put_chunk_by_reader(&self, chunk_id: &ChunkId, chunk_size: u64, reader: &mut Reader) -> NdnResult<()> {
        // 伪代码：
        // 1) (w,ticket)=open_chunk_writer(chunk_id, Some(chunk_size), 0)
        // 2) copy exactly chunk_size bytes
        // 3) complete_chunk_writer(ticket)
        todo!()
    }

    pub async fn put_chunk(&self, chunk_id: &ChunkId, data: &[u8], verify: bool) -> NdnResult<()> {
        // 伪代码：
        // 1) 若 verify：计算 sha256 与 chunk_id 比对
        // 2) open_chunk_writer(chunk_id, Some(data.len()), 0)
        // 3) write_all
        // 4) complete
        todo!()
    }

    pub async fn open_chunklist_reader(&self, chunklist_obj_id: &ObjId, seek_from: SeekFrom, opts: ReadOptions) -> NdnResult<(Reader, u64)> {
        // 伪代码：store.open_chunklist_reader
        todo!()
    }

    /// ========== 9.7 Link 管理（外部纳管 / 别名 / 切片） ==========

    pub fn set_chunk_link(&self, chunk_id: &ChunkId, link: LinkType) -> NdnResult<()> {
        // 伪代码：
        // 1) 写入 store 的 link 表（并写反向索引）
        // 2) 注意：Writer 永远只写 internal；link 仅影响 Reader 路径
        todo!()
    }

    pub fn list_chunks_by_external_file(&self, file_path: &PathBuf) -> NdnResult<Vec<ChunkId>> {
        // 伪代码：store.list_chunks_by_external_file
        todo!()
    }

    /// ========== 9.8 Layout/Admin：扩容/回退/懒迁移 ==========

    pub fn expand_capacity(&self, new_target: StoreTarget) -> NdnResult<u64> {
        // 伪代码（O(1) 扩容）：
        // 1) layout = store.get_layout()
        // 2) 校验 target 可用
        // 3) layout.targets.push(new_target)
        // 4) layout.epoch += 1
        // 5) store.update_layout(layout)（持久化）
        // 6) return new epoch
        todo!()
    }

    pub fn trigger_background_tasks(&self) -> NdnResult<()> {
        // 伪代码：
        // - 启动 scrubber：清理 chunk.tmp、修正 Writing 过期状态
        // - 启动 staged commit 状态机推进（cooling->linked->finalized）
        // - 启动 lazy migration worker（读到旧 epoch 后投递任务）
        todo!()
    }
}

/// 后台任务管理器（占位）
pub struct BackgroundMgr {
    // 伪代码：
    // - staged queue
    // - lazy migration queue
    // - tmp scrubber config
}

impl BackgroundMgr {
    pub fn new() -> Self {
        todo!()
    }
}


/// =======================
/// 10) NamedStore 的关键算法接口形状（把文档契约落到 reader/writer）
/// =======================

pub struct NamedStoreImpl {
    layout: Arc<RwLock<StoreLayout>>,
    // db/index/targets...
}

impl NamedStoreImpl {
    pub fn new(layout: StoreLayout) -> Self {
        todo!()
    }

    fn open_internal_chunk_with_backoff(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        opts: &ReadOptions,
    ) -> NdnResult<(Reader, u64, u64 /*found_epoch*/)> {
        // 伪代码（epoch backoff <= 2）：
        // 1) cur = layout.epoch
        // 2) for e in [cur, cur-1, cur-2]:
        //    - targets = layout.select_targets(chunk_id, e)
        //    - 依次尝试打开 chunk.final
        //    - 若打开成功：返回 (reader, chunk_size, e)
        // 3) NotFound
        todo!()
    }

    fn trigger_lazy_migration(&self, chunk_id: &ChunkId, data: Vec<u8>, to_targets: Vec<StoreTarget>) {
        // 伪代码：
        // - 后台线程/任务：把 data 写入新 targets（按当前 epoch）
        // - 可选：写成功后标记位置；旧位置留给后台回收（或永不自动删）
        todo!()
    }

    fn resolve_linked_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        opts: &ReadOptions,
        visited: &mut HashSet<ChunkId>,
    ) -> NdnResult<(Reader, u64)> {
        // 伪代码（防环 + max depth）：
        // 1) if visited.len() >= opts.max_link_depth => InvalidData("link too deep")
        // 2) if visited contains chunk_id => InvalidData("link cycle")
        // 3) visited.insert(chunk_id)
        // 4) link = get_link(chunk_id) else NeedPull
        // 5) match link:
        //    - SameAs(real) => resolve_linked_reader(real, offset, ...)
        //    - PartOf{base, range} =>
        //         real_offset = range.start + offset
        //         (r, base_size) = open_chunk_reader(base, real_offset, opts)
        //         wrap r with limit = range.len - offset
        //    - ExternalFile{path, range, qcid} =>
        //         if !opts.allow_external => NeedPull/InvalidParam
        //         if opts.external_verify requires qcid and qcid mismatch => LinkInvalid
        //         open file + seek(range.start + offset) + limit(range)
        todo!()
    }
}


/// =======================
/// 11) Registry（兼容旧版 get_named_data_mgr_by_id 风格）
/// =======================

pub type NamedDataMgrRef = Arc<tokio::sync::Mutex<NamedDataMgr>>;

lazy_static::lazy_static! {
    pub static ref NAMED_DATA_MGR_MAP: Arc<tokio::sync::Mutex<HashMap<String, NamedDataMgrRef>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
}

impl NamedDataMgr {
    pub async fn get_named_data_mgr_by_id(mgr_id: Option<&str>) -> Option<NamedDataMgrRef> {
        // 伪代码：
        // 1) id = mgr_id.unwrap_or("default")
        // 2) map = NAMED_DATA_MGR_MAP.lock().await
        // 3) return map.get(id).cloned()
        todo!()
    }

    pub async fn register_named_data_mgr(mgr_id: &str, mgr: NamedDataMgr) -> NdnResult<NamedDataMgrRef> {
        // 伪代码：
        // 1) wrap mgr into Arc<Mutex<_>>
        // 2) insert into NAMED_DATA_MGR_MAP
        // 3) return ref
        todo!()
    }
}
