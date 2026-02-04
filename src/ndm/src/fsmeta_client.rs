
pub const MAX_EPOCH_BACKOFF: u64 = 2;

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
    pub ref_count: i32,
    pub last_access_time: Option<u64>,
}

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

#[derive(Debug, Clone)]
pub enum TargetStatus {
    Active,
    ReadOnly,
    Offline,
}





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferNodeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileBufferId(pub String);

#[derive(Debug, Clone)]
pub enum BufferStage {
    Writing,
    Cooling { closed_at: u64 },
    Linked { obj_id: ObjId, linked_at: u64 },
    Finalized { obj_id: ObjId, finalized_at: u64 },
}

#[derive(Debug, Clone)]
pub struct FileBufferHandle {
    pub id: FileBufferId,
    pub path: NdmPath,
    pub session: SessionId,
    pub fence: FenceToken,

    /// 单机版可为 local；分布式版对应 buffer node
    pub node_id: BufferNodeId,
    pub remote_path: PathBuf,
}

#[derive(Debug, Clone)]
pub enum CommitPolicy {
    /// 传统模式：close 后立刻 objectify + 写入 internal store（慢，但简单）
    ImmediateFinalize,

    /// 分级提交：close 先进入 cooling；冷却后计算 objid 并 ExternalLink；冻结后搬入 internal
    Staged {
        cooling_ms: u64,
        frozen_ms: u64,
    },
}

/// Path 层：定义非 POSIX 的 namespace 事务、挂载互斥、Strict Single Writer lease
pub trait FsMeta: Send + Sync {
    fn check_mount_conflict(&self, path: &NdmPath) -> NdnResult<()>;
    fn resolve(&self, path: &NdmPath) -> NdnResult<NodeState>;

    fn stat(&self, path: &NdmPath) -> NdnResult<PathStat>;
    fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat>;

    /// O(1) 绑定：只写元数据，不代表本地已 pull
    fn add_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()>;
    fn add_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()>;

    fn delete(&self, path: &NdmPath) -> NdnResult<()>;
    fn move_path(&self, parent: NodeId, name: &str, new_parent: NodeId, new_name: &str) -> NdnResult<()>;
    fn copy_path(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()>;
    fn link();

    //list 保存在 fsmeta db 中, 属于BaseLayer的DirBuffer
    fn list(&self, parent: NodeId, pos: u32, page_size: u32) -> NdnResult<Vec<(String, PathStat)>>;

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
