

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


    
    pub fn stat(&self, path: &NdmPath) -> NdnResult<PathStat> {
        // 伪代码：直接委托 meta_db.stat
        unimplemented!()
    }

    pub fn stat_by_objid(&self, obj_id: &ObjId) -> NdnResult<ObjStat> {
        // 伪代码：
        // - meta_db.stat_by_objid（namespace 维度）可能只知道绑定/元信息
        // - store_layout.stat_object 负责判断本地是否 materialized
        unimplemented!()
    }

    // 系统里的 Dir写和File写，本质上都是一样的
    //   File写 -> 基于BaseChunkList修改ChunkList[index] = new Chunk
    //   Dir写 -> 基于BaseDirObject修改Children[name] = new ObjId
    // 不同是File写要等Close,其它人才能看到，而Dir的写操作，通常是立刻就能生效的

    pub fn set_file(&self, path: &NdmPath, obj_id: ObjId) -> NdnResult<()> {
        // 伪代码：
        // 1) meta_db.check_mount_conflict(path)
        // 2) meta_db.set_file(path, obj_id)
        // 3) 更新ref_count
        // 注意：O(1) 绑定，不保证已 pull

        // 新流程
        //  会触发 parent dir 的非物化（Committed->Working）
        //  这个过程是一个递归向上的过程： 一个物化的dir里，只能包含物化的Child Object，但非物化的Dir里，可以包含非物化的Child Object和物化的Child Object
        

        unimplemented!()
    }

    pub fn set_dir(&self, path: &NdmPath, dir_obj_id: ObjId) -> NdnResult<()> {
        // 同 set_file，
        // 更新ref_count的逻辑会比较复杂
        unimplemented!()
    }

    pub fn delete(&self, path: &NdmPath) -> NdnResult<()> {
        // 伪代码：删除 path 绑定
        // 更新ref_count,不触发store数据删除
        unimplemented!()
    }

    pub fn move_path(&self, old_path: &NdmPath, new_path: &NdmPath) -> NdnResult<()> {
        // 伪代码：namespace 事务，O(1) 改指针
        unimplemented!()
    }

    pub fn copy_path(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        // 伪代码：
        // 如果src指向的是Object 复制绑定（语义上“快照”），仍是 O(1)
        // 如果src指向的是None,则需要根据list的结果，出发一系列复制
        // 需要更新ref_count
        unimplemented!()
    }

    pub fn list(&self, path: &NdmPath, pos: u32, page_size: u32) -> NdnResult<Vec<(String, PathStat)>> {
        // 伪代码：
        // 1) stat(path)
        // 2) 若 Working: 根据Working状态返回 DirBufferHandle
        //   - DirBufferHandle可能会基于一个Ready状态的DirObjed
        // 3）若 Committed(objid):
        //   - Objid的类型要是DirObject
        //   - store_layout.get_object(objid) 取 DirObject
        //   - 解析 children 列表，分页返回
        // 不会触发自动pull
        unimplemented!()
    }

    /// ========== 9.2 读接口：open_reader / open_reader_by_id ==========

    pub async fn open_reader(&self, path: &NdmPath, opts: ReadOptions) -> NdnResult<(Reader, u64)> {
        // 伪代码：
        // 1) stat(path), 会处理inner_path切分
        // 2) 若 Working：根据Working状态返回 FileBufferHandle
        //    - open_reader_by_file_buffer(fb, opts)
        // 3) 若 Committed(objid)：
        //    - objid的类型要支持打开open_reader
        //    - 先判断 obj material 状态：store.stat_object_material
        //    - 若 NotPulled/Partial：返回 NeedPull
        //    - 否则 open_reader_by_id(objid, None, opts)
        // 4) 注意支持 InnerPath，比如  /data/dir1/file1 , dir1 是 DirObject，
        //    - open_reader_by_id(objid_of_dir1, Some("file1"), opts)
        unimplemented!()
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
        unimplemented!()
    }

    pub fn get_object(&self, obj_id: &ObjId) -> NdnResult<Vec<u8>> {
        // 伪代码：store_layout.get_object(obj_id)
        // 对get_object(fileobjid)，会返回fileobj,但是open_reader_by_id(fileobjid),会返回content对应的reader
        unimplemented()
    }

    pub async fn get_object_by_path(&self, path: &NdmPath) -> NdnResult<String> {

        // 逻辑与open_reader基本相同
        // 1) 从 store.get_object(obj_id) 取对象体（jwt/json/bytes）
        // 2) 若对象体缺失：NeedPull
    }

    /// ========== 9.3 写接口：Strict Single Writer（FileBuffer） ==========

    pub fn create_dir(&self, path: &NdmPath) -> NdnResult<()> {
        // self.set_dir(path, None), 占位
        // 以后这个Path不能set_dir 了
        unimplemented!()
    }

    pub fn open_file_writer(&self, path: &NdmPath, expected_size: Option<u64>, flags) -> NdnResult<FileBufferHandle> {
        // 伪代码：
        // 1) meta_db.check_mount_conflict(path)
        // 2) lease = meta_db.acquire_write_lease(path, current_session, flags)
        // 3) fb = buffer.create_buffer(path, &lease, expected_size)
        // 4) meta_db.update_working_state(path, &lease, BufferStage::Writing, &fb)
        // 5) 返回 fb
        unimplemented!()
    }

    pub fn append(&self, path: &NdmPath, data: &[u8]) -> NdnResult<()> {
        // 伪代码：
        // - writer = open_file_writer(path, None, flags)
        // - writer.append(data)
        // - close_file(writer)
       unimplemented!()
    }


    pub fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        // 伪代码：buffer.flush(fb)
        unimplemented!()
    }

    pub fn close_file(&self, fb: &FileBufferHandle) -> NdnResult<()> {
        // 伪代码：
        // 1) buffer.close(fb)
        // 2) meta_db.update_working_state(path, lease, BufferStage::Cooling{closed_at}, fb)
        // 3) 投递后台：推进 staged 状态机（cooling -> linked -> finalized）
        unimplemented!()
    }

    /// cacl_name：把 buffer 对象化为 ObjId，并提交 path（Working -> Committed）
    /// 该函数由fs_meta servcie在后台自动调用
    // pub fn cacl_name(&self, fb: &FileBufferHandle, policy: Option<CommitPolicy>) -> NdnResult<ObjId> {
    //     // 伪代码（对应文档 write/commit 流程）：
    //     // 1) buffer.flush(fb)
    //     // 2) use policy = policy.unwrap_or(self.default_commit_policy)
    //     // 3) import = store.import_buffer_as_object(fb, policy)
    //     //    - Immediate：切 chunk、hash、写 internal（.tmp->.final）
    //     //    - Staged：可能先算 objid 并写 ExternalLink，随后后台 finalize
    //     // 4) meta_db.commit_path(fb.path, lease, import.obj_id)
    //     // 5) buffer.remove(fb)
    //     // 6) return import.obj_id
    //     todo!()
    // }

    pub fn snapshot(&self, src: &NdmPath, target: &NdmPath) -> NdnResult<()> {
        // 伪代码：
        // 难点是
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
    /// 9.6 给 ndn_router 用的低层 chunk API（兼容旧版 NamedDataMgr）
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

    pub fn publish_dir(&self, path: &NdmPath) -> NdnResult<()> {
        // 伪代码（占位）：
        // 判断 得到Path的inode信息，如果已经是Committed状态，且是DirObject，则直接返回成功
        // 开始物化整个目录树
        //  得到所有的，未物化的child
        //  尝试物化child,得到child objid
        // 更新当前Path的inode信息，并将状态设置为Committed（物化完成）
    
    }

    /// ========== 9.9 低层 Chunk 接口, 暂不实现 ==========
    // pub async fn open_chunk_writer(&self, chunk_id: &ChunkId, expect_size: Option<u64>, offset: u64) -> NdnResult<(Writer, ChunkWriteTicket)> {
    //     // 伪代码（只写 internal；忽略 link）：
    //     // - store.open_chunk_writer(chunk_id, expect_size, offset)
    //     // - writer 写入 .tmp
    //     // - complete_chunk_writer(ticket) 负责校验 hash/size + rename .tmp->.final + 更新 db
    //     todo!()
    // }

    // pub fn complete_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()> {
    //     // 伪代码：store.complete_chunk_writer(ticket)
    //     todo!()
    // }

    // pub fn abort_chunk_writer(&self, ticket: ChunkWriteTicket) -> NdnResult<()> {
    //     // 伪代码：store.abort_chunk_writer(ticket)
    //     todo!()
    // }

    // pub async fn put_chunk_by_reader(&self, chunk_id: &ChunkId, chunk_size: u64, reader: &mut Reader) -> NdnResult<()> {
    //     // 伪代码：
    //     // 1) (w,ticket)=open_chunk_writer(chunk_id, Some(chunk_size), 0)
    //     // 2) copy exactly chunk_size bytes
    //     // 3) complete_chunk_writer(ticket)
    //     todo!()
    // }

    // pub async fn put_chunk(&self, chunk_id: &ChunkId, data: &[u8], verify: bool) -> NdnResult<()> {
    //     // 伪代码：
    //     // 1) 若 verify：计算 sha256 与 chunk_id 比对
    //     // 2) open_chunk_writer(chunk_id, Some(data.len()), 0)
    //     // 3) write_all
    //     // 4) complete
    //     todo!()
    // }

    // pub async fn open_chunklist_reader(&self, chunklist_obj_id: &ObjId, seek_from: SeekFrom, opts: ReadOptions) -> NdnResult<(Reader, u64)> {
    //     // 伪代码：store.open_chunklist_reader
    //     todo!()
    // }
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
