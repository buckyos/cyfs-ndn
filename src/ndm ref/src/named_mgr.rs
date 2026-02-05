
// ------------------------------
// Options / Plan types
// ------------------------------

#[derive(Clone, Copy)]
struct MoveOptions {
    /// Whether to overwrite destination if destination is visible in Upper layer.
    overwrite_upper: bool,

    /// If true: also check destination Base (merged view no-clobber).
    /// This requires Base materialized, otherwise NEED_PULL.
    strict_check_base: bool,
}

impl Default for MoveOptions {
    fn default() -> Self {
        Self {
            overwrite_upper: false,
            strict_check_base: false,
        }
    }
}

#[derive(Clone, Debug)]
enum MoveSource {
    /// Source resolved from Upper dentry (fast, no base needed)
    Upper {
        target: DentryTarget,      // IndexNodeId | ObjId
        kind_hint: ObjectKind,     // used for cycle check & optional validations
    },
    /// Source resolved from Base (no upper dentry exists)
    Base {
        obj_id: ObjId,
        kind: ObjectKind,
        /// To protect correctness: we snapshot (rev, base_obj_id) of src parent,
        /// and later verify rev in txn (OCC). base_obj_id is optional.
        src_parent_rev0: u64,
        src_parent_base0: Option<ObjId>,
    },
}

#[derive(Clone, Debug)]
struct MovePlan {
    src_parent: IndexNodeId,
    src_name: String,
    dst_parent: IndexNodeId,
    dst_name: String,

    /// Snapshot rev for OCC; move must bump rev for any modified dir.
    src_rev0: u64,
    dst_rev0: u64,

    source: MoveSource,
}

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

     fn check_dest_conflict_pre(
        &self,
        dst_parent: IndexNodeId,
        dst_name: &str,
        dst_dir_node: &NodeRecord,
        opts: MoveOptions,
    ) -> NdnResult<()> {
        // Upper check (fast)
        if let Some(d) = self.fsmeta.get_dentry(&dst_parent, dst_name)? {
            match d.target {
                DentryTarget::Tombstone => { /* treat as not-exists */ }
                DentryTarget::IndexNodeId(_) | DentryTarget::ObjId(_) => {
                    if !opts.overwrite_upper {
                        return Err(NdmError::AlreadyExists);
                    }
                }
            }
        }

        if !opts.strict_check_base {
            // Overlay-friendly policy:
            // - do not check Base existence
            // - allow shadowing Base entry without pulling it
            return Ok(());
        }

        // Strict policy: also check Base (merged view no-clobber)
        let base = match &dst_dir_node.base_obj_id {
            None => return Ok(()),
            Some(oid) => oid.clone(),
        };
        if !self.store.has_materialized(&base)? {
            return Err(NdmError::NeedPull);
        }
        let dir_obj = self.store.get_dir_object(&base)?;
        if dir_obj.children.iter().any(|c| c.name == dst_name) {
            return Err(NdmError::AlreadyExists);
        }
        Ok(())
    }

    fn plan_move_source(
            &self,
            src_parent: IndexNodeId,
            src_name: &str,
            src_dir_node: &NodeRecord,
            src_rev0: u64,
        ) -> NdnResult<MoveSource> {
            // 1) Upper first
            if let Some(d) = self.fsmeta.get_dentry(&src_parent, src_name)? {
                return match d.target {
                    DentryTarget::Tombstone => Err(NdmError::NotFound),
                    DentryTarget::IndexNodeId(fid) => {
                        // need kind hint for cycle check
                        let inode = self.fsmeta.get_node(&fid)?.ok_or(NdmError::NotFound)?;
                        let kind_hint = match inode.kind {
                            NodeKind::Dir => ObjectKind::Dir,
                            NodeKind::File => ObjectKind::File,
                        };
                        Ok(MoveSource::Upper { target: DentryTarget::IndexNodeId(fid), kind_hint })
                    }
                    DentryTarget::ObjId(oid) => {
                        // kind hint best-effort (meta may be missing if not pulled)
                        let kind_hint = self.store.get_object_meta(&oid)?.map(|m| m.kind).unwrap_or(ObjectKind::Unknown);
                        Ok(MoveSource::Upper { target: DentryTarget::ObjId(oid), kind_hint })
                    }
                };
            }

            // 2) Upper miss => Base-only
            let base0 = src_dir_node.base_obj_id.clone();
            let base_oid = base0.clone().ok_or(NdmError::NotFound)?;

            // Must read DirObject to know which ObjId we are moving.
            // If base is not materialized, we cannot discover child obj_id => NEED_PULL.
            if !self.store.has_materialized(&base_oid)? {
                return Err(NdmError::NeedPull);
            }

            let dir_obj = self.store.get_dir_object(&base_oid)?;
            for c in dir_obj.children.iter() {
                if c.name == src_name {
                    return Ok(MoveSource::Base {
                        obj_id: c.obj_id.clone(),
                        kind: c.kind.clone(),
                        src_parent_rev0: src_rev0,
                        src_parent_base0: base0,
                    });
                }
            }

            Err(NdmError::NotFound)
        }
    }

    // helpers (pseudocode)
    fn is_dir_like(src: &MoveSource) -> bool {
        match src {
            MoveSource::Upper { kind_hint, .. } => *kind_hint == ObjectKind::Dir,
            MoveSource::Base { kind, .. } => *kind == ObjectKind::Dir,
        }
    }   
    fn move_path_with_opts(&self, old_path: &NdmPath, new_path: &NdmPath, opts: MoveOptions) -> NdnResult<()> {
        // -------- 0) trivial / safety checks
        // normalize paths (remove trailing '/', resolve "." etc.) - pseudocode
        if normalize_path(old_path) == normalize_path(new_path) {
            return Ok(()); // no-op
        }
        if is_root_path(old_path) {
            return Err(NdmError::InvalidName); // refuse moving root
        }

        // -------- 1) parse (src_parent_path, src_name), (dst_parent_path, dst_name)
        let (src_parent_path, src_name) = split_parent_name(old_path)?;
        let (dst_parent_path, dst_name) = split_parent_name(new_path)?;
        if !is_valid_name(&src_name) || !is_valid_name(&dst_name) {
            return Err(NdmError::InvalidName);
        }

        // -------- 2) ensure parents are directory inodes (mutable ops require inode)
        // This may materialize a directory inode if it was ObjId-only.
        let src_parent = self.ensure_dir_inode(&src_parent_path)?;
        let dst_parent = self.ensure_dir_inode(&dst_parent_path)?;

        // -------- 3) snapshot parent nodes (mount_mode, base_obj_id, rev)
        let src_dir = self.fsmeta.get_node(&src_parent)?.ok_or(NdmError::NotFound)?;
        let dst_dir = self.fsmeta.get_node(&dst_parent)?.ok_or(NdmError::NotFound)?;
        if src_dir.kind != NodeKind::Dir || dst_dir.kind != NodeKind::Dir {
            return Err(NdmError::NotFound);
        }
        if src_dir.mount_mode == Some(MountMode::ReadOnly) || dst_dir.mount_mode == Some(MountMode::ReadOnly) {
            return Err(NdmError::ReadOnly);
        }

        let src_rev0 = src_dir.rev.unwrap_or(0);
        let dst_rev0 = dst_dir.rev.unwrap_or(0);

        // -------- 4) build a MovePlan (resolve source with overlay rules)
        // IMPORTANT: any slow IO (reading base DirObject) must happen OUTSIDE fsmeta txn.
        let source = self.plan_move_source(src_parent.clone(), &src_name, &src_dir, src_rev0)?;

        // -------- 5) cycle prevention (best-effort, cheap)
        // Only meaningful if moving a directory.
        if is_dir_like(&source) {
            // prevent moving "/a" into "/a/..."
            if is_descendant_path(new_path, old_path) {
                return Err(NdmError::InvalidName);
            }
        }

        // -------- 6) destination conflict check (policy-driven)
        // default: check Upper only; allow shadowing Base without pull.
        self.check_dest_conflict_pre(dst_parent.clone(), &dst_name, &dst_dir, opts)?;

        // -------- 7) commit in one fsmeta transaction (atomic across two dirs)
        let plan = MovePlan {
            src_parent: src_parent.clone(),
            src_name: src_name.clone(),
            dst_parent: dst_parent.clone(),
            dst_name: dst_name.clone(),
            src_rev0,
            dst_rev0,
            source,
        };

        self.apply_move_plan_txn(plan, opts)
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
        // -------- A) 解析 parent/name（纯 NDM 逻辑）
        let (parent_dir_path, name) = split_parent(path)?;
        let parent_dir_id = ensure_dir_inode(parent_dir_path)?;     // 若是 ObjId dir -> materialize inode
        let parent_node = fsmeta.get_node(parent_dir_id)?.ok_or(NotFound)?;
        if parent_node.mount_mode == ReadOnly { return Err(ReadOnly); }

        // -------- B) 确保 name 对应的是 “文件 inode”
        // Strategy B: dentry target 允许 ObjId；但要写入必须转成 IndexNodeId
        // 注意：这一步建议走 fsmeta txn，避免并发 create 同名
        let file_id = {
            let mut tx = fsmeta.begin_txn()?;

            match tx.get_dentry(parent_dir_id, &name)? {
                None | Some(Dentry{target: Tombstone,..}) => {
                    // 新文件
                    let fid = fsmeta.alloc_inode(File)?;
                    tx.upsert_dentry(parent_dir_id, &name, Target::IndexNodeId(fid))?;
                    // node 初始 committed base 为空，或指向空文件对象（实现选择）
                    tx.put_node(NodeRecord::new_file(fid))?;
                    tx.commit()?;
                    fid
                }

                Some(Dentry{target: ObjId(oid), ..}) => {
                    // 之前是只读绑定：写入需要 materialize inode，并把 base_obj_id=oid
                    let fid = fsmeta.alloc_inode(File)?;
                    let mut n = NodeRecord::new_file(fid);
                    n.base_obj_id = Some(oid);
                    tx.put_node(&n)?;
                    tx.upsert_dentry(parent_dir_id, &name, Target::IndexNodeId(fid))?;
                    tx.commit()?;
                    fid
                }

                Some(Dentry{target: IndexNodeId(fid), ..}) => {
                    // 已经是 inode
                    tx.commit()?; // no-op
                    fid
                }
            }
        };

        // -------- C) 获取严格单写 lease（fence）
        // 这里必须由 fsmeta 保证 “同一时刻一个 writer”
        // fence 用于防止 ABA：旧 writer 释放后新 writer 拿到新 fence，后台/旧请求不能覆盖新状态
        let fence = fsmeta.acquire_file_lease(file_id, session, ttl=cfg.file_lease_ttl)?;

        // -------- D) 决策：复用旧 buffer 还是开启新 buffer
        // 这一步体现 NDM 的“指挥调度”：
        // - Reading/Committed 在 store
        // - Writing/Cooling/Linked 的热数据在 BufferNode
        // - 状态机由 fsmeta node.state 驱动
        let node = fsmeta.get_node(file_id)?.ok_or(NotFound)?;
        match node.state {
            Writing{..} => return Err(PathBusy), // 正在写（理论上 lease 已阻止，但这里再兜底）

            Cooling{buffer_node, remote_path, closed_at} => {
                // ✅ 复活：同一个 remote_path 继续写，重置 last_write_at
                // 注意：这里不用等待后台；后台扫描看到 state=Writing 就不会 objectify
                fsmeta.update_file_state_with_fence(
                    file_id, session, fence,
                    new_state = Writing{session,fence,buffer_node,remote_path,last_write_at=now()}
                )?;
                return Ok(FileBufferHandle{file_id,session,fence,buffer_node,remote_path});
            }

            Linked{obj_id, buffer_node, remote_path, ..} => {
                // ⚠️ Linked 的 remote_path 已经被 ExternalLink 引用，不能再修改
                // 开新版本：新建 remote_path（可选：在 BufferNode 内做 server-side copy 作为起点）
                let bn = buffer_master.pick_node_for_write(expect_size)?;
                let (node_id, new_remote_path) = buffer_master.create_remote_file(&bn, hint=&name)?;
                // 可选：bn.clone_file(remote_path -> new_remote_path) or seed from obj_id
                // 这里先简化：不做拷贝，走“重写式”写入
                fsmeta.update_file_state_with_fence(
                    file_id, session, fence,
                    new_state = Writing{session,fence,buffer_node:node_id,remote_path:new_remote_path,last_write_at=now()}
                )?;
                return Ok(FileBufferHandle{file_id,session,fence,buffer_node:node_id,remote_path:new_remote_path});
            }

            Finalized{obj_id,..} | Committed{..} => {
                // 已经是冷态对象，开新版本写：分配新 buffer
                let bn = buffer_master.pick_node_for_write(expect_size)?;
                let (node_id, remote_path) = buffer_master.create_remote_file(&bn, hint=&name)?;
                fsmeta.update_file_state_with_fence(
                    file_id, session, fence,
                    new_state = Writing{session,fence,buffer_node:node_id,remote_path,last_write_at=now()}
                )?;
                return Ok(FileBufferHandle{file_id,session,fence,buffer_node:node_id,remote_path});
            }
        }
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

        // A) 快速 flush 到 BufferNode（保证缓冲落盘/可恢复）
        let bn = buffer_master.get_node(&fb.buffer_node)?;
        bn.flush(&fb.remote_path)?;

        // B) 事务性地把 inode 状态变成 Cooling，并释放 lease
        // 关键：要带 fence，防止旧 handle 写入或 close 覆盖新 session
        fsmeta.update_file_state_with_fence(
            fb.file_id, fb.session, fb.fence,
            new_state = Cooling{buffer_node:fb.buffer_node, remote_path:fb.remote_path, closed_at:now()}
        )?;

        fsmeta.release_file_lease(fb.file_id, fb.session, fb.fence)?;
        Ok(())
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

    fn cacl_dir(dir_path: Path) -> Result<ObjId> {
        // -------- 1) resolve dir inode
        let dir_id = ensure_dir_inode(dir_path)?; // 目录操作必须有 inode（持 mount_mode/rev/base）
        let dir_node = fsmeta.get_node(dir_id)?.ok_or(NotFound)?;
        if dir_node.kind != Dir { return Err(NotFound); }
        if dir_node.mount_mode == ReadOnly { return Err(ReadOnly); }

        // -------- 2) snapshot（不持锁，避免长事务）
        let base_oid_opt = dir_node.base_obj_id.clone();
        let rev0 = dir_node.rev.unwrap_or(0);

        // Upper layer：通常稀疏，可一次性读出
        let upper_raw = fsmeta.list_dentries(dir_id)?; // Vec<DentryRecord>

        // -------- 3) 把 Upper layer 归一化成 “name -> UpperOp(put/delete)”（并解析 IndexNodeId→ObjId）
        // 这里是 NDM “调度其它组件” 的核心：既查 fsmeta，又决定是否需要 NeedPull/PathBusy
        let mut upper_ops: BTreeMap<String, UpperOp> = BTreeMap::new();

        for d in upper_raw {
            match d.target {
                Tombstone => {
                    upper_ops.insert(d.name, UpperOp::Delete);
                }
                ObjId(oid) => {
                    // 只要有 ObjId 就能写入 DirObject；不要求 materialized
                    let kind = store.get_object_meta(&oid)?.map(|m| m.kind).unwrap_or(Unknown);
                    upper_ops.insert(d.name, UpperOp::Put{obj_id:oid, kind});
                }
                IndexNodeId(child_id) => {
                    // 必须把 inode 解析成一个“可引用的 ObjId”
                    let child = fsmeta.get_node(child_id)?.ok_or(NotFound)?;

                    let (obj_id, kind) = match child.kind {
                        File => {
                            // 选择可稳定引用的版本：Linked/Finalized/Committed
                            match child.state {
                                Linked{s,..} => (s.obj_id, File),
                                Finalized{s,..} => (s.obj_id, File),
                                Committed{base_obj_id} => (base_obj_id, File),

                                // Working 态：目录快照怎么处理？
                                // 方案 A（严格）：直接 PATH_BUSY
                                // 方案 B（折中）：若 child.base_obj_id 存在，引用“最后已提交版本”
                                Writing{..} | Cooling{..} => {
                                    if let Some(base) = child.base_obj_id {
                                        (base, File) // “只快照 committed 视图”
                                    } else {
                                        return Err(PathBusy);
                                    }
                                }

                                _ => return Err(PathBusy),
                            }
                        }
                        Dir => {
                            // 目录 inode 自身必须有一个 base DirObject 才能被引用
                            if let Some(oid) = child.base_obj_id {
                                (oid, Dir)
                            } else {
                                // 允许把“空目录 inode”对象化成 EMPTY_DIR_OBJ（一次性常量对象）
                                let empty = ensure_empty_dir_object_in_store()?;
                                (empty, Dir)
                            }
                        }
                    };

                    upper_ops.insert(d.name, UpperOp::Put{obj_id, kind});
                }
            }
        }

        // -------- 4) base dir 必须 materialized（否则无法 merge）
        let base_iter = match base_oid_opt {
            None => DirIter::empty(),
            Some(base_oid) => {
                if !store.has_materialized(&base_oid)? { return Err(NeedPull); }
                store.dir_children_iter_sorted(&base_oid)? // streaming iterator (name sorted)
            }
        };

        // -------- 5) merge-join（流式，不把 base 全量载入）
        // upper_ops 是 BTreeMap => name 有序；base_iter 也是有序
        let mut upper_it = upper_ops.into_iter().peekable();
        let mut out_children: Vec<DirChild> = Vec::new();

        let mut base_it = base_iter.peekable();
        while base_it.peek().is_some() || upper_it.peek().is_some() {
            match (base_it.peek(), upper_it.peek()) {
                (Some(base), Some((uname, uop))) => {
                    if base.name < *uname {
                        // base-only
                        out_children.push(DirChild{name:base.name.clone(), kind:base.kind, obj_id:base.obj_id.clone()});
                        base_it.next();
                    } else if base.name > *uname {
                        // upper-only
                        apply_upper_only(&mut out_children, uname.clone(), uop.clone());
                        upper_it.next();
                    } else {
                        // same name: upper wins
                        apply_upper_override(&mut out_children, uname.clone(), uop.clone());
                        base_it.next();
                        upper_it.next();
                    }
                }
                (Some(base), None) => {
                    out_children.push(DirChild{...base...});
                    base_it.next();
                }
                (None, Some((uname, uop))) => {
                    apply_upper_only(&mut out_children, uname.clone(), uop.clone());
                    upper_it.next();
                }
                (None, None) => break,
            }
        }

        fn apply_upper_only(out: &mut Vec<DirChild>, name: String, op: UpperOp) {
            match op {
                Delete => { /* nothing */ }
                Put{obj_id, kind} => out.push(DirChild{name, kind, obj_id})
            }
        }
        fn apply_upper_override(out: &mut Vec<DirChild>, name: String, op: UpperOp) {
            match op {
                Delete => { /* remove */ }
                Put{obj_id, kind} => out.push(DirChild{name, kind, obj_id})
            }
        }

        // -------- 6) 构造新 DirObject 并写入 store
        let new_dir = DirObject{children: out_children}; // must already be sorted & unique
        let new_dir_obj_id = store.put_dir_object(&new_dir)?; // canonical encode -> hash -> obj_id

        // -------- 7) fsmeta commit（短事务 + OCC）
        // 这里体现“merge 很慢，但提交很快”：只在最后持锁很短时间
        {
            let mut tx = fsmeta.begin_txn()?;

            let cur = tx.get_node(&dir_id)?.ok_or(NotFound)?;
            if cur.rev != Some(rev0) {
                tx.rollback()?;
                return Err(Conflict); // 目录期间有并发修改，调用方可重试
            }
            if cur.mount_mode == Some(ReadOnly) { tx.rollback()?; return Err(ReadOnly); }

            // 更新 base snapshot
            let mut updated = cur.clone();
            updated.base_obj_id = Some(new_dir_obj_id.clone());
            updated.rev = Some(rev0 + 1);
            tx.put_node(&updated)?;

            // 清空 upper dentries（tombstone 一并清理）
            tx.clear_all_dentries_under(dir_id)?;   // 需要 fsmeta 提供批量接口

            tx.commit()?;
        }

        Ok(new_dir_obj_id)
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

    fn cooling_to_linked(file_id: IndexNodeId) -> Result<()> {
        // 1) 读 inode，必须仍然是 Cooling 且到达防抖窗口
        let n = fsmeta.get_node(file_id)?.ok_or(NotFound)?;
        let cooling = match n.state {
            Cooling{buffer_node, remote_path, closed_at} => (buffer_node, remote_path, closed_at),
            _ => return Ok(()), // 被复活/被新版本写覆盖了
        };
        if now() - cooling.closed_at < cfg.cooling_grace { return Ok(()); }

        // 2) 关键：用 CAS 抢占“处理权”，避免多个后台重复 objectify
        //    这里用一个中间态 CoolingProcessing 更稳（伪代码略）
        if !fsmeta.cas_state(file_id, expected=Cooling{...}, new=CoolingProcessing{...})? {
            return Ok(()); // 有人先处理/状态变了
        }

        // 3) BufferNode seal + objectify（本地计算 hash/chunklist，不传数据）
        let bn = buffer_master.get_node(&cooling.buffer_node)?;
        bn.seal(&cooling.remote_path)?;
        let obj = bn.objectify(&cooling.remote_path)?; 
        // obj: {file_obj_id, chunks: [ExternalChunkLink], file_size}

        // 4) 在 named_store 写入：FileObject + chunk ExternalLink
        //    这一步使得 open_reader_by_id(obj_id) 成立（虽然数据仍在 BufferNode）
        for c in obj.chunks {
            store.upsert_chunk_link(
                c.chunk_id,
                LinkType::ExternalFile{url:c.url, range:c.range, qcid:c.qcid}
            )?;
        }
        store.put_file_object(FileObject{obj_id:obj.file_obj_id, chunks:..., size:obj.file_size})?;
        store.put_object_meta(NamedObjectMeta{obj_id:obj.file_obj_id, kind:File, size:Some(obj.file_size)})?;

        // 5) fsmeta：CAS 把状态推进到 Linked
        //    注意：若这时用户又 create_file 把它复活成 Writing，这里必须 CAS 失败并放弃更新
        if !fsmeta.cas_state(
            file_id,
            expected=CoolingProcessing{buffer_node:..., remote_path:...},
            new=Linked{obj_id:obj.file_obj_id, buffer_node:..., remote_path:..., linked_at:now()}
        )? {
            // 竞争失败：产生了“孤儿对象/链接”，后续靠软状态清理或引用计数处理
            // (这在家用 zero-op 初期可以接受：不自动删业务数据，只清软状态)
            return Ok(());
        }

        Ok(())
    }

    fn linked_to_finalized(file_id: IndexNodeId) -> Result<()> {
        let n = fsmeta.get_node(file_id)?.ok_or(NotFound)?;
        let linked = match n.state {
            Linked{obj_id, buffer_node, remote_path, linked_at} => (obj_id, buffer_node, remote_path, linked_at),
            _ => return Ok(()),
        };
        if now() - linked.linked_at < cfg.frozen_grace { return Ok(()); }

        // CAS 抢占处理权（Finalizing 中间态）
        if !fsmeta.cas_state(file_id, expected=Linked{...}, new=Finalizing{...})? {
            return Ok(());
        }

        // 1) 让 BufferNode 推送数据到 named_store 内部 chunk 存储
        let bn = buffer_master.get_node(&linked.buffer_node)?;
        // push_to_store 内部会按 placement 写 chunk.tmp -> 校验 sha256 -> rename chunk.final -> COMPLETE
        bn.push_to_store(&linked.remote_path, store)?;

        // 2) named_store “提升”对象：ExternalLink -> Internal COMPLETE
        store.promote_obj_to_internal(&linked.obj_id)?;

        // 3) fsmeta：更新 inode 状态为 Finalized（稳定冷态）
        fsmeta.cas_state(
            file_id,
            expected=Finalizing{...},
            new=Finalized{obj_id:linked.obj_id, finalized_at:now()}
        )?;

        // 4) best-effort 清理 BufferNode 热文件
        let _ = bn.delete_buffer_file(&linked.remote_path);
        Ok(())
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
