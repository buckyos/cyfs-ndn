# NamedFileSystem 关键操作与实现流程（v3 / Ops）

> 版本：v3（操作语义版）
> 日期：2026-07-14
> 定位：本文以**关键操作**为单位，用自然语言伪代码描述每个操作的实现流程（设计目标）。
> 本文是 normative 文档：与《NamedFileSystem_Arch_v2_Overlay_InodeDentry.md》冲突之处，以本文为准；
> 与《doc/Reviews/ndm_v2核心设计与性能分析.md》互补（该文侧重数据结构与性能假设）。
>
> 记法约定：
> - 伪代码为编号步骤 + 缩进分支，动词开头；`txn { ... }` 表示步骤在同一个 fs_meta 事务内。
> - **【现状】** 标注当前实现与设计目标的已知差距（截至 beta2.2 分支）。无标注的步骤 = 已按本文实现。
> - **【待决策】** 标注尚未定案、需要 owner 拍板的设计点。

---

## 0. 数据模型与系统不变式（操作的公共语言）

### 0.1 三平面

- **路径平面（fs_meta）**：可变命名空间。SQLite 中的 `nodes`（inode）+ `dentries`（目录项），强一致。
- **对象平面（named_store）**：不可变内容寻址存储。FileObject / DirObject / ChunkList / Chunk，`ObjId = sha256(canonical-json)`（JCS，键序确定）。
- **缓冲平面（file_buffer）**：写入中文件的可变数据（diff chunk list），物化后回收。

路径解析的产物只有三种：**Inode 引用**（可变，可进状态机）、**ObjId + inner_path**（已提交、只读，剩余路径在对象平面继续解析）、**SymLink target**（一个路径字符串）。

### 0.2 核心记录

```
nodes:    inode_id(PK), read_only, base_obj_id, state, rev(仅目录),
          lease_client_session/lease_seq/lease_expire_at(仅文件),
          fb_handle/last_write_at/closed_at, linked_*/finalized_*, ref_by, meta_json

dentries: dentry_id(PK), (parent_inode_id, name) UNIQUE,
          target_type ∈ { INODE, OBJ, TOMBSTONE, SYMLINK },
          target_inode_id / target_obj_id(OBJ 存 ObjId；SYMLINK 存目标路径), mtime

NodeState（kind 融合在 state 里）:
  目录: DirNormal | DirOverlay（带 base 的 overlay 目录，行为上与 DirNormal 等价，仅作标记）
  文件: FileNormal | Working(fb_handle) | Cooling(fb_handle) |
        Linked(obj_id, qcid, filebuffer_id) | Finalized(obj_id)
```

### 0.3 系统不变式（所有操作必须维护）

- **I1 严格树 / 无 hardlink**：一个 inode 至多被一个 INODE 型 dentry 引用。
  由部分唯一索引 `uniq_inode_target` 在 DB 层强制，并用 `nodes.ref_by`（反向指向持有它的 dentry_id）双向绑定。
  推论：从任意 inode 沿 `ref_by` 向上走，路径唯一 —— 这是 move 环检测（§3.5）与安全 finalize 的基础。
- **I2 Overlay 合并规则**：`视图 = merge(base DirObject children, upper dentries)`，同名 upper 优先；TOMBSTONE 只用于**屏蔽 base 中的同名项**。父目录无 base 或 base 无同名项时，不应产生 tombstone（直接删行）。
  **【现状】** delete/move 无条件写 tombstone，upper-only 场景会积累垃圾行。
- **I3 目录 rev 单调**：目录 inode 的每次结构变更（dentry 增/删/改）rev+1。所有 dentry 写操作携带 `expected_parent_rev` 做 CAS，失败返回 REV_MISMATCH（可重试）。
- **I4 严格单写**：文件写入必须持有绑定在 **file inode** 上的租约（session + seq + TTL）。租约过期由后台任务回收，并将 Working 状态转为 Cooling。
- **I5 committed 对象不可变**：ObjId/dentry OBJ 目标指向的内容永不改写；"修改"只能通过换绑定（新 dentry target / 新 base_obj_id）表达。
- **I6 三态查找**：对 base 的查找必须区分 `Found / Missing / NeedPull（base 对象未拉取到本地）`，禁止把 NeedPull 折叠成 Missing。
  **【现状】** named_store 的 `get_dir_child` 把"对象不在本地"和"目录中无此名"都返回 NotFound，fs_meta 一律当 Missing 处理 —— 违反 I6（NDM 层自己的 `get_dir_child` 已用 `NotReady` 区分，需下沉统一）。

### 0.4 分层与接口形态

- **fs_meta service**：本文 §1–§6 全部结构操作的宿主；持有 SQLite；通过 `with_named_store` 可读对象平面（用于 base 查找与合并）。
- **FsMetaClient**：InProcess（直连 handler）与 kRPC 两种模式。**路径级高层 API（set_file/delete/move/…）当前 InProcess-only**；kRPC 只暴露低层 inode/dentry 原语。
  **【现状】** 低层原语（create_dentry / set_inode / …）经 RPC 可直接调用，会绕过 read_only 等高层检查。设计目标：不变式检查（I1–I4、read_only）必须在 fs_meta 服务端的 dentry/inode 原语层强制，而不是只在高层入口。
- **NamedFileMgr（NDM）**：进程内编排层，组合 fs_meta + file_buffer + named_store(layout)，提供 open_reader/open_file_writer/list/publish_dir 等面向应用的 API。

---

## 1. 并发控制原语

### 1.1 事务（begin_txn / commit / rollback）

```
begin_txn():
1. 新开一条独立 SQLite 连接，执行 BEGIN IMMEDIATE（立即取全库写锁）。
2. 生成 txid，登记到事务表（含 last_used、in_flight 计数、closing 标志、touched_edges 集合）。
3. 返回 txid。后续任何带 txid 的操作路由到这条连接。

commit(txid):
1. 置 closing，等待 in_flight 归零（上限 30s）。
2. 执行 COMMIT，从事务表移除。
3. 用 touched_edges 精确失效 resolve_path 缓存（(parent_inode, name) 边集合）。

守护规则:
- TxnGuard 析构未 commit 则异步 rollback（保证任何 early-return 不留悬挂事务）。
- 后台任务每 30s 回收超时事务（默认 TTL 5 分钟）。
```

设计要点：`BEGIN IMMEDIATE` 意味着**结构写操作全局串行**。因此任何事务内步骤都禁止做慢 IO（对象加载、网络、逐子项计算）——见 §6.1 的三阶段模式。
**【现状】** publish_dir 违反此规则（§6.1）；事务 TTL 5 分钟意味着最坏情况悬挂事务可阻塞全部写入 5 分钟，需压缩或按操作类型细分 TTL。

### 1.2 目录 rev OCC（所有结构变更的写法）

```
任何 dentry 写操作(create / replace_target / delete)，在事务内:
1. SAVEPOINT。
2. 读 parent.rev，与调用方携带的 expected_parent_rev 比较，不等 → 回滚到 SAVEPOINT，返回 REV_MISMATCH。
3. （replace_target 额外）读当前 dentry target，与 expected_old_target 比较，不等 → TARGET_MISMATCH。
4. 执行行变更；维护 ref_by 双向绑定：
   - 新 target 是 inode → CAS 认领（ref_by 为空或已是本 dentry，否则 → 违反 I1，报错）。
   - 旧 target 是 inode 且被换掉 → 释放 ref_by。
5. parent.rev += 1；RELEASE SAVEPOINT。
```

跨目录操作（move）用"先按 inode_id 升序锁行 + 双 rev CAS"避免 ABBA 死锁（§3.5）。

### 1.3 文件写租约

```
acquire_file_lease(inode, session, ttl):
1. 若该 inode 的现有租约属于同 session 且未过期 → 续期，返回现有 seq。
2. 否则若无租约或已过期 → 原子抢占：写入 session，seq+1，expire=now+ttl，返回新 seq。
3. 否则 → LEASE_CONFLICT。

renew / release: 按 (session, seq) 精确匹配，防止旧持有者误操作（fencing）。

后台回收（每 30s）:
1. 扫描 lease_expire_at <= now 的 inode，清空租约；
2. 若其 state == Working → 转 Cooling（closed_at=now），使超时写入进入正常物化通道。
```

---

## 2. 路径解析

### 2.1 lookup_one(dir_inode, name) —— 单段查找（被 resolve / create 去重 / move 复用）

```
1. 查 upper: dentries(dir_inode, name)
   - TOMBSTONE  → 返回 Missing（显式删除，屏蔽 base，不再看 base）
   - INODE      → 返回 Inode(id)
   - OBJ        → 返回 Obj(obj_id)
   - SYMLINK    → 返回 SymLink(target_path)
2. upper 无此行 → 查 base:
   - dir 无 base_obj_id → 返回 Missing
   - base DirObject 不在本地 store → 返回 NeedPull        【现状】被折叠成 Missing（违反 I6）
   - base children 中无此名 → 返回 Missing
   - 命中 → 返回 Obj(child_obj_id)；若 child 是内嵌对象体，顺手 put 进 store（写副作用，见 §2.2 注）
```

### 2.2 resolve_path(path, sym_limit) → Inode | ObjId+inner_path | SymLink+tail | NOT_FOUND

```
1. 归一化 path 为组件序列；空序列 → 返回根 inode。
2. 查解析缓存:
   a. 终端缓存命中（某前缀已知解析为 ObjId/SymLink）→ 直接进入第 4/5 步的对应分支。
   b. 前缀缓存命中（最长已知 inode 链）→ 从该深度继续。
3. 从当前 inode 起逐段 lookup_one:
   - Inode(id):
     · 不是最后一段 → 继续下一段（中间段必须是目录 inode）。
     · 是最后一段 → 读 inode 记录，写前缀缓存，返回 Inode{inode_id, node}。
   - Obj(obj_id):
     · 写终端缓存；返回 ObjId + inner_path=剩余段。
       （剩余段留给对象平面：调用方用 DirObject/FileObject 的 inner-path 语义继续走，fs_meta 不展开对象内部。）
   - SymLink(target):
     · 写终端缓存。
     · sym_limit == 0 → 返回 SymLink + tail=剩余段（调用方自己决定跟不跟）。
     · 否则 sym_limit -= 1，将 target（支持相对路径, ".", ".."）在当前父前缀上归一化，
       拼接剩余段，回到第 2 步重新解析（最多 40 层）。
   - Missing → 返回 NOT_FOUND。
   - NeedPull → 返回 NEED_PULL(缺失的 obj_id)。       【现状】返回 NOT_FOUND
4. 缓存失效依据: 每次 dentry 变更记录 (parent, name) 边，提交时按边失效终端/前缀缓存。
```

注（设计约束）：解析途中允许**读** store（base 查找），且必须发生在任何 fs_meta 事务之外；"内嵌子对象 put 回 store"是当前实现为配合 `get_dir_child` 的写副作用，属可接受的幂等缓存行为，但要求 store put 幂等且失败不阻塞解析。**【待决策】** 是否把该副作用移到后台。

各入口的 sym_limit 约定：

| 入口 | sym_limit | 含义 |
|---|---|---|
| stat / open_reader | 0 | 不跟随，symlink 本身作为结果返回 |
| ensure_dir_inode（写路径的父目录解析） | 40 | 跟随 |

**【待决策】** 读路径（open_reader）目前不跟随 symlink（直接报"is a symbolic link"），写路径跟随——语义不对称。建议：默认跟随（40），另提供 no-follow 变体；定案后统一各入口。

### 2.3 stat(path)

```
1. resolve_path(path, 0)。
2. NOT_FOUND → PathStat{kind: NotFound}。
3. Inode → kind 由 state 推出（Dir*→Dir，其余→File）；
   obj_id = Linked/Finalized 的 obj_id，否则 base_obj_id；有 obj_id 时读对象取 size。
4. ObjId → kind 由 obj_type 推出；读对象取 size；带回 inner_path。
5. SymLink → kind=SymLink，target 放 obj_inner_path。
```

---

## 3. 目录结构操作

通用前置（下称"**结构写前置**"）：

```
0. 解析出 parent_inode；要求 kind==Dir；
   要求 parent 及其挂载链上无 read_only（见 §7 read_only 语义）。
   【现状】只检查直接 parent 的 read_only 位；delete 连这一步都缺失。
```

### 3.1 create_dir(path) —— mkdir -p 语义

```
client 侧 ensure_dir_inode(path)（带 8 次冲突重试）:
1. 先 resolve_path(path, 40)：已是目录 inode → 返回；是非目录 → NOT_DIR。
2. 逐段向下走，对每一段:
   - upper INODE(dir) → 进入。
   - upper OBJ(DirObject) → materialize（见下）后进入。
   - upper OBJ(非 dir)/SYMLINK → NOT_DIR。
   - upper TOMBSTONE → 在此新建目录 inode（tombstone 被覆盖，base 同名项保持被屏蔽）。
   - upper 无、base 有 DirObject 子项 → materialize 后进入。
   - upper 无、base 有非目录子项 → NOT_DIR。
   - upper 无、base 无（Missing）→ 新建目录 inode。
   - base NeedPull → 返回 NEED_PULL，由上层 pull 后重试。   【现状】当作 Missing，会在未拉取的 base 上"新建"出遮蔽目录

新建目录 inode（单事务）:
txn {
  1. 复查 (parent, name) 去重（含 base，规则同 §3.2 第 2 步）。
  2. alloc inode{state: DirNormal, rev: 0}。
  3. create_dentry(parent, name → INODE(new_id)) @ expected_parent_rev。
} commit

materialize_dir_from_obj(parent, name, dir_obj_id) —— DirObject 惰性变 inode:
txn {
  1. alloc inode{state: DirOverlay, base_obj_id: dir_obj_id, rev: 0,
                 read_only: 继承 parent.read_only}。    【现状】未继承，恒 false
  2. replace_target(parent, name: OBJ(dir_obj_id) → INODE(new_id)) @ expected_parent_rev。
} commit
不可变性说明: materialize 不复制 children（零成本），只是给这个 DirObject 套一个可写的 overlay 壳。
```

### 3.2 set_file(path, obj_id) —— 绑定已提交对象（add_file）

```
txn {
1. 结构写前置。
2. 去重 ensure_name_absent:
   - upper 有非 tombstone dentry → EXISTS。
   - upper 是 tombstone → 允许（覆盖屏蔽位）。
   - upper 无 → 查 base: Found → EXISTS；NeedPull → NEED_PULL【现状】当 Missing；Missing → 允许。
3. upsert dentry(parent, name → OBJ(obj_id)) @ expected_parent_rev。
4. obj_stat(obj_id) 引用计数 +1。（引用计数协议见 §8）
} commit
```

后续需要写这个文件时，由 open_file_writer 把 OBJ dentry 惰性升级为 file inode（§5.1）。

### 3.3 set_dir(path, dir_obj_id) —— Overlay 挂载

```
同 set_file，仅差异:
- 要求 obj_type == DIR。
- 不 materialize：dentry 直接指 OBJ(dir_obj_id)。首次在其下发生结构写/文件写时，
  沿途由 §3.1 的 materialize 惰性展开——这就是"海量目录秒级挂载"的实现方式。
- 引用计数: 挂载计 dir_obj_id 一个引用；其 children 不逐个计数（间接引用由对象平面 GC 处理，见 §8）。
  【现状】set_dir 完全没有计数（代码中留有 TODO）。
```

### 3.4 delete(path)

```
txn {
1. 结构写前置（含 read_only 检查）。               【现状】handle_delete 缺 read_only 检查
2. lookup_one(parent, name) 判定三态:
   - Missing  → NOT_FOUND（delete 不静默幂等）。    【现状】不查存在性，无条件写 tombstone
   - NeedPull → NEED_PULL。
   - upper INODE(id):
     a. 若该 inode 是文件且 state==Working（有活租约）→ PATH_BUSY。
     b. base 有同名项 → replace_target(… → TOMBSTONE)；base 无同名 → 直接删 dentry 行（I2）。
     c. 登记 inode 回收任务（后台判定无引用后 remove_inode + 释放 filebuffer）。
        【现状】b 恒写 tombstone；c 不存在，inode 成为孤儿行
   - upper OBJ / SYMLINK: 同上 b（obj 引用计数 -1，见 §8）。
   - upper 无、base 有 → create_dentry(parent, name → TOMBSTONE)（屏蔽 base，"删除回显"防护）。
} commit
```

### 3.5 move_path(old_path, new_path) —— rename / 跨目录移动

```
入口（client）:
1. 拆出 (src_parent_path, src_name), (dst_parent_path, dst_name)。
2. 解析两个父目录（dst 父目录 mkdir -p 语义：ensure_dir_inode）。
3. 【现状】client 用字符串前缀做环检查（TOCTOU、不识别 symlink、kRPC 模式整个 move 不可用）。
   设计目标：环检查移到服务端事务内（见第 8 步），client 不做。

服务端 handle_move_path:
4. 读 src_dir / dst_dir 记录: kind==Dir、无 read_only；记 src_rev0 / dst_rev0。
5. 定源 plan_move_source(src_parent, src_name):
   - upper TOMBSTONE / Missing 且 base Missing → NOT_FOUND。
   - upper INODE/OBJ/SYMLINK → MoveSource::Upper(target)。
   - upper 无、base Found(child_obj_id) → MoveSource::Base(child_obj_id)。
       【现状】未实现：base-only 条目 move 直接 NOT_FOUND（v2 文档的 O(1) base rename 缺失）
   - upper 无、base NeedPull → NEED_PULL（不持锁等待 pull，由上层 pull 后重试）。
6. 目标去重: lookup_one(dst_parent, dst_name) 非 Missing 时按覆盖语义处理
   （同名覆盖允许，等价 delete(dst)+move；目录不允许被非空覆盖）。
   【待决策】当前实现允许直接覆盖任意已有目标；是否禁止覆盖非空目录需定案。
7. txn {
   a. 复读两 parent 的 rev，与 rev0 比较，不等 → REV_MISMATCH（放弃，调用方重试）。
   b. 按 inode_id 升序对两 parent 做行级锁定（防 ABBA）。
   c. 环检测（source 是目录 inode 时）:
      从 dst_parent 沿 ref_by → dentry.parent 逐级上行至根（I1 保证路径唯一，O(深度)），
      途中遇到 source inode → INVALID_MOVE。同时覆盖 source==dst_parent 的直接自嵌。
      【现状】只挡直接自嵌；把 /a move 进 /a/b/c 会造成子树脱环离根
   d. 源侧: base 有同名(或源自 base) → 写 TOMBSTONE(src_parent, src_name)；否则直接删行（I2）。
   e. 目标侧: upsert dentry(dst_parent, dst_name → 源 target) 
      @ expected_rev = (同目录 ? src_rev0+1 : dst_rev0)。
      源自 base 时 target = OBJ(child_obj_id)（纯元数据 O(1)，与子树规模无关）。
   } commit
8. INODE 源在整个流程中 file_id 不变（I1 下 ref_by 换绑），对上层不可感知。
```

### 3.6 symlink(link_path, target_path)

```
txn {
1. 结构写前置 + 去重（同 §3.2 第 2 步）。
2. create_dentry(link_parent, link_name → SYMLINK(target_path 原样字符串，可相对))。
} commit
解析语义见 §2.2；上限 40 层；目标不存在不报错（悬挂链接合法）。
对象化语义见 §6（当前 DirObject 无法表达 symlink —— 未决问题）。
```

---

## 4. 目录视图：list

### 4.1 start_list(path) / list_next(session, page_size) / stop_list(session)

```
start_list(path):
1. resolve_path(path, 0):
   - Inode(dir) → 走第 2 步（overlay 目录）。
   - ObjId(DirObject)（纯对象目录，未 materialize）→ 加载 DirObject，
     children 直接构造只读视图，建本地 session，返回。
   - 其他 → NOT_FOUND / NOT_DIR。
2. overlay 目录（合并只做一层，产出按 name 字节序的有序快照）:
   a. upper = 全量 dentries(dir)（稀疏，通常远小于 base）。
   b. base_iter = base DirObject children（无 base 则为空）。
   c. merge: 同名 upper 覆盖 base；TOMBSTONE 从结果剔除；
      INODE 条目附带 inode 记录（批量读取，禁止每条目一次点查）。
      【现状】每个 INODE 条目一次 get_inode；且 fs_meta 与 NDM 各自做了一遍完整 merge
      （fs_meta.start_list 已合并，NDM 再拉全量重新合并）——双倍内存与解析开销，需去掉一层。
   d. 建 session{快照, cursor}，返回 session_id。
3. session 生命周期: TTL（如 60s 不活动）+ 总数上限，超限逐出最旧；stop_list 显式释放。
   【现状】无 TTL 无上限，忘记 stop 即永久泄漏

list_next(session, page_size):
1. 取 session；从 cursor 之后按 name 字节序取 page_size 条（page_size==0 表示全部）。
2. 更新 cursor 为最后一条的 name；返回条目（name → PathStat/entry）。
   （cursor 基于 name 而非偏移，快照有序 → 翻页稳定，可安全用作断点续传。）
```

**【待决策】（架构级）** 本节描述的是"会话快照"模型（现状）。它的负载与 base 目录规模成正比，与 v2 文档"fs_meta 只返回稀疏增量、NDM 流式合并"的目标冲突。若 base 目录规模要支持百万级，必须改为：fs_meta 只回 upper + base_obj_id，NDM 用 DirObject 的**有序流式迭代**做 merge-join —— 而这又依赖 DirObject 引入可流式的编码（见 §9 附录 A）。短期修正：去掉双重合并、补 session TTL/上限；长期按上述方向重构。

---

## 5. 文件读写（staged commit 生命周期）

文件 inode 状态机（设计目标全链路）：

```
(new)──open_file_writer──▶ Working ──close──▶ Cooling ──debounce+hash──▶ Linked ──chunk迁移──▶ Finalized
                             ▲  │                │                          │
                             │  └─lease 超时─────┘（后台回收，转 Cooling）    │
                             └────ContinueWrite（断点续写，Cooling→Working）──┘
```

- **Working**：数据在 file_buffer（base chunklist + diff）。
- **Cooling**：writer 已关，防抖等待期（短时间内再写则回 Working，避免反复物化）。
- **Linked**：内容哈希已定（FileObject/ChunkList 已生成并写入 store），chunk 数据仍可能由 filebuffer 经 ExternalLink 提供；qcid 用于 ExternalLink 访问前快检。**内容寻址自此稳定。**
- **Finalized**：chunk 全部迁入内部 store，filebuffer 可回收。

**【现状】** Working/Cooling 及租约回收已实现；**Cooling→Linked→Finalized 的后台推进器未实现**（BackgroundMgr 有 FinalizeDir/LazyMigration 队列骨架，但执行器从未接线启动）。当前 Linked/Finalized 状态只能由测试或外部调用 update_inode_state 人为驱动。这是文件平面最大的未完成项。

### 5.1 open_file_writer(path, flag, expected_size) → fb_handle

```
1. 解析 parent（ensure_dir_inode，mkdir -p）。
2. txn {
   a. 结构写前置（parent kind / read_only）。
   b. lookup_one(parent, name) 并按 flag 校验存在性:
      Append/ContinueWrite 要求存在；CreateExclusive 要求不存在；CreateOrTruncate/CreateOrAppend 皆可。
      base NeedPull → NEED_PULL。                       【现状】当 Missing
   c. 归一化出 file inode（"要写谁"）:
      - upper 无 & base 无        → alloc inode{FileNormal}，dentry → INODE。
      - upper 无 & base 有文件对象 → alloc inode{FileNormal, base_obj_id=该对象}，dentry → INODE
                                     （写时惰性 materialize，保留 base 供 append/diff）。
      - upper TOMBSTONE           → 视为新建（同第一种）。
      - upper OBJ(file)           → alloc inode{base_obj_id=obj}，replace_target → INODE。
      - upper OBJ(dir)/base 目录  → IS_DIR。
      - upper SYMLINK             → IS_SYMLINK（写入不跟随，见 §2.2 待决策）。
      - upper INODE(file):
        · state==Working: ContinueWrite → 允许；其他 flag → PATH_BUSY。
        · state==Cooling: ContinueWrite/Append 等 → 允许（回 Working）。
   d. 按 flag 准备内容基线:
      - Truncate 类: 清 inode.base_obj_id，空 chunklist 起写。
      - Append 类:  从 base FileObject 解出既有 chunklist 作为 diff 基线。
        【现状】fs_meta 内的 load_file_chunklist 是空实现（append 于已提交文件=从头写）；
                NDM 层的实现是完整的，需要收敛到一处
      - ContinueWrite: 复用既有 fb_handle（buffer 必须仍在，否则报错）。
   e. acquire_file_lease(file_inode, session=instance:file_id, TTL 5min)（§1.3）。
   f. 非续写: buffer.alloc_buffer(file_id, 基线 chunklist, lease, expected_size) → fb_handle。
   g. update_inode_state(→ Working{fb_handle}, CAS on 旧 state)。
   } commit
3. 返回 fb_handle；NDM 侧据此构造 DiffChunkListWriter（base chunklist + diff 文件）。
```

### 5.2 write / flush（NDM，不经 fs_meta）

```
write_all/seek: 写 DiffChunkListWriter（按固定 chunk 大小切块，重写部分进 diff 文件，
                未触碰部分继续引用 base chunk —— 大文件小改动的存储成本 ≈ 改动量）。
flush:
1. writer.close() 得到 writer_state（chunk 布局、总大小、位置）。
2. writer_state 写回 filebuffer 的 diff_state 并持久化（buffer.flush）。
   —— 崩溃后可从 diff_state 重建 writer（ContinueWrite 路径）。
```

### 5.3 close_file_writer(file_inode)

```
txn {
1. 读 inode，要求 kind==File，state ∈ {Working, Cooling}，取 fb_handle。
2. buffer.close(fb)（停止写入接受，数据保留）。
3. update_inode_state(→ Cooling{fb_handle, closed_at=now}, CAS)。
} commit
4. 事务外 release_file_lease(session, seq)。
```

### 5.4 后台物化（设计目标；未实现，见上）

```
物化扫描（周期任务，Cooling 且 closed_at 超过防抖窗口，如 30s）:
1. 读 filebuffer 内容，计算 ChunkList 与各 chunk id；生成 FileObject{size, content=chunklist_id}。
2. 计算 qcid（快速校验哈希）。
3. put FileObject / ChunkList 对象进 store（chunk 数据先不动，通过 ExternalLink 指向 filebuffer）。
4. update_inode_state(Cooling → Linked{obj_id, qcid, filebuffer_id}, CAS；
   若期间被 ContinueWrite 抢回 Working，CAS 失败即放弃本轮)。

迁移扫描（LazyMigration，Linked 状态且空闲）:
5. 逐 chunk 从 filebuffer 拷入内部 store（幂等，可断点）。
6. 全部落定 → update_inode_state(→ Finalized{obj_id})；释放 filebuffer；解除 ExternalLink。
```

### 5.5 open_reader(path) → (reader, size)

```
1. resolve_path(path, 0):
   - SymLink → IS_SYMLINK（跟随策略待定，§2.2）。
   - ObjId + inner_path → 对象平面: layout_mgr.open_reader(obj_id, inner_path)
     （DirObject 沿 inner_path 下钻到 FileObject → ChunkList → chunk 流；支持多版本 layout 回退，最多 2 版）。
   - Inode(file) 按 state:
     · Working/Cooling → 从 filebuffer 构造 DiffChunkListReader（base chunklist ⊕ diff，读到未 close 的最新数据）。
     · Linked/Finalized → 按 obj_id 走对象平面。
     · FileNormal 且有 base_obj_id → 按 base_obj_id 走对象平面。
     · 其他 → NO_CONTENT。
2. 数据不在本地且配置了 fetcher → 触发 pull（NEED_PULL/NotReady 上抛，调度拉取后重试）。
   【现状】NDM 的 pull 接口整体被注释未启用
```

---

## 6. 目录对象化（把 overlay 目录变成 DirObject）

两个不同目的的操作，**不可混用**：

| | publish_dir（发布快照） | finalize_dir（元数据瘦身） |
|---|---|---|
| 目的 | 得到当前目录树的不可变 DirObjectId（分享/同步/备份） | 冷子树收缩 fs_meta 元数据 |
| inode | **保留**（base_obj_id 刷新 + upper 清空） | **删除**（父 dentry 改指 OBJ，整棵 inode 子树移除） |
| file_id 稳定性 | 保持（I1 意义下路径→inode 不变） | **丧失**（再次写入时 materialize 出新 inode_id） |
| 现状 | 已实现（NDM），但持锁模式违规（见下） | 代码在 fs_meta 但 dead_code，后台队列未接线 |

### 6.1 publish_dir(path) → DirObjectId —— 三阶段短锁（设计目标）

```
阶段 1 读取（无锁）:
1. resolve 到 dir inode；记 rev0 与 base_obj_id0。
2. 快照 upper dentries。

阶段 2 计算（无锁，可长耗时）:
3. 前置: 所有 INODE 子项必须已有稳定 obj_id 且无未发布的下层变更 ——
   - 子文件: state ∈ {Linked, Finalized}（否则该子项 PENDING）。
   - 子目录: 递归自底向上先 publish（否则 PENDING）。
     子目录是 DirOverlay 且 upper 非空时，绝不允许拿它的旧 base_obj_id 充数。
     【现状】直接取子目录旧 base_obj_id → 陈旧快照 + 后续第 7 步删 dentry 导致未发布变更永久丢失（数据丢失级缺陷）
   有 PENDING → 把 PENDING 子项与本目录压入后台发布队列，返回 PUBLISH_PENDING。
4. merge(base children, upper)（规则同 I2），生成新 DirObject（children 一律以 ObjId 形式引用；
   size/count 统计尽量取自子对象元数据缓存，避免逐子项加载）。
5. gen_obj_id → new_dir_obj_id；put 进 store（幂等）。
6. symlink 子项: 【待决策】DirObject 尚无 symlink 条目类型。定案前，含 SYMLINK dentry 的目录
   publish 必须报 UNSUPPORTED_SYMLINK 而不是静默丢弃。
   【现状】静默跳过，publish 后 symlink 彻底消失（第 7 步还会清掉 upper 里的 symlink dentry）

阶段 3 提交（单事务，毫秒级）:
txn {
7. CAS: dir.rev == rev0（期间目录变过 → REV_MISMATCH，回阶段 1 重试）；read_only 检查。
8. dir.base_obj_id = new_dir_obj_id；state=DirOverlay；rev+1。
9. 一条语句清空该目录全部 upper dentries（DELETE WHERE parent=dir）。
   被清 dentry 指向的 inode 登记回收（文件 inode 已 Linked/Finalized，目录 inode 已递归发布为空壳）。
   【现状】逐条 delete_dentry（O(N) 语句），且不回收 inode（孤儿泄漏）
} commit
10. 返回 new_dir_obj_id。

【现状】最严重差距: 当前实现把阶段 1、2 全部塞进 BEGIN IMMEDIATE 事务里
（含全量 DirObject 加载、逐子项 store 读、put_object），大目录 publish 期间全库写阻塞——
rev0 CAS 机制已经在位，改造只需把 begin_txn 移到阶段 3。
```

### 6.2 finalize_dir(path)（未启用；启用前需过一遍本节约束）

```
1. 前置同 publish 阶段 2（全部子项已稳定），否则子项入队、本目录挂起。
2. txn（CAS parent.rev 与 dir.rev）{
   a. 生成/复用 DirObjectId（同 §6.1）。
   b. replace_target(parent, name: INODE(dir) → OBJ(dir_obj_id))。
   c. 删除该目录全部 dentries 与全部子 inode，最后删本 dir inode。
   } commit
3. 约束: 仅适用于确认冷的子树（file_id 不再需要稳定）；
   read_only 子树是天然候选。绝不能对活跃目录自动触发。
```

---

## 7. read_only 语义（设计目标）

```
1. read_only 是目录 inode 上的位，含义: 以该目录为根的子树禁止一切结构变更与文件写
   （create/delete/move/symlink/set_*/open_file_writer/publish 的提交阶段均须检查）。
2. 传播规则: materialize 子目录时继承父的 read_only（§3.1）；
   显式设置入口: set_dir(path, obj_id, read_only=true)（只读挂载）或 snapshot(src, target)（复制后置只读）。
3. 检查位置: fs_meta 的 dentry/inode 写原语层（保证 kRPC 低层调用也受约束），
   检查的是"直接 parent"位 —— 配合传播规则等价于子树语义，无需沿途上溯。
4. 解除: 仅允许对子树根显式解除（管理操作）。

【现状】与目标差距: 没有任何入口能置 read_only=true（snapshot 的 readonly 选项被丢弃、
mount 无参数）；materialize 不继承；handle_delete 不检查；低层 RPC 全不检查。
即 read_only 目前是"有列无语义"。
```

---

## 8. GC 与引用计数（obj_stat）

```
原语（已实现）: obj_stat_bump(obj_id, ±n)（0 时记 zero_since）/ obj_stat_get /
              obj_stat_list_zero(阈值) / obj_stat_delete_if_zero。

引用计数协议（设计目标，【待决策】）:
+1 的时机: dentry 产生 OBJ 目标（set_file/set_dir/move 源自 base/发布后 base_obj_id）
-1 的时机: 该 OBJ 目标被覆盖 / tombstone / 删行；inode.base_obj_id 被替换或清除
不计数:    DirObject 内部 children（间接引用——对象平面 GC 从根集合可达性判定）

【现状】只有 set_file 做了 +1，所有减引用路径缺失 → 计数只增不减，zero 集合永远空，
按 zero_since 的 GC 实际不可能触发。姊妹文档已提出把 ObjStat 迁回 named_store 由
store_mgr 统一管理 —— 若采纳，本节协议整体移交对象平面，fs_meta 不再维护计数。
```

---

## 9. 错误码规范（设计目标）

所有对外操作返回下列**结构化**错误之一（跨 kRPC 序列化保留错误类别）：

| 错误 | 含义 | 调用方动作 |
|---|---|---|
| NOT_FOUND | 名字确定不存在 | — |
| NEED_PULL(obj_id) | base 对象未拉取，无法判定 | pull 后重试 |
| REV_MISMATCH | 目录 rev CAS 失败 | 直接重试 |
| EXISTS | 创建目标已存在 | — |
| PATH_BUSY / LEASE_CONFLICT | 写租约被他人持有 | 等待/抢占策略 |
| READ_ONLY | 只读子树 | — |
| NOT_DIR / IS_DIR / IS_SYMLINK | 类型不符 | — |
| INVALID_MOVE | move 成环/自嵌 | — |
| PUBLISH_PENDING | 子项未就绪，已入队 | 稍后重试/订阅完成 |
| UNSUPPORTED | 该客户端模式不支持 | 换入口 |

**【现状】** 全部是 `ReasonError(字符串)`，client 靠 `msg.contains("rev mismatch")` 判断可重试——跨版本极脆，是错误处理层第一优先级整改项。

---

## 附录 A：已知架构级未决问题（超出单个操作）

1. **DirObject 单体 JSON vs 海量目录**：单次 base 子项查找/合并都要全量加载解析 DirObject（O(N)）。
   §2/§4/§6 的流式假设（`children_iter_sorted`）需要 DirObject 分块/有序编码支持（参见《对象大容器需求草案》）。定案前，base 目录规模有实际上限（建议文档化一个数值，如 10 万条目）。
2. **list 架构二选一**（§4.1 待决策）。
3. **fs_meta 的 Raft 化路线 vs 当前"fs_meta 内做 store IO"的耦合**：resolve/list/publish 依赖 named_store 读写，状态机确定性要求这些 IO 全部移出共识路径（挪到 NDM 或预取层）。
4. **symlink 的对象平面表达**（§6.1 第 6 步）。
5. **路径级 API 的 kRPC 化**：目前 set_file/delete/move/… 仅 InProcess；远程部署要么补齐 RPC（服务端已有同名 handler，主要是补 dispatch 与错误码），要么明确远程只走低层原语 + 客户端编排（则低层原语必须补不变式检查，见 §0.4）。

## 附录 B：与实现的差距清单（按风险排序，供排期）

| # | 差距 | 风险 | 涉及 |
|---|---|---|---|
| 1 | publish_dir 长事务 + 子目录陈旧快照 + symlink/upper 丢失 + 孤儿 inode | 全库写阻塞；**数据丢失** | §6.1 |
| 2 | NeedPull 被折叠成 Missing | 假 NOT_FOUND、误建遮蔽项 | I6, §2, §3 |
| 3 | move 无服务端环检测 | 子树脱环离根（结构损坏） | §3.5 |
| 4 | 文件后台物化链路未接线 | Cooling 永不 Linked/Finalized，buffer 永不回收 | §5.4 |
| 5 | 引用计数只增不减 | GC 永不触发 | §8 |
| 6 | list 双重合并 + session 无 TTL | 内存泄漏、双倍开销 | §4.1 |
| 7 | read_only 无语义 | snapshot/只读挂载承诺落空 | §7 |
| 8 | base-only rename 未实现 | 挂载目录 rename 报 NOT_FOUND | §3.5 |
| 9 | delete 无条件写 tombstone、不查存在、不回收 inode | 垃圾行/孤儿 inode 累积 | §3.4 |
| 10 | 错误码字符串匹配 | 跨版本脆弱 | §9 |
| 11 | symlink 读写跟随策略不一致 | 行为不可预期 | §2.2 |
| 12 | 结构化错误/低层 RPC 绕过不变式 | 远程调用可破坏 read_only/树约束 | §0.4 |
