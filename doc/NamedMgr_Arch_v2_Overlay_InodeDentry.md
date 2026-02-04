# NamedMgr / NamedDataMgr 架构设计说明（Overlay + Inode/Dentry 版）

> 版本：v2（基于《named_mgr discuss.md》与《new discuss.md》的演进，并吸收你在对话中明确的选择）  
> 日期：2026-02-03  
> 目标：在保持“非 POSIX、严格单写、内容寻址、Zero-op 可用”的总体风格不变的前提下，引入 **Overlay 目录视图** 与 **Inode/Dentry 元数据模型**，解决目录 rename 的可扩展性、超大目录挂载与“目录也走 staged commit”的一致性。

---

## 0. 系统契约（继承 + 修订）

本节在原“系统契约”基础上补充/修订目录与元数据规则；其余条款（内容寻址、placement 回退、qcid、默认不自动 GC、严格单写等）保持不变。

### 0.1 内容寻址与对象协议强规范（不变）
- ObjId / ChunkId 采用 `sha256:xxxx`，对象 canonical encoding / 类型/版本由 cyfs:// 协议规范强约束。

### 0.2 Path 语义不是 POSIX（不变）
- NamedMgr 提供“类 FS API”，一致性/原子性/并发语义以本文档为准，不得按 POSIX 推断。

### 0.3 严格单写（不变，但新增 inode 视角）
- **文件写入**仍遵循“单写会话（session）持有写租约”的规则；冲突返回 `PATH_BUSY / LEASE_CONFLICT`。
- 新增：写租约绑定在 **File Inode（file_id）** 上；path 仅是到 inode 的名字解析结果。

### 0.4 目录挂载语义（重大修订）
- **默认：Overlay 挂载**。目录由两层组成：
  1) Base Layer：一个可选的 `DirObjectId`（不可变、可来自远端/快照）
  2) Upper Layer：fs-meta-db 中的 `dentries` 增量（可变，稀疏）
- **例外：ReadOnly 挂载**。当用户以 `read_only` 方式挂载目录时：
  - 该目录及其子树禁止任何结构性变更（create/delete/rename/move/bind）。
  - 该模式下可恢复旧文档中“挂载互斥”的直觉：子树完全由 DirObject 决定（禁止 overlay 覆盖）。

> 说明：Overlay 用“优先级规则”解决冲突；ReadOnly 用“禁止冲突”获得最强的可预期只读行为。

### 0.5 list 接口（修订：去掉 pos/page_size）
- 对外的 `list(path)` 不再由 fs-meta-service 做分页；合并发生在 NDM。
- fs-meta-service 只返回 **稀疏增量（dentries）+ base_dir_obj_id + mount_mode**，其负载与目录规模（base children 数量）无关。

---

## 1. 关键术语（新增/更新）

- **Inode / FileID（file_id）**：fs-meta-db 内部的永久身份标识（递增 u64），目录 rename 不会改变 file_id。
- **Dentry**：`(parent_file_id, name) -> target` 的“目录项”。
- **DentryTarget（策略 B）**：dentry 的 target 允许两种形式：
  - `Target::FileId(file_id)`：指向一个 inode（可进入 working/lease 等状态机）
  - `Target::ObjId(obj_id)`：直接指向一个已提交对象（视为 committed，只读、不可持有写租约）
- **Tombstone / Whiteout**：一种特殊 dentry 标记，用于在 Overlay 中“显式删除/屏蔽”Base Layer 的同名项。
- **Base Layer**：目录 inode 上记录的 `base_dir_obj_id`（可空，空表示 base 为空目录）。
- **Overlay View（目录视图）**：`View = merge(BaseDirObject.children, UpperDentries)`，且 Upper 优先。
- **MountMode**：`overlay | read_only`。
- **DirRev（目录修订号）**：目录 inode 的 revision（用于乐观提交/避免长持锁）。

---

## 2. 主要组件总览（保持原风格）

- **named_mgr / NamedDataMgr（NDM）**：对外“类 FS API”，负责：
  - path 解析、Overlay 合并、读写语义与 staged commit orchestration。
- **fs_meta（service）**：强一致元数据域（单机 SQLite / 未来 klog+Raft），负责 inode/dentry/lease/mount_mode/rev。
- **named_store**：对象与 chunk 的 CAS 存储（ObjId/ChunkId），提供 open_reader_by_id/get_object 等绕过 path 的访问。
- **store_layout（placement）**：按 ObjId/ChunkId 选择存储目标，读支持最多回退 2 个版本。
- **file_buffer（buffer service）**：文件工作缓冲（单机 mmap / 多节点 GFS-like buffer nodes 可演进）。

---

## 3. fs-meta-db 数据模型（v2：inode + dentry + overlay）

### 3.1 设计目标
1) 目录 rename/move：O(1) 元数据更新（不随子树规模增长）。  
2) 支持“海量目录秒级挂载”：Base children 不灌 DB，仅记录稀疏增量。  
3) 目录与文件统一 staged commit：目录的 delta 可对象化为新的 DirObject。

### 3.2 表结构（建议最小集）

#### nodes（inode）
- `file_id INTEGER PRIMARY KEY`
- `kind ENUM('file','dir')`
- `base_obj_id TEXT NULL`  
  - file：已提交 FileObjectId  
  - dir：已提交 DirObjectId
- `state ENUM('committed','working','cooling','linked','finalized', ...)`（文件可用；目录通常为 committed）
- `mount_mode ENUM('overlay','read_only')`（仅 dir）
- `rev INTEGER`（仅 dir，目录变更 +1）
- `lease_session TEXT NULL, lease_fence INTEGER, lease_expire_ts INTEGER`（文件单写；目录短锁/可选）

#### dentries（upper layer / sparse delta）
- `parent_file_id INTEGER`
- `name TEXT`
- `target_type ENUM('file_id','obj_id','tombstone')`
- `target_file_id INTEGER NULL`
- `target_obj_id TEXT NULL`
- `mtime INTEGER`（可选）
- `PRIMARY KEY(parent_file_id, name)`
- 索引建议：
  - `INDEX(parent_file_id, name)`
  - `INDEX(target_file_id)`
  - `INDEX(target_obj_id)`（若需要反查/清理）

> Check 约束（逻辑约束）：
> - target_type='file_id' => target_file_id NOT NULL AND target_obj_id IS NULL  
> - target_type='obj_id' => target_obj_id NOT NULL AND target_file_id IS NULL  
> - target_type='tombstone' => target_file_id IS NULL AND target_obj_id IS NULL

### 3.3 路径解析与目录视图

#### 3.3.1 单段查找（lookup_one）
给定目录 inode（dir_id）与 name：

1) 先查 `dentries(dir_id, name)`：
- tombstone => NOT_FOUND（显式删除/屏蔽 base）
- file_id => 返回 inode 引用
- obj_id => 返回对象引用（committed）

2) 若 dentry 不存在：查 base layer（DirObject.children）：
- 若 base_dir_obj_id 为空 => NOT_FOUND
- 若 base 未 pull => NEED_PULL（建议不要返回“空”，避免误导）
- 若 base 有该 name => 返回对象引用（committed，obj_id）

> 注意：策略 B 使得“从 base 得到的条目”不必立刻 materialize 成 inode。

#### 3.3.2 list（目录迭代）= merge join
NDM 负责合并：
- `base_iter = DirObject.children_iter_sorted()`
- `upper = fsmeta.list_dentries(dir_id)`（通常稀疏，小到可全量读入内存 map；若很大也可做流式）

合并规则：
- 同名：upper 覆盖 base
- tombstone：从最终结果移除该 name
- 结果输出建议保证稳定排序（按 name 的字节序/规范化规则，需明确）

### 3.4 Overlay 下的 delete/rename 规则（关键）

- **删除（delete name）**：
  - 写入 `dentries(parent, name, tombstone)`（而不是删除行）
  - 目的：屏蔽 base 中同名项，避免“删除回显”

- **rename base-only 条目**（旧名仅存在于 base，不存在于 upper）：
  - `INSERT dentries(parent, new_name, Target::ObjId(child_obj_id))`
  - `INSERT dentries(parent, old_name, Tombstone)`
  - 这使 rename 成为纯元数据 O(1)，且不需要把 base children 导入 DB

- **rename upper-only 条目**：
  - 直接更新 dentry 的 name（或 delete old + insert new，取决于实现/约束）
  - 若 target 是 file_id，可保持 inode 不变

---

## 4. 目录对象化（cacl_dir）：目录的 staged commit

### 4.1 触发场景
- 发布目录快照（生成新的 DirObjectId）
- 清理 tombstone/增量（让目录 delta 回到稀疏状态）
- 与同步/备份工具协作：以不可变对象形式导出

### 4.2 基本流程（推荐“短持锁 + rev 校验”）
为了避免在超大目录上长时间锁住目录：

1) **读取阶段（不持目录锁）**
- 读取 `dir.base_dir_obj_id`
- 读取 `dir.rev`（记为 `rev0`）
- 读取 `upper_dentries`（快照）

2) **计算阶段（不持目录锁）**
- 以 base_iter（DirObject children streaming）+ upper_map 合并
- 生成新的 DirObject（保持排序/去重）
- 得到 `new_dir_obj_id`

3) **提交阶段（短持锁，CAS）**
- 开启事务：检查 `dir.rev == rev0` 且 `mount_mode != read_only`
- 写入 `dir.base_dir_obj_id = new_dir_obj_id`
- 清空该目录的 upper_dentries（或仅清理已应用条目，视实现）
- `dir.rev += 1`
- 事务提交

若 rev 不匹配，说明目录在你计算过程中发生了变更：放弃提交，重试即可。

---

## 5. 对外接口（更新点）

> 下述接口沿用原文档的“按 FS 接口理解 NamedMgr”风格，只列出与本次变更相关的部分；对象存储/placement/qcid/erase/GC 等条款不变。

### 5.1 目录挂载与 bind
- `add_dir(path, dir_obj_id, mount_mode=overlay|read_only)`
  - overlay：允许后续在该目录上产生 upper dentries
  - read_only：禁止任何结构变更；list/open_reader 仍可用（若 base 已 pull）

- `add_file(path, obj_id)`
  - 推荐实现为：在父目录写入一个 dentry（Target::ObjId 或 Target::FileId 皆可）
  - 若需要后续写入/lease：应为该 path 分配 inode，并让 dentry 指向 file_id

### 5.2 list（无 pos/page_size）
- `list(path) -> DirIter`
  - NDM 内部：
    - 获取 dir inode meta（base_obj_id, mount_mode）
    - 拉取 upper dentries（稀疏）
    - base children 用 DirObject streaming
    - 输出 merge 后的迭代器

### 5.3 结构性修改（create/delete/rename/move）
- 这些操作本质都落到“父目录 inode + dentry ops”的事务上。
- 在 overlay 挂载下：
  - 允许覆盖 base
  - 删除用 tombstone
- 在 read_only 挂载下：
  - 全部失败（READ_ONLY）

---

## 6. 目录 lease（短锁）讨论：为什么需要、以及隐含问题

### 6.1 结论先行
- **是的，目录锁/lease 的理想持有时间应该非常短**：单次结构操作基本就是一次 fsmeta 事务，通常是毫秒级。
- 但是有两个“隐含的长耗时路径”必须被设计规避，否则会把短锁变成长锁：
  1) `cacl_dir`（大目录对象化）
  2) “需要读取 base DirObject 才能决定”的操作（base 未 pull 时）

### 6.2 为什么目录也需要并发控制
即便你整体是“非 POSIX”，目录结构也需要至少满足：
- rename/move 的原子性（跨目录）
- create 的去重（同名冲突）
- delete 与 rename 的一致性（不出现幽灵条目/双份条目）

在单机 SQLite 场景下，DB 的事务锁已经能提供串行化；但为了未来的 Raft/klog，以及为了把语义从“隐式 DB 锁”升级为“显式协议”，建议保留“目录 lease/短锁”的抽象。

### 6.3 建议的协议形态（最小可演进）
- 对目录 inode 维护：`dir.rev`（修订号） + （可选）`dir_lease_session/fence/ttl`
- 修改目录时：
  - Either：显式 AcquireDirLease（返回 fence），带 fence 写入
  - Or：直接用 `expected_rev` 做 OCC（推荐更轻量，锁持有更短）

### 6.4 隐含问题 A：跨目录 move/rename 的死锁
跨目录操作需要同时修改 source_dir 与 target_dir。
- 解决方式：规定锁顺序（按 file_id 升序获取），拿不到就回退重试。
- 在 OCC 模式：同时携带两个 dir 的 expected_rev，事务内 CAS 更新，失败则重试。

### 6.5 隐含问题 B：base 未 pull 导致的“等待 IO”
有些操作（例如 rename base-only 条目）需要知道 base 中该 name 指向哪个 obj_id。
- 若 base 未 pull，此时不应在持有目录锁时触发 pull/等待 IO。
- 建议语义：直接返回 `NEED_PULL`，由上层先 pull，再重试 rename。

### 6.6 隐含问题 C：cacl_dir 的长耗时
- 规避方式：使用 §4.2 的“短持锁 + rev 校验”两阶段提交。
- 这使目录锁持有时间稳定为“提交事务”的毫秒级，而不是随目录大小增长。

---

## 7. 与旧“挂载互斥”规则的关系（明确替代）

- 原规则：若已有显式子绑定，则禁止父目录绑定到会产生冲突子树的 DirObject。
- 新规则：
  - **Overlay 挂载**：允许冲突，按“Upper 优先 + tombstone”规则消解。
  - **ReadOnly 挂载**：禁止冲突（子树只读且互斥），维持最强可预期性。

---

## 8. 需要实现的关键测试（v2 必测）

1) **目录 rename O(1)**：子项数量从 1 到 1,000,000，rename 写入规模恒定。  
2) **删除回显**：base 有 `a`，写 tombstone 后 list 不出现 `a`。  
3) **base rename**：base 有 `a`，rename 为 `b` 后 list 只见 `b`。  
4) **跨目录 move 原子性**：不会出现“双份/丢失”的中间态。  
5) **read_only mount**：任何结构修改返回 READ_ONLY；list/open_reader 正常。  
6) **cacl_dir 并发**：计算期间目录发生变更，提交必须失败并可重试。  
7) **NEED_PULL 路径**：base 未 pull 时 rename base-only 条目返回 NEED_PULL，而不是卡住目录锁。  

---

## 9. 兼容性说明（保持不变的部分）
- placement 版本回退最多 2 次、qcid 仅用于 ExternalLink 访问前快检、默认不启用自动业务数据删除、erase_obj_by_id 的语义等均保持原文档定义。
