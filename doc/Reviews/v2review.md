
## 0. 我先把系统“写冲突域”抽出来

从你描述看，真正会产生分布式写冲突/一致性风险的资源主要就三类：

1. **目录内容（dentry 集合）**

* 存在于 RDB 的 `dentries(parent,name)->target`
* 通过 `DirNode.rev` 做目录级乐观锁

2. **inode 的可写状态机（尤其是 FileBuffer 相关）**

* `NodeRecord.state` + `lease_client_session/lease_seq/expire`
* 通过“写租约 + NodeState CAS”做互斥/线性化

3. **dentry target 的类型变换（IndexNodeId ↔ ObjId ↔ Tombstone）**

* finalize / instantiate / delete / snapshot 都会碰它
* 这是“目录冲突”和“inode 状态机”交汇处，也是最容易出竞态的地方

后面识别长事务、讨论 DirObject 展开与否、列冲突点，都会围绕这三类资源展开。

---

## 1) 识别可能的长事务（含“逻辑长事务”和“数据库长事务”）

我这里把“长事务”按危害分三档：

* **A. 事务/锁持有时间长（会阻塞其它写）**
* **B. 操作扫描/遍历大（会造成 p99/p999 抖动）**
* **C. 跨服务组合步骤多（失败回滚复杂、容易留下中间态）**

### 1.1 目录 list 被用于写前去重：**高风险长事务（B）**

你明确写了：

> 添加 DentryRecord（在目录中添加一个新 item）涉及去重处理，都会需要 list
> list = `select * from dentries where parent=dirnode` 是 fsmeta 最慢操作

这在“build 大量创建小文件”的场景会非常致命：

* 热目录（比如 `bazel-out/`、`target/`）会出现**高并发 create**
* 如果每次 create 都先 list 全目录做去重，目录越大越慢，冲突越多越慢
* 最终会把“目录写”变成 **O(N)** 的扫描，尾延迟爆炸

**建议（强烈）：去重不要依赖 list，全靠 DB 唯一约束 + 条件写入。**

* 给 `dentries(parent, name)` 加 **唯一索引**
* create 走：

  * `INSERT ...`（冲突则失败或 do nothing）
  * 或 `INSERT ... ON CONFLICT DO NOTHING RETURNING ...`
* 冲突处理走“读回/返回 AlreadyExists”，完全不需要 list

> 这样你目录写冲突的控制点就从“全量 list + 客户端判断”转成“DB 原子保证唯一性”，性能和正确性都更强。

### 1.2 ensure_path（mkdir -p 语义）：**典型逻辑长事务（C + A）**

`ensure_path(path)` 的内核困难在于：

* 每个 path component 可能触发：resolve →（不存在则）create → 更新父目录 rev
* 一条很深的路径会有很多 component（比如 build 输出路径）
* 中间任何一步遇到 rev 冲突要重试；如果你试图把整条路径做成一个大事务，就会变成**长事务 + 高冲突**

**建议：确保 ensure_path 不要做成“大而全的一次性原子事务”**，而是：

* 每一层 component 是一个**短事务**（读/写父目录 + rev CAS）
* 允许部分成功（符合 mkdir -p 直觉），最终返回最后一级 dir inode
* 发生 rev 冲突只重试当前层（或从冲突层往后重试）

这会显著降低事务跨度与锁竞争。

### 1.3 rename/move（改 parent + 改两个目录 rev）：**潜在 A 类长事务 & 死锁点**

rename 需要同时改：

* 源 parent 目录：删除/替换一个 dentry + rev++
* 目标 parent 目录：创建/替换一个 dentry + rev++

如果你用数据库事务把这两个目录的 rev 更新和 dentry 修改一起做，是对的（要原子），但会带来：

* **锁多个热点行**（两个 parent 的 NodeRecord/dir rev 行 + 相关 dentry 行）
* 高并发时容易形成**死锁**（不同请求锁顺序不一致）

**建议：所有涉及多目录写的事务必须有“固定锁顺序”**
例如按 `min(parent_inode_id)` 先锁、再锁 `max(...)`，保证全系统一致。

### 1.4 finalize / snapshot：**跨服务长事务（C）**

finalize 通常包含：

* fb_service flush / close / 读 diff
* chunk hash、建 chunklist、写 named_store
* fsmeta 更新 inode state（Cooling→Linked/Finalized）
* fsmeta 更新 dentry target（IndexNodeId→ObjId）

如果你把这些步骤放在一个“持有 fsmeta 事务/锁”的大流程里，会非常危险（RPC/IO 时间不可控）。

**建议：finalize/snapshot 必须是“状态机 + 短 CAS 步骤”，中间做 IO 时不要持有 fsmeta 事务。**
典型拆分：

1. fsmeta：CAS 把 node state 从 `Cooling` → `Linking/Finalizing`（一个短事务）
2. 做 IO：hash/上传/构建对象（无 fsmeta 锁）
3. fsmeta：CAS 从 `Finalizing` → `Finalized(obj_id)` + 目录里 ReplaceTarget（短事务）

这样即使 IO 卡住，也不会把 fsmeta 写路径拖死。

### 1.5 copy_dir（递归遍历所有 dentry）：**必然长事务（B + C）**

你已经写了“建议客户端驱动”，我同意：

* 递归 copy_dir 在服务端做，天然是长耗时、失败恢复复杂、还容易占用大量内存/事务资源
* 客户端驱动 + 可中断/可续传 + 限速更靠谱

### 1.6 resolve_path_ex：**可能的长读路径（B + C）**

它本身是读操作，但如果：

* 内部需要不断展开多个 DirObject（A/B/C）
* 又伴随 symlink 展开和大量 cache 逻辑
* 还在 fsmeta 内部拉 named_store 的对象

那它会变成：

* 读路径的高尾延迟来源
* 复杂 cache 的一致性维护成本来源（尤其 symlink + overlay）

这直接关联你第 2 个问题（fsmeta 是否还要展开 DirObject）。

---

## 2) fsmeta 是否可以不再内部展开 DirObject？

结论先给：**可以，而且建议“至少不做全量展开”，最好把“DirObject 内部遍历/解析”移出 fsmeta。**
但要分清“展开”是哪一种：

* **全量展开（bad）**：把 DirObject 的 object_map 整体读出、合并成“真实 children 列表”，再继续处理
* **按需查询（good）**：只做 `get_child(name)`，没必要把整个目录孩子都展开出来

你现在的痛点主要来自前者。

---

### 2.1 最推荐的拆法：fsmeta 只做“可变层”，immutable 层交给 named_store_mgr

把路径解析（resolve）拆成两段：

#### A) fsmeta 负责的部分（可变层 / dentries）

* 只解析“当前在 RDB 里有 inode/dentry 的部分”
* 目录孩子查找优先走：

  1. `get_dentry(parent_inode, name)`（RDB，O(1)/O(logN)）
  2. 如果没命中且该目录有 `base_obj_id`（overlay base），再去 immutable 层查

#### B) named_store_mgr 负责的部分（DirObject/FileObject 只读层）

提供两个非常关键的 RPC（或本地库接口）就够了：

1. `dirobj_get_child(dir_obj_id, name) -> Option<obj_id + kind>`

* 不需要列目录，只做按名查找
* DirObject 不可变，缓存命中率会很高

2. `resolve_in_object_tree(root_obj_id, inner_path, sym_limit) -> (obj_id, inner_path_left)`

* 这对应你现在 fsmeta 里很复杂的 `resolve_path_ex` 在 “ObjId/inner_path” 那段的逻辑
* 因为 immutable，可缓存、可做前缀缓存、甚至可做内部索引结构优化

> 这样 fsmeta 不再需要“加载多个 DirObjectA/B/C 并记住它们来判断路径是否存在”，它只需要在“遇到 ObjId 时把剩余路径甩给 named_store_mgr”。

---

### 2.2 如果你希望“客户端做更多”：fsmeta 也可以直接“遇到 ObjId 就停”

你其实已经有接口形态了：

* `resolve_path_ex(...) -> (node_id | obj_id, Option<inner_path>)`

你完全可以定义一个更明确的契约：

* **fsmeta resolve 只保证解析到：**

  * 可变 inode（IndexNodeId），或
  * 一个 immutable root（ObjId）+ `inner_path_remaining`

* 剩余 `inner_path` 由客户端用 named_store 去解析（或用 named_store_mgr 提供的 resolve RPC）

这会让 fsmeta 实现大幅简化（尤其是缓存逻辑）。

**代价**：

* 客户端需要更聪明，或者你需要提供一个“组合 RPC”减少往返（例如一次调用同时走 fsmeta+named_store_mgr 解析）。

---

### 2.3 一个关键点：不展开 DirObject 之后，“写入路径”怎么处理？

最大的问题是：

> 如果某一级目录在 dentry 里是 `ObjId(DirObject)`，那它没有 inode_id，你怎么在它下面创建新的 dentry？

这里你需要一个明确机制：**实例化（instantiate）/提升（promote）**。

推荐的最小 API 形态（避免全量展开）：

* `instantiate_child_dir(parent_inode, name, expected_obj_id) -> child_inode_id`

  * 前置条件：`get_dentry(parent,name)` 目前指向 `ObjId(expected_obj_id)` 且 parent.rev 未变
  * 事务里做：

    1. 创建一个新的 DirNode inode（DirOverlay 或 DirNormal，带 base_obj_id=expected_obj_id）
    2. ReplaceTarget：把该 dentry 从 ObjId 换成 IndexNodeId
    3. parent.rev++

这样写入路径只会在“需要写的那一层”付出实例化成本，仍然不需要列目录。

---

## 3) 是否有潜在的状态冲突点？（按你三类并发控制逐个对照）

下面我按“最容易出 bug 的竞态组合”列出来，并说明你现有的 **写租约 / NodeState CAS / DirNode.rev** 是否足够，以及我建议补的约束点。

---

### 3.1 文件写租约：**“过期 writer / 重入 close”竞态**

**场景**：

* Client A 拿到 lease 开始写
* lease 过期后 Client B 拿到新 lease
* Client A 还在 flush/close，试图把状态改成 Cooling 或释放 lease

**风险**：老 writer 把新 writer 的 lease/state 覆盖掉（典型“stale close”问题）。

**你现有机制是否够？**
只靠“有 lease 字段”不够，必须把它纳入 CAS 条件。

**建议硬规则**：

* 所有“释放 lease / 改写相关 state”的操作必须 CAS on：

  * `lease_client_session` + `lease_seq`（至少要有 `lease_seq`）
  * `node_state`（期望状态）
* close 流程必须是：

  * `CAS(state==Working && lease_seq==X) -> Cooling(lease cleared, lease_seq maybe++ 或记录 last_lease_seq)`
* 任何 open_writer 必须校验：

  * `state==Working` 且 `lease_seq==当前` 且 `lease_client_session==我`

> 也就是说：**lease_seq 是防“ABA/stale writer”的核心**，一定要“每次重新授予 lease 都递增”。

---

### 3.2 finalize vs delete：**最危险的“已删除文件被 finalize 复活”**

**场景**（非常常见）：

* 一个文件 inode 正在 finalize（准备把 dentry target 换成 ObjId）
* 同时另一个客户端把这个文件删了（Delete 或 Tombstone）
* finalize 最后一步“ReplaceTarget”晚到，把 dentry 又改成 ObjId（等于复活）

**你现有机制是否够？**

* 目录 rev 能挡住一部分（如果 finalize 在 ReplaceTarget 时校验 parent.rev），但不够稳：

  * finalize 可能并不知道“目标 dentry 目前是谁”
  * 仅校验 rev 也可能被其它无关修改干扰（误失败/误重试）

**建议增加一个非常关键的约束：dentry ReplaceTarget 必须带“期望旧 target”做 CAS**

* `replace_target(parent,name, expected_target=IndexNodeId(inode_id), new_target=ObjId(obj_id), expected_parent_rev=R)`
* 如果 dentry 已经被 delete/tombstone/rename 改掉，CAS 直接失败，finalize 只能放弃（或转入 GC/孤儿对象处理）

同时，最好把“inode 绑定的 dentry”显式化（你文中也提了“双向绑定？”）：

* 在 NodeRecord 或单独表记录 `bound_parent + bound_name`（或一个 dentry_id）
* finalize 时核对绑定未变
  这能解决“inode 被 rename/move 后 finalize 写错地方”的问题（见下一条）。

---

### 3.3 finalize vs rename/move：**finalize 写错目录/错名字**

**场景**：

* inode 还是同一个，但文件被 rename/move 到另一个 parent/name
* finalize 还拿着旧 (parent,name) 去 ReplaceTarget

**风险**：

* 旧名字位置被覆盖成 ObjId（污染目录）
* 新名字位置仍指向 inode（不一致）

**解决**：

* 同上：ReplaceTarget 做 expected_target CAS
* * 维护 inode↔dentry 的绑定关系（推荐）

---

### 3.4 instantiate（ObjId→inode）vs finalize（inode→ObjId）：**目标类型来回翻**

这是你体系里很容易出现的“对冲操作”：

* 读路径为了写入：instantiate（把 ObjId 变成 inode）
* 后台为了收敛：finalize（把 inode 变成 ObjId）

如果二者并发没有互斥，会出现：

* dentry target 在 ObjId 和 IndexNodeId 之间来回切
* 甚至产生“inode 状态 Finalized 但 dentry 还是 IndexNodeId”之类的中间态

**建议**：

1. dentry ReplaceTarget 一律 CAS on expected_target（避免踩踏）
2. inode state 机里增加“中间态互斥位”（哪怕只是枚举状态）：

   * 例如 `InstantiatingDir` / `FinalizingFile`
   * 进入中间态后，另一类操作直接失败或延后

---

### 3.5 rename/move 的双目录 rev：**部分成功/死锁/覆盖**

**风险点**：

* 同时修改两个目录 rev，必须在一个事务内原子提交
* 必须固定锁顺序，避免死锁
* 目标目录同名冲突：必须由 `(parent,name)` 唯一索引兜底，不能靠 list

**推荐 rename 的原子语义**（一个事务内）：

* 校验 src dentry 仍指向 expected target（CAS）
* 校验 dest 不存在（靠唯一索引/条件插入）
* 删除/标 tombstone src dentry
* 插入 dest dentry（或 replace）
* 两个 parent.rev++（都做 expected_rev CAS）

---

### 3.6 目录 rev 的覆盖范围：**是否漏掉“语义变化但名字未变”的更新？**

你已经注意到：

* Create/Delete/Update 都需要更新 parent.rev
* Update（ReplaceTarget）也要更新 parent.rev ✅

这里再强调一次：只要影响“name->target 解析结果”的操作，都要 bump rev，包括：

* IndexNodeId ↔ ObjId
* ObjId ↔ Tombstone
* SymLink(String) 变化（如果 symlink 存在于 dentry record）

这样你的 list cache 才不会返回过期映射。

---

## 4) 我建议你把“冲突控制契约”写成三条硬约束（落到 API/SQL）

为了让实现不靠“人肉记忆”，建议你把下面三条写进接口契约（甚至体现在函数签名里）：

### 4.1 目录写：必须显式携带 expected_parent_rev

* `create_dentry(parent, name, target, expected_parent_rev)`
* `delete_dentry(parent, name, expected_parent_rev)`
* `replace_target(parent, name, expected_old_target, new_target, expected_parent_rev)`

> 不要出现“我读了 rev 但写时不带它”的隐式用法。

### 4.2 dentry ReplaceTarget：必须带 expected_old_target

这是挡住 finalize/delete/rename 并发踩踏的关键。

### 4.3 文件状态机操作：必须 CAS on (state, lease_seq)

* open_writer / close / change_state / finalize_prepare / finalize_commit
  都必须把 `lease_seq` 纳入比较条件，防 stale writer。

---

## 5) 回到你三个问题的“简明结论”

### (1) 可能的长事务

* **最危险**：目录 create 依赖 `list + 去重`（应立刻改成唯一索引+条件插入）
* **高风险**：`ensure_path`（深路径、多步写，必须拆成短事务并允许部分成功）
* **高风险**：`rename/move`（双目录写，固定锁顺序 + 单事务原子）
* **跨服务长事务**：`finalize/snapshot`（必须状态机拆分，IO 阶段不持有 fsmeta 事务）
* **必然长**：`copy_dir`（强烈建议客户端驱动）

### (2) fsmeta 是否可以不再内部展开 DirObject

* **可以，建议至少停止“全量展开”**
* 最佳实践是：fsmeta 只管可变层；immutable 的 DirObject 遍历/解析下沉到 named_store_mgr（按需 lookup / resolve_in_object_tree）
* 写入需要实例化时，用“按名实例化 + dentry CAS(expected ObjId)”解决，不用列目录

### (3) 潜在状态冲突点

* 重点冲突在：**finalize vs delete/rename/instantiate**、**lease 过期后的 stale close**、**rename 双目录写死锁**
* 你的三件武器方向正确，但要“更硬”：

  * dentry ReplaceTarget 必须 CAS on expected_old_target
  * inode 状态变更必须 CAS on (state, lease_seq)
  * rename 必须固定锁顺序 + DB 唯一索引兜底
