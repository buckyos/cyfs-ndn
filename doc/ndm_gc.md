# NDM GC 整合方案

## 0. 与 `named_store_gc.md` 的关系

`doc/named_store_gc.md` 已经定型为 `named_store` 内部的"缓存优先 + Pin / fs_anchor 即持久化"模型。本文档**不重复**那里描述过的机制——cache 三档存活、`incoming_refs` 边集、`edge_outbox` 异步 cascade、shadow 对象、Skeleton 前向拦截、Maglev 分片、SameAs 大 chunk、强制 GC 红线、`last_access_time` 与 LRU、布局迁移……都直接引用，不再展开。

本文档的工作是把这套底层模型与 `fs_meta` / `ndm` 的整合点对齐：

- **`named_store` 给的存活语义只有三档**：class 0 cache（普通 `put`，O(1)，无引用记账）、class 1 referenced（被某个 cascade 根持有的子树成员）、class 2 anchored（自身有 `pins` 行 *或* `fs_anchor_count > 0`）。普通 `put` 不写 `incoming_refs`、不写 `edge_outbox`、不触发 cascade。
- **`fs_meta` 在 `dentries` / `nodes.*_obj_id` 里挂着的"我引用某个 obj"必须落到这三档里**，否则 `named_store` 完全不知道这个对象不能被 LRU/强制 GC 动。
- **入口走 `named_store_gc.md` §5.5 的 `fs_acquire` / `fs_release`**：它语义等价于"以 (inode, field) 为 owner 的批量 Recursive pin"，触发完整 cascade，让整棵子树（FileObject → ChunkList → chunks）都被同一套 `incoming_refs` 机制保护。落表落到独立的 `fs_anchors` 表 + 物化整数列 `fs_anchor_count`，让 GC 候选过滤继续是单 SQL。
- **用户的"反向选择"靠 `Skeleton` pin**（`named_store_gc.md` §5.6）：Skeleton 是一条**前向拦截器**——"在不可变 DirObject DAG 里挑这一支留个壳"。它默认覆盖 `fs_acquire` 的 strict cascade，让用户可以在系统默认"完整下载"的前提下，针对特定子目录显式声明"不要把它的内容拉下来"。
- 其它原语（`pin` / `unpin` / `apply_edge` / `add_chunk_by_same_as` / `forced_gc_until` / `compact` / Maglev cascade …）一律按 `named_store_gc.md` §5–§11 的描述实现。本文档只描述 `fs_meta` ↔ `named_store` 的接缝。

> 一句话：`named_store_gc.md` 是底层机制；本文档是 `fs_meta` 这一类 root provider 的优化集成方案。

---

## 1. 目标

把 `doc/named_mgrv2.md`（NDM v2.1 架构）与 `doc/named_store_gc.md` 整合，为整个 NDM 系统落地端到端的 NamedData 释放路径。

约束：

- **家用而非云盘**：必须真的能删数据。`fs_meta` 不能像传统商用 DFS 那样靠"不删"绕开问题。
- **家用而非缓存层**：默认 `fs_acquire` 的语义就是"这个文件真的要在我盘上活着"——FileObject 被 cascade 锁住的同时，它引用的 ChunkList 与 chunks 也必须被同一棵 cascade 保护。家用 NAS 的用户做"我有这个文件"的判断是直观物理层面的，不能让"目录骨架已保存但叶子要重新下载"成为默认行为。
- **`fs_meta` 反向索引规模未经实测**：候选集大小不能随 `dentries` 规模线性膨胀。
- **GC 决策必须是"本地存储层可独立判断"**：不能依赖运行期回调到 `fs_meta`。
- **无历史包袱**：`fs_meta` 中半成品的 `obj_stat` 表整块放弃，不需要平滑迁移。

---

## 2. 现状盘点

### `named_store`

- `src/named_store/src/backend.rs`：`NamedDataStoreBackend` 两态协议（`NotExist | Completed`）。`LocalFsBackend`（`src/named_store/src/local_fs_backend.rs`）按注释"元数据刻意不落 DB：chunk 是否存在 == 最终文件是否存在"，自身不承担任何引用记账职责。
- `src/named_store/src/store_db.rs`：只有 `chunk_items` 和 `objects` 两张表，**没有 state 列、没有 eviction_class、没有 pins / fs_anchors / incoming_refs / edge_outbox 表**。`remove_chunk` / `remove_object` 无条件删除。
- `src/named_store/src/local_store.rs`：`put_object` / `put_chunk*` 不做任何级联；`remove_*` 没有保护；`ChunkStoreState::LocalLink` 用来挂外部文件，不进 blob 仓。

### `fs_meta`

- `src/fs_meta/src/fs_meta_service.rs`：
  - 第 500–545 行：`nodes` / `dentries` 表；`idx_dentries_target_obj` 索引已存在。
  - 第 550 行：`obj_stat(obj_id, ref_count, zero_since, updated_at)` —— 半成品。只有 `handle_set_file` 会 `bump(+1)`，`handle_set_dir` / `handle_replace_target` / `handle_delete_dentry` 都**没有**对应的 `-1`，泄漏严重。
  - 第 3596–3760 行：`obj_stat` 的 RPC handler 保留，但没有被正确维护。
  - 第 3247 行 `handle_replace_target`、第 3126 行 `handle_delete_dentry`：对 `obj_stat` 零动作。
- `src/ndm-lib/src/fsmeta_client.rs`：`obj_stat_get/bump/list_zero/delete_if_zero` 的客户端方法与 trait handler 仍在（1830–1925 / 2271–2298 / 2571–2595 行）。

### `ndm`

- `src/ndm/src/named_mgr.rs`：
  - 第 1600 行附近 `erase_obj_by_id` 直接调 `layout_mgr.remove_object(obj_id)`，**没有任何安全检查**。
  - 第 1691 行附近 `publish_dir` 调 `layout_mgr.put_object(dir_obj_id, ...)`，但没有对应的引用记账。

**结论**：GC 所需要的基础设施既不在 `fs_meta`，也不在 `named_store`，需要重新划线。

---

## 3. 为什么走"等价于 Recursive pin 的 fs_anchor"这条路

### 3.1 方案 A：继续在 `fs_meta` 里做 `obj_stat`（已废弃）

把"某个 obj 被多少个 dentry 引用"这件事写成 `fs_meta` 里的一列，由 `set_file / set_dir / replace_target / delete_dentry / publish_dir` 六个点维护。

**致命问题**：

1. **分层反了**。`obj_stat` 表达的是存储层的事实（"这个物理对象还需不需要"），但维护责任全部压在文件系统层。`named_store` 想做任何安全处理都得反过来调 `fs_meta`。
2. **崩溃恢复困难**。`obj_stat` 是从 `dentries` 归纳出来的派生状态，一旦有任何一条写路径漏了 bump/unbump，整张表就失真。要重建必须全量扫描 `dentries`。
3. **规模不可控**。`obj_stat` 行数与"曾经出现过的 obj"成正比，而不是"当前活着的 obj"成正比。家用场景下快速产生/删除临时对象时，这张表会膨胀。
4. **和 Overlay 生命周期打架**。`Working → Cooling → Linked → Finalized` 中同一个 inode 会先后绑定 `base_obj_id` / `linked_obj_id` / `finalized_obj_id`，`obj_stat` 需要跟着做很多细粒度的加减，极易出错。

这也是当前实现只在 `handle_set_file` 加了一次 `+1`、其它点都没接的根本原因：方向不对，接不下去。

### 3.2 方案 B：`named_store` GC 时通过 `RootProvider::is_rooted` 回调 `fs_meta`

`doc/named_store_gc.md` §4.5 提到的 `RootProvider` trait：GC 找到候选后，逐个问 `fs_meta`"你还挂着吗"。在单进程原型下没问题，但在 NDM 的分布式目标下不够：

1. **可靠性**：GC 决策依赖一次 `fs_meta` 的活跃调用。`fs_meta` 不可达 / 超时 / 误返回 `false`，就是一次直接误删——而误删物理数据在家用场景**不可恢复**。
2. **性能**：N 个候选 → N 次 RPC + N 次 `fs_meta` 反向索引查询。GC 一轮的延迟与候选集和 `fs_meta` 索引规模都相关。
3. **TOCTOU**：GC 问完 `is_rooted == false` 还没 `remove` 的瞬间，`fs_meta` 并发 `set_file(obj_id)`，就会留下一条悬空 path 指向已删 blob。跨进程要解决这个问题需要分布式锁 / epoch / 两阶段提交。
4. **候选集膨胀**：cache-first 模型下，绝大多数 cache 对象天然就处在 class 0，候选集规模 ≈ 活跃缓存对象数。在传统 dentries 规模下挨个回调 fs_meta 不可接受。

`named_store_gc.md` 仍然保留 `RootProvider` 接口是为了**少量、慢变**的 root 来源（例如未来外部备份系统的快照声明），不是给 `fs_meta` 这种大规模反向索引用的。**`fs_meta` 不走 `RootProvider`**，走 §4 的 `fs_anchors` + cascade 方案。

### 3.3 方案 C：把每条 fs 引用都做成 `pins` 表里的一行 Single pin

最自然的对接思路是让 `fs_meta` 在每条引用 obj 的字段上调一次 `pin(obj, owner='fs:<inode>:<field>', scope=Single)`。

**问题**：这会把 `pins` 表的规模拉到 `dentries` 同阶。`named_store_gc.md` §7.2 的候选查询本来是单 SQL 走 `(eviction_class, last_access_time)` 复合索引，方案 C 下需要 `WHERE NOT EXISTS (SELECT 1 FROM pins WHERE obj_id=? AND owner LIKE 'fs:%')`——索引大小接近 `dentries`，每行还带 `(scope, owner, created_at, expires_at)` 元数据，比一个整数列贵很多。`pins` 表的设计预期场景是"少量临时持有 + 少量用户手动 pin"，不是承载整个文件系统的反向索引。

**另一个更严重的问题**：`Single` pin 自 `named_store_gc.md` 升格为 `Skeleton` 后是**前向拦截器**——意思是把 fs 引用全灌成 Single pin 会让 `apply_edge` 在每一个 dentry 处停下，整个文件系统的 cascade 都会被自己挡死。这在语义上完全反了。

### 3.4 方案 D：`fs_acquire` 只锁根，子树靠 LRU（曾经的中间方案，已废弃）

曾经考虑过把 `fs_acquire` 实现为"只 +1 一个整数列、不触发 cascade，FileObject → ChunkList → chunks 全部留在 class 0 cache 队列里"。理由是这样 acquire 是真正 O(1) 的，候选集也不用经过 cascade 边的扩散。

但这条路在家用 NAS 语义上是错的：

- 用户 `cp file ./` 之后期望"这个文件就在我盘上"，不期望"目录元数据在、内容随时可能被淘汰、再读要重新走网络下载"。
- 普通 `put` 是 O(1) 是因为它在网络扩散的中间路径上、内容是不是被自己缓存住与用户感知无关。`fs_acquire` 不一样：它是用户语义"我要它"的最后一棒。
- 强制 GC 红线只保护 class ≥ 1。如果只锁根，那 ChunkList / chunks 在水位告急时会被强制 GC 清掉——文件系统层就需要再做一遍"按 inode 找内容补回"，这是把存储层的责任偷偷推回 fs 层。

所以本文档**全面拥抱 strict cascade**：`fs_acquire(F)` 等价于"以 (inode, field) 为 owner 在 F 上做一次 Recursive pin"，cascade 一直走到 chunk 层。代价是 `incoming_refs` 表规模 ~ `Σ files × chunks_per_file`，但收益是用户语义直观、`fs_meta` 不需要参与 GC 决策、Skeleton 前向拦截器可以单独表达"我不要这一支"的反向选择。

> 接受的成本：`incoming_refs` 表规模与单进程 sqlite 的写入吞吐。这些是分片层（Maglev + edge_outbox）和 in-memory 热表（LRU flusher）共同处理的、`named_store_gc.md` 已经定型的工程问题。

---

## 4. 核心设计：`fs_acquire` = 批量 Recursive pin + 独立 `fs_anchors` 表

### 4.1 三档存活语义里再多一种 root：fs_anchor

`named_store_gc.md` §0 的三档：

| class | 触发条件 | 是否可被 GC |
|---|---|---|
| 0 cache | 普通 `put` 默认 | 任何水位都可被 LRU 回收 |
| 1 referenced | `incoming_refs(X) ≠ ∅`（被某个 cascade 根持有） | 强制 GC 红线之内**不可** |
| 2 anchored | `pins` 表中有 active 行 **或** `fs_anchor_count > 0` | 仅显式 unpin / fs_release |

`fs_anchor_count > 0` 是一条**与 `pins` 完全等价的 anchor 来源**。它的物理意义是"`fs_meta` 里有多少条非 tombstone、指向 X 的字段"——和 `pins` 行一样是上层"我承诺这玩意必须留下"的硬声明，受同一条强制 GC 红线保护（`named_store_gc.md` §7.3 / §13）。

`fs_anchor_count` 是从 `fs_anchors` 表物化出来的整数列。`fs_anchors` 才是事实的权威，物化列只是为了让 GC 候选查询继续走单索引扫描——这是 §3 列出的多个方案里只有 `fs_anchors` + 物化列同时满足"分层名分归位 + 候选集尺寸与 dentries 解耦"的组合。

`named_store_gc.md` §4.1 已经把 `eviction_class` 描述为"`pins ∪ incoming_refs ∪ fs_anchors` 的物化缓存"。本文档完全继承这条规则，不需要扩展。

### 4.2 数据模型

`named_store_gc.md` §4.6 已经把表定下来，本文档只是在语境里复述：

```sql
CREATE TABLE fs_anchors (
    obj_id     TEXT    NOT NULL,
    inode_id   INTEGER NOT NULL,
    field_tag  INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (obj_id, inode_id, field_tag)
);
CREATE INDEX fs_anchors_by_obj ON fs_anchors(obj_id);

ALTER TABLE objects     ADD COLUMN fs_anchor_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN fs_anchor_count INTEGER NOT NULL DEFAULT 0;
```

要点：

- **`fs_anchors` 不存 path、不存 dentry 路径串**。`(inode_id, field_tag)` 是 ndm 层的"反引用句柄"，让 ndm 知道在 inode 析构、`replace_target`、`delete_dentry` 等点需要 release 哪一行。所有 path / 重命名 / 移动语义封装在 ndm 层，`named_store` 完全无感。
- **`fs_anchor_count` 是物化列**：任何修改 `fs_anchors` 的事务里必须 `+1`/`-1`，配套 `recompute_eviction_class`（或等价的 set_eviction_class）一并更新行的 class。崩溃恢复路径靠 `SELECT COUNT(*) FROM fs_anchors GROUP BY obj_id` 重算（`named_store_gc.md` §10.5）。
- **GC 候选查询完全沿用 `named_store_gc.md` §6.3 / §7.2，不需要改**：

```sql
SELECT obj_id FROM objects
WHERE eviction_class = 0
  AND home_epoch IS current_epoch
ORDER BY last_access_time ASC
LIMIT ?;
```

候选集规模 ≈ "纯 cache 对象数 + 最近 release/unpin 之后掉到 class 0 的对象数"，**与 `dentries` 规模无关**。这是满足"团队对分布式可靠性 / 性能顾虑"的关键指标——`fs_anchor_count` 物化吸收掉了，GC 候选的扫描完全是单索引顺序读。

`named_store_gc.md` 的 `pins` 表（带 `scope` 列）保留原状，专门承载"用户手动 pin / Skeleton 拦截器 / 临时持有"。**`fs_meta` 不写 `pins` 表，只写 `fs_anchors`**。

### 4.3 三种 root 维度完全正交、独立可重建

| 维度 | 承载 | 由谁维护 | 重建方式 |
|---|---|---|---|
| `incoming_refs` 边集 | 某个 cascade 根（pin Recursive 或 fs_anchor）出的边 | `named_store` 内部 cascade（`named_store_gc.md` §5） | 遍历 `pins where scope='recursive'` 与 `fs_anchors` 的 distinct obj_id，重新跑 cascade（`named_store_gc.md` §10.4） |
| `fs_anchors` 行集 + `fs_anchor_count` 物化列 | `fs_meta` 里非 tombstone、指向该 obj 的字段 | `fs_meta` 写路径在共享事务里调 `fs_acquire` / `fs_release` | 扫 `fs_meta.dentries` + `nodes.*_obj_id` 聚合 + 重算 `fs_anchor_count` |
| `pins` 行 | 用户手动 pin / Skeleton 反向选择 / Lease 临时持有 | `NamedLocalStore::pin` / `unpin` | 启动期按策略丢弃或保留 |

任何一个维度失真都能独立机械重算，互不依赖。**这是 `fs_anchors` 用独立表的核心收益**：`fs_meta` 的 root 信息不和用户 pin、不和 cascade 内部边混在一起。

### 4.4 cascade 走 `named_store_gc.md` §5 的统一路径

`fs_acquire(obj_id, inode_id, field_tag)` 在 home bucket 的本地事务里执行 `named_store_gc.md` §5.5 描述的逻辑：写 `fs_anchors` 行 → `fs_anchor_count += 1` → `eviction_class = 2` → 解析 obj 的 children → 投递 `add` outbox。后续 cascade 由 `apply_edge` 在每个 referee 的 home bucket 自然展开。`fs_release` 对称：`fs_anchors` 行删除 → 计数 -1 → 计数归零时投递 `remove` outbox 撤回 cascade。

**Skeleton 拦截在每一层都生效**：`apply_edge` 检查 `has_skeleton_pin(referee)`，命中就停止下钻（`named_store_gc.md` §5.3）。所以"用户先 Skeleton 一个目录、再 fs_acquire 它的祖先"的组合会自然落地——那一支的子内容不会被强制下载/锁住。

**默认是 strict cascade**，**Skeleton 是用户手工墓碑**：默认行为反映了家用 NAS"用户期待文件真的在盘上"的诉求；用户对 cache 容量有意识时，可以用 Skeleton 在不可变 DirObject DAG 上做反向选择，告诉系统"这一支不要"。这两层加起来精确表达了用户在三个语义上的意图：

1. 完整保存这棵子树 → 默认 fs_acquire 即可，不需要任何额外操作；
2. 留个目录壳但不要内容 → 在希望"留壳"的子目录上 `pin(D, _, Skeleton)`，再让上层正常 fs_acquire；
3. 临时持有不影响他人 → 内部模块用 `Lease` scope。

---

## 5. `fs_meta` / `ndm` 的 `fs_acquire` / `fs_release` 调用点

`fs_meta` 在自己的写路径上、显式调用 6 个点，把"我开始 / 不再引用 obj_id"告诉 `named_store`。两个原语都是**幂等的 (obj_id, inode_id, field_tag) 三元组加减**。

```rust
/// 文件系统层声明 inode_id 上的 field_tag 字段开始引用 obj_id。
/// 同一 (obj_id, inode_id, field_tag) 重复调用是 no-op，幂等。
/// 第一次插入触发完整 Recursive cascade（细节见 named_store_gc.md §5.5）。
pub async fn fs_acquire(
    &self,
    obj_id:    &ObjId,
    inode_id:  u64,
    field_tag: u32,
    tx:        Option<&Transaction>,   // 传入则共享事务
) -> NdnResult<()>;

/// 对称释放。重复 release 是 no-op，幂等。
/// 计数归零时投递 remove cascade，撤回相应的 incoming_refs。
pub async fn fs_release(
    &self,
    obj_id:    &ObjId,
    inode_id:  u64,
    field_tag: u32,
    tx:        Option<&Transaction>,
) -> NdnResult<()>;

/// inode 销毁专用：把这个 inode 上所有字段一次性 release。
pub async fn fs_release_inode(&self, inode_id: u64, tx: Option<&Transaction>)
    -> NdnResult<usize>;
```

实现细节见 `named_store_gc.md` §5.5。本文档的责任是把六个 fs 写路径接到这个 API 上。

### 5.1 六个调用点

映射到 `fs_meta_service.rs` 的现有写路径：

| # | 位置 | 语义 | 调用 |
|---|---|---|---|
| 1 | `handle_set_file` | 新建/替换文件 dentry 指向 `content_obj_id` | `fs_acquire(new, inode, FIELD_CONTENT)`；替换旧值时 `fs_release(old, inode, FIELD_CONTENT)` |
| 2 | `handle_set_dir` | 目录 dentry 指向 DirObject | `fs_acquire(new, inode, FIELD_DIR)`；替换时 `fs_release(old, ...)` |
| 3 | `handle_replace_target` | dentry 目标切换（含 tombstone 化） | `fs_release(old, ...)`；新目标是 obj 类 dentry 则 `fs_acquire(new, ...)` |
| 4 | `handle_delete_dentry` | 真正删除 dentry 行（而非 tombstone） | `fs_release(target_obj, inode, ...)` |
| 5 | `publish_dir`（`ndm/src/named_mgr.rs`）/ Overlay Finalize | `Linked → Finalized`，把 `finalized_obj_id` 写进 inode | `fs_acquire(finalized_obj_id, inode, FIELD_FINALIZED)`；旧 `base_obj_id` 按 `fs_release` 规则释放 |
| 6 | inode 销毁（垃圾回收 orphan inode） | 该 inode 上所有 `*_obj_id` 字段释放 | `fs_release_inode(inode)` |

`field_tag` 是一个小整数枚举（`FIELD_CONTENT` / `FIELD_DIR` / `FIELD_BASE` / `FIELD_LINKED` / `FIELD_FINALIZED` …），仅在 ndm 层有意义，`named_store` 把它当不透明 u32 存起来。它的作用是让"同一 inode 上多字段同时引用同一 obj"的去重和 release 都是精确的。

**Tombstone 处理**：Tombstone 自身（`DENTRY_TARGET_TOMBSTONE = 2`）不持有任何 obj 引用——它只是个墓碑。`handle_replace_target` 从 `target_obj` 切到 tombstone 时，必须对原 target 调一次 `fs_release`。这是目前代码**完全缺失**的点。

**Staged Commit 处理**（来自 `named_mgrv2.md` §5/§6）：

- `Working` 态：`base_obj_id` 是 Overlay 的基线。`fs_acquire(base_obj_id, inode, FIELD_BASE)` 在 inode 创建时完成。
- `Cooling` 态：等待内容 publish。`base_obj_id` 仍被持有；inode 被删走点 #6 释放整个 inode。
- `Linked` 态：`linked_obj_id` 已算出但还未写 `finalized_obj_id`。**此时**调 `fs_acquire(linked_obj_id, inode, FIELD_LINKED)`；`base_obj_id` 在确认 `linked_obj_id` 登记成功后 `fs_release(base, inode, FIELD_BASE)`。
- `Finalized` 态：`finalized_obj_id = linked_obj_id`。如果 `FIELD_FINALIZED` 与 `FIELD_LINKED` 在 ndm 层是两个独立 tag，需要再 `fs_acquire(finalized, inode, FIELD_FINALIZED)`；如果 ndm 直接把 `FIELD_LINKED` 改名复用，那一步就不必要。本文档建议后者，少一次 acquire/release。

Grace pin 在 `NamedLocalStore::put_object` 自动打上（`Lease` scope，见 §8），负责兜底"根对象已 put 进仓、但 `fs_meta` 还没跑到 acquire"的时间窗。

### 5.2 cascade 行为：用户视角

`fs_acquire(F)` 触发的 cascade 行为对用户是这样的：

- F 是 FileObject → cascade 到 ChunkList → cascade 到每个 chunk。所有节点都升到 class ≥ 1，强制 GC 红线之内不动。
- F 是 DirObject → cascade 走遍整棵子树（DirObject → child DirObjects / FileObjects → ChunkLists → chunks），整棵树都受 strict 保护。
- 任意一层有 Skeleton pin → cascade 在该层停下。Skeleton pin 自身仍然是 anchor（class 2），但它的 children 不会被这次 cascade 触及。

`fs_release(F)` 是对称的：anchor 计数归零时投递 `remove` cascade，靠 `incoming_refs` 的 `INSERT OR IGNORE` / `DELETE` 在每个 referee 的 home bucket 解决去重。如果 F 在另一个 inode 上还有别的 fs_anchor，或者还在某个 Recursive pin 子树里（即 `incoming_refs(F) ≠ ∅`），那 F 自身仍然是 class ≥ 1，不会被 GC 触及。

> 注意：strict cascade 的代价是"删一个大文件" cascade 会发出大量 `remove` outbox，最终一致地把每个 chunk 的 `incoming_refs` 行清掉。这个过程是异步的，但 GC 红线不会受影响——`apply_edge` 是事务性的，不会出现"边删了一半就被 GC 看到 class 0"的中间状态。

### 5.3 用户的"反向选择"通过 Skeleton 表达

家用场景常见诉求："我从一个共享 / 备份目录里收下来的整棵 5TB 子树，但我家里只有 500GB 盘，只想要其中的某几个子目录"。

这条路在本设计里是这样表达的：

1. ndm 上层接收到一个 DirObject `R`（5TB 的根），按某种入口（用户拷贝、订阅、手动 mount）触发 `fs_acquire(R, inode_of_R, FIELD_DIR)`——cascade 默认会一路展开。
2. **用户预先**对自己**不要**的子目录调 `pin(D_i, owner='user:skeleton', scope=Skeleton)`。`Skeleton` pin 是前向拦截器，所以即便 `fs_acquire(R)` 后到，cascade 走到 D_i 时就停下，D_i 的子内容不会被锁住。
3. 如果用户改主意了想要原本被 Skeleton 屏蔽的子目录里的某个文件 / 子目录：直接 `unpin(D_i, owner='user:skeleton')`。**但这不会自动重新展开 cascade**（`named_store_gc.md` §5.6 Case C），上层 ndm 必须显式 `fs_release(R) + fs_acquire(R)` 或者调用一个等价的 `refresh_anchor(R)`。这是"前向语义"的代价，换来 unpin O(1) 的承诺。
4. 如果用户希望"对 D_i 子树更精细的反向选择"——比如 D_i 下的 100 个子文件夹只要其中 10 个——直觉上是调 100 次 Skeleton 把不要的挡掉。但这又把规模问题带回来了。**实际工程里这种细粒度选择应该走"用户重新发布一个新 DirObject"或者"上层 ndm 创建一个虚拟视图"**，不应该指望底层 GC 做。这一条是 `named_store_gc.md` §0 的设计意图："不鼓励用户因为选择而不是发布去创建一个 DirObject 的新版本"。

> 形象地说：`fs_acquire` 是"在一大堆里告诉系统我要这棵"，Skeleton 是"在那一棵里告诉系统这一支我不要"。两者都是在不可变 DAG 上做视图层选择，永不修改 DirObject 本身。

### 5.4 `Lease` scope 的位置

`named_store_gc.md` 引入了第三种 PinScope `Lease`，专门给 grace pin / 下载 session / 短期持有用。`Lease` 的特征：

- **锁自己**（class 2），让 LRU 不会把根对象立刻清掉；
- **不 cascade**，所以不会带来 outbox 流量；
- **对 cascade 透明**，不像 Skeleton 会拦截上游——这点关键，否则 grace pin 会意外切断并发的 fs_acquire cascade。

`Lease` 是 `named_store` 内部和 ndm 内部用的，不暴露给最终用户。本文档涉及的两个 Lease 用法：

1. `put_object` 落地后种的 grace pin（§8）。
2. 下载会话期间的临时持有：上层 ndm 在 `pull(obj)` 开始时 `pin(obj, owner='lease:pull:<uuid>', scope=Lease, ttl=session_ttl)`，结束时 unpin 或让 ttl 自然过期。这条路可以让"下载到一半但还没注册 fs_anchor"的对象不被 GC 抢走，又不会污染 cascade。

---

## 6. 共享 sqlite：单进程阶段的原子性解法

TOCTOU 问题（`fs_meta` 加 path 和 GC 删 blob 并发发生）在单进程 NDM 里用**共享 sqlite** 解决：

```
┌──────────────────────────────────────────────┐
│         ndm.sqlite （同一个文件）              │
│                                              │
│   fs_meta.*   ← nodes / dentries / ...       │
│   ns.*        ← objects / chunk_items /      │
│                  pins / fs_anchors /         │
│                  incoming_refs /             │
│                  edge_outbox / tombstones    │
└──────────────────────────────────────────────┘
          ▲                    ▲
          │                    │
   fs_meta_service      named_store_db
          │                    │
          └─── 共享 Arc<Mutex<Connection>> ──┘
```

`NamedLocalStoreDB::new` 增加一个"接受外部连接"的构造函数：

```rust
impl NamedLocalStoreDB {
    pub fn new(db_path: String) -> NdnResult<Self> { ... }        // 现有：独占连接

    pub fn new_with_connection(
        conn: Arc<Mutex<Connection>>,
    ) -> NdnResult<Self> { ... }                                   // 新增：共享连接
}
```

`fs_meta` 的每个 handler 在开事务后，把 `&Transaction` 顺着传给 `NamedLocalStore::fs_acquire` / `fs_release` / `put_object`，所有操作在同一个事务里提交。这样：

- `handle_set_file` 改一条 `dentry` + 一次 `fs_acquire` → 要么都成（包括对应的 cascade outbox 入队），要么都不成。
- GC 跑在 `BEGIN IMMEDIATE` 里，扫描候选 → 校验 → 删除全部在一个事务内。并发的 `fs_acquire` 被自然串行化到 GC 事务之外，**不可能**出现"GC 读到 class 0、`fs_acquire` 插队、GC 删除"的时序。

`fs_meta_service` 当前分两层初始化（sqlite 在 `fs_meta_service.rs`；`NamedLocalStore` 在 `named_mgr.rs`），共享连接需要把 `ndm` 的启动顺序翻过来：**先开一次 sqlite 连接 → 分别注入两个模块**。

> 注意：cascade 投递是异步的（outbox + sender），不是事务的一部分。事务保证的是"`fs_anchors` 行 + `fs_anchor_count` 物化列 + `eviction_class` 升级 + 第一批 outbox 入队"原子完成；后续 outbox 投递可以失败重试，cascade 走最终一致路径。这与 `named_store_gc.md` §5 / §8 的承诺一致。

---

## 7. 端到端删除流程：150MB 照片

假设用户删一个 150MB 的 JPG，FileObject `F` → ChunkList `L` → 5 个 30MB 的 sub-chunk `C1..C5`。`C2` 和另一个文件共享。**用户没有显式 Skeleton 这个文件**，所以走的是默认 strict cascade。

put 阶段（一段时间前发生）：

- `put_object(F)` / `put_object(L)` / `put_chunk(Ci)` 都是 O(1)，**不写 incoming_refs / edge_outbox**。它们落地时 `eviction_class = 0`，落地的瞬间种一条 `Lease` grace pin（§8）作为 60 秒兜底，挂在 cache LRU 队列里。
- `fs_meta.handle_set_file(...)` 在共享事务里调 `fs_acquire(F, inode, FIELD_CONTENT)`：
  - `fs_anchors(F, inode, FIELD_CONTENT)` 插入；
  - `F.fs_anchor_count`: 0 → 1，`F.eviction_class` = 2；
  - 解析 F → `[L]`，向 L 的 home bucket 投递 `add(referee=L, referrer=F)` outbox。
- 后续 `apply_edge` 在 L 的 home bucket：`incoming_refs(L,F)` 插入，`L.eviction_class` 升到 1，解析 L → `[C1..C5]`，再投 5 条 `add(referee=Ci, referrer=L)` outbox。
- 每个 chunk 的 home bucket 收到对应 `add` 后：`incoming_refs(Ci, L)` 插入，`Ci.eviction_class` 升到 1。其中 `C2` 之前已经因为另一个文件被 cascade 持有，`incoming_refs` 主键唯一保证 `INSERT OR IGNORE` 幂等。
- F 自己的 grace pin 此时可以释放（或自然过期）。
- 最终状态：F 是 class 2（fs_anchor），L、C1..C5 都是 class 1（incoming_refs），全部受强制 GC 红线保护。

删除事件：

1. `fs_meta.handle_delete_dentry(photo_dentry)` 开事务（共享 sqlite）。
2. 事务内读出 dentry 的 target = `F`、所属 inode = `inode_of_photo`、字段 = `FIELD_CONTENT`。
3. 事务内 `fs_release(F, inode_of_photo, FIELD_CONTENT)`：
   - `fs_anchors(F, inode, FIELD_CONTENT)` 删除；
   - `F.fs_anchor_count`: 1 → 0；
   - 没有 active pin、`incoming_refs(F)` 也是空 → `F.eviction_class` 降到 0；
   - 解析 F → `[L]`，投 `remove(referee=L, referrer=F)` outbox。
4. 事务内 `DELETE FROM dentries ...` 或 tombstone。
5. `COMMIT`。

后续的 outbox 投递（异步、最终一致）：

- L 的 home bucket 收到 `remove(L, F)` → `incoming_refs(L,F)` 删除 → `L.has_incoming` 变 false 且无 anchor → `L.eviction_class` 降到 0 → 解析 L → 5 条 `remove(referee=Ci, referrer=L)`。
- 每个 chunk 的 home bucket 收到 `remove(Ci, L)` → `incoming_refs(Ci, L)` 删除 → `Ci` 是否降到 class 0 取决于它还有没有别的 incoming_refs。`C2` 因为另一个文件还在持有 → 仍然是 class 1，**不**降。其它 chunks → class 0，进入 LRU 候选范围。

物理回收：

- **空间充裕时**：F、L、C1/C3/C4/C5 都在 LRU 队列里，等水位 high 后台 GC 慢慢清。
- **空间紧张时**：写入路径触发 `forced_gc_until(target_bytes)`（`named_store_gc.md` §7.3）。该路径只动 class 0，按 `last_access_time` 升序回收。

整条删除路径的本地事务部分是 O(N_anchors) 而不是 O(子树边数)——大头是 outbox 入队和后续 cascade 异步展开。这是 strict cascade 的工程取舍。

### 7.1 为什么要让 cascade 等到 outbox 异步展开

把 cascade 在 `fs_release` 路径上同步走完（在同一个事务内一路 walk 到底）会出三个问题：

1. **跨 bucket 不可行**。Maglev 分片下 L、Ci 可能落在另一个 bucket，必须走 RPC 才能读到 content 解析 children。同步走 RPC 等于把 `fs_release` 的延迟拉到全网最慢的一跳。
2. **崩溃恢复成本**。同步 walk 完没 commit 就崩溃，下一次启动需要决定"已经走到哪一层了"。outbox 模型把这个状态显式地存在 `edge_outbox` 里，自然续走。
3. **删除一棵大树时事务尺寸不可控**。150MB 照片只有 5 个 chunk 还能容忍，但删一个 50 万文件的备份目录用同一个事务跑完会把 sqlite 的写入吞吐撑爆。

异步 cascade 的代价是"`fs_release` 完成之后到 chunks 真的回到 class 0 之间有一段窗口"。这段窗口内 cache 看上去比"应有水平"多占一点空间——但对家用 NAS 是完全可接受的：水位还不告急时这些 chunks 反正活在 cache 里，水位告急时强制 GC 会优先动 class 0 的老对象，新近 cascade 出来的会进队列尾部。

### 7.2 用户希望"删了立刻还空间"

家用场景下用户删大文件确实期望立即释放：

```rust
// ndm 在删大文件 / 用户主动"清理"时调
named_store.forced_gc_until(target_bytes).await?;
```

`forced_gc_until` 走 `named_store_gc.md` §7.3 的红线：只清 class 0、绝不动 class 1/2、空间真不够就 `ENOSPC`。

由于 F 在 `fs_release` 后立即降到 class 0，它会在这一轮被回收。L 和 chunks 严格来说要等 cascade outbox 在它们的 home bucket 上 apply 完才会降到 class 0，所以**第一次 forced_gc_until 不一定能释放整棵子树**——剩下的会在 outbox drain 完后的下一次 GC 周期被释放。如果用户对"立刻释放"极度敏感，ndm 可以在 `forced_gc_until` 之前调一次"等待 outbox 排空"的辅助接口（`named_store_gc.md` §12.2 的 mgr 接口可以提供）。

> 这条是 strict cascade 路径**真正额外承担的成本**：删的时候快不快取决于 outbox sender 的进度。`named_store_gc.md` §0 已经说了"NamedData 本身做到的是语义 size > 实际 size，大部分时候的收益更大"——这里说的就是这种 cache LRU 和 cascade 异步配合，最终把瞬时空间占用收敛到长期均值的现象。

---

## 8. 新写入对象的 Grace 保护

`put_object` 落盘后默认 `eviction_class = 0`，是个等待被接住的根。`fs_meta` 跑 `fs_acquire` 之前的时间窗里，水位告急可能把它清掉。

沿用 `named_store_gc.md` 的方案：`NamedLocalStore::put_object` 内部自动种一个 **`Lease` grace pin**：

```rust
db.put_object(...);
db.pin_internal(
    obj_id,
    owner = format!("lease:put_object:{}", uuid),
    scope = PinScope::Lease,
    expires_at = now + GRACE_TTL,    // 60s
    txn,
);
```

`Lease` scope 是关键：

- 它锁住根对象自己，不让 LRU 在 60 秒窗口内动它；
- 它**不 cascade**，也不写 outbox，是 O(1)；
- 它**对 cascade 透明**——并发的 `fs_acquire` 走过 grace 对象时不会被截断。这是 Lease 与 Skeleton 的根本区别，详见 `named_store_gc.md` §0 的 PinScope 表。

60 秒内 `fs_meta` 跑完 `fs_acquire` 后可以主动 drop 这条 grace pin（或者让它自然过期都行）。Staged Commit 的"根对象已到、dir 还在 Cooling"场景由**下载会话 pin**覆盖（同样是 `Lease` scope）。

---

## 9. 强制 GC 红线与水位

完全沿用 `named_store_gc.md` §7。要点：

- 水位空闲 → 不 GC；high → 后台慢节奏；critical → 全速；hard_limit → 写路径同步触发 `forced_gc_until`。
- 强制 GC **永不淘汰 class ≥ 1 的对象**——包括 `fs_anchor_count > 0`、有 active `pins` 行、有 `incoming_refs` 的对象。这三个来源在 GC 视角下完全平权。
- 空间真不够 → 返回 `ENOSPC`，由 `ndm` 上层决定是清 pin / `fs_release` / 引导用户删 path / 还是放弃写入。`named_store` 自己绝不替用户做这个决定。

`fs_meta` 整合下，"空间真不够"的解读是：用户的 dentries 总和（即 `Σ fs_anchor_count > 0 的根 obj + 它们 cascade 到的所有 chunk size`）+ 用户/会话 pin 的总和已经超过设备容量。这是个**容量配置问题**，需要 `ndm` / 用户介入清理 dentries、对子目录设 Skeleton 反向选择、或迁移到外接存储——GC 算法本身无能为力。

---

## 10. `erase_obj_by_id` 与 `evicted` 状态

`named_mgrv2.md` §7.2 要求：`erase_obj_by_id` 是**容量管理**——把本地物理副本腾出去，但保留元数据和 path，下一次 `open` 触发 `NEED_PULL`。这**不是** GC，是用户/上层显式越过红线。

为此扩展 `named_store_gc.md` §4.1 的 `state` 取值，多一个 `evicted`：

| state | 物理文件 | DB 行 | `fs_anchor_count` | `incoming_refs` | 可被 GC |
|---|---|---|---|---|---|
| `present` | 存在 | 存在 | 任意 | 任意 | 仅 class 0 时 |
| `evicted` | 不存在 | 存在 | > 0 或有 incoming | 同左 | **否**（GC filter 排除） |
| `shadow` | 不存在 | 存在 | 0 | 任意（用作占位） | 是（无 incoming 时） |
| `incompleted` | 部分 | 存在 | 0 | 0 | 写路径用 |

`erase_obj_by_id(obj_id)`：

```rust
require!(fs_anchor_count(obj_id) > 0
      || has_active_pin(obj_id)
      || has_incoming(obj_id));
db.update(state = 'evicted');           // 先改 DB
backend.remove_object_or_chunk(...);    // 再删物理文件
```

GC 候选条件里**明确排除** `evicted`：

```sql
WHERE eviction_class = 0
  AND state = 'present'
  AND home_epoch IS current_epoch
```

`evicted` 行因为有 `fs_anchor_count > 0` 或 `has_incoming` 已经是 class ≥ 1，本来也不会被 GC 看到；这里再加一道 `state = 'present'` 过滤是双保险——避免"只通过 `eviction_class` 过滤就够了"的认知漂移。

`ndm/src/named_mgr.rs::erase_obj_by_id` 当前直接 `layout_mgr.remove_object(obj_id)`，需改为：

```rust
self.local_store.mark_evicted(obj_id).await?;   // DB state=evicted
self.backend.remove_object(obj_id).await?;      // 删物理文件
```

**顺序**：先改 DB 再删文件。崩溃在中间 = 文件孤儿，由启动期扫描兜底；反向会导致"文件已删但 state 还是 present"——会被 GC 当成正常对象误删。

> 注意：`erase_obj_by_id` 只动单个对象的物理副本，**不**触发 cascade，也**不**影响这个对象的 `incoming_refs` / `fs_anchors`——它的逻辑身份仍然在，只是 blob 暂时不在本地。下一次 `open` 走 `NEED_PULL` 重新下载时，`put_object` 会把 state 从 `evicted` 改回 `present`，原有的 `incoming_refs` / `fs_anchors` 完全保留。

---

## 11. 分布式演进路径

未来 `fs_meta` 与 `named_store` 跨进程 / 跨节点时，共享事务消失。本方案为此预留三件东西：

1. **`(inode_id, field_tag)` 作幂等键**。`fs_anchors` 主键 `(obj_id, inode_id, field_tag)` 天然是幂等的：`INSERT OR IGNORE` 重放安全，`DELETE` 同样幂等。同一字段重放 N 次只算一次，RPC 幂等就解决了。这正是 `fs_anchors` 用三元组而不是单个 owner string 的理由。
2. **写者侧 outbox**。`fs_meta` 的每次写在本地事务里同时写 `dentries` 改动和自己的 `fs_outbox(op, obj_id, inode_id, field_tag, lsn)`；后台 pusher 把 outbox 投递到 `named_store`。`named_store` 按 `(obj_id, inode_id, field_tag)` 去重。这套 outbox 与 `named_store_gc.md` §4.3 的 `edge_outbox` **完全独立**：`edge_outbox` 服务于内部 cascade 的跨 bucket 投递；`fs_outbox` 服务于 `fs_meta → named_store` 的跨进程 acquire/release。
3. **离线 `rebuild_fs_anchors`**。`fs_meta` 侧工具：扫一遍 `dentries` 和 `nodes.*_obj_id`，生成应有的 `fs_anchors` 行集，和 `named_store` 比对、修正。修正之后调一次 `recompute_fs_anchor_count` 把物化列对齐。不动 `incoming_refs`、不动 `pins`，三者完全解耦。

**单进程阶段**只实现 acquire/release 的共享事务版本，outbox/rebuild 作为占位接口。当 `fs_meta` 和 `named_store` 跨进程时，acquire/release 的内部实现切换为"写 outbox"，六个调用点的代码完全不动。

`named_store` 内部的 Maglev 分片和 `edge_outbox` 由 `named_store_gc.md` §3 / §5.3 / §9 处理，本文档不重复。`fs_acquire` / `fs_release` 在 mgr 层路由到 `obj_id` 当前的 home bucket，与 `pin` / `unpin` 同走一条路。

---

## 12. 一些细节的处理

**去重（dedup）**：`put_object` 对已存在的 `present` 行短路；`fs_acquire` 的幂等性靠 `(obj_id, inode_id, field_tag)` 主键的 `INSERT OR IGNORE`。

**chunks 被多文件共享**：cascade 模型下每个共享 chunk 的 `incoming_refs` 集合里会有多条边（每个引用它的 ChunkList 一条）。GC 看到 `has_incoming(C) == true` 就保留；只有最后一条边被撤回（即所有引用它的文件都被 `fs_release` 完）才会回到 class 0。这是引用计数的精确表达。

**先删后再加（race）**：`fs_meta` 在同一事务内 `fs_release(X, ...)` + `fs_acquire(X, ...)`（替换到同一目标），`fs_anchors` 主键不冲突（`(obj, inode, field)` 三元组），删后插的瞬间不会被 GC 看到——它读不到未 commit 的中间状态。

**Working/Cooling 态下删 inode**：走点 #6 的销毁路径 `fs_release_inode(inode)`，所有非空字段一次释放，对应的 cascade `remove` outbox 在事务提交后异步展开。

**Linked 的 `ExternalFile` / `LocalLink`**：物理数据在 FileBuffer。`named_store` 的对应行是 `LocalLink`/`ExternalFile`，同样走 `fs_anchor_count` + `eviction_class` + cascade。GC 删行时 backend 的 `remove_object` 对 `LocalLink` 是 no-op（它不拥有物理文件）——正确。

**用户主动"完整保留"某个文件 / 目录**：默认 `fs_acquire` 已经是 strict cascade，不需要额外操作。如果用户确实想发出更强的"我承诺这棵子树我永远要"的声明，可以平行调一次 `pin(obj_id, owner='user:keep:<inode>', scope=Recursive)`——这会在 `pins` 表里多一条独立行，让 `named_store` 在该对象上同时有两个 anchor 来源。`fs_release` 不会清掉它。

**用户主动"反向选择"不要某个子目录**：`pin(D, owner='user:skip:<inode>', scope=Skeleton)`。先后顺序无关——见 `named_store_gc.md` §5.6 Case A/B/C。

---

## 13. 备选方案

### 13.1 `fs_acquire` 不触发 cascade，靠 LRU 自然处理（已废弃）

详见 §3.4。废弃理由：与"家用 NAS 用户期待文件真的在盘上"的语义直觉冲突；让强制 GC 红线无法保护 chunks；把存储层责任偷偷推回 fs 层。

### 13.2 `fs_anchors` 行存 path 而不是 `(inode, field)`

诱惑是 `named_store` 自己就能回答"这个 obj 现在在文件系统里挂在哪里"。问题：

- path 是可变的（rename / mv），每次重命名要重写整张表的子树相关行；
- 跨 bucket 复制 path 字符串浪费空间；
- 重建工具要爬 `fs_meta` 重算 path 还要保证字符串编码一致。

`(inode, field)` 是 ndm 层的不可变句柄，对 `named_store` 完全不透明，重建工具直接走 `dentries` 即可。

### 13.3 把 `fs_anchors` 行直接灌进 `pins` 表

诱惑是少一张表、少一组重建工具。问题在 `pins` 索引规模、`scope` 多变体的语义混乱、以及 `(owner, obj)` 唯一性退化为字符串拼接（`owner = "fs:inode_id:field_tag"`），dedup 比 `(obj, inode, field)` 三元组主键贵得多。

两个方案的对外 API（`fs_acquire` / `fs_release`）完全一致——`fs_meta` 看到的接口不变，将来如果要切换实现，调用点不动。

### 13.4 让 `fs_meta` 实现 `RootProvider` trait（运行期回调）

`named_store_gc.md` §4.5 提供的 trait。GC 候选时调 `is_rooted(obj_id)` 询问 `fs_meta`。本文档 §3.2 已经分析过这条路在分布式可靠性 / 性能 / TOCTOU / 候选集膨胀上的全部问题。

但 `RootProvider` 接口本身仍然有用——供未来其它"少量、慢变"的 root 来源使用（例如外部备份系统的快照声明）。**`fs_meta` 不走这条路**。

---

## 14. 崩溃恢复

四层独立的恢复（前三层来自 `named_store_gc.md` §10.3 / §10.4 / §10.5，本文档增加第四层）：

1. **文件孤儿扫描**：启动时遍历 backend 的 `chunks/` 和 `objects/`，对每个文件查 DB。对应行不存在 / state ∉ {`present`} → 删文件。
2. **`incoming_refs` 重建**：遍历 `pins where scope='recursive'` 与 `fs_anchors` 的 distinct obj_id，为每个根重新 cascade（`named_store_gc.md` §10.4）。`apply_edge` 在 Skeleton 节点会自然停下，所以重建不需要专门跳过 Skeleton 子树。
3. **`fs_anchor_count` 物化重建**：按 `obj_id` GROUP BY `fs_anchors` 重算物化列（`named_store_gc.md` §10.5）。这一步很便宜（一次 GROUP BY），建议每次启动都做一遍兜底。
4. **`fs_anchors` 重建**：`fs_meta` 侧工具，扫 `dentries` / `nodes.*_obj_id`，生成应有的 `(obj_id, inode_id, field_tag)` 三元组集合，和 `named_store.fs_anchors` 比对、修正。修正之后再触发第 3 步。

四者**互不混合**——任何一类失真都能独立机械重算。这是分四类记账（`incoming_refs` / `fs_anchors` / `fs_anchor_count` 物化 / `pins`）的最大收益。

`pins` 表清理：按策略丢弃 `lease:*` 类 owner 的临时 pin（`Lease` scope 的，启动时全部清掉）；保留 `user`、`user:skip:*`（Skeleton）、其它持久 owner。

---

## 15. 实施顺序

1. **`store_db.rs` schema**：先按 `named_store_gc.md` §4 落地全部 schema（`state` / `last_access_time` / `eviction_class` / `home_epoch` / `fs_anchor_count` 列、`incoming_refs` / `edge_outbox` / `pins` / `fs_anchors` / `tombstones` 表、复合索引、`pins_skeleton_by_obj` 部分索引、`fs_anchors_by_obj`）。
2. **`store_db.rs` 内部原语**：`fs_acquire_local` / `fs_release_local` / `recompute_eviction_class` / `recompute_fs_anchor_count` / `mark_evicted` / `has_skeleton_pin`。`recompute_eviction_class` 综合 `pins` / `incoming_refs` / `fs_anchor_count` 三项重写一行，集中维护 §4.1 的物化规则。`named_store_gc.md` §12.3 列出的其它 CRUD 同步落地。
3. **`HasRefs` trait + `parse_obj_refs`**：放 `ndn-lib`，给 `DirObject` / `FileObject` / `RelationObject` / `ChunkList` 实现解析（`named_store_gc.md` §12.4）。`SameAs` 大 chunk 的 `parse_obj_refs` 返回 `[chunk_list_id]`。
4. **`local_store.rs` 写路径改造**：
   - `put_object` / `put_chunk*` 改成 O(1) cache 路径（**不**写 outbox），落地后种 `Lease` grace pin。
   - 加 32MB 写入 guard（`named_store_gc.md` §11.1）。
   - 实现 `add_chunk_by_same_as`（`named_store_gc.md` §11.2）。
5. **`local_store.rs` cascade / pin / GC 路径**：实现 `pin` / `unpin` / `fs_acquire` / `fs_release` / `apply_edge`（含 `has_skeleton_pin` 拦截）/ `forced_gc_until` / `mark_evicted`（按 `named_store_gc.md` §5–§7 与 §10）。
6. **`NamedLocalStoreDB::new_with_connection`**：共享 sqlite 连接，打通 `ndm` 启动路径。
7. **`fs_meta_service.rs` 六个调用点**：
   - 先删现有的 `obj_stat` 表 / `handle_obj_stat_*` handler / `ndm-lib/src/fsmeta_client.rs` 里对应的客户端方法和 trait 条目。
   - 在 `handle_set_file` / `handle_set_dir` / `handle_replace_target` / `handle_delete_dentry` 接入 `fs_acquire` / `fs_release`，事务共享。
8. **`named_mgr.rs` 生命周期点**：
   - `publish_dir` / Overlay `Linked → Finalized` 切换调 `fs_acquire`。
   - `erase_obj_by_id` 改为 `mark_evicted` + 删文件。
   - 启动时把 `fs_meta` 的 sqlite 连接共享给 `NamedLocalStore`。
   - 删大文件 / 用户主动"清理"时按需调 `forced_gc_until`。
9. **mgr 层后台任务**：`start_edge_sender` / `start_migration_worker` / `start_lru_flusher`（`named_store_gc.md` §12.2）。
10. **恢复与工具**：启动期文件孤儿扫描；`rebuild_incoming_refs` / `rebuild_fs_anchor_count`（`named_store` 内部）；`rebuild_fs_anchors`（`fs_meta` 侧）。

`obj_stat` 及其 handler 直接删——当前系统里它本来就没被正确维护过，无需平滑迁移。

---

## 16. 与现有代码的差量

| 文件 | 改动 |
|---|---|
| `src/named_store/src/store_db.rs` | `named_store_gc.md` §4 描述的全部 schema + 索引 + CRUD（含 `fs_anchors` 表 / `fs_anchor_count` 列 / `pins_skeleton_by_obj` 部分索引）；`fs_acquire_local` / `fs_release_local` / `recompute_eviction_class` / `recompute_fs_anchor_count` / `mark_evicted` / `has_skeleton_pin`；`new_with_connection` 构造器 |
| `src/named_store/src/local_store.rs` | 写路径加 32MB guard；普通 `put_object` / `put_chunk*` 改成 O(1)（不写 outbox）+ `Lease` grace pin；新增 `pin` / `unpin` / `fs_acquire` / `fs_release` / `apply_edge`（含 Skeleton 拦截）/ `add_chunk_by_same_as` / SameAs 读路径；新增 LRU touch、本地 `forced_gc_until`、`mark_evicted` |
| `src/named_store/src/local_store.rs::PinScope` | 三变体 `Recursive` / `Skeleton` / `Lease`（替换 beta1 的 `Recursive` / `Single`） |
| `src/named_store/src/local_store.rs::ChunkStoreState` | 新增 `SameAs(ObjId)` 变体；`can_open_reader` / `parse_obj_refs` 处理它 |
| `src/named_store/src/store_mgr.rs` | 新增 `start_edge_sender` / `start_migration_worker` / `start_lru_flusher`；`compact` 改为"先确认无 visiting 再 truncate"；`pin` / `unpin` / `fs_acquire` / `fs_release` 路由到 home bucket |
| `src/named_store/src/backend.rs`、`local_fs_backend.rs` | **不动**。backend 继续只负责两态物理存储，GC 逻辑一律走 `store_db` |
| `src/ndn-lib/...` | `HasRefs` trait + 各 obj 类型的 ref 解析器（`DirObject` / `ChunkList` / `RelationObject` / `FileObject`） |
| `src/fs_meta/src/fs_meta_service.rs` | 删 `obj_stat` 表 / `handle_obj_stat_*`（第 550 / 3596–3760 行）；在 `handle_set_file`（3809）/ `handle_set_dir`（3879）/ `handle_replace_target`（3247）/ `handle_delete_dentry`（3126）四个点加 `fs_acquire` / `fs_release`，事务共享 |
| `src/ndm-lib/src/fsmeta_client.rs` | 删 `obj_stat_get/bump/list_zero/delete_if_zero`（1830–1925 / 2271–2298 / 2571–2595 行） |
| `src/ndm/src/named_mgr.rs` | `publish_dir`（1691 附近）调 `fs_acquire`；Overlay 状态机在 Linked/Finalized 切换时同步维护；`erase_obj_by_id`（1600 附近）改为 `mark_evicted`；启动时构造共享 sqlite 连接；删大文件 / 清理时按需调 `forced_gc_until` |

---

## 17. 不变量清单

`named_store_gc.md` §13 的全部 20 条不变量在本文档之上仍然成立。本文档**追加**两条 ndm 层语义不变量：

1. 任何一次 `fs_acquire` / `fs_release` 与触发它的 `dentries` / `nodes` 写入在**同一事务**内提交（单进程阶段靠共享 sqlite，分布式阶段靠 `fs_outbox` 幂等键）
2. `state == 'evicted' ⇒ (fs_anchor_count > 0 ∨ has_active_pin ∨ has_incoming) ∧ blob 文件不存在`

`named_store_gc.md` §13 已经包含的、本文档反复依赖的关键条款：

- `eviction_class` 是 `pins ∪ incoming_refs ∪ fs_anchors` 的物化缓存（§13.9）；
- 强制 GC 永不淘汰 `eviction_class >= 1`（§13.10）；
- Skeleton 前向不变（§13.13）：`apply_edge` 在 `has_skeleton_pin(referee)` 命中时不下钻；
- Skeleton 仅前向生效，不追溯撤回（§13.14）；
- `fs_anchor_count(X) == COUNT(*) FROM fs_anchors WHERE obj_id == X`（§13.8）。

---

## 18. 不打算做的事

- **不**在 `fs_meta` 重建 `obj_stat`。
- **不**让 `named_store` GC 在运行期通过 RPC 询问 `fs_meta`。`RootProvider` trait 给其它 root 来源用，`fs_meta` 不走。
- **不**让 `backend`（`LocalFsBackend` / 未来的 `http-store`）参与任何引用计数——它们继续只负责两态物理存储。
- **不**把 grace pin / 下载会话 pin / 用户 Recursive pin / `fs_anchors` 混进同一张表——四类 root 维度必须独立可重建。
- **不**让 `fs_acquire` 走"只锁根、不 cascade"的偷懒路径（§3.4 / §13.1）。家用 NAS 的语义要求 cascade 必须走完。
- **不**让 `fs_meta` 把每条 dentry 都灌进 `pins` 表（§3.3 / §13.3）。
- **不**在 `fs_anchors` 行里存 path 字符串（§13.2）。`(inode_id, field_tag)` 是 ndm 不可变句柄。
- **不**对外暴露 `insert_incoming_ref` / `remove_incoming_ref`。`incoming_refs` 只能由 `pin` / `fs_acquire` cascade 自动产生。
- **不**让 Skeleton 升档触发追溯 tear-down，也**不**让 unpin Skeleton 自动重新展开 cascade。Skeleton 的语义全部是前向的——见 `named_store_gc.md` §5.6。
- **不**为 cache-first 的 `put_object` 加同步 cascade。`named_store_gc.md` §0 的"`put` 是廉价的、可被淘汰的"承诺不能动。
- **不**为强制 GC 让步红线。`class >= 1` 一律不动。空间不够就 `ENOSPC`，让上层去清 pin / `fs_release` / 引导用户用 Skeleton 反向选择。
