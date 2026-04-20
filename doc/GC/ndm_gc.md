# NDM GC 整合方案（修订版）

## 0. 与 `named_store_gc.md` 的关系

本文档建立在修订后的 `named_store` GC 模型之上：

- 普通 `put` 是 O(1) cache 写入；
- `pin` / `fs_acquire` 是仅有的持久化承诺；
- `incoming_refs` 只服务 anchored 子树；
- `children_expanded` 明确建模“children 是否已展开”；
- `logical_size` 与 `owned_bytes` 分开，水位 / GC 只看 `owned_bytes`；
- `Skeleton` 是硬屏障：加上会撤回已展开子树，去掉会按当前理由自动恢复；
- `fs_acquire(root)` 的事务语义是“root 立即 anchored + 第一层 outbox durable”，整棵子树通过 outbox 最终收敛；
- `evicted / NEED_PULL` 不进入本次 P0 落地范围；
- P0 的乱序安全边界见 `named_store_gc.md` §5.8：P0 要求不做在线 epoch 迁移，或迁移前强制 drain outbox；上层 UI 可见的 per-anchor completeness 见 §5.9，P0 只回答 root 级 `Pending` / `Materializing`。

因此，本文档只讨论两件事：

1. `fs_meta` / `ndm` 应该在什么时机调用 `fs_acquire` / `fs_release`；
2. 如何让这套底层机制与 `fs_meta` 的事务、Overlay 生命周期、以及用户语义对齐。

一句话：

> **`named_store` 负责“对象如何活着 / 如何被回收”；`ndm` 负责“什么时候声明我还持有它”。**

---

## 1. 目标

1. **把 `fs_meta` 对对象的引用，降落为 `named_store` 可独立判断的本地事实。**
2. **默认保持家用 NAS 的直觉语义**：文件系统说“我有这个文件”，最终应表现为这条对象链路在本地被完整保护。
3. **避免 `fs_meta` 运行期参与 GC 判定**：不在候选扫描时 RPC 回调 `fs_meta`。
4. **共享 sqlite 的单进程阶段保证原子性**：`dentry` 变更与 `fs_acquire` / `fs_release` 同事务提交。
5. **明确 P0 / P2 边界**：本次只落地 cache-first + `fs_anchors` + strict cascade；`evicted/NEED_PULL` 另行设计。

---

## 2. 现状盘点

### `named_store`

现有实现只有 `objects` / `chunk_items` 两张表，没有：

- `pins`
- `fs_anchors`
- `incoming_refs`
- `edge_outbox`
- `children_expanded`
- `logical_size / owned_bytes`
- `eviction_class`

因此当前 `remove_object` / `remove_chunk` 是无条件删除，无法表达“对象虽然在文件系统里还有名字，但暂时只是 cache”与“对象已经被上层承诺必须留下”之间的差别。

### `fs_meta`

当前存在一个半成品 `obj_stat`：

- 只在个别路径上做 `+1`；
- 没有对称的 `-1`；
- 无法正确覆盖 Overlay 生命周期；
- 需要 `fs_meta` 在运行期持续维护存储层计数。

这条路应整体放弃。

### `ndm`

`named_file_mgr.rs` 当前有两类危险点：

1. 发布 / finalize 只做 `put_object`，不向底层声明“我还引用它”；
2. `erase_obj_by_id` 直接删对象，没有检查 anchor / incoming / pin。

这两处都必须改。

---

## 3. 为什么是 `fs_acquire = 命名的 Recursive root`

### 3.1 为什么不继续做 `obj_stat`

`obj_stat` 的问题不是“代码没补全”，而是模型本身分层反了：

- 它表达的是存储层 reachability；
- 却要求 `fs_meta` 手工维护加减；
- 一旦某条写路径漏掉 bump/unbump，就必须全表扫描重建；
- GC 想判定对象能不能删，还得反过来问 `fs_meta`。

这会让 `named_store` 永远无法成为“本地独立决策”的模块。

### 3.2 为什么不让 `fs_meta` 实现 `RootProvider`

候选时回调 `is_rooted(obj_id)` 看似简单，但有四个根本问题：

1. **可靠性**：`fs_meta` 一次误返回 `false` 就可能误删；
2. **性能**：GC 候选数 N ⇒ N 次查询 / RPC；
3. **TOCTOU**：GC 问完和真正删除之间，`fs_meta` 可能并发新建引用；
4. **候选集膨胀**：cache-first 下 class 0 候选天然很多，不应该每个都去问 `fs_meta`。

所以 `RootProvider` 只保留给少量、慢变的外部 root；`fs_meta` 绝不走这条路。

### 3.3 为什么不把每条 fs 引用灌进 `pins`

如果把每个 `(obj, inode, field)` 都伪装成一条 pin：

- `pins` 表规模会直接上升到 `dentries` 同阶；
- `pins` 本来承载的是“少量用户 pin / session pin / skeleton pin”，不是文件系统全量反索引；
- `Skeleton` 作为 pin scope 会混入 fs 维度，语义上容易打架；
- 用 `owner = "fs:<inode>:<field>"` 这类字符串也远不如 `(obj_id, inode_id, field_tag)` 三元组主键直接。

所以 `fs_anchors` 必须单独成表。

### 3.4 为什么默认必须 strict cascade

`fs_acquire(file_obj)` 只锁根、不锁 ChunkList / chunks 的设计曾经很诱人，因为它看起来是 O(1)。
但这会直接破坏家用语义：

- 文件系统层看到“我有这个文件”；
- 存储层却允许 chunk 在强制 GC 时被回收；
- 结果是目录还在、文件名还在、真实内容却可能被淘汰。

这不是用户能接受的默认语义。

因此本方案把 `fs_acquire(obj)` 定义为：

> **在 `obj` 上新增一条命名的 Recursive root。**

也就是说：

- root 立刻变成 class 2；
- children 通过 outbox 最终扩展为 class 1；
- 用户需要“不要这一支”时，用 `Skeleton` 明确表达，而不是把默认行为降级成“只锁根”。

---

## 4. 核心设计

### 4.1 `fs_anchors` + `fs_anchor_count`

沿用底层方案：

```sql
CREATE TABLE fs_anchors (
    obj_id     TEXT    NOT NULL,
    inode_id   INTEGER NOT NULL,
    field_tag  INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (obj_id, inode_id, field_tag)
);
CREATE INDEX fs_anchors_by_obj ON fs_anchors(obj_id);
```

并在 `objects` / `chunk_items` 里增加：

- `fs_anchor_count`
- `eviction_class`
- `children_expanded`
- `logical_size`
- `owned_bytes`

其中：

- `fs_anchors` 是权威行集；
- `fs_anchor_count` 是物化列；
- `eviction_class` 由 `pins ∪ incoming_refs ∪ fs_anchors` 共同决定；
- `children_expanded` 决定 children 是否已被展开，不由 `fs_meta` 直接维护。

### 4.2 三个维度各管各的

| 维度 | 承载 | 谁维护 | 如何重建 |
|---|---|---|---|
| `fs_anchors` / `fs_anchor_count` | `fs_meta` 的命名 root | `ndm` 写路径调用 `fs_acquire` / `fs_release` | 扫 `dentries` / `nodes.*_obj_id` |
| `incoming_refs` | anchored 子树里的边 | `named_store` 内部 cascade | 清空后从 roots 重新展开 |
| `pins` | 用户 pin / Skeleton / Lease | `named_store` pin API | 启动时按 owner 策略保留或丢弃 |

这三个维度彼此独立、独立可重建。

### 4.3 `fs_acquire` 的真实语义

`fs_acquire(obj_id, inode_id, field_tag)` 在 home bucket 的事务里做四件事：

1. `upsert_shadow_if_absent(obj_id)`；
2. `INSERT OR IGNORE fs_anchors(...)`；
3. `fs_anchor_count += 1`；
4. 跑 `recompute_eviction_class + reconcile_expand_state`。

注意这里没有单独的“如果第一条 anchor 才 cascade”。
是否产生 add/remove 全由 `reconcile_expand_state` 的 `want/have` 决定：

- `0 -> 1` 会触发展开；
- `1 -> 2` 不会重复展开；
- `1 -> 0` 且无 incoming / recursive pin 时才撤回；
- 加上 `Skeleton` 会让 `should_expand=false`，于是撤回；
- 去掉 `Skeleton` 且上游理由仍在时，会自动恢复展开。

### 4.4 事务语义与收敛语义分开说

`fs_acquire(root)` 返回成功，只能保证：

- root 自己已是 class 2；
- 第一层 outbox 已 durable；
- 任何以后到达的内容都会被 placeholder / `shadow->present` 补展开正确接住。

不能保证“所有跨 bucket 子孙边已经全部 apply 完”。

因此 `ndm` 需要根据场景选择：

- **一般路径**：接受最终一致的收敛；
- **需要严格 UX / 测试边界**：在 commit 之后调用 `await_cascade_idle()`，再向上层报告“整棵子树已收敛”。

### 4.5 Skeleton 在 `ndm` 里的含义

`Skeleton` 现在是**硬屏障**：

- 用户先 `Skeleton(D)`，后 `fs_acquire(ancestor)`：展开在 `D` 处停下；
- 用户先 `fs_acquire(ancestor)`，后 `Skeleton(D)`：`D` 下方已展开的子树会被异步撤回；
- 去掉 `Skeleton(D)` 且上游 root 仍然存在：`D` 会自动恢复展开。

这让“默认全留 + 用户局部反选不留”的产品语义变得一致，而且不再需要额外的 `refresh_anchor()`。

---

## 5. `fs_meta` / `ndm` 的调用点

两个原语：

```rust
pub async fn fs_acquire(
    &self,
    obj_id:    &ObjId,
    inode_id:  u64,
    field_tag: u32,
    tx:        Option<&Transaction>,
) -> NdnResult<()>;

pub async fn fs_release(
    &self,
    obj_id:    &ObjId,
    inode_id:  u64,
    field_tag: u32,
    tx:        Option<&Transaction>,
) -> NdnResult<()>;

pub async fn fs_release_inode(&self, inode_id: u64, tx: Option<&Transaction>)
    -> NdnResult<usize>;
```

### 5.1 六个调用点

| # | 位置 | 语义 | 调用 |
|---|---|---|---|
| 1 | `handle_set_file` | 新建 / 替换文件 dentry 指向 `content_obj_id` | `fs_acquire(new, inode, FIELD_CONTENT)`；如有旧值，先 / 后对称 `fs_release(old, inode, FIELD_CONTENT)` |
| 2 | `handle_set_dir` | 目录 dentry 指向 DirObject | `fs_acquire(new, inode, FIELD_DIR)`；替换旧值时 `fs_release(old, ...)` |
| 3 | `handle_replace_target` | dentry 目标切换（含切 tombstone） | 旧目标 `fs_release(old, ...)`；新目标若是 object 则 `fs_acquire(new, ...)` |
| 4 | `handle_delete_dentry` | 真删 dentry | `fs_release(target_obj, inode, field)` |
| 5 | `publish_dir` / Overlay finalize | `linked/finalized` 对象进入 inode 的持久字段 | `fs_acquire(new_final, inode, FIELD_FINALIZED)`；对旧字段做 `fs_release` |
| 6 | inode 销毁 / orphan inode 清理 | 释放该 inode 上所有对象字段 | `fs_release_inode(inode)` |

### 5.2 `field_tag`

`field_tag` 是小整数枚举，例如：

- `FIELD_CONTENT`
- `FIELD_DIR`
- `FIELD_BASE`
- `FIELD_LINKED`
- `FIELD_FINALIZED`

它只在 `ndm` 侧有业务含义；`named_store` 只把它当幂等键的一部分。

### 5.3 Tombstone 规则

`tombstone` 本身不持有 obj。
因此任何“从 object target 切到 tombstone”的路径，必须做一次对旧 obj 的 `fs_release`。

### 5.4 Overlay / staged commit

建议对 Overlay 生命周期采用“字段显式 acquire / release”的做法：

- `Working`：`base_obj_id` 进入 inode 时，`fs_acquire(base, inode, FIELD_BASE)`；
- `Cooling`：base 仍被持有；
- `Linked / Finalized`：新对象写进 inode 对应字段时，先 `fs_acquire(new, inode, FIELD_FINALIZED)`，再在同一事务里 `fs_release(old, inode, FIELD_BASE)`；
- inode 最终销毁时，`fs_release_inode(inode)`。

这样 Overlay 的每一次“对象字段换绑”都具备完全对称的 acquire/release 对。

---

## 6. 共享 sqlite：单进程阶段的原子性方案

单进程阶段最稳妥的做法仍然是：`fs_meta` 与 `named_store` 共享同一个 sqlite 文件与同一把写锁。

```text
┌──────────────────────────────────────────────┐
│                ndm.sqlite                    │
│                                              │
│  fs_meta.*   ← nodes / dentries / ...        │
│  ns.*        ← objects / chunk_items /       │
│                pins / fs_anchors /           │
│                incoming_refs / edge_outbox   │
└──────────────────────────────────────────────┘
           ▲                      ▲
           │                      │
   fs_meta_service         named_store_db
           └────── 共享 Arc<Mutex<Connection>> ──────┘
```

这样一来：

- `handle_set_file` 更新 dentry + `fs_acquire` 在同一事务里；
- `handle_delete_dentry` 删 dentry + `fs_release` 在同一事务里；
- GC 的 candidate double-check 与并发 acquire/release 自动串行化；
- 不会出现“`dentry` 已经 commit，但 anchor 还没记上”或反过来的半状态。

### 6.1 仍然要接受的异步边界

共享事务只能保证：

- `fs_anchors` 行
- `fs_anchor_count`
- `eviction_class`
- root 的 `children_expanded` 更新
- 第一层 outbox 入队

这些东西一起提交。

它**不能**把跨 bucket 的所有 descendants 也放进同一事务里。因此 outbox 收敛仍然是异步的，这一点不会因为共享 sqlite 而消失。

---

## 7. 一个完整例子：删除 150MB 照片

假设：

- FileObject `F`
- ChunkList `L`
- 5 个 30MB sub-chunk `C1..C5`
- `C2` 与另一张照片共享

### 7.1 写入阶段

1. `put_object(F)` / `put_object(L)` / `put_chunk(Ci)`：都只是 cache 写入，`eviction_class=0`；
2. 写入时自动种一条 `Lease` grace pin，防止 root 在 `fs_acquire` 之前被 LRU 抢走；
3. `handle_set_file(...)` 在共享事务里调用 `fs_acquire(F, inode, FIELD_CONTENT)`；
4. 事务内：
   - `fs_anchors(F, inode, FIELD_CONTENT)` 插入；
   - `F.fs_anchor_count = 1`；
   - `F.eviction_class = 2`；
   - `reconcile_expand_state(F)` 把 `children_expanded(F)` 置 1，并为 `F -> L` 发 add outbox；
5. `apply_edge(add, L <- F)`：`incoming_refs(L,F)` 插入，`L.eviction_class=1`，`children_expanded(L)=1`，继续为 `L -> C1..C5` 发 add；
6. 每个 chunk 收到 add 后都变成 class 1；共享 chunk `C2` 可能本来就有别的 incoming，但 `incoming_refs` 主键唯一，天然幂等。

### 7.2 删除事件

1. `handle_delete_dentry(photo)` 开事务；
2. 读出 target=`F`、inode、field；
3. 调 `fs_release(F, inode, FIELD_CONTENT)`；
4. 事务内：
   - 删除 `fs_anchors(F, inode, FIELD_CONTENT)`；
   - `F.fs_anchor_count: 1 -> 0`；
   - 若无别的 pin / incoming，则 `F.eviction_class: 2 -> 0`；
   - `reconcile_expand_state(F)` 把 `children_expanded(F): 1 -> 0`，为 `F -> L` 发 remove outbox；
5. 删除 dentry；
6. commit。

### 7.3 后续异步收敛

- `apply_edge(remove, L <- F)`：
  - 删除 `incoming_refs(L,F)`；
  - 若 `L` 不再有任何 incoming / anchor，则 `L.eviction_class: 1 -> 0`；
  - `children_expanded(L): 1 -> 0`，为 `L -> C1..C5` 发 remove；
- 对每个 `Ci`：
  - 删除 `incoming_refs(Ci,L)`；
  - `C2` 因为还有别的 incoming，仍保持 class 1；
  - 其他 chunk 回到 class 0。

### 7.4 物理释放

此后空间回收完全按 class 0 的 `owned_bytes` 走：

- `F` 会很快进入 class 0 候选；
- `L` 与 `C1/C3/C4/C5` 要等 remove 收敛后才进入 class 0；
- `C2` 因为共享，还保持 class 1，不会被回收。

如果用户需要“删了立刻见到释放效果”，`ndm` 可在删路径上：

1. commit 删除事务；
2. 等 `await_cascade_idle()`；
3. 再调一次 `forced_gc_until(...)`。

---

## 8. 新写入对象的 Grace 保护

写路径保持 cache-first，不因为 `fs_meta` 的业务时序让 `put_object` 直接变成 cascade 操作。
因此需要一个短期 `Lease`：

```rust
db.put_object(...);
db.pin_internal(
    obj_id,
    owner = format!("lease:put:{}", uuid),
    scope = PinScope::Lease,
    expires_at = now + GRACE_TTL,
    txn,
);
```

`Lease` 的作用：

- 锁住根对象自身；
- 不 cascade；
- 对上游 incoming / `fs_acquire` 透明，不会像 `Skeleton` 那样切断展开。

因此：

- `put_object` 到 `fs_acquire` 之间的短窗口有保护；
- 一旦 `fs_acquire` 成功，grace pin 可主动 drop 或自然过期；
- 下载 session、临时持有也可以复用 `Lease`。

---

## 9. 水位、强制 GC 与容量解释

所有容量计算都只看 `owned_bytes`：

- 普通 blob：`owned_bytes > 0`；
- `SameAs` / `LocalLink` / shadow：`owned_bytes = 0`；
- class 1 / 2 的对象都受红线保护，不因压力被强制删。

在 `ndm` 语境下，“磁盘不够”要理解为：

> 用户当前持有的 dentries / inode 字段所对应的 anchored roots，再加上它们已经收敛展开到本地的子树，总 `owned_bytes` 已经接近或超过设备容量。

这不是 GC 算法的问题，而是：

- 用户需要 `fs_release` 一部分内容；
- 或者对大目录打 `Skeleton`；
- 或者扩容 / 外接存储。

`named_store` 在这里的职责只是不越过红线。

---

## 10. `erase_obj_by_id` 与 `evicted`：移出 P0

这一版修订明确把 `evicted / NEED_PULL` 移出本次落地范围。

原因：

1. 它会把底层 state machine 从三态改成四态；
2. 需要明确“逻辑身份保留但物理副本删除”时，`owned_bytes`、读路径、恢复路径、迁移路径怎样一致；
3. 需要额外定义 `NEED_PULL` 与用户可见错误面；
4. 这是一个独立于 cache-first GC 的 P2 主题。

因此 P0 的结论是：

- `named_file_mgr.rs::erase_obj_by_id` 不能再直接删 anchored / referenced 对象；
- 对外可先禁用，或者只允许作用于 `eviction_class=0 && owned_bytes>0` 的纯 cache 行；
- 用户要释放空间，P0 只支持“`fs_release` / unpin + 等收敛 + GC”。

如果后续必须支持“保留逻辑身份、只删物理副本”，再单独引入 `state='evicted'` 的 P2 方案。

---

## 11. 分布式演进路径

未来 `fs_meta` 与 `named_store` 分进程 / 分节点时，共享 sqlite 不再存在。当前设计已经为此留出三件事：

1. **幂等键**：`fs_anchors` 的主键是 `(obj_id, inode_id, field_tag)`，天然适合重复投递；
2. **写者侧 outbox**：`fs_meta` 可以在本地事务里同时写 `dentries` 变更与 `fs_outbox(op, obj_id, inode_id, field_tag, lsn)`，后台再推送给 `named_store`；
3. **离线重建**：扫描 `dentries` / `nodes.*_obj_id`，重新生成应有的 `fs_anchors` 集合，再让 `named_store` 侧重算 `fs_anchor_count` 并重放 cascade roots。

因此六个调用点的业务代码不需要改；只需要把 `fs_acquire` / `fs_release` 的底层实现从“共享事务”切成“本地 outbox + 异步投递”。

---

## 12. 崩溃恢复

四层恢复保持独立：

1. **文件孤儿扫描**：删掉 DB 不认的本地 blob；
2. **`fs_anchors` 重建**：扫 `dentries` / `nodes.*_obj_id`，恢复 `(obj_id, inode_id, field_tag)` 行集；
3. **`fs_anchor_count` 重建**：按 `fs_anchors` GROUP BY 重算；
4. **`incoming_refs + children_expanded` 重建**：清空边表、把 `children_expanded=0`，然后从 recursive pins 与 `SELECT DISTINCT obj_id FROM fs_anchors` 重新展开。

这四者互不混合，互相不依赖隐式历史。

---

## 13. 实施顺序

1. **先落底层 schema**：`state`、`eviction_class`、`fs_anchor_count`、`logical_size`、`owned_bytes`、`children_expanded`、`incoming_refs`、`edge_outbox`、`pins`、`fs_anchors`。
2. **实现统一 helper**：`recompute_eviction_class`、`reconcile_expand_state`、`upsert_shadow_if_absent`、`has_recursive_pin`、`has_skeleton_pin`。
3. **改写 `put_object` / `put_chunk`**：保持 O(1) cache 写入 + `Lease` grace pin。
4. **实现 `pin` / `unpin` / `fs_acquire` / `fs_release` / `apply_edge`**，全部走同一个状态机。
5. **接入 shared sqlite**：让 `fs_meta_service` 的事务能把 `fs_acquire` / `fs_release` 带进去。
6. **替换六个写路径**：删 `obj_stat`，在 `handle_set_file` / `set_dir` / `replace_target` / `delete_dentry` / finalize / inode destroy 上全部接通 acquire/release。
7. **补后台任务**：outbox sender、LRU flusher、迁移 worker。
8. **最后才考虑 P2**：`evicted / NEED_PULL`。

---

## 14. 与现有代码的差量

| 文件 | 改动 |
|---|---|
| `src/named_store/src/store_db.rs` | 增加 `fs_anchors`、`incoming_refs`、`edge_outbox`、`pins`，以及 `fs_anchor_count` / `eviction_class` / `logical_size` / `owned_bytes` / `children_expanded` 列；实现 `recompute_eviction_class`、`reconcile_expand_state` |
| `src/named_store/src/local_store.rs` | `put_*` 改成 O(1) cache 写入 + `Lease`；实现 `pin` / `unpin` / `fs_acquire` / `fs_release` / `apply_edge`；`Skeleton` 改成硬屏障语义 |
| `src/named_store/src/store_mgr.rs` | 增加 sender / flusher / migration worker；暴露 `await_cascade_idle()` |
| `src/ndn-lib/...` | `HasRefs` trait 与 `parse_obj_refs`；`SameAs` 返回 `[chunk_list_id]` |
| `src/fs_meta/src/fs_meta_service.rs` | 删除 `obj_stat` 相关表与 handler；六个写路径接入 `fs_acquire` / `fs_release`，事务共享 |
| `src/cyfs-lib/src/fsmeta_client.rs` | 删除 `obj_stat_*` API；加入必要的 `fs_acquire/release` 客户端能力（若跨进程）；暴露 `fs_anchor_state` 读接口给 UI 层 |
| `src/cyfs/src/named_file_mgr.rs` | finalize 路径接 `fs_acquire`；`erase_obj_by_id` 在 P0 禁用或只允许纯 cache 对象；按需调用 `await_cascade_idle()` + `forced_gc_until()`；把 `fs_anchor_state` 的 `Pending`/`Materializing` 透给上层 UI |

---

## 15. 追加不变量

在底层文档的不变量之上，`ndm` 侧再追加四条：

1. 任一 `dentry` / `inode field` 的 object 变更，都必须与对应的 `fs_acquire` / `fs_release` 在**同一事务**提交；
2. 对同一个 `(obj_id, inode_id, field_tag)`，`fs_acquire` / `fs_release` 必须完全对称；
3. 用户可见的“删除后释放空间”若依赖整棵子树收敛，则必须显式等待 `await_cascade_idle()`，不能把“root 已 release”误当成“所有 descendants 已可回收”。
4. 上层 UI 在 P0 对 "这个文件 / 目录是不是已经到本地了" 的判断**只能**读 `fs_anchor_state(obj, inode, field)` 返回的 `Pending` / `Materializing`（见 `named_store_gc.md` §5.9），这只回答 root 对象自身是否落地；"整棵子树是否已完整物化"在 P0 没有 per-anchor 答案，只能通过全局 `await_cascade_idle()` 做粗粒度等待。这一语义落差必须在 UI 设计时显式表达，不允许把 `Materializing` 渲染成 "已离线"。

---

## 16. 不打算做的事

- 不继续维护 `obj_stat`；
- 不让 `fs_meta` 在 GC 时充当运行期裁判；
- 不让普通 `put` 直接触发 cascade；
- 不把 `fs_anchors` 混入 `pins`；
- 不在 `fs_anchors` 行里存 path 字符串；
- 不把 `evicted / NEED_PULL` 塞进本次 P0；
- 不保留“Skeleton 只拦未来、不自动恢复”的旧语义；
- 不越过 class 1 / 2 的 GC 红线。
