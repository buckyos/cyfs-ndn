# NDM GC 整合方案

## 目标

把 `doc/named_mgrv2.md`（NDM v2.1 架构）与 `doc/named_store_gc.md`（named_store 的引用计数 GC 原语）整合起来，为整个 NDM 系统落地**端到端的底层 NamedData 释放路径**。

面向的场景与约束：

- **家用而非云盘**：必须真的能删数据，`fs_meta` 不能像传统商用 DFS 那样靠"不删"规避问题。
- **分层的考虑**是分布式系统视角下的**可靠性和性能**：一次删除决策不能依赖跨进程 RPC 回调，GC 候选集的规模不能随 `fs_meta` 的 path 规模线性膨胀。
- **`fs_meta` 的反向索引规模未经实测**：不能假设 v2.1 的 Overlay 一定能把 `dentries` 压得很小，设计必须在"传统规模"下仍然成立。
- **无历史包袱**：`fs_meta` 里之前未完成的 `obj_stat` 方案可以整块放弃，不需要考虑平滑迁移。

## 现状盘点

### `named_store`

- `src/named_store/src/backend.rs`：`NamedDataStoreBackend` 两态协议（`NotExist | Completed`）。`LocalFsBackend`（`src/named_store/src/local_fs_backend.rs`）是本地 loopback 实现，按注释"元数据刻意不落 DB：chunk 是否存在 == 最终文件是否存在"，自身不承担任何引用记账职责。
- `src/named_store/src/store_db.rs`：只有 `chunk_items` 和 `objects` 两张表，**没有 refcount 列、没有 state 列、没有 pins 表**。`remove_chunk` / `remove_object` 无条件删除。
- `src/named_store/src/local_store.rs`：`put_object` / `put_chunk*` 不做任何级联；`remove_*` 没有保护。`ChunkStoreState::LocalLink` 用来挂外部文件，不进 blob 仓。

### `fs_meta`

- `src/fs_meta/src/fs_meta_service.rs`：
  - 第 500–545 行：`nodes` / `dentries` 表；`idx_dentries_target_obj` 索引已存在。
  - 第 550 行：`obj_stat(obj_id, ref_count, zero_since, updated_at)` ——半成品，只有 `handle_set_file` 会 `bump(+1)`，`handle_set_dir` / `handle_replace_target` / `handle_delete_dentry` 都**没有**对应的 `-1`，泄漏严重。
  - 第 3596–3760 行：`obj_stat` 的 RPC handler 保留，但没有被正确地维护。
  - 第 3247 行 `handle_replace_target`、第 3126 行 `handle_delete_dentry`：对 `obj_stat` 零动作。
- `src/ndm-lib/src/fsmeta_client.rs`：`obj_stat_get/bump/list_zero/delete_if_zero` 的客户端方法与 trait handler 仍在（1830–1925 / 2271–2298 / 2571–2595 行）。

### `ndm`

- `src/ndm/src/named_mgr.rs`：
  - 第 1600 行附近 `erase_obj_by_id` 直接调 `layout_mgr.remove_object(obj_id)`，**没有任何安全检查**。
  - 第 1691 行附近 `publish_dir` 调 `layout_mgr.put_object(dir_obj_id, ...)`，但没有对应的引用记账。

**结论**：GC 所需要的基础设施既不在 `fs_meta`，也不在 `named_store`，需要重新划线。

## 为什么两种"直觉方案"都不走

### 方案 A：继续在 `fs_meta` 里做 `obj_stat`（已废弃）

把"某个 obj 被多少个 dentry 引用"这件事写成 `fs_meta` 里的一列，由 `set_file / set_dir / replace_target / delete_dentry / publish_dir` 六个点维护。

**致命问题**：

1. **分层反了**。`obj_stat` 表达的是存储层（named_store）的事实（"这个物理对象还需不需要"），但维护责任全部压在文件系统层，`named_store` 自己无法独立判断能不能删。`named_store` 想做任何安全处理都得反过来调 `fs_meta`。
2. **崩溃恢复困难**。`obj_stat` 是从 `dentries` 归纳出来的"派生状态"，一旦有任何一条写路径漏了 bump/unbump，整张表就失真。要重建就必须全量扫描 `dentries`。
3. **规模不可控**。`obj_stat` 行数与"曾经出现过的 obj"成正比，而不是"当前活着的 obj"成正比。家用场景下快速产生/删除临时对象时，这张表会膨胀。
4. **和 Overlay 生命周期打架**。`Working → Cooling → Linked → Finalized` 中同一个 inode 会先后绑定 `base_obj_id` / `linked_obj_id` / `finalized_obj_id`，`obj_stat` 需要跟着做很多细粒度的加减，极易出错。

这也就是当前实现只在 `handle_set_file` 加了一次 `+1`、其它点都没接的根本原因：方向不对，接不下去。

### 方案 B：`named_store` GC 时通过 `RootProvider::is_rooted` 回调 `fs_meta`

`doc/named_store_gc.md` 里提到的 `RootProvider` trait：GC 找到 `refcount == 0` 的候选后，逐个问 `fs_meta`"你还挂着吗"。在单进程原型下没问题，但在 NDM 的分布式目标下不够：

1. **可靠性**：GC 决策依赖一次 `fs_meta` 的活跃调用。`fs_meta` 不可达 / 超时 / 误返回 `false`，就是一次直接误删——而误删物理数据在家用场景是**不可恢复**的。
2. **性能**：假设候选集有 N 个对象，就要 N 次 RPC + N 次 `fs_meta` 反向索引查询。GC 一轮的延迟与候选集大小成正比，又与 `fs_meta` 的内部索引规模相关。
3. **TOCTOU**：GC 问完 `is_rooted == false`、还没 `remove` 的那一瞬间，`fs_meta` 并发 `set_file(obj_id)`，就会留下一条悬空 path 指向已删 blob。跨进程 RPC 下解决这个问题需要引入分布式锁 / epoch / 两阶段提交，成本很高。
4. **候选集膨胀**：`refcount == 0` 只代表"没有其它 obj 的内容字节指向它"。DAG 的根节点（FS 顶层目录、FileObject 的根 ChunkList）永远 `refcount == 0`，全部会在每一轮 GC 里变成候选，全部要走一次 `fs_meta` 回调。候选集规模 ≈ 活跃根对象数，在"传统规模"下就是 `dentries` 规模，完全不可接受。

团队对分层的顾虑正好落在这两点——可靠性和性能。必须把"FS 层在引用我"这件事从**运行期回调**改成**存储层本地可查的持久状态**。

## 核心设计：`named_store` 双列 refcount

让 `named_store` 自己**就地**持有一个"上层文件系统在引用我"的计数，不依赖外部活跃查询。

### 数据模型

```sql
-- objects / chunk_items 各加三列 + 一个 pins 表

ALTER TABLE objects
    ADD COLUMN refcount     INTEGER NOT NULL DEFAULT 0;  -- 内部内容引用
ALTER TABLE objects
    ADD COLUMN fs_refcount  INTEGER NOT NULL DEFAULT 0;  -- 文件系统 root 引用
ALTER TABLE objects
    ADD COLUMN state        TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE objects
    ADD COLUMN zero_since   INTEGER;                     -- refcount+fs_refcount 归零的时刻

ALTER TABLE chunk_items
    ADD COLUMN refcount     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items
    ADD COLUMN fs_refcount  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items
    ADD COLUMN zero_since   INTEGER;

CREATE TABLE pins (
    obj_id     TEXT NOT NULL,
    owner      TEXT NOT NULL,          -- 'session:xxx' / 'lease:yyy' / 'user'
    created_at INTEGER NOT NULL,
    expires_at INTEGER,                -- NULL = 永久
    PRIMARY KEY (obj_id, owner)
);
CREATE INDEX pins_by_expire ON pins(expires_at) WHERE expires_at IS NOT NULL;

-- 关键：GC 候选的**部分索引**
CREATE INDEX objects_zero_live
    ON objects(obj_id)
    WHERE refcount = 0 AND fs_refcount = 0 AND state = 'present';

CREATE INDEX chunk_items_zero_live
    ON chunk_items(chunk_id)
    WHERE refcount = 0 AND fs_refcount = 0 AND chunk_state = 'completed';
```

三个计数器的语义**完全正交、完全独立可重建**：

| 列 | 承载 | 由谁维护 | 重建方式 |
|---|---|---|---|
| `refcount` | 其它 Present 对象的字节里出现过 `obj_id` | `named_store` 内部 `put_object` / `remove_object` 解析内容自动 cascade | 扫所有 Present 对象 → `parse_obj_refs` → 重新 cascade |
| `fs_refcount` | `fs_meta` 里有多少条 "非 Tombstone、指向该 obj 的 dentry / inode 字段" | `fs_meta` 的写路径显式 `fs_acquire` / `fs_release` | 扫 `fs_meta.dentries` + `nodes.*_obj_id` 字段聚合 |
| `pins` | 临时持有（下载会话、grace lease、手动用户 pin） | `NamedLocalStore::pin` / `unpin` | 启动期按策略决定丢弃还是保留 |

`state` 取值与 `doc/named_store_gc.md` 一致：`present` / `shadow` / `incompleted`。再加一个 `evicted`（见 §`erase_obj_by_id`）。

**`fs_refcount` 列不是对 `fs_meta` 反向索引的复制**：它只存一个**整数**，不存 path、不存 inode 号、不存关系。真正的 `obj_id → {path}` 反向查询仍然在 `fs_meta` 的 `idx_dentries_target_obj` 上；`named_store` 完全不知道 path 的存在。这就是这套方案能同时满足"名分归位"和"GC 不回调 `fs_meta`"的根本原因。

### GC 判定公式

```
alive(X) ⟺ refcount(X) > 0
        ∨ fs_refcount(X) > 0
        ∨ ∃ 未过期 pin 认领 X
```

全部是 `named_store` 自家表的整数比较 / 索引查询，单 SQL 就能拿到候选集：

```sql
-- 一次 GC 扫描的候选集，完全走 objects_zero_live 部分索引
SELECT obj_id
  FROM objects
 WHERE refcount = 0
   AND fs_refcount = 0
   AND state = 'present'
   AND NOT EXISTS (
       SELECT 1 FROM pins p
        WHERE p.obj_id = objects.obj_id
          AND (p.expires_at IS NULL OR p.expires_at > :now)
   );
```

候选集规模 ≈ **最近 release 到 0 的对象数**，**与 `fs_meta` 的 path 规模无关**。即便 `dentries` 有千万行，只要最近只删了 10 个文件，GC 候选集就只有那些对应的 10 条根对象 + 级联释放下去的中间节点。这是满足"团队对分布式可靠性/性能顾虑"的关键指标。

## `fs_meta` 的 `fs_acquire` / `fs_release` 协议

`fs_meta` 只负责在自己的写路径上、显式调用 6 个点，把"我要开始/不再引用这个 `obj_id`"告诉 `named_store`。两个原语都是**幂等的计数加减**，不是集合操作。

```rust
/// 文件系统层声明它开始引用 obj_id。可以重复调用，每次 +1。
pub async fn fs_acquire(
    &self,
    obj_id: &ObjId,
    owner: FsOwner,             // inode 号 + 字段名，用于审计 / 未来的幂等去重
    tx: Option<&Transaction>,   // 传入则共享事务
) -> NdnResult<()>;

/// 对称释放。必须和 acquire 成对。允许降到 0。
pub async fn fs_release(
    &self,
    obj_id: &ObjId,
    owner: FsOwner,
    tx: Option<&Transaction>,
) -> NdnResult<()>;
```

### 六个调用点

映射到 `fs_meta_service.rs` 的现有写路径：

| # | 位置 | 语义 | 调用 |
|---|---|---|---|
| 1 | `handle_set_file` | 新建/替换文件 dentry 指向 `content_obj_id` | `fs_acquire(new)`，如果替换了旧值再 `fs_release(old)` |
| 2 | `handle_set_dir` | 目录 dentry 指向 DirObject | `fs_acquire(new)`，替换时 `fs_release(old)` |
| 3 | `handle_replace_target` | dentry 目标切换（含 tombstone 化） | `fs_release(old)`；若新目标是 obj 类 dentry 则 `fs_acquire(new)` |
| 4 | `handle_delete_dentry` | 真正删除 dentry 行（而非 tombstone） | `fs_release(target_obj)` |
| 5 | `publish_dir`（`ndm/src/named_mgr.rs`）/ Overlay Finalize | `Linked → Finalized`，把 `finalized_obj_id` 写进 inode | `fs_acquire(finalized_obj_id)`；旧 `base_obj_id` 按需 `fs_release` |
| 6 | inode 销毁（垃圾回收 orphan inode） | 该 inode 上所有 `*_obj_id` 字段释放 | 对每个非空字段调 `fs_release` |

**Tombstone 的处理**：Tombstone 自身 (`DENTRY_TARGET_TOMBSTONE = 2`) 不持有任何 obj 引用——它只是个墓碑。`handle_replace_target` 从 `target_obj` 切到 tombstone 时，必须对原 target 调一次 `fs_release`。这是目前代码**完全缺失**的点。

**Staged Commit 的处理**（来自 `named_mgrv2.md` 第 5/6 章）：

- `Working` 态：`base_obj_id` 是 Overlay 的基线，`linked_obj_id` 还没算出。`fs_acquire(base_obj_id)` 在 inode 创建时完成。
- `Cooling` 态：等待内容 publish。`base_obj_id` 仍被持有。期间如果 inode 被删，走点 #6 释放 base。
- `Linked` 态：`linked_obj_id` 已算出但还未写 `finalized_obj_id`。**此时**调 `fs_acquire(linked_obj_id)`；`base_obj_id` 在确认 `linked_obj_id` 登记成功后 `fs_release`。
- `Finalized` 态：`finalized_obj_id = linked_obj_id`，已经是 acquire 过的，不需要再加。

Grace pin 在 `NamedLocalStore::put_object` 自动打上（见 §新写入对象的保护），负责兜底"根对象已经 put 进仓、但 `fs_meta` 还没跑到 acquire"的时间窗。

### `FsOwner` 是做什么的

`FsOwner = (inode_id, field_tag)` 做两件事：

1. **审计**：出问题时可以回溯某一次 `fs_release` 到底来自哪个写路径；
2. **未来演进**：分布式场景下 `(obj_id, owner)` 配合 outbox 表做**幂等去重**，见 §分布式演进路径。

当前单进程实现里，`FsOwner` 只走日志路径，不参与计数逻辑——就是个整数加减。

## 共享 sqlite：单进程阶段的原子性解法

TOCTOU 问题（`fs_meta` 加 path 和 GC 删 blob 并发发生）在单进程 NDM 里用**共享 sqlite**解决：

```
┌──────────────────────────────────────────────┐
│         ndm.sqlite （同一个文件）              │
│                                              │
│   fs_meta.*   ← nodes / dentries / ...       │
│   ns.*        ← objects / chunk_items /      │
│                  pins / ...                  │
└──────────────────────────────────────────────┘
          ▲                    ▲
          │                    │
   fs_meta_service      named_store_db
          │                    │
          └─── 共享 Arc<Mutex<Connection>> ──┘
```

实现上，`NamedLocalStoreDB::new` 要增加一个"接受外部连接"的构造函数：

```rust
impl NamedLocalStoreDB {
    pub fn new(db_path: String) -> NdnResult<Self> { ... }        // 现有：独占连接

    pub fn new_with_connection(
        conn: Arc<Mutex<Connection>>,
    ) -> NdnResult<Self> { ... }                                   // 新增：共享连接
}
```

`fs_meta` 的每个 handler 在开事务后，把 `&Transaction` 顺着传给 `NamedLocalStore::fs_acquire` / `fs_release` / `put_object`，所有操作在同一个事务里提交。这样：

- `handle_set_file` 改一条 `dentry` + 一次 `fs_acquire` → 要么都成，要么都不成。
- GC 跑在 `BEGIN IMMEDIATE` 里，扫描候选 → 校验 → 删除全部在一个事务内。并发的 `fs_acquire` 会被串行化到 GC 事务之外，**不可能**出现"GC 读到 0、`fs_acquire` 插队、GC 删除"的时序。

注意：`fs_meta_service` 当前分两层初始化（sqlite 在 `fs_meta_service.rs`；`NamedLocalStore` 在 `named_mgr.rs`），共享连接需要把 `ndm` 的启动顺序翻过来：**先开一次 sqlite 连接 → 分别把它注入两个模块**。

## 分布式演进路径

一旦 `fs_meta` 与 `named_store` 跨进程/跨节点，共享事务不存在了。本方案为此预留三件东西：

1. **`FsOwner` 作幂等键**。真正分布式化时，`fs_acquire(obj, owner)` 变成"把 `(obj_id, owner)` 插入一张 `fs_refs` 表，成功则 `fs_refcount += 1`"，`fs_release` 对称地按 `(obj_id, owner)` 删行 `-1`。同一个 owner 重放 N 次 acquire 只算一次，RPC 幂等就解决了。
2. **写者侧 outbox**。`fs_meta` 的每次写在本地事务里同时写 `dentries` 改动和 `outbox(op, obj_id, owner, lsn)`；后台 pusher 把 outbox 复制到 `named_store`。`named_store` 根据 `lsn + owner` 做去重。崩溃恢复靠回放 outbox + `rebuild_fs_refcount`。
3. **离线 `rebuild_fs_refcount`**。由 `fs_meta` 端实现的工具：扫一遍自己的 `dentries` 和 `nodes.*_obj_id`，重算每个 `obj_id` 的 `fs_refcount` 目标值，和 `named_store` 里当前值比对、修正。这一步不动 `refcount` 列、不动 `pins`，三者完全解耦。

**单进程阶段**只需要实现 acquire/release 的共享事务版本，outbox/rebuild 作为**占位接口**暂不实现。当 `fs_meta` 和 `named_store` 跨进程时，acquire/release 的内部实现从"共享事务 +1"切换为"写 outbox"，六个调用点的代码完全不动。这正是对"团队对分层的考虑是分布式系统可靠性角度和性能"这一诉求的直接回应：协议现在就按分布式的形状来设计，单进程只是其退化实现。

## 新写入对象的 Grace 保护

`put_object` 把对象落盘后，`refcount = 0` 且 `fs_refcount = 0`——它是个等待被接住的根。直接 GC 会在 `fs_meta` 跑 `fs_acquire` 之前把它删掉。

解法：`NamedLocalStore::put_object` 内部自动种一个 grace pin：

```rust
db.put_object(...)?;
db.pin_internal(
    obj_id,
    owner = format!("lease:put_object:{}", uuid),
    expires_at = now + GRACE_TTL,   // 典型 60s
    txn,
)?;
```

GC 跳过任何未过期 pin。60 秒窗口内 `fs_meta` 只要跑完对应的 `fs_acquire`，pin 就可以被 `fs_meta` 在同一事务里主动 drop（或者让它自然过期，都没影响）。

Staged Commit 的"根对象已到、dir 还在 Cooling"场景由**下载会话 pin**覆盖，见 `named_store_gc.md` §影子对象的乱序到达。

## 端到端删除流程：150MB 照片

假设用户删一个 150MB 的 JPG，它的 FileObject 指向一个 ChunkList（`L`），ChunkList 指向 5 个 30MB 的 sub-chunk `C1..C5`。另外 `C2` 和另一个文件共享。

1. `fs_meta.handle_delete_dentry(photo_dentry)` 开事务。
2. 事务内读出 dentry 的 target = `FileObject_id = F`。
3. 事务内调 `named_store.fs_release(F, owner=(inode,target_obj))`：
   - `F.fs_refcount`: 1 → 0。
   - `F.refcount` 也是 0（没有别的 Present 对象引用它）。
   - `F` 进入 `objects_zero_live` 部分索引。
4. 事务内 `DELETE FROM dentries ...` 或 tombstone。
5. `COMMIT`。**此时 `F` 还活着**，磁盘文件没动。GC 和主写路径解耦，写入热路径零额外阻塞。
6. 下一轮 `run_gc`：
   - 候选集查询命中 `F`。
   - `collect_one(F)`:
     - 读 `F` 内容 → `parse_obj_refs` → `[L]`。
     - cascade `release_ref(L)`: `L.refcount` 1 → 0。
     - `F.state == 'present' ∧ refcount+fs_refcount == 0` → 删 blob 文件、删 `objects` 行。
   - 同一轮 GC 现在发现 `L` 也进了部分索引：
     - `collect_one(L)`:
       - 读 `L` 内容（ChunkList）→ `[C1..C5]`。
       - cascade `release_ref` 到 `chunk_items`: `C1/C3/C4/C5.refcount` 1→0；`C2.refcount` 2→1。
     - 删 `L` 的 blob + 行。
   - 下一步 `chunk_items_zero_live` 有新候选 `C1/C3/C4/C5`（`C2` 仍活着）：
     - 逐个 `backend.remove_chunk` + 删行。
7. GC 一轮结束，报告 `collected = 1 obj + 1 obj(list) + 4 chunks`，150MB 中有 120MB 被释放，30MB（`C2`）因另一个文件仍在引用而保留。

**关键不变量**：

- DB commit 永远发生在磁盘文件删除**之前**（`store_db.rs` 事务提交 → 再 `backend.remove_chunk`）。崩溃在 commit 之后、`backend.remove_chunk` 之前 = 文件孤儿 → 启动期扫描清理。
- 反向不允许（先删文件、再 commit）。
- GC 是迭代式的：一轮把能摘的摘了，下一轮处理这一轮新产生的孤儿。**不走递归**，否则 `BEGIN IMMEDIATE` 下会被自己锁住。

## 一些细节的处理

**去重（dedup）**：`put_object` 对已有 `present` 行短路，不重复 cascade。这意味着同一个 obj 被多条 path 引用时，`refcount` 记一次，`fs_refcount` 记 N 次——这正是两列分离的意义：谁都不需要知道对方怎么算。

**chunks 被多文件共享**：上例中 `C2` 被第二个文件引用；那个文件的 FileObject 在 put 时 cascade add_ref 过 `C2` 所在的 ChunkList，所以 `C2.refcount >= 1`。GC 发现 `C2.refcount > 0`，自动跳过。

**先删后再加（race）**：`fs_meta` 在同一事务内 `fs_release(X)` + `fs_acquire(X)`（例如替换到同一目标），`fs_refcount` 先减后加，GC 根本拿不到"瞬间 0"，因为它看不见未 commit 的中间状态。

**Working/Cooling 态下删 inode**：inode 走的是状态点 #6 的销毁路径，`base_obj_id`（还没 publish 的基线）被 `fs_release`，`linked_obj_id` / `finalized_obj_id` 字段为空，无需动。

**Linked 的 `ExternalFile` / LocalLink**：`named_mgrv2.md` 里 Linked 态下物理数据在 FileBuffer，`named_store` 里的对应行是 `LocalLink`/`ExternalFile`。这些行同样走 `fs_refcount`；GC 把"行"删除时，backend 的 `remove_object` 对 LocalLink 是 no-op（它本来就不拥有物理文件）——正确。

## `erase_obj_by_id` 与 `evicted` 状态

v2.1 第 7.2 节要求：`erase_obj_by_id` 是**容量管理**——把本地物理副本腾出去，但保留元数据和 path，下一次 `open` 应当触发 `NEED_PULL`。这**不是** GC。

为此在 `state` 上加一个 `evicted`：

| state | 物理文件 | DB 行 | `fs_refcount` | `get` 行为 |
|---|---|---|---|---|
| `present` | 存在 | 存在 | 任意 | 返回 |
| `evicted` | 不存在 | 存在 | > 0 | `NotFound → NEED_PULL` |
| `shadow` | 不存在 | 存在 | `fs_refcount == 0` 必为 0 | `NotFound` |
| `incompleted` | 部分 | 存在 | 0 | 写路径用 |

`erase_obj_by_id(obj_id)`：

```rust
// 必要条件：不能擦除一个没人引用的对象——那是 GC 的活
require!(fs_refcount(obj_id) > 0 || pinned(obj_id));
db.update state = 'evicted';            // 先改 DB
backend.remove_object/remove_chunk(...); // 再删物理文件
```

GC 的候选条件里**明确排除** `evicted`：

```sql
WHERE refcount = 0 AND fs_refcount = 0 AND state = 'present'
```

所以 `evicted` 行不会被 GC 当成垃圾删掉——`fs_refcount = 0` 的 evicted 行是非法状态，只能由 `rebuild` 修复。

`ndm/src/named_mgr.rs` 第 1600 行附近的 `erase_obj_by_id` 当前直接 `layout_mgr.remove_object(obj_id)`，需要改成：

```rust
self.local_store.mark_evicted(obj_id).await?;   // DB 行 state=evicted
self.backend.remove_object(obj_id).await?;      // 删物理文件
```

**顺序**：先改 DB state，再删文件。崩溃在中间 = 文件孤儿，由启动扫描兜底；反之会出现"文件已删但 state 还是 present"，GC 会把本来该 NEED_PULL 的行彻底删掉。

## 崩溃恢复

三层独立的恢复：

1. **文件孤儿扫描**：启动时遍历 `backend` 的 `chunks/` 和 `objects/`，对每个文件查 DB。对应行不存在 / state ∉ {`present`} → 删文件。
2. **`refcount` 重建**：离线工具，遍历所有 `state == 'present'` 对象，重算 cascade。零代码依赖 `fs_meta`。
3. **`fs_refcount` 重建**：`fs_meta` 侧工具，扫 `dentries` / `nodes.*_obj_id`，聚合到 `(obj_id, count)`，写 `objects/chunk_items.fs_refcount`。

三者**互不混合**，正是分两列的最大收益——任何一列失真都能独立地机械重算。

## 实施顺序

建议按下列顺序推进，每一步都能独立验证：

1. **`store_db.rs` schema 扩展**：加 `refcount` / `fs_refcount` / `state` / `zero_since` 列 + `pins` 表 + 两个部分索引。内部 `add_ref` / `release_ref` / `fs_acquire` / `fs_release` / `pin` / `unpin` / `list_gc_candidates` / `mark_evicted` 私有方法。
2. **`HasRefs` trait + `parse_obj_refs`**：放 `ndn-lib`，给 `DirObject` / `FileObject` / `RelationObject` / `ChunkList` 实现解析。
3. **`local_store.rs` 写路径改造**：
   - `put_object` / `put_chunk*` 调 `parse_obj_refs` + cascade `add_ref` + grace pin，全部在事务内。
   - 加 32MB 写入 guard（`named_store_gc.md` §32MB 上限）。
   - 实现 `add_chunk_by_same_as`。
4. **`local_store.rs` 删除路径改造**：`remove_*` 改为安全版（检查 `fs_refcount` 与 `refcount` + `state` 转换）。暴露 `run_gc` / `rebuild_refcount`。
5. **`NamedLocalStoreDB::new_with_connection`**：共享 sqlite 连接，打通 `ndm` 启动路径。
6. **`fs_meta_service.rs` 六个调用点**：
   - 先删掉现有的 `obj_stat` 表、`handle_obj_stat_*` handler、`ndm-lib/src/fsmeta_client.rs` 里对应的客户端方法和 trait 条目。
   - 在 `handle_set_file` / `handle_set_dir` / `handle_replace_target` / `handle_delete_dentry` 四个点接入 `fs_acquire` / `fs_release`，事务共享。
7. **`named_mgr.rs` 生命周期点**：
   - `publish_dir` / Overlay Linked→Finalized 切换调 `fs_acquire`。
   - `erase_obj_by_id` 改为 `evicted` 语义。
   - 启动时把 `fs_meta` 的 sqlite 连接共享给 `NamedLocalStore`。
8. **恢复与工具**：启动期文件孤儿扫描；`rebuild_refcount`（纯内容）；`rebuild_fs_refcount`（`fs_meta` 侧）。

`obj_stat` 及其 handler 的删除属于第 6 步内部的清理动作，**不**需要跨步骤兼容窗口——当前系统里它本来就没被正确维护过，直接删。

## 与现有代码的差量

| 文件 | 改动 |
|---|---|
| `src/named_store/src/store_db.rs` | 新增 schema 列、`pins` 表、部分索引；新增 `add_ref` / `release_ref` / `fs_acquire` / `fs_release` / `pin` / `unpin` / `list_gc_candidates` / `mark_evicted` 等内部原语；`remove_chunk` / `remove_object` 从无条件删除改为 state 转换；`new_with_connection` 构造器 |
| `src/named_store/src/local_store.rs` | `put_object` / `put_chunk*` cascade + grace pin + 32MB guard；`remove_*` 安全化；`run_gc` / `rebuild_refcount`；`add_chunk_by_same_as` |
| `src/named_store/src/backend.rs`、`local_fs_backend.rs` | **不动**。backend 继续只负责两态物理存储，GC 逻辑一律走 `store_db`。这是当前 trait 化重构之后的正确分工 |
| `src/ndn-lib/...` | `HasRefs` trait + 各 obj 类型的实现 + `parse_obj_refs` 分发 |
| `src/fs_meta/src/fs_meta_service.rs` | 删 `obj_stat` 表 / `handle_obj_stat_*`（第 550 / 3596–3760 行）；在 `handle_set_file`（3809）/ `handle_set_dir`（3879）/ `handle_replace_target`（3247）/ `handle_delete_dentry`（3126）四个点加 `fs_acquire`/`fs_release` 调用；事务和 `named_store` 共享 |
| `src/ndm-lib/src/fsmeta_client.rs` | 删 `obj_stat_get/bump/list_zero/delete_if_zero`（1830–1925 / 2271–2298 / 2571–2595 行） |
| `src/ndm/src/named_mgr.rs` | `publish_dir`（1691 附近）调 `fs_acquire`；Overlay 状态机在 Linked/Finalized 切换时同步维护；`erase_obj_by_id`（1600 附近）改为 `evicted` 语义；启动时构造共享 sqlite 连接 |

## 备选方案：把 `fs_refcount` 做成 `pins` 行

如果团队严格解读"`named_store` 不应感知任何 FS 状态"，可以把"FS 层引用"也降级成 `pins` 表里的若干行，owner 命名为 `fs:<inode>:<field>`：

- `fs_acquire(obj_id, owner)` = `INSERT OR IGNORE INTO pins(obj_id, owner='fs:inode:field', expires_at=NULL)`。
- `fs_release(obj_id, owner)` = `DELETE FROM pins WHERE obj_id=? AND owner=?`。
- GC 判定公式不变，`fs_refcount > 0` 改成 `EXISTS (SELECT 1 FROM pins WHERE obj_id=? AND owner LIKE 'fs:%')`。

**代价**：

- `pins` 表规模从"活跃临时持有数"涨到"活跃 FS 引用总数"，候选查询要 `NOT EXISTS pins` 子查询而不是整数比较；在"传统 FS 规模"下需要为 `pins(obj_id)` 建常规索引，索引尺寸与 `dentries` 同阶——和主方案想避免的问题是一样的。
- 分布式演进路径仍然成立：`(obj_id, owner)` 天然幂等。
- 优点是 `named_store.objects` schema 干净：只有 `refcount` 一个整数列，"是否被 FS 引用"是 `pins` 的派生事实。

当前推荐主方案（`fs_refcount` 独立列），因为：

1. 单 SQL 候选查询更便宜（整数比较 + 部分索引，不需要 `NOT EXISTS`）；
2. `fs_refcount` 列可以独立 `rebuild_fs_refcount`，不会和 grace lease / 下载会话 pin 这些临时状态纠缠；
3. `FsOwner` 做幂等键可以留在 outbox 层，不必把每条 FS 引用都物化成 `pins` 行。

但备选方案的架构洁净度更高；两种方案的对外 API（`fs_acquire` / `fs_release`）完全一致，将来可以平滑过渡。

## 不变量清单

下列任何一条被打破都算 bug：

1. `refcount(X) == |{Y : Y.state=='present' ∧ X_id ∈ parse_refs(Y)}|`
2. `fs_refcount(X) == fs_meta 里所有非 tombstone、非已删、指向 X 的字段数`
3. `state ∈ {'present', 'shadow', 'incompleted', 'evicted'}`
4. `state == 'shadow' ⇒ fs_refcount == 0 ∧ blob 文件不存在`
5. `state == 'evicted' ⇒ fs_refcount > 0 ∧ blob 文件不存在`
6. `state == 'present' ⇒ blob 文件存在`
7. DB commit 永远先于磁盘文件删除
8. 引用图始终是 DAG（由 SHA-256 内容寻址保证）
9. 任何一次 `fs_acquire` / `fs_release` 与触发它的 `dentries` / `nodes` 写入在**同一事务**内
10. GC 的单轮候选集大小上界 = 上一轮至今新发生 release 到 0 的对象数，**与 `fs_meta` 规模无关**

## 不打算做的事

- **不**在 `fs_meta` 重建 `obj_stat`。
- **不**让 `named_store` 的 GC 在运行期通过 RPC 询问 `fs_meta`。
- **不**让 `backend`（`LocalFsBackend` 及未来的 `http-store` 实现）参与任何引用计数——它们继续只负责两态物理存储。
- **不**把 grace pin / 下载会话 pin 与 `fs_refcount` 混进同一列。
- **不**对外暴露 `add_ref` / `release_ref` / `fs_acquire` / `fs_release`。它们只能被 `put_object` / `remove_*` / `fs_meta` 的写路径**自动**触发；对外暴露会允许调用方伪造引用关系。
- **不**在单进程阶段做 outbox / 两阶段提交。共享 sqlite + `FsOwner` 幂等键已经足够，outbox 只在分布式化时打开。
