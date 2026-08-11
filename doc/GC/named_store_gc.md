# named_store GC 方案（修订版）

## 0. 核心模型：缓存优先，Pin / fs_anchor 是持久化承诺

`named_store` 在产品形态上**首先是一个内容缓存**，不是权威数据库。
普通 `put_object` / `put_chunk` 默认只把内容放进 cache 层，不写 `incoming_refs`，不触发 cascade；超过水位后可按 LRU 淘汰。
**只有显式持久化承诺**——用户 `pin`，或上层文件系统调用 `fs_acquire` 产生 `fs_anchor`——才会让对象进入“受保护”状态，并驱动引用边的异步展开。

整套设计坚持一句话：

> **`put` 是廉价的、可被淘汰的；`pin` / `fs_anchor` 是昂贵的、需要被守护的。**

### 0.1 三档存活语义

| 档位 | 来源 | 本地判定 | 何时可回收 |
|---|---|---|---|
| **0 · cache** | 普通 `put`，无任何 anchor | 无 active pin、`fs_anchor_count=0`、`incoming_refs=0` | 任意水位下都可回收 |
| **1 · referenced** | 处于某个 anchored root 的 cascade 子树内 | `incoming_refs(X) != ∅`，但自身没有 active pin / fs_anchor | 当所有入边撤回后回到 class 0 |
| **2 · anchored** | 直接被 pin（任意 scope）或 `fs_anchor_count(X) > 0` | `has_active_pin(X) || fs_anchor_count(X) > 0` | 仅显式 unpin / `fs_release` |

强制 GC 的**红线**非常明确：

> **强制 GC 永远不动 class 1 / class 2。**
>
> class 0 清空之后仍然不够，就返回 `ENOSPC`；这代表容量配置问题，而不是 GC 算法应该替用户“撕掉承诺”。

### 0.2 “对象自己活着”与“children 是否展开”必须分开建模

这版修订最大的变化是：

- `eviction_class` 只描述**对象自己**是否受保护；
- `children_expanded` 单独描述“这个对象的 children 边是否已经向下展开”。

这是共享 DAG 下 correctness 的关键。一个对象 `D` 可能同时被多个父节点引用：

- `P1 -> D`
- `P2 -> D`
- `D -> X`

这时删掉 `(D, P1)` 以后，只要 `(D, P2)` 还在，`D -> X` 就不应该被撤回。单靠“当前处理的是 add 还是 remove”不够，必须显式记录 `D` 的 children 当前是否处于已展开状态，并在本地事务里做一次**状态重算**。

### 0.3 `Skeleton` 是硬屏障，不是“只拦未来”的软注释

`Skeleton` pin 的语义统一成：

- 锁住目标自身（class 2）；
- **阻止任意 cascade 穿过自己**；
- 如果子树之前已经被展开，则在本地事务里把 `children_expanded` 从 `1 -> 0`，随后通过 outbox **异步撤回**整棵子树下方的边。

反过来，去掉 `Skeleton` 以后，如果当前仍然存在上游 incoming、直接 `Recursive` pin、或 `fs_anchor` 这类展开理由，则会自动把 `children_expanded` 从 `0 -> 1` 并重新展开。

这个模型比“只对未来生效、不追溯 tear-down”更简单，也更一致：`Skeleton` 是否存在，永远直接决定“这个节点下方能不能继续展开”。

### 0.4 `fs_acquire` 的 contract：root 立即生效，子树通过 outbox 收敛

`fs_acquire(root)` / `pin(root, Recursive)` 提交成功后，**root 自己**立刻进入 class 2；
children 的保护通过 `edge_outbox` 在各自 home bucket 上逐步收敛。

因此需要区分两层语义：

1. **事务语义**：root 的 anchor 已经落库，第一层 add/remove outbox 已经 durable；
2. **收敛语义**：整棵子树的 `incoming_refs` 已经全部到位或撤完。

对用户或上层模块，如果某个操作必须在“整棵子树都已受保护/都已释放”之后才算完成，应调用显式的观测接口，例如 `await_cascade_idle()`（见 §12）。

---

## 1. 目标

1. **缓存优先**：普通 `put` 保持 O(1)；不写 `incoming_refs`，不做 cascade。
2. **Pin 与 fs_anchor 是唯一持久化路径**：只有这两类承诺能把对象从 class 0 拉到 class 1/2。
3. **共享 DAG 下 remove 正确**：多个父节点共享子树时，不因单条 remove 错撤整棵下游边。
4. **乱序到达可恢复**：允许“先 anchor / 先收到边，再收到对象内容”；shadow 占位 + 状态重算保证最终一致。
5. **大小语义明确**：`logical_size` 与 `owned_bytes` 分开记账，GC / 水位 / `ENOSPC` 只看实际占用。
6. **崩溃可恢复**：`pins`、`fs_anchors`、`incoming_refs`、`children_expanded`、outbox 都能重建。
7. **兼容 Maglev 分片**：每个 bucket 只根据本地事实做 GC 与边处理；跨 bucket 通过 outbox 异步传播。
8. **大 Chunk 必须走 ChunkList + SameAs**：不允许把 >32MB 的大 chunk 当单 blob 直接落盘。

---

## 2. 设计原理

### 2.1 内容寻址 ⇒ 引用图天然 DAG

所有 ObjId / ChunkId 都是内容哈希。`A` 引用 `B` 等价于 `B_id` 被写入 `A` 的内容中，因此 `B_id` 必须先于 `A_id` 存在。除非出现哈希碰撞，否则不可能形成环。

直接结论：

- 自引用不可能；
- 引用图天然是 DAG；
- **不需要 mark-sweep**，纯 refcount / edge-set 足以表达 reachability；
- 如果未来引入“可变命名指针”，它不能作为 DAG 节点进入 `incoming_refs`，只能作为外部 root 存在。

### 2.2 本地只看四类事实

在任意 bucket 上，决定一个对象本地状态的只有四类事实：

1. 自己是否有 active pin；
2. `fs_anchor_count` 是否大于 0；
3. `incoming_refs` 是否非空；
4. `state` / `children_expanded` / `owned_bytes` 这类本地物化列。

GC、`apply_edge`、`put_object`、迁移与恢复都只依赖这些本地事实；不在 GC 时做跨 bucket 查询，也不在运行期问 `fs_meta`。

### 2.3 `children_expanded` 的本地判定

定义三个派生谓词：

```text
has_self_anchor(X)   = has_active_pin(X) || fs_anchor_count(X) > 0
has_expand_root(X)   = has_recursive_pin(X) || fs_anchor_count(X) > 0
should_expand(X)     = state(X) == 'present'
                     && !has_skeleton_pin(X)
                     && (has_expand_root(X) || has_incoming(X))
```

其中：

- `has_self_anchor` 决定 `eviction_class == 2`；
- `has_expand_root` 决定“X 自己是不是一个展开根”；
- `should_expand` 决定 children 当前**应不应该**处于展开状态。

然后用一个统一的 helper：

```text
reconcile_expand_state(X):
    want = should_expand(X)
    have = children_expanded(X)

    if want && !have:
        children_expanded(X) = 1
        enqueue add(X -> each child)

    if !want && have:
        children_expanded(X) = 0
        enqueue remove(X -> each child)
```

这一条规则贯穿 pin / unpin / `fs_acquire` / `fs_release` / `apply_edge` / `put_object(shadow->present)` 六条路径，是本设计里最关键的不变量维护点。

---

## 3. 分片环境下的结论

`NamedStoreMgr` 通过 Maglev 一致性哈希把 ObjId 映射到某个 `NamedLocalStore` bucket。直接后果：

1. 父子对象几乎一定不在同一 bucket；
2. 不存在跨 bucket sqlite 事务；
3. add/remove 一定会乱序、会重放、会失败重试；
4. GC 必须完全本地独立；
5. 所有跨 bucket 副作用都必须通过 durable outbox 表达。

因此：

- `incoming_refs` 必须是**以边为主键的集合表**，不能只是一个整数列；
- 普通 `put` 不写边；
- 只有“anchor 触发的展开”才写 `edge_outbox`；
- `children_expanded` 的变更也只能通过本地事务落库，再由 outbox 异步向下扩散。

---

## 4. 数据结构

### 4.1 对象 / Chunk 表扩展

```sql
ALTER TABLE objects     ADD COLUMN state              TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE objects     ADD COLUMN last_access_time   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN eviction_class     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN fs_anchor_count    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN logical_size       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN owned_bytes        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN children_expanded  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN home_epoch         INTEGER;

ALTER TABLE chunk_items ADD COLUMN state              TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE chunk_items ADD COLUMN last_access_time   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN eviction_class     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN fs_anchor_count    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN logical_size       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN owned_bytes        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN children_expanded  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN home_epoch         INTEGER;

CREATE INDEX objects_lru_present     ON objects(eviction_class, state, last_access_time);
CREATE INDEX chunk_items_lru_present ON chunk_items(eviction_class, state, last_access_time);
```

`state`：

| state | 含义 | 可读 | 是否可被 incoming 指向 |
|---|---|---|---|
| `present` | 内容已落盘 | 是 | 是 |
| `incompleted` | 写入中 | 否 | 否 |
| `shadow` | 占位符，无内容 | 否 | 是 |

`logical_size` / `owned_bytes`：

| 字段 | 含义 | 用途 |
|---|---|---|
| `logical_size` | 这个对象在内容层面的逻辑大小 | 审计、上层统计、SameAs 校验 |
| `owned_bytes` | 这个 bucket 实际拥有并会被 GC 回收的字节 | 水位、GC、`ENOSPC` 的唯一依据 |

典型取值：

| 类型 | `logical_size` | `owned_bytes` |
|---|---:|---:|
| 普通 present blob / inline object | 实际内容大小 | 实际落盘字节 |
| `SameAs(big_chunk -> chunk_list)` | big chunk 的逻辑大小 | 0 |
| `LocalLink` | 逻辑大小可按外部文件长度记 | 0 |
| `shadow` | 0 或未知 | 0 |
| `incompleted` | 已知则填，否则 0 | 0 |

`eviction_class` 的物化规则保持不变：

```text
if has_active_pin(X) || fs_anchor_count(X) > 0:    eviction_class = 2
elif has_incoming(X):                               eviction_class = 1
else:                                               eviction_class = 0
```

新增的 `children_expanded` 不是 GC class，而是“children 边当前是否已经物化展开”的状态位。

### 4.2 `incoming_refs`：只服务 anchored 子树

```sql
CREATE TABLE incoming_refs (
    referee        TEXT NOT NULL,
    referrer       TEXT NOT NULL,
    declared_epoch INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (referee, referrer)
);
CREATE INDEX incoming_refs_by_referee ON incoming_refs(referee);
```

这张表**只**由两类展开根写入：

1. `pin(_, Recursive)`；
2. `fs_acquire(_)`。

普通 `put` 不写 `incoming_refs`。

### 4.3 `edge_outbox`：跨 bucket 的 durable 副作用

```sql
CREATE TABLE edge_outbox (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    op           TEXT NOT NULL,         -- 'add' | 'remove'
    referee      TEXT NOT NULL,
    referrer     TEXT NOT NULL,
    target_epoch INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_try_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX edge_outbox_ready ON edge_outbox(next_try_at);
```

任何会影响 reachability 的操作——`pin` / `unpin` / `fs_acquire` / `fs_release` / `reconcile_expand_state`——都必须在同一本地事务里把 outbox 一起落库。

### 4.4 `pins`

```sql
CREATE TABLE pins (
    obj_id         TEXT NOT NULL,
    owner          TEXT NOT NULL,
    scope          TEXT NOT NULL,          -- 'recursive' | 'skeleton' | 'lease'
    cascade_state  TEXT NOT NULL DEFAULT 'Pending', -- 'Pending' | 'Materializing'
    created_at     INTEGER NOT NULL,
    expires_at     INTEGER,
    PRIMARY KEY (obj_id, owner)
);
CREATE INDEX pins_by_owner            ON pins(owner);
CREATE INDEX pins_by_expire           ON pins(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX pins_recursive_by_obj    ON pins(obj_id) WHERE scope = 'recursive';
CREATE INDEX pins_skeleton_by_obj     ON pins(obj_id) WHERE scope = 'skeleton';
```

`cascade_state` 的 P0 取值与语义见 §5.9；在 P0 范围内 `Skeleton` / `Lease` 行的状态永远是 `Materializing`（它们不依赖子树物化）。

三种 scope：

| Scope | 锁自己 | 让自己成为展开根 | 屏蔽穿过自己的展开 | 典型用途 |
|---|---|---|---|---|
| `Recursive` | ✓ | ✓ | ✗ | 用户“完整保存这棵子树” |
| `Skeleton` | ✓ | ✗ | **✓** | 用户“这一支留个壳，不要内容” |
| `Lease` | ✓ | ✗ | ✗ | grace pin / session / 下载持有 |

规则：

- `(obj_id, owner)` 主键保证幂等；
- 同一对象可以被多个 owner 同时 pin；
- `Skeleton` 在本节点上优先级最高：只要存在任意 skeleton pin，`should_expand(X)` 就为 false；
- `Lease` 只影响对象自身的 class，不影响 children 展开。

### 4.5 RootProvider

--删除，不使用旧的RootProvider设计

### 4.6 `fs_anchors`

```sql
CREATE TABLE fs_anchors (
    obj_id         TEXT    NOT NULL,
    inode_id       INTEGER NOT NULL,
    field_tag      INTEGER NOT NULL,
    cascade_state  TEXT    NOT NULL DEFAULT 'Pending', -- 'Pending' | 'Materializing'
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (obj_id, inode_id, field_tag)
);
CREATE INDEX fs_anchors_by_obj ON fs_anchors(obj_id);
```

语义：`fs_acquire(obj_id, inode_id, field_tag)` 等价于“`fs_meta` 在这个 obj 上声明了一条命名的 Recursive root”。

关键点：

- `fs_anchors` 是权威行集；
- `fs_anchor_count` 是物化列，只为快速判活与 GC 过滤存在；
- `fs_anchors` 不存 path，不存 inode 名字，只存 `(obj_id, inode_id, field_tag)`；
- `fs_acquire` / `fs_release` 与用户 `pin(_, Recursive)` 共用同一套 `reconcile_expand_state` + `incoming_refs` + outbox 机制；
- `cascade_state` 只表达"这条 anchor 自己的 root 对象是不是已经到货"，供上层 UI 判断"我能读这个 root 的内容了吗"，详见 §5.9。整棵子树的完成度（`Complete` / `Broken`）及其 verifier 是 P1 专题，P0 不实现。

---

## 5. 统一协议：Pin / fs_anchor / apply_edge / put_object

### 5.1 中央 helper：`recompute_eviction_class` 与 `reconcile_expand_state`

```rust
fn recompute_eviction_class(obj_id, txn):
    if db.has_active_pin(obj_id) || db.fs_anchor_count(obj_id) > 0:
        db.set_eviction_class(obj_id, 2)
    elif db.has_incoming(obj_id):
        db.set_eviction_class(obj_id, 1)
    else:
        db.set_eviction_class(obj_id, 0)

fn reconcile_expand_state(obj_id, txn):
    if !db.is_present(obj_id):
        want = false
    else:
        want = !db.has_skeleton_pin(obj_id)
            && (db.has_recursive_pin(obj_id)
                || db.fs_anchor_count(obj_id) > 0
                || db.has_incoming(obj_id))

    have = db.children_expanded(obj_id)

    if want && !have:
        db.set_children_expanded(obj_id, true)
        for child in parse_obj_refs(obj_id):
            enqueue_outbox('add', referee=child, referrer=obj_id, epoch)

    if !want && have:
        db.set_children_expanded(obj_id, false)
        for child in parse_obj_refs(obj_id):
            enqueue_outbox('remove', referee=child, referrer=obj_id, epoch)
```

只要任何事务改变了以下任一条件，就必须调用它：

- `pins` 行发生变化；
- `fs_anchor_count` 变化；
- `incoming_refs` 插入或删除；
- `state` 从 `shadow -> present`；
- `Skeleton` pin 的有无发生变化。

### 5.2 `pin(obj_id, owner, scope, ttl)`

```rust
fn pin_local(obj_id, owner, scope, ttl):
    txn.begin()
        db.upsert_shadow_if_absent(obj_id)      // 允许先 pin，后到货
        db.upsert_pin(obj_id, owner, scope, ttl)
        // §5.9: Recursive 用 root 的 state 决定初始 cascade_state；
        //       Skeleton / Lease 永远直接写 Materializing
        state0 = if scope != 'recursive' || db.is_present(obj_id) {
            'Materializing'
        } else {
            'Pending'
        }
        db.set_pin_cascade_state(obj_id, owner, state0)
        recompute_eviction_class(obj_id, txn)
        reconcile_expand_state(obj_id, txn)
    txn.commit()
```

关键点：

- **先插 placeholder**：即便 root 还没到本地，也必须先让它有一行 shadow，这样 class 2 和后续补 cascade 才有落点；
- `Recursive` / `Skeleton` / `Lease` 都通过同一条路径；
- `Skeleton` 的效果不是“pin 时不写 outbox”那么简单，而是通过 `reconcile_expand_state` 让 `children_expanded` 收敛到正确值。

### 5.3 `unpin(obj_id, owner)`

```rust
fn unpin_local(obj_id, owner):
    txn.begin()
        if !db.delete_pin(obj_id, owner):
            txn.commit(); return

        recompute_eviction_class(obj_id, txn)
        reconcile_expand_state(obj_id, txn)
    txn.commit()
```

共享 DAG 下不需要显式“针对某个 owner 的 remove walk”。只要 `reconcile_expand_state` 看到 `should_expand(obj_id)` 变成 false，就会为当前 children 集合一次性发 remove；如果还有别的 incoming / direct root 在，`should_expand` 仍为 true，则不会误撤。

### 5.4 `apply_edge(msg)`

```rust
fn apply_edge(msg):
    txn.begin()
        db.upsert_shadow_if_absent(msg.referee)

        if msg.op == 'add':
            db.insert_incoming_ref_if_absent(msg.referee, msg.referrer, msg.epoch)
        else:
            db.remove_incoming_ref(msg.referee, msg.referrer)

        recompute_eviction_class(msg.referee, txn)
        reconcile_expand_state(msg.referee, txn)
    txn.commit()
```

这版修订取消了“add/remove 手写对子节点继续下钻”的分支逻辑，统一改成：

1. 先修改本节点的 `incoming_refs`；
2. 再跑 `reconcile_expand_state(referee)`；
3. 是否向 children 发 add/remove，完全由 `want/have` 决定。

这样自然解决：

- 多父共享 DAG；
- `Skeleton` 新增时对子树的撤回；
- `Skeleton` 移除时的自动恢复；
- “先 shadow、后到货”的补展开；
- add/remove 重放时的幂等性。

### 5.5 `put_object` / `put_chunk`：普通 put 仍然是 O(1)

```rust
fn put_object(obj_id, content, epoch):
    txn.begin()
        row = db.lookup(obj_id)
        match row:
            None:
                db.insert_present(obj_id,
                                  logical_size = content.len(),
                                  owned_bytes  = bytes_written,
                                  content      = content)
            Some(r) if r.state == 'shadow':
                db.update_shadow_to_present(obj_id,
                                            logical_size = content.len(),
                                            owned_bytes  = bytes_written,
                                            content      = content)
                // §5.9: shadow -> present 同事务里把所有指向本对象的
                // fs_anchors / recursive pins 的 cascade_state 从
                // Pending 推进到 Materializing
                db.promote_anchor_cascade_state_on_present(obj_id)
            Some(r) if r.state == 'present':
                db.touch_lru(obj_id)
                txn.commit(); return

        db.touch_lru(obj_id)
        recompute_eviction_class(obj_id, txn)
        reconcile_expand_state(obj_id, txn)
    txn.commit()
```

要点：

- 对普通 class 0 对象，`reconcile_expand_state` 只会看到 `want=false`，不会产生任何 outbox；
- 对已经被 pin / `fs_acquire` / incoming 命中的 shadow，对象一旦落地就会自动补展开；
- 这也覆盖了“先 direct root placeholder，后对象到货”的场景。

### 5.6 `fs_acquire` / `fs_release`

```rust
fn fs_acquire_local(obj_id, inode_id, field_tag):
    txn.begin()
        db.upsert_shadow_if_absent(obj_id)
        inserted = db.insert_fs_anchor(obj_id, inode_id, field_tag)
        if inserted:
            db.fs_anchor_count_inc(obj_id)
            // §5.9: 最小 per-anchor completeness
            state0 = if db.is_present(obj_id) { 'Materializing' } else { 'Pending' }
            db.set_fs_anchor_cascade_state(obj_id, inode_id, field_tag, state0)
        recompute_eviction_class(obj_id, txn)
        reconcile_expand_state(obj_id, txn)
    txn.commit()

fn fs_release_local(obj_id, inode_id, field_tag):
    txn.begin()
        deleted = db.delete_fs_anchor(obj_id, inode_id, field_tag)
        if deleted:
            db.fs_anchor_count_dec(obj_id)
        recompute_eviction_class(obj_id, txn)
        reconcile_expand_state(obj_id, txn)
    txn.commit()
```

注意这里不再显式写“第一条 anchor 才发 add / 最后一条 release 才发 remove”。
是否产生 add/remove 全交给 `reconcile_expand_state`：

- `fs_anchor_count: 0 -> 1` 会让 `should_expand` 变 true，于是展开；
- `fs_anchor_count: 1 -> 2` 不改变 `should_expand`，于是无副作用；
- `fs_anchor_count: 1 -> 0` 且没有 incoming / recursive pin 时，`should_expand` 变 false，于是撤回；
- `fs_anchor_count: 2 -> 1` 不改变 `should_expand`，于是无副作用。

### 5.7 Skeleton 的正式语义

三种典型情况现在都由同一条规则描述：

**Case A：先 Skeleton，后上游 anchor / incoming 到达**

- `has_skeleton_pin(D)=true`；
- `should_expand(D)=false`；
- `D` 自身可以是 class 2 / class 1，但 children 不会展开。

**Case B：先上游展开，后加 Skeleton**

- 原本 `children_expanded(D)=1`；
- 新增 skeleton 后，`should_expand(D)` 变 false；
- `reconcile_expand_state(D)` 把 `children_expanded` 改成 0，并为 D 的直接 children 发 remove；
- 更深层的撤回由这些 children 的 `apply_edge(remove)` 继续逐层传播。

**Case C：移除 Skeleton，而上游理由仍然存在**

- 只要 `has_incoming(D)`、`has_recursive_pin(D)` 或 `fs_anchor_count(D)>0` 仍然为真；
- `should_expand(D)` 会重新变成 true；
- `reconcile_expand_state(D)` 自动恢复展开，不需要额外 `refresh_anchor()`。

这种语义是**严格前向屏障 + 自动收敛**，比“只拦未来、不追溯、不自动恢复”更一致，也更容易在代码里验证。

#### 5.7.1 追溯 tear-down 的代价与流控

Case B（先 cascade，后加 Skeleton）下的追溯 tear-down 并非 O(1)。总工作量与被拦截子树里的边数成正比，只是通过 outbox 摊到后台：

- `reconcile_expand_state(D)` **只在本事务里** enqueue `D` 的第一层 remove outbox；
- 每一个直接 child 收到 remove 后各自跑 `reconcile_expand_state`，若自己也失去最后一个展开理由，就继续向下一层 enqueue remove；
- 整棵被剪掉的子树以“每层 outbox → 每层本地事务 → 下一层 outbox”的方式逐层收敛，**不存在一次性 walk 子树**，因此单次事务的写入量与 `|children(D)|` 同阶，不是 `|subtree(D)|` 同阶。

但**总**跨 bucket 流量仍然是 `O(子树边数)`。在"对百万文件目录上的高频 Skeleton 切换"这类极端场景下，outbox sender 会长时间满负荷运转。工程上的应对：

- 对 `edge_outbox` 实现 sender 侧的速率上限（每秒投递条数、并发 inflight 上限），避免一次 Skeleton 切换把整个 NDM 的跨 bucket 通道打满；
- 上层在构造 UI 时，把 Skeleton 的瞬时切换视作"异步生效"，不要期望它在前台交互时间尺度内完成；
- `await_cascade_idle()` 可以用来观测这种大规模 tear-down 的真正完成时刻。

这些约束和上层 `fs_acquire` 对大目录的首次展开是同一类代价——对称地付出、对称地被摊到后台。

### 5.8 迟到消息与 epoch 迁移的 P0 边界

本修订版刻意**不**再引入 `cascade_gen` / `incoming_cascade_state` 这一整套单调时钟过滤机制。代价是在某些跨 epoch / 迁移 / tombstone 转发 / 长时间重试叠加的场景下，一条早于当前状态的 `add` 可以通过 `apply_edge` 成功写入 `incoming_refs`，从而把一条已经被 `remove` 清掉的边"复活"一次。下面明确 P0 的边界与取舍。

#### 5.8.1 `reconcile_expand_state` 对常规乱序是幂等的

在**同一 epoch 内**的正常乱序、重放、重试场景，新模型天然正确：

- `apply_edge(add)` 与 `apply_edge(remove)` 都是对 `incoming_refs(referee, referrer)` 这一个主键行的 upsert / delete，天然幂等；
- 最终状态只由"当前 `incoming_refs` 行集 + 当前 pins + 当前 fs_anchors"决定，不依赖消息到达顺序；
- 多父共享 DAG、Skeleton 新增/移除、shadow→present 补展开，全都通过 `reconcile_expand_state` 的 `want/have` 收敛到正确值。

单 epoch 内不存在"过期消息复活"——源 bucket 在发出 remove 之前必定先发出过 add，两条消息都只针对同一 `(referee, referrer)` 键，`reconcile_expand_state` 读到的始终是最新的行集。

#### 5.8.2 跨 epoch 迁移是本修订版不直接处理的问题

现版 `doc/named_store_gc.md` §4.7 / §5.3 引入 `cascade_gen` 的原因，是下列序列在**跨 epoch 迁移 + tombstone 转发 + outbox 重试**叠加时会出现：

```
t0  旧 epoch：referrer R 在 bucket B_old 上发出 add(S<-R, seq=1)，未送达
t1  epoch 切换：R 迁到 B_new；B_old 留下 tombstone
t2  新 epoch：R 在 B_new 上发出 remove(S<-R, seq=2)，投递成功
t3  B_old 的 tombstone 终于把 add(S<-R, seq=1) 转发出去，晚于 t2 到达
t4  apply_edge 看到 add，重新写入 incoming_refs(S,R) —— 本不该存在
```

本修订版把 `cascade_gen` / `last_seen_gen` / `declared_gen` 三件事一起删掉之后，**必须**接受下面两条之一作为 P0 的前置假设：

1. **P0 不做 epoch 迁移**。`NamedStoreMgr` 在 P0 保持单 layout，不做 Maglev 重平衡 / compact 切换。`home_epoch` 列仍然保留作为未来字段，但运行期不参与决策。或者，
2. **P0 做 epoch 迁移，但要求在切换前 outbox 必须整体 drain 完毕**。迁移 worker 切换 `current_epoch` 之前 `await_cascade_idle()`，拒绝在有 in-flight outbox 时推进 epoch；同时停掉 tombstone 的异步转发功能。

这两条的实际效果都是把"跨 epoch 乱序消息"这个问题在发送侧消除掉。

#### 5.8.3 何时必须重新引入 gen 模型

下面任一条件被打破时，P0 的保证就不够用，必须升级为带 gen 的版本（或其它等价方案）：

- 引入在线 Maglev 重平衡 / 分片迁移；
- 引入跨设备 tombstone 转发；
- `edge_outbox` 的最长重试窗口超过一次 epoch 切换的最短间隔；
- 出现"同一对 `(referee, referrer)` 的 add/remove 可以被不同 epoch 的 writer 并发生成"的场景。

**这是 P1 的显式专题**，见 §14.2。在 P1 补回 gen 之前，任何向 P0 系统引入上述能力的 PR 都必须同时回答本节的乱序问题——不要悄悄地绕开。

> 历史注解：现版 `doc/named_store_gc.md` 在有 gen 机制的前提下也运行良好，gen 模型本身没有错，只是与本修订版的"`reconcile_expand_state` 零历史依赖"简化方向耦合过紧。P1 重新引入时应优先考虑一种"可以关掉、只在迁移启用"的侵入性更小的形式，例如把 gen 记进 `edge_outbox` 与 `incoming_refs`，而 `reconcile_expand_state` 仍然保持现在的 want/have 语义。

### 5.9 最小 per-anchor completeness（P0 只做 root 级）

P0 不实现完整的子树完成度 verifier，但保留一列 `cascade_state` 让上层可以回答一个最基本的问题：

> "我刚刚 `fs_acquire` / `pin(Recursive)` 的这条 anchor 的 **root 对象本身** 已经到货了吗？"

这个问题的回答只依赖 root 的 `state` 字段，是一次本地 O(1) 查询，不需要走任何跨 bucket 扫描，也不需要后台 verifier。

#### 5.9.1 P0 状态定义

| 状态 | 含义 | 判定 |
|---|---|---|
| `Pending` | anchor 行已登记，但 root 对象本身仍是 `shadow` / 未到货 | anchor 插入瞬间 root 的 `state != 'present'` |
| `Materializing` | root 对象已 `present`；子树是否完全到齐在 P0 范围内**不做保证** | anchor 插入瞬间 root 已 `present`，或后续 `shadow -> present` 补展开路径命中本 anchor |

P0 **刻意不提供** `Complete` / `Broken`。上层 UI 在 P0 只能回答两件事：

- "root 有没有内容" → `Pending` vs `Materializing`；
- "整棵子树收敛了没有" → 只能通过全局 `await_cascade_idle()` 或 P1 的 verifier 回答。

把两者混为一谈是现版 §5.7 的陷阱之一，这里显式分开。

#### 5.9.2 状态推进的三个事件

所有状态推进都在本地事务内完成，不跨 bucket、不依赖异步 verifier：

1. **acquire / pin 落表**：`fs_acquire_local` / `pin_local` 在 upsert anchor 行之后读 root 的 `state`；若 `present` 写 `Materializing`，否则写 `Pending`。
2. **`shadow -> present` 补展开**：`put_object(obj_id)` 把一行从 shadow 改成 present 的同一事务里，对所有 `obj_id = obj_id` 的 `fs_anchors` 行和所有 `obj_id = obj_id AND scope = 'recursive'` 的 `pins` 行执行 `cascade_state = 'Materializing'`。这一步与 `reconcile_expand_state` 在同一事务里。
3. **release / unpin**：行被删，状态字段自然消失，不需要单独处理。

`Skeleton` / `Lease` 的行在插入时直接写 `Materializing`（它们对子树物化没有承诺，所以永远只看 root 自己）。

#### 5.9.3 P0 上层 API

```rust
pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str)
    -> NdnResult<CascadeStateP0>;

pub async fn fs_anchor_state(&self, obj_id: &ObjId,
                             inode_id: u64, field_tag: u32)
    -> NdnResult<CascadeStateP0>;

pub enum CascadeStateP0 {
    Pending,
    Materializing,
}
```

这两个接口的实现只是单行点查，没有后台扫描、没有 verifier 调用。

P1 可以在此之上加一个 `verify_anchor(obj_id)` 返回 `CascadeState::{Pending, Materializing, Complete, Broken}`，并引入 `verified_at` 列、periodic verifier、`Broken` 阈值等——**那一套与 P0 的 column 是前向兼容的**，P0 的 `Pending` / `Materializing` 取值不需要改名。

---

## 6. LRU 与访问时间

### 6.1 `last_access_time`

`objects` 和 `chunk_items` 都必须有自己的 `last_access_time`；不能依赖文件系统 `atime`。

### 6.2 in-memory 热表 + relatime flush

读路径只更新内存热表，后台批量刷回 DB：

```rust
struct LruHotTable {
    entries: DashMap<ObjId, (u64, bool)>,
    flush_threshold: Duration,
}

fn touch(obj_id):
    let now = unix_timestamp();
    let prev = hot.entries.get(obj_id).map(|e| e.0).unwrap_or(0);
    if now - prev < FLUSH_THRESHOLD {
        return;
    }
    hot.entries.insert(obj_id, (now, true));

fn flush():
    let dirty = collect_dirty_batch();
    db.batch_update_last_access_with_max(dirty);
```

### 6.3 LRU 只对 `owned_bytes > 0` 的 class 0 生效

```sql
SELECT obj_id, owned_bytes
FROM objects
WHERE eviction_class = 0
  AND state = 'present'
  AND owned_bytes > 0
ORDER BY last_access_time ASC
LIMIT ?;
```

`SameAs` / `LocalLink` / shadow 这类 `owned_bytes=0` 的行不会出现在空间压力 GC 候选里。是否清理它们，交给单独的低优先级 metadata sweep。

---

## 7. 水位与 GC

### 7.1 水位只看 `owned_bytes`

```text
used_bytes = SUM(owned_bytes WHERE state='present')
```

也可以按 class 分解：

```text
used_class0 = SUM(owned_bytes WHERE eviction_class=0 AND state='present')
used_class1 = SUM(owned_bytes WHERE eviction_class=1 AND state='present')
used_class2 = SUM(owned_bytes WHERE eviction_class=2 AND state='present')
```

`logical_size` 不进入水位判断。

### 7.2 `gc_round`

```rust
async fn gc_round(target_bytes):
    freed = 0
    loop:
        candidates = db.list_lru_candidates(class=0, state='present', owned_bytes>0)
        if candidates.is_empty(): break

        for (obj_id, owned_bytes) in candidates:
            if providers.any(|p| p.is_rooted(obj_id).unwrap_or(true)):
                continue

            txn.begin()
                if db.has_active_pin(obj_id)
                    || db.has_incoming(obj_id)
                    || db.fs_anchor_count(obj_id) > 0:
                    recompute_eviction_class(obj_id, txn)
                    txn.rollback_continue()
                    continue

                db.delete_row(obj_id)
            txn.commit()

            delete_blob_file(obj_id)
            freed += owned_bytes
            if freed >= target_bytes: return freed
```

说明：

- class 0 且 `owned_bytes > 0` 的 present 行，可以直接删 row，再删 blob；
- 不需要转 shadow，因为 class 0 的定义已经保证它不承担任何当前有效的 cascade 责任；
- 如果扫描到过期 class（物化列漂移），事务内 double-check 会把它修正回来。

### 7.3 metadata sweep（低优先级）

单独的后台任务可以清理：

- `eviction_class = 0` 且 `owned_bytes = 0` 的 `present` 行；
- `shadow` 且无 incoming / 无 anchor 的占位行。

这不参与空间压力回收，只是避免数据库长期堆积零字节垃圾。

### 7.4 强制 GC 红线

```rust
async fn forced_gc_until(target_bytes):
    freed = gc_round(target_bytes)
    if freed >= target_bytes:
        return Ok(freed)

    Err(OutOfSpace("no class-0 owned bytes left to evict"))
```

class 1 / 2 一律不动。

---

## 8. shadow、乱序到达与收敛

### 8.1 两类 shadow 来源

`shadow` 现在有两种合法来源：

1. **incoming 占位**：`apply_edge(add)` 时，child 还没到本地；
2. **direct root 占位**：`pin` / `fs_acquire` 命中了一个还没到货的 root。

两者都是同一种结构：

- 有一行 DB 记录；
- `owned_bytes = 0`；
- `state = 'shadow'`；
- class 可为 1 或 2；
- `children_expanded = 0`。

对象内容一旦到达，`shadow -> present`，然后统一走 `reconcile_expand_state()`。

### 8.2 典型场景：先 `fs_acquire(F)`，后 `F` 到货

1. `fs_acquire(F)`：插入 `F` 的 shadow row，`fs_anchor_count=1`，`eviction_class=2`，`children_expanded=0`；
2. 稍后 `put_object(F, content)`：`state` 变为 `present`；
3. `reconcile_expand_state(F)` 看到 `has_expand_root(F)=true`，于是展开到 `F` 的 children。

### 8.3 典型场景：先 `add(S <- D)`，后 `S` 到货

1. `apply_edge(add, referee=S, referrer=D)`：插入 shadow `S`，`incoming_refs(S,D)` 落库，`eviction_class=1`；
2. 稍后 `put_object(S)`：`shadow -> present`；
3. `reconcile_expand_state(S)` 看到 `has_incoming(S)=true`，于是展开到 `S` 的 children。

### 8.4 收敛观测

因为展开/撤回都是 outbox 驱动的，系统对外只能保证“**最终收敛**”。
因此建议至少提供：

- `await_cascade_idle()`：等待当前节点 / 全局 outbox 排空；
- `debug_dump_expand_state(obj_id)`：查看 `eviction_class`、`children_expanded`、`incoming_refs count`、`fs_anchor_count`。

---

## 9. 布局迁移与 `children_expanded`

迁移时，除了内容、`incoming_refs`、`pins`、`fs_anchors`、`last_access_time` 外，还要搬：

- `logical_size`；
- `owned_bytes`；
- `children_expanded`；
- `edge_outbox where referrer = this obj`。

原则保持不变：

- **GC 只动 home 对象**；
- **迁移只搬 visiting 对象**；
- `apply_edge` 只在 referee 的 home bucket 落盘，非 home 一律转发。

如果迁移实现希望更稳妥，也可以在目标 bucket merge 完后，对该对象再次跑一次 `recompute_eviction_class + reconcile_expand_state`，把任何旧 epoch 遗留漂移收敛掉。

---

## 10. 竞态、一致性与恢复

### 10.1 GC 与并发 anchor / incoming

GC 候选扫描和删除之间，可能并发出现：

- 新 pin；
- 新 `fs_acquire`；
- 新 incoming edge。

所以 `collect_one` 必须在事务里 double-check：

- `has_active_pin(obj_id)`
- `has_incoming(obj_id)`
- `fs_anchor_count(obj_id) > 0`

命中任一项就取消这次回收，并重算 class。

### 10.2 文件与 DB 的顺序

- 写入：先写 tmp / rename 成 final，再写 DB，最后 commit；
- 删除：先删 DB row，commit 后删 blob；
- 崩溃留下的孤儿文件，启动时扫描清理。

**永远不允许先删文件再改 DB。**

### 10.3 `incoming_refs` / `children_expanded` 重建

重建流程：

1. 清空 `incoming_refs`；
2. 把所有对象 / chunk 的 `children_expanded` 置 0；
3. 清空旧 `edge_outbox`；
4. 遍历所有 `pins where scope='recursive'` 的 root，与 `SELECT DISTINCT obj_id FROM fs_anchors` 的 root；
5. 对每个 root 调一次本地 `reconcile_expand_state(root)` 或直接 enqueue 第一层 add。

之后让 outbox sender 自然展开。因为 `Skeleton` 的效果已经包含在 `should_expand()` 里，重建时不需要单独剪枝 skeleton 子树。

### 10.4 `fs_anchor_count` 重建

```sql
UPDATE objects
SET fs_anchor_count = COALESCE((
    SELECT COUNT(*) FROM fs_anchors WHERE fs_anchors.obj_id = objects.obj_id
), 0);

UPDATE chunk_items
SET fs_anchor_count = COALESCE((
    SELECT COUNT(*) FROM fs_anchors WHERE fs_anchors.obj_id = chunk_items.chunk_id
), 0);
```

然后全表跑一遍 `recompute_eviction_class` 即可。

### 10.5 `owned_bytes` 恢复

`owned_bytes` 必须与 backend 的真实拥有关系一致：

- 普通 present blob：来自本地文件长度；
- `SameAs` / `LocalLink` / shadow：固定为 0；
- 启动期 orphan scan 之后，可按 backend 再校准一次。

---

## 11. 大 Chunk 与 `SameAs`

### 11.1 32MB 上限

写入 guard 保持：

```rust
const MAX_STANDARD_CHUNK_SIZE: u64 = 32 * 1024 * 1024;
if chunk_size > MAX_STANDARD_CHUNK_SIZE {
    return Err("chunk exceeds 32MB; use ChunkList + SameAs");
}
```

### 11.2 `add_chunk_by_same_as`

```rust
pub async fn add_chunk_by_same_as(
    &self,
    big_chunk_id: &ChunkId,
    chunk_list_id: &ObjId,
) -> NdnResult<()>;
```

行为：

1. 校验 `chunk_list_id` 已存在；
2. 校验 sub-chunks 完整；
3. 流式 hash 校验拼接内容的 ChunkId 等于 `big_chunk_id`；
4. 写入一行 `state='present'`、`logical_size=big_chunk_size`、`owned_bytes=0`、`ChunkStoreState::SameAs(chunk_list_id)`。

`parse_obj_refs(big_chunk)` 在 `SameAs` 分支返回 `[chunk_list_id]`，因此如果这个 big chunk 被 anchored，它的引用链仍会自然 cascade 到 `ChunkList` 与 sub-chunks。

---

## 12. 接口

### 12.1 NamedLocalStore

```rust
pub async fn pin(&self, obj_id: &ObjId, owner: &str,
                 scope: PinScope, ttl: Option<Duration>) -> NdnResult<()>;
pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()>;
pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize>;

pub async fn fs_acquire(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;
pub async fn fs_release(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;
pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize>;

pub async fn apply_edge(&self, msg: EdgeMsg) -> NdnResult<()>;

pub fn touch(&self, obj_id: &ObjId);

pub async fn run_background_gc(&self) -> NdnResult<GcReport>;
pub async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64>;

pub async fn add_chunk_by_same_as(&self,
                                  big_chunk_id: &ChunkId,
                                  chunk_list_id: &ObjId) -> NdnResult<()>;

pub async fn await_cascade_idle(&self) -> NdnResult<()>;
pub async fn debug_dump_expand_state(&self, obj_id: &ObjId) -> NdnResult<ExpandDebug>;

// Per-anchor completeness，P0 只回答 root 级（见 §5.9）
pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str)
    -> NdnResult<CascadeStateP0>;
pub async fn fs_anchor_state(&self, obj_id: &ObjId,
                             inode_id: u64, field_tag: u32)
    -> NdnResult<CascadeStateP0>;
```

### 12.2 store_db 内部新增

```rust
fn upsert_shadow_if_absent(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<()>;
fn has_recursive_pin(&self, obj_id: &ObjId) -> NdnResult<bool>;
fn has_skeleton_pin(&self, obj_id: &ObjId) -> NdnResult<bool>;
fn recompute_eviction_class(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<()>;
fn reconcile_expand_state(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<()>;

fn set_logical_and_owned_bytes(&self, obj_id: &ObjId,
                               logical: u64, owned: u64,
                               txn: &Transaction) -> NdnResult<()>;
fn batch_touch_last_access_with_max(&self, items: &[(ObjId, u64)]) -> NdnResult<()>;

// §5.9 最小 per-anchor completeness 的维护入口
fn set_pin_cascade_state(&self, obj_id: &ObjId, owner: &str,
                         state: CascadeStateP0,
                         txn: &Transaction) -> NdnResult<()>;
fn set_fs_anchor_cascade_state(&self, obj_id: &ObjId,
                               inode_id: u64, field_tag: u32,
                               state: CascadeStateP0,
                               txn: &Transaction) -> NdnResult<()>;
/// 把所有指向 obj_id 的 Recursive pin / fs_anchor 的 cascade_state
/// 从 Pending 推进到 Materializing；shadow -> present 事务里调用。
fn promote_anchor_cascade_state_on_present(
    &self, obj_id: &ObjId, txn: &Transaction,
) -> NdnResult<()>;
```

---

## 13. 不变量

1. `state ∈ {'present', 'shadow', 'incompleted'}`。
2. `state == 'shadow' ⇒ owned_bytes == 0`。
3. `state == 'present' ∧ owned_bytes > 0 ⇒ backend 上存在且仅存在一个本地拥有的 blob`。
4. `logical_size` 只表达逻辑内容大小；水位与 GC 永远只看 `owned_bytes`。
5. `incoming_refs(referee, referrer)` 主键唯一，幂等。
6. `fs_anchors(obj_id, inode_id, field_tag)` 主键唯一，幂等。
7. 引用图天然是 DAG。
8. `fs_anchor_count(X) == COUNT(*) FROM fs_anchors WHERE obj_id == X`。
9. `eviction_class` 始终等于 `pins ∪ incoming_refs ∪ fs_anchors` 的物化结果。
10. **强制 GC 永不淘汰 `eviction_class >= 1` 的对象。**
11. `children_expanded(X) == 1` 只可能发生在 `state='present'` 的对象上。
12. `children_expanded(X) == 1` 蕴含：最近一次对 X 跑 `reconcile_expand_state` 时，`should_expand(X)` 为真。
13. `children_expanded(X) == 0` 且 `should_expand(X)==true` 时，系统最终会把它收敛到 1；反之亦然。
14. 任何修改 `pins` / `fs_anchors` / `incoming_refs` / `state` 的事务，都必须在同一事务里重算 `eviction_class` 与 `children_expanded`。
15. `SameAs` / `LocalLink` / shadow 的 `owned_bytes == 0`。
16. GC 只回收 home 对象；迁移只搬 visiting 对象。
17. `apply_edge` 只在 referee 的 home bucket 落盘，非 home 一律转发。
18. `last_access_time` 通过 `MAX(old, new)` 刷回，单调不减。
19. 普通 `put` 的额外副作用仍然是 O(1)：不会因为 class 0 行而向外发送 outbox。
20. `Skeleton` 是硬屏障：只要 `has_skeleton_pin(X)` 为真，`should_expand(X)` 就为假。
21. P0 范围内，`cascade_state ∈ {'Pending', 'Materializing'}`；`Pending` 蕴含 `state(root) != 'present'`，`Materializing` 蕴含 `state(root) == 'present'`。状态转换只发生在 acquire/pin 落表事务与 `shadow -> present` 事务内，不依赖任何异步 verifier。
22. P0 不承诺 epoch 迁移 / tombstone 转发 / 重试窗口跨越 epoch 切换下的乱序消息过滤（见 §5.8）。任何打破 §5.8 假设的工程改动必须与 gen 模型的重新引入一起落地。

---

## 14. 分阶段落地与暂不实现

### 14.1 P0：本次落地范围

- cache-first `put`
- `pins` / `fs_anchors`（含最小 `cascade_state ∈ {Pending, Materializing}` 列，只做 root 级语义，见 §5.9）
- `incoming_refs` / `edge_outbox`
- `children_expanded`
- `logical_size` / `owned_bytes`
- `SameAs`
- LRU / 水位 / 强制 GC
- **前置约束**：P0 要求 §5.8 的假设成立——要么不做 epoch 迁移，要么 epoch 切换前强制 drain outbox。

### 14.2 P1：可选增强

- **跨 epoch 乱序过滤**：重新引入 `cascade_gen` / `incoming_cascade_state`（或等价方案），解锁在线 Maglev 迁移与 tombstone 转发；
- **完整子树 completeness**：在 `cascade_state` 上增加 `Complete` / `Broken` 取值，配合后台 `verify_anchor` / `verified_at` / periodic verifier，让上层能回答"整棵子树是否已物化"；
- 更细粒度的 per-root 收敛观测；
- metadata sweep 的策略化；
- 更激进的迁移与 compact 优化。

### 14.3 暂不进入本版的内容

- **`evicted` / `NEED_PULL` 显式“只删物理副本、不删逻辑身份”模型**。这会改写 base state machine，应单独作为 P2 设计；
- mark-sweep / tracing；
- 让普通 `put` 写 `incoming_refs`；
- 在 GC 时做跨 bucket 查询；
- 让 `fs_meta` 走 `RootProvider` 运行期回调；
- 让 `Skeleton` 退回“只拦未来、不自动恢复”的弱语义。
