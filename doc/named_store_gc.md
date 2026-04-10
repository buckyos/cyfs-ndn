# named_store GC 方案

## 0. 核心模型：缓存优先，Pin / fs_anchor 即持久化

`named_store` 在产品形态上**首先是一个内容缓存**，而不是一个权威数据库。
所有 `put_object` / `put_chunk` 默认进入"缓存层"，没有任何引用计数副作用，超过水位就按 LRU 淘汰。
**只有显式持久化承诺（用户 pin 或上层 fs_anchor）才会触发 cascade、写引用边、保护对象不被淘汰**。

为什么要这样：

- 真实使用中，绝大多数 put 是网络扩散途中的搬运，没人需要它们永久落地。一个百万文件目录通常当天就扩散完毕,"误淘汰"对扩散过程几乎无害（再请求一次即可），但"全量持久化"对家用环境的硬盘是致命的。
- 主要的工程难题是**家用环境磁盘不够时被迫触发的强制 GC**——而不是引用计数本身的正确性。整个 GC 设计要围绕"强制 GC 时该淘汰谁、绝对不能淘汰谁"展开。
- Pin 的语义在 beta1 经过实际打磨，定位是"用户对一棵子树的持久化意图"。三种 scope（见下表）给了用户在"完整保存 / 目录骨架 / 临时持有"之间的明确开关。
- 上层文件系统对存储层的引用走 **fs_anchor**，它是一种被命名的、批量的 Recursive pin——同一套 cascade 机制，但用一张专门的表 + 物化整数列把规模问题扛住。详细设计在 `doc/ndm_gc.md`。

记住整篇文档里反复出现的一句话：

> **`put` 是廉价的、可被淘汰的；`pin` / `fs_anchor` 是昂贵的、需要被守护的。**
>
> GC 的全部复杂度，都是为了让这句话在分片、迁移、强制回收的各种压力下仍然成立。

### 三档存活语义（Mental Model）

| 档位 | 来源 | 行为 | 何时被回收 |
|---|---|---|---|
| **0 · cache** | 普通 `put`，无任何 anchor | LRU 排队，无 incoming_refs，无 cascade | 任何水位下都可被回收（先于 1、2） |
| **1 · referenced** | 因为属于某个 pinned / anchored 子树而被 cascade 持有，自身没有被直接 pin / anchor | 有 `incoming_refs`，受 cascade 保护 | 父 root 释放、cascade 减完，回到 cache 状态再被淘汰 |
| **2 · anchored** | 直接被 `pin` 引用（任何 scope）**或** `fs_anchor_count > 0` | 有 `pins` 行或 fs_anchor 计数 | 只能被显式 unpin / fs_release，**强制 GC 红线之内绝不触碰** |

`Recursive` pin / `fs_anchor` 的根是 class 2，子树里的所有可解析后代是 class 1。`Skeleton` pin 只让目标自身进入 class 2，**并且阻止任何后续 cascade 穿过它**——children 留在 class 0。`Lease` pin 也只锁目标自身但**对 cascade 透明**，是 grace / session / 下载持有专用。

### 三种 PinScope

| Scope | 锁自己 | cascade 自己的 children | 拦截上游 cascade 穿过自己 | 典型用途 |
|---|---|---|---|---|
| `Recursive` | ✓ | ✓ | — | 用户"完整保存这棵子树" |
| `Skeleton` | ✓ | ✗ | **✓** | 用户"在一棵不可变 DAG 里反向选择，把这一支留个壳"（前向拦截器） |
| `Lease` | ✓ | ✗ | ✗ | grace / session / 下载等内部短期持有 |

`Skeleton` 是 beta1 旧 `Single` 的语义升格：它不再只是被动的"最小持有"，而是一条**负面断言**——"这一支我要留壳，任何后续的持久化承诺（pin Recursive / fs_anchor）走到我这里就停"。从用户角度看，pin Recursive 是"在一大堆里选几棵保存"，pin Skeleton 是"在一大堆里挑几棵不要"，两者都是在不可变 DirObject 这棵 DAG 上做**视图层选择**，永远不修改 DirObject 本身。

`Skeleton` 的拦截**只对未来 cascade 生效**，不追溯撤回已有的 incoming_refs（参见 §5.6）。`Lease` 留出一个 cascade 透明的窗口，让 grace pin / 下载 session 之类的短期持有不会意外把上游 cascade 截断在中间——否则等 lease 过期就再没机会补回来了。

---

## 1. 目标

1. **缓存优先**：`put` 默认 O(1)，不做 cascade，不写 `incoming_refs`，不影响其他对象。
2. **Pin 与 fs_anchor 是仅有的持久化路径**：`Recursive` pin / `fs_anchor` 通过 cascade 把整棵子树锁住；`Skeleton` pin 只锁自身且前向拦截 cascade（用户骨架视图）；`Lease` pin 只锁自身且对 cascade 透明（grace / session）。两条入口（用户 pin 与 fs_anchor）共用一套 cascade 机制。
3. **强制 GC 的红线明确**：水位告急时，绝对不能误删 class 1/2。空间真不够时返回 `ENOSPC`，不向已 pin / 已 anchor 的承诺让步。
4. **大 Chunk（>32MB）必须用 ChunkList + SameAs**，不允许直接落盘成单文件。
5. **乱序到达可恢复**：父子对象可以以任意顺序到达，cascade 配合"影子对象"任何顺序都能恢复正确状态。
6. **崩溃可恢复**：所有 GC 状态（LRU、pins、fs_anchors、incoming_refs、outbox）可由本地事实重建。
7. **兼容 Maglev 分片**：父对象 D 与其引用的子对象 S 几乎一定不在同一 bucket，甚至不同设备。GC 必须**每个 bucket 本地独立运行**，cascade 走异步 outbox。

---

## 2. 设计原理

### 2.1 内容寻址 ⇒ 引用图天然 DAG

所有 ObjId 都是内容的密码学哈希。`A` 引用 `B` 等价于 `B` 的 ObjId 写在 `A` 的字节里，意味着 `B_id` 在计算 `A_id` 之前就已存在。要构成环 `A → B → A` 需要哈希碰撞，在 SHA-256 强度下视同不可能。

直接推论：

- 自引用不可能；
- 任意引用图都是 DAG；
- **纯引用计数足以做 GC**，不需要 mark-sweep 来打破环；
- 如果未来引入任何"可变命名指针"，它**不能**作为内容寻址 DAG 的节点参与 refcount，必须以外部 root 的形式存在。

### 2.2 "存活"的四个理由

一个对象 X 应当存活，当且仅当下列任一条件成立：

1. **被直接 pin**（class 2）：`pins` 表里有 X 的行，且未过期。
2. **被 cascade 持有**（class 1）：`incoming_refs(X)` 非空——意味着某个被 pin 的祖先在自己 cascade 下声明持有 X。
3. **外部根**：某个 `RootProvider` 声明它持有 X（保留给未来文件系统层用，beta2 阶段实现可能为空 provider）。
4. **缓存命中且水位未告急**（class 0）：LRU 还没轮到它，且当前水位允许 cache 占用空间。

强制 GC 的判定优先级是 4 → 3 → 2 → 1，**红线** = 不能跨过 2 这一档。

### 2.3 关键设计要点

**Dir-as-tombstone（弱 pin 的核心特性）**

`Single` pin 一个 DirObject 时，cascade 不展开，但 Dir 自身被持久化。Dir 的内容里**就是**其 children 的 ObjId 列表——这相当于"这个目录我留着，但具体文件按需重新下载"。换句话说，**Dir 的字节本身就是一份"被淘汰子节点的清单"**，在用户视角里是"选择性保存了一个大目录的目录结构"。

不要把这个特性当成 hack 或 corner case——它是 `Single` 这一档存在的主要价值。当你向用户解释 weak pin 时，用"目录占位"或"目录骨架"这种说法，而不是"弱引用"的抽象词。

**强制 GC 的红线**

家用环境下硬盘不够触发的强制 GC 是最危险的场景。准则只有一条：

> **强制 GC 永远不动 class 1 和 class 2。**
>
> 空间真的腾不出来，宁可返回 `ENOSPC` 让上层处理，也不允许"为了腾空间撕掉用户已经 pin 的目录"。

class 0 缓存层吃完之后再没空间，意味着用户 pin 的总量已经超过设备容量，这是一个**配置/容量问题**，不是 GC 算法能（或者应该）解决的问题。

---

## 3. 分片环境下的关键结论

`NamedStoreMgr`（`store_mgr.rs`）通过 Maglev 一致性哈希把 ObjId 映射到某个 `NamedLocalStore` bucket，且 `StoreTarget.device_did` 允许 bucket 跨设备。直接后果：

1. **父子几乎一定不同家**。`put_object(D)` 落在 `bucket_of(D)`，而 D 解析出来的子引用 S 几乎一定哈希到另一个 bucket。
2. **不存在跨 bucket 事务**。pin 触发的 cascade add_ref 不能做成一个 sqlite transaction，跨设备更不可能。
3. **投递必须重试、会乱序、会重放**。cascade 路径上的所有操作必须幂等。
4. **GC 必须本地独立**。每个 bucket 只根据自己库里的事实决定要不要删一个对象，不在 GC 时做跨 bucket 查询。

这些结论驱动下面两个设计：

- `incoming_refs` **不是一个整数列**，而是一张以"边"为主键的幂等集合表。
- 跨 bucket 的 cascade 走 **本地 outbox + 异步投递**，与本地事务一起 commit。

并且——这是这一版与上一版最大的差别——**只有 pin cascade 路径才会写 outbox / incoming_refs**。普通 `put` 不会触发任何跨 bucket 流量。

---

## 4. 数据结构

### 4.1 对象/Chunk 表扩展

```sql
ALTER TABLE objects     ADD COLUMN state            TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE objects     ADD COLUMN last_access_time INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN eviction_class   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN fs_anchor_count  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN cascade_gen      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN home_epoch       INTEGER;

ALTER TABLE chunk_items ADD COLUMN state            TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE chunk_items ADD COLUMN last_access_time INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN eviction_class   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN fs_anchor_count  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN cascade_gen      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_items ADD COLUMN home_epoch       INTEGER;

CREATE INDEX objects_lru     ON objects     (eviction_class, last_access_time);
CREATE INDEX chunk_items_lru ON chunk_items (eviction_class, last_access_time);
```

`cascade_gen` 是这个对象**作为 cascade 源（referrer）的单调递增逻辑时钟**，用于解决跨 epoch / 迁移场景下 add/remove 消息乱序导致的"过期边复活"问题。详见 §4.7 与 §5.3。每次 pin / unpin / fs_acquire / fs_release 或 shadow→present 触发的 cascade 突发都会原子地 `cascade_gen += 1` 并把新值打进本次突发的所有 outbox 消息里。普通 `put` 不动这一列。

`state` 取值：

| state | 含义 | 可读 | 可被 `incoming_refs` 指向 |
|---|---|---|---|
| `present` | 内容已落盘 | 是 | 是 |
| `incompleted` | 写入中 | 否 | 否 |
| `shadow` | 仅占位，没有内容 | 否 | 是 |

`eviction_class` 取值（参见 §0）：

| 值 | 含义 | 谁能更改它 |
|---|---|---|
| `0` cache | 普通 put 落盘的默认状态 | put / unpin cascade / fs_release cascade |
| `1` referenced | 因为被某个 anchored 祖先 cascade 持有 | apply_edge（incoming_refs 增加时）/ release_edge |
| `2` anchored | `pins` 表中有自己的行 **或** `fs_anchor_count > 0` | pin / unpin / fs_acquire / fs_release |

**`eviction_class` 是 `pins` ∪ `fs_anchors` ∪ `incoming_refs` 三类记账状态的物化缓存**，目的是让 LRU 扫描走一条复合索引就能跳过 1/2，不必每次 join 验证。任何修改 `pins` / `fs_anchors` / `fs_anchor_count` / `incoming_refs` 的事务里必须同时更新这一列。物化规则：

```
if has_active_pin(X) or fs_anchor_count(X) > 0:    eviction_class = 2
elif has_incoming(X):                               eviction_class = 1
else:                                               eviction_class = 0
```

`last_access_time` 是 chunk LRU 修复的核心——见 §6。

### 4.2 `incoming_refs`：以边为主键的集合（仅服务 anchored 子树）

```sql
CREATE TABLE incoming_refs (
    referee        TEXT NOT NULL,    -- 被引用者（本 bucket 拥有，或是 shadow）
    referrer       TEXT NOT NULL,    -- 引用者 obj_id（来源于另一 bucket 的某个父亲）
    declared_epoch INTEGER NOT NULL, -- 声明这条边时的 layout epoch
    declared_gen   INTEGER NOT NULL, -- 这条边由哪一次 referrer cascade 突发写入（见 §4.7）
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (referee, referrer)
);
CREATE INDEX incoming_refs_by_referee ON incoming_refs(referee);
```

**重要变化（与上一版的差别）：这张表不再被普通 `put` 写入。** 它只在以下四种场景下被填充：

1. `pin(root, Recursive)` cascade 展开时，从 root 开始递归把所有 (child, parent) 边写进来；
2. `fs_acquire(root)` cascade 展开时（与 1 用同一份 cascade 机制，只是触发源是 fs_anchor 而非 pin）；
3. anchored 祖先链上有新对象加入时（落地一个原本是 shadow 的对象），把它解析出的子边补进来；
4. 离线工具的 `rebuild`。

普通 `put` 既不写 `incoming_refs` 也不写 `edge_outbox`——这就是为什么"缓存模型"能做到 O(1)。`Skeleton` pin 会**拦截**穿过自己的 cascade（见 §5.6），所以一个被 Skeleton-pin 的对象的 children 不会因为某个上游 Recursive pin / fs_anchor 而进入 `incoming_refs`。

### 4.3 `edge_outbox`：本地事务中写入的跨 bucket cascade 意图

```sql
CREATE TABLE edge_outbox (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    op           TEXT NOT NULL,           -- 'add' | 'remove'
    referee      TEXT NOT NULL,
    referrer     TEXT NOT NULL,
    referrer_gen INTEGER NOT NULL,        -- referrer.cascade_gen 在写入这条消息时刻的快照
    target_epoch INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_try_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX edge_outbox_ready ON edge_outbox(next_try_at);
```

只在 pin/unpin/fs_acquire/fs_release cascade 路径上被写入。崩溃后重启 sender 会继续投递。FIFO 保序**不**充分保证乱序安全——同一对 `(referee, referrer)` 的 add/remove 在跨 epoch / 迁移 / 重试时仍然可能乱到，所以必须靠 `referrer_gen` 在 target 端做单调过滤；详见 §4.7 与 §5.3。

> 历史教训：第一版没有 `referrer_gen` 时，假设"同一边的 add/remove 总是由同一源 bucket 顺序发出"。这个假设在迁移 + tombstone 转发 + 重试三件事叠加之后会被打破——旧 epoch 的 add 经 tombstone 转发到达新 home bucket 时，可能晚于新 epoch 已经发出的 remove，结果"复活"一条本该已经撤回的边。`referrer_gen` 是这条路径上唯一的可靠保障。

### 4.4 `pins` 表

```sql
CREATE TABLE pins (
    obj_id     TEXT NOT NULL,
    owner      TEXT NOT NULL,         -- 'user', 'user:xxx', 'session:xxx', 'lease:yyy'
    scope      TEXT NOT NULL,         -- 'recursive' | 'skeleton' | 'lease'
    created_at INTEGER NOT NULL,
    expires_at INTEGER,               -- NULL = 永久
    PRIMARY KEY (obj_id, owner)
);
CREATE INDEX pins_by_owner    ON pins(owner);
CREATE INDEX pins_by_expire   ON pins(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX pins_skeleton_by_obj ON pins(obj_id) WHERE scope = 'skeleton';
```

`scope` 取值见 §0 的三档表。简述：

| scope | 用途 | cascade 自己的 children | 拦截上游 cascade 穿过自己 |
|---|---|---|---|
| `recursive` | 用户"完整保存这棵子树" | ✓ | — |
| `skeleton` | 用户"留个壳，不要内容"（前向拦截器） | ✗ | **✓** |
| `lease` | grace / session / 下载等内部短期持有 | ✗ | ✗ |

设计要点：

- `(obj_id, owner)` 主键 ⇒ pin 天然幂等。
- 同一对象可被多个 owner 独立 pin/unpin，互不干扰。
- 同一 owner 对同一对象**只允许一种 scope**，pin 重复时 `INSERT OR REPLACE` 用新 scope 覆盖，并相应触发 cascade 增/减（recursive→skeleton 撤回 cascade、skeleton→recursive 展开 cascade、跟 lease 之间互转同理）。
- 会话断开 = `DELETE FROM pins WHERE owner='session:xxx'`，配合 unpin cascade。
- TTL 清扫 = `DELETE FROM pins WHERE expires_at < now`，配合 unpin cascade。
- **这张表不承载文件系统反向索引**——FS 层走 `fs_anchors`（§4.6），不写 `pins`。
- `pins_skeleton_by_obj` 部分索引专门给 cascade 路径上的 `has_skeleton(X)` 检查用——这条查询会被 `apply_edge` 在每条 cascade 续展前调用，必须 O(log N)。

### 4.5 RootProvider 接口（保留给非 FS 的少量、慢变 root）

```rust
pub trait RootProvider: Send + Sync {
    fn name(&self) -> &str;
    fn is_rooted(&self, obj_id: &ObjId) -> NdnResult<bool>;
    fn enumerate_roots(&self) -> NdnResult<Box<dyn Iterator<Item = ObjId> + '_>>;
}

impl NamedLocalStore {
    pub fn register_root_provider(&self, p: Arc<dyn RootProvider>);
}
```

GC 在淘汰前对每个候选问一次 `is_rooted`，被认领的不删。这条接口**不**给 `fs_meta` 用——fs_meta 走 §4.6 的 `fs_anchors` 物化方案，避免运行期回调和候选集膨胀（详见 `doc/ndm_gc.md` §3.2）。`RootProvider` 是为未来"少量、慢变"的 root 来源准备的，例如外部备份系统的快照声明。

### 4.6 `fs_anchors`：上层文件系统的命名 root

```sql
CREATE TABLE fs_anchors (
    obj_id     TEXT    NOT NULL,
    inode_id   INTEGER NOT NULL,
    field_tag  INTEGER NOT NULL,        -- content_obj / dir_obj / linked / finalized / ...
    created_at INTEGER NOT NULL,
    PRIMARY KEY (obj_id, inode_id, field_tag)
);
CREATE INDEX fs_anchors_by_obj ON fs_anchors(obj_id);
```

设计要点：

- **语义上等价于"`fs_meta` 这个 owner 的批量 Recursive pin"**：每行 `(obj_id, inode_id, field_tag)` 表达"上层 FS 的某个 inode 的某个字段引用了 obj_id"。`field_tag` 是字段的类型枚举（不是路径字符串），用于审计 + 幂等去重。
- **`fs_anchors` 表行 + `fs_anchor_count` 整数列是同一份事实的两种视图**：行集是权威源 + 幂等键；整数列是物化缓存供 `eviction_class` 快速判活。任何修改 `fs_anchors` 的事务里必须同时维护 `fs_anchor_count` 和 `eviction_class`。
- **这张表里不存 path、不存 inode 名字、不存任何文件系统结构**——只有一个对象 id 和它的"持有者标识三元组"。`fs_meta` 仍然在自己的 `idx_dentries_target_obj` 上做反向索引；`named_store` 这边只关心"是否还有持有者"。
- **cascade 用 `pins` 表的 Recursive 路径**：fs_acquire 触发的 cascade 与 `pin(_, Recursive)` 走完全相同的 edge_outbox / apply_edge / incoming_refs 机制，差别只在 root 标记表不同（pins 行 vs fs_anchors 行）。
- **rebuild 廉价**：扫一遍 `fs_meta.dentries` + `nodes.*_obj_id` 即可重建整张表 + 整列；和 `incoming_refs` rebuild 同样靠"遍历 root 表 + 重新 cascade"展开。
- 详细的 fs_acquire / fs_release 协议、与 `fs_meta` 写路径的对接细节、Staged Commit 下的状态机，全部在 `doc/ndm_gc.md`。本文档只描述 cascade 机制本身（§5.5）。

### 4.7 `incoming_cascade_state`：referrer 维度的单调时钟

```sql
CREATE TABLE incoming_cascade_state (
    referrer       TEXT    NOT NULL PRIMARY KEY,
    last_seen_gen  INTEGER NOT NULL,
    last_seen_at   INTEGER NOT NULL
);
```

每个 bucket 在自己的本地 db 里维护这张表。**它是 apply_edge 的乱序消息过滤器**：

- `last_seen_gen[R]` = 本 bucket 已经从 referrer R 处接收并应用的最高 cascade 突发 gen。
- `apply_edge('add', referee=X, referrer=R, gen=g)` 与 `apply_edge('remove', ...)` 在落库前都要先比较 `g` 与 `last_seen_gen[R]`：
  - 若 `g < last_seen_gen[R]` → **直接丢弃**，不动 `incoming_refs`、不动 `eviction_class`、不再下钻。
  - 若 `g >= last_seen_gen[R]` → 按 §5.3 的逻辑落库；同事务里 `last_seen_gen[R] = max(last_seen_gen[R], g)`，`last_seen_at = now`。
- 老化：后台维护任务定期 `DELETE FROM incoming_cascade_state WHERE last_seen_at < now - DROP_GRACE`。`DROP_GRACE` 必须长于 outbox 重试 + 跨节点离线的最坏窗口（建议 7 天起步）。被老化掉的 referrer 之后再来消息时按"全新 referrer"处理——这是安全的：能在 7 天后还在传递的旧消息几乎不存在；万一存在也只会在历史 referrer 被删除之后影响一条不再相关的边。

为什么用一张独立小表而不是 `objects.cascade_gen` 这一列？

- `objects.cascade_gen` 是**referrer 自己的家 bucket** 上的"我已经发出过哪一代"——是发送侧的源时钟。
- `incoming_cascade_state.last_seen_gen` 是**referee 的家 bucket** 上的"我从某个 referrer 已经接收到的最高代"——是接收侧的水位线。
- 大多数情况下二者不在同一个 bucket，必须分开存。
- 行数上界 = 该 bucket 在生命周期内见过的 unique referrer 数，对所有合理工作负载都远小于 `incoming_refs` 的规模。

> 这张表的内容是**纯粹的去重水位线**，不参与 GC 决策、不影响 LRU、不进 eviction_class 计算。它的唯一作用是给 `apply_edge` 提供"这条消息是不是来自一个已经过气的 cascade 突发"的判断。

---

## 5. Pin / fs_anchor / Cascade 协议

这是 cascade 的两条入口（pin 与 fs_acquire）和它们共用的 cascade 机制。

### 5.1 `pin(obj_id, owner, scope, ttl)`

```rust
fn pin(obj_id, owner, scope, ttl):
    home = mgr.layout.select(obj_id)
    home.pin_local(obj_id, owner, scope, ttl)

fn pin_local(obj_id, owner, scope, ttl):    // 在 home bucket 的本地事务里
    txn.begin()
        // 1. pins 行
        old = db.lookup_pin(obj_id, owner)
        db.upsert_pin(obj_id, owner, scope, expires_at)

        // 2. eviction_class 升级到 2
        db.set_eviction_class(obj_id, 2)

        // 3. cascade（仅 Recursive；Skeleton/Lease 到此结束）
        if scope == Recursive:
            if old.is_none() or old.scope != Recursive:
                // 原子地为这次 cascade 突发分配新 gen
                gen = db.bump_cascade_gen(obj_id)
                refs = parse_obj_refs_or_empty(obj_id)   // 若是 shadow，refs 为空
                for child in refs:
                    enqueue_outbox('add', referee=child, referrer=obj_id,
                                   referrer_gen=gen, epoch)
                // 注意：即便 refs 为空（shadow 根），gen 仍然消耗了。
                // 这是必要的，否则后续 §5.4 的 catch-up cascade 会与
                // 当前 pin 的 unpin/重 pin 序列乱序——见 §5.4 注释。
        // 4. Skeleton 升档**不**追溯 tear-down 已经穿过 obj_id 的 cascade。
        //    Skeleton 是前向拦截器；语义和理由见 §5.6。
    txn.commit()
```

关键点：

- pin 自身永远只动 home bucket 的本地表。
- cascade 通过 outbox 异步散到其他 bucket。
- pin 一个 shadow 对象**不会立即 cascade**——shadow 的 children 现在不可知；等内容真正落地（见 §5.4）时再补 cascade，**复用本次 pin 已经分配的 cascade_gen**。
- `Skeleton` 与 `Lease` 永远不写 outbox，是真正的 O(1)。
- `bump_cascade_gen(obj_id)` 在事务内对 `objects.cascade_gen` 做 `cascade_gen + 1` 并 RETURNING 新值——这是单调的源时钟，跨进程重启后继续向上。

### 5.2 `unpin(obj_id, owner)`

```rust
fn unpin_local(obj_id, owner):
    txn.begin()
        old = db.lookup_pin(obj_id, owner)
        if old.is_none(): return
        db.delete_pin(obj_id, owner)

        // 还有别的 owner 在 pin 自己吗？没有就降级
        // 注意 fs_anchor_count 也要参与判定
        if not db.has_active_pin(obj_id) and db.fs_anchor_count(obj_id) == 0:
            db.set_eviction_class(obj_id,
                if db.has_incoming(obj_id) { 1 } else { 0 })

        // 撤回 cascade（仅 Recursive 需要；Skeleton/Lease 当初没 cascade）
        if old.scope == Recursive
                and not db.has_active_recursive_pin(obj_id)
                and db.fs_anchor_count(obj_id) == 0:
            // 没有别的 cascade 源还指向这棵子树时才发 remove 突发
            gen = db.bump_cascade_gen(obj_id)
            refs = parse_obj_refs_or_empty(obj_id)
            for child in refs:
                enqueue_outbox('remove', referee=child, referrer=obj_id,
                               referrer_gen=gen, epoch)
        // 移除 Skeleton 不会自动重新展开任何 cascade。
        // 想恢复 cascade 必须由上层 ndm 提供 refresh_anchor 显式触发，见 §5.6。
    txn.commit()
```

`cascade_gen` 在 unpin 时**也**要 bump（即便接下来又被立刻重新 pin），因为 target bucket 上的 `incoming_cascade_state[obj_id].last_seen_gen` 必须看到一个比上一次 add 突发严格更大的 gen，才会接受这次 remove。同理，**unpin 后立刻 re-pin** 在 cascade_gen 上是两次独立的 +1，target 端按单调序处理。

### 5.3 outbox 投递与 `apply_edge`

```rust
fn deliver(entry):
    target_bucket = mgr.layout_at(entry.target_epoch).select(entry.referee)
    if target_bucket.is_local():
        target_bucket.apply_edge(msg)
    else:
        rpc(target_bucket.device_did, msg)
    db.delete_outbox(entry.seq)

fn apply_edge(msg):    // 在 referee 的 home bucket 本地事务里
    txn.begin()
        // ----- Gen 过滤：referee 端的水位线 -----
        last = db.get_cascade_state(msg.referrer)         // None 视作 last_seen_gen=0
        if msg.referrer_gen < last.last_seen_gen:
            // 这是一条已经被 referrer 后续突发覆盖掉的过期消息，整体丢弃。
            // 不写 incoming_refs、不动 eviction_class、不再下钻、不发新 outbox。
            txn.commit()
            return
        // 即便 msg.referrer_gen == last_seen_gen 也要继续：可能是同一突发里
        // 不同 referee 的并发消息，互不影响。
        db.upsert_cascade_state(msg.referrer,
                                last_seen_gen = max(last.last_seen_gen, msg.referrer_gen),
                                last_seen_at  = now())

        if msg.op == 'add':
            db.upsert_shadow_if_absent(msg.referee)
            db.execute("INSERT OR REPLACE INTO incoming_refs
                        (referee, referrer, declared_epoch, declared_gen, created_at)
                        VALUES (?,?,?,?,?)", ...)
            // 用 INSERT OR REPLACE：当同一对 (referee, referrer) 的 declared_gen
            // 落后于本次消息时刷新成新 gen；这条边自身的 gen 也是单调的。

            // 升 class：原 0 → 1；原 2 不动
            db.execute("
                UPDATE objects SET eviction_class = MAX(eviction_class, 1)
                WHERE obj_id = ?
            ", msg.referee)

            // referee 自己又是个有 children 的对象 → 进一步 cascade（pinned 子树需要）
            // 关键：被 Skeleton pin 的节点是前向拦截器，cascade 到此为止
            if db.is_present(msg.referee) and not db.has_skeleton_pin(msg.referee):
                // 下钻使用 referee 自己的 cascade_gen 突发（独立于 msg.referrer_gen）
                child_gen = db.bump_cascade_gen(msg.referee)
                child_refs = parse_obj_refs(msg.referee)
                for c in child_refs:
                    enqueue_outbox('add', referee=c, referrer=msg.referee,
                                   referrer_gen=child_gen, epoch)
        else /* remove */:
            // 只删 declared_gen <= msg.referrer_gen 的边——避免误删一条更新的同名边
            // (例如：本 bucket 上 incoming_refs 行的 declared_gen=10，
            //  这条 remove 消息 gen=8 → 不能删，应当忽略)
            db.execute("DELETE FROM incoming_refs
                        WHERE referee = ? AND referrer = ? AND declared_gen <= ?",
                        msg.referee, msg.referrer, msg.referrer_gen)
            // 入边降到 0、没 pin、也没 fs_anchor → eviction_class 降到 0
            if not db.has_incoming(msg.referee)
                    and not db.has_active_pin(msg.referee)
                    and db.fs_anchor_count(msg.referee) == 0:
                db.set_eviction_class(msg.referee, 0)
            // 同样要把这条对象的子 cascade 撤回。注意对称性：
            // 当初 add 因 Skeleton 没有下钻，现在 remove 同样不能下钻——
            // 否则会撤回别人留下的合法 incoming_refs。
            if db.is_present(msg.referee) and not db.has_skeleton_pin(msg.referee):
                child_gen = db.bump_cascade_gen(msg.referee)
                child_refs = parse_obj_refs(msg.referee)
                for c in child_refs:
                    enqueue_outbox('remove', referee=c, referrer=msg.referee,
                                   referrer_gen=child_gen, epoch)
    txn.commit()
```

注意 cascade 是**在 apply_edge 内**继续向下扩散的——这避免了源 bucket 需要预先知道整棵树的形状。`has_skeleton_pin` 在 home bucket 本地表上是 O(1) 的索引点查（见 §4.4 的 `pins_skeleton_by_obj` 部分索引）。

**Gen 模型的关键不变量**：

1. 同一 referrer 的所有 cascade 突发在它的家 bucket 上由 `cascade_gen` 单调递增。
2. 同一 referrer 的所有 outbox 消息携带它在被写入时刻的 gen 快照。
3. target bucket 用 `incoming_cascade_state.last_seen_gen` 拒绝任何 gen 严格更小的消息，并且对 `incoming_refs` 的 add/remove 都做 `declared_gen <= msg.gen` 的二级守护。
4. 跨 epoch 迁移时，`cascade_gen` 列与 `incoming_cascade_state` 表都被复制到新 home bucket（见 §9.3），单调性跨 epoch 保持。

**为什么需要 `incoming_refs.declared_gen` 这一列**——光靠 `incoming_cascade_state` 不够：`incoming_cascade_state` 是 referrer 维度的水位线，但同一 referrer 在不同 referee 上的边可能在不同时间被覆盖。如果一条 add(C1, R, gen=5) 早就到了，而 last_seen_gen[R] 已经因为另一条 add(C2, R, gen=10) 升到了 10，这时一条 remove(C1, R, gen=7) 到达——它的 gen=7 < last_seen_gen[R]=10，水位线会拒绝它。但 declared_gen 守护让我们在水位线放行时也只删"<= 自己 gen"的边，杜绝了"被晚到的 add 短暂复活的边在更晚到的 remove 之前被错误地保留"这种次生问题。两道防线对正确性都是必需的。

> 这条 gen 模型替换了上一版隐含假设的 "FIFO 保序就够了"。FIFO 在 epoch 切换 + tombstone 转发 + 重试三件事叠加之后会失效，gen 模型是它的正交补丁。

### 5.4 影子对象的延迟 cascade

`apply_edge('add')` 命中 shadow（`is_present == false`）时只能写边、升级 class，没法再继续 cascade（不知道 children 是谁）。同样地，pin 一个 shadow 根（§5.1）也只能写 pin 行、留下空 cascade。等到内容真正落地：

```rust
fn put_object(obj_id, content, epoch):
    txn.begin()
        row = db.lookup(obj_id)
        match row:
            None:
                db.insert(obj_id, state='present', content, eviction_class=0)
            Some(r) if r.state == 'shadow':
                db.update(obj_id, state='present', content)
                // 关键：自己被某个 anchor 持有时，必须把当初没法发出的 cascade 补上。
                // anchor 来源有三种：
                //   (a) 直接被 Recursive pin (root case — 没有 incoming_refs)
                //   (b) 直接被 fs_anchor 持有 (root case — 同样没有 incoming_refs)
                //   (c) 被某个上游 cascade 拉成 shadow (作为中间节点 — 有 incoming_refs)
                // 任何一种成立都要补 cascade；Skeleton 拦截器永远优先。
                let needs_cascade =
                    db.has_active_recursive_pin(obj_id)    // (a)
                    or db.fs_anchor_count(obj_id) > 0      // (b)
                    or db.has_incoming(obj_id);            // (c)
                if needs_cascade and not db.has_skeleton_pin(obj_id):
                    // 触发"补 cascade"突发：分配新 gen
                    let gen = db.bump_cascade_gen(obj_id);
                    let child_refs = parse_obj_refs(content);
                    for c in child_refs:
                        enqueue_outbox('add', referee=c, referrer=obj_id,
                                       referrer_gen=gen, epoch);
            Some(r) if r.state == 'present':
                db.touch_lru(obj_id)
                return
    txn.commit()
```

> **Bug 修复历史**：上一版只检查了 `has_incoming(obj_id)`。这对中间节点（case c）正确，但对**直接被 Recursive pin 或 fs_anchor 持有的根**（case a/b）是漏的——根节点通常没有 incoming_refs，只有自己的 pin/anchor 行。结果是"用户 pin 了一个还在下载中的根，根下载完之后子树永远不展开"。新增的 `has_active_recursive_pin` / `fs_anchor_count` 两条检查闭合了这个洞。

普通 `put`（既不是 shadow，也没有任何 anchor 持有它）走入第一个分支，**不写任何 outbox**——这就是缓存模型的 O(1) 来源。

### 5.5 `fs_acquire` / `fs_release`

`fs_acquire(obj_id, inode_id, field_tag)` 是 ndm 调用 named_store 的 FS 层接口，**语义等价于"以 FS 为 owner 的批量 Recursive pin"**。它走和 `pin(_, Recursive)` 完全一样的 cascade 通路，区别只在于：

- 落表落到 `fs_anchors`（§4.6），不写 `pins`；
- 触发 `fs_anchor_count` 物化列 +1，让强制 GC 候选过滤可以单 SQL 完成（见 §7）；
- owner 身份是 (`inode_id`, `field_tag`) 而不是 `OwnerId`，让上层 ndm 可以在 inode 析构、`replace_target`、`delete_dentry` 时精确反引用。

```rust
fn fs_acquire(obj_id, inode_id, field_tag):
    home = mgr.layout.select(obj_id)
    home.fs_acquire_local(obj_id, inode_id, field_tag)

fn fs_acquire_local(obj_id, inode_id, field_tag):
    txn.begin()
        // 1. anchor 行（同一 (obj, inode, field) 幂等）
        inserted = db.execute("
            INSERT OR IGNORE INTO fs_anchors(obj_id, inode_id, field_tag, created_at)
            VALUES(?,?,?,?)", ...)
        if inserted == 0: txn.commit(); return    // 重复 anchor，无副作用

        // 2. 物化列 +1，eviction_class 升到 2
        was_zero = (db.fs_anchor_count(obj_id) == 0)   // 读旧值
        db.execute("UPDATE objects
                    SET fs_anchor_count = fs_anchor_count + 1,
                        eviction_class  = 2
                    WHERE obj_id = ?", obj_id)
        // chunk_items 同理（如果 obj_id 是 chunk）

        // 3. cascade（行为与 pin Recursive 一致）
        //    只有从 0 升到 1 的那一次需要触发突发——这是该 obj 第一次成为 cascade 源。
        //    后续 anchor 加在同一棵根上时 cascade 已经在了，不再重发；
        //    target 端无论如何都靠 gen 模型去重，所以即便重复也只是浪费 outbox 流量。
        if was_zero:
            gen = db.bump_cascade_gen(obj_id)
            refs = parse_obj_refs_or_empty(obj_id)
            for child in refs:
                enqueue_outbox('add', referee=child, referrer=obj_id,
                               referrer_gen=gen, epoch)
    txn.commit()

fn fs_release_local(obj_id, inode_id, field_tag):
    txn.begin()
        deleted = db.execute("DELETE FROM fs_anchors
                              WHERE obj_id=? AND inode_id=? AND field_tag=?", ...)
        if deleted == 0: txn.commit(); return    // 幂等：重复 release 不报错

        // 物化列 -1
        new_cnt = db.execute("UPDATE objects
                              SET fs_anchor_count = fs_anchor_count - 1
                              WHERE obj_id = ?
                              RETURNING fs_anchor_count", obj_id)

        // 还有别的 anchor / pin 吗？没有就降级
        if new_cnt == 0 and not db.has_active_pin(obj_id):
            db.set_eviction_class(obj_id,
                if db.has_incoming(obj_id) { 1 } else { 0 })

        // 撤回 cascade（仅当真的没人 anchor 这棵根了）
        // 同 unpin：还要排除 active recursive pin 这个并行 cascade 源
        if new_cnt == 0 and not db.has_active_recursive_pin(obj_id):
            gen = db.bump_cascade_gen(obj_id)
            refs = parse_obj_refs_or_empty(obj_id)
            for child in refs:
                enqueue_outbox('remove', referee=child, referrer=obj_id,
                               referrer_gen=gen, epoch)
    txn.commit()
```

要点：

- `fs_acquire` / `fs_release` 不感知 path、不感知 dentry，只是一个 (obj, inode, field) 三元组的引用计数 + cascade 触发器。所有路径/重命名/移动语义都在 ndm 层完成。
- `fs_release` 触发的 `'remove'` cascade 只在 `fs_anchor_count` 真正归零时发出，避免反复 attach/detach 同一份内容产生 outbox 风暴。
- **Skeleton 拦截对 fs_acquire 的 cascade 同样生效**——`apply_edge` 在每一层都会检查 `has_skeleton_pin`，所以用户预先 Skeleton 一个目录，再 `fs_acquire` 它的祖先，那一支的子内容不会被强制下载/锁住。这正是 §0 的设计意图："手工墓碑覆盖默认的 strict cascade"。
- 详细的上层调用规则（六个 fs_meta 调用点、状态机、回滚）见 `doc/ndm_gc.md`。

### 5.6 Skeleton 的前向语义（正式约定）

Skeleton 是用户在不可变 DirObject DAG 上做"反向选择"的工具。它和 Recursive pin / fs_anchor cascade 之间存在三种交互场景，下面给出明确的承诺：

**Case A — 先 Skeleton，后 cascade**

时间线：
1. `pin(D, _, Skeleton)`
2. `pin(P, _, Recursive)`，其中 P 是 D 的祖先（或 `fs_acquire(P)`）

承诺：cascade 走到 D 时通过 `apply_edge` 的 `has_skeleton_pin` 检查停下，D 自身被记入 incoming_refs（class 2 因为 Skeleton 自带 anchor 资格），但 D 的 children 完全不被 cascade 覆盖。这是 Skeleton 的首要语义。

**Case B — 先 cascade，后 Skeleton**

时间线：
1. `pin(P, _, Recursive)`：cascade 已经把 D 及 D 的所有可解析后代都升到 class 1
2. `pin(D, _, Skeleton)` 后到

承诺：**不追溯 tear-down**。已经存在于 D 子树里的 incoming_refs 不动；Skeleton 升档只把 D 自身从 class 1 升到 class 2，并把 D 注册成"未来的拦截器"。已经下来的内容继续受 cascade 保护，直到 P 被 unpin 才会随 `apply_edge('remove')` 自然清退（那次 remove 同样会因为 Skeleton 在 D 这一层停下，正是 §5.3 的对称要求所保证的）。

为什么不追溯：

- 追溯需要 walk 整棵 D 的子树并对每条 incoming_refs 边发 `remove` outbox，复杂度 O(子树边数)，对家用 NAS 来说在最坏情况下就是把一个百万文件目录的引用边全部翻译成跨 bucket 流量。
- Skeleton 的产品语义是"我想留壳"，对"已经下来的东西要不要立刻删"用户其实不在意——他可以再发一条 `fs_release` 或者等强制 GC 自然处理（class 1 在水位告急时是可以被淘汰候选的，只要不破坏 class 2 的承诺）。
- 追溯逻辑会让 Skeleton 升档变成一次潜在的全量重写，违背"pin 永远只动 home bucket 本地表"的轻量假设。

**Case C — 先 cascade、再 Skeleton、再 unpin Skeleton**

承诺：unpin Skeleton 不会自动重新展开 cascade。如果上层（ndm）希望恢复"P 的 Recursive cascade 应该重新覆盖 D 子树"的语义，必须显式重新 `fs_acquire(D)` 或者 ndm 提供 `refresh_anchor(P)` 重做一次 cascade（实现上等价于 `fs_release(P) + fs_acquire(P)`，但靠 outbox 去重做到不发多余流量）。

为什么不自动重展开：

- 自动重展开等于在 unpin 路径上塞一次 walk，违反 unpin 应该 O(1) 的承诺。
- 用户撤掉 Skeleton 通常意味着"我想换个选择"，而不是"立刻把那一支补全"。延迟到下一次 `fs_acquire` 重发 cascade 反而是更自然的语义。
- Skeleton 的整张语义都是"前向"的——前向拦截、前向恢复——这条规则避免了任何"过去发生过什么"的状态查询。

**实现侧的小观察**：上述三个 case 都不需要 named_store 维护任何额外的"曾经被 cascade 过吗"的历史信息。所有判定都靠当前事务里能立刻看到的 `pins` / `incoming_refs` / `fs_anchor_count`，这正是 cascade 模型可以单 home bucket 局部决策的根本原因。

### 5.7 Pin / fs_anchor 的完成度状态（completeness state）

到这里为止 §5.1–§5.6 描述的全部都是"我对存储层做出的承诺"。一条 `pin(_, Recursive)` 或 `fs_acquire(_)` **登记**成功只意味着 anchor 行已经写下、cascade 突发已经入了 outbox——它**不**意味着整棵 DAG 已经在本地物化完毕。在 publisher 离线 / 网络不通 / chunk 还在路上等真实场景下，cascade 会停在某些 shadow 节点上，子树长期处在"骨架已建、内容未到"的状态。

如果不把这件事显式建模，上层应用（ndm 自己、用户 UI、备份系统）会把"我已经 pin 了"误读成"我已经离线持有了完整内容"。这是一类静默正确性 bug，必须通过 API 层把"承诺"和"已实现"分开。借鉴 IPFS 的 pin 语义和远端 pin 状态机，本文档在 anchor 层引入四个状态：

| 状态 | 语义 | 何时达到 | 何时离开 |
|---|---|---|---|
| `Pending` | anchor 行已写、根对象本身还是 shadow，cascade 未发出 | pin/fs_acquire 落表的瞬间，若根 obj 此刻 state=='shadow' | 根 obj 转 present 的 §5.4 catch-up 突发完成入队 |
| `Materializing` | cascade 已经在路上：根 present，但子树里至少一个节点还是 shadow，**或** 还有未投递的 outbox 消息归属于本 anchor 的 cascade | pin/fs_acquire 时根已 present；或 Pending → 根落地 | 子树所有 reachable 节点都 present 且没有 in-flight outbox |
| `Complete` | 子树（受 Skeleton 拦截器自然剪枝）所有节点都 present，无 in-flight cascade | 由 verifier / 内部完成度跟踪器在所有候选都满足时打上 | 任何 shadow 重新出现（极罕见——通常是 backend 文件被外部删除后启动期 fix） |
| `Broken` | 至少一个子树节点已经在 shadow 状态卡住超过阈值（可配置，默认 7 天） | verifier 扫到陈旧 shadow | 该 shadow 落地，或上层主动解除该 anchor |

`Complete` 是上层"我可以离线工作"的唯一可信判定；`Broken` 是上层"我应该报警 / 重新拉取 publisher / 提示用户"的信号。

#### 5.7.1 状态如何存

`pins` 与 `fs_anchors` 表各加两列：

```sql
ALTER TABLE pins        ADD COLUMN cascade_state TEXT    NOT NULL DEFAULT 'Pending';
ALTER TABLE pins        ADD COLUMN verified_at   INTEGER;
ALTER TABLE fs_anchors  ADD COLUMN cascade_state TEXT    NOT NULL DEFAULT 'Pending';
ALTER TABLE fs_anchors  ADD COLUMN verified_at   INTEGER;
```

`cascade_state` 是上层可读、由 verifier 写。`verified_at` 是上次扫描完成的时间戳，让上层判断信息新鲜度。

> 注意：一行 `pins`/`fs_anchors` 永远只对应一个 anchor 主体（pin row 或三元组），不跨 owner 聚合。同一 obj 上的多条 anchor 各自独立维护状态。

#### 5.7.2 状态推进的事件源

四个事件影响状态，全都本地可感知：

1. **anchor 落表**（pin / fs_acquire 路径）：
   - 根 `state == 'present'` → 写 `Materializing`
   - 根 `state == 'shadow'` → 写 `Pending`
2. **shadow → present catch-up**（§5.4）：本 bucket 触发 cascade 突发后，对它的所有"直接挂着的 anchor 行"做 `Pending → Materializing`。
3. **完成度证据汇总**（verifier 路径，见 §5.7.3）：把 `Materializing → Complete` 或 `→ Broken`。
4. **anchor 解除**（unpin / fs_release）：anchor 行被删，状态字段连带被删。无需额外处理。

注意 (1)(2)(4) 都是 anchor 自己的家 bucket 上的本地事务里能完成的事——不需要跨 bucket。**只有 (3) 需要走全树扫描**，所以做成异步 verifier。

#### 5.7.3 `verify_anchor(obj_id)` —— 权威完成度检查

```rust
async fn verify_anchor(obj_id: &ObjId) -> NdnResult<CascadeState>
```

实现是一次以 obj_id 为根的 BFS，受 Skeleton 拦截器自然剪枝：

```text
seen   = {}
queue  = [obj_id]
worst  = Complete         // 起步乐观
oldest_shadow_age = None
while let Some(x) = queue.pop_front():
    if x in seen: continue; seen.insert(x)
    row = lookup_anywhere(x)        // 通过 mgr 跨 bucket 路由
    match row.state:
        present:
            if has_skeleton_pin(x):
                continue            // 拦截器：x 自己 ok，不再下钻
            for c in parse_obj_refs(x.content):
                queue.push_back(c)
        shadow:
            worst = max(worst, Materializing)
            age = now - row.created_at
            oldest_shadow_age = max(oldest_shadow_age, age)
        evicted:
            // 已被 erase_obj_by_id 显式腾出物理副本，逻辑身份在
            // 视为 Materializing：下次 open 会触发 NEED_PULL 拉回
            worst = max(worst, Materializing)
        incompleted | none:
            worst = max(worst, Materializing)

// 阈值判定
if worst == Materializing and oldest_shadow_age > BROKEN_THRESHOLD:
    worst = Broken

// 把结果写回 anchor 行
db.update_anchor_state(obj_id, owner_or_triple, worst, verified_at=now)
return worst
```

要点：

- **跨 bucket 路由**：verifier 只走读路径，不写边、不发 outbox；走 mgr 提供的 `lookup_anywhere`（按 Maglev 散到对应 bucket 读一行）。
- **代价**：与子树规模成正比。是一次显式承担的、可控的代价——上层选择何时调用 verify。
- **Skeleton 拦截器在 verifier 路径上同样剪枝**：被 Skeleton 标的节点视为完成度叶子（"我承诺到这里为止"），其 children 不参与判定。这与 §5.3 的 cascade 拦截语义对齐——Skeleton 节点本身存在即满足，不要求其下方的内容存在。
- **`evicted` 与 `Materializing`**：`erase_obj_by_id` 的 evicted 状态在完成度模型里被视为"未完成"。这是有意的：它真实地反映了"物理副本不在本地"的事实，让上层 UI 不会误以为离线可用。

#### 5.7.4 Verifier 的运行模式

三种触发模式可叠加，按用户预算配置：

| 模式 | 触发 | 用途 |
|---|---|---|
| **on-demand** | 上层显式调 `verify_anchor(obj_id)` | 回答"现在能离线读吗" |
| **opportunistic** | 每次 §5.4 的 catch-up cascade 完成时（即 outbox 该突发的最后一条 ack 回来），mgr 把对应 anchor 重新跑一次 verify | "下载完成"事件的自动 promote |
| **periodic** | 后台 worker 定期扫描 `cascade_state IN ('Pending','Materializing')` 的 anchor，分批 verify；同时扫描 `cascade_state == 'Complete' AND verified_at < now - REVERIFY_INTERVAL` 的做兜底回查 | 检测 Broken / 检测 cache 自然抖动 |

`opportunistic` 模式需要 mgr 知道"哪条 cascade 是属于哪个 anchor 的"。简单实现：边 enqueue 时给 outbox 行加一个 `originating_anchor` 字段（可选），sender 在最后一条 ack 时回调 mgr 触发 verify。这条优化是 nice-to-have，不实现也行——`periodic` 模式足够兜底。

#### 5.7.5 上层 API

```rust
impl NamedLocalStoreMgr {
    /// 立即返回当前已存的 cascade_state（不触发新一轮 verify）
    pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str)
        -> NdnResult<(CascadeState, Option<u64> /*verified_at*/)>;

    /// 立即返回 fs_anchor 的状态
    pub async fn fs_anchor_state(&self, obj_id: &ObjId,
                                 inode_id: u64, field_tag: u32)
        -> NdnResult<(CascadeState, Option<u64>)>;

    /// 同步触发一次 verify_anchor，写回状态后返回
    pub async fn verify_anchor(&self, obj_id: &ObjId) -> NdnResult<CascadeState>;
}
```

`ndm` 用法详见 `doc/ndm_gc.md` §17（新增章节）。家用 UI 的简单语义：

- 文件或目录显示"已离线" iff 对应 anchor 是 `Complete`。
- 状态为 `Materializing` → 显示进度图标 / "正在下载"。
- 状态为 `Broken` → 红色感叹号 / "publisher 不可达，可能需要重新连接源"。
- 状态为 `Pending` → 灰色 / "等待源响应"。

> 这一节是对 review 第 3 条意见的正式回应。**单独建模 pin completeness** 让上层不必再用 "anchor 行存在" 来推断 "内容已物化"——这两个事实在所有真实场景里都必须分开。

beta1 chunk 没有自己的 `last_access_time`，淘汰时回退到 `stat(file).atime`，受文件系统挂载选项影响（`noatime` / `relatime`）。这是一个长期未察觉的 bug，导致 chunk 的淘汰顺序不可控。修复方案：

### 6.1 表结构

`last_access_time` 直接进 `chunk_items` 和 `objects`，索引为 `(eviction_class, last_access_time)`。LRU 扫描走这个复合索引，先按 class 过滤再按时间排序，可以非常自然地"只淘汰 class=0"。

### 6.2 in-memory 热表 + relatime 回写

每次 `open_chunk_reader` / `get_object` 都走 db UPDATE 是不可接受的（家用 NAS 上读吞吐会被毁掉）。采用 **relatime + in-memory 热表**：

```rust
struct LruHotTable {
    // obj_id → (last_seen_in_mem, dirty)
    entries: DashMap<ObjId, (u64, bool)>,
    flush_threshold: Duration,    // 例如 5 分钟
}

fn touch(obj_id):
    let now = unix_timestamp();
    let prev = hot.entries.get(obj_id).map(|e| e.0).unwrap_or(0);
    // relatime 规则：差值小于阈值就跳过
    if now - prev < FLUSH_THRESHOLD: return;
    hot.entries.insert(obj_id, (now, true));   // 标 dirty

// 后台 flush 任务，定期批量写回 db
fn flush():
    let dirty = hot.entries.iter().filter(|e| e.1).take(BATCH).collect();
    db.batch_update_last_access(dirty);
    for d in dirty: hot.entries.alter(d.key, |_, _| (d.value.0, false));
```

要点：

- 读路径只摸 in-memory 表，不阻塞。
- 后台 flush 把 dirty 项批量写入 db；崩溃丢失最近 N 分钟的访问时间在缓存语义里完全可以接受。
- LRU 扫描时若想精确，可以先 `flush()` 一次再扫。
- chunk 与 object 共享同一个热表实现。

### 6.3 LRU 候选查询

```sql
-- 取 N 个最老的 cache 对象
SELECT obj_id FROM objects
WHERE eviction_class = 0
ORDER BY last_access_time ASC
LIMIT ?;

-- chunk 同理
SELECT chunk_id FROM chunk_items
WHERE eviction_class = 0
ORDER BY last_access_time ASC
LIMIT ?;
```

复合索引 `(eviction_class, last_access_time)` 让这条查询走单次索引扫描，不需要排序。

---

## 7. 强制 GC 与水位

### 7.1 水位定义

| 水位 | 含义 | 行为 |
|---|---|---|
| `low` | 用量 < low_watermark | 不做任何 GC |
| `high` | low ≤ 用量 < high_watermark | 后台 GC 慢节奏，每轮淘汰一小批 cache 对象 |
| `critical` | high ≤ 用量 < hard_limit | 后台 GC 全速，写入路径开始限流 |
| `hard_limit` | 用量 ≥ hard_limit | **写入路径立即报错 ENOSPC**，强制 GC 同步触发 |

水位只看 class=0 还能腾出多少：

```
free_for_cache = hard_limit - sum(class=2 size) - sum(class=1 size)
                            - sum(class=0 size)
```

class=2 的来源现在有两个等价路径：`pins` 行 *或* `fs_anchor_count > 0`。从水位决策的角度它们没有区别——两者都是上层"我承诺这玩意必须留下"的硬声明，受同一条红线保护。

如果 `sum(class=1) + sum(class=2)` 已经 ≥ `hard_limit`，**直接进入 ENOSPC 状态**，不再尝试腾空间——参见 §7.3 红线。

### 7.2 GC 算法

```rust
async fn gc_round(&self, target_bytes: u64) -> NdnResult<u64> {
    let mut freed = 0;
    let batch = 256;

    loop {
        // 候选：本 bucket 的 home 对象、class=0、最老的一批
        let candidates = db.execute("
            SELECT obj_id, size FROM objects
            WHERE eviction_class = 0
              AND home_epoch IS current_epoch
            ORDER BY last_access_time ASC
            LIMIT ?
        ", batch)?;

        if candidates.is_empty() { break; }

        for (obj_id, size) in candidates {
            // 外部 root 认领即跳过（RootProvider 仅给"轻量黑名单"用户使用，
            // 不给 fs_meta 用——FS 走 fs_anchor_count，已经在上面的 SQL 里过滤掉了）
            if self.providers.iter().any(|p| p.is_rooted(&obj_id).unwrap_or(true)) {
                continue;
            }
            // double-check：扫描和回收之间可能并发 pin / fs_acquire
            if db.has_active_pin(&obj_id)?
                || db.has_incoming(&obj_id)?
                || db.fs_anchor_count(&obj_id)? > 0 {
                db.fix_eviction_class(&obj_id);   // 修补漂移
                continue;
            }
            self.collect_one(&obj_id).await?;
            freed += size;
            if freed >= target_bytes { return Ok(freed); }
        }
    }
    Ok(freed)
}
```

`collect_one` 路径：

```rust
fn collect_one(obj_id):
    txn.begin()
        row = db.lookup(obj_id)
        if row.state == 'present':
            // class 0 就是普通 cache，不发 outbox（它本来也没注册过 cascade）
            if db.has_incoming(obj_id):
                db.update(obj_id, state='shadow', content=NULL)   // 留住边
            else:
                db.delete(obj_id)
        elif row.state == 'shadow' and not db.has_incoming(obj_id):
            db.delete(obj_id)
    txn.commit()
    delete_blob_file(obj_id)   // commit 之后做
```

为什么淘汰 class=0 时不需要 outbox：class=0 意味着自己没被任何 pinned 祖先 cascade 持有，也没被自己直接 pin。这种对象当初进入 store 时就没写过任何 outbox/incoming_refs，所以删除时也不需要撤回任何东西。这是缓存模型的最大简化收益。

### 7.3 红线：强制 GC 不动 class 1/2

```rust
// 强制 GC 入口（写入路径触发）
async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64> {
    let freed = self.gc_round(target_bytes).await?;
    if freed >= target_bytes {
        return Ok(freed);
    }

    // 还不够 → class=0 已经空了，剩下全是 class 1/2
    // 红线：绝不淘汰 pinned / referenced 对象
    Err(NdnError::OutOfSpace(format!(
        "no class-0 cache left to evict; freed {}/{}; pinned+referenced exceeds quota",
        freed, target_bytes
    )))
}
```

写入路径在收到 `OutOfSpace` 时**直接传给上层**，由上层决定是清理 pin / `fs_release` 一些 anchor，还是放弃写入。`named_store` 自己绝不替用户做这个决定。

红线的等价表述：**`pins` 行任意非空 OR `fs_anchor_count > 0` 都是不可侵犯的承诺**。两条入口（用户 pin 与 fs_anchor）在 GC 视角下完全平权，没有谁可以 override 谁——这正是把 fs_anchor 设计成"批量 Recursive pin"的根本目的。

### 7.4 后台 GC 节奏

```
loop {
    let pressure = self.measure_pressure();
    match pressure {
        Low      => sleep(60s),
        High     => { gc_round(small_batch); sleep(10s); }
        Critical => { gc_round(big_batch);   sleep(1s); }
        Hard     => { /* 由写入路径触发 forced_gc_until */ sleep(0); }
    }
}
```

后台 GC 永远只做一件事：**把 class=0 里最老的赶出去**。

---

## 8. 影子对象的乱序到达（在 cache-first 模型下）

普通 put 不写 incoming_refs，所以"乱序到达"的复杂度只发生在 **pinned 子树展开过程中**。场景：

- 用户对 `D`（DirObject）执行 `pin(D, Recursive)`。
- `D` 引用 `S`（SubDir），`S` 引用 `SS`。
- Maglev 把 `D / S / SS` 散到三个不同 bucket，且 `S` 还没下载到本地。

| 时刻 | 位置 | 事件 | 结果 |
|---|---|---|---|
| t0 | B_d | `pin_local(D, Recursive)` | `D.eviction_class=2`, `pins(D)+`, outbox `add(referee=S, referrer=D)` |
| t1 | B_d → B_s | sender 投递 | `B_s: S shadow`, `incoming_refs += (S,D)`, `S.eviction_class=1`。S 是 shadow，不能继续 cascade |
| t2 | B_s | `put_object(S, content)` 实际下载到达 | shadow → present；因为 `has_incoming(S)`，把 S 的 children cascade 出去：outbox `add(referee=SS, referrer=S)` |
| t3 | B_s → B_ss | sender 投递 | `B_ss: SS shadow / 或 present`, `incoming_refs += (SS,S)`, `SS.eviction_class=1` |

不变量在每个 bucket 本地成立：

> `incoming_refs(X) 行集 == 所有"已被声明过的、referee=X 的边"`
>
> "已被声明过"意味着源 bucket 的 outbox 已经投递到 X 的 home bucket。

t1→t2 之间存在延迟：`S` 在 `B_s` 上被标成 shadow + class=1，但还没人下载内容。这段时间里 `B_s` 的 GC 看到 `S` 是 class=1，**不会**淘汰它。这是 eviction_class 升级早于 content 到达的好处。

如果 t2 永远不发生（比如 `S` 的发布者下线了）：`S` 在 `B_s` 上以 shadow + class=1 长期占位，不占 blob 空间，只占一行 db。`Recursive` pin 的语义在这种情况下退化成"目录骨架已保存，叶子内容按需重拉"，与 `Single` pin 在视觉上很相似，只是 cascade 的边都已经物化好了。

### 8.1 跨设备离线

如果 `B_s` 所在设备一直离线：

- `B_d` 的 outbox 条目 `(add, S, D)` 无限重试，按指数退避。
- `D` 本身在 `B_d` 上 durable。
- `B_s` 上线后 sender 把积压的 cascade 投过去——若 `S` 在此期间被某个老版本逻辑误删，shadow 会被重建，`incoming_refs` 也会被重建，**只是内容需要重拉**。
- 这正是缓存模型可以接受的：**引用关系最终一致；缓存内容可以丢，因为它本来就可以丢**。

---

## 9. 布局迁移（epoch 变化）与 GC 状态搬迁

### 9.1 现状

`NamedStoreMgr` 当前 `compact()` 直接 `truncate(1)` 丢弃旧 layout——这在无 GC 时勉强能工作，但加上 GC 后必须改为"先迁移再丢弃"。

### 9.2 home / visiting / tombstone

`home_epoch` 列记录该行是哪个 epoch 写入的。运行时按 `current_layout` 重新计算每个对象的归属：

- **home 对象**：`current_layout.select(obj_id).bucket == this_bucket`。可被 GC 淘汰。
- **visiting 对象**：`current_layout.select(obj_id).bucket != this_bucket`。**不可 GC**，由迁移 worker 搬走。
- **tombstone**：迁移完成的对象在源 bucket 留一条短期记录，用于转发晚到的 cascade edge。

GC 第一道 filter：

```sql
WHERE eviction_class = 0
  AND <home check : current_layout.select(obj_id).bucket == this_bucket>
```

### 9.3 迁移协议

由 mgr 启动的后台 worker，每个 bucket 独立扫描自己的 visiting 对象。**单对象迁移搬的不只是 content，还包括 GC 元数据**：

**Phase 1: Copy + Merge** —— 在新 home bucket 的本地事务里：

```rust
fn migrate_phase1(s, b_old, b_new):
    let old_content      = b_old.get_content(s);
    let old_refs         = b_old.list_incoming_refs(s);   // 含 declared_gen
    let old_pins         = b_old.list_pins(s);
    let old_anchors      = b_old.list_fs_anchors(s);
    let old_outbox       = b_old.list_outbox_where_referrer(s);  // 含 referrer_gen
    let old_atime        = b_old.last_access_time(s);
    let old_cascade_gen  = b_old.cascade_gen(s);          // s 作为 referrer 的发送侧时钟
    let old_in_states    = b_old.list_incoming_cascade_states_for(s);
    // ↑ s 作为 referee 的接收侧水位线集合：
    // 即 incoming_cascade_state 表里所有 (referrer=Y, last_seen_gen=g) 行，
    // 其中 Y ∈ s 的 incoming_refs。

    let txn = b_new.begin();
    match b_new.lookup(s) {
        Some(r) if r.state == "shadow" => txn.update(s, state="present", content=old_content),
        Some(r) if r.state == "present" => {/* 内容寻址相同，幂等 */},
        None => txn.insert(s, state="present", content=old_content, last_access_time=old_atime),
    }

    // cascade_gen 是发送侧单调时钟，必须取 max(新, 旧)，禁止回退
    txn.exec("UPDATE objects SET cascade_gen = MAX(cascade_gen, ?) WHERE obj_id = ?",
             old_cascade_gen, s);

    for r in old_refs    {
        // 同样：incoming_refs.declared_gen 也只能向上更新
        txn.exec("INSERT INTO incoming_refs(...) VALUES(...)
                  ON CONFLICT(referee, referrer) DO UPDATE
                    SET declared_gen = MAX(declared_gen, excluded.declared_gen)", r);
    }
    for p in old_pins    { txn.exec("INSERT OR IGNORE INTO pins ...", p); }
    for a in old_anchors { txn.exec("INSERT OR IGNORE INTO fs_anchors ...", a); }
    for o in old_outbox  { txn.push_outbox(o); }   // referrer_gen 跟着搬

    // 接收侧水位线：把 s 涉及的 (referrer, last_seen_gen) 也合并进新 bucket，
    // 同样取 max——确保单调性跨 epoch 不破。
    for (referrer, gen, ts) in old_in_states {
        txn.exec("INSERT INTO incoming_cascade_state(referrer, last_seen_gen, last_seen_at)
                  VALUES(?,?,?)
                  ON CONFLICT(referrer) DO UPDATE
                    SET last_seen_gen = MAX(last_seen_gen, excluded.last_seen_gen),
                        last_seen_at  = MAX(last_seen_at, excluded.last_seen_at)",
                 referrer, gen, ts);
    }

    txn.recompute_fs_anchor_count(s);   // 从 fs_anchors 重算物化列
    txn.recompute_eviction_class(s);    // 根据合并后的 pins/incoming_refs/fs_anchor_count 算
    txn.commit();
```

> **`cascade_gen` 跨 epoch 单调性是正确性的关键**：如果新 bucket 上 `s.cascade_gen` 起始为 0，而旧 bucket 上 `s.cascade_gen` 已经到了 1000，那么任何延迟到达的 gen=500 旧消息都会比新 bucket 上的水位线大，被错误地接受。所以 phase1 必须把 `cascade_gen` 也搬过来。同理，`incoming_cascade_state.last_seen_gen` 必须按 referrer 取 max 合并——这样新 bucket 在迁移完成后立刻具备拒绝任何 gen 落后于旧 bucket 已处理水位的旧消息的能力。

**Phase 2: Tombstone + 清理** —— 在旧 bucket：

```rust
fn migrate_phase2(s, b_old):
    let txn = b_old.begin();
    txn.delete_content(s);
    txn.delete_incoming_refs(s);
    txn.delete_pins(s);
    txn.delete_fs_anchors(s);
    txn.delete_outbox_where_referrer(s);
    txn.insert_tombstone(s, migrated_to_epoch=current_epoch, expires=now+grace);
    txn.commit();
    b_old.delete_blob_file(s);
```

**Phase 3: tombstone 到期清理** —— 例行维护中：

```sql
DELETE FROM tombstones WHERE expires < strftime('%s','now');
```

### 9.4 晚到的 edge 转发

```rust
fn apply_edge(msg):
    if let Some(tomb) = db.get_tombstone(msg.referee) {
        let new_bucket = layout_at(tomb.migrated_to_epoch).select(msg.referee);
        return forward_edge(msg, new_bucket);
    }
    if current_layout.select(msg.referee).bucket != this_bucket {
        return forward_edge(msg, current_layout.select(msg.referee));
    }
    /* 正常落库 —— 进入 §5.3 的 gen 过滤 */
}
```

**`apply_edge` 只在 home bucket 落盘，非 home 一律转发。** 转发不改变 `referrer_gen`——gen 由发送侧分配，途中无论转发多少跳都是同一份，最终由 referee 的 home bucket 上的 §5.3 gen 过滤决定接受 / 丢弃。

> **`referrer_gen` 是这条转发链的正确性保险**：tombstone 转发会让一条 add/remove 比预期晚到目标 bucket，叠加迁移时 `incoming_cascade_state` 已经被搬家、新 bucket 又处理了若干新 gen 之后，旧 add 才被转发到。没有 gen 过滤时它会把已经被撤回的边复活；有 gen 过滤后它在 §5.3 的第一条 if 就被丢弃。这是 review 第 2 条意见的核心修复。

### 9.5 触发条件

| 触发 | 场景 |
|---|---|
| 后台扫描 | epoch 切换后 worker 在空闲时逐个扫描旧桶的 visiting 对象 |
| 读触发（copy-on-read） | `get_object` 在旧桶 fallback 命中时顺便 migrate |
| compact 前置条件 | `compact()` 在 truncate 旧 layout 之前必须先确认所有 visiting 对象已迁完 |

```rust
pub async fn compact(&self) -> NdnResult<()> {
    let versions = self.versions.read().await;
    if versions.len() <= 1 { return Ok(()); }
    let oldest = versions.last().unwrap();
    for bucket in self.buckets_in(oldest) {
        if bucket.count_visiting(oldest.epoch)? > 0 {
            return Err(NdnError::InvalidState(format!(
                "bucket {} still has visiting objects from epoch {}",
                bucket.store_id(), oldest.epoch
            )));
        }
    }
    drop(versions);
    self.versions.write().await.pop();
    Ok(())
}
```

### 9.6 GC 与迁移的时序约束

> **GC 只动 home 对象，迁移只动 visiting 对象。** 两者互不干扰，可以并行。

---

## 10. 竞态与一致性

### 10.1 加 path 与 GC 的 TOCTOU

**问题**：GC 看到 X 是 class=0、准备删；FS 并发 `insert path → X`（即 ndm 调用 `fs_acquire(X)`）；GC 删除 X，FS 留下悬空 path。

**解法**：共享同一个 sqlite 文件，且 ndm 的 `fs_acquire` 与 named_store 的 GC 都跑在同一连接的 IMMEDIATE 事务里。SQLite 的写串行化保证 GC 的 `collect_one` 和 ndm 的 `fs_acquire_local` 不会同时进行——竞态被还原成单进程内的"先到先得"，由 §7.2 的 double-check (`fs_anchor_count > 0` 在事务内复查) 兜底。

`RootProvider` 仍然存在，但只服务非 FS 的轻量黑名单需求；ndm 不再走 RootProvider，详细原因见 §15。

### 10.2 GC 与并发 pin / fs_acquire 的 TOCTOU

GC 候选扫描和 `collect_one` 之间可能并发出现 pin 或 `fs_acquire`。`collect_one` 必须在事务内**重新检查** `has_active_pin`、`has_incoming` 和 `fs_anchor_count`，命中就放弃这次回收并修正 eviction_class。这就是 §7.2 中那段 double-check 的作用。

### 10.3 文件与 db 的一致性

- `put_chunk`：先写 tmp → fsync → rename 到 final → db 事务（insert）→ commit。崩溃在 commit 前 = tmp/final 残留 → 启动期孤儿扫描清理。
- `collect_one`：db 事务（删行或转 shadow）→ commit → 删 blob 文件。崩溃在删文件前 = 文件成孤儿 → 启动期扫描清理。
- 反向永远不允许：**不能先动文件再动 db**。db 是事实的权威。

启动期扫描伪代码：

```
for each blob file F on disk:
    if not db.contains(F.obj_id) or db.row(F.obj_id).state != 'present':
        delete F
```

### 10.4 incoming_refs 重建

`incoming_refs` 现在只服务 cascade（pin Recursive 与 fs_anchor 两条入口），所以重建变得很便宜——遍历两张承诺表，对每个 cascade 根重新跑一次：

```
for each pin in pins where scope='recursive':
    enqueue_outbox('add', referee=child, referrer=pin.obj_id, epoch)
        for each child in parse_obj_refs(pin.obj_id)

for each anchor_root in (SELECT DISTINCT obj_id FROM fs_anchors):
    enqueue_outbox('add', referee=child, referrer=anchor_root, epoch)
        for each child in parse_obj_refs(anchor_root)
```

剩下的让 outbox sender 自然展开。`Skeleton` / `Lease` pin 不需要 cascade；普通 cache 对象不需要任何边。`apply_edge` 在每一层走 `has_skeleton_pin` 检查，所以重建时不需要专门跳过 Skeleton 子树——它会被自然拦住。

### 10.5 fs_anchor_count 的物化重建

`fs_anchor_count` 是从 `fs_anchors` 物化出来的整数列。崩溃恢复或迁移时按 `obj_id` 分组重算即可：

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

`pins` 表和 `RootProvider` 完全不受影响。`fs_anchors` 表和 `pins` 表彼此独立，但通过共同的物化列 `eviction_class` 在 GC 视角下平权——这就是把 fs_anchor 设计成"批量 Recursive pin"的全部技术含义。

---

## 11. 大 Chunk 的 SameAs 接口

### 11.1 32MB 上限的写入 guard

```rust
const MAX_STANDARD_CHUNK_SIZE: u64 = 32 * 1024 * 1024;
if chunk_size > MAX_STANDARD_CHUNK_SIZE {
    return Err(NdnError::InvalidParam(
        "chunk exceeds 32MB; use ChunkList + SameAs".into()
    ));
}
```

涉及位置：`open_chunk_writer` / `open_new_chunk_writer` / `put_chunk` / `put_chunk_by_reader`。
`add_chunk_by_link_to_local_file` 是兼容已有大文件的后门，**不**受此限制。

### 11.2 `add_chunk_by_same_as`

```rust
pub async fn add_chunk_by_same_as(
    &self,
    big_chunk_id: &ChunkId,
    chunk_list_id: &ObjId,
) -> NdnResult<()>;
```

行为：

1. 校验 `chunk_list_id` 已 Present；
2. 解析 ChunkList，校验所有 sub-chunks 已 Present，size 累加 == big_chunk_id.size；
3. 流式 hash ChunkList 的内容拼接，得到的 ChunkId 必须 == big_chunk_id（**这是密码学校验，不是元数据声明**）；
4. 写入 `chunk_items` 一行 `state=present`、`ChunkStoreState::SameAs(chunk_list_id)`。

读路径在 `ChunkStoreState::SameAs(list_id)` 分支上重定向到 ChunkList，按顺序拼接 sub-chunks。

注意：**这个调用本身不写 incoming_refs / outbox**。big_chunk 与 ChunkList 之间的关系靠 `SameAs` 这个 state 字段直接表达；当 big_chunk 因为属于某个 pinned 子树而被 cascade 持有时，cascade 走的是普通的 `parse_obj_refs` 链，到 big_chunk 这一层会自然把 ChunkList 也 cascade 进去（因为 SameAs 应被视为一条出边）。

### 11.3 `ChunkStoreState::SameAs(ObjId)` 新变体

```rust
pub enum ChunkStoreState {
    New,
    Completed,
    Incompleted,
    Disabled,
    NotExist,
    LocalLink(ChunkLocalInfo),
    SameAs(ObjId),     // 新增
}
```

`can_open_reader` 增加 `SameAs(_) => true`。`parse_obj_refs(big_chunk)` 在 SameAs 分支返回 `[chunk_list_id]`，让 cascade 链能自然延伸到 ChunkList。

---

## 12. 接口落地

### 12.1 NamedLocalStore（每个 bucket 的本地存储）

```rust
// root provider
pub fn register_root_provider(&self, p: Arc<dyn RootProvider>);

// pin / unpin
pub async fn pin(&self, obj_id: &ObjId, owner: &str,
                 scope: PinScope, ttl: Option<Duration>) -> NdnResult<()>;
pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()>;
pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize>;

// fs_anchor（FS 层入口；语义=批量 Recursive pin，详见 §5.5 与 doc/ndm_gc.md）
pub async fn fs_acquire(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;
pub async fn fs_release(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;
pub async fn fs_release_inode(&self, inode_id: u64) -> NdnResult<usize>;

// SameAs 大 Chunk
pub async fn add_chunk_by_same_as(
    &self,
    big_chunk_id: &ChunkId,
    chunk_list_id: &ObjId,
) -> NdnResult<()>;

// 跨 bucket cascade 的接收端
pub async fn apply_edge(&self, msg: EdgeMsg) -> NdnResult<()>;

// LRU touch（在读路径上调）
pub fn touch(&self, obj_id: &ObjId);

// GC
pub async fn run_background_gc(&self) -> NdnResult<GcReport>;
pub async fn forced_gc_until(&self, target_bytes: u64) -> NdnResult<u64>;

// 完成度查询（§5.7）
pub async fn anchor_state(&self, obj_id: &ObjId, owner: &str)
    -> NdnResult<(CascadeState, Option<u64>)>;
pub async fn fs_anchor_state(&self, obj_id: &ObjId,
                             inode_id: u64, field_tag: u32)
    -> NdnResult<(CascadeState, Option<u64>)>;
pub async fn verify_anchor(&self, obj_id: &ObjId) -> NdnResult<CascadeState>;
```

### 12.2 NamedStoreMgr（mgr 层）

```rust
pub async fn start_edge_sender(&self);
pub async fn start_migration_worker(&self);
pub async fn start_lru_flusher(&self);

pub async fn pin(&self, obj_id: &ObjId, owner: &str,
                 scope: PinScope, ttl: Option<Duration>) -> NdnResult<()>;
pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()>;

pub async fn fs_acquire(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;
pub async fn fs_release(&self, obj_id: &ObjId,
                        inode_id: u64, field_tag: u32) -> NdnResult<()>;

pub async fn rebuild_all_refs(&self) -> NdnResult<()>;
pub async fn run_gc(&self) -> NdnResult<GcReport>;
pub async fn compact(&self) -> NdnResult<()>;   // 改为先确认 visiting=0 再 truncate
```

### 12.3 store_db.rs 内部新增

```rust
// state / class / lru
fn upsert_shadow_if_absent(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<()>;
fn set_eviction_class(&self, obj_id: &ObjId, class: u8) -> NdnResult<()>;
fn touch_last_access(&self, obj_id: &ObjId, ts: u64) -> NdnResult<()>;
fn batch_touch_last_access(&self, items: &[(ObjId, u64)]) -> NdnResult<()>;

// incoming_refs CRUD
fn insert_incoming_ref(&self, referee: &ObjId, referrer: &ObjId, epoch: u64,
                       txn: &Transaction) -> NdnResult<()>;
fn remove_incoming_ref(&self, referee: &ObjId, referrer: &ObjId,
                       txn: &Transaction) -> NdnResult<()>;
fn has_incoming(&self, referee: &ObjId) -> NdnResult<bool>;

// outbox CRUD
fn push_outbox(&self, op: &str, referee: &ObjId, referrer: &ObjId, epoch: u64,
               txn: &Transaction) -> NdnResult<()>;
fn pop_outbox_batch(&self, limit: usize) -> NdnResult<Vec<OutboxEntry>>;
fn ack_outbox(&self, seq: i64) -> NdnResult<()>;

// pins
fn upsert_pin(&self, obj_id: &ObjId, owner: &str, scope: PinScope,
              expires_at: Option<u64>, txn: &Transaction) -> NdnResult<()>;
fn delete_pin(&self, obj_id: &ObjId, owner: &str, txn: &Transaction) -> NdnResult<()>;
fn has_active_pin(&self, obj_id: &ObjId) -> NdnResult<bool>;
fn has_skeleton_pin(&self, obj_id: &ObjId) -> NdnResult<bool>;

// fs_anchors
fn insert_fs_anchor(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32,
                    txn: &Transaction) -> NdnResult<bool>;
fn delete_fs_anchor(&self, obj_id: &ObjId, inode_id: u64, field_tag: u32,
                    txn: &Transaction) -> NdnResult<bool>;
fn fs_anchor_count(&self, obj_id: &ObjId) -> NdnResult<u64>;

// LRU 候选
fn list_lru_candidates(&self, class: u8, limit: usize) -> NdnResult<Vec<(ObjId, u64)>>;
```

### 12.4 ndn-lib 层

```rust
pub trait HasRefs {
    fn referenced_ids(&self) -> Vec<ObjId>;
}
// 实现：DirObject、ChunkList、RelationObject、FileObject、...
pub fn parse_obj_refs(obj_type: &str, content: &str) -> NdnResult<Vec<ObjId>>;
```

---

## 13. 不变量清单

下列不变量在每个 bucket 本地成立，不依赖跨 bucket 实时查询。EC = Eventually Consistent（outbox drain 完毕后才成立）。

1. **[强]** `state in {'present', 'shadow', 'incompleted'}`
2. **[强]** `state == 'shadow' ⇒ obj_data IS NULL ∧ blob 文件不存在`
3. **[强]** `state == 'present' ⇒ obj_data IS NOT NULL`（或对应 blob 存在）
4. **[强]** 所有 blob 文件都对应一行 `state=='present'` 的 db 记录
5. **[强]** `incoming_refs(X, Y)` 主键唯一 ⇒ 幂等
6. **[强]** `fs_anchors(obj_id, inode_id, field_tag)` 主键唯一 ⇒ 幂等
7. **[强]** 引用图始终是 DAG（由内容寻址保证）
8. **[强]** `fs_anchor_count(X) == COUNT(*) FROM fs_anchors WHERE obj_id == X`（物化列与 `fs_anchors` 始终一致，由同一事务维护）
9. **[强]** `eviction_class` 是 `pins ∪ incoming_refs ∪ fs_anchors` 的物化缓存：
   - `has_active_pin(X) ∨ fs_anchor_count(X) > 0 ⇒ eviction_class(X) == 2`
   - `¬(has_active_pin(X) ∨ fs_anchor_count(X) > 0) ∧ has_incoming(X) ⇒ eviction_class(X) == 1`
   - 其它 ⇒ `eviction_class(X) == 0`
10. **[强]** **强制 GC 永不淘汰 `eviction_class >= 1` 的对象**（红线）
11. **[强]** outbox 条目与 pin/unpin/fs_acquire/fs_release/apply_edge 在同一本地事务中 commit
12. **[EC]** `incoming_refs(X) 行集 == {(Y, X) : Y 在某个 active cascade 根（pin Recursive 或 fs_anchor）所辖的子树内, 且 Y 与 X 之间不存在 Skeleton 拦截器, X ∈ parse_refs(Y), bucket_of(X)==this_bucket}`
13. **[强]** Skeleton 前向不变：若 `has_skeleton_pin(D)`，`apply_edge` 在 `referee == D` 的事务里**不**为 D 的 children enqueue 任何 outbox（add 与 remove 对称）
14. **[弱·非追溯]** Skeleton 仅前向生效：在 `pin(D, Skeleton)` 时刻已经存在的 incoming_refs 不被追溯撤回（见 §5.6 Case B）
15. **[强]** ChunkList 内容里出现的每个 sub-chunk_id 在 `chunk_items` 里有 row（present 或 shadow）—— 由 cascade 落地建立
16. **[强]** GC 只回收 home 对象，迁移只动 visiting 对象
17. **[强]** `compact()` 不得删除仍有 visiting 对象的 layout version
18. **[强]** `apply_edge` 只在 referee 的 home bucket 落盘，非 home 一律转发
19. **[强]** `last_access_time` 单调不减（in-mem 热表 flush 时取 `MAX(old, new)`）
20. **[弱]** 普通 `put_object` 是 O(1)：不写 outbox、不写 incoming_refs、不写 fs_anchors、不触发 cascade
21. **[强]** `objects.cascade_gen(X)` 单调不减：所有写路径（`bump_cascade_gen`、迁移合并）都只允许向上走
22. **[强]** `incoming_cascade_state.last_seen_gen[R]` 在 referee 的 home bucket 上对每个 referrer R 单调不减（迁移合并时取 max）
23. **[强]** `incoming_refs(X, Y).declared_gen` 单调不减：`apply_edge` 在写入 / 更新一条边时只允许把 declared_gen 提高
24. **[强]** Cascade 消息接受规则：`apply_edge(msg)` 落库（含 incoming_refs 的 add 与 remove、向下 cascade 的入队）当且仅当 `msg.referrer_gen >= last_seen_gen[msg.referrer]`，否则整条消息被丢弃
25. **[强]** Cascade 消息覆盖规则：`apply_edge('remove', X, Y, g)` 只删 `incoming_refs(X, Y)` 中 `declared_gen <= g` 的边；`apply_edge('add', X, Y, g)` 只覆盖 `declared_gen < g` 的同名行
26. **[弱]** Pin / fs_anchor 完成度状态（§5.7）只反映"已写回时刻"的真相：`anchor.cascade_state` 与 `anchor.verified_at` 不必实时——`Materializing → Complete` 的提升靠 verifier；上层判定离线可用性必须用最近一次 `verified_at` 的结果
27. **[强]** `cascade_state == 'Complete' ⇒` 在 `verified_at` 时刻该 anchor 的整棵子树（被 Skeleton 拦截器自然剪枝后）所有节点 `state == 'present'` 且没有 in-flight 的 cascade outbox 归属于本 anchor

---

## 14. 与现有代码的差量

| 文件 | 改动 |
|---|---|
| `store_db.rs` | 新建 `incoming_refs` / `edge_outbox` / `pins` / `fs_anchors` / `tombstones` 表；为 `chunk_items` / `objects` 加 `state`、`last_access_time`、`eviction_class`、`fs_anchor_count`、`home_epoch` 列；建复合索引（含 `pins_skeleton_by_obj` 部分索引、`fs_anchors_by_obj`）；CRUD 全套 |
| `local_store.rs` | 写入路径加 32MB guard；普通 `put_object` / `put_chunk` 改成 O(1)（不写 outbox）；新增 `pin` / `unpin` / `fs_acquire` / `fs_release` / `apply_edge` / `add_chunk_by_same_as` / SameAs 读路径；新增 LRU touch 与本地 GC 入口；`apply_edge` 内加 `has_skeleton_pin` cascade-stop 检查 |
| `local_store.rs::PinScope` | 三变体 `Recursive` / `Skeleton` / `Lease`（替换 beta1 的 `Recursive` / `Single`） |
| `local_store.rs::ChunkStoreState` | 新增 `SameAs(ObjId)` 变体；`can_open_reader` / `parse_obj_refs` 处理它 |
| `store_mgr.rs` | 新增 `start_edge_sender` / `start_migration_worker` / `start_lru_flusher`；`compact` 改为"先确认无 visiting 再 truncate"；`pin` / `unpin` / `fs_acquire` / `fs_release` 路由到 home bucket；`run_gc` 依次调各 bucket 只动 home |
| `store_layout.rs` | 不变 |
| `ndn-lib` | `HasRefs` trait + 各对象的 ref 解析器（DirObject, ChunkList, RelationObject, FileObject） |
| FS 层 (`ndm`) | 调用 `fs_acquire` / `fs_release`，详见 `doc/ndm_gc.md` |

---

## 15. 不打算做的事

- **不**做 mark-sweep 或 tracing。DAG + `incoming_refs` 集合 ⇒ 纯 refcount 充分。
- **不**让普通 `put` 写 `incoming_refs` / `fs_anchors` 或 outbox。普通 put 只进缓存层，O(1)。
- **不**在 named_store 内部维护 path 字符串。`fs_anchors` 只存 `(obj_id, inode_id, field_tag)` 三元组——inode 与 field 是 ndm 层的"反引用句柄"，不是路径。所有 path / dentry / 重命名语义都封装在 ndm 层，named_store 完全无感。
- **不**把 pin / fs_anchor 混入 `incoming_refs`。两类承诺是用户/上层语义，引用边是内容语义，两者独立可重建。
- **不**对外暴露 `insert_incoming_ref` / `remove_incoming_ref`。引用边只能从 cascade 自动产生。
- **不**在 GC 时做跨 bucket 查询。每个 bucket 的 GC 完全基于本地表 (含 `pins`、`fs_anchors`、`fs_anchor_count`) + RootProvider（仅限非 FS 的轻量黑名单）。
- **不**要求 outbox 投递的强实时性。cascade 是最终一致的；under-counting 期间 cache 模型可以接受重拉。
- **不**为强制 GC 让步红线。`class >= 1` 一律不动。空间不够就 `ENOSPC`，让上层去清 pin / fs_release。
- **不**让 Skeleton 升档触发追溯 tear-down，也**不**让 unpin Skeleton 自动重新展开 cascade。Skeleton 的语义全部是前向的——见 §5.6 Case A/B/C。
- **不**让 `Lease` pin 阻塞 cascade。Lease 是 cascade 透明的短期持有，避免短期窗口意外切断上游。
- **不**在 `local_store` 层处理"可变命名指针"。如果未来要做，那一层自己当 RootProvider 或调 `fs_acquire`。
