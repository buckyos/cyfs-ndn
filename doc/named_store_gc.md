# named_store GC 方案

## 目标

为 `named_store` 设计一套引用计数 GC，满足：

1. 大 Chunk（>32MB）必须用 ChunkList + SameAs 的方式表达，不允许直接落盘成单文件。
2. 一个 ObjId 在被父亲引用、被路径挂载、被会话临时持有的任意一种情况下都应当存活。
3. 父子对象可以乱序到达（"影子对象"问题），任何到达顺序都能恢复正确的引用计数。
4. 上层文件系统的 `path → obj_id` 海量映射不进 `named_store` 的内部存储。
5. 崩溃后可恢复：`refcount` 列必须可以从对象内容机械重算。

## 设计原理

### 内容寻址 ⇒ 引用图天然 DAG

所有 ObjId 都是内容的密码学哈希。`A` 引用 `B` 等价于 `B` 的 ObjId 写在 `A` 的字节里，意味着 `B_id` 在计算 `A_id` 之前就已存在。要构成环 `A → B → A` 需要哈希碰撞，在 SHA-256 强度下视同不可能。直接推论：

- 自引用不可能；
- 任意引用图都是 DAG；
- **纯引用计数 GC 是充分的**，不需要 mark-sweep 来打破环。

这个性质是整个方案能用引用计数的根本前提。一旦未来引入任何"可变容器"（可被改写的命名指针、目录条目等），它**不能**作为内容寻址 DAG 的节点参与 refcount，必须以外部 root 的形式存在。

### "存活"的三个理由

一个对象 X 应当存活，当且仅当下列任一条件成立：

1. **内部引用**：仓里某个 Present 对象的内容里写到了 `X_id`。由 `refcount` 列记账。
2. **外部根**：某个 `RootProvider`（典型是文件系统 path）声明它持有 X。
3. **临时持有**：会话/TTL/手动 pin 持有 X。由 `pins` 表记账。

GC 判定公式：

```
alive(X)  ⟺  refcount(X) > 0
           ∨  ∃ provider. provider.is_rooted(X)
           ∨  ∃ pin row for X (未过期)
```

三类理由分别由不同的数据结构持有，**互不混合**。这是整个设计的核心约束。

## 数据结构

### 对象/Chunk 表扩展

```sql
ALTER TABLE objects     ADD COLUMN refcount INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects     ADD COLUMN state    TEXT    NOT NULL DEFAULT 'present';
ALTER TABLE chunk_items ADD COLUMN refcount INTEGER NOT NULL DEFAULT 0;
```

`state` 取值：

| state | 含义 | 可读 | 可被引用 |
|---|---|---|---|
| `present` | 内容已落盘 | 是 | 是 |
| `incompleted` | 写入中（chunk_items 已有） | 否 | 否 |
| `shadow` | 仅占位，没有内容 | 否 | 是（持 refcount） |

`refcount` 列**只**反映"内容引用"，由 `add_ref` / `release_ref` 维护，可以随时清零重算。

### pins 表（仅给临时持有用）

```sql
CREATE TABLE pins (
    obj_id      TEXT NOT NULL,
    owner       TEXT NOT NULL,         -- 'user', 'session:xxx', 'lease:yyy'
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER,               -- NULL = 永久
    PRIMARY KEY (obj_id, owner)
);
CREATE INDEX pins_by_owner  ON pins(owner);
CREATE INDEX pins_by_expire ON pins(expires_at) WHERE expires_at IS NOT NULL;
```

设计要点：

- `(obj_id, owner)` 主键 ⇒ pin 天然幂等。
- 同一对象可被多个 owner 独立 pin/unpin，互不干扰。
- 会话断开 = `DELETE FROM pins WHERE owner='session:xxx'`。
- TTL 清扫 = `DELETE FROM pins WHERE expires_at < now`。
- **这张表不承载文件系统 path 引用**——FS 层有自己的反向索引，不要在这里复制一份。

### RootProvider 接口

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

文件系统层实现这个 trait，`is_rooted` 直接查它自己的 `obj_id → paths` 反向索引。`enumerate_roots` 仅在一致性扫描和 trace 调试时使用，可以慢一点。

## 引用计数协议

### 落盘对象时

```rust
fn put_object(obj_id, content):
    refs = parse_obj_refs(content)        // 解析内容里出现的所有 ObjId
    txn.begin()
        match db.lookup(obj_id):
            None:
                db.insert(obj_id, state='present', content, refcount=0)
                cascade_add_ref(refs)
            Some(row) where row.state == 'shadow':
                db.update(obj_id, state='present', content)   // 保留 refcount
                cascade_add_ref(refs)
            Some(row) where row.state == 'present':
                // 幂等：同一内容重复 put，不再 cascade
                return Ok(())
    txn.commit()
```

关键点：

- 是否 `cascade_add_ref` **由升迁前的 state 决定**，不是"行是否存在"。否则同一对象重复 put 会双计 refcount。
- `add_ref` 用 upsert 自动创建 shadow 行：
  ```sql
  INSERT INTO objects (obj_id, state, refcount) VALUES (?, 'shadow', 1)
  ON CONFLICT(obj_id) DO UPDATE SET refcount = refcount + 1;
  ```
- 整个写入 + cascade 必须在同一个 sqlite 事务里。

### 删除对象时

```rust
fn remove_object(obj_id):
    row = db.lookup(obj_id)
    if row.state == 'present':
        refs = parse_obj_refs(row.content)
        cascade_release_ref(refs)
        delete_blob_file(obj_id)
        if row.refcount > 0:
            db.update(obj_id, state='shadow', content=NULL)   // 还有人引用，降级
        else:
            db.delete(obj_id)
    elif row.state == 'shadow' and row.refcount == 0:
        db.delete(obj_id)
```

降级成 `shadow` 是必要的：当用户手动删除一个还被父亲引用的对象时，必须保留它的记账行，否则下一次 `release_ref` 会找不到行。

### 大 Chunk 的 SameAs 级联

`add_chunk_by_same_as(big_id, list_id)`：

1. 校验 `list_id` 已 Present；
2. 解析 ChunkList，验证所有 sub-chunk 都已 Present，size 累加 == big_id 的 size；
3. 流式 hash 一遍 ChunkList 内容，得到的 ChunkId 必须 == big_id；
4. `add_ref(list_id)`，把 big_id 写成 `ChunkStoreState::SameAs(list_id)`。

注意：**不要**对 sub-chunks 重复 add_ref。ChunkList 落盘时已经 cascade add_ref 过 sub-chunks 了，SameAs 只需要持有 ChunkList 一次。释放时对称：`release_ref(list_id)` → ChunkList 归 0 → cascade release sub-chunks。

## 影子对象的乱序到达

场景：D（DirObject）引用 S（SubDir），S 引用 SS。SS 已经在仓里，D 后到，S 最后到。

| 时刻 | 操作 | 状态 |
|---|---|---|
| t0 | SS 已 Present | `SS: present rc=1`（被某 root 持有） |
| t1 | `put_object(D)`，parse → [S] | `D: present rc=0`<br>`S: shadow rc=1`<br>`SS: present rc=1` |
| t2 | FS 给 D 挂上 path | FS.is_rooted(D) = true |
| t3 | `put_object(S)`，命中 shadow 行 | `S: present rc=1`（state 升迁，rc 不变） |
| t3 | parse S → [SS]，cascade add_ref | `SS: present rc=2` |

不变量在每一步都成立：

> `rc(X) == 仓里所有 Present 对象中真正在内容里写到 X 的个数`

**反向到达**（SS 比 D 先到，但 D 还没到）：t0 时 SS 刚 put 进来，rc=0，没人引用。如果立刻 GC 就丢了。这是 refcount 的能力边界——它不能预知未来。解决方式只能靠**外部 root**：

- 下载会话 pin：客户端开始下载某棵树前 `pin(root_id, owner='session:xxx')`，下载结束再 unpin；
- 新对象 lease：`put_object` 时给一个 grace TTL pin，到点未被父亲接住才回收；
- staging 模式：到达的对象先进暂存区，根对象到齐后批量提交。

实践中常用前两者叠加。

## GC 算法

```rust
async fn gc_round(&self) -> NdnResult<usize> {
    let candidates = self.db.list_zero_rc_objects()?;   // refcount == 0 的所有行
    let mut collected = 0;
    for obj_id in candidates {
        // 任一 provider 认领即跳过
        if self.providers.iter().any(|p| p.is_rooted(&obj_id).unwrap_or(true)) {
            continue;
        }
        // 任一未过期 pin 认领即跳过
        if self.db.has_active_pin(&obj_id)? {
            continue;
        }
        self.collect_one(&obj_id).await?;
        collected += 1;
    }
    Ok(collected)
}

async fn collect_one(&self, obj_id: &ObjId) -> NdnResult<()> {
    // 与 remove_object 同步路径
    self.remove_object_internal(obj_id).await
}
```

为什么不需要 trace：

- 候选集 = `refcount == 0` 的行，在健康系统里就是"DAG 的根 + 真正的垃圾"，规模与 FS path 数无关。
- 大多数对象 `refcount > 0`（被父亲引用），自动跳过。
- `is_rooted` 是 O(1) 查表，FS 反向索引天然支持。

GC 单轮成本 ≈ O(|根| + |垃圾|)，与 |存活内部节点| 和 |FS path| 都无关。

## 竞态与一致性

### 加 path 与 GC 的 TOCTOU

**问题**：GC 读到 X.rc=0、问 FS 未 rooted、准备删；FS 并发 `insert path → X`；GC 删除 X，FS 留下悬空 path。

**解法**（任选其一）：

1. **共享事务**：FS 与 named_store 共用同一个 sqlite 文件，GC 整段跑在 IMMEDIATE 事务里，并发的 path insert 被自然串行化。简单首选。
2. **GC epoch**：FS 每次写 path 自增 epoch；GC 第一阶段标记候选 + 当前 epoch，第二阶段删之前重新校验 epoch。适用于 FS 不在同一 db 的情况。
3. **写穿 named_store**：FS 加 path 之前调 `named_store.protect(obj_id, &mut txn)`，把 X 锁进事务的黑名单。代码侵入性高，不推荐。

### 文件与 db 的一致性

- `put_chunk` 路径：先写 tmp 文件 → fsync → rename 到 final → db 事务（insert + cascade add_ref）→ commit。崩溃在 commit 前 = tmp/final 残留 → 启动期孤儿扫描清理。
- `remove_*` 路径：db 事务（delete + cascade release_ref）→ commit → 删 blob 文件。崩溃在删文件前 = 文件成孤儿 → 启动期扫描清理。
- 反向永远不允许：**不能先动 db 再去动文件**。db 是事实的权威，文件只是 db 的物化。

启动期扫描伪代码：

```
for each blob file F on disk:
    if not db.contains(F.obj_id) or db.row(F.obj_id).state != 'present':
        delete F
```

### refcount 重建

崩溃 / 误用 / 历史代码漂移可能让 refcount 失真。提供一个离线工具或低峰任务：

```sql
BEGIN;
UPDATE objects     SET refcount = 0;
UPDATE chunk_items SET refcount = 0;
-- 然后遍历所有 Present 对象的内容，重新 cascade add_ref
COMMIT;
```

这一步**完全不碰** `pins` 表和 RootProvider，纯粹由内容驱动。这就是为什么 `refcount` 列必须只承载内容引用——一旦混入 pin/root，就再也不能这么重建了。

## 接口落地

`NamedLocalStore` 新增：

```rust
// root provider
pub fn register_root_provider(&self, p: Arc<dyn RootProvider>);

// pin / unpin (临时持有)
pub async fn pin(&self, obj_id: &ObjId, owner: &str, ttl: Option<Duration>) -> NdnResult<()>;
pub async fn unpin(&self, obj_id: &ObjId, owner: &str) -> NdnResult<()>;
pub async fn unpin_owner(&self, owner: &str) -> NdnResult<usize>;

// SameAs 大 Chunk
pub async fn add_chunk_by_same_as(
    &self,
    big_chunk_id: &ChunkId,
    chunk_list_id: &ObjId,
) -> NdnResult<()>;

// GC
pub async fn run_gc(&self) -> NdnResult<GcReport>;
pub async fn rebuild_refcount(&self) -> NdnResult<()>;
```

`store_db.rs` 内部新增：

```rust
fn add_ref(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<u64>;
fn release_ref(&self, obj_id: &ObjId, txn: &Transaction) -> NdnResult<u64>;
fn list_zero_rc(&self) -> NdnResult<Vec<ObjId>>;
fn has_active_pin(&self, obj_id: &ObjId) -> NdnResult<bool>;
```

`ndn-lib` 层补一个：

```rust
pub trait HasRefs {
    fn referenced_ids(&self) -> Vec<ObjId>;
}

// 实现：RelationObject、ChunkList、DirObject、...
fn parse_obj_refs(obj_type: &str, content: &str) -> NdnResult<Vec<ObjId>>;
```

## 32MB 上限的写入 guard

所有写入入口加：

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

## 不变量清单

下列不变量在任意时刻、任意崩溃恢复后都必须成立：

1. `refcount(X) == |{Y : Y.state=='present' ∧ X_id ∈ parse_refs(Y)}|`
2. `state in {'present', 'shadow', 'incompleted'}`
3. `state == 'shadow' ⇒ obj_data IS NULL ∧ blob 文件不存在`
4. `state == 'present' ⇒ obj_data IS NOT NULL`（或对应 blob 文件存在）
5. `state == 'shadow' ∧ refcount == 0` 是临时状态，下一次 GC 之前必须消失
6. 所有 blob 文件都对应一行 `state=='present'` 的 db 记录
7. ChunkList 内容里出现的每个 sub-chunk_id 都在 chunk_items 表里有 row（present 或 shadow）
8. 引用图始终是 DAG（由内容寻址保证，无需运行时校验）

## 与现有代码的差量

| 文件 | 改动 |
|---|---|
| `store_db.rs` | 加 `refcount` 列、`state` 列、`pins` 表；`add_ref`/`release_ref`/`list_zero_rc`/pin 系列 |
| `local_store.rs` | 写入路径加 32MB guard；`put_object`/`remove_*` 接 cascade；新增 `add_chunk_by_same_as` 与 SameAs 读路径；GC 入口 |
| `ndn-lib` | `HasRefs` trait + 各对象类型的 ref 解析器 |
| `local_store.rs::ChunkStoreState` | 新增 `SameAs(ObjId)` 变体 |
| FS 层 | 实现 `RootProvider`，注册到 named_store |

## 不打算做的事

- **不**做 mark-sweep。DAG 性质保证 refcount 充分。
- **不**在 named_store 内部维护 path 集合。FS 已经有了，复制只会引入同步问题。
- **不**把 pin 计数混进 refcount 列。混入会摧毁一致性扫描的可恢复性。
- **不**对外暴露 `add_ref` / `release_ref`。它们是内部原语，由 `put_object` / `remove_*` 在解析内容后自动调用。一旦暴露，调用方就能伪造引用关系，引入逻辑环。
- **不**在 `local_store` 层处理"可变命名指针"。如果未来要做，那一层自己当 RootProvider。
