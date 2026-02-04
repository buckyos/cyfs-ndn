# NDM系统架构设计摘要

> 这份摘要面向“第二位架构师”：用最短路径建立**正确心智模型**——NDM到底解决什么问题、核心抽象是什么、读写/扩容/一致性语义怎么定义、哪些地方千万别按 POSIX 或传统 DFS 经验脑补。

---

## 1. NDM 的定位与目标

**NDM（Named Data Manager / NamedMgr 系统）**是一个“内容寻址（CAS） + 可变命名（Path） + 多目标池化存储（Placement）”的组合系统，对外提供一组**类文件系统 API**，但**语义不是 POSIX**。

它主要解决两类需求：

1. **内容发布与同步**

* 底层数据以 `sha256:` 形式寻址（ObjId/ChunkId），对象不可变、可验证、可跨节点分发/拉取。
* 上层用 `path -> objid` 进行发布与更新：更新是“改指针”（COW），不是就地覆盖。

2. **在家用/边缘/Zero-Ops 环境中逐步扩容**

* 节点异构、不稳定、逐步加盘加节点。
* 强一致复制并不作为起点（可靠性可先依赖备份），但系统要能随着规模变大逐步提升可用性/可靠性。

---

## 2. 系统契约（必须牢记的硬规则）

这些规则决定了接口、实现与故障语义（很多坑都来自忽略它们）：

### 2.1 内容寻址与对象不可变

* ObjId / ChunkId 使用 `sha256:...`，属于内容寻址。
* **对象/Chunk 一旦写入并提交完成，就按不可变对待**（修改=生成新对象/新 chunk）。

### 2.2 Path 语义不是 POSIX

* NamedMgr 提供“类 FS 的 API”，但一致性/原子性/并发语义以 NDM 文档定义为准。
* 例如：`move/copy/delete` 多数情况下只影响 **path 绑定**，不等价于 POSIX 文件系统的“数据移动/删除”。

### 2.3 严格单写（Strict Single Writer）

* 同一时刻，一个 path **只允许一个写会话持有写租约**并写入工作态（file_buffer）。
* 其他写请求必须失败（如 `PATH_BUSY / LEASE_CONFLICT`）。
* 读：允许并发读取 **committed**；对 working 默认拒绝（可选允许同 session）。

### 2.4 Placement（类 CRUSH）支持 epoch 回退最多 2 个版本

* 写：始终使用当前 layout/selector（current epoch）计算 target。
* 读：如果当前 epoch 算不到/找不到数据，最多回退到 `epoch-1`、`epoch-2` 再查（最多 3 轮）。
* 这使得扩容/变更 layout 可以做到“改配置即生效”，数据迁移可延迟（lazy）。

### 2.5 ExternalLink 的 qcid 只是“快速变化检测”

* qcid 用来判断外部文件是否被改动，避免把可变外部数据当作稳定内容使用。
* 它**不参与对象寻址**，也不是常规路径。

### 2.6 默认不自动 GC 删除业务数据

* 大规模场景默认不做“自动删除业务数据”。
* 必须清理软状态（tmp、过期 lease、失败 pull 残留等）。
* 业务数据删除/本地驱逐通过显式接口（如 `erase_obj_by_id`）。

---

## 3. 核心概念与命名空间

### 3.1 命名空间隔离

* **Pool（pool_id）**：zone/集群级边界。
* **NDM Instance（ndm_id）**：pool 内 NamedMgr 的逻辑实例（命名空间隔离单元）。
* 完整路径：`ndm://pool_id/ndm_id/path`

### 3.2 三层标识体系（强烈建议用这个心智模型）

* **Path（可变）**：发布入口，映射到某个对象版本（objid）。
* **ObjId（不可变）**：对象（FileObject/DirObject/ChunkList 等）的内容地址。
* **ChunkId（不可变）**：块内容地址（≤128MB）。

---

## 4. 分层架构总览

可以把系统拆成 “Path 控制面” + “内容数据面” + “传输/同步面”：

```
   [App / API]
        |
    NamedMgr (类FS接口：add/move/open/create/pull/stat...)
        |
  +-----+-------------------------+
  |                               |
FsMetaDb (Path层强一致/租约)      NamedStore/NamedDataMgr (内容层CAS)
  |                               |
  |                      StoreLayout/Placement (按chunkid选target)
  |
FileBuffer Service (工作态写缓冲：单机mmap 或 多BufferNode/GFS风格)
        |
ndn_client / ndn_router (cyfs:// over http 传输、pull/push、跨设备访问)
```

### 4.1 FsMetaDb（Path 层）

* 负责 `path -> objid` 的绑定、目录/文件的 namespace 事务。
* 负责 **Strict Single Writer**：写租约 lease / fence / TTL。
* 负责挂载互斥（避免 path 子树与 DirObject 展开结果冲突）。
* 提供 `stat` 观测 committed/working/materialization 状态（pull 可观测）。

> 关键点：FsMetaDb 的一致性域决定“path 强一致”的边界（单机 SQLite / 未来可演进到 Raft）。

### 4.2 NamedStore / NamedDataMgr（内容层 CAS）

* 对象（NamedObject）存数据库（对象库）。
* Chunk 实体存文件系统（按 ChunkId 派生路径）。
* Link（SameAs/PartOf/ExternalFile）及反向索引属于内容层索引。
* 提供绕过 path 的 `open_reader_by_id / get_object` 等能力。

### 4.3 FileBuffer Service（工作态）

* 写入不直接写入最终 chunk store，而是先进入“工作态 buffer”。
* 单机实现可用 mmap；分布式实现可按目录策略选择 buffer node（更像 GFS chunkserver 模式）。
* 与 Strict Single Writer 联动：一个 path 的工作态只有一个写者。

### 4.4 Placement / StoreLayout（多 target 池化）

* 输入 ObjId/ChunkId，输出一个或多个 store target（store_id）。
* layout 变更通过 epoch 递增，读路径允许最多回退 2 个 epoch。
* 迁移可以 lazy（读修复/后台扫描），不作为 correctness 前提。

### 4.5 ndn_client / ndn_router（跨设备）

* ndn_router 绑定一个 named_mgr，对外提供 cyfs:// 服务端。
* ndn_client 负责拉取、校验、同步工具集（pull_chunk、pull_dir_to_local 等）。

---

## 5. 数据模型与对象关系（读懂就读懂一半）

### 5.1 NamedObject（对象）

* **DirObject**：目录对象，内部包含子项到 objid 的映射（可递归）。
* **FileObject**：文件对象，典型字段 `content -> ChunkList`。
* **ChunkList**：chunk 序列（大文件分片）。

### 5.2 Chunk（≤128MB）原子提交

默认不依赖断点续传：

* 写入 `chunk.tmp`
  -（可选）fsync
* 校验 sha256
* rename 为 `chunk.final`
* 标记 COMPLETE

### 5.3 Link（虚实结合）

对 chunk 支持三类 Link：

* `SameAs(A==B)`：别名/去重
* `PartOf(A ⊂ B)`：切片复用
* `ExternalFile(ChunkA由外部文件提供)`：原地纳管（qcid 快检）

**Reader 允许走 Link，Writer 永远只写 internal**（避免可变外部数据污染内容寻址语义）。

---

## 6. 关键流程（读、写、拉取、驱逐）

### 6.1 读路径（Path -> ObjId -> InnerPath -> ChunkReader）

1. `open_reader(path)`

* 若 path 为 committed：允许并发读。
* 若未 materialize：返回 `NEED_PULL/NOT_AVAILABLE`。
* 若 path 为 working：默认 `PATH_BUSY`（可选同 session 读 working）。

2. `open_reader_by_id(objid)`

* 直接从对象库解析对象，并按 inner_path 逐级解析：
  `DirObject -> child objid -> FileObject -> content -> ChunkList -> chunk[i]`

3. `GetChunkReader(chunkid)`

* 先尝试 internal chunk（带 epoch backoff）
* internal 不存在/不完整再解析 Link（SameAs/PartOf/ExternalFile）
* ExternalFile 读前做 qcid 快检，失败则视为 link invalid

### 6.2 写路径（Strict Single Writer + 工作态）

核心思想：**写入发生在 file_buffer，提交发生在“对象化（cacl）”**。

标准流程（逻辑）：

1. `create_file(path)`

* FsMetaDb：校验挂载互斥；获取写租约 lease（严格单写）。
* FileBuffer：分配 working buffer。

2. `append/flush`

* 只对 working buffer 生效。

3. `close_file`

* 结束写会话，进入“待提交/冷却”的状态（具体实现可采用分级提交，见下）。

4. `cacl_name(path)`

* 把 working buffer 对象化：切 chunk、计算 sha256、写入 internal store 或生成 external link
* 更新 `path: working -> committed(objid)`，释放 buffer

#### 推荐理解实现：分级提交（适配 Zero-Ops 与热点文件）

为避免频繁 close 触发昂贵的哈希/搬运，系统采用下面方法分级提交：

* Writing → Cooling（防抖）
* Cooling → Linked（计算 objid，用 ExternalLink 指向 buffer node 文件，快速可读）
* Linked → Finalized（冻结后搬运进 internal store，变冷数据）

这让：

* close 变得非常快
* 热点文件不会频繁“落冷存储”
* 归档搬运可在空闲时间进行

### 6.3 Pull / Materialization（对象可先绑定后拉取）

* `add_file/add_dir` 只做 path 绑定（O(1)），不代表本地可读。
* `pull(path)`/`pull_by_objid(objid)` 启动异步拉取；完成后 `stat` 可观测 material 状态。
* chunk 拉取默认整块下载，原子提交。

### 6.4 erase_obj_by_id：本地驱逐=变“未 pull”

`erase_obj_by_id(objid)` 的语义非常明确：

* 删除/释放本地已物化的对象体与相关 chunk（实现可删 body 或 body+chunk）。
* **保留 path->objid 绑定与对象元信息**。
* 后续访问返回 `NEED_PULL/NOT_PULLED`，可用 pull 重新拉取恢复。

这在“默认不自动删业务数据”的前提下，提供了**显式回收/空间治理**的手段。

---

## 7. 扩容与迁移（NDM 运维最友好的部分）

扩容的本质：**新增 StoreTarget + layout epoch++**

* 写：新数据按新 epoch 直接落到新 target（无需搬旧数据）。
* 读：找不到时最多回退 `epoch-1/epoch-2` 查旧位置。
* 迁移：可以读时触发 lazy migration（读修复），也可以后台低优先级 rebalance 扫描。

结果是：

* 扩容是 **O(1) 配置操作**，不需要立即重平衡风暴。
* 迁移成本被摊平到后续访问与后台时段。

---

## 8. 一致性/并发语义速查

### 8.1 Path 层（强一致域）

* FsMetaDb 事务保证 `add/move/copy/delete/lease/commit` 的一致性。
* 同 path 严格单写：写冲突直接失败，而不是阻塞等待（更适合 Zero-Ops 环境）。

### 8.2 数据层（最终一致 + 可观测）

* path 可能绑定到某 objid，但 obj 的数据可能未 pull。
* read 失败会给出可操作的错误（NeedPull），而不是假装成功。

### 8.3 常见错误码语义

* `PATH_BUSY`：path 当前 working/被写会话占用
* `LEASE_CONFLICT`：写租约/fence 冲突
* `NEED_PULL/NOT_PULLED`：元信息存在但本地未物化
* `LINK_INVALID_OR_CHANGED`：ExternalLink 对应外部文件发生变化

---

## 9. GC 与长期运行策略

* 默认不做“自动删除业务数据”的 GC（尤其海量场景）。
* 必须常开：软状态清理

  * `chunk.tmp`、失败下载残留
  * 过期 lease、崩溃遗留 working buffer
  * pull 中断中间态
* 业务数据清理由显式接口驱动（erase / 管理面），可结合审计权限。

---

## 10. 给新架构师的“最短理解路径”（建议按这个顺序读/想）

1. **Path 是可变指针，ObjId/ChunkId 是不可变内容**
2. 写入永远先进入 **FileBuffer 工作态**，提交是 **对象化 + 改指针**
3. Strict Single Writer 把并发写复杂度砍掉（用失败语义换简单与可预测）
4. Placement epoch backoff 让扩容变成 O(1) 配置，迁移可 lazy
5. ExternalLink 是“原地纳管”的关键，但必须接受它的失效语义（qcid 快失败）
6. 默认不自动删业务数据：空间治理靠显式 erase + 软状态 GC
7. 分布式一致性只把“必须强一致”的部分放在 FsMetaDb；数据层走最终一致 + 可观测

