

# NamedDataMgr (NDM) 架构设计说明 (v2.1 整合版)

**版本：** 2.1 (合并版)
**日期：** 2026-02-03
**范围：** 核心架构、元数据模型 (Overlay)、存储布局与写入生命周期。

---

## 0. 系统契约与设计哲学

### 0.1 内容寻址与强协议规范

* **身份标识：** 所有数据通过内容 ID (ObjId/ChunkId) 标识，通常使用 `sha256:xxxx`。
* **协议约束：** 对象的规范编码 (Canonical encoding)、类型和版本受到 **cyfs:// 协议规范** 的严格约束。
* **不可变性：** ObjId 一旦生成，其内容即不可变。任何修改都会生成新的 ObjId。

### 0.2 非 POSIX 语义

* NDM 提供“类文件系统 API”（目录、文件、路径），但 **不保证 POSIX 兼容性**。
* **一致性：** 原子性和并发性由本文档定义（例如 Overlay 规则、分级提交），而非操作系统标准。

### 0.3 严格单写（基于会话）

* **排他写入：** 一个文件路径（解析为 File Inode 后）在同一时刻只允许存在 **一个活跃的写会话**。
* **租约机制：** 写入者必须持有 Inode 的有效租约。并发写入尝试会立即失败并返回 `PATH_BUSY` 或 `LEASE_CONFLICT`。
* **无并发修改：** 不支持重叠写入或多用户并发追加。

### 0.4 Overlay 目录模型

目录是 **逻辑叠加（Logical Overlay）**，由两层组成：

1. **Base Layer (不可变层)：** 一个已提交的 `DirObjectId`（可以是远程对象、只读对象或快照）。
2. **Upper Layer (可变层)：** 本地 `fs-meta-db` 中存储的稀疏 `Dentries`（目录项集合）。

* **规则：** Upper Layer 的条目优先级高于 Base Layer。Upper Layer 中的删除操作定义为 **“墓碑” (Tombstones)**，用于隐藏 Base Layer 中的项。
* **只读挂载 (ReadOnly Mount)：** 目录可以被挂载为 `read_only`，此时禁止创建 Upper Layer（即禁止任何结构性变更）。

### 0.5 放置与扩展性

* **版本化布局：** 读取操作支持“时光倒流”查找（当前 Epoch -> 回退到 Epoch-1 -> Epoch-2），以支持无锁扩容和再平衡。
* **手动 GC：** 默认情况下，大规模部署 **不自动删除** 业务数据。GC 是显式的管理操作。
* **懒惰迁移 (Lazy Migration)：** 数据再平衡发生在读取时（读时修复）或通过后台任务进行，而非在拓扑变更时强制迁移。

---

## 1. 关键术语

* **Pool / NDM 实例：** 逻辑隔离边界（命名空间）。路径：`ndm://pool_id/ndm_id/path`。
* **Inode / FileID：** 内部不可变的 64 位整数，用于标识文件或目录节点。重命名文件只会改变路径，不会改变 FileID。
* **Dentry (目录项)：** 映射关系 `(ParentFileID, Name) -> Target`。
* **ObjId / ChunkId：** 内容寻址哈希 ID。
* **Store Target：** 物理存储单元（卷/目录）。
* **Placement Layout：** `ObjId -> [StoreTarget]` 的映射算法。
* **Tombstone (墓碑)：** 一种特殊的 Dentry 类型，表示该名称已从 Base Layer 中“删除”。

---

## 2. 组件架构

1. **NamedDataMgr (NDM)：** 中央协调器。实现“文件系统”逻辑、Overlay 合并，并编排文件的生命周期。
2. **FsMeta (服务/DB)：** 管理逻辑层。
* 存储 Inodes、Dentries、租约 (Leases) 和挂载状态 (Mount states)。
* 实现：SQLite（单机/边缘）或 分布式 KV/Raft（集群）。


3. **NamedStore：** 管理物理层。
* 存储 `NamedObject` 元数据和 `Chunks`（载荷）。
* 处理 **放置 (Placement)** 和 **链接解析 (Link Resolution)**（去重）。


4. **FileBuffer (服务/节点)：** 管理“热”数据。
* 处理活跃的写会话。
* 可以是本地 (mmap) 或分布式 (类 GFS 的 Buffer Nodes)。



---

## 3. 元数据模型：Inode 与 Overlay (FsMeta)

### 3.1 Schema 设计

元数据层针对 O(1) 重命名和稀疏 Overlay 进行了优化。

**表：Nodes (Inode)**

* `file_id` (PK): u64, 自增。
* `kind`: `File` | `Dir`.
* `base_obj_id`:
* 对于 Dir: Base Layer 的 `DirObjectId`。
* 对于 File: 已提交的 `FileObjectId`。


* `state`: `Committed`, `Working` (Leased), `Cooling`, `Linked` (见第 5 节)。
* `mount_mode`: `Overlay` | `ReadOnly`.
* `rev`: u64 (用于 OCC 的修订号)。

**表：Dentries (Upper Layer / 可变层)**

* `parent_file_id`: 外键指向 Nodes。
* `name`: 字符串 (文件名)。
* `target_type`:
* `FileId`: 指向一个可变的 Inode (用于 working/local 文件)。
* `ObjId`: 直接指向一个已提交的 Object (只读导入的优化)。
* `Tombstone`: 显式隐藏一个名称。


* `target_id`: 基于类型的实际 ID。
* *主键:* `(parent_file_id, name)`。

### 3.2 路径解析与 Overlay 逻辑

查找 `/parent/child` 的过程：

1. **查 Upper (Dentries):** 查询 `dentries` 表，条件 `parent = parent_id AND name = 'child'`。
* 如果是 **Tombstone**: 返回 `NOT_FOUND` (显式删除)。
* 如果是 **FileId**: 解析 Inode -> 返回句柄。
* 如果是 **ObjId**: 返回 Object 句柄 (只读)。


2. **查 Base (DirObject):** 如果 Upper 未命中，查询 `base_obj_id` (DirObject)。
* 如果 Base 为空/Null: 返回 `NOT_FOUND`。
* 如果 Base 未 Pull: 返回 `NEED_PULL` (元数据存在，但 Body 缺失)。
* 如果 Base 中存在: 返回 Object 句柄。



### 3.3 List 操作 (合并视图)

`list(path)` 返回一个合并后的迭代器：

* **源 A:** `DirObject.children` (Base)。
* **源 B:** `Dentries.select(parent_id)` (Upper)。
* **合并逻辑:**
* 遍历 A。如果名称在 B 中作为 Tombstone 存在，跳过。如果名称在 B 中作为新条目存在，跳过 A（使用 B）。
* 遍历 B。输出所有非 Tombstone 的条目。



---

## 4. 目录操作与分级提交

### 4.1 O(1) 重命名与移动

重命名目录（即使包含数百万子项）纯粹是元数据操作：

1. **插入:** 新 Dentry `(NewParent, NewName) -> TargetFileId`。
2. **删除:** 插入 Tombstone `(OldParent, OldName) -> Tombstone`。

* *注意:* 不需要递归处理子项。

### 4.2 对象化 (`cacl_dir`)

将可变目录 (Inode + Dentries) 转换为不可变 `DirObject` (用于快照/导出)：

* **挑战:** 避免长时间锁定大目录。
* **策略: 乐观并发控制 (OCC)**
1. **快照:** 读取 `dir.rev` (v1)、`base_obj_id` 和所有 `dentries`。
2. **计算:** 在内存中合并 Base + Upper，生成新 `DirObject`，计算 Hash。(耗时部分)。
3. **提交:**
* 锁定目录。
* 检查 `dir.rev == v1`。
* 如果匹配: 更新 `base_obj_id = NewHash`，清空 `dentries`，`rev++`。
* 如果不匹配: 失败并重试。





---

## 5. 文件生命周期："分级提交" 写入模型

NDM 采用了受 GFS/HDFS 启发的“冷/热”分离逻辑，针对缓冲可能在本地或分布式的“零运维”环境进行了优化。

### 5.1 状态

1. **Working (热):** 活跃写租约。数据在 `FileBuffer` 中（本地 RAM/Disk 或远程 BufferNode）。
2. **Cooling (冷却):** 会话关闭。等待“去抖动”期（如 10秒）。暂不处理。
3. **Linked (温):**
* 计算出 Hash。
* 在 NamedStore 创建 **External Link**: `ObjId -> ExternalFile(BufferPath)`。
* 文件通过 `ObjId` 全局可读，但物理上仍驻留在 Buffer 区域。


4. **Finalized (冷):**
* 数据从 Buffer 迁移到 **Internal Store** (存储目标)。
* Store Link 从 External 升级为 Internal。
* 清理 Buffer。



### 5.2 "严格单写" 流程

1. **create_file(path):**
* 解析 Inode。检查 `MountMode != ReadOnly`。
* 如果 Inode 为 `Working`: 返回 `PATH_BUSY`。
* 分配 **FileBuffer** (基于策略选择本地或远程)。
* 设置状态: `Working`。返回句柄。


2. **write/append:** 写入 FileBuffer。
3. **close_file:**
* 设置状态: `Cooling`。释放租约。
* *用户立即看到成功。*


4. **后台守护进程 (Background Daemon):**
* 将 `Cooling` 晋升为 `Linked` (计算 Hash, 注册 Link)。
* 将 `Linked` 晋升为 `Finalized` (复制数据, 更新 Store, 清理 Buffer)。



---

## 6. 存储层：CAS 与 Links (NamedStore)

### 6.1 Link 类型

`NamedStore` 通过 Link 抽象物理位置，支持去重和外部引用。

1. **Internal:** Chunk 完全驻留在受管的 Store Target 中。
2. **SameAs(ObjId):** 另一个对象的别名。
3. **ExternalFile(Path, Range, QCID):**
* 数据驻留在非受管的本地文件（或 FileBuffer）中。
* **QCID (快速检查 ID):** 完整性的关键。NDM 在读取前会根据 QCID 检查文件大小/mtime/inode。如果发生变化，链接视为 `INVALID`。



### 6.2 放置与回退 (支持扩容)

* **写:** 始终使用 `Layout.Epoch(Current)`。
* **读:**
1. 使用 `Epoch(Current)` 计算 Target。
2. 如果缺失，使用 `Epoch(Current - 1)` 计算。
3. 如果缺失，使用 `Epoch(Current - 2)` 计算。


* **优势:** 添加节点是 O(1) 操作（更新 Epoch）。数据通过懒惰迁移（读时修复）或后台任务移动，保持可用性。

---

## 7. 数据可用性与管理

### 7.1 Pull / 物化 (Materialization)

* 将路径绑定到 ObjId（通过 `add_file`）仅是元数据操作（轻量级）。
* **Pull:** 显式将 Body/Chunks 拉取到本地存储。
* **访问:**
* `stat(path)`: 返回元数据。
* `open_reader(path)`: 如果 Body 不存在，返回 `NEED_PULL`。



### 7.2 物理驱逐 (`erase_obj_by_id`)

* 从 Store Target 中删除物理 Chunk。
* **不会** 删除 Inode 或 路径绑定。
* 效果: 对象回到“未拉取”状态。`open_reader` 返回 `NEED_PULL`。

### 7.3 垃圾回收 (GC)

* **软状态 (Soft State):** (临时 Chunk, Cooling buffers) -> 积极的自动 GC。
* **业务数据:** 默认不进行自动 GC。删除需要显式的 API 调用 (`delete path` + `erase obj`)。

---

## 8. 语义总结 (API 契约)

| 操作 | POSIX | NDM (NamedDataMgr) |
| --- | --- | --- |
| **Path** | 可变身份 | 不可变 Inode/ObjId 的可变别名 |
| **Write** | 并发/Range | **严格单写** (整文件或仅追加会话) |
| **一致性** | 立即 | **分级** (Working -> Linked -> Finalized)。保证“读己之写”。 |
| **Rename Dir** | O(N) 或 O(1) | **O(1)** (Overlay Dentry Tombstone + Insert) |
| **扩容** | 各异 | **懒惰再平衡** (读取回退 + 后台迁移) |
| **完整性** | 仅元数据 | **内容寻址** (SHA256) |

---

## 9. “零运维”/家庭使用实现说明

* **本地优先写入:** 如果设备有磁盘，`FileBuffer` 应映射到本地目录以获得最大速度。
* **池化:** 使用 `NamedStore` 将不同的驱动器（USB、NAS、本地）聚合成逻辑池，而无需 RAID。
* **可靠性:** 依赖 `Linked` -> `Finalized` 的晋升过程，异步执行到稳定节点（NAS/Cloud）的复制/备份。