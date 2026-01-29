# fs_meta_db 模块技术实现文档

本文档描述 fs_meta_db（path 元数据管理）模块的职责、数据模型与实现要点。内容基于 `doc/named_mgr.md`，并明确区分 fs_meta_db 与其他组件接口边界。

## 1. 范围与职责

fs_meta_db 是 NamedMgr 内部的“path 元数据管理层”，负责维护 `path -> ObjId` 绑定、写会话记录与对象引用计数。

核心职责：

1. 维护 namespace 内的 path 绑定（文件/目录）。
2. 强一致的 path 事务：绑定、解绑、重命名、复制、列表与查询。
3. 简化的 path 状态管理与写会话租约。
4. 严格单写租约（lease）与会话围栏（fence）。
5. 挂载互斥（mount exclusivity）校验。
6. 维护对象引用计数并提供 0 引用对象的清理能力（GC）。

非职责（不在 fs_meta_db 内实现）：

- chunk/object 实体读写、校验、link 解析。
- file_buffer 的 IO 与 append/flush。
- pull 任务的网络传输与写入流程。
- open_reader_by_id / get_object 等绕过 path 的对象读取。

## 2. 与其他组件的边界

fs_meta_db 与以下模块协作：

- **file_buffer（buffer service）**：实际写入工作态数据；fs_meta_db 仅记录 working/lease 与 buffer 元信息。
- **NamedDataMgr / named_store**：对象与 chunk 的 CAS 存储；fs_meta_db 仅保存 ObjId 与物化状态。
- **store_layout（placement）**：决定 ObjId/ChunkId 落盘位置；不感知 path。
- **ndn_client / ndn_router**：负责 pull/push 与跨设备传输；fs_meta_db 只记录状态。

## 3. 状态模型

### 3.1 Path 状态（简化）

每个 path 绑定条目具有状态：

- **bound**：path 指向一个已确定的 ObjId（FileObject 或 DirObject）。
- **writing**：path 绑定为工作态 file_buffer（存在写会话）。

writing 状态内使用一个简单标记区分写入是否已结束：

- `write_open = true`：可写。
- `write_open = false`：已封口，等待对象化提交。

状态转换（由上层流程驱动，fs_meta_db 仅保证原子性）：

- reserve_write: none -> writing (write_open=true)
- seal_write: writing (write_open=true) -> writing (write_open=false)
- commit_binding: writing (write_open=false) -> bound
- unbind_path: bound/writing -> removed

### 3.2 单写租约（Lease）

对每个 working path 维护租约字段：

- session_id：写会话标识
- fence：递增围栏（防止旧会话回写）
- lease_expire_at：到期时间

所有写相关操作必须校验 lease 与 fence；冲突返回 `PATH_BUSY` / `LEASE_CONFLICT`。

## 4. 数据模型（SQLite 示例）

> 仅为实现建议，字段可根据实际需要精简或扩展。

### 4.1 paths 表

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| path | TEXT PRIMARY KEY | 规范化路径（绝对路径） |
| obj_id | TEXT | 绑定的 ObjId（bound 状态） |
| type | INTEGER | 0=file, 1=dir |
| state | INTEGER | bound/writing |
| write_open | INTEGER | 1=可写, 0=已封口（仅 writing） |
| mtime | INTEGER | 更新时间（可选） |
| size | INTEGER | 文件大小（可选，committed 后更新） |

### 4.2 writing 表

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| path | TEXT PRIMARY KEY | 对应 paths.path |
| session_id | TEXT | 写会话 |
| fence | INTEGER | 围栏版本 |
| lease_expire_at | INTEGER | 租约过期时间 |
| buffer_id | TEXT | file_buffer 句柄（或路径） |
| expect_size | INTEGER | 预期大小（可选） |

### 4.3 path_meta（可选）

存储额外元数据（权限、标签、扩展属性等）。

### 4.4 obj_refcount 表（GC）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| obj_id | TEXT PRIMARY KEY | ObjId |
| ref_count | INTEGER | 有效引用计数（direct/indirect） |
| last_update | INTEGER | 更新时间 |

### 4.5 path_refset 表（强一致必需）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| path | TEXT | path |
| obj_id | TEXT | 该 path 直接/间接引用的 ObjId |
| kind | INTEGER | 0=direct, 1=indirect |

> 为保证引用计数强一致，path_refset 作为事务内的“事实记录”必须存在，避免仅靠后台重算导致窗口不一致。
> fs_meta_db 在事务内通过对象库快照计算“可达闭包集合”并写入本表，不依赖上层传入集合。

## 5. 关键约束与校验

### 5.1 挂载互斥（Mount Exclusivity）

当 path 绑定为 DirObject 时，其子树必须由 DirObject 决定：

- 若已有子路径显式绑定（path 表中存在 `path LIKE parent/%`），禁止将 parent 绑定为会展开冲突子树的 DirObject。
- 反之，若 parent 已绑定为 DirObject，则禁止在其子路径上创建显式绑定。

该校验由 fs_meta_db 在 bind_dir/create_dir/rename_path/copy_path 事务中完成。

### 5.2 Path 规范化

- 统一使用绝对路径，去除重复分隔符与尾部 `/`。
- 空路径非法；禁止 `..` 逃逸。

### 5.3 事务边界

所有 path 绑定更新必须在单事务中完成：

- add/delete/move/copy
- create_file/create_dir（含 lease 记录）
- close_file/cacl_name 的状态变更

## 6. fs_meta_db 对外接口（仅本模块）

以下接口由 fs_meta_db 提供或直接依赖其原子事务。它们只处理 path 元数据，不触达对象库或 chunk 实体。

### 6.1 Namespace 绑定操作

- `bind_file(path, obj_id)`：绑定已构造 FileObject；校验挂载互斥并更新引用计数。
- `bind_dir(path, dir_obj_id)`：绑定 DirObject；校验挂载互斥并更新引用计数。
- `unbind_path(path)`：删除 path 绑定，不触发对象物理删除；更新引用计数。
- `rename_path(old_path, new_path)`：仅变更 path 绑定关系，O(1)，引用计数不变。
- `copy_path(src, target)`：复制绑定关系并增加引用计数。
- `list_children(path, pos, page_size)`：列出目录下子项。

### 6.2 状态与查询

- `get_path_status(path) -> PathStatus`：返回状态与 ObjId 等。

### 6.3 写会话控制（仅元数据）

- `create_dir(path)`：创建空目录元数据（立即 bound）。
- `reserve_write(path, expect_size) -> FileBufferMeta`：创建 writing 记录与 lease。
- `seal_write(path, session_id, fence)`：写入封口，禁止继续写入。
- `commit_binding(path, obj_id, session_id, fence)`：提交 ObjId 绑定并更新引用计数。

> 说明：append/flush/calc chunklist 等不属于 fs_meta_db；这里只记录状态变更与 lease。

### 6.4 引用计数与 GC

- `update_refcount_on_bind(path, new_obj_id)`：事务内记录可达集合并增加计数。
- `update_refcount_on_unbind(path, old_obj_id)`：事务内删除可达集合并减少计数。
- `gc_list_zero_refs(limit) -> [ObjId]`：列出 0 引用对象。
- `gc_sweep_zero_refs(limit)`：清理 0 引用对象（调用对象存储层删除实体）。

## 7. 非 fs_meta_db 接口（协作组件）

以下接口列在 `doc/named_mgr.md` 的“fs-meta-db 主要接口”章节中，但实际属于其他模块：

- **对象读写/访问**：`open_reader`、`open_reader_by_id`、`get_object`、`close_reader`（NamedMgr + NamedDataMgr）。
- **pull 相关**：`pull`、`pull_by_objid`（ndn_client/NamedMgr 调度）。
- **file_buffer IO**：`append`、`flush`（buffer service）。
- **对象化计算**：`cacl_name`（NamedMgr + NamedDataMgr + store_layout）。
- **物理删除/驱逐**：`erase_obj_by_id`（对象存储层）；fs_meta_db 的 GC 仅负责选择要删除的 ObjId。
- **snapshot**：高层语义（NamedMgr 组合操作）。

fs_meta_db 只需为这些流程提供事务性 path 状态与可观测字段。

## 8. 典型流程中的 fs_meta_db 行为

### 8.1 bind_file/bind_dir

1. 校验 path 规范化与挂载互斥。
2. 写入 paths 表，state=bound。
3. 计算对象可达集合并更新引用计数。

### 8.2 reserve_write -> seal_write -> commit_binding

1. `reserve_write` 写入 paths(state=writing, write_open=true) 与 writing 记录。
2. `seal_write` 标记 write_open=false，lease 进入只读/短租。
3. `commit_binding` 更新 paths 为 bound 并写入 obj_id，清理 writing 记录。
4. 计算对象可达集合并更新引用计数。

### 8.3 rename/copy

在同事务内更新 paths 的绑定关系，并执行挂载互斥校验。
copy 需要增加引用计数；rename 不变。

### 8.4 stat

返回：

- path 是否存在
- path state（bound/writing）
- obj_id（若 committed）
- lease 信息（如需要对外暴露）

## 9. 对象引用计数与 GC

### 9.1 有效引用的定义

当对象被直接或间接挂载到一个 path 上，即视为“有效引用”。

- **直接引用**：path 绑定的 ObjId。
- **间接引用**：DirObject 内部展开所引用的子对象（DirObject/FileObject 等）。

fs_meta_db 维护 ObjId 的引用计数；计数为 0 的对象可被 GC 清理。

### 9.2 引用计数更新策略（强一致）

当绑定变更时，fs_meta_db 必须在同一数据库事务内完成可达集合与计数更新：

- bind/commit（覆盖式绑定）：若 path 已存在旧绑定，先对旧 ObjId 的可达集合 -1，再对新 ObjId 的可达集合 +1。
- unbind：对旧 ObjId 的可达集合 -1。
- copy：对目标 path 的可达集合 +1。
- rename：引用集合不变。

对象可达集合由 fs_meta_db 通过对象库快照解析（NamedDataMgr 提供遍历能力），并在事务内写入 path_refset 与 obj_refcount。

强一致事务步骤（示例：bind/commit）：

1. 事务内读取现有 path_refset(path)，得到 `S_old`（若存在）。
2. 读取对象图快照（基于 ObjId 版本，稳定可重放）。
3. 计算可达集合 `S_new`（含 direct 与 indirect）。
4. 事务内以 `S_new` 替换 path_refset(path)。
5. 事务内对 `S_old` 中每个 obj_id 执行 `ref_count -= 1`。
6. 事务内对 `S_new` 中每个 obj_id 执行 `ref_count += 1`。
7. 提交事务。

解绑事务步骤（示例：unbind）：

1. 事务内读取 path_refset(path) 得到 `S_old`。
2. 事务内删除 path_refset(path) 记录。
3. 事务内对 `S_old` 中每个 obj_id 执行 `ref_count -= 1`。
4. 提交事务。

copy/rename 规则：

- copy：等同新增绑定，使用 `S_new` 增加计数。
- rename：不改变引用集合，仅更新 path 键值。

### 9.3 GC 扫描与清理

GC 仅处理 ref_count=0 的对象：

- `gc_list_zero_refs`：分页列出 0 引用对象。
- `gc_sweep_zero_refs`：调用对象存储层删除（或驱逐）对象实体。

fs_meta_db 仅决定可删对象集合，实际删除由对象存储层执行。

一致性约束：

- GC 只允许删除 ref_count=0 且不在任何 path_refset 中出现的 ObjId。
- 若对象库遍历失败或快照不可用，必须拒绝更新 refcount，保证计数不被破坏。

## 10. 错误码建议

- `PATH_NOT_FOUND`：path 不存在。
- `PATH_BUSY`：存在 working 写会话。
- `LEASE_CONFLICT`：session/fence 校验失败。
- `MOUNT_CONFLICT`：挂载互斥冲突。
- `INVALID_PATH`：非法 path。

## 11. 并发与一致性

1. 所有写事务必须串行化或使用数据库事务隔离级别保证可重复读。
2. 单写严格：只允许一个写会话持有 lease。
3. 读 committed 不阻塞写，但不得读取 working（除同 session 特例，由上层决定）。

## 12. 兼容与演进

- 当前实现以单机 SQLite 为主；未来可切换为 klog + Raft 以扩展一致性域。
- 事务语义必须保持不变，确保上层 NamedMgr 行为稳定。
