# fs_meta_db 技术实现文档

本文档基于 `doc/named_mgr.md` 的约束，定义 fs_meta_db 模块在 NamedMgr 中的实现细节与接口语义。fs_meta_db 只负责 path 元数据与一致性，不负责对象或 chunk 的实体存储。

## 1. 模块定位

- 维护命名空间内 `path -> ObjId` 的强一致绑定
- 实现严格单写（single writer）与工作态/提交态状态机
- 提供挂载互斥、路径事务与状态查询
- 不承担对象内容与 chunk 的存储（由 named_store / placement / file_buffer 负责）

## 2. 语义约束（与 NamedMgr 对齐）

- Path 语义非 POSIX，一致性规则以本文档为准
- 严格单写：同一 path 同时只允许一个写会话持有租约
- 读默认只允许 committed；working 除同 session 外拒绝
- 路径操作不触发业务数据删除；仅显式 `erase_obj_by_id` 才会本地驱逐
- 挂载互斥：path 子树的显式绑定与 DirObject 展开互斥

## 3. 数据模型

### 3.1 核心概念

- PathEntry：路径元数据记录
- WorkingEntry：工作态写入记录（file_buffer 绑定）
- LeaseEntry：写租约记录（session_id + fence + ttl）
- ObjStat/PathStat：状态查询输出

### 3.2 状态机

```
empty -> working(writing) -> working(closed) -> committed
```

- empty：路径无绑定
- working(writing)：处于写会话，持有租约
- working(closed)：写入结束，等待对象化与提交
- committed：path 指向 ObjId 的稳定版本

## 4. SQLite 表结构建议

> 表名/字段可按工程约定调整，语义需保持一致。

```sql
CREATE TABLE path_entry (
  path TEXT PRIMARY KEY,      -- 规范化 path 字符串
  obj_id TEXT,                -- committed ObjId
  obj_type INTEGER,           -- 1=file,2=dir
  state INTEGER NOT NULL,     -- 0=empty,1=working,2=committed
  mtime INTEGER NOT NULL,     -- 最近修改时间（毫秒时间戳）
  ctime INTEGER NOT NULL,     -- 创建时间（毫秒时间戳）
  version INTEGER NOT NULL,   -- 变更版本，用于并发检测
  meta BLOB                   -- 扩展元数据（可选）
);

CREATE TABLE working_entry (
  path TEXT PRIMARY KEY,      -- 绑定的 path
  session_id TEXT NOT NULL,   -- 写会话标识
  fence INTEGER NOT NULL,     -- 会话栅栏（递增）
  lease_ttl INTEGER NOT NULL, -- 租约 TTL（毫秒/秒，约定制）
  buffer_id TEXT NOT NULL,    -- file_buffer 句柄或标识
  expect_size INTEGER,        -- 期望大小（可选）
  state INTEGER NOT NULL,     -- 0=writing,1=closed
  create_time INTEGER NOT NULL, -- 创建时间（毫秒时间戳）
  update_time INTEGER NOT NULL  -- 更新时间（毫秒时间戳）
);

CREATE TABLE lease_entry (
  path TEXT PRIMARY KEY,      -- 绑定的 path
  session_id TEXT NOT NULL,   -- 写会话标识
  fence INTEGER NOT NULL,     -- 会话栅栏（递增）
  lease_ttl INTEGER NOT NULL, -- 租约 TTL（毫秒/秒，约定制）
  update_time INTEGER NOT NULL -- 更新时间（毫秒时间戳）
);

CREATE TABLE path_mount_index (
  parent_path TEXT NOT NULL,  -- 作为挂载点的父路径
  child_prefix TEXT NOT NULL, -- 子路径前缀（用于互斥检测）
  PRIMARY KEY (parent_path, child_prefix)
);
```

推荐索引：

- `path_entry(state)` 用于扫描 working/committed
- `working_entry(session_id)` 用于会话清理

## 5. 事务与一致性

### 5.1 事务边界

以下操作必须在单事务内完成：

- `add_file/add_dir`
- `create_file/create_dir`
- `close_file/cacl_name`
- `delete/move_path/copy_path/snapshot`

### 5.2 租约与乐观并发

- 写操作校验 `session_id + fence + lease_ttl`
- 租约过期后视为无效，写请求返回 `LEASE_CONFLICT`
- 读操作不持有租约，仅检查状态

### 5.3 崩溃恢复

- 启动时扫描 `working_entry/lease_entry`
- 过期租约直接释放，working 标记为 aborted 或保持 PATH_BUSY
- 清理失败写入的 buffer 由 file_buffer 负责

## 6. 挂载互斥

为避免 path 子树与 DirObject 展开冲突：

- 若 `path` 下已有显式子路径绑定，禁止将 `path` 绑定为 DirObject
- 若 `path` 已绑定为 DirObject，禁止新增 `path/child` 显式绑定

实现建议：

- 使用 `path_mount_index` 维护前缀关系
- 所有绑定类事务中先校验互斥，再写入

## 7. API 实现语义

### 7.1 绑定与查询

- `add_file(path, obj_id)`
  - 校验挂载互斥
  - 写入 `path_entry.state=committed`
  - 若 path 正在 working 返回 `PATH_BUSY`

- `add_dir(path, dir_obj_id)`
  - 同 `add_file`，`obj_type=dir`

- `stat(path)` / `stat_by_objid(objid)`
  - 返回 committed/working/materialization 状态
  - 若对象未 pull，返回 `NEED_PULL/NOT_PULLED`

### 7.2 写流程

- `create_file(path, expect_size)`
  - 校验 path 不处于 working
  - 生成 `buffer_id` 并写入 `working_entry`
  - `path_entry.state=working`

- `append(path, data)`
  - fs_meta_db 校验租约与状态
  - 数据写入由 file_buffer 执行

- `close_file(fb)`
  - `working_entry.state=closed`
  - 缩短或释放租约

- `cacl_name(path)`
  - 前置条件：working closed
  - 上层对象化生成 fileobject/chunklist
  - fs_meta_db 原子提交 `path_entry.obj_id` 并切换到 committed
  - 清理 `working_entry`

### 7.3 读流程

- `open_reader(path)`
  - committed：允许并发读
  - working：默认返回 `PATH_BUSY`（可选同 session 例外）

- `open_reader_by_id(objid)`
  - 绕过 path，fs_meta_db 仅参与 stat/物化状态

### 7.4 删除与复制

- `delete(path)`：仅移除 path 绑定
- `move_path(old, new)`：原子变更 path 指针，不搬迁内容
- `copy_path(src, dst)`：复制 committed 绑定
- `snapshot(src, dst)`：等同 copy committed；src working 则 `PATH_BUSY`

### 7.5 物理删除（本地驱逐）

- `erase_obj_by_id(objid)`
  - 删除本地物化数据
  - 保留 path 绑定与对象元信息
  - 后续访问返回 `NEED_PULL/NOT_PULLED`

## 8. 状态与错误码建议

- PATH_BUSY：working 占用或写冲突
- LEASE_CONFLICT：租约冲突或过期
- NEED_PULL/NOT_PULLED：对象未物化
- NOT_FOUND：path 不存在
- MOUNT_CONFLICT：挂载互斥冲突
- INVALID_STATE：非法状态转移

## 9. GC 与软状态清理

fs_meta_db 仅负责软状态：

- 过期 lease
- 崩溃遗留的 working 标记
- pull 中间态标记

业务数据删除仅通过 `erase_obj_by_id` 显式触发。

## 10. 与其他模块的协作

- file_buffer：保存工作态数据，fs_meta_db 仅持有 buffer_id 与租约
- named_store：对象与 chunk 实体存储，fs_meta_db 只记录 obj_id
- placement：目标选择，不在 fs_meta_db 中体现
- ndn_client/router：拉取/发布，不改变 fs_meta_db 的一致性规则

## 11. 实现步骤建议

1. SQLite schema 与基础 CRUD
2. 租约校验与状态机转移
3. 挂载互斥检测
4. 崩溃恢复与过期清理
5. 与 NamedMgr 的集成测试
