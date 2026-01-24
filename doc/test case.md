# NamedMgr Test Cases (Planned)

本文档基于 `doc/named_mgr.md` 的设计说明，重新规划 NamedMgr 相关测试用例，覆盖路径元数据、读写语义、对象/Chunk 管理、Link 语义、placement 回退、GC/erase 等核心行为。重点补齐可执行细节、可观测点与边界/异常场景。

## 1. 测试范围

- fs-meta-db 的路径元数据管理与读写接口
- NamedDataMgr / named_store 的对象与 chunk 存储语义
- NamedMgr 的单写、多读与 commit/working 语义
- Link 模型（SameAs / PartOf / ExternalFile）与 qcid 检测
- placement layout 版本回退
- erase 与 GC 软清理语义
- path 与 inner_path 解析与挂载互斥

## 2. 测试环境与前置

- 单机 SQLite 版 fs-meta-db
- 至少 2 个 store target（用于 placement 回退场景）
- 可模拟的外部文件路径（ExternalFile）
- 可控的 qcid 计算/校验接口
- 具备多 session 并发能力（用于单写冲突测试）
- 可控制 placement target 可用性（mock 或配置开关）
- 可观测接口：`stat`/`stat_by_objid`、chunk state、lease 信息

## 3. 术语与约定

- committed: 已提交对象版本，可并发读
- working: 写入中的 file_buffer 状态
- PATH_BUSY / LEASE_CONFLICT: 单写冲突错误
- NEED_PULL / NOT_PULLED: 对象未 materialize
- NOT_AVAILABLE: 数据缺失或无法读取（link invalid/placement miss 等）
- LINK_INVALID_OR_CHANGED: ExternalFile 快速校验失败

## 4. 用例模板（统一格式）

每条用例尽量补齐以下字段，确保可落地和可断言：

- 优先级: P0/P1/P2
- 前置: 初始对象/路径/环境状态
- 输入: API 参数（path/objid/offset/range 等）
- 步骤: 调用序列
- 观察点: `stat`/`stat_by_objid` 字段、lease/info、chunk state
- 期望: 返回值/错误码 + 状态 + 副作用

## 5. 观察点清单（建议）

- `stat(path)`：committed/working/materialized、working_closed、objid、lease_id/ttl
- `stat_by_objid(objid)`：materialized 状态、pull 信息
- chunk state：ABSENT/WRITING/COMPLETE
- placement rollback：记录使用 epoch、miss 次数（如有）

## 6. 测试用例

### A. 路径元数据与命名空间

**TC-A01 路径绑定到 FileObject**

- 优先级: P0
- 前置: 生成 FileObjectId
- 输入: `add_file(path, objid)`
- 步骤: 调用 `add_file` 后 `stat(path)`
- 观察点: `stat(path)` 的 objid/committed/materialized
- 期望: path -> objid 绑定成功；`stat(path)` 显示绑定对象元信息；未 pull 则 `stat` 标记未 materialize

**TC-A02 路径绑定到 DirObject**

- 优先级: P0
- 前置: 生成 DirObjectId
- 输入: `add_dir(path, dir_objid)`
- 步骤: `add_dir` 后 `list(path, ...)`
- 观察点: `list` 返回子项；`stat(path)` objid/type
- 期望: 绑定成功；`list` 返回 DirObject 子项

**TC-A03 move 仅变更路径指针**

- 优先级: P1
- 前置: path1 已绑定 objid
- 输入: `move_path(path1, path2)`
- 期望: path1 解绑；path2 绑定同一 objid；对象内容未复制

**TC-A04 copy 复制路径指针**

- 优先级: P1
- 前置: path1 已绑定 objid
- 输入: `copy_path(path1, path2)`
- 期望: path1 与 path2 同时指向同一 objid

**TC-A05 delete 不触发业务数据删除**

- 优先级: P0
- 前置: path 已绑定 objid 并已 pull
- 输入: `delete(path)`
- 观察点: `stat_by_objid` materialized；`open_reader_by_id`
- 期望: path 解绑；对象与 chunk 仍可通过 objid 访问

**TC-A06 move 到已存在目标路径**

- 优先级: P1
- 前置: path1/path2 均已绑定
- 输入: `move_path(path1, path2)`
- 期望: 返回错误；原绑定不变

**TC-A07 非法路径/根路径**

- 优先级: P1
- 前置: 非法路径或 `/`
- 输入: `add_file`/`add_dir`
- 期望: 返回参数错误；无绑定产生

**TC-A08 list 分页边界**

- 优先级: P1
- 前置: path 绑定 DirObject，子项数 > page_size
- 输入: `list(path, pos, page_size)`
- 步骤: pos=0/page_size=n，再 pos=n
- 期望: 分页不重叠/不遗漏；越界返回空

### B. 单写与工作态

**TC-B01 单写租约冲突**

- 优先级: P0
- 前置: session1 对 path `create_file`
- 输入: session2 对同一 path `create_file`
- 期望: session2 返回 PATH_BUSY/LEASE_CONFLICT；`stat(path)` 显示 working

**TC-B02 working 状态读拒绝**

- 优先级: P0
- 前置: session1 进入 working（create_file）
- 输入: session2 对该 path `open_reader`
- 期望: 返回 PATH_BUSY

**TC-B03 working 同 session 特例读取**

- 优先级: P1
- 前置: session1 working
- 输入: session1 `open_reader`
- 期望: 若支持特例则可读；否则明确返回 PATH_BUSY

**TC-B04 close_file 后写入禁止**

- 优先级: P0
- 前置: create_file 后 append 数据
- 输入: `close_file(fb)` 后继续 append
- 期望: 写入被拒绝；`stat` 显示 working-closed

**TC-B05 cacl_name 提交成功**

- 优先级: P0
- 前置: working-closed 状态
- 输入: `cacl_name(path)`
- 期望: path 进入 committed；`open_reader(path)` 可读

**TC-B06 working 未关闭时 cacl_name 失败**

- 优先级: P0
- 前置: working 未 close
- 输入: `cacl_name(path)`
- 期望: 返回错误；不产生 committed 版本

**TC-B07 close_file 后 open_reader 行为**

- 优先级: P1
- 前置: working-closed，但未 cacl_name
- 输入: `open_reader(path)`
- 期望: 返回 PATH_BUSY 或明确错误码；不得读到半成品数据

**TC-B08 lease 过期/会话崩溃清理**

- 优先级: P0
- 前置: session1 create_file 后模拟崩溃/ttl 过期
- 输入: session2 `create_file` / `open_reader`
- 期望: 单写租约被释放；working 软状态被清理

### C. 读取语义与 pull/materialization

**TC-C01 committed 可并发读**

- 优先级: P0
- 前置: path committed
- 输入: 多 session 并发 `open_reader`
- 期望: 读成功且一致

**TC-C02 未 pull 返回 NEED_PULL**

- 优先级: P0
- 前置: path 绑定 objid，但对象未 materialize
- 输入: `open_reader(path)`
- 期望: 返回 NEED_PULL/NOT_AVAILABLE

**TC-C03 pull 后可读**

- 优先级: P0
- 前置: 同 TC-C02
- 输入: `pull(path, remote)` -> 完成后 `open_reader`
- 期望: read 成功；`stat` 显示 materialized

**TC-C04 open_reader_by_id 绕过 path**

- 优先级: P1
- 前置: objid 存在
- 输入: `open_reader_by_id(objid)`
- 期望: 可直接读取对象内容

**TC-C05 pull_by_objid 与 path 一致性**

- 优先级: P1
- 前置: path->objid 绑定，未 materialize
- 输入: `pull_by_objid(objid)` 后 `open_reader(path)`
- 期望: read 成功；状态一致

**TC-C06 open_reader_by_id 未 materialize**

- 优先级: P1
- 前置: objid 存在但未 materialize
- 输入: `open_reader_by_id(objid)`
- 期望: 返回 NEED_PULL/NOT_PULLED

### D. chunk 写入与一致性

**TC-D01 chunk 原子写入**

- 优先级: P0
- 前置: 准备 chunk 数据
- 输入: 写入 `chunk.tmp` -> 校验 sha256 -> rename -> COMPLETE
- 期望: 中途失败不会出现 incomplete 可读数据

**TC-D02 写入校验失败**

- 优先级: P1
- 前置: 模拟 sha256 不匹配
- 输入: 执行写入流程
- 期望: 写入失败，chunk 不进入 COMPLETE

**TC-D03 range 越界读取**

- 优先级: P1
- 前置: 已 COMPLETE chunk
- 输入: `GetChunkReader(chunk, offset/range)` 越界
- 期望: 返回范围错误，不读取数据

### E. Link 语义

**TC-E01 SameAs 链路读取**

- 优先级: P1
- 前置: ChunkA SameAs ChunkB，ChunkB 已 COMPLETE
- 输入: `GetChunkReader(ChunkA)`
- 期望: 读取 ChunkB 内容

**TC-E02 PartOf 范围读取**

- 优先级: P1
- 前置: ChunkA PartOf ChunkB，范围合法
- 输入: `GetChunkReader(ChunkA, offset)`
- 期望: 返回 ChunkB 对应 slice

**TC-E03 ExternalFile qcid 正常**

- 优先级: P0
- 前置: ExternalFile link + 正确 qcid
- 输入: `GetChunkReader(chunk)`
- 期望: 读取外部文件 range 成功

**TC-E04 ExternalFile qcid 变化**

- 优先级: P0
- 前置: ExternalFile link，外部文件已变更
- 输入: `GetChunkReader(chunk)`
- 期望: 返回 LINK_INVALID_OR_CHANGED

**TC-E05 Writer 忽略 link**

- 优先级: P1
- 前置: chunk 存在 link
- 输入: `GetChunkWriter(chunkid)`
- 期望: 只允许内部存储写入，不基于 link 写外部数据

**TC-E06 Link 环检测**

- 优先级: P1
- 前置: SameAs 或 PartOf 形成环
- 输入: `GetChunkReader`
- 期望: 返回错误，避免无限递归

**TC-E07 ExternalFile 路径无效**

- 优先级: P1
- 前置: ExternalFile link 指向不存在文件
- 输入: `GetChunkReader`
- 期望: 返回 NOT_AVAILABLE 或明确错误码

**TC-E08 qcid 仅用于 ExternalFile**

- 优先级: P2
- 前置: SameAs/PartOf link
- 输入: `GetChunkReader`
- 期望: 不做 qcid 校验，读取正常

### F. path 与 inner_path

**TC-F01 inner_path 解析成功**

- 优先级: P0
- 前置: `/movies/dir1` 绑定 DirObject
- 输入: `open_reader("/movies/dir1/actions/MI6/content/4")`
- 期望: 逐层解析 inner_path 并读取目标 chunk

**TC-F02 挂载互斥**

- 优先级: P0
- 前置: `/movies/dir1/actions/Gongfu` 已绑定 fileobject
- 输入: 尝试将 `/movies/dir1` 绑定到包含 `actions/*` 的 DirObject
- 期望: 绑定失败（挂载点互斥）

**TC-F03 inner_path 中段类型冲突**

- 优先级: P1
- 前置: inner_path 中段指向非 DirObject 字段
- 输入: `open_reader(path/inner_path)`
- 期望: 返回路径解析错误

**TC-F04 inner_path 越界**

- 优先级: P1
- 前置: chunklist 长度有限
- 输入: `open_reader(path/.../content/out_of_range)`
- 期望: 返回范围错误

**TC-F05 子 path 与 DirObject 冲突（反向）**

- 优先级: P1
- 前置: `/movies/dir1` 绑定 DirObject，且包含 `actions/*`
- 输入: `add_file("/movies/dir1/actions/Gongfu", objid)`
- 期望: 绑定失败（挂载点互斥）

### G. placement layout 与回退

**TC-G01 current 版本写入**

- 优先级: P0
- 前置: layout_epoch = current
- 输入: 写入对象
- 期望: target 按 current 选择

**TC-G02 读回退到 epoch-1**

- 优先级: P1
- 前置: current 计算 miss，epoch-1 有数据
- 输入: 读取对象
- 期望: 回退到 epoch-1 成功；记录回退次数

**TC-G03 读回退到 epoch-2**

- 优先级: P1
- 前置: current 与 epoch-1 miss，epoch-2 有数据
- 输入: 读取对象
- 期望: 回退到 epoch-2 成功；不再继续回退

**TC-G04 回退全部失败**

- 优先级: P1
- 前置: current/epoch-1/epoch-2 均 miss
- 输入: 读取对象
- 期望: 返回 NOT_AVAILABLE 或明确错误码

### H. snapshot/copy

**TC-H01 snapshot 正常**

- 优先级: P1
- 前置: src committed
- 输入: `snapshot(src, target)`
- 期望: target 指向同一 committed 版本

**TC-H02 snapshot 遇到 working**

- 优先级: P1
- 前置: src working
- 输入: `snapshot(src, target)`
- 期望: 返回 PATH_BUSY

**TC-H03 copy_path 与 snapshot 差异**

- 优先级: P2
- 前置: src committed
- 输入: `copy_path(src, target)` vs `snapshot(src, target)`
- 期望: 语义一致性与元数据差异可被观察（如需要）

### I. erase 与 GC

**TC-I01 erase_obj_by_id**

- 优先级: P0
- 前置: objid 已 materialize
- 输入: `erase_obj_by_id(objid)` 后 `open_reader_by_id`
- 期望: 返回 NEED_PULL/NOT_PULLED；path 绑定保持不变

**TC-I02 soft GC 清理**

- 优先级: P0
- 前置: 存在 `chunk.tmp`、过期 lease、失败 pull 残留
- 输入: 触发软清理
- 期望: 临时/软状态被清理，committed 数据不受影响

**TC-I03 业务数据删除需显式接口**

- 优先级: P0
- 前置: 已有 committed 对象
- 输入: 执行 delete(path) 或软清理
- 期望: 业务数据不被删除；仅显式 erase 可触发本地驱逐

**TC-I04 erase 后 stat_by_objid 状态**

- 优先级: P1
- 前置: objid 已 materialize
- 输入: `erase_obj_by_id` 后 `stat_by_objid`
- 期望: 状态显示未 materialize；path 绑定不变
