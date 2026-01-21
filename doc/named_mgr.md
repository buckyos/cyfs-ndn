

# ndn-lib 主要组件与 NamedMgr 设计说明（整合版）

## 0. 系统契约

1. **内容寻址与协议强规范**

   * ObjId / ChunkId 采用 `sha256:xxxx`（内容寻址）。
   * canonical encoding / 对象类型/版本等由 **cyfs:// 协议规范**强约束，本实现严格遵循，不在本设计文档重复定义。

2. **Path 语义不是 POSIX**

   * NamedMgr 提供一组“类 FS 的 API”，但其一致性、原子性、并发语义**以本文档为准**，不得按 POSIX 推断。
   * 架构文档中必须**完整枚举**对外暴露的 FS 接口及其语义（见后文 fs-meta-db API 章节）。

3. **严格单写（Single Writer）** 

   * 任意时刻，一个 path 只允许一个写会话（session）持有写租约并写入 file_buffer。
   * 其他 session 对该 path 的写请求必须失败（PATH_BUSY / LEASE_CONFLICT）。
   * 读：允许并发读取“已提交（committed）版本”；对“工作态（working/file_buffer）”的读取默认禁止（除非同 session 特例，见 open_reader 语义）。

4. **Placement（类 CRUSH selector）支持版本回退最多 2 个版本** 

   * 写：始终按当前 layout/selector 版本选择 target。
   * 读：按当前版本计算失败时，最多回退到前 2 个历史版本进行查找（共最多 3 轮）。

5. **qcid 仅用于访问 External Link 外部数据前的快速校验** 

   * qcid 用于“外部文件是否被改动”的快速检测；**不是常规路径**、也不参与对象寻址。
   * 若无 ExternalLink，则系统基本不使用 qcid。

6. **大规模默认不启用自动 GC 删除业务数据** 

   * 有海量数据的核心系统默认不做自动数据删除。
   * 但必须存在：临时态/软状态清理（tmp chunk、过期 lease、失败 pull 的残留等）。
   * 提供 **基于 ObjId 的“物理删除/本地驱逐（erase）接口”**（见后文），其语义是：立刻把该 ObjId 变为“未 pull 状态”。


## 1. 关键术语

* **Pool（pool_id）**：zone/集群级边界（命名空间与资源的主要隔离域之一）。
* **NDM Instance（ndm_id）**：NamedMgr 在一个 pool 内的逻辑实例（命名空间隔离单元）。

  * 完整路径：`ndm://pool_id/ndm_id/path`
* **Placement Layout / Selector（store_layout）**：对象级 placement 算法（类 CRUSH），输入 ObjId/ChunkId 输出一个或多个存储目标。
* **Store Target（store_id / target）**：pool 中的可用空间单元（通常以目录/卷形式存在，常见容量 ~128GB，可扩容新增）。


> 注：path 层（fs-meta-db）与对象存储层（placement + named_store）解耦：**placement 层看不到 path，只按 ObjId/ChunkId 做选择**。

## 2. ndn-lib 主要组件总览

* **named_mgr**：基于 NamedData（对象/块）对外提供“类文件系统”的抽象（非 POSIX），包含：

  * **fs_meta_db**：path 元数据管理，实现逻辑文件系统与强一致绑定（单机 SQLite / 未来 klog+Raft）。
  * **file_buffer（buffer service）**：工作态写缓冲（单机 mmap；分布式版本可按目录配置策略：高速/可靠/并发写）。
  * **NamedDataMgr（含 named_store 能力）**：保存 NamedObject、Chunk、Link 及其索引；提供 open_reader_by_id/get_object 等绕过 path 的访问能力。
  * **store_layout（placement）**：管理多个 store target，按 ObjId/ChunkId 选择写入目标；读支持最多回退 2 个版本。

* **ndn_client / ndn_router（ndn_server）**：实现 cyfs:// 客户端与服务端，提供跨 named_mgr（跨 device/节点）的传输能力：

  * ndn_client：cyfs 扩展 HTTP 的客户端、校验、同步工具集。
  * ndn_router：绑定到一个 named_mgr，对外提供 cyfs:// 服务端（鉴权、inner_path、push_chunk、rhttp 等）。

* **分布式方向**：在 DCFS 完整实现前，named_mgr 可通过 ndn_client/router + 多 device 的 named_store/target 实现“分布式内容同步/拉取”，但 **path 的强一致域**仍由 fs_meta_db 的后端决定（单机 SQLite 或未来 Raft）。


## 3. NamedDataMgr / named_store（对象与 chunk 的 CAS 存储）

> 说明：由于“数据库共享/统一索引”的问题，named_store 的很多功能属于 NamedDataMgr 的一部分，但逻辑上仍按“对象库 + chunk 实体 + link 索引”拆分描述。

### 3.1 存储模型

* **NamedObject（小）**：存数据库（对象库）。
* **Chunk（≤128MB）**：实体存本地文件（按 ChunkId 派生路径）。
* **无内容缓存**：所有操作尽量减少内存占用，尽早/原子/快速完成磁盘 IO。

### 3.2 Chunk 写入与一致性（简化：默认不做断点续传）

由于 chunk ≤128MB，且网络环境允许，我们默认 **chunk 下载/写入为原子提交**：

* 写入到 `chunk.tmp`
* （可选）fsync
* 校验 sha256
* rename 为 `chunk.final`
* 标记 COMPLETE

> 断点续传可以作为可选优化，但 v1 设计不依赖它（pull_chunk 默认整块下载）。

### 3.3 Link 模型（允许多个 ObjId 共享实体）

支持 3 类 Link（以 chunk 为主）：

1. **SameAs(A == B)**：ChunkA 与 ChunkB 完全相同（别名/去重）。
2. **PartOf(A ⊂ B)**：A 是 B 的一部分（用于复用/切片场景，是否启用取决于实现）。
3. **ExternalFile(ChunkA 由外部文件提供)**：ChunkA 数据来自本地文件（支持相对路径 + range）。

   * 访问前使用 **qcid** 对外部文件做快速变化检测；如检测变化则视为 link invalid（见后文 reader 语义）。

并且针对 3 种 Link 支持反向查询索引：

* 哪些对象与 B 相同（SameAs 反查）
* 哪些对象是 B 的一部分（PartOf 反查）
* 给定 LocalFile，哪些 Chunk 引用了它（ExternalFile 反查）

### 3.4 在有 Link 的情况下的逻辑（reader/writer）

```python
# 只有完成的内部 chunk 才可直接打开 reader
def GetChunkReader(chunkid, offset):
    item = db.query_chunk_item(chunkid)
    if item and item.state == COMPLETE:
        path = store.get_chunk_path(chunkid)
        return open(path, offset)

    # 若内部不存在/不完整，则尝试 link
    link = db.query_link(chunkid)
    if not link:
        return null

    match link.type:
        case SameAs(real_chunk_id):
            # 防环：应带 visited set（略）
            return GetChunkReader(real_chunk_id, offset)

        case ExternalFile(path, range, qcid_opt):
            # 仅此处使用 qcid：访问外部数据前快速校验
            if qcid_opt and not qcid_check(path, qcid_opt):
                return error(LINK_INVALID_OR_CHANGED)
            return open_file_range(path, range, offset)

        case PartOf(base_chunk_id, range):
            # 可选：实现为打开 base_chunk_reader 并做 range slice
            r = GetChunkReader(base_chunk_id, range.start + offset)
            return slice_reader(r, range)

    return null


# Writer：仅允许写入内部存储（忽略 link），保证内容寻址语义不被外部可变数据污染
def GetChunkWriter(chunkid):
    item = db.query_chunk_item(chunkid)
    if item and item.state in [ABSENT, WRITING]:
        tmp_path = store.get_chunk_tmp_path(chunkid)
        return open_tmp_writer(tmp_path)
    return null
```


## 4. NamedMgr（对外的“类 FS 抽象”，非 POSIX）

### 4.1 NamedMgr 的核心职责

* **Path 绑定（发布）**：基于 fs-meta-db 实现 `path -> named_data(objid)`。path 通常具有逻辑语义，是可更新的内容发布手段。
* **严格单写**：同一 path 同时只能存在一个写会话的 working buffer。
* **读访问**：对 committed 对象提供稳定并发读；对 working 默认拒绝（可选允许同 session）。
* **pull / materialization**：对象可先绑定后拉取；stat 可观测拉取状态。
* **（小规模可选）自动回收**：不启用自动删业务数据时，仅清理软状态；业务数据删除只通过显式接口。
* **ObjId 驱逐/物理删除（本地）**：提供按 ObjId 删除本地物化数据的接口，使其立即回到“未 pull 状态”。

### 4.2 pool / ndm_id（命名空间隔离）

* pool 通常基于 zone_id 标识。
* ndm_id 表示一个 NamedMgr 实例（命名空间隔离单元）：

  * 完整路径：`ndm://pool_id/ndm_id/path`
* 不同 ndm_id 之间：

  * **path 元数据、对象库隔离**（各自 fs-meta-db/对象库独立或逻辑隔离）
  * **chunk 实体可共享（可选）**：通过全局 chunk store 或 SameAs/link 实现去重复用


## 5. 从“FS 接口”的角度看 NamedMgr 设计

### 5.1 元数据管理（path 层）

* 将 path 指向 DirObjectId / FileObjectId（若支持发布可验证，还需要 PathMetaObject/签名）。
* 支持高效率 move：仅变更 path 指针，不搬内容。
* 支持设置 path meta_data（或 PathMetaObject）。

### 5.2 读 ObjId（绕过 path）

* `open_reader_by_id(objid)`：绕过 path，直接读对象/inner_path。
* `get_object(objid)`：读对象元数据（对象库）。

### 5.3 chunk 管理（两种来源）

* **内部存储**：chunk 完整落在 store target 内（首选、语义最强）。
* **ExternalLink**：chunk 由外部文件提供（访问前 qcid 快检；可选升级 sha256 强校验）。


## 6. path 与 inner_path（解析规则与挂载互斥）

例：路径 `/movies/dir1/actions/MI6/content/4` 可以 open_reader 成功

* `/movies/dir1 -> dir_obj_1`
* `dir_obj_1` 存在 sub_item `actions/MI6 -> fileObjectA`
* `fileObjectA` 字段 `content -> chunklistA`
* `chunklistA[4] -> chunkid`

此例中：

* **path** = `/movies/dir1`
* **inner_path** 三段：`actions/MI6`、`content`、`4`

### 6.1 挂载互斥

为了避免 path 子树与 DirObject 展开结果冲突：

* 当系统已经存在 `/movies/dir1/actions/Gongfu -> fileobjectB`（显式 path 子绑定）时，系统**禁止**将 `/movies/dir1` 绑定到一个会产生 `actions/*` 子树的 DirObject（例如 `dir_obj_1`）。
* 这是“挂载点（mount）互斥”语义：一旦 `/movies/dir1` 绑定为 DirObject，其子树应由该 DirObject 决定，不允许再由 path 层覆盖。



## 7. fs-meta-db 主要接口


```rust
// add_file/add_dir：在 namespace 中绑定一个“已构造对象”的 ObjId
// 该操作与对象大小无关（O(1) 元数据事务），成功不代表 open_reader/list 立刻可用（可能未 pull）
fn add_file(&self, path: &Path, obj_id: ObjId) {
  // 事务：写 path->objid 绑定；校验挂载互斥；更新必要的 path meta
}

fn add_dir(&self, path: &Path, dir_obj_id: ObjId) {
  // 同上
}

// pull：把对象 body/chunk 拉到本地（异步/可观测）
// 调用后立刻返回，后续通过 stat/stat_by_objid 获取 pull 状态
fn pull(&self, path: &Path, remote: PullContext) { }

fn pull_by_objid(&self, obj_id: ObjId, remote: PullContext) { }

// stat：必须可观测 committed/working/materialization 等
fn stat(&self, path: &Path) -> PathStat { }

fn stat_by_objid(&self, obj_id: ObjId) -> ObjStat { }

// delete/move/copy：仅作用于 path 绑定（namespace 事务）
// 注意：不自动触发业务数据删除（大规模默认不启用 GC）
fn delete(&self, path: &Path) { }

fn move_path(&self, old_path: &Path, new_path: &Path) { }

fn copy_path(&self, src: &Path, target: &Path) { }

fn list(&self, path: &Path, pos: u32, page_size: u32) { }

// open_reader：读取语义（非 POSIX）
// - 若 path 指向 committed object：允许并发读；若未 pull 则返回 NEED_PULL/NOT_AVAILABLE
// - 若 path 指向 working file_buffer：默认拒绝（PATH_BUSY）；可选允许同 session 读
fn open_reader(&self, path: &Path) -> Reader { }

fn open_reader_by_id(&self, objid: ObjId) -> Reader { }

fn get_object(&self, objid: ObjId) -> NamedObject { }

fn close_reader(reader: Reader) { }


// ------- 写接口（严格单写） -------

// create_dir：创建空目录（实现上可先创建 working dir buffer 或直接生成空 DirObject）
fn create_dir(&self, dir_path: &Path) { }

// create_file：创建工作态 file_buffer，并对 path 获取写租约（严格单写）
fn create_file(&self, path: &Path, expect_size: u32) -> FileBuffer {
  // 1) 选择 buffer_node/本地 mmap 文件（单机版）
  // 2) fs-meta-db 事务：创建 path，并记录 working 状态 + lease(session,fence,ttl)
  // 3) 返回 filebuffer handle
}

// append：高速追加（仅对 working file_buffer；有大小限制）
fn append(&self, path: &Path, data: Buffer) { }

// flush：确保 filebuffer 数据落盘到 buffer_node
fn flush(fb: FileBuffer) { }

// close_file：结束写入，释放 lease，并进入待提交队列（auto-cacl）
fn close_file(fb: FileBuffer) {
  // flush
  // fs-meta-db: 标记 working closed（不再允许写），释放/缩短 lease
  // 投递到 auto-cacl 队列
}

// cacl_name：把 working file_buffer “对象化”为 FileObject（content->chunklist），并提交为 committed
fn cacl_name(&self, path: Path) {
  // 前置条件：path 当前处于 working-closed，且无其他写者（严格单写）
  // 1) 计算 chunklist（按 placement 写入内部 chunk store；或按策略生成 external link）
  // 2) 写入所需 chunk/object
  // 3) 更新 path: working -> committed(fileobjectid)
  // 4) 释放 file_buffer
}

// snapshot：构造只读快照（语义上等同 copy committed 版本）
// v若 src 正在 working，则 snapshot 返回 PATH_BUSY（要求先 close + cacl）
fn snapshot(&self, src: Path, target: Path) { }


// erase_obj_by_id：删除本地已物化数据，使该 objid 立即变为“未 pull 状态”
// - 不改变 path->objid 绑定
// - 后续访问将触发 NEED_PULL / NOT_PULLED
fn erase_obj_by_id(&self, objid: ObjId) { }
```

## 8. “物理删除接口”的语义（ObjId 驱逐 = 变未 pull）


* `erase_obj_by_id(objid)`：

  * 删除/释放本地缓存的 object body 与相关 chunk（实现可按需：只删 body、或 body+chunk）
  * 保留 namespace 绑定与对象元信息（至少能让 stat 显示该对象存在但未物化）
  * 后续：

    * `open_reader_by_id(objid)` / `open_reader(path->objid)` 返回 `NEED_PULL / NOT_PULLED`
    * 可以通过 `pull_by_objid` 重新拉取恢复


## 9. GC 的实现（按规模分层：软状态清理 vs 业务数据删除）

### 9.1 软状态清理（必须常开）


* `chunk.tmp`、失败下载残留
* 过期 lease / 崩溃遗留 working buffer
* pull 任务中断后的中间态标记（不影响 committed 可用性）

### 9.2 业务数据删除（大规模系统默认关闭）

* 大规模（50PB+）默认关闭“自动删除业务数据”的 GC。
* 业务数据删除仅通过显式接口（例如管理面 delete/erase），并以审计/权限控制。


## 10. store_layout / selector

> 这块有专门文档、细节很多。

### 10.1 目标

* 输入：ObjId / ChunkId（sha256:…）
* 输出：一个或多个 Store Target（store_id），用于写入/副本
* 支持：按规模切换 selector（类 CRUSH 改进算法）

### 10.2 layout 版本与读回退（最多 2 版）

* 写：使用 `layout_epoch = current`
* 读：若按 current 计算的 target 集合 miss，则最多回退 `epoch-1`、`epoch-2` 再试
* 迁移：可选（lazy rebalance），不作为 correctness 前提

### 10.3 Store Target（可用空间单元）

* 通常以目录/卷形式存在，典型大小 ~128GB
* 有独立配置文件，允许不同实现（不同介质/不同后端）

## 11. helper / Cacl / 同步工具（tools.rs）

### 11.1 发布相关

* 发布本地文件成为 fileobject 到指定 path（相当于保存文件）
* 发布本地目录成为 dirobject（计算子项 ObjId）

### 11.2 Cacl（对象化）

* 计算 localdir 的 dirobject：

  * 模式 A：chunk 以 Link 模式保存（ExternalFile）
  * 模式 B：chunk 全部写入 named_mgr 内部 store（推荐用于可验证/可迁移场景）
* 通过 qcid 加快“外部文件是否改变”的判断（仅 ExternalLink 访问前使用）

### 11.3 copy/sync（DirObject 复制到本地）

* 智能同步：保留本地更新（重命名），同步删除/新增
* 完全同步：同步前删除目标目录再复制

### 11.4 本地两个 named_mgr 之间同步

* `copy_dir(src_named_mgr, dest_named_mgr, pull_mode)`
* 由于严格单写，dest 中 path 的更新应走 add_file/add_dir 或 snapshot/copy 语义；不允许并发写冲突。


## 12. ndn_client（cyfs:// over http 扩展 + 同步工具集）

* cyfs 协议扩展 HTTP 头并进行验证：

  * http 默认开启验证，https 默认关闭（可强制开启）。
* Helper：

  * `get_obj_by_url`
  * `get_chunk_reader_by_url`（支持 chunklist/inner_path 的扩展可在后续明确）

### 12.1 pull/download 语义（区分是否落入 NamedMgr）

* `pull_chunk`：从远端同步 chunk 到 ndn-client 绑定的 named_mgr（默认整块下载，原子提交）
* `download_chunk`：下载 chunk 到本地文件，不影响 named_mgr
* 组合接口：

  * `pull_chunk_to_local`：下载到本地并以 Link 模式登记（若 named_mgr 内已存在则不创建 Link）
  * `pull_file_to_local`：下载文件到本地并 Link
  * `pull_dir_to_local`：下载目录（DirObject/FileObject 进入 named_mgr，文件用 pull_file_to_local）

### 12.2 更新文件（remote_is_better）

* 判断远端 fileobject 是否更新
* 有更新则下载并更新本地数据与 fileobj 路径


## 13. ndn_router（ndn_server）

绑定到一个 named_mgr，提供 cyfs:// 服务端实现（依赖 cyfs-gateway-lib）：

* 正确解析 URL
* 身份/权限验证（含付费认证）
* O-Link：基于 object 接口访问对象并解析多级 inner_path
* R-Link：基于 path 接口访问对象并解析多级 inner_path
* HTTP POST：实现 Push_Chunk 服务端
* cyfs:// rhttp：允许服务端向客户端发起 object get 请求（创建可运行的 ndn_client）




