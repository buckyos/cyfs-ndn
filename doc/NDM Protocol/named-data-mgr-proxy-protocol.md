# NamedDataMgr Proxy 协议设计

## 1. 文档定位

本文描述一套新的 `NamedDataMgr Proxy Protocol`。

它的目标不是复述当前 `src/named_store/src/ndm_zone_gateway.rs` 已有的浏览器 / tus 协议，也不是复述 `named-store-http-protocol.md` 那套 bucket 级最薄读写协议，而是补出一层**面向 Zone 内受信进程的 `NamedDataMgr` 远程代理协议**。

这份文档是**目标设计稿**，不是当前实现说明。实现阶段可以分批落地，但协议边界、路由模型和能力归属应以本文为准。

典型调用链：

`Zone App -> ndm_client -> NDM Proxy -> NamedDataMgr`

这里的 `NamedDataMgr` 当前对应实现主要是：

- `src/named_store/src/ndm.rs`

## 2. 协议角色与边界

### 2.1 它解决什么问题

这套协议解决的是：

> Zone 内一个受信进程，如果不能直接 in-process 调用 `NamedDataMgr`，应该如何通过网络协议拿到尽量等价的 `NamedDataMgr` 能力。

它面向的调用方包括：

- Zone 内 App
- Docker 内 service
- Agent / daemon
- 需要通过 loopback 或 NodeGateway 访问 NDM 的进程

### 2.2 它不解决什么问题

它不是下面两类协议的替代品：

- 不是浏览器上传协议
  浏览器友好的查重、tus 会话、上传缓存、配额和 TTL，继续留在 `named-data-mgr-zone-protocol.md`
- 不是 bucket/data-plane 最薄协议
  直接的 object / chunk bucket 读写，继续留在 `named-store-http-protocol.md`

### 2.3 与其他协议的关系

三层分工如下：

- `CYFS Protocol`
  解决跨 Zone pull、可验证引用和公网访问
- `NamedDataMgr Proxy Protocol`
  解决 Zone 内受信客户端如何远程调用 `NamedDataMgr`
- `Named Store HTTP Protocol`
  解决选定 bucket 之后最薄的一层 object / chunk 读写

`NamedDataMgr Proxy` 可以在服务端内部继续调用 `NamedStoreMgr`，也可以在必要时复用 bucket 协议做跨设备落桶；但这些都属于服务端实现细节，不直接暴露给客户端。

## 3. 设计目标

1. 覆盖 `src/named_store/src/ndm.rs` 中**适合远程代理**的核心能力。
2. 让 `ndm_client` 可以在不直接链接服务端内存对象的前提下，获得接近本地调用的开发体验。
3. 把控制面和流式 data-plane 区分清楚，避免把所有东西都塞进 JSON RPC。
4. 不把浏览器上传的 tus 状态机、文件级编排、缓存配额等语义带进来。
5. 不把 store layout 生命周期和进程内注册类能力硬暴露为远程接口。

## 4. 非目标

- 不为浏览器定义 CORS 友好接口
- 不定义 tus、断点续传和文件级 upload session
- 不暴露服务端本地路径相关能力，如 `add_chunk_by_link_to_local_file`
- 不把 `register_store`、`add_layout` 这类管理器装配能力做成常规远程接口
- 不替代 bucket 级协议中的最薄 object / chunk 直读直写模型

## 5. `ndm.rs` 能力纳入矩阵

下表规定 `NamedDataMgr` 当前方法在代理协议中的归属。

| `ndm.rs` 方法 | 是否纳入代理协议 | 说明 |
|---|---|---|
| `get_object` | 是 | 小对象 / JWT / JSON 读取，走 JSON RPC |
| `open_object` | 是 | 路径解析后返回对象串，走 JSON RPC |
| `get_dir_child` | 是 | 路径子节点解析，走 JSON RPC |
| `is_object_stored` | 是 | 走 JSON RPC |
| `is_object_exist` | 是 | 走 JSON RPC |
| `query_object_by_id` | 是 | 走 JSON RPC |
| `put_object` | 是 | 走 JSON RPC |
| `remove_object` | 是 | 走 JSON RPC |
| `have_chunk` | 是 | 走 JSON RPC |
| `query_chunk_state` | 是 | 走 JSON RPC |
| `open_chunk_reader` | 是 | 走流式读接口 |
| `open_chunklist_reader` | 是 | 走流式读接口 |
| `open_reader` | 是 | 走流式读接口 |
| `get_chunk_data` | 是 | 走“整块读取”二进制接口 |
| `get_chunk_piece` | 是 | 走“定长读取”二进制接口 |
| `put_chunk_by_reader` | 是 | 走流式写接口 |
| `put_chunk` | 是 | 客户端便利方法，协议上复用同一流式写接口 |
| `remove_chunk` | 是 | 走 JSON RPC |
| `add_chunk_by_same_as` | 是 | 走 JSON RPC |
| `apply_edge` | 是 | 走 JSON RPC，默认受限 |
| `pin` | 是 | 走 JSON RPC，默认受限 |
| `unpin` | 是 | 走 JSON RPC，默认受限 |
| `unpin_owner` | 是 | 走 JSON RPC，默认受限 |
| `fs_acquire` | 是 | 走 JSON RPC，默认受限 |
| `fs_release` | 是 | 走 JSON RPC，默认受限 |
| `fs_release_inode` | 是 | 走 JSON RPC，默认受限 |
| `fs_anchor_state` | 是 | 走 JSON RPC，默认受限 |
| `forced_gc_until` | 是 | 走 JSON RPC，默认受限 |
| `outbox_count` | 是 | 走 JSON RPC，默认受限 |
| `debug_dump_expand_state` | 是 | 走 JSON RPC，默认受限 |
| `anchor_state` | 是 | 走 JSON RPC，默认受限 |
| `get_store_mgr` | 否 | 构造 / 装配行为，只在进程内 |
| `new` / `with_max_versions` | 否 | 构造行为，只在进程内 |
| `register_store` / `unregister_store` | 否 | 运行时装配行为 |
| `add_layout` | 否 | 集群 / 管理面能力，不属于普通 client proxy |
| `current_layout` / `get_layout` / `all_versions` | 否 | 管理 / 观测面能力，后续若需要可单独开 admin 协议 |
| `select_store_for_write` / `select_store_for_read` | 否 | 服务端内部路由细节 |
| `version_count` / `current_epoch` / `compact` | 否 | 管理器内部能力 |
| `get_store` / `get_store_ids` | 否 | 存储拓扑暴露，默认不进入普通 client proxy |
| `add_chunk_by_link_to_local_file` | 否 | 依赖服务端本地路径，不应远程暴露 |

## 6. 协议总览

### 6.1 基础约定

- 基础前缀：`/ndm/proxy/v1`
- 默认协议：`HTTP/1.1`
- 所有 JSON 请求与响应使用 `application/json; charset=utf-8`
- 所有二进制流接口返回 `application/octet-stream`
- 所有错误响应统一返回 JSON
- 这套协议默认只面向 Zone 内受信调用方
- 推荐部署在 loopback、Unix domain socket 或受控的 NodeGateway 内网入口上

### 6.2 路由分层

这套协议按语义分为三类路由：

| 路由前缀 | 作用 |
|---|---|
| `/ndm/proxy/v1/rpc/{method}` | 小报文 JSON RPC 控制面 |
| `/ndm/proxy/v1/read/*` | 流式读取 |
| `/ndm/proxy/v1/write/*` | 流式写入 |

### 6.3 为什么不全部做成 JSON RPC

`NamedDataMgr` 里有三类能力：

- 纯控制面与小对象操作
- 可组合的 reader 打开语义
- chunk 二进制写入

前一类适合 JSON RPC。  
后两类如果也强行包成 JSON，会把 `reader` 语义打碎成 base64 大包，失去流式优势，也会和 `NamedStoreHttp` 的 data-plane 设计冲突。

因此本协议明确采用：

- `JSON RPC` 负责控制面
- `binary stream` 负责 read / write data-plane

## 7. 通用错误模型

### 7.1 错误响应体

统一返回：

```json
{
  "error": "error_code",
  "message": "detail message"
}
```

### 7.2 `NdnError` 到 HTTP 的映射

| 内部错误 | HTTP | `error` |
|---|---:|---|
| `NotFound` | `404` | `not_found` |
| `InvalidParam` | `400` | `invalid_param` |
| `InvalidData` | `400` | `invalid_data` |
| `InvalidId` | `400` | `invalid_id` |
| `InvalidObjType` | `400` | `invalid_obj_type` |
| `VerifyError` | `409` | `verify_error` |
| `PermissionDenied` | `403` | `permission_denied` |
| `AlreadyExists` | `409` | `already_exists` |
| `OffsetTooLarge` | `416` | `offset_too_large` |
| `Unsupported` | `405` | `unsupported` |
| 其他错误 | `500` | `internal_error` |

补充规则：

- 未知路由返回 `404 not_found`
- 已知路由但 method 不匹配返回 `405 unsupported`
- 受限操作在鉴权失败时返回 `403 permission_denied`

## 8. 类型编码约定

### 8.1 ID 编码

- `obj_id`
  使用 `ObjId::to_string()`
- `chunk_id`
  在协议上仍以字符串传输，本质上是 chunk 类型的 `ObjId`
- `chunk_list_id`
  使用 `ObjId::to_string()`

### 8.2 `inner_path`

`inner_path` 的 `null`、空串和 `/` 统一规范化为 `None`。

### 8.3 `ObjectState`

`query_object_by_id` 返回：

```json
{ "state": "not_exist" }
```

或：

```json
{ "state": "object", "obj_data": "..." }
```

### 8.4 `ChunkStoreState`

`query_chunk_state` 使用如下 JSON 形状：

```json
{ "state": "completed", "chunk_size": 123 }
```

可能的 `state`：

- `new`
- `completed`
- `disabled`
- `not_exist`
- `local_link`
- `same_as`

其中：

- `local_link` 额外返回 `local_info`
- `same_as` 额外返回 `same_as`

### 8.5 其他内部类型

下列类型在协议上直接复用其当前 JSON 结构：

- `EdgeMsg`
- `PinRequest`
- `ExpandDebug`

下列状态型返回值序列化为字符串：

- `CascadeStateP0`
- `PinScope`

## 9. 路由总表

### 9.1 JSON RPC

| 路由 | 方法 | 说明 |
|---|---|---|
| `/ndm/proxy/v1/rpc/get_object` | `POST` | 获取对象串 |
| `/ndm/proxy/v1/rpc/open_object` | `POST` | 带 `inner_path` 的对象解析 |
| `/ndm/proxy/v1/rpc/get_dir_child` | `POST` | 解析目录子项 |
| `/ndm/proxy/v1/rpc/is_object_stored` | `POST` | 递归完整性检查 |
| `/ndm/proxy/v1/rpc/is_object_exist` | `POST` | 对象是否存在 |
| `/ndm/proxy/v1/rpc/query_object_by_id` | `POST` | 查询对象状态 |
| `/ndm/proxy/v1/rpc/put_object` | `POST` | 写入对象 |
| `/ndm/proxy/v1/rpc/remove_object` | `POST` | 删除对象 |
| `/ndm/proxy/v1/rpc/have_chunk` | `POST` | chunk 是否可用 |
| `/ndm/proxy/v1/rpc/query_chunk_state` | `POST` | 查询 chunk 状态 |
| `/ndm/proxy/v1/rpc/remove_chunk` | `POST` | 删除 chunk |
| `/ndm/proxy/v1/rpc/add_chunk_by_same_as` | `POST` | 注册 same_as |
| `/ndm/proxy/v1/rpc/apply_edge` | `POST` | GC edge 操作 |
| `/ndm/proxy/v1/rpc/pin` | `POST` | pin |
| `/ndm/proxy/v1/rpc/unpin` | `POST` | unpin |
| `/ndm/proxy/v1/rpc/unpin_owner` | `POST` | 按 owner 批量 unpin |
| `/ndm/proxy/v1/rpc/fs_acquire` | `POST` | fs anchor acquire |
| `/ndm/proxy/v1/rpc/fs_release` | `POST` | fs anchor release |
| `/ndm/proxy/v1/rpc/fs_release_inode` | `POST` | 按 inode 批量 release |
| `/ndm/proxy/v1/rpc/fs_anchor_state` | `POST` | 查询 fs anchor 状态 |
| `/ndm/proxy/v1/rpc/forced_gc_until` | `POST` | 强制 GC |
| `/ndm/proxy/v1/rpc/outbox_count` | `POST` | outbox 数量 |
| `/ndm/proxy/v1/rpc/debug_dump_expand_state` | `POST` | 调试展开状态 |
| `/ndm/proxy/v1/rpc/anchor_state` | `POST` | 查询 anchor 状态 |

### 9.2 读接口

| 路由 | 方法 | 说明 |
|---|---|---|
| `/ndm/proxy/v1/read/chunk/open` | `POST` | 打开 chunk reader |
| `/ndm/proxy/v1/read/chunk/data` | `POST` | 读取整块 chunk |
| `/ndm/proxy/v1/read/chunk/piece` | `POST` | 读取 chunk 定长片段 |
| `/ndm/proxy/v1/read/chunklist/open` | `POST` | 打开 chunklist reader |
| `/ndm/proxy/v1/read/object/open` | `POST` | 打开泛化 reader |

### 9.3 写接口

| 路由 | 方法 | 说明 |
|---|---|---|
| `/ndm/proxy/v1/write/chunk/{chunk_id}` | `PUT` | 一次性流式写入 chunk |

## 10. JSON RPC 详细定义

### 10.1 通用约定

- 请求方法统一为 `POST`
- 请求体统一为 JSON
- 查询类通常返回 `200 OK + JSON`
- 写操作通常返回 `204 No Content`

### 10.2 对象类接口

#### `POST /ndm/proxy/v1/rpc/get_object`

请求：

```json
{ "obj_id": "..." }
```

响应：

```json
{ "obj_id": "...", "obj_data": "..." }
```

#### `POST /ndm/proxy/v1/rpc/open_object`

请求：

```json
{ "obj_id": "...", "inner_path": "/a/b" }
```

响应：

```json
{ "obj_data": "..." }
```

#### `POST /ndm/proxy/v1/rpc/get_dir_child`

请求：

```json
{ "dir_obj_id": "...", "item_name": "foo" }
```

响应：

```json
{ "obj_id": "..." }
```

#### `POST /ndm/proxy/v1/rpc/is_object_stored`

请求：

```json
{ "obj_id": "...", "inner_path": "/a/b" }
```

响应：

```json
{ "stored": true }
```

#### `POST /ndm/proxy/v1/rpc/is_object_exist`

请求：

```json
{ "obj_id": "..." }
```

响应：

```json
{ "exists": true }
```

#### `POST /ndm/proxy/v1/rpc/query_object_by_id`

请求：

```json
{ "obj_id": "..." }
```

响应见 §8.3。

#### `POST /ndm/proxy/v1/rpc/put_object`

请求：

```json
{ "obj_id": "...", "obj_data": "..." }
```

响应：

- `204 No Content`

约束：

- 不接受 chunk id
- 语义与 `NamedDataMgr::put_object` 一致

#### `POST /ndm/proxy/v1/rpc/remove_object`

请求：

```json
{ "obj_id": "..." }
```

响应：

- `204 No Content`

约束：

- 不接受 chunk id

### 10.3 Chunk 元数据接口

#### `POST /ndm/proxy/v1/rpc/have_chunk`

请求：

```json
{ "chunk_id": "..." }
```

响应：

```json
{ "exists": true }
```

#### `POST /ndm/proxy/v1/rpc/query_chunk_state`

请求：

```json
{ "chunk_id": "..." }
```

响应见 §8.4。

#### `POST /ndm/proxy/v1/rpc/remove_chunk`

请求：

```json
{ "chunk_id": "..." }
```

响应：

- `204 No Content`

#### `POST /ndm/proxy/v1/rpc/add_chunk_by_same_as`

请求：

```json
{
  "big_chunk_id": "...",
  "chunk_list_id": "...",
  "big_chunk_size": 123
}
```

响应：

- `204 No Content`

### 10.4 GC / Anchor / Debug 接口

这些接口默认属于**受限控制面**。协议允许定义它们，但实现端应默认要求更高权限。

#### `POST /ndm/proxy/v1/rpc/apply_edge`

请求体直接为 `EdgeMsg` JSON。  
响应为 `204 No Content`。

#### `POST /ndm/proxy/v1/rpc/pin`

请求体直接为 `PinRequest` JSON。  
响应为 `204 No Content`。

#### `POST /ndm/proxy/v1/rpc/unpin`

请求：

```json
{ "obj_id": "...", "owner": "..." }
```

响应为 `204 No Content`。

#### `POST /ndm/proxy/v1/rpc/unpin_owner`

请求：

```json
{ "owner": "..." }
```

响应：

```json
{ "count": 123 }
```

#### `POST /ndm/proxy/v1/rpc/fs_acquire`

请求：

```json
{ "obj_id": "...", "inode_id": 1, "field_tag": 2 }
```

响应为 `204 No Content`。

#### `POST /ndm/proxy/v1/rpc/fs_release`

请求：

```json
{ "obj_id": "...", "inode_id": 1, "field_tag": 2 }
```

响应为 `204 No Content`。

#### `POST /ndm/proxy/v1/rpc/fs_release_inode`

请求：

```json
{ "inode_id": 1 }
```

响应：

```json
{ "count": 123 }
```

#### `POST /ndm/proxy/v1/rpc/fs_anchor_state`

请求：

```json
{ "obj_id": "...", "inode_id": 1, "field_tag": 2 }
```

响应：

```json
{ "state": "Pending" }
```

#### `POST /ndm/proxy/v1/rpc/forced_gc_until`

请求：

```json
{ "target_bytes": 123 }
```

响应：

```json
{ "freed_bytes": 123 }
```

#### `POST /ndm/proxy/v1/rpc/outbox_count`

请求体可为空对象：

```json
{}
```

响应：

```json
{ "count": 123 }
```

#### `POST /ndm/proxy/v1/rpc/debug_dump_expand_state`

请求：

```json
{ "obj_id": "..." }
```

响应：

```json
{ "...": "ExpandDebug JSON" }
```

#### `POST /ndm/proxy/v1/rpc/anchor_state`

请求：

```json
{ "obj_id": "...", "owner": "..." }
```

响应：

```json
{ "state": "Materializing" }
```

## 11. 流式读接口

### 11.1 通用响应头

所有成功的读接口都应返回：

- `Content-Type: application/octet-stream`
- `NDM-Total-Size: {logical_total_size}`

按需返回：

- `NDM-Resolved-Object-ID: {obj_id}`
- `NDM-Reader-Kind: chunk | chunklist | object`
- `Content-Length: {returned_bytes}`

如果返回的是某个偏移之后的流，额外返回：

- `NDM-Offset: {offset}`

### 11.2 `POST /ndm/proxy/v1/read/chunk/open`

请求：

```json
{
  "chunk_id": "...",
  "offset": 0
}
```

响应：

- `200 OK`
- body 为从 `offset` 开始的 chunk 二进制流

语义：

- 对应 `NamedDataMgr::open_chunk_reader`
- `NDM-Total-Size` 表示 chunk 总长度，不是剩余长度

### 11.3 `POST /ndm/proxy/v1/read/chunk/data`

请求：

```json
{ "chunk_id": "..." }
```

响应：

- `200 OK`
- body 为完整 chunk 字节

语义：

- 对应 `NamedDataMgr::get_chunk_data`
- 这是便利接口，本质上等价于 `open_chunk_reader(offset=0)` 后读到 EOF

### 11.4 `POST /ndm/proxy/v1/read/chunk/piece`

请求：

```json
{
  "chunk_id": "...",
  "offset": 0,
  "piece_size": 4096
}
```

响应：

- `200 OK`
- body 为定长 `piece_size` 字节

语义：

- 对应 `NamedDataMgr::get_chunk_piece`
- `Content-Length` 必须等于实际返回字节数
- 若底层无法读满 `piece_size`，返回错误而不是短读成功

### 11.5 `POST /ndm/proxy/v1/read/chunklist/open`

请求：

```json
{
  "chunk_list_id": "...",
  "offset": 0
}
```

响应：

- `200 OK`
- body 为逻辑拼接后的二进制流

语义：

- 对应 `NamedDataMgr::open_chunklist_reader`
- `NDM-Reader-Kind` 应为 `chunklist`
- `NDM-Total-Size` 为逻辑总长度

### 11.6 `POST /ndm/proxy/v1/read/object/open`

请求：

```json
{
  "obj_id": "...",
  "inner_path": "/a/b"
}
```

响应：

- `200 OK`
- body 为最终 reader 对应的二进制流

语义：

- 对应 `NamedDataMgr::open_reader`
- 服务端可解析 `file object -> chunk/chunklist`，再返回最终内容流
- `NDM-Resolved-Object-ID` 表示最终落到的 reader 对象 ID

## 12. 流式写接口

### 12.1 `PUT /ndm/proxy/v1/write/chunk/{chunk_id}`

请求头：

| Header | 必填 | 说明 |
|---|---|---|
| `Content-Type` | 是 | 必须为 `application/octet-stream` |
| `Content-Length` | 是 | 必须等于 chunk_size |
| `NDM-Chunk-Size` | 是 | chunk 总长度 |

请求体：

- 原始 chunk 二进制流

成功响应：

- `201 Created`

或：

- `200 OK`

响应头：

- `NDM-Chunk-Size: {chunk_size}`
- `NDM-Chunk-Write-Outcome: written | already_exists`
- `NDM-Chunk-Object-ID: {chunk_obj_id}`

关键语义：

1. 这是 `put_chunk_by_reader` 的协议绑定。
2. `put_chunk` 只是客户端本地持有 `Vec<u8>` 时的便利方法，协议上仍复用这个接口。
3. 服务端必须以**原子写入**语义处理：
   成功则 chunk 整体可见，失败则保持 `not_exist` 或原有 `already_exists` 状态。
4. 服务端必须校验 body 内容与 `chunk_id` 一致。
5. 若 chunk 事先已存在，允许直接返回 `already_exists`，且不必消费完整 body。

### 12.2 为什么不用 tus

这是 `ndm_client` 代理协议，不是浏览器上传协议。

这里追求的是：

- 与 `NamedDataMgr::put_chunk_by_reader` 接近的语义
- 原子一次写入
- 结果明确区分 `written` 与 `already_exists`

这些语义与 browser 版 `per chunk session + tus PATCH` 不是同一层问题。

## 13. 权限模型

协议定义两类能力：

- 普通能力
  对象查询、对象写入、chunk 状态查询、reader 打开、chunk 写入
- 受限能力
  `apply_edge`、`pin`、`unpin*`、`fs_*`、`forced_gc_until`、`outbox_count`、`debug_dump_expand_state`、`anchor_state`

推荐实现策略：

- 默认部署只开放普通能力
- 受限能力需要显式配置开启
- 若入口是 NodeGateway，应再叠加 Zone 内身份鉴别

## 14. 与现有 Zone Gateway 协议的差异

这份协议与 `named-data-mgr-zone-protocol.md` 的差异必须保持清晰：

| 维度 | Zone Gateway 协议 | NDM Proxy 协议 |
|---|---|---|
| 目标客户端 | 浏览器 / WebUI / 上传页面 | Zone 内受信进程 / `ndm_client` |
| 上传语义 | tus，会话化，per chunk session | 原子一次写入 |
| 主要路由 | `/ndm/v1/uploads` `/ndm/v1/store/*` | `/ndm/proxy/v1/rpc/*` `/read/*` `/write/*` |
| data-plane | 浏览器友好，强调 upload 编排 | 强调 `NamedDataMgr` 代理等价性 |
| 典型能力 | 查重、session、上传缓存、结构化控制面 | 对象控制面、reader 打开、chunk 写入、GC 受限控制面 |

两者可以共存于同一个 NodeGateway 实现中，但不应在文档或 API 形态上混成同一份协议。

## 15. 客户端实现建议

建议 `ndm_client` 分成三层封装：

- `NdmProxyRpcClient`
  负责 `/rpc/*`
- `NdmProxyReaderClient`
  负责 `/read/*`
- `NdmProxyWriterClient`
  负责 `/write/*`

再在最外层组合成与 `NamedDataMgr` 接近的 API：

- `get_object/open_object/query_object_by_id`
- `open_chunk_reader/open_chunklist_reader/open_reader`
- `put_chunk_by_reader/put_chunk`
- `pin/fs_*/forced_gc_until`

这样可以最大限度保持：

- 协议层清晰
- 实现层职责分离
- 客户端体验接近本地 `NamedDataMgr`

## 16. 落地顺序建议

1. 先实现 `/rpc/*` 的对象类接口与 chunk 元数据接口。
2. 再实现 `/read/chunk/open` 与 `/write/chunk/{chunk_id}`，形成最小读写闭环。
3. 再补 `/read/chunklist/open` 与 `/read/object/open`。
4. 最后再补 GC / pin / fs 这组受限控制面。

这样落地时可以先满足大多数 `ndm_client` 的对象和 chunk 访问场景，再逐步向 `ndm.rs` 靠齐。
