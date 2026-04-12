# NDM Zone Gateway 结构化 Store API 设计

## 1. 目标

`src/named_store/src/ndm_zone_gateway.rs` 当前已经承载两类能力：

- `GET /ndm/v1/objects/lookup`：上传前查重；
- `/ndm/v1/uploads/*`：基于 TUS 的 chunk 上传协议。

但 `src/named_store/src/store_mgr.rs` 里还有一批**不依赖 ChunkReader/ChunkWriter 的结构化接口**，目前没有统一的 Zone Gateway 协议承载。本文定义一组新的 HTTP JSON API，用于让 `ndm_zone_gateway.rs` 在不引入流式 chunk/data-plane 语义的前提下，直接暴露 `store_mgr` 的控制面能力。

本文只覆盖：

- 小报文 JSON 请求/响应；
- 对象查询、路径解析、chunk 元数据查询；
- GC / pin / fs-anchor 等控制面调用。

本文明确不覆盖：

- `open_chunk_reader` / `open_chunklist_reader` / `open_reader` 一类流式读取；
- `put_chunk_by_reader` / `put_chunk` 一类二进制写入；
- `get_chunk_data` / `get_chunk_piece` 一类原始字节返回；
- `add_chunk_by_link_to_local_file` 这类依赖服务端本地路径的能力；
- `register_store` / `add_layout` / `compact` 等本机内部管理接口。

说明：虽然 `put_chunk` / `get_chunk_data` 本身不直接暴露 Reader/Writer 类型，但它们仍属于 data-plane，应该继续走现有上传协议或未来独立的下载协议，不放进本次“结构化 API”。

---

## 2. 总体设计

### 2.1 新路由前缀

建议在 `ndm_zone_gateway.rs` 中新增一组统一前缀：

`/ndm/v1/store/`

其下采用**方法型 RPC 路由**，而不是继续扩展资源型 URI。原因如下：

1. `store_mgr.rs` 本身就是一组方法语义，和 RPC 映射天然一一对应；
2. `obj_id`、`inner_path`、`owner`、`inode_id`、`field_tag` 等参数组合较多，统一放 JSON body 更容易避免 URL 编码歧义；
3. `ChunkStoreState`、`ObjectState` 这类返回值并不是天然的 REST 资源，更适合结构化 JSON 响应；
4. 后续继续补接口时，不会和现有 `/ndm/v1/uploads/*`、`/cyfs/*`、`/ndn/*` 路由混在一起。

### 2.2 请求与响应约束

- 除个别纯查询能力可兼容 `GET` 外，**统一使用 `POST` + JSON body**。
- 请求头固定：
  - `Content-Type: application/json`
  - `Accept: application/json`
- 成功响应：
  - 有返回值时：`200 OK` + JSON
  - 纯动作型接口：`204 No Content`
- 错误响应：
  - 继续沿用 `ndm_zone_gateway.rs` 现有的 `build_error_response()` 风格；
  - 对结构化接口，建议 body 统一为 JSON：

```json
{
  "error": "not_found",
  "message": "object xxx not found"
}
```

### 2.3 鉴权边界

这批接口不是浏览器公开上传面，而是 **Zone 内受信调用面**。建议默认要求：

- 设备到设备；
- Agent 到 Gateway；
- 或具有明确后端能力标识的 SDK/service token。

特别是以下接口不应直接暴露给普通浏览器会话：

- `put_object`
- `remove_object`
- `remove_chunk`
- `apply_edge`
- `pin` / `unpin`
- `forced_gc_until`
- `fs_*`

### 2.4 推荐在 `ndm_zone_gateway.rs` 中的落点

建议新增：

- `route_store_structured_request(req)`
- `handle_store_rpc(method, req_body)`

路由判断顺序建议放在：

1. `objects/lookup`
2. `uploads/*`
3. `store/*`
4. `cyfs/*` / `ndn/*`

这样不会影响现有上传协议。

---

## 3. 通用编码规则

### 3.1 `obj_id` / `chunk_id`

- 统一使用字符串；
- `chunk_id` 在协议中仍按 `ObjId` 字符串格式传输；
- 服务端内部再用 `ObjId::new()` / `ChunkId::from_obj_id()` 解析。

### 3.2 `inner_path`

- 允许省略、空串、`"/"`，都视为 `None`；
- 非空时必须按 `store_mgr.rs` 的规则规范化为以 `/` 开头；
- 不在网关层做额外语义扩展，只做安全与格式校验。

### 3.3 `ObjectState` 的 JSON 投影

`store_mgr.query_object_by_id()` 返回的 Rust 枚举不直接透传，统一映射为：

```json
{
  "state": "not_exist"
}
```

或

```json
{
  "state": "object",
  "obj_data": "{...原始对象串...}"
}
```

### 3.4 `ChunkStoreState` 的 JSON 投影

建议统一映射为：

```json
{
  "state": "completed",
  "chunk_size": 1048576
}
```

当状态带额外字段时：

```json
{
  "state": "local_link",
  "chunk_size": 1048576,
  "local_info": {
    "qcid": "xxx",
    "last_modify_time": 1710000000,
    "range": {
      "start": 0,
      "end": 4096
    }
  }
}
```

或

```json
{
  "state": "same_as",
  "chunk_size": 4194304,
  "same_as": "chunklist:..."
}
```

---

## 4. API 清单

### 4.1 对象接口

### 4.1.1 `get_object`

- 路由：`POST /ndm/v1/store/get_object`
- 对应：`NamedStoreMgr::get_object`

请求：

```json
{
  "obj_id": "file:..."
}
```

响应：

```json
{
  "obj_id": "file:...",
  "obj_data": "{...}"
}
```

语义：

- 只接受非 chunk 对象；
- 由 `store_mgr` 负责多 layout 版本回退查找；
- 找不到返回 `404 not_found`。

### 4.1.2 `open_object`

- 路由：`POST /ndm/v1/store/open_object`
- 对应：`NamedStoreMgr::open_object`

请求：

```json
{
  "obj_id": "dir:...",
  "inner_path": "/a/b/c"
}
```

响应：

```json
{
  "obj_data": "{...resolved object...}"
}
```

说明：

- 这是“解析 inner_path 后返回最终对象串”的组合接口；
- 不返回字节流，只返回最终对象的原始字符串表示；
- 如果后续实现需要更强调试能力，可以额外回显 `resolved_obj_id`，但不是本期必需字段。

### 4.1.3 `get_dir_child`

- 路由：`POST /ndm/v1/store/get_dir_child`
- 对应：`NamedStoreMgr::get_dir_child`

请求：

```json
{
  "dir_obj_id": "dir:...",
  "item_name": "photos"
}
```

响应：

```json
{
  "obj_id": "dir:..."
}
```

说明：

- 该接口保留 `store_mgr` 现有语义：若 child 是内嵌对象，Gateway 侧会先落库，再返回 `obj_id`；
- 比纯 `open_object` 更适合逐级浏览目录树。

### 4.1.4 `is_object_stored`

- 路由：`POST /ndm/v1/store/is_object_stored`
- 对应：`NamedStoreMgr::is_object_stored`

请求：

```json
{
  "obj_id": "file:...",
  "inner_path": "/content"
}
```

响应：

```json
{
  "stored": true
}
```

说明：

- 这里的 `stored=true` 不是“根对象存在”而是“目标对象及其强依赖都可用”；
- 对 chunk / chunklist / file / dir 的递归判断逻辑保持与 `store_mgr.rs` 一致。

### 4.1.5 `is_object_exist`

- 路由：`POST /ndm/v1/store/is_object_exist`
- 对应：`NamedStoreMgr::is_object_exist`

请求：

```json
{
  "obj_id": "file:..."
}
```

响应：

```json
{
  "exists": true
}
```

### 4.1.6 `query_object_by_id`

- 路由：`POST /ndm/v1/store/query_object_by_id`
- 对应：`NamedStoreMgr::query_object_by_id`

请求：

```json
{
  "obj_id": "file:..."
}
```

响应：

```json
{
  "state": "object",
  "obj_data": "{...}"
}
```

或：

```json
{
  "state": "not_exist"
}
```

### 4.1.7 `put_object`

- 路由：`POST /ndm/v1/store/put_object`
- 对应：`NamedStoreMgr::put_object`

请求：

```json
{
  "obj_id": "file:...",
  "obj_data": "{...}"
}
```

响应：`204 No Content`

语义：

- 按当前 layout 选择写目标；
- 保持现有 upsert 行为，重复写入同一对象允许覆盖；
- 如果调用方需要“只写不存在对象”，应另加 `if_absent` 扩展，而不是改变现有默认语义。

### 4.1.8 `remove_object`

- 路由：`POST /ndm/v1/store/remove_object`
- 对应：`NamedStoreMgr::remove_object`

请求：

```json
{
  "obj_id": "file:..."
}
```

响应：`204 No Content`

说明：

- 语义保持 best-effort；
- 多 layout 命中多个 store 时由服务端内部处理。

---

### 4.2 Chunk 元数据接口

### 4.2.1 `have_chunk`

- 路由：`POST /ndm/v1/store/have_chunk`
- 对应：`NamedStoreMgr::have_chunk`

请求：

```json
{
  "chunk_id": "chunk:..."
}
```

响应：

```json
{
  "exists": true
}
```

说明：

- `exists=true` 的语义是“可以打开 reader”，即兼容 `Completed` / `LocalLink` / `SameAs`。

### 4.2.2 `query_chunk_state`

- 路由：`POST /ndm/v1/store/query_chunk_state`
- 对应：`NamedStoreMgr::query_chunk_state`

请求：

```json
{
  "chunk_id": "chunk:..."
}
```

响应示例：

```json
{
  "state": "completed",
  "chunk_size": 1048576
}
```

或：

```json
{
  "state": "same_as",
  "chunk_size": 4194304,
  "same_as": "chunklist:..."
}
```

### 4.2.3 `remove_chunk`

- 路由：`POST /ndm/v1/store/remove_chunk`
- 对应：`NamedStoreMgr::remove_chunk`

请求：

```json
{
  "chunk_id": "chunk:..."
}
```

响应：`204 No Content`

### 4.2.4 `add_chunk_by_same_as`

- 路由：`POST /ndm/v1/store/add_chunk_by_same_as`
- 对应：`NamedStoreMgr::add_chunk_by_same_as`

请求：

```json
{
  "big_chunk_id": "chunk:...",
  "chunk_list_id": "chunklist:...",
  "big_chunk_size": 4194304
}
```

响应：`204 No Content`

设计约束：

- `big_chunk_size` 建议保留为必填，保持与 `store_mgr` 现有函数一致；
- 若后续想偷懒由网关推导 size，可以把它降为可选扩展字段，但不建议把推导逻辑写成默认路径。

---

### 4.3 GC / Anchor / 调试接口

这一组接口与 `src/named_store/src/store_http_gateway.rs` 里的 `/_gc/*` 高度相似。为降低重复实现成本，建议 **请求体结构直接沿用已有 JSON 形状**，只把路由前缀统一到 `/ndm/v1/store/`。

### 4.3.1 `apply_edge`

- 路由：`POST /ndm/v1/store/apply_edge`
- 对应：`NamedStoreMgr::apply_edge`
- 请求体：直接使用 `EdgeMsg`
- 响应：`204 No Content`

### 4.3.2 `pin`

- 路由：`POST /ndm/v1/store/pin`
- 对应：`NamedStoreMgr::pin`
- 请求体：直接使用 `PinRequest`
- 响应：`204 No Content`

### 4.3.3 `unpin`

- 路由：`POST /ndm/v1/store/unpin`
- 对应：`NamedStoreMgr::unpin`

请求：

```json
{
  "obj_id": "file:...",
  "owner": "app:alice"
}
```

响应：`204 No Content`

### 4.3.4 `unpin_owner`

- 路由：`POST /ndm/v1/store/unpin_owner`
- 对应：`NamedStoreMgr::unpin_owner`

请求：

```json
{
  "owner": "app:alice"
}
```

响应：

```json
{
  "count": 12
}
```

### 4.3.5 `fs_acquire`

- 路由：`POST /ndm/v1/store/fs_acquire`
- 对应：`NamedStoreMgr::fs_acquire`

请求：

```json
{
  "obj_id": "file:...",
  "inode_id": 1001,
  "field_tag": 1
}
```

响应：`204 No Content`

### 4.3.6 `fs_release`

- 路由：`POST /ndm/v1/store/fs_release`
- 对应：`NamedStoreMgr::fs_release`

请求体与 `fs_acquire` 相同，响应 `204 No Content`。

### 4.3.7 `fs_release_inode`

- 路由：`POST /ndm/v1/store/fs_release_inode`
- 对应：`NamedStoreMgr::fs_release_inode`

请求：

```json
{
  "inode_id": 1001
}
```

响应：

```json
{
  "count": 3
}
```

### 4.3.8 `fs_anchor_state`

- 路由：`POST /ndm/v1/store/fs_anchor_state`
- 对应：`NamedStoreMgr::fs_anchor_state`

请求：

```json
{
  "obj_id": "file:...",
  "inode_id": 1001,
  "field_tag": 1
}
```

响应：

```json
{
  "state": "Pending"
}
```

### 4.3.9 `forced_gc_until`

- 路由：`POST /ndm/v1/store/forced_gc_until`
- 对应：`NamedStoreMgr::forced_gc_until`

请求：

```json
{
  "target_bytes": 104857600
}
```

响应：

```json
{
  "freed_bytes": 104857600
}
```

### 4.3.10 `outbox_count`

- 路由：`POST /ndm/v1/store/outbox_count`
- 对应：`NamedStoreMgr::outbox_count`

请求：

```json
{}
```

响应：

```json
{
  "count": 7
}
```

### 4.3.11 `debug_dump_expand_state`

- 路由：`POST /ndm/v1/store/debug_dump_expand_state`
- 对应：`NamedStoreMgr::debug_dump_expand_state`

请求：

```json
{
  "obj_id": "file:..."
}
```

响应：直接返回 `ExpandDebug` JSON。

### 4.3.12 `anchor_state`

- 路由：`POST /ndm/v1/store/anchor_state`
- 对应：`NamedStoreMgr::anchor_state`

请求：

```json
{
  "obj_id": "file:...",
  "owner": "app:alice"
}
```

响应：

```json
{
  "state": "Materializing"
}
```

---

## 5. `store_mgr.rs` 到 API 的映射表

| `store_mgr.rs` 方法 | 是否纳入 | API |
| --- | --- | --- |
| `get_object` | 是 | `POST /ndm/v1/store/get_object` |
| `open_object` | 是 | `POST /ndm/v1/store/open_object` |
| `get_dir_child` | 是 | `POST /ndm/v1/store/get_dir_child` |
| `is_object_stored` | 是 | `POST /ndm/v1/store/is_object_stored` |
| `is_object_exist` | 是 | `POST /ndm/v1/store/is_object_exist` |
| `query_object_by_id` | 是 | `POST /ndm/v1/store/query_object_by_id` |
| `put_object` | 是 | `POST /ndm/v1/store/put_object` |
| `remove_object` | 是 | `POST /ndm/v1/store/remove_object` |
| `have_chunk` | 是 | `POST /ndm/v1/store/have_chunk` |
| `query_chunk_state` | 是 | `POST /ndm/v1/store/query_chunk_state` |
| `remove_chunk` | 是 | `POST /ndm/v1/store/remove_chunk` |
| `add_chunk_by_same_as` | 是 | `POST /ndm/v1/store/add_chunk_by_same_as` |
| `apply_edge` | 是 | `POST /ndm/v1/store/apply_edge` |
| `pin` | 是 | `POST /ndm/v1/store/pin` |
| `unpin` | 是 | `POST /ndm/v1/store/unpin` |
| `unpin_owner` | 是 | `POST /ndm/v1/store/unpin_owner` |
| `fs_acquire` | 是 | `POST /ndm/v1/store/fs_acquire` |
| `fs_release` | 是 | `POST /ndm/v1/store/fs_release` |
| `fs_release_inode` | 是 | `POST /ndm/v1/store/fs_release_inode` |
| `fs_anchor_state` | 是 | `POST /ndm/v1/store/fs_anchor_state` |
| `forced_gc_until` | 是 | `POST /ndm/v1/store/forced_gc_until` |
| `outbox_count` | 是 | `POST /ndm/v1/store/outbox_count` |
| `debug_dump_expand_state` | 是 | `POST /ndm/v1/store/debug_dump_expand_state` |
| `anchor_state` | 是 | `POST /ndm/v1/store/anchor_state` |
| `open_chunk_reader` | 否 | 流式接口，保留给 data-plane |
| `open_chunklist_reader` | 否 | 流式接口，保留给 data-plane |
| `open_reader` | 否 | 流式接口，保留给 data-plane |
| `get_chunk_data` | 否 | 原始字节接口，不属于结构化控制面 |
| `get_chunk_piece` | 否 | 原始字节接口，不属于结构化控制面 |
| `put_chunk_by_reader` | 否 | 流式写入，继续走 TUS / 上传协议 |
| `put_chunk` | 否 | 原始二进制写入，不放进结构化 API |
| `add_chunk_by_link_to_local_file` | 否 | 依赖服务端本地路径，不应远程暴露 |

---

## 6. 推荐实现方式

### 6.1 路由分发

在 `NamedStoreMgrZoneGateway::route_request()` 中增加：

```rust
if let Some(method) = strip_prefix_segment(&path, "/ndm/v1/store/") {
    if req.method() == Method::POST {
        return self.handle_store_rpc(&method, req).await;
    }
}
```

然后在 `handle_store_rpc()` 中按 `method.as_str()` 做 `match`。

### 6.2 请求体解析

建议每个 handler 使用最小请求结构，而不是直接吃 `serde_json::Value`：

```rust
#[derive(Deserialize)]
struct ObjIdRequest {
    obj_id: String,
}
```

这样可以减少参数名漂移，也便于后续生成客户端。

### 6.3 响应体封装

重点封装两个内部枚举：

1. `ObjectState -> JSON`
2. `ChunkStoreState -> JSON`

这两个适配层建议写成独立函数，避免在每个 handler 里重复判断。

### 6.4 复用现有实现

`store_http_gateway.rs` 里已经有一套成熟的 GC handler，可直接迁移其 JSON 结构和参数校验逻辑。推荐：

- 保持 `EdgeMsg` / `PinRequest` 不变；
- 只调整路由前缀；
- 把通用 helper（如 `collect_body` / `json_response`）沉到底层复用。

---

## 7. 为什么不直接复用 `store_http_gateway.rs`

`store_http_gateway.rs` 更像“裸 named-store / backend 网关”，特点是：

- 同时承载 object GET/PUT 和 chunk GET/PUT；
- URL 更偏资源风格；
- GC 路由挂在 `/_gc/*` 下；
- 不和 Zone Gateway 上传协议放在同一个命名空间里。

而 `ndm_zone_gateway.rs` 的角色已经更偏 **Zone 入口**。因此建议：

- 上传协议继续保留 `/ndm/v1/uploads/*`；
- 结构化控制面统一挂在 `/ndm/v1/store/*`；
- 原始字节下载仍留给未来 `/cyfs/*` 或 `/ndn/*` 数据面。

这样职责更清晰：

- `/ndm/v1/uploads/*`：浏览器友好的 chunk 上传面；
- `/ndm/v1/store/*`：受信调用的结构化 store 控制面；
- `/cyfs/*`、`/ndn/*`：未来数据读取面。

---

## 8. 结论

建议在 `ndm_zone_gateway.rs` 中补一组新的 `POST /ndm/v1/store/{method}` JSON RPC 端点，用来覆盖 `store_mgr.rs` 中所有**非流式、非原始二进制**的方法。这样有几个直接收益：

1. `store_mgr` 的对象树解析、chunk 元数据判断、GC 控制能力都可以通过 Zone Gateway 统一暴露；
2. 不会污染现有 TUS 上传路由；
3. 与 `store_http_gateway.rs` 现有 GC 实现高度兼容，迁移成本低；
4. 为后续实现 `HttpBackend` 的“结构化远程控制面”打下稳定协议基础。
