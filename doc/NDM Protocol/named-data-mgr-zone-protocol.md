# NamedMgr Zone Gateway 协议文档

## 1. 文档定位

本文以 `src/named_store/src/ndm_zone_gateway.rs` 的当前实现为准，整理 `NamedDataMgrZoneGateway` 已经对外暴露的 HTTP 协议。

这份文档描述的是“当前可用协议”，不是目标需求稿。若与历史拆分文档或旧需求稿有差异，以实现行为为准。

从部署角色上看，这个 gateway 可以理解为：

- 每个 Zone 内 Device 的 `NodeGateway` 上都有机会提供的一层 NDM 访问代理；
- 典型调用链是 `Device/Agent -> NodeGateway -> NamedStoreMgr`；
- 它同时承接浏览器友好的上传面和 Zone 内受信的结构化控制面。

当前网关承载两类能力：

- 基于 tus 1.0.0 风格的 chunk 上传协议；
- `/ndm/v1/store/*` 下的一组结构化 NDM/Store 控制面 JSON API。


这里的 upload session 是明确的 `per chunk session`：

- 一次 `POST /ndm/v1/uploads` 只对应一个 chunk；
- 一个文件若被切成多个 chunk，就需要为每个 chunk 分别创建一个 session；
- 文件级上传在当前协议里只是客户端侧编排，不是服务端侧的一等会话对象。

## 2. 总览

### 2.1 监听与基础行为

- 服务类型：`impl HttpServer for NamedDataMgrZoneGateway`
- HTTP 版本：`HTTP/1.1`
- 支持 `X-HTTP-Method-Override`，会优先用该 header 覆盖实际 method
- 所有 JSON 响应均为 `content-type: application/json; charset=utf-8`
- tus 路径上的错误响应会带 `Tus-Resumable: 1.0.0`

### 2.2 路由总表

| 路由 | 方法 | 说明 |
|---|---|---|
| `/ndm/v1/objects/lookup` | `GET` | 上传前对象/Chunk 查重 |
| `/ndm/v1/uploads` | `OPTIONS` | tus 能力发现 |
| `/ndm/v1/uploads` | `POST` | 创建 chunk 上传会话 |
| `/ndm/v1/uploads/{session_id}` | `HEAD` | 查询会话状态 |
| `/ndm/v1/uploads/{session_id}` | `PATCH` | 追加 chunk 数据 |
| `/ndm/v1/store/{method}` | `POST` | 结构化 Store JSON API |


### 2.3 默认配置

`NdmZoneGatewayConfig::default()` 当前默认值：

- `cache_dir`: `/tmp/ndm_upload_cache`
- `session_ttl`: `3600s`
- `default_app_quota`: `512 MiB`

后台 GC 每 `60s` 扫描一次，清理超时的未完成上传会话。

## 3. 通用响应与错误模型

### 3.1 错误响应体

网关统一返回：

```json
{
  "error": "error_code",
  "message": "detail message"
}
```

### 3.2 `NdnError` 到 HTTP 状态码映射

| 内部错误 | HTTP | `error` |
|---|---:|---|
| `NotFound` | `404` | `not_found` |
| `NotReady` | `410` | `session_expired` |
| `InvalidParam` | `400` | `invalid_param` |
| `InvalidData` | `400` | `invalid_data` |
| `InvalidId` | `400` | `invalid_id` |
| `VerifyError` 且包含 `Tus-Resumable` | `412` | `precondition_failed` |
| 其他 `VerifyError` | `409` | `offset_conflict` |
| `PermissionDenied` | `403` | `permission_denied` |
| `AlreadyExists` | `409` | `already_exists` |
| `OffsetTooLarge` | `416` | `offset_too_large` |
| `Unsupported` | `405` | `unsupported` |
| 其他错误 | `500` | `internal_error` |

补充说明：

- `POST /ndm/v1/uploads` 在 `Upload-Length > 32 MiB` 时，直接返回 `413 payload_too_large`
- 当前实现里“配额不足”最终落到 `500 internal_error`，还没有单独映射成 `507`
- `HEAD` 查询已超时会话时，当前实现返回 `404 not_found`；`PATCH` 访问已超时会话时返回 `410 session_expired`

### 3.3 tus 版本协商

以下请求必须带 `Tus-Resumable: 1.0.0`：

- `POST /ndm/v1/uploads`
- `PATCH /ndm/v1/uploads/{session_id}`
- `HEAD /ndm/v1/uploads/{session_id}`

缺失或版本不匹配时返回：

- `412 Precondition Failed`
- tus 错误响应带 `Tus-Resumable: 1.0.0`
- 若是版本不匹配，还会额外带 `Tus-Version: 1.0.0`

## 4. 上传前查重接口

### 4.1 路由

`GET /ndm/v1/objects/lookup?scope={scope}&quick_hash={value}[&inner_path={path}]`

### 4.2 请求参数

| 参数 | 必填 | 说明 |
|---|---|---|
| `scope` | 是 | 仅接受 `app` 或 `global` |
| `quick_hash` | 是 | 当前实现要求它实际上是一个可解析的 `ObjId` 字符串 |
| `inner_path` | 否 | 仅在非 chunk 对象查找时透传给 `is_object_stored` |

### 4.3 实际语义

当前实现不是“任意 quick hash 算法查询”，而是：

1. 先把 `quick_hash` 当作 `ObjId` 解析；
2. 若解析失败，直接返回 `404 not_found`；
3. 若该 `ObjId` 是 chunk：
   - 转为 `ChunkId`
   - 调用 `store_mgr.query_chunk_state`
4. 若该 `ObjId` 不是 chunk：
   - 调用 `store_mgr.is_object_stored(obj_id, inner_path)`

`scope=app` 与 `scope=global` 在当前实现里只体现在响应字段里，还没有分化出不同的鉴权或可见性逻辑。

### 4.4 响应

#### chunk 命中

返回 `200 OK`，格式与 `query_chunk_state` 对齐，并额外附带 `object_id` 与 `scope`：

```json
{
  "state": "completed",
  "chunk_size": 1048576,
  "object_id": "....",
  "scope": "app"
}
```

`state` 可能值：

- `new`
- `completed`
- `disabled`
- `not_exist`
- `local_link`
- `same_as`

其中：

- `local_link` 会返回 `local_info.qcid`、`local_info.last_modify_time`，若有 range 还会返回 `local_info.range.start/end`
- `same_as` 会返回 `same_as`

#### 非 chunk 对象命中

返回 `200 OK`：

```json
{
  "object_id": "....",
  "scope": "app",
  "exists": true
}
```

#### 未命中

返回 `404`：

```json
{
  "error": "not_found",
  "message": "object not found"
}
```

## 5. tus 上传协议

### 5.1 OPTIONS 能力发现

#### 请求

`OPTIONS /ndm/v1/uploads`

#### 响应

`204 No Content`，并带：

- `Tus-Resumable: 1.0.0`
- `Tus-Version: 1.0.0`
- `Tus-Extension: creation,expiration`
- `Tus-Max-Size: 33554432`

### 5.2 上传元数据 `Upload-Metadata`

当前解析器同时支持两种格式：

- tus 标准格式：`key base64value`
- 简化格式：`key=value`

例如：

```text
app_id=my_app,logical_path=docs/readme.txt,chunk_index=0,chunk_hash=...
```

或：

```text
app_id bXlfYXBw,logical_path ZG9jcy9yZWFkbWUudHh0,chunk_index MA==
```

已识别字段如下：

| 字段 | 必填 | 说明 |
|---|---|---|
| `app_id` | 是 | App 命名空间 |
| `logical_path` | 是 | 逻辑路径 |
| `chunk_index` | 否 | 默认 `0` |
| `file_name` | 否 | 仅透传保存 |
| `file_size` | 否 | 仅透传保存 |
| `file_hash` | 否 | 参与幂等 key 与旧会话失效判断 |
| `quick_hash` | 否 | 当前不会用于上传链路内逻辑 |
| `chunk_hash` | 否 | 若是合法 chunk `ObjId`，用于 chunk 级秒传 |
| `mime_type` | 否 | 仅透传保存 |

### 5.3 逻辑路径校验

`logical_path` 必须满足：

- 非空
- 不包含 `..`
- 不以 `/` 开头
- 仅允许 ASCII
- 不允许控制字符
- 不允许反斜杠 `\`

不满足时返回 `400 invalid_param`。

### 5.4 会话标识与状态

当前实现中的“上传会话”始终是 chunk 级，而不是文件级：

- 一个 session 只跟踪一个 chunk 的 `offset/status/temp_file`
- 同一文件的不同 chunk 会拥有不同的 `session_id`
- 文件级上传进度需要由客户端在多个 chunk session 之上自行汇总

#### 5.4.1 `session_id`

服务端生成格式：

```text
us_{unix_millis_hex}_{seq_hex}
```

#### 5.4.2 `canonical_upload_id`

来源规则：

- 若请求头带 `NDM-Upload-ID`，直接使用
- 否则自动生成 `path:{app_id}/{logical_path}`

当前实现还没有自动生成 `oid:{object_id}` 这种模式。

#### 5.4.3 会话唯一键

幂等键为：

```text
(app_id, logical_path, file_hash_or_empty, chunk_index)
```

#### 5.4.4 状态枚举

| 状态 | 说明 |
|---|---|
| `pending` | 会话已创建，尚未写入数据 |
| `uploading` | 已写入部分数据 |
| `completed` | 已写满并成功落入对象存储 |
| `skipped` | chunk 已存在，直接跳过上传 |
| `expired` | 仅内部使用，表示缓存已过期或被淘汰 |

### 5.5 创建上传会话

#### 请求

`POST /ndm/v1/uploads`

请求头：

| Header | 必填 | 说明 |
|---|---|---|
| `Tus-Resumable` | 是 | 必须是 `1.0.0` |
| `Upload-Length` | 是 | chunk 总长度，单位字节 |
| `Upload-Metadata` | 是 | 元数据 |
| `NDM-Upload-ID` | 否 | 自定义业务上传 ID |

请求体为空。

#### 成功响应

新会话创建成功时返回 `201 Created`，并带：

- `Tus-Resumable: 1.0.0`
- `Location: /ndm/v1/uploads/{session_id}`
- `NDM-Upload-ID: {canonical_upload_id}`
- `Upload-Offset: {offset}`
- `Upload-Length: {chunk_size}`
- `NDM-Chunk-Status: pending|completed`
- 活跃会话额外带 `Upload-Expires: {HTTP-date}`

#### 创建流程细节

1. 校验 `Upload-Length`
2. 解析并校验 `Upload-Metadata`
3. 清理同一 `(app_id, logical_path)` 下、`file_hash` 不同的旧未完成会话
4. 若命中相同幂等键的现有会话：
   - `pending/uploading/completed/skipped` 都直接返回已有会话
   - HTTP 状态为 `200 OK`
5. 若提供 `chunk_hash` 且它是合法 chunk `ObjId`，并且 `store_mgr.have_chunk` 为真：
   - 创建一个 `skipped` 会话
   - 返回 `200 OK`
   - `Upload-Offset` 直接等于 `Upload-Length`
   - 返回 `NDM-Chunk-Object-ID`
6. 检查 App 缓存配额，必要时按 LRU 淘汰本 App 的旧活跃会话
7. 为该会话创建临时文件
8. 若 `Upload-Length == 0`：
   - 直接创建 `completed` 会话
   - 删除刚创建的临时文件
   - 返回 `201 Created`

对接侧需要注意：

- 每个 chunk 都需要各自调用一次 `POST /ndm/v1/uploads`
- 之后每个 chunk 再分别走自己的 `HEAD/PATCH`
- 即使某个 chunk 通过 `chunk_hash` 命中秒传，当前实现也会创建一个 `skipped` session，只是无需继续上传数据

#### 特殊错误

| 场景 | 响应 |
|---|---|
| `Upload-Length` 缺失 | `400 invalid_param` |
| `Upload-Length > 32 MiB` | `413 payload_too_large` |
| `Upload-Metadata` 缺失或字段非法 | `400 invalid_param` |
| 逻辑路径不安全 | `400 invalid_param` |
| 配额不足且 LRU 后仍不足 | `500 internal_error` |

### 5.6 查询上传状态

#### 请求

`HEAD /ndm/v1/uploads/{session_id}`

请求头：

- `Tus-Resumable: 1.0.0`

#### 响应头

- `Tus-Resumable: 1.0.0`
- `Upload-Offset`
- `Upload-Length`
- `NDM-Chunk-Status`
- `NDM-Upload-ID`
- `Cache-Control: no-store`
- 若创建时提供过 `Upload-Metadata`，会原样回显 `Upload-Metadata`
- 活跃会话会返回 `Upload-Expires`
- 已有对象 ID 时会返回 `NDM-Chunk-Object-ID`

#### 关键语义

- `HEAD` 不返回 body
- 若会话不存在，返回 `404 not_found`
- 若会话存在但已超出 TTL，当前实现也返回 `404 not_found`
- `completed/skipped` 会话仍可通过 `HEAD` 查询到最终状态

### 5.7 追加上传数据

#### 请求

`PATCH /ndm/v1/uploads/{session_id}`

请求头：

| Header | 必填 | 说明 |
|---|---|---|
| `Tus-Resumable` | 是 | 必须是 `1.0.0` |
| `Content-Type` | 是 | 必须精确等于 `application/offset+octet-stream` |
| `Upload-Offset` | 是 | 客户端认为的当前 offset |

请求体为原始二进制块。

#### 成功响应

未完成时返回 `204 No Content`：

- `Tus-Resumable: 1.0.0`
- `Upload-Offset: {new_offset}`
- `NDM-Chunk-Status: uploading`

写满并落库后返回 `204 No Content`：

- `Tus-Resumable: 1.0.0`
- `Location: /ndm/v1/uploads/{session_id}`
- `NDM-Upload-ID: {canonical_upload_id}`
- `Upload-Offset: {chunk_size}`
- `Upload-Length: {chunk_size}`
- `NDM-Chunk-Status: completed`
- `NDM-Chunk-Object-ID: {chunk_obj_id}`

若会话在进入 `PATCH` 前已经是 `completed/skipped`，服务端也会直接返回当前会话状态，不再写入。

#### 服务端处理逻辑

1. 校验 `Content-Type`
2. 校验 `Upload-Offset`
3. 读取会话并检查：
   - 会话存在
   - 未过期
   - 当前状态不是 `expired`
   - offset 一致
4. 读取整个请求体到内存
5. 要求 body 非空
6. 追加写入临时文件并刷新
7. 更新 `offset` 和状态
8. 若写满：
   - 若提供 `chunk_hash`，将其当作目标 `ChunkId`
   - 否则读取临时文件内容并重新计算 `ChunkId`
   - 调用 `store_mgr.put_chunk_by_reader`
   - 会话标记为 `completed`
   - 删除临时文件

#### 错误行为

| 场景 | 响应 |
|---|---|
| `Content-Type` 不对 | `400 invalid_param` |
| `Upload-Offset` 缺失 | `400 invalid_param` |
| body 为空 | `400 invalid_param` |
| 会话不存在 | `404 not_found` |
| offset 不匹配 | `409 offset_conflict` |
| 会话已过期 | `410 session_expired` |
| `chunk_hash` 不是合法 chunk ID | `400 invalid_param` |
| 本次写入导致超过 `Upload-Length` | `400 invalid_param` |

#### 校验与实现说明

- 当前实现定义了 `Upload-Checksum` header 常量，但没有真正校验 checksum
- 当未提供 `chunk_hash` 时，服务端在完成阶段会重新读取临时文件并计算 `ChunkId`
- 当前实现用写锁保证同一进程内对同一会话的串行写入

### 5.8 缓存、TTL 与淘汰

#### 5.8.1 缓存文件位置

临时文件路径：

```text
{cache_dir}/{app_id}/{session_id}_{chunk_index}.tmp
```

#### 5.8.2 TTL 过期

- 仅 `pending/uploading` 会话会被 TTL 清理
- 判断依据：`now - updated_at > session_ttl`
- 被清理时会：
  - 删除 `sessions`
  - 删除幂等索引 `key_index`
  - 扣减该 App 的缓存占用统计
  - 删除临时文件

#### 5.8.3 LRU 淘汰

- 仅在创建新会话且 App 配额不足时触发
- 候选集为该 App 下所有 `pending/uploading` 会话
- 按 `updated_at` 从旧到新淘汰
- 释放量按每个会话当前 `offset` 计算

#### 5.8.4 旧文件版本失效

当同一 `(app_id, logical_path)` 出现新的 `file_hash` 时，旧的未完成会话会被主动清理。

## 6. `/ndm/v1/store/*` 结构化 Store API

### 6.1 角色与边界

`/ndm/v1/store/*` 这一组接口不是独立于 Zone Gateway 的另一份协议，而是本协议里“结构化控制面”的那一部分。它的定位是：

- 作为 Zone 内 Device 的 `NodeGateway` 可能提供的 NDM 访问代理；
- 给设备进程、Agent、后端 service 或受信 SDK 使用；
- 通过小报文 JSON 暴露 `NamedStoreMgr` 的对象查询、路径解析、chunk 元数据、GC / pin / anchor 等控制面能力。

这组接口默认应被视为 **Zone 内受信调用面**，不等价于公开浏览器上传面。特别是以下能力不应按匿名浏览器接口来理解：

- `put_object`
- `remove_object`
- `remove_chunk`
- `apply_edge`
- `pin` / `unpin`
- `forced_gc_until`
- `fs_*`

同时，它明确 **不覆盖 data-plane**：

- `open_chunk_reader`
- `open_chunklist_reader`
- `open_reader`
- `get_chunk_data`
- `get_chunk_piece`
- `put_chunk_by_reader`
- `put_chunk`
- `add_chunk_by_link_to_local_file`

这些能力要么已经落在 tus 上传链路里，要么应继续放在未来的 `/cyfs/*`、`/ndn/*` 下载/数据面协议中，而不是塞进结构化 JSON RPC。

### 6.2 通用约定

- 统一路由：`POST /ndm/v1/store/{method}`
- 请求体：JSON
- 请求头通常为：
  - `Content-Type: application/json`
  - `Accept: application/json`
- 成功响应：
  - 查询类通常返回 `200 OK + JSON`
  - 写操作通常返回 `204 No Content`
- 非 `POST` 访问该前缀时返回 `405 unsupported`
- 未知 `method` 返回 `404 not_found`
- `obj_id` / `chunk_id` 统一使用字符串；`chunk_id` 在协议上仍按 `ObjId` 字符串传输
- `inner_path` 的 `null`、空串和 `/` 会被规范化为 `None`

### 6.3 对象接口

| 方法 | 请求 JSON | 成功响应 |
|---|---|---|
| `get_object` | `{"obj_id":"..."}` | `200 {"obj_id":"...","obj_data":"..."}` |
| `open_object` | `{"obj_id":"...","inner_path":"..."}` | `200 {"obj_data":"..."}` |
| `get_dir_child` | `{"dir_obj_id":"...","item_name":"..."}` | `200 {"obj_id":"..."}` |
| `is_object_stored` | `{"obj_id":"...","inner_path":"..."}` | `200 {"stored":true}` |
| `is_object_exist` | `{"obj_id":"..."}` | `200 {"exists":true}` |
| `query_object_by_id` | `{"obj_id":"..."}` | `200 {"state":"not_exist"}` 或 `200 {"state":"object","obj_data":"..."}` |
| `put_object` | `{"obj_id":"...","obj_data":"..."}` | `204` |
| `remove_object` | `{"obj_id":"..."}` | `204` |

补充说明：

- `put_object` 与 `remove_object` 不接受 chunk ID；若传入 chunk ID，返回 `400 invalid_param`

### 6.4 Chunk 元数据接口

| 方法 | 请求 JSON | 成功响应 |
|---|---|---|
| `have_chunk` | `{"chunk_id":"..."}` | `200 {"exists":true}` |
| `query_chunk_state` | `{"chunk_id":"..."}` | `200 {"state":"completed","chunk_size":123}` |
| `remove_chunk` | `{"chunk_id":"..."}` | `204` |
| `add_chunk_by_same_as` | `{"big_chunk_id":"...","chunk_list_id":"...","big_chunk_size":123}` | `204` |

`query_chunk_state` 返回体与前文查重接口里的 chunk 状态一致，可能返回：

- `new`
- `completed`
- `disabled`
- `not_exist`
- `local_link`
- `same_as`

### 6.5 GC / Anchor / Debug 接口

| 方法 | 请求 JSON | 成功响应 |
|---|---|---|
| `apply_edge` | `EdgeMsg` | `204` |
| `pin` | `PinRequest` | `204` |
| `unpin` | `{"obj_id":"...","owner":"..."}` | `204` |
| `unpin_owner` | `{"owner":"..."}` | `200 {"count":123}` |
| `fs_acquire` | `{"obj_id":"...","inode_id":1,"field_tag":2}` | `204` |
| `fs_release` | `{"obj_id":"...","inode_id":1,"field_tag":2}` | `204` |
| `fs_release_inode` | `{"inode_id":1}` | `200 {"count":123}` |
| `fs_anchor_state` | `{"obj_id":"...","inode_id":1,"field_tag":2}` | `200 {"state":"..."}` |
| `forced_gc_until` | `{"target_bytes":123}` | `200 {"freed_bytes":123}` |
| `outbox_count` | 任意 JSON | `200 {"count":123}` |
| `debug_dump_expand_state` | `{"obj_id":"..."}` | `200 { ...ExpandDebug... }` |
| `anchor_state` | `{"obj_id":"...","owner":"..."}` | `200 {"state":"..."}` |

说明：

- `apply_edge` 与 `pin` 直接反序列化为内部类型 `EdgeMsg` / `PinRequest`
- `debug_dump_expand_state` 的响应结构直接序列化内部 `ExpandDebug`，字段形状以后续代码为准

### 6.6 `ndm.rs` 能力纳入范围

下表给出 `NamedStoreMgr` 当前方法与协议暴露面的对应关系：

| `ndm.rs` 方法 | 是否纳入 | API / 说明 |
|---|---|---|
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
| `open_chunk_reader` | 否 | 流式读取，属于 data-plane |
| `open_chunklist_reader` | 否 | 流式读取，属于 data-plane |
| `open_reader` | 否 | 流式读取，属于 data-plane |
| `get_chunk_data` | 否 | 原始字节接口，不属于结构化控制面 |
| `get_chunk_piece` | 否 | 原始字节接口，不属于结构化控制面 |
| `put_chunk_by_reader` | 否 | 流式写入，继续走 tus / 上传协议 |
| `put_chunk` | 否 | 原始二进制写入，不放进结构化 API |
| `add_chunk_by_link_to_local_file` | 否 | 依赖服务端本地路径，不应远程暴露 |

## 7. 当前未实现项

### 7.1 下载路径

命中以下前缀时，当前实现统一返回：

- `/cyfs/*`
- `/ndn/*`

响应为 `405 unsupported`，消息大意为：

```text
CYFS get/download not yet implemented in zone gateway, coming in next iteration
```

### 7.2 与需求稿相比的已知差异

以下几点在接入侧需要特别注意：

- `scope=app` 和 `scope=global` 当前没有实际权限差异
- `quick_hash` 当前实际上必须是 `ObjId` 字符串，不是任意快速哈希
- `Upload-Checksum` 还未实现校验
- 配额不足还未映射成 `507`
- `HEAD` 查询超时会话返回 `404`，不是 `410`
- 默认 `canonical_upload_id` 目前只会自动生成 `path:{app_id}/{logical_path}`

## 8. 推荐接入顺序

对接一个完整上传客户端时，建议流程如下：

1. 调 `GET /ndm/v1/objects/lookup`
2. 若未命中，对每个 chunk 调 `POST /ndm/v1/uploads`
3. 若返回 `skipped`，直接记录 `NDM-Chunk-Object-ID`
4. 否则可先 `HEAD` 获取 offset
5. 再持续 `PATCH` 直到 `NDM-Chunk-Status=completed`
6. 收集所有 chunk object id，交给上层对象组装逻辑

这也是当前 `NamedDataMgrZoneGateway` 实现已经稳定支持的最小闭环。

## 9. 标准 tus-client 视角示例

这一节用“标准 tus client 如何接这个协议”的角度说明实际用法。

核心结论只有一条：

- 不要把“整个文件”交给一个 tus upload
- 要先在客户端把文件切成多个 chunk
- 然后为每个 chunk 分别创建一个 tus upload

但在进入 chunk 上传之前，标准接入流程还应该先做一次“整文件级 skip 检查”：

- 客户端先计算文件的 `qcid`，并作为 `quick_hash`
- 调用 `GET /ndm/v1/objects/lookup?scope=app&quick_hash=...`
- 若已命中，则整个文件直接跳过，不再创建任何 chunk session
- 只有未命中时，才进入后续的 per-chunk tus upload

也就是说，当前网关协议更接近：

```text
1 file = N chunks = N tus uploads = N session_id
```

### 9.1 错误示例：把整文件当成一个 tus upload

下面这种思路不适用于当前协议：

```js
new tus.Upload(file, {
  endpoint: "/ndm/v1/uploads",
})
```

原因是：

- `POST /ndm/v1/uploads` 的 `Upload-Length` 语义是“单个 chunk 的长度”
- 服务端会为这次 `POST` 创建一个 chunk 级 session
- 若直接上传整文件，服务端不会替客户端自动切 chunk

### 9.2 正确示例：先切 chunk，再为每个 chunk 创建 tus upload

下面给一个 `tus-js-client` 风格的伪代码示例。重点不是具体库 API，而是接入方式：

```js
import * as tus from "tus-js-client";

const CHUNK_SIZE = 32 * 1024 * 1024;

async function lookupWholeFileByQcid(qcid, scope = "app") {
  const url = `/ndm/v1/objects/lookup?scope=${encodeURIComponent(scope)}&quick_hash=${encodeURIComponent(qcid)}`;
  const resp = await fetch(url, { method: "GET" });

  if (resp.status === 404) {
    return null;
  }

  if (!resp.ok) {
    throw new Error(`lookup failed: ${resp.status}`);
  }

  return await resp.json();
}

function buildMetadata(file, appId, logicalPath, fileHash, chunkIndex, chunkHash) {
  return {
    app_id: appId,
    logical_path: logicalPath,
    file_name: file.name,
    file_size: String(file.size),
    file_hash: fileHash ?? "",
    chunk_index: String(chunkIndex),
    chunk_hash: chunkHash ?? "",
    mime_type: file.type || "application/octet-stream",
  };
}

async function uploadFile(file, appId, logicalPath, qcid, fileHash, chunkHashes = []) {
  const wholeFileHit = await lookupWholeFileByQcid(qcid, "app");

  if (wholeFileHit?.exists || wholeFileHit?.state === "completed") {
    return {
      skippedByQcid: true,
      objectId: wholeFileHit.object_id,
      chunks: [],
    };
  }

  const chunkCount = Math.ceil(file.size / CHUNK_SIZE);
  const chunkResults = [];

  for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex += 1) {
    const start = chunkIndex * CHUNK_SIZE;
    const end = Math.min(file.size, start + CHUNK_SIZE);
    const chunkBlob = file.slice(start, end);

    const result = await uploadOneChunk({
      chunkBlob,
      file,
      appId,
      logicalPath,
      fileHash,
      chunkIndex,
      chunkHash: chunkHashes[chunkIndex],
    });

    chunkResults.push(result);
  }

  return {
    skippedByQcid: false,
    objectId: null,
    chunks: chunkResults,
  };
}

function uploadOneChunk({
  chunkBlob,
  file,
  appId,
  logicalPath,
  fileHash,
  chunkIndex,
  chunkHash,
}) {
  return new Promise((resolve, reject) => {
    const upload = new tus.Upload(chunkBlob, {
      endpoint: "/ndm/v1/uploads",
      retryDelays: [0, 1000, 3000, 5000],
      metadata: buildMetadata(file, appId, logicalPath, fileHash, chunkIndex, chunkHash),
      onError(error) {
        reject(error);
      },
      onSuccess() {
        const xhr = upload._req?.getUnderlyingObject?.();
        resolve({
          chunkIndex,
          uploadUrl: upload.url,
          chunkObjectId: xhr?.getResponseHeader("NDM-Chunk-Object-ID") || null,
        });
      },
    });

    upload.start();
  });
}
```

### 9.3 这个示例在协议层面做了什么

以上代码实际对应的是：

1. 客户端先计算整个文件的 `qcid`
2. 先调用 `GET /ndm/v1/objects/lookup`
3. 若命中：
   - 直接结束
   - 不创建任何 chunk session
4. 若未命中：
   - 再把 `file` 按 `32 MiB` 切块
   - 每个 `chunkBlob` 各自发起一次 `POST /ndm/v1/uploads`
   - 网关为每个 chunk 返回一个独立的 `session_id`
   - tus client 再按标准流程对这个 session 做 `HEAD/PATCH`
   - chunk 完成后，从响应头读取 `NDM-Chunk-Object-ID`
5. 所有 chunk 完成后，客户端再做文件级组装

### 9.4 tus client 接入时要注意的点

- `endpoint` 始终是 `/ndm/v1/uploads`
- 真正区分不同 chunk 的不是 URL，而是每次 `POST` 时提交的 `Upload-Metadata.chunk_index`
- `logical_path` 对同一个文件的所有 chunk 应保持一致
- `file_hash` 对同一个文件的所有 chunk 应保持一致
- `chunk_index` 必须按 chunk 分别填写
- 如果客户端已经有 chunk 内容哈希，建议填写 `chunk_hash`，这样服务端可能直接返回 `skipped`

### 9.5 关于秒传 chunk 的表现

如果某个 chunk 提供了 `chunk_hash`，并且服务端已经存在该 chunk，对标准 tus client 来说通常会表现为：

1. `POST /ndm/v1/uploads`
2. 服务端直接返回 `200 OK`
3. 响应头里有：
   - `NDM-Chunk-Status: skipped`
   - `Upload-Offset: {Upload-Length}`
   - `NDM-Chunk-Object-ID: ...`
4. 这个 chunk 不需要再继续 `PATCH`

也就是说，虽然它仍然创建了一个 chunk session，但这个 session 一创建就是“已完成可复用”的状态。

### 9.6 关于基于 qcid 的整文件 skip

整文件 skip 和 chunk skip 是两层逻辑：

- 第一层是整文件级 skip：
  - 先用 `qcid` 调 `GET /ndm/v1/objects/lookup`
  - 若命中，整个文件上传流程直接结束
  - 此时不会创建任何 chunk session
- 第二层是 chunk 级 skip：
  - 整文件未命中后，单个 chunk 仍可通过 `chunk_hash` 命中
  - 这种情况下会创建 `skipped` session，但不会传 chunk 数据

两者可以同时存在，推荐客户端都实现。

### 9.7 若使用自动续传能力

若 tus client 自带 URL 存储或自动 resume，建议它的 resume key 至少包含：

- `app_id`
- `logical_path`
- `file_hash`
- `chunk_index`

不要只用 `logical_path` 作为恢复键，否则同一路径下新旧文件切换时容易串 session。


------------
基于下面原始文档为实现动机

### Zone 内上传

- 在纯浏览器环境中，使用 `tus` 协议上传。
- 在增强浏览器环境中，使用 `NDM Cache` 实现上传。
- 在有 `cyfs-gateway` 的环境中，使用 `rtcp` 协议实现上传。

> 对应能力是 `named-store` 的 `open_chunk_writer`。

- 在纯浏览器中同步文件夹。

> “同步文件夹”是一个很重的场景，我们并不认为它适合在浏览器单页环境下工作。这里仅用于说明 `cyfs://` 的设计方向。

用户流程：

1. 在浏览器中选择要同步的文件夹。
2. 通过 WebSocket 建立 `rhttp tunnel`。
3. `file-sync-backend` 通过该 `rhttp tunnel`，用标准的 `DirObject Pull` 语义获得更新的文件。

`cyfs rhttp tunnel` 的基本框架是：让“服务器”也可以从“客户端”下载 Chunk 数据。

- tunnel protocol

基于 WebSocket。  
允许服务器通过 tunnel 发送请求（携带 stream session id）；发送请求后，客户端通过 `rhttp stream` 发送响应。

- rhttp stream protocol

客户端使用 `HTTP POST` 连接服务器的特定 URL。  
根据 `HTTP POST Header` 里的字段，找到等待中的 stream session；匹配成功后，将该 stream session 的响应通过 `HTTP POST Body` 发送出去。

基于 `rhttp` 协议，客户端 `upload dir` 的流程会变成 `server download dir`，流程如下：

1. client 访问服务器业务接口，得到 `upload dir` 对应的 tunnel session。
2. server 启动 `dir download session`。
3.1 client 使用 `cyfs rhttp tunnel` 协议与服务器建立 WebSocket 连接。  
3.2 client 在本地运行一个 `ndn_router`，准备处理来自 server 的 `get_obj` 和 `pull_chunk` 请求。  
4. server 的 `dir download session` 创建成功，并持有一个 tunnel session 对象。  
5. server 运行 `dir download logic`，基于该 tunnel 创建 `get_obj` 和 `pull_chunk`。
