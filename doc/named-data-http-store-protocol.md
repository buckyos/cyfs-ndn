# Named Data HTTP Store 协议设计

## 0. 背景与目标

`NamedLocalStore`（`src/named_store/src/local_store.rs`）目前直接调用本地文件系统与 SQLite 数据库实现对象（Object）和数据块（Chunk）的存取。为了让 `NamedLocalStore` 内部的"读写后端"成为可替换组件，并以此为基础实现 `NamedRemoteStore`，需要先抽象出一套**与传输无关**、但**与 HTTP 语义对齐**的存储协议：`named-data-http-store`。

设计目标：
1. 为 `NamedLocalStore` 内部的文件读写行为提供可替换的抽象层（本地实现 = `loopback` HTTP；远程实现 = 真实 HTTP/HTTPS）。
2. 协议表达力覆盖 `local_store.rs` 中所有"对象 + Chunk 读写 + 状态查询"语义。
3. **简化 chunk 写入**：不引入断点续传，一次写入只有两种最终状态——`成功`（Completed）或 `失败`（Aborted/NotExist）。
4. 与已有的 `cyfs_ndn_client` URL/对象寻址习惯保持一致（`{base}/{obj_id}`，或将 `obj_id` 嵌入 host）。
5. 协议自身可被未来的 `NamedRemoteStore` 直接复用，避免出现"本地一套、远程一套"的双轨。

非目标：
- **不**设计任何对象级权限/签名校验机制（这一层由调用方在更高层添加，例如 CYFS Head/JWT）。
- **不**设计断点续传、分片并发上传、CDN 协商等复杂上传策略。
- **不**保留 `local_store.rs` 中 `LocalLink`（`add_chunk_by_link_to_local_file`）相关的本机文件别名功能 —— 这是文件系统专属的优化手段，不进入网络协议层；本地实现仍可在 store 内部走 fast path。

---

## 1. 抽象接口（Rust trait）

协议的语义实体先用一个 trait 描述，HTTP 是它的一种绑定。后续 `NamedLocalStore` 改造为：内部持有一个 `dyn NamedDataStoreBackend`，本地后端走文件系统，远程后端走 HTTP。

```rust
#[async_trait::async_trait]
pub trait NamedDataStoreBackend: Send + Sync {
    // ---- Object ----
    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<String>;
    async fn put_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<()>;

    // ---- Chunk ----
    async fn get_chunk_state(&self, chunk_id: &ChunkId) -> NdnResult<ChunkStateInfo>;

    /// 从 `offset` 起读取 chunk。返回的 reader 在 EOF 处自然结束。
    async fn open_chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> NdnResult<(ChunkReader, u64 /* total chunk size */)>;

    /// 一次性写入 chunk：调用方提供完整数据来源（reader），
    /// 后端要么把它整体落盘并标记 Completed，要么整体丢弃并视作 NotExist。
    /// 没有"半写状态"对外暴露。
    async fn open_chunk_writer(
        &self,
        chunk_id: &ChunkId,
        chunk_size: u64,
        source: ChunkReader,
    ) -> NdnResult<()>;
}

#[derive(Debug, Clone)]
pub struct ChunkStateInfo {
    pub state: ChunkStoreState, // NotExist | Completed
    pub chunk_size: u64,        // Completed 时有效；NotExist 为 0
}
```

注：原 `local_store.rs` 中 `Incompleted / progress` 字段在协议层被移除。`open_chunk_writer` 在协议视角下是原子的。本地实现内部仍可使用 `xxx.tmp` 临时文件 + `rename` 实现原子提交，但这是实现细节，不暴露给调用方。

---

## 2. URL 与资源命名

沿用 `cyfs_ndn_client` 的两种寻址模式：

| 模式 | 形态 | 示例 |
| --- | --- | --- |
| 路径模式 | `{base}/{obj_id}` | `https://store.example/ndn/sha256:abcd...` |
| Host 模式 | `{obj_id_base32}.{base_host}{path}` | `https://abcd...base32.store.example/ndn` |

`obj_id` 字符串遵循 `ObjId::to_string()` / `ObjId::to_base32()` 的现有约定，**Chunk** 同样以 `ObjId` 形式参与寻址（`chunk_id.to_obj_id()`），即 chunk 与普通 object 共用一个命名空间。

资源类型由 `obj_id.is_chunk()` 决定：
- 非 chunk → object 资源（小数据，文本或 JSON/JWT 字符串）
- chunk → chunk 资源（不限大小的二进制流）

如有"集合"操作（HEAD 状态查询、列表、删除等），统一使用同一个 URL，由 HTTP method / Header 区分语义，不在路径里加 `?op=...` 或 `/state` 之类的子路径。这与 RESTful 风格一致，也方便部署在 CDN/反代后。

---

## 3. HTTP 操作映射总表

| 抽象方法 | HTTP 方法 | 关键 Header / Body | 说明 |
| --- | --- | --- | --- |
| `get_object` | `GET {url}` | Resp: `Content-Type: application/cyfs-object`，body 为 `obj_str` | 仅用于非 chunk |
| `put_object` | `PUT {url}` | Req: `Content-Type: application/cyfs-object`，body 为 `obj_str`；`X-CYFS-Obj-Id: {obj_id}` | 幂等 |
| `get_chunk_state` | `HEAD {url}` | Resp: `X-CYFS-Chunk-State`, `Content-Length`（chunk 大小） | 不返回 body |
| `open_chunk_reader` | `GET {url}` | Req: `Range: bytes={offset}-`；Resp: `206`/`200`，`Content-Length` 为剩余字节 | 与 RFC7233 一致，流式传输 |
| `open_chunk_writer` | `PUT {url}` | Req: `Content-Type: application/octet-stream`，`Content-Length: {chunk_size}`，`X-CYFS-Chunk-Size: {chunk_size}` | 一次性，无续传 |
| 删除 chunk/object（可选，本地实现使用） | `DELETE {url}` | — | 不在最小集合中，但保留 |

资源类型由 `obj_id` 自身决定（`obj_id.is_chunk()`），**不需要额外 header**。服务端解析 URL 中的 `obj_id` 后即可判定是 chunk 还是 object，无歧义。

---

## 4. 详细方法定义

### 4.1 `get_object`

```
GET /{obj_id}
Accept: application/cyfs-object
```

**响应**

```
200 OK
Content-Type: application/cyfs-object
Content-Length: {N}
X-CYFS-Obj-Id: {obj_id}

<obj_str N bytes>
```

错误：
- `404 Not Found` → `NdnError::NotFound`
- `403 Forbidden` → `NdnError::PermissionDenied`

**body 形态**：对应 `local_store.get_object` 返回的 `String`，对 JWT 友好，因此这里不强求 JSON 解析，仅保证字节级一致。

### 4.2 `put_object`

```
PUT /{obj_id}
X-CYFS-Obj-Id: {obj_id}
Content-Type: application/cyfs-object
Content-Length: {N}

<obj_str N bytes>
```

**响应**

```
204 No Content
```

或 `200 OK` 携带空 body。`put_object` **必须**幂等：同一 `obj_id` 重复 PUT 完全相同的内容应返回成功。若 `obj_id` 与 body 内容不自洽（例如服务端能验证 hash 时），返回 `409 Conflict` → `NdnError::VerifyError`。

错误：
- `403 Forbidden` → 后端只读
- `409 Conflict` → 内容与 obj_id 校验失败

### 4.3 `get_chunk_state`

```
HEAD /{obj_id}
```

**响应（存在）**

```
200 OK
Content-Length: {chunk_size}
X-CYFS-Chunk-State: completed
X-CYFS-Chunk-Size: {chunk_size}
Accept-Ranges: bytes
```

**响应（不存在）**

```
404 Not Found
X-CYFS-Chunk-State: not_exist
```

不再暴露 `incompleted` / `progress` —— 协议视角只有 `completed` 与 `not_exist`。`X-CYFS-Chunk-State` 仅作为冗余/调试信息，权威依据是 HTTP status code + `Content-Length`。

### 4.4 `open_chunk_reader`

```
GET /{obj_id}
Range: bytes={offset}-
```

**响应（offset = 0）**

```
200 OK
Content-Type: application/octet-stream
Content-Length: {chunk_size}
X-CYFS-Chunk-Size: {chunk_size}
Accept-Ranges: bytes

<bytes ...>
```

**响应（offset > 0）**

```
206 Partial Content
Content-Type: application/octet-stream
Content-Range: bytes {offset}-{chunk_size-1}/{chunk_size}
Content-Length: {chunk_size - offset}
X-CYFS-Chunk-Size: {chunk_size}

<bytes ...>
```

错误：
- `404 Not Found` → chunk 不存在
- `416 Range Not Satisfiable` → `offset > chunk_size`，对应 `NdnError::OffsetTooLarge`
- 服务端如需做完整性校验失败上报：在传输尾部用 `Trailer: X-CYFS-Verify` + `X-CYFS-Verify: failed` 表达；客户端遇到该 trailer 应丢弃数据并返回 `NdnError::VerifyError`。

注意：客户端拿到 `(reader, total_size)` 与 trait 签名一致 —— `total_size` 即 `X-CYFS-Chunk-Size`（不是 `Content-Length`，因为带 offset 时 `Content-Length` 是剩余长度）。

**流式传输**：服务端 **必须** 以流式方式发送 chunk body（边读边发），不得将整个 chunk 缓冲到内存后再发送。客户端同样应以流式方式消费响应 body。这对大 chunk（最大 32MB）的内存占用至关重要。

### 4.5 `open_chunk_writer`（一次性写入，无续传）

```
PUT /{obj_id}
X-CYFS-Chunk-Size: {chunk_size}
Content-Type: application/octet-stream
Content-Length: {chunk_size}
Expect: 100-continue   ; 可选，但推荐

<bytes chunk_size 字节>
```

**关键语义（务必遵守）**

1. **不接受 `Range` / `Content-Range` 头**。任何带 `Range` 的 PUT 都返回 `400 Bad Request`，避免与"无续传"语义冲突。
2. **不接受 chunked transfer**。`Content-Length` 必须出现，并必须等于 `X-CYFS-Chunk-Size`。否则 `411 Length Required` 或 `400`。这样服务端在写入前就能拒绝大小不符的请求，避免半写。
3. 服务端在收到完整 body 之前，**对外**始终视该 chunk 为 `not_exist`。`get_chunk_state` 在写入过程中必须返回 404，不暴露任何中间态。
4. 服务端 **必须** 做端到端校验：根据 `chunk_id` 的 hash 类型重算 hash 并与 `obj_id` 对比。校验失败的处理：
   - 不创建/不替换最终对象。
   - 返回 `409 Conflict`，body 可携带 `{"error":"verify_failed", ...}`，对应 `NdnError::VerifyError`。
5. **原子可见性**：成功写入的最后一步必须是原子操作（本地实现：`rename(tmp, final)`；对象存储后端：multipart commit 或 server-side copy）。在该步骤完成之前，并发的 `open_chunk_reader`/`get_chunk_state` 必须看到 `not_exist`；之后必须看到 `completed`。
6. **失败即清理**：传输中断、客户端断开、校验失败、磁盘错误等任何分支，服务端都必须保证 chunk 最终状态为 `not_exist`（清理临时文件、不更新元数据）。
7. **`Expect: 100-continue` 推荐**：客户端先发头，服务端检查 `chunk_id` 是否已存在或 `chunk_size` 是否合法，再用 `100 Continue` 放行 body。已存在的 chunk 服务端可直接回 `200 OK` + `X-CYFS-Chunk-Already: 1` 跳过 body（`PUT` 幂等）。

**响应**

```
201 Created
X-CYFS-Chunk-Size: {chunk_size}
X-CYFS-Obj-Id: {obj_id}
```

或当 chunk 已存在时：

```
200 OK
X-CYFS-Chunk-Already: 1
X-CYFS-Chunk-Size: {chunk_size}
```

错误码映射：

| HTTP | 含义 | NdnError |
| --- | --- | --- |
| 400 | 请求中带 Range / 缺少 X-CYFS-Chunk-Size | `InvalidParam` |
| 403 | store 只读 | `PermissionDenied` |
| 409 | hash 校验失败 | `VerifyError` |
| 411 | 缺 Content-Length | `InvalidParam` |
| 413 | chunk 超过服务器限制 | `LimitExceeded` |
| 5xx | 服务端 IO/网络 | `IoError` |

> 注：`local_store.rs` 中 `open_chunk_writer` 旧签名返回 `(ChunkWriter, progress)` 暴露了"半写"概念。新协议有意把它收回到调用方内部，调用方需要把"准备一个 reader 一次写完"作为唯一模式。如果调用方手里只有一个增量 writer（例如边接收边写盘），可以在客户端用 pipe（`tokio::io::duplex`）把 writer 端转成 reader 端再调用 `open_chunk_writer`。

### 4.6 删除（可选 / 内部用）

```
DELETE /{obj_id}
```

`200 OK` 或 `204 No Content`，幂等；不存在也返回 `204`。`NamedLocalStore::remove_chunk` / `remove_object` 走这个接口。

---

## 5. 错误响应通用格式

所有 4xx/5xx 在条件允许时返回 JSON：

```json
{
  "error": "verify_failed",
  "message": "chunk hash mismatch",
  "obj_id": "sha256:abcd..."
}
```

`error` 字段为枚举字符串（见各方法），客户端按字段映射 `NdnError`。无 body 时退化为根据 status code 映射。

---

## 6. 并发与一致性约定

1. `put_object`、`open_chunk_writer` 均为幂等。客户端重试是允许的。
2. 同一 chunk 的两个并发 `open_chunk_writer`：服务端必须串行化最终提交，并保证最后一个成功的写入版本与 `chunk_id` 自洽（因为有 hash 校验，所有合法版本字节相同）。
3. 写入未完成期间禁止任何外部可见的"半态"（见 4.5#3 / #5）。
4. `open_chunk_reader` 在 chunk 进入 `completed` 后才可成功；`completed` 后字节流不可变。

---

## 7. `NamedLocalStore` 改造说明（实现路线，不改协议）

1. 抽出 `NamedDataStoreBackend` trait（见 §1）。
2. 新建 `LocalFsBackend`：把现有 `local_store.rs` 中所有直接 `tokio::fs::*` / `OpenOptions::*` 的代码搬进来，作为 trait 的本地实现。`open_chunk_writer` 的实现仍然走 `tmp + rename`，但**对外暴露 reader-入参 接口**而非 writer-返回 接口；旧 `open_chunk_writer(chunk_id, size, offset)` 在 store 内部仍可保留为 private helper，不再出现在 trait 上。
3. 新建 `HttpBackend`：实现同一个 trait，按 §3-§5 发起 HTTP 请求。`NamedRemoteStore` 即是 `NamedLocalStore { backend: HttpBackend, db: 远端无 }`，或者干脆直接用 `HttpBackend`。
4. `NamedLocalStore` 的现有 public API 保持兼容；`Incompleted` 状态在元数据 DB 中变成实现细节（写入过程中临时存在，对外查询时一律折叠为 `NotExist`）。
5. `LocalLink`/`add_chunk_by_link_to_local_file` 不进入 trait，仅 `LocalFsBackend` 自带的扩展方法，由 `NamedLocalStore` 在调用方判断后端类型后调用。

---

## 8. 与现有 `cyfs_ndn_client` 的关系

- `cyfs_ndn_client` 当前是面向"语义对象/文件/chunk-list"的高层客户端，会一次性 pull 一个 FileObject 及其所有 chunk。
- 本协议是面向**单个对象/单个 chunk**的低层 store 协议，是 `cyfs_ndn_client` 的一种可能后端。
- 二者并不冲突：未来 `cyfs_ndn_client` 在 pull 完成后调用 `NamedStoreMgr.put_object / put_chunk_by_reader`，后者在 `HttpBackend` 模式下，就会把数据按本协议 PUT 到一个远程 `named-data-http-store` 服务上 —— 这就直接构成了 `NamedRemoteStore`。

---

## 9. 待定 / 可扩展

- **批量 HEAD**（一次查询多个 chunk 状态）：可加 `POST /_state` + JSON 数组。本期不做。
- **签名 / 鉴权**：留空。生产部署应在前面套 reverse proxy 或在 Header 加 `Authorization`。
- **压缩**：协议层不强制；如果客户端/服务端协商 `Content-Encoding`，必须在校验 hash **之前**做解码。

---

## 10. GC 控制面协议（`/_gc/` 端点）

数据面（§3-§6）只覆盖 object/chunk 的 CRUD。跨 bucket 的 GC 引用边传播、pin/unpin 等操作属于**控制面**，统一挂在 `/_gc/` 路径前缀下。

所有 GC 端点的请求和响应 body 均为 `application/json`。

### 10.1 URL 命名空间

```
{base}/_gc/{operation}
```

例如 `https://store.example/ndn/_gc/edge`。

### 10.2 GC 操作映射总表

| 操作 | HTTP 方法 | URL | 请求 Body | 响应 |
|---|---|---|---|---|
| `apply_edge` | `POST /_gc/edge` | `EdgeMsg` JSON | `204 No Content` |
| `pin` | `POST /_gc/pin` | `PinRequest` JSON | `204 No Content` |
| `unpin` | `POST /_gc/unpin` | `{"obj_id":"...","owner":"..."}` | `204 No Content` |
| `unpin_owner` | `POST /_gc/unpin_owner` | `{"owner":"..."}` | `{"count": N}` |
| `fs_acquire` | `POST /_gc/fs_acquire` | `{"obj_id":"...","inode_id":N,"field_tag":N}` | `204 No Content` |
| `fs_release` | `POST /_gc/fs_release` | `{"obj_id":"...","inode_id":N,"field_tag":N}` | `204 No Content` |
| `outbox_count` | `GET /_gc/outbox_count` | (无) | `{"count": N}` |
| `expand_state` | `GET /_gc/expand_state/{obj_id}` | (无) | `ExpandDebug` JSON |
| `anchor_state` | `GET /_gc/anchor_state/{obj_id}?owner=...` | (无) | `{"state":"..."}` |

### 10.3 `apply_edge` — 跨 bucket 边消息投递

```
POST /_gc/edge
Content-Type: application/json

{
  "op": "add",            // "add" | "remove"
  "referee": "sha256:...",  // 被引用的 child obj_id
  "referrer": "cydir:...",  // 发起引用的 parent obj_id
  "target_epoch": 1         // 声明 bucket 的 epoch
}
```

**响应**：`204 No Content`。

这是 outbox sender 的投递目标。bucket A 的 outbox sender 把本地 `edge_outbox` 中到期的条目通过此端点发到 bucket B（referee 的 home bucket）。服务端收到后执行 `apply_edge` 事务（见 `named_store_gc.md` §5.4）：

1. `upsert_shadow_if_absent(referee)`;
2. 插入/删除 `incoming_refs`;
3. `recompute_eviction_class` + `reconcile_expand_state`。

**幂等**：重复投递同一条边消息是安全的。

### 10.4 `pin` — 远程 pin 对象

```
POST /_gc/pin
Content-Type: application/json

{
  "obj_id": "sha256:...",
  "owner": "my-app",
  "scope": "recursive",     // "recursive" | "skeleton" | "lease"
  "ttl_secs": 3600           // 可选，null 表示永不过期
}
```

**响应**：`204 No Content`。

服务端根据 `obj_id` 路由到对应 bucket 并调用 `pin(obj_id, owner, scope, ttl)`。

### 10.5 `unpin` — 远程 unpin 对象

```
POST /_gc/unpin
Content-Type: application/json

{
  "obj_id": "sha256:...",
  "owner": "my-app"
}
```

**响应**：`204 No Content`。

### 10.6 `unpin_owner` — 按 owner 批量 unpin

```
POST /_gc/unpin_owner
Content-Type: application/json

{
  "owner": "my-app"
}
```

**响应**：

```json
{"count": 5}
```

服务端遍历所有 bucket，删除该 owner 的全部 pin。返回被删除的总数。

### 10.7 `fs_acquire` / `fs_release` — 文件系统 anchor

```
POST /_gc/fs_acquire
Content-Type: application/json

{
  "obj_id": "sha256:...",
  "inode_id": 12345,
  "field_tag": 0
}
```

```
POST /_gc/fs_release
Content-Type: application/json

{
  "obj_id": "sha256:...",
  "inode_id": 12345,
  "field_tag": 0
}
```

**响应**：均为 `204 No Content`。

### 10.8 观测端点

**`GET /_gc/outbox_count`**

返回所有 bucket 的 outbox 条目总数。

```json
{"count": 42}
```

**`GET /_gc/expand_state/{obj_id}`**

返回对象在 GC 模型中的完整状态（调试用）。

```json
{
  "obj_id": "sha256:...",
  "state": "present",
  "eviction_class": 2,
  "children_expanded": true,
  "fs_anchor_count": 1,
  "incoming_refs_count": 0,
  "has_recursive_pin": true,
  "has_skeleton_pin": false,
  "has_lease_pin": false,
  "owned_bytes": 1024,
  "logical_size": 1024,
  "last_access_time": 1712700000
}
```

**`GET /_gc/anchor_state/{obj_id}?owner=my-app`**

返回特定 (obj_id, owner) 对的 cascade state。

```json
{"state": "Materializing"}
```

### 10.9 错误格式

GC 端点复用 §5 的通用错误格式：

```json
{
  "error": "not_found",
  "message": "no store for referee sha256:..."
}
```

错误码与数据面一致（`not_found`, `invalid_param`, `invalid_data`, `permission_denied` 等）。

### 10.10 客户端实现

- **`HttpGcClient`**（`http_gc_client.rs`）：封装所有 `/_gc/` 端点的 reqwest 客户端。
- **`HttpEdgeRouter`**（`outbox_sender.rs`）：实现 `EdgeRouter` trait，把 outbox 条目通过 `HttpGcClient::apply_edge()` 投递到远端。
- **`MgrEdgeRouter`**（`outbox_sender.rs`）：实现 `EdgeRouter` trait，通过 `NamedStoreMgr::apply_edge()` 在同机多 bucket 间路由。
