# NamedDataMgr Proxy Protocol 测试用例设计

## 1. 目标

本文基于 [named-data-mgr-proxy-protocol.md](./NDM%20Protocol/named-data-mgr-proxy-protocol.md) 设计一组面向实现落地的测试用例，重点覆盖以下高风险点：

- 协议边界：路由前缀、HTTP method、Header 约束、返回码与错误体格式。
- 参数边界：ID 编码、`inner_path` 规范化、offset / piece_size / chunk_size 的边界值。
- 语义边界：对象接口与 chunk 接口的类型隔离、流式读写的总长度语义、原子写入语义。
- 常见错误：错误码映射错误、短读被误判成功、写入时未校验 `chunk_id`、受限接口默认暴露。

本文不替代协议设计文档，而是把协议中“最容易实现错”和“客户端最依赖稳定行为”的点转成可执行测试集合。

## 2. 测试策略

### 2.1 分层

- L1 通用协议层：路由、HTTP method、`Content-Type`、统一错误体、`NdnError -> HTTP` 映射。
- L2 JSON RPC 层：对象类接口、chunk 元数据接口、受限控制面接口。
- L3 流式读层：`open` / `data` / `piece` / `chunklist` / `object` 的 Header 与 body 语义。
- L4 流式写层：chunk 原子写入、重复写入、内容校验、Header 校验与早返回语义。
- L5 端到端语义层：对象查到 reader、chunklist 拼接、权限隔离、接口间一致性。

### 2.2 优先级

- P0：实现错误会直接导致协议不互通、数据错误或权限失效。
- P1：实现错误会导致行为偏差、错误诊断困难或兼容性下降。
- P2：协议未完全展开，但建议提前固化为测试，避免后续语义漂移。

### 2.3 测试环境建议

- 部署方式至少覆盖两种：
  - loopback HTTP
  - 受控 NodeGateway 内网入口
- 权限配置至少覆盖两种：
  - 仅开放普通能力
  - 显式开放受限能力
- 数据集至少覆盖：
  - 空对象与小对象
  - 单 chunk 文件
  - 多 chunk 的 chunklist 文件
  - 不存在对象 / chunk
  - 已存在 chunk
  - same_as chunk
  - local_link chunk

### 2.4 边界值样本

- `inner_path`：`null`、`""`、`"/"`、`"/a"`、`"/a/b"`、非法路径字符串
- `offset`：`0`、`chunk_size - 1`、`chunk_size`、`chunk_size + 1`
- `piece_size`：`1`、`4096`、`chunk_size`、`chunk_size + 1`
- `chunk_size`：`0`、`1`、`4096`、`32MiB - 1`、`32MiB`、`32MiB + 1`
- HTTP body 场景：
  - 空 body
  - 非法 JSON
  - 缺字段
  - 字段类型错误
  - Header 缺失
  - Header 与 body 不一致

## 3. 覆盖重点

| 模块 | 风险点 | 典型错误 |
| --- | --- | --- |
| 通用协议 | 路由、method、统一错误体 | 错把 method 错误返回 404；错误体不是 JSON |
| ID 与参数 | `obj_id` / `chunk_id` 类型检查，`inner_path` 归一化 | chunk id 被对象接口接受；`null` 与 `"/"` 行为不一致 |
| 对象 RPC | 对象类与 chunk 类语义隔离 | `put_object` 接受 chunk id，破坏对象语义 |
| chunk 元数据 RPC | 状态返回 shape 与扩展字段 | `same_as` / `local_link` 缺额外字段 |
| 流式读 | `NDM-Total-Size`、`NDM-Offset`、定长读取 | `piece` 短读后返回 200；总长度返回剩余长度 |
| 流式写 | 原子写入、`chunk_id` 校验、重复写策略 | 内容与 `chunk_id` 不匹配仍成功；部分写入可见 |
| 权限模型 | 受限接口默认关闭 | 普通入口能直接 `forced_gc_until` 或 `pin` |

## 4. 详细测试用例

## 4.1 通用协议与错误模型

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| GEN-01 | P0 | 基础前缀固定为 `/ndm/proxy/v1` | 服务已启动 | 请求 `/ndm/proxy/v1/rpc/get_object` | 路由可命中 |
| GEN-02 | P0 | 未知路由返回 `404 not_found` | 服务已启动 | 请求 `/ndm/proxy/v1/rpc/not_exists` | HTTP 404，错误体为 `{ "error": "not_found", "message": ... }` |
| GEN-03 | P0 | 已知路由但 method 不匹配返回 `405 unsupported` | 服务已启动 | 对 `/rpc/get_object` 发 `GET`；对 `/write/chunk/{id}` 发 `POST` | HTTP 405，`error=unsupported` |
| GEN-04 | P0 | 所有错误响应统一为 JSON | 任选一个非法请求 | 构造非法 JSON 或非法参数 | `Content-Type=application/json; charset=utf-8`，错误体 shape 固定为 `error/message` |
| GEN-05 | P0 | 非法 JSON 请求映射到 400 类错误 | 服务已启动 | 向任意 `POST /rpc/*` 发送截断 JSON | HTTP 400，`error` 为 `invalid_data` 或实现约定的 400 类错误码，不得返回 500 |
| GEN-06 | P0 | 必填字段缺失映射到 `invalid_param` | 服务已启动 | 向 `get_object` 发送 `{}` | HTTP 400，`error=invalid_param` |
| GEN-07 | P0 | ID 字段格式非法映射到 `invalid_id` | 服务已启动 | 向 `get_object` 或 `have_chunk` 发送非法 ID 字符串 | HTTP 400，`error=invalid_id` |
| GEN-08 | P1 | `NdnError -> HTTP` 映射完整 | 能构造各类底层错误 | 分别触发 `NotFound`、`VerifyError`、`AlreadyExists`、`OffsetTooLarge`、`PermissionDenied` | HTTP code 与 `error` 字段符合协议表 |
| GEN-09 | P1 | `application/json` 请求编码稳定 | 服务已启动 | 发送 `Content-Type: application/json; charset=utf-8` 与 `application/json` | 二者行为一致 |
| GEN-10 | P1 | 未声明的额外字段不破坏正常处理 | 服务已启动 | 在合法请求中添加无关字段 | 服务端忽略额外字段或按实现规则处理，但不得导致错误语义漂移 |

## 4.2 对象类 JSON RPC

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| OBJ-01 | P0 | `get_object` 成功返回 `obj_id` 与 `obj_data` | 准备已存在普通对象 | 调用 `POST /rpc/get_object` | HTTP 200，响应包含 `obj_id`、`obj_data` |
| OBJ-02 | P0 | `get_object` 查询不存在对象 | 不存在目标对象 | 调用 `get_object` | HTTP 404，`error=not_found` |
| OBJ-03 | P0 | `open_object` 的 `inner_path` 规范化 | 准备支持 `inner_path=None` 的对象 | 分别用 `null`、`""`、`"/"` 调用 `open_object` | 三次行为一致，等价于 `inner_path=None` |
| OBJ-04 | P0 | `open_object` 正常解析嵌套路径 | 准备包含子路径的对象 | 用 `"/a/b"` 调用 `open_object` | 返回对应 `obj_data` |
| OBJ-05 | P0 | `open_object` 非法 `inner_path` 返回 400 | 服务已启动 | 发送非法路径编码或错误类型 | HTTP 400，`error=invalid_param` 或 `invalid_data` |
| OBJ-06 | P0 | `get_dir_child` 正常返回子对象 ID | 准备目录对象 | 用合法 `dir_obj_id` + `item_name` 调用 | HTTP 200，返回 `{ "obj_id": "..." }` |
| OBJ-07 | P0 | `get_dir_child` 访问不存在子项 | 准备目录对象 | 查询不存在 `item_name` | HTTP 404，`error=not_found` |
| OBJ-08 | P0 | `is_object_stored` 对完整对象返回 true | 准备完整对象与依赖 | 调用 `is_object_stored` | 响应 `{ "stored": true }` |
| OBJ-09 | P0 | `is_object_stored` 对缺依赖对象返回 false | 准备对象元数据存在但内容不完整 | 调用 `is_object_stored` | HTTP 200，`stored=false`，而不是 404 |
| OBJ-10 | P0 | `is_object_exist` 与 `query_object_by_id` 的 not_exist 语义一致 | 准备不存在对象 | 分别调用两个接口 | `is_object_exist.exists=false`；`query_object_by_id.state=not_exist` |
| OBJ-11 | P0 | `query_object_by_id` 返回 object 态 | 准备已存在对象 | 调用 `query_object_by_id` | 返回 `{ "state": "object", "obj_data": ... }` |
| OBJ-12 | P0 | `put_object` 成功写入 | 准备一个未存在普通对象 | 调用 `put_object`，随后 `get_object` | `put_object` 返回 204，后续可读回相同对象 |
| OBJ-13 | P0 | `put_object` 不接受 chunk id | 准备一个合法 chunk id 串 | 用 chunk id 调用 `put_object` | HTTP 400，`error=invalid_obj_type` 或等价 400 类错误 |
| OBJ-14 | P0 | `remove_object` 成功删除 | 准备已存在普通对象 | 调用 `remove_object` 后再 `is_object_exist` | `remove_object` 返回 204，后续 `exists=false` |
| OBJ-15 | P0 | `remove_object` 不接受 chunk id | 准备一个合法 chunk id 串 | 用 chunk id 调用 `remove_object` | HTTP 400，不能误删 chunk |
| OBJ-16 | P1 | `put_object` 重复写同一对象 | 准备已存在相同对象 | 重复调用 `put_object` | 行为与 `NamedDataMgr::put_object` 一致；不得产生损坏或异常 500 |

## 4.3 Chunk 元数据 JSON RPC

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| CHK-01 | P0 | `have_chunk` 对已存在 chunk 返回 true | 准备已存在 chunk | 调用 `have_chunk` | 响应 `{ "exists": true }` |
| CHK-02 | P0 | `have_chunk` 对不存在 chunk 返回 false | 不存在目标 chunk | 调用 `have_chunk` | 响应 `{ "exists": false }` |
| CHK-03 | P0 | `query_chunk_state` 的 `completed` 形状 | 准备完整 chunk | 调用 `query_chunk_state` | 返回 `{ "state": "completed", "chunk_size": n }` |
| CHK-04 | P0 | `query_chunk_state` 的 `not_exist` 形状 | 不存在目标 chunk | 调用 `query_chunk_state` | 返回 `{ "state": "not_exist" }` 或实现等价形状，不得误报 200 completed |
| CHK-05 | P0 | `query_chunk_state` 的 `same_as` 扩展字段 | 准备 same_as chunk | 调用 `query_chunk_state` | 返回 `state=same_as` 且包含 `same_as` 字段 |
| CHK-06 | P0 | `query_chunk_state` 的 `local_link` 扩展字段 | 准备 local_link chunk | 调用 `query_chunk_state` | 返回 `state=local_link` 且包含 `local_info` 字段 |
| CHK-07 | P0 | `remove_chunk` 成功删除 | 准备已存在 chunk | 调用 `remove_chunk` 后再 `have_chunk` | `remove_chunk` 返回 204，后续 `exists=false` 或状态变为 `not_exist` |
| CHK-08 | P0 | `add_chunk_by_same_as` 参数合法时成功 | 准备已有 `chunk_list_id` 与大 chunk 目标 | 调用 `add_chunk_by_same_as` | 返回 204，后续状态可查为 `same_as` |
| CHK-09 | P0 | `add_chunk_by_same_as` 缺字段返回 400 | 服务已启动 | 省略 `big_chunk_size` | HTTP 400，`error=invalid_param` |
| CHK-10 | P0 | `add_chunk_by_same_as` 中 `big_chunk_size=0` 的边界 | 服务已启动 | 发送 `big_chunk_size=0` | 依据实现语义返回成功或 400，但行为必须固定且可回归；不得 silent accept 又产生无效状态 |
| CHK-11 | P1 | `have_chunk` 与 `query_chunk_state` 一致性 | 准备 completed / same_as / not_exist 三类 chunk | 分别调用两个接口 | `have_chunk` 是否为 true 与状态语义一致，不得互相矛盾 |

## 4.4 流式读接口

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| READ-01 | P0 | 所有成功读接口返回 `application/octet-stream` | 准备可读对象 | 分别调用 `chunk/open`、`chunk/data`、`chunk/piece`、`chunklist/open`、`object/open` | `Content-Type=application/octet-stream` |
| READ-02 | P0 | 所有成功读接口返回 `NDM-Total-Size` | 准备可读对象 | 调用任一读接口 | Header 中存在 `NDM-Total-Size` |
| READ-03 | P0 | `read/chunk/open` 从 offset=0 开始读完整 chunk | 准备完整 chunk | 调用 `chunk/open offset=0` 并读到 EOF | body 内容与源 chunk 一致，`NDM-Total-Size=chunk 总长` |
| READ-04 | P0 | `read/chunk/open` 的 offset 边界值 | 准备大小为 N 的 chunk | 分别用 `0`、`N-1`、`N`、`N+1` 调用 | `0` 和 `N-1` 成功；`N` 是否允许返回空流需实现固定；`N+1` 返回 416 `offset_too_large` |
| READ-05 | P0 | `read/chunk/open` 返回偏移头 | 准备大小为 N 的 chunk | 用 `offset>0` 调用 | 成功时返回 `NDM-Offset=offset` |
| READ-06 | P0 | `read/chunk/data` 等价于 `chunk/open(offset=0)` | 准备完整 chunk | 分别调用两个接口 | body 完全相同，`NDM-Total-Size` 相同 |
| READ-07 | P0 | `read/chunk/piece` 正常返回定长数据 | 准备大小大于 `piece_size` 的 chunk | 调用 `chunk/piece` | HTTP 200，`Content-Length=piece_size`，body 长度等于 `piece_size` |
| READ-08 | P0 | `read/chunk/piece` 不允许短读成功 | 准备大小为 N 的 chunk | 用 `offset=N-1, piece_size=2` 调用 | 返回错误，不得返回 200 + 1 字节 |
| READ-09 | P0 | `read/chunk/piece` 越界返回 416 | 准备大小为 N 的 chunk | 用 `offset=N+1` 调用 | HTTP 416，`error=offset_too_large` |
| READ-10 | P0 | `read/chunklist/open` 返回逻辑拼接流 | 准备包含多个 chunk 的 chunklist | 调用 `chunklist/open offset=0` 并读取 | body 为按顺序拼接后的完整数据流 |
| READ-11 | P0 | `read/chunklist/open` 的 `NDM-Reader-Kind=chunklist` | 准备 chunklist | 调用 `chunklist/open` | Header 中 `NDM-Reader-Kind=chunklist` |
| READ-12 | P0 | `read/chunklist/open` 的 `NDM-Total-Size` 为逻辑总长度 | 准备多 chunk chunklist | 调用 `chunklist/open` | `NDM-Total-Size` 等于拼接后的总长度，而不是当前子 chunk 长度 |
| READ-13 | P0 | `read/object/open` 支持 `inner_path` 规范化 | 准备可打开 reader 的对象 | 分别用 `null`、`""`、`"/"` 调用 `object/open` | 三次结果一致 |
| READ-14 | P0 | `read/object/open` 能解析到最终 reader | 准备 `file object -> chunk/chunklist` 样本 | 调用 `object/open` | 返回最终内容流 |
| READ-15 | P0 | `read/object/open` 返回 `NDM-Resolved-Object-ID` | 准备能落到 reader 的对象 | 调用 `object/open` | Header 中存在最终 reader 对象 ID |
| READ-16 | P1 | `Content-Length` 与实际返回字节一致 | 准备稳定样本 | 检查 `chunk/data` 与 `chunk/piece` 的 header/body | `Content-Length` 不得与实际 body 长度不一致 |
| READ-17 | P1 | 不存在对象 / chunk 的读请求返回 404 | 不存在目标 | 调用各读接口 | HTTP 404，`error=not_found` |
| READ-18 | P1 | 非法 ID 的读请求返回 400 | 服务已启动 | 用非法 `chunk_id` / `obj_id` 请求读接口 | HTTP 400，`error=invalid_id` |

## 4.5 流式写接口

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| WRITE-01 | P0 | 路径格式为 `PUT /write/chunk/{chunk_id}` | 服务已启动 | 对目标路径发送合法 PUT | 路由可命中 |
| WRITE-02 | P0 | `Content-Type` 必须为 `application/octet-stream` | 服务已启动 | 使用错误 `Content-Type` 发 PUT | 返回 400 或 405 类错误，不得写入成功 |
| WRITE-03 | P0 | `Content-Length` 必填 | 服务支持校验 | 省略 `Content-Length` 发 PUT | 返回 400 类错误 |
| WRITE-04 | P0 | `NDM-Chunk-Size` 必填 | 服务支持校验 | 省略 `NDM-Chunk-Size` 发 PUT | 返回 400 类错误 |
| WRITE-05 | P0 | `Content-Length` 必须等于 `NDM-Chunk-Size` | 服务已启动 | 两者设置成不同值 | 返回 400 `invalid_param` 或等价错误，不得写入成功 |
| WRITE-06 | P0 | body 长度必须等于声明的 chunk_size | 服务已启动 | 发送少于或多于声明长度的 body | 请求失败；不得出现截断写入成功 |
| WRITE-07 | P0 | body 内容必须与 `chunk_id` 一致 | 准备一段与路径 `chunk_id` 不匹配的数据 | 发 PUT | 返回 409 `verify_error` 或协议约定错误；不得创建错误 chunk |
| WRITE-08 | P0 | 首次写入成功时返回 `written` | 准备不存在 chunk | 发合法 PUT | HTTP 201 或 200，`NDM-Chunk-Write-Outcome=written` |
| WRITE-09 | P0 | 已存在 chunk 可返回 `already_exists` | 准备已存在 chunk | 对相同 `chunk_id` 重复 PUT | HTTP 200 或 201，`NDM-Chunk-Write-Outcome=already_exists` |
| WRITE-10 | P0 | `already_exists` 时可不消费完整 body | 准备已存在 chunk 与大 body | 重复 PUT，并在客户端观测服务端提前结束连接或快速返回 | 服务端可早返回，但不得影响已存在 chunk 状态 |
| WRITE-11 | P0 | 原子写入语义 | 准备可制造中途失败的写入场景 | 在传输中断或校验失败时写入 chunk | 失败后 chunk 仍为 `not_exist` 或原有 `already_exists`；不得出现部分可读 |
| WRITE-12 | P0 | 成功响应头完整 | 准备不存在 chunk | 成功 PUT | 响应包含 `NDM-Chunk-Size`、`NDM-Chunk-Write-Outcome`、`NDM-Chunk-Object-ID` |
| WRITE-13 | P1 | `chunk_size=0` 边界 | 服务已启动 | 对零长度 chunk 发 PUT | 实现行为必须固定：要么允许零长度 chunk，要么明确拒绝；不得出现状态不一致 |
| WRITE-14 | P1 | 写入成功后与读接口一致 | 准备不存在 chunk | PUT 成功后调用 `have_chunk`、`query_chunk_state`、`chunk/data` | 三者都能观察到一致的已完成状态和相同内容 |

## 4.6 受限控制面与权限模型

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| AUTH-01 | P0 | 默认部署仅开放普通能力 | 服务按默认配置启动 | 调用 `apply_edge`、`pin`、`unpin`、`fs_*`、`forced_gc_until`、`outbox_count`、`debug_dump_expand_state`、`anchor_state` | HTTP 403，`error=permission_denied` |
| AUTH-02 | P0 | 显式开放后受限接口可调用 | 服务开启受限能力 | 调用一个受限接口 | 按接口定义返回 200 / 204 |
| AUTH-03 | P0 | `unpin_owner` 成功返回计数 | 服务开启受限能力并准备 owner 数据 | 调用 `unpin_owner` | HTTP 200，返回 `{ "count": n }` |
| AUTH-04 | P0 | `fs_release_inode` 成功返回计数 | 服务开启受限能力并准备 inode 数据 | 调用 `fs_release_inode` | HTTP 200，返回 `{ "count": n }` |
| AUTH-05 | P1 | `outbox_count` 接受空对象 `{}` | 服务开启受限能力 | 调用 `outbox_count` with `{}` | HTTP 200，返回 `{ "count": n }` |
| AUTH-06 | P1 | `fs_anchor_state` / `anchor_state` 返回字符串状态 | 服务开启受限能力并准备状态 | 分别调用两个接口 | 响应中的 `state` 为字符串，而不是内部枚举整数 |
| AUTH-07 | P1 | 鉴权失败与参数错误优先级清晰 | 默认关闭受限能力 | 向受限接口发送非法参数 | 应优先返回 `403 permission_denied` 或明确固定顺序，避免实现漂移 |

## 4.7 接口一致性与端到端回归

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| E2E-01 | P0 | `put_object -> get_object -> remove_object` 闭环 | 准备一个普通对象 | 顺序调用 3 个接口 | 状态变化符合预期，无残留 |
| E2E-02 | P0 | `write/chunk -> have_chunk -> query_chunk_state -> read/chunk/data` 闭环 | 准备一个 chunk 数据样本 | 顺序写入并读取 | 写后即读一致，状态为 `completed` |
| E2E-03 | P0 | `read/object/open` 与底层 chunk 读取一致 | 准备 `file object -> chunk` 样本 | 分别用 `object/open` 和 `chunk/data` 获取内容 | 两种方式得到完全相同的数据 |
| E2E-04 | P0 | `read/object/open` 与底层 chunklist 读取一致 | 准备 `file object -> chunklist` 样本 | 分别用 `object/open` 和 `chunklist/open` 获取内容 | 两种方式得到完全相同的数据 |
| E2E-05 | P1 | `query_object_by_id` / `is_object_exist` / `get_object` 的存在性语义一致 | 准备存在与不存在两类对象 | 分别调用三个接口 | 返回互相可推导，不出现“可查询但不可读取”的异常状态 |
| E2E-06 | P1 | `have_chunk` / `query_chunk_state` / `read/chunk/data` 的存在性语义一致 | 准备 completed、same_as、not_exist 三类 chunk | 分别调用三个接口 | 元数据状态与实际可读性一致 |
| E2E-07 | P1 | 错误路径不污染状态 | 准备不存在对象和 chunk | 连续发送非法读写请求 | 后续合法请求结果不受影响，不出现脏状态 |

## 5. 自动化建议

- 单元测试：
  - ID 解析与类型检查
  - `inner_path` 规范化
  - `NdnError -> HTTP` 映射函数
  - `chunk_id` 与 body 内容一致性校验
- 集成测试：
  - `/rpc/*` 参数校验与错误体
  - `/read/*` 的 Header 完整性与 offset / piece 边界
  - `/write/*` 的原子写入、重复写与早返回
- 端到端测试：
  - 对象闭环
  - chunk 读写闭环
  - object reader 与 chunk / chunklist reader 一致性
  - 默认权限与开启权限两套部署配置
- Fuzz 建议：
  - JSON body 缺字段、错类型、超长字符串
  - `inner_path` 空串、重复斜杠、极长路径
  - `offset` / `piece_size` / `chunk_size` 的整型边界与溢出值
  - Header 缺失、重复、冲突、不一致

## 6. 建议首批回归子集

首轮若只保留最小高价值回归集，建议优先自动化以下 12 个用例：

- `GEN-03`
- `GEN-07`
- `OBJ-03`
- `OBJ-13`
- `CHK-05`
- `READ-04`
- `READ-08`
- `READ-12`
- `WRITE-05`
- `WRITE-07`
- `WRITE-11`
- `AUTH-01`
