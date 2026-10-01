# CYFS 标准对象

本文档描述当前 `cyfs-ndn` 仓库已经实现的 **CYFS 标准对象**、`ObjId`/`ChunkId` 表示、对象 ID 计算规则，以及各对象的 JSON 形态。



## 1. 术语与约定

- **NamedObject**：可序列化为 JSON 或 JWT claims 的结构化对象，其标识为 `ObjId`。
- **NamedData / Chunk**：二进制数据块，其标识为 `ChunkId`，也是 `ObjId` 的一种。
- **ObjType**：`ObjId` 的类型前缀，例如 `cyfile`、`cydir`、`clist`。
- **Canonical JSON**：当前实现使用 `serde_jcs::to_string`，也就是 RFC 8785 JSON Canonicalization Scheme（JCS）风格的稳定 JSON 编码。

## 2. 当前实现的 ObjType

当前启用的 ObjType 常量定义在 `src/ndn-lib/src/lib.rs`：

| ObjType | Rust 类型/用途 | 状态 |
| --- | --- | --- |
| `cyfile` | `FileObject` | 已实现 |
| `cydir` | `DirObject` | 已实现 |
| `cypath` | `PathObject` | 已实现 |
| `cyinc` | `InclusionProof` | 已实现 |
| `cyact` | `ActionObject` | 已实现 |
| `cyrel` | `RelationObject` | 已实现 |
| `cymsg` | `MsgObject` | 已实现 |
| `cyrece` | `ReceiptObj` | 已实现 |
| `pkg` | `PackageMeta` | 已实现，位于 `package-lib` |
| `cymap` | `SimpleObjectMap` | 已实现，主要作为容器组件使用 |
| `cylist` | simple object list | 仅保留类型常量，当前没有独立结构实现 |
| `clist` | `ChunkList` | 已实现，简单 ChunkId 数组 |
| `cypack` | object set | 仅保留类型常量，当前没有独立结构实现 |

历史草案中出现过 `cymap-mtp`、`cytrie`、`cytrie-s`、`cylist-mtree`、`cl`、`clist-fix`、`cl-sf` 等类型名。当前仓库没有启用这些 ObjType 常量，也没有对应稳定实现，本文不把它们列为已实现标准对象。

## 3. ObjId 表示与解析

`ObjId` 的结构为：

```rust
pub struct ObjId {
    pub obj_type: String,
    pub obj_hash: Vec<u8>,
}
```

当前实现支持两种文本表示。

### 3.1 Hex 形式

```text
{obj_type}:{hex(obj_hash)}
```

示例：

```text
sha256:0203040506
cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed
```

规范：

- JSON 中表达 `ObjId`、`ChunkId` 字段时必须使用字符串。
- 字符串形态应优先使用 hex 形式，便于人工检查与日志排查。
- `ObjId::to_string()` 当前返回 hex 形式。

### 3.2 Base32 形式

Base32 形式把字节串：

```text
obj_type UTF-8 bytes || ":" || obj_hash bytes
```

按 RFC 4648 base32 lower/no-padding 编码。

示例：

```text
sha256:0203040506  <->  onugcmrvgy5aeayeauda
```

规范与实现注意：

- `ObjId::to_base32()` 使用 RFC 4648 小写字母表，不带 padding。
- `ObjId::new()` 在没有 `:` 的情况下按 base32 解析。
- 当前实现的 base32 解码使用 lower/no-padding 字母表；协议实现应输出小写 base32。接收方如果要支持 hostname 场景，建议在调用解析前自行转小写。
- `Display` 实现输出 base32；而 `ObjId::to_string()` 这个固有方法输出 hex 形式。协议文本和 JSON 字段应明确使用 hex 字符串。

### 3.3 字节表示

`ObjId::to_bytes()` 与 `ObjId::from_bytes()` 使用同一字节格式：

```text
obj_type UTF-8 bytes || ":" || obj_hash bytes
```

`ChunkId` 的字节格式与 `ObjId` 相同，只是 `obj_type` 必须是已知 chunk type。

## 4. ObjId JSON 编码

`ObjId` 和 `ChunkId` 的 serde 实现已经固定为字符串：

```json
{
  "target": "cyfile:1234567890abcdef",
  "chunk": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5"
}
```

结构体形态不再被当前 `ObjId` 反序列化接受：

```json
{
  "obj_type": "cyfile",
  "obj_hash": [1, 2, 3, 4]
}
```

规范：

- 标准对象字段中的 ObjId/ChunkId 必须是字符串。
- 使用结构体形态会导致当前实现反序列化失败，或者在其他 JSON 场景中产生不同的 canonical JSON 与 ObjId。

## 5. NamedObject 的 ObjId 计算

### 5.1 JSON 对象

`NamedObject::gen_obj_id()` 的默认规则是：

1. 将对象序列化为 `serde_json::Value`。
2. 用 `serde_jcs::to_string` 生成 canonical JSON 字符串 `S`。
3. 计算 `sha256(S.as_bytes())` 得到 32 字节 `obj_hash`。
4. 构造 `ObjId { obj_type, obj_hash }`。
5. 对外文本通常表示为 `{obj_type}:{hex(obj_hash)}`。

实现函数：

- `build_named_object_by_json(obj_type, json_value)`
- `build_obj_id(obj_type, obj_json_str)`
- `verify_named_object(obj_id, json_value)`
- `verify_named_object_from_str(obj_id, obj_str)`

注意：

- 对象字段缺失、字段值为 `null`、字段值为默认值但被序列化出来，都会产生不同的 ObjId。
- 当前很多结构通过 `skip_serializing_if` 省略空值或默认值；协议实现必须按实际 serde 形态对齐。
- `serde_jcs::to_string` 失败时当前实现会退化为 `"{}"`。协议实现不应依赖这个容错路径，生成对象前应保证 JSON 可 canonicalize。

### 5.2 JWT 对象

对象数据可以用 JWT 传输。ObjId 计算基于 JWT claims，而不是 JWT header 或 signature：

1. `decode_jwt_claim_without_verify(jwt_str)` 得到 claims JSON。
2. 按 5.1 的 JSON 规则计算 ObjId。

实现函数：

- `build_named_object_by_jwt(obj_type, jwt_str)`
- `verify_named_object_from_jwt(obj_id, jwt_str)`
- `load_named_object_from_obj_str(obj_str)`

规范：

- 接收方验证 ObjId 前必须先取得 JWT claims。
- ObjId 验证不等价于签名验证。签名验证属于上层信任、授权或投递协议。

## 6. ChunkId

`ChunkId` 是 `ObjId` 的一种。其 `obj_type` 是 chunk type，`obj_hash` 是 hash 结果，mix 类型在 hash 结果前编码数据长度。

当前 `ChunkType::is_chunk_type()` 接受：

| Chunk type | 基础算法 | 长度前缀 | Hash 状态 |
| --- | --- | --- | --- |
| `sha256` | SHA-256 | 否 | 已实现 |
| `mix256` | SHA-256 | 是 | 已实现 |
| `sha512` | SHA-512 | 否 | 已实现 |
| `mix512` | SHA-512 | 是 | 已实现 |
| `blake2s256` | BLAKE2s-256 | 否 | 已实现 |
| `mixblake2s256` | BLAKE2s-256 | 是 | 已实现 |
| `keccak256` | Keccak-256 | 否 | 已实现 |
| `mixkeccak256` | Keccak-256 | 是 | 已实现 |
| `qcid` | SHA-256（全文或三片采样） | 是 | 已实现 |

### 6.1 mix 长度编码

对 `mix*` 和 `qcid` 类型：

```text
obj_hash = unsigned_varint(u64(data_length)) || raw_hash_bytes
```

其中：

- `unsigned_varint` 使用 `unsigned-varint` crate 的 u64 编码，即无符号 LEB128 风格编码。
- `data_length` 是 chunk 原始字节长度。
- `raw_hash_bytes` 是基础算法对 chunk 原始字节计算出的完整摘要。

`ChunkId::get_length()` 只对 mix 类型返回长度；非 mix 类型返回 `None`。

### 6.2 QCID 计算

设文件长度为 `L`，固定片长 `P = 4096`：

- `L < 3P`：`raw_hash_bytes = SHA256(file[0..L])`。
- `L >= 3P`：`raw_hash_bytes = SHA256(head || center || tail)`，其中：
  - `head = file[0..P]`
  - `center_start = floor((L-P)/2)`
  - `center = file[center_start..center_start+P]`
  - `tail = file[L-P..L]`

最终按 mix 长度编码生成：

```text
QCID.obj_hash = unsigned_varint(L) || raw_hash_bytes
```

所有大小的文件都可以计算 QCID。恰好 `L = 3P` 时，三片按顺序完整覆盖文件，结果等价于全文 SHA-256。

QCID 对大文件只采样部分内容，因此只能用于快速候选匹配或修改检测，不能代替完整内容 Hash 做严格完整性证明。

### 6.3 ChunkId JSON 示例

```json
{
  "chunk": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5"
}
```

## 7. ChunkList（ObjType: `clist`）

实现：`src/ndn-lib/src/chunk/chunk_list.rs`

当前 `ChunkList` 是简单 ChunkId 数组：

```rust
pub struct ChunkList {
    pub total_size: u64,
    pub body: Vec<ChunkId>,
}
```

对象数据本体是 JSON 数组，而不是带 `body` 字段的 JSON 对象：

```json
[
  "mix256:...",
  "mix256:..."
]
```

### 7.1 构造约束

`ChunkList::from_chunk_list()` 和 `append_chunk()` 要求每个 `ChunkId` 都能通过 `get_length()` 取得长度。因此当前 `clist` 的成员实际必须是 mix 类型 chunk id。普通 `sha256` chunk id 无法用于自动计算 `total_size`。

### 7.2 ObjId 计算

`ChunkList::gen_obj_id()` 的算法不同于普通 JSON NamedObject：

```text
S        = JCS canonical JSON of Vec<ChunkId>
H        = sha256(S)
obj_hash = unsigned_varint(u64(total_size)) || H
ObjId    = clist:hex(obj_hash)
```

其中 `total_size` 是所有 chunk 长度之和。

因此：

- `clist` 的 `obj_hash` 不是单纯 hash，而是长度前缀加 hash。
- 客户端可以仅凭 `clist` ObjId 解码出文件总大小。

## 8. SimpleObjectMap（ObjType: `cymap`）

实现：`src/ndn-lib/src/simple_object_map.rs`

`SimpleObjectMap` 是小规模 `key -> object` 容器，通常嵌入 `DirObject`。结构为：

```rust
pub struct SimpleObjectMap {
    pub body: HashMap<String, SimpleMapItem>,
}
```

`SimpleMapItem` 有三种 JSON 形态：

1. ObjId 字符串：

```json
"cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed"
```

2. 内嵌 JSON 对象：

```json
{
  "obj_type": "cyfile",
  "body": {
    "name": "readme.txt",
    "create_time": 1700000000,
    "last_update_time": 1700000000,
    "size": 12,
    "content": "mix256:..."
  }
}
```

3. 内嵌 JWT：

```json
{
  "obj_type": "cyfile",
  "jwt": "<jwt-string>"
}
```

### 8.1 ObjId 归一化规则

`SimpleObjectMap::gen_obj_id_with_real_obj(result_obj_type, real_obj)` 计算上层对象 ObjId 时，会先把 `body` 归一化为 `key -> ObjId hex 字符串`：

- 如果 item 已经是 ObjId 字符串，直接使用该 ObjId。
- 如果 item 是 `{ "obj_type": "...", "body": ... }`，按 JSON NamedObject 规则计算子对象 ObjId。
- 如果 item 是 `{ "obj_type": "...", "jwt": ... }`，按 JWT claims 规则计算子对象 ObjId。

归一化后的 `body` 形态类似：

```json
{
  "body": {
    "readme.txt": "cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed"
  }
}
```

规范：

- 内嵌 `body`/`jwt` 是传输优化，用于减少额外抓取；它们不得直接参与上层对象 hash。
- 上层对象 hash 只绑定归一化后的子对象 ObjId 字符串。

## 9. BaseContentObject

实现：`src/ndn-lib/src/base_content.rs`

`BaseContentObject` 是内容对象的通用元信息基类，本身不是独立 NamedObject。当前字段如下：

```rust
pub struct BaseContentObject {
    pub did: Option<DID>,
    pub name: String,
    pub author: String,
    pub owner: DID,
    pub create_time: u64,
    pub last_update_time: u64,
    pub copyright: Option<String>,
    pub tags: Vec<String>,
    pub categories: Vec<String>,
    pub base_on: Option<ObjId>,
    pub directory: HashMap<String, Curator>,
    pub references: HashMap<String, Reference>,
    pub exp: u64,
}
```

序列化行为：

- `did`、`copyright`、`base_on` 为 `None` 时省略。
- `name`、`author` 为空字符串时省略。
- `owner` 为无效 DID 时省略。
- `tags`、`categories`、`directory`、`references` 为空时省略。
- `exp == 0` 时省略。
- `create_time`、`last_update_time` 总是序列化，即使为 `0`。

## 10. FileObject（ObjType: `cyfile`）

实现：`src/ndn-lib/src/fileobj.rs`

`FileObject = BaseContentObject + size + content + flattened meta`：

```rust
pub struct FileObject {
    pub content_obj: BaseContentObject,
    pub size: u64,
    pub content: String,
    pub meta: HashMap<String, serde_json::Value>,
}
```

字段：

- `size`：文件总大小，`0` 时省略。
- `content`：chunk id 或 `clist` id 字符串，空字符串时省略。
- `meta`：额外自定义字段，flatten 到顶层。

示例：

```json
{
  "name": "hello.txt",
  "author": "alice",
  "create_time": 1700000000,
  "last_update_time": 1700000120,
  "size": 12,
  "content": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "mime": "text/plain"
}
```

## 11. DirObject（ObjType: `cydir`）

实现：`src/ndn-lib/src/dirobj.rs`

`DirObject = BaseContentObject + meta + 目录统计字段 + SimpleObjectMap`：

```rust
pub struct DirObject {
    pub content_obj: BaseContentObject,
    pub meta: HashMap<String, serde_json::Value>,
    pub total_size: u64,
    pub file_count: u64,
    pub file_size: u64,
    pub object_map: SimpleObjectMap,
}
```

JSON 形态：

```json
{
  "name": "root",
  "create_time": 1700000000,
  "last_update_time": 1700000000,
  "total_size": 12,
  "file_count": 1,
  "file_size": 12,
  "body": {
    "hello.txt": {
      "obj_type": "cyfile",
      "body": {
        "name": "hello.txt",
        "create_time": 1700000000,
        "last_update_time": 1700000000,
        "size": 12,
        "content": "mix256:..."
      }
    }
  }
}
```

ObjId 计算：

- `DirObject::gen_obj_id()` 不直接使用序列化出来的完整目录 JSON。
- 它先构造包含基础字段和统计字段的 `real_obj`。
- 再调用 `SimpleObjectMap::gen_obj_id_with_real_obj("cydir", real_obj)`。
- 因此最终参与目录 hash 的 `body` 是 `key -> child ObjId 字符串`，不是内嵌子对象正文。

## 12. PathObject（ObjType: `cypath`）

实现：`src/ndn-lib/src/fileobj.rs`

`PathObject` 表达“语义路径 -> 目标 ObjId”的可验证绑定，常以 JWT 传输并签名。

```rust
pub struct PathObject {
    pub path: String,
    pub iat: u64,
    pub target: ObjId,
    pub exp: u64,
}
```

示例：

```json
{
  "path": "/repo/apps/demo",
  "iat": 1700000200,
  "target": "cyfile:1234567890abcdef",
  "exp": 1700086600
}
```

注意：当前字段名是 `iat`，不是旧文档中的 `uptime`。

## 13. InclusionProof（ObjType: `cyinc`）

实现：`src/ndn-lib/src/base_content.rs`

`InclusionProof` 表达“收录者对内容的收录证明”。实现建议将 JSON 作为 JWT claims 并由收录者签名。

```rust
pub struct InclusionProof {
    pub content_id: String,
    pub content_obj: serde_json::Value,
    pub curator: DID,
    pub editor: Vec<String>,
    pub meta: Option<serde_json::Value>,
    pub rank: i64,
    pub collection: Vec<String>,
    pub review_url: Option<String>,
    pub iat: u64,
    pub exp: u64,
}
```

示例：

```json
{
  "content_id": "cyfile:1234567890abcdef",
  "content_obj": {
    "name": "hello.txt",
    "size": 12,
    "content": "mix256:..."
  },
  "curator": "did:web:curator.example.com",
  "editor": ["did:web:editor.example.com"],
  "meta": {"score": 9.6, "comment": "stable"},
  "rank": 88,
  "collection": ["docs", "featured"],
  "review_url": "https://curator.example.com/review/hello.txt",
  "iat": 1700000300,
  "exp": 1703110300
}
```

## 14. ActionObject（ObjType: `cyact`）

实现：`src/ndn-lib/src/action_obj.rs`

`ActionObject` 表达“某主体对某目标执行某动作”的事件。

```rust
pub struct ActionObject {
    pub subject: ObjId,
    pub action: String,
    pub target: ObjId,
    pub base_on: Option<ObjId>,
    pub details: Option<serde_json::Value>,
    pub iat: u64,
    pub exp: u64,
}
```

已定义 action 常量：

- `viewed`
- `download`
- `installed`
- `shared`
- `liked`
- `unliked`
- `purchased`

示例：

```json
{
  "subject": "cyfile:aaaaaaaaaaaaaaaa",
  "action": "viewed",
  "target": "cymsg:bbbbbbbbbbbbbbbb",
  "base_on": "cyact:cccccccccccccccc",
  "details": {"device": "desktop", "source": "unit-test"},
  "iat": 1700000500,
  "exp": 1700086900
}
```

## 15. RelationObject（ObjType: `cyrel`）

实现：`src/ndn-lib/src/relation_obj.rs`

`RelationObject` 表达两个对象之间的弱关系，可通过 flatten 的 `body` 携带扩展字段。

```rust
pub struct RelationObject {
    pub source: ObjId,
    pub relation: String,
    pub target: ObjId,
    pub body: HashMap<String, serde_json::Value>,
    pub iat: Option<u64>,
    pub exp: Option<u64>,
}
```

已定义关系类型：

- `same`
- `part_of`

`same` 示例：

```json
{
  "source": "cyfile:1234567890abcdef",
  "relation": "same",
  "target": "cyfile:fedcba0987654321"
}
```

`part_of` 示例：

```json
{
  "source": "cyfile:1234567890abcdef",
  "relation": "part_of",
  "target": "sha256:1122334455667788",
  "range": {"start": 0, "end": 12},
  "note": "excerpt",
  "iat": 1700000400,
  "exp": 1700086800
}
```

`range`、`note` 等字段位于 `body`/flatten 区域。

## 16. MsgObject（ObjType: `cymsg`）

实现：`src/ndn-lib/src/msgobj.rs`

`MsgObject` 是不可变消息对象。

> **v2（2026-09-30，breaking change）**：本节是 MsgObject v2 的定义。相对 v1：
>
> - 新增 `to_session`，指定目标实体下的具名会话；`thread.topic` 只作语义 hint，不再参与路由；
> - 新增 `relates_to`（编辑、撤回、回应、话题）与 `mentions`（提及）；
> - 删除 `proof` 字段。需要签名时，与其它标准对象一样使用 §5.2 的 JWT 形式（见 16.5）；
> - 删除 `thread.tunnel_id`（transport 信息属于投递层，不属于消息语义）。
>
> `msgobj.rs` 已按本节实现。v1 对象里的 `proof` 在反序列化时落入 `meta`，`MsgObject::validate()` 会拒绝它。

```rust
pub struct MsgObject {
    pub from: DID,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    pub to_session: Option<String>,        // v2，见 16.1
    pub thread: TopicThread,               // 见 16.2
    pub relates_to: Option<MsgRelation>,   // v2，见 16.3
    pub mentions: Option<MsgMentions>,     // v2，见 16.4
    pub workspace: Option<DID>,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub nonce: Option<u64>,
    pub content: MsgContent,
    pub meta: BTreeMap<String, serde_json::Value>,
}

pub struct TopicThread {
    pub topic: Option<String>,
    pub reply_to: Option<ObjId>,
    pub correlation_id: Option<String>,
}

pub struct MsgRelation {
    pub rel: MsgRelType,        // edit | redact | reaction | thread；其它值保留为 Unknown
    pub target: ObjId,          // 被关联的消息，必须是 cymsg
    pub key: Option<String>,    // 仅 reaction：回应内容，例如一个 emoji
}

pub struct MsgMentions {
    pub dids: Vec<DID>,
    pub all: bool,
}
```

序列化规则：

- `Option` 字段为 `None` 时省略；`thread` 的三个字段都为空时整体省略。
- `MsgMentions.dids` 为空时省略，`all` 为 `false` 时省略；两者都省略时 `mentions` 必须为 `None`，不能序列化为空对象。
- `to_session` 不能是空字符串。
- `rel` 使用 snake_case。
- `meta` 是 flatten 的扩展字段，键不能与上面的字段名冲突。`proof` 是保留名，不能作为 `meta` 的键。

这些规则保证同一语义只有一种 canonical JSON，也使不使用 v2 字段、也没有 `proof` 的消息与 v1 得到相同的 ObjId。

实现：

- 反序列化保持宽松，已存储的旧记录总能读出；对象级规则（`to_session`、`mentions`、`relates_to`、`meta` 保留键）由 `MsgObject::validate()` 检查，接收方在入口调用，失败即拒绝。
- `MsgObject::from_json_value_checked(value)` 反序列化、校验，并确认对象重新序列化后的 ObjId 与收到的 JSON 一致（拒绝 `"mentions":{}`、`null` 字段等非 canonical 写法），返回由收到的 JSON 计算的 ObjId。
- 序列化时空的 `mentions` 被省略，因此本实现不会产生 `"mentions":{}`。

`kind` 使用 snake_case 枚举：

- `chat`
- `group_msg`
- `deliver`
- `notify`
- `event`
- `operation`

### 16.1 寻址：`to` 与 `to_session`

- `to` 是接收实体的 DID 列表。
- `to_session` 指定接收实体下的具名会话，对应 MailboxAddress 的 session 部分（`to[0]/to_session`）。省略表示默认会话。
- `to_session` 只能在 `to` 恰有一个 DID 时使用。多目标消息带 `to_session` 是非法对象，接收方拒绝。
- 取值规则与 MailboxAddress 的 session 部分相同：1–200 个字符，不含首尾空白和控制字符，不能是 `.` 或 `..`。
- 接收方如何对待 `to_session` 由接收实体决定。托管会话的实体（例如群）必须严格按它路由：会话不存在时拒绝，不能退回默认会话。个人收件方可以把它映射到自己的本地会话。
- `to_session` 属于对象内容，参与 ObjId 计算，签名时也在 JWT claims 中，任何环节都不能改写。

### 16.2 语义线索：`thread`

- `topic`：发送方给出的主题标签等语义 hint。接收方可以用它辅助归类，但**不能**把它当作路由依据。v1 中用 `topic` 指定会话的做法改用 `to_session`。
- `reply_to`：被回复的消息。
- `correlation_id`：发送方用于关联请求与响应的标识。
- v1 的 `tunnel_id` 已删除。

### 16.3 消息关系：`relates_to`

原消息不可变，所有修改都以新消息表达。关系消息是一条普通的 MsgObject，用 `relates_to` 指向另一条消息：

| `rel` | 含义 | 约束 |
| --- | --- | --- |
| `edit` | 用本消息的 `content` 替换原消息的展示内容 | `from` 必须等于原消息的 `from`；`content` 是完整的新内容，不是差量；每次编辑都指向原消息，不指向上一次编辑；不能编辑关系消息 |
| `redact` | 撤回或删除原消息 | 发送者是原消息作者，或是接收实体规则授权的操作者（例如群管理员）；`content.content` 可以写明原因；撤回原消息同时使它的编辑和回应失效；撤回一条 `reaction` 就是取消该回应 |
| `reaction` | 对原消息的回应 | `key` 必填，不超过 64 字节；`content` 可以为空；同一个 `(from, target, key)` 只算一次 |
| `thread` | 本消息属于以原消息为根的话题 | 根消息本身不能是 `thread` 关系消息 |

通用规则：

- 关系消息的 `to`、`to_session` 和 `kind` 必须与原消息相同。接收方发现不一致时拒绝。
- 多条编辑的先后，由接收方分配的顺序决定，不由 `created_at_ms` 决定。
- 除 `reaction` 外，关系消息的 `content.content` 应当包含可读的回落文本，例如「[已编辑] …」「撤回了一条消息」。
- 接收方不认识的 `rel` 值：保留消息，按普通消息展示，不执行任何关系语义。
- 是否接受某条关系消息（例如编辑时间窗、谁可以删除他人消息），由接收实体的规则决定。

### 16.4 提及：`mentions`

- `dids` 是被提及的 DID；`all` 表示提及目标会话的全体参与者。
- 提醒语义只来自这个字段，接收方不解析正文中的 `@` 文本。正文中如何显示提及由客户端决定。
- 接收方可以按自己的规则忽略或拒绝提及，例如群只允许有权限的成员使用 `all`。

### 16.5 签名：JWT 形式

MsgObject 不设专门的签名字段。需要证明消息确实由 `from` 创建、之后没有被改动时，与 `PathObject`、`InclusionProof` 等标准对象一样，使用 §5.2 的 JWT 形式：

```text
JWT header = {"alg":"EdDSA","kid":"<签名密钥的 DID URL>"}
JWT claims = MsgObject 的 JSON
```

- `alg` 目前只使用 `EdDSA`（Ed25519），与 `named_obj_to_jwt` 一致。
- `kid` 指向的验证方法必须属于 `from`：要么直接列在 `from` 的 DID Document 中，要么是该 DID Document 授权的设备或 Agent 密钥。
- 验证步骤：用 `kid` 对应的公钥验证 JWT 签名；确认该密钥属于 `from`；按 §5.2 从 claims 计算 ObjId。
- 签名覆盖整个对象，包括 `to_session`、`relates_to`、`mentions` 和 `created_at_ms`。
- ObjId 只由 claims 计算，与签名无关。因此同一条消息的 JSON 形式与 JWT 形式是同一个 ObjId，编辑、回应、去重都不受是否签名影响。
- 签名无法伪造，也无法用来改动内容；但它可能在传递中被丢掉，只剩 JSON 形式。只有 JSON 形式的消息，来源只由投递或提供它的一方背书。需要强证明的一方，必须取得 JWT 形式。
- 以 JWT 形式收到 MsgObject 的一方，在保存和转发时应当保留 JWT 原文，供后续读者验证。
- 依赖签名的接收方必须校验。托管会话的实体（例如群）收到签名无效的 JWT 必须拒绝，不能把它降级为 JSON 形式接受。
- 跨 Zone 投递 JWT 形式的消息时，使用 `application/cyfs-named-object+jwt`（《CYFS Protocol》dispatch 一节）。
- 密钥轮换后如何验证历史消息，本版不定义。

实现：

- `MsgObject::to_jwt(key, kid)`：先 `validate()`，再以 `named_obj_to_jwt` 签名。
- `verify_msg_object_jwt(jwt, public_key)`：要求 header 的 `alg = EdDSA` 且带 `kid`，校验签名，再按 `from_json_value_checked` 解出对象；不要求 `exp` 等 JWT 注册声明。
- `decode_msg_object_jwt(jwt)`：只解码不验签，供不依赖签名的场景使用。
- 两者返回 `MsgObjectJwt { msg, obj_id, kid }`。`kid` 对应的公钥由接收方解析；`MsgObjectJwt::kid_did()` 取出 `kid` 的 DID 部分，接收方据此确认密钥属于 `from`。

### 16.6 时间与去重

- `created_at_ms` 是发送方声明的创建时间，只用于展示。接收方不能用它作为排序、同步游标或权限时间窗的依据；需要顺序时，由接收方（例如群 host）在接受消息时自行分配。
- 去重按 ObjId 进行。两条内容完全相同的消息会得到同一个 ObjId，因此发送方应当为每条消息填写随机的 `nonce`，避免被误判为重复消息。

### 16.7 MsgContent

```rust
pub struct MsgContent {
    pub title: Option<String>,
    pub format: Option<MsgContentFormat>,
    pub content: String,
    pub machine: Option<MachineContent>,
    pub refs: Vec<RefItem>,
}
```

`format` 是 MIME 字符串，例如 `text/plain`、`text/markdown`、`application/json`、`application/pdf`。未知 MIME 会以原字符串保留。

引用对象 `RefItem` 支持：

- `data_obj`：引用一个 `ObjId`，可带 `uri_hint`。
- `service_did`：引用一个 DID 服务。

### 16.8 示例

群消息（发往群的具名会话 `release`，提及 Bob；需要签名时，把这段 JSON 作为 JWT claims，见 16.5）：

```json
{
  "from": "did:web:alice.example.com",
  "to": ["did:web:team.example.com"],
  "kind": "group_msg",
  "to_session": "release",
  "thread": {
    "topic": "发布准备",
    "reply_to": "cymsg:010203040506",
    "correlation_id": "corr-001"
  },
  "mentions": {
    "dids": ["did:web:bob.example.com"]
  },
  "workspace": "did:web:workspace.example.com",
  "created_at_ms": 1700000000000,
  "expires_at_ms": 1700086400000,
  "nonce": 7,
  "content": {
    "title": "Hello",
    "format": "application/json",
    "content": "{\"status\":\"ok\"}",
    "machine": {
      "intent": "sync",
      "data": {
        "level": 3,
        "urgent": true
      }
    },
    "refs": [
      {
        "role": "input",
        "target": {
          "type": "data_obj",
          "obj_id": "cyfile:1234567890abcdef",
          "uri_hint": "cyfs://hello.txt"
        },
        "label": "attachment"
      }
    ]
  },
  "priority": 1,
  "lang": "zh-CN"
}
```

回应（Bob 对上一条消息点赞）：

```json
{
  "from": "did:web:bob.example.com",
  "to": ["did:web:team.example.com"],
  "kind": "group_msg",
  "to_session": "release",
  "relates_to": {
    "rel": "reaction",
    "target": "cymsg:0a0b0c0d",
    "key": "👍"
  },
  "created_at_ms": 1700000005000,
  "nonce": 42,
  "content": {}
}
```

管理员删除消息：

```json
{
  "from": "did:web:admin.example.com",
  "to": ["did:web:team.example.com"],
  "kind": "group_msg",
  "to_session": "release",
  "relates_to": {
    "rel": "redact",
    "target": "cymsg:0a0b0c0d"
  },
  "created_at_ms": 1700000009000,
  "nonce": 43,
  "content": {
    "content": "管理员删除了一条消息：违反群规"
  }
}
```

## 17. ReceiptObj（ObjType: `cyrece`）

实现：`src/ndn-lib/src/msgobj.rs`

`ReceiptObj` 是可选的不可变投递回执对象。

```rust
pub struct ReceiptObj {
    pub obj_id: ObjId,
    pub iss: DID,
    pub channel: Option<String>,
    pub iat: u64,
    pub status: ReceiptStatus,
    pub reason: Option<String>,
}
```

`status` 使用 snake_case 枚举：

- `accepted`
- `rejected`
- `quarantined`

示例：

```json
{
  "obj_id": "cymsg:010203040506",
  "iss": "did:web:inbox.example.com",
  "channel": "group",
  "iat": 1700000100000,
  "status": "accepted",
  "reason": "delivered"
}
```

注意：当前实现的 ObjType 是 `cyrece`。旧文档中的 `cymsgr`/`MsgReceiptObj` 不是当前代码里的命名。

## 18. PackageMeta（ObjType: `pkg`）

实现：`src/package-lib/src/meta.rs`

`PackageMeta` flatten 继承 `FileObject`，并增加版本语义：

```rust
pub struct PackageMeta {
    pub _base: FileObject,
    pub version: String,
    pub version_tag: Option<String>,
    pub deps: HashMap<String, String>,
}
```

字段：

- `version`：版本字符串。
- `version_tag`：可选标签，例如 `stable`、`beta`、`latest`。
- `deps`：依赖映射，`pkg_name -> version_req_str`。

示例：

```json
{
  "name": "demo.pkg",
  "author": "alice",
  "owner": "did:bns:buckyos.ai",
  "create_time": 1700000000,
  "last_update_time": 1700000100,
  "exp": 1700086400,
  "size": 4096,
  "content": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "channel": "nightly",
  "version": "1.2.3",
  "version_tag": "stable",
  "deps": {
    "demo.dep": ">=0.9.0"
  }
}
```

`PackageMeta::from_str()` 通过 `name_lib::EncodedDocument` 读取 JSON/JWT 等编码文档，再反序列化为 `PackageMeta`。

## 19. 当前可识别的标准对象子项遍历

`KnownStandardObject::from_obj_data()` 当前只识别三类对象：

- `cydir` -> `KnownStandardObject::Dir`
- `cyfile` -> `KnownStandardObject::File`
- `clist` -> `KnownStandardObject::ChunkList`

`get_child_objs()` 的行为：

- 对 `DirObject`：遍历目录 `body`，返回每个子项 ObjId；如果子项是内嵌对象/JWT，同时返回归一化后的子对象 JSON 字符串。
- 对 `FileObject`：解析 `content` 为 ObjId 并返回。
- 对 `ChunkList`：返回每个 `ChunkId` 对应的 ObjId。

这说明当前实现把目录、文件、ChunkList 作为 NDN 递归拉取的核心可展开对象。

## 20. Rust 参考结构

以下定义摘取当前实现的协议关键字段，省略了部分 impl：

```rust
pub struct ObjId {
    pub obj_type: String,
    pub obj_hash: Vec<u8>,
}

pub struct FileObject {
    pub content_obj: BaseContentObject,
    pub size: u64,
    pub content: String,
    pub meta: HashMap<String, serde_json::Value>,
}

pub struct DirObject {
    pub content_obj: BaseContentObject,
    pub meta: HashMap<String, serde_json::Value>,
    pub total_size: u64,
    pub file_count: u64,
    pub file_size: u64,
    pub object_map: SimpleObjectMap,
}

pub struct PathObject {
    pub path: String,
    pub iat: u64,
    pub target: ObjId,
    pub exp: u64,
}

pub struct InclusionProof {
    pub content_id: String,
    pub content_obj: serde_json::Value,
    pub curator: DID,
    pub editor: Vec<String>,
    pub meta: Option<serde_json::Value>,
    pub rank: i64,
    pub collection: Vec<String>,
    pub review_url: Option<String>,
    pub iat: u64,
    pub exp: u64,
}

pub struct ActionObject {
    pub subject: ObjId,
    pub action: String,
    pub target: ObjId,
    pub base_on: Option<ObjId>,
    pub details: Option<serde_json::Value>,
    pub iat: u64,
    pub exp: u64,
}

pub struct RelationObject {
    pub source: ObjId,
    pub relation: String,
    pub target: ObjId,
    pub body: HashMap<String, serde_json::Value>,
    pub iat: Option<u64>,
    pub exp: Option<u64>,
}

// v2（§16）
pub struct MsgObject {
    pub from: DID,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    pub to_session: Option<String>,
    pub thread: TopicThread,
    pub relates_to: Option<MsgRelation>,
    pub mentions: Option<MsgMentions>,
    pub workspace: Option<DID>,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub nonce: Option<u64>,
    pub content: MsgContent,
    pub meta: BTreeMap<String, serde_json::Value>,
}

pub struct MsgRelation {
    pub rel: MsgRelType,
    pub target: ObjId,
    pub key: Option<String>,
}

pub struct MsgMentions {
    pub dids: Vec<DID>,
    pub all: bool,
}

pub struct ReceiptObj {
    pub obj_id: ObjId,
    pub iss: DID,
    pub channel: Option<String>,
    pub iat: u64,
    pub status: ReceiptStatus,
    pub reason: Option<String>,
}

pub struct ChunkList {
    pub total_size: u64,
    pub body: Vec<ChunkId>,
}

pub struct PackageMeta {
    pub _base: FileObject,
    pub version: String,
    pub version_tag: Option<String>,
    pub deps: HashMap<String, String>,
}
```
