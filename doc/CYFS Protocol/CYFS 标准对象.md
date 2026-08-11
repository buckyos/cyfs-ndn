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
| `qcid` | QCID | 是 | 类型已保留，hash 计算路径当前未实现 |

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

### 6.2 ChunkId JSON 示例

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

```rust
pub struct MsgObject {
    pub from: DID,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    pub thread: TopicThread,
    pub workspace: Option<DID>,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub nonce: Option<u64>,
    pub content: MsgContent,
    pub proof: Option<String>,
    pub meta: BTreeMap<String, serde_json::Value>,
}
```

`kind` 使用 snake_case 枚举：

- `chat`
- `group_msg`
- `deliver`
- `notify`
- `event`
- `operation`

`MsgContent`：

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

示例：

```json
{
  "from": "did:web:alice.example.com",
  "to": ["did:web:bob.example.com", "did:web:carol.example.com"],
  "kind": "chat",
  "thread": {
    "topic": "release",
    "reply_to": "cymsg:010203040506",
    "correlation_id": "corr-001",
    "tunnel_id": "tnl-001"
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
  "proof": "proof-001",
  "priority": 1,
  "lang": "zh-CN"
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

pub struct MsgObject {
    pub from: DID,
    pub to: Vec<DID>,
    pub kind: MsgObjKind,
    pub thread: TopicThread,
    pub workspace: Option<DID>,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub nonce: Option<u64>,
    pub content: MsgContent,
    pub proof: Option<String>,
    pub meta: BTreeMap<String, serde_json::Value>,
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
