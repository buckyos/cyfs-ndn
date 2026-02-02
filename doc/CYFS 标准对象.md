# CYFS 标准对象

本文档描述 `cyfs-ndn` 中实现的 **CYFS 标准对象**（NamedObject/NamedData）以及 ObjId 的计算规则。

文风：标准协议参考（简洁、准确、全面）。

## 1. 术语与约定

- **NamedObject**：可序列化为 JSON（或 JWT claims）的结构化对象，其标识为 `ObjId`。
- **NamedData / Chunk**：二进制数据块，其标识为 `ChunkId`（也是 `ObjId` 的一种）。
- **ObjType**：`ObjId` 的前缀字符串，表示对象类型（例如 `cyfile`、`cydir`、`clist`）。
- **Canonical JSON（稳定编码）**：为保证哈希可重复，JSON 对象的 key 需要按字典序递归排序后再编码为紧凑 JSON 字符串。

本文使用“必须/应/可以”表示规范性要求。

## 1.1 本仓库已定义的标准 ObjType

实现常量定义见 `src/ndn-lib/src/lib.rs`：

| ObjType | 含义 |
| --- | --- |
| `cyfile` | FileObject |
| `cydir` | DirObject |
| `cypath` | PathObject |
| `cyinc` | InclusionProof |
| `cyact` | ActionObject |
| `cyrel` | RelationObject |
| `pkg` | PackageMeta（PkgMetaObject） |
| `cymap` | SimpleObjectMap（simple） |
| `cymap-mtp` | ObjectMap（Merkle Tree） *未稳定 |
| `cytrie-s` | TrieObjectMap（simple） *未稳定 |
| `cytrie` | TrieObjectMap（normal）*未稳定 |
| `cylist` | ObjectArray（simple） |
| `cylist-mtree` | ObjectArray（normal）*未稳定 |
| `clist` | SimpleChunkList |
| `clist-fix` | ChunkList（fixed-size）*未稳定 |
| `cl` | ChunkList（variable-size）*未稳定 |
| `cl-sf` | ChunkList（simple fixed-size）*未稳定 |

## 2. ObjId 表示与解析

实现（`src/ndn-lib/src/object.rs`）支持两种 ObjId 文本表示：

1) **Hex 形式（推荐用于 JSON 字段）**

```
{obj_type}:{hex(obj_hash)}
```

示例：

```text
sha256:0203040506
cyrel:e0ad5f3b656a883de323e4c6e7999207ce3026d3fa5dcb47518d2caeb4d92aa0
```

2) **Base32 形式（紧凑，用于 URL/Hostname 更友好）**

将字节串 `"{obj_type}:{obj_hash_bytes}"` 做 RFC4648 base32（小写，无 padding）编码。

示例（来自单元测试）：

```text
sha256:0203040506  <->  onugcmrvgy5aeayeauda
```

规范：

- 实现必须同时接受以上两种表示。
- 在 JSON 中表达 `ObjId/ChunkId` 字段时，应优先使用 Hex 形式（便于人工检查）。

### 2.1 ObjId 的 JSON 编码形态

本仓库目前同时存在两种 ObjId 的 JSON 编码形态（取决于具体对象定义）：

1) **字符串形态**（更接近协议/文本传输习惯）：

为了稳定编码，应强制要求在json里引用objid必须使用此种格式

```json
"cyfile:513788..."
```

2) **结构体形态**（直接序列化 `ObjId { obj_type, obj_hash }`）：

注：即将放弃

```json
{
  "obj_type": "cyfile",
  "obj_hash": [1, 2, 3, 4]
}
```

注意：两种形态参与稳定编码与哈希计算时会得到不同结果；因此某个标准对象必须固定其采用的形态。


## 3. ObjId 计算：标准 JSON NamedObject

### 3.1 计算公式

对“标准 JSON 对象”（非 Chunk、非容器派生特殊规则）使用以下规则（`build_named_object_by_json`）：

1. 将待计算对象表示为 JSON 值 `V`。
2. 对 `V` 进行稳定化（递归排序 JSON object key；数组保持顺序）。
3. 使用紧凑 JSON 编码得到字符串 `S`。
4. 计算 `sha256(S)` 得到 `obj_hash`。
5. `ObjId = {obj_type}:{obj_hash}`。
6. 在ObjId中编码长度信息(特殊)

注意：当前实现固定使用 `sha256`（见 `build_obj_id`）。

编码注意事项：

- JSON 中字段的“缺省值”不等价于 `null`。如果某字段参与序列化，`null` 与“字段缺失”会产生不同的 ObjId。
- 因此：对可选字段，应在缺省时省略字段（而不是显式写 `null`），以避免 ObjId 分叉。

### 3.2 JWT 形式的 NamedObject

实现允许“对象数据”以 JWT 传输；其 ObjId 计算基于 **JWT claims（payload）**：

- 解码 JWT 得到 claims JSON。
- 按 3.1 的稳定化与 `sha256` 规则计算 ObjId。

规范：

- 如果对象以 JWT 传输，接收方在验证 ObjId 前必须先解码得到 claims。
- ObjId 的计算不依赖 JWT header 与签名部分；签名验证属于更上层的信任/授权机制。

## 4. Chunk（NamedData）与 ChunkId

ChunkId 是 `ObjId` 的一种：`obj_type` 为某种 hash/type，`obj_hash` 为哈希结果（部分类型包含长度编码）。

### 4.1 Chunk 类型字符串

实现（`src/ndn-lib/src/chunk/chunk.rs`）支持以下 `chunk_type`：

- `sha256`
- `mix256`
- `sha512`
- `mix512`
- `qcid`
- `blake2s256`
- `mixblake2s256`
- `keccak256`
- `mixkeccak256`

其中 `mix*` 与 `qcid` 被视为“mix 模式”，其 `obj_hash` 前缀可包含 **varint 编码的数据长度**。

### 4.2 mix 长度编码

对 `mix*` ChunkId：

- `obj_hash_bytes = varint(u64(data_length)) || raw_hash_bytes`

因此：

- `ChunkId.get_length()` 可以从 `obj_hash_bytes` 解码得到 `data_length`。
- `ChunkId.get_hash()`（实现内部逻辑）应跳过 varint 前缀，仅对后续 hash bytes 做比对/上层计算。

### 4.3 ChunkId JSON 示例

ChunkId 在 JSON 中以字符串表示：

```json
{
  "chunk": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5"
}
```

## 5. 容器对象（Container Objects）

容器对象用于组织大量子对象。按“可验证方式”分两类：

1) **简单容器（Simple）**：对象数据中直接包含全部 children（或至少包含可推导 children ObjId 的信息）。
2) **可证明容器（Proof-capable）**：ObjId 绑定一个树根（Merkle/MPT 等），子项可通过 proof 验证，无需传输整个容器。

同一语义的容器通常存在 `simple` 与 `normal` 两种 ObjType。

### 5.1 SimpleObjectMap（ObjType: `cymap`）

实现：`src/ndn-lib/src/object_map/simple_object_map.rs`

SimpleObjectMap 作为“可嵌入组件”使用（通常被 `DirObject` 等对象 `#[serde(flatten)]`）。

#### 5.1.1 JSON 结构

```json
{
  "body": {
    "key": "<objid-string>",
    "file1": {
      "obj_type": "cyfile",
      "body": {"name": "a.txt", "size": 3, "content": "mix256:..."}
    },
    "file2": {
      "obj_type": "cyfile",
      "jwt": "<jwt-string>"
    }
  }
}
```

其中每个 value 是 `SimpleMapItem`：

- 字符串：必须可解析为 `ObjId`（Hex 或 Base32）。
- `{ "obj_type": "...", "body": <json> }`：内嵌对象（未容器化）。
- `{ "obj_type": "...", "jwt": "..." }`：内嵌 JWT。

#### 5.1.2 ObjId 计算规则（关键）

对“真实对象”计算 ObjId 时，SimpleObjectMap 的 `body` 必须先被 **归一化**：

- 对每个 item：
  - 若是 ObjId 字符串：取其 ObjId。
  - 若是内嵌 `body`：按 3.1 规则计算其 ObjId。
  - 若是内嵌 `jwt`：按 3.2 规则计算其 ObjId。
- 归一化后的 `body` 形态为：`{ key -> "{obj_type}:{hex(hash)}" }`（字符串值是 ObjId 的 hex 形式）。

规范：

- 内嵌 `body/jwt` 仅用于降低 RTT；不得影响上层对象的 ObjId计算。

### 5.2 DirObject（ObjType: `cydir`）

实现：`src/ndn-lib/src/dirobj.rs`

DirObject 本质上是 `BaseContentObject + 目录统计字段 + SimpleObjectMap`。

#### 5.2.1 JSON 示例（完整结构）

（示例来自 `doc/demo_dir.json` 的结构化形式，做了裁剪）

```json
{
  "name": "root",
  "create_time": 1755329130,
  "last_update_time": 1755329130,

  "total_size": 1268095537,
  "file_count": 74,
  "file_size": 1261121897,

  "body": {
    "subdir": "cydir:0db931deec2d6842e42a8699e1824f4f187212057752f4f684c2ad78fb35e246",
    "readme.txt": {
      "obj_type": "cyfile",
      "body": {
        "name": "readme.txt",
        "size": 123,
        "content": "mix256:..."
      }
    }
  }
}
```

#### 5.2.2 ObjId 计算规则

DirObject 的 ObjId 计算必须使用 5.1.2 的归一化规则：

- `body` 中最终参与哈希的值是 **子项 ObjId 字符串**（而不是子项对象正文）。

### 5.3 ObjectArray（ObjType: `cylist` / `cylist-mtree`）

> 实现未稳定

实现：`src/ndn-lib/src/object_array/object_array.rs`

ObjectArray 用于按 index 存放 `ObjId` 列表，并支持基于 Merkle Tree 的单项/批量 proof。

#### 5.3.1 Body JSON

```json
{
  "root_hash": "<base32>",
  "hash_method": "sha256",
  "total_count": 1024
}
```

#### 5.3.2 单项 Proof 的 JSON 编码

> 实现未稳定

实现提供 `ObjectArrayItemProofCodec`（`src/ndn-lib/src/object_array/proof.rs`），将 proof path 编码为 JSON 数组（每个节点的哈希值用 base64）：

```json
[
  {"i": "0", "v": "AAECAwQ="},
  {"i": "1", "v": "BQYHCAk="}
]
```

规范：

- `root_hash` 为 Merkle Tree 根哈希的 base32 字符串。
- `hash_method` 取值见 `HashMethod`（实现当前用于 proof 的 hash 算法）。

### 5.4 ChunkList

ChunkList 用于表示“文件内容由多个 Chunk 组成”。本仓库存在两套实现：

1) **SimpleChunkList（ObjType: `clist`）**：对象数据为 `ChunkId` 数组；ObjId 带总长度前缀（mix 风格）。
2) **ChunkList（ObjType: `cl`/`clist`/`clist-fix`/`cl-sf`）**：基于 ObjectArray 的通用实现，ObjId 由 `ChunkListBody` 计算。

#### 5.4.1 SimpleChunkList（ObjType: `clist`）

实现：`src/ndn-lib/src/chunk/simple_chunk_list.rs`

JSON（对象数据本体）为数组：

```json
[
  "mix256:...",
  "mix256:..."
]
```

ObjId 特殊规则：

- 先对上述数组按 3.1 规则计算 `sha256(json_array)` 得到 `H`。
- 再将 `varint(total_size) || H` 作为最终 `obj_hash`。

因此 `clist` 的 `obj_hash` 并非纯 hash bytes；其前缀可解码出 `total_size`。

#### 5.4.2 ChunkListBody（通用 ChunkList）

实现：`src/ndn-lib/src/chunk/chunk_list.rs`

```json
{
  "object_array": {
    "root_hash": "<base32>",
    "hash_method": "sha256",
    "total_count": 4096
  },
  "total_count": 4096,
  "total_size": 123456789,
  "fix_size": null
}
```

其中 `fix_size` 非空表示“定长 chunk list”。

### 5.5 ObjectMap（ObjType: `cymap-mtp` / `cymap`）

实现：`src/ndn-lib/src/object_map/object_map.rs`

ObjectMap 是“key -> ObjId”的可证明映射，支持 Merkle Tree proof。

#### 5.5.1 Body JSON（用于解析/传输）

```json
{
  "root_hash": "<base32>",
  "hash_method": "sha256",
  "total_count": 123,
  "content": null
}
```

#### 5.5.2 ObjId 计算规则（重要：与普通 JSON 不同）

ObjectMap 的 ObjId 只由以下三项决定（实现中称为 `ObjectMapBodyInner`）：

- `root_hash`
- `hash_method`
- `total_count`

即使 `content` 字段存在（内存模式），也不得影响 ObjId。

### 5.6 TrieObjectMap（ObjType: `cytrie` / `cytrie-s`）

实现：`src/ndn-lib/src/trie_object_map/object_map.rs`

TrieObjectMap 使用 MPT（Merkle Patricia Trie）结构提供 `key -> ObjId` 与 proof。

Body JSON：

```json
{
  "root_hash": "<base32>",
  "hash_method": "sha256",
  "total_count": 123
}
```

proof 编码：proof nodes 以 base64 字符串序列化成 JSON（见 `TrieObjectMapProofNodesCodec`）。

JSON 示例（nodes 编码结果）：

```json
{
  "nodes": [
    "AAECAwQ=",
    "BQYHCAk="
  ]
}
```

## 6. BaseContentObject（抽象基类）

实现：`src/ndn-lib/src/base_content.rs`

BaseContentObject 是一组通用内容元信息字段：

- `did`: 可选 DID
- `name`: 友好名称（文件名等）
- `author`: 作者字符串
- `owner`: DID
- `create_time`, `last_update_time`: 时间戳（秒）
- `copyright`: 可选
- `tags`, `categories`: 标签/分类
- `base_on`: 可选，表示基于另一个内容
- `directory`, `references`: 扩展信息
- `exp`: 可选过期时间

BaseContentObject 本身没有 ObjType；不能作为独立标准对象实体化。它通过 `#[serde(flatten)]` 被下述对象复用。

## 7. FileObject（ObjType: `cyfile`）

实现：`src/ndn-lib/src/fileobj.rs`

FileObject = `BaseContentObject` + 文件内容引用：

- `size`: 文件总大小（字节）
- `content`: `ChunkId` 或 `ChunkListId`（即 ObjId 字符串）
- 允许额外自定义字段：实现中通过 `meta: HashMap<String, Value>` 并 `flatten` 到顶层。

### 7.1 JSON 示例（最小）

```json
{
  "name": "readme.txt",
  "create_time": 1755329130,
  "last_update_time": 1755329130,
  "size": 3813,
  "content": "mix256:..."
}
```

### 7.2 JSON 示例（带自定义字段）

```json
{
  "name": "readme.txt",
  "create_time": 1755329130,
  "last_update_time": 1755329130,
  "size": 3813,
  "content": "clist:...",

  "mime": "text/plain",
  "sha1": "...",
  "x-extra": {"k": "v"}
}
```

## 8. PkgMetaObject / PackageMeta（ObjType: `pkg`）

实现：`src/package-lib/src/meta.rs`

PackageMeta 继承 FileObject（JSON 上通过 `flatten`）并增加版本语义：

- `version`: 版本字符串（semver 兼容）
- `version_tag`: 可选标签（如 `stable`/`beta`/`latest`）
- `deps`: 依赖映射（`pkg_name -> version_req_str`）

### 8.1 JSON 示例

```json
{
  "name": "test-pkg",
  "author": "author1",
  "owner": "did:bns:buckyos.ai",
  "create_time": 1767754917,
  "last_update_time": 1767754917,
  "exp": 1770346917,

  "size": 123,
  "content": "mix256:deadbeef",

  "version": "1.0.0",
  "version_tag": "stable",
  "deps": {
    "dep1": ">=1.0.0"
  }
}
```

### 8.2 AppDoc

实际使用中，BuckyOS在PkgMetaObject上增加了更多约束，构造了可安装在BuckyOS上的AppDoc.
- AppDoc是PkgMetaObject的特殊形式，所有的AppDoc都是合法的PkgMetaObject
- AppDoc实现了BuckyOS的`App安装协议`
- AppDoc目前支持3总类型的App: AppService(有docker),StaticWeb, Agent

### 8.2.1 AppDoc 实例

1. 标准的，基于docker的app

```json
{
  "@schema": "buckyos.app.meta.v1",
    "did":"did:bns:filebrowser.buckyos",
    "name": "buckyos_filebrowser",
    "version": "2.27.0",
    "meta": {    
         "show_name": "File Browser",
        "icon_url": "https://example.com/icon.png",
        "homepage_url": "https://example.com",
        "support_url": "https://example.com/support","en": "A web-based file manager.", "zh": "一个基于 Web 的文件管理器。","license": "Apache-2.0" 
    },
    "pub_time": 1760000000,
    "exp": 0,
    "deps": {},
    "tas": ["file", "web", "nas"],
    "category": "app",

    "author": "Filebrowser Team",
    "owner": "did:bucky:authorxxxx",
    "curators" ["did:bns:curator1","did:web:gitpot.ai"],

    //付费应用填写，这个比较精细（站在对用户公开的角度来支持合适的细节），支持多种版本购买
    "economics": {
        "version" : "*", //购买的是所有版本， ^1.0 只购买1.0版本
        "revenue_split": { "author": 0.8, "source": 0.15, "referrer": 0.05 },
        "payment": { "usdb": {
            "prices" : "1.99",
            "contract" : "付款合约地址",// usdb有默认的付费合约地址，这里不应设置
        } }
    },

// install主要是列出app希望申请的资源
  "install": {
    "selector_type": "single",
    "install_config_tips": {
      "data_mount_point": ["/data"],
      "local_cache_mount_point": [],
      "service_ports": { "www": 80 },
      "container_param": null,
      "custom_config": {}
    },
    "services": [
      {
        "name": "www",
        "protocol": "tcp",
        "container_port": 80,
        "expose": {
          "mode": "gateway_http",
          "default_subdomain": "file",
          "default_path_prefix": "/",
          "tls": "optional"
        }
      }
    ],
    "mounts": [
      { "kind": "data", "container_path": "/data", "persistence": "keep_on_uninstall" },
      { "kind": "config", "container_path": "/config", "persistence": "delete_on_uninstall" },
      { "kind": "cache", "container_path": "/cache", "persistence": "delete_on_uninstall" }
    ],
    "network": { "bind_default": "127.0.0.1", "allow_bind_public": true }
  },
  "permissions": {
    "fs": {
      "sandbox": true,
      "home": {
        "private": { "read": false, "write": false },
        "public": { "read": true, "write": true },
        "shared": { "read": true, "write": true }
      }
    },
    "system": { "need_privileged": false, "devices": [], "capabilities": [] }
  }
}

```

2. Static web App

该类型的app,pkg_list只有web

```json
{
  "name": "buckyos_systest", 
  "version": "0.5.1", 
  "description": {
    "detail": "BuckyOS System Test App"
  },
  "categories": ["web"],
  "create_time": 1743008063,
  "last_update_time": 1743008063,
  "exp": 1837616063,
  "tag": "latest",
  "author": "did:web:buckyos.ai",
  "owner": "did:web:buckyos.ai",
  "show_name": "BuckyOS System Test",
  "selector_type": "static",
  "install_config_tips": {
  },
  "pkg_list": {
    "web": {
      "pkg_id": "nightly-linux-amd64.buckyos_systest#0.5.1"
    }
  }
}     
```

3. Agent

Agent类型的App，所有的提示词都包含在了meta中，pkg_list为空空

## 9. PathObject（ObjType: `cypath`）

实现：`src/ndn-lib/src/fileobj.rs`

PathObject 用于表达“语义路径 -> 目标 ObjId”的可验证绑定（常以 JWT 传输并签名）。

字段：

- `path`: 不含域名的路径（例如 `/repo/readme.txt`）
- `uptime`: 绑定上线时间戳
- `target`: 目标 `ObjId`
- `exp`: 过期时间戳

JSON 示例：

```json
{
  "path": "/repo/pub_meta_index.db",
  "uptime": 1730000000,
  "target": {"obj_type": "cyfile", "obj_hash": [1, 2, 3, 4]},
  "exp": 1820000000
}
```

## 10. InclusionProof（ObjType: `cyinc`）

实现：`src/ndn-lib/src/base_content.rs`

InclusionProof 表达“收录者对内容的收录证明”。实现建议将其 JSON 作为 JWT claims，并由收录者签名。

JSON 示例：

```json
{
  "content_id": "cyfile:1234",
  "content_obj": {"name": "test_app"},

  "curator": "did:web:gitpot.ai",
  "editor": ["did:web:wcy.gitpot.ai"],
  "meta": {"note": "reviewed"},
  "rank": 1,
  "collection": ["apps"],
  "review_url": "https://gitpot.ai/reviews/apps/test_app",

  "iat": 1730000000,
  "exp": 1730000000
}
```

## 11. ActionObject（ObjType: `cyact`）

实现：`src/ndn-lib/src/action_obj.rs`

ActionObject 表达“某主体对某目标执行某动作”的事件。
实现建议将其 JSON 作为 JWT claims，并由相关设备签名。

已定义 action 常量：

- `viewed`
- `download`
- `installed`
- `shared`
- `liked`
- `unliked`
- `purchased`

JSON 示例：

```json
{
  "subject": {"obj_type": "test", "obj_hash": [18, 52]},
  "action": "download",
  "target": {"obj_type": "cyfile", "obj_hash": [1, 2, 3, 4]},
  "details": {"channel": "web"},
  "iat": 1730000000,
  "exp": 1730003600
}
```

## 12. RelationObject（ObjType: `cyrel`）

实现：`src/ndn-lib/src/relation_obj.rs`

RelationObject 表达两个对象间的弱关系（观察/标注视角），可带自定义扩展字段（通过 `flatten` 到顶层）。

已定义关系类型：

- `same`
- `part_of`

### 12.1 JSON 示例：SameAs

```json
{
  "source": {"obj_type": "test", "obj_hash": [18, 52]},
  "relation": "same",
  "target": {"obj_type": "test", "obj_hash": [18, 52]}
}
```

### 12.2 JSON 示例：PartOf（带 range）

```json
{
  "source": {"obj_type": "test", "obj_hash": [18, 52]},
  "relation": "part_of",
  "target": {"obj_type": "test", "obj_hash": [18, 52]},
  "range": {"start": 0, "end": 100}
}
```

（实现中 `range` 位于 `body`/flatten 区域。）

### 12.3 ObjId 示例（来自单元测试，hex 形式）

- `SameAs(test:1234 -> test:1234)` 的 ObjId：`cyrel:e0ad5f3b656a883de323e4c6e7999207ce3026d3fa5dcb47518d2caeb4d92aa0`
- `PartOf(test:1234, range 0..100)` 的 ObjId：`cyrel:cf2ff05aaa9165e9c7fb2bc642cea5a02b730d9f0415f907fc9d4a6bad66bca9`

## 13. Rust 参考结构定义

以下 Rust 定义与仓库实现保持一致（便于对照字段与序列化行为）。

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ObjId {
    pub obj_type: String,
    pub obj_hash: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileObject {
    #[serde(flatten)]
    pub content_obj: BaseContentObject,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    #[serde(flatten)]
    pub meta: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirObject {
    #[serde(flatten)]
    pub content_obj: BaseContentObject,
    #[serde(default)]
    #[serde(flatten)]
    pub meta: HashMap<String, serde_json::Value>,
    pub total_size: u64,
    pub file_count: u64,
    pub file_size: u64,
    #[serde(flatten)]
    pub object_map: SimpleObjectMap,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PathObject {
    pub path: String,
    pub uptime: u64,
    pub target: ObjId,
    pub exp: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InclusionProof {
    pub content_id: String,
    pub content_obj: serde_json::Value,
    pub curator: name_lib::DID,
    pub editor: Vec<String>,
    pub meta: Option<serde_json::Value>,
    pub rank: i64,
    #[serde(default)]
    pub collection: Vec<String>,
    #[serde(default)]
    pub review_url: Option<String>,
    pub iat: u64,
    pub exp: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RelationObject {
    pub source: ObjId,
    pub relation: String,
    pub target: ObjId,
    #[serde(flatten)]
    pub body: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub iat: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exp: Option<u64>,
}
```
