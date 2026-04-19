# CYFS协议现有标准对象

本文档只统计当前仓库里已经有具体结构定义、编码逻辑，或者可以直接给出实例的 CYFS 标准对象。

- 统一“编码后写文件，再从文件解码”的回环测试已经覆盖：
  - `cyfile` / `cypath` / `cyinc` / `cyrel` / `cyact` / `cymsg` / `cymsgr` / `pkg`
- 对应测试：
  - `src/ndn-lib/src/object.rs` 中的 `test_non_chunk_named_object_file_roundtrip`
  - `src/package-lib/src/meta.rs` 中的 `test_package_meta_file_roundtrip`

## 1. 当前已实现的标准对象

| ObjType | Rust类型 | 说明 | 文件回环测试 |
| --- | --- | --- | --- |
| `cyfile` | `FileObject` | 文件对象 | 是 |
| `cydir` | `DirObject` | 目录对象 | 否，单独有 `gen_obj_id()` |
| `cypath` | `PathObject` | 路径映射对象 | 是 |
| `cyinc` | `InclusionProof` | 收录证明 | 是 |
| `cyrel` | `RelationObject` | 对象关系 | 是 |
| `cyact` | `ActionObject` | 行为对象 | 是 |
| `cymsg` | `MsgObject` | 消息对象 | 是 |
| `cymsgr` | `MsgReceiptObj` | 消息回执对象 | 是 |
| `cymap` | `SimpleObjectMap` | 简单对象映射容器 | 否，作为容器组件使用 |
| `clist` | `SimpleChunkList` | 简单 ChunkList | 否，属于 Chunk 相关对象 |
| `pkg` | `PackageMeta` | 包元数据对象 | 是 |

## 2. 非 Chunk NamedObject 实际实例

下面的内容直接来自当前新增测试打印出的 `objid:objjson` 结果，可作为最小可用实例。

### 2.1 `cyfile` / `FileObject`

```text
cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed:{"author":"alice","content":"mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5","create_time":1700000000,"last_update_time":1700000120,"mime":"text/plain","name":"hello.txt","size":12}
```

### 2.2 `cypath` / `PathObject`

```text
cypath:a24ecb3ebd0e12b3e87d78ae33cf0e5aa7c8764e475531a74b963f11eef12ee6:{"exp":1700086600,"path":"/repo/apps/demo","target":"cyfile:1234567890abcdef","uptime":1700000200}
```

### 2.3 `cyinc` / `InclusionProof`

```text
cyinc:8b3dcc5e386d81cf636dcaff5ebb9a470aa332fd21dfb2c1669e3c2279e58299:{"collection":["docs","featured"],"content_id":"cyfile:1234567890abcdef","content_obj":{"content":"mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5","name":"hello.txt","size":12},"curator":"did:web:curator.example.com","editor":["did:web:editor.example.com"],"exp":1703110300,"iat":1700000300,"meta":{"comment":"stable","score":9.6},"rank":88,"review_url":"https://curator.example.com/review/hello.txt"}
```

### 2.4 `cyrel` / `RelationObject`

```text
cyrel:c79bd4eee65382062625c86ad5c9845882743dc5fe125a74798b40167cae0461:{"exp":1700086800,"iat":1700000400,"note":"excerpt","range":{"end":12,"start":0},"relation":"part_of","source":"cyfile:1234567890abcdef","target":"sha256:1122334455667788"}
```

### 2.5 `cyact` / `ActionObject`

```text
cyact:3dcf493420e0d82f36e9c3ea4484c3cc29b5001694354d05b03199f14d88dcef:{"action":"viewed","base_on":"cyact:cccccccccccccccc","details":{"device":"desktop","source":"unit-test"},"exp":1700086900,"iat":1700000500,"subject":"cyfile:aaaaaaaaaaaaaaaa","target":"cymsg:bbbbbbbbbbbbbbbb"}
```

### 2.6 `cymsg` / `MsgObject`

```text
cymsg:179fa355bad0e0a154f3654b61fe4187d78561fea73bd83f79dff81d0f7a1676:{"content":{"content":"{\"status\":\"ok\"}","format":"application/json","machine":{"data":{"level":3,"urgent":true},"intent":"sync"},"refs":[{"label":"attachment","role":"input","target":{"obj_id":"cyfile:1234567890abcdef","type":"data_obj","uri_hint":"cyfs://hello.txt"}}],"title":"Hello"},"created_at_ms":1700000000000,"expires_at_ms":1700086400000,"from":"did:web:alice.example.com","kind":"chat","lang":"zh-CN","nonce":7,"priority":1,"proof":"proof-001","thread":{"correlation_id":"corr-001","reply_to":"cymsg:010203040506","topic":"release","tunnel_id":"tnl-001"},"to":["did:web:bob.example.com","did:web:carol.example.com"],"workspace":"did:web:workspace.example.com"}
```

### 2.7 `cymsgr` / `MsgReceiptObj`

```text
cymsgr:7b7e89c2c51b2000a10e8fcdbf8accd779d8995c25c1b4e1ba052cb5d9fd8814:{"at_ms":1700000100000,"group_id":"did:web:group.example.com","iss":"did:web:inbox.example.com","msg_id":"cymsg:010203040506","reader":"did:web:bob.example.com","reason":"delivered","status":"accepted"}
```

### 2.8 `pkg` / `PackageMeta`

```text
pkg:368aa598c5e58c225c149f5e15a59cf548be23ddf6871456adad28519753727e:{"author":"alice","channel":"nightly","content":"mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5","create_time":1700000000,"deps":{"demo.dep":">=0.9.0"},"exp":1700086400,"last_update_time":1700000100,"name":"demo.pkg","owner":"did:bns:buckyos.ai","size":4096,"version":"1.2.3","version_tag":"stable"}
```

## 3. 已实现的容器/Chunk相关对象实例

这些对象当前仓库里已有结构和编码逻辑，但不在本次“非 Chunk NamedObject 文件回环测试”的范围内。

### 3.1 `cydir` / `DirObject`

```json
{
  "name": "root",
  "create_time": 1700000000,
  "last_update_time": 1700000120,
  "total_size": 12,
  "file_count": 1,
  "file_size": 12,
  "body": {
    "hello.txt": {
      "obj_type": "cyfile",
      "body": {
        "author": "alice",
        "content": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
        "create_time": 1700000000,
        "last_update_time": 1700000120,
        "mime": "text/plain",
        "name": "hello.txt",
        "size": 12
      }
    }
  }
}
```

说明：`DirObject::gen_obj_id()` 计算时会先把 `body` 中的真实对象归一化为子对象 `ObjId` 字符串，再计算目录自身 `ObjId`。

### 3.2 `cymap` / `SimpleObjectMap`

```json
{
  "body": {
    "hello.txt": "cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed",
    "note": {
      "obj_type": "cymsg",
      "body": {
        "from": "did:web:alice.example.com",
        "to": [
          "did:web:bob.example.com"
        ],
        "kind": "chat",
        "content": {
          "title": "Hello",
          "content": "hi"
        }
      }
    }
  }
}
```

### 3.3 `clist` / `SimpleChunkList`

```json
[
  "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5"
]
```

说明：`SimpleChunkList::gen_obj_id()` 不是普通 JSON 对象哈希，而是先对数组内容求 `clist` 基础哈希，再把总长度编码到 `ObjId` 中。

## 4. 当前只保留 ObjType，尚无独立实例结构的类型

以下类型码已经在 `src/ndn-lib/src/lib.rs` 中保留，但当前仓库里没有稳定、可直接举例的独立结构定义，本文不强行给伪实例：

- `cypack`
- `cytrie`
- `cytrie-s`
- `cymap-mtp`
- `cylist`
- `cylist-mtree`
- `cl`
- `clist-fix`
- `cl-sf`

后续如果这些类型补齐了结构体和编码入口，应当把本文升级为“实例文档”，并同步补上对应的回环测试。
