# CYFS Protocol

> `CYFS` 或 `cyfs://` 都是在说协议；单独说 `cyfs` 时，通常指基于 CYFS Protocol 定义的标准对象实现的 DFS。

当前文档状态：0.9，最新草案，领先所有实现。

## Named Data 的基本概念

Named Data 常常也被称作内容寻址。简单地说，就是用内容的 Hash 作为其 `ObjectId`，并在此基础上实现内容之间的引用，进而形成内容网络。

### 从 Chunk 开始

我们使用 `sha256sum` 对一个文件进行计算，可以得到：

```text
d1127e660d0de222a3383609d74ff8d4b4ba97a226f861e184e1f92eee25d3b9  README.md
```

此时，`README.md` 文件的 `ChunkId` 为：

```text
sha256:d1127e660d0de222a3383609d74ff8d4b4ba97a226f861e184e1f92eee25d3b9
```

其格式为 `{hash_type}:{hash_data_hex_text}`。

上述 `ChunkId` 还有另一种 base32 编码方式：

```text
base32_rfc4648_encode("sha256:" + hash_data)
```

在系统中，使用上述两种 `ChunkId` 来表达同一个`不再修改的数据块`是等价的。

### 通过 ChunkId 可靠地获取数据

在支持 `cyfs://` 的 Zone 中，我们可以使用 3 个常见 URL 来获取上述 `README.md`：

```text
http://$zoneid/ndn/{sha256_chunkid}
http://$zoneid/ndn/{base32_chunkid}
http://{base32_objid}.$zoneid/
```

第三种形式在协议上支持，但一般不会默认开启，多用于一些特别大的DirObject。

在标准浏览器里，使用 `http` 会触发“不安全警告”。但只要浏览器完整支持了 `cyfs://`，即使使用 `http` 也是相对安全的，因为浏览器可以基于 URL 中的 Hash 信息对获得的数据进行校验；校验通过后，该内容就是安全可靠的。

通过上述流程，我们可以理解 `cyfs://` 的关键设计理念：

1. “已发布内容”是互联网上重要的一类数据，这类数据在发布后完全公开并且不再修改。
2. “已发布内容”在 `cyfs://` 中被称作 `NamedData`（非结构化）或 `NamedObject`（结构化），都拥有 `Named Object Id`（`ObjectId`）。根据计算 Hash 的方法不同，`ObjectId` 有不同的类型，我们可用 `{obj_type}:{hash_data}` 表达 `ObjectId`。
3. `cyfs:// GET` 协议对标准 `HTTP GET` 做了扩展。其核心差异在于：客户端在发起请求时已经知晓 `Named Object Id`，这样就可以不依赖 TLS 的可信传输来校验获得的数据，无惧数据篡改。
4. `cyfs://` 的关键设计目标之一，是通过改进 `http://` 的使用方式来更高效地传输 `NamedObject`。由于 `http://` 的明文特性，它还更适合做分布式 CDN、路由缓存加速、减少 404，以及优化整体网络性能。

这里需要额外说明：`http` 只解决“防篡改”，不解决“防监听”。也就是说，使用 `http` 时默认接受攻击者可能知道自己请求了某个公开内容。

### 在现有语义 URL 上增加 ChunkId 支持

上述流程能成功工作的前提，是 `客户端在发起请求时已经知晓 Named Object Id`。但并不是所有 URL 都适合直接带上编码后的 `ObjectId`。这类传统 URL 在 `cyfs://` 中又被称作“语义 URL”，其路径对应的已发布内容允许发生变化。

通过语义 URL 获得 `NamedObject`，理论上可以分为两步：

第一步：通过语义 URL 得到 `ObjectId`。  
第二步：基于 `ObjectId`，使用前述流程获取完整的 `NamedObject`。

从实现简单的角度，我们可以要求先用传统 URL `https://$zoneid/buckyos/readme.md` 获得 `readme.md` 文件的 `ChunkId`，再用 `http://$zoneid/ndn/$chunkid` 可信地获得 `readme.md` 的内容。`cyfs://` 的设计是解耦的，并不反对采用上述流程。对于一些系统来说，通过这种方式集成 `cyfs://`，以减少 `https` 的使用，可能是最简单快捷的办法。

能不能在一次 `http` 请求内完成上面两步？可以。方法是在一次响应里同时携带数据和用于验证的信息：

1. 在 HTTP Header 中以可验证的方法，说明该语义 URL 当前指向哪个 `ObjectId`。
2. 继续返回文件内容，客户端基于上一步得到的 `ObjectId` 对响应体做校验。

支持 CYFS 扩展的 HTTP 响应如下：

```text
<http-header>
cyfs-obj-id: $obj_id
cyfs-path-obj: $path_jwt
</http-header>
<body/>
```

`cyfs-path-obj` 是关键扩展 Header。其内容是一个 JWT（签名后的 JSON 对象），解码后如下：

```json
{
  "path": "/buckyos/readme.txt",
  "target": "sha256:xxxx",
  "uptime": 232332,
  "exp": 132323
}
```

这个对象只说明一件事：某个路径（不含域名）当前指向哪个 `NamedObject` 的 `ObjectId`，以及这条绑定关系的上线时间 `uptime` 和过期时间 `exp`。

在使用 `target ObjectId` 之前，需要先验证 `PathObject`（JWT）：

0. 获得可信公钥，通常它与 URL 的 hostname 相关。
1. 使用公钥验证 JWT，确定该路径确实指向该 `ObjectId`。
2. 与本地缓存的 `PathObject`（如有）比较时间戳，防止重放攻击。

获得可信公钥的流程是解耦的。`cyfs://` 通过一个可扩展的框架，目前支持下面 3 种方法来获取验证 `PathObject` 的公钥：

- 将公钥保存在 DNS Record 里，适用于完全没有 HTTPS 证书的情况。
- 使用 W3C DID Document 机制获取公钥，适用于已有 HTTPS 证书、但希望减少 HTTPS 流量使用的情况。
- 使用 BNS（智能合约）查询 `zoneid` 对应的可信公钥，适用于完全没有 HTTPS 证书，但客户端能读取智能合约状态的情况。

服务提供者可以根据自己的实际情况，在兼容性和性能之间权衡选择。

让内容的可信发布不依赖中心化 CA，也是 `cyfs://` 的另一个核心设计目标。

### PathObject 也是 NamedObject

构造可验证的 `PathObject` 分为两步：

1. 对 `PathObject` JSON 做稳定编码并计算 Hash。
2. 对该对象或其摘要进行签名，并以 JWT 的方式传输。

因为有计算 Hash 的过程，所以任何一个 JSON 都可以 `NamedObject` 化并得到一个 `ObjectId`。

```ts
import jsSHA from 'jssha'

function sha256Utf8(text: string): string {
    const sha = new jsSHA('SHA-256', 'TEXT', { encoding: 'UTF8' })
    sha.update(text)
    return sha.getHash('HEX')
}

function canonicalizeJson(value: any): any {
    if (value === null || typeof value !== 'object') return value

    if (Array.isArray(value)) {
        return value.map(canonicalizeJson)
    }

    const result: Record<string, any> = {}
    for (const key of Object.keys(value).sort()) {
        result[key] = canonicalizeJson(value[key])
    }
    return result
}

function toCanonicalJsonString(value: any): string {
    return JSON.stringify(canonicalizeJson(value))
}

function buildObjId(objType: string, objJsonStr: string): string {
    const hash = sha256Utf8(objJsonStr)
    return `${objType}:${hash}`
}

function buildNamedObjectByJson(objType: string, jsonValue: any): [string, string] {
    const jsonStr = toCanonicalJsonString(jsonValue)
    const objId = buildObjId(objType, jsonStr)
    return [objId, jsonStr]
}

const pathObject = {
    path: '/articles/intro.md',
    target: 'cyfile:1234567890abcdef',
    uptime: 1709999900,
    exp: 1710000000,
}

const [objId, jsonStr] = buildNamedObjectByJson('cypath', pathObject)
console.log(objId)
console.log(jsonStr)
```

这段代码里最关键的是两点：

- `buildObjId` 只负责对传入的 JSON 字符串做 Hash。
- “稳定编码”是在 `buildNamedObjectByJson` 里完成的，也就是先排序对象字段，再 `JSON.stringify`。

基于上述流程，可以得到下面结论：

- 通过 `Named Object Id` 可以获得一个可验证的 JSON。
- 使用 JSON 稳定编码算法后，相同语义的 JSON 每次都会编码得到相同的 `Named Object Id`。

通过 HTTP 协议获得一个 `NamedObject`，是 `cyfs://` 获取结构化数据的部分。从接口语义上看，我们总是假设 `NamedObject` 不太大，可以通过一次原子的 `GET` 完成获取；而打开一个 Chunk（`NamedData`）通常是 `OpenStream`（`OpenReader`）语义，需要支持断点续传等更复杂的能力。

也正是因为语义不同，`ndn_client` 提供了两类接口分别处理 `NamedObject` 和 `NamedData`。这意味着即使我们不知道一个 URL 最终指向什么具体内容，也至少要知道它指向的是哪一类数据。

### 使用 FileObject 而不是 Chunk

很多时候，直接使用 Chunk 发布内容并不方便，因为发布内容时往往还要同时发布一些基础元信息，比如大小、文件名、MIME 类型等。因此，我们可以发布一个包含必要元信息的 `NamedObject`，在这个对象的元数据中去引用 Chunk。CYFS 定义了这样一个标准对象 `FileObject`，一个典型示例如下：

```json
{
  "author": "alice",
  "content": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "create_time": 1700000000,
  "last_update_time": 1700000120,
  "mime": "text/plain",
  "name": "hello.txt",
  "size": 12
}
```

编码后得到的 `ObjectId` 为：

```text
cyfile:7d28f1f3c4f9405ea9812bd6db6d7d25986c8c678fc12f1de4cd6222852700ed
```

这个例子里：

- `content` 指向文件内容对应的 `ChunkId` 或 `ChunkListId`。
- `name`、`size`、`create_time`、`last_update_time` 是 `FileObject` 的基础字段。
- `mime` 不是框架内置字段，但可以像这样直接平铺在 JSON 根上作为自定义元信息。

`cyfile` 是 `cyfs://` 定义的标准对象。标准对象约定了一些字段的含义和是否可选；同时得益于 JSON 的可扩展性，用户也可以在此基础上扩展自己的自定义字段。

通过 `FileObject` 发布 Chunk 后，我们可以通过下面流程完成文件下载：

```text
file_obj = get_obj_by_url()
chunk_reader = open_chunk_by_url(file_obj.content)
```

上述逻辑很简单，但它需要与服务器通信两次。能否只通信一次？可以。

按一般的 `cyfs://` 规范，用下面 3 个 URL 都可以下载 `FileObject` 的内容：

```text
file_reader = open_reader_by_url("http://$zone_id/readme.md/@/content")
file_reader = open_reader_by_url("http://$zone_id/readme.md")
file_reader = open_reader_by_url("http://$zone_id/cyfile:513788234cfb679121c148ba4fd768390bf948bfb17d6cfced79b205d5c82c9d")
```

在第一个 URL 中，`/@/content` 表示对 `FileObject` 执行一层 `inner_path` 解析。服务器在处理请求时，需要检查当前对象上 `content` 字段的值：

- 如果该值不是 `ObjectId`，则直接返回该字段值。
- 如果该值是 `ObjectId`，则默认继续返回该 `ObjectId` 指向的对象或数据。

在引入 `inner_path` 后，我们仍然可以通过 CYFS Header 对返回结果进行验证：

```text
open_reader_by_url("http://$zone_id/ndn/all_images/@/readme/@/content")

<header>
cyfs-path-obj: $path_jwt      (target 是 $dir_obj_id)
cyfs-parents: [$dir_obj, $file_obj]
cyfs-obj-id: $objid
cyfs-chunk-size: $full_chunk_size
</header>
<body />
```

这里的例子里包含两层 `inner_path`，因此 `cyfs-parents` 里也包含 2 个完整对象：`DirObject` 和 `FileObject`。

客户端基于这些 Header 的验证流程可以简化为：

1. 从原始 URL 中拆出语义路径部分和两层 `inner_path`。这里 `cyfs-path-obj` 负责把语义路径绑定到 `DirObject`。
2. 获取 `$zone_id` 对应的可信公钥，验证 `cyfs-path-obj` 的签名，确认它的 `target` 确实是 `DirObject` 的 `ObjectId`。
3. 对 `cyfs-parents[0]` 做标准 `NamedObject` 校验，确认 `DirObject` 的 JSON 与 `cyfs-path-obj.target` 匹配。
4. 在 `DirObject` 上执行第一层 `inner_path`，例如 `/readme`，确认其结果确实指向 `cyfs-parents[1]` 对应的 `FileObject`。
5. 对 `cyfs-parents[1]` 做标准 `NamedObject` 校验，确认 `FileObject` 的 JSON 自己是可验证的。
6. 在 `FileObject` 上执行第二层 `inner_path`，例如 `/content`，确认其结果等于 `cyfs-obj-id`。
7. 最后验证 HTTP Body：
   - 如果 Body 是 `NamedObject JSON`，就按前文的稳定编码规则重新计算 `ObjectId`，并与 `cyfs-obj-id` 比较。
   - 如果 Body 是 Chunk 数据，就直接计算 `ChunkId` 并与 `cyfs-obj-id` 比较；`cyfs-chunk-size` 只是辅助检查完整大小。

这样客户端虽然只发起了一次 HTTP 请求，但逻辑上仍然完成了：

```text
语义路径 -> DirObject -> FileObject -> content -> 最终返回内容
```

这一整条链路的完整验证。

### inner_path 的使用规范

1. `inner_path` 用于在 `NamedObject` 内部做字段寻址，其语义与 JSON Path 的“按字段逐层取值”一致。
2. URL 层使用字面量 `"/@/"` 作为分隔符；每出现一次，就表示“在当前对象上继续执行一层 `inner_path`”。
3. `http://$zone_id/all_images/@/readme/@/content` 应理解为两步：
   - 先在 `all_images` 对应的对象上执行 `/readme`。
   - 再在上一步得到的对象上执行 `/content`。
4. 如果某一层 `inner_path` 的结果是 `ObjectId`，默认继续解引用并返回它指向的对象或 Chunk。
5. 如果某一层 `inner_path` 的结果不是 `ObjectId`，则该值就是最终返回值；此时不会再继续解引用。
6. 如果中间某一层返回的不是 `ObjectId`，但 URL 里后面还有新的 `"/@/"` 段，则该请求非法。

### 得到 Raw Object JSON

有时我们希望简单高效地得到某个对象的原始 JSON，而不需要任何额外 Header，也不需要服务端自动展开更多引用。可以使用 `resp=raw`：

```text
object_reader = open_object_by_url("http://$zone_id/allimages/@/readme.md?resp=raw")

返回：
<http-header>
没有任何附加头
</http-header>
<body>
readme.md 的 FileObject JSON
</body>
```

这个模式适合客户端已经验证了 `http://$zone_id/allimages` 这个 `DirObject`，接下来只是批量遍历内部 child 对象的场景。

### ChunkList

任意长度的内容都可以计算 Hash 并得到 `ChunkId`，但 `cyfs://` 从整体系统性能出发，限制底层存储系统保存的标准 Chunk 大小上限为 `32MB`。也就是说，系统内部真正落盘的 Chunk 不能超过 `32MB`。基于这个限制，对于超过 `32MB` 的文件，就需要引入 `ChunkList`。

它的逻辑很简单：

1. 先把大文件按 `32MB` 分块。
2. 每一块分别计算 `ChunkId`。
3. 再把这些 `ChunkId` 按顺序组成一个列表对象。

一个 `SimpleChunkList` 的 JSON 形态就是一个字符串数组，例如：

```json
[
  "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "mix256:91c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d6"
]
```

`ChunkList` 自己也有一个 `ObjectId`。需要注意的是，`SimpleChunkList` 的 `ObjectId` 不是普通 `NamedObject` 的“稳定 JSON 直接 Hash”，而是由下面两部分共同决定：

- `ChunkId` 列表的顺序内容。
- 整个文件的总大小 `total_size`。

因此，一个大文件对应的 `FileObject` 会变成：

```json
{
  "name": "movie.mp4",
  "size": 67108864,
  "content": "clist:1234567890abcdef"
}
```

这里的 `content` 不再直接指向某一个 Chunk，而是指向 `ChunkList`。客户端拿到 `ChunkList` 后，就可以按顺序下载其中列出的每一个 Chunk，并拼接还原出原始文件。

### SameAs

现实世界里仍然存在很多“整块 Hash”，比如某个 Linux 发行版镜像通常会直接公布其 `sha256`。这种情况下，用户仍然希望通过下面这种 URL 访问：

```text
file_reader = open_reader_by_url("http://$zone_id/sha256:213788234cfb679121c148ba4fd768390bf948bfb17d6cfced79b205d5c82c9d")
```

在 CYFS Server 的内部实现里，可以通过 `SameAs` 关系查询，把这个 `3.1G` 的大 `sha256 ChunkId` 映射到一个 `ChunkList`，最后完成下载。

这里的 `SameAs` 指的是服务端内部的一种“内容等价”关系：一个大 `ChunkId` 对应的内容，等价于某个 `ChunkList` 依次拼接后的结果。

### 使用 `container_id/@/key` 可信地获取对象

注：本章内容目前还是实验性设计，尚未定稿。

这里的 `key` 也可以理解成一层 `inner_path`。

对于小容器，`key` 访问的可验证性与标准的 `NamedObject + inner_path` 一致：都是先拿到完整的父对象，再判断 `key` 指向的 child `ObjectId`。

当容器里含有大量元素（超过 `4096` 个）时，我们称其为大容器。大容器的困难在于：无法把完整容器 JSON 放进 `cyfs-parents`。此时可以切换到`部分可验证获取模式`。它的核心设计是：在信任 `container ObjectId` 的前提下，通过类似 Merkle Tree 的理论，相信：

```text
container[key] = target_obj_id
```

同时由服务端返回一份路径证明。

因此在请求：

```text
http://$zoneid/$container_id/@/key/@/content
```

时，可以返回：

```text
cyfs-parents: [$parent_obj]
cyfs-inner-proof: [$proof-data]   <= 证明 $container_id[key] = ObjId($parent_obj)
cyfs-obj-id: $content_obj_id
<body: content obj data>
```

如果底层基于 Merkle Tree 来实现 `cyfs-inner-proof`，那么 proof 大概会长成“叶子定位信息 + 每一层的兄弟节点 Hash”这样的结构。例如：

```json
{
  "key": "key",
  "leaf_index": 12000,
  "leaf_value": "cyfile:abcdef1234567890",
  "siblings": [
    "h1....",
    "h2....",
    "h3...."
  ]
}
```

其中：

- `leaf_index` 表示这个 key 在 Merkle Tree 叶子层中的位置。对于 map 类型容器，一般意味着服务端和客户端都约定了同一种“key 排序后再编号”的方法。
- `leaf_value` 就是这个 key 指向的对象 id，也就是这里的 `ObjId($parent_obj)`。
- `siblings` 是从叶子到根路径上，每一层需要用到的兄弟节点 Hash。

客户端验证时，可以先用 `key + leaf_value` 计算叶子 Hash，再结合 `siblings` 一层层向上计算，最后得到根 Hash，并确认它等于 `$container_id` 对应容器的根 Hash。这样就能在不下载完整大容器的情况下，相信 `container[key] = ObjId($parent_obj)`。

客户端验证流程如下：

1. 先验证 `cyfs-parents[0]`，也就是 `parent_obj`。如果它是一个 `NamedObject`，就按前文的稳定编码规则重新计算它的 `ObjectId`。
2. 用 `cyfs-inner-proof` 验证大容器关系：确认 `$container_id[key] = ObjId($parent_obj)`。
3. 在 `parent_obj` 上继续执行下一层 `inner_path`，也就是这里的 `/content`，得到目标 `content_obj_id`。
4. 检查这个结果是否等于 Header 里的 `cyfs-obj-id`。
5. 最后验证 HTTP Body：
   - 如果 Body 返回的是 `NamedObject JSON`，就重新计算它的 `ObjectId`，并与 `cyfs-obj-id` 比较。
   - 如果 Body 返回的是 Chunk 数据，就直接计算 `ChunkId`，并与 `cyfs-obj-id` 比较。

这样客户端虽然没有下载完整的大容器，但仍然完成了下面这条链路的验证：

```text
container_id -> key -> parent_obj -> /content -> cyfs-obj-id -> body
```

## CYFS 里的上传

CYFS 本身没有“上传公共数据”的统一协议设计，因为 CYFS 的定位是在互联网上高效可靠地获取公共数据，实现 `Content Network`。

**No Push! `cyfs://` 是 `pull-first` 的协议。**

但在很多产品逻辑里，仍然会出现“传统的上传行为”。比如用户在手机上选择一张照片，用 MessageHub 给朋友发送消息，在消息真正发出前，就会先发生一个“照片上传到 OOD”的过程。这个过程不结束，消息就无法发送。本章节基于这类场景讨论 CYFS 里的上传。

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

### Zone 间传播

Zone 间**没有上传的概念**。如果 `ZoneA` 给 `ZoneB` 发送一个带附件的 `MessageObject`，并不会有所谓“附件上传”的逻辑。

其核心流程如下：

```text
ZoneA Call ZoneB.sendmsg(MessageObject)
ZoneB.onmsg(MessageObject):
    业务逻辑判断
    决定下载附件
ZoneB.open_reader_by_url(MessageObject.ref_obj[0])
```

## 身份认证

严格的身份认证走的是 Zone 内的 NDM 相关协议，这里不展开讨论。落到 `cyfs://` 协议上，通常已经是跨 Zone 访问，此时至少偏向“半公开”场景，因为数据一旦到了公网，就没有 100% 可靠的方法阻止其继续传播。这里描述的是 `cyfs://` 里的“君子协定”。

```headers
cyfs-original-user: $user-did
Reference:
```

### 基于 ReferencePath 的权限控制

这里的核心思路是：请求方不仅表明“我是谁”，还表明“我是因为什么上游动作或页面跳转来到这里的”。对于一些半公开内容，服务端并不追求绝对防扩散，而是希望把访问能力绑定在某条业务链路上。

因此，权限控制可以同时参考：

- `cyfs-original-user`：谁发起了请求。
- `Reference` / `ReferencePath`：请求是从哪条内容传播链路进入的。
- `cyfs-cascades`：请求背后的动作链，例如“浏览页面 -> 点击购买 -> 请求附件”。

这种机制更接近“带上下文的访问约束”，而不是传统意义上的强访问控制。

### 验证购买收据

CYFS 构建 `Content Network` 的一个关键要素，是形成 Content 的交易。其典型流程是：

1. 用户访问链接，链接告知 Content 的商品信息和购买方法（兼容 HTTP 402）。
2. 用户根据购买方法的指引完成购买，得到收据。我们自带的去中心购买方法是基于 USDB 的内容购买合约。
3. 用户再次访问链接，并携带自己的收据。
4. CYFS 网关对用户身份和收据进行验证，然后返回内容数据。

## Content 的传播证明

### 安装证明

### 下载证明

### 收录证明

### 分享证明

## CYFS 网络的传输加速

### 多源发现（Tracker）

- 原始 URL。
- `Reference`，也就是“谁传播了这个内容”。
- 本地 Cache：由基础环境搭建者提供，可在一个范围内实现加速，例如在台式机上看过的内容，稍后又在笔记本上看。
- 收录者信息。

收录者通常也是一个 Tracker，可以查询到更多源，类似传统 BitTorrent。  
同时也可以通过基于下载证明的激励，建立更健康的 `P2P` 体系。最近获取过某个内容的 Zone，也会在一段时间内继续为该内容提供加速支持。

### 主动加速

在 Pull 流程中，可以根据 File 的多源信息，同时从不同源获取不同 Chunk。  
从可验证性角度看，一个 Chunk 的一次完整读取只能对应一个已知 `ChunkId`，但不同 Chunk 可以来自不同源。

对于速度过慢的 Chunk，可以切换源下载，既可以断点续传，也可以重头开始。  
Pull 调度器可以根据历史记录和到源的距离，决定 `chunk x` 从 `source y` 下载。

### 透明加速

核心思路是：把 Pull 请求透明地拦截到 Cache Node，实现方式与现有透明加速网关类似。但因为要实现：

```text
将 Client 发往源 X 的请求，重定向到更快的源 Y
```

所以必须能识别并重定向 Pull 目标。这通常依赖明文的 CYFS 流量，或者依赖 Zone 内可共享可信私钥时对 HTTPS 级流量进行拦截。

## 附录：协议参考

### 含有 ObjId 和 inner_path 的 URL

CYFS URL 可以分成两层：

1. 根定位部分：用于确定“从哪个对象开始解析”。
2. `inner_path` 链：用于在对象内部继续寻址。

根定位部分有两种常见形式：

- 直接对象链接（O Link）

```text
http://$zone_id/$obj_id
http://$zone_id/ndn/$chunk_id
```

- 语义链接（R Link）

```text
http://$zone_id/readme.md
http://$zone_id/all_images
```

在此基础上，可以继续附加 `inner_path` 链。其 URL 形式统一写作：

```text
http://$zone_id/<root_locator>(/@/<path_step>)*
```

在实际文本里，分隔符写成 `"/@/"`，因此常见例子如下：

```text
http://$zone_id/cyfile:abcd/@/content
http://$zone_id/all_images/@/readme/@/content
http://$zone_id/$container_id/@/key/@/content
```

解析规则如下：

1. `"/@/"` 左侧部分是根定位部分，可以是 `ObjectId`，也可以是语义路径。
2. 每个 `"/@/<step>"` 表示在“当前对象”上执行一次 `inner_path="/<step>"`。
3. 如果某一步的结果是 `ObjectId`，默认继续解引用，并把它当作下一步的当前对象。
4. 如果某一步的结果不是 `ObjectId`，那么它必须是最后一步；此时该字段值就是响应结果。
5. 对 Chunk URL 不能继续附加 `inner_path`，因为 Chunk 本身不是结构化对象。
6. `?resp=raw` 用于请求“当前定位到的对象原始 JSON”，不自动附加辅助验证 Header，也不要求服务器继续做额外展开。

典型例子：

- `http://$zone_id/cyfile:abcd/@/content`

先定位到 `cyfile:abcd`，再执行 `/content`，若结果是 `ChunkId`，则默认返回对应 Chunk 数据。

- `http://$zone_id/all_images/@/readme/@/content`

先由语义路径 `all_images` 得到一个 `DirObject`；  
在该对象上执行 `/readme`，得到 `FileObject`；  
再在该对象上执行 `/content`，得到最终 `ChunkId` 并返回对应内容。

### 辅助验证的 HTTP Header 扩展

**CYFS RespHeader扩展**

- `cyfs-obj-id`: `ObjectId`。如果返回内容是一个 `NamedObject`，或者是一个 Chunk（或其 Range），则填写。如果返回的是 `NamedObject` 的某个非 `ObjectId` 字段值，则不填写。
- `cyfs-path-obj`: `JWT`。只有请求中包含语义路径时才会使用，用于证明“语义路径 -> 根对象”的绑定关系。
- `cyfs-parents`: `Array<json|ObjectId>`。用于给出 `inner_path` 解析过程中需要的父对象；小对象场景里通常直接塞完整对象，大对象场景里也可以只给出必要的 `ObjectId`。
- `cyfs-inner-proof`: `Array<json>`。用于证明 `$child_objid = resolve($parent_obj, inner_path)`；典型场景是大容器或 Merkle Tree 路径证明。
- `cyfs-chunk-size`: `u64`。当返回的是 Chunk 或 Chunk Range 时，表示该 Chunk 的完整大小，不受 HTTP Range 影响。

**CYFS ReqHeader扩展**

- `cyfs-original-user`: `DID`。说明请求是由哪个用户 DID 发起的。
- `cyfs-cascades`: `json`，`ActionObject Array`。说明该请求是因为什么上游动作链被构造出来的，通常隐晦地表达了逻辑权限，最大长度为 `6`。
- `cyfs-proofs`: `json`，`JWT Array`。用于携带各种行为证明，最常见的是购买证明（收据）；某些 P2P 流程还可能要求用户提供“下载证明”后才开始下载。
- `cyfs-access-code`: `String` 或 `JWT`。纯粹的访问代码，一般自带过期时间。

### `get_object_by_url` 流程

```text
get_object_by_url(any://$clientid/$listenerid/$objid)
```

最小验证流程：

1. 发起请求并获得 Body。
2. 如果 URL 已经包含 `ObjectId`，则直接对 Body 重新计算 `ObjectId` 并校验。
3. 如果 URL 是语义链接，则先验证 `cyfs-path-obj`，再根据其中的 `target` 对 Body 做校验。
4. 如果响应还包含 `cyfs-parents` 或 `cyfs-inner-proof`，则继续验证 `inner_path` 链路。

### `open_reader_by_url` 流程

`open_reader_by_url` 处理的是 `NamedData` 读取语义。与 `get_object_by_url` 相比，它更关注：

- 最终返回值是否是 Chunk 或 ChunkList。
- 是否支持 HTTP Range / 断点续传。
- 是否需要根据 `cyfs-chunk-size` 还原完整内容边界。
- 是否需要基于 `FileObject -> content -> Chunk/ChunkList` 的链路做校验。

因此它的核心流程通常是：

1. 解析 URL，必要时解析语义路径和 `inner_path`。
2. 根据 Header 验证最终 `cyfs-obj-id`。
3. 如果 `cyfs-obj-id` 是 ChunkId，则直接打开 Reader。
4. 如果 `cyfs-obj-id` 指向的是 `FileObject` 或 `ChunkList`，则继续按对象语义展开到最终 Reader。

### 标准对象参考

参考 [CYFS协议现有标准对象实例.md](./CYFS协议现有标准对象实例.md)。
