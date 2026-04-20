# CYFS Protocol

> `CYFS` 或 `cyfs://` 都是在说协议；单独说 `cyfs` 时，通常指基于 CYFS Protocol 定义的标准对象实现的 DFS。

当前文档状态：0.9，最新草案，领先现有代码实现。

## Named Data Network (NDN)的基本概念

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

其格式为 `{hash_type}:{hash_data_hex_text}`。我们称作`标准ObjId`

上述 `ChunkId` 还有另一种 base32 编码方式：

```text
base32_rfc4648_encode_lowercase_no_padding("sha256:" + hash_data)
```

base32 编码规范（MUST）：

- 采用 RFC 4648 base32 字母表。
- **不使用 padding**（去掉尾部的 `=`）。
- **统一使用小写字母**；解析时应先转换为小写再解码，以兼容 URL 子域名大小写不敏感的场景。

在系统中，使用上述两种 `ChunkId` 来表达同一个`不再修改的数据块`是等价的。

> 原则上,base32编码的objid只用在URL中，其他地方都应该使用标准的objid string

### 通过 ChunkId 可靠地获取数据

在支持 `cyfs://` 的 Zone 中，我们可以使用 3 个常见 URL 来获取上述 `README.md`：

```text
http://$zoneid/ndn/{sha256_chunkid}
http://$zoneid/ndn/{base32_chunkid}
http://{base32_objid}.$zoneid/
```

第三种形式在协议上支持，但一般不会默认开启，多用于一些特别大的 DirObject：把 `base32_objid` 放进 hostname，可以让浏览器把该对象视为一个独立 origin，便于做浏览器级的隔离与缓存（同时 URL 里携带的对象 id 天然跨路径复用）。由于 hostname 大小写不敏感，`base32_objid` 的编码**必须**使用上一节约定的小写无 padding 形式，否则不同大小写的 hostname 将无法归一化回同一个 `ObjectId`。

在标准浏览器里，使用 `http` 会触发“不安全警告”。但只要浏览器完整支持了 `cyfs://`，即使底层走 `http`，虽然传输层没有加密，内容完整性仍然有 Hash 保障：浏览器可以基于 URL 中的 Hash 信息对获得的数据进行校验，校验通过后，该内容就是不可篡改的。

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
  "iat": 232332,
  "exp": 132323
}
```

这个对象只说明一件事：某个路径（不含域名）当前指向哪个 `NamedObject` 的 `ObjectId`，以及这条绑定关系的签发时间 `iat` 与过期时间 `exp`（字段命名与标准 JWT 生态一致）。

为避免实现分歧，`PathObject JWT` 的 Header 需要补充下面几个约束：

- `alg` **MUST** 明确填写签名算法。当前实现基线 **MUST 支持 `EdDSA`（Ed25519）**；为了兼容现有 WebPKI / P-256 生态，**MAY 支持 `ES256`**。
- `alg = none` **MUST NOT** 被接受。
- `kid` **SHOULD** 填写，用于指明“应使用哪把已发布公钥来验证该 JWT”。

当可信公钥来自 DID Document 时，`kid` 最好直接使用对应 `verificationMethod.id`（完整 DID URL，或至少是可在该 DID Document 内唯一解析的 fragment）；客户端解析到 `kid` 后，应当在该 DID Document 中找到对应公钥，并确认这把 key 被允许用于此类声明的签名验证。

当可信公钥来自 DNS Record 或 BNS 时，`kid` 的语义可以退化为“该发布体系下的 key 名称 / 版本号 / 记录名”；只要客户端能在当前 `zoneid` 对应的可信 key 集里唯一定位到同一把公钥即可。

在使用 `target ObjectId` 之前，需要先验证 `PathObject`（JWT）：

0. 获得可信公钥，通常它与 URL 的 hostname 相关。
1. 根据 JWT Header 中的 `alg` 与 `kid` 选择正确的公钥，并使用该公钥验证 JWT，确定该路径确实指向该 `ObjectId`。
2. 检查 `iat` / `exp` 是否处于可接受时间窗口内，防止过期绑定被当作当前绑定使用。
3. 与本地缓存的 `PathObject`（如有）比较 `iat` 等版本信息，避免旧版本绑定覆盖新版本绑定。

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

#### 稳定 JSON 编码规范（Canonical JSON）

**CYFS Canonical JSON 完全兼容 [RFC 8785 (JSON Canonicalization Scheme, JCS)](https://datatracker.ietf.org/doc/html/rfc8785)。** 任何符合 RFC 8785 的实现都可以被 CYFS 协议直接采用；不同语言的实现只要都通过了 RFC 8785 的测试向量，就一定能算出相同的 `ObjectId`。

RFC 8785 已经覆盖了下面这些关键规则（这里仅作快速提示，权威定义以 RFC 为准）：

- **对象字段**：按 key 的 UTF-16 code unit 顺序排序；**禁止重复 key**。
- **字符串**：按 RFC 8259 的 JSON 字符串转义规则序列化，内部文本应为 **NFC 归一化**的 Unicode。
- **数字**：按 ECMAScript `Number.prototype.toString` 描述的规则输出（整数无小数点、浮点数使用最短可回读表示、不输出多余前导/尾部零、不输出 `+` 号等）。
- **结构**：紧凑输出，不插入任何空白字符。
- **禁止值**：`NaN`、`Infinity`、`-Infinity`、`undefined` 不允许出现在 canonical JSON 里；出现即视为非法对象。
- **null vs 缺省**：`null` 与“字段缺失”是两个不同的 JSON 值，会产生不同的 ObjectId。因此：**对可选字段，缺省时必须省略该字段，而不是显式写 `null`**。

Hash 计算流程（`build_named_object_by_json`）：

1. 按 RFC 8785 规则得到紧凑 canonical JSON 字符串 `S`（UTF-8 字节序列）。
2. 计算 `obj_hash = sha256(S)`。
3. `ObjectId = "{obj_type}:" + hex(obj_hash)`（或其等价 base32 形式，见前文）。

对于少量“特殊对象”（如 `clist`、`cymap-mtp` 等），其 `ObjectId` 的计算在上述基础上还包含额外的长度/根哈希绑定，具体见标准对象文档的相应章节。

基于上述流程，可以得到下面结论：

- 通过 `Named Object Id` 可以获得一个可验证的 JSON。
- 使用 RFC 8785 稳定编码后，相同语义的 JSON 每次都会编码得到相同的 `Named Object Id`。

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

- `content` 指向文件内容对应的 `ChunkId` 或 `ChunkListId`。本例使用 `mix256` 哈希类型，其定义见下文 **“mix 类 Hash”** 一节。
- `name`、`size`、`create_time`、`last_update_time` 是 `FileObject` 的基础字段。
- `mime` 不是框架内置字段，但可以像这样直接平铺在 JSON 根上作为自定义元信息。

#### mix 类 Hash（`mix256` 等）

CYFS 在 `sha256` / `sha512` / `blake2s256` / `keccak256` 等标准哈希的基础上定义了一族 **“mix” 哈希类型**（`mix256`、`mix512`、`mixblake2s256`、`mixkeccak256` 等）。`mix` 哈希的动机是：在 `ChunkId` 里同时编码“内容哈希”和“数据长度”，让调度器无需打开 Chunk 本体就能知道它的大小。

`mix*` ChunkId 的字节结构为：

```text
obj_hash_bytes = varint(u64(data_length))  ||  raw_hash_bytes(base_algorithm)
```

其中：

- `varint` 为 **无符号 LEB128** 编码（与 Protocol Buffers 的 `varint` 等价）。
- `base_algorithm` 就是 `mix` 前缀对应的基础哈希：`mix256` → `sha256`，`mix512` → `sha512`，`mixblake2s256` → `blake2s256`，`mixkeccak256` → `keccak256`。
- `raw_hash_bytes` 是对 Chunk 原始字节调用基础哈希得到的完整摘要（例如 `mix256` 为 32 字节）。

对应的文本形式仍为 `{obj_type}:{hex(obj_hash_bytes)}`，但解析时需要先从 hex 字节中剥离 varint 前缀，再参与哈希比对。

这里要注意：`mix*` 前缀里的 `data_length`，语义永远是**这一个 Chunk 自身的字节长度**。后文 `clist` 也会在 `ObjectId` 前缀里编码一个长度字段，但那里的长度语义不同，表示的是**整个 `ChunkList` 还原后内容的总字节长度**，而不是某个成员 Chunk 的长度。


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
cyfs-parents-0: json:$base64url_dir_obj
cyfs-parents-1: json:$base64url_file_obj
cyfs-obj-id: $objid
cyfs-chunk-size: $full_chunk_size
</header>
<body />
```

这里的例子里包含两层 `inner_path`，因此响应里也包含 2 个按顺序编号的 `cyfs-parents-N` Header：`cyfs-parents-0` 对应 `DirObject`，`cyfs-parents-1` 对应 `FileObject`。

`cyfs-parents-N` 的编码规则如下：

- `N` 从 `0` 开始递增，表示 `inner_path` 链上的父对象顺序；编号 **MUST** 连续，不能跳号。
- 每个 Header 只承载一个 parent item，避免把整个数组塞进单个 Header 带来的长度限制、多值解析和联合类型歧义。
- Header value 采用带前缀的字符串形式：
  - `oid:$objid`：表示这一项只是一个 `ObjectId`。
  - `json:$base64url_canonical_json`：表示这一项是完整 `NamedObject JSON` 的 UTF-8 canonical JSON，再做 base64url 编码后的结果。

之所以不用单个 `cyfs-parents: [ ... ]`，是因为父对象链很容易触发通用 HTTP Header 长度限制（很多实现默认只有几 KB），而 `json:$base64url...` 这种单项编码也更容易区分“这是完整对象”还是“只是对象 id”。

客户端基于这些 Header 的验证流程可以简化为：

1. 从原始 URL 中拆出语义路径部分和两层 `inner_path`。这里 `cyfs-path-obj` 负责把语义路径绑定到 `DirObject`。
2. 获取 `$zone_id` 对应的可信公钥，验证 `cyfs-path-obj` 的签名，确认它的 `target` 确实是 `DirObject` 的 `ObjectId`。
3. 解码 `cyfs-parents-0`，对其中的 `DirObject` 做标准 `NamedObject` 校验，确认它的 JSON 与 `cyfs-path-obj.target` 匹配。
4. 在 `DirObject` 上执行第一层 `inner_path`，例如 `/readme`，确认其结果确实指向 `cyfs-parents-1` 对应的 `FileObject`。
5. 解码 `cyfs-parents-1`，对其中的 `FileObject` 做标准 `NamedObject` 校验，确认该对象本身是可验证的。
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

1. `inner_path` 用于在 `NamedObject` 内部做字段寻址，其语义与 JSON Path 的“按字段逐层取值”一致。**单个 `"/@/<step>"` 段内部，`<step>` 可以是多级字段路径**（由 `/` 分隔），会被整体视为一次 JSON Path 操作，结果允许是任意 JSON 值（对象、数组、标量）。
2. URL 层使用字面量 `"/@/"` 作为分隔符；每出现一次，就表示“结束当前 JSON Path 步骤，并在其结果之上开启下一层 `inner_path`”。
3. `http://$zone_id/all_images/@/readme/@/content` 应理解为两步：
   - 先在 `all_images` 对应的对象上执行 `/readme`。
   - 再在上一步得到的对象上执行 `/content`。
4. `"/@/"` 段之间的“跨段”行为**只在 ObjectId 边界处断裂**：
   - 如果一段内部解析得到的中间值是 `ObjectId`，**在同一段内也会**继续解引用并在其 JSON 上继续执行该段剩余字段路径；
   - 只有**整段解析完毕**得到的结果是 `ObjectId` 时，才会自动跨到下一个 `"/@/"` 段继续。
5. 如果某一段最终结果是 `ObjectId`，默认继续解引用并返回它指向的对象或 Chunk。
6. 如果某一段最终结果**不是** `ObjectId`（可以是 JSON 对象、数组或标量），则该值就是最终返回值；此时**不会再自动跨段**。
7. 如果一段的最终结果不是 `ObjectId`，但 URL 里后面还有新的 `"/@/"` 段，则该请求非法。客户端若确实希望继续深入这个 JSON 值内部，应当把更深的字段路径直接写在当前段内（参见规则 1），而不是再加一个 `"/@/"`。

下面用一个对比例子说明规则 4：

假设：

- `/a` 指向一个 `ObjectId = X`
- `X` 解引用后得到一个 `DirObject`
- 在 `X` 的 JSON 上继续取 `/b`，得到另一个 `ObjectId = Y`

那么下面两种写法都是合法的：

```text
/root/@/a/b
/root/@/a/@/b
```

它们的求值过程分别是：

- `/root/@/a/b`
  - 在同一段内先取 `/a`，得到 `ObjectId X`
  - 因为 `a/b` 还没有走完，所以**在同一段内自动解引用** `X`
  - 再在 `X` 的 JSON 上继续取 `/b`，得到 `ObjectId Y`
  - 因为这一整段的最终结果是 `Y`，所以按默认规则继续解引用并返回 `Y` 指向的内容

- `/root/@/a/@/b`
  - 第一段只执行 `/a`，得到 `ObjectId X`
  - 因为第一段已经结束，且结果是 `X`，所以**在段边界处**自动把 `X` 作为下一段的当前对象
  - 第二段再在 `X` 上执行 `/b`，得到 `ObjectId Y`
  - 因为第二段的最终结果是 `Y`，所以继续解引用并返回 `Y` 指向的内容

在这个例子里，两种写法的最终返回内容通常相同。它们真正的区别主要体现在**验证链的表达方式**上：

- 写成 `/root/@/a/b` 时，客户端逻辑上只看到“一个段”，服务端未必需要把 `X` 单独列为一个明确的中间 parent。
- 写成 `/root/@/a/@/b` 时，`X` 是显式的段边界对象，服务端更容易把它单独体现在 `cyfs-parents-N` 链里，验证路径也更清晰。

因此，二者不是语义上互相矛盾的两种规则，而是“更紧凑的写法”和“更显式的对象边界写法”。  
**推荐做法是：当你已经知道某一步会跨过一个 `ObjectId` 边界，并且希望验证链更清楚时，优先写成 `"/@/"` 分段形式。**

> 一句话概括：**`/` 在段内做 JSON Path，`/@/` 只在 ObjectId 边界使用。**

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

`resp=raw` 的规范：

- **服务端 MUST 支持 `resp=raw`**。不允许忽略该参数去返回“展开/验证”形式的响应。
- **对 Chunk URL** 加 `resp=raw` 仍然合法：服务端按正常 Chunk 语义返回字节流（支持 HTTP Range），只是**不附加任何 CYFS 验证 Header**（不带 `cyfs-obj-id` 等）。客户端可自行根据 URL 里的 `ChunkId` 做校验。
- **与 `inner_path` 组合**时：`resp=raw` 只改变“是否展开/是否加辅助 Header”，不改变寻址结果本身。最后一段 `inner_path` 的最终值是什么，就原样返回什么——指向 `ObjectId` 则返回该 ObjectId（JSON 字符串形式，或 hostname 里的 base32），指向普通 value 则返回该 value。服务端**不会**因为结果是 `ObjectId` 就自动继续解引用。

⚠️ 安全提示：`resp=raw` **不附带任何可信 Header**，因此它只适合下面两类场景：

- URL 根定位本身就是直接对象链接（O Link），客户端可以直接用 URL 里的 `ObjectId/ChunkId` 校验响应体。
- 客户端已经通过其它请求独立验证了根对象，此次只是批量读取其 child 对象的 raw JSON 或 raw value。

如果对语义 URL 直接使用 `resp=raw`，那么它在可信性上就退化成了传统 HTTP GET：客户端可以得到数据，但**不能**依赖这次响应本身证明“这个语义路径当前确实绑定到了这个对象”。

### ChunkList

从 `NamedObject` 的表达能力上看，`ChunkList` 里的每个 `ChunkId` 原则上都可以对应任意大小的 Chunk；也就是说，**`ChunkList` 作为一种对象结构，本身并不限制其成员 Chunk 的大小**。

但对 `cyfs://` 来说，仅仅“允许表达”还不够。为了保证不同实现针对同一个文件都能构造出**完全一致**的 `ChunkList`，协议还必须定义一套统一的标准切分算法。

因此，`cyfs://` 约定：**构造标准 `ChunkList` 时，Chunk 大小上限固定为 `32MiB`（`33554432` 字节）**。这里的“固定上限”指的是：

- 按文件字节流顺序切分。
- 每个非最后一个 Chunk 的大小都**必须**是 `32MiB`。
- 最后一个 Chunk 允许小于 `32MiB`。

只有采用这套固定规则，所有人针对同一个文件切分时，才会得到完全一致的 `ChunkList`，从而保证可互操作的去中心化多源加速。

关于 `32MiB` 这个数字：

- 它来源于底层 `named-store` 块存储引擎的最佳工作区间（在内存开销、随机寻址成本、网络往返开销和多源调度粒度之间取得平衡）。
- 这里的强制性只作用于**标准 `ChunkList` 的构造规则**，并不作用于所有独立存在的 Chunk：`ChunkId` 既可以指向小 Chunk，也可以指向更大的 Chunk（作为 `SameAs` 等价源存在）。
- 因此应区分两件事：`ChunkList` 这个对象格式允许记录任意大小的 Chunk；而“标准 `ChunkList` 如何从一个文件生成出来”则必须遵守 `32MiB` 规则。


它的逻辑很简单：

1. 先把大文件按 `32MiB` 固定上限顺序分块。
2. 每一块分别计算 mix256类型的 `ChunkId`。
3. 再把这些 `ChunkId` 按顺序组成一个列表对象。

一个 `ChunkList` 的 JSON 形态就是一个字符串数组，例如：

```json
[
  "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
  "mix256:91c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d6"
]
```

`ChunkList` 自己也有一个 `ObjectId`。需要注意的是，`ChunkList` 的 `ObjectId` 不是普通 `NamedObject` 的“稳定 JSON 直接 Hash”，而是由下面两部分共同决定：

- `ChunkId` 列表的顺序内容。
- 整ChunkList的总大小 `total_size`。

**ChunkList ObjectId 的精确算法**（`obj_type = "clist"`）：

```text
S           = RFC 8785 canonical JSON of the ChunkId array  // 如上面 JSON 示例
H           = sha256(S)                                      // 32 bytes
obj_hash    = varint(u64(total_size)) || H                   // varint = LEB128 unsigned
ObjectId    = "clist:" || hex(obj_hash)
```

其中 `total_size` 是该列表拼接还原后**原始文件的字节总长度**（不是 ChunkList JSON 的字节数，也不是各 Chunk 的长度之和的可见部分——如果最后一个 Chunk 是截短的，`total_size` 就是该截短后的真实长度）。

这里容易和前文 `mix*` 的长度前缀混淆，二者虽然都使用 `varint(u64(length))`，但语义并不相同：

- 对 `mix*` 来说，前缀里的 `length` 是**单个 Chunk 本身的长度**。
- 对 `clist` 来说，前缀里的 `total_size` 是**整个 `ChunkList` 依次拼接后还原出的完整内容总长度**。

也就是说，`clist` 前缀编码的是“这份逻辑大文件有多大”，而不是“列表里某一个 Chunk 有多大”。

这种“长度前缀 + 内容哈希”的编码形式与前文 `mix*` 哈希的思路一致：客户端拿到 `clist:...` 之后，仅凭 ObjectId 字符串就可以知道待下载文件的总大小，无需先抓取 ChunkList JSON。

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

这里需要特别说明：`SameAs` 本身首先是**服务端内部的存储与调度语义**，并不是说客户端必须无条件相信“某个 `ChunkList` 等价于某个大 `ChunkId`”。真正的可信性，仍然来自**对原始请求目标 `ChunkIdA` 的最终校验**。

也就是说，当客户端请求：

```text
open_reader_by_url("http://$zone_id/$chunk_id_a")
```

服务端完全可以在内部执行下面的流程：

1. 查询本地 `SameAs($chunk_id_a -> $chunk_list_b)`。
2. 打开 `ChunkListB`，按顺序读取其中列出的 sub-chunks。
3. 把这些 sub-chunks 依次拼接成一个连续字节流返回给客户端。

从客户端视角看，自己请求的仍然是 `$chunk_id_a`，因此验证规则也保持不变：

1. 以 URL 中的 `$chunk_id_a` 作为最终目标 `ChunkId`。
2. 按收到的字节流顺序做增量 Hash。
3. 同时累计总字节数，并与 `$chunk_id_a`（若是 `mix*`）或响应中的 `cyfs-chunk-size` 做一致性检查。
4. 整个流结束后，比对最终算出的 `ChunkId` 是否等于 `$chunk_id_a`。

只要最后一步成立，就说明：

```text
concat(ChunkListB) == ChunkA
```

于是 `SameAs($chunk_id_a -> $chunk_list_b)` 这条关系就在内容层面被验证了。

因此，`SameAs` 的关键点在于：

- **下载前**：`SameAs` 只是一个调度提示，告诉系统“可以用 `ChunkListB` 来尝试满足对 `ChunkA` 的读取”。
- **下载后**：只有当拼接后的完整结果重新计算得到的 `ChunkId` 确实等于 `ChunkA` 时，这条 `SameAs` 才真正被验证成立。

这也解释了为什么 `SameAs` 很适合做“大 Chunk 的兼容访问”，却不改变协议的可信根：可信根始终是**用户最初请求的那个 `ObjectId/ChunkId`**，而不是服务端临时选择的内部展开路径。

对于 `HTTP Range` 场景，客户端在没有拿到完整内容之前，通常只能先信任“这个 Range 来自一个可能正确的 `SameAs` 展开”；只有在后续把整份内容补齐并完成一次完整 Hash 后，才能把这条 `SameAs` 关系升级为本地可信缓存。也就是说，`Range` 校验解决的是“这一段字节没坏”，而 `SameAs` 的最终确认仍然依赖一次完整内容校验。


### 使用 `container_id/@/key` 可信地获取对象

注：本章内容目前还是实验性设计，尚未定稿。

这里的 `key` 也可以理解成一层 `inner_path`。

对于小容器，`key` 访问的可验证性与标准的 `NamedObject + inner_path` 一致：都是先拿到完整的父对象，再判断 `key` 指向的 child `ObjectId`。

当容器里含有大量元素（超过 `4096` 个）时，我们称其为大容器。大容器的困难在于：无法把完整容器 JSON 继续塞进一组 `cyfs-parents-N` Header。此时可以切换到`部分可验证获取模式`。它的核心设计是：在信任 `container ObjectId` 的前提下，通过类似 Merkle Tree 的理论，相信：

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
cyfs-parents-0: json:$base64url_parent_obj
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

1. 先解码并验证 `cyfs-parents-0`，也就是 `parent_obj`。如果它是一个 `NamedObject`，就按前文的稳定编码规则重新计算它的 `ObjectId`。
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


## CYFS 网络的传输加速

当FileObject都基于ChunkList构造后，我们就得到了如下的好处
- 下载一个文件时，可以基于Chunk同时开始下载，如果有一些Chunk已经在另一个文件中存在，那么无需下载
- 所有的Chunk都是可验证的，因此可以从任何来源下载。

在此基础上，我们能实现简单可靠的传输加速。在 Pull 流程中，可以根据 File 的多源信息，同时从不同源获取不同 Chunk。  
从可验证性角度看，一个 Chunk 的一次完整读取只能对应一个已知 `ChunkId`，但不同 Chunk 可以来自不同源。

对于速度过慢的 Chunk，可以切换源下载，既可以断点续传，也可以重头开始。  
Pull 调度器可以根据历史记录和到源的距离，决定 `chunk x` 从 `source y` 下载。

### 多源发现

消费者(Consumer)如何发现多个源呢?
核心原则是：**源发现**和**内容校验**是两件彼此解耦的事。我们可以用很多不完全可信的办法去“猜测谁可能有这个内容”，但一旦真正开始下载，仍然只依赖 `ChunkId/ObjectId` 做最终验证。

因此，多源发现本质上是在收集“谁可能持有这个内容”的线索。常见线索包括：

- 原始源 URL。
- `Reference`，也就是“谁传播了这个内容”。
- 收录者。
- 本地 Cache：由基础环境搭建者提供，可在一个范围内实现加速，例如在台式机上看过的内容，稍后又在笔记本上看。

原始源 URL 是最直接的来源：当一个 `FileObject`、`ChunkId` 或语义 URL 被发布时，最初发布它的 Zone 通常就是第一个可用源。

`Reference` 代表传播路径。比如某个内容是 Alice 通过 feed、消息或页面跳转带给 Bob 的，那么 Alice 至少在最近一段时间里“很可能”持有这个内容，或者知道哪里能拿到这个内容。因此，传播者不一定是最终数据源，但通常是发现更多源的重要入口。

收录者同样很关键。一个收录者既然愿意把内容纳入自己的目录、榜单或集合，它通常就会在自己的基础设施上保留该内容，或者至少维护“谁有这个内容”的索引。对用户来说，收录者一方面提供内容发现，另一方面也天然扮演了 Tracker 的角色。

本地 Cache 则是最容易被忽略、但实际非常重要的一类源。很多情况下，用户并不是第一次接触某个内容，只是换了一台设备、换了一条传播路径或再次打开了同一个对象。只要 Zone 内基础设施之间能够共享“本地最近拿到过哪些 Chunk”的事实，就能显著降低重复下载成本。

在协议实现上，一个 Pull 调度器通常会把这些线索统一整理成一个候选源列表：

```text
candidate_sources = [
    original_source,
    referrers,
    curators,
    tracker_results,
    local_cache_nodes
]
```

然后再结合最近的下载证明、时延、失败率、带宽估计和距离，决定每个 Chunk 具体从哪个源拉取。

这里要强调：`谁告诉我“某个源可能有数据”` 与 `这个源返回的数据是否可信` 完全是两套逻辑。前者可以很宽松，后者必须严格。正是这种解耦，才让 CYFS 能在开放网络中同时获得“多源加速”和“内容可信”。


收录者通常也是一个 Tracker，可以查询到更多源，类似传统 BitTorrent。

> Tracker协议还未做详细的设计，但其基本返回结构是一个 数组，说明"谁拥有哪些Chunk"，可以从下载证明中反推出来

### 有激励的P2P网络
收录者通常会设计一定的激励机制，要求下载者提供“下载证明"
同时也可以通过基于下载证明的激励，建立更健康的 `P2P` 体系。最近获取过某个内容的 Zone，也会在一段时间内继续为该内容提供加速支持。


### 透明加速

> 本章节内容为占位，还未做详细的协议设计

核心思路是：网络基础设施(比如路由器) 把 Pull 请求透明地拦截到 Cache Node，实现方式与现有透明加速网关类似。但因为要实现：

```text
将 Client 发往源 X 的请求，重定向到更快的源 Y
```

所以必须能识别并重定向 Pull 目标。这通常依赖明文的 CYFS 流量，或者依赖 Zone 内可共享可信私钥时对 HTTPS 级流量进行拦截。

## 内容购买与认证

> 我只返回数据给 '满足于条件的用户'

这里的认证描述的是 `cyfs://` 里的“君子协定”:协议只保证**诚实节点**会按约束传播和访问，并不在密码学上阻止恶意节点复制与转发。换句话说，这是一种**弱强制 + 基于声誉**的约束：配合 `cyfs-cascades`、`Reference` 等上下文信号，正常节点会选择遵守；恶意节点虽然技术上可以绕过，但也会因此失去后续收益分配、Curator 信用背书等上层好处。而且基于CYFS构建的Content Network的多源特性，一个节点拒绝返回数据给用户，通常不能100%保证用户无法得到数据。这个君子协定设计的目的是希望被大部分诚实节点遵守，提高”不道德节点“的作恶成本。严格的身份认证通常是写相关的，走的是 Zone 内的 NDM 相关协议，这里不展开讨论。落到 `cyfs://` 协议上，通常已经是跨 Zone 访问，此时至少偏向“半公开”场景，因为数据一旦到了公网，就没有 100% 可靠的方法阻止其继续传播。

```headers
cyfs-original-user: $user-did
Reference:
```

这里的核心思路是：请求方不仅表明“我是谁”，还表明“我是因为什么上游动作或页面跳转来到这里的”。对于一些半公开内容，服务端并不追求绝对防扩散，而是希望把访问能力绑定在某条业务链路上。

因此，权限控制可以同时参考：

- `cyfs-original-user`：谁发起了请求，这里有基于DID的身份信息
- `Reference` / `ReferencePath`：请求是从哪条内容传播链路进入的。
- `cyfs-cascades`：请求背后的动作链，例如“浏览页面 -> 点击购买 -> 请求附件”。
- `cyfs-proofs`：JWT格式的证明，基于original-user的身份构造

这种机制更接近“带上下文的访问约束”，而不是传统意义上的强访问控制。

这里需要特别说明“购买收据”和“访问许可”之间的边界。  
在 `cyfs://` 里，收据首先表达的是一个**经济事实**，例如：

```text
用户 Alice 购买了内容 movie:xxx
```

这条事实的主要价值是：网络里的诚实节点可以围绕它继续做收益分配、传播归因和后续激励。  
但它**不天然等价于**“只有 Alice 本人才能继续读取这个内容”。在很多 `Content Network` 场景里，别的用户携带这张收据继续访问，并不被协议视为有问题。比如 Alice 购买了一份研究报告，把报告链接和购买收据一起分享给 Bob；Bob 再带着这张收据去访问另一个诚实的 CYFS Gateway，该网关完全可以接受这张收据，因为它证明的是“这份内容已经有人为其付费”，而不是“只有付款人本人才能看到”。

这也正是前面“君子协定”那一节的落点：CYFS 记录和传播的是“谁付过钱、谁带来了这次传播、谁因为这次传播获得后续收益”这些事实，而不是试图在公网里用密码学绝对阻止二次扩散。  
如果某个业务确实需要“收据只能由付款人本人使用”的强约束，那应当把这种约束明确写进业务规则，并通常配合更强的 Zone 内身份认证去实现；它不是 `cyfs://` 默认假设的唯一模式。

### 验证购买收据

购买收据的设计目标，并不只是“付了钱才能看内容”这么简单。从 `Content Network` 的视角看，它更像是在协议层引入一种**可验证的收益分配起点**：当用户真正消费了某个内容，就能围绕这次消费，透明地把利益连接到`作者、收录者、传播者、消费者`这几类角色。

这里的核心思想是：

- 协议要保障“通过发布内容直接获得收入”的权利，而不是把定价、结算、分发全部交给中心化平台。
- 收益分配的起点应该尽量接近真实消费行为，而不是只看平台内部的展示量、点击量或模糊分成规则。
- 最理想的路径仍然是`消费者直接付费给创作者`，但协议也允许围绕收录、传播、导购等动作做后续分账。
- 购买凭证应该是**可携带、可缓存、可跨站点复用**的对象，而不是某个平台数据库里一条只能本平台识别的状态。

因此，购买收据在协议里通常至少要绑定下面这些信息：

- 谁买的：购买者 DID，说明“最初是谁完成了这次购买”。
- 买了什么：目标内容的 `ObjectId`，或某个内容集合/版本范围。
- 买到了什么权利：例如永久读取、限时读取、可下载次数、是否允许继续传播、是否允许他人基于该收据继续读取等。
- 付了多少钱：金额、币种、结算合约或订单号。
- 这张收据是否仍然有效：签发时间、过期时间、撤销状态。

这样，网关在“验证购买收据”时，做的事情就比较清晰了：

1. 验证收据本身的签名、链上状态或合约状态，确认它不是伪造的。
2. 读取收据里的业务语义：它到底是“付款事实证明”，还是“只绑定付款人本人的访问许可”。
3. 验证收据覆盖的内容范围，确认它确实允许读取当前请求的对象。
4. 如果业务规则要求“付款人与请求人一致”，再检查收据里的购买者身份是否与当前请求中的 `cyfs-original-user` 匹配；如果业务规则允许转交或继续传播，则这一步可以不要求一致。
5. 验证收据仍在有效期内，且没有被撤销或重复消费到超限。

当这些条件满足后，网关就可以把“用户有权读取这个内容”转化成一次正常的 `cyfs:// GET` 返回。也就是说，收据解决的是“这次读取是否被授权”，而内容本身的完整性校验仍然完全由 `ObjectId/ChunkId` 负责。

**CYFS协议的一个根本设计目标，是为了保障**
- 每个人都有通过发布内容获得收入的自由和权利
- 网络本身就是内容发行的基础设施
- 更合理的经济模型，让内容发行流程中的每个角色都能得到合理的收入，但最合理的是`消费者直接付费给创作者`

1. 用户访问链接，链接告知 Content 的商品信息和购买方法（兼容 HTTP 402）。
2. 用户根据购买方法的指引完成购买，得到收据。我们自带的去中心购买方法是基于 USDB 的内容购买合约。
3. 用户再次访问链接，并携带自己的收据。
4. CYFS 网关对用户身份和收据进行验证，然后返回内容数据。



## 内容的发布与Zone 内上传

CYFS 本身没有“上传公共数据”的统一协议设计，因为 CYFS 的定位是在互联网上高效可靠地获取公共数据，实现 `Content Network`。

**No Push! `cyfs://` 是 `pull-first` 的协议。** 跨 Zone 之间**不存在**“我把内容推给你”的语义，所有跨 Zone 分发最终都落在“对方主动 Pull”上。

但在单个 Zone 内部，很多产品逻辑仍然会出现“传统的上传行为”——比如用户在手机上选择一张照片，用 MessageHub 给朋友发送消息，在消息真正发出前，需要先把照片从手机搬到 OOD 上。这是**Zone 内**的发布流程，不违反 pull-first 约束。关于Zone内的数据流转，参考NDM系列协议(Named Data Maanger Protocol)的设计

### Zone间的内容传播


Zone 间**没有上传的概念**。如果 `ZoneA` 给 `ZoneB` 发送一个带附件的 `MessageObject`，并不会有所谓“附件上传”的逻辑。

其核心流程如下：`ZoneA` 只是把“消息对象”和“附件引用”交给 `ZoneB`，真正的数据流动仍然发生在 `ZoneB` 后续主动发起的 Pull 中。

也就是说，在跨 Zone 场景里要区分两件事：

1. **消息到达**：`MessageObject` 本身是一个较小的 `NamedObject`，可以通过应用层 API 直接送达。
2. **内容获取**：附件、引用对象、页面资源等较大的 `NamedData`，永远由接收方按需拉取。

因此，`sendmsg` 更像是在说：

```text
“我通知你：这里有一个对象，和一些你可以选择去 Pull 的引用。”
```

而不是：

```text
“我已经把附件推送到了你的 Zone 里。”
```

`ZoneB` 在收到消息后，通常会自己完成下面这些判断：

- 这个消息是否合法，发送者是否可信。
- 自己是否真的需要附件。
- 附件应该立刻下载、延迟下载，还是根本不下载。
- 应该优先从 `ZoneA` 拉，还是先向本地 Cache、收录者或其它已知源查询更快的来源。

只有在这些业务判断完成之后，`ZoneB` 才会对 `MessageObject.ref_obj[i]` 执行标准的 `open_reader_by_url` / `get_object_by_url`。一旦开始 Pull，后续流程就重新回到本文前面介绍的通用 CYFS 下载语义：验证 `PathObject`、验证 `NamedObject`、验证 `ChunkId`，必要时再走 `ChunkList`、`SameAs` 和多源调度。

这个设计有两个重要好处：

- 它保持了 `pull-first` 的统一语义。跨 Zone 的数据分发无需单独再设计一套“远程上传协议”。
- 它让接收方始终拥有最终控制权。是否下载、何时下载、从谁下载，都由接收方决定，而不是由发送方强行推送。

因此，跨 Zone 传播的本质不是“上传附件”，而是“发送一个可验证的引用，然后等待对方决定是否消费这个引用”。

```text
ZoneA Call ZoneB.sendmsg(MessageObject) # sendmsg是一个app service API
ZoneB.onmsg(MessageObject): # ZoneB自己的业务处理流程
    业务逻辑判断
    决定下载附件
ZoneB.open_reader_by_url(MessageObject.ref_obj[0]) # ZoneB决定从ZoneA下载内容
```


## 附录：协议参考

### ObjId的计算规则

`ObjId` 的本质是：

```text
{obj_type}:{obj_hash_bytes}
```

其中左侧的 `obj_type` 说明“这是哪一类对象”，右侧的 `obj_hash_bytes` 说明“这一类对象是如何被唯一绑定到具体内容上的”。在文本表达上，最常见的是：

```text
{obj_type}:{hex(obj_hash_bytes)}
```

在 URL hostname 等场景里，也可以使用前文约定的 base32 等价形式。

对协议实现来说，关键不是“长得像不像 Hash”，而是**不同类型的对象，`obj_hash_bytes` 的构造规则可能不同**。目前主要分成下面几类：

1. 标准 ChunkId  
   对原始字节直接计算标准哈希。

   ```text
   obj_hash_bytes = raw_hash_bytes(data)
   ObjectId       = "{hash_type}:" + hex(obj_hash_bytes)
   ```

2. 标准 NamedObject  
   对对象的 canonical JSON 做 Hash。

   ```text
   S              = RFC 8785 canonical JSON bytes
   obj_hash_bytes = sha256(S)
   ObjectId       = "{obj_type}:" + hex(obj_hash_bytes)
   ```

3. 带长度信息的对象  
   在摘要前面拼一个 `varint(length)`，让客户端仅凭 `ObjectId` 就能知道逻辑大小。`mix*` 与 `clist` 都属于这一类。

   ```text
   obj_hash_bytes = varint(u64(length)) || raw_digest_bytes
   ObjectId       = "{obj_type}:" + hex(obj_hash_bytes)
   ```

   这类对象里，`length` 的语义要按 `obj_type` 区分：

   | 类型 | `length` 的语义 |
   | --- | --- |
   | `mix*` | 单个 Chunk 自身的字节长度 |
   | `clist` | 整个 `ChunkList` 依次拼接后还原出的总字节长度 |

4. 特殊规则对象  
   少数标准对象会在“canonical JSON + Hash”的基础上再增加额外绑定信息，其规则由该对象自己的标准定义决定。

因此，一个实现只要掌握两件事，就能正确处理 CYFS `ObjId`：

- 先根据 `obj_type` 确定该类型使用哪一种 `obj_hash_bytes` 计算规则。
- 再按该规则对对象内容重新计算，并与文本里的 `obj_hash_bytes` 比较。

#### 特殊的ObjId
- `mix256`:在ObjId中编码了Chunk长度的sha256,是系统中用的最多的chunkId类型。
- `qcid`:快速全文Hash,取文件的5片数据进行mix256。这通常用于一些非严格场景的文件秒传和LocalLink模式的改变发现。
- `clist` chunklist， 其ObjId用和mix256一致的方法在id中编码了长度信息，其长度是整个chunklist所有的chunk的大小的总和

### 含有 ObjId 和 inner_path 的 URL

CYFS URL 可以分成两层：

1. 根定位部分：用于确定“从哪个对象开始解析”。
2. `inner_path` 链：用于在对象内部继续寻址。

根定位部分有两种常见形式：

- 直接对象链接（O Link）

```text
http://$zone_id/$obj_id
http://$zone_id/ndn/$chunk_id
http://$objid.$zoneid/
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

### CYFS HTTP Header 扩展

**CYFS RespHeader扩展**

- `cyfs-obj-id`: `ObjectId`。如果返回内容是一个 `NamedObject`，或者是一个 Chunk（或其 Range），则填写。如果返回的是 `NamedObject` 的某个非 `ObjectId` 字段值，则不填写。
- `cyfs-path-obj`: `JWT`。只有请求中包含语义路径时才会使用，用于证明“语义路径 -> 根对象”的绑定关系。
- `cyfs-parents-N`: `String`。用于给出 `inner_path` 解析过程中需要的父对象，`N` 从 `0` 开始连续编号。单项值格式为：
  - `oid:$objid`
  - `json:$base64url_canonical_json`
  小对象场景里通常直接给完整对象；大对象场景里也可以只给必要的 `ObjectId`，再配合 `cyfs-inner-proof` 验证。服务端 **SHOULD** 避免把过大的完整对象直接塞进 Header；当某个 parent object 过大时，应优先返回 `oid:` 形式，或切换到额外 proof / 二次获取的模式。
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
4. 如果响应还包含 `cyfs-parents-N` 或 `cyfs-inner-proof`，则继续验证 `inner_path` 链路。

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
