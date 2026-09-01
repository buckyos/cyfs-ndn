# NFSP：Named File System Protocol v0（草案）

> 文档状态：Draft / 非 normative（尚未实现）
> 版本：v0
> 日期：2026-08-31
> 定位：定义 **NamedFileSystem 对外的网络协议**。此前 `NamedFileMgr(NDM)` 是一个进程内编排层，
> FUSE 与 WebUI 各自以不同方式接入；本文把这层"协议化"，使 FUSE / 浏览器 / SDK / 跨 Zone 客户端
> 成为同一份协议的不同客户端。
>
> 上游依赖文档：
> - [NamedFileSystem_Ops_v3.md](./NamedFileSystem_Ops_v3.md)（操作语义，本文的 §3 数据模型全部继承自它）
> - [NamedFileSystem_Arch_v2_Overlay_InodeDentry.md](./NamedFileSystem_Arch_v2_Overlay_InodeDentry.md)
> - [NDM Protocol/overview.md](./NDM%20Protocol/overview.md)（本文是第二层"Zone 内访问"的**文件系统面**，与 `NamedDataMgr Zone Protocol` 并列）
> - [CYFS Protocol](./CYFS%20Protocol/CYFS%20Protocol.md)（对象与验证语义）
>
> 首个消费者：[buckyos/product/bucky_file/filebrowser_PRD.md](../../buckyos/product/bucky_file/filebrowser_PRD.md)
>
> 记法：**【待决策】** = 需要 owner 拍板；**【现状】** = 当前实现与本草案的差距。

---

## 0. 为什么不是 WebDAV，也不是 NFSv4

BuckyOS File Browser 是一个浏览器产品，最省事的做法是"实现一遍 WebDAV，前端接 WebDAV 客户端"。
本节先说明为什么这条路走不通，因为它同时也解释了 NFSP 的每一条设计取舍。

WebDAV（RFC 4918 + ACL 3744 + DASL 5323 + BIND 5842）的核心假设是：

1. **资源的身份就是 URL**，属性挂在 URL 上；
2. **服务器是唯一的数据源**，内容不可寻址、不可离线验证；
3. **一致性靠独占锁**（LOCK/UNLOCK），没有失效通知，客户端靠轮询；
4. **复制就是复制字节**（COPY），移动就是搬运；
5. **元数据是无类型的 XML property bag**，没有来源、没有置信度、没有可寻址的关联项。

而 BuckyOS 的现实是：内容寻址（ObjId）、Base/Upper overlay、单写租约、跨 Zone Pull、
AI 派生元数据、多维权限（含"能否进 AI 管线"）。这五条假设**每一条都不成立**。

| 维度 | WebDAV | NFSP v0 |
|---|---|---|
| 资源身份 | URL | `Ref = InodeRef(id,gen) \| ObjRef(obj_id[,inner_path])`；路径只在 resolve 中出现 |
| 目录列举 | `PROPFIND Depth:1`，一次吐全量 XML，无分页无排序保证 | `list` 带 attr 掩码、游标分页、稳定排序、返回 `dir_rev` |
| 冻结子树 | 无此概念 | `Frozen{obj_id}`：整棵子树不可变，可从任意不可信源拉取并本地验证 |
| 元数据 | dead property 绑 URL，rename 即失联 | meta 绑 **ObjId**（内容），带 `ns/source/confidence/provenance` |
| 上传 | `PUT`，无查重、断点靠非标准 Range PUT | `probe(hash)` 秒传 → tus 续传 → `commit`；天然去重 |
| 下载 | `GET`，只能信 TLS + 服务器 | Range GET + mtree path proof，可多源并发、可走 CDN |
| 复制 / 快照 | `COPY` 复制字节，大目录不可用 | `link_obj` / `publish_dir`：O(1) 换绑定，快照即 `DirObjId` |
| 并发 | `LOCK` 独占写锁（实现普遍形同虚设） | file lease（fencing seq）+ dir `rev` CAS + 主动 recall |
| 变更通知 | 无（轮询） | `watch` SSE：`dir_changed(rev)` / `meta_changed` / `view_updated` / `lease_recall` |
| 聚合视图 | 无；用 `BIND` 会造成"文件被复制"的错觉 | `View` 一等只读对象，成员强制携带 `canonical_path` + `provenance` |
| 搜索 | DASL（RFC 5323），几乎无人实现，无法表达语义召回 | `search` 一等 op，返回 `match_source` + `explain` + 统一游标 |
| 权限 | ACL(RFC 3744)，只有 CRUD 维度 | 多维 policy（含 `index` / `ai_process` / `publish`）+ 可离线验证的 cap token |
| 错误 | HTTP 状态码，语义有损 | 结构化状态：`NEED_PULL` / `REV_MISMATCH` / `LEASE_CONFLICT` / `REFERRAL` |
| 扩展 | 自定义 XML 命名空间（无 must-understand 语义） | 会话级 feature 协商 + 请求级 `ext[{id, critical}]` + **对象 schema 扩展** |

NFSv4.1/SMB3 在一致性与扩展机制上远比 WebDAV 成熟（COMPOUND、session slot、delegation/lease、
pNFS layout、attribute bitmap），NFSP 在这些点上直接借鉴；但它们同样假设"服务器是唯一数据源"、
"元数据是定长属性集"，无法表达冻结子树、内容寻址去重、AI 派生元数据和管线权限。
**NFSP = NFSv4 的状态机严谨性 + git/CAS 的数据面 + 面向浏览器的传输绑定。**

---

## 1. 设计目标与非目标

### 1.1 目标

- **G1 单一协议、多种客户端**：FUSE daemon、浏览器 File Browser、Rust SDK、跨 Zone 客户端说同一份协议。
  FUSE 从"实现方"降级为"有损适配器"。
- **G2 把不可变/可变的分界暴露给客户端**：`Frozen` 子树可被无限缓存、任意源分发、离线验证。
  这是 NFSP 相对一切传统 FS 协议的结构性优势，是整个设计的支点。
- **G3 浏览器一等公民**：控制面纯 JSON，可被 `fetch` 直接调用；数据面必须能塞进 `<img src>` / `<video src>`
  并支持 Range，否则缩略图与视频预览无法实现。
- **G4 高信息密度的列举**：一次 `list` 就能填满 File Browser 的列表视图 + 图标视图，不需要 N+1 次往返。
- **G5 元数据是一等公民且可解释**：AI meta、Topic、Story、搜索命中都必须携带来源与理由。
- **G6 不变式在协议边界强制**：`I1..I6`（见 Ops_v3 §0.3）的检查点必须在协议 handler 内，
  不存在"绕过高层 API 的低层入口"。
- **G7 演进不破坏**：新增能力通过 feature 协商 + `ext` 记录 + 对象 schema 扩展，不改变已有报文语义。

### 1.2 非目标

- **不追求 POSIX 兼容**。语义以本文与 Ops_v3 为准（继承 Arch_v2 §0.2）。
- **不定义身份系统与支付**（沿用 CYFS Protocol 的边界）。
- **不定义 AI 管线本身**。协议只定义 meta 的载体、来源标注和管线权限的表达。
- **不做多写者冲突合并**。严格单写（I4）是前提；协作编辑是上层组件的事。
- **不在 v0 定义跨 Zone 写**。v0 跨 Zone 只读（Pull），写留给后续版本。

---

## 2. 术语（在 Ops_v3 基础上新增）

| 术语 | 含义 |
|---|---|
| **Ref** | 客户端持有的资源引用。`InodeRef{inode_id, gen}` 或 `ObjRef{obj_id, inner_path?}`。**所有 op 只认 Ref，不认路径。** |
| **Frozen / Live** | `resolve` 的两种成功结果。Frozen = 落在不可变对象平面；Live = 落在可变命名空间（inode）。 |
| **Realm** | 一个独立的挂载命名空间。v0 定义两个内建 realm：`dfs`（DFS 逻辑视图）与 `dev:<device_id>`（设备/裸盘视图）。 |
| **Referral** | list/resolve 结果中的"此处通向另一个 realm/zone"的条目，客户端需换端点继续。 |
| **View** | 只读的虚拟集合（Topic / 搜索结果 / 标签筛选）。成员强制携带 `canonical_path`。 |
| **MetaRecord** | 一条带命名空间与来源的元数据。锚定在 ObjId（内容）或 InodeRef（可变节点）上。 |
| **Cap** | 能力凭证。签名的、可离线验证的、限定 `(subtree, ops, expiry)` 的令牌。分享链接与 AI 管线授权都用它。 |
| **AccessUrl** | 同一份内容的一个可访问入口。一个文件可以同时有多个（fs path / cyfs ObjId / public URL / signed URL）。 |

---

## 3. 数据模型

### 3.1 三平面（继承 Ops_v3 §0.1）+ 第四平面

| 平面 | 性质 | NFSP 中的位置 |
|---|---|---|
| 命名空间（fs_meta） | 可变、强一致、小消息、延迟敏感 | 控制面 `POST /nfs/v1/*` |
| 对象（named_store） | 不可变、内容寻址、带宽敏感、无限可缓存 | 数据面裸 URL，复用 NDM/CYFS 既有入口 |
| 写缓冲（file_buffer） | 可变、易失、单写者 | 上传面，复用 tus |
| **通知 / 撤销**（新增） | 服务端 → 客户端 push | `GET /nfs/v1/watch`（SSE） |

第四平面是当前架构缺失的。没有它，客户端不敢缓存目录视图，`dir.rev` 只能做冲突检测而不能做缓存失效，
多标签页 / 多设备之间的"上传后别处看不到"只能靠轮询解决。

### 3.2 Resolve 的四种结果（核心）

```
Resolve(realm, path, opts) →
  Live     { inode_ref, kind, rev, attrs, caps, policy_digest }
  Frozen   { obj_ref, inner_path, kind, attrs, proof? }
  Referral { target_realm, target_endpoint, rest_path }
  NeedPull { obj_id, hints[] }          // 不是错误，是状态
  NotFound
```

**`Frozen` 是本协议最重要的返回值。** 拿到它的客户端获得如下授权：

- 该 `obj_id` 指向的子树**永不改变**，可以按 `Cache-Control: immutable` 缓存到天荒地老；
- 可以从**任何来源**（CDN、邻居设备、另一个 Zone、本地缓存）拉取，用 mtree path proof
  与容器 key proof 本地验证（见 [基于mtree的chunkid.md](./Reviews/基于mtree的chunkid.md)、
  [对象大容器需求草案.md](./Reviews/对象大容器需求草案.md)）；
- 子树内部的后续解析**不需要再联系元数据服务**（`inner_path` 在对象平面自行展开）。

这相当于"子树级 delegation，且无需 recall"——NFSv4 的 delegation 只能委托单文件且必须可召回。
对 File Browser 的直接收益：浏览已发布/已快照的目录树时，除首次 resolve 外零元数据请求。

**`NeedPull` 必须与 `NotFound` 区分**（Ops_v3 I6）。返回给客户端而非在服务端阻塞拉取，使得：
客户端自己可以成为数据源；File Browser 可以显示"离线/需拉取"的第三态而不是假装文件不存在；
FUSE 适配器可自行选择"同步拉完再返回"或"返回 EAGAIN + 后台预取"。

**【现状】** `named_store::get_dir_child` 把"对象不在本地"与"目录中无此名"都返回 `NotFound`，
违反 I6。这是 NFSP 落地前必须先修的前置项。

### 3.3 属性集与掩码

所有返回属性的 op 都带 `want: [attr...]` 掩码。属性分组（新增属性 = 新名字，老客户端不请求即不返回）：

```
base    : name, kind, size, mtime, ctime, flags
ident   : obj_id, inode_id, gen, etag
frozen  : frozen(bool), base_obj_id, need_pull
access  : access_urls[]              // §3.6
meta    : meta_digest, meta_summary  // 轻量摘要，完整 meta 走 get_meta
thumb   : thumb{obj_id|url, w, h}    // §3.7
policy  : policy_digest, effective_ops[]
stats   : subtree_size, subtree_count   // 仅目录，可能是估算
```

`kind ∈ { file, dir, symlink, referral, view }`。

`flags` 是位集合：`frozen | need_pull | read_only | writing | shared | public | pipeline_excluded`。
File Browser 的列表视图靠这一个字段就能画出全部角标。

### 3.4 MetaRecord（相对 WebDAV property 的关键升级）

```jsonc
{
  "ns": "exif" | "fs" | "user" | "ai.vision.v1" | "ai.kb.v1" | "story.im",
  "key": "gps",
  "value": { /* 任意 JSON */ },
  "source": {
    "kind": "system" | "user" | "pipeline" | "peer",
    "app_id": "photo-indexer",
    "pipeline_ver": "2026.07",
    "at": 1756600000
  },
  "confidence": 0.86,              // 可选，AI 产出必填
  "anchor": "obj:sha256:...",      // 锚在内容上；可变文件则为 "inode:1234"
  "links": [                        // 可点击关联项（PRD 9.6 硬需求）
    { "rel": "topic", "ref": "view:topic/2026-hokkaido", "label": "北海道之行" },
    { "rel": "place", "ref": "view:place/sapporo",       "label": "札幌" },
    { "rel": "person","ref": "view:person/alice",        "label": "Alice" }
  ],
  "visibility": "private" | "zone" | "public"
}
```

三个设计要点：

1. **锚定在 ObjId 上，不锚定在路径上。** 文件移动、重命名、被多处引用、跨 Zone 复制之后，
   AI 分析结果不丢失也不需要重算。WebDAV 的 dead property 绑 URL，`MOVE` 之后属性能否跟随
   由服务器实现自由决定——这在需要昂贵 AI 推理的场景下是不可接受的。
2. **`links` 里的每一项都是可寻址的 `Ref`。** PRD 9.6 要求 tag / topic / 地点 / 人物"点了能跳"，
   协议直接返回可跳转目标，前端不需要猜。
3. **`source` + `confidence` 是强制的可解释性载体**，对应 PRD 7.5 与 12.6：
   哪条 meta 由哪个管线在什么版本生成、可信度多少、可见性如何，全部结构化。

### 3.5 View（Topic / 搜索结果 / 筛选）

PRD 9.4 有一条强约束：**Topic 不能让用户觉得文件被复制了**，且明确否决了"为 Topic 造符号路径"。
协议侧的答案是：View 是一个**独立 kind**，不出现在任何目录的 `list` 结果里，且每个成员强制带
`canonical_path`。

```jsonc
{
  "view_id": "topic/2026-hokkaido",
  "origin": "auto" | "manual" | "merged",
  "title": "北海道之行",
  "generation": 17,                  // 变化时通过 watch 推送 view_updated
  "stale": false,
  "groups": [                        // 最多 3 层（PRD 硬约束），由服务端决定分组维度
    { "by": "source", "label": "来自 Alice", "count": 34,
      "members": [
        { "ref": {"obj_id":"sha256:..."},
          "canonical_path": "dfs://home/photos/2026/07/IMG_0912.HEIC",
          "provenance": { "why": "同一 IM 会话内接收", "matched_by": "story.im", "score": 0.91 } }
      ] }
  ]
}
```

- `canonical_path` 与 `provenance.why` 是**协议强制字段**，直接对应 PRD 7.3 / 9.4"必须能回到真实路径"
  与"用户要理解为什么"。
- View **只读**。手工调整通过 `view.patch` 表达为一层 overlay（`add / remove / pin`），
  与文件系统的 Base/Upper overlay 同构：自动发现结果 = Base Layer，用户手工调整 = Upper Layer。
  这样 PRD §14.2"手动 Topic 与自动 Topic 冲突如何处理"就不再是产品问题，而是既有的 overlay 合并规则。

### 3.6 AccessUrl（一个文件的多重身份）

PRD 9.2 明确要求：`public` 目录下的文件要同时显示存储路径和公网 URL，且"用户必须能明确理解
文件真实存放在哪、从互联网访问该用哪个 URL"。协议直接把这件事结构化：

```jsonc
"access_urls": [
  { "kind": "fs",     "url": "dfs://home/public/report.pdf", "primary": true },
  { "kind": "cyfs",   "url": "cyfs://o/$zoneid/sha256:abcd...", "immutable": true },
  { "kind": "public", "url": "https://alice.buckyos.io/public/report.pdf" },
  { "kind": "pinned", "url": "https://alice.buckyos.io/ndn/sha256:abcd...", "immutable": true },
  { "kind": "signed", "url": "https://alice.buckyos.io/s/eyJhb...", "expires_at": 1756699999,
    "cap_id": "cap_7f3a", "revocable": true }
]
```

这一条同时回答了 PRD §14.7 的三个子问题：
多域名 = 服务端返回多个 `public` 条目；版本化链接 = `kind: pinned`（指向 ObjId，天然不可变）；
临时链接 = `kind: signed`（cap token，带 `expires_at` 且可撤销）。

### 3.7 派生表示（缩略图 / 预览）

图标视图和预览面板是 MVP 需求，但 PRD 未定义缩略图从哪来。协议侧定义：
**派生表示本身也是一个 ObjId**，作为原对象的 `derived` 关系存在。

```
GET /nfs/v1/repr/{obj_id}/{profile}      // profile: thumb256 | thumb1024 | preview | text
  → 302 到该 profile 的 ObjId 数据面 URL（可缓存、可 CDN、可秒传）
  → 202 + {state:"generating", retry_after} 若尚未生成
```

好处：缩略图与原文件走同一套去重、分发、验证机制；同一张图在多个 Topic 里出现只生成一次；
浏览器可以直接 `<img src="/nfs/v1/repr/sha256:.../thumb256">`。

---

## 4. 传输绑定与消息骨架

### 4.1 端点

沿用既有风格（对齐 `/ndm/v1/store/{method}`）：

| 面 | 端点 | 说明 |
|---|---|---|
| 控制面 | `POST /nfs/v1/{method}`，JSON in/out | 所有元数据操作 |
| 批量 | `POST /nfs/v1/batch` | COMPOUND-lite，见 §4.3 |
| 通知面 | `GET /nfs/v1/watch` | SSE（`text/event-stream`） |
| 数据面（读） | `GET /ndn/{obj_id}`、`GET /nfs/v1/repr/{obj_id}/{profile}` | 裸 URL，支持 Range，可直接用于 `<img>/<video>` |
| 上传查缺 | `POST /nfs/v1/probe` | 批量 `FindMissingBlobs`，秒传的前提 |
| 数据面（写） | 复用 `POST /ndm/v1/uploads`（tus 1.0.0） | 见 §5.3 |

**【待决策】** 是否在 v0 就定义 QUIC/kRPC 绑定。建议 v0 只定义 HTTP 绑定（浏览器与 FUSE 都够用），
把消息结构设计成传输无关，QUIC 绑定留到 v1（届时 `watch` 走 server stream 而非 SSE）。

### 4.2 请求 / 响应信封

```jsonc
// 请求
{
  "session": "sess_...",         // hello 返回；携带 cap 与协商结果
  "seq": 1042,                   // 单调递增，用于 exactly-once（§6.3）
  "at": { "realm": "dfs", "path": "/home/photos" },   // 或 {"ref": {...}}
  "want": ["base","ident","frozen","thumb"],
  "args": { /* 方法参数 */ },
  "ext": [ { "id": "x.buckyos.trace", "critical": false, "payload": {...} } ]
}

// 响应
{
  "ok": true,
  "result": { /* 方法结果 */ },
  "server_rev": 88213,           // 全局逻辑时钟，用于客户端排序/去重
  "ext": [...]
}
```

### 4.3 batch（COMPOUND-lite）

```jsonc
POST /nfs/v1/batch
{
  "session": "...",
  "start": { "realm": "dfs", "path": "/home" },
  "ops": [
    { "m": "walk",  "args": { "names": ["photos","2026"] } },   // 移动游标
    { "m": "list",  "args": { "limit": 200 }, "want": ["base","ident","thumb"] },
    { "m": "stat",  "args": { "name": "cover.jpg" }, "want": ["meta","access"] }
  ],
  "on_error": "abort" | "continue"
}
```

规则（借鉴 NFSv4 COMPOUND）：
- 顺序执行，共享一个 **解析游标**（current ref），`walk` 移动游标；
- 默认 `abort`：首个失败即停止并返回已完成结果 + 失败位置；
- batch **不是事务**。需要原子性的多步操作必须用专门的 op（如 `move`），不能靠 batch 拼。
  这一点必须在文档里写死，否则会被误用。

收益：`open("/a/b/c.txt", O_CREAT|O_WRONLY)` 从 4 个 RTT 变成 1 个；
File Browser 打开一个目录 = 1 次请求拿到列表 + 首屏缩略图引用 + 面包屑属性。

### 4.4 扩展机制（三层）

1. **会话级 feature 协商**：`hello` 一次性协商，不在每个 op 上协商。
   ```jsonc
   POST /nfs/v1/hello
   { "versions": ["nfsp/0"], "features": ["frozen-subtree","view","search.semantic","watch.sse","repr"] }
   →
   { "version": "nfsp/0", "features": [...交集...], "limits": { "max_batch": 64, "max_list": 1000 },
     "realms": [ {"id":"dfs","writable":true}, {"id":"dev:node-a","writable":false} ] }
   ```
2. **请求级扩展记录**：`ext: [{id, critical, payload}]`。未知且 `critical` → 拒绝并返回
   `UNSUPPORTED_EXT`；未知且非 critical → 忽略。语义同 X.509 critical extension / QUIC transport parameter。
   比 NFSv4 的全局 attribute bit 注册表更易演进，也比 WebDAV 自定义 XML 命名空间（无 must-understand 语义）安全。
3. **对象 schema 扩展（免费午餐）**：ACL、xattr、业务字段、AI meta 全部作为 FileObject/DirObject/MetaObject
   的新字段，canonical JSON 编码，老客户端读到不认识的字段直接忽略。

**结论：协议本体刻意做小，扩展性预算全部花在对象 schema 与 meta namespace 上。**
新增一类 AI meta 不需要改协议；新增一种 View 分组维度不需要改协议。

---

## 5. 操作集

分六组：会话、解析与列举、写入、视图与搜索、元数据与策略、通知。
MVP 子集见 §12.2。

### 5.1 会话

| method | 说明 |
|---|---|
| `hello` | 版本/feature/limits/realms 协商，建立 session |
| `bye` | 主动释放 session（连带释放租约与 watch） |

### 5.2 解析与列举

#### `resolve` / `stat`

```jsonc
{ "at": {"realm":"dfs","path":"/home/photos/2026/cover.jpg"},
  "want": ["base","ident","frozen","access","thumb","meta"],
  "args": { "follow_symlink": true, "sym_limit": 40 } }
→
{ "kind":"file", "state":"frozen",
  "obj_id":"sha256:...", "inner_path": null,
  "size": 4194304, "mtime": 1756500000,
  "flags": ["frozen","public"],
  "access_urls":[...], "thumb": {"obj_id":"sha256:...","w":256,"h":171},
  "meta_summary": { "ai.vision.v1": 3, "exif": 12, "user": 1 } }
```

**【待决策】** Ops_v3 §2.2 记录了一处不对称：读路径（open_reader）不跟随 symlink，写路径跟随。
NFSP 必须定死：**默认跟随（`sym_limit=40`），`follow_symlink:false` 为显式变体**，各入口统一。

#### `list`

```jsonc
{ "at": {...}, "want": [...],
  "args": { "cursor": null, "limit": 200,
            "order": "name" | "mtime" | "size",
            "filter": { "kind": ["dir","file"], "name_glob": "*.jpg" } } }
→
{ "entries": [ { "name":"IMG_0912.HEIC", "kind":"file", "ref":{...},
                 "size":..., "mtime":..., "flags":["frozen"], "thumb":{...} } ],
  "next_cursor": "c_8f21...", "dir_rev": 4417, "base_obj_id": "sha256:...",
  "mount_mode": "overlay", "truncated": false,
  "watch_token": "w_92ab" }
```

关键约束：

- **只有 Depth:1**。不提供 WebDAV `Depth: infinity` 那种递归列举——它在大目录上是 DoS。
  需要整棵子树时用 `get_tree`（仅对 Frozen 子树可用，因为它本质是读一个不可变对象）。
- **游标必须在并发变更下稳定**。`cursor` 编码 `(dir_rev_at_start, last_name)`；
  若期间 `dir_rev` 变化，服务端返回 `rev_changed: true` 但**继续按 name 序推进**（不重置），
  客户端可选择接受轻微不一致或重新拉取。这条必须写死，否则超大目录 + 上传并发下前端会重复/漏项。
- **排序在服务端做且必须稳定**（按 name 的字节序，规范化规则见 Arch_v2 §3.3.2）。
  Overlay 合并（base + upper dentries）对客户端透明。
- `watch_token` 让客户端可以只订阅当前打开的几个目录（对应 PRD 的多标签页）。

**【现状】** 当前 FUSE 侧用 `start_list / list_next / stop_list` 三段式有状态列举
（见 [fuse_behavior.md](../src/fs_daemon/fuse_behavior.md)），服务端要维护会话状态。
NFSP 改为**无状态游标**：服务端不记忆列举会话，标签页关掉不需要清理，也更适合浏览器。

#### `get_tree`

```jsonc
{ "args": { "obj_id": "sha256:...", "depth": 3, "want": [...] } }
```
仅对 Frozen 子树可用，一次返回整棵不可变子树。这是 File Browser 展开已发布目录树（侧边栏）
和"打包下载"的基础。对应 Bazel RE 的 `GetTree`。

#### `referral`

`list`/`resolve` 返回 `kind: "referral"` 的条目时携带：
```jsonc
{ "kind":"referral", "name":"node-a",
  "target": { "realm":"dev:node-a", "endpoint":"https://node-a.zone.local/nfs/v1", "path":"/" },
  "state": "online" | "offline" | "unauthorized" }
```
这是 PRD 9.3"设备/裸盘视图"的协议表达：设备树的每个节点是一个 referral，
客户端换端点继续 `list`。`state` 让 UI 能区分"设备离线"与"无权限"——PRD 没提，但这是必须的
（否则用户只会看到一个诡异的空目录）。

### 5.3 写入路径

| method | 说明 |
|---|---|
| `mkdir` | 幂等，mkdir -p 语义 |
| `open_write` | 取文件写租约 + 返回上传目标（layout） |
| `commit_file` | 把 `obj_id` 或 `fb_handle` 绑到路径，释放租约 |
| `link_obj` | **把一个已存在的 ObjId 绑到路径**（秒传 / 引用），O(1)，无字节复制 |
| `move` | 换绑定，O(1)，跨目录双 rev CAS |
| `delete` | 写 tombstone 或删行（按 base 是否存在同名项） |
| `publish_dir` | 目录快照 → 新 `DirObjId`（分享 / 备份 / 冻结） |
| `set_read_only` | 切换挂载模式 |

所有结构写操作携带 `expected_rev`（目录）或 `(session, lease_seq)`（文件），
失败返回 `REV_MISMATCH` / `LEASE_CONFLICT`，客户端可重试。这替代了 WebDAV 的 `LOCK`——
WebDAV 锁是悲观的、有超时的、且实现质量普遍很差；NFSP 对目录用乐观并发（rev CAS），
对文件用带 fencing 的租约（Ops_v3 §1.3）。

#### 上传流程（去重 + 断点，对应 PRD 9.7）

```
1. 客户端本地计算 hash（大文件用 mtree，边读边算）
2. probe:  POST /nfs/v1/probe { "digests": [ {hash,size}, ... ] }
           → { "missing": [...] }                      // 命中即"秒传"
3. 对 missing 的每个 chunk：走既有 tus 流程 POST /ndm/v1/uploads + PATCH（断点续传）
4. commit_file { at: path, obj_id, expected_rev }       // 一次原子绑定
```

- 步骤 2 直接复用 `GET /ndm/v1/objects/lookup` 的能力，但改为**批量**（`FindMissingBlobs` 语义），
  否则一个 1000 张照片的文件夹上传要 1000 次往返。
- 步骤 4 之前文件在命名空间中**不可见**——避免 File Browser 显示"上传中的半个文件"。
  【待决策】是否需要"可见的上传占位项"（用户体验上通常需要进度条条目）。
  建议：占位项由**客户端本地渲染**，不进命名空间；这样多标签页之间不会互相看到对方的半成品。

**【待决策】** 当前 zone gateway 的 upload session 是 **per-chunk** 的
（见 [named-data-mgr-zone-protocol.md](./NDM%20Protocol/named-data-mgr-zone-protocol.md) §1），
文件级上传只是客户端编排。对 File Browser 的"拖入一个 5GB 视频"场景，
需要一个**文件级上传会话**（服务端记住已完成 chunk 集合），否则刷新页面就得客户端自己重建进度。
建议 NFSP v0 增加 `upload_session` 作为 per-chunk session 之上的一层编排对象。

### 5.4 视图与搜索

#### `open_view` / `view_page` / `view_patch`

```jsonc
open_view { "view_id":"topic/2026-hokkaido", "group_by": null, "limit": 100 }
→ { view 对象，见 §3.5 }

view_patch { "view_id":"...", "ops":[ {"add": ref}, {"remove": ref}, {"pin": ref} ] }
→ { "generation": 18 }   // 生成 upper overlay，不改动自动发现结果
```

`group_by: null` 表示由服务端（AI）决定分组维度——PRD 9.4 要求分组结构"由底层 AI 提示词根据
文件量与聚合特征动态构造"，所以协议不能把分组维度写死成枚举，而是服务端返回 `groups[].by` 的**字符串**，
前端按通用规则渲染。**层级由服务端保证 ≤ 3**（PRD 硬约束），协议在响应里显式带 `depth` 供前端断言。

#### `search`

```jsonc
{ "args": {
    "q": "北海道 滑雪",
    "scope": { "realm":"dfs", "path":"/home", "recursive": true },   // null = 全部有权限范围
    "modes": ["name","fulltext","semantic"],
    "cursor": null, "limit": 50 },
  "want": ["base","ident","thumb","access"] }
→
{ "hits": [
    { "ref": {...}, "canonical_path": "dfs://home/photos/2026/IMG_0912.HEIC",
      "match_source": "semantic",              // name | dirname | fulltext | semantic | meta
      "score": 0.83,
      "explain": { "matcher": "ai.vision.v1",
                   "evidence": "图像描述包含『雪山』『滑雪板』",
                   "evidence_ref": {"ns":"ai.vision.v1","key":"caption"} },
      "snippet": "...在<em>北海道</em>的第三天..." } ],
  "next_cursor": "...", "partial": true,
  "sources": [ { "mode":"fulltext", "state":"ok", "took_ms": 12 },
               { "mode":"semantic", "state":"degraded", "reason":"kb_index_lagging" } ] }
```

三个为 PRD 专门设计的字段：

- **`match_source`** 直接对应 PRD 9.5"结果需标识来源类型：文件名命中 / 文件夹名命中 / 全文命中 / AI 语义命中"。
- **`explain`** 对应"用户应能大致理解为什么它会出现在结果里"。结构化返回，展示到什么程度由前端决定
  ——这正好回答 PRD §14.3。
- **`sources[]`** 对应"多路召回融合"的现实：知识库索引可能滞后或不可用。
  没有这个字段，前端只能在语义召回失效时假装一切正常，用户的"掌控感"就没了。

`partial: true` 表示还有其他 mode 的结果在路上。**【待决策】** 是否支持流式返回
（SSE 分批推送先到的 name 命中，语义命中后到）。File Browser 的体验上强烈建议支持，
建议 v0 定义为可选 feature `search.stream`。

**权限过滤必须在服务端做**（PRD 12.3），且过滤后的 `total` 不应泄露被过滤掉的数量。

### 5.5 元数据与策略

| method | 说明 |
|---|---|
| `get_meta` | `{ ref, ns: ["exif","ai.*"] }` → `MetaRecord[]` |
| `set_meta` | 只允许写 `user` ns；`ai.*` 需要携带 pipeline cap |
| `get_policy` | 返回**有效策略**及其继承来源 |
| `set_policy` | 目录级设置，文件级可覆盖 |
| `grant` / `revoke` | 生成 / 撤销 cap token（分享链接） |

#### policy 的多维结构（PRD 9.8 / 12.6 的协议表达）

传统 FS 协议（含 WebDAV ACL）只有 CRUD 维度。BuckyOS 需要"这个目录能不能进知识库"这类维度：

```jsonc
get_policy { "at": {"realm":"dfs","path":"/home/private/medical"} }
→
{ "effective": {
    "read": "allow", "write": "allow", "list": "allow",
    "share": "deny",
    "index.fulltext": "allow",
    "index.semantic": "deny",       // 不进知识库
    "ai.process":     "deny",        // 不触发 AI 后处理管线
    "ai.external":    "deny"         // 即使允许 ai.process，也禁止出网模型
  },
  "inherited_from": {
    "index.semantic": "dfs://home/private",
    "ai.external":    "dfs://"        // zone 默认策略
  },
  "triggers": [                        // PRD 9.8：让用户看到这里挂了什么
    { "app_id":"kb-indexer", "on":"on_new_file_upload", "state":"suppressed",
      "reason":"policy index.semantic = deny" }
  ] }
```

`inherited_from` 与 `triggers[].reason` 是**可解释性的协议载体**，直接支撑 PRD 10.5
"用户理解 AI 为什么处理了某个目录"这个关键场景。没有它，前端只能显示一个开关，
用户永远不知道为什么某个目录的开关看起来是开的但实际没生效。

**【待决策】**（对应 PRD §14.6）管线权限的最小粒度。建议：**目录级为主 + 文件级覆盖**，
不做策略模板。理由：文件级为主会导致 policy 表与文件数同阶且难以解释；
纯目录级则无法处理"这个目录整体可索引，但这一份合同不行"。

#### cap token（分享 / 协作）

```jsonc
grant { "args": { "subtree": {"realm":"dfs","path":"/home/public/trip"},
                  "ops": ["read","list"], "ttl": 604800,
                  "audience": "did:bns:bob" | null,   // null = bearer（任何人凭链接）
                  "max_uses": null } }
→ { "cap_id":"cap_7f3a", "token":"eyJhb...", "url":"https://alice.buckyos.io/s/eyJhb..." }
```

相对 WebDAV ACL 的差别：cap 是**可离线验证**的签名凭证，不需要接收方在本 Zone 有账号，
也不需要每次访问回源查 ACL；跨 Zone 分享天然可用。撤销通过 `cap_id` 黑名单 + 短 TTL。

### 5.6 通知（watch）

```
GET /nfs/v1/watch?session=...&tokens=w_92ab,w_31cd
Accept: text/event-stream

event: dir_changed
data: {"inode":1234,"rev":4418,"hint":{"added":["new.jpg"],"removed":[]}}

event: meta_changed
data: {"anchor":"obj:sha256:...","ns":["ai.vision.v1"]}

event: view_updated
data: {"view_id":"topic/2026-hokkaido","generation":18}

event: lease_recall
data: {"inode":5678,"reason":"conflicting_writer","grace_ms":3000}

event: policy_changed
data: {"path":"dfs://home/private","dims":["index.semantic"]}
```

这是 File Browser 从"能用"到"好用"的分水岭：

- 多标签页打开同一目录，一处上传另一处立刻刷新；
- 手机上传的照片在桌面浏览器里自动出现；
- AI 管线跑完后 meta 面板自动填充，不需要用户手工刷新（这对"AI 增强"的体感是决定性的）；
- FUSE daemon 收到 `dir_changed` 后调 `fuse_lowlevel_notify_inval_entry`，
  内核缓存可以开到很长的 `entry_timeout` / `attr_timeout`，getattr/readdir 的 RPC 量下降一个数量级。

`hint` 是尽力而为的：客户端**不得**依赖 hint 做增量更新的正确性来源，
只能用它做"是否值得重拉"的优化。正确性来源永远是 `rev`。

**【待决策】** watch 的重连语义。建议：SSE 断线重连时带 `Last-Event-ID`（= 上次收到的 `server_rev`），
服务端若无法补齐（缓冲已滚动）则返回一条 `resync` 事件，客户端全量重拉。
不设计"保证不丢事件"——那会把服务端拖成一个消息队列。

---

## 6. 一致性、缓存与幂等

### 6.1 三档缓存

| 对象 | 缓存策略 | 失效方式 |
|---|---|---|
| Frozen 子树（含其中所有文件、目录、缩略图） | `Cache-Control: public, max-age=31536000, immutable` | 永不失效 |
| Live 目录视图 | 客户端按 `dir_rev` 缓存 | `watch` 推 `dir_changed`；无 watch 时按 `hello` 返回的 `attr_ttl` 兜底 |
| Live 文件属性 | 同上 | 同上 |
| Meta | 按 `meta_digest` 缓存 | `meta_changed` |

第一档是 NFSP 的价值所在：一旦 File Browser 浏览到一个已 `publish_dir` 的目录，
整棵子树（包括所有缩略图）都可以被浏览器 HTTP 缓存、Service Worker、乃至 CDN 无条件缓存。
WebDAV 里做不到这件事，因为 URL 背后的内容随时可能变。

### 6.2 ETag = ObjId

`GET` 数据面的 `ETag` 直接用 `obj_id`。这使得：
- `If-None-Match` 天然正确；
- 两个不同路径指向同一内容时浏览器只下一次；
- 断点续传与多源并发下载的一致性由内容哈希保证，而不是由服务器承诺保证。

### 6.3 exactly-once（借鉴 NFSv4.1 session slot）

写操作在网络重试下必须幂等。协议做法：
- `session` + 单调 `seq` 构成重放检测键，服务端保留最近 N（`hello` 返回 `replay_window`）条结果；
- 重复 `seq` 直接返回缓存的响应，不重新执行；
- `seq` 超出窗口 → `SEQ_OUT_OF_WINDOW`，客户端必须重新 `resolve` 后再决定。

没有这一层，`mkdir` / `link_obj` / `commit_file` 在超时重试下会产生重复项或错误的 `AlreadyExists`。
这是 WebDAV 长期存在但从未解决的问题（`PUT` 幂等，但 `MKCOL` / `MOVE` 不是）。

### 6.4 batch 不是事务（重申）

见 §4.3。需要原子性时用专门 op。**【待决策】** 是否暴露显式事务
（fs_meta 已有 `begin_txn/commit/rollback`，Ops_v3 §1.1）。
建议**不暴露给网络客户端**：Ops_v3 已经指出 `BEGIN IMMEDIATE` 会全局串行化结构写，
且事务 TTL 5 分钟意味着一个失联的浏览器标签页可以阻塞整个 Zone 的写入 5 分钟。
网络协议只暴露 CAS 原语，事务留给服务端内部编排。

---

## 7. 安全模型

### 7.1 三种主体，一套凭证

| 主体 | 获取 cap 的方式 |
|---|---|
| 本 Zone 已登录用户（浏览器） | SSO 换取 session cap，作用域 = 该用户的可见子树 |
| Zone 内受信进程 / FUSE daemon | 设备身份签发的长期 cap |
| 外部访问者（分享链接） | `grant` 签发的 bearer / 定向 cap |

`uid/gid` **不进协议**。FUSE 适配层负责把本地 uid 翻译成一张 cap（这同时回答了
fuse_behavior.md 里"TODO: 定义 UID/GID/permissions 映射规则"）。跨 Zone 场景下 uid 本就无意义。

### 7.2 不变式必须在协议边界强制

**【现状】** Ops_v3 §0.4 记录："低层原语（create_dentry / set_inode / …）经 RPC 可直接调用，
会绕过 read_only 等高层检查"。NFSP 的对策是结构性的：**协议不暴露低层原语**。
`I1`（严格树）、`I3`（rev 单调）、`I4`（单写租约）、`read_only` 挂载链检查，
全部在 NFSP handler 与 fs_meta 的 dentry/inode 原语层双重强制。
不存在"高层 API"与"低层 RPC"两条路径。

### 7.3 数据面的鉴权

数据面必须是裸 URL（G3），但裸 URL 不能裸奔。三种模式：

| 模式 | 用途 | 形式 |
|---|---|---|
| session cookie | 同源的 File Browser | 常规 |
| cap in path | 分享链接、`<img src>` | `https://.../s/{token}/...` |
| 公开 | `public` 目录下的内容 | 无凭证 |

对 Frozen 内容有一个重要简化：**知道 ObjId 本身就接近一种能力**（256 bit 不可猜）。
但这不足以作为访问控制——它只保证不可枚举。`public` 之外的内容仍需凭证。
**【待决策】** 是否为"知道 ObjId 即可读"开一个 Zone 级开关（对内网/可信 CDN 场景有用）。

### 7.4 隐私维度（PRD 12.6）

`ai.external: deny` 这类维度是**协议必须强制而不是应用自觉**的：
策略检查点在 NFSP 的 `get_meta`/`set_meta`/`search` handler 上——
一个被标记 `index.semantic: deny` 的目录，其内容不会出现在 `search` 的 semantic 召回里，
也不接受 `ai.*` ns 的 `set_meta`。这样即使某个插件写得不好，也无法把隐私目录喂给外部模型。

---

## 8. 错误模型

结构化错误码，HTTP 状态码只作为粗粒度提示。

| code | HTTP | 语义 | 客户端应做 |
|---|---:|---|---|
| `NOT_FOUND` | 404 | 确实不存在 | 显示不存在 |
| `NEED_PULL` | 409 | 对象未在本地，附 `obj_id` + `hints[]` | **显示"离线/拉取中"第三态**，或自行从 hints 拉取 |
| `REV_MISMATCH` | 409 | 目录 rev CAS 失败 | 重新 `list` 后重试 |
| `TARGET_MISMATCH` | 409 | dentry target CAS 失败 | 同上 |
| `LEASE_CONFLICT` | 423 | 他人持有写租约 | 显示"文件正被编辑" + 持有者信息 |
| `SEQ_OUT_OF_WINDOW` | 409 | 重放窗口外 | 重新 resolve |
| `REFERRAL` | 307 | 目标在另一 realm/zone，附 endpoint | 换端点重试 |
| `PERMISSION_DENIED` | 403 | 权限不足 | 附 `required_op`，用于精确提示 |
| `POLICY_DENIED` | 403 | 被策略维度拒绝（如 `ai.process`） | 附 `dim` + `inherited_from`，用于可解释提示 |
| `UNSUPPORTED_EXT` | 400 | 未知 critical 扩展 | 降级 |
| `QUOTA_EXCEEDED` | 507 | 配额不足 | 提示 |

区分 `PERMISSION_DENIED` 与 `POLICY_DENIED` 是产品要求：前者是"你不能看"，
后者是"你能看，但这个目录被设置为不进 AI 管线"。UI 表达完全不同。

**【现状】** zone gateway 当前把"配额不足"落到 `500 internal_error`，尚未映射为 `507`。

---

## 9. 与 File Browser PRD 的逐项映射

| PRD 需求 | NFSP 支撑 | 缺口 |
|---|---|---|
| 9.1 基础浏览（列表/图标/多标签） | `list` + `want:[base,ident,thumb]` + 无状态游标 | — |
| 9.2 路径展示（含 public URL） | `want:["access"]` → `access_urls[]`（§3.6） | public URL 的生成规则需 Zone 侧配置 |
| 9.3 DFS 视图 + 设备视图 | `realm` + `referral`（§5.2） | 设备侧 `fs_export` 配置格式未定义 |
| 9.4 AI Topic | `View` + `canonical_path` + `provenance`（§3.5） | Topic 生成器与协议的接口未定义 |
| 9.5 搜索（多路召回 + 可解释） | `search` + `match_source` + `explain` + `sources[]`（§5.4） | 流式返回是否 MVP |
| 9.6 Preview / Meta / Story | `get_meta` + `MetaRecord.links` + `repr`（§3.4/§3.7） | Story 的 ns 白名单未定义 |
| 9.7 上传/下载/分享 | `probe` + tus + `commit_file` + `grant`（§5.3/§5.5） | 文件级 upload session 缺失 |
| 9.8 插件/触发器/管线权限 | `get_policy` + `triggers[]` + 多维 policy（§5.5） | 触发器注册表的读接口未定义 |
| 11.5 路径始终可信 | View / search 结果强制带 `canonical_path` | — |
| 12.x 权限与隐私 | cap + 多维 policy + 服务端过滤（§7） | — |

### 9.1 一次目录打开的完整请求（MVP 参考实现）

```jsonc
POST /nfs/v1/batch
{ "start": {"realm":"dfs","path":"/home/photos/2026"},
  "ops": [
    { "m":"stat", "want":["base","ident","access","policy"] },       // 面包屑 + 顶部路径栏 + 触发器角标
    { "m":"list", "want":["base","ident","frozen","thumb"],
      "args": {"limit":200, "order":"name"} }                        // 主内容区
  ] }
```
一次往返即可渲染完整界面。缩略图随后由浏览器按 `thumb.obj_id` 并发拉取（可走 CDN、可被 SW 缓存）。
右侧 Preview 面板在用户选中时才发 `get_meta`（懒加载，避免列表阶段拉全量 meta）。

---

## 10. PRD §14 待确认问题的协议侧答案

| # | PRD 问题 | 协议侧结论 |
|---|---|---|
| 1 | Topic 自动发现的触发机制与更新频率 | 协议不定义生成时机，只定义 `View.generation` + `stale` 标志 + `view_updated` 事件。生成由 trigger 平面驱动，前端只需订阅。 |
| 2 | 手动 Topic 与自动 Topic 冲突如何合并 | **用与文件系统同构的 overlay 解决**：自动结果 = Base Layer，`view_patch` 的 add/remove/pin = Upper Layer，`origin` 标为 `merged`。不需要新的冲突处理规则。 |
| 3 | AI 命中解释展示到什么程度 | 协议返回结构化 `explain{matcher, evidence, evidence_ref}`，展示程度是纯前端决策，可随时调整而不动协议。 |
| 4 | Story 的数据来源边界 | 由 **meta ns 白名单** 定义（`story.im` / `story.share` / `story.kb` …），每条 record 带 `source.app_id` 与 `visibility`。边界是配置项，不是硬编码。协议保证：`visibility` 低于当前主体权限的 record 在服务端就被过滤掉（PRD 12.4）。 |
| 5 | 设备视图暴露策略由谁配置 | 由**设备自己**通过 `fs_export` 声明导出哪些子树 + 需要何种 cap；Zone 侧只做聚合与鉴权，不能替设备决定。协议表达为 realm 注册 + referral。 |
| 6 | 管线权限最小粒度 | **目录级为主 + 文件级覆盖**，不做策略模板。`get_policy` 必须返回 `inherited_from` 使其可解释。 |
| 7 | public URL 是否支持多域名/版本化/临时链接 | 全部支持，且统一为 `access_urls[]` 的四种 `kind`：`public`（多条 = 多域名）、`pinned`（指向 ObjId，天然版本化不可变）、`signed`（cap token，带 `expires_at` 且可撤销）、`cyfs`（协议原生）。 |

---

## 11. PRD 未覆盖、但协议必须定义的细节

这一节是从协议视角反推出的产品缺口，建议回填进 PRD。

1. **离线/未拉取状态的 UI 表达。** `NEED_PULL` 是分布式 + 跨 Zone 场景的常态（尤其设备视图与
   跨 Zone 引用）。PRD 通篇假设文件"要么在要么不在"。需要定义第三态的图标、是否自动触发拉取、
   拉取进度如何展示。**这是 MVP 必须回答的问题。**

2. **超大目录。** PRD 未给出目录规模上限。协议已定义游标分页与稳定排序，但前端需要定义：
   虚拟滚动、排序切换时是否重新拉取（服务端排序意味着换 `order` 就是新游标）、
   "全选"在未加载完时的语义。

3. **缩略图的生命周期。** 谁生成、生成失败怎么显示、视频/PDF/文档是否有缩略图、
   加密或隐私目录是否生成缩略图（缩略图本身也是内容，会被 CDN 缓存——`ai.external: deny` 的目录
   是否允许生成缩略图？）。**这条有隐私影响，建议 PRD 明确。**

4. **上传中的可见性。** 见 §5.3。半成品是否进命名空间、多标签页之间是否互相可见、
   刷新页面后进度能否恢复。

5. **写冲突的用户表达。** `LEASE_CONFLICT` 意味着"别人正在写这个文件"。
   PRD 的协作章节只有一句"支持协作相关入口"。至少需要定义：显示谁在写、能否只读打开、能否抢占。

6. **设备离线 / 无权限的区分。** referral 的 `state` 三态，PRD 未提。

7. **搜索的权限与分页交互。** 服务端过滤后，`total` 与 `next_cursor` 的语义；
   语义召回降级时（`sources[].state = degraded`）是否提示用户。PRD 强调"掌控感"，
   建议提示。

8. **`public` 目录的语义边界。** 放进 `public` 是否等于立即公网可达？是否需要二次确认？
   移出 `public` 后已被 CDN 缓存的 `pinned` URL 如何处理（内容寻址意味着**无法真正撤回**）。
   **这是内容寻址系统的固有属性，产品必须显式告知用户。**

9. **Frozen 子树在 UI 上的表达。** 一个已发布的快照目录在 File Browser 里应该长什么样？
   是否显示为只读？是否显示"已发布 / 快照于某时间"？这是 NFSP 相对传统网盘的独特能力，
   PRD 完全没有涉及，但它恰恰是"掌控感"的重要来源。

---

## 12. 分期

### 12.1 前置修复（NFSP 落地前必须做）

- P0：修 `I6`，`NeedPull` 从 `named_store::get_dir_child` 一路透传（Ops_v3 已列为【现状】违反）。
- P0：不变式下沉到 fs_meta 的 dentry/inode 原语层（Ops_v3 §0.4【现状】）。
- P1：`objects/lookup` 批量化（`probe`），否则批量上传体验不可用。
- P1：`list` 从三段式有状态列举改为无状态游标。

### 12.2 NFSP v0 MVP 子集（对应 PRD 13.1）

`hello` / `bye` / `resolve` / `stat` / `list` / `batch` /
`probe` / `open_write` / `commit_file` / `mkdir` / `move` / `delete` /
`get_meta` / `search` / `open_view` / `grant` / `repr` / `watch`

不含：`get_tree`、`link_obj`、`publish_dir`、`view_patch`、`set_meta`、`set_policy`、
`referral`（设备视图可先只读硬编码）、跨 Zone。

### 12.3 v0.5

- `referral` + 设备视图 realm
- `get_policy` + `triggers[]`（PRD 9.8）
- `view_patch`（PRD 13.2.1 手动 Topic）
- `publish_dir` + Frozen 子树的完整 UI 表达
- 文件级 `upload_session`

### 12.4 v1

- QUIC / kRPC 绑定，`watch` 走 server stream
- 跨 Zone 写
- FUSE daemon 切换为 NFSP 客户端（当前是 in-process 直连）
- `search.stream`

---

## 13. 待决策清单（汇总）

| # | 决策点 | 建议 |
|---|---|---|
| D1 | v0 是否定义 QUIC 绑定 | 否，只做 HTTP；消息结构保持传输无关 |
| D2 | symlink 跟随语义统一 | 默认跟随（40 层），`follow_symlink:false` 为显式变体 |
| D3 | 是否向网络客户端暴露事务 | 否（会全局串行化写入 + 失联客户端可阻塞 5 分钟） |
| D4 | 上传中的文件是否进命名空间 | 否，占位项由客户端本地渲染 |
| D5 | 是否增加文件级 upload session | 是，v0.5 |
| D6 | `search` 是否流式 | 可选 feature `search.stream`，v1 |
| D7 | 管线权限粒度 | 目录级 + 文件级覆盖 |
| D8 | "知道 ObjId 即可读"是否开 Zone 级开关 | 待定，默认关 |
| D9 | 隐私目录是否生成缩略图 | 待定，倾向于跟随 `ai.process` 维度 |
| D10 | `list` 游标在并发变更下的语义 | 不重置，返回 `rev_changed` 由客户端决策 |
| D11 | watch 断线重连是否保证不丢事件 | 否，提供 `resync` 事件即可 |

---

## 附录 A：与 WebDAV 的三个深入对比

### A.1 "移动一个 10GB 的目录"

- **WebDAV**：`MOVE` 在同一 collection 内通常是元数据操作，但跨 collection、跨存储后端时
  规范允许服务器复制字节。客户端无法预知代价，也无法取消。属性（dead property）是否跟随由实现决定。
- **NFSP**：`move` 永远是 O(1) 的换绑定（Ops_v3 §3.5，按 inode_id 升序锁行 + 双 rev CAS）。
  meta 锚定在 ObjId 上，不存在"属性跟不跟随"的问题。AI 分析结果一次都不需要重算。

### A.2 "把一个目录分享给外部"

- **WebDAV**：需要在服务端建账号或开匿名访问，ACL 粒度到 collection，撤销靠改 ACL。
  接收方每次访问都要回源鉴权。无法表达"只读一个快照"。
- **NFSP**：`publish_dir` 生成不可变 `DirObjId`（O(1)），`grant` 签发限定该子树的 cap。
  接收方拿到的是一个**冻结的、可验证的、可从任意 CDN 加速的**子树；即使原目录后续被修改，
  分享出去的快照也不受影响——这是"分享一个版本"而不是"分享一个可变位置"，
  而后者正是网盘分享链接最常见的事故来源。
  代价：内容寻址意味着已分发的内容**无法真正撤回**（见 §11.8）。

### A.3 "一个 1000 张照片的相册首屏"

- **WebDAV**：`PROPFIND Depth:1` 返回 1000 个 `<D:response>` 的 XML（数百 KB～数 MB），
  无分页、无排序保证、无缩略图概念。缩略图需要 1000 次额外 GET，且无法去重。
- **NFSP**：一次 `batch`（stat + list limit=200）返回首屏，`want` 掩码控制负载；
  缩略图作为 ObjId 由浏览器并发拉取，同一张图在任何位置只下载一次，
  `immutable` 缓存头意味着第二次打开相册零网络请求。
  若该目录已 `publish_dir`，整个相册连元数据请求都不需要。

---

## 附录 B：设计来源

| 设计点 | 来源 |
|---|---|
| `batch` / 共享解析游标 | NFSv4.1 COMPOUND (RFC 5661) |
| session + seq exactly-once | NFSv4.1 session slot table |
| 属性 `want` 掩码 | NFSv4 attribute bitmap |
| `referral` / realm | NFSv4 `fs_locations` |
| 文件租约 + fencing seq | SMB3 lease / Ceph MDS caps |
| `lease_recall` / `dir_changed` push | SMB3 lease break / AFS callback |
| `probe` 批量查缺 | Bazel RE `FindMissingBlobs` |
| `get_tree` | Bazel RE `GetTree` / git protocol v2 |
| Base/Upper overlay、`publish_dir` | git tree + index / Plan 9 union mount |
| View 的 overlay 式手工修正 | 同上，复用同一心智模型 |
| ETag = ObjId、immutable 缓存 | 内容寻址通用实践（IPFS / Nix / ostree） |
| cap token | Tahoe-LAFS read-cap / macaroon / UCAN |
| mtree path proof、容器 key proof | 本仓库 [基于mtree的chunkid.md](./Reviews/基于mtree的chunkid.md)、[对象大容器需求草案.md](./Reviews/对象大容器需求草案.md) |
| `ext[{id, critical}]` | X.509 critical extension / QUIC transport parameter |
