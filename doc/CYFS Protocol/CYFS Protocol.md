# CYFS Protocol

> `CYFS` 或 `cyfs://` 都是在说协议；单独说 `cyfs` 时，通常指基于 CYFS Protocol 定义的标准对象实现的 DFS。

当前文档状态：0.91 修订草案，领先现有代码实现。本版在 0.9 基础上补充 **No-Push 的精确定义**、**跨 Zone 小对象投递 `dispatch`**、**语义路径目录的两种形态** 与 **PathObject 的 sub-host DID 公钥扩展**。

本次修订（在 0.91 基础上）将"强名字"与"端到端发布消费场景"整合进协议正文，并新增"数字内容的发行"与"内容生命周期"两章，明确 BNS 强名字作为 CYFS 数字内容网络入口与信任锚点的定位。其中 OwnerDocument / ZoneDocument 的字段、签名规则、密钥分级、撤销与轮换协议**仍归 BNS 文档规范**，本文档只引用其名称并定义它们对内容网络的契约。

## 设计哲学与边界

在进入任何CYFS的设计之前，先说明 CYFS 协议的**定位**和**边界**——也就是 CYFS 做什么、不做什么、以及为什么把某些"看似相关"的能力推到了协议之外。

很多在传统 P2P 或内容分发系统里被认为"理所当然应该包含"的模块——身份体系、支付结算、传输层连接、DHT、piece 级 P2P 交换、Tracker——在 CYFS 里都被有意识地移出了协议正文。它们要么由更下层的 tunnel 框架承担，要么由 W3C DID 等外部生态承担，要么由 CYFS 生态内的其他独立协议承担。这一节解释这些设计边界的由来。

### 定位：最小化的语义层协议

CYFS 是一个**内容网络协议（Content Network Protocol）**。它关心的是内容的**语义层**：

- 如何描述一个"已发布内容"（标准对象、`ObjectId`、不可变性约定）。
- 如何在互联网尺度上描述"内容如何在逻辑节点之间流转"。这里的逻辑节点指的是 **Zone**，而不是具体设备——一个人的手机和电脑在 CYFS 看来通常属于同一个 Zone。
- 如何围绕真实的用户消费行为，构造可验证的"事实存根"（签名后的下载证明、购买收据、动作链），作为上层经济模型的起点。

在工程上，CYFS 被设计成对现有 HTTP 协议的**最小必要扩展**。凡是可以复用既有生态（HTTP、JWT、W3C DID、HTTP 402、RFC 8785）的地方，就不在协议内部重新发明。这种"做减法"的取舍决定了下面列出的一系列"非目标"。

CYFS 的几条贯穿性原则：

- **最小化扩展 HTTP**：协议只定义 `cyfs://` 必须扩展的那部分，其余继续复用 HTTP 生态。
- **可信传输与中心化 CA 解绑**：让"HTTPS 证书的等价物"可以通过 DID 体系获得。
- **No-Push / Pull-first**：站在 `Content Network` 的视角，内容不会、也不需要通过 Push 来发布。已发布内容的扩散、收录与消费，最终都应落在收录者或消费者主动 Pull 上；接收方拥有是否下载、何时下载、从谁下载的最终控制权。跨 Zone 可以存在小型 `NamedObject` 的投递，用于消息、评论、通知、索引变更等语义事件，但这类投递不等价于内容 Push、附件上传或把公共数据强行写入对方 Zone。
- **源发现与内容校验解耦**：谁告诉我"某个源可能有数据"可以很宽松，这个源返回的数据是否可信必须严格。
- **从真实用户场景反推传播过程**，而不是从协议抽象反推用户行为——这是 CYFS 和很多传统 P2P 协议最根本的视角差异。

### 非目标 1：不定义身份系统，只消费 DID

CYFS 协议层**不定义**身份认证方案。所有身份标识 **MUST** 遵循 W3C DID 标准格式，形如 `did:<method>:<identifier>`。

CYFS 生态里存在一套更完整的去中心化身份扩展（BNS DID），以及基于 DID Document 的钱包地址绑定、原生转账、支付链路选择等机制。但这些机制是作为 DID 生态的扩展定义的，**不是 CYFS 协议本身的一部分**。任何符合 W3C DID 规范的身份系统原则上都可以接入 CYFS。

DID 到域名的反向映射（用于 URL hostname 中出现的 `$zone_id`）属于传输实现层细节，见相关文档。

**为什么这样设计**：身份系统是一个独立演进的领域，把它钉死在内容协议里会制造不必要的耦合。CYFS 协议只需要在"请求方是谁"和"收据绑定给谁"这两个地方消费 DID，不需要自己定义 DID 是什么。

但要注意：CYFS **不止"消费 DID"**——它结构性地依赖 DID 背后存在一个**强名字系统**（生态默认实现是 BNS）：名字唯一、稀缺、可解析、可追新。这四条假设是 CYFS 数字内容网络可信性的基石，纯 hash 寻址（IPFS）或弱名字（普通账号体系）都不能替代。下一章 [强名字：数字内容网络的入口](#强名字数字内容网络的入口) 展开为什么这个假设是 CYFS 数字内容网络的核心，并说明它如何塑造后面所有技术章节的设计。名字系统本身的协议（DID Document 字段、签名规则、密钥分级、撤销机制等）由 BNS 等独立协议规范，不在本文档讨论范围内。

### 非目标 2：不定义支付协议，只要求"购买证明"

CYFS 协议层只约定一件事：

> 当一个请求访问受保护内容时，**MUST** 提供一份可验证的购买证明（收据）；内容的发布方只在验证通过后返回内容。

协议**不规定**这份收据是如何得到的。它可以是：

- 基于 USDB（CYFS 生态默认的理想链）的内容购买合约产物；
- 基于 HTTP 402 标准流程得到的凭据；
- 通过 DID Document 扩展机制指向的任意第三方支付链路。

具体的支付/结算流程统一视为 HTTP 协议框架下的扩展，而不是 CYFS 的一部分。

**为什么这样设计**：结算方式的演化速度远快于内容协议本身。把支付链路的选择权交给 DID Document 和 HTTP 402，既保留了 CYFS 自带的 USDB 路径，又允许生态自由选择。

### 非目标 3：不定义传输层，只定义"通道之上的语义"

CYFS 把"两个节点之间如何建立一条可信链路"这件事完全下沉到 **cyfs-gateway 的 tunnel 框架**。tunnel 框架的核心理念是：

> **从面向 IP 地址的协议，转向面向身份（DID）的协议。**

一条 tunnel 通过 `open_tunnel(target_did)` 打开，底层可以跑在 TCP、TLS、rTCP、rHTTP 等任意 stream 通道上。CYFS 协议本身只假定"一条已建立的 stream 通道"存在，然后定义在这条通道上：如何请求一个 Chunk、HTTP 头怎么写、返回的 Chunk 如何与 ChunkId 校验——只定义这些。

这里有一个**关键的设计意图**：让"可信传输"这件事与**中心化 CA 解绑**。换句话说，让 HTTPS 证书的等价物可以通过 DID 体系获得。CYFS 生态自带一套基于区块链 DNS 的方案（BNS），但如果未来 W3C 给出一个接受度更广的方案，CYFS 也会选择拥抱——因为传输层的可信性来源对 CYFS 协议本身是**可替换**的。

**为什么这样设计**：CYFS 的核心承诺是"没有 CA 也能可信发布内容"，而这个承诺的实现方式应该是可插拔的，而不是写死在协议里。

### 非目标 4：不使用 DHT

CYFS 协议里**没有任何 DHT 相关内容**。这是一个有意识的选择："DHT的历史使命已经基本完成，大部分情况下智能合约都是更好的方案"

因此，过去在 P2P 网络里必须依赖 DHT 解决的"真正去中心化的元数据读取"问题，在 CYFS 里统一交由智能合约解决。BNS、收录者目录、Zone 的可信公钥绑定——这些过去可能放在 DHT 上的东西，现在都放在合约状态里。

### 非目标 5：不定义 piece 级 P2P 交换协议

传统 BitTorrent / BitSwap 类协议把一个文件切成很小的 piece（几十 KB 到 MB），并在这些 piece 之间设计复杂的 tit-for-tat 交换规则。CYFS 不这样做：

- `FileObject -> ChunkList -> Chunk` 体系已经在**语义层**提供了多源下载的全部基础：每个 Chunk 都有独立的可验证 `ChunkId`，可以从任意源拉取。
- 具体"如何高效拉一个 Chunk"——从哪些源并发拉、怎么换源、怎么做流控——属于传输层（tunnel）和调度层的职责，CYFS 协议不规范它。
- 在当前主流的千兆级家庭宽带和稳定骨干网场景下，粗粒度 Chunk + HTTP Range + 多源并发的组合，在大多数情况下已经能够达到或超过传统 piece 级 P2P 的吞吐，同时大幅降低了协议复杂度和实现成本。对于弱连接、高丢包等特殊网络场景，更细粒度的传输优化可以在 tunnel 层独立演进，不需要 CYFS 协议介入。

CYFS 过去曾经设计过基于喷泉码、基于 UDP piece 交换的更细粒度协议；今天选择把这些内容从协议正文中移除，是对"最小化语义层"定位的一次收敛。

一句话：**CYFS 规范的是"一个大文件如何被切成可验证的 Chunk"，不规范"这些 Chunk 具体怎么被拉回来"。**

### 非目标 6：不定义传统 Tracker 协议

传统 Tracker 协议的本质是一个"谁拥有哪些数据"的目录服务。CYFS 没有这样的独立协议，原因是：

CYFS 倾向于在协议里定义**逻辑广播 / 逻辑传播**，而不是**协议层广播**。"逻辑传播"的意思是：协议里出现的每一次"内容扩散"，都必须对应一次真实的用户行为。例如：

- 用户 A 是某个内容的**收录者（Curator）**，下载者在访问该内容时，至少能拿到"原作者 + 收录者"这两个源的线索。
- 收录者为了评估内容热度，会要求下载者在消费结束时提交一份**带签名的下载证明 / 安装证明 / 消费证明**，并给予一定经济激励。
- 这些带签名的消费证明本身，就构成了"谁最近获取过这个内容"的**高价值源信息**——比传统 Tracker 里匿名上报的"IP 列表"更可信、更可溯源。

配合 `cyfs-cascades`（动作链）机制——每个 action 都有 `baseOn`，指向它是"基于哪个上游动作"构造的——最终得到的不只是一个源列表，而是一棵**可溯源的传播树**。

因此 CYFS 没有独立的 Tracker 协议，但整个体系在功能上覆盖了 Tracker 的用途，并且在激励对齐和可信度上更优。

**为什么这样设计**：我们希望从**真实用户场景**反推内容网络的传播过程，而不是从协议抽象反推用户行为。这是一个自下而上的建模思路。

### 小结：CYFS 与生态伙伴的边界

下表把上述边界关系整理成一个速查表：

| 能力 | CYFS 协议本身是否定义 | 由谁承担 |
| --- | --- | --- |
| 身份标识 | 否 | W3C DID（生态内为 BNS DID 扩展） |
| 支付与结算 | 否 | USDB 合约 / HTTP 402 / 任意 DID Document 扩展链路 |
| 节点间连接建立 | 否 | cyfs-gateway tunnel 框架 |
| 传输层可信性 | 否 | tunnel 层（基于 DID / BNS / 未来的 W3C 方案） |
| 元数据全局状态 | 否 | 智能合约（取代 DHT） |
| piece 级 P2P 交换 | 否 | tunnel 层或应用调度层 |
| Tracker | 否 | 基于动作链的逻辑传播模型 + 收录者 |
| 内容寻址与不可变对象 | **是** | CYFS |
| 语义 URL 与 `PathObject` | **是** | CYFS |
| 跨 Zone 小对象投递 `dispatch` | **是** | CYFS 语义资源层 |
| 语义路径目录的强一致 / 尽力而为形态 | **是** | CYFS 语义资源层 |
| 公共内容的 Push 发布 / 跨 Zone 附件上传 | 否 | 收录者与消费者主动 Pull；Zone 内数据搬运由本地实现承担 |
| `FileObject` / `ChunkList` / `SameAs` | **是** | CYFS |
| `inner_path` 与可验证字段寻址 | **是** | CYFS |
| 购买证明的验证语义 | **是** | CYFS |

## 强名字：数字内容网络的入口

前一节描述了 CYFS 不做什么，把身份、支付、传输都推给了生态伙伴。这一节回到 CYFS 自己要做什么的核心：**用强名字组织起一张可信的数字内容网络**。

CYFS 把 BNS 这类**强名字系统**作为整个内容网络的入口与信任锚点，而不是一个可选的扩展。这是 CYFS 与传统 P2P / 内容寻址协议（IPFS、BitTorrent）最大的区别——后者的可信根是裸 hash，前者的可信根是用户能识别的名字。

> 名字系统本身的协议（如何注册、如何解析、DID Document 怎么签、密钥怎么轮换）由 BNS 文档规范。本章只讲"名字"在数字内容网络里扮演什么角色，以及 CYFS 协议对底层名字系统的最小依赖。

### 为什么内容网络需要强名字

传统的"纯 hash 内容寻址"在协议层是优雅的，但在用户层是失败的：

- `bafybeibwzifwthlvyov3v6h7s5fhix7nnffcpu5g35rkzyuk2vwzpdqfhq` —— 一个 IPFS CID，用户无法识别、无法记忆、无法在不同语境下复用。
- `0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1` —— 区块链地址同理。

研究表明，用户面对长串字符时只看前 4 后 4，中间整体当作不透明黑块处理。这意味着任何**依赖"用户每次都仔细比对"的安全机制等于没有这个安全机制**。攻击者只要 grind 出前后缀一致的伪造地址就能钓鱼成功——而这种 grind 在今天的算力下是廉价的。

CYFS 选择让"识别"代替"比对"。当用户看到 `BCOS` 时，大脑 0.1 秒就能识别；当用户看到一个 32 字节哈希时，大脑直接放弃，转入"我相信前面流程检查过了"模式。一个好名字的安全价值，超过一切"用户教育"——这是被工业界长期低估的工程真理。

强名字之所以能承担这个角色，是因为它满足三件事：

1. **稀缺**：名字数量有限，注册和持有都要付出真实经济成本。零成本批量抢注 typosquatting 名字（`buckyos-1`、`buckyos.io`、`buckyos_org` 等）的攻击经济学被打掉。
2. **唯一**：在同一时刻一个名字只对应一个所有者，不存在"同一名字不同主"的歧义。
3. **可识别**：名字本身是人类可读的、能在日常语境中复用的（域名、用户名、品牌名性质）。

满足这三件事的名字系统才能承担"用户身份信任锚点"的职能。CYFS 协议本身不规定如何实现这三件事，但**假定底层名字系统已经实现了它们**。如果换一个不满足这些性质的名字系统（如纯哈希派生的 `did:key`），CYFS 协议可以正常运行，但失去了"用户能用名字识别发布者"这个最重要的安全保证。

### 名字、Zone、内容三者的关系

CYFS 网络里所有可寻址的东西都挂在某个**名字**之下。一个典型的内容访问 URL 是：

```text
cyfs://$zone_id/<sem_path>/...
```

其中 `$zone_id` 既可以是一个 BNS 强名字（如 `did:bns:zhicong`），也可以是 `did:web:zhicong.me` 之类的 Web DID，或者是一个 base32 编码的对象 ID。在面向用户的场景里，强名字是默认形式。

名字背后挂的是一个 **Zone**（一个发布者的逻辑节点集合，可能由若干设备组成）。Zone 在自己的命名空间里组织语义路径，每条路径指向一个 `NamedObject`。这条链路是：

```text
强名字 → Zone → 语义路径 → NamedObject → ChunkList → Chunk
```

- 名字层负责"我是谁、我可信吗"。
- Zone 层负责"在我的命名空间里这条路径当前指向哪个对象"。
- 对象层负责"这一段字节是不是真的"。

CYFS 协议正文（NDN 章节及其后）规定的是 Zone 层和对象层；名字层由 BNS 等独立协议规定，但 CYFS 假定它存在并提供下面四条契约：

1. **唯一性**：在同一时刻一个名字只对应一个所有者。
2. **稀缺性**：名字注册要付出真实经济成本（链上手续费、续期费），让 typosquatting 不经济。
3. **可解析**：任何客户端都能在不依赖中心化服务的情况下，把一个名字解析到一份权威的元数据文档（OwnerDocument、ZoneDocument 等）。具体的解析协议由 BNS 规范，CYFS 客户端通过 `resolve(name)` 接口消费它。
4. **可追新**：客户端能拿到该名字当前最新状态，旧版本不会被恶意冒充为新版本。这是单调性原则——信任只能向前推进，不能回退。

凡是依赖这四条契约的内容网络行为——签名验证、收录证明、版本判断、撤销响应——都默认建立在它们之上。如果底层名字系统不能保证这四条，CYFS 内容网络的可信性也无从谈起。

### `(name, type)`：同一个名字承载多种内容视图

CYFS 协议本身只关心**内容**，但它直接消费一个被反复使用的名字系统设计模式：**`(name, type)` 寻址**。

同一个名字 `BCOS`，在不同的语境下对应不同种类的内容：

| 调用语境 | 隐含的 type | 解析得到 |
| --- | --- | --- |
| 浏览器地址栏输入 `BCOS` | `Web` | 网站入口 / Zone Document |
| 应用商店搜索 `BCOS` | `App` | App Document（含版本、Pkg 引用） |
| 钱包发起转账给 `BCOS` | `Identity` | Owner Document（含收款地址） |
| AI 平台搜索 `BCOS` 的代理 | `Agent` | Agent Document |

所有这些视图的所有者是同一个名字所有者。用户买名字一次，所有这些视图自动归属。这种设计在工程上类似于 IP 网络里的 `(ip, port)` 或 HTTP 的 Content Negotiation——同一资源的不同视角，所有权统一，对外形态多样。

从 CYFS 数字内容网络的角度看，这个机制带来三件好事：

1. **用户的识别成本是常数**：无论是装 App、访问网站还是给 BCOS 转账，用户面对的是同一个名字 `BCOS`。任何一种用法被钓鱼，攻击者都要在名字层伪造（极困难），而不是在某个独立的标识符（域名、UUID、账号 ID）上伪造（容易）。
2. **新内容形态的零成本接入**：将来出现 VR 空间、AI Agent、机器人控制等新形态时，BCOS 这个名字自动获得在新形态下的命名权，不需要重新抢注。这对老品牌是巨大优势，也减少了生态扩张时的混乱。
3. **跨内容类型的信用复用**：BCOS 作为发布者的历史信用同时为它的网站、App、Agent 背书。任何一个维度的信任建立，自动加强其他维度的信任——是相乘效应而不是相加效应。

CYFS 协议本身规定的是**内容相关的 type**（Web、App、Article、Model、Dataset 等）的对外协议；其它 type 的协议由对应领域自行规定。但所有 type 都共享同一个名字根、同一份 owner 信用、同一套撤销机制。

> Type 注册机制、name 到 type 集合的映射规则、不同 type 的合法签发者规则，归 BNS 文档管理，本协议不展开。

## 端到端场景：从一个名字到一段可信内容

为了让后面的技术章节有一个连贯的叙事背景，先用一个完整场景串起整个内容网络。这个场景里特意不出现 HTTPS 也不出现中心化 CA——CYFS 数字内容网络的所有可信性都建立在名字系统与内容签名之上。

### 场景描述

`zhicong` 是个独立创作者。他想发布一篇博客文章 `/blog/2026/05/some-thoughts.md`，希望任何 CYFS 客户端都能在不依赖任何中心化平台、不申请 HTTPS 证书的前提下，可信地下载这篇文章。

读者 `alice` 在某个收录站看到了这篇文章的链接，从来没访问过 `zhicong` 的站点。她要做的事情只有一件：点击链接，得到这篇文章。她身后的客户端要做的事情有四件：

1. 找到 `zhicong` 这个名字现在挂在哪个 Zone 上。
2. 从该 Zone 拿到"这条语义路径目前指向哪个 ObjectId"的可验证声明。
3. 把对应的 Chunk 下载回来。
4. 验证下载到的字节就是声称的那个对象。

### 发布者侧的准备

`zhicong` 在发布之前的一次性准备：

1. **注册名字**：在 BNS 上注册 `did:bns:zhicong`，合约调用由 Owner 钱包私钥完成。这一步是链上交易，需要真实手续费——这笔钱不是协议的成本，而是协议安全的一部分（让攻击者无法零成本批量抢注相似名字）。
2. **声明默认 Zone**：在 OwnerDocument 中声明默认 Zone 是 `did:web:zhicong.me`。
3. **部署 Zone**：在一台 VPS 上部署 CYFS Gateway。Zone 内部生成自己的签名密钥对，并由 Owner 签发一份 ZoneDocument 声明这把 Zone Key 是当前 Zone 的有效签名密钥；同时声明 Gateway Device 列表（外部可达的 device，本协议中通常只有 Gateway 一类对外 device）。
4. **发布内容**：把 `some-thoughts.md` 文件作为一个 `FileObject` 发布到 Zone 的 `/blog/2026/05/some-thoughts.md` 语义路径下。Zone Key 同时签发一份 `PathObject`（前文 NDN 章节会展开其格式），把这条语义路径绑定到 `FileObject` 的 `ObjectId`。

> OwnerDocument、ZoneDocument 的具体字段、签名算法、密钥分级与轮换机制都由 BNS 协议规范，本节只引用其名称与功能。

到这一步，发布者侧的完整信任链就建好了：

```text
BNS 链 (信任根)
  ↓ 提供
OwnerDocument (含 Owner 公钥)
  ↓ 授权
ZoneDocument (含 Zone 公钥, Gateway 列表)
  ↓ 签发
PathObject (语义路径 → ObjectId)
  ↓ 绑定
FileObject → ChunkList → Chunk (内容自验证)
```

每一层都是上一层用密钥签出来的，每一层的密钥都被上一层的文档授权。最终的 Chunk 数据用自身的 hash 自验证，不依赖任何额外的传输层授权。

这种"密钥分级"的好处对应了实际的安全需求：

- **Owner Key**：人/组织持有，最稳定，最少变动，被攻陷是真正的灾难。
- **Zone Key**：部署单元持有，中等稳定，可能换 VPS。被攻陷只需要 Owner 重发 ZoneDocument 即可恢复。
- **Gateway / Device Key**：具体硬件持有，最易变动。被攻陷只需要 Zone 重发 DeviceDocument 即可恢复。

密钥的轮换频率从下到上递减——这和实际的攻防场景完全匹配。

### 消费者侧的访问

`alice` 点开链接 `cyfs://did:bns:zhicong/blog/2026/05/some-thoughts.md`。她的客户端做以下事情：

1. **解析名字**：调用 `resolve("did:bns:zhicong")` 拿到 OwnerDocument。这一步如何工作由 BNS 协议规定（轻节点同步区块头 + Merkle proof 是默认推荐路径）。CYFS 客户端只需要相信底层名字系统返回的是当前最新的、未被篡改的 OwnerDocument。
2. **找到 Zone**：从 OwnerDocument 读到默认 Zone 是 `did:web:zhicong.me`，向这个 Zone 的 Gateway 发起 HTTP 请求。
3. **拿到 ZoneDocument**：客户端从 Gateway（通过 `.well-known` 端点或随首次响应捎带）拿到 ZoneDocument JWT，用 OwnerDocument 里声明的 Owner 公钥验证它的签名。验证通过后，客户端就掌握了当前有效的 Zone 公钥集合。
4. **请求内容**：客户端发起 `GET cyfs://did:web:zhicong.me/blog/2026/05/some-thoughts.md`。Gateway 返回：
   - `cyfs-path-obj`：用 Zone Key 签发的 PathObject JWT，把这条语义路径绑定到 `FileObject` 的 `ObjectId`。
   - `cyfs-obj-id`：`FileObject` 的 `ObjectId`。
   - HTTP body：`FileObject` 的 canonical JSON（或者通过 `inner_path` 直接展开到 `content` 字段拿到 Chunk）。
5. **验证链路**：客户端用 ZoneDocument 里的 Zone 公钥验 PathObject 的签名；用 PathObject 里的 `target` 与响应里的 `cyfs-obj-id` 比对；用 `cyfs-obj-id` 与 body 重新计算的 hash 比对。任何一步不一致，请求失败。
6. **缓存**：所有解析结果（OwnerDocument、ZoneDocument、PathObject）都按各自的版本号 / 过期时间缓存到本地。下次访问 `did:bns:zhicong` 下的任何内容都能复用这些缓存。客户端的缓存策略遵循单调性原则——只接受比缓存更新的版本，拒绝比缓存更旧的版本，避免攻击者用旧文档让客户端"降级"。

整个过程没有 HTTPS、没有 CA、没有任何中心化平台。Gateway 可以是不可信的（任何中间镜像也可以是不可信的），因为最终 alice 拿到的字节是被层层签名 + hash 链验证过的。

### 这个场景说明了什么

这个端到端场景定义了 CYFS 数字内容网络的最小可信发布闭环。后面的章节都是在这个闭环上扩展更多能力：

- **Named Data Network 基本概念** 章节展开 `ChunkList`、`SameAs`、`inner_path` 这些对象层细节，让"内容自验证"在大文件、引用复杂的对象图上也成立。
- **CYFS 网络的传输加速** 章节展开多源调度，让"任何镜像都不可信"在工程上能跑出可接受的下载性能。
- **内容购买与认证** 章节展开经济层，让发布者能从这个网络里直接收到钱。
- **内容的发布、No-Push 与 Zone 内上传** 章节展开 Zone 接收语义事件的机制（dispatch、目录形态等）。
- **数字内容的发行：发布、收录与消费** 章节把单一发布者扩展到"发布权与分发权解耦"的多方生态，让内容能像 App 一样被多方独立背书与分发。
- **内容生命周期：版本、撤销与信任卫生** 章节展开撤销与版本管理，让被攻陷的密钥、被发现有问题的内容版本能在网络里及时失效。

每一章都加复杂度，但每一章加的复杂度都是在解决这个基础场景里被推迟的问题，而不是在堆抽象。读者可以始终把后续章节理解成"这个端到端场景在某个维度上的展开"。

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

#### sub-host DID 的 PathObject 验证

Zone 级根签名密钥安全等级高，不适合频繁用来签发路径到对象的绑定关系。为了支持更细粒度的日常签名，`cyfs://` URL 的 hostname 部分可以使用 **sub-host DID**。

当 hostname 是一个 sub-host DID 时：

1. 客户端通过标准 DID 解析流程获取该 sub-host DID 的 DID Document。
2. DID Document 中允许用于 CYFS path binding 的公钥，自动加入该 hostname scope 下的 `PathObject` 可信公钥集。
3. 客户端使用该公钥集验证 `cyfs-path-obj` 中的 JWT 签名。
4. 这不会修改主 Zone 的可信公钥列表，也不要求主 Zone 根密钥参与日常 path binding 签发。

这相当于在 Zone 内建立轻量化的密钥分级体系：根 Zone DID 持有最高权限的密钥，主要用于建立或撤销 sub-host DID 的授权；sub-host DID 持有自己 scope 内的日常签名密钥。

本协议不引入 path 内的显式 DID 公钥前缀，例如 `/-ndn-did-/<did>/<rest>`。这类 magic prefix 与 sub-host DID 解决的是同一个问题，协议层只保留 hostname / DID Document 这一条路线。

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

这里的认证描述的是 `cyfs://` 里的“君子协定”:协议只保证**诚实节点**会按约束传播和访问，并不在密码学上阻止恶意节点复制与转发。换句话说，这是一种**弱强制 + 基于声誉**的约束：配合 `cyfs-cascades`、`Reference` 等上下文信号，正常节点会选择遵守；恶意节点虽然技术上可以绕过，但也会因此失去后续收益分配、Curator 信用背书等上层好处。而且基于CYFS构建的Content Network的多源特性，一个节点拒绝返回数据给用户，通常不能100%保证用户无法得到数据。这个君子协定设计的目的是希望被大部分诚实节点遵守，提高”不道德节点“的作恶成本。严格的身份认证通常是写相关的，走的是 Zone 内部的本地实现，这里不展开讨论。落到 `cyfs://` 协议上，通常已经是跨 Zone 访问，此时至少偏向“半公开”场景，因为数据一旦到了公网，就没有 100% 可靠的方法阻止其继续传播。

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



## 内容的发布、No-Push 与 Zone 内上传

CYFS 本身没有“上传公共数据”的统一协议设计，因为 CYFS 的定位是在互联网上高效可靠地获取公共数据，实现 `Content Network`。

本章重新精确定义 `No-Push / Pull-first`，并在此基础上补充跨 Zone 小对象投递 `dispatch` 与语义路径目录的两种形态。

### No-Push 的精确定义

**No-Push 约束的是内容发布与消费，不是所有请求体写入。**

一份内容能被某个消费者拿到，是因为该消费者，或代消费者工作的收录者，主动决定要拿，而不是因为发布者把它推过来。这是数据主权的体现：我的设备上存什么、向谁可见、何时可见，由我决定。

因此，`No-Push / Pull-first` 主要约束的是 **NamedData / Chunk / FileObject content / 大对象 / 附件 / 已发布公共内容** 的传播方式。它不禁止“把一个 `NamedObject` 投递到 target Zone 控制的逻辑接收点”这类语义事件。投递是否接受、是否对外可见、何时可见，完全由 target Zone 决定。

### Zone 内上传

在单个 Zone 内部，很多产品逻辑仍然会出现“传统上传行为”。例如用户在手机上选择一张照片，用 MessageHub 给朋友发送消息，在消息真正发出前，需要先把照片从手机搬到 OOD 上。

这是 **Zone 内** 的发布准备流程，不违反 `No-Push / Pull-first` 约束。Zone 内的数据流转、复制、缓存和设备协同不是 `cyfs://` 的跨 Zone 内容发布模型。

### Zone 间内容传播

Zone 间 **没有上传公共内容** 的概念。如果 `ZoneA` 给 `ZoneB` 发送一个带附件的 `MessageObject`，并不会有所谓“附件上传”的逻辑。

其核心流程如下：`ZoneA` 只是把一个较小的消息对象、对象引用或语义事件交给 `ZoneB`。真正的数据流动仍然发生在 `ZoneB` 后续主动发起的 Pull 中。

也就是说，在跨 Zone 场景里要区分三件事：

1. **语义事件到达**：例如消息、评论、通知、索引更新等小型 `NamedObject` 到达目标处理逻辑。
2. **引用传播**：这些小对象里可以携带 `ObjectId`、`FileObject`、`ChunkList`、语义 URL 或其它可 Pull 的引用。
3. **内容获取**：附件、引用对象、页面资源、Chunk、FileObject content 等较大的 `NamedData` 永远由接收方按需 Pull。

只有在这些业务判断完成之后，接收方才会对 `MessageObject.ref_objs[i]` 或其它引用执行标准的 `open_reader_by_url` / `get_object_by_url`。一旦开始 Pull，后续流程就重新回到通用 CYFS 下载语义：验证 `PathObject`、验证 `NamedObject`、验证 `ChunkId`，必要时再走 `ChunkList`、`SameAs` 和多源调度。

### 跨 Zone 小对象投递：`dispatch`

CYFS 定义一个标准写 verb：

```text
PUT cyfs://$target_zone_id/<sem_path>
Content-Type: application/cyfs-named-object+json
<body: NamedObject canonical JSON>
```

语义是：

> 我创建了一个 `NamedObject`，希望把它投递到你 NDN 路径 `<sem_path>` 对应的逻辑接收点。

使用 `PUT` 而不是 `POST`，是因为 CYFS 对象是内容寻址、immutable 的。同一对象重复投递应该收敛到同一结果，这正是 `PUT` 的幂等语义。

协议只定义下面几件事：

1. body **MUST** 是 canonical JSON 形式的 `NamedObject`。
2. body **MUST NOT** 直接携带 `NamedData` 或 Chunk 数据。如果 `NamedObject` 内部引用了大附件、`FileObject` 或 `ChunkList`，target Zone **MUST** 在自己后续主动 Pull。
3. 如果 `<sem_path>` 上没有挂处理逻辑，target Zone **MUST** 返回明确失败，建议使用 `404` 并附带 `cyfs-dispatch-error: no-handler`。
4. 是否接受、如何落地、ACL 如何判定、最终写到哪里，CYFS 协议不规定，由 target Zone 自行决定。

#### ACL 与请求上下文

ACL 不是 `dispatch` 的协议级问题。target Zone 的后端 service 可以使用 CYFS 已经定义的通用请求 header 自行判定：

- `cyfs-original-user`：请求发起者 DID。
- `cyfs-cascades`：上游动作链。
- `cyfs-proofs`：各类行为证明。
- `cyfs-access-code`：纯访问代码。

具体策略，例如白名单、群成员资格、staking、声誉、邀请码、组合策略等，由 target Zone 的 service 决定。协议只定义请求语法，不定义权限模型。

#### 路径结构推荐

语义路径的对外组织建议遵循：

```text
<entity>[/<inner_logical_path>]
```

其中 `<entity>` 是接收方标识，可以是 DID、`ObjectId` 或其它可被生态理解的实体标识；`<inner_logical_path>` 表达“接收方的哪一部分”，例如 `inbox`、`comments`、`notifications`。

示例：

```text
PUT cyfs://$ood/did:bns:my-group/inbox
PUT cyfs://$ood/did:bns:my-group/sub/eng/inbox
PUT cyfs://$ood/cyobj:article-xxx/comments
PUT cyfs://$ood/did:bns:bob/inbox
```

这是 RESTful 推荐，不是协议硬约束。建议生态遵循这个隐喻，是因为它让读和写的路径形态同构：`GET .../inbox` 和 `PUT .../inbox` 操作的是同一个逻辑接收点。

子群可以表达为父群 DID 下的命名子路径，而不一定拥有独立 DID。只有当子群有独立对外身份需求，例如独立支付绑定、独立被非父群成员订阅时，才需要使用独立 DID。这个判据由应用层自行决定。

#### `/` 与 `/@/` 的分工

`/` 是语义路径分隔符，用于表达 host Zone 的资源逻辑组织；`/@/` 是对象 `inner_path` 分隔符，只用于在 `NamedObject` 内部做字段寻址。

完整 URL 结构是：

```text
cyfs://$zoneid/<sem_path>(/@/<inner_path_step>)*
```

`dispatch` 遵循同样规则，但 **不允许带 `inner_path` 段**：

```text
PUT cyfs://$zoneid/<sem_path>          # 合法
PUT cyfs://$zoneid/<sem_path>/@/<...>  # 非法
```

理由是 `NamedObject` 是 immutable 的，对它内部字段做“写”在语义上不存在；要更新只能投递一个新对象。

### 语义路径目录的两种形态

语义路径作为目录使用时，集合成员关系可以有不同可信级别。CYFS 明确支持两种合法形态。

#### 为什么需要两种形态

如果所有目录式语义路径都强制绑定到一个强一致容器对象，那么每写入一条消息都需要更新目录根对象。在大群 inbox、评论、通知这类写多读多路径上，这会把写入成本放大到不可接受。

因此，协议必须允许 host Zone 根据场景选择目录可信级别：有些目录需要强一致、可封版、可多源验证；有些目录只需要低成本返回当前可见的 child 列表。

#### 形态 A：强一致容器路径

语义路径绑定到一个固定的 container `ObjectId`，例如 `clist`、`cymap-mtp` 等。这个形态已经由前文 `container_id/@/key` 机制支持。

特征：

- 整个目录的 `ObjectId` 是确定的，发布即封版。
- `cyfs-path-obj` 把 path 绑定到该 container `ObjectId`，可密码学验证。
- 多源拉取友好：同一个 container `ObjectId` 加 Range 可以从多个 Zone 并发拉取。
- 写代价高：每次写入都要更新 container `ObjectId`。

适用场景是版本敏感、需要精确多源校验、写少读多的目录，例如已封版的消息归档、版本化文档目录、被收录者维护的索引。

#### 形态 B：尽力而为公共目录

语义路径不绑定到任何固定的 container `ObjectId`。它是一个逻辑聚合点，host Zone 在响应时根据自己的实现聚合并返回当前 child 列表。

特征：

- 不承诺两次请求看到的列表完全一致。
- 写入便宜，`dispatch` 一条新对象进来不需要重算父 `ObjectId`。
- 协议只保证返回的每个 child item 自身有 `ObjectId`，可被独立校验。

适用场景是高吞吐写入的 inbox、comments、notifications、log-tail。

形态 B **不附带** `cyfs-path-obj`，因为没有固定 container `ObjectId` 可以绑定。客户端必须明确知道：拿到的列表是 host Zone 当前时刻的 best-effort 视图，不是密码学上不可篡改的承诺。**集合成员关系**是 best-effort 的；**集合成员本身**仍然是 verifiable 的。

实现层建议：客户端 SDK 在使用形态 B 时 **SHOULD** 暴露明确 API 区分，例如 `list_loose()` 与 `list_verifiable()`，避免调用者无意识地拿到 best-effort 数据。

#### 形态 B 请求语法

通过 query 参数显式选择形态：

```text
GET cyfs://$ood/did:bns:my-group/inbox?list=loose
GET cyfs://$ood/did:bns:my-group/inbox?list=loose&after=<ts>&limit=100
```

参数：

- `list=loose`：明确请求形态 B。无 query 参数时，形态由 host 决定。
- `before` / `after` / `limit`：标准翻页参数。
- `before` / `after` 接受 `ObjectId` 或时间戳，由 host 在响应中声明自己使用了哪种锚点。

形态 B 的响应 body **仅返回 child 的 `ObjectId` 数组**：

```json
[
  "cyobj:msg-aaa...",
  "cyobj:msg-bbb...",
  "cyobj:msg-ccc..."
]
```

客户端拿到 `ObjectId` 列表后，比对本地已持有集合，只对缺失的 `ObjectId` 再发起标准 `get_object_by_url` 拉取。

协议不在形态 B 中支持“内联完整对象”的返回格式。应用层如果真的需要批量获取，可以单独发起 batch 请求作为应用扩展，不污染协议层。

#### 形态 B 硬约束

- 单次响应 **MUST** 不超过 **4096** 个 child。这个阈值与前文大容器阈值对齐，便于实现复用。
- 客户端要看更多 child，必须使用翻页参数。
- host **MUST** 在响应 header 中明确声明本次响应是否截断，建议使用 `cyfs-list-truncated: true|false`。

形态选择由 host Zone 决策。客户端通过 `?list=` 参数声明期望形态；host 不支持时可以返回 `415 Unsupported Media Type`，也可以按自己默认形态降级，并通过响应 header 声明实际使用的形态，例如 `cyfs-list-mode: loose|strict`。

| 形态 | path 是否绑定固定 ObjectId | cyfs-path-obj | 写代价 | 多源拉取 | 适用场景 |
| --- | --- | --- | --- | --- | --- |
| A 强一致容器 | 是 | 返回 | 高 | 完整支持 | 封版归档、版本化目录 |
| B 尽力而为目录 | 否 | 不返回 | 低 | 仅成员级 | 高吞吐 inbox / comments |

### MessageHub 示例

Alice 给群发一条带图消息：

```text
PUT cyfs://$group_ood/did:bns:my-group/inbox
Content-Type: application/cyfs-named-object+json
cyfs-original-user: did:bns:alice
cyfs-cascades: [...]
<body: MessageObject canonical JSON>
```

`MessageObject` 是 KB 级对象，附件字段只引用 `FileObject` 的 `ObjectId`。group OOD 接受投递后，不会因为 dispatch 自动下载附件；附件由订阅者按需 Pull。

Bob 拉群消息：

```text
GET cyfs://$group_ood/did:bns:my-group/inbox?list=loose&after=<last_seen_ts>&limit=4096
```

返回 `ObjectId` 数组。Bob 比对本地已持有集合，仅对缺失项调用标准 `get_object_by_url` 拉对应 `MessageObject`。看到附件引用后，按需 `open_reader_by_url` 拉图片 Chunk。

子群和文章评论使用同一范式：

```text
PUT cyfs://$group_ood/did:bns:my-group/sub/eng/inbox
GET cyfs://$group_ood/did:bns:my-group/sub/eng/inbox?list=loose&...

PUT cyfs://$author_ood/cyobj:article-xxx/comments
GET cyfs://$author_ood/cyobj:article-xxx/comments?list=loose&...
```

## 数字内容的发行：发布、收录与消费

前面几章描述了"如何在一个 Zone 里发布并提供可信内容"。本章把视角放大到整个数字内容生态：当一个发布者想把自己的内容（一个 App、一篇文章、一首歌、一个 AI 模型、一个数据集）让全网用户都能可信地发现并消费时，CYFS 数字内容网络在协议层为他和他的用户准备了什么。

数字内容发行的信任问题已经折腾了 30 年，主流方案各有缺陷：

| 方案 | 信任根 | 主要问题 |
| --- | --- | --- |
| App Store（Apple / Google） | 平台审核 | 平台垄断、抽成 30%、审核黑箱 |
| Linux 包管理（apt / yum） | 发行版维护者 GPG | 中心化、覆盖窄、更新慢 |
| npm / PyPI | 域名 + token | 投毒事件频发（typosquatting） |
| Docker Hub | 用户名 | 镜像伪造、root 攻击 |
| GitHub Releases | OAuth 账号 | 账号劫持、社会工程 |

每种方案的根本失败模式都不同，但都没真正解决"普通用户安装一个 App 时如何验证它真的来自声称的发布者"这个核心问题。CYFS 用**强名字 + 内容签名 + 收录证明**这三件事的组合给出一个完整答案。本章描述的机制不限于 App，同一范式适用于所有"有发行人意识的数字内容"。

### 内容也是一个名字

CYFS 把每一份"独立发行的数字内容"都当作一个**独立的名字**来对待。例如：

- 一个 App：`did:bns:app1.buckyos`
- 一篇付费长文：`did:bns:paper1.zhicong`
- 一个 AI 模型：`did:bns:model1.lab`

这些名字的所有者是发布者，但名字本身是**内容自己的身份**，独立于发布者的身份。这不是为了绕开 W3C DID 的语义，而是为了让内容能像产品一样被独立运营：

- **可转让**：开源项目作者把项目卖给公司、个人退休把项目交给社区——内容名字过户，但用户安装的还是同一个 `app1.buckyos`。
- **信用独立**：某个 App 出了安全问题，污染的是这个 App 的信用，不会自动污染同一发布者旗下其他 App 的信用。如果没有独立 DID，所有 App 共享 Owner 信用——一个 App 出问题污染全部。
- **生命周期独立**：每个内容名字下挂自己的版本历史、撤销记录、安全公告，不污染发布者的主文档。
- **心智简单**：用户记的是"产品名"，不是"发布者旗下的某个产品"。在大众消费场景，"产品"心智更适合。

为内容名字付费上链，是这一套机制成立的经济学根基。如果发布免费，攻击者会注册海量相似名字（`app1`、`app01`、`app-1`、`app-l`）做 typosquatting 投毒——npm 和 PyPI 的几乎所有投毒事件都是这么来的。让每个名字都有真实持续成本，把"零成本广撒网"的攻击经济学打掉。

> 名字注册、续期、转让、撤销的具体机制由 BNS 文档规范。本协议只假定这些能力存在。

### ContentDocument：内容的可携带元数据

每个内容名字下挂一份 **ContentDocument**（具体类型可以是 AppDocument、ArticleDocument、ModelDocument、DatasetDocument 等，统称 ContentDocument）。这份文档由内容名字的所有者用其私钥签发，描述这份内容的：

- 当前可用的版本列表（每个版本指向一个具体的 `FileObject`、`ChunkList` 或更复杂的 `NamedObject` 结构）。
- 每个版本的发布类型（`stable` / `beta` / `alpha` / `nightly`）、是否已废弃、是否带有已知安全公告。
- 默认 Zone（最初发布该内容的 Zone）和已知的收录者列表。
- 发布者声明的元信息：名称、描述、图标、许可证、依赖等。

ContentDocument 是一个标准的 `NamedObject`，因此天然可以走 CYFS 的所有传输与验证路径。它本身可以被多源拉取、可以被收录者镜像、可以做断点续传。任何人拿到一份 ContentDocument 都能独立验证：

1. 它的签名确实来自这个内容名字的当前所有者（通过 `resolve` 内容名字得到所有者公钥）。
2. 它声称的某个版本指向的 `FileObject` 的 hash 确实是声明的 ObjectId。
3. 这份文档是不是当前最新版本（通过版本号或上链锚点比对）。

> ContentDocument 的具体字段 schema 由 BNS / 各应用领域文档规范，本协议只规定它必须是一个标准 NamedObject，并通过名字系统的 `(name, type)` 寻址机制可被定位。

### 收录者：发布权与分发权解耦

传统 App Store 的根本问题是**发布权和分发权绑定**——平台既审核内容也独占分发渠道，结果是垄断和抽成。CYFS 把这两件事彻底解耦：

- **发布权**属于内容名字的所有者。任何人都能注册一个内容名字、签发 ContentDocument、把内容上传到自己的 Zone。这一步不需要任何平台审批。
- **分发权与审核权**属于**收录者（Curator）**。收录者是一个有自己 Zone 的实体（可以是一家公司、一个社区、一个 DAO），它对自己感兴趣的内容做审核、签发**收录证明（ListingCertificate）**，并在自己的 Zone 上提供镜像和索引服务。

收录证明本身是一个由收录者 Zone Key 签发的 NamedObject，至少包含：

- 被收录的内容名字（如 `did:bns:app1.buckyos`）和具体版本号、版本 `ObjectId`。
- 收录者对该版本的评级：综合评分 + 分维度评分（如安全审计、代码质量、维护活跃度等）。
- 收录者声明审核了哪些项（如 `no_known_vulnerabilities`、`open_source`、`reproducible_build`）。这是枚举集合而非自由文本，便于客户端做策略匹配。
- 收录证明的签发时间和有效期（强制定期续签，避免"老的优秀评级永远有效"被攻击者利用）。

任何客户端拿到一份收录证明都能独立验证它的签名和声明。这把"我说"变成了"密码学可证明的我说"——任何人拿到都可以独立验证"是这个收录者真的这么说的"。

收录者的查询接口与内容名字解析自然组合：客户端用 `collector_zone.resolve("did:bns:app1.buckyos")` 一次调用就能同时拿到 ContentDocument 与该收录者签发的收录证明，把"查询应用"和"查询收录证明"合并为同一次交互。

### 客户端的综合信任策略

由于一个内容名字可以被多个收录者并行收录，每个用户/客户端可以**自己决定信任哪些收录者、按什么策略组合**。一个典型的信任配置：

```text
最少需要 N 个独立收录者都给出有效收录证明
最低评级 X.Y
必须审核了 ['security_audit', 'reproducible_build'] 这些项
信任的收录者列表 [...]
```

客户端在安装一个 App 时，会：

1. 通过 `resolve(did:bns:app1.buckyos)` 拿到 ContentDocument，验证 owner 签名。
2. 向用户配置的收录者列表分别请求该内容的收录证明。
3. 把所有收录证明汇总，按用户的信任策略综合判断。
4. 选择满足条件的版本，从 ContentDocument 声明的源（默认 Zone + 收录者镜像 + 任意可选 mirror）拉取对应 `FileObject`，按标准 CYFS 流程做内容验证。
5. 验证通过后才安装。

整个过程里没有任何强制的中心化平台。安全洁癖的用户可以要求"3 个以上 5 星收录"，普通用户可以接受"1 个 4 星收录就装"。**信任策略是用户可配置的，而不是平台强加的。** 这也是 CYFS 与传统 App Store 模型最大的差别。

下载源同样是去中心的：

```text
源 1：收录者 zone + /ndn/$objid
源 2：default zone + /ndn/$objid
源 3：任意 mirror + /ndn/$objid
```

`$objid` 是内容 hash，所以所有源都是等价的。下载到哪一个，最后用 hash 验证一定能确保拿到正确内容。这意味着 CDN / 镜像可以是不可信的，任何人都可以做镜像，断点续传 / 多源下载 / P2P 都自动可用。

### 协议中立原则

CYFS 协议本身**不评判**任何发布者、收录者或内容的可信度。所有信用判断都来自生态参与者的链上行为和签名声明。CYFS 的角色相当于 W3C 之于 HTTP，而不是 Apple 之于 App Store——只规定数据结构和验证流程，不参与具体的信任决策。

这条原则不只是工程上的边界感，也是法律和生态意义上的必需：

- **法律层面**：协议设计者不为某个具体收录者或内容的可信度负连带责任。
- **生态层面**：所有参与者预期清晰，不怕协议团队"政策"突变。
- **反脆弱**：单个收录者作恶不影响整体网络可用性。

但 CYFS 协议要为生态留好接口：客户端能查询到一份 ContentDocument 被哪些收录者收录、能拿到所有相关收录证明做交叉验证、能识别一份证明已被撤销。这些机制让生态可以自己演化出**信用聚合服务**、**争议仲裁**、**监督告警**等上层基础设施，而不需要 CYFS 协议正文介入。

具体的反制工具（多收录者交叉验证、收录者保证金 slashing、第三方信用市场）属于经济与治理层，由生态自行设计与演进。CYFS 协议层的责任是让这些工具有协议级的接口可以挂载，而不是替生态实现它们。

### 通用性：所有数字内容都遵循同一范式

把上述模型里的"App"换成其它类型的数字内容，流程几乎不变：

| 内容类型 | 等价于"App" | 等价于"收录者" |
| --- | --- | --- |
| 音乐专辑 | `did:bns:album1.singer` | 音乐平台、电台 |
| 新闻文章 | `did:bns:article1.media` | 新闻聚合器 |
| 学术论文 | `did:bns:paper1.author` | 期刊、arXiv 类机构 |
| AI 模型 | `did:bns:model1.lab` | Model Hub |
| 游戏 | `did:bns:game1.studio` | Game Store |
| 数据集 | `did:bns:dataset1.org` | 数据市场 |
| NFT 艺术品 | `did:bns:art1.artist` | 拍卖行、画廊 |
| 3D 资产 | `did:bns:asset1.studio` | 资产市场 |

所有这些场景都有相同的核心问题：内容真实性、内容完整性、内容评级、内容分发。传统方案在每个领域各搞一套（App Store / Spotify / Steam / Hugging Face / OpenSea），互不通用，每个都有自己的问题。CYFS 是第一个在协议层就把这些场景统一到同一个发行范式下的方案——

发布权与分发权解耦、内容名字独立、收录证明可独立验证、客户端可配置信任策略——四件事可以原样套用到任何"有发行需求"的数字内容上。这种统一性本身就是 CYFS 数字内容网络的核心价值之一。

## 内容生命周期：版本、撤销与信任卫生

发布只是开始。内容上线后，发布者可能要修复 bug、升级版本、撤销有问题的版本；收录者可能要修改自己的评级、撤销收录；用户安装的内容可能在不知情中变成"已知恶意"。CYFS 在协议层为这些场景定义统一的语义，避免每个应用各自实现一套不兼容的生命周期管理。

本章只讨论与**内容网络**直接相关的生命周期机制。名字层（OwnerDocument 与 ZoneDocument 自身）的版本与撤销由 BNS 协议规范，本节只引用它们的存在。

### 版本是 ContentDocument 的一等公民

ContentDocument 不是"指向一个固定 ObjectId 的指针"，而是"一份带版本历史的内容档案"。一个典型的 ContentDocument 同时声明：

- 当前所有可用版本的清单。
- 每个版本的发布类型（`stable` / `beta` / `alpha` / `nightly`）。
- 每个版本是否已被发布者标记为废弃（`deprecated`）。
- 每个版本是否带有已知安全公告（`security_advisory`）。
- 版本之间的替代关系（`supersedes`），不一定按 semver 顺序。

客户端可以根据用户的偏好策略自动选择"我只装 stable 版本，不装有 advisory 的"——这种判断不需要平台介入，因为所有这些信息都是发布者签名后写在 ContentDocument 里的。

发布新版本就是签发新版本的 ContentDocument。客户端通过版本号单调递增（`version_seq`）判断哪份是最新的，并**拒绝接受比缓存版本旧的 ContentDocument**——这条规则避免攻击者用截获的旧文档让客户端"降级"到已知有问题的版本。

> 版本号既可以是发布者本地维护的单调计数器，也可以使用上链锚点（区块高度、状态根）作为天然来源——后者由名字系统天然保证单调性和不可伪造性。具体使用哪种由 BNS 规范，本协议消费"version_seq 单调可比较"这个能力。

### 内容撤销 = 发布新文档

CYFS 不引入独立的"撤销列表"机制。撤销一个版本就是发布一份新的 ContentDocument，把对应版本标记为 `deprecated` 或挂上 `security_advisory`。客户端通过追新机制自动看到撤销，不需要去查独立的 CRL / OCSP 之类的东西。

这把传统 PKI 里"撤销"这个独立机制，简化成了"版本升级"的副产品。少一个独立机制 = 少一个攻击面 = 少一份运维负担。这种思路与 Git 的"不可变历史 + 可移动指针"同构：每份 ContentDocument 自身不可变，名字下挂的"当前版本"指针随时间前进；客户端追最新指针就自动获得最新真相。

紧急情况下（例如某个版本的密钥被攻陷、某个版本被发现有恶意代码），发布者可以走两条路径：

| 路径 | 机制 | 成本 | 生效延迟 |
| --- | --- | --- | --- |
| 常规路径 | 签发新 ContentDocument，把出问题版本标记 deprecated | 零成本 | 客户端缓存自然过期后生效 |
| 紧急路径 | 在 OwnerDocument 中声明"某时间点之前签发的所有 JWT 一律失效"（validity floor） | 链上手续费 | 客户端下次解析名字即可拿到最新 OwnerDocument，立即生效 |

紧急路径的具体协议（OwnerDocument 上链更新、validity floor 时间锚定方式等）由 BNS 规范，本协议消费这个能力。

### 收录证明的撤销与续签

收录证明本身也有生命周期。一个收录者发现自己收录的某个内容版本后来被发现有问题时，可以：

- **主动撤销**：签发一份"撤销声明"覆盖原来的收录证明。客户端在重新验证时看到撤销，触发"trust degraded"提示。
- **不续签**：收录证明强制带 `valid_until`，到期不续签等价于"放弃为该版本背书"。

CYFS 客户端 **SHOULD** 实现一个轻量的后台任务：定期（如每天）重新拉取已安装内容的收录证明，识别撤销和过期。发现"trust degraded"时不一定立刻删除已安装内容（用户可能还在依赖它），但要清楚地通知用户："这个内容之前被 X 收录者推荐，现在收录被撤销了，请考虑是否继续使用。"

这种 trust monitoring 是传统 App Store 没有的——传统 Store 撤包后用户的已安装版本继续运行，不会有任何提示。

### 时效性分级：让应用按需选择

不是所有应用对吊销时效性的要求都一样。CYFS 在协议层提供三档时效性：

| 时效档 | 机制 | 延迟 | 适用场景 |
| --- | --- | --- | --- |
| 快档 | 客户端实时同步名字系统底层（轻节点 / 全节点） | 秒级 | 金融、支付、关键基础设施 |
| 中档 | 客户端定期查询轻节点 RPC 或 OwnerDocument 端点 | 分钟级 | 普通 App、文档阅读 |
| 慢档 | 仅依赖文档自身的 `exp` 字段自然过期 | TTL 长度 | 社交浏览、低风险消费 |

应用层根据自己的安全需求选择。同一套基础设施同时支持金融级和聊天级的需求，不需要两套实现。这种"信任灵活性"是优于 HTTPS PKI 的——HTTPS 的吊销时效性几乎没法选，要么慢（CRL）要么不可靠（OCSP soft-fail）。

### 发现窗口远比响应窗口重要

工业实战经验：成熟攻击者攻陷一把密钥后，会非常小心地签发恶意内容（频繁签发会触发监控告警，留下可观测痕迹）。攻防真正的瓶颈是**发现窗口**——攻击潜伏多久才被注意到——而不是**响应窗口**——发现后多久能撤销。

这背后的统计事实是：工业界 APT 攻击的平均潜伏时间（dwell time）通常在几个月到一年以上，平均响应时间在几小时到几天，比值是 1000:1 量级。这意味着：

- 把响应时间从"几小时"压到"几分钟"，对总安全性几乎没影响。
- 把发现时间从"几个月"压到"几周"，安全性提升几个数量级。

因此 CYFS 在协议层为"发现"而不是"响应"留好钩子：

- **签发日志可观测**：Zone 在签发任何 PathObject、ContentDocument 时 **SHOULD** 同时写入一份 append-only log，并复制到 Owner 配置的备份地址。如果 Zone 私钥被攻陷但签发日志没被同时攻陷，异常签发就有"现场记录"，用于事后审计或实时监控。
- **客户端反向告警**：客户端在发现 ContentDocument 短时间内 `version_seq` 异常跳跃、或同一资源 PathObject 频繁更新时，可以选择性地向 Owner 配置的 webhook 推送告警。owner 收到大量"我没签的"告警时，就能快速发现攻击。
- **透明性日志（可选）**：参考 Certificate Transparency，所有 ContentDocument 签发可以广播到一个公开的 append-only log，让任何第三方（包括独立监控服务）都能审计。CT 让 HTTPS PKI 的"恶意 CA"问题大幅缓解（DigiNotar、Symantec 都是这样被发现的），CYFS 可以借鉴。

这些都是协议预留的"监控钩子"，具体的监控产品和服务由生态自行实现。CYFS 协议的责任是让这些钩子存在，而不是替运营者监控。

### 与名字系统的边界

最后再次明确：本章描述的所有机制都是**内容层**的生命周期。名字层（一个名字所有者把自己的密钥轮换、把名字本身吊销、把名字过户、紧急 validity floor 上链）的机制由 BNS 协议规范。

这个边界不是技术上的隔离，而是责任的清晰划分：

- **名字层失效** = 整个发布者的所有内容信任根崩塌 → 由 BNS 协议处理。
- **内容层失效** = 单个内容产品的某个版本不可信 → 由 CYFS ContentDocument 处理。

两层各自演进，互不污染。一个名字的密钥轮换不需要重新发布所有内容；一个内容版本的撤销不需要动用名字层资源。两层之间通过 `(name, type)` 寻址解耦：客户端永远先解析名字、再解析内容，名字层的真相变化自然向下传播到所有内容层结果。

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
http://$zone_id/did:bns:group123/inbox
http://$zone_id/did:bns:group123/objects/cyfile:abc/comments
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
- `cyfs-dispatch-error`: `String`。dispatch 失败原因，例如 `no-handler`。
- `cyfs-list-mode`: `String`。语义路径目录响应的实际形态，建议取值为 `strict` 或 `loose`。
- `cyfs-list-truncated`: `Boolean`。语义路径目录响应是否被截断；形态 B 响应中必须明确声明。

**CYFS ReqHeader扩展**

- `cyfs-original-user`: `DID`。说明请求是由哪个用户 DID 发起的。该 Header 只是身份声明，不等价于签名；服务端应结合 tunnel 身份、请求签名或 `cyfs-proofs` 验证真实性。
- `cyfs-cascades`: `json`，`ActionObject Array`。说明该请求是因为什么上游动作链被构造出来的，通常隐晦地表达了逻辑权限，最大长度为 `6`。
- `cyfs-proofs`: `json`，`JWT Array`。用于携带各种行为证明，最常见的是购买证明（收据）；dispatch 场景中也可携带成员证明、邀请证明、请求签名或访问许可。
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
