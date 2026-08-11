# cyfs-ndn

[English](README.md) | [简体中文](README.zh-CN.md)

`cyfs-ndn` 是 CYFS 内容网络 / Named Data Network 的基础实现仓库。它围绕 `CYFS Protocol` 中定义的命名数据、命名对象、Chunk、PathObject、FileObject、ChunkList 等核心概念，提供对象建模、本地存储、跨 Zone 获取、网关访问、文件系统映射和工具链支持。

如果只用一句话概括：

> 这是一个把“基于内容寻址的对象网络”落到 Rust workspace 中的实现仓库。

## 仓库目标

结合协议文档，本仓库主要解决下面几类问题：

- 如何把不可变内容表示为可校验的 `Chunk` / `ObjectId`
- 如何把结构化对象表示为 `NamedObject`
- 如何通过 `PathObject` 把语义路径绑定到当前对象
- 如何用 `FileObject -> ChunkList -> Chunk` 表示大文件与目录
- 如何在本地与跨 Zone 场景下读取、缓存、上传和分发命名数据
- 如何把 CYFS 的对象能力挂载为本地文件系统接口

## CYFS 协议定位

根据 [CYFS Protocol](doc/CYFS%20Protocol/CYFS%20Protocol.md) 的定义，CYFS 是一个偏“语义层”的内容网络协议，而不是一个把所有网络能力都包进去的大而全协议。

它重点定义的是：

- 内容如何被命名和校验
- 内容对象如何组织和互相引用
- 语义 URL 如何绑定到当前对象
- 大文件如何切分成可验证的 Chunk
- 内容如何以 Pull 为中心在 Zone 之间传播
- 受保护内容如何通过“购买证明 / 收据”完成访问控制

它刻意不定义或不强绑定的是：

- 身份系统：消费 W3C DID / BNS DID 扩展，不在协议层重新发明身份体系
- 支付协议：只要求可验证的购买证明，不绑定单一支付系统
- 传输层建连：依赖 `cyfs-gateway` 的 tunnel 框架
- DHT：不把 DHT 作为协议基础设施
- piece 级 P2P 交换协议：协议只定义 Chunk 语义，不规定更细粒度的交换机制
- 传统 Tracker：以动作链、收录者和消费证明替代传统匿名源列表

这也是本仓库的实现边界：它聚焦内容对象和数据网络本身，而不是把 DID、支付、tunnel、链上状态都塞进同一个代码库。

## 核心对象模型

README 级别最值得先记住的是下面几个概念。

### 1. Chunk

`Chunk` 是最小的不可变数据块。其 `ChunkId` 通常由内容哈希计算得到，例如：

```text
sha256:<hash>
```

协议同时支持适合放进 URL / hostname 的 base32 表示形式，但语义上它们是同一个对象标识。

### 2. ObjectId / NamedObject

CYFS 把“内容寻址”从原始字节推广到结构化对象。任何经过稳定编码并计算摘要的对象，都可以拥有 `ObjectId`，从而成为 `NamedObject`。

这意味着：

- 不仅文件内容可以被命名
- JSON 结构、目录、路径绑定关系也可以被命名
- 对象之间的引用可以组成内容网络

### 3. PathObject

`PathObject` 解决的是“语义路径当前指向哪个对象”的问题。协议允许在 HTTP 响应头里返回一个带签名的路径绑定对象，让客户端在不完全依赖 TLS 的前提下验证：

- 某个路径当前指向哪个 `ObjectId`
- 这个绑定是否由可信发布方签发
- 这个绑定是否仍然有效

这使得 CYFS 既能支持直接按 `ObjectId` 访问，也能支持更接近传统 Web 的语义 URL。

### 4. FileObject / ChunkList

大文件不会只用一个 Chunk 表示，而是使用：

```text
FileObject -> ChunkList -> Chunk
```

其中：

- `FileObject` 描述文件本身及其元信息
- `ChunkList` 描述文件由哪些 Chunk 组成
- 每个 `Chunk` 都能独立校验、缓存和分发

这为多源下载、断点读取、透明加速和跨节点缓存提供了基础。

### 5. SameAs / inner_path

- `SameAs` 用于表达对象之间的内容等价、引用或别名关系
- `inner_path` 用于在容器对象内部继续定位字段或子对象

这两个概念共同扩展了“通过 URL 获取对象”的能力，使 CYFS 不止能拿到一个 blob，还能可信地定位到对象内部结构。

## 数据分发模型

CYFS 的分发模型有几个很重要的实现导向：

- Pull-first：跨 Zone 分发最终都落在“请求方主动拉取”
- Source discovery 与 content verification 解耦：谁提供线索可以宽松，校验内容必须严格
- Multi-source：Chunk 可以从不同来源并发获取
- Transparent acceleration：缓存、边缘节点、HTTP 扩展都可以参与加速
- Proof-driven distribution：下载证明、消费证明和动作链既参与激励，也参与可信传播

因此，`cyfs-ndn` 更像“一个可验证内容网络的基础设施层”，而不是传统 BT 风格的 tracker + piece 交换实现。

## 仓库结构

本仓库是一个 Rust workspace，工作区定义在 [src/Cargo.toml](src/Cargo.toml)。

主要 crate 如下：

- `src/ndn-lib`：对象模型与 NDN 基础库，包含 `Chunk`、`Object`、`FileObject`、`DirObject`、HTTP 扩展等核心实现
- `src/named_store`：命名数据本地存储、HTTP 后端、网关、GC、store manager 等
- `src/cyfs-lib`：CYFS 文件系统相关类型与接口抽象
- `src/cyfs`：较高层的 CYFS 能力封装，导出 `NamedFileMgr` 等接口
- `src/fs_meta`：文件系统元数据服务
- `src/fs_buffer`：文件缓冲区与 mmap/本地缓存相关实现
- `src/fs_daemon`：FUSE 守护进程，把 CYFS 能力挂载到本地文件系统
- `src/ndn-toolkit`：测试、客户端和辅助工具
- `src/package-lib`：与包、发布和工具链相关的扩展能力

## 快速开始

### 构建 workspace

```bash
cd src
cargo build --workspace
```

### 运行测试

```bash
cd src
cargo test --workspace
```

### 启动文件系统守护进程

`fs_daemon` 可以把 CYFS 能力挂载为本地文件系统：

```bash
cd src
cargo run -p fs_daemon -- <mountpoint> [--store-config <path>] [--service-config <path>]
```

例如：

```bash
cd src
cargo run -p fs_daemon -- /mnt/cyfs \
  --store-config /opt/buckyos/etc/store_layout.json \
  --service-config /opt/buckyos/etc/fs_daemon.json
```

更多 FUSE 相关说明见 [src/fs_daemon/readme.md](src/fs_daemon/readme.md)。

## 推荐阅读顺序

如果你是第一次接触这个仓库，建议按下面顺序阅读：

1. [README.md](README.md)
2. [doc/CYFS Protocol/CYFS Protocol.md](doc/CYFS%20Protocol/CYFS%20Protocol.md)
3. [src/ndn-lib/readme.md](src/ndn-lib/readme.md)
4. [src/fs_daemon/readme.md](src/fs_daemon/readme.md)
5. `src/named_store`、`src/cyfs`、`src/fs_meta` 的源码

## 相关文档

- 协议总览：[doc/CYFS Protocol/CYFS Protocol.md](doc/CYFS%20Protocol/CYFS%20Protocol.md)
- Content Network 说明：[doc/CYFS Protocol/Content Network.md](doc/CYFS%20Protocol/Content%20Network.md)
- 标准对象参考：[doc/CYFS Protocol/CYFS 标准对象.md](doc/CYFS%20Protocol/CYFS%20%E6%A0%87%E5%87%86%E5%AF%B9%E8%B1%A1.md)
- NDM 协议概览：[doc/NDM Protocol/overview.md](doc/NDM%20Protocol/overview.md)
- named fs v2：[doc/named_fs_v2.md](doc/named_fs_v2.md)
- 守护进程与挂载实现：[src/fs_daemon/readme.md](src/fs_daemon/readme.md)

## 当前 README 的定位

这个 README 只做仓库入口说明，不试图替代协议文档本身。更细的协议细节，例如：

- `PathObject JWT` 的字段约束
- `Canonical JSON` / `ObjectId` 计算规则
- `inner_path` 的 URL 规范
- `get_object_by_url` / `open_reader_by_url` 流程
- 内容购买收据验证流程

请直接查看协议文档对应章节。
