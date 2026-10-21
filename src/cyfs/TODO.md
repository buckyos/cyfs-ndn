# CYFS 独立开发与验证 TODO

更新日期：2026-10-20。

目标来自 [readme.md](readme.md)：面向家用分布式环境，通过分布式存储扩容，通过读写缓存改善可用性，通过定期备份提高可靠性；持续将冷数据对象化，减少 fs_meta 的可变记录，逐步支持更大的文件系统。

## 开发边界与推进顺序

- **正式接入 BuckyOS 是 0.8 版本的规划。当前在 cyfs-ndn 内独立开发和验证，不依赖运行中的 BuckyOS Zone、nfs-server、Desktop 或用户数据。**
- 当前 nfs-server 的活文件读写以宿主文件系统为后端，尚不能代表 CYFS 的端到端性能；当前 fs_daemon 使用进程内 fs_meta/fs_buffer，也不是 NFSP 客户端。
- 先验证自有 client、协议和服务的完整链路，再验证 FUSE 系统接口。复用现有协议和组件，独立测试入口不复制 nfs-server 的完整产品功能。
- 原生 API 的一致性、单写、提交和缓存契约应先明确。FUSE 的 POSIX 兼容能力单独列出，不能用接口存在或返回成功代替语义实现。
- 建议顺序：**M0 正确性回归 → M1 单机原生链路 → M2 独立网络链路 → M3 分布式生命周期与恢复 → M4 FUSE → M5 集成准入**。性能和故障测试随各阶段持续执行。
- FUSE 按系统接口的交付范围独立验收；未启用 FUSE 时，不把完整 POSIX 兼容作为原生服务接入的前置条件。

优先级：P0 为数据正确性或相应阶段的验收阻断；P1 为设计目标、关键性能或恢复能力；P2 为在基础能力稳定后优化的项目。下列未勾选项均未验收完成。

参考设计：[CYFS v2](../../doc/cyfs_v2.md)、[NFSP](../../doc/NamedFileSystem_Protocol_v0.md)、[FUSE 行为](../fs_daemon/fuse_behavior.md)、[FUSE 用例](../fs_daemon/testcase.md)。文档描述与实现不一致时，先明确契约并同步修正文档。

## M0：把已复现的数据问题纳入仓库回归

2026-10-20 的审查在隔离副本中运行了原有 135 个 fs_meta 测试和 11 个 fs-buffer 测试，均通过；另加的四个回归均暴露问题。新增测试当时仅位于临时副本，尚未进入仓库。以下验收必须重新执行，不能用历史结果代替。

| 优先级 | TODO | 验收条件 | 代码入口 |
| --- | --- | --- | --- |
| P0 | [ ] 修复 Linked → Finalized 将 LocalLink 当成已完成物化，随后删除其 backing buffer | 写入 64 MiB + 1 字节，Linked 前后、finalize 后和服务重启后逐字节一致；先确认目标内容持久化，再删除源 buffer；覆盖普通文件及 overlay dirty chunks | [local_filebuffer.rs](../fs_buffer/src/local_filebuffer.rs)、[ndm.rs](../named_store/src/ndm.rs) |
| P0 | [ ] 实现 fs_meta 的 `load_file_chunklist`，修复冷对象追加丢失前缀 | 已有 `existing-prefix-`，append `tail` 后必须为 `existing-prefix-tail`；覆盖 ObjId dentry、Finalized、Cooling、跨 chunk 追加及普通非截断写打开 | [fs_meta_service.rs](../fs_meta/src/fs_meta_service.rs)、[named_file_mgr.rs](src/named_file_mgr.rs) |
| P0 | [ ] 修复 `publish_dir` 跳过 SymLink 后清空全部 upper dentries | 含符号链接的目录发布后 namespace 保持一致；若对象格式暂不支持，必须在修改 namespace 前明确拒绝，不能静默删除 | [named_file_mgr.rs](src/named_file_mgr.rs) |
| P0 | [ ] 修复 `publish_dir` 失败路径遗留事务 | 未发布 child、对象读取/写入失败、提交冲突等出口均释放事务；失败后无关目录写入成功，不等待事务超时清理 | [named_file_mgr.rs](src/named_file_mgr.rs)、[fs_meta_service.rs](../fs_meta/src/fs_meta_service.rs) |

- [ ] 将上述四个回归正式加入对应 crate；同时覆盖正常路径和故障重试，修复后保持回归通过。
- [ ] 核对并补齐 `set_file_with_body`、`copy_dir`、snapshot 等公开 API 中的占位实现；未支持能力返回明确错误，不能触发 `unimplemented!()` panic。
- [ ] 单独记录自动生命周期尚未接通的现状：Linked/finalize 问题已能显式调用复现，不能误写成当前后台已经自动执行该流程。

## M1：单机原生链路的语义、持久化与成本

### P0：统一提交及恢复契约

- [ ] 明确 `write`、`flush`、`close`、`CommitPolicy::Immediate/Staged`、对象化完成分别承诺什么；区分用户态已接收、本地持久化、远端持久化、副本确认和备份完成。
- [ ] 消费并验证 commit policy，避免配置存在但不改变行为。单机验证可先实现本地持久化，其他等级在实现前不得宣称支持。
- [ ] 修复 diff 文件原地 truncate/rewrite 的崩溃窗口。采用可恢复的日志、追加写或版本切换方案，后续失败不能破坏上一次已确认持久化的内容。
- [ ] 逐一验证数据文件、diff state、buffer DB、fs_meta 之间的提交顺序；在各边界注入失败并重启，保证不会发布不可读对象或删除唯一有效副本。
- [ ] 明确读己之写、另一个 reader 的可见时点、读句柄绑定的版本，以及失败/取消后的资源所有权。

### P1：使写缓存真正有界，并消除累计放大

- [ ] 将 `DiffChunkListWriter` 从全量 dirty bytes 常驻内存改为有界缓存；超过预算后持续落盘，不能等文件 close 才写出全部内容。
- [ ] 实现增量 flush，避免每次截断、重写和 hash 全部历史 dirty chunks；持久化后正确推进 dirty 状态。
- [ ] 避免重开 writer 时完整读入 diff 再逐块复制；恢复成本应由必要元数据和工作集决定。
- [ ] 降低小范围覆盖已有大 chunk 的读取和重写放大；分别设计可变写入粒度与最终对象 chunk 粒度。
- [ ] 统一追加分块策略，避免 chunk 大小随单次 write 调用大小变化；消除按 chunk 数线性扫描定位的热点。
- [ ] 完善空间计账：启动恢复实际占用、未知文件长度、追加增长、失败清理、共享引用和磁盘满；达到容量上限时提供可预期的背压或错误。

当前实现观察：顺序写入的内存可能随文件大小增长；每次追加 b 字节并 flush，N 次累计重写量可能为 `b × N(N+1)/2`；首次修改一个冷的 32 MiB chunk 中的 4 KiB，会读取整个 chunk。这些是需要消除或约束的算法成本，不是吞吐实测值。

入口：[diff_chunk_list.rs](../named_store/src/diff_chunk_list.rs)、[local_filebuffer.rs](../fs_buffer/src/local_filebuffer.rs)、[named_file_mgr.rs](src/named_file_mgr.rs)。

### P1：原生读写和元数据接口

- [ ] 提供 `read_at/range` 或可 seek 的 reader，并维护稳定 reader handle；读取尾部范围不能从文件头扫描。
- [ ] 在 Working/Cooling 等状态维护 logical size；`stat/lookup` 只能读元数据，不能扫描正文求长度。
- [ ] 明确 `ReadOptions::auto_pull` 和 `NEED_PULL` 的行为，接通需要的获取逻辑；无效选项不能被静默忽略。
- [ ] 大目录采用稳定 cursor/快照和按需分页，批量取 inode/属性；限制 list session 的 TTL、数量和内存，客户端退出后可回收。
- [ ] 冷目录对象也应支持合理的按需查找与列举；测量完整 DirObject 加载、解析和 overlay 合并成本，再确定是否需要分片/索引。
- [ ] 缩短 fs_meta 写事务：buffer 分配、同步、远端 I/O 和目录对象构建移出全库写锁；提交时通过 revision/OCC 检查一致性。
- [ ] 用并发测试验证 path cache 的填充与失效：旧 resolver 不能在 rename/delete 后重新填入过期路径；缓存命中不应绕过版本验证。
- [ ] 减少深路径操作逐层 `get_dentry/get_inode` 的串行调用，优先在服务端完成解析或复用带版本的父目录 handle。

入口：[fsmeta_client.rs](../cyfs-lib/src/fsmeta_client.rs)、[fs_meta_service.rs](../fs_meta/src/fs_meta_service.rs)、[list_cache.rs](../fs_meta/src/list_cache.rs)、[path_resolve_cache.rs](../fs_meta/src/path_resolve_cache.rs)。

## M2：独立 client + 协议 + 服务 E2E

### P0：独立运行与并发正确性

- [ ] 在本仓库提供最小独立启动和测试入口，使用临时数据目录、显式配置和可控端口；支持自动启动、停止、重启、日志采集和清理，不需要 BuckyOS runtime。
- [ ] 保留进程内原生 API 基线，并增加实际跨进程的 client/service 路径；不能以 in-process mock 代替网络链路验收。
- [ ] 明确首阶段部署拓扑。可先采用 `自有 client → 文件协议 service → 进程内 NamedFileMgr/fs_meta/fs_buffer`，完成外部网络 E2E，无需先把内部组件拆成独立服务。
- [ ] 若选择 fs_meta 分离部署，再补齐该拓扑的远程 client/service 操作覆盖。当前 `open_file_writer` 已有 KRPC 分支，但 `open_file_reader/close_file_writer` 及多项 namespace 高层操作仍未贯通；这是分离部署的验收门槛。
- [ ] 明确权威元数据、客户端缓存和数据后端的部署边界。多个客户端必须共享一致的 namespace；多个独立 SQLite 实例指向同一 HTTP bucket，不等于共享文件系统。
- [ ] 将 writer session 与实际调用者绑定；实现长写入续租、过期、取消和 fencing，buffer/commit 端拒绝旧 token。覆盖超过当前 300 秒租约的上传及断线重连。
- [ ] 验证同文件单写、不同文件并发、rename/delete 与打开句柄交错、请求重试和响应丢失；明确幂等键及服务重启后的保证范围。

### P1：数据面与缓存

- [ ] 完善远端对象和 chunk 元数据的访问/恢复机制。当前 HTTP backend 之前仍需要本地 SQLite 记录；验证新客户端、服务重启、元数据重建后可发现并读取既有内容。
- [ ] 覆盖“chunk 已落盘但元数据尚未登记”的崩溃窗口；重复 PUT 必须能够完成恢复，不能成功返回却仍不可读。
- [ ] 支持流式上传、断点续传及有界并发；验证大于客户端/服务端内存预算的文件，避免完整文件预加载或强制预哈希阻塞首字节传输。
- [ ] 依据 RTT、带宽和内存预算设置分片/窗口；协议保证串行的步骤不得直接并行化，应先定义 offset、幂等和提交顺序。
- [ ] 为顺序读增加有界预取，合并相邻 range；同 store 的锁不得覆盖整个慢网络请求而阻塞无关操作。
- [ ] 区分 immutable ObjId 缓存与可变路径缓存，明确版本、TTL、失效、重连 resync 和离线可读范围。
- [ ] 明确离线写是否纳入首阶段。若支持，必须有本地持久队列和恢复冲突策略；若不支持，应明确拒绝，不能以 buffer 存在宣称支持。

协议复用 [NFSP](../../doc/NamedFileSystem_Protocol_v0.md) 与现有接口设计；当前 BuckyOS WebUI 的整文件预哈希、nfs-server 的 native list/复制实现等问题保留为 0.8 集成检查项，不作为本仓库独立开发的外部前置条件。

## M3：兑现对象化、扩容、可用性与备份目标

### P0/P1：可恢复的后台生命周期

- [ ] 为 `BackgroundTaskExecutor` 提供生产实现并接通 worker；串联 Cooling → Linked → Finalized、目录发布和冷目录收敛。
- [ ] 在 M0 数据问题修复后才启用自动执行。后台任务需具备可恢复状态、幂等、重试、取消及失败观测能力。
- [ ] 修复 linked/finalized 标记先落本地、fs_meta 更新随后失败时的恢复路径；不能因标记已存在就提前返回成功。
- [ ] 发布目录采用短事务/OCC；并发修改时重试或明确冲突，不能丢目录项。
- [ ] 清理被对象化替代的可变 inode/dentry，尊重活跃 writer、读句柄、快照和引用；区分可回收的元数据/临时数据与需要显式管理的业务对象 GC。
- [ ] 为后台 hash、物化、迁移和清理设置 CPU、I/O、网络及并发预算，保证前台读写优先；暴露 backlog、最老任务年龄和失败数。

验收：写入一批文件并关闭后，后台能完成对象化，fs_meta 记录数及 buffer 占用实际下降；各阶段重启后均可继续，namespace 与内容保持一致。单纯 state 字段变化不能视为完成。

入口：[background.rs](../fs_meta/src/background.rs)、[fs_meta_service.rs](../fs_meta/src/fs_meta_service.rs)、[local_filebuffer.rs](../fs_buffer/src/local_filebuffer.rs)。

### P0/P1：扩容和节点故障

- [ ] 持久化 store/layout/epoch 变更，并验证重启恢复；不要只更新进程内布局。
- [ ] 引入迁移完成屏障；旧布局仍承担数据定位时不得因超过三版或调用 `compact()` 被直接淘汰。
- [ ] 实现并验证读时修复或后台迁移，覆盖连续多次扩容、迁移中重启、目标磁盘满以及节点离线；迁移后再验证旧位置清理。
- [ ] 分别定义元数据服务、热 buffer 和冷对象节点不可用时的行为；缓存命中可用范围、失败返回时间及恢复时间必须可测。
- [ ] 明确首阶段使用单副本、备份还是在线冗余；如提供多副本，定义确认条件和故障域。不能把多 store 的容量分布等同于冗余副本。

入口：[ndm.rs](../named_store/src/ndm.rs)、[store_layout.rs](../named_store/src/store_layout.rs)、[named_file_mgr.rs](src/named_file_mgr.rs)。

### P1：备份与恢复

- [ ] 定义一致备份边界，覆盖 namespace 元数据、对象引用、数据内容和必要布局配置，明确是否包含未 finalized 的写入。
- [ ] 实现独立调度或可调度命令，支持增量备份、保留策略、失败重试和结果校验。
- [ ] 在全新数据目录/替代节点恢复，逐项核对路径、属性、链接和内容；不能只验证备份文件生成成功。
- [ ] 给出实测恢复时间和可接受的数据丢失窗口，区分本地进程故障、节点丢失和备份目标不可用。

## M4：FUSE 作为同一核心能力的系统适配层

FUSE 可与前述开发并行修复，但只有原生路径语义通过后，才比较它的额外开销。

- [ ] **P0：修复挂载生命周期。** 当前 `spawn_mount2` 后立刻 `session.join()`，锁定的 fuser 0.14 中 join 会先卸载。验收必须通过真实 mount、持续操作、正常退出和清理。
- [ ] **P0：实现真实 fsync 和错误传播。** 当前 flush/fsync 直接成功，真正持久化延迟到 release；fsync 后不 close，立即 kill daemon，重挂载验证已确认内容。进程崩溃测试与断电持久性验证应分别标注。
- [ ] **P1：使用 range/seek 和 reader handle。** 每次 read 不得重新从零扫描到 offset；覆盖大文件首、中、尾读取及跨 chunk 请求。
- [ ] **P1：解除全局串行瓶颈。** 合理派发请求，避免 callback `block_on`、全局 NamedFileMgr 锁、整个 handle map 锁覆盖数据 I/O；一个慢文件不得阻塞所有文件。
- [ ] **P1：目录句柄保存稳定分页状态。** 避免每次 readdir 都 `list_next(0)` 全量枚举；验证并发增删时不重复/遗漏无关项，按需支持 readdirplus。
- [ ] **P1：属性读取不触发正文 I/O。** 修复缺 size 时扫描文件的 fallback；补 inode forget 和缓存回收，遍历大目录树不能永久保留全部路径。
- [ ] **P0/P1：按支持范围补齐系统语义。** 包括非零 truncate、稀疏写、append、open-unlink、rename 覆盖/flags、非空 rmdir、mode/umask、属性与 xattr 持久化；未支持操作明确返回错误，禁止假成功。
- [ ] **P1：明确 FUSE 与原生客户端并存时的缓存一致性。** 建立版本/失效通知机制后再决定 page cache、direct I/O、writeback cache 策略，验证另一客户端的同尺寸覆盖与 rename。
- [ ] 将 [testcase.md](../fs_daemon/testcase.md) 的用例转为可重复执行的真实挂载测试；不能只测 helper，也不能用 fsync 后再 close 的用例掩盖空 fsync。

算法回归：当前 1 GiB 文件按 128 KiB/request 读取，前缀扫描模型会处理约 4 TiB 数据；优化后，处理量应与请求范围及有界预取相关，而非与累计 offset 相关。该数字是代码模型，不是挂载实测。

入口：[fs_daemon.rs](../fs_daemon/src/fs_daemon.rs)、[现有测试](../fs_daemon/src/fs_daemon_tests.rs)、[挂载测试入口](../fs_daemon/tests/fuse_cli_e2e.sh)。

## 持续 E2E 验证矩阵

对照组使用同一数据集和后端：宿主文件系统基线、进程内 NamedFileMgr、独立 client/service、FUSE。尚未实现的链路标为未测试；每组保持相同提交等级和缓存条件。

| 场景 | 至少覆盖 | 重点指标/断言 |
| --- | --- | --- |
| 顺序大文件 | 64 MiB 阈值两侧、1 GiB、超过内存预算；读与写；不同 chunk 边界 | 吞吐、首字节、RSS、close/持久化延迟、实际后端字节数 |
| 随机范围 I/O | 4 KiB/128 KiB，首/中/尾，跨 chunk，局部覆盖 | p50/p95/p99、读写放大；成本不能随文件 offset 线性增长 |
| 持续追加与频繁提交 | 长期开 fd、重复打开、每次/每批 flush，写入时长超过 lease TTL | 原内容保留、租约有效、脏数据预算、增量落盘和崩溃恢复 |
| 小文件与目录 | 万级/十万级文件，深路径，大目录首页/完整翻页/树遍历 | RPC/SQL 数、首屏延迟、session/inode 缓存占用、目录变更一致性 |
| 多客户端 | 并发 1/8/32，不同文件及同一文件，原生/FUSE 混合访问 | 聚合吞吐、公平性、尾延迟、严格单写、版本失效 |
| 网络条件 | loopback、LAN、模拟 WAN；明确 RTT/带宽/丢包参数 | 分片/预取窗口、重试量、断点恢复、连接数和内存上限 |
| 缓存状态 | 客户端冷/热、服务端冷/热、内核 page cache、对象缺失 | 命中率、真实回源流量；避免把缓存读当作远端吞吐 |
| 生命周期与扩容 | 前台写入同时冷却/备份/迁移；超过三次布局变更 | 前台 p99、后台积压、元数据收敛、旧数据可读性 |
| 故障注入 | kill/restart、响应丢失、磁盘满、节点掉线、提交阶段失败 | 已确认数据不丢、无悬挂引用/写锁、重试幂等、恢复时间 |

- [ ] 提供一键执行的独立 fixture，故障仅作用于测试进程和临时目录；普通单测不要求真实 mount，多进程和 FUSE 用例明确环境要求。
- [ ] 保存硬件、文件系统、网络参数、构建模式、配置、数据集、缓存状态和提交等级；性能测试使用 release 构建并记录原始结果。
- [ ] 在正式基线测量后设定吞吐/延迟预算；先检查内存是否有界、增量 flush 是否仍累计放大、范围读是否重复扫描等结构性门槛，不预填未经测量的 MiB/s。
- [ ] 同时统计 CPU、RSS、RPC/SQL 数、后端读写字节、锁等待、持久化延迟、后台积压和恢复时间，避免只看 API 返回速度。
- [ ] 修复构建可重复性：核对 manifest 与 Cargo.lock，锁文件由 Cargo 生成；明确 FUSE 系统库与平台要求。GitHub Git 依赖继续显式使用 `main` 分支。

## M5：BuckyOS 0.8 集成准入与后续工作

以下为独立验证完成后的交付边界，不要求当前在 BuckyOS 中实施。

- [ ] 原生 API、跨进程协议、持久化等级、错误码、幂等和缓存语义已有文档与自动回归；已复现的数据丢失/锁泄漏问题全部关闭。
- [ ] 单机、多客户端、节点故障、扩容、冷数据收敛与备份恢复具备可重复结果；明确尚不支持的能力和适用工作负载。
- [ ] 若交付包含 FUSE，则通过所声明的系统语义验收，并提供与原生链路可比较的性能结果；数据库、VM 磁盘等负载没有专项验证前不宣称支持。
- [ ] 给出独立 client/service 的配置、启动、升级、恢复和观测接口，作为 nfs-server 接入 NamedFileMgr 的稳定边界。
- [ ] 在 BuckyOS 0.8 阶段另行检查 nfs-server 后端替换、鉴权/身份、服务部署、既有本地文件导入、客户端上传/分页/复制及跨接口缓存失效；这些工作不混入本轮 CYFS 核心修复。
