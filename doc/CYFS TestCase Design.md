# CYFS TestCase Design

## 1. 目标

本文基于 [CYFS Protocol.md](./CYFS%20Protocol/CYFS%20Protocol.md) 设计一组面向实现落地的测试用例，重点覆盖两类风险最高的内容：

- 边界条件：长度边界、时间窗口边界、编码边界、Header 组装边界、分段解析边界。
- 复杂设计：`PathObject JWT`、RFC 8785 Canonical JSON、`inner_path` 自动解引用、`resp=raw`、`ChunkList` / `SameAs`、大容器 `cyfs-inner-proof`、购买收据与 `pull-first` 传播语义。

本文不直接替代协议，而是把协议中的“容易实现错”和“不同实现容易出现分歧”的地方转成可执行测试集合。

## 2. 测试策略

### 2.1 分层

- L1 对象与编码层：`ObjectId`、`ChunkId`、base32、`mix*`、`clist`、Canonical JSON。
- L2 网关响应层：`cyfs-path-obj`、`cyfs-parents-N`、`cyfs-inner-proof`、`cyfs-chunk-size`、`resp=raw`。
- L3 客户端验证层：`get_object_by_url`、`open_reader_by_url`、Range 校验、`SameAs` 最终确认。
- L4 端到端语义层：购买收据、传播链、跨 Zone `pull-first` 行为。

### 2.2 优先级

- P0：会破坏可信根或导致不同实现不互通。
- P1：会造成行为偏差、缓存污染或安全退化。
- P2：协议未完全定稿，但必须通过测试尽早暴露分歧。

### 2.3 测试基线数据

- Zone：`alice.example`、`bob.example`
- DID：`did:example:alice`、`did:example:bob`
- 签名算法：`EdDSA(Ed25519)` 为必测，`ES256` 为兼容测
- 文件尺寸样本：
  - `0B`
  - `1B`
  - `127B` / `128B`
  - `16383B` / `16384B`
  - `32MiB - 1`
  - `32MiB`
  - `32MiB + 1`
  - `64MiB`
- 容器规模样本：
  - `4096` 项
  - `4097` 项
- 时间窗口样本：
  - `now = iat`
  - `now = exp`
  - `now = exp + 1`
- Header 场景样本：
  - 连续编号
  - 跳号
  - 重号
  - `json:` 与 `oid:` 混用

## 3. 覆盖重点

| 模块 | 风险点 | 典型错误 |
| --- | --- | --- |
| ObjId / ChunkId 编码 | base32 大小写、无 padding、长度前缀 varint | 同一对象被编码成多个字符串；长度解析错 |
| Canonical JSON | Unicode、数字规范、缺省 vs `null`、重复 key | 不同语言算出不同 `ObjectId` |
| PathObject JWT | `alg`/`kid` 处理、时间窗口、旧版本覆盖 | 接受伪造路径绑定或被旧缓存回滚 |
| `inner_path` | 段内自动解引用、跨段规则、Chunk 非结构化 | 服务端展开链与客户端校验链不一致 |
| `resp=raw` | 不附加 Header、不自动展开 | 原本想要 raw，却返回了验证展开形式 |
| `ChunkList` / `SameAs` | `32MiB` 标准切分、总长度语义、Range 最终确认 | 大文件互不兼容，或错误信任 SameAs |
| 大容器 proof | `>4096` 转 proof、proof 与 parent 绑定 | 部分可验证链路被伪造 |
| 购买与传播 | 收据语义边界、`cyfs-original-user`、`pull-first` | 把“付款事实”误实现成“强绑定访问许可” |

## 4. 详细测试用例

## 4.1 ObjId / ChunkId / base32

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| OBJ-01 | P0 | 标准 `sha256:` 与 base32 形式等价 | 准备同一 Chunk 的两种 URL | 分别用标准串和 base32 URL 请求同一 Chunk | 两次请求得到相同内容，客户端归一化后识别为同一 `ChunkId` |
| OBJ-02 | P0 | base32 输出必须小写、无 padding | 生成 `ObjectId` URL | 检查服务端或 SDK 输出 | 不出现大写字母和 `=`；hostname 形式可稳定往返转换 |
| OBJ-03 | P1 | base32 输入大小写归一 | 准备带大写字母的 base32 URL | 用混合大小写发起请求 | 解析前先转小写，仍能命中同一对象 |
| OBJ-04 | P1 | 非法 padding 拒绝 | 准备带 `=` 的 base32 hostname | 发起请求 | 明确报错或拒绝解析，不能静默映射到其他对象 |
| OBJ-05 | P0 | `mix256` 长度前缀边界 | 准备 `127B`、`128B`、`16383B`、`16384B` Chunk | 计算 `mix256`，再反解析长度 | varint 编码与解码一致，跨 LEB128 字节边界不出错 |
| OBJ-06 | P0 | `clist` 总长度前缀语义 | 准备 `32MiB+1` 文件得到 2 段 `ChunkList` | 计算 `clist`，解析长度 | `clist` 前缀长度等于完整文件总长，而不是第一段长度，也不是 JSON 长度 |
| OBJ-07 | P2 | `0B` 文件表示的一致性 | 准备空文件 | 构造 `FileObject.content` 与 `ChunkList` / 空 Chunk 表示 | 各实现必须产生一致结果；若当前无规范，测试应暴露分歧并阻止 silent divergence |

## 4.2 Canonical JSON 与 NamedObject

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| CAN-01 | P0 | 对象字段顺序不影响 `ObjectId` | 准备字段顺序不同但语义相同的 JSON | 分别 canonicalize 并计算 `ObjectId` | 结果相同 |
| CAN-02 | P0 | `null` 与缺省不同 | 准备一份显式 `null`，一份省略字段 | 计算两者 `ObjectId` | 两者不同；实现不得把缺省自动补成 `null` |
| CAN-03 | P0 | 重复 key 非法 | 构造含重复字段 JSON | 尝试构建 NamedObject | 必须拒绝，不能“取最后一个”后继续 |
| CAN-04 | P0 | 非法数字拒绝 | 构造 `NaN`、`Infinity`、`-Infinity`、`undefined` | 尝试 canonicalize | 必须拒绝 |
| CAN-05 | P1 | Unicode NFC 一致性 | 准备 NFC / 非 NFC 但视觉等价字符串 | 计算 `ObjectId` | 若实现声称兼容 RFC 8785，应规范化后得到一致结果；若未做 NFC，则测试应暴露跨语言不一致 |
| CAN-06 | P1 | 数字最短可回读表示 | 准备 `1`、`1.0`、`1e0`、`0.00001` 等输入 | canonicalize | 输出符合 JCS 最短形式；不保留多余零和 `+` |
| CAN-07 | P0 | `FileObject` 自定义字段参与哈希 | 在标准字段外增加 `mime` | 计算 `cyfile` `ObjectId` | 自定义字段会改变 `ObjectId`，不能被忽略 |

## 4.3 PathObject JWT 与语义路径绑定

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| PATH-01 | P0 | `alg=EdDSA` 正常校验 | 准备合法 `cyfs-path-obj` | 请求语义 URL | 客户端验证通过，`target` 可作为可信根对象 |
| PATH-02 | P0 | `alg=none` 必须拒绝 | 构造无签名 JWT | 请求语义 URL | 客户端和网关均拒绝 |
| PATH-03 | P0 | `alg` 与真实签名不匹配 | JWT Header 声称 `EdDSA`，但内容按其他算法签 | 请求语义 URL | 校验失败 |
| PATH-04 | P1 | `kid` 精确选 key | DID Document 中配置多把 key | 用错误 `kid` 签名或把签名 key 与 `kid` 不一致 | 必须拒绝，不能用“任意可用 key”兜底 |
| PATH-05 | P1 | 缺失 `kid` 的兼容策略 | 准备单 key 与多 key 两种 DID Document | 分别请求 | 单 key 场景可兼容通过；多 key 场景必须失败或显式告警，避免歧义 |
| PATH-06 | P0 | `iat` / `exp` 时间边界 | 准备 `now=iat`、`now=exp`、`now=exp+1` | 请求语义 URL | 前两者通过，`exp+1` 失败；不能接受过期绑定 |
| PATH-07 | P0 | 旧版本绑定不能覆盖新版本缓存 | 本地缓存较新 `iat`，服务端返回旧 JWT | 再次请求同一路径 | 客户端保留较新版本，拒绝被旧绑定回滚 |
| PATH-08 | P1 | 路径只绑定 path，不绑定 hostname 之外内容 | 准备相同 path 不同 zone | 混用 JWT | 必须以当前 zone 的可信 key 集校验，不能跨 zone 复用 |

## 4.4 `inner_path`、`cyfs-parents-N` 与响应链

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| INNER-01 | P0 | 单段多级字段路径 | 准备 `/root/@/a/b/c` | 请求并校验 | 单个 `"/@/"` 段内部按 JSON Path 逐级取值 |
| INNER-02 | P0 | 段内遇到 `ObjectId` 自动解引用 | 令 `/a` 返回 `ObjectId X`，段内剩余 `/b` | 请求 `/root/@/a/b` | 同一段内自动解引用 `X` 后继续 `/b` |
| INNER-03 | P0 | 显式跨段解引用 | 同上 | 请求 `/root/@/a/@/b` | 与 `INNER-02` 最终值相同，但验证链可显式出现 `X` |
| INNER-04 | P0 | 非 `ObjectId` 后继续加 `"/@/"` 非法 | 令 `/a` 返回普通字符串 | 请求 `/root/@/a/@/b` | 服务端或客户端明确报非法请求 |
| INNER-05 | P0 | Chunk URL 不能追加 `inner_path` | 准备 `/ndn/$chunkid/@/x` | 请求 | 必须拒绝 |
| INNER-06 | P0 | `cyfs-parents-N` 必须连续编号 | 构造只返回 `0` 和 `2` 的响应 | 客户端校验 | 失败，不能忽略跳号 |
| INNER-07 | P1 | `json:` 与 `oid:` 混用 | 第一层 parent 返回完整 JSON，第二层只返回 `oid:` + proof / 二次获取 | 请求两层 `inner_path` | 客户端可按模式分别校验，不因混用出错 |
| INNER-08 | P1 | 标量最终值时不应携带 `cyfs-obj-id` | 令最终值为字符串或数字 | `open_object_by_url` | 返回该值本身，`cyfs-obj-id` 缺失或不参与校验 |
| INNER-09 | P1 | `cyfs-chunk-size` 表示完整 Chunk 大小 | 对 Chunk 做 Range 请求 | 校验 Header | `cyfs-chunk-size` 是整体大小，不是本次 Range 大小 |

## 4.5 `resp=raw`

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| RAW-01 | P0 | 服务端必须支持 `resp=raw` | 准备对象 URL | 请求 `?resp=raw` | 返回 raw 结果，不得忽略参数后继续返回“展开/验证模式” |
| RAW-02 | P0 | `resp=raw` 不附加 CYFS 验证 Header | 准备语义 URL 与对象 URL | 请求 `?resp=raw` | 响应不带 `cyfs-path-obj`、`cyfs-parents-N`、`cyfs-obj-id` 等附加 Header |
| RAW-03 | P0 | `resp=raw` 不自动展开最终 `ObjectId` | 令最后一步 `inner_path` 结果是 `ObjectId` | 请求 `?resp=raw` | Body 返回该 `ObjectId` 本身，而不是继续解引用后的对象或 Chunk |
| RAW-04 | P1 | Chunk + `resp=raw` + Range 组合 | 准备 Chunk URL | 请求 `Range` + `resp=raw` | 正常返回字节范围，但不附加 CYFS Header；客户端只能依赖 URL 中 `ChunkId` |
| RAW-05 | P1 | 语义 URL + `resp=raw` 的安全退化 | 准备会变化的语义路径 | 连续两次请求 `?resp=raw` | 客户端不能把这次响应本身当作路径绑定证明 |

## 4.6 `ChunkList`、`SameAs`、Range 与大文件

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| CLIST-01 | P0 | `32MiB - 1` 不应切分 | 准备 `32MiB - 1` 文件 | 构造标准 `ChunkList` | 结果应为单 Chunk，而不是两段 |
| CLIST-02 | P0 | `32MiB` 恰好单 Chunk | 准备 `32MiB` 文件 | 构造标准 `ChunkList` | 仍为单 Chunk；最后一段大小 = `32MiB` |
| CLIST-03 | P0 | `32MiB + 1` 必须切成 `32MiB + 1B` | 准备 `32MiB + 1` 文件 | 构造标准 `ChunkList` | 得到两段，前段固定 `32MiB`，后段 `1B` |
| CLIST-04 | P0 | 非最后一段不能小于 `32MiB` | 人工构造 `16MiB + 16MiB + ...` `ChunkList` | 作为“标准 `ChunkList`”导入 | 必须拒绝或标记为非标准，不能与标准切分混淆 |
| CLIST-05 | P1 | `ChunkList` JSON 顺序敏感 | 调换两个 `ChunkId` 顺序 | 计算 `clist` `ObjectId` | `ObjectId` 必须改变 |
| CLIST-06 | P0 | `SameAs` 只能在完整校验后成立 | 配置 `SameAs(chunkA -> clistB)` | 下载完整文件后计算 `chunkA` | 仅当整流最终哈希等于 `chunkA`，才将这条 `SameAs` 记为可信 |
| CLIST-07 | P0 | Range 不能提前确认 `SameAs` | 仅下载 `chunkA` 的中间一段 Range | 校验局部字节 | 可确认“这段字节没坏”，但不得把 `SameAs` 升级为可信缓存 |
| CLIST-08 | P1 | `cyfs-chunk-size` 与 `mix*` 长度一致 | 准备 `mix256` Chunk | 响应读取 | Header 中完整大小与 `mix256` 解析长度一致 |
| CLIST-09 | P1 | 直接请求大 `sha256 ChunkId`，服务端内部走 `SameAs` | 准备 3.1G 逻辑大 Chunk 与等价 `ChunkList` | 客户端请求大 `sha256:` URL | 客户端无需知道内部展开路径，只验证原始目标 `ChunkId` |
| CLIST-10 | P2 | 异常末段长度处理 | 人工构造 `ChunkList`，最后一段长度与 `total_size` 不一致 | 下载并重组 | 客户端最终校验失败；暴露切分器或元数据错误 |

## 4.7 大容器 `cyfs-inner-proof`

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| PROOF-01 | P0 | `4096` 项容器的边界处理 | 准备 `4096` 项容器 | 请求 `container/@/key` | 可继续走完整 parent Header；若实现选择 proof，也必须与协议说明一致且可验证 |
| PROOF-02 | P0 | `4097` 项容器切换到部分可验证模式 | 准备 `4097` 项容器 | 请求 `container/@/key/@/content` | 响应包含 `cyfs-inner-proof`，而不是试图把完整容器塞入 Header |
| PROOF-03 | P0 | proof 与 `parent_obj` 绑定 | 篡改 `leaf_value` 或替换 `cyfs-parents-0` | 客户端验证 | 任一被篡改都应失败 |
| PROOF-04 | P1 | proof key 排序一致性 | 使用边界 key，如大小写、Unicode、长 key | 构造 proof 并验证 | 服务端和客户端对叶子排序规则一致，否则测试失败 |
| PROOF-05 | P1 | proof 只证明当前 key，不外溢为整个容器可信 | 构造单 key proof | 访问另一个 key | 不得复用旧 proof 去证明其他 key |

## 4.8 购买收据、访问上下文与传播语义

| ID | 优先级 | 测试点 | 前置条件 | 步骤 | 预期 |
| --- | --- | --- | --- | --- | --- |
| AUTH-01 | P0 | 购买收据签名或链上状态有效 | 准备合法与伪造收据 | 携带 `cyfs-proofs` 请求受保护内容 | 合法收据通过，伪造收据失败 |
| AUTH-02 | P0 | 收据覆盖内容范围校验 | 准备只覆盖 `ObjectA` 的收据 | 请求 `ObjectA` 与 `ObjectB` | 只允许读取覆盖范围内内容 |
| AUTH-03 | P1 | “付款事实”与“强绑定访问许可”区分 | 准备可转交收据与强绑定收据两类业务规则 | Bob 携带 Alice 收据访问 | 可转交规则应通过；强绑定规则应要求 `cyfs-original-user` 匹配 |
| AUTH-04 | P1 | 收据过期 / 撤销 / 次数超限 | 准备过期、撤销、超限收据 | 请求受保护内容 | 必须拒绝 |
| AUTH-05 | P1 | `cyfs-cascades` 长度上限 | 构造长度 `6` 与 `7` 的动作链 | 发起请求 | `6` 合法，`7` 被拒绝或截断并显式报错；不能静默接受超长链 |
| AUTH-06 | P2 | 缺少 `Reference` / `ReferencePath` 的兼容性 | 准备仅有 DID、无传播上下文的请求 | 访问半公开内容 | 是否允许访问应由业务策略明确；测试用于暴露实现是否错误地把 `Reference` 视为协议必填 |
| AUTH-07 | P0 | 跨 Zone `pull-first` 语义 | ZoneA 向 ZoneB 发送含附件引用的 `MessageObject` | 观察消息到达后行为 | ZoneB 收到消息时不应被动接收附件；只有业务决定后才主动 `open_reader_by_url` Pull |
| AUTH-08 | P1 | 接收方拥有最终下载控制权 | ZoneB 收到消息后选择“不下载” | 观察网络行为 | 不发生隐式跨 Zone 数据推送，不应出现“发送即上传成功”的语义 |

## 5. 组合场景测试

下面三组端到端场景建议作为回归主链，优先自动化。

| 场景 ID | 优先级 | 场景描述 | 覆盖点 |
| --- | --- | --- | --- |
| E2E-01 | P0 | 语义 URL `-> PathObject JWT -> DirObject -> FileObject -> content -> Chunk` | `cyfs-path-obj`、`cyfs-parents-N`、`inner_path`、`cyfs-obj-id`、Chunk 校验 |
| E2E-02 | P0 | 大文件 `FileObject -> clist -> 多 Chunk -> Range -> 完整重组` | 标准切分、`clist` 长度、Range、最终完整哈希 |
| E2E-03 | P0 | 大容器 `container/@/key/@/content` + proof + 购买收据 | `cyfs-inner-proof`、父对象绑定、内容授权、最终对象校验 |

## 6. 自动化建议

- 单元测试：
  - `ObjectId` / `ChunkId` 计算器
  - base32 编解码
  - RFC 8785 Canonical JSON
  - `mix*` / `clist` 的 varint 解析
- 协议集成测试：
  - 网关 Header 组装
  - `resp=raw` 与普通模式切换
  - `cyfs-parents-N` 连续性校验
- 端到端测试：
  - 客户端从 URL 到最终内容的整链校验
  - `SameAs` + Range + 完整下载后的可信缓存升级
  - 收据授权与跨 Zone `pull-first`
- Fuzz 建议：
  - JWT Header 字段变异：`alg`、`kid`、`iat`、`exp`
  - `inner_path` 变异：重复 `"/@/"`、空 step、标量后续分段
  - Header 变异：缺失、跳号、同名多值、超长 Header

## 7. 待协议澄清项

这些点已经适合写成测试，但当前协议文字仍可能引发不同实现做出不同选择，建议在正式落地前补充为显式规范。

- `0B` 文件应映射为“空 Chunk”还是“空 `ChunkList`”。
- Canonical JSON 中“Unicode NFC 归一化”在实现责任上是否为强制要求，还是只作为推荐约束。
- `kid` 缺失时的兼容行为是否允许；若允许，歧义场景应如何处理。
- `4096` 项容器是否仍推荐完整 parent Header，还是允许直接切 proof 模式。
- `cyfs-cascades` 超过 `6` 时是协议错误还是网关可裁剪后继续。

## 8. 建议的首批回归子集

若首轮只做一套最小但高价值的回归集，建议优先落地下面 12 个用例：

- `OBJ-05`
- `CAN-01`
- `CAN-02`
- `PATH-02`
- `PATH-06`
- `INNER-04`
- `INNER-06`
- `RAW-01`
- `CLIST-03`
- `CLIST-06`
- `PROOF-03`
- `AUTH-07`

这 12 个用例基本覆盖了 CYFS 协议最关键的可信边界和最复杂的交互设计。
