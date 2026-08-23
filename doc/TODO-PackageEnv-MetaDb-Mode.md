# TODO：用 MetaDb 开关替代 PackageEnv 严格模式

状态：已完成（2026-08-23）。

## 1. 背景

当前 `PackageEnvConfig::enable_strict_mode` 使用“严格/非严格”描述两种加载行为，但“严格”没有说明约束对象，容易与以下概念混淆：

- `PackageId` 是否指定精确版本或 ObjId。
- 包名是否包含平台 prefix。
- 是否验证包签名或内容哈希。
- 是否允许版本范围和 latest 解析。

特别是从安全语义理解，“严格模式”通常容易被认为包含以下保证：

- 每次加载时重新计算并校验包内容 hash。
- 每次加载时验证 `PackageMeta` 或包内容的数字签名。
- 拒绝没有签名的包，只允许加载可信发布者签名的包。
- 对已经解压的目录逐文件进行完整性验证。

当前 `enable_strict_mode` 并不表达、也不提供上述保证。当前加载路径的核心区别只是：是否必须先通过 metadata DB 得到精确的 `PackageMeta` ObjId 和物理安装目录。`load()` 成功也不能仅凭“strict”这个名称推导出包已经完成 hash、签名或发布者信任校验。

继续使用“严格模式”会给调用者造成错误的安全预期。将其改名为 `enable_meta_db` 不只是命名清理，也是为了准确限定该配置所提供的保证；hash 校验、签名验证和发布者信任策略如果需要，必须分别通过独立配置、接口和错误类型定义，不能隐含在 MetaDb 开关中。

上层实际按当前环境是否启用、部署 metadata DB 选择加载路径。因此应该删除“严格模式”概念，改用直接表达数据来源的 `enable_meta_db`。

本改造与 `PackageId`、`PackagePrefix` 命名规则正交：

- `PackageId/Prefix` 负责生成规范包名和候选包名。
- `enable_meta_db` 负责决定使用哪一种存储查找路径。
- 两者不得相互推断或改变对方的语义。

## 2. 目标配置

将：

```rust
pub enable_strict_mode: bool
```

替换为：

```rust
pub enable_meta_db: bool
```

对应的判断接口使用：

```rust
pub fn is_meta_db_enabled(&self) -> bool
```

不得继续暴露以下名称：

```text
enable_strict_mode
is_strict
strict mode
non-strict mode
```

`is_enable_meta_db()` 不作为推荐命名；Rust 接口统一使用 `is_meta_db_enabled()`。

## 3. 两条加载路径

`PackageEnv::load()` 必须先由统一的 `PackageId/Prefix` 解析逻辑得到规范请求或候选包名，然后只根据 `enable_meta_db` 选择一条加载路径。

### 3.1 `enable_meta_db = true`

使用 metadata DB 精确加载：

```text
PackageId
  -> lock DB / metadata DB
  -> PackageMeta + meta ObjId
  -> pkgs/{canonical_package_name}/{meta_objid_filename}
```

要求：

- metadata DB 是必需依赖。
- DB 文件不存在、无法打开、schema 不兼容或查询失败时必须返回错误。
- 包或匹配版本不存在时返回 NotFound。
- 通过 DB 得到精确 `PackageMeta` 和 meta ObjId 后才能构造物理目录。
- 必须校验请求中的包名、版本、tag、ObjId 与 `PackageMeta` 一致。
- 不得在失败后回退到友好目录或开发目录。
- 不得因为 DB 错误切换到 `enable_meta_db = false` 的路径。

### 3.2 `enable_meta_db = false`

使用无 metadata DB 的目录加载：

```text
PackageId
  -> directory resolver
  -> 开发目录、友好目录或显式 ObjId 目录
```

要求：

- 不得打开、查询或创建 metadata DB。
- 不得先尝试 DB 再回退目录；该模式从一开始就走目录解析。
- 无版本、无 ObjId 的请求可以加载约定的开发/友好目录。
- 显式提供 ObjId 时，可以按 ObjId 构造确定的包目录。
- 不能在没有 DB 的情况下假装完成 latest、tag 或版本范围解析。
- 无法由目录结构准确表达的版本请求必须明确报错，不能忽略版本条件后返回某个目录。

目录布局的最终规范需要在实现前确认，但不能影响 `PackageId/Prefix` 的解析规则。

## 4. Parent PackageEnv

每个 `PackageEnv` 独立使用自己的 `enable_meta_db` 配置：

```text
current env 加载失败
  -> 如果允许查询 parent
  -> 使用 parent 自己的 enable_meta_db 和加载路径
```

子环境不得把自己的 MetaDb 模式强制传递给 parent，也不得根据 parent 是否存在改变当前环境的模式。

是否继续查询 parent 属于环境搜索策略，不属于“是否启用 MetaDb”的定义。

## 5. MetaIndexDb 打开方式

当前 `MetaIndexDb::new(db_path, ready_only)` 没有使用 `ready_only`，并始终通过 `READ_WRITE | CREATE` 打开 SQLite。这会导致查询路径意外创建空 DB，必须一并修正。

建议拆分为两个明确接口：

```rust
MetaIndexDb::open_existing_readonly(path)
MetaIndexDb::create_or_open(path)
```

约束：

- 加载和查询现有索引使用 `open_existing_readonly()`。
- 创建、更新和安装流程使用 `create_or_open()`。
- `enable_meta_db = false` 时两个接口都不得调用。
- `enable_meta_db = true` 且 DB 不存在时，加载必须报错，不能自动创建空 DB。
- DB 文件存在但损坏时必须报错，不能把损坏解释为“未部署 DB”。

## 6. 建议重命名

为了删除“严格模式”遗留语义，建议同步重命名：

| 当前名称 | 建议名称 |
| --- | --- |
| `enable_strict_mode` | `enable_meta_db` |
| `is_strict()` | `is_meta_db_enabled()` |
| `load_strictly()` | `load_from_meta_db()` |
| `dev_try_load()` | `load_from_directory()` |
| `get_pkg_strict_dir()` | `get_pkg_object_dir()` |
| `pkg_strict_dir` | `pkg_object_dir` |

`is_dev_mode` 当前通过 `pkg.cfg.json` 是否存在推导，且没有参与 `load()` 的模式选择。应该删除，或者在另一个需求中重新定义；不得继续用它表示 MetaDb 是否启用。

## 7. 配置迁移

旧配置：

```json
{
  "enable_strict_mode": true
}
```

新配置：

```json
{
  "enable_meta_db": true
}
```

建议迁移规则：

```text
enable_strict_mode = true  -> enable_meta_db = true
enable_strict_mode = false -> enable_meta_db = false
```

需要注意：旧的 `false` 会先尝试 metadata DB，再回退目录；新的 `false` 将完全不访问 DB。这是有意的行为变化，升级说明中必须明确标注。

兼容窗口内可以在反序列化时接受旧字段，但：

- 新旧字段不能同时出现；同时出现应报配置冲突。
- 序列化时只输出 `enable_meta_db`。
- 读取旧字段时应输出一次弃用日志。
- 兼容逻辑应在约定版本后删除。

## 8. 与 PackageId/Prefix 的边界

本 TODO 不修改以下规则：

- Prefix 的格式及合法字符。
- `unique_name` 的格式及合法字符。
- 有 prefix 和无 prefix 请求的候选名称生成规则。
- 版本表达式、tag 和 ObjId 的字符串语法。

MetaDb 和目录两条加载路径必须接收同一个结构化 `PackageId` 解析结果，不得各自使用 `find('.')`、`split('.')` 或 `starts_with()` 重新解释包名。

如果无 prefix 请求会生成“当前平台包”和“通用包”两个候选，那么候选生成顺序由 `PackageId/Prefix` 规则决定；`enable_meta_db` 只决定每个候选通过 DB 还是目录解析。

## 9. 验收测试

至少需要覆盖：

### MetaDb 启用

- DB 存在且记录、物理目录均存在时加载成功。
- DB 不存在时返回明确错误，并且不创建 DB 文件。
- DB 损坏或 schema 不兼容时返回明确错误。
- DB 中不存在请求包时返回 NotFound，不检查友好目录。
- DB 返回的包名、版本、tag 或 ObjId 不匹配时拒绝加载。
- 精确目录不存在时拒绝加载，不回退开发目录。

### MetaDb 禁用

- 即使默认 DB 路径存在，也不打开或查询 DB。
- 无版本请求可以从约定的开发/友好目录加载。
- 显式 ObjId 请求可以从确定的 ObjId 目录加载。
- 无法准确解析的版本、tag 或范围请求返回明确错误。
- 目录不存在时返回 NotFound。

### 正交性

- 同一个带 prefix 的 `PackageId` 能分别进入 MetaDb 和目录加载路径。
- 同一个无 prefix 的 `PackageId` 在两种路径中使用相同的候选包名顺序。
- 切换 `enable_meta_db` 不改变 `PackageId` 的解析结果、prefix 或 `unique_name`。
- current env 和 parent env 可以使用不同的 `enable_meta_db` 配置。

## 10. 完成条件

- 生产代码中不再存在 `enable_strict_mode`、`is_strict()` 或 `load_strictly()`。
- `enable_meta_db` 明确且唯一地选择 MetaDb 或目录加载路径。
- 查询现有 DB 不再隐式创建数据库文件或表。
- MetaDb 关闭时，加载链路不会访问 metadata DB。
- 两条路径共享统一的 `PackageId/Prefix` 解析结果。
- 配置迁移、错误语义和上述测试全部完成。

## 11. 实现与升级说明

本改造已在 `package-lib` 落地：

- `PackageEnvConfig` 的公开字段和序列化输出统一为 `enable_meta_db`，判断接口为 `is_meta_db_enabled()`。
- `load()` 在生成统一候选 `PackageId` 后，只选择 `load_from_meta_db()` 或 `load_from_directory()` 之一；parent 使用自己的配置重新执行选择。
- MetaDb 加载会校验包名、版本、tag、索引 ObjId 和 `PackageMeta` 计算得到的 ObjId，并只加载精确对象目录。
- 目录加载只接受无版本请求或显式 ObjId 请求；不能由目录表达的版本、tag 和范围请求返回明确错误。
- `MetaIndexDb::open_existing_readonly()` 负责只读查询并校验完整性/schema，`MetaIndexDb::create_or_open()` 只用于创建和更新流程。
- 兼容窗口内仍可反序列化旧配置字段；新旧字段同时出现会报冲突，读取旧字段只记录一次弃用日志，序列化只产生新字段。

升级时需要特别注意：旧配置值 `false` 曾表示“先查询 MetaDb，失败后回退目录”；迁移为 `enable_meta_db: false` 后会从一开始只走目录解析，完全不访问 MetaDb。这是有意的不兼容行为变化。
