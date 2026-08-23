# PackageId 命名与解析规则

状态：Implemented。

本文定义 `package-lib` 对 `PackageId`、平台前缀和 `unique_name` 的统一语义。文中的“必须”“不得”和“应该”均为解析器、索引、安装器和加载器共同遵循的规范。

## 1. 设计目标

- 同一个逻辑包在不同平台上共享稳定的 `unique_name`。
- 平台相关包通过明确、可验证的 `{channel}-{os}-{arch}` 前缀区分。
- `unique_name` 可以包含 `.`，不能再用“是否包含 `.`”判断有无平台前缀。
- 包查询、依赖解析、安装目录、友好链接和 DID 转换使用同一套解析结果。
- 所有包名都有唯一的规范字符串表示，避免大小写、路径分隔符和不同平台文件系统产生歧义。

## 2. 基本结构

一个完整的 `PackageId` 由包名以及可选的选择条件组成。选择条件只能是版本选择器或精确对象 ID 之一，二者互斥：

```text
PackageId = PackageName
          | PackageName "#" VersionSelector
          | PackageName "#" ObjectId
```

其中：

```text
PackageName  = [PackagePrefix "."] UniqueName
PackagePrefix = Channel "-" OS "-" Arch
UniqueName   = NameLabel *("." NameLabel)
```

示例：

```text
filebrowser.buckyos.ai
filebrowser.buckyos.ai#1.2.3
nightly-windows-amd64.filebrowser.buckyos.ai#1.2.3
filebrowser.buckyos.ai#:latest
filebrowser.buckyos.ai#pkg:<hex-hash>
```

`#` 是 `PackageId` 各部分的分隔符，不属于 `PackageName`。

## 3. PackagePrefix

### 3.1 语义

`PackagePrefix` 表示一个平台相关的发布变体：

```text
{channel}-{os}-{arch}
```

例如：

```text
nightly-linux-amd64
nightly-linux-aarch64
nightly-windows-amd64
nightly-windows-aarch64
nightly-apple-amd64
nightly-apple-aarch64
```

三个字段的含义如下：

| 字段 | 含义 | 当前示例 |
| --- | --- | --- |
| `channel` | 构建或发布通道 | `nightly`、`beta`、`stable` |
| `os` | 目标操作系统的规范名称 | `linux`、`windows`、`apple` |
| `arch` | 目标 CPU 架构的规范名称 | `amd64`、`aarch64` |

`channel` 是包发布策略的一部分。基础库只验证其词法格式，不解释不同 channel 的发布策略。

`channel` 必须满足：

```text
[a-z][a-z0-9_]{0,31}
```

即 channel 只能包含 ASCII 小写字母、数字和 `_`，必须以小写字母开头，长度为 1～32 个字符。channel 中不得出现 `-`，因为 `-` 是 prefix 三个字段之间的分隔符。

`os` 和 `arch` 必须来自基础库维护的注册表。当前注册值为：

```text
os   = linux | windows | apple
arch = amd64 | aarch64
```

增加新的 OS 或架构必须先扩展基础库注册表和测试，不能由调用者传入任意字符串后静默接受。

### 3.2 规范拼写

序列化后的 prefix 必须使用规范拼写：

| 非规范值 | 规范值 |
| --- | --- |
| `macos`、`darwin` | `apple` |
| `x86_64`、`x64` | `amd64` |
| `arm64` | `aarch64` |

这些别名可以在构建目标输入阶段转换，但不得出现在规范化后的 `PackageId` 中。

### 3.3 Prefix 的识别

解析器只检查 `PackageName` 第一个 `.` 之前的字段。该字段必须完整匹配：

```text
Channel "-" OS "-" Arch
```

只有成功解析且 OS、架构均已注册时，才认为 `PackageName` 带 prefix。

不能使用下面的旧判断：

```text
PackageName 包含 "." => 已带 prefix
```

第一个 name label 如果具有三个 `-` 分隔字段的 prefix 外形，则属于 prefix 保留空间。它不能作为普通 `UniqueName` 的第一个 label：

```text
my-cool-app.module       # 非法：第一段与 prefix 语法冲突
nightly-freebsd-amd64.x  # 非法：OS 未注册
nightly-linux-riscv64.x  # 非法：架构未注册
```

需要表达类似名称时，应改用不会与 prefix 冲突的形式，例如：

```text
my_cool_app.module
```

这样可以避免将拼错的平台 prefix 静默解释为平台无关包，也避免未来新增平台时改变旧名称的含义。

### 3.4 默认 Prefix

默认 prefix 在编译 `package-lib` 时根据目标平台生成：

```text
nightly-{target_os}-{target_arch}
```

当前规范化映射为：

```text
macos   -> apple
x86_64  -> amd64
```

默认 channel 当前是 `nightly`。未来如果允许配置默认 channel，配置值也必须通过本规范的 prefix 校验。

## 4. UniqueName

### 4.1 语义

`UniqueName` 是跨平台稳定的逻辑包名，也就是不带 `PackagePrefix` 的 `PackageName`：

```text
PackageName = UniqueName
            | PackagePrefix "." UniqueName
```

因此：

- 无 prefix 时，`PackageName` 与 `UniqueName` 是同一个字符串。
- 有 prefix 时，`UniqueName` 是去掉已校验 prefix 后的剩余部分。
- 合法的 `UniqueName` 再按 `PackageName` 解析，结果一定没有 prefix。
- `UniqueName` 的第一个 label 不得具有三段 `-` 分隔外形 `{x}-{y}-{z}`。该外形属于 prefix 保留空间，无论后面是否还有 `.`、无论三段是否已注册。

```text
my-cool-app              # 非法：第一段是三段外形
my-cool-app.module       # 非法：第一段是三段外形
nightly-linux-amd64      # 非法：第一段是三段外形，且缺少 UniqueName
nightly-freebsd-amd64.x  # 非法：第一段是三段外形，OS 未注册也不能当 UniqueName
```

平台构建变体必须共享同一个 `UniqueName`：


```text
nightly-linux-amd64.filebrowser.buckyos.ai
nightly-windows-amd64.filebrowser.buckyos.ai
nightly-apple-aarch64.filebrowser.buckyos.ai
```

上述三个包的 `UniqueName` 都是：

```text
filebrowser.buckyos.ai
```

不带 prefix 的包名同时表示平台无关的通用包：

```text
filebrowser.buckyos.ai
```

### 4.2 合法字符

`UniqueName` 只允许 ASCII 小写字符，以保证 Linux、Windows 和 Apple 文件系统上的规范形式一致。

每个 `NameLabel` 必须满足：

```text
[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?
```

即：

- 允许小写字母 `a-z`。
- 允许数字 `0-9`。
- 允许 `-` 和 `_`，但不得位于 label 的开头或结尾。
- `.` 只用于分隔 label，不属于 label 本身。
- 每个 label 长度必须为 1～63 个 ASCII 字符。
- 完整 `PackageName`（包含可选 prefix）不得超过 255 个 ASCII 字节。

合法示例：

```text
filebrowser
buckyos_filebrowser
buckyos-dev_filebrowser
filebrowser.buckyos.ai
app2.runtime_core
```

非法示例：

```text
FileBrowser          # 包含大写字母
文件浏览器            # 包含非 ASCII 字符
-filebrowser         # label 以 '-' 开头
filebrowser_         # label 以 '_' 结尾
.filebrowser         # 存在空 label
filebrowser.         # 存在空 label
filebrowser..ai      # 存在空 label
file/browser         # 包含路径分隔符
file\\browser        # 包含 Windows 路径分隔符
file browser         # 包含空格
file#browser         # '#' 是 PackageId 分隔符
file:browser         # ':' 是版本 tag/ObjId 的保留分隔符
file@browser         # 包含未保留给包名的符号
```

### 4.3 明确禁止的字符和名称

`PackageName` 中不得出现：

- 大写字母。
- 非 ASCII 字符。
- 空格、制表符、换行符、NUL 或其他控制字符。
- `/`、`\\` 等路径分隔符。
- `#`、`:`、`@`、`%`、`?`、`&`、`=`、`+`、`*`、引号等非名称字符。
- 连续的 `.`，或者开头、结尾的 `.`。
- 单独的 `.`、`..` 或任何空 label。

为保证 Windows 文件系统兼容，任意 label 都不得等于以下保留设备名：

```text
con  prn  aux  nul
com1 ... com9
lpt1 ... lpt9
```

比较保留设备名时不区分大小写；由于规范包名只允许小写，规范字符串中只会出现小写形式。

### 4.4 UniqueName 的提取

`get_unique_name()` 必须先调用严格的 prefix 解析器：

- 有合法 prefix：返回第一个 `.` 后的全部内容。
- 无 prefix：返回完整 `PackageName`。
- prefix 外形存在但内容非法：返回解析错误，不能猜测。

示例：

| PackageName | Prefix | UniqueName |
| --- | --- | --- |
| `filebrowser` | 无 | `filebrowser` |
| `filebrowser.buckyos.ai` | 无 | `filebrowser.buckyos.ai` |
| `nightly-linux-amd64.filebrowser` | `nightly-linux-amd64` | `filebrowser` |
| `nightly-windows-amd64.filebrowser.buckyos.ai` | `nightly-windows-amd64` | `filebrowser.buckyos.ai` |

不得无条件删除第一个 `.` 之前的内容，也不得通过 `split('.').last()` 只保留最后一个 label。

## 5. 版本选择器和 Tag

### 5.1 版本选择器

版本选择器位于第一个 `#` 后，使用现有 Rust `semver` 库支持的精确版本或 `VersionReq` 语法。

精确版本遵循 SemVer：

```text
1.2.3
1.2.3-alpha.1
1.2.3+build250326
1.2.3-alpha.1+build250326
```

版本范围示例：

```text
>=1.2.0
>=1.2.0, <2.0.0
^1.2.3
~1.2.3
*
```

版本表达式中的合法符号由 `semver` 语法决定，可能包含：

```text
0-9  A-Z  a-z  .  -  +  <  >  =  ^  ~  *  ,  ASCII 空格
```

字符合法不代表表达式合法；最终必须由 `semver::Version` 或 `semver::VersionReq` 完整解析。制表符、换行符和其他 Unicode 空白不得出现。

### 5.2 版本 Tag

Tag 使用 `:` 附加在版本表达式之后：

```text
PackageName#VersionSelector:Tag
PackageName#:Tag
```

示例：

```text
filebrowser#1.2.3:stable
filebrowser#>=1.2.0, <2.0.0:beta
filebrowser#:latest
```

Tag 必须满足：

```text
[a-z0-9](?:[a-z0-9._-]{0,61}[a-z0-9])?
```

- 只允许 ASCII 小写字母、数字、`.`、`_`、`-`。
- 长度为 1～63 个字符。
- 不得以 `. _ -` 开头或结尾。
- 一个版本表达式中最多有一个 `:` tag 分隔符。

以下形式非法：

```text
filebrowser#           # 空版本、无 tag、无 ObjId
filebrowser#1.2.3:     # 空 tag
filebrowser#:          # 空 tag
filebrowser#1.2.3:A    # tag 含大写字母
```

## 6. 精确 ObjectId

精确对象 ID 与版本选择器互斥：有 `ObjectId` 时不再携带版本选择器，有版本选择器时不再携带 `ObjectId`。

```text
PackageName#ObjectId
```

规范形式必须是可被 `ObjId::new()` 解析、且对象类型为 `pkg` 的类型化 ID：

```text
pkg:<lowercase-hex-hash>
```

例如：

```text
filebrowser#pkg:bcc479e2547e3ce5c6805ec12cffdb460e2f5856dda3ec600e27f0de570e248a
```

要求如下：

- 类型必须是 `pkg`，不能使用其他对象类型。
- hash 必须使用小写十六进制规范形式。
- hash 的长度和内容必须通过 `ObjId` 校验；当前包对象通常使用 SHA-256，即 64 个十六进制字符。
- 序列化时必须输出类型化规范形式。
- 不再将“任意 ASCII 字母数字串”直接当作精确对象 ID，因为它会与版本表达式产生歧义。
- 紧凑 Base32 ObjId 如需兼容，可以在输入边界解析，但必须立即规范化，不能作为 `PackageId::to_string()` 的输出。

`PackageId` 最多允许一个 `#`。版本选择器和对象 ID 不能同时出现。下面的形式非法：

```text
filebrowser##pkg:abcd
filebrowser#1.2.3#pkg:abcd
filebrowser#>=1.2.0#pkg:abcd
```

## 7. 规范化和比较

- `PackageId::parse()` 必须校验整个输入，不能只解析前缀后忽略剩余字符。
- 包名和 tag 不执行自动大小写转换；非规范输入直接报错。
- OS/架构别名只允许在构建环境转换，不允许在 `PackageId::parse()` 中静默改写。
- 精确版本和版本范围由 `semver` 库生成规范字符串。
- ObjectId 统一序列化为类型化小写十六进制形式。
- 两个 `PackageId` 的规范字符串相同，才表示同一个包选择条件。
- 路径、URL 或 JSON 中的转义不属于 `PackageId` 本身；必须先完成外层解码，再按本文校验。

## 8. 加载语义

### 8.1 显式平台 Prefix

请求中包含合法 prefix 时，它表示调用者要求一个精确平台变体：

```text
load("nightly-windows-amd64.filebrowser.buckyos.ai#1.2.3")
```

加载器只查询该名称，不得替换为当前平台 prefix，也不得自动回退到无 prefix 的通用包。

### 8.2 无 Prefix

请求中没有 prefix 时，加载器按以下顺序查询：

```text
1. {current_prefix}.{unique_name}  # 当前平台变体
2. {unique_name}                   # 平台无关通用包
```

例如 Windows AMD64 环境中：

```text
load("filebrowser.buckyos.ai#1.2.3")

1. nightly-windows-amd64.filebrowser.buckyos.ai#1.2.3
2. filebrowser.buckyos.ai#1.2.3
```

版本选择器、tag 和 ObjectId 在生成候选名称时必须原样保留。

只有第一个候选明确返回“未找到包或未找到匹配版本”时，才能尝试通用包。元数据损坏、签名失败、对象哈希不匹配、权限错误或 I/O 错误不得触发静默回退，以免掩盖安全和数据一致性问题。

MetaDb 模式和目录模式可以使用不同存储目录，但必须共享上述名称解析与候选顺序，不能让相同 `PackageId` 在两种模式中具有不同的命名语义。

## 9. 安装目录与友好名称

解析完成后，所有目录逻辑必须使用结构化字段：

```text
strict package name = [prefix "."] unique_name
friendly name       = unique_name
```

例如：

```text
PackageName  = nightly-windows-amd64.filebrowser.buckyos.ai
UniqueName   = filebrowser.buckyos.ai
FriendlyPath = {work_dir}/filebrowser.buckyos.ai
```

不得用以下字符串启发式逻辑生成友好名称：

```text
name.find('.')
name.split('.').last()
name.starts_with(current_prefix)
```

严格安装目录可以继续采用：

```text
{work_dir}/pkgs/{canonical_package_name}/{object_id_filename}
```

## 10. 实现状态与兼容边界

`package-lib` 已实施以下规则：

- `PackagePrefix` 和 `PackageName` 负责严格解析、注册值校验及规范序列化。
- `PackageId::parse()` 校验完整输入，并保证版本选择器和精确 ObjectId 互斥。
- `PackageId::load_candidates()` 统一生成平台变体和通用包候选。
- MetaDb 与目录加载共用同一候选顺序；只有“未找到”允许继续查询通用候选或 parent env，损坏数据及其他错误不会触发静默回退。
- 安装目录使用已校验的规范 `PackageName`，友好路径使用完整 `unique_name`。
- 默认构建 prefix 对 OS/arch 执行注册表映射；环境配置中的自定义 prefix 也必须通过同一解析器。

为减少现有调用者的源码迁移成本，`PackageId` 暂时保留公开的 `name: String`、`version_exp: Option<VersionExp>` 和 `objid: Option<String>` 字段。外部输入必须通过 `PackageId::parse()` 构造；直接写字段无法获得本规范的合法性与互斥性保证。

底层 metadata DB 查找和对象目录仍可识别历史 Base32 ObjId，以读取旧数据；该兼容只存在于存储边界，`PackageId::parse()` 和 `PackageId::to_string()` 不接受或输出这种形式。

## 11. 兼容性与迁移

严格规则会改变一部分旧名称的含义，迁移时必须显式处理：

1. 盘点 metadata DB、lock 文件、依赖声明和安装目录中的现有包名。
2. 找出“包含 `.` 但第一个 label 不是合法 prefix”的名称；新规则会将它们识别为无 prefix 的 `UniqueName`。
3. 找出包含大写字母、Unicode、路径字符、空 label 或 Windows 保留名称的包。
4. 找出依赖裸字母数字 ObjId 的 `PackageId`，转换为类型化 `pkg:<hash>`。
5. 找出 `name#Version#ObjectId`，根据调用意图改为版本选择或精确 ObjectId 选择之一。
6. 迁移期间如需 legacy fallback，必须记录明确的弃用日志；不得让 legacy 结果覆盖新规则的精确匹配结果。
7. 发布方、索引服务和加载端必须在同一兼容窗口内升级，避免同一个字符串在不同节点上产生不同解释。

## 12. 必需测试用例

基础库至少应覆盖以下行为：

| 输入 | 结果 |
| --- | --- |
| `filebrowser` | 合法；无 prefix；`unique_name=filebrowser` |
| `filebrowser.buckyos.ai` | 合法；无 prefix；完整名称都是 `unique_name` |
| `nightly-linux-amd64.filebrowser` | 合法；识别 Linux AMD64 prefix |
| `nightly-windows-amd64.filebrowser.buckyos.ai#1.2.3` | 合法；保留多段 `unique_name` 和版本 |
| `nightly-macos-x86_64.filebrowser` | 非法；使用了非规范 OS/arch |
| `nightly-freebsd-amd64.filebrowser` | 非法；OS 未注册 |
| `nightly-linux-riscv64.filebrowser` | 非法；arch 未注册 |
| `my-cool-app.module` | 非法；第一段占用 prefix 保留语法 |
| `.filebrowser` | 非法；空 label |
| `filebrowser.` | 非法；空 label |
| `filebrowser..ai` | 非法；空 label |
| `FileBrowser` | 非法；包含大写字母 |
| `file/browser` | 非法；包含路径分隔符 |
| `con.tools` | 非法；包含 Windows 保留设备名 |
| `filebrowser#1.2.3:stable` | 合法；精确版本和 tag |
| `filebrowser#:latest` | 合法；默认版本范围和 tag |
| `filebrowser#` | 非法；空选择器 |
| `filebrowser#abc123` | 非法；不再把任意字母数字串当作 ObjId |
| `filebrowser#pkg:<valid-hash>` | 合法；精确包对象 |
| `filebrowser#1.2.3#pkg:<valid-hash>` | 非法；版本选择器与 ObjectId 互斥 |

加载测试还必须验证：

- 有合法 prefix 的请求只查询精确名称。
- 无 prefix 的请求先查询当前平台名称，再查询通用名称。
- `filebrowser.buckyos.ai` 不会被误认为 prefix 为 `filebrowser`。
- 平台候选发生校验错误时不会静默回退到通用包。
- MetaDb 模式和目录模式得到相同的候选包名顺序。
- 安装后的友好路径保留完整 `unique_name`。

## 13. 基础类型与统一接口

命名结构由以下基础类型表示：

```rust
struct PackagePrefix {
    channel: String,
    os: PackageOs,
    arch: PackageArch,
}

struct PackageName {
    prefix: Option<PackagePrefix>,
    unique_name: String,
}

struct PackageId {
    // 为源码兼容暂时保留字符串字段；parse() 使用 PackageName 校验它。
    name: String,
    version_exp: Option<VersionExp>,
    objid: Option<String>,
}
```

统一接口包括：

```text
PackagePrefix::parse()
PackageName::parse()
PackageName::unique_name()
PackageName::with_prefix()
PackageName::without_prefix()
PackageName::is_with_prefix()
PackageId::parse()
PackageId::load_candidates(current_prefix)
```

元数据索引、依赖解析、安装和加载只能使用这些接口，不应再次通过 `find('.')`、`split('.')` 或 `starts_with()` 推断命名结构。

## 14. 已确认的规范决定

以下兼容性决定已经纳入解析器和测试：

1. 包名统一限制为 ASCII 小写；非规范输入直接报错，不自动转小写。
2. 每个 label 最长 63 字符，完整 `PackageName` 最长 255 字节。
3. `x-y-z.name` 形式的首个 label 全部保留给 prefix，因此 `my-cool-app.module` 非法。
4. Apple 平台的规范 OS 名称使用 `apple`；构建脚本将 Cargo 的 `macos` 目标映射为 `apple`。
5. 无 prefix 请求采用“当前平台变体优先、平台无关通用包其次”的查询顺序。
6. 显式 prefix 请求禁止自动回退到通用包。
7. 精确对象只接受类型化 `pkg:<hash>`，不接受裸字母数字 ObjId。
8. `unique_name` 允许任意数量的 `.` 分段，并在友好目录和 DID 相关逻辑中完整保留。
9. 版本选择器与精确 ObjectId 互斥，不支持 `name#Version#ObjectId`。
