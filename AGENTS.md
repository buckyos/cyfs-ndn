# AGENTS

## GitHub Git 依赖规则

- BuckyOS 家族仓库统一遵循此规则：所有从 GitHub Git 仓库引入的源码或包依赖必须显式使用分支名，当前统一为 `main`。Cargo/uv 写 `branch = "main"`，pip/uvx Git URL 写 `@main`，npm Git URL 写 `#main`，克隆依赖仓库时指定 `--branch main`。
- 依赖声明禁止使用 `rev`、提交 SHA 或 `tag` 固定版本，也不要省略分支。修改时同步检查直接依赖、家族仓库之间的传递依赖、构建脚本、CI、测试和文档中的安装命令，避免同一 crate 因来源不同而产生类型冲突。
- 锁文件由包管理器生成的提交 SHA 可以保留，用于记录分支实际解析到的提交；不要手工删除或改写。
