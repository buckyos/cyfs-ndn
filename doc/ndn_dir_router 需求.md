# ndn_dir_server

类似传统的static dir http server,通过简单配置，就可以构造一个支持cyfs://的 http server

## 初始化
- 语义根目录
- NamedStoreMgr配置路径
- 模式：LocalLink or InStore
- URL路径前缀 比如/ndn/readme -> /readme
- 是否支持域名里的objid发现
- 可选的私钥，用来构造path-obj jwt

## 支持 O-Link
通过NamedStoreMgr作为底层，直接支持O-Link

## 支持R-Link
核心逻辑：如果在语义根目录下存在
/readme.txt
/readme.txt.cyobj  （包含对象化后的信息，和必要的path-object-jwt)

则: readme.cyobj是readme.txt对象化后的信息，可以用来构造cyfs://所需要的必要信息
通过 /ndn/readme.txt 可以访问

管理员可以随意把文件/目录复制到该目录，该ndn_dir_server会定期自动进行对象化（更新xxxx.cyobj)
根据模式的不同，会选择是否保留原始文件
选择 LocalLink模式：保留 ，选择InStore模式，原始文件在保存到NamedStoreMgr中后，会被删除

文件在自动对象化的过程中，会向根目录寻找 object.template,根据obj-type找到模板

### object.template格式
`object.template` 只在语义根目录读取一次，作用于本次扫描中所有自动对象化的文件和目录。当前实现按 obj-type 查找模板，并只读取对应对象里的 `meta` 字段作为默认元数据。

```json
{
  "cyfile": {
    "meta": {
      "content_type": "application/octet-stream",
      "tags": ["example"]
    }
  },
  "cydir": {
    "meta": {
      "collection": "images",
      "visibility": "public"
    }
  }
}
```

- `cyfile.meta` 会应用到自动生成的 `FileObject`。
- `cydir.meta` 会应用到自动生成的 `DirObject`。
- `meta` 必须是 JSON object；其中的 key/value 会原样写入目标对象。
- 模板中的其它字段目前不会被合并到对象中。
- 文件对象的 `meta` 在 `FileObject` 序列化时是 flatten 的：`cyfile.meta` 里的字段会出现在 `obj_json` 顶层，而不是嵌套在 `meta` 字段下。
- 目录模板的 `cydir.meta` 会写入内存中的 `DirObject.meta`；目录 sidecar 使用 `DirObject::gen_obj_id()` 生成的规范 JSON，最终 `obj_json` 以该规范 JSON 为准。

## 文件夹的自动对象化
默认文件夹不会对象化（而是作为语义路径存在），但如果文件夹的根目录有 dirobj.meta, 则说明用户希望该文件夹变成一个对象
比如 存在 /images/v2002/a.iso, /images/dirobj.meta
此时对象化后，如果模式为InStore 只会有 /images.cyobj

### dirobj.meta格式
`dirobj.meta` 是目录对象化标记，也是该目录 `DirObject` 的本地覆盖配置。当前实现支持如下格式：

```json
{
  "name": "images",
  "meta": {
    "collection": "images",
    "version": "v2002"
  }
}
```

- `name` 可选，字符串；存在时覆盖生成的 `DirObject.name`。
- `meta` 可选，JSON object；存在时覆盖/补充该目录 `DirObject.meta`。
- `object.template` 的 `cydir.meta` 会先应用，`dirobj.meta` 里的同名字段后应用，因此 `dirobj.meta` 优先级更高。
- `dirobj.meta` 的其它字段目前会被忽略；文件为空、不是合法 JSON、或 `meta` 不是 object 时，不会中断对象化。

### 自动生成的.cyobj格式
自动对象化会在源路径旁边写入 `<name>.cyobj` sidecar。该文件不是 `meta` 模板，而是对象化结果记录：

```json
{
  "obj_type": "cyfile",
  "obj_id": "cyfile:...",
  "obj_json": {
    "name": "readme.txt",
    "size": 123,
    "content": "chunk:...",
    "content_type": "text/plain"
  },
  "path_obj_jwt": "...",
  "source_qcid": "...",
  "source_mtime": 1710000000,
  "source_size": 123,
  "source_template_qcid": "tpl:..."
}
```

- `obj_type` 是内嵌对象类型，如 `cyfile` 或 `cydir`。
- `obj_id` 是 `obj_json` 的对象ID。
- `obj_json` 是实际 NamedObject JSON；自动对象化时模板/目录覆盖后的元数据会体现在这里。
- `path_obj_jwt` 只有配置了签名私钥时才会生成。
- `source_qcid`、`source_mtime`、`source_size` 用于扫描器判断 sidecar 是否需要刷新。目录对象化时 `source_qcid` 保存目录签名，`source_mtime` 为空，`source_size` 为目录总大小。
- `source_template_qcid` 保存本次对象化使用的根目录 `object.template` 签名；模板内容变化时，即使源文件/目录内容未变，也会刷新 sidecar。

## 添加任意NamedObject
通过手工创建xxx.cyobj,也可以在该路径位置创建任意NamedObject
