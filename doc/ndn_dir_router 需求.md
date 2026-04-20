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

## 文件夹的自动对象化
默认文件夹不会对象化（而是作为语义路径存在），但如果文件夹的根目录有 dirobj.meta, 则说明用户希望该文件夹变成一个对象
比如 存在 /images/v2002/a.iso, /images/dirobj.meta
此时对象化后，如果模式为InStore 只会有 /images.dir.cyobj

## 添加任意NamedObject
通过手工创建xxx.cyobj,也可以在该路径位置创建任意NamedObject