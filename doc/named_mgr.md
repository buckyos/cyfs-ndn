# ndn-lib主要组件

分2层
底层：named_store 基于本地文件系统保存named_data （没有GC的概念）
逻辑层: named_mgr, 实现了语义路径(path),并在path的基础上，并在此基础上实现GC
named_mgr也有机会在DCFS实现前，具有分布式特性，可以链接多个device上的named_store
上述两个组件都是本地组件，使用其接口永远不会触发网络行为

行为组件是ndn_client,ndn_router(ndn_server)
ndn_client的接口会可能会改变其绑定的named_mgr
ndn_client本质是对cyfs://协议的客户端实现，应提供所有协议的原始行为封装
ndn_router(ndn_server)是对cyfs://协议的服务端实现，原理上依赖cyfs-gateway的基础架构

## named_store

用数据库保存named-object
用本地文件保存chunk（实体)
没有缓存，所有操作都是尽可能的减少内存占用，尽早/原子/快速的完成磁盘IO。

- 管理named object
  put_object / get_object / remove_object 即可
- 管理chunk （TODO：需要review)
  - chunk支持半状态，可以带断点续传的写入
  - chunkitem是对chunk实体状态的统一描述
  - 通过chunkreader/chunkwriter,来细粒度的访问chunk的数据
- 管理Link?,允许多个objid共享一个实体
  - 支持反向查询（对象图谱）

在有Link的情况下的逻辑
```python
# 只有完完成的chunk可以打开reader
def GetChunkReader(chunkid,offset)
  chunk_item = db.query_chunk_item(chunkid)
  if chunk_item:
    if chunk_item.state == COMPLETE:
      chunk_file_path = ndn_store.get_chunk_path(chunkid)
      return open(chunk_file_path,offset)

  link_data = db.query_link(chunkid)
  if link_data:
    match link_data.type:
    # 当前chunk与另一个chunk相同
    SameAs(real_chunk_id)=> {
      return GetChunkReader(real_chunk_id)
    },
    LocalFile(path,range)=>{
      return open_file(path,range)
    }

  return null

# 只有保存在内部的chunk可以打开writer并进行后续操作，
# 该操作会无视link(本地存储的优先级更高？)
def GetChunkWriter(chunkid):
  chunk_item = db.query_chunk_item(chunkid)
  if chunk_item:
    if chunk_item.state.can_open_writer():#
      chunk_file_path = ndn_store.get_chunk_path(chunkid)
      return open(chunk_file_path,chunk_item.progress.offset)
  
  return null
```

## named_mgr
实现path->named_data (FS)
对于未链接到path的named_data,就是可以释放的named_data

对于特定的容器，当有path->named_container时，会同时保留其children

path通常具有某种逻辑语义，这是一种可更新的内容发布手段。

除此之外，named_mgr也提供了named_store的全部功能，并有可能为了提高访问速度，增加缓存。

### named_mgr的fs介绍
path 与 inner_path
pathObject (可验证内容创建者的path)


### helper函数

发布本定文件成为fileobject到指定路径（相当于保存文件）

计算localdir的dirobject,
  以Link模式保存所有的chunk
  将所有chunk保存在ndn_mgr中


通过qcid加快cacl local dir的方法
  对本地文件计算qcid,如果

在本地的2个named_mgr之间同步数据？ （tools.rs)
  等backup lib处理disk 2 disk的备份时添加



## ndn_client
ndnclient的关键是实现cyfs协议对http://协议的扩展。能正确的根据HTTP头中的cyfs协议扩展对数据进行验证。这种验证默认只在http模式下打开,https模式下会关闭。用户也可以强制打开。

除此之外，ndnclient还提供了一系列Helper函数，将NamedObject从一个named-mgr同步到另一个named-mgr.

get_obj_by_url
get_chunk_reader_by_url (支持chunklist?)

- pull_chunk（list) 从远端同步一个chunk到ndn-client绑定的named_mgr
如果pull的时候，不需要保存在Named_mgr中，则直接下载到local file(会失去进度保存能力和多线程能力）
   用户需要pull完成后手工添加local link才会让link生效
如果pull的时候，named_mgr中chunk已经存在，则直接将chunk复制到local file
如果pull的时候，同时需要保存在named_mgr和local file中，则先下载到named_mgr中，再复制到local file(由于有chunklist，这对大文件还是比较友好的)
如果pull的时候，需要保存在named_mgr中，且已经有已知的local file,会先尝试从local file中copy,再复制

- pull_file 从远端同步一个fileobject（包含chunk)到指定的named_mgr

- download_chunk 从远端下载一个chunk到本地文件（注意与pull_chunk的区别），过程中不影响named_mgr

- 增加函数组 
pull_chunk_to_local
从远程下载chunk到本地，并用Link模式保存在NamedMgr中
pull_file_to_local
从远程下载文件到本地，并用Link模式保存在NamedMgr中
(如果named mgr中已经存在该chunk,则不会创建Link)
pull_dir_to_local
从远程下载文件夹到本地，所有的DirObject都会保存到ndn-client对应的named-mgr中（包含所有的fileobject),所有的file都是用pull_file_to_local下载的。


### 更新文件
假设本地有文件/home/alice/a.data，其fileobject的路径为 /home/alice/a.fileobj, 给定一remote ndn path(指向fileobject),可以判断远程是否更新，并对本地fileobj进行更新

- remote_is_better 判断远程文件于本地文件不同，且更新
- download_fileobj 有更新后，下载远程文件，并更新a.data和a.fileobj







## ndn_router(ndn_server)