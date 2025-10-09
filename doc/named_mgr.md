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
  - chunk支持半状态，可以带断点续传的写入（系统里比较少的，非事务性的写入操作)
  - 通过chunkreader/chunkwriter,来细粒度的访问chunk的数据
- 管理Link,允许多个objid共享一个实体(目前主要支持chunk)
  - 3中Link， A与B相同，A是B的一部分，ChunkA使用外部File保存（支持相对路径）
  - 针对3种Link，支持反向查询：
    哪些对象与B相同，哪些对象是B的一部分，给定LocalFile，哪些Chunk引用了。
  

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

named_mgr和核心是基于NamedData提供了FileSystem的抽象
实现path->named_data ,path通常具有某种逻辑语义，这是一种可更新的内容发布手段。
实现GC:对于未链接到path的named_data,就是可以释放的named_data
named_mgr也提供了named_store的全部功能，并有可能为了提高访问速度，增加缓存。大部分情况下，我们都不鼓励直接使用named_store

### 从FS的角度看NamedMgr的设计:
- 元数据管理：
  将path指向dirobj / fileobj (如果要发布还需要签名)，该行为潜在的支持了高效率的file move
  设置path的meta_data
- 读objid:最简单基础的操作，易于分布式.objdata都保存在db中
- chunk管理：chunk比较大，有两种管理模式，1种是内部保存，另一种是使用link模式（可能会出错）
- buffer是逻辑上的写缓冲，实际实现的时候是一种支持分布式访问的，基于内存映射文件的实现
- 读操作：
  基于session(context)判断，path指向objid还是指向buffer
  如果指向buffer,则需要访问buffer
- 写操作:
  基于session(context)，得到path指向的buffer(使用不同的锁模式创建buffer)
  执行写操作，改变buffer的内容
  写操作结束后，投递到fileobj更新队列，由cacl进行fileobjid的更新操作,更新后会释放buffer
  cacl chunklist时，有可能构造更易于diff的chunklist(复用已经存在的chunk)
- dirobj的更新
  基于fileobj的更新队列，会
- gc:当一个obj / buffer 不被path引用时，会在gc过程中被释放
  该设计潜在的要求，当设置一个path指向dirobj时，其所有的sub-items都要设置到path里.. 让gc系统可以识别dirobject?
  使用传统的标注法进行分代GC？

### path 与 inner_path
有路径 /movies/dir1/actions/MI6/content/4, 用该路径可以open_chunk_reader成功
- /movies/dir1 -> dir_obj_1
- dir_obj1 存在一个sub_item actions/MI6, 指向fileobjectA
- fileObjectA有确定的filed:content,指向chunklistA
- 最终返回了chunklistA的第4个元素

在该例子中，path为/movies/dir1，inner_path有3层，分别是actions/MI6，content, 4

在进行上述操作时，Named_Mgr对path的互斥性管理：
如果系统中已经有了 `/movies/dir1/actions/Gongfu -> fileobjectB`,此时系统无法将/movies/dir1 绑定到dir_obj_1

### PathMetaObject (可验证内容创建者的path)
对于支持内容发布的路径来说，有时需要额外的签名来更强的声明PATH确实是指向Object的。cyfs://提供内置的PathObject来表达这一点。从设计上，PathObject可以看做是曾经附着在PATH上的MetaData。 
NamedMgr提供了一系列API来管理PathMetaObject.

  
### Link to local file的潜在问题
chunk link模式带来的潜在问题
- Link模式不存在打开Writer
- 文件丢失：打开reader失败
- 文件修改：发现修改时间改变，打开reader失败

local file被修改后，导致一些chunk实际上失效了
  如何正确识别？（不会通过chunkid打开错误的chunk reader):link模式，需要在open时对大小和qcid进行检查
  这些chunk丢失是否会导致严重的问题：一些之前已经 ready的file/dir 变的不ready了:不应该做src chunk,

2选一？ 要么所有的chunk都是store_in_named_mgr,要么全部都是link to local file? 

当需要写入local文件时的痛苦选择
  - local文件已经存在了？
    已经存在的文件和fileobject一样：跳过（如何快速判断？）
    已经存在的文件比fileobject更加新: 本地文件有冲突，需要交给上层处理
    已经存在的文件比fileobject更老：如何只更新需要更新的部分？通过qcid快速判读？
  - local dir已经存在了? 断点续传
    存在文件：冲突，需要交给上层处理
  - 是否需要删除dir中不存在的item

解决冲突的时候，需要同步修改旧的Link
写入完成一个Chunk后，更新Link
在store_in_ndn_mgr模式下，可以可靠的保存一个dir是否是全就绪的 -> 需要更整体的思考与PATH系统的关系
基于named-mgr link模式快速构造dir object
- 通过fs log，快速得到改变的file 
- 能否通过fs的dir meta data,快速的判断local dir是否修改
- 通过link file的大小和最后修改时间判断是否需要计算hash
- 通过link file的qcid判断是否需要计算完整hash
- 计算fileobj时，能基于“上个版本信息”，构建更合理的chunklist

### helper函数(tools.rs)
#### 发布相关
- 发布本地文件成为fileobject到指定路径（相当于保存文件）

### Cacl
- 计算localdir的dirobject,
  以Link模式保存所有的chunk
  或将所有chunk保存在ndn_mgr中
- 通过qcid加快cacl local dir

### copy(sync) dir object from named_mgr

copy（sync) dir object from named_mgr to local
  - 最简单的情况是local是一个空文件
  - 当local已经存在时，有几种覆盖模式
    - 智能同步： 会同步删除，会保留新文件，如果本地文件的修改时间更晚，则本地文件会被重命名后保存。
    - 完全同步： 相当于同步前先删除目标目录再复制


### 在本地的2个named_mgr之间同步数据 
  copy_dir(src_named_mgr,dest_named_mgr,pull_mode)
  如何在多次复制后，在dest_named_mgr中支持多版本？(pull_mode必须是store_in_named_mgr)

### GC的实现
  - 核心是基于PATH->Obj进行标注，并辅助以Obj->Obj追踪标注，未被标注的对象会被删除
  - 基于version的分代GC，保存对象时，记录当前version，GC时每次只会尝试删除某个version以前的Object
  - 分代GC，能尽快删除大量的临时对象？
  - GC的触发时机:定时触发，使用空间告警触发

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
ndn_rouer绑定到一个named_mgr,提供对cyfs://的服务端实现（依赖cyfs-gateway-lib).
ndn_router的主要功能有
- 根据配置正确解析URL
- 正确实现cyfs://定义的，基于身份的基础权限验证（包括付费认证）
- 处理O-Link URL,使用named_mgr的object相关接口访问对象，并处理多级Inner_path
- 处理R-Link URL,使用named_mgr的path相关接口访问对象，并处理多级inner_path
- 处理HTTP Post，并正确实现Push_Chunk的服务端
- 实现cyfs:// rhttp, 允许服务端向客户端发起object get请求(创建可运行的ndn_client)
