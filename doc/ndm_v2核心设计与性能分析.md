# NDM 核心设计与性能分析

文件系统使用的基本假设:大部分数据都是 创建->读取循环，少部分数据，频繁修改。修改模式场景的是Append

- coding场景，build过程大量创建小文件，使用后批量删除
- 照片管理场景，查看多年前的照片

## 核心数据结构的设计

- 使用经典的DentryItem + IndexNode(inode) 模型构建目录结构，DentryItem和IndexNode都保存在RDB中
  - `parent/child/child_child/` 目录结构:parent_inode <- dentry_item -target-> child_inode  <- dentry_item -target-> child_child_inode 
- 对DirNode的list操作，需要`select * from dentries where parent=dirnode`, 这是fsmeta里最慢的一个操作,也是很多优化的重点
  - 主要涉及到添加DentryRecord行为(`在目录中添加一个新item`)，涉及到去重处理，都会需要list
  - 建立Cache,使用dirnode添加rev来让cache管理更轻松 
- 通过NamedObject体系里的标准FileObject和DirObject,表达文件系统里的不可变部分
- IndexNode->NamedObject被称作物化(Finalize)
- 通过ChunkList保存文件的内容
- 未物化的文件内容保存在FileBuffer中，已经物化的文件内容保存在named_store(chunk_manage中）
- inode`支持Overlay Base On NamedObject`模式:
  - DirObject Children + DentryItem Map + => Real Dir Chidren
  - FileObject ChunkList + DiffList@FileBuffer => Real Chunk Lilst 
- 只支持Symlink,不支持hardlink,所以inode只会成为一个dentry_item的target(使用双向绑定来强化?)
- 为了支持NamedObject的GC,在fsmeta中有ObjStat表 =>（应该迁移回obj_store,由store_mgr来管理Object的GC)

```rust
pub enum DentryTarget {
    IndexNodeId(IndexNodeId),
    SymLink(String),
    ObjId(ObjId),
    Tombstone,
}

pub struct DentryRecord {
    pub parent: IndexNodeId,
    pub name: String,
    pub target: DentryTarget,
    pub mtime: Option<u64>,
}

pub enum NodeState {
    /// Directory node (usually committed; delta lives in dentries)
    DirNormal,
    DirOverlay, // overlay mode (upper layer only)

    NewFile,//TODO:新文件，还没有分配FileBuffer
    /// File node: currently writable, bound to FileBuffer
    Working(FileWorkingState),
    /// File node: closed, waiting to stabilize (debounce)
    Cooling(FileCoolingState),
    /// File node: hashed & published via ExternalLink (content address stable)
    Linked(FileLinkedState),
    /// File & Object node: data promoted into internal store (chunks finalized)
    Finalized(FinalizedObjState),
}

pub struct NodeRecord {
    pub inode_id: IndexNodeId,
    pub state: NodeState,
    pub read_only: bool,
    pub base_obj_id: Option<ObjId>, // committed base snapshot (file or dir)
    pub rev: Option<u64>,           // only for dirs
    // metas:
    pub meta: Option<Value>,
    // leases:
    pub lease_client_session: Option<ClientSessionId>,
    pub lease_seq: Option<u64>,
    pub lease_expire_at: Option<u64>,
}

pub struct FileObject {
    pub size: u64,
    pub content: String, //chunkid or chunklistid
    pub meta: HashMap<String, serde_json::Value>,
}

pub struct DirObject {
    pub meta: HashMap<String, serde_json::Value>,
    pub total_size: u64, //包含所有子文件夹和当前文件夹下文件的总大小
    pub file_count: u64,
    pub file_size: u64, //不包含子文件，只计算当前文件夹下文件的总大小
    pub object_map: SimpleObjectMap, // item-name -> obj_id, 结构和dentry_item类似
}

//FileBuffer Overlay模式:,非Overlay模式就是一个标准的连续文件。
pub struct DiffChunkList {
    pub base_chunk_list: ObjId, // Base ChunkList,不可变
    pub diff_file_path: PathBuf, //可变的文件，FileBuffer实际保存
    //决定了diff_file的排布： 
    pub chunk_indices: Vec<u64>, 
    pub chunk_ids: Option<Vec<ChunkId>>,
}

```

### 上述核心数据结构的操作接口与事务性

- named_object没有修改，其构造，查找，删除接口在named_store里，完全不依赖fsmeta


#### inode的管理接口

- 针对单个inode的CRUD接口
- 高级:同时操作多个inode的事务性支持
- 基于rev,对DirNode的修改可以进行“乐观锁控制"
- clone:
  - FileNode的Clone，会涉及到FileBuffer的Clone.FileBuffer处于Working状态时会Clone失败
  - DirNode的Clone成功没有FS意义，FS语义必须吧child dentry item都复制过来才算目录clone成功
  

#### dentry 管理(目录sub item管理)
- list 接口 （注意返回parent DirNode.rev)
- get_dentry(parent_node_id,child_name) 通常用来做冲突检查
- 针对dentry item的CRUD接口
  - Read 通过parent_inode + item_name，可以查询得到dentry
    - 考虑到dentry和inode的一对一关系，是不是也可以通过inode反查dentry?
  - Create 需要更新Parent DirNode.rev，并做乐观锁控制
  - Delete 需要更新Parent DirNode.rev,并做乐观锁控制（因为没有hardlink,可以顺手删除TargetInode?)
    在Layout模式下，Delete可能变成Create Tombstone Dentry
  - Update（叫RepalceTarget更好)
    - 物化流程，物化完成后，改变Dentry的Target，
    - 实例化流程，实例化后，改变Dentry的Target
    - 删除流程，改变Dentry的Targt
  - 注意:Create/Delete/Update 都需要更新parent DirNode的rev
- 目录管理接口
  - rename(move) 修改一个dentry item 的 parent_inode,这里还需要修改2个inode的rev
  - snapshot 其本质是`基于src目录构建DirObject(物化),并将DirObject bind到target目录（ReadOnly模式）`,相对标准的物化流程，snapshot可能不会让src目录物化，而只是构建DirObject
  - copy_file 根本上是dentry_item创建 + inode.clone
    - dest_inode = src_inode.clone()
    - create dest_dentry_item -target->dest_inode
  - copy_dir 涉及到所有dentry_item的clone
    - dest_dirnode = src_dirnode.clone()
    - create dest_dentry_item ->target->dest_dirnode  
    - 遍历src_dirnode的dentry item,挨个clone()，如果dentry_item也是dir,会递归触发 
    - 上面的产品语义可能需要跑很久，因此主要流程应该在客户端驱动，可以减少系统复杂性并在内核稳定的基础上，有客户端提供更多的复制选项

#### FileBuffer 管理

FileBuffer Service提供可靠的系统写入/读取缓存，配合客户端逻辑一起，提供告诉的新文件读写支持。
系统里，文件的的写入流程如下
- alloc
{
  file_node = fsmeta.create_new_file()
  file_node.lease_token = client.session_token
  filebuffer_handle = fb_service.alloc(file_node.node_id,file_node.base_objid,file_node.lease_token)
  dentry_item = fsmeta.create_dentry_item(parent_node_id,file_node)
  return OK()
}
- open writer
{
  filebuffer_handle = fb_service.get_filebuffer(file_node.nodeid)
  filebuffer_handle = fb_service.open_writer(filebuffer_handle,client.session_token)
  base_chunklist_reader = named_store.get_chunklist_reader(filebuffer_handle.base_objid)
  //filebuffer_handle.diff_file_path 说明这总是本地的
  file_writer = new DiffChunkListReader(filebuffer_handle.diff_file_path,base_chunklist_reader)
  return file_writer
}
- write
{
  file_writer.write(xxx)
  file_writer.seek(xxx)
  file_writer.write(xxx)
}
- close 
{
  //Local模式就是将上面写入的内容刷入本地磁盘
  //Remote模式则需要通过网络操作，确保将diff_path的内容完全同步上fb_service()
  file_writer.flush(file_writer)
  //要求fb_service将filebuffer_handle关闭，后续再写入数据就要重新打开了
  fb_service.close(filebuffer_handle) 
  //修改状态，并释放lease_client_session
  fsmeta.changeNodeState(file_node.node_id,COOLING)
}

##### FileBuffer Service的接口

- alloc(inode_id,filesize,base_chunk_list,token),//申请filebuffer 通常由fsmeta调用
- get_filebuffer(inode_id) -> filebuffer_handle 
- open_writer(filebuffer_handle,token)
- open_reader(filebuffer_handle)
- flush_filebuffer(filebuffer_handle)
- close_filebuffer(filebuffer_handle)
- clone_filebuffer(src_handle,dest_filenode_id)

#### 运维接口，通常内部调用

- cacl_name(inode_id)
- finalize(inode_id) 

#### 高阶业务接口

原理上高阶业务接口，通过上述基础接口+事务都能在客户端实现。
高阶业务接口的目
- 减少RPC的次数+本地事务，提高整体性能。
- 协助处理复杂的Overlay逻辑
- 通过path:NdmPath，代替parent_dirnode_id的接口，少了一次resolve(NdmPath)得到dirnode_id的过程
- 处理DirNode(BaseDirObject)的情况（能否把这个处理完全移动到客户端，来保持fsmeta实现的简单性?)

下面是fsmeta目前支持的高阶业务接口

- resolve_path_ex(path:NdmPath,sym_count: u32) -> Result<(node_id | obj_id,Option<inner_path>)>
  这也是一个复杂的函数，会一路检查NdmPath,自动展开符号链接，最后确定path是否存在
  当出现`inode -> inode -> inode(DirObjectA) / DirObjectB / DirObjectC(Parent) / filename` 这种路径时，展开的时候需要记载DirObjectA,DirObjectB,DirObjectC,才可以确定路径是否存在（不会检查filename是否存在)
  系统为了加速解析速度，为该函数建立了复杂的cache
  因为DirObject的不可变性，因此$dirobjid/inner_path 的解析结果也可以缓存，实现在named_store_mgr中
- ensure_path(path:NdmPath) -> dirnode_id ，这是一个复杂的写操作
  类似mkdir -p ,完成path上所有的目录的获取或创建，最后得到dirnode_id,随后可以基于该dirnode_id添加item
  实现时，有类似resolve_path_ex内部的同质展开循环，来
  该函数表达出一定的事务性，提高了fsmeta的实现复杂度（TODO:把整个流程移动到客户端去）
- open_file_writer 合并了文件创建和打开已有文件
- open_file_reader (可以去掉，客户端有resolve_path_ex就非常好处理了)

## 场景一、build
### 创建临时小文件

具体流程参考FileBuffer管理

#### fb_handle_id = fsmeta.open_file_writer("parent_dir","filename") 

> meta看来的树结构 `inode -> inode -> DirObject / DirObject / DirObject / filename`

总是要确保文件能打开成功(获得写权限)

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过inode_id，查询得到parent_inode，判断状态是否可读。上面这种路径，[metadb.read 1]
  如果filename处已经有同名DirObject那么是会失败的，如果有同名filename,需要得到old_file_inode,要看flag是否允许覆盖
- 在DirObject模式下，如果有需要，需要填补路径上的parent_inode,parent_parent_inode,  
  例子的这种情况，需要创建3个inode. 注意DirObject的child只能是Object,不能是inode,因此必须从DirChildItem变成DentryItem
- 创建dentry item filename_item -> file_inode -> fb_handle(Option<BaseFileObject>)（下面操作是事务) [metadb.write 3]
  - 创建的新inode,判断是否需要有BaseFileObject? [metadb.write 1]
    Flag是否是新文件，新文件一定不需要BaseFileObject（小文件大概率不要)
    旧路径上的old_file_inode如果已经指向了一个FileObject,需要BaseFileObject
    通过DirObject定位到了一个FileObject,需要BaseFileObject  [是否需要 DirObjId_inner_path -> objid的cache?]
  - 申请file_write_lease,主要是记录现在个inode暂时分配给了“那个lease-session,多长时间” [metadb.write 1]
  - 添加目录项 upsert_dentry(parent_dir_id, &name, Target::IndexNodeId(fid) [metadb.write 1]
  
- 基于lease_session + file_inode + fb_id，返回filebuffer_handle

总计: [metadb.read 2,metadb.write 3]

#### fb = fb_service.open(fb_handle)

- 根据filebuffer_handle里的BaseFileObject信息，打开BaseReader（小文件大概率不要)
- local_fb_service.alloc_buffer(fb_id) [fs_service_db.write 1,fs_service_fs.createfile 1]
  根据filebuffer_handle选择合适的fb_service(单机版必然是fs_meta所在的机器，未来主要是选择本机)分配FileBuffer
- 返回filebuffer

总计:[fs_service_db.write 1,fs_service_fs.createfile 3]

#### fb.write

- 本地文件操作

#### fb.close

- 本地文件操作，如果文件足够小，可以利用OSBuffer还在的时候，直接计算chunkId

#### fb.close_file(fb_handle)

fb_service会在内部调用fsmeta.close_file,调整inode状态

根据fb_handle的信息，要求meta更新file_inode的状态:

- 释放lease_session
- 更新file_node的状态

总计:[metadb.write 2]

### 小文件被另一个应用读取

```
ndm.open_file_reader:
  file_resp = fsmeta.open_file_reader("parent_dir/filename")
  if file_resp.is_filebuffer() {
      base_reader= open_reader(store_layout.get_obj(file_resp.fb_buffer.base_obj_id))?
      fb_buffer_instance = fb_service.open_reader(fb_handle.id)?
      return new FileBufferReader(fb_buffer_instance,base_reader)
  } else {
    return store_layout.open_reader(file_resp.objid,file_resp.inner_path)
  }

...
reader = ndm.open_file_reader("parent_dir/filename")
data = reader.read_to_end()
drop(reader)


```

#### fb_handle = fsmeta.open_reader("parent_dir/filename")

> meta看来的树结构 `inode -> inode -> inode_base_dir -> inode_base_dir -> inode_base_dir (Parent) / filename`

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过get_dentry(parent_dir_node_id,"filename"),得到file_node [metadb.read 1]
- 此时file_node指向一个filebuffer_handle（这个流程肯定不会触发物化搬运)

总计:[metadb.read 2]

#### 如果filenode已经处于Linked状态,open_chunk_read_by_id(chunk_list[0])

如果本地cache已经有该chunk,直接使用
否则走 store_layout.select流程，向named_store请求chunk

#### 如果只有filebuffer_handle

```
fb = fb_service.open_reader(fb_handle)
```

- 根据fb_handle.id 得到fb的信息 [fs_service_db.read 1]
- 根据filebuffer_handle里的BaseFileObject信息，打开BaseReader（小文件大概率不要)
- 通过fb_service打开fb_buffer_reader
- 返回真正的reader,这个reader会根据ReadRange使用BaseRader + fb_buffer_reader (分层读取)

总计:[fs_service_db.read 1]

### build结束，通过list得到所有的小文件

```
dir_result = fsmeta.open_dir("parent/")
dentries = fs_meta.list(dir_result.inode)
if dir_result.base_dir_obj_id {
  dentries.merge(store_layout.get_obj(dir_result.base_dir_obj_id ).children)
}
return dentries
```
> meta看来的树结构 `inode -> inode -> inode -> inode -> inode (Parent)

parent_node是一个有BaseDirObject的DirNode

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- metadb.list_dentry(parent_inode_id) , 得到所有的children
- 得到BaseDirObject的第一层所有children
- 合并上面的两个children

注意这里支持游标(?需要缓存上面的合并结果)

总计 [metadb.read 1,metadb.select 1]

### 复制build结果到汇总目录

#### 删除parent_dir : fsmeta.remove_dir(parent_dir)

> meta看来的树结构 `inode -> inode -> inode_base_dir -> inode_base_dir -> inode_base_dir (Parent)`


- 从路径得到parent_parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过parent_parent_dir_inode,得到parent_parent_dir_node [metadb.read 1]
- 因为parent_parent_dir_node有Base Dir Object,且该Base Dir Object里含有parent item，所以通过添加墓碑item实现删除 [metadb.write 1]]

总计 [metadb.read 2,metadb.write 1]

## 场景二、浏览家庭照片库

path: /home/lzc/photos/2016/香港旅游

```
dir_result = fsmeta.open_dir("/home/lzc/photos/2016/香港旅游")
if dir_result.is_dir_object () {
  //物化分支，后续list不需要再和fsmeta通信
} 
```

因为照片库里的照片，很多早就已经物化，因此

> meta看来的树结构 `inode -> inode -> inode -> DirObject` / 香港旅游
- 从路径得到/home/lzc/photos/2016/ 的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通用inode_id得到DirNode [metadb.read 1]
- 该DirNode指向DirObject

后续在该文件夹内的浏览，基本是重复下面流程,不用和metadb打交道
```
named_store = store_layout.select(DirObjectId | FileObjectId)
named_store.get(DirObjectId | FileObjectId)
for fileObject in dir_object.get_children()
    fileObject.get_content_reader()

```

总计 [metadb.read 2]


## 场景三、同步盘场景


用户把ndm的一个目录，挂载到多个设备。用户的期望
1. 常规情况下，能正常使用
2. 当设备断网的情况下，提供有限的可用性
3. 设备在断网的情况下做的修改，在网络回复后可以继续同步。如果出现了冲突，有一定的自动解决能力

这需要基于ndm的基础协议，实现一个更高级的客户端 -> NamedDataMgr Client Daemon

结论：同步盘的两种模式
NDMClient的强Cache模式:离线模式下只能读不能写
Local NDM + Remote NDM模式:离线模式下能读能写，同步本质上是两个NDM之间的同步


## 物化（finalize) 流程分析

物化过程的状态机
> FileNode物化: Working --Close--> Cooling --cacl_name--> Linked --move_to_store--> Finalized
> DirNode的物化: inode --all children is Object --> Object

Object Ready不是物化:
- 通过 create_file -> frozne的物化，只是状态转换，不涉及到新的数据的下载
- 通过bind_object添加到系统里的对象，默认并不会自动pull任何chunk,需要外界手工pull

因为cacl_name的和move_to_store都可能耗时，因此系统会根据inode的flag和状态，使用不同的策略进行物化。

- 不物化，适合一些临时文件。这些文件期待很快就会删除
  - 对一些经常写入的热门文件，物化也有是有价值：可以把文件里不写入的chunk物化，在filer_buffer service上只会保留
- close时直接物化，比较适合小文件（小于32MB的文件)
- 标准冷却策略:系统把写入后没有新的修改的文件自动物化，这个超时会受系统状态的影响。当写入缓存容量告急时，会极速的减小自动物化的时间，以尽快腾出空间
- 手工物化，此时会锁定目标path直到物化完成

### 物化流程 FileNode -> FileObject

Stage1 Cacl (在fb_meta servcie内部运行）
- 查询所有状态处于close状态并且close了足够久的的File Node
- 调用fb_service.cacl_name (这期间文件又有写入怎么办？)
- 将fil_enode状态改成Linked（ObjId)
- 根据Commit策略，进行快速下一步处理
    - 如果rename就可以实现完成move_to_store,立刻Commit（这个是基于chunk粒度的),基本上只工作与全闪单机环境
    - 如果文件不大，立刻Commit
    - 根据计算出来的结果，创建ExternalLink Chunk Item到named_store

Stage2 Commiting (在fb_service内部运行，单机版也是fsmeta service的一部分)
这是真正的落盘流程（Commiting)
- 查询所有处于Frozne状态并且Fronze了足够久的 filebuffer
- fb_service调用move_to_store函数，搬运filebuffer到正确的named_store
  - 最快是一次rename
  - 本地SSD->HDD的数据复制比较常见
  - 本地SSD->Remote Named Store最慢
- metadb.update_file_node -> Finalized(ObjectId)


### 物化流程 DirNode -> DirObject 

这是系统里最延迟的物化流程。只有目录里的所有SubItem都物化后，才会触发

- 遍历系统里很久没有写入的DirNode和被设置为Readonly的DirNode
- 尝试查看起SubItem是否都已经物化了 (FileItem按上面流程，只要长久没写入肯定是会自动物化的)
  如果SubItem特别多（超过一个范围，那么就不自动物化了)
- 构造DirObject
- 用该DirObject占据DentryItem
- 删除原有DentryItem的所有child dentry item

根据上面流程，肯定是从最深的子目录开始逐步物化的
这个流程，会减少系统里的DentryItem

打快照后，会立刻对快照dest开始执行DirObject物化（因为快照是readonly的）

### 实例化流程 FileObject -> FileNode

- 创建FileBufferWithBaseChunkList
- 当第一次写入的时候，如果BaseChunkList很小，那么根据DirtyChunk的设计，会变成标准的FileBuffer(整个都脏了)
  - 这个过程，会在FileBuffer中复制BaseChunkList[0]作为COW的初始化，结果上是FileObject的反物化
- NodeState是Linkded状态,不用实例化
- NodeState是Finlaize状态，需要实例化(FileBuffer已经被释放了)

### 实例化流程 DirObject -> DirNode 

当DirObject需要转换成inode时，就是反物化。有两种

- DirNodeWithBaseDirObject (默认选项)
- DirNode 当DirObject的Children总数比较少时

DirObject要转换成inode,这个DirObject必须已经在NamedStore里了。

## 一些精细的状态管理

> 所有的状态冲突，都是写操作引起的

### 创建file的潜在状态冲突

1. 得到parent_dirnode
2. 通用list_sub_dentries确定可以创建dentry_item (名字不冲突)
3. create FileNode (无需分配Filebuffer提高速度)
4. 创建dentry_item-target->filenode (基于dirnode.rev乐观锁)
5. 返回成功

该流程里的list的性能问题的源头
当parent_dirnode的状态发生改变后,第4步的dentry_item可能无效（通过乐观锁避免）


### File在物化流程中，再次被打开
1. cacl已经在进行中，并且期望完成后，修改node_state(用CAS修改node_state)
2. 打开用户看到的node_state是cooling状态，可以打开写入（变成working)
3. 用户写入数据后,cacl也结束了，如果能成功修改node_state则带来bug(所以这里要用CAS，让第二个打开写操作能成功)




## 系统整体性能分析

### 事务整理(识别长事务)

### 

### 通过Cache减少复杂的数据库查询


### FileBuffer Service开销

- 一个FileBuffer Servcie可以管理多个 Local Buffer(有多少SSD)
- 系统里可以有多个FileBuffer Service，组成了系统的写入缓存池子。(可以算出可用大小)
- FileBuffer是高可用向的，配置成高可靠性必然会带来写放大（比如至少要在2个FileBuffer Service写入成功才算成功）
- FileBuffer不会在FileService之间复制，只会单向的移动到named_store去提高可靠性


### GC Object的流程和性能开销

- 没有在object_stat表里的obj,默认ref_count = 0
- 通过定期删除object_state里ref_count = 0 的元素，实现GC
- named_store可以通过object_state反查自己的所有obj,来实现深度GC

#### GC Dentry Item的流程

- DentryItem怎么删除么？
- iNode(FileNode/DirNode)怎么删除?

#### Mount一个超大的DirObject到系统中对GC的影响

按下面的流程:

```rust
fn OnMountObject(objid,option<obj_body>,deep) {
    obj_stat.update_ref_count(objid,1)
    if obj_body.same() {
        //dir 的chldren是objid, file的children是chunkid
        children = obj_body.get_children()
        if deep > 2 {
            gc_queue.append(objid,1)
        } else {
            for childobjid,child_obj in children {
                OnMountObject(childobjid,child_obj,deep+1)
            }
        }
    }
}
```
如果超大的DirObject的所有children,都已物化，那么必然会在obj_stat中创建组数的条目。

obj_stat表保存在哪？
- objid->objstat是可变的，因此不能周named_store的逻辑，否则扩容了就不知道以为为准了。
- 目前还是作为fsmeta db的一部分，但这必然会对fsmeta造成比较大的压力

