# NDM 性能分析

基本假设，大部分数据都是 创建->读取循环
少部分数据，频繁修改。修改模式场景的是Append
coding场景，build过程大量创建小文件，使用后批量删除
照片管理场景，查看多年前的照片

## 创建临时小文件

### fb_handle = fsmeta.open_file_writer("parent_dir","filename") 

> meta看来的树结构 `inode -> inode -> DirObject / DirObject / DirObject(Parent) / filename`

总是要确保文件能打开成功(获得写权限)

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过inode_id，查询得到parent_inode，判断状态是否可读。上面这种路径，[metadb.read 1]
  如果filename处已经有同名DirObject那么是会失败的，如果有同名filename,需要得到old_file_inode,要看flag是否允许覆盖
- 在DirObject模式下，如果有需要，需要填补路径上的parent_inode,parent_parent_inode, 
  例子的这种情况，需要创建3个inode. 注意DirObject的child只能是Object,不能是inode
- 创建目录项 filename_item -> file_inode -> fb_handle(Option<BaseFileObject>)（下面操作是事务) [metadb.write 3]
  - 创建的新inode,判断是否需要有BaseFileObject? [metadb.write 1]
    Flag是否是新文件，新文件一定不需要BaseFileObject（小文件大概率不要)
    旧路径上的old_file_inode如果已经指向了一个FileObject,需要BaseFileObject
    通过DirObject定位到了一个FileObject,需要BaseFileObject  [是否需要 DirObjId_inner_path -> objid的cache?]
  - 申请file_write_lease,主要是记录现在个inode暂时分配给了“那个lease-session,多长时间” [metadb.write 1]
  - 添加目录项 upsert_dentry(parent_dir_id, &name, Target::IndexNodeId(fid) [metadb.write 1]
  
- 基于lease_session + file_inode + fb_id，返回filebuffer_handle

总计: [metadb.read 2,metadb.write 3]

### fb = fb_service.open(fb_handle)

- 根据filebuffer_handle里的BaseFileObject信息，打开BaseReader（小文件大概率不要)
- local_fb_service.alloc_buffer(fb_id) [fs_service_db.write 1,fs_service_fs.createfile 1]
  根据filebuffer_handle选择合适的fb_service(单机版必然是fs_meta所在的机器，未来主要是选择本机)分配FileBuffer
- 返回filebuffer

总计:[fs_service_db.write 1,fs_service_fs.createfile 3]

### fb.write

- 本地文件操作

### fb.close

- 本地文件操作，如果文件足够小，可以利用OSBuffer还在的时候，直接计算chunkId

### fsmeta.close_file(fb_handle)

根据fb_handle的信息，要求meta更新file_inode的状态:

- 释放lease_session
- 更新file_node的状态

总计:[metadb.write 2]

## 小文件被另一个应用读取

### fb_handle = fsmeta.open_reader("parent_dir/filename")

> meta看来的树结构 `inode -> inode -> inode_base_dir -> inode_base_dir -> inode_base_dir (Parent) / filename`

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过get_dentry(parent_dir_node_id,"filename"),得到file_node [metadb.read 1]
- 此时file_node指向一个filebuffer_handle（这个流程肯定不会触发物化搬运)

总计:[metadb.read 2]

### 如果已经有chunk_list,open_chunk_read_by_id(chunk_list[0])

如果本地cache已经有该chunk,直接使用
否则走 store_layout.select流程，向named_store请求chunk

### 如果只有filebuffer_handle

```
fb = fb_service.open_reader(fb_handle)
```

- 根据fb_handle.id 得到fb的信息 [fs_service_db.read 1]
- 根据filebuffer_handle里的BaseFileObject信息，打开BaseReader（小文件大概率不要)
- 通过fb_service打开fb_buffer_reader
- 返回真正的reader,这个reader会根据ReadRange使用BaseRader + fb_buffer_reader (分层读取)

总计:[fs_service_db.read 1]

## build结束，通过list得到所有的小文件

fs_meta.list("parent_dir_path")

> meta看来的树结构 `inode -> inode -> inode -> inode -> inode (Parent)

parent_node是一个有BaseDirObject的DirNode

- 从路径得到parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- metadb.list_dentry(parent_inode_id) , 得到所有的children
- 得到BaseDirObject的第一层所有children
- 合并上面的两个children

注意这里支持游标(?需要缓存上面的合并结果)

总计 [metadb.read 1,metadb.select 1]

## 删除parent_dir

> meta看来的树结构 `inode -> inode -> inode_base_dir -> inode_base_dir -> inode_base_dir (Parent)`

- 从路径得到parent_parent_dir的inode_id，查询次数和路径的深度有关，但有cache可以让这个过程变成一次数据库查询 [metadb.read 1]
- 通过parent_parent_dir_inode,得到parent_parent_dir_node [metadb.read 1]
- 因为parent_parent_dir_node有Base Dir Object,且该Base Dir Object里含有parent item，所以通过添加墓碑item实现删除 [metadb.write 1]]

总计 [metadb.read 2,metadb.write 1]

## 场景二、浏览家庭照片库

path: /home/lzc/photos/2016/香港旅游

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


## inode状态管理

### inode的类型
- ObjectNode
    -> Finalized(AnyObjectId)
- FileNode
    -> Working(FileBufferHandle with BaseFileObjectId)
    -> Cooling(FileBufferHandle with BaseFileObjectId)
    -> Linked(FileObjectId)
    -> Finalized(FileObjectId)
- DirNode
    -> Finalized(DirObjectId)
    -> Overlay(BaseDirObjectId)
    -> Normal

## Dentry Item
```rust
struct DentryItem {
    namespace_id:u32,
    parent_inode:u64, //根目录为0
    name:String,
    inode:u64,//-->inode
}
```


## 系统整体性能分析

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

## 物化（对象化)

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

### 物化流程 FileBuffer -> FileObject

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

### FileObject -> FileBuffer(反物化)

- 创建FileBufferWithBaseChunkList
- 当第一次写入的时候，如果BaseChunkList很小，那么根据DirtyChunk的设计，会变成标准的FileBuffer(整个都脏了)
  - 这个过程，会在FileBuffer中复制BaseChunkList[0]作为COW的初始化，结果上是FileObject的反物化

### DirObject -> inode （反物化）

当DirObject需要转换成inode时，就是反物化。有两种

- DirNodeWithBaseDirObject (默认选项)
- DirNode 当DirObject的Children总数比较少时

DirObject要转换成inode,这个DirObject必须已经在NamedStore里了。

### inode -> DirObject （物化）

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

