# Named Store Manger (store_mgr) 里的工具函数

在DirObject/FileObject/SimpleChunkList 这些对象的帮助下，store_mgr其实已经成为一个分布式的，只读文件系统。其经典路径是

> $dirobjid / subdir1 / subdir2 / subdir3 / filename --> fileobj.content --> chunklist1

可使用 open_reader($dirobjid,"/subdir1/subdir2/subdir3/filename") 这种类似文件系统的API语义，打开chunklist1的reader,实现文件读取。

这个过程中，需要依赖路径上的所有对象都在store_mgr中已经存在。
如果开启chunklist lazy eval,则在read时，才会检查具体的chunkid是否存在。
为了支持上述抽象，系统提供了一些有潜规则的

## open($obj_id,path) 

- 对已知对象有特殊处理
- 非已知对象走标准的get_json_path
- path必然指向的是一个ObjId
```python
def open_obj($obj_id,path) : #返回json
    root_obj = store.get_object($obj_id)
    if path:  
        if root_obj.is_known_object()
            #根据root_obj的类型不同，消费的深度不同
            next_objid,new_path = root_obj.extract_objid_by_path(path)?
        else:
            #从最大深度开始尝试,深度越浅消耗的path越少
            next_objid,new_path = json_obj.try_extract_objid_by_path(path)?
        #递归调用,new_path为Null(完全被消费)就会真正完成
        return open_obj(next_objid,new_path)
    else:
        return root_obj
        
```
open_obj函数是精确的打开对象(得到一个json),不能打开chunk,因此上述路径最深可以得到chunklist,但不能得到具体某个chunk
`open_obj($dirobjid,"/subdir1/subdir2/subdir3/filename/content")` OK
`open_obj($dirobjid,"/subdir1/subdir2/subdir3/filename/content/0")` 错误，无法得到用open_obj打开chunk-list的第一个chunk

### extract_objid_by_path 的DirObject特例处理

DirObject每次只会消耗一层
DirObject如果内嵌对象，会尝试直接使用内嵌对象而不是走一个get_object(提高性能)


## open_reader($obj_id,path)
根据path最终指向的目标可有下面特例
- 指向ChunkId ：不是特例，就是打开ChunkReader
- 指向ChunkListId : 特例，打开SimpeListChunkReader
- 指向FileObjectId : 特例： 打开Content(Content可能是ChunkId或ChunkListId)
```python
def open_reader($obj_id,path):
    if obj_id.is_chunk:
        return open_chunk_reader(obj_id)
    if obj_id.is_chunklist:
        return open_chunklist_reader(obj_id)
    if obj_id.is_fileobject and !path:
        return open_fileobject_reader(obj_id) #内部根据content确定是chunk/chunklist reader

    if path:  
        root_obj = store.get_object($obj_id)
        if root_obj.is_known_object()
            #根据root_obj的类型不同，消费的深度不同
            next_objid,new_path = root_obj.extract_objid_by_path(path)?
        else:
            #从最大深度开始尝试,深度越浅消耗的path越少
            next_objid,new_path = json_obj.try_extract_objid_by_path(path)?
        #递归调用,new_path为Null(完全被消费)就会真正完成
        return open_reader(next_objid,new_path)
```

按时上面逻辑,下面两个调用等价
```
open_reader($dirobjid,"/subdir1/subdir2/subdir3/filename")
open_reader($dirobjid,"/subdir1/subdir2/subdir3/filename/content")
```

## 没有open_writer
store_mgr本身偏只读，因此必须使用store的API来写入Chunk




## FileObject / DirObject 的本地提取

`NamedDataMgr::open_reader(&ObjId, Option<String>)` 返回 `(ChunkReader, u64)`，可读取单 chunk、ChunkList、FileObject 及 DirObject 的内部文件路径；读取不会隐式落盘。

`NamedDataMgr::extract_object(&ObjId, &Path)` 将 FileObject / DirObject 提取到调用方提供的、不存在的目标路径。它复用上述 reader，逐块复制并核对 FileObject 大小，保留目录树和空目录；目录入口仅允许文件和目录，拒绝路径穿越、分隔符、NUL、冒号以及超过 256 层的目录。目标已存在时失败且不修改它；本次提取失败只删除本次创建的目标，不删除 NamedStore 对象或共享引用。调用方负责目标祖先路径的权限与符号链接检查、业务授权和提交。

服务应先校验用户对入口 ObjId 的读取权限。提取期间可使用独立 owner 的临时 recursive pin；完成为实体文件后，持久性由目标文件系统负责，不依赖 NDM 上传 holding 的 24 小时 lease。仅引用对象的业务仍需自己的 retention，不能用本地提取 API 替代。
