## 基本设计



```python

def cacl_named_file(source_file,packed_obj_pipline):
    if file_size(source_file) > MAX_CHUNK_SIZE:
        chunklist = build_chunklist(source_file) # sub chunk already write to named_mgr
        packed_obj_pipline.push(chunklist)
        return create_file_obj_with_chunklist(chunklist)
    else:
        chunk = build_chunk(source_file) # chunk alaready write to named_mgr
        return create_file_obj(chunk)


def cacl_named_dir(source_dir,packed_obj_pipline):
    sub_items = get_children(source_dir)
    for item in sub_items:
        if is_dir(item):
            item.named_obj = cacl_named_dir(item)
            packed_obj_pipline.push(item.named_obj)
        else:
            item.named_obj = cacl_named_file(item)

    dir_obj = create_dir_object(sub_items)
    return dir_obj

```
按上面代码，一个典型的pipline里的对象是

```
/a/b/1.data
/a/b/2.data
/a/b/3.data
/a/b/4.data
/a/1.data
/a/2.data
/a/3.data
/a/4.data
```

```
file_a_b_1.chunklist
file_a_b_3.chunklist
dir_a_b
file_a_1.chunklist
file_a_4.chunklist
dir_a

```

发送时
dir_a_b
file_a_b_3.chunklist
file_a_b_1.chunklist
dir_a
file_a_1.chunklist
file_a_4.chunklist
```python

def rebuild_file_obj(file_obj,packed_obj_pipline):
    if file_obj.chunklist:
        chunklist = packed_obj_pipline.pop()
        for sub_chunk in chunklist:
            named_object_mgr.pull_chunk(sub_chunk,packed_obj_pipline.chunk_session)
    else:
        named_object_mgr.pull_chunk(file_obj.content,packed_obj_pipline.chunk_session)

def rebuild_dir_object(dir_obj,packed_obj_pipline):
    state = named_object_mgr.get_state(dir_obj.objid)
    if state == "ok" # all sub item is ok
        packed_obj_pipline.seek_forward(dir_obj.total_items)

    for item in dir_obj.sub_items:
        if is_dir_object(item):
            sub_dir = packed_obj_pipline.pop()
            rebuild_dir_object(sub_dir)
        else:
            rebuild_file_object(item)


```

pipline的顺序和上面的相反(dir_a是第一个)

下面是ndn_router中的实现
```python
def on_upload_dir(request,session_id):
    pipline_info = read_from_reqeust(request)
    req_stream = reqeust.body()
    pipline = get_ppipline(pipline_info,req_stream)
    
    while True:
        item = await pipline.pop_header()
        match item.type:
            case CHUNK_ID:
                upload_chunk_stream = get_upload_chunk_session(session_id)
                await ndn_mgr.pull_chunk(item.chunk_id,[upload_chunk_stream])
            case CHUNK_LIST:
                await ndn_mgr.set_object(item.chunklist_id,item.chunk_list)
            case SIMPLE_OBJECT_MAP:
                await ndn_mgr.set_simple_object_map(item.object_map_id,item.object_map)

def on_upload_chunk_session(request,session_id)
    req_stream = reqeust.body()
    set_upload_chunk_session(req_stream,session_id)

```
要解决的核心问题：
如何高效的，在两个设备之间，(利用网络）高性能的传递大量的NamedObject和Chunk,并充分利用NDN的去重和可信多源基础设施

- 通过pipline，以push方式传递命名对象
- 当push的是objid时（通常是一个容器），会强制等待对面确认该容器是否存在，如果存在则可以自动skip掉发送该容器内的全部sub items
- 接收段总是用 ndn-channel来下载chunk, 从数据的获取方来看，一个chunkid可以有多个候选的channel,但是可以同时下载多个chunkid
- 从数据的发送段来看，可以构造临时的ndn-channel-source,该source中，只支持有限的chunk获取


边扫描边上传和直接上传的区别：
- push模型：边扫描边上传，是一个深度有效的小目录上传，客户端和服务器在差不多的时间里得到了待上传目录的root_dir_id
- pull模型：直接上传通常是本地已经有了root_dir_id,此时服务器的逻辑是下载该root_dir_id
原理上，push模型的在产品上的感觉就是反映更迅速一些（会不会是一种过早优化？）

通过文件系统日志更快的完成dirobj的构建 (cacl_dir_object_with_fs_logs)
- 已经完成过旧版本的构建
- 构建过程中，文件系统发生了变化，判断变化是否涉及了已经构建的对象
- 在构建完成后，基于修改日志再起构建

通过本地版本记录更快的完成dirobj的构建(跳过hash计算) (cacl_dir_object_with_local_version)
- 通过qcid+精确大小 判断文件是否变化，如果未变化则跳过chunk/chunklist的构建
- 如果是大文件，通过路径/文件系统日志，找到文件的上一个版本，并有机会基于diff算法构建更合理的chunklist

通过本地版本记录更快完成local dir新版本的更新(download_dir_object_with_local_version)
- 通过本地版本的记录，跳过已知的dirobject（以及所有sub item）的重建
- 通过本地版本记录，得知当前目录的文件的hash,并跳过重建


通过确认远程的旧版本版本来减少新版本上传的网络传输 (upload_dir_object_with_known_remote_version)
- 因为已经知道了本地的新版本，所以适用下载模型
- 基于下载模型，天然就有机会跳过跳过重复的object,尽力只传输应该传输的部分
- upload_dir_object_with_known_remote_version 和 download_dir_object_with_local_version 本质上是一样的？

思考下载流的问题
```python
def download_dir(dir_obj_id):
    if ndn_mgr.exist(dir_obj_id)
        return

    dir_obj = get_obj(dir_obj_id)
    for sub_item in dir_obj:
        if sub_item.is_dir()
            download_dir(sub_item)
        else:
            will_download_file.push(sub_item)

    for file in will_download_file:
        download_file(file)
    
    ndn_mgr.put_obj(dir_obj)

def download_file(file_obj):
    if ndn_mgr.exist(file_obj):
        return
    file_obj = get_obj(file_obj)
    if file_obj.content.is_chunk_list:
        download_chunk_list(file_obj.content)
    else:
        dowloand_chunk(file_obj.content)
    ndn_mgr.put_obj(file_obj)
```


如何实现任务的暂停恢复：
- 如果能明确的控制source目录的写入，那么pipline的实现只需要是有本地存储的，就可以在这个基础上实现断点续传
- 上传的恢复: 
  - 精确恢复：pipline记录target的状态，并从确定位置开始
  - 基于“已存在跳过的”的恢复：利用pipline的目录跳过机制，快速的跳过进而进入确定的上传



根本问题：为什么客户端不能是服务器！ 这样只需要写一次download dir就好了！

- 纯网页难以实现dir 上传？


rhttp tunnel的设计

- http 命令通道（tunnel）通道，http_stream,
client->Server request
Server->Client 单向Stream，每一行是一个命令（cmd）

- http rstream 
client->server reqest with cmd
client->server response of cmd


第二个问题：cyfs里是否要包含标准的dirobject sync协议？ 
同步盘必然是一个关键需求，sync用推模型还是拉模型？
- 推模型： client明确的知道server的当前版本，只推送新版本和当前版本的改变部分过去
客户端通过确定的服务器版本，能很快的计算出diff(或则放弃计算diff),然后通过推送diff来完成新版本的推送
logs
```
upload dir_a_a
upload file_a_1.data
```

基于pipline的推模型，更适合”边扫描边推送“, 


- 拉模型： server当然知道自己的版本，通过标准的"获得obj前先通过objid去重“的方法，来减少实际获取Obj的行为

server通过标准的，namedobject天然的去重机制，来尽快完成新版本的重建
logs
``` 
download dir_a_a
download file_a_1.data
download dir_a_b
download file_a_b_1.data

```

