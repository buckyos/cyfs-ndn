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

