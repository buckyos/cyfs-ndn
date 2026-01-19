# 概述

本文档描述`NDN`系统的测试用例设计。其中有一些自定义的名词，在文档后面有相关注解。

# 用例设计

用例设计从几个维度分类，先设计`一般性用例`，这些用例应该在不同维度下分别实现，不同维度下的特别用例单独列出。在不同维度下的不同取值设计交叉覆盖。`一般性用例`在部分维度下只能取限定值会明确标识，无标识用例在各维度下可以取任意值。

1. 存储数据类型

    - Chunk

        非结构化数据

    - Object

        结构化数据，`JSON`格式

    - File

        内置的特殊`Object`，表达一个文件；构建测试用例时应该从磁盘文件构造`FileObj`

    - Dir

        内置的特殊`Object`，表达一个目录，构建测试用例时应该从磁盘文件系统构造`DirObj`

2. 节点拓扑结构
    - 单节点
    - 多节点
      用例应该覆盖读写节点相同或不同
        - 同`Zone`节点
        - 不同`Zone`节点
3. `API`选用

    列出待选用的`API`，按功能分组，相似功能的`API`分成一组，构造测试用例时尽量从相同组中随机选择以更全面的覆盖

    - URL 生成
        - `NdnClient`：`gen_chunk_url`、`gen_obj_url`
    - NamedDataMgr 管理
        - `NamedDataMgr`：`set_mgr_by_id`、`get_named_data_mgr_by_path`、`get_named_data_mgr_by_id`、`is_named_data_mgr_exist`、`get_mgr_id`、`get_base_dir`
    - 对象读取（本地优先，必要时回源）
        - `NdnClient`：`get_obj_by_id`、`get_obj_by_url`
        - `NamedDataMgr`：`get_object/get_object_impl`、`get_real_object_impl`、`query_object_by_id`、`is_object_exist`
    - 对象写入
        - `NamedDataMgr`：`put_object/put_object_impl`、
    - 对象关联
      `link_same_object`、`link_part_of`、`query_source_object_by_target`
    - 对象挂载路径管理
        - `NamedDataMgr`：`get_obj_id_by_path_impl/get_obj_id_by_path`、`select_obj_id_by_path_impl/select_obj_id_by_path`、`get_cache_path_obj`、`update_cache_path_obj`、`create_file_impl/create_file`、`set_file_impl/set_file`、`remove_file_impl/remove_file`、`get_chunk_reader_by_path_impl/get_chunk_reader_by_path`
    - Chunk 状态查询与上传
        - `NdnClient`：`query_chunk_state`、`push_chunk`
        - `NamedDataMgr`：`have_chunk/have_chunk_impl`、`query_chunk_state/query_chunk_state_impl`、`check_chunk_exist_impl`（未实现）
    - Chunk 下载/读取
        - `NdnClient`：`pull_chunk`、`pull_chunk_by_url`、`open_chunk_reader_by_url`
        - `NamedDataMgr`：`open_store_chunk_reader_impl`、`open_chunk_reader_impl/open_chunk_reader`、`open_chunklist_reader`、`get_chunk_data`、`get_chunk_piece`
    - Chunk 写入/导入
        - `NdnClient`：`pull_chunk`（落本地/本地存储）作为拉取入口
        - `NamedDataMgr`：`open_chunk_writer_impl/open_chunk_writer`、`open_new_chunk_writer_impl`、`update_chunk_progress_impl`、`complete_chunk_writer_impl/complete_chunk_writer`、`complete_chunk_writer_and_rename_impl`（未实现）、`put_chunk_by_reader_impl/put_chunk_by_reader`、`put_chunk`、`add_chunk_by_link_to_local_file_impl`
    - 文件/目录下载
        - `NdnClient`：`pull_file`、`pull_chunklist`、`pull_dir`、`download_fileobj`（落盘 `FileObject`）
    - 发布（挂载到对象路径）/签名
        - `NamedDataMgr`：`pub_object`、`sign_obj`、`sigh_path_obj_impl/sigh_path_obj`
    - 辅助校验/同步
        - `NdnClient`：`remote_is_better`
    - GC
        - `NamedDataMgr`：`gc_worker`、`gc_objects`、`start_gc_thread`

4. 错误码

5. 权限
   开放权限的节点执行相关操作时成功，否则失败
    - 对某些节点开放读权限
    - 对某些节点开放写权限
    - 对某些节点开放读写权限

## 用例落地细化（执行指引）

- 覆盖矩阵与优先级  
  - 最小组合：`数据类型 × 拓扑 × 权限`，其中 API 组在组合内随机选 1~2 个代表接口；必测组合：`Chunk/Object` × `单节点` × `默认权限`、`File/Dir` × `单节点` × `默认权限`、`Chunk/Object` × `同 Zone 多节点` × `默认权限`。选测组合：跨 Zone、权限受限（读/写/读写）、Link 模式。  
  - 优先级建议：存取 > GC > 路径挂载/发布 > 下载/同步 > Link 模式。  
  - 抽样规则：在选测组合中至少覆盖每类拓扑 1 例、每类权限 1 例，API 组内随机选择不同接口轮换。
- API 选用与断言  
  - 读写：`put_object/put_object_impl` + `get_object/get_object_impl` 成功返回，数据 HASH/ID 一致；读不存在返回 NotFound。  
  - 路径管理：`create_file`/`set_file` 后用 `get_obj_id_by_path` 校验路径指向；删除用 `remove_file` 校验路径失效。  
  - 下载/读取：`pull_chunk/pull_chunk_by_url/open_chunk_reader_by_url` 返回的内容与源一致；`pull_file/pull_dir` 落盘文件校验大小/HASH。  
  - GC：`start_gc_thread` 定时 15s 调度，`gc_objects`/`gc_worker` 可手动触发；`last_access_time < now-86400` 被清理。发布到 `NDN-Path` 的对象应被保活。  
  - 未实现/不测：`check_chunk_exist_impl`、`complete_chunk_writer_and_rename_impl` 标记为 TODO/跳过。  
  - 错误码：使用 `NdnError`（`NotFound`、`InvalidId`、`InvalidLink`、`AlreadyExists`、`VerifyError`、`IoError`、`DbError`、`InComplete`、`RemoteError`、`DecodeError`、`OffsetTooLarge`、`InvalidObjType`、`InvalidData`、`InvalidParam`）。前置条件：参数合法、ObjId 可解析、必要文件/目录存在、网络请求成功。
- 错误码与“其他错误”  
  - 常见：`NotFound`（对象/Chunk 不存在）、`InvalidId`（ObjId/DID/PathObj 签名不符）、`InvalidLink`（O-link/R-link 解析失败）、`VerifyError`（数据校验失败）、`IoError`（本地读写失败）、`DbError`、`InComplete`（Chunk 未完成）、`RemoteError`（远端 HTTP 失败）、`DecodeError`（JSON/JWT 解析失败）、`InvalidParam`。  
  - 其他错误：网络超时视为 `RemoteError`；内部异常使用 `Internal`。  
- 权限用例配置  
  - 当前 `NamedDataMgr`/`NdnClient` 内部未实现 ACL，只有 PathObj 使用 `resolve_auth_key` 验签 DID（`ndn_client.rs`）。默认视为开放访问；权限用例需依赖外部 Gateway/服务端策略。  
  - cyfs-gateway（`cyfs-gateway` 仓库）权限入口：CLI 需要 admin 密码，配置文件支持 include/远程同步（`doc/cyfs-gateway cli product design.md`）；转发/访问控制通过 process_chain rule（`doc/概念设计/ProcessChain.md`）实现，规则命令支持 `policy accept/drop/return` 和 `match`/`forward` 等，可用于基于 Host/IP/标签的访问控制。  
  - 针对 NDN 的规则示例（写在 `rootfs/etc/user_gateway.yaml` 并 `cyfs reload --all` 或重启 web3-gateway）：  
    ```yaml
    servers:
      node_gateway:
        hook_point:
          main:
            blocks:
              ndn_acl:
                id: ndn_acl
                priority: -5
                block: |
                  # 禁止来自 bob.web3.buckyos.io 的写入请求
                  match ${REQ_HEADER.host} "bob\\.web3\\.buckyos\\.io" && match ${REQ.method} "POST" && reject "deny bob write";
                  # 只允许指定 DID 的访问
                  match ${REQ_HEADER.did} "did:dev:ALLOWED.*" || reject "deny unauth did";
                  accept;
    ```  
    配置步骤：编辑 `user_gateway.yaml`（覆盖自动生成配置）、`cyfs reload --all` 或重启 web3-gateway（`sudo python3 /opt/web3-gateway/start.py`）；断言方法：访问被拒绝的 Host/DID 返回 403/拒绝日志，允许的请求成功命中后续 block。
- GC 语义澄清  
  - 代码保留 1 天：`last_access_time < now - 60*60*24` 删除；调度周期 15s。  
  - 发布到 `NDN-Path`、以及被保活对象引用的对象应跳过删除。  
  - 标注/引用：`update_obj_ref_count` 遇到非 Chunk 对象会递归子对象（通过 `KnownStandardObject::get_child_objs`）更新 ref_count；`obj_ref_update_queue` 延迟消费，`gc_worker` 批量写入并清零队列后才进入 `gc_objects` 的删除窗口。
- 数据篡改/ID 不匹配构造方式  
  - 构造合法对象后手工修改 `obj_data` 或 chunk 内容，再用原 ObjId 读，预期校验失败；或伪造 ObjId 访问，预期返回 NotFound/IntegrityError。  
  - 断言：Chunk 校验失败返回 `VerifyError`（来自 `chunk::hasher`）；ObjId/PathObj 不符返回 `InvalidId`；不存在返回 `NotFound`；日志关键词包含具体错误信息。
- 多节点/跨 Zone 前置  
  - 需要准备：SN/Gateway 配置、Zone/DID/Device ID、路由/域名、证书/鉴权信息。  
  - 单节点/本地：`get_named_data_mgr_by_path` 自动创建 `ndn_mgr.json` 和 DB；`set_mgr_by_id` 注册实例。  
  - 多节点（参照 buckyos-devkit 标准分布式测试环境）：3 个 Zone：A=`test.buckyos.io`，B=`bob.web3.buckyos.io`，SN=`sn.buckyos.io/web3.buckyos.io`；拓扑 DEV→VM_SN→（NAT1→VM_NODE_A2 10.0.1.2，NAT2→VM_NODE_B1 10.0.2.2），NODE_A1 在宿主机，需在 `etc/resolv.conf` 指向 SN。  
  - 部署步骤概览：安装 multipass → 配置 bridge（`dev_vm_config.json`）→ `main.py create` 创建 VM → 构建 buckyos → `main.py install --all` → `main.py active_sn`/`active --all` 启动 SN/节点，确认 DNS 解析与节点启动 (`buckycli sys_config --get boot/config`)。  
  - Gateway/SN 启动与配置示例：SN 使用 `main.py active_sn` 生成配置并 `main.py start_sn` 启动；节点侧 web3-gateway 安装包包含 `rootfs/etc/cyfs_gateway.yaml`（include boot/user/post/node），启动命令 `sudo python3 /opt/web3-gateway/start.py`（或服务方式），需保证 `boot_gateway.yaml` 中 `node_rtcp` 绑定 2980、`zone_gateway_http`/`node_gateway_http` 绑定 80/3180，必要时在 `user_gateway.yaml` 覆盖自定义规则后执行 `cyfs reload --all`。  
- Link 模式场景  
  - 覆盖：本地文件缺失（应报错）、本地文件被修改（mtime 变化应拒绝或重新计算）、Link→Store 转换优先 Store、断点续传、外部文件冲突处理。  
  - 预期：缺失/修改导致读失败或重新计算（通常 `IoError`/`VerifyError`）；Link 覆盖为 Store 后优先使用 Store；断点续传依赖 ChunkWriter 进度；冲突时返回 `AlreadyExists`/`InvalidParam`。  
- 下载/同步判定  
  - `remote_is_better`：判断远端是否较新；`download_fileobj`：有更新则落盘文件与 fileobj；`pull_file/pull_dir`：按模式（智能同步/完全同步）覆盖冲突策略。  
  - “智能同步”：同步删除缺失项，保留新增文件；本地文件若 mtime 更新更晚则重命名保留（参考 `named_mgr.md` 描述）。  
  - “完全同步”：等同同步前清空目标目录再复制。  
  - 判定：校验最终文件集合、冲突文件重命名存在与否、日志含模式标识。

## 一般性用例

1. 存取
    - 存入并成功读取数据
    - 读取不存在的数据
    - 读取数据校验失败
        - 数据和 ID 不匹配
        - 数据被篡改
        - 其他
    - 鉴权失败
    - 其他错误
2. GC
    - 存入`NDN`节点的数据，会默认保留 1 天，超时后被清理
    - 发布到一个`NDN-Path`的数据，会被一直持有
    - 被多于一个保活对象(新对象、发布对象)引用或间接引用的对象会被持有到引用它的对象生命周期结束

## 单`Device`

1. 基于`NamedDataMgr`接口的本地纯`ndn-lib`单元测试
    - 存取
    - GC
2. 基于`NDNClient`接口向本地`NDN`节点远程测试
    - 存取
    - GC

## 多设备

1. 基于`NDNClient`接口从本地`NDN`节点向同`Zone`其他`NDN`节点远程测试
    - 存取
    - GC
2. 基于`NDNClient`接口从本地`NDN`节点向其他`Zone`其他`NDN`节点远程测试
    - 存取
    - GC

# 名词解释

1. DID

    一个实体(自然人、组织等)的唯一分布式账号

2. Device

    一个部署`BuckyOS`的分布式节点实例，它通常属于一个`DID`

3. Zone

    `DID`相同的一组`Device`组成一个`Zone`

4. Gateway

    类似于`HTTP`协议的`Nginx`，支持`Device`之间的连通

5. SN

    即`SuperNode`，保存`Device`的连通信息(物理地址，域名，注册`SN`等)，用辅助`NAT`穿透、转发等方式辅助`Gateway`实现`Device`之间的连通

6. Object

    任何格式化数据都可以构成一个`Object`，它们通常由一个`JSON`文本描述，这个`JSON`文本按照确定的顺序编码成字符串，并对它计算`HASH`获得其`ObjId`

7. NDN

    即`Named Data Network`，以`ObjId`命名数据，并按照一定的规则组织、存储、发布，发布后的数据可以被其他节点获取。`NDN`里的节点本身也是一个`Named Data`，节点要获取`NDN`里的数据，通常需要数据存放的节点名字和数据本身的名字

8. NamedDataMgr

    `ndn-lib`实现的一个本地组件，负责组织本地命名数据，并提供本地读写功能

9. NdnClient

    类似于`HTTP`的访问客户端，负责通过`HTTP`链接访问(读/写)`NDN`网络中的数据；这个`HTTP`链接跟传统`HTTP`链接一样有一个域名段和`Path`段：https://{domain}/{path}

10. NDN-Path

    `NDN`节点服务启动时通常会在`Gateway`注册，类似`Nginx`一样对外提供服务，注册时候会有一个在本节点范围内的唯一服务名称，在访问`NDN`网络中命名数据的`HTTP`链接中会以这个服务名为`Path`段的第一段，剩余的`Path`部分标识要访问的对象：
    https://{domain}/{ndn-serive-name}/{object-name}

11. O-link

    访问`NDN`节点中的数据时，可以直接通过`ObjId`进行，对应的`HTTP`链接称为`O-link`：
    https://{domain}/{ndn-service-name}/{obj-id}

    因为`ObjId`是一组确定数据的`HASH`，所以`O-link`指向的数据是确定的

12. R-link

    `NDN`节点可以像文件系统组织文件一样，把各个`Object`挂载到一个用户指定的`Path`下，用户可以通过这个`Path`替代`ObjId`访问数据，这种`HTTP`链接成为`R-link`：
    https://{domain}/{ndn-service-name}/{obj-path}

    这种链接更易读，但因为用户可以更新`{obj-path}`上挂载的`Object`，所以这种链接指向的数据是可以被更新的

13. inner-path

    如果一个对象的`JSON`字段中包含有数据块(`Chunk`/`ChunkList`)，那么可以通过父对象和这个字段在`JSON`数据里的路径（这个路径称为`inner-path`）访问这个数据块：

    `O-link`: https://{domain}/{ndn-service-name}/{obj-id}/{field-name}
    `R-link`: https://{domain}/{ndn-service-name}/{obj-path}/{field-name}

    **简单对象类型的子对象不能通过这种方式访问**

14. NDN Router

    实现在`Gateway`里，解析`O-link`/`R-link`找到目标对象的标识，并路由到正确的`NDN`服务

15. 权限

    可以为`NDN`节点管理的数据配置合适的权限，为其他用户提供服务
