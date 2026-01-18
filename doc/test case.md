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

    TODO：

4. 错误码

5. 权限
   开放权限的节点执行相关操作时成功，否则失败
    - 对某些节点开放读权限
    - 对某些节点开放写权限
    - 对某些节点开放读写权限

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
    - 存入`NDN`节点的数据，会默认保留一段时间(todo: 多久?)，超时后被清理
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
