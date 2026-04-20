CYFS-NDN仓库定义很多协议，整体上看，分为三层。

CYFS Protocl ：
    公网协议，核心是“从一个Zone Pull一个Named Data "
NDM Http Protocol : Zone 内协议，核心是 设备上的一个进程，用什么协议可以访问Zone内的Named Data Mgr
    Device->ZoneGateway->NamedDataMgr
    - 基于Post的结构化API
    - 基于TUS的Chunk上传
Named Strore Http Protocol : 实现层协议，当一个Device上有Named Store桶时，怎么通过协议访问这个桶
    Device->NodeGateway->NamedStoreBucket