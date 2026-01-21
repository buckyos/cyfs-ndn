### Link to local file的潜在问题

chunk link模式带来的潜在问题

- Link模式不存在打开Writer
- 文件丢失：打开reader失败
- 文件修改：发现修改时间改变，打开reader失败

当需要写入local文件时的痛苦选择

- local file已经是link模式了! 不应该主动给自己找麻烦？
- local文件已经存在了？
  已经存在的文件和fileobject一样：跳过（如何快速判断？）
  已经存在的文件比fileobject更加新: 本地文件有冲突，需要交给上层处理
  已经存在的文件比fileobject更老：如何只更新需要更新的部分？通过qcid快速判读？
- local dir已经存在了? 断点续传
  存在文件：冲突，需要交给上层处理
- 是否需要删除dir中不存在的item

基于named-mgr link模式快速cacl dir object

- 通过fs log，快速得到改变的file
- 能否通过fs的dir meta data,快速的判断local dir是否修改
- 通过link file的大小和最后修改时间判断是否需要计算hash
- 通过link file的qcid判断是否需要计算完整hash
- 计算fileobj时，能基于“上个版本信息”，构建更合理的chunklist

### named_mgr是否可以同时支持用普通方便保存chunk,和用link方式保存chunk

逻辑上应该是可以的，但本着Chunk只有一个Location的原则:
使用Link模式添加Chunk和使用Store模式添加Chunk的逻辑完全不同

- chunk的location可以更新，但是只能从Link变成Store模式
  系统先用Link模式保存了一个Chunk，然后再用Store模式保存，后续再访问该Chunk都会是Store模式
  在更新的过程中，有可能使用旧的Link来加速Chunk Store的过程
- 已经Store的Chunk，不会有机会变成Link模式
