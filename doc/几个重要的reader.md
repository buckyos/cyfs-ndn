# 几个重要的reader+writer


## ChunkReader
系统重要的基础Reader,是不能Seek的
- 基于本地File打开（seek能力被阉割了）
- 基于一个http read stream打开

## ChunkWrite
NamedStore通过ChunkWrite支持写入，但是对应用层来说，不应该在NamedStore以外使用ChunkWrite.

## SimpleChunkListReader (Lazy Eval)
- 基于一个SimpleChunkList（Vec<ChunkId>）来打开Reader，
- 可以要求以本地模式:所有的chunk都在NamedStoreMgr中,否则就会出错。
- 要支持Seek，就需要知道所有Chunk的长度:
  - ChunkId是Mix类型，带长度，
  - 由定长Chunk组成的
  - 否则就是本地模式，已经在初始化时得到了ChunkSize数组 Vec<u64>

- 正常情况下是lazy eval的，只会打开读到的Reader
  可以定义打开OpenChunkReader的方法，通用named_store_mgr打开失败后，会进入这个闭包来获得Reader
  比如可以约定某个特定的chunk使用

## 没有SimpleChunkListWrite,要做的事情是构造一个新的ChunkList

## FileReader
基于Content的类型，要么是ChunkListReader要么是ChundReader

## DiffChunkList

注意下面两个对象是同构的
```rust
struct DiffChunkList {
  pub base_chunk_list:ObjId,
  pub diff_file_path:Path,
  pub Vec<chunk_index>,  
  pub Option<Vec<chunk_id>>, //Named化以后该字段不为None.
}
```
###  DiffChunkListReader ，可Seek
是一个基于chunklist的合并Reader
经典模式:
一个不可变的SimpleChunkListReader + 一个ChunkDiffList:  HashMap<chunk_index,location> (哪个chunk变了，保存在哪里)

diff_file_path中按Vec的顺序，保存了改变的Chunk(大小参考从Base SimpleChunkList得到)
其基本逻辑是如果落到了ChunkIndex的范围，就通过location的信息来加载修改后的Chunk
DiffChunkListReader,在不特别要求的情况下也是lazy-eval的，能不能读取都要看seek到的时候对应的Reader能否打开
基于该Reader，可以随时构建一个新的SimpleChunkList,该SimpleChunkList里有很多ChunkId与Base相同，节约NamedStore中的实际使用空间。

### DiffChunkListWriter 一定可以Seek
与 DiffChunkListReaderr对应，会通过实际的写入创建一个ChunkListDiff
这里使用的是COW模型，当一个ChunkListDiff里的新Item被创建时，会先从原Reader复制数据
注意尾部的Append优化，如果原有的BaseChunkList的最后一个Chunk很小的时候，每次Append都会产生一个Chunk,这会导致大量的ChunkList




