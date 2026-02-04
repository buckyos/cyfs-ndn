# fs buffer 

通过fs meta,申请得到一个fs buffer handle
f= open_fs_buffer(fs_buffer_id)
    根据fs_buffer_handle的信息s，在本地打开一个mmap文件
    fs_buffer_handle在有parent chunklist的情况下，使用DirectChunk的方式进行管理

该fsbuffer是一个可读写的文件，在单机版，是基于一个mmap文件
