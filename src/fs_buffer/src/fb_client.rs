

/// buffer 服务：单机 mmap / 多 BufferNode(GFS 模型) 都可落到这里
pub trait FileBufferService: Send + Sync {
    fn create_buffer(&self, path: &NdmPath, lease: &WriteLease, expected_size: Option<u64>) -> NdnResult<FileBufferHandle>;

    fn append(&self, fb: &FileBufferHandle, data: &[u8]) -> NdnResult<()>;
    fn flush(&self, fb: &FileBufferHandle) -> NdnResult<()>;
    fn close(&self, fb: &FileBufferHandle) -> NdnResult<()>;

    fn open_reader(&self, fb: &FileBufferHandle, seek_from: SeekFrom) -> NdnResult<Reader>;

    /// Staged 模式：让 buffer node 计算 hash（避免把数据搬回本地再算）
    fn calc_obj_id(&self, fb: &FileBufferHandle) -> NdnResult<ObjId>;

    /// Finalize：把数据从 buffer node 推到 NamedStore internal（IO 密集型）
    fn push_to_store(&self, fb: &FileBufferHandle, store: &dyn NamedStore) -> NdnResult<()>;

    fn remove(&self, fb: &FileBufferHandle) -> NdnResult<()>;
}
