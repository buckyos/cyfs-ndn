//store layout从fsmeta下载后，完全保存在本地，select target操作不需要与fsmeta交互

#[derive(Debug, Clone)]
pub struct StoreTarget {
    pub store_id: StoreId,
    pub base_path: PathBuf,
    pub capacity: Option<u64>,
    pub status: TargetStatus,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct StoreLayout {
    pub epoch: u64,
    pub targets: Vec<StoreTarget>,

    // 伪代码：一致性哈希/CRUSH ring 等 placement 结构
    // pub ring: HashRing,
}

impl StoreLayout {
    pub fn select_targets(&self, chunk_id: &ChunkId, epoch: u64) -> Vec<StoreTarget> {
        // 伪代码：
        // - 用 epoch 对 ring 做确定性映射
        // - 选择 1..N 个 target（按可靠性策略/副本数/纠删码策略）
        // - 过滤 Offline/ReadOnly（写路径只选 Active；读路径可选 ReadOnly）
        todo!()
    }
}
