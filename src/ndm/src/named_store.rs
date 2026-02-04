

//当NamedStore Service化的时候，需要自己有一定的基于layout的数据自修复能力
pub struct NamedStoreService {
    layout: Arc<RwLock<StoreLayout>>,
    // db/index/targets...
}

impl NamedStoreService {
    pub fn new(layout: StoreLayout) -> Self {
        todo!()
    }


    pub fn trigger_background_tasks(&self) -> NdnResult<()> ;
    {
        // 伪代码：
        // - 启动 scrubber：清理 chunk.tmp、修正 Writing 过期状态
        // - 启动 staged commit 状态机推进（cooling->linked->finalized）
        // - 启动 lazy migration worker（读到旧 epoch 后投递任务）
        todo!()
    }


}