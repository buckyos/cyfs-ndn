// ========== Background Manager ==========

pub struct BackgroundMgr {
    // Placeholder for background task management
    _staged_queue: Vec<String>,
    _lazy_migration_queue: Vec<String>,
}

impl BackgroundMgr {
    pub fn new() -> Self {
        Self {
            _staged_queue: Vec::new(),
            _lazy_migration_queue: Vec::new(),
        }
    }
}

impl Default for BackgroundMgr {
    fn default() -> Self {
        Self::new()
    }
}
