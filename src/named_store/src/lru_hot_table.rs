use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// In-memory hot table for LRU access tracking.
/// Reads only update this table; a background flush writes dirty entries to DB.
pub struct LruHotTable {
    inner: Mutex<HotInner>,
    /// Minimum interval (seconds) between DB flushes for the same key.
    relatime_threshold: u64,
}

struct HotInner {
    entries: HashMap<String, (u64, bool)>, // (timestamp, dirty)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl LruHotTable {
    pub fn new(relatime_threshold_secs: u64) -> Self {
        Self {
            inner: Mutex::new(HotInner {
                entries: HashMap::new(),
            }),
            relatime_threshold: relatime_threshold_secs,
        }
    }

    /// Touch an object, marking it as recently accessed.
    /// Only marks dirty if the previous touch was older than the relatime threshold.
    pub fn touch(&self, obj_id: &str) {
        let now = now_secs();
        let mut inner = self.inner.lock().unwrap();
        let prev = inner.entries.get(obj_id).map(|e| e.0).unwrap_or(0);
        if now.saturating_sub(prev) >= self.relatime_threshold {
            inner.entries.insert(obj_id.to_string(), (now, true));
        }
    }

    /// Collect all dirty entries and clear the dirty flag.
    /// Returns (obj_id_str, timestamp) pairs.
    pub fn collect_dirty_batch(&self) -> Vec<(String, u64)> {
        let mut inner = self.inner.lock().unwrap();
        let mut batch = Vec::new();
        for (key, (ts, dirty)) in inner.entries.iter_mut() {
            if *dirty {
                batch.push((key.clone(), *ts));
                *dirty = false;
            }
        }
        batch
    }
}
