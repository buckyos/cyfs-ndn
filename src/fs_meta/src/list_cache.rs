use ndm::FsMetaListEntry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

#[derive(Default)]
struct ListSession {
    entries: BTreeMap<String, FsMetaListEntry>,
    cursor: Option<String>,
}

/// In-memory list session cache used by fsmeta start_list/list_next/stop_list.
pub(crate) struct ListCache {
    next_session_id: u64,
    sessions: HashMap<u64, ListSession>,
}

impl Default for ListCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ListCache {
    pub(crate) fn new() -> Self {
        Self {
            next_session_id: 1,
            sessions: HashMap::new(),
        }
    }

    pub(crate) fn start_session(&mut self, entries: BTreeMap<String, FsMetaListEntry>) -> u64 {
        let session_id = self.next_session_id;
        self.next_session_id = self.next_session_id.saturating_add(1);
        self.sessions.insert(
            session_id,
            ListSession {
                entries,
                cursor: None,
            },
        );
        session_id
    }

    pub(crate) fn list_next(
        &mut self,
        list_session_id: u64,
        page_size: u32,
    ) -> Option<BTreeMap<String, FsMetaListEntry>> {
        let session = self.sessions.get_mut(&list_session_id)?;

        let start_bound = match session.cursor.as_ref() {
            Some(cursor) => Bound::Excluded(cursor.clone()),
            None => Bound::Unbounded,
        };
        let limit = if page_size == 0 {
            usize::MAX
        } else {
            page_size as usize
        };

        let mut out = BTreeMap::new();
        for (name, entry) in session
            .entries
            .range((start_bound, Bound::Unbounded))
            .take(limit)
        {
            out.insert(name.clone(), entry.clone());
        }

        if let Some((last_name, _)) = out.iter().next_back() {
            session.cursor = Some(last_name.clone());
        }

        Some(out)
    }

    pub(crate) fn stop_session(&mut self, list_session_id: u64) -> bool {
        self.sessions.remove(&list_session_id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndm::DentryTarget;

    fn sample_entry(name: &str) -> FsMetaListEntry {
        FsMetaListEntry {
            name: name.to_string(),
            target: DentryTarget::Tombstone,
            inode: None,
        }
    }

    #[test]
    fn test_list_cache_paging() {
        let mut cache = ListCache::new();
        let mut entries = BTreeMap::new();
        entries.insert("a".to_string(), sample_entry("a"));
        entries.insert("b".to_string(), sample_entry("b"));
        entries.insert("c".to_string(), sample_entry("c"));
        let session_id = cache.start_session(entries);

        let page1 = cache.list_next(session_id, 2).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1.keys().next().unwrap(), "a");
        assert_eq!(page1.keys().next_back().unwrap(), "b");

        let page2 = cache.list_next(session_id, 2).unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2.keys().next().unwrap(), "c");

        let page3 = cache.list_next(session_id, 2).unwrap();
        assert!(page3.is_empty());
    }

    #[test]
    fn test_stop_list_cache_session() {
        let mut cache = ListCache::new();
        let mut entries = BTreeMap::new();
        entries.insert("x".to_string(), sample_entry("x"));
        let session_id = cache.start_session(entries);
        assert!(cache.stop_session(session_id));
        assert!(cache.list_next(session_id, 1).is_none());
    }
}
