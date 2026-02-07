use ndm::FsMetaListEntry;
use std::collections::HashMap;

#[derive(Default)]
struct ListSession {
    entries: Vec<FsMetaListEntry>,
    cursor: usize,
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

    pub(crate) fn start_session(&mut self, entries: Vec<FsMetaListEntry>) -> u64 {
        let session_id = self.next_session_id;
        self.next_session_id = self.next_session_id.saturating_add(1);
        self.sessions
            .insert(session_id, ListSession { entries, cursor: 0 });
        session_id
    }

    pub(crate) fn list_next(
        &mut self,
        list_session_id: u64,
        page_size: u32,
    ) -> Option<Vec<FsMetaListEntry>> {
        let session = self.sessions.get_mut(&list_session_id)?;
        if session.cursor >= session.entries.len() {
            return Some(Vec::new());
        }

        let end = if page_size == 0 {
            session.entries.len()
        } else {
            (session.cursor + page_size as usize).min(session.entries.len())
        };
        let out = session.entries[session.cursor..end].to_vec();
        session.cursor = end;
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
        let session_id = cache.start_session(vec![
            sample_entry("a"),
            sample_entry("b"),
            sample_entry("c"),
        ]);

        let page1 = cache.list_next(session_id, 2).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].name, "a");
        assert_eq!(page1[1].name, "b");

        let page2 = cache.list_next(session_id, 2).unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].name, "c");

        let page3 = cache.list_next(session_id, 2).unwrap();
        assert!(page3.is_empty());
    }

    #[test]
    fn test_stop_list_cache_session() {
        let mut cache = ListCache::new();
        let session_id = cache.start_session(vec![sample_entry("x")]);
        assert!(cache.stop_session(session_id));
        assert!(cache.list_next(session_id, 1).is_none());
    }
}
