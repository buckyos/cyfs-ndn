use ndm::IndexNodeId;
use std::collections::HashMap;
use std::sync::Arc;

/// Default maximum number of cached entries.
const DEFAULT_MAX_ENTRIES: usize = 10000;

/// Unique identifier for each cached entry, used for LRU tracking.
type EntryId = u64;

#[derive(Default)]
struct PathResolveCacheNode {
    children: HashMap<String, Box<PathResolveCacheNode>>,
    /// Cached value for this exact path.
    value: Option<Arc<Vec<IndexNodeId>>>,
    /// Deepest cached value within this subtree (may be at a descendant path).
    deepest: Option<Arc<Vec<IndexNodeId>>>,
    /// Entry ID for LRU tracking (only set if this node has a value).
    entry_id: Option<EntryId>,
}

impl PathResolveCacheNode {
    fn recompute_deepest(&mut self) {
        let mut best = self.value.clone();
        for child in self.children.values() {
            if let Some(v) = child.deepest.clone() {
                let better = match &best {
                    Some(cur) => v.len() > cur.len(),
                    None => true,
                };
                if better {
                    best = Some(v);
                }
            }
        }
        self.deepest = best;
    }
}

/// LRU entry storing the path components for eviction.
struct LruEntry {
    path: Vec<String>,
}

pub(crate) struct PathResolveCache {
    root: PathResolveCacheNode,
    /// Edge index for invalidation: (parent_inode_id, name) -> path components of the child.
    edge_index: HashMap<(IndexNodeId, String), Vec<String>>,
    /// Maximum number of entries allowed in the cache.
    max_entries: usize,
    /// Current number of cached entries (nodes with values).
    entry_count: usize,
    /// Next entry ID to assign.
    next_entry_id: EntryId,
    /// LRU tracking: entry_id -> LruEntry. Entries are ordered by insertion/access time.
    /// We use a HashMap + separate ordering via entry_id (lower = older).
    lru_map: HashMap<EntryId, LruEntry>,
}

impl Default for PathResolveCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES)
    }
}

impl PathResolveCache {
    /// Create a new cache with the specified maximum number of entries.
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            root: PathResolveCacheNode::default(),
            edge_index: HashMap::new(),
            max_entries,
            entry_count: 0,
            next_entry_id: 0,
            lru_map: HashMap::new(),
        }
    }

    /// Returns the current number of cached entries.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entry_count
    }

    pub(crate) fn get_ids_for_path(&mut self, components: &[&str]) -> Option<Vec<IndexNodeId>> {
        // First, find the node and check if we have a hit.
        let mut node = &self.root as *const PathResolveCacheNode;
        for c in components {
            // SAFETY: we only read through this pointer within the loop.
            let n = unsafe { &*node };
            node = n.children.get(*c)?.as_ref();
        }

        let n = unsafe { &*node };
        let v = n.value.clone().or_else(|| n.deepest.clone())?;
        if v.len() < components.len() + 1 {
            return None;
        }

        // Update LRU: if this node has a value with an entry_id, refresh it.
        if let Some(old_entry_id) = n.entry_id {
            // Remove old entry and create new one with higher ID (more recent).
            if let Some(lru_entry) = self.lru_map.remove(&old_entry_id) {
                let new_entry_id = self.next_entry_id;
                self.next_entry_id += 1;
                self.lru_map.insert(new_entry_id, lru_entry);

                // Update the node's entry_id - need mutable access.
                let mut node_mut = &mut self.root;
                for c in components {
                    node_mut = node_mut.children.get_mut(*c).unwrap().as_mut();
                }
                node_mut.entry_id = Some(new_entry_id);
            }
        }

        Some(v[..components.len() + 1].to_vec())
    }

    fn clear_shallow_values_on_path(&mut self, components: &[String]) {
        let mut node = &mut self.root;
        for c in components {
            // If this node has a value, remove it from LRU tracking.
            if let Some(entry_id) = node.entry_id.take() {
                self.lru_map.remove(&entry_id);
                self.entry_count = self.entry_count.saturating_sub(1);
            }
            node.value = None;
            if let Some(next) = node.children.get_mut(c) {
                node = next.as_mut();
            } else {
                break;
            }
        }
    }

    /// Evict the least recently used entry.
    fn evict_lru(&mut self) {
        // Find the entry with the smallest entry_id (oldest).
        let oldest_id = match self.lru_map.keys().min().copied() {
            Some(id) => id,
            None => return,
        };

        let lru_entry = match self.lru_map.remove(&oldest_id) {
            Some(e) => e,
            None => return,
        };

        // Decrement entry count since we removed from lru_map.
        self.entry_count = self.entry_count.saturating_sub(1);

        // Remove only this specific entry from the tree (not the entire subtree).
        self.remove_single_entry(&lru_entry.path);
    }

    /// Remove a single entry's value from the tree without invalidating subtree.
    fn remove_single_entry(&mut self, path: &[String]) {
        if path.is_empty() {
            // Remove root's value.
            self.root.value = None;
            self.root.entry_id = None;
            self.root.recompute_deepest();
            return;
        }

        // Walk down to the target node.
        let mut nodes: Vec<*mut PathResolveCacheNode> = Vec::with_capacity(path.len() + 1);
        let mut node: *mut PathResolveCacheNode = &mut self.root;
        nodes.push(node);

        for c in path {
            let next = unsafe { &mut *node }.children.get_mut(c);
            let Some(next) = next else {
                return; // Path doesn't exist, nothing to remove.
            };
            node = next.as_mut();
            nodes.push(node);
        }

        // Clear the value at target node.
        let target = unsafe { &mut *node };
        target.value = None;
        target.entry_id = None;

        // Remove edge index entries for this path.
        // Only remove entries that exactly match this path length.
        let path_len = path.len();
        self.edge_index.retain(|_, comps| {
            if comps.len() != path_len {
                return true;
            }
            for (i, p) in path.iter().enumerate() {
                if &comps[i] != p {
                    return true;
                }
            }
            false
        });

        // Recompute deepest up the path.
        for n in nodes.into_iter().rev() {
            let n = unsafe { &mut *n };
            n.recompute_deepest();
        }
    }

    pub(crate) fn put_ids_for_path(&mut self, components: Vec<String>, ids: Vec<IndexNodeId>) {
        if ids.len() != components.len() + 1 {
            return;
        }

        // Check if this is an update to an existing entry.
        let is_new_entry = {
            let mut node = &self.root;
            let mut found = true;
            for c in &components {
                if let Some(next) = node.children.get(c) {
                    node = next.as_ref();
                } else {
                    found = false;
                    break;
                }
            }
            !found || node.value.is_none()
        };

        // If adding a new entry and we're at capacity, evict LRU entries.
        if is_new_entry && self.entry_count >= self.max_entries {
            self.evict_lru();
        }

        let v = Arc::new(ids);

        // Coverage rule: deeper cache covers shallow ones.
        self.clear_shallow_values_on_path(&components);

        // Insert.
        let mut path_nodes: Vec<*mut PathResolveCacheNode> =
            Vec::with_capacity(components.len() + 1);
        let mut node: *mut PathResolveCacheNode = &mut self.root;
        path_nodes.push(node);
        for c in &components {
            // SAFETY: only used for local recomputation after mutation.
            let next = unsafe { &mut *node }
                .children
                .entry(c.clone())
                .or_insert_with(|| Box::new(PathResolveCacheNode::default()));
            node = next.as_mut();
            path_nodes.push(node);
        }

        // Check if we're replacing an existing value.
        let target_node = unsafe { &mut *node };
        if let Some(old_entry_id) = target_node.entry_id.take() {
            self.lru_map.remove(&old_entry_id);
            self.entry_count = self.entry_count.saturating_sub(1);
        }

        target_node.value = Some(v.clone());
        target_node.deepest = Some(v.clone());

        // Assign new entry ID for LRU tracking.
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;
        target_node.entry_id = Some(entry_id);
        self.lru_map.insert(
            entry_id,
            LruEntry {
                path: components.clone(),
            },
        );
        self.entry_count += 1;

        // Update deepest up the path.
        for n in path_nodes.into_iter().rev() {
            let n = unsafe { &mut *n };
            let replace = match &n.deepest {
                Some(cur) => v.len() > cur.len(),
                None => true,
            };
            if replace {
                n.deepest = Some(v.clone());
            }
        }

        // Update edge index: (parent_inode, name) -> prefix path components.
        // components[i] corresponds to edge from ids[i] -> ids[i+1].
        for i in 0..components.len() {
            let parent = v[i];
            let name = components[i].clone();
            let prefix = components[..=i].to_vec();
            self.edge_index.insert((parent, name), prefix);
        }
    }

    /// Internal invalidation that optionally updates LRU map.
    fn invalidate_prefix_internal(&mut self, prefix: &[&str], update_lru: bool) {
        if prefix.is_empty() {
            self.root = PathResolveCacheNode::default();
            self.edge_index.clear();
            if update_lru {
                self.lru_map.clear();
                self.entry_count = 0;
            }
            return;
        }

        // Walk down to parent and collect entry IDs to remove.
        let mut nodes: Vec<*mut PathResolveCacheNode> = Vec::with_capacity(prefix.len() + 1);
        let mut node: *mut PathResolveCacheNode = &mut self.root;
        nodes.push(node);
        for c in &prefix[..prefix.len() - 1] {
            let next = unsafe { &mut *node }.children.get_mut(*c);
            let Some(next) = next else {
                return;
            };
            node = next.as_mut();
            nodes.push(node);
        }

        let leaf = prefix[prefix.len() - 1];

        // Before removing, collect all entry IDs in the subtree to be removed.
        if update_lru {
            if let Some(subtree) = unsafe { &mut *node }.children.get(leaf) {
                let entry_ids = Self::collect_entry_ids(subtree.as_ref());
                for id in entry_ids {
                    self.lru_map.remove(&id);
                    self.entry_count = self.entry_count.saturating_sub(1);
                }
            }
        }

        unsafe { &mut *node }.children.remove(leaf);

        // Drop edge index entries under this prefix.
        let prefix_len = prefix.len();
        self.edge_index.retain(|_, comps| {
            if comps.len() < prefix_len {
                return true;
            }
            for (i, p) in prefix.iter().enumerate() {
                if comps[i] != *p {
                    return true;
                }
            }
            false
        });

        // Recompute deepest up the path.
        for n in nodes.into_iter().rev() {
            let n = unsafe { &mut *n };
            n.recompute_deepest();
        }
    }

    fn invalidate_prefix(&mut self, prefix: &[&str]) {
        self.invalidate_prefix_internal(prefix, true);
    }

    /// Collect all entry IDs in a subtree (for LRU cleanup during invalidation).
    fn collect_entry_ids(node: &PathResolveCacheNode) -> Vec<EntryId> {
        let mut ids = Vec::new();
        if let Some(id) = node.entry_id {
            ids.push(id);
        }
        for child in node.children.values() {
            ids.extend(Self::collect_entry_ids(child.as_ref()));
        }
        ids
    }

    pub(crate) fn invalidate_by_edge(&mut self, parent: IndexNodeId, name: &str) {
        let key = (parent, name.to_string());
        let Some(prefix) = self.edge_index.get(&key).cloned() else {
            return;
        };
        let prefix_refs: Vec<&str> = prefix.iter().map(|s| s.as_str()).collect();
        self.invalidate_prefix(&prefix_refs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create path components from a slice of &str
    fn components(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    // ==================== Basic Put and Get Tests ====================

    #[test]
    fn test_put_and_get_single_component() {
        let mut cache = PathResolveCache::default();
        // Path: ["a"], IDs: [1, 2] (root=1, "a"=2)
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        let result = cache.get_ids_for_path(&["a"]);
        assert_eq!(result, Some(vec![1, 2]));
    }

    #[test]
    fn test_put_and_get_multiple_components() {
        let mut cache = PathResolveCache::default();
        // Path: ["a", "b", "c"], IDs: [1, 2, 3, 4]
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);

        let result = cache.get_ids_for_path(&["a", "b", "c"]);
        assert_eq!(result, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_get_nonexistent_path() {
        let mut cache = PathResolveCache::default();
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        // Query a path that doesn't exist
        let result = cache.get_ids_for_path(&["b"]);
        assert_eq!(result, None);

        let result = cache.get_ids_for_path(&["a", "b"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_empty_cache() {
        let mut cache = PathResolveCache::default();
        let result = cache.get_ids_for_path(&["a"]);
        assert_eq!(result, None);
    }

    // ==================== IDs Length Validation Tests ====================

    #[test]
    fn test_put_with_mismatched_ids_length_too_short() {
        let mut cache = PathResolveCache::default();
        // Path has 2 components, but IDs has only 2 elements (should be 3)
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2]);

        // Should not be cached due to length mismatch
        let result = cache.get_ids_for_path(&["a", "b"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_put_with_mismatched_ids_length_too_long() {
        let mut cache = PathResolveCache::default();
        // Path has 2 components, but IDs has 4 elements (should be 3)
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3, 4]);

        // Should not be cached due to length mismatch
        let result = cache.get_ids_for_path(&["a", "b"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_put_empty_path_with_single_id() {
        let mut cache = PathResolveCache::default();
        // Empty path should have exactly 1 ID (the root)
        cache.put_ids_for_path(vec![], vec![1]);

        // Query with empty path - get_ids_for_path returns value for root
        // The root node's value should be set
        let result = cache.get_ids_for_path(&[]);
        assert_eq!(result, Some(vec![1]));
    }

    // ==================== Deep Coverage Rule Tests ====================
    // "Deeper cache covers shallower ones" - when inserting a deeper path,
    // shallower paths on the same branch should have their values cleared.

    #[test]
    fn test_deep_covers_shallow() {
        let mut cache = PathResolveCache::default();

        // First, cache a shallow path
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));

        // Now cache a deeper path on the same branch
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);

        // The deeper path should be accessible
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));

        // Query shallow path ["a"] - should return prefix from deepest cache
        // Since deepest has [1, 2, 3], querying ["a"] returns [1, 2]
        let result = cache.get_ids_for_path(&["a"]);
        assert_eq!(result, Some(vec![1, 2]));
    }

    #[test]
    fn test_deep_covers_shallow_multiple_levels() {
        let mut cache = PathResolveCache::default();

        // Cache paths at different depths
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);

        // All should return appropriate prefixes from deepest
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));
        assert_eq!(
            cache.get_ids_for_path(&["a", "b", "c"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_deepest_propagates_correctly() {
        let mut cache = PathResolveCache::default();

        // Insert a deep path
        cache.put_ids_for_path(components(&["a", "b", "c", "d"]), vec![1, 2, 3, 4, 5]);

        // Query intermediate paths should use deepest value
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));
        assert_eq!(
            cache.get_ids_for_path(&["a", "b", "c"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    // ==================== Multiple Branches Tests ====================

    #[test]
    fn test_multiple_branches_independent() {
        let mut cache = PathResolveCache::default();

        // Two independent branches from root
        cache.put_ids_for_path(components(&["a", "x"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["b", "y"]), vec![1, 10, 11]);

        assert_eq!(cache.get_ids_for_path(&["a", "x"]), Some(vec![1, 2, 3]));
        assert_eq!(cache.get_ids_for_path(&["b", "y"]), Some(vec![1, 10, 11]));

        // Cross-paths should not exist
        assert_eq!(cache.get_ids_for_path(&["a", "y"]), None);
        assert_eq!(cache.get_ids_for_path(&["b", "x"]), None);
    }

    #[test]
    fn test_sibling_branches() {
        let mut cache = PathResolveCache::default();

        // Two branches from the same parent
        cache.put_ids_for_path(components(&["a", "b1"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["a", "b2"]), vec![1, 2, 4]);

        assert_eq!(cache.get_ids_for_path(&["a", "b1"]), Some(vec![1, 2, 3]));
        assert_eq!(cache.get_ids_for_path(&["a", "b2"]), Some(vec![1, 2, 4]));

        // Parent should use deepest from either branch
        let result = cache.get_ids_for_path(&["a"]);
        assert!(result == Some(vec![1, 2]));
    }

    #[test]
    fn test_deepest_selects_longer_branch() {
        let mut cache = PathResolveCache::default();

        // Two branches with different depths
        cache.put_ids_for_path(components(&["a", "short"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["a", "long", "path"]), vec![1, 2, 10, 11]);

        // Query ["a"] - should get prefix from the longer branch
        let result = cache.get_ids_for_path(&["a"]);
        assert_eq!(result, Some(vec![1, 2]));

        // Query ["a", "short"] - exact match
        assert_eq!(cache.get_ids_for_path(&["a", "short"]), Some(vec![1, 2, 3]));

        // Query ["a", "long"] - should get prefix from deeper cache
        assert_eq!(cache.get_ids_for_path(&["a", "long"]), Some(vec![1, 2, 10]));
    }

    // ==================== Invalidation Tests ====================

    #[test]
    fn test_invalidate_by_edge_simple() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));

        // Invalidate edge from root (1) to "a" (2)
        cache.invalidate_by_edge(1, "a");

        // Path should no longer exist
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
    }

    #[test]
    fn test_invalidate_by_edge_subtree() {
        let mut cache = PathResolveCache::default();

        // Create a subtree
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);
        cache.put_ids_for_path(components(&["a", "b", "d"]), vec![1, 2, 3, 5]);

        // Invalidate the "b" edge from "a" (2)
        cache.invalidate_by_edge(2, "b");

        // Entire subtree should be gone
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
        assert_eq!(cache.get_ids_for_path(&["a", "b", "c"]), None);
        assert_eq!(cache.get_ids_for_path(&["a", "b", "d"]), None);

        // But "a" might still have a node (though no value)
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
    }

    #[test]
    fn test_invalidate_by_edge_leaf_only() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);

        // Invalidate only the leaf edge "c"
        cache.invalidate_by_edge(3, "c");

        // Leaf should be gone
        assert_eq!(cache.get_ids_for_path(&["a", "b", "c"]), None);

        // Parent paths should still work via deepest, but deepest is now gone
        // So intermediate queries will return None
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
    }

    #[test]
    fn test_invalidate_by_edge_preserves_sibling() {
        let mut cache = PathResolveCache::default();

        // Two sibling paths
        cache.put_ids_for_path(components(&["a", "b1"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["a", "b2"]), vec![1, 2, 4]);

        // Invalidate only b1
        cache.invalidate_by_edge(2, "b1");

        // b1 should be gone
        assert_eq!(cache.get_ids_for_path(&["a", "b1"]), None);

        // b2 should still exist
        assert_eq!(cache.get_ids_for_path(&["a", "b2"]), Some(vec![1, 2, 4]));

        // Parent should now use b2's deepest
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
    }

    #[test]
    fn test_invalidate_nonexistent_edge() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        // Invalidate an edge that doesn't exist - should not crash or affect existing data
        cache.invalidate_by_edge(999, "nonexistent");

        // Existing data should be preserved
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
    }

    #[test]
    fn test_invalidate_root_edge() {
        let mut cache = PathResolveCache::default();

        // Multiple top-level entries
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);

        // Invalidate "a" from root
        cache.invalidate_by_edge(1, "a");

        assert_eq!(cache.get_ids_for_path(&["a"]), None);
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
    }

    // ==================== Edge Index Tests ====================

    #[test]
    fn test_edge_index_built_correctly() {
        let mut cache = PathResolveCache::default();

        // Path: ["a", "b", "c"], IDs: [1, 2, 3, 4]
        // Should create edges: (1, "a"), (2, "b"), (3, "c")
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);

        // Invalidate middle edge
        cache.invalidate_by_edge(2, "b");

        // "a" edge should still work
        assert_eq!(cache.get_ids_for_path(&["a"]), None); // No deepest anymore

        // Re-add and verify
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
    }

    #[test]
    fn test_edge_index_updated_on_overwrite() {
        let mut cache = PathResolveCache::default();

        // First insert
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);

        // Overwrite with different IDs
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 20, 30]);

        // Invalidate using new parent ID
        cache.invalidate_by_edge(20, "b");

        // Should be invalidated
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
    }

    // ==================== Overwrite/Update Tests ====================

    #[test]
    fn test_overwrite_same_path() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));

        // Overwrite with new IDs
        cache.put_ids_for_path(components(&["a"]), vec![1, 100]);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 100]));
    }

    #[test]
    fn test_shorter_path_after_longer() {
        let mut cache = PathResolveCache::default();

        // First insert a long path
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);

        // Now insert a shorter path on the same branch
        // This should clear the shorter path's value but longer path remains
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        // Both should work - deeper still accessible via tree structure
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(
            cache.get_ids_for_path(&["a", "b", "c"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    // ==================== Special Characters and Edge Cases ====================

    #[test]
    fn test_path_with_special_characters() {
        let mut cache = PathResolveCache::default();

        // Test various special characters in path names
        cache.put_ids_for_path(components(&["a b", "c.d", "e-f_g"]), vec![1, 2, 3, 4]);

        assert_eq!(
            cache.get_ids_for_path(&["a b", "c.d", "e-f_g"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_path_with_unicode() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["文件夹", "文件"]), vec![1, 2, 3]);

        assert_eq!(
            cache.get_ids_for_path(&["文件夹", "文件"]),
            Some(vec![1, 2, 3])
        );
        assert_eq!(cache.get_ids_for_path(&["文件夹"]), Some(vec![1, 2]));
    }

    #[test]
    fn test_path_with_empty_string_component() {
        let mut cache = PathResolveCache::default();

        // Empty string as a component (unusual but should work)
        cache.put_ids_for_path(components(&["a", "", "b"]), vec![1, 2, 3, 4]);

        assert_eq!(
            cache.get_ids_for_path(&["a", "", "b"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_very_long_path() {
        let mut cache = PathResolveCache::default();

        // Create a very long path
        let long_path: Vec<String> = (0..100).map(|i| format!("dir{}", i)).collect();
        let long_ids: Vec<IndexNodeId> = (1..=101).collect();

        let path_refs: Vec<&str> = long_path.iter().map(|s| s.as_str()).collect();

        cache.put_ids_for_path(long_path.clone(), long_ids.clone());

        assert_eq!(cache.get_ids_for_path(&path_refs), Some(long_ids));
    }

    // ==================== Deepest Recomputation Tests ====================

    #[test]
    fn test_deepest_recomputed_after_invalidation() {
        let mut cache = PathResolveCache::default();

        // Create two branches with different depths
        cache.put_ids_for_path(components(&["a", "deep", "path"]), vec![1, 2, 3, 4]);
        cache.put_ids_for_path(components(&["a", "short"]), vec![1, 2, 10]);

        // Initially deepest should be the longer path
        // Query ["a"] should return [1, 2] from deepest ([1,2,3,4])
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));

        // Invalidate the deeper path
        cache.invalidate_by_edge(2, "deep");

        // Now ["a", "short"] should be the deepest
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(
            cache.get_ids_for_path(&["a", "short"]),
            Some(vec![1, 2, 10])
        );
    }

    #[test]
    fn test_deepest_recomputed_when_all_children_removed() {
        let mut cache = PathResolveCache::default();

        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);

        // Invalidate the only child
        cache.invalidate_by_edge(2, "b");

        // Parent should have no deepest now
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
    }

    // ==================== Concurrent-like Scenario Tests ====================
    // (These test sequences of operations that might occur in concurrent usage)

    #[test]
    fn test_rapid_insert_invalidate_cycle() {
        let mut cache = PathResolveCache::default();

        for i in 0..10 {
            let name = format!("item{}", i);
            cache.put_ids_for_path(vec![name.clone()], vec![1, i as u64 + 10]);

            // Immediately invalidate every other one
            if i % 2 == 0 {
                cache.invalidate_by_edge(1, &name);
            }
        }

        // Only odd items should remain
        for i in 0..10 {
            let name = format!("item{}", i);
            let result = cache.get_ids_for_path(&[&name]);
            if i % 2 == 0 {
                assert_eq!(result, None, "item{} should be invalidated", i);
            } else {
                assert_eq!(
                    result,
                    Some(vec![1, i as u64 + 10]),
                    "item{} should exist",
                    i
                );
            }
        }
    }

    #[test]
    fn test_rebuild_after_full_invalidation() {
        let mut cache = PathResolveCache::default();

        // Build initial cache
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);
        cache.put_ids_for_path(components(&["a", "b", "d"]), vec![1, 2, 3, 5]);

        // Invalidate everything from root
        cache.invalidate_by_edge(1, "a");

        // Cache should be empty for this branch
        assert_eq!(cache.get_ids_for_path(&["a"]), None);

        // Rebuild
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 20, 30, 40]);

        // Should work with new IDs
        assert_eq!(
            cache.get_ids_for_path(&["a", "b", "c"]),
            Some(vec![1, 20, 30, 40])
        );
    }

    // ==================== Return Value Truncation Tests ====================

    #[test]
    fn test_get_returns_correct_prefix_length() {
        let mut cache = PathResolveCache::default();

        // Cache deep path
        cache.put_ids_for_path(components(&["a", "b", "c", "d"]), vec![1, 2, 3, 4, 5]);

        // Query for ["a"] should return [1, 2] (2 elements for 1 component + root)
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));

        // Query for ["a", "b"] should return [1, 2, 3] (3 elements)
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));

        // Query for ["a", "b", "c"] should return [1, 2, 3, 4] (4 elements)
        assert_eq!(
            cache.get_ids_for_path(&["a", "b", "c"]),
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_get_returns_none_when_deepest_too_shallow() {
        let mut cache = PathResolveCache::default();

        // Cache a shallow path
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        // Query for deeper path that doesn't exist
        // The deepest for node "a" is [1, 2], which has length 2
        // Query ["a", "b"] needs length 3, so should return None
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
    }

    // ==================== Stress Test ====================

    #[test]
    fn test_many_entries_same_parent() {
        let mut cache = PathResolveCache::default();

        // Create many entries under the same parent
        for i in 0..100 {
            let name = format!("child{}", i);
            cache.put_ids_for_path(vec!["parent".to_string(), name], vec![1, 2, i as u64 + 100]);
        }

        // All should be retrievable
        for i in 0..100 {
            let name = format!("child{}", i);
            assert_eq!(
                cache.get_ids_for_path(&["parent", &name]),
                Some(vec![1, 2, i as u64 + 100])
            );
        }

        // Parent should work
        assert_eq!(cache.get_ids_for_path(&["parent"]), Some(vec![1, 2]));
    }

    #[test]
    fn test_deeply_nested_structure() {
        let mut cache = PathResolveCache::default();

        // Create a deep nested structure
        let depth = 50;
        for d in 1..=depth {
            let path: Vec<String> = (0..d).map(|i| format!("level{}", i)).collect();
            let ids: Vec<IndexNodeId> = (1..=d as u64 + 1).collect();
            cache.put_ids_for_path(path, ids);
        }

        // Query various depths
        for d in 1..=depth {
            let path: Vec<&str> = (0..d)
                .map(|i| {
                    // Create static strings for the test
                    let s: &'static str = Box::leak(format!("level{}", i).into_boxed_str());
                    s
                })
                .collect();
            let expected: Vec<IndexNodeId> = (1..=d as u64 + 1).collect();
            assert_eq!(cache.get_ids_for_path(&path), Some(expected));
        }
    }

    // ==================== LRU Eviction Tests ====================

    #[test]
    fn test_lru_basic_eviction() {
        // Create a cache with max 3 entries
        let mut cache = PathResolveCache::new(3);

        // Insert 3 entries
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));

        // Insert 4th entry - should evict "a" (oldest)
        cache.put_ids_for_path(components(&["d"]), vec![1, 5]);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_ids_for_path(&["a"]), None); // Evicted
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
        assert_eq!(cache.get_ids_for_path(&["d"]), Some(vec![1, 5]));
    }

    #[test]
    fn test_lru_access_refreshes_entry() {
        // Create a cache with max 3 entries
        let mut cache = PathResolveCache::new(3);

        // Insert 3 entries
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);

        // Access "a" to refresh it (make it most recently used)
        let _ = cache.get_ids_for_path(&["a"]);

        // Insert 4th entry - should evict "b" (now oldest) instead of "a"
        cache.put_ids_for_path(components(&["d"]), vec![1, 5]);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2])); // Refreshed, not evicted
        assert_eq!(cache.get_ids_for_path(&["b"]), None); // Evicted
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
        assert_eq!(cache.get_ids_for_path(&["d"]), Some(vec![1, 5]));
    }

    #[test]
    fn test_lru_overwrite_does_not_increase_count() {
        let mut cache = PathResolveCache::new(3);

        // Insert 3 entries
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);

        assert_eq!(cache.len(), 3);

        // Overwrite "a" - count should stay at 3
        cache.put_ids_for_path(components(&["a"]), vec![1, 20]);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 20]));
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
    }

    #[test]
    fn test_lru_invalidation_updates_count() {
        let mut cache = PathResolveCache::new(5);

        // Insert entries
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);

        assert_eq!(cache.len(), 3);

        // Invalidate one
        cache.invalidate_by_edge(1, "b");

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(cache.get_ids_for_path(&["b"]), None);
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
    }

    #[test]
    fn test_lru_subtree_invalidation_updates_count() {
        let mut cache = PathResolveCache::new(10);

        // Create a subtree with multiple entries
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);
        cache.put_ids_for_path(components(&["a", "b", "d"]), vec![1, 2, 3, 5]);
        cache.put_ids_for_path(components(&["a", "e"]), vec![1, 2, 6]);

        assert_eq!(cache.len(), 3);

        // Invalidate the subtree under "b"
        cache.invalidate_by_edge(2, "b");

        // Only "a/e" should remain
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_ids_for_path(&["a", "e"]), Some(vec![1, 2, 6]));
    }

    #[test]
    fn test_lru_eviction_order() {
        let mut cache = PathResolveCache::new(3);

        // Insert a, b, c in order
        // LRU order after: a(0) < b(1) < c(2) (entry_id in parentheses)
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);

        // Access in order: c, a (b is not accessed, becomes oldest)
        // After accessing c: a(0) < b(1) < c(3)
        // After accessing a: b(1) < c(3) < a(4)
        let _ = cache.get_ids_for_path(&["c"]);
        let _ = cache.get_ids_for_path(&["a"]);

        // Insert d - should evict b (entry_id=1, oldest)
        // After: c(3) < a(4) < d(5)
        cache.put_ids_for_path(components(&["d"]), vec![1, 5]);

        // Check b is evicted (this doesn't refresh anything since b doesn't exist)
        assert_eq!(cache.get_ids_for_path(&["b"]), None);

        // Check others exist (note: these get calls will refresh them!)
        // After a: c(3) < d(5) < a(6)
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        // After c: d(5) < a(6) < c(7)
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
        // After d: a(6) < c(7) < d(8)
        assert_eq!(cache.get_ids_for_path(&["d"]), Some(vec![1, 5]));

        // Insert e - should evict a (entry_id=6, now oldest after the get refreshes)
        cache.put_ids_for_path(components(&["e"]), vec![1, 6]);

        assert_eq!(cache.get_ids_for_path(&["a"]), None); // a was evicted, not c
        assert_eq!(cache.get_ids_for_path(&["c"]), Some(vec![1, 4]));
        assert_eq!(cache.get_ids_for_path(&["d"]), Some(vec![1, 5]));
        assert_eq!(cache.get_ids_for_path(&["e"]), Some(vec![1, 6]));
    }

    #[test]
    fn test_lru_with_capacity_one() {
        let mut cache = PathResolveCache::new(1);

        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));

        // Insert second - evicts first
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
    }

    #[test]
    fn test_lru_deep_covers_shallow_eviction() {
        let mut cache = PathResolveCache::new(3);

        // Insert shallow path
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        assert_eq!(cache.len(), 1);

        // Insert deeper path on same branch - shallow value is cleared
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);
        // The shallow path's value was cleared, so count should still be 1
        // (only the deep path has a value now)
        assert_eq!(cache.len(), 1);

        // Both queries should work (deep path's value provides data for shallow query)
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_lru_stress_many_evictions() {
        let mut cache = PathResolveCache::new(10);

        // Insert 100 entries, causing many evictions
        for i in 0..100u64 {
            let name = format!("item{}", i);
            cache.put_ids_for_path(vec![name], vec![1, i + 10]);
        }

        // Should have exactly 10 entries
        assert_eq!(cache.len(), 10);

        // Only the last 10 should remain (items 90-99)
        for i in 0..90u64 {
            let name = format!("item{}", i);
            assert_eq!(
                cache.get_ids_for_path(&[&name]),
                None,
                "item{} should be evicted",
                i
            );
        }
        for i in 90..100u64 {
            let name = format!("item{}", i);
            assert_eq!(
                cache.get_ids_for_path(&[&name]),
                Some(vec![1, i + 10]),
                "item{} should exist",
                i
            );
        }
    }

    #[test]
    fn test_lru_access_during_stress() {
        let mut cache = PathResolveCache::new(5);

        // Insert 5 entries
        for i in 0..5u64 {
            let name = format!("item{}", i);
            cache.put_ids_for_path(vec![name], vec![1, i + 10]);
        }

        // Repeatedly access item0 to keep it fresh
        for _ in 0..10 {
            let _ = cache.get_ids_for_path(&["item0"]);

            // Insert a new item, causing eviction of oldest non-item0
            let name = format!("new{}", rand_id());
            cache.put_ids_for_path(vec![name], vec![1, 999]);
        }

        // item0 should still exist because we kept accessing it
        assert_eq!(cache.get_ids_for_path(&["item0"]), Some(vec![1, 10]));
    }

    // Helper for generating unique names in tests
    fn rand_id() -> u64 {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    #[test]
    fn test_lru_entry_count_consistency() {
        let mut cache = PathResolveCache::new(5);

        // Complex sequence of operations
        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        assert_eq!(cache.len(), 2);

        // Overwrite
        cache.put_ids_for_path(components(&["a"]), vec![1, 20]);
        assert_eq!(cache.len(), 2);

        // Deep covers shallow
        cache.put_ids_for_path(components(&["a", "x"]), vec![1, 20, 30]);
        assert_eq!(cache.len(), 2); // "a"'s value was cleared

        // Invalidate
        cache.invalidate_by_edge(1, "a");
        assert_eq!(cache.len(), 1); // Only "b" remains

        // Add more
        cache.put_ids_for_path(components(&["c"]), vec![1, 4]);
        cache.put_ids_for_path(components(&["d"]), vec![1, 5]);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_lru_zero_capacity() {
        // Edge case: zero capacity should still work (immediate eviction)
        let mut cache = PathResolveCache::new(0);

        cache.put_ids_for_path(components(&["a"]), vec![1, 2]);

        // With capacity 0, entries are immediately evicted when next one is added
        // But this first entry should exist until we add another
        // Actually, with capacity 0, we try to evict before adding, but lru_map is empty
        // So the entry is added but count becomes 1 > 0
        // The implementation evicts when count >= max_entries, so:
        // - First put: is_new_entry=true, count=0, max=0, 0>=0 is true, evict (nothing), then add
        // This means with capacity 0, every put tries to evict first

        // Let's verify the behavior
        assert_eq!(cache.len(), 1);

        // Adding second entry evicts first
        cache.put_ids_for_path(components(&["b"]), vec![1, 3]);
        // Before adding "b", count=1 >= 0, so we evict "a", then add "b"
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_ids_for_path(&["a"]), None);
        assert_eq!(cache.get_ids_for_path(&["b"]), Some(vec![1, 3]));
    }

    #[test]
    fn test_lru_large_capacity() {
        // Large capacity - no evictions should occur
        let mut cache = PathResolveCache::new(1000);

        for i in 0..100u64 {
            let name = format!("item{}", i);
            cache.put_ids_for_path(vec![name], vec![1, i + 10]);
        }

        assert_eq!(cache.len(), 100);

        // All entries should exist
        for i in 0..100u64 {
            let name = format!("item{}", i);
            assert_eq!(cache.get_ids_for_path(&[&name]), Some(vec![1, i + 10]));
        }
    }

    #[test]
    fn test_lru_eviction_with_deep_paths() {
        let mut cache = PathResolveCache::new(3);

        // Insert deep paths
        cache.put_ids_for_path(components(&["a", "b", "c"]), vec![1, 2, 3, 4]);
        cache.put_ids_for_path(components(&["x", "y"]), vec![1, 10, 11]);
        cache.put_ids_for_path(components(&["m"]), vec![1, 20]);

        assert_eq!(cache.len(), 3);

        // Insert another - should evict "a/b/c" (oldest)
        cache.put_ids_for_path(components(&["n"]), vec![1, 21]);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_ids_for_path(&["a", "b", "c"]), None);
        assert_eq!(cache.get_ids_for_path(&["x", "y"]), Some(vec![1, 10, 11]));
        assert_eq!(cache.get_ids_for_path(&["m"]), Some(vec![1, 20]));
        assert_eq!(cache.get_ids_for_path(&["n"]), Some(vec![1, 21]));
    }

    #[test]
    fn test_lru_eviction_cleans_tree_structure() {
        let mut cache = PathResolveCache::new(2);

        // Insert two deep paths on same branch
        cache.put_ids_for_path(components(&["a", "b"]), vec![1, 2, 3]);
        cache.put_ids_for_path(components(&["a", "c"]), vec![1, 2, 4]);

        assert_eq!(cache.len(), 2);

        // Insert third entry - evicts "a/b"
        cache.put_ids_for_path(components(&["x"]), vec![1, 10]);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_ids_for_path(&["a", "b"]), None);
        assert_eq!(cache.get_ids_for_path(&["a", "c"]), Some(vec![1, 2, 4]));
        assert_eq!(cache.get_ids_for_path(&["x"]), Some(vec![1, 10]));

        // "a" node should still exist (has child "c") but have no value
        // Query via deepest should still work
        assert_eq!(cache.get_ids_for_path(&["a"]), Some(vec![1, 2]));
    }
}
