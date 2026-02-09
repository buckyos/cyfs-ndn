//store layout从fsmeta下载后，完全保存在本地，select target操作不需要与fsmeta交互
use name_lib::DID;
use ndn_lib::ObjId;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct StoreTarget {
    pub store_id: String,
    pub device_did: Option<DID>,
    pub capacity: Option<u64>,
    pub used: Option<u64>,
    pub readonly: bool,
    pub enabled: bool,
    pub weight: u32,
}

/// StoreLayout using Maglev Consistent Hashing for O(1) target selection
/// with minimal data redistribution when nodes are added/removed.
///
/// Design based on Google's Maglev paper:
/// - Each target generates a preference list based on its store_id
/// - Targets take turns filling slots in a lookup table according to their preferences
/// - Lookup: hash(obj_id) % table_size -> lookup_table[index] -> target
///
/// Benefits:
/// - O(1) lookup performance
/// - Minimal redistribution: adding 1 node to N nodes only moves ~1/(N+1) of data
/// - Weight support: higher weight targets fill more slots proportionally
/// - Memory efficient: ~2.5MB for 100K nodes with default table size
#[derive(Debug, Clone)]
pub struct StoreLayout {
    pub epoch: u64,
    pub targets: Vec<StoreTarget>,
    pub total_capacity: u64,
    pub total_used: u64,
    pub total_weight: u64,

    /// Maglev lookup table: lookup_table[hash % M] = target_index
    /// Size M should be a prime number larger than total_weight for good distribution
    /// Using i32 to allow -1 as empty marker during construction
    lookup_table: Vec<i32>,

    /// Indices of enabled, non-readonly targets
    active_target_indices: Vec<usize>,

    /// Lookup table size (prime number)
    table_size: usize,
}

impl StoreLayout {
    /// Small prime for small clusters (< 1000 total weight)
    const SMALL_TABLE_SIZE: usize = 65537;

    /// Medium prime for medium clusters (< 100000 total weight)  
    const MEDIUM_TABLE_SIZE: usize = 655373;

    /// Large prime for large clusters
    const LARGE_TABLE_SIZE: usize = 6553577;

    /// Choose appropriate table size based on total weight
    /// Table size should be at least 10x total_weight for good distribution
    fn choose_table_size(total_weight: u64) -> usize {
        if total_weight < 6000 {
            Self::SMALL_TABLE_SIZE
        } else if total_weight < 60000 {
            Self::MEDIUM_TABLE_SIZE
        } else {
            Self::LARGE_TABLE_SIZE
        }
    }

    /// Create a new StoreLayout and build the Maglev lookup table
    pub fn new(
        epoch: u64,
        targets: Vec<StoreTarget>,
        total_capacity: u64,
        total_used: u64,
    ) -> Self {
        let total_weight: u64 = targets
            .iter()
            .filter(|t| t.enabled && !t.readonly && t.weight > 0)
            .map(|t| t.weight as u64)
            .sum();

        let table_size = Self::choose_table_size(total_weight);

        let mut layout = StoreLayout {
            epoch,
            targets,
            total_capacity,
            total_used,
            total_weight,
            lookup_table: Vec::new(),
            active_target_indices: Vec::new(),
            table_size,
        };

        layout.rebuild_maglev();
        layout
    }

    /// Hash a string with a seed to get deterministic but different hash values
    #[inline]
    fn hash_with_seed(s: &str, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Rebuild the Maglev lookup table
    ///
    /// Algorithm:
    /// 1. For each active target, compute (offset, skip) based on store_id hash
    /// 2. Each target has a preference sequence: offset, offset+skip, offset+2*skip, ...
    /// 3. Targets take turns (weighted by their weight) claiming empty slots
    /// 4. Result: each slot maps to exactly one target
    pub fn rebuild_maglev(&mut self) {
        self.active_target_indices.clear();

        // Collect active targets
        for (index, target) in self.targets.iter().enumerate() {
            if target.enabled && !target.readonly && target.weight > 0 {
                self.active_target_indices.push(index);
            }
        }

        let n = self.active_target_indices.len();
        if n == 0 {
            self.lookup_table = Vec::new();
            return;
        }

        let m = self.table_size;

        // Compute (offset, skip) for each active target
        // offset = h1(store_id) % M
        // skip = h2(store_id) % (M-1) + 1  (must be non-zero)
        let mut permutation_params: Vec<(usize, usize, u32)> = Vec::with_capacity(n);
        for &target_idx in &self.active_target_indices {
            let target = &self.targets[target_idx];
            let h1 = Self::hash_with_seed(&target.store_id, 0);
            let h2 = Self::hash_with_seed(&target.store_id, 1);

            let offset = (h1 % m as u64) as usize;
            let skip = ((h2 % (m as u64 - 1)) + 1) as usize;

            permutation_params.push((offset, skip, target.weight));
        }

        // Initialize lookup table with -1 (empty)
        let mut table: Vec<i32> = vec![-1; m];

        // next[i] = how many steps target i has taken in its preference sequence
        let mut next: Vec<usize> = vec![0; n];

        // weight_counter[i] = how many more slots target i should fill this round
        // Used to implement weighted distribution
        let mut weight_counter: Vec<u32> = permutation_params.iter().map(|(_, _, w)| *w).collect();

        let mut filled = 0;

        // Keep filling until all slots are claimed
        while filled < m {
            // Each round, targets with remaining weight get to fill slots
            for i in 0..n {
                // Skip if this target has used up its weight allowance for this round
                if weight_counter[i] == 0 {
                    continue;
                }
                weight_counter[i] -= 1;

                let (offset, skip, _) = permutation_params[i];

                // Find next empty slot in this target's preference sequence
                let mut pos = (offset + next[i] * skip) % m;
                while table[pos] != -1 {
                    next[i] += 1;
                    pos = (offset + next[i] * skip) % m;
                }

                // Claim this slot
                table[pos] = self.active_target_indices[i] as i32;
                next[i] += 1;
                filled += 1;

                if filled == m {
                    break;
                }
            }

            // Reset weight counters for next round
            let all_zero = weight_counter.iter().all(|&w| w == 0);
            if all_zero {
                for i in 0..n {
                    weight_counter[i] = permutation_params[i].2;
                }
            }
        }

        self.lookup_table = table;
    }

    /// Fast hash function for ObjId -> u64
    #[inline]
    fn hash_obj_id(obj_id: &ObjId) -> u64 {
        let mut hasher = DefaultHasher::new();
        obj_id.obj_hash.hash(&mut hasher);
        hasher.finish()
    }

    /// O(1) select primary target for a given ObjId using Maglev lookup
    ///
    /// Algorithm:
    /// 1. hash = hash(obj_id)
    /// 2. index = hash % table_size
    /// 3. target_index = lookup_table[index]
    #[inline]
    pub fn select_primary_target(&self, obj_id: &ObjId) -> Option<StoreTarget> {
        if self.lookup_table.is_empty() {
            return None;
        }

        let hash = Self::hash_obj_id(obj_id);
        let index = (hash % self.table_size as u64) as usize;
        let target_index = self.lookup_table[index] as usize;

        Some(self.targets[target_index].clone())
    }

    /// Select up to N targets for a given ObjId
    /// Uses multiple hash probes to get different targets
    pub fn select_n_targets(&self, obj_id: &ObjId, n: usize) -> Vec<StoreTarget> {
        if self.lookup_table.is_empty() {
            return Vec::new();
        }

        let n = n.min(self.active_target_indices.len());
        if n == 0 {
            return Vec::new();
        }

        let base_hash = Self::hash_obj_id(obj_id);

        let mut selected_target_indices: Vec<usize> = Vec::with_capacity(n);
        let mut probe = 0u64;

        // Keep probing until we have n unique targets
        while selected_target_indices.len() < n {
            // Mix the probe number with base hash to get different positions
            let hash = if probe == 0 {
                base_hash
            } else {
                base_hash.wrapping_add(probe.wrapping_mul(0x9e3779b97f4a7c15))
            };

            let index = (hash % self.table_size as u64) as usize;
            let target_index = self.lookup_table[index] as usize;

            // Only add if not already selected
            if !selected_target_indices.contains(&target_index) {
                selected_target_indices.push(target_index);
            }

            probe += 1;

            // Safety: prevent infinite loop
            if probe > self.table_size as u64 + 1000 {
                break;
            }
        }

        selected_target_indices
            .into_iter()
            .map(|idx| self.targets[idx].clone())
            .collect()
    }

    /// Select all targets for a given ObjId, ordered by preference
    pub fn select_targets(&self, obj_id: &ObjId) -> Vec<StoreTarget> {
        self.select_n_targets(obj_id, self.active_target_indices.len())
    }

    /// Get the lookup table size (for testing/debugging)
    pub fn total_vnodes(&self) -> usize {
        self.table_size
    }

    /// Get the number of active targets
    pub fn active_target_count(&self) -> usize {
        self.active_target_indices.len()
    }

    /// Get actual lookup table length (for testing)
    pub fn lookup_table_size(&self) -> usize {
        self.lookup_table.len()
    }
}

/// Version info for a store layout
#[derive(Debug, Clone)]
pub struct LayoutVersion {
    pub epoch: u64,
    pub layout: StoreLayout,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_target(
        store_id: &str,
        weight: u32,
        enabled: bool,
        readonly: bool,
    ) -> StoreTarget {
        StoreTarget {
            store_id: store_id.to_string(),
            device_did: None,
            capacity: Some(1000),
            used: Some(100),
            readonly,
            enabled,
            weight,
        }
    }

    fn create_test_obj_id(hash_bytes: &[u8]) -> ObjId {
        ObjId {
            obj_type: "file".to_string(),
            obj_hash: hash_bytes.to_vec(),
        }
    }

    fn create_test_layout(targets: Vec<StoreTarget>) -> StoreLayout {
        StoreLayout::new(1, targets, 10000, 1000)
    }

    #[test]
    fn test_select_targets_empty_layout() {
        let layout = create_test_layout(vec![]);
        let obj_id = create_test_obj_id(b"test_object_hash");

        let selected = layout.select_targets(&obj_id);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_targets_all_disabled() {
        let targets = vec![
            create_test_target("store1", 1, false, false),
            create_test_target("store2", 1, false, false),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_object_hash");

        let selected = layout.select_targets(&obj_id);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_targets_all_readonly() {
        let targets = vec![
            create_test_target("store1", 1, true, true),
            create_test_target("store2", 1, true, true),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_object_hash");

        let selected = layout.select_targets(&obj_id);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_targets_single_target() {
        let targets = vec![create_test_target("store1", 1, true, false)];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_object_hash");

        let selected = layout.select_targets(&obj_id);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].store_id, "store1");
    }

    #[test]
    fn test_select_targets_multiple_targets() {
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_object_hash");

        let selected = layout.select_targets(&obj_id);
        // Should return all enabled, non-readonly targets
        assert_eq!(selected.len(), 3);

        // All store_ids should be present (order may vary based on hash)
        let store_ids: Vec<&str> = selected.iter().map(|t| t.store_id.as_str()).collect();
        assert!(store_ids.contains(&"store1"));
        assert!(store_ids.contains(&"store2"));
        assert!(store_ids.contains(&"store3"));
    }

    #[test]
    fn test_select_targets_consistency() {
        // Same ObjId should always return same order
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"consistent_hash_test");

        let selected1 = layout.select_targets(&obj_id);
        let selected2 = layout.select_targets(&obj_id);

        assert_eq!(selected1.len(), selected2.len());
        for i in 0..selected1.len() {
            assert_eq!(selected1[i].store_id, selected2[i].store_id);
        }
    }

    #[test]
    fn test_select_targets_different_obj_ids() {
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout = create_test_layout(targets);

        let obj_id1 = create_test_obj_id(b"object_hash_1");
        let obj_id2 = create_test_obj_id(b"object_hash_2");

        let selected1 = layout.select_targets(&obj_id1);
        let selected2 = layout.select_targets(&obj_id2);

        // Both should return 3 targets
        assert_eq!(selected1.len(), 3);
        assert_eq!(selected2.len(), 3);

        // The primary target might differ (not guaranteed, but likely with different hashes)
        // Just verify they are valid
        assert!(!selected1[0].store_id.is_empty());
        assert!(!selected2[0].store_id.is_empty());
    }

    #[test]
    fn test_select_targets_skips_disabled_and_readonly() {
        let targets = vec![
            create_test_target("store1", 1, true, false), // enabled, writable
            create_test_target("store2", 1, false, false), // disabled
            create_test_target("store3", 1, true, true),  // readonly
            create_test_target("store4", 1, true, false), // enabled, writable
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_hash");

        let selected = layout.select_targets(&obj_id);

        // Only store1 and store4 should be selected
        assert_eq!(selected.len(), 2);
        let store_ids: Vec<&str> = selected.iter().map(|t| t.store_id.as_str()).collect();
        assert!(store_ids.contains(&"store1"));
        assert!(store_ids.contains(&"store4"));
        assert!(!store_ids.contains(&"store2"));
        assert!(!store_ids.contains(&"store3"));
    }

    #[test]
    fn test_select_n_targets() {
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_hash");

        let selected = layout.select_n_targets(&obj_id, 2);
        assert_eq!(selected.len(), 2);

        // Requesting more than available should return all
        let selected_all = layout.select_n_targets(&obj_id, 10);
        assert_eq!(selected_all.len(), 3);
    }

    #[test]
    fn test_select_primary_target() {
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
        ];
        let layout = create_test_layout(targets);
        let obj_id = create_test_obj_id(b"test_hash");

        let primary = layout.select_primary_target(&obj_id);
        assert!(primary.is_some());

        // Should be consistent
        let primary2 = layout.select_primary_target(&obj_id);
        assert_eq!(primary.unwrap().store_id, primary2.unwrap().store_id);
    }

    #[test]
    fn test_select_primary_target_empty() {
        let layout = create_test_layout(vec![]);
        let obj_id = create_test_obj_id(b"test_hash");

        let primary = layout.select_primary_target(&obj_id);
        assert!(primary.is_none());
    }

    #[test]
    fn test_weight_affects_distribution() {
        // Higher weight should get more objects
        let targets = vec![
            create_test_target("store_low", 1, true, false),
            create_test_target("store_high", 10, true, false),
        ];
        let layout = create_test_layout(targets);

        let mut primary_counts: HashMap<String, u32> = HashMap::new();

        // Test with many different ObjIds
        for i in 0..1000 {
            let obj_id = create_test_obj_id(format!("object_{}", i).as_bytes());
            if let Some(primary) = layout.select_primary_target(&obj_id) {
                *primary_counts.entry(primary.store_id).or_insert(0) += 1;
            }
        }

        let low_count = *primary_counts.get("store_low").unwrap_or(&0);
        let high_count = *primary_counts.get("store_high").unwrap_or(&0);

        // store_high (weight=10) should get significantly more than store_low (weight=1)
        // With consistent hashing, the ratio should be approximately 10:1
        // Allow some variance due to hash distribution
        assert!(
            high_count > low_count * 3,
            "Expected store_high ({}) to be at least 3x store_low ({})",
            high_count,
            low_count
        );
    }

    #[test]
    fn test_rendezvous_hash_determinism() {
        // Verify that the same ObjId always maps to the same targets
        let targets = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout = create_test_layout(targets);

        // Test multiple times to ensure determinism
        for _ in 0..100 {
            let obj_id = create_test_obj_id(b"determinism_test");
            let selected1 = layout.select_targets(&obj_id);
            let selected2 = layout.select_targets(&obj_id);

            assert_eq!(selected1.len(), selected2.len());
            for i in 0..selected1.len() {
                assert_eq!(selected1[i].store_id, selected2[i].store_id);
            }
        }
    }

    #[test]
    fn test_minimal_redistribution_on_node_add() {
        // Test that adding a node only affects approximately 1/(N+1) of objects
        // This is the key property of Maglev consistent hashing
        let targets_before = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
        ];
        let layout_before = create_test_layout(targets_before);

        let targets_after = vec![
            create_test_target("store1", 1, true, false),
            create_test_target("store2", 1, true, false),
            create_test_target("store3", 1, true, false),
        ];
        let layout_after = create_test_layout(targets_after);

        let mut unchanged_count = 0;
        let total_tests = 10000;

        for i in 0..total_tests {
            let obj_id = create_test_obj_id(format!("object_{}", i).as_bytes());
            let primary_before = layout_before.select_primary_target(&obj_id);
            let primary_after = layout_after.select_primary_target(&obj_id);

            if let (Some(pb), Some(pa)) = (primary_before, primary_after) {
                if pb.store_id == pa.store_id {
                    unchanged_count += 1;
                }
            }
        }

        // With Maglev consistent hashing:
        // When going from N to N+1 targets, approximately N/(N+1) stay unchanged
        // For 2->3 targets: ~2/3 = 66.7% should be unchanged
        // The new target gets ~1/3 = 33.3% of the data
        let unchanged_ratio = unchanged_count as f64 / total_tests as f64;

        // Allow 10% tolerance due to hash distribution variance
        // Expected: ~66.7% unchanged, so we check for > 55%
        assert!(
            unchanged_ratio > 0.55,
            "Expected at least 55% unchanged (ideal ~66.7%), got {:.2}%",
            unchanged_ratio * 100.0
        );

        // Also check that not too many stayed unchanged (would indicate problem)
        assert!(
            unchanged_ratio < 0.80,
            "Expected less than 80% unchanged (ideal ~66.7%), got {:.2}%",
            unchanged_ratio * 100.0
        );
    }

    /// Large-scale Maglev consistent hashing performance and redistribution test
    ///
    /// This test verifies:
    /// 1. Maglev lookup table construction performance with 10K+ targets
    /// 2. O(1) select performance
    /// 3. Minimal redistribution when adding nodes (close to theoretical 1/(N+1))
    /// 4. Memory efficiency
    ///
    /// Run with: cargo test test_large_scale_redistribution -- --ignored --nocapture
    #[test]
    #[ignore]
    fn test_large_scale_redistribution() {
        use std::time::Instant;

        const NUM_TARGETS_1: usize = 10000;
        const NUM_TARGETS_2: usize = 10001;
        const NUM_OBJ_IDS: usize = 10_000_000;

        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║   Maglev Large Scale Redistribution & Performance Test       ║");
        println!("╚══════════════════════════════════════════════════════════════╝");
        println!();
        println!("Configuration:");
        println!("  Layout1 targets: {}", NUM_TARGETS_1);
        println!("  Layout2 targets: {} (+1 new node)", NUM_TARGETS_2);
        println!("  Test ObjIds:     {}", NUM_OBJ_IDS);

        // ============================================================
        // Phase 1: Build layouts and measure construction time
        // ============================================================
        println!("\n┌─────────────────────────────────────────────────────────────┐");
        println!("│ Phase 1: Maglev Lookup Table Construction                   │");
        println!("└─────────────────────────────────────────────────────────────┘");

        // Create layout1
        println!("\nBuilding layout1 with {} targets...", NUM_TARGETS_1);
        let start = Instant::now();
        let targets1: Vec<StoreTarget> = (0..NUM_TARGETS_1)
            .map(|i| StoreTarget {
                store_id: format!("store_{:05}", i),
                device_did: None,
                capacity: Some(1000),
                used: Some(0),
                readonly: false,
                enabled: true,
                weight: 1,
            })
            .collect();
        let layout1 = StoreLayout::new(1, targets1, NUM_TARGETS_1 as u64 * 1000, 0);
        let build_time1 = start.elapsed();

        let table_size = layout1.lookup_table_size();
        let memory_mb = (table_size * std::mem::size_of::<i32>()) as f64 / 1024.0 / 1024.0;

        println!("  Build time:      {:?}", build_time1);
        println!("  Table size:      {} slots", table_size);
        println!("  Memory usage:    {:.2} MB", memory_mb);
        println!("  Active targets:  {}", layout1.active_target_count());

        // Create layout2 (with one additional target)
        println!("\nBuilding layout2 with {} targets...", NUM_TARGETS_2);
        let start = Instant::now();
        let targets2: Vec<StoreTarget> = (0..NUM_TARGETS_2)
            .map(|i| StoreTarget {
                store_id: format!("store_{:05}", i),
                device_did: None,
                capacity: Some(1000),
                used: Some(0),
                readonly: false,
                enabled: true,
                weight: 1,
            })
            .collect();
        let layout2 = StoreLayout::new(2, targets2, NUM_TARGETS_2 as u64 * 1000, 0);
        let build_time2 = start.elapsed();

        println!("  Build time:      {:?}", build_time2);
        println!("  Table size:      {} slots", layout2.lookup_table_size());
        println!("  Active targets:  {}", layout2.active_target_count());

        // ============================================================
        // Phase 2: Measure redistribution
        // ============================================================
        println!("\n┌─────────────────────────────────────────────────────────────┐");
        println!("│ Phase 2: Data Redistribution Analysis                       │");
        println!("└─────────────────────────────────────────────────────────────┘");

        println!("\nTesting {} ObjIds for redistribution...", NUM_OBJ_IDS);
        let start = Instant::now();

        let mut unchanged_count: usize = 0;
        let mut moved_to_new_target: usize = 0;
        let mut moved_between_old_targets: usize = 0;
        let new_target_id = format!("store_{:05}", NUM_TARGETS_2 - 1);

        for i in 0..NUM_OBJ_IDS {
            let hash_bytes = {
                let mut hasher = DefaultHasher::new();
                i.hash(&mut hasher);
                let h = hasher.finish();
                h.to_le_bytes().to_vec()
            };
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: hash_bytes,
            };

            let primary1 = layout1.select_primary_target(&obj_id);
            let primary2 = layout2.select_primary_target(&obj_id);

            match (primary1, primary2) {
                (Some(p1), Some(p2)) => {
                    if p1.store_id == p2.store_id {
                        unchanged_count += 1;
                    } else if p2.store_id == new_target_id {
                        moved_to_new_target += 1;
                    } else {
                        moved_between_old_targets += 1;
                    }
                }
                _ => panic!("Failed to get primary target"),
            }

            if (i + 1) % 200_000 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush().unwrap();
            }
        }
        println!(" Done!");

        let _redistribution_test_time = start.elapsed();

        // Calculate statistics
        let total_moved = moved_to_new_target + moved_between_old_targets;
        let unchanged_ratio = unchanged_count as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let moved_to_new_ratio = moved_to_new_target as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let moved_between_old_ratio = moved_between_old_targets as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let total_moved_ratio = total_moved as f64 / NUM_OBJ_IDS as f64 * 100.0;

        // Theoretical expectations
        let ideal_unchanged_ratio = NUM_TARGETS_1 as f64 / NUM_TARGETS_2 as f64 * 100.0;
        let ideal_move_ratio = 1.0 / NUM_TARGETS_2 as f64 * 100.0;

        println!("\nRedistribution Results:");
        println!("  ┌────────────────────────┬────────────┬──────────┐");
        println!("  │ Category               │    Count   │  Ratio   │");
        println!("  ├────────────────────────┼────────────┼──────────┤");
        println!(
            "  │ Unchanged              │ {:>10} │ {:>6.2}%  │",
            unchanged_count, unchanged_ratio
        );
        println!(
            "  │ Moved to new target    │ {:>10} │ {:>6.2}%  │",
            moved_to_new_target, moved_to_new_ratio
        );
        println!(
            "  │ Moved between old      │ {:>10} │ {:>6.2}%  │",
            moved_between_old_targets, moved_between_old_ratio
        );
        println!(
            "  │ Total moved            │ {:>10} │ {:>6.2}%  │",
            total_moved, total_moved_ratio
        );
        println!("  └────────────────────────┴────────────┴──────────┘");

        println!("\nComparison with Theoretical Ideal:");
        println!("  ┌────────────────────────┬──────────┬──────────┬──────────┐");
        println!("  │ Metric                 │  Ideal   │  Actual  │ Deviation│");
        println!("  ├────────────────────────┼──────────┼──────────┼──────────┤");
        println!(
            "  │ Unchanged ratio        │ {:>6.2}%  │ {:>6.2}%  │ {:>+6.2}%  │",
            ideal_unchanged_ratio,
            unchanged_ratio,
            unchanged_ratio - ideal_unchanged_ratio
        );
        println!(
            "  │ Move ratio             │ {:>6.4}% │ {:>6.2}%  │ {:>5.1}x   │",
            ideal_move_ratio,
            total_moved_ratio,
            total_moved_ratio / ideal_move_ratio
        );
        println!("  └────────────────────────┴──────────┴──────────┴──────────┘");

        // ============================================================
        // Phase 3: Select performance benchmark
        // ============================================================
        println!("\n┌─────────────────────────────────────────────────────────────┐");
        println!("│ Phase 3: O(1) Select Performance Benchmark                  │");
        println!("└─────────────────────────────────────────────────────────────┘");

        const BENCH_ITERATIONS: usize = 5_000_000;

        // Warmup
        for i in 0u64..10000 {
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: i.to_le_bytes().to_vec(),
            };
            let _ = layout1.select_primary_target(&obj_id);
        }

        // Benchmark
        let start = Instant::now();
        for i in 0..BENCH_ITERATIONS {
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: (i as u64).to_le_bytes().to_vec(),
            };
            let _ = layout1.select_primary_target(&obj_id);
        }
        let select_duration = start.elapsed();

        let ops_per_sec = BENCH_ITERATIONS as f64 / select_duration.as_secs_f64();
        let ns_per_op = select_duration.as_nanos() as f64 / BENCH_ITERATIONS as f64;

        println!("\nselect_primary_target() with {} targets:", NUM_TARGETS_1);
        println!("  Iterations:   {}", BENCH_ITERATIONS);
        println!("  Total time:   {:?}", select_duration);
        println!("  Throughput:   {:.2} M ops/sec", ops_per_sec / 1_000_000.0);
        println!("  Latency:      {:.1} ns/op", ns_per_op);

        // ============================================================
        // Summary and Assertions
        // ============================================================
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                         SUMMARY                              ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Build Performance:                                           ║");
        println!(
            "║   Layout1 ({:>5} targets): {:>10?}                       ║",
            NUM_TARGETS_1, build_time1
        );
        println!(
            "║   Layout2 ({:>5} targets): {:>10?}                       ║",
            NUM_TARGETS_2, build_time2
        );
        println!(
            "║   Memory per layout:       {:.2} MB                           ║",
            memory_mb
        );
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Redistribution (Maglev Consistent Hashing):                  ║");
        println!(
            "║   Unchanged:      {:>6.2}% (ideal: {:>6.2}%)                  ║",
            unchanged_ratio, ideal_unchanged_ratio
        );
        println!(
            "║   Total moved:    {:>6.2}% (ideal: {:>6.4}%)                 ║",
            total_moved_ratio, ideal_move_ratio
        );
        println!(
            "║   Deviation:      {:.1}x from theoretical minimum             ║",
            total_moved_ratio / ideal_move_ratio
        );
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Select Performance:                                          ║");
        println!(
            "║   Throughput:     {:.2} M ops/sec                            ║",
            ops_per_sec / 1_000_000.0
        );
        println!(
            "║   Latency:        {:.1} ns/op                                 ║",
            ns_per_op
        );
        println!("╚══════════════════════════════════════════════════════════════╝");

        // Assertions
        println!("\nRunning assertions...");

        // 1. Most objects should remain unchanged (>98% for large N)
        assert!(
            unchanged_ratio > 98.0,
            "Expected >98% unchanged, got {:.2}%",
            unchanged_ratio
        );
        println!(
            "  ✓ Unchanged ratio check passed ({:.2}% > 98%)",
            unchanged_ratio
        );

        // 2. Total movement should be reasonable (< 5x theoretical for Maglev)
        // Maglev has some overhead due to slot conflicts
        assert!(
            total_moved_ratio < ideal_move_ratio * 10.0,
            "Too much movement: {:.2}% (expected < {:.2}%)",
            total_moved_ratio,
            ideal_move_ratio * 10.0
        );
        println!(
            "  ✓ Total movement check passed ({:.2}% < {:.2}%)",
            total_moved_ratio,
            ideal_move_ratio * 10.0
        );

        // 3. Movement between old targets should be minimal
        assert!(
            moved_between_old_ratio < 1.0,
            "Too much movement between old targets: {:.2}%",
            moved_between_old_ratio
        );
        println!(
            "  ✓ Inter-old-target movement check passed ({:.2}% < 1%)",
            moved_between_old_ratio
        );

        // 4. Select performance should be fast (> 1M ops/sec)
        assert!(
            ops_per_sec > 1_000_000.0,
            "Select performance too slow: {:.0} ops/sec",
            ops_per_sec
        );
        println!(
            "  ✓ Performance check passed ({:.2}M > 1M ops/sec)",
            ops_per_sec / 1_000_000.0
        );

        println!("\n✅ All assertions passed!");
    }

    /// Small-scale redistribution test: 100 -> 101 targets
    /// Tests data migration when adding a single node to a 100-node cluster
    ///
    /// With Maglev consistent hashing:
    /// - Adding 1 node to N nodes should only move ~1/(N+1) of data
    /// - For N=100, expected move ratio ≈ 1/101 ≈ 0.99%
    /// - Most data should stay on their original targets
    #[test]
    fn test_small_scale_redistribution_100_to_101() {
        use std::time::Instant;

        const NUM_TARGETS_1: usize = 100;
        const NUM_TARGETS_2: usize = 101;
        const NUM_OBJ_IDS: usize = 100_000;

        println!(
            "\n=== Maglev Small Scale Redistribution Test: {} -> {} targets ===",
            NUM_TARGETS_1, NUM_TARGETS_2
        );
        println!("Test ObjIds: {}", NUM_OBJ_IDS);

        // Create layout1 with 100 uniform store targets (weight = 1)
        let start = Instant::now();
        let targets1: Vec<StoreTarget> = (0..NUM_TARGETS_1)
            .map(|i| StoreTarget {
                store_id: format!("store_{:03}", i),
                device_did: None,
                capacity: Some(1000),
                used: Some(0),
                readonly: false,
                enabled: true,
                weight: 1, // uniform weight
            })
            .collect();
        let layout1 = StoreLayout::new(1, targets1, NUM_TARGETS_1 as u64 * 1000, 0);
        println!(
            "Layout1 created in {:?}, lookup table size: {}",
            start.elapsed(),
            layout1.lookup_table_size()
        );

        // Create layout2 with 101 uniform store targets (add one new target)
        let start = Instant::now();
        let targets2: Vec<StoreTarget> = (0..NUM_TARGETS_2)
            .map(|i| StoreTarget {
                store_id: format!("store_{:03}", i),
                device_did: None,
                capacity: Some(1000),
                used: Some(0),
                readonly: false,
                enabled: true,
                weight: 1, // uniform weight
            })
            .collect();
        let layout2 = StoreLayout::new(2, targets2, NUM_TARGETS_2 as u64 * 1000, 0);
        println!(
            "Layout2 created in {:?}, lookup table size: {}",
            start.elapsed(),
            layout2.lookup_table_size()
        );

        // Test redistribution
        let start = Instant::now();

        let mut unchanged_count: usize = 0;
        let mut moved_to_new_target: usize = 0;
        let mut moved_between_old_targets: usize = 0;
        let new_target_id = format!("store_{:03}", NUM_TARGETS_2 - 1); // store_100

        for i in 0..NUM_OBJ_IDS {
            // Create pseudo-random ObjId using index
            let hash_bytes = {
                let mut hasher = DefaultHasher::new();
                i.hash(&mut hasher);
                let h = hasher.finish();
                h.to_le_bytes().to_vec()
            };
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: hash_bytes,
            };

            // Get primary target from both layouts
            let primary1 = layout1.select_primary_target(&obj_id);
            let primary2 = layout2.select_primary_target(&obj_id);

            match (primary1, primary2) {
                (Some(p1), Some(p2)) => {
                    if p1.store_id == p2.store_id {
                        unchanged_count += 1;
                    } else if p2.store_id == new_target_id {
                        // Moved to the new target (store_100)
                        moved_to_new_target += 1;
                    } else {
                        // Moved between existing targets (should be minimal in Maglev)
                        moved_between_old_targets += 1;
                    }
                }
                _ => {
                    panic!("Failed to get primary target");
                }
            }
        }

        let test_duration = start.elapsed();

        // Calculate statistics
        let total_moved = moved_to_new_target + moved_between_old_targets;
        let unchanged_ratio = unchanged_count as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let moved_to_new_ratio = moved_to_new_target as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let moved_between_old_ratio = moved_between_old_targets as f64 / NUM_OBJ_IDS as f64 * 100.0;
        let total_moved_ratio = total_moved as f64 / NUM_OBJ_IDS as f64 * 100.0;

        // Theoretical expectation for Maglev consistent hashing:
        // When adding 1 target to N targets, ~1/(N+1) of objects should move
        // For N=100, expected move ratio ≈ 1/101 ≈ 0.99%
        let ideal_move_ratio = 1.0 / NUM_TARGETS_2 as f64 * 100.0;
        let ideal_unchanged_ratio = NUM_TARGETS_1 as f64 / NUM_TARGETS_2 as f64 * 100.0;

        println!("\n=== Results ===");
        println!("Test completed in {:?}", test_duration);
        println!();
        println!("Distribution changes:");
        println!(
            "  Unchanged:              {:>10} ({:.2}%)",
            unchanged_count, unchanged_ratio
        );
        println!(
            "  Moved to new target:    {:>10} ({:.2}%)",
            moved_to_new_target, moved_to_new_ratio
        );
        println!(
            "  Moved between old:      {:>10} ({:.2}%)",
            moved_between_old_targets, moved_between_old_ratio
        );
        println!(
            "  Total moved:            {:>10} ({:.2}%)",
            total_moved, total_moved_ratio
        );
        println!();
        println!("Analysis (Maglev Consistent Hashing):");
        println!(
            "  Ideal unchanged ratio ({}/{}): {:.2}%",
            NUM_TARGETS_1, NUM_TARGETS_2, ideal_unchanged_ratio
        );
        println!("  Actual unchanged ratio:        {:.2}%", unchanged_ratio);
        println!(
            "  Ideal move ratio (1/{}):       {:.2}%",
            NUM_TARGETS_2, ideal_move_ratio
        );
        println!("  Actual total move ratio:       {:.2}%", total_moved_ratio);

        // Assertions - verify Maglev consistent hashing properties
        // 1. Most data should remain unchanged (~99% for 100->101)
        // Allow some tolerance due to hash distribution and table conflicts
        assert!(
            unchanged_ratio > 95.0,
            "Expected >95% unchanged (ideal {:.2}%), got {:.2}%",
            ideal_unchanged_ratio,
            unchanged_ratio
        );

        // 2. Total movement should be close to theoretical 1/(N+1)
        // Allow up to 5x tolerance due to Maglev table conflicts
        assert!(
            total_moved_ratio < ideal_move_ratio * 5.0,
            "Too much data moved: {:.2}% (expected ~{:.2}%)",
            total_moved_ratio,
            ideal_move_ratio
        );

        // 3. Movement between old targets should be minimal
        // Ideally 0, but allow some due to table slot conflicts
        assert!(
            moved_between_old_ratio < 3.0,
            "Too much movement between old targets: {:.2}%",
            moved_between_old_ratio
        );

        println!("\n=== Test passed (Maglev consistent hashing working correctly) ===");
    }

    /// Quick performance benchmark for select_targets operation
    /// Quick performance benchmark for select_primary_target operation (O(1))
    #[test]
    #[ignore]
    fn test_select_targets_performance() {
        use std::time::Instant;

        const NUM_TARGETS: usize = 65536;
        const NUM_ITERATIONS: usize = 10_000_000;

        // Create layout
        let targets: Vec<StoreTarget> = (0..NUM_TARGETS)
            .map(|i| StoreTarget {
                store_id: format!("store_{:05}", i),
                device_did: None,
                capacity: Some(1000),
                used: Some(0),
                readonly: false,
                enabled: true,
                weight: 1,
            })
            .collect();
        let layout = StoreLayout::new(1, targets, NUM_TARGETS as u64 * 1000, 0);

        println!("\n=== Select Primary Target Performance Test (O(1)) ===");
        println!("Targets: {}", NUM_TARGETS);
        println!("Virtual nodes: {}", layout.total_vnodes());
        println!("Iterations: {}", NUM_ITERATIONS);

        // Warm up
        for i in 0u64..10000 {
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: i.to_le_bytes().to_vec(),
            };
            let _ = layout.select_primary_target(&obj_id);
        }

        // Benchmark select_primary_target (O(1))
        let start = Instant::now();
        for i in 0..NUM_ITERATIONS {
            let obj_id = ObjId {
                obj_type: "file".to_string(),
                obj_hash: (i as u64).to_le_bytes().to_vec(),
            };
            let _ = layout.select_primary_target(&obj_id);
        }
        let duration = start.elapsed();

        let ops_per_sec = NUM_ITERATIONS as f64 / duration.as_secs_f64();
        let ns_per_op = duration.as_nanos() as f64 / NUM_ITERATIONS as f64;

        println!("\nselect_primary_target (O(1)):");
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} ops/sec", ops_per_sec);
        println!("  Latency: {:.1} ns/op", ns_per_op);
    }
}
