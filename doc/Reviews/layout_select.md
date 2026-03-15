下面是对你这份 `store_layout.rs` 的一次“偏工程落地”的 Review（重点围绕：**O(1) select、扩容迁移量≈新增容量、以及分布式一致性**）。

我先说结论：你现在的 Maglev 版本**整体方向是对的**（lookup O(1)、支持权重、分布也不错），但有 **3 个高优先级问题**会直接破坏你最初提的目标（尤其是“扩容只迁移接近扩容容量的数据”），需要优先改掉。

---

## 你做得好的地方

1. **select 路径非常短**

* `hash(obj_id) -> index = hash % M -> lookup_table[index]`
  这是典型的 Maglev Lookup Table 设计，在线路径基本 O(1)（非常适合高频 select）。

2. **把“可写 target”提前过滤**（enabled && !readonly && weight>0）

* 这能保证 lookup_table 构建后 select 不需要再做运维属性判断（快、且逻辑更干净）。

3. **构建过程用 offset/skip 产生 permutation**

* 你确保了 `skip ∈ [1, M-1]`，并且你选的 `M` 都是 prime（我检查了：65537、655373、6553577 都是素数），这满足 Maglev 里“每个 backend permutation 覆盖全表”的核心条件。

---

## 高优先级问题（会直接破坏“扩容只迁移新增容量”）

### 1) `table_size` 会跳变，跨阈值扩容会导致“近乎全量迁移”

你现在的 `choose_table_size(total_weight)` 是分段的：

* `<6000 -> 65537`
* `<60000 -> 655373`
* `else -> 6553577`

这意味着只要某次扩容让 `total_weight` 穿过阈值，比如 **5999 -> 6001**，`M` 会从 **65537 跳到 655373**。

**后果非常严重：**

* select 用的是 `hash % M`
* 一旦 `M` 变了，几乎所有 key 的 index 都变（不是 1/(N+1) 那种小迁移，而是接近“全量变更”）
* 这直接违背你最初的要求：**扩容时只迁移接近扩容容量的数据**

✅ 建议（强烈）：

* **把 `table_size` 变成“集群级固定参数”**（或至少是“长期固定参数”）
* 一旦选定（比如 655373 或 6553577），后续扩容不要因为 `total_weight` 变化而改 `M`
* 如果你确实需要扩大 `M`（比如未来规模变大），那应该走**“双表/双写 + 渐进迁移”**的运维流程，而不是一次 rebuild 直接换 `M`

> 这条是你现在实现里最关键的坑。只要踩到一次阈值扩容，你的迁移量就会爆炸。

---

### 2) `DefaultHasher` 的稳定性问题（分布式一致性风险）

你在两个关键点用了 `DefaultHasher`：

* `hash_with_seed(store_id, seed)` 用于 offset/skip
* `hash_obj_id(obj_id)` 用于选表 index

`DefaultHasher` 在 Rust 里是**“默认实现细节”**，不保证跨 Rust 版本/不同实现永远一致（即便很多时候看起来是稳定的）。
如果你的布局需要在 **不同机器、不同进程、甚至不同版本客户端**上得到**完全一致**的映射，这里是风险点。

✅ 建议：

* 换成“明确指定、跨平台稳定”的 hash：`xxh3_64` / `murmur3` / `siphash(固定key)` 等
* 我更推荐 `xxh3`：快、实现成熟、确定性强

示例（你可以直接替换）：

```rust
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

#[inline]
fn hash_store_id(store_id: &str, seed: u64) -> u64 {
    xxh3_64_with_seed(store_id.as_bytes(), seed)
}

#[inline]
fn hash_obj_id(obj_id: &ObjId) -> u64 {
    // obj_hash 通常已经接近随机分布；xxh3 做一次压缩很快
    xxh3_64(obj_id.obj_hash.as_slice())
}
```

---

### 3) backend 顺序依赖：`active_target_indices` 的顺序不“规范化”，会导致不同节点构建出不同表

`rebuild_maglev()` 收集 active target 的顺序来自：

```rust
for (index, target) in self.targets.iter().enumerate() {
    ...
    self.active_target_indices.push(index);
}
```

也就是说：**lookup_table 的最终结果依赖 `targets` 的输入顺序**。

如果 fsmeta 下发的 targets 顺序在不同客户端可能不同（哪怕集合相同），那你会得到**不同的 lookup_table**，导致“同一个 ObjId 在不同节点 select 到不同 store”。

✅ 建议：

* 在构建 Maglev 前，对 active targets 做一个**确定性排序**（canonical order）
* 例如按 `store_id` 排序，或者按 `hash(store_id)` 排序

注意：排序策略会影响“加入新节点时的扰动”。
如果你按 `store_id` 排序，新节点插入中间会改变轮转顺序，可能增加一些“old-old”迁移概率。
如果你特别在意“只从旧节点迁到新节点”，你可能需要：

* 维护一个 **稳定的 backend 顺序列表**（比如把旧 layout 的顺序继承下来，新节点永远 append 到末尾），或者
* 改用 Jump Hash + append-only buckets（这一点我后面会提）

---

## 中优先级问题（影响性能/工程质量，但不一定立刻炸）

### 4) `select_primary_target()` 每次都 `clone()`：在线路径会有不必要的分配/拷贝

```rust
pub fn select_primary_target(&self, obj_id: &ObjId) -> Option<StoreTarget> {
    ...
    Some(self.targets[target_index].clone())
}
```

如果 select 是高频路径（通常是），clone 一个含 `String` 的 struct 会有额外开销。

✅ 建议：

* 改成返回引用 `Option<&StoreTarget>` 或返回 `usize` index

例如：

```rust
pub fn select_primary_index(&self, obj_id: &ObjId) -> Option<usize> {
    if self.lookup_table.is_empty() { return None; }
    let h = hash_obj_id(obj_id);
    let idx = (h % self.table_size as u64) as usize;
    let t = self.lookup_table[idx];
    if t < 0 { return None; }
    Some(t as usize)
}

pub fn select_primary(&self, obj_id: &ObjId) -> Option<&StoreTarget> {
    let i = self.select_primary_index(obj_id)?;
    self.targets.get(i)
}
```

---

### 5) `select_n_targets()` 去重是 O(k²)，probe 上限过大

你现在是：

* `Vec.contains()` 去重：`O(k²)`（k=副本数）
* probe 上限：`table_size + 1000`（表很大时这个上限没意义）

如果副本数固定很小（2~3），现在也能用；但写法上可以更稳健。

✅ 建议：

* 如果 `n` 通常 <= 4：用 `smallvec` + 手写小数组去重即可
* probe 上限建议改为 `probe > 20 * n` 这种更合理的界（通常很快就能取到不同 target）

---

### 6) lookup_table 用 i32 + cast usize，有潜在“ -1 被当成超大 usize ”风险

理论上你会填满表，不会出现 -1；但为了防御式编程，建议避免：

```rust
let target_index = self.lookup_table[index] as usize;
```

✅ 建议：

* 构建阶段使用 sentinel（`u32::MAX`），最终表用 `Vec<u32>` 存 target index
* 或者至少在 select 时检查 `>=0`

---

## 你现在的实现，是否满足“扩容只迁移新增容量附近的数据”？

**结论：不严格满足**，原因有两类：

1. **只要 table_size 变化，就会接近全量迁移**（这是硬伤，必须修）
2. 即使 table_size 不变，Maglev 也可能出现少量 **old-old 迁移**
   你测试里已经体现了 `moved_between_old_targets` 并允许它存在。

如果你的“总是只有接近扩容容量的数据要迁移”要求是**强约束**（期望：迁移的 key 只会从旧节点迁到新增节点/新增容量，不应在旧节点之间洗牌），那从算法性质上：

* **Rendezvous/HRW**：加节点时 key 只会迁到新节点（old-old 基本不发生），但在线 select 是 O(N)（除非你做额外索引结构）
* **Jump Consistent Hash + append-only bucket_to_target**：加 bucket 时 key 只会迁到新增 bucket（从而只迁到新增容量），在线路径也很短

而 **Maglev**：是“接近最小扰动”，但不是“严格只迁到新增”。

✅ 如果你接受 “几乎只迁到新增节点，old-old 极少” —— Maglev OK
✅ 如果你要 “严格只迁到新增容量” —— 建议换 Jump + append-only bucket 映射（或者你在 Maglev 之上做“只追加桶”的层）

---

## 你注释里有一个事实错误（建议改）

你写了：

> Memory efficient: ~2.5MB for 100K nodes with default table size

但按你当前 `LARGE_TABLE_SIZE = 6_553_577`：

* 6,553,577 * 4 bytes ≈ 25 MB（仅 lookup_table）

2.5MB 对应的是 655,373 级别的表。

✅ 建议把注释改成真实数字，比如：

* 65,537 slots ≈ 0.25MB
* 655,373 slots ≈ 2.5MB
* 6,553,577 slots ≈ 25MB

---

## 测试建议（能帮你提前抓到“大迁移”问题）

你现在的迁移测试都没有覆盖“跨阈值导致 M 变化”的情况。建议补一个：

* 构造 `total_weight = 5999` 的 layout（M=65537）
* 扩容到 `6001`（M=655373）
* 统计迁移比例 —— 你会看到“迁移量非常大”，从而迫使你修复“固定 M”问题

另外建议加：

* 移除节点（N -> N-1）的迁移比例
* 只增加某个节点 weight（不增节点数）的迁移比例（更贴近“增加容量”场景）

---

## 我建议你下一步最优先改的 3 件事（按收益排序）

1. **固定 table_size（M）**：从“自动选择”改为“配置/元数据指定”，避免扩容跨阈值全量迁移
2. **改 hash 为显式稳定 hash（xxh3）**：保证分布式一致性、跨版本一致性
3. **规范化 active targets 的顺序**：保证不同节点构建同一 lookup_table（至少按 store_id 排序；更高级的做法是继承旧顺序、新节点 append）

---

如果你愿意，我也可以直接给你一个“改动最小”的 patch 方案（不换 Maglev）：

* 固定 M（从 layout 元数据里带下来）
* 改 xxh3
* active targets 用稳定顺序
* select 返回引用
* lookup_table 改 Vec<u32>

同时再给一个“严格只迁到新增容量”的 Jump+append-only buckets 版本，供你对比迁移行为。
