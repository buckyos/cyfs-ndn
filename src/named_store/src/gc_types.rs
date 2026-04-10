use ndn_lib::ObjId;
use std::fmt;
use std::time::Duration;

/// Pin scope: how a pin protects the target and its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PinScope {
    /// Lock self + expand children recursively.
    Recursive,
    /// Lock self + block any cascade through self (hard barrier).
    Skeleton,
    /// Lock self only; no effect on children expansion.
    Lease,
}

impl PinScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            PinScope::Recursive => "recursive",
            PinScope::Skeleton => "skeleton",
            PinScope::Lease => "lease",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "recursive" => Some(PinScope::Recursive),
            "skeleton" => Some(PinScope::Skeleton),
            "lease" => Some(PinScope::Lease),
            _ => None,
        }
    }
}

impl fmt::Display for PinScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Minimal per-anchor completeness for P0 (root-level only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CascadeStateP0 {
    /// Anchor registered but root object is still shadow / not arrived.
    Pending,
    /// Root object is present; subtree completeness is NOT guaranteed in P0.
    Materializing,
}

impl CascadeStateP0 {
    pub fn as_str(&self) -> &'static str {
        match self {
            CascadeStateP0::Pending => "Pending",
            CascadeStateP0::Materializing => "Materializing",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Pending" => Some(CascadeStateP0::Pending),
            "Materializing" => Some(CascadeStateP0::Materializing),
            _ => None,
        }
    }
}

impl fmt::Display for CascadeStateP0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Edge operation in outbox / apply_edge messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeOp {
    Add,
    Remove,
}

impl EdgeOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            EdgeOp::Add => "add",
            EdgeOp::Remove => "remove",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "add" => Some(EdgeOp::Add),
            "remove" => Some(EdgeOp::Remove),
            _ => None,
        }
    }
}

/// A cross-bucket edge message (used by outbox sender and apply_edge).
#[derive(Debug, Clone)]
pub struct EdgeMsg {
    pub op: EdgeOp,
    /// The child being referenced.
    pub referee: ObjId,
    /// The parent doing the referencing.
    pub referrer: ObjId,
    /// Epoch of the declaring bucket.
    pub target_epoch: u64,
}

/// Outbox entry read from DB.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub seq: i64,
    pub msg: EdgeMsg,
    pub attempts: u32,
    pub next_try_at: u64,
    pub created_at: u64,
}

/// Object state in the GC model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemState {
    Present,
    Shadow,
    Incompleted,
}

impl ItemState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ItemState::Present => "present",
            ItemState::Shadow => "shadow",
            ItemState::Incompleted => "incompleted",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "present" => ItemState::Present,
            "shadow" => ItemState::Shadow,
            "incompleted" => ItemState::Incompleted,
            _ => ItemState::Present,
        }
    }

    pub fn is_present(&self) -> bool {
        matches!(self, ItemState::Present)
    }
}

impl fmt::Display for ItemState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Pin request parameters.
#[derive(Debug, Clone)]
pub struct PinRequest {
    pub obj_id: ObjId,
    pub owner: String,
    pub scope: PinScope,
    pub ttl: Option<Duration>,
}

/// Debug info for expand state inspection.
#[derive(Debug, Clone)]
pub struct ExpandDebug {
    pub obj_id: ObjId,
    pub state: ItemState,
    pub eviction_class: u32,
    pub children_expanded: bool,
    pub fs_anchor_count: u32,
    pub incoming_refs_count: u32,
    pub has_recursive_pin: bool,
    pub has_skeleton_pin: bool,
    pub has_lease_pin: bool,
    pub owned_bytes: u64,
    pub logical_size: u64,
    pub last_access_time: u64,
}

/// GC report after a gc_round.
#[derive(Debug, Clone, Default)]
pub struct GcReport {
    pub freed_bytes: u64,
    pub evicted_objects: u64,
    pub evicted_chunks: u64,
    pub skipped_protected: u64,
}
