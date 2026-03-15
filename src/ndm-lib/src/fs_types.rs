use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenWriteFlag {
    /// Append to existing file (file must exist)
    /// Returns error if file not found
    Append,

    /// Continue previous write session (file must exist, state must be Cooling/Working)
    /// For resuming interrupted writes
    ContinueWrite,

    /// Create new file exclusively (fails if file exists)
    CreateExclusive,

    /// Create if not exist, truncate if exists
    CreateOrTruncate,

    /// Create if not exist, append if exists (useful for distributed logging)
    CreateOrAppend,
}
