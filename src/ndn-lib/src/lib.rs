#![allow(unused, dead_code)]

mod chunk;
mod object;
mod relation_obj;
mod action_obj;
mod cyfs_http;
mod fileobj;
mod dirobj;
mod hash;
mod simple_object_map;
mod base_content;
//mod example;

pub use object::*;
pub use chunk::*;
pub use base_content::*;
pub use relation_obj::*;
pub use cyfs_http::*;
pub use fileobj::*;
pub use dirobj::*;
pub use hash::*;
pub use simple_object_map::*;


use std::path::PathBuf;
use reqwest::StatusCode;
use thiserror::Error;
use std::pin::Pin;
use std::future::Future;
use std::ops::Range;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt, AsyncReadExt, AsyncSeekExt};
use tokio::io::{SeekFrom, BufReader, BufWriter};

#[macro_use]
extern crate log;

#[derive(Error, Debug)]
pub enum NdnError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid object id format: {0}")]
    InvalidId(String),
    #[error("invalid object link: {0}")]
    InvalidLink(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("verify chunk error: {0}")]
    VerifyError(String),
    #[error("I/O error: {0}")]
    IoError(String),
    #[error("db error: {0}")]
    DbError(String),
    #[error("chunk not completed: {0}")]
    InComplete(String),
    #[error("remote error: {0}")]
    RemoteError(String),
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("offset too large: {0}")]
    OffsetTooLarge(String),
    #[error("invalid obj type: {0}")]
    InvalidObjType(String),

    #[error("invalid data: {0}")]
    InvalidData(String),

    #[error("invalid param: {0}")]
    InvalidParam(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

impl NdnError {
    pub fn from_http_status(code: StatusCode,info:String) -> Self {
        match code {
            StatusCode::NOT_FOUND => NdnError::NotFound(info),
            StatusCode::INTERNAL_SERVER_ERROR => NdnError::Internal(info),
            _ => NdnError::RemoteError(format!("HTTP error: {} for {}", code, info)),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, NdnError::NotFound(_))
    }
}


pub type NdnResult<T> = std::result::Result<T, NdnError>;

impl From<std::io::Error> for NdnError {
    fn from(err: std::io::Error) -> Self {
        NdnError::IoError(err.to_string())
    }
}


pub const OBJ_TYPE_FILE: &str = "cyfile";
pub const OBJ_TYPE_DIR: &str = "cydir";
pub const OBJ_TYPE_PATH: &str = "cypath";
pub const OBJ_TYPE_INCLUSION_PROOF: &str = "cyinc"; // curator -> creator: content inclusion proof (recommend JWT signed by curator)
pub const OBJ_TYPE_RELATION: &str = "cyrel";
pub const OBJ_TYPE_ACTION: &str = "cyact";
pub const OBJ_TYPE_PACK: &str = "cypack"; // object set

pub const OBJ_TYPE_TRIE: &str = "cytrie"; // trie object map
pub const OBJ_TYPE_TRIE_SIMPLE: &str = "cytrie-s"; // simple trie object map

pub const OBJ_TYPE_OBJMAP: &str = "cymap-mtp"; // object map
pub const OBJ_TYPE_OBJMAP_SIMPLE: &str = "cymap"; // simple object map

pub const OBJ_TYPE_LIST: &str = "cylist-mtree"; // object list
pub const OBJ_TYPE_LIST_SIMPLE: &str = "cylist"; // simple object list

pub const OBJ_TYPE_CHUNK_LIST: &str = "cl"; // normal chunk list with variable size
pub const OBJ_TYPE_CHUNK_LIST_SIMPLE: &str = "clist"; // simple chunk list with mixhash chunk
pub const OBJ_TYPE_CHUNK_LIST_FIX_SIZE: &str = "clist-fix"; // simple chunk list with fixed size
pub const OBJ_TYPE_CHUNK_LIST_SIMPLE_FIX_SIZE: &str = "cl-sf"; // simple chunk list with fixed size

pub const OBJ_TYPE_PKG: &str = "pkg"; // package

pub const RELATION_TYPE_SAME: &str = "same";
pub const RELATION_TYPE_PART_OF: &str = "part_of";

#[derive(Debug,Clone)]
pub enum NdnAction {
    PreFile,
    FileOK(ObjId,u64),
    ChunkOK(ChunkId,u64),
    PreDir,
    DirOK(ObjId,u64),
    Skip(u64),
}

impl ToString for NdnAction {
    fn to_string(&self) -> String {
        match self {
            NdnAction::PreFile => "PreFile".to_string(),
            NdnAction::FileOK(obj_id,size) => format!("FileOK {} ({})",obj_id.to_string(),size),
            NdnAction::ChunkOK(chunk_id,size) => format!("ChunkOK {} ({})",chunk_id.to_string(),size),
            NdnAction::PreDir => "PreDir".to_string(),
            NdnAction::DirOK(obj_id,size) => format!("DirOK {} ({})",obj_id.to_string(),size),
            NdnAction::Skip(size) => format!("Skip:{}",size),
        }
    }
}

pub enum ProgressCallbackResult {
    Continue,//default, continue to the next item
    Skip,//skip the current item
    Stop,//stop the process
}

impl ProgressCallbackResult {
    pub fn is_continue(&self) -> bool {
        match self {
            ProgressCallbackResult::Continue => true,
            ProgressCallbackResult::Skip => true,
            _ => false,
        }
    }

    pub fn is_skip(&self) -> bool {
        match self {
            ProgressCallbackResult::Skip => true,
            _ => false,
        }
    }
}
// PullProgressCallback(inner_path, action), return true if continue, false if stop
pub type NdnProgressCallback = Box<dyn FnMut(String, NdnAction) -> Pin<Box<dyn Future<Output = NdnResult<ProgressCallbackResult>> + Send + 'static>> + Send>;


#[derive(Clone,Debug,PartialEq)]
pub enum StoreMode {
    //local file path and range, store in local file or named mgr?
    LocalFile(PathBuf,Range<u64>,bool),
    StoreInNamedMgr,
    NoStore,
}

impl Default for StoreMode {
    fn default() -> Self {
        Self::StoreInNamedMgr
    }
}

impl StoreMode {
    pub fn new_local() -> Self {
        return Self::LocalFile(PathBuf::new(), 0..0, false);
    }

    pub fn is_store_to_local(&self) -> bool {
        match self {
            StoreMode::LocalFile(_,_,_) => true,
            StoreMode::StoreInNamedMgr => false,
            StoreMode::NoStore => false,
        }
    }

    pub fn gen_sub_store_mode(&self,sub_item_name:&String)->Self {
        match self {
            StoreMode::LocalFile(local_path,range,need_pull_to_named_mgr) => {
                StoreMode::LocalFile(local_path.clone().join(sub_item_name), 
                0..0, *need_pull_to_named_mgr)
            }
            _ => self.clone(),
        }
    }

    pub fn need_store_to_named_mgr(&self) -> bool {
        match self {
            StoreMode::LocalFile(_,_,need_pull_to_named_mgr) => *need_pull_to_named_mgr,
            StoreMode::StoreInNamedMgr => true,
            StoreMode::NoStore => false,
        }
    }

    pub async fn open_local_writer(&self) -> NdnResult<ChunkWriter> {
        match self {
            StoreMode::LocalFile(local_file_path,range,_) => {
                if let Some(parent) = local_file_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| NdnError::IoError(format!("Failed to create directory: {}", e)))?;
                }

                let mut local_file = OpenOptions::new()
                    .write(true)
                    .open(&local_file_path)
                    .await
                    .map_err(|e| {
                        warn!("open_chunk_writer: open file failed! {}", e.to_string());
                        NdnError::IoError(e.to_string())
                    })?;
                if range.start != 0 {
                    local_file.seek(SeekFrom::Start(range.start)).await?;
                }
                return Ok(Box::pin(local_file));
            }
            StoreMode::StoreInNamedMgr => {
                return Err(NdnError::InvalidState("not a local file".to_string()));
            }
            StoreMode::NoStore => {
                return Err(NdnError::InvalidState("not a local file".to_string()));
            }
        }
    }
}


/// NDM path representation
#[derive(Debug, Clone)]
pub struct NdmPath(pub String);

impl NdmPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split path into parent and name components
    pub fn split_parent_name(&self) -> Option<(NdmPath, String)> {
        let path = self.0.trim_end_matches('/');
        if path.is_empty() || path == "/" {
            return None;
        }
        let last_slash = path.rfind('/')?;
        let parent = if last_slash == 0 {
            "/".to_string()
        } else {
            path[..last_slash].to_string()
        };
        let name = path[last_slash + 1..].to_string();
        if name.is_empty() {
            None
        } else {
            Some((NdmPath(parent), name))
        }
    }

    pub fn is_root(&self) -> bool {
        let s = self.0.trim_end_matches('/');
        s.is_empty() || s == "/"
    }
}

fn is_descendant_path(potential_child: &String, potential_parent: &String) -> bool {
    let child = potential_child.as_str().trim_end_matches('/');
    let parent = potential_parent.as_str().trim_end_matches('/');

    if child.len() <= parent.len() {
        return false;
    }

    child.starts_with(parent)
        && (child.as_bytes().get(parent.len()) == Some(&b'/') || parent == "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ndm_path_split() {
        let path = NdmPath::new("/foo/bar/baz");
        let (parent, name) = path.split_parent_name().unwrap();
        assert_eq!(parent.as_str(), "/foo/bar");
        assert_eq!(name, "baz");

        let root_child = NdmPath::new("/foo");
        let (parent, name) = root_child.split_parent_name().unwrap();
        assert_eq!(parent.as_str(), "/");
        assert_eq!(name, "foo");

        let root = NdmPath::new("/");
        assert!(root.split_parent_name().is_none());
        assert!(root.is_root());
    }

    #[test]
    fn test_is_descendant_path() {
        assert!(is_descendant_path(
            &"/a/b/c".to_string(),
            &"/a/b".to_string()
        ));
        assert!(is_descendant_path(
            &"/a/b/c".to_string(),
            &"/a".to_string()
        ));
        assert!(is_descendant_path(
            &"/a/b".to_string(),
            &"/".to_string()
        ));
        assert!(!is_descendant_path(
            &"/a/b".to_string(),
            &"/a/b".to_string()
        ));
        assert!(!is_descendant_path(
            &"/a/bc".to_string(),
            &"/a/b".to_string()
        ));
    }
}
