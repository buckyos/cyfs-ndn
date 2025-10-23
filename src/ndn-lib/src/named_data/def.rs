use crate::{
    ChunkHasher, ChunkId, ChunkReader, ChunkWriter, NdnError, NdnResult, ObjId,
};
use async_trait::async_trait;
use buckyos_kit::buckyos_get_unix_timestamp;
use name_lib::EncodedDocument;
use rusqlite::types::{ToSql, FromSql, ValueRef};
use std::{ops::Range, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};
use tokio::io::{AsyncRead, AsyncSeek};
use serde::{Serialize, Deserialize};
use crate::ObjectLinkData;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkLocalInfo {
    #[serde(skip_serializing,default)]
    pub path: String,//文件的本地路径
    pub qcid: String,//创建链接时，文件的qcid
    pub last_modify_time: u64,//创建链接时，文件的最后修改时间
    #[serde(skip_serializing_if = "Option::is_none",default)]
    pub range: Option<Range<u64>>,//文件的部分，为None表示整个文件
}

impl Default for ChunkLocalInfo {
    fn default() -> Self {
        Self {
            path: "".to_string(),
            qcid: "".to_string(),
            last_modify_time: 0,
            range: None,
        }
    }
}

impl ChunkLocalInfo {
    pub fn create_by_info_str(path:String,info_str:&str) -> NdnResult<Self> {
        let mut local_info: ChunkLocalInfo = serde_json::from_str(info_str).map_err(|e| {
            NdnError::InvalidParam(e.to_string())
        })?;
        local_info.path = path;
        Ok(local_info)
    }
}

pub enum ObjectState {
    NotExist,
    Object(String),           //json_str
    Link(ObjectLinkData),//TODO:可以不要?
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkState {
    New,         //刚创建
    Completed,   //完成
    Incompleted, //未完成
    Disabled,    //禁用
    NotExist,    //不存在
    LocalLink(ChunkLocalInfo),//数据在本地文件里
}

impl ChunkState {
    pub fn from_str(s: &str) -> Self {
        match s {
            "new" => ChunkState::New,
            "completed" => ChunkState::Completed,
            "incompleted" => ChunkState::Incompleted,
            "disabled" => ChunkState::Disabled,
            "not_exist" => ChunkState::NotExist,
            "local_link" => ChunkState::LocalLink(ChunkLocalInfo::default()),
            _ => ChunkState::NotExist,
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            ChunkState::New => "new".to_string(),
            ChunkState::Completed => "completed".to_string(),
            ChunkState::Incompleted => "incompleted".to_string(),
            ChunkState::Disabled => "disabled".to_string(),
            ChunkState::NotExist => "not_exist".to_string(),
            ChunkState::LocalLink(_) => "local_link".to_string(),
        }
    }

    pub fn can_open_reader(&self) -> bool {
        match self {
            ChunkState::Completed => true,
            ChunkState::LocalLink(_) => true,
            _ => false,
        }
    }

    pub fn can_open_writer(&self) -> bool {
        match self {
            ChunkState::Incompleted => true,
            ChunkState::New => true,
            ChunkState::NotExist => true,
            _ => false,
        }
    }

    pub fn can_open_new_writer(&self) -> bool {
        match self {
            ChunkState::New => true,
            ChunkState::NotExist => true,
            _ => false,
        }
    }

    pub fn is_local_link(&self) -> bool {
        match self {
            ChunkState::LocalLink(_) => true,
            _ => false,
        }
    }
}

impl ToSql for ChunkState {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = match self {
            ChunkState::New => "new",
            ChunkState::Completed => "completed",
            ChunkState::Incompleted => "incompleted",
            ChunkState::Disabled => "disabled",
            ChunkState::NotExist => "not_exist",
            ChunkState::LocalLink(_) => "local_link",
        };
        Ok(s.into())
    }
}

impl FromSql for ChunkState {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str().unwrap();
        Ok(ChunkState::from_str(s))
    }
}




pub struct ChunkItem {
    pub chunk_id: ChunkId,
    pub chunk_size: u64,
    pub chunk_state: ChunkState,
    pub ref_count: i32,
    pub progress: String,
    pub create_time: u64,
    pub update_time: u64,
}

impl ChunkItem {
    pub fn new(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let now_time = buckyos_get_unix_timestamp();
        Self {
            chunk_id: chunk_id.clone(),
            chunk_size,
            chunk_state: ChunkState::New,
            ref_count: 0,
            progress: "".to_string(),
            create_time: now_time,
            update_time: now_time,
        }
    }

    pub fn new_completed(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkState::Completed;
        result
    }

    pub fn new_local_file(chunk_id: &ChunkId, chunk_size: u64, 
        path: &PathBuf, qcid: &ChunkId, last_modify_time: u64, range: Option<Range<u64>>) -> Self {
        let local_info = ChunkLocalInfo {
            path: path.to_string_lossy().to_string(),
            qcid: qcid.to_string(),
            last_modify_time,
            range,
        };
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkState::LocalLink(local_info.clone());
        result
    }
}

// Create a new trait that combines AsyncRead and AsyncSeek
pub trait ChunkReadSeek: AsyncRead + AsyncSeek {}

// Blanket implementation for any type that implements both traits
impl<T: AsyncRead + AsyncSeek> ChunkReadSeek for T {}




