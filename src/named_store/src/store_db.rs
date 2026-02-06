use log::{debug, warn};
use ndn_lib::{ChunkId, NdnError, NdnResult, ObjId};
use rusqlite::types::{FromSql, ToSql, ValueRef};
use rusqlite::{params, Connection};
use std::ops::Range;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChunkLocalInfo {
    #[serde(skip_serializing, default)]
    pub path: String,
    pub qcid: String,
    pub last_modify_time: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub range: Option<Range<u64>>,
}

impl Default for ChunkLocalInfo {
    fn default() -> Self {
        Self {
            path: String::new(),
            qcid: String::new(),
            last_modify_time: 0,
            range: None,
        }
    }
}

impl ChunkLocalInfo {
    pub fn create_by_info_str(path: String, info_str: &str) -> NdnResult<Self> {
        let mut local_info: ChunkLocalInfo =
            serde_json::from_str(info_str).map_err(|e| NdnError::InvalidParam(e.to_string()))?;
        local_info.path = path;
        Ok(local_info)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStoreState {
    New,
    Completed,
    Incompleted,
    Disabled,
    NotExist,
    LocalLink(ChunkLocalInfo),
}

impl ChunkStoreState {
    pub fn from_str(s: &str) -> Self {
        match s {
            "new" => ChunkStoreState::New,
            "completed" => ChunkStoreState::Completed,
            "incompleted" => ChunkStoreState::Incompleted,
            "disabled" => ChunkStoreState::Disabled,
            "not_exist" => ChunkStoreState::NotExist,
            "local_link" => ChunkStoreState::LocalLink(ChunkLocalInfo::default()),
            _ => ChunkStoreState::NotExist,
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            ChunkStoreState::New => "new".to_string(),
            ChunkStoreState::Completed => "completed".to_string(),
            ChunkStoreState::Incompleted => "incompleted".to_string(),
            ChunkStoreState::Disabled => "disabled".to_string(),
            ChunkStoreState::NotExist => "not_exist".to_string(),
            ChunkStoreState::LocalLink(_) => "local_link".to_string(),
        }
    }

    pub fn can_open_reader(&self) -> bool {
        matches!(
            self,
            ChunkStoreState::Completed | ChunkStoreState::LocalLink(_)
        )
    }

    pub fn can_open_writer(&self) -> bool {
        matches!(
            self,
            ChunkStoreState::Incompleted | ChunkStoreState::New | ChunkStoreState::NotExist
        )
    }

    pub fn can_open_new_writer(&self) -> bool {
        matches!(self, ChunkStoreState::New | ChunkStoreState::NotExist)
    }

    pub fn is_local_link(&self) -> bool {
        matches!(self, ChunkStoreState::LocalLink(_))
    }
}

impl ToSql for ChunkStoreState {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = match self {
            ChunkStoreState::New => "new",
            ChunkStoreState::Completed => "completed",
            ChunkStoreState::Incompleted => "incompleted",
            ChunkStoreState::Disabled => "disabled",
            ChunkStoreState::NotExist => "not_exist",
            ChunkStoreState::LocalLink(_) => "local_link",
        };
        Ok(s.into())
    }
}

impl FromSql for ChunkStoreState {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str().unwrap_or("not_exist");
        Ok(ChunkStoreState::from_str(s))
    }
}

#[derive(Debug, Clone)]
pub struct ChunkItem {
    pub chunk_id: ChunkId,
    pub chunk_size: u64,
    pub chunk_state: ChunkStoreState,
    pub progress: String,
    pub create_time: u64,
    pub update_time: u64,
}

impl ChunkItem {
    pub fn new(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let now_time = unix_timestamp();
        Self {
            chunk_id: chunk_id.clone(),
            chunk_size,
            chunk_state: ChunkStoreState::New,
            progress: String::new(),
            create_time: now_time,
            update_time: now_time,
        }
    }

    pub fn new_completed(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkStoreState::Completed;
        result
    }

    pub fn new_incompleted(chunk_id: &ChunkId, chunk_size: u64) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkStoreState::Incompleted;
        result
    }

    pub fn new_local_file(
        chunk_id: &ChunkId,
        chunk_size: u64,
        chunk_local_info: &ChunkLocalInfo,
    ) -> Self {
        let mut result = Self::new(chunk_id, chunk_size);
        result.chunk_state = ChunkStoreState::LocalLink(chunk_local_info.clone());
        result
    }
}

pub struct NamedLocalStoreDB {
    pub db_path: String,
    conn: Mutex<Connection>,
}

impl NamedLocalStoreDB {
    pub fn new(db_path: String) -> NdnResult<Self> {
        debug!("NamedLocalStoreDB: new db path: {}", db_path);
        let conn = Connection::open(&db_path).map_err(|e| {
            warn!("NamedLocalStoreDB: open db failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunk_items (
                chunk_id TEXT PRIMARY KEY,
                chunk_size INTEGER NOT NULL,
                chunk_state TEXT NOT NULL,
                local_path TEXT,
                local_info TEXT,
                progress TEXT,
                create_time INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedLocalStoreDB: create table chunk_items failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                obj_id TEXT PRIMARY KEY,
                obj_type TEXT NOT NULL,
                obj_data TEXT,
                create_time INTEGER NOT NULL,
                last_access_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedLocalStoreDB: create objects table failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        Ok(Self {
            db_path,
            conn: Mutex::new(conn),
        })
    }

    pub fn set_chunk_item(&self, chunk_item: &ChunkItem) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();

        match &chunk_item.chunk_state {
            ChunkStoreState::LocalLink(local_info) => {
                let local_info_str = serde_json::to_string(local_info).unwrap();
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items
                    (chunk_id, chunk_size, chunk_state, local_path, local_info, progress,
                     create_time, update_time)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size as i64,
                        chunk_item.chunk_state,
                        local_info.path,
                        local_info_str,
                        chunk_item.progress,
                        chunk_item.create_time as i64,
                        chunk_item.update_time as i64,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedLocalStoreDB: insert chunk failed! {}", e);
                    NdnError::DbError(e.to_string())
                })?;
            }
            _ => {
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items
                    (chunk_id, chunk_size, chunk_state, progress,
                     create_time, update_time)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size as i64,
                        chunk_item.chunk_state,
                        chunk_item.progress,
                        chunk_item.create_time as i64,
                        chunk_item.update_time as i64,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedLocalStoreDB: insert chunk failed! {}", e);
                    NdnError::DbError(e.to_string())
                })?;
            }
        }

        Ok(())
    }

    pub fn get_chunk_item(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT chunk_size, chunk_state, progress, create_time, update_time, local_path, local_info
                 FROM chunk_items WHERE chunk_id = ?1",
            )
            .map_err(|e| NdnError::DbError(e.to_string()))?;

        let chunk = stmt
            .query_row(params![chunk_id.to_string()], |row| {
                let mut chunk_state: ChunkStoreState = row.get(1)?;
                if chunk_state.is_local_link() {
                    let local_path: String = row.get(5)?;
                    let local_info_str: String = row.get(6)?;
                    let local_info =
                        ChunkLocalInfo::create_by_info_str(local_path, local_info_str.as_str())
                            .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?;
                    chunk_state = ChunkStoreState::LocalLink(local_info);
                }

                Ok(ChunkItem {
                    chunk_id: chunk_id.clone(),
                    chunk_size: row.get::<_, i64>(0)? as u64,
                    chunk_state,
                    progress: row.get(2)?,
                    create_time: row.get::<_, i64>(3)? as u64,
                    update_time: row.get::<_, i64>(4)? as u64,
                })
            })
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    NdnError::NotFound(format!("chunk not found: {}", chunk_id.to_string()))
                }
                _ => {
                    warn!("NamedLocalStoreDB: get chunk failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                }
            })?;

        Ok(chunk)
    }

    pub fn update_chunk_progress(&self, chunk_id: &ChunkId, progress: String) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE chunk_items SET progress = ?1, update_time = ?2 WHERE chunk_id = ?3",
            params![progress, unix_timestamp() as i64, chunk_id.to_string()],
        )
        .map_err(|e| {
            warn!(
                "NamedLocalStoreDB: update chunk progress failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedLocalStoreDB: transaction failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "DELETE FROM chunk_items WHERE chunk_id = ?1",
            params![chunk_id.to_string()],
        )
        .map_err(|e| {
            warn!("NamedLocalStoreDB: delete chunk failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("NamedLocalStoreDB: commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn set_object(&self, obj_id: &ObjId, obj_type: &str, obj_str: &str) -> NdnResult<()> {
        let now_time = unix_timestamp();

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedLocalStoreDB: transaction failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "INSERT OR REPLACE INTO objects (obj_id, obj_type, obj_data, create_time, last_access_time)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                obj_id.to_string(),
                obj_type,
                obj_str,
                now_time as i64,
                now_time as i64
            ],
        )
        .map_err(|e| {
            warn!("NamedLocalStoreDB: insert object failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("NamedLocalStoreDB: commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn get_object(&self, obj_id: &ObjId) -> NdnResult<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT obj_type, obj_data FROM objects WHERE obj_id = ?1")
            .map_err(|e| {
                warn!("NamedLocalStoreDB: query object failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        let obj_data = stmt
            .query_row(params![obj_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| {
                warn!("NamedLocalStoreDB: query object failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        Ok(obj_data)
    }

    pub fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM objects WHERE obj_id = ?1",
            params![obj_id.to_string()],
        )
        .map_err(|e| {
            warn!("NamedLocalStoreDB: remove object failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(())
    }
}
