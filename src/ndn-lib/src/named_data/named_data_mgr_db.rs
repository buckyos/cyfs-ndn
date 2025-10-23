use crate::tools::KnownStandardObject;
use crate::{
    build_named_object_by_json, load_named_object_from_obj_str, ChunkHasher, ChunkId, ChunkListReader, ChunkLocalInfo, ChunkReadSeek, ChunkState, FileObject, NdnError, NdnResult, ObjectLinkData, PathObject, RelationObject, RELATION_TYPE_SAME
};
use buckyos_kit::get_buckyos_named_data_dir;
use buckyos_kit::{
    buckyos_get_unix_timestamp, get_buckyos_root_dir, get_by_json_path, get_relative_path,
};
use futures_util::stream;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::*;
use memmap::Mmap;
use name_lib::decode_jwt_claim_without_verify;
use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io as std_io;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::{path::PathBuf, pin::Pin};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};
use tokio_util::bytes::BytesMut;
use tokio_util::io::StreamReader;

use crate::{ChunkList, ChunkReader, ChunkWriter, ObjId};
use super::def::{ChunkItem};



impl From<NdnError> for std_io::Error {
    fn from(err: NdnError) -> Self {
        std_io::Error::new(std_io::ErrorKind::Other, err.to_string())
    }
}
pub struct NamedDataMgrDB {
    pub db_path: String,
    conn: Mutex<Connection>,
}

impl NamedDataMgrDB {
    pub fn new(db_path: String) -> NdnResult<Self> {
        debug!("NamedDataMgrDB: new db path: {}", db_path);
        let conn = Connection::open(&db_path).map_err(|e| {
            warn!("NamedDataMgrDB: open db failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paths (
                path TEXT PRIMARY KEY,
                obj_id TEXT NOT NULL,
                obj_type TEXT NOT NULL,
                path_obj_jwt TEXT,
                app_id TEXT NOT NULL,
                user_id TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedDataMgrDB: create PATHS table failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS path_metas (
                path TEXT PRIMARY KEY,
                meta_json TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: create PATH_METAS table failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        // conn.execute(
        //     "CREATE TABLE IF NOT EXISTS local_file_info (
        //         local_file_path TEXT PRIMARY KEY,
        //         qcid TEXT NOT NULL,
        //         last_modify_time INTEGER NOT NULL,
        //         content TEXT NOT NULL
        //     )",
        //     [],
        // )
        // .map_err(|e| {
        //     warn!("NamedDataMgrDB: create local file info table failed! {}", e.to_string());
        //     NdnError::DbError(e.to_string())
        // })?;

        // Create tables from NamedDataDb
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunk_items (
                chunk_id TEXT PRIMARY KEY,
                chunk_size INTEGER NOT NULL, 
                chunk_state TEXT NOT NULL,
                local_path TEXT,
                local_info TEXT,
                ref_count INTEGER NOT NULL DEFAULT 0,
                progress TEXT,
                create_time INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedDataMgrDB: create table chunk_items failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        //create ref_count update queue table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS obj_ref_update_queue (
                obj_id TEXT PRIMARY KEY, 
                ref_count INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: create table obj_ref_update_queue failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        // Create objects table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                obj_id TEXT PRIMARY KEY,
                obj_type TEXT NOT NULL,
                obj_data TEXT,
                ref_count INTEGER NOT NULL,
                create_time INTEGER NOT NULL,
                last_access_time INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedDataMgrDB: create objects table failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS object_relations (
                obj_id TEXT NOT NULL,
                source TEXT NOT NULL,
                target TEXT NOT NULL,
                relation_type INTEGER NOT NULL,
                relation_object TEXT NOT NULL,
                create_time INTEGER NOT NULL,
                PRIMARY KEY (obj_id)
            )",
            [],
        )
        .map_err(|e| {
            warn!(
                "NamedDataMgrDB: create table object_relations failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        // 添加索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_paths_path ON paths(path)",
            [],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: create index failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        Ok(Self {
            db_path,
            conn: Mutex::new(conn),
        })
    }

    //return (result_path, obj_id,path_obj_jwt,relative_path)
    pub fn find_longest_matching_path(
        &self,
        path: &str,
    ) -> NdnResult<(String, ObjId, Option<String>, Option<String>)> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare("SELECT path, obj_id,path_obj_jwt FROM paths WHERE ? LIKE (path || '%') ORDER BY length(path) DESC LIMIT 1")
            .map_err(|e| {
                warn!("NamedDataMgrDB: prepare statement failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        let record: (String, String, Option<String>) = stmt
            .query_row([path], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(|e| {
                warn!(
                    "NamedDataMgrDB: query {} obj id failed! {}",
                    path,
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        let result_path = record.0;
        let obj_id_str = record.1;
        let path_obj_jwt = record.2;
        if path_obj_jwt.is_some() {
            info!(
                "NamedDataMgrDB: find_longest_matching_path, path_obj_jwt {}",
                path_obj_jwt.as_ref().unwrap()
            );
        }
        let obj_id = ObjId::new(&obj_id_str)?;
        let relative_path = get_relative_path(&result_path, path);
        Ok((result_path, obj_id, path_obj_jwt, Some(relative_path)))
    }

    pub fn get_path_target_objid(&self, path: &str) -> NdnResult<(ObjId, Option<String>)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT obj_id,path_obj_jwt FROM paths WHERE path = ?1")
            .map_err(|e| {
                warn!(
                    "NamedDataMgrDB: prepare statement failed! {}",
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        let (obj_id_str, path_obj_jwt): (String, Option<String>) = stmt
            .query_row([path], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| {
                warn!(
                    "NamedDataMgrDB: query {} target obj failed! {}",
                    path,
                    e.to_string()
                );
                NdnError::DbError(e.to_string())
            })?;

        let obj_id = ObjId::new(&obj_id_str).map_err(|e| {
            warn!("NamedDataMgrDB: invalid obj_id format! {}", e.to_string());
            NdnError::Internal(e.to_string())
        })?;

        Ok((obj_id, path_obj_jwt))
    }

    pub fn create_path(
        &self,
        path: &str,
        obj_id: &ObjId,
        obj_data:Option<&str>,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<()> {
        if path.len() < 2 {
            return Err(NdnError::InvalidParam(
                "path length must be greater than 2".to_string(),
            ));
        }

        let mut conn = self.conn.lock().unwrap();
        let obj_id_str = obj_id.to_string();
        let tx = conn.transaction().map_err(|e| {
            warn!(
                "NamedDataMgrDB: tx.transaction error, create path failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;

        // 检查父目录冲突
        let conflict_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM paths WHERE obj_type = ?1 AND ?2 LIKE (path || '/%') AND path != ?2",
            [crate::OBJ_TYPE_DIR, &path],
            |row| row.get(0)
        ).map_err(|e| {
            warn!("NamedDataMgrDB: check parent dir conflict failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        
        if conflict_count > 0 {
            return Err(NdnError::AlreadyExists(
                format!("Cannot set path {}: parent directory already exists", path)
            ));
        }

        tx.execute(
            "INSERT INTO paths (path, obj_id, obj_type, app_id, user_id) VALUES (?1, ?2, ?3, ?4, ?5)",
            [&path, obj_id_str.as_str(), &obj_id.obj_type, app_id, user_id],
        )
        .map_err(|e| {
            warn!(
                "NamedDataMgrDB: tx.execute error, create path failed! {:?}",
                &e  
            );

            NdnError::DbError(e.to_string())
        })?;

        Self::update_obj_ref_count(&tx, obj_id, obj_data, 1)?;

        tx.commit().map_err(|e| {
            warn!(
                "NamedDataMgrDB:tx.commit error, create path failed! {}",
                e.to_string()
            );
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    //if new_obj_data is not None, will insert obj into objects table
    pub fn set_path(
        &self,
        path: &str,
        new_obj_id: &ObjId,
        new_obj_data:Option<&str>,
        path_obj_str: String,
        app_id: &str,
        user_id: &str,
    ) -> NdnResult<Option<ObjId>> {
        //如果不存在路径则创建，否则更新已经存在的路径指向的chunk
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedDataMgrDB: set path failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        // Check if the path exists
        let existing_obj_id: Result<String, _> =
            tx.query_row("SELECT obj_id FROM paths WHERE path = ?1", [&path], |row| {
                row.get(0)
            });

        let obj_id_str = new_obj_id.to_string();
        let mut old_obj_id = None;

        match existing_obj_id {
            Ok(existing_obj_id) => {
                // Path exists, update the obj_id
                let the_old_obj_id = ObjId::new(&existing_obj_id)?;

                tx.execute(
                    "UPDATE paths SET obj_id = ?1, obj_type = ?2, path_obj_jwt = ?3, app_id = ?4, user_id = ?5 WHERE path = ?6",
                    [obj_id_str.as_str(), &new_obj_id.obj_type, path_obj_str.as_str(), app_id, user_id, &path],
                ).map_err(|e| {
                    warn!("NamedDataMgrDB: set path failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                Self::update_obj_ref_count(&tx, &the_old_obj_id, None, -1)?;
                Self::update_obj_ref_count(&tx, new_obj_id, new_obj_data, 1)?;

                old_obj_id = Some(the_old_obj_id);
            }
            Err(_) => {
                // Path does not exist, create a new path

                // 检查父目录冲突
                let conflict_count: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM paths WHERE obj_type = ?1 AND ?2 LIKE (path || '/%') AND path != ?2",
                        [crate::OBJ_TYPE_DIR, &path],
                        |row| row.get(0)
                    )
                    .map_err(|e| {
                        warn!("NamedDataMgrDB: check parent dir conflict failed! {}", e.to_string());
                        NdnError::DbError(e.to_string())
                    })?;
                
                if conflict_count > 0 {
                    return Err(NdnError::AlreadyExists(
                        format!("Cannot set path {}: parent directory already exists", path)
                    ));
                }
                
                tx.execute(
                    "INSERT INTO paths (path, obj_id, obj_type, path_obj_jwt, app_id, user_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    [&path, obj_id_str.as_str(), &new_obj_id.obj_type, path_obj_str.as_str(), app_id, user_id],
                ).map_err(|e| {
                    warn!("NamedDataMgrDB: set path failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                Self::update_obj_ref_count(&tx, new_obj_id, new_obj_data, 1)?;
            }
        }

        tx.commit().map_err(|e| {
            warn!("NamedDataMgrDB: set path failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(old_obj_id)
    }

    pub fn remove_path(&self, path: &str,app_id: &str, user_id: &str) -> NdnResult<ObjId> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedDataMgrDB: remove path failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        // Get the obj_id for this path
        let obj_id: String = tx
            .query_row("SELECT obj_id FROM paths WHERE path = ?1", [&path], |row| {
                row.get(0)
            })
            .map_err(|e| {
                warn!("NamedDataMgrDB: remove path failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        // Remove the path
        tx.execute("DELETE FROM paths WHERE path = ?1", [&path])
            .map_err(|e| {
                warn!("NamedDataMgrDB: remove path failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        Self::update_obj_ref_count(&tx, &ObjId::new(&obj_id)?, None, -1)?;

        tx.commit().map_err(|e| {
            warn!("NamedDataMgrDB: remove path failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(ObjId::new(&obj_id)?)
    }


    pub fn set_path_obj_jwt(&self, path: &str, path_obj_jwt: &str) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE paths SET path_obj_jwt = ?1 WHERE path = ?2",
            [path_obj_jwt, path],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: set path obj jwt failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    // pub fn remove_dir_path(&self, path: &str) -> NdnResult<()> {
    //     let mut conn = self.conn.lock().unwrap();
    //     let tx = conn.transaction().map_err(|e| {
    //         warn!("NamedDataMgrDB: remove dir path failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     // Get all paths and their chunk_ids that start with the given directory path
    //     let mut stmt = tx
    //         .prepare("SELECT path, obj_id FROM paths WHERE path LIKE ?1")
    //         .map_err(|e| {
    //             warn!("NamedDataMgrDB: remove dir path failed! {}", e.to_string());
    //             NdnError::DbError(e.to_string())
    //         })?;

    //     let rows = stmt
    //         .query_map([format!("{}%", path)], |row| Ok((row.get(0)?, row.get(1)?)))
    //         .map_err(|e| {
    //             warn!("NamedDataMgrDB: remove dir path failed! {}", e.to_string());
    //             NdnError::DbError(e.to_string())
    //         })?;

    //     let path_objs: Vec<(String, String)> = rows.filter_map(Result::ok).collect();

    //     // Remove paths and update chunk ref counts within the transaction
    //     for (path, obj_id) in path_objs {
    //         // Remove the path
    //         tx.execute("DELETE FROM paths WHERE path = ?1", [&path])
    //             .map_err(|e| {
    //                 warn!("NamedDataMgrDB: remove dir path failed! {}", e.to_string());
    //                 NdnError::DbError(e.to_string())
    //             })?;
    //     }

    //     drop(stmt);
    //     tx.commit().map_err(|e| {
    //         warn!("ChunkMgrDB: remove dir path failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     Ok(())
    // }


    // Chunk related methods from NamedDataDb
    pub fn set_chunk_item(&self, chunk_item: &ChunkItem) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();

        match &chunk_item.chunk_state {
            ChunkState::LocalLink(ref local_info) => {
                let local_info_str = serde_json::to_string(local_info).unwrap();
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items 
                    (chunk_id, chunk_size, chunk_state, local_path,local_info, progress, 
                     create_time, update_time)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size,
                        chunk_item.chunk_state,
                        local_info.path,
                        local_info_str,
                        chunk_item.progress,
                        chunk_item.create_time,
                        chunk_item.update_time,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedDataMgrDB: insert chunk failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
                debug!("NamedDataMgrDB: insert local link chunk item success: {},path:{},info:{}", chunk_item.chunk_id.to_string(), local_info.path, local_info_str);
            }
            _ => {
                //debug!("set_chunk_item: chunk_state: {:?}", chunk_item.chunk_state);
                conn.execute(
                    "INSERT OR REPLACE INTO chunk_items 
                    (chunk_id, chunk_size, chunk_state, progress, 
                     create_time, update_time)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        chunk_item.chunk_id.to_string(),
                        chunk_item.chunk_size,
                        chunk_item.chunk_state,
                        chunk_item.progress,
                        chunk_item.create_time,
                        chunk_item.update_time,
                    ],
                )
                .map_err(|e| {
                    warn!("NamedDataMgrDB: insert chunk failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
            }
        }
        
        Ok(())
    }

    pub fn get_chunk_item(&self, chunk_id: &ChunkId) -> NdnResult<ChunkItem> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT chunk_size, chunk_state, ref_count, progress, create_time, update_time, local_path, local_info FROM chunk_items WHERE chunk_id = ?1")
            .map_err(|e| {
                NdnError::DbError(e.to_string())
            })?;

        let chunk = stmt
            .query_row(params![chunk_id.to_string()], |row| {
                let mut chunk_state: ChunkState = row.get(1)?;
                if chunk_state.is_local_link() {
                    let local_path:String = row.get(6)?;
                    let local_info_str:String = row.get(7)?;
                    let local_info = ChunkLocalInfo::create_by_info_str(local_path, local_info_str.as_str()).map_err(|e| {
                        rusqlite::Error::InvalidColumnName(e.to_string())
                    })?;
                    chunk_state = ChunkState::LocalLink(local_info);
                }
                debug!("get_chunk_item {} : chunk_state: {:?}", chunk_id.to_string(), chunk_state);

                Ok(ChunkItem {
                    chunk_id: chunk_id.clone(),
                    chunk_size: row.get(0)?,
                    chunk_state: chunk_state,
                    ref_count: row.get(2)?,
                    progress: row.get(3)?,
                    create_time: row.get(4)?,
                    update_time: row.get(5)?,
                })
            })
            .map_err(|e| {
                match e {
                    rusqlite::Error::QueryReturnedNoRows => {
                        NdnError::NotFound(format!("chunk not found: {}", chunk_id.to_string()))
                    }
                    _ => {
                        warn!("NamedDataMgrDB: get_chunk failed! {}", e.to_string());
                        NdnError::DbError(e.to_string())
                    }
                }
            })?;

        Ok(chunk)
    }

    // pub fn put_chunk_list(&self, chunk_list: Vec<ChunkItem>) -> NdnResult<()> {
    //     let mut conn = self.conn.lock().unwrap();
    //     let tx = conn.transaction().map_err(|e| {
    //         warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     for chunk in chunk_list {
    //         tx.execute(
    //             "INSERT OR REPLACE INTO chunk_items 
    //             (chunk_id, chunk_size, chunk_state, ref_count, progress, description, create_time, update_time)
    //             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    //             params![
    //                 chunk.chunk_id.to_string(),
    //                 chunk.chunk_size,
    //                 chunk.chunk_state,
    //                 chunk.ref_count,
    //                 chunk.progress,
    //                 chunk.description,
    //                 chunk.create_time,
    //                 chunk.update_time,
    //             ],
    //         )
    //         .map_err(|e| {
    //             warn!("NamedDataMgrDB: insert chunk failed! {}", e.to_string());
    //             NdnError::DbError(e.to_string())
    //         })?;
    //     }

    //     tx.commit().map_err(|e| {
    //         warn!("NamedDataMgrDB: commit failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     Ok(())
    // }

    pub fn update_chunk_progress(&self, chunk_id: &ChunkId, progress: String) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE chunk_items SET progress = ?1, chunk_state = 'incompleted', update_time = ?2 WHERE chunk_id = ?3",
            params![progress, buckyos_get_unix_timestamp(), chunk_id.to_string()],
        ).map_err(|e| {
            warn!("NamedDataMgrDB: update chunk progress failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn remove_chunk(&self, chunk_id: &ChunkId) -> NdnResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.execute(
            "DELETE FROM chunk_items WHERE chunk_id = ?1",
            params![chunk_id.to_string()],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: delete chunk failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;

        tx.commit().map_err(|e| {
            warn!("NamedDataMgrDB: commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    // Object related methods from NamedDataDb
    pub fn set_object(&self, obj_id: &ObjId, obj_type: &str, obj_str: &str) -> NdnResult<()> {
        let now_time = buckyos_get_unix_timestamp();

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| {
            warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        
        // 先检查旧数据
        let old_obj_data: Option<(String, i32)> = tx.query_row(
            "SELECT obj_data,ref_count FROM objects WHERE obj_id = ?1",
            params![obj_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
        ).ok(); // 使用 ok() 处理记录不存在的情况
        
        // 执行更新
        tx.execute(
            "INSERT OR REPLACE INTO objects (obj_id, obj_type, obj_data, ref_count, create_time, last_access_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![obj_id.to_string(), obj_type, obj_str, 0, now_time, now_time],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: insert object failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        
        // 如果从 null 变成非 null，执行ref_count更新
        if old_obj_data.is_some() {
            let (old_obj_data, old_ref_count) = old_obj_data.unwrap();
            if old_obj_data.len() < 2 && old_ref_count > 0 && obj_str.len() > 0 {
                debug!("NamedDataMgrDB: update obj ref_count from null to non-null, obj_id: {}", obj_id.to_string());
                Self::update_obj_ref_count(&tx, obj_id, Some(obj_str), old_ref_count)?;
            }
        }
        
        tx.commit().map_err(|e| {
            warn!("NamedDataMgrDB: commit failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn get_object(&self, obj_id: &ObjId) -> NdnResult<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT obj_type, obj_data FROM objects WHERE obj_id = ?1")
            .map_err(|e| {
                warn!("NamedDataMgrDB: query object failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        let obj_data = stmt
            .query_row(params![obj_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| {
                warn!("NamedDataMgrDB: query object failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;

        Ok(obj_data)
    }

    fn push_obj_id_to_add_ref_count_queue<'a>(
        tx: &Transaction<'a>, 
        obj_id: &str, 
        add_ref_count: i32
    ) -> NdnResult<()> {
        let now_time = buckyos_get_unix_timestamp();
        
        // 先执行 upsert
        tx.execute(
            "INSERT INTO obj_ref_update_queue (obj_id, ref_count, update_time)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(obj_id) DO UPDATE SET
                 ref_count = ref_count + excluded.ref_count,
                 update_time = ?3",
            params![obj_id, add_ref_count, now_time],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: push obj id to add ref count queue failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        
        // 然后删除 ref_count 为 0 的记录。 这里留在queue work时统一批量处理会更快速
        // tx.execute(
        //     "DELETE FROM obj_ref_update_queue WHERE obj_id = ?1 AND ref_count = 0",
        //     params![obj_id],
        // )
        // .map_err(|e| {
        //     warn!("NamedDataMgrDB: delete zero ref count record failed! {}", e.to_string());
        //     NdnError::DbError(e.to_string())
        // })?;
        
        Ok(())
    }

    pub fn update_obj_ref_count<'a>(tx: &Transaction<'a>, obj_id: &ObjId, obj_data: Option<&str>, add_ref_count: i32) -> NdnResult<()> {
        if obj_data.is_some() {
            if obj_id.is_chunk() {
                tx.execute(
                    "UPDATE chunk_items SET ref_count = ref_count + (?1) WHERE chunk_id = ?2",
                    params![add_ref_count, obj_id.to_string()],
                )
                .map_err(|e| {
                    warn!("NamedDataMgrDB: update chunk ref count failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;
            } else {
                tx.execute(
                    "UPDATE objects SET ref_count = ref_count + (?1), last_access_time = ?2 WHERE obj_id = ?3",
                    params![add_ref_count, buckyos_get_unix_timestamp(), obj_id.to_string()],
                )
                .map_err(|e| {
                    warn!("NamedDataMgrDB: update object ref count failed! {}", e.to_string());
                    NdnError::DbError(e.to_string())
                })?;

                let obj_data_str = obj_data.unwrap();
                let known_standard_object = KnownStandardObject::from_obj_data(obj_id, obj_data_str);
                if known_standard_object.is_ok() {
                    let known_standard_object = known_standard_object.unwrap();
                    let child_objs = known_standard_object.get_child_objs()?;
                    for (child_obj_id,obj_str) in child_objs {
                        match obj_str {
                            Some(obj_str) => {
                                Self::update_obj_ref_count(tx, &child_obj_id, Some(obj_str.as_str()), add_ref_count)?;
                            }
                            None => {
                                Self::update_obj_ref_count(tx, &child_obj_id, None, add_ref_count)?;
                            }
                        }

                    }
                }
            }
        } else  {
            Self::push_obj_id_to_add_ref_count_queue(tx, obj_id.to_string().as_str(), add_ref_count)?;
        }
        
        Ok(())
    }


    
    // pub async fn remove_object(&self, obj_id: &ObjId) -> NdnResult<()> {
    //     let conn = self.conn.lock().unwrap();
    //     conn.execute(
    //         "DELETE FROM objects WHERE obj_id = ?1",
    //         params![obj_id.to_string()],
    //     )
    //     .map_err(|e| {
    //         warn!("NamedDataMgrDB: remove object failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     Ok(())
    // }

    pub fn set_relation_object(&self, relation_object: &RelationObject,obj_ids:Option<(ObjId,String)>) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        let mut create_time;
        if relation_object.iat.is_none() {
            create_time = buckyos_get_unix_timestamp();
        } else {
            create_time = relation_object.iat.unwrap();
        }
        let relation_obj_ids: (ObjId,String);
        if obj_ids.is_none() {;
            relation_obj_ids = relation_object.gen_obj_id();
        } else {
            relation_obj_ids = obj_ids.unwrap();
        }
        conn.execute(
            "INSERT OR REPLACE INTO object_relations (obj_id, source, target, relation_type, relation_object, create_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![relation_obj_ids.0.to_string(),relation_object.source.to_string(),relation_object.target.to_string(), relation_object.relation, relation_obj_ids.1, create_time],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: insert relation object failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    pub fn get_relation_by_source(&self, reation_type:&str,source: &ObjId) -> NdnResult<Vec<RelationObject>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT relation_object FROM object_relations WHERE source = ?1 AND relation_type = ?2")
            .map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_source failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
        let mut rows = stmt.query(params![source.to_string(), reation_type]).map_err(|e| {
            warn!("NamedDataMgrDB: get_relation_by_source failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        let mut relation_objects = Vec::new();
        while let Some(row) = rows.next().map_err(|e| {
            warn!("NamedDataMgrDB: get_relation_by_source failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })? {
            let relation_object_str: String = row.get(0).map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_source failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            let relation_object_json = load_named_object_from_obj_str(&relation_object_str)?;
            let relation_object = serde_json::from_value(relation_object_json).map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_source failed! {},parser obj_str failed.", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            relation_objects.push(relation_object);
        }
        if relation_objects.is_empty() {
            return Err(NdnError::NotFound(format!("No relation object found for source:{} and relation_type:{}", source.to_string(), reation_type)));
        }
        Ok(relation_objects)
    }

    pub fn get_relation_by_target(&self, reation_type:&str,target: &ObjId) -> NdnResult<Vec<RelationObject>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT relation_object FROM object_relations WHERE target = ?1 AND relation_type = ?2")
            .map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_target failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
        let mut rows = stmt.query(params![target.to_string(), reation_type]).map_err(|e| {
            warn!("NamedDataMgrDB: get_relation_by_target failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        let mut relation_objects = Vec::new();
        while let Some(row) = rows.next().map_err(|e| {
            warn!("NamedDataMgrDB: get_relation_by_target failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })? {
            let relation_object_str: String = row.get(0).map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_target failed! {}", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            let relation_object_json = load_named_object_from_obj_str(&relation_object_str)?;
            let relation_object = serde_json::from_value(relation_object_json).map_err(|e| {
                warn!("NamedDataMgrDB: get_relation_by_target failed! {},parser obj_str failed.", e.to_string());
                NdnError::DbError(e.to_string())
            })?;
            relation_objects.push(relation_object);
        }
        if relation_objects.is_empty() {
            return Err(NdnError::NotFound(format!("No relation object found for target:{} and relation_type:{}", target.to_string(), reation_type)));
        }
        Ok(relation_objects)
    }

    pub fn remove_relation_object_by_obj_id(&self, relation_obj_id: &ObjId) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM object_relations WHERE obj_id = ?1",
            params![relation_obj_id.to_string()],
        )
        .map_err(|e| {
            warn!("NamedDataMgrDB: remove relation object failed! {}", e.to_string());
            NdnError::DbError(e.to_string())
        })?;
        Ok(())
    }

    // Object link related methods from NamedDataDb
    pub fn set_object_link(&self, obj_id: &ObjId, obj_link: &ObjectLinkData) -> NdnResult<()> {
        let relation_object = RelationObject::create_by_link_data(obj_id.clone(), obj_link.clone());
        self.set_relation_object(&relation_object, None)
    }

    pub fn get_same_as_object_by_target(&self, target: &ObjId) -> NdnResult<Vec<ObjId>> {
        let relation_objects = self.get_relation_by_target(RELATION_TYPE_SAME, target)?;
        let mut same_as_object_ids = Vec::new();
        for relation_object in relation_objects {
            same_as_object_ids.push(relation_object.source);
        }
        if same_as_object_ids.is_empty() {
            return Err(NdnError::NotFound(format!("No same as object found for target:{}", target.to_string())));
        }
        Ok(same_as_object_ids)
    }


    //pub fn query_local_file_info_by_qcid(&self, qcid: &str) -> NdnResult<Option<LocalFileInfo>> {
    // let mut conn = self.conn.lock().unwrap();

    // let tx = conn.transaction().map_err(|e| {
    //     warn!("NamedDataMgrDB: transaction failed! {}", e.to_string());
    //     NdnError::DbError(e.to_string())
    // })?;
    

    // let same_as_link_data = LinkData::SameAs(ObjId::new(qcid)?);
    // let same_as_link_data_str = same_as_link_data.to_string();
    // //从object_links table中查找qcid对应的信息
    // let mut rows = tx.execute(
    //     "SELECT obj_id FROM object_links WHERE obj_link = ?1 ORDER BY create_time DESC LIMIT 1",
    //     params![same_as_link_data_str],
    // )
    // .map_err(|e| {
    //     warn!("NamedDataMgrDB: query object link failed! {}", e.to_string());
    //     NdnError::DbError(e.to_string())
    // })?;

    // let content_obj_id: String = rows.get(0).map_err(|e| {
    //     warn!("NamedDataMgrDB: query object link failed! {}", e.to_string());
    //     NdnError::DbError(e.to_string())
    // })?;

    // let obj_link = stmt.query_row(params![qcid], |row| row.get::<_, String>(0)).map_err(|e| {
    //     warn!("NamedDataMgrDB: query object link failed! {}", e.to_string());
    //     NdnError::DbError(e.to_string())
    // })?;

    // let link_data = LinkData::from_string(&obj_link)?;
    // match link_data {
    //     LinkData::LocalFile(file_path, range, last_modify_time, qcid) => {
    //         let local_file_info = LocalFileInfo {
    //             qcid: qcid.to_string(),
    //             last_modify_time: last_modify_time,
    //             content: file_path,
    //         };
    //         return Ok(Some(local_file_info));
    //     }
    //     _ => {
    //         return Ok(None);
    //     }
    // }
    //    unimplemented!();
    //}

    // pub fn set_local_file_info(&self, local_file_path: &str, local_file_info: &LocalFileInfo) -> NdnResult<()> {
    //     let conn = self.conn.lock().unwrap();
    //     conn.execute(
    //         "INSERT OR REPLACE INTO local_file_info (local_file_path, qcid, last_modify_time, content)
    //          VALUES (?1, ?2, ?3, ?4)",
    //         params![local_file_path, local_file_info.qcid.as_str(), local_file_info.last_modify_time, local_file_info.content.as_str()],
    //     )
    //     .map_err(|e| {
    //         warn!("NamedDataMgrDB: add local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;
    //     Ok(())
    // }

    // pub fn remove_local_file_info(&self, local_file_path: &str) -> NdnResult<()> {
    //     let conn = self.conn.lock().unwrap();
    //     conn.execute(
    //         "DELETE FROM local_file_info WHERE local_file_path = ?1",
    //         params![local_file_path],
    //     )
    //     .map_err(|e| {
    //         warn!("NamedDataMgrDB: remove local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;
    //     Ok(())
    // }

    // pub fn query_local_file_info(&self, local_file_path: &str) -> NdnResult<Option<LocalFileInfo>> {
    //     let conn = self.conn.lock().unwrap();
    //     let mut stmt = conn
    //         .prepare("SELECT qcid, last_modify_time, content FROM local_file_info WHERE local_file_path = ?1")
    //         .map_err(|e| {
    //             warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //             NdnError::DbError(e.to_string())
    //         })?;

    //     let mut rows = stmt.query(params![local_file_path]).map_err(|e| {
    //         warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     let row = rows.next().map_err(|e| {
    //         warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;

    //     if row.is_none() {
    //         return Ok(None);
    //     }

    //     let row = row.unwrap();
    //     let qcid: String = row.get(0).map_err(|e| {
    //         warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;
    //     let last_modify_time: u64 = row.get(1).map_err(|e| {
    //         warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;
    //     let content: String = row.get(2).map_err(|e| {
    //         warn!("NamedDataMgrDB: query local file info failed! {}", e.to_string());
    //         NdnError::DbError(e.to_string())
    //     })?;
    //     let local_file_info = LocalFileInfo {
    //         qcid,
    //         last_modify_time,
    //         content,
    //     };
        
    //     Ok(Some(local_file_info))
    // }
}
