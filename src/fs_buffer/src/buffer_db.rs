use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use ndn_lib::{NdnError, NdnResult};

use crate::local_filebuffer::{FileBufferRecord, FileBufferRecordMeta};

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub struct LocalFileBufferDB {
    conn: Mutex<Connection>,
}

impl LocalFileBufferDB {
    pub fn new(db_path: PathBuf) -> NdnResult<Self> {
        let conn = Connection::open(db_path)
            .map_err(|e| NdnError::DbError(format!("open db failed: {}", e)))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS file_buffers (
                handle_id TEXT PRIMARY KEY,
                meta_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| NdnError::DbError(format!("create table failed: {}", e)))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn add_buffer(&self, record: &FileBufferRecord) -> NdnResult<()> {
        let meta = record.to_meta();
        let meta_json = serde_json::to_string(&meta)
            .map_err(|e| NdnError::InvalidParam(format!("serialize meta failed: {}", e)))?;

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO file_buffers (handle_id, meta_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                record.handle_id,
                meta_json,
                unix_timestamp() as i64,
                unix_timestamp() as i64
            ],
        )
        .map_err(|e| NdnError::DbError(format!("insert buffer failed: {}", e)))?;
        Ok(())
    }

    pub fn get_buffer(&self, handle_id: &str) -> NdnResult<FileBufferRecord> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT meta_json FROM file_buffers WHERE handle_id = ?1")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;

        let row = stmt.query_row(params![handle_id], |row| row.get::<_, String>(0));

        match row {
            Ok(meta_json) => {
                let meta: FileBufferRecordMeta = serde_json::from_str(&meta_json)
                    .map_err(|e| NdnError::DecodeError(format!("decode meta failed: {}", e)))?;
                Ok(FileBufferRecord::from_meta(meta))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(NdnError::NotFound(format!(
                "buffer not found: {}",
                handle_id
            ))),
            Err(e) => Err(NdnError::DbError(format!("query failed: {}", e))),
        }
    }

    pub fn list_handles(&self) -> NdnResult<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT handle_id FROM file_buffers")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| NdnError::DbError(format!("query failed: {}", e)))?;

        let mut handles = Vec::new();
        for handle in rows {
            handles.push(handle.map_err(|e| NdnError::DbError(format!("read row failed: {}", e)))?);
        }
        Ok(handles)
    }

    pub fn load_all(&self) -> NdnResult<Vec<FileBufferRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT meta_json FROM file_buffers")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| NdnError::DbError(format!("query failed: {}", e)))?;

        let mut records = Vec::new();
        for row in rows {
            let meta_json =
                row.map_err(|e| NdnError::DbError(format!("read row failed: {}", e)))?;
            let meta: FileBufferRecordMeta = serde_json::from_str(&meta_json)
                .map_err(|e| NdnError::DecodeError(format!("decode meta failed: {}", e)))?;
            records.push(FileBufferRecord::from_meta(meta));
        }
        Ok(records)
    }

    pub fn set_meta(&self, handle_id: &str, meta: &FileBufferRecordMeta) -> NdnResult<()> {
        let meta_json = serde_json::to_string(meta)
            .map_err(|e| NdnError::InvalidParam(format!("serialize meta failed: {}", e)))?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE file_buffers SET meta_json = ?1, updated_at = ?2 WHERE handle_id = ?3",
            params![meta_json, unix_timestamp() as i64, handle_id],
        )
        .map_err(|e| NdnError::DbError(format!("update meta failed: {}", e)))?;
        Ok(())
    }

    pub fn remove(&self, handle_id: &str) -> NdnResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM file_buffers WHERE handle_id = ?1",
            params![handle_id],
        )
        .map_err(|e| NdnError::DbError(format!("delete failed: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_filebuffer::{
        FileBufferBaseReader, FileBufferDiffState, FileBufferRecordMeta,
    };
    use std::sync::{Arc, RwLock};
    use tempfile::tempdir;

    fn make_test_record(handle_id: &str, inode_id: u64) -> FileBufferRecord {
        FileBufferRecord {
            handle_id: handle_id.to_string(),
            file_inode_id: inode_id,
            base_reader: FileBufferBaseReader::None,
            read_only: false,
            diff_file_path: PathBuf::from(format!("/tmp/{}.buf", handle_id)),
            diff_state: Arc::new(RwLock::new(FileBufferDiffState::default())),
        }
    }

    #[test]
    fn test_add_get_buffer() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let record = make_test_record("fb-1", 123);
        db.add_buffer(&record).unwrap();

        let loaded = db.get_buffer("fb-1").unwrap();
        assert_eq!(loaded.handle_id, "fb-1");
        assert_eq!(loaded.file_inode_id, 123);
        assert_eq!(loaded.diff_file_path, PathBuf::from("/tmp/fb-1.buf"));
    }

    #[test]
    fn test_set_meta_and_reload() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let record = make_test_record("fb-2", 234);
        db.add_buffer(&record).unwrap();

        let new_meta = FileBufferRecordMeta {
            handle_id: "fb-2".to_string(),
            file_inode_id: 999,
            base_reader: FileBufferBaseReader::None,
            read_only: false,
            diff_file_path: PathBuf::from("/tmp/fb-2-new.buf"),
            diff_state: FileBufferDiffState {
                total_size: 42,
                position: 40,
                ..Default::default()
            },
        };

        db.set_meta("fb-2", &new_meta).unwrap();
        let loaded = db.get_buffer("fb-2").unwrap();
        assert_eq!(loaded.file_inode_id, 999);
        assert_eq!(loaded.diff_file_path, PathBuf::from("/tmp/fb-2-new.buf"));
        assert_eq!(loaded.diff_state.read().unwrap().total_size, 42);
    }

    #[test]
    fn test_load_all_and_remove() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        db.add_buffer(&make_test_record("fb-a", 1)).unwrap();
        db.add_buffer(&make_test_record("fb-b", 2)).unwrap();

        let records = db.load_all().unwrap();
        assert_eq!(records.len(), 2);

        db.remove("fb-a").unwrap();
        let handles = db.list_handles().unwrap();
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0], "fb-b");
    }
}
