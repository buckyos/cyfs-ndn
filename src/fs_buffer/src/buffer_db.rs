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
                dirty_order TEXT NOT NULL,
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

    /// Add a new buffer record to the database
    pub fn add_buffer(&self, record: &FileBufferRecord) -> NdnResult<()> {
        let meta = record.to_meta();
        let meta_json = serde_json::to_string(&meta)
            .map_err(|e| NdnError::InvalidParam(format!("serialize meta failed: {}", e)))?;
        let order = record
            .dirty_layout
            .read()
            .map_err(|_| NdnError::InvalidState("dirty layout poisoned".to_string()))?;
        let order_json = serde_json::to_string(&order.order)
            .map_err(|e| NdnError::InvalidParam(format!("serialize order failed: {}", e)))?;

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO file_buffers (handle_id, dirty_order, meta_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.handle_id,
                order_json,
                meta_json,
                unix_timestamp() as i64,
                unix_timestamp() as i64
            ],
        )
        .map_err(|e| NdnError::DbError(format!("insert buffer failed: {}", e)))?;
        Ok(())
    }

    /// Get a buffer record by handle_id
    pub fn get_buffer(&self, handle_id: &str) -> NdnResult<FileBufferRecord> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT meta_json, dirty_order FROM file_buffers WHERE handle_id = ?1")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;

        let row = stmt.query_row(params![handle_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        });

        match row {
            Ok((meta_json, order_json)) => {
                let meta: FileBufferRecordMeta = serde_json::from_str(&meta_json)
                    .map_err(|e| NdnError::DecodeError(format!("decode meta failed: {}", e)))?;
                let order: Vec<u32> = serde_json::from_str(&order_json)
                    .map_err(|e| NdnError::DecodeError(format!("decode order failed: {}", e)))?;
                Ok(FileBufferRecord::from_meta_with_layout(meta, order))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(NdnError::NotFound(format!(
                "buffer not found: {}",
                handle_id
            ))),
            Err(e) => Err(NdnError::DbError(format!("query failed: {}", e))),
        }
    }

    /// List all buffer handle_ids
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

    /// Load all buffer records from database
    pub fn load_all(&self) -> NdnResult<Vec<FileBufferRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT meta_json, dirty_order FROM file_buffers")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| NdnError::DbError(format!("query failed: {}", e)))?;

        let mut records = Vec::new();
        for row in rows {
            let (meta_json, order_json) =
                row.map_err(|e| NdnError::DbError(format!("read row failed: {}", e)))?;
            let meta: FileBufferRecordMeta = serde_json::from_str(&meta_json)
                .map_err(|e| NdnError::DecodeError(format!("decode meta failed: {}", e)))?;
            let order: Vec<u32> = serde_json::from_str(&order_json)
                .map_err(|e| NdnError::DecodeError(format!("decode order failed: {}", e)))?;
            records.push(FileBufferRecord::from_meta_with_layout(meta, order));
        }
        Ok(records)
    }

    pub fn get_dirty_order(&self, handle_id: &str) -> NdnResult<Option<Vec<u32>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT dirty_order FROM file_buffers WHERE handle_id = ?1")
            .map_err(|e| NdnError::DbError(format!("prepare failed: {}", e)))?;
        let row = stmt.query_row(params![handle_id], |row| row.get::<_, String>(0));
        match row {
            Ok(order_json) => {
                let order: Vec<u32> = serde_json::from_str(&order_json)
                    .map_err(|e| NdnError::DecodeError(e.to_string()))?;
                Ok(Some(order))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(NdnError::DbError(format!("query failed: {}", e))),
        }
    }

    pub fn set_dirty_order(&self, handle_id: &str, order: &[u32]) -> NdnResult<()> {
        let order_json =
            serde_json::to_string(order).map_err(|e| NdnError::InvalidParam(e.to_string()))?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE file_buffers SET dirty_order = ?1, updated_at = ?2 WHERE handle_id = ?3",
            params![order_json, unix_timestamp() as i64, handle_id,],
        )
        .map_err(|e| NdnError::DbError(format!("update dirty_order failed: {}", e)))?;
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
    use crate::local_filebuffer::FileBufferBaseReader;
    use tempfile::tempdir;

    fn make_test_record(handle_id: &str, order: Vec<u32>) -> FileBufferRecord {
        FileBufferRecord::from_meta_with_layout(
            crate::local_filebuffer::FileBufferRecordMeta {
                handle_id: handle_id.to_string(),
                file_inode_id: 12345,
                base_reader: FileBufferBaseReader::None,
                read_only: false,
            },
            order,
        )
    }

    #[test]
    fn test_dirty_order_roundtrip() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let handle_id = "fb-test-1";
        let order = vec![3u32, 1u32, 7u32];

        // First add a buffer record
        let record = make_test_record(handle_id, order.clone());
        db.add_buffer(&record).unwrap();

        // Verify dirty order was saved
        let loaded = db.get_dirty_order(handle_id).unwrap().unwrap();
        assert_eq!(loaded, order);

        // Update the dirty order
        let new_order = vec![5u32, 2u32, 9u32];
        db.set_dirty_order(handle_id, &new_order).unwrap();
        let loaded = db.get_dirty_order(handle_id).unwrap().unwrap();
        assert_eq!(loaded, new_order);

        // Remove and verify
        db.remove(handle_id).unwrap();
        let loaded = db.get_dirty_order(handle_id).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_add_get_buffer() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb2.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let handle_id = "fb-test-2";
        let order = vec![1u32, 2u32, 3u32];
        let record = make_test_record(handle_id, order.clone());

        db.add_buffer(&record).unwrap();

        let loaded = db.get_buffer(handle_id).unwrap();
        assert_eq!(loaded.handle_id, handle_id);
        assert_eq!(loaded.file_inode_id, 12345);
        assert!(!loaded.read_only);

        let loaded_order = loaded.dirty_layout.read().unwrap().order.clone();
        assert_eq!(loaded_order, order);
    }

    #[test]
    fn test_load_all() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb3.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let record1 = make_test_record("fb-a", vec![1, 2]);
        let record2 = make_test_record("fb-b", vec![3, 4, 5]);

        db.add_buffer(&record1).unwrap();
        db.add_buffer(&record2).unwrap();

        let all = db.load_all().unwrap();
        assert_eq!(all.len(), 2);

        let ids: Vec<&str> = all.iter().map(|r| r.handle_id.as_str()).collect();
        assert!(ids.contains(&"fb-a"));
        assert!(ids.contains(&"fb-b"));
    }
}
