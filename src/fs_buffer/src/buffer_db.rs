use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use ndn_lib::{NdnError, NdnResult};

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

    pub fn get_dirty_order(&self, handle_id: &str) -> NdnResult<Option<Vec<u32>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT dirty_order FROM file_buffers WHERE handle_id = ?1",
            )
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
        let order_json = serde_json::to_string(order)
            .map_err(|e| NdnError::InvalidParam(e.to_string()))?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO file_buffers (handle_id, dirty_order, meta_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(handle_id) DO UPDATE SET
                dirty_order = excluded.dirty_order,
                updated_at = excluded.updated_at",
            params![
                handle_id,
                order_json,
                "{}",
                unix_timestamp() as i64,
                unix_timestamp() as i64
            ],
        )
        .map_err(|e| NdnError::DbError(format!("insert failed: {}", e)))?;
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
    use tempfile::tempdir;

    #[test]
    fn test_dirty_order_roundtrip() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fb.db");
        let db = LocalFileBufferDB::new(db_path).unwrap();

        let handle_id = "fb-test-1";
        let order = vec![3u32, 1u32, 7u32];
        db.set_dirty_order(handle_id, &order).unwrap();

        let loaded = db.get_dirty_order(handle_id).unwrap().unwrap();
        assert_eq!(loaded, order);

        db.remove(handle_id).unwrap();
        let loaded = db.get_dirty_order(handle_id).unwrap();
        assert!(loaded.is_none());
    }
}
