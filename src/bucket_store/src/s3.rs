//! Records behind the S3 compatible face (§12.3): access keys bound to a
//! `(user, app)` with an optional narrowing scope, and multipart sessions
//! whose parts are chunks already in the named store.

use crate::access::ActionSet;
use crate::db::{exec, to_json};
use crate::error::*;
use crate::store::BucketStore;
use crate::types::*;
use rand::Rng;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

/// MinIO service-account style narrowing of a key (§4.4.8).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct S3KeyScope {
    /// Bucket ids the key may touch; empty = every bucket of its (user, app).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<ActionSet>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Key {
    pub access_key_id: String,
    /// Only returned when the key is created.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret: String,
    pub user_id: String,
    pub appid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<S3KeyScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket_id: BucketId,
    pub key: String,
    /// The binding fields given at creation (content type, metadata, ...).
    pub template: PutRequest,
    pub created_by: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultipartPart {
    pub part_number: u32,
    pub chunk_ids: Vec<String>,
    pub size: u64,
    pub md5: String,
}

fn random_token(len: usize, alphabet: &[u8]) -> String {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| alphabet[rng.gen_range(0..alphabet.len())] as char).collect()
}

const KEY_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const SECRET_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn key_from_row(row: &rusqlite::Row) -> rusqlite::Result<S3Key> {
    let scope: Option<String> = row.get(4)?;
    Ok(S3Key {
        access_key_id: row.get(0)?,
        secret: row.get(1)?,
        user_id: row.get(2)?,
        appid: row.get(3)?,
        scope: scope.and_then(|s| serde_json::from_str(&s).ok()),
        expires_at: row.get(5)?,
        revoked_at: row.get(6)?,
        created_at: row.get(7)?,
    })
}

const KEY_COLS: &str = "access_key_id, secret, user_id, appid, scope_json, expires_at, revoked_at, created_at";

impl BucketStore {
    /// Issues a key for `(user, appid)`; the secret is only in this result.
    pub fn create_s3_key(&self, user_id: &str, appid: &str, scope: Option<S3KeyScope>, expires_at: Option<i64>) -> BucketResult<S3Key> {
        if user_id.is_empty() || appid.is_empty() {
            return Err(BucketError::InvalidArgument("an S3 key belongs to a user and an app".into()));
        }
        if expires_at.is_some_and(|t| t <= now_ms()) {
            return Err(BucketError::InvalidArgument("expires_at must be in the future".into()));
        }
        let key = S3Key {
            access_key_id: format!("BK{}", random_token(18, KEY_ALPHABET)),
            secret: random_token(40, SECRET_ALPHABET),
            user_id: user_id.into(),
            appid: appid.into(),
            scope,
            expires_at,
            revoked_at: None,
            created_at: now_ms(),
        };
        self.db.write(|tx| {
            exec(
                tx,
                &format!("INSERT INTO s3_access_keys ({KEY_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7)"),
                params![key.access_key_id, key.secret, key.user_id, key.appid, key.scope.as_ref().map(to_json), key.expires_at, key.created_at],
            )?;
            Ok(())
        })?;
        log::info!("s3 key {} issued for {}|{}", key.access_key_id, user_id, appid);
        Ok(key)
    }

    /// A usable key (not revoked, not expired), secret included.
    pub fn s3_key(&self, access_key_id: &str) -> BucketResult<Option<S3Key>> {
        let key = self.db.read(|c| {
            Ok(c.query_row(&format!("SELECT {KEY_COLS} FROM s3_access_keys WHERE access_key_id = ?1"), params![access_key_id], key_from_row)
                .optional()?)
        })?;
        let now = now_ms();
        Ok(key.filter(|k| k.revoked_at.is_none() && k.expires_at.map_or(true, |t| t > now)))
    }

    /// Keys of a user, secrets withheld.
    pub fn list_s3_keys(&self, user_id: &str) -> BucketResult<Vec<S3Key>> {
        self.db.read(|c| {
            let mut stmt = c.prepare(&format!("SELECT {KEY_COLS} FROM s3_access_keys WHERE user_id = ?1 ORDER BY created_at"))?;
            let rows = stmt.query_map(params![user_id], key_from_row)?;
            let mut out = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            for k in &mut out {
                k.secret.clear();
            }
            Ok(out)
        })
    }

    pub fn revoke_s3_key(&self, user_id: &str, access_key_id: &str) -> BucketResult<()> {
        let n = self.db.write(|tx| {
            exec(
                tx,
                "UPDATE s3_access_keys SET revoked_at = ?3 WHERE access_key_id = ?1 AND user_id = ?2 AND revoked_at IS NULL",
                params![access_key_id, user_id, now_ms()],
            )
        })?;
        if n == 0 {
            return Err(BucketError::InvalidArgument(format!("no active key {access_key_id} of {user_id}")));
        }
        log::info!("s3 key {access_key_id} of {user_id} revoked");
        Ok(())
    }

    // ---------- multipart sessions ----------

    pub fn create_multipart(&self, bucket_id: &BucketId, template: &PutRequest, created_by: &str) -> BucketResult<MultipartUpload> {
        validate_key(&template.key)?;
        self.bucket(bucket_id)?;
        let upload = MultipartUpload {
            upload_id: random_token(32, SECRET_ALPHABET),
            bucket_id: bucket_id.clone(),
            key: template.key.clone(),
            template: template.clone(),
            created_by: created_by.into(),
            created_at: now_ms(),
        };
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO s3_multipart_uploads (upload_id, bucket_id, key, metadata_json, created_by, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![upload.upload_id, bucket_id.to_string(), upload.key, to_json(&upload.template), created_by, upload.created_at],
            )?;
            Ok(())
        })?;
        Ok(upload)
    }

    pub fn multipart(&self, upload_id: &str) -> BucketResult<Option<MultipartUpload>> {
        self.db.read(|c| {
            let row = c
                .query_row(
                    "SELECT upload_id, bucket_id, key, metadata_json, created_by, created_at FROM s3_multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, String>(4)?,
                            r.get::<_, i64>(5)?,
                        ))
                    },
                )
                .optional()?;
            let Some((upload_id, bucket, key, template, created_by, created_at)) = row else { return Ok(None) };
            Ok(Some(MultipartUpload {
                upload_id,
                bucket_id: bucket.parse()?,
                key,
                template: template.and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default(),
                created_by,
                created_at,
            }))
        })
    }

    pub fn list_multipart(&self, bucket_id: &BucketId) -> BucketResult<Vec<MultipartUpload>> {
        let ids: Vec<String> = self.db.read(|c| {
            let mut stmt = c.prepare("SELECT upload_id FROM s3_multipart_uploads WHERE bucket_id = ?1 ORDER BY key, created_at")?;
            let rows = stmt.query_map(params![bucket_id.to_string()], |r| r.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        let mut out = Vec::new();
        for id in ids {
            out.extend(self.multipart(&id)?);
        }
        Ok(out)
    }

    pub fn put_multipart_part(&self, upload_id: &str, part: &MultipartPart) -> BucketResult<()> {
        self.db.write(|tx| {
            exec(
                tx,
                "INSERT INTO s3_multipart_parts (upload_id, part_number, chunk_ids_json, size, md5) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(upload_id, part_number) DO UPDATE SET chunk_ids_json = excluded.chunk_ids_json, size = excluded.size, md5 = excluded.md5",
                params![upload_id, part.part_number, to_json(&part.chunk_ids), part.size as i64, part.md5],
            )?;
            Ok(())
        })
    }

    pub fn multipart_parts(&self, upload_id: &str) -> BucketResult<Vec<MultipartPart>> {
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT part_number, chunk_ids_json, size, md5 FROM s3_multipart_parts WHERE upload_id = ?1 ORDER BY part_number")?;
            let rows = stmt.query_map(params![upload_id], |r| {
                let chunks: String = r.get(1)?;
                Ok(MultipartPart {
                    part_number: r.get::<_, i64>(0)? as u32,
                    chunk_ids: serde_json::from_str(&chunks).unwrap_or_default(),
                    size: r.get::<_, i64>(2)? as u64,
                    md5: r.get(3)?,
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn delete_multipart(&self, upload_id: &str) -> BucketResult<()> {
        self.db.write(|tx| {
            exec(tx, "DELETE FROM s3_multipart_parts WHERE upload_id = ?1", params![upload_id])?;
            exec(tx, "DELETE FROM s3_multipart_uploads WHERE upload_id = ?1", params![upload_id])?;
            Ok(())
        })
    }

    /// Multipart sessions older than `max_age_ms` (abandoned uploads to abort).
    pub fn stale_multipart(&self, max_age_ms: i64) -> BucketResult<Vec<String>> {
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT upload_id FROM s3_multipart_uploads WHERE created_at <= ?1")?;
            let rows = stmt.query_map(params![now_ms() - max_age_ms], |r| r.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Writes a FileObject over already-stored chunks (the parts of a multipart upload) and binds it.
    pub async fn put_chunks(
        &self,
        id: &BucketId,
        caller: &crate::access::Caller,
        chunk_ids: Vec<ndn_lib::ChunkId>,
        size: u64,
        mut req: PutRequest,
    ) -> BucketResult<PutResult> {
        let bucket = self.bucket(id)?;
        let name = req.key.rsplit('/').next().unwrap_or(&req.key).to_string();
        let content = match chunk_ids.len() {
            0 => String::new(),
            1 => chunk_ids[0].to_string(),
            _ => {
                let list = ndn_lib::ChunkList::from_chunk_list(chunk_ids).map_err(store_err)?;
                let (list_id, list_str) = list.gen_obj_id();
                self.ndm.put_object(&list_id, &list_str).await.map_err(store_err)?;
                list_id.to_string()
            }
        };
        let mut file = ndn_lib::FileObject::new(name, size, content);
        if let Some(m) = &req.content_type {
            file.meta.insert("mime".into(), serde_json::json!(m));
        }
        let (obj_id, body) = ndn_lib::NamedObject::gen_obj_id(&file);
        self.ndm.put_object(&obj_id, &body).await.map_err(store_err)?;
        req.obj_id = obj_id.to_string();
        self.put_trusted(&bucket, caller, req).await
    }
}
