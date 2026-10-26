//! Turning bytes into named objects and back (direct PUT, snapshot manifests).

use crate::error::*;
use named_store::NamedDataMgr;
use ndn_lib::{load_named_obj, ChunkHasher, ChunkId, ChunkList, FileObject, NamedObject, ObjId, CHUNK_DEFAULT_SIZE};
use tokio::io::AsyncReadExt;

pub fn chunk_id_of(bytes: &[u8]) -> BucketResult<ChunkId> {
    let hasher = ChunkHasher::new(None).map_err(store_err)?;
    let method = hasher.hash_method;
    let raw = hasher.calc_from_bytes(bytes);
    ChunkId::from_mix_hash_result_by_hash_method(bytes.len() as u64, &raw, method).map_err(store_err)
}

/// Stores `bytes` as chunks (a chunk list beyond 32 MiB) plus a FileObject.
pub async fn store_bytes(ndm: &NamedDataMgr, name: &str, bytes: &[u8], mime: Option<&str>) -> BucketResult<(FileObject, ObjId)> {
    let mut chunks = Vec::new();
    for piece in bytes.chunks(CHUNK_DEFAULT_SIZE as usize) {
        let piece = piece.to_vec();
        let id = tokio::task::spawn_blocking({
            let piece = piece.clone();
            move || chunk_id_of(&piece)
        })
        .await
        .map_err(|e| BucketError::Internal(format!("hash task: {e}")))??;
        if !ndm.have_chunk(&id).await {
            ndm.put_chunk(&id, &piece).await.map_err(store_err)?;
        }
        chunks.push(id);
    }
    let content = match chunks.len() {
        0 => String::new(),
        1 => chunks[0].to_string(),
        _ => {
            let list = ChunkList::from_chunk_list(chunks).map_err(store_err)?;
            let (list_id, list_str) = list.gen_obj_id();
            ndm.put_object(&list_id, &list_str).await.map_err(store_err)?;
            list_id.to_string()
        }
    };
    let mut obj = FileObject::new(name.to_string(), bytes.len() as u64, content);
    if let Some(m) = mime {
        obj.meta.insert("mime".into(), serde_json::json!(m));
    }
    let (id, body) = obj.gen_obj_id();
    ndm.put_object(&id, &body).await.map_err(store_err)?;
    Ok((obj, id))
}

/// Reads a whole (small) FileObject's content.
pub async fn read_file(ndm: &NamedDataMgr, file: &FileObject, limit: u64) -> BucketResult<Vec<u8>> {
    if file.size > limit {
        return Err(BucketError::InvalidArgument(format!("{} bytes exceed {limit}", file.size)));
    }
    if file.content.is_empty() {
        return Ok(Vec::new());
    }
    let content = ObjId::new(&file.content).map_err(store_err)?;
    let (mut reader, _) = ndm.open_reader(&content, None).await.map_err(|e| BucketError::ContentNotAvailable(e.to_string()))?;
    let mut out = Vec::with_capacity(file.size as usize);
    reader.read_to_end(&mut out).await.map_err(|e| BucketError::ContentNotAvailable(e.to_string()))?;
    Ok(out)
}

pub async fn load_file(ndm: &NamedDataMgr, id: &ObjId) -> BucketResult<FileObject> {
    let body = ndm.get_object(id).await.map_err(|e| BucketError::ContentNotAvailable(e.to_string()))?;
    load_named_obj(&body).map_err(store_err)
}
