use super::chunk::{
    ChunkId, ChunkType, CALC_HASH_PIECE_SIZE, COPY_CHUNK_BUFFER_SIZE, QCID_HASH_PIECE_SIZE,
};
use crate::hash::DEFAULT_HASH_METHOD;
use crate::{HashHelper, HashMethod, Hasher, NdnError, NdnResult, QCID_SAMPLE_THRESHOLD};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::str::FromStr;
use std::{future::Future, io::SeekFrom, ops::Range, path::PathBuf, pin::Pin};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};

// 添加类型别名来简化 copy_chunk 的签名
pub type ChunkProgressCallback = Option<
    Box<
        dyn FnMut(
                ChunkId,
                u64,
                &Option<ChunkHasher>,
            ) -> Pin<Box<dyn Future<Output = NdnResult<()>> + Send + 'static>>
            + Send,
    >,
>;

pub struct ChunkHasher {
    pub hash_method: HashMethod,
    pub hash_length: u64,
    pub hasher: Box<dyn Hasher + Send + Sync>,
    //can extend other hash type in the future
}

impl ChunkHasher {
    pub fn new(hash_type: Option<&str>) -> NdnResult<Self> {
        // default is sha256
        let hash_method = match hash_type {
            Some(ht) => HashMethod::from_str(ht)?,
            None => HashMethod::default(),
        };

        let hasher = HashHelper::create_hasher(hash_method)?;

        Ok(Self {
            hash_method,
            hash_length: 0,
            hasher: hasher,
        })
    }

    pub fn new_with_hash_method(hash_type: HashMethod) -> NdnResult<Self> {
        let hasher = HashHelper::create_hasher(hash_type)?;

        Ok(Self {
            hash_method: hash_type,
            hash_length: 0,
            hasher,
        })
    }

    pub fn get_pos(&self) -> u64 {
        self.hasher.get_pos()
    }

    pub fn restore_from_state(state_json: serde_json::Value) -> NdnResult<Self> {
        let mut hash_str_type = DEFAULT_HASH_METHOD;
        let hash_type = state_json.get("hash_type");
        if hash_type.is_some() {
            hash_str_type = hash_type.unwrap().as_str().unwrap();
        }
        let hash_method = HashMethod::from_str(hash_str_type)?;

        // Load hash length
        let hash_length = state_json
            .get("hash_length")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Load hasher state
        let mut hasher = HashHelper::create_hasher(hash_method)?;
        hasher.restore_from_state(state_json)?;

        Ok(Self {
            hash_method,
            hash_length,
            hasher,
        })
    }

    pub fn save_state(&self) -> NdnResult<serde_json::Value> {
        let mut v = self.hasher.save_state()?;

        // Add hash length
        v.as_object_mut().unwrap().insert(
            "hash_length".to_string(),
            serde_json::json!(self.hash_length),
        );

        Ok(v)
    }

    //return the hash result and the total read size
    pub async fn calc_from_reader<T: AsyncRead + Unpin>(
        mut self,
        reader: &mut T,
    ) -> NdnResult<(Vec<u8>, u64)> {
        //TODO: add other hash type support

        let mut buffer = vec![0u8; CALC_HASH_PIECE_SIZE as usize];
        let mut total_read = 0;
        loop {
            let n = reader.read(&mut buffer).await.map_err(|e| {
                warn!("ChunkHasher: read failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            // 如果读取到0字节，表示已经到达EOF
            if n == 0 {
                break;
            }

            // 更新哈希计算器
            self.hasher.update_from_bytes(&buffer[..n]);
            total_read += n as u64;
        }

        self.hash_length += total_read;

        Ok((self.hasher.finalize().to_vec(), total_read))
    }

    //size表示从reader中计算的长度
    pub async fn calc_from_reader_with_length<T: AsyncRead + Unpin>(
        mut self,
        reader: &mut T,
        cacl_len: u64,
    ) -> NdnResult<(Vec<u8>, u64)> {
        let mut total_read = 0;
        loop {
            let mut buffer_len = cacl_len - total_read;
            if buffer_len > CALC_HASH_PIECE_SIZE {
                buffer_len = CALC_HASH_PIECE_SIZE
            }
            let mut buffer = vec![0u8; buffer_len as usize];
            let n = reader.read(&mut buffer).await.map_err(|e| {
                warn!("ChunkHasher: read failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;

            // 如果读取到0字节，表示已经到达EOF
            if n == 0 {
                break;
            }

            // 更新哈希计算器
            self.hasher.update_from_bytes(&buffer[..n]);
            total_read += n as u64;
        }

        self.hash_length += total_read;

        Ok((self.hasher.finalize().to_vec(), total_read))
    }

    pub fn calc_from_bytes(mut self, bytes: &[u8]) -> Vec<u8> {
        self.hash_length += bytes.len() as u64;
        self.hasher.update_from_bytes(bytes);
        self.hasher.finalize().to_vec()
    }

    pub fn calc_chunk_id_from_bytes(mut self, bytes: &[u8]) -> ChunkId {
        self.hash_length += bytes.len() as u64;
        self.hasher.update_from_bytes(bytes);
        self.finalize_chunk_id()
    }

    pub fn calc_mix_chunk_id_from_bytes(mut self, bytes: &[u8]) -> NdnResult<ChunkId> {
        self.hash_length += bytes.len() as u64;
        self.hasher.update_from_bytes(bytes);
        self.finalize_mix_chunk_id()
    }

    pub fn update_from_bytes(&mut self, bytes: &[u8]) {
        self.hash_length += bytes.len() as u64;
        self.hasher.update_from_bytes(bytes);
    }

    pub fn finalize(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }

    pub fn finalize_chunk_id(self) -> ChunkId {
        let hash_result = self.hasher.finalize();
        let chunk_type = ChunkType::from_hash_type(self.hash_method, false).unwrap();
        ChunkId::from_hash_result(&hash_result, chunk_type)
    }

    pub fn finalize_mix_chunk_id(self) -> NdnResult<ChunkId> {
        let hash_result = self.hasher.finalize();
        let chunk_type = ChunkType::from_hash_type(self.hash_method, true)?;
        Ok(ChunkId::from_mix_hash_result(
            self.hash_length,
            &hash_result,
            chunk_type,
        ))
    }
}

fn finalize_qcid(length: u64, hasher: Sha256) -> ChunkId {
    let hash_result = hasher.finalize();
    ChunkId::from_mix_hash_result(length, &hash_result, ChunkType::QCID)
}

fn qcid_sample_offsets(length: u64) -> Option<[u64; 3]> {
    if length < QCID_SAMPLE_THRESHOLD {
        None
    } else {
        Some([
            0,
            (length - QCID_HASH_PIECE_SIZE) / 2,
            length - QCID_HASH_PIECE_SIZE,
        ])
    }
}

/// Calculate the canonical QCID of a seekable reader.
///
/// Files smaller than 12 KiB are hashed in full. Larger files hash three
/// 4 KiB pieces at the head, centered midpoint, and tail. All offsets are
/// absolute from the beginning of the reader.
pub async fn calc_quick_hash<T: AsyncRead + AsyncSeek + Unpin>(
    reader: &mut T,
    length: Option<u64>,
) -> NdnResult<ChunkId> {
    let length = if let Some(length) = length {
        length
    } else {
        let length = reader.seek(SeekFrom::End(0)).await.map_err(|e| {
            warn!("calc_quick_hash: seek file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        reader.seek(SeekFrom::Start(0)).await.map_err(|e| {
            warn!("calc_quick_hash: seek file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        length
    };

    let mut hasher = Sha256::new();
    if let Some(offsets) = qcid_sample_offsets(length) {
        let mut piece = [0u8; QCID_HASH_PIECE_SIZE as usize];
        for offset in offsets {
            reader.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                warn!("calc_quick_hash: seek file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
            reader.read_exact(&mut piece).await.map_err(|e| {
                warn!("calc_quick_hash: read file failed! {}", e.to_string());
                NdnError::IoError(e.to_string())
            })?;
            hasher.update(&piece);
        }
    } else {
        reader.seek(SeekFrom::Start(0)).await.map_err(|e| {
            warn!("calc_quick_hash: seek file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        let mut buffer = vec![0u8; length as usize];
        reader.read_exact(&mut buffer).await.map_err(|e| {
            warn!("calc_quick_hash: read file failed! {}", e.to_string());
            NdnError::IoError(e.to_string())
        })?;
        hasher.update(&buffer);
    }

    Ok(finalize_qcid(length, hasher))
}

/// Calculate the canonical QCID of an in-memory file.
pub fn calc_quick_hash_by_buffer(buffer: &[u8]) -> NdnResult<ChunkId> {
    let length = buffer.len() as u64;
    let mut hasher = Sha256::new();
    if let Some(offsets) = qcid_sample_offsets(length) {
        let piece_size = QCID_HASH_PIECE_SIZE as usize;
        for offset in offsets {
            let start = offset as usize;
            hasher.update(&buffer[start..start + piece_size]);
        }
    } else {
        hasher.update(buffer);
    }

    Ok(finalize_qcid(length, hasher))
}

pub async fn calculate_file_chunk_id(
    file_path: &str,
    chunk_type: ChunkType,
) -> NdnResult<(ChunkId, u64)> {
    let hash_method = chunk_type.to_hash_method()?;

    let mut file_reader = tokio::fs::File::open(file_path).await.map_err(|err| {
        warn!(
            "calculate_file_chunk_id: open file failed! {}",
            err.to_string()
        );
        NdnError::IoError(err.to_string())
    })?;

    let mut hasher = ChunkHasher::new_with_hash_method(hash_method)?;
    let (hash_result, file_size) = hasher.calc_from_reader(&mut file_reader).await?;
    if chunk_type.is_mix() {
        let mix_chunk_id = ChunkId::from_mix_hash_result(file_size, &hash_result, chunk_type);
        return Ok((mix_chunk_id, file_size));
    } else {
        let chunk_id = ChunkId::from_hash_result(&hash_result, chunk_type);
        return Ok((chunk_id, file_size));
    }
}

fn same_file_metadata(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

/// Calculate a file QCID and return the stable metadata snapshot used for it.
pub async fn calculate_qcid_from_file_with_metadata(
    file_path: &Path,
) -> NdnResult<(ChunkId, std::fs::Metadata)> {
    let mut file_reader = tokio::fs::File::open(file_path).await.map_err(|err| {
        warn!(
            "calculate_qcid_from_file_with_metadata: open file failed! {}",
            err.to_string()
        );
        NdnError::IoError(err.to_string())
    })?;
    let before = file_reader
        .metadata()
        .await
        .map_err(|err| NdnError::IoError(err.to_string()))?;
    let qcid = calc_quick_hash(&mut file_reader, Some(before.len())).await?;
    let after = file_reader
        .metadata()
        .await
        .map_err(|err| NdnError::IoError(err.to_string()))?;
    let path_after = tokio::fs::metadata(file_path)
        .await
        .map_err(|err| NdnError::IoError(err.to_string()))?;

    if !same_file_metadata(&before, &after) || !same_file_metadata(&after, &path_after) {
        return Err(NdnError::InvalidData(format!(
            "file changed while calculating qcid: {}",
            file_path.display()
        )));
    }

    Ok((qcid, after))
}

pub async fn caculate_qcid_from_file(file_path: &Path) -> NdnResult<ChunkId> {
    Ok(calculate_qcid_from_file_with_metadata(file_path).await?.0)
}

pub async fn copy_chunk<R, W>(
    chunk_id: ChunkId,
    mut chunk_reader: R,
    mut chunk_writer: W,
    mut hasher: Option<ChunkHasher>,
    mut progress_callback: ChunkProgressCallback,
) -> NdnResult<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut total_copied: u64 = 0;
    let mut buffer = vec![0u8; COPY_CHUNK_BUFFER_SIZE];

    loop {
        let n = tokio::io::AsyncReadExt::read(&mut chunk_reader, &mut buffer)
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;
        if n == 0 {
            break;
        }

        if let Some(ref mut hasher) = hasher {
            let hash_method = chunk_id.chunk_type.to_hash_method()?;
            if hasher.hash_method == hash_method {
                hasher.update_from_bytes(&buffer[..n]);
            } else {
                return Err(NdnError::Internal(format!(
                    "hash type mismatch:{}",
                    hasher.hash_method.as_str()
                )));
            }
        }

        tokio::io::AsyncWriteExt::write_all(&mut chunk_writer, &buffer[..n])
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?;
        total_copied += n as u64;

        if let Some(ref mut progress_callback) = progress_callback {
            progress_callback(chunk_id.clone(), total_copied, &hasher).await?;
        }
    }

    if let Some(hasher) = hasher {
        let result_chunk_id;
        if chunk_id.chunk_type.is_mix() {
            result_chunk_id = hasher.finalize_mix_chunk_id()?;
        } else {
            result_chunk_id = hasher.finalize_chunk_id();
        }

        if result_chunk_id != chunk_id {
            return Err(NdnError::VerifyError(format!(
                "copy chunk hash mismatch:{}",
                result_chunk_id.to_string()
            )));
        }
    }

    Ok(total_copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::io::Cursor;

    fn qcid_test_data(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn test_qcid_canonical_vectors_and_reader_buffer_consistency() {
        let vectors = [
            (
                0usize,
                "qcid:00e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                1,
                "qcid:016e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
            ),
            (
                4095,
                "qcid:ff1f45de2924756389e3ccab98bdaacbef8a81cdeb651b59f916a6d6385b4f7b999d",
            ),
            (
                4096,
                "qcid:8020d67c656e01756650d77717b0839985a056ec28ffe174601d690fc407a2ceffca",
            ),
            (
                12287,
                "qcid:ff5f78ac95e2a27d63518fe831893f210699f7d6d6091071e6d5adccaaa476caf39c",
            ),
            (
                12288,
                "qcid:80602ffe74f47a7bb7350e913f6b9259080cbe3cee97b2d313d5e2fe2942108d98e9",
            ),
            (
                12289,
                "qcid:8160e526e1336f9c56d5012c520783dba4c96852a49a515a988447b940253faa28f1",
            ),
        ];

        for (length, expected) in vectors {
            let data = qcid_test_data(length);
            let buffer_qcid = calc_quick_hash_by_buffer(&data).unwrap();
            assert_eq!(buffer_qcid.to_string(), expected, "length={length}");

            let mut reader = Cursor::new(data);
            let reader_qcid = calc_quick_hash(&mut reader, None).await.unwrap();
            assert_eq!(reader_qcid, buffer_qcid, "length={length}");
        }
    }

    #[test]
    fn test_qcid_large_file_samples_head_center_and_tail() {
        let length = QCID_SAMPLE_THRESHOLD as usize + 101;
        let data = vec![0u8; length];
        let original = calc_quick_hash_by_buffer(&data).unwrap();
        let center_start = (length - QCID_HASH_PIECE_SIZE as usize) / 2;

        for changed_offset in [0, center_start, length - 1] {
            let mut changed = data.clone();
            changed[changed_offset] = 1;
            assert_ne!(calc_quick_hash_by_buffer(&changed).unwrap(), original);
        }
    }

    #[test]
    fn test_chunk_hasher_save_state() {
        let mut buffer = vec![0u8; 2048];
        let mut rng = rand::thread_rng();
        rng.fill(&mut buffer[..]);

        let mut chunk_hasher = ChunkHasher::new(None).unwrap();
        let hash_result = chunk_hasher.calc_from_bytes(&buffer);
        let chunk_id2 = ChunkId::from_mix_hash_result(2048, &hash_result, ChunkType::Mix256);
        println!("chunk_id2: {}", chunk_id2.to_string());

        let hash_result_restored = {
            let mut chunk_hasher = ChunkHasher::new(None).unwrap();
            chunk_hasher.update_from_bytes(&buffer[..1024]);
            let state_json = chunk_hasher.save_state().unwrap();
            println!("state_json:{}", state_json.to_string());

            let mut chunk_hasher_restored = ChunkHasher::restore_from_state(state_json).unwrap();
            chunk_hasher_restored.update_from_bytes(&buffer[1024..]);
            // let hash = chunk_hasher_restored.finalize();

            let chunk_id = chunk_hasher_restored.finalize_mix_chunk_id().unwrap();
            let length = chunk_id.get_length().unwrap_or(0);
            println!("chunk_id: {}, length: {}", chunk_id.to_string(), length);
            assert_eq!(length, 2048);

            chunk_id.hash_result.clone()
        };

        assert_eq!(chunk_id2.hash_result, hash_result_restored);
    }
}
