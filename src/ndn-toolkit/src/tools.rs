use log::{debug, info, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use ndm::NamedDataMgr;
use named_store::ChunkLocalInfo;
use ndn_lib::{
    caculate_qcid_from_file, ChunkHasher, ChunkId, ChunkType, DirObject, FileObject, NamedObject,
    NdnAction, NdnError, NdnProgressCallback, NdnResult, ObjId, ProgressCallbackResult,
    SimpleChunkList, StoreMode, CHUNK_DEFAULT_SIZE,
};

#[derive(PartialEq)]
pub enum CheckMode {
    ByQCID,
    ByFullHash,
}

impl CheckMode {
    pub fn is_support_quick_check(&self) -> bool {
        self == &CheckMode::ByQCID
    }
}

pub enum ContentToStore {
    Chunk(ChunkId, u64, ChunkLocalInfo),
    Object(ObjId, String),
}

impl ContentToStore {
    pub fn from_local_file(chunk_id: ChunkId, chunk_size: u64, chunk_local_info: ChunkLocalInfo) -> Self {
        ContentToStore::Chunk(chunk_id, chunk_size, chunk_local_info)
    }

    pub fn from_obj(obj_id: ObjId, obj_str: String) -> Self {
        ContentToStore::Object(obj_id, obj_str)
    }

    pub fn to_obj(self) -> NdnResult<(ObjId, String)> {
        match self {
            ContentToStore::Object(obj_id, obj_str) => Ok((obj_id, obj_str)),
            _ => Err(NdnError::InvalidParam("Invalid content to store".to_string())),
        }
    }

    pub fn to_local_file(self) -> NdnResult<(ChunkId, u64, ChunkLocalInfo)> {
        match self {
            ContentToStore::Chunk(chunk_id, chunk_size, local_info) => Ok((chunk_id, chunk_size, local_info)),
            _ => Err(NdnError::InvalidParam("Invalid content to store".to_string())),
        }
    }
}

async fn store_content_compat(content: ContentToStore, store_mode: StoreMode) -> NdnResult<()> {
    match store_mode {
        StoreMode::NoStore => Ok(()),
        StoreMode::StoreInNamedMgr => {
            warn!(
                "store_content_to_ndn_mgr: store-in-named-mgr path is not exposed by current NDM API, skip"
            );
            Ok(())
        }
        StoreMode::LocalFile(local_path, range, _) => {
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if !local_path.exists() {
                tokio::fs::File::create(&local_path).await?;
            }

            match content {
                ContentToStore::Object(_, obj_str) => {
                    let mut target = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(local_path)
                        .await?;
                    target.seek(std::io::SeekFrom::Start(range.start)).await?;
                    target.write_all(obj_str.as_bytes()).await?;
                    target.flush().await?;
                    Ok(())
                }
                ContentToStore::Chunk(_, _, chunk_local_info) => {
                    let bytes = tokio::fs::read(&chunk_local_info.path).await?;
                    let slice = if let Some(src_range) = chunk_local_info.range {
                        let start = src_range.start as usize;
                        let end = src_range.end as usize;
                        if end < start || end > bytes.len() {
                            return Err(NdnError::InvalidParam(format!(
                                "invalid source range {}..{} for file {}",
                                src_range.start, src_range.end, chunk_local_info.path
                            )));
                        }
                        &bytes[start..end]
                    } else {
                        &bytes
                    };

                    let mut target = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(local_path)
                        .await?;
                    target.seek(std::io::SeekFrom::Start(range.start)).await?;
                    target.write_all(slice).await?;
                    target.flush().await?;
                    Ok(())
                }
            }
        }
    }
}

pub async fn store_content_to_ndn_mgr_impl(
    _ndn_mgr: &NamedDataMgr,
    content: ContentToStore,
    store_mode: StoreMode,
) -> NdnResult<()> {
    store_content_compat(content, store_mode).await
}

pub async fn store_content_to_ndn_mgr(
    ndn_mgr_id: Option<&str>,
    content: ContentToStore,
    store_mode: StoreMode,
) -> NdnResult<()> {
    if matches!(store_mode, StoreMode::StoreInNamedMgr)
        && NamedDataMgr::get_named_data_mgr_by_id(ndn_mgr_id).await.is_none()
    {
        warn!("store_content_to_ndn_mgr: named data mgr not found, skip");
    }

    store_content_compat(content, store_mode).await
}

async fn call_ndn_callback(
    progress_callback: &Option<Arc<Mutex<NdnProgressCallback>>>,
    inner_path: String,
    action: NdnAction,
) -> NdnResult<ProgressCallbackResult> {
    if let Some(callback) = progress_callback {
        let mut callback = callback.lock().await;
        return callback(inner_path, action).await;
    }
    Ok(ProgressCallbackResult::Continue)
}

pub async fn cacl_file_object(
    ndn_mgr_id: Option<&str>,
    local_file_path: &Path,
    fileobj_template: &FileObject,
    use_chunklist: bool,
    check_mode: &CheckMode,
    store_mode: StoreMode,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
) -> NdnResult<(FileObject, ObjId, String)> {
    let file_meta = tokio::fs::metadata(local_file_path).await?;
    let file_size = file_meta.len();
    let file_last_modify_time = file_meta
        .modified()
        .ok()
        .and_then(|v| v.duration_since(UNIX_EPOCH).ok())
        .map(|v| v.as_secs())
        .unwrap_or_default();

    let mut file_obj_result = fileobj_template.clone();
    file_obj_result.size = file_size;
    file_obj_result.content_obj.create_time = file_last_modify_time;
    file_obj_result.content_obj.last_update_time = file_last_modify_time;
    file_obj_result.content_obj.name = local_file_path
        .file_name()
        .map(|v| v.to_string_lossy().to_string())
        .unwrap_or_default();

    let mut qcid_string = String::new();
    if check_mode.is_support_quick_check() {
        if let Ok(qcid) = caculate_qcid_from_file(local_file_path).await {
            qcid_string = qcid.to_string();
        }
    }

    let use_chunk_list_now = use_chunklist && file_size > CHUNK_DEFAULT_SIZE;
    let mut chunk_ids = Vec::new();

    if file_size == 0 {
        let hasher = ChunkHasher::new(None)?;
        let hash_method = hasher.hash_method;
        let hash_result = hasher.calc_from_bytes(&[]);
        let chunk_id = ChunkId::from_mix_hash_result_by_hash_method(0, &hash_result, hash_method)?;
        chunk_ids.push(chunk_id.clone());

        let local_info = ChunkLocalInfo {
            path: local_file_path.to_string_lossy().to_string(),
            qcid: qcid_string.clone(),
            last_modify_time: file_last_modify_time,
            range: Some(0..0),
        };
        let content = ContentToStore::from_local_file(chunk_id.clone(), 0, local_info);
        store_content_to_ndn_mgr(ndn_mgr_id, content, store_mode.clone()).await?;

        let callback_result = call_ndn_callback(
            &progress_callback,
            local_file_path.to_string_lossy().to_string(),
            NdnAction::ChunkOK(chunk_id, 0),
        )
        .await?;
        if !callback_result.is_continue() {
            return Err(NdnError::InvalidState("break by user".to_string()));
        }
    } else {
        let mut file_reader = tokio::fs::File::open(local_file_path).await?;
        let mut read_pos = 0u64;
        let calc_chunk_size = if use_chunk_list_now {
            CHUNK_DEFAULT_SIZE
        } else {
            file_size
        };

        while read_pos < file_size {
            let chunk_hasher = ChunkHasher::new(None)?;
            let hash_method = chunk_hasher.hash_method;
            let (chunk_raw_id, chunk_size) = chunk_hasher
                .calc_from_reader_with_length(&mut file_reader, calc_chunk_size)
                .await?;

            if chunk_size == 0 {
                break;
            }

            let chunk_id =
                ChunkId::from_mix_hash_result_by_hash_method(chunk_size, &chunk_raw_id, hash_method)?;
            debug!(
                "cacl_file_object: calc chunk_id success, chunk_id={}, chunk_size={}",
                chunk_id.to_string(),
                chunk_size
            );

            let range = Some(read_pos..read_pos + chunk_size);
            let local_info = ChunkLocalInfo {
                path: local_file_path.to_string_lossy().to_string(),
                qcid: qcid_string.clone(),
                last_modify_time: file_last_modify_time,
                range: range.clone(),
            };
            let content = ContentToStore::from_local_file(chunk_id.clone(), chunk_size, local_info);
            store_content_to_ndn_mgr(ndn_mgr_id, content, store_mode.clone()).await?;

            let inner_path = if use_chunk_list_now {
                format!(
                    "{}/{}:{}",
                    local_file_path.to_string_lossy(),
                    read_pos,
                    read_pos + chunk_size
                )
            } else {
                local_file_path.to_string_lossy().to_string()
            };
            let callback_result =
                call_ndn_callback(&progress_callback, inner_path, NdnAction::ChunkOK(chunk_id.clone(), chunk_size))
                    .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }

            chunk_ids.push(chunk_id);
            read_pos += chunk_size;
        }
    }

    if use_chunk_list_now {
        let chunk_list = SimpleChunkList::from_chunk_list(chunk_ids)?;
        let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
        file_obj_result.content = chunk_list_id.to_string();

        let content = ContentToStore::from_obj(chunk_list_id, chunk_list_str);
        store_content_to_ndn_mgr(ndn_mgr_id, content, store_mode.clone()).await?;
    } else if let Some(chunk_id) = chunk_ids.first() {
        file_obj_result.content = chunk_id.to_string();
    }

    let (file_obj_id, file_obj_str) = file_obj_result.gen_obj_id();
    let content = ContentToStore::from_obj(file_obj_id.clone(), file_obj_str.clone());
    store_content_to_ndn_mgr(ndn_mgr_id, content, store_mode).await?;

    Ok((file_obj_result, file_obj_id, file_obj_str))
}

pub async fn cacl_dir_object(
    ndn_mgr_id: Option<&str>,
    source_dir: &Path,
    file_obj_template: &FileObject,
    check_mode: &CheckMode,
    store_mode: StoreMode,
    progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>,
) -> NdnResult<(DirObject, ObjId, String)> {
    let mut this_dir_obj = DirObject::new(
        source_dir
            .file_name()
            .map(|v| v.to_string_lossy().to_string()),
    );

    if let Ok(meta) = tokio::fs::metadata(source_dir).await {
        if let Ok(modified) = meta.modified() {
            if let Ok(dur) = modified.duration_since(UNIX_EPOCH) {
                let ts = dur.as_secs();
                this_dir_obj.content_obj.create_time = ts;
                this_dir_obj.content_obj.last_update_time = ts;
            }
        }
    }

    let mut read_dir = tokio::fs::read_dir(source_dir).await?;
    let mut sub_paths = Vec::new();
    while let Some(entry) = read_dir.next_entry().await? {
        sub_paths.push(entry.path());
    }
    sub_paths.sort();

    for sub_path in sub_paths {
        if sub_path.is_dir() {
            let callback_result = call_ndn_callback(
                &progress_callback,
                sub_path.to_string_lossy().to_string(),
                NdnAction::PreDir,
            )
            .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }
            if callback_result.is_skip() {
                continue;
            }

            let (sub_dir_obj, sub_dir_obj_id, _sub_dir_str) = Box::pin(cacl_dir_object(
                ndn_mgr_id,
                &sub_path,
                file_obj_template,
                check_mode,
                store_mode.clone(),
                progress_callback.clone(),
            ))
            .await?;

            let callback_result = call_ndn_callback(
                &progress_callback,
                sub_path.to_string_lossy().to_string(),
                NdnAction::DirOK(sub_dir_obj_id.clone(), sub_dir_obj.total_size),
            )
            .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }
            if callback_result.is_skip() {
                continue;
            }

            this_dir_obj.add_directory(
                sub_path
                    .file_name()
                    .map(|v| v.to_string_lossy().to_string())
                    .unwrap_or_default(),
                sub_dir_obj_id,
                sub_dir_obj.total_size,
            )?;
        } else if sub_path.is_file() {
            let callback_result = call_ndn_callback(
                &progress_callback,
                sub_path.to_string_lossy().to_string(),
                NdnAction::PreFile,
            )
            .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }
            if callback_result.is_skip() {
                continue;
            }

            let (file_object, file_object_id, _file_object_str) = cacl_file_object(
                ndn_mgr_id,
                &sub_path,
                file_obj_template,
                true,
                check_mode,
                store_mode.clone(),
                progress_callback.clone(),
            )
            .await?;
            let file_object_json = serde_json::to_value(&file_object)
                .map_err(|e| NdnError::InvalidData(format!("serialize FileObject failed: {}", e)))?;

            let callback_result = call_ndn_callback(
                &progress_callback,
                sub_path.to_string_lossy().to_string(),
                NdnAction::FileOK(file_object_id, file_object.size),
            )
            .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }
            if callback_result.is_skip() {
                continue;
            }

            this_dir_obj.add_file(
                sub_path
                    .file_name()
                    .map(|v| v.to_string_lossy().to_string())
                    .unwrap_or_default(),
                file_object_json,
                file_object.size,
            )?;
        }
    }

    let (dir_obj_id, dir_obj_str) = this_dir_obj.gen_obj_id()?;
    let dir_obj_store_str = serde_json::to_string(&this_dir_obj)
        .map_err(|e| NdnError::InvalidData(format!("serialize DirObject failed: {}", e)))?;
    let content = ContentToStore::from_obj(dir_obj_id.clone(), dir_obj_store_str);
    store_content_to_ndn_mgr(ndn_mgr_id, content, store_mode).await?;

    Ok((this_dir_obj, dir_obj_id, dir_obj_str))
}

pub async fn restore_file_object(
    _file_object: ObjId,
    _ndn_mgr_id: Option<&str>,
    _target_file: &Path,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "restore_file_object is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn restore_dir_object(
    _dir_object: ObjId,
    _ndn_mgr_id: Option<&str>,
    _target_dir: &Path,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "restore_dir_object is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn put_local_file_as_chunk(
    _mgr_id: Option<&str>,
    chunk_type: ChunkType,
    local_file_path: &PathBuf,
    _store_mode: StoreMode,
) -> NdnResult<ChunkId> {
    let mut file_reader = tokio::fs::File::open(local_file_path).await?;
    let hash_method = chunk_type.to_hash_method()?;
    let chunk_hasher = ChunkHasher::new_with_hash_method(hash_method)?;
    let (chunk_raw_id, chunk_size) = chunk_hasher.calc_from_reader(&mut file_reader).await?;

    let chunk_id = if chunk_type.is_mix() {
        ChunkId::from_mix_hash_result(chunk_size, &chunk_raw_id, chunk_type)
    } else {
        ChunkId::from_hash_result(&chunk_raw_id, chunk_type)
    };

    info!(
        "put_local_file_as_chunk: local_file={} => chunk_id={} size={}",
        local_file_path.display(),
        chunk_id.to_string(),
        chunk_size
    );
    Ok(chunk_id)
}

pub async fn pub_local_file_as_fileobj(
    mgr_id: Option<&str>,
    local_file_path: &PathBuf,
    _ndn_path: &str,
    fileobj_template: &mut FileObject,
    _user_id: &str,
    _app_id: &str,
) -> NdnResult<(FileObject, ObjId, String)> {
    cacl_file_object(
        mgr_id,
        local_file_path,
        fileobj_template,
        true,
        &CheckMode::ByFullHash,
        StoreMode::NoStore,
        None,
    )
    .await
}

pub async fn copy_file_from_ndn_mgr(
    _source_ndn_mgr_id: &String,
    _target_ndn_mgr_id: &String,
    _file_obj_id: &ObjId,
    _file_object: &FileObject,
    _pull_mode: StoreMode,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "copy_file_from_ndn_mgr is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn copy_dir_from_ndn_mgr(
    _source_ndn_mgr_id: &String,
    _target_ndn_mgr_id: &String,
    _dir_object_id: &ObjId,
    _pull_mode: StoreMode,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "copy_dir_from_ndn_mgr is unavailable after NDM refactor".to_string(),
    ))
}
