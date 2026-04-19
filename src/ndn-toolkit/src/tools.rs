use log::{debug, info};
use named_store::{ChunkLocalInfo, NamedStoreMgr};
use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex;

use ndn_lib::{
    caculate_qcid_from_file, load_named_object_from_obj_str, ChunkHasher, ChunkId, ChunkType,
    DirObject, FileObject, MsgObject, NamedObject, NdnAction, NdnError, NdnProgressCallback,
    NdnResult, ObjId, ProgressCallbackResult, RefRole, RefTarget, ChunkList, StoreMode,
    CHUNK_DEFAULT_SIZE,
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
    pub fn from_local_file(
        chunk_id: ChunkId,
        chunk_size: u64,
        chunk_local_info: ChunkLocalInfo,
    ) -> Self {
        ContentToStore::Chunk(chunk_id, chunk_size, chunk_local_info)
    }

    pub fn from_obj(obj_id: ObjId, obj_str: String) -> Self {
        ContentToStore::Object(obj_id, obj_str)
    }

    pub fn to_obj(self) -> NdnResult<(ObjId, String)> {
        match self {
            ContentToStore::Object(obj_id, obj_str) => Ok((obj_id, obj_str)),
            _ => Err(NdnError::InvalidParam(
                "Invalid content to store".to_string(),
            )),
        }
    }

    pub fn to_local_file(self) -> NdnResult<(ChunkId, u64, ChunkLocalInfo)> {
        match self {
            ContentToStore::Chunk(chunk_id, chunk_size, local_info) => {
                Ok((chunk_id, chunk_size, local_info))
            }
            _ => Err(NdnError::InvalidParam(
                "Invalid content to store".to_string(),
            )),
        }
    }
}

async fn read_chunk_bytes(local_info: &ChunkLocalInfo, chunk_size: u64) -> NdnResult<Vec<u8>> {
    let bytes = tokio::fs::read(&local_info.path).await?;
    let slice = if let Some(src_range) = local_info.range.clone() {
        let start = src_range.start as usize;
        let end = src_range.end as usize;
        if end < start || end > bytes.len() {
            return Err(NdnError::InvalidParam(format!(
                "invalid source range {}..{} for file {}",
                src_range.start, src_range.end, local_info.path
            )));
        }
        &bytes[start..end]
    } else {
        &bytes
    };

    if slice.len() as u64 != chunk_size {
        return Err(NdnError::InvalidParam(format!(
            "chunk size mismatch: expect {} actual {} for {}",
            chunk_size,
            slice.len(),
            local_info.path
        )));
    }

    Ok(slice.to_vec())
}

pub async fn store_content_to_ndn_mgr_impl(
    store_mgr: &NamedStoreMgr,
    content: ContentToStore,
    store_mode: StoreMode,
) -> NdnResult<()> {
    match store_mode {
        StoreMode::NoStore => Ok(()),
        StoreMode::StoreInNamedMgr => match content {
            ContentToStore::Object(obj_id, obj_str) => {
                store_mgr.put_object(&obj_id, &obj_str).await
            }
            ContentToStore::Chunk(chunk_id, chunk_size, local_info) => {
                let chunk_bytes = read_chunk_bytes(&local_info, chunk_size).await?;
                store_mgr.put_chunk(&chunk_id, &chunk_bytes).await
            }
        },
        StoreMode::LocalFile(_, _, _) => match content {
            ContentToStore::Object(obj_id, obj_str) => {
                store_mgr.put_object(&obj_id, &obj_str).await
            }
            ContentToStore::Chunk(chunk_id, chunk_size, local_info) => {
                store_mgr
                    .add_chunk_by_link_to_local_file(&chunk_id, chunk_size, &local_info)
                    .await
            }
        },
    }
}

pub async fn store_content_to_ndn_mgr(
    store_mgr: Option<&NamedStoreMgr>,
    content: ContentToStore,
    store_mode: StoreMode,
) -> NdnResult<()> {
    if matches!(store_mode, StoreMode::NoStore) {
        return Ok(());
    }

    let store_mgr = store_mgr.ok_or_else(|| {
        NdnError::NotFound(format!(
            "named store mgr is required for store mode: {:?}",
            store_mode
        ))
    })?;

    store_content_to_ndn_mgr_impl(store_mgr, content, store_mode).await
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
    store_mgr: Option<&NamedStoreMgr>,
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
    if qcid_string.is_empty() && matches!(store_mode, StoreMode::LocalFile(_, _, _)) {
        qcid_string = caculate_qcid_from_file(local_file_path).await?.to_string();
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
        store_content_to_ndn_mgr(store_mgr, content, store_mode.clone()).await?;

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

            let chunk_id = ChunkId::from_mix_hash_result_by_hash_method(
                chunk_size,
                &chunk_raw_id,
                hash_method,
            )?;
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
            store_content_to_ndn_mgr(store_mgr, content, store_mode.clone()).await?;

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
            let callback_result = call_ndn_callback(
                &progress_callback,
                inner_path,
                NdnAction::ChunkOK(chunk_id.clone(), chunk_size),
            )
            .await?;
            if !callback_result.is_continue() {
                return Err(NdnError::InvalidState("break by user".to_string()));
            }

            chunk_ids.push(chunk_id);
            read_pos += chunk_size;
        }
    }

    if use_chunk_list_now {
        let chunk_list = ChunkList::from_chunk_list(chunk_ids)?;
        let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
        file_obj_result.content = chunk_list_id.to_string();

        let content = ContentToStore::from_obj(chunk_list_id, chunk_list_str);
        store_content_to_ndn_mgr(store_mgr, content, store_mode.clone()).await?;
    } else if let Some(chunk_id) = chunk_ids.first() {
        file_obj_result.content = chunk_id.to_string();
    }

    let (file_obj_id, file_obj_str) = file_obj_result.gen_obj_id();
    let content = ContentToStore::from_obj(file_obj_id.clone(), file_obj_str.clone());
    store_content_to_ndn_mgr(store_mgr, content, store_mode).await?;

    Ok((file_obj_result, file_obj_id, file_obj_str))
}

pub async fn collect_missing_chunks_for_file_object(
    store_mgr: &NamedStoreMgr,
    file_obj: &FileObject,
) -> NdnResult<Vec<ChunkId>> {
    if file_obj.content.is_empty() {
        return Ok(Vec::new());
    }

    let content_obj_id = ObjId::new(file_obj.content.as_str())?;
    if content_obj_id.is_chunk() {
        let chunk_id = ChunkId::from_obj_id(&content_obj_id);
        if store_mgr.have_chunk(&chunk_id).await {
            return Ok(Vec::new());
        }

        return Ok(vec![chunk_id]);
    }

    if content_obj_id.is_chunk_list() {
        let chunklist_json = store_mgr.get_object(&content_obj_id).await?;
        let chunk_list = ChunkList::from_json(chunklist_json.as_str())?;
        let mut missing_chunks = Vec::new();

        for chunk_id in chunk_list.body.iter() {
            if !store_mgr.have_chunk(chunk_id).await {
                missing_chunks.push(chunk_id.clone());
            }
        }

        return Ok(missing_chunks);
    }

    Err(NdnError::InvalidObjType(format!(
        "file object content {} is not chunk or chunklist",
        content_obj_id
    )))
}

pub async fn check_file_object_content_ready(
    store_mgr: &NamedStoreMgr,
    file_obj: &FileObject,
) -> NdnResult<()> {
    if file_obj.content.is_empty() {
        return Err(NdnError::InvalidParam(format!(
            "file object {} has empty content",
            file_obj.name
        )));
    }

    let missing_chunks = collect_missing_chunks_for_file_object(store_mgr, file_obj).await?;
    if missing_chunks.is_empty() {
        return Ok(());
    }

    Err(NdnError::NotFound(format!(
        "file object {} missing {} chunk(s): {}",
        file_obj.name,
        missing_chunks.len(),
        missing_chunks
            .iter()
            .map(|chunk_id| chunk_id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )))
}

pub async fn cacl_dir_object(
    store_mgr: Option<&NamedStoreMgr>,
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
                store_mgr,
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
                store_mgr,
                &sub_path,
                file_obj_template,
                true,
                check_mode,
                store_mode.clone(),
                progress_callback.clone(),
            )
            .await?;
            let file_object_json = serde_json::to_value(&file_object).map_err(|e| {
                NdnError::InvalidData(format!("serialize FileObject failed: {}", e))
            })?;

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
    store_content_to_ndn_mgr(store_mgr, content, store_mode).await?;

    Ok((this_dir_obj, dir_obj_id, dir_obj_str))
}

pub async fn restore_file_object(
    _file_object: ObjId,
    _store_mgr: Option<&NamedStoreMgr>,
    _target_file: &Path,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "restore_file_object is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn restore_dir_object(
    _dir_object: ObjId,
    _store_mgr: Option<&NamedStoreMgr>,
    _target_dir: &Path,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "restore_dir_object is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn put_local_file_as_chunk(
    _store_mgr: Option<&NamedStoreMgr>,
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
    _store_mgr: Option<&NamedStoreMgr>,
    local_file_path: &PathBuf,
    _ndn_path: &str,
    fileobj_template: &mut FileObject,
    _user_id: &str,
    _app_id: &str,
) -> NdnResult<(FileObject, ObjId, String)> {
    cacl_file_object(
        None,
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
    _source_store_mgr: &NamedStoreMgr,
    _target_store_mgr: &NamedStoreMgr,
    _file_obj_id: &ObjId,
    _file_object: &FileObject,
    _pull_mode: StoreMode,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "copy_file_from_ndn_mgr is unavailable after NDM refactor".to_string(),
    ))
}

pub async fn copy_dir_from_ndn_mgr(
    _source_store_mgr: &NamedStoreMgr,
    _target_store_mgr: &NamedStoreMgr,
    _dir_object_id: &ObjId,
    _pull_mode: StoreMode,
) -> NdnResult<()> {
    Err(NdnError::Unsupported(
        "copy_dir_from_ndn_mgr is unavailable after NDM refactor".to_string(),
    ))
}

async fn ensure_chunk_list_chunks_ready(
    store_mgr: &NamedStoreMgr,
    owner_obj_id: &ObjId,
    chunk_list: &ChunkList,
) -> NdnResult<Vec<ChunkId>> {
    let mut missing_chunks = Vec::new();
    for chunk_id in chunk_list.body.iter() {
        if !store_mgr.have_chunk(chunk_id).await {
            missing_chunks.push(chunk_id.clone());
        }
    }

    if !missing_chunks.is_empty() {
        return Err(NdnError::NotFound(format!(
            "object {} missing {} chunk(s): {}",
            owner_obj_id,
            missing_chunks.len(),
            missing_chunks
                .iter()
                .map(|chunk_id| chunk_id.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )));
    }

    Ok(chunk_list.body.clone())
}

async fn load_stored_object(
    store_mgr: &NamedStoreMgr,
    obj_id: &ObjId,
    owner_obj_id: &ObjId,
) -> NdnResult<String> {
    store_mgr.get_object(obj_id).await.map_err(|err| match err {
        NdnError::NotFound(_) => NdnError::NotFound(format!(
            "object {} referenced by {} not found",
            obj_id, owner_obj_id
        )),
        NdnError::DbError(info) if info.contains("no rows") => NdnError::NotFound(format!(
            "object {} referenced by {} not found",
            obj_id, owner_obj_id
        )),
        other => other,
    })
}

fn parse_known_object_json(obj_str: &str) -> NdnResult<Value> {
    match serde_json::from_str::<Value>(obj_str) {
        Ok(value) => Ok(value),
        Err(_) => load_named_object_from_obj_str(obj_str),
    }
}

#[async_recursion::async_recursion]
async fn get_chunklist_from_known_named_object_impl(
    store_mgr: &NamedStoreMgr,
    obj_id: &ObjId,
    obj_json: &Value,
    visited_obj_ids: &mut HashSet<ObjId>,
) -> NdnResult<Vec<ChunkId>> {
    if !obj_id.is_chunk() && !visited_obj_ids.insert(obj_id.clone()) {
        return Ok(Vec::new());
    }

    //dir object直接失败
    //FileObject解析content,如果chunklist在namedsotremgr中不存在，则失败,成功把chunklist中的所有chunkid返回
    //其它known object类推
    if obj_id.is_dir_object() {
        return Err(NdnError::InvalidObjType(format!(
            "dir object {} does not have direct chunk list",
            obj_id
        )));
    }

    if obj_id.is_file_object() {
        let file_obj: FileObject = serde_json::from_value(obj_json.clone()).map_err(|e| {
            NdnError::InvalidData(format!("parse file object from json failed: {}", e))
        })?;
        if file_obj.content.is_empty() {
            return Ok(Vec::new());
        }

        let content_obj_id = ObjId::new(file_obj.content.as_str())?;
        if content_obj_id.is_chunk() {
            let chunk_id = ChunkId::from_obj_id(&content_obj_id);
            if !store_mgr.have_chunk(&chunk_id).await {
                return Err(NdnError::NotFound(format!(
                    "chunk {} referenced by object {} not found",
                    chunk_id.to_string(),
                    obj_id
                )));
            }
            return Ok(vec![chunk_id]);
        }

        if content_obj_id.is_chunk_list() {
            let chunklist_json = load_stored_object(store_mgr, &content_obj_id, obj_id).await?;
            let chunklist_obj_json = parse_known_object_json(chunklist_json.as_str())?;
            return get_chunklist_from_known_named_object_impl(
                store_mgr,
                &content_obj_id,
                &chunklist_obj_json,
                visited_obj_ids,
            )
            .await;
        }

        return Err(NdnError::InvalidObjType(format!(
            "file object content {} is not chunk or chunklist",
            content_obj_id
        )));
    }

    if obj_id.is_chunk_list() {
        let chunk_list = ChunkList::from_json_value(obj_json.clone())?;
        return ensure_chunk_list_chunks_ready(store_mgr, obj_id, &chunk_list).await;
    }

    if obj_id.obj_type == MsgObject::get_obj_type() {
        let msg_obj: MsgObject = serde_json::from_value(obj_json.clone()).map_err(|e| {
            NdnError::InvalidData(format!("parse msg object from json failed: {}", e))
        })?;

        let mut chunk_ids = Vec::new();
        for ref_item in msg_obj.content.refs.iter() {
            if ref_item.role != RefRole::Output {
                continue;
            }

            let RefTarget::DataObj {
                obj_id: ref_obj_id, ..
            } = &ref_item.target
            else {
                continue;
            };

            if ref_obj_id.is_chunk() {
                let chunk_id = ChunkId::from_obj_id(ref_obj_id);
                if !store_mgr.have_chunk(&chunk_id).await {
                    return Err(NdnError::NotFound(format!(
                        "chunk {} referenced by object {} not found",
                        chunk_id.to_string(),
                        obj_id
                    )));
                }
                chunk_ids.push(chunk_id);
                continue;
            }

            let ref_obj_str = load_stored_object(store_mgr, ref_obj_id, obj_id).await?;
            let ref_obj_json = parse_known_object_json(ref_obj_str.as_str())?;
            chunk_ids.extend(
                get_chunklist_from_known_named_object_impl(
                    store_mgr,
                    ref_obj_id,
                    &ref_obj_json,
                    visited_obj_ids,
                )
                .await?,
            );
        }

        return Ok(chunk_ids);
    }

    Err(NdnError::InvalidObjType(format!(
        "object {} is not a supported known named object",
        obj_id
    )))
}

pub async fn get_chunklist_from_known_named_object(
    store_mgr: &NamedStoreMgr,
    obj_id: &ObjId,
    obj_json: &Value,
) -> NdnResult<Vec<ChunkId>> {
    let mut visited_obj_ids = HashSet::new();
    get_chunklist_from_known_named_object_impl(store_mgr, obj_id, obj_json, &mut visited_obj_ids)
        .await
}
