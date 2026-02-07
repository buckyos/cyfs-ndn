use crate::{CHUNK_NORMAL_SIZE, ChunkHasher, ChunkLocalInfo, ChunkState, ChunkType, DirObject, FileObject, NamedDataMgr, NamedObject, NdnAction, NdnError, NdnProgressCallback, NdnResult, OBJ_TYPE_CHUNK_LIST_SIMPLE, OBJ_TYPE_DIR, OBJ_TYPE_FILE, ObjId, PackedObjItem, ProgressCallbackResult, SimpleChunkList, StoreMode};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::UNIX_EPOCH;

use buckyos_kit::buckyos_get_unix_timestamp;
use tokio::fs;

use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncReadExt;
use crate::chunk::ChunkId;
use crate::packed_obj_pipline::{PackedObjPiplineWriter, PackedObjPiplineReader, ChunkChannelSourceWriter};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::chunk::caculate_qcid_from_file;


#[derive(PartialEq)]
pub enum CheckMode {
    ByQCID,
    ByFullHash
}

impl CheckMode {
    pub fn is_support_quick_check(&self) -> bool {
        return self == &CheckMode::ByQCID;
    }
}


pub enum ContentToStore {
    Chunk(ChunkId,u64,ChunkLocalInfo),
    Object(ObjId,String),
}

impl ContentToStore {
    pub fn from_local_file(chunk_id:ChunkId,chunk_size:u64,chunk_local_info:ChunkLocalInfo) -> Self{
        return ContentToStore::Chunk(chunk_id,chunk_size,chunk_local_info);
    }

    pub fn from_obj(obj_id:ObjId,obj_str:String) -> Self {
        return ContentToStore::Object(obj_id,obj_str);
    }


    pub fn to_obj(self) -> NdnResult<(ObjId,String)> {
        match self {
            ContentToStore::Object(obj_id,obj_str) => Ok((obj_id,obj_str)),
            _ => Err(NdnError::InvalidParam(format!("Invalid content to store"))),
        }
    }

    pub fn to_local_file(self) -> NdnResult<(ChunkId,u64,ChunkLocalInfo)> {
        match self {
            ContentToStore::Chunk(chunk_id,chunk_size,local_info) => Ok((chunk_id,chunk_size,local_info)),
            _ => Err(NdnError::InvalidParam(format!("Invalid content to store"))),
        }
    }
}
 
pub async fn store_content_to_ndn_mgr_impl(ndn_mgr:&NamedDataMgr,
    content:ContentToStore,store_mode:StoreMode) -> NdnResult<()> {
    let mut need_store_in_named_mgr = false;

    match store_mode {
        StoreMode::NoStore => {
            return Ok(());
        },
        StoreMode::LocalFile(_,_,is_store_in_named_mgr) => {
             match content {
                ContentToStore::Chunk(chunk_id,chunk_size,chunk_local_info) => {
                    return ndn_mgr.add_chunk_by_link_to_local_file_impl(&chunk_id, chunk_size, &chunk_local_info).await;
                },
                ContentToStore::Object(obj_id,obj_str) => {
                    return ndn_mgr.put_object_impl(&obj_id,&obj_str).await;
                }
             }
        },
        StoreMode::StoreInNamedMgr => {
            match content {
                ContentToStore::Chunk(chunk_id,chunk_size,chunk_local_info) => {
                    if ndn_mgr.have_chunk_impl(&chunk_id).await {
                        return Ok(());
                    }

                    let mut reader = tokio::fs::File::open(chunk_local_info.path).await.map_err(|e| {
                        NdnError::IoError(format!("Failed to open file: {}", e))
                    })?;
                    //limit the reader from chunk_local_info.range
                    let mut reader: Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>> = if let Some(range) = chunk_local_info.range {
                        let start = range.start;
                        let len = range.end - range.start;
                        // position to start, then wrap with take to cap reads to len bytes
                        reader.seek(SeekFrom::Start(start)).await?;
                        Box::pin(reader.take(len))
                    } else {
                        Box::pin(reader)
                    };
                    return ndn_mgr.put_chunk_by_reader_impl(&chunk_id, chunk_size, &mut reader).await;
                },
                ContentToStore::Object(obj_id,obj_str) => {
                    return ndn_mgr.put_object_impl(&obj_id,&obj_str).await;
                }
            }
        }
    }
    Ok(())
}

pub async fn store_content_to_ndn_mgr(ndn_mgr_id:Option<&str>,
    content:ContentToStore,store_mode:StoreMode) -> NdnResult<()> {
    let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(ndn_mgr_id).await;
    if named_mgr.is_none() {
        return Err(NdnError::NotFound(format!("named data mgr not found")));
    }
    let named_mgr = named_mgr.unwrap();
    let real_named_mgr = named_mgr.lock().await;
    
    store_content_to_ndn_mgr_impl(&real_named_mgr,content,store_mode).await
}

//use Link Mode to cacl dir object
pub async fn cacl_file_object(ndn_mgr_id:Option<&str>,
    local_file_path:&Path,fileobj_template:&FileObject,use_chunklist:bool,
    check_mode:&CheckMode,store_mode:StoreMode,mut progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>) -> NdnResult<(FileObject,ObjId,String)> {
    let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(ndn_mgr_id).await;
    if named_mgr.is_none() {
        return Err(NdnError::NotFound(format!("named data mgr not found")));
    }
    let named_mgr = named_mgr.unwrap();

    debug!("cacl_file_object: {:?}", local_file_path);

    let mut file_obj_result = fileobj_template.clone();
    let file_meta = tokio::fs::metadata(local_file_path).await.unwrap();
    let file_size = file_meta.len();
    let file_last_modify_time = file_meta.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    let mut is_use_chunklist = false;
    let mut chunk_size = CHUNK_NORMAL_SIZE as u64;
    let mut chunk_list:SimpleChunkList = SimpleChunkList::new();
    let mut chunk_id:ChunkId;
    let mut new_qcid:Option<ChunkId> = None;
    let mut qcid_string = "".to_string();

    if file_size > CHUNK_NORMAL_SIZE as u64 {
        if use_chunklist {
            is_use_chunklist = true;
        } else {
            chunk_size = file_size;
        }
    }

    file_obj_result.size = file_size;
    file_obj_result.content_obj.create_time = file_last_modify_time;
    file_obj_result.content_obj.last_update_time = file_last_modify_time;
    file_obj_result.content_obj.name = local_file_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    //
    if check_mode.is_support_quick_check() {    
        let qcid = caculate_qcid_from_file(local_file_path).await;
        if qcid.is_ok() {
            let qcid = qcid.unwrap();
            let qcid_obj_id = qcid.to_obj_id();
            let real_named_mgr = named_mgr.lock().await;
            let source_obj = real_named_mgr.query_source_object_by_target(&qcid_obj_id).await?;
            drop(real_named_mgr);
            if source_obj.is_some() {
                let source_obj = source_obj.unwrap();
                info!("qcid already exists! file {} : {}=>{}", local_file_path.display(), source_obj.to_string(), qcid_obj_id.to_string());
                //store_content_to_ndn_mgr(&real_named_mgr,&source_obj,store_mode.clone()).await?;
                file_obj_result.content = source_obj.to_string();
                if progress_callback.is_some() {
                    if source_obj.obj_type.as_str() == OBJ_TYPE_CHUNK_LIST_SIMPLE {
                        let chunk_list_body = NamedDataMgr::get_object(ndn_mgr_id,&source_obj,None).await?;
                        let chunk_list = SimpleChunkList::from_json_value(chunk_list_body)?;
                        let mut start_pos = 0;
                        for chunk_id in chunk_list.body.iter() {
                            let chunk_size = chunk_id.get_length().unwrap();
                            let inner_path = format!("{}/{}:{}",local_file_path.to_path_buf().to_string_lossy().to_string(),start_pos,start_pos + chunk_size);
                            start_pos += chunk_size;
                            let callback_result = call_ndn_callback(&progress_callback,
                                inner_path,
                                NdnAction::ChunkOK(chunk_id.clone(),chunk_size)).await?;
                            if !callback_result.is_continue() { 
                                return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
                            }
                        }
                    } else {
                        let callback_result = call_ndn_callback(&progress_callback,
                            local_file_path.to_path_buf().to_string_lossy().to_string(),
                            NdnAction::ChunkOK(ChunkId::from_obj_id(&source_obj),file_size)).await?;
                        if !callback_result.is_continue() { 
                            return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
                        }
                    }
                }
                let (file_obj_id, file_obj_str) = file_obj_result.gen_obj_id();
                let content = ContentToStore::from_obj(file_obj_id.clone(),file_obj_str.clone());
                store_content_to_ndn_mgr(ndn_mgr_id,content,store_mode).await?;
                return Ok((file_obj_result,file_obj_id,file_obj_str));
            } else {
                qcid_string = qcid.to_string();
                new_qcid = Some(qcid);
            }
        }
    }

    let mut file_reader = tokio::fs::File::open(local_file_path).await.map_err(|e| {
        error!("open local_file_path failed, err:{}", e);
        NdnError::IoError(format!("open local_file_path failed, err:{}", e))
    })?;
    
    let mut read_pos = 0;
    let mut chunk_index = 0;
    while read_pos < file_size {
        let mut chunk_hasher = ChunkHasher::new(None).unwrap();
        let chunk_type = chunk_hasher.hash_method.clone();
        //file_reader.seek(SeekFrom::Start(read_pos)).await;
        let (chunk_raw_id, chunk_size) = chunk_hasher
            .calc_from_reader_with_length(&mut file_reader, chunk_size)
            .await
            .unwrap();

        chunk_id = ChunkId::from_mix_hash_result_by_hash_method(chunk_size, &chunk_raw_id, chunk_type)?;
        debug!("cacl_file_object:calc chunk_id success,chunk_id:{},chunk_size:{}",chunk_id.to_string(),chunk_size);
        {
            let mut range = None;
            if is_use_chunklist {
                chunk_list.append_chunk(chunk_id.clone())?;
                range = Some(read_pos..read_pos + chunk_size);
                chunk_index += 1;
            } else {
                file_obj_result.content = chunk_id.to_string();
            }
            //store chunk to ndn mgr
            let chunk_local_info = ChunkLocalInfo {
                path: local_file_path.to_path_buf().to_string_lossy().to_string(),
                qcid: qcid_string.clone(),
                last_modify_time: file_last_modify_time,
                range,
            };
            let content = ContentToStore::from_local_file(chunk_id.clone(),chunk_size,chunk_local_info);
            let inner_path = if is_use_chunklist {
                format!("{}/{}:{}",local_file_path.to_path_buf().to_string_lossy().to_string(),read_pos,read_pos + chunk_size)
            } else {
                local_file_path.to_path_buf().to_string_lossy().to_string()
            };
            let callback_result = call_ndn_callback(&progress_callback,inner_path,NdnAction::ChunkOK(chunk_id.clone(),chunk_size)).await?;
            if !callback_result.is_continue() { 
                return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
            }
            store_content_to_ndn_mgr(ndn_mgr_id,content,store_mode.clone()).await?;
        }    
        read_pos += chunk_size;
    }

    if is_use_chunklist {
        //gen chunk list id
        let sub_item_count = chunk_list.body.len();
        let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
        file_obj_result.content = chunk_list_id.to_string();

        let content = ContentToStore::from_obj(chunk_list_id,chunk_list_str);
        store_content_to_ndn_mgr(ndn_mgr_id,content,store_mode.clone()).await?;
    } 

    if new_qcid.is_some() {
        let real_named_mgr = named_mgr.lock().await;
        let content_obj_id = ObjId::new(file_obj_result.content.as_str())?;
        let new_qcid_obj_id = new_qcid.unwrap().to_obj_id();
        real_named_mgr.link_same_object(&content_obj_id, &new_qcid_obj_id).await?;
        info!("cacl {} file: link qcid to content: {}=>{}", local_file_path.display(), content_obj_id.to_string(), new_qcid_obj_id.to_string());
    }

    let (file_obj_id, file_obj_str) = file_obj_result.gen_obj_id();
    let content = ContentToStore::from_obj(file_obj_id.clone(),file_obj_str.clone());
    store_content_to_ndn_mgr(ndn_mgr_id,content,store_mode).await?;
    Ok((file_obj_result,file_obj_id,file_obj_str))
}


async fn call_ndn_callback(progress_callback: &Option<Arc<Mutex<NdnProgressCallback>>>,inner_path:String,action:NdnAction) -> NdnResult<ProgressCallbackResult> {
    if let Some(callback) = progress_callback {
        let mut callback = callback.lock().await;
        return callback(inner_path,action).await;
    }
    Ok(ProgressCallbackResult::Continue)
} 

//use Link Mode to cacl dir object
pub async fn cacl_dir_object(ndn_mgr_id:Option<&str>,
    source_dir:&Path,file_obj_template:&FileObject,
    check_mode:&CheckMode,store_mode:StoreMode,progress_callback: Option<Arc<Mutex<NdnProgressCallback>>>) -> NdnResult<(DirObject,ObjId,String)> {
    //遍历source_dir下的所有文件和子目录
    let mut read_dir = fs::read_dir(source_dir).await?;
    let mut will_process_file = Vec::new();
    let mut this_dir_obj = DirObject::new(None);

    // Fill BaseContentObject fields for determinism (otherwise BaseContentObject::default() uses "now",
    // which makes DirObject id/str unstable across runs).
    if let Ok(meta) = tokio::fs::metadata(source_dir).await {
        if let Ok(modified) = meta.modified() {
            if let Ok(dur) = modified.duration_since(UNIX_EPOCH) {
                let ts = dur.as_secs();
                this_dir_obj.content_obj.create_time = ts;
                this_dir_obj.content_obj.last_update_time = ts;
            }
        }
    }
    if let Some(name) = source_dir.file_name() {
        this_dir_obj.content_obj.name = name.to_string_lossy().to_string();
    }
    while let Some(entry) = read_dir.next_entry().await? {
        let sub_path = entry.path();
        if sub_path.is_dir() {
            //println!("dir: {}", sub_path.display());
            let callback_result = call_ndn_callback(&progress_callback,sub_path.to_string_lossy().to_string(),NdnAction::PreDir).await?;
            if !callback_result.is_continue() { 
                return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
            }

            if callback_result.is_skip() {
                debug!("user skip sub dir: {}", sub_path.display());
                continue;
            }

            let (sub_dir_obj,sub_dir_obj_id,sub_dir_str) = Box::pin(cacl_dir_object(ndn_mgr_id,&sub_path,file_obj_template,check_mode,store_mode.clone(),progress_callback.clone())).await?;
            let sub_total_size = sub_dir_obj.total_size;
            let callback_result = call_ndn_callback(&progress_callback,sub_path.to_string_lossy().to_string(),NdnAction::DirOK(sub_dir_obj_id.clone(),sub_total_size)).await?;
            
            if !callback_result.is_continue() { 
                return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
            }
            if callback_result.is_skip() {
                debug!("user skip sub dir: {}", sub_path.display());
                continue;
            }
            let sub_dir_obj_str = serde_json::to_string(&sub_dir_obj).unwrap();
            //let (sub_dir_obj_id, _) = sub_dir_obj.gen_obj_id()?;
            this_dir_obj.add_directory(sub_path.file_name().unwrap().to_string_lossy().to_string(), 
                sub_dir_obj_id, sub_total_size);
        } else {
            //println!("file: {}", sub_path.display());
            will_process_file.push(sub_path);
        }
    }
    
    for file in will_process_file {
        //println!("file: {}", file.display());
        let callback_result = call_ndn_callback(&progress_callback,file.to_string_lossy().to_string(),NdnAction::PreFile).await?;
        if !callback_result.is_continue() {
            break;
        }
        if callback_result.is_skip() {
            debug!("user skip file: {}", file.display());
            continue;
        }
        let (file_object,file_object_id,file_object_str) = cacl_file_object(ndn_mgr_id,&file,file_obj_template,true,check_mode,store_mode.clone(),progress_callback.clone()).await?;
        let file_object_json = serde_json::to_value(&file_object).unwrap();
        
        let callback_result = call_ndn_callback(&progress_callback,file.to_string_lossy().to_string(),NdnAction::FileOK(file_object_id.clone(),file_object.size)).await?;
        if !callback_result.is_continue() {
            return Err(NdnError::InvalidState(format!("break by user, callback result is not continue")));
        }
        if callback_result.is_skip() {
            debug!("user skip file: {}", file.display());
            continue;
        }
        this_dir_obj.add_file(file.file_name().unwrap().to_string_lossy().to_string(), file_object_json, file_object.size);
    }

    let (dir_obj_id, dir_obj_str) = this_dir_obj.gen_obj_id()?;
    let dir_obj_store_str = serde_json::to_string(&this_dir_obj).unwrap();
    //store dir object to ndn mgr
    let content = ContentToStore::from_obj(dir_obj_id.clone(),dir_obj_store_str);
    store_content_to_ndn_mgr(ndn_mgr_id,content,store_mode).await?;
    return Ok((this_dir_obj,dir_obj_id,dir_obj_str));
}



pub async fn restore_file_object(file_object:ObjId,ndn_mgr_id:Option<&str>,target_file:&Path) -> NdnResult<()> {
    let file_obj_json = NamedDataMgr::get_object(ndn_mgr_id,&file_object,None).await?;
    let file_obj:FileObject = serde_json::from_value(file_obj_json).map_err(|e| NdnError::Internal(format!("Failed to parse FileObject: {}", e)))?;
    let content_id = ObjId::new(file_obj.content.as_str())?;

    if target_file.exists() {
        warn!("restore_file_object: file already exists, remove it: {}", target_file.display());
        std::fs::remove_file(target_file).map_err(|e| NdnError::IoError(format!("Failed to remove file: {}", e)))?;
    } 
    let mut file_writer = tokio::fs::File::create(target_file).await?;

    let mut start_pos = 0;
    if content_id.is_chunk() {
        let chunk_id = ChunkId::new(file_obj.content.as_str())?;
        let (mut chunk_reader,chunk_size) = NamedDataMgr::open_chunk_reader(ndn_mgr_id,&chunk_id,0,false).await?;
        start_pos = chunk_size;
        tokio::io::copy(&mut chunk_reader,&mut file_writer).await?;
    } else if content_id.is_chunk_list() {
        let chunk_list_json = NamedDataMgr::get_object(ndn_mgr_id,&content_id,None).await?;
        let chunk_list:Vec<ChunkId> = serde_json::from_value(chunk_list_json).map_err(|e| NdnError::Internal(format!("Failed to parse SimpleChunkList: {}", e)))?;
        for chunk_id in chunk_list.iter() {
            let (mut chunk_reader,chunk_size) = NamedDataMgr::open_chunk_reader(ndn_mgr_id,&chunk_id,0,false).await?;
            start_pos += chunk_size;
            tokio::io::copy(&mut chunk_reader,&mut file_writer).await?;
        }
    }
    info!("restore_file_object: restore file {} success, file_size:{}", target_file.display(), start_pos);
    Ok(())
}

pub async fn restore_dir_object(dir_object:ObjId,ndn_mgr_id:Option<&str>,target_dir:&Path) -> NdnResult<()> {
    let dir_obj_json = NamedDataMgr::get_object(ndn_mgr_id,&dir_object,None).await?;
    let dir_obj:DirObject = serde_json::from_value(dir_obj_json).map_err(|e| NdnError::Internal(format!("Failed to parse DirObject: {}", e)))?;
    
    if !target_dir.exists() {
        info!("restore_dir_object: target directory not exists, create it: {}", target_dir.display());
        std::fs::create_dir_all(target_dir).map_err(|e| NdnError::IoError(format!("Failed to create directory: {}", e)))?;
    }

    for (sub_name,sub_item) in dir_obj.iter() {
        let sub_item_type = sub_item.get_obj_type();
        match sub_item_type.as_str() {
            OBJ_TYPE_DIR => {
                let (sub_item_obj_id,_) = sub_item.get_obj_id()?;
                let sub_dir_path = target_dir.join(sub_name);
                Box::pin(restore_dir_object(sub_item_obj_id,ndn_mgr_id,&sub_dir_path)).await?;
            },
            OBJ_TYPE_FILE => {
                let (sub_item_obj_id,_) = sub_item.get_obj_id()?;
                let sub_file_path = target_dir.join(sub_name);
                restore_file_object(sub_item_obj_id,ndn_mgr_id,&sub_file_path).await?;
            },
            _ => {
                warn!("restore_dir_object: unknown sub item type:{}",sub_item_type);
                continue;
            }
        }
    }
    info!("restore_dir_object: restore dir {} success", target_dir.display());
    Ok(())
}


pub async fn put_local_file_as_chunk(
    mgr_id: Option<&str>,
    chunk_type: ChunkType,
    local_file_path: &PathBuf,
    store_mode: StoreMode,
) -> NdnResult<ChunkId> {
    let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
    if named_mgr.is_none() {
        return Err(NdnError::NotFound(format!("named data mgr not found")));
    }
    let named_mgr = named_mgr.unwrap();
    //TODO：优化，边算边传，支持断点续传
    debug!(
        "start pub pub_local_file_as_chunk, local_file_path:{}",
        local_file_path.display()
    );
    let mut file_reader = tokio::fs::File::open(local_file_path).await.map_err(|e| {
        error!("open local_file_path failed, err:{}", e);
        NdnError::IoError(format!("open local_file_path failed, err:{}", e))
    })?;
    debug!("open local_file_path success");
    //TODO: support other hash type
    let mut chunk_hasher = ChunkHasher::new(None).unwrap();
    let chunk_type = chunk_hasher.hash_method.clone();

    file_reader.seek(SeekFrom::Start(0)).await;
    let (chunk_raw_id, chunk_size) = chunk_hasher
        .calc_from_reader(&mut file_reader)
        .await
        .unwrap();

    let chunk_id = ChunkId::from_mix_hash_result_by_hash_method(chunk_size, &chunk_raw_id, chunk_type)?;
    info!(
        "pub_local_file_as_fileobj:calc chunk_id success,chunk_id:{},chunk_size:{}",
        chunk_id.to_string(),
        chunk_size
    );

    let is_exist = NamedDataMgr::have_chunk(mgr_id,&chunk_id).await;
    if !is_exist {
        if store_mode.is_store_to_local() {
            let last_modify_time = tokio::fs::metadata(local_file_path).await.unwrap().modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let mut qcid_string = "".to_string();
            let qcid = caculate_qcid_from_file(local_file_path).await;
            if qcid.is_ok() {
                qcid_string = qcid.unwrap().to_string();
            }
            let real_named_mgr = named_mgr.lock().await;
            let local_info = ChunkLocalInfo {
                path: local_file_path.to_path_buf().to_string_lossy().to_string(),
                qcid: qcid_string.clone(),
                last_modify_time: last_modify_time,
                range: None,
            };
            real_named_mgr.add_chunk_by_link_to_local_file_impl(&chunk_id, chunk_size, &local_info).await?;
            info!("pub_local_file_as_chunk: chunk_id:{} link to {:?} success,qcid:{}", chunk_id.to_string(),local_file_path, qcid_string);
        } else {
            let real_named_mgr = named_mgr.lock().await;
            let (mut chunk_writer, _) = real_named_mgr
                .open_chunk_writer_impl(&chunk_id, chunk_size, 0)
                .await?;
            drop(real_named_mgr);

            file_reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let copy_bytes = tokio::io::copy(&mut file_reader, &mut chunk_writer)
                .await
                .map_err(|e| {
                    error!(
                        "copy local_file {:?} to named-mgr failed, err:{}",
                        local_file_path, e
                    );
                    NdnError::IoError(format!("copy local_file to named-mgr failed, err:{}", e))
                })?;

            info!("pub_local_file_as_fileobj:copy local_file {:?} to named-mgr's chunk success,copy_bytes:{}", local_file_path, copy_bytes);
                
            NamedDataMgr::complete_chunk_writer(mgr_id,&chunk_id).await?;
        }
    }
    Ok(chunk_id)
}

//这个函数和cacl_file_object类似，就是最后要发布到路径上
pub async fn pub_local_file_as_fileobj(
    mgr_id: Option<&str>,
    local_file_path: &PathBuf,
    ndn_path: &str,
    fileobj_template: &mut FileObject,
    user_id: &str,
    app_id: &str
) -> NdnResult<(FileObject,ObjId,String)> {
    let named_mgr = NamedDataMgr::get_named_data_mgr_by_id(mgr_id).await;
    if named_mgr.is_none() {
        return Err(NdnError::NotFound(format!("named data mgr not found")));
    }
    let named_mgr = named_mgr.unwrap();
    let (file_object,file_object_id,file_object_str) = cacl_file_object(mgr_id,local_file_path,fileobj_template,true,&CheckMode::ByFullHash,StoreMode::StoreInNamedMgr,None).await?;
    let real_named_mgr = named_mgr.lock().await;
    real_named_mgr.create_file_impl(ndn_path, &file_object_id, app_id, user_id).await?;
    return Ok((file_object,file_object_id,file_object_str));
}




// pub async fn restore_dir_object_from_source(root_object:FileObject,
//     source_pipline:Arc<Mutex<dyn PackedObjPiplineReader>>,
//     source_chunk_channel:Arc<Mutex<dyn ChunkChannelSourceWriter>>,
//     local_ndn_mgr:&NamedDataMgr,target_dir:&Path) -> NdnResult<()> {
//     //get dir object by root_object_id
//     //if local_ndn_mgr.is_exist: jump a big group
//     //get object from source_pipline
//     //get chunk from channel + other source(optional)
//     //
    
//     unimplemented!()

// }

pub async fn copy_file_from_ndn_mgr(source_ndn_mgr_id:&String,target_ndn_mgr_id:&String,
    file_obj_id:&ObjId,file_object:&FileObject,pull_mode:StoreMode) -> NdnResult<()> {
    let content_id = ObjId::new(file_object.content.as_str())?;
    
    match pull_mode {
        StoreMode::NoStore => {
            unimplemented!()
        },
        StoreMode::LocalFile(local_file_path,range,is_store_in_named_mgr) => {
            unimplemented!()
        },
        StoreMode::StoreInNamedMgr => {
            if content_id.is_chunk() {
                let chunk_id = ChunkId::from_obj_id(&content_id);
                if !NamedDataMgr::have_chunk(Some(source_ndn_mgr_id.as_str()),&chunk_id).await {
                    info!("chunk {} not exist, copy from source ndn mgr", chunk_id.to_string());
                    let (mut chunk_reader,chunk_size) = NamedDataMgr::open_chunk_reader(Some(source_ndn_mgr_id.as_str()),&chunk_id,0,false).await?;
                    let (mut chunk_writer,_) = NamedDataMgr::open_chunk_writer(Some(target_ndn_mgr_id.as_str()),&chunk_id,chunk_size,0).await?;
                    tokio::io::copy(&mut chunk_reader,&mut chunk_writer).await?;
                    NamedDataMgr::complete_chunk_writer(Some(target_ndn_mgr_id.as_str()),&chunk_id).await?;
                } 
            } else if content_id.is_chunk_list() {
                let chunk_list_json = NamedDataMgr::get_object(Some(source_ndn_mgr_id.as_str()),&content_id,None).await?;
                let chunk_list:Vec<ChunkId> = serde_json::from_value(chunk_list_json).map_err(|e| NdnError::Internal(format!("Failed to parse SimpleChunkList: {}", e)))?;
                for chunk_id in chunk_list.iter() {
                    if !NamedDataMgr::have_chunk(Some(source_ndn_mgr_id.as_str()),&chunk_id).await {
                        info!("chunk {} not exist, copy from source ndn mgr", chunk_id.to_string());
                        let (mut chunk_reader,chunk_size) = NamedDataMgr::open_chunk_reader(Some(source_ndn_mgr_id.as_str()),&chunk_id,0,false).await?;
                        let (mut chunk_writer,_) = NamedDataMgr::open_chunk_writer(Some(target_ndn_mgr_id.as_str()),&chunk_id,chunk_size,0).await?;
                        tokio::io::copy(&mut chunk_reader,&mut chunk_writer).await?;
                        NamedDataMgr::complete_chunk_writer(Some(target_ndn_mgr_id.as_str()),&chunk_id).await?;
                    } 
                }
                let json_str = serde_json::to_string(&chunk_list).unwrap();
                NamedDataMgr::put_object(Some(target_ndn_mgr_id.as_str()),&content_id,&json_str).await?;

            } else {
                return Err(NdnError::Internal(format!("Unsupported content obj type: {}", content_id.to_string())));
            }

            let file_object_str = serde_json::to_string(file_object).unwrap();
            NamedDataMgr::put_object(Some(target_ndn_mgr_id.as_str()),file_obj_id,&file_object_str).await?;
            info!("copy file_obj {} from {} to {} success", file_obj_id.to_string(),source_ndn_mgr_id.as_str(),target_ndn_mgr_id.as_str());
        }
    }
    Ok(())
}

pub async fn copy_dir_from_ndn_mgr(source_ndn_mgr_id:&String,target_ndn_mgr_id:&String,
    dir_object_id:&ObjId,pull_mode:StoreMode) -> NdnResult<()> {
    let dir_object = NamedDataMgr::get_object(Some(source_ndn_mgr_id.as_str()),dir_object_id,None).await?;
    let dir_object_str = serde_json::to_string(&dir_object).unwrap();
    let dir_object:DirObject = serde_json::from_value(dir_object).map_err(|e| NdnError::Internal(format!("Failed to parse DirObject: {}", e)))?;
    
    
    match pull_mode {
        StoreMode::NoStore => {
            unimplemented!()
        },
        StoreMode::LocalFile(local_file_path,range,is_store_in_named_mgr) => {
            unimplemented!()
        },
        StoreMode::StoreInNamedMgr => {
            //TODO:如何快速退出
        }
    }

    for (sub_name,sub_item) in dir_object.iter() {
        let sub_item_type = sub_item.get_obj_type();
        match sub_item_type.as_str() {
            OBJ_TYPE_DIR => {
                let (sub_item_obj_id,_) = sub_item.get_obj_id()?;
                Box::pin(copy_dir_from_ndn_mgr(source_ndn_mgr_id,target_ndn_mgr_id,&sub_item_obj_id,pull_mode.clone())).await?;
            },
            OBJ_TYPE_FILE => {
                let (sub_item_obj_id,file_obj_str) = sub_item.get_obj_id()?;
                let file_obj:FileObject = serde_json::from_str(file_obj_str.as_str()).map_err(|e| NdnError::Internal(format!("Failed to parse FileObject: {}", e)))?;
                copy_file_from_ndn_mgr(source_ndn_mgr_id,target_ndn_mgr_id,&sub_item_obj_id,&file_obj,pull_mode.clone()).await?;
            },
            _ => {
                warn!("copy_dir_from_ndn_mgr: unknown sub item type:{}",sub_item_type);
                continue;
            }
        }
    }

    NamedDataMgr::put_object(Some(target_ndn_mgr_id.as_str()),dir_object_id,&dir_object_str).await?;
    return Ok(());
}

mod test {
    use super::*;
    use buckyos_kit::*;
    use tempfile::tempdir;
    use crate::{NamedDataMgrConfig, NdnClient};

    #[tokio::test]
    async fn test_file_name() {
        let hexstr = "80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5";
        let len = hexstr.len();
        let dir1 = &hexstr[len-4..len-2];
        let dir2 = &hexstr[len-2..len];
        let file_name = format!("{}.{}",&hexstr,ChunkType::Mix256.to_string().as_str());
        println!("dir1: {}", dir1);
        assert_eq!(dir1, "75");
        println!("dir2: {}", dir2);
        assert_eq!(dir2, "d5");
        println!("file_name: {}", file_name);
    }
    /*
    构造测试
    # 针对一个目录，基于named_mgrA,使用link模式计算dir object idA该过程可以中断重试(用qcid快速跳过已经计算过的fileobject)
    # 对2个超过16MB的文件进行修改（追加内容），再次构造dir objectB,此时只有2个新的Chunk被创建
    # 将dir objectA pull到named_mgrB,使用in store模式，该过程可以中断后重试
    # 将dir objectA 从named_mgrB pull到named_mgrC,使用local-file link模式，该过程可以中断后重试
    # 将dir objectB 从named_mgrA pull到named_mgrC,使用local-file link模式，该过程中，只有2个文件被修改
     */
    #[tokio::test]
    async fn test_all() {
        let test_dirA = tempdir().unwrap();
        let configA = NamedDataMgrConfig::default();
        let named_mgrA = NamedDataMgr::from_config(
            Some("testA".to_string()),
            test_dirA.path().to_path_buf(),
            configA,
        )
        .await.unwrap();
        let test_dirB = tempdir().unwrap();
        let configB = NamedDataMgrConfig::default();
        let named_mgrB = NamedDataMgr::from_config( 
            Some("testB".to_string()),
            test_dirB.path().to_path_buf(),
            configB,
        )
        .await.unwrap();
        let test_dirC = tempdir().unwrap();
        let configC = NamedDataMgrConfig::default();
        let named_mgrC = NamedDataMgr::from_config(
            Some("testC".to_string()),
            test_dirC.path().to_path_buf(),
            configC,
        )
        .await.unwrap();

        let source_dir_tmp = tempdir().unwrap();
        let source_dir = source_dir_tmp.path();
        std::fs::write(source_dir.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir_all(source_dir.join("sub")).unwrap();
        std::fs::write(source_dir.join("sub").join("b.txt"), b"world").unwrap();
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let (dir_objectA,dir_object_id,dir_object_str) = cacl_dir_object(Some("testA"),source_dir,&file_obj_template,&CheckMode::ByQCID,StoreMode::StoreInNamedMgr,None).await.unwrap();
        let dir_object_str = serde_json::to_string_pretty(&dir_objectA).unwrap();
        println!("dir_object_str: {}", dir_object_str);
        println!("dir_object_id: {}", dir_object_id.to_string());

        //let ndn_clientB = NdnClient::new(named_mgrB.clone());
        //NdnClient::pull_dir(Some("testB"), dir_objectA, PullMode::default(), None).await.unwrap();
    }

    #[tokio::test]
    async fn test_cacl_dir_object() {
        //std::env::set_var("BUCKY_LOG", "warn");
        init_logging("ndn-lib-test", false);
        let test_dir = tempdir().unwrap();
        let config = NamedDataMgrConfig::default();
        
        let target_dir_tmp = tempdir().unwrap();
        let target_dir = target_dir_tmp.path();
        let named_mgr = NamedDataMgr::from_config(
            Some("test".to_string()),
            target_dir.to_path_buf(),
            config,
        )
        .await.unwrap();

        let test_dir = tempdir().unwrap();
        let config2 = NamedDataMgrConfig::default();
        NamedDataMgr::set_mgr_by_id(Some("test"),named_mgr).await.unwrap();
        let named_mgr = NamedDataMgr::from_config(
            Some("test2".to_string()),
            test_dir.path().to_path_buf(),
            config2,
        )
        .await.unwrap();
        NamedDataMgr::set_mgr_by_id(Some("test2"),named_mgr).await.unwrap();

        let progress_callback: Option<Arc<Mutex<NdnProgressCallback>>> = Some(Arc::new(Mutex::new(Box::new(|inner_path:String,action:NdnAction| {
            Box::pin(async move {
                info!("ndn_callback: {} {}", inner_path, action.to_string());
                Ok(ProgressCallbackResult::Continue)
            }) as Pin<Box<dyn std::future::Future<Output = NdnResult<ProgressCallbackResult>> + Send + 'static>>
        }))));

        info!("---start calc dir object in store to named mgr mode");
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let source_dir_tmp = tempdir().unwrap();
        let source_dir = source_dir_tmp.path();
        std::fs::write(source_dir.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir_all(source_dir.join("sub")).unwrap();
        std::fs::write(source_dir.join("sub").join("b.txt"), b"world").unwrap();
        //let (dir_object,dir_object_id,dir_object_str) = cacl_dir_object(Some("test"),&Path::new("/Users/liuzhicong/Downloads/"),&file_obj_template,&CheckMode::ByQCID,StoreMode::StoreInNamedMgr,progress_callback.clone()).await.unwrap();
        let (dir_object,dir_object_id,dir_object_str) = cacl_dir_object(Some("test"),source_dir,&file_obj_template,&CheckMode::ByQCID,StoreMode::StoreInNamedMgr,None).await.unwrap();
     
        let dir_object_store_str = serde_json::to_string_pretty(&dir_object).unwrap();
        println!("dir_object_store_str: {}", dir_object_store_str);
        println!("dir_object_str: {}", dir_object_str);
        println!("dir_object_id: {}", dir_object_id.to_string());


        info!("---start calc dir object in local file mode");
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let (dir_object,dir_object_id2,dir_object_str2) = cacl_dir_object(Some("test2"),source_dir,&file_obj_template,&CheckMode::ByQCID,StoreMode::new_local(),progress_callback.clone()).await.unwrap();
        //let (dir_object,dir_object_id2,dir_object_str2) = cacl_dir_object(Some("test2"),&Path::new("/Users/liuzhicong/Downloads/"),&file_obj_template,&CheckMode::ByQCID,StoreMode::new_local(),None).await.unwrap();
 
        let dir_object_store_str = serde_json::to_string_pretty(&dir_object).unwrap();
        println!("dir_object_store_str: {}", dir_object_store_str);
        println!("dir_object_id2: {}", dir_object_id2.to_string());
        println!("dir_object_str2: {}", dir_object_str2);
        assert_eq!(dir_object_str, dir_object_str2);
        assert_eq!(dir_object_id, dir_object_id2);

        info!("---end");

    }
}
