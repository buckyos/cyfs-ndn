use crate::{ChunkHasher, DirObject, FileObject, NamedDataMgr, NdnError, NdnResult, ObjId, PackedObjItem, PullMode, SimpleChunkList, CHUNK_NORMAL_SIZE, OBJ_TYPE_DIR, OBJ_TYPE_FILE};
use std::ops::Range;
use std::path::Path;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use tokio::fs;

use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
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

//use Link Mode to cacl dir object
pub async fn cacl_file_object(local_file_path:&Path,fileobj_template:&FileObject,use_chunklist:bool,
    ndn_mgr:&NamedDataMgr,check_mode:&CheckMode) -> NdnResult<FileObject> {
    let mut file_obj_result = fileobj_template.clone();
    let file_meta = tokio::fs::metadata(local_file_path).await.unwrap();
    let file_size = file_meta.len();
    let file_last_modify_time = file_meta.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    let mut is_use_chunklist = false;
    let mut chunk_size = CHUNK_NORMAL_SIZE as u64;
    let mut chunk_list:SimpleChunkList = SimpleChunkList::new();
    let mut chunk_id:ChunkId;

    if file_size > CHUNK_NORMAL_SIZE as u64 {
        if use_chunklist {
            is_use_chunklist = true;
        } else {
            chunk_size = file_size;
        }
    }

    if check_mode.is_support_quick_check() {
        let new_qcid = caculate_qcid_from_file(local_file_path).await?;
        let local_file_info = ndn_mgr.query_local_file_info_by_qcid_impl(new_qcid.as_str());
        if local_file_info.is_some() {
            let local_file_info = local_file_info.unwrap();
            //TODO:need check path and last modify time?
            debug!("cacl_file_object:local file is not changed ByQCID, reuse linked local file info");
            file_obj_result.content = local_file_info.content.clone();
            return Ok(file_obj_result);
        } 
    }

    let mut file_reader = tokio::fs::File::open(local_file_path).await.map_err(|e| {
        error!("open local_file_path failed, err:{}", e);
        NdnError::IoError(format!("open local_file_path failed, err:{}", e))
    })?;
    debug!("open local_file_path success");
    let mut read_pos = 0;
    
    while read_pos < file_size {
        let mut chunk_hasher = ChunkHasher::new(None).unwrap();
        let chunk_type = chunk_hasher.hash_method.clone();
        //file_reader.seek(SeekFrom::Start(read_pos)).await;
        let (chunk_raw_id, chunk_size) = chunk_hasher
            .calc_from_reader_with_length(&mut file_reader, chunk_size)
            .await
            .unwrap();

        chunk_id = ChunkId::from_mix_hash_result_by_hash_method(chunk_size, &chunk_raw_id, chunk_type)?;
        info!(
            "cacl_file_object:calc chunk_id success,chunk_id:{},chunk_size:{}",
            chunk_id.to_string(),
            chunk_size
        );

        let is_exist = ndn_mgr.is_chunk_exist_impl(&chunk_id).await?;
        if !is_exist {
            //TODO: link时需要指定Range
            let range = Range{start:read_pos,end:read_pos + chunk_size};
            //ndn_mgr.link_chunk_to_local_impl(&chunk_id, &local_file_path,range,file_last_modify_time).await?;
        } 
        if is_use_chunklist {
            chunk_list.append_chunk(chunk_id.clone())?;
        } else {
            file_obj_result.content = chunk_id.to_string();
        }
        read_pos += chunk_size;
    }
    if is_use_chunklist {
        //gen chunk list id
        let sub_item_count = chunk_list.body.len();
        let (chunk_list_id, chunk_list_str) = chunk_list.gen_obj_id();
        ndn_mgr.put_object_impl(&chunk_list_id, &chunk_list_str).await?;
        file_obj_result.content = chunk_list_id.to_string();
    } 

    file_obj_result.size = file_size;
    file_obj_result.create_time = None;

    Ok(file_obj_result)
}

//use Link Mode to cacl dir object
pub async fn cacl_dir_object(source_dir:&Path,file_obj_template:&FileObject,ndn_mgr:&NamedDataMgr,check_mode:&CheckMode) -> NdnResult<DirObject> {
    //遍历source_dir下的所有文件和子目录
    let mut read_dir = fs::read_dir(source_dir).await?;
    let mut will_process_file = Vec::new();
    let mut this_dir_obj = DirObject::new(None);
    while let Some(entry) = read_dir.next_entry().await? {
        let sub_path = entry.path();
        if sub_path.is_dir() {
            //println!("dir: {}", sub_path.display());
            let sub_dir_obj = Box::pin(cacl_dir_object(&sub_path,file_obj_template,ndn_mgr,check_mode)).await?;
            let sub_total_size = sub_dir_obj.total_size;
            let sub_dir_obj_str = serde_json::to_string(&sub_dir_obj).unwrap();
            let (sub_dir_obj_id, _) = sub_dir_obj.gen_obj_id()?;
            ndn_mgr.put_object_impl(&sub_dir_obj_id, &sub_dir_obj_str).await?;
            this_dir_obj.add_directory(sub_path.file_name().unwrap().to_string_lossy().to_string(), 
                sub_dir_obj_id, sub_total_size);
        } else {
            //println!("file: {}", sub_path.display());
            will_process_file.push(sub_path);
        }
    }
    
    for file in will_process_file {
        //println!("file: {}", file.display());
        let file_object = cacl_file_object(&file,file_obj_template,true,ndn_mgr,check_mode).await?;
        let file_object_str = serde_json::to_string(&file_object).unwrap();
        let file_object_json = serde_json::to_value(&file_object).unwrap();
        let (file_object_id, _) = file_object.gen_obj_id();
        ndn_mgr.put_object_impl(&file_object_id, &file_object_str).await?;
        this_dir_obj.add_file(file.file_name().unwrap().to_string_lossy().to_string(), file_object_json, file_object.size);
    }

    return Ok(this_dir_obj);
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
    file_obj_id:&ObjId,file_object:&FileObject,pull_mode:PullMode) -> NdnResult<()> {
    let content_id = ObjId::new(file_object.content.as_str())?;
    
    match pull_mode {
        PullMode::LocalFile(local_file_path,range,is_store_in_named_mgr) => {
            unimplemented!()
        },
        PullMode::StoreInNamedMgr => {
            if content_id.is_chunk() {
                let chunk_id = ChunkId::from_obj_id(&content_id);
                if !NamedDataMgr::have_chunk(&chunk_id,Some(source_ndn_mgr_id.as_str())).await {
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
                    if !NamedDataMgr::have_chunk(&chunk_id,Some(source_ndn_mgr_id.as_str())).await {
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
    dir_object_id:&ObjId,pull_mode:PullMode) -> NdnResult<()> {
    let dir_object = NamedDataMgr::get_object(Some(source_ndn_mgr_id.as_str()),dir_object_id,None).await?;
    let dir_object_str = serde_json::to_string(&dir_object).unwrap();
    let dir_object:DirObject = serde_json::from_value(dir_object).map_err(|e| NdnError::Internal(format!("Failed to parse DirObject: {}", e)))?;
    
    
    match pull_mode {
        PullMode::LocalFile(local_file_path,range,is_store_in_named_mgr) => {
            unimplemented!()
        },
        PullMode::StoreInNamedMgr => {
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

        let source_dir = Path::new("/Users/liuzhicong/Downloads/");
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let dir_objectA = cacl_dir_object(source_dir,&file_obj_template,&named_mgrA,&CheckMode::ByQCID).await.unwrap();
        let dir_object_str = serde_json::to_string_pretty(&dir_objectA).unwrap();
        println!("dir_object_str: {}", dir_object_str);
        let (dir_object_id, dir_obj_str_for_gen_obj_id) = dir_objectA.gen_obj_id().unwrap();
        println!("dir_object_id: {}", dir_object_id.to_string());

        //let ndn_clientB = NdnClient::new(named_mgrB.clone());
        //NdnClient::pull_dir(Some("testB"), dir_objectA, PullMode::default(), None).await.unwrap();
    }

    #[tokio::test]
    async fn test_cacl_dir_object() {
        init_logging("ndn-lib-test", false);
        let test_dir = tempdir().unwrap();
        let config = NamedDataMgrConfig::default();
    
        let named_mgr = NamedDataMgr::from_config(
            Some("test".to_string()),
            test_dir.path().to_path_buf(),
            config,
        )
        .await.unwrap();

        
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let dir_object = cacl_dir_object(&Path::new("/Users/liuzhicong/Downloads/"),&file_obj_template,&named_mgr,&CheckMode::ByQCID).await.unwrap();
        let dir_object_str = serde_json::to_string_pretty(&dir_object).unwrap();
        println!("dir_object_str: {}", dir_object_str);
        let (dir_object_id, dir_obj_str_for_gen_obj_id) = dir_object.gen_obj_id().unwrap();
        println!("dir_object_id: {}", dir_object_id.to_string());
        println!("dir_obj_str_for_gen_obj_id: {}", dir_obj_str_for_gen_obj_id);
    }
}