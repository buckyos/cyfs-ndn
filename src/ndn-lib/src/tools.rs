use crate::{NamedDataMgr, NdnResult, DirObject,FileObject,NdnError,CHUNK_NORMAL_SIZE,ChunkHasher};
use std::path::Path;
use tokio::fs;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
use crate::chunk::ChunkId;

pub async fn cacl_file_object(local_file_path:&Path,fileobj_template:&FileObject,use_chunklist:bool,ndn_mgr:&NamedDataMgr) -> NdnResult<FileObject> {
    let mut file_obj_result = fileobj_template.clone();
    let file_size = tokio::fs::metadata(local_file_path).await.unwrap().len();
    
    let mut is_use_chunklist = false;
    let mut chunk_size = CHUNK_NORMAL_SIZE as u64;

    if file_size > CHUNK_NORMAL_SIZE as u64 {
        if use_chunklist {
            is_use_chunklist = true;
        } else {
            chunk_size = file_size;
        }
    }

    let mut file_reader = tokio::fs::File::open(local_file_path).await.map_err(|e| {
        error!("open local_file_path failed, err:{}", e);
        NdnError::IoError(format!("open local_file_path failed, err:{}", e))
    })?;
    debug!("open local_file_path success");
    let mut read_pos = 0;

    let mut chunk_hasher = ChunkHasher::new(None).unwrap();
    let chunk_type = chunk_hasher.hash_method.clone();
    file_reader.seek(SeekFrom::Start(read_pos)).await;
    let (chunk_raw_id, chunk_size) = chunk_hasher
        .calc_from_reader_with_length(&mut file_reader, chunk_size)
        .await
        .unwrap();

    let chunk_id = ChunkId::from_mix_hash_result_by_hash_method(chunk_size, &chunk_raw_id, chunk_type)?;
    info!(
        "cacl_file_object:calc chunk_id success,chunk_id:{},chunk_size:{}",
        chunk_id.to_string(),
        chunk_size
    );

    let is_exist = ndn_mgr.is_chunk_exist_impl(&chunk_id).await.unwrap();
    if !is_exist {
        //使用软链接的方法写入ndn_mgr
        
    } 
    file_obj_result.content = chunk_id.to_string();
    file_obj_result.size = chunk_size;
    file_obj_result.create_time = None;

    Ok(file_obj_result)
}

pub async fn cacl_dir_object(source_dir:&Path,file_obj_template:&FileObject,ndn_mgr:&NamedDataMgr) -> NdnResult<DirObject> {
    //遍历source_dir下的所有文件和子目录
    let mut read_dir = fs::read_dir(source_dir).await?;
    let mut will_process_file = Vec::new();
    let mut this_dir_obj = DirObject::new(None);
    while let Some(entry) = read_dir.next_entry().await? {
        let sub_path = entry.path();
        if sub_path.is_dir() {
            //println!("dir: {}", sub_path.display());
            let sub_dir_obj = Box::pin(cacl_dir_object(&sub_path,file_obj_template,ndn_mgr)).await?;
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
        let file_object = cacl_file_object(&file,file_obj_template,false,ndn_mgr).await?;
        let file_object_str = serde_json::to_string(&file_object).unwrap();
        let file_object_json = serde_json::to_value(&file_object).unwrap();
        let (file_object_id, _) = file_object.gen_obj_id();
        ndn_mgr.put_object_impl(&file_object_id, &file_object_str).await?;
        this_dir_obj.add_file(file.file_name().unwrap().to_string_lossy().to_string(), file_object_json, file_object.size);
    }

    return Ok(this_dir_obj);
}

pub async fn restore_dir_object(dir_object:&DirObject,ndn_mgr:&NamedDataMgr,target_dir:&Path) -> NdnResult<()> {
    unimplemented!()
}

mod test {
    use super::*;
    use buckyos_kit::*;
    use tempfile::tempdir;
    use crate::NamedDataMgrConfig;

    #[tokio::test]
    async fn test_cacl_dir_object() {
        init_logging("ndn-lib-test", false);
        let test_dir = tempdir().unwrap();
        let config = NamedDataMgrConfig {
            local_stores: vec![test_dir.path().to_str().unwrap().to_string()],
            local_cache: None,
            mmap_cache_dir: None,
        };
    
        let named_mgr = NamedDataMgr::from_config(
            Some("test".to_string()),
            test_dir.path().to_path_buf(),
            config,
        )
        .await.unwrap();

        
        let file_obj_template = FileObject::new("".to_string(), 0, "".to_string());
        let dir_object = cacl_dir_object(&Path::new("/Users/liuzhicong/Downloads/"),&file_obj_template,&named_mgr).await.unwrap();
        let dir_object_str = serde_json::to_string_pretty(&dir_object).unwrap();
        println!("dir_object_str: {}", dir_object_str);
        let (dir_object_id, dir_obj_str_for_gen_obj_id) = dir_object.gen_obj_id().unwrap();
        println!("dir_object_id: {}", dir_object_id.to_string());
        println!("dir_obj_str_for_gen_obj_id: {}", dir_obj_str_for_gen_obj_id);

    }
}