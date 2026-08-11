/*
PackageEnv 当前实现说明

1. 目录结构

- 通用文件
  - `work_dir/pkg.cfg.json`：env 配置；不存在时使用默认配置
  - `work_dir/pkgs/env.lock`：安装和索引更新的全局写锁
  - `work_dir/pkgs/meta_index.db`：pkg 元数据索引
  - `work_dir/pkgs/meta_index.db.old`：索引更新时的备份

- 严格模式目录（也是安装后的真实落盘位置）
  - `work_dir/pkgs/<pkg_name>/<meta_obj_id.to_filename()>`
  - 这里是包的唯一物理目录，`load_strictly` 和安装逻辑都以这个路径为准

- 普通模式目录（`enable_strict_mode = false`）
  - 先尝试严格目录
  - 失败后再尝试开发态/友好路径
  - 友好路径只保留 `work_dir/<pkg_name>`
  - 当 `enable_link = true` 时，友好路径指向严格目录
  - 当 `enable_link = false` 时，友好路径是从严格目录复制出来的一份目录

2. 主要接口流程

- `get_pkg_meta(pkg_id)`
  - 先查进程内 `lock_db`
  - 再查当前 env 的 `meta_index.db`
  - 当前 env 查不到时递归查 parent env

- `load(pkg_id)`
  - 先走 `load_strictly`
  - 普通模式下若严格加载失败，再回退到友好路径 / 开发态目录
  - 当前 env 失败后继续尝试 parent env

- `check_pkg_ready(meta_db, pkg_id, store_mgr, miss_chunk_list)`
  - 从 `meta_index.db` 取得 `PackageMeta`
  - 将 `PackageMeta` 视为 `FileObject`
  - 检查其 `content` 指向的 chunk 或 chunklist 是否已经全部存在于 named store
  - 缺失的 chunk 会写入 `miss_chunk_list`

- `check_deps_ready(meta_db, pkg_id, store_mgr, miss_chunk_list)`
  - 递归检查依赖 pkg 是否 ready
  - 不检查当前 pkg 自身内容

- `install_pkg(pkg_id, install_deps, force_install)`
  - 获取写锁
  - 读取 `PackageMeta`
  - 如需要先递归安装依赖
  - 通过 `named_store_config_path + http_backend_links` 构造 `NamedStoreMgr`
  - 安装前先检查 `FileObject.content` 引用的数据是否已全部在 store 中
  - 用 `open_reader` 打开包内容 reader，最终统一落到 `do_install_pkg_from_data`

- `install_pkg_from_local_file(pkg_meta_content, local_file)`
  - 这是开发态/本地文件安装入口
  - 直接打开本地 tar.gz，调用 `do_install_pkg_from_data`
  - 安装后会把 `pkg_meta` 写入当前 env 的 `meta_index.db`

- `do_install_pkg_from_data(...)`
  - 将 tar.gz reader 解压到严格目录
  - 若当前包是 latest，再根据 `enable_link` 维护 `work_dir/<pkg_name>` 友好路径

- `try_update_index_db(new_index_db)`
  - 获取写锁
  - 备份旧索引
  - 覆盖为新索引
*/

use async_trait::async_trait;
use fs_extra::dir::*;
use log::*;
use name_lib::{EncodedDocument, DID};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs as tokio_fs;
use tokio::sync::{oneshot, Mutex as TokioMutex};

//use std::fs::File;
//use std::io;
use async_compression::tokio::bufread::GzipDecoder;
use async_fd_lock::RwLockWriteGuard;
use async_fd_lock::{LockRead, LockWrite};
use named_store::NamedDataMgr;
use ndn_lib::*;
use ndn_toolkit::{check_file_object_content_ready, collect_missing_chunks_for_file_object};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio_tar::Archive;

use crate::error::*;
use crate::meta::*;
use crate::meta_index_db::*;
use crate::package_id::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageEnvConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>, //如果指定了，那么加载无 . 的pkg_name时，会自动补上prefix,变成加载 $prefix.$pkg_name
    pub enable_link: bool,
    pub enable_strict_mode: bool,
    pub index_db_path: Option<String>,
    pub parent: Option<PathBuf>, //parent package env work_dir
    pub ready_only: bool,        //read only env cann't install any new pkgs
    pub named_store_config_path: Option<String>, //如果指定了，则使用 named_store 配置文件路径作为默认 read chunk 的来源
    #[serde(default)]
    pub http_backend_links: HashMap<String, String>, //device_did -> http backend前缀；未命中表示本地桶
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    #[serde(default)]
    pub installed: HashSet<String>, //pkg_id列表，表示已经安装的pkg
}

impl PackageEnvConfig {
    pub fn get_default_prefix() -> String {
        let env_str = env!("PACKAGE_DEFAULT_PERFIX").to_string();
        if env_str.len() > 1 {
            return env_str;
        }

        //得到操作系统类型
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let os_type = "nightly-linux-amd64";
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let os_type = "nightly-linux-aarch64";
        #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
        let os_type = "nightly-windows-amd64";
        #[cfg(all(target_os = "windows", target_arch = "aarch64"))]
        let os_type = "nightly-windows-aarch64";
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        let os_type = "nightly-apple-amd64";
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let os_type = "nightly-apple-aarch64";

        os_type.to_string()
    }
}

impl Default for PackageEnvConfig {
    fn default() -> Self {
        let os_type = PackageEnvConfig::get_default_prefix();

        Self {
            enable_link: true,
            enable_strict_mode: false, //默认是非严格的开发模式
            index_db_path: None,
            parent: None,
            ready_only: false,
            named_store_config_path: None,
            http_backend_links: HashMap::new(),
            prefix: Some(os_type.to_string()),
            installed: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MediaType {
    Dir,
    File,
}

#[derive(Debug, Clone)]
pub struct MediaInfo {
    pub pkg_id: PackageId,
    pub full_path: PathBuf,
    pub media_type: MediaType,
}

#[derive(Clone)]
pub struct PackageEnv {
    pub work_dir: PathBuf,
    pub config: PackageEnvConfig,
    is_dev_mode: bool,
    lock_db: Arc<TokioMutex<Option<HashMap<String, (String, PackageMeta)>>>>,
}

impl PackageEnv {
    pub fn new(work_dir: PathBuf) -> Self {
        let config_path = work_dir.join("pkg.cfg.json");
        let mut env_config = PackageEnvConfig::default();
        let mut is_dev_mode = true;
        if config_path.exists() {
            let config = std::fs::read_to_string(config_path);
            if config.is_ok() {
                let config = config.unwrap();
                let config_result = serde_json::from_str(&config);
                if config_result.is_ok() {
                    env_config = config_result.unwrap();
                    is_dev_mode = false;
                    debug!("pkg_env {} load pkg.cfg.json OK.", work_dir.display());
                    if env_config.parent.is_some() {
                        if env_config.parent.as_ref().unwrap().is_relative() {
                            let parent_path = format!(
                                "{}/{}",
                                work_dir.display(),
                                env_config.parent.as_ref().unwrap().display()
                            );
                            let parent_path = buckyos_kit::normalize_path(&parent_path);
                            let parent_path = PathBuf::from(parent_path);
                            debug!(
                                "pkg_env {} parent abs path: {}",
                                work_dir.display(),
                                parent_path.display()
                            );
                            env_config.parent = Some(parent_path);
                        } else {
                            let parent_path = env_config.parent.as_ref().unwrap();
                            debug!(
                                "pkg_env {} parent abs path: {}",
                                work_dir.display(),
                                parent_path.display()
                            );
                        }
                    }
                } else {
                    warn!(
                        "pkg_env {} load pkg.cfg.json failed. {}",
                        work_dir.display(),
                        config_result.err().unwrap()
                    );
                }
            }
        }

        Self {
            work_dir,
            config: env_config,
            is_dev_mode: is_dev_mode,
            lock_db: Arc::new(TokioMutex::new(None)),
        }
    }

    pub fn is_dev_mode(&self) -> bool {
        self.is_dev_mode
    }

    pub fn is_strict(&self) -> bool {
        self.config.enable_strict_mode
    }

    pub fn update_config_file(&self, config: &PackageEnvConfig) -> PkgResult<()> {
        let config_path = self.work_dir.join("pkg.cfg.json");
        if config_path.exists() {
            let config_str = serde_json::to_string(config).unwrap();
            std::fs::write(config_path, config_str).unwrap();
        } else {
            return Err(PkgError::FileNotFoundError(
                "Package config file not found".to_owned(),
            ));
        }

        Ok(())
    }

    // 基于env获得pkg的meta信息
    pub async fn get_pkg_meta(&self, pkg_id: &str) -> PkgResult<(String, PackageMeta)> {
        // 先检查lock db
        let pkg_id = PackageId::parse(pkg_id)?;
        let pkg_id = self.prefix_pkg_id(&pkg_id);
        let pkg_id_str = pkg_id.to_string();
        if let Some(lock_db) = self.lock_db.lock().await.as_ref() {
            if let Some((meta_obj_id, meta)) = lock_db.get(&pkg_id_str) {
                return Ok((meta_obj_id.clone(), meta.clone()));
            }
        }

        let meta_db_path = self.get_meta_db_path();
        //info!("get meta db from {}", meta_db_path.display());
        let meta_db = MetaIndexDb::new(meta_db_path, true)?;
        if let Some((meta_obj_id, pkg_meta)) = meta_db.get_pkg_meta(&pkg_id_str)? {
            return Ok((meta_obj_id, pkg_meta));
        }

        if self.config.parent.is_some() {
            let parent_env = PackageEnv::new(self.config.parent.as_ref().unwrap().clone());
            let (meta_obj_id, pkg_meta) = Box::pin(parent_env.get_pkg_meta(&pkg_id_str)).await?;
            return Ok((meta_obj_id, pkg_meta));
        }

        Err(PkgError::LoadError(
            pkg_id_str,
            "Package metadata not found".to_owned(),
        ))
    }

    fn prefix_pkg_id(&self, pkg_id: &PackageId) -> PackageId {
        let mut pkg_id = pkg_id.clone();
        if pkg_id.name.find(".").is_some() {
            return pkg_id;
        }
        let prefix = self.get_prefix();
        pkg_id.name = format!("{}.{}", prefix, pkg_id.name.as_str());
        pkg_id
    }

    //加载pkg,加载成功说明pkg已经安装
    pub async fn load(&self, pkg_id_str: &str) -> PkgResult<MediaInfo> {
        match self.load_strictly(pkg_id_str).await {
            Ok(media_info) => Ok(media_info),
            Err(error) => {
                if self.is_strict() {
                    if let Some(parent_path) = &self.config.parent {
                        let parent_env = PackageEnv::new(parent_path.clone());
                        // 使用 Box::pin 来处理递归的异步调用
                        let future = Box::pin(parent_env.load(pkg_id_str));
                        if let Ok(media_info) = future.await {
                            return Ok(media_info);
                        }
                    }
                    warn!(
                        "load strict pkg {} failed:{}",
                        pkg_id_str,
                        error.to_string()
                    );
                } else {
                    debug!(
                        "dev mode env {} : try load pkg: {}",
                        self.work_dir.display(),
                        pkg_id_str
                    );
                    let media_info = self.dev_try_load(pkg_id_str).await;
                    if media_info.is_ok() {
                        return Ok(media_info.unwrap());
                    }
                    if let Some(parent_path) = &self.config.parent {
                        let parent_env = PackageEnv::new(parent_path.clone());
                        let future = Box::pin(parent_env.load(pkg_id_str));
                        if let Ok(media_info) = future.await {
                            return Ok(media_info);
                        }
                    }
                    warn!(
                        "load dev mode pkg {} failed:{}",
                        pkg_id_str,
                        error.to_string()
                    );
                }

                Err(PkgError::LoadError(
                    pkg_id_str.to_owned(),
                    format!(
                        "Package {} metadata not found : {}",
                        pkg_id_str,
                        error.to_string()
                    ),
                ))
            }
        }
    }

    pub async fn cacl_pkg_deps_metas(
        &self,
        pkg_meta: &PackageMeta,
        deps: &mut HashMap<String, PackageMeta>,
    ) -> PkgResult<()> {
        let mut visiting = HashSet::new();
        visiting.insert(pkg_meta.get_package_id().to_string());
        self.cacl_pkg_deps_metas_impl(pkg_meta, deps, &mut visiting)
            .await
    }

    async fn cacl_pkg_deps_metas_impl(
        &self,
        pkg_meta: &PackageMeta,
        deps: &mut HashMap<String, PackageMeta>,
        visiting: &mut HashSet<String>,
    ) -> PkgResult<()> {
        for (dep_name, dep_version) in pkg_meta.deps.iter() {
            let dep_id = format!("{}#{}", dep_name, dep_version);
            let (meta_obj_id, dep_meta) = self.get_pkg_meta(&dep_id).await?;
            let dep_pkg_id = dep_meta.get_package_id().to_string();
            if visiting.contains(&dep_pkg_id) {
                return Err(PkgError::LoadError(
                    dep_pkg_id,
                    "Package dependency cycle detected".to_owned(),
                ));
            }
            if deps.contains_key(&meta_obj_id) {
                continue;
            }

            visiting.insert(dep_pkg_id.clone());
            let next_future = Box::pin(self.cacl_pkg_deps_metas_impl(&dep_meta, deps, visiting));
            let result = next_future.await;
            visiting.remove(&dep_pkg_id);
            result?;
            deps.insert(meta_obj_id, dep_meta);
        }
        Ok(())
    }

    // 只检查当前 pkg 的内容是否在本机就绪，不递归检查依赖
    pub async fn check_pkg_ready(
        meta_index_db: &PathBuf,
        pkg_id: &str,
        store_mgr: &NamedDataMgr,
        miss_chunk_list: &mut Vec<ChunkId>,
    ) -> PkgResult<()> {
        let meta_db = MetaIndexDb::new(meta_index_db.clone(), true)?;
        let meta_info = meta_db.get_pkg_meta(pkg_id)?;
        if meta_info.is_none() {
            return Err(PkgError::LoadError(
                pkg_id.to_owned(),
                "Package metadata not found".to_owned(),
            ));
        }

        let (meta_obj_id, pkg_meta) = meta_info.unwrap();
        // 检查chunk是否存在
        if !pkg_meta.content.is_empty() {
            let missing_chunks = collect_missing_chunks_for_file_object(store_mgr, &pkg_meta)
                .await
                .map_err(|e| {
                    PkgError::LoadError(
                        meta_obj_id.clone(),
                        format!("check package content ready failed: {}", e),
                    )
                })?;
            for chunk_id in missing_chunks {
                if !miss_chunk_list.contains(&chunk_id) {
                    miss_chunk_list.push(chunk_id);
                }
            }
        }

        Ok(())
    }

    // 递归检查依赖 pkg 是否都已经在本机就绪，不检查 pkg 自身内容
    pub async fn check_deps_ready(
        meta_index_db: &PathBuf,
        pkg_id: &str,
        store_mgr: &NamedDataMgr,
        miss_chunk_list: &mut Vec<ChunkId>,
    ) -> PkgResult<()> {
        let meta_db = MetaIndexDb::new(meta_index_db.clone(), true)?;
        let meta_info = meta_db.get_pkg_meta(pkg_id)?;
        if meta_info.is_none() {
            return Err(PkgError::LoadError(
                pkg_id.to_owned(),
                "Package metadata not found".to_owned(),
            ));
        }

        let (_, pkg_meta) = meta_info.unwrap();
        let mut visiting = HashSet::new();
        visiting.insert(pkg_meta.get_package_id().to_string());
        Self::check_deps_ready_impl(
            meta_index_db,
            &pkg_meta,
            store_mgr,
            miss_chunk_list,
            &mut visiting,
        )
        .await
    }

    async fn check_deps_ready_impl(
        meta_index_db: &PathBuf,
        pkg_meta: &PackageMeta,
        store_mgr: &NamedDataMgr,
        miss_chunk_list: &mut Vec<ChunkId>,
        visiting: &mut HashSet<String>,
    ) -> PkgResult<()> {
        let meta_db = MetaIndexDb::new(meta_index_db.clone(), true)?;

        for (dep_name, dep_version) in pkg_meta.deps.iter() {
            let dep_id = format!("{}#{}", dep_name, dep_version);
            let meta_info = meta_db.get_pkg_meta(&dep_id)?;
            let Some((_, dep_meta)) = meta_info else {
                return Err(PkgError::LoadError(
                    dep_id,
                    "Package metadata not found".to_owned(),
                ));
            };

            let dep_pkg_id = dep_meta.get_package_id().to_string();
            if visiting.contains(&dep_pkg_id) {
                return Err(PkgError::LoadError(
                    dep_pkg_id,
                    "Package dependency cycle detected".to_owned(),
                ));
            }

            Self::check_pkg_ready(meta_index_db, &dep_pkg_id, store_mgr, miss_chunk_list).await?;

            visiting.insert(dep_pkg_id.clone());
            let result = Box::pin(Self::check_deps_ready_impl(
                meta_index_db,
                &dep_meta,
                store_mgr,
                miss_chunk_list,
                visiting,
            ))
            .await;
            visiting.remove(&dep_pkg_id);
            result?;
        }

        Ok(())
    }

    //尝试更新env的meta-index-db,这是个写入操作，更新后之前的load操作可能会失败，需要再执行一次install_pkg才能加载
    pub async fn try_update_index_db(&self, new_index_db: &Path) -> PkgResult<()> {
        if self.config.ready_only {
            return Err(PkgError::AccessDeniedError(
                "Cannot update index db in read-only mode".to_owned(),
            ));
        }

        let _lock = self.acquire_lock().await?;

        let mut index_db_path = self.get_meta_db_path();
        let backup_path = index_db_path.with_extension("old");
        if tokio_fs::metadata(&backup_path).await.is_ok() {
            tokio_fs::remove_file(&backup_path).await?;
            info!("delete backup index db: {:?}", backup_path);
        }

        if tokio_fs::metadata(&index_db_path).await.is_ok() {
            let backup_path = index_db_path.with_extension("old");
            info!(
                "rename old index db: {:?} to {:?}",
                index_db_path, backup_path
            );
            tokio_fs::rename(&index_db_path, &backup_path).await?;
        }

        // 移动新数据库
        tokio_fs::copy(new_index_db, &index_db_path).await?;
        info!("update index db: {:?} OK", index_db_path);
        Ok(())
    }

    //插入一条新的pkg_meta,注意如果meta_db不存在要自动创建
    pub async fn set_pkg_meta_to_index_db(
        &self,
        meta_obj_id: &str,
        pkg_meta: &PackageMeta,
    ) -> PkgResult<()> {
        if self.config.ready_only {
            return Err(PkgError::InstallError(
                meta_obj_id.to_owned(),
                "Cannot update index db in read-only mode".to_owned(),
            ));
        }

        let (expected_meta_obj_id, pkg_meta_str) = pkg_meta.gen_obj_id();
        if expected_meta_obj_id.to_string() != meta_obj_id {
            return Err(PkgError::ParseError(
                meta_obj_id.to_owned(),
                format!(
                    "meta obj id does not match package meta, expected {}",
                    expected_meta_obj_id
                ),
            ));
        }

        let _filelock = self.acquire_lock().await?;
        self.write_pkg_meta_to_db(meta_obj_id, &pkg_meta_str, pkg_meta)?;

        info!(
            "set_pkg_meta_to_index_db: pkg {} indexed successfully",
            pkg_meta.get_package_id().to_string()
        );
        Ok(())
    }

    async fn install_pkg_impl(
        &mut self,
        meta_obj_id: &str,
        pkg_meta: &PackageMeta,
        force_install: bool,
    ) -> PkgResult<()> {
        let pkg_id = pkg_meta.get_package_id().to_string();
        let real_meta_obj_id = ObjId::new(meta_obj_id)
            .map_err(|e| PkgError::ParseError(meta_obj_id.to_owned(), e.to_string()))?;

        //新逻辑：
        // 1） pkg_meta现在一定是一个fileobj,所以可以用FileObject来处理
        // 2)  使用ndn-toolkit的辅助函数，将fileobj还原为本地文件，并解压安装到env中
        // 3） 注意，如果通过named_store_config_path配置的named_store_mgr没有这个chunk，则失败。下载是安装的前置逻辑,package-lib本身不管理下载

        if pkg_meta.content.is_empty() {
            return Err(PkgError::InstallError(
                pkg_id,
                "Package content is empty".to_owned(),
            ));
        }

        let store_config_path = self
            .config
            .named_store_config_path
            .as_ref()
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    self.work_dir.join(path)
                }
            })
            .ok_or_else(|| {
                PkgError::InstallError(
                    pkg_id.clone(),
                    "named_store_config_path is required for package installation".to_owned(),
                )
            })?;
        let store_mgr =
            NamedDataMgr::get_store_mgr(&store_config_path, &self.config.http_backend_links)
                .await
                .map_err(|e| {
                    PkgError::InstallError(
                        pkg_id.clone(),
                        format!(
                            "Failed to open named store config {}: {}",
                            store_config_path.display(),
                            e
                        ),
                    )
                })?;

        check_file_object_content_ready(&store_mgr, pkg_meta)
            .await
            .map_err(|e| {
                PkgError::InstallError(
                    pkg_id.clone(),
                    format!("Package content is not ready in named store: {}", e),
                )
            })?;

        let content_obj_id = ObjId::new(pkg_meta.content.as_str()).map_err(|e| {
            PkgError::InstallError(
                pkg_id.clone(),
                format!("Invalid package content obj id {}: {}", pkg_meta.content, e),
            )
        })?;

        let (chunk_reader, _) =
            store_mgr
                .open_reader(&content_obj_id, None)
                .await
                .map_err(|e| {
                    PkgError::InstallError(
                        pkg_id.clone(),
                        format!(
                            "Failed to open package content {} from named store: {}",
                            content_obj_id, e
                        ),
                    )
                })?;

        self.do_install_pkg_from_data(pkg_meta, &real_meta_obj_id, chunk_reader, force_install)
            .await?;

        Ok(())
    }

    pub async fn install_pkg_from_local_file(
        &mut self,
        pkg_meta_content: &str,
        local_file: &Path,
    ) -> PkgResult<()> {
        //这种安装模式不会检查dep
        //安装后,pkg_meta会写入当前env的meta-index-db中
        //local_file指向的是tar.gz的本地文件路径,用只读方法打开

        if self.config.ready_only {
            return Err(PkgError::InstallError(
                local_file.display().to_string(),
                "Cannot install in read-only mode".to_owned(),
            ));
        }

        // 获取文件锁
        let _filelock = self.acquire_lock().await?;
        let pkg_meta = PackageMeta::from_str(pkg_meta_content)?;
        let (meta_obj_id, pkg_meta_str) = pkg_meta.gen_obj_id();

        // 打开本地 tar.gz 文件
        let file = File::open(local_file).await.map_err(|e| {
            PkgError::FileNotFoundError(format!(
                "Failed to open local file {}: {}",
                local_file.display(),
                e
            ))
        })?;

        // 创建 ChunkReader
        let chunk_reader: ChunkReader = Box::pin(file);

        // 解压 tar.gz 文件到目标目录
        self.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await?;

        // 将 pkg_meta 写入 meta_index.db
        self.write_pkg_meta_to_db(&meta_obj_id.to_string(), &pkg_meta_str, &pkg_meta)?;

        info!(
            "install_pkg_from_local_file: pkg {} installed successfully from {}",
            pkg_meta.name,
            local_file.display()
        );

        Ok(())
    }

    // cd my_env && buckycli pkg_install $pkg_id
    //安装pkg，安装成功后该pkg可以加载成功,返回安装成功的pkg的meta_obj_id
    //安装操作会锁定env，直到安装完成（不会出现两个安装操作同时进行）
    //安装过程会根据env是否支持符号链接，尝试建立有好的符号链接
    //在parent envinstall pkg成功，会对所有的child env都有影响
    //在child env install pkg成功，对parent env没有影响
    pub async fn install_pkg(
        &mut self,
        pkg_id: &str,
        install_deps: bool,
        force_install: bool,
    ) -> PkgResult<String> {
        if self.config.ready_only {
            return Err(PkgError::InstallError(
                pkg_id.to_owned(),
                "Cannot install in read-only mode".to_owned(),
            ));
        }
        // 获取文件锁
        let _filelock = self.acquire_lock().await?;
        //先将必要的chunk下载到named_mgr中,对于单OOD系统，这些chunk可能都已经准备好了
        let (meta_obj_id, pkg_meta) = self.get_pkg_meta(pkg_id).await?;

        let will_install_pkg_id = pkg_meta.get_package_id();
        // if self.config.installed.insert(will_install_pkg_id.to_string()) {
        //     self.update_config_file(&self.config)?;
        //     info!("added pkg {} to env.pkg_cfg.json installed list", pkg_id);
        // }

        if install_deps {
            info!("install deps for pkg {}", pkg_id);
            let mut deps = HashMap::new();
            self.cacl_pkg_deps_metas(&pkg_meta, &mut deps).await?;

            for (dep_meta_obj_id, dep_pkg_meta) in deps.iter() {
                info!(
                    "install dep pkg {}#{}",
                    dep_pkg_meta.name, dep_pkg_meta.version
                );

                let install_result = self
                    .install_pkg_impl(dep_meta_obj_id.as_str(), &dep_pkg_meta, force_install)
                    .await;
                match install_result {
                    Ok(_) => {}
                    Err(e) => match e {
                        PkgError::PackageAlreadyInstalled(pkg_id) => {
                            info!("dep pkg {} already installed, skip", pkg_id);
                            continue;
                        }
                        _ => {
                            return Err(e);
                        }
                    },
                }
            }
        }

        self.install_pkg_impl(&meta_obj_id, &pkg_meta, force_install)
            .await?;
        Ok(meta_obj_id)
    }

    pub fn is_latest_version(&self, pkg_id: &PackageId) -> PkgResult<bool> {
        let meta_db = MetaIndexDb::new(self.get_meta_db_path(), true)?;
        let is_latest = meta_db.is_latest_version(pkg_id)?;
        if !is_latest {
            return Ok(false);
        }

        if let Some(parent_path) = &self.config.parent {
            let parent_env = PackageEnv::new(parent_path.clone());
            return parent_env.is_latest_version(pkg_id);
        }

        Ok(true)
    }

    async fn do_install_pkg_from_data(
        &self,
        pkg_meta: &PackageMeta,
        meta_obj_id: &ObjId,
        chunk_reader: ChunkReader,
        force_install: bool,
    ) -> PkgResult<()> {
        // 将 tar.gz reader 解压到严格目录。
        // 若该包是 latest，再额外维护单一友好路径 `work_dir/<pkg_name>`。
        info!("extract pkg {} from chunk", pkg_meta.name.as_str());

        let buf_reader = BufReader::new(chunk_reader);
        let gz_decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(gz_decoder);
        let strict_dir_name = meta_obj_id.to_filename();
        let synlink_target = format!("./pkgs/{}/{}", pkg_meta.name, strict_dir_name);
        let target_dir = self.work_dir.join(synlink_target.clone());
        if target_dir.exists() {
            if force_install {
                info!(
                    "force install pkg {}, remove target dir {}",
                    meta_obj_id,
                    target_dir.display()
                );
                tokio::fs::remove_dir_all(&target_dir).await?;
            } else {
                return Err(PkgError::PackageAlreadyInstalled(meta_obj_id.to_string()));
            }
        }

        tokio::fs::create_dir_all(&target_dir).await?;
        archive.unpack(&target_dir).await?;

        let pkg_id = pkg_meta.get_package_id();
        let link_pkg_name;
        if pkg_meta.name.starts_with(self.get_prefix().as_str()) {
            link_pkg_name = pkg_meta.name.split(".").last().unwrap().to_string();
        } else {
            link_pkg_name = pkg_meta.name.clone();
        }

        if !self.is_latest_version(&pkg_id)? {
            return Ok(());
        }

        let friendly_path = self.work_dir.join(format!("./{}", link_pkg_name));

        if self.config.enable_link {
            if friendly_path.exists() {
                info!("remove friendly symlink: {}", friendly_path.display());
                let metadata = tokio::fs::symlink_metadata(&friendly_path).await?;
                if metadata.file_type().is_symlink() || metadata.is_file() {
                    tokio::fs::remove_file(&friendly_path).await?;
                } else {
                    tokio::fs::remove_dir_all(&friendly_path).await?;
                }
            }
            #[cfg(target_family = "unix")]
            tokio::fs::symlink(&synlink_target, &friendly_path).await?;
            #[cfg(target_family = "windows")]
            std::os::windows::fs::symlink_dir(&synlink_target, &friendly_path)?;
            info!(
                "create friendly symlink: {} -> {}",
                friendly_path.display(),
                synlink_target.as_str()
            );
        } else {
            warn!(
                "env {} does not support link mode, copying latest pkg {} to friendly path {}",
                self.work_dir.display(),
                pkg_meta.get_package_id().to_string(),
                friendly_path.display()
            );
            if friendly_path.exists() {
                info!("remove friendly dir: {}", friendly_path.display());
                let metadata = tokio::fs::symlink_metadata(&friendly_path).await?;
                if metadata.file_type().is_symlink() || metadata.is_file() {
                    tokio::fs::remove_file(&friendly_path).await?;
                } else {
                    tokio::fs::remove_dir_all(&friendly_path).await?;
                }
            }
            let target_dir = target_dir.clone();
            let friendly_path_clone = friendly_path.clone();
            tokio::task::spawn_blocking(move || {
                let mut options = CopyOptions::new();
                options.copy_inside = true;
                copy(&target_dir, &friendly_path_clone, &options)
            })
            .await
            .map_err(|e| PkgError::InstallError(pkg_id.to_string(), e.to_string()))?
            .map_err(|e| PkgError::InstallError(pkg_id.to_string(), e.to_string()))?;
            info!(
                "copy pkg {} to friendly path {} OK.",
                pkg_meta.name.as_str(),
                friendly_path.display()
            );
        }

        Ok(())
    }

    pub fn get_prefix(&self) -> String {
        if let Some(prefix) = &self.config.prefix {
            prefix.clone()
        } else {
            PackageEnvConfig::get_default_prefix()
        }
    }

    async fn load_strictly(&self, pkg_id_str: &str) -> PkgResult<MediaInfo> {
        let pkg_id = PackageId::parse(pkg_id_str)?;
        let pkg_id = self.prefix_pkg_id(&pkg_id);
        // 在严格模式下，先获取包的元数据以获得准确的物理目录
        let real_pkg_id = pkg_id.to_string();
        let (meta_obj_id, pkg_meta) = self.get_pkg_meta(&real_pkg_id).await?;

        // 使用元数据中的信息构建准确的物理路径
        let pkg_strict_dir = self.get_pkg_strict_dir(&meta_obj_id, &pkg_meta)?;

        if tokio_fs::metadata(&pkg_strict_dir).await.is_ok() {
            let metadata = tokio_fs::metadata(&pkg_strict_dir).await?;
            let media_type = if metadata.is_dir() {
                MediaType::Dir
            } else {
                MediaType::File
            };

            return Ok(MediaInfo {
                pkg_id,
                full_path: pkg_strict_dir,
                media_type,
            });
        }

        Err(PkgError::LoadError(
            pkg_id_str.to_owned(),
            "pkg not found in strict mode".to_owned(),
        ))
    }

    async fn dev_try_load(&self, pkg_id_str: &str) -> PkgResult<MediaInfo> {
        let pkg_dirs = self.get_pkg_dir(pkg_id_str)?;
        for pkg_dir in pkg_dirs {
            debug!("try load pkg {} from {}", pkg_id_str, pkg_dir.display());
            if tokio_fs::metadata(&pkg_dir).await.is_ok() {
                debug!("try load pkg {} from {} OK.", pkg_id_str, pkg_dir.display());
                return Ok(MediaInfo {
                    pkg_id: PackageId::parse(pkg_id_str)?,
                    full_path: pkg_dir,
                    media_type: MediaType::Dir,
                });
            }
        }
        Err(PkgError::LoadError(
            pkg_id_str.to_owned(),
            "Package not found".to_owned(),
        ))
    }

    fn get_install_dir(&self) -> PathBuf {
        self.work_dir.join("pkgs")
    }

    fn get_meta_db_path(&self) -> PathBuf {
        let mut meta_db_path;
        if let Some(index_db_path) = &self.config.index_db_path {
            meta_db_path = PathBuf::from(index_db_path);
        } else {
            meta_db_path = self.work_dir.join("pkgs/meta_index.db")
        }
        meta_db_path
    }

    fn get_pkg_strict_dir(&self, meta_obj_id: &str, pkg_meta: &PackageMeta) -> PkgResult<PathBuf> {
        let pkg_name = pkg_meta.name.clone();
        let real_obj_id = ObjId::new(meta_obj_id)
            .map_err(|e| PkgError::ParseError(meta_obj_id.to_string(), e.to_string()))?;
        //pkgs/pkg_nameA/$meta_obj_id
        Ok(self
            .get_install_dir()
            .join(pkg_name)
            .join(real_obj_id.to_filename()))
    }

    fn get_pkg_dir(&self, pkg_id: &str) -> PkgResult<Vec<PathBuf>> {
        let pkg_id = PackageId::parse(pkg_id)?;
        let pkg_name = pkg_id.name.clone();
        let mut pkg_dirs = Vec::new();

        if pkg_id.objid.is_some() {
            let obj_id = ObjId::new(pkg_id.objid.as_ref().unwrap())
                .map_err(|e| PkgError::ParseError(pkg_id.to_string(), e.to_string()))?;
            pkg_dirs.push(
                self.get_install_dir()
                    .join(pkg_name)
                    .join(obj_id.to_filename()),
            );
        } else {
            if pkg_id.version_exp.is_some() {
                //TODO: 要考虑如何结合lock文件进行查找
                pkg_dirs.push(self.get_install_dir().join(pkg_name));
            } else {
                pkg_dirs.push(self.work_dir.join(pkg_name));
            }
        }
        Ok(pkg_dirs)
    }

    fn write_pkg_meta_to_db(
        &self,
        meta_obj_id: &str,
        pkg_meta_str: &str,
        pkg_meta: &PackageMeta,
    ) -> PkgResult<()> {
        let meta_db = MetaIndexDb::new(self.get_meta_db_path(), false)?;
        meta_db.add_pkg_meta(meta_obj_id, pkg_meta_str, &pkg_meta.author, None)?;
        meta_db.set_pkg_version(
            &pkg_meta.name,
            &pkg_meta.author,
            &pkg_meta.version,
            meta_obj_id,
            pkg_meta.version_tag.as_deref(),
        )?;
        Ok(())
    }

    // 添加一个新的私有方法来管理锁文件
    async fn acquire_lock(&self) -> PkgResult<RwLockWriteGuard<File>> {
        let lock_path = self.work_dir.join("pkgs/env.lock");

        // 确保pkgs目录存在
        if let Err(e) = tokio_fs::create_dir_all(self.work_dir.join("pkgs")).await {
            return Err(PkgError::LockError(format!(
                "Failed to create lock directory: {}",
                e
            )));
        }

        // 以读写模式打开或创建锁文件
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .await
            .map_err(|e| PkgError::LockError(format!("Failed to open lock file: {}", e)))?;

        let lock = file.lock_write().await.map_err(|e| {
            PkgError::LockError(format!("Failed to open lock file: {:?}", lock_path))
        })?;
        Ok(lock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buckyos_kit::*;
    use name_lib::DID;
    use named_store::{NamedDataMgr, NamedLocalStore, StoreLayout, StoreTarget};
    use ndn_lib::{ChunkList, FileObject, ObjId, StoreMode, CHUNK_DEFAULT_SIZE};
    use ndn_toolkit::{cacl_file_object, CheckMode};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    async fn setup_test_env() -> (PackageEnv, tempfile::TempDir) {
        unsafe {
            std::env::set_var("BUCKY_LOG", "debug");
        }
        init_logging("test_package_lib", false);
        let temp_dir = tempdir().unwrap();
        let env = PackageEnv::new(temp_dir.path().to_path_buf());

        // 创建pkgs目录
        tokio_fs::create_dir_all(env.get_install_dir())
            .await
            .unwrap();

        (env, temp_dir)
    }

    async fn create_test_package(env: &PackageEnv, pkg_name: &str, version: &str) -> PathBuf {
        let pkg_dir = env
            .get_install_dir()
            .join(format!("{}#{}", pkg_name, version));
        tokio_fs::create_dir_all(&pkg_dir).await.unwrap();

        // 创建meta文件
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let mut meta = PackageMeta::new(pkg_name, version, "test", &owner, Some("test"));
        //meta.category = Some("test".to_string());
        //meta.chunk_url = Some("http://test.com".to_string());
        meta._base.size = 100;
        meta._base.content = "test_chunk".to_string();

        let meta_path = pkg_dir.join(".pkg.meta");
        tokio_fs::write(&meta_path, serde_json::to_string(&meta).unwrap())
            .await
            .unwrap();

        //TODO: modify meta_index.db

        pkg_dir
    }

    async fn create_test_store_mgr(base_dir: &Path) -> NamedDataMgr {
        let store = NamedLocalStore::get_named_store_by_path(base_dir.join("named_store"))
            .await
            .unwrap();
        let store_id = store.store_id().to_string();
        let store_ref = Arc::new(tokio::sync::Mutex::new(store));

        let store_mgr = NamedDataMgr::new();
        store_mgr.register_store(store_ref).await;
        store_mgr
            .add_layout(StoreLayout::new(
                1,
                vec![StoreTarget {
                    store_id,
                    device_did: String::new(),
                    capacity: None,
                    used: None,
                    readonly: false,
                    enabled: true,
                    weight: 1,
                }],
                0,
                0,
            ))
            .await;

        store_mgr
    }

    async fn create_test_pkg_archive(base_dir: &Path) -> PathBuf {
        let archive_path = base_dir.join("test-pkg.tar.gz");
        let file = File::create(&archive_path).await.unwrap();
        let encoder = async_compression::tokio::write::GzipEncoder::new(file);
        let mut builder = tokio_tar::Builder::new(encoder);

        let payload = b"hello from package";
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "bin/hello.txt", &payload[..])
            .await
            .unwrap();

        let mut encoder = builder.into_inner().await.unwrap();
        encoder.shutdown().await.unwrap();

        archive_path
    }

    async fn create_test_store_config(base_dir: &Path) -> PathBuf {
        let config_path = base_dir.join("named_store_config.json");
        let config = serde_json::json!({
            "epoch": 1,
            "stores": [
                {
                    "path": "named_store",
                    "readonly": false,
                    "enabled": true,
                    "weight": 1
                }
            ]
        });
        tokio_fs::write(&config_path, serde_json::to_vec(&config).unwrap())
            .await
            .unwrap();
        config_path
    }

    async fn create_installable_test_pkg(
        env: &mut PackageEnv,
        base_dir: &Path,
        pkg_name: &str,
        version: &str,
    ) -> PackageMeta {
        let store_mgr = create_test_store_mgr(base_dir).await;
        let store_config_path = create_test_store_config(base_dir).await;
        env.config.named_store_config_path = Some(store_config_path.to_string_lossy().to_string());
        env.config.http_backend_links = HashMap::new();

        let archive_path = create_test_pkg_archive(base_dir).await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let mut pkg_meta = PackageMeta::new(pkg_name, version, "test", &owner, None);
        let (file_obj, _, _) = cacl_file_object(
            Some(&store_mgr),
            &archive_path,
            &pkg_meta._base,
            false,
            &CheckMode::ByFullHash,
            StoreMode::StoreInNamedMgr,
            None,
        )
        .await
        .unwrap();
        pkg_meta.size = file_obj.size;
        pkg_meta.content = file_obj.content;

        pkg_meta
    }

    fn insert_pkg_meta_to_db(env: &PackageEnv, pkg_meta: &PackageMeta) -> ObjId {
        let meta_db = MetaIndexDb::new(env.get_meta_db_path(), false).unwrap();
        let (meta_obj_id, pkg_meta_str) = pkg_meta.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &meta_obj_id.to_string(),
                &pkg_meta_str,
                &pkg_meta.author,
                None,
            )
            .unwrap();
        meta_db
            .set_pkg_version(
                &pkg_meta.name,
                &pkg_meta.author,
                &pkg_meta.version,
                &meta_obj_id.to_string(),
                pkg_meta.version_tag.as_deref(),
            )
            .unwrap();
        meta_obj_id
    }

    #[tokio::test]
    async fn test_load_env_config() {
        let config_str = r#"
{"enable_link": true, "enable_strict_mode": true, "parent": "/opt/buckyos/local/node_daemon/root_pkg_env", "ready_only": false, "prefix": "nightly-linux-amd64"}
        "#;
        let config = serde_json::from_str::<PackageEnvConfig>(config_str).unwrap();
        println!("config: {:?}", config);
    }

    // #[tokio::test]
    async fn test_load_strictly() {
        let (env, _temp) = setup_test_env().await;

        // 创建测试包
        let pkg_dir = create_test_package(&env, "test-pkg", "1.0.0").await;

        // 测试严格模式加载
        let media_info = env.load_strictly("test-pkg#1.0.0").await.unwrap();
        assert_eq!(media_info.pkg_id.name, "test-pkg");
        assert_eq!(
            media_info.pkg_id.version_exp.as_ref().unwrap().to_string(),
            "1.0.0".to_string()
        );
        assert_eq!(media_info.full_path, pkg_dir);

        // 测试不存在的包
        assert!(env.load_strictly("not-exist#1.0.0").await.is_err());
    }

    //#[tokio::test]
    async fn test_try_load() {
        let (env, _temp) = setup_test_env().await;

        // 创建测试包
        let pkg_dir = create_test_package(&env, "test-pkg", "1.0.0").await;

        // 测试模糊版本匹配
        let media_info = env.dev_try_load("test-pkg").await.unwrap();
        assert_eq!(media_info.pkg_id.name, "test-pkg");
        assert_eq!(media_info.full_path, pkg_dir);

        // 测试不存在的包
        assert!(env.dev_try_load("not-exist#1.0.0").await.is_err());
    }

    // #[tokio::test]
    // async fn test_install_pkg() {
    //     let (env, _temp) = setup_test_env().await;

    //     // 创建测试包及其依赖
    //     create_test_package(&env, "test-pkg", "1.0.0").await;
    //     create_test_package(&env, "dep-pkg", "0.1.0").await;

    //     // 测试安装包(不包含依赖)
    //     let task_id = env.install_pkg("test-pkg#1.0.0", false).await.unwrap();
    //     assert_eq!(task_id, "test-pkg#1.0.0");

    //     // 等待任务完成
    //     env.wait_task(&task_id).await.unwrap();

    //     // 验证任务状态
    //     let tasks = env.install_tasks.lock().await;
    //     let task = tasks.get(&task_id).unwrap();
    //     assert!(matches!(task.status, InstallStatus::Completed));
    //     assert!(task.sub_tasks.is_empty());
    // }

    //#[tokio::test]
    async fn test_get_pkg_meta() {
        let (env, _temp) = setup_test_env().await;

        // 创建测试包
        create_test_package(&env, "test-pkg", "1.0.0").await;

        // 测试获取meta信息
        let (meta_obj_id, meta) = env.get_pkg_meta("test-pkg#1.0.0").await.unwrap();
        assert_eq!(meta.name, "test-pkg");
        assert_eq!(meta.version, "1.0.0".to_string());

        // 测试不存在的包
        assert!(env.get_pkg_meta("not-exist#1.0.0").await.is_err());
    }

    #[tokio::test]
    async fn test_install_pkg_from_local_file_can_load_strictly() {
        let (mut env, temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let pkg_meta_str = serde_json::to_string(&pkg_meta).unwrap();
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();
        let expected_dir = env
            .get_pkg_strict_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        let archive_path = create_test_pkg_archive(temp.path()).await;

        env.install_pkg_from_local_file(&pkg_meta_str, &archive_path)
            .await
            .unwrap();

        let media_info = env.load_strictly("test.pkg#1.0.0").await.unwrap();
        assert_eq!(media_info.full_path, expected_dir);
        assert!(tokio_fs::metadata(&expected_dir).await.unwrap().is_dir());
        assert_eq!(
            tokio_fs::read_to_string(expected_dir.join("bin/hello.txt"))
                .await
                .unwrap(),
            "hello from package"
        );
    }

    #[tokio::test]
    async fn test_load_in_non_strict_mode_switches_from_friendly_to_strict_after_indexed() {
        let (mut env, temp) = setup_test_env().await;
        let friendly_path = env.work_dir.join("test.pkg");
        tokio_fs::create_dir_all(&friendly_path).await.unwrap();
        tokio_fs::write(friendly_path.join("hello.txt"), "friendly")
            .await
            .unwrap();

        let media_info = env.load("test.pkg").await.unwrap();
        assert_eq!(media_info.full_path, friendly_path);
        assert!(matches!(media_info.media_type, MediaType::Dir));

        let pkg_meta =
            create_installable_test_pkg(&mut env, temp.path(), "test.pkg", "1.0.1").await;
        let (meta_obj_id, pkg_meta_str) = pkg_meta.gen_obj_id();
        let get_result = env
            .get_pkg_meta(&format!("test.pkg#{}", meta_obj_id.to_string()))
            .await;
        assert!(get_result.is_err());
        println!("get_result: {:?}", get_result);

        let meta_obj_id = insert_pkg_meta_to_db(&env, &pkg_meta);
        let strict_path = env
            .get_pkg_strict_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();

        let installed_meta_obj_id = env
            .install_pkg("test.pkg#1.0.1", false, false)
            .await
            .unwrap();
        assert_eq!(installed_meta_obj_id, meta_obj_id.to_string());

        let media_info = env.load("test.pkg").await.unwrap();
        assert_eq!(media_info.full_path, strict_path);
        assert!(matches!(media_info.media_type, MediaType::Dir));
        assert_eq!(
            tokio_fs::read_to_string(media_info.full_path.join("bin/hello.txt"))
                .await
                .unwrap(),
            "hello from package"
        );
    }

    #[tokio::test]
    async fn test_load_in_non_strict_mode_ignores_older_indexed_version_with_objid() {
        let (mut env, temp) = setup_test_env().await;
        let friendly_path = env.work_dir.join("test.pkg");
        tokio_fs::create_dir_all(&friendly_path).await.unwrap();
        tokio_fs::write(friendly_path.join("hello.txt"), "friendly")
            .await
            .unwrap();

        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta_v1 = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        assert!(pkg_meta_v1.content.is_empty());
        let meta_obj_id_v1 = insert_pkg_meta_to_db(&env, &pkg_meta_v1);

        let (resolved_meta_obj_id, resolved_meta) = env.get_pkg_meta("test.pkg").await.unwrap();
        assert_eq!(resolved_meta_obj_id, meta_obj_id_v1.to_string());
        assert_eq!(resolved_meta.version, "1.0.0");
        assert!(resolved_meta.content.is_empty());

        let media_info = env.load("test.pkg").await.unwrap();
        assert_eq!(media_info.full_path, friendly_path);
        assert!(matches!(media_info.media_type, MediaType::Dir));
        assert_eq!(
            tokio_fs::read_to_string(media_info.full_path.join("hello.txt"))
                .await
                .unwrap(),
            "friendly"
        );

        let old_pkg_base_dir = temp.path().join("pkg-0.9.0");
        tokio_fs::create_dir_all(&old_pkg_base_dir).await.unwrap();
        let old_pkg_meta =
            create_installable_test_pkg(&mut env, &old_pkg_base_dir, "test.pkg", "0.9.0").await;
        assert!(!old_pkg_meta.content.is_empty());
        let old_meta_obj_id = insert_pkg_meta_to_db(&env, &old_pkg_meta);
        assert_ne!(old_meta_obj_id, meta_obj_id_v1);

        let (resolved_meta_obj_id, resolved_meta) = env.get_pkg_meta("test.pkg").await.unwrap();
        assert_eq!(resolved_meta_obj_id, meta_obj_id_v1.to_string());
        assert_eq!(resolved_meta.version, "1.0.0");
        assert!(resolved_meta.content.is_empty());

        let media_info = env.load("test.pkg").await.unwrap();
        assert_eq!(media_info.full_path, friendly_path);
        assert!(matches!(media_info.media_type, MediaType::Dir));
        assert_eq!(
            tokio_fs::read_to_string(media_info.full_path.join("hello.txt"))
                .await
                .unwrap(),
            "friendly"
        );

        let new_pkg_base_dir = temp.path().join("pkg-1.0.1");
        tokio_fs::create_dir_all(&new_pkg_base_dir).await.unwrap();
        let new_pkg_meta =
            create_installable_test_pkg(&mut env, &new_pkg_base_dir, "test.pkg", "1.0.1").await;
        assert!(!new_pkg_meta.content.is_empty());
        let new_meta_obj_id = insert_pkg_meta_to_db(&env, &new_pkg_meta);
        let strict_path = env
            .get_pkg_strict_dir(&new_meta_obj_id.to_string(), &new_pkg_meta)
            .unwrap();

        let installed_meta_obj_id = env
            .install_pkg("test.pkg#1.0.1", false, false)
            .await
            .unwrap();
        assert_eq!(installed_meta_obj_id, new_meta_obj_id.to_string());

        let (resolved_meta_obj_id, resolved_meta) = env.get_pkg_meta("test.pkg").await.unwrap();
        assert_eq!(resolved_meta_obj_id, new_meta_obj_id.to_string());
        assert_eq!(resolved_meta.version, "1.0.1");
        assert_eq!(resolved_meta.content, new_pkg_meta.content);

        let media_info = env.load("test.pkg").await.unwrap();
        assert_eq!(media_info.full_path, strict_path);
        assert!(matches!(media_info.media_type, MediaType::Dir));
        assert_eq!(
            tokio_fs::read_to_string(media_info.full_path.join("bin/hello.txt"))
                .await
                .unwrap(),
            "hello from package"
        );
    }

    #[tokio::test]
    async fn test_load_by_meta_obj_id_is_exact() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta_v1 = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let pkg_meta_v2 = PackageMeta::new("test.pkg", "2.0.0", "test", &owner, None);

        let meta_obj_id_v1 = insert_pkg_meta_to_db(&env, &pkg_meta_v1);
        let meta_obj_id_v2 = insert_pkg_meta_to_db(&env, &pkg_meta_v2);
        //println!("meta_obj_id_v1: {}", meta_obj_id_v1.to_string());

        let strict_path_v1 = env
            .get_pkg_strict_dir(&meta_obj_id_v1.to_string(), &pkg_meta_v1)
            .unwrap();
        let strict_path_v2 = env
            .get_pkg_strict_dir(&meta_obj_id_v2.to_string(), &pkg_meta_v2)
            .unwrap();
        tokio_fs::create_dir_all(&strict_path_v1).await.unwrap();
        tokio_fs::create_dir_all(&strict_path_v2).await.unwrap();

        let media_info = env
            .load(&format!("test.pkg#{}", meta_obj_id_v1))
            .await
            .unwrap();
        assert_eq!(media_info.full_path, strict_path_v1);
        assert_ne!(media_info.full_path, strict_path_v2);

        let (meta_obj_id, pkg_meta) = env
            .get_pkg_meta(&format!("test.pkg#{}", meta_obj_id_v1.to_string()))
            .await
            .unwrap();
        assert_eq!(meta_obj_id, meta_obj_id_v1.to_string());
        assert_eq!(pkg_meta.version, "1.0.0");
    }

    #[tokio::test]
    async fn test_set_pkg_meta_to_index_db_persists_meta() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.2.3", "test", &owner, Some("stable"));
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();

        env.set_pkg_meta_to_index_db(&meta_obj_id.to_string(), &pkg_meta)
            .await
            .unwrap();

        let (stored_meta_obj_id, stored_meta) =
            env.get_pkg_meta("test.pkg#1.2.3:stable").await.unwrap();
        assert_eq!(stored_meta_obj_id, meta_obj_id.to_string());
        assert_eq!(stored_meta, pkg_meta);
    }

    #[tokio::test]
    async fn test_set_pkg_meta_to_index_db_rejects_read_only_env() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.ready_only = true;

        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.2.3", "test", &owner, None);
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();

        let err = env
            .set_pkg_meta_to_index_db(&meta_obj_id.to_string(), &pkg_meta)
            .await
            .err()
            .expect("read-only env should reject index updates");

        assert!(matches!(err, PkgError::InstallError(_, _)));
    }

    #[tokio::test]
    async fn test_check_pkg_ready_handles_chunklist_missing_chunks() {
        let (env, temp) = setup_test_env().await;
        let store_mgr = create_test_store_mgr(temp.path()).await;
        let file_path = temp.path().join("pkg.data");
        let file_bytes = vec![7u8; CHUNK_DEFAULT_SIZE as usize + 17];
        tokio_fs::write(&file_path, &file_bytes).await.unwrap();

        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let mut pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let (file_obj, _, _) = cacl_file_object(
            Some(&store_mgr),
            &file_path,
            &FileObject::default(),
            true,
            &CheckMode::ByFullHash,
            StoreMode::StoreInNamedMgr,
            None,
        )
        .await
        .unwrap();
        pkg_meta.size = file_obj.size;
        pkg_meta.content = file_obj.content.clone();

        let chunk_list = ChunkList::from_json(
            store_mgr
                .get_object(&ObjId::new(&pkg_meta.content).unwrap())
                .await
                .unwrap()
                .as_str(),
        )
        .unwrap();
        let missing_chunk = chunk_list.body[0].clone();
        store_mgr.remove_chunk(&missing_chunk).await.unwrap();

        let meta_db = MetaIndexDb::new(env.get_meta_db_path(), false).unwrap();
        let (meta_obj_id, pkg_meta_str) = pkg_meta.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &meta_obj_id.to_string(),
                &pkg_meta_str,
                &pkg_meta.author,
                None,
            )
            .unwrap();
        meta_db
            .set_pkg_version(
                &pkg_meta.name,
                &pkg_meta.author,
                &pkg_meta.version,
                &meta_obj_id.to_string(),
                pkg_meta.version_tag.as_deref(),
            )
            .unwrap();

        let mut missing_chunks = Vec::new();
        PackageEnv::check_pkg_ready(
            &env.get_meta_db_path(),
            "test.pkg#1.0.0",
            &store_mgr,
            &mut missing_chunks,
        )
        .await
        .unwrap();

        assert_eq!(missing_chunks, vec![missing_chunk]);
    }

    #[tokio::test]
    async fn test_check_deps_ready_only_checks_dependencies() {
        let (env, temp) = setup_test_env().await;
        let store_mgr = create_test_store_mgr(temp.path()).await;
        let file_path = temp.path().join("dep.data");
        let file_bytes = vec![9u8; CHUNK_DEFAULT_SIZE as usize + 23];
        tokio_fs::write(&file_path, &file_bytes).await.unwrap();

        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();

        let mut dep_meta = PackageMeta::new("dep.pkg", "1.0.0", "test", &owner, None);
        let (dep_file_obj, _, _) = cacl_file_object(
            Some(&store_mgr),
            &file_path,
            &FileObject::default(),
            true,
            &CheckMode::ByFullHash,
            StoreMode::StoreInNamedMgr,
            None,
        )
        .await
        .unwrap();
        dep_meta.size = dep_file_obj.size;
        dep_meta.content = dep_file_obj.content.clone();

        let chunk_list = ChunkList::from_json(
            store_mgr
                .get_object(&ObjId::new(&dep_meta.content).unwrap())
                .await
                .unwrap()
                .as_str(),
        )
        .unwrap();
        let missing_chunk = chunk_list.body[0].clone();
        store_mgr.remove_chunk(&missing_chunk).await.unwrap();

        let dep_meta_obj_id = insert_pkg_meta_to_db(&env, &dep_meta);

        let mut root_meta = PackageMeta::new("root.pkg", "1.0.0", "test", &owner, None);
        root_meta
            .deps
            .insert("dep.pkg".to_string(), "1.0.0".to_string());
        let _root_meta_obj_id = insert_pkg_meta_to_db(&env, &root_meta);

        let mut pkg_missing_chunks = Vec::new();
        PackageEnv::check_pkg_ready(
            &env.get_meta_db_path(),
            "root.pkg#1.0.0",
            &store_mgr,
            &mut pkg_missing_chunks,
        )
        .await
        .unwrap();
        assert!(pkg_missing_chunks.is_empty());

        let mut dep_missing_chunks = Vec::new();
        PackageEnv::check_deps_ready(
            &env.get_meta_db_path(),
            "root.pkg#1.0.0",
            &store_mgr,
            &mut dep_missing_chunks,
        )
        .await
        .unwrap();
        assert_eq!(dep_missing_chunks, vec![missing_chunk.clone()]);

        let mut dep_self_missing = Vec::new();
        PackageEnv::check_pkg_ready(
            &env.get_meta_db_path(),
            &format!("dep.pkg#1.0.0#{}", dep_meta_obj_id),
            &store_mgr,
            &mut dep_self_missing,
        )
        .await
        .unwrap();
        assert_eq!(dep_self_missing, vec![missing_chunk]);
    }

    #[tokio::test]
    async fn test_get_pkg_dir_with_objid_uses_filename_once() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();
        let pkg_dirs = env
            .get_pkg_dir(&format!("test.pkg#1.0.0#{}", meta_obj_id))
            .unwrap();

        assert_eq!(
            pkg_dirs,
            vec![env
                .get_install_dir()
                .join("test.pkg")
                .join(meta_obj_id.to_filename())]
        );
    }

    #[tokio::test]
    async fn test_cacl_pkg_deps_metas_detects_cycles() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();

        let mut pkg_a = PackageMeta::new("cycle.a", "1.0.0", "test", &owner, None);
        let mut pkg_b = PackageMeta::new("cycle.b", "1.0.0", "test", &owner, None);
        pkg_a
            .deps
            .insert("cycle.b".to_string(), "1.0.0".to_string());
        pkg_b
            .deps
            .insert("cycle.a".to_string(), "1.0.0".to_string());

        insert_pkg_meta_to_db(&env, &pkg_a);
        insert_pkg_meta_to_db(&env, &pkg_b);

        let mut deps = HashMap::new();
        let err = env
            .cacl_pkg_deps_metas(&pkg_a, &mut deps)
            .await
            .err()
            .expect("dependency cycle should be rejected");
        assert!(matches!(err, PkgError::LoadError(_, _)));
    }

    #[tokio::test]
    async fn test_do_install_pkg_from_data_only_creates_unversioned_friendly_path() {
        let (env, temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let meta_obj_id = insert_pkg_meta_to_db(&env, &pkg_meta);
        let strict_dir = env
            .get_pkg_strict_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        let archive_path = create_test_pkg_archive(temp.path()).await;
        let file = File::open(&archive_path).await.unwrap();
        let chunk_reader: ChunkReader = Box::pin(file);

        env.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await
            .unwrap();

        let friendly_path = env.work_dir.join("test.pkg");
        let old_versioned_path = env.work_dir.join("test.pkg#1.0.0");
        assert!(tokio_fs::metadata(&strict_dir).await.unwrap().is_dir());
        assert!(tokio_fs::symlink_metadata(&friendly_path).await.is_ok());
        assert!(tokio_fs::metadata(&friendly_path).await.unwrap().is_dir());
        assert!(tokio_fs::symlink_metadata(&old_versioned_path)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_do_install_pkg_from_data_copy_friendly_path_when_link_disabled() {
        let (mut env, temp) = setup_test_env().await;
        env.config.enable_link = false;

        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let meta_obj_id = insert_pkg_meta_to_db(&env, &pkg_meta);
        let strict_dir = env
            .get_pkg_strict_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        let archive_path = create_test_pkg_archive(temp.path()).await;
        let file = File::open(&archive_path).await.unwrap();
        let chunk_reader: ChunkReader = Box::pin(file);

        env.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await
            .unwrap();

        let friendly_path = env.work_dir.join("test.pkg");
        assert!(tokio_fs::metadata(&strict_dir).await.unwrap().is_dir());
        assert!(tokio_fs::metadata(&friendly_path).await.unwrap().is_dir());

        tokio_fs::write(strict_dir.join("bin/hello.txt"), "strict only")
            .await
            .unwrap();
        assert_eq!(
            tokio_fs::read_to_string(friendly_path.join("bin/hello.txt"))
                .await
                .unwrap(),
            "hello from package"
        );
        assert!(tokio_fs::symlink_metadata(&friendly_path)
            .await
            .unwrap()
            .file_type()
            .is_dir());
    }

    #[tokio::test]
    async fn test_try_update_index_db() {
        let (env, temp) = setup_test_env().await;

        // 创建测试数据库文件
        let new_db_path = temp.path().join("new_index.db");
        tokio_fs::write(&new_db_path, "test data").await.unwrap();

        // 测试更新数据库
        env.try_update_index_db(&new_db_path).await.unwrap();

        // 验证更新结果
        let db_path = env.work_dir.join("pkgs/meta_index.db");
        assert!(tokio_fs::metadata(&db_path).await.is_ok());
        assert_eq!(
            tokio_fs::read_to_string(db_path).await.unwrap(),
            "test data"
        );
    }
}
