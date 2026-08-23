/*
PackageEnv layout and loading summary

- `work_dir/pkg.cfg.json`: environment configuration
- `work_dir/pkgs/env.lock`: global write lock for install/index updates
- `work_dir/pkgs/meta_index.db`: package metadata index
- `work_dir/pkgs/<pkg_name>/<meta_obj_id.to_filename()>`: installed object directory
- `work_dir/<unique_name>`: developer/friendly directory

`load()` parses the request and builds package-name candidates once. It then selects
exactly one current-environment resolver:

- `enable_meta_db = true`: resolve exact metadata from the existing read-only index,
  validate it, and load the installed object directory.
- `enable_meta_db = false`: never access the index; load an unversioned friendly
  directory or an explicit ObjId object directory.

If the current environment fails and a parent is configured, the parent parses the
original request and selects a resolver from its own configuration.

Other major flows:

- `get_pkg_meta(pkg_id)` checks the in-process lock DB, the current index, then parent.

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
  - 将 tar.gz reader 解压到 ObjId 对应的对象目录
  - 若当前包是 latest，再根据 `enable_link` 维护 `work_dir/<unique_name>` 友好路径

- `try_update_index_db(new_index_db)`
  - 获取写锁
  - 备份旧索引
  - 覆盖为新索引
*/

use async_trait::async_trait;
use fs_extra::dir::*;
use log::*;
use name_lib::{EncodedDocument, DID};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once};
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

#[derive(Debug, Clone, Serialize)]
pub struct PackageEnvConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>, //如果指定了，那么加载无 . 的pkg_name时，会自动补上prefix,变成加载 $prefix.$pkg_name
    pub enable_link: bool,
    pub enable_meta_db: bool,
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
        let env_str = env!("PACKAGE_DEFAULT_PREFIX").to_string();
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
            enable_meta_db: false,
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

#[derive(Deserialize)]
#[serde(default)]
struct PackageEnvConfigWire {
    prefix: Option<String>,
    enable_link: bool,
    enable_meta_db: Option<bool>,
    #[serde(rename = "enable_strict_mode")]
    legacy_meta_db_mode: Option<bool>,
    index_db_path: Option<String>,
    parent: Option<PathBuf>,
    ready_only: bool,
    named_store_config_path: Option<String>,
    http_backend_links: HashMap<String, String>,
    installed: HashSet<String>,
}

impl Default for PackageEnvConfigWire {
    fn default() -> Self {
        let defaults = PackageEnvConfig::default();
        Self {
            prefix: defaults.prefix,
            enable_link: defaults.enable_link,
            enable_meta_db: None,
            legacy_meta_db_mode: None,
            index_db_path: defaults.index_db_path,
            parent: defaults.parent,
            ready_only: defaults.ready_only,
            named_store_config_path: defaults.named_store_config_path,
            http_backend_links: defaults.http_backend_links,
            installed: defaults.installed,
        }
    }
}

impl<'de> Deserialize<'de> for PackageEnvConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PackageEnvConfigWire::deserialize(deserializer)?;
        let enable_meta_db = match (wire.enable_meta_db, wire.legacy_meta_db_mode) {
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "enable_meta_db conflicts with deprecated enable_strict_mode",
                ));
            }
            (Some(value), None) => value,
            (None, Some(value)) => {
                static LEGACY_MODE_WARNING: Once = Once::new();
                LEGACY_MODE_WARNING.call_once(|| {
                    warn!(
                        "package config field enable_strict_mode is deprecated; use enable_meta_db"
                    );
                });
                value
            }
            (None, None) => false,
        };
        if let Some(prefix) = wire.prefix.as_deref() {
            PackagePrefix::parse(prefix).map_err(serde::de::Error::custom)?;
        }

        Ok(Self {
            prefix: wire.prefix,
            enable_link: wire.enable_link,
            enable_meta_db,
            index_db_path: wire.index_db_path,
            parent: wire.parent,
            ready_only: wire.ready_only,
            named_store_config_path: wire.named_store_config_path,
            http_backend_links: wire.http_backend_links,
            installed: wire.installed,
        })
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
    lock_db: Arc<TokioMutex<Option<HashMap<String, (String, PackageMeta)>>>>,
}

impl PackageEnv {
    pub fn new(work_dir: PathBuf) -> Self {
        let config_path = work_dir.join("pkg.cfg.json");
        let mut env_config = PackageEnvConfig::default();
        if config_path.exists() {
            let config = std::fs::read_to_string(config_path);
            if config.is_ok() {
                let config = config.unwrap();
                let config_result = serde_json::from_str(&config);
                if config_result.is_ok() {
                    env_config = config_result.unwrap();
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
            lock_db: Arc::new(TokioMutex::new(None)),
        }
    }

    pub fn is_meta_db_enabled(&self) -> bool {
        self.config.enable_meta_db
    }

    pub fn update_config_file(&self, config: &PackageEnvConfig) -> PkgResult<()> {
        if let Some(prefix) = config.prefix.as_deref() {
            PackagePrefix::parse(prefix)?;
        }
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
        let requested = PackageId::parse(pkg_id)?;
        let candidates = self.package_id_candidates(&requested)?;
        let current_result = self.get_pkg_meta_from_current_env(&candidates).await;
        if current_result.is_ok() {
            return current_result;
        }

        if Self::is_resolution_not_found(current_result.as_ref().unwrap_err()) {
            if let Some(parent_path) = &self.config.parent {
                let parent_env = PackageEnv::new(parent_path.clone());
                return Box::pin(parent_env.get_pkg_meta(pkg_id)).await;
            }
        }

        current_result
    }

    fn is_resolution_not_found(error: &PkgError) -> bool {
        matches!(
            error,
            PkgError::FileNotFoundError(_) | PkgError::VersionNotFoundError(_)
        ) || matches!(
            error,
            PkgError::MetaDbError(message) if message.starts_with("metadata DB does not exist:")
        )
    }

    fn package_id_candidates(&self, pkg_id: &PackageId) -> PkgResult<Vec<PackageId>> {
        let prefix = PackagePrefix::parse(&self.get_prefix())?;
        pkg_id.load_candidates(&prefix)
    }

    async fn get_pkg_meta_from_current_env(
        &self,
        candidates: &[PackageId],
    ) -> PkgResult<(String, PackageMeta)> {
        if let Some(lock_db) = self.lock_db.lock().await.as_ref() {
            for candidate in candidates {
                if let Some((meta_obj_id, meta)) = lock_db.get(&candidate.to_string()) {
                    Self::validate_resolved_meta(candidate, meta_obj_id, meta)?;
                    return Ok((meta_obj_id.clone(), meta.clone()));
                }
            }
        }

        let meta_db = MetaIndexDb::open_existing_readonly(self.get_meta_db_path())?;
        for candidate in candidates {
            if let Some((meta_obj_id, pkg_meta)) = meta_db.get_pkg_meta(&candidate.to_string())? {
                Self::validate_resolved_meta(candidate, &meta_obj_id, &pkg_meta)?;
                return Ok((meta_obj_id, pkg_meta));
            }
        }

        Err(PkgError::FileNotFoundError(format!(
            "package metadata not found: {}",
            candidates
                .first()
                .map(ToString::to_string)
                .unwrap_or_default()
        )))
    }

    fn validate_resolved_meta(
        request: &PackageId,
        meta_obj_id: &str,
        pkg_meta: &PackageMeta,
    ) -> PkgResult<()> {
        if pkg_meta.name != request.name {
            return Err(PkgError::LoadError(
                request.to_string(),
                format!(
                    "metadata package name mismatch: expected {}, got {}",
                    request.name, pkg_meta.name
                ),
            ));
        }

        if let Some(version_exp) = &request.version_exp {
            if let Some(expected_tag) = version_exp.tag.as_deref() {
                if pkg_meta.version_tag.as_deref() != Some(expected_tag) {
                    return Err(PkgError::LoadError(
                        request.to_string(),
                        format!(
                            "metadata tag mismatch: expected {}, got {:?}",
                            expected_tag, pkg_meta.version_tag
                        ),
                    ));
                }
            }

            let version_matches = match &version_exp.version_exp {
                VersionExpType::None => true,
                VersionExpType::Version(expected) => {
                    VersionExp::compare_versions(&pkg_meta.version, &expected.to_string())
                        == std::cmp::Ordering::Equal
                }
                VersionExpType::Req(requirement) => semver::Version::parse(&pkg_meta.version)
                    .map(|version| requirement.matches(&version))
                    .unwrap_or(false),
            };
            if !version_matches {
                return Err(PkgError::LoadError(
                    request.to_string(),
                    format!(
                        "metadata version mismatch: request {}, got {}",
                        version_exp.to_string(),
                        pkg_meta.version
                    ),
                ));
            }
        }

        let (calculated_obj_id, _) = pkg_meta.gen_obj_id();
        let indexed_obj_id = ObjId::new(meta_obj_id).map_err(|e| {
            PkgError::LoadError(
                request.to_string(),
                format!("invalid metadata ObjId {}: {}", meta_obj_id, e),
            )
        })?;
        if calculated_obj_id != indexed_obj_id {
            return Err(PkgError::LoadError(
                request.to_string(),
                format!(
                    "metadata ObjId mismatch: index {}, calculated {}",
                    indexed_obj_id, calculated_obj_id
                ),
            ));
        }

        if let Some(expected_obj_id) = request.objid.as_deref() {
            let expected_obj_id = ObjId::new(expected_obj_id).map_err(|e| {
                PkgError::ParseError(request.to_string(), format!("invalid ObjId: {}", e))
            })?;
            if expected_obj_id != indexed_obj_id {
                return Err(PkgError::LoadError(
                    request.to_string(),
                    format!(
                        "metadata ObjId mismatch: expected {}, got {}",
                        expected_obj_id, indexed_obj_id
                    ),
                ));
            }
        }

        Ok(())
    }

    //加载pkg,加载成功说明pkg已经安装
    pub async fn load(&self, pkg_id_str: &str) -> PkgResult<MediaInfo> {
        let requested = PackageId::parse(pkg_id_str)?;
        let candidates = self.package_id_candidates(&requested)?;
        let current_result = if self.is_meta_db_enabled() {
            self.load_from_meta_db(&candidates).await
        } else {
            self.load_from_directory(&candidates).await
        };
        if current_result.is_ok() {
            return current_result;
        }

        if Self::is_resolution_not_found(current_result.as_ref().unwrap_err()) {
            if let Some(parent_path) = &self.config.parent {
                let parent_env = PackageEnv::new(parent_path.clone());
                return Box::pin(parent_env.load(pkg_id_str)).await;
            }
        }

        current_result
    }

    pub async fn cacl_pkg_deps_metas(
        &self,
        pkg_meta: &PackageMeta,
        deps: &mut HashMap<String, PackageMeta>,
    ) -> PkgResult<()> {
        let mut visiting = HashSet::new();
        visiting.insert(pkg_meta.try_get_package_id()?.to_string());
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
            let dep_pkg_id = dep_meta.try_get_package_id()?.to_string();
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
        let meta_db = MetaIndexDb::open_existing_readonly(meta_index_db)?;
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
        let meta_db = MetaIndexDb::open_existing_readonly(meta_index_db)?;
        let meta_info = meta_db.get_pkg_meta(pkg_id)?;
        if meta_info.is_none() {
            return Err(PkgError::LoadError(
                pkg_id.to_owned(),
                "Package metadata not found".to_owned(),
            ));
        }

        let (_, pkg_meta) = meta_info.unwrap();
        let mut visiting = HashSet::new();
        visiting.insert(pkg_meta.try_get_package_id()?.to_string());
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
        let meta_db = MetaIndexDb::open_existing_readonly(meta_index_db)?;

        for (dep_name, dep_version) in pkg_meta.deps.iter() {
            let dep_id = format!("{}#{}", dep_name, dep_version);
            let meta_info = meta_db.get_pkg_meta(&dep_id)?;
            let Some((_, dep_meta)) = meta_info else {
                return Err(PkgError::LoadError(
                    dep_id,
                    "Package metadata not found".to_owned(),
                ));
            };

            let dep_pkg_id = dep_meta.try_get_package_id()?.to_string();
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

        // Validate the replacement before touching the deployed index. This opens
        // the source read-only and cannot turn a missing file into an empty DB.
        MetaIndexDb::open_existing_readonly(new_index_db)?;

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
        let pkg_id = pkg_meta.try_get_package_id()?;

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
            pkg_id.to_string()
        );
        Ok(())
    }

    async fn install_pkg_impl(
        &mut self,
        meta_obj_id: &str,
        pkg_meta: &PackageMeta,
        force_install: bool,
    ) -> PkgResult<()> {
        let pkg_id = pkg_meta.try_get_package_id()?.to_string();
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

        // Local installation is an index update flow. Create the writable DB
        // explicitly before extraction code checks whether this is the latest package.
        MetaIndexDb::create_or_open(self.get_meta_db_path())?;

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

        let will_install_pkg_id = pkg_meta.try_get_package_id()?;
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
        let meta_db = MetaIndexDb::open_existing_readonly(self.get_meta_db_path())?;
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
        // 将 tar.gz reader 解压到 ObjId 对应的对象目录。
        // 若该包是 latest，再额外维护单一友好路径 `work_dir/<unique_name>`。
        let package_name = PackageName::parse(&pkg_meta.name)?;
        let pkg_id = pkg_meta.try_get_package_id()?;
        info!("extract pkg {} from chunk", package_name);

        let buf_reader = BufReader::new(chunk_reader);
        let gz_decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(gz_decoder);
        let object_dir_name = meta_obj_id.to_filename();
        let synlink_target = format!("./pkgs/{}/{}", package_name, object_dir_name);
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

        let link_pkg_name = package_name.unique_name;

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
                pkg_id.to_string(),
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

    async fn load_from_meta_db(&self, candidates: &[PackageId]) -> PkgResult<MediaInfo> {
        let (meta_obj_id, pkg_meta) = self.get_pkg_meta_from_current_env(candidates).await?;
        let resolved_pkg_id = candidates
            .iter()
            .find(|candidate| candidate.name == pkg_meta.name)
            .cloned()
            .ok_or_else(|| {
                PkgError::LoadError(
                    pkg_meta.name.clone(),
                    "metadata returned an unexpected package name".to_owned(),
                )
            })?;
        let pkg_object_dir = self.get_pkg_object_dir(&meta_obj_id, &pkg_meta)?;

        match tokio_fs::metadata(&pkg_object_dir).await {
            Ok(metadata) => Ok(MediaInfo {
                pkg_id: resolved_pkg_id,
                full_path: pkg_object_dir,
                media_type: if metadata.is_dir() {
                    MediaType::Dir
                } else {
                    MediaType::File
                },
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(PkgError::FileNotFoundError(format!(
                    "package object directory does not exist: {}",
                    pkg_object_dir.display()
                )))
            }
            Err(error) => Err(PkgError::IOError(error)),
        }
    }

    async fn load_from_directory(&self, candidates: &[PackageId]) -> PkgResult<MediaInfo> {
        let requested = candidates.first().ok_or_else(|| {
            PkgError::LoadError(String::new(), "package candidate list is empty".to_owned())
        })?;

        if requested.version_exp.is_some() {
            return Err(PkgError::VersionError(format!(
                "directory loading cannot resolve or validate version/tag request: {}",
                requested.to_string()
            )));
        }

        for candidate in candidates {
            let pkg_path = if let Some(obj_id) = candidate.objid.as_deref() {
                let obj_id = ObjId::new(obj_id).map_err(|e| {
                    PkgError::ParseError(candidate.to_string(), format!("invalid ObjId: {}", e))
                })?;
                self.get_install_dir()
                    .join(&candidate.name)
                    .join(obj_id.to_filename())
            } else {
                self.work_dir.join(&candidate.name)
            };

            debug!(
                "try directory package candidate {} from {}",
                candidate.to_string(),
                pkg_path.display()
            );
            match tokio_fs::metadata(&pkg_path).await {
                Ok(metadata) => {
                    return Ok(MediaInfo {
                        pkg_id: candidate.clone(),
                        full_path: pkg_path,
                        media_type: if metadata.is_dir() {
                            MediaType::Dir
                        } else {
                            MediaType::File
                        },
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(PkgError::IOError(error)),
            }
        }

        Err(PkgError::FileNotFoundError(format!(
            "package directory not found for {}",
            requested.to_string()
        )))
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

    fn get_pkg_object_dir(&self, meta_obj_id: &str, pkg_meta: &PackageMeta) -> PkgResult<PathBuf> {
        let pkg_name = PackageName::parse(&pkg_meta.name)?.to_string();
        let real_obj_id = ObjId::new(meta_obj_id)
            .map_err(|e| PkgError::ParseError(meta_obj_id.to_string(), e.to_string()))?;
        //pkgs/pkg_nameA/$meta_obj_id
        Ok(self
            .get_install_dir()
            .join(pkg_name)
            .join(real_obj_id.to_filename()))
    }

    fn write_pkg_meta_to_db(
        &self,
        meta_obj_id: &str,
        pkg_meta_str: &str,
        pkg_meta: &PackageMeta,
    ) -> PkgResult<()> {
        pkg_meta.try_get_package_id()?;
        let meta_db = MetaIndexDb::create_or_open(self.get_meta_db_path())?;
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

    fn insert_pkg_meta_to_db(env: &PackageEnv, pkg_meta: &PackageMeta) -> ObjId {
        let meta_db = MetaIndexDb::create_or_open(env.get_meta_db_path()).unwrap();
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

    async fn create_indexed_object_dir(env: &PackageEnv, pkg_meta: &PackageMeta) -> PathBuf {
        let meta_obj_id = insert_pkg_meta_to_db(env, pkg_meta);
        let object_dir = env
            .get_pkg_object_dir(&meta_obj_id.to_string(), pkg_meta)
            .unwrap();
        tokio_fs::create_dir_all(&object_dir).await.unwrap();
        object_dir
    }

    #[test]
    fn test_package_env_config_migration_and_conflict() {
        let legacy = serde_json::from_str::<PackageEnvConfig>(
            r#"{"enable_strict_mode":true,"prefix":"nightly-linux-amd64"}"#,
        )
        .unwrap();
        assert!(legacy.enable_meta_db);

        let serialized = serde_json::to_value(&legacy).unwrap();
        assert_eq!(serialized["enable_meta_db"], true);
        assert!(serialized.get("enable_strict_mode").is_none());

        let conflict = serde_json::from_str::<PackageEnvConfig>(
            r#"{"enable_meta_db":true,"enable_strict_mode":false}"#,
        );
        assert!(conflict.is_err());

        let current = serde_json::from_str::<PackageEnvConfig>(
            r#"{"enable_meta_db":false,"prefix":"nightly-linux-amd64"}"#,
        )
        .unwrap();
        assert!(!current.enable_meta_db);

        assert!(
            serde_json::from_str::<PackageEnvConfig>(r#"{"prefix":"nightly-freebsd-amd64"}"#)
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_meta_db_load_success_and_missing_object_directory() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, Some("stable"));
        let object_dir = create_indexed_object_dir(&env, &pkg_meta).await;

        let loaded = env.load("test.pkg#1.0.0:stable").await.unwrap();
        assert_eq!(loaded.full_path, object_dir);

        tokio_fs::remove_dir_all(&object_dir).await.unwrap();
        let friendly_dir = env.work_dir.join("test.pkg");
        tokio_fs::create_dir_all(&friendly_dir).await.unwrap();
        let error = env.load("test.pkg#1.0.0:stable").await.unwrap_err();
        assert!(matches!(error, PkgError::FileNotFoundError(_)));
    }

    #[tokio::test]
    async fn test_meta_db_load_missing_db_does_not_create_or_fallback() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        let db_path = env.get_meta_db_path();
        let friendly_dir = env.work_dir.join("test.pkg");
        tokio_fs::create_dir_all(&friendly_dir).await.unwrap();

        let error = env.load("test.pkg").await.unwrap_err();
        assert!(matches!(error, PkgError::MetaDbError(_)));
        assert!(!db_path.exists());
    }

    #[tokio::test]
    async fn test_meta_db_load_rejects_corrupt_and_incompatible_db() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        let db_path = env.get_meta_db_path();
        tokio_fs::write(&db_path, b"not a sqlite database")
            .await
            .unwrap();
        assert!(matches!(
            env.load("test.pkg").await.unwrap_err(),
            PkgError::MetaDbError(_)
        ));

        tokio_fs::remove_file(&db_path).await.unwrap();
        rusqlite::Connection::open(&db_path)
            .unwrap()
            .execute("CREATE TABLE pkg_metas (metaobjid TEXT PRIMARY KEY)", [])
            .unwrap();
        assert!(matches!(
            env.load("test.pkg").await.unwrap_err(),
            PkgError::MetaDbError(_)
        ));
    }

    #[tokio::test]
    async fn test_meta_db_not_found_does_not_check_friendly_directory() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        MetaIndexDb::create_or_open(env.get_meta_db_path()).unwrap();
        tokio_fs::create_dir_all(env.work_dir.join("test.pkg"))
            .await
            .unwrap();

        assert!(matches!(
            env.load("test.pkg").await.unwrap_err(),
            PkgError::FileNotFoundError(_)
        ));
    }

    #[tokio::test]
    async fn test_meta_db_load_rejects_mismatched_metadata_fields() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let meta_db = MetaIndexDb::create_or_open(env.get_meta_db_path()).unwrap();

        let wrong_name = PackageMeta::new("other.pkg", "1.0.0", "test", &owner, Some("stable"));
        let (wrong_name_id, wrong_name_json) = wrong_name.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &wrong_name_id.to_string(),
                &wrong_name_json,
                &wrong_name.author,
                None,
            )
            .unwrap();
        meta_db
            .set_pkg_version(
                "test.pkg",
                "test",
                "1.0.0",
                &wrong_name_id.to_string(),
                Some("stable"),
            )
            .unwrap();
        assert!(matches!(
            env.load("test.pkg#1.0.0:stable").await.unwrap_err(),
            PkgError::LoadError(_, _)
        ));

        let wrong_version =
            PackageMeta::new("version.pkg", "2.0.0", "test", &owner, Some("stable"));
        let (wrong_version_id, wrong_version_json) = wrong_version.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &wrong_version_id.to_string(),
                &wrong_version_json,
                &wrong_version.author,
                None,
            )
            .unwrap();
        meta_db
            .set_pkg_version(
                "version.pkg",
                "test",
                "1.0.0",
                &wrong_version_id.to_string(),
                Some("stable"),
            )
            .unwrap();
        assert!(matches!(
            env.load("version.pkg#1.0.0:stable").await.unwrap_err(),
            PkgError::LoadError(_, _)
        ));

        let wrong_tag = PackageMeta::new("tag.pkg", "1.0.0", "test", &owner, Some("beta"));
        let (wrong_tag_id, wrong_tag_json) = wrong_tag.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &wrong_tag_id.to_string(),
                &wrong_tag_json,
                &wrong_tag.author,
                None,
            )
            .unwrap();
        meta_db
            .set_pkg_version(
                "tag.pkg",
                "test",
                "1.0.0",
                &wrong_tag_id.to_string(),
                Some("stable"),
            )
            .unwrap();
        assert!(matches!(
            env.load("tag.pkg#1.0.0:stable").await.unwrap_err(),
            PkgError::LoadError(_, _)
        ));

        let indexed_meta = PackageMeta::new("obj.pkg", "1.0.0", "test", &owner, None);
        let replacement_meta = PackageMeta::new("obj.pkg", "1.0.1", "test", &owner, None);
        let (indexed_obj_id, _) = indexed_meta.gen_obj_id();
        let (_, replacement_json) = replacement_meta.gen_obj_id();
        meta_db
            .add_pkg_meta(
                &indexed_obj_id.to_string(),
                &replacement_json,
                &replacement_meta.author,
                None,
            )
            .unwrap();
        assert!(matches!(
            env.load(&format!("obj.pkg#{}", indexed_obj_id.to_string()))
                .await
                .unwrap_err(),
            PkgError::LoadError(_, _)
        ));
    }

    #[tokio::test]
    async fn test_directory_load_ignores_db_and_rejects_version_resolution() {
        let (env, _temp) = setup_test_env().await;
        let db_path = env.get_meta_db_path();
        tokio_fs::write(&db_path, b"corrupt DB that must remain unopened")
            .await
            .unwrap();
        let friendly_dir = env.work_dir.join("test.pkg");
        tokio_fs::create_dir_all(&friendly_dir).await.unwrap();

        let loaded = env.load("test.pkg").await.unwrap();
        assert_eq!(loaded.full_path, friendly_dir);
        for request in ["test.pkg#1.0.0", "test.pkg#>=1.0.0", "test.pkg#:latest"] {
            assert!(matches!(
                env.load(request).await.unwrap_err(),
                PkgError::VersionError(_)
            ));
        }
        assert_eq!(
            tokio_fs::read(&db_path).await.unwrap(),
            b"corrupt DB that must remain unopened"
        );
    }

    #[tokio::test]
    async fn test_directory_load_by_obj_id_is_exact() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();
        let object_dir = env
            .get_pkg_object_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        tokio_fs::create_dir_all(&object_dir).await.unwrap();

        let loaded = env
            .load(&format!("test.pkg#{}", meta_obj_id.to_string()))
            .await
            .unwrap();
        assert_eq!(loaded.full_path, object_dir);
        assert!(matches!(
            env.load("missing.pkg").await.unwrap_err(),
            PkgError::FileNotFoundError(_)
        ));
    }

    #[tokio::test]
    async fn test_modes_share_candidate_order_and_prefixed_package_id() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.prefix = Some("nightly-linux-amd64".to_owned());
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let prefixed_name = "nightly-linux-amd64.demo";
        let generic_dir = env.work_dir.join("demo");
        let prefixed_dir = env.work_dir.join(prefixed_name);
        tokio_fs::create_dir_all(&generic_dir).await.unwrap();
        tokio_fs::create_dir_all(&prefixed_dir).await.unwrap();

        let directory_loaded = env.load("demo").await.unwrap();
        assert_eq!(directory_loaded.pkg_id.name, prefixed_name);
        assert_eq!(directory_loaded.full_path, prefixed_dir);
        assert_eq!(
            env.load(prefixed_name).await.unwrap().full_path,
            prefixed_dir
        );

        let pkg_meta = PackageMeta::new(prefixed_name, "1.0.0", "test", &owner, None);
        let object_dir = create_indexed_object_dir(&env, &pkg_meta).await;
        env.config.enable_meta_db = true;
        let db_loaded = env.load("demo").await.unwrap();
        assert_eq!(db_loaded.pkg_id.name, prefixed_name);
        assert_eq!(db_loaded.full_path, object_dir);
        assert_eq!(env.load(prefixed_name).await.unwrap().full_path, object_dir);

        let generic_friendly_dir = env.work_dir.join("fallback");
        tokio_fs::create_dir_all(&generic_friendly_dir)
            .await
            .unwrap();
        let generic_meta = PackageMeta::new("fallback", "1.0.0", "test", &owner, None);
        let generic_object_dir = create_indexed_object_dir(&env, &generic_meta).await;
        assert_eq!(
            env.load("fallback").await.unwrap().full_path,
            generic_object_dir
        );
        env.config.enable_meta_db = false;
        assert_eq!(
            env.load("fallback").await.unwrap().full_path,
            generic_friendly_dir
        );

        let explicit = PackageId::parse(&format!("{}#1.0.0", prefixed_name)).unwrap();
        let before = env.package_id_candidates(&explicit).unwrap();
        env.config.enable_meta_db = true;
        let after = env.package_id_candidates(&explicit).unwrap();
        assert_eq!(before, after);
        assert_eq!(before, vec![explicit]);
    }

    #[tokio::test]
    async fn test_parent_uses_its_own_meta_db_mode() {
        let child_temp = tempdir().unwrap();
        let parent_temp = tempdir().unwrap();
        let mut parent_config = PackageEnvConfig::default();
        parent_config.enable_meta_db = true;
        tokio_fs::write(
            parent_temp.path().join("pkg.cfg.json"),
            serde_json::to_vec(&parent_config).unwrap(),
        )
        .await
        .unwrap();
        let parent_env = PackageEnv::new(parent_temp.path().to_path_buf());
        tokio_fs::create_dir_all(parent_env.get_install_dir())
            .await
            .unwrap();
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("parent.pkg", "1.0.0", "test", &owner, None);
        let object_dir = create_indexed_object_dir(&parent_env, &pkg_meta).await;

        let mut child_env = PackageEnv::new(child_temp.path().to_path_buf());
        child_env.config.enable_meta_db = false;
        child_env.config.parent = Some(parent_temp.path().to_path_buf());
        let loaded = child_env.load("parent.pkg").await.unwrap();
        assert_eq!(loaded.full_path, object_dir);

        let directory_parent_temp = tempdir().unwrap();
        let friendly_dir = directory_parent_temp.path().join("directory.pkg");
        tokio_fs::create_dir_all(&friendly_dir).await.unwrap();
        let mut db_child = PackageEnv::new(child_temp.path().join("db-child"));
        db_child.config.enable_meta_db = true;
        db_child.config.parent = Some(directory_parent_temp.path().to_path_buf());
        let loaded = db_child.load("directory.pkg").await.unwrap();
        assert_eq!(loaded.full_path, friendly_dir);
    }

    #[tokio::test]
    async fn test_load_by_meta_obj_id_is_exact() {
        let (mut env, _temp) = setup_test_env().await;
        env.config.enable_meta_db = true;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta_v1 = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let pkg_meta_v2 = PackageMeta::new("test.pkg", "2.0.0", "test", &owner, None);

        let meta_obj_id_v1 = insert_pkg_meta_to_db(&env, &pkg_meta_v1);
        let meta_obj_id_v2 = insert_pkg_meta_to_db(&env, &pkg_meta_v2);
        //println!("meta_obj_id_v1: {}", meta_obj_id_v1.to_string());

        let object_path_v1 = env
            .get_pkg_object_dir(&meta_obj_id_v1.to_string(), &pkg_meta_v1)
            .unwrap();
        let object_path_v2 = env
            .get_pkg_object_dir(&meta_obj_id_v2.to_string(), &pkg_meta_v2)
            .unwrap();
        tokio_fs::create_dir_all(&object_path_v1).await.unwrap();
        tokio_fs::create_dir_all(&object_path_v2).await.unwrap();

        let media_info = env
            .load(&format!("test.pkg#{}", meta_obj_id_v1.to_string()))
            .await
            .unwrap();
        assert_eq!(media_info.full_path, object_path_v1);
        assert_ne!(media_info.full_path, object_path_v2);

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

        let meta_db = MetaIndexDb::create_or_open(env.get_meta_db_path()).unwrap();
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
            &format!("dep.pkg#{}", dep_meta_obj_id.to_string()),
            &store_mgr,
            &mut dep_self_missing,
        )
        .await
        .unwrap();
        assert_eq!(dep_self_missing, vec![missing_chunk]);
    }

    #[tokio::test]
    async fn test_get_pkg_object_dir_uses_filename_once() {
        let (env, _temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new("test.pkg", "1.0.0", "test", &owner, None);
        let (meta_obj_id, _) = pkg_meta.gen_obj_id();
        let pkg_dir = env
            .get_pkg_object_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();

        assert_eq!(
            pkg_dir,
            env.get_install_dir()
                .join("test.pkg")
                .join(meta_obj_id.to_filename())
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
        let object_dir = env
            .get_pkg_object_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        let archive_path = create_test_pkg_archive(temp.path()).await;
        let file = File::open(&archive_path).await.unwrap();
        let chunk_reader: ChunkReader = Box::pin(file);

        env.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await
            .unwrap();

        let friendly_path = env.work_dir.join("test.pkg");
        let old_versioned_path = env.work_dir.join("test.pkg#1.0.0");
        assert!(tokio_fs::metadata(&object_dir).await.unwrap().is_dir());
        assert!(tokio_fs::symlink_metadata(&friendly_path).await.is_ok());
        assert!(tokio_fs::metadata(&friendly_path).await.unwrap().is_dir());
        assert!(tokio_fs::symlink_metadata(&old_versioned_path)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_prefixed_install_preserves_complete_unique_name_in_friendly_path() {
        let (env, temp) = setup_test_env().await;
        let owner = DID::from_str("did:bns:buckyos.ai").unwrap();
        let pkg_meta = PackageMeta::new(
            "nightly-apple-aarch64.filebrowser.buckyos.ai",
            "1.0.0",
            "test",
            &owner,
            None,
        );
        let meta_obj_id = insert_pkg_meta_to_db(&env, &pkg_meta);
        let archive_path = create_test_pkg_archive(temp.path()).await;
        let file = File::open(&archive_path).await.unwrap();
        let chunk_reader: ChunkReader = Box::pin(file);

        env.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await
            .unwrap();

        let friendly_path = env.work_dir.join("filebrowser.buckyos.ai");
        assert!(tokio_fs::metadata(&friendly_path).await.unwrap().is_dir());
        assert!(tokio_fs::symlink_metadata(env.work_dir.join("ai"))
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
        let object_dir = env
            .get_pkg_object_dir(&meta_obj_id.to_string(), &pkg_meta)
            .unwrap();
        let archive_path = create_test_pkg_archive(temp.path()).await;
        let file = File::open(&archive_path).await.unwrap();
        let chunk_reader: ChunkReader = Box::pin(file);

        env.do_install_pkg_from_data(&pkg_meta, &meta_obj_id, chunk_reader, false)
            .await
            .unwrap();

        let friendly_path = env.work_dir.join("test.pkg");
        assert!(tokio_fs::metadata(&object_dir).await.unwrap().is_dir());
        assert!(tokio_fs::metadata(&friendly_path).await.unwrap().is_dir());

        tokio_fs::write(object_dir.join("bin/hello.txt"), "object only")
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

        let new_db_path = temp.path().join("new_index.db");
        MetaIndexDb::create_or_open(&new_db_path).unwrap();

        env.try_update_index_db(&new_db_path).await.unwrap();

        let db_path = env.work_dir.join("pkgs/meta_index.db");
        assert!(tokio_fs::metadata(&db_path).await.is_ok());
        MetaIndexDb::open_existing_readonly(db_path).unwrap();
    }
}
