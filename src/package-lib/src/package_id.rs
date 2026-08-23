use crate::error::{PkgError, PkgResult};
use name_lib::DID;
use ndn_lib::ObjId;
use semver::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use version_compare::Cmp;

use log::info;

const MAX_CHANNEL_LEN: usize = 32;
const MAX_NAME_LABEL_LEN: usize = 63;
const MAX_PACKAGE_NAME_LEN: usize = 255;
const MAX_TAG_LEN: usize = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageOs {
    Linux,
    Windows,
    Apple,
}

impl PackageOs {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
            Self::Apple => "apple",
        }
    }
}

impl FromStr for PackageOs {
    type Err = PkgError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "linux" => Ok(Self::Linux),
            "windows" => Ok(Self::Windows),
            "apple" => Ok(Self::Apple),
            _ => Err(PkgError::ParseError(
                value.to_owned(),
                "unregistered package OS".to_owned(),
            )),
        }
    }
}

impl Display for PackageOs {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageArch {
    Amd64,
    Aarch64,
}

impl PackageArch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Amd64 => "amd64",
            Self::Aarch64 => "aarch64",
        }
    }
}

impl FromStr for PackageArch {
    type Err = PkgError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "amd64" => Ok(Self::Amd64),
            "aarch64" => Ok(Self::Aarch64),
            _ => Err(PkgError::ParseError(
                value.to_owned(),
                "unregistered package architecture".to_owned(),
            )),
        }
    }
}

impl Display for PackageArch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackagePrefix {
    pub channel: String,
    pub os: PackageOs,
    pub arch: PackageArch,
}

impl PackagePrefix {
    pub fn new(channel: &str, os: PackageOs, arch: PackageArch) -> PkgResult<Self> {
        validate_channel(channel)?;
        Ok(Self {
            channel: channel.to_owned(),
            os,
            arch,
        })
    }

    pub fn parse(value: &str) -> PkgResult<Self> {
        let parts = value.split('-').collect::<Vec<_>>();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(PkgError::ParseError(
                value.to_owned(),
                "package prefix must be {channel}-{os}-{arch}".to_owned(),
            ));
        }

        Self::new(
            parts[0],
            PackageOs::from_str(parts[1])?,
            PackageArch::from_str(parts[2])?,
        )
    }
}

impl FromStr for PackagePrefix {
    type Err = PkgError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Display for PackagePrefix {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}-{}-{}", self.channel, self.os, self.arch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageName {
    pub prefix: Option<PackagePrefix>,
    pub unique_name: String,
}

impl PackageName {
    pub fn parse(value: &str) -> PkgResult<Self> {
        if value.is_empty() {
            return Err(PkgError::ParseError(
                value.to_owned(),
                "package name cannot be empty".to_owned(),
            ));
        }
        if !value.is_ascii() || value.len() > MAX_PACKAGE_NAME_LEN {
            return Err(PkgError::ParseError(
                value.to_owned(),
                format!(
                    "package name must be ASCII and at most {} bytes",
                    MAX_PACKAGE_NAME_LEN
                ),
            ));
        }

        let (first_label, remainder) = match value.split_once('.') {
            Some((first, remainder)) => (first, Some(remainder)),
            None => (value, None),
        };

        if has_prefix_shape(first_label) {
            let prefix = PackagePrefix::parse(first_label)?;
            let unique_name = remainder.ok_or_else(|| {
                PkgError::ParseError(
                    value.to_owned(),
                    "a package prefix must be followed by a unique name".to_owned(),
                )
            })?;
            validate_unique_name(unique_name)?;
            return Ok(Self {
                prefix: Some(prefix),
                unique_name: unique_name.to_owned(),
            });
        }

        validate_unique_name(value)?;
        Ok(Self {
            prefix: None,
            unique_name: value.to_owned(),
        })
    }

    pub fn unique_name(&self) -> &str {
        &self.unique_name
    }

    pub fn with_prefix(&self, prefix: &PackagePrefix) -> PkgResult<Self> {
        Self::parse(&format!("{}.{}", prefix, self.unique_name))
    }

    pub fn without_prefix(&self) -> Self {
        Self {
            prefix: None,
            unique_name: self.unique_name.clone(),
        }
    }

    pub fn is_with_prefix(&self) -> bool {
        self.prefix.is_some()
    }
}

impl FromStr for PackageName {
    type Err = PkgError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Display for PackageName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(prefix) = &self.prefix {
            write!(formatter, "{}.{}", prefix, self.unique_name)
        } else {
            formatter.write_str(&self.unique_name)
        }
    }
}

fn validate_channel(channel: &str) -> PkgResult<()> {
    let valid = !channel.is_empty()
        && channel.len() <= MAX_CHANNEL_LEN
        && channel.as_bytes()[0].is_ascii_lowercase()
        && channel
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(PkgError::ParseError(
            channel.to_owned(),
            "channel must match [a-z][a-z0-9_]{0,31}".to_owned(),
        ))
    }
}

fn has_prefix_shape(label: &str) -> bool {
    let mut parts = label.split('-');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(first), Some(second), Some(third), None)
            if !first.is_empty() && !second.is_empty() && !third.is_empty()
    )
}

fn validate_unique_name(unique_name: &str) -> PkgResult<()> {
    if unique_name.is_empty() || !unique_name.is_ascii() {
        return Err(PkgError::ParseError(
            unique_name.to_owned(),
            "unique name must be non-empty ASCII".to_owned(),
        ));
    }

    let labels = unique_name.split('.').collect::<Vec<_>>();
    if labels.iter().any(|label| label.is_empty()) {
        return Err(PkgError::ParseError(
            unique_name.to_owned(),
            "unique name cannot contain empty labels".to_owned(),
        ));
    }
    if has_prefix_shape(labels[0]) {
        return Err(PkgError::ParseError(
            unique_name.to_owned(),
            "the first unique-name label is reserved for package prefixes".to_owned(),
        ));
    }

    for label in labels {
        validate_name_label(label)?;
    }
    Ok(())
}

fn validate_name_label(label: &str) -> PkgResult<()> {
    let bytes = label.as_bytes();
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let valid = !bytes.is_empty()
        && bytes.len() <= MAX_NAME_LABEL_LEN
        && is_alphanumeric(bytes[0])
        && is_alphanumeric(bytes[bytes.len() - 1])
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-' || *byte == b'_'
        });
    if !valid {
        return Err(PkgError::ParseError(
            label.to_owned(),
            "name label must match [a-z0-9](?:[a-z0-9_-]*[a-z0-9])? and be at most 63 bytes"
                .to_owned(),
        ));
    }

    let is_windows_reserved = matches!(label, "con" | "prn" | "aux" | "nul")
        || (label.len() == 4
            && matches!(&label[..3], "com" | "lpt")
            && matches!(label.as_bytes()[3], b'1'..=b'9'));
    if is_windows_reserved {
        return Err(PkgError::ParseError(
            label.to_owned(),
            "name label is a Windows reserved device name".to_owned(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionExpType {
    None,
    Req(VersionReq),
    Version(Version),
}

impl ToString for VersionExpType {
    fn to_string(&self) -> String {
        match self {
            VersionExpType::Req(req) => req.to_string(),
            VersionExpType::Version(version) => version.to_string(),
            VersionExpType::None => "".to_string(),
        }
    }
}

impl FromStr for VersionExpType {
    type Err = PkgError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(VersionExpType::None);
        }

        let version = Version::parse(s);
        if version.is_ok() {
            return Ok(VersionExpType::Version(version.unwrap()));
        }

        let req = VersionReq::parse(s);
        if req.is_ok() {
            return Ok(VersionExpType::Req(req.unwrap()));
        }

        Err(PkgError::ParseError(
            s.to_string(),
            "Invalid version expression".to_string(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionExp {
    pub tag: Option<String>,
    pub version_exp: VersionExpType,
}

impl Default for VersionExp {
    fn default() -> Self {
        VersionExp {
            tag: None,
            version_exp: VersionExpType::None,
        }
    }
}

impl FromStr for VersionExp {
    type Err = PkgError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.is_ascii()
            || s.bytes()
                .any(|byte| byte.is_ascii_whitespace() && byte != b' ')
        {
            return Err(PkgError::ParseError(
                s.to_owned(),
                "version expression only permits ASCII spaces".to_owned(),
            ));
        }

        let parts = s.split(':').collect::<Vec<&str>>();
        match parts.len() {
            1 => {
                let version_exp = VersionExpType::from_str(parts[0])?;
                return Ok(VersionExp {
                    tag: None,
                    version_exp,
                });
            }
            2 => {
                validate_tag(parts[1])?;
                let tag = parts[1].to_string();
                let version_exp = VersionExpType::from_str(parts[0])?;
                return Ok(VersionExp {
                    tag: Some(tag),
                    version_exp,
                });
            }
            _ => {
                return Err(PkgError::ParseError(
                    s.to_string(),
                    "Invalid version expression".to_string(),
                ));
            }
        }
    }
}

fn validate_tag(tag: &str) -> PkgResult<()> {
    let bytes = tag.as_bytes();
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let valid = !bytes.is_empty()
        && bytes.len() <= MAX_TAG_LEN
        && is_alphanumeric(bytes[0])
        && is_alphanumeric(bytes[bytes.len() - 1])
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(*byte, b'.' | b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(PkgError::ParseError(
            tag.to_owned(),
            "tag must be 1-63 lowercase ASCII name characters".to_owned(),
        ))
    }
}

impl ToString for VersionExp {
    fn to_string(&self) -> String {
        if let Some(tag) = &self.tag {
            format!("{}:{}", self.version_exp.to_string(), tag)
        } else {
            self.version_exp.to_string()
        }
    }
}

impl VersionExp {
    pub fn is_version(&self) -> bool {
        matches!(self.version_exp, VersionExpType::Version(_))
    }

    pub fn to_range_int(&self) -> PkgResult<(u64, u64)> {
        match &self.version_exp {
            VersionExpType::Req(req) => match req.comparators.len() {
                1 => {
                    let comparator = &req.comparators[0];
                    match comparator.op {
                        Op::Greater | Op::GreaterEq => {
                            let min = Self::comparator_to_int(comparator)?;
                            let max = i64::MAX;
                            Ok((min as u64, max as u64))
                        }
                        Op::Less | Op::LessEq => {
                            let min = i64::MIN;
                            let max = Self::comparator_to_int(comparator)?;
                            Ok((min as u64, max as u64))
                        }
                        _ => {
                            return Err(PkgError::ParseError(
                                self.to_string(),
                                "VersionExp can not be converted to range int".to_string(),
                            ));
                        }
                    }
                }
                2 => {
                    let comparator1 = &req.comparators[0];
                    let comparator2 = &req.comparators[1];
                    let min = Self::comparator_to_int(comparator1)?;
                    let max = Self::comparator_to_int(comparator2)?;
                    if min > max {
                        return Ok((max, min));
                    }
                    Ok((min, max))
                }
                _ => {
                    return Err(PkgError::ParseError(
                        self.to_string(),
                        "VersionExp can not be converted to range int".to_string(),
                    ));
                }
            },
            _ => {
                return Err(PkgError::ParseError(
                    self.to_string(),
                    "VersionExp can not be converted to range int".to_string(),
                ));
            }
        }
    }

    pub fn comparator_to_int(comparator: &Comparator) -> PkgResult<u64> {
        let major = comparator.major;
        let minor = comparator.minor.unwrap_or(0);
        let patch = comparator.patch.unwrap_or(0);
        let build_str = comparator.pre.to_string();
        let digits_only = build_str.trim_start_matches(|c: char| !c.is_digit(10));
        let build = digits_only.parse::<u64>().unwrap_or(0);

        let version_int =
            (major as u64) << 56 | (minor as u64) << 40 | (patch as u64) << 24 | build as u64;
        Ok(version_int)
    }

    // 将版本号转换为整数表示
    pub fn version_to_int(version: &str) -> PkgResult<u64> {
        // 处理semver格式，先移除预发布版本和构建元数据部分
        let build_pos = version.find(|c| c == '-' || c == '+');
        let version_core = if let Some(pos) = build_pos {
            &version[0..pos]
        } else {
            version
        };
        let mut parts: Vec<&str> = version_core.split('.').collect();

        // 基本格式检查
        if parts.len() < 1 || parts.len() > 4 {
            return Err(PkgError::VersionError(format!(
                "无效的版本格式: {}",
                version
            )));
        }

        if parts.len() == 3 {
            if build_pos.is_some() {
                let build_str = &version[build_pos.unwrap()..];
                parts.push(build_str);
            }
        }

        // 解析各部分
        let major = parts
            .get(0)
            .and_then(|v| {
                // 忽略第一个数字前的其它字符
                let digits_only = v.trim_start_matches(|c: char| !c.is_digit(10));
                digits_only.parse::<u64>().ok()
            })
            .unwrap_or(0);
        if major > 0xff {
            return Err(PkgError::VersionError(format!(
                "主版本号超出范围: {}",
                version
            )));
        }

        let minor = parts
            .get(1)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if minor > 0xffff {
            return Err(PkgError::VersionError(format!(
                "次版本号超出范围: {}",
                version
            )));
        }

        let patch = parts
            .get(2)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if patch > 0xffff {
            return Err(PkgError::VersionError(format!(
                "补丁版本号超出范围: {}",
                version
            )));
        }

        let build = parts
            .get(3)
            .and_then(|v| {
                // 忽略第一个数字前的其它字符
                let digits_only = v.trim_start_matches(|c: char| !c.is_digit(10));
                digits_only.parse::<u64>().ok()
            })
            .unwrap_or(0);
        if build > 0xffffff {
            return Err(PkgError::VersionError(format!(
                "构建号超出范围: {}",
                version
            )));
        }
        //0xff , 0xffff, 0xffff, 0xffffff ,build号用24位，支持 15-12-25 这样的6位日期
        let version_int =
            (major as u64) << 56 | (minor as u64) << 40 | (patch as u64) << 24 | build as u64;

        Ok(version_int)
    }

    pub fn compare_versions(v1: &str, v2: &str) -> std::cmp::Ordering {
        match (semver::Version::parse(v1), semver::Version::parse(v2)) {
            (Ok(v1), Ok(v2)) => {
                //info!("compare_versions: v1 {} v2 {}", v1, v2);
                v1.cmp(&v2)
            }
            // 处理非标准版本格式的情况
            _ => {
                // 自定义比较逻辑，使用我们的整数表示进行比较
                match (Self::version_to_int(v1), Self::version_to_int(v2)) {
                    (Ok(v1_int), Ok(v2_int)) => v1_int.cmp(&v2_int),
                    // 如果转换失败，则按字符串比较
                    _ => v1.cmp(v2),
                }
            }
        }
    }
}

/// A canonical package name plus either a version expression or an exact package ObjId.
///
/// The public string fields are retained for API compatibility. `parse` is the only supported
/// constructor for external input and guarantees that `version_exp` and `objid` are mutually
/// exclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageId {
    pub name: String,
    pub version_exp: Option<VersionExp>,
    pub objid: Option<String>,
}

impl FromStr for PackageId {
    type Err = PkgError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PackageId::parse(s)
    }
}

impl ToString for PackageId {
    fn to_string(&self) -> String {
        let mut result = self.name.clone();
        if let Some(version) = &self.version_exp {
            result.push_str("#");
            result.push_str(&version.to_string());
        }
        if let Some(objid) = &self.objid {
            result.push_str("#");
            result.push_str(objid);
        }
        result
    }
}

impl PackageId {
    pub fn get_author_from_unique_name(unique_name: &str) -> Option<String> {
        let package_name = PackageName::parse(unique_name).ok()?;
        if package_name.is_with_prefix() {
            return None;
        }
        let parts = unique_name.split('_').collect::<Vec<_>>();
        (parts.len() == 2).then(|| parts[0].to_owned())
    }

    pub fn unique_name_to_did(unique_name: &str) -> PkgResult<DID> {
        let package_name = PackageName::parse(unique_name)?;
        if package_name.is_with_prefix() {
            return Err(PkgError::ParseError(
                unique_name.to_owned(),
                "DID conversion requires a unique name without a package prefix".to_owned(),
            ));
        }

        let parts = unique_name.split('_').collect::<Vec<&str>>();
        let did_str = if parts.len() == 2 {
            //did:bns:module_name.author
            format!("did:bns:{}.{}", parts[1], parts[0])
        } else {
            format!("did:bns:{}", unique_name)
        };
        DID::from_str(&did_str)
            .map_err(|error| PkgError::ParseError(unique_name.to_owned(), error.to_string()))
    }

    pub fn to_did(&self) -> PkgResult<DID> {
        let unique_name = self.get_unique_name()?;
        Self::unique_name_to_did(&unique_name)
    }

    pub fn from_did(did: &DID) -> PkgResult<PackageId> {
        if did.method.as_str() != "bns" {
            return Err(PkgError::ParseError(
                did.to_string(),
                "Invalid did method".to_string(),
            ));
        }
        let parts = did.id.split('.').collect::<Vec<&str>>();
        if parts.len() == 2 {
            let author = parts[1];
            let module_name = parts[0];
            return PackageId::parse(&format!("{}_{}", author, module_name));
        } else {
            return PackageId::parse(&did.id);
        }
    }

    pub fn get_pkg_id_unique_name(pkg_id: &str) -> String {
        let the_pkg_id = PackageId::parse(pkg_id);
        if the_pkg_id.is_err() {
            return pkg_id.to_string();
        }
        let the_pkg_id = the_pkg_id.unwrap();
        the_pkg_id
            .get_unique_name()
            .unwrap_or_else(|_| pkg_id.to_owned())
    }

    pub fn get_pkgid_with_objid(pkg_id: &str, pkg_obj_id: Option<ObjId>) -> PkgResult<String> {
        let mut the_pkg_id = PackageId::parse(pkg_id)?;
        let Some(pkg_obj_id) = pkg_obj_id else {
            the_pkg_id.version_exp = None;
            return Ok(the_pkg_id.to_string());
        };
        let normalized_objid = normalize_package_objid(&pkg_obj_id.to_string())?;

        if let Some(existing_objid) = the_pkg_id.objid.as_deref() {
            if existing_objid != normalized_objid {
                return Err(PkgError::ParseError(
                    pkg_id.to_string(),
                    format!(
                        "pkg obj id mismatch: existing {} != provided {}",
                        existing_objid, normalized_objid
                    ),
                ));
            }
        }
        the_pkg_id.version_exp = None;
        the_pkg_id.objid = Some(normalized_objid);
        Ok(the_pkg_id.to_string())
    }

    pub fn get_unique_name(&self) -> PkgResult<String> {
        Ok(PackageName::parse(&self.name)?.unique_name)
    }

    pub fn package_name(&self) -> PkgResult<PackageName> {
        PackageName::parse(&self.name)
    }

    pub fn is_with_prefix(&self) -> PkgResult<bool> {
        Ok(self.package_name()?.is_with_prefix())
    }

    pub fn load_candidates(&self, current_prefix: &PackagePrefix) -> PkgResult<Vec<Self>> {
        let package_name = self.package_name()?;
        if package_name.is_with_prefix() {
            return Ok(vec![self.clone()]);
        }

        let mut prefixed = self.clone();
        prefixed.name = package_name.with_prefix(current_prefix)?.to_string();
        Ok(vec![prefixed, self.clone()])
    }

    pub fn parse(pkg_id: &str) -> PkgResult<PackageId> {
        if pkg_id.matches('#').count() > 1 {
            return Err(PkgError::ParseError(
                pkg_id.to_owned(),
                "package id permits at most one selector".to_owned(),
            ));
        }

        let (name, selector) = match pkg_id.split_once('#') {
            Some((name, selector)) => (name, Some(selector)),
            None => (pkg_id, None),
        };
        let package_name = PackageName::parse(name)?;

        let Some(selector) = selector else {
            return Ok(PackageId {
                name: package_name.to_string(),
                version_exp: None,
                objid: None,
            });
        };
        if selector.is_empty() {
            return Err(PkgError::ParseError(
                pkg_id.to_owned(),
                "package selector cannot be empty".to_owned(),
            ));
        }

        if selector.starts_with("pkg:") {
            return Ok(PackageId {
                name: package_name.to_string(),
                version_exp: None,
                objid: Some(normalize_package_objid(selector)?),
            });
        }

        let version_exp = VersionExp::from_str(selector)?;
        Ok(PackageId {
            name: package_name.to_string(),
            version_exp: Some(version_exp),
            objid: None,
        })
    }
}

fn normalize_package_objid(value: &str) -> PkgResult<String> {
    let objid = ObjId::new(value)
        .map_err(|error| PkgError::ParseError(value.to_owned(), error.to_string()))?;
    let normalized = objid.to_string();
    if objid.obj_type != "pkg"
        || objid.obj_hash.is_empty()
        || normalized != value
        || !value[4..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(PkgError::ParseError(
            value.to_owned(),
            "exact package ObjId must use canonical pkg:<lowercase-hex-hash> form".to_owned(),
        ));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    const VALID_OBJID_1: &str =
        "pkg:bcc479e2547e3ce5c6805ec12cffdb460e2f5856dda3ec600e27f0de570e248a";
    const VALID_OBJID_2: &str =
        "pkg:acc479e2547e3ce5c6805ec12cffdb460e2f5856dda3ec600e27f0de570e248a";

    fn objid_1() -> ObjId {
        ObjId::new(VALID_OBJID_1).unwrap()
    }

    fn objid_2() -> ObjId {
        ObjId::new(VALID_OBJID_2).unwrap()
    }

    #[test]
    fn test_parse() {
        let pkg_id = "buckyos-dev_filebrowser";
        let result = PackageId::parse(pkg_id).unwrap();
        assert_eq!(&result.name, "buckyos-dev_filebrowser");
        assert_eq!(
            PackageId::get_author_from_unique_name(&result.name).as_deref(),
            Some("buckyos-dev")
        );
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);
        let did = result.to_did().unwrap();
        assert_eq!(did.method, "bns");
        assert_eq!(did.id, "filebrowser.buckyos-dev");

        let pkg_id = "a#0.1.0:stable";
        let result = PackageId::parse(pkg_id).unwrap();
        assert_eq!(&result.name, "a");
        assert_eq!(
            result.version_exp.as_ref().unwrap().to_string(),
            "0.1.0:stable".to_string()
        );
        assert_eq!(
            result.version_exp.as_ref().unwrap().tag,
            Some("stable".to_string())
        );
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);

        let pkg_id = "nightly-linux-amd64.buckyos_filebrowser#0.4.0";
        let result = PackageId::parse(pkg_id).unwrap();
        assert_eq!(&result.name, "nightly-linux-amd64.buckyos_filebrowser");
        assert_eq!(
            result.version_exp.as_ref().unwrap().to_string(),
            "0.4.0".to_string()
        );
        assert_eq!(result.version_exp.as_ref().unwrap().tag, None);
        let app_name = result.get_unique_name().unwrap();
        assert_eq!(app_name, "buckyos_filebrowser".to_string());

        let did1 = result.to_did().unwrap();
        let did = PackageId::unique_name_to_did("buckyos_filebrowser").unwrap();
        let host_name = did.to_host_name();
        assert_eq!(host_name, "filebrowser.buckyos.bns.did");
        assert_eq!(did1, did);
        assert_eq!(did.method, "bns");
        assert_eq!(did.id, "filebrowser.buckyos");
        let pkg_id = PackageId::from_did(&did).unwrap();
        assert_eq!(pkg_id.name, "buckyos_filebrowser");
        assert_eq!(pkg_id.version_exp, None);
        assert_eq!(pkg_id.objid, None);

        let dotted = PackageId::parse("filebrowser.buckyos.ai").unwrap();
        let dotted_did = dotted.to_did().unwrap();
        assert_eq!(dotted_did.to_string(), "did:bns:filebrowser.buckyos.ai");
        assert_eq!(PackageId::from_did(&dotted_did).unwrap(), dotted);

        let pkg_id = format!("a#{}", VALID_OBJID_1);
        let result = PackageId::parse(&pkg_id).unwrap();
        assert_eq!(&result.name, "a");
        assert_eq!(result.objid, Some(VALID_OBJID_1.to_string()));
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);

        let pkg_id = format!("a#{}", VALID_OBJID_2);
        let result = PackageId::parse(&pkg_id).unwrap();
        assert_eq!(&result.name, "a");
        assert_eq!(result.version_exp, None);
        assert_eq!(result.objid, Some(VALID_OBJID_2.to_string()));
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);

        let pkg_id = "a#>0.1.0";
        let result = PackageId::parse(pkg_id).unwrap();
        assert_eq!(&result.name, "a");
        assert_eq!(
            result.version_exp.as_ref().unwrap().to_string(),
            ">0.1.0".to_string()
        );
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);

        let pkg_id = "a#>0.1.0, <0.1.2:stable";
        let result = PackageId::parse(pkg_id).unwrap();
        assert_eq!(&result.name, "a");
        //println!("result.version_exp: {:?}", result.version_exp.as_ref().unwrap().to_string());
        assert_eq!(
            result.version_exp.as_ref().unwrap().to_string(),
            ">0.1.0, <0.1.2:stable".to_string()
        );
        let pkg_id2 = result.to_string();
        assert_eq!(pkg_id, pkg_id2);

        assert!(PackageId::parse("a#not-a-version").is_err());
        assert!(VersionExp::from_str("not-a-version").is_err());
        let latest = VersionExp::from_str(":latest").unwrap();
        assert_eq!(latest.version_exp, VersionExpType::None);
        assert_eq!(latest.tag.as_deref(), Some("latest"));
    }

    #[test]
    fn test_get_pkgid_with_objid() {
        assert_eq!(PackageId::get_pkgid_with_objid("a", None).unwrap(), "a");
        assert_eq!(
            PackageId::get_pkgid_with_objid("a#0.5.1", None).unwrap(),
            "a"
        );
        assert_eq!(
            PackageId::get_pkgid_with_objid("bb.a#0.5.1", Some(objid_1())).unwrap(),
            format!("bb.a#{}", VALID_OBJID_1)
        );
        assert_eq!(
            PackageId::get_pkgid_with_objid("a#1.0.0", Some(objid_1())).unwrap(),
            format!("a#{}", VALID_OBJID_1)
        );
        assert_eq!(
            PackageId::get_pkgid_with_objid(&format!("a#{}", VALID_OBJID_1), Some(objid_1()))
                .unwrap(),
            format!("a#{}", VALID_OBJID_1)
        );
        assert!(PackageId::get_pkgid_with_objid(
            &format!("a#1.0.0#{}", VALID_OBJID_1),
            Some(objid_1())
        )
        .is_err());
        assert!(
            PackageId::get_pkgid_with_objid(&format!("a#{}", VALID_OBJID_1), Some(objid_2()))
                .is_err()
        );
        assert!(PackageId::get_pkgid_with_objid(
            &format!("a#1.0.0#{}", VALID_OBJID_1),
            Some(objid_2())
        )
        .is_err());
    }

    #[test]
    fn test_package_prefix_and_name_rules() {
        let prefix = PackagePrefix::parse("nightly-linux-amd64").unwrap();
        assert_eq!(prefix.channel, "nightly");
        assert_eq!(prefix.os, PackageOs::Linux);
        assert_eq!(prefix.arch, PackageArch::Amd64);
        assert_eq!(prefix.to_string(), "nightly-linux-amd64");

        let generic = PackageName::parse("filebrowser.buckyos.ai").unwrap();
        assert!(!generic.is_with_prefix());
        assert_eq!(generic.unique_name(), "filebrowser.buckyos.ai");
        let platform = generic.with_prefix(&prefix).unwrap();
        assert!(platform.is_with_prefix());
        assert_eq!(platform.unique_name(), "filebrowser.buckyos.ai");
        assert_eq!(
            platform.to_string(),
            "nightly-linux-amd64.filebrowser.buckyos.ai"
        );
        assert_eq!(platform.without_prefix(), generic);

        let max_channel = format!("a{}-linux-amd64", "1".repeat(31));
        assert!(PackagePrefix::parse(&max_channel).is_ok());
        assert!(PackagePrefix::parse(&format!("a{}-linux-amd64", "1".repeat(32))).is_err());

        let max_label = "a".repeat(63);
        let max_name = std::iter::repeat(max_label.as_str())
            .take(4)
            .collect::<Vec<_>>()
            .join(".");
        assert_eq!(max_name.len(), 255);
        assert!(PackageName::parse(&max_name).is_ok());
        assert!(PackageName::parse(&"a".repeat(64)).is_err());

        for invalid in [
            "nightly-macos-x86_64.filebrowser",
            "nightly-freebsd-amd64.filebrowser",
            "nightly-linux-riscv64.filebrowser",
            "my-cool-app.module",
            "nightly-linux-amd64",
            ".filebrowser",
            "filebrowser.",
            "filebrowser..ai",
            "FileBrowser",
            "file/browser",
            "con.tools",
            "tools.com1",
            "tools.lpt9",
            "-filebrowser",
            "filebrowser_",
        ] {
            assert!(PackageName::parse(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn test_package_id_strict_selectors_and_candidates() {
        let prefix = PackagePrefix::parse("nightly-windows-amd64").unwrap();
        let request = PackageId::parse("filebrowser.buckyos.ai#1.2.3").unwrap();
        let candidates = request.load_candidates(&prefix).unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].to_string(),
            "nightly-windows-amd64.filebrowser.buckyos.ai#1.2.3"
        );
        assert_eq!(candidates[1].to_string(), "filebrowser.buckyos.ai#1.2.3");

        let exact = PackageId::parse(&format!("filebrowser#{}", VALID_OBJID_1)).unwrap();
        assert_eq!(exact.objid.as_deref(), Some(VALID_OBJID_1));
        for invalid in [
            "filebrowser#",
            "filebrowser#abc123",
            "filebrowser#1.2.3:Stable",
            "filebrowser#1.2.3#pkg:bcc4",
            "filebrowser#sha256:bcc4",
            "filebrowser#pkg:BCC4",
        ] {
            assert!(PackageId::parse(invalid).is_err(), "accepted {invalid}");
        }

        let explicit =
            PackageId::parse("nightly-windows-amd64.filebrowser.buckyos.ai#:latest").unwrap();
        assert_eq!(explicit.load_candidates(&prefix).unwrap(), vec![explicit]);
    }

    #[test]
    fn test_version_to_int() -> PkgResult<()> {
        // 测试版本号转整数
        let test_cases = vec![
            ("1", 0x01_0000_0000_000000),
            ("1.0", 0x01_0000_0000_000000),
            ("1.2", 0x01_0002_0000_000000),
            ("1.2.3", 0x01_0002_0003_000000),
            ("1.2.3.4", 0x01_0002_0003_000004),
            ("10.20.30.40", 0x0A_0014_001E_000028),
            ("0.0.0.0", 0x00_0000_0000_000000),
            ("1.0.3", 0x01_0000_0003_000000),
            ("1.0.3-250326", 0x01_0000_0003_03d1d6),
            ("1.0.0-alpha_123", 0x01_0000_0000_00007b),
            ("0.4.0-250724", 0x00_0004_0000_03d364),
            ("0.4.0", 0x00_0004_0000_000000),
            (">1.0.3-build250326", 0x01_0000_0003_03d1d6),
        ];

        for (version, expected) in &test_cases {
            let result = VersionExp::version_to_int(version)?;
            assert_eq!(
                result, *expected,
                "版本 {} 转换为整数应该是 {:#X}, 但得到了 {:#X}",
                version, expected, result
            );
        }

        Ok(())
    }

    #[test]
    fn test_comparator_to_int() -> PkgResult<()> {
        let comparator =
            VersionExp::comparator_to_int(&Comparator::parse(">1.0.3-build250326").unwrap())
                .unwrap();
        assert_eq!(comparator, 0x01_0000_0003_03d1d6);

        let package_id = PackageId::parse("a#>1.0.3-build250326, <=1.0.4-build250426").unwrap();
        let range = package_id
            .version_exp
            .as_ref()
            .unwrap()
            .to_range_int()
            .unwrap();
        assert_eq!(range, (0x01_0000_0003_03d1d6, 0x01_0000_0004_03d23a));
        Ok(())
    }

    #[test]
    fn test_version_comparison() -> PkgResult<()> {
        buckyos_kit::init_logging("package-lib test", false);
        // 测试标准semver格式的版本比较
        let semver_test_cases = vec![
            ("1.0.0", "1.0.0", Ordering::Equal),
            ("1.0.0", "1.0.1", Ordering::Less),
            ("1.0.1", "1.0.0", Ordering::Greater),
            ("1.0.0", "1.1.0", Ordering::Less),
            ("1.1.0", "1.0.0", Ordering::Greater),
            ("1.0.0", "2.0.0", Ordering::Less),
            ("2.0.0", "1.0.0", Ordering::Greater),
            ("1.0.0-alpha", "1.0.0", Ordering::Less),
            ("1.0.0", "1.0.0-alpha", Ordering::Greater),
            ("1.0.0-alpha", "1.0.0-beta", Ordering::Less),
            ("1.0.0-beta", "1.0.0-alpha", Ordering::Greater),
            ("1.0.0-beta", "1.0.0-alpha+323ad", Ordering::Greater),
            ("1.0.0", "1.0.0+250725", Ordering::Less),
        ];

        for (v1, v2, expected) in semver_test_cases {
            let result = VersionExp::compare_versions(v1, v2);
            assert_eq!(
                result, expected,
                "比较 {} 和 {} 应该得到 {:?}, 但得到了 {:?}",
                v1, v2, expected, result
            );
        }

        // 测试非标准格式的版本比较（使用我们的自定义逻辑）
        let custom_test_cases = vec![
            ("1", "1", Ordering::Equal),
            ("1", "1.0", Ordering::Equal),
            ("1.0", "1.0.0", Ordering::Equal),
            ("1", "2", Ordering::Less),
            ("2", "1", Ordering::Greater),
            ("1.2", "1.3", Ordering::Less),
            ("1.3", "1.2", Ordering::Greater),
            ("1.2.3", "1.2.4", Ordering::Less),
            ("1.2.4", "1.2.3", Ordering::Greater),
            ("1.2.3.4", "1.2.3.5", Ordering::Less),
            ("1.2.3.5", "1.2.3.4", Ordering::Greater),
            ("1.2.3", "1.2.3.0", Ordering::Equal),
            ("1.2.0", "1.2", Ordering::Equal),
            ("1.0.0", "1", Ordering::Equal),
        ];

        for (v1, v2, expected) in custom_test_cases {
            let result = VersionExp::compare_versions(v1, v2);
            assert_eq!(
                result, expected,
                "比较 {} 和 {} 应该得到 {:?}, 但得到了 {:?}",
                v1, v2, expected, result
            );
        }

        Ok(())
    }
}
