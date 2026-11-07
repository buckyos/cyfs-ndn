//! Common meta of a FileObject (`CYFS 标准对象` §10): whoever constructs a
//! FileObject writes it, with this helper.
//!
//! The parsers read only what they need through [`MediaByteSource`], never the
//! whole file, and stop at [`MEDIA_META_READ_LIMIT`] bytes. The input is
//! untrusted: a parse error drops that group of fields and is never reported.
//!
//! buckyos-websdk `src/file_meta.ts` is a line-by-line port, and
//! `tests/media_meta_vectors.json` of the SDK is printed by
//! `test_media_meta_vectors` here. Keep the read pattern and the arithmetic
//! identical: a different result is a different ObjId.

use crate::FileObject;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Upper bound of the bytes read from one file. Past it the group being parsed
/// is dropped, as for any other parse error.
pub const MEDIA_META_READ_LIMIT: u64 = 8 * 1024 * 1024;

const READ_WINDOW: u64 = 64 * 1024;
const SNIFF_BYTES: u64 = 4096;
const MAX_TEXT_BYTES: u64 = 4096;
const MAX_TEXT_CHARS: usize = 128;
const MAX_JPEG_SEGMENTS: usize = 1024;
const MAX_PNG_CHUNKS: usize = 1024;
const MAX_RIFF_CHUNKS: usize = 64;
const MAX_ISO_BOXES: usize = 4096;
const MAX_ID3_FRAMES: usize = 1024;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MP4_EPOCH_TO_UNIX_SECS: u64 = 2_082_844_800;
const MAX_UNIX_SECS: u64 = 253_402_300_799;

/// Random access to the bytes of a file.
#[async_trait]
pub trait MediaByteSource: Send + Sync {
    fn size(&self) -> u64;
    /// Exactly `length` bytes at `offset`, or an error.
    async fn read_at(&self, offset: u64, length: usize) -> io::Result<Vec<u8>>;
}

pub struct MemoryByteSource<'a> {
    bytes: &'a [u8],
}

impl<'a> MemoryByteSource<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

#[async_trait]
impl MediaByteSource for MemoryByteSource<'_> {
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    async fn read_at(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let end = offset.saturating_add(length as u64);
        if end > self.bytes.len() as u64 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(self.bytes[offset as usize..end as usize].to_vec())
    }
}

pub struct LocalFileByteSource {
    file: tokio::sync::Mutex<tokio::fs::File>,
    size: u64,
}

impl LocalFileByteSource {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = tokio::fs::File::open(path).await?;
        let size = file.metadata().await?.len();
        Ok(Self {
            file: tokio::sync::Mutex::new(file),
            size,
        })
    }
}

#[async_trait]
impl MediaByteSource for LocalFileByteSource {
    fn size(&self) -> u64 {
        self.size
    }

    async fn read_at(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let mut file = self.file.lock().await;
        file.seek(io::SeekFrom::Start(offset)).await?;
        let mut out = vec![0u8; length];
        file.read_exact(&mut out).await?;
        Ok(out)
    }
}

/// `geo`, WGS84. `lat` and `lon` keep 6 decimals, the others 1.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaGeo {
    pub lat: f64,
    pub lon: f64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub heading: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub accuracy_m: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaCamera {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub make: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub lens: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaExposure {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub iso: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub f_number: Option<f64>,
    /// Reduced fraction such as `1/120`; whole seconds are written `2`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exposure_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub focal_length_mm: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub focal_length_35mm: Option<u64>,
}

/// The common meta keys of a `cyfile`. A field that cannot be determined is
/// omitted, and an empty group is not written.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaMeta {
    /// Without parameters, never `application/octet-stream`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mime: Option<String>,
    /// Display size, rotation already applied.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub width: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub video_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub audio_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub artist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub album: Option<String>,
    /// ISO 8601. Without an offset the time zone is unknown: not UTC.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub captured_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub geo: Option<MediaGeo>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub camera: Option<MediaCamera>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exposure: Option<MediaExposure>,
}

impl MediaMeta {
    pub fn to_meta_map(&self) -> HashMap<String, serde_json::Value> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
}

impl FileObject {
    /// Write `meta` into the flattened meta of this object.
    pub fn apply_media_meta(&mut self, meta: &MediaMeta) {
        self.meta.extend(meta.to_meta_map());
    }
}

/// Media type of a file name by its extension. Same table as the SDK's `content_mime.ts`.
pub fn media_type_from_extension(ext: &str) -> Option<String> {
    let ext = ext.to_lowercase();
    let mapped = match ext.as_str() {
        "png" => "image/png",
        "apng" => "image/apng",
        "jpg" | "jpeg" | "jfif" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "ico" => "image/x-icon",
        "avif" => "image/avif",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "tif" | "tiff" => "image/tiff",
        "psd" => "image/vnd.adobe.photoshop",
        "svg" => "image/svg+xml",
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        "avi" => "video/x-msvideo",
        "ogv" => "video/ogg",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "m4a" => "audio/mp4",
        "aac" => "audio/aac",
        "pdf" => "application/pdf",
        "html" | "htm" => "text/html",
        "xhtml" => "application/xhtml+xml",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "doc" => "application/msword",
        "xls" => "application/vnd.ms-excel",
        "ppt" => "application/vnd.ms-powerpoint",
        "odt" => "application/vnd.oasis.opendocument.text",
        "zip" => "application/zip",
        "tar" => "application/x-tar",
        "gz" => "application/gzip",
        "7z" => "application/x-7z-compressed",
        "rar" => "application/vnd.rar",
        "epub" => "application/epub+zip",
        "md" | "markdown" => "text/markdown",
        "json" => "application/json",
        "xml" => "application/xml",
        "csv" => "text/csv",
        "yaml" | "yml" => "application/yaml",
        "toml" => "application/toml",
        "js" | "mjs" | "cjs" => "text/javascript",
        "css" => "text/css",
        "txt" | "text" | "log" | "ini" | "conf" | "cfg" | "env" | "tsv" | "rst" | "adoc"
        | "jsonc" | "json5" | "ts" | "tsx" | "jsx" | "rs" | "py" | "go" | "c" | "h" | "cc"
        | "cpp" | "hpp" | "java" | "kt" | "swift" | "rb" | "php" | "sql" | "sh" | "bash"
        | "zsh" | "ps1" | "bat" | "scss" | "less" | "vue" | "svelte" | "lua" | "dart" | "r"
        | "scala" | "ex" | "exs" | "dockerfile" | "makefile" | "gitignore" | "editorconfig"
        | "lock" => return Some(format!("text/x-{}", ext)),
        _ => return None,
    };
    Some(mapped.to_string())
}

fn extension_of(name: &str) -> String {
    let base = name.rsplit('/').next().unwrap_or(name);
    match base.rfind('.') {
        Some(dot) if dot > 0 => base[dot + 1..].to_lowercase(),
        _ => String::new(),
    }
}

fn ascii_at(bytes: &[u8], start: usize, expected: &[u8]) -> bool {
    bytes.len() >= start + expected.len() && &bytes[start..start + expected.len()] == expected
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

fn is_heic_brand(brand: &[u8]) -> bool {
    [
        b"heic", b"heix", b"heim", b"heis", b"hevc", b"hevx", b"hevm", b"hevs",
    ]
    .iter()
    .any(|known| &known[..] == brand)
}

fn compatible_brands(b: &[u8]) -> Vec<&[u8]> {
    let size = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let end = b.len().min(if size > 16 { size as usize } else { 16 });
    let mut brands = Vec::new();
    let mut at = 16;
    while at + 4 <= end {
        brands.push(&b[at..at + 4]);
        at += 4;
    }
    brands
}

/// The confirmed media type of a byte prefix.
pub fn sniff_magic(b: &[u8]) -> Option<&'static str> {
    if b.len() < 4 {
        return None;
    }
    if b[..4] == [0x89, 0x50, 0x4e, 0x47] {
        return Some("image/png");
    }
    if b[..3] == [0xff, 0xd8, 0xff] {
        return Some("image/jpeg");
    }
    if ascii_at(b, 0, b"GIF8") {
        return Some("image/gif");
    }
    if ascii_at(b, 0, b"RIFF") && b.len() >= 12 {
        match &b[8..12] {
            b"WEBP" => return Some("image/webp"),
            b"WAVE" => return Some("audio/wav"),
            b"AVI " => return Some("video/x-msvideo"),
            _ => {}
        }
    }
    if b[..2] == [0x42, 0x4d] {
        return Some("image/bmp");
    }
    if b[..4] == [0, 0, 1, 0] {
        return Some("image/x-icon");
    }
    if ascii_at(b, 0, b"II*\0") || ascii_at(b, 0, b"MM\0*") {
        return Some("image/tiff");
    }
    if ascii_at(b, 0, b"8BPS") {
        return Some("image/vnd.adobe.photoshop");
    }
    if ascii_at(b, 0, b"%PDF-") {
        return Some("application/pdf");
    }
    if b.len() >= 12 && ascii_at(b, 4, b"ftyp") {
        let brand = &b[8..12];
        if brand == b"avif" || brand == b"avis" {
            return Some("image/avif");
        }
        if is_heic_brand(brand) {
            return Some("image/heic");
        }
        if brand == b"mif1" || brand == b"msf1" {
            let compatible = compatible_brands(b);
            if compatible.iter().any(|c| *c == b"avif" || *c == b"avis") {
                return Some("image/avif");
            }
            return Some(if compatible.iter().any(|c| is_heic_brand(c)) {
                "image/heic"
            } else {
                "image/heif"
            });
        }
        if brand == b"qt  " {
            return Some("video/quicktime");
        }
        if brand == b"M4A " {
            return Some("audio/mp4");
        }
        return Some("video/mp4");
    }
    if b[..4] == [0x1a, 0x45, 0xdf, 0xa3] {
        return Some(if contains_bytes(&b[..b.len().min(64)], b"webm") {
            "video/webm"
        } else {
            "video/x-matroska"
        });
    }
    if ascii_at(b, 0, b"OggS") {
        return Some(if contains_bytes(&b[..b.len().min(96)], b"theora") {
            "video/ogg"
        } else {
            "audio/ogg"
        });
    }
    if ascii_at(b, 0, b"ID3") {
        return Some("audio/mpeg");
    }
    if b[0] == 0xff && (b[1] & 0xe6) == 0xe2 && (b[1] & 0x18) != 0x08 {
        return Some("audio/mpeg");
    }
    if ascii_at(b, 0, b"fLaC") {
        return Some("audio/flac");
    }
    if b[0] == 0x50 && b[1] == 0x4b && (b[2] == 0x03 || b[2] == 0x05 || b[2] == 0x07) {
        return Some("application/zip");
    }
    if b[0] == 0x1f && b[1] == 0x8b {
        return Some("application/gzip");
    }
    if b.len() >= 6 && b[..6] == [b'7', b'z', 0xbc, 0xaf, 0x27, 0x1c] {
        return Some("application/x-7z-compressed");
    }
    if ascii_at(b, 0, b"Rar!") {
        return Some("application/vnd.rar");
    }
    let head = text_head(b);
    if head.starts_with(b"<?xml") || head.starts_with(b"<svg") || head.starts_with(b"<!doctype svg")
    {
        if contains_bytes(&head, b"<svg") {
            return Some("image/svg+xml");
        }
        if head.starts_with(b"<?xml") {
            return Some("application/xml");
        }
    }
    if head.starts_with(b"<!doctype html")
        || head.starts_with(b"<html")
        || head.starts_with(b"<head")
        || head.starts_with(b"<body")
    {
        return Some("text/html");
    }
    None
}

// Lower-cased first 512 bytes, after a UTF-8 BOM and leading whitespace.
fn text_head(b: &[u8]) -> Vec<u8> {
    let start = if b.starts_with(&[0xef, 0xbb, 0xbf]) { 3 } else { 0 };
    let head = &b[start..b.len().min(start + 512)];
    let first = head
        .iter()
        .position(|c| !matches!(c, 0x09..=0x0d | 0x20 | 0xa0))
        .unwrap_or(head.len());
    head[first..].to_ascii_lowercase()
}

fn is_media_type(mime: &str) -> bool {
    mime.starts_with("image/") || mime.starts_with("video/") || mime.starts_with("audio/")
}

// `type/subtype` without parameters, or nothing for a generic or malformed value.
fn declared_media_type(declared: Option<&str>) -> Option<String> {
    let mime = declared?.split(';').next()?.trim().to_lowercase();
    if mime == "application/octet-stream" || mime == "binary/octet-stream" {
        return None;
    }
    let (main, sub) = mime.split_once('/')?;
    let valid = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'+' | b'-'))
    };
    (valid(main) && valid(sub)).then_some(mime)
}

fn resolve_mime(magic: Option<&str>, name: &str, declared: Option<&str>) -> Option<String> {
    magic
        .filter(|mime| is_media_type(mime))
        .map(str::to_string)
        .or_else(|| declared_media_type(declared))
        .or_else(|| media_type_from_extension(&extension_of(name)))
        .or_else(|| magic.map(str::to_string))
}

struct Stop;
type R<T> = Result<T, Stop>;

struct Reader<'a> {
    source: &'a dyn MediaByteSource,
    size: u64,
    remaining: u64,
    window_start: u64,
    window: Vec<u8>,
}

impl<'a> Reader<'a> {
    fn new(source: &'a dyn MediaByteSource) -> Self {
        Self {
            source,
            size: source.size(),
            remaining: MEDIA_META_READ_LIMIT,
            window_start: 0,
            window: Vec::new(),
        }
    }

    async fn read(&mut self, offset: u64, length: u64) -> R<Vec<u8>> {
        let end = offset.checked_add(length).ok_or(Stop)?;
        if end > self.size {
            return Err(Stop);
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let window_end = self.window_start + self.window.len() as u64;
        if offset >= self.window_start && end <= window_end {
            let from = (offset - self.window_start) as usize;
            return Ok(self.window[from..from + length as usize].to_vec());
        }
        let fetch = length.max(READ_WINDOW).min(self.size - offset);
        if fetch > self.remaining {
            return Err(Stop);
        }
        self.remaining -= fetch;
        let bytes = self
            .source
            .read_at(offset, fetch as usize)
            .await
            .map_err(|_| Stop)?;
        if bytes.len() as u64 != fetch {
            return Err(Stop);
        }
        self.window_start = offset;
        self.window = bytes;
        Ok(self.window[..length as usize].to_vec())
    }
}

fn be16(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 2).ok_or(Stop)?;
    Ok(u16::from_be_bytes([s[0], s[1]]) as u64)
}

fn be32(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 4).ok_or(Stop)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as u64)
}

fn be64(b: &[u8], at: usize) -> R<u64> {
    let value = be32(b, at)? * (1u64 << 32) + be32(b, at + 4)?;
    if value > MAX_SAFE_INTEGER {
        return Err(Stop);
    }
    Ok(value)
}

fn be_uint(b: &[u8], at: usize, bytes: usize) -> R<u64> {
    match bytes {
        0 => Ok(0),
        2 => be16(b, at),
        4 => be32(b, at),
        8 => be64(b, at),
        _ => Err(Stop),
    }
}

fn le16(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 2).ok_or(Stop)?;
    Ok(u16::from_le_bytes([s[0], s[1]]) as u64)
}

fn le24(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 3).ok_or(Stop)?;
    Ok(s[0] as u64 | (s[1] as u64) << 8 | (s[2] as u64) << 16)
}

fn le32(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 4).ok_or(Stop)?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]) as u64)
}

/// Round the absolute value, then restore the sign; `-0` becomes `0`. JS
/// `Math.round` differs on negative `.5`, so the SDK does the same.
fn round_to(value: f64, scale: f64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    let rounded = (value.abs() * scale).round() / scale;
    let signed = if value < 0.0 { -rounded } else { rounded };
    Some(if signed == 0.0 { 0.0 } else { signed })
}

fn millis(amount: u64, per_second: u64) -> Option<u64> {
    if amount == 0 || per_second == 0 {
        return None;
    }
    let ms = (amount as f64 * 1000.0 / per_second as f64).round();
    (ms >= 1.0 && ms <= MAX_SAFE_INTEGER as f64).then_some(ms as u64)
}

fn first_value(text: &str) -> &str {
    let text = text.trim_start_matches('\0');
    match text.find('\0') {
        Some(end) => &text[..end],
        None => text,
    }
}

fn clean_text(text: &str) -> Option<String> {
    let visible: String = first_value(text).chars().filter(|c| !c.is_control()).collect();
    let cut: String = visible.trim().chars().take(MAX_TEXT_CHARS).collect();
    let out = cut.trim_end();
    (!out.is_empty()).then(|| out.to_string())
}

fn clean_bytes(bytes: &[u8]) -> Option<String> {
    clean_text(&String::from_utf8_lossy(bytes))
}

fn digits(text: &[u8]) -> Option<u32> {
    if text.is_empty() {
        return None;
    }
    let mut value = 0u32;
    for c in text {
        if !c.is_ascii_digit() {
            return None;
        }
        value = value * 10 + (c - b'0') as u32;
    }
    Some(value)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
}

// `YYYY<sep>MM<sep>DD<t>HH:MM:SS`, as `YYYY-MM-DDTHH:MM:SS`.
fn parse_date_time(text: &[u8], date_sep: u8, time_sep: u8) -> Option<String> {
    if text.len() != 19
        || text[4] != date_sep
        || text[7] != date_sep
        || text[10] != time_sep
        || text[13] != b':'
        || text[16] != b':'
    {
        return None;
    }
    let year = digits(&text[0..4])?;
    let month = digits(&text[5..7])?;
    let day = digits(&text[8..10])?;
    let hour = digits(&text[11..13])?;
    let minute = digits(&text[14..16])?;
    let second = digits(&text[17..19])?;
    if year == 0
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    Some(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    ))
}

// `.123`, or nothing when it is not all digits. At most nanoseconds.
fn parse_fraction(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() || !text.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!(".{}", &text[..text.len().min(9)]))
}

// `Z`, `+HH:MM`, `+HHMM` or `+HH`, as `Z` or `+HH:MM`.
fn parse_offset(text: &str) -> Option<String> {
    let bytes = text.trim().as_bytes();
    if bytes == b"Z" {
        return Some("Z".to_string());
    }
    let sign = *bytes.first()?;
    if sign != b'+' && sign != b'-' {
        return None;
    }
    let rest = &bytes[1..];
    let (hour, minute) = match rest.len() {
        2 => (rest, &b"00"[..]),
        4 => (&rest[..2], &rest[2..]),
        5 if rest[2] == b':' => (&rest[..2], &rest[3..]),
        _ => return None,
    };
    let hours = digits(hour)?;
    let minutes = digits(minute)?;
    if hours > 14 || minutes > 59 {
        return None;
    }
    Some(format!("{}{:02}:{:02}", sign as char, hours, minutes))
}

// ISO 8601 as written in `com.apple.quicktime.creationdate`.
fn parse_iso_date_time(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 || !bytes.is_ascii() {
        return None;
    }
    let base = parse_date_time(&bytes[..19], b'-', b'T')?;
    let mut rest = &text[19..];
    let mut out = base;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let end = after_dot
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_dot.len());
        out.push_str(&parse_fraction(&after_dot[..end])?);
        rest = &after_dot[end..];
    }
    if !rest.is_empty() {
        out.push_str(&parse_offset(rest)?);
    }
    Some(out)
}

fn utc_from_mp4_time(seconds: u64) -> Option<String> {
    if seconds <= MP4_EPOCH_TO_UNIX_SECS {
        return None;
    }
    let unix = seconds - MP4_EPOCH_TO_UNIX_SECS;
    if unix > MAX_UNIX_SECS {
        return None;
    }
    let time = chrono::DateTime::from_timestamp(unix as i64, 0)?;
    Some(time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn gcd(mut a: i64, mut b: i64) -> i64 {
    while b != 0 {
        let next = a % b;
        a = b;
        b = next;
    }
    a
}

fn some_if_any<T: Default + PartialEq>(value: T) -> Option<T> {
    (value != T::default()).then_some(value)
}

// ---- EXIF: a TIFF structure, read in place ----

const TAG_MAKE: u16 = 0x010f;
const TAG_MODEL: u16 = 0x0110;
const TAG_ORIENTATION: u16 = 0x0112;
const TAG_EXIF_IFD: u16 = 0x8769;
const TAG_GPS_IFD: u16 = 0x8825;
const TAG_EXPOSURE_TIME: u16 = 0x829a;
const TAG_F_NUMBER: u16 = 0x829d;
const TAG_ISO: u16 = 0x8827;
const TAG_DATE_TIME_ORIGINAL: u16 = 0x9003;
const TAG_DATE_TIME_DIGITIZED: u16 = 0x9004;
const TAG_OFFSET_TIME_ORIGINAL: u16 = 0x9011;
const TAG_OFFSET_TIME_DIGITIZED: u16 = 0x9012;
const TAG_FOCAL_LENGTH: u16 = 0x920a;
const TAG_SUB_SEC_ORIGINAL: u16 = 0x9291;
const TAG_SUB_SEC_DIGITIZED: u16 = 0x9292;
const TAG_FOCAL_LENGTH_35MM: u16 = 0xa405;
const TAG_LENS_MODEL: u16 = 0xa434;
const TAG_GPS_LAT_REF: u16 = 0x0001;
const TAG_GPS_LAT: u16 = 0x0002;
const TAG_GPS_LON_REF: u16 = 0x0003;
const TAG_GPS_LON: u16 = 0x0004;
const TAG_GPS_ALT_REF: u16 = 0x0005;
const TAG_GPS_ALT: u16 = 0x0006;
const TAG_GPS_IMG_DIRECTION: u16 = 0x0011;
const TAG_GPS_H_ERROR: u16 = 0x001f;

#[derive(Clone, Copy)]
struct IfdEntry {
    kind: u16,
    count: u64,
    value_at: u64,
}

type Ifd = HashMap<u16, IfdEntry>;

fn exif_type_size(kind: u16) -> Option<u64> {
    match kind {
        1 | 2 | 6 | 7 | 129 => Some(1),
        3 | 8 => Some(2),
        4 | 9 | 11 | 13 => Some(4),
        5 | 10 | 12 => Some(8),
        _ => None,
    }
}

struct Tiff<'r, 'a> {
    reader: &'r mut Reader<'a>,
    base: u64,
    limit: u64,
    little_endian: bool,
}

impl Tiff<'_, '_> {
    fn u16_at(&self, b: &[u8], at: usize) -> R<u64> {
        if self.little_endian {
            le16(b, at)
        } else {
            be16(b, at)
        }
    }

    fn u32_at(&self, b: &[u8], at: usize) -> R<u64> {
        if self.little_endian {
            le32(b, at)
        } else {
            be32(b, at)
        }
    }

    async fn read_ifd(&mut self, offset: u64) -> R<Ifd> {
        if offset + 2 > self.limit {
            return Err(Stop);
        }
        let head = self.reader.read(self.base + offset, 2).await?;
        let count = self.u16_at(&head, 0)?;
        if offset + 2 + count * 12 > self.limit {
            return Err(Stop);
        }
        let first = self.base + offset + 2;
        let bytes = self.reader.read(first, count * 12).await?;
        let mut ifd = Ifd::new();
        for i in 0..count as usize {
            let at = i * 12;
            let tag = self.u16_at(&bytes, at)? as u16;
            let kind = self.u16_at(&bytes, at + 2)? as u16;
            let count = self.u32_at(&bytes, at + 4)?;
            ifd.entry(tag).or_insert(IfdEntry {
                kind,
                count,
                value_at: first + at as u64 + 8,
            });
        }
        Ok(ifd)
    }

    async fn value(&mut self, entry: &IfdEntry, max_bytes: u64) -> R<Vec<u8>> {
        let total = exif_type_size(entry.kind).ok_or(Stop)? * entry.count;
        let wanted = total.min(max_bytes);
        if total <= 4 {
            return self.reader.read(entry.value_at, wanted).await;
        }
        let field = self.reader.read(entry.value_at, 4).await?;
        let offset = self.u32_at(&field, 0)?;
        if offset + total > self.limit {
            return Err(Stop);
        }
        self.reader.read(self.base + offset, wanted).await
    }

    async fn text(&mut self, ifd: &Ifd, tag: u16) -> Option<String> {
        let entry = ifd.get(&tag)?;
        if entry.kind != 2 && entry.kind != 129 {
            return None;
        }
        let bytes = self.value(entry, MAX_TEXT_BYTES).await.ok()?;
        clean_bytes(&bytes)
    }

    async fn uint(&mut self, ifd: &Ifd, tag: u16) -> Option<u64> {
        let entry = ifd.get(&tag)?;
        if entry.count == 0 {
            return None;
        }
        match entry.kind {
            1 => {
                let bytes = self.value(entry, 1).await.ok()?;
                bytes.first().map(|b| *b as u64)
            }
            3 => {
                let bytes = self.value(entry, 2).await.ok()?;
                self.u16_at(&bytes, 0).ok()
            }
            4 | 13 => {
                let bytes = self.value(entry, 4).await.ok()?;
                self.u32_at(&bytes, 0).ok()
            }
            _ => None,
        }
    }

    // `count` (numerator, denominator) pairs, none with a zero denominator.
    async fn rationals(&mut self, ifd: &Ifd, tag: u16, count: usize) -> Option<Vec<(i64, i64)>> {
        let entry = ifd.get(&tag)?;
        if (entry.kind != 5 && entry.kind != 10) || entry.count < count as u64 {
            return None;
        }
        let bytes = self.value(entry, count as u64 * 8).await.ok()?;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let raw_num = self.u32_at(&bytes, i * 8).ok()?;
            let raw_den = self.u32_at(&bytes, i * 8 + 4).ok()?;
            let (num, den) = if entry.kind == 10 {
                (raw_num as u32 as i32 as i64, raw_den as u32 as i32 as i64)
            } else {
                (raw_num as i64, raw_den as i64)
            };
            if den == 0 {
                return None;
            }
            out.push((num, den));
        }
        Some(out)
    }

    async fn rational(&mut self, ifd: &Ifd, tag: u16) -> Option<f64> {
        let values = self.rationals(ifd, tag, 1).await?;
        Some(values[0].0 as f64 / values[0].1 as f64)
    }

    async fn sub_ifd(&mut self, ifd: &Ifd, tag: u16) -> Option<Ifd> {
        let offset = self.uint(ifd, tag).await?;
        self.read_ifd(offset).await.ok()
    }

    async fn degrees(&mut self, ifd: &Ifd, tag: u16, ref_tag: u16, negative: char) -> Option<f64> {
        let parts = self.rationals(ifd, tag, 3).await?;
        let reference = self.text(ifd, ref_tag).await?;
        let first = reference.chars().next()?.to_ascii_uppercase();
        let value = parts[0].0 as f64 / parts[0].1 as f64
            + parts[1].0 as f64 / parts[1].1 as f64 / 60.0
            + parts[2].0 as f64 / parts[2].1 as f64 / 3600.0;
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        let positive = match negative {
            'S' => 'N',
            _ => 'E',
        };
        if first == negative {
            Some(-value)
        } else if first == positive {
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct ExifFields {
    orientation: Option<u64>,
    captured_at: Option<String>,
    geo: Option<MediaGeo>,
    camera: Option<MediaCamera>,
    exposure: Option<MediaExposure>,
}

fn valid_geo(lat: f64, lon: f64) -> Option<(f64, f64)> {
    let lat = round_to(lat, 1e6)?;
    let lon = round_to(lon, 1e6)?;
    if !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lon)
        || (lat == 0.0 && lon == 0.0)
    {
        return None;
    }
    Some((lat, lon))
}

async fn read_captured_at(tiff: &mut Tiff<'_, '_>, exif: &Ifd) -> Option<String> {
    async fn date(tiff: &mut Tiff<'_, '_>, exif: &Ifd, tag: u16) -> Option<String> {
        let text = tiff.text(exif, tag).await?;
        parse_date_time(text.as_bytes(), b':', b' ')
    }
    let (base, digitized) = match date(tiff, exif, TAG_DATE_TIME_ORIGINAL).await {
        Some(base) => (base, false),
        None => (date(tiff, exif, TAG_DATE_TIME_DIGITIZED).await?, true),
    };
    let mut fraction = tiff.text(exif, TAG_SUB_SEC_ORIGINAL).await;
    let mut offset = tiff.text(exif, TAG_OFFSET_TIME_ORIGINAL).await;
    if digitized {
        if fraction.is_none() {
            fraction = tiff.text(exif, TAG_SUB_SEC_DIGITIZED).await;
        }
        if offset.is_none() {
            offset = tiff.text(exif, TAG_OFFSET_TIME_DIGITIZED).await;
        }
    }
    let mut out = base;
    if let Some(fraction) = fraction.as_deref().and_then(parse_fraction) {
        out.push_str(&fraction);
    }
    if let Some(offset) = offset.as_deref().and_then(parse_offset) {
        out.push_str(&offset);
    }
    Some(out)
}

async fn read_geo(tiff: &mut Tiff<'_, '_>, gps: &Ifd) -> Option<MediaGeo> {
    let lat = tiff.degrees(gps, TAG_GPS_LAT, TAG_GPS_LAT_REF, 'S').await?;
    let lon = tiff.degrees(gps, TAG_GPS_LON, TAG_GPS_LON_REF, 'W').await?;
    let (lat, lon) = valid_geo(lat, lon)?;
    let mut geo = MediaGeo {
        lat,
        lon,
        ..MediaGeo::default()
    };
    if let Some(alt) = tiff.rational(gps, TAG_GPS_ALT).await {
        let below = tiff.uint(gps, TAG_GPS_ALT_REF).await == Some(1);
        geo.alt = round_to(if below { -alt.abs() } else { alt }, 1e1);
    }
    geo.heading = tiff
        .rational(gps, TAG_GPS_IMG_DIRECTION)
        .await
        .filter(|heading| (0.0..=360.0).contains(heading))
        .and_then(|heading| round_to(heading, 1e1));
    geo.accuracy_m = tiff
        .rational(gps, TAG_GPS_H_ERROR)
        .await
        .filter(|accuracy| *accuracy >= 0.0)
        .and_then(|accuracy| round_to(accuracy, 1e1));
    Some(geo)
}

async fn read_exposure(tiff: &mut Tiff<'_, '_>, exif: &Ifd) -> Option<MediaExposure> {
    let positive = |value: Option<f64>, scale: f64| {
        value
            .filter(|v| *v > 0.0)
            .and_then(|v| round_to(v, scale))
    };
    let mut exposure = MediaExposure {
        iso: tiff.uint(exif, TAG_ISO).await.filter(|v| *v > 0),
        f_number: positive(tiff.rational(exif, TAG_F_NUMBER).await, 1e2),
        focal_length_mm: positive(tiff.rational(exif, TAG_FOCAL_LENGTH).await, 1e2),
        focal_length_35mm: tiff.uint(exif, TAG_FOCAL_LENGTH_35MM).await.filter(|v| *v > 0),
        ..MediaExposure::default()
    };
    if let Some(parts) = tiff.rationals(exif, TAG_EXPOSURE_TIME, 1).await {
        let (num, den) = parts[0];
        if num > 0 && den > 0 {
            let divisor = gcd(num, den);
            let (num, den) = (num / divisor, den / divisor);
            exposure.exposure_time = Some(if den == 1 {
                num.to_string()
            } else {
                format!("{}/{}", num, den)
            });
        }
    }
    some_if_any(exposure)
}

async fn read_exif(reader: &mut Reader<'_>, base: u64, limit: u64) -> Option<ExifFields> {
    if limit < 8 {
        return None;
    }
    let head = reader.read(base, 8).await.ok()?;
    let little_endian = match (head[0], head[1]) {
        (b'I', b'I') => true,
        (b'M', b'M') => false,
        _ => return None,
    };
    let mut tiff = Tiff {
        reader,
        base,
        limit,
        little_endian,
    };
    if tiff.u16_at(&head, 2).ok()? != 42 {
        return None;
    }
    let first = tiff.u32_at(&head, 4).ok()?;
    let ifd0 = tiff.read_ifd(first).await.ok()?;
    let mut fields = ExifFields {
        orientation: tiff.uint(&ifd0, TAG_ORIENTATION).await,
        ..ExifFields::default()
    };
    let exif = tiff.sub_ifd(&ifd0, TAG_EXIF_IFD).await.unwrap_or_default();
    let camera = MediaCamera {
        make: tiff.text(&ifd0, TAG_MAKE).await,
        model: tiff.text(&ifd0, TAG_MODEL).await,
        lens: tiff.text(&exif, TAG_LENS_MODEL).await,
    };
    fields.camera = some_if_any(camera);
    fields.captured_at = read_captured_at(&mut tiff, &exif).await;
    fields.exposure = read_exposure(&mut tiff, &exif).await;
    if let Some(gps) = tiff.sub_ifd(&ifd0, TAG_GPS_IFD).await {
        fields.geo = read_geo(&mut tiff, &gps).await;
    }
    Some(fields)
}

fn apply_exif(meta: &mut MediaMeta, fields: ExifFields) {
    meta.captured_at = fields.captured_at;
    meta.geo = fields.geo;
    meta.camera = fields.camera;
    meta.exposure = fields.exposure;
}

fn set_size(meta: &mut MediaMeta, width: u64, height: u64) {
    if width > 0 && height > 0 {
        meta.width = Some(width);
        meta.height = Some(height);
    }
}

// ---- Photos ----

async fn probe_jpeg(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let mut exif_at = None;
    let mut frame = None;
    let mut at = 2u64;
    for _ in 0..MAX_JPEG_SEGMENTS {
        let Ok(head) = reader.read(at, 2).await else {
            break;
        };
        if head[0] != 0xff {
            break;
        }
        let marker = head[1];
        if marker == 0xff {
            at += 1;
            continue;
        }
        if marker == 0x01 || (0xd0..=0xd8).contains(&marker) {
            at += 2;
            continue;
        }
        if marker == 0xd9 || marker == 0xda {
            break;
        }
        let Ok(length) = reader.read(at + 2, 2).await.and_then(|b| be16(&b, 0)) else {
            break;
        };
        if length < 2 {
            break;
        }
        if marker == 0xe1 && exif_at.is_none() && length >= 8 {
            if let Ok(prefix) = reader.read(at + 4, 6).await {
                if prefix == b"Exif\0\0" {
                    exif_at = Some((at + 10, length - 8));
                }
            }
        }
        if (0xc0..=0xcf).contains(&marker) && !matches!(marker, 0xc4 | 0xc8 | 0xcc) {
            if let Ok(sof) = reader.read(at + 4, 5).await {
                if let (Ok(height), Ok(width)) = (be16(&sof, 1), be16(&sof, 3)) {
                    frame = Some((width, height));
                }
            }
            break;
        }
        at += 2 + length;
    }
    let mut orientation = None;
    if let Some((base, limit)) = exif_at {
        if let Some(fields) = read_exif(reader, base, limit).await {
            orientation = fields.orientation;
            apply_exif(meta, fields);
        }
    }
    if let Some((width, height)) = frame {
        if matches!(orientation, Some(5..=8)) {
            set_size(meta, height, width);
        } else {
            set_size(meta, width, height);
        }
    }
}

async fn probe_png(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    if let Ok(head) = reader.read(8, 16).await {
        if &head[4..8] == b"IHDR" {
            if let (Ok(width), Ok(height)) = (be32(&head, 8), be32(&head, 12)) {
                set_size(meta, width, height);
            }
        }
    }
    let mut at = 8u64;
    for _ in 0..MAX_PNG_CHUNKS {
        let Ok(head) = reader.read(at, 8).await else {
            return;
        };
        let Ok(length) = be32(&head, 0) else {
            return;
        };
        match &head[4..8] {
            b"IDAT" | b"IEND" => return,
            b"eXIf" => {
                if let Some(fields) = read_exif(reader, at + 8, length).await {
                    apply_exif(meta, fields);
                }
                return;
            }
            _ => at += 12 + length,
        }
    }
}

async fn probe_gif(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    if let Ok(head) = reader.read(6, 4).await {
        if let (Ok(width), Ok(height)) = (le16(&head, 0), le16(&head, 2)) {
            set_size(meta, width, height);
        }
    }
}

async fn probe_webp(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let Ok(riff) = reader.read(0, 12).await else {
        return;
    };
    let Ok(riff_size) = le32(&riff, 4) else {
        return;
    };
    let end = reader.size.min(8 + riff_size);
    let mut extended = false;
    let mut at = 12u64;
    for _ in 0..MAX_RIFF_CHUNKS {
        if at + 8 > end {
            return;
        }
        let Ok(head) = reader.read(at, 8).await else {
            return;
        };
        let Ok(size) = le32(&head, 4) else {
            return;
        };
        let data = at + 8;
        match &head[0..4] {
            b"VP8 " if !extended => {
                if let Ok(frame) = reader.read(data, 10).await {
                    if frame[3..6] == [0x9d, 0x01, 0x2a] {
                        if let (Ok(width), Ok(height)) = (le16(&frame, 6), le16(&frame, 8)) {
                            set_size(meta, width & 0x3fff, height & 0x3fff);
                        }
                    }
                }
                return;
            }
            b"VP8L" if !extended => {
                if let Ok(frame) = reader.read(data, 5).await {
                    if frame[0] == 0x2f {
                        let width = 1 + (frame[1] as u64 | ((frame[2] & 0x3f) as u64) << 8);
                        let height = 1
                            + ((frame[2] >> 6) as u64
                                | (frame[3] as u64) << 2
                                | ((frame[4] & 0x0f) as u64) << 10);
                        set_size(meta, width, height);
                    }
                }
                return;
            }
            b"VP8X" => {
                let Ok(header) = reader.read(data, 10).await else {
                    return;
                };
                if let (Ok(width), Ok(height)) = (le24(&header, 4), le24(&header, 7)) {
                    set_size(meta, width + 1, height + 1);
                }
                if header[0] & 0x08 == 0 {
                    return;
                }
                extended = true;
            }
            b"EXIF" => {
                let Ok(prefix) = reader.read(data, size.min(6)).await else {
                    return;
                };
                let skip = if prefix == b"Exif\0\0" { 6 } else { 0 };
                if let Some(fields) = read_exif(reader, data + skip, size - skip).await {
                    apply_exif(meta, fields);
                }
                return;
            }
            _ => {}
        }
        at = data + size + (size & 1);
    }
}

async fn probe_tiff(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let size = reader.size;
    if let Some(fields) = read_exif(reader, 0, size).await {
        apply_exif(meta, fields);
    }
}

// ---- ISO BMFF: HEIF, MP4, MOV, m4a ----

struct IsoBox {
    kind: [u8; 4],
    at: u64,
    start: u64,
    end: u64,
    next: u64,
}

fn parse_iso_box(b: &[u8], at: u64, end: u64) -> R<Option<IsoBox>> {
    if at + 8 > end {
        return Ok(None);
    }
    let base = at as usize;
    let mut size = be32(b, base)?;
    let kind = [b[base + 4], b[base + 5], b[base + 6], b[base + 7]];
    let mut header = 8;
    if size == 1 {
        if at + 16 > end {
            return Ok(None);
        }
        size = be64(b, base + 8)?;
        header = 16;
    } else if size == 0 {
        size = end - at;
    }
    if size < header {
        return Ok(None);
    }
    Ok(Some(IsoBox {
        kind,
        at,
        start: at + header,
        end: end.min(at + size),
        next: at + size,
    }))
}

// A box of an in-memory buffer. The `at` and `end` are offsets in `b`.
fn iso_children(b: &[u8], start: u64, end: u64) -> R<Vec<IsoBox>> {
    let mut boxes = Vec::new();
    let mut at = start;
    while boxes.len() < MAX_ISO_BOXES {
        match parse_iso_box(b, at, end)? {
            Some(found) => {
                at = found.next;
                boxes.push(found);
            }
            None => break,
        }
    }
    Ok(boxes)
}

async fn read_iso_box(reader: &mut Reader<'_>, at: u64, end: u64) -> R<Option<IsoBox>> {
    if at + 8 > end {
        return Ok(None);
    }
    let head = reader.read(at, 8).await?;
    let mut size = be32(&head, 0)?;
    let kind = [head[4], head[5], head[6], head[7]];
    let mut header = 8;
    if size == 1 {
        if at + 16 > end {
            return Ok(None);
        }
        let wide = reader.read(at + 8, 8).await?;
        size = be64(&wide, 0)?;
        header = 16;
    } else if size == 0 {
        size = end - at;
    }
    if size < header {
        return Ok(None);
    }
    Ok(Some(IsoBox {
        kind,
        at,
        start: at + header,
        end: end.min(at + size),
        next: at + size,
    }))
}

// Boxes of `[start, end)` without reading their payload, so a large `stbl`
// table costs nothing.
async fn read_children(reader: &mut Reader<'_>, start: u64, end: u64) -> R<Vec<IsoBox>> {
    let mut boxes = Vec::new();
    let mut at = start;
    while boxes.len() < MAX_ISO_BOXES {
        match read_iso_box(reader, at, end).await? {
            Some(found) => {
                at = found.next;
                boxes.push(found);
            }
            None => break,
        }
    }
    Ok(boxes)
}

fn find_box<'b>(boxes: &'b [IsoBox], kind: &[u8; 4]) -> Option<&'b IsoBox> {
    boxes.iter().find(|b| &b.kind == kind)
}

/// Displayed size of a HEIF primary image from a buffer whose first boxes hold
/// `meta`: the `ispe` of `pitm`, swapped for a 90°/270° `irot`. Never reads EXIF
/// orientation, which `irot` already covers.
pub fn heif_image_size(b: &[u8]) -> Option<(u64, u64)> {
    let top = iso_children(b, 0, b.len() as u64).ok()?;
    let meta = find_box(&top, b"meta")?;
    let children = iso_children(b, meta.start + 4, meta.end).ok()?;
    let pitm = find_box(&children, b"pitm")?;
    let iprp = find_box(&children, b"iprp")?;
    let pitm_at = pitm.start as usize;
    let primary = if *b.get(pitm_at)? == 0 {
        be16(b, pitm_at + 4).ok()?
    } else {
        be32(b, pitm_at + 4).ok()?
    };
    let props = iso_children(b, iprp.start, iprp.end).ok()?;
    let ipco = find_box(&props, b"ipco")?;
    let properties = iso_children(b, ipco.start, ipco.end).ok()?;
    let mut width = 0;
    let mut height = 0;
    let mut quarter_turns = 0;
    for ipma in props.iter().filter(|p| &p.kind == b"ipma") {
        let mut at = ipma.start as usize;
        let version = *b.get(at)?;
        let wide_index = b.get(at + 3)? & 1 == 1;
        at += 4;
        let count = be32(b, at).ok()?;
        at += 4;
        for _ in 0..count {
            if at as u64 >= ipma.end {
                break;
            }
            let item = if version < 1 {
                be16(b, at).ok()?
            } else {
                be32(b, at).ok()?
            };
            at += if version < 1 { 2 } else { 4 };
            let associations = *b.get(at)?;
            at += 1;
            for _ in 0..associations {
                let index = if wide_index {
                    be16(b, at).ok()? & 0x7fff
                } else {
                    (*b.get(at)? & 0x7f) as u64
                } as usize;
                at += if wide_index { 2 } else { 1 };
                if item != primary || index == 0 {
                    continue;
                }
                let Some(property) = properties.get(index - 1) else {
                    continue;
                };
                let start = property.start as usize;
                if &property.kind == b"ispe" {
                    width = be32(b, start + 4).ok()?;
                    height = be32(b, start + 8).ok()?;
                } else if &property.kind == b"irot" {
                    quarter_turns = *b.get(start)? & 3;
                }
            }
        }
    }
    if width == 0 || height == 0 {
        return None;
    }
    Some(if quarter_turns % 2 == 1 {
        (height, width)
    } else {
        (width, height)
    })
}

// The item id of the first `Exif` item in `iinf`.
fn heif_exif_item(b: &[u8], iinf: &IsoBox) -> R<Option<u64>> {
    let at = iinf.start as usize;
    let version = *b.get(at).ok_or(Stop)?;
    let first = iinf.start + if version == 0 { 6 } else { 8 };
    for infe in iso_children(b, first, iinf.end)? {
        if &infe.kind != b"infe" {
            continue;
        }
        let at = infe.start as usize;
        let version = *b.get(at).ok_or(Stop)?;
        let (item, kind_at) = match version {
            2 => (be16(b, at + 4)?, at + 8),
            3 => (be32(b, at + 4)?, at + 10),
            _ => continue,
        };
        if b.get(kind_at..kind_at + 4) == Some(&b"Exif"[..]) {
            return Ok(Some(item));
        }
    }
    Ok(None)
}

// File offset and length of a single-extent item stored in the file, from `iloc`.
fn heif_item_extent(b: &[u8], iloc: &IsoBox, wanted: u64) -> R<Option<(u64, u64)>> {
    let mut at = iloc.start as usize;
    let version = *b.get(at).ok_or(Stop)?;
    if version > 2 {
        return Ok(None);
    }
    let sizes = *b.get(at + 4).ok_or(Stop)?;
    let sizes2 = *b.get(at + 5).ok_or(Stop)?;
    let offset_size = (sizes >> 4) as usize;
    let length_size = (sizes & 0x0f) as usize;
    let base_size = (sizes2 >> 4) as usize;
    let index_size = if version >= 1 { (sizes2 & 0x0f) as usize } else { 0 };
    at += 6;
    let count = if version < 2 { be16(b, at)? } else { be32(b, at)? };
    at += if version < 2 { 2 } else { 4 };
    for _ in 0..count {
        let item = if version < 2 { be16(b, at)? } else { be32(b, at)? };
        at += if version < 2 { 2 } else { 4 };
        let method = if version >= 1 {
            let value = be16(b, at)? & 0x0f;
            at += 2;
            value
        } else {
            0
        };
        at += 2;
        let base = be_uint(b, at, base_size)?;
        at += base_size;
        let extents = be16(b, at)?;
        at += 2;
        let mut extent = None;
        for _ in 0..extents {
            at += index_size;
            let offset = be_uint(b, at, offset_size)?;
            at += offset_size;
            let length = be_uint(b, at, length_size)?;
            at += length_size;
            extent = Some((base + offset, length));
        }
        if item == wanted {
            return Ok(if method == 0 && extents == 1 { extent } else { None });
        }
    }
    Ok(None)
}

async fn find_top_level_box(reader: &mut Reader<'_>, kind: &[u8; 4]) -> R<Option<IsoBox>> {
    let size = reader.size;
    let mut at = 0u64;
    for _ in 0..MAX_ISO_BOXES {
        let Some(found) = read_iso_box(reader, at, size).await? else {
            break;
        };
        if &found.kind == kind {
            return Ok(Some(found));
        }
        at = found.next;
    }
    Ok(None)
}

async fn probe_heif(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let Ok(Some(meta_box)) = find_top_level_box(reader, b"meta").await else {
        return;
    };
    let Ok(bytes) = reader.read(meta_box.at, meta_box.end - meta_box.at).await else {
        return;
    };
    if let Some((width, height)) = heif_image_size(&bytes) {
        set_size(meta, width, height);
    }
    let Some(extent) = heif_exif_extent(&bytes) else {
        return;
    };
    let (offset, length) = extent;
    if length < 4 {
        return;
    }
    let Ok(prefix) = reader.read(offset, 4).await else {
        return;
    };
    let Ok(tiff_offset) = be32(&prefix, 0) else {
        return;
    };
    if tiff_offset + 4 > length {
        return;
    }
    if let Some(fields) = read_exif(reader, offset + 4 + tiff_offset, length - 4 - tiff_offset).await {
        apply_exif(meta, fields);
    }
}

fn heif_exif_extent(b: &[u8]) -> Option<(u64, u64)> {
    let top = iso_children(b, 0, b.len() as u64).ok()?;
    let meta = find_box(&top, b"meta")?;
    let children = iso_children(b, meta.start + 4, meta.end).ok()?;
    let item = heif_exif_item(b, find_box(&children, b"iinf")?).ok()??;
    heif_item_extent(b, find_box(&children, b"iloc")?, item).ok()?
}

#[derive(Default)]
struct IsoMedia {
    has_video: bool,
    has_audio: bool,
    width: u64,
    height: u64,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    duration_ms: Option<u64>,
    mvhd_time: Option<String>,
    creation_date: Option<String>,
    location: Option<MediaGeo>,
    accuracy: Option<f64>,
    make: Option<String>,
    model: Option<String>,
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
}

fn codec_name(format: &[u8]) -> Option<&'static str> {
    match format {
        b"avc1" | b"avc3" => Some("h264"),
        b"hvc1" | b"hev1" => Some("hevc"),
        b"av01" => Some("av1"),
        b"vp09" => Some("vp9"),
        b"mp4a" => Some("aac"),
        _ => None,
    }
}

// `+34.9949-135.7567+012.300/`. Only decimal degrees; other ISO 6709 forms fall
// out of the coordinate range and are dropped.
fn parse_iso6709(text: &str) -> Option<MediaGeo> {
    let body = text.strip_suffix('/').unwrap_or(text);
    let mut parts: Vec<f64> = Vec::new();
    let mut start = None;
    for (index, c) in body.char_indices() {
        match c {
            '+' | '-' => {
                if let Some(from) = start {
                    parts.push(parse_coordinate(&body[from..index])?);
                }
                start = Some(index);
            }
            '0'..='9' | '.' if start.is_some() => {}
            _ => return None,
        }
    }
    parts.push(parse_coordinate(&body[start?..])?);
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }
    let (lat, lon) = valid_geo(parts[0], parts[1])?;
    Some(MediaGeo {
        lat,
        lon,
        alt: parts.get(2).and_then(|alt| round_to(*alt, 1e1)),
        ..MediaGeo::default()
    })
}

fn parse_coordinate(text: &str) -> Option<f64> {
    let unsigned = &text[1..];
    let digits = unsigned.bytes().filter(u8::is_ascii_digit).count();
    let dots = unsigned.bytes().filter(|c| *c == b'.').count();
    if digits == 0 || dots > 1 || unsigned.starts_with('.') || unsigned.ends_with('.') {
        return None;
    }
    text.parse().ok()
}

fn apply_item(media: &mut IsoMedia, key: &str, text: &str) {
    let clean = clean_text(text);
    match key {
        "com.apple.quicktime.creationdate" => {
            if media.creation_date.is_none() {
                media.creation_date = clean.as_deref().and_then(parse_iso_date_time);
            }
        }
        "com.apple.quicktime.location.ISO6709" => {
            if media.location.is_none() {
                media.location = clean.as_deref().and_then(parse_iso6709);
            }
        }
        "com.apple.quicktime.location.accuracy.horizontal" => {
            media.accuracy = clean
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value >= 0.0)
                .and_then(|value| round_to(value, 1e1));
        }
        "com.apple.quicktime.make" => media.make = clean,
        "com.apple.quicktime.model" => media.model = clean,
        _ => {}
    }
}

async fn read_item_text(reader: &mut Reader<'_>, item: &IsoBox) -> R<Option<String>> {
    for child in read_children(reader, item.start, item.end).await? {
        if &child.kind != b"data" || child.end < child.start + 8 {
            continue;
        }
        let head = reader.read(child.start, 4).await?;
        if be32(&head, 0)? & 0x00ff_ffff != 1 {
            return Ok(None);
        }
        let length = (child.end - child.start - 8).min(MAX_TEXT_BYTES);
        let bytes = reader.read(child.start + 8, length).await?;
        return Ok(Some(String::from_utf8_lossy(&bytes).into_owned()));
    }
    Ok(None)
}

// `meta`: a FullBox in ISO files, a plain box in QuickTime. Holds `ilst`, keyed
// by `keys` in QuickTime and by four-character codes in iTunes.
async fn read_meta_box(reader: &mut Reader<'_>, meta: &IsoBox, media: &mut IsoMedia) -> R<()> {
    if meta.end < meta.start + 8 {
        return Ok(());
    }
    let probe = reader.read(meta.start + 4, 4).await?;
    let first = if probe == b"hdlr" { meta.start } else { meta.start + 4 };
    let children = read_children(reader, first, meta.end).await?;
    let mut keys: Vec<String> = Vec::new();
    if let Some(keys_box) = find_box(&children, b"keys") {
        let head = reader.read(keys_box.start, 8).await?;
        let count = be32(&head, 4)?;
        let mut at = keys_box.start + 8;
        for _ in 0..count.min(MAX_ISO_BOXES as u64) {
            let Some(entry) = read_iso_box(reader, at, keys_box.end).await? else {
                break;
            };
            let length = (entry.end - entry.start).min(MAX_TEXT_BYTES);
            let bytes = reader.read(entry.start, length).await?;
            keys.push(String::from_utf8_lossy(&bytes).into_owned());
            at = entry.next;
        }
    }
    let Some(ilst) = find_box(&children, b"ilst") else {
        return Ok(());
    };
    for item in read_children(reader, ilst.start, ilst.end).await? {
        let index = u32::from_be_bytes(item.kind) as usize;
        if index >= 1 && index <= keys.len() {
            let key = keys[index - 1].clone();
            if let Some(text) = read_item_text(reader, &item).await? {
                apply_item(media, &key, &text);
            }
            continue;
        }
        let slot = match &item.kind {
            b"\xa9nam" => 0,
            b"\xa9ART" => 1,
            b"\xa9alb" => 2,
            _ => continue,
        };
        if let Some(text) = read_item_text(reader, &item).await? {
            let value = clean_text(&text);
            match slot {
                0 => media.title = value,
                1 => media.artist = value,
                _ => media.album = value,
            }
        }
    }
    Ok(())
}

async fn read_udta(reader: &mut Reader<'_>, udta: &IsoBox, media: &mut IsoMedia) -> R<()> {
    for child in read_children(reader, udta.start, udta.end).await? {
        if &child.kind == b"\xa9xyz" && child.end >= child.start + 4 {
            let head = reader.read(child.start, 2).await?;
            let length = be16(&head, 0)?
                .min(child.end - child.start - 4)
                .min(MAX_TEXT_BYTES);
            let bytes = reader.read(child.start + 4, length).await?;
            if media.location.is_none() {
                media.location = clean_bytes(&bytes).as_deref().and_then(parse_iso6709);
            }
        } else if &child.kind == b"meta" {
            read_meta_box(reader, &child, media).await?;
        }
    }
    Ok(())
}

async fn read_mvhd(reader: &mut Reader<'_>, mvhd: &IsoBox, media: &mut IsoMedia) -> R<()> {
    let bytes = reader.read(mvhd.start, (mvhd.end - mvhd.start).min(32)).await?;
    let version = *bytes.first().ok_or(Stop)?;
    let (created, timescale, duration) = if version == 1 {
        (be64(&bytes, 4), be32(&bytes, 20)?, be64(&bytes, 24))
    } else {
        (be32(&bytes, 4), be32(&bytes, 12)?, be32(&bytes, 16))
    };
    media.mvhd_time = created.ok().and_then(utc_from_mp4_time);
    media.duration_ms = duration
        .ok()
        .filter(|value| version == 1 || *value != 0xffff_ffff)
        .and_then(|value| millis(value, timescale));
    Ok(())
}

async fn read_trak(reader: &mut Reader<'_>, trak: &IsoBox, media: &mut IsoMedia) -> R<()> {
    let children = read_children(reader, trak.start, trak.end).await?;
    let Some(mdia) = find_box(&children, b"mdia") else {
        return Ok(());
    };
    let mdia_children = read_children(reader, mdia.start, mdia.end).await?;
    let Some(hdlr) = find_box(&mdia_children, b"hdlr") else {
        return Ok(());
    };
    let handler = reader.read(hdlr.start + 8, 4).await?;
    let is_video = handler == b"vide";
    let is_audio = handler == b"soun";
    if !is_video && !is_audio {
        return Ok(());
    }
    if is_audio {
        media.has_audio = true;
    } else {
        media.has_video = true;
    }
    let mut format = None;
    if let Some(minf) = find_box(&mdia_children, b"minf") {
        let minf_children = read_children(reader, minf.start, minf.end).await?;
        if let Some(stbl) = find_box(&minf_children, b"stbl") {
            let stbl_children = read_children(reader, stbl.start, stbl.end).await?;
            if let Some(stsd) = find_box(&stbl_children, b"stsd") {
                if stsd.end >= stsd.start + 16 {
                    format = Some(reader.read(stsd.start + 12, 4).await?);
                }
            }
        }
    }
    let codec = format.as_deref().and_then(codec_name).map(str::to_string);
    if is_audio {
        if media.audio_codec.is_none() {
            media.audio_codec = codec;
        }
        return Ok(());
    }
    if media.width > 0 {
        return Ok(());
    }
    let Some(tkhd) = find_box(&children, b"tkhd") else {
        return Ok(());
    };
    let bytes = reader.read(tkhd.start, (tkhd.end - tkhd.start).min(96)).await?;
    let version = *bytes.first().ok_or(Stop)?;
    let matrix = if version == 1 { 52 } else { 40 };
    let fixed = |at: usize| -> R<u64> { Ok((be32(&bytes, at)? + 0x8000) >> 16) };
    let width = fixed(matrix + 36)?;
    let height = fixed(matrix + 40)?;
    if width == 0 || height == 0 {
        return Ok(());
    }
    let a = be32(&bytes, matrix)?;
    let b = be32(&bytes, matrix + 4)?;
    let c = be32(&bytes, matrix + 12)?;
    let d = be32(&bytes, matrix + 16)?;
    let quarter_turn = a == 0 && d == 0 && b != 0 && c != 0;
    media.width = if quarter_turn { height } else { width };
    media.height = if quarter_turn { width } else { height };
    media.video_codec = codec;
    Ok(())
}

async fn read_moov(reader: &mut Reader<'_>, media: &mut IsoMedia) -> R<()> {
    let Some(moov) = find_top_level_box(reader, b"moov").await? else {
        return Ok(());
    };
    for child in read_children(reader, moov.start, moov.end).await? {
        match &child.kind {
            b"mvhd" => read_mvhd(reader, &child, media).await?,
            b"trak" => read_trak(reader, &child, media).await?,
            b"udta" => read_udta(reader, &child, media).await?,
            b"meta" => read_meta_box(reader, &child, media).await?,
            _ => {}
        }
    }
    Ok(())
}

async fn probe_iso_media(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let mut media = IsoMedia::default();
    let _ = read_moov(reader, &mut media).await;
    if meta.mime.as_deref() == Some("video/mp4") && !media.has_video && media.has_audio {
        meta.mime = Some("audio/mp4".to_string());
    }
    meta.duration_ms = media.duration_ms;
    meta.audio_codec = media.audio_codec;
    if meta.mime.as_deref().is_some_and(|mime| mime.starts_with("audio/")) {
        meta.title = media.title;
        meta.artist = media.artist;
        meta.album = media.album;
        return;
    }
    set_size(meta, media.width, media.height);
    meta.video_codec = media.video_codec;
    meta.captured_at = media.creation_date.or(media.mvhd_time);
    meta.geo = media.location.map(|mut geo| {
        geo.accuracy_m = media.accuracy;
        geo
    });
    meta.camera = some_if_any(MediaCamera {
        make: media.make,
        model: media.model,
        lens: None,
    });
}

// ---- Audio ----

async fn probe_flac(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let Ok(head) = reader.read(4, 22).await else {
        return;
    };
    if head[0] & 0x7f != 0 {
        return;
    }
    let info = &head[4..];
    let rate = (info[10] as u64) << 12 | (info[11] as u64) << 4 | (info[12] as u64) >> 4;
    let Ok(low) = be32(info, 14) else {
        return;
    };
    let samples = ((info[13] & 0x0f) as u64) * (1u64 << 32) + low;
    meta.duration_ms = millis(samples, rate);
}

async fn probe_wav(reader: &mut Reader<'_>, meta: &mut MediaMeta) {
    let Ok(riff) = reader.read(0, 12).await else {
        return;
    };
    let Ok(riff_size) = le32(&riff, 4) else {
        return;
    };
    let end = reader.size.min(8 + riff_size);
    let mut byte_rate = None;
    let mut data_size = None;
    let mut at = 12u64;
    for _ in 0..MAX_RIFF_CHUNKS {
        if at + 8 > end {
            break;
        }
        let Ok(head) = reader.read(at, 8).await else {
            break;
        };
        let Ok(size) = le32(&head, 4) else {
            break;
        };
        let data = at + 8;
        if &head[0..4] == b"fmt " {
            let Ok(format) = reader.read(data, 12).await else {
                break;
            };
            byte_rate = le32(&format, 8).ok();
        } else if &head[0..4] == b"data" {
            data_size = Some(size.min(reader.size - data));
        }
        if byte_rate.is_some() && data_size.is_some() {
            break;
        }
        at = data + size + (size & 1);
    }
    if let (Some(rate), Some(size)) = (byte_rate, data_size) {
        meta.duration_ms = millis(size, rate);
    }
}

fn syncsafe(b: &[u8], at: usize) -> R<u64> {
    let s = b.get(at..at + 4).ok_or(Stop)?;
    if s.iter().any(|c| c & 0x80 != 0) {
        return Err(Stop);
    }
    Ok((s[0] as u64) << 21 | (s[1] as u64) << 14 | (s[2] as u64) << 7 | s[3] as u64)
}

fn decode_id3_text(b: &[u8]) -> Option<String> {
    let (encoding, payload) = b.split_first()?;
    let text = match encoding {
        0 => payload.iter().map(|c| *c as char).collect(),
        1 | 2 => {
            let (big_endian, body) = match (*encoding, payload) {
                (1, [0xfe, 0xff, rest @ ..]) => (true, rest),
                (1, [0xff, 0xfe, rest @ ..]) => (false, rest),
                (1, rest) => (false, rest),
                (_, rest) => (true, rest),
            };
            let units: Vec<u16> = body
                .chunks_exact(2)
                .map(|pair| {
                    if big_endian {
                        u16::from_be_bytes([pair[0], pair[1]])
                    } else {
                        u16::from_le_bytes([pair[0], pair[1]])
                    }
                })
                .collect();
            String::from_utf16_lossy(&units)
        }
        3 => String::from_utf8_lossy(payload).into_owned(),
        _ => return None,
    };
    clean_text(&text)
}

async fn read_id3(reader: &mut Reader<'_>, meta: &mut MediaMeta) -> R<()> {
    let header = reader.read(0, 10).await?;
    let version = header[3];
    if &header[..3] != b"ID3" || !(2..=4).contains(&version) || header[5] & 0x80 != 0 {
        return Ok(());
    }
    let end = (10 + syncsafe(&header, 6)?).min(reader.size);
    let mut at = 10u64;
    if header[5] & 0x40 != 0 {
        match version {
            3 => at += 4 + be32(&reader.read(at, 4).await?, 0)?,
            4 => at += syncsafe(&reader.read(at, 4).await?, 0)?,
            _ => return Ok(()),
        }
    }
    let header_size: u64 = if version == 2 { 6 } else { 10 };
    for _ in 0..MAX_ID3_FRAMES {
        if at + header_size > end {
            break;
        }
        let frame = reader.read(at, header_size).await?;
        if frame[0] == 0 {
            break;
        }
        let (id, size, skip): (&[u8], u64, bool) = match version {
            2 => (
                &frame[..3],
                (frame[3] as u64) << 16 | (frame[4] as u64) << 8 | frame[5] as u64,
                false,
            ),
            3 => (&frame[..4], be32(&frame, 4)?, frame[9] & 0xe0 != 0),
            _ => (&frame[..4], syncsafe(&frame, 4)?, frame[9] & 0x4f != 0),
        };
        let slot = match id {
            b"TIT2" | b"TT2" => Some(0),
            b"TPE1" | b"TP1" => Some(1),
            b"TALB" | b"TAL" => Some(2),
            _ => None,
        };
        if let (Some(slot), false, true) = (slot, skip, size > 0) {
            let length = size.min(MAX_TEXT_BYTES).min(end - (at + header_size));
            let bytes = reader.read(at + header_size, length).await?;
            let target = match slot {
                0 => &mut meta.title,
                1 => &mut meta.artist,
                _ => &mut meta.album,
            };
            if target.is_none() {
                *target = decode_id3_text(&bytes);
            }
            if meta.title.is_some() && meta.artist.is_some() && meta.album.is_some() {
                break;
            }
        }
        at += header_size + size;
    }
    Ok(())
}

/// The common meta of a file. Never fails: a field that cannot be determined is
/// omitted. `name` gives the extension; `declared_mime` is what the caller
/// knows about a file that is not media.
pub async fn probe_media_meta(
    source: &dyn MediaByteSource,
    name: &str,
    declared_mime: Option<&str>,
) -> MediaMeta {
    let mut reader = Reader::new(source);
    let head = reader
        .read(0, reader.size.min(SNIFF_BYTES))
        .await
        .unwrap_or_default();
    let mut meta = MediaMeta {
        mime: resolve_mime(sniff_magic(&head), name, declared_mime),
        ..MediaMeta::default()
    };
    match meta.mime.clone().as_deref() {
        Some("image/jpeg") => probe_jpeg(&mut reader, &mut meta).await,
        Some("image/png") => probe_png(&mut reader, &mut meta).await,
        Some("image/gif") => probe_gif(&mut reader, &mut meta).await,
        Some("image/webp") => probe_webp(&mut reader, &mut meta).await,
        Some("image/heic") | Some("image/heif") | Some("image/avif") => {
            probe_heif(&mut reader, &mut meta).await
        }
        Some("image/tiff") => probe_tiff(&mut reader, &mut meta).await,
        Some("video/mp4") | Some("video/quicktime") | Some("audio/mp4") => {
            probe_iso_media(&mut reader, &mut meta).await
        }
        Some("audio/flac") => probe_flac(&mut reader, &mut meta).await,
        Some("audio/wav") => probe_wav(&mut reader, &mut meta).await,
        Some("audio/mpeg") => {
            let _ = read_id3(&mut reader, &mut meta).await;
        }
        _ => {}
    }
    meta
}

/// [`probe_media_meta`] of a local file. A file that cannot be opened has no meta.
pub async fn probe_local_file_media_meta(
    path: impl AsRef<Path>,
    name: &str,
    declared_mime: Option<&str>,
) -> MediaMeta {
    match LocalFileByteSource::open(path).await {
        Ok(source) => probe_media_meta(&source, name, declared_mime).await,
        Err(_) => MediaMeta {
            mime: resolve_mime(None, name, declared_mime),
            ..MediaMeta::default()
        },
    }
}

#[cfg(test)]
#[path = "media_meta_tests.rs"]
mod tests;
