//! Run with `--nocapture`, `test_media_meta_vectors` prints the array stored as
//! `tests/media_meta_vectors.json` in buckyos-websdk: `src/file_meta.ts` must
//! produce the same `expected` for every entry.

use super::*;
use base64::Engine;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

// ---- TIFF ----

#[derive(Clone)]
enum E {
    Ascii(u16, &'static str),
    Raw(u16, u16, u32, Vec<u8>),
    Short(u16, u16),
    Long(u16, u32),
    Byte(u16, u8),
    Rat(u16, Vec<(u32, u32)>),
}

struct W {
    le: bool,
}

impl W {
    fn u16(&self, v: u16) -> Vec<u8> {
        if self.le {
            v.to_le_bytes().to_vec()
        } else {
            v.to_be_bytes().to_vec()
        }
    }
    fn u32(&self, v: u32) -> Vec<u8> {
        if self.le {
            v.to_le_bytes().to_vec()
        } else {
            v.to_be_bytes().to_vec()
        }
    }
    fn rats(&self, values: &[(u32, u32)]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|(n, d)| [self.u32(*n), self.u32(*d)].concat())
            .collect()
    }
}

fn entry_parts(w: &W, e: &E) -> (u16, u16, u32, Vec<u8>) {
    match e {
        E::Ascii(tag, text) => {
            let mut bytes = text.as_bytes().to_vec();
            bytes.push(0);
            (*tag, 2, bytes.len() as u32, bytes)
        }
        E::Raw(tag, kind, count, bytes) => (*tag, *kind, *count, bytes.clone()),
        E::Short(tag, v) => (*tag, 3, 1, w.u16(*v)),
        E::Long(tag, v) => (*tag, 4, 1, w.u32(*v)),
        E::Byte(tag, v) => (*tag, 1, 1, vec![*v]),
        E::Rat(tag, values) => (*tag, 5, values.len() as u32, w.rats(values)),
    }
}

fn write_ifd(w: &W, out: &mut Vec<u8>, entries: &[E]) {
    let start = out.len();
    let values_at = start + 2 + entries.len() * 12 + 4;
    let mut values = Vec::new();
    out.extend(w.u16(entries.len() as u16));
    for e in entries {
        let (tag, kind, count, bytes) = entry_parts(w, e);
        out.extend(w.u16(tag));
        out.extend(w.u16(kind));
        out.extend(w.u32(count));
        if bytes.len() <= 4 {
            let mut inline = bytes.clone();
            inline.resize(4, 0);
            out.extend(inline);
        } else {
            out.extend(w.u32((values_at + values.len()) as u32));
            values.extend(bytes);
        }
    }
    out.extend(w.u32(0));
    out.extend(values);
}

fn ifd_size(w: &W, entries: &[E]) -> usize {
    let mut out = Vec::new();
    write_ifd(w, &mut out, entries);
    out.len()
}

fn tiff(le: bool, ifd0: &[E], exif: &[E], gps: &[E]) -> Vec<u8> {
    let w = W { le };
    let mut root = ifd0.to_vec();
    if !exif.is_empty() {
        root.push(E::Long(0x8769, 0));
    }
    if !gps.is_empty() {
        root.push(E::Long(0x8825, 0));
    }
    let exif_at = 8 + ifd_size(&w, &root);
    let gps_at = exif_at + if exif.is_empty() { 0 } else { ifd_size(&w, exif) };
    for e in root.iter_mut() {
        match e {
            E::Long(0x8769, v) => *v = exif_at as u32,
            E::Long(0x8825, v) => *v = gps_at as u32,
            _ => {}
        }
    }
    let mut out = if le { b"II".to_vec() } else { b"MM".to_vec() };
    out.extend(w.u16(42));
    out.extend(w.u32(8));
    write_ifd(&w, &mut out, &root);
    if !exif.is_empty() {
        write_ifd(&w, &mut out, exif);
    }
    if !gps.is_empty() {
        write_ifd(&w, &mut out, gps);
    }
    out
}

fn camera_ifd(orientation: Option<u16>) -> Vec<E> {
    let mut ifd = vec![E::Ascii(0x010f, "Sony"), E::Ascii(0x0110, "ILCE-7M3")];
    if let Some(o) = orientation {
        ifd.push(E::Short(0x0112, o));
    }
    ifd
}

fn photo_exif() -> Vec<E> {
    vec![
        E::Rat(0x829a, vec![(10, 1200)]),
        E::Rat(0x829d, vec![(28, 10)]),
        E::Short(0x8827, 400),
        E::Ascii(0x9003, "2024:02:29 13:05:09"),
        E::Ascii(0x9004, "2020:01:01 00:00:00"),
        E::Ascii(0x9011, "+08:00"),
        E::Ascii(0x9291, "125"),
        E::Rat(0x920a, vec![(5000, 100)]),
        E::Short(0xa405, 75),
        E::Ascii(0xa434, "FE 50mm F1.8"),
    ]
}

fn north_east_gps() -> Vec<E> {
    vec![
        E::Ascii(0x0001, "N"),
        E::Rat(0x0002, vec![(34, 1), (59, 1), (4164, 100)]),
        E::Ascii(0x0003, "E"),
        E::Rat(0x0004, vec![(135, 1), (45, 1), (2412, 100)]),
        E::Byte(0x0005, 0),
        E::Rat(0x0006, vec![(1234, 100)]),
        E::Rat(0x0011, vec![(9050, 100)]),
        E::Rat(0x001f, vec![(65, 10)]),
    ]
}

fn south_west_gps() -> Vec<E> {
    vec![
        E::Ascii(0x0001, "S"),
        E::Rat(0x0002, vec![(33, 1), (51, 1), (5400, 100)]),
        E::Ascii(0x0003, "W"),
        E::Rat(0x0004, vec![(70, 1), (40, 1), (0, 1)]),
        E::Byte(0x0005, 1),
        E::Rat(0x0006, vec![(75, 10)]),
    ]
}

fn expected_exposure() -> Value {
    json!({
        "exposure_time": "1/120",
        "f_number": 2.8,
        "focal_length_35mm": 75,
        "focal_length_mm": 50.0,
        "iso": 400,
    })
}

fn expected_camera() -> Value {
    json!({"make": "Sony", "model": "ILCE-7M3", "lens": "FE 50mm F1.8"})
}

fn expected_geo() -> Value {
    json!({"lat": 34.9949, "lon": 135.7567, "alt": 12.3, "heading": 90.5, "accuracy_m": 6.5})
}

// ---- Containers ----

fn jpeg_segment(marker: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0xff, marker];
    out.extend(((payload.len() + 2) as u16).to_be_bytes());
    out.extend(payload);
    out
}

fn jpeg(exif: Option<Vec<u8>>, width: u16, height: u16) -> Vec<u8> {
    let mut out = vec![0xff, 0xd8];
    out.extend(jpeg_segment(0xe0, b"JFIF\0\x01\x02\0\0\x01\0\x01\0\0"));
    out.extend(jpeg_segment(0xe1, b"http://ns.adobe.com/xap/1.0/\0<x:xmpmeta/>"));
    if let Some(exif) = exif {
        let mut payload = b"Exif\0\0".to_vec();
        payload.extend(exif);
        out.extend(jpeg_segment(0xe1, &payload));
    }
    let mut sof = vec![8];
    sof.extend(height.to_be_bytes());
    sof.extend(width.to_be_bytes());
    sof.extend([1, 1, 0x11, 0]);
    out.extend(jpeg_segment(0xc0, &sof));
    out.extend([0xff, 0xda, 0, 2, 0xff, 0xd9]);
    out
}

fn png_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut out = (data.len() as u32).to_be_bytes().to_vec();
    out.extend(kind);
    out.extend(data);
    out.extend([0, 0, 0, 0]);
    out
}

fn png(exif: Option<Vec<u8>>, width: u32, height: u32) -> Vec<u8> {
    let mut out = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    let mut ihdr = width.to_be_bytes().to_vec();
    ihdr.extend(height.to_be_bytes());
    ihdr.extend([8, 6, 0, 0, 0]);
    out.extend(png_chunk(b"IHDR", &ihdr));
    if let Some(exif) = exif {
        out.extend(png_chunk(b"eXIf", &exif));
    }
    out.extend(png_chunk(b"IDAT", &[0; 8]));
    out.extend(png_chunk(b"IEND", &[]));
    out
}

fn riff_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut out = kind.to_vec();
    out.extend((data.len() as u32).to_le_bytes());
    out.extend(data);
    if data.len() % 2 == 1 {
        out.push(0);
    }
    out
}

fn riff(form: &[u8; 4], chunks: &[Vec<u8>]) -> Vec<u8> {
    let body: Vec<u8> = chunks.concat();
    let mut out = b"RIFF".to_vec();
    out.extend(((body.len() + 4) as u32).to_le_bytes());
    out.extend(form);
    out.extend(body);
    out
}

fn webp_vp8x(width: u32, height: u32, exif: Option<Vec<u8>>) -> Vec<u8> {
    let mut vp8x = vec![if exif.is_some() { 0x08 } else { 0 }, 0, 0, 0];
    vp8x.extend(&(width - 1).to_le_bytes()[..3]);
    vp8x.extend(&(height - 1).to_le_bytes()[..3]);
    let mut chunks = vec![
        riff_chunk(b"VP8X", &vp8x),
        riff_chunk(b"VP8 ", &[0; 11]),
    ];
    if let Some(exif) = exif {
        chunks.push(riff_chunk(b"EXIF", &exif));
    }
    riff(b"WEBP", &chunks)
}

fn iso_box(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let mut out = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
    out.extend(kind);
    out.extend(payload);
    out
}

fn full_box(kind: &[u8; 4], version: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![version, 0, 0, 0];
    body.extend(payload);
    iso_box(kind, &body)
}

fn ftyp(brand: &[u8; 4], compatible: &[&[u8; 4]]) -> Vec<u8> {
    let mut body = brand.to_vec();
    body.extend([0, 0, 0, 0]);
    for c in compatible {
        body.extend(*c);
    }
    iso_box(b"ftyp", &body)
}

fn ispe(width: u32, height: u32) -> Vec<u8> {
    let mut body = width.to_be_bytes().to_vec();
    body.extend(height.to_be_bytes());
    full_box(b"ispe", 0, &body)
}

// A grid primary (item 1) with 2 tiles, as an iPhone writes it. `prefix` is
// the 4-byte TIFF header offset plus `Exif\0\0` of the item data.
fn heic(
    grid: (u32, u32),
    tile: (u32, u32),
    irot: Option<u8>,
    exif: Option<Vec<u8>>,
    brand: &[u8; 4],
) -> Vec<u8> {
    let ftyp_box = ftyp(brand, &[b"mif1", b"heic"]);
    let mut infe = Vec::new();
    for (id, kind) in [(1u16, b"grid"), (2, b"hvc1"), (3, b"hvc1"), (4, b"Exif")] {
        let mut body = id.to_be_bytes().to_vec();
        body.extend([0, 0]);
        body.extend(kind);
        body.push(0);
        infe.extend(full_box(b"infe", 2, &body));
    }
    let mut iinf_body = 4u16.to_be_bytes().to_vec();
    iinf_body.extend(infe);
    let iinf = full_box(b"iinf", 0, &iinf_body);
    let mut ipco = ispe(tile.0, tile.1);
    ipco.extend(ispe(grid.0, grid.1));
    if let Some(turns) = irot {
        ipco.extend(iso_box(b"irot", &[turns]));
    }
    let mut ipma = 3u32.to_be_bytes().to_vec();
    let mut assoc = vec![2u8];
    if irot.is_some() {
        assoc.push(3);
    }
    ipma.extend(1u16.to_be_bytes());
    ipma.push(assoc.len() as u8);
    ipma.extend(assoc);
    for tile_id in [2u16, 3] {
        ipma.extend(tile_id.to_be_bytes());
        ipma.extend([1, 1]);
    }
    let mut iprp = iso_box(b"ipco", &ipco);
    iprp.extend(full_box(b"ipma", 0, &ipma));
    let build = |exif_offset: u32, exif_len: u32| -> Vec<u8> {
        let mut meta = full_box(b"hdlr", 0, b"\0\0\0\0pict\0\0\0\0\0\0\0\0\0\0\0\0\0");
        meta.extend(full_box(b"pitm", 0, &1u16.to_be_bytes()));
        meta.extend(iinf.clone());
        let mut iloc = vec![0x44, 0x00];
        iloc.extend(1u16.to_be_bytes());
        iloc.extend(4u16.to_be_bytes());
        iloc.extend([0, 0]);
        iloc.extend(1u16.to_be_bytes());
        iloc.extend(exif_offset.to_be_bytes());
        iloc.extend(exif_len.to_be_bytes());
        meta.extend(full_box(b"iloc", 0, &iloc));
        meta.extend(iso_box(b"iprp", &iprp));
        full_box(b"meta", 0, &meta)
    };
    let mut item = 6u32.to_be_bytes().to_vec();
    item.extend(b"Exif\0\0");
    item.extend(exif.clone().unwrap_or_default());
    let meta_len = build(0, 0).len();
    let padding = iso_box(b"free", &[0; 32]);
    let exif_offset = ftyp_box.len() + meta_len + padding.len() + 8;
    let mut out = ftyp_box;
    let (offset, length) = if exif.is_some() {
        (exif_offset as u32, item.len() as u32)
    } else {
        (0, 0)
    };
    out.extend(build(offset, length));
    out.extend(padding);
    out.extend(iso_box(b"mdat", &item));
    out
}

fn mvhd_v0(created: u32, timescale: u32, duration: u32) -> Vec<u8> {
    let mut body = created.to_be_bytes().to_vec();
    body.extend(created.to_be_bytes());
    body.extend(timescale.to_be_bytes());
    body.extend(duration.to_be_bytes());
    body.extend([0; 80]);
    full_box(b"mvhd", 0, &body)
}

fn mvhd_v1(created: u64, timescale: u32, duration: u64) -> Vec<u8> {
    let mut body = created.to_be_bytes().to_vec();
    body.extend(created.to_be_bytes());
    body.extend(timescale.to_be_bytes());
    body.extend(duration.to_be_bytes());
    body.extend([0; 80]);
    full_box(b"mvhd", 1, &body)
}

const ROTATE_NONE: [i32; 4] = [0x10000, 0, 0, 0x10000];
const ROTATE_90: [i32; 4] = [0, 0x10000, -0x10000, 0];
const ROTATE_180: [i32; 4] = [-0x10000, 0, 0, -0x10000];

fn tkhd(version: u8, matrix: [i32; 4], width: u32, height: u32) -> Vec<u8> {
    let mut body = Vec::new();
    if version == 1 {
        body.extend([0; 8 + 8 + 4 + 4 + 8]);
    } else {
        body.extend([0; 4 + 4 + 4 + 4 + 4]);
    }
    body.extend([0; 8 + 2 + 2 + 2 + 2]);
    body.extend(matrix[0].to_be_bytes());
    body.extend(matrix[1].to_be_bytes());
    body.extend([0; 4]);
    body.extend(matrix[2].to_be_bytes());
    body.extend(matrix[3].to_be_bytes());
    body.extend([0; 4]);
    body.extend([0, 0, 0, 0, 0, 0, 0, 0, 0x40, 0, 0, 0]);
    body.extend((width << 16).to_be_bytes());
    body.extend((height << 16).to_be_bytes());
    full_box(b"tkhd", version, &body)
}

fn trak(handler: &[u8; 4], format: &[u8; 4], tkhd_box: Vec<u8>, table_len: usize) -> Vec<u8> {
    let mut hdlr = vec![0; 4];
    hdlr.extend(handler);
    hdlr.extend([0; 12]);
    let mut stsd = 1u32.to_be_bytes().to_vec();
    stsd.extend(iso_box(format, &[0; 8]));
    let mut stbl = full_box(b"stsd", 0, &stsd);
    stbl.extend(full_box(b"stsz", 0, &vec![0; table_len]));
    let minf = iso_box(b"stbl", &stbl);
    let mut mdia = full_box(b"hdlr", 0, &hdlr);
    mdia.extend(iso_box(b"minf", &minf));
    let mut body = tkhd_box;
    body.extend(iso_box(b"mdia", &mdia));
    iso_box(b"trak", &body)
}

fn data_box(text: &str) -> Vec<u8> {
    let mut body = 1u32.to_be_bytes().to_vec();
    body.extend([0; 4]);
    body.extend(text.as_bytes());
    iso_box(b"data", &body)
}

fn quicktime_meta(items: &[(&str, &str)]) -> Vec<u8> {
    let mut hdlr = vec![0; 4];
    hdlr.extend(b"mdta");
    hdlr.extend([0; 13]);
    let mut keys = (items.len() as u32).to_be_bytes().to_vec();
    let mut ilst = Vec::new();
    for (index, (key, value)) in items.iter().enumerate() {
        keys.extend(iso_box(b"mdta", key.as_bytes()));
        ilst.extend(iso_box(&((index + 1) as u32).to_be_bytes(), &data_box(value)));
    }
    let mut body = full_box(b"hdlr", 0, &hdlr);
    body.extend(full_box(b"keys", 0, &keys));
    body.extend(iso_box(b"ilst", &ilst));
    iso_box(b"meta", &body)
}

fn itunes_udta(items: &[(&[u8; 4], &str)]) -> Vec<u8> {
    let mut ilst = Vec::new();
    for (kind, value) in items {
        ilst.extend(iso_box(kind, &data_box(value)));
    }
    let mut hdlr = vec![0; 4];
    hdlr.extend(b"mdir");
    hdlr.extend([0; 13]);
    let mut meta = full_box(b"hdlr", 0, &hdlr);
    meta.extend(iso_box(b"ilst", &ilst));
    iso_box(b"udta", &full_box(b"meta", 0, &meta))
}

fn xyz_udta(text: &str) -> Vec<u8> {
    let mut body = (text.len() as u16).to_be_bytes().to_vec();
    body.extend([0x15, 0xc7]);
    body.extend(text.as_bytes());
    iso_box(b"udta", &iso_box(b"\xa9xyz", &body))
}

fn mp4(brand: &[u8; 4], moov_children: &[Vec<u8>], moov_last: bool) -> Vec<u8> {
    let moov = iso_box(b"moov", &moov_children.concat());
    let mdat = iso_box(b"mdat", &[0; 64]);
    let mut out = ftyp(brand, &[b"isom"]);
    if moov_last {
        out.extend(mdat);
        out.extend(moov);
    } else {
        out.extend(moov);
        out.extend(mdat);
    }
    out
}

// 2024-01-02T03:04:05Z since 1904.
const MP4_TIME: u32 = 3_787_009_445;

fn flac(rate: u32, samples: u64) -> Vec<u8> {
    let mut out = b"fLaC".to_vec();
    out.extend([0x80, 0, 0, 34]);
    out.extend([0; 10]);
    out.push((rate >> 12) as u8);
    out.push((rate >> 4) as u8);
    out.push(((rate & 0x0f) << 4) as u8 | 0x02);
    out.push(0xf0 | ((samples >> 32) & 0x0f) as u8);
    out.extend((samples as u32).to_be_bytes());
    out.extend([0; 16]);
    out
}

fn wav(byte_rate: u32, data_len: usize) -> Vec<u8> {
    let mut fmt = vec![1, 0, 2, 0];
    fmt.extend(44100u32.to_le_bytes());
    fmt.extend(byte_rate.to_le_bytes());
    fmt.extend([4, 0, 16, 0]);
    riff(
        b"WAVE",
        &[riff_chunk(b"fmt ", &fmt), riff_chunk(b"data", &vec![0; data_len])],
    )
}

fn id3_size(size: u32) -> [u8; 4] {
    [
        ((size >> 21) & 0x7f) as u8,
        ((size >> 14) & 0x7f) as u8,
        ((size >> 7) & 0x7f) as u8,
        (size & 0x7f) as u8,
    ]
}

fn id3_frame(version: u8, id: &str, body: &[u8]) -> Vec<u8> {
    let mut out = id.as_bytes().to_vec();
    match version {
        2 => out.extend(&(body.len() as u32).to_be_bytes()[1..]),
        3 => out.extend((body.len() as u32).to_be_bytes()),
        _ => out.extend(id3_size(body.len() as u32)),
    }
    if version != 2 {
        out.extend([0, 0]);
    }
    out.extend(body);
    out
}

fn id3(version: u8, flags: u8, frames: &[Vec<u8>]) -> Vec<u8> {
    let body = frames.concat();
    let mut out = b"ID3".to_vec();
    out.extend([version, 0, flags]);
    out.extend(id3_size(body.len() as u32 + 32));
    out.extend(body);
    out.extend([0; 32]);
    out.extend([0xff, 0xfb, 0x90, 0x00]);
    out
}

fn utf16_le(text: &str) -> Vec<u8> {
    let mut out = vec![1, 0xff, 0xfe];
    for unit in text.encode_utf16() {
        out.extend(unit.to_le_bytes());
    }
    out
}

fn utf16_be_no_bom(text: &str) -> Vec<u8> {
    let mut out = vec![2];
    for unit in text.encode_utf16() {
        out.extend(unit.to_be_bytes());
    }
    out
}

// ---- Vectors ----

struct Vector {
    name: String,
    input: Vec<u8>,
    file_name: Option<&'static str>,
    declared_mime: Option<&'static str>,
    expected: Value,
}

fn v(name: &str, input: Vec<u8>, expected: Value) -> Vector {
    Vector {
        name: name.to_string(),
        input,
        file_name: None,
        declared_mime: None,
        expected,
    }
}

fn named(name: &str, file_name: &'static str, declared: Option<&'static str>, input: &[u8], expected: Value) -> Vector {
    Vector {
        name: name.to_string(),
        input: input.to_vec(),
        file_name: Some(file_name),
        declared_mime: declared,
        expected,
    }
}

fn vectors() -> Vec<Vector> {
    let exif_le = tiff(true, &camera_ifd(None), &photo_exif(), &north_east_gps());
    let exif_be = tiff(false, &camera_ifd(None), &photo_exif(), &north_east_gps());
    let full = |mime: &str, width: u32, height: u32| {
        json!({
            "mime": mime,
            "width": width,
            "height": height,
            "captured_at": "2024-02-29T13:05:09.125+08:00",
            "geo": expected_geo(),
            "camera": expected_camera(),
            "exposure": expected_exposure(),
        })
    };
    let mut out = vec![
        // JPEG
        v("jpeg_exif_little_endian", jpeg(Some(exif_le.clone()), 6000, 4000), full("image/jpeg", 6000, 4000)),
        v("jpeg_exif_big_endian", jpeg(Some(exif_be.clone()), 6000, 4000), full("image/jpeg", 6000, 4000)),
        v("jpeg_no_exif", jpeg(None, 640, 480), json!({"mime": "image/jpeg", "width": 640, "height": 480})),
    ];
    for (orientation, width, height) in [(1, 6000, 4000), (3, 6000, 4000), (5, 4000, 6000), (6, 4000, 6000), (8, 4000, 6000)] {
        let exif = tiff(true, &camera_ifd(Some(orientation)), &[], &[]);
        out.push(v(
            &format!("jpeg_orientation_{}", orientation),
            jpeg(Some(exif), 6000, 4000),
            json!({"mime": "image/jpeg", "width": width, "height": height, "camera": {"make": "Sony", "model": "ILCE-7M3"}}),
        ));
    }
    out.extend([
        // Other EXIF carriers
        v("png_exif", png(Some(exif_be.clone()), 800, 600), full("image/png", 800, 600)),
        v("png_plain", png(None, 1, 1), json!({"mime": "image/png", "width": 1, "height": 1})),
        v("gif", b"GIF89a\x40\x01\xf0\x00\0\0\0".to_vec(), json!({"mime": "image/gif", "width": 320, "height": 240})),
        v("webp_vp8x_exif", webp_vp8x(1920, 1080, Some(exif_le.clone())), full("image/webp", 1920, 1080)),
        v("webp_vp8x_exif_prefixed", webp_vp8x(1920, 1080, Some([b"Exif\0\0".to_vec(), exif_le.clone()].concat())), full("image/webp", 1920, 1080)),
        v("webp_vp8x_plain", webp_vp8x(16, 9, None), json!({"mime": "image/webp", "width": 16, "height": 9})),
        v(
            "webp_vp8",
            riff(b"WEBP", &[riff_chunk(b"VP8 ", &[0, 0, 0, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0xe0, 0x01])]),
            json!({"mime": "image/webp", "width": 640, "height": 480}),
        ),
        v(
            "webp_vp8l",
            riff(b"WEBP", &[riff_chunk(b"VP8L", &[0x2f, 0x7f, 0xc2, 0x77, 0x00])]),
            json!({"mime": "image/webp", "width": 640, "height": 480}),
        ),
        v(
            "tiff_raw_like",
            tiff(true, &camera_ifd(Some(6)), &photo_exif(), &north_east_gps()),
            json!({
                "mime": "image/tiff",
                "captured_at": "2024-02-29T13:05:09.125+08:00",
                "geo": expected_geo(),
                "camera": expected_camera(),
                "exposure": expected_exposure(),
            }),
        ),
        // HEIF: EXIF orientation must not stack on `irot`.
        v(
            "heic_grid_irot_portrait_exif_orientation_ignored",
            heic((5712, 4284), (512, 512), Some(3), Some(tiff(false, &camera_ifd(Some(6)), &photo_exif(), &north_east_gps())), b"heic"),
            full("image/heic", 4284, 5712),
        ),
        v(
            "heic_landscape_without_irot",
            heic((1024, 768), (512, 512), None, None, b"heic"),
            json!({"mime": "image/heic", "width": 1024, "height": 768}),
        ),
        v(
            "heif_generic_brand",
            {
                let mut bytes = heic((100, 50), (50, 50), Some(2), None, b"mif1");
                bytes[20..24].copy_from_slice(b"mif1");
                bytes[24..28].copy_from_slice(b"msf1");
                bytes
            },
            json!({"mime": "image/heif", "width": 100, "height": 50}),
        ),
        v(
            "avif",
            heic((200, 100), (100, 100), Some(1), None, b"avif"),
            json!({"mime": "image/avif", "width": 100, "height": 200}),
        ),
        // Geo edge cases
        v(
            "geo_south_west_below_sea",
            jpeg(Some(tiff(true, &[], &[E::Ascii(0x9003, "2024:02:29 13:05:09")], &south_west_gps())), 10, 10),
            json!({
                "mime": "image/jpeg", "width": 10, "height": 10,
                "captured_at": "2024-02-29T13:05:09",
                "geo": {"lat": -33.865, "lon": -70.666667, "alt": -7.5},
            }),
        ),
        v(
            "geo_zero_zero_dropped",
            jpeg(Some(tiff(true, &[], &[], &[
                E::Ascii(0x0001, "N"), E::Rat(0x0002, vec![(0, 1), (0, 1), (0, 1)]),
                E::Ascii(0x0003, "E"), E::Rat(0x0004, vec![(0, 1), (0, 1), (0, 1)]),
            ])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "geo_out_of_range_dropped",
            jpeg(Some(tiff(true, &[], &[], &[
                E::Ascii(0x0001, "N"), E::Rat(0x0002, vec![(91, 1), (0, 1), (0, 1)]),
                E::Ascii(0x0003, "E"), E::Rat(0x0004, vec![(10, 1), (0, 1), (0, 1)]),
            ])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "geo_zero_denominator_dropped",
            jpeg(Some(tiff(true, &[], &[], &[
                E::Ascii(0x0001, "N"), E::Rat(0x0002, vec![(34, 0), (0, 1), (0, 1)]),
                E::Ascii(0x0003, "E"), E::Rat(0x0004, vec![(10, 1), (0, 1), (0, 1)]),
            ])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "geo_alt_rounds_to_zero_not_negative_zero",
            jpeg(Some(tiff(true, &[], &[], &[
                E::Ascii(0x0001, "N"), E::Rat(0x0002, vec![(1, 1), (0, 1), (0, 1)]),
                E::Ascii(0x0003, "E"), E::Rat(0x0004, vec![(1, 1), (0, 1), (0, 1)]),
                E::Byte(0x0005, 1), E::Rat(0x0006, vec![(4, 100)]),
            ])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "geo": {"lat": 1.0, "lon": 1.0, "alt": 0.0}}),
        ),
        v(
            "round_half_away_from_zero_on_negative",
            jpeg(Some(tiff(true, &[], &[], &[
                E::Ascii(0x0001, "S"), E::Rat(0x0002, vec![(1, 1), (0, 1), (0, 1)]),
                E::Ascii(0x0003, "W"), E::Rat(0x0004, vec![(1, 1), (0, 1), (0, 1)]),
                E::Byte(0x0005, 1), E::Rat(0x0006, vec![(25, 100)]),
            ])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "geo": {"lat": -1.0, "lon": -1.0, "alt": -0.3}}),
        ),
        // Dates
        v(
            "date_digitized_fallback_never_datetime",
            jpeg(Some(tiff(true, &[E::Ascii(0x0132, "2030:01:01 00:00:00")], &[
                E::Ascii(0x9003, "0000:00:00 00:00:00"),
                E::Ascii(0x9004, "2021:03:04 05:06:07"),
                E::Ascii(0x9012, "-05:30"),
                E::Ascii(0x9292, "5"),
            ], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "captured_at": "2021-03-04T05:06:07.5-05:30"}),
        ),
        v(
            "date_only_datetime_is_dropped",
            jpeg(Some(tiff(true, &[E::Ascii(0x0132, "2030:01:01 00:00:00"), E::Ascii(0x010f, "X")], &[], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "X"}}),
        ),
    ]);
    for (label, date) in [
        ("zero", "0000:00:00 00:00:00"),
        ("blank", "    :  :     :  :  "),
        ("feb30", "2023:02:30 00:00:00"),
        ("feb29_not_leap", "2023:02:29 00:00:00"),
        ("month13", "2023:13:01 00:00:00"),
        ("hour24", "2023:01:01 24:00:00"),
        ("dashes", "2023-01-01 00:00:00"),
    ] {
        out.push(v(
            &format!("date_invalid_{}", label),
            jpeg(Some(tiff(true, &[], &[E::Ascii(0x9003, date)], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ));
    }
    out.push(v(
        "date_bad_offset_and_fraction_dropped",
        jpeg(Some(tiff(true, &[], &[
            E::Ascii(0x9003, "2024:02:29 13:05:09"),
            E::Ascii(0x9011, "   :  "),
            E::Ascii(0x9291, "1a"),
        ], &[])), 10, 10),
        json!({"mime": "image/jpeg", "width": 10, "height": 10, "captured_at": "2024-02-29T13:05:09"}),
    ));
    out.extend([
        // Strings
        v(
            "text_nul_control_trim_and_length",
            jpeg(Some(tiff(true, &[
                E::Raw(0x010f, 2, 16, b"\0\0  A\x01C\x7fME \0junk".to_vec()),
                E::Raw(0x0110, 2, 150, vec![b'x'; 150]),
                E::Raw(0x0131, 2, 4, b"abc\0".to_vec()),
            ], &[], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "ACME", "model": "x".repeat(128)}}),
        ),
        v(
            "text_invalid_utf8_is_replaced",
            jpeg(Some(tiff(true, &[E::Raw(0x010f, 2, 8, vec![0xe2, 0x82, 0x41, 0xff, 0xf0, 0x9f, 0x98, 0x42])], &[], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "\u{fffd}A\u{fffd}\u{fffd}B"}}),
        ),
        v(
            "text_unicode_whitespace_and_bom_rule",
            jpeg(Some(tiff(true, &[
                E::Raw(0x010f, 2, 8, "\u{3000}a\u{a0}".as_bytes().iter().copied().chain([0, 0]).collect()),
                E::Raw(0x0110, 2, 5, "\u{feff}b\0".as_bytes().to_vec()),
            ], &[], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "a", "model": "\u{feff}b"}}),
        ),
        v(
            "exposure_whole_seconds_and_wrong_types",
            jpeg(Some(tiff(true, &[], &[
                E::Rat(0x829a, vec![(4, 2)]),
                E::Ascii(0x829d, "2.8"),
                E::Long(0x8827, 3200),
                E::Rat(0x920a, vec![(0, 1)]),
            ], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "exposure": {"exposure_time": "2", "iso": 3200}}),
        ),
        v(
            "exposure_zero_denominator_dropped",
            jpeg(Some(tiff(true, &[E::Ascii(0x010f, "X")], &[E::Rat(0x829a, vec![(1, 0)]), E::Rat(0x829d, vec![(28, 0)])], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "X"}}),
        ),
        // Malformed EXIF keeps the rest
        v(
            "exif_not_tiff",
            jpeg(Some(b"XXnot tiff".to_vec()), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "exif_first_ifd_out_of_range",
            jpeg(Some(b"II*\0\xff\xff\0\0".to_vec()), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "exif_ifd_points_back_to_itself",
            {
                let mut bytes = b"II*\0\x08\0\0\0".to_vec();
                bytes.extend([2, 0]);
                bytes.extend(0x8769u16.to_le_bytes());
                bytes.extend(4u16.to_le_bytes());
                bytes.extend(1u32.to_le_bytes());
                bytes.extend(8u32.to_le_bytes());
                bytes.extend(0x8825u16.to_le_bytes());
                bytes.extend(4u16.to_le_bytes());
                bytes.extend(1u32.to_le_bytes());
                bytes.extend(8u32.to_le_bytes());
                bytes.extend([0; 4]);
                jpeg(Some(bytes), 10, 10)
            },
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "exif_value_offset_past_segment",
            jpeg(Some({
                let mut bytes = b"II*\0\x08\0\0\0\x01\0".to_vec();
                bytes.extend(0x010fu16.to_le_bytes());
                bytes.extend(2u16.to_le_bytes());
                bytes.extend(100u32.to_le_bytes());
                bytes.extend(0xffff_fff0u32.to_le_bytes());
                bytes.extend([0; 4]);
                bytes
            }), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10}),
        ),
        v(
            "exif_sub_ifd_out_of_range_keeps_camera",
            jpeg(Some(tiff(true, &[E::Ascii(0x010f, "X"), E::Long(0x8769, 0x00ff_ffff)], &[], &[])), 10, 10),
            json!({"mime": "image/jpeg", "width": 10, "height": 10, "camera": {"make": "X"}}),
        ),
        v("jpeg_truncated_in_segment", vec![0xff, 0xd8, 0xff, 0xe1, 0xff], json!({"mime": "image/jpeg"})),
        v("jpeg_zero_height", jpeg(None, 100, 0), json!({"mime": "image/jpeg"})),
        v("png_truncated", vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0], json!({"mime": "image/png"})),
        v("webp_truncated", b"RIFF\xff\xff\xff\xffWEBPVP8X".to_vec(), json!({"mime": "image/webp"})),
        v("heic_truncated_meta", [ftyp(b"heic", &[]), vec![0, 0, 0xff, 0xff], b"meta".to_vec(), vec![0; 8]].concat(), json!({"mime": "image/heic"})),
        v("mp4_box_size_too_small", [ftyp(b"isom", &[]), vec![0, 0, 0, 4], b"moov".to_vec()].concat(), json!({"mime": "video/mp4"})),
        v("mp4_moov_overruns_file", [ftyp(b"isom", &[]), vec![0xff, 0xff, 0xff, 0xff], b"moov".to_vec(), vec![0; 4]].concat(), json!({"mime": "video/mp4"})),
        // Video
        v(
            "mp4_h264_aac_moov_first",
            mp4(b"isom", &[
                mvhd_v0(MP4_TIME, 1000, 12345),
                trak(b"vide", b"avc1", tkhd(0, ROTATE_NONE, 1920, 1080), 0),
                trak(b"soun", b"mp4a", tkhd(0, ROTATE_NONE, 0, 0), 0),
            ], false),
            json!({
                "mime": "video/mp4", "width": 1920, "height": 1080, "duration_ms": 12345,
                "video_codec": "h264", "audio_codec": "aac", "captured_at": "2024-01-02T03:04:05Z",
            }),
        ),
        v(
            "mp4_moov_last_rotated_hevc_v1",
            mp4(b"mp42", &[
                mvhd_v1(MP4_TIME as u64, 600, 900),
                trak(b"vide", b"hvc1", tkhd(1, ROTATE_90, 1920, 1080), 64),
            ], true),
            json!({
                "mime": "video/mp4", "width": 1080, "height": 1920, "duration_ms": 1500,
                "video_codec": "hevc", "captured_at": "2024-01-02T03:04:05Z",
            }),
        ),
        v(
            "mp4_rotate_180_keeps_size_av1",
            mp4(b"isom", &[trak(b"vide", b"av01", tkhd(0, ROTATE_180, 640, 360), 0)], false),
            json!({"mime": "video/mp4", "width": 640, "height": 360, "video_codec": "av1"}),
        ),
        v(
            "mp4_vp9_opus_unknown_codec_omitted",
            mp4(b"isom", &[
                trak(b"vide", b"vp09", tkhd(0, ROTATE_NONE, 64, 64), 0),
                trak(b"soun", b"Opus", tkhd(0, ROTATE_NONE, 0, 0), 0),
            ], false),
            json!({"mime": "video/mp4", "width": 64, "height": 64, "video_codec": "vp9"}),
        ),
        v(
            "mp4_location_xyz",
            mp4(b"isom", &[
                mvhd_v0(0, 1000, 0),
                trak(b"vide", b"hev1", tkhd(0, ROTATE_NONE, 64, 64), 0),
                xyz_udta("+34.9949-135.7567+012.340/"),
            ], false),
            json!({"mime": "video/mp4", "width": 64, "height": 64, "video_codec": "hevc", "geo": {"lat": 34.9949, "lon": -135.7567, "alt": 12.3}}),
        ),
        v(
            "mov_apple_metadata_wins_over_mvhd",
            mp4(b"qt  ", &[
                mvhd_v0(MP4_TIME, 1000, 2000),
                trak(b"vide", b"avc3", tkhd(0, ROTATE_NONE, 64, 64), 0),
                quicktime_meta(&[
                    ("com.apple.quicktime.creationdate", "2023-05-06T07:08:09+0900"),
                    ("com.apple.quicktime.location.ISO6709", "-33.8650+151.2094-005.000/"),
                    ("com.apple.quicktime.location.accuracy.horizontal", "65.04"),
                    ("com.apple.quicktime.make", "Apple"),
                    ("com.apple.quicktime.model", "iPhone 15"),
                    ("com.apple.quicktime.software", "17.0"),
                ]),
            ], false),
            json!({
                "mime": "video/quicktime", "width": 64, "height": 64, "duration_ms": 2000, "video_codec": "h264",
                "captured_at": "2023-05-06T07:08:09+09:00",
                "geo": {"lat": -33.865, "lon": 151.2094, "alt": -5.0, "accuracy_m": 65.0},
                "camera": {"make": "Apple", "model": "iPhone 15"},
            }),
        ),
        v(
            "mov_bad_creationdate_falls_back_to_mvhd",
            mp4(b"qt  ", &[
                mvhd_v0(MP4_TIME, 1000, 0),
                quicktime_meta(&[("com.apple.quicktime.creationdate", "yesterday"), ("com.apple.quicktime.location.ISO6709", "+00.0000+000.0000/")]),
            ], false),
            json!({"mime": "video/quicktime", "captured_at": "2024-01-02T03:04:05Z"}),
        ),
        v(
            "mp4_mvhd_before_1970_is_dropped",
            mp4(b"isom", &[mvhd_v0(1000, 1000, 0xffff_ffff)], false),
            json!({"mime": "video/mp4"}),
        ),
        // Audio
        v(
            "m4a_itunes_tags",
            mp4(b"M4A ", &[
                mvhd_v0(0, 44100, 441000),
                trak(b"soun", b"mp4a", tkhd(0, ROTATE_NONE, 0, 0), 0),
                itunes_udta(&[(b"\xa9nam", "歌名"), (b"\xa9ART", "Artist"), (b"\xa9alb", " Album "), (b"cpil", "x")]),
            ], false),
            json!({"mime": "audio/mp4", "duration_ms": 10000, "audio_codec": "aac", "title": "歌名", "artist": "Artist", "album": "Album"}),
        ),
        v(
            "mp4_audio_only_is_audio",
            mp4(b"isom", &[
                mvhd_v0(0, 1000, 500),
                trak(b"soun", b"mp4a", tkhd(0, ROTATE_NONE, 0, 0), 0),
                itunes_udta(&[(b"\xa9nam", "T")]),
            ], false),
            json!({"mime": "audio/mp4", "duration_ms": 500, "audio_codec": "aac", "title": "T"}),
        ),
        v(
            "mp4_video_ignores_audio_tags",
            mp4(b"isom", &[
                trak(b"vide", b"avc1", tkhd(0, ROTATE_NONE, 64, 64), 0),
                itunes_udta(&[(b"\xa9nam", "T")]),
            ], false),
            json!({"mime": "video/mp4", "width": 64, "height": 64, "video_codec": "h264"}),
        ),
        v("flac", flac(44100, 88200), json!({"mime": "audio/flac", "duration_ms": 2000})),
        v("flac_36bit_samples", flac(48000, 0xf_0000_0000), json!({"mime": "audio/flac", "duration_ms": 1_342_177_280u64})),
        v("flac_zero_rate", flac(0, 88200), json!({"mime": "audio/flac"})),
        v("wav", wav(176400, 88200), json!({"mime": "audio/wav", "duration_ms": 500})),
        v("wav_data_size_lies", {
            let mut bytes = wav(1000, 100);
            let at = bytes.len() - 104;
            bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            bytes
        }, json!({"mime": "audio/wav", "duration_ms": 100})),
        v(
            "mp3_id3v23_utf16",
            id3(3, 0, &[
                id3_frame(3, "TIT2", &utf16_le("标题😀")),
                id3_frame(3, "TPE1", &utf16_be_no_bom("Artist")),
                id3_frame(3, "APIC", &[0; 200]),
                id3_frame(3, "TALB", b"\0Album\0Second"),
            ]),
            json!({"mime": "audio/mpeg", "title": "标题😀", "artist": "Artist", "album": "Album"}),
        ),
        v(
            "mp3_id3v24_utf8_syncsafe",
            id3(4, 0, &[id3_frame(4, "TIT2", &[b"\x03T\xc3\xa9st".to_vec()].concat()), id3_frame(4, "TPE1", b"\x00Caf\xe9")]),
            json!({"mime": "audio/mpeg", "title": "Tést", "artist": "Café"}),
        ),
        v(
            "mp3_id3v22",
            id3(2, 0, &[id3_frame(2, "TT2", b"\0Old"), id3_frame(2, "TP1", b"\0Band"), id3_frame(2, "TAL", b"\0LP")]),
            json!({"mime": "audio/mpeg", "title": "Old", "artist": "Band", "album": "LP"}),
        ),
        v(
            "mp3_id3_unsynchronised_tag_dropped",
            id3(3, 0x80, &[id3_frame(3, "TIT2", b"\0T")]),
            json!({"mime": "audio/mpeg"}),
        ),
        v(
            "mp3_id3v24_compressed_frame_skipped",
            id3(4, 0, &[{
                let mut frame = id3_frame(4, "TIT2", b"\0T");
                frame[9] = 0x08;
                frame
            }, id3_frame(4, "TPE1", b"\0A")]),
            json!({"mime": "audio/mpeg", "artist": "A"}),
        ),
        v(
            "mp3_id3_lone_surrogate_replaced",
            id3(3, 0, &[id3_frame(3, "TIT2", &[1, 0xff, 0xfe, 0x3d, 0xd8, b'x', 0])]),
            json!({"mime": "audio/mpeg", "title": "\u{fffd}x"}),
        ),
        v("mp3_bare_frame", vec![0xff, 0xfb, 0x90, 0x00, 0, 0], json!({"mime": "audio/mpeg"})),
        v(
            "m4a_has_no_capture_fields",
            mp4(b"M4A ", &[
                mvhd_v0(MP4_TIME, 1000, 500),
                trak(b"vide", b"avc1", tkhd(0, ROTATE_NONE, 64, 64), 0),
                xyz_udta("+34.9949-135.7567/"),
            ], false),
            json!({"mime": "audio/mp4", "duration_ms": 500}),
        ),
        // Written by ffmpeg, not by these builders.
        v(
            "ffmpeg_mov_apple_keys",
            include_bytes!("../testdata/media_meta/apple_keys.mov").to_vec(),
            json!({
                "mime": "video/quicktime", "width": 32, "height": 24, "duration_ms": 200,
                "video_codec": "h264", "audio_codec": "aac",
                "captured_at": "2023-05-06T07:08:09+09:00",
                "geo": {"lat": 34.9949, "lon": -135.7567, "alt": 12.3},
                "camera": {"make": "Apple", "model": "iPhone 15"},
            }),
        ),
        v(
            "ffmpeg_mov_location_xyz",
            include_bytes!("../testdata/media_meta/location_xyz.mov").to_vec(),
            json!({
                "mime": "video/quicktime", "width": 32, "height": 24, "duration_ms": 200, "video_codec": "h264",
                "captured_at": "2024-01-02T03:04:05Z", "geo": {"lat": -33.865, "lon": 151.2094},
            }),
        ),
        v(
            "ffmpeg_mov_rotated_90",
            include_bytes!("../testdata/media_meta/rotated_90.mov").to_vec(),
            json!({
                "mime": "video/quicktime", "width": 24, "height": 32, "duration_ms": 200, "video_codec": "h264",
                "geo": {"lat": -33.865, "lon": 151.2094},
            }),
        ),
        v(
            "ffmpeg_m4a_tagged",
            include_bytes!("../testdata/media_meta/tagged.m4a").to_vec(),
            json!({"mime": "audio/mp4", "duration_ms": 200, "audio_codec": "aac", "title": "TheTitle", "artist": "TheArtist", "album": "TheAlbum"}),
        ),
        v(
            "ffmpeg_mp3_id3v23",
            include_bytes!("../testdata/media_meta/id3v23.mp3").to_vec(),
            json!({"mime": "audio/mpeg", "title": "标题", "artist": "TheArtist", "album": "TheAlbum"}),
        ),
        // mime without media parsing
        v("webm_magic", {
            let mut bytes = vec![0x1a, 0x45, 0xdf, 0xa3];
            bytes.extend(b"\x01\0\0\0\0\0\0\x1f\x42\x86\x81\x01webm");
            bytes
        }, json!({"mime": "video/webm"})),
        v("ogg_magic", b"OggS\0\x02\0\0\0\0\0\0".to_vec(), json!({"mime": "audio/ogg"})),
        v("bmp_magic", b"BM\0\0\0\0".to_vec(), json!({"mime": "image/bmp"})),
        v("svg_magic", b"\xef\xbb\xbf  <?xml version=\"1.0\"?><SVG>".to_vec(), json!({"mime": "image/svg+xml"})),
        v("empty_input", Vec::new(), json!({})),
        v("unknown_bytes", vec![1, 2, 3, 4, 5, 6, 7, 8], json!({})),
        named("mime_media_magic_beats_name_and_declared", "a.txt", Some("text/plain"), b"GIF89a\x01\0\x01\0", json!({"mime": "image/gif", "width": 1, "height": 1})),
        named("mime_declared_beats_extension", "a.txt", Some("application/json; charset=utf-8"), b"{}", json!({"mime": "application/json"})),
        named("mime_declared_is_lowercased", "a", Some(" Text/Plain "), b"x", json!({"mime": "text/plain"})),
        named("mime_generic_declared_is_ignored", "a.csv", Some("application/octet-stream"), b"x", json!({"mime": "text/csv"})),
        named("mime_malformed_declared_is_ignored", "a.csv", Some("not a mime"), b"x", json!({"mime": "text/csv"})),
        named("mime_generic_never_written", "a.bin", Some("binary/octet-stream"), b"x", json!({})),
        named("mime_non_media_magic_after_extension", "a.bin", None, b"%PDF-1.7", json!({"mime": "application/pdf"})),
        named("mime_extension_beats_non_media_magic", "a.docx", None, b"PK\x03\x04", json!({"mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"})),
        named("mime_path_and_case", "dir.x/A.JPEG", None, b"x", json!({"mime": "image/jpeg"})),
        named("mime_leading_dot_is_no_extension", ".png", None, b"x", json!({})),
        named("mime_declared_jpeg_with_garbage", "a", Some("image/jpeg"), b"garbage", json!({"mime": "image/jpeg"})),
    ]);
    for (ext, mime) in extension_table() {
        let file_name: &'static str = Box::leak(format!("f.{}", ext).into_boxed_str());
        out.push(named(&format!("ext_{}", ext), file_name, None, b"x", json!({"mime": mime})));
    }
    out
}

fn extension_table() -> Vec<(&'static str, String)> {
    let plain = [
        ("png", "image/png"), ("apng", "image/apng"), ("jpg", "image/jpeg"), ("jpeg", "image/jpeg"),
        ("jfif", "image/jpeg"), ("gif", "image/gif"), ("webp", "image/webp"), ("bmp", "image/bmp"),
        ("ico", "image/x-icon"), ("avif", "image/avif"), ("heic", "image/heic"), ("heif", "image/heif"),
        ("tif", "image/tiff"), ("tiff", "image/tiff"), ("psd", "image/vnd.adobe.photoshop"),
        ("svg", "image/svg+xml"), ("mp4", "video/mp4"), ("m4v", "video/mp4"), ("webm", "video/webm"),
        ("mov", "video/quicktime"), ("mkv", "video/x-matroska"), ("avi", "video/x-msvideo"),
        ("ogv", "video/ogg"), ("mp3", "audio/mpeg"), ("wav", "audio/wav"), ("flac", "audio/flac"),
        ("ogg", "audio/ogg"), ("oga", "audio/ogg"), ("opus", "audio/ogg"), ("m4a", "audio/mp4"),
        ("aac", "audio/aac"), ("pdf", "application/pdf"), ("html", "text/html"), ("htm", "text/html"),
        ("xhtml", "application/xhtml+xml"),
        ("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        ("doc", "application/msword"), ("xls", "application/vnd.ms-excel"),
        ("ppt", "application/vnd.ms-powerpoint"), ("odt", "application/vnd.oasis.opendocument.text"),
        ("zip", "application/zip"), ("tar", "application/x-tar"), ("gz", "application/gzip"),
        ("7z", "application/x-7z-compressed"), ("rar", "application/vnd.rar"),
        ("epub", "application/epub+zip"), ("md", "text/markdown"), ("markdown", "text/markdown"),
        ("json", "application/json"), ("xml", "application/xml"), ("csv", "text/csv"),
        ("yaml", "application/yaml"), ("yml", "application/yaml"), ("toml", "application/toml"),
        ("js", "text/javascript"), ("mjs", "text/javascript"), ("cjs", "text/javascript"), ("css", "text/css"),
    ];
    let text = [
        "txt", "text", "log", "ini", "conf", "cfg", "env", "tsv", "rst", "adoc", "jsonc", "json5",
        "ts", "tsx", "jsx", "rs", "py", "go", "c", "h", "cc", "cpp", "hpp", "java", "kt", "swift",
        "rb", "php", "sql", "sh", "bash", "zsh", "ps1", "bat", "scss", "less", "vue", "svelte",
        "lua", "dart", "r", "scala", "ex", "exs", "dockerfile", "makefile", "gitignore",
        "editorconfig", "lock",
    ];
    plain
        .iter()
        .map(|(ext, mime)| (*ext, mime.to_string()))
        .chain(text.iter().map(|ext| (*ext, format!("text/x-{}", ext))))
        .collect()
}

#[tokio::test]
async fn test_media_meta_vectors() {
    let mut printed = Vec::new();
    let mut failures = Vec::new();
    for vector in vectors() {
        let source = MemoryByteSource::new(&vector.input);
        let meta = probe_media_meta(&source, vector.file_name.unwrap_or(""), vector.declared_mime).await;
        let actual = serde_json::to_value(&meta).unwrap();
        if actual != vector.expected {
            failures.push(format!("{}:\n  expected {}\n  actual   {}", vector.name, vector.expected, actual));
        }
        let mut entry = json!({
            "name": vector.name,
            "input": base64::engine::general_purpose::STANDARD.encode(&vector.input),
            "expected": actual,
        });
        if let Some(file_name) = vector.file_name {
            entry["file_name"] = json!(file_name);
        }
        if let Some(declared) = vector.declared_mime {
            entry["declared_mime"] = json!(declared);
        }
        printed.push(entry);
    }
    println!("{}", serde_json::to_string_pretty(&printed).unwrap());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

struct CountingSource {
    bytes: Vec<(u64, Vec<u8>)>,
    size: u64,
    read: AtomicU64,
}

#[async_trait]
impl MediaByteSource for CountingSource {
    fn size(&self) -> u64 {
        self.size
    }

    async fn read_at(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.read.fetch_add(length as u64, Ordering::Relaxed);
        let mut out = vec![0u8; length];
        for (start, data) in &self.bytes {
            for (i, byte) in data.iter().enumerate() {
                let at = start + i as u64;
                if at >= offset && at < offset + length as u64 {
                    out[(at - offset) as usize] = *byte;
                }
            }
        }
        Ok(out)
    }
}

// A 40 GiB MP4: the `moov` sits at the end and its `stsz` claims 2 GiB.
#[tokio::test]
async fn test_large_mp4_is_read_sparsely() {
    let mut head_boxes = ftyp(b"isom", &[]);
    let mdat_at = head_boxes.len() as u64;
    head_boxes.extend(1u32.to_be_bytes());
    head_boxes.extend(b"mdat");
    head_boxes.extend(40_000_000_000u64.to_be_bytes());
    let moov_at = mdat_at + 40_000_000_000;

    let hdlr = {
        let mut body = vec![0; 4];
        body.extend(b"vide");
        body.extend([0; 12]);
        full_box(b"hdlr", 0, &body)
    };
    let stsd = {
        let mut body = 1u32.to_be_bytes().to_vec();
        body.extend(iso_box(b"avc1", &[0; 8]));
        full_box(b"stsd", 0, &body)
    };
    let table_size: u64 = 2_000_000_000;
    let mut stbl_body = stsd.clone();
    stbl_body.extend(((table_size + 8) as u32).to_be_bytes());
    stbl_body.extend(b"stsz");
    let stbl_len = 8 + stbl_body.len() as u64 + table_size;
    let minf_len = 8 + stbl_len;
    let mdia_len = 8 + hdlr.len() as u64 + minf_len;
    let tkhd_box = tkhd(0, ROTATE_NONE, 3840, 2160);
    let trak_len = 8 + tkhd_box.len() as u64 + mdia_len;
    let mvhd = mvhd_v0(MP4_TIME, 1000, 60_000);
    let moov_len = 8 + mvhd.len() as u64 + trak_len;

    let mut moov = (moov_len as u32).to_be_bytes().to_vec();
    moov.extend(b"moov");
    moov.extend(&mvhd);
    moov.extend((trak_len as u32).to_be_bytes());
    moov.extend(b"trak");
    moov.extend(&tkhd_box);
    moov.extend((mdia_len as u32).to_be_bytes());
    moov.extend(b"mdia");
    moov.extend(&hdlr);
    moov.extend((minf_len as u32).to_be_bytes());
    moov.extend(b"minf");
    moov.extend((stbl_len as u32).to_be_bytes());
    moov.extend(b"stbl");
    moov.extend(&stbl_body);

    let source = CountingSource {
        bytes: vec![(0, head_boxes), (moov_at, moov)],
        size: moov_at + moov_len,
        read: AtomicU64::new(0),
    };
    let meta = probe_media_meta(&source, "big.mp4", None).await;
    assert_eq!(meta.width, Some(3840));
    assert_eq!(meta.height, Some(2160));
    assert_eq!(meta.duration_ms, Some(60_000));
    assert_eq!(meta.video_codec.as_deref(), Some("h264"));
    assert!(source.read.load(Ordering::Relaxed) <= 4 * READ_WINDOW);
}

// A `moov` larger than the limit is dropped without reading past it.
#[tokio::test]
async fn test_read_limit_drops_the_group() {
    let mut head = ftyp(b"heic", &[]);
    head.extend(100_000_000u32.to_be_bytes());
    head.extend(b"meta");
    let source = CountingSource {
        bytes: vec![(0, head)],
        size: 200_000_000,
        read: AtomicU64::new(0),
    };
    let meta = probe_media_meta(&source, "a.heic", None).await;
    assert_eq!(meta.mime.as_deref(), Some("image/heic"));
    assert_eq!(meta.width, None);
    assert!(source.read.load(Ordering::Relaxed) <= MEDIA_META_READ_LIMIT);
}

struct FailingSource;

#[async_trait]
impl MediaByteSource for FailingSource {
    fn size(&self) -> u64 {
        1000
    }

    async fn read_at(&self, _offset: u64, _length: usize) -> io::Result<Vec<u8>> {
        Err(io::ErrorKind::Other.into())
    }
}

#[tokio::test]
async fn test_io_error_is_not_reported() {
    let meta = probe_media_meta(&FailingSource, "a.jpg", None).await;
    assert_eq!(meta.mime.as_deref(), Some("image/jpeg"));
    assert_eq!(meta.width, None);
}

#[tokio::test]
async fn test_local_file_source_matches_memory() {
    let bytes = jpeg(Some(tiff(true, &camera_ifd(None), &photo_exif(), &north_east_gps())), 6000, 4000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.jpg");
    tokio::fs::write(&path, &bytes).await.unwrap();
    let source = LocalFileByteSource::open(&path).await.unwrap();
    let meta = probe_media_meta(&source, "a.jpg", None).await;
    assert_eq!(meta, probe_media_meta(&MemoryByteSource::new(&bytes), "a.jpg", None).await);
    assert_eq!(meta.width, Some(6000));
}

#[test]
fn test_apply_media_meta_writes_flat_keys_and_is_stable() {
    let mut file = FileObject::new("a.jpg".to_string(), 3, "mix256:00".to_string());
    file.apply_media_meta(&MediaMeta {
        mime: Some("image/jpeg".to_string()),
        width: Some(10),
        geo: Some(MediaGeo {
            lat: 34.9949,
            lon: -135.7567,
            alt: Some(-7.5),
            ..MediaGeo::default()
        }),
        exposure: Some(MediaExposure {
            f_number: Some(2.8),
            exposure_time: Some("1/120".to_string()),
            ..MediaExposure::default()
        }),
        ..MediaMeta::default()
    });
    let value = serde_json::to_value(&file).unwrap();
    assert_eq!(value["mime"], json!("image/jpeg"));
    assert_eq!(value["width"], json!(10));
    assert_eq!(value["geo"]["lat"], json!(34.9949));
    assert!(value.get("height").is_none());
    let decoded: FileObject = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, file);
}

// The same object as `FileObject with nested meta` in the SDK's
// tests/file_meta.test.ts: the two ObjIds must stay equal.
#[test]
fn test_nested_meta_obj_id_matches_sdk() {
    let body = r#"{
        "name": "IMG.heic", "size": 3358143, "content": "mix256:80c00940db74383f24e9a59c3eaf03f301a24e8c21252055cc118a662405fe3bf175d5",
        "create_time": 0, "last_update_time": 0,
        "mime": "image/heic", "width": 4284, "height": 5712, "captured_at": "2024-02-29T13:05:09.125+08:00",
        "geo": {"lat": 34.9949, "lon": -135.7567, "alt": 12.3, "heading": 90.5},
        "camera": {"make": "Apple", "model": "iPhone 15", "lens": "main"},
        "exposure": {"iso": 400, "f_number": 1.78, "exposure_time": "1/120", "focal_length_mm": 6.86, "focal_length_35mm": 26}
    }"#;
    let file: FileObject = serde_json::from_str(body).unwrap();
    let (obj_id, _) = crate::NamedObject::gen_obj_id(&file);
    assert_eq!(obj_id.to_string(), NESTED_META_OBJ_ID);
}

const NESTED_META_OBJ_ID: &str = "cyfile:5d0620d5989224c8cfb66f8622bfb9bbbc3003b670b0fbee4d5f1789613d1e8a";

fn samples_dir() -> Option<std::path::PathBuf> {
    let dir = std::env::var("BUCKYOS_MEDIA_SAMPLES")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../media_samples")
        });
    dir.is_dir().then_some(dir)
}

// Real photos hold real places and times, so neither they nor their expected
// values are committed. Set BUCKYOS_MEDIA_META_DUMP to a private path to write
// the results for the SDK's test to compare with.
#[tokio::test]
async fn test_real_samples() {
    let Some(dir) = samples_dir() else {
        return;
    };
    let mut dump = serde_json::Map::new();
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    for name in names {
        let source = LocalFileByteSource::open(dir.join(&name)).await.unwrap();
        let meta = probe_media_meta(&source, &name, None).await;
        assert!(meta.mime.as_deref().is_some_and(|m| m.starts_with("image/")), "{}", name);
        assert!(meta.width.is_some() && meta.height.is_some(), "{}", name);
        assert!(meta.captured_at.is_some(), "{}", name);
        assert!(meta.camera.is_some(), "{}", name);
        assert!(meta.exposure.is_some(), "{}", name);
        if name == "IMG_1516.heic" {
            assert_eq!((meta.width, meta.height), (Some(4284), Some(5712)));
        }
        dump.insert(name, serde_json::to_value(&meta).unwrap());
    }
    if let Ok(path) = std::env::var("BUCKYOS_MEDIA_META_DUMP") {
        std::fs::write(path, serde_json::to_string_pretty(&dump).unwrap()).unwrap();
    }
}
