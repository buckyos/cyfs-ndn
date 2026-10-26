//! `Content-Encoding: aws-chunked` bodies: `<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n…`
//! ending with a zero-size chunk and optional trailers. Signed variants chain
//! each chunk's signature from the request's seed signature.

use crate::error::S3Error;
use crate::sigv4::SigningContext;
use sha2::{Digest, Sha256};

const MAX_LINE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Header,
    Data(u64),
    DataEnd,
    Trailers,
    Done,
}

pub struct ChunkedDecoder {
    pending: Vec<u8>,
    state: State,
    /// Signed bodies: the context and the previous signature.
    signer: Option<(SigningContext, String)>,
    chunk_signature: Option<String>,
    chunk_hash: Sha256,
}

fn bad(message: &str) -> S3Error {
    S3Error::new(http::StatusCode::BAD_REQUEST, "IncompleteBody", message.to_string())
}

impl ChunkedDecoder {
    pub fn new(signer: Option<SigningContext>) -> Self {
        ChunkedDecoder {
            pending: Vec::new(),
            state: State::Header,
            signer: signer.map(|ctx| {
                let seed = ctx.seed_signature.clone();
                (ctx, seed)
            }),
            chunk_signature: None,
            chunk_hash: Sha256::new(),
        }
    }

    fn take_line(&mut self) -> Result<Option<String>, S3Error> {
        let Some(pos) = self.pending.windows(2).position(|w| w == b"\r\n") else {
            if self.pending.len() > MAX_LINE {
                return Err(bad("aws-chunked line too long"));
            }
            return Ok(None);
        };
        let line = String::from_utf8_lossy(&self.pending[..pos]).into_owned();
        self.pending.drain(..pos + 2);
        Ok(Some(line))
    }

    fn verify_chunk(&mut self) -> Result<(), S3Error> {
        let digest = hex::encode(std::mem::replace(&mut self.chunk_hash, Sha256::new()).finalize());
        let Some((ctx, previous)) = &mut self.signer else { return Ok(()) };
        let got = self.chunk_signature.take().ok_or_else(|| bad("aws-chunked chunk without signature"))?;
        let want = ctx.chunk_signature(previous, &digest);
        if want != got {
            return Err(S3Error::auth("SignatureDoesNotMatch", "an aws-chunked chunk signature does not match"));
        }
        *previous = got;
        Ok(())
    }

    /// Decodes what `input` completes; payload bytes are appended to `out`.
    pub fn feed(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), S3Error> {
        self.pending.extend_from_slice(input);
        loop {
            match self.state {
                State::Header => {
                    let Some(line) = self.take_line()? else { return Ok(()) };
                    let mut parts = line.split(';');
                    let size = u64::from_str_radix(parts.next().unwrap_or("").trim(), 16).map_err(|_| bad("bad aws-chunked size"))?;
                    for ext in parts {
                        if let Some(sig) = ext.trim().strip_prefix("chunk-signature=") {
                            self.chunk_signature = Some(sig.to_string());
                        }
                    }
                    if size == 0 {
                        self.verify_chunk()?;
                        self.state = State::Trailers;
                    } else {
                        self.state = State::Data(size);
                    }
                }
                State::Data(remaining) => {
                    if self.pending.is_empty() {
                        return Ok(());
                    }
                    let n = (remaining as usize).min(self.pending.len());
                    self.chunk_hash.update(&self.pending[..n]);
                    out.extend_from_slice(&self.pending[..n]);
                    self.pending.drain(..n);
                    self.state = if remaining as usize == n { State::DataEnd } else { State::Data(remaining - n as u64) };
                }
                State::DataEnd => {
                    if self.pending.len() < 2 {
                        return Ok(());
                    }
                    if &self.pending[..2] != b"\r\n" {
                        return Err(bad("aws-chunked data is not followed by CRLF"));
                    }
                    self.pending.drain(..2);
                    self.verify_chunk()?;
                    self.state = State::Header;
                }
                State::Trailers => {
                    let Some(line) = self.take_line()? else { return Ok(()) };
                    if line.is_empty() {
                        self.state = State::Done;
                    }
                    // x-amz-checksum-* and x-amz-trailer-signature are accepted, not checked (§12.3).
                }
                State::Done => {
                    self.pending.clear();
                    return Ok(());
                }
            }
        }
    }

    pub fn finish(&mut self) -> Result<(), S3Error> {
        if self.state == State::Trailers && self.pending.is_empty() {
            self.state = State::Done;
        }
        if self.state != State::Done {
            return Err(bad("the aws-chunked body ended early"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::{sha256_hex, signing_key};

    fn encode(ctx: Option<&SigningContext>, chunks: &[&[u8]], trailer: bool) -> Vec<u8> {
        let mut out = Vec::new();
        let mut prev = ctx.map(|c| c.seed_signature.clone()).unwrap_or_default();
        let mut all: Vec<&[u8]> = chunks.to_vec();
        all.push(b"");
        for data in all {
            let ext = match ctx {
                Some(c) => {
                    let sig = c.chunk_signature(&prev, &sha256_hex(data));
                    prev = sig.clone();
                    format!(";chunk-signature={sig}")
                }
                None => String::new(),
            };
            out.extend_from_slice(format!("{:x}{ext}\r\n", data.len()).as_bytes());
            if !data.is_empty() {
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
        }
        if trailer {
            out.extend_from_slice(b"x-amz-checksum-crc32:AAAAAA==\r\n");
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    fn ctx() -> SigningContext {
        SigningContext {
            key: signing_key("secret", "20261026", "us-east-1", "s3"),
            amz_date: "20261026T000000Z".into(),
            scope: "20261026/us-east-1/s3/aws4_request".into(),
            seed_signature: "seed".into(),
        }
    }

    #[test]
    fn decodes_signed_and_unsigned_bodies_in_pieces() {
        let c = ctx();
        for (signed, trailer) in [(true, false), (false, true), (true, true)] {
            let body = encode(signed.then_some(&c), &[b"hello ", b"aws-chunked world"], trailer);
            let mut d = ChunkedDecoder::new(signed.then(|| c.clone()));
            let mut out = Vec::new();
            for piece in body.chunks(5) {
                d.feed(piece, &mut out).unwrap();
            }
            d.finish().unwrap();
            assert_eq!(out, b"hello aws-chunked world");
        }
    }

    #[test]
    fn rejects_tampered_and_truncated_bodies() {
        let c = ctx();
        let mut body = encode(Some(&c), &[b"payload"], false);
        let at = body.windows(7).position(|w| w == b"payload").unwrap();
        body[at] = b'P';
        let mut d = ChunkedDecoder::new(Some(c.clone()));
        assert!(d.feed(&body, &mut Vec::new()).is_err());
        let body = encode(Some(&c), &[b"payload"], false);
        let mut d = ChunkedDecoder::new(Some(c));
        d.feed(&body[..body.len() - 4], &mut Vec::new()).unwrap();
        assert!(d.finish().is_err());
    }
}
