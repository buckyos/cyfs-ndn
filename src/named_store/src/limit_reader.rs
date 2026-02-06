use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf, Take};

/// 一个限制 AsyncRead 读取长度的 reader。
///
/// 内部直接复用 Tokio 的 `take()`，避免重复实现限长逻辑。
pub struct LimitReader {
    inner: Take<Pin<Box<dyn AsyncRead + Unpin + Send>>>,
}

impl LimitReader {
    /// 从任意 AsyncRead 创建限长 reader。
    pub fn from_reader(reader: Pin<Box<dyn AsyncRead + Unpin + Send>>, size: u64) -> Self {
        Self {
            inner: reader.take(size),
        }
    }

    /// 打开文件某段：先 seek 到 `start`，再限制最多读取 `size` 字节。
    pub async fn from_file<P: AsRef<std::path::Path>>(
        path: P,
        start: u64,
        size: u64,
    ) -> Result<Self, std::io::Error> {
        let mut file = File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        Ok(Self::from_reader(Box::pin(file), size))
    }

    /// 取出内部 Tokio `Take` reader。
    pub fn into_inner(self) -> Take<Pin<Box<dyn AsyncRead + Unpin + Send>>> {
        self.inner
    }
}

/// Helper: 基于 Tokio `take()` 限制读取长度。
pub fn take_reader<R>(reader: R, size: u64) -> Take<R>
where
    R: AsyncRead,
{
    reader.take(size)
}

/// Helper: 打开文件某段，直接返回 Tokio `Take<File>`。
pub async fn open_file_segment<P: AsRef<std::path::Path>>(
    path: P,
    start: u64,
    size: u64,
) -> Result<Take<File>, std::io::Error> {
    let mut file = File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    Ok(file.take(size))
}

impl AsyncRead for LimitReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_limit_reader_from_reader() {
        let data = b"hello-world".to_vec();
        let cursor = std::io::Cursor::new(data);
        let mut reader = LimitReader::from_reader(Box::pin(cursor), 5);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello".to_vec());
    }

    #[tokio::test]
    async fn test_open_file_segment() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("segment.bin");
        tokio::fs::write(&file_path, b"0123456789").await.unwrap();

        let mut reader = open_file_segment(&file_path, 3, 4).await.unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"3456".to_vec());
    }
}
