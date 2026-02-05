
#[pin_project]
pub struct SimpleChunkListReader {
    named_data_mgr: NamedDataMgrRef,
    auto_cache: bool,

    #[pin]
    loading_future: Option<BoxFuture<'static, std::io::Result<ChunkReader>>>,
    current_reader: Option<ChunkReader>,
    first_chunk: Option<SimpleChunkInfo>,
    remaining_chunks: std::iter::Skip<std::vec::IntoIter<ChunkId>>,
}


impl SimpleChunkListReader {
    pub async fn new(
        named_data_mgr: NamedDataMgrRef,
        chunk_list: SimpleChunkList,
        seek_from: SeekFrom,
        auto_cache: bool,
    ) -> NdnResult<Self> {
        

        // Calculate the first chunk index and offset based on the seek_from position
        let (chunk_index, chunk_offset) = chunk_list.get_chunk_index_by_offset(seek_from)?;

        // Get the first chunk ID from the chunk list
        let first_chunk_id = chunk_list.body.get(chunk_index as usize);
        if first_chunk_id.is_none() {
            let msg = format!(
                "chunk index {} not found in chunk list ",
                chunk_index
            );
            warn!("{}", msg);
            return Err(NdnError::NotFound(msg));
        }

        let first_chunk_id = first_chunk_id.unwrap().clone();

        let remaining_chunks = chunk_list.body.into_iter().skip(chunk_index as usize + 1);

        Ok(Self {
            named_data_mgr,
            auto_cache,
            current_reader: None,
            loading_future: None,
            first_chunk: Some(SimpleChunkInfo {
                chunk_id: first_chunk_id.clone(),
                offset: chunk_offset,
            }),
            remaining_chunks: remaining_chunks,
        })
    }

    async fn load_chunk_reader(
        named_data_mgr: NamedDataMgrRef,
        chunk_id: ChunkId,
        offset: u64,
        auto_cache: bool,
    ) -> std::io::Result<ChunkReader> {
        let mut mgr = named_data_mgr.lock().await;
        let (reader, _) = mgr
            .open_chunk_reader_impl(&chunk_id, offset, auto_cache)
            .await
            .map_err(|e| {
                warn!(
                    "Failed to open chunk reader for {}: {}",
                    chunk_id.to_base32(),
                    e
                );
                std::io::Error::new(std::io::ErrorKind::Other, e)
            })?;
        Ok(reader)
    }
}

impl AsyncRead for SimpleChunkListReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.get_mut();

        loop {
            // First check if we have a current reader
            if let Some(reader) = this.current_reader.as_mut() {
                let current_len = buf.filled().len();
                match Pin::new(reader).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - current_len;
                        if bytes_read > 0 {
                            break Poll::Ready(Ok(()));
                        } else {
                            this.current_reader = None; // Clear current reader if no bytes read
                        }
                    }
                    Poll::Ready(Err(e)) => break Poll::Ready(Err(e)),
                    Poll::Pending => break Poll::Pending,
                }
            }

            // If no current reader, try to load next chunk reader
            if let Some(fut) = this.loading_future.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(new_reader)) => {
                        this.current_reader = Some(new_reader);
                        this.loading_future = None; // Clear the loading future
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                let (next_chunk, chunk_offset) = if let Some(info) = this.first_chunk.take() {
                    (Some(info.chunk_id), info.offset)
                } else {
                    (this.remaining_chunks.next(), 0)
                };
                
                if let Some(chunk_id) = next_chunk {
                    // Load the next chunk reader
                    this.loading_future = Some(
                        Self::load_chunk_reader(
                            this.named_data_mgr.clone(),
                            chunk_id, 
                            chunk_offset,
                            this.auto_cache,
                        ).boxed(),
                    );
                } else {
                    // No more chunks to read, return EOF
                    break Poll::Ready(Ok(()));
                }

            }
        }
    }
}