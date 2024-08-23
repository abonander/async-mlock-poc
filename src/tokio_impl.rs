use crate::MmapFile;
use std::io::SeekFrom;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek, ReadBuf};

impl AsyncRead for MmapFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let read_buf = ready!(self.deref_mut().poll_fill_buf_inner(cx))?;

        let read = buf.capacity();
        buf.put_slice(read_buf);

        self.deref_mut().consume_inner(read);

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for MmapFile {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        Pin::into_inner(self).poll_fill_buf_inner(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.consume_inner(amt)
    }
}

impl AsyncSeek for MmapFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        self.request_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        self.poll_complete_request(cx)
    }
}
