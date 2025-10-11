use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::poll_fn, AsyncRead, AsyncWrite, Stream};
use yamux::{Config, Connection, Mode};

pub struct YamuxSession<T> {
    inner: Connection<T>,
}

impl<T> YamuxSession<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(conn: Connection<T>) -> Self {
        Self { inner: conn }
    }

    pub fn new_client(socket: T, config: Option<Config>) -> Self {
        let cfg = config.unwrap_or_default();
        let conn = Connection::new(socket, cfg, Mode::Client);
        Self { inner: conn }
    }

    pub fn new_server(socket: T, config: Option<Config>) -> Self {
        let cfg = config.unwrap_or_default();
        let conn = Connection::new(socket, cfg, Mode::Server);
        Self { inner: conn }
    }

    pub async fn create_stream(&mut self) -> anyhow::Result<yamux::Stream> {
        poll_fn(|cx| self.inner.poll_new_outbound(cx)).await.map_err(|e| anyhow::anyhow!(e))
    }
}

impl<T> Stream for YamuxSession<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = anyhow::Result<yamux::Stream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.inner.poll_next_inbound(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(stream)) => Poll::Ready(Some(stream.map_err(|e| {
                log::error!("[YamuxSession] poll_next_inbound error {e:?}");
                anyhow::anyhow!(e)
            }))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
