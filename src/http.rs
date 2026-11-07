//! Small HTTP/1 helpers shared by the signaller, management listener and probe.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub type Body = Full<Bytes>;

/// Header buffer bound; hyper answers 431 beyond it.
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_CONNECTIONS: usize = 128;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connect: {0}")]
    Connect(std::io::Error),
    #[error("http: {0}")]
    Http(#[from] hyper::Error),
    #[error("timed out")]
    Timeout,
}

/// Sends one request on a fresh connection and returns only the status; the body is never read.
pub async fn send(
    addr: SocketAddr,
    request: Request<Body>,
    timeout: Duration,
) -> Result<StatusCode, ClientError> {
    let exchange = async {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(ClientError::Connect)?;
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let driver = tokio::spawn(conn);
        let status = sender.send_request(request).await.map(|r| r.status());
        driver.abort();
        Ok(status?)
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| ClientError::Timeout)?
}

pub fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    response
}

/// Connection-level limits applied by [`serve`].
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub header_timeout: Duration,
    /// Hard lifetime of one connection; bounds slow writes and idle keep-alive.
    pub connection_timeout: Duration,
}

/// Accepts until `stop`, serving each connection with bounded concurrency, then waits for them.
pub async fn serve<F, Fut>(
    listener: TcpListener,
    limits: Limits,
    handler: F,
    stop: CancellationToken,
) where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    let tracker = TaskTracker::new();
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let accepted = tokio::select! {
            () = stop.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, _)) = accepted else { continue };
        let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
            continue;
        };
        let handler = handler.clone();
        tracker.spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(handler(req).await) }
            });
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_timeout)
                .max_buf_size(MAX_HEADER_BYTES)
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service);
            let _ = tokio::time::timeout(limits.connection_timeout, conn).await;
            drop(permit);
        });
    }
    tracker.close();
    tracker.wait().await;
}
