use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone)]
pub struct Route {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub size: Option<usize>,
    pub body: Option<Bytes>,
    pub delay: Duration,
    pub chunk_delay: Duration,
    pub conditional: bool,
    pub echo: Option<String>,
    pub finished: Arc<AtomicBool>,
}

impl Route {
    pub fn headers(headers: &[(&str, &str)]) -> Self {
        Self {
            status: 200,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            size: None,
            body: None,
            delay: Duration::ZERO,
            chunk_delay: Duration::ZERO,
            conditional: false,
            echo: None,
            finished: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Default)]
struct State {
    routes: HashMap<String, Route>,
    counts: HashMap<String, usize>,
    conditional: HashMap<String, usize>,
}

pub struct Origin {
    pub port: u16,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
    pub peak: Arc<AtomicUsize>,
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Origin {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(State::default()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let shared = state.clone();
        let max = peak.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let state = shared.clone();
                        let active = active.clone();
                        let peak = max.clone();
                        connections.spawn(async move {
                            let service = hyper::service::service_fn(move |req| {
                                respond(req, state.clone(), active.clone(), peak.clone())
                            });
                            let _ = tokio::time::timeout(Duration::from_secs(60),
                                hyper::server::conn::http1::Builder::new()
                                    .serve_connection(TokioIo::new(stream), service)).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            port,
            state,
            task,
            peak,
        }
    }

    pub fn set(&self, path: &str, route: Route) {
        self.state.lock().unwrap().routes.insert(path.into(), route);
    }

    pub fn count(&self, path: &str) -> usize {
        *self.state.lock().unwrap().counts.get(path).unwrap_or(&0)
    }

    pub fn validations(&self, path: &str) -> usize {
        *self
            .state
            .lock()
            .unwrap()
            .conditional
            .get(path)
            .unwrap_or(&0)
    }
}

type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

async fn respond(
    req: Request<Incoming>,
    state: Arc<Mutex<State>>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_owned();
    let (route, n, validated) = {
        let mut state = state.lock().unwrap();
        let route = state
            .routes
            .get(&path)
            .cloned()
            .unwrap_or_else(|| Route::headers(&[("cache-control", "max-age=60")]));
        let count = state.counts.entry(path.clone()).or_default();
        *count += 1;
        let n = *count;
        let validated = route.conditional
            && (req
                .headers()
                .get("if-none-match")
                .is_some_and(|v| v == "\"version-one\"")
                || req.headers().get("if-modified-since").is_some());
        if validated {
            *state.conditional.entry(path.clone()).or_default() += 1;
        }
        (route, n, validated)
    };
    let concurrent = active.fetch_add(1, Ordering::SeqCst) + 1;
    peak.fetch_max(concurrent, Ordering::SeqCst);
    tokio::time::sleep(route.delay).await;
    active.fetch_sub(1, Ordering::SeqCst);
    let mut response = Response::builder().status(if validated { 304 } else { route.status });
    for (key, value) in &route.headers {
        response = response.header(key, value);
    }
    let echo = route
        .echo
        .as_ref()
        .and_then(|h| req.headers().get(h))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("absent");
    let body = route
        .body
        .unwrap_or_else(|| Bytes::from(format!("{path} #{n} {echo}")));
    let empty = validated || route.status == 204 || req.method() == hyper::Method::HEAD;
    let size = if empty {
        0
    } else {
        route.size.unwrap_or(body.len())
    };
    let delay = route.chunk_delay;
    let finished = route.finished;
    let generated = route.size.is_some();
    let chunks = futures_util::stream::unfold(
        (0, body, finished),
        move |(offset, body, finished)| async move {
            if offset == size {
                finished.store(true, Ordering::SeqCst);
                return None;
            }
            if offset > 0 {
                tokio::time::sleep(delay).await;
            }
            let len = (size - offset).min(64 * 1024);
            let bytes = if generated {
                Bytes::from(vec![b'x'; len])
            } else {
                body.slice(offset..offset + len)
            };
            Some((
                Ok::<_, Infallible>(Frame::data(bytes)),
                (offset + len, body, finished),
            ))
        },
    );
    Ok(response
        .body(StreamBody::new(chunks).boxed_unsync())
        .unwrap())
}
