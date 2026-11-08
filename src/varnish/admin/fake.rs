//! In-process fake CLI server that misbehaves on request.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::{AdminConfig, VclName, auth_response};

const CHALLENGE: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyzabcdef";

#[derive(Debug, Clone, Default)]
pub struct Behavior {
    pub fragment: bool,
    pub reject_auth: bool,
    pub stall_on: Option<&'static str>,
    pub oversize_on: Option<&'static str>,
    pub truncate_on: Option<&'static str>,
    /// Applies the command, then closes without replying, once.
    pub lose_reply_on: Option<&'static str>,
    /// Fails `vcl.discard` with a command error.
    pub fail_discard: bool,
    /// Fails every `vcl.use` with a command error, leaving the active VCL unchanged.
    pub reject_use: bool,
}

#[derive(Debug)]
pub struct Vcl {
    pub name: String,
    pub source: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct State {
    pub vcls: Vec<Vcl>,
    pub active: String,
    pub inline_count: usize,
    pub lost_reply: bool,
    pub bans: Vec<String>,
}

pub struct FakeVarnish {
    pub addr: SocketAddr,
    pub secret_path: PathBuf,
    pub state: Arc<Mutex<State>>,
}

impl FakeVarnish {
    pub async fn start(behavior: Behavior) -> Self {
        let dir = crate::testutil::temp_dir("fake-admin");
        let secret_path = dir.join("secret");
        std::fs::write(&secret_path, b"s3cret\n").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State {
            vcls: vec![Vcl {
                name: "boot".into(),
                source: b"vcl 4.1;".to_vec(),
            }],
            active: "boot".into(),
            ..State::default()
        }));
        let shared = Arc::clone(&state);
        let secret = std::fs::read(&secret_path).unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let behavior = behavior.clone();
                let state = Arc::clone(&shared);
                tokio::spawn(serve(stream, behavior, state, secret.clone()));
            }
        });
        Self {
            addr,
            secret_path,
            state,
        }
    }

    pub fn config(&self) -> AdminConfig {
        AdminConfig {
            addr: self.addr,
            secret_file: self.secret_path.clone(),
            timeout: Duration::from_millis(300),
        }
    }

    pub fn source(&self, name: &VclName) -> Option<Vec<u8>> {
        let state = self.state.lock().unwrap();
        state
            .vcls
            .iter()
            .find(|v| v.name == name.as_str())
            .map(|v| v.source.clone())
    }

    pub fn active(&self) -> String {
        self.state.lock().unwrap().active.clone()
    }

    pub fn names(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state.vcls.iter().map(|v| v.name.clone()).collect()
    }
}

async fn write_frame(stream: &mut TcpStream, fragment: bool, status: u16, body: &[u8]) {
    let mut frame = format!("{status:03} {:<8}\n", body.len()).into_bytes();
    frame.extend_from_slice(body);
    frame.push(b'\n');
    if fragment {
        for b in frame {
            let _ = stream.write_all(&[b]).await;
            tokio::task::yield_now().await;
        }
    } else {
        let _ = stream.write_all(&frame).await;
    }
}

async fn serve(stream: TcpStream, behavior: Behavior, state: Arc<Mutex<State>>, secret: Vec<u8>) {
    let mut reader = BufReader::new(stream);
    let mut banner = CHALLENGE.to_vec();
    banner.extend_from_slice(b"\n\nAuthentication required.\n");
    write_frame(reader.get_mut(), behavior.fragment, 107, &banner).await;
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await.unwrap_or(0) == 0 {
        return;
    }
    let expected = format!("auth {}\n", auth_response(CHALLENGE, &secret));
    if behavior.reject_auth || line != expected.as_bytes() {
        write_frame(
            reader.get_mut(),
            behavior.fragment,
            107,
            b"Authentication required.",
        )
        .await;
        return;
    }
    write_frame(
        reader.get_mut(),
        behavior.fragment,
        200,
        b"Varnish Cache CLI fake",
    )
    .await;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let text = String::from_utf8_lossy(&line).trim_end().to_owned();
        let verb = text.split(' ').next().unwrap_or_default().to_owned();
        if behavior.stall_on == Some(verb.as_str()) {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            return;
        }
        if behavior.oversize_on == Some(verb.as_str()) {
            let _ = reader.get_mut().write_all(b"200 2000000 \n").await;
            continue;
        }
        if behavior.truncate_on == Some(verb.as_str()) {
            write_frame(reader.get_mut(), behavior.fragment, 201, b"truncated").await;
            continue;
        }
        let (status, body) = apply(&text, &behavior, &state);
        let lose = behavior.lose_reply_on == Some(verb.as_str()) && {
            let mut st = state.lock().unwrap();
            !std::mem::replace(&mut st.lost_reply, true)
        };
        if lose {
            return;
        }
        write_frame(reader.get_mut(), behavior.fragment, status, body.as_bytes()).await;
    }
}

fn apply(text: &str, behavior: &Behavior, state: &Mutex<State>) -> (u16, String) {
    let mut st = state.lock().unwrap();
    let mut parts = text.splitn(3, ' ');
    let verb = parts.next().unwrap_or_default();
    let name = parts.next().unwrap_or_default().to_owned();
    match verb {
        "ping" => (200, "PONG 1700000000 1.0".into()),
        "ban" => {
            st.bans.push(text.trim_start_matches("ban ").to_owned());
            (200, String::new())
        }
        "vcl.inline" => {
            let source = unquote(parts.next().unwrap_or_default());
            st.inline_count += 1;
            if source.windows(12).any(|w| w == b"COMPILE_FAIL") {
                return (
                    106,
                    "Message from VCC-compiler:\nExpected ';'\nAt: ('<vcl.inline>' Line 1 Pos 1)\nCOMPILE_FAIL private_detail\n#####\nVCL compilation failed".into(),
                );
            }
            st.vcls.push(Vcl { name, source });
            (200, "VCL compiled.".into())
        }
        "vcl.use" if behavior.reject_use => (106, "VCL cannot be activated".into()),
        "vcl.use" if st.vcls.iter().any(|v| v.name == name) => {
            st.active = name.clone();
            (200, format!("VCL '{name}' now active"))
        }
        "vcl.discard" if behavior.fail_discard => (300, "refused".into()),
        "vcl.discard" if st.active == name => {
            (300, format!("Cannot discard active VCL program {name}"))
        }
        "vcl.discard" if st.vcls.iter().any(|v| v.name == name) => {
            st.vcls.retain(|v| v.name != name);
            (200, String::new())
        }
        "vcl.list" => {
            let rows: Vec<String> = st
                .vcls
                .iter()
                .map(|v| {
                    let status = if v.name == st.active {
                        "active"
                    } else {
                        "available"
                    };
                    format!("{status:<11} auto    warm   0   {}", v.name)
                })
                .collect();
            (200, rows.join("\n"))
        }
        _ => (106, format!("No VCL named {name} known")),
    }
}

fn unquote(raw: &str) -> Vec<u8> {
    let inner = raw
        .strip_prefix('"')
        .and_then(|r| r.strip_suffix('"'))
        .unwrap_or(raw);
    let mut out = Vec::new();
    let mut bytes = inner.bytes();
    while let Some(b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            Some(b'n') => out.push(b'\n'),
            Some(b'r') => out.push(b'\r'),
            Some(b't') => out.push(b'\t'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}
