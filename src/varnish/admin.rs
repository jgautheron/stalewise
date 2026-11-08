//! Minimal Varnish CLI client: framing, challenge authentication and VCL commands.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const HEADER_LEN: usize = 13;
const MAX_BODY: usize = 1 << 20;
const MAX_SECRET: u64 = 64 * 1024;
const CHALLENGE_LEN: usize = 32;
const MAX_DIAGNOSTIC: usize = 1024;
const STATUS_OK: u16 = 200;
const STATUS_AUTH: u16 = 107;

/// Admin protocol failures; received bytes never panic the client.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("admin I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("admin command timed out")]
    Timeout,
    #[error("admin authentication failed")]
    Authentication,
    #[error("admin protocol violation: {0}")]
    Protocol(&'static str),
    #[error("admin response exceeds the size limit")]
    TooLarge,
    #[error("admin command failed with status {status}: {diagnostic}")]
    Command { status: u16, diagnostic: String },
}

type Result<T> = std::result::Result<T, AdminError>;

/// How to reach and authenticate to the loopback admin port.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub addr: SocketAddr,
    pub secret_file: PathBuf,
    pub timeout: Duration,
}

/// A VCL name made only of identifier characters, so it cannot split a CLI command.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VclName(String);

/// Prefix marking configurations this process loaded and may discard.
pub const OWNED_PREFIX: &str = "stalewise_";

impl VclName {
    /// A fresh owned name: monotonic sequence plus random suffix.
    pub fn generate(sequence: u64) -> Self {
        let suffix = crate::entropy::weak_u64() as u32;
        Self(format!("{OWNED_PREFIX}{sequence}_{suffix:08x}"))
    }

    pub fn is_owned(&self) -> bool {
        self.0.starts_with(OWNED_PREFIX)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for VclName {
    type Err = AdminError;

    fn from_str(raw: &str) -> Result<Self> {
        let ok = raw.len() <= 255
            && raw.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && raw
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if ok {
            Ok(Self(raw.to_owned()))
        } else {
            Err(AdminError::Protocol("invalid VCL name"))
        }
    }
}

impl fmt::Display for VclName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VclStatus {
    Active,
    Available,
    Discarded,
}

/// One `vcl.list` row; labels are listed but never treated as owned programs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VclState {
    pub name: VclName,
    pub status: VclStatus,
    pub is_label: bool,
    pub labelled: bool,
}

/// The `ping` reply body, e.g. `PONG 1700000000 1.0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pong(pub String);

/// An authenticated CLI session; any error closes it for good.
#[derive(Debug)]
pub struct Client {
    stream: Option<TcpStream>,
    timeout: Duration,
}

/// Connects, reads the banner and answers the challenge with the current secret file.
pub async fn connect(config: &AdminConfig) -> Result<Client> {
    let secret = read_secret(config).await?;
    let stream = tokio::time::timeout(config.timeout, TcpStream::connect(config.addr))
        .await
        .map_err(|_| AdminError::Timeout)??;
    let mut client = Client {
        stream: Some(stream),
        timeout: config.timeout,
    };
    client.authenticate(&secret).await?;
    Ok(client)
}

async fn read_secret(config: &AdminConfig) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(&config.secret_file).await?;
    let mut secret = Vec::new();
    file.take(MAX_SECRET + 1).read_to_end(&mut secret).await?;
    if secret.is_empty() || secret.len() as u64 > MAX_SECRET {
        return Err(AdminError::Authentication);
    }
    Ok(secret)
}

/// Lowercase hex SHA-256 of `challenge LF secret challenge LF`, per the CLI protocol.
pub fn auth_response(challenge: &[u8], secret: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(challenge);
    hash.update(b"\n");
    hash.update(secret);
    hash.update(challenge);
    hash.update(b"\n");
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

impl Client {
    async fn authenticate(&mut self, secret: &[u8]) -> Result<()> {
        let (status, body) = self.exchange(None).await?;
        if status != STATUS_AUTH {
            self.stream = None;
            return Err(AdminError::Authentication);
        }
        let challenge = body
            .get(..CHALLENGE_LEN)
            .filter(|c| c.iter().all(u8::is_ascii_alphabetic))
            .ok_or(AdminError::Protocol("malformed challenge"))?
            .to_vec();
        let line = format!("auth {}\n", auth_response(&challenge, secret));
        let (status, _) = self.exchange(Some(line.as_bytes())).await?;
        if status != STATUS_OK {
            self.stream = None;
            return Err(AdminError::Authentication);
        }
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<Pong> {
        let body = self.command(b"ping\n").await?;
        Ok(Pong(String::from_utf8_lossy(&body).trim().to_owned()))
    }

    /// Compiles and loads `source` under `name`; 201 (truncated) counts as failure.
    pub async fn inline(&mut self, name: &VclName, source: &[u8]) -> Result<()> {
        let mut line = format!("vcl.inline {name} ").into_bytes();
        quote_into(source, &mut line)?;
        line.push(b'\n');
        self.command(&line).await.map(drop)
    }

    pub async fn use_vcl(&mut self, name: &VclName) -> Result<()> {
        self.command(format!("vcl.use {name}\n").as_bytes())
            .await
            .map(drop)
    }

    pub async fn discard(&mut self, name: &VclName) -> Result<()> {
        self.command(format!("vcl.discard {name}\n").as_bytes())
            .await
            .map(drop)
    }

    /// Bans every object cached longer than `age`, in whole seconds. The expression uses only
    /// `obj.*`, so the ban lurker evaluates it without waiting for requests.
    pub async fn ban_older_than(&mut self, age: Duration) -> Result<()> {
        self.command(format!("ban obj.age > {}s\n", age.as_secs()).as_bytes())
            .await
            .map(drop)
    }

    pub async fn list(&mut self) -> Result<Vec<VclState>> {
        let body = self.command(b"vcl.list\n").await?;
        let text =
            std::str::from_utf8(&body).map_err(|_| AdminError::Protocol("non-UTF-8 vcl.list"))?;
        parse_list(text)
    }

    async fn command(&mut self, line: &[u8]) -> Result<Vec<u8>> {
        let (status, body) = self.exchange(Some(line)).await?;
        if status == STATUS_OK {
            return Ok(body);
        }
        Err(AdminError::Command {
            status,
            diagnostic: sanitize_diagnostic(&body),
        })
    }

    /// One bounded request/response; failures and timeouts drop the socket.
    async fn exchange(&mut self, line: Option<&[u8]>) -> Result<(u16, Vec<u8>)> {
        let mut stream = self
            .stream
            .take()
            .ok_or(AdminError::Protocol("session already closed"))?;
        let outcome = tokio::time::timeout(self.timeout, round_trip(&mut stream, line)).await;
        let result = outcome.map_err(|_| AdminError::Timeout).and_then(|r| r);
        if result.is_ok() {
            self.stream = Some(stream);
        }
        result
    }
}

async fn round_trip(stream: &mut TcpStream, line: Option<&[u8]>) -> Result<(u16, Vec<u8>)> {
    if let Some(line) = line {
        stream.write_all(line).await?;
        stream.flush().await?;
    }
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let (status, len) = parse_header(&header)?;
    let mut body = vec![0u8; len + 1];
    stream.read_exact(&mut body).await?;
    if body.pop() != Some(b'\n') {
        return Err(AdminError::Protocol("missing frame terminator"));
    }
    Ok((status, body))
}

/// Parses `NNN LLLLLLLL\n`: three status digits, space, space-padded decimal length.
pub fn parse_header(header: &[u8; HEADER_LEN]) -> Result<(u16, usize)> {
    let malformed = AdminError::Protocol("malformed status line");
    let (status, rest) = header.split_at(3);
    if !status.iter().all(u8::is_ascii_digit)
        || rest.first() != Some(&b' ')
        || rest.last() != Some(&b'\n')
    {
        return Err(malformed);
    }
    let field = rest
        .get(1..rest.len() - 1)
        .ok_or(AdminError::Protocol("short header"))?;
    let digits_end = field.iter().position(|b| *b == b' ').unwrap_or(field.len());
    let (digits, padding) = field.split_at(digits_end);
    if digits.is_empty()
        || !digits.iter().all(u8::is_ascii_digit)
        || !padding.iter().all(|b| *b == b' ')
    {
        return Err(malformed);
    }
    let status = ascii_number(status).ok_or(AdminError::Protocol("malformed status"))?;
    let len = ascii_number(digits).ok_or(AdminError::Protocol("malformed length"))?;
    if len > MAX_BODY {
        return Err(AdminError::TooLarge);
    }
    let status = u16::try_from(status).map_err(|_| AdminError::Protocol("status out of range"))?;
    Ok((status, len))
}

fn ascii_number(digits: &[u8]) -> Option<usize> {
    digits.iter().try_fold(0usize, |acc, d| {
        acc.checked_mul(10)?.checked_add(usize::from(d - b'0'))
    })
}

/// Appends a CLI double-quoted argument; other control bytes are refused, never passed raw.
pub fn quote_into(source: &[u8], out: &mut Vec<u8>) -> Result<()> {
    out.push(b'"');
    for &b in source {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'"' => out.extend_from_slice(b"\\\""),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b if b.is_ascii_control() => {
                return Err(AdminError::Protocol("control byte in VCL source"));
            }
            b => out.push(b),
        }
    }
    out.push(b'"');
    Ok(())
}

/// Parses the 9.1 `vcl.list` text layout; anything unrecognised fails closed.
pub fn parse_list(text: &str) -> Result<Vec<VclState>> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_row)
        .collect()
}

fn parse_row(line: &str) -> Result<VclState> {
    let unknown = AdminError::Protocol("unknown vcl.list layout");
    let cols: Vec<&str> = line.split_whitespace().collect();
    let [status, state, temperature, busy, name, rest @ ..] = cols.as_slice() else {
        return Err(unknown);
    };
    let status = match *status {
        "active" => VclStatus::Active,
        "available" => VclStatus::Available,
        "discarded" => VclStatus::Discarded,
        _ => return Err(unknown),
    };
    let known_state = ["auto", "cold", "warm", "label"].contains(state);
    let known_temp = ["init", "cold", "warm", "busy", "cooling"].contains(temperature);
    if !known_state || !known_temp || !busy.bytes().all(|b| b.is_ascii_digit()) {
        return Err(unknown);
    }
    let labelled = match rest {
        [] | ["->", _] => false,
        ["<-", count, "label)" | "labels)"] if count.starts_with('(') => true,
        _ => return Err(unknown),
    };
    Ok(VclState {
        name: name.parse()?,
        status,
        is_label: *state == "label",
        labelled,
    })
}

/// Keeps compiler messages but drops VCL source excerpts and marker lines after `At:`.
pub fn sanitize_diagnostic(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut kept = Vec::new();
    let mut skip = 0;
    for line in text.lines() {
        if skip > 0 {
            skip -= 1;
            continue;
        }
        if line.trim_start().starts_with("At: (")
            || (line.contains("Line ") && line.contains(" Pos "))
        {
            skip = 2;
        }
        if !line.trim().is_empty() {
            kept.push(line.trim());
        }
    }
    let joined: String = kept
        .join(" | ")
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_DIAGNOSTIC)
        .collect();
    joined
}

#[cfg(test)]
pub(crate) mod fake;

#[cfg(test)]
mod tests {
    use super::fake::{Behavior, FakeVarnish};
    use super::*;

    const LIST_FIXTURE: &str = "available   auto    warm   0   boot\n\
active      auto    warm   0   x      <-   (1 label)\n\
available   label   warm   0   lab    ->   x\n\
available   cold    cold   0   stalewise_3_0000beef\n\n";

    #[test]
    fn parses_recorded_vcl_list_layout() {
        let rows = parse_list(LIST_FIXTURE).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[1].status, VclStatus::Active);
        assert!(rows[1].labelled);
        assert!(rows[2].is_label);
        assert!(rows[3].name.is_owned());
    }

    #[test]
    fn unknown_list_layout_fails_closed() {
        for bad in [
            "weird auto warm 0 x",
            "active auto warm 0",
            "active auto warm x y",
            "active auto warm 0 x <- y",
        ] {
            assert!(parse_list(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn header_parsing_is_strict() {
        assert_eq!(parse_header(b"200 12      \n").unwrap(), (200, 12));
        assert_eq!(parse_header(b"107 59      \n").unwrap(), (107, 59));
        assert!(matches!(
            parse_header(b"200 99999999\n"),
            Err(AdminError::TooLarge)
        ));
        for bad in [
            b"20x 1       \n",
            b"200 1 2     \n",
            b"200         \n",
            b"200 1       x",
            b"200-1       \n",
        ] {
            assert!(parse_header(bad).is_err());
        }
    }

    #[test]
    fn quoting_cannot_introduce_a_second_command() {
        let mut out = Vec::new();
        quote_into(b"a\"b\\c\nvcl.discard boot\r\t", &mut out).unwrap();
        assert_eq!(out, b"\"a\\\"b\\\\c\\nvcl.discard boot\\r\\t\"");
        assert!(!out.contains(&b'\n'));
        assert!(quote_into(b"a\x01", &mut Vec::new()).is_err());
    }

    #[test]
    fn auth_response_matches_protocol_reference() {
        // challenge + LF + secret + challenge + LF, secret newline preserved.
        let challenge = b"ixslvvxrgkjptxmcgnnsdxsvdmvfympg";
        let got = auth_response(challenge, b"foo\n");
        let mut input = challenge.to_vec();
        input.extend_from_slice(b"\nfoo\n");
        input.extend_from_slice(challenge);
        input.push(b'\n');
        let want: String = Sha256::digest(&input)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn diagnostics_drop_source_excerpts() {
        let body = b"Message from VCC-compiler:\nSymbol not found: 'req.foo'\nAt: ('<vcl.inline>' Line 1 Pos 45) -- (Pos 51)\nsecret_backend_source { set req.foo = 1; }\n-----####---\n\nRunning VCC-compiler failed, exited with 2\nVCL compilation failed";
        let got = sanitize_diagnostic(body);
        assert!(got.contains("Symbol not found"));
        assert!(got.contains("VCL compilation failed"));
        assert!(!got.contains("secret_backend_source"));
        assert!(!got.contains("####"));
    }

    #[test]
    fn vcl_names_cannot_carry_syntax() {
        assert!("a b".parse::<VclName>().is_err());
        assert!("1a".parse::<VclName>().is_err());
        assert!("x\n".parse::<VclName>().is_err());
        let generated = VclName::generate(7);
        assert!(generated.is_owned());
        assert!(generated.as_str().parse::<VclName>().is_ok());
    }

    #[tokio::test]
    async fn fragmented_frames_authenticate_and_run_commands() {
        let fake = FakeVarnish::start(Behavior {
            fragment: true,
            ..Behavior::default()
        })
        .await;
        let mut client = connect(&fake.config()).await.unwrap();
        assert!(client.ping().await.unwrap().0.starts_with("PONG"));
        let name = VclName::generate(1);
        client
            .inline(&name, b"vcl 4.1;\n\"quoted\"\\")
            .await
            .unwrap();
        client.use_vcl(&name).await.unwrap();
        let list = client.list().await.unwrap();
        assert!(
            list.iter()
                .any(|v| v.name == name && v.status == VclStatus::Active)
        );
        assert_eq!(fake.source(&name).unwrap(), b"vcl 4.1;\n\"quoted\"\\");
        assert_eq!(fake.active(), name.as_str());
        client.discard(&"boot".parse().unwrap()).await.unwrap();
        assert_eq!(fake.names(), [name.as_str()]);
    }

    #[tokio::test]
    async fn rejected_auth_is_typed() {
        let fake = FakeVarnish::start(Behavior {
            reject_auth: true,
            ..Behavior::default()
        })
        .await;
        assert!(matches!(
            connect(&fake.config()).await,
            Err(AdminError::Authentication)
        ));
    }

    #[tokio::test]
    async fn wrong_secret_is_rejected() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        std::fs::write(&fake.secret_path, b"different").unwrap();
        assert!(matches!(
            connect(&fake.config()).await,
            Err(AdminError::Authentication)
        ));
    }

    #[tokio::test]
    async fn stalled_server_times_out_and_closes_session() {
        let fake = FakeVarnish::start(Behavior {
            stall_on: Some("ping"),
            ..Behavior::default()
        })
        .await;
        let mut client = connect(&fake.config()).await.unwrap();
        assert!(matches!(client.ping().await, Err(AdminError::Timeout)));
        assert!(matches!(client.ping().await, Err(AdminError::Protocol(_))));
    }

    #[tokio::test]
    async fn oversize_response_is_rejected() {
        let fake = FakeVarnish::start(Behavior {
            oversize_on: Some("vcl.list"),
            ..Behavior::default()
        })
        .await;
        let mut client = connect(&fake.config()).await.unwrap();
        assert!(matches!(client.list().await, Err(AdminError::TooLarge)));
    }

    #[tokio::test]
    async fn compile_failure_is_a_command_error_with_sanitized_diagnostic() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let mut client = connect(&fake.config()).await.unwrap();
        let err = client
            .inline(&VclName::generate(2), b"COMPILE_FAIL private_detail")
            .await
            .unwrap_err();
        match err {
            AdminError::Command { status, diagnostic } => {
                assert_eq!(status, 106);
                assert!(!diagnostic.contains("private_detail"));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(
            client.ping().await.is_ok(),
            "command errors keep the session"
        );
    }

    #[tokio::test]
    async fn truncated_201_counts_as_failure() {
        let fake = FakeVarnish::start(Behavior {
            truncate_on: Some("vcl.inline"),
            ..Behavior::default()
        })
        .await;
        let mut client = connect(&fake.config()).await.unwrap();
        let err = client
            .inline(&VclName::generate(3), b"vcl 4.1;")
            .await
            .unwrap_err();
        assert!(matches!(err, AdminError::Command { status: 201, .. }));
    }
}
