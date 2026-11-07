//! Validated network identities shared by discovery, rendering and the signaller.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use serde::Serialize;

/// Rejected network identity input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    #[error("invalid DNS name {0:?}")]
    Dns(String),
    #[error("invalid CIDR {0:?}")]
    Cidr(String),
    #[error("invalid authority {0:?}")]
    Authority(String),
}

const MAX_DNS_LEN: usize = 253;
const MAX_LABEL_LEN: usize = 63;

/// A lowercase RFC 1123 hostname; it cannot carry VCL or ban-expression syntax.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DnsName(String);

impl DnsName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DnsName {
    type Err = AddressError;

    /// Parses a hostname; rejects IP literals, empty labels and non-LDH bytes.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let name = raw.strip_suffix('.').unwrap_or(raw).to_ascii_lowercase();
        let valid = !name.is_empty()
            && name.len() <= MAX_DNS_LEN
            && name.parse::<IpAddr>().is_err()
            && name.split('.').all(valid_label);
        if valid {
            Ok(Self(name))
        } else {
            Err(AddressError::Dns(raw.to_owned()))
        }
    }
}

fn valid_label(label: &str) -> bool {
    let bytes = label.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_LABEL_LEN
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
        && bytes.first() != Some(&b'-')
        && bytes.last() != Some(&b'-')
}

impl fmt::Display for DnsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where an endpoint lives: a literal address or a Service DNS name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Host {
    Ip(IpAddr),
    Dns(DnsName),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => ip.fmt(f),
            Self::Dns(name) => name.fmt(f),
        }
    }
}

impl Serialize for Host {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// One addressable origin or peer; `name` is a VCL-safe identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Endpoint {
    pub name: String,
    pub host: Host,
    pub port: u16,
}

impl Endpoint {
    /// Builds an endpoint with its sanitised VCL identifier.
    pub fn new(host: Host, port: u16) -> Self {
        Self {
            name: vcl_identifier(&host, port),
            host,
            port,
        }
    }

    /// `host:port` with IPv6 brackets, as used in Host headers and socket strings.
    pub fn authority(&self) -> String {
        authority(&self.host, self.port)
    }
}

/// Formats `host:port`, bracketing IPv6 literals.
pub fn authority(host: &Host, port: u16) -> String {
    match host {
        Host::Ip(IpAddr::V6(ip)) => format!("[{ip}]:{port}"),
        other => format!("{other}:{port}"),
    }
}

/// `ip_`/`dns_` prefix, `[a-z0-9_]` body, then port and address family.
pub fn vcl_identifier(host: &Host, port: u16) -> String {
    let (prefix, body, family) = match host {
        Host::Ip(ip @ IpAddr::V4(_)) => ("ip", ip.to_string(), "_v4"),
        Host::Ip(ip @ IpAddr::V6(_)) => ("ip", ip.to_string(), "_v6"),
        Host::Dns(name) => ("dns", name.to_string(), ""),
    };
    let body: String = body
        .chars()
        .map(|c| match c.to_ascii_lowercase() {
            c @ ('a'..='z' | '0'..='9') => c,
            _ => '_',
        })
        .collect();
    format!("{prefix}_{body}_{port}{family}")
}

/// An IP network with host bits cleared, so VCL ACL compilation never sees a mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// The single-address network, `/32` or `/128`, for `addr`.
    pub fn host(addr: IpAddr) -> Self {
        let prefix = if addr.is_ipv4() { 32 } else { 128 };
        Self { addr, prefix }
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub fn prefix(&self) -> u8 {
        self.prefix
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                mask_v4(u32::from(ip), self.prefix) == u32::from(net)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                mask_v6(u128::from(ip), self.prefix) == u128::from(net)
            }
            _ => false,
        }
    }
}

fn mask_v4(bits: u32, prefix: u8) -> u32 {
    u32::MAX
        .checked_shl(32 - u32::from(prefix))
        .map_or(0, |mask| bits & mask)
}

fn mask_v6(bits: u128, prefix: u8) -> u128 {
    u128::MAX
        .checked_shl(128 - u32::from(prefix))
        .map_or(0, |mask| bits & mask)
}

impl FromStr for Cidr {
    type Err = AddressError;

    /// Parses `addr/prefix` or a bare address (full-length prefix).
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let invalid = || AddressError::Cidr(raw.to_owned());
        let (addr, prefix) = match raw.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (raw, None),
        };
        let addr: IpAddr = addr.parse().map_err(|_| invalid())?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) if p.bytes().all(|b| b.is_ascii_digit()) => {
                p.parse::<u8>().map_err(|_| invalid())?
            }
            Some(_) => return Err(invalid()),
            None => max,
        };
        if prefix > max {
            return Err(invalid());
        }
        let addr = match addr {
            IpAddr::V4(ip) => IpAddr::V4(mask_v4(u32::from(ip), prefix).into()),
            IpAddr::V6(ip) => IpAddr::V6(mask_v6(u128::from(ip), prefix).into()),
        };
        Ok(Self { addr, prefix })
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl Serialize for Cidr {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Cidr", 2)?;
        st.serialize_field("addr", &self.addr.to_string())?;
        st.serialize_field("prefix", &self.prefix)?;
        st.end()
    }
}

/// A validated `host[:port]` HTTP authority used as a Host header value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Authority(String);

impl Authority {
    pub fn from_parts(host: &Host, port: Option<u16>) -> Self {
        match port {
            Some(port) => Self(authority(host, port)),
            None => match host {
                Host::Ip(IpAddr::V6(ip)) => Self(format!("[{ip}]")),
                other => Self(other.to_string()),
            },
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Authority {
    type Err = AddressError;

    /// Accepts `name`, `name:port`, `ip`, `ip:port`, `[v6]` or `[v6]:port`.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let invalid = || AddressError::Authority(raw.to_owned());
        let (host, port) = split_authority(raw).ok_or_else(invalid)?;
        let port = port
            .map(|p| {
                p.parse::<u16>()
                    .ok()
                    .filter(|p| *p != 0)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        let host = match host.parse::<IpAddr>() {
            Ok(ip) => Host::Ip(ip),
            Err(_) => Host::Dns(host.parse().map_err(|_| invalid())?),
        };
        Ok(Self::from_parts(&host, port))
    }
}

fn split_authority(raw: &str) -> Option<(&str, Option<&str>)> {
    if let Some(rest) = raw.strip_prefix('[') {
        let (host, tail) = rest.split_once(']')?;
        host.parse::<std::net::Ipv6Addr>().ok()?;
        return match tail {
            "" => Some((host, None)),
            t => Some((host, Some(t.strip_prefix(':')?))),
        };
    }
    match raw.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => Some((host, Some(port))),
        Some(_) => None,
        None => Some((raw, None)),
    }
}

impl fmt::Display for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_vcl_safe_and_family_tagged() {
        let v4 = Endpoint::new(Host::Ip("10.0.0.7".parse().unwrap()), 8080);
        assert_eq!(v4.name, "ip_10_0_0_7_8080_v4");
        let v6 = Endpoint::new(Host::Ip("fd00::1".parse().unwrap()), 80);
        assert_eq!(v6.name, "ip_fd00__1_80_v6");
        assert_eq!(v6.authority(), "[fd00::1]:80");
        let dns = Endpoint::new(Host::Dns("Catalog.apps.svc".parse().unwrap()), 80);
        assert_eq!(dns.name, "dns_catalog_apps_svc_80");
    }

    #[test]
    fn dns_names_reject_injection_bytes() {
        for bad in ["", "a b", "a\"b", "a..b", "-a.b", "10.0.0.1", "a;b", "a/b"] {
            assert!(bad.parse::<DnsName>().is_err(), "{bad}");
        }
        assert_eq!(
            "Example.COM.".parse::<DnsName>().unwrap().as_str(),
            "example.com"
        );
    }

    #[test]
    fn cidr_clears_host_bits_and_bounds_prefix() {
        assert_eq!(
            "10.1.2.3/8".parse::<Cidr>().unwrap().to_string(),
            "10.0.0.0/8"
        );
        assert_eq!("::1".parse::<Cidr>().unwrap().to_string(), "::1/128");
        assert_eq!(
            "0.0.0.0/0".parse::<Cidr>().unwrap().to_string(),
            "0.0.0.0/0"
        );
        for bad in ["10.0.0.0/33", "::/129", "x/8", "10.0.0.0/+8", "10.0.0.0/"] {
            assert!(bad.parse::<Cidr>().is_err(), "{bad}");
        }
        let net: Cidr = "127.0.0.0/8".parse().unwrap();
        assert!(net.contains("127.0.0.6".parse().unwrap()));
        assert!(!net.contains("::1".parse().unwrap()));
    }

    #[test]
    fn authority_accepts_hosts_and_brackets_ipv6() {
        assert_eq!(
            "svc.ns:8080".parse::<Authority>().unwrap().as_str(),
            "svc.ns:8080"
        );
        assert_eq!(
            "[::1]:80".parse::<Authority>().unwrap().as_str(),
            "[::1]:80"
        );
        assert_eq!(
            "10.0.0.1".parse::<Authority>().unwrap().as_str(),
            "10.0.0.1"
        );
        for bad in ["::1", "a:0", "a:99999", "a b:80", "[x]:1", "a:"] {
            assert!(bad.parse::<Authority>().is_err(), "{bad}");
        }
    }
}
