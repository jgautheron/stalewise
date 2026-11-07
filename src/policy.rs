//! The `CachePolicy` resource, its runtime validation and most-specific-prefix resolution.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{CustomResource, CustomResourceExt, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// API group of every stalewise resource.
pub const GROUP: &str = "stalewise.dev";
/// Longest accepted duration override, in seconds.
pub const MAX_SECONDS: u32 = 604_800;
const MAX_ITEMS: usize = 64;
const MAX_NAME: usize = 64;
const MAX_PATH: usize = 256;
const MAX_HEADER_VALUE: usize = 256;
/// Content-Length values with 16 or more digits are always rejected by the VCL.
const MAX_OBJECT_BYTES: u64 = 999_999_999_999_999;

/// Application-owned cache settings for one Service path prefix.
#[derive(CustomResource, KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[kube(
    group = "stalewise.dev",
    version = "v1alpha1",
    kind = "CachePolicy",
    namespaced,
    status = "CachePolicyStatus",
    shortname = "cpol",
    printcolumn = r#"{"name":"Service","type":"string","jsonPath":".spec.serviceRef.name"}"#,
    printcolumn = r#"{"name":"Path","type":"string","jsonPath":".spec.pathPrefix"}"#,
    printcolumn = r#"{"name":"Accepted","type":"string","jsonPath":".status.conditions[?(@.type==\"Accepted\")].status"}"#,
    printcolumn = r#"{"name":"Attached","type":"string","jsonPath":".status.conditions[?(@.type==\"Attached\")].status"}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CachePolicySpec {
    pub service_ref: ServiceRef,
    #[schemars(length(min = 1, max = 256), pattern(r"^/[A-Za-z0-9/._~%-]*$"))]
    pub path_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overrides: Option<Overrides>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64))]
    pub bypass: Option<Vec<BypassRule>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attach: Option<Attach>,
}

/// A Service in the policy's own namespace.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceRef {
    #[schemars(length(min = 1, max = 63), pattern(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"))]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<IntOrString>,
}

/// Fields that replace header-derived settings; absent fields keep header semantics.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Overrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(max = 604800))]
    pub ttl_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(max = 604800))]
    pub grace_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(max = 604800))]
    pub keep_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 2), extend("x-kubernetes-list-type" = "set"))]
    pub cacheable_methods: Option<Vec<ReadMethod>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), inner(range(min = 200, max = 499)), extend("x-kubernetes-list-type" = "set"))]
    pub cacheable_status_codes: Option<Vec<u16>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 999999999999999u64))]
    pub max_object_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyAdditions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookies: Option<Cookies>,
}

/// GET or HEAD; v0.2 never caches body-bearing methods.
#[derive(
    JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub enum ReadMethod {
    #[serde(rename = "GET")]
    Get,
    #[serde(rename = "HEAD")]
    Head,
}

impl ReadMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
        }
    }
}

/// Extra cache-key inputs.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyAdditions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), inner(length(max = 64), pattern(r"^[A-Za-z][A-Za-z0-9-]*$")), extend("x-kubernetes-list-type" = "set"))]
    pub headers: Option<Vec<String>>,
    /// An empty list removes every query parameter from the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64), inner(length(min = 1, max = 64), pattern(r"^[A-Za-z0-9_.-]+$")), extend("x-kubernetes-list-type" = "set"))]
    pub query_allow_list: Option<Vec<String>>,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CookieMode {
    Pass,
    StripListed,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[x_kube(validation = Rule::new("self.mode == 'pass' || has(self.names)").message("stripListed needs names"))]
pub struct Cookies {
    pub mode: CookieMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), inner(length(min = 1, max = 64), pattern(r"^[A-Za-z0-9_.-]+$")), extend("x-kubernetes-list-type" = "set"))]
    pub names: Option<Vec<String>>,
}

/// Exactly one of `pathPrefix` or `header`; a match always passes to the origin.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[x_kube(validation = Rule::new("has(self.pathPrefix) != has(self.header)").message("set exactly one of pathPrefix or header"))]
pub struct BypassRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256), pattern(r"^/[A-Za-z0-9/._~%-]*$"))]
    pub path_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<HeaderMatch>,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeaderMatch {
    #[schemars(length(min = 1, max = 64), pattern(r"^[A-Za-z][A-Za-z0-9-]*$"))]
    pub name: String,
    #[schemars(length(max = 256))]
    pub exact: String,
}

/// A request to validate existing routes; never authority to change them.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[x_kube(validation = Rule::new("!has(self.loopProtection) || self.loopProtection != 'directService' || has(self.directServiceRef)").message("directService needs directServiceRef"))]
pub struct Attach {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<AttachMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 2), extend("x-kubernetes-list-type" = "set"))]
    pub methods: Option<Vec<ReadMethod>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), inner(length(min = 1, max = 256), pattern(r"^/[A-Za-z0-9/._~%-]*$")), extend("x-kubernetes-list-type" = "set"))]
    pub path_prefixes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_protection: Option<LoopProtection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_service_ref: Option<ServiceRef>,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AttachMode {
    Route,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum LoopProtection {
    #[default]
    SourceLabels,
    DirectService,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CachePolicyStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<Condition>,
    /// Generation whose settings the active VCL currently enforces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<i64>,
}

/// The CRD manifest shipped in the chart's `crds/` directory.
pub fn crd_yaml() -> Result<String, serde_norway::Error> {
    serde_norway::to_string(&CachePolicy::crd())
}

/// Why a policy cannot be applied; the value is the status condition reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidReason {
    Conflict,
    TooManyPolicies,
    UnknownService,
    InvalidOverride,
}

impl InvalidReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conflict => "Conflict",
            Self::TooManyPolicies => "TooManyPolicies",
            Self::UnknownService => "UnknownService",
            Self::InvalidOverride => "InvalidOverride",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.as_str())]
pub struct Rejection {
    pub reason: InvalidReason,
    pub message: String,
}

fn reject(reason: InvalidReason, message: impl Into<String>) -> Rejection {
    Rejection {
        reason,
        message: message.into(),
    }
}

fn bad(message: impl Into<String>) -> Rejection {
    reject(InvalidReason::InvalidOverride, message)
}

/// The Service a cache fronts, as policies must reference it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnService {
    pub name: String,
    pub port_name: Option<String>,
    pub port_number: Option<u16>,
}

/// A header the cache key or a bypass rule reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeaderRule {
    pub name: String,
    pub exact: String,
}

/// A validated policy in template-ready form; every string is safe to embed in VCL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RenderPolicy {
    /// One-based position in specificity order; filled in by [`resolve`].
    pub id: usize,
    pub name: String,
    pub generation: i64,
    pub path_prefix: String,
    /// Anchored URL regex that matches the prefix on segment boundaries.
    pub url_regex: String,
    pub ttl_seconds: Option<u32>,
    pub grace_seconds: Option<u32>,
    pub keep_seconds: Option<u32>,
    pub methods: Vec<&'static str>,
    pub status_codes: Vec<u16>,
    pub max_object_bytes: Option<u64>,
    pub key_headers: Vec<String>,
    /// Regex alternation of retained query names; `None` keeps the whole query.
    pub query_allow: Option<Vec<String>>,
    pub strip_cookies: Vec<String>,
    pub bypass_paths: Vec<String>,
    pub bypass_headers: Vec<HeaderRule>,
}

impl RenderPolicy {
    fn specificity(&self) -> usize {
        self.path_prefix.len()
    }
}

/// Validates every field the schema may not have enforced; never trusts admission.
pub fn validate(
    name: &str,
    generation: i64,
    spec: &CachePolicySpec,
    own: &OwnService,
) -> Result<RenderPolicy, Rejection> {
    check_service(&spec.service_ref, own)?;
    let path_prefix = normalize_prefix(&spec.path_prefix)?;
    let overrides = spec.overrides.clone().unwrap_or_default();
    let key = overrides.key.clone().unwrap_or_default();
    let (bypass_paths, bypass_headers) = bypass(spec.bypass.as_deref())?;
    if let Some(attach) = &spec.attach {
        attach_request(attach)?;
    }
    Ok(RenderPolicy {
        id: 0,
        name: name.to_owned(),
        generation,
        url_regex: prefix_regex(&path_prefix),
        path_prefix,
        ttl_seconds: seconds("ttlSeconds", overrides.ttl_seconds)?,
        grace_seconds: seconds("graceSeconds", overrides.grace_seconds)?,
        keep_seconds: seconds("keepSeconds", overrides.keep_seconds)?,
        methods: methods("cacheableMethods", overrides.cacheable_methods.as_deref())?,
        status_codes: status_codes(overrides.cacheable_status_codes.as_deref())?,
        max_object_bytes: object_bytes(overrides.max_object_bytes)?,
        key_headers: names("key.headers", key.headers.as_deref(), header_name)?.unwrap_or_default(),
        query_allow: names("key.queryAllowList", key.query_allow_list.as_deref(), token)?
            .map(|v| v.iter().map(|n| regex_escape(n)).collect()),
        strip_cookies: cookies(overrides.cookies.as_ref())?,
        bypass_paths,
        bypass_headers,
    })
}

fn check_service(service: &ServiceRef, own: &OwnService) -> Result<(), Rejection> {
    if service.name != own.name {
        return Err(reject(
            InvalidReason::UnknownService,
            format!(
                "serviceRef {:?} is not the cached Service {:?}",
                service.name, own.name
            ),
        ));
    }
    let port_ok = match &service.port {
        None => true,
        Some(IntOrString::String(name)) => own.port_name.as_deref().is_none_or(|own| own == name),
        Some(IntOrString::Int(number)) => {
            own.port_number.is_none_or(|own| i32::from(own) == *number)
        }
    };
    if port_ok {
        Ok(())
    } else {
        Err(reject(
            InvalidReason::UnknownService,
            "serviceRef.port is not the cached port",
        ))
    }
}

/// Strips a trailing slash so `/api` and `/api/` are the same specificity.
pub fn normalize_prefix(raw: &str) -> Result<String, Rejection> {
    let ok = raw.starts_with('/')
        && raw.len() <= MAX_PATH
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._~%-".contains(&b));
    if !ok {
        return Err(bad(format!(
            "path prefix {raw:?} must start with / and use only [A-Za-z0-9/._~%-]"
        )));
    }
    let trimmed = raw.trim_end_matches('/');
    Ok(if trimmed.is_empty() { "/" } else { trimmed }.to_owned())
}

/// `/api` matches `/api`, `/api/x` and `/api?q`, never `/apix`.
pub fn prefix_regex(prefix: &str) -> String {
    if prefix == "/" {
        return "^/".to_owned();
    }
    format!("^{}(/|\\?|$)", regex_escape(prefix))
}

/// Validated inputs only contain `.` among regex metacharacters.
fn regex_escape(raw: &str) -> String {
    raw.replace('.', "\\.")
}

fn seconds(field: &str, value: Option<u32>) -> Result<Option<u32>, Rejection> {
    match value {
        Some(v) if v > MAX_SECONDS => Err(bad(format!("{field} exceeds {MAX_SECONDS}"))),
        other => Ok(other),
    }
}

fn object_bytes(value: Option<u64>) -> Result<Option<u64>, Rejection> {
    match value {
        Some(v) if v == 0 || v > MAX_OBJECT_BYTES => {
            Err(bad("maxObjectBytes must be positive and below 10^15"))
        }
        other => Ok(other),
    }
}

/// Final responses Varnish can store whole; 206, 304 and errors from 500 up are excluded.
fn status_codes(raw: Option<&[u16]>) -> Result<Vec<u16>, Rejection> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    bounded("cacheableStatusCodes", raw)?;
    let invalid = raw
        .iter()
        .find(|c| !(200..500).contains(*c) || **c == 206 || **c == 304);
    if let Some(code) = invalid {
        return Err(bad(format!("status {code} cannot be made cacheable")));
    }
    Ok(raw.to_vec())
}

fn methods(field: &str, raw: Option<&[ReadMethod]>) -> Result<Vec<&'static str>, Rejection> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    bounded(field, raw)?;
    Ok(raw.iter().map(|m| m.as_str()).collect())
}

/// Nonempty, at most 64 items, no duplicates.
fn bounded<T: PartialEq>(field: &str, items: &[T]) -> Result<(), Rejection> {
    let duplicate = items
        .iter()
        .enumerate()
        .any(|(i, a)| items.iter().skip(i + 1).any(|b| a == b));
    if items.is_empty() || items.len() > MAX_ITEMS || duplicate {
        return Err(bad(format!("{field} must hold 1-{MAX_ITEMS} unique items")));
    }
    Ok(())
}

fn names(
    field: &str,
    raw: Option<&[String]>,
    valid: fn(&str) -> bool,
) -> Result<Option<Vec<String>>, Rejection> {
    let Some(raw) = raw else { return Ok(None) };
    if !raw.is_empty() {
        bounded(field, raw)?;
    }
    if let Some(name) = raw.iter().find(|n| !valid(n)) {
        return Err(bad(format!("{field}: {name:?} is not a valid name")));
    }
    Ok(Some(raw.to_vec()))
}

fn header_name(raw: &str) -> bool {
    raw.len() <= MAX_NAME
        && raw.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
        && raw.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// Query and cookie names: a conservative subset of the protocol token syntax.
fn token(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= MAX_NAME
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

fn cookies(raw: Option<&Cookies>) -> Result<Vec<String>, Rejection> {
    let Some(cookies) = raw else {
        return Ok(Vec::new());
    };
    if cookies.mode == CookieMode::Pass {
        return Ok(Vec::new());
    }
    let listed = names("cookies.names", cookies.names.as_deref(), token)?.unwrap_or_default();
    if listed.is_empty() {
        return Err(bad("cookies.mode stripListed needs names"));
    }
    Ok(listed.iter().map(|n| regex_escape(n)).collect())
}

fn bypass(raw: Option<&[BypassRule]>) -> Result<(Vec<String>, Vec<HeaderRule>), Rejection> {
    let Some(rules) = raw else {
        return Ok((Vec::new(), Vec::new()));
    };
    bounded("bypass", rules)?;
    let mut paths = Vec::new();
    let mut headers = Vec::new();
    for rule in rules {
        match (&rule.path_prefix, &rule.header) {
            (Some(path), None) => paths.push(prefix_regex(&normalize_prefix(path)?)),
            (None, Some(header)) => headers.push(header_rule(header)?),
            _ => {
                return Err(bad(
                    "each bypass rule sets exactly one of pathPrefix or header",
                ));
            }
        }
    }
    Ok((paths, headers))
}

fn header_rule(header: &HeaderMatch) -> Result<HeaderRule, Rejection> {
    let value_ok = header.exact.len() <= MAX_HEADER_VALUE
        && header
            .exact
            .bytes()
            .all(|b| (b' '..=b'~').contains(&b) && b != b'"' && b != b'\\');
    if !header_name(&header.name) || !value_ok {
        return Err(bad(format!(
            "bypass header {:?} needs a token name and a printable value without quotes or backslashes",
            header.name
        )));
    }
    Ok(HeaderRule {
        name: header.name.clone(),
        exact: header.exact.clone(),
    })
}

/// Methods, prefixes and loop protection a route validation request asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachRequest {
    pub methods: Vec<&'static str>,
    pub path_prefixes: Vec<String>,
    pub loop_protection: LoopProtection,
    pub direct: Option<String>,
}

/// Fills documented defaults: GET/HEAD, every path, source-label loop protection.
pub fn attach_request(attach: &Attach) -> Result<AttachRequest, Rejection> {
    let mut methods = methods("attach.methods", attach.methods.as_deref())?;
    if methods.is_empty() {
        methods = vec!["GET", "HEAD"];
    }
    let path_prefixes = match attach.path_prefixes.as_deref() {
        None => vec!["/".to_owned()],
        Some(raw) => {
            bounded("attach.pathPrefixes", raw)?;
            raw.iter()
                .map(|p| normalize_prefix(p))
                .collect::<Result<_, _>>()?
        }
    };
    let loop_protection = attach.loop_protection.unwrap_or_default();
    let direct = attach.direct_service_ref.as_ref().map(|r| r.name.clone());
    if loop_protection == LoopProtection::DirectService && direct.is_none() {
        return Err(bad(
            "attach.loopProtection directService needs directServiceRef",
        ));
    }
    Ok(AttachRequest {
        methods,
        path_prefixes,
        loop_protection,
        direct,
    })
}

/// One policy as read from the API, already validated.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub name: String,
    pub generation: i64,
    pub outcome: Result<RenderPolicy, Rejection>,
}

/// Per-policy decision for the current generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Valid,
    Invalid(Rejection),
}

/// Policies to render, most specific first, and a verdict for every candidate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolution {
    pub policies: Vec<RenderPolicy>,
    pub verdicts: BTreeMap<String, Verdict>,
}

/// Picks one spec per policy (an invalid generation keeps its `accepted` spec) and drops
/// every policy sharing a prefix with another, regardless of age.
pub fn resolve(candidates: &[Candidate], accepted: &BTreeMap<String, RenderPolicy>) -> Resolution {
    let mut verdicts = BTreeMap::new();
    let mut effective: Vec<RenderPolicy> = Vec::new();
    for candidate in candidates {
        let (verdict, policy) = match &candidate.outcome {
            Ok(policy) => (Verdict::Valid, Some(policy.clone())),
            Err(rejection) => (
                Verdict::Invalid(rejection.clone()),
                accepted.get(&candidate.name).cloned(),
            ),
        };
        verdicts.insert(candidate.name.clone(), verdict);
        effective.extend(policy);
    }
    let mut by_prefix: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for policy in &effective {
        by_prefix
            .entry(policy.path_prefix.clone())
            .or_default()
            .push(policy.name.clone());
    }
    let conflicted: BTreeMap<&String, &Vec<String>> = by_prefix
        .iter()
        .filter(|(_, names)| names.len() > 1)
        .collect();
    for (prefix, names) in &conflicted {
        for name in names.iter() {
            let message = conflict_message(prefix, name, names);
            verdicts.insert(
                name.clone(),
                Verdict::Invalid(reject(InvalidReason::Conflict, message)),
            );
        }
    }
    effective.retain(|p| !conflicted.contains_key(&p.path_prefix));
    effective.sort_by(|a, b| {
        b.specificity()
            .cmp(&a.specificity())
            .then_with(|| a.name.cmp(&b.name))
    });
    for (index, policy) in effective.iter_mut().enumerate() {
        policy.id = index + 1;
    }
    Resolution {
        policies: effective,
        verdicts,
    }
}

/// Splits policies at `max`, oldest first with ties broken by `age`'s later fields; callers
/// validate only the admitted ones.
pub fn admit<T, K: Ord>(mut items: Vec<T>, max: usize, age: impl Fn(&T) -> K) -> (Vec<T>, Vec<T>) {
    items.sort_by_cached_key(age);
    let excess = items.split_off(max.min(items.len()));
    (items, excess)
}

/// The verdict for a policy past the `--max-cache-policies` budget.
pub fn over_budget(max: usize) -> Verdict {
    Verdict::Invalid(reject(
        InvalidReason::TooManyPolicies,
        format!("more than {max} CachePolicies reference this Service; older policies win"),
    ))
}

/// Rivals named in one conflict diagnostic; the rest are only counted.
const CONFLICT_NAMES_SHOWN: usize = 3;

/// Names a few rivals plus a count, so diagnostics stay linear in the number of policies.
fn conflict_message(prefix: &str, name: &str, claimants: &[String]) -> String {
    let shown: Vec<&str> = claimants
        .iter()
        .filter(|n| *n != name)
        .take(CONFLICT_NAMES_SHOWN)
        .map(String::as_str)
        .collect();
    let hidden = claimants.len().saturating_sub(1 + shown.len());
    let mut message = format!(
        "path prefix {prefix:?} is also claimed by {}",
        shown.join(", ")
    );
    if hidden > 0 {
        message.push_str(&format!(" and {hidden} more"));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own() -> OwnService {
        OwnService {
            name: "catalog".into(),
            port_name: Some("http".into()),
            port_number: None,
        }
    }

    fn spec(yaml: &str) -> CachePolicySpec {
        serde_norway::from_str(yaml).unwrap()
    }

    fn valid(name: &str, prefix: &str) -> Candidate {
        let s = spec(&format!(
            "serviceRef: {{name: catalog}}\npathPrefix: {prefix}\n"
        ));
        Candidate {
            name: name.into(),
            generation: 1,
            outcome: validate(name, 1, &s, &own()),
        }
    }

    #[test]
    fn full_reference_spec_validates_into_safe_render_values() {
        let s = spec(
            r#"
serviceRef: {name: catalog, port: http}
pathPrefix: /v1.2/
overrides:
  ttlSeconds: 0
  graceSeconds: 30
  cacheableMethods: [GET]
  cacheableStatusCodes: [200, 404]
  key: {headers: [X-Application-Version], queryAllowList: [page, a.b]}
  cookies: {mode: stripListed, names: [analytics]}
bypass:
  - pathPrefix: /account
  - header: {name: X-Preview, exact: "true"}
"#,
        );
        let p = validate("catalog", 3, &s, &own()).unwrap();
        assert_eq!(p.path_prefix, "/v1.2");
        assert_eq!(p.url_regex, "^/v1\\.2(/|\\?|$)");
        assert_eq!(p.ttl_seconds, Some(0), "explicit zero is kept, not dropped");
        assert_eq!(p.keep_seconds, None);
        assert_eq!(p.query_allow.unwrap(), ["page", "a\\.b"]);
        assert_eq!(p.strip_cookies, ["analytics"]);
        assert_eq!(p.bypass_paths, ["^/account(/|\\?|$)"]);
    }

    #[test]
    fn unknown_fields_are_rejected_at_parse() {
        let parsed: Result<CachePolicySpec, _> =
            serde_norway::from_str("serviceRef: {name: catalog}\npathPrefix: /\nttl: 3\n");
        assert!(parsed.is_err());
    }

    #[test]
    fn invalid_overrides_are_rejected_with_a_reason() {
        for (yaml, reason) in [
            (
                "serviceRef: {name: other}\npathPrefix: /",
                InvalidReason::UnknownService,
            ),
            (
                "serviceRef: {name: catalog, port: grpc}\npathPrefix: /",
                InvalidReason::UnknownService,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: api",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /a?b",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {ttlSeconds: 604801}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {cacheableStatusCodes: [206]}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {cacheableStatusCodes: [503]}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {cacheableMethods: [GET, GET]}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {maxObjectBytes: 0}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {key: {headers: [\"X Bad\"]}}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {key: {queryAllowList: [\"a|b\"]}}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {cookies: {mode: stripListed}}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\nbypass: [{}]",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\nattach: {loopProtection: directService}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\nattach: {pathPrefixes: [x]}",
                InvalidReason::InvalidOverride,
            ),
            (
                "serviceRef: {name: catalog}\npathPrefix: /\nbypass: [{header: {name: X-A, exact: 'a\"}'}}]",
                InvalidReason::InvalidOverride,
            ),
        ] {
            let err = validate("p", 1, &spec(yaml), &own()).unwrap_err();
            assert_eq!(err.reason, reason, "{yaml}");
        }
    }

    #[test]
    fn empty_query_allow_list_differs_from_absent() {
        let empty = spec(
            "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {key: {queryAllowList: []}}",
        );
        assert_eq!(
            validate("p", 1, &empty, &own()).unwrap().query_allow,
            Some(vec![])
        );
        let absent = spec("serviceRef: {name: catalog}\npathPrefix: /");
        assert_eq!(validate("p", 1, &absent, &own()).unwrap().query_allow, None);
    }

    #[test]
    fn longest_prefix_sorts_first_and_ids_follow_order() {
        let r = resolve(
            &[
                valid("root", "/"),
                valid("api", "/api/"),
                valid("deep", "/api/v1"),
            ],
            &BTreeMap::new(),
        );
        let order: Vec<(&str, usize)> =
            r.policies.iter().map(|p| (p.name.as_str(), p.id)).collect();
        assert_eq!(order, [("deep", 1), ("api", 2), ("root", 3)]);
        assert!(r.verdicts.values().all(|v| *v == Verdict::Valid));
    }

    #[test]
    fn equal_prefixes_invalidate_every_competitor() {
        let r = resolve(
            &[valid("a", "/api"), valid("b", "/api/"), valid("c", "/")],
            &BTreeMap::new(),
        );
        assert_eq!(r.policies.len(), 1);
        for name in ["a", "b"] {
            assert!(
                matches!(&r.verdicts[name], Verdict::Invalid(x) if x.reason == InvalidReason::Conflict)
            );
        }
    }

    #[test]
    fn five_thousand_conflicting_policies_get_bounded_diagnostics_quickly() {
        const COUNT: usize = 5_000;
        let name = |i: usize| format!("{}-{i:05}", "p".repeat(200));
        let candidates: Vec<Candidate> = (0..COUNT).map(|i| valid(&name(i), "/api")).collect();
        let started = std::time::Instant::now();
        let r = resolve(&candidates, &BTreeMap::new());
        let elapsed = started.elapsed();
        let bytes: usize = r
            .verdicts
            .values()
            .map(|v| match v {
                Verdict::Invalid(x) => {
                    assert_eq!(x.reason, InvalidReason::Conflict);
                    assert!(x.message.len() <= 1024, "{} bytes", x.message.len());
                    x.message.len()
                }
                Verdict::Valid => panic!("a conflicting policy stayed valid"),
            })
            .sum();
        assert!(bytes <= COUNT * 1024, "{bytes} diagnostic bytes");
        assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
        assert!(r.policies.is_empty());
        let sample = &r.verdicts[&name(0)];
        assert!(
            matches!(sample, Verdict::Invalid(x) if x.message.contains("and 4996 more")),
            "{sample:?}"
        );
    }

    #[test]
    fn policy_budget_keeps_the_oldest_and_never_validates_the_excess() {
        let items: Vec<(u32, String)> = (0..5_000u32).rev().map(|i| (i, format!("p{i}"))).collect();
        let started = std::time::Instant::now();
        let (kept, excess) = admit(items, 256, |(age, name)| (*age, name.clone()));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(kept.len(), 256);
        assert_eq!(excess.len(), 4_744);
        assert!(kept.iter().all(|(age, _)| *age < 256), "the oldest win");
        let (all, none) = admit(vec![(1, "a")], 256, |p| *p);
        assert_eq!((all.len(), none.len()), (1, 0));
        assert!(
            matches!(over_budget(256), Verdict::Invalid(r) if r.reason == InvalidReason::TooManyPolicies)
        );
    }

    #[test]
    fn invalid_update_keeps_last_accepted_spec_in_force() {
        let good = valid("a", "/api").outcome.unwrap();
        let broken = Candidate {
            name: "a".into(),
            generation: 2,
            outcome: Err(bad("nope")),
        };
        let accepted = BTreeMap::from([("a".to_owned(), good.clone())]);
        let r = resolve(std::slice::from_ref(&broken), &accepted);
        assert_eq!(r.policies[0].generation, good.generation);
        assert!(matches!(r.verdicts["a"], Verdict::Invalid(_)));
        assert!(resolve(&[broken], &BTreeMap::new()).policies.is_empty());
    }

    #[test]
    fn committed_crd_matches_generated() {
        let committed = include_str!("../charts/stalewise/crds/cachepolicies.stalewise.dev.yaml");
        assert_eq!(
            committed,
            crd_yaml().unwrap(),
            "run `cargo run -- crd > charts/stalewise/crds/cachepolicies.stalewise.dev.yaml`"
        );
    }
}
