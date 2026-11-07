//! Read-only validation of mesh route attachment against VirtualService objects.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::policy::LoopProtection;

/// Header that sends a request straight to the origin.
pub const BYPASS_HEADER: &str = "x-stalewise-bypass";

/// The identities and coverage one CachePolicy asks the routes to provide.
#[derive(Debug, Clone)]
pub struct Target<'a> {
    pub namespace: &'a str,
    /// Service whose VirtualService carries the cache rules.
    pub routed: &'a str,
    /// DNS name of the cache's traffic Service.
    pub cache_host: &'a str,
    pub source_labels: &'a BTreeMap<String, String>,
    pub methods: Vec<&'static str>,
    /// Normalised prefixes; `/` alone means every path.
    pub path_prefixes: Vec<String>,
    pub loop_protection: LoopProtection,
    pub direct: Option<&'a str>,
}

/// Status reason for the `Attached` condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Attached,
    NoVirtualService,
    MultipleVirtualServices,
    MissingSourceBypass,
    MissingHeaderBypass,
    MissingCacheRoute,
    UnrestrictedCacheRoute,
    Loop,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Attached => "Attached",
            Self::NoVirtualService => "NoVirtualService",
            Self::MultipleVirtualServices => "MultipleVirtualServices",
            Self::MissingSourceBypass => "MissingSourceBypass",
            Self::MissingHeaderBypass => "MissingHeaderBypass",
            Self::MissingCacheRoute => "MissingCacheRoute",
            Self::UnrestrictedCacheRoute => "UnrestrictedCacheRoute",
            Self::Loop => "Loop",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub reason: Reason,
    pub message: String,
}

impl Outcome {
    pub fn attached(&self) -> bool {
        self.reason == Reason::Attached
    }
}

fn fail(reason: Reason, message: impl Into<String>) -> Outcome {
    Outcome {
        reason,
        message: message.into(),
    }
}

/// A synthetic request evaluated against the rules in order.
#[derive(Debug, Clone, Copy)]
struct Probe<'a> {
    method: &'a str,
    path: &'a str,
    bypass_header: bool,
    from_cache: bool,
}

/// Checks the one VirtualService for `target.routed` without contacting the data plane.
pub fn validate(target: &Target<'_>, virtual_services: &[Value]) -> Outcome {
    let routed = (target.routed.to_owned(), target.namespace.to_owned());
    let owning: Vec<&Value> = virtual_services
        .iter()
        .filter(|vs| applies_to_mesh(vs) && hosts(vs, target.namespace).contains(&routed))
        .collect();
    let rules = match owning.as_slice() {
        [] => {
            return fail(
                Reason::NoVirtualService,
                format!("no mesh VirtualService routes {}", target.routed),
            );
        }
        [one] => http_rules(one),
        _ => {
            return fail(
                Reason::MultipleVirtualServices,
                format!("{} VirtualServices claim {}", owning.len(), target.routed),
            );
        }
    };
    let checks = [loop_check, header_check, cache_check, restriction_check];
    checks
        .iter()
        .find_map(|check| check(target, &rules).err())
        .unwrap_or_else(|| {
            fail(
                Reason::Attached,
                "routes send reads to the cache and bypasses to the origin",
            )
        })
}

/// One `(path, method)` pair per attach prefix and method; a prefix probes its first child path.
fn probes(target: &Target<'_>) -> Vec<(String, &'static str)> {
    target
        .path_prefixes
        .iter()
        .flat_map(|p| {
            let path = if p == "/" {
                "/".to_owned()
            } else {
                format!("{p}/")
            };
            target.methods.iter().map(move |m| (path.clone(), *m))
        })
        .collect()
}

fn first<'r>(rules: &'r [&'r Value], probe: Probe<'_>, target: &Target<'_>) -> Option<&'r Value> {
    rules
        .iter()
        .copied()
        .find(|rule| rule_matches(rule, probe, target))
}

fn loop_check(target: &Target<'_>, rules: &[&Value]) -> Result<(), Outcome> {
    let cache = service_key(target.cache_host, target.namespace);
    if cache
        .as_ref()
        .is_some_and(|(name, ns)| name == target.routed && ns == target.namespace)
    {
        return Err(fail(
            Reason::Loop,
            "cache Service equals the routed Service",
        ));
    }
    if target.loop_protection == LoopProtection::DirectService {
        return direct_check(target, cache);
    }
    for (path, method) in probes(target) {
        let probe = Probe {
            method,
            path: &path,
            bypass_header: false,
            from_cache: true,
        };
        let Some(rule) = first(rules, probe, target) else {
            return Err(fail(
                Reason::MissingSourceBypass,
                format!("no rule matches cache-sourced {method} {path}"),
            ));
        };
        if routes_to(rule, target, |d| Some(d) == cache.as_ref()) {
            let reason = if rules.iter().any(|r| has_source_match(r)) {
                Reason::Loop
            } else {
                Reason::MissingSourceBypass
            };
            return Err(fail(
                reason,
                format!("cache-sourced {method} {path} routes back to the cache"),
            ));
        }
    }
    Ok(())
}

fn direct_check(target: &Target<'_>, cache: Option<(String, String)>) -> Result<(), Outcome> {
    let Some(direct) = target.direct else {
        return Err(fail(Reason::Loop, "directService needs directServiceRef"));
    };
    let is_cache = cache.is_some_and(|(name, ns)| name == direct && ns == target.namespace);
    if direct == target.routed || is_cache {
        return Err(fail(
            Reason::Loop,
            "direct Service must differ from the routed and cache Services",
        ));
    }
    Ok(())
}

fn header_check(target: &Target<'_>, rules: &[&Value]) -> Result<(), Outcome> {
    let cache = service_key(target.cache_host, target.namespace);
    for (path, method) in probes(target) {
        let probe = Probe {
            method,
            path: &path,
            bypass_header: true,
            from_cache: false,
        };
        let bypassed = first(rules, probe, target)
            .is_some_and(|r| !routes_to(r, target, |d| Some(d) == cache.as_ref()));
        if !bypassed {
            return Err(fail(
                Reason::MissingHeaderBypass,
                format!("{method} {path} with {BYPASS_HEADER}: true still reaches the cache"),
            ));
        }
    }
    Ok(())
}

fn cache_check(target: &Target<'_>, rules: &[&Value]) -> Result<(), Outcome> {
    let cache = service_key(target.cache_host, target.namespace);
    for (path, method) in probes(target) {
        let probe = Probe {
            method,
            path: &path,
            bypass_header: false,
            from_cache: false,
        };
        let cached =
            first(rules, probe, target).is_some_and(|r| only_routes_to(r, target, cache.as_ref()));
        if !cached {
            return Err(fail(
                Reason::MissingCacheRoute,
                format!("{method} {path} is not routed wholly to the cache"),
            ));
        }
    }
    Ok(())
}

/// Restricted prefixes must not sit beside a method-only cache match.
fn restriction_check(target: &Target<'_>, rules: &[&Value]) -> Result<(), Outcome> {
    if target.path_prefixes.iter().any(|p| p == "/") {
        return Ok(());
    }
    let cache = service_key(target.cache_host, target.namespace);
    let unrestricted = rules.iter().any(|rule| {
        routes_to(rule, target, |d| Some(d) == cache.as_ref())
            && matches(rule)
                .is_none_or(|ms| ms.is_empty() || ms.iter().any(|m| m.get("uri").is_none()))
    });
    if unrestricted {
        return Err(fail(
            Reason::UnrestrictedCacheRoute,
            "a cache rule has no uri condition although pathPrefixes are restricted",
        ));
    }
    Ok(())
}

fn applies_to_mesh(vs: &Value) -> bool {
    match vs.pointer("/spec/gateways").and_then(Value::as_array) {
        None => true,
        Some(gateways) => gateways.iter().any(|g| g.as_str() == Some("mesh")),
    }
}

fn hosts(vs: &Value, namespace: &str) -> Vec<(String, String)> {
    let vs_namespace = vs
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .unwrap_or(namespace);
    vs.pointer("/spec/hosts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(|h| service_key(h, vs_namespace))
        .collect()
}

fn http_rules(vs: &Value) -> Vec<&Value> {
    vs.pointer("/spec/http")
        .and_then(Value::as_array)
        .map(|rules| rules.iter().collect())
        .unwrap_or_default()
}

/// `(service, namespace)` for in-cluster Service names; `None` for external hosts.
pub fn service_key(host: &str, namespace: &str) -> Option<(String, String)> {
    let host = host.to_ascii_lowercase();
    let labels: Vec<&str> = host.trim_end_matches('.').split('.').collect();
    match labels.as_slice() {
        [name] => Some(((*name).to_owned(), namespace.to_owned())),
        [name, ns] => Some(((*name).to_owned(), (*ns).to_owned())),
        [name, ns, "svc", ..] => Some(((*name).to_owned(), (*ns).to_owned())),
        _ => None,
    }
}

fn matches(rule: &Value) -> Option<&Vec<Value>> {
    rule.get("match").and_then(Value::as_array)
}

fn has_source_match(rule: &Value) -> bool {
    matches(rule).is_some_and(|ms| ms.iter().any(|m| m.get("sourceLabels").is_some()))
}

fn rule_matches(rule: &Value, probe: Probe<'_>, target: &Target<'_>) -> bool {
    match matches(rule) {
        None => true,
        Some(ms) if ms.is_empty() => true,
        Some(ms) => ms.iter().any(|m| match_object(m, probe, target)),
    }
}

/// Every condition must hold; conditions this validator cannot evaluate never match.
fn match_object(m: &Value, probe: Probe<'_>, target: &Target<'_>) -> bool {
    let Some(fields) = m.as_object() else {
        return false;
    };
    fields.iter().all(|(key, value)| match key.as_str() {
        "name" => true,
        "method" => string_match(value, probe.method),
        "uri" => string_match(value, probe.path),
        "headers" => headers_match(value, probe),
        "sourceLabels" => probe.from_cache && labels_match(value, target.source_labels),
        "sourceNamespace" => probe.from_cache && value.as_str() == Some(target.namespace),
        "gateways" => value
            .as_array()
            .is_some_and(|g| g.iter().any(|g| g.as_str() == Some("mesh"))),
        _ => false,
    })
}

fn string_match(condition: &Value, actual: &str) -> bool {
    let Some(fields) = condition.as_object() else {
        return false;
    };
    fields
        .iter()
        .all(|(kind, want)| match (kind.as_str(), want.as_str()) {
            ("exact", Some(want)) => actual == want,
            ("prefix", Some(want)) => actual.starts_with(want),
            _ => false,
        })
}

fn headers_match(headers: &Value, probe: Probe<'_>) -> bool {
    let Some(fields) = headers.as_object() else {
        return false;
    };
    fields.iter().all(|(name, condition)| {
        name.eq_ignore_ascii_case(BYPASS_HEADER)
            && probe.bypass_header
            && string_match(condition, "true")
    })
}

fn labels_match(labels: &Value, cache: &BTreeMap<String, String>) -> bool {
    let Some(fields) = labels.as_object() else {
        return false;
    };
    fields
        .iter()
        .all(|(k, v)| cache.get(k).map(String::as_str) == v.as_str())
}

fn destinations(rule: &Value, namespace: &str) -> Vec<Option<(String, String)>> {
    rule.get("route")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|r| {
            r.pointer("/destination/host")
                .and_then(Value::as_str)
                .and_then(|h| service_key(h, namespace))
        })
        .collect()
}

fn routes_to(rule: &Value, target: &Target<'_>, is: impl Fn(&(String, String)) -> bool) -> bool {
    destinations(rule, target.namespace)
        .iter()
        .flatten()
        .any(is)
}

fn only_routes_to(rule: &Value, target: &Target<'_>, cache: Option<&(String, String)>) -> bool {
    let all = destinations(rule, target.namespace);
    !all.is_empty() && all.iter().all(|d| d.as_ref() == cache)
}

#[cfg(test)]
mod tests {
    use super::*;

    const REFERENCE: &str = include_str!("../tests/fixtures/virtualservice-reference.yaml");

    fn labels() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("app.kubernetes.io/name".to_owned(), "stalewise".to_owned()),
            (
                "app.kubernetes.io/instance".to_owned(),
                "catalog-cache".to_owned(),
            ),
            ("pod-template-hash".to_owned(), "abc".to_owned()),
        ])
    }

    fn target(labels: &BTreeMap<String, String>) -> Target<'_> {
        Target {
            namespace: "apps",
            routed: "catalog",
            cache_host: "catalog-cache.apps.svc.cluster.local",
            source_labels: labels,
            methods: vec!["GET", "HEAD"],
            path_prefixes: vec!["/".into()],
            loop_protection: LoopProtection::SourceLabels,
            direct: None,
        }
    }

    fn reference() -> Value {
        serde_norway::from_str(REFERENCE).unwrap()
    }

    fn rules(vs: &mut Value) -> &mut Vec<Value> {
        vs.pointer_mut("/spec/http")
            .unwrap()
            .as_array_mut()
            .unwrap()
    }

    fn check(vs: Value) -> Reason {
        let l = labels();
        validate(&target(&l), &[vs]).reason
    }

    #[test]
    fn fixture_is_the_adr_reference_verbatim() {
        assert!(include_str!("../docs/adr/0003-traffic-attachment.md").contains(REFERENCE));
    }

    #[test]
    fn reference_virtual_service_is_attached() {
        assert_eq!(check(reference()), Reason::Attached);
    }

    #[test]
    fn no_or_competing_virtual_services_are_reported() {
        let l = labels();
        assert_eq!(validate(&target(&l), &[]).reason, Reason::NoVirtualService);
        assert_eq!(
            validate(&target(&l), &[reference(), reference()]).reason,
            Reason::MultipleVirtualServices
        );
    }

    #[test]
    fn missing_source_bypass_is_reported() {
        let mut vs = reference();
        rules(&mut vs).remove(0);
        assert_eq!(check(vs), Reason::MissingSourceBypass);
    }

    #[test]
    fn source_bypass_after_cache_rule_is_a_loop() {
        let mut vs = reference();
        let source = rules(&mut vs).remove(0);
        rules(&mut vs).insert(2, source);
        assert_eq!(check(vs), Reason::Loop);
    }

    #[test]
    fn source_labels_the_cache_does_not_carry_leave_a_loop_open() {
        let mut vs = reference();
        let m = rules(&mut vs)[0]
            .pointer_mut("/match/0/sourceLabels")
            .unwrap();
        m["app.kubernetes.io/instance"] = "other-cache".into();
        assert_eq!(check(vs), Reason::Loop);
    }

    #[test]
    fn missing_header_bypass_is_reported() {
        let mut vs = reference();
        rules(&mut vs).remove(1);
        assert_eq!(check(vs), Reason::MissingHeaderBypass);
    }

    #[test]
    fn missing_or_partial_cache_route_is_reported() {
        let mut vs = reference();
        rules(&mut vs).remove(2);
        assert_eq!(check(vs), Reason::MissingCacheRoute);
        let mut vs = reference();
        rules(&mut vs)[2]
            .pointer_mut("/match")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .remove(1);
        assert_eq!(
            check(vs),
            Reason::MissingCacheRoute,
            "HEAD falls through to the origin"
        );
    }

    #[test]
    fn restricted_prefixes_reject_a_method_only_cache_match() {
        let l = labels();
        let mut t = target(&l);
        t.path_prefixes = vec!["/public".into()];
        assert_eq!(
            validate(&t, &[reference()]).reason,
            Reason::UnrestrictedCacheRoute
        );
        let mut vs = reference();
        for m in rules(&mut vs)[2]
            .pointer_mut("/match")
            .unwrap()
            .as_array_mut()
            .unwrap()
        {
            m["uri"] = serde_json::json!({"prefix": "/public/"});
        }
        assert_eq!(validate(&t, &[vs]).reason, Reason::Attached);
    }

    #[test]
    fn direct_service_must_differ_from_routed_and_cache() {
        let l = labels();
        let mut t = target(&l);
        t.loop_protection = LoopProtection::DirectService;
        let mut vs = reference();
        rules(&mut vs).remove(0);
        for (direct, reason) in [
            ("catalog", Reason::Loop),
            ("catalog-cache", Reason::Loop),
            ("catalog-direct", Reason::Attached),
        ] {
            t.direct = Some(direct);
            assert_eq!(
                validate(&t, std::slice::from_ref(&vs)).reason,
                reason,
                "{direct}"
            );
        }
    }

    #[test]
    fn gateway_only_virtual_service_does_not_count() {
        let mut vs = reference();
        vs.pointer_mut("/spec/gateways").unwrap()[0] = "ingress".into();
        assert_eq!(check(vs), Reason::NoVirtualService);
    }

    #[test]
    fn short_and_qualified_hosts_name_the_same_service() {
        for host in [
            "catalog",
            "catalog.apps",
            "catalog.apps.svc",
            "catalog.apps.svc.cluster.local",
        ] {
            assert_eq!(
                service_key(host, "apps"),
                Some(("catalog".into(), "apps".into())),
                "{host}"
            );
        }
        assert_eq!(service_key("example.com.au.x", "apps"), None);
    }
}
