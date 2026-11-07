//! Watches CachePolicies and VirtualServices, feeds accepted policies to the reloader and
//! reports Accepted/Invalid/Attached status. Never writes anything but CachePolicy status.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use kube::ResourceExt;
use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector::{self, Store};
use kube::runtime::watcher::{self, Event};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::attach::{self, Outcome, Target};
use crate::config::{AttachConfig, PolicyConfig};
use crate::policy::{self, CachePolicy, CachePolicyStatus, Candidate, RenderPolicy, Verdict};
use crate::reload::{Active, Policies};

const ACCEPTED: &str = "Accepted";
const INVALID: &str = "Invalid";
const ATTACHED: &str = "Attached";
const FIELD_MANAGER: &str = "stalewise";

/// Attachment knowledge for one policy at reconcile time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attachment {
    /// The policy asked for no validation, or validation is disabled.
    NotRequested,
    /// VirtualServices could not be listed yet.
    Unknown,
    Checked(Outcome),
}

/// Runs until `stop`; watch errors back off and retry, keeping the last published policies.
pub async fn run(
    client: kube::Client,
    config: PolicyConfig,
    out: watch::Sender<Option<Policies>>,
    mut active: watch::Receiver<Option<Active>>,
    stop: CancellationToken,
) {
    let api: Api<CachePolicy> = Api::namespaced(client.clone(), &config.namespace);
    let (policies, writer) = reflector::store();
    let mut policy_events = watcher::watcher(api.clone(), watcher::Config::default())
        .default_backoff()
        .reflect(writer)
        .boxed();
    let (virtual_services, vs_events) = virtual_service_watch(&client, &config);
    let mut vs_events = vs_events;
    let mut policies_ready = false;
    let mut vs_ready = false;
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            event = policy_events.next() => policies_ready |= synced(event, "CachePolicy"),
            event = vs_events.next() => vs_ready |= synced(event, "VirtualService"),
            changed = active.changed() => if changed.is_err() { return },
        }
        if !policies_ready {
            continue;
        }
        let vs = (vs_ready && config.attachment.is_some()).then(|| objects(&virtual_services));
        let current = active.borrow().as_ref().map(|a| Arc::clone(&a.policies));
        reconcile(&api, &config, &policies, vs.as_deref(), current, &out).await;
    }
}

type EventStream<K> =
    std::pin::Pin<Box<dyn Stream<Item = Result<Event<K>, watcher::Error>> + Send>>;

fn virtual_service_watch(
    client: &kube::Client,
    config: &PolicyConfig,
) -> (Store<DynamicObject>, EventStream<DynamicObject>) {
    let gvk = GroupVersionKind::gvk("networking.istio.io", "v1", "VirtualService");
    let resource = ApiResource::from_gvk_with_plural(&gvk, "virtualservices");
    let writer = reflector::store::Writer::new(resource.clone());
    let store = writer.as_reader();
    if config.attachment.is_none() {
        return (store, futures_util::stream::pending().boxed());
    }
    let api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), &config.namespace, &resource);
    let stream = watcher::watcher(api, watcher::Config::default())
        .default_backoff()
        .reflect(writer)
        .boxed();
    (store, stream)
}

/// True once a complete list has been applied to the store.
fn synced<K>(event: Option<Result<Event<K>, watcher::Error>>, kind: &'static str) -> bool {
    match event {
        Some(Ok(Event::InitDone)) => true,
        Some(Ok(_)) | None => false,
        Some(Err(err)) => {
            tracing::warn!(kind, error = %err, "watch failed; retrying with backoff");
            false
        }
    }
}

fn objects(store: &Store<DynamicObject>) -> Vec<serde_json::Value> {
    store
        .state()
        .iter()
        .filter_map(|o| serde_json::to_value(o.as_ref()).ok())
        .collect()
}

async fn reconcile(
    api: &Api<CachePolicy>,
    config: &PolicyConfig,
    store: &Store<CachePolicy>,
    virtual_services: Option<&[serde_json::Value]>,
    active: Option<Policies>,
    out: &watch::Sender<Option<Policies>>,
) {
    let owned: Vec<Arc<CachePolicy>> = store
        .state()
        .into_iter()
        .filter(|p| p.spec.service_ref.name == config.own.name)
        .collect();
    let (owned, excess) = policy::admit(owned, config.max_policies, |p| {
        (p.metadata.creation_timestamp.clone(), p.name_any())
    });
    let candidates: Vec<Candidate> = owned
        .iter()
        .map(|p| Candidate {
            name: p.name_any(),
            generation: p.metadata.generation.unwrap_or_default(),
            outcome: policy::validate(
                &p.name_any(),
                p.metadata.generation.unwrap_or_default(),
                &p.spec,
                &config.own,
            ),
        })
        .collect();
    let accepted: BTreeMap<String, RenderPolicy> = active
        .iter()
        .flat_map(|set| set.iter())
        .map(|p| (p.name.clone(), p.clone()))
        .collect();
    let resolution = policy::resolve(&candidates, &accepted);
    let next: Policies = resolution.policies.into();
    out.send_if_modified(|current| {
        let changed = current.as_deref() != Some(&*next);
        if changed {
            *current = Some(Arc::clone(&next));
        }
        changed
    });
    let now = Time(Timestamp::now());
    for p in owned {
        let name = p.name_any();
        let Some(verdict) = resolution.verdicts.get(&name) else {
            continue;
        };
        let attachment = attachment(
            &p,
            config.attachment.as_ref(),
            virtual_services,
            &config.namespace,
        );
        let status = desired_status(&p, verdict, accepted.get(&name), &attachment, &now);
        if !same_status(p.status.as_ref(), &status) {
            write_status(api, &name, status).await;
        }
    }
    reject_excess(api, excess, config.max_policies, &now).await;
}

/// Marks policies past the budget Invalid without validating or attaching them.
async fn reject_excess(
    api: &Api<CachePolicy>,
    excess: Vec<Arc<CachePolicy>>,
    max: usize,
    now: &Time,
) {
    if !excess.is_empty() {
        tracing::warn!(rejected = excess.len(), max, "CachePolicy budget exceeded");
    }
    let verdict = policy::over_budget(max);
    for p in excess {
        let status = desired_status(&p, &verdict, None, &Attachment::NotRequested, now);
        if !same_status(p.status.as_ref(), &status) {
            write_status(api, &p.name_any(), status).await;
        }
    }
}

fn attachment(
    p: &CachePolicy,
    config: Option<&AttachConfig>,
    virtual_services: Option<&[serde_json::Value]>,
    namespace: &str,
) -> Attachment {
    let (Some(config), Some(request)) = (config, p.spec.attach.as_ref()) else {
        return Attachment::NotRequested;
    };
    let Some(virtual_services) = virtual_services else {
        return Attachment::Unknown;
    };
    let request = match policy::attach_request(request) {
        Ok(request) => request,
        Err(rejection) => {
            return Attachment::Checked(Outcome {
                reason: attach::Reason::MissingCacheRoute,
                message: rejection.message,
            });
        }
    };
    let target = Target {
        namespace,
        routed: &p.spec.service_ref.name,
        cache_host: config.cache_host.as_str(),
        source_labels: &config.source_labels,
        methods: request.methods,
        path_prefixes: request.path_prefixes,
        loop_protection: request.loop_protection,
        direct: request.direct.as_deref(),
    };
    Attachment::Checked(attach::validate(&target, virtual_services))
}

async fn write_status(api: &Api<CachePolicy>, name: &str, status: CachePolicyStatus) {
    let patch = serde_json::json!({ "status": status });
    let params = PatchParams::apply(FIELD_MANAGER);
    if let Err(err) = api.patch_status(name, &params, &Patch::Merge(&patch)).await {
        tracing::warn!(policy = name, error = %err, "status update failed; retrying on the next event");
    }
}

fn condition(
    kind: &str,
    status: &str,
    reason: &str,
    message: String,
    generation: i64,
    now: &Time,
) -> Condition {
    Condition {
        type_: kind.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message,
        observed_generation: Some(generation),
        last_transition_time: now.clone(),
    }
}

/// Status the controller wants; `Accepted=True` only once the active VCL carries this generation.
pub fn desired_status(
    p: &CachePolicy,
    verdict: &Verdict,
    active: Option<&RenderPolicy>,
    attachment: &Attachment,
    now: &Time,
) -> CachePolicyStatus {
    let generation = p.metadata.generation.unwrap_or_default();
    let live = active.is_some_and(|a| a.generation == generation);
    let (accepted, invalid) = match verdict {
        Verdict::Invalid(r) => (
            condition(
                ACCEPTED,
                "False",
                r.reason.as_str(),
                r.message.clone(),
                generation,
                now,
            ),
            condition(
                INVALID,
                "True",
                r.reason.as_str(),
                r.message.clone(),
                generation,
                now,
            ),
        ),
        Verdict::Valid if live => (
            condition(
                ACCEPTED,
                "True",
                "Active",
                "the active VCL enforces this generation".into(),
                generation,
                now,
            ),
            condition(INVALID, "False", "Valid", String::new(), generation, now),
        ),
        Verdict::Valid => (
            condition(
                ACCEPTED,
                "False",
                "Pending",
                "waiting for the VCL to activate".into(),
                generation,
                now,
            ),
            condition(INVALID, "False", "Valid", String::new(), generation, now),
        ),
    };
    let mut conditions = vec![accepted, invalid];
    conditions.extend(attached_condition(attachment, generation, now));
    let previous = p
        .status
        .as_ref()
        .map(|s| s.conditions.as_slice())
        .unwrap_or_default();
    for c in &mut conditions {
        keep_transition_time(c, previous);
    }
    CachePolicyStatus {
        conditions,
        active_generation: active.map(|a| a.generation),
    }
}

fn attached_condition(attachment: &Attachment, generation: i64, now: &Time) -> Option<Condition> {
    let (status, reason, message) = match attachment {
        Attachment::NotRequested => return None,
        Attachment::Unknown => (
            "Unknown",
            "VirtualServicesUnavailable",
            "VirtualServices are not readable yet".to_owned(),
        ),
        Attachment::Checked(o) if o.attached() => ("True", o.reason.as_str(), o.message.clone()),
        Attachment::Checked(o) => ("False", o.reason.as_str(), o.message.clone()),
    };
    Some(condition(
        ATTACHED, status, reason, message, generation, now,
    ))
}

fn keep_transition_time(c: &mut Condition, previous: &[Condition]) {
    let unchanged = previous
        .iter()
        .find(|p| p.type_ == c.type_ && p.status == c.status);
    if let Some(p) = unchanged {
        c.last_transition_time = p.last_transition_time.clone();
    }
}

/// Equality ignoring nothing but timestamps already carried over.
fn same_status(current: Option<&CachePolicyStatus>, desired: &CachePolicyStatus) -> bool {
    current.is_some_and(|c| c == desired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{InvalidReason, OwnService, Rejection};

    fn policy(generation: i64) -> CachePolicy {
        let mut p = CachePolicy::new(
            "catalog",
            serde_norway::from_str("serviceRef: {name: catalog}\npathPrefix: /").unwrap(),
        );
        p.metadata.generation = Some(generation);
        p
    }

    fn rendered(generation: i64) -> RenderPolicy {
        let own = OwnService {
            name: "catalog".into(),
            port_name: None,
            port_number: None,
        };
        policy::validate("catalog", generation, &policy(generation).spec, &own).unwrap()
    }

    fn reason(status: &CachePolicyStatus, kind: &str) -> (String, String) {
        let c = status.conditions.iter().find(|c| c.type_ == kind).unwrap();
        (c.status.clone(), c.reason.clone())
    }

    fn now() -> Time {
        Time(Timestamp::now())
    }

    #[test]
    fn accepted_only_once_the_active_vcl_carries_the_generation() {
        let p = policy(2);
        let pending = desired_status(
            &p,
            &Verdict::Valid,
            Some(&rendered(1)),
            &Attachment::NotRequested,
            &now(),
        );
        assert_eq!(
            reason(&pending, ACCEPTED),
            ("False".into(), "Pending".into())
        );
        assert_eq!(pending.active_generation, Some(1));
        let live = desired_status(
            &p,
            &Verdict::Valid,
            Some(&rendered(2)),
            &Attachment::NotRequested,
            &now(),
        );
        assert_eq!(reason(&live, ACCEPTED), ("True".into(), "Active".into()));
        assert_eq!(reason(&live, INVALID), ("False".into(), "Valid".into()));
        assert!(
            live.conditions
                .iter()
                .all(|c| c.observed_generation == Some(2))
        );
    }

    #[test]
    fn invalid_generation_reports_reason_and_last_active_generation() {
        let rejection = Rejection {
            reason: InvalidReason::Conflict,
            message: "x".into(),
        };
        let s = desired_status(
            &policy(3),
            &Verdict::Invalid(rejection),
            Some(&rendered(2)),
            &Attachment::NotRequested,
            &now(),
        );
        assert_eq!(reason(&s, INVALID), ("True".into(), "Conflict".into()));
        assert_eq!(reason(&s, ACCEPTED), ("False".into(), "Conflict".into()));
        assert_eq!(s.active_generation, Some(2));
    }

    #[test]
    fn unchanged_conditions_keep_their_transition_time_so_writes_converge() {
        let mut p = policy(1);
        let first = desired_status(
            &p,
            &Verdict::Valid,
            Some(&rendered(1)),
            &Attachment::Unknown,
            &Time(Timestamp::UNIX_EPOCH),
        );
        p.status = Some(first.clone());
        let again = desired_status(
            &p,
            &Verdict::Valid,
            Some(&rendered(1)),
            &Attachment::Unknown,
            &now(),
        );
        assert!(same_status(p.status.as_ref(), &again));
        assert_eq!(
            reason(&again, ATTACHED),
            ("Unknown".into(), "VirtualServicesUnavailable".into())
        );
    }
}
