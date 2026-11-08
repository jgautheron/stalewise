//! Startup memory budget: checks the malloc stevedores against the container memory limit.

use std::path::Path;

/// Resident memory of an idle stalewise container (varnishd, its shared log and stalewise), with
/// headroom for loaded VCLs and worker threads; measured about 102 MiB idle on Varnish 9.1.
pub const BASE_OVERHEAD: u64 = 128 * MIB;
/// Memory each cached object costs outside the stevedore size (objcore, objhead, hash, allocator
/// slack); measured 0.6 to 1.2 KiB on Varnish 9.1.
pub const OBJECT_OVERHEAD: u64 = 1024;
/// The budget is tight when the headroom holds fewer objects than storage filled with 4 KiB ones.
const TIGHT_OBJECT_SIZE: u64 = 4 * 1024;
const MIB: u64 = 1024 * 1024;
const CGROUP_V2: &str = "/sys/fs/cgroup/memory.max";
const CGROUP_V1: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
const USAGE_V2: &str = "sys/fs/cgroup/memory.current";
const STAT_V2: &str = "sys/fs/cgroup/memory.stat";
const USAGE_V1: &str = "sys/fs/cgroup/memory/memory.usage_in_bytes";
const STAT_V1: &str = "sys/fs/cgroup/memory/memory.stat";
/// cgroup v1 reports "unlimited" as a page-aligned value near `i64::MAX`.
const UNLIMITED_V1: u64 = 1 << 60;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "memory limit {limit_mib} MiB cannot hold storage and transient storage ({stores_mib} MiB) plus \
     the {base_mib} MiB base overhead; raise the limit or shrink --storage/--transient-storage"
)]
pub struct CapacityError {
    pub limit_mib: u64,
    pub stores_mib: u64,
    pub base_mib: u64,
}

/// How the malloc stevedores fit the memory limit.
#[derive(Debug, PartialEq, Eq)]
pub enum Budget {
    /// No limit, or a stevedore whose memory use is unknown.
    Unchecked,
    Fits {
        objects: u64,
    },
    Tight {
        objects: u64,
    },
}

/// Bytes a `-s` argument reserves in memory: `[name=]malloc,<size>` or `default,<size>`.
/// Returns `None` for other stevedores and for sizes Varnish would not accept.
pub fn stevedore_bytes(spec: &str) -> Option<u64> {
    let spec = spec.split_once('=').map_or(spec, |(_, rest)| rest);
    let (kind, size) = spec.split_once(',')?;
    if !matches!(kind, "malloc" | "default") {
        return None;
    }
    varnish_size(size)
}

/// Parses Varnish byte sizes: digits plus an optional k, m, g or t suffix, binary multiples.
fn varnish_size(size: &str) -> Option<u64> {
    let size = size.strip_suffix(['b', 'B']).unwrap_or(size);
    let split = size
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(size.len());
    let (digits, unit) = size.split_at(split);
    let shift = match unit.to_ascii_lowercase().as_str() {
        "" => 0,
        "k" => 10,
        "m" => 20,
        "g" => 30,
        "t" => 40,
        _ => return None,
    };
    digits.parse::<u64>().ok()?.checked_mul(1 << shift)
}

/// The container memory limit from cgroup v2, then v1; `None` when unlimited or unreadable.
pub fn cgroup_limit(root: &Path) -> Option<u64> {
    let v2 = std::fs::read_to_string(root.join(CGROUP_V2.trim_start_matches('/')));
    if let Ok(v2) = v2 {
        return v2.trim().parse().ok();
    }
    let v1: u64 = std::fs::read_to_string(root.join(CGROUP_V1.trim_start_matches('/')))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    (v1 < UNLIMITED_V1).then_some(v1)
}

/// Container memory use as the kubelet sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    /// Raw cgroup usage, page cache included.
    pub current: u64,
    /// `current` minus reclaimable inactive page cache; what OOM and eviction act on.
    pub working_set: u64,
}

/// Reads cgroup v2 `memory.current`, then v1 `memory.usage_in_bytes`; `None` when unreadable.
pub fn cgroup_usage(root: &Path) -> Option<Usage> {
    let read = |path: &str| std::fs::read_to_string(root.join(path)).ok();
    let (current, stat, key) = match read(USAGE_V2) {
        Some(current) => (current, read(STAT_V2), "inactive_file"),
        None => (read(USAGE_V1)?, read(STAT_V1), "total_inactive_file"),
    };
    let current: u64 = current.trim().parse().ok()?;
    let inactive = stat
        .as_deref()
        .and_then(|s| stat_field(s, key))
        .unwrap_or(0);
    Some(Usage {
        current,
        working_set: current.saturating_sub(inactive),
    })
}

fn stat_field(stat: &str, key: &str) -> Option<u64> {
    stat.lines()
        .filter_map(|line| line.split_once(' '))
        .find(|(name, _)| *name == key)
        .and_then(|(_, value)| value.trim().parse().ok())
}

/// Compares the stores plus the base overhead with `limit`; errors when they cannot fit.
pub fn assess(
    limit: Option<u64>,
    storage: Option<u64>,
    transient: Option<u64>,
) -> Result<Budget, CapacityError> {
    let (Some(limit), Some(storage), Some(transient)) = (limit, storage, transient) else {
        return Ok(Budget::Unchecked);
    };
    let fixed = storage
        .saturating_add(transient)
        .saturating_add(BASE_OVERHEAD);
    let Some(headroom) = limit.checked_sub(fixed).filter(|h| *h > 0) else {
        return Err(CapacityError {
            limit_mib: limit / MIB,
            stores_mib: storage.saturating_add(transient) / MIB,
            base_mib: BASE_OVERHEAD / MIB,
        });
    };
    let objects = headroom / OBJECT_OVERHEAD;
    if objects < storage / TIGHT_OBJECT_SIZE {
        return Ok(Budget::Tight { objects });
    }
    Ok(Budget::Fits { objects })
}

/// Logs the budget for the running container; errors when the stores clearly cannot fit.
pub fn check(storage: &str, transient: &str) -> Result<(), CapacityError> {
    let limit = cgroup_limit(Path::new("/"));
    let budget = assess(limit, stevedore_bytes(storage), stevedore_bytes(transient))?;
    match budget {
        Budget::Unchecked => {
            tracing::info!(memory_limit = ?limit, "memory budget not checked");
        }
        Budget::Fits { objects } => {
            tracing::info!(memory_limit = ?limit, max_objects = objects, "memory budget");
        }
        Budget::Tight { objects } => tracing::warn!(
            memory_limit = ?limit,
            max_objects = objects,
            object_overhead_bytes = OBJECT_OVERHEAD,
            "memory budget is tight: more small objects than this risks an OOM kill; raise the \
             memory limit or shrink --storage"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stevedore_sizes_follow_varnish_syntax() {
        assert_eq!(stevedore_bytes("malloc,256m"), Some(256 * MIB));
        assert_eq!(stevedore_bytes("s0=malloc,1G"), Some(1024 * MIB));
        assert_eq!(stevedore_bytes("default,512k"), Some(512 * 1024));
        assert_eq!(stevedore_bytes("malloc,1048576"), Some(MIB));
        assert_eq!(stevedore_bytes("malloc"), None, "unbounded malloc");
        assert_eq!(stevedore_bytes("file,/tmp/x,1g"), None);
        assert_eq!(stevedore_bytes("malloc,1.5g"), None);
        assert_eq!(stevedore_bytes("malloc,99999999999t"), None, "overflow");
    }

    #[test]
    fn stores_beyond_the_limit_refuse_to_start() {
        let err = assess(Some(192 * MIB), Some(64 * MIB), Some(16 * MIB)).unwrap_err();
        assert_eq!(err.limit_mib, 192);
        assert!(
            assess(Some(208 * MIB), Some(64 * MIB), Some(16 * MIB)).is_err(),
            "zero headroom"
        );
    }

    #[test]
    fn headroom_decides_between_fitting_and_tight() {
        // The chart default: 256m + 64m + base in 512Mi leaves 64 MiB, 65536 objects.
        let default = assess(Some(512 * MIB), Some(256 * MIB), Some(64 * MIB));
        assert_eq!(default, Ok(Budget::Fits { objects: 65536 }));
        let tight = assess(Some(456 * MIB), Some(256 * MIB), Some(64 * MIB));
        assert_eq!(tight, Ok(Budget::Tight { objects: 8 * 1024 }));
    }

    #[test]
    fn missing_limit_or_unknown_stevedore_is_unchecked() {
        assert_eq!(assess(None, Some(MIB), Some(MIB)), Ok(Budget::Unchecked));
        assert_eq!(assess(Some(MIB), None, Some(MIB)), Ok(Budget::Unchecked));
    }

    #[test]
    fn cgroup_limit_reads_v2_then_v1() {
        let root = crate::testutil::temp_dir("cgroup");
        assert_eq!(cgroup_limit(&root), None, "no cgroup files");
        let v1 = root.join(CGROUP_V1.trim_start_matches('/'));
        std::fs::create_dir_all(v1.parent().unwrap()).unwrap();
        std::fs::write(&v1, "9223372036854771712\n").unwrap();
        assert_eq!(cgroup_limit(&root), None, "v1 unlimited");
        std::fs::write(&v1, "268435456\n").unwrap();
        assert_eq!(cgroup_limit(&root), Some(256 * MIB));
        std::fs::write(root.join(CGROUP_V2.trim_start_matches('/')), "max\n").unwrap();
        assert_eq!(cgroup_limit(&root), None, "v2 unlimited wins over v1");
        std::fs::write(root.join(CGROUP_V2.trim_start_matches('/')), "536870912\n").unwrap();
        assert_eq!(cgroup_limit(&root), Some(512 * MIB));
    }

    #[test]
    fn working_set_excludes_inactive_page_cache() {
        let root = crate::testutil::temp_dir("usage");
        assert_eq!(cgroup_usage(&root), None, "no cgroup files");
        let v1 = root.join(USAGE_V1);
        std::fs::create_dir_all(v1.parent().unwrap()).unwrap();
        std::fs::write(&v1, "1000\n").unwrap();
        std::fs::write(root.join(STAT_V1), "cache 5\ntotal_inactive_file 300\n").unwrap();
        let usage = cgroup_usage(&root).unwrap();
        assert_eq!((usage.current, usage.working_set), (1000, 700));
        std::fs::write(root.join(USAGE_V2), "2000\n").unwrap();
        let usage = cgroup_usage(&root).unwrap();
        assert_eq!(
            usage.working_set, 2000,
            "v2 wins; missing memory.stat counts nothing"
        );
        std::fs::write(
            root.join(STAT_V2),
            "anon 10\nactive_file 1\ninactive_file 1500\n",
        )
        .unwrap();
        assert_eq!(cgroup_usage(&root).unwrap().working_set, 500);
        std::fs::write(root.join(STAT_V2), "inactive_file 9999\n").unwrap();
        assert_eq!(cgroup_usage(&root).unwrap().working_set, 0, "saturates");
        std::fs::write(root.join(USAGE_V2), "garbage\n").unwrap();
        assert_eq!(cgroup_usage(&root), None);
    }
}
