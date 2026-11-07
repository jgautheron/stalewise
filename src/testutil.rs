//! Shared test fixtures.

use std::path::PathBuf;

/// A fresh, canonical (symlink-free) temporary directory.
pub fn temp_dir(prefix: &str) -> PathBuf {
    let suffix = crate::entropy::weak_u64();
    let dir = std::env::temp_dir().join(format!("stalewise-{prefix}-{suffix:016x}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::canonicalize(dir).unwrap()
}
