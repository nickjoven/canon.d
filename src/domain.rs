use serde::{Deserialize, Serialize};

/// A named collection of schemas representing a knowledge domain.
/// Domains are content-addressed: serialize canonically, hash, store in CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub name: String,
    pub version: u32,
    /// Schema CIDs belonging to this domain. Kept sorted lexicographically
    /// for deterministic serialization.
    pub schema_cids: Vec<String>,
}

impl Domain {
    /// Create a new empty domain with the given name and version.
    pub fn new(name: &str, version: u32) -> Self {
        Self {
            name: name.to_string(),
            version,
            schema_cids: Vec::new(),
        }
    }

    /// Builder method: inserts a schema CID in sorted position.
    /// Duplicates are ignored.
    pub fn with_schema(mut self, schema_cid: &str) -> Self {
        match self.schema_cids.binary_search(&schema_cid.to_string()) {
            Ok(_) => {} // already present, skip
            Err(pos) => self.schema_cids.insert(pos, schema_cid.to_string()),
        }
        self
    }

    /// Serialize to canonical bytes (field order fixed by derive).
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Domain serialization should not fail")
    }

    /// Check whether a schema CID belongs to this domain (binary search).
    pub fn contains(&self, schema_cid: &str) -> bool {
        self.schema_cids
            .binary_search(&schema_cid.to_string())
            .is_ok()
    }

    /// Number of schemas in this domain.
    pub fn schema_count(&self) -> usize {
        self.schema_cids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_determinism() {
        let d1 = Domain::new("math", 1)
            .with_schema("cid_aaa")
            .with_schema("cid_zzz")
            .with_schema("cid_mmm");

        let d2 = Domain::new("math", 1)
            .with_schema("cid_zzz")
            .with_schema("cid_mmm")
            .with_schema("cid_aaa");

        assert_eq!(d1.to_canonical_bytes(), d2.to_canonical_bytes());
    }

    #[test]
    fn contains_check() {
        let d = Domain::new("physics", 1)
            .with_schema("cid_alpha")
            .with_schema("cid_beta");

        assert!(d.contains("cid_alpha"));
        assert!(d.contains("cid_beta"));
        assert!(!d.contains("cid_gamma"));
    }

    #[test]
    fn dedup_sorted_insertion() {
        let d = Domain::new("bio", 1)
            .with_schema("cid_one")
            .with_schema("cid_two")
            .with_schema("cid_one");

        assert_eq!(d.schema_count(), 2);
        assert!(d.contains("cid_one"));
        assert!(d.contains("cid_two"));
    }
}
