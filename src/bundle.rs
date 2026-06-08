//! Portable bundles — ship the boundary, regenerate the bulk.
//!
//! A [`Bundle`] packages a substrate's **boundary** (sealed quanta + grounding
//! rules + transparency-log payloads + vouches) plus its **consensus root** as an
//! integrity seal. A fresh substrate [`import`]s it: re-verifies every quantum
//! (re-seal == cid), replays the log, rebuilds the closure, and **checks the
//! recomputed consensus root equals the shipped one**. A corrupted bundle is
//! rejected; a valid one returns a [`Loaded`] substrate whose certified facts are
//! grounding leaves for forward projections.
//!
//! This is the content-level portability artifact — the holographic principle as a
//! file: the **bulk** (the closure, the certain core) is *not* shipped; it is
//! regenerated on import and checked against the root. So "open a repo, init the
//! substrate, load harmonics, compute forward" reduces to `import` + a forward
//! generator grounding in the loaded facts.
//!
//! Bounds: schemas are referenced by a fixed [`SchemaKind`] (the substrate's
//! built-in vocabulary), not shipped as quanta — a cross-vocabulary bundle would
//! seal schemas too (recursive closure). Single in-memory/JSON bundle, no streaming.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    assertion_schema, attestation_schema, consensus_root, generator_schema, proposition_schema,
    Closure, Quantum, QuantumError, Schema, TransparencyLog, Vouch,
};

/// Which built-in schema a bundled quantum conforms to (so the importer can
/// reconstruct the schema to re-verify it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaKind {
    Attestation,
    Generator,
    Proposition,
    Assertion,
}

fn schema_for(k: SchemaKind) -> Schema {
    match k {
        SchemaKind::Attestation => attestation_schema(),
        SchemaKind::Generator => generator_schema(),
        SchemaKind::Proposition => proposition_schema(),
        SchemaKind::Assertion => assertion_schema(),
    }
}

fn body_digest(body: &Value) -> String {
    // canonical bytes: serde_json sorts object keys (preserve_order off)
    blake3::hash(&serde_json::to_vec(body).expect("body serializes")).to_hex().to_string()
}

/// A sealed quantum in transit: its schema kind, canonical body, claimed CID, and
/// a **full-body digest**. The CID commits only the *identity* projection (so a
/// tampered non-identity field — e.g. an attestation's `value` — would not change
/// it); the body digest commits the *whole* body, closing that gap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleEntry {
    pub kind: SchemaKind,
    pub body: Value,
    pub cid: String,
    pub body_digest: String,
}

impl BundleEntry {
    pub fn of(kind: SchemaKind, q: &Quantum) -> Self {
        BundleEntry { kind, body: q.body.clone(), cid: q.cid.clone(), body_digest: body_digest(&q.body) }
    }
}

/// A grounding rule `head ⟸ body` (the closure structure, shipped explicitly).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub head: String,
    pub body: Vec<String>,
}

/// The portable artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub entries: Vec<BundleEntry>,
    pub anchors: Vec<String>,
    pub rules: Vec<Rule>,
    pub log_payloads: Vec<String>,
    pub vouches: Vec<Vouch>,
    /// The integrity seal: the consensus root the importer must reproduce.
    pub consensus_root: String,
}

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("quantum {cid} does not re-seal to its address (corrupt body or wrong schema)")]
    CorruptQuantum { cid: String },
    #[error("transparency log failed to verify on replay")]
    CorruptLog,
    #[error("consensus root mismatch: bundle claims {claimed}, rebuilt {rebuilt}")]
    RootMismatch { claimed: String, rebuilt: String },
    #[error(transparent)]
    Quantum(#[from] QuantumError),
}

/// A substrate loaded from a verified bundle: the rebuilt + settled closure, a
/// `cid → quantum` lookup (the certified facts, available as grounding leaves for
/// forward projections), and the verified transparency log.
pub struct Loaded {
    pub closure: Closure,
    pub quanta: BTreeMap<String, Quantum>,
    pub log: TransparencyLog,
}

impl Loaded {
    /// A certified fact by CID — present in the bundle *and* certain in the rebuilt
    /// closure (grounded back to a bundled anchor). The thing a forward projection
    /// grounds in.
    pub fn certain_fact(&self, cid: &str) -> Option<&Quantum> {
        if self.closure.is_certain(cid) {
            self.quanta.get(cid)
        } else {
            None
        }
    }
}

fn build_closure(anchors: &[String], rules: &[Rule]) -> Closure {
    let mut cl = Closure::new();
    for a in anchors {
        cl.add_anchor(a);
    }
    for r in rules {
        cl.add_rule(r.head.clone(), r.body.iter().cloned().collect());
    }
    cl
}

/// Pack a substrate's boundary into a bundle. The consensus root is computed here
/// (over the rebuilt closure) and shipped as the integrity seal.
pub fn export(
    entries: Vec<BundleEntry>,
    anchors: Vec<String>,
    rules: Vec<Rule>,
    log: &TransparencyLog,
    vouches: Vec<Vouch>,
) -> Bundle {
    let consensus_root = consensus_root(&build_closure(&anchors, &rules));
    Bundle { entries, anchors, rules, log_payloads: log.payloads(), vouches, consensus_root }
}

/// Import a bundle into a fresh substrate: verify every quantum, replay+verify the
/// log, rebuild the closure, and check the consensus root. Rejects corruption;
/// returns the loaded substrate.
pub fn import(b: &Bundle) -> Result<Loaded, BundleError> {
    // 1. every quantum must re-seal to its claimed cid (the per-node verify).
    let mut quanta = BTreeMap::new();
    for e in &b.entries {
        // body digest commits the whole body; cid re-seal confirms the claimed
        // address is the right one for that body. Both must hold.
        let q = Quantum::seal(&schema_for(e.kind), &e.body)?;
        if q.cid != e.cid || body_digest(&e.body) != e.body_digest {
            return Err(BundleError::CorruptQuantum { cid: e.cid.clone() });
        }
        quanta.insert(q.cid.clone(), q);
    }
    // 2. replay + verify the transparency log (regenerated from its payloads).
    let mut log = TransparencyLog::new();
    for p in &b.log_payloads {
        log.append(p);
    }
    if !log.verify() {
        return Err(BundleError::CorruptLog);
    }
    // 3. rebuild the bulk (the closure) from the boundary.
    let closure = build_closure(&b.anchors, &b.rules);
    // 4. the integrity seal: the regenerated bulk must match the shipped root.
    let rebuilt = consensus_root(&closure);
    if rebuilt != b.consensus_root {
        return Err(BundleError::RootMismatch { claimed: b.consensus_root.clone(), rebuilt });
    }
    Ok(Loaded { closure, quanta, log })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn anchor_bundle() -> (String, Bundle) {
        let a = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"x","dataset":"y","locator":"z","value":1.0,"vouched_by":"n"}),
        )
        .unwrap();
        let log = TransparencyLog::new();
        let b = export(
            vec![BundleEntry::of(SchemaKind::Attestation, &a)],
            vec![a.cid.clone()],
            vec![],
            &log,
            vec![],
        );
        (a.cid, b)
    }

    #[test]
    fn round_trips_through_json_and_imports() {
        let (anchor_cid, b) = anchor_bundle();
        let json = serde_json::to_string(&b).unwrap();
        let back: Bundle = serde_json::from_str(&json).unwrap();
        let loaded = import(&back).unwrap();
        assert!(loaded.certain_fact(&anchor_cid).is_some(), "the bundled anchor is certain after import");
    }

    #[test]
    fn import_rejects_corrupt_quantum() {
        let (_, mut b) = anchor_bundle();
        // tamper the body but keep the claimed cid → re-seal mismatch
        b.entries[0].body["value"] = json!(999.0);
        assert!(matches!(import(&b), Err(BundleError::CorruptQuantum { .. })));
    }

    #[test]
    fn import_rejects_root_mismatch() {
        let (_, mut b) = anchor_bundle();
        b.consensus_root = "0000000000000000".into();
        assert!(matches!(import(&b), Err(BundleError::RootMismatch { .. })));
    }
}
