//! The **quantum**: the minimal sealed unit of epistemic knowledge.
//!
//! A quantum is one content-addressed claim. Its fields partition into three
//! roles (`Field`'s `identity`/`witness` flags), decided by the two-question rule
//! from `ket/DESIGN.md`:
//!
//! - **Identity (I)** — binds the CID. The canonical, gauge-fixed structure,
//!   *including the grounding targets* (a `Set(Cid)` field), because a claim's
//!   grounding topology is part of what it *is*. "Change this → different concept."
//! - **Witness (W)** — a value recomputed by a route independent of the
//!   *canonicalizer's reduction step* (a rational's reduced form, a polynomial
//!   fingerprint, a unit's dimension, a checker verdict). It must be **exact**, not
//!   lossy — a float witness collides above 2^53 and yields false `UnderMerge`s.
//!   It does **not** bind the CID; it is what the cross-audit diffs identity
//!   against. This is `gnosis/invariant.py`'s dual route, given a principled home.
//!   (Honest bound: for a rational there is no route independent of the underlying
//!   datum — re-reduction catches an *unreduced canonical form*, not a wrong value.)
//! - **Projection (P)** — detachable names, prose, the NL source utterance,
//!   attribution. Canonicalized for transport, binds nothing, audits nothing.
//!
//! The address is `blake3(schema_cid ‖ identity_projection)` — meaning is
//! schema-relative, so the schema CID is part of identity. Because the substrate
//! is a CAS, sealing is idempotent and order-independent (a commutative monoid),
//! exactly as in `gnosis/ingest.py`.
//!
//! ## The split-edge decision (the one open L2 choice in DESIGN.md)
//!
//! A typed epistemic edge is split:
//! - the edge **target** is an identity `Set(Cid)` field on the claim — grounding
//!   topology binds the CID;
//! - the edge **kind** (`grounds`/`derives`/…) lives in a *separate*
//!   [`edge_annotation_schema`] quantum, keyed on `(from, to, annotator)` with
//!   `kind` as a non-identity field. Re-asserting a kind for the same edge by the
//!   same annotator **supersedes** (same identity → upsert); two annotators
//!   asserting different kinds **coexist** as a surfaced
//!   [`Disagreement`](crate::Disagreement). This satisfies both gnosis's
//!   "grounded-by-X is part of identity" and DESIGN.md's "an edge's kind must be
//!   correctable through the same lineage machinery."

use crate::canon::{Canon, CanonError};
use crate::schema::{FieldKind, Schema};
use serde_json::Value;

/// The five typed epistemic edge kinds. `grounds`/`derives`/`proposes` come from
/// `gnosis`; `supersedes`/`contradicts` carry the lineage and conflict channels
/// that resolution-as-a-logged-event and conflict-as-typed-diagnostic require.
pub const EDGE_KINDS: [&str; 5] = [
    "grounds",
    "derives",
    "proposes",
    "supersedes",
    "contradicts",
];

#[derive(Debug, thiserror::Error)]
pub enum QuantumError {
    #[error(transparent)]
    Canon(#[from] CanonError),
    #[error("unknown edge kind `{0}` (expected one of {EDGE_KINDS:?})")]
    UnknownEdgeKind(String),
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// The schema CID: hash of the schema's own canonical bytes. A schema is just
/// another sealed blob (DESIGN.md's recursive closure), so it is addressed the
/// same way every other quantum is.
pub fn schema_cid(schema: &Schema) -> String {
    blake3_hex(&schema.to_canonical_bytes())
}

/// The address function. The schema CID is folded into identity (meaning is
/// schema-relative), separated by an ASCII unit separator so the two byte ranges
/// can never collide.
pub fn address(schema_cid: &str, identity_projection: &[u8]) -> String {
    let mut buf = Vec::with_capacity(schema_cid.len() + 1 + identity_projection.len());
    buf.extend_from_slice(schema_cid.as_bytes());
    buf.push(0x1f);
    buf.extend_from_slice(identity_projection);
    blake3_hex(&buf)
}

/// A sealed claim.
#[derive(Debug, Clone)]
pub struct Quantum {
    /// CID of the schema this claim conforms to (part of identity).
    pub schema_cid: String,
    /// The claim's content address: `blake3(schema_cid ‖ identity_projection)`.
    pub cid: String,
    /// The full canonical record (Identity ∪ Witness ∪ Projection), as the schema
    /// ordered and validated it. Read fields back via [`Quantum::field`].
    pub body: Value,
}

impl Quantum {
    /// Seal `input` against `schema`: validate, canonicalize, and address. The
    /// returned `body` is the canonical full record; `cid` is computed from the
    /// identity projection only. Idempotent: same meaning → same CID, regardless
    /// of key order, projection-field differences, or grounding-set order.
    pub fn seal(schema: &Schema, input: &Value) -> Result<Quantum, QuantumError> {
        let canon = Canon::new(schema);
        let sc = schema_cid(schema);
        let id = canon.identity_projection(input)?;
        let body_bytes = canon.encode(input)?;
        let body: Value = serde_json::from_slice(&body_bytes).map_err(CanonError::from)?;
        Ok(Quantum {
            schema_cid: sc.clone(),
            cid: address(&sc, &id),
            body,
        })
    }

    /// Read a canonicalized field back from the body.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.body.as_object().and_then(|o| o.get(name))
    }

    /// The witness projection bytes for this quantum (the independently-checkable
    /// value), per `schema`. Empty if the schema declares no witness fields.
    pub fn witness_bytes(&self, schema: &Schema) -> Result<Vec<u8>, QuantumError> {
        Ok(Canon::new(schema).witness_projection(&self.body)?)
    }

    /// Self-audit one quantum: does an *independently recomputed* witness agree
    /// with the sealed one? `recomputed` carries the witness field(s) computed by
    /// a route that shares no machinery with the canonicalizer. Disagreement means
    /// the form and the invariant disagree on identity — a canonicalizer or
    /// witness-computation bug, the highest-severity conflict in DESIGN.md.
    pub fn verify_witness(
        &self,
        schema: &Schema,
        recomputed: &Value,
    ) -> Result<bool, QuantumError> {
        let canon = Canon::new(schema);
        let sealed = canon.witness_projection(&self.body)?;
        let fresh = canon.witness_projection(recomputed)?;
        Ok(sealed == fresh)
    }

    /// **Self-audit** — the executable form of DESIGN.md's contract ("identity is
    /// a function of content, so wrongness is detectable without an oracle"):
    /// re-derive this quantum's address from its stored body + schema and confirm
    /// it still equals `cid`. A tampered body, a wrong `schema_cid`, or a drifted
    /// canonicalizer all fail it. This is the `verify_cas` primitive for an
    /// in-memory quantum; cheap, and the cornerstone the substrate's honesty rests
    /// on. Returns `false` (not `Err`) on a mismatch — the mismatch *is* the
    /// finding.
    pub fn verify(&self, schema: &Schema) -> Result<bool, QuantumError> {
        if schema_cid(schema) != self.schema_cid {
            return Ok(false);
        }
        let id = Canon::new(schema).identity_projection(&self.body)?;
        Ok(address(&self.schema_cid, &id) == self.cid)
    }
}

/// A cross-audit finding over a set of quanta — the native form of
/// `gnosis/ingest.py`'s form-vs-invariant check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrossAuditConflict {
    /// One witness value resolves to **multiple distinct identity CIDs**. The
    /// canonicalizer *under-merged*: things the witness says are identical were
    /// sealed as different concepts. Dedup-by-form alone cannot catch this,
    /// because the forms genuinely differ.
    UnderMerge { witness: String, cids: Vec<String> },
    /// One identity CID carries **multiple distinct witnesses**. Either the
    /// witness computation is non-deterministic, or two voices disagree about the
    /// invariant of the same concept — surfaced honestly rather than clobbered.
    WitnessDisagreement { cid: String, witnesses: Vec<String> },
}

/// Run the form-vs-witness cross-audit over a set of quanta. Pure and
/// order-independent: it groups by witness and by CID and reports the two ways
/// the form route and the witness route can disagree on identity. Quanta whose
/// schema declares no witness are skipped (they are the *witness-free* tier — the
/// honest residue that needs human review, not a substrate check).
pub fn cross_audit(
    schema: &Schema,
    quanta: &[Quantum],
) -> Result<Vec<CrossAuditConflict>, QuantumError> {
    use std::collections::BTreeMap;

    if schema.witness_fields().is_empty() {
        return Ok(Vec::new());
    }
    let canon = Canon::new(schema);

    // witness -> set of identity CIDs ; cid -> set of witnesses
    let mut by_witness: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut by_cid: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for q in quanta {
        let w = String::from_utf8_lossy(&canon.witness_projection(&q.body)?).into_owned();
        push_unique(by_witness.entry(w.clone()).or_default(), &q.cid);
        push_unique(by_cid.entry(q.cid.clone()).or_default(), &w);
    }

    let mut out = Vec::new();
    for (witness, cids) in by_witness {
        if cids.len() > 1 {
            out.push(CrossAuditConflict::UnderMerge { witness, cids });
        }
    }
    for (cid, witnesses) in by_cid {
        if witnesses.len() > 1 {
            out.push(CrossAuditConflict::WitnessDisagreement { cid, witnesses });
        }
    }
    Ok(out)
}

fn push_unique(v: &mut Vec<String>, s: &str) {
    if !v.iter().any(|x| x == s) {
        v.push(s.to_string());
    }
}

/// The substrate-level schema for a **typed epistemic edge annotation** — the
/// "kind" half of the split edge.
///
/// Identity = `(from, to, annotator)`. `kind` is a required *non-identity* field,
/// so the same annotator re-asserting a kind for the same edge **supersedes**
/// (identity projection unchanged → upsert), while two annotators disagreeing
/// **coexist** (different identity → a surfaced [`Disagreement`](crate::Disagreement)).
/// `evidence` is optional projection.
pub fn edge_annotation_schema() -> Schema {
    Schema::new("edge_annotation", 1)
        .identity("from", FieldKind::Cid)
        .identity("to", FieldKind::Cid)
        .identity("annotator", FieldKind::String)
        .required("kind", FieldKind::String)
        .optional("evidence", FieldKind::String)
}

/// The substrate-level schema for a **head succession** record — the sealed
/// form of "re-sealing `name` moved its canonical head from `old` to `new`"
/// (issue #6's lineage-linked re-sealing; the plot move that makes canonical
/// heads knowable instead of leaving re-seals as anonymous siblings).
///
/// Identity = `(name, old, new, annotator)`, deliberately **matching
/// [`edge_annotation_schema`]'s convention**: there the annotator binds
/// identity too, so the same annotator re-declaring the same fact **upserts**
/// (idempotent — same CID) while two annotators declaring it independently
/// **coexist** as separate attestations, never clobbering each other. v2's
/// commitment 8 (provenance in the envelope, never content) will move the
/// annotator out of identity for *both* schemas in one version bump; until
/// then the two lineage channels stay convention-identical.
pub fn head_succession_schema() -> Schema {
    Schema::new("head_succession", 1)
        .identity("name", FieldKind::String)
        .identity("old", FieldKind::Cid)
        .identity("new", FieldKind::Cid)
        .identity("annotator", FieldKind::String)
}

/// Validate that an edge-annotation body carries a known `kind`. (canon.d treats
/// `kind` as an opaque string; this is the domain check the substrate offers for
/// the one vocabulary it does own.)
pub fn validate_edge_kind(annotation: &Value) -> Result<(), QuantumError> {
    let kind = annotation
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if EDGE_KINDS.contains(&kind) {
        Ok(())
    } else {
        Err(QuantumError::UnknownEdgeKind(kind.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A reference "claim" schema exercising all three roles:
    /// Identity: subject + value (num/den) + grounding set; Witness: value;
    /// Projection: label, prose (the NL source).
    fn claim_schema() -> Schema {
        Schema::new("ratio_claim", 1)
            .identity("subject", FieldKind::String)
            .identity("num", FieldKind::Integer)
            .identity("den", FieldKind::Integer)
            .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
            .witness("value", FieldKind::String)
            .optional("label", FieldKind::String)
            .optional("prose", FieldKind::String)
    }

    #[test]
    fn dedup_by_identity_not_name() {
        let s = claim_schema();
        // Same structural claim, two human framings (the Stern-Brocot / Farey case).
        let a = Quantum::seal(&s, &json!({
            "subject":"omega_lambda","num":13,"den":19,"grounds":["m"],
            "value":"13/19","label":"Stern-Brocot depth-6 forcing","prose":"thirteen nineteenths"
        })).unwrap();
        let b = Quantum::seal(
            &s,
            &json!({
                "subject":"omega_lambda","num":13,"den":19,"grounds":["m"],
                "value":"13/19","label":"Farey mediant 13/19","prose":"the dark-energy fraction"
            }),
        )
        .unwrap();
        assert_eq!(
            a.cid, b.cid,
            "names/prose are projection — must not fork identity"
        );
    }

    #[test]
    fn grounding_set_is_order_independent() {
        let s = claim_schema();
        let a = Quantum::seal(
            &s,
            &json!({
                "subject":"x","num":1,"den":2,"grounds":["A","B"],"value":"1/2"
            }),
        )
        .unwrap();
        let b = Quantum::seal(
            &s,
            &json!({
                "subject":"x","num":1,"den":2,"grounds":["B","A"],"value":"1/2"
            }),
        )
        .unwrap();
        assert_eq!(
            a.cid, b.cid,
            "grounding is a SET — target order must not fork identity"
        );
    }

    #[test]
    fn different_grounding_forks_identity() {
        let s = claim_schema();
        let a = Quantum::seal(
            &s,
            &json!({
                "subject":"x","num":1,"den":2,"grounds":["A"],"value":"1/2"
            }),
        )
        .unwrap();
        let b = Quantum::seal(
            &s,
            &json!({
                "subject":"x","num":1,"den":2,"grounds":["A","B"],"value":"1/2"
            }),
        )
        .unwrap();
        assert_ne!(
            a.cid, b.cid,
            "grounding topology is structural — different grounds, different claim"
        );
    }

    #[test]
    fn cross_audit_catches_under_merge() {
        let s = claim_schema();
        // A buggy emitter that forgot to reduce: 13/19 and 26/38 sealed as
        // DIFFERENT forms but the SAME witness value. Dedup sees nothing wrong
        // (the forms differ); the witness route catches it.
        let reduced = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        let unreduced = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":26,"den":38,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        assert_ne!(reduced.cid, unreduced.cid, "the two forms genuinely differ");

        let conflicts = cross_audit(&s, &[reduced, unreduced]).unwrap();
        assert!(
            conflicts.iter().any(
                |c| matches!(c, CrossAuditConflict::UnderMerge { cids, .. } if cids.len() == 2)
            ),
            "one witness resolving to two CIDs must surface as UnderMerge, got {conflicts:?}"
        );
    }

    #[test]
    fn grounds_in_identity_false_positives_on_corroboration() {
        // THE DEFECT (fixed in `strata`): because `grounds` binds identity here,
        // two INDEPENDENT, CORRECT derivations of one fact (same value, different
        // grounds) seal to different CIDs but share a witness — so cross_audit
        // mistakes corroboration for a canonicalizer UnderMerge. This test pins
        // the false positive so the proposition/assertion split has a target.
        let s = claim_schema();
        let via_planck = Quantum::seal(
            &s,
            &json!({
                "subject":"omega_lambda","num":13,"den":19,"grounds":["planck"],"value":"13/19"
            }),
        )
        .unwrap();
        let via_wmap = Quantum::seal(
            &s,
            &json!({
                "subject":"omega_lambda","num":13,"den":19,"grounds":["wmap"],"value":"13/19"
            }),
        )
        .unwrap();
        assert_ne!(
            via_planck.cid, via_wmap.cid,
            "grounds-in-identity forks the two tellings"
        );

        let conflicts = cross_audit(&s, &[via_planck, via_wmap]).unwrap();
        assert!(
            conflicts.iter().any(|c| matches!(c, CrossAuditConflict::UnderMerge { .. })),
            "DEFECT: corroboration is misreported as UnderMerge — strata::proposition_schema fixes this"
        );
    }

    #[test]
    fn cross_audit_clean_when_forms_agree() {
        let s = claim_schema();
        let a = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        // idempotent re-seal of the same meaning (different projection) — one node.
        let a2 = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19","label":"again"
            }),
        )
        .unwrap();
        assert_eq!(a.cid, a2.cid);
        assert!(
            cross_audit(&s, &[a, a2]).unwrap().is_empty(),
            "idempotent re-seal is not a conflict"
        );
    }

    #[test]
    fn witness_self_audit() {
        let s = claim_schema();
        let q = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        // independent recomputation 13/19 = 0.6842 -> agrees
        assert!(q.verify_witness(&s, &json!({"value":"13/19"})).unwrap());
        // a wrong recomputation -> disagrees (form vs invariant conflict)
        assert!(!q.verify_witness(&s, &json!({"value":"7/10"})).unwrap());
    }

    #[test]
    fn witness_free_schema_is_skipped() {
        // A schema with no witness fields: the witness-free tier. cross_audit is a
        // no-op (these are the claims a human must review, not a check fires on).
        let s = Schema::new("nl_claim", 1)
            .identity("subject", FieldKind::String)
            .identity("assertion", FieldKind::String)
            .optional("prose", FieldKind::String);
        let q = Quantum::seal(&s, &json!({"subject":"x","assertion":"y"})).unwrap();
        assert!(cross_audit(&s, &[q]).unwrap().is_empty());
    }

    #[test]
    fn edge_kind_supersedes_for_same_annotator_disagrees_across() {
        let s = edge_annotation_schema();
        let canon = Canon::new(&s);
        // same (from,to,annotator), different kind -> same identity projection (kind is correctable)
        let grounds_a = canon
            .identity_projection(&json!({
                "from":"A","to":"B","annotator":"claude","kind":"grounds"
            }))
            .unwrap();
        let derives_a = canon
            .identity_projection(&json!({
                "from":"A","to":"B","annotator":"claude","kind":"derives"
            }))
            .unwrap();
        assert_eq!(
            grounds_a, derives_a,
            "same annotator re-typing the same edge must SUPERSEDE"
        );

        // different annotator -> different identity -> coexists as a Disagreement
        let derives_b = canon
            .identity_projection(&json!({
                "from":"A","to":"B","annotator":"gpt","kind":"derives"
            }))
            .unwrap();
        assert_ne!(
            grounds_a, derives_b,
            "two annotators must COEXIST, not clobber"
        );
    }

    #[test]
    fn head_succession_identity_matches_edge_annotation_convention() {
        let s = head_succession_schema();
        // Same (name, old, new, annotator) resealed → one CID: re-declaring a
        // succession is an upsert, so replaying an intake run seals nothing new.
        let a = Quantum::seal(
            &s,
            &json!({"name":"glossary","old":"OLD","new":"NEW","annotator":"intake"}),
        )
        .unwrap();
        let b = Quantum::seal(
            &s,
            &json!({"name":"glossary","old":"OLD","new":"NEW","annotator":"intake"}),
        )
        .unwrap();
        assert_eq!(a.cid, b.cid, "identical succession reseals to one CID");

        // The annotator binds identity — edge_annotation's convention, kept
        // deliberately: two declarers of one succession COEXIST as separate
        // attestations (the commitment-8 envelope move is a later version bump
        // for both schemas at once).
        let c = Quantum::seal(
            &s,
            &json!({"name":"glossary","old":"OLD","new":"NEW","annotator":"gpt"}),
        )
        .unwrap();
        assert_ne!(a.cid, c.cid, "two annotators coexist, as in edge_annotation");

        // A different hop in the chain is a different record.
        let d = Quantum::seal(
            &s,
            &json!({"name":"glossary","old":"NEW","new":"NEWER","annotator":"intake"}),
        )
        .unwrap();
        assert_ne!(a.cid, d.cid, "each (old, new) hop seals its own quantum");
    }

    #[test]
    fn edge_kind_validation() {
        assert!(validate_edge_kind(&json!({"kind":"grounds"})).is_ok());
        assert!(validate_edge_kind(&json!({"kind":"causes"})).is_err());
    }

    // --- audit-driven coverage: the contract primitive and the untested paths ---

    #[test]
    fn verify_round_trips_and_detects_tamper() {
        // The executable form of DESIGN.md's contract: a sealed quantum re-hashes
        // to its own address; a tampered body does not.
        let s = claim_schema();
        let q = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        assert!(q.verify(&s).unwrap(), "a freshly sealed quantum verifies");

        let mut tampered = q.clone();
        tampered.body["num"] = json!(99); // mutate an identity field, keep the old cid
        assert!(
            !tampered.verify(&s).unwrap(),
            "a tampered identity field fails verify"
        );

        // tampering a projection field does NOT change identity → still verifies
        let mut relabeled = q.clone();
        relabeled.body["label"] = json!("renamed");
        assert!(
            relabeled.verify(&s).unwrap(),
            "projection edits don't break the address"
        );
    }

    #[test]
    fn witness_disagreement_is_caught() {
        // Same identity (subject/num/den/grounds) → same CID, but two different
        // witnesses → the substrate surfaces the disagreement rather than clobber.
        let s = claim_schema();
        let a = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"13/19"
            }),
        )
        .unwrap();
        let b = Quantum::seal(
            &s,
            &json!({
                "subject":"r","num":13,"den":19,"grounds":["m"],"value":"99/1"
            }),
        )
        .unwrap();
        assert_eq!(a.cid, b.cid, "same identity → one CID");
        let conflicts = cross_audit(&s, &[a, b]).unwrap();
        assert!(
            conflicts.iter().any(|c| matches!(c, CrossAuditConflict::WitnessDisagreement { witnesses, .. } if witnesses.len() == 2)),
            "one CID with two witnesses must surface as WitnessDisagreement, got {conflicts:?}"
        );
    }

    #[test]
    fn identity_is_schema_relative() {
        // The same body under two different schemas addresses differently —
        // meaning is schema-relative (the schema CID folds into the address).
        let a = Schema::new("alpha", 1).identity("x", FieldKind::String);
        let b = Schema::new("beta", 1).identity("x", FieldKind::String);
        let qa = Quantum::seal(&a, &json!({"x":"v"})).unwrap();
        let qb = Quantum::seal(&b, &json!({"x":"v"})).unwrap();
        assert_ne!(
            qa.cid, qb.cid,
            "same body, different schema → different CID"
        );
        assert_ne!(qa.schema_cid, qb.schema_cid);
    }

    #[test]
    fn serde_json_preserve_order_is_off() {
        // Load-bearing build invariant: schema/quantum addresses are stable only
        // while serde_json sorts object keys (preserve_order OFF). If this fails,
        // a transitive dep enabled the feature and every CID has silently moved.
        let v: Value = serde_json::from_str(r#"{"z":1,"a":2}"#).unwrap();
        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"{"a":2,"z":1}"#,
            "serde_json must sort keys (preserve_order must be OFF)"
        );
    }
}
