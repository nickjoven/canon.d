//! The NLP↔CID bridge: intake and grounding.
//!
//! **Principle: natural language is always Projection, never identity.** The
//! bridge does not make language meaning-addressed — it makes the *structured
//! object the language points to* addressable, and records the structuring step
//! as a supersedable proposal so a human (HITL) can accept or re-structure it.
//!
//! The intake pipeline, in canon.d terms:
//!
//! 1. **Utter** — seal the NL as an [`utterance_schema`] blob. Its CID is a byte
//!    CID (meaningless as a *meaning* address — paraphrases do not collapse); it
//!    is pure projection, attached to the claim, binding nothing.
//! 2. **Structure** — an emitter (the HITL LLM) names the formal object the
//!    utterance means and seals it as a [`Quantum`]. The emitter's proposal is a
//!    [`structuring_schema`] quantum keyed on `(utterance, annotator)` with the
//!    chosen `claim` as the *correctable* field — so the same emitter
//!    re-structuring **supersedes**, and two emitters disagreeing about meaning
//!    **coexist** as a surfaced [`Disagreement`](crate::Disagreement).
//! 3. **Witness** — if the claim's schema declares a witness, it self-checks
//!    (`cross_audit`). If not, it is the *witness-free* tier — [`needs_review`]
//!    is the honest queue a human must clear, not a check the substrate fires on.
//! 4. **Ground** — a claim's grounding targets must resolve. Leaves that touch
//!    reality are [`attestation_schema`] quanta — the one place an instrument or
//!    human vouches. [`ground_audit`] is the native form of gnosis's `ungrounded`
//!    check: a claim whose grounds don't resolve is "about nothing."

use crate::generator::{EvalError, Rat};
use crate::quantum::{Quantum, QuantumError};
use crate::schema::{FieldKind, Schema};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// The canonical witness string for an exact rational `num/den`.
///
/// Reuses [`Rat`]'s reduced canonical form (`gcd=1`, `den>0`, rendered
/// `"num/den"`). Byte-stable and exact for ANY rational in the i64 algebra,
/// including values with no finite binary float (e.g. `1/3`). A witness field
/// typed [`FieldKind::String`] carrying this is the F3-exact alternative to a
/// [`FieldKind::Float`] witness, which only verifies for binary-representable
/// values and is not pinned across serde_json versions. Returns `EvalError`
/// (`DivByZero`/`Overflow`) when the inputs leave the `Rat` domain.
pub fn rat_witness(num: i64, den: i64) -> Result<String, EvalError> {
    Ok(Rat::new(num, den)?.reduced_string())
}

/// A natural-language source utterance — pure projection. Identity is the bytes
/// `(text, lang)`, so byte-identical utterances dedup while paraphrases (which
/// mean the same thing but read differently) deliberately do **not** — NL is not
/// meaning-addressed.
pub fn utterance_schema() -> Schema {
    Schema::new("utterance", 1)
        .identity("text", FieldKind::String)
        .identity("lang", FieldKind::String)
        .optional("author", FieldKind::String)
        .optional("at", FieldKind::String)
}

/// An emitter's proposal that an utterance *means* a particular structured claim.
/// Identity = `(utterance, annotator)`; `claim` is the correctable target, so the
/// same emitter re-structuring the same utterance **supersedes**, and two
/// emitters proposing different claims **coexist** as a meaning-Disagreement.
/// This is the supersedable structuring step — the one deliberate boundary where
/// language→structure ambiguity lives.
pub fn structuring_schema() -> Schema {
    Schema::new("structuring", 1)
        .identity("utterance", FieldKind::Cid)
        .identity("annotator", FieldKind::String)
        .required("claim", FieldKind::Cid)
        .optional("rationale", FieldKind::String)
}

/// A grounding leaf that touches reality — the one place an instrument or human
/// vouches. **Witness-free by design**: reality is the witness; `vouched_by`
/// records who attests. Identity = `(instrument, dataset, locator)` — *which
/// record in the world* — so `value` is non-identity: a corrected reading of the
/// same record surfaces as an upsert/Disagreement, never a silent second concept.
pub fn attestation_schema() -> Schema {
    Schema::new("attestation", 1)
        .identity("instrument", FieldKind::String)
        .identity("dataset", FieldKind::String)
        .identity("locator", FieldKind::String)
        .required("value", FieldKind::Float)
        .required("vouched_by", FieldKind::String)
        .optional("unit", FieldKind::String)
        .optional("uncertainty", FieldKind::Float)
        .optional("retrieved_at", FieldKind::String)
        .optional("source_url", FieldKind::String)
}

/// The audit trail of one structuring: the NL blob, the structured claim, and the
/// supersedable proposal that links them.
#[derive(Debug, Clone)]
pub struct Structuring {
    pub utterance: Quantum,
    pub claim: Quantum,
    pub structuring: Quantum,
}

/// Run the intake bridge: seal the utterance, seal the emitter's structured
/// claim, and record the supersedable `(utterance, annotator) -> claim` proposal.
/// The NL never enters the claim's identity; it is recoverable via the structuring
/// quantum's `utterance` field.
pub fn structure(
    utterance_text: &str,
    lang: &str,
    emitter: &str,
    claim_schema: &Schema,
    claim_body: &Value,
) -> Result<Structuring, QuantumError> {
    let utterance = Quantum::seal(
        &utterance_schema(),
        &json!({ "text": utterance_text, "lang": lang, "author": emitter }),
    )?;
    let claim = Quantum::seal(claim_schema, claim_body)?;
    let structuring = Quantum::seal(
        &structuring_schema(),
        &json!({ "utterance": utterance.cid, "annotator": emitter, "claim": claim.cid }),
    )?;
    Ok(Structuring { utterance, claim, structuring })
}

/// True when a schema declares no witness field — i.e. claims under it cannot
/// self-check and belong to the human-review queue.
pub fn needs_review(schema: &Schema) -> bool {
    schema.witness_fields().is_empty()
}

/// A claim whose grounding target does not resolve to any known sealed CID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ungrounded {
    /// CID of the claim with the dangling ground.
    pub claim: String,
    /// The grounding target that did not resolve.
    pub missing: String,
}

/// Grounding audit (native form of gnosis's `ungrounded`): every CID in each
/// claim's `grounds_field` set must resolve to a member of `known` (the CIDs of
/// sealed attestations and other claims). A dangling ground means the claim is
/// "about nothing." Pure and order-independent — pass the known CID set.
pub fn ground_audit(
    grounds_field: &str,
    claims: &[Quantum],
    known: &BTreeSet<String>,
) -> Vec<Ungrounded> {
    let mut out = Vec::new();
    for q in claims {
        let Some(arr) = q.field(grounds_field).and_then(|v| v.as_array()) else {
            continue; // no grounds field / not a list — not this audit's concern
        };
        for target in arr {
            if let Some(t) = target.as_str() {
                if !known.contains(t) {
                    out.push(Ungrounded { claim: q.cid.clone(), missing: t.to_string() });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Canon;

    fn claim_schema() -> Schema {
        Schema::new("ratio_claim", 1)
            .identity("subject", FieldKind::String)
            .identity("num", FieldKind::Integer)
            .identity("den", FieldKind::Integer)
            .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
            // Exact-rational witness: a reduced "num/den" string, not an f64.
            // Verifies for ANY reduced rational; an f64 witness verifies only for
            // binary-representable values and is unpinned across serde_json.
            .witness("value", FieldKind::String)
            .optional("prose", FieldKind::String)
    }

    #[test]
    fn nl_is_projection_paraphrases_share_one_claim() {
        let cs = claim_schema();
        let att = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                    "value":0.6847,"vouched_by":"nick"}),
        )
        .unwrap();
        let body = json!({"subject":"omega_lambda","num":13,"den":19,
                          "grounds":[att.cid],"value":rat_witness(13,19).unwrap()});

        let a = structure("the dark-energy fraction is thirteen nineteenths", "en", "claude", &cs, &body).unwrap();
        let b = structure("Omega_Lambda equals 13/19", "en", "claude", &cs, &body).unwrap();

        // Two different NL utterances -> two distinct utterance blobs ...
        assert_ne!(a.utterance.cid, b.utterance.cid, "paraphrases are distinct utterances");
        // ... but the SAME structured meaning -> one claim CID.
        assert_eq!(a.claim.cid, b.claim.cid, "NL is projection; structure binds identity");
        // The NL never entered the claim body's identity.
        assert!(a.claim.field("prose").is_none());
    }

    #[test]
    fn restructure_supersedes_disagreement_coexists() {
        let cs = claim_schema();
        let g = "0000"; // a stand-in grounding cid
        let body1 = json!({"subject":"x","num":13,"den":19,"grounds":[g],"value":rat_witness(13,19).unwrap()});
        let body2 = json!({"subject":"x","num":13,"den":18,"grounds":[g],"value":rat_witness(13,18).unwrap()});

        let s1 = structure("ratio claim", "en", "claude", &cs, &body1).unwrap();
        // Same emitter, same utterance, RE-structured to a different claim -> supersede.
        let s2 = structure("ratio claim", "en", "claude", &cs, &body2).unwrap();
        let ss = structuring_schema();
        let canon = Canon::new(&ss);
        let id1 = canon.identity_projection(&json!({"utterance":s1.utterance.cid,"annotator":"claude","claim":s1.claim.cid})).unwrap();
        let id2 = canon.identity_projection(&json!({"utterance":s2.utterance.cid,"annotator":"claude","claim":s2.claim.cid})).unwrap();
        assert_eq!(id1, id2, "same emitter re-structuring the same utterance must SUPERSEDE");

        // A different emitter -> different structuring identity -> coexists (meaning Disagreement).
        let s3 = structure("ratio claim", "en", "gpt", &cs, &body2).unwrap();
        let id3 = canon.identity_projection(&json!({"utterance":s3.utterance.cid,"annotator":"gpt","claim":s3.claim.cid})).unwrap();
        assert_ne!(id1, id3, "two emitters proposing meanings must COEXIST, not clobber");
    }

    #[test]
    fn rat_witness_is_exact_for_non_binary_rationals() {
        // 1/3 has no finite binary float; an f64 witness rounds it. The reduced
        // "num/den" string is exact and reduces, so it verifies and re-reduces.
        assert_eq!(rat_witness(1, 3).unwrap(), "1/3");
        assert_eq!(rat_witness(2, 6).unwrap(), "1/3", "reduces to canonical form");

        let cs = claim_schema();
        let q = Quantum::seal(
            &cs,
            &json!({"subject":"x","num":1,"den":3,"grounds":["m"],"value":rat_witness(1,3).unwrap()}),
        )
        .unwrap();
        // Independent recomputation of 1/3 agrees; a wrong value disagrees. An f64
        // witness could not represent 1/3 to compare exactly at all.
        assert!(q.verify_witness(&cs, &json!({"value": rat_witness(1, 3).unwrap()})).unwrap());
        assert!(!q.verify_witness(&cs, &json!({"value": rat_witness(1, 2).unwrap()})).unwrap());
        // out-of-domain inputs are typed errors, not panics
        assert!(rat_witness(1, 0).is_err());
    }

    #[test]
    fn attestation_is_witness_free_review_tier() {
        assert!(needs_review(&attestation_schema()), "grounding leaves are witness-free by design");
        assert!(!needs_review(&claim_schema()), "a claim with a witness self-checks");
    }

    #[test]
    fn attestation_value_is_not_identity() {
        let ats = attestation_schema();
        let canon = Canon::new(&ats);
        // Same record in the world, two readings of the value -> same identity (a
        // surfaced correction/Disagreement), not two silent concepts.
        let read1 = json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda","value":0.6847,"vouched_by":"a"});
        let read2 = json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda","value":0.6850,"vouched_by":"b"});
        assert_eq!(
            canon.identity_projection(&read1).unwrap(),
            canon.identity_projection(&read2).unwrap(),
            "same (instrument,dataset,locator) is one attestation identity"
        );
    }

    #[test]
    fn ground_audit_flags_dangling_then_clean() {
        let cs = claim_schema();
        let att = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"Planck","dataset":"2018","locator":"OL","value":0.6847,"vouched_by":"n"}),
        )
        .unwrap();
        let grounded = Quantum::seal(&cs, &json!({"subject":"x","num":13,"den":19,"grounds":[att.cid],"value":rat_witness(13,19).unwrap()})).unwrap();
        let dangling = Quantum::seal(&cs, &json!({"subject":"y","num":1,"den":2,"grounds":["deadbeef"],"value":rat_witness(1,2).unwrap()})).unwrap();

        let mut known = BTreeSet::new();
        known.insert(att.cid.clone());

        let report = ground_audit("grounds", &[grounded.clone(), dangling.clone()], &known);
        assert_eq!(report.len(), 1, "exactly one dangling ground");
        assert_eq!(report[0].claim, dangling.cid);
        assert_eq!(report[0].missing, "deadbeef");

        // Add the dangling target to the known set -> clean.
        known.insert("deadbeef".to_string());
        assert!(ground_audit("grounds", &[grounded, dangling], &known).is_empty());
    }
}
