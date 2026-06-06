//! E2E-1 — the happy path: the **full pipeline in one journey**.
//!
//! One fact (Ω_Λ = 13/19) is threaded from the NL intake boundary all the way to
//! the certified consensus core, and we assert every **seam** holds end to end —
//! the CID that one layer produces is the exact CID the next layer consumes:
//!
//! utterance/derivation → anchor → vouch + log → audit → regenerate → ground →
//! certify → consensus root.
//!
//! Threads `bridge` + `crypto` + `log` + `generator` + `strata` + `closure` +
//! `chain`. Split into focused `#[test]`s, each pinning one seam, plus the NL
//! intake projection seam (the bridge): two utterances, one structured claim.

mod common;
use common::{build_closure, omega_corpus, trusted_log};

use canon_d::{
    audit_anchor, consensus_root, proposition_schema, structure, verify_regeneration,
};

/// Seam 1 — **regeneration link**: the serialized fact is the faithful, reproducible
/// output of its claimed generator (the strongest cryptographic link in the chain).
#[test]
fn regeneration_link_reproduces_the_fact() {
    let c = omega_corpus();
    assert!(
        verify_regeneration(&c.program, &[], "omega_lambda", "harmonics", &c.proposition.cid)
            .unwrap(),
        "the generator re-run reproduces exactly the proposition's CID"
    );
}

/// Seam 2 — **grounded certainty**: closing the chain (anchor → generator →
/// proposition) makes the fact *certain*, grounded back through its generator to
/// the lab anchor.
#[test]
fn grounded_closure_makes_the_fact_certain() {
    let c = omega_corpus();
    let closure = build_closure(&c, false);
    assert!(
        closure.is_certain(&c.proposition.cid),
        "the fact is grounded back through generator → anchor, therefore certain"
    );
}

/// Seam 3 — **accuracy is auditable**: a trusted party vouched for the anchor AND
/// that vouch is recorded in a log that itself verifies. Accuracy is *audited*, not
/// proven — but here the boundary clears the audit.
#[test]
fn anchor_accuracy_is_auditable() {
    let c = omega_corpus();
    let (log, trusted) = trusted_log(&c);
    assert!(log.verify(), "the transparency log itself is untampered");

    let audit = audit_anchor(&c.anchor.cid, std::slice::from_ref(&c.vouch), &trusted, &log);
    assert!(
        audit.is_auditable(),
        "a trusted party vouched AND the vouch is logged ⇒ auditable"
    );
    assert_eq!(audit.vouched_by, vec![c.vouch.signer.clone()]);
}

/// Seam 4 — **consensus**: the root over the certain core is non-empty and stable,
/// and the certified set is exactly the one containing our fact.
#[test]
fn consensus_root_commits_to_the_certain_fact() {
    let c = omega_corpus();
    let closure = build_closure(&c, false);

    let root = consensus_root(&closure);
    assert!(!root.is_empty(), "the consensus root is non-empty");
    // Stable: recomputing over the same closure yields the same root.
    assert_eq!(root, consensus_root(&closure), "the root is stable");

    assert!(
        closure.certain().contains(&c.proposition.cid),
        "the proposition's certainty is what the consensus root commits to"
    );
}

/// Seam 5 — **the NL intake path is projection** (threads the bridge): two *different*
/// natural-language utterances about the same structured fact produce two distinct
/// `utterance` CIDs (NL is byte-addressed projection, paraphrases do not collapse)
/// but a single `claim` CID (structure binds identity).
#[test]
fn nl_intake_is_projection_two_utterances_one_claim() {
    // Reuse the proposition schema as the claim schema; the structured body is the
    // identity-bearing object both utterances point at.
    let cs = proposition_schema();
    let body = serde_json::json!({"subject":"omega_lambda","num":13,"den":19,"value":"13/19"});

    let a = structure(
        "the dark-energy fraction is thirteen nineteenths",
        "en",
        "claude",
        &cs,
        &body,
    )
    .unwrap();
    let b = structure("Omega_Lambda equals 13/19", "en", "claude", &cs, &body).unwrap();

    assert_ne!(
        a.utterance.cid, b.utterance.cid,
        "paraphrases are distinct utterances — NL is byte projection"
    );
    assert_eq!(
        a.claim.cid, b.claim.cid,
        "same structured meaning ⇒ one claim CID — structure binds identity"
    );

    // The seam to the rest of the pipeline: the claim the bridge binds is the same
    // object the generator regenerates and the closure certifies.
    let c = omega_corpus();
    assert_eq!(
        a.claim.cid, c.proposition.cid,
        "the structured claim IS the regenerated, certified fact — one CID, end to end"
    );
}
