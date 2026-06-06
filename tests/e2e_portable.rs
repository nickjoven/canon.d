//! E2E-6 — portability + correct forward projection (the (A)+(B) vertical slice).
//!
//! The goal made real on one chain: take a harmonics fact (Ω_Λ = 13/19, grounded
//! in a mediant-walk generator, grounded in a Planck attestation), **export** it to
//! a portable bundle, round-trip it through JSON, **import** it into a *fresh*
//! substrate (which re-verifies every quantum, replays the log, rebuilds the
//! closure, and checks the consensus root), then author a **forward projection** —
//! a new generator computing Ω_matter = 1 − Ω_Λ = 6/19 — that grounds in the
//! *imported* Ω_Λ. A built-forward projection is **correct** iff it is *certain*
//! (grounded transitively back to the imported, certified Planck anchor) and
//! *exact* (an exact rational, not a lossy float).

mod common;
use common::{omega_corpus, trusted_log, Corpus};

use canon_d::{
    export, import, verify_regeneration, Bundle, BundleEntry, Memo, Rule, SchemaKind,
};
use serde_json::json;

/// Pack the Ω_Λ chain (anchor ← generator ← proposition, + the back-link
/// assertion, vouch, log) into a portable bundle.
fn omega_bundle(c: &Corpus) -> Bundle {
    let (log, _trusted) = trusted_log(c);
    export(
        vec![
            BundleEntry::of(SchemaKind::Attestation, &c.anchor),
            BundleEntry::of(SchemaKind::Generator, &c.generator),
            BundleEntry::of(SchemaKind::Proposition, &c.proposition),
            BundleEntry::of(SchemaKind::Assertion, &c.assertion),
        ],
        vec![c.anchor.cid.clone()],
        vec![
            Rule { head: c.generator.cid.clone(), body: vec![c.anchor.cid.clone()] },
            Rule { head: c.proposition.cid.clone(), body: vec![c.generator.cid.clone()] },
        ],
        &log,
        vec![c.vouch.clone()],
    )
}

#[test]
fn bundle_round_trips_and_imports_the_certified_fact() {
    let c = omega_corpus();
    let bundle = omega_bundle(&c);

    // a portable artifact: write to bytes (JSON) and read back in a fresh process
    let json = serde_json::to_string(&bundle).unwrap();
    let reloaded: Bundle = serde_json::from_str(&json).unwrap();

    // import into a fresh substrate — verifies quanta, replays log, rebuilds the
    // closure, and checks the consensus root (the integrity seal)
    let loaded = import(&reloaded).unwrap();

    // the harmonics fact is certain after import — grounded back to the Planck anchor
    assert!(
        loaded.certain_fact(&c.proposition.cid).is_some(),
        "Ω_Λ = 13/19 is certain in the freshly loaded substrate"
    );
}

#[test]
fn forward_projection_is_certain_and_exact() {
    let c = omega_corpus();
    let mut loaded = import(&omega_bundle(&c)).unwrap();
    assert!(loaded.closure.is_certain(&c.proposition.cid), "imported Ω_Λ is certain");

    // FORWARD PROJECTION: a new generator computing Ω_matter = 1 − Ω_Λ. Its input
    // is the IMPORTED Ω_Λ quantum — its value (13/19) is read from that fact, not
    // supplied by us, so the computation is bound to the input it cites. (Clone to
    // release the borrow on `loaded` before we mutate its closure below.)
    let imported = loaded
        .certain_fact(&c.proposition.cid)
        .expect("Ω_Λ is a certified, value-bearing fact")
        .clone();
    let fwd_program = json!({
        "op":"add",
        "args":[{"lit":[1,1]}, {"op":"mul","args":[{"lit":[-1,1]}, {"in":0}]}]
    });
    let inputs = [&imported];
    let mut memo = Memo::new();
    let fwd = canon_d::project(&mut memo, &fwd_program, &inputs, "omega_matter", "downstream").unwrap();

    // EXACT: 1 − 13/19 = 6/19, an exact rational (not a lossy float)
    assert_eq!(fwd.proposition.field("num").and_then(|v| v.as_i64()), Some(6));
    assert_eq!(fwd.proposition.field("den").and_then(|v| v.as_i64()), Some(19));
    assert_eq!(fwd.proposition.field("value").and_then(|v| v.as_str()), Some("6/19"), "exact, not float");

    // the regeneration link holds for the forward generator
    assert!(
        verify_regeneration(&fwd_program, &inputs, "omega_matter", "downstream", &fwd.proposition.cid).unwrap(),
        "the forward generator reproduces 6/19"
    );

    // CORRECT: ground it across the import boundary — fwd-generator ← imported Ω_Λ,
    // and Ω_matter ← fwd-generator — and it becomes certain, grounded transitively
    // back to the imported, certified Planck anchor.
    loaded.closure.add_rule(fwd.generator.cid.clone(), [c.proposition.cid.clone()].into_iter().collect());
    loaded.closure.add_rule(fwd.proposition.cid.clone(), [fwd.generator.cid.clone()].into_iter().collect());
    assert!(
        loaded.closure.is_certain(&fwd.proposition.cid),
        "the forward projection is certain — grounded back to the imported boundary"
    );
}

#[test]
fn import_rejects_a_corrupted_bundle() {
    let c = omega_corpus();

    // tampered consensus root → the rebuilt bulk won't match → rejected
    let mut bad_root = omega_bundle(&c);
    bad_root.consensus_root = "deadbeefdeadbeef".into();
    assert!(import(&bad_root).is_err(), "a tampered root is rejected");

    // tampered fact body (the witness value) → body-digest mismatch → rejected
    let mut bad_body = omega_bundle(&c);
    // entry[2] is the proposition; corrupt its witness value
    bad_body.entries[2].body["value"] = json!("99/1");
    assert!(import(&bad_body).is_err(), "a tampered body is rejected");
}
