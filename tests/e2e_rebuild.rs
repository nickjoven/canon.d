//! E2E-5 — the rebuild-bit-identical contract (`ket/DESIGN.md`), at the system
//! level: *throw away everything but the addressed content; rebuild every artifact
//! bit-identically.* Plus the seam invariant unit tests can't reach — that one
//! fact's CID is the same object at every layer it passes through.

mod common;
use common::{build_closure, ground, omega_corpus};

use canon_d::{
    attestation_schema, consensus_root, generator_schema, proposition_schema, verify_regeneration,
    Closure,
};

#[test]
fn every_sealed_quantum_reverifies() {
    // The meta-contract, per node: each sealed quantum re-hashes to its address.
    let c = omega_corpus();
    assert!(c.anchor.verify(&attestation_schema()).unwrap());
    assert!(c.generator.verify(&generator_schema()).unwrap());
    assert!(c.proposition.verify(&proposition_schema()).unwrap());
}

#[test]
fn one_fact_cid_threads_every_layer() {
    // The seam invariant: the fact the generator PRODUCED is the fact the assertion
    // GROUNDS is the fact the closure CERTIFIES is the fact regeneration REPRODUCES.
    let c = omega_corpus();
    assert_eq!(
        c.assertion.field("proposition").and_then(|v| v.as_str()),
        Some(c.proposition.cid.as_str()),
        "the back-link grounds exactly the produced fact"
    );
    let cl = build_closure(&c, false);
    assert!(cl.is_certain(&c.proposition.cid), "the closure certifies that same CID");
    assert!(
        verify_regeneration(&c.program, &[], "omega_lambda", "harmonics", &c.proposition.cid).unwrap(),
        "and the generator reproduces that same CID"
    );
}

#[test]
fn rebuild_is_bit_identical_across_order_and_laziness() {
    let c = omega_corpus();
    let root = consensus_root(&build_closure(&c, false));

    // (a) lazy stage+settle == eager
    assert_eq!(root, consensus_root(&build_closure(&c, true)), "eager == lazy");

    // (b) different insertion order — rules before their grounds, anchor last
    let mut rev = Closure::new();
    rev.add_rule(c.proposition.cid.clone(), ground(&c.generator.cid));
    rev.add_rule(c.generator.cid.clone(), ground(&c.anchor.cid));
    rev.add_anchor(&c.anchor.cid);
    assert_eq!(root, consensus_root(&rev), "order-independent");

    // (c) rebuilt from scratch — content-addressing makes CIDs and root identical
    let c2 = omega_corpus();
    assert_eq!(c.proposition.cid, c2.proposition.cid, "rebuilt fact is bit-identical");
    assert_eq!(
        root,
        consensus_root(&build_closure(&c2, false)),
        "rebuilt consensus is bit-identical — the meta-contract, whole-system"
    );
}
