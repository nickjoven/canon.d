//! Shared e2e fixture: build the Ω_Λ = 13/19 corpus from the lab boundary to the
//! certain fact, using **only** `canon_d`'s public API — so the e2e tests also
//! validate that the public surface is sufficient to do real work.
#![allow(dead_code)]

use std::collections::BTreeSet;

use canon_d::{attestation_schema, project, signing_key, vouch, Closure, Memo, Quantum, Vouch};
use serde_json::{json, Value};

pub struct Corpus {
    pub anchor: Quantum,      // the lab attestation (boundary)
    pub program: Value,       // the generator's program term
    pub generator: Quantum,   // (program, inputs)
    pub proposition: Quantum, // the regenerated fact, 13/19
    pub assertion: Quantum,   // the back-link: proposition ← generator
    pub vouch: Vouch,         // a signature over the anchor
}

/// Deterministic by construction (content-addressing → identical CIDs every call).
pub fn omega_corpus() -> Corpus {
    let anchor = Quantum::seal(
        &attestation_schema(),
        &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                "value":0.6847,"vouched_by":"nick"}),
    )
    .unwrap();

    let v = vouch(&signing_key(&[7u8; 32]), &anchor.cid);

    let program = json!({"walk":"LRRLLLLL"}); // Stern-Brocot walk → 13/19
    let mut memo = Memo::new();
    let p = project(&mut memo, &program, &[], "omega_lambda", "harmonics").unwrap();

    Corpus {
        anchor,
        program,
        generator: p.generator,
        proposition: p.proposition,
        assertion: p.assertion,
        vouch: v,
    }
}

/// A single-ground body, for closure rules.
pub fn ground(cid: &str) -> BTreeSet<String> {
    [cid.to_string()].into_iter().collect()
}

/// Assemble the grounded closure: anchor (boundary) ← generator ← proposition.
/// `lazy` toggles stage+settle vs eager add — the result must be identical.
pub fn build_closure(c: &Corpus, lazy: bool) -> Closure {
    let mut cl = Closure::new();
    if lazy {
        cl.stage_anchor(&c.anchor.cid);
        cl.stage_rule(c.generator.cid.clone(), ground(&c.anchor.cid));
        cl.stage_rule(c.proposition.cid.clone(), ground(&c.generator.cid));
        cl.settle();
    } else {
        cl.add_anchor(&c.anchor.cid);
        cl.add_rule(c.generator.cid.clone(), ground(&c.anchor.cid));
        cl.add_rule(c.proposition.cid.clone(), ground(&c.generator.cid));
    }
    cl
}
