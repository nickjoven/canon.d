//! Shared e2e fixture: build the Ω_Λ = 13/19 corpus from the lab boundary to the
//! certain fact, using **only** `canon_d`'s public API — so the e2e tests also
//! validate that the public surface is sufficient to do real work.
#![allow(dead_code)]

use std::collections::BTreeSet;

use canon_d::{
    attestation_schema, project, signing_key, vouch, Claim, Closure, Memo, Quantum, Rat,
    RatInterval, TransparencyLog, Vouch,
};
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

/// The Ω_Λ claim at three resolutions, for subsumption journeys:
/// `precise` 13/19 ⊊ `approx` [0.67,0.69] ⊊ `loose` [0.6,0.7].
pub fn omega_claims() -> Vec<Claim> {
    let r = |n: i64, d: i64| Rat::new(n, d).unwrap();
    vec![
        Claim {
            cid: "precise".into(),
            interval: RatInterval::point(r(13, 19)),
        },
        Claim {
            cid: "approx".into(),
            interval: RatInterval::new(r(67, 100), r(69, 100)).unwrap(),
        },
        Claim {
            cid: "loose".into(),
            interval: RatInterval::new(r(3, 5), r(7, 10)).unwrap(),
        },
    ]
}

/// A transparency log with the corpus's anchor and vouch recorded, plus the
/// trusted-signer set — ready for `audit_anchor`.
pub fn trusted_log(c: &Corpus) -> (TransparencyLog, Vec<String>) {
    let mut log = TransparencyLog::new();
    log.append(&c.anchor.cid);
    log.append(&c.vouch.signature);
    (log, vec![c.vouch.signer.clone()])
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
