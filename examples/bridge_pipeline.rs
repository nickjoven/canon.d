//! End-to-end walk of the NLP↔CID bridge over one real claim (Ω_Λ = 13/19).
//!
//!   cargo run --example bridge_pipeline
//!
//! Utter (NL, projection) → Structure (schema-canonical claim, identity) →
//! Witness (independent self-check) → Ground (attestation leaf that touches
//! reality) → Audit (cross-audit + ground-audit). Names and prose never bind;
//! structure does.

use canon_d::{
    attestation_schema, cross_audit, ground_audit, needs_review, structure, FieldKind, Quantum,
    Schema,
};
use serde_json::json;
use std::collections::BTreeSet;

fn ratio_claim_schema() -> Schema {
    Schema::new("ratio_claim", 1)
        .identity("subject", FieldKind::String)
        .identity("num", FieldKind::Integer)
        .identity("den", FieldKind::Integer)
        .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid))) // edge targets bind identity
        .witness("value", FieldKind::Float) // the independent check
        .optional("label", FieldKind::String) // projection
        .optional("prose", FieldKind::String) // projection (the NL source)
}

fn short(cid: &str) -> &str {
    &cid[..16]
}

fn main() {
    // 1. GROUND — a leaf that touches reality. Witness-free by design; a human vouches.
    let att = Quantum::seal(
        &attestation_schema(),
        &json!({
            "instrument": "Planck", "dataset": "2018 TT,TE,EE+lowE+lensing",
            "locator": "Table 2: Omega_Lambda", "value": 0.6847,
            "uncertainty": 0.0073, "vouched_by": "nick", "source_url": "arXiv:1807.06209"
        }),
    )
    .unwrap();
    println!("attestation   {}  (witness-free, needs human vouch: {})", short(&att.cid), needs_review(&attestation_schema()));

    // 2. STRUCTURE — two human framings of one claim, each from a different NL utterance.
    let cs = ratio_claim_schema();
    let body = json!({
        "subject": "omega_lambda", "num": 13, "den": 19,
        "grounds": [att.cid], "value": 0.6842,
        "label": "Stern-Brocot depth-6 forcing",
        "prose": "the dark-energy fraction is thirteen nineteenths"
    });
    let a = structure("the dark-energy fraction is thirteen nineteenths", "en", "claude", &cs, &body).unwrap();
    let b = structure("Omega_Lambda = 13/19", "en", "claude",
        &cs, &json!({"subject":"omega_lambda","num":13,"den":19,"grounds":[att.cid],
                     "value":0.6842,"label":"Farey mediant 13/19"})).unwrap();

    println!("utterance A   {}", short(&a.utterance.cid));
    println!("utterance B   {}   (distinct NL — paraphrases do not collapse)", short(&b.utterance.cid));
    println!("claim A       {}", short(&a.claim.cid));
    println!("claim B       {}   <- SAME node: NL is projection, structure is identity", short(&b.claim.cid));
    assert_eq!(a.claim.cid, b.claim.cid);

    // 3. WITNESS — independent recomputation 13/19 = 0.6842 must agree with the form.
    let ok = a.claim.verify_witness(&cs, &json!({"value": 0.6842})).unwrap();
    let bad = a.claim.verify_witness(&cs, &json!({"value": 0.7000})).unwrap();
    println!("witness self-audit: correct value agrees={ok}, wrong value agrees={bad}");

    // 4. CROSS-AUDIT — plant a buggy emitter that forgot to reduce (26/38). Dedup
    //    sees two distinct forms; the witness route catches the under-merge.
    let buggy = Quantum::seal(&cs, &json!({
        "subject":"omega_lambda","num":26,"den":38,"grounds":[att.cid],"value":0.6842
    })).unwrap();
    let conflicts = cross_audit(&cs, &[a.claim.clone(), buggy.clone()]).unwrap();
    println!("cross-audit (reduced 13/19 vs unreduced 26/38): {} conflict(s)", conflicts.len());
    for c in &conflicts {
        println!("  - {c:?}");
    }

    // 5. GROUND-AUDIT — a claim grounded in a nonexistent CID is "about nothing".
    let dangling = Quantum::seal(&cs, &json!({
        "subject":"floating","num":1,"den":2,"grounds":["deadbeef"],"value":0.5
    })).unwrap();
    let mut known = BTreeSet::new();
    known.insert(att.cid.clone());
    let ungrounded = ground_audit("grounds", &[a.claim.clone(), dangling], &known);
    println!("ground-audit: {} ungrounded claim(s)", ungrounded.len());
    for u in &ungrounded {
        println!("  - claim {} grounds on missing {}", short(&u.claim), u.missing);
    }

    println!("\nthe reviewed entry is the grounded closure that survives the audit.");
}
