//! Through the microscope — the ground-floor walk-through.
//!
//! Starts from the most primitive act the substrate models: an observer looks
//! at something and writes down what they see. No prior knowledge is assumed;
//! the chain is built from that one reading upward. It exists to show, end to
//! end and in order, what each layer adds:
//!
//!   1. attestation   — the reading itself: instrument, sample, value, who vouches
//!   2. utterance     — the observer's sentence about it, sealed verbatim
//!   3. structuring   — "I read THIS sentence as THIS claim", attributed
//!   4. claim         — the structured proposition, grounded on the attestation
//!   5. witness check — recompute the claim's invariant by an independent route
//!   6. ground audit  — confirm the claim is about something that exists
//!
//! Run: `cargo run --example through_the_microscope`

use canon_d::bridge::{attestation_schema, ground_audit, needs_review, rat_witness, structure};
use canon_d::{FieldKind, Quantum, Schema};
use serde_json::json;
use std::collections::BTreeSet;

/// The claim under observation: a counted ratio (cells showing a trait, out of
/// total counted). Identity is the reduced-ratio coordinates and what it counts;
/// the witness is the reduced fraction as an exact "num/den" string, recomputed
/// from the count by an independent route. It is exact for ANY rational — unlike
/// an f64 witness, which verifies only for binary-representable values (3/5 by
/// luck, never 1/3) and is not pinned across serde_json versions.
fn count_ratio_schema() -> Schema {
    Schema::new("count_ratio", 1)
        .identity("sample", FieldKind::String)
        .identity("trait", FieldKind::String)
        .identity("hits", FieldKind::Integer)
        .identity("total", FieldKind::Integer)
        .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
        .witness("fraction", FieldKind::String)
        .optional("prose", FieldKind::String)
}

fn main() {
    // 1. ATTESTATION — the reading. An observer at a microscope counts: of 40
    //    cells on the slide, 24 are stained. This is a grounding leaf: reality
    //    is the witness, `vouched_by` records who looked.
    let reading = Quantum::seal(
        &attestation_schema(),
        &json!({
            "instrument": "microscope/AO-1230",
            "dataset": "slide-7",
            "locator": "field-3/stained-count",
            "value": 24.0,
            "vouched_by": "observer:nick",
            "unit": "cells",
        }),
    )
    .expect("seal attestation");
    println!("1. attestation   {}  (24 of 40 cells stained, observer-vouched)", short(&reading.cid));

    // 2-4. The observer writes a sentence; the bridge seals it, records the
    //       structuring proposal, and seals the structured claim grounded on the
    //       reading. 24/40 reduces to 3/5 in the claim's identity coordinates.
    let cs = count_ratio_schema();
    let claim_body = json!({
        "sample": "slide-7",
        "trait": "stained",
        "hits": 3,
        "total": 5,
        "grounds": [reading.cid],
        "fraction": rat_witness(3, 5).expect("3/5"),
    });
    let s = structure(
        "Three in five cells on slide 7 took the stain.",
        "en",
        "observer:nick",
        &cs,
        &claim_body,
    )
    .expect("structure");

    println!("2. utterance     {}  (the sentence, sealed verbatim)", short(&s.utterance.cid));
    println!("3. structuring   {}  (utterance -> claim, attributed to observer:nick)", short(&s.structuring.cid));
    println!("4. claim         {}  (sample=slide-7 trait=stained 3/5, grounded)", short(&s.claim.cid));

    // A paraphrase of the same observation: different sentence, same structure.
    let s2 = structure(
        "On slide 7, 60% of cells were stained (24/40).",
        "en",
        "observer:nick",
        &cs,
        &claim_body,
    )
    .expect("structure paraphrase");
    assert_ne!(s.utterance.cid, s2.utterance.cid);
    assert_eq!(s.claim.cid, s2.claim.cid);
    println!("   paraphrase     utterance {} != ; claim {} ==  (NL projects, structure binds)",
        short(&s2.utterance.cid), short(&s2.claim.cid));

    // 5. WITNESS CHECK — recompute the reduced fraction from the count by an
    //    independent route (reduce 3/5) and confirm it matches the sealed witness.
    //    The claim schema declares a witness, so it self-checks rather than going
    //    to review.
    let recomputed = json!({ "fraction": rat_witness(3, 5).expect("3/5") });
    let witness_ok = s.claim.verify_witness(&cs, &recomputed).expect("verify_witness");
    println!("5. witness        fraction recomputed (reduce 3/5) == sealed: {witness_ok}");
    assert!(witness_ok);
    assert!(!needs_review(&cs), "a claim with a witness self-checks");
    assert!(needs_review(&attestation_schema()), "the reading is witness-free; reality is its witness");

    // A wrong reading is caught here, not trusted: claim 3/5 but sealed witness 7/10.
    let bad = Quantum::seal(
        &cs,
        &json!({"sample":"slide-7","trait":"stained","hits":3,"total":5,
                "grounds":[reading.cid],"fraction":rat_witness(7,10).expect("7/10")}),
    )
    .expect("seal bad");
    let bad_ok = bad.verify_witness(&cs, &json!({"fraction": rat_witness(3, 5).expect("3/5")})).expect("verify");
    println!("   tamper check   claim says 3/5 but witness sealed 7/10 -> verifies: {bad_ok}");
    assert!(!bad_ok);

    // EXACT-RATIONAL CHECK — the reason the witness is a reduced "num/den" string,
    // not an f64. 1/3 has no finite binary float: an f64 witness would round it,
    // so the sealed and recomputed bytes need not agree and the value is unpinned
    // across serde_json. The reduced string is exact for ANY rational. A second
    // count of the same fraction (2/6) reduces to the same witness; an off-by-one
    // count (1/2) does not.
    let third = Quantum::seal(
        &cs,
        &json!({"sample":"slide-8","trait":"stained","hits":1,"total":3,
                "grounds":[reading.cid],"fraction":rat_witness(1,3).expect("1/3")}),
    )
    .expect("seal 1/3");
    let third_ok = third.verify_witness(&cs, &json!({"fraction": rat_witness(2, 6).expect("2/6")})).expect("verify");
    let third_bad = third.verify_witness(&cs, &json!({"fraction": rat_witness(1, 2).expect("1/2")})).expect("verify");
    println!("   exact 1/3      sealed 1/3 vs recompute 2/6 (reduces equal): {third_ok}; vs 1/2: {third_bad}");
    assert!(third_ok, "1/3 verifies exactly — an f64 witness could not represent it to compare");
    assert!(!third_bad);

    // 6. GROUND AUDIT — every grounding target must resolve to something sealed.
    //    The claim grounds on the reading (known); a claim grounding on nothing
    //    is "about nothing" and is flagged.
    let mut known = BTreeSet::new();
    known.insert(reading.cid.clone());
    let ungrounded = Quantum::seal(
        &cs,
        &json!({"sample":"slide-9","trait":"stained","hits":1,"total":2,
                "grounds":["deadbeef"],"fraction":rat_witness(1,2).expect("1/2")}),
    )
    .expect("seal ungrounded");
    let report = ground_audit("grounds", &[s.claim.clone(), ungrounded.clone()], &known);
    println!("6. ground audit   {} grounded, {} dangling ({} -> {})",
        2 - report.len(), report.len(),
        short(&report[0].claim), &report[0].missing);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].claim, ungrounded.cid);

    println!();
    println!("reading -> sentence -> structuring -> claim, each addressed; the");
    println!("claim self-checks against an independent route and resolves to a");
    println!("reading that exists. Nothing above rests on the model knowing what");
    println!("a cell is — only on the count, and on who vouched for it.");
}

fn short(cid: &str) -> &str {
    &cid[..8.min(cid.len())]
}
