//! Ground-up intake starting from a direct observation, witnessed by dimension.
//!
//! Companion to `through_the_microscope` (which witnesses a counted fraction).
//! Same chain, different witness route: here the claim is a dimensioned
//! quantity, and the independent check is its physical dimension recomputed
//! from the unit. No prior knowledge is assumed; the leaf is an instrument
//! reading. Strata, bottom to top:
//!
//!   attestation   the look through the microscope (reality is the witness)
//!   utterance     the lab note, verbatim bytes
//!   structuring   one emitter's reading of the note as a claim
//!   claim         a dimensioned quantity; dimension is the witness route
//!
//! Run: cargo run --example microscope

use canon_d::bridge::{attestation_schema, ground_audit, needs_review, structure};
use canon_d::{FieldKind, Quantum, Schema};
use serde_json::json;
use std::collections::BTreeSet;

/// A measured quantity. Identity: subject, exact magnitude (num/den — floats
/// do not bind addresses), unit, grounds. Witness: the dimension, recomputed
/// from the unit by a route that shares nothing with the canonicalizer.
fn quantity_schema() -> Schema {
    Schema::new("quantity", 1)
        .identity("subject", FieldKind::String)
        .identity("num", FieldKind::Integer)
        .identity("den", FieldKind::Integer)
        .identity("unit", FieldKind::String)
        .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
        .witness("dimension", FieldKind::String)
}

/// Independent witness route: unit string -> base dimension. A lookup table,
/// not a parser — it must share no machinery with the seal path.
fn dimension_of(unit: &str) -> &'static str {
    match unit {
        "um" | "mm" | "m" => "L",
        "s" | "ms" => "T",
        "g" | "kg" => "M",
        _ => "?",
    }
}

fn short(cid: &str) -> &str {
    &cid[..8.min(cid.len())]
}

fn main() {
    // 1. The look. One record in the world: this instrument, this slide, this
    //    field, this cell. The value is a reading of that record, not part of
    //    its identity — a corrected reading surfaces as a correction.
    let att = Quantum::seal(
        &attestation_schema(),
        &json!({
            "instrument": "olympus-cx23/400x",
            "dataset": "pond-sample-2026-06-12",
            "locator": "slide-3/field-2/cell-1",
            "value": 110.0,
            "unit": "um",
            "uncertainty": 5.0,
            "vouched_by": "nick",
        }),
    )
    .unwrap();
    println!(
        "attestation  {}  olympus-cx23/400x slide-3/field-2/cell-1  110 um +/-5",
        short(&att.cid)
    );
    println!(
        "             review tier: {} (no witness field; reality is the witness)",
        needs_review(&attestation_schema())
    );

    // 2 + 3. The note, and one emitter's reading of it. The note text is a
    //    projection; its utterance CID is a byte address, not a meaning address.
    //    The structured claim seals the magnitude exactly (110/1 um) and carries
    //    the dimension as its witness.
    let qs = quantity_schema();
    let claim_body = json!({
        "subject": "slide-3/field-2/cell-1 major-axis",
        "num": 110,
        "den": 1,
        "unit": "um",
        "grounds": [att.cid],
        "dimension": dimension_of("um"),
    });
    let s = structure(
        "Major axis of the largest cell in field 2: 110 um.",
        "en",
        "emitter:nick",
        &qs,
        &claim_body,
    )
    .unwrap();
    println!("utterance    {}  (the lab note, sealed verbatim)", short(&s.utterance.cid));
    println!("structuring  {}  (note -> claim, attributed to emitter:nick)", short(&s.structuring.cid));
    println!("claim        {}  (major-axis = 110/1 um, dimension L, grounded)", short(&s.claim.cid));

    // 4. Witness check. Recompute the dimension from the unit by the independent
    //    lookup and confirm it matches the sealed witness.
    let recomputed = json!({ "dimension": dimension_of("um") });
    let ok = s.claim.verify_witness(&qs, &recomputed).unwrap();
    println!("witness      dimension recomputed from unit == sealed: {ok}");
    assert!(ok);
    assert!(!needs_review(&qs), "a claim with a witness self-checks");

    // The dimension witness catches a unit/dimension mismatch — the substrate's
    // form of the Mars Climate Orbiter error. A claim that says "110 s" but
    // seals dimension L is internally inconsistent and fails the check; nothing
    // downstream gets to trust it.
    let mismatched = Quantum::seal(
        &qs,
        &json!({"subject":"x","num":110,"den":1,"unit":"s","grounds":[att.cid],
                "dimension":"L"}),
    )
    .unwrap();
    let caught = mismatched
        .verify_witness(&qs, &json!({ "dimension": dimension_of("s") }))
        .unwrap();
    println!("mismatch     claim says unit=s but sealed dimension=L -> verifies: {caught}");
    assert!(!caught);

    // 5. Ground audit. Every grounding target must resolve to something sealed.
    let mut known = BTreeSet::new();
    known.insert(att.cid.clone());
    let ungrounded = Quantum::seal(
        &qs,
        &json!({"subject":"y","num":1,"den":1,"unit":"m","grounds":["deadbeef"],
                "dimension":"L"}),
    )
    .unwrap();
    let report = ground_audit("grounds", &[s.claim.clone(), ungrounded.clone()], &known);
    println!(
        "ground audit {} grounded, {} dangling ({} -> {})",
        2 - report.len(),
        report.len(),
        short(&report[0].claim),
        &report[0].missing
    );
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].claim, ungrounded.cid);

    println!();
    println!("Same chain as through_the_microscope, witnessed by dimension instead");
    println!("of fraction: the unit binds the address and the dimension checks it.");
    println!("Only the reading and who vouched for it are taken on trust.");
}
