//! Demo: dedup by *entailment*, not string/embedding match — skip the remote call.
//!
//!   cargo run --example skip_the_call
//!
//! Documented failure (baseline): "systematic failures in basic two-hop reasoning
//! — combining only two facts" — Song, Han & Goodman, "Large Language Model
//! Reasoning Failures," arXiv:2602.06176 §4.1 (2026). Recognizing that a held fact
//! *entails* the next one is exactly that two-hop step; the gate makes it on
//! implication, provably (interval containment, exact).

use std::collections::BTreeSet;

use canon_d::{dedup_gate, gate::Candidate, proposition_schema, Claim, Disposition, Quantum, Rat, RatInterval};
use serde_json::json;

fn seal_ratio(subject: &str, n: i64, d: i64) -> Quantum {
    Quantum::seal(
        &proposition_schema(),
        &json!({"subject":subject,"num":n,"den":d,"value":Rat::new(n,d).unwrap().reduced_string()}),
    )
    .unwrap()
}
fn point(n: i64, d: i64) -> RatInterval {
    RatInterval::point(Rat::new(n, d).unwrap())
}

fn main() {
    // Known corpus: Ω_Λ = 13/19 is already certified.
    let omega = seal_ratio("omega_lambda", 13, 19);
    let known_cids: BTreeSet<String> = [omega.cid.clone()].into_iter().collect();
    let known_claims = vec![Claim { cid: omega.cid.clone(), interval: point(13, 19) }];

    // A downstream agent is about to compute three facts and would, naively, make
    // three remote calls.
    let candidates = vec![
        Candidate { label: "Ω_Λ (exact, re-requested)".into(), cid: omega.cid.clone(), interval: point(13, 19) },
        Candidate {
            label: "Ω_Λ ∈ (0.6, 0.7) (a coarse bound)".into(),
            cid: "would-be-cid-coarse".into(),
            interval: RatInterval::new(Rat::new(3, 5).unwrap(), Rat::new(7, 10).unwrap()).unwrap(),
        },
        Candidate { label: "Ω_b = 1/19 (genuinely new)".into(), cid: "would-be-cid-omega-b".into(), interval: point(1, 19) },
    ];

    let n = candidates.len();
    let report = dedup_gate(&candidates, &known_cids, &known_claims);

    println!("Agent wants {n} facts. Naively → {n} remote calls.\n");
    for (label, disp) in &report.items {
        let verdict = match disp {
            Disposition::Known { .. } => "SKIP  memo hit (exact match)        ",
            Disposition::Entailed { .. } => "SKIP  entailed by a stronger fact   ",
            Disposition::Novel => "CALL  genuinely new                 ",
        };
        println!("  {verdict}  {label}");
    }
    println!("\n→ {} of {n} calls avoided. Remote loops: {} (not {n}).", report.skipped, report.novel.len());
    println!("\nThe interesting skip: a cache keyed on identity would RE-COMPUTE");
    println!("\"Ω_Λ ∈ (0.6,0.7)\" — we skipped it because 13/19 already *entails* it,");
    println!("and we can prove the skip was safe (interval containment, exact).");
}
