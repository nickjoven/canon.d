//! Demo: verify a fact you didn't compute — portable, tamper-evident,
//! reputation-free provenance, then empirically validated against reality.
//!
//!   cargo run --example verify_what_you_didnt_compute
//!
//! Documented failure (baseline): "LLMs struggle even in assessing reasoning
//! process … an arguably easier task than generation" — Song, Han & Goodman,
//! arXiv:2602.06176 §4.3 (2026). The model can't tell a wrong derivation from a
//! right one; here the check is externalized — re-verify without trusting the
//! producer, then reconcile against the measurement the value claims.

use canon_d::{
    attestation_schema, consensus_root, export, import, project, proposition_schema,
    reconcile_gate, Bundle, BundleEntry, Memo, Quantum, Rule, SchemaKind, Tolerance,
    TransparencyLog,
};
use serde_json::json;

fn main() {
    // --- Party A builds a small corpus: Ω_Λ = 13/19 ← mediant walk ← Planck ---
    let anchor = Quantum::seal(
        &attestation_schema(),
        &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                "value":0.6847,"uncertainty":0.0073,"vouched_by":"nick"}),
    )
    .unwrap();
    let program = json!({"walk":"LRRLLLLL"}); // Stern-Brocot → 13/19
    let mut memo = Memo::new();
    let proj = project(&mut memo, &program, &[], "omega_lambda", "harmonics").unwrap();

    let bundle = export(
        vec![
            BundleEntry::of(SchemaKind::Attestation, &anchor),
            BundleEntry::of(SchemaKind::Generator, &proj.generator),
            BundleEntry::of(SchemaKind::Proposition, &proj.proposition),
            BundleEntry::of(SchemaKind::Assertion, &proj.assertion),
        ],
        vec![anchor.cid.clone()],
        vec![
            Rule {
                head: proj.generator.cid.clone(),
                body: vec![anchor.cid.clone()],
            },
            Rule {
                head: proj.proposition.cid.clone(),
                body: vec![proj.generator.cid.clone()],
            },
        ],
        &TransparencyLog::new(),
        vec![],
    );

    let wire = serde_json::to_string(&bundle).unwrap();
    println!("Party A ships a {}-byte bundle.", wire.len());
    println!("  consensus root: {}\n", &bundle.consensus_root[..16]);

    // --- Party B imports & verifies, having computed nothing ---
    let received: Bundle = serde_json::from_str(&wire).unwrap();
    let loaded = import(&received).expect("verified");
    println!(
        "Party B imported it — re-verified every quantum, replayed the log, rebuilt the closure."
    );
    println!(
        "  agree on the entire knowledge state by ONE hash? {}",
        consensus_root(&loaded.closure) == bundle.consensus_root
    );
    println!(
        "  Ω_Λ certain in B's substrate (B computed nothing)? {}",
        loaded.closure.is_certain(&proj.proposition.cid)
    );
    println!("  (reputation-free: this verifies the same whether A is famous or obscure.)\n");

    // --- Tamper one field → rejected ---
    let mut forged = received.clone();
    forged.entries[2].body["value"] = json!("99/1"); // alter the fact's witness
    match import(&forged) {
        Ok(_) => println!("  TAMPER ACCEPTED — bug!"),
        Err(e) => println!("  Flipped one field → import REJECTED: {e}\n"),
    }

    // --- Empirical: does the imported fact agree with the measurement? ---
    let omega = loaded.certain_fact(&proj.proposition.cid).unwrap();
    let ok = reconcile_gate(&[(omega, &anchor)], &Tolerance::default()).unwrap();
    println!("Reconcile 13/19 vs Planck 0.6847 ± 0.0073:");
    println!(
        "  {} at {:.3}σ  → gate {:?}",
        ok.verdicts[0].1.label(),
        ok.verdicts[0].1.z(),
        ok.outcome
    );

    // --- A wrong derived value → Falsified → blocked ---
    let wrong = Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":1,"den":2,"value":"1/2"}),
    )
    .unwrap();
    let bad = reconcile_gate(&[(&wrong, &anchor)], &Tolerance::default()).unwrap();
    println!("Reconcile 1/2 vs Planck:");
    println!(
        "  {} at {:.1}σ  → gate {:?} (exit {})",
        bad.verdicts[0].1.label(),
        bad.verdicts[0].1.z(),
        bad.outcome,
        bad.outcome.exit_code()
    );
}
