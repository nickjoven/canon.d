//! Demo: multi-agent memory that can't silently drift — corroboration vs. a
//! canonicalizer bug, told apart *exactly*.
//!
//!   cargo run --example no_silent_drift
//!
//! Failure modes: (1) a naive dedup flags two independent derivations of one fact
//! as a conflict-to-resolve; (2) a canonicalizer bug silently enters a duplicate
//! "fact" into shared memory. Both handled correctly here.

use canon_d::{cross_audit, proposition_schema, CrossAuditConflict, Quantum};
use serde_json::json;

fn seal(num: i64, den: i64, witness: &str) -> Quantum {
    Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":num,"den":den,"value":witness}),
    )
    .unwrap()
}
fn short(c: &str) -> &str {
    &c[..16]
}

fn main() {
    let schema = proposition_schema();

    println!("== Two agents reach Ω_Λ = 13/19 by different derivation paths ==");
    let a = seal(13, 19, "13/19"); // agent A
    let b = seal(13, 19, "13/19"); // agent B, independent
    println!("  agent A → {}", short(&a.cid));
    println!("  agent B → {}", short(&b.cid));
    println!("  same CID? {}  → ONE node, corroborated (NOT a conflict to resolve)", a.cid == b.cid);
    let clean = cross_audit(&schema, &[a, b]).unwrap();
    println!("  cross-audit conflicts: {}", clean.len());
    println!("  (a string- or embedding-dedup might have flagged these as a clash.)\n");

    println!("== Agent C ships a canonicalizer bug: leaves 26/38 unreduced ==");
    let good = seal(13, 19, "13/19");
    let buggy = seal(26, 38, "13/19"); // SAME value, wrong (unreduced) form
    println!("  13/19 → {}", short(&good.cid));
    println!("  26/38 → {}  (different address — looks like a brand-new fact)", short(&buggy.cid));
    let conflicts = cross_audit(&schema, &[good, buggy]).unwrap();
    for c in &conflicts {
        if let CrossAuditConflict::UnderMerge { witness, cids } = c {
            println!("  ⚠ UnderMerge: one value {witness} resolved to {} forms → the bug is CAUGHT.", cids.len());
        }
    }
    println!("\nThe failure mode: 26/38 would have silently entered memory as a second");
    println!("\"fact.\" The dual-witness cross-audit (form vs. an independent value) made");
    println!("the silent drift loud — the one thing embedding-similarity memory can't do.");
}
