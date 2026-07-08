//! CI/CD gates over the substrate (`COST_DEPTH.md`). They **avoid** work (dedup)
//! and **validate** it (reconcile). Exit codes follow the `scripts/drift`
//! convention — `0 = clean`, `1 = violation`.
//!
//! The dedup gate is the biggest energy lever: it answers *"do we already have
//! this, or something that entails it?"* **before** any compute or remote
//! round-trip, using only the candidate's cheap descriptor (its CID + interval) —
//! not a full derivation.

use std::collections::BTreeSet;

use crate::quantum::Quantum;
use crate::reconcile::{reconcile_quanta, Agreement, ReconcileError, Tolerance};
use crate::subsume::{Claim, RatInterval};

// ---------------------------------------------------------------------------
// Dedup / memo gate
// ---------------------------------------------------------------------------

/// A cheap descriptor of a fact you're *about to* compute — enough to ask the
/// dedup question before paying. The `cid` is the canonical address of the target
/// fact (cheap: canonicalize its subject+value); the `interval` is its value/bound
/// for the subsumption check.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub label: String,
    pub cid: String,
    pub interval: RatInterval,
}

/// What the dedup gate decided for a candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Exact CID already present — a memo hit. Skip.
    Known { by: String },
    /// A stronger known fact entails it (subsumption). Skip.
    Entailed { by: String },
    /// Genuinely new — compute it (the only candidate for a remote loop).
    Novel,
}

/// The dedup gate's result. It never *fails* — its value is `skipped` (the
/// computations, and remote loops, you avoided) and `novel` (the work list).
#[derive(Debug, Clone)]
pub struct DedupReport {
    pub items: Vec<(String, Disposition)>,
    pub novel: Vec<String>,
    pub skipped: usize,
}

/// Classify each candidate against what's already known. `known_cids` gives exact
/// (memo) dedup; `known_claims` gives subsumption dedup (a strictly stronger known
/// fact makes the candidate redundant).
pub fn dedup_gate(
    candidates: &[Candidate],
    known_cids: &BTreeSet<String>,
    known_claims: &[Claim],
) -> DedupReport {
    let mut items = Vec::with_capacity(candidates.len());
    let mut novel = Vec::new();
    let mut skipped = 0usize;
    for c in candidates {
        let disp = if known_cids.contains(&c.cid) {
            skipped += 1;
            Disposition::Known { by: c.cid.clone() }
        } else if let Some(k) = known_claims
            .iter()
            .find(|k| k.interval.strictly_subsumes(&c.interval))
        {
            skipped += 1;
            Disposition::Entailed { by: k.cid.clone() }
        } else {
            novel.push(c.label.clone());
            Disposition::Novel
        };
        items.push((c.label.clone(), disp));
    }
    DedupReport {
        items,
        novel,
        skipped,
    }
}

// ---------------------------------------------------------------------------
// σ-reconciliation gate
// ---------------------------------------------------------------------------

/// A gate's CI verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateOutcome {
    Pass,
    Warn,
    Fail,
}

impl GateOutcome {
    /// `0` clean (Pass/Warn), `1` violation (Fail) — the drift-suite convention.
    pub fn exit_code(&self) -> i32 {
        match self {
            GateOutcome::Fail => 1,
            _ => 0,
        }
    }
}

/// Per-fact verdicts and the aggregate gate outcome.
#[derive(Debug, Clone)]
pub struct ReconcileReport {
    pub verdicts: Vec<(String, Agreement)>,
    pub falsified: Vec<String>,
    pub tension: Vec<String>,
    pub outcome: GateOutcome,
}

/// Reconcile each `(derived, attestation)` pair against its measurement. Any
/// `Falsified` ⇒ `Fail` (block the merge); any `Tension` ⇒ `Warn` (flag); else
/// `Pass`.
pub fn reconcile_gate(
    pairs: &[(&Quantum, &Quantum)],
    tol: &Tolerance,
) -> Result<ReconcileReport, ReconcileError> {
    let mut verdicts = Vec::with_capacity(pairs.len());
    let mut falsified = Vec::new();
    let mut tension = Vec::new();
    for (derived, attestation) in pairs {
        let a = reconcile_quanta(derived, attestation, tol)?;
        match a {
            Agreement::Falsified { .. } => falsified.push(derived.cid.clone()),
            Agreement::Tension { .. } => tension.push(derived.cid.clone()),
            Agreement::Consistent { .. } => {}
        }
        verdicts.push((derived.cid.clone(), a));
    }
    let outcome = if !falsified.is_empty() {
        GateOutcome::Fail
    } else if !tension.is_empty() {
        GateOutcome::Warn
    } else {
        GateOutcome::Pass
    };
    Ok(ReconcileReport {
        verdicts,
        falsified,
        tension,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{attestation_schema, proposition_schema, Quantum, Rat};
    use serde_json::json;

    fn ival(n: i64, d: i64) -> RatInterval {
        RatInterval::point(Rat::new(n, d).unwrap())
    }
    fn band(lo: (i64, i64), hi: (i64, i64)) -> RatInterval {
        RatInterval::new(Rat::new(lo.0, lo.1).unwrap(), Rat::new(hi.0, hi.1).unwrap()).unwrap()
    }
    fn ratio(n: i64, d: i64) -> Quantum {
        Quantum::seal(
            &proposition_schema(),
            &json!({"subject":"omega_lambda","num":n,"den":d,"value":Rat::new(n,d).unwrap().reduced_string()}),
        )
        .unwrap()
    }
    fn planck() -> Quantum {
        Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                    "value":0.6847,"uncertainty":0.0073,"vouched_by":"nick"}),
        )
        .unwrap()
    }

    #[test]
    fn dedup_skips_known_and_entailed_computes_novel() {
        let known_cids: BTreeSet<String> = ["already-sealed".to_string()].into_iter().collect();
        let known_claims = vec![Claim {
            cid: "precise".into(),
            interval: ival(13, 19),
        }];

        let candidates = vec![
            // exact CID present → Known
            Candidate {
                label: "dup".into(),
                cid: "already-sealed".into(),
                interval: ival(1, 2),
            },
            // [0.6,0.7] is entailed by the stronger 13/19 → Entailed
            Candidate {
                label: "coarse".into(),
                cid: "new1".into(),
                interval: band((3, 5), (7, 10)),
            },
            // 1/3 is not subsumed → Novel
            Candidate {
                label: "novel".into(),
                cid: "new2".into(),
                interval: ival(1, 3),
            },
        ];

        let r = dedup_gate(&candidates, &known_cids, &known_claims);
        assert_eq!(
            r.skipped, 2,
            "known + entailed are skipped — compute avoided"
        );
        assert_eq!(r.novel, vec!["novel".to_string()]);
        assert!(matches!(r.items[0].1, Disposition::Known { .. }));
        assert!(matches!(&r.items[1].1, Disposition::Entailed { by } if by == "precise"));
        assert!(matches!(r.items[2].1, Disposition::Novel));
    }

    #[test]
    fn reconcile_gate_passes_consistent_blocks_falsified() {
        let p = planck();
        // 13/19 ≈ 0.07σ → consistent → Pass
        let ok = ratio(13, 19);
        let pass = reconcile_gate(&[(&ok, &p)], &Tolerance::default()).unwrap();
        assert_eq!(pass.outcome, GateOutcome::Pass);
        assert_eq!(pass.outcome.exit_code(), 0);

        // 1/2 = 0.5 ≈ 25σ → falsified → Fail (exit 1)
        let bad = ratio(1, 2);
        let fail = reconcile_gate(&[(&ok, &p), (&bad, &p)], &Tolerance::default()).unwrap();
        assert_eq!(fail.outcome, GateOutcome::Fail);
        assert_eq!(fail.outcome.exit_code(), 1);
        assert_eq!(fail.falsified, vec![bad.cid.clone()]);
    }

    #[test]
    fn reconcile_gate_warns_on_tension() {
        let p = planck();
        let near = ratio(12, 17); // ≈ 2.9σ → tension
        let r = reconcile_gate(&[(&near, &p)], &Tolerance::default()).unwrap();
        assert_eq!(r.outcome, GateOutcome::Warn);
        assert_eq!(r.outcome.exit_code(), 0, "tension flags but does not block");
        assert_eq!(r.tension.len(), 1);
    }
}
