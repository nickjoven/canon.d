//! E2E-3 — the lifecycle journey (`tests/PLAN.md`): the certain core evolving
//! through a realistic edit history, asserted only against the public API.
//!
//! A loose incumbent stands alone, then a stronger forced claim **dethrones** it by
//! subsumption; certainty **flows down the order** from the single strongest anchor;
//! **retraction** shrinks the certain set non-monotonically; and a **contested**
//! fact can no longer certify what rested on it. The thread: the certain core is
//! honest through time — a stronger claim dissipates the weaker, certainty is
//! borrowed (withdraw the strong fact and its coarsenings lose their certificate),
//! and the cautious core excludes a contested fact rather than forcing a winner.

mod common;
use common::omega_claims;

use std::collections::BTreeSet;

use canon_d::{entailment_edges, maximal_antichain, redundant, Closure};

fn certified_omega() -> Closure {
    // Wire the entailment edges as grounding rules (Q ⟸ {P} for each P ⊊ Q), then
    // certify ONLY the strongest fact — its coarsenings follow for free.
    let claims = omega_claims();
    let mut cl = Closure::new();
    for (weaker, stronger) in entailment_edges(&claims) {
        cl.add_rule(weaker, [stronger].into_iter().collect());
    }
    cl.add_anchor("precise");
    cl
}

/// Step 1 — Incumbent then dethroning. The lone loose claim is irredundant (nothing
/// stronger exists). When the precise/approx/loose trio is present, the forced
/// precise claim entails both coarsenings, making them redundant — and the maximal
/// antichain collapses to just the strongest. Forcing *acts*: it dethrones.
#[test]
fn incumbent_alone_then_dethroned_by_a_stronger_claim() {
    let all = omega_claims();
    let loose = all[2].clone();

    // The incumbent alone: nothing stronger, so nothing is redundant.
    assert!(
        redundant(std::slice::from_ref(&loose)).is_empty(),
        "a lone incumbent is not redundant — no stronger claim has arrived"
    );

    // The full trio: precise entails approx and loose, dethroning both.
    let red = redundant(&all);
    let want: BTreeSet<String> =
        ["approx", "loose"].into_iter().map(String::from).collect();
    assert_eq!(red, want, "the coarsenings are dethroned by the precise claim");
    assert_eq!(
        maximal_antichain(&all),
        vec!["precise".to_string()],
        "the minimal consensus is just the strongest — the rest is derivable"
    );
}

/// Step 2 — Certainty flows DOWN the order. Certify the single strongest fact via an
/// anchor; the coarsenings become certain for free through the entailment rules.
#[test]
fn certainty_flows_down_from_the_single_anchor() {
    let cl = certified_omega();
    assert!(cl.is_certain("precise"), "the anchored strongest fact is certain");
    assert!(cl.is_certain("approx"), "approx is certain for free — entailed by precise");
    assert!(cl.is_certain("loose"), "loose is certain for free — entailed transitively");
}

/// Step 3 — Retraction is non-monotone. Withdraw the strong anchor and its entire
/// blast radius decertifies: the coarsenings' certainty was *borrowed* from precise.
#[test]
fn retracting_the_strong_anchor_decertifies_the_borrowers() {
    let mut cl = certified_omega();
    assert!(cl.is_certain("approx") && cl.is_certain("loose"), "certain before retraction");

    cl.retract("precise");

    assert!(!cl.is_certain("precise"), "the withdrawn anchor is no longer certain");
    assert!(
        !cl.is_certain("approx"),
        "approx loses certainty — it had no grounding but the now-withdrawn precise"
    );
    assert!(!cl.is_certain("loose"), "loose loses certainty for the same reason");
}

/// Step 4 — Contesting excludes and propagates. A contested fact is not certain and
/// cannot certify what rested on it — while an unrelated anchor is untouched. Honest
/// certainty is the cautious core, not a forced winner.
#[test]
fn contesting_the_strong_fact_excludes_it_and_what_rested_on_it() {
    let mut cl = certified_omega();

    // A separate, unrelated certain anchor — to show contesting is targeted, not global.
    cl.add_anchor("unrelated");
    assert!(cl.is_certain("unrelated"), "the unrelated anchor is certain");

    cl.mark_contested("precise");

    assert!(!cl.is_certain("precise"), "a contested fact is excluded from the certain core");
    assert!(
        !cl.is_certain("approx"),
        "what rested on the contested fact is not certain either"
    );
    assert!(!cl.is_certain("loose"), "the contested fact cannot certify its coarsenings");
    assert!(cl.is_certain("unrelated"), "the unrelated anchor stays certain — contesting is targeted");
}
