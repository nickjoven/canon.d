//! E2E-4 — **scale & laziness** (`tests/PLAN.md`): batching, flooring, and
//! calibration change the *path* to the answer and its *cost*, but never the final
//! answer. The settled certain core — and the one hash that commits to it, the
//! `consensus_root` — is invariant under how lazily you got there.
//!
//! The graph here is a synthetic uniform chain `a → P ⟸ {a} → Q ⟸ {P} → R ⟸ {Q}`,
//! built inline from CID-string nodes (the `ground` helper supplies the single-cid
//! bodies). It is deliberately *not* the Ω corpus: this journey is about the
//! closure's lazy machinery (stage/settle, floored settle, calibrated floor), which
//! a uniform chain exercises cleanly — every hop attenuates by the same ratio, so
//! the spectrum is geometric and the budget/floor arithmetic is exact and legible.

mod common;
use common::ground;

use canon_d::{calibrate_floor, consensus_root, floor_for_budget, Closure};

use std::collections::BTreeSet;

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// The synthetic chain `a → P → Q → R`, built eagerly. The reference answer every
/// lazy path must reproduce.
fn eager_chain() -> Closure {
    let mut c = Closure::new();
    c.add_anchor("a");
    c.add_rule("P".into(), ground("a"));
    c.add_rule("Q".into(), ground("P"));
    c.add_rule("R".into(), ground("Q"));
    c
}

/// The same chain's *rules* primed WITHOUT the anchor — nothing is certain yet, but
/// the dependent graph is fully populated (so amplitude/spectrum/floor all see the
/// real reach). The anchor arrives later via the lazy paths under test.
fn primed_chain() -> Closure {
    let mut c = Closure::new();
    c.add_rule("P".into(), ground("a"));
    c.add_rule("Q".into(), ground("P"));
    c.add_rule("R".into(), ground("Q"));
    assert!(c.certain().is_empty(), "no anchor yet → nothing certain");
    c
}

/// **(1) Warm-start batching == eager.** Build the closure two ways — eager
/// add_anchor/add_rule vs stage everything then a single settle() — and assert the
/// certain core, and the one hash that commits to it, are identical. Pending tracks
/// the deferral honestly: non-zero after staging, zero after settle.
#[test]
fn warm_start_batching_equals_eager() {
    let eager = eager_chain();

    let mut lazy = Closure::new();
    lazy.stage_anchor("a");
    lazy.stage_rule("P".into(), ground("a"));
    lazy.stage_rule("Q".into(), ground("P"));
    lazy.stage_rule("R".into(), ground("Q"));
    // staging records the deltas but cascades nothing: the answer is deferred …
    assert_eq!(lazy.pending(), 4, "four staged changes await settle");
    assert!(lazy.certain().is_empty(), "nothing cascaded before settle");

    // … one batched settle folds them all in.
    lazy.settle();
    assert_eq!(lazy.pending(), 0, "settle drains the batch");

    // The PATH differed (per-delta cascade vs one batched fold); the ANSWER does not.
    assert_eq!(lazy.certain(), eager.certain(), "batched settle == eager certain core");
    assert_eq!(*lazy.certain(), set(&["a", "P", "Q", "R"]));
    assert_eq!(
        consensus_root(&lazy),
        consensus_root(&eager),
        "and the consensus root — the hash two verifiers compare — is identical"
    );
}

/// **(2) Floored settle is a sound under-approximation with explicit pending.**
/// Prime the rules without the anchor, then stage the anchor and settle with a floor
/// that cuts the chain partway. The near nodes settle; the far ones are *deferred*,
/// not denied — `certain()` stays a subset of the eager core (never false
/// certainty), and the deferral is queryable via `pending()`. A full settle then
/// recovers the exact core.
#[test]
fn floored_settle_is_sound_under_approximation() {
    let eager = eager_chain();

    let mut c = primed_chain();
    c.stage_anchor("a");
    // amplitudes from `a` at attenuation 0.5: a=1.0, P=0.5 (≥0.3), Q=0.25 (<0.3) cut,
    // R behind the cut. So only a, P settle; Q, R are deferred.
    c.settle_floored(1.0, 0.5, 0.3);

    // near nodes certain …
    assert!(c.is_certain("a") && c.is_certain("P"), "above-floor near nodes settle");
    // … far nodes deferred, not wrongly denied …
    assert!(!c.is_certain("Q") && !c.is_certain("R"), "below-floor far nodes deferred");
    // … the deferral is EXPLICIT (no silent drift) …
    assert!(c.pending() > 0, "the cut frontier is pending, queryable");
    // … and certainty is a SOUND UNDER-APPROXIMATION: subset of eager, never false.
    assert!(
        c.certain().is_subset(eager.certain()),
        "floored certain ⊆ eager certain — the substrate never claims false certainty"
    );
    assert_eq!(*c.certain(), set(&["a", "P"]));

    // A full settle drains the deferred frontier to the EXACT eager core.
    c.settle();
    assert_eq!(c.pending(), 0, "settle drains the deferral");
    assert_eq!(c.certain(), eager.certain(), "full settle recovers the exact core");
    assert_eq!(*c.certain(), set(&["a", "P", "Q", "R"]));
    assert_eq!(
        consensus_root(&c),
        consensus_root(&eager),
        "and the recovered root equals the eager root — flooring changed only the path"
    );
}

/// **(3) Calibration drives the floor.** Read the amplitude spectrum a change at `a`
/// would induce, pick the floor that settles a fixed budget via `floor_for_budget`,
/// run the floored settle at it, and assert exactly the budgeted prefix is certain
/// (rest deferred). A full settle then recovers the exact core and root. The chain's
/// spectrum is geometric, so `calibrate_floor` honestly reports `separation ≈ 1` —
/// there is no clean coarse/fine gap to cut; the budget floor is the robust knob.
#[test]
fn calibration_drives_the_floor() {
    let eager = eager_chain();

    let mut c = primed_chain();
    let spectrum = c.amplitude_spectrum(&["a"], 1.0, 0.5);
    assert_eq!(spectrum.len(), 4, "a, P, Q, R all reachable — the full distribution");

    // A budget-2 floor: settle exactly the coarse prefix {a, P}, defer {Q, R}.
    let floor = floor_for_budget(&spectrum, 2);
    assert_eq!(
        spectrum.iter().filter(|&&amp| amp >= floor).count(),
        2,
        "the budget floor admits exactly 2 nodes of the spectrum"
    );

    c.stage_anchor("a");
    c.settle_floored(1.0, 0.5, floor);
    assert_eq!(*c.certain(), set(&["a", "P"]), "the budget floor settles exactly the coarse 2");
    assert!(c.pending() > 0, "the rest is deferred, explicitly");

    // Calibration reports honestly: a uniform/geometric chain has no distinguished
    // gap, so a gap-based floor would cut signal — separation ≈ 1 says "don't".
    let cal = calibrate_floor(&spectrum).unwrap();
    assert!(
        (cal.separation - 1.0).abs() < 1e-9,
        "uniform chain ⇒ separation ≈ 1 (no clean gap; budget floor is the right tool)"
    );

    // Full settle → exact core and the SAME consensus root as eager. The whole point:
    // calibration/flooring is about cost and path; the final answer is invariant.
    c.settle();
    assert_eq!(c.certain(), eager.certain(), "full settle recovers the exact core");
    assert_eq!(*c.certain(), set(&["a", "P", "Q", "R"]));
    assert_eq!(
        consensus_root(&c),
        consensus_root(&eager),
        "calibrated path, identical destination — the consensus root is invariant"
    );
}
