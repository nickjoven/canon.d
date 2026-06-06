//! Floored, attenuating propagation — be lazy about the far-reaching.
//!
//! The closure's eager cascade pays the **full** transitive blast radius on every
//! change. Most changes are fine-grained and peripheral; paying global cost for
//! them is the waste. This module makes propagation **frequency-dependent and
//! floored**, the way the Stribeck lattice does it (`RESULTS.md`: "high-frequency
//! dissipates; the subharmonic propagates with negligible attenuation"):
//!
//! - A change carries an **amplitude** (its significance).
//! - It propagates to dependents, **attenuating** per hop. `attenuation` is the
//!   *channel*: near 1 = low channel = travels far; near 0 = high channel = dies
//!   locally.
//! - Propagation **stops below the `floor`** — the expensive far-reaching steps
//!   are never taken for a signal that has dissipated. The floor is checked
//!   *before* the work, so laziness is by construction.
//!
//! This is simultaneously (a) the cost bound — work ∝ nodes reached above floor,
//! not the whole graph; (b) the dissipation mechanism — fine/high-channel changes
//! attenuate locally, so they never gravitationally accrue at distant nodes; and
//! (c) the scale separation — a coarse (low-channel) node legitimately reaches
//! far, but only while above floor. The floor (and per-channel attenuation) is the
//! calibration knob: floor → 0 recovers the exact eager closure; a higher floor
//! buys laziness by dropping sub-floor far effects (a deliberate multi-resolution
//! approximation, like multigrid / adaptive mesh refinement / LOD).

use std::collections::BTreeMap;

use crate::lineage::{TypedEdge, GROUNDING_KINDS};

/// Build the dependent graph: `to → [from…]` over grounding edges. A change at a
/// node flows to whoever is grounded in it.
fn dependents(edges: &[TypedEdge]) -> BTreeMap<String, Vec<String>> {
    let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for e in edges {
        if GROUNDING_KINDS.contains(&e.kind.as_str()) {
            m.entry(e.to.clone()).or_default().push(e.from.clone());
        }
    }
    m
}

/// Propagate a change of `amplitude` from `source`, attenuating by `attenuation`
/// per hop, stopping below `floor`. Returns the affected nodes (including the
/// source) with the best amplitude each received — the *bounded* far-reaching set.
///
/// Cost is `O(reached-above-floor)`, not `O(graph)`: a fine signal touches almost
/// nothing; a coarse one reaches far but still terminates at the floor.
pub fn propagate(
    source: &str,
    amplitude: f64,
    edges: &[TypedEdge],
    attenuation: f64,
    floor: f64,
) -> BTreeMap<String, f64> {
    let deps = dependents(edges);
    let mut best: BTreeMap<String, f64> = BTreeMap::new();
    // best-amplitude-first frontier (a node's amplitude = max over paths)
    let mut frontier: Vec<(String, f64)> = vec![(source.to_string(), amplitude)];
    while let Some((node, amp)) = frontier.pop() {
        if amp < floor {
            continue;
        }
        if best.get(&node).is_some_and(|&prev| prev >= amp) {
            continue; // already reached at least this strongly
        }
        best.insert(node.clone(), amp);
        let next = amp * attenuation;
        if next >= floor {
            for d in deps.get(&node).into_iter().flatten() {
                frontier.push((d.clone(), next));
            }
        }
    }
    best
}

/// The **reach** of a node: how many nodes a change to it would touch with no
/// floor — i.e. the cost of the *eager* far-reaching cascade. This is what the
/// floor is gating; a high-reach (low-channel/gravitational) node is exactly the
/// one worth being lazy about.
pub fn reach(source: &str, edges: &[TypedEdge]) -> usize {
    propagate(source, 1.0, edges, 1.0, 0.0).len()
}

/// Assign a **hierarchy level** by grounding depth from the anchors: level 0 =
/// anchor (coarsest / lowest channel / farthest-reaching), increasing outward to
/// the fine, local leaves. A change at a low level reaches far; at a high level,
/// locally. (Unreachable nodes are omitted.)
pub fn levels(anchors: &[&str], edges: &[TypedEdge]) -> BTreeMap<String, usize> {
    let deps = dependents(edges);
    let mut level: BTreeMap<String, usize> = BTreeMap::new();
    let mut frontier: Vec<(String, usize)> = anchors.iter().map(|a| (a.to_string(), 0)).collect();
    while let Some((node, lvl)) = frontier.pop() {
        if level.get(&node).is_some_and(|&prev| prev <= lvl) {
            continue;
        }
        level.insert(node.clone(), lvl);
        for d in deps.get(&node).into_iter().flatten() {
            frontier.push((d.clone(), lvl + 1));
        }
    }
    level
}

/// A calibrated floor and how well it separates two populations.
#[derive(Debug, Clone, PartialEq)]
pub struct Calibration {
    /// The floor to use (Step 3 → Step 2's `settle_floored`).
    pub floor: f64,
    /// How much the chosen gap stands out vs the typical gap. `~1` = **no clean
    /// separation** (a uniform/geometric graph — flooring is lossy here, reported
    /// honestly); `≫1` = a clean coarse/fine split, so the floor sits in a real gap
    /// and drops only noise. This is the Arnold-tongue separatrix made into a number.
    pub separation: f64,
}

/// **Step 3 — calibrate the floor by the largest gap** in the amplitude spectrum:
/// the separatrix between the coarse population that should propagate far and the
/// fine noise that should dissipate. Returns `None` for fewer than two distinct
/// levels. Read `separation`: near 1 means there is no distinguished floor (don't
/// floor — it would cut signal); large means a genuine coarse/fine split.
pub fn calibrate_floor(spectrum: &[f64]) -> Option<Calibration> {
    let mut s: Vec<f64> = spectrum.iter().copied().filter(|x| *x > 0.0).collect();
    s.sort_by(|a, b| b.partial_cmp(a).unwrap());
    s.dedup();
    if s.len() < 2 {
        return None;
    }
    let ratios: Vec<f64> = s.windows(2).map(|w| w[0] / w[1]).collect();
    let (mut bi, mut bratio) = (0usize, 0.0f64);
    for (i, &r) in ratios.iter().enumerate() {
        if r > bratio {
            bratio = r;
            bi = i;
        }
    }
    let floor = (s[bi] * s[bi + 1]).sqrt(); // geometric mean of the bracketing pair
    let mut sorted = ratios.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[sorted.len() / 2];
    let separation = if median > 0.0 { bratio / median } else { bratio };
    Some(Calibration { floor, separation })
}

/// The floor that admits exactly `budget` nodes — the cost-constrained
/// (WQS / ket-opt) calibration. `budget ≥ |spectrum|` ⇒ `0.0` (exact);
/// `budget == 0` ⇒ above the max (settle nothing). Robust: needs no gap.
pub fn floor_for_budget(spectrum: &[f64], budget: usize) -> f64 {
    let mut s: Vec<f64> = spectrum.iter().copied().filter(|x| *x > 0.0).collect();
    s.sort_by(|a, b| b.partial_cmp(a).unwrap());
    if budget >= s.len() {
        return 0.0;
    }
    if budget == 0 {
        return s.first().map_or(1.0, |m| m * 2.0);
    }
    (s[budget - 1] * s[budget]).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calibrate_finds_a_clean_gap() {
        // a coarse plateau (1.0, 0.9, 0.85) and a fine band (0.2, 0.15, 0.1) with a
        // gap between — the floor lands in the gap, separation stands out.
        let c = calibrate_floor(&[1.0, 0.9, 0.85, 0.2, 0.15, 0.1]).unwrap();
        assert!(c.floor > 0.2 && c.floor < 0.85, "floor sits in the gap: {}", c.floor);
        assert!(c.separation > 2.0, "the gap clearly stands out: {}", c.separation);
    }

    #[test]
    fn uniform_spectrum_reports_no_separation() {
        // pure geometric decay: every gap is the same ratio → no distinguished cut.
        let c = calibrate_floor(&[1.0, 0.5, 0.25, 0.125, 0.0625]).unwrap();
        assert!((c.separation - 1.0).abs() < 1e-9, "uniform ⇒ separation ≈ 1 (flooring is lossy)");
    }

    #[test]
    fn calibrate_none_for_degenerate() {
        assert!(calibrate_floor(&[]).is_none());
        assert!(calibrate_floor(&[0.7]).is_none());
    }

    #[test]
    fn floor_for_budget_admits_exactly_k() {
        let spec = [1.0, 0.5, 0.25, 0.125];
        let f = floor_for_budget(&spec, 2);
        assert_eq!(spec.iter().filter(|&&a| a >= f).count(), 2, "exactly 2 above the floor");
        assert_eq!(floor_for_budget(&spec, 9), 0.0, "budget covers all ⇒ exact (floor 0)");
        assert!(floor_for_budget(&spec, 0) > 1.0, "budget 0 ⇒ floor above the max");
    }

    // a keystone `a`, then a line of consequences grounded outward: b←a, c←b, …
    fn line() -> Vec<TypedEdge> {
        ["b a", "c b", "d c", "e d"]
            .iter()
            .map(|s| {
                let (from, to) = s.split_once(' ').unwrap();
                TypedEdge { from: from.into(), to: to.into(), kind: "grounds".into() }
            })
            .collect()
    }

    #[test]
    fn low_channel_reaches_far_high_channel_stays_local() {
        let e = line();
        // low channel (atten 0.9): a change at the keystone reaches the whole line
        let lo = propagate("a", 1.0, &e, 0.9, 0.3);
        assert_eq!(lo.len(), 5, "low-channel/low-attenuation propagates outward");
        // high channel (atten 0.3): dies after ~1 hop above the same floor
        let hi = propagate("a", 1.0, &e, 0.3, 0.3);
        assert!(hi.len() < lo.len() && hi.contains_key("a"), "high-channel dissipates locally");
    }

    #[test]
    fn floor_bounds_cost_laziness() {
        let e = line();
        let full = reach("a", &e); // eager: the whole transitive set
        assert_eq!(full, 5);
        let floored = propagate("a", 1.0, &e, 0.5, 0.2).len();
        assert!(floored < full, "the floor stops the expensive far steps — fewer nodes touched");
    }

    #[test]
    fn amplitude_gates_reach() {
        let e = line();
        let big = propagate("a", 1.0, &e, 0.5, 0.1);
        let small = propagate("a", 0.2, &e, 0.5, 0.1);
        assert!(small.len() < big.len(), "a smaller change reaches less far at the same floor");
    }

    #[test]
    fn peripheral_change_is_local_for_free() {
        let e = line();
        // `e` is a leaf — nothing is grounded in it, so a change there touches only itself.
        assert_eq!(propagate("e", 1.0, &e, 0.9, 0.1).keys().collect::<Vec<_>>(), vec!["e"]);
        assert_eq!(reach("e", &e), 1, "a fine peripheral node has reach 1 — laziness by structure");
        assert!(reach("a", &e) > reach("e", &e), "the keystone is gravitational; the leaf is not");
    }

    #[test]
    fn levels_assign_the_hierarchy() {
        let e = line();
        let lv = levels(&["a"], &e);
        assert_eq!(lv["a"], 0, "anchor = coarsest / lowest channel");
        assert_eq!(lv["b"], 1);
        assert_eq!(lv["e"], 4, "leaf = finest / highest channel / most local");
    }
}
