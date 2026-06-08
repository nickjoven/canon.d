//! The grounded closure — the certain core, computed incrementally.
//!
//! "Reviewed consensus = grounded closure" (`ket/DESIGN.md`). A proposition is in
//! the closure — **certain** — iff it has a justification *all* of whose grounds
//! are in the closure; the boundary (attestations / anchors) is given certain.
//! This is Datalog's least model over the grounding relation: Horn-only, PTIME,
//! order-independent (`SPINE.md` §6, and Horn-only per its §0 rigor register —
//! genuine disagreement is *surfaced* as `contested`, not collapsed into a forced
//! winner).
//!
//! ## The cost of certainty, made structural
//!
//! The asymmetry of this type *is* the answer to "what is the cost of certainty":
//!
//! - **Acquiring** it is cheap and monotone — [`Closure::add_anchor`] /
//!   [`Closure::add_justification`] fire only the newly satisfiable rules
//!   (semi-naive, incremental). This doubles as the warm-start **delta-fold**: a
//!   resuming session folds log deltas into the closure rather than recomputing.
//! - **Maintaining** it is expensive and non-monotone — [`Closure::retract`]
//!   removes the blast radius and re-derives it. Certainty is *borrowed* from the
//!   boundary; withdraw an anchor and everything grounded through it loses its
//!   certificate.
//! - At the **frontier** it is unattainable mechanically — a proposition with no
//!   resolving justification never enters the closure; it needs a human vouch (a
//!   boundary anchor), and a **contested** proposition is excluded. Honest
//!   certainty is the cautious core, not the cheap forced answer.

use std::collections::{BTreeMap, BTreeSet};

use crate::quantum::Quantum;

/// An incremental grounded closure.
#[derive(Debug, Default, Clone)]
pub struct Closure {
    /// Boundary anchors — axiomatically certain (their cost is the vouch, outside).
    boundary: BTreeSet<String>,
    /// proposition CID → its justifications (each a set of ground CIDs).
    rules: BTreeMap<String, Vec<BTreeSet<String>>>,
    /// ground CID → propositions that mention it (for incremental cascade).
    dependents: BTreeMap<String, BTreeSet<String>>,
    /// The certain core — the least model.
    admitted: BTreeSet<String>,
    /// Surfaced disagreement — excluded from the certain core by policy.
    contested: BTreeSet<String>,
    /// Changes recorded but not yet cascaded (the lazy/batched path). `settle`
    /// processes them; `pending` reports how many await.
    staged: BTreeSet<String>,
    /// The **cut frontier** from a floored settle: nodes a change reached *below*
    /// the floor, whose certainty was deliberately not recomputed. Everything
    /// behind them is transitively deferred. This is what keeps flooring from being
    /// silent drift — the deferral is explicit and recoverable (a full `settle`
    /// drains it to the exact result).
    deferred: BTreeSet<String>,
}

impl Closure {
    pub fn new() -> Self {
        Closure::default()
    }

    /// Add a boundary anchor (an attestation / vouched primary): immediately
    /// certain, then cascade anything it newly grounds.
    pub fn add_anchor(&mut self, cid: &str) {
        self.boundary.insert(cid.to_string());
        if !self.contested.contains(cid) && self.admitted.insert(cid.to_string()) {
            self.cascade(cid.to_string());
        }
    }

    /// Record a rule into the graph without admitting/cascading (shared by the
    /// eager and lazy paths).
    fn register(&mut self, head: &str, body: BTreeSet<String>) {
        for g in &body {
            self.dependents.entry(g.clone()).or_default().insert(head.to_string());
        }
        self.rules.entry(head.to_string()).or_default().push(body);
    }

    /// Register a justification rule `head ⟸ body`. If it fires now, admit the
    /// head and cascade. Incremental: only the head and its dependents are touched.
    pub fn add_rule(&mut self, head: String, body: BTreeSet<String>) {
        let fires = body.is_subset(&self.admitted);
        self.register(&head, body);
        if fires && !self.contested.contains(&head) && self.admitted.insert(head.clone()) {
            self.cascade(head);
        }
    }

    // --- Step 1: the lazy / batched path (defer, then settle once) ---

    /// Stage an anchor without cascading. Pairs with [`settle`](Self::settle).
    pub fn stage_anchor(&mut self, cid: &str) {
        self.boundary.insert(cid.to_string());
        self.staged.insert(cid.to_string());
    }

    /// Stage a rule without admitting/cascading. The change is recorded into the
    /// graph but its consequences are deferred — the warm-start delta-fold: stage
    /// every delta, settle once, instead of paying a cascade per delta.
    pub fn stage_rule(&mut self, head: String, body: BTreeSet<String>) {
        self.register(&head, body);
        self.staged.insert(head);
    }

    /// How many changes await settling — staged writes plus the deferred cut
    /// frontier. Non-zero ⇒ `certain()` is a **sound under-approximation** (every
    /// fact in it is certain, but some certain facts are deferred); call
    /// [`settle`](Self::settle) for the exact core. This is what keeps both the
    /// batched and the floored paths from being silent: deferred work is queryable.
    pub fn pending(&self) -> usize {
        self.staged.len() + self.deferred.len()
    }

    /// Process all pending changes (staged ∪ deferred) in one batched fixpoint,
    /// restricted to the affected region — cost ∝ the region touched. After
    /// `settle`, `certain()` is **exactly** the eager result and `pending()` is 0.
    pub fn settle(&mut self) {
        if self.staged.is_empty() && self.deferred.is_empty() {
            return;
        }
        let seeds: Vec<String> = self.staged.iter().chain(self.deferred.iter()).cloned().collect();
        // affected = seeds ∪ transitive dependents
        let mut affected: BTreeSet<String> = BTreeSet::new();
        let mut stack = seeds;
        while let Some(c) = stack.pop() {
            if !affected.insert(c.clone()) {
                continue;
            }
            if let Some(ds) = self.dependents.get(&c) {
                for d in ds {
                    if !affected.contains(d) {
                        stack.push(d.clone());
                    }
                }
            }
        }
        for c in &self.staged {
            if self.boundary.contains(c) && !self.contested.contains(c) {
                self.admitted.insert(c.clone());
            }
        }
        self.fixpoint(&affected);
        self.staged.clear();
        self.deferred.clear();
    }

    /// **Step 2 — floored settle.** Propagate certainty outward from the staged
    /// changes (plus any deferred frontier) at `amplitude`, attenuating per hop;
    /// nodes reached **at or above `floor`** are re-evaluated, nodes reached
    /// **below** it become the deferred cut frontier. Cost ∝ the above-floor reach,
    /// not the whole graph.
    ///
    /// Safety: this is a **sound under-approximation** — it never admits a fact
    /// whose grounds aren't already certain, so `certain()` ⊆ the exact core; it
    /// only *defers* certainty, and the deferral is explicit in `pending`. A
    /// subsequent [`settle`](Self::settle) (or `settle_floored` with `floor = 0`)
    /// recovers the exact result. Low attenuation (low channel) reaches far; high
    /// attenuation (fine) settles locally — the Stribeck dissipation, governing the
    /// closure.
    pub fn settle_floored(&mut self, amplitude: f64, attenuation: f64, floor: f64) {
        // staged anchors are certain at the source
        for c in &self.staged {
            if self.boundary.contains(c) && !self.contested.contains(c) {
                self.admitted.insert(c.clone());
            }
        }
        let seeds: Vec<String> = self.staged.iter().chain(self.deferred.iter()).cloned().collect();
        let mut best: BTreeMap<String, f64> = BTreeMap::new();
        let mut region: BTreeSet<String> = BTreeSet::new();
        let mut cut: BTreeSet<String> = BTreeSet::new();
        let mut frontier: Vec<(String, f64)> = seeds.iter().map(|s| (s.clone(), amplitude)).collect();
        while let Some((node, amp)) = frontier.pop() {
            if amp < floor {
                cut.insert(node); // the deferred frontier — do not recurse past it
                continue;
            }
            if best.get(&node).is_some_and(|&p| p >= amp) {
                continue;
            }
            best.insert(node.clone(), amp);
            region.insert(node.clone());
            let next = amp * attenuation;
            if let Some(ds) = self.dependents.get(&node) {
                for d in ds {
                    frontier.push((d.clone(), next));
                }
            }
        }
        self.fixpoint(&region);
        for c in &region {
            cut.remove(c); // reached above-floor by another path ⇒ not deferred
        }
        self.deferred = cut;
        self.staged.clear();
    }

    /// Admit any node in `region` whose rule fires, to fixpoint. Shared by the
    /// exact and floored settles.
    fn fixpoint(&mut self, region: &BTreeSet<String>) {
        loop {
            let mut changed = false;
            for p in region {
                if self.admitted.contains(p) || self.contested.contains(p) {
                    continue;
                }
                if self.rules.get(p).is_some_and(|rs| rs.iter().any(|b| b.is_subset(&self.admitted))) {
                    self.admitted.insert(p.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// The **reach** of a node over this closure's own graph: how many nodes a
    /// change to it would touch (itself + transitive dependents). This is the cost
    /// the floor will gate (Step 2) — a high-reach node is the gravitational one
    /// worth being lazy about.
    pub fn reach(&self, cid: &str) -> usize {
        let mut seen = BTreeSet::new();
        let mut stack = vec![cid.to_string()];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.clone()) {
                continue;
            }
            if let Some(ds) = self.dependents.get(&c) {
                for d in ds {
                    if !seen.contains(d) {
                        stack.push(d.clone());
                    }
                }
            }
        }
        seen.len()
    }

    /// Register a justification from an `assertion` quantum (`proposition` +
    /// `grounds`). This is the delta consumer for warm-start.
    pub fn add_justification(&mut self, assertion: &Quantum) {
        let head = assertion.field("proposition").and_then(|v| v.as_str());
        let grounds = assertion.field("grounds").and_then(|v| v.as_array());
        if let (Some(head), Some(grounds)) = (head, grounds) {
            let body: BTreeSet<String> =
                grounds.iter().filter_map(|g| g.as_str().map(String::from)).collect();
            self.add_rule(head.to_string(), body);
        }
    }

    /// Mark a proposition contested (e.g. from a `cross_audit` finding): remove it
    /// and its downstream from the certain core, and keep it out. Honest certainty
    /// excludes what the substrate cannot settle.
    pub fn mark_contested(&mut self, cid: &str) {
        self.contested.insert(cid.to_string());
        self.retract_internal(cid, /*as_contested=*/ true);
    }

    /// Retract an anchor or proposition: remove it and re-derive its blast radius.
    /// The non-monotone, expensive direction — the maintenance cost of certainty.
    pub fn retract(&mut self, cid: &str) {
        self.boundary.remove(cid);
        self.retract_internal(cid, false);
    }

    fn retract_internal(&mut self, cid: &str, _as_contested: bool) {
        self.admitted.remove(cid);
        // everything currently certain that is grounded through `cid`
        let affected = self.downstream_admitted(cid);
        for p in &affected {
            self.admitted.remove(p);
        }
        // re-admit any whose alternative grounding survives (fixpoint over the radius)
        loop {
            let mut changed = false;
            for p in &affected {
                if self.admitted.contains(p) || self.contested.contains(p) {
                    continue;
                }
                if self
                    .rules
                    .get(p)
                    .is_some_and(|rs| rs.iter().any(|b| b.is_subset(&self.admitted)))
                {
                    self.admitted.insert(p.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    fn downstream_admitted(&self, start: &str) -> BTreeSet<String> {
        let mut seen = BTreeSet::new();
        let mut stack = vec![start.to_string()];
        while let Some(c) = stack.pop() {
            if let Some(deps) = self.dependents.get(&c) {
                for d in deps {
                    if self.admitted.contains(d) && seen.insert(d.clone()) {
                        stack.push(d.clone());
                    }
                }
            }
        }
        seen
    }

    fn cascade(&mut self, seed: String) {
        let mut stack = vec![seed];
        while let Some(c) = stack.pop() {
            let deps: Vec<String> =
                self.dependents.get(&c).map(|s| s.iter().cloned().collect()).unwrap_or_default();
            for d in deps {
                if self.admitted.contains(&d) || self.contested.contains(&d) {
                    continue;
                }
                if self.rules.get(&d).is_some_and(|rs| rs.iter().any(|b| b.is_subset(&self.admitted))) {
                    self.admitted.insert(d.clone());
                    stack.push(d);
                }
            }
        }
    }

    /// The **amplitude spectrum** a change of `amplitude` from `seeds` would
    /// induce over the dependent graph, attenuating per hop — the data the floor
    /// calibrator reads (Step 3). Sorted descending. Computed with no floor (the
    /// full reach), so calibration sees the whole distribution it is about to cut.
    pub fn amplitude_spectrum(&self, seeds: &[&str], amplitude: f64, attenuation: f64) -> Vec<f64> {
        let mut best: BTreeMap<String, f64> = BTreeMap::new();
        let mut frontier: Vec<(String, f64)> = seeds.iter().map(|s| (s.to_string(), amplitude)).collect();
        while let Some((node, amp)) = frontier.pop() {
            if amp <= 0.0 {
                continue;
            }
            if best.get(&node).is_some_and(|&p| p >= amp) {
                continue;
            }
            best.insert(node.clone(), amp);
            let next = amp * attenuation;
            if let Some(ds) = self.dependents.get(&node) {
                for d in ds {
                    frontier.push((d.clone(), next));
                }
            }
        }
        let mut v: Vec<f64> = best.into_values().collect();
        v.sort_by(|a, b| b.partial_cmp(a).unwrap());
        v
    }

    /// Is this CID in the certain core?
    pub fn is_certain(&self, cid: &str) -> bool {
        self.admitted.contains(cid)
    }

    /// The certain core — the grounded least model (the reviewed consensus).
    pub fn certain(&self) -> &BTreeSet<String> {
        &self.admitted
    }

    /// Certainty **strength** of a proposition: how many of its justifications are
    /// currently fully grounded — i.e. independent corroboration. More support =
    /// costlier to falsify (more anchors must be withdrawn to un-certify it).
    pub fn support(&self, cid: &str) -> usize {
        self.rules
            .get(cid)
            .map_or(0, |rs| rs.iter().filter(|b| b.is_subset(&self.admitted)).count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn admits_transitively() {
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_anchor("b");
        c.add_rule("P".into(), set(&["a", "b"]));
        c.add_rule("Q".into(), set(&["P"]));
        assert_eq!(*c.certain(), set(&["a", "b", "P", "Q"]));
    }

    #[test]
    fn ungrounded_never_certain() {
        // P depends on `x`, which is never an anchor nor derived → P needs a vouch.
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_rule("P".into(), set(&["a", "x"]));
        assert!(!c.is_certain("P"), "a fact with an unresolved ground stays uncertain");
    }

    #[test]
    fn incremental_equals_batch() {
        // Order-independent: the certain core is the least model regardless of the
        // order deltas arrive (the warm-start guarantee).
        let mut a = Closure::new();
        a.add_anchor("a");
        a.add_rule("P".into(), set(&["a"]));
        a.add_rule("Q".into(), set(&["P"]));

        let mut b = Closure::new();
        b.add_rule("Q".into(), set(&["P"])); // rule before its grounds exist
        b.add_rule("P".into(), set(&["a"]));
        b.add_anchor("a"); // anchor arrives last
        assert_eq!(a.certain(), b.certain());
        assert_eq!(*b.certain(), set(&["a", "P", "Q"]));
    }

    #[test]
    fn retract_shrinks_non_monotone() {
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_anchor("b");
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("Q".into(), set(&["P"]));
        assert_eq!(*c.certain(), set(&["a", "b", "P", "Q"]));
        c.retract("a"); // withdraw the anchor P (and thus Q) rest on
        assert_eq!(*c.certain(), set(&["b"]), "the blast radius loses certainty");
    }

    #[test]
    fn retract_keeps_alternative_grounding() {
        // Corroboration is robustness: P grounded two independent ways survives the
        // loss of one. The cost to un-certify it is BOTH anchors.
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_anchor("b");
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("P".into(), set(&["b"]));
        assert_eq!(c.support("P"), 2, "two independent justifications");
        c.retract("a");
        assert!(c.is_certain("P"), "P survives on its other grounding");
        assert_eq!(c.support("P"), 1);
        c.retract("b");
        assert!(!c.is_certain("P"), "only when both anchors are gone");
    }

    // --- Step 1: lazy / batched path ---

    #[test]
    fn staged_then_settle_equals_eager() {
        // eager reference
        let mut eager = Closure::new();
        eager.add_anchor("a");
        eager.add_rule("P".into(), set(&["a"]));
        eager.add_rule("Q".into(), set(&["P"]));

        // lazy: stage everything, then settle once
        let mut lazy = Closure::new();
        lazy.stage_anchor("a");
        lazy.stage_rule("P".into(), set(&["a"]));
        lazy.stage_rule("Q".into(), set(&["P"]));
        assert_eq!(lazy.pending(), 3, "three staged changes await settle");
        assert!(lazy.certain().is_empty(), "nothing cascaded before settle");

        lazy.settle();
        assert_eq!(lazy.pending(), 0);
        assert_eq!(lazy.certain(), eager.certain(), "batched settle == eager result");
        assert_eq!(*lazy.certain(), set(&["a", "P", "Q"]));
    }

    #[test]
    fn settle_is_idempotent_and_handles_late_anchor() {
        let mut c = Closure::new();
        c.stage_rule("Q".into(), set(&["P"])); // rule before its grounds exist
        c.stage_rule("P".into(), set(&["a"]));
        c.stage_anchor("a"); // anchor staged last
        c.settle();
        assert_eq!(*c.certain(), set(&["a", "P", "Q"]), "order-independent within a batch");
        c.settle(); // no pending → no-op
        assert_eq!(*c.certain(), set(&["a", "P", "Q"]));
    }

    #[test]
    fn reach_is_the_cost_the_floor_will_gate() {
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("Q".into(), set(&["P"]));
        assert_eq!(c.reach("a"), 3, "a change at the anchor touches a, P, Q");
        assert_eq!(c.reach("Q"), 1, "a change at the leaf touches only itself — cheap");
        assert!(c.reach("a") > c.reach("Q"), "the anchor is gravitational; the leaf is local");
    }

    // --- Step 2: floored settle ---

    // rules wired before the anchor exists (so nothing is certain yet); the anchor
    // then arrives and its certainty propagates outward through a→P→Q→R.
    fn primed_chain() -> Closure {
        let mut c = Closure::new();
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("Q".into(), set(&["P"]));
        c.add_rule("R".into(), set(&["Q"]));
        assert!(c.certain().is_empty(), "no anchor yet → nothing certain");
        c
    }

    #[test]
    fn floored_zero_floor_equals_exact() {
        let mut c = primed_chain();
        c.stage_anchor("a");
        c.settle_floored(1.0, 0.5, 0.0); // floor 0 ⇒ nothing cut ⇒ exact
        assert_eq!(*c.certain(), set(&["a", "P", "Q", "R"]));
        assert_eq!(c.pending(), 0, "floor 0 leaves nothing deferred — recovers eager exactly");
    }

    #[test]
    fn floored_defers_far_effects_without_false_certainty() {
        let mut c = primed_chain();
        c.stage_anchor("a");
        c.settle_floored(1.0, 0.5, 0.3); // a=1, P=0.5 (≥), Q=0.25 (<) cut, R behind
        // sound UNDER-approximation: only what's reached above floor is certain …
        assert_eq!(*c.certain(), set(&["a", "P"]));
        assert!(!c.is_certain("Q") && !c.is_certain("R"), "far effects deferred, not denied wrongly");
        // … and the deferral is EXPLICIT (no silent drift) …
        assert!(c.pending() > 0, "the cut frontier is pending");
        // … and a full settle RECOVERS the exact core.
        c.settle();
        assert_eq!(*c.certain(), set(&["a", "P", "Q", "R"]));
        assert_eq!(c.pending(), 0);
    }

    #[test]
    fn floored_never_claims_false_certainty() {
        // whatever the floor, certain() after a floored settle is a subset of the
        // exact core — the substrate never asserts certainty it hasn't grounded.
        let mut exact = primed_chain();
        exact.stage_anchor("a");
        exact.settle();
        for &floor in &[0.0_f64, 0.3, 0.6, 0.95] {
            let mut c = primed_chain();
            c.stage_anchor("a");
            c.settle_floored(1.0, 0.5, floor);
            assert!(c.certain().is_subset(exact.certain()), "floor {floor}: no false certainty");
        }
    }

    #[test]
    fn coarse_reaches_far_fine_settles_local() {
        // low attenuation (low channel / coarse): reaches the whole chain, no deferral
        let mut coarse = primed_chain();
        coarse.stage_anchor("a");
        coarse.settle_floored(1.0, 0.9, 0.3);
        assert_eq!(*coarse.certain(), set(&["a", "P", "Q", "R"]));
        assert_eq!(coarse.pending(), 0, "coarse signal propagates far");

        // high attenuation (fine / high channel): settles locally, rest deferred
        let mut fine = primed_chain();
        fine.stage_anchor("a");
        fine.settle_floored(1.0, 0.3, 0.3);
        assert!(fine.certain().len() < 4 && fine.pending() > 0, "fine signal dissipates locally");
    }

    // --- Step 3: calibrate the floor (the self-calibration loop) ---

    #[test]
    fn calibrated_budget_floor_bounds_the_settle() {
        use crate::propagation::floor_for_budget;
        let mut c = primed_chain(); // a→P→Q→R, rules primed (dependents populated)
        // read the spectrum a change at `a` would induce, then pick the floor that
        // settles a 2-node budget — calibration drives the floor, not a guess.
        let spectrum = c.amplitude_spectrum(&["a"], 1.0, 0.5);
        assert_eq!(spectrum.len(), 4, "a, P, Q, R all reachable");
        let floor = floor_for_budget(&spectrum, 2);

        c.stage_anchor("a");
        c.settle_floored(1.0, 0.5, floor);
        assert_eq!(*c.certain(), set(&["a", "P"]), "the budget floor settles exactly the coarse 2");
        assert!(c.pending() > 0, "the rest is deferred, explicitly");
        c.settle();
        assert_eq!(*c.certain(), set(&["a", "P", "Q", "R"]), "and recovers exactly");
    }

    #[test]
    fn calibrate_gap_separates_or_declines() {
        use crate::propagation::calibrate_floor;
        // a chain's spectrum is geometric (uniform) → calibration declines to floor
        // (separation ≈ 1), honestly: there is no clean coarse/fine split to cut.
        let c = primed_chain();
        let spectrum = c.amplitude_spectrum(&["a"], 1.0, 0.5);
        let cal = calibrate_floor(&spectrum).unwrap();
        assert!((cal.separation - 1.0).abs() < 1e-9, "uniform graph ⇒ no distinguished floor");
    }

    #[test]
    fn contested_is_excluded_and_propagates() {
        // Honest certainty: a contested fact is not certain, and cannot certify
        // others — the cautious core, not a forced winner.
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("Q".into(), set(&["P"]));
        assert!(c.is_certain("Q"));
        c.mark_contested("P");
        assert!(!c.is_certain("P"));
        assert!(!c.is_certain("Q"), "what rests on a contested fact is not certain either");
        assert!(c.is_certain("a"), "the untouched anchor stays certain");
    }
}
