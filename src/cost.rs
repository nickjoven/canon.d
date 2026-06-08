//! Cost / depth accounting over a closure (`COST_DEPTH.md`) — the measurement half
//! of the discipline. The gates *avoid* work; this *reports* how much, and turns
//! the corpus's shape into the headline numbers: coverage (W(Ω)), compression (the
//! holographic facts-per-bit-of-rule), and the per-fact maintenance cost.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::closure::Closure;
use crate::gate::GateOutcome;
use crate::generator::mdl;

/// Cost/depth metrics over a closure + its generators. Each field maps to a
/// warranted-depth axis from `COST_DEPTH.md`.
#[derive(Debug, Clone)]
pub struct CostReport {
    // SIGNAL
    /// Facts in the certain core.
    pub certain: usize,
    /// All known facts (denominator for coverage).
    pub total_facts: usize,
    /// W(Ω) coverage = certain / total.
    pub locked_fraction: f64,
    // COST
    /// Σ `mdl(program)` over the generators — the *deciding* / boundary cost (bits).
    pub generator_mdl: usize,
    /// Σ `reach` over the certain core — the maintenance blast-radius.
    pub total_reach: usize,
    /// Un-settled changes (the lazy debt).
    pub pending: usize,
    // RATIOS (the headline numbers)
    /// certain / generator_mdl — facts per bit of rule (the holographic ratio).
    pub compression: f64,
    /// total_reach / certain — average blast-radius per certified fact.
    pub cost_per_fact: f64,
}

/// Compute the report. `all_facts` is the full known node set (coverage
/// denominator; clamped to ≥ certain). `generators` are the program terms whose
/// description length is the boundary cost.
pub fn cost_report(closure: &Closure, all_facts: &BTreeSet<String>, generators: &[&Value]) -> CostReport {
    let certain = closure.certain().len();
    let total_facts = all_facts.len().max(certain);
    let locked_fraction = if total_facts == 0 { 1.0 } else { certain as f64 / total_facts as f64 };

    let generator_mdl: usize = generators.iter().map(|g| mdl(g)).sum();
    let total_reach: usize = closure.certain().iter().map(|c| closure.reach(c)).sum();
    let pending = closure.pending();

    let compression = if generator_mdl == 0 { 0.0 } else { certain as f64 / generator_mdl as f64 };
    let cost_per_fact = if certain == 0 { 0.0 } else { total_reach as f64 / certain as f64 };

    CostReport {
        certain,
        total_facts,
        locked_fraction,
        generator_mdl,
        total_reach,
        pending,
        compression,
        cost_per_fact,
    }
}

/// A budget to ratchet the report from advisory into a gate.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub min_locked: f64,
    pub max_cost_per_fact: f64,
}

impl CostReport {
    /// Check the report against a budget: `Fail` if coverage is below the floor or
    /// the per-fact cost is above the ceiling, else `Pass`.
    pub fn check(&self, b: &Budget) -> GateOutcome {
        if self.locked_fraction < b.min_locked || self.cost_per_fact > b.max_cost_per_fact {
            GateOutcome::Fail
        } else {
            GateOutcome::Pass
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // a→P→Q→R, with R left uncertain (no anchor reaches it)
    fn chain() -> Closure {
        let mut c = Closure::new();
        c.add_anchor("a");
        c.add_rule("P".into(), set(&["a"]));
        c.add_rule("Q".into(), set(&["P"]));
        c.add_rule("R".into(), set(&["Z"])); // Z is never certain → R uncertain
        c
    }

    #[test]
    fn report_computes_coverage_and_compression() {
        let c = chain();
        let gen = json!({"walk":"LR"}); // a small generator, for MDL
        let all = set(&["a", "P", "Q", "R"]);
        let r = cost_report(&c, &all, &[&gen]);

        assert_eq!(r.certain, 3, "a, P, Q are certain; R is not");
        assert_eq!(r.total_facts, 4);
        assert!((r.locked_fraction - 0.75).abs() < 1e-9, "3/4 coverage");
        assert!(r.generator_mdl > 0);
        assert!(r.compression > 0.0, "facts per bit of rule");
        assert!(r.cost_per_fact > 0.0, "avg blast radius per fact");
        assert_eq!(r.pending, 0);
    }

    #[test]
    fn report_checks_against_a_budget() {
        let c = chain();
        let gen = json!({"walk":"LR"});
        let r = cost_report(&c, &set(&["a", "P", "Q", "R"]), &[&gen]);

        // generous budget passes
        assert_eq!(r.check(&Budget { min_locked: 0.5, max_cost_per_fact: 100.0 }), GateOutcome::Pass);
        // demanding coverage fails (we're at 0.75)
        assert_eq!(r.check(&Budget { min_locked: 0.9, max_cost_per_fact: 100.0 }), GateOutcome::Fail);
        // tight cost ceiling fails
        assert_eq!(r.check(&Budget { min_locked: 0.5, max_cost_per_fact: 0.5 }), GateOutcome::Fail);
    }
}
