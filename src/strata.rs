//! Proposition / assertion stratification — the fix for the grounds-in-identity
//! defect pinned in `quantum::tests::grounds_in_identity_false_positives_on_corroboration`.
//!
//! Putting a claim's `grounds` into identity (as the reference `claim_schema`
//! does) conflates two epistemic objects, and the bug is that there is **only one
//! level**. Two *independent* derivations of one fact — `Ω_Λ = 13/19` grounded
//! once in Planck and once in WMAP — get different identity CIDs (the grounds
//! differ) but one witness (the value is route-independent), so
//! [`cross_audit`](crate::quantum::cross_audit) fires a spurious
//! [`UnderMerge`](crate::quantum::CrossAuditConflict::UnderMerge). That is not a
//! bug — it is **corroboration**.
//!
//! The fix stratifies identity into two levels:
//!
//! - A **proposition** is the extensional content only: `subject` + canonical
//!   value. **No grounds in identity.** Dedup and the form-vs-witness cross-audit
//!   happen *here*. Two tellings of one value → one proposition CID.
//! - An **assertion** is `(proposition, grounds, agent)`. Grounds bind identity
//!   *here* — preserving the split-edge "grounds-target binds identity" decision,
//!   now correctly scoped to the assertion. Many assertions per proposition =
//!   corroboration, counted and weighted by independence.
//!
//! This is a doctrine **reversal at the claim layer**, and it is the right
//! default only for value-witnessed claims: where a claim's identity genuinely
//! *is* its derivation (a proof, not its conclusion), grounds belong in identity
//! and the proposition layer collapses onto the assertion. See `SPINE.md` §4.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::bridge::needs_review;
use crate::quantum::Quantum;
use crate::schema::{FieldKind, Schema};

/// The **proposition** schema: extensional content only.
///
/// Identity: `subject` + the canonical value (`num`/`den`). Witness: `value`, the
/// route-independent decimal, so corroboration is comparable across tellings.
/// Crucially there is **no grounds field** — grounds live on the assertion. Two
/// derivations of the same ratio seal to the *same* proposition CID.
pub fn proposition_schema() -> Schema {
    Schema::new("proposition", 1)
        .identity("subject", FieldKind::String)
        .identity("num", FieldKind::Integer)
        .identity("den", FieldKind::Integer)
        .witness("value", FieldKind::Float)
}

/// The **assertion** schema: `(proposition, grounds, agent)`.
///
/// Identity: the `proposition` CID and the `grounds` set it was derived from —
/// where grounds *correctly* bind identity (the split-edge target rule, scoped to
/// the assertion). `agent` is detachable projection. Many distinct assertions can
/// point at one proposition; that count is the corroboration ([`corroboration`]).
pub fn assertion_schema() -> Schema {
    Schema::new("assertion", 1)
        .identity("proposition", FieldKind::Cid)
        .identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
        .required("agent", FieldKind::String)
}

/// Count corroboration: proposition CID → the distinct agents asserting it.
///
/// Given a bag of *assertion* quanta, group by the asserted `proposition` and
/// collect the distinct asserting `agent`s. The corroboration count for a
/// proposition is `result[&prop_cid].len()`. Assertions whose body lacks a
/// `proposition` are skipped; agent lists are deduplicated and sorted for
/// determinism. (Independence weighting — distinct *grounds*, not just distinct
/// agents — is the support axis of `SPINE.md` §7, left to the caller for now.)
pub fn corroboration(assertions: &[Quantum]) -> BTreeMap<String, Vec<String>> {
    let mut by_prop: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for a in assertions {
        let Some(prop) = a.field("proposition").and_then(|v| v.as_str()) else {
            continue;
        };
        let agent = a
            .field("agent")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let agents = by_prop.entry(prop.to_string()).or_default();
        if !agents.contains(&agent) {
            agents.push(agent);
        }
    }
    for agents in by_prop.values_mut() {
        agents.sort();
    }
    by_prop
}

/// The W(Ω)-style **locked fraction** — coverage that replaces a flat "% sealed".
///
/// A quantum is *locked* (auto-checkable, no human vouch) when its schema
/// declares a witness the cross-audit can recompute and diff; a witness-free
/// schema (an attestation, a bare NL claim) falls to the review tier
/// ([`needs_review`]). Lockability is a property of the schema, so this reports
/// `1.0` (all locked) or `0.0` (all review) for a homogeneous bag; an empty bag
/// is fully covered. Mixed-schema bags are the natural generalization — see
/// `SPINE.md` §9 (the measure-zero unlocked residue is what this number tracks
/// shrinking).
pub fn locked_fraction(schema: &Schema, quanta: &[Quantum]) -> f64 {
    if quanta.is_empty() {
        return 1.0;
    }
    if needs_review(schema) {
        0.0
    } else {
        1.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantum::{cross_audit, CrossAuditConflict};
    use serde_json::json;

    #[test]
    fn corroboration_is_clean_at_proposition_level() {
        // The defect's input, fixed: two independent derivations of one fact,
        // grounds NOT here, collapse to one proposition CID — cross_audit clean.
        let s = proposition_schema();
        let a = Quantum::seal(&s, &json!({"subject":"omega_lambda","num":13,"den":19,"value":0.6842})).unwrap();
        let b = Quantum::seal(&s, &json!({"subject":"omega_lambda","num":13,"den":19,"value":0.6842})).unwrap();
        assert_eq!(a.cid, b.cid, "two tellings of one value → one proposition CID");
        assert!(
            cross_audit(&s, &[a, b]).unwrap().is_empty(),
            "corroboration must not be flagged as UnderMerge"
        );
    }

    #[test]
    fn real_under_merge_still_caught_at_proposition_level() {
        // A genuine canonicalizer under-merge: 26/38 and 13/19 are the same value
        // but two distinct canonical forms (the reducer failed). At the
        // proposition level: two CIDs, one witness → UnderMerge still fires.
        let s = proposition_schema();
        let reduced = Quantum::seal(&s, &json!({"subject":"r","num":13,"den":19,"value":0.6842})).unwrap();
        let unreduced = Quantum::seal(&s, &json!({"subject":"r","num":26,"den":38,"value":0.6842})).unwrap();
        assert_ne!(reduced.cid, unreduced.cid);
        let conflicts = cross_audit(&s, &[reduced, unreduced]).unwrap();
        assert!(
            conflicts.iter().any(|c| matches!(c, CrossAuditConflict::UnderMerge { .. })),
            "a real under-merge must still fire, got {conflicts:?}"
        );
    }

    #[test]
    fn corroboration_counts_distinct_agents() {
        let ps = proposition_schema();
        let prop = Quantum::seal(&ps, &json!({"subject":"omega_lambda","num":13,"den":19,"value":0.6842})).unwrap();
        let ass = assertion_schema();
        let a = Quantum::seal(&ass, &json!({"proposition":prop.cid,"grounds":["planck"],"agent":"claude"})).unwrap();
        let b = Quantum::seal(&ass, &json!({"proposition":prop.cid,"grounds":["wmap"],"agent":"gpt"})).unwrap();
        // duplicate telling — same agent, same grounds — not new corroboration
        let c = Quantum::seal(&ass, &json!({"proposition":prop.cid,"grounds":["planck"],"agent":"claude"})).unwrap();
        assert_eq!(a.cid, c.cid, "identical assertion dedups");

        let counts = corroboration(&[a, b, c]);
        assert_eq!(counts[&prop.cid], vec!["claude".to_string(), "gpt".to_string()]);
    }

    #[test]
    fn locked_fraction_splits_on_witness() {
        let ps = proposition_schema();
        let prop = Quantum::seal(&ps, &json!({"subject":"r","num":1,"den":2,"value":0.5})).unwrap();
        assert_eq!(locked_fraction(&ps, &[prop]), 1.0, "a witnessed proposition is locked");

        let ass = assertion_schema();
        let a = Quantum::seal(&ass, &json!({"proposition":"deadbeef","grounds":["g"],"agent":"claude"})).unwrap();
        assert!(needs_review(&ass));
        assert_eq!(locked_fraction(&ass, &[a]), 0.0, "a witness-free assertion needs review");

        assert_eq!(locked_fraction(&ps, &[]), 1.0, "empty bag is fully covered");
    }
}
