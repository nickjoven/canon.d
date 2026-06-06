//! Subsumption over rationals — entailment as exact interval containment.
//!
//! Subsumption upgrades dedup from *equality* (same CID) to *implication*: a claim
//! `P` **subsumes** `Q` (`P ⊢ Q`, P is the stronger/tighter claim) when P's
//! interval is contained in Q's. A precise value subsumes its coarsenings:
//! `13/19` ⊢ `≈0.68` ⊢ `∈(0.6,0.7)`. Built on `Rat`'s exact `Ord` (no float, no
//! bignum) — so the order is exact, which it must be (a wrong order is false
//! certainty).
//!
//! One module delivers four of the leveraged design changes:
//! - **subsumption-dedup** ([`redundant`]) — claims already entailed by a stronger
//!   one are redundant (duplicate reasoning), skipped not re-derived;
//! - **minimal consensus** ([`maximal_antichain`]) — commit only the ⊢-strongest;
//!   the rest is derivable;
//! - **certainty flows down the order** ([`entailment_edges`] → `Closure`) — certify
//!   the strongest fact and its coarsenings follow for free;
//! - **dethroning** — a stronger claim makes a weaker incumbent redundant; this is
//!   the principled dissipation that fixes attention favoritism (forcing *acts*).
//!
//! Bounds (honest): closed intervals here (open/closed boundaries are a
//! refinement); pairwise `O(n²)` (a real index is an interval tree, lazy/floored
//! like the closure); confined to the formal core (rationals) — incomparable or
//! NL claims form antichains that do not compress.

use std::collections::BTreeSet;

use crate::generator::Rat;

/// A closed rational interval `[lo, hi]`. A point value is `[v, v]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RatInterval {
    pub lo: Rat,
    pub hi: Rat,
}

impl RatInterval {
    /// A point claim (an exact value).
    pub fn point(r: Rat) -> Self {
        RatInterval { lo: r, hi: r }
    }

    /// An interval `[lo, hi]`; `None` if `lo > hi`.
    pub fn new(lo: Rat, hi: Rat) -> Option<Self> {
        if lo <= hi {
            Some(RatInterval { lo, hi })
        } else {
            None
        }
    }

    /// `self ⊢ other`: self is the tighter, stronger claim — its interval is
    /// contained in `other`'s, so asserting self already asserts other.
    pub fn subsumes(&self, other: &RatInterval) -> bool {
        other.lo <= self.lo && self.hi <= other.hi
    }

    /// Strictly stronger: subsumes and is not the same interval (a proper
    /// coarsening). Equal intervals are CID-level dedup, not subsumption.
    pub fn strictly_subsumes(&self, other: &RatInterval) -> bool {
        self != other && self.subsumes(other)
    }
}

/// A rational claim paired with its CID.
#[derive(Debug, Clone)]
pub struct Claim {
    pub cid: String,
    pub interval: RatInterval,
}

/// The **redundant** claims: those strictly subsumed by some other claim — a
/// stronger claim already entails them, so re-deriving them is duplicate
/// reasoning. (Subsumption-dedup, and the dethroning set: a newly-arrived stronger
/// claim moves the weaker incumbent into here.)
pub fn redundant(claims: &[Claim]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for q in claims {
        if claims.iter().any(|p| p.cid != q.cid && p.interval.strictly_subsumes(&q.interval)) {
            out.insert(q.cid.clone());
        }
    }
    out
}

/// The ⊢-**maximal antichain** — the strongest claims, not subsumed by any other.
/// The irredundant core: the consensus need commit only to these, since every
/// weaker claim is recoverable by subsumption rather than by storage.
pub fn maximal_antichain(claims: &[Claim]) -> Vec<String> {
    let red = redundant(claims);
    claims.iter().filter(|c| !red.contains(&c.cid)).map(|c| c.cid.clone()).collect()
}

/// Entailment edges `(weaker, stronger)`: for each strict subsumption `P ⊊ Q`, an
/// edge meaning *Q is grounded by P*. Fed to a [`Closure`](crate::Closure) as
/// rules `Q ⟸ {P}`, they make certainty **flow down the order**: certify the
/// strongest fact and its coarsenings become certain for free (no separate
/// grounding needed for each).
pub fn entailment_edges(claims: &[Claim]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in claims {
        for q in claims {
            if p.cid != q.cid && p.interval.strictly_subsumes(&q.interval) {
                out.push((q.cid.clone(), p.cid.clone()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Closure;
    use std::collections::BTreeSet;

    fn r(n: i64, d: i64) -> Rat {
        Rat::new(n, d).unwrap()
    }

    // Ω_Λ claims at three resolutions: 13/19 ≈ 0.6842 ⊊ [0.67,0.69] ⊊ [0.6,0.7]
    fn omega_claims() -> Vec<Claim> {
        vec![
            Claim { cid: "precise".into(), interval: RatInterval::point(r(13, 19)) },
            Claim { cid: "approx".into(), interval: RatInterval::new(r(67, 100), r(69, 100)).unwrap() },
            Claim { cid: "loose".into(), interval: RatInterval::new(r(3, 5), r(7, 10)).unwrap() },
        ]
    }

    #[test]
    fn subsumes_is_exact_interval_containment() {
        let c = omega_claims();
        let (precise, approx, loose) = (&c[0].interval, &c[1].interval, &c[2].interval);
        assert!(precise.subsumes(approx) && approx.subsumes(loose), "tighter ⊢ looser");
        assert!(!loose.subsumes(precise), "the loose claim does not subsume the precise one");
        assert!(precise.strictly_subsumes(loose));
        assert!(!precise.strictly_subsumes(precise), "a claim does not strictly subsume itself");
    }

    #[test]
    fn redundant_and_antichain_keep_only_the_strongest() {
        let red = redundant(&omega_claims());
        let want: BTreeSet<String> = ["approx", "loose"].into_iter().map(String::from).collect();
        assert_eq!(red, want, "the coarsenings are redundant — already entailed");
        assert_eq!(maximal_antichain(&omega_claims()), vec!["precise".to_string()], "commit only the strongest");
    }

    #[test]
    fn certainty_flows_down_the_order() {
        let claims = omega_claims();
        let mut c = Closure::new();
        for (weaker, stronger) in entailment_edges(&claims) {
            c.add_rule(weaker, [stronger].into_iter().collect());
        }
        // certify ONLY the strongest fact …
        c.add_anchor("precise");
        // … and its coarsenings are certain for free, via subsumption.
        assert!(c.is_certain("precise"));
        assert!(c.is_certain("approx"), "approx is entailed by precise");
        assert!(c.is_certain("loose"), "loose is entailed too");
    }

    #[test]
    fn stronger_claim_dethrones_weaker_incumbent() {
        let loose = Claim { cid: "loose".into(), interval: RatInterval::new(r(3, 5), r(7, 10)).unwrap() };
        // the incumbent alone is not redundant — nothing stronger exists yet
        assert!(redundant(std::slice::from_ref(&loose)).is_empty());
        // a forced, precise claim arrives → the loose incumbent becomes redundant.
        // Subsumption is the dissipation: forcing dethrones the weaker account.
        let precise = Claim { cid: "precise".into(), interval: RatInterval::point(r(13, 19)) };
        let red = redundant(&[loose, precise]);
        assert!(red.contains("loose") && !red.contains("precise"), "the stronger claim dethrones the weaker");
    }
}
