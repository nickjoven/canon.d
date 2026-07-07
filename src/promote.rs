//! Unit 2 proper — cross-audit the N structurer routes and promote only what
//! survives two independent checks (INTAKE.md §"The audit: N independent
//! structurers").
//!
//! (Born as a second `intake.rs` on quantum-tier in parallel with the Unit 1
//! spine — a live W5, two modules claiming one name — and renamed `promote`
//! at merge: [`crate::intake`] is the spine that seals and buckets; this
//! module is the audit that decides promotion. Root re-exports unchanged.)
//!
//! The deterministic routes ([`structurer`](crate::structurer)) each *propose*
//! prose→structure readings; nothing they emit is trusted on its own. This module
//! is the audit that decides which proposals become promoted facts:
//!
//!   1. **Domain witness** — a value is checked against its subject's declared
//!      value domain. Ω_Λ is a density fraction, so it must lie in `(0, 1)`; a
//!      reading of `Ω_Λ = 12` fails and is *queued*, not promoted. This is the
//!      filter for the honest residual `beats_grep`/`intake_corpus` surfaced: a
//!      handful of `Ω_Λ = 12`-style corpus fragments the single greedy route
//!      could not tell from real facts. The witness is a *value* law, not a
//!      corpus label — it does not hand-scope anything.
//!   2. **Cross-route corroboration** — the same proposition reached by more than
//!      one independent route (prefix `Ω_Λ = 13/19` and postfix `13/19 = Ω_Λ`),
//!      or from more than one utterance, is stronger than a lone mention. The
//!      count is recorded so the promotion is *attributed*, never anonymous.
//!
//! A promoted fact therefore carries: its proposition CID, corroboration, and the
//! routes that agreed. A queued one carries the reason. Nothing is silently
//! dropped and nothing is silently trusted — the two failure modes INTAKE.md
//! names.

use std::collections::{BTreeMap, BTreeSet};

use crate::bridge::Structuring;
use crate::generator::Rat;

// ---------------------------------------------------------------------------
// Domain witness
// ---------------------------------------------------------------------------

/// A subject's declared value domain: an exact rational range with independently
/// open or closed bounds. `Ω_Λ ∈ (0, 1)` is `open(0, 1)`.
///
/// Exact throughout — bounds are [`Rat`]s and membership is decided by `Rat`'s
/// exact `Ord`, never an `f64`. A density fraction of *exactly* 1 (the whole
/// universe dark energy) is as implausible as 12, which is why the bounds are
/// open, not the closed [`RatInterval`](crate::subsume::RatInterval).
#[derive(Debug, Clone)]
pub struct DomainRange {
    lo: Rat,
    hi: Rat,
    lo_open: bool,
    hi_open: bool,
    /// A short human label for the range, e.g. `"(0, 1)"`, used in queue reasons.
    label: String,
}

impl DomainRange {
    /// The open range `(lo, hi)`.
    pub fn open(lo: Rat, hi: Rat) -> Self {
        let label = format!("({}, {})", lo.reduced_string(), hi.reduced_string());
        DomainRange {
            lo,
            hi,
            lo_open: true,
            hi_open: true,
            label,
        }
    }

    /// The closed range `[lo, hi]`.
    pub fn closed(lo: Rat, hi: Rat) -> Self {
        let label = format!("[{}, {}]", lo.reduced_string(), hi.reduced_string());
        DomainRange {
            lo,
            hi,
            lo_open: false,
            hi_open: false,
            label,
        }
    }

    /// Exact membership: `lo (< | ≤) v (< | ≤) hi` per the bound openness.
    pub fn contains(&self, v: Rat) -> bool {
        let above_lo = if self.lo_open {
            self.lo < v
        } else {
            self.lo <= v
        };
        let below_hi = if self.hi_open {
            v < self.hi
        } else {
            v <= self.hi
        };
        above_lo && below_hi
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

/// The verdict of the domain witness on one `(subject, value)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plausibility {
    /// The subject declares a domain and the value lies inside it.
    InDomain,
    /// The subject declares a domain and the value lies outside it — the reason
    /// carries the violated range (e.g. `"Ω_Λ ∈ (0, 1)"`).
    OutOfDomain { domain: String },
    /// The subject declares no domain — plausibility is unknown, not a failure.
    NoConstraint,
}

impl Plausibility {
    /// Whether this verdict blocks promotion. Only [`OutOfDomain`](Self::OutOfDomain)
    /// does — an unknown domain is not a violation.
    pub fn blocks(&self) -> bool {
        matches!(self, Plausibility::OutOfDomain { .. })
    }
}

/// `subject` → its declared value domain. A subject absent from the map is
/// unconstrained (its readings pass the witness by default — the honest state
/// for "we have not declared a law for this quantity yet").
#[derive(Debug, Clone, Default)]
pub struct SubjectDomains {
    map: BTreeMap<String, DomainRange>,
}

impl SubjectDomains {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare `range` as `subject`'s value domain.
    pub fn with(mut self, subject: &str, range: DomainRange) -> Self {
        self.map.insert(subject.to_string(), range);
        self
    }

    /// The cosmology domains for the harmonics corpus: `Ω_Λ` is a density
    /// fraction, so its value is in the open interval `(0, 1)`. A value law, not a
    /// corpus label — it constrains *what Ω_Λ can be*, independent of any file.
    pub fn harmonics() -> Self {
        let zero = Rat::new(0, 1).expect("0/1");
        let one = Rat::new(1, 1).expect("1/1");
        Self::new().with("omega_lambda", DomainRange::open(zero, one))
    }

    /// Witness a `(subject, num/den)` value. `num`/`den` are `i128` (the
    /// structurer's width); a value too large for the `Rat` algebra cannot lie in
    /// any finite declared domain, so it is [`OutOfDomain`](Plausibility::OutOfDomain)
    /// when the subject is constrained.
    pub fn witness(&self, subject: &str, num: i128, den: i128) -> Plausibility {
        let Some(range) = self.map.get(subject) else {
            return Plausibility::NoConstraint;
        };
        let out_of = || Plausibility::OutOfDomain {
            domain: format!("{subject} ∈ {}", range.label),
        };
        let (Ok(n), Ok(d)) = (i64::try_from(num), i64::try_from(den)) else {
            return out_of(); // too large to be in a finite range
        };
        match Rat::new(n, d) {
            Ok(v) if range.contains(v) => Plausibility::InDomain,
            _ => out_of(),
        }
    }
}

// ---------------------------------------------------------------------------
// N-route cross-audit + promotion
// ---------------------------------------------------------------------------

/// A proposition promoted to a fact: it passed the domain witness and is recorded
/// with how strongly (and by which routes) it was corroborated.
#[derive(Debug, Clone)]
pub struct Promoted {
    pub proposition: String,
    pub subject: String,
    pub value: String,
    /// Distinct utterances that asserted it (the same fact seen in many places).
    pub corroboration: usize,
    /// Distinct structurer routes that reached it — cross-route agreement.
    pub routes: Vec<String>,
}

/// A proposition held back from promotion, with the reason a human must clear.
#[derive(Debug, Clone)]
pub struct Queued {
    pub proposition: String,
    pub subject: String,
    pub value: String,
    pub corroboration: usize,
    /// Why it was not promoted (e.g. `"outside domain omega_lambda ∈ (0, 1)"`).
    pub reason: String,
}

/// The result of a cross-audit: what promoted and what was queued.
#[derive(Debug, Clone, Default)]
pub struct CrossAuditReport {
    pub promoted: Vec<Promoted>,
    pub queued: Vec<Queued>,
}

impl CrossAuditReport {
    /// Distinct propositions considered.
    pub fn considered(&self) -> usize {
        self.promoted.len() + self.queued.len()
    }
}

/// The threshold for promotion beyond the domain witness.
#[derive(Debug, Clone, Copy)]
pub struct PromotionPolicy {
    /// A proposition needs at least this many distinct utterances corroborating
    /// it. Default 1 — a single in-domain mention promotes (but records its low
    /// corroboration); raise it to demand repeated independent assertion.
    pub min_corroboration: usize,
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        PromotionPolicy {
            min_corroboration: 1,
        }
    }
}

/// Cross-audit a bag of structurings from one or more routes and decide, for each
/// distinct proposition, promote vs. queue.
///
/// A proposition is **promoted** when the domain witness does not block it *and*
/// its corroboration meets [`PromotionPolicy::min_corroboration`]. Otherwise it is
/// **queued** with the reason (out-of-domain, or under-corroborated). Corroboration
/// counts distinct utterance CIDs; the promoted routes are the distinct structuring
/// annotators. Pure and order-independent — the same bag yields the same report.
pub fn cross_audit_routes(
    structurings: &[Structuring],
    domains: &SubjectDomains,
    policy: &PromotionPolicy,
) -> CrossAuditReport {
    // proposition CID → accumulated evidence.
    struct Acc {
        subject: String,
        value: String,
        num: i128,
        den: i128,
        utterances: BTreeSet<String>,
        routes: BTreeSet<String>,
    }
    let mut by_prop: BTreeMap<String, Acc> = BTreeMap::new();

    for s in structurings {
        let prop = s.claim.cid.clone();
        let subject = str_field(s, "subject");
        let value = str_field_of(&s.claim, "value");
        let num = int_field(&s.claim, "num");
        let den = int_field(&s.claim, "den");
        let route = str_field_of(&s.structuring, "annotator");

        let acc = by_prop.entry(prop).or_insert_with(|| Acc {
            subject,
            value,
            num,
            den,
            utterances: BTreeSet::new(),
            routes: BTreeSet::new(),
        });
        acc.utterances.insert(s.utterance.cid.clone());
        if !route.is_empty() {
            acc.routes.insert(route);
        }
    }

    let mut report = CrossAuditReport::default();
    for (prop, acc) in by_prop {
        let corroboration = acc.utterances.len();
        let plausibility = domains.witness(&acc.subject, acc.num, acc.den);
        let routes: Vec<String> = acc.routes.into_iter().collect();

        if let Plausibility::OutOfDomain { domain } = &plausibility {
            report.queued.push(Queued {
                proposition: prop,
                subject: acc.subject,
                value: acc.value,
                corroboration,
                reason: format!("outside domain {domain}"),
            });
        } else if corroboration < policy.min_corroboration {
            report.queued.push(Queued {
                proposition: prop,
                subject: acc.subject,
                value: acc.value,
                corroboration,
                reason: format!(
                    "under-corroborated ({corroboration} < {})",
                    policy.min_corroboration
                ),
            });
        } else {
            report.promoted.push(Promoted {
                proposition: prop,
                subject: acc.subject,
                value: acc.value,
                corroboration,
                routes,
            });
        }
    }
    // Strongest first — most corroborated promotions, then most-corroborated queues.
    report.promoted.sort_by(|a, b| {
        b.corroboration
            .cmp(&a.corroboration)
            .then(a.value.cmp(&b.value))
    });
    report.queued.sort_by(|a, b| {
        b.corroboration
            .cmp(&a.corroboration)
            .then(a.value.cmp(&b.value))
    });
    report
}

fn str_field(s: &Structuring, field: &str) -> String {
    str_field_of(&s.claim, field)
}
fn str_field_of(q: &crate::Quantum, field: &str) -> String {
    q.field(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}
fn int_field(q: &crate::Quantum, field: &str) -> i128 {
    q.field(field)
        .and_then(|v| v.as_i64())
        .map(|n| n as i128)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structurer::{structure_span_with, Route, SubjectLexicon};

    fn lex() -> SubjectLexicon {
        SubjectLexicon::harmonics()
    }

    #[test]
    fn domain_witness_rejects_out_of_range_and_accepts_in_range() {
        let d = SubjectDomains::harmonics();
        assert_eq!(d.witness("omega_lambda", 13, 19), Plausibility::InDomain);
        assert_eq!(
            d.witness("omega_lambda", 6847, 10000),
            Plausibility::InDomain
        );
        assert!(
            d.witness("omega_lambda", 12, 1).blocks(),
            "12 is not a density fraction"
        );
        assert!(
            d.witness("omega_lambda", 1, 1).blocks(),
            "exactly 1 is out of the open range"
        );
        // an undeclared subject is unknown, not a violation
        assert_eq!(
            d.witness("some_other_quantity", 5, 1),
            Plausibility::NoConstraint
        );
    }

    #[test]
    fn the_residual_is_filtered_but_the_real_fact_promotes() {
        // The exact scenario the corpus surfaced: one true fact and one Ω_Λ = 12
        // fragment. The witness promotes the fact and queues the residual.
        let mut all = Vec::new();
        all.extend(
            structure_span_with("Ω_Λ = 13/19", "en", &lex(), Route::Prefix)
                .unwrap()
                .structured,
        );
        all.extend(
            structure_span_with("Ω_Λ = 12", "en", &lex(), Route::Prefix)
                .unwrap()
                .structured,
        );

        let report = cross_audit_routes(
            &all,
            &SubjectDomains::harmonics(),
            &PromotionPolicy::default(),
        );
        assert_eq!(report.promoted.len(), 1);
        assert_eq!(report.promoted[0].value, "13/19");
        assert_eq!(report.queued.len(), 1);
        assert_eq!(report.queued[0].value, "12/1");
        assert!(report.queued[0].reason.contains("outside domain"));
    }

    #[test]
    fn two_routes_corroborate_one_proposition() {
        // Prefix and postfix reach Ω_Λ = 13/19 from two different utterances. One
        // promoted proposition, corroboration 2, both routes named.
        let mut all = Vec::new();
        all.extend(
            structure_span_with("Ω_Λ = 13/19", "en", &lex(), Route::Prefix)
                .unwrap()
                .structured,
        );
        all.extend(
            structure_span_with("13/19 = Ω_Λ", "en", &lex(), Route::Postfix)
                .unwrap()
                .structured,
        );

        let report = cross_audit_routes(
            &all,
            &SubjectDomains::harmonics(),
            &PromotionPolicy::default(),
        );
        assert_eq!(report.promoted.len(), 1);
        let p = &report.promoted[0];
        assert_eq!(p.value, "13/19");
        assert_eq!(p.corroboration, 2, "two distinct utterances");
        assert_eq!(p.routes.len(), 2, "prefix and postfix both agreed");
    }

    #[test]
    fn under_corroboration_queues_even_when_in_domain() {
        let one = structure_span_with("Ω_Λ = 13/19", "en", &lex(), Route::Prefix)
            .unwrap()
            .structured;
        let strict = PromotionPolicy {
            min_corroboration: 2,
        };
        let report = cross_audit_routes(&one, &SubjectDomains::harmonics(), &strict);
        assert!(report.promoted.is_empty());
        assert_eq!(report.queued.len(), 1);
        assert!(report.queued[0].reason.contains("under-corroborated"));
    }
}
