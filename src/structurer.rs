//! The deterministic structurer — the first of INTAKE.md's N structurer routes.
//!
//! INTAKE.md's load-bearing gap is the step **from prose to structure**: a flat
//! corpus writes `Ω_Λ = 13/19`, `Omega_Lambda equals 26/38`, `Ω_Λ ∈ [13/19, …]`,
//! and something must decide *which formal object each span names* before the
//! substrate can address or reconcile it. That decision is a **structurer**, and
//! its judgment is sealed and attributed like everything else — never an ambient
//! preprocessing step (INTAKE.md §"structuring is a falsifiable act").
//!
//! This is the *deterministic* route among the N: "fast, deterministic, free —
//! where it agrees it adds corroboration; where it disagrees,
//! `structurer_disagreement_rate` measures it." It has two moving parts:
//!
//!   * **numeric lift** — urtext's `regions()` + `to_rational()` pull every exact
//!     rational out of a span with no `f64` ever constructed (`13/19` and `26/38`
//!     both land as `13/19`); and
//!   * **subject attribution** — a [`SubjectLexicon`] maps the surface names a
//!     corpus actually uses (`Ω_Λ`, `Omega_Lambda`, `dark energy`) to one
//!     canonical subject, and each lifted value is bound to the nearest preceding
//!     cue in the span.
//!
//! The honest boundary: a value with **no** recognizable subject cue is *not*
//! guessed. It is returned unattributed, and [`structure_span`] routes it to the
//! `needs_review` queue — a visible coverage gap, which is exactly harmonics' W1
//! failure ("a regex misses a prose reference") made loud instead of silent.
//!
//! Feature-gated on `prose` (this route *is* the urtext bridge). The N-route
//! cross-audit that promotes agreement across structurers (Unit 2 proper) sits
//! one layer up and consumes what this route emits.

use crate::bridge::{structure, Structuring};
use crate::quantum::QuantumError;
use crate::strata::proposition_schema;
use serde_json::json;
use urtext::{regions, to_rational, RegionKind};

/// The annotator identity this route seals its structurings under. Stable, so a
/// re-run supersedes its own prior structuring (identity = `(utterance,
/// annotator)`) rather than forking a second opinion.
pub const ROUTE: &str = "urtext-regex-v1";

/// Canonical subject ← the surface forms a corpus uses to name it.
///
/// Attribution is deliberately a *lexicon*, not a model: the forms are declared,
/// so the route is reproducible and its misses are inspectable. Matching is
/// case-insensitive and longest-cue-wins, so `dark energy` beats a bare `Ω` and
/// `Omega_Lambda` is not shadowed by a shorter prefix.
#[derive(Debug, Clone, Default)]
pub struct SubjectLexicon {
    /// `(canonical_subject, surface_cue)` pairs; cues are matched lowercased.
    entries: Vec<(String, String)>,
}

impl SubjectLexicon {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `cue` (a surface form) as naming `subject`. Returns `self` for
    /// chaining. Duplicate cues are harmless — the first canonical binding wins,
    /// keeping the lexicon a function from cue to subject.
    pub fn with(mut self, subject: &str, cue: &str) -> Self {
        self.entries.push((subject.to_string(), cue.to_lowercase()));
        self
    }

    /// Register several cues for one subject at once.
    pub fn with_cues(mut self, subject: &str, cues: &[&str]) -> Self {
        for cue in cues {
            self.entries.push((subject.to_string(), cue.to_lowercase()));
        }
        self
    }

    /// The cosmology subjects the harmonics corpus actually names, keyed to the
    /// surface forms measured in `sync_cost/derivations` (`Ω_Λ` 472×, `dark
    /// energy` 38×, `Omega_Lambda` 28×). A convenience starting point — a corpus
    /// with other subjects builds its own lexicon.
    pub fn harmonics() -> Self {
        Self::new().with_cues(
            "omega_lambda",
            &["Ω_Λ", "Omega_Lambda", "Omega_L", "dark energy fraction", "dark energy"],
        )
    }

    /// The subject a `Prose` region *binds to the value that immediately follows
    /// it* — or `None`. Binding is **adjacency-gated** for precision: a cue only
    /// licenses the next value when the text between the cue and the region's end
    /// is a pure relational connector (`=`, `∈`, `:`, `equals`, `is`, brackets,
    /// whitespace). "Ω_Λ = " binds; "Ω_Λ is derived below, and 13/19" does not.
    ///
    /// This is the fix for greedy attribution: without it, one `Ω_Λ` on a line
    /// captured *every* later integer on that line (`= 1/1`, `= 13/1`, section
    /// numbers) as false Ω_Λ facts. Precision over recall — an un-bound value is a
    /// visible `needs_review` gap, never a confident wrong attribution.
    ///
    /// Last-cue wins (the cue nearest the value is operative); ties on end
    /// position break toward the longer, more specific cue.
    fn binding_subject(&self, prose: &str) -> Option<String> {
        let hay = prose.to_lowercase();
        let mut best: Option<(usize, usize, &str)> = None; // (end, len, subject)
        for (subject, cue) in &self.entries {
            let Some(pos) = hay.rfind(cue.as_str()) else { continue };
            let end = pos + cue.len();
            if !is_connector(&hay[end..]) {
                continue; // cue present but not adjacent to the value — no bind
            }
            let better = match best {
                None => true,
                Some((be, bl, _)) => end > be || (end == be && cue.len() > bl),
            };
            if better {
                best = Some((end, cue.len(), subject));
            }
        }
        best.map(|(_, _, s)| s.to_string())
    }
}

/// True when `tail` (the text between a subject cue and the value) is *only* a
/// relational connector — so the cue genuinely introduces the value. Connector
/// words are blanked, then every remaining char must be relational punctuation or
/// whitespace. Any real content (another word, another number) fails the check,
/// which is the whole point: it keeps attribution local.
fn is_connector(tail: &str) -> bool {
    let mut s = tail.to_string();
    for w in ["equals", "equal", "is", "in", "of", "the", "fraction", "value", "be"] {
        s = s.replace(w, " ");
    }
    // Empty tail = cue sits exactly at the region boundary (tightest adjacency).
    s.chars().all(|c| c.is_whitespace() || "=∈≈~:[](){}<>,±".contains(c))
        // a genuine connector is short; a long tail is prose that happens to be
        // punctuation-heavy, not an introduction.
        && tail.chars().count() <= 16
}

/// One exact rational the route lifted from a span, with the subject it was
/// attributed to (or `None` — a coverage gap, not a guess).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extraction {
    /// The canonical subject, or `None` if no lexicon cue preceded the value.
    pub subject: Option<String>,
    /// Numerator of the exact reduced rational (from urtext, no `f64`).
    pub num: i128,
    /// Denominator of the exact reduced rational.
    pub den: i128,
    /// The reduced canonical form, e.g. `"13/19"` — the proposition witness.
    pub value: String,
    /// The as-written spelling in the span (`"26/38"` before reduction).
    pub spelling: String,
}

/// Lift every exact rational from `span` and attribute each to a subject.
///
/// Walks urtext regions in order, carrying the most recent subject cue forward:
/// a `Prose` region updates the current subject when it contains a lexicon cue; a
/// `Number` region that lifts to an exact rational is emitted as an [`Extraction`]
/// bound to that current subject. Deterministic and pure — same span, same
/// lexicon, same output. `Quantity` regions (unit-bearing) are out of scope for
/// this route (units are a separate F3 unit) and are skipped.
pub fn extract(span: &str, lex: &SubjectLexicon) -> Vec<Extraction> {
    // urtext needs a trailing newline to close the final region; add one if the
    // caller's span lacks it (harmless — it only affects region termination).
    let owned;
    let input = if span.ends_with('\n') {
        span
    } else {
        owned = format!("{span}\n");
        &owned
    };

    // `pending` is the subject the *immediately preceding* prose region licensed
    // for the very next value — consumed once. A cue does not persist across a
    // line; each value must be introduced by its own adjacent cue, or it is
    // unattributed. This is what makes attribution local (precision-first).
    let mut pending: Option<String> = None;
    let mut out = Vec::new();
    for region in regions(input) {
        match region.kind {
            RegionKind::Prose => {
                pending = lex.binding_subject(&region.text);
            }
            RegionKind::Number => {
                if let Some(rat) = to_rational(&region) {
                    out.push(Extraction {
                        subject: pending.take(),
                        num: rat.numerator(),
                        den: rat.denominator(),
                        value: rat.reduced_string(),
                        spelling: region.text.clone(),
                    });
                } else {
                    pending = None; // a non-lifting number still breaks adjacency
                }
            }
            _ => pending = None, // any other region (Quantity, math) breaks adjacency
        }
    }
    out
}

/// The full result of structuring one span: the sealed structurings for every
/// *attributed* value, and the *unattributed* values that need human review.
#[derive(Debug, Clone, Default)]
pub struct SpanStructuring {
    /// One sealed `(utterance, claim, structuring)` per attributed value. Two
    /// spellings of the same subject+value (`13/19`, `26/38`) seal to the **same**
    /// proposition CID — corroboration, for free.
    pub structured: Vec<Structuring>,
    /// Values the route lifted but could not attribute — the coverage gap made
    /// visible (INTAKE.md: "un-structured utterances are a visible coverage gap").
    pub needs_review: Vec<Extraction>,
}

/// Run the deterministic route over one prose span: extract, then seal each
/// *attributed* value as a proposition + structuring proposal, collecting the
/// unattributed remainder as `needs_review`.
///
/// The span is the utterance (sealed once per structuring, canonical-content
/// addressed via the `prose` bridge). Each attributed value becomes a
/// `proposition_schema` claim `(subject, num, den, value)` linked to that
/// utterance by a structuring quantum attributed to [`ROUTE`]. The prose→structure
/// judgment is thereby recorded and supersedable, never ambient.
pub fn structure_span(
    span: &str,
    lang: &str,
    lex: &SubjectLexicon,
) -> Result<SpanStructuring, QuantumError> {
    let mut result = SpanStructuring::default();
    for ex in extract(span, lex) {
        match &ex.subject {
            Some(subject) => {
                let claim = json!({
                    "subject": subject,
                    "num": ex.num,
                    "den": ex.den,
                    "value": ex.value,
                });
                let s = structure(span, lang, ROUTE, &proposition_schema(), &claim)?;
                result.structured.push(s);
            }
            None => result.needs_review.push(ex),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex() -> SubjectLexicon {
        SubjectLexicon::harmonics()
    }

    #[test]
    fn lifts_and_attributes_the_canonical_case() {
        let ex = extract("Ω_Λ = 13/19", &lex());
        assert_eq!(ex.len(), 1);
        assert_eq!(ex[0].subject.as_deref(), Some("omega_lambda"));
        assert_eq!(ex[0].value, "13/19");
    }

    #[test]
    fn two_spellings_same_subject_seal_to_one_proposition() {
        // 13/19 and 26/38 are the same value; both attributed to Ω_Λ → one CID.
        let a = structure_span("Ω_Λ = 13/19", "en", &lex()).unwrap();
        let b = structure_span("Omega_Lambda equals 26/38 exactly", "en", &lex()).unwrap();
        assert_eq!(a.structured.len(), 1);
        assert_eq!(b.structured.len(), 1);
        assert_eq!(
            a.structured[0].claim.cid, b.structured[0].claim.cid,
            "same subject + same value → one proposition, corroborated"
        );
        // ...but the utterances differ (distinct prose) — NL is projection.
        assert_ne!(a.structured[0].utterance.cid, b.structured[0].utterance.cid);
    }

    #[test]
    fn unattributed_value_goes_to_needs_review_not_a_guess() {
        // A bare value with no subject cue must NOT be silently attributed.
        let r = structure_span("the coefficient is 3/7 here", "en", &lex()).unwrap();
        assert!(r.structured.is_empty(), "no cue → nothing structured");
        assert_eq!(r.needs_review.len(), 1);
        assert_eq!(r.needs_review[0].value, "3/7");
        assert_eq!(r.needs_review[0].subject, None);
    }

    #[test]
    fn only_the_adjacent_value_binds_not_the_whole_line() {
        // "Ω_Λ ∈ [13/19, 11/16]": only 13/19 is introduced by the cue; 11/16 sits
        // behind a ", " with no cue, so it is NOT silently attributed. This is the
        // fix for greedy binding — precision over recall, the gap stays visible.
        let ex = extract("Ω_Λ ∈ [13/19, 11/16]", &lex());
        assert_eq!(ex.len(), 2);
        assert_eq!(ex[0].value, "13/19");
        assert_eq!(ex[0].subject.as_deref(), Some("omega_lambda"));
        assert_eq!(ex[1].value, "11/16");
        assert_eq!(ex[1].subject, None, "second value is not adjacent to the cue");
    }

    #[test]
    fn a_cue_does_not_capture_unrelated_later_integers() {
        // The real-corpus bug: "Ω_Λ = 13/19 is the 13:6 ratio" must not attribute
        // the later 13 (or 6) to Ω_Λ. Only the introduced 13/19 binds.
        let ex = extract("Ω_Λ = 13/19 is the 13:6 combinatorial ratio", &lex());
        let bound: Vec<_> = ex.iter().filter(|e| e.subject.is_some()).collect();
        assert_eq!(bound.len(), 1, "exactly one value is introduced by the cue");
        assert_eq!(bound[0].value, "13/19");
    }

    #[test]
    fn approximate_values_do_not_lift() {
        // urtext refuses to rationalize a declared-approximate number (~), so it
        // never enters structure as if exact.
        let ex = extract("Ω_Λ ≈ ~0.68 roughly", &lex());
        assert!(ex.is_empty(), "approximate marker blocks the exact lift");
    }

    #[test]
    fn structuring_is_attributed_to_the_route() {
        let r = structure_span("Ω_Λ = 13/19", "en", &lex()).unwrap();
        let annot = r.structured[0].structuring.field("annotator").unwrap();
        assert_eq!(annot.as_str(), Some(ROUTE));
    }
}
