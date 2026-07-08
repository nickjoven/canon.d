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
            &[
                "Ω_Λ",
                "Omega_Lambda",
                "Omega_L",
                "dark energy fraction",
                "dark energy",
            ],
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
            let Some(pos) = hay.rfind(cue.as_str()) else {
                continue;
            };
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

    /// The subject a `Prose` region binds to the value that immediately
    /// *precedes* it — the mirror of [`binding_subject`](Self::binding_subject),
    /// for the postfix route ("13/19 = Ω_Λ", "13/19 (dark energy)"). A cue binds
    /// only when the text from the region start up to the cue is a pure connector,
    /// so the value genuinely leads into the name. First-cue wins (nearest the
    /// value); ties break toward the longer cue.
    fn binding_subject_leading(&self, prose: &str) -> Option<String> {
        let hay = prose.to_lowercase();
        let mut best: Option<(usize, usize, &str)> = None; // (start, len, subject)
        for (subject, cue) in &self.entries {
            let Some(pos) = hay.find(cue.as_str()) else {
                continue;
            };
            if !is_connector(&hay[..pos]) {
                continue; // cue present but not adjacent to the value — no bind
            }
            let better = match best {
                None => true,
                Some((bs, bl, _)) => pos < bs || (pos == bs && cue.len() > bl),
            };
            if better {
                best = Some((pos, cue.len(), subject));
            }
        }
        best.map(|(_, _, s)| s.to_string())
    }
}

/// Which surface direction a route reads the cue relative to the value. Two
/// *independent* deterministic readings of the same corpus — where they agree,
/// they corroborate; where only one fires, the [cross-audit](crate::intake) keeps
/// the fact provisional. Prefix is "Ω_Λ = 13/19"; postfix is "13/19 = Ω_Λ".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Cue precedes the value: `Ω_Λ = 13/19`.
    Prefix,
    /// Value precedes the cue: `13/19 = Ω_Λ`, `13/19 (dark energy)`.
    Postfix,
}

impl Route {
    /// The stable annotator identity this direction seals under.
    pub fn annotator(self) -> &'static str {
        match self {
            Route::Prefix => "urtext-regex-prefix-v1",
            Route::Postfix => "urtext-regex-postfix-v1",
        }
    }
}

/// True when `tail` (the text between a subject cue and the value) is *only* a
/// relational connector — so the cue genuinely introduces the value. Connector
/// words are blanked, then every remaining char must be relational punctuation or
/// whitespace. Any real content (another word, another number) fails the check,
/// which is the whole point: it keeps attribution local.
fn is_connector(tail: &str) -> bool {
    let mut s = tail.to_string();
    for w in [
        "equals", "equal", "is", "in", "of", "the", "fraction", "value", "be",
    ] {
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

/// Lift every exact rational from `span` and attribute each via the **prefix**
/// route (`Ω_Λ = 13/19`). Equivalent to [`extract_with`] with [`Route::Prefix`];
/// kept as the ergonomic default.
pub fn extract(span: &str, lex: &SubjectLexicon) -> Vec<Extraction> {
    extract_with(span, lex, Route::Prefix)
}

/// Lift every exact rational from `span` and attribute each to a subject, reading
/// the cue in the given [`Route`]'s direction.
///
/// Walks urtext regions in order; a `Number` region that lifts to an exact
/// rational is emitted as an [`Extraction`], attributed to the adjacent cue (if
/// any) on the route's side — the preceding prose for [`Route::Prefix`], the
/// following prose for [`Route::Postfix`]. Attribution is adjacency-gated and
/// consumed once, so a cue never captures a non-adjacent value. Deterministic and
/// pure. `Quantity` regions (unit-bearing) are out of scope and break adjacency.
pub fn extract_with(span: &str, lex: &SubjectLexicon, route: Route) -> Vec<Extraction> {
    // urtext needs a trailing newline to close the final region; add one if the
    // caller's span lacks it (harmless — it only affects region termination).
    let owned;
    let input = if span.ends_with('\n') {
        span
    } else {
        owned = format!("{span}\n");
        &owned
    };
    match route {
        Route::Prefix => extract_prefix(input, lex),
        Route::Postfix => extract_postfix(input, lex),
    }
}

/// Prefix walk: the cue in the *preceding* prose licenses the *next* value.
fn extract_prefix(input: &str, lex: &SubjectLexicon) -> Vec<Extraction> {
    // `pending` is the subject the immediately preceding prose region licensed for
    // the very next value — consumed once. A cue does not persist across a line;
    // each value needs its own adjacent cue or it is unattributed (precision-first).
    let mut pending: Option<String> = None;
    let mut out = Vec::new();
    for region in regions(input) {
        match region.kind {
            RegionKind::Prose => pending = lex.binding_subject(&region.text),
            RegionKind::Number => match to_rational(&region) {
                Some(rat) => out.push(rat_extraction(pending.take(), &rat, &region.text)),
                None => pending = None, // a non-lifting number still breaks adjacency
            },
            _ => pending = None, // any other region (Quantity, math) breaks adjacency
        }
    }
    out
}

/// Postfix walk: a value binds to the cue in the *following* prose. The mirror of
/// [`extract_prefix`] — a value is held until the next region decides it.
fn extract_postfix(input: &str, lex: &SubjectLexicon) -> Vec<Extraction> {
    let mut held: Option<Extraction> = None;
    let mut out = Vec::new();
    for region in regions(input) {
        match region.kind {
            RegionKind::Number => {
                if let Some(prev) = held.take() {
                    out.push(prev); // no cue followed the previous value → unattributed
                }
                if let Some(rat) = to_rational(&region) {
                    held = Some(rat_extraction(None, &rat, &region.text));
                }
            }
            RegionKind::Prose => {
                if let Some(mut ex) = held.take() {
                    ex.subject = lex.binding_subject_leading(&region.text);
                    out.push(ex);
                }
            }
            _ => {
                if let Some(prev) = held.take() {
                    out.push(prev);
                }
            }
        }
    }
    if let Some(prev) = held.take() {
        out.push(prev); // a value ending the span, with nothing after to name it
    }
    out
}

fn rat_extraction(subject: Option<String>, rat: &urtext::Rat, spelling: &str) -> Extraction {
    Extraction {
        subject,
        num: rat.numerator(),
        den: rat.denominator(),
        value: rat.reduced_string(),
        spelling: spelling.to_string(),
    }
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

/// Run the **prefix** route over one span. Equivalent to [`structure_span_with`]
/// with [`Route::Prefix`]; the ergonomic default.
pub fn structure_span(
    span: &str,
    lang: &str,
    lex: &SubjectLexicon,
) -> Result<SpanStructuring, QuantumError> {
    structure_span_with(span, lang, lex, Route::Prefix)
}

/// Run one deterministic route over a span: extract, then seal each *attributed*
/// value as a proposition + structuring proposal, collecting the unattributed
/// remainder as `needs_review`.
///
/// The span is the utterance (sealed once, canonical-content addressed via the
/// `prose` bridge). Each attributed value becomes a `proposition_schema` claim
/// `(subject, num, den, value)` linked to that utterance by a structuring quantum
/// attributed to the route's [`annotator`](Route::annotator) — so two routes'
/// readings are distinguishable and cross-auditable, and a re-run of one route
/// supersedes its own prior structuring rather than forking.
pub fn structure_span_with(
    span: &str,
    lang: &str,
    lex: &SubjectLexicon,
    route: Route,
) -> Result<SpanStructuring, QuantumError> {
    let mut result = SpanStructuring::default();
    for ex in extract_with(span, lex, route) {
        match &ex.subject {
            Some(subject) => {
                let claim = json!({
                    "subject": subject,
                    "num": ex.num,
                    "den": ex.den,
                    "value": ex.value,
                });
                let s = structure(span, lang, route.annotator(), &proposition_schema(), &claim)?;
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
        assert_eq!(
            ex[1].subject, None,
            "second value is not adjacent to the cue"
        );
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
        assert_eq!(annot.as_str(), Some(Route::Prefix.annotator()));
    }

    #[test]
    fn postfix_route_reads_the_mirror_form() {
        // "13/19 = Ω_Λ" and "13/19 (dark energy)" — value first, cue after.
        for span in ["13/19 = Ω_Λ", "13/19 (dark energy)"] {
            let ex = extract_with(span, &lex(), Route::Postfix);
            assert_eq!(ex.len(), 1, "one value in {span:?}");
            assert_eq!(
                ex[0].subject.as_deref(),
                Some("omega_lambda"),
                "in {span:?}"
            );
            assert_eq!(ex[0].value, "13/19");
        }
    }

    #[test]
    fn each_route_fires_only_on_its_own_direction() {
        // The prefix form is invisible to the postfix route and vice-versa — the
        // two are genuinely independent, so agreement between them is meaningful.
        let prefix_form = "Ω_Λ = 13/19";
        assert!(extract_with(prefix_form, &lex(), Route::Prefix)[0]
            .subject
            .is_some());
        assert!(extract_with(prefix_form, &lex(), Route::Postfix)[0]
            .subject
            .is_none());

        let postfix_form = "13/19 = Ω_Λ";
        assert!(extract_with(postfix_form, &lex(), Route::Postfix)[0]
            .subject
            .is_some());
        assert!(extract_with(postfix_form, &lex(), Route::Prefix)[0]
            .subject
            .is_none());
    }

    #[test]
    fn both_routes_seal_the_same_proposition_cid() {
        // Independent routes reading the same value → one proposition → the CID a
        // cross-audit corroborates across routes.
        let p = structure_span_with("Ω_Λ = 13/19", "en", &lex(), Route::Prefix).unwrap();
        let q = structure_span_with("13/19 = Ω_Λ", "en", &lex(), Route::Postfix).unwrap();
        assert_eq!(p.structured[0].claim.cid, q.structured[0].claim.cid);
        // ...but the structuring proposals are distinct (different annotators).
        assert_ne!(
            p.structured[0].structuring.cid,
            q.structured[0].structuring.cid
        );
    }
}
