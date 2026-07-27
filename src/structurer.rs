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

use std::collections::{BTreeMap, BTreeSet};

use crate::bridge::{structure, Structuring};
use crate::intake::{RatioClaim, Structurer, StructuringOutput};
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
        Self::new()
            .with_cues(
                "omega_lambda",
                &[
                    "Ω_Λ",
                    "Omega_Lambda",
                    "Omega_L",
                    "dark energy fraction",
                    "dark energy",
                ],
            )
            .with_cues("omega_b", &["Ω_b", "Omega_b", "baryon fraction"])
            .with_cues("omega_dm", &["Ω_DM", "Omega_DM", "dark matter fraction"])
            // Card 3e: the boundary weights. `w_+ = 13/14` sealed under
            // omega_b in v1 because no w_plus subject existed to claim it.
            .with_cues("w_plus", &["w_+", "w_plus"])
            .with_cues("w_minus", &["w_-", "w_minus"])
    }

    /// The subject a `Prose` region *binds to the value that immediately follows
    /// it* — or `None`. Binding is **assignment-scoped** (v2): a cue only
    /// licenses the next value when the text between the cue and the region's
    /// end classifies as a single-expression connector — see
    /// [`classify_connector`]. "Ω_Λ = " binds; "Ω_Λ is derived below, and
    /// 13/19" does not; nor does anything across a comma.
    ///
    /// This is the fix for greedy attribution: without it, one `Ω_Λ` on a line
    /// captured *every* later integer on that line (`= 1/1`, `= 13/1`, section
    /// numbers) as false Ω_Λ facts. Precision over recall — an un-bound value is a
    /// visible `needs_review` gap, never a confident wrong attribution.
    ///
    /// Last-cue wins (the cue nearest the value is operative); ties on end
    /// position break toward the longer, more specific cue.
    fn binding_subject(&self, prose: &str) -> Option<(String, Connector)> {
        let hay = prose.to_lowercase();
        // (end, len, subject, connector)
        let mut best: Option<(usize, usize, &str, Connector)> = None;
        for (subject, cue) in &self.entries {
            let Some(pos) = hay.rfind(cue.as_str()) else {
                continue;
            };
            // An operator or digit hugging the cue from the left means the
            // cue is a term inside an expression, not a bare subject:
            // `1 − w_+ = 1/14` must not seal w_plus = 1/14 (the complement
            // of the actual claim), and `2w_+` is not `w_+`.
            let pre = hay[..pos].trim_end();
            let before = pre.chars().next_back();
            if before.is_some_and(|c| OPERATORS.contains(c) || c.is_ascii_digit()) {
                continue;
            }
            // A compound subject list — the cue preceded by `<other cue>,` —
            // names several subjects at once: `(Ω_Λ, Ω_DM, Ω_b) = (13/19,
            // 5/19, 1/19)` must not seal the tuple's first value under the
            // LAST subject. An ordinary clause comma (`…, Ω_Λ = 13/19`) has
            // no cue before it and still binds.
            if let Some(list) = pre.strip_suffix(',').map(str::trim_end) {
                if self.entries.iter().any(|(_, c)| list.ends_with(c.as_str())) {
                    continue;
                }
            }
            let end = pos + cue.len();
            let Some(conn) = classify_connector(&hay[end..], false) else {
                continue; // cue present but not in one expression with the value
            };
            let better = match best {
                None => true,
                Some((be, bl, _, _)) => end > be || (end == be && cue.len() > bl),
            };
            if better {
                best = Some((end, cue.len(), subject, conn));
            }
        }
        best.map(|(_, _, s, c)| (s.to_string(), c))
    }

    /// The subject a `Prose` region binds to the value that immediately
    /// *precedes* it — the mirror of [`binding_subject`](Self::binding_subject),
    /// for the postfix route ("13/19 = Ω_Λ", "13/19 (dark energy)"). A cue binds
    /// only when the text from the region start up to the cue classifies as a
    /// single-expression connector in the leading direction (mirror assignment
    /// or parenthetical appositive; a label colon refuses). First-cue wins
    /// (nearest the value); ties break toward the longer cue.
    fn binding_subject_leading(&self, prose: &str) -> Option<(String, Connector)> {
        let hay = prose.to_lowercase();
        // (start, len, subject, connector)
        let mut best: Option<(usize, usize, &str, Connector)> = None;
        for (subject, cue) in &self.entries {
            let Some(pos) = hay.find(cue.as_str()) else {
                continue;
            };
            // The mirror of the prefix walk's expression guard: an operator
            // or digit hugging the cue from the right (`13/19 (w_+ / 2)`)
            // means the cue is a term inside an expression, not the name
            // the value leads into.
            let after = hay[pos + cue.len()..].trim_start().chars().next();
            if after.is_some_and(|c| OPERATORS.contains(c) || c.is_ascii_digit()) {
                continue;
            }
            let Some(conn) = classify_connector(&hay[..pos], true) else {
                continue; // cue present but not in one expression with the value
            };
            let better = match best {
                None => true,
                Some((bs, bl, _, _)) => pos < bs || (pos == bs && cue.len() > bl),
            };
            if better {
                best = Some((pos, cue.len(), subject, conn));
            }
        }
        best.map(|(_, _, s, c)| (s.to_string(), c))
    }

    /// The lexicon as a subject normalizer (harmonics#328 Card 3d): every cue,
    /// lowercased, mapped to its canonical subject — first binding wins, same
    /// as attribution. Fed to `IntakeConfig::subject_aliases` so the strict
    /// scanner's verbatim spellings (`Omega_Lambda = 11/16`) group into the
    /// same proposition as the lexicon routes' canonical subject, instead of
    /// sealing a case-split pair of CIDs.
    pub fn alias_map(&self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for (subject, cue) in &self.entries {
            map.entry(cue.clone()).or_insert_with(|| subject.clone());
        }
        map
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
    /// The stable annotator identity this direction seals under. v2 =
    /// assignment-scoped binding + the value gate (harmonics#328 Card 3):
    /// the judgment procedure changed, so the identity must — a v2 report
    /// is distinguishable from a v1 report at every sealed structuring.
    pub fn annotator(self) -> &'static str {
        match self {
            Route::Prefix => "urtext-regex-prefix-v2",
            Route::Postfix => "urtext-regex-postfix-v2",
        }
    }
}

/// Arithmetic context characters. A value or cue touching one of these is a
/// *fragment of a larger expression*, never a bare `subject = value`
/// assignment — the completion of Card 3a's assignment scoping. The two
/// junk classes this kills surfaced on the first v2 re-ingest:
/// `w_+ = 0.04930 / (1 − 0.0986)` sealed the formula's head as
/// `w_plus = 493/10000`, and `1 − w_+ = 1/14` sealed the complement of the
/// actual claim. `*` is deliberately absent: in a markdown corpus it is
/// emphasis (`**w_+ = 12/13**` is a bare assignment, bold), and this corpus
/// writes multiplication as `·` or `×`.
const OPERATORS: &str = "+-−·×/^";

/// What kind of connector sits between a subject cue and its value — the v2
/// binding evidence (harmonics#328 Card 3a). Carried through to the value
/// gate: integer (den-1) values bind only through an explicit equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Connector {
    /// The connector contains an explicit `=` (or an equality word). This is
    /// the "explicit `= N` assignment" den-1 values require (Card 3c).
    has_eq: bool,
}

/// Classify the text between a subject cue and the value — `Some` only when
/// the cue and value sit inside **one assignment expression**. v1 accepted any
/// short punctuation-and-whitespace gap, which bound across clause boundaries:
/// `Ω_b = 35/132, Ω_Λ = …` donated Ω_Λ to 35/132 through the comma, and
/// `2018: Ω_b …` read the year as an Ω_b value through the label colon.
///
/// v2 refuses clause punctuation (`,`, `;`) outright, refuses a *leading*
/// colon (label syntax, not a mirror assignment; a trailing `X: 13/19` is
/// still a legitimate table form), and requires an explicit relational token —
/// bare adjacency (`Ω_b 0.12`) no longer attributes. `leading` is the postfix
/// direction: the text runs from the value up to the cue.
fn classify_connector(tail: &str, leading: bool) -> Option<Connector> {
    if tail.contains(',') || tail.contains(';') || (leading && tail.contains(':')) {
        return None; // clause boundary or label — two expressions, not one
    }
    // a genuine connector is short; a long tail is prose that happens to be
    // punctuation-heavy, not an introduction.
    if tail.chars().count() > 16 {
        return None;
    }
    let mut s = tail.to_string();
    for w in ["equals", "equal", "is", "in"] {
        s = s.replace(w, "="); // equality/membership words are relations
    }
    for w in ["of", "the", "fraction", "value", "be"] {
        s = s.replace(w, " ");
    }
    if !s
        .chars()
        .all(|c| c.is_whitespace() || "=∈≈~:[](){}<>±".contains(c))
    {
        return None; // real content between cue and value — not adjacent
    }
    let relational = s.chars().any(|c| "=∈≈~:".contains(c));
    // `13/19 (dark energy)` — the parenthetical appositive is postfix's
    // second legitimate form alongside the mirror assignment `13/19 = Ω_Λ`.
    let appositive = leading && s.trim_start().starts_with('(');
    if !relational && !appositive {
        return None; // v2: adjacency without a relation is not an assignment
    }
    Some(Connector {
        has_eq: s.contains('='),
    })
}

/// The v2 value gate (harmonics#328 Card 3b/3c) — exclusions that no subject
/// cue can override:
///
///   * a bare 4-digit integer in year range is a date artifact (`2018: Ω_b …`
///     sealed `omega_b = 2018/1` under v1), and
///   * a den-1 witness (`0/1`, `13/1`, `181/1`) binds only through an explicit
///     `=` — an integer next to a name is usually a count, a section number,
///     or a date, not the subject's exact rational.
///
/// A refused value stays in the output unattributed — a visible coverage gap,
/// never a confident wrong fact.
fn attributable(ex: &Extraction, conn: Connector) -> bool {
    if ex.den == 1 {
        let bare_year = !ex.spelling.contains('/') && (1000..=2100).contains(&ex.num);
        if bare_year || !conn.has_eq {
            return false;
        }
    }
    true
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

/// True when `next` (the text following a value) marks the value as
/// non-final: a `%` makes it a percentage rendering (`6.7%` sealed
/// `omega_b = 67/10` under v1, Card 3b); an arithmetic operator makes it the
/// head of a longer expression (`w_+ = (14 - 1) / 14` must not seal
/// w_plus = 14/1 — the RHS is a formula, its first number is not the value).
/// No cue on either side can rescue such a value.
fn value_continues(next: &str) -> bool {
    next.starts_with('%')
        || next
            .trim_start()
            .chars()
            .next()
            .is_some_and(|c| OPERATORS.contains(c))
}

/// [`value_continues`] read against the region after `i` — the prefix walk's
/// lookahead form — plus the ratio-list rule: a colon-ONLY gap onto another
/// number is a partition/list continuation (`Ω_b = 13 : 5 : 1 / 19` — 13 is
/// the partition's first share, not Ω_b's value), while a colon followed by
/// words is an ordinary sentence colon and does not disturb the bind.
fn expression_continues(regs: &[urtext::Region], i: usize) -> bool {
    let Some(next) = regs.get(i + 1) else {
        return false;
    };
    if value_continues(&next.text) {
        return true;
    }
    next.text.trim() == ":"
        && regs
            .get(i + 2)
            .is_some_and(|r| r.kind == RegionKind::Number)
}

/// Prefix walk: the cue in the *preceding* prose licenses the *next* value.
fn extract_prefix(input: &str, lex: &SubjectLexicon) -> Vec<Extraction> {
    // `pending` is the subject the immediately preceding prose region licensed for
    // the very next value — consumed once. A cue does not persist across a line;
    // each value needs its own adjacent cue or it is unattributed (precision-first).
    let regs = regions(input);
    let mut pending: Option<(String, Connector)> = None;
    let mut out = Vec::new();
    for (i, region) in regs.iter().enumerate() {
        match region.kind {
            RegionKind::Prose => pending = lex.binding_subject(&region.text),
            RegionKind::Number => match to_rational(region) {
                Some(rat) => {
                    let bound = pending.take();
                    let mut ex = rat_extraction(None, &rat, &region.text);
                    if !expression_continues(&regs, i) {
                        if let Some((subject, conn)) = bound {
                            if attributable(&ex, conn) {
                                ex.subject = Some(subject);
                            }
                        }
                    }
                    out.push(ex);
                }
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
                    // a leading `%` or operator marks the held value as a
                    // percentage / formula fragment — excluded before any
                    // cue in this region can bind it.
                    if !value_continues(&region.text) {
                        if let Some((subject, conn)) = lex.binding_subject_leading(&region.text) {
                            if attributable(&ex, conn) {
                                ex.subject = Some(subject);
                            }
                        }
                    }
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

/// The deterministic routes as spine [`Structurer`]s — Unit 2's wiring. A
/// prefix or postfix reading enters the **same** intake pipeline as
/// `ratio/v1`, so cross-route agreement is measured by the spine's claim
/// grouping instead of a parallel path (the overlap recorded at the
/// quantum-tier merge, resolved).
///
/// Attributed extractions become ratio claims; unattributed ones are
/// *counted* (`StructuringOutput::unattributed` → corpus telemetry), not
/// silently dropped and not flooded into the review queue — over a real
/// corpus the unattributed tail is thousands of values, and a queue nobody
/// can clear is as silent as a drop.
pub struct LexiconRatioRoute {
    lex: SubjectLexicon,
    dir: Route,
}

impl LexiconRatioRoute {
    pub fn new(lex: SubjectLexicon, dir: Route) -> Self {
        LexiconRatioRoute { lex, dir }
    }

    /// The harmonics lexicon, reading in the given direction.
    pub fn harmonics(dir: Route) -> Self {
        Self::new(SubjectLexicon::harmonics(), dir)
    }
}

impl Structurer for LexiconRatioRoute {
    fn id(&self) -> &str {
        self.dir.annotator()
    }
    fn structure(&self, _doc: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        for ex in extract_with(text, &self.lex, self.dir) {
            match ex.subject {
                Some(subject) => {
                    let claim = RatioClaim {
                        subject,
                        num: ex.num,
                        den: ex.den,
                    };
                    if !out.ratios.contains(&claim) {
                        out.ratios.push(claim);
                    }
                }
                None => out.unattributed += 1,
            }
        }
        out
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
    fn lexicon_routes_feed_the_spine_and_agreement_is_attributed() {
        // Unit 2's wiring, end to end: one doc says the same fact three ways —
        // the strict scanner form, the prefix form, the postfix form. The
        // spine groups them into ONE proposition whose route set records the
        // three independent readers. That route set is the corroboration the
        // parallel pipeline used to compute separately.
        use crate::intake::{intake_with_routes, IntakeConfig, RatioRoute, Structurer};

        let doc = "omega_lambda = 13/19\nΩ_Λ = 13/19\nand 26/38 = Ω_Λ closes it\n";
        let prefix = LexiconRatioRoute::harmonics(Route::Prefix);
        let postfix = LexiconRatioRoute::harmonics(Route::Postfix);
        let routes: [&dyn Structurer; 3] = [&RatioRoute, &prefix, &postfix];
        let r = intake_with_routes("doc", doc, &IntakeConfig::new("t"), &routes).unwrap();

        assert_eq!(r.propositions.len(), 1, "one claim, three readings");
        let p = &r.propositions[0];
        assert_eq!(p.witness, "13/19", "26/38 reduces into the same witness");
        assert_eq!(
            p.routes,
            vec![
                "ratio/v1".to_string(),
                Route::Postfix.annotator().to_string(),
                Route::Prefix.annotator().to_string(),
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
            "all three routes attributed"
        );
    }

    #[test]
    fn a_comma_never_donates_a_subject_across_assignments() {
        // The Card 3a bug: on a multi-assignment line, v1's postfix walk bound
        // the FIRST assignment's value to the SECOND assignment's subject
        // through the comma (audit_punch_list_2026-04.md:17 sealed
        // omega_lambda = 35/132). v2 refuses clause punctuation.
        let line = "Ω_b = 35/132, Ω_Λ = 13/19";
        let post = extract_with(line, &lex(), Route::Postfix);
        assert!(
            post.iter().all(|e| e.subject.is_none()),
            "no postfix bind crosses the comma: {post:?}"
        );
        // ...while the prefix walk still reads both assignments correctly.
        let pre = extract_with(line, &lex(), Route::Prefix);
        let bound: Vec<_> = pre
            .iter()
            .filter_map(|e| e.subject.as_deref().map(|s| (s, e.value.as_str())))
            .collect();
        assert_eq!(
            bound,
            vec![("omega_b", "35/132"), ("omega_lambda", "13/19")]
        );
    }

    #[test]
    fn a_year_label_is_not_a_value() {
        // The Card 3b bug: "2018: Ω_b …" sealed omega_b = 2018/1 — the label
        // colon read as a mirror assignment. v2 refuses the leading colon AND
        // the bare 4-digit integer, independently.
        let ex = extract_with("2018: Ω_b = 13/264 holds", &lex(), Route::Postfix);
        let year = ex.iter().find(|e| e.value == "2018/1").unwrap();
        assert_eq!(year.subject, None, "a year never attributes");
        // The year is excluded even through an explicit assignment.
        let ex = extract("Ω_b = 2018", &lex());
        assert_eq!(ex[0].subject, None, "4-digit bare integer, even with `=`");
        // ...and the same line's genuine assignment still binds via prefix.
        let ex = extract("2018: Ω_b = 13/264 holds", &lex());
        let bound: Vec<_> = ex.iter().filter(|e| e.subject.is_some()).collect();
        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].value, "13/264");
        assert_eq!(bound[0].subject.as_deref(), Some("omega_b"));
    }

    #[test]
    fn a_percentage_is_not_an_exact_rational() {
        // The Card 3b bug: "6.7%" sealed omega_b = 67/10 — a percentage
        // rendering read as the subject's value. v2 excludes %-context on
        // both routes; the value stays visible as unattributed.
        let ex = extract("Ω_b = 0.12% residual", &lex());
        assert!(
            ex.iter().all(|e| e.subject.is_none()),
            "prefix: % blocks the bind: {ex:?}"
        );
        let ex = extract_with("0.12% (baryon fraction)", &lex(), Route::Postfix);
        assert!(
            ex.iter().all(|e| e.subject.is_none()),
            "postfix: % blocks the bind: {ex:?}"
        );
    }

    #[test]
    fn integers_bind_only_through_an_explicit_eq() {
        // Card 3c: den-1 witnesses (0/1, 13/1, 181/1) were junk — an integer
        // next to a name is usually a count or a section number. v2 requires
        // an explicit `=` for den-1; other relations don't suffice.
        let ex = extract("Ω_Λ ∈ 13", &lex());
        assert_eq!(ex[0].subject, None, "∈ does not license an integer");
        let ex = extract_with("13 (dark energy)", &lex(), Route::Postfix);
        assert_eq!(
            ex[0].subject, None,
            "appositive does not license an integer"
        );
        // The explicit assignment form still works (and 13 is no year).
        let ex = extract("Ω_Λ = 13", &lex());
        assert_eq!(ex[0].subject.as_deref(), Some("omega_lambda"));
        assert_eq!(ex[0].value, "13/1");
    }

    #[test]
    fn a_formula_head_is_not_the_value() {
        // First v2 re-ingest junk class: the RHS is an expression, so its
        // leading number is NOT the subject's value. `w_+ = 0.04930 / (1 −
        // 0.0986)` sealed w_plus = 493/10000; `w_+ = (14 - 1) / 14` sealed
        // w_plus = 14/1.
        for span in [
            "w_+ = 0.04930 / (1 - 0.0986) ≈ 0.9298",
            "w_+ = (14 - 1) / 14 = 13 / 14",
        ] {
            let ex = extract(span, &lex());
            assert!(
                ex.iter().all(|e| e.subject.is_none()),
                "no bind on a formula head in {span:?}: {ex:?}"
            );
        }
        // ...and the plain assignment of the same value still binds.
        let ex = extract("w_+ = 13/14", &lex());
        assert_eq!(ex[0].subject.as_deref(), Some("w_plus"));
    }

    #[test]
    fn an_expression_term_is_not_a_bare_subject() {
        // The complement misread: `1 − w_+ = 1/14` states w_+ = 13/14; v2
        // must not seal w_plus = 1/14. The cue is operator-hugged, so it is
        // a term inside an expression, not a subject being assigned.
        for span in ["1 − w_+ = 1/14", "1 - w_+ = 1/14"] {
            let ex = extract(span, &lex());
            assert!(
                ex.iter().all(|e| e.subject.is_none()),
                "no bind through an expression term in {span:?}: {ex:?}"
            );
        }
        // Mirror direction: an operator-hugged cue after the value.
        let ex = extract_with("13/19 (w_+ / 2)", &lex(), Route::Postfix);
        assert!(ex.iter().all(|e| e.subject.is_none()), "{ex:?}");
    }

    #[test]
    fn a_partition_share_is_not_the_last_subjects_value() {
        // `Ω_Λ : Ω_DM : Ω_b = 13 : 5 : 1 / 19` — 13 is the partition's first
        // share; v1-style binding gave it to the nearest cue (Ω_b). The
        // colon-ONLY gap onto another number marks the list continuation.
        let ex = extract("Ω_Λ : Ω_DM : Ω_b = 13 : 5 : 1 / 19", &lex());
        assert!(
            ex.iter().all(|e| e.subject.is_none()),
            "no bind inside a ratio list: {ex:?}"
        );
        // A sentence colon after the value does not disturb the bind.
        let ex = extract("Predictions at w_+ = 0.9298: the rows follow", &lex());
        let bound: Vec<_> = ex.iter().filter(|e| e.subject.is_some()).collect();
        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].value, "4649/5000");
    }

    #[test]
    fn a_tuple_assignment_does_not_bind_the_last_subject() {
        // `(Ω_Λ, Ω_DM, Ω_b) = (13/19, 5/19, 1/19)` — the tuple's first value
        // is Ω_Λ's, but the nearest cue to it is Ω_b. The compound-subject
        // signature (cue preceded by `<other cue>,`) refuses the bind for
        // every cue in the list.
        let ex = extract("(Ω_Λ, Ω_DM, Ω_b) = (13/19, 5/19, 1/19)", &lex());
        assert!(
            ex.iter().all(|e| e.subject.is_none()),
            "no bind inside a tuple assignment: {ex:?}"
        );
        // The multi-assignment clause comma still binds (regression guard
        // for a_comma_never_donates_a_subject_across_assignments).
        let ex = extract("as shown, Ω_Λ = 13/19 survives", &lex());
        let bound: Vec<_> = ex.iter().filter(|e| e.subject.is_some()).collect();
        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].subject.as_deref(), Some("omega_lambda"));
    }

    #[test]
    fn markdown_bold_is_not_multiplication() {
        // `**w_+ = 12/13**` is a bare assignment wearing emphasis; `*` must
        // not read as an operator hugging the cue.
        let ex = extract("unique rational solution **w_+ = 12/13**", &lex());
        let bound: Vec<_> = ex.iter().filter(|e| e.subject.is_some()).collect();
        assert_eq!(bound.len(), 1, "{ex:?}");
        assert_eq!(bound[0].subject.as_deref(), Some("w_plus"));
        assert_eq!(bound[0].value, "12/13");
    }

    #[test]
    fn bare_adjacency_no_longer_attributes() {
        // v1 accepted a whitespace-only gap ("Ω_b 0.12"), which is how prose
        // mentions became sealed values. v2 requires a relational token.
        let ex = extract("Ω_b 3/25 in passing", &lex());
        assert_eq!(ex[0].subject, None, "no relation, no bind");
    }

    #[test]
    fn the_boundary_weights_have_subjects() {
        // Card 3e: w_+ = 13/14 sealed under omega_b in v1 because no w_plus
        // subject existed to claim it.
        let ex = extract("w_+ = 13/14", &lex());
        assert_eq!(ex[0].subject.as_deref(), Some("w_plus"));
        assert_eq!(ex[0].value, "13/14");
        let ex = extract("w_- = 1/14", &lex());
        assert_eq!(ex[0].subject.as_deref(), Some("w_minus"));
    }

    #[test]
    fn ratio_scanner_subjects_fold_into_canonical() {
        // Card 3d: `Omega_Lambda = 11/16` (ratio/v1, verbatim subject) and
        // `Ω_Λ = 11/16` (lexicon route, canonical subject) must seal ONE
        // proposition, not a case-split pair.
        use crate::intake::{intake_with_routes, IntakeConfig, RatioRoute, Structurer};

        let doc = "Omega_Lambda = 11/16\nΩ_Λ = 11/16\n";
        let prefix = LexiconRatioRoute::harmonics(Route::Prefix);
        let routes: [&dyn Structurer; 2] = [&RatioRoute, &prefix];
        let mut cfg = IntakeConfig::new("t");
        cfg.subject_aliases = SubjectLexicon::harmonics().alias_map();
        let r = intake_with_routes("doc", doc, &cfg, &routes).unwrap();

        assert_eq!(r.propositions.len(), 1, "one subject, one proposition");
        assert_eq!(r.propositions[0].subject, "omega_lambda");
        assert_eq!(
            r.propositions[0].routes,
            vec![
                "ratio/v1".to_string(),
                Route::Prefix.annotator().to_string()
            ]
        );
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
