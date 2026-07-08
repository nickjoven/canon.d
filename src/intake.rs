//! The intake spine — INTAKE.md Unit 1, made mechanical.
//!
//! `intake` is the one write path from a prose document into the substrate:
//!
//! 1. **Seal the utterance** ([`crate::bridge::seal_utterance`]) — the doc's
//!    urtext-canonical content becomes content-addressed ground before any
//!    interpretation. The mode is pinned loudly in the report
//!    (`canonicalizer: "urtext" | "identity"`).
//! 2. **Run structurer routes** over the *canonical* text (never the raw
//!    bytes): the lineage route types `## Lineage` edges; the reference route
//!    finds untyped citations against the corpus id set; the ratio route
//!    lifts strict `subject = num/den` claims to exact-rational propositions.
//! 3. **Bucket every proposal** — nothing is silently dropped. A proposal ends
//!    in exactly one of: **promoted** (typed edge sealed as an
//!    [`edge_annotation`](crate::edge_annotation_schema) quantum; ratio claim
//!    sealed as proposition + grounded assertion), **needs_review** (W4-class
//!    unresolvable lineage targets, out-of-domain ratios), or **blocked**
//!    (a Falsified proposition CID re-asserted — the negative-results ledger
//!    firing at intake instead of depending on authors reading it).
//!
//! Untyped `references` are *reported*, never sealed: per [`crate::lineage`],
//! typing is an epistemic act; a flat citation is telemetry, not a claim.
//!
//! The whole pipeline is **idempotent and deterministic**: same bytes in, the
//! same report out, byte for byte. No clocks, no randomness, no iteration
//! order leaks. That is what makes it safe to wire into CI, hooks, and cron.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{json, Value};

use crate::bridge::{seal_utterance_with, Canonicalizer};
use crate::generator::Rat;
use crate::lineage::{lineage_to_annotations, parse_lineage, TypedEdge};
use crate::promote::{
    CrossAuditReport, Plausibility, Promoted, PromotionPolicy, Queued, SubjectDomains,
};
use crate::quantum::{cross_audit, CrossAuditConflict, Quantum, QuantumError};
use crate::strata::{proposition_schema, seal_assertion, term_schema, StrataError};

/// Errors that abort an intake (environment-class failures). Everything
/// content-shaped lands in a report bucket instead.
#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    #[error(transparent)]
    Quantum(#[from] QuantumError),
    #[error(transparent)]
    Strata(#[from] StrataError),
}

/// Intake configuration. `corpus_ids` is the known-document set that lineage
/// targets and references resolve against; `falsified` is the negative-results
/// set of proposition CIDs whose re-assertion is blocked at the gate;
/// `canonicalizer` is the domain pack's noise quotient (prose collapses
/// formatting noise; code and data corpora need byte-identity — DOMAINS.md F2).
#[derive(Debug, Clone, Default)]
pub struct IntakeConfig {
    pub annotator: String,
    pub lang: String,
    pub corpus_ids: BTreeSet<String>,
    pub falsified: BTreeSet<String>,
    pub canonicalizer: Canonicalizer,
    /// Subject value laws (the Unit 2 domain witness): a claim whose value
    /// falls outside its subject's declared domain is queued
    /// (`out_of_domain`), never promoted. Empty = unconstrained.
    pub domains: SubjectDomains,
    /// Corpus-level promotion threshold: a proposition corroborated by fewer
    /// distinct documents than this is queued, not promoted.
    pub policy: PromotionPolicy,
}

impl IntakeConfig {
    pub fn new(annotator: &str) -> Self {
        IntakeConfig {
            annotator: annotator.to_string(),
            lang: "en".to_string(),
            corpus_ids: BTreeSet::new(),
            falsified: BTreeSet::new(),
            canonicalizer: Canonicalizer::Text,
            domains: SubjectDomains::default(),
            policy: PromotionPolicy::default(),
        }
    }
}

/// An exact-rational claim proposal: `subject`'s value is `num/den`. Emitted
/// by any route that can read one — the strict `ratio/v1` scanner, the
/// urtext-backed prefix/postfix lexicon routes — and the deliberate
/// narrowness is the point: routes propose only what seals with an exact
/// witness; everything fuzzier stays in the utterance backlog. `i128` to
/// match the urtext lift; values outside `i64` cannot enter the `Rat`
/// algebra and are queued at the witness step, not silently truncated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RatioClaim {
    pub subject: String,
    pub num: i128,
    pub den: i128,
}

/// A route-proposed review item (DOMAINS.md F3): a route that recognizes a
/// condition worth a human's attention — a deprecated definition, a malformed
/// convention — proposes it here and the spine buckets it into `needs_review`
/// with the document filled in. Same nothing-vanishes contract as every other
/// proposal channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteFinding {
    pub kind: String,
    pub subject: String,
    pub detail: String,
}

/// A route-proposed vocabulary binding: `term → home` (the vocab layer's
/// feed; DOMAINS.md F3). The spine seals accepted bindings as `term` quanta
/// ([`crate::strata::term_schema`]) with an assertion grounded on the source
/// utterance. The pack chooses the term string's granularity — bare names
/// where one-canonical-home should be enforced, qualified names (`mod::Item`)
/// where coexistence is legitimate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TermBinding {
    pub term: String,
    pub home: String,
}

/// One structurer route's proposals for a document. Routes are deliberately
/// unequal: the lineage route proposes *typed* edges, the reference route
/// proposes *untyped* citations, the ratio route proposes claims, and any
/// route may propose findings (review items) or term bindings.
#[derive(Debug, Clone, Default)]
pub struct StructuringOutput {
    pub typed_edges: Vec<TypedEdge>,
    pub references: Vec<String>,
    pub ratios: Vec<RatioClaim>,
    pub findings: Vec<RouteFinding>,
    pub terms: Vec<TermBinding>,
    /// Values the route lifted but could not attribute to a subject — counted
    /// (surfaced in telemetry as the coverage gap), neither guessed nor
    /// flooded into the review queue.
    pub unattributed: usize,
}

/// A structurer: one route from canonical text to structure proposals. The id
/// is sealed into every annotation it produces — no anonymous extraction.
pub trait Structurer {
    fn id(&self) -> &str;
    fn structure(
        &self,
        doc_id: &str,
        canonical_text: &str,
        corpus_ids: &BTreeSet<String>,
    ) -> StructuringOutput;
}

/// The lineage route: canon.d's `## Lineage` parser with the harmonics import
/// convention layered on — `.md`/`.py` suffixes on targets are stripped, so
/// `grounds: klein_bottle.md` and `grounds: klein_bottle` propose one edge.
pub struct LineageRoute;

impl Structurer for LineageRoute {
    fn id(&self) -> &str {
        "lineage/v1"
    }
    fn structure(&self, doc_id: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        for mut e in parse_lineage(doc_id, text) {
            e.to = strip_doc_suffix(&e.to).to_string();
            if !out.typed_edges.contains(&e) {
                out.typed_edges.push(e);
            }
        }
        out
    }
}

/// The reference route: the five citation patterns of harmonics'
/// `build_derivation_graph.py::extract_references`, ported literally so the
/// shadow-mode diff against the incumbent is a diff of *corpora*, not of
/// pattern sets. Corpus-driven: it looks for each known id, so it needs
/// `corpus_ids` and finds nothing outside them (exactly like the original).
pub struct ReferenceRoute;

impl Structurer for ReferenceRoute {
    fn id(&self) -> &str {
        "references/v1"
    }
    fn structure(&self, doc_id: &str, text: &str, corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        for id in corpus {
            if id == doc_id {
                continue;
            }
            if cites(text, id) {
                out.references.push(id.clone());
            }
        }
        out
    }
}

/// The ratio route: lift strict `subject = num/den` lines to claims.
pub struct RatioRoute;

impl Structurer for RatioRoute {
    fn id(&self) -> &str {
        "ratio/v1"
    }
    fn structure(&self, _doc: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        StructuringOutput {
            ratios: extract_ratio_claims(text),
            ..Default::default()
        }
    }
}

/// The vocab route: lift glossary table rows (`| **term** | …`) to term
/// bindings. **Glossary-scoped by design**: it structures only documents
/// whose id contains `glossary` — the glossary is the corpus's *declared*
/// binding registry, while status maps and scorecards merely bold terms they
/// use; binding from usage sites is exactly the fabrication path the vocab
/// layer exists to close. `home` is the glossary document itself in v1: it
/// owns the *binding*; extracting the definition's source doc from the row is
/// a later refinement.
pub struct VocabRoute;

impl Structurer for VocabRoute {
    fn id(&self) -> &str {
        "vocab/v1"
    }
    fn structure(&self, doc_id: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        if !doc_id.contains("glossary") {
            return out;
        }
        for line in text.lines() {
            let t = line.trim_start();
            let Some(rest) = t.strip_prefix("| **") else {
                continue;
            };
            let Some(end) = rest.find("**") else {
                continue;
            };
            let term = rest[..end].trim().to_string();
            if term.is_empty() {
                continue;
            }
            let binding = TermBinding {
                term,
                home: doc_id.to_string(),
            };
            if !out.terms.contains(&binding) {
                out.terms.push(binding);
            }
        }
        out
    }
}

fn strip_doc_suffix(target: &str) -> &str {
    target
        .strip_suffix(".md")
        .or_else(|| target.strip_suffix(".py"))
        .unwrap_or(target)
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Does `text` cite document `id`? The five patterns, in the original's order:
/// `` `id.md` ``, `` `id.py` ``, `[label](id.md)`, `` (`id.md`) ``, bare
/// `id.md` on word boundaries. (Patterns 1/2/4 are substring-exact; 3 needs
/// the link form; 5 needs boundaries so `foo_id.md` doesn't match `id.md`.)
fn cites(text: &str, id: &str) -> bool {
    let code_md = format!("`{id}.md`");
    let code_py = format!("`{id}.py`");
    if text.contains(&code_md) || text.contains(&code_py) {
        return true;
    }
    let link = format!("]({id}.md)");
    if text.contains(&link) {
        return true;
    }
    // bare `id.md` with a word boundary on the left (the `.md` suffix already
    // bounds the right); this also covers the original's pattern 4.
    let bare = format!("{id}.md");
    let mut start = 0;
    while let Some(pos) = text[start..].find(&bare) {
        let at = start + pos;
        let bounded = at == 0 || !is_word(text[..at].chars().next_back().unwrap());
        let after = at + bare.len();
        let closed = text[after..].chars().next().map_or(true, |c| !is_word(c));
        if bounded && closed {
            return true;
        }
        start = at + bare.len();
    }
    false
}

/// Scan for strict `subject = num/den` claims: identifier `=` integer `/`
/// integer, all on one line, word-bounded on both flanks, `==` excluded.
/// First-occurrence order, exact duplicates deduplicated.
pub fn extract_ratio_claims(text: &str) -> Vec<RatioClaim> {
    let mut out: Vec<RatioClaim> = Vec::new();
    for line in text.lines() {
        let bytes: Vec<char> = line.chars().collect();
        for (i, &c) in bytes.iter().enumerate() {
            if c != '=' {
                continue;
            }
            // reject == on either side
            if i + 1 < bytes.len() && bytes[i + 1] == '=' || i > 0 && bytes[i - 1] == '=' {
                continue;
            }
            // left: optional spaces, then an identifier
            let mut l = i;
            while l > 0 && bytes[l - 1] == ' ' {
                l -= 1;
            }
            let id_end = l;
            while l > 0 && is_word(bytes[l - 1]) {
                l -= 1;
            }
            let subject: String = bytes[l..id_end].iter().collect();
            if subject.is_empty() || subject.chars().next().unwrap().is_ascii_digit() {
                continue;
            }
            // Left flank must be a real boundary. `is_word` is ASCII, so a
            // Greek-lettered subject like `Ω_b` used to *truncate* to `_b` —
            // a confident wrong subject over the live corpus (`_b = 1/19` is
            // Ω_b's claim under a junk name). Any alphanumeric neighbor,
            // ASCII or not, means this is a fragment: skip it. Precision
            // over recall — the lexicon routes own non-ASCII subjects.
            if l > 0 && (is_word(bytes[l - 1]) || bytes[l - 1].is_alphanumeric()) {
                continue; // left flank not word-bounded
            }
            // right: optional spaces, digits, '/', digits, bounded
            let mut r = i + 1;
            while r < bytes.len() && bytes[r] == ' ' {
                r += 1;
            }
            let num_start = r;
            while r < bytes.len() && bytes[r].is_ascii_digit() {
                r += 1;
            }
            if r == num_start || r >= bytes.len() || bytes[r] != '/' {
                continue;
            }
            let num: i128 = match bytes[num_start..r].iter().collect::<String>().parse() {
                Ok(n) => n,
                Err(_) => continue,
            };
            r += 1;
            let den_start = r;
            while r < bytes.len() && bytes[r].is_ascii_digit() {
                r += 1;
            }
            if r == den_start {
                continue;
            }
            if r < bytes.len() && is_word(bytes[r]) {
                continue; // right flank not word-bounded
            }
            let den: i128 = match bytes[den_start..r].iter().collect::<String>().parse() {
                Ok(n) => n,
                Err(_) => continue,
            };
            let claim = RatioClaim { subject, num, den };
            if !out.contains(&claim) {
                out.push(claim);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// report types
// ---------------------------------------------------------------------------

/// A promoted, sealed typed edge.
#[derive(Debug, Clone, Serialize)]
pub struct EdgeOut {
    pub from: String,
    pub to: String,
    pub kind: String,
    /// CID of the sealed `edge_annotation` quantum.
    pub annotation_cid: String,
}

/// A promoted ratio claim: proposition + the assertion grounding it in the
/// source utterance. One entry per distinct `(subject, witness)` in the doc,
/// however many routes proposed it — the routes are recorded, so promotion
/// is attributed, never anonymous (INTAKE.md Unit 2).
#[derive(Debug, Clone, Serialize)]
pub struct PropositionOut {
    pub subject: String,
    pub num: i64,
    pub den: i64,
    /// Exact reduced witness (`Rat::reduced_string`).
    pub witness: String,
    /// The structurer routes that independently proposed this claim in this
    /// document. Cross-route agreement is corroboration.
    pub routes: Vec<String>,
    pub proposition_cid: String,
    pub assertion_cid: String,
}

/// A promoted term binding: term quantum + the assertion grounding it in the
/// source utterance.
#[derive(Debug, Clone, Serialize)]
pub struct TermOut {
    pub term: String,
    pub home: String,
    pub term_cid: String,
    pub assertion_cid: String,
}

/// One human-review queue entry. `kind` is spine vocabulary
/// (`unresolved_lineage_target` (W4), `invalid_ratio`,
/// `cross_audit_conflict`, `term_home_conflict`) or a route-proposed kind
/// ([`RouteFinding`], e.g. the code pack's `deprecated_item`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ReviewItem {
    pub kind: String,
    pub doc: String,
    pub subject: String,
    pub detail: String,
}

/// A re-assertion of a Falsified proposition, refused at the gate.
#[derive(Debug, Clone, Serialize)]
pub struct Blocked {
    pub doc: String,
    pub subject: String,
    pub proposition_cid: String,
}

/// The full result of one document's intake. Serializes deterministically;
/// [`IntakeReport::exit_code`] is the U5 contract (0 clean / 1 findings).
#[derive(Debug, Clone, Serialize)]
pub struct IntakeReport {
    pub doc_id: String,
    pub utterance_cid: String,
    /// Which canonicalizer sealed the utterance — `"urtext"` with the `prose`
    /// feature, `"identity"` without. Pinned per INTAKE.md: a corpus ingested
    /// in one mode has visibly different CIDs than the other.
    pub canonicalizer: &'static str,
    pub edges: Vec<EdgeOut>,
    pub references: Vec<String>,
    pub propositions: Vec<PropositionOut>,
    pub terms: Vec<TermOut>,
    pub needs_review: Vec<ReviewItem>,
    pub blocked: Vec<Blocked>,
    /// Values routes lifted but could not attribute to a subject — the
    /// coverage gap, counted (W1 made measurable without flooding review).
    pub unattributed_values: usize,
    /// Did any route produce structure? `false` is the sealed-but-unstructured
    /// backlog (`utterance_coverage` telemetry).
    pub structured: bool,
}

impl IntakeReport {
    /// 0 = clean, 1 = review/blocked findings. (2 = environment error, which
    /// is the CLI's to report — the library returns `Err` for that class.)
    pub fn exit_code(&self) -> i32 {
        if self.needs_review.is_empty() && self.blocked.is_empty() {
            0
        } else {
            1
        }
    }
}

// ---------------------------------------------------------------------------
// the spine
// ---------------------------------------------------------------------------

/// The prose (harmonics) route roster: `## Lineage` typed edges, corpus-id
/// citations, strict `subject = num/den` ratio claims, glossary term
/// bindings. This is a *domain pack* (DOMAINS.md), not the spine — the spine
/// takes whatever routes the caller composes.
pub fn prose_routes() -> [&'static dyn Structurer; 4] {
    [&LineageRoute, &ReferenceRoute, &RatioRoute, &VocabRoute]
}

/// Intake one document with the prose pack ([`prose_routes`]). See
/// [`intake_with_routes`] for the domain-generic spine.
pub fn intake(
    doc_id: &str,
    raw_text: &str,
    cfg: &IntakeConfig,
) -> Result<IntakeReport, IntakeError> {
    intake_with_routes(doc_id, raw_text, cfg, &prose_routes())
}

/// Intake one document through an explicit route roster. See the module doc
/// for the pipeline; the invariant to hold onto: **every proposal lands in
/// exactly one report bucket**, and the report is a pure function of
/// `(doc_id, raw_text, cfg, routes)`. The spine itself is domain-free —
/// everything domain-flavored arrives through `routes` (DOMAINS.md).
pub fn intake_with_routes(
    doc_id: &str,
    raw_text: &str,
    cfg: &IntakeConfig,
    routes: &[&dyn Structurer],
) -> Result<IntakeReport, IntakeError> {
    let utterance = seal_utterance_with(cfg.canonicalizer, raw_text, &cfg.lang, &cfg.annotator)?;
    // Structure the *canonical* text — the substrate's form, not the wire form.
    let canonical = utterance
        .field("text")
        .and_then(|v| v.as_str())
        .unwrap_or(raw_text)
        .to_string();

    let mut merged = StructuringOutput::default();
    // Claims keep their proposing route: attribution survives the merge, so
    // cross-route agreement is measurable downstream (Unit 2).
    let mut route_claims: Vec<(String, RatioClaim)> = Vec::new();
    for r in routes {
        let o = r.structure(doc_id, &canonical, &cfg.corpus_ids);
        merged.typed_edges.extend(o.typed_edges);
        merged.references.extend(o.references);
        route_claims.extend(o.ratios.into_iter().map(|c| (r.id().to_string(), c)));
        merged.findings.extend(o.findings);
        merged.terms.extend(o.terms);
        merged.unattributed += o.unattributed;
    }

    let mut needs_review: Vec<ReviewItem> = Vec::new();
    let mut blocked: Vec<Blocked> = Vec::new();

    // Route findings: proposed review items pass straight through with the
    // document filled in (the route knows the condition, the spine knows the
    // doc).
    for f in &merged.findings {
        needs_review.push(ReviewItem {
            kind: f.kind.clone(),
            doc: doc_id.into(),
            subject: f.subject.clone(),
            detail: f.detail.clone(),
        });
    }

    // Typed edges: resolvable targets promote to sealed annotations; a target
    // outside the corpus is the W4 failure mode, made loud.
    let (resolved, dangling): (Vec<TypedEdge>, Vec<TypedEdge>) = merged
        .typed_edges
        .into_iter()
        .partition(|e| cfg.corpus_ids.contains(&e.to));
    for e in &dangling {
        needs_review.push(ReviewItem {
            kind: "unresolved_lineage_target".into(),
            doc: doc_id.into(),
            subject: e.to.clone(),
            detail: format!("`{}: {}` names no document in the corpus", e.kind, e.to),
        });
    }
    let annotations = lineage_to_annotations(&resolved, &cfg.annotator);
    let edges: Vec<EdgeOut> = resolved
        .iter()
        .zip(&annotations)
        .map(|(e, a)| EdgeOut {
            from: e.from.clone(),
            to: e.to.clone(),
            kind: e.kind.clone(),
            annotation_cid: a.cid.clone(),
        })
        .collect();

    // References: reported, never sealed (typing is an epistemic act).
    let mut references = merged.references;
    references.sort();
    references.dedup();

    // Ratio claims, in three steps (Unit 2 wired into the spine):
    //   1. group by (subject, reduced witness) across routes — cross-route
    //      agreement collapses to one claim with its route set;
    //   2. domain witness — a value outside its subject's declared law is
    //      queued (`out_of_domain`), never promoted;
    //   3. exact witness sealed as proposition + assertion; Falsified CIDs
    //      blocked at the gate.
    struct ClaimAcc {
        num: i64,
        den: i64,
        routes: BTreeSet<String>,
    }
    let mut grouped: BTreeMap<(String, String), ClaimAcc> = BTreeMap::new();
    for (route_id, c) in &route_claims {
        let (Ok(num), Ok(den)) = (i64::try_from(c.num), i64::try_from(c.den)) else {
            needs_review.push(ReviewItem {
                kind: "invalid_ratio".into(),
                doc: doc_id.into(),
                subject: c.subject.clone(),
                detail: format!(
                    "{}/{} is outside the i64 exact-rational domain",
                    c.num, c.den
                ),
            });
            continue;
        };
        let rat = match Rat::new(num, den) {
            Ok(r) => r,
            Err(e) => {
                needs_review.push(ReviewItem {
                    kind: "invalid_ratio".into(),
                    doc: doc_id.into(),
                    subject: c.subject.clone(),
                    detail: format!("{num}/{den} is outside the exact-rational domain: {e}"),
                });
                continue;
            }
        };
        // Seal the *reduced* identity: `x = 2/6` and `x = 1/3` are one
        // proposition (one CID), not one witness under two forms. The
        // cross-audit still re-reduces independently, so a future
        // non-reducing emitter is caught rather than trusted.
        grouped
            .entry((c.subject.clone(), rat.reduced_string()))
            .or_insert(ClaimAcc {
                num: rat.numerator(),
                den: rat.denominator(),
                routes: BTreeSet::new(),
            })
            .routes
            .insert(route_id.clone());
    }

    let mut propositions: Vec<PropositionOut> = Vec::new();
    for ((subject, witness), acc) in grouped {
        if let Plausibility::OutOfDomain { domain } =
            cfg.domains
                .witness(&subject, i128::from(acc.num), i128::from(acc.den))
        {
            needs_review.push(ReviewItem {
                kind: "out_of_domain".into(),
                doc: doc_id.into(),
                subject: subject.clone(),
                detail: format!("{witness} violates the declared value law {domain}"),
            });
            continue;
        }
        let prop = Quantum::seal(
            &proposition_schema(),
            &json!({"subject": subject, "num": acc.num, "den": acc.den, "value": witness}),
        )?;
        if cfg.falsified.contains(&prop.cid) {
            blocked.push(Blocked {
                doc: doc_id.into(),
                subject,
                proposition_cid: prop.cid,
            });
            continue;
        }
        let assertion = seal_assertion(&prop, &[&utterance], &cfg.annotator)?;
        propositions.push(PropositionOut {
            subject,
            num: acc.num,
            den: acc.den,
            witness,
            routes: acc.routes.into_iter().collect(),
            proposition_cid: prop.cid,
            assertion_cid: assertion.cid,
        });
    }

    // Term bindings: sealed as term quanta, asserted on the source utterance.
    let mut terms: Vec<TermOut> = Vec::new();
    let mut seen_terms: BTreeSet<(String, String)> = BTreeSet::new();
    for b in &merged.terms {
        if !seen_terms.insert((b.term.clone(), b.home.clone())) {
            continue;
        }
        let tq = Quantum::seal(&term_schema(), &json!({"term": b.term, "home": b.home}))?;
        let assertion = seal_assertion(&tq, &[&utterance], &cfg.annotator)?;
        terms.push(TermOut {
            term: b.term.clone(),
            home: b.home.clone(),
            term_cid: tq.cid,
            assertion_cid: assertion.cid,
        });
    }

    needs_review.sort();
    let structured = !edges.is_empty() || !propositions.is_empty() || !terms.is_empty();
    Ok(IntakeReport {
        doc_id: doc_id.to_string(),
        utterance_cid: utterance.cid,
        canonicalizer: cfg.canonicalizer.mode(),
        edges,
        references,
        propositions,
        terms,
        needs_review,
        blocked,
        unattributed_values: merged.unattributed,
        structured,
    })
}

// ---------------------------------------------------------------------------
// corpus intake
// ---------------------------------------------------------------------------

/// Corpus-level result: per-doc reports, the corpus-wide cross-audit, and
/// aggregate telemetry.
#[derive(Debug, Clone, Serialize)]
pub struct CorpusReport {
    pub reports: Vec<IntakeReport>,
    /// Cross-audit conflicts over every sealed proposition in the corpus —
    /// e.g. one witness under two forms (a non-reducing emitter) surfaces as
    /// an UnderMerge review item here, not in any single doc.
    pub cross_conflicts: Vec<ReviewItem>,
    /// The corpus-level promotion verdict (Unit 2): every distinct
    /// proposition with its corroboration (distinct documents) and the union
    /// of routes that reached it; under-corroborated propositions are queued
    /// per [`IntakeConfig::policy`]. Out-of-domain claims never got this far
    /// — they were queued per-doc by the domain witness.
    pub promotions: CrossAuditReport,
    pub telemetry: Telemetry,
}

/// The INTAKE.md §telemetry counters that Unit 1 can already measure.
#[derive(Debug, Clone, Serialize)]
pub struct Telemetry {
    pub docs: usize,
    pub utterances_sealed: usize,
    /// Sealed-but-unstructured backlog (docs with no promoted structure).
    pub unstructured: usize,
    pub edges_promoted: usize,
    pub references_untyped: usize,
    pub propositions: usize,
    pub terms_bound: usize,
    /// Promoted propositions backed by more than one document or route.
    pub corroborated: usize,
    /// Values lifted but not attributed to any subject (the coverage gap).
    pub unattributed_values: usize,
    pub needs_review_depth: usize,
    pub reassertion_blocks: usize,
}

impl CorpusReport {
    pub fn exit_code(&self) -> i32 {
        let clean = self.cross_conflicts.is_empty()
            && self.promotions.queued.is_empty()
            && self.reports.iter().all(|r| r.exit_code() == 0);
        if clean {
            0
        } else {
            1
        }
    }
}

/// Intake a whole corpus with the prose pack. See
/// [`intake_corpus_with_routes`] for the domain-generic spine.
pub fn intake_corpus(
    docs: &[(String, String)],
    cfg: &IntakeConfig,
) -> Result<CorpusReport, IntakeError> {
    intake_corpus_with_routes(docs, cfg, &prose_routes())
}

/// Intake a whole corpus through an explicit route roster. `docs` is
/// `(doc_id, raw_text)`; the corpus id set is the docs' own ids plus
/// `cfg.corpus_ids` (so edges may resolve to documents already in the
/// substrate but not in this batch). Deterministic: docs are processed in
/// sorted id order regardless of input order.
pub fn intake_corpus_with_routes(
    docs: &[(String, String)],
    cfg: &IntakeConfig,
    routes: &[&dyn Structurer],
) -> Result<CorpusReport, IntakeError> {
    let mut sorted: Vec<&(String, String)> = docs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut full_cfg = cfg.clone();
    full_cfg
        .corpus_ids
        .extend(docs.iter().map(|(id, _)| id.clone()));

    let mut reports = Vec::new();
    for (id, text) in sorted {
        reports.push(intake_with_routes(id, text, &full_cfg, routes)?);
    }

    // Corpus-wide cross-audit: re-seal every promoted proposition (idempotent —
    // same CIDs) and diff form against witness. Grouped **per subject**: the
    // witness is the value alone, so two different subjects sharing a value
    // (x = 1/3 and y = 1/3) legitimately have one witness under two identity
    // CIDs — that is the corpus talking about two things, not an UnderMerge.
    // Auditing the whole bag at once reports exactly that false positive (the
    // same class quantum.rs pins in
    // `grounds_in_identity_false_positives_on_corroboration`); within one
    // subject, one witness under two forms IS the non-reducing-emitter bug.
    let schema = proposition_schema();
    let mut by_subject: BTreeMap<String, Vec<Quantum>> = BTreeMap::new();
    let mut prop_doc: BTreeMap<String, String> = BTreeMap::new();
    for r in &reports {
        for p in &r.propositions {
            let q = Quantum::seal(
                &schema,
                &json!({"subject": p.subject, "num": p.num, "den": p.den, "value": p.witness}),
            )?;
            prop_doc
                .entry(q.cid.clone())
                .or_insert_with(|| r.doc_id.clone());
            by_subject.entry(p.subject.clone()).or_default().push(q);
        }
    }
    let mut conflicts_raw = Vec::new();
    for quanta in by_subject.values() {
        conflicts_raw.extend(cross_audit(&schema, quanta)?);
    }

    // Term audit: identity is the term name alone, so one term bound to two
    // homes is one identity CID with disagreeing witnesses — duplicate-
    // definition detection (`term_home_conflict`). Grouped per term for the
    // same reason propositions group per subject.
    let tschema = term_schema();
    let mut by_term: BTreeMap<String, Vec<Quantum>> = BTreeMap::new();
    for r in &reports {
        for t in &r.terms {
            let q = Quantum::seal(&tschema, &json!({"term": t.term, "home": t.home}))?;
            prop_doc
                .entry(q.cid.clone())
                .or_insert_with(|| r.doc_id.clone());
            by_term.entry(t.term.clone()).or_default().push(q);
        }
    }
    let mut term_conflicts_raw = Vec::new();
    for quanta in by_term.values() {
        term_conflicts_raw.extend(cross_audit(&tschema, quanta)?);
    }

    let mut cross_conflicts = Vec::new();
    for c in term_conflicts_raw {
        if let CrossAuditConflict::WitnessDisagreement { cid, witnesses } = &c {
            cross_conflicts.push(ReviewItem {
                kind: "term_home_conflict".into(),
                doc: prop_doc.get(cid).cloned().unwrap_or_default(),
                subject: cid.clone(),
                detail: format!(
                    "one term, {} homes: {}",
                    witnesses.len(),
                    witnesses.join(", ")
                ),
            });
        }
    }
    for c in conflicts_raw {
        let (kind, subject, detail) = match &c {
            CrossAuditConflict::UnderMerge { witness, cids } => (
                "cross_audit_conflict",
                witness.clone(),
                format!("one witness, {} forms: {}", cids.len(), cids.join(", ")),
            ),
            CrossAuditConflict::WitnessDisagreement { cid, witnesses } => (
                "cross_audit_conflict",
                cid.clone(),
                format!(
                    "one form, {} witnesses: {}",
                    witnesses.len(),
                    witnesses.join(", ")
                ),
            ),
        };
        let doc = match &c {
            CrossAuditConflict::UnderMerge { cids, .. } => cids
                .first()
                .and_then(|cid| prop_doc.get(cid))
                .cloned()
                .unwrap_or_default(),
            CrossAuditConflict::WitnessDisagreement { cid, .. } => {
                prop_doc.get(cid).cloned().unwrap_or_default()
            }
        };
        cross_conflicts.push(ReviewItem {
            kind: kind.into(),
            doc,
            subject,
            detail,
        });
    }
    cross_conflicts.sort();

    // Corpus-level promotion (Unit 2): aggregate every doc's sealed
    // propositions by CID. Corroboration = distinct documents asserting it;
    // routes = union across docs. Under the policy threshold → queued.
    struct PropAcc {
        subject: String,
        value: String,
        docs: BTreeSet<String>,
        routes: BTreeSet<String>,
    }
    let mut agg: BTreeMap<String, PropAcc> = BTreeMap::new();
    for r in &reports {
        for p in &r.propositions {
            let acc = agg.entry(p.proposition_cid.clone()).or_insert(PropAcc {
                subject: p.subject.clone(),
                value: p.witness.clone(),
                docs: BTreeSet::new(),
                routes: BTreeSet::new(),
            });
            acc.docs.insert(r.doc_id.clone());
            acc.routes.extend(p.routes.iter().cloned());
        }
    }
    let mut promotions = CrossAuditReport::default();
    for (cid, acc) in agg {
        let corroboration = acc.docs.len();
        if corroboration < cfg.policy.min_corroboration {
            promotions.queued.push(Queued {
                proposition: cid,
                subject: acc.subject,
                value: acc.value,
                corroboration,
                reason: format!(
                    "under-corroborated ({corroboration} < {})",
                    cfg.policy.min_corroboration
                ),
            });
        } else {
            promotions.promoted.push(Promoted {
                proposition: cid,
                subject: acc.subject,
                value: acc.value,
                corroboration,
                routes: acc.routes.into_iter().collect(),
            });
        }
    }
    promotions.promoted.sort_by(|a, b| {
        b.corroboration
            .cmp(&a.corroboration)
            .then(a.value.cmp(&b.value))
    });
    promotions.queued.sort_by(|a, b| {
        b.corroboration
            .cmp(&a.corroboration)
            .then(a.value.cmp(&b.value))
    });

    let telemetry = Telemetry {
        docs: reports.len(),
        utterances_sealed: reports.len(),
        unstructured: reports.iter().filter(|r| !r.structured).count(),
        edges_promoted: reports.iter().map(|r| r.edges.len()).sum(),
        references_untyped: reports.iter().map(|r| r.references.len()).sum(),
        propositions: reports.iter().map(|r| r.propositions.len()).sum(),
        terms_bound: reports.iter().map(|r| r.terms.len()).sum(),
        corroborated: promotions
            .promoted
            .iter()
            .filter(|p| p.corroboration > 1 || p.routes.len() > 1)
            .count(),
        unattributed_values: reports.iter().map(|r| r.unattributed_values).sum(),
        needs_review_depth: reports.iter().map(|r| r.needs_review.len()).sum::<usize>()
            + cross_conflicts.len()
            + promotions.queued.len(),
        reassertion_blocks: reports.iter().map(|r| r.blocked.len()).sum(),
    };

    Ok(CorpusReport {
        reports,
        cross_conflicts,
        promotions,
        telemetry,
    })
}

/// The compat graph projection (INTAKE.md integration step 1): the corpus as
/// a `derivation-graph.json`-shaped object, so harmonics' surfaces can render
/// intake output without noticing the engine swap. Every node carries the
/// utterance CID it was sealed at (`sealed: true` by construction — sealing is
/// the entry ticket). Deterministic: no timestamps; the version stamp is the
/// caller's (a git sha, a corpus pin), not a clock's.
pub fn corpus_graph(report: &CorpusReport, generated_by: &str) -> Value {
    let mut inverse: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for r in &report.reports {
        for e in &r.edges {
            inverse
                .entry(e.to.clone())
                .or_default()
                .insert(r.doc_id.clone());
        }
        for t in &r.references {
            inverse
                .entry(t.clone())
                .or_default()
                .insert(r.doc_id.clone());
        }
    }
    let nodes: Vec<Value> = report
        .reports
        .iter()
        .map(|r| {
            let mut depends: BTreeSet<String> = r.references.iter().cloned().collect();
            depends.extend(r.edges.iter().map(|e| e.to.clone()));
            let lineage: BTreeMap<&str, &str> = r
                .edges
                .iter()
                .map(|e| (e.to.as_str(), e.kind.as_str()))
                .collect();
            json!({
                "id": r.doc_id,
                "cid": r.utterance_cid,
                "sealed": true,
                "depends_on": depends,
                "depended_on_by": inverse.get(&r.doc_id).cloned().unwrap_or_default(),
                "lineage": lineage,
            })
        })
        .collect();
    json!({
        "generated": {
            "tool": "canon.d intake",
            "canonicalizer": report
                .reports
                .first()
                .map(|r| r.canonicalizer)
                .unwrap_or_else(|| Canonicalizer::Text.mode()),
            "by": generated_by,
        },
        "nodes": nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::ground_audit;
    use crate::bridge::seal_utterance;
    use crate::quantum::validate_edge_kind;

    #[test]
    fn out_of_domain_claims_queue_instead_of_promoting() {
        // The Unit 2 domain witness in the spine: Ω_Λ is a density fraction,
        // so a corpus fragment reading "omega_lambda = 12/1" queues with the
        // violated law named — never sealed as a promoted fact.
        let mut cfg = IntakeConfig::new("t");
        cfg.domains = SubjectDomains::harmonics();
        let r = intake("doc", "omega_lambda = 13/19\nomega_lambda = 12/1\n", &cfg).unwrap();
        assert_eq!(r.propositions.len(), 1, "only the in-domain claim seals");
        assert_eq!(r.propositions[0].witness, "13/19");
        let q = r
            .needs_review
            .iter()
            .find(|n| n.kind == "out_of_domain")
            .expect("the 12/1 reading must queue");
        assert!(
            q.detail.contains("omega_lambda ∈ (0/1, 1/1)"),
            "{}",
            q.detail
        );
        assert_eq!(r.exit_code(), 1);
    }

    #[test]
    fn same_claim_in_two_docs_corroborates_at_corpus_level() {
        let docs = vec![
            ("a".to_string(), "x = 1/3\n".to_string()),
            ("b".to_string(), "x = 2/6\n".to_string()), // reduces to the same witness
        ];
        let report = intake_corpus(&docs, &IntakeConfig::new("t")).unwrap();
        assert_eq!(report.promotions.promoted.len(), 1);
        let p = &report.promotions.promoted[0];
        assert_eq!(p.value, "1/3");
        assert_eq!(p.corroboration, 2, "two documents assert one proposition");
        assert_eq!(report.telemetry.corroborated, 1);
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn under_corroborated_claims_queue_under_a_stricter_policy() {
        let docs = vec![("a".to_string(), "x = 1/3\n".to_string())];
        let mut cfg = IntakeConfig::new("t");
        cfg.policy = PromotionPolicy {
            min_corroboration: 2,
        };
        let report = intake_corpus(&docs, &cfg).unwrap();
        assert!(report.promotions.promoted.is_empty());
        assert_eq!(report.promotions.queued.len(), 1);
        assert!(report.promotions.queued[0].reason.contains("1 < 2"));
        assert_eq!(report.exit_code(), 1, "queued promotions are findings");
    }

    #[test]
    fn vocab_route_binds_glossary_rows_only() {
        let glossary = "| **mediant** | Stern-Brocot mediant | op | `mediant_derivation.md` |\n\
                        | --- | --- |\n\
                        | **K_STAR** | operating coupling | val | `k_axis.md` |\n";
        let cfg = IntakeConfig::new("t");
        let r = intake("canonical_glossary", glossary, &cfg).unwrap();
        let bound: Vec<&str> = r.terms.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(bound, vec!["mediant", "K_STAR"]);
        assert!(r.structured, "term bindings are promoted structure");
        assert!(r.terms.iter().all(|t| t.home == "canonical_glossary"));

        // The same rows in a non-glossary doc bind nothing: status maps bold
        // terms they *use*; only the registry *binds*.
        let r2 = intake("framework_status", glossary, &cfg).unwrap();
        assert!(r2.terms.is_empty());
    }

    #[test]
    fn duplicate_term_homes_surface_as_term_home_conflict() {
        let docs = vec![
            (
                "old_glossary".to_string(),
                "| **mediant** | x | y | z |\n".to_string(),
            ),
            (
                "new_glossary".to_string(),
                "| **mediant** | x | y | z |\n".to_string(),
            ),
        ];
        let report = intake_corpus(&docs, &IntakeConfig::new("t")).unwrap();
        assert_eq!(report.exit_code(), 1);
        let conflict = report
            .cross_conflicts
            .iter()
            .find(|c| c.kind == "term_home_conflict")
            .expect("one term bound to two homes must surface");
        assert!(conflict.detail.contains("2 homes"));
    }

    #[test]
    fn route_findings_land_in_needs_review_with_doc() {
        struct Flagger;
        impl Structurer for Flagger {
            fn id(&self) -> &str {
                "test/flag/v1"
            }
            fn structure(&self, _d: &str, _t: &str, _c: &BTreeSet<String>) -> StructuringOutput {
                StructuringOutput {
                    findings: vec![RouteFinding {
                        kind: "deprecated_item".into(),
                        subject: "old_fn".into(),
                        detail: "#[deprecated]".into(),
                    }],
                    ..Default::default()
                }
            }
        }
        let cfg = IntakeConfig::new("t");
        let routes: [&dyn Structurer; 1] = [&Flagger];
        let r = intake_with_routes("some_mod", "text", &cfg, &routes).unwrap();
        assert_eq!(r.exit_code(), 1);
        assert_eq!(r.needs_review[0].kind, "deprecated_item");
        assert_eq!(r.needs_review[0].doc, "some_mod");
        assert_eq!(r.needs_review[0].subject, "old_fn");
    }

    fn corpus_cfg(ids: &[&str]) -> IntakeConfig {
        let mut cfg = IntakeConfig::new("claude");
        cfg.corpus_ids = ids.iter().map(|s| s.to_string()).collect();
        cfg
    }

    const DOC: &str = "\
# baryon_fraction

The cosmic partition gives omega_b = 1/19 with the remainder split
elsewhere; see `klein_bottle.md` for the topology.

## Lineage
- grounds: klein_bottle.md
- derives: numerology_inventory
";

    #[test]
    fn lineage_targets_strip_doc_suffix_and_seal() {
        let cfg = corpus_cfg(&["klein_bottle", "numerology_inventory"]);
        let r = intake("baryon_fraction", DOC, &cfg).unwrap();
        assert_eq!(r.edges.len(), 2);
        let g = r.edges.iter().find(|e| e.kind == "grounds").unwrap();
        assert_eq!(
            g.to, "klein_bottle",
            "`.md` suffix stripped before resolution"
        );
        for e in &r.edges {
            assert!(
                !e.annotation_cid.is_empty(),
                "typed edges are sealed annotations"
            );
            assert!(validate_edge_kind(&json!({"kind": e.kind})).is_ok());
        }
        assert_eq!(r.exit_code(), 0);
    }

    #[test]
    fn unresolvable_lineage_target_is_loud_not_dropped() {
        // W4: the typo'd target. The incumbent regex silently drops it; intake
        // refuses to promote it and queues it with the reason attached.
        let cfg = corpus_cfg(&["klein_bottle"]);
        let doc = "## Lineage\n- grounds: klein_bottel.md\n";
        let r = intake("typo_doc", doc, &cfg).unwrap();
        assert!(r.edges.is_empty());
        assert_eq!(r.needs_review.len(), 1);
        assert_eq!(r.needs_review[0].kind, "unresolved_lineage_target");
        assert_eq!(r.needs_review[0].subject, "klein_bottel");
        assert_eq!(r.exit_code(), 1, "a queued finding is exit 1, not silence");
    }

    #[test]
    fn references_found_by_each_pattern_form() {
        let cfg = corpus_cfg(&["alpha", "beta", "gamma", "delta", "unrelated"]);
        let doc = "\
see `alpha.md` and [the beta doc](beta.md); also (`gamma.py` internals)
and a bare mention of delta.md in prose. not_alpha.md is a different id.
";
        let r = intake("citer", doc, &cfg).unwrap();
        assert_eq!(r.references, vec!["alpha", "beta", "delta", "gamma"]);
        // boundary check: `not_alpha.md` must not have matched `alpha`
        assert!(!r.references.contains(&"unrelated".to_string()));
    }

    #[test]
    fn ratio_claim_seals_proposition_and_grounded_assertion() {
        let cfg = corpus_cfg(&["klein_bottle", "numerology_inventory"]);
        let r = intake("baryon_fraction", DOC, &cfg).unwrap();
        assert_eq!(r.propositions.len(), 1);
        let p = &r.propositions[0];
        assert_eq!((p.subject.as_str(), p.num, p.den), ("omega_b", 1, 19));
        assert_eq!(p.witness, "1/19");

        // The assertion's provenance resolves to the sealed utterance —
        // ground_audit clean against {utterance_cid}.
        let assertion = seal_assertion(
            &Quantum::seal(
                &proposition_schema(),
                &json!({"subject":"omega_b","num":1,"den":19,"value":"1/19"}),
            )
            .unwrap(),
            &[&seal_utterance(DOC, "en", "claude").unwrap()],
            "claude",
        )
        .unwrap();
        assert_eq!(
            assertion.cid, p.assertion_cid,
            "assertion grounds = the source utterance"
        );

        let known: BTreeSet<String> = [r.utterance_cid.clone()].into_iter().collect();
        assert!(ground_audit("grounds", &[assertion], &known).is_empty());
    }

    #[test]
    fn ratio_scanner_is_strict() {
        // == excluded, word boundaries both flanks, digits only.
        let claims = extract_ratio_claims(
            "a == 1/2 and x1 = 3/4 but 5 = 1/2 no; prefix_x1 = 3/4 ok; y = 7/8th no",
        );
        assert_eq!(
            claims,
            vec![
                RatioClaim {
                    subject: "x1".into(),
                    num: 3,
                    den: 4
                },
                RatioClaim {
                    subject: "prefix_x1".into(),
                    num: 3,
                    den: 4
                },
            ]
        );
    }

    #[test]
    fn invalid_ratio_is_review_not_panic() {
        let cfg = IntakeConfig::new("claude");
        let r = intake("div0", "bad = 1/0\n", &cfg).unwrap();
        assert!(r.propositions.is_empty());
        assert_eq!(r.needs_review.len(), 1);
        assert_eq!(r.needs_review[0].kind, "invalid_ratio");
    }

    #[test]
    fn falsified_proposition_is_blocked_at_intake() {
        // The negative-results ledger, enforced: seal what S_v = 16 would be,
        // put its CID in the falsified set, and watch re-assertion bounce.
        let falsified_prop = Quantum::seal(
            &proposition_schema(),
            &json!({"subject":"s_v","num":16,"den":1,"value":"16/1"}),
        )
        .unwrap();
        let mut cfg = IntakeConfig::new("claude");
        cfg.falsified.insert(falsified_prop.cid.clone());

        let r = intake("relitigator", "recall that s_v = 16/1 exactly\n", &cfg).unwrap();
        assert!(
            r.propositions.is_empty(),
            "no assertion sealed for a Falsified claim"
        );
        assert_eq!(r.blocked.len(), 1);
        assert_eq!(r.blocked[0].proposition_cid, falsified_prop.cid);
        assert_eq!(r.exit_code(), 1);
    }

    #[test]
    fn intake_is_idempotent_and_deterministic() {
        let cfg = corpus_cfg(&["klein_bottle", "numerology_inventory"]);
        let a = serde_json::to_string(&intake("baryon_fraction", DOC, &cfg).unwrap()).unwrap();
        let b = serde_json::to_string(&intake("baryon_fraction", DOC, &cfg).unwrap()).unwrap();
        assert_eq!(a, b, "same bytes in → the same report out, byte for byte");
    }

    #[test]
    fn corpus_cross_audit_catches_non_reducing_emitter() {
        // Doc A asserts r = 13/19; doc B asserts r = 26/38. The spine used to
        // seal each form's raw identity, so this surfaced as an UnderMerge —
        // the spine *was* the non-reducing emitter. It now reduces identity
        // at the claim-grouping step, so two spellings are one proposition
        // CID (corroboration 2), no conflict, exit 0. The corpus cross-audit
        // remains in place to catch any *other* emitter sealing unreduced
        // propositions into the corpus.
        let docs = vec![
            ("a".to_string(), "r = 13/19\n".to_string()),
            ("b".to_string(), "r = 26/38\n".to_string()),
        ];
        let report = intake_corpus(&docs, &IntakeConfig::new("claude")).unwrap();
        assert!(report.cross_conflicts.is_empty());
        assert_eq!(report.promotions.promoted.len(), 1);
        assert_eq!(report.promotions.promoted[0].value, "13/19");
        assert_eq!(report.promotions.promoted[0].corroboration, 2);
        assert_eq!(
            report.reports[0].propositions[0].proposition_cid,
            report.reports[1].propositions[0].proposition_cid,
            "one proposition CID across both spellings"
        );
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn shared_value_across_subjects_is_not_a_conflict() {
        // Found by the first run over the real harmonics corpus: many subjects
        // legitimately share a value (several docs each define some ratio as
        // 1/3). The witness is the value alone, so a whole-bag audit calls
        // that an UnderMerge; the per-subject grouping must not.
        let docs = vec![
            ("a".to_string(), "x = 1/3\n".to_string()),
            ("b".to_string(), "y = 1/3\n".to_string()),
        ];
        let report = intake_corpus(&docs, &IntakeConfig::new("claude")).unwrap();
        assert!(
            report.cross_conflicts.is_empty(),
            "two subjects, one value — two claims, not a canonicalizer bug"
        );
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn corpus_intake_is_order_independent() {
        let fwd = vec![
            ("a".to_string(), "x = 1/2\nsee `b.md`\n".to_string()),
            ("b".to_string(), "## Lineage\n- grounds: a\n".to_string()),
        ];
        let rev: Vec<_> = fwd.iter().rev().cloned().collect();
        let cfg = IntakeConfig::new("claude");
        let fa = serde_json::to_string(&intake_corpus(&fwd, &cfg).unwrap()).unwrap();
        let fb = serde_json::to_string(&intake_corpus(&rev, &cfg).unwrap()).unwrap();
        assert_eq!(
            fa, fb,
            "corpus intake is a commutative fold, like the CAS it feeds"
        );
    }

    #[test]
    fn corpus_graph_shape_and_inverse_edges() {
        let docs = vec![
            ("a".to_string(), "x = 1/2\n".to_string()),
            ("b".to_string(), "## Lineage\n- grounds: a\n".to_string()),
        ];
        let report = intake_corpus(&docs, &IntakeConfig::new("claude")).unwrap();
        let g = corpus_graph(&report, "test-pin");
        let nodes = g["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
        let a = nodes.iter().find(|n| n["id"] == "a").unwrap();
        let b = nodes.iter().find(|n| n["id"] == "b").unwrap();
        assert_eq!(a["sealed"], true);
        assert!(
            a["cid"].as_str().unwrap().len() == 64,
            "nodes carry the utterance CID"
        );
        assert_eq!(b["depends_on"][0], "a");
        assert_eq!(a["depended_on_by"][0], "b");
        assert_eq!(b["lineage"]["a"], "grounds");
    }

    #[test]
    fn unstructured_docs_are_backlog_telemetry_not_errors() {
        let docs = vec![("plain".to_string(), "just prose, no claims\n".to_string())];
        let report = intake_corpus(&docs, &IntakeConfig::new("claude")).unwrap();
        assert_eq!(report.telemetry.unstructured, 1);
        assert_eq!(
            report.exit_code(),
            0,
            "an unstructured doc is a coverage gap, not a failure"
        );
    }

    /// With urtext wired in, a reformatting of the corpus produces the SAME
    /// report — the intake quotient inherits `normalize`'s idempotence.
    #[cfg(feature = "prose")]
    #[test]
    fn reformatting_yields_the_identical_report() {
        let cfg = corpus_cfg(&["klein_bottle"]);
        let a = intake("d", "x = 1/2 per\r\n`klein_bottle.md`\n", &cfg).unwrap();
        let b = intake("d", "x = 1/2 per `klein_bottle.md`\n", &cfg).unwrap();
        assert_eq!(a.utterance_cid, b.utterance_cid, "N2/N5 collapse at intake");
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap(),
            "the whole report is invariant under formatting noise"
        );
        assert_eq!(a.canonicalizer, "urtext");
    }

    #[cfg(not(feature = "prose"))]
    #[test]
    fn degraded_mode_is_pinned_loudly() {
        let r = intake("d", "x = 1/2\n", &IntakeConfig::new("claude")).unwrap();
        assert_eq!(
            r.canonicalizer, "identity",
            "the mode is in the report, not ambient"
        );
    }
}
