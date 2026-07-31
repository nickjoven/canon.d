//! Golden claim-set snapshot over a fixture corpus — the pre-merge oracle
//! for route behavior (harmonics#328 follow-through).
//!
//! The junk classes the live harmonics corpus taught us arrived in strata:
//! each extension of the routes' reach exposed a new one, and each was
//! discovered *after* a re-ingest, by inspecting the claim delta by hand.
//! This test moves that discovery to the PR diff: the fixture corpus below
//! carries one exemplar of every known junk class next to the legitimate
//! form it imitates, and the EXPECTED table is the exact claim set the
//! production configuration must extract. Any route change that shifts
//! behavior — new bind, lost bind, moved subject — diffs this table, in
//! review, before any corpus meets it.
//!
//! When a change legitimately moves the table (a new rule, a new lexicon
//! subject), updating EXPECTED is part of the change, and the diff IS the
//! behavioral review. When it moves the table unexpectedly, that is the
//! catch working.
//!
//! Runs in ci.yml's `test-prose` job (gated on the URTEXT_READ_TOKEN
//! secret; skips VISIBLY when absent) and in any local
//! `cargo test --features prose`; compiles to nothing without the
//! feature.
#![cfg(feature = "prose")]

use canon_d::intake::{intake_corpus_with_routes, prose_routes, Structurer};
use canon_d::structurer::{LexiconRatioRoute, Route, SubjectLexicon};
use canon_d::{IntakeConfig, SubjectDomains};

/// One doc per family. Each line is either a legitimate form (expected to
/// extract) or a named junk exemplar (expected NOT to, or to queue).
const FIXTURE: &[(&str, &str)] = &[
    (
        "legit_forms",
        "Ω_Λ = 13/19 is the anchor.\n\
         Omega_Lambda equals 26/38 in the reduced check.\n\
         and 13/19 = Ω_Λ closes the mirror form.\n\
         13/19 (dark energy) as an appositive.\n\
         unique rational solution **w_+ = 12/13** in bold.\n\
         Predictions at w_+ = 0.9298: rows follow.\n\
         Ω_b = 13/264 and Omega_b = 1/19 case-fold together.\n\
         w_- = 1 structurally (den-1 through an explicit `=`).\n",
    ),
    (
        "junk_role",
        "2018: Ω_b = 13/264 held (the year must not bind).\n\
         Ω_b = 6.7% residual (a percentage rendering).\n\
         Ω_Λ ∈ 13 (a weak relation cannot license an integer).\n\
         13 (dark energy) — nor can an appositive.\n",
    ),
    (
        "junk_scope",
        "Ω_b = 35/132, Ω_Λ = 13/19 (comma must not donate).\n\
         w_+ = 0.04930 / (1 - 0.0986) ≈ 0.9298 (formula head).\n\
         1 − w_+ = 1/14 (the complement, not the claim).\n\
         Ω_Λ : Ω_DM : Ω_b = 13 : 5 : 1 / 19 (partition shares).\n\
         (Ω_Λ, Ω_DM, Ω_b) = (13/19, 5/19, 1/19) (tuple form).\n",
    ),
    (
        "junk_domain",
        "Ω_DM = 5 passes syntax; the value law queues it.\n\
         w_+ = 14/13 likewise exceeds its closed interval.\n",
    ),
];

/// The exact claim set the production configuration extracts from FIXTURE:
/// `(doc, subject, witness)`, sorted. THIS TABLE IS THE GOLDEN — an entry
/// appearing, vanishing, or moving subject is a behavioral change that must
/// be explained in the PR that causes it.
const EXPECTED: &[(&str, &str, &str)] = &[
    ("junk_role", "omega_b", "13/264"),
    ("junk_scope", "omega_b", "35/132"),
    ("junk_scope", "omega_lambda", "13/19"),
    ("legit_forms", "omega_b", "1/19"),
    ("legit_forms", "omega_b", "13/264"),
    ("legit_forms", "omega_lambda", "13/19"),
    ("legit_forms", "w_minus", "1/1"),
    ("legit_forms", "w_plus", "12/13"),
    ("legit_forms", "w_plus", "4649/5000"),
];

/// Junk that clears syntax but violates a declared value law: it must land
/// in the out-of-domain queue, visibly — neither promoted nor dropped.
const EXPECTED_OUT_OF_DOMAIN: &[(&str, &str)] =
    &[("junk_domain", "omega_dm"), ("junk_domain", "w_plus")];

#[test]
fn golden_claim_set_over_the_fixture_corpus() {
    // Mirror canon-demo's prose-pack configuration exactly: the same routes,
    // lexicon, aliases, and domains the real ingest runs under.
    let docs: Vec<(String, String)> = FIXTURE
        .iter()
        .map(|(id, text)| (id.to_string(), text.to_string()))
        .collect();
    let prefix = LexiconRatioRoute::harmonics(Route::Prefix);
    let postfix = LexiconRatioRoute::harmonics(Route::Postfix);
    let mut routes: Vec<&dyn Structurer> = prose_routes().to_vec();
    routes.push(&prefix);
    routes.push(&postfix);
    let mut cfg = IntakeConfig::new("golden-fixture");
    cfg.domains = SubjectDomains::harmonics();
    cfg.subject_aliases = SubjectLexicon::harmonics().alias_map();

    let report = intake_corpus_with_routes(&docs, &cfg, &routes).expect("intake");

    let mut claims: Vec<(String, String, String)> = report
        .reports
        .iter()
        .flat_map(|r| {
            r.propositions
                .iter()
                .map(|p| (r.doc_id.clone(), p.subject.clone(), p.witness.clone()))
        })
        .collect();
    claims.sort();
    let expected: Vec<(String, String, String)> = EXPECTED
        .iter()
        .map(|(d, s, w)| (d.to_string(), s.to_string(), w.to_string()))
        .collect();
    assert_eq!(
        claims, expected,
        "\nthe extracted claim set moved — if the change is intended, \
         updating EXPECTED is part of it; if not, a junk class just \
         (re)appeared\n"
    );

    let mut queued: Vec<(String, String)> = report
        .reports
        .iter()
        .flat_map(|r| {
            r.needs_review
                .iter()
                .filter(|i| i.kind == "out_of_domain")
                .map(|i| (i.doc.clone(), i.subject.clone()))
        })
        .collect();
    queued.sort();
    queued.dedup();
    let expected_queued: Vec<(String, String)> = EXPECTED_OUT_OF_DOMAIN
        .iter()
        .map(|(d, s)| (d.to_string(), s.to_string()))
        .collect();
    assert_eq!(
        queued, expected_queued,
        "\nout-of-domain queue moved — a value law changed, or junk that \
         the domain witness should quarantine is being promoted\n"
    );
}
