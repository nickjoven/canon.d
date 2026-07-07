//! The code domain pack — Rust source as an intake corpus.
//!
//! Three routes:
//! - [`ImportsRoute`] lifts `use crate::…` statements and `mod …;`
//!   declarations into dependency citations against the corpus id set
//!   (module stems). Imports land as **untyped references** deliberately —
//!   the same stance the prose pack takes on citations: an import is a
//!   dependency *citation*; typing it (grounds? derives?) would be an
//!   epistemic claim the syntax doesn't make.
//! - [`ApiRoute`] binds top-level public items as qualified term quanta
//!   (the public-API census; F3's terms channel).
//! - [`DeprecatedRoute`] surfaces `#[deprecated]` definitions as review
//!   findings (the tombstone census; F3's findings channel).
//!
//! Known limits: `use super::…` / relative paths are skipped — resolving
//! them needs the module tree, not a line scanner.
//!
//! Parsing is a line scanner, not a Rust parser, on purpose: the same
//! hand-rolled, regex-free, deterministic style as the prose routes. It reads
//! `use`/`mod` items at any brace depth (tests included) and understands
//! one level of `use crate::{a, b::c}` grouping, which covers this repo; a
//! real code pack would sit on syn or rust-analyzer, behind the same trait.

use std::collections::BTreeSet;

use crate::intake::{RouteFinding, Structurer, StructuringOutput, TermBinding};

/// `use crate::…` and `mod …;` → dependency citations, filtered to the
/// corpus id set. Corpus-driven like the prose reference route: a target
/// outside `corpus_ids` proposes nothing.
pub struct ImportsRoute;

impl Structurer for ImportsRoute {
    fn id(&self) -> &str {
        "code/imports/v1"
    }
    fn structure(&self, doc_id: &str, text: &str, corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        for target in import_targets(text) {
            if target != doc_id && corpus.contains(&target) && !out.references.contains(&target) {
                out.references.push(target);
            }
        }
        out
    }
}

/// Top-level `pub fn|struct|enum|trait NAME` → term bindings (the public-API
/// census). Terms are **qualified** (`quantum::Quantum`) on purpose: bare
/// code names legitimately collide across modules (`new` lives everywhere),
/// so the one-canonical-home audit that bare terms buy for prose vocabulary
/// would be all noise here. `pub(crate)`/`pub(super)` items are not API and
/// don't bind; indented items are impl/trait members, likewise skipped.
pub struct ApiRoute;

impl Structurer for ApiRoute {
    fn id(&self) -> &str {
        "code/api/v1"
    }
    fn structure(&self, doc_id: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        for line in text.lines() {
            // Column 0 only: an indented `pub fn` is an impl member.
            let Some(rest) = line.strip_prefix("pub ") else {
                continue;
            };
            let Some(name) = item_name(rest) else {
                continue;
            };
            let binding = TermBinding {
                term: format!("{doc_id}::{name}"),
                home: doc_id.to_string(),
            };
            if !out.terms.contains(&binding) {
                out.terms.push(binding);
            }
        }
        out
    }
}

/// `#[deprecated]` definitions → route findings (the tombstone census).
/// A deprecated item is exactly the negative-space entry similarity search
/// can't serve (DOMAINS.md): surfacing it at intake is what makes the
/// tombstone visible to review instead of discoverable-and-ignored.
pub struct DeprecatedRoute;

impl Structurer for DeprecatedRoute {
    fn id(&self) -> &str {
        "code/deprecated/v1"
    }
    fn structure(&self, _doc: &str, text: &str, _corpus: &BTreeSet<String>) -> StructuringOutput {
        let mut out = StructuringOutput::default();
        let lines: Vec<&str> = text.lines().collect();
        for (i, raw) in lines.iter().enumerate() {
            let line = raw.trim_start();
            if !line.starts_with("#[deprecated") {
                continue;
            }
            // The deprecated item is the first following line that is an
            // item, skipping further attributes and doc comments.
            let name = lines[i + 1..]
                .iter()
                .map(|l| l.trim_start())
                .find(|l| !l.starts_with("#[") && !l.starts_with("//"))
                .and_then(|l| item_name(strip_visibility(l)))
                .unwrap_or_else(|| "?".to_string());
            out.findings.push(RouteFinding {
                kind: "deprecated_item".into(),
                subject: name,
                detail: line.to_string(),
            });
        }
        out
    }
}

/// `fn|struct|enum|trait NAME…` → NAME.
fn item_name(s: &str) -> Option<String> {
    for kw in ["fn ", "struct ", "enum ", "trait "] {
        if let Some(rest) = s.strip_prefix(kw) {
            let name: String = rest.chars().take_while(|c| is_word(*c)).collect();
            if !name.is_empty() {
                return Some(name);
            }
        }
    }
    None
}

/// Extract candidate module targets from Rust source: the first path segment
/// after `use crate::` (including one level of `{…}` grouping) and the name
/// in `mod name;` declarations. Order of first occurrence, duplicates kept
/// (the spine dedups references).
pub fn import_targets(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = strip_visibility(raw.trim_start());
        if let Some(rest) = line.strip_prefix("use crate::") {
            collect_use_targets(rest, &mut out);
        } else if let Some(rest) = line.strip_prefix("mod ") {
            // `mod name;` declares a child module: a structural dependency.
            // `mod name {` opens an inline module: not a dependency on a
            // sibling document, so only the `;` form counts.
            let name: String = rest.chars().take_while(|c| is_word(*c)).collect();
            if !name.is_empty() && rest[name.len()..].trim_start().starts_with(';') {
                out.push(name);
            }
        }
    }
    out
}

/// `pub use` / `pub(crate) use` / `pub(in …) use` → `use`; same for `mod`.
fn strip_visibility(line: &str) -> &str {
    let Some(rest) = line.strip_prefix("pub") else {
        return line;
    };
    let rest = rest.trim_start();
    if let Some(after) = rest.strip_prefix('(') {
        match after.find(')') {
            Some(close) => after[close + 1..].trim_start(),
            None => line,
        }
    } else {
        rest
    }
}

/// `rest` is what follows `use crate::`. Either a plain path (`quantum::X;`)
/// or a group (`{quantum, strata::Y}` — one level, depth-aware split).
fn collect_use_targets(rest: &str, out: &mut Vec<String>) {
    let rest = rest.trim_start();
    if let Some(group) = rest.strip_prefix('{') {
        let inner = match close_of_group(group) {
            Some(end) => &group[..end],
            None => group,
        };
        for entry in split_top_level(inner) {
            push_leading_segment(entry.trim_start(), out);
        }
    } else {
        push_leading_segment(rest, out);
    }
}

fn push_leading_segment(s: &str, out: &mut Vec<String>) {
    let seg: String = s.chars().take_while(|c| is_word(*c)).collect();
    if !seg.is_empty() && seg != "self" && seg != "super" && seg != "crate" {
        out.push(seg);
    }
}

fn close_of_group(s: &str) -> Option<usize> {
    let mut depth = 1usize;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intake::{intake_corpus_with_routes, IntakeConfig};

    #[test]
    fn plain_grouped_and_mod_forms_parse() {
        let src = "\
use std::collections::BTreeMap;
use crate::quantum::{cross_audit, Quantum};
pub use crate::{bridge, strata::seal_assertion};
pub(crate) use crate::generator::Rat;
use super::helpers;
mod lineage;
pub mod schema;
mod tests { }
";
        assert_eq!(
            import_targets(src),
            vec![
                "quantum",
                "bridge",
                "strata",
                "generator",
                "lineage",
                "schema"
            ]
        );
    }

    #[test]
    fn api_route_binds_qualified_top_level_items_only() {
        let src = "\
pub struct Quantum;
pub fn cross_audit() {}
pub(crate) fn internal() {}
    pub fn method_in_impl() {}
pub trait Structurer {}
pub use crate::other::Thing;
";
        let out = ApiRoute.structure("quantum", src, &BTreeSet::new());
        let terms: Vec<&str> = out.terms.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(
            terms,
            vec![
                "quantum::Quantum",
                "quantum::cross_audit",
                "quantum::Structurer"
            ]
        );
        assert!(out.terms.iter().all(|t| t.home == "quantum"));
    }

    #[test]
    fn deprecated_route_names_the_tombstoned_item() {
        let src = "\
#[deprecated(note = \"use seal_utterance_with\")]
#[allow(unused)]
pub fn old_seal() {}
";
        let out = DeprecatedRoute.structure("bridge", src, &BTreeSet::new());
        assert_eq!(out.findings.len(), 1);
        assert_eq!(out.findings[0].kind, "deprecated_item");
        assert_eq!(out.findings[0].subject, "old_seal");
    }

    #[test]
    fn route_filters_to_corpus_and_self() {
        let corpus: BTreeSet<String> = ["quantum", "intake"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let out = ImportsRoute.structure(
            "intake",
            "use crate::quantum::Quantum;\nuse crate::intake::intake;\nuse crate::nope::X;\n",
            &corpus,
        );
        assert_eq!(out.references, vec!["quantum"]);
        assert!(out.typed_edges.is_empty() && out.ratios.is_empty());
    }

    /// The two-domain test, live: intake canon.d's own `src/*.rs` through the
    /// code pack. The spine is untouched — this test compiling and passing
    /// against `intake_corpus_with_routes` IS the flexibility contract of
    /// DOMAINS.md holding for pack #2.
    #[test]
    fn self_host_canon_d_source() {
        let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut docs: Vec<(String, String)> = Vec::new();
        for entry in std::fs::read_dir(&src_dir).expect("read src/") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let id = path.file_stem().unwrap().to_string_lossy().to_string();
                let text = std::fs::read_to_string(&path).expect("read source file");
                docs.push((id, text));
            }
        }
        assert!(docs.len() > 10, "expected the canon.d module tree");

        let mut cfg = IntakeConfig::new("self-host-test");
        // The code pack's noise quotient is byte-identity: prose normalization
        // over Rust source destroys `use` lines (DOMAINS.md F2, caught by this
        // very test under --features prose).
        cfg.canonicalizer = crate::bridge::Canonicalizer::Identity;
        let routes: [&dyn Structurer; 3] = [&ImportsRoute, &ApiRoute, &DeprecatedRoute];
        let report = intake_corpus_with_routes(&docs, &cfg, &routes).expect("intake");
        for r in &report.reports {
            assert_eq!(r.canonicalizer, "identity", "pack quotient must pin");
        }

        // Ground truth we can read off intake.rs's own use-block: it imports
        // bridge, generator, lineage, quantum, and strata.
        let intake_refs = &report
            .reports
            .iter()
            .find(|r| r.doc_id == "intake")
            .expect("intake.rs in corpus")
            .references;
        for dep in ["bridge", "generator", "lineage", "quantum", "strata"] {
            assert!(
                intake_refs.contains(&dep.to_string()),
                "intake.rs should cite {dep}, got {intake_refs:?}"
            );
        }

        // lib.rs declares the module tree, so it must be the widest citer.
        let lib_refs = &report
            .reports
            .iter()
            .find(|r| r.doc_id == "lib")
            .expect("lib.rs in corpus")
            .references;
        assert!(lib_refs.contains(&"intake".to_string()));
        assert!(lib_refs.len() >= 15, "lib.rs cites the whole tree");

        // The API census binds qualified terms (F3's terms channel): spot-
        // check a known public item, and qualified names mean zero
        // term_home_conflicts by construction.
        let quantum_terms = &report
            .reports
            .iter()
            .find(|r| r.doc_id == "quantum")
            .expect("quantum.rs in corpus")
            .terms;
        assert!(
            quantum_terms.iter().any(|t| t.term == "quantum::Quantum"),
            "public-API census should bind quantum::Quantum"
        );

        // Same telemetry contract as the prose corpus; imports are citations,
        // so nothing needs review (canon.d has no deprecated items), and the
        // code corpus is clean at exit 0.
        assert_eq!(report.telemetry.docs, docs.len());
        assert_eq!(report.telemetry.propositions, 0);
        assert!(report.telemetry.terms_bound > 100, "the API census is real");
        assert_eq!(report.exit_code(), 0);

        // Determinism: the projection is byte-identical across runs.
        let g1 = crate::intake::corpus_graph(&report, "self-host");
        let report2 = intake_corpus_with_routes(&docs, &cfg, &routes).expect("intake");
        let g2 = crate::intake::corpus_graph(&report2, "self-host");
        assert_eq!(
            serde_json::to_string(&g1).unwrap(),
            serde_json::to_string(&g2).unwrap()
        );
    }
}
