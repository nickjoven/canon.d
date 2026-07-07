//! The code domain pack, v1 — Rust source as an intake corpus.
//!
//! One route: [`ImportsRoute`] lifts `use crate::…` statements and `mod …;`
//! declarations into dependency citations against the corpus id set (module
//! stems). Imports land as **untyped references** deliberately — the same
//! stance the prose pack takes on citations: an import is a dependency
//! *citation*; typing it (grounds? derives?) would be an epistemic claim the
//! syntax doesn't make.
//!
//! What v1 knowingly does not do (DOMAINS.md findings ledger):
//! - no `#[deprecated]` tombstone route — `StructuringOutput` has no
//!   findings/tombstone channel yet (F3);
//! - no public-API term census — no terms channel yet (F3);
//! - `use super::…` / relative paths are skipped: resolving them needs the
//!   module tree, not a line scanner.
//!
//! Parsing is a line scanner, not a Rust parser, on purpose: the same
//! hand-rolled, regex-free, deterministic style as the prose routes. It reads
//! `use`/`mod` items at any brace depth (tests included) and understands
//! one level of `use crate::{a, b::c}` grouping, which covers this repo; a
//! real code pack would sit on syn or rust-analyzer, behind the same trait.

use std::collections::BTreeSet;

use crate::intake::{Structurer, StructuringOutput};

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
        let routes: [&dyn Structurer; 1] = [&ImportsRoute];
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

        // Same telemetry contract as the prose corpus; imports are citations,
        // so nothing is promoted, nothing needs review, and the code corpus
        // is clean at exit 0.
        assert_eq!(report.telemetry.docs, docs.len());
        assert_eq!(report.telemetry.propositions, 0);
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
