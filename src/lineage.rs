//! Edge typing — import the legacy `## Lineage` prose convention into the
//! substrate's typed-edge model, then feed the grounding edges into the closure.
//!
//! The topology of the harmonics corpus is *undertyped*: every edge is a flat
//! `references`, so the closure can only read **citation shape**, not **certainty
//! shape**. This module types the edges — but as an **importer**, not a source of
//! truth. Regex-over-prose is precisely what the substrate moves away from
//! (`ket/DESIGN.md`: "a graph derived from the tree heals to whatever the tree
//! says"). So the prose `## Lineage` block is the *import boundary*: parsed once,
//! emitted as typed [`edge_annotation`](crate::edge_annotation_schema) quanta
//! (content-addressed, supersedable), and from then on the typed edges live as
//! sealed annotations — never re-parsed. The grounding edges (`grounds`/`derives`)
//! then feed [`Closure`], turning the citation graph into a certainty graph.
//!
//! Convention — under a `## Lineage` heading, one kind per line:
//! ```text
//! ## Lineage
//! - grounds: klein_bottle, born_rule
//! - derives: baryon_fraction
//! ```
//! Untyped citations elsewhere remain `references`; typing *replaces* a flat
//! reference with an epistemic role.

use serde_json::json;

use crate::closure::Closure;
use crate::quantum::{edge_annotation_schema, Quantum, EDGE_KINDS};

/// Grounding-bearing edge kinds: a claim *depends on* its grounds and derivation
/// premises (both feed the closure). `proposes`/`supersedes`/`contradicts` are
/// other channels and do not ground.
pub const GROUNDING_KINDS: [&str; 2] = ["grounds", "derives"];

/// A typed epistemic edge parsed from a Lineage block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
}

/// Parse a `## Lineage` block out of a markdown doc. Returns the explicit typed
/// edges; a doc with no Lineage block yields none (its citations stay untyped).
/// Robust and line-based (not a full markdown parser): the block runs from a
/// `## Lineage` heading to the next heading or EOF; unknown kinds are skipped.
pub fn parse_lineage(doc_id: &str, markdown: &str) -> Vec<TypedEdge> {
    let mut out = Vec::new();
    let mut in_block = false;
    for line in markdown.lines() {
        let t = line.trim();
        if !in_block {
            if t.eq_ignore_ascii_case("## lineage") {
                in_block = true;
            }
            continue;
        }
        if t.starts_with("# ") || t.starts_with("## ") {
            break; // next heading ends the block
        }
        let body = t.trim_start_matches(['-', '*', ' ']);
        if let Some((kind, targets)) = body.split_once(':') {
            let kind = kind.trim();
            if EDGE_KINDS.contains(&kind) {
                for target in targets.split(',') {
                    let target = target.trim();
                    if !target.is_empty() {
                        out.push(TypedEdge {
                            from: doc_id.to_string(),
                            to: target.to_string(),
                            kind: kind.to_string(),
                        });
                    }
                }
            }
        }
    }
    out
}

/// Seal the typed edges as `edge_annotation` quanta (the principled, supersedable
/// form). After this, the typed edges are content-addressed substrate objects;
/// the prose is no longer consulted. `annotator` records who imported them, so a
/// later re-typing supersedes (same `(from,to,annotator)`).
pub fn lineage_to_annotations(edges: &[TypedEdge], annotator: &str) -> Vec<Quantum> {
    edges
        .iter()
        .filter_map(|e| {
            Quantum::seal(
                &edge_annotation_schema(),
                &json!({ "from": e.from, "to": e.to, "annotator": annotator, "kind": e.kind }),
            )
            .ok()
        })
        .collect()
}

/// Build a [`Closure`] over the grounding edges: each doc is grounded in the
/// union of its `grounds`/`derives` targets; `anchors` are the vouched leaves.
/// This is the move that lets the closure read the graph as **certainty** (what is
/// grounded back to an anchor) rather than **citation** (what cites what).
pub fn lineage_closure(anchors: &[&str], edges: &[TypedEdge]) -> Closure {
    use std::collections::{BTreeMap, BTreeSet};
    let mut bodies: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for e in edges {
        if GROUNDING_KINDS.contains(&e.kind.as_str()) {
            bodies.entry(e.from.clone()).or_default().insert(e.to.clone());
        }
    }
    let mut c = Closure::new();
    for a in anchors {
        c.add_anchor(a);
    }
    for (head, body) in bodies {
        c.add_rule(head, body);
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantum::validate_edge_kind;

    const DOC: &str = "\
# baryon_fraction

Some derivation prose about the cosmic partition.

## Lineage
- grounds: klein_bottle, born_rule
- derives: numerology_inventory
- proposes: some_conjecture

## References

unrelated trailing section
";

    #[test]
    fn parses_typed_edges_from_lineage() {
        let edges = parse_lineage("baryon_fraction", DOC);
        assert_eq!(edges.len(), 4, "two grounds + one derives + one proposes");
        assert!(edges.iter().any(|e| e.to == "klein_bottle" && e.kind == "grounds"));
        assert!(edges.iter().any(|e| e.to == "born_rule" && e.kind == "grounds"));
        assert!(edges.iter().any(|e| e.to == "numerology_inventory" && e.kind == "derives"));
        assert!(edges.iter().any(|e| e.kind == "proposes"));
        // every edge carries a known kind
        for e in &edges {
            assert!(EDGE_KINDS.contains(&e.kind.as_str()));
        }
    }

    #[test]
    fn no_lineage_block_yields_no_typed_edges() {
        let edges = parse_lineage("plain", "# plain\n\njust prose, cites klein_bottle inline\n");
        assert!(edges.is_empty(), "untyped docs stay untyped (references), not invented grounds");
    }

    #[test]
    fn unknown_kinds_are_skipped() {
        let edges = parse_lineage("d", "## Lineage\n- causes: x\n- grounds: y\n");
        assert_eq!(edges.len(), 1, "`causes` is not an edge kind; only `grounds: y` is taken");
        assert_eq!(edges[0].to, "y");
    }

    #[test]
    fn edges_seal_as_valid_edge_annotations() {
        let edges = parse_lineage("baryon_fraction", DOC);
        let anns = lineage_to_annotations(&edges, "importer");
        assert_eq!(anns.len(), 4);
        for a in &anns {
            assert!(validate_edge_kind(&a.body).is_ok(), "each sealed annotation has a valid kind");
            assert_eq!(a.field("annotator").and_then(|v| v.as_str()), Some("importer"));
        }
    }

    #[test]
    fn typed_grounds_feed_the_closure_as_certainty() {
        // baryon_fraction grounds-on klein_bottle; omega derives-from baryon_fraction.
        let mut edges = parse_lineage("baryon_fraction", "## Lineage\n- grounds: klein_bottle\n");
        edges.extend(parse_lineage("omega_lambda", "## Lineage\n- derives: baryon_fraction\n"));

        // klein_bottle is the vouched keystone (an anchor). Then certainty flows.
        let c = lineage_closure(&["klein_bottle"], &edges);
        assert!(c.is_certain("klein_bottle"));
        assert!(c.is_certain("baryon_fraction"), "grounded back to the anchor");
        assert!(c.is_certain("omega_lambda"), "derived from a certain fact");

        // WITHOUT anchoring the keystone, nothing is certain — typing alone is not
        // certainty; certainty is borrowed from a grounded anchor (the boundary).
        let c2 = lineage_closure(&[], &edges);
        assert!(c2.certain().is_empty(), "no anchor → no certainty, however well-typed");

        // And flat `references` (no grounding edges) would give NO closure rules at
        // all — typing is exactly what turns citation into certainty.
        let refs = vec![TypedEdge { from: "a".into(), to: "b".into(), kind: "references".into() }];
        assert!(lineage_closure(&["b"], &refs).is_certain("b"));
        assert!(!lineage_closure(&["b"], &refs).is_certain("a"), "a `references` edge grounds nothing");
    }
}
