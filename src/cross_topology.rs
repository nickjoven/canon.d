//! Cross-domain topology analysis.
//!
//! This module reads — it never writes. It indexes mappings and domains
//! to answer cross-domain questions: which domains are bridged, where
//! agents disagree, where they converge, and what transitive paths exist
//! through the mapping graph.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::domain::Domain;
use crate::mapping::{Direction, Mapping};

/// Summary of how two domains are connected through mappings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainBridge {
    pub source_domain: String,
    pub target_domain: String,
    pub mapping_count: usize,
    /// Unique source fields covered by mappings in this bridge.
    pub source_coverage: usize,
    /// Unique target fields covered by mappings in this bridge.
    pub target_coverage: usize,
    pub avg_confidence: f64,
}

/// A set of competing mappings for the same field pair from multiple agents.
#[derive(Debug, Clone)]
pub struct Disagreement {
    pub source_schema: String,
    pub source_field: String,
    pub target_schema: String,
    pub target_field: String,
    pub competing: Vec<Mapping>,
    pub agent_count: usize,
}

/// A multi-hop path through the mapping graph.
#[derive(Debug, Clone)]
pub struct TransitivePath {
    /// (schema_cid, field_name) at each hop, including origin.
    pub steps: Vec<(String, String)>,
    /// Product of step confidences.
    pub combined_confidence: f64,
}

/// Read-only cross-domain topology built from domains and mappings.
pub struct CrossTopologyView {
    /// Mappings indexed by identity key (source_schema, source_field, target_schema, target_field).
    mappings: HashMap<(String, String, String, String), Vec<Mapping>>,
    /// Schema CID -> domain name lookup.
    schema_to_domain: HashMap<String, String>,
}

impl CrossTopologyView {
    /// Build a cross-topology view from the provided domains and mappings.
    ///
    /// Indexes schema-to-domain membership and groups mappings by identity key.
    pub fn new(domains: &[Domain], mappings: &[Mapping]) -> Self {
        let mut schema_to_domain = HashMap::new();
        for domain in domains {
            for cid in &domain.schema_cids {
                schema_to_domain.insert(cid.clone(), domain.name.clone());
            }
        }

        let mut grouped: HashMap<(String, String, String, String), Vec<Mapping>> = HashMap::new();
        for m in mappings {
            grouped.entry(m.identity_key()).or_default().push(m.clone());
        }

        CrossTopologyView {
            mappings: grouped,
            schema_to_domain,
        }
    }

    /// Compute bridges between domain pairs, aggregating mapping statistics.
    ///
    /// Returns bridges sorted by mapping_count descending.
    pub fn domain_bridges(&self) -> Vec<DomainBridge> {
        // Accumulator: (source_domain, target_domain) -> (count, source_fields, target_fields, confidence_sum)
        let mut acc: HashMap<
            (String, String),
            (usize, HashSet<String>, HashSet<String>, f64),
        > = HashMap::new();

        for mappings in self.mappings.values() {
            for m in mappings {
                let src_dom = match self.schema_to_domain.get(&m.source_schema) {
                    Some(d) => d.clone(),
                    None => continue,
                };
                let tgt_dom = match self.schema_to_domain.get(&m.target_schema) {
                    Some(d) => d.clone(),
                    None => continue,
                };

                let entry = acc.entry((src_dom, tgt_dom)).or_insert_with(|| {
                    (0, HashSet::new(), HashSet::new(), 0.0)
                });
                entry.0 += 1;
                entry.1.insert(m.source_field.clone());
                entry.2.insert(m.target_field.clone());
                entry.3 += m.confidence;
            }
        }

        let mut bridges: Vec<DomainBridge> = acc
            .into_iter()
            .map(|((src, tgt), (count, src_fields, tgt_fields, conf_sum))| DomainBridge {
                source_domain: src,
                target_domain: tgt,
                mapping_count: count,
                source_coverage: src_fields.len(),
                target_coverage: tgt_fields.len(),
                avg_confidence: if count > 0 {
                    conf_sum / count as f64
                } else {
                    0.0
                },
            })
            .collect();

        bridges.sort_by(|a, b| b.mapping_count.cmp(&a.mapping_count));
        bridges
    }

    /// Find identity keys where multiple agents disagree.
    ///
    /// A disagreement exists when multiple distinct agents contributed mappings
    /// and either confidence values differ by more than 0.1 or directions differ.
    ///
    /// Returns disagreements sorted by agent_count descending.
    pub fn disagreements(&self) -> Vec<Disagreement> {
        let mut result = Vec::new();

        for ((ss, sf, ts, tf), mappings) in &self.mappings {
            if mappings.len() < 2 {
                continue;
            }

            let agents: HashSet<&str> = mappings.iter().map(|m| m.agent.as_str()).collect();
            if agents.len() < 2 {
                continue;
            }

            // Check for actual disagreement: confidence spread > 0.1 or direction mismatch
            let has_confidence_spread = {
                let confidences: Vec<f64> = mappings.iter().map(|m| m.confidence).collect();
                let min = confidences.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = confidences.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                (max - min) > 0.1
            };

            let has_direction_mismatch = {
                let first_dir = &mappings[0].direction;
                mappings.iter().any(|m| &m.direction != first_dir)
            };

            if has_confidence_spread || has_direction_mismatch {
                result.push(Disagreement {
                    source_schema: ss.clone(),
                    source_field: sf.clone(),
                    target_schema: ts.clone(),
                    target_field: tf.clone(),
                    competing: mappings.clone(),
                    agent_count: agents.len(),
                });
            }
        }

        result.sort_by(|a, b| b.agent_count.cmp(&a.agent_count));
        result
    }

    /// Find mappings where multiple agents converge (agree).
    ///
    /// Convergence: multiple distinct agents, confidence within 0.1, same direction.
    /// Returns one representative mapping per convergent identity key, sorted by
    /// confidence descending.
    pub fn convergent_mappings(&self) -> Vec<&Mapping> {
        let mut result = Vec::new();

        for mappings in self.mappings.values() {
            if mappings.len() < 2 {
                continue;
            }

            let agents: HashSet<&str> = mappings.iter().map(|m| m.agent.as_str()).collect();
            if agents.len() < 2 {
                continue;
            }

            let confidences: Vec<f64> = mappings.iter().map(|m| m.confidence).collect();
            let min = confidences.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = confidences.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let within_tolerance = (max - min) <= 0.1;

            let first_dir = &mappings[0].direction;
            let same_direction = mappings.iter().all(|m| &m.direction == first_dir);

            if within_tolerance && same_direction {
                // Pick the mapping with highest confidence as representative
                let best = mappings
                    .iter()
                    .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap())
                    .unwrap();
                result.push(best);
            }
        }

        result.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
        result
    }

    /// BFS transitive closure from a starting (schema_cid, field) position.
    ///
    /// Follows forward mappings and also reversed bidirectional mappings.
    /// Multiplies confidence at each step. Bounded by max_hops.
    /// Tracks visited (schema, field) pairs to avoid cycles.
    ///
    /// Returns all paths found, sorted by combined_confidence descending.
    pub fn transitive_closure(
        &self,
        schema_cid: &str,
        field: &str,
        max_hops: usize,
    ) -> Vec<TransitivePath> {
        let mut results = Vec::new();
        let mut visited: HashSet<(String, String)> = HashSet::new();
        visited.insert((schema_cid.to_string(), field.to_string()));

        // Queue entries: (current_schema, current_field, path_so_far, combined_confidence, hops_taken)
        let mut queue: VecDeque<(String, String, Vec<(String, String)>, f64, usize)> =
            VecDeque::new();

        let start_path = vec![(schema_cid.to_string(), field.to_string())];
        queue.push_back((
            schema_cid.to_string(),
            field.to_string(),
            start_path,
            1.0,
            0,
        ));

        while let Some((cur_schema, cur_field, path, conf, hops)) = queue.pop_front() {
            if hops >= max_hops {
                continue;
            }

            // Collect all reachable next positions from current position
            let mut next_positions: Vec<(String, String, f64)> = Vec::new();

            for ((ss, sf, ts, tf), mappings) in &self.mappings {
                for m in mappings {
                    // Forward match: source matches current position
                    if ss == &cur_schema && sf == &cur_field {
                        next_positions.push((ts.clone(), tf.clone(), m.confidence));
                    }

                    // Reverse match: target matches current position and mapping
                    // is bidirectional (or we treat reverse direction as traversable)
                    if ts == &cur_schema && tf == &cur_field {
                        match m.direction {
                            Direction::Bidirectional => {
                                next_positions.push((ss.clone(), sf.clone(), m.confidence));
                            }
                            Direction::Reverse => {
                                // A reverse mapping from source->target means the
                                // real flow is target->source, so if we're at target
                                // we can reach source.
                                next_positions.push((ss.clone(), sf.clone(), m.confidence));
                            }
                            Direction::Forward => {
                                // Forward only goes source->target; can't traverse backwards.
                            }
                        }
                    }
                }
            }

            for (next_schema, next_field, step_conf) in next_positions {
                if visited.contains(&(next_schema.clone(), next_field.clone())) {
                    continue;
                }

                visited.insert((next_schema.clone(), next_field.clone()));

                let new_conf = conf * step_conf;
                let mut new_path = path.clone();
                new_path.push((next_schema.clone(), next_field.clone()));

                // Record this as a result (every reachable node is a valid path)
                results.push(TransitivePath {
                    steps: new_path.clone(),
                    combined_confidence: new_conf,
                });

                queue.push_back((next_schema, next_field, new_path, new_conf, hops + 1));
            }
        }

        results.sort_by(|a, b| {
            b.combined_confidence
                .partial_cmp(&a.combined_confidence)
                .unwrap()
        });
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::Mapping;

    fn make_mapping(
        src_schema: &str,
        src_field: &str,
        tgt_schema: &str,
        tgt_field: &str,
        confidence: f64,
        agent: &str,
        direction: Direction,
    ) -> Mapping {
        Mapping::build(src_schema, src_field, tgt_schema, tgt_field)
            .confidence(confidence)
            .agent(agent)
            .direction(direction)
            .justification("test")
            .finish()
    }

    #[test]
    fn domain_bridges_computed() {
        let domains = vec![
            Domain::new("physics", 1).with_schema("schema_p1").with_schema("schema_p2"),
            Domain::new("math", 1).with_schema("schema_m1"),
        ];

        let mappings = vec![
            make_mapping("schema_p1", "mass", "schema_m1", "value", 0.9, "claude", Direction::Forward),
            make_mapping("schema_p1", "velocity", "schema_m1", "rate", 0.8, "claude", Direction::Forward),
            make_mapping("schema_p2", "charge", "schema_m1", "quantity", 0.7, "claude", Direction::Forward),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);
        let bridges = view.domain_bridges();

        assert_eq!(bridges.len(), 1);
        assert_eq!(bridges[0].source_domain, "physics");
        assert_eq!(bridges[0].target_domain, "math");
        assert_eq!(bridges[0].mapping_count, 3);
        assert_eq!(bridges[0].source_coverage, 3); // mass, velocity, charge
        assert_eq!(bridges[0].target_coverage, 3); // value, rate, quantity
        let expected_avg = (0.9 + 0.8 + 0.7) / 3.0;
        assert!((bridges[0].avg_confidence - expected_avg).abs() < 1e-10);
    }

    #[test]
    fn disagreement_detected() {
        let domains = vec![
            Domain::new("a", 1).with_schema("s1"),
            Domain::new("b", 1).with_schema("s2"),
        ];

        let mappings = vec![
            make_mapping("s1", "x", "s2", "y", 0.9, "claude", Direction::Forward),
            make_mapping("s1", "x", "s2", "y", 0.5, "codex", Direction::Forward),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);
        let disagreements = view.disagreements();

        assert_eq!(disagreements.len(), 1);
        assert_eq!(disagreements[0].source_field, "x");
        assert_eq!(disagreements[0].target_field, "y");
        assert_eq!(disagreements[0].agent_count, 2);
        assert_eq!(disagreements[0].competing.len(), 2);
    }

    #[test]
    fn convergence_detected() {
        let domains = vec![
            Domain::new("a", 1).with_schema("s1"),
            Domain::new("b", 1).with_schema("s2"),
        ];

        let mappings = vec![
            make_mapping("s1", "x", "s2", "y", 0.85, "claude", Direction::Forward),
            make_mapping("s1", "x", "s2", "y", 0.90, "codex", Direction::Forward),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);

        // Should not be a disagreement (within 0.1 tolerance and same direction)
        assert!(view.disagreements().is_empty());

        let convergent = view.convergent_mappings();
        assert_eq!(convergent.len(), 1);
        assert_eq!(convergent[0].source_field, "x");
        assert_eq!(convergent[0].target_field, "y");
        // Representative should be the highest confidence
        assert_eq!(convergent[0].confidence, 0.90);
    }

    #[test]
    fn transitive_closure_two_hops() {
        let domains = vec![
            Domain::new("a", 1).with_schema("sa"),
            Domain::new("b", 1).with_schema("sb"),
            Domain::new("c", 1).with_schema("sc"),
        ];

        let mappings = vec![
            make_mapping("sa", "x", "sb", "y", 0.9, "claude", Direction::Forward),
            make_mapping("sb", "y", "sc", "z", 0.8, "claude", Direction::Forward),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);
        let paths = view.transitive_closure("sa", "x", 3);

        // Should find two paths: sa.x -> sb.y and sa.x -> sb.y -> sc.z
        assert_eq!(paths.len(), 2);

        // Highest confidence first: the one-hop path (0.9) beats two-hop (0.72)
        assert!((paths[0].combined_confidence - 0.9).abs() < 1e-10);
        assert_eq!(paths[0].steps.len(), 2);

        assert!((paths[1].combined_confidence - 0.72).abs() < 1e-10);
        assert_eq!(paths[1].steps.len(), 3);
        assert_eq!(paths[1].steps[0], ("sa".to_string(), "x".to_string()));
        assert_eq!(paths[1].steps[1], ("sb".to_string(), "y".to_string()));
        assert_eq!(paths[1].steps[2], ("sc".to_string(), "z".to_string()));
    }

    #[test]
    fn max_hops_respected() {
        let domains = vec![
            Domain::new("a", 1).with_schema("sa"),
            Domain::new("b", 1).with_schema("sb"),
            Domain::new("c", 1).with_schema("sc"),
        ];

        let mappings = vec![
            make_mapping("sa", "x", "sb", "y", 0.9, "claude", Direction::Forward),
            make_mapping("sb", "y", "sc", "z", 0.8, "claude", Direction::Forward),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);
        let paths = view.transitive_closure("sa", "x", 1);

        // With max_hops=1, only the direct hop to sb.y is reachable
        assert_eq!(paths.len(), 1);
        assert!((paths[0].combined_confidence - 0.9).abs() < 1e-10);
        assert_eq!(paths[0].steps.len(), 2);
    }

    #[test]
    fn no_cycles_in_transitive() {
        let domains = vec![
            Domain::new("a", 1).with_schema("sa"),
            Domain::new("b", 1).with_schema("sb"),
        ];

        // Bidirectional mapping: sa.x <-> sb.y
        let mappings = vec![
            make_mapping("sa", "x", "sb", "y", 0.9, "claude", Direction::Bidirectional),
        ];

        let view = CrossTopologyView::new(&domains, &mappings);
        let paths = view.transitive_closure("sa", "x", 10);

        // Should find exactly one path: sa.x -> sb.y
        // Cycle back to sa.x is prevented by visited set.
        assert_eq!(paths.len(), 1);
        assert!((paths[0].combined_confidence - 0.9).abs() < 1e-10);
    }
}
