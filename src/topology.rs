//! Topology: emergent knowledge structure from DAG traversal.
//!
//! This module reads — it never writes. The topology is whatever
//! agents have built by writing schema-tagged nodes into the DAG.
//!
//! Key questions topology answers:
//! - Which schemas co-occur in the same lineage chains?
//! - Which identity projections appear most frequently? (concepts)
//! - What clusters of nodes share both schema and identity? (convergence)
//!
//! All of these are pure graph queries over existing DAG data.
//! No embeddings, no LLM calls, no intelligence above the line.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A cluster of nodes sharing the same schema and identity projection.
///
/// When multiple agents independently produce observations with the same
/// identity fields, those observations form a cluster. The cluster size
/// tells you how much agreement (or at least attention) that concept has.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    /// The schema CID these nodes share.
    pub schema_cid: String,
    /// The canonical identity projection bytes (hashed for lookup).
    pub identity_hash: String,
    /// CIDs of the nodes in this cluster.
    pub node_cids: Vec<String>,
    /// Number of distinct agents that contributed.
    pub agent_count: usize,
}

/// A read-only view of the DAG's emergent topology.
///
/// Constructed from a list of nodes — topology doesn't query the DAG
/// directly. The caller provides the nodes (from `ket dag ls`, lineage
/// traversal, or filtered queries), and TopologyView indexes them.
pub struct TopologyView {
    /// Clusters indexed by (schema_cid, identity_hash).
    clusters: HashMap<(String, String), Cluster>,
    /// Schema co-occurrence: how often two schemas appear in the same
    /// lineage chain. Key is (min(s1,s2), max(s1,s2)) for dedup.
    co_occurrence: HashMap<(String, String), usize>,
}

/// A node descriptor for topology analysis.
/// Lightweight — just the metadata needed, not the full content.
pub struct NodeInfo {
    pub node_cid: String,
    pub schema_cid: Option<String>,
    pub identity_hash: Option<String>,
    pub agent: String,
    /// Schema CIDs of this node's ancestors (for co-occurrence).
    pub ancestor_schemas: Vec<String>,
}

impl TopologyView {
    /// Build a topology view from a set of node descriptors.
    pub fn from_nodes(nodes: &[NodeInfo]) -> Self {
        let mut clusters: HashMap<(String, String), Cluster> = HashMap::new();
        let mut co_occurrence: HashMap<(String, String), usize> = HashMap::new();

        for node in nodes {
            // Cluster by (schema_cid, identity_hash) when both are present
            if let (Some(ref schema), Some(ref identity)) =
                (&node.schema_cid, &node.identity_hash)
            {
                let key = (schema.clone(), identity.clone());
                let cluster = clusters.entry(key).or_insert_with(|| Cluster {
                    schema_cid: schema.clone(),
                    identity_hash: identity.clone(),
                    node_cids: Vec::new(),
                    agent_count: 0,
                });
                cluster.node_cids.push(node.node_cid.clone());
                // agent_count is recomputed below
            }

            // Co-occurrence: pair this node's schema with each ancestor schema
            if let Some(ref schema) = node.schema_cid {
                for ancestor_schema in &node.ancestor_schemas {
                    if ancestor_schema != schema {
                        let pair = if schema < ancestor_schema {
                            (schema.clone(), ancestor_schema.clone())
                        } else {
                            (ancestor_schema.clone(), schema.clone())
                        };
                        *co_occurrence.entry(pair).or_insert(0) += 1;
                    }
                }
            }
        }

        // Recompute agent_count per cluster
        for cluster in clusters.values_mut() {
            let agents: std::collections::HashSet<&str> = nodes
                .iter()
                .filter(|n| cluster.node_cids.contains(&n.node_cid))
                .map(|n| n.agent.as_str())
                .collect();
            cluster.agent_count = agents.len();
        }

        TopologyView {
            clusters,
            co_occurrence,
        }
    }

    /// All clusters, sorted by size (largest first).
    pub fn clusters(&self) -> Vec<&Cluster> {
        let mut v: Vec<&Cluster> = self.clusters.values().collect();
        v.sort_by(|a, b| b.node_cids.len().cmp(&a.node_cids.len()));
        v
    }

    /// Clusters with more than one contributing agent — convergence points.
    pub fn convergent_clusters(&self) -> Vec<&Cluster> {
        self.clusters()
            .into_iter()
            .filter(|c| c.agent_count > 1)
            .collect()
    }

    /// Schema pairs that frequently co-occur in lineage chains.
    /// Returns (schema_a, schema_b, count), sorted by count descending.
    pub fn schema_co_occurrences(&self) -> Vec<(&str, &str, usize)> {
        let mut v: Vec<(&str, &str, usize)> = self
            .co_occurrence
            .iter()
            .map(|((a, b), &count)| (a.as_str(), b.as_str(), count))
            .collect();
        v.sort_by(|a, b| b.2.cmp(&a.2));
        v
    }

    /// How many distinct schemas are represented.
    pub fn schema_count(&self) -> usize {
        let schemas: std::collections::HashSet<&str> = self
            .clusters
            .values()
            .map(|c| c.schema_cid.as_str())
            .collect();
        schemas.len()
    }

    /// Total number of clusters.
    pub fn cluster_count(&self) -> usize {
        self.clusters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clusters_form_by_schema_and_identity() {
        let nodes = vec![
            NodeInfo {
                node_cid: "aaa".into(),
                schema_cid: Some("schema1".into()),
                identity_hash: Some("id1".into()),
                agent: "claude".into(),
                ancestor_schemas: vec![],
            },
            NodeInfo {
                node_cid: "bbb".into(),
                schema_cid: Some("schema1".into()),
                identity_hash: Some("id1".into()),
                agent: "codex".into(),
                ancestor_schemas: vec![],
            },
            NodeInfo {
                node_cid: "ccc".into(),
                schema_cid: Some("schema1".into()),
                identity_hash: Some("id2".into()),
                agent: "claude".into(),
                ancestor_schemas: vec![],
            },
        ];

        let topo = TopologyView::from_nodes(&nodes);
        assert_eq!(topo.cluster_count(), 2);

        let convergent = topo.convergent_clusters();
        assert_eq!(convergent.len(), 1);
        assert_eq!(convergent[0].node_cids.len(), 2);
        assert_eq!(convergent[0].agent_count, 2);
    }

    #[test]
    fn co_occurrence_tracks_schema_pairs() {
        let nodes = vec![
            NodeInfo {
                node_cid: "aaa".into(),
                schema_cid: Some("observation".into()),
                identity_hash: Some("id1".into()),
                agent: "claude".into(),
                ancestor_schemas: vec!["claim".into()],
            },
            NodeInfo {
                node_cid: "bbb".into(),
                schema_cid: Some("observation".into()),
                identity_hash: Some("id2".into()),
                agent: "claude".into(),
                ancestor_schemas: vec!["claim".into()],
            },
        ];

        let topo = TopologyView::from_nodes(&nodes);
        let co = topo.schema_co_occurrences();
        assert_eq!(co.len(), 1);
        assert_eq!(co[0].2, 2); // "observation" co-occurs with "claim" twice
    }
}
