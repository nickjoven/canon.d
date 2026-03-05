//! Structural alignment engine.
//!
//! Compares two schemas and produces candidate field mappings based on
//! name similarity, type compatibility, and identity alignment. Pure
//! structural comparison — no ML, no external crates.

use crate::mapping::{Direction, Mapping};
use crate::schema::{FieldKind, Schema};

/// Configuration for the alignment engine.
#[derive(Debug, Clone)]
pub struct AlignConfig {
    /// Minimum confidence threshold — candidates below this are excluded.
    pub min_confidence: f64,
    /// Weight given to name similarity (0.0–1.0).
    pub name_weight: f64,
    /// Weight given to type compatibility (0.0–1.0).
    pub type_weight: f64,
    /// Weight given to identity alignment (0.0–1.0).
    pub identity_weight: f64,
}

impl Default for AlignConfig {
    fn default() -> Self {
        AlignConfig {
            min_confidence: 0.3,
            name_weight: 0.4,
            type_weight: 0.4,
            identity_weight: 0.2,
        }
    }
}

/// Breakdown of how a candidate's confidence was computed.
#[derive(Debug, Clone)]
pub struct AlignRationale {
    /// Score from name similarity (0.0–1.0).
    pub name_score: f64,
    /// Score from type compatibility (0.0–1.0).
    pub type_score: f64,
    /// Score from identity alignment (0.0–1.0).
    pub identity_score: f64,
}

/// A candidate mapping between a source field and a target field.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Name of the source field.
    pub source_field: String,
    /// Name of the target field.
    pub target_field: String,
    /// Weighted confidence score (0.0–1.0).
    pub confidence: f64,
    /// Breakdown of the score components.
    pub rationale: AlignRationale,
}

/// Align two schemas, producing candidate mappings sorted by confidence descending.
///
/// For each source field, every target field is compared. The weighted score
/// is computed from name similarity, type compatibility, and identity alignment.
/// Candidates below `config.min_confidence` are filtered out.
pub fn align(source: &Schema, target: &Schema, config: &AlignConfig) -> Vec<Candidate> {
    let mut candidates = Vec::new();

    for sf in &source.fields {
        for tf in &target.fields {
            let ns = name_similarity(&sf.name, &tf.name);
            let ts = type_compatibility(&sf.kind, &tf.kind);
            let is = identity_alignment(sf.identity, tf.identity);

            let confidence = config.name_weight * ns
                + config.type_weight * ts
                + config.identity_weight * is;

            if confidence >= config.min_confidence {
                candidates.push(Candidate {
                    source_field: sf.name.clone(),
                    target_field: tf.name.clone(),
                    confidence,
                    rationale: AlignRationale {
                        name_score: ns,
                        type_score: ts,
                        identity_score: is,
                    },
                });
            }
        }
    }

    candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
    candidates
}

/// Normalized Levenshtein distance with substring bonus.
///
/// Returns 0.0 (completely different) to 1.0 (identical).
/// Both strings are compared case-insensitively. If one string contains
/// the other as a substring, a 0.3 bonus is added (clamped to 1.0).
fn name_similarity(a: &str, b: &str) -> f64 {
    let a_lower = a.to_lowercase();
    let b_lower = b.to_lowercase();

    if a_lower.is_empty() && b_lower.is_empty() {
        return 1.0;
    }
    if a_lower.is_empty() || b_lower.is_empty() {
        return 0.0;
    }

    let a_bytes = a_lower.as_bytes();
    let b_bytes = b_lower.as_bytes();
    let m = a_bytes.len();
    let n = b_bytes.len();

    // Two-row DP Levenshtein
    let mut prev = vec![0usize; n + 1];
    let mut curr = vec![0usize; n + 1];

    for j in 0..=n {
        prev[j] = j;
    }

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_bytes[i - 1] == b_bytes[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    let distance = prev[n];
    let max_len = m.max(n);
    let similarity = 1.0 - (distance as f64 / max_len as f64);

    // Substring bonus
    let bonus = if a_lower.contains(&b_lower) || b_lower.contains(&a_lower) {
        0.3
    } else {
        0.0
    };

    (similarity + bonus).min(1.0)
}

/// Type compatibility score.
///
/// - Same variant: 1.0
/// - Integer + Float: 0.5
/// - String + Cid: 0.5 (CIDs are stored as strings)
/// - Ref + Cid: 0.3
/// - List(X) + List(Y): recursive type_compatibility(X, Y)
/// - Everything else: 0.0
fn type_compatibility(a: &FieldKind, b: &FieldKind) -> f64 {
    if a == b {
        return 1.0;
    }

    match (a, b) {
        (FieldKind::Integer, FieldKind::Float) | (FieldKind::Float, FieldKind::Integer) => 0.5,
        (FieldKind::String, FieldKind::Cid) | (FieldKind::Cid, FieldKind::String) => 0.5,
        (FieldKind::Ref(_), FieldKind::Cid) | (FieldKind::Cid, FieldKind::Ref(_)) => 0.3,
        (FieldKind::List(inner_a), FieldKind::List(inner_b)) => {
            type_compatibility(inner_a, inner_b)
        }
        _ => 0.0,
    }
}

/// Identity alignment score.
///
/// - Both identity-bearing: 1.0
/// - Neither identity-bearing: 1.0 (same status = agreement)
/// - Mismatched: 0.3
fn identity_alignment(a_identity: bool, b_identity: bool) -> f64 {
    if a_identity == b_identity {
        1.0
    } else {
        0.3
    }
}

/// Convert alignment candidates into `Mapping` objects.
///
/// Each candidate becomes a forward mapping with the candidate's confidence
/// and a justification string built from the rationale scores.
pub fn candidates_to_mappings(
    source_schema_cid: &str,
    target_schema_cid: &str,
    candidates: &[Candidate],
    agent: &str,
) -> Vec<Mapping> {
    candidates
        .iter()
        .map(|c| {
            let justification = format!(
                "structural alignment: name={:.2}, type={:.2}, identity={:.2}",
                c.rationale.name_score, c.rationale.type_score, c.rationale.identity_score,
            );
            Mapping::build(
                source_schema_cid,
                &c.source_field,
                target_schema_cid,
                &c.target_field,
            )
            .confidence(c.confidence)
            .justification(&justification)
            .agent(agent)
            .direction(Direction::Forward)
            .finish()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldKind, Schema};

    #[test]
    fn identical_schemas() {
        let schema = Schema::new("test", 1)
            .identity("id", FieldKind::String)
            .required("name", FieldKind::String)
            .required("value", FieldKind::Float);

        let config = AlignConfig::default();
        let candidates = align(&schema, &schema, &config);

        // Each field should match itself with 1.0 confidence
        for field in &schema.fields {
            let self_match = candidates
                .iter()
                .find(|c| c.source_field == field.name && c.target_field == field.name)
                .expect("each field should have a self-match");
            assert!(
                (self_match.confidence - 1.0).abs() < 1e-9,
                "field '{}' self-match confidence should be 1.0, got {}",
                field.name,
                self_match.confidence
            );
        }
    }

    #[test]
    fn name_similarity_cases() {
        // Exact match
        assert!((name_similarity("patient_id", "patient_id") - 1.0).abs() < 1e-9);

        // High similarity with substring bonus
        let score = name_similarity("patient_id", "patient_identifier");
        assert!(score > 0.7, "patient_id vs patient_identifier should be high, got {}", score);

        // Low similarity
        let score = name_similarity("name", "address");
        assert!(score < 0.4, "name vs address should be low, got {}", score);

        // Substring bonus: "id" is contained in "identifier"
        let score = name_similarity("id", "identifier");
        assert!(score >= 0.5 - 1e-9, "id vs identifier should get substring bonus, got {}", score);
    }

    #[test]
    fn type_compat_cases() {
        assert!((type_compatibility(&FieldKind::String, &FieldKind::String) - 1.0).abs() < 1e-9);
        assert!((type_compatibility(&FieldKind::Integer, &FieldKind::Float) - 0.5).abs() < 1e-9);
        assert!((type_compatibility(&FieldKind::String, &FieldKind::Bool) - 0.0).abs() < 1e-9);

        // List recursion
        let list_int = FieldKind::List(Box::new(FieldKind::Integer));
        let list_float = FieldKind::List(Box::new(FieldKind::Float));
        assert!((type_compatibility(&list_int, &list_float) - 0.5).abs() < 1e-9);

        // String + Cid
        assert!((type_compatibility(&FieldKind::String, &FieldKind::Cid) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn cross_domain_alignment() {
        let medical = Schema::new("medical", 1)
            .identity("patient_id", FieldKind::String)
            .required("diagnosis", FieldKind::String)
            .required("date", FieldKind::String);

        let insurance = Schema::new("insurance", 1)
            .identity("member_id", FieldKind::String)
            .required("condition", FieldKind::String)
            .required("claim_date", FieldKind::String);

        let config = AlignConfig::default();
        let candidates = align(&medical, &insurance, &config);

        // patient_id <-> member_id: name similarity is low ("patient_id" vs "member_id"),
        // but identity alignment is high (both are identity fields).
        // diagnosis <-> condition: both non-identity, same type, but name similarity is low.
        // date <-> claim_date: "date" is a substring of "claim_date", so substring bonus applies.

        // date <-> claim_date should have good confidence due to substring bonus
        let date_claim = candidates
            .iter()
            .find(|c| c.source_field == "date" && c.target_field == "claim_date")
            .expect("date -> claim_date candidate should exist");
        assert!(
            date_claim.confidence > 0.5,
            "date -> claim_date should have decent confidence, got {}",
            date_claim.confidence
        );

        // patient_id <-> member_id should exist (identity boost helps)
        let pid_mid = candidates
            .iter()
            .find(|c| c.source_field == "patient_id" && c.target_field == "member_id");
        assert!(
            pid_mid.is_some(),
            "patient_id -> member_id should be a candidate"
        );

        // Verify that patient_id <-> member_id does NOT score highest on name alone
        if let Some(c) = pid_mid {
            assert!(
                c.rationale.name_score < 0.5,
                "patient_id vs member_id name score should be low, got {}",
                c.rationale.name_score
            );
        }
    }

    #[test]
    fn min_confidence_filter() {
        let source = Schema::new("a", 1)
            .required("foo", FieldKind::String)
            .required("bar", FieldKind::Integer);

        let target = Schema::new("b", 1)
            .required("baz", FieldKind::Bool)
            .required("qux", FieldKind::Float);

        // High threshold should filter out most/all candidates
        let strict = AlignConfig {
            min_confidence: 0.95,
            ..AlignConfig::default()
        };
        let candidates = align(&source, &target, &strict);
        // With very different names and types, nothing should pass 0.95
        for c in &candidates {
            assert!(
                c.confidence >= 0.95,
                "candidate {} -> {} with confidence {} should not have passed filter",
                c.source_field,
                c.target_field,
                c.confidence
            );
        }

        // Lenient threshold should allow more candidates
        let lenient = AlignConfig {
            min_confidence: 0.0,
            ..AlignConfig::default()
        };
        let lenient_candidates = align(&source, &target, &lenient);
        assert!(
            lenient_candidates.len() >= candidates.len(),
            "lenient filter should produce at least as many candidates"
        );
    }

    #[test]
    fn candidates_to_mappings_conversion() {
        let candidates = vec![
            Candidate {
                source_field: "src_a".to_string(),
                target_field: "tgt_a".to_string(),
                confidence: 0.85,
                rationale: AlignRationale {
                    name_score: 0.7,
                    type_score: 1.0,
                    identity_score: 0.5,
                },
            },
            Candidate {
                source_field: "src_b".to_string(),
                target_field: "tgt_b".to_string(),
                confidence: 0.6,
                rationale: AlignRationale {
                    name_score: 0.4,
                    type_score: 0.5,
                    identity_score: 1.0,
                },
            },
        ];

        let mappings = candidates_to_mappings("cid_source", "cid_target", &candidates, "test-agent");

        assert_eq!(mappings.len(), 2);

        assert_eq!(mappings[0].source_schema, "cid_source");
        assert_eq!(mappings[0].target_schema, "cid_target");
        assert_eq!(mappings[0].source_field, "src_a");
        assert_eq!(mappings[0].target_field, "tgt_a");
        assert_eq!(mappings[0].confidence, 0.85);
        assert_eq!(mappings[0].agent, "test-agent");
        assert_eq!(mappings[0].direction, Direction::Forward);
        assert!(
            mappings[0].justification.contains("name=0.70"),
            "justification should contain name score: {}",
            mappings[0].justification
        );
        assert!(
            mappings[0].justification.contains("type=1.00"),
            "justification should contain type score: {}",
            mappings[0].justification
        );
        assert!(
            mappings[0].justification.contains("identity=0.50"),
            "justification should contain identity score: {}",
            mappings[0].justification
        );

        assert_eq!(mappings[1].source_field, "src_b");
        assert_eq!(mappings[1].confidence, 0.6);
    }
}
