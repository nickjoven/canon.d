//! Cross-schema field mappings with provenance.
//!
//! A `Mapping` declares that a field in one schema corresponds to a field in
//! another schema, with a confidence score, justification, and directionality.
//! Mappings are themselves canonical objects — they have a schema, serialize
//! deterministically, and can be stored in CAS.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Direction of a mapping between two schema fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Forward,
    Reverse,
    Bidirectional,
}

/// A cross-schema field mapping with provenance metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mapping {
    pub source_schema: String,
    pub source_field: String,
    pub target_schema: String,
    pub target_field: String,
    pub confidence: f64,
    pub justification: String,
    pub agent: String,
    pub direction: Direction,
}

impl Mapping {
    /// Start building a new mapping between two schema fields.
    pub fn build(
        source_schema: &str,
        source_field: &str,
        target_schema: &str,
        target_field: &str,
    ) -> MappingBuilder {
        MappingBuilder {
            source_schema: source_schema.to_string(),
            source_field: source_field.to_string(),
            target_schema: target_schema.to_string(),
            target_field: target_field.to_string(),
            confidence: 0.5,
            justification: String::new(),
            agent: "unknown".to_string(),
            direction: Direction::Forward,
        }
    }

    /// Serialize to a JSON object matching the mapping_schema field names.
    pub fn to_json(&self) -> Value {
        json!({
            "source_schema": self.source_schema,
            "source_field": self.source_field,
            "target_schema": self.target_schema,
            "target_field": self.target_field,
            "confidence": self.confidence,
            "justification": self.justification,
            "agent": self.agent,
            "direction": match self.direction {
                Direction::Forward => "forward",
                Direction::Reverse => "reverse",
                Direction::Bidirectional => "bidirectional",
            },
        })
    }

    /// Parse a Mapping from a JSON value.
    pub fn from_json(value: &Value) -> Result<Self, String> {
        let obj = value.as_object().ok_or("expected JSON object")?;

        let get_str = |key: &str| -> Result<String, String> {
            obj.get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| format!("missing or invalid field: {}", key))
        };

        let direction = match get_str("direction")?.as_str() {
            "forward" => Direction::Forward,
            "reverse" => Direction::Reverse,
            "bidirectional" => Direction::Bidirectional,
            other => return Err(format!("invalid direction: {}", other)),
        };

        let confidence = obj
            .get("confidence")
            .and_then(|v| v.as_f64())
            .ok_or("missing or invalid field: confidence")?;

        Ok(Mapping {
            source_schema: get_str("source_schema")?,
            source_field: get_str("source_field")?,
            target_schema: get_str("target_schema")?,
            target_field: get_str("target_field")?,
            confidence,
            justification: get_str("justification")?,
            agent: get_str("agent")?,
            direction,
        })
    }

    /// Returns the identity key: (source_schema, source_field, target_schema, target_field).
    pub fn identity_key(&self) -> (String, String, String, String) {
        (
            self.source_schema.clone(),
            self.source_field.clone(),
            self.target_schema.clone(),
            self.target_field.clone(),
        )
    }

    /// Create a reversed mapping: source becomes target and vice versa.
    /// Forward flips to Reverse and vice versa; Bidirectional stays.
    pub fn reversed(&self) -> Mapping {
        Mapping {
            source_schema: self.target_schema.clone(),
            source_field: self.target_field.clone(),
            target_schema: self.source_schema.clone(),
            target_field: self.source_field.clone(),
            confidence: self.confidence,
            justification: self.justification.clone(),
            agent: self.agent.clone(),
            direction: match self.direction {
                Direction::Forward => Direction::Reverse,
                Direction::Reverse => Direction::Forward,
                Direction::Bidirectional => Direction::Bidirectional,
            },
        }
    }
}

/// Builder for constructing `Mapping` instances.
pub struct MappingBuilder {
    source_schema: String,
    source_field: String,
    target_schema: String,
    target_field: String,
    confidence: f64,
    justification: String,
    agent: String,
    direction: Direction,
}

impl MappingBuilder {
    /// Set confidence, clamped to 0.0..=1.0.
    pub fn confidence(mut self, v: f64) -> Self {
        self.confidence = v.clamp(0.0, 1.0);
        self
    }

    /// Set justification text.
    pub fn justification(mut self, s: &str) -> Self {
        self.justification = s.to_string();
        self
    }

    /// Set the agent that produced this mapping.
    pub fn agent(mut self, s: &str) -> Self {
        self.agent = s.to_string();
        self
    }

    /// Set the direction of this mapping.
    pub fn direction(mut self, d: Direction) -> Self {
        self.direction = d;
        self
    }

    /// Consume the builder and produce a `Mapping`.
    pub fn finish(self) -> Mapping {
        Mapping {
            source_schema: self.source_schema,
            source_field: self.source_field,
            target_schema: self.target_schema,
            target_field: self.target_field,
            confidence: self.confidence,
            justification: self.justification,
            agent: self.agent,
            direction: self.direction,
        }
    }
}

/// Returns the canonical Schema for Mapping objects.
pub fn mapping_schema() -> crate::schema::Schema {
    use crate::schema::{FieldKind, Schema};
    Schema::new("canon.d/mapping", 1)
        .identity("source_schema", FieldKind::Cid)
        .identity("source_field", FieldKind::String)
        .identity("target_schema", FieldKind::Cid)
        .identity("target_field", FieldKind::String)
        .required("confidence", FieldKind::Float)
        .required("justification", FieldKind::String)
        .required("agent", FieldKind::String)
        .required("direction", FieldKind::String)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let m = Mapping::build("s1", "f1", "s2", "f2").finish();
        assert_eq!(m.confidence, 0.5);
        assert_eq!(m.justification, "");
        assert_eq!(m.agent, "unknown");
        assert_eq!(m.direction, Direction::Forward);
    }

    #[test]
    fn builder_clamps_confidence() {
        let high = Mapping::build("s1", "f1", "s2", "f2")
            .confidence(2.0)
            .finish();
        assert_eq!(high.confidence, 1.0);

        let low = Mapping::build("s1", "f1", "s2", "f2")
            .confidence(-0.5)
            .finish();
        assert_eq!(low.confidence, 0.0);
    }

    #[test]
    fn json_roundtrip() {
        let original = Mapping::build("schema_a", "field_x", "schema_b", "field_y")
            .confidence(0.85)
            .justification("semantically equivalent")
            .agent("claude")
            .direction(Direction::Bidirectional)
            .finish();

        let json = original.to_json();
        let restored = Mapping::from_json(&json).expect("roundtrip should succeed");

        assert_eq!(restored.source_schema, original.source_schema);
        assert_eq!(restored.source_field, original.source_field);
        assert_eq!(restored.target_schema, original.target_schema);
        assert_eq!(restored.target_field, original.target_field);
        assert_eq!(restored.confidence, original.confidence);
        assert_eq!(restored.justification, original.justification);
        assert_eq!(restored.agent, original.agent);
        assert_eq!(restored.direction, original.direction);
    }

    #[test]
    fn identity_key_stable() {
        let m1 = Mapping::build("s1", "f1", "s2", "f2")
            .confidence(0.3)
            .finish();
        let m2 = Mapping::build("s1", "f1", "s2", "f2")
            .confidence(0.9)
            .finish();

        assert_eq!(m1.identity_key(), m2.identity_key());
    }

    #[test]
    fn reversed_swaps_correctly() {
        let m = Mapping::build("alpha", "a_field", "beta", "b_field")
            .confidence(0.7)
            .justification("test")
            .agent("claude")
            .direction(Direction::Forward)
            .finish();

        let r = m.reversed();
        assert_eq!(r.source_schema, "beta");
        assert_eq!(r.source_field, "b_field");
        assert_eq!(r.target_schema, "alpha");
        assert_eq!(r.target_field, "a_field");
        assert_eq!(r.direction, Direction::Reverse);
        assert_eq!(r.confidence, 0.7);

        // Bidirectional stays bidirectional
        let bi = Mapping::build("a", "x", "b", "y")
            .direction(Direction::Bidirectional)
            .finish()
            .reversed();
        assert_eq!(bi.direction, Direction::Bidirectional);
    }

    #[test]
    fn mapping_schema_canonical() {
        let s1 = mapping_schema();
        let s2 = mapping_schema();
        assert_eq!(s1.to_canonical_bytes(), s2.to_canonical_bytes());
    }
}
