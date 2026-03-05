//! The canonicalizer: structured data in, deterministic bytes out.
//!
//! Given a Schema and a JSON object, the Canon:
//! 1. Validates required fields are present
//! 2. Drops unknown fields (not in schema)
//! 3. Orders fields according to schema order
//! 4. Serializes to compact JSON with no trailing whitespace
//!
//! The result: semantically identical inputs produce byte-identical outputs.

use crate::schema::{FieldKind, Schema};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum CanonError {
    #[error("missing required field: {0}")]
    MissingField(String),
    #[error("field `{field}` expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// The canonicalizer.
pub struct Canon<'a> {
    schema: &'a Schema,
}

impl<'a> Canon<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Canon { schema }
    }

    /// Canonicalize a JSON value according to the schema.
    ///
    /// Returns canonical bytes ready for `ket put`.
    pub fn encode(&self, input: &Value) -> Result<Vec<u8>, CanonError> {
        let obj = input
            .as_object()
            .ok_or_else(|| CanonError::TypeMismatch {
                field: "<root>".into(),
                expected: "object".into(),
                actual: type_name(input).into(),
            })?;

        let mut canonical = serde_json::Map::new();

        // Walk fields in schema order
        for field in &self.schema.fields {
            match obj.get(&field.name) {
                Some(val) => {
                    validate_type(&field.name, &field.kind, val)?;
                    canonical.insert(field.name.clone(), normalize_value(&field.kind, val));
                }
                None => {
                    if field.required {
                        return Err(CanonError::MissingField(field.name.clone()));
                    }
                    // Optional absent fields are simply omitted — not null.
                }
            }
        }

        // serde_json::Map preserves insertion order, and we inserted
        // in schema field order. Compact serialization, no trailing newline.
        let bytes = serde_json::to_vec(&Value::Object(canonical))?;
        Ok(bytes)
    }

    /// Decode canonical bytes back to a JSON value.
    pub fn decode(&self, bytes: &[u8]) -> Result<Value, CanonError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Extract only the identity-bearing fields from an input,
    /// canonicalize them, and return the bytes.
    ///
    /// Two records with the same identity projection represent
    /// "the same thing" even if their non-identity fields differ.
    pub fn identity_projection(&self, input: &Value) -> Result<Vec<u8>, CanonError> {
        let obj = input
            .as_object()
            .ok_or_else(|| CanonError::TypeMismatch {
                field: "<root>".into(),
                expected: "object".into(),
                actual: type_name(input).into(),
            })?;

        let mut canonical = serde_json::Map::new();
        for field in &self.schema.fields {
            if !field.identity {
                continue;
            }
            match obj.get(&field.name) {
                Some(val) => {
                    validate_type(&field.name, &field.kind, val)?;
                    canonical.insert(field.name.clone(), normalize_value(&field.kind, val));
                }
                None => {
                    return Err(CanonError::MissingField(field.name.clone()));
                }
            }
        }

        Ok(serde_json::to_vec(&Value::Object(canonical))?)
    }
}

/// Check that a value matches the expected field kind.
fn validate_type(field_name: &str, kind: &FieldKind, val: &Value) -> Result<(), CanonError> {
    let ok = match kind {
        FieldKind::String | FieldKind::Cid => val.is_string(),
        FieldKind::Integer => val.is_i64() || val.is_u64(),
        FieldKind::Float => val.is_number(),
        FieldKind::Bool => val.is_boolean(),
        FieldKind::Ref(_) => val.is_string(), // stored as CID string
        FieldKind::List(_) => val.is_array(),
    };
    if ok {
        Ok(())
    } else {
        Err(CanonError::TypeMismatch {
            field: field_name.to_string(),
            expected: format!("{kind:?}"),
            actual: type_name(val).to_string(),
        })
    }
}

/// Normalize a value for canonical output.
///
/// - Strings: kept as-is (trimming is the caller's job)
/// - Numbers: serialized via serde_json (deterministic)
/// - Lists: elements normalized recursively, order preserved
/// - Objects: not directly supported at field level (use Ref)
fn normalize_value(kind: &FieldKind, val: &Value) -> Value {
    match kind {
        FieldKind::List(inner_kind) => {
            if let Some(arr) = val.as_array() {
                Value::Array(
                    arr.iter()
                        .map(|v| normalize_value(inner_kind, v))
                        .collect(),
                )
            } else {
                val.clone()
            }
        }
        _ => val.clone(),
    }
}

fn type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn observation_schema() -> Schema {
        Schema::new("observation", 1)
            .identity("subject", FieldKind::String)
            .identity("predicate", FieldKind::String)
            .required("value", FieldKind::String)
            .optional("confidence", FieldKind::Float)
    }

    #[test]
    fn canonical_determinism() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        // Same data, different JSON key order
        let a: Value = serde_json::from_str(
            r#"{"subject":"rust","predicate":"is","value":"fast","confidence":0.9}"#,
        )
        .unwrap();
        let b: Value = serde_json::from_str(
            r#"{"confidence":0.9,"value":"fast","predicate":"is","subject":"rust"}"#,
        )
        .unwrap();

        let ca = canon.encode(&a).unwrap();
        let cb = canon.encode(&b).unwrap();
        assert_eq!(ca, cb, "different key order must produce identical bytes");
    }

    #[test]
    fn drops_unknown_fields() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        let input: Value = serde_json::from_str(
            r#"{"subject":"x","predicate":"y","value":"z","extra":"should be dropped"}"#,
        )
        .unwrap();

        let bytes = canon.encode(&input).unwrap();
        let decoded: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(decoded.get("extra").is_none());
    }

    #[test]
    fn rejects_missing_required() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        let input: Value =
            serde_json::from_str(r#"{"subject":"x","predicate":"y"}"#).unwrap();

        assert!(canon.encode(&input).is_err());
    }

    #[test]
    fn optional_fields_omitted_when_absent() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        let with: Value = serde_json::from_str(
            r#"{"subject":"x","predicate":"y","value":"z","confidence":0.8}"#,
        )
        .unwrap();
        let without: Value =
            serde_json::from_str(r#"{"subject":"x","predicate":"y","value":"z"}"#).unwrap();

        let bw = canon.encode(&with).unwrap();
        let bwo = canon.encode(&without).unwrap();
        assert_ne!(bw, bwo, "with and without optional should differ");

        let decoded: Value = serde_json::from_slice(&bwo).unwrap();
        assert!(decoded.get("confidence").is_none());
    }

    #[test]
    fn identity_projection_ignores_non_identity() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        let a: Value = serde_json::from_str(
            r#"{"subject":"rust","predicate":"is","value":"fast","confidence":0.9}"#,
        )
        .unwrap();
        let b: Value = serde_json::from_str(
            r#"{"subject":"rust","predicate":"is","value":"slow","confidence":0.1}"#,
        )
        .unwrap();

        let pa = canon.identity_projection(&a).unwrap();
        let pb = canon.identity_projection(&b).unwrap();
        assert_eq!(pa, pb, "same identity fields must produce same projection");
    }

    #[test]
    fn roundtrip() {
        let schema = observation_schema();
        let canon = Canon::new(&schema);

        let input: Value = serde_json::from_str(
            r#"{"subject":"x","predicate":"y","value":"z","confidence":0.5}"#,
        )
        .unwrap();

        let bytes = canon.encode(&input).unwrap();
        let decoded = canon.decode(&bytes).unwrap();

        assert_eq!(decoded["subject"], "x");
        assert_eq!(decoded["predicate"], "y");
        assert_eq!(decoded["value"], "z");
        assert_eq!(decoded["confidence"], 0.5);
    }
}
