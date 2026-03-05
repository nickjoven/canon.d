//! Schema definitions for canonical serialization.
//!
//! A Schema declares:
//! - What fields exist (name, type, required/optional)
//! - Which fields are **identity-bearing** (determine sameness)
//! - A canonical field ordering (deterministic serialization)
//!
//! The schema itself is content-addressed: serialize it canonically,
//! hash it, store it in CAS. The schema CID then tags every node
//! whose output conforms to it.

use serde::{Deserialize, Serialize};

/// What kind of value a field holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    /// UTF-8 string.
    String,
    /// Integer (i64).
    Integer,
    /// Floating point (f64).
    Float,
    /// Boolean.
    Bool,
    /// A CID referencing another object in the store.
    Cid,
    /// Nested object conforming to another schema (by CID).
    Ref(std::string::String),
    /// Ordered list of values (homogeneous kind).
    List(Box<FieldKind>),
}

/// A single field in a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Field name. Must be unique within the schema.
    pub name: std::string::String,
    /// Value type.
    pub kind: FieldKind,
    /// Is this field required? Optional fields are omitted from canonical
    /// form when absent (not serialized as null).
    pub required: bool,
    /// Is this field identity-bearing? Identity fields determine whether
    /// two records represent "the same thing." Non-identity fields are
    /// still canonicalized but don't affect sameness judgments.
    pub identity: bool,
}

/// A schema for canonical serialization.
///
/// Fields are stored in canonical order — the order they appear in `fields`
/// is the order they serialize. This is the schema author's responsibility:
/// choose an ordering and stick with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Human-readable name (e.g., "observation", "code-review", "claim").
    pub name: std::string::String,
    /// Schema version. Incrementing this produces a new schema CID,
    /// making version transitions visible in the DAG.
    pub version: u32,
    /// Ordered list of fields. The order defines canonical serialization order.
    pub fields: Vec<Field>,
}

impl Schema {
    /// Create a new schema with no fields.
    pub fn new(name: &str, version: u32) -> Self {
        Schema {
            name: name.to_string(),
            version,
            fields: Vec::new(),
        }
    }

    /// Add a required, identity-bearing field.
    pub fn identity(mut self, name: &str, kind: FieldKind) -> Self {
        self.fields.push(Field {
            name: name.to_string(),
            kind,
            required: true,
            identity: true,
        });
        self
    }

    /// Add a required, non-identity field.
    pub fn required(mut self, name: &str, kind: FieldKind) -> Self {
        self.fields.push(Field {
            name: name.to_string(),
            kind,
            required: true,
            identity: false,
        });
        self
    }

    /// Add an optional, non-identity field.
    pub fn optional(mut self, name: &str, kind: FieldKind) -> Self {
        self.fields.push(Field {
            name: name.to_string(),
            kind,
            required: false,
            identity: false,
        });
        self
    }

    /// Return only the identity-bearing fields.
    pub fn identity_fields(&self) -> Vec<&Field> {
        self.fields.iter().filter(|f| f.identity).collect()
    }

    /// Serialize the schema itself to canonical bytes.
    /// This is what you hash to get the schema CID.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        // Schema serialization is itself canonical: serde_json with sorted
        // keys isn't needed here because the struct field order is fixed
        // by derive(Serialize). We just need deterministic output.
        serde_json::to_vec(self).expect("schema serialization cannot fail")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_canonical_determinism() {
        let s1 = Schema::new("observation", 1)
            .identity("subject", FieldKind::String)
            .identity("predicate", FieldKind::String)
            .required("value", FieldKind::String)
            .optional("confidence", FieldKind::Float);

        let s2 = Schema::new("observation", 1)
            .identity("subject", FieldKind::String)
            .identity("predicate", FieldKind::String)
            .required("value", FieldKind::String)
            .optional("confidence", FieldKind::Float);

        assert_eq!(s1.to_canonical_bytes(), s2.to_canonical_bytes());
    }

    #[test]
    fn identity_fields_filter() {
        let s = Schema::new("test", 1)
            .identity("id", FieldKind::String)
            .required("data", FieldKind::String)
            .optional("note", FieldKind::String);

        let id_fields = s.identity_fields();
        assert_eq!(id_fields.len(), 1);
        assert_eq!(id_fields[0].name, "id");
    }
}
