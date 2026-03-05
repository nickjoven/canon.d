//! canon.d — Canonical serialization for content-addressed substrates.
//!
//! Three operations, one principle:
//!
//! - **Write**: Encode structured data into canonical byte form. Identical claims
//!   produce identical bytes, therefore identical CIDs. This is the dedup guarantee.
//!
//! - **Read**: Given a CID and its schema, decode back to structured data with
//!   field-level access. Schemas teach agents what fields exist and how they relate.
//!
//! - **Topology**: Traverse a DAG filtered by schema. The shape of the subgraph —
//!   which nodes cluster, which schemas co-occur, which lineage chains share
//!   structure — is emergent knowledge about the domain. Nobody designs it;
//!   it falls out of what agents write.
//!
//! canon.d is a preprocessor and postprocessor for a content-addressed store.
//! It does not replace the store. It sits between the agent and `ket put`.

pub mod alignment;
mod canon;
pub mod cross_topology;
pub mod domain;
pub mod mapping;
mod schema;
mod topology;

pub use alignment::{AlignConfig, AlignRationale, Candidate, align, candidates_to_mappings};
pub use canon::{Canon, CanonError};
pub use cross_topology::{CrossTopologyView, DomainBridge, Disagreement, TransitivePath};
pub use domain::Domain;
pub use mapping::{Direction, Mapping, MappingBuilder, mapping_schema};
pub use schema::{Field, FieldKind, Schema};
pub use topology::{Cluster, TopologyView};
