//! Domain packs — the pluggable, domain-flavored half of intake.
//!
//! The spine ([`crate::intake::intake_with_routes`]) is domain-free: it seals,
//! runs routes, buckets proposals, audits. Everything that knows what a
//! *document in some domain* looks like lives in a pack: a route roster plus
//! (eventually) a normalizer choice, a status taxonomy, and external-witness
//! importers. DOMAINS.md is the design doc; the flexibility contract is that
//! **adding a pack touches zero spine code** — any spine edit a new pack
//! forces is a leak of domain knowledge into the core, and gets recorded in
//! DOMAINS.md's findings ledger.
//!
//! Current packs:
//! - **prose** (harmonics): lives in [`crate::intake`] as
//!   [`crate::intake::prose_routes`] — lineage sections, corpus citations,
//!   exact-rational claims. Physically moving it here is deliberate churn we
//!   skip; it is a pack by role.
//! - **code** ([`code`]): Rust source as corpus — `use crate::…` / `mod …;`
//!   dependency citations.

pub mod code;
