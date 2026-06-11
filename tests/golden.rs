//! Golden vectors — the cross-environment release contract (`RELEASE.md`).
//!
//! These constants are the substrate's observable identity: if ANY of them moves,
//! every address in every deployed corpus moves with it. A new toolchain, a new
//! serde_json, a new platform, a refactor — all must reproduce these bytes
//! exactly, or the change is a **law change** and ships only as a major release
//! with a new constitution root.
//!
//! Updating a constant here is therefore never routine maintenance: it is the
//! release act itself. The diff *is* the migration notice.

use canon_d::{proposition_schema, seal_constitution, Quantum};
use canon_d::quantum::schema_cid;
use serde_json::json;

/// V1 — the law's own bytes: the proposition schema's CID.
/// Pins: Schema::to_canonical_bytes (incl. the serde_json `preserve_order`-OFF
/// dependency) and blake3.
const GOLDEN_PROPOSITION_SCHEMA_CID: &str =
    "90931a421873d007b417634c2876d3d4d43fabfbc5b7ac4ef6ca8c163f76388d";

/// V2 — the seal path: Ω_Λ = 13/19 sealed as a proposition.
/// Pins: Canon::encode field ordering, identity projection, the address function
/// (schema_cid ‖ 0x1f ‖ identity).
const GOLDEN_OMEGA_CID: &str =
    "febf3ea4dac3d3284995cbde0569a8fe17d23ffece1748f60e449df15f98e2b0";

/// V3 — the meta tier: the keystone (meta-schema sealed under itself).
/// Pins: the self-hosting fixpoint and the treaty's canonicalizer fingerprint.
const GOLDEN_KEYSTONE_CID: &str =
    "f9899f6c43c6971adf0f9c2242da26ecf49b7e5443c93b09b5f9905c2502f8fc";

/// V4 — the consensus construction: the constitution root.
/// Pins: closure settle semantics + consensus_root's domain-separated,
/// length-prefixed Merkle fold, over all nine laws at once.
const GOLDEN_CONSTITUTION_ROOT: &str =
    "c8ccc5efa0d4adfbb8726071649ca388507613a81ea811e4b49f4511f6d35211";

fn omega() -> Quantum {
    Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":13,"den":19,"value":"13/19"}),
    )
    .unwrap()
}

#[test]
fn golden_v1_schema_bytes() {
    assert_eq!(schema_cid(&proposition_schema()), GOLDEN_PROPOSITION_SCHEMA_CID);
}

#[test]
fn golden_v2_seal_path() {
    assert_eq!(omega().cid, GOLDEN_OMEGA_CID);
}

#[test]
fn golden_v3_keystone() {
    assert_eq!(seal_constitution().meta.cid, GOLDEN_KEYSTONE_CID);
}

#[test]
fn golden_v4_consensus() {
    assert_eq!(seal_constitution().root, GOLDEN_CONSTITUTION_ROOT);
}
