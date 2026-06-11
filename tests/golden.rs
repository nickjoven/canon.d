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
/// History: `f9899f…f8fc` (schema.v1, identity-only) → current (schema.v2 adds
/// the census witness — the constitution gains the dual-route audit facts have).
const GOLDEN_KEYSTONE_CID: &str =
    "223a939a7c4410aa2feae800b11b5369e8a99c1caab531bbf5e94a45bed6dc1c";

/// V4 — the consensus construction: the constitution root.
/// Pins: closure settle semantics + consensus_root's domain-separated,
/// length-prefixed Merkle fold, over all nine laws at once.
/// History: `c8ccc5…5211` (under schema.v1) → current (under schema.v2).
const GOLDEN_CONSTITUTION_ROOT: &str =
    "d48290d6b66938a82d12209eccada7143f18d807be0c1cf709b07d5c1088a460";

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
