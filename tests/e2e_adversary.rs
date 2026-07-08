//! E2E-2 — the **adversary journey**: build the honest Ω_Λ = 13/19 corpus once,
//! then inject each *class* of corruption and assert the matching defense fires —
//! all **composed** on top of one corpus. The point of doing them together is to
//! show the defenses don't interfere: the fix for one attack (the proposition /
//! assertion strata split, content-addressing, the cross-audit, the trust set)
//! doesn't blind another, and corroboration is not mistaken for an attack.
//!
//! One honest corpus, six focused tests:
//!   1. body tamper            → `Quantum::verify` (re-hash) catches it
//!   2. substituted fact       → `consensus_root` moves
//!   3. buggy generator        → `verify_regeneration` is false
//!   4. forged / untrusted vouch → `audit_anchor` not auditable; `verify_vouch` false
//!   5. canonicalizer under-merge → `cross_audit` → `UnderMerge`
//!   6. corroboration          → `cross_audit` stays CLEAN (the strata fix, e2e)

mod common;
use common::{build_closure, omega_corpus};

use canon_d::{
    audit_anchor, consensus_root, cross_audit, proposition_schema, signing_key,
    verify_regeneration, verify_vouch, vouch, Closure, CrossAuditConflict, Quantum, Vouch,
};
use serde_json::json;

/// 1. **Body tamper caught by verify.** Mutate an identity field on a clone of the
/// honest proposition, keeping its CID. The stored bytes no longer hash to the
/// address, so `verify` is false — while the untouched original still verifies.
#[test]
fn body_tamper_is_caught_by_verify() {
    let c = omega_corpus();
    let schema = proposition_schema();

    // honest fact re-hashes to its own address
    assert!(
        c.proposition.verify(&schema).unwrap(),
        "the honest proposition verifies"
    );

    // in-place tamper: keep the CID, mutate an identity field
    let mut tampered = c.proposition.clone();
    tampered.body["num"] = json!(99);
    assert!(
        !tampered.verify(&schema).unwrap(),
        "a tampered body no longer hashes to its stored address"
    );

    // the original is unaffected by the clone-and-mutate
    assert!(
        c.proposition.verify(&schema).unwrap(),
        "tampering a clone does not corrupt the honest quantum"
    );
}

/// 2. **Substituted fact moves the consensus root.** Seal a *different* honest
/// proposition (different num/den → different CID, because identity is content)
/// and ground it where the real fact stood. Content-addressing makes the
/// substitution surface as a different root — two verifiers comparing one hash
/// detect it. (Mirrors `chain::tests`: a real substitution is a re-seal, not a
/// clone mutation.)
#[test]
fn substituted_fact_moves_the_consensus_root() {
    let c = omega_corpus();
    let honest_root = consensus_root(&build_closure(&c, false));

    // a genuinely different fact, sealed honestly → a different CID
    let substituted = Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":99,"den":1,"value":"99/1"}),
    )
    .unwrap();
    assert_ne!(
        substituted.cid, c.proposition.cid,
        "a different fraction is a different proposition CID"
    );

    // build the same chain but ground the substituted fact instead of the real one
    let mut tampered = Closure::new();
    tampered.add_anchor(&c.anchor.cid);
    tampered.add_rule(
        c.generator.cid.clone(),
        [c.anchor.cid.clone()].into_iter().collect(),
    );
    tampered.add_rule(
        substituted.cid.clone(),
        [c.generator.cid.clone()].into_iter().collect(),
    );

    assert_ne!(
        honest_root,
        consensus_root(&tampered),
        "a substituted fact moves the consensus root"
    );
}

/// 3. **Buggy generator caught by verify_regeneration.** Claim the generator
/// produced a fact it did not (a wrong proposition CID): re-running the program
/// reproduces 13/19, which is not the claimed CID, so the link is false. The
/// untampered claim (the real CID) verifies true.
#[test]
fn buggy_generator_is_caught_by_verify_regeneration() {
    let c = omega_corpus();

    // the honest regeneration link holds
    assert!(
        verify_regeneration(
            &c.program,
            &[],
            "omega_lambda",
            "harmonics",
            &c.proposition.cid
        )
        .unwrap(),
        "the generator reproduces the real fact"
    );

    // a wrong claimed output: the program produces 13/19, not 1/2
    let wrong = Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":1,"den":2,"value":"1/2"}),
    )
    .unwrap();
    assert_ne!(wrong.cid, c.proposition.cid);
    assert!(
        !verify_regeneration(&c.program, &[], "omega_lambda", "harmonics", &wrong.cid).unwrap(),
        "claiming a generator produced a fact it did not is caught by regeneration"
    );
}

/// 4. **Untrusted / forged vouch fails the audit.** A vouch from a *different* key
/// over the anchor is cryptographically valid (`verify_vouch` true) but its signer
/// is not in the trusted set → the anchor is not auditable. Separately, a vouch
/// whose `attestation_cid` is tampered fails `verify_vouch` outright.
#[test]
fn untrusted_or_forged_vouch_fails_the_audit() {
    let c = omega_corpus();
    let (log, trusted) = common::trusted_log(&c);

    // sanity: the honest vouch audits the anchor
    let honest = audit_anchor(&c.anchor.cid, &[c.vouch.clone()], &trusted, &log);
    assert!(
        honest.is_auditable(),
        "the trusted, logged vouch audits the anchor"
    );

    // a stranger's key signs the *same* anchor: cryptographically valid …
    let stranger = vouch(&signing_key(&[1u8; 32]), &c.anchor.cid);
    assert!(
        verify_vouch(&stranger),
        "the stranger's signature is cryptographically valid over the anchor"
    );
    // … but the signer is outside the root of trust → not auditable
    let untrusted = audit_anchor(&c.anchor.cid, &[stranger], &trusted, &log);
    assert!(
        !untrusted.is_auditable(),
        "a valid signature from outside the trusted set does not make the anchor auditable"
    );

    // a vouch whose vouched CID is tampered fails verify_vouch (signature no longer matches)
    let mut forged = c.vouch.clone();
    forged.attestation_cid = "not-the-anchor".to_string();
    assert!(
        !verify_vouch(&forged),
        "tampering what was vouched breaks the signature"
    );
    // and so it audits nothing
    let forged_audit = audit_anchor(&c.anchor.cid, &[forged], &trusted, &log);
    assert!(
        !forged_audit.is_auditable(),
        "a forged vouch audits nothing"
    );
}

/// 5. **Canonicalizer under-merge caught by cross_audit.** Two propositions with
/// the SAME witness `value:"13/19"` but different identity (`13/19` vs the
/// unreduced `26/38` a buggy canonicalizer would leave): two CIDs, one witness →
/// `UnderMerge`. The witness route catches what dedup-by-form cannot.
#[test]
fn canonicalizer_under_merge_is_caught_by_cross_audit() {
    let c = omega_corpus();
    let schema = proposition_schema();

    let reduced = Quantum::seal(
        &schema,
        &json!({"subject":"omega_lambda","num":13,"den":19,"value":"13/19"}),
    )
    .unwrap();
    let unreduced = Quantum::seal(
        &schema,
        &json!({"subject":"omega_lambda","num":26,"den":38,"value":"13/19"}),
    )
    .unwrap();
    // the real fact equals the reduced form — the corpus's proposition
    assert_eq!(
        reduced.cid, c.proposition.cid,
        "the reduced form is the honest fact"
    );
    assert_ne!(reduced.cid, unreduced.cid, "the two forms genuinely differ");

    let conflicts = cross_audit(&schema, &[reduced, unreduced]).unwrap();
    assert!(
        conflicts.iter().any(|conflict| matches!(
            conflict,
            CrossAuditConflict::UnderMerge { cids, .. } if cids.len() == 2
        )),
        "one witness resolving to two CIDs must surface as UnderMerge, got {conflicts:?}"
    );
}

/// 6. **Corroboration is NOT a false positive.** Two propositions with the SAME
/// identity (subject/num/den) and same witness → the SAME CID → `cross_audit` is
/// clean. This is the defect the strata split fixed (`grounds` removed from
/// proposition identity); asserting it holds *end to end* pins that the fix
/// doesn't regress and doesn't fight defense 5 above.
#[test]
fn corroboration_is_not_a_false_positive() {
    let c = omega_corpus();
    let schema = proposition_schema();

    // two independent tellings of the same value — same identity → same CID
    let via_planck = Quantum::seal(
        &schema,
        &json!({"subject":"omega_lambda","num":13,"den":19,"value":"13/19"}),
    )
    .unwrap();
    let via_wmap = Quantum::seal(
        &schema,
        &json!({"subject":"omega_lambda","num":13,"den":19,"value":"13/19"}),
    )
    .unwrap();
    assert_eq!(
        via_planck.cid, via_wmap.cid,
        "two tellings of one value → one proposition CID"
    );
    assert_eq!(
        via_planck.cid, c.proposition.cid,
        "and it is the corpus's honest fact"
    );

    let conflicts: Vec<CrossAuditConflict> = cross_audit(&schema, &[via_planck, via_wmap]).unwrap();
    assert!(
        conflicts.is_empty(),
        "corroboration must stay clean end-to-end (the strata split), got {conflicts:?}"
    );
}

// Compile-time guard that the fixture's vouch type is in scope (also exercised above).
const _: fn(&Vouch) -> bool = verify_vouch;
