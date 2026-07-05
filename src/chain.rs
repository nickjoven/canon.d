//! End-to-end chain verification: lab → serialized → regeneration → closure.
//!
//! Can we *cryptographically determine* that the chain from an (obscure) lab,
//! through the serialized layer, through regeneration, to the interpreted
//! consensus, maintains **consistency and accuracy**? The answer splits on the
//! two words — the same split as boundary vs bulk:
//!
//! - **Consistency** (internal faithfulness: nothing altered, every artifact is
//!   what it claims relative to its inputs) — **yes, cryptographically.** Every
//!   node is content-addressed; verifying the chain = re-hash every node
//!   ([`Quantum::verify`](crate::Quantum::verify)) + re-run every regeneration to
//!   the same CID ([`verify_regeneration`], the reproducible-build check) +
//!   re-derive the closure and compare one [`consensus_root`]. This is
//!   reputation-free: an obscure lab's chain verifies on the same footing as a
//!   famous one's — identity is by structure, not fame.
//! - **Accuracy** (correspondence to reality: the claims are *true*) — **no, not
//!   provable.** No hash certifies that 13/19 is the dark-energy fraction.
//!   Accuracy bottoms out at the boundary **anchors** (attestations); the most
//!   cryptography does is *bind* the chain to exactly which vouches it rests on
//!   and (with signed attestations — not yet built) make each vouch
//!   non-repudiable. Accuracy is made **auditable**, not **provable**: verify the
//!   chain mechanically, then judge the anchors.
//!
//! So the honest end-to-end statement a verifier can sign is: *"this consensus is
//! the faithful, reproducible regeneration of these sealed claims, which rest on
//! these anchors"* — consistency proven, accuracy delegated to the boundary.

use serde_json::Value;

use crate::closure::Closure;
use crate::crypto::{verify_vouch, Vouch};
use crate::generator::{project, EvalError, Memo};
use crate::log::TransparencyLog;
use crate::quantum::Quantum;

/// A single hash committing to the **entire certain core** — a Merkle-style root
/// over the closure's admitted set. Two parties agree on the consensus iff their
/// roots match (the closure is order-independent, so equal boundary + equal
/// justifications ⇒ equal root). Commits to **consistency**, not accuracy: it
/// says "exactly these facts are grounded," not "these facts are true."
pub fn consensus_root(closure: &Closure) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"canon.d/consensus-root/v1");
    // certain() is a BTreeSet — already sorted, so the root is canonical.
    for cid in closure.certain() {
        h.update(&(cid.len() as u64).to_le_bytes()); // length-prefix: no concat ambiguity
        h.update(cid.as_bytes());
    }
    h.finalize().to_hex().to_string()
}

/// Verify the **regeneration link**: re-run a generator's program against its
/// inputs and confirm it reproduces the expected proposition CID. This is the
/// reproducible-build check (Nix/Bazel) for a derived fact — the strongest link
/// in the chain, because regeneration is a deterministic function of the
/// content-addressed generator. A mismatch means the serialized fact is *not* the
/// faithful output of its claimed generator.
pub fn verify_regeneration(
    program_term: &Value,
    inputs: &[&Quantum],
    subject: &str,
    agent: &str,
    expected_proposition_cid: &str,
) -> Result<bool, EvalError> {
    let mut memo = Memo::new();
    let p = project(&mut memo, program_term, inputs, subject, agent)?;
    Ok(p.proposition.cid == expected_proposition_cid)
}

/// The accuracy-audit verdict for one anchor: *who* vouched (verified,
/// trusted signers) and whether the vouch is *logged* in a verified transparency
/// log. This is the boundary where accuracy is **audited** (not proven): an anchor
/// is auditably-accurate when a trusted party non-repudiably vouched for it and
/// that vouch is in an untampered log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorAudit {
    pub attestation_cid: String,
    /// Verified vouchers (public keys) whose signature over the CID checks out and
    /// who are in the trusted set.
    pub vouched_by: Vec<String>,
    /// Is at least one valid vouch recorded in the (verified) log?
    pub logged: bool,
}

impl AnchorAudit {
    /// Auditably accurate iff some trusted party vouched *and* it was logged.
    pub fn is_auditable(&self) -> bool {
        !self.vouched_by.is_empty() && self.logged
    }
}

/// Audit an anchor's accuracy provenance: collect the valid vouches from trusted
/// signers, and confirm at least one is recorded in a verified log. `trusted` is
/// the set of public keys (hex) you accept vouches from — your root of trust at
/// the boundary. The log must itself verify, else `logged` is false (a rewritten
/// log vouches for nothing).
pub fn audit_anchor(
    attestation_cid: &str,
    vouches: &[Vouch],
    trusted: &[String],
    log: &TransparencyLog,
) -> AnchorAudit {
    let log_ok = log.verify();
    let mut vouched_by = Vec::new();
    let mut logged = false;
    for v in vouches {
        if v.attestation_cid != attestation_cid || !verify_vouch(v) {
            continue;
        }
        if !trusted.iter().any(|t| t == &v.signer) {
            continue;
        }
        if !vouched_by.contains(&v.signer) {
            vouched_by.push(v.signer.clone());
        }
        // a vouch counts as logged when the log verifies and records it
        if log_ok && log.contains(&v.signature) {
            logged = true;
        }
    }
    AnchorAudit {
        attestation_cid: attestation_cid.to_string(),
        vouched_by,
        logged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{signing_key, vouch};
    use crate::generator::{generator_schema, seal_program};
    use crate::quantum::Quantum;
    use crate::{attestation_schema, proposition_schema};
    use serde_json::json;

    /// The full chain, cryptographically determined end-to-end.
    ///
    /// lab anchor (attestation) → serialized generator → regenerated proposition →
    /// grounded closure → consensus root. Every link is re-checked by hash, and a
    /// tamper at any node is detected.
    #[test]
    fn full_chain_is_cryptographically_consistent_and_tamper_evident() {
        // --- LAB / BOUNDARY: an anchor the lab vouches for (accuracy enters here) ---
        let anchor = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"harmonics-lab","dataset":"sync_cost","locator":"mediant 2/3,11/16",
                    "value":0.6842,"vouched_by":"nick"}),
        )
        .unwrap();
        assert!(
            anchor.verify(&attestation_schema()).unwrap(),
            "link 1: the anchor re-hashes"
        );

        // --- SERIALIZED: a generator (program + inputs), content-addressed ---
        let program = json!({"walk":"LRRLLLLL"});
        let (program_cid, _) = seal_program(&program);
        let generator = Quantum::seal(
            &generator_schema(),
            &json!({"program": program_cid, "inputs": [anchor.cid]}),
        )
        .unwrap();
        assert!(
            generator.verify(&generator_schema()).unwrap(),
            "link 2: the generator re-hashes"
        );

        // --- REGENERATION: project the fact, then VERIFY it reproduces ---
        let mut memo = Memo::new();
        let proj = project(&mut memo, &program, &[], "omega_lambda", "harmonics").unwrap();
        assert!(
            proj.proposition.verify(&proposition_schema()).unwrap(),
            "link 3: the fact re-hashes"
        );
        assert!(
            verify_regeneration(&program, &[], "omega_lambda", "harmonics", &proj.proposition.cid).unwrap(),
            "link 3: regeneration is reproducible (the fact is the faithful output of its generator)"
        );

        // --- CLOSURE / INTERPRETATION: ground the chain and root it ---
        // anchor (certain) → generator (grounded in anchor) → proposition (grounded in generator)
        let mut c = Closure::new();
        c.add_anchor(&anchor.cid);
        c.add_rule(
            generator.cid.clone(),
            [anchor.cid.clone()].into_iter().collect(),
        );
        c.add_rule(
            proj.proposition.cid.clone(),
            [generator.cid.clone()].into_iter().collect(),
        );
        assert!(
            c.is_certain(&proj.proposition.cid),
            "the regenerated fact is certain — grounded to the lab anchor"
        );

        let root = consensus_root(&c);
        // Rebuilding from the same boundary + justifications yields the SAME root.
        let mut c2 = Closure::new();
        c2.add_rule(
            proj.proposition.cid.clone(),
            [generator.cid.clone()].into_iter().collect(),
        ); // different order
        c2.add_rule(
            generator.cid.clone(),
            [anchor.cid.clone()].into_iter().collect(),
        );
        c2.add_anchor(&anchor.cid);
        assert_eq!(
            root,
            consensus_root(&c2),
            "the consensus root is order-independent — two verifiers agree by one hash"
        );

        // --- TAMPER EVIDENCE, two modes ---
        // (a) tamper IN PLACE — keep the CID, mutate the body. The stored bytes no
        //     longer hash to the address, so `verify` catches it.
        let mut in_place = proj.proposition.clone();
        in_place.body["num"] = json!(99);
        assert!(
            !in_place.verify(&proposition_schema()).unwrap(),
            "an in-place tamper fails its hash"
        );
        // (b) SUBSTITUTE — seal a different fact honestly. Its CID differs, so a
        //     chain grounding it has a different consensus root: two verifiers
        //     comparing one hash detect the substitution.
        let substituted = Quantum::seal(
            &proposition_schema(),
            &json!({"subject":"omega_lambda","num":99,"den":1,"value":"99/1"}),
        )
        .unwrap();
        assert_ne!(substituted.cid, proj.proposition.cid);
        let mut c3 = Closure::new();
        c3.add_anchor(&anchor.cid);
        c3.add_rule(
            generator.cid.clone(),
            [anchor.cid.clone()].into_iter().collect(),
        );
        c3.add_rule(
            substituted.cid.clone(),
            [generator.cid].into_iter().collect(),
        );
        assert_ne!(
            root,
            consensus_root(&c3),
            "a substituted fact moves the consensus root"
        );
    }

    #[test]
    fn anchor_accuracy_is_auditable_signed_and_logged() {
        // The pair: a non-repudiable vouch + a tamper-evident log upgrade an
        // anchor's accuracy from "delegated" to "auditable".
        let anchor = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                    "value":0.6847,"vouched_by":"nick"}),
        )
        .unwrap();

        // A trusted voucher signs the anchor's CID; the vouch is appended to the log.
        let sk = signing_key(&[9u8; 32]);
        let v = vouch(&sk, &anchor.cid);
        let trusted = vec![v.signer.clone()];
        let mut log = TransparencyLog::new();
        log.append(&anchor.cid);
        log.append(&v.signature);

        let audit = audit_anchor(&anchor.cid, &[v.clone()], &trusted, &log);
        assert!(audit.is_auditable(), "trusted + logged ⇒ auditable");
        assert_eq!(audit.vouched_by, vec![v.signer.clone()]);

        // An UNTRUSTED signer's vouch doesn't count, even if cryptographically valid.
        let stranger = vouch(&signing_key(&[1u8; 32]), &anchor.cid);
        let a2 = audit_anchor(&anchor.cid, &[stranger], &trusted, &log);
        assert!(
            !a2.is_auditable(),
            "a valid signature from outside the root of trust is not enough"
        );

        // A vouch that was never logged: trusted but not auditable (no record).
        let unlogged_log = TransparencyLog::new();
        let a3 = audit_anchor(&anchor.cid, &[v.clone()], &trusted, &unlogged_log);
        assert_eq!(
            a3.vouched_by,
            vec![v.signer.clone()],
            "the vouch is valid …"
        );
        assert!(!a3.logged, "… but unlogged → not auditable");
        assert!(!a3.is_auditable());

        // A vouch over a DIFFERENT anchor doesn't audit this one.
        let other = vouch(&sk, "some-other-cid");
        let a4 = audit_anchor(&anchor.cid, &[other], &trusted, &log);
        assert!(
            a4.vouched_by.is_empty(),
            "a vouch for another CID is irrelevant here"
        );
    }

    #[test]
    fn regeneration_mismatch_is_caught() {
        // Claiming a generator produced a fact it did not → verify_regeneration false.
        let program = json!({"walk":"LRRLLLLL"}); // = 13/19
        let wrong = Quantum::seal(
            &proposition_schema(),
            &json!({"subject":"omega_lambda","num":1,"den":2,"value":"1/2"}),
        )
        .unwrap();
        assert!(!verify_regeneration(&program, &[], "omega_lambda", "h", &wrong.cid).unwrap());
    }
}
