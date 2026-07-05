//! Signed attestations — non-repudiable vouches (Ed25519).
//!
//! Accuracy bottoms out at the boundary anchors (`attestation_schema`), where a
//! human or instrument vouches. A bare `vouched_by` name is repudiable and
//! unattributable. A **`Vouch`** signs the attestation's *CID* (which is its
//! content) with the voucher's secret key: only the key-holder could produce it,
//! and anyone can verify it against the public key. This is what turns accuracy
//! from *delegated* into *auditable* — you learn exactly **who** vouched, and they
//! cannot deny it. (Cryptography still cannot prove the claim *true*; it makes the
//! vouch non-repudiable. Accuracy auditable, never provable.)
//!
//! Keys are derived deterministically from a 32-byte seed (the secret), so the
//! reference impl needs no RNG and tests are reproducible.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// Derive a signing key from a 32-byte seed (the secret). The seed IS the secret;
/// guard it as you would a private key.
pub fn signing_key(seed: &[u8; 32]) -> SigningKey {
    SigningKey::from_bytes(seed)
}

/// A non-repudiable vouch: `signer` attests to `attestation_cid`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Vouch {
    /// The attestation CID being vouched for (its content address).
    pub attestation_cid: String,
    /// The voucher's public (verifying) key, hex.
    pub signer: String,
    /// Ed25519 signature over `attestation_cid`, hex.
    pub signature: String,
}

/// Produce a vouch: sign the attestation CID with `sk`.
pub fn vouch(sk: &SigningKey, attestation_cid: &str) -> Vouch {
    let sig: Signature = sk.sign(attestation_cid.as_bytes());
    Vouch {
        attestation_cid: attestation_cid.to_string(),
        signer: to_hex(sk.verifying_key().as_bytes()),
        signature: to_hex(&sig.to_bytes()),
    }
}

/// Verify a vouch: the signature checks out over the attestation CID under the
/// stated public key. A tampered CID, signature, or key fails. Returns `false`
/// (not `Err`) on any malformed field — the falsity *is* the finding.
pub fn verify_vouch(v: &Vouch) -> bool {
    let (Some(pk_bytes), Some(sig_bytes)) = (from_hex(&v.signer), from_hex(&v.signature)) else {
        return false;
    };
    let (Ok(pk_arr), Ok(sig_arr)) = (
        <[u8; 32]>::try_from(pk_bytes.as_slice()),
        <[u8; 64]>::try_from(sig_bytes.as_slice()),
    ) else {
        return false;
    };
    let Ok(pk) = VerifyingKey::from_bytes(&pk_arr) else {
        return false;
    };
    pk.verify(
        v.attestation_cid.as_bytes(),
        &Signature::from_bytes(&sig_arr),
    )
    .is_ok()
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vouch_verifies_and_is_tamper_evident() {
        let sk = signing_key(&[7u8; 32]);
        let v = vouch(&sk, "attestation-cid-abc");
        assert!(verify_vouch(&v), "a well-formed vouch verifies");

        // tamper the vouched CID → signature no longer matches
        let mut wrong_cid = v.clone();
        wrong_cid.attestation_cid = "attestation-cid-XYZ".into();
        assert!(!verify_vouch(&wrong_cid), "changing what was vouched fails");

        // tamper the signature
        let mut wrong_sig = v.clone();
        wrong_sig.signature.replace_range(0..2, "00");
        assert!(!verify_vouch(&wrong_sig), "a forged signature fails");
    }

    #[test]
    fn different_seeds_are_distinct_signers() {
        let a = vouch(&signing_key(&[1u8; 32]), "cid");
        let b = vouch(&signing_key(&[2u8; 32]), "cid");
        assert_ne!(
            a.signer, b.signer,
            "distinct secrets → distinct public keys"
        );
        // each verifies under its own key; you cannot impersonate by swapping keys
        let forged = Vouch {
            signer: a.signer.clone(),
            ..b.clone()
        };
        assert!(
            !verify_vouch(&forged),
            "b's signature does not verify under a's key"
        );
    }

    #[test]
    fn keygen_is_deterministic() {
        // same seed → same public key (reproducible, no RNG)
        assert_eq!(
            signing_key(&[42u8; 32]).verifying_key().as_bytes(),
            signing_key(&[42u8; 32]).verifying_key().as_bytes()
        );
    }
}
