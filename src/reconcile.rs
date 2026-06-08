//! Boundary↔bulk reconciliation — does a derived *exact* value agree with the
//! *measurement* it grounds in?
//!
//! The substrate grounds a derived fact (Ω_Λ = 13/19, exact) in a measurement
//! anchor (Planck 0.6847 ± 0.0073) *structurally* — but until now never checked
//! they **agree numerically**. That check is harmonics' own headline test: the
//! residual in σ (the famous 0.07σ for Ω_Λ). This module computes it and emits a
//! typed [`Agreement`] verdict — the verification edge of `STAR_SUBSTRATE.md`,
//! finally built.
//!
//! ## Precision bound (this is a *precision* gate)
//!
//! The comparison is between an exact rational and a *float* measurement (a
//! measurement is approximate by nature — that is what σ means), so the residual
//! is computed in f64. This is sound while the rational's own f64 error is far
//! below σ — i.e. for denominators up to ~10¹³ at σ ~ 10⁻³, which covers every
//! harmonics ratio with room to spare. An *exact* verdict (rational ∈ rational
//! band) would convert the float bounds to rationals — needing bignum (`Rat` is
//! i64). Until then: float residual, exact derived value, bound stated.

use serde_json::json;

use crate::generator::{input_value, Rat};
use crate::quantum::{Quantum, QuantumError};
use crate::schema::{FieldKind, Schema};

/// The agreement verdict between a derived value and a measurement, with the
/// residual `z` in units of σ.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Agreement {
    /// Within `consistent_sigma` — the derivation agrees with the measurement.
    Consistent { z: f64 },
    /// Between `consistent_sigma` and `falsify_sigma` — a mild disagreement worth
    /// surfacing (not yet a contradiction).
    Tension { z: f64 },
    /// Beyond `falsify_sigma` — the derivation contradicts the measurement.
    Falsified { z: f64 },
}

impl Agreement {
    pub fn z(&self) -> f64 {
        match self {
            Agreement::Consistent { z } | Agreement::Tension { z } | Agreement::Falsified { z } => *z,
        }
    }
    pub fn label(&self) -> &'static str {
        match self {
            Agreement::Consistent { .. } => "consistent",
            Agreement::Tension { .. } => "tension",
            Agreement::Falsified { .. } => "falsified",
        }
    }
    pub fn is_consistent(&self) -> bool {
        matches!(self, Agreement::Consistent { .. })
    }
    pub fn is_falsified(&self) -> bool {
        matches!(self, Agreement::Falsified { .. })
    }
}

/// The σ thresholds for the verdict. Defaults: consistent within 2σ, falsified
/// beyond 5σ (the particle-physics discovery/exclusion convention).
#[derive(Debug, Clone, Copy)]
pub struct Tolerance {
    pub consistent_sigma: f64,
    pub falsify_sigma: f64,
}

impl Default for Tolerance {
    fn default() -> Self {
        Tolerance { consistent_sigma: 2.0, falsify_sigma: 5.0 }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("derived {0} is not a value-bearing proposition (no num/den)")]
    NotValueBearing(String),
    #[error("attestation {0} has no measured `value`")]
    NoMeasurement(String),
    #[error("attestation {0} has no `uncertainty` (σ) — cannot reconcile")]
    NoUncertainty(String),
    #[error(transparent)]
    Quantum(#[from] QuantumError),
}

/// Reconcile a derived exact rational against a measured value ± σ. The residual
/// `z = |derived − measured| / σ` classifies the verdict. (`σ ≤ 0` ⇒ the band is a
/// point: consistent iff exactly equal, else falsified.)
pub fn reconcile(derived: Rat, measured: f64, sigma: f64, tol: &Tolerance) -> Agreement {
    let resid = (derived.value() - measured).abs();
    let z = if sigma > 0.0 {
        resid / sigma
    } else if resid == 0.0 {
        0.0
    } else {
        f64::INFINITY
    };
    if z <= tol.consistent_sigma {
        Agreement::Consistent { z }
    } else if z <= tol.falsify_sigma {
        Agreement::Tension { z }
    } else {
        Agreement::Falsified { z }
    }
}

/// Reconcile a derived **proposition quantum** against an **attestation quantum**:
/// the derived value is read from the proposition's own `num`/`den` (the #1 gate —
/// bound to identity), the measured `value` and `uncertainty` from the
/// attestation. Both grounding and agreement now travel together.
pub fn reconcile_quanta(
    derived: &Quantum,
    attestation: &Quantum,
    tol: &Tolerance,
) -> Result<Agreement, ReconcileError> {
    let value = input_value(derived).map_err(|_| ReconcileError::NotValueBearing(derived.cid.clone()))?;
    let measured = attestation
        .field("value")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| ReconcileError::NoMeasurement(attestation.cid.clone()))?;
    let sigma = attestation
        .field("uncertainty")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| ReconcileError::NoUncertainty(attestation.cid.clone()))?;
    Ok(reconcile(value, measured, sigma, tol))
}

/// The **verification edge** schema: identity `(derived, against)`, with the
/// `verdict` as a correctable field — re-reconciling the same pair (e.g. after a
/// new measurement) **supersedes**. `z_sigma` is informational projection.
pub fn verification_schema() -> Schema {
    Schema::new("verification", 1)
        .identity("derived", FieldKind::Cid)
        .identity("against", FieldKind::Cid)
        .required("verdict", FieldKind::String)
        .optional("z_sigma", FieldKind::String)
}

/// Seal a reconciliation as a verification quantum (the verdict, addressed by the
/// `(derived, against)` pair).
pub fn seal_verification(
    derived: &Quantum,
    attestation: &Quantum,
    agreement: &Agreement,
) -> Result<Quantum, QuantumError> {
    Quantum::seal(
        &verification_schema(),
        &json!({
            "derived": derived.cid,
            "against": attestation.cid,
            "verdict": agreement.label(),
            "z_sigma": format!("{:.4}", agreement.z()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{attestation_schema, proposition_schema, Closure};
    use serde_json::json;

    fn planck_omega() -> Quantum {
        Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                    "value":0.6847,"uncertainty":0.0073,"vouched_by":"nick"}),
        )
        .unwrap()
    }
    fn ratio(subject: &str, n: i64, d: i64) -> Quantum {
        Quantum::seal(
            &proposition_schema(),
            &json!({"subject":subject,"num":n,"den":d,"value":Rat::new(n,d).unwrap().reduced_string()}),
        )
        .unwrap()
    }

    #[test]
    fn omega_lambda_13_19_is_consistent_at_about_007_sigma() {
        // the framework's headline: 13/19 vs Planck Ω_Λ = 0.6847 ± 0.0073 ≈ 0.07σ
        let a = reconcile_quanta(&ratio("omega_lambda", 13, 19), &planck_omega(), &Tolerance::default()).unwrap();
        assert!(a.is_consistent(), "13/19 agrees with Planck: {a:?}");
        assert!(a.z() < 0.1, "the residual is ~0.07σ, got {}", a.z());
    }

    #[test]
    fn a_wrong_value_is_falsified() {
        // 1/2 = 0.5 vs 0.6847 ± 0.0073 ≈ 25σ
        let a = reconcile_quanta(&ratio("omega_lambda", 1, 2), &planck_omega(), &Tolerance::default()).unwrap();
        assert!(a.is_falsified(), "1/2 contradicts Planck: {a:?}");
        assert!(a.z() > 5.0);
    }

    #[test]
    fn a_near_miss_is_tension() {
        // 12/17 ≈ 0.7059 vs 0.6847 ± 0.0073 ≈ 2.9σ — between 2σ and 5σ
        let a = reconcile_quanta(&ratio("omega_lambda", 12, 17), &planck_omega(), &Tolerance::default()).unwrap();
        assert!(matches!(a, Agreement::Tension { .. }), "got {a:?}");
        assert!(a.z() > 2.0 && a.z() < 5.0);
    }

    #[test]
    fn missing_uncertainty_is_an_error() {
        let no_sigma = Quantum::seal(
            &attestation_schema(),
            &json!({"instrument":"x","dataset":"y","locator":"z","value":0.68,"vouched_by":"n"}),
        )
        .unwrap();
        assert!(matches!(
            reconcile_quanta(&ratio("x", 13, 19), &no_sigma, &Tolerance::default()),
            Err(ReconcileError::NoUncertainty(_))
        ));
    }

    #[test]
    fn verdict_seals_and_re_reconciliation_supersedes() {
        let derived = ratio("omega_lambda", 13, 19);
        let anchor = planck_omega();
        let a = reconcile_quanta(&derived, &anchor, &Tolerance::default()).unwrap();
        let v = seal_verification(&derived, &anchor, &a).unwrap();
        assert_eq!(v.field("verdict").and_then(|x| x.as_str()), Some("consistent"));

        // same (derived, against) → same identity, so a later re-reconciliation
        // (e.g. tighter σ → a different verdict) supersedes rather than coexists.
        let vs = verification_schema();
        let canon = crate::Canon::new(&vs);
        let id1 = canon.identity_projection(&json!({"derived":derived.cid,"against":anchor.cid,"verdict":"consistent"})).unwrap();
        let id2 = canon.identity_projection(&json!({"derived":derived.cid,"against":anchor.cid,"verdict":"tension"})).unwrap();
        assert_eq!(id1, id2, "the verdict is correctable — re-reconciling supersedes");
    }

    #[test]
    fn a_falsified_verdict_can_contest_the_derived_fact() {
        // policy demonstration: when reality falsifies a derived fact, the caller
        // contests it and it leaves the certain core.
        let derived = ratio("omega_lambda", 1, 2); // wrong
        let anchor = planck_omega();
        let mut cl = Closure::new();
        cl.add_anchor("planck_root");
        cl.add_rule(derived.cid.clone(), [String::from("planck_root")].into_iter().collect());
        assert!(cl.is_certain(&derived.cid), "structurally grounded → certain");

        let a = reconcile_quanta(&derived, &anchor, &Tolerance::default()).unwrap();
        if a.is_falsified() {
            cl.mark_contested(&derived.cid);
        }
        assert!(!cl.is_certain(&derived.cid), "a falsified fact is contested out of the certain core");
    }
}
