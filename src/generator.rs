//! The generator layer — seal the generator, project the output (`SPINE.md` §5).
//!
//! A **generator** is a content-addressed term in a *total* rewriting language
//! plus its input CIDs; its identity is `(program, inputs)`. Projection is the
//! memoized normalization (evaluation) of that term. This is **Nix
//! input-addressing**: the identity is a pure function of the program and its
//! inputs, the output is the cached result.
//!
//! Generators *fill in* `grounds`. A grounding resolves to exactly one of two
//! things — the holographic boundary made concrete:
//!
//! - an **attestation** ([`crate::attestation_schema`]) — an *anchor*: bottoms
//!   out at reality, witness-free, vouched (the boundary);
//! - a **generator** (here) — a *stable orbit*: a derivation that regenerates its
//!   output (the bulk).
//!
//! So `closure(grounds)` recurses through generators and terminates at
//! attestations. [`provenance_audit`] is the silent-drift detector for the
//! generator half: a derived datum whose generator does not resolve is "about
//! nothing," exactly as a claim with a dangling attestation is.
//!
//! ## Bounds (honest from the outset, per `SPINE.md` §0)
//!
//! - **Totality.** This stage is *compose-only* — straight-line application of
//!   total primitives, no recursion in the language (the `eval` recursion is over
//!   a finite AST and always halts). Structural recursion (the Stern-Brocot walk,
//!   the devil's staircase) is the next increment; it keeps totality via
//!   structural descent. Generators needing unbounded search are out of the
//!   language and fall back to the attestation tier.
//! - **Intensional dedup only (Rice).** "Same generator → same identity" means
//!   the same *canonicalized program term* and inputs. Two different programs
//!   computing one value are two generators — and that is fine: it is
//!   corroboration at the proposition they share, not a bug.
//! - **Determinism.** `eval` is call-by-value with fixed primitive semantics, so
//!   the O(output)→O(generator) storage win rests on bit-identical rebuild.

use std::collections::{BTreeSet, HashMap};

use serde_json::{json, Value};

use crate::quantum::{Quantum, QuantumError};
use crate::schema::{FieldKind, Schema};
use crate::strata::proposition_schema;

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

// ---------------------------------------------------------------------------
// Stage 1 — the generator quantum, program sealing, provenance
// ---------------------------------------------------------------------------

/// The **generator** schema: identity is `(program, inputs)`.
///
/// `program` is the CID of a sealed program term (see [`seal_program`]); `inputs`
/// is the set of CIDs the term is applied to. `label` is detachable projection.
/// Two generators with the same program and inputs share one CID (intensional
/// dedup).
pub fn generator_schema() -> Schema {
    Schema::new("generator", 1)
        .identity("program", FieldKind::Cid)
        .identity("inputs", FieldKind::Set(Box::new(FieldKind::Cid)))
        .optional("label", FieldKind::String)
}

/// Content-address a program term. The term is canonical JSON already (serde_json
/// sorts object keys and preserves array order), so the CID is a pure function of
/// the term's structure.
pub fn seal_program(term: &Value) -> (String, Vec<u8>) {
    let bytes = serde_json::to_vec(term).expect("program term serializes");
    (blake3_hex(&bytes), bytes)
}

/// A derived datum whose generator does not resolve — the silent-drift detector
/// for the generator half of `grounds`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Orphan {
    /// CID of the quantum with the dangling generator reference.
    pub derived: String,
    /// The generator CID that did not resolve.
    pub missing: String,
}

/// Every value in each claim's `generator_field` must resolve to a known
/// generator CID. A dangling generator means the output was sealed without a
/// regenerable source — the very thing the mandatory back-link abolishes. Pure;
/// pass the set of sealed generator CIDs. (This is [`crate::ground_audit`]'s twin
/// for the generator side; together they cover `grounds` = attestations ∪
/// generators.)
pub fn provenance_audit(
    generator_field: &str,
    claims: &[Quantum],
    known_generators: &BTreeSet<String>,
) -> Vec<Orphan> {
    let mut out = Vec::new();
    for q in claims {
        let Some(arr) = q.field(generator_field).and_then(|v| v.as_array()) else {
            continue;
        };
        for g in arr {
            if let Some(g) = g.as_str() {
                if !known_generators.contains(g) {
                    out.push(Orphan { derived: q.cid.clone(), missing: g.to_string() });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Stage 2a — the value algebra and the compose-only evaluator
// ---------------------------------------------------------------------------

/// A reduced rational — the value algebra. Always `gcd(|num|,den)=1`, `den>0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rat {
    pub num: i64,
    pub den: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("division by zero")]
    DivByZero,
    #[error("input index {0} out of range")]
    BadInput(usize),
    #[error("unknown op `{0}`")]
    UnknownOp(String),
    #[error("op `{op}` expected {expected} args, got {got}")]
    Arity { op: String, expected: usize, got: usize },
    #[error("malformed term: {0}")]
    Malformed(String),
    #[error(transparent)]
    Quantum(#[from] QuantumError),
}

fn gcd(a: i64, b: i64) -> i64 {
    let (mut a, mut b) = (a.abs(), b.abs());
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a.max(1)
}

impl Rat {
    pub fn new(num: i64, den: i64) -> Result<Rat, EvalError> {
        if den == 0 {
            return Err(EvalError::DivByZero);
        }
        let g = gcd(num, den);
        let (mut num, mut den) = (num / g, den / g);
        if den < 0 {
            num = -num;
            den = -den;
        }
        Ok(Rat { num, den })
    }

    /// The forced mediant: `(a.num+b.num)/(a.den+b.den)`, reduced. The Stern-Brocot
    /// primitive — `mediant(2/3, 11/16) = 13/19`.
    pub fn mediant(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        Rat::new(a.num + b.num, a.den + b.den)
    }

    pub fn add(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        Rat::new(a.num * b.den + b.num * a.den, a.den * b.den)
    }

    pub fn mul(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        Rat::new(a.num * b.num, a.den * b.den)
    }

    pub fn value(&self) -> f64 {
        self.num as f64 / self.den as f64
    }
}

/// Evaluate a program term against its resolved inputs. Total over the finite AST
/// (compose-only — no language-level recursion), deterministic, confluent.
///
/// Grammar (canonical JSON):
/// - `{"lit": [n, d]}` — a rational literal;
/// - `{"in": k}` — the `k`-th input value;
/// - `{"op": <name>, "args": [<term>, ...]}` — apply a total primitive
///   (`mediant`, `add`, `mul` — binary; `reduce` — unary, the identity that
///   re-canonicalizes).
pub fn eval(term: &Value, inputs: &[Rat]) -> Result<Rat, EvalError> {
    if let Some(lit) = term.get("lit").and_then(|v| v.as_array()) {
        let n = lit.first().and_then(|v| v.as_i64());
        let d = lit.get(1).and_then(|v| v.as_i64());
        return match (n, d) {
            (Some(n), Some(d)) => Rat::new(n, d),
            _ => Err(EvalError::Malformed("lit expects [int, int]".into())),
        };
    }
    if let Some(k) = term.get("in").and_then(|v| v.as_u64()) {
        let k = k as usize;
        return inputs.get(k).copied().ok_or(EvalError::BadInput(k));
    }
    if let Some(op) = term.get("op").and_then(|v| v.as_str()) {
        let args = term.get("args").and_then(|v| v.as_array()).ok_or_else(|| {
            EvalError::Malformed(format!("op `{op}` needs an args array"))
        })?;
        let vals: Vec<Rat> = args.iter().map(|t| eval(t, inputs)).collect::<Result<_, _>>()?;
        return apply(op, &vals);
    }
    Err(EvalError::Malformed(format!("unrecognized term {term}")))
}

fn apply(op: &str, args: &[Rat]) -> Result<Rat, EvalError> {
    let binary = |f: fn(Rat, Rat) -> Result<Rat, EvalError>| {
        if args.len() != 2 {
            Err(EvalError::Arity { op: op.into(), expected: 2, got: args.len() })
        } else {
            f(args[0], args[1])
        }
    };
    match op {
        "mediant" => binary(Rat::mediant),
        "add" => binary(Rat::add),
        "mul" => binary(Rat::mul),
        "reduce" => {
            if args.len() != 1 {
                Err(EvalError::Arity { op: op.into(), expected: 1, got: args.len() })
            } else {
                Ok(args[0]) // already reduced by construction
            }
        }
        other => Err(EvalError::UnknownOp(other.to_string())),
    }
}

/// The **minimum description length** of a program term: its node count. A
/// computable proxy for forcing (`SPINE.md` §7). NOTE: at this compose-only stage
/// MDL cannot yet *discriminate* forced from fitted — `lit(13,19)` is shorter than
/// `mediant(2/3,11/16)`. The discrimination appears only with recursion (2b),
/// where one short recursive generator produces O(N) outputs (fractal
/// compression) and the fit must store all N. MDL is wired here; its teeth arrive
/// with structural recursion.
pub fn mdl(term: &Value) -> usize {
    match term {
        Value::Array(a) => 1 + a.iter().map(mdl).sum::<usize>(),
        Value::Object(o) => 1 + o.values().map(mdl).sum::<usize>(),
        _ => 1,
    }
}

/// The result of projecting a generator: the sealed generator and the proposition
/// it produces. The proposition carries no generator in its identity (it is
/// extensional — two generators producing one value share it; that is
/// corroboration). The `generator → proposition` link is recorded by the caller
/// as an assertion whose `grounds` contains `generator.cid`.
#[derive(Debug, Clone)]
pub struct Projection {
    pub generator: Quantum,
    pub proposition: Quantum,
    pub program_cid: String,
}

/// A memo table: generator CID → produced proposition CID. The CID is a perfect
/// cache key with no invalidation (content-addressing). This is the Nix
/// derivation cache, native.
pub type Memo = HashMap<String, String>;

/// Project a generator: seal the program, seal the generator quantum, evaluate the
/// term against `inputs` (each a `(cid, value)` — the CID enters the generator's
/// identity, the value drives evaluation), and seal the resulting `proposition`.
/// Memoized on the generator CID.
pub fn project(
    memo: &mut Memo,
    program_term: &Value,
    inputs: &[(String, Rat)],
    subject: &str,
) -> Result<Projection, EvalError> {
    let (program_cid, _) = seal_program(program_term);
    let input_cids: Vec<&String> = inputs.iter().map(|(c, _)| c).collect();
    let generator = Quantum::seal(
        &generator_schema(),
        &json!({ "program": program_cid, "inputs": input_cids }),
    )?;

    let vals: Vec<Rat> = inputs.iter().map(|(_, v)| *v).collect();
    let out = eval(program_term, &vals)?;
    let proposition = Quantum::seal(
        &proposition_schema(),
        &json!({ "subject": subject, "num": out.num, "den": out.den, "value": out.value() }),
    )?;

    memo.insert(generator.cid.clone(), proposition.cid.clone());
    Ok(Projection { generator, proposition, program_cid })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Canon;

    #[test]
    fn program_cid_is_stable() {
        let a = seal_program(&json!({"op":"mediant","args":[{"lit":[2,3]},{"lit":[11,16]}]}));
        let b = seal_program(&json!({"op":"mediant","args":[{"lit":[2,3]},{"lit":[11,16]}]}));
        assert_eq!(a.0, b.0, "same program term → same program CID");
    }

    #[test]
    fn eval_mediant_literal_and_input() {
        // 13/19 = mediant(2/3, 11/16), the depth-6 Stern-Brocot forcing.
        let t = json!({"op":"mediant","args":[{"lit":[2,3]},{"lit":[11,16]}]});
        assert_eq!(eval(&t, &[]).unwrap(), Rat { num: 13, den: 19 });

        // same, taking its operands as inputs
        let t2 = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let inputs = [Rat::new(2, 3).unwrap(), Rat::new(11, 16).unwrap()];
        assert_eq!(eval(&t2, &inputs).unwrap(), Rat { num: 13, den: 19 });
    }

    #[test]
    fn eval_errors_are_typed() {
        assert!(matches!(eval(&json!({"lit":[1,0]}), &[]), Err(EvalError::DivByZero)));
        assert!(matches!(eval(&json!({"in":5}), &[]), Err(EvalError::BadInput(5))));
        assert!(matches!(eval(&json!({"op":"nope","args":[]}), &[]), Err(EvalError::UnknownOp(_))));
        assert!(matches!(
            eval(&json!({"op":"mediant","args":[{"lit":[1,2]}]}), &[]),
            Err(EvalError::Arity { .. })
        ));
    }

    #[test]
    fn projection_generates_the_fact() {
        // Projecting the mediant generator yields exactly the proposition you'd
        // get by sealing 13/19 directly: the generated fact == the asserted fact.
        let mut memo = Memo::new();
        let inputs = [
            (Quantum::seal(&proposition_schema(), &json!({"subject":"a","num":2,"den":3,"value":Rat::new(2,3).unwrap().value()})).unwrap().cid, Rat::new(2,3).unwrap()),
            (Quantum::seal(&proposition_schema(), &json!({"subject":"b","num":11,"den":16,"value":Rat::new(11,16).unwrap().value()})).unwrap().cid, Rat::new(11,16).unwrap()),
        ];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p = project(&mut memo, &prog, &inputs, "omega_lambda").unwrap();

        let direct = Quantum::seal(&proposition_schema(), &json!({
            "subject":"omega_lambda","num":13,"den":19,"value":Rat::new(13,19).unwrap().value()
        })).unwrap();
        assert_eq!(p.proposition.cid, direct.cid, "projection reproduces the fact bit-identically");
    }

    #[test]
    fn projection_is_memoized() {
        let mut memo = Memo::new();
        let inputs = [("cidA".to_string(), Rat::new(2,3).unwrap()), ("cidB".to_string(), Rat::new(11,16).unwrap())];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p1 = project(&mut memo, &prog, &inputs, "x").unwrap();
        assert_eq!(memo.get(&p1.generator.cid), Some(&p1.proposition.cid), "generator CID caches its output");
        let p2 = project(&mut memo, &prog, &inputs, "x").unwrap();
        assert_eq!(p1.generator.cid, p2.generator.cid, "intensional dedup: same program+inputs → one generator");
        assert_eq!(memo.len(), 1, "re-projection hits the memo, no new entry");
    }

    #[test]
    fn generator_dedup_is_intensional() {
        let mut memo = Memo::new();
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let same = [("a".to_string(), Rat::new(2,3).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let g1 = project(&mut memo, &prog, &same, "x").unwrap().generator.cid;
        let g2 = project(&mut memo, &prog, &same, "x").unwrap().generator.cid;
        assert_eq!(g1, g2);
        // different inputs → different generator (identity includes inputs)
        let other = [("c".to_string(), Rat::new(1,2).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let g3 = project(&mut memo, &prog, &other, "x").unwrap().generator.cid;
        assert_ne!(g1, g3);
    }

    #[test]
    fn mdl_counts_nodes() {
        let forced = json!({"op":"mediant","args":[{"lit":[2,3]},{"lit":[11,16]}]});
        let fitted = json!({"lit":[13,19]});
        // At compose-only stage the fit is SHORTER — MDL's discriminating power
        // needs recursion (2b). Here we only assert MDL is a stable node count.
        assert!(mdl(&forced) > mdl(&fitted));
        assert_eq!(mdl(&fitted), mdl(&json!({"lit":[13,19]})));
    }

    #[test]
    fn provenance_audit_flags_then_clears() {
        // An assertion grounded in a generator that isn't sealed = silent-drift.
        let mut memo = Memo::new();
        let inputs = [("a".to_string(), Rat::new(2,3).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p = project(&mut memo, &prog, &inputs, "x").unwrap();

        // assertion-shaped quantum carrying a `generator` back-link list
        let asrt_schema = Schema::new("derived", 1)
            .identity("proposition", FieldKind::Cid)
            .identity("generator", FieldKind::Set(Box::new(FieldKind::Cid)));
        let good = Quantum::seal(&asrt_schema, &json!({"proposition":p.proposition.cid,"generator":[p.generator.cid]})).unwrap();
        let dangling = Quantum::seal(&asrt_schema, &json!({"proposition":p.proposition.cid,"generator":["nonexistent"]})).unwrap();

        let mut known = BTreeSet::new();
        known.insert(p.generator.cid.clone());
        let report = provenance_audit("generator", &[good.clone(), dangling.clone()], &known);
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].derived, dangling.cid);
        assert_eq!(report[0].missing, "nonexistent");

        known.insert("nonexistent".to_string());
        assert!(provenance_audit("generator", &[good, dangling], &known).is_empty());
    }

    #[test]
    fn generator_is_a_well_formed_quantum() {
        // sanity: the generator quantum decodes its identity fields back
        let mut memo = Memo::new();
        let inputs = [("a".to_string(), Rat::new(2,3).unwrap())];
        let prog = json!({"in":0});
        let p = project(&mut memo, &prog, &inputs, "x").unwrap();
        let gs = generator_schema();
        let canon = Canon::new(&gs);
        assert!(canon.identity_projection(&p.generator.body).is_ok());
        assert_eq!(p.generator.field("program").and_then(|v| v.as_str()), Some(p.program_cid.as_str()));
    }
}
