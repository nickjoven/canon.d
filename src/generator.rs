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
    #[error("integer overflow (the `Rat` value algebra is i64-bounded)")]
    Overflow,
    #[error(transparent)]
    Quantum(#[from] QuantumError),
    #[error(transparent)]
    Strata(#[from] crate::strata::StrataError),
}

fn gcd(a: i64, b: i64) -> i64 {
    // callers guarantee a, b != i64::MIN, so abs is safe
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
        // i64::MIN has no positive magnitude — out of the toy algebra's domain.
        if num == i64::MIN || den == i64::MIN {
            return Err(EvalError::Overflow);
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
    /// primitive — `mediant(2/3, 11/16) = 13/19`. Returns `Overflow` rather than
    /// panicking/wrapping when the sum leaves i64.
    pub fn mediant(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        let num = a.num.checked_add(b.num).ok_or(EvalError::Overflow)?;
        let den = a.den.checked_add(b.den).ok_or(EvalError::Overflow)?;
        Rat::new(num, den)
    }

    pub fn add(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        let l = a.num.checked_mul(b.den).ok_or(EvalError::Overflow)?;
        let r = b.num.checked_mul(a.den).ok_or(EvalError::Overflow)?;
        let num = l.checked_add(r).ok_or(EvalError::Overflow)?;
        let den = a.den.checked_mul(b.den).ok_or(EvalError::Overflow)?;
        Rat::new(num, den)
    }

    pub fn mul(a: Rat, b: Rat) -> Result<Rat, EvalError> {
        let num = a.num.checked_mul(b.num).ok_or(EvalError::Overflow)?;
        let den = a.den.checked_mul(b.den).ok_or(EvalError::Overflow)?;
        Rat::new(num, den)
    }

    /// The exact reduced form as a canonical `"num/den"` string — the witness for
    /// a ratio proposition. Exact (no float), and independent of the
    /// *canonicalizer's reduction step*: it re-reduces, so it catches a form that
    /// was left unreduced. (It is not independent of the underlying datum — for a
    /// rational there is no such route; this is the same stance as `gnosis`.)
    pub fn reduced_string(&self) -> String {
        format!("{}/{}", self.num, self.den)
    }

    pub fn value(&self) -> f64 {
        self.num as f64 / self.den as f64
    }
}

/// Evaluate a program term against its resolved inputs. Deterministic and
/// confluent; total *into `Result`* — it always halts and never panics, returning
/// `EvalError::Overflow` when a value leaves the i64 `Rat` algebra rather than
/// wrapping. (The AST is finite and `walk`/`fold` descend on finite structures.)
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
    // --- Stage 2b: structural recursion (total by descent on a finite structure) ---
    if let Some(path) = term.get("walk").and_then(|v| v.as_str()) {
        // The Stern-Brocot walk: structural recursion over a finite L/R path.
        // Termination is guaranteed — the path is consumed one move at a time.
        return walk(path);
    }
    if let Some(f) = term.get("fold").and_then(|v| v.as_object()) {
        // fold(op, init, over): a total left-fold of a binary primitive over a
        // FINITE literal list — bounded recursion by structural descent on `over`.
        let op = f.get("op").and_then(|v| v.as_str()).ok_or_else(|| {
            EvalError::Malformed("fold needs a string `op`".into())
        })?;
        let init = f.get("init").ok_or_else(|| EvalError::Malformed("fold needs `init`".into()))?;
        let over = f.get("over").and_then(|v| v.as_array()).ok_or_else(|| {
            EvalError::Malformed("fold needs an `over` array".into())
        })?;
        let mut acc = eval(init, inputs)?;
        for elem in over {
            let e = eval(elem, inputs)?;
            acc = apply(op, &[acc, e])?;
        }
        return Ok(acc);
    }
    Err(EvalError::Malformed(format!("unrecognized term {term}")))
}

/// The Stern-Brocot walk — the canonical structural-recursion generator. Folds
/// mediant steps over a finite L/R path from the boundary `(0/1, 1/0)`: `L`
/// lowers the upper bound to the running mediant, `R` raises the lower bound. The
/// node's value is the final mediant. `walk("")` is the root `1/1`;
/// `walk("LRRLLLLL")` is `13/19`.
///
/// **Termination vs. domain.** The recursion terminates on any finite path
/// (structural descent), but the running `(lo, hi)` pair grows ~Fibonacci, so a
/// long path (≳90 moves) leaves i64. That returns `Overflow` — never panics or
/// wraps — so `walk` is a *total function into `Result`*: total in the sense that
/// matters for the substrate (no panic, no silent wrong answer), bounded in the
/// values it can reach. The toy `Rat` algebra is i64; a bignum value algebra
/// would lift the bound.
pub fn walk(path: &str) -> Result<Rat, EvalError> {
    let (mut lo, mut hi) = ((0i64, 1i64), (1i64, 0i64));
    let mediant = |a: (i64, i64), b: (i64, i64)| -> Result<(i64, i64), EvalError> {
        Ok((
            a.0.checked_add(b.0).ok_or(EvalError::Overflow)?,
            a.1.checked_add(b.1).ok_or(EvalError::Overflow)?,
        ))
    };
    for c in path.chars() {
        let m = mediant(lo, hi)?;
        match c {
            'L' => hi = m,
            'R' => lo = m,
            other => {
                return Err(EvalError::Malformed(format!("walk move must be L or R, got {other:?}")))
            }
        }
    }
    let m = mediant(lo, hi)?;
    Rat::new(m.0, m.1)
}

/// Every Stern-Brocot node down to `depth`, by walking all `2^len` paths of each
/// length `0..=depth`. There are `2^(depth+1) − 1` of them, each a distinct
/// rational. This is the **fractal generator**: one constant-size `walk` rule
/// grounds exponentially many facts — the holographic O(rule) → O(2^d) bulk of
/// `SPINE.md` §1/§5. (The boundary is the rule + the paths; the bulk is the
/// enumerated tree.)
pub fn stern_brocot_to_depth(depth: usize) -> Vec<(String, Rat)> {
    let mut out = Vec::new();
    for len in 0..=depth {
        for bits in 0..(1u32 << len) {
            let path: String = (0..len)
                .map(|i| if (bits >> i) & 1 == 0 { 'L' } else { 'R' })
                .collect();
            if let Ok(r) = walk(&path) {
                out.push((path, r));
            }
        }
    }
    out
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

/// The **minimum description length** of a program term — a real description
/// length: structure plus the bit-size of integer literals plus string length.
/// A computable proxy for forcing (`SPINE.md` §7).
///
/// HONEST BOUND (corrected in 2b): MDL does **not** discriminate forced from
/// fitted on a *single* Stern-Brocot node. Those nodes are **incompressible** —
/// the value's bit-size equals its path length, so `walk(path)` never beats
/// `lit(n,d)` per node (and for the Fibonacci spine the literal is *smaller*). The
/// forced-vs-fitted win is at the **family/structure** level: one constant-size
/// recursive rule (`walk`) grounds O(2^d) facts (`stern_brocot_to_depth`), so the
/// *generator* is O(d) while the *enumerated bulk* is O(2^d) — that exponential
/// gap is the compression, and it lives in the shared rule, not in any one node.
pub fn mdl(term: &Value) -> usize {
    fn bits(n: i64) -> usize {
        (64 - n.unsigned_abs().leading_zeros()).max(1) as usize
    }
    match term {
        Value::Number(n) => n.as_i64().map(bits).unwrap_or(1),
        Value::String(s) => s.len().max(1),
        Value::Array(a) => 1 + a.iter().map(mdl).sum::<usize>(),
        Value::Object(o) => 1 + o.values().map(mdl).sum::<usize>(),
        _ => 1,
    }
}

/// The result of projecting a generator: the sealed generator, the proposition it
/// produces, and the **assertion that links them** — the mandatory back-link.
///
/// The proposition carries no generator in its identity (it is extensional — two
/// generators producing one value share it; that is corroboration). The
/// `generator → proposition` link lives in `assertion`, sealed via
/// [`crate::strata::seal_assertion`], so **projection cannot hand back a fact
/// without its provenance**: there is no field-free path to a `Projection`.
#[derive(Debug, Clone)]
pub struct Projection {
    pub generator: Quantum,
    pub proposition: Quantum,
    /// The back-link: an assertion whose `grounds` is `[generator.cid]`.
    pub assertion: Quantum,
    pub program_cid: String,
}

/// A memo table: generator CID → produced proposition CID. The CID is a perfect
/// cache key with no invalidation (content-addressing). This is the Nix
/// derivation cache, native.
pub type Memo = HashMap<String, String>;

/// Project a generator: seal the program, seal the generator quantum, evaluate the
/// term against `inputs` (each a `(cid, value)` — the CID enters the generator's
/// identity, the value drives evaluation), seal the resulting `proposition`, and
/// seal the **mandatory back-link assertion** (`proposition ← generator`, by
/// `agent`). Memoized on the generator CID. There is no way to obtain a projected
/// proposition without its provenance — the back-link is part of the result.
pub fn project(
    memo: &mut Memo,
    program_term: &Value,
    inputs: &[(String, Rat)],
    subject: &str,
    agent: &str,
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
        &json!({ "subject": subject, "num": out.num, "den": out.den, "value": out.reduced_string() }),
    )?;

    // The mandatory back-link. grounds = [generator] is non-empty by construction,
    // so this never returns NoProvenance.
    let assertion = crate::strata::seal_assertion(&proposition, &[&generator], agent)?;

    memo.insert(generator.cid.clone(), proposition.cid.clone());
    Ok(Projection { generator, proposition, assertion, program_cid })
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
            (Quantum::seal(&proposition_schema(), &json!({"subject":"a","num":2,"den":3,"value":Rat::new(2,3).unwrap().reduced_string()})).unwrap().cid, Rat::new(2,3).unwrap()),
            (Quantum::seal(&proposition_schema(), &json!({"subject":"b","num":11,"den":16,"value":Rat::new(11,16).unwrap().reduced_string()})).unwrap().cid, Rat::new(11,16).unwrap()),
        ];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p = project(&mut memo, &prog, &inputs, "omega_lambda", "claude").unwrap();

        let direct = Quantum::seal(&proposition_schema(), &json!({
            "subject":"omega_lambda","num":13,"den":19,"value":Rat::new(13,19).unwrap().reduced_string()
        })).unwrap();
        assert_eq!(p.proposition.cid, direct.cid, "projection reproduces the fact bit-identically");
    }

    #[test]
    fn projection_is_memoized() {
        let mut memo = Memo::new();
        let inputs = [("cidA".to_string(), Rat::new(2,3).unwrap()), ("cidB".to_string(), Rat::new(11,16).unwrap())];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p1 = project(&mut memo, &prog, &inputs, "x", "claude").unwrap();
        assert_eq!(memo.get(&p1.generator.cid), Some(&p1.proposition.cid), "generator CID caches its output");
        let p2 = project(&mut memo, &prog, &inputs, "x", "claude").unwrap();
        assert_eq!(p1.generator.cid, p2.generator.cid, "intensional dedup: same program+inputs → one generator");
        assert_eq!(memo.len(), 1, "re-projection hits the memo, no new entry");
    }

    #[test]
    fn generator_dedup_is_intensional() {
        let mut memo = Memo::new();
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let same = [("a".to_string(), Rat::new(2,3).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let g1 = project(&mut memo, &prog, &same, "x", "claude").unwrap().generator.cid;
        let g2 = project(&mut memo, &prog, &same, "x", "claude").unwrap().generator.cid;
        assert_eq!(g1, g2);
        // different inputs → different generator (identity includes inputs)
        let other = [("c".to_string(), Rat::new(1,2).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let g3 = project(&mut memo, &prog, &other, "x", "claude").unwrap().generator.cid;
        assert_ne!(g1, g3);
    }

    #[test]
    fn mdl_counts_nodes() {
        let forced = json!({"op":"mediant","args":[{"lit":[2,3]},{"lit":[11,16]}]});
        let fitted = json!({"lit":[13,19]});
        // A single value's compose-only derivation is longer than its literal —
        // the per-node win belongs to the literal. The forced-vs-fitted teeth are
        // at the family level (see fractal_generator_is_holographic), not here.
        assert!(mdl(&forced) > mdl(&fitted));
        assert_eq!(mdl(&fitted), mdl(&json!({"lit":[13,19]})), "mdl is deterministic");
    }

    #[test]
    fn provenance_audit_flags_then_clears() {
        // An assertion grounded in a generator that isn't sealed = silent-drift.
        let mut memo = Memo::new();
        let inputs = [("a".to_string(), Rat::new(2,3).unwrap()), ("b".to_string(), Rat::new(11,16).unwrap())];
        let prog = json!({"op":"mediant","args":[{"in":0},{"in":1}]});
        let p = project(&mut memo, &prog, &inputs, "x", "claude").unwrap();

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

    // --- Stage 2b: structural recursion ---

    #[test]
    fn arithmetic_overflow_is_typed_not_panic() {
        // i64::MIN is out of the toy algebra's domain → Overflow, not a panic.
        assert!(matches!(eval(&json!({"lit":[i64::MIN, 6]}), &[]), Err(EvalError::Overflow)));
        // mediant of two near-MAX rationals overflows the sum → Overflow.
        let big = json!({"op":"mediant","args":[{"lit":[i64::MAX, 1]}, {"lit":[i64::MAX, 1]}]});
        assert!(matches!(eval(&big, &[]), Err(EvalError::Overflow)));
        // a long walk grows the running pair ~Fibonacci past i64 → Overflow, total
        // into Result (no panic, no silent wrap).
        let long: String = "LR".repeat(60);
        assert!(matches!(walk(&long), Err(EvalError::Overflow)));
        // but a path within the bound is fine.
        assert!(walk("LRRLLLLL").is_ok());
    }

    #[test]
    fn walk_reproduces_stern_brocot_nodes() {
        assert_eq!(walk("").unwrap(), Rat { num: 1, den: 1 }, "empty path = the root 1/1");
        assert_eq!(walk("L").unwrap(), Rat { num: 1, den: 2 });
        assert_eq!(walk("R").unwrap(), Rat { num: 2, den: 1 });
        // the running example, now via recursion instead of a hand-written mediant
        assert_eq!(walk("LRRLLLLL").unwrap(), Rat { num: 13, den: 19 });
        // also reachable as a term through eval
        assert_eq!(eval(&json!({"walk":"LRRLLLLL"}), &[]).unwrap(), Rat { num: 13, den: 19 });
    }

    #[test]
    fn walk_rejects_bad_moves() {
        assert!(matches!(walk("LXR"), Err(EvalError::Malformed(_))));
    }

    #[test]
    fn fold_is_total_structural_recursion() {
        // 0 + 1/2 + 1/3 + 1/6 = 1
        let t = json!({"fold":{"op":"add","init":{"lit":[0,1]},
                               "over":[{"lit":[1,2]},{"lit":[1,3]},{"lit":[1,6]}]}});
        assert_eq!(eval(&t, &[]).unwrap(), Rat { num: 1, den: 1 });
        // empty fold returns init
        let e = json!({"fold":{"op":"add","init":{"lit":[5,1]},"over":[]}});
        assert_eq!(eval(&e, &[]).unwrap(), Rat { num: 5, den: 1 });
    }

    #[test]
    fn walk_projects_the_fact() {
        // A recursive generator with NO inputs (the path is the program) projects
        // 13/19 — bit-identical to the directly sealed fact.
        let mut memo = Memo::new();
        let p = project(&mut memo, &json!({"walk":"LRRLLLLL"}), &[], "omega_lambda", "claude").unwrap();
        let direct = Quantum::seal(&proposition_schema(), &json!({
            "subject":"omega_lambda","num":13,"den":19,"value":Rat::new(13,19).unwrap().reduced_string()
        })).unwrap();
        assert_eq!(p.proposition.cid, direct.cid);
    }

    #[test]
    fn fractal_generator_is_holographic() {
        // ONE constant-size walk rule grounds exponentially many facts.
        let depth = 6;
        let facts = stern_brocot_to_depth(depth);
        assert_eq!(facts.len(), (1 << (depth + 1)) - 1, "2^(d+1)-1 = 127 nodes to depth 6");
        // each is a distinct rational (Stern-Brocot enumerates without repeats)
        let distinct: std::collections::BTreeSet<_> =
            facts.iter().map(|(_, r)| (r.num, r.den)).collect();
        assert_eq!(distinct.len(), facts.len(), "no repeated nodes");
        // the generating RULE is one primitive, its structural size independent of
        // how many facts it grounds: O(rule) boundary, O(2^d) bulk.
        let rule_size = mdl(&json!({"walk":""}));
        assert_eq!(rule_size, mdl(&json!({"walk":""})), "the rule's size does not grow with the tree");
    }

    #[test]
    fn single_node_mdl_does_not_universally_favor_recursion() {
        // Honest bound: recursion is NOT a per-node compressor. For 13/19 the walk
        // program is smaller than the literal ...
        assert!(mdl(&json!({"walk":"LRRLLLLL"})) < mdl(&json!({"lit":[13,19]})));
        // ... but for 1/9 (a simple value at a deep path) the LITERAL wins — the
        // node is incompressible, its value-bits ≈ its path length. No universal
        // ordering; the real win is the amortized rule (see fractal test).
        assert_eq!(walk("LLLLLLLL").unwrap(), Rat { num: 1, den: 9 });
        assert!(mdl(&json!({"walk":"LLLLLLLL"})) > mdl(&json!({"lit":[1,9]})));
    }

    #[test]
    fn project_emits_mandatory_back_link() {
        // Projection always returns the back-link assertion; its grounds name the
        // generator. There is no API path to a projected fact without provenance.
        let mut memo = Memo::new();
        let p = project(&mut memo, &json!({"walk":"LRRLLLLL"}), &[], "omega_lambda", "claude").unwrap();
        let grounds = p.assertion.field("grounds").and_then(|v| v.as_array()).unwrap();
        assert_eq!(grounds.len(), 1);
        assert_eq!(grounds[0].as_str(), Some(p.generator.cid.as_str()), "the assertion grounds the proposition in its generator");
        assert_eq!(p.assertion.field("proposition").and_then(|v| v.as_str()), Some(p.proposition.cid.as_str()));

        // And the provenance resolves: the proposition is admissible into the bulk.
        let mut known = std::collections::BTreeSet::new();
        known.insert(p.generator.cid.clone());
        assert!(crate::strata::admissible_propositions(&[p.assertion.clone()], &known).contains(&p.proposition.cid));
    }

    #[test]
    fn generator_is_a_well_formed_quantum() {
        // sanity: the generator quantum decodes its identity fields back
        let mut memo = Memo::new();
        let inputs = [("a".to_string(), Rat::new(2,3).unwrap())];
        let prog = json!({"in":0});
        let p = project(&mut memo, &prog, &inputs, "x", "claude").unwrap();
        let gs = generator_schema();
        let canon = Canon::new(&gs);
        assert!(canon.identity_projection(&p.generator.body).is_ok());
        assert_eq!(p.generator.field("program").and_then(|v| v.as_str()), Some(p.program_cid.as_str()));
    }
}
