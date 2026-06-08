# Cost / Depth — schema & CI/CD best practices

Energy and compute are the binding constraint. The substrate's optimizations
(canonicalization → memoization, the generator memo, the lazy/floored closure,
subsumption, MDL, the σ-reconciliation gate) all reduce to one discipline: **never
compute beyond the depth the signal warrants, and never recompute what is already
addressed.** This guide turns that into a metric, a schema practice, and a
pipeline.

The organizing fact (from `SPINE.md`): **cost is in *deciding*, not *computing*.**
The schema is a one-time act of deciding what's signal vs noise; once drawn,
computing rides nearly free (canonicalization is cheap, results are memoized by
CID, the closure settles only deltas). So the leverage is almost entirely in the
schema — invest there, and the pipeline mostly *avoids* work.

---

## 0. The metric: warranted depth

Every computation has a **depth** — and cost scales with it:

| Depth axis | Cost driver | What bounds it (the warrant) |
|---|---|---|
| **Precision** | bits / bignum / algebraic | the measurement's **σ** — bits past σ are noise (`reconcile`) |
| **Reach** | nodes re-settled | the propagation **floor** / a node budget (`floor_for_budget`) |
| **Formalization** | LLM / proof search | the **decidability frontier** — past it, route to human (witness-free tier) |
| **Recursion** | generator unfolding | the **MDL** of the rule — a short generator is cheap and forced; a long one is a fit |
| **Novelty** | the whole computation | **subsumption / memo** — if it's already known or entailed, depth is 0 |

**The rule:** `actual_depth ≤ warranted_depth`, on every axis. Exceeding it is
wasted energy; meeting it is the optimum. The single number to watch is **cost per
unit of signal** = compute ÷ (facts certified × their σ-warranted precision).
Minimize it by maximizing reuse and matching depth to σ.

---

## 1. Which domains benefit most (reason from here)

The substrate's optimizations pay off in proportion to four properties. Target the
domains that have them; starve the ones that don't.

| | High leverage (compute deep) | Low leverage (stay cheap / route away) |
|---|---|---|
| **Reuse** | one generator → many facts; cited often (keystones) | one-off facts, cited once |
| **Canonicalizability** | exact formal core (rationals, lemmas) — dedups, memoizes | NL / fuzzy — no exact quotient, no dedup |
| **Measurement-grounded** | σ caps the depth; `reconcile` validates | unmeasured — no depth bound, no empirical check |
| **Fan-out** | high closure in-degree (a change reaches far, worth settling) | leaves (local, cheap by structure) |

**Tier A — invest depth.** High-reuse, exactly-canonicalizable, measurement-grounded
facts: the forced dimensionless constants, structural lemmas, anything one rule
grounds many times. Here memoization (compute once), subsumption (skip the
entailed), the exact witness (never recompute), and the σ-gate (stop at σ) all
compound. Best ROI in the system.

**Tier B — compute, don't over-invest.** Domain-exact but low-reuse: a one-off
exact calculation. Memoize it, but it won't amortize; keep its depth minimal.

**Tier C — do not spend deep compute.** NL, fuzzy, unmeasured, one-off. No exact
quotient → no dedup; no σ → no depth bound; no reuse → no amortization. Route to
the **witness-free / human tier**. A remote deep loop here is pure loss.

**The targeting heuristic:** spend depth where the *attenuators* pay off (high
reuse, exact, measured, far-reaching); the substrate was built to make exactly
those domains cheap-at-scale.

---

## 2. Schema best practices (the budget lives here)

The schema sets the attenuation budget — draw it deliberately.

1. **Minimize the identity tier.** Every identity field is a *fork* (a way for two
   things to differ); every projection field is *free attenuation* (collapses to
   nothing). Push to projection everything that doesn't bind *meaning* — names,
   prose, provenance-of-the-telling. Tighter identity ⇒ more dedup ⇒ more memo
   reuse ⇒ less recompute.
2. **Declare a witness wherever the domain has an independent check.** It buys the
   cross-audit, self-verification, and the σ-gate — and it keeps the fact out of
   the expensive witness-free/human tier. Minimize witness-free schemas.
3. **Make the witness exact** (a reduced string / structural digest, never a
   float). A lossy witness silently false-merges and forces recompute — it defeats
   the memo. Exactness is what makes "same CID ⇒ skip" *sound*.
4. **Set precision to σ, not to exactness, where a measurement bounds it.** Carry
   the resolution the warrant supports; don't seal 50 digits when σ is 10⁻³.
   `reconcile` is the check; the schema is where you stop.
5. **Prefer short generators (low MDL).** Forced beats fitted: one rule over many
   literals. A generator whose description length ≈ its output is a fit — high
   cost, no compression. The minimal-MDL generator is the canonical one.
6. **Bind values to identity; order positional inputs.** A generator must *fully
   determine* its output (ordered `List` inputs, values read from the cited fact —
   not supplied), or it is not soundly memoizable.
7. **Type the grounding edges** (`grounds`/`derives`). The closure then reads
   *certainty* (cheap, grounded) not *citation*; subsumption needs the order to
   skip the entailed.
8. **Seal the schema itself** and version it. The budget is the highest-stakes
   artifact (recursive closure) — a schema change is a logged, supersedable event,
   not a silent re-canonicalization. Audit it like any other quantum.

---

## 3. CI/CD best practices (enforce the budget mechanically)

Gates, cheapest-first (the `scripts/drift` ratchet pattern), each
`0 = clean / 1 = violation / 2 = env-error`:

1. **Dedup / memo gate (the biggest energy save).** *Before computing*, check the
   CID / consensus root / imported bundle: if the fact is present — or **entailed**
   by a stronger one (subsumption) — **skip**. No recompute, no remote round-trip.
   This is the gate that turns "open repo, init substrate" into "most answers are
   already here."
2. **Warranted-depth gate.** Each computation declares its depth; reject any that
   exceeds the warrant: precision > σ, reach > the floor budget, or formalizing a
   witness-free claim. A per-pipeline depth budget.
3. **Cost gate (lazy by default).** Settle the closure **floored**
   (`floor_for_budget`) in CI; reserve exact (`floor = 0`) for a nightly/audit run.
   Run on the **delta** (the warm-start fold), not the corpus — the lazy closure
   touches only the blast radius.
4. **Integrity gate (near-free).** Re-`verify` quanta and compare the **consensus
   root** — one hash, order-independent, embarrassingly parallel. Cheap enough to
   run every commit.
5. **Reconciliation gate.** Derived facts checked against their measurements (σ):
   `Falsified` → block the merge; `Tension` → flag for review; `Consistent` →
   pass. The empirical check, in the pipeline.
6. **Coverage ratchet.** Watch the W(Ω) locked-fraction climb; promote advisory
   checks to gating only as coverage reaches 100% (per `PORTING.md`).

### Routing: local vs. remote (the S/N decision)

A remote deep loop costs energy and latency; spend it **only** when all three hold:

> **dedup-miss × warranted-depth-high × reuse-high**

- *dedup-miss* — the local CID / consensus-root / bundle check found nothing and
  subsumption finds nothing entailing it.
- *warranted-depth-high* — σ is tight, the change is coarse/far-reaching, the
  domain is exact.
- *reuse-high* — the result is a keystone (high closure fan-out) that amortizes.

Otherwise: resolve **locally** (memo/subsumption hit → free; low warranted depth →
shallow/floored; low signal → witness-free tier). The substrate exists to make the
local "do we already have this / is it entailed / is it deep enough to matter?"
check cheap — so the expensive remote loop is reserved for the genuine
miss-high-high minority.

---

## 4. The honest bounds

- **The budget doesn't trivialize.** Computing is free; *deciding* (the schema, the
  floor, σ, the tolerance) is the irreducible cost. Invest there; it is paid once.
- **Calibrate, don't fix.** The floor / σ / tolerance budgets want the
  self-calibration loop (gap or budget), not magic constants.
- **The frontier has poor ROI for deep compute.** NL, irrationals (φ, √5 — `Rat` is
  i64), and the measurement vouch are where attenuators don't pay; route away, flag
  honestly, and let a human or a bignum/algebraic upgrade handle them *when a real
  chain needs it* — not speculatively.

**The one-line policy:** *Draw a tight schema once; then skip what's addressed,
stop at σ, floor the far, and reserve the remote loop for the rare
deep-novel-reused fact.*
