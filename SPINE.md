# SPINE — the integrated next iteration

> `ket/DESIGN.md` states the invariants the substrate is *for*. `canon.d`
> implements the first layer of them: the **quantum** (I/W/P), the cross-audit,
> the split edge, the NLP↔CID bridge. **This note states the spine** — the
> integrated layer that ties those parts into one mechanism and gives the
> irreducible fuzziness a *shape*, a *location*, and a *calibrator*.
>
> It is subject to its own discipline: sealed, superseded rather than re-argued.

## Abstract

The substrate already partitions every datum into content-addressed-primary or
fully-derived, and rejects everything else as silent drift. The spine reads that
partition **holographically**: information lives on a thin **boundary**
(generators + grounding attestations); the **bulk** (the entailment closure,
documents, rendered graphs) is a *determined, regenerable, dissipation-free*
projection of the boundary. You seal, audit, and pay cost only on the boundary.

The boundary survives unavoidable fuzziness two ways, and only two: an **anchor**
pins identity by external vouch (reality pins it, fuzz is named and carried as
±σ); a **stable orbit** pins identity by internal attraction (the structure
forces it, fuzz is absorbed by the basin). Canonicalization is re-read as a
**contraction**, not a function: a CID is a **lock**, not a representative, and
every fuzzy variant inside its basin flows to the same attractor. Five quantities
that looked independent — tongue width, MDL, forcing, support, fuzz-tolerance —
turn out to be one **unified ruler**: the depth of the attractor. The cross-audit
then **self-calibrates** that ruler by minimizing its own conflict. The
irreducible residue does not vanish; it relocates to a thin, measure-zero set of
basin boundaries, routed to anchors and humans.

One line: **identity is a stable orbit where the structure forces it, and an
anchor where reality pins it; the bulk mode-locks, the boundary vouches, the
irreducible fuzz lives on a thin set between basins.**

---

## 0. Rigor register — mechanism vs analogy

This spine borrows vocabulary from dynamical systems, thermodynamics, and
algorithmic information theory. An adversarial grounding pass (sealed alongside)
separates **load-bearing mechanism** from **evocative analogy**. Read the
analogies as intuition pumps, not claims; the mechanism is what the code
implements. Where the spine says "=", read "co-varies with"; where it says
"contraction," read "idempotent normalization"; where it says "reversible," read
"reconstructible."

| § | The framing | The actual mechanism | Status |
|---|---|---|---|
| §3 | mode-locking / "contraction" / Arnold tongue; a CID is an attractor | **idempotent normalization to a normal form** under a confluent, terminating rewrite (Church–Rosser; Newman's Lemma; normalization-by-evaluation), then hash. The "basin" is the *exact* preimage of a normal form under a **decidable quotient**. | Analogy over sound mechanism. "Tongue width" is a **discrete** schema knob (how hard the canonicalizer quotients), not a continuous dial. Content-addressing is **exact**; the only tolerance is the quotient applied *before* the hash — the **opposite** of LSH/SimHash (probabilistic, unverifiable, not addresses). There is no dynamical system, so "attractor/basin" is figurative. |
| §1 | Landauer / NAND; derived = reversible, silent-drift = erasure | **reconstructibility** — a derived datum re-runs forward from a *retained* source (Bennett-style history retention). | Analogy. "Regenerable from source" is *keeping the input and re-running*, **not** injectivity of the derivation map (which is lossy — hashing is). "silent-drift = NAND" carries no thermodynamic content. |
| §7 | tongue width = MDL = forcing = support = fuzz-tolerance | only **MDL ≈ −log Bayesian evidence** is a theorem (Rissanen; MML/Wallace), up to coding choice, asymptotic. | The five **co-vary**; they are **not one quantity** — different units (bits vs phase-space measure vs proof steps). The "=" is shorthand for "co-varies." ("Forcing" also collides with set-theoretic Cohen forcing — unrelated.) |
| §5 | generators; same generator → same identity | **Nix input-addressing**: identity = (program-CID, inputs); memoized normalization = build cache (Bazel/Buck remote cache). | Sound, but algorithmic dedup is decidable only up to **intensional** normal-form equality of the *generator*, **never** extensional output-equality (**Rice's theorem**). Totality buys strong normalization at the cost of Turing-completeness. O(output)→O(generator) is a **time/space tradeoff** conditioned on deterministic rebuild. |
| §6 | Datalog least model = consensus | least-model evaluation for **definite/Horn** programs (van Emden–Kowalski; PTIME). | Holds **only without real disagreement.** Conflicting heads/negation ⇒ **multiple stable models (ASP; Gelfond–Lifschitz)**; "consensus core" = **intersection of stable models = cautious consequence = coNP-hard**, not a PTIME least model. Cannot claim a unique least model *and* honest disagreement-surfacing at once. |

The honest reading: the **engineering core is a clean composition of known, exact
mechanisms** — a confluent canonical rewrite + content-addressing + Nix-style
input-addressing + a dual-route witness/cross-audit (the one genuinely novel
composition). The dynamical/thermodynamic language is scaffolding for intuition
and must not be mistaken for the mechanism.

---

## 1. Boundary vs bulk — the holographic partition

The `DESIGN.md` partition (primary | derived | ☠️) is a statement about *where
information lives*. Read it holographically:

- **Boundary** — the information-bearing surface. Exactly two kinds of datum sit
  here: **generators** (content-addressed-primary terms that *inject* bits) and
  **grounding attestations** (the witness-free leaves that touch reality). These
  are the only things sealed, audited, and paid for.
- **Bulk** — everything determined by the boundary: the entailment closure, the
  consensus head, projected documents, rendered graphs, SQL views, indexes. The
  bulk carries **no independent information**. It is a *dissipation-free*
  projection: regenerating it costs compute but injects zero new bits.

The `DESIGN.md` test — *throw away everything but the addressed content and the
log; rebuild every other artifact bit-identically* — **is the holographic
statement**: the bulk is reconstructible from the boundary, so the bulk holds
nothing. "Rebuild bit-identically" **is the reversibility test.**

### The Landauer / reversible-computing reading

| Partition row | Logical character | Thermodynamic reading |
|---|---|---|
| **Fully derived** (carries its generator) | **Reversible** — the output back-links to its source; nothing is erased; the input is recoverable | Toffoli/Fredkin — no `kT ln 2` floor; regenerable for free (in bits) |
| **Content-addressed-primary** (the injected bits) | **The boundary input** — the bits you deliberately put in | The energy you *choose* to spend; the only line item |
| **Silent drift** (primary, not addressed / derived, not regenerable) | **NAND** — irreversibly erases the generator; the preimage is gone | The heat-death failure mode: erasure with no audit, no heal |

The silent-drift class is precisely the **irreversible-erasure** class: a value
present in the system whose generator was thrown away. You cannot re-derive it
(no preimage) and you cannot diff it (no witness) — the two-question rule failing
is the logical-irreversibility detector. §5 shows that *mandatory back-links from
output to generator* make projection reversible **by construction**, abolishing
this class rather than policing it.

---

## 2. Anchors vs stable orbits — the two ways to survive fuzziness

Some identity is fuzzy and no amount of canonicalization removes it. There are
**exactly two** disciplined responses, and they are duals:

| | **Anchor** | **Stable orbit** |
|---|---|---|
| Pins identity by | external **vouch** | internal **attraction** (forcing) |
| Lives in | **boundary** | **bulk** |
| Fuzz is | **named and bounded** — carried as data (±σ) | **absorbed** — the basin eats it |
| Modality | **contingent** (could have been otherwise) | **forced** (the structure admits one lock) |
| Physics analogue | dimensionful **address** (H₀, v_EW) | dimensionless **ratio** (13/19, n_s) |
| In canon.d | the **attestation** leaf — witness-free | a **generated + witness-checked** claim |
| Description length | irreducible (a measured number) | **MDL-short** (one mediant rule) |
| Register | the "tuba" — a placed, sounded pitch | the "contrabass" — a forced interval |

An **anchor** says *this is where reality pinned a number*: Planck's
`Omega_Lambda = 0.6847 ± σ`. It is `attestation_schema` — identity is
`(instrument, dataset, locator)`, the `value` is non-identity, the uncertainty is
optional projection. The fuzz is **named**: ±σ is a field, not a silence. An
anchor binds nothing structurally; it *vouches*.

A **stable orbit** says *the structure forces this and only this*: the dark-energy
fraction locks to the mediant `13/19` because mode-locking (§2 of the harmonics
framework — Arnold tongues, the devil's staircase, Stern–Brocot mediants) admits
no nearby stable lock. The fuzz is **absorbed**: every value inside the tongue
flows to `13/19`. A stable orbit is generated, witness-checked, and MDL-short.

This is the framework's **address-vs-structure** distinction. The substrate is
built around a real physics derivation framework ("harmonics") that *forces*
dimensionless constants by mode-locking; the anchor/orbit split is how that
framework's two kinds of quantity map onto two kinds of identity in the store.
The boundary tolerates anchors (unlocked contingent data); the bulk admits only
orbits (§3).

---

## 3. Identity as mode-locking

The current canonicalizer (`canon.rs`) treats canonicalization as a *function*:
structured input → exact bytes → CID. That makes content-addressing **brittle** —
a cosmetic edit, a float in the last ulp, a re-derivation by a different route all
produce different bytes and therefore a different CID, churning the graph with
spurious forks. The spine reframes it:

> Canonicalization is not a function needing an exact preimage. It is a
> **contraction** whose fixed points are the canonical forms. A **CID is a lock,
> not a representative.**

Every fuzzy variant inside the basin — the Arnold **tongue** — flows to the same
attractor: paraphrase, independent re-derivation, float-within-tolerance,
cosmetic edit. The **tongue width is the robustness of the concept**: a wide
tongue means many variants collapse to one lock (a robust, forced concept); a
narrow tongue means the lock is delicate (a fragile distinction).

This cures content-addressing's over-sensitivity:

- **Cosmetic edits stay in the basin** → same CID → no churn. The byte-exact
  canonicalizer is the *degenerate, zero-width* case of this contraction; the
  spine generalizes it to positive width.
- **Drift becomes a phase transition.** A genuine semantic change is a **basin
  crossing** — the variant leaves one tongue and enters another, flipping the
  CID. "Did the meaning drift?" is no longer "did any byte change?" but "did we
  cross a basin boundary?" — a real, localized event, not churn.

### Self-consistency = existence of a stable lock

Mode-locking gives a free admission test. A **self-consistent** generator
converges to a fixed point (it locks). An **inconsistent** generator either fails
to converge or locks to ⊥ (the contradiction join, §6) — and is **rejected at
admission**. Hence the asymmetry already visible in the code:

- **The bulk admits only mode-locking generators.** A claim that does not lock is
  not a derived fact; it is noise, and `cross_audit` / closure refuse it.
- **The boundary tolerates unlocked data.** An anchor *is* contingent, unforced,
  un-converged — that is what makes it boundary information. It is admitted on a
  *vouch* (`vouched_by`), not on a lock.

`canon.d` today implements the width-zero limit. The spine's open work (§Open
targets) is the metric that gives the contraction positive, *calibrated* width.

---

## 4. Quantum stratification — proposition / assertion / justification

**This fixes a real defect in the current code.** `quantum.rs` puts the grounding
set `grounds` into **identity**:

```rust
.identity("grounds", FieldKind::Set(Box::new(FieldKind::Cid)))
```

That is correct for *some* notion of a claim and wrong for another, and the bug is
that there is **only one level**. Consider two *independent* derivations of one
fact — the same `Omega_Lambda = 13/19` grounded once in Planck and once in WMAP.
They have:

- **different identity CIDs** (different `grounds` sets → different identity
  projection → `different_grounding_forks_identity`), but
- **one witness** (the value-by-division `0.6842` is route-independent).

So `cross_audit` sees *one witness → many CIDs* and fires **`UnderMerge`** — it
reports a canonicalizer under-merge. But there is no bug: this is
**corroboration**, two independent supports for one proposition. The current
schema **mistakes corroboration for a defect** — a false positive baked into the
identity choice.

The fix is to **stratify the quantum into three levels**:

| Level | Identity | Role | Cardinality |
|---|---|---|---|
| **Proposition** | subject + canonical value, **no grounds** | extensional content — "the fact itself" | dedup & cross-audit happen **here** |
| **Assertion** | (proposition, agent, time) | "agent A asserts P at t" | many per proposition |
| **Justification** | (proposition, **grounds**, agent) | "P because of these grounds, per A" | **many per proposition** |

- **Proposition** carries *no grounds in identity*. Two independent derivations of
  `13/19` collapse to **one proposition CID**, one witness — `cross_audit` is
  clean. Dedup and the form-vs-witness audit live exclusively at this level.
- **Justification** is where grounds belong. `(proposition, grounds, agent)` —
  many per proposition. **Corroboration is multiple justifications into one
  proposition**, counted and weighted by independence (independent grounds count
  more than shared ones; §7's *support* axis).
- **Assertion** records *who said it when*, the channel for resolution-as-event.

This **refines, not overturns, the split-edge decision.** `quantum.rs`'s comment —
"a claim's grounding topology is part of what it *is*" — was right **for an
assertion/justification** and wrong **for a proposition**. The single-level
schema conflated the two. Note this is a **doctrine reversal at the claim layer**,
not a local patch: the existing `different_grounding_forks_identity` test and the
grounds-bind-identity comments encode the *opposite* default and must move with
it. And it is the right default only for **value-witnessed claims** — where a
claim's identity genuinely *is* its derivation (a proof whose identity is the
proof, not its conclusion), grounds belong in identity and the proposition layer
collapses onto the assertion. The stratification is the default for anything
carrying a value-witness, not a blanket law. The split edge (`edge_annotation_schema`, kind
correctable via supersession) is exactly the justification level done for one
edge; the spine generalizes it to the whole claim. `grounds` moves *down* a
level, out of proposition-identity and into justification-identity.

---

## 5. The generator layer — seal the generator, not the output

The deepest move: **content-address the generator, not its output.** A generator
is a content-addressed **term in a total rewriting language** plus its input CIDs.
Its identity is:

```
generator_identity = (program_CID, input_CIDs…)
```

The output is a **memoized normalization** of that term. Every output carries a
**mandatory back-link to its generator**. That single rule has four consequences:

1. **Projection becomes reversible by construction.** Output → generator is always
   present, so the output *carries its preimage*. This is Toffoli, not NAND
   (§1): the silent-drift class is **abolished by construction**, not policed by
   audit. A derived value with no generator back-link is *unrepresentable*, not
   merely *flagged*.
2. **Dedup jumps from structure to algorithm.** Today dedup is "same canonical
   bytes → same CID." With generators, **same generator → same identity**, even
   when outputs are produced by different surface routes. Dedup moves up the
   manageability ladder, from mechanical-byte-equality to algorithmic-equality.
3. **Storage goes O(output) → O(generator).** A devil's staircase with millions
   of plateaus is **one mediant rule**. You store the Stern–Brocot generator, not
   the tree — *fractal compression*. The boundary shrinks to the generators that
   actually inject information.
4. **Compute is traded in, but lands in the good column.** You now recompute the
   bulk on demand. That cost is tamed (§10): **memoized per generator-CID** (this
   is exactly Nix derivations — the CID is the perfect cache key), **amortized by
   partial evaluation** (Futamura projections specialize the generator to its
   inputs), and made **incremental** (differential dataflow recomputes only the
   marginal change).

A generator is a *stable orbit's program* (§2): MDL-short, forced, witness-
checkable. The generator layer is what makes "forced vs fitted" *computable* —
the generator's description length is the measure (§7).

---

## 6. The entailment lattice = Datalog

Facts do not form a *set under equality*. They form a **preorder under
entailment** (⊢). `DESIGN.md`'s "reviewed consensus = grounded closure" is, read
exactly, **Datalog's unique least model**:

| Substrate notion | Lattice / Datalog notion |
|---|---|
| `closure()` — the reviewed, grounded consensus | **bottom-up least-model evaluation** |
| supersession (resolution-as-event) | **stratified negation** |
| consensus core (what all voices share) | **meet** (greatest common content) |
| contradiction | **join = ⊥** |
| "resolution is a position, not an overwrite" | a **position in the order**, not a cell value |

Resolution stops being "pick the winning row" and becomes "locate the claim in the
entailment order." The consensus head is the least model; a contradiction is two
facts whose join is the inconsistent theory (conventionally ⊤ — everything
follows; `contradicts`, in `EDGE_KINDS`).

**The hard boundary (§0).** "Least model = consensus" holds *only* for a
definite/Horn program, which has a unique least model. The moment the substrate
honors **genuine disagreement** — its whole point (`DESIGN.md`: "two voices, same
CID, contradicting claim = healthy") — conflicting heads put you in **multiple
stable models** (ASP; Gelfond–Lifschitz), where there is no unique least model to
*be* the consensus. "Consensus core" is then the **intersection of stable models**
(cautious/skeptical consequence) — a meet over models, **coNP-hard**, not a PTIME
Datalog least model. You cannot have both a unique least model and honest
disagreement-surfacing: Datalog is the *consistent fragment*; the disagreement
channel lives one rung up in expressiveness and cost.

Crucially, **the entailment check is the witness's decision procedure.** Checking
`P ⊢ Q` uses the *same* machinery the witness already needs:

- rationals → **interval containment** (the witness is value-by-division);
- boolean → **BDD-implies** (the witness is a fingerprint);
- units → **dimension-match** (the witness is the dimension).

So entailment **scales exactly as far as the witness does** — no new oracle. The
witness tier (`witness_projection`, `verify_witness`) is already the entailment
engine in miniature; the spine reads it as such and runs closure over it.

---

## 7. THE UNIFIED RULER

Five quantities appeared independently across the design — in the
canonicalizer, the generator layer, the justification count, the physics
framework, and the basin metric. **They co-vary**, and it is tempting to call
them one quantity ("the depth of the attractor"). *Resist the identity* (§0):
only one pairing is a theorem — **MDL ≈ −log Bayesian evidence** (Rissanen;
MML/Wallace), up to coding choice and asymptotic. The rest are correlations
between quantities with **different units** (bits vs phase-space measure vs proof
steps). Tune one and the others *tend* to move — a load-bearing heuristic, not an
equation. The "=" in the table below is shorthand for "co-varies with."

| Name | Where it shows up | High value means |
|---|---|---|
| **Tongue width** | mode-locking (§3) — basin radius | many variants lock to one CID; robust concept |
| **MDL** | generator layer (§5) — program length | short generator; the rule is simple |
| **Forcing** | anchor/orbit (§2) — forced > fitted | the structure admits one lock, not a fit |
| **Support** | justification (§4) — evidence weight | many independent justifications corroborate |
| **Fuzz-tolerance** | the contraction — absorption radius | how much perturbation the lock survives |

A deep attractor *tends to be* wide-tongued, short-MDL, forced, well-supported,
and fuzz-tolerant; a shallow one narrow, long-MDL, fitted, thinly-supported, and
brittle. This is the spine's central **heuristic** (not unification): the five
correlate strongly enough that moving one usually moves the rest — useful for
calibration (§8), but the only one you can *compute against* with a theorem
behind it is MDL.

The payoff: **"forced vs fitted" becomes computable** — but at the right level,
and 2b's implementation sharpened *which*. The naive hope ("a short generator for
a value is forcing; a generator as long as its value is a fit") **fails per
single node**: a Stern-Brocot node is *incompressible* — its value's bit-size
equals its path length — so `walk(path)` does not beat `lit(n,d)` node-by-node
(for the Fibonacci spine the literal is strictly smaller; see
`generator.rs::single_node_mdl_does_not_universally_favor_recursion`). The win is
at the **family / structure** level: one constant-size recursive rule grounds
O(2^d) facts (`stern_brocot_to_depth`), so the *generator* is O(d) while the
*enumerated bulk* is O(2^d). Forced-vs-fitted is the **amortized description
length of the shared rule across everything it grounds**, not the length of any
one derivation — exactly the holographic O(rule)→O(bulk) of §1. MDL turns the
call mechanical, but the unit of comparison is the rule-over-a-family, not the
single fact.

---

## 8. The self-calibrating cross-audit

The unified ruler has **one knob**: tongue width. Mis-set it and you get one of
two failures, and **each failure surfaces through a different existing audit
channel**:

- **Too wide** → distinct concepts fall into one basin → **false merge**. This
  surfaces as a **witness `UnderMerge`**: one witness now spans CIDs that should
  have stayed distinct, so the cross-audit screams.
- **Too narrow** → corroborations fall into different basins → **false split**.
  This surfaces as **corroboration false-split**: multiple justifications that
  should fold into one proposition (§4) instead fork it.

So the cross-audit is not just a bug detector — it is a **two-sided error signal
on the width knob**. Minimizing cross-audit conflict **is the Lagrangian that
sets the tongue widths.** Widen until you start under-merging; narrow until you
start false-splitting; the fixed point between is the calibrated width. This is a
**fixed-point self-calibration** — the substrate tunes its own resolution.

This is exactly what `ket`'s **WQS / ket-opt** does: it searches the width
parameters to minimize surfaced conflict. The cross-audit (`cross_audit` in
`quantum.rs`) is already the objective function; the spine names it as such and
closes the loop.

---

## 9. W(Ω) — the locked-fraction coverage metric

`PORTING.md` reports coverage as a **flat percentage sealed** (harmonics ~31%).
The spine replaces it with a sharper, shaped metric. Let **W(Ω)** be the fraction
of the corpus that **mode-locks automatically** — variants that flow to a lock
with no human vouch — versus the **unlocked residue** that needs an anchor.

The key structural fact: **the fuzziness is measure-zero.** The devil's staircase
is mostly **locked plateaus** — exact identity, automatic, **full measure** —
separated by an **unlocked Cantor set** — genuine ambiguity, **measure-zero but
dense at basin boundaries**. Therefore:

- The corpus is **mostly locked**: W(Ω) → 1 on a stabilized corpus, and the
  locked part is *fully automatic* (no review).
- The fuzz is **concentrated, not smeared.** It does not haze every datum; it sits
  on a thin set of basin boundaries, and it is **routed** — to anchors (the
  vouch) and to humans (`needs_review`, the witness-free queue).

This is a strictly better coverage number than "% sealed": it distinguishes *the
part you got for free* (locked plateaus) from *the part that genuinely needs a
vouch* (the Cantor residue), and it tells you **where** the residue is (boundaries,
not interiors). The ratchet `PORTING.md` describes — watch coverage climb —
becomes a ratchet on W(Ω): watch the unlocked residue shrink toward its
measure-zero floor.

---

## 10. Cost model — boundary storage vs bulk compute

The spine's economics: **store the boundary, compute the bulk.** Two engines make
the bulk cheap.

1. **Memoization.** The CID is a **perfect cache key with no invalidation
   problem** — content-addressing means a changed input *is* a changed key, so a
   stale cache entry is *unrepresentable*. Generators (§5) make this Nix-style
   derivation caching: memoize per generator-CID.
2. **The commutative monoid.** Sealing is **idempotent and order-independent**
   (already true in `quantum.rs` — "a commutative monoid, exactly as in
   `gnosis/ingest.py`"). That makes ingest **embarrassingly parallel** — shard,
   ingest, merge, with the merge proved associative/commutative (gnosis-proved).
   No coordination, no write conflicts.

With both, plus **incrementality** (differential dataflow), the cost of an edit is
the **marginal** cost — what changed — not the **corpus** cost.

### The residue that escapes both engines

Honest about where cheapness ends:

| Frontier | Worst case | Practical reality |
|---|---|---|
| **Graph canonicalization** (mode-locking over a graph) | quasi-polynomial (GI-hard tail) | **expected near-linear** via Weisfeiler–Leman color refinement; exact GI only on rare WL collisions |
| **Entailment** (§6) in rich logics | undecidable / superpolynomial | **cost = the fragment you choose** — stay in PTIME / Datalog and it is bottom-up linear |
| **LLM structuring** (the NL→structure boundary, `structuring_schema`) | unbounded per-utterance | **parallel, memoizable, and riding exogenous unit-cost deflation** — the one cost line that falls over time on its own |

The structuring frontier is the only one whose unit cost is *exogenous* (set by
the LLM market, not by us); it is also memoizable (same utterance + emitter →
cached structuring) and parallel. The other two are bounded by **choices we
control**: how rich a graph, how rich a logic.

---

## 11. The honest floor

Every layer trivializes **computing**. None trivializes **deciding.** The
irreducible floor, named explicitly:

- **Choosing the schema / the logic fragment.** §6's "stay in Datalog" is a
  *decision*, made once, versioned (recursive closure). The substrate cannot make
  it.
- **Proof search.** Checking `P ⊢ Q` for a *given* proof is cheap; *finding* the
  proof is the open frontier.
- **The human vouch at a grounding leaf.** The anchor (§2) is contingent by
  definition — reality pins it, and someone must `vouched_by` it. No contraction
  removes that.
- **The basin metric itself.** Mode-locking needs a notion of "near" (§3). Picking
  that metric is the deciding act under the computing; §8 *tunes* a chosen
  metric's one knob, but the metric's *form* is decided.

The deepest bound: **program equality is undecidable (Rice's theorem).** So §5's
"same generator → same identity" is only mechanical in **total / restricted
generator languages** — the same formal frontier that forces §6 into a decidable
fragment. Algorithmic dedup and decidable entailment are **the same wall**, seen
from two sides.

And the measure-zero unlocked set (§9) **never disappears.** It is genuine
ambiguity; the substrate's honesty is that it *relocates* it — out of every
datum's interior and onto a thin set of basin boundaries, then routes it to
anchors and humans. **The frontier from every layer is the same frontier.** What
the spine adds is not its removal but its *characterization*:

- a **shape** — the devil's staircase (locked plateaus, Cantor residue);
- a **concentration** — measure-zero, living at basin boundaries;
- a **calibrator** — the cross-audit, which sets the one knob by minimizing its
  own conflict.

The frontier was always there. The spine makes it **thin, located, and tuned.**

---

## Open targets / not-yet-built

These are design targets, not done work — stated in the `DESIGN.md` register so
they are not mistaken for the endpoint.

- **Positive-width contraction.** `canon.rs` implements the width-zero
  (byte-exact) limit. The basin metric, the tongue, and the basin-crossing
  detector (§3) are unbuilt. *Decision under the computing: the metric's form.*
- **Quantum stratification.** ✅ **Built** (`src/strata.rs`): `proposition_schema`
  (no grounds in identity — dedup + cross-audit here), `assertion_schema`
  (`proposition` + `grounds` + `agent`), `corroboration`, `locked_fraction`. The
  false-positive `UnderMerge` is pinned (`quantum.rs`) and fixed.
- **Generator quanta.** ✅ **Stage 1 + 2a + 2b built** (`src/generator.rs`):
  `generator_schema` (identity `(program, inputs)`), `seal_program`,
  `provenance_audit` (silent-drift detector for the generator half of `grounds`),
  the `Rat` algebra + total `eval`, `project` (memoized normalization → a
  proposition), `mdl`, and — **2b** — structural recursion: `walk` (the
  Stern-Brocot fold, total by descent on a finite path) and `fold` (a total
  left-fold over a finite list), plus `stern_brocot_to_depth` demonstrating one
  O(rule) generator grounding O(2^d) facts. **Finding:** forced-vs-fitted MDL
  discriminates at the *family* level (the amortized rule), **not** per node —
  Stern-Brocot nodes are individually incompressible.
- **Back-link as a constraint.** ✅ **Built** (`src/strata.rs`, `generator.rs`):
  `seal_assertion(proposition, grounds: &[&Quantum], agent)` takes grounds as
  *existing sealed quanta* and rejects an empty set (`StrataError::NoProvenance`),
  so a derived datum without provenance is **unrepresentable** — you cannot name a
  ground that does not exist, nor zero grounds. `project` now returns the back-link
  assertion as part of `Projection`, so there is no API path to a projected fact
  without its provenance. `admissible_propositions` is the positive closure (a
  proposition is in the bulk iff a justification's grounds all resolve). §5's
  "abolish, don't police" is now kept by construction; `provenance_audit` remains
  the drift check for grounds retracted *after* sealing. *Not yet:* user-defined
  recursion (only the `walk`/`fold` primitives recurse — a general
  structural-recursion *combinator* with a descent checker is the 2c extension);
  and value-algebra beyond `Rat`.
- **Entailment / closure.** ✅ **Grounded closure built** (`src/closure.rs`): the
  incremental Datalog least model over the grounding relation — the certain core /
  reviewed consensus. `add_anchor`/`add_justification` are the monotone, semi-naive
  **warm-start delta-fold**; `retract` is the non-monotone blast-radius re-derive
  (the maintenance cost of certainty); `mark_contested` keeps the cautious core
  honest; `support` is corroboration strength. Order-independent (batch ==
  incremental). *Not yet:* the **subsumption order** via the witness decision
  procedures (interval-containment / BDD-implies / dimension-match) — the *stronger*
  dedup that catches re-derivation of an already-entailed claim — and supersession
  as stratified negation.
- **Self-calibration loop.** Wire `cross_audit` as the objective; search tongue
  widths to the fixed point (§8). The WQS/ket-opt analogue, native to canon.d.
- **W(Ω) metric.** Replace flat "% sealed" with locked-fraction coverage (§9):
  report locked plateaus vs the unlocked residue, and *where* the residue sits.
- **Corroboration weighting.** Count justifications per proposition, weighted by
  ground independence (§4, §7-support).
- **Anchor uncertainty as first-class.** `attestation_schema` already has optional
  `uncertainty`; make ±σ flow through closure so the bulk carries propagated
  bounds, not bare values.

---

## The one-line statement

> **Identity is a stable orbit where the structure forces it, and an anchor where
> reality pins it; the bulk mode-locks, the boundary vouches, and the irreducible
> fuzz lives on a thin set between basins.**
