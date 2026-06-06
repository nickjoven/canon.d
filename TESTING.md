# Test success criteria

What "correct" means for canon.d, as falsifiable criteria mapped to tests. This
is the substrate's own discipline applied to itself: a property is *claimed* only
if a test establishes it, and the bounds it does **not** establish are stated as
loudly as the ones it does. Written after an adversarial audit (2026-06-05) that
found — and this document now pins — real defects.

## 0. The meta-criterion (the contract)

> `ket/DESIGN.md`: *throw away everything but the addressed content; can you
> re-derive every other artifact and confirm it bit-identically?*

Operationalized as **`Quantum::verify(schema)`** — re-derive the address from the
stored body + schema and confirm it equals `cid`. The substrate is correct only
if this holds for every sealed quantum and *fails* on any tamper.

- ✅ `quantum::verify_round_trips_and_detects_tamper` — fresh seal verifies; a
  mutated **identity** field fails; a mutated **projection** field still verifies
  (projection doesn't bind the address).
- ✅ `quantum::serde_json_preserve_order_is_off` — guards the load-bearing build
  invariant the address determinism rests on (sorted JSON keys).

If `verify` ever returns `false` for an untampered quantum, the substrate is
broken regardless of any other green test.

## 1. Identity & canonicalization (`canon.rs`, `quantum.rs`, `schema.rs`)

| Criterion | Test |
|---|---|
| Same meaning → same CID regardless of key order | `canon::canonical_determinism`, `quantum::dedup_by_identity_not_name` |
| Projection fields (names/prose) never bind identity | `quantum::dedup_by_identity_not_name` |
| `Set(Cid)` fields are order-independent & deduped | `quantum::grounding_set_is_order_independent` |
| Different grounding topology forks identity (assertion-level) | `quantum::different_grounding_forks_identity` |
| Identity is **schema-relative** (schema CID folds into address) | `quantum::identity_is_schema_relative` |
| Missing required field is rejected | `canon::rejects_missing_required` |
| Idempotent re-seal is not a conflict | `quantum::cross_audit_clean_when_forms_agree` |

## 2. Witness & cross-audit (`quantum.rs`)

| Criterion | Test |
|---|---|
| Witness self-audit agrees iff the recomputation matches | `quantum::witness_self_audit` |
| One witness → many CIDs surfaces as `UnderMerge` | `quantum::cross_audit_catches_under_merge` |
| One CID → many witnesses surfaces as `WitnessDisagreement` | `quantum::witness_disagreement_is_caught` |
| Witness-free schemas are skipped (the review tier) | `quantum::witness_free_schema_is_skipped` |
| The witness is **exact** (string), not a lossy float | `strata::*` use `"13/19"`; enforced by `proposition_schema` |

## 3. Stratification & corroboration (`strata.rs`)

| Criterion | Test |
|---|---|
| Two tellings of one value → one proposition CID (no false `UnderMerge`) | `strata::corroboration_is_clean_at_proposition_level` |
| A real under-merge still fires at proposition level | `strata::real_under_merge_still_caught_at_proposition_level` |
| Corroboration counts **distinct** asserters | `strata::corroboration_counts_distinct_agents` |
| The earlier defect (grounds-in-identity false positive) is pinned | `quantum::grounds_in_identity_false_positives_on_corroboration` |

## 4. Back-link constraint (`strata.rs`, `generator.rs`)

| Criterion | Test |
|---|---|
| An assertion with zero grounds is unrepresentable (`NoProvenance`) | `strata::seal_assertion_rejects_empty_provenance` |
| A proposition is admissible only if its grounds resolve | `strata::admissible_requires_resolving_grounds` |
| `project` always emits the back-link assertion | `generator::project_emits_mandatory_back_link` |

## 5. Generator layer (`generator.rs`)

| Criterion | Test |
|---|---|
| Program CID is stable | `program_cid_is_stable` |
| `eval` is total *into `Result`* — overflow is `Overflow`, never panic/wrap | `arithmetic_overflow_is_typed_not_panic`, `eval_errors_are_typed` |
| Projection reproduces the fact bit-identically | `projection_generates_the_fact`, `walk_projects_the_fact` |
| Generator dedup is intensional (program + inputs) | `generator_dedup_is_intensional`, `projection_is_memoized` |
| `walk`/`fold` are total structural recursion | `walk_reproduces_stern_brocot_nodes`, `fold_is_total_structural_recursion` |
| One O(rule) generator grounds O(2^d) distinct facts | `fractal_generator_is_holographic` |

## 5b. Grounded closure — the certain core (`closure.rs`)

| Criterion | Test |
|---|---|
| A proposition is certain iff a justification's grounds are all certain (transitive) | `closure::admits_transitively` |
| An unresolved ground → never certain (needs a vouch / anchor) | `closure::ungrounded_never_certain` |
| Incremental == batch (warm-start order-independence) | `closure::incremental_equals_batch` |
| Retract is non-monotone — the blast radius loses certainty | `closure::retract_shrinks_non_monotone` |
| Corroboration is robustness — alternative grounding survives a retract | `closure::retract_keeps_alternative_grounding` |
| A contested fact is excluded and cannot certify others (cautious core) | `closure::contested_is_excluded_and_propagates` |

## 5c. Chain verification — consistency, end to end (`chain.rs`)

| Criterion | Test |
|---|---|
| The full chain (anchor → generator → fact → closure) re-hashes and re-regenerates | `chain::full_chain_is_cryptographically_consistent_and_tamper_evident` |
| The consensus root is order-independent (two verifiers agree by one hash) | same |
| In-place tamper fails `verify`; substitution moves the consensus root | same |
| A generator that did not produce a fact fails `verify_regeneration` | `chain::regeneration_mismatch_is_caught` |

## 6. Bridge (`bridge.rs`)

| Criterion | Test |
|---|---|
| NL is projection (paraphrases → distinct utterances, one claim) | `nl_is_projection_paraphrases_share_one_claim` |
| Structuring supersedes (same emitter) / coexists (two emitters) | `restructure_supersedes_disagreement_coexists` |
| Attestations are witness-free; value is non-identity | `attestation_is_witness_free_review_tier`, `attestation_value_is_not_identity` |
| Dangling grounds are flagged | `ground_audit_flags_then_clean` |

---

## Negative criteria — what the suite deliberately does NOT claim

Stated per the rigor register (`SPINE.md` §0). These are **known bounds**, not
oversights; a test that claimed them would be lying.

1. **No fuzzy/approximate identity.** Content-addressing is exact; "tolerance" is
   only the canonicalizer's *exact quotient* applied before hashing. There is no
   LSH/SimHash-style near-match.
2. **No extensional generator dedup (Rice).** "Same generator → same identity" is
   *intensional* (same program term + inputs). Two different programs computing
   one value are two generators (→ corroboration), never auto-merged.
3. **The witness is not independent of the datum.** For a rational it re-reduces;
   it catches an *unreduced canonical form*, not a wrong value. It is exact (no
   float collision) but not a second source of truth.
4. **`Rat` is i64-bounded.** Beyond ~i64 the value algebra returns `Overflow`
   (tested), not a bignum result. `walk` of a long path (≳90 moves) overflows by
   design. A bignum algebra would lift this; it is a toy bound, now explicit.
5. **MDL does not discriminate forced-vs-fitted per node** (Stern-Brocot nodes are
   incompressible); only the amortized rule across a family compresses
   (`single_node_mdl_does_not_universally_favor_recursion`).
6. **`seal_assertion` requires provenance at construction, resolves it at
   closure** — it does not validate that a ground is a *relevant* generator at
   seal time; `admissible_propositions` does that downstream.
7. **`locked_fraction` is schema-homogeneous** (1.0 or 0.0 per call) — mixed-bag
   coverage is not yet computed.
9. **Consistency is proven; accuracy is not.** `chain` verifies the chain is
   internally faithful and reproducible (consistency) and roots it in its anchors,
   but **no test claims a fact is true** — accuracy bottoms out at the anchors'
   vouches. Two crypto constructs that would make accuracy *auditable* (still not
   provable) are unbuilt: a **hash-chained / Merkle transparency log** (so "the log
   wasn't rewritten" is verifiable — today the log is trusted) and **signed
   attestations** (so each anchor's vouch is non-repudiable — today `vouched_by` is
   a plain name).
8. **Not built / not claimed:** the **subsumption order** (entailment via the
   witness decision procedures — the *stronger* dedup that catches a re-derivation
   of an already-entailed claim; the grounded closure in §5b is built, this is
   not), supersession as stratified negation, the self-calibration loop,
   user-defined recursion, and cross-process determinism. See `SPINE.md` open
   targets.

## Running

```sh
cargo test          # all criteria above; must be 81/81 green, 0 warnings
cargo doc --no-deps # intra-doc links must resolve clean
# clippy is not installed in the reference env; run it where available
```

**Coverage ratchet** (per `PORTING.md`): the criteria list only grows. A new
capability ships with the criteria it establishes *and* the negative criteria it
does not — adding the latter is part of the definition of done.
