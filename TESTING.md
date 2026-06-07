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
| Generator dedup is intensional **and ordered** (program + ordered inputs); reorder → different generator | `generator_dedup_is_intensional_and_ordered`, `projection_is_memoized` |
| **Forward-projection value is bound to identity** — read from the cited input quantum, not caller-supplied; non-value-bearing input rejected | `generator::input_value_is_read_from_the_cited_fact_not_the_caller` |
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
| **Lazy/batched** (Step 1): stage many, settle once == eager; pending() surfaces unsettled work | `closure::staged_then_settle_equals_eager`, `closure::settle_is_idempotent_and_handles_late_anchor` |
| `reach()` is the per-change cost the floor will gate (Step 2) | `closure::reach_is_the_cost_the_floor_will_gate` |
| **Floored settle** (Step 2): `floor → 0` == exact; defers far effects to `pending` (recoverable, no silent drift) | `closure::floored_zero_floor_equals_exact`, `closure::floored_defers_far_effects_without_false_certainty` |
| Floored settle is a **sound under-approximation** — never claims false certainty at any floor | `closure::floored_never_claims_false_certainty` |
| Coarse (low-attenuation) signal reaches far; fine settles local | `closure::coarse_reaches_far_fine_settles_local` |

## 5c. Chain verification — consistency, end to end (`chain.rs`)

| Criterion | Test |
|---|---|
| The full chain (anchor → generator → fact → closure) re-hashes and re-regenerates | `chain::full_chain_is_cryptographically_consistent_and_tamper_evident` |
| The consensus root is order-independent (two verifiers agree by one hash) | same |
| In-place tamper fails `verify`; substitution moves the consensus root | same |
| A generator that did not produce a fact fails `verify_regeneration` | `chain::regeneration_mismatch_is_caught` |

## 5d. Accuracy provenance — signed vouches + transparency log (`crypto.rs`, `log.rs`, `chain.rs`)

| Criterion | Test |
|---|---|
| A vouch verifies and is tamper-evident (changing the CID or signature fails) | `crypto::vouch_verifies_and_is_tamper_evident` |
| Distinct secrets → distinct signers; no impersonation by key-swap | `crypto::different_seeds_are_distinct_signers` |
| Keygen is deterministic (reproducible, no RNG) | `crypto::keygen_is_deterministic` |
| Append-only log verifies; editing/reordering/dropping a past entry is detected | `log::editing_a_past_entry_is_detected`, `log::reorder_and_drop_are_detected` |
| The head commits to the whole history | `log::head_commits_to_history` |
| An anchor is auditable iff a **trusted** party vouched **and** it was logged | `chain::anchor_accuracy_is_auditable_signed_and_logged` |

## 5e. Edge typing — Lineage importer → closure (`lineage.rs`)

| Criterion | Test |
|---|---|
| A `## Lineage` block parses into typed edges (`grounds`/`derives`/`proposes`) | `lineage::parses_typed_edges_from_lineage` |
| A doc with no Lineage block yields no typed edges (untyped stays `references`) | `lineage::no_lineage_block_yields_no_typed_edges` |
| Unknown kinds are skipped (no invented edges) | `lineage::unknown_kinds_are_skipped` |
| Typed edges seal as valid `edge_annotation` quanta (the supersedable form) | `lineage::edges_seal_as_valid_edge_annotations` |
| `grounds`/`derives` feed the closure as **certainty** (anchored ⇒ certain; no anchor ⇒ nothing; `references` grounds nothing) | `lineage::typed_grounds_feed_the_closure_as_certainty` |

*Applies the capability; does not author content.* The real harmonics corpus has
**0 / 285** docs with a Lineage block, so running this over it requires the lab to
author the grounds/derives relations (domain judgment). The importer closes the
mechanical gap — citation → certainty — once the content exists.

## 5f. Floored propagation — be lazy about the far-reaching (`propagation.rs`)

| Criterion | Test |
|---|---|
| Low-channel (low attenuation) reaches far; high-channel dissipates locally | `propagation::low_channel_reaches_far_high_channel_stays_local` |
| The floor bounds cost — floored reach < full transitive reach (laziness) | `propagation::floor_bounds_cost_laziness` |
| A smaller change reaches less far at the same floor (amplitude gates reach) | `propagation::amplitude_gates_reach` |
| A peripheral (leaf) change is local for free; the keystone is gravitational | `propagation::peripheral_change_is_local_for_free` |
| Levels assign the hierarchy by grounding depth (anchor = coarsest) | `propagation::levels_assign_the_hierarchy` |
| **Calibrate by gap** (Step 3): floor lands in a real coarse/fine gap; a uniform graph honestly reports no separation | `propagation::calibrate_finds_a_clean_gap`, `propagation::uniform_spectrum_reports_no_separation` |
| **Calibrate by budget** (Step 3): the floor admits exactly K nodes (WQS/ket-opt) | `propagation::floor_for_budget_admits_exactly_k` |
| Calibration drives `settle_floored` end-to-end (spectrum → floor → bounded settle → exact recovery) | `closure::calibrated_budget_floor_bounds_the_settle`, `closure::calibrate_gap_separates_or_declines` |

Note: floored propagation is a *deliberate multi-resolution approximation* —
`floor → 0` recovers the exact eager closure; a higher floor drops sub-floor far
effects in exchange for laziness. The floor / per-channel attenuation is the
calibration knob (same unbuilt self-calibration loop as SPINE §8).

## 5g. Subsumption — entailment as interval containment (`subsume.rs`)

| Criterion | Test |
|---|---|
| `subsumes` is exact interval containment on `Rat`'s i128 order (a precise value ⊢ its coarsenings; loose does not subsume precise) | `subsume::subsumes_is_exact_interval_containment` |
| Subsumption-dedup: coarsenings are redundant; the consensus keeps only the ⊢-strongest (maximal antichain) | `subsume::redundant_and_antichain_keep_only_the_strongest` |
| Certainty flows **down** the order: certify the strongest, coarsenings follow for free | `subsume::certainty_flows_down_the_order` |
| Dethroning: a stronger claim makes a weaker incumbent redundant (the dissipation that fixes attention favoritism) | `subsume::stronger_claim_dethrones_weaker_incumbent` |

## 5h. End-to-end journeys (`tests/`)

Integration tests against the **public API only** (a separate crate — also
validates the surface is complete). Cover the seams between layers and the
system-level invariants no unit test reaches. Plan + status: `tests/PLAN.md`.

| Criterion | Test |
|---|---|
| Every sealed quantum re-verifies (meta-contract, per node) | `e2e_rebuild::every_sealed_quantum_reverifies` |
| One fact's CID threads every layer (generator → assertion → closure → regeneration) | `e2e_rebuild::one_fact_cid_threads_every_layer` |
| **Rebuild bit-identical** across order / eager-vs-lazy / from scratch (E2E-5) | `e2e_rebuild::rebuild_is_bit_identical_across_order_and_laziness` |
| **Happy path** (E2E-1): full pipeline — regenerate → ground → certify → audit → root; NL is projection | `e2e_happy.rs` (5 tests) |
| **Adversary** (E2E-2): each corruption caught by its defense, composed — tamper/substitute/buggy-gen/forged-vouch/under-merge; corroboration stays clean | `e2e_adversary.rs` (6 tests) |
| **Lifecycle** (E2E-3): dethroning, certainty-flows-down, non-monotone retract, contested-excluded | `e2e_lifecycle.rs` (4 tests) |
| **Scale & laziness** (E2E-4): batch==eager, floored sound-under-approx, calibration drives the floor; final root invariant | `e2e_scale.rs` (3 tests) |
| **Portability + forward projection** (E2E-6): export → JSON round-trip → import (verify) → forward-project a new fact grounded in the imported one → certain + exact; corrupt bundle rejected | `e2e_portable.rs` (3 tests) |

## 5i. Portable bundle (`bundle.rs`)

| Criterion | Test |
|---|---|
| A bundle round-trips through JSON and imports (the certified fact is certain) | `bundle::round_trips_through_json_and_imports` |
| Import rejects a corrupt quantum (full-body digest, not just the identity CID) | `bundle::import_rejects_corrupt_quantum` |
| Import rejects a consensus-root mismatch (regenerated bulk ≠ shipped root) | `bundle::import_rejects_root_mismatch` |

## 5j. Boundary↔bulk reconciliation — empirical agreement (`reconcile.rs`)

| Criterion | Test |
|---|---|
| A derived exact value reconciles with a measurement ± σ — the residual-in-σ verdict | `reconcile::omega_lambda_13_19_is_consistent_at_about_007_sigma` (13/19 vs Planck ≈ 0.07σ → Consistent) |
| A wrong value is **Falsified**; a near miss is **Tension** | `reconcile::a_wrong_value_is_falsified`, `reconcile::a_near_miss_is_tension` |
| Reconciliation reads the derived value from the cited fact's identity (composes with #1) | `reconcile::*` use `reconcile_quanta` |
| The verdict seals as a verification quantum; re-reconciliation supersedes | `reconcile::verdict_seals_and_re_reconciliation_supersedes` |
| A falsified verdict can contest the derived fact out of the certain core | `reconcile::a_falsified_verdict_can_contest_the_derived_fact` |
| (e2e) An imported fact is empirically validated against the measurement | `e2e_portable::imported_fact_is_empirically_validated_against_the_measurement` |

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
8. **Consistency is proven; accuracy is auditable, never provable.** `chain`
   verifies the chain is internally faithful and reproducible (consistency); the
   signed-vouch + transparency-log pair (`crypto`/`log`) makes each anchor's vouch
   **non-repudiable and tamper-evidently logged** — so accuracy is now *auditable*
   (you learn who vouched, attributably, and that it wasn't retro-edited). But
   **no test claims a fact is true**: a vouch is a non-repudiable *assertion of*
   accuracy, not a proof of it. Still open: wiring the log into `.ket` persistence,
   multi-use key management (Ed25519 keys are multi-use here, but there is no
   revocation/rotation), and Merkle *inclusion proofs* (the log gives a hash chain,
   not succinct membership proofs — see `consensus_root` for the closure analogue).
9. **Not built / not claimed:** the **subsumption order** (entailment via the
   witness decision procedures — the *stronger* dedup that catches a re-derivation
   of an already-entailed claim; the grounded closure in §5b is built, this is
   not), supersession as stratified negation, the self-calibration loop,
   user-defined recursion, and cross-process determinism. See `SPINE.md` open
   targets.

## Running

```sh
cargo test          # all criteria above; must be 152/152 green (127 unit + 25 e2e), 0 warnings
cargo doc --no-deps # intra-doc links must resolve clean
# clippy is not installed in the reference env; run it where available
```

**Coverage ratchet** (per `PORTING.md`): the criteria list only grows. A new
capability ships with the criteria it establishes *and* the negative criteria it
does not — adding the latter is part of the definition of done.
