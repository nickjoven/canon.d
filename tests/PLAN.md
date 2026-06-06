# E2E test plan

End-to-end journeys that exercise the **seams** between layers (the CID threaded
between modules actually matches end to end) and the **system-level invariants** no
unit test can reach. They live in `tests/` — a separate crate compiled against the
public API only, which also validates the public surface is sufficient to do real
work. Shared fixture: `tests/common/mod.rs` (the Ω_Λ = 13/19 corpus, public-API
only).

| # | Journey | Threads | Uniquely asserts | Status |
|---|---|---|---|---|
| **E2E-5** | **Rebuild bit-identical** — capture a consensus root, rebuild in different order / eager-vs-lazy / from scratch → identical | quantum, generator, strata, closure, chain | the meta-contract whole-system; one fact's CID threads every layer | ✅ `e2e_rebuild.rs` |
| E2E-1 | **Happy path** — utterance/derivation → anchor → vouch+log → audit → regenerate → ground → certify → root | bridge, crypto, log, generator, strata, closure, chain | the full pipeline in one journey; seam consistency | planned |
| E2E-2 | **Adversary** — inject each corruption (body tamper, forged vouch, rewritten log, substituted fact, buggy generator, unreduced form) and assert the matching defense fires *together* | all defenses | defenses compose — one path's fix doesn't blind another | planned |
| E2E-3 | **Lifecycle** — loose claim certified → stronger forced claim dethrones it (subsumption) → retract anchor → subtree decertifies → contested excluded | subsume, closure, crypto/log | the certain core evolves correctly through a realistic edit history | planned |
| E2E-4 | **Scale & laziness** — stage many deltas (warm-start fold) → floored settle defers to pending → calibrate floor → full settle recovers; root matches eager | closure (lazy/floored/calibrate), propagation | laziness/flooring never changes the *final* answer, only the path | planned |

## Negative criteria (what e2e does NOT prove)

- Not a performance benchmark (O(n²) subsumption, in-memory store — toy scale).
- Not the NL frontier (`structure()` is fed hand-supplied structured bodies; no real LLM emitter).
- Not `.ket` persistence (in-memory; the transparency log isn't disk-wired).
- Not concurrency (single-threaded; the monoid property is asserted via order-independence, not parallel execution).

## Recommended build order

E2E-5 (done — smallest, headline contract, forces the fixture into existence) →
E2E-1 (comprehensive journey) → E2E-2 (adversary) → E2E-3/4 (refinements).
