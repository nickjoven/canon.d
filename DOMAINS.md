# DOMAINS.md — the intake spine across domains

## What this file is

The design doc for making intake **domain-generic**: the same spine
(seal → structure → bucket → audit) over corpora whose documents are
prose derivations, source code, or — eventually — regulated domains
like medical knowledge. It records the invariant core, the parameters
that vary per domain, the *domain pack* boundary that separates them,
and a findings ledger of every place domain knowledge has been caught
leaking into the core.

The companion implementation: `src/intake.rs` (the domain-free spine),
`src/packs/` (domain packs), INTAKE.md (the prose pipeline this
generalizes).

## The invariant core

These hold in every domain; none of them mentions prose, rationals, or
Rust:

- **Truth-maker locality** — facts made by the corpus's own event
  history must be retrieved by address; facts guaranteed by a stable
  external commons may live in priors/embeddings. Similarity may
  *route attention*; only addresses *decide*.
- **Quotient before lineage** — identity is computed wherever a normal
  form exists (content hash, canonicalized text, reduced rational);
  asserted lineage covers only the undecidable residue.
- **Nothing vanishes** — every proposal lands in exactly one bucket
  (promoted / needs_review / blocked); untyped citations are reported
  telemetry.
- **Tombstones are first-class** — retracted/falsified content must be
  refusable at the gate, not discoverable-and-ignored.
- **Determinism** — same bytes in, byte-identical report out. No
  clocks, no randomness, no iteration-order leaks.

## What varies: the six domain parameters

| Parameter | prose (harmonics) | code | medical (future) |
|---|---|---|---|
| **Noise quotient** | urtext normalize (N1–N9) | formatter/AST; byte-identity in v1 | units, coding systems (SNOMED, RxNorm) |
| **Decidable oracles** | exact rationals, SB normal forms | richest: types, tests, builds | thinnest: statistical, evidence-graded claims |
| **Commons stability** | stable (mathematics) | stable (language semantics) | unstable — the commons itself retracts |
| **Negative-space cost** | wasted derivation effort | shipped vulnerability (CVE tombstones) | patient harm (retracted-but-cited studies) |
| **Consensus source** | one author + measurement | maintainers + CI as mechanical agent | institutional: journals, committees, regulators |
| **Signatures** | deferrable (CIDs bind content) | useful (commit signing exists) | load-bearing from day one |

Two asymmetries worth naming. Code is the domain where this
discipline was invented piecemeal (git is a CAS, lockfiles are pinned
addresses, `@deprecated` is a tombstone, CI is a grounded assertion) —
which makes it the cheapest pack to build and the best control group.
Medicine inverts the addressed/embedded partition: because its commons
is itself an event stream (supersessions, withdrawals, retractions),
far more must be addressed, and it lands **last**, on a pack boundary
already validated by two easier domains.

## The domain-pack boundary

A pack is everything domain-flavored, in four slots:

1. **Normalizer** — the noise quotient
   (`bridge::Canonicalizer`; pack-selected since F2).
2. **Routes** — the structurer roster
   (`intake::Structurer`; spine takes `&[&dyn Structurer]` since F1).
3. **Taxonomy** — status/regime vocabulary (Class 1–5 vs CVE severity
   vs GRADE). *Not yet parameterized; lives in harmonics' registry,
   outside the spine.*
4. **Witness importers** — external-reality assertions (Planck values,
   CI runs, trial registries). *Not yet built (INTAKE.md Unit 5).*

Current packs: **prose** (`intake::prose_routes` — lineage sections,
corpus citations, exact-rational claims, glossary term bindings) and
**code** (`packs::code` — dependency citations, public-API term
census, `#[deprecated]` tombstone findings; byte-identity quotient).

## The flexibility contract

**Adding a pack touches zero spine code.** Any spine edit a new pack
forces is a leak of domain knowledge into the core. Leaks are not
failures of the exercise — surfacing them is the exercise — but each
one gets a ledger entry, and the only admissible fix is one that
*moves knowledge out of the core* (a parameter), never one that adds
domain awareness to it (a branch on the domain).

The test is mechanical, not aspirational: the code pack's
`self_host_canon_d_source` test intakes this repo's own `src/*.rs`
through `intake_corpus_with_routes` and passes with the spine
untouched.

## Findings ledger

- **F1 (fixed)** — the prose route roster was hardcoded inside
  `intake()`. Fix: routes became a spine parameter
  (`intake_with_routes` / `intake_corpus_with_routes`); the trio is
  now the named prose pack (`prose_routes`).
- **F2 (fixed)** — the noise quotient was selected by the `prose`
  *build feature*, not by the pack: a prose-featured build ran
  urtext normalization over Rust source and destroyed the `use` lines
  its structure lives in. Caught live by the self-host test under
  `--features prose` (intake.rs's citations collapsed from five to
  one). Fix: `bridge::Canonicalizer` — the pack picks the quotient
  per run; the report pins what actually bound the CIDs.
- **F3 (fixed)** — `StructuringOutput` had no findings/terms channel:
  routes could propose edges, citations, and ratio claims, but not
  review items or term bindings. Fix: `RouteFinding` (spine buckets
  them into `needs_review` with the doc filled in) and `TermBinding`
  (spine seals them as `term` quanta — `strata::term_schema`, identity
  = the term string, witness = the home doc — asserted on the source
  utterance, with a per-term corpus cross-audit surfacing duplicate
  homes as `term_home_conflict`). Unblocked in the same change: the
  code pack's `ApiRoute` (public-API census, qualified terms) and
  `DeprecatedRoute` (tombstone census), and the prose pack's
  `VocabRoute` (glossary rows → term bindings — the vocab layer's
  first sealed stratum).
- **F4 (open, narrowed)** — the proposal vocabulary is still partly
  prose-flavored: `RatioClaim` is a harmonics proposition type living
  in the core output struct (a generalized claim-with-witness type is
  the fix), and `structured` counts promoted structure only, so a
  citation-only corpus reads as unstructured despite real citations.
  Narrowed by F3: term bindings now count as structure.

## Status

v1, two packs, contract holding modulo the open ledger. The prose
pack remains the default everywhere (`intake`, `intake_corpus`, the
CLI without `--pack`); behavior and CIDs for prose corpora are
unchanged by the generalization.
