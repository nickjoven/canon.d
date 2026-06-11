# RELEASE.md — units of release & the test ecosystem

The substrate's release problem is unusual: **a release is a body of law.** Change
one schema field and every address in every deployed corpus moves. So the unit of
release is not "a set of features" — it is *what becomes frozen*, in dependency
order, each unit with the gate that proves the freeze held.

A release is identified by the triple:

```
(crate semver, constitution root, treaty wire_version)
```

`canon-demo constitution --json` prints the root; CI compares it against the
released one. **Root drift without a major bump fails the build** — the law may
not change under a minor version, by construction rather than by review.

---

## 1 · Units of release

Frozen first → frozen last. Each unit ships only when its gate is green *and*
every unit below it is already frozen.

| Unit | Contents | What freezes | Release gate |
|---|---|---|---|
| **U0 Treaty** | the address function (`schema_cid ‖ 0x1f ‖ identity`), blake3, `body_digest`, `consensus_root` (domain-separated, length-prefixed), `wire_version` | the *bytes* of agreement | golden vectors V1–V4 (`tests/golden.rs`) reproduce on every CI platform |
| **U1 Kernel** | `schema.rs`, `canon.rs`, `quantum.rs` (seal / verify / cross_audit) | what a claim *is* | determinism tests + `preserve_order`-OFF assertion + V1/V2 |
| **U2 Reasoning** | `strata`, `closure`, `propagation`, `subsume` (`Rat`), `generator` | what *certainty* and *entailment* mean | `e2e_rebuild` (bit-identical) + calibration tests |
| **U3 Provenance** | `crypto` (Ed25519), `log`, `chain`, `lineage` | who said it, in what order | chain verify + anchor audit e2e |
| **U4 Portability + Constitution** | `bundle`, `constitution` | what *travels*, and the law itself | `self_hosts()` + tamper-rejected + V3/V4 + a peer machine reproduces the root |
| **U5 Gates** | `cost`, `gate`, `reconcile` + the **exit-code contract** | the CI interface | exit codes frozen: `0` Pass/Warn, `1` Fail, `2` usage |
| **U6 Surfaces** | `canon-demo`, `web/`, `examples/`, deck | pedagogy | demo `--json` outputs as snapshot tests; cited numbers stable |

Two consequences worth stating plainly:

- **U0 is smaller than U1.** The treaty is not "the kernel" — it is the five
  byte-level decisions two *different* kernels must share to interoperate. It
  freezes first and hardest.
- **U6 may change freely under a minor version; U0 may never.** Everything else
  sits on the gradient between.

Versioning rule: a change to any U0–U2 byte semantics ⇒ **major** (new
constitution root, migration via `mapping` quanta). New schema, new gate, new
surface ⇒ **minor**. Anything that golden vectors can't see ⇒ **patch**.

Queued for U4 (fast-follow, flagged by the seal-constitution workflow): give
`schema_schema()` a **witness field** (e.g. an independently computed field-count
or field-list digest) so the constitution is audited by the same *two* routes a
fact is — today the fixpoint rests on identity-address verification alone.

---

## 2 · Telemetry

Principle: **the substrate's telemetry is sealed in the substrate.** Counters are
attestation quanta under a telemetry schema; the stream is tamper-evident the
same way facts are. And telemetry carries **CIDs, never bodies** — content-free
by construction, so it can leave the trust boundary.

| Signal | What it is | What it's the canary for | Action threshold |
|---|---|---|---|
| `undermerge_rate` | UnderMerge conflicts per 10k seals | **canonicalizer drift** — the highest-severity defect | > 0 in prod = page |
| `witness_disagreement_rate` | WitnessDisagreement per 10k seals | non-deterministic witness computation | > 0 = investigate |
| `dedup_skip_ratio` | `skipped / requested` from `dedup_gate` | the economic claim ("skip the call") — if it decays, entailment isn't firing | trend alarm |
| `sigma_histogram` | z-values from `reconcile_gate` | drift between derivations and reality; a slow walk toward 3σ precedes Falsified | distribution shift |
| `gate_outcomes` | Pass / Warn / Fail counts | health of the merge gate | Fail spike |
| `import_failure_taxonomy` | CorruptQuantum / CorruptLog / RootMismatch counts | wire corruption vs. adversarial tamper vs. version skew — three different responses | any RootMismatch = treaty check |
| `constitution_root` | emitted once per process start | **the heartbeat.** Any change is a release event; an unexplained change is an incident | != released root = halt |
| `memo_hit_rate`, `settle_iterations`, `seal_latency` | perf | regression budget | per-release baseline |

The top row and the bottom row bracket the design: `undermerge_rate` says the
*law* is sick; `seal_latency` says the *machine* is. Everything between is the
substrate doing its job — making disagreement loud and attributable.

---

## 3 · Environment guarantees

What the substrate demands of any machine that runs it. Each guarantee has an
enforcement, not a hope:

1. **`serde_json/preserve_order` stays OFF.** The load-bearing build dependency
   (`schema.rs` documents it): one crate enabling that feature silently moves
   every address. Enforced by the existing test assertion; promote to a
   `cargo-deny`/feature-audit step in CI so it fails at *build graph* time, not
   test time.
2. **Golden vectors reproduce everywhere.** `tests/golden.rs` pins four hashes —
   law bytes (V1), the seal path (V2), the keystone (V3), the consensus
   construction (V4) — and the CI matrix (Linux/macOS/Windows; later WASM) must
   reproduce all four. This single mechanism subsumes most platform questions:
   endianness, hasher version, JSON float formatting, field ordering. If the
   vectors hold, the environment is conforming; nothing else needs trusting.
3. **No floats in identity.** `f64` formatting is the one serde_json behavior we
   can't easily pin across versions. Floats are legal in witness/projection
   fields (Planck's `0.6847`), never in identity. Today this is discipline;
   the lint (reject `identity(_, FieldKind::Float)` at `Schema` construction)
   is a cheap U1 hardening.
4. **Seal paths are pure.** No clock, no randomness, no environment reads
   anywhere under `Quantum::seal` / `consensus_root`. Already true; the
   determinism tests are the regression net.
5. **Pinned toolchain + lockfile.** `rust-toolchain.toml` + committed
   `Cargo.lock` for release builds. The golden vectors then verify the pin did
   its job rather than substitute for it.

The shape to notice: guarantees 1, 3, 4, 5 are *causes*; guarantee 2 is the
*observable*. The vectors don't prevent drift — they make every drift loud,
attributable, and release-blocking. Same philosophy as the substrate itself.

---

## 4 · Handshakes

In trust order, outermost first:

**H0 — Constitution exchange (the treaty handshake).** Before any facts move,
peers exchange constitution bundles (`Constitution::bundle()`, ~5 KB), `import`
each other's, and compare:

| Comparison | Meaning | Protocol response |
|---|---|---|
| same root | same law, byte for byte | full interop — exchange facts freely |
| same keystone, different articles | same canonicalizer, different law subset | negotiate the article intersection; facts under shared laws interop |
| different keystone | different canonicalizer — different *physics* | no direct interop; bridge via explicit `mapping` quanta, or refuse |

The treaty triad (`wire_version`, `body_digest_algo`, `canon_rule_cid`) is the
first message; a higher `wire_version` than the importer speaks is rejected
loudly, never best-effort parsed.

**H1 — Fact exchange.** Bundle → `import` (re-verify every quantum, replay the
log, rebuild the closure) → compare `consensus_root`. Agreement on an entire
knowledge state is one hash comparison. Reputation-free by construction.

**H2 — Reality reconciliation.** Derived facts vs. instrument attestations
through `reconcile_gate` under an explicit `Tolerance`. The σ residual is the
handshake with the world: Consistent / Tension / Falsified.

**H3 — CI handshake.** The exit-code contract (U5): `0` clean (Pass *and* Warn —
tension flags, never blocks), `1` violation, `2` usage. `canon-demo` already
honors it; freezing it makes every CI system a conforming peer.

**H4 — The human handshake.** What no design absorbs: the *initial* trust in an
attestation (`vouched_by`), the choice of `Tolerance`, the decision that a schema
is law. The Ed25519 `Vouch` layer doesn't remove this trust — it makes it
explicit, signed, and auditable. The design's job ends at making the human
commitment visible; it cannot make it for you.

---

## 5 · The friction ledger

Where each known friction is absorbed — by design (unrepresentable), by
interface (handled at the boundary), or residually by humans (made explicit,
not removed):

| Friction | Absorbed by | Mechanism |
|---|---|---|
| JSON key order, set order | **design** | canonicalizer; `Set(_)` sorts + dedups |
| equivalent-but-rephrased forms (26/38 vs 13/19) | **design** | one address; dual-witness audit makes the residue loud |
| tampered content | **design** | CID re-seal + `body_digest`; forgery is detectable, not preventable-by-policy |
| silent schema drift | **design** | versioned schema CIDs — a law change *is* a new address |
| replay / reordering of history | **design** | hash-chained transparency log |
| schema migration | **interface** | explicit `mapping` quanta; no silent migration path exists |
| "which law did you compute under?" | **interface** | H0 treaty handshake; constitution bundle |
| CI integration | **interface** | exit-code contract + `--json` |
| ontology inaccessibility (narrow bands) | **interface** | failure-led demos (arXiv:2602.06176 baselines); CLI/web/deck |
| re-verification toil | **interface** | `seal-constitution` workflow — one invocation, adversarial verifiers, one verdict |
| platform/toolchain drift | **interface** | golden vectors turn it into a red CI row |
| initial trust in attestations | **human** | `Vouch` makes it signed and explicit |
| choosing tolerances | **human** | `Tolerance` is a parameter, never a default hidden in code |
| deciding what is law | **human** | `builtin_laws()` is a reviewed, versioned list — constitution changes are commits |
| push / release authority | **human** | nothing in the substrate self-publishes |

The pattern the whole table follows: **design makes wrongness unrepresentable
where it can, the interface makes it loud where it can't, and what's left is a
named human commitment rather than an ambient assumption.**

---

## 6 · Order of work

1. **Now (this release):** golden vectors V1–V4 (`tests/golden.rs`) + this doc.
2. **U1 hardening:** float-in-identity lint; promote `preserve_order` audit to
   build-graph time.
3. **U4 fast-follow:** witness field on `schema_schema()` (close the
   constitution's single-route audit gap).
4. **Telemetry schema:** seal the §2 counters as attestation quanta; wire the
   constitution-root heartbeat into `canon-demo`.
5. **CI matrix:** golden vectors on Linux/macOS/Windows; pin toolchain.
6. **First treaty test:** two checkouts on two machines exchange constitution
   bundles and reach H0 "same root" — the cross-machine analogue of
   `constitution_is_portable_and_reproducible`.
