# INTAKE.md — canonicalized intake of unstructured text

The proposal: make **structuring a falsifiable act, not a preprocessing step.**

Two neighbor systems define the problem from opposite ends:

- **harmonics** has ~220 prose derivations and builds its graph by *regex over
  prose* (`build_derivation_graph.py`). Known failure modes: references that miss
  the five regex patterns vanish silently (W1); ~47% of graph nodes are unsealed
  and can drift (W2); a typo'd `## Lineage` target is dropped without a warning
  (W4); equivalent derivation paths are lost (W5); the negative-results ledger
  and the prose corpus desynchronize by hand (W9).
- **gnosis** has the right substrate discipline (canonical form + independent
  invariant, commutative ingest, cross-audit) but *refuses unstructured input by
  design*: "this spike assumes claims arrive structured." Its README names the
  open problem this document answers.

Both fail at the same seam: **the step from prose to structure is currently
either untrusted automation (regex) or unrecorded human labor (hand-crafted
dicts).** In both cases the step itself leaves no auditable trace — it is the
one link in the chain with no CID, no witness, no falsifier.

## The model: four strata, every step sealed

```
 source attestation        "this PDF/file/instrument exists"   (bridge: attestation_schema)
        ▲ grounds
 utterance quantum         the raw text, verbatim, sealed      (bridge: utterance_schema)
        ▲ grounds
 structuring quantum       "I read THIS span as THIS claim"    (bridge: structuring_schema)
        ▲ grounds                — attributed, confidence-scored, falsifiable
 proposition + assertion   the claim itself, one CID per meaning   (strata)
```

1. **Utterance stratum.** Every source document is sealed at entry — in its
   **urtext canonical form** (the `urtext` crate: content in → bytes out;
   deterministic, idempotent, metadata structurally excluded), so the address
   of the quote precedes any interpretation *and* survives reformatting. Two
   copies of the same content — different wrap width, line endings, bullet
   style — seal to one CID; raw bytes remain available beside it as a
   projection when byte-exact provenance matters. This alone repairs
   harmonics W2: the prose corpus becomes content-addressed ground, and
   "utterances with no structuring yet" is a *coverage metric*, not a silent
   hole.

2. **Structuring stratum** — the load-bearing novelty. The NL→structure step is
   itself a sealed quantum: identity = (utterance CID, extracted claim),
   projection = the prose span, the structurer's identity, confidence. An
   extraction error is now a *wrong claim with an address and an author*, not a
   silent regex miss. gnosis's "agents must emit structured claims" assumption
   becomes a recorded, auditable act.

3. **Proposition stratum.** The structured claim seals under
   `proposition_schema`: the same value extracted from two different papers, two
   different phrasings, two different structurers → **one CID** → corroboration
   (the existing `corroboration`/`locked_fraction` machinery), not duplication.
   harmonics W5 (equivalent derivations lost) inverts into a feature: multiple
   assertions on one node *strengthen* it.

4. **Grounding chain.** Assertions back-link (constructor-enforced) through
   structuring → utterance → source attestation. The full citation chain is a
   lineage closure with the typed edges harmonics already names —
   `grounds`/`derives`/`proposes` map 1:1 onto `lineage.rs`'s `TypedEdge`. A
   lineage target that doesn't exist fails the closure **loudly** (fixes W4: the
   typo'd edge can no longer be silently dropped).

## The audit: N independent structurers

The cross-audit that catches 26/38 generalizes to extraction. Run **N
structurers** over the same utterance — different prompts, different models, a
regex pass, a human — each producing a structuring quantum:

- **All agree** (same proposition CID): auto-promote. Agreement across
  independent routes is the witness.
- **Disagree** (one span → multiple proposition CIDs): an UnderMerge-shaped
  conflict → the `needs_review` queue, *with the disagreeing readings attached
  as evidence*. The human resolves a presented conflict instead of auditing an
  invisible pipeline.

This is gnosis §5e ("detector, not resolver") answered structurally: detection
stays mechanical, resolution becomes a worked queue, and the resolution itself
is a new structuring quantum that supersedes — attributed, like everything else.

## What each neighbor's weakness becomes

| Weakness | Today | Under this design |
|---|---|---|
| harmonics W1: regex misses a prose reference | silent missing edge | un-structured utterances are a visible coverage gap; extraction is N-routed |
| harmonics W2: ~47% nodes unsealed | silent drift | sealing is the entry ticket; coverage is telemetry |
| harmonics W4: lineage typo dropped | no feedback | nonexistent CID fails closure import loudly |
| harmonics W5: alternate derivations lost | one `source:` listed | same proposition CID, many assertions = corroboration |
| harmonics W6/W9: ledger/prose desync | hand-sync of 3 markdown files | Falsified verdicts are quanta; surfaces query the closure |
| harmonics MANIFEST.yml residuals | hand-curated "0.07σ" strings | `reconcile_gate` computes σ exactly; scorecard is *generated* |
| gnosis 5f: assumes structured input | external, unrecorded | the structuring stratum, sealed + attributed |
| gnosis 5a: encoding provenance lost | `26/38`→`13/19`, walk forgotten | generator quanta seal the *program* (the walk is the provenance, replayable) |
| gnosis 5d: rounded invariants | `round(…, 12)` heuristic | exact `Rat` (i128 Ord); σ vs measurement uncertainty, not decimals |
| gnosis 5b: immutable ground conflicts | two sealed truths, no path | supersession by new attestation + Tension/Falsified verdicts + log order |

And one **active enforcement** neither has: harmonics' negative-results ledger
("do not re-assert S_v = 16") becomes *enforceable* — a Falsified proposition's
CID sits in the known set, so the dedup gate **blocks re-assertion at intake**
rather than depending on authors reading a ledger.

## gnosis's typed domains are witness routes

The deepest unification: gnosis's four domains each pair a *canonical form*
with an *independent invariant* —

| domain | canonical form (→ Identity) | invariant (→ Witness) |
|---|---|---|
| rational | reduced num/den | value by division |
| units | SI base magnitude + dim vector | the dimension vector (homogeneity) |
| polynomial | expanded sorted monomials | Schwartz–Zippel fingerprint (GF(2⁶¹−1)) |
| boolean | ROBDD | truth table |

That pair **is** canon.d's I/W split. Porting a gnosis domain = writing a schema
whose identity fields carry the canonical form and whose witness field carries
the invariant; `cross_audit` then does what each `certify()` does — with
provenance, portability, and the constitution behind it. The Mars-Climate-
Orbiter catch (lbf ≠ N) arrives as a schema, not a library.

## Integration strategy: build separately, then update the pin

harmonics already pins one submodule — `ket` at its current HEAD (`3530ad5`,
the commit that gave ket-dag the grounds/derives/proposes edge kinds) — and
regenerates `docs/derivation-graph.json` via CI bot commits on every PR. The
intake pipeline does **not** replace `build_derivation_graph.py` in-repo on
day one; it lands in three steps:

1. **Build in canon.d.** Units 1–2 below, plus a `--emit-graph-json` compat
   output matching harmonics' current `derivation-graph.json` shape, so the
   scorecard and docs surfaces never notice the engine swap.
2. **Shadow mode.** harmonics CI runs *both* pipelines and publishes the diff
   as an artifact — the W1 measurement (edges regex missed, edges it
   hallucinated) quantified before anything is replaced.
3. **Cutover = a submodule update.** canon.d is pinned alongside ket — at a
   tagged release identified by its constitution root (`RELEASE.md`), never a
   floating branch — and `regen-derivation-graph.yml` swaps the python script
   for `canon-demo intake-corpus`.

**The regex is demoted, not deleted**: `build_derivation_graph.py` becomes one
structurer route among N. Fast, deterministic, free — where it agrees it adds
corroboration; where it disagrees, `structurer_disagreement_rate` measures its
error rate continuously instead of anyone guessing it.

**The graph is re-grounded, not adapted.** harmonics' derivation graph is
*not* a Merkle DAG: 308 nodes, 221 with `cid: null`, and every edge is
name-addressed (`{"target": "FRAMEWORK_TOPOLOGY", "kind": "references"}`) —
edges never commit to their target's content, and the file is regenerated
wholesale (its only version stamp is a git sha). There is no hash structure to
translate. The port instead **re-grounds**: pin a corpus revision, seal every
node, re-resolve each name edge to a CID→CID edge at that revision. The
resolution step is fallible (renames, W4 typos, deletions) and runs through
intake like everything else — failures become needs_review entries, not silent
drops. Edge *kinds* must be assigned too: the live graph is dominated by the
regex catch-all `references`, so promoting an edge to grounds/derives/proposes
is a structurer judgment under the same N-route audit. ket's CAS still helps
at the *byte* layer — the 87 already-sealed nodes arrive as prior utterance
attestations to corroborate against — but the topology is built natively in
canon.d; there is nothing upstream to anchor it to.

## Units of work

1. **Intake spine** *(bridge.rs exists — extend + surface)*: `canon-demo intake
   <file.md>` — seal utterance, run one structurer, emit proposition +
   assertion + needs_review list, `--emit-graph-json` compat output. Exit
   codes per the U5 contract.
2. **N-structurer cross-audit + `intake-corpus` workflow**: an invokable
   workflow (sibling of `seal-constitution`) that fans out N structurer agents
   per utterance, auto-promotes agreement, queues disagreement with evidence.
   First corpus: harmonics' `sync_cost/derivations/*.md` (~220 files).
3. **Lineage re-grounding** *(not a port — see above)*: at a pinned corpus
   revision, seal all nodes, resolve name edges → CID→CID `lineage.rs` edges,
   assign kinds (most are the regex catch-all `references` today); unresolvable
   names and kind judgments land in needs_review; closure import makes
   dangling targets loud.
4. **Reconcile scorecard**: MANIFEST.yml rows → attestation/proposition pairs →
   `reconcile_gate`; the scorecard becomes generated output; Falsified CIDs
   feed the dedup-block.
5. **Domain witnesses**: port gnosis units (first — cheapest, highest
   catch-rate), then polynomial and boolean as schema+witness pairs.

Telemetry (extends `RELEASE.md` §2): `structurer_disagreement_rate` (the
extraction-quality canary), `utterance_coverage` (sealed-but-unstructured
backlog), `needs_review_depth`, `corroboration_distribution`, and
`reassertion_blocks` (negative-ledger enforcement firing).

What stays human (H4, named not ambient): resolving structurer disagreement,
trusting source attestations, choosing N and the promotion threshold.
