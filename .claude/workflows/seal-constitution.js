// seal-constitution — the invokable handle behind canon.d's self-hosting tier.
//
// Re-seals the substrate's constitution (its own laws, sealed as quanta under the
// meta-schema) and adversarially verifies that it still self-hosts: one skeptical
// verifier per constitutional invariant, then a single operator-facing verdict.
//
// Invoke:
//   Workflow({ name: 'seal-constitution' })
//   Workflow({ scriptPath: '/home/nick/code/canon.d/.claude/workflows/seal-constitution.js' })
//
// Re-run it whenever a law changes — a new constitution root is the change made
// visible. Mutates nothing in the repo; it only builds, runs, and tests.

export const meta = {
  name: 'seal-constitution',
  description: "Re-seal canon.d's constitution and adversarially verify it self-hosts",
  phases: [
    { title: 'Seal', detail: 'build + seal the constitution; capture root, keystone, treaty' },
    { title: 'Verify', detail: 'one skeptical verifier per constitutional invariant' },
    { title: 'Synthesize', detail: 'a single verdict — does the substrate host its own grammar?' },
  ],
}

const REPO = '/home/nick/code/canon.d'

const SEAL_SCHEMA = {
  type: 'object',
  required: ['keystone_cid', 'constitution_root', 'self_hosts', 'laws'],
  properties: {
    keystone_cid: { type: 'string' },
    constitution_root: { type: 'string' },
    self_hosts: { type: 'boolean' },
    laws: { type: 'number' },
    law_names: { type: 'array', items: { type: 'string' } },
    canon_rule_cid: { type: 'string' },
    bundle_bytes: { type: 'number' },
    portable_root_reproduced: { type: 'boolean' },
    exit: { type: 'number' },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['invariant', 'holds', 'evidence'],
  properties: {
    invariant: { type: 'string' },
    holds: { type: 'boolean' },
    evidence: { type: 'string' },
    omissions: { type: 'array', items: { type: 'string' } },
  },
}

// ---- Seal -----------------------------------------------------------------
phase('Seal')
const sealed = await agent(
  `In ${REPO}: run \`cargo build -q --bins 2>&1 | tail -5\`, then ` +
  `\`cargo run -q --bin canon-demo -- constitution --json\`. Parse the JSON the ` +
  `binary prints and report its fields verbatim — keystone_cid, constitution_root, ` +
  `self_hosts, laws, the article law-names (law_names), treaty.canon_rule_cid, ` +
  `bundle_bytes, portable_root_reproduced, exit. Recompute nothing; report exactly ` +
  `what the binary printed.`,
  { phase: 'Seal', schema: SEAL_SCHEMA },
)

if (!sealed) {
  log('Seal failed — the constitution did not build/run. Aborting.')
  return { ok: false, reason: 'seal-failed' }
}
log(`Sealed: root ${sealed.constitution_root}, keystone ${sealed.keystone_cid}, ${sealed.laws} laws, self_hosts=${sealed.self_hosts}`)

// ---- Verify ---------------------------------------------------------------
phase('Verify')
const INVARIANTS = [
  {
    key: 'fixpoint',
    ask:
      `Confirm the keystone is genuinely sealed under ITSELF. Run ` +
      `\`cd ${REPO} && cargo test -q --lib constitution::tests::keystone_is_its_own_law -- --exact 2>&1 | tail -8\`. ` +
      `Then read src/constitution.rs (schema_schema, schema_to_body, seal_schema) and confirm the ` +
      `meta-schema's sealed body IS its own description — i.e. seal_schema(schema_schema()) closes the loop, ` +
      `and the keystone's schema_cid equals schema_cid(schema_schema()). holds=true ONLY if the test passes ` +
      `AND the code genuinely closes the self-reference (not merely re-hashes bytes).`,
  },
  {
    key: 'determinism',
    ask:
      `Confirm rebuild-bit-identical. Run \`cd ${REPO} && cargo run -q --bin canon-demo -- constitution --json\` ` +
      `TWICE and confirm constitution_root and keystone_cid are byte-identical across both runs; also run ` +
      `\`cargo test -q --lib constitution::tests::rebuild_is_bit_identical -- --exact 2>&1 | tail -8\`. ` +
      `holds=false if the two runs disagree on either hash.`,
  },
  {
    key: 'portability',
    ask:
      `Confirm a peer re-derives the constitution root having sealed nothing. Run ` +
      `\`cd ${REPO} && cargo test -q --lib constitution::tests::constitution_is_portable_and_reproducible -- --exact 2>&1 | tail -8\` ` +
      `and confirm the seal report's portable_root_reproduced was true (${sealed.portable_root_reproduced}).`,
  },
  {
    key: 'tamper',
    ask:
      `Confirm a forged law cannot enter the substrate. Run ` +
      `\`cd ${REPO} && cargo test -q --lib constitution::tests::tampered_constitution_is_rejected -- --exact 2>&1 | tail -8\`. ` +
      `holds=true only if a tampered law body causes import to error.`,
  },
  {
    key: 'coverage',
    ask:
      `Is any law SILENTLY omitted from the constitution? In ${REPO}, grep every \`pub fn *_schema()\` ` +
      `constructor under src/ and compare against the laws listed in builtin_laws() in src/constitution.rs ` +
      `(the binary sealed ${sealed.laws}: ${(sealed.law_names || []).join(', ')}). List in \`omissions\` any ` +
      `schema constructor that exists but is NOT sealed. holds=true only if omissions is empty OR every ` +
      `omission is a deliberate non-law you can justify in one phrase.`,
  },
]

const verdicts = (await parallel(
  INVARIANTS.map((inv) => () =>
    agent(
      `Adversarially verify canon.d's constitution invariant "${inv.key}". ${inv.ask} ` +
      `Be skeptical: if anything is inconclusive or the command errors, holds=false. ` +
      `Return invariant="${inv.key}", holds, a one-sentence evidence string citing the command output.`,
      { label: `verify:${inv.key}`, phase: 'Verify', schema: VERDICT_SCHEMA },
    ),
  ),
)).filter(Boolean)

// ---- Synthesize -----------------------------------------------------------
phase('Synthesize')
const allHold = verdicts.length === INVARIANTS.length && verdicts.every((v) => v.holds)
const failed = verdicts.filter((v) => !v.holds).map((v) => v.invariant)

const report = await agent(
  `Synthesize the canon.d constitution verification for an operator.\n` +
  `Sealed: root ${sealed.constitution_root}, keystone ${sealed.keystone_cid}, ${sealed.laws} laws, ` +
  `self_hosts=${sealed.self_hosts}, treaty fingerprint ${sealed.canon_rule_cid}.\n` +
  `Verdicts: ${JSON.stringify(verdicts)}.\n` +
  `Write 4–6 sentences: does the substrate host its own grammar? State the constitution root, confirm ` +
  `whether every invariant held, and name any that failed with its evidence. End with exactly one line: ` +
  `"VERDICT: SELF-HOSTING" if all five held, otherwise "VERDICT: BROKEN (<failed invariants>)".`,
  { phase: 'Synthesize' },
)

return {
  ok: allHold,
  constitution_root: sealed.constitution_root,
  keystone_cid: sealed.keystone_cid,
  laws: sealed.laws,
  self_hosts: sealed.self_hosts,
  invariants_held: allHold,
  failed,
  verdicts,
  report,
}
