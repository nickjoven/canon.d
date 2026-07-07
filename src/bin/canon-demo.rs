//! canon-demo — the demo scenarios and the intake surface as a CLI.
//!
//!   cargo run --bin canon-demo -- <skip|drift|verify|all> [--json]
//!   cargo run --bin canon-demo -- intake <file.md> [--json] [--annotator ID]
//!   cargo run --bin canon-demo -- intake-corpus <dir> [--json] [--annotator ID]
//!                                 [--emit-graph-json <path>] [--pack prose|code]
//!
//! Human-readable by default; `--json` emits one machine-readable object per
//! command (the same numbers the web page and deck render). Exit codes follow
//! the U5 contract everywhere: 0 = clean, 1 = findings (needs_review/blocked/
//! gate violation), 2 = environment error — so every command drops straight
//! into CI.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::exit;

use canon_d::intake::{intake_corpus_with_routes, prose_routes, Structurer};
use canon_d::packs::code::ImportsRoute;
use canon_d::Canonicalizer;
use canon_d::{
    attestation_schema, consensus_root, corpus_graph, cross_audit, dedup_gate, export,
    gate::Candidate, import, intake, project, proposition_schema, reconcile_gate,
    seal_constitution, Bundle, BundleEntry, Claim, CrossAuditConflict, Disposition, IntakeConfig,
    IntakeReport, Memo, Quantum, Rat, RatInterval, Rule, SchemaKind, Tolerance, TransparencyLog,
};
use serde_json::{json, Value};

fn seal_ratio(subject: &str, n: i64, d: i64) -> Quantum {
    Quantum::seal(
        &proposition_schema(),
        &json!({"subject":subject,"num":n,"den":d,"value":Rat::new(n,d).unwrap().reduced_string()}),
    )
    .unwrap()
}
fn point(n: i64, d: i64) -> RatInterval {
    RatInterval::point(Rat::new(n, d).unwrap())
}
fn short(c: &str) -> &str {
    &c[..16.min(c.len())]
}

// ---------------------------------------------------------------------------
// skip — dedup by entailment, not string/embedding match.
// ---------------------------------------------------------------------------
fn demo_skip() -> Value {
    let omega = seal_ratio("omega_lambda", 13, 19);
    let known_cids: BTreeSet<String> = [omega.cid.clone()].into_iter().collect();
    let known_claims = vec![Claim {
        cid: omega.cid.clone(),
        interval: point(13, 19),
    }];

    let candidates = vec![
        Candidate {
            label: "Ω_Λ (exact, re-requested)".into(),
            cid: omega.cid.clone(),
            interval: point(13, 19),
        },
        Candidate {
            label: "Ω_Λ ∈ (0.6, 0.7) (a coarse bound)".into(),
            cid: "would-be-cid-coarse".into(),
            interval: RatInterval::new(Rat::new(3, 5).unwrap(), Rat::new(7, 10).unwrap()).unwrap(),
        },
        Candidate {
            label: "Ω_b = 1/19 (genuinely new)".into(),
            cid: "would-be-cid-omega-b".into(),
            interval: point(1, 19),
        },
    ];

    let n = candidates.len();
    let report = dedup_gate(&candidates, &known_cids, &known_claims);

    let items: Vec<Value> = report
        .items
        .iter()
        .map(|(label, disp)| {
            let (verdict, reason) = match disp {
                Disposition::Known { .. } => ("SKIP", "memo hit (exact match)"),
                Disposition::Entailed { .. } => ("SKIP", "entailed by a stronger fact"),
                Disposition::Novel => ("CALL", "genuinely new"),
            };
            json!({"label": label, "verdict": verdict, "reason": reason})
        })
        .collect();

    json!({
        "demo": "skip",
        "title": "Skip the call — dedup by entailment",
        "failure": "Compositional (two-hop) reasoning failure",
        "cite": "Song, Han & Goodman 2026, §4.1",
        "quote": "systematic failures in basic two-hop reasoning — combining only two facts",
        "requested": n,
        "calls_avoided": report.skipped,
        "remote_calls": report.novel.len(),
        "items": items,
        "exit": 0,
    })
}

// ---------------------------------------------------------------------------
// drift — corroboration vs. canonicalizer bug, told apart exactly.
// ---------------------------------------------------------------------------
fn demo_drift() -> Value {
    let schema = proposition_schema();

    let a = seal_ratio("omega_lambda", 13, 19);
    let b = seal_ratio("omega_lambda", 13, 19);
    let same_cid = a.cid == b.cid;
    let corroborated_cid = a.cid.clone();
    let clean = cross_audit(&schema, &[a, b]).unwrap();

    let good = seal_ratio("omega_lambda", 13, 19);
    let buggy = Quantum::seal(
        &schema,
        &json!({"subject":"omega_lambda","num":26,"den":38,"value":"13/19"}),
    )
    .unwrap();
    let good_cid = good.cid.clone();
    let buggy_cid = buggy.cid.clone();
    let conflicts = cross_audit(&schema, &[good, buggy]).unwrap();

    let undermerge: Vec<Value> = conflicts
        .iter()
        .filter_map(|c| match c {
            CrossAuditConflict::UnderMerge { witness, cids } => {
                Some(json!({"witness": witness, "forms": cids.len()}))
            }
            _ => None,
        })
        .collect();

    json!({
        "demo": "drift",
        "title": "No silent drift — corroboration vs. a canonicalizer bug",
        "failure": "Framing-effect / surface-form robustness failure",
        "cite": "Song, Han & Goodman 2026, §3.1 & §4.2",
        "quote": "logically equivalent but differently phrased prompts can lead to different results",
        "corroboration": {
            "same_cid": same_cid,
            "cid": short(&corroborated_cid),
            "conflicts": clean.len(),
        },
        "canonicalizer_bug": {
            "reduced_cid": short(&good_cid),
            "unreduced_cid": short(&buggy_cid),
            "undermerge": undermerge,
        },
        "exit": 0,
    })
}

// ---------------------------------------------------------------------------
// verify — verify a fact you didn't compute; then validate it against reality.
// ---------------------------------------------------------------------------
fn demo_verify() -> Value {
    let anchor = Quantum::seal(
        &attestation_schema(),
        &json!({"instrument":"Planck","dataset":"2018","locator":"Omega_Lambda",
                "value":0.6847,"uncertainty":0.0073,"vouched_by":"nick"}),
    )
    .unwrap();
    let program = json!({"walk":"LRRLLLLL"});
    let mut memo = Memo::new();
    let proj = project(&mut memo, &program, &[], "omega_lambda", "harmonics").unwrap();

    let bundle = export(
        vec![
            BundleEntry::of(SchemaKind::Attestation, &anchor),
            BundleEntry::of(SchemaKind::Generator, &proj.generator),
            BundleEntry::of(SchemaKind::Proposition, &proj.proposition),
            BundleEntry::of(SchemaKind::Assertion, &proj.assertion),
        ],
        vec![anchor.cid.clone()],
        vec![
            Rule {
                head: proj.generator.cid.clone(),
                body: vec![anchor.cid.clone()],
            },
            Rule {
                head: proj.proposition.cid.clone(),
                body: vec![proj.generator.cid.clone()],
            },
        ],
        &TransparencyLog::new(),
        vec![],
    );

    let wire = serde_json::to_string(&bundle).unwrap();
    let bundle_bytes = wire.len();
    let root = bundle.consensus_root.clone();

    let received: Bundle = serde_json::from_str(&wire).unwrap();
    let loaded = import(&received).expect("verified");
    let agree_one_hash = consensus_root(&loaded.closure) == bundle.consensus_root;
    let certain = loaded.closure.is_certain(&proj.proposition.cid);

    // tamper one field → rejected
    let mut forged = received.clone();
    forged.entries[2].body["value"] = json!("99/1");
    let tamper_rejected = import(&forged).is_err();

    // empirical: imported fact vs the measurement
    let omega = loaded.certain_fact(&proj.proposition.cid).unwrap();
    let ok = reconcile_gate(&[(omega, &anchor)], &Tolerance::default()).unwrap();

    // a wrong derived value → Falsified → blocked
    let wrong = Quantum::seal(
        &proposition_schema(),
        &json!({"subject":"omega_lambda","num":1,"den":2,"value":"1/2"}),
    )
    .unwrap();
    let bad = reconcile_gate(&[(&wrong, &anchor)], &Tolerance::default()).unwrap();

    json!({
        "demo": "verify",
        "title": "Verify what you didn't compute",
        "failure": "Self-assessment failure",
        "cite": "Song, Han & Goodman 2026, §4.3",
        "quote": "LLMs struggle even in assessing reasoning process … an arguably easier task than generation",
        "bundle_bytes": bundle_bytes,
        "consensus_root": short(&root),
        "agree_one_hash": agree_one_hash,
        "certain_without_computing": certain,
        "tamper_rejected": tamper_rejected,
        "reconcile_good": {
            "label": ok.verdicts[0].1.label(),
            "sigma": ok.verdicts[0].1.z(),
            "outcome": format!("{:?}", ok.outcome),
        },
        "reconcile_bad": {
            "label": bad.verdicts[0].1.label(),
            "sigma": bad.verdicts[0].1.z(),
            "outcome": format!("{:?}", bad.outcome),
            "exit": bad.outcome.exit_code(),
        },
        "exit": bad.outcome.exit_code(),
    })
}

// ---------------------------------------------------------------------------
// constitution — the substrate seals its own laws (self-hosting tier).
// ---------------------------------------------------------------------------
fn demo_constitution() -> Value {
    let c = seal_constitution();
    let self_hosts = c.self_hosts();

    let articles: Vec<Value> = c
        .articles
        .iter()
        .map(|a| {
            json!({
                "law": a.name,
                "schema_cid": short(&a.schema_cid),
                "quantum_cid": short(&a.quantum.cid),
            })
        })
        .collect();

    // Ship the law and re-derive its root, having sealed nothing: the treaty.
    let bundle = c.bundle();
    let wire = serde_json::to_string(&bundle).unwrap();
    let received: Bundle = serde_json::from_str(&wire).unwrap();
    let imported = import(&received)
        .map(|l| consensus_root(&l.closure) == c.root)
        .unwrap_or(false);

    let ok = self_hosts && imported;
    json!({
        "demo": "constitution",
        "title": "The substrate seals its own laws",
        "failure": "Ungrounded self-description — a system that can't state, under its own rules, the rules it computed under",
        "self_hosts": self_hosts,
        "keystone_cid": short(&c.meta.cid),
        "constitution_root": short(&c.root),
        "laws": c.articles.len(),
        "articles": articles,
        "treaty": {
            "wire_version": c.treaty.wire_version,
            "body_digest_algo": c.treaty.body_digest_algo,
            "canon_rule_cid": short(&c.treaty.canon_rule_cid),
        },
        "bundle_bytes": wire.len(),
        "portable_root_reproduced": imported,
        "exit": if ok { 0 } else { 1 },
    })
}

fn print_constitution(v: &Value) {
    println!("\x1b[1mThe substrate seals its own laws\x1b[0m — self-hosting tier\n");
    println!(
        "Keystone (meta-schema, sealed under itself): {}",
        v["keystone_cid"].as_str().unwrap()
    );
    println!(
        "  self-hosts (fixpoint · articles reverify · witness route · rebuild-bit-identical)? {}",
        v["self_hosts"]
    );
    println!(
        "\nLaws sealed as quanta under the meta-schema: {}",
        v["laws"]
    );
    for a in v["articles"].as_array().unwrap() {
        println!(
            "  {:<16} schema {}  →  quantum {}",
            a["law"].as_str().unwrap(),
            a["schema_cid"].as_str().unwrap(),
            a["quantum_cid"].as_str().unwrap()
        );
    }
    let t = &v["treaty"];
    println!("\nTreaty (the inter-substrate contract):");
    println!(
        "  wire v{}, body digest {}",
        t["wire_version"],
        t["body_digest_algo"].as_str().unwrap()
    );
    println!(
        "  canonicalizer fingerprint: {}",
        t["canon_rule_cid"].as_str().unwrap()
    );
    println!(
        "\nConstitution root: {}",
        v["constitution_root"].as_str().unwrap()
    );
    println!(
        "  shipped a {}-byte bundle; a peer re-derived the root sealing nothing? {}",
        v["bundle_bytes"], v["portable_root_reproduced"]
    );
}

// ---------------------------------------------------------------------------
// human-readable rendering
// ---------------------------------------------------------------------------
// lead every demo with the documented LLM failure it neutralizes.
fn print_failure_header(v: &Value) {
    println!(
        "\x1b[2mDocumented failure:\x1b[0m \x1b[1m{}\x1b[0m",
        v["failure"].as_str().unwrap()
    );
    println!("\x1b[2m  “{}”\x1b[0m", v["quote"].as_str().unwrap());
    println!(
        "\x1b[2m  — {}  arXiv:2602.06176\x1b[0m\n",
        v["cite"].as_str().unwrap()
    );
}

fn print_skip(v: &Value) {
    print_failure_header(v);
    let n = v["requested"].as_u64().unwrap();
    println!("\x1b[1mSkip the call\x1b[0m — the substrate does the two-hop step the model can't\n");
    println!("Agent wants {n} facts. Naively → {n} remote calls.\n");
    for it in v["items"].as_array().unwrap() {
        let pad = format!("{:<28}", it["reason"].as_str().unwrap());
        println!(
            "  {}  {}  {}",
            it["verdict"].as_str().unwrap(),
            pad,
            it["label"].as_str().unwrap()
        );
    }
    println!(
        "\n→ {} of {n} calls avoided. Remote loops: {} (not {n}).",
        v["calls_avoided"], v["remote_calls"]
    );
}

fn print_drift(v: &Value) {
    print_failure_header(v);
    println!("\x1b[1mNo silent drift\x1b[0m — equivalent-but-rephrased can't diverge here\n");
    let c = &v["corroboration"];
    println!("Two agents reach Ω_Λ = 13/19 by different paths:");
    println!(
        "  same CID? {}  → ONE node, corroborated (not a conflict)",
        c["same_cid"]
    );
    println!("  cross-audit conflicts: {}\n", c["conflicts"]);
    let b = &v["canonicalizer_bug"];
    println!("Agent C ships a bug: leaves 26/38 unreduced:");
    println!("  13/19 → {}", b["reduced_cid"].as_str().unwrap());
    println!(
        "  26/38 → {}  (different address — looks brand-new)",
        b["unreduced_cid"].as_str().unwrap()
    );
    for u in b["undermerge"].as_array().unwrap() {
        println!(
            "  ⚠ UnderMerge: one value {} resolved to {} forms → bug CAUGHT.",
            u["witness"].as_str().unwrap(),
            u["forms"]
        );
    }
}

fn print_verify(v: &Value) {
    print_failure_header(v);
    println!("\x1b[1mVerify what you didn't compute\x1b[0m — assessment, externalized\n");
    println!(
        "Party A ships a {}-byte bundle (root {}).",
        v["bundle_bytes"],
        v["consensus_root"].as_str().unwrap()
    );
    println!("Party B imported it — re-verified every quantum, replayed the log.");
    println!(
        "  agree on the whole state by ONE hash? {}",
        v["agree_one_hash"]
    );
    println!(
        "  Ω_Λ certain in B (B computed nothing)? {}",
        v["certain_without_computing"]
    );
    println!(
        "  flip one field → import rejected? {}\n",
        v["tamper_rejected"]
    );
    let g = &v["reconcile_good"];
    println!("Reconcile 13/19 vs Planck 0.6847 ± 0.0073:");
    println!(
        "  {} at {:.3}σ → gate {}",
        g["label"].as_str().unwrap(),
        g["sigma"].as_f64().unwrap(),
        g["outcome"].as_str().unwrap()
    );
    let bad = &v["reconcile_bad"];
    println!("Reconcile 1/2 vs Planck:");
    println!(
        "  {} at {:.1}σ → gate {} (exit {})",
        bad["label"].as_str().unwrap(),
        bad["sigma"].as_f64().unwrap(),
        bad["outcome"].as_str().unwrap(),
        bad["exit"]
    );
}

// ---------------------------------------------------------------------------
// intake — the Unit-1 surface (INTAKE.md). Exit codes: 0 clean, 1 findings,
// 2 environment error (missing file/dir, unreadable input, unwritable output).
// ---------------------------------------------------------------------------

/// `id` for a doc path: the file stem, matching harmonics' node-id convention.
fn doc_id(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
}

/// Positional arguments: everything that is neither a flag nor a value
/// consumed by a value-taking flag. `positionals(&args)[0]` is the subcommand.
fn positionals(args: &[String]) -> Vec<&str> {
    const VALUE_FLAGS: [&str; 3] = ["--annotator", "--emit-graph-json", "--pack"];
    let mut out = Vec::new();
    let mut skip = false;
    for a in args {
        if skip {
            skip = false;
            continue;
        }
        if VALUE_FLAGS.contains(&a.as_str()) {
            skip = true;
            continue;
        }
        if a.starts_with("--") {
            continue;
        }
        out.push(a.as_str());
    }
    out
}

fn print_intake_report(r: &IntakeReport) {
    println!(
        "\x1b[1m{}\x1b[0m  utterance {}  (canonicalizer: {})",
        r.doc_id,
        short(&r.utterance_cid),
        r.canonicalizer
    );
    for e in &r.edges {
        println!(
            "  edge     {} \x1b[2m--{}->\x1b[0m {}   annotation {}",
            e.from,
            e.kind,
            e.to,
            short(&e.annotation_cid)
        );
    }
    if !r.references.is_empty() {
        println!(
            "  refs     {} untyped (reported, not sealed)",
            r.references.len()
        );
    }
    for p in &r.propositions {
        println!(
            "  claim    {} = {}   proposition {}  assertion {}",
            p.subject,
            p.witness,
            short(&p.proposition_cid),
            short(&p.assertion_cid)
        );
    }
    for b in &r.blocked {
        println!(
            "  \x1b[31mBLOCKED\x1b[0m  {} — re-asserts Falsified {}",
            b.subject,
            short(&b.proposition_cid)
        );
    }
    for n in &r.needs_review {
        println!(
            "  \x1b[33mREVIEW\x1b[0m   [{}] {} — {}",
            n.kind, n.subject, n.detail
        );
    }
}

fn cmd_intake(args: &[String], json_out: bool) -> i32 {
    let Some(&path) = positionals(args).get(1) else {
        eprintln!("usage: canon-demo intake <file.md> [--json] [--annotator ID]");
        return 2;
    };
    let path = Path::new(path);
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {}: {e}", path.display());
            return 2;
        }
    };
    let mut cfg = IntakeConfig::new(flag_value(args, "--annotator").unwrap_or("canon-demo"));
    // Single-file mode: the corpus is the file's siblings, so lineage targets
    // and references resolve exactly as they would in a corpus run.
    if let Some(dir) = path.parent() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|x| x == "md") {
                    cfg.corpus_ids.insert(doc_id(&p));
                }
            }
        }
    }
    match intake(&doc_id(path), &text, &cfg) {
        Ok(r) => {
            if json_out {
                println!("{}", serde_json::to_string_pretty(&r).unwrap());
            } else {
                print_intake_report(&r);
            }
            r.exit_code()
        }
        Err(e) => {
            eprintln!("intake failed: {e}");
            2
        }
    }
}

fn cmd_intake_corpus(args: &[String], json_out: bool) -> i32 {
    let Some(&dir) = positionals(args).get(1) else {
        eprintln!("usage: canon-demo intake-corpus <dir> [--json] [--annotator ID] [--emit-graph-json <path>] [--pack prose|code]");
        return 2;
    };
    // Domain pack selection (DOMAINS.md): the pack decides which files are
    // corpus documents and which structurer routes run. The spine is shared.
    let pack = flag_value(args, "--pack").unwrap_or("prose");
    let (ext, routes): (&str, Vec<&dyn Structurer>) = match pack {
        "prose" => ("md", prose_routes().to_vec()),
        "code" => ("rs", vec![&ImportsRoute]),
        other => {
            eprintln!("unknown pack `{other}` (available: prose, code)");
            return 2;
        }
    };
    let mut docs: Vec<(String, String)> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("cannot read {dir}: {e}");
            return 2;
        }
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == ext) {
            match std::fs::read_to_string(&p) {
                Ok(t) => docs.push((doc_id(&p), t)),
                Err(err) => {
                    eprintln!("cannot read {}: {err}", p.display());
                    return 2;
                }
            }
        }
    }
    if docs.is_empty() {
        eprintln!("no .{ext} documents under {dir}");
        return 2;
    }
    let mut cfg = IntakeConfig::new(flag_value(args, "--annotator").unwrap_or("canon-demo"));
    if pack == "code" {
        // The code pack's noise quotient is byte-identity (DOMAINS.md F2).
        cfg.canonicalizer = Canonicalizer::Identity;
    }
    let report = match intake_corpus_with_routes(&docs, &cfg, &routes) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("intake failed: {e}");
            return 2;
        }
    };
    if let Some(out) = flag_value(args, "--emit-graph-json") {
        let graph = corpus_graph(&report, dir);
        if let Err(e) = std::fs::write(out, serde_json::to_string_pretty(&graph).unwrap()) {
            eprintln!("cannot write {out}: {e}");
            return 2;
        }
    }
    if json_out {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        for r in &report.reports {
            print_intake_report(r);
        }
        for c in &report.cross_conflicts {
            println!(
                "\x1b[33mREVIEW\x1b[0m [corpus:{}] {} — {}",
                c.kind, c.subject, c.detail
            );
        }
        let t = &report.telemetry;
        println!("\n{} docs · {} sealed · {} unstructured · {} edges · {} refs · {} propositions · review depth {} · {} blocked",
            t.docs, t.utterances_sealed, t.unstructured, t.edges_promoted,
            t.references_untyped, t.propositions, t.needs_review_depth, t.reassertion_blocks);
    }
    report.exit_code()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let json_out = args.iter().any(|a| a == "--json");
    let which = positionals(&args).first().copied().unwrap_or("all");

    match which {
        "intake" => exit(cmd_intake(&args, json_out)),
        "intake-corpus" => exit(cmd_intake_corpus(&args, json_out)),
        _ => {}
    }

    let demos: Vec<Value> = match which {
        "skip" => vec![demo_skip()],
        "drift" => vec![demo_drift()],
        "verify" => vec![demo_verify()],
        "constitution" | "const" => vec![demo_constitution()],
        "all" => vec![demo_skip(), demo_drift(), demo_verify()],
        other => {
            eprintln!("unknown demo '{other}' — expected skip|drift|verify|constitution|all");
            exit(2);
        }
    };

    let worst = demos
        .iter()
        .map(|d| d["exit"].as_i64().unwrap_or(0))
        .max()
        .unwrap_or(0);

    if json_out {
        let out = if demos.len() == 1 {
            demos[0].clone()
        } else {
            json!(demos)
        };
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        for (i, d) in demos.iter().enumerate() {
            if i > 0 {
                println!("\n{}\n", "─".repeat(60));
            }
            match d["demo"].as_str().unwrap() {
                "skip" => print_skip(d),
                "drift" => print_drift(d),
                "verify" => print_verify(d),
                "constitution" => print_constitution(d),
                _ => {}
            }
        }
    }

    exit(worst as i32);
}
