//! Demo: the substrate beats grep on a real corpus — measured, not claimed.
//!
//!   cargo run --example beats_grep --features prose            # default corpus
//!   cargo run --example beats_grep --features prose -- <dir>   # any .md corpus
//!
//! The question (from the harmonics work): can a content-addressed substrate
//! *retrieve and judge* a value that a flat corpus spells three different ways,
//! when grep structurally cannot? Ω_Λ appears as the exact fraction `13/19`, as
//! the Planck decimal `0.6847`, and as the truncated decimal `0.6842`. grep sees
//! three disjoint string sets and renders no judgment. The substrate lifts every
//! spelling to one exact rational (urtext, F2→F3) and reconciles it against the
//! measurement to an exact σ verdict (canon.d).
//!
//! Three acts, over the *real* files:
//!   ACT 1 — grep's ceiling: three spellings → three disjoint result sets.
//!   ACT 2 — urtext lifts each spelling to an exact rational (no f64 ever).
//!   ACT 3 — canon.d reconciles the concept to a σ verdict with an exit code.
//!
//! Needs `--features prose` (the urtext canonicalizer). Without it, this prints
//! how to re-run and exits.

#[cfg(not(feature = "prose"))]
fn main() {
    eprintln!(
        "this example needs the prose feature (it uses urtext):\n  \
         cargo run --example beats_grep --features prose -- [corpus-dir]"
    );
}

#[cfg(feature = "prose")]
fn main() {
    real::run();
}

#[cfg(feature = "prose")]
mod real {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::Path;

    use canon_d::{attestation_schema, proposition_schema, reconcile_gate, Quantum, Tolerance};
    use serde_json::json;
    use urtext::{regions, to_rational, RegionKind};

    const DEFAULT_CORPUS: &str = "/home/nick/code/harmonics/sync_cost/derivations";

    /// The three surface forms of Ω_Λ that a flat corpus actually carries.
    const FRACTION: &str = "13/19"; // the exact prediction
    const PLANCK_DEC: &str = "0.6847"; // the measurement, as a decimal
    const TRUNC_DEC: &str = "0.6842"; // the prediction, truncated to a decimal

    pub fn run() {
        let dir = std::env::args()
            .nth(1)
            .unwrap_or_else(|| DEFAULT_CORPUS.to_string());
        let docs = read_corpus(Path::new(&dir));
        if docs.is_empty() {
            eprintln!("no .md files under {dir} — pass a corpus dir as the first argument");
            std::process::exit(2);
        }
        println!("corpus: {} markdown files under {dir}\n", docs.len());

        act1_grep_ceiling(&docs);
        act2_urtext_lifts();
        let exit = act3_canon_reconciles();

        println!("\n=== verdict ===");
        println!("grep: 3 disjoint string sets, zero judgment about whether they agree.");
        println!("substrate: one concept (13/19 exact), reconciled to an exact σ verdict.");
        std::process::exit(exit);
    }

    /// Read every `.md` file under `dir` as (path, contents). In-process so the
    /// demo is self-contained; a real ingest would stream.
    fn read_corpus(dir: &Path) -> Vec<(String, String)> {
        let mut out = Vec::new();
        collect_md(dir, &mut out);
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn collect_md(dir: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_md(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
                if let Ok(text) = fs::read_to_string(&path) {
                    out.push((path.display().to_string(), text));
                }
            }
        }
    }

    /// Docs (by path) whose text contains `needle` — the `grep -l` a knowledge
    /// worker would actually run.
    fn docs_containing<'a>(docs: &'a [(String, String)], needle: &str) -> BTreeSet<&'a str> {
        docs.iter()
            .filter(|(_, text)| text.contains(needle))
            .map(|(path, _)| path.as_str())
            .collect()
    }

    fn act1_grep_ceiling(docs: &[(String, String)]) {
        println!("── ACT 1 — grep's ceiling: three spellings, disjoint result sets ──");
        let frac = docs_containing(docs, FRACTION);
        let planck = docs_containing(docs, PLANCK_DEC);
        let trunc = docs_containing(docs, TRUNC_DEC);
        println!(
            "  grep '{FRACTION}'   → {:>3} docs   (the exact prediction, as a fraction)",
            frac.len()
        );
        println!(
            "  grep '{PLANCK_DEC}'  → {:>3} docs   (the Planck measurement, as a decimal)",
            planck.len()
        );
        println!(
            "  grep '{TRUNC_DEC}'  → {:>3} docs   (the prediction, truncated to a decimal)",
            trunc.len()
        );

        let planck_missed: Vec<_> = planck.difference(&frac).collect();
        let trunc_missed: Vec<_> = trunc.difference(&frac).collect();
        println!(
            "  docs with {PLANCK_DEC} that grep '{FRACTION}' MISSES: {}",
            planck_missed.len()
        );
        println!(
            "  docs with {TRUNC_DEC} that grep '{FRACTION}' MISSES: {}",
            trunc_missed.len()
        );
        println!("  → no single grep string retrieves the concept. This is the retrieval gap.\n");
    }

    fn act2_urtext_lifts() {
        println!("── ACT 2 — urtext lifts every spelling to an exact rational (no f64) ──");
        // Same value, two fraction spellings → one exact rational.
        for spelling in [FRACTION, "26/38", TRUNC_DEC, PLANCK_DEC] {
            match lift(spelling) {
                Some((reduced, n, d)) => {
                    println!("  {spelling:>7}  →  exact {reduced:<10} ({n}/{d})");
                }
                None => println!("  {spelling:>7}  →  (not an exact-rational Number region)"),
            }
        }
        println!(
            "  → 13/19 == 26/38 (unified); 0.6842 = 3421/5000 stays distinct (refuse-to-round).\n"
        );
    }

    /// Lift a numeric spelling through urtext: scan → first Number region →
    /// exact rational. Returns (reduced_string, numerator, denominator).
    fn lift(spelling: &str) -> Option<(String, i128, i128)> {
        let scanned = regions(&format!("{spelling}\n"));
        let region = scanned.iter().find(|r| r.kind == RegionKind::Number)?;
        let rat = to_rational(region)?;
        Some((rat.reduced_string(), rat.numerator(), rat.denominator()))
    }

    fn act3_canon_reconciles() -> i32 {
        println!("── ACT 3 — canon.d reconciles the concept to a σ verdict ──");

        // The grounding leaf: Planck 2018 Ω_Λ = 0.6847 ± 0.0073.
        let planck = Quantum::seal(
            &attestation_schema(),
            &json!({
                "instrument": "Planck", "dataset": "2018", "locator": "Omega_Lambda",
                "value": 0.6847, "uncertainty": 0.0073, "vouched_by": "harmonics",
            }),
        )
        .unwrap();

        // The prediction, lifted from its fraction spelling — proof the exact
        // value flows from urtext into the seal, not a hand-typed literal.
        let (_, n, d) = lift(FRACTION).expect("13/19 lifts");
        let prediction = seal_ratio(n as i64, d as i64);
        // A deliberately wrong value, to show the gate blocks (exit 1).
        let wrong = seal_ratio(1, 2);

        let tol = Tolerance::default();
        let report = reconcile_gate(&[(&prediction, &planck), (&wrong, &planck)], &tol).unwrap();

        for (i, (_cid, agreement)) in report.verdicts.iter().enumerate() {
            let (num, den) = if i == 0 { (n as i64, d as i64) } else { (1, 2) };
            println!(
                "  Ω_Λ = {num}/{den:<3} vs Planck 0.6847 ± 0.0073  →  {} at {:.3}σ",
                agreement.label(),
                agreement.z(),
            );
        }
        let exit = report.outcome.exit_code();
        println!(
            "  gate outcome: {:?}  (exit {exit})  — a wrong value is falsified, not merged.",
            report.outcome
        );
        exit
    }

    fn seal_ratio(num: i64, den: i64) -> Quantum {
        Quantum::seal(
            &proposition_schema(),
            &json!({
                "subject": "omega_lambda", "num": num, "den": den,
                "value": format!("{num}/{den}"),
            }),
        )
        .unwrap()
    }
}
