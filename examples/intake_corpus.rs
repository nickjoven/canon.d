//! Demo: automated structuring over the real harmonics corpus — no hand-scoping.
//!
//!   cargo run --example intake_corpus --features prose            # default corpus
//!   cargo run --example intake_corpus --features prose -- <dir>   # any .md corpus
//!
//! `beats_grep` proved the substrate can *judge* a value — but it hand-scoped the
//! subject (a human told canon.d that `13/19` is Ω_Λ). This removes that hand and
//! runs the full Unit 2 intake:
//!
//!   * **two independent structurer routes** (prefix `Ω_Λ = 13/19` and postfix
//!     `13/19 = Ω_Λ`) read every line, lift each exact rational with urtext, and
//!     attribute it to a subject from a declared lexicon — sealing each judgment
//!     as a supersedable structuring quantum;
//!   * the **cross-audit** groups the routes' proposals by proposition, and the
//!     **domain witness** (`Ω_Λ ∈ (0,1)`) promotes the in-domain facts while
//!     queuing the `Ω_Λ = 12`-style residual for review — a value law, not a
//!     corpus label;
//!   * what no route could attribute goes to `needs_review`, a visible coverage
//!     gap, never a guess (INTAKE.md's W1 answer).
//!
//! It reports Unit 2's telemetry over the real files: coverage, promoted vs.
//! queued, and each promoted fact's corroboration and the routes that agreed.
//!
//! Needs `--features prose` (the structurer routes are the urtext bridge).

#[cfg(not(feature = "prose"))]
fn main() {
    eprintln!(
        "this example needs the prose feature (it uses urtext):\n  \
         cargo run --example intake_corpus --features prose -- [corpus-dir]"
    );
}

#[cfg(feature = "prose")]
fn main() {
    real::run();
}

#[cfg(feature = "prose")]
mod real {
    use std::fs;
    use std::path::Path;

    use canon_d::{
        cross_audit_routes, structure_span_with, PromotionPolicy, Route, SubjectDomains,
        SubjectLexicon,
    };

    const DEFAULT_CORPUS: &str = "/home/nick/code/harmonics/sync_cost/derivations";

    pub fn run() {
        let dir = std::env::args()
            .nth(1)
            .unwrap_or_else(|| DEFAULT_CORPUS.to_string());
        let mut files = Vec::new();
        collect_md(Path::new(&dir), &mut files);
        files.sort();
        if files.is_empty() {
            eprintln!("no .md files under {dir} — pass a corpus dir as the first argument");
            std::process::exit(2);
        }
        let lex = SubjectLexicon::harmonics();

        let mut lines_scanned = 0usize;
        let mut values_lifted = 0usize;
        let mut attributed = 0usize;
        let mut needs_review = 0usize;
        // Every sealed structuring from BOTH routes, fed to the cross-audit.
        let mut structurings = Vec::new();
        let mut review_samples: Vec<String> = Vec::new();

        for path in &files {
            let Ok(text) = fs::read_to_string(path) else {
                continue;
            };
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                lines_scanned += 1;
                // Two independent routes read each line; where they agree, the
                // cross-audit will see the corroboration.
                for route in [Route::Prefix, Route::Postfix] {
                    let Ok(s) = structure_span_with(line, "en", &lex, route) else {
                        continue;
                    };
                    attributed += s.structured.len();
                    values_lifted += s.structured.len() + s.needs_review.len();
                    structurings.extend(s.structured);
                    for ex in &s.needs_review {
                        needs_review += 1;
                        if route == Route::Prefix && review_samples.len() < 8 {
                            review_samples.push(format!("{}  ({})", ex.value, line.trim()));
                        }
                    }
                }
            }
        }

        // Unit 2 proper: cross-audit the two routes and apply the domain witness.
        let report = cross_audit_routes(
            &structurings,
            &SubjectDomains::harmonics(),
            &PromotionPolicy::default(),
        );

        println!("corpus: {} markdown files under {dir}\n", files.len());
        println!("── automated structuring — 2 routes (prefix + postfix), no hand-scoping ──");
        println!("  lines scanned         : {lines_scanned}");
        println!("  exact rationals lifted: {values_lifted}  (across both routes)");
        println!(
            "  auto-attributed       : {attributed}  ({:.1}% of lifted)",
            pct(attributed, values_lifted)
        );
        println!(
            "  → needs_review        : {needs_review}  ({:.1}% — a visible gap, not a guess)",
            pct(needs_review, values_lifted)
        );

        println!("\n── cross-audit: domain witness (Ω_Λ ∈ (0,1)) + corroboration ──");
        println!("  distinct propositions : {}", report.considered());
        println!("  PROMOTED (in-domain)  : {}", report.promoted.len());
        println!("  QUEUED   (needs human): {}", report.queued.len());

        println!("\n── promoted facts (corroboration × routes) ──");
        for p in report.promoted.iter().take(6) {
            println!(
                "  {:>4}×  {} = {}   [routes: {}]",
                p.corroboration,
                p.subject,
                p.value,
                p.routes.len()
            );
        }

        if !report.queued.is_empty() {
            println!("\n── queued by the domain witness (the residual, now caught) ──");
            for q in report.queued.iter().take(6) {
                println!(
                    "  {:>4}×  {} = {}   — {}",
                    q.corroboration, q.subject, q.value, q.reason
                );
            }
        }

        if !review_samples.is_empty() {
            println!("\n── needs_review sample (unattributed — the honest backlog) ──");
            for s in review_samples.iter().take(5) {
                let s: String = s.chars().take(74).collect();
                println!("  {s}");
            }
        }

        println!("\n=== what changed since beats_grep ===");
        println!("beats_grep: a human told canon.d that 13/19 is Ω_Λ (one hand-scoped fact).");
        println!(
            "here: two independent routes attributed Ω_Λ across {} files themselves,",
            files.len()
        );
        println!("the domain witness caught the Ω_Λ = 12 residual, and every promoted fact");
        println!("carries its corroboration and the routes that agreed — nothing anonymous.");
    }

    fn pct(n: usize, d: usize) -> f64 {
        if d == 0 {
            0.0
        } else {
            100.0 * n as f64 / d as f64
        }
    }

    fn collect_md(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_md(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
                out.push(path);
            }
        }
    }
}
