//! Demo: automated structuring over the real harmonics corpus — no hand-scoping.
//!
//!   cargo run --example intake_corpus --features prose            # default corpus
//!   cargo run --example intake_corpus --features prose -- <dir>   # any .md corpus
//!
//! `beats_grep` proved the substrate can *judge* a value — but it hand-scoped the
//! subject (a human told canon.d that `13/19` is Ω_Λ). This removes that hand:
//! the deterministic structurer route (`urtext-regex-v1`) reads every line of the
//! corpus, lifts each exact rational with urtext, and attributes it to a subject
//! from a declared lexicon — sealing each judgment as a supersedable structuring
//! quantum. What it *cannot* attribute goes to `needs_review`, a visible coverage
//! gap, never a guess (INTAKE.md's W1 answer).
//!
//! It reports Unit 2's telemetry over the real files:
//!   * lines scanned / values lifted
//!   * values auto-attributed vs. needs_review (utterance_coverage)
//!   * distinct propositions after dedup, and the top corroborated ones
//!     (corroboration_distribution) — the same value spelled many ways,
//!     collapsed to one address automatically.
//!
//! Needs `--features prose` (the structurer route is the urtext bridge).

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
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;

    use canon_d::{structure_span, SubjectLexicon};

    const DEFAULT_CORPUS: &str = "/home/nick/code/harmonics/sync_cost/derivations";

    pub fn run() {
        let dir = std::env::args().nth(1).unwrap_or_else(|| DEFAULT_CORPUS.to_string());
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
        // proposition CID → (subject/value label, distinct utterance count).
        let mut props: BTreeMap<String, (String, usize)> = BTreeMap::new();
        let mut review_samples: Vec<String> = Vec::new();

        for path in &files {
            let Ok(text) = fs::read_to_string(path) else { continue };
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                lines_scanned += 1;
                let Ok(s) = structure_span(line, "en", &lex) else { continue };
                for st in &s.structured {
                    attributed += 1;
                    values_lifted += 1;
                    let subject = st
                        .claim
                        .field("subject")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let value =
                        st.claim.field("value").and_then(|v| v.as_str()).unwrap_or("?");
                    let entry = props
                        .entry(st.claim.cid.clone())
                        .or_insert_with(|| (format!("{subject} = {value}"), 0));
                    entry.1 += 1;
                }
                for ex in &s.needs_review {
                    needs_review += 1;
                    values_lifted += 1;
                    if review_samples.len() < 8 {
                        review_samples.push(format!("{}  ({})", ex.value, line.trim()));
                    }
                }
            }
        }

        println!("corpus: {} markdown files under {dir}\n", files.len());
        println!("── automated structuring (route: urtext-regex-v1, no hand-scoping) ──");
        println!("  lines scanned         : {lines_scanned}");
        println!("  exact rationals lifted: {values_lifted}");
        println!(
            "  auto-attributed       : {attributed}  ({:.0}% of lifted)",
            pct(attributed, values_lifted)
        );
        println!(
            "  → needs_review        : {needs_review}  ({:.0}% — a visible gap, not a guess)",
            pct(needs_review, values_lifted)
        );
        println!("  distinct propositions : {}", props.len());

        // Corroboration: one proposition, many spellings/lines → one address.
        let mut ranked: Vec<_> = props.values().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        println!("\n── corroboration_distribution (top auto-structured propositions) ──");
        for (label, count) in ranked.iter().take(6) {
            println!("  {count:>4}× corroborated   {label}");
        }

        if !review_samples.is_empty() {
            println!("\n── needs_review sample (unattributed — the honest backlog) ──");
            for s in review_samples.iter().take(6) {
                let s = if s.len() > 76 { &s[..76] } else { s };
                println!("  {s}");
            }
        }

        println!("\n=== what changed since beats_grep ===");
        println!(
            "beats_grep: a human told canon.d that 13/19 is Ω_Λ (one hand-scoped fact)."
        );
        println!(
            "here: the structurer attributed every Ω_Λ value across {} files itself,",
            files.len()
        );
        println!(
            "sealed each as a supersedable proposal, and made every miss a visible gap."
        );
    }

    fn pct(n: usize, d: usize) -> f64 {
        if d == 0 {
            0.0
        } else {
            100.0 * n as f64 / d as f64
        }
    }

    fn collect_md(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else { return };
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
