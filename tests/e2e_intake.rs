//! e2e: the intake CLI honors the U5 exit-code contract and emits stable,
//! machine-readable output. Drives the real binary (CARGO_BIN_EXE), the same
//! way CI will.

use std::path::Path;
use std::process::{Command, Output};

fn canon_demo(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_canon-demo"))
        .args(args)
        .output()
        .expect("binary runs")
}

fn fixtures(sub: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(sub)
        .to_string_lossy()
        .into_owned()
}

#[test]
fn clean_corpus_exits_zero_with_full_telemetry() {
    let out = canon_demo(&["intake-corpus", &fixtures("intake"), "--json"]);
    assert_eq!(out.status.code(), Some(0), "clean corpus is exit 0");

    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    let t = &v["telemetry"];
    assert_eq!(t["docs"], 2);
    assert_eq!(
        t["utterances_sealed"], 2,
        "sealing is the entry ticket — every doc"
    );
    assert_eq!(
        t["edges_promoted"], 1,
        "baryon_fraction --grounds-> klein_bottle"
    );
    assert_eq!(t["propositions"], 1, "omega_b = 1/19");
    assert_eq!(t["needs_review_depth"], 0);

    // klein_bottle has no structure of its own: backlog telemetry, not failure.
    assert_eq!(t["unstructured"], 1);
}

#[test]
fn w4_typo_exits_one_and_names_the_dangling_target() {
    let out = canon_demo(&["intake-corpus", &fixtures("intake_findings"), "--json"]);
    assert_eq!(out.status.code(), Some(1), "a queued finding is exit 1");

    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let review = &v["reports"][0]["needs_review"][0];
    assert_eq!(review["kind"], "unresolved_lineage_target");
    assert_eq!(
        review["subject"], "klein_bottel",
        "the typo arrives named, not dropped"
    );
}

#[test]
fn single_file_intake_resolves_against_siblings() {
    let file = format!("{}/baryon_fraction.md", fixtures("intake"));
    let out = canon_demo(&["intake", &file, "--json", "--annotator", "e2e"]);
    assert_eq!(out.status.code(), Some(0));

    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        v["edges"][0]["to"], "klein_bottle",
        "sibling docs form the corpus"
    );
    assert_eq!(v["propositions"][0]["witness"], "1/19");
    assert_eq!(v["utterance_cid"].as_str().unwrap().len(), 64);
}

#[test]
fn missing_input_is_environment_error_two() {
    assert_eq!(
        canon_demo(&["intake", "/nonexistent/x.md"]).status.code(),
        Some(2)
    );
    assert_eq!(
        canon_demo(&["intake-corpus", "/nonexistent"]).status.code(),
        Some(2)
    );
    assert_eq!(
        canon_demo(&["intake"]).status.code(),
        Some(2),
        "usage error is env class"
    );
}

#[test]
fn prior_heads_roundtrip_across_two_runs() {
    // The cross-run head chain (#6), driven through the real binary: run N's
    // --json report is run N+1's --prior-heads. Two intakes over a temp
    // corpus, one file edited between them — the edited doc must supersede,
    // the stable doc must stay silent.
    let dir = std::env::temp_dir().join("canon_e2e_prior_heads");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("edited.md"), "x = 1/3\n").unwrap();
    std::fs::write(dir.join("stable.md"), "y = 1/2\n").unwrap();
    let dir_s = dir.to_string_lossy().into_owned();

    let run1 = canon_demo(&["intake-corpus", &dir_s, "--json"]);
    assert_eq!(run1.status.code(), Some(0));
    let v1: serde_json::Value = serde_json::from_slice(&run1.stdout).unwrap();
    assert_eq!(v1["telemetry"]["heads_superseded"], 0, "first run: no chain");
    let report_path = std::env::temp_dir().join("canon_e2e_prior_heads_run1.json");
    std::fs::write(&report_path, &run1.stdout).unwrap();
    let report_s = report_path.to_string_lossy().into_owned();

    std::fs::write(dir.join("edited.md"), "x = 1/4\n").unwrap();
    let run2 = canon_demo(&["intake-corpus", &dir_s, "--json", "--prior-heads", &report_s]);
    assert_eq!(run2.status.code(), Some(0), "a succession is not a finding");
    let v2: serde_json::Value = serde_json::from_slice(&run2.stdout).unwrap();

    assert_eq!(v2["telemetry"]["heads_superseded"], 1);
    let find = |v: &serde_json::Value, id: &str| -> serde_json::Value {
        v["reports"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["doc_id"] == id)
            .unwrap()
            .clone()
    };
    let edited = find(&v2, "edited");
    let s = &edited["head_succession"];
    assert_eq!(
        s["old"],
        find(&v1, "edited")["utterance_cid"],
        "old = run 1's head, straight from the artifact"
    );
    assert_eq!(s["new"], edited["utterance_cid"], "new = run 2's re-seal");
    assert_eq!(
        s["cid"].as_str().unwrap().len(),
        64,
        "the succession is a sealed quantum, not report prose"
    );
    assert!(
        find(&v2, "stable").get("head_succession").is_none(),
        "unchanged doc emits nothing"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_file(&report_path);
}

#[test]
fn graph_emission_is_deterministic_and_compat_shaped() {
    let dir = fixtures("intake");
    let tmp = std::env::temp_dir().join("canon_e2e_graph.json");
    let tmp2 = std::env::temp_dir().join("canon_e2e_graph2.json");
    let tmp_s = tmp.to_string_lossy().into_owned();
    let tmp2_s = tmp2.to_string_lossy().into_owned();

    assert_eq!(
        canon_demo(&["intake-corpus", &dir, "--emit-graph-json", &tmp_s])
            .status
            .code(),
        Some(0)
    );
    assert_eq!(
        canon_demo(&["intake-corpus", &dir, "--emit-graph-json", &tmp2_s])
            .status
            .code(),
        Some(0)
    );
    let a = std::fs::read_to_string(&tmp).unwrap();
    let b = std::fs::read_to_string(&tmp2).unwrap();
    assert_eq!(
        a, b,
        "two runs, byte-identical graph — no clocks in the projection"
    );

    let g: serde_json::Value = serde_json::from_str(&a).unwrap();
    let nodes = g["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    let bf = nodes.iter().find(|n| n["id"] == "baryon_fraction").unwrap();
    assert_eq!(bf["sealed"], true);
    assert_eq!(bf["depends_on"][0], "klein_bottle");
    assert_eq!(bf["lineage"]["klein_bottle"], "grounds");
    let kb = nodes.iter().find(|n| n["id"] == "klein_bottle").unwrap();
    assert_eq!(kb["depended_on_by"][0], "baryon_fraction");

    let _ = std::fs::remove_file(tmp);
    let _ = std::fs::remove_file(tmp2);
}
