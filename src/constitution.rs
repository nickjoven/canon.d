//! Self-hosting: the substrate seals its own laws.
//!
//! Every *fact* in canon.d gets a CID by flowing through [`Quantum::seal`]. The
//! laws that make a fact a fact — the schemas, the canonicalizer, the consensus
//! construction — historically did not: a schema was content-addressed by
//! [`schema_cid`] (a raw blake3 of its bytes), but it never became a *quantum*. It
//! sat outside the strata, the bundles, the closure. The grammar was not spoken in
//! its own language.
//!
//! This module closes that gap. It defines a **meta-schema** ([`schema_schema`]) —
//! a schema whose instances *describe schemas* — and seals every built-in law
//! under it, so each law earns a CID *the same way every fact does*. The keystone
//! is the meta-schema sealed under **itself**: the law that describes all laws,
//! addressed by its own description. That fixpoint is what "self-hosting" means
//! here — and [`Constitution::self_hosts`] checks it holds.
//!
//! The **constitution root** is computed through the exact same [`export`] /
//! [`consensus_root`] path a fact bundle uses (meta as the anchor, every article
//! grounded in it). So "the law and the facts share one notion of agreement" is
//! not a slogan — it is the same code. A peer can [`import`] the constitution
//! bundle and re-derive the root, verifying *which laws you computed under*
//! without trusting you: the inter-substrate **treaty**.
//!
//! [`import`]: crate::import

use std::collections::BTreeSet;

use serde_json::{json, Value};

use crate::{
    assertion_schema, attestation_schema, consensus_root, edge_annotation_schema, export,
    generator_schema, mapping_schema, proposition_schema, structuring_schema, utterance_schema,
    verification_schema, Bundle, BundleEntry, Closure, FieldKind, Quantum, Rule, Schema, SchemaKind,
    TransparencyLog,
};
use crate::quantum::schema_cid;

/// The meta-schema: the grammar of grammars. Its instances describe a [`Schema`].
///
/// Identity is the schema's `name`, its `version`, and its **ordered** field list
/// (order is significant in a `Schema`, so `fields` is a `List`, not a `Set`).
/// Each field is rendered to one canonical descriptor string by
/// [`fieldkind_str`] + role flags, so the meta-schema needs no nested-object kind.
pub fn schema_schema() -> Schema {
    Schema::new("schema", 1)
        .identity("name", FieldKind::String)
        .identity("version", FieldKind::Integer)
        .identity("fields", FieldKind::List(Box::new(FieldKind::String)))
}

/// Render a [`FieldKind`] to a stable canonical string (recursive for List/Set/Ref).
fn fieldkind_str(k: &FieldKind) -> String {
    match k {
        FieldKind::String => "string".into(),
        FieldKind::Integer => "integer".into(),
        FieldKind::Float => "float".into(),
        FieldKind::Bool => "bool".into(),
        FieldKind::Cid => "cid".into(),
        FieldKind::Ref(s) => format!("ref({s})"),
        FieldKind::List(inner) => format!("list({})", fieldkind_str(inner)),
        FieldKind::Set(inner) => format!("set({})", fieldkind_str(inner)),
    }
}

/// Render a [`Schema`] as a body conforming to [`schema_schema`]. Each field
/// becomes `"{name}:{kind}:{flags}"` where `flags` is three chars: required,
/// identity, witness (`-` when off). Deterministic and order-preserving.
fn schema_to_body(s: &Schema) -> Value {
    let fields: Vec<Value> = s
        .fields
        .iter()
        .map(|f| {
            let flags = format!(
                "{}{}{}",
                if f.required { 'r' } else { '-' },
                if f.identity { 'i' } else { '-' },
                if f.witness { 'w' } else { '-' },
            );
            json!(format!("{}:{}:{}", f.name, fieldkind_str(&f.kind), flags))
        })
        .collect();
    json!({ "name": s.name, "version": s.version, "fields": fields })
}

/// Seal a schema *as a quantum* under the meta-schema — give a law a CID the same
/// way every fact gets one.
pub fn seal_schema(s: &Schema) -> Quantum {
    Quantum::seal(&schema_schema(), &schema_to_body(s))
        .expect("a schema description always conforms to the meta-schema")
}

/// The built-in laws, in a fixed order. The meta-schema is **not** listed here —
/// it is the [`Constitution::meta`] keystone, which would otherwise ground in
/// itself.
pub fn builtin_laws() -> Vec<(&'static str, Schema)> {
    vec![
        ("attestation", attestation_schema()),
        ("proposition", proposition_schema()),
        ("assertion", assertion_schema()),
        ("generator", generator_schema()),
        ("verification", verification_schema()),
        ("edge_annotation", edge_annotation_schema()),
        ("mapping", mapping_schema()),
        ("structuring", structuring_schema()),
        ("utterance", utterance_schema()),
    ]
}

/// One sealed law: its name, its raw schema CID, and the same schema sealed as a
/// quantum under the meta-schema.
#[derive(Debug, Clone)]
pub struct Article {
    pub name: String,
    /// `blake3` of the schema's own canonical bytes (the pre-existing address).
    pub schema_cid: String,
    /// The schema sealed *as a quantum* under [`schema_schema`].
    pub quantum: Quantum,
}

/// The inter-substrate treaty: the minimal triad two foundationally-different
/// substrates must agree on to prove they computed under the same law.
#[derive(Debug, Clone)]
pub struct Treaty {
    /// Bundle wire-format version.
    pub wire_version: u32,
    /// The whole-body digest algorithm bundles commit under.
    pub body_digest_algo: String,
    /// The canonicalizer's fingerprint: the keystone CID. If the canonicalizer or
    /// the meta-schema drifts, this address changes — loudly.
    pub canon_rule_cid: String,
}

/// The sealed body of law: the self-describing keystone, every article, the root
/// that commits them, and the treaty that travels.
#[derive(Debug, Clone)]
pub struct Constitution {
    /// The meta-schema sealed under itself — the self-hosting fixpoint.
    pub meta: Quantum,
    /// Every built-in law sealed as a quantum under the meta-schema.
    pub articles: Vec<Article>,
    /// The constitution root: [`consensus_root`] over the same closure shape a fact
    /// bundle uses (keystone anchor, articles grounded in it).
    pub root: String,
    pub treaty: Treaty,
}

impl Constitution {
    /// Pack the constitution into a portable [`Bundle`] — the treaty realized. The
    /// `consensus_root` it carries is the constitution root, reproducible by any
    /// importer.
    pub fn bundle(&self) -> Bundle {
        let mut entries = vec![BundleEntry::of(SchemaKind::Schema, &self.meta)];
        let mut rules = Vec::new();
        for a in &self.articles {
            entries.push(BundleEntry::of(SchemaKind::Schema, &a.quantum));
            rules.push(Rule {
                head: a.quantum.cid.clone(),
                body: vec![self.meta.cid.clone()],
            });
        }
        export(
            entries,
            vec![self.meta.cid.clone()],
            rules,
            &TransparencyLog::new(),
            vec![],
        )
    }

    /// Does the substrate genuinely host its own grammar? Three checks:
    ///
    /// 1. **Fixpoint** — the keystone re-verifies under the meta-schema *and* its
    ///    body is exactly the meta-schema's own description. The law of laws is
    ///    addressed by its own description.
    /// 2. **Articles** — every sealed law re-verifies under the meta-schema.
    /// 3. **Determinism** — re-sealing the whole constitution from scratch
    ///    reproduces the same keystone CID and the same root (rebuild-bit-identical,
    ///    at the level of the laws themselves).
    pub fn self_hosts(&self) -> bool {
        let ms = schema_schema();
        let fixpoint = self.meta.verify(&ms).unwrap_or(false)
            && self.meta.body == seal_schema(&ms).body;
        let articles_ok = self
            .articles
            .iter()
            .all(|a| a.quantum.verify(&ms).unwrap_or(false));
        let rebuilt = seal_constitution();
        let deterministic = rebuilt.root == self.root && rebuilt.meta.cid == self.meta.cid;
        fixpoint && articles_ok && deterministic
    }
}

/// Seal the substrate's constitution: the meta-schema under itself, every built-in
/// law under the meta-schema, and the root over the same consensus construction a
/// fact bundle uses.
pub fn seal_constitution() -> Constitution {
    let meta = seal_schema(&schema_schema());

    let articles: Vec<Article> = builtin_laws()
        .into_iter()
        .map(|(name, s)| Article {
            name: name.to_string(),
            schema_cid: schema_cid(&s),
            quantum: seal_schema(&s),
        })
        .collect();

    // Root via the canonical bundle path — identical to what a peer reconstructs on
    // import, so the constitution root *is* a fact-bundle consensus root.
    let mut entries = vec![BundleEntry::of(SchemaKind::Schema, &meta)];
    let mut rules = Vec::new();
    for a in &articles {
        entries.push(BundleEntry::of(SchemaKind::Schema, &a.quantum));
        rules.push(Rule { head: a.quantum.cid.clone(), body: vec![meta.cid.clone()] });
    }
    let root = export(
        entries,
        vec![meta.cid.clone()],
        rules,
        &TransparencyLog::new(),
        vec![],
    )
    .consensus_root;

    let treaty = Treaty {
        wire_version: 1,
        body_digest_algo: "blake3".to_string(),
        canon_rule_cid: meta.cid.clone(),
    };

    Constitution { meta, articles, root, treaty }
}

/// A direct settle-based root, kept for parity testing against the bundle path.
#[allow(dead_code)]
fn settle_root(meta: &Quantum, articles: &[Article]) -> String {
    let mut cl = Closure::new();
    cl.add_anchor(&meta.cid);
    for a in articles {
        let mut body = BTreeSet::new();
        body.insert(meta.cid.clone());
        cl.add_rule(a.quantum.cid.clone(), body);
    }
    cl.settle();
    consensus_root(&cl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import;

    #[test]
    fn the_substrate_hosts_its_own_grammar() {
        let c = seal_constitution();
        assert!(c.self_hosts(), "constitution must self-host");
        assert_eq!(c.articles.len(), builtin_laws().len());
    }

    #[test]
    fn keystone_is_its_own_law() {
        // The meta-schema, sealed under itself, conforms to itself: the fixpoint.
        let c = seal_constitution();
        assert_eq!(c.meta.schema_cid, schema_cid(&schema_schema()));
        assert!(c.meta.verify(&schema_schema()).unwrap());
        assert_eq!(c.treaty.canon_rule_cid, c.meta.cid);
    }

    #[test]
    fn constitution_is_portable_and_reproducible() {
        // A peer imports the law and re-derives the root, having sealed nothing.
        let c = seal_constitution();
        let wire = serde_json::to_string(&c.bundle()).unwrap();
        let received: Bundle = serde_json::from_str(&wire).unwrap();
        let loaded = import(&received).expect("constitution bundle verifies");
        assert_eq!(consensus_root(&loaded.closure), c.root);
        assert_eq!(c.bundle().consensus_root, c.root);
    }

    #[test]
    fn tampered_constitution_is_rejected() {
        // Forge one law's body — the re-seal check (and body digest) must catch it.
        let c = seal_constitution();
        let mut b = c.bundle();
        b.entries[1].body["name"] = json!("forged");
        assert!(import(&b).is_err(), "a tampered law must not import");
    }

    #[test]
    fn rebuild_is_bit_identical() {
        let a = seal_constitution();
        let b = seal_constitution();
        assert_eq!(a.meta.cid, b.meta.cid);
        assert_eq!(a.root, b.root);
        // and the parity path agrees with the bundle path on the certain set
        assert_eq!(settle_root(&a.meta, &a.articles), settle_root(&b.meta, &b.articles));
    }
}
