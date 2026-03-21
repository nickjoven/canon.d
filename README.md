# canon.d

**Purpose:** Canonical serialization layer that ensures semantically identical structured data produces byte-identical output, so content-addressing ([ket](https://github.com/nickjoven/ket)) actually deduplicates equivalent knowledge. Three operations: Write (canonicalize), Read (traverse), Topology (emergent structure).

---

canon.d sits between agents and `ket put`. It ensures that semantically identical structured data produces byte-identical output, so content-addressing actually deduplicates equivalent knowledge.

## The problem

Content-addressed stores (like ket's CAS) dedup by hash: identical bytes = identical CID. But agents produce data with arbitrary key ordering, optional whitespace, and inconsistent field inclusion. Two observations that mean exactly the same thing get different CIDs.

You can tell agents "sort your keys." They'll forget. You can validate after the fact. The data is already stored.

canon.d solves this at the encoding boundary.

## Three operations

### Write — encode truth claims into canonical form

```rust
use canon_d::{Canon, Schema, FieldKind};

let schema = Schema::new("observation", 1)
    .identity("subject", FieldKind::String)
    .identity("predicate", FieldKind::String)
    .required("value", FieldKind::String)
    .optional("confidence", FieldKind::Float);

let canon = Canon::new(&schema);

// These produce identical bytes despite different key order:
let a = serde_json::json!({"subject":"rust","predicate":"is","value":"fast"});
let b = serde_json::json!({"value":"fast","subject":"rust","predicate":"is"});

assert_eq!(canon.encode(&a).unwrap(), canon.encode(&b).unwrap());
```

The schema defines:
- **Field order** — canonical serialization follows schema field order, not input order
- **Identity fields** — which fields determine "sameness" (like a composite primary key)
- **Required vs optional** — absent optional fields are omitted, not null
- **Type constraints** — reject malformed data before it hits the store

Unknown fields are silently dropped. The canonical form contains only what the schema declares.

### Read — discover relationships through graph traversal

Schemas are themselves content-addressed. Store a schema in CAS, tag nodes with its CID. Now you can query:

```
ket dag ls --schema <schema_cid>     # all nodes conforming to this schema
ket schema stats <schema_cid>        # dedup effectiveness
```

Reading a schema teaches an agent the domain: what fields exist, which are identity-bearing, how concepts are structured. The schema is documentation that's enforced at write time.

### Topology — emergent knowledge structure

Nobody designs the topology. It emerges from what agents write and how they link it.

```rust
use canon_d::{TopologyView, NodeInfo};

let topo = TopologyView::from_nodes(&nodes);

// Clusters: groups of nodes sharing schema + identity projection
// "3 agents independently observed that Rust is fast" = cluster of 3
for cluster in topo.convergent_clusters() {
    println!("{} agents agree on {:?}", cluster.agent_count, cluster.identity_hash);
}

// Co-occurrence: which schemas appear together in lineage chains
// "observations often descend from claims" = structural pattern
for (s1, s2, count) in topo.schema_co_occurrences() {
    println!("{s1} <-> {s2}: {count} co-occurrences");
}
```

Topology answers:
- **What do agents agree on?** Convergent clusters (same identity, multiple agents)
- **What concepts relate?** Schema co-occurrence in lineage chains
- **Where is attention concentrated?** Cluster size distribution

## Design lines

**canon.d normalizes. ket stores. Agents reason.**

- canon.d never touches the DAG directly — it produces bytes for `ket put` and reads bytes from `ket get`
- Schema definition is the user's responsibility — canon.d enforces structure, not meaning
- Topology is read-only — it interprets what's already in the DAG, never writes back
- No embeddings, no LLM calls, no intelligence above the substrate line

## Identity projection

The most powerful primitive: extract only the identity-bearing fields, canonicalize them, hash them. Two records with the same identity projection represent "the same concept" even if their non-identity fields differ.

```rust
let canon = Canon::new(&schema);

// Same subject+predicate, different value and confidence
let old = serde_json::json!({"subject":"rust","predicate":"speed","value":"fast","confidence":0.7});
let new = serde_json::json!({"subject":"rust","predicate":"speed","value":"very fast","confidence":0.95});

// Identity projection is identical — these are about the same thing
assert_eq!(
    canon.identity_projection(&old).unwrap(),
    canon.identity_projection(&new).unwrap()
);
```

This enables "upsert" semantics in a content-addressed world: same identity = same concept, new CID = updated understanding.

## Relationship to ket

canon.d is a ket companion, not a replacement:

| Concern | Owner |
|---------|-------|
| Storage, hashing, dedup | ket-cas |
| Provenance, lineage, DAG | ket-dag |
| Schema CID on nodes | ket-dag (schema_cid field) |
| Canonical serialization | **canon.d** |
| Field-level structure | **canon.d** |
| Emergent topology | **canon.d** |
| Scoring, tasks, agents | ket-score, ket-agent |

## License

MIT
