//! The hash-chained transparency log — append-only, tamper-evident.
//!
//! `ket/DESIGN.md`: "the append-only log is the source of truth for *events*."
//! Today that log is *trusted* (the drift tooling cross-checks it via `git log`).
//! This makes "the log was not rewritten" **verifiable**: each entry commits to
//! the previous one, so the `head` hash commits to the entire history. Altering
//! any past entry changes its hash, which breaks every subsequent link and moves
//! the head — detected by `verify` in one replay.
//!
//! This is the Certificate-Transparency / Merkle-log discipline in its simplest
//! form (a hash chain). A Merkle *tree* would add succinct inclusion proofs (prove
//! entry N is in the log without sending the whole log); that is the extension —
//! see `consensus_root` for the analogous root over the closure.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: u64,
    /// Hash of the previous entry (the chain link).
    pub prev: String,
    /// The logged payload — typically a sealed quantum's CID, or a vouch.
    pub payload: String,
    /// This entry's hash: `H(domain ‖ prev ‖ seq ‖ payload)`.
    pub hash: String,
}

/// The genesis link — a fixed root every log chains from.
fn genesis() -> String {
    blake3::hash(b"canon.d/log/v1/genesis").to_hex().to_string()
}

fn entry_hash(prev: &str, seq: u64, payload: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"canon.d/log/v1/entry");
    h.update(&(prev.len() as u64).to_le_bytes());
    h.update(prev.as_bytes());
    h.update(&seq.to_le_bytes());
    h.update(&(payload.len() as u64).to_le_bytes());
    h.update(payload.as_bytes());
    h.finalize().to_hex().to_string()
}

#[derive(Debug, Clone)]
pub struct TransparencyLog {
    entries: Vec<LogEntry>,
    head: String,
}

impl Default for TransparencyLog {
    fn default() -> Self {
        TransparencyLog { entries: Vec::new(), head: genesis() }
    }
}

impl TransparencyLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a payload, returning the new head. Append-only: there is no
    /// `remove` or `set` — the only mutation is to extend.
    pub fn append(&mut self, payload: &str) -> String {
        let seq = self.entries.len() as u64;
        let prev = self.head.clone();
        let hash = entry_hash(&prev, seq, payload);
        self.entries.push(LogEntry { seq, prev, payload: payload.to_string(), hash: hash.clone() });
        self.head = hash.clone();
        hash
    }

    /// The head commits to the entire history — publish it and the log is pinned.
    pub fn head(&self) -> &str {
        &self.head
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Is this payload recorded? (Linear scan; a Merkle tree would give a succinct
    /// inclusion proof instead.)
    pub fn contains(&self, payload: &str) -> bool {
        self.entries.iter().any(|e| e.payload == payload)
    }

    /// Replay the chain from genesis and confirm every link: sequence, prev-link,
    /// and recomputed hash, ending at the published head. `false` ⇒ the log was
    /// rewritten (an entry edited, reordered, inserted, or dropped).
    pub fn verify(&self) -> bool {
        let mut prev = genesis();
        for (i, e) in self.entries.iter().enumerate() {
            if e.seq != i as u64 || e.prev != prev {
                return false;
            }
            if entry_hash(&e.prev, e.seq, &e.payload) != e.hash {
                return false;
            }
            prev = e.hash.clone();
        }
        prev == self.head
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_only_log_verifies() {
        let mut log = TransparencyLog::new();
        let h0 = log.append("seal:a");
        let h1 = log.append("seal:b");
        let h2 = log.append("seal:c");
        assert!(log.verify());
        assert_eq!(log.head(), h2);
        assert_ne!(h0, h1);
        assert!(log.contains("seal:b"));
        assert!(!log.contains("seal:z"));
    }

    #[test]
    fn editing_a_past_entry_is_detected() {
        let mut log = TransparencyLog::new();
        log.append("seal:a");
        log.append("seal:b");
        log.append("seal:c");
        let pinned_head = log.head().to_string();
        assert!(log.verify());

        // rewrite history: change the middle payload
        log.entries[1].payload = "seal:FORGED".into();
        assert!(!log.verify(), "an edited past entry breaks the chain");

        // the head no longer reflects a valid replay — anyone holding the pinned
        // head detects the tamper without trusting the log holder.
        let mut honest = TransparencyLog::new();
        honest.append("seal:a");
        honest.append("seal:FORGED");
        honest.append("seal:c");
        assert_ne!(honest.head(), pinned_head, "the forged history has a different head");
    }

    #[test]
    fn reorder_and_drop_are_detected() {
        let mut log = TransparencyLog::new();
        log.append("a");
        log.append("b");
        let mut reordered = log.clone();
        reordered.entries.swap(0, 1);
        assert!(!reordered.verify(), "reordering breaks prev-links");

        let mut dropped = log.clone();
        dropped.entries.remove(0);
        assert!(!dropped.verify(), "dropping an entry breaks the sequence");
    }

    #[test]
    fn head_commits_to_history() {
        let mut a = TransparencyLog::new();
        a.append("x");
        a.append("y");
        let mut b = TransparencyLog::new();
        b.append("x");
        b.append("y");
        assert_eq!(a.head(), b.head(), "same history → same head");
        b.append("z");
        assert_ne!(a.head(), b.head(), "extending moves the head");
    }
}
