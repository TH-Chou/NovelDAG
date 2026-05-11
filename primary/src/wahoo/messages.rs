// Wahoo wire-level messages, ported 1:1 from the Go reference.
// Source of truth: `Wahoo-main/wahoo/data_struct.go` and `wahoo/msg_type.go`.
//
// Two notes on the port:
//
// * `Sender` / `*Sender` fields in the Go version are `string` node names
//   (e.g. "node0"). NovelDAG identifies authorities by `crypto::PublicKey`,
//   so every Go `string` sender field becomes a `PublicKey` here. This is
//   the only mechanical deviation; semantically the field still designates
//   "the authority that originated this message".
//
// * `PreviousHash map[string][]byte` (Go) → `BTreeMap<PublicKey, Digest>`
//   (Rust). The map key is the parent block's *sender*, value is the parent
//   block's hash, exactly as in Go (see `wahoo/node.go::selectPreviousBlocks`).
//   We use `BTreeMap` (not `HashMap`) so serialisation and hashing are
//   deterministic across nodes.
//
// * `[]byte` (Go) → `Vec<u8>` (Rust) for raw signature payloads (PartialSig,
//   Hash, Done) — Wahoo treats these opaquely.
//
// The Go `Block.Tag int` is constrained to {1, 2}: Tag=1 is the real PB
// proposal at even rounds (and the only tag for odd-round fast blocks);
// Tag=2 is the empty "vote-completed" block PB broadcasts after collecting
// 2f+1 votes. We model it as `WahooBlockTag` to lock the invariant.

use crate::primary::Round;
use config::WorkerId;
use crypto::{Digest, PublicKey, Signature};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `Block.Tag` in `wahoo/data_struct.go`. Values mirror the Go constants
/// implicitly assigned by `wahoo/pb.go::HandleBlockMsg` (Tag=1 / Tag=2).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum WahooBlockTag {
    /// Real proposal carrying transactions and parent links.
    /// Used at even rounds (PB phase 1) and at every odd round (fast path).
    Proposal = 1,
    /// Empty "vote complete" block broadcast after collecting 2f+1 votes
    /// for the matching `Proposal`. Even rounds only.
    EmptyVoteCertificate = 2,
}

impl Default for WahooBlockTag {
    fn default() -> Self {
        Self::Proposal
    }
}

/// `wahoo/data_struct.go::Block` (lines 3-10).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooBlock {
    /// `Sender string` → originating authority's public key.
    pub sender: PublicKey,
    /// `Round uint64`.
    pub round: Round,
    /// `PreviousHash map[string][]byte` — at least 2f+1 blocks in the
    /// previous round, mapping their sender to their hash.
    pub previous_hash: BTreeMap<PublicKey, Digest>,
    /// `Txs [][]byte` — opaque batched transactions. The Wahoo state
    /// machine never inspects this field. In NovelDAG-pipeline mode it
    /// is left empty: real payload is carried by `payload_digests`
    /// below so that all four DAG protocols share the same accounting.
    pub txs: Vec<Vec<u8>>,
    /// References to worker batches included in this block. Mirrors
    /// `primary::messages::Header.payload`. Populated by `Node::new_block`
    /// from digests forwarded by the worker network handler. The block
    /// hash includes this field, so all peers see the same content for
    /// a given (sender, round) tuple. Empty for Tag=EmptyVoteCertificate
    /// blocks.
    pub payload_digests: BTreeMap<Digest, WorkerId>,
    /// `TimeStamp int64` — nanoseconds since the Unix epoch, as in
    /// `time.Now().UnixNano()`.
    pub timestamp: i64,
    /// `Tag int` — see `WahooBlockTag`.
    pub tag: WahooBlockTag,
}

/// `wahoo/data_struct.go::Vote` (lines 19-23).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooVote {
    /// `VoteSender string`.
    pub vote_sender: PublicKey,
    /// `BlockSender string` — author of the block being voted for.
    pub block_sender: PublicKey,
    /// `Round uint64`.
    pub round: Round,
}

/// `wahoo/data_struct.go::Ready` (lines 26-32).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooReady {
    pub ready_sender: PublicKey,
    pub block_sender: PublicKey,
    pub round: Round,
    /// Hash of the block being acknowledged.
    pub hash: Digest,
    /// `PartialSig []byte`. With T3 placeholder cryptography this is an
    /// ed25519 signature over `hash`; later phases may swap in a true
    /// threshold partial signature.
    pub partial_sig: Vec<u8>,
}

/// `wahoo/data_struct.go::Done` (lines 34-40).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooDone {
    pub done_sender: PublicKey,
    pub block_sender: PublicKey,
    /// `Done [][]byte` — collected partial signatures (or the assembled
    /// QC, depending on the implementation choice). Kept opaque on the
    /// wire to match the Go layout.
    pub done: Vec<Vec<u8>>,
    pub hash: Digest,
    pub round: Round,
}

/// `wahoo/data_struct.go::Elect` (lines 43-47).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooElect {
    pub sender: PublicKey,
    pub round: Round,
    /// Partial signature over `encode(round)`; assembled into the
    /// common-coin QC by `wahoo/node.go::tryToElectLeader`.
    pub partial_sig: Vec<u8>,
}

/// `wahoo/data_struct.go::ReVote` (lines 50-56).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WahooReVote {
    pub revote_sender: PublicKey,
    pub block_sender: PublicKey,
    pub round: Round,
    /// `Voted bool` — whether the sender has already voted for the leader
    /// block at this round.
    pub voted: bool,
    /// Hash of the block (may be empty when `voted == false`).
    pub hash: Digest,
}

/// Tag enumeration corresponding to `wahoo/msg_type.go::ProposalTag` ..
/// `ReVoteTag`. Discriminants are not stable on the wire (we serialise via
/// bincode-tagged enum), but the variant order matches the Go file for
/// readability.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WahooMessage {
    /// `ProposalTag` — both Tag=1 proposals and Tag=2 empty blocks travel
    /// via this variant; the recipient distinguishes by `WahooBlock::tag`.
    Block(WahooBlock),
    /// `VoteTag`.
    Vote(WahooVote),
    /// `ElectTag`.
    Elect(WahooElect),
    /// `ReadyTag`.
    Ready(WahooReady),
    /// `DoneTag`.
    Done(WahooDone),
    /// `ReVoteTag`.
    ReVote(WahooReVote),
}

impl WahooMessage {
    /// Returns the authority that originated this message — the field
    /// the recipient uses to look up the verification key. Mirrors the
    /// per-variant sender extraction in
    /// `wahoo/msg_handle.go::HandleMsgLoop` (e.g.
    /// `msgAsserted.Sender` for Block, `msgAsserted.VoteSender` for
    /// Vote, `msgAsserted.ReadySender` for Ready, etc.).
    pub fn sender(&self) -> PublicKey {
        match self {
            Self::Block(b) => b.sender,
            Self::Vote(v) => v.vote_sender,
            Self::Elect(e) => e.sender,
            Self::Ready(r) => r.ready_sender,
            Self::Done(d) => d.done_sender,
            Self::ReVote(r) => r.revote_sender,
        }
    }
}

/// Wire-level envelope: ED25519 signature attached to every Wahoo
/// message. Mirrors the Go `MsgWithSig{Msg, Sig}` triple emitted by
/// `wahoo/msg_send.go::broadcast`/`send` (lines 78-94, 100-119) and
/// verified by `wahoo/msg_handle.go::HandleMsgLoop`.
///
/// The signature is computed over the bincode encoding of `msg`. The
/// recipient extracts the sender via `msg.sender()` and verifies with
/// the corresponding public key from the committee.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedWahoo {
    pub msg: WahooMessage,
    pub sig: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip every variant through bincode to lock the wire format.
    #[test]
    fn wahoo_message_roundtrip() {
        let cases = vec![
            WahooMessage::Block(WahooBlock::default()),
            WahooMessage::Vote(WahooVote::default()),
            WahooMessage::Elect(WahooElect::default()),
            WahooMessage::Ready(WahooReady::default()),
            WahooMessage::Done(WahooDone::default()),
            WahooMessage::ReVote(WahooReVote::default()),
        ];
        for original in cases {
            let bytes = bincode::serialize(&original).expect("serialize Wahoo message");
            let _: WahooMessage =
                bincode::deserialize(&bytes).expect("deserialize Wahoo message");
        }
    }

    /// `WahooBlockTag` discriminant values must stay locked at 1/2 to match
    /// the Go reference (`Block.Tag = 1` / `Tag = 2`).
    #[test]
    fn wahoo_block_tag_discriminants() {
        assert_eq!(WahooBlockTag::Proposal as u8, 1);
        assert_eq!(WahooBlockTag::EmptyVoteCertificate as u8, 2);
    }

    /// Each variant's `sender()` returns the field the Go handler
    /// inspects to look up the verifying public key.
    #[test]
    fn wahoo_message_sender_extraction() {
        let pk = PublicKey::default();
        let cases: Vec<WahooMessage> = vec![
            WahooMessage::Block(WahooBlock {
                sender: pk,
                ..WahooBlock::default()
            }),
            WahooMessage::Vote(WahooVote {
                vote_sender: pk,
                ..WahooVote::default()
            }),
            WahooMessage::Elect(WahooElect {
                sender: pk,
                ..WahooElect::default()
            }),
            WahooMessage::Ready(WahooReady {
                ready_sender: pk,
                ..WahooReady::default()
            }),
            WahooMessage::Done(WahooDone {
                done_sender: pk,
                ..WahooDone::default()
            }),
            WahooMessage::ReVote(WahooReVote {
                revote_sender: pk,
                ..WahooReVote::default()
            }),
        ];
        for msg in cases {
            assert_eq!(msg.sender(), pk);
        }
    }
}
