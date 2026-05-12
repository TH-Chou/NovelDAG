// Wahoo wire-level messages, ported from the Go reference.
// Source of truth: `Wahoo-main/wahoo/data_struct.go` and `wahoo/msg_type.go`.
//
// Phase B Step 3b — `WahooBlock` is now an alias for `messages::Header`.
// All the per-field rename steps (3a-i sender→author, 3a-ii previous_hash
// →parents, 3a-iii drop timestamp, 3a-iv drop tag) preceded this swap so
// that the alias is a drop-in. The Go `Block.Tag ∈ {1, 2}` distinction is
// preserved via `Header.wahoo_tag ∈ {Some(Pbc), Some(PbcVoteComplete)}`.
//
// `Sender` / `*Sender` fields in the Go version are `string` node names
// (e.g. "node0"); NovelDAG identifies authorities by `crypto::PublicKey`,
// so every Go `string` sender field becomes a `PublicKey` here.
// `[]byte` (Go) → `Vec<u8>` (Rust) for raw signature payloads.

use crate::messages::{Header, Vote, WahooTag};
use crate::primary::Round;
use crypto::{Digest, PublicKey, Signature};
use serde::{Deserialize, Serialize};

/// `Block.Tag` in `wahoo/data_struct.go` collapsed onto `messages::WahooTag`:
///
/// * Go Tag=1 (Proposal)             → `Some(WahooTag::Pbc)` / `Some(WahooTag::EpbcTf)`
/// * Go Tag=2 (EmptyVoteCertificate) → `Some(WahooTag::PbcVoteComplete)`
///
/// Helper constructors and predicates live here so PB/Node code keeps
/// reading Go-style.
#[allow(dead_code)]
pub fn is_pbc_proposal(block: &WahooBlock) -> bool {
    matches!(block.wahoo_tag, Some(WahooTag::Pbc))
}

pub fn is_pbc_vote_complete(block: &WahooBlock) -> bool {
    matches!(block.wahoo_tag, Some(WahooTag::PbcVoteComplete))
}

/// Phase B Step 3b: `WahooBlock` is just `messages::Header`. The Go
/// reference's per-field shape is preserved by the field renames in
/// Steps 3a-i..3a-iv:
///   author / round / parents / payload — same shape as Go.
///   wahoo_tag — replaces Go `Tag int` (`Pbc` ≡ Tag=1, `PbcVoteComplete`
///   ≡ Tag=2, `EpbcTf` for odd-round fast-path blocks).
///   id / signature / parents_2 / qc / coin_share / leader_link — extra
///   `Header` fields the Wahoo state machine simply does not populate yet
///   (they default to None/empty and survive serialisation as zero-cost
///   bytes). The Phase D unification activates them.
pub type WahooBlock = Header;

/// Phase B Step 3c: `WahooVote` is now an alias for `messages::Vote`.
/// Field mapping vs the Go reference:
///   Go `VoteSender`  → `Vote.author`
///   Go `BlockSender` → `Vote.origin`
///   Go `Round`       → `Vote.round` (and `voter_round`)
///   _extra:           `id` = digest of the voted block, used by Core in
///                     non-Wahoo protocols and Phase D Wahoo as a strong
///                     identifier (Go relies on (sender, round) uniqueness)
///   _extra:           `wahoo_phase` = which of EPBC/PBC quorum bucket
///                     this share contributes to; PB votes carry
///                     `Some(WahooVotePhase::Pbc)`.
pub type WahooVote = Vote;

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
            Self::Block(b) => b.author,
            Self::Vote(v) => v.author,
            Self::Elect(e) => e.sender,
            Self::Ready(r) => r.ready_sender,
            Self::Done(d) => d.done_sender,
            Self::ReVote(r) => r.revote_sender,
        }
    }
}

/// Phase B Step 3d (DONE): `WahooBlock` / `WahooVote` now travel on the
/// wire as first-class `PrimaryMessage::Header` / `PrimaryMessage::Vote`
/// envelopes, signed via their inline `signature` field rather than the
/// outer `SignedWahoo` wrapper. These `From` conversions stay as
/// idiomatic call-site helpers — e.g. `let m: PrimaryMessage = block.into();`
/// — even though `msg_send::broadcast_header` / `send_vote` no longer
/// need them.
///
/// The reverse direction (PrimaryMessage -> WahooMessage) is trivially
/// destructured in `WahooReceiverHandler::dispatch`; no `From` impl
/// required.
impl From<WahooBlock> for crate::primary::PrimaryMessage {
    fn from(b: WahooBlock) -> Self {
        crate::primary::PrimaryMessage::Header(b)
    }
}

impl From<WahooVote> for crate::primary::PrimaryMessage {
    fn from(v: WahooVote) -> Self {
        crate::primary::PrimaryMessage::Vote(v)
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

    /// Phase B Step 3b retired `WahooBlockTag`. The Go Tag=1 / Tag=2
    /// invariant is now expressed via `WahooTag::Pbc` vs
    /// `WahooTag::PbcVoteComplete` on the unified `Header.wahoo_tag`.
    #[test]
    fn wahoo_block_tag_round_trip() {
        let b1 = WahooBlock {
            wahoo_tag: Some(WahooTag::Pbc),
            ..WahooBlock::default()
        };
        let b2 = WahooBlock {
            wahoo_tag: Some(WahooTag::PbcVoteComplete),
            ..WahooBlock::default()
        };
        assert!(super::is_pbc_proposal(&b1));
        assert!(super::is_pbc_vote_complete(&b2));
    }

    /// Each variant's `sender()` returns the field the Go handler
    /// inspects to look up the verifying public key.
    #[test]
    fn wahoo_message_sender_extraction() {
        let pk = PublicKey::default();
        let cases: Vec<WahooMessage> = vec![
            WahooMessage::Block(WahooBlock {
                author: pk,
                ..WahooBlock::default()
            }),
            WahooMessage::Vote(WahooVote {
                author: pk,
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
