// Port of `Wahoo-main/wahoo/pb.go`.
//
// PB ("Provable Broadcast") is the even-round 2-phase broadcast subprotocol
// of Wahoo. The proposer first broadcasts `Tag=Proposal` carrying real
// transactions; recipients reply with `Vote`s; once 2f+1 votes are
// gathered, the proposer broadcasts an empty `Tag=EmptyVoteCertificate`
// block as a "vote complete" certificate. Recipients deliver the block
// upward only after receiving BOTH the Tag=1 proposal and the Tag=2
// completion. This guarantees that an honest delivery implies 2f+1
// nodes have seen the proposal.
//
// In the Go reference PB is a separate goroutine-managed type that holds
// network handles and emits delivered blocks via `blockCh`. In the Rust
// port we keep PB stateless w.r.t. networking — the parent `Node`
// dispatches PB's emitted `PbAction`s to the network sender. This makes
// the state machine deterministic and unit-testable without any TCP
// stand-in. Behavioural fidelity is preserved: every state mutation in
// `pb.go` has a 1:1 counterpart here.

use crate::primary::Round;
use crate::wahoo::messages::{
    WahooBlock, WahooBlockTag, WahooMessage, WahooVote,
};
use crypto::PublicKey;
use std::collections::HashMap;

/// Action emitted by PB for the parent `Node` to execute. Mirrors the
/// network and channel-output side-effects performed inline in `pb.go`.
#[derive(Debug, Clone)]
pub enum PbAction {
    /// `pb.go::sendVote` — unicast the vote to the proposer.
    SendVote { target: PublicKey, vote: WahooVote },
    /// `pb.go::broadcastBlock2` — broadcast the empty Tag=2 block.
    BroadcastBlock2(WahooBlock),
    /// `pb.go::tryToOutputBlocks` — block is ready for DAG insertion.
    /// Equivalent to writing into `blockCh` in the Go version.
    OutputBlock(WahooBlock),
}

/// PB state. Field names follow the Go `PB` struct (lines 15-37 of pb.go).
pub struct Pb {
    name: PublicKey,
    quorum_num: usize,

    /// `pendingBlocks map[round][sender]*Block` — Tag=1 proposals.
    pending_blocks: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// `pendingBlock2s map[round][sender]*Block` — Tag=2 empty blocks.
    pending_block2s: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// `pendingVote map[round][block_sender]int`.
    pending_vote: HashMap<Round, HashMap<PublicKey, usize>>,
    /// `blockOutput map[round][sender]bool`.
    block_output: HashMap<Round, HashMap<PublicKey, bool>>,
    /// `block2Send map[round]bool`.
    block2_send: HashMap<Round, bool>,
}

impl Pb {
    /// `pb.go::NewPBer` — quorum_num corresponds to the Go `q` parameter
    /// (`int(math.Ceil(2*nodeNum/3))`).
    pub fn new(name: PublicKey, quorum_num: usize) -> Self {
        Self {
            name,
            quorum_num,
            pending_blocks: HashMap::new(),
            pending_block2s: HashMap::new(),
            pending_vote: HashMap::new(),
            block_output: HashMap::new(),
            block2_send: HashMap::new(),
        }
    }

    /// `pb.go::BroadcastBlock(block *Block)` — proposer kicks off PB at
    /// the start of an even round.  The block must already carry
    /// `Tag = Proposal`.
    pub fn broadcast_block(&mut self, block: &WahooBlock) {
        debug_assert_eq!(block.tag, WahooBlockTag::Proposal);
        self.store_block_msg(block);
        // Note: the network broadcast itself is performed by Node, which
        // calls `Node::pb_action_dispatch` after invoking this method. We
        // do NOT emit a BroadcastTag1 action here because the Node also
        // wraps the proposal with an ED25519 signature and timestamp; it
        // is simpler for Node to broadcast directly.
    }

    /// `pb.go::HandleBlockMsg(block *Block)` (lines 122-138).
    pub fn handle_block(&mut self, block: WahooBlock) -> Vec<PbAction> {
        let mut actions = Vec::new();
        match block.tag {
            WahooBlockTag::Proposal => {
                let round = block.round;
                let sender = block.sender;
                self.store_block_msg(&block);
                actions.push(PbAction::SendVote {
                    target: sender,
                    vote: WahooVote {
                        vote_sender: self.name,
                        block_sender: sender,
                        round,
                    },
                });
                let block2_already_seen = self
                    .pending_block2s
                    .get(&round)
                    .and_then(|m| m.get(&sender))
                    .is_some();
                if block2_already_seen {
                    if let Some(out) = self.try_to_output(round, sender) {
                        actions.push(PbAction::OutputBlock(out));
                    }
                }
            }
            WahooBlockTag::EmptyVoteCertificate => {
                let round = block.round;
                let sender = block.sender;
                self.store_block2_msg(&block);
                if let Some(out) = self.try_to_output(round, sender) {
                    actions.push(PbAction::OutputBlock(out));
                }
            }
        }
        actions
    }

    /// `pb.go::HandleVoteMsg(vote *Vote)` (lines 140-145).
    pub fn handle_vote(&mut self, vote: WahooVote) -> Vec<PbAction> {
        let mut actions = Vec::new();
        // store_vote_msg increments the count.
        let count = {
            let entry = self
                .pending_vote
                .entry(vote.round)
                .or_insert_with(HashMap::new)
                .entry(vote.block_sender)
                .or_insert(0);
            *entry += 1;
            *entry
        };
        // `pb.go::checkIfQuorumVote` (lines 185-200).
        if count >= self.quorum_num {
            let already_sent = *self
                .block2_send
                .get(&vote.round)
                .unwrap_or(&false);
            if !already_sent {
                let block2 = self.generate_block2(vote.round, vote.block_sender);
                self.block2_send.insert(vote.round, true);
                self.store_block2_msg(&block2);
                actions.push(PbAction::BroadcastBlock2(block2));
            }
            if let Some(out) = self.try_to_output(vote.round, vote.block_sender) {
                actions.push(PbAction::OutputBlock(out));
            }
        }
        actions
    }

    /// `pb.go::storeBlockMsg`.
    fn store_block_msg(&mut self, block: &WahooBlock) {
        self.pending_blocks
            .entry(block.round)
            .or_insert_with(HashMap::new)
            .insert(block.sender, block.clone());
    }

    /// `pb.go::storeBlock2Msg`.
    fn store_block2_msg(&mut self, block: &WahooBlock) {
        self.pending_block2s
            .entry(block.round)
            .or_insert_with(HashMap::new)
            .insert(block.sender, block.clone());
    }

    /// `pb.go::tryToOutputBlocks(round, sender)` (lines 230-252).
    fn try_to_output(&mut self, round: Round, sender: PublicKey) -> Option<WahooBlock> {
        let already = self
            .block_output
            .entry(round)
            .or_insert_with(HashMap::new)
            .get(&sender)
            .copied()
            .unwrap_or(false);
        if already {
            return None;
        }
        let block = self
            .pending_blocks
            .get(&round)
            .and_then(|m| m.get(&sender))
            .cloned()?;
        // Wahoo's tryToOutputBlocks intentionally does NOT require Tag=2
        // to be present on the proposer-completing path (the proposer
        // calls it directly after generating block2). It does, however,
        // require Tag=2 on the receive path because that path only
        // arrives via `HandleBlockMsg(Tag=2)`. We therefore only output
        // when either:
        //   (a) we just stored Tag=2 for this (round, sender) — the
        //       receive path, or
        //   (b) we ourselves broadcast Tag=2 (block2_send[round] == true)
        //       — the proposer path.
        let has_block2 = self
            .pending_block2s
            .get(&round)
            .and_then(|m| m.get(&sender))
            .is_some();
        let we_sent_block2 = *self.block2_send.get(&round).unwrap_or(&false);
        if !has_block2 && !we_sent_block2 {
            return None;
        }
        self.block_output
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(sender, true);
        Some(block)
    }

    /// `pb.go::generateBlock2` — empty block carrying Tag=2.
    fn generate_block2(&self, round: Round, block_sender: PublicKey) -> WahooBlock {
        WahooBlock {
            sender: block_sender,
            round,
            previous_hash: Default::default(),
            txs: Vec::new(),
            payload_digests: Default::default(),
            timestamp: 0,
            tag: WahooBlockTag::EmptyVoteCertificate,
        }
    }
}

/// Convenience for callers that want to queue actions on a stack.
impl PbAction {
    #[allow(dead_code)]
    pub fn into_message(self) -> Option<WahooMessage> {
        match self {
            PbAction::SendVote { vote, .. } => Some(WahooMessage::Vote(vote)),
            PbAction::BroadcastBlock2(b) => Some(WahooMessage::Block(b)),
            PbAction::OutputBlock(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    fn block(sender: PublicKey, round: Round, tag: WahooBlockTag) -> WahooBlock {
        WahooBlock {
            sender,
            round,
            previous_hash: BTreeMap::new(),
            txs: vec![vec![1, 2, 3]],
            payload_digests: BTreeMap::new(),
            timestamp: 1,
            tag,
        }
    }

    #[test]
    fn proposer_path_outputs_after_quorum_votes() {
        let me = key(1);
        let mut pb = Pb::new(me, 3);

        // Proposer broadcasts its own Tag=1 block.
        let b1 = block(me, 2, WahooBlockTag::Proposal);
        pb.broadcast_block(&b1);

        // Two votes — not enough.
        for voter in [key(2), key(3)] {
            let actions = pb.handle_vote(WahooVote {
                vote_sender: voter,
                block_sender: me,
                round: 2,
            });
            assert!(actions
                .iter()
                .all(|a| !matches!(a, PbAction::OutputBlock(_))));
        }
        // Third vote hits quorum: triggers Block2 broadcast + OutputBlock.
        let actions = pb.handle_vote(WahooVote {
            vote_sender: key(4),
            block_sender: me,
            round: 2,
        });
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::BroadcastBlock2(_))));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(b) if b.round == 2 && b.sender == me)));
    }

    #[test]
    fn receiver_path_needs_tag2_before_output() {
        let me = key(1);
        let proposer = key(2);
        let mut pb = Pb::new(me, 3);

        // Tag=1 arrives first: send vote, no output yet.
        let actions = pb.handle_block(block(proposer, 2, WahooBlockTag::Proposal));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::SendVote { .. })));
        assert!(!actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(_))));

        // Tag=2 arrives: now we can output.
        let actions = pb.handle_block(block(proposer, 2, WahooBlockTag::EmptyVoteCertificate));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(_))));
    }

    #[test]
    fn output_is_deduplicated() {
        let me = key(1);
        let proposer = key(2);
        let mut pb = Pb::new(me, 3);
        pb.handle_block(block(proposer, 2, WahooBlockTag::Proposal));
        let first =
            pb.handle_block(block(proposer, 2, WahooBlockTag::EmptyVoteCertificate));
        let second =
            pb.handle_block(block(proposer, 2, WahooBlockTag::EmptyVoteCertificate));
        assert_eq!(
            first
                .iter()
                .filter(|a| matches!(a, PbAction::OutputBlock(_)))
                .count(),
            1
        );
        assert_eq!(
            second
                .iter()
                .filter(|a| matches!(a, PbAction::OutputBlock(_)))
                .count(),
            0
        );
    }
}
