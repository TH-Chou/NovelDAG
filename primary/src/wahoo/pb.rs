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

use crate::aggregators::{WahooQuorum, WahooVotesAggregator};
use crate::messages::{Certificate, WahooTag, WahooVotePhase};
use crate::primary::Round;
use crate::wahoo::messages::{is_pbc_vote_complete, WahooBlock, WahooMessage, WahooVote};
use config::Committee;
use crypto::PublicKey;
use std::collections::HashMap;

/// Action emitted by PB for the parent `Node` to execute. Mirrors the
/// network and channel-output side-effects performed inline in `pb.go`.
#[derive(Debug, Clone)]
pub enum PbAction {
    /// `pb.go::sendVote` — unicast the vote to the proposer.
    SendVote { target: PublicKey, vote: WahooVote },
    /// `pb.go::broadcastBlock2` — broadcast the empty Tag=2 block.
    /// Carries the PBC Certificate that justified emission (2f+1 Pbc
    /// votes aggregated by `WahooVotesAggregator`). Phase D will let
    /// Node broadcast the `Certificate` directly and retire Block2.
    BroadcastBlock2(WahooBlock, Certificate),
    /// `pb.go::tryToOutputBlocks` — block is ready for DAG insertion.
    /// Equivalent to writing into `blockCh` in the Go version.
    OutputBlock(WahooBlock),
}

/// PB state. Field names follow the Go `PB` struct (lines 15-37 of pb.go).
pub struct Pb {
    name: PublicKey,
    /// Phase B Step 3e: committee is now part of PB state so votes can
    /// be aggregated into stake-weighted `WahooVotesAggregator` buckets
    /// instead of the simple Go `pendingVote[round][sender]int` counter.
    committee: Committee,

    /// `pendingBlocks map[round][sender]*Block` — Tag=1 proposals.
    pending_blocks: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// `pendingBlock2s map[round][sender]*Block` — Tag=2 empty blocks.
    pending_block2s: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// Replaces Go's `pendingVote[round][block_sender]int`: one
    /// `WahooVotesAggregator` per (round, block-author) tuple that
    /// routes incoming votes into the correct phase bucket (PBC for even
    /// rounds; TS1/TS2/TF will be used at odd-round EPBC callsites in a
    /// later phase). The aggregator latches once a phase fires, so
    /// duplicate votes after quorum are ignored silently.
    pending_vote: HashMap<(Round, PublicKey), WahooVotesAggregator>,
    /// `blockOutput map[round][sender]bool`.
    block_output: HashMap<Round, HashMap<PublicKey, bool>>,
    /// `block2Send map[round]bool`.
    block2_send: HashMap<Round, bool>,
}

impl Pb {
    /// `pb.go::NewPBer`. Quorum threshold is now derived from the
    /// committee (`committee.quorum_threshold()`), matching the rest of
    /// the codebase and allowing non-uniform stakes in the future.
    pub fn new(name: PublicKey, committee: Committee) -> Self {
        Self {
            name,
            committee,
            pending_blocks: HashMap::new(),
            pending_block2s: HashMap::new(),
            pending_vote: HashMap::new(),
            block_output: HashMap::new(),
            block2_send: HashMap::new(),
        }
    }

    /// `pb.go::BroadcastBlock(block *Block)` — proposer kicks off PB at
    /// the start of an even round.  The block must already carry
    /// `wahoo_tag == Some(WahooTag::Pbc)` (Go Tag=1 "proposal").
    pub fn broadcast_block(&mut self, block: &WahooBlock) {
        debug_assert_eq!(block.wahoo_tag, Some(WahooTag::Pbc));
        self.store_block_msg(block);
        // Note: the network broadcast itself is performed by Node, which
        // calls `Node::pb_action_dispatch` after invoking this method. We
        // do NOT emit a BroadcastTag1 action here because the Node also
        // wraps the proposal with an ED25519 signature and timestamp; it
        // is simpler for Node to broadcast directly.
    }

    /// `pb.go::HandleBlockMsg(block *Block)` (lines 122-138). Dispatches
    /// on `block.wahoo_tag`: `Some(Pbc)` is the Go Tag=1 proposal path,
    /// `Some(PbcVoteComplete)` is the Go Tag=2 "vote-complete" path.
    pub fn handle_block(&mut self, block: WahooBlock) -> Vec<PbAction> {
        let mut actions = Vec::new();
        let is_vote_complete = is_pbc_vote_complete(&block);
        if !is_vote_complete {
            // Tag=1: real PB proposal.
            {
                let round = block.round;
                let sender = block.author;
                let block_id = crypto::Hash::digest(&block);
                self.store_block_msg(&block);
                // Phase B Step 3c: WahooVote is now `messages::Vote`. PB
                // votes always carry `wahoo_phase = Some(Pbc)` so the
                // unified `WahooVotesAggregator` can route them into the
                // PBC bucket once Step 3e lands.
                actions.push(PbAction::SendVote {
                    target: sender,
                    vote: WahooVote {
                        id: block_id,
                        round,
                        voter_round: round,
                        origin: sender,
                        author: self.name,
                        wahoo_phase: Some(WahooVotePhase::Pbc),
                        ..WahooVote::default()
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
        } else {
            // Tag=2: empty "vote-complete" block.
            let round = block.round;
            let sender = block.author;
            self.store_block2_msg(&block);
            if let Some(out) = self.try_to_output(round, sender) {
                actions.push(PbAction::OutputBlock(out));
            }
        }
        actions
    }

    /// `pb.go::HandleVoteMsg(vote *Vote)` (lines 140-145). Post-3e the
    /// Go `pendingVote[round][sender]int` counter is replaced by a
    /// `WahooVotesAggregator` per (round, block-author); emission is
    /// gated on `WahooQuorum::Pbc` (2f+1 stake of `WahooVotePhase::Pbc`
    /// votes) rather than a naive count.
    pub fn handle_vote(&mut self, vote: WahooVote) -> Vec<PbAction> {
        let mut actions = Vec::new();
        let round = vote.round;
        let origin = vote.origin;
        // The vote references a specific block via `origin` + `round`.
        // The aggregator needs the `Header` to build the PBC certificate;
        // Wahoo PB sends votes directly to the block's author, so the
        // header is always already in `pending_blocks` by the time the
        // votes arrive. If it isn't (misbehaving peer / malformed vote),
        // silently drop — matching `pb.go`'s implicit no-op for votes on
        // unknown blocks.
        let header = match self
            .pending_blocks
            .get(&round)
            .and_then(|m| m.get(&origin))
            .cloned()
        {
            Some(h) => h,
            None => return actions,
        };
        let aggregator = self
            .pending_vote
            .entry((round, origin))
            .or_insert_with(WahooVotesAggregator::new);
        let quorum = match aggregator.append(vote, &self.committee, &header) {
            Ok(Some(q)) => q,
            Ok(None) => return actions,
            Err(_) => return actions, // duplicate / malformed — drop
        };
        // `pb.go::checkIfQuorumVote` (lines 185-200). PB only cares about
        // the `Pbc` bucket; EPBC buckets (Ts1/Ts2/Tf) are fed from the
        // odd-round pipeline in `Node` and don't flow through `Pb`.
        let cert = match quorum {
            WahooQuorum::Pbc(c) => c,
            _ => return actions,
        };
        let already_sent = *self.block2_send.get(&round).unwrap_or(&false);
        if !already_sent {
            let block2 = self.generate_block2(round, origin);
            self.block2_send.insert(round, true);
            self.store_block2_msg(&block2);
            actions.push(PbAction::BroadcastBlock2(block2, cert));
        }
        if let Some(out) = self.try_to_output(round, origin) {
            actions.push(PbAction::OutputBlock(out));
        }
        actions
    }

    /// `pb.go::storeBlockMsg`.
    fn store_block_msg(&mut self, block: &WahooBlock) {
        self.pending_blocks
            .entry(block.round)
            .or_insert_with(HashMap::new)
            .insert(block.author, block.clone());
    }

    /// `pb.go::storeBlock2Msg`.
    fn store_block2_msg(&mut self, block: &WahooBlock) {
        self.pending_block2s
            .entry(block.round)
            .or_insert_with(HashMap::new)
            .insert(block.author, block.clone());
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

    /// `pb.go::generateBlock2` — empty block carrying the unified
    /// `wahoo_tag = Some(PbcVoteComplete)` (Go Tag=2 equivalent).
    fn generate_block2(&self, round: Round, block_sender: PublicKey) -> WahooBlock {
        let mut block = WahooBlock {
            author: block_sender,
            round,
            wahoo_tag: Some(WahooTag::PbcVoteComplete),
            ..WahooBlock::default()
        };
        block.id = crypto::Hash::digest(&block);
        block
    }
}

/// Convenience for callers that want to queue actions on a stack.
impl PbAction {
    #[allow(dead_code)]
    pub fn into_message(self) -> Option<WahooMessage> {
        match self {
            PbAction::SendVote { vote, .. } => Some(WahooMessage::Vote(vote)),
            PbAction::BroadcastBlock2(b, _cert) => Some(WahooMessage::Block(b)),
            PbAction::OutputBlock(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Phase B Step 3e: Pb now holds a `Committee`, so fixtures must use
    /// real committee authorities (stakes must be non-zero in the
    /// `WahooVotesAggregator`). `crate::common::committee()` is the same
    /// deterministic 4-authority fixture used by the aggregator tests.
    fn committee_keys() -> (Committee, Vec<PublicKey>) {
        let c = crate::common::committee();
        let mut keys: Vec<PublicKey> = c.authorities.keys().cloned().collect();
        keys.sort();
        (c, keys)
    }

    /// In the post-Step-3b world, what used to be `WahooBlockTag` is just
    /// the `wahoo_tag` field on `Header`. We accept either the legacy
    /// `Proposal`/`EmptyVoteCertificate` style or the unified `WahooTag`
    /// values via these test helpers.
    fn proposal_block(sender: PublicKey, round: Round) -> WahooBlock {
        let mut b = WahooBlock {
            author: sender,
            round,
            wahoo_tag: Some(WahooTag::Pbc),
            ..WahooBlock::default()
        };
        b.id = crypto::Hash::digest(&b);
        b
    }

    fn vote_complete_block(sender: PublicKey, round: Round) -> WahooBlock {
        let mut b = WahooBlock {
            author: sender,
            round,
            wahoo_tag: Some(WahooTag::PbcVoteComplete),
            ..WahooBlock::default()
        };
        b.id = crypto::Hash::digest(&b);
        b
    }

    #[test]
    fn proposer_path_outputs_after_quorum_votes() {
        let (committee, keys) = committee_keys();
        let me = keys[0];
        let mut pb = Pb::new(me, committee);

        // Proposer broadcasts its own Tag=1 block.
        let b1 = proposal_block(me, 2);
        pb.broadcast_block(&b1);

        // Two votes — not enough (n=4, f=1, quorum=3).
        for voter in [keys[1], keys[2]] {
            let actions = pb.handle_vote(WahooVote {
                author: voter,
                origin: me,
                round: 2,
                voter_round: 2,
                id: crypto::Hash::digest(&b1),
                wahoo_phase: Some(WahooVotePhase::Pbc),
                ..WahooVote::default()
            });
            assert!(actions
                .iter()
                .all(|a| !matches!(a, PbAction::OutputBlock(_))));
        }
        // Third vote hits quorum: triggers Block2 broadcast + OutputBlock.
        let actions = pb.handle_vote(WahooVote {
            author: keys[3],
            origin: me,
            round: 2,
            voter_round: 2,
            id: crypto::Hash::digest(&b1),
            wahoo_phase: Some(WahooVotePhase::Pbc),
            ..WahooVote::default()
        });
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::BroadcastBlock2(_, _))));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(b) if b.round == 2 && b.author == me)));
    }

    #[test]
    fn receiver_path_needs_tag2_before_output() {
        let (committee, keys) = committee_keys();
        let me = keys[0];
        let proposer = keys[1];
        let mut pb = Pb::new(me, committee);

        // Tag=1 arrives first: send vote, no output yet.
        let actions = pb.handle_block(proposal_block(proposer, 2));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::SendVote { .. })));
        assert!(!actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(_))));

        // Tag=2 arrives: now we can output.
        let actions = pb.handle_block(vote_complete_block(proposer, 2));
        assert!(actions
            .iter()
            .any(|a| matches!(a, PbAction::OutputBlock(_))));
    }

    #[test]
    fn output_is_deduplicated() {
        let (committee, keys) = committee_keys();
        let me = keys[0];
        let proposer = keys[1];
        let mut pb = Pb::new(me, committee);
        pb.handle_block(proposal_block(proposer, 2));
        let first = pb.handle_block(vote_complete_block(proposer, 2));
        let second = pb.handle_block(vote_complete_block(proposer, 2));
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
