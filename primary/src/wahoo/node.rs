
use crate::primary::Round;
use crate::messages::{LeaderLink, LeaderProof, RecpMessage, WahooTag};
use crate::wahoo::messages::{
    SignedWahoo, WahooBlock, WahooDone, WahooElect, WahooMessage, WahooReady,
};
use crate::wahoo::msg_send;
use crate::wahoo::pb::{Pb, PbAction};
use crate::wahoo::tools::unix_nano_now;
use bytes::Bytes;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, Hash as _, PublicKey, SignatureService};
use log::{debug, info, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};


/// `wahoo/node.go::Chain` (lines 13-16). The Go version stores committed
/// blocks keyed by hash-string; we keep the same shape for exact fidelity
/// during recursive ancestor commit.
struct Chain {
    /// Last committed leader round.
    round: Round,
    /// Committed blocks keyed by their hash digest (hex-equivalent in Go).
    blocks: HashMap<Digest, WahooBlock>,
}

/// Direction-typed action emitted to consensus once a round of Wahoo
/// commits. The consensus layer simply forwards these to `tx_output`.
#[derive(Debug, Clone)]
pub struct CommittedBlock {
    pub block: WahooBlock,
    /// Per-block end-to-end latency in nanoseconds (commit_time -
    /// block.timestamp). Surfaced for benchmark consumers; mirrors
    /// `wahoo/node.go::evaluation`.
    #[allow(dead_code)]
    pub commit_latency_nanos: i64,
}

pub struct Node {
    // ---- identity & topology ----
    name: PublicKey,
    committee: Committee,
    /// Sorted list of all authority public keys; index = "node id" in Go.
    authorities_sorted: Vec<PublicKey>,
    node_num: usize,
    quorum_num: usize,
    /// Threshold parameter for the Elect coin. Equals 2f so that 2f+1
    /// distinct partials recover the combined signature.
    elect_threshold: usize,
    /// Threshold parameter for the RECP BLS pipeline (paper Section IV-B
    /// Step 4d). Equals f so that f+1 distinct partials recover the
    /// aggregate signature used by `LeaderProof::ExclusiveCommit`.
    recp_threshold: usize,
    batch_size: usize,

    // ---- crypto ----
    signature_service: SignatureService,

    // ---- network ----
    sender: ReliableSender,
    /// Keep the `CancelHandler`s returned by `ReliableSender::send`/
    /// `broadcast` alive until acked. Dropping them immediately makes
    /// the inner connection task treat the message as cancelled and
    /// silently discard it (see `network::reliable_sender::Connection`,
    /// where `handler.is_closed()` is checked before flushing the
    /// buffer). Mirrors the pattern used by `Core::cancel_handlers` /
    /// `Proposer::cancel_handlers` in the rest of NovelDAG.
    cancel_handlers: Vec<CancelHandler>,

    // ---- DAG state ----
    /// `dag map[round][sender]*Block` — accepted blocks.
    dag: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// `pendingBlocks map[round][sender]*Block` — blocks whose parents
    /// haven't been observed yet.
    pending_blocks: HashMap<Round, HashMap<PublicKey, WahooBlock>>,
    /// Step 3a-ii: per-digest index over all blocks that have ever
    /// entered `dag`. Replaces the Go reference's `previous_hash[sender]`
    /// pattern: instead of remembering each parent's author on the wire,
    /// every parent reference is a raw digest and we recover the author
    /// (and the parent's full block) via this index.
    blocks_by_digest: HashMap<Digest, WahooBlock>,
    /// Committed blocks.
    chain: Chain,

    // ---- leader / commit state ----
    leader: HashMap<Round, PublicKey>,
    /// `done map[round][sender]*Done` keyed by block_sender at the outer
    /// level (Go: `done[round][BlockSender]`).
    done: HashMap<Round, HashMap<PublicKey, WahooDone>>,
    /// `elect map[round][sender][]byte` — partial sig per voter.
    elect: HashMap<Round, HashMap<PublicKey, Vec<u8>>>,
    /// `ready map[round][block_sender][ready_sender][]byte`.
    ready: HashMap<Round, HashMap<PublicKey, HashMap<PublicKey, Vec<u8>>>>,
    /// Phase C Step 4a: reception-assertion pool.
    ///
    /// Paper Section IV-B Algorithm 2 line 5: every time a node delivers
    /// an EPBC block it broadcasts `⟨RECP, h, ρ⟩` — a (f+1)-threshold
    /// signature share over the block hash `h`. Honest nodes use the
    /// pooled shares, signed by peers at the same wave, to construct the
    /// next EPBC header's `leader_link`:
    ///   * n-f distinct shares on the SAME block -> exclusive-commit proof
    ///   * n-f distinct shares on DIFFERENT blocks -> no-commit proof
    /// Keyed by `round -> block_hash -> author -> RecpMessage` so the
    /// leader-link builder can both (a) count distinct authors per block
    /// for the `ExclusiveCommit` path and (b) emit the original RECP
    /// messages required by the `NoCommit` proof variant in
    /// `messages::LeaderProof`.
    recp_pool: HashMap<Round, HashMap<Digest, HashMap<PublicKey, RecpMessage>>>,

    // ---- round counters & flags ----
    /// `n.round` — current local round.
    round: Round,
    /// `moveRound map[round]int`.
    move_round: HashMap<Round, usize>,
    /// Used for `tryToNextRound` to ensure single-fire per round.
    next_round_signaled: HashSet<Round>,
    /// `leaderElect map[round]bool`.
    leader_elect: HashSet<Round>,
    /// `blockSend map[round]bool`.
    block_send: HashSet<Round>,
    /// `doneSend map[round]bool`.
    done_send: HashSet<Round>,

    // ---- PB sub-state ----
    pb: Pb,

    // ---- benchmarking ----
    /// `evaluation` — per-block latency in ns (block.timestamp -> commit
    /// time).
    evaluation: Vec<i64>,
    /// `commitTime` — wall-clock time of each commit event.
    commit_time: Vec<i64>,
    /// `blockQurey` — counter incremented when a block had to be parked
    /// in `pending_blocks` due to missing parents.
    block_query: u64,

    // ---- output channels ----
    tx_committed: Sender<CommittedBlock>,
    /// Inbound message stream from the network handler. `None` means we
    /// own the receiver after construction; populated only via `spawn`.
    rx_messages: Option<Receiver<WahooMessage>>,
    /// Stream of worker-batch digests produced by our local workers. We
    /// drain whatever has accumulated each time we mint a block, mirror-
    /// ing how `Proposer` populates `Header.payload` for the other three
    /// DAG protocols. Empty until `spawn_wahoo` wires it up.
    rx_workers: Option<Receiver<(Digest, WorkerId)>>,
    /// Buffer of digests received from workers but not yet packed into a
    /// block. Drained by `new_block`.
    pending_digests: Vec<(Digest, WorkerId)>,
    /// Phase C Step 4a: receiver for RECP shares broadcast by peers at
    /// the start of each EPBC phase (paper Algorithm 2 line 5). Moved
    /// out by `run` into the `tokio::select!` loop.
    rx_recp: Option<Receiver<RecpMessage>>,
}

impl Node {
    /// Construct a new Wahoo node (`node.go::NewNode`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        batch_size: usize,
        rx_messages: Receiver<WahooMessage>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_recp: Receiver<RecpMessage>,
        tx_committed: Sender<CommittedBlock>,
    ) -> Self {
        let mut authorities_sorted: Vec<PublicKey> =
            committee.authorities.keys().cloned().collect();
        authorities_sorted.sort();
        let node_num = authorities_sorted.len();
        // `quorumNum := int(math.Ceil(2*nodeNum/3))` — Go ceiling.
        let quorum_num = (2 * node_num + 2) / 3;
        // f = (n - 1) / 3, threshold = 2f so that 2f+1 = quorum_num
        // partials suffice to combine the Elect QC.
        let f = (node_num.saturating_sub(1)) / 3;
        let elect_threshold = 2 * f;
        let recp_threshold = crypto::recp_threshold(node_num);

        let pb = Pb::new(name, committee.clone());

        Self {
            name,
            committee,
            authorities_sorted,
            node_num,
            quorum_num,
            elect_threshold,
            recp_threshold,
            batch_size,
            signature_service,
            sender: ReliableSender::new(),
            cancel_handlers: Vec::new(),
            dag: HashMap::new(),
            pending_blocks: HashMap::new(),
            blocks_by_digest: HashMap::new(),
            chain: Chain {
                round: 0,
                blocks: HashMap::new(),
            },
            leader: HashMap::new(),
            done: HashMap::new(),
            elect: HashMap::new(),
            ready: HashMap::new(),
            recp_pool: HashMap::new(),
            rx_recp: Some(rx_recp),
            round: 1,
            move_round: HashMap::new(),
            next_round_signaled: HashSet::new(),
            leader_elect: HashSet::new(),
            block_send: HashSet::new(),
            done_send: HashSet::new(),
            pb,
            evaluation: Vec::new(),
            commit_time: Vec::new(),
            block_query: 0,
            tx_committed,
            rx_messages: Some(rx_messages),
            rx_workers: Some(rx_workers),
            pending_digests: Vec::new(),
        }
    }

    /// Drive the protocol forever. Mirrors `node.go::RunLoop` minus the
    /// bounded `roundNumber` termination — NovelDAG processes are
    /// long-running and rely on workspace lifecycle for shutdown.
    pub async fn run(mut self) {
        // Start round 1.
        self.broadcast_block(1).await;

        // Main message dispatch. Combines msg_handle.go::HandleMsgLoop
        // and the protocol-driving timers, plus a worker-digest fan-in
        // so worker-emitted batches are buffered for the next block.
        let mut rx = self.rx_messages.take().expect("messages already taken");
        let mut rx_workers = self
            .rx_workers
            .take()
            .expect("workers receiver already taken");
        let mut rx_recp = self
            .rx_recp
            .take()
            .expect("recp receiver already taken");
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    self.handle_message(msg).await;
                }
                Some((digest, wid)) = rx_workers.recv() => {
                    self.pending_digests.push((digest, wid));
                }
                Some(recp) = rx_recp.recv() => {
                    self.handle_recp(recp);
                }
                else => break,
            }
        }
    }

    /// Phase C Step 4a: absorb an incoming RECP share.
    ///
    /// Validation:
    ///   * author must be a committee member (stake > 0)
    ///   * (round, block_hash, author) must not already be in the pool
    ///     (silent dedup, not an error — RECPs may be broadcast multiple
    ///     times by peers)
    ///
    /// Cryptographic verification of the threshold share is deferred to
    /// Phase C Step 4b, which will run it through the BLS verifier when
    /// the leader-link builder consumes the pool.
    fn handle_recp(&mut self, recp: RecpMessage) {
        if self.committee.stake(&recp.author) == 0 {
            warn!(
                "Wahoo RECP: dropped share from unknown authority {}",
                recp.author
            );
            return;
        }
        let entry = self
            .recp_pool
            .entry(recp.round)
            .or_insert_with(HashMap::new)
            .entry(recp.block_hash.clone())
            .or_insert_with(HashMap::new);
        if entry.contains_key(&recp.author) {
            debug!(
                "Wahoo RECP: duplicate share from {} at round {} for block {}",
                recp.author, recp.round, recp.block_hash
            );
            return;
        }
        let (round, author, block_hash) = (recp.round, recp.author, recp.block_hash.clone());
        entry.insert(author, recp);
        debug!(
            "Wahoo RECP: pool[r={}][h={}] now has {} share(s)",
            round,
            block_hash,
            entry.len()
        );
    }

    /// Phase C Step 4a: expose pool contents for the leader-link builder
    /// and for unit tests. Returns the (author -> share) map for a given
    /// (round, block_hash), or an empty view if none are known.
    #[allow(dead_code)]
    pub(crate) fn recp_shares_for(
        &self,
        round: Round,
        block_hash: &Digest,
    ) -> Option<&HashMap<PublicKey, RecpMessage>> {
        self.recp_pool.get(&round).and_then(|m| m.get(block_hash))
    }

    /// Phase C Step 4b: build the `leader_link` for an EPBC header at
    /// `round` (odd, >= 3). Consults `recp_pool[round - 2]` — the RECP
    /// shares peers broadcast over blocks delivered at the previous
    /// wave's EPBC phase.
    ///
    /// Returns:
    ///   * `Some(ExclusiveCommit)` if some block hash collected f+1
    ///     shares (paper Section IV-B: "the previous leader's block was
    ///     committed at tier >= TS2 — commit it"). The proof bytes are a
    ///     simple concatenation of the share bytes; full BLS aggregate
    ///     verification lands when threshold crypto is wired in.
    ///   * `Some(NoCommit)` if at least n-f distinct authors broadcast
    ///     RECPs (regardless of which block) but no single block
    ///     reached f+1 (paper: "no possible commit — safe to skip").
    ///   * `None` for round < 3 (no previous EPBC) or if neither
    ///     threshold is met yet.
    ///
    /// When both conditions hold (f+1 same-block AND n-f distinct
    /// authors total), `ExclusiveCommit` takes precedence because it
    /// conveys strictly more information.
    #[allow(dead_code)]
    pub(crate) fn build_leader_link(&self, round: Round) -> Option<LeaderLink> {
        if round < 3 || round % 2 == 0 {
            return None;
        }
        let prev = round - 2;
        let buckets = self.recp_pool.get(&prev)?;
        let f_plus_1: Stake = self.committee.validity_threshold();
        let total_stake: Stake = self
            .committee
            .authorities
            .keys()
            .map(|name| self.committee.stake(name))
            .sum();
        let n_minus_f = total_stake - f_plus_1 + 1;

        // Pass 1: ExclusiveCommit — any single block with >= f+1 stake?
        // Iterate in a deterministic order so all honest nodes pick the
        // same proof if multiple blocks happen to be eligible (only
        // possible under equivocation, where any choice is safe).
        let mut sorted_blocks: Vec<&Digest> = buckets.keys().collect();
        sorted_blocks.sort();
        for h in &sorted_blocks {
            let entries = &buckets[*h];
            let weight: Stake = entries.keys().map(|a| self.committee.stake(a)).sum();
            if weight >= f_plus_1 {
                // Phase C Step 4d: combine the per-author BLS partial
                // signatures over `(prev_round, block_hash)` into a
                // single aggregate signature via threshold_crypto's
                // Lagrange interpolation. `proof_bytes` is now a real
                // 96-byte BLS signature that any verifier can check
                // against the master public key derived from the
                // committee — see `LeaderLink::verify_with_crypto`.
                let shares: Vec<(PublicKey, Vec<u8>)> = entries
                    .iter()
                    .map(|(a, recp)| (*a, recp.share.clone()))
                    .collect();
                let proof_bytes = match crypto::combine_recp_shares(
                    &self.authorities_sorted,
                    self.recp_threshold,
                    prev,
                    *h,
                    &shares,
                ) {
                    Some(p) => p,
                    None => {
                        // Combine failed (shares didn't verify
                        // individually, or insufficient distinct
                        // indices). Fall through to NoCommit pass.
                        log::warn!(
                            "Wahoo build_leader_link: combine_recp_shares failed for round {} hash {:?}; falling back to NoCommit",
                            prev, h
                        );
                        continue;
                    }
                };
                return Some(LeaderLink {
                    hash: Some((*h).clone()),
                    proof: LeaderProof::ExclusiveCommit(proof_bytes),
                });
            }
        }

        // Pass 2: NoCommit — do we have n-f distinct authors total?
        // For each author seen anywhere in `prev`, keep ONE RecpMessage
        // (deterministically the lexicographically-smallest block_hash
        // they signed on, so honest nodes converge on the same proof).
        let mut per_author: HashMap<PublicKey, RecpMessage> = HashMap::new();
        for h in &sorted_blocks {
            for (author, recp) in &buckets[*h] {
                per_author.entry(*author).or_insert_with(|| recp.clone());
            }
        }
        let weight: Stake = per_author.keys().map(|a| self.committee.stake(a)).sum();
        if weight >= n_minus_f {
            let mut recps: Vec<RecpMessage> = per_author.into_values().collect();
            // Deterministic ordering for byte-identical serialisation
            // across honest nodes (their RecpMessage sets are equal
            // post-dedup; sorting kills HashMap iteration nondeterminism).
            recps.sort_by_key(|r| r.author);
            return Some(LeaderLink {
                hash: None,
                proof: LeaderProof::NoCommit(recps),
            });
        }
        None
    }

    /// Phase C Step 4c+4d: self-broadcast RECP shares for blocks we've
    /// delivered at `prev_round` (paper Algorithm 2 line 5).
    ///
    /// Iterates every block in `self.dag[prev_round]`, computes a real
    /// `(f+1)`-threshold BLS partial signature over `(prev_round,
    /// block_hash)` via `crypto::make_recp_share`, and broadcasts a
    /// `PrimaryMessage::Recp(_)`. Threshold parameter `f` is derived
    /// from `node_num` via `crypto::recp_threshold` so all nodes agree
    /// on the same key set.
    ///
    /// Self-delivers each emitted RECP into our own pool so the local
    /// `build_leader_link(prev_round + 2)` sees our share without a
    /// network round trip.
    async fn broadcast_self_recps(&mut self, prev_round: Round) {
        let blocks: Vec<WahooBlock> = self
            .dag
            .get(&prev_round)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        for block in blocks {
            let block_hash = crypto::Hash::digest(&block);
            let share = match crypto::make_recp_share(
                &self.authorities_sorted,
                self.recp_threshold,
                &self.name,
                prev_round,
                &block_hash,
            ) {
                Some(s) => s,
                None => {
                    log::warn!(
                        "Wahoo RECP: failed to produce BLS share (round={}, author={}); skipping",
                        prev_round,
                        self.name
                    );
                    continue;
                }
            };
            let recp = RecpMessage {
                block_hash: block_hash.clone(),
                round: prev_round,
                author: self.name,
                share,
            };
            // Self-deliver to our own pool.
            self.handle_recp(recp.clone());
            // Broadcast to peers.
            let hs =
                msg_send::broadcast_recp(&mut self.sender, &self.committee, &self.name, recp)
                    .await;
            self.cancel_handlers.extend(hs);
        }
    }

    // ============================================================
    //  Inbound message dispatch — `wahoo/msg_handle.go::HandleMsgLoop`
    // ============================================================

    async fn handle_message(&mut self, msg: WahooMessage) {
        match msg {
            WahooMessage::Block(b) => {
                info!(
                    "Wahoo recv Block round={} sender={} tag={:?}",
                    b.round, b.author, b.wahoo_tag
                );
                if b.round % 2 == 0 {
                    let actions = self.pb.handle_block(b);
                    self.dispatch_pb_actions(actions).await;
                } else {
                    self.handle_fast_block(b).await;
                }
            }
            WahooMessage::Vote(v) => {
                let actions = self.pb.handle_vote(v);
                self.dispatch_pb_actions(actions).await;
            }
            WahooMessage::Elect(e) => self.handle_elect(e).await,
            WahooMessage::Ready(r) => self.handle_ready(r).await,
            WahooMessage::Done(d) => self.handle_done(d).await,
            WahooMessage::ReVote(rv) => {
                debug!(
                    "ReVote received from {} for round {} (proposer {})",
                    rv.revote_sender, rv.round, rv.block_sender
                );
                // `msg_handle.go` leaves ReVote as a "TO DO..." stub.
                // Mirror that.
            }
        }
    }

    /// Sign a `WahooMessage` for transport. Mirrors the Go envelope
    /// (`wahoo/msg_send.go::broadcast/send` calling `sign.SignEd25519`
    /// over the encoded message). The digest function must match
    /// `primary::wahoo_digest` so that
    /// `WahooReceiverHandler::dispatch` can verify with the sender's
    /// public key.
    async fn sign_wahoo(&mut self, msg: WahooMessage) -> SignedWahoo {
        let payload = bincode::serialize(&msg).expect("Failed to serialize Wahoo message");
        let digest = crate::primary::wahoo_digest(&payload);
        let sig = self.signature_service.request_signature(digest).await;
        SignedWahoo { msg, sig }
    }

    /// Execute the action list emitted by PB.
    async fn dispatch_pb_actions(&mut self, actions: Vec<PbAction>) {
        for action in actions {
            match action {
                PbAction::SendVote { target, mut vote } => {
                    // Phase B Step 3d: sign the vote inline and send as
                    // `PrimaryMessage::Vote`, not `SignedWahoo`.
                    vote.signature = self
                        .signature_service
                        .request_signature(vote.digest())
                        .await;
                    let h = msg_send::send_vote(
                        &mut self.sender,
                        &self.committee,
                        &target,
                        vote,
                    )
                    .await;
                    self.cancel_handlers.push(h);
                }
                PbAction::BroadcastBlock2(block, _pbc_cert) => {
                    // `_pbc_cert` carries the 2f+1 PBC quorum evidence
                    // assembled by `WahooVotesAggregator`. Phase D will
                    // route it onto the wire as `Certificate` directly;
                    // for now we keep emitting Block2 (a synthesised
                    // empty header carrying `wahoo_tag = PbcVoteComplete`)
                    // but it now travels as `PrimaryMessage::Header`,
                    // signed inline.
                    let mut block = block;
                    block.signature = self
                        .signature_service
                        .request_signature(block.id.clone())
                        .await;
                    let hs = msg_send::broadcast_header(
                        &mut self.sender,
                        &self.committee,
                        &self.name,
                        block,
                    )
                    .await;
                    self.cancel_handlers.extend(hs);
                }
                PbAction::OutputBlock(block) => {
                    self.handle_pb_output(block).await;
                }
            }
        }
    }

    /// `msg_handle.go::handlePBBlock` — block delivered from PB into the
    /// DAG layer.
    async fn handle_pb_output(&mut self, block: WahooBlock) {
        debug!(
            "PB delivered block round={} sender={}",
            block.round, block.author
        );
        self.try_to_update_dag(block).await;
    }

    /// `msg_handle.go::handleFastBlockMsg`.
    async fn handle_fast_block(&mut self, block: WahooBlock) {
        let hash = block.digest();
        let round = block.round;
        let sender = block.author;
        info!("Wahoo fast_block round={} sender={}", round, sender);
        // Fast-path blocks are inserted into DAG immediately.
        self.try_to_update_dag(block).await;
        // Send Ready unless we have already advanced past this odd round.
        // (Mirrors `n.blockSend[block.Round+1]` check in Go.)
        if !self.block_send.contains(&(round + 1)) {
            self.send_ready(round, hash, sender).await;
        } else {
            info!("Wahoo fast_block: skip Ready for round={} (already sent r+1)", round);
        }
    }

    /// `msg_handle.go::handleReadyMsg`.
    async fn handle_ready(&mut self, ready: WahooReady) {
        self.store_ready(&ready);
        self.check_if_enough_ready(ready).await;
    }

    /// `msg_handle.go::handleElectMsg`.
    async fn handle_elect(&mut self, elect: WahooElect) {
        self.store_elect(&elect);
        self.try_to_elect_leader(elect.round).await;
    }

    /// `msg_handle.go::handleDoneMsg`.
    async fn handle_done(&mut self, done: WahooDone) {
        debug!(
            "Done received from {} round {} (proposer {})",
            done.done_sender, done.round, done.block_sender
        );
        self.store_done(&done);
        let round = done.round;
        self.try_to_next_round(round).await;
        self.try_to_commit_leader(round).await;
    }

    // ============================================================
    //  Outbound primitives — `wahoo/msg_send.go`
    // ============================================================

    /// `msg_send.go::broadcastBlock` (and proposer's `pb.BroadcastBlock`
    /// for even rounds). Drives RunLoop's per-round emission.
    /// Boxed because `try_to_next_round` (which broadcasts the next
    /// round's block) is called downstream of message handlers that
    /// themselves go through `broadcast_block` via self-delivery.
    fn broadcast_block(&mut self, round: Round) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            let previous_hash = self.select_previous_blocks(round.saturating_sub(1));
            let block = self.new_block(round, previous_hash);
            self.block_send.insert(round);

            // Benchmark log: emit one `Created B{round}({author}) -> {digest}`
            // line per batch digest carried by this block, exactly the
            // way `proposer.rs::make_header` does for
            // Narwhal/Bullshark/NovelDAG. `consensus::wahoo::run` emits
            // a matching `Committed ...` line for each digest when the
            // block lands in the chain (its loop already iterates
            // `header.payload.keys()`), and the worker-side
            // `Batch <d> contains <n> B` log feeds `LogParser.sizes`.
            // This puts all four protocols on the exact same accounting
            // pipeline — TPS and latency become directly comparable.
            #[cfg(feature = "benchmark")]
            for digest in block.payload.keys() {
                info!("Created B{}({}) -> {:?}", round, self.name, digest);
            }

            // Phase B Step 3d: sign the header inline so it travels as
            // a first-class `PrimaryMessage::Header` instead of being
            // wrapped in `SignedWahoo`. The `signature` field is set
            // here (after `new_block` locked in the `id`); peers verify
            // via `block.signature.verify(&block.id, &block.author)` in
            // `WahooReceiverHandler`.
            let mut block = block;
            block.signature = self
                .signature_service
                .request_signature(block.id.clone())
                .await;
            if round % 2 == 0 {
                // Even round: PB phase 1 broadcast.
                self.pb.broadcast_block(&block);
                let hs = msg_send::broadcast_header(
                    &mut self.sender,
                    &self.committee,
                    &self.name,
                    block.clone(),
                )
                .await;
                self.cancel_handlers.extend(hs);
                // Self-deliver the proposal to PB so handle_block_msg
                // paths are exercised symmetrically with peers.
                let actions = self.pb.handle_block(block);
                self.dispatch_pb_actions(actions).await;
                // Elect for the leader of the previous (odd) round.
                self.broadcast_elect(round).await;
            } else {
                // Odd round: fast path, direct broadcast.
                // Phase C Step 4c: at the START of each EPBC phase, paper
                // Section IV-B Algorithm 2 line 5 says every node broadcasts
                // a RECP share for the block it delivered at the previous
                // EPBC wave (round - 2). This lets the NEXT wave's proposer
                // build a leader_link out of `recp_pool[round]` two rounds
                // later. Broadcast for every delivered round-(round-2)
                // block in our DAG so peers see the full reception fan-in.
                if round >= 3 {
                    self.broadcast_self_recps(round - 2).await;
                }
                let hs = msg_send::broadcast_header(
                    &mut self.sender,
                    &self.committee,
                    &self.name,
                    block.clone(),
                )
                .await;
                self.cancel_handlers.extend(hs);
                // Self-deliver to keep our own DAG and Ready logic in sync.
                self.handle_fast_block(block).await;
            }
        })
    }

    /// `msg_send.go::sendReady` — unicast Ready+partial-sig to proposer.
    async fn send_ready(&mut self, round: Round, hash: Digest, block_sender: PublicKey) {
        let partial_sig = self.signature_service.request_signature(hash.clone()).await;
        let ready = WahooReady {
            ready_sender: self.name,
            block_sender,
            round,
            hash,
            partial_sig: partial_sig.to_bytes().to_vec(),
        };
        // Self-deliver as well to keep counts symmetric across peers.
        self.handle_ready(ready.clone()).await;
        let signed = self.sign_wahoo(WahooMessage::Ready(ready)).await;
        let h = msg_send::send(&mut self.sender, &self.committee, &block_sender, signed).await;
        self.cancel_handlers.push(h);
    }

    /// `msg_send.go::broadcastElect`.
    async fn broadcast_elect(&mut self, round: Round) {
        let partial_sig = crypto::make_coin_share(
            &self.authorities_sorted,
            self.elect_threshold,
            &self.name,
            round,
        )
        .expect("Wahoo Elect: this authority is in the committee");
        let elect = WahooElect {
            sender: self.name,
            round,
            partial_sig,
        };
        // Self-deliver.
        self.handle_elect(elect.clone()).await;
        let signed = self.sign_wahoo(WahooMessage::Elect(elect)).await;
        let hs =
            msg_send::broadcast(&mut self.sender, &self.committee, &self.name, signed).await;
        self.cancel_handlers.extend(hs);
    }

    /// `msg_send.go::broadcastDone`.
    async fn broadcast_done(&mut self, done: WahooDone) {
        let signed = self.sign_wahoo(WahooMessage::Done(done)).await;
        let hs =
            msg_send::broadcast(&mut self.sender, &self.committee, &self.name, signed).await;
        self.cancel_handlers.extend(hs);
    }

    // ============================================================
    //  Storage helpers — `wahoo/node.go::store*`
    // ============================================================

    fn store_done(&mut self, done: &WahooDone) {
        let round_done = self
            .done
            .entry(done.round)
            .or_insert_with(HashMap::new);
        if !round_done.contains_key(&done.block_sender) {
            round_done.insert(done.block_sender, done.clone());
            *self.move_round.entry(done.round).or_insert(0) += 1;
        }
    }

    fn store_ready(&mut self, ready: &WahooReady) {
        self.ready
            .entry(ready.round)
            .or_insert_with(HashMap::new)
            .entry(ready.block_sender)
            .or_insert_with(HashMap::new)
            .insert(ready.ready_sender, ready.partial_sig.clone());
    }

    fn store_elect(&mut self, elect: &WahooElect) {
        self.elect
            .entry(elect.round)
            .or_insert_with(HashMap::new)
            .insert(elect.sender, elect.partial_sig.clone());
    }

    fn store_pending_block(&mut self, block: WahooBlock) {
        self.pending_blocks
            .entry(block.round)
            .or_insert_with(HashMap::new)
            .insert(block.author, block);
    }

    // ============================================================
    //  DAG maintenance — `node.go::tryToUpdateDAG*`
    // ============================================================

    /// `node.go::selectPreviousBlocks` — returns the digests of every
    /// block we've accepted at the requested round. Step 3a-ii dropped the
    /// per-author keying since the unified `Header.parents` is a plain
    /// set; the on-the-wire shape is now identical to NovelDAG's.
    fn select_previous_blocks(&self, round: Round) -> BTreeSet<Digest> {
        if round == 0 {
            return BTreeSet::new();
        }
        let mut out = BTreeSet::new();
        if let Some(level) = self.dag.get(&round) {
            for block in level.values() {
                out.insert(block.digest());
            }
        }
        out
    }

    fn try_to_update_dag(&mut self, block: WahooBlock) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            if self.check_whether_can_add_to_dag(&block) {
                let round = block.round;
                let sender = block.author;
                // Maintain the digest index BEFORE moving the block into
                // `dag` so that subsequent parent lookups by other blocks
                // in the same round can resolve to this one.
                self.blocks_by_digest.insert(block.digest(), block.clone());
                self.dag
                    .entry(round)
                    .or_insert_with(HashMap::new)
                    .insert(sender, block);
                if round % 2 == 0 {
                    *self.move_round.entry(round).or_insert(0) += 1;
                    self.try_to_next_round(round).await;
                } else {
                    self.try_to_commit_leader(round).await;
                }
                self.try_to_update_dag_from_pending(round + 1).await;
            } else {
                self.store_pending_block(block);
                self.block_query += 1;
            }
        })
    }

    fn try_to_update_dag_from_pending(
        &mut self,
        round: Round,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            let drained = match self.pending_blocks.remove(&round) {
                Some(m) => m,
                None => return,
            };
            for (_, block) in drained {
                self.try_to_update_dag(block).await;
            }
        })
    }

    /// `node.go::checkWhetherCanAddToDAG` — every parent digest must be a
    /// block we have already accepted into the dag, and its round must be
    /// exactly `block.round - 1`.
    fn check_whether_can_add_to_dag(&self, block: &WahooBlock) -> bool {
        if block.round == 0 {
            return true;
        }
        let expected_parent_round = block.round - 1;
        if block.parents.is_empty() {
            // Empty parent set is only legal pre-genesis; matches the Go
            // reference's behaviour of returning early when `dag[r-1]` is
            // missing AND `previous_hash` is empty.
            return !self.dag.contains_key(&expected_parent_round);
        }
        for digest in &block.parents {
            match self.blocks_by_digest.get(digest) {
                Some(parent) if parent.round == expected_parent_round => {}
                _ => return false,
            }
        }
        true
    }

    // ============================================================
    //  Leader election — `node.go::tryToElectLeader`
    // ============================================================

    async fn try_to_elect_leader(&mut self, round: Round) {
        if self.leader_elect.contains(&round) {
            return;
        }
        let elects = match self.elect.get(&round) {
            Some(e) => e,
            None => return,
        };
        if elects.len() < self.quorum_num {
            return;
        }
        self.leader_elect.insert(round);

        let shares: Vec<(PublicKey, Vec<u8>)> = elects
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        let coin = crypto::recover_coin(
            &self.authorities_sorted,
            self.elect_threshold,
            round,
            &shares,
        );
        let coin = match coin {
            Some(c) => c,
            None => {
                warn!("Wahoo: failed to recover Elect coin for round {}", round);
                return;
            }
        };
        // Wahoo Go: leaderId = qcAsInt % n.nodeNum (uses just the first 4
        // bytes of the BLS sig). Our recover_coin already collapses the
        // BLS signature to a u64 — we mod by node_num to match.
        let leader_id = (coin as usize) % self.node_num;
        let leader_name = self.authorities_sorted[leader_id];
        let prev_round = round.saturating_sub(1);
        self.leader.insert(prev_round, leader_name);
        debug!(
            "Wahoo elect: round {} elects leader {:?} for round {}",
            round, leader_name, prev_round
        );

        // node.go also broadcasts a stub ReVote here (`broadcastReVote(round-1, true)`).
        // The ReVote handler is itself a stub in the Go reference, so we
        // omit the broadcast for behavioural fidelity (no observable
        // effect beyond extra network traffic).

        self.try_to_commit_leader(prev_round).await;
    }

    // ============================================================
    //  Round advancement — `node.go::tryToNextRound`,
    //                      `node.go::checkIfEnoughReady`
    // ============================================================

    async fn check_if_enough_ready(&mut self, ready: WahooReady) {
        let round = ready.round;
        let block_sender = ready.block_sender;
        let count = self
            .ready
            .get(&round)
            .and_then(|m| m.get(&block_sender))
            .map(|m| m.len())
            .unwrap_or(0);
        info!(
            "Wahoo Ready count: round={} block_sender={} count={}/{}",
            round, block_sender, count, self.node_num
        );
        // Paper ALGepbc dual-path (Section IV-B):
        //   Fast path (TF):  broadcaster receives n    Readies → Done immediately.
        //   Slow path (TS1): broadcaster receives n-f  Readies → Done as well.
        // n-f == quorum_num in this codebase (ceil(2n/3) >= n-f for all n,f).
        // We only broadcast Done for our own block (block_sender == self.name).
        let threshold_met = block_sender == self.name
            && count >= self.quorum_num
            && !self.done_send.contains(&round);
        if threshold_met {
            self.done_send.insert(round);
            let done = WahooDone {
                done_sender: self.name,
                block_sender,
                done: Vec::new(),
                hash: Digest::default(),
                round,
            };
            self.handle_done(done.clone()).await;
            self.broadcast_done(done).await;
        }
    }

    async fn try_to_next_round(&mut self, round: Round) {
        if round != self.round {
            return;
        }
        let count = *self.move_round.get(&round).unwrap_or(&0);
        info!(
            "Wahoo try_to_next_round: round={} move_count={}/{} next_signaled={}",
            round,
            count,
            self.quorum_num,
            self.next_round_signaled.contains(&round)
        );
        if count >= self.quorum_num && !self.next_round_signaled.contains(&round) {
            self.next_round_signaled.insert(round);
            self.round += 1;
            let next = self.round;
            self.broadcast_block(next).await;
            self.try_to_next_round_boxed(next).await;
        }
    }

    fn try_to_next_round_boxed(
        &mut self,
        round: Round,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move { self.try_to_next_round(round).await })
    }

    // ============================================================
    //  Commit pipeline — `node.go::tryToCommitLeader*`
    // ============================================================

    async fn try_to_commit_leader(&mut self, round: Round) {
        if round <= self.chain.round {
            return;
        }
        let leader = match self.leader.get(&round) {
            Some(l) => *l,
            None => return,
        };
        let done_seen = self
            .done
            .get(&round)
            .map_or(false, |m| m.contains_key(&leader));
        if !done_seen {
            return;
        }
        let block = match self.dag.get(&round).and_then(|m| m.get(&leader)) {
            Some(b) => b.clone(),
            None => return,
        };
        // Walk back through past unhappy leaders that are linked.
        self.try_to_commit_ancestor_leader(round).await;

        let hash = block.digest();
        self.chain.round = round;
        self.chain.blocks.insert(hash, block.clone());
        info!(
            "Wahoo commit leader: round={} proposer={}",
            round, block.author
        );
        // Step 3a-iii: `WahooBlock.timestamp` was dropped to align with
        // `Header`. We no longer report per-block latency here; the
        // benchmark uses the unified `Created`/`Committed` log timestamps.
        let latency = 0i64;
        self.evaluation.push(latency);
        // Walk parents transitively.
        self.commit_ancestor_blocks(round).await;
        self.commit_time.push(unix_nano_now());

        // Emit the leader block to consensus. (Ancestor blocks are emitted
        // inside commit_ancestor_blocks.)
        let _ = self
            .tx_committed
            .send(CommittedBlock {
                block,
                commit_latency_nanos: latency,
            })
            .await;
    }

    async fn try_to_commit_ancestor_leader(&mut self, round: Round) {
        if round < 2 || round.saturating_sub(2) <= self.chain.round {
            return;
        }
        let valid = self.find_valid_leader(round);
        // Iterate odd rounds 1, 3, ..., round-2.
        let mut r = 1u64;
        while r < round {
            if let Some(_) = valid.get(&r) {
                let leader = match self.leader.get(&r) {
                    Some(l) => *l,
                    None => {
                        r += 2;
                        continue;
                    }
                };
                let block = match self.dag.get(&r).and_then(|m| m.get(&leader)) {
                    Some(b) => b.clone(),
                    None => {
                        r += 2;
                        continue;
                    }
                };
                let hash = block.digest();
                self.chain.round = r;
                self.chain.blocks.insert(hash, block.clone());
                info!(
                    "Wahoo commit ancestor leader: round={} proposer={}",
                    r, block.author
                );
                let latency = 0i64;
                self.evaluation.push(latency);
                let _ = self
                    .tx_committed
                    .send(CommittedBlock {
                        block,
                        commit_latency_nanos: latency,
                    })
                    .await;
                self.commit_ancestor_blocks_box(r).await;
            }
            r += 2;
        }
    }

    fn find_valid_leader(&self, round: Round) -> HashMap<Round, PublicKey> {
        let mut valid = HashMap::new();
        let mut frontier: HashMap<Round, HashMap<Digest, WahooBlock>> = HashMap::new();
        let leader = match self.leader.get(&round) {
            Some(l) => *l,
            None => return valid,
        };
        let block = match self.dag.get(&round).and_then(|m| m.get(&leader)) {
            Some(b) => b.clone(),
            None => return valid,
        };
        let hash = block.digest();
        let mut start = HashMap::new();
        start.insert(hash, block);
        frontier.insert(round, start);

        let mut r = round;
        while r > 0 && r > self.chain.round {
            let mut next_level: HashMap<Digest, WahooBlock> = HashMap::new();
            if let Some(level) = frontier.get(&r) {
                for b in level.values() {
                    if b.round % 2 == 1 {
                        if let Some(l) = self.leader.get(&b.round) {
                            if *l == b.author {
                                valid.insert(b.round, b.author);
                            }
                        }
                    }
                    if r == 0 {
                        continue;
                    }
                    let parent_round = r - 1;
                    let _ = parent_round; // round is implied by the parent block; kept for parity with Go.
                    for digest in &b.parents {
                        if let Some(pb) = self.blocks_by_digest.get(digest) {
                            next_level.insert(digest.clone(), pb.clone());
                        }
                    }
                }
            }
            if r == 0 {
                break;
            }
            r -= 1;
            frontier.insert(r, next_level);
        }

        valid
    }

    fn commit_ancestor_blocks_box(
        &mut self,
        round: Round,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move { self.commit_ancestor_blocks(round).await })
    }

    /// `node.go::commitAncestorBlocks` — flatten the ancestor sub-DAG
    /// rooted at the leader, emit blocks in the same order Go would.
    async fn commit_ancestor_blocks(&mut self, round: Round) {
        let leader = match self.leader.get(&round) {
            Some(l) => *l,
            None => return,
        };
        let leader_block = match self.dag.get(&round).and_then(|m| m.get(&leader)) {
            Some(b) => b.clone(),
            None => return,
        };

        let mut frontier: HashMap<Round, HashMap<Digest, WahooBlock>> = HashMap::new();
        let leader_hash = leader_block.digest();
        let mut start = HashMap::new();
        start.insert(leader_hash, leader_block);
        frontier.insert(round, start);

        let mut r = round;
        loop {
            // Collect blocks at level r whose hash isn't already in chain,
            // and queue parents at r-1.
            let mut next_level: HashMap<Digest, WahooBlock> = HashMap::new();
            let level = match frontier.get(&r) {
                Some(l) => l.clone(),
                None => HashMap::new(),
            };
            for (h, b) in level.iter() {
                if !self.chain.blocks.contains_key(h) {
                    self.chain.blocks.insert(h.clone(), b.clone());
                    let latency = 0i64;
                    self.evaluation.push(latency);
                    // Don't re-emit the leader (already sent in
                    // try_to_commit_leader); emit other ancestors.
                    if !(b.round == round && b.author == leader) {
                        let _ = self
                            .tx_committed
                            .send(CommittedBlock {
                                block: b.clone(),
                                commit_latency_nanos: latency,
                            })
                            .await;
                    }
                }
                if r > 0 {
                    for digest in &b.parents {
                        if let Some(pb) = self.blocks_by_digest.get(digest) {
                            if !self.chain.blocks.contains_key(digest) {
                                next_level.insert(digest.clone(), pb.clone());
                            }
                        }
                    }
                }
            }
            if next_level.is_empty() || r == 0 {
                break;
            }
            r -= 1;
            frontier.insert(r, next_level);
        }
    }

    // ============================================================
    //  Block construction — `node.go::NewBlock`
    // ============================================================

    fn new_block(
        &mut self,
        round: Round,
        parents: BTreeSet<Digest>,
    ) -> WahooBlock {
        // Pack as many pending worker-batch digests as we have. We do
        // not gate round advancement on payload size — Wahoo rounds are
        // driven by Done/Ready quorums, not by `header_size` like
        // `Proposer`. Empty blocks are legal and just have no Created/
        // Committed lines, which is fine for the benchmark.
        let payload_digests: BTreeMap<Digest, WorkerId> =
            self.pending_digests.drain(..).collect();
        // `txs` is left empty: the protocol never inspects it, and the
        // benchmark accounting now flows through `payload_digests` ->
        // worker-emitted `Batch ... contains ... B` lines, identical to
        // the other three protocols.
        let _ = self.batch_size;
        // Phase B Step 3b: `WahooBlock` is now `messages::Header`. The Go
        // `Tag = Proposal` maps to `wahoo_tag = Some(Pbc)` at even rounds
        // (PB phase 1) and to `Some(EpbcTf)` at odd rounds (fast path).
        let tag = if round % 2 == 0 {
            WahooTag::Pbc
        } else {
            WahooTag::EpbcTf
        };
        // Phase C Step 4b: for odd-round EPBC headers, consult the RECP
        // pool from the previous wave (round - 2) and build the
        // leader-link proof per paper Section IV-B Algorithm 2 lines 6-20.
        // `build_leader_link` returns `None` for round < 3 (no previous
        // EPBC) or if neither the ExclusiveCommit nor NoCommit threshold
        // is met; in those cases we emit a header without `leader_link`,
        // which `Header::verify_wahoo_structure` accepts for EpbcTf.
        let leader_link = if round % 2 == 1 {
            self.build_leader_link(round)
        } else {
            None
        };
        let mut header = WahooBlock {
            author: self.name,
            round,
            parents,
            payload: payload_digests,
            wahoo_tag: Some(tag),
            leader_link,
            ..WahooBlock::default()
        };
        // Lock in the canonical digest. Phase B Step 3d (post-cutover):
        // the Wahoo Node now sends headers as `PrimaryMessage::Header`
        // with an inline `signature` field set in `broadcast_block` /
        // `dispatch_pb_actions` immediately after `new_block` returns.
        // We leave `header.signature` default here so the digest stays
        // independent of the signature (callers fill it via
        // `signature_service.request_signature(header.id)` next).
        header.id = crypto::Hash::digest(&header);
        header
    }
}

/// `node.go::Stake` is not used in the port (Wahoo doesn't weight votes),
/// but we keep this import path live so future stake-aware extensions
/// don't have to re-wire imports.
#[allow(dead_code)]
fn _stake_marker() -> Stake {
    0
}
#[allow(dead_code)]
fn _bytes_marker() -> Option<Bytes> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Authority, Committee, PrimaryAddresses, WorkerAddresses};
    use crypto::{generate_keypair, SignatureService};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;

    fn make_test_committee(n: usize) -> (Vec<crypto::PublicKey>, Vec<crypto::SecretKey>, Committee) {
        let mut publics = Vec::new();
        let mut secrets = Vec::new();
        let mut authorities = BTreeMap::new();
        for i in 0..n {
            let mut rng =
                rand::rngs::StdRng::from_seed([i as u8; 32]);
            let (pk, sk) = generate_keypair(&mut rng);
            publics.push(pk);
            secrets.push(sk);
            // Distinct ephemeral-style ports per authority. 0 lets the
            // OS assign at bind time but we don't actually bind in this
            // test, so any sentinel works.
            let prim = PrimaryAddresses {
                primary_to_primary: format!("127.0.0.1:{}", 5000 + i)
                    .parse::<SocketAddr>()
                    .unwrap(),
                worker_to_primary: format!("127.0.0.1:{}", 6000 + i)
                    .parse::<SocketAddr>()
                    .unwrap(),
            };
            let mut workers = std::collections::HashMap::new();
            workers.insert(
                0u32,
                WorkerAddresses {
                    transactions: format!("127.0.0.1:{}", 7000 + i)
                        .parse::<SocketAddr>()
                        .unwrap(),
                    worker_to_worker: format!("127.0.0.1:{}", 8000 + i)
                        .parse::<SocketAddr>()
                        .unwrap(),
                    primary_to_worker: format!("127.0.0.1:{}", 9000 + i)
                        .parse::<SocketAddr>()
                        .unwrap(),
                },
            );
            authorities.insert(
                pk,
                Authority {
                    stake: 1,
                    primary: prim,
                    workers,
                },
            );
        }
        (publics, secrets, Committee { authorities })
    }

    use rand::SeedableRng as _;

    /// Sanity: `Node::new` initialises every field to the same shape as
    /// `wahoo/node.go::NewNode` (round=1, empty maps, batch_size honoured,
    /// quorum_num = ceil(2n/3)).
    #[tokio::test]
    async fn node_new_initial_state() {
        let (publics, secrets, committee) = make_test_committee(4);
        // Pick the first authority as our identity.
        let me = publics[0];
        // Move the secret out so SignatureService takes ownership.
        let mut secrets = secrets;
        let secret = secrets.remove(0);
        let sig_service = SignatureService::new(secret);

        let (_tx_msg, rx_msg) = tokio::sync::mpsc::channel(64);
        let (_tx_workers, rx_workers) = tokio::sync::mpsc::channel(64);
        let (_tx_recp, rx_recp) = tokio::sync::mpsc::channel(64);
        let (tx_committed, _rx_committed) = tokio::sync::mpsc::channel(64);

        let node = Node::new(
            me, committee, sig_service, 4, rx_msg, rx_workers, rx_recp, tx_committed,
        );
        assert_eq!(node.round, 1);
        assert_eq!(node.node_num, 4);
        // ceil(2*4/3) = 3
        assert_eq!(node.quorum_num, 3);
        // f=1, threshold=2f=2 → 2f+1=3 partials recover.
        assert_eq!(node.elect_threshold, 2);
        assert_eq!(node.batch_size, 4);
        assert_eq!(node.chain.round, 0);
        assert!(node.dag.is_empty());
    }

    /// Build a 4-round Wahoo DAG by hand, place a leader at round 1 with
    /// a Done message, and verify `try_to_commit_leader` produces a
    /// CommittedBlock for the leader (round 1) plus (optionally) round-0
    /// ancestors via `commit_ancestor_blocks`.
    #[tokio::test]
    async fn try_to_commit_leader_emits_block() {
        let (publics, secrets, committee) = make_test_committee(4);
        let me = publics[0];
        let leader = publics[1];
        let mut secrets = secrets;
        let secret = secrets.remove(0);
        let sig_service = SignatureService::new(secret);
        let (_tx_msg, rx_msg) = tokio::sync::mpsc::channel(64);
        let (_tx_workers, rx_workers) = tokio::sync::mpsc::channel(64);
        let (_tx_recp, rx_recp) = tokio::sync::mpsc::channel(64);
        let (tx_committed, mut rx_committed) = tokio::sync::mpsc::channel(64);
        let mut node = Node::new(
            me,
            committee,
            sig_service,
            1,
            rx_msg,
            rx_workers,
            rx_recp,
            tx_committed,
        );

        // Insert a leader block at round 1 (odd).
        let leader_block = {
            let mut b = WahooBlock {
                author: leader,
                round: 1,
                parents: BTreeSet::new(),
                payload: BTreeMap::new(),
                wahoo_tag: Some(WahooTag::EpbcTf),
                ..WahooBlock::default()
            };
            b.id = crypto::Hash::digest(&b);
            b
        };
        node.blocks_by_digest.insert(leader_block.digest(), leader_block.clone());
        node.dag
            .entry(1)
            .or_insert_with(HashMap::new)
            .insert(leader, leader_block.clone());
        // Mark leader[1] = leader.
        node.leader.insert(1, leader);
        // Inject a Done from peer 2 for round 1, leader's block.
        let done = WahooDone {
            done_sender: publics[2],
            block_sender: leader,
            done: Vec::new(),
            hash: Digest::default(),
            round: 1,
        };
        node.done
            .entry(1)
            .or_insert_with(HashMap::new)
            .insert(leader, done);

        node.try_to_commit_leader(1).await;

        // Expect at least one committed block (the leader's).
        let committed = rx_committed.recv().await.expect("commit should fire");
        assert_eq!(committed.block.round, 1);
        assert_eq!(committed.block.author, leader);
        // Chain advanced.
        assert_eq!(node.chain.round, 1);
    }

    /// Phase C Step 4a: `handle_recp` buffers shares per (round,
    /// block_hash, author), silently dedupes duplicates, and drops
    /// shares from non-committee authors. `recp_shares_for` returns the
    /// accumulated view for downstream leader-link construction.
    #[tokio::test]
    async fn recp_pool_accumulates_and_dedups() {
        let (publics, secrets, committee) = make_test_committee(4);
        let me = publics[0];
        let mut secrets = secrets;
        let secret = secrets.remove(0);
        let sig_service = SignatureService::new(secret);
        let (_tx_msg, rx_msg) = tokio::sync::mpsc::channel(64);
        let (_tx_workers, rx_workers) = tokio::sync::mpsc::channel(64);
        let (_tx_recp, rx_recp) = tokio::sync::mpsc::channel(64);
        let (tx_committed, _rx_committed) = tokio::sync::mpsc::channel(64);
        let mut node = Node::new(
            me,
            committee,
            sig_service,
            1,
            rx_msg,
            rx_workers,
            rx_recp,
            tx_committed,
        );

        let block_hash = Digest([7u8; 32]);
        let round: Round = 3;

        // Three distinct committee authors submit shares for the same block.
        for author in &publics[1..4] {
            node.handle_recp(RecpMessage {
                block_hash: block_hash.clone(),
                round,
                author: *author,
                share: vec![1, 2, 3],
            });
        }
        let shares = node
            .recp_shares_for(round, &block_hash)
            .expect("pool populated");
        assert_eq!(shares.len(), 3);

        // Duplicate (same author, same block, same round) is silently ignored.
        node.handle_recp(RecpMessage {
            block_hash: block_hash.clone(),
            round,
            author: publics[1],
            share: vec![9, 9],
        });
        assert_eq!(
            node.recp_shares_for(round, &block_hash).unwrap().len(),
            3,
            "dedup must leave count unchanged"
        );

        // Unknown authority (not in committee) is rejected.
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let (unknown_pk, _) = crypto::generate_keypair(&mut rng);
        node.handle_recp(RecpMessage {
            block_hash: block_hash.clone(),
            round,
            author: unknown_pk,
            share: vec![0],
        });
        assert_eq!(
            node.recp_shares_for(round, &block_hash).unwrap().len(),
            3,
            "unknown-authority RECP must not enter the pool"
        );

        // Different block hash at same round lives in its own bucket.
        let other_hash = Digest([8u8; 32]);
        node.handle_recp(RecpMessage {
            block_hash: other_hash.clone(),
            round,
            author: publics[1],
            share: vec![4],
        });
        assert_eq!(
            node.recp_shares_for(round, &other_hash).unwrap().len(),
            1
        );
        assert_eq!(
            node.recp_shares_for(round, &block_hash).unwrap().len(),
            3,
            "per-block-hash isolation"
        );
    }

    /// Phase C Step 4b: `build_leader_link` covers the three paper cases.
    /// Test fixture is n=4, f=1, so f+1=2 and n-f=3.
    #[tokio::test]
    async fn build_leader_link_paper_cases() {
        let (publics, secrets, committee) = make_test_committee(4);
        let me = publics[0];
        let mut secrets = secrets;
        let secret = secrets.remove(0);
        let sig_service = SignatureService::new(secret);
        let (_tx_msg, rx_msg) = tokio::sync::mpsc::channel(64);
        let (_tx_workers, rx_workers) = tokio::sync::mpsc::channel(64);
        let (_tx_recp, rx_recp) = tokio::sync::mpsc::channel(64);
        let (tx_committed, _rx_committed) = tokio::sync::mpsc::channel(64);
        let mut node = Node::new(
            me,
            committee,
            sig_service,
            1,
            rx_msg,
            rx_workers,
            rx_recp,
            tx_committed,
        );

        // Case A: round < 3 -> always None (first EPBC has no predecessor).
        assert!(node.build_leader_link(1).is_none());
        assert!(node.build_leader_link(2).is_none(), "even round -> None");

        // Case B: empty pool at prev round -> None.
        assert!(node.build_leader_link(3).is_none());

        // Case C: ExclusiveCommit. Put f+1 = 2 RECPs on the same block
        // at round 1, building a leader_link for round 3. Step 4d:
        // shares are real BLS partial sigs over (round=1, leader_hash)
        // so `combine_recp_shares` accepts them and produces a valid
        // aggregate (96-byte BLS signature) inside `ExclusiveCommit`.
        let leader_hash = Digest([0xAAu8; 32]);
        for author in &node.authorities_sorted.clone()[0..2] {
            let share = crypto::make_recp_share(
                &node.authorities_sorted,
                node.recp_threshold,
                author,
                1,
                &leader_hash,
            )
            .expect("test authority is in committee");
            node.handle_recp(RecpMessage {
                block_hash: leader_hash.clone(),
                round: 1,
                author: *author,
                share,
            });
        }
        let link = node
            .build_leader_link(3)
            .expect("ExclusiveCommit threshold met");
        assert_eq!(link.hash, Some(leader_hash.clone()));
        assert!(matches!(link.proof, LeaderProof::ExclusiveCommit(ref b) if !b.is_empty()));
        // Structural verification must accept this proof.
        link.verify_structure(&node.committee)
            .expect("ExclusiveCommit verifies");

        // Case D: NoCommit. Wipe round 1 and re-populate with 3 RECPs on
        // 3 DIFFERENT blocks (no single block has f+1). This crosses the
        // n-f = 3 distinct-author threshold but not the f+1 same-block one.
        node.recp_pool.remove(&1);
        let other_hashes = [Digest([1u8; 32]), Digest([2u8; 32]), Digest([3u8; 32])];
        for i in 0..3 {
            node.handle_recp(RecpMessage {
                block_hash: other_hashes[i].clone(),
                round: 1,
                author: publics[i],
                share: vec![i as u8],
            });
        }
        let link = node.build_leader_link(3).expect("NoCommit threshold met");
        assert_eq!(link.hash, None);
        match &link.proof {
            LeaderProof::NoCommit(recps) => {
                assert_eq!(recps.len(), 3, "one RECP per distinct author");
                // Authors should be sorted for deterministic encoding.
                let authors: Vec<PublicKey> = recps.iter().map(|r| r.author).collect();
                let mut sorted = authors.clone();
                sorted.sort();
                assert_eq!(authors, sorted);
            }
            _ => panic!("expected NoCommit proof"),
        }
        link.verify_structure(&node.committee)
            .expect("NoCommit verifies");

        // Case E: insufficient evidence. Only 2 distinct authors total,
        // and no block has f+1 -> None.
        node.recp_pool.remove(&1);
        node.handle_recp(RecpMessage {
            block_hash: other_hashes[0].clone(),
            round: 1,
            author: publics[0],
            share: vec![0],
        });
        node.handle_recp(RecpMessage {
            block_hash: other_hashes[1].clone(),
            round: 1,
            author: publics[1],
            share: vec![1],
        });
        assert!(
            node.build_leader_link(3).is_none(),
            "2 authors < n-f=3 and no block has f+1"
        );
    }
}
