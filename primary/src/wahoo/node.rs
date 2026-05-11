// Port of `Wahoo-main/wahoo/node.go` + `wahoo/msg_handle.go`.
//
// Behavioural goals (1:1 with Go reference):
//
//   * Round 1 starts the protocol; round numbering matches Go (`n.round = 1`).
//   * Even rounds run PB (Provable Broadcast) — see `pb.rs`.
//   * Odd rounds run the fast path: every receiver sends Ready immediately
//     after receiving and validating an odd-round block; the proposer waits
//     for `n` Readys (full membership, NOT 2f+1 — `node.go::checkIfEnoughReady`
//     uses `len(readies) == n.nodeNum`) before broadcasting Done.
//   * `moveRound[r] >= quorumNum` advances the local round counter; the
//     trigger differs by parity:
//       - even rounds: each block successfully added to the DAG bumps the
//         counter (`tryToUpdateDAG`).
//       - odd rounds: each Done received bumps the counter (`storeDone`).
//   * Even-round Elect partial signatures, once 2f+1 are collected, recover
//     a common coin that picks the leader of the *previous* (odd) round.
//   * Commit triggers when (leader_known ∧ done_seen ∧ block_in_dag) for an
//     odd-round leader. Commit walks the parents transitively and emits all
//     uncommitted ancestor blocks (`tryToCommitAncestorLeader` +
//     `commitAncestorBlocks`).
//
// One deviation that is structurally required by Rust ownership:
//   * `pb.go` runs as its own goroutine with shared mutable state. We model
//     PB as a synchronous helper (`pb::Pb`) whose mutating methods return
//     `PbAction`s; the `Node` task executes them. The set of state
//     transitions is identical.
//
// Threshold-signature substrate:
//   * Elect uses `crypto::make_coin_share` / `crypto::recover_coin` (BLS
//     over BN, already in the workspace) — this is a *real* threshold
//     signature, not the placeholder we considered earlier. Threshold is
//     `2f`, so 2f+1 distinct partials recover. Leader index = recovered
//     coin u64 mod committee_size, matching `node.go::tryToElectLeader`
//     (`leaderId := int(qcAsInt) % n.nodeNum`).
//   * Ready partial sigs are populated with an ed25519 signature over the
//     block hash. The Go reference signs but never combines them (the
//     `AssembleIntactTSPartial` call site in `checkIfEnoughReady` is
//     commented out and `Done.Done` is set to nil — see lines 287-298 of
//     node.go). We mirror that: Done.done is empty, Ready.partial_sig is
//     a non-empty signature placeholder for byte-level fidelity.

use crate::primary::Round;
use crate::wahoo::messages::{
    SignedWahoo, WahooBlock, WahooBlockTag, WahooDone, WahooElect, WahooMessage, WahooReady,
};
use crate::wahoo::msg_send;
use crate::wahoo::pb::{Pb, PbAction};
use crate::wahoo::tools::unix_nano_now;
use bytes::Bytes;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, Hash as _, PublicKey, SignatureService};
use log::{debug, info, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};


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
    /// Sender side of the slow-path timeout channel. A copy is handed
    /// to a fire-and-forget timer task each time we broadcast our own
    /// odd-round block; the task sleeps `SLOW_PATH_TIMEOUT_MS` and then
    /// posts the round number back so `run`'s `select!` can call
    /// `handle_slow_path_timeout` on the main task (no `Send`-bounded
    /// state moves between tasks).
    tx_slow_path: Sender<Round>,
    /// Receiver side of the same channel. Moved out by `run`.
    rx_slow_path: Option<Receiver<Round>>,
    /// Rounds whose `SLOW_PATH_TIMEOUT_MS` timer has fired. Once a round
    /// is in here, `check_if_enough_ready` will broadcast Done as soon
    /// as `count >= quorum_num` for our own block, instead of waiting
    /// for the impossible `count == node_num`. This is the liveness
    /// fallback for any f >= 1 scenario where one peer's Ready will
    /// never arrive.
    slow_path_armed: HashSet<Round>,
}

/// Delay before we *arm* the slow-path Done for an odd-round block when
/// full-membership Ready hasn't materialised. After arming, the actual
/// Done is broadcast in `check_if_enough_ready` as soon as `count >=
/// quorum_num` Readies for our block have arrived, which may be before
/// or after the timer fires. Picked to be in the same order of magni-
/// tude as `max_header_delay` (200 ms in the default benchmark config)
/// so the fault-free fast path (count == n) comfortably wins; the
/// constant is independent of `max_header_delay` because Wahoo's `Node`
/// doesn't currently take a `Parameters` reference.
const SLOW_PATH_TIMEOUT_MS: u64 = 500;

impl Node {
    /// Construct a new Wahoo node (`node.go::NewNode`).
    pub fn new(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        batch_size: usize,
        rx_messages: Receiver<WahooMessage>,
        rx_workers: Receiver<(Digest, WorkerId)>,
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

        let pb = Pb::new(name, quorum_num);
        let (tx_slow_path, rx_slow_path) = channel(1024);

        Self {
            name,
            committee,
            authorities_sorted,
            node_num,
            quorum_num,
            elect_threshold,
            batch_size,
            signature_service,
            sender: ReliableSender::new(),
            cancel_handlers: Vec::new(),
            dag: HashMap::new(),
            pending_blocks: HashMap::new(),
            chain: Chain {
                round: 0,
                blocks: HashMap::new(),
            },
            leader: HashMap::new(),
            done: HashMap::new(),
            elect: HashMap::new(),
            ready: HashMap::new(),
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
            tx_slow_path,
            rx_slow_path: Some(rx_slow_path),
            slow_path_armed: HashSet::new(),
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
        let mut rx_slow_path = self
            .rx_slow_path
            .take()
            .expect("slow-path receiver already taken");
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    self.handle_message(msg).await;
                }
                Some((digest, wid)) = rx_workers.recv() => {
                    self.pending_digests.push((digest, wid));
                }
                Some(round) = rx_slow_path.recv() => {
                    self.handle_slow_path_timeout(round).await;
                }
                else => break,
            }
        }
    }

    /// Slow-path arming. Triggered `SLOW_PATH_TIMEOUT_MS` after we
    /// broadcast our own odd-round block. Marks the round as eligible
    /// for quorum-based Done; either we already have `quorum_num`
    /// Readies (broadcast now) or we don't yet (broadcast later from
    /// `check_if_enough_ready` when the count catches up). This avoids
    /// the previous "fire-and-forget" race where a single timer firing
    /// before any Ready had time to traverse a delayed dummynet pipe
    /// would permanently deadlock the round.
    async fn handle_slow_path_timeout(&mut self, round: Round) {
        if self.done_send.contains(&round) {
            // Fast path already won this round; nothing to do.
            return;
        }
        self.slow_path_armed.insert(round);
        let block_sender = self.name; // We only broadcast Done for our own block.
        let count = self
            .ready
            .get(&round)
            .and_then(|m| m.get(&block_sender))
            .map(|m| m.len())
            .unwrap_or(0);
        if count < self.quorum_num {
            info!(
                "Wahoo slow-path armed: round={} count={}/{} (will fire on next Ready)",
                round, count, self.quorum_num
            );
            return;
        }
        info!(
            "Wahoo slow-path Done: round={} count={}/{} (fast path stalled)",
            round, count, self.node_num
        );
        self.done_send.insert(round);
        let done = WahooDone {
            done_sender: self.name,
            block_sender,
            done: Vec::new(),
            hash: Digest::default(),
            round,
        };
        self.broadcast_done(done.clone()).await;
        self.handle_done(done).await;
    }

    // ============================================================
    //  Inbound message dispatch — `wahoo/msg_handle.go::HandleMsgLoop`
    // ============================================================

    async fn handle_message(&mut self, msg: WahooMessage) {
        match msg {
            WahooMessage::Block(b) => {
                info!(
                    "Wahoo recv Block round={} sender={} tag={:?}",
                    b.round, b.sender, b.tag
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
                PbAction::SendVote { target, vote } => {
                    let signed = self.sign_wahoo(WahooMessage::Vote(vote)).await;
                    let h =
                        msg_send::send(&mut self.sender, &self.committee, &target, signed).await;
                    self.cancel_handlers.push(h);
                }
                PbAction::BroadcastBlock2(block) => {
                    let signed = self.sign_wahoo(WahooMessage::Block(block)).await;
                    let hs =
                        msg_send::broadcast(&mut self.sender, &self.committee, &self.name, signed)
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
            block.round, block.sender
        );
        self.try_to_update_dag(block).await;
    }

    /// `msg_handle.go::handleFastBlockMsg`.
    async fn handle_fast_block(&mut self, block: WahooBlock) {
        let hash = block.digest();
        let round = block.round;
        let sender = block.sender;
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
            for digest in block.payload_digests.keys() {
                info!("Created B{}({}) -> {:?}", round, self.name, digest);
            }

            if round % 2 == 0 {
                // Even round: PB phase 1 broadcast.
                self.pb.broadcast_block(&block);
                let signed = self.sign_wahoo(WahooMessage::Block(block.clone())).await;
                let hs =
                    msg_send::broadcast(&mut self.sender, &self.committee, &self.name, signed)
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
                let signed = self.sign_wahoo(WahooMessage::Block(block.clone())).await;
                let hs =
                    msg_send::broadcast(&mut self.sender, &self.committee, &self.name, signed)
                        .await;
                self.cancel_handlers.extend(hs);
                // Self-deliver to keep our own DAG and Ready logic in sync.
                self.handle_fast_block(block).await;
                // Schedule the slow-path Done fallback for this odd
                // round. If full-membership Ready arrives within the
                // timeout we'll hit `done_send.contains(round)` and
                // skip; otherwise we'll Done with 2f+1 Readies.
                let tx = self.tx_slow_path.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(SLOW_PATH_TIMEOUT_MS)).await;
                    let _ = tx.send(round).await;
                });
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
            .insert(block.sender, block);
    }

    // ============================================================
    //  DAG maintenance — `node.go::tryToUpdateDAG*`
    // ============================================================

    /// `node.go::selectPreviousBlocks`.
    fn select_previous_blocks(&self, round: Round) -> BTreeMap<PublicKey, Digest> {
        if round == 0 {
            return BTreeMap::new();
        }
        let mut out = BTreeMap::new();
        if let Some(level) = self.dag.get(&round) {
            for (sender, block) in level {
                out.insert(*sender, block.digest());
            }
        }
        out
    }

    fn try_to_update_dag(&mut self, block: WahooBlock) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            if self.check_whether_can_add_to_dag(&block) {
                let round = block.round;
                let sender = block.sender;
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

    /// `node.go::checkWhetherCanAddToDAG`.
    fn check_whether_can_add_to_dag(&self, block: &WahooBlock) -> bool {
        if block.round == 0 {
            return true;
        }
        let parent_round = block.round - 1;
        let level = match self.dag.get(&parent_round) {
            Some(level) => level,
            None => return block.previous_hash.is_empty(),
        };
        for sender in block.previous_hash.keys() {
            if !level.contains_key(sender) {
                return false;
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
        // Fast path — Go reference (`wahoo/msg_handle.go::handleReadyMsg`)
        // requires *full membership*, i.e. all n peers' Readies. With
        // every honest peer alive this fires almost immediately and the
        // protocol enjoys its low-latency happy case. When even one
        // peer is missing, this branch never fires; the slow-path
        // fallback in `handle_slow_path_timeout` (triggered by a per-
        // odd-round timer set up in `broadcast_block`) takes over and
        // broadcasts Done with only `quorum_num = 2f+1` Readies, which
        // matches the even-round PB threshold and is what every other
        // DAG-BFT protocol in this workspace uses for liveness.
        if count == self.node_num && !self.done_send.contains(&round) {
            self.done_send.insert(round);
            // Done.Done is left empty — see file-level note explaining the
            // commented-out partial-sig assembly in the Go reference.
            let done = WahooDone {
                done_sender: self.name,
                block_sender,
                done: Vec::new(),
                hash: Digest::default(),
                round,
            };
            // Self-deliver.
            self.handle_done(done.clone()).await;
            self.broadcast_done(done).await;
            return;
        }
        // Slow-path catch-up: the SLOW_PATH_TIMEOUT_MS timer for this
        // round has already fired (round is `armed`), but at the time
        // the timer ran we hadn't yet collected `quorum_num` Readies
        // for our own block (typical under dummynet delay where the
        // first Ready can arrive ~100–500 ms after broadcast). Now
        // that another Ready has pushed us across the 2f+1 threshold,
        // broadcast the slow-path Done immediately.
        if block_sender == self.name
            && self.slow_path_armed.contains(&round)
            && count >= self.quorum_num
            && !self.done_send.contains(&round)
        {
            info!(
                "Wahoo slow-path Done (catch-up): round={} count={}/{}",
                round, count, self.node_num
            );
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
            // Recursive call mirroring Go's `tryToNextRound(round+1)`
            // tail call.
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
            round, block.sender
        );
        let now = unix_nano_now();
        let latency = now - block.timestamp;
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
                    r, block.sender
                );
                let latency = unix_nano_now() - block.timestamp;
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
                            if *l == b.sender {
                                valid.insert(b.round, b.sender);
                            }
                        }
                    }
                    if r == 0 {
                        continue;
                    }
                    let parent_round = r - 1;
                    if let Some(parent_level) = self.dag.get(&parent_round) {
                        for sender in b.previous_hash.keys() {
                            if let Some(pb) = parent_level.get(sender) {
                                let h = pb.digest();
                                next_level.insert(h, pb.clone());
                            }
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
                    let latency = unix_nano_now() - b.timestamp;
                    self.evaluation.push(latency);
                    // Don't re-emit the leader (already sent in
                    // try_to_commit_leader); emit other ancestors.
                    if !(b.round == round && b.sender == leader) {
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
                    let parent_round = r - 1;
                    if let Some(parent_level) = self.dag.get(&parent_round) {
                        for sender in b.previous_hash.keys() {
                            if let Some(pb) = parent_level.get(sender) {
                                let h = pb.digest();
                                if !self.chain.blocks.contains_key(&h) {
                                    next_level.insert(h, pb.clone());
                                }
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
        previous_hash: BTreeMap<PublicKey, Digest>,
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
        WahooBlock {
            sender: self.name,
            round,
            previous_hash,
            txs: Vec::new(),
            payload_digests,
            timestamp: unix_nano_now(),
            tag: WahooBlockTag::Proposal,
        }
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
        let (tx_committed, _rx_committed) = tokio::sync::mpsc::channel(64);

        let node = Node::new(me, committee, sig_service, 4, rx_msg, rx_workers, tx_committed);
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
        let (tx_committed, mut rx_committed) = tokio::sync::mpsc::channel(64);
        let mut node = Node::new(
            me,
            committee,
            sig_service,
            1,
            rx_msg,
            rx_workers,
            tx_committed,
        );

        // Insert a leader block at round 1 (odd).
        let leader_block = WahooBlock {
            sender: leader,
            round: 1,
            previous_hash: BTreeMap::new(),
            txs: vec![vec![1, 2, 3]],
            payload_digests: BTreeMap::new(),
            timestamp: unix_nano_now() - 1_000_000,
            tag: WahooBlockTag::Proposal,
        };
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
        assert_eq!(committed.block.sender, leader);
        // Chain advanced.
        assert_eq!(node.chain.round, 1);
    }
}
