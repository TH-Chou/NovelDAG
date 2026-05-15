// Copyright(C) Facebook, Inc. and its affiliates.
// Unified consensus router: dispatches to narwhal, bullshark, or noveldag based on dag_protocol.
use config::{Committee, ConsensusProtocol, DagProtocol, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

mod noveldag;
mod wahoo;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
pub(crate) struct State {
    /// The last committed round.
    last_committed_round: Round,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    pub(crate) committee: Committee,
    /// The depth of the garbage collector.
    pub(crate) gc_depth: Round,
    /// Which DAG protocol variant is running.
    dag_protocol: DagProtocol,
    /// The consensus leader election mode.
    pub(crate) consensus_protocol: ConsensusProtocol,
    /// The public key of this authority, used by NovelDAG for round-completion detection.
    pub(crate) name: PublicKey,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    pub(crate) rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    pub(crate) tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    pub(crate) tx_output: Sender<Certificate>,

    /// The genesis certificates.
    pub(crate) genesis: Vec<Certificate>,
    


}

impl Consensus {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        Self::spawn_with_protocol(
            name,
            committee,
            gc_depth,
            DagProtocol::NovelDAG,
            ConsensusProtocol::RoundRobin,
            rx_primary,
            tx_primary,
            tx_output,
        );
    }

    pub fn spawn_with_protocol(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        dag_protocol: DagProtocol,
        consensus_protocol: ConsensusProtocol,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                dag_protocol,
                consensus_protocol,
                name,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
<<<<<<< HEAD
        debug_assert!(ROUNDS_PER_WAVE >= 2 && ROUNDS_PER_WAVE % 2 == 0);

        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        #[cfg(feature = "benchmark")]
        let mut diag_seen_certificates = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_commit_round_checks = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_not_wave_boundary = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_round_no_quorum = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_leader_already_committed = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_leader_unavailable = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_missing_b2 = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_missing_b1 = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_skip_qc_chain_invalid = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_commits_emitted = 0u64;

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            #[cfg(feature = "benchmark")]
            {
                diag_seen_certificates += 1;
            }

            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // Drop certificates whose origin's round is already committed.
            if state
                .last_committed
                .get(&certificate.origin())
                .map_or(false, |r| *r >= round)
            {
                continue;
            }

            // Add the new certificate to the local storage.
            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            // Pre-compute order_dag for the upcoming wave if we're one round before the boundary.
            // This shifts the DFS traversal work ahead of the commit decision point.
            let upcoming = round + 1;
            if upcoming >= ROUNDS_PER_WAVE && upcoming % ROUNDS_PER_WAVE == 0 {
                let pre_leader_round = upcoming - (ROUNDS_PER_WAVE - 1);
                if pre_leader_round > state.last_committed_round {
                    if let Some(ordered) = self.precompute_order(pre_leader_round, upcoming, &state) {
                        self.precomputed.insert(pre_leader_round, ordered);
                    }
                }
            }

            // Try to order the dag to commit using the section-6 rule from DAG构建(1).md:
            // - trigger only when round r ends and r is a multiple of 4;
            // - elect leader at round r-3;
            // - require same-author chain b3 (r-3), b2 (r-2), b1 (r-1);
            // - require embedded QC links b2->b3 and b1->b2.
            let commit_round = round;

            #[cfg(feature = "benchmark")]
            {
                diag_commit_round_checks += 1;
            }

            if commit_round < ROUNDS_PER_WAVE || commit_round % ROUNDS_PER_WAVE != 0 {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_not_wave_boundary += 1;
                }
                continue;
            }

            // We only consider a round ended for commit purposes once we have a quorum for that round.
            // 延迟构成-阶段2：如果当前轮 stake 未达 quorum，会在这里直接等待，提交无法推进。
            if !self.round_has_quorum(commit_round, &state.dag) {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_round_no_quorum += 1;
                }
                continue;
            }

            // Get the certificate of the wave leader. If we already ordered this leader,
            // there is nothing to do.
            let leader_round = commit_round - (ROUNDS_PER_WAVE - 1);
            if leader_round <= state.last_committed_round {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_leader_already_committed += 1;
                }
                continue;
            }

            let (_, leader) = match self.leader(leader_round, commit_round, &state.dag) {
                Some(x) => x,
                None => {
                    #[cfg(feature = "benchmark")]
                    {
                        diag_skip_leader_unavailable += 1;
                    }
                    continue;
                }
            };

            let b3 = leader.clone();
            #[cfg(feature = "benchmark")]
            info!(
                "DIAG_COMMIT_CANDIDATE commit_round={} leader_round={} leader_author={}",
                commit_round,
                b3.round(),
                b3.origin()
            );
            // 延迟构成-阶段3：同作者链 b3->b2->b1 不完整时，commit 会持续被跳过。
            let Some(b2) = self.certificate_by_author(leader_round + 1, b3.origin(), &state.dag) else {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_missing_b2 += 1;
                }
                continue;
            };
            let Some(b1) = self.certificate_by_author(leader_round + 2, b3.origin(), &state.dag) else {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_missing_b1 += 1;
                }
                continue;
            };

            if !self.embedded_qc_links(b2, &b3, commit_round)
                || !self.embedded_qc_links(b1, b2, commit_round)
            {
                // 延迟构成-阶段4：embedded QC 链不满足时，当前波次不会产生命中提交。
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_qc_chain_invalid += 1;
                }
                debug!("Leader {:?} does not satisfy b3->b2->b1 QC chain", b3);
                continue;
            }

            debug!("Leader {:?} satisfies section-6 commit rule", b3);
            #[cfg(feature = "benchmark")]
            info!(
                "DIAG_COMMIT_CHAIN_OK commit_round={} leader_round={} expected_commit_gap={}",
                commit_round,
                b3.round(),
                commit_round.saturating_sub(b3.round())
            );
            // Use pre-computed order if available, otherwise compute on demand.
            let sequence = if let Some(cached) = self.precomputed.remove(&leader_round) {
                cached
            } else {
                self.order_dag(&b3, &state)
            };

            for x in &sequence {
                state.update(x, self.gc_depth);
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            #[cfg(feature = "benchmark")]
            let leader_id = b3.header.id.clone();
            #[cfg(feature = "benchmark")]
            let leader_round = b3.round();
            for certificate in sequence {
                #[cfg(feature = "benchmark")]
                if certificate.header.id == leader_id {
                    info!(
                        "DIAG_LEADER_COMMIT committed_leader_round={} commit_round={} commit_gap={} leader_author={}",
                        leader_round,
                        commit_round,
                        commit_round.saturating_sub(leader_round),
                        certificate.origin()
                    );
                }

                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }

                #[cfg(feature = "benchmark")]
                {
                    diag_commits_emitted += 1;
                }
            }

            #[cfg(feature = "benchmark")]
            if commit_round % 20 == 0 {
                info!(
                    "DIAG_CONSENSUS_COMMIT round={} seen_certificates={} commit_checks={} commits_emitted={} skip_not_wave_boundary={} skip_round_no_quorum={} skip_leader_already_committed={} skip_leader_unavailable={} skip_missing_b2={} skip_missing_b1={} skip_qc_chain_invalid={}",
                    commit_round,
                    diag_seen_certificates,
                    diag_commit_round_checks,
                    diag_commits_emitted,
                    diag_skip_not_wave_boundary,
                    diag_skip_round_no_quorum,
                    diag_skip_leader_already_committed,
                    diag_skip_leader_unavailable,
                    diag_skip_missing_b2,
                    diag_skip_missing_b1,
                    diag_skip_qc_chain_invalid,
                );
            }
=======
        match self.dag_protocol {
            DagProtocol::Narwhal => narwhal::run(self).await,
            DagProtocol::Bullshark => bullshark::run(self).await,
            DagProtocol::NovelDAG => noveldag::run(self).await,
            DagProtocol::Wahoo => wahoo::run(self).await,
>>>>>>> unify-three-protocols
        }
    }

    // ---------------- Shared helpers (used by multiple protocol modules) ----------------

<<<<<<< HEAD
        // We elect the leader of round r-2 using either:
        // - round-robin (deterministic fallback), or
        // - a reproducible common-coin value derived from round-r certificates.
        let leader = match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => {
                let coin = self.round_robin_coin(round);
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
            ConsensusProtocol::CommonCoin => {
                let coin = self
                    .common_coin(coin_round, dag)
                    .unwrap_or_else(|| self.round_robin_coin(round));
                // Use the full committee for leader selection so that an adversary
                // cannot bias the outcome by suppressing certificates from specific
                // authorities at the coin round.
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
        };

        // Return its certificate and the certificate's digest.
        by_round.get(&leader)
    }

    fn round_robin_coin(&self, round: Round) -> Round {
=======
    pub(crate) fn round_robin_coin(&self, round: Round) -> Round {
>>>>>>> unify-three-protocols
        #[cfg(test)]
        {
            let _ = round;
            0
        }
        #[cfg(not(test))]
        {
            round
        }
    }

    pub(crate) fn common_coin(&self, round: Round, dag: &Dag) -> Option<Round> {
        let certificates = dag.get(&round)?;
        let weight: Stake = certificates
            .values()
            .map(|(_, certificate)| self.committee.stake(&certificate.origin()))
            .sum();
        if weight < self.committee.quorum_threshold() {
            return None;
        }

        let mut digests: Vec<_> = certificates
            .values()
            .map(|(digest, _)| digest.clone())
            .collect();
        digests.sort();

        let mut seed = round;
        for digest in digests {
            let mut chunk = [0u8; 8];
            chunk.copy_from_slice(&digest.0[..8]);
            seed ^= u64::from_le_bytes(chunk);
            seed = seed.rotate_left(13).wrapping_mul(0x9E37_79B1_85EB_CA87);
        }
        Some(seed)
    }

    /// NovelDAG threshold coin: combines BLS coin_shares embedded in headers at
    /// `round`. Requires f+1 valid shares, which is guaranteed by the 2f+1
    /// quorum at `round`. Returns a deterministic u64 that all honest nodes
    /// will reproduce, regardless of which specific 2f+1 certificates they hold.
    pub(crate) fn threshold_coin(&self, round: Round, dag: &Dag) -> Option<Round> {
        let certificates = dag.get(&round)?;
        let weight: Stake = certificates
            .values()
            .map(|(_, c)| self.committee.stake(&c.origin()))
            .sum();
        if weight < self.committee.quorum_threshold() {
            return None;
        }
        let authorities: Vec<PublicKey> = self.committee.authorities.keys().cloned().collect();
        let threshold = crypto::coin_threshold(self.committee.size());
        let shares: Vec<(PublicKey, Vec<u8>)> = certificates
            .values()
            .filter_map(|(_, cert)| {
                if cert.header.coin_share.is_empty() {
                    None
                } else {
                    Some((cert.origin(), cert.header.coin_share.clone()))
                }
            })
            .collect();
        crypto::recover_coin(&authorities, threshold, round, &shares).map(|coin| coin as Round)
    }

    /// Returns the certificate (and digest) originated by the leader.
    pub(crate) fn leader<'a>(
        &self,
        round: Round,
        coin_round: Round,
        dag: &'a Dag,
    ) -> Option<&'a (Digest, Certificate)> {
        let by_round = dag.get(&round)?;

        let leader = match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => {
                let coin = self.round_robin_coin(round);
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
            ConsensusProtocol::CommonCoin => {
                let coin = if self.dag_protocol == DagProtocol::NovelDAG {
                    self.threshold_coin(coin_round, dag)
                        .unwrap_or_else(|| self.round_robin_coin(round))
                } else {
                    self
                        .common_coin(coin_round, dag)
                        .unwrap_or_else(|| self.round_robin_coin(round))
                };
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
        };

        by_round.get(&leader)
    }

    pub(crate) fn round_has_quorum(&self, round: Round, dag: &Dag) -> bool {
        let Some(certificates) = dag.get(&round) else {
            return false;
        };
        let weight: Stake = certificates
            .values()
            .map(|(_, certificate)| self.committee.stake(&certificate.origin()))
            .sum();
        weight >= self.committee.quorum_threshold()
    }

    pub(crate) fn certificate_by_author<'a>(
        &self,
        round: Round,
        author: PublicKey,
        dag: &'a Dag,
    ) -> Option<&'a Certificate> {
        dag.get(&round)
            .and_then(|by_authority| by_authority.get(&author))
            .map(|(_, certificate)| certificate)
    }

    pub(crate) fn embedded_qc_links(
        &self,
        child: &Certificate,
        parent: &Certificate,
        commit_round: Round,
    ) -> bool {
        let Some(qc) = child.header.qc.as_ref() else {
            return false;
        };
        let structural_ok = qc.target == parent.header.id
            && qc.round == parent.round()
            && qc.round < commit_round
<<<<<<< HEAD
            && qc.votes.iter().all(|vote| vote.voter_round < commit_round)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or(false, |r| *r >= certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }

            // Also traverse second-hop parents (parents_2) to ensure causal completeness
            // when a Byzantine leader omits some r-1 parents from parents_1.
            if x.round() >= 2 {
                for parent in &x.header.parents_2 {
                    let (digest, certificate) = match state
                        .dag
                        .get(&(x.round() - 2))
                        .map(|x| x.values().find(|(x, _)| x == parent))
                        .flatten()
                    {
                        Some(x) => x,
                        None => continue,
                    };

                    let mut skip = already_ordered.contains(&digest);
                    skip |= state
                        .last_committed
                        .get(&certificate.origin())
                        .map_or(false, |r| *r >= certificate.round());
                    if !skip {
                        buffer.push(certificate);
                        already_ordered.insert(digest);
                    }
                }
            }
=======
            && qc.votes.iter().all(|vote| vote.voter_round < commit_round);
        if structural_ok {
            // 防御深度：QC 投票权重应在 Primary 层已验证 ≥ 2f+1。
            // debug_assert! 在 release 构建中被编译器移除，零运行时开销。
            // 空投票的 QC 来自合成 peer 证书（maybe_synthesize_peer_cert）
            // 或测试夹具，此时跳过权重检查。
            debug_assert!(
                qc.votes.is_empty() || qc.votes.iter().map(|v| self.committee.stake(&v.author)).sum::<Stake>()
                    >= self.committee.quorum_threshold(),
                "embedded QC lacks quorum weight"
            );
>>>>>>> unify-three-protocols
        }
        structural_ok
    }
}