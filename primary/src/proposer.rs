// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, EmbeddedQc, Header};
use crate::primary::Round;
use config::{Committee, ConsensusProtocol, DagProtocol, WorkerId};
use crypto::Hash as _;
use crypto::{coin_threshold, make_coin_share, Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use log::{log_enabled, warn};
use std::collections::BTreeSet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
#[derive(Clone, Debug)]
pub struct ProposerSignal {
    pub round: Round,
    pub parents_1: Vec<Digest>,
    pub parents_2: Vec<Digest>,
    pub qc: Option<EmbeddedQc>,
    /// Full certificates for the parents_1 set (only filled by Bullshark core).
    pub certificates_1: Vec<Certificate>,
}

pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Which DAG protocol variant is running.
    dag_protocol: DagProtocol,
    /// The committee information (needed by Bullshark leader tracking).
    committee: Committee,
    /// The consensus leader election mode.
    consensus_protocol: ConsensusProtocol,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,
    /// Authorities used for threshold-coin shares (Shortfin-family only).
    coin_authorities: Vec<PublicKey>,
    /// Threshold used for threshold-coin shares (Shortfin-family only).
    coin_threshold: usize,
    /// Threshold for parents_2 (dual-hop) references. Shortfin-family protocols require
    /// the second-hop references to cover a quorum from round r-2.
    parents_2_threshold: usize,

    /// Receives construction signals from `Core`.
    rx_core: Receiver<ProposerSignal>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the first-hop parents (`r-1`) waiting to be included in the next header.
    parents_1: Vec<Digest>,
    /// Holds the second-hop parents (`r-2`) waiting to be included in the next header.
    parents_2: Vec<Digest>,
    /// Holds the QC of our previous-round block (Shortfin-family only).
    last_qc: Option<EmbeddedQc>,
    /// Holds full certificates for parents_1 (Bullshark only).
    last_parent_certs: Vec<Certificate>,
    /// Holds the certificate of the last leader (Bullshark only).
    last_leader: Option<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        dag_protocol: DagProtocol,
        consensus_protocol: ConsensusProtocol,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<ProposerSignal>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();
        let genesis_certs = Certificate::genesis(committee);
        let coin_authorities: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let coin_threshold = coin_threshold(committee.size());
        let parents_2_threshold = committee.quorum_threshold() as usize;
        let committee = committee.clone();

        tokio::spawn(async move {
            // Bullshark starts at round 0; Narwhal/Shortfin-family protocols start at round 1.
            let initial_round = if dag_protocol == DagProtocol::Bullshark {
                0
            } else {
                1
            };
            // Bullshark stores genesis certificates; Narwhal/Shortfin-family protocols store digests.
            let initial_parent_certs = if dag_protocol == DagProtocol::Bullshark {
                genesis_certs
            } else {
                Vec::new()
            };

            Self {
                name,
                dag_protocol,
                committee: committee.clone(),
                consensus_protocol,
                signature_service,
                header_size,
                max_header_delay,
                coin_authorities,
                coin_threshold,
                parents_2_threshold,
                rx_core,
                rx_workers,
                tx_core,
                round: initial_round,
                parents_1: genesis,
                parents_2: Vec::new(),
                last_qc: None,
                last_parent_certs: initial_parent_certs,
                last_leader: None,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        let coin_share = if self.dag_protocol.is_shortfin_family() {
            make_coin_share(
                &self.coin_authorities,
                self.coin_threshold,
                &self.name,
                self.round,
            )
            .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.parents_1.drain(..).collect::<BTreeSet<_>>(),
            if self.dag_protocol.is_shortfin_family() {
                self.parents_2.drain(..).collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            },
            if self.dag_protocol.is_shortfin_family() {
                self.last_qc.clone()
            } else {
                None
            },
            coin_share,
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // ---------------- Bullshark helpers ----------------

    fn round_robin_coin(&self, round: Round) -> Round {
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

    fn common_coin_from_parents(&self) -> Option<Round> {
        if self.last_parent_certs.is_empty() {
            return None;
        }

        let weight = self
            .last_parent_certs
            .iter()
            .map(|certificate| self.committee.stake(&certificate.origin()))
            .sum::<u32>();
        if weight < self.committee.quorum_threshold() {
            return None;
        }

        let mut digests: Vec<_> = self
            .last_parent_certs
            .iter()
            .map(|certificate| certificate.digest())
            .collect();
        digests.sort();

        let mut seed = self.round;
        for digest in digests {
            let mut chunk = [0u8; 8];
            chunk.copy_from_slice(&digest.0[..8]);
            seed ^= u64::from_le_bytes(chunk);
            seed = seed.rotate_left(13).wrapping_mul(0x9E37_79B1_85EB_CA87);
        }
        Some(seed)
    }

    /// Update the last leader (Bullshark even-round logic).
    fn update_leader(&mut self) -> bool {
        let leader_name = match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => self.committee.leader(self.round as usize),
            ConsensusProtocol::CommonCoin => {
                let mut keys: Vec<_> = self
                    .last_parent_certs
                    .iter()
                    .map(|certificate| certificate.origin())
                    .collect();
                keys.sort();
                keys.dedup();
                if keys.is_empty() {
                    self.last_leader = None;
                    return false;
                }

                let coin = self
                    .common_coin_from_parents()
                    .unwrap_or_else(|| self.round_robin_coin(self.round));
                keys[coin as usize % keys.len()]
            }
        };
        self.last_leader = self
            .last_parent_certs
            .iter()
            .find(|x| x.origin() == leader_name)
            .cloned();

        if let Some(leader) = self.last_leader.as_ref() {
            debug!("Got leader {} for round {}", leader.origin(), self.round);
        }

        self.last_leader.is_some()
    }

    /// Check whether we have (i) 2f+1 votes for the leader, (ii) f+1 nodes not voting
    /// for the leader, or (iii) there is no leader to vote for (Bullshark odd-round logic).
    fn enough_votes(&self) -> bool {
        let leader = match &self.last_leader {
            Some(x) => x.digest(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for certificate in &self.last_parent_certs {
            let stake = self.committee.stake(&certificate.origin());
            if certificate.header.parents.contains(&leader) {
                votes_for_leader += stake;
            } else {
                no_votes += stake;
            }
        }

        let mut enough_votes = votes_for_leader >= self.committee.quorum_threshold();
        if log_enabled!(log::Level::Debug) && enough_votes {
            if let Some(leader) = self.last_leader.as_ref() {
                debug!(
                    "Got enough support for leader {} at round {}",
                    leader.origin(),
                    self.round
                );
            }
        }
        enough_votes |= no_votes >= self.committee.validity_threshold();
        enough_votes
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        #[cfg(feature = "benchmark")]
        let mut diag_blocked_attempts = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_missing_parents_1 = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_missing_parents_2 = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_missing_qc = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_blocked_windows = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_blocked_total_ms = 0u64;
        #[cfg(feature = "benchmark")]
        let mut diag_blocked_since: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_headers_created = 0u64;

        // Per-round gate timing for critical-path analysis (Shortfin-family protocols).
        // Reset whenever signal.round advances; captures the first moment
        // each gate became satisfied within the current round.
        #[cfg(feature = "benchmark")]
        let mut diag_round_started_at: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_parents_1_ready_at: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_parents_2_ready_at: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_qc_ready_at: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_last_propose_at: Option<Instant> = None;
        #[cfg(feature = "benchmark")]
        let mut diag_last_blocked_log_at: Option<Instant> = None;
        // Aggregate gate-wait sums over a window so we can spot the
        // dominant critical-path gate without log-flooding.
        #[cfg(feature = "benchmark")]
        let mut diag_sum_parents_1_wait_ms: u64 = 0;
        #[cfg(feature = "benchmark")]
        let mut diag_sum_parents_2_wait_ms: u64 = 0;
        #[cfg(feature = "benchmark")]
        let mut diag_sum_qc_wait_ms: u64 = 0;
        #[cfg(feature = "benchmark")]
        let mut diag_sum_round_period_ms: u64 = 0;
        #[cfg(feature = "benchmark")]
        let mut diag_round_period_samples: u64 = 0;

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        // Bullshark advance flag.
        let mut advance = true;

        loop {
            match self.dag_protocol {
                DagProtocol::Shortfin | DagProtocol::Sailfin => {
                    // Check if we can propose a new header. We propose a new header when one of
                    // the following conditions is met:
                    // 1. We have a quorum of certificates from the previous round and enough
                    //    batches' digests;
                    // 2. We have a quorum of certificates from the previous round and the
                    //    specified maximum inter-header delay has passed.
                    let enough_parents_1 = !self.parents_1.is_empty();
                    let enough_parents_2 =
                        self.round < 2 || self.parents_2.len() >= self.parents_2_threshold;
                    let enough_qc = self.round < 2 || self.last_qc.is_some();
                    let enough_digests = self.payload_size >= self.header_size;
                    let timer_expired = timer.is_elapsed();
                    let ready_to_propose = timer_expired || enough_digests;

                    #[cfg(feature = "benchmark")]
                    if ready_to_propose && !(enough_parents_1 && enough_parents_2 && enough_qc) {
                        diag_blocked_attempts += 1;
                        if !enough_parents_1 {
                            diag_missing_parents_1 += 1;
                        }
                        if !enough_parents_2 {
                            diag_missing_parents_2 += 1;
                        }
                        if !enough_qc {
                            diag_missing_qc += 1;
                        }
                        if diag_blocked_since.is_none() {
                            diag_blocked_windows += 1;
                            diag_blocked_since = Some(Instant::now());
                        }
                        let now = Instant::now();
                        let should_log = diag_last_blocked_log_at
                            .map(|last| now.saturating_duration_since(last).as_secs() >= 5)
                            .unwrap_or(true);
                        if should_log {
                            diag_last_blocked_log_at = Some(now);
                            info!(
                                "DIAG_PROPOSER_BLOCKED round={} parents_1={} parents_2={}/{} qc={} payload_size={} timer_expired={} missing_parents_1={} missing_parents_2={} missing_qc={} blocked_total_ms={}",
                                self.round,
                                self.parents_1.len(),
                                self.parents_2.len(),
                                self.parents_2_threshold,
                                self.last_qc.is_some(),
                                self.payload_size,
                                timer_expired,
                                diag_missing_parents_1,
                                diag_missing_parents_2,
                                diag_missing_qc,
                                diag_blocked_since
                                    .map(|started| now.saturating_duration_since(started).as_millis() as u64)
                                    .unwrap_or(0),
                            );
                        }
                    }

                    if ready_to_propose && enough_parents_1 && enough_parents_2 && enough_qc {
                        #[cfg(feature = "benchmark")]
                        if let Some(started_at) = diag_blocked_since.take() {
                            diag_blocked_total_ms += started_at.elapsed().as_millis() as u64;
                        }

                        // Critical-path measurement: how long each gate took
                        // from this round's start until it became satisfied.
                        #[cfg(feature = "benchmark")]
                        {
                            let now = Instant::now();
                            if let Some(round_start) = diag_round_started_at {
                                let p1_wait = diag_parents_1_ready_at
                                    .map(|t| {
                                        t.saturating_duration_since(round_start).as_millis() as u64
                                    })
                                    .unwrap_or(0);
                                let p2_wait = diag_parents_2_ready_at
                                    .map(|t| {
                                        t.saturating_duration_since(round_start).as_millis() as u64
                                    })
                                    .unwrap_or(0);
                                let qc_wait = diag_qc_ready_at
                                    .map(|t| {
                                        t.saturating_duration_since(round_start).as_millis() as u64
                                    })
                                    .unwrap_or(0);
                                diag_sum_parents_1_wait_ms += p1_wait;
                                diag_sum_parents_2_wait_ms += p2_wait;
                                diag_sum_qc_wait_ms += qc_wait;
                                if let Some(last) = diag_last_propose_at {
                                    diag_sum_round_period_ms +=
                                        now.saturating_duration_since(last).as_millis() as u64;
                                    diag_round_period_samples += 1;
                                }
                            }
                            diag_last_propose_at = Some(now);
                        }

                        // Make a new header.
                        self.make_header().await;
                        self.payload_size = 0;

                        #[cfg(feature = "benchmark")]
                        {
                            diag_headers_created += 1;
                            // Per-round detailed timing.
                            let round_start_ms = diag_round_started_at
                                .map(|t| t.elapsed().as_millis() as u64)
                                .unwrap_or(0);
                            let p1_ready_ms = diag_parents_1_ready_at
                                .map(|t| t.elapsed().as_millis() as u64)
                                .unwrap_or(0);
                            let p2_ready_ms = diag_parents_2_ready_at
                                .map(|t| t.elapsed().as_millis() as u64)
                                .unwrap_or(0);
                            let qc_ready_ms = diag_qc_ready_at
                                .map(|t| t.elapsed().as_millis() as u64)
                                .unwrap_or(0);
                            let blocked_window_ms = diag_blocked_total_ms;
                            info!(
                                "DIAG_PROPOSER_PER_ROUND round={} header={} round_age_ms={} p1_age_ms={} p2_age_ms={} qc_age_ms={} blocked_total_ms={} blocked_windows={} period_ms={}",
                                self.round,
                                diag_headers_created,
                                round_start_ms,
                                p1_ready_ms,
                                p2_ready_ms,
                                qc_ready_ms,
                                blocked_window_ms,
                                diag_blocked_windows,
                                diag_last_propose_at
                                    .map(|last| Instant::now().saturating_duration_since(last).as_millis() as u64)
                                    .unwrap_or(0),
                            );
                            // Aggregate summary every 20 headers.
                            if diag_headers_created % 20 == 0 {
                                let n = diag_headers_created.max(1);
                                let periods = diag_round_period_samples.max(1);
                                info!(
                                    "DIAG_PROPOSER_GATE round={} headers={} blocked_attempts={} blocked_windows={} missing_parents_1={} missing_parents_2={} missing_qc={} blocked_total_ms={} avg_p1_wait_ms={} avg_p2_wait_ms={} avg_qc_wait_ms={} avg_round_period_ms={}",
                                    self.round,
                                    diag_headers_created,
                                    diag_blocked_attempts,
                                    diag_blocked_windows,
                                    diag_missing_parents_1,
                                    diag_missing_parents_2,
                                    diag_missing_qc,
                                    diag_blocked_total_ms,
                                    diag_sum_parents_1_wait_ms / n,
                                    diag_sum_parents_2_wait_ms / n,
                                    diag_sum_qc_wait_ms / n,
                                    diag_sum_round_period_ms / periods,
                                );
                            }
                        }

                        // Reschedule the timer.
                        let deadline =
                            Instant::now() + Duration::from_millis(self.max_header_delay);
                        timer.as_mut().reset(deadline);
                    }
                }

                DagProtocol::Narwhal => {
                    let enough_parents = !self.parents_1.is_empty();
                    let enough_digests = self.payload_size >= self.header_size;
                    let timer_expired = timer.is_elapsed();
                    if (timer_expired || enough_digests) && enough_parents {
                        self.make_header().await;
                        self.payload_size = 0;

                        let deadline =
                            Instant::now() + Duration::from_millis(self.max_header_delay);
                        timer.as_mut().reset(deadline);
                    }
                }

                DagProtocol::Bullshark | DagProtocol::Wahoo => {
                    let enough_parents = !self.last_parent_certs.is_empty();
                    let enough_digests = self.payload_size >= self.header_size;
                    let timer_expired = timer.is_elapsed();

                    if (timer_expired || (enough_digests && advance)) && enough_parents {
                        if timer_expired {
                            warn!("Timer expired for round {}", self.round);
                        }

                        // Advance to the next round.
                        self.round += 1;
                        debug!("Dag moved to round {}", self.round);

                        // Build header from stored parent digests.
                        self.parents_1 = self
                            .last_parent_certs
                            .drain(..)
                            .map(|x| x.digest())
                            .collect();
                        self.make_header().await;
                        self.payload_size = 0;

                        let deadline =
                            Instant::now() + Duration::from_millis(self.max_header_delay);
                        timer.as_mut().reset(deadline);
                    }
                }
            }

            tokio::select! {
                Some(signal) = self.rx_core.recv() => {
                    match self.dag_protocol {
                        DagProtocol::Shortfin | DagProtocol::Sailfin => {
                            if signal.round < self.round {
                                continue;
                            }

                            // Round advanced: reset per-round timing.
                            #[cfg(feature = "benchmark")]
                            if signal.round > self.round {
                                let now = Instant::now();
                                diag_round_started_at = Some(now);
                                diag_parents_1_ready_at = None;
                                diag_parents_2_ready_at = None;
                                diag_qc_ready_at = None;
                            }

                            self.round = signal.round;
                            // Only overwrite parents if the signal carries them (non-empty).
                            // A QC-only follow-up has empty parents and only updates last_qc.
                            if !signal.parents_1.is_empty() {
                                self.parents_1 = signal.parents_1;
                                #[cfg(feature = "benchmark")]
                                {
                                    if diag_parents_1_ready_at.is_none() {
                                        diag_parents_1_ready_at = Some(Instant::now());
                                    }
                                }
                            }
                            if !signal.parents_2.is_empty() {
                                self.parents_2 = signal.parents_2;
                                #[cfg(feature = "benchmark")]
                                {
                                    if diag_parents_2_ready_at.is_none() {
                                        diag_parents_2_ready_at = Some(Instant::now());
                                    }
                                }
                            }
                            if signal.qc.is_some() {
                                self.last_qc = signal.qc;
                                #[cfg(feature = "benchmark")]
                                {
                                    if diag_qc_ready_at.is_none() {
                                        diag_qc_ready_at = Some(Instant::now());
                                    }
                                }
                            }
                            debug!("Dag moved to round {}", self.round);
                        }
                        DagProtocol::Narwhal => {
                            if signal.round < self.round {
                                continue;
                            }

                            // Advance to the next round.
                            self.round = signal.round;
                            self.parents_1 = signal.parents_1;
                            debug!("Dag moved to round {}", self.round);
                        }
                        DagProtocol::Bullshark | DagProtocol::Wahoo => {
                            use std::cmp::Ordering;
                            match signal.round.cmp(&self.round) {
                                Ordering::Greater => {
                                    self.round = signal.round;
                                    self.last_parent_certs = signal.certificates_1;
                                },
                                Ordering::Less => {
                                    // Ignore parents from older rounds.
                                },
                                Ordering::Equal => {
                                    self.last_parent_certs.extend(signal.certificates_1);
                                }
                            }

                            // Check whether we can advance to the next round.
                            advance = match self.round % 2 {
                                0 => self.update_leader(),
                                _ => self.enough_votes(),
                            };
                        }
                    }
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
