// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, CertificatesVecAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, EmbeddedQc, Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use crate::proposer::ProposerSignal;
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, DagProtocol};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
#[cfg(feature = "benchmark")]
use log::info;
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender, SimpleSender};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Which DAG protocol variant is running.
    dag_protocol: DagProtocol,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<Certificate>,
    /// Send block-construction contexts to the `Proposer`.
    tx_proposer: Sender<ProposerSignal>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<Digest>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// The local Shortfin round entered by this voter.
    local_round: Round,
    /// Own headers that may still receive late votes.
    own_headers: HashMap<Digest, Header>,
    /// Aggregates votes into certificates, keyed by own header id.
    votes_aggregators: HashMap<Digest, VotesAggregator>,
    /// Aggregates certificates to use as parents for new headers (Narwhal/Bullshark).
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// Aggregates certificates to use as parents (Bullshark full-cert variant).
    certificates_vec_aggregators: HashMap<Round, Box<CertificatesVecAggregator>>,
    /// Certificates observed per round keyed by authority.
    certificates_by_round: HashMap<Round, HashMap<PublicKey, Certificate>>,
    /// Certificates observed by certificate digest for incremental history checks.
    certificates_by_digest: HashMap<Digest, Certificate>,
    /// Lazy cache for reachable authors at one queried round from one root.
    history_support_cache: HashMap<(Digest, Round), HashSet<PublicKey>>,
    /// Lazy cache for whether one target header id is reachable from one root.
    history_id_cache: HashMap<(Digest, Digest), bool>,
    /// Votes for own blocks that arrived before the voter's block at
    /// `voter_round` was visible in our local DAG.
    pending_votes: HashMap<Digest, Vec<Vote>>,
    /// Next round whose completion we still need to signal to the proposer.
    next_round_to_signal: Round,
    /// Rounds for which we sent a proposer signal without QC (own cert not yet ready).
    pending_qc_signals: HashSet<Round>,
    /// Low-frequency diagnostics for Shortfin-family proposer signal stalls.
    #[cfg(feature = "benchmark")]
    diag_signal_blocked_round: Option<Round>,
    #[cfg(feature = "benchmark")]
    diag_signal_blocked_since: Option<Instant>,
    #[cfg(feature = "benchmark")]
    diag_last_signal_block_log_at: Option<Instant>,
    /// A network sender to broadcast headers and certificates reliably.
    network: ReliableSender,
    /// A best-effort network sender for votes (no retry needed).
    #[allow(dead_code)]
    vote_network: SimpleSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        dag_protocol: DagProtocol,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<ProposerSignal>,
    ) {
        let genesis = Certificate::genesis(&committee);
        let genesis_by_authority: HashMap<_, _> = genesis
            .into_iter()
            .map(|certificate| (certificate.origin(), certificate))
            .collect();
        let genesis_by_digest: HashMap<_, _> = genesis_by_authority
            .values()
            .map(|certificate| (certificate.digest(), certificate.clone()))
            .collect();
        tokio::spawn(async move {
            Self {
                name,
                committee,
                dag_protocol,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                local_round: 1,
                own_headers: HashMap::with_capacity(2 * gc_depth as usize),
                votes_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                certificates_vec_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                certificates_by_round: [(0, genesis_by_authority)].iter().cloned().collect(),
                certificates_by_digest: genesis_by_digest,
                history_support_cache: HashMap::with_capacity(2 * gc_depth as usize),
                history_id_cache: HashMap::with_capacity(2 * gc_depth as usize),
                pending_votes: HashMap::with_capacity(2 * gc_depth as usize),
                next_round_to_signal: 1,
                pending_qc_signals: HashSet::new(),
                #[cfg(feature = "benchmark")]
                diag_signal_blocked_round: None,
                #[cfg(feature = "benchmark")]
                diag_signal_blocked_since: None,
                #[cfg(feature = "benchmark")]
                diag_last_signal_block_log_at: None,
                network: ReliableSender::new(),
                vote_network: SimpleSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
            }
            .run()
            .await;
        });
    }

    fn certificate_to_embedded_qc(certificate: &Certificate) -> EmbeddedQc {
        EmbeddedQc {
            target: certificate.header.id.clone(),
            round: certificate.round(),
            votes: certificate.votes.clone(),
        }
    }

    fn round_digests(&self, round: Round) -> Vec<Digest> {
        self.certificates_by_round
            .get(&round)
            .map(|by_authority| by_authority.values().map(|x| x.digest()).collect())
            .unwrap_or_default()
    }

    fn latest_ref_digests(&self) -> Vec<Digest> {
        self.committee
            .authorities
            .keys()
            .filter_map(|name| {
                self.certificates_by_round
                    .values()
                    .filter_map(|by_authority| by_authority.get(name))
                    .max_by_key(|certificate| certificate.round())
                    .map(|certificate| certificate.digest())
            })
            .collect()
    }

    fn index_certificate(&mut self, certificate: &Certificate) {
        self.certificates_by_digest
            .insert(certificate.digest(), certificate.clone());
    }

    fn index_roots(&mut self, roots: &[Certificate]) {
        for certificate in roots {
            self.index_certificate(certificate);
        }
    }

    fn reachable_authors_at_round(
        &mut self,
        root: &Digest,
        round: Round,
        visiting: &mut HashSet<Digest>,
    ) -> Option<HashSet<PublicKey>> {
        if let Some(authors) = self.history_support_cache.get(&(root.clone(), round)) {
            return Some(authors.clone());
        }

        if !visiting.insert(root.clone()) {
            return Some(HashSet::new());
        }

        let Some(certificate) = self.certificates_by_digest.get(root).cloned() else {
            visiting.remove(root);
            return None;
        };

        let mut authors = HashSet::new();
        if certificate.round() == round {
            authors.insert(certificate.origin());
        } else if certificate.round() > round {
            for parent in certificate
                .header
                .parents
                .iter()
                .chain(certificate.header.parents_2.iter())
            {
                let Some(parent_authors) = self.reachable_authors_at_round(parent, round, visiting)
                else {
                    visiting.remove(root);
                    return None;
                };
                authors.extend(parent_authors);
            }
        }

        visiting.remove(root);
        self.history_support_cache
            .insert((root.clone(), round), authors.clone());
        Some(authors)
    }

    fn history_support_weight_from_roots(&mut self, roots: &[Certificate], round: Round) -> u32 {
        self.history_authors_at_round_from_roots(roots, round)
            .iter()
            .map(|author| self.committee.stake(author))
            .sum()
    }

    fn history_authors_at_round_from_roots(
        &mut self,
        roots: &[Certificate],
        round: Round,
    ) -> HashSet<PublicKey> {
        self.index_roots(roots);

        let mut authors = HashSet::new();
        for root in roots {
            let mut visiting = HashSet::new();
            if let Some(root_authors) =
                self.reachable_authors_at_round(&root.digest(), round, &mut visiting)
            {
                authors.extend(root_authors);
            }
        }
        authors
    }

    fn history_contains_header_from_root(
        &mut self,
        root: &Digest,
        id: &Digest,
        origin: PublicKey,
        min_round: Round,
        visiting: &mut HashSet<Digest>,
    ) -> Option<bool> {
        if let Some(contains) = self.history_id_cache.get(&(root.clone(), id.clone())) {
            return Some(*contains);
        }

        if !visiting.insert(root.clone()) {
            return Some(false);
        }

        let Some(certificate) = self.certificates_by_digest.get(root).cloned() else {
            visiting.remove(root);
            return None;
        };

        if certificate.header.id == *id
            && certificate.origin() == origin
            && certificate.round() == min_round
        {
            visiting.remove(root);
            self.history_id_cache
                .insert((root.clone(), id.clone()), true);
            return Some(true);
        } else if certificate.round() < min_round {
            visiting.remove(root);
            self.history_id_cache
                .insert((root.clone(), id.clone()), false);
            return Some(false);
        } else {
            let parents: Vec<_> = certificate
                .header
                .parents
                .iter()
                .chain(certificate.header.parents_2.iter())
                .cloned()
                .collect();
            for parent in parents {
                match self
                    .history_contains_header_from_root(&parent, id, origin, min_round, visiting)
                {
                    Some(true) => {
                        visiting.remove(root);
                        self.history_id_cache
                            .insert((root.clone(), id.clone()), true);
                        return Some(true);
                    }
                    Some(false) => {}
                    None => {
                        visiting.remove(root);
                        return None;
                    }
                }
            }
        }

        visiting.remove(root);
        self.history_id_cache
            .insert((root.clone(), id.clone()), false);
        Some(false)
    }

    fn history_contains_header_from_roots(
        &mut self,
        roots: &[Certificate],
        id: &Digest,
        origin: PublicKey,
        round: Round,
    ) -> bool {
        self.index_roots(roots);

        if roots
            .iter()
            .any(|root| root.header.id == *id && root.origin() == origin && root.round() == round)
        {
            return true;
        }

        roots.iter().any(|root| {
            let mut visiting = HashSet::new();
            self.history_contains_header_from_root(&root.digest(), id, origin, round, &mut visiting)
                .unwrap_or(false)
        })
    }

    #[cfg(feature = "benchmark")]
    #[allow(clippy::too_many_arguments)]
    fn diag_signal_blocked(
        &mut self,
        round: Round,
        reason: &'static str,
        round_authorities: usize,
        round_weight: u32,
        round_threshold: u32,
        parents_1_len: usize,
        previous_round: Round,
        parents_2_authorities: usize,
        parents_2_weight: u32,
        parents_2_threshold: u32,
        own_qc: bool,
    ) {
        if !self.dag_protocol.is_shortfin_family() {
            return;
        }

        let now = Instant::now();
        if self.diag_signal_blocked_round != Some(round) {
            self.diag_signal_blocked_round = Some(round);
            self.diag_signal_blocked_since = Some(now);
            self.diag_last_signal_block_log_at = None;
        }

        let should_log = self
            .diag_last_signal_block_log_at
            .map(|last| now.saturating_duration_since(last).as_secs() >= 5)
            .unwrap_or(true);
        if !should_log {
            return;
        }

        self.diag_last_signal_block_log_at = Some(now);
        let blocked_total_ms = self
            .diag_signal_blocked_since
            .map(|started| now.saturating_duration_since(started).as_millis() as u64)
            .unwrap_or_default();

        info!(
            "DIAG_CORE_SIGNAL_BLOCKED round={} target_round={} reason={} round_authorities={} round_weight={}/{} parents_1={} previous_round={} parents_2_authorities={} parents_2_weight={}/{} own_qc={} blocked_total_ms={}",
            round,
            round + 1,
            reason,
            round_authorities,
            round_weight,
            round_threshold,
            parents_1_len,
            previous_round,
            parents_2_authorities,
            parents_2_weight,
            parents_2_threshold,
            own_qc,
            blocked_total_ms,
        );
    }

    async fn try_signal_proposer(&mut self) {
        loop {
            let round = self.next_round_to_signal;
            let Some(by_authority) = self.certificates_by_round.get(&round) else {
                let previous_round = if round == 1 { 0 } else { round - 1 };
                let (_parents_2_authorities, _parents_2_weight) = self
                    .certificates_by_round
                    .get(&previous_round)
                    .map(|by_authority| {
                        (
                            by_authority.len(),
                            by_authority
                                .keys()
                                .map(|name| self.committee.stake(name))
                                .sum::<u32>(),
                        )
                    })
                    .unwrap_or_default();
                #[cfg(feature = "benchmark")]
                self.diag_signal_blocked(
                    round,
                    "missing_round_certificates",
                    0,
                    0,
                    self.committee.quorum_threshold(),
                    0,
                    previous_round,
                    _parents_2_authorities,
                    _parents_2_weight,
                    self.committee.quorum_threshold(),
                    false,
                );
                break;
            };

            let round_weight: u32 = by_authority
                .keys()
                .map(|name| self.committee.stake(name))
                .sum();
            if round_weight < self.committee.quorum_threshold() {
                let previous_round = if round == 1 { 0 } else { round - 1 };
                let (_parents_2_authorities, _parents_2_weight) = self
                    .certificates_by_round
                    .get(&previous_round)
                    .map(|by_authority| {
                        (
                            by_authority.len(),
                            by_authority
                                .keys()
                                .map(|name| self.committee.stake(name))
                                .sum::<u32>(),
                        )
                    })
                    .unwrap_or_default();
                #[cfg(feature = "benchmark")]
                self.diag_signal_blocked(
                    round,
                    "round_weight_below_quorum",
                    by_authority.len(),
                    round_weight,
                    self.committee.quorum_threshold(),
                    by_authority.len(),
                    previous_round,
                    _parents_2_authorities,
                    _parents_2_weight,
                    self.committee.quorum_threshold(),
                    by_authority.contains_key(&self.name),
                );
                break;
            }

            let parents_1 = self.round_digests(round);
            if parents_1.is_empty() {
                let previous_round = if round == 1 { 0 } else { round - 1 };
                let (_parents_2_authorities, _parents_2_weight) = self
                    .certificates_by_round
                    .get(&previous_round)
                    .map(|by_authority| {
                        (
                            by_authority.len(),
                            by_authority
                                .keys()
                                .map(|name| self.committee.stake(name))
                                .sum::<u32>(),
                        )
                    })
                    .unwrap_or_default();
                #[cfg(feature = "benchmark")]
                self.diag_signal_blocked(
                    round,
                    "empty_parents_1",
                    by_authority.len(),
                    round_weight,
                    self.committee.quorum_threshold(),
                    parents_1.len(),
                    previous_round,
                    _parents_2_authorities,
                    _parents_2_weight,
                    self.committee.quorum_threshold(),
                    by_authority.contains_key(&self.name),
                );
                break;
            }

            let previous_round = if round == 1 { 0 } else { round - 1 };

            if self.dag_protocol.is_mahi_mahi() {
                let signal = ProposerSignal {
                    round: round + 1,
                    parents_1,
                    parents_2: Vec::new(),
                    qc: None,
                    certificates_1: Vec::new(),
                };
                self.tx_proposer
                    .send(signal)
                    .await
                    .expect("Failed to send certificate");
                self.next_round_to_signal += 1;
                continue;
            }

            if self.dag_protocol.is_shortfin_family() {
                let parents_1 = self.latest_ref_digests();
                let own_certificate = by_authority
                    .get(&self.name)
                    .filter(|certificate| !certificate.votes.is_empty());
                let qc = own_certificate.map(|c| Self::certificate_to_embedded_qc(c));
                let needs_qc = round >= 1; // proposer target round >= 2
                if needs_qc && qc.is_none() {
                    #[cfg(feature = "benchmark")]
                    self.diag_signal_blocked(
                        round,
                        "missing_own_qc",
                        by_authority.len(),
                        round_weight,
                        self.committee.quorum_threshold(),
                        parents_1.len(),
                        previous_round,
                        0,
                        0,
                        self.committee.quorum_threshold(),
                        false,
                    );
                    break;
                }

                let signal = ProposerSignal {
                    round: round + 1,
                    parents_1,
                    parents_2: Vec::new(),
                    qc,
                    certificates_1: Vec::new(),
                };

                self.tx_proposer
                    .send(signal)
                    .await
                    .expect("Failed to send certificate");
                self.local_round = self.local_round.max(round + 1);

                #[cfg(feature = "benchmark")]
                if self.diag_signal_blocked_round == Some(round) {
                    self.diag_signal_blocked_round = None;
                    self.diag_signal_blocked_since = None;
                    self.diag_last_signal_block_log_at = None;
                }

                self.next_round_to_signal += 1;
                continue;
            }

            let parents_2 = self.round_digests(previous_round);

            let (_parents_2_authorities, parents_2_weight): (usize, u32) = self
                .certificates_by_round
                .get(&previous_round)
                .map(|by_authority| {
                    (
                        by_authority.len(),
                        by_authority
                            .keys()
                            .map(|name| self.committee.stake(name))
                            .sum::<u32>(),
                    )
                })
                .unwrap_or_default();
            if round >= 2 && parents_2_weight < self.committee.quorum_threshold() {
                #[cfg(feature = "benchmark")]
                self.diag_signal_blocked(
                    round,
                    "parents_2_weight_below_quorum",
                    by_authority.len(),
                    round_weight,
                    self.committee.quorum_threshold(),
                    parents_1.len(),
                    previous_round,
                    _parents_2_authorities,
                    parents_2_weight,
                    self.committee.quorum_threshold(),
                    by_authority.contains_key(&self.name),
                );
                break;
            }

            // Require our own certificate's QC before signalling the proposer.
            // Without it the proposer would deadlock: the follow-up QC signal
            // arrives too late once the proposer has advanced to a later round.
            let own_certificate = by_authority.get(&self.name);
            let qc = own_certificate.map(|c| Self::certificate_to_embedded_qc(c));
            let needs_qc = round >= 1; // proposer target round >= 2
            if needs_qc && qc.is_none() {
                #[cfg(feature = "benchmark")]
                self.diag_signal_blocked(
                    round,
                    "missing_own_qc",
                    by_authority.len(),
                    round_weight,
                    self.committee.quorum_threshold(),
                    parents_1.len(),
                    previous_round,
                    _parents_2_authorities,
                    parents_2_weight,
                    self.committee.quorum_threshold(),
                    false,
                );
                break;
            }

            let signal = ProposerSignal {
                round: round + 1,
                parents_1,
                parents_2,
                qc,
                certificates_1: Vec::new(),
            };

            self.tx_proposer
                .send(signal)
                .await
                .expect("Failed to send certificate");

            #[cfg(feature = "benchmark")]
            if self.diag_signal_blocked_round == Some(round) {
                self.diag_signal_blocked_round = None;
                self.diag_signal_blocked_since = None;
                self.diag_last_signal_block_log_at = None;
            }

            self.next_round_to_signal += 1;
        }
    }

    /// Send a QC-only follow-up signal when our own certificate arrives late.
    async fn send_qc_signal(&mut self, certificate: &Certificate) {
        let target_round = certificate.round() + 1;
        if self.pending_qc_signals.remove(&target_round) {
            let qc = Self::certificate_to_embedded_qc(certificate);
            let signal = ProposerSignal {
                round: target_round,
                parents_1: Vec::new(),
                parents_2: Vec::new(),
                qc: Some(qc),
                certificates_1: Vec::new(),
            };
            let _ = self.tx_proposer.send(signal).await;
        }
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Track the own header so late votes can still assemble its QC after
        // the proposer has moved on.
        if self.dag_protocol.is_shortfin_family() {
            self.local_round = self.local_round.max(header.round);
        }
        self.current_header = header.clone();
        self.own_headers.insert(header.id.clone(), header.clone());
        self.votes_aggregators
            .entry(header.id.clone())
            .or_insert_with(VotesAggregator::new);

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        // Indicate that we are processing this header.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector; it will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        // 延迟构成-Primary阶段C：这里是两跳 parent 依赖等待点，缺失会直接挂起当前 header 处理。
        let (parents_1, parents_2) = self.synchronizer.get_parents(header).await?;
        if parents_1.is_empty() && !header.parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }

        if header.round == 0 {
            // Genesis/initialization headers have no parent quorum requirements.
        } else {
            match self.dag_protocol {
                DagProtocol::Shortfin | DagProtocol::Sailfin => {
                    let support_round = header.round.saturating_sub(1);
                    let stake_1 = self.history_support_weight_from_roots(&parents_1, support_round);
                    ensure!(
                        stake_1 >= self.committee.quorum_threshold(),
                        DagError::HeaderRequiresQuorum(header.id.clone())
                    );

                    ensure!(
                        parents_2.is_empty(),
                        DagError::MalformedHeader(header.id.clone())
                    );

                    if header.round >= 2 {
                        let qc = header
                            .qc
                            .as_ref()
                            .ok_or_else(|| DagError::MalformedHeader(header.id.clone()))?;
                        ensure!(
                            qc.round + 1 == header.round,
                            DagError::MalformedHeader(header.id.clone())
                        );
                        ensure!(
                            self.history_contains_header_from_roots(
                                &parents_1,
                                &qc.target,
                                header.author,
                                qc.round,
                            ),
                            DagError::MalformedHeader(header.id.clone())
                        );
                        let mut voters_by_round: HashMap<Round, HashSet<PublicKey>> =
                            HashMap::new();
                        for vote in &qc.votes {
                            ensure!(
                                vote.voter_round > 0,
                                DagError::MalformedHeader(header.id.clone())
                            );
                            voters_by_round
                                .entry(vote.voter_round)
                                .or_insert_with(HashSet::new)
                                .insert(vote.author);
                        }

                        for (voter_round, voters) in voters_by_round {
                            let voter_round_authors =
                                self.history_authors_at_round_from_roots(&parents_1, voter_round);
                            ensure!(
                                voters.is_subset(&voter_round_authors),
                                DagError::HeaderRequiresQuorum(header.id.clone())
                            );
                            let vote_support_round = voter_round.saturating_sub(1);
                            ensure!(
                                self.history_support_weight_from_roots(
                                    &parents_1,
                                    vote_support_round
                                ) >= self.committee.quorum_threshold(),
                                DagError::HeaderRequiresQuorum(header.id.clone())
                            );
                        }
                    }
                }
                DagProtocol::Narwhal
                | DagProtocol::Bullshark
                | DagProtocol::MahiMahi
                | DagProtocol::MahiMahi4
                | DagProtocol::MahiMahi5
                | DagProtocol::Wahoo => {
                    // Single-parent validation: r-1 parents must form a quorum.
                    let mut stake_1 = 0;
                    for x in &parents_1 {
                        ensure!(
                            x.round() + 1 == header.round,
                            DagError::MalformedHeader(header.id.clone())
                        );
                        stake_1 += self.committee.stake(&x.origin());
                    }
                    if header.round > 0 {
                        ensure!(
                            stake_1 >= self.committee.quorum_threshold(),
                            DagError::HeaderRequiresQuorum(header.id.clone())
                        );
                    }
                }
            }
        }

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        // 延迟构成-Primary阶段D：payload 不齐也会挂起，间接拖慢后续投票/证书形成。
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Store the header.
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        if self.dag_protocol.is_mahi_mahi() {
            return Ok(());
        }

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // Make a vote and send it to the header's creator.
            let voter_round = if self.dag_protocol.is_shortfin_family() {
                self.local_round
            } else {
                header.round
            };
            let vote =
                Vote::new(header, voter_round, &self.name, &mut self.signature_service).await;
            debug!("Created {:?}", vote);
            if vote.origin == self.name {
                self.process_vote(vote)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                let address = self
                    .committee
                    .primary(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    /// Uncertified DAG protocols: peer blocks never arrive as independent Certificates.
    /// Shortfin-family carries the author's QC in the next header; Mahi-Mahi
    /// has no votes at all and treats the signed header itself as the block.
    /// To keep the downstream DAG-tracking logic uniform, we synthesize a
    /// local empty-votes Certificate from every peer Header we successfully
    /// processed. The consensus layer only reads certificate.header.* fields
    /// (never certificate.votes), so an empty-votes certificate is
    /// semantically equivalent to a real one here. This is only invoked at
    /// the direct Header dispatch points — never inside process_certificate's
    /// header-processing path, to avoid double-emitting a cert for the same
    /// block.
    async fn maybe_synthesize_uncertified_cert(&mut self, header: &Header) {
        if !self.dag_protocol.is_uncertified_dag() {
            return;
        }
        let synthetic = Certificate {
            header: header.clone(),
            votes: Vec::new(),
        };
        if let Err(e) = self.process_certificate(synthetic).await {
            warn!("Failed to process synthetic certificate: {}", e);
        }
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        let Some(header) = self.own_headers.get(&vote.id).cloned() else {
            return Err(DagError::UnexpectedVote(vote.id.clone()));
        };

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) = self
            .votes_aggregators
            .entry(header.id.clone())
            .or_insert_with(VotesAggregator::new)
            .append(vote, &self.committee, &header)?
        {
            debug!("Assembled {:?}", certificate);

            // Broadcast the certificate (Narwhal/Bullshark: the cert itself
            // is the 3rd network phase). Shortfin-family protocols skip this phase entirely:
            // the QC is delivered by piggybacking inside the next round's
            // header.qc field, saving one delta of latency per round.
            if !self.dag_protocol.is_uncertified_dag() {
                let addresses = self
                    .committee
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(_, x)| x.primary_to_primary)
                    .collect();
                let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                    .expect("Failed to serialize our own certificate");
                let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(certificate.round())
                    .or_insert_with(Vec::new)
                    .extend(handlers);
            }

            // Process the new certificate locally in all modes.
            self.process_certificate(certificate)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        // Process the header embedded in the certificate if we haven't already voted for it (if we already
        // voted, it means we already processed it). Since this header got certified, we are sure that all
        // the data it refers to (ie. its payload and its parents) are available. We can thus continue the
        // processing of the certificate even if we don't have them in store right now.
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // This function may still throw an error if the storage fails.
            self.process_header(&certificate.header).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }

        match self.dag_protocol {
            DagProtocol::Shortfin
            | DagProtocol::Sailfin
            | DagProtocol::MahiMahi
            | DagProtocol::MahiMahi4
            | DagProtocol::MahiMahi5 => {
                // Uncertified DAG certificates are never broadcast: peer blocks
                // arrive as headers and are synthesized locally with empty votes.
                // Store them after local header validation so HeaderWaiter
                // notify_read() calls wake up and helpers can answer sync
                // requests for locally observed Shortfin-family parents.
                let bytes =
                    bincode::serialize(&certificate).expect("Failed to serialize certificate");
                self.store.write(certificate.digest().to_vec(), bytes).await;
                self.synchronizer.cache_certificate(&certificate);

                self.certificates_by_round
                    .entry(certificate.round())
                    .or_insert_with(HashMap::new)
                    .insert(certificate.origin(), certificate.clone());
                self.index_certificate(&certificate);

                self.process_pending_votes().await?;
                self.try_signal_proposer().await;

                // Shortfin-family only: if this is our own newly-formed
                // certificate, send a QC follow-up.
                if self.dag_protocol.is_shortfin_family()
                    && certificate.origin() == self.name
                    && !certificate.votes.is_empty()
                {
                    self.send_qc_signal(&certificate).await;
                }
            }
            DagProtocol::Narwhal => {
                // Store to disk for crash recovery.
                let bytes =
                    bincode::serialize(&certificate).expect("Failed to serialize certificate");
                self.store.write(certificate.digest().to_vec(), bytes).await;

                if let Some(parents) = self
                    .certificates_aggregators
                    .entry(certificate.round())
                    .or_insert_with(|| Box::new(CertificatesAggregator::new()))
                    .append(certificate.clone(), &self.committee)?
                {
                    let signal = ProposerSignal {
                        round: certificate.round() + 1,
                        parents_1: parents,
                        parents_2: Vec::new(),
                        qc: None,
                        certificates_1: Vec::new(),
                    };
                    self.tx_proposer
                        .send(signal)
                        .await
                        .expect("Failed to send certificate");
                }
            }
            DagProtocol::Bullshark | DagProtocol::Wahoo => {
                // Store to disk for crash recovery.
                let bytes =
                    bincode::serialize(&certificate).expect("Failed to serialize certificate");
                self.store.write(certificate.digest().to_vec(), bytes).await;

                if let Some(parents) = self
                    .certificates_vec_aggregators
                    .entry(certificate.round())
                    .or_insert_with(|| Box::new(CertificatesVecAggregator::new()))
                    .append(certificate.clone(), &self.committee)?
                {
                    let parents_1: Vec<Digest> = parents.iter().map(|c| c.digest()).collect();
                    let signal = ProposerSignal {
                        round: certificate.round(),
                        parents_1,
                        parents_2: Vec::new(),
                        qc: None,
                        certificates_1: parents,
                    };
                    self.tx_proposer
                        .send(signal)
                        .await
                        .expect("Failed to send certificate");
                }
            }
        }

        // Send it to the consensus layer.
        let id = certificate.header.id.clone();
        if let Err(e) = self.tx_consensus.send(certificate).await {
            warn!(
                "Failed to deliver certificate {} to the consensus: {}",
                id, e
            );
        }
        Ok(())
    }

    async fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );

        // Reject headers with round numbers that are too far in the future.
        let local_round = if self.dag_protocol.is_shortfin_family() {
            self.local_round
        } else {
            self.current_header.round
        };
        let max_future_round = local_round.saturating_add(10);
        ensure!(
            header.round <= max_future_round,
            DagError::TooOld(header.id.clone(), header.round)
        );

        // Verify the header's signature (CPU-bound; runs on blocking pool).
        header
            .verify_async(&self.committee, self.dag_protocol)
            .await?;

        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.gc_round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );

        let Some(header) = self.own_headers.get(&vote.id) else {
            return Err(DagError::UnexpectedVote(vote.id.clone()));
        };
        ensure!(
            vote.origin == header.author && vote.round == header.round,
            DagError::UnexpectedVote(vote.id.clone())
        );

        if self.dag_protocol.is_shortfin_family() {
            ensure!(
                vote.voter_round > 0,
                DagError::UnexpectedVote(vote.id.clone())
            );
        }

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn vote_pending_reason(&self, vote: &Vote) -> Option<&'static str> {
        if vote.voter_round == 0 {
            return None;
        }

        let voter_block_available = self
            .certificates_by_round
            .get(&vote.voter_round)
            .map(|by_authority| by_authority.contains_key(&vote.author))
            .unwrap_or(false);
        if voter_block_available {
            None
        } else {
            Some("missing_voter_round_block")
        }
    }

    fn cache_pending_vote(&mut self, vote: Vote) {
        debug!(
            "Caching vote {:?}: missing round block for voter {} round {}",
            vote.digest(),
            vote.author,
            vote.voter_round
        );
        self.pending_votes
            .entry(vote.id.clone())
            .or_insert_with(Vec::new)
            .push(vote);
    }

    async fn process_vote_when_countable(&mut self, vote: Vote) -> DagResult<()> {
        if self.vote_pending_reason(&vote).is_none() {
            self.process_vote(vote).await
        } else {
            self.cache_pending_vote(vote);
            Ok(())
        }
    }

    async fn process_pending_votes(&mut self) -> DagResult<()> {
        let pending_votes = std::mem::take(&mut self.pending_votes);
        if pending_votes.is_empty() {
            return Ok(());
        }

        for (id, pending) in pending_votes {
            let mut still_pending = Vec::new();
            for vote in pending {
                if self.vote_pending_reason(&vote).is_none() {
                    self.process_vote(vote).await?;
                } else {
                    still_pending.push(vote);
                }
            }

            if !still_pending.is_empty() {
                self.pending_votes.insert(id, still_pending);
            }
        }
        Ok(())
    }

    async fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );

        // Verify the certificate (and the embedded header); CPU-bound work
        // runs on the blocking pool.
        certificate
            .verify_async(&self.committee, self.dag_protocol)
            .await?;
        Ok(())
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            let result = match self.sanitize_header(&header).await {
                                Ok(()) => self.process_header(&header).await,
                                error => error,
                            };
                            if result.is_ok() {
                                self.maybe_synthesize_uncertified_cert(&header).await;
                            }
                            result
                        },
                PrimaryMessage::Vote(vote) => {
                    match self.sanitize_vote(&vote) {
                        Ok(()) if self.dag_protocol.is_shortfin_family() => {
                            self.process_vote_when_countable(vote).await
                        }
                        Ok(()) => self.process_vote(vote).await,
                        error => error,
                    }
                },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate).await {
                                Ok(()) => self.process_certificate(certificate).await,
                                error => error,
                            }
                        },
                        _ => panic!("Unexpected core message"),
                    }
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => {
                    let result = self.process_header(&header).await;
                    if result.is_ok() {
                        self.maybe_synthesize_uncertified_cert(&header).await;
                    }
                    result
                },

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => {
                    let result = self.process_own_header(header.clone()).await;
                    if result.is_ok() {
                        self.maybe_synthesize_uncertified_cert(&header).await;
                    }
                    result
                },
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.own_headers
                    .retain(|_, header| header.round >= gc_round);
                let live_own_headers: HashSet<_> = self.own_headers.keys().cloned().collect();
                self.votes_aggregators
                    .retain(|id, _| live_own_headers.contains(id));
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.certificates_vec_aggregators
                    .retain(|k, _| k >= &gc_round);
                self.certificates_by_round.retain(|k, _| k >= &gc_round);
                let live_digests: HashSet<_> = self
                    .certificates_by_round
                    .values()
                    .flat_map(|by_authority| {
                        by_authority
                            .values()
                            .map(|certificate| certificate.digest())
                            .collect::<Vec<_>>()
                    })
                    .collect();
                self.certificates_by_digest
                    .retain(|digest, _| live_digests.contains(digest));
                self.history_support_cache
                    .retain(|(digest, _), _| live_digests.contains(digest));
                self.history_id_cache
                    .retain(|(digest, _), _| live_digests.contains(digest));
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}
