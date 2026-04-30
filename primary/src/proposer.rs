// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, EmbeddedQc, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{coin_threshold, make_coin_share, Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
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
}

pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,
    /// Authorities used for threshold-coin shares.
    coin_authorities: Vec<PublicKey>,
    /// Threshold used for threshold-coin shares.
    coin_threshold: usize,
    /// Quorum threshold used by construction rules.
    quorum_threshold: usize,

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
    /// Holds the QC of our previous-round block.
    last_qc: Option<EmbeddedQc>,
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
        let coin_authorities: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let coin_threshold = coin_threshold(committee.size());
        let quorum_threshold = committee.quorum_threshold() as usize;

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                coin_authorities,
                coin_threshold,
                quorum_threshold,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                parents_1: genesis,
                parents_2: Vec::new(),
                last_qc: None,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        let coin_share = make_coin_share(
            &self.coin_authorities,
            self.coin_threshold,
            &self.name,
            self.round,
        )
        .unwrap_or_default();

        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.parents_1.drain(..).collect::<BTreeSet<_>>(),
            self.parents_2.drain(..).collect::<BTreeSet<_>>(),
            self.last_qc.clone(),
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

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents_1 = !self.parents_1.is_empty();
            let enough_parents_2 = self.round < 2 || self.parents_2.len() >= self.quorum_threshold;
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
            }

            if ready_to_propose && enough_parents_1 && enough_parents_2 && enough_qc {
                #[cfg(feature = "benchmark")]
                if let Some(started_at) = diag_blocked_since.take() {
                    diag_blocked_total_ms += started_at.elapsed().as_millis() as u64;
                }

                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                #[cfg(feature = "benchmark")]
                {
                    diag_headers_created += 1;
                    if diag_headers_created % 20 == 0 {
                        info!(
                            "DIAG_PROPOSER_GATE round={} headers={} blocked_attempts={} blocked_windows={} missing_parents_1={} missing_parents_2={} missing_qc={} blocked_total_ms={}",
                            self.round,
                            diag_headers_created,
                            diag_blocked_attempts,
                            diag_blocked_windows,
                            diag_missing_parents_1,
                            diag_missing_parents_2,
                            diag_missing_qc,
                            diag_blocked_total_ms,
                        );
                    }
                }

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some(signal) = self.rx_core.recv() => {
                    if signal.round < self.round {
                        continue;
                    }

                    self.round = signal.round;
                    // Only overwrite parents if the signal carries them (non-empty).
                    // A QC-only follow-up has empty parents and only updates last_qc.
                    if !signal.parents_1.is_empty() {
                        self.parents_1 = signal.parents_1;
                    }
                    if !signal.parents_2.is_empty() {
                        self.parents_2 = signal.parents_2;
                    }
                    if signal.qc.is_some() {
                        self.last_qc = signal.qc;
                    }
                    debug!("Dag moved to round {}", self.round);
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
