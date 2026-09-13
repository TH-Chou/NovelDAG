// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
use std::collections::HashSet;
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

fn mahi_equivocation_batches(
    batch: &Batch,
    count: usize,
    sequence: u64,
) -> Vec<(Digest, SerializedBatchMessage)> {
    let mut variants = (0..count)
        .map(|slot| {
            let mut variant = batch.clone();
            let mut marker = [0u8; 16];
            marker[..8].copy_from_slice(&sequence.to_le_bytes());
            marker[8..].copy_from_slice(&(slot as u64).to_le_bytes());
            if let Some(transaction) = variant.first_mut() {
                if transaction.len() >= marker.len() {
                    let offset = transaction.len() - marker.len();
                    transaction[offset..].copy_from_slice(&marker);
                } else {
                    transaction.extend_from_slice(&marker);
                }
            } else {
                variant.push(marker.to_vec());
            }
            let message = WorkerMessage::Batch(variant);
            let serialized = bincode::serialize(&message)
                .expect("Failed to serialize Byzantine Mahi-Mahi batch");
            let hash = Sha512::digest(&serialized);
            let digest = Digest(hash[..32].try_into().unwrap());
            (digest, serialized)
        })
        .collect::<Vec<_>>();
    variants.sort_by(|(left, _), (right, _)| left.cmp(right));
    variants
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// Direct path used by bounded Mahi-Mahi attack batches, which have one
    /// honest holder rather than a worker availability quorum.
    tx_processor: Sender<SerializedBatchMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// Whether this Byzantine worker generates one payload per honest peer.
    mahi_equivocation: bool,
    /// Worker-to-worker addresses belonging to Byzantine authorities.
    byzantine_addresses: HashSet<SocketAddr>,
    /// Makes every generated payload unique across base batches and versions.
    attack_sequence: u64,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        tx_processor: Sender<SerializedBatchMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        let mahi_equivocation = std::env::var("NOVELDAG_BYZANTINE_ATTACK").as_deref()
            == Ok("equivocation")
            && std::env::var("NOVELDAG_DAG_PROTOCOL")
                .map(|value| value.replace('-', "_").starts_with("mahi_mahi"))
                .unwrap_or(false);
        let byzantine_addresses = std::env::var("NOVELDAG_BYZANTINE_WORKER_ADDRS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|value| value.parse::<SocketAddr>().ok())
            .collect();
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                tx_processor,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                mahi_equivocation,
                byzantine_addresses,
                attack_sequence: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.len() > 8 && tx[0] == 0u8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        if self.mahi_equivocation {
            self.seal_mahi_equivocation(batch, size, tx_ids).await;
            return;
        }
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let hash = Sha512::digest(&serialized);
            let digest = Digest(hash[..32].try_into().unwrap());

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }

    /// Produce one full, distinct payload for every honest worker. Each
    /// payload is sent to exactly one honest holder and then exposed to the
    /// local primary. This models Mahi-Mahi's lack of a data-availability
    /// certificate without turning the worker layer into unbounded spam.
    async fn seal_mahi_equivocation(
        &mut self,
        batch: Batch,
        _original_size: usize,
        _tx_ids: Vec<[u8; 8]>,
    ) {
        let mut honest_workers = self
            .workers_addresses
            .iter()
            .filter(|(_, address)| !self.byzantine_addresses.contains(address))
            .cloned()
            .collect::<Vec<_>>();
        honest_workers.sort_by_key(|(name, _)| *name);
        if honest_workers.is_empty() {
            return;
        }

        self.attack_sequence = self.attack_sequence.wrapping_add(1);
        let variants =
            mahi_equivocation_batches(&batch, honest_workers.len(), self.attack_sequence);

        let mut handlers = Vec::with_capacity(variants.len());
        for ((_digest, serialized), (_, address)) in variants.iter().zip(&honest_workers) {
            #[cfg(feature = "benchmark")]
            {
                for id in &_tx_ids {
                    info!(
                        "Batch {:?} contains sample tx {}",
                        _digest,
                        u64::from_be_bytes(*id)
                    );
                }
                info!("Batch {:?} contains {} B", _digest, _original_size);
                info!(
                    "Byzantine Mahi-Mahi payload {:?} targeted to {}",
                    _digest, address
                );
            }
            handlers.push(
                self.network
                    .send(*address, Bytes::from(serialized.clone()))
                    .await,
            );
        }

        // Do not expose a digest to the primary until its unique honest
        // holder acknowledged the payload. The Byzantine worker deliberately
        // bypasses the normal 2f+1 availability quorum.
        futures::future::join_all(handlers).await;
        for (_, serialized) in variants {
            self.tx_processor
                .send(serialized)
                .await
                .expect("Failed to process Byzantine Mahi-Mahi batch");
        }
    }
}
