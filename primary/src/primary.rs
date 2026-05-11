// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{Certificate, Header, Vote};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use crate::wahoo::{messages::SignedWahoo, Node as WahooNode, WahooMessage};
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, DagProtocol, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
    /// All Wahoo-specific traffic is multiplexed through this variant.
    /// The inner `SignedWahoo` envelope carries an ED25519 signature
    /// over the bincode-encoded `WahooMessage`, mirroring Go's
    /// `MsgWithSig{Msg, Sig}` triple. Only delivered when the running
    /// `dag_protocol` is `DagProtocol::Wahoo`.
    Wahoo(SignedWahoo),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The primary indicates a round update.
    Cleanup(Round),
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// The worker indicates it sealed a new batch.
    OurBatch(Digest, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) {
        if parameters.dag_protocol == DagProtocol::Wahoo {
            // Wahoo runs an entirely independent state machine that does
            // not use Header/Vote/Certificate or the
            // Core/Proposer/Synchronizer pipeline. We spawn its own node
            // here and return.
            Self::spawn_wahoo(keypair, committee, parameters, store, tx_consensus, rx_consensus);
            return;
        }
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);

        // Write the parameters to the logs.
        parameters.log();

        // Parse the public and secret key of this authority.
        let name = keypair.name;
        let secret = keypair.secret;

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages,
                tx_cert_requests,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );

        // Spawn the network receiver listening to messages from our workers.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        // The `Synchronizer` provides auxiliary methods helping to `Core` to sync.
        let synchronizer = Synchronizer::new(
            name,
            &committee,
            parameters.dag_protocol,
            store.clone(),
            /* tx_header_waiter */ tx_sync_headers,
            /* tx_certificate_waiter */ tx_sync_certificates,
        );

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(secret);

        // The `Core` receives and handles headers, votes, and certificates from the other primaries.
        Core::spawn(
            name,
            committee.clone(),
            parameters.dag_protocol,
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* rx_primaries */ rx_primary_messages,
            /* rx_header_waiter */ rx_headers_loopback,
            /* rx_certificate_waiter */ rx_certificates_loopback,
            /* rx_proposer */ rx_headers,
            tx_consensus,
            /* tx_proposer */ tx_parents,
        );

        // Keeps track of the latest consensus round and allows other tasks to clean up their their internal state
        GarbageCollector::spawn(&name, &committee, consensus_round.clone(), rx_consensus);

        // Receives batch digests from other workers. They are only used to validate headers.
        PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

        // Whenever the `Synchronizer` does not manage to validate a header due to missing parent certificates of
        // batch digests, it commands the `HeaderWaiter` to synchronizer with other nodes, wait for their reply, and
        // re-schedule execution of the header once we have all missing data.
        HeaderWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,
            parameters.sync_retry_nodes,
            /* rx_synchronizer */ rx_sync_headers,
            /* tx_core */ tx_headers_loopback,
        );

        // The `CertificateWaiter` waits to receive all the ancestors of a certificate before looping it back to the
        // `Core` for further processing.
        CertificateWaiter::spawn(
            store.clone(),
            /* rx_synchronizer */ rx_sync_certificates,
            /* tx_core */ tx_certificates_loopback,
        );

        // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
        // digests from our workers and it back to the `Core`.
        Proposer::spawn(
            name,
            &committee,
            parameters.dag_protocol,
            parameters.consensus_protocol,
            signature_service,
            parameters.header_size,
            parameters.max_header_delay,
            /* rx_core */ rx_parents,
            /* rx_workers */ rx_our_digests,
            /* tx_core */ tx_headers,
        );

        // The `Helper` is dedicated to reply to certificates requests from other primaries.
        Helper::spawn(committee.clone(), store, rx_cert_requests);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

impl Primary {
    /// Wahoo-mode entry point. Spawns the standalone `wahoo::Node` task,
    /// a network handler that forwards `PrimaryMessage::Wahoo(_)` to it,
    /// and a draining handler for worker batches (Wahoo's blocks carry
    /// synthetic txs, mirroring the Go reference's
    /// `wahoo/node.go::NewBlock` which generates txs internally).
    fn spawn_wahoo(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        _store: Store,
        tx_consensus: Sender<Certificate>,
        mut rx_consensus: Receiver<Certificate>,
    ) {
        parameters.log();
        let name = keypair.name;
        let secret = keypair.secret;
        let signature_service = SignatureService::new(secret);

        let (tx_wahoo_messages, rx_wahoo_messages) = channel::<WahooMessage>(CHANNEL_CAPACITY);
        let (tx_committed, mut rx_committed) =
            channel::<crate::wahoo::CommittedBlock>(CHANNEL_CAPACITY);
        // Same shape as the channels created in `Primary::spawn` for the
        // other three protocols. Wahoo only consumes our own batches
        // (others' batches are still drained so workers don't deadlock
        // their reliable senders, but we discard them).
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_others_digests, mut rx_others_digests) = channel(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            while rx_others_digests.recv().await.is_some() {}
        });

        // Network listener for primary-to-primary traffic.
        let mut primary_addr = committee
            .primary(&name)
            .expect("Our public key is not in the committee")
            .primary_to_primary;
        primary_addr.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            primary_addr,
            WahooReceiverHandler {
                tx_wahoo_messages,
                committee: committee.clone(),
            },
        );
        info!(
            "Wahoo primary {} listening to primary messages on {}",
            name, primary_addr
        );

        // Network listener for worker-to-primary traffic. We reuse the
        // generic `WorkerReceiverHandler` from the non-Wahoo path so the
        // same `WorkerPrimaryMessage::OurBatch(digest, wid)` envelopes
        // workers send for Narwhal/Bullshark/NovelDAG flow into Wahoo's
        // `Node` unchanged.
        let mut worker_addr = committee
            .primary(&name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        worker_addr.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            worker_addr,
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Wahoo primary {} listening to worker messages on {}",
            name, worker_addr
        );

        // Snapshot the IP before `committee` moves into WahooNode.
        let wahoo_boot_ip = committee
            .primary(&name)
            .expect("Our public key is not in the committee")
            .primary_to_primary
            .ip();

        // Spawn the Wahoo state machine.
        let node = WahooNode::new(
            name,
            committee,
            signature_service,
            parameters.batch_size,
            rx_wahoo_messages,
            rx_our_digests,
            tx_committed,
        );
        tokio::spawn(async move {
            node.run().await;
        });

        // Bridge committed Wahoo blocks → tx_consensus by synthesising an
        // empty-vote Certificate around each block's metadata. The
        // consensus layer's `wahoo::run` forwards them to `tx_output`.
        tokio::spawn(async move {
            while let Some(committed) = rx_committed.recv().await {
                let cert = wahoo_block_to_certificate(&committed.block);
                if tx_consensus.send(cert).await.is_err() {
                    break;
                }
            }
        });

        // Drain tx_primary feedback (consensus → primary). Wahoo doesn't
        // need it for GC, but the consensus task expects the channel to
        // be alive.
        tokio::spawn(async move {
            while rx_consensus.recv().await.is_some() {}
        });

        info!(
            "Wahoo primary {} successfully booted on {}",
            name, wahoo_boot_ip
        );
    }
}

/// Synthesise a `Certificate` from a committed Wahoo block.
///
/// Three fields matter downstream:
///   * `header.author` / `header.round` — feed `Header::Display` which
///     produces `B<round>(<author>)`, the prefix
///     `benchmark/benchmark/logs.py` expects on every `Committed` line.
///   * `header.payload` — copied verbatim from the Wahoo block's
///     `payload_digests`. `consensus::wahoo::run` iterates
///     `header.payload.keys()` to emit one `Committed B{r}({a}) -> {d}`
///     per worker-batch digest, exactly mirroring how
///     Narwhal/Bullshark/NovelDAG report committed payload. Each digest
///     pairs with the worker's `Batch <d> contains <n> B` log line and
///     the matching `Created` line emitted in `Node::broadcast_block`.
///   * `header.id` — refreshed via `Header::digest()` so the synthetic
///     certificate is internally consistent.
fn wahoo_block_to_certificate(block: &crate::wahoo::messages::WahooBlock) -> Certificate {
    use crypto::Hash as _;
    let mut header = Header::default();
    header.author = block.sender;
    header.round = block.round;
    header.payload = block.payload_digests.clone();
    header.id = header.digest();
    Certificate {
        header,
        votes: Vec::new(),
    }
}

#[derive(Clone)]
struct WahooReceiverHandler {
    tx_wahoo_messages: Sender<WahooMessage>,
    /// All authority public keys, indexed by `PublicKey`. Used to verify
    /// the per-message ED25519 signature, mirroring Go's
    /// `wahoo/msg_handle.go::HandleMsgLoop` which calls
    /// `verifySigED25519` against `n.publicKeyMap[sender]`.
    committee: Committee,
}

#[async_trait]
impl MessageHandler for WahooReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        match bincode::deserialize::<PrimaryMessage>(&serialized) {
            Ok(msg) => match msg {
            PrimaryMessage::Wahoo(signed) => {
                let sender = signed.msg.sender();
                if self.committee.stake(&sender) == 0 {
                    log::warn!("Wahoo: dropped message from unknown authority {}", sender);
                    return Ok(());
                }
                // Recompute the digest the sender signed: the bincode
                // encoding of the inner `WahooMessage`, hashed via the
                // shared `Hash` trait.
                let payload =
                    bincode::serialize(&signed.msg).map_err(DagError::SerializationError)?;
                let digest = wahoo_digest(&payload);
                if signed.sig.verify(&digest, &sender).is_err() {
                    log::warn!(
                        "Wahoo: signature verification failed (sender={}); dropping",
                        sender
                    );
                    return Ok(());
                }
                self.tx_wahoo_messages
                    .send(signed.msg)
                    .await
                    .expect("Wahoo channel closed");
            }
            other => log::warn!(
                "Wahoo node received non-Wahoo PrimaryMessage; dropping: {:?}",
                other
            ),
            },
            Err(e) => log::warn!("Wahoo dispatch: bincode deserialize failed: {}", e),
        }
        Ok(())
    }
}

/// Hash the bincode-encoded `WahooMessage` to produce the digest signed
/// by `Node::sign_wahoo`. Public so test code in `wahoo::node` can
/// reproduce it.
pub(crate) fn wahoo_digest(payload: &[u8]) -> Digest {
    use ed25519_dalek::{Digest as _, Sha512};
    use std::convert::TryInto;
    let mut hasher = Sha512::new();
    hasher.update(payload);
    let out = hasher.finalize();
    Digest(out[..32].try_into().expect("sha512 truncation"))
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Digest, WorkerId)>,
    tx_others_digests: Sender<(Digest, WorkerId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => self
                .tx_our_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => self
                .tx_others_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}
