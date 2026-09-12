// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use config::{ConsensusProtocol, DagProtocol};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        DagProtocol::Narwhal,
        ConsensusProtocol::RoundRobin,
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee(), DagProtocol::Narwhal).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        DagProtocol::Narwhal,
        ConsensusProtocol::RoundRobin,
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee(), DagProtocol::Narwhal).is_ok());
}

#[tokio::test]
async fn shortfin_only_carries_shares_at_common_coin_wave_boundaries() {
    for protocol in [
        ConsensusProtocol::RoundRobin,
        ConsensusProtocol::PseudoRandom,
        ConsensusProtocol::CommonCoin,
    ] {
        let (name, secret) = keys().pop().unwrap();
        let signature_service = SignatureService::new(secret);
        let (tx_parents, rx_parents) = channel(1);
        let (_tx_our_digests, rx_our_digests) = channel(1);
        let (tx_headers, mut rx_headers) = channel(1);

        Proposer::spawn(
            name,
            &committee(),
            DagProtocol::Shortfin,
            protocol,
            signature_service,
            1_000,
            20,
            rx_parents,
            rx_our_digests,
            tx_headers,
        );

        let header = rx_headers.recv().await.unwrap();
        assert_eq!(header.round, 1);
        assert!(header.coin_share.is_empty());

        tx_parents
            .send(ProposerSignal {
                round: 4,
                parents_1: vec![Digest::default()],
                parents_2: Vec::new(),
                qc: Some(EmbeddedQc::default()),
                certificates_1: Vec::new(),
            })
            .await
            .unwrap();
        let header = rx_headers.recv().await.unwrap();
        assert_eq!(header.round, 4);
        assert_eq!(
            !header.coin_share.is_empty(),
            matches!(protocol, ConsensusProtocol::CommonCoin)
        );
    }
}

#[test]
fn coin_share_rounds_match_each_protocol_schedule() {
    assert!(!Proposer::carries_coin_share(DagProtocol::Shortfin, 3));
    assert!(Proposer::carries_coin_share(DagProtocol::Shortfin, 4));
    assert!(!Proposer::carries_coin_share(DagProtocol::Narwhal, 3));
    assert!(!Proposer::carries_coin_share(DagProtocol::Narwhal, 2));
    assert!(Proposer::carries_coin_share(DagProtocol::Narwhal, 4));
    assert!(!Proposer::carries_coin_share(DagProtocol::MahiMahi, 4));
    assert!(Proposer::carries_coin_share(DagProtocol::MahiMahi, 5));
    assert!(!Proposer::carries_coin_share(DagProtocol::MahiMahi4, 3));
    assert!(Proposer::carries_coin_share(DagProtocol::MahiMahi4, 4));
    assert!(!Proposer::carries_coin_share(DagProtocol::Wahoo, 4));
}
