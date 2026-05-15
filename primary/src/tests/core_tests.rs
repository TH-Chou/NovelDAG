// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::DagProtocol;
use crate::common::{
    certificate, committee, committee_with_base_port, header, headers, keys, listener, votes,
};
use crate::proposer::ProposerSignal;
use crypto::Signature;
use std::collections::BTreeSet;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn process_header() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, secret) = keys.pop().unwrap();
    let mut signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the vote we expect to receive.
    let expected = Vote::new(&header(), header().round, &name, &mut signature_service).await;

    // Spawn a listener to receive the vote.
    let address = committee
        .primary(&header().author)
        .unwrap()
        .primary_to_primary;
    let handle = listener(address);

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::NovelDAG,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote.
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    // Ensure the header is correctly stored.
    let stored = store
        .read(header().id.to_vec())
        .await
        .unwrap()
        .map(|x| bincode::deserialize(&x).unwrap());
    assert_eq!(stored, Some(header()));
}

#[tokio::test]
async fn process_header_missing_parent() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_parent";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        parents: [Digest::default()].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_missing_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        payload: [(Digest::default(), 0)].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_votes() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_100);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::NovelDAG,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Set a valid current header (as if it was produced by our proposer).
    let proposed_header = header();
    _tx_headers.send(proposed_header.clone()).await.unwrap();

    // Wait for the injected header to be processed before opening listeners.
    loop {
        if store
            .read(proposed_header.id.to_vec())
            .await
            .unwrap()
            .is_some()
        {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Make the certificate we expect to receive.
    let expected = certificate(&proposed_header);

    // Send votes on the current header to the core.
    for vote in votes(&proposed_header) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    // Ensure the core produced and forwarded the expected certificate.
    let received = rx_consensus.recv().await.unwrap();
    assert_eq!(received, expected);
}

#[tokio::test]
async fn process_certificates() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(3);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(3);
    let (tx_parents, mut rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_certificates";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a quorum of certificates that includes our own authority, otherwise
    // `try_signal_proposer` cannot produce a QC for the next round.
    let round_1_headers = headers();
    let own_header = round_1_headers
        .iter()
        .find(|header| header.author == name)
        .cloned()
        .unwrap();
    let mut selected_headers: Vec<_> = vec![own_header.clone()];
    selected_headers.extend(
        round_1_headers
            .iter()
            .filter(|header| header.author != name)
            .take(2)
            .cloned(),
    );
    let certificates: Vec<_> = selected_headers
        .iter()
        .map(|header| {
            Certificate {
                header: header.clone(),
                votes: votes(header),
            }
        })
        .collect();
    let own_certificate = certificates
        .iter()
        .find(|certificate| certificate.origin() == name)
        .cloned()
        .unwrap();

    for x in certificates.clone() {
        tx_primary_messages
            .send(PrimaryMessage::Certificate(x))
            .await
            .unwrap();
    }

    // Ensure the core sends the parents of the certificates to the proposer.
    let received = timeout(Duration::from_secs(1), rx_parents.recv())
        .await
        .expect("timed out waiting for proposer signal")
        .unwrap();
    let parents_1 = certificates.iter().map(|x| x.digest()).collect();
    let expected = ProposerSignal {
        round: 2,
        parents_1,
        parents_2: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
        qc: Some(crate::messages::EmbeddedQc {
            target: own_certificate.header.id,
            round: 1,
            votes: own_certificate.votes,
        }),
        certificates_1: Vec::new(),
    };
    assert_eq!(received.round, expected.round);
    assert_eq!(received.parents_1.len(), expected.parents_1.len());
    assert_eq!(received.parents_2.len(), expected.parents_2.len());
    assert!(received.qc.is_some());

    // Ensure the core sends the certificates to the consensus.
    for x in certificates.clone() {
        let received = timeout(Duration::from_secs(1), rx_consensus.recv())
            .await
            .expect("timed out waiting for consensus certificate")
            .unwrap();
        assert_eq!(received, x);
    }

    // Note: in NovelDAG mode certificates are no longer persisted to the
    // store (they use an in-memory cache instead), so we skip the DB
    // assertion here.
}

#[tokio::test]
async fn process_header_round_2_requires_qc() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_process_header_round_2_requires_qc";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let mut header = Header {
        round: 2,
        parents: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
        parents_2: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
        qc: None,
        ..header()
    };

    let (_, author_secret) = keys()
        .into_iter()
        .find(|(pk, _)| *pk == header.author)
        .unwrap();
    header.id = header.digest();
    header.signature = Signature::new(&header.id, &author_secret);
    let id = header.id.clone();

    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_round_2_rejects_qc_target_outside_parents_1() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_process_header_round_2_rejects_qc_target";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let round_1_headers = headers();
    let round_1_certificates: Vec<_> = round_1_headers.iter().map(certificate).collect();
    for certificate in &round_1_certificates {
        let bytes = bincode::serialize(certificate).unwrap();
        store.write(certificate.digest().to_vec(), bytes).await;
    }

    let qc_source = round_1_certificates[0].clone();
    let parents_1: BTreeSet<_> = round_1_certificates
        .iter()
        .skip(1)
        .map(|x| x.digest())
        .collect();
    let parents_2: BTreeSet<_> = Certificate::genesis(&committee())
        .iter()
        .map(|x| x.digest())
        .collect();

    let mut header = Header {
        author: qc_source.header.author,
        round: 2,
        parents: parents_1,
        parents_2,
        qc: Some(crate::messages::EmbeddedQc {
            target: qc_source.header.id.clone(),
            round: 1,
            votes: qc_source.votes.clone(),
        }),
        ..Header::default()
    };

    let (_, author_secret) = keys()
        .into_iter()
        .find(|(pk, _)| *pk == header.author)
        .unwrap();
    header.id = header.digest();
    header.signature = Signature::new(&header.id, &author_secret);
    let id = header.id.clone();

    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn vote_new_sets_voter_round() {
    let (_, secret) = keys().pop().unwrap();
    let mut signature_service = SignatureService::new(secret);
    let voter = keys().pop().unwrap().0;

    let vote = Vote::new(&header(), 7, &voter, &mut signature_service).await;
    assert_eq!(vote.round, header().round);
    assert_eq!(vote.voter_round, 7);
}

#[tokio::test]
async fn process_header_round_2_rejects_qc_vote_with_wrong_target() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_process_header_round_2_rejects_qc_vote_target";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let round_1_headers = headers();
    let round_1_certificates: Vec<_> = round_1_headers.iter().map(certificate).collect();
    for certificate in &round_1_certificates {
        let bytes = bincode::serialize(certificate).unwrap();
        store.write(certificate.digest().to_vec(), bytes).await;
    }

    let qc_source = round_1_certificates[0].clone();
    let parents_1: BTreeSet<_> = round_1_certificates.iter().map(|x| x.digest()).collect();
    let parents_2: BTreeSet<_> = Certificate::genesis(&committee())
        .iter()
        .map(|x| x.digest())
        .collect();

    let mut bad_votes = qc_source.votes.clone();
    let first_vote = bad_votes.get_mut(0).unwrap();
    first_vote.id = Digest::default();
    let (_, voter_secret) = keys()
        .into_iter()
        .find(|(pk, _)| *pk == first_vote.author)
        .unwrap();
    first_vote.signature = Signature::new(&first_vote.digest(), &voter_secret);

    let mut header = Header {
        author: qc_source.header.author,
        round: 2,
        parents: parents_1,
        parents_2,
        qc: Some(crate::messages::EmbeddedQc {
            target: qc_source.header.id.clone(),
            round: 1,
            votes: bad_votes,
        }),
        ..Header::default()
    };

    let (_, author_secret) = keys()
        .into_iter()
        .find(|(pk, _)| *pk == header.author)
        .unwrap();
    header.id = header.digest();
    header.signature = Signature::new(&header.id, &author_secret);
    let id = header.id.clone();

    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_certificate_rejects_vote_origin_mismatch() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_process_certificate_rejects_vote_origin_mismatch";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::NovelDAG,
        store.clone(),
        synchronizer,
        signature_service,
        Arc::new(AtomicU64::new(0)),
        50,
        rx_primary_messages,
        rx_headers_loopback,
        rx_certificates_loopback,
        rx_headers,
        tx_consensus,
        tx_parents,
    );

    let base_header = header();
    let mut bad_certificate = certificate(&base_header);
    let forged_origin = keys()[0].0;

    let first_vote = bad_certificate.votes.get_mut(0).unwrap();
    first_vote.origin = forged_origin;
    let (_, voter_secret) = keys()
        .into_iter()
        .find(|(pk, _)| *pk == first_vote.author)
        .unwrap();
    first_vote.signature = Signature::new(&first_vote.digest(), &voter_secret);

    let digest = bad_certificate.digest();
    tx_primary_messages
        .send(PrimaryMessage::Certificate(bad_certificate))
        .await
        .unwrap();

    assert!(timeout(Duration::from_millis(300), rx_consensus.recv())
        .await
        .is_err());
    assert!(store.read(digest.to_vec()).await.unwrap().is_none());
}
