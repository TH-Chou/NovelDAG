// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    certificate, committee, committee_with_base_port, header, headers, keys, listener,
    vote_listener, votes,
};
use crate::header_waiter::WaiterMessage;
use crate::proposer::ProposerSignal;
use config::DagProtocol;
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
        DagProtocol::Shortfin,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        DagProtocol::Shortfin,
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
        DagProtocol::Shortfin,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
    let mut all_keys = keys();
    let (name, secret) = all_keys.pop().unwrap();
    let (author, author_secret) = all_keys.pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_080);

    let (tx_sync_headers, mut rx_sync_headers) = channel(2);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(2);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Shortfin,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        DagProtocol::Shortfin,
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

    let batch = Digest([7; 32]);
    let unsigned = Header {
        author,
        round: 1,
        payload: [(batch.clone(), 0)].iter().cloned().collect(),
        parents: Certificate::genesis(&committee)
            .iter()
            .map(|certificate| certificate.digest())
            .collect(),
        ..Header::default()
    };
    let header = Header {
        id: unsigned.digest(),
        signature: Signature::new(&unsigned.digest(), &author_secret),
        ..unsigned
    };
    let id = header.id.clone();
    let address = committee.primary(&author).unwrap().primary_to_primary;
    let mut vote_handle = vote_listener(address);
    tx_primary_messages
        .send(PrimaryMessage::Header(header.clone()))
        .await
        .unwrap();

    let request = timeout(Duration::from_secs(1), rx_sync_headers.recv())
        .await
        .expect("payload synchronization was not requested")
        .unwrap();
    match request {
        WaiterMessage::SyncBatches {
            missing,
            source,
            deliver,
        } => {
            assert_eq!(missing.get(&batch), Some(&0));
            assert_eq!(source, author);
            assert_eq!(deliver.id, id);
        }
        message => panic!("Unexpected waiter message: {:?}", message),
    }

    let bubble = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("bubble was not admitted to the structural DAG")
        .unwrap();
    assert_eq!(bubble.header.id, id);
    assert!(bubble.votes.is_empty());
    assert!(store.read(id.to_vec()).await.unwrap().is_some());
    assert!(timeout(Duration::from_millis(100), &mut vote_handle)
        .await
        .is_err());

    let key = [batch.as_ref(), &0u32.to_le_bytes()].concat();
    store.write(key, Vec::new()).await;
    tx_headers_loopback.send(header).await.unwrap();
    let vote = timeout(Duration::from_secs(1), &mut vote_handle)
        .await
        .expect("payload-complete block did not receive a vote")
        .unwrap();
    assert_eq!(vote.id, id);
}

#[tokio::test]
async fn shortfin_qc_carrier_waits_for_target_payload() {
    let mut all_keys = keys();
    let (name, secret) = all_keys.pop().unwrap();
    let target_author = all_keys[0].0;
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_090);

    let (tx_sync_headers, mut rx_sync_headers) = channel(8);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(16);
    let (tx_headers_loopback, rx_headers_loopback) = channel(8);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(16);
    let (tx_parents, _rx_parents) = channel(8);

    let path = ".db_test_shortfin_qc_target_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );
    Core::spawn(
        name,
        committee.clone(),
        DagProtocol::Shortfin,
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

    let missing_batch = Digest([9; 32]);
    let mut round_1 = Vec::new();
    for (author, author_secret) in all_keys.iter() {
        let unsigned = Header {
            author: *author,
            round: 1,
            payload: if *author == target_author {
                [(missing_batch.clone(), 0)].iter().cloned().collect()
            } else {
                Default::default()
            },
            parents: Certificate::genesis(&committee)
                .iter()
                .map(|certificate| certificate.digest())
                .collect(),
            ..Header::default()
        };
        round_1.push(Header {
            id: unsigned.digest(),
            signature: Signature::new(&unsigned.digest(), author_secret),
            ..unsigned
        });
    }

    for header in &round_1 {
        tx_primary_messages
            .send(PrimaryMessage::Header(header.clone()))
            .await
            .unwrap();
    }
    for _ in 0..round_1.len() {
        timeout(Duration::from_secs(1), rx_consensus.recv())
            .await
            .expect("round-1 structural record was not admitted")
            .unwrap();
    }
    let initial_sync = timeout(Duration::from_secs(1), rx_sync_headers.recv())
        .await
        .expect("target bubble did not request its payload")
        .unwrap();
    assert!(matches!(
        initial_sync,
        WaiterMessage::SyncBatches { ref deliver, .. } if deliver.id == round_1[0].id
    ));

    let target = round_1
        .iter()
        .find(|header| header.author == target_author)
        .cloned()
        .unwrap();
    let referenced_authors: HashSet<_> = round_1.iter().map(|header| header.author).collect();
    let qc_votes = votes(&target)
        .into_iter()
        .filter(|vote| referenced_authors.contains(&vote.author))
        .collect::<Vec<_>>();
    let unsigned_carrier = Header {
        author: target_author,
        round: 2,
        parents: round_1
            .iter()
            .map(|header| Certificate {
                header: header.clone(),
                votes: Vec::new(),
            })
            .map(|certificate| certificate.digest())
            .collect(),
        qc: Some(crate::messages::EmbeddedQc {
            target: target.id.clone(),
            round: target.round,
            votes: qc_votes.clone(),
        }),
        ..Header::default()
    };
    let carrier = Header {
        id: unsigned_carrier.digest(),
        signature: Signature::new(&unsigned_carrier.digest(), &all_keys[0].1),
        ..unsigned_carrier
    };
    tx_primary_messages
        .send(PrimaryMessage::Header(carrier.clone()))
        .await
        .unwrap();

    let carrier_sync = timeout(Duration::from_secs(1), rx_sync_headers.recv())
        .await
        .expect("QC carrier did not wait for its target payload")
        .unwrap();
    assert!(matches!(
        carrier_sync,
        WaiterMessage::SyncBatches { ref deliver, .. } if deliver.id == carrier.id
    ));
    assert!(timeout(Duration::from_millis(100), rx_consensus.recv())
        .await
        .is_err());

    let key = [missing_batch.as_ref(), &0u32.to_le_bytes()].concat();
    store.write(key, Vec::new()).await;
    tx_headers_loopback.send(target.clone()).await.unwrap();
    tx_headers_loopback.send(carrier.clone()).await.unwrap();

    let mut target_certified = false;
    let mut carrier_admitted = false;
    for _ in 0..2 {
        let update = timeout(Duration::from_secs(1), rx_consensus.recv())
            .await
            .expect("missing Shortfin bubble state update")
            .unwrap();
        if update.header.id == target.id {
            target_certified = update.votes == qc_votes;
        }
        if update.header.id == carrier.id {
            carrier_admitted = update.votes.is_empty();
        }
    }
    assert!(target_certified);
    assert!(carrier_admitted);
}

#[tokio::test]
async fn process_votes() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_100);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(8);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(8);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Shortfin,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        DagProtocol::Shortfin,
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

    // Shortfin delivers the signed header to consensus immediately as an
    // uncertified DAG record. Drain it before the later local QC uses the
    // single-slot test channel.
    let synthetic = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("timed out waiting for synthetic certificate")
        .unwrap();
    assert_eq!(synthetic.header, proposed_header);
    assert!(synthetic.votes.is_empty());

    // A Shortfin vote is only countable after the proposer's DAG contains
    // the voter's own block at voter_round. Populate those round-1 contexts.
    let peer_headers: Vec<_> = headers()
        .into_iter()
        .filter(|peer_header| peer_header.author != name)
        .collect();
    for peer_header in &peer_headers {
        tx_primary_messages
            .send(PrimaryMessage::Header(peer_header.clone()))
            .await
            .unwrap();
        let peer_certificate = Certificate {
            header: peer_header.clone(),
            votes: Vec::new(),
        };
        while store
            .read(peer_certificate.digest().to_vec())
            .await
            .unwrap()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    }
    for _ in &peer_headers {
        let peer_synthetic = timeout(Duration::from_secs(1), rx_consensus.recv())
            .await
            .expect("timed out waiting for peer synthetic certificate")
            .unwrap();
        assert!(peer_synthetic.votes.is_empty());
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
async fn shortfin_vote_waits_for_voter_round_block() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_180);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(8);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(8);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_shortfin_vote_waits_for_voter_block";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );
    Core::spawn(
        name,
        committee,
        DagProtocol::Shortfin,
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

    let proposed_header = header();
    tx_headers.send(proposed_header.clone()).await.unwrap();
    let own_synthetic = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("timed out waiting for own synthetic certificate")
        .unwrap();
    assert_eq!(own_synthetic.header, proposed_header);

    let mut peer_headers = headers()
        .into_iter()
        .filter(|peer_header| peer_header.author != name);
    let available_voter_block = peer_headers.next().unwrap();
    let delayed_voter_block = peer_headers.next().unwrap();

    tx_primary_messages
        .send(PrimaryMessage::Header(available_voter_block.clone()))
        .await
        .unwrap();
    let available_synthetic = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("timed out waiting for available voter block")
        .unwrap();
    assert_eq!(available_synthetic.header, available_voter_block);

    let target_votes = votes(&proposed_header);
    for voter in [available_voter_block.author, delayed_voter_block.author] {
        let vote = target_votes
            .iter()
            .find(|vote| vote.author == voter)
            .cloned()
            .unwrap();
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    assert!(timeout(Duration::from_millis(100), rx_consensus.recv())
        .await
        .is_err());

    tx_primary_messages
        .send(PrimaryMessage::Header(delayed_voter_block))
        .await
        .unwrap();
    let certificate = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("cached vote was not retried after its voter block arrived")
        .unwrap();
    assert_eq!(certificate.header, proposed_header);
    assert_eq!(
        certificate.votes.len(),
        committee_with_base_port(13_180).quorum_threshold() as usize
    );
}

#[tokio::test]
async fn narwhal_votes_skip_shortfin_predecessor_gate() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_150);
    let quorum_threshold = committee.quorum_threshold() as usize;

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(4);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_narwhal_vote_gate";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Narwhal,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee,
        DagProtocol::Narwhal,
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

    let proposed_header = header();
    tx_headers.send(proposed_header.clone()).await.unwrap();
    while store
        .read(proposed_header.id.to_vec())
        .await
        .unwrap()
        .is_none()
    {
        tokio::task::yield_now().await;
    }

    // A Narwhal vote must not wait on Shortfin's voter-round predecessor
    // index. Use a deliberately higher voter round so the regression would
    // leave these votes pending forever.
    let expected_votes: Vec<_> = keys()
        .into_iter()
        .map(|(author, secret)| {
            let vote = Vote {
                id: proposed_header.id.clone(),
                round: proposed_header.round,
                voter_round: proposed_header.round + 1,
                origin: proposed_header.author,
                author,
                wahoo_phase: None,
                signature: Signature::default(),
            };
            Vote {
                signature: Signature::new(&vote.digest(), &secret),
                ..vote
            }
        })
        .collect();

    for vote in &expected_votes {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote.clone()))
            .await
            .unwrap();
    }

    let received = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("Narwhal votes were incorrectly held by the Shortfin gate")
        .unwrap();
    assert_eq!(received.header, proposed_header);
    assert_eq!(received.votes.len(), quorum_threshold);
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
        DagProtocol::Shortfin,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
        .map(|header| Certificate {
            header: header.clone(),
            votes: votes(header),
        })
        .collect();
    let own_certificate = certificates
        .iter()
        .find(|certificate| certificate.origin() == name)
        .cloned()
        .unwrap();

    // Register the local header through the proposer path before injecting
    // its assembled certificate. In production, an own certificate can only
    // exist after this step.
    _tx_headers.send(own_header.clone()).await.unwrap();
    let synthetic = timeout(Duration::from_secs(1), rx_consensus.recv())
        .await
        .expect("timed out waiting for own synthetic certificate")
        .unwrap();
    assert_eq!(synthetic.header, own_header);
    assert!(synthetic.votes.is_empty());

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
    let parents_1 = Certificate::genesis(&committee())
        .into_iter()
        .map(|genesis| {
            certificates
                .iter()
                .find(|certificate| certificate.origin() == genesis.origin())
                .map(|certificate| certificate.digest())
                .unwrap_or_else(|| genesis.digest())
        })
        .collect();
    let expected = ProposerSignal {
        round: 2,
        parents_1,
        parents_2: Vec::new(),
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

    // Once the signal has advanced this node to round 2, a late round-1
    // header must receive a vote carrying the voter's true local round.
    let late_header = round_1_headers
        .iter()
        .find(|header| {
            !selected_headers
                .iter()
                .any(|selected| selected.author == header.author)
        })
        .cloned()
        .unwrap();
    let address = committee()
        .primary(&late_header.author)
        .unwrap()
        .primary_to_primary;
    let handle = vote_listener(address);
    tx_primary_messages
        .send(PrimaryMessage::Header(late_header.clone()))
        .await
        .unwrap();
    let vote = handle.await.unwrap();
    assert_eq!(vote.round, late_header.round);
    assert_eq!(vote.voter_round, 2);

    // Note: in Shortfin mode certificates are no longer persisted to the
    // store (they use an in-memory cache instead), so we skip the DB
    // assertion here.
}

#[tokio::test]
async fn shortfin_accepts_ref_above_header_round() {
    let mut all_keys = keys();
    let (name, secret) = all_keys.pop().unwrap();
    let (carrier_author, carrier_secret) = all_keys.pop().unwrap();
    let (high_author, high_secret) = all_keys.pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(13_250);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(2);
    let (tx_parents, _rx_parents) = channel(1);

    let path = ".db_test_shortfin_high_ref";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let round_1 = headers()
        .into_iter()
        .find(|header| header.author == high_author)
        .unwrap();
    let mut high_header = Header {
        author: high_author,
        round: 2,
        parents: Certificate::genesis(&committee)
            .iter()
            .map(|certificate| certificate.digest())
            .collect(),
        qc: Some(crate::messages::EmbeddedQc {
            target: round_1.id.clone(),
            round: 1,
            votes: votes(&round_1),
        }),
        ..Header::default()
    };
    high_header.id = high_header.digest();
    high_header.signature = Signature::new(&high_header.id, &high_secret);
    let high_certificate = Certificate {
        header: high_header.clone(),
        votes: Vec::new(),
    };
    store
        .write(
            high_certificate.digest().to_vec(),
            bincode::serialize(&high_certificate).unwrap(),
        )
        .await;

    let mut carrier = Header {
        author: carrier_author,
        round: 1,
        parents: [high_certificate.digest()].iter().cloned().collect(),
        ..Header::default()
    };
    carrier.id = carrier.digest();
    carrier.signature = Signature::new(&carrier.id, &carrier_secret);

    let address = committee
        .primary(&carrier.author)
        .unwrap()
        .primary_to_primary;
    let handle = listener(address);
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );
    Core::spawn(
        name,
        committee,
        DagProtocol::Shortfin,
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

    tx_primary_messages
        .send(PrimaryMessage::Header(carrier.clone()))
        .await
        .unwrap();
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        PrimaryMessage::Vote(vote) => {
            assert_eq!(vote.id, carrier.id);
            assert_eq!(vote.voter_round, 1);
        }
        message => panic!("Unexpected message: {:?}", message),
    }
    assert!(high_header.round > carrier.round);
    assert!(store.read(carrier.id.to_vec()).await.unwrap().is_some());
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
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
        DagProtocol::Shortfin,
        store.clone(),
        tx_sync_headers,
        tx_sync_certificates,
    );

    Core::spawn(
        name,
        committee(),
        DagProtocol::Shortfin,
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
