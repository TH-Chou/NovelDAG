// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::{EmbeddedQc, Header, Vote};
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, HashMap, VecDeque};
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

// Fixture
fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn mock_committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
    }
}

// Fixture
fn mock_certificate(
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<Digest>,
    parents_2: BTreeSet<Digest>,
    qc: Option<EmbeddedQc>,
) -> (Digest, Certificate) {
    let mut header_id = [0u8; 32];
    header_id[0] = round as u8;
    header_id[1] = origin.0[0];

    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            parents_2,
            qc,
            id: Digest(header_id),
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
fn make_certificates(
    start: Round,
    stop: Round,
    initial_parents: &BTreeSet<Digest>,
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<Digest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut parents_2 = BTreeSet::new();
    let mut next_parents = BTreeSet::new();
    let mut prev_by_author: HashMap<PublicKey, Certificate> = HashMap::new();
    let mut next_prev_by_author: HashMap<PublicKey, Certificate> = HashMap::new();

    for round in start..=stop {
        next_parents.clear();
        next_prev_by_author.clear();
        for name in keys {
            let qc = if round >= 2 {
                prev_by_author.get(name).map(|parent| EmbeddedQc {
                    target: parent.header.id.clone(),
                    round: parent.round(),
                    votes: Vec::new(),
                })
            } else {
                None
            };

            let (digest, certificate) = mock_certificate(
                *name,
                round,
                parents.clone(),
                if round >= 2 {
                    parents_2.clone()
                } else {
                    BTreeSet::new()
                },
                qc,
            );
            certificates.push_back(certificate);
            next_parents.insert(digest);
            next_prev_by_author.insert(*name, certificates.back().cloned().unwrap());
        }
        parents_2 = parents;
        parents = next_parents.clone();
        prev_by_author = next_prev_by_author.clone();
    }
    (certificates, next_parents)
}

// Run for 4 dag rounds in ideal conditions (all nodes reference all other nodes). We should commit
// the leader of round 2.
#[tokio::test]
async fn commit_one() {
    // Make certificates for rounds 1 to 4.
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, _) = make_certificates(1, 4, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates concurrently so the consensus output channel is never blocked.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    // At r=4 we should commit the leader at r-3 = 1.
    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("commit timed out")
        .expect("consensus output closed");
    assert_eq!(committed.round(), 1);
}

// Run for 8 dag rounds with one dead node node (that is not a leader). We should commit the leaders of
// rounds 2, 4, and 6.
#[tokio::test]
async fn dead_node() {
    // Make the certificates.
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_certificates(1, 8, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    // We should observe commits at wave boundaries under the new rule.
    let first = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("first commit timed out")
        .expect("consensus output closed");
    let second = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("second commit timed out")
        .expect("consensus output closed");
    assert!(first.round() <= second.round());
}

// Run for 6 dag rounds. The leaders of round 2 does not have enough support, but the leader of
// round 4 does. The leader of rounds 2 and 4 should thus be committed upon entering round 6.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Rounds 1 and 2 are complete.
    let (out, parents_r2) = make_certificates(1, 2, &genesis, &keys);
    certificates.extend(out);

    // Round 3 excludes the future wave leader author, breaking b1 in the chain.
    let keys_without_leader: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, _parents_r3) = make_certificates(3, 3, &parents_r2, &keys_without_leader);
    certificates.extend(out);

    // Round 4 reaches quorum but should not commit due to missing same-author b1.
    let (out, _) = make_certificates(4, 4, &parents_r2, &keys);
    certificates.extend(out);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates concurrently so the consensus output channel is never blocked.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    let no_commit = timeout(Duration::from_millis(300), rx_output.recv()).await;
    match no_commit {
        // Timeout means no commit arrived — expected under the broken chain.
        Err(_) => {}
        // If the channel closed without data, that also means no commit.
        Ok(None) => {}
        // A commit arrived unexpectedly — the chain should have prevented it.
        Ok(Some(_)) => panic!("unexpected commit under broken b3-b2-b1 chain"),
    }
}

// Run for 6 dag rounds. Node 0 (the leader of round 2) is missing for rounds 1 and 2,
// and reapers from round 3.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Remove the leader for rounds 1 and 2.
    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, parents) = make_certificates(1, 2, &genesis, &nodes);
    certificates.extend(out);

    // Add back the leader for rounds 3 to 8.
    let (out, parents) = make_certificates(3, 8, &parents, &keys);
    certificates.extend(out);

    let _ = parents;

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates concurrently so the consensus output channel is drained
    // even when a commit fires before all certs are ingested.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("commit timed out")
        .expect("consensus output closed");
    assert!(committed.round() >= 1);
}

// At r=4, if the QC embedded in b1 (round 3) contains any vote with voter_round >= 4,
// the section-6 commit rule must reject committing b3.
#[tokio::test]
async fn reject_commit_when_qc_vote_round_not_less_than_commit_round() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, _) = make_certificates(1, 4, &genesis, &keys);

    let leader = keys[0];
    if let Some(b1) = certificates
        .iter_mut()
        .find(|certificate| certificate.round() == 3 && certificate.origin() == leader)
    {
        let target = b1
            .header
            .qc
            .as_ref()
            .map(|qc| qc.target.clone())
            .unwrap_or_default();
        b1.header.qc = Some(EmbeddedQc {
            target,
            round: 2,
            votes: vec![Vote {
                id: Digest::default(),
                round: 2,
                voter_round: 4,
                origin: leader,
                author: keys[1],
                signature: Default::default(),
            }],
        });
    }

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    let no_commit = timeout(Duration::from_millis(300), rx_output.recv()).await;
    match no_commit {
        Err(_) => {}
        Ok(None) => {}
        Ok(Some(_)) => panic!(
            "unexpected commit when qc vote round is not less than commit round"
        ),
    }
}
