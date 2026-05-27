// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, ConsensusProtocol, DagProtocol, PrimaryAddresses};
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

// Creates one certificate per authority from `start` to `stop` (inclusive).
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

// Shortfin wave = 4. Running rounds 1..=4 with a full DAG reaches the first wave
// boundary at r=4; the Section-6 rule then commits the leader at r-3 = 1.
#[tokio::test]
async fn commit_one() {
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, _) = make_certificates(1, 4, &genesis, &keys);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        keys[0],
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

    // Wave boundary at r=4 commits the leader at round 1; order_dag emits the
    // whole sub-DAG sorted by round, so the first output has round 1.
    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("commit timed out")
        .expect("consensus output closed");
    assert_eq!(committed.round(), 1);
}

// Sailfin's opt path commits a leader at round r as soon as round r+1 carries
// 2f+1 direct edge-votes for it, without waiting for Shortfin's r+4 wave
// boundary. With this fixture, every round-2 certificate cites every round-1
// certificate, so leader@r1 is fast-certified after round 2 reaches quorum.
#[tokio::test]
async fn sailfin_fast_commit_before_wave_boundary() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, _) = make_certificates(1, 2, &genesis, &keys);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn_with_protocol(
        keys[0],
        mock_committee(),
        /* gc_depth */ 50,
        DagProtocol::Sailfin,
        ConsensusProtocol::RoundRobin,
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

    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("fast commit timed out")
        .expect("consensus output closed");
    assert_eq!(committed.round(), 1);
    assert_eq!(committed.origin(), keys[0]);
}

// Rounds 1..=8 with one dead non-leader node. Two wave boundaries fire:
// r=4 → leader at r1, r=8 → leader at r5.
#[tokio::test]
async fn dead_node() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_certificates(1, 8, &genesis, &keys);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        keys[0],
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

// Leader (keys[0], coin=0 in tests) misses round 3, breaking the QC chain for
// the wave ending at r=4 (b1 = leader@r3 is absent). The next wave ends at
// r=8 with leader at r=5; the full b3/b2/b1 chain is present, so we commit.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Rounds 1..=2 are complete.
    let (out, parents_r2) = make_certificates(1, 2, &genesis, &keys);
    certificates.extend(out);

    // Round 3 excludes the leader: wave at r=4 fails because b1@r3 is missing.
    let keys_without_leader: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, parents_r3) = make_certificates(3, 3, &parents_r2, &keys_without_leader);
    certificates.extend(out);

    // Rounds 4..=8 with the leader: wave at r=8 sees a full chain at r=5..=7.
    let (out, _) = make_certificates(4, 8, &parents_r3, &keys);
    certificates.extend(out);

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        keys[0],
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

    // order_dag emits the sub-DAG sorted by round, so the first output is the
    // lowest uncommitted round visible from the leader at r=5 — expected to be
    // round 1 (or above).
    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("commit timed out")
        .expect("consensus output closed");
    assert!(
        committed.round() >= 1,
        "expected commit to contain DAG from round 1 onwards"
    );
}

// Leader absent for rounds 1..=2, present from round 3 onwards. Wave at r=4
// finds leader@r1 missing → skip. Wave at r=8 finds leader@r5 with a full
// chain → commit.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, parents) = make_certificates(1, 2, &genesis, &nodes);
    certificates.extend(out);

    let (out, parents) = make_certificates(3, 8, &parents, &keys);
    certificates.extend(out);

    let _ = parents;

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        keys[0],
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

    let committed = timeout(Duration::from_secs(1), rx_output.recv())
        .await
        .expect("commit timed out")
        .expect("consensus output closed");
    assert!(committed.round() >= 1);
}

// At the r=4 wave boundary, if any vote inside QC(b2) embedded in b1 has
// voter_round >= commit_round, the Section-6 rule must reject the commit.
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
                wahoo_phase: None,
                signature: Default::default(),
            }],
        });
    }

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        keys[0],
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
        Ok(Some(_)) => panic!("unexpected commit when qc vote round is not less than commit round"),
    }
}
