// Copyright(C) Facebook, Inc. and its affiliates.
// Narwhal consensus: leader at r-2, f+1 support from r-1 children, linked-path ordering.
use crate::Consensus;
use crate::Dag;
use crate::State;
use config::{ConsensusProtocol, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::collections::{HashMap, HashSet};

pub(crate) async fn run(consensus: &mut Consensus) {
    let mut state = State::new(consensus.genesis.clone());

    while let Some(certificate) = consensus.rx_primary.recv().await {
        debug!("Processing {:?}", certificate);
        let round = certificate.round();

        // Add the new certificate to the local storage.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // Try to order the dag to commit. Start from the highest round for which we have
        // at least 2f+1 certificates. This is needed to reveal the common coin.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 4 {
            continue;
        }

        // Get the certificate's digest of the leader of round r-2.
        let leader_round = r - 2;
        let coin_round = r;
        if leader_round <= state.last_committed_round {
            continue;
        }
        let (leader_digest, leader) = match leader(consensus, leader_round, coin_round, &state.dag)
        {
            Some(x) => x,
            None => continue,
        };

        // Check if the leader has f+1 support from its children (round r-1).
        let stake: Stake = state
            .dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(&leader_digest))
            .map(|(_, x)| consensus.committee.stake(&x.origin()))
            .sum();

        if stake < consensus.committee.validity_threshold() {
            debug!("Leader {:?} does not have enough support", leader);
            continue;
        }

        // Get an ordered list of past leaders that are linked to the current leader.
        debug!("Leader {:?} has enough support", leader);
        let mut sequence = Vec::new();
        for leader in order_leaders(consensus, leader, &state).iter().rev() {
            for x in order_dag(leader, &state) {
                state.update(&x, consensus.gc_depth);
                sequence.push(x);
            }
        }

        // Log the latest committed round of every authority.
        if log_enabled!(log::Level::Debug) {
            for (name, round) in &state.last_committed {
                debug!("Latest commit of {}: Round {}", name, round);
            }
        }

        // Output the sequence in the right order.
        for certificate in sequence {
            #[cfg(not(feature = "benchmark"))]
            info!("Committed {}", certificate.header);

            #[cfg(feature = "benchmark")]
            for digest in certificate.header.payload.keys() {
                info!("Committed {} -> {:?}", certificate.header, digest);
            }

            consensus
                .tx_primary
                .send(certificate.clone())
                .await
                .expect("Failed to send certificate to primary");

            if let Err(e) = consensus.tx_output.send(certificate).await {
                warn!("Failed to output certificate: {}", e);
            }
        }
    }
}

/// Returns the certificate (and the certificate's digest) originated by the leader of the
/// specified round (if any).
fn leader<'a>(
    consensus: &Consensus,
    round: Round,
    coin_round: Round,
    dag: &'a Dag,
) -> Option<&'a (Digest, Certificate)> {
    let by_round = dag.get(&round)?;

    let leader = match consensus.consensus_protocol {
        ConsensusProtocol::RoundRobin => round_robin_leader(consensus, round),
        ConsensusProtocol::CommonCoin => {
            let coin = consensus
                .common_coin(coin_round, dag)
                .unwrap_or_else(|| consensus.round_robin_coin(round));
            // Original Narwhal: leader selected from by_round.keys() only.
            let mut keys: Vec<_> = by_round.keys().cloned().collect();
            if keys.is_empty() {
                return None;
            }
            keys.sort();
            keys[coin as usize % keys.len()]
        }
    };

    by_round.get(&leader)
}

fn round_robin_leader(consensus: &Consensus, round: Round) -> PublicKey {
    let coin = consensus.round_robin_coin(round);
    let mut keys: Vec<_> = consensus.committee.authorities.keys().cloned().collect();
    keys.sort();
    keys[coin as usize % consensus.committee.size()]
}

/// Order the past leaders that we didn't already commit.
fn order_leaders(
    consensus: &Consensus,
    leader: &Certificate,
    state: &State,
) -> Vec<Certificate> {
    let mut to_commit = vec![leader.clone()];
    let mut current = leader;
    for r in (state.last_committed_round + 2..=current.round() - 2)
        .rev()
        .step_by(2)
    {
        let (_, prev_leader) = match self::leader(consensus, r, r + 2, &state.dag) {
            Some(x) => x,
            None => continue,
        };

        if linked(current, prev_leader, &state.dag) {
            to_commit.push(prev_leader.clone());
            current = prev_leader;
        }
    }
    to_commit
}

/// Checks if there is a path between two leaders.
fn linked(leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
    let mut parents = vec![leader];
    for r in (prev_leader.round()..leader.round()).rev() {
        parents = dag
            .get(&(r))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
            .map(|(_, certificate)| certificate)
            .collect();
    }
    parents.contains(&prev_leader)
}

/// Flatten the dag referenced by the input certificate (Narwhal: parents only, == comparison).
fn order_dag(leader: &Certificate, state: &State) -> Vec<Certificate> {
    debug!("Processing sub-dag of {:?}", leader);
    let mut ordered = Vec::new();
    let mut already_ordered = HashSet::new();

    let mut buffer = vec![leader];
    while let Some(x) = buffer.pop() {
        debug!("Sequencing {:?}", x);
        ordered.push(x.clone());
        for parent in &x.header.parents {
            let (digest, certificate) = match state
                .dag
                .get(&(x.round() - 1))
                .map(|x| x.values().find(|(x, _)| x == parent))
                .flatten()
            {
                Some(x) => x,
                None => continue,
            };

            // Original Narwhal: uses == comparison (original bug preserved).
            let mut skip = already_ordered.contains(&digest);
            skip |= state
                .last_committed
                .get(&certificate.origin())
                .map_or_else(|| false, |r| r == &certificate.round());
            if !skip {
                buffer.push(certificate);
                already_ordered.insert(digest);
            }
        }
    }

    // Original Narwhal does not have the retain by gc_depth, but including it is safe.
    ordered.sort_by_key(|x| x.round());
    ordered
}
