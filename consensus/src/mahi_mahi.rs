// Experimental Mahi-Mahi-style committer over NovelDAG's uncertified DAG substrate.
use crate::Consensus;
use crate::Dag;
use crate::State;
use config::Stake;
use crypto::{Hash as _, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::collections::{HashMap, HashSet};

const LEADER_COUNT: usize = 2;

pub(crate) async fn run(consensus: &mut Consensus) {
    let mut state = State::new(consensus.genesis.clone());
    let mut decided = HashSet::new();
    let wave_length = consensus.dag_protocol.mahi_mahi_wave_length().unwrap_or(5);

    while let Some(certificate) = consensus.rx_primary.recv().await {
        debug!("Processing {:?}", certificate);
        let round = certificate.round();

        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        if round < wave_length - 1 {
            continue;
        }

        let mut sequence = Vec::new();
        let min_leader_round = state.last_committed_round.saturating_sub(wave_length);
        let max_leader_round = round - (wave_length - 1);
        for leader_round in min_leader_round..=max_leader_round {
            if leader_round == 0 {
                continue;
            }
            for leader_index in 0..LEADER_COUNT {
                let Some(leader_key) = leader_for(
                    consensus,
                    leader_round,
                    wave_length,
                    leader_index,
                    &state.dag,
                ) else {
                    continue;
                };
                let Some((_, leader)) = state
                    .dag
                    .get(&leader_round)
                    .and_then(|by_author| by_author.get(&leader_key))
                else {
                    continue;
                };
                if !decided.insert((leader_round, leader_key)) {
                    continue;
                }

                if !enough_leader_support(consensus, leader, &state.dag, wave_length) {
                    decided.remove(&(leader_round, leader_key));
                    continue;
                }

                debug!("Mahi-Mahi leader {:?} has enough support", leader);
                for x in order_dag(leader, &state) {
                    state.update(&x, consensus.gc_depth);
                    sequence.push(x);
                }
            }
        }

        if log_enabled!(log::Level::Debug) {
            for (name, round) in &state.last_committed {
                debug!("Latest commit of {}: Round {}", name, round);
            }
        }

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

fn leader_for(
    consensus: &Consensus,
    round: Round,
    wave_length: Round,
    leader_index: usize,
    dag: &Dag,
) -> Option<PublicKey> {
    let coin_round = round + wave_length - 1;
    consensus.leader_authority(round, coin_round, leader_index, dag)
}

fn enough_leader_support(
    consensus: &Consensus,
    leader: &Certificate,
    dag: &Dag,
    wave_length: Round,
) -> bool {
    let decision_round = leader.round() + wave_length - 1;
    let Some(decision_blocks) = dag.get(&decision_round) else {
        return false;
    };

    let mut stake = 0;
    for (_, decision_block) in decision_blocks.values() {
        if carries_certificate(consensus, decision_block, leader, dag, wave_length) {
            stake += consensus.committee.stake(&decision_block.origin());
            if stake >= consensus.committee.quorum_threshold() {
                return true;
            }
        }
    }
    false
}

fn carries_certificate(
    consensus: &Consensus,
    decision_block: &Certificate,
    leader: &Certificate,
    dag: &Dag,
    wave_length: Round,
) -> bool {
    let voting_round = leader.round() + wave_length - 2;
    if decision_block.round() != voting_round + 1 {
        return false;
    }

    let Some(voting_blocks) = dag.get(&voting_round) else {
        return false;
    };

    let mut stake: Stake = 0;
    for parent in &decision_block.header.parents {
        let Some((_, vote)) = voting_blocks.values().find(|(digest, _)| digest == parent) else {
            continue;
        };
        if linked(vote, leader, dag) {
            stake += consensus.committee.stake(&vote.origin());
            if stake >= consensus.committee.quorum_threshold() {
                return true;
            }
        }
    }
    false
}

fn linked(from: &Certificate, target: &Certificate, dag: &Dag) -> bool {
    if from.round() < target.round() {
        return false;
    }
    if from.round() == target.round() {
        return from == target;
    }

    let mut frontier = vec![from];
    for r in (target.round() + 1..=from.round()).rev() {
        let Some(previous_round) = dag.get(&(r - 1)) else {
            return false;
        };
        frontier = previous_round
            .values()
            .filter(|(digest, _)| frontier.iter().any(|x| x.header.parents.contains(digest)))
            .map(|(_, certificate)| certificate)
            .collect();
        if frontier.iter().any(|x| *x == target) {
            return true;
        }
    }
    false
}

fn order_dag(leader: &Certificate, state: &State) -> Vec<Certificate> {
    debug!("Processing sub-dag of {:?}", leader);
    let mut ordered = Vec::new();
    let mut already_ordered = HashSet::new();

    let mut buffer = vec![leader];
    while let Some(x) = buffer.pop() {
        debug!("Sequencing {:?}", x);
        ordered.push(x.clone());
        for parent in &x.header.parents {
            let Some((digest, certificate)) = state
                .dag
                .get(&(x.round() - 1))
                .and_then(|round| round.values().find(|(digest, _)| digest == parent))
            else {
                continue;
            };

            let mut skip = already_ordered.contains(digest);
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

    ordered.sort_by_key(|x| x.round());
    ordered
}
