// Copyright(C) Facebook, Inc. and its affiliates.
// NovelDAG consensus: 4-round wave, r-3 leader, b3→b2→b1 embedded QC chain.
// Faithful implementation of Section 6 of the design doc.
use crate::Consensus;
use crate::State;
use crypto::Hash as _;
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::collections::{HashMap, HashSet};

/// Wave length (Section 6 of the design doc).
const WAVE: Round = 4;

pub(crate) async fn run(consensus: &mut Consensus) {
    let mut state = State::new(consensus.genesis.clone());
    let name = consensus.name;

    // Track which rounds have already had their commit check run.
    let mut completed_rounds: HashSet<Round> = HashSet::new();

    #[cfg(feature = "benchmark")]
    let mut diag_seen_certificates = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_commit_round_checks = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_skip_round_no_quorum = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_skip_leader_unavailable = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_skip_missing_b2 = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_skip_missing_b1 = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_skip_qc_chain_invalid = 0u64;
    #[cfg(feature = "benchmark")]
    let mut diag_commits_emitted = 0u64;

    while let Some(certificate) = consensus.rx_primary.recv().await {
        #[cfg(feature = "benchmark")]
        {
            diag_seen_certificates += 1;
        }

        debug!("Processing {:?}", certificate);
        let round = certificate.round();

        // Drop certificates whose origin's round is already committed.
        if state
            .last_committed
            .get(&certificate.origin())
            .map_or(false, |r| *r >= round)
        {
            continue;
        }

        // Add the new certificate to the local storage.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // --- Wave boundary gate (Section 6) ---
        // Only waves ending at r%4==0 can trigger a commit.
        // The minimum viable wave ends at r=4 with leader at r-3=1.
        if round < WAVE || round % WAVE != 0 || completed_rounds.contains(&round) {
            continue;
        }

        // --- Round-end condition (Section 5.1) ---
        // Round r ends locally when:
        //   (1) our own r-block exists in the dag (our QC was formed), AND
        //   (2) we have received at least 2f+1 r-blocks.
        let our_cert_exists = state
            .dag
            .get(&round)
            .map(|by_auth| by_auth.contains_key(&name))
            .unwrap_or(false);
        if !our_cert_exists {
            continue;
        }
        if !consensus.round_has_quorum(round, &state.dag) {
            #[cfg(feature = "benchmark")]
            {
                diag_skip_round_no_quorum += 1;
            }
            continue;
        }

        completed_rounds.insert(round);

        // Wave reached. Apply Section-6 commit rule.
        let commit_round = round;
        let leader_round = commit_round - 3;

        #[cfg(feature = "benchmark")]
        {
            diag_commit_round_checks += 1;
        }

        // Nothing to do if the would-be leader was already committed in a previous wave.
        if leader_round <= state.last_committed_round {
            continue;
        }

        let (_, leader) = match consensus.leader(leader_round, commit_round, &state.dag) {
            Some(x) => x,
            None => {
                #[cfg(feature = "benchmark")]
                {
                    diag_skip_leader_unavailable += 1;
                }
                continue;
            }
        };

        let b3 = leader.clone();
        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CANDIDATE commit_round={} leader_round={} leader_author={}",
            commit_round,
            b3.round(),
            b3.origin()
        );

        // b2 is the leader's block at r-2, b1 is the leader's block at r-1.
        let Some(b2) =
            consensus.certificate_by_author(leader_round + 1, b3.origin(), &state.dag)
        else {
            #[cfg(feature = "benchmark")]
            {
                diag_skip_missing_b2 += 1;
            }
            continue;
        };
        let Some(b1) =
            consensus.certificate_by_author(leader_round + 2, b3.origin(), &state.dag)
        else {
            #[cfg(feature = "benchmark")]
            {
                diag_skip_missing_b1 += 1;
            }
            continue;
        };

        // b2 must embed QC(b3), b1 must embed QC(b2); all votes in both QCs
        // must have voter_round < commit_round.
        if !consensus.embedded_qc_links(b2, &b3, commit_round)
            || !consensus.embedded_qc_links(b1, b2, commit_round)
        {
            #[cfg(feature = "benchmark")]
            {
                diag_skip_qc_chain_invalid += 1;
            }
            debug!("Leader {:?} does not satisfy b3->b2->b1 QC chain", b3);
            continue;
        }

        debug!("Leader {:?} satisfies Section-6 commit rule", b3);
        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CHAIN_OK commit_round={} leader_round={} commit_gap={}",
            commit_round,
            b3.round(),
            commit_round.saturating_sub(b3.round())
        );

        // Recursively order the sub-DAG rooted at b3 (Section 6 last line).
        let sequence = order_dag(&b3, &state, consensus.gc_depth);

        for x in &sequence {
            state.update(x, consensus.gc_depth);
        }

        // Conservative GC of completed_rounds: any round older than
        // last_committed_round - gc_depth is beyond the garbage-collection
        // horizon and can never be reached again.
        let cutoff = state.last_committed_round.saturating_sub(consensus.gc_depth);
        completed_rounds.retain(|r| *r >= cutoff);

        // Log the latest committed round of every authority.
        if log_enabled!(log::Level::Debug) {
            for (name, round) in &state.last_committed {
                debug!("Latest commit of {}: Round {}", name, round);
            }
        }

        #[cfg(feature = "benchmark")]
        let leader_id = b3.header.id.clone();
        #[cfg(feature = "benchmark")]
        let leader_round_log = b3.round();
        for certificate in sequence {
            #[cfg(feature = "benchmark")]
            if certificate.header.id == leader_id {
                info!(
                    "DIAG_LEADER_COMMIT committed_leader_round={} commit_round={} commit_gap={} leader_author={}",
                    leader_round_log,
                    commit_round,
                    commit_round.saturating_sub(leader_round_log),
                    certificate.origin()
                );
            }

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

            #[cfg(feature = "benchmark")]
            {
                diag_commits_emitted += 1;
            }
        }

        #[cfg(feature = "benchmark")]
        if commit_round % (WAVE * 5) == 0 {
            info!(
                "DIAG_CONSENSUS_COMMIT round={} seen_certificates={} commit_checks={} commits_emitted={} skip_round_no_quorum={} skip_leader_unavailable={} skip_missing_b2={} skip_missing_b1={} skip_qc_chain_invalid={}",
                commit_round,
                diag_seen_certificates,
                diag_commit_round_checks,
                diag_commits_emitted,
                diag_skip_round_no_quorum,
                diag_skip_leader_unavailable,
                diag_skip_missing_b2,
                diag_skip_missing_b1,
                diag_skip_qc_chain_invalid,
            );
        }
    }
}

/// Flatten the sub-DAG referenced by the committed leader. Traverses both
/// parents (r-1) and parents_2 (r-2) edges so every block causally referenced
/// by the leader is ordered exactly once.
fn order_dag(leader: &Certificate, state: &State, gc_depth: Round) -> Vec<Certificate> {
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
                .map(|level| level.values().find(|(d, _)| d == parent))
                .flatten()
            {
                Some(v) => v,
                None => continue,
            };

            let mut skip = already_ordered.contains(&digest);
            skip |= state
                .last_committed
                .get(&certificate.origin())
                .map_or(false, |r| *r >= certificate.round());
            if !skip {
                buffer.push(certificate);
                already_ordered.insert(digest);
            }
        }

        if x.round() >= 2 {
            for parent in &x.header.parents_2 {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 2))
                    .map(|level| level.values().find(|(d, _)| d == parent))
                    .flatten()
                {
                    Some(v) => v,
                    None => continue,
                };

                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or(false, |r| *r >= certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }
    }

    ordered.retain(|x| x.round() + gc_depth >= state.last_committed_round);
    ordered.sort_by_key(|x| x.round());
    ordered
}
