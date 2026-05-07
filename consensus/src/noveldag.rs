// Copyright(C) Facebook, Inc. and its affiliates.
// NovelDAG consensus: round-completion-driven model with 2-round leader-to-commit delay,
// b3→b2→b1 leader chain, embedded QC links, every-round commits.
use crate::Consensus;
use crate::State;
use crypto::Hash as _;
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::collections::{HashMap, HashSet};

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

        // --- Round-completion detection ---
        // A round is "complete" (for this node) when:
        //   1. Our own certificate exists at this round (meaning our block got QC'd), AND
        //   2. At least 2f+1 certificates exist at this round.
        //
        // This matches the design doc Section 5.1 round-end condition:
        // own QC formed + quorum received → round ends → run commit logic.
        if round < 3 || completed_rounds.contains(&round) {
            continue;
        }

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

        // Round completed. Run commit logic.
        // NovelDAG uses 2-round leader-to-commit delay: leader at round-2 is committed
        // when round completes, needing only b2 (round-1) and b1 (round) carrying embedded QCs.
        let commit_round = round;

        #[cfg(feature = "benchmark")]
        {
            diag_commit_round_checks += 1;
        }

        let leader_round = commit_round.saturating_sub(2);

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

        // If this leader's block was already committed, skip.
        if state
            .last_committed
            .get(&leader.origin())
            .map_or(false, |r| *r >= leader.round())
        {
            continue;
        }

        let b3 = leader.clone();
        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CANDIDATE commit_round={} leader_round={} leader_author={}",
            commit_round,
            b3.round(),
            b3.origin()
        );

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

        // b2 embeds QC(b3): b2.qc.target == b3.id, b2.qc.round == b3.round < commit_round
        // b1 embeds QC(b2): b1.qc.target == b2.id, b1.qc.round == b2.round < commit_round
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

        debug!("Leader {:?} satisfies 2-delay commit rule", b3);
        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CHAIN_OK commit_round={} leader_round={} commit_gap={}",
            commit_round,
            b3.round(),
            commit_round.saturating_sub(b3.round())
        );

        // Use pre-computed order if available, otherwise compute on demand.
        let sequence = if let Some(cached) = consensus.precomputed.remove(&leader_round) {
            cached
        } else {
            order_dag(&b3, &state, consensus.gc_depth)
        };

        for x in &sequence {
            state.update(x, consensus.gc_depth);
        }

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

        // Pre-compute order_dag for the next commit (leader at round-1, committed when round+1 completes).
        // With 2-round delay, b1 sits at the commit round itself, so precompute may often fail —
        // the fallback to on-demand order_dag handles this transparently.
        let pre_leader_round = commit_round.saturating_sub(1);
        if pre_leader_round > state.last_committed_round
            && pre_leader_round >= 1
        {
            if let Some(ordered) =
                precompute_order(consensus, pre_leader_round, commit_round + 2, &state)
            {
                consensus.precomputed.insert(pre_leader_round, ordered);
            }
        }

        #[cfg(feature = "benchmark")]
        if commit_round % 20 == 0 {
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

fn precompute_order(
    consensus: &Consensus,
    leader_round: Round,
    _commit_round: Round,
    state: &State,
) -> Option<Vec<Certificate>> {
    let (_, leader) = consensus.leader(leader_round, _commit_round, &state.dag)?;
    let b3 = leader.clone();

    let b2 = consensus.certificate_by_author(leader_round + 1, b3.origin(), &state.dag)?;
    let b1 = consensus.certificate_by_author(leader_round + 2, b3.origin(), &state.dag)?;

    if !consensus.embedded_qc_links(b2, &b3, _commit_round)
        || !consensus.embedded_qc_links(b1, b2, _commit_round)
    {
        return None;
    }

    Some(order_dag(&b3, state, consensus.gc_depth))
}

/// Flatten the dag referenced by the input certificate (NovelDAG version with parents_2 traversal).
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
                .map(|x| x.values().find(|(x, _)| x == parent))
                .flatten()
            {
                Some(x) => x,
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

        // Also traverse second-hop parents (parents_2) to ensure causal completeness.
        if x.round() >= 2 {
            for parent in &x.header.parents_2 {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 2))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
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
