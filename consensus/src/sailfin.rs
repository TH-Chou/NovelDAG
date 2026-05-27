// TJU BLOCKCHAIN RESEARCH
// Sailfin experimental variant baseline.
//
// This starts as an isolated copy of Shortfin's embedded-QC implementation.
// Edge-voted fast commit logic will be added here without changing the
// Shortfin baseline entry point.
// 严格遵循设计文档 Section 5.1（轮结束条件）和 Section 6（提交规则）。

use crate::Consensus;
use crate::State;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use primary::{Certificate, Round};
use std::collections::{HashMap, HashSet};
#[cfg(feature = "benchmark")]
use std::time::Instant;

// ── 常量 ────────────────────────────────────────────────────

/// Wave 长度（设计文档 Section 6）。
const WAVE: Round = 4;

// ── 诊断计数器 ──────────────────────────────────────────────
//
// benchmark feature 激活时记录完整诊断数据；否则为零开销占位结构，
// 编译器会将所有 `diag.xxx` 访问优化为空操作。

#[cfg(feature = "benchmark")]
mod diag {
    use super::*;
    use std::time::Instant;

    pub struct Diag {
        pub seen_certificates: u64,
        pub commit_round_checks: u64,
        #[allow(dead_code)]
        pub skip_round_no_quorum: u64,
        #[allow(dead_code)]
        pub skip_leader_unavailable: u64,
        #[allow(dead_code)]
        pub skip_missing_b2: u64,
        #[allow(dead_code)]
        pub skip_missing_b1: u64,
        #[allow(dead_code)]
        pub skip_qc_chain_invalid: u64,
        #[allow(dead_code)]
        pub commits_emitted: u64,
        pub cert_received_at: HashMap<Digest, Instant>,
        pub cert_age_sum_ms: u64,
        pub cert_age_samples: u64,
    }

    impl Diag {
        pub fn new() -> Self {
            Self {
                seen_certificates: 0,
                commit_round_checks: 0,
                skip_round_no_quorum: 0,
                skip_leader_unavailable: 0,
                skip_missing_b2: 0,
                skip_missing_b1: 0,
                skip_qc_chain_invalid: 0,
                commits_emitted: 0,
                cert_received_at: HashMap::new(),
                cert_age_sum_ms: 0,
                cert_age_samples: 0,
            }
        }
    }
}

#[cfg(not(feature = "benchmark"))]
mod diag {
    pub struct Diag;
    impl Diag {
        pub fn new() -> Self {
            Self
        }
    }
}

use diag::Diag;

// ── 主循环 ──────────────────────────────────────────────────

pub(crate) async fn run(consensus: &mut Consensus) {
    let mut state = State::new(consensus.genesis.clone());
    let name = consensus.name;

    // 已触发过提交检查的轮次（防重入）。
    let mut completed_rounds: HashSet<Round> = HashSet::new();
    // Sailfin fast path emits early certificates before Shortfin's wave
    // fallback advances State. Keep a digest-level guard so the fallback can
    // still use the original State frontier without duplicating outputs.
    let mut ordered_certificates: HashSet<Digest> = HashSet::new();
    let mut next_fast_round: Round = 1;
    #[cfg(feature = "benchmark")]
    let mut diag = Diag::new();
    #[cfg(not(feature = "benchmark"))]
    let _diag = Diag::new();

    while let Some(certificate) = consensus.rx_primary.recv().await {
        let cutoff = state
            .last_committed_round
            .saturating_sub(consensus.gc_depth);
        completed_rounds.retain(|r| *r >= cutoff);

        #[cfg(feature = "benchmark")]
        {
            diag.seen_certificates += 1;
            diag.cert_received_at
                .entry(certificate.header.id.clone())
                .or_insert(Instant::now());
        }

        debug!("处理证书 {:?}", certificate);
        let round = certificate.round();

        // 丢弃已提交轮次的证书。
        if state
            .last_committed
            .get(&certificate.origin())
            .map_or(false, |r| *r >= round)
        {
            continue;
        }

        // 存入本地 DAG。
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // ── Sailfin opt path: one-round edge-voted leader commit ──
        //
        // A leader at round r is fast-certified once 2f+1 certified round r+1
        // vertices directly cite it in `parents`. The opt path emits only that
        // leader, in round order; the fallback below absorbs the rest of the
        // DAG using a protocol-fixed leader-first deterministic ordering.
        while next_fast_round > 0 && next_fast_round + 1 <= round {
            let Some(leader) = fast_certified_leader(consensus, next_fast_round, &state) else {
                break;
            };

            #[cfg(feature = "benchmark")]
            let leader_id = leader.header.id.clone();
            #[cfg(feature = "benchmark")]
            let leader_round = leader.round();
            let sequence = collect_fast_leader(leader, &state, &ordered_certificates);
            if sequence.is_empty() {
                next_fast_round += 1;
                continue;
            }

            #[cfg(feature = "benchmark")]
            info!(
                "DIAG_SAILFIN_FAST_COMMIT leader_round={} leader_author={} fast_blocks={}",
                leader_round,
                leader.origin(),
                sequence.len()
            );

            for certificate in sequence {
                if !ordered_certificates.insert(certificate.header.id.clone()) {
                    continue;
                }

                #[cfg(feature = "benchmark")]
                {
                    let cert_age_ms = diag
                        .cert_received_at
                        .get(&certificate.header.id)
                        .map(|t| t.elapsed().as_millis() as u64)
                        .unwrap_or(0);
                    diag.cert_age_sum_ms += cert_age_ms;
                    diag.cert_age_samples += 1;
                    info!(
                        "DIAG_FAST_COMMIT_LATENCY round={} author={} cert_age_ms={} leader_round={}",
                        certificate.round(),
                        certificate.origin(),
                        cert_age_ms,
                        leader_round,
                    );
                }
                #[cfg(feature = "benchmark")]
                if certificate.header.id == leader_id {
                    info!(
                        "DIAG_FAST_LEADER_COMMIT committed_leader_round={} leader_author={}",
                        leader_round,
                        certificate.origin()
                    );
                }

                #[cfg(not(feature = "benchmark"))]
                info!("Fast committed {}", certificate.header);

                consensus
                    .tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("向 primary 发送 fast-path 证书失败");

                if let Err(e) = consensus.tx_output.send(certificate).await {
                    warn!("输出 fast-path 证书失败: {}", e);
                }
            }

            next_fast_round += 1;
        }

        // ── Wave 边界闸门 ──
        if round < WAVE || round % WAVE != 0 || completed_rounds.contains(&round) {
            continue;
        }

        // ── Section 5.1 轮结束条件 ──
        if !round_ended(round, name, consensus, &state) {
            #[cfg(feature = "benchmark")]
            {
                diag.skip_round_no_quorum += 1;
            }
            continue;
        }
        completed_rounds.insert(round);

        // ── Wave 到达，尝试提交 ──
        let commit_round = round;

        #[cfg(feature = "benchmark")]
        {
            diag.commit_round_checks += 1;
        }

        // Leader 已在前一波提交则跳过。
        if commit_round.saturating_sub(3) <= state.last_committed_round {
            continue;
        }

        // ── Section 6 提交规则：验证 Leader 的 b3→b2→b1 链 ──
        let (b3, b2, b1) = match verify_leader_chain(consensus, commit_round, &state) {
            Ok(triple) => triple,
            Err(reason) => {
                #[cfg(feature = "benchmark")]
                match reason {
                    ChainError::LeaderUnavailable => diag.skip_leader_unavailable += 1,
                    ChainError::MissingB2 => diag.skip_missing_b2 += 1,
                    ChainError::MissingB1 => diag.skip_missing_b1 += 1,
                    ChainError::QcChainInvalid => diag.skip_qc_chain_invalid += 1,
                }
                #[cfg(not(feature = "benchmark"))]
                let _ = reason;
                continue;
            }
        };

        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CANDIDATE commit_round={} leader_round={} leader_author={}",
            commit_round,
            b3.round(),
            b3.origin()
        );

        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_COMMIT_CHAIN_OK commit_round={} leader_round={} commit_gap={}",
            commit_round,
            b3.round(),
            commit_round.saturating_sub(b3.round())
        );

        // ── 收集并提交 wave 内所有安全区块 ──
        let mut sequence = collect_wave(
            commit_round,
            &state,
            consensus.committee.validity_threshold() as usize,
            &[&b3, &b2, &b1],
        );
        prioritize_round_leaders(consensus, &state, &mut sequence, commit_round);

        #[cfg(feature = "benchmark")]
        let leader_id = b3.header.id.clone();
        #[cfg(feature = "benchmark")]
        let leader_round_log = b3.round();

        #[cfg(feature = "benchmark")]
        info!(
            "DIAG_WAVE_BATCH commit_round={} wave_blocks={}",
            commit_round,
            sequence.len(),
        );

        let sequence: Vec<Certificate> = sequence
            .into_iter()
            .filter(|certificate| ordered_certificates.insert(certificate.header.id.clone()))
            .collect();

        for x in sequence.iter() {
            state.update(x, consensus.gc_depth);
        }
        next_fast_round = next_fast_round.max(state.last_committed_round + 1);

        for certificate in sequence {
            #[cfg(feature = "benchmark")]
            {
                let cert_age_ms = diag
                    .cert_received_at
                    .get(&certificate.header.id)
                    .map(|t| t.elapsed().as_millis() as u64)
                    .unwrap_or(0);
                diag.cert_age_sum_ms += cert_age_ms;
                diag.cert_age_samples += 1;
                info!(
                    "DIAG_COMMIT_LATENCY round={} author={} cert_age_ms={} commit_round={} leader_round={}",
                    certificate.round(),
                    certificate.origin(),
                    cert_age_ms,
                    commit_round,
                    leader_round_log,
                );
            }
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
                .expect("向 primary 发送证书失败");

            if let Err(e) = consensus.tx_output.send(certificate).await {
                warn!("输出证书失败: {}", e);
            }
        }
    }
}

// ── Section 5.1: 轮结束条件 ─────────────────────────────────

/// 本地判定 round `r` 是否已结束。
///
/// 条件（两者必须同时满足）：
/// 1. 本节点在 `r` 轮有自己的块（说明自己的 QC 已形成）；
/// 2. `r` 轮至少有 2f+1 个不同作者的块。
fn round_ended(
    round: Round,
    name: crypto::PublicKey,
    consensus: &Consensus,
    state: &State,
) -> bool {
    let our_cert_exists = state
        .dag
        .get(&round)
        .map(|by_auth| by_auth.contains_key(&name))
        .unwrap_or(false);
    our_cert_exists && consensus.round_has_quorum(round, &state.dag)
}

// ── Sailfin opt path: edge-voted fast certificate ───────────

/// Return the round-`r` leader once it has a quorum of direct round-`r+1`
/// edge-votes. Each counted voter is itself a certified DAG vertex, so the
/// evidence is carried in-band by normal DAG dissemination.
fn fast_certified_leader<'a>(
    consensus: &Consensus,
    leader_round: Round,
    state: &'a State,
) -> Option<&'a Certificate> {
    let vote_round = leader_round + 1;
    if !consensus.round_has_quorum(vote_round, &state.dag) {
        return None;
    }

    let (leader_digest, leader) = consensus.leader(leader_round, vote_round, &state.dag)?;
    let support = state
        .dag
        .get(&vote_round)?
        .values()
        .filter(|(_, cert)| cert.header.parents.contains(leader_digest))
        .map(|(_, cert)| consensus.committee.stake(&cert.origin()))
        .sum::<u32>();

    if support >= consensus.committee.quorum_threshold() {
        Some(leader)
    } else {
        None
    }
}

/// Collect only the fast-certified leader. Emitting a broader local causal past
/// would make the early prefix depend on which ancestors a replica has already
/// delivered; the wave fallback remains responsible for committing the rest of
/// the DAG in a deterministic order.
fn collect_fast_leader(
    leader: &Certificate,
    state: &State,
    already_ordered: &HashSet<Digest>,
) -> Vec<Certificate> {
    if already_ordered.contains(&leader.header.id)
        || state
            .last_committed
            .get(&leader.origin())
            .map_or(false, |last_r| *last_r >= leader.round())
    {
        Vec::new()
    } else {
        vec![leader.clone()]
    }
}

/// Make fallback output agree with any opt-path prefix: all selected round
/// leaders contained in the wave are placed first by leader round, and the
/// remaining certificates keep Shortfin's `(round, digest)` order. This rule is
/// independent of which fast certificates this replica has already observed.
fn prioritize_round_leaders(
    consensus: &Consensus,
    state: &State,
    sequence: &mut [Certificate],
    commit_round: Round,
) {
    let round_leaders: HashMap<Digest, Round> = (1..commit_round)
        .filter_map(|round| {
            consensus
                .leader(round, round + 1, &state.dag)
                .map(|(_, leader)| (leader.header.id.clone(), round))
        })
        .collect();

    sequence.sort_by_key(|cert| {
        round_leaders
            .get(&cert.header.id)
            .map(|leader_round| (0, *leader_round, cert.header.id.clone()))
            .unwrap_or_else(|| (1, cert.round(), cert.header.id.clone()))
    });
}

// ── Section 6: Leader 链验证 ─────────────────────────────────

/// 验证 Section 6 提交规则所需的同作者三块链 b3→b2→b1。
///
/// ```text
/// commit_round = r（r%4==0, r≥4）
/// leader_round = r-3
///
/// b3 = Leader 在 leader_round 的块
/// b2 = 同作者在 leader_round+1 的块（须携带 b3 的 QC）
/// b1 = 同作者在 leader_round+2 的块（须携带 b2 的 QC）
/// ```
///
/// 返回 `Some((b3, b2, b1))` 当且仅当：
/// - Leader 在 `leader_round` 存在；
/// - b2、b1 均存在且属于同作者；
/// - b2.qc → b3 且 b1.qc → b2（含 `voter_round < commit_round` 检查）。
enum ChainError {
    LeaderUnavailable,
    MissingB2,
    MissingB1,
    QcChainInvalid,
}

fn verify_leader_chain<'a>(
    consensus: &Consensus,
    commit_round: Round,
    state: &'a State,
) -> Result<(&'a Certificate, &'a Certificate, &'a Certificate), ChainError> {
    debug_assert!(commit_round >= WAVE, "commit_round must be >= WAVE");
    let leader_round = commit_round - 3;

    let (_, b3) = consensus
        .leader(leader_round, commit_round, &state.dag)
        .ok_or(ChainError::LeaderUnavailable)?;

    let b2 = consensus
        .certificate_by_author(leader_round + 1, b3.origin(), &state.dag)
        .ok_or(ChainError::MissingB2)?;

    let b1 = consensus
        .certificate_by_author(leader_round + 2, b3.origin(), &state.dag)
        .ok_or(ChainError::MissingB1)?;

    if !consensus.embedded_qc_links(b2, b3, commit_round)
        || !consensus.embedded_qc_links(b1, b2, commit_round)
    {
        debug!("Leader {:?} 不满足 b3→b2→b1 QC 链", b3);
        return Err(ChainError::QcChainInvalid);
    }

    debug!("Leader {:?} 满足 Section-6 提交规则", b3);
    Ok((b3, b2, b1))
}

// ── 因果可达性 BFS ──────────────────────────────────────────

/// 从种子集合出发，沿 `parents`（r-1 跳）和 `parents_2`（r-2 跳）反向
/// BFS 遍历因果可达顶点。`index` 提供 Digest→&Certificate 的 O(1) 映射。
/// BFS 遍历 DAG，收集所有因果可达区块的 `header.id`。
///
/// 种子包含：
/// - Leader 三块链（b3, b2, b1），由内嵌 QC 链验证；
/// - 锚定的 commit_round-1 轮块（被 ≥f+1 个 commit_round 块引用）。
///
/// 不在可达集合中的块是"孤儿块"——可能由拜占庭节点注入，与已验证的
/// 提交前沿无因果联系，不在当前 wave 提交。
fn causal_reachability(
    seeds: &[&Certificate],
    index: &HashMap<Digest, &Certificate>,
    last_committed: &HashMap<PublicKey, Round>,
) -> HashSet<Digest> {
    let floor = seeds
        .iter()
        .map(|c| c.round())
        .min()
        .unwrap_or(0)
        .saturating_sub(100);
    let mut reachable: HashSet<Digest> = HashSet::new();
    let mut buffer: Vec<&Certificate> = seeds.to_vec();

    while let Some(cert) = buffer.pop() {
        if cert.round() < floor {
            continue;
        }

        if last_committed
            .get(&cert.origin())
            .map_or(false, |r| *r >= cert.round())
        {
            continue;
        }

        if !reachable.insert(cert.header.id.clone()) {
            continue;
        }

        for parent_digest in &cert.header.parents {
            if let Some(parent_cert) = index.get(parent_digest) {
                if !reachable.contains(&parent_cert.header.id) {
                    buffer.push(parent_cert);
                }
            }
        }

        for parent_digest in &cert.header.parents_2 {
            if let Some(parent_cert) = index.get(parent_digest) {
                if !reachable.contains(&parent_cert.header.id) {
                    buffer.push(parent_cert);
                }
            }
        }
    }

    reachable
}

// ── Wave 提交收集 ───────────────────────────────────────────

/// 收集当前 wave 中所有可安全提交的未提交证书。
///
/// 安全性由两层互补检查保证：
///
/// 1. **锚定**（仅 commit_round-1 轮）：
///    一个 round = commit_round-1 的证书只有当 ≥ `validity_threshold`（=f+1）
///    个 commit_round 块在 `parents` 中引用它时才被提交。这保证所有诚实节点
///    对该证书的存在达成共识。
///
/// 2. **因果可达性**（round < commit_round-1 的轮次）：
///    更早轮次的证书只有当其 `header.id` 在提交前沿的因果可达集合中时才被
///    提交。提交前沿 = {b3, b2, b1} ∪ {锚定的 commit_round-1 块}。
///    这防止提交与已验证块无因果联系的孤儿块。
///
/// Leader 三块链被显式纳入提交前沿，因为即使拜占庭 Leader 在彼此 `parents`
/// 中互相排除，内嵌 QC 链已验证它们的存在与唯一性。
///
/// 最终按 (round, digest) 字典序输出，保证所有诚实节点复现完全相同的序列。
fn collect_wave(
    commit_round: Round,
    state: &State,
    validity_threshold: usize,
    leader_blocks: &[&Certificate],
) -> Vec<Certificate> {
    // ── 锚定计数：统计每个 r-1 摘要被 commit_round 块引用的次数 ──
    let anchored: HashMap<Digest, usize> = match state.dag.get(&commit_round) {
        Some(by_auth) => {
            let mut counts = HashMap::new();
            for (_, cert) in by_auth.values() {
                for parent in &cert.header.parents {
                    *counts.entry(parent.clone()).or_insert(0) += 1;
                }
            }
            counts
        }
        None => HashMap::new(),
    };

    // ── 锚定的 r-1 块（引用数 ≥ f+1） ──
    let anchored_r1: Vec<&Certificate> = state
        .dag
        .get(&(commit_round - 1))
        .map(|by_auth| {
            by_auth
                .values()
                .filter(|(_, cert)| {
                    anchored
                        .get(&cert.digest())
                        .map_or(false, |&count| count >= validity_threshold)
                })
                .map(|(_, cert)| cert)
                .collect()
        })
        .unwrap_or_default();

    // ── 提交前沿 = Leader 链 + 锚定 r-1 块 ──
    let mut seeds: Vec<&Certificate> = leader_blocks.to_vec();
    seeds.extend(anchored_r1.iter().copied());
    // 构建反向索引：Digest → &Certificate，使 causal_reachability O(1) 查找。
    let index: HashMap<Digest, &Certificate> = state
        .dag
        .iter()
        .filter(|(r, _)| **r >= state.last_committed_round && **r < commit_round)
        .flat_map(|(_, by_auth)| by_auth.values().map(|(_, cert)| (cert.digest(), cert)))
        .filter(|(_, cert)| {
            state
                .last_committed
                .get(&cert.origin())
                .map_or(true, |last_r| cert.round() > *last_r)
        })
        .collect();
    let reachable = causal_reachability(&seeds, &index, &state.last_committed);

    // ── 收集并过滤 ──
    let mut blocks: Vec<&Certificate> = state
        .dag
        .iter()
        .filter(|(r, _)| **r >= state.last_committed_round && **r < commit_round)
        .flat_map(|(_, by_auth)| by_auth.values().map(|(_, cert)| cert))
        .filter(|cert| {
            state
                .last_committed
                .get(&cert.origin())
                .map_or(true, |last_r| cert.round() > *last_r)
        })
        .filter(|cert| {
            if cert.round() == commit_round - 1 {
                // 锚定检查：必须被 ≥f+1 个 commit_round 块引用。
                anchored
                    .get(&cert.digest())
                    .map_or(false, |&count| count >= validity_threshold)
            } else {
                // 因果可达检查：必须在提交前沿的可达集合中。
                reachable.contains(&cert.header.id)
            }
        })
        .collect();

    // 确定性排序：先按轮次，再按 header.id 字典序。
    blocks.sort_by_key(|c| (c.round(), c.header.id.clone()));
    blocks.into_iter().cloned().collect()
}
