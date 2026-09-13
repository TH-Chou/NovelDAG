// TJU BLOCKCHAIN RESEARCH
// Shortfin baseline consensus: 4-round waves, r-3 leader, b3→b2→b1 embedded-QC chain.
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

/// Exact Shortfin records indexed by certificate digest. Unlike the generic
/// Narwhal DAG, this index retains every equivocation at `(author, round)`.
type ShortfinRecords = HashMap<Digest, Certificate>;

fn insert_record(state: &mut State, records: &mut ShortfinRecords, certificate: Certificate) {
    let digest = certificate.digest();
    let origin = certificate.origin();
    let round = certificate.round();

    records
        .entry(digest.clone())
        .and_modify(|stored| {
            if stored.votes.is_empty() && !certificate.votes.is_empty() {
                *stored = certificate.clone();
            }
        })
        .or_insert_with(|| certificate.clone());

    // The generic round index is still useful for distinct-author quorum and
    // coin recovery. Keep its first branch stable, but allow the exact same
    // structural record to be upgraded when its QC becomes available.
    let by_author = state.dag.entry(round).or_insert_with(HashMap::new);
    match by_author.get_mut(&origin) {
        Some((stored_digest, stored)) if *stored_digest == digest => {
            if stored.votes.is_empty() && !certificate.votes.is_empty() {
                *stored = certificate;
            }
        }
        Some(_) => {}
        None => {
            by_author.insert(origin, (digest, certificate));
        }
    }
}

fn records_at_round(records: &ShortfinRecords, round: Round) -> impl Iterator<Item = &Certificate> {
    records
        .values()
        .filter(move |certificate| certificate.round() == round)
}

// ── 主循环 ──────────────────────────────────────────────────

pub(crate) async fn run(consensus: &mut Consensus) {
    let mut state = State::new(consensus.genesis.clone());
    let mut records = consensus
        .genesis
        .iter()
        .cloned()
        .map(|certificate| (certificate.digest(), certificate))
        .collect::<ShortfinRecords>();
    let name = consensus.name;

    // Boundaries that have already produced a decision. Failed checks remain
    // retryable when another boundary record arrives.
    let mut processed_boundaries: HashSet<Round> = HashSet::new();
    #[cfg(feature = "benchmark")]
    let mut diag = Diag::new();
    #[cfg(not(feature = "benchmark"))]
    let _diag = Diag::new();

    while let Some(certificate) = consensus.rx_primary.recv().await {
        let cutoff = state
            .last_committed_round
            .saturating_sub(consensus.gc_depth);
        processed_boundaries.retain(|r| *r >= cutoff);

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

        // Store the exact branch without allowing a sibling equivocation to
        // overwrite it. Structural records are upgraded in place once the QC
        // and payload have both been validated by Primary.
        insert_record(&mut state, &mut records, certificate);

        // ── Wave 边界闸门 ──
        if round < WAVE || round % WAVE != 0 || processed_boundaries.contains(&round) {
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
        let (b3, b2, b1) = match verify_leader_chain(consensus, commit_round, &state, &records) {
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
        processed_boundaries.insert(round);

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
        let sequence = collect_wave(
            commit_round,
            &state,
            consensus.committee.validity_threshold() as usize,
            &[&b3, &b2, &b1],
            &records,
        );

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

        for x in sequence.iter() {
            state.update(x, consensus.gc_depth);
        }

        let min_round = state
            .last_committed_round
            .saturating_sub(consensus.gc_depth);
        records.retain(|_, certificate| certificate.round() >= min_round);

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
            if !certificate.header.benchmark_invalid_payload {
                for digest in certificate.header.payload.keys() {
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }
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
    state: &State,
    records: &'a ShortfinRecords,
) -> Result<(&'a Certificate, &'a Certificate, &'a Certificate), ChainError> {
    debug_assert!(commit_round >= WAVE, "commit_round must be >= WAVE");
    let leader_round = commit_round - 3;

    let coin_certificates = state
        .dag
        .get(&commit_round)
        .into_iter()
        .flat_map(|by_author| by_author.values())
        .map(|(_, certificate)| certificate)
        .collect::<Vec<_>>();
    let leader = consensus
        .leader_authority_from_certificates(leader_round, commit_round, 0, &coin_certificates)
        .ok_or(ChainError::LeaderUnavailable)?;

    let mut b3_candidates = records_at_round(records, leader_round)
        .filter(|certificate| certificate.origin() == leader)
        .collect::<Vec<_>>();
    b3_candidates.sort_by_key(|certificate| certificate.digest());
    if b3_candidates.is_empty() {
        return Err(ChainError::LeaderUnavailable);
    }

    let mut b2_candidates = records_at_round(records, leader_round + 1)
        .filter(|certificate| certificate.origin() == leader)
        .collect::<Vec<_>>();
    b2_candidates.sort_by_key(|certificate| certificate.digest());
    if b2_candidates.is_empty() {
        return Err(ChainError::MissingB2);
    }

    let mut b1_candidates = records_at_round(records, leader_round + 2)
        .filter(|certificate| certificate.origin() == leader)
        .collect::<Vec<_>>();
    b1_candidates.sort_by_key(|certificate| certificate.digest());
    if b1_candidates.is_empty() {
        return Err(ChainError::MissingB1);
    }

    for b3 in b3_candidates {
        for b2 in &b2_candidates {
            if !consensus.embedded_qc_links(b2, b3, commit_round) {
                continue;
            }
            for b1 in &b1_candidates {
                if consensus.embedded_qc_links(b1, b2, commit_round) {
                    debug!("Leader {:?} 满足 Section-6 提交规则", b3);
                    return Ok((b3, b2, b1));
                }
            }
        }
    }

    debug!("Leader {} 不满足 b3→b2→b1 QC 链", leader);
    Err(ChainError::QcChainInvalid)
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
pub(crate) fn collect_wave(
    commit_round: Round,
    state: &State,
    validity_threshold: usize,
    leader_blocks: &[&Certificate],
    records: &ShortfinRecords,
) -> Vec<Certificate> {
    // Count distinct authors rather than blocks, so one equivocating author
    // cannot amplify its anchoring weight by emitting multiple variants.
    let mut anchored_authors: HashMap<Digest, HashSet<PublicKey>> = HashMap::new();
    let mut refs_by_author: HashMap<PublicKey, HashSet<Digest>> = HashMap::new();
    for certificate in records_at_round(records, commit_round) {
        refs_by_author
            .entry(certificate.origin())
            .or_default()
            .extend(certificate.header.parents.iter().cloned());
    }
    for (author, parents) in refs_by_author {
        for parent in parents {
            anchored_authors.entry(parent).or_default().insert(author);
        }
    }
    let anchored = anchored_authors
        .into_iter()
        .map(|(digest, authors)| (digest, authors.len()))
        .collect::<HashMap<_, _>>();

    // ── 锚定的 r-1 块（引用数 ≥ f+1） ──
    let anchored_r1 = records_at_round(records, commit_round - 1)
        .filter(|certificate| {
            anchored
                .get(&certificate.digest())
                .map_or(false, |&count| count >= validity_threshold)
        })
        .collect::<Vec<_>>();

    // ── 提交前沿 = Leader 链 + 锚定 r-1 块 ──
    let mut seeds: Vec<&Certificate> = leader_blocks.to_vec();
    seeds.extend(anchored_r1.iter().copied());
    // 构建反向索引：Digest → &Certificate，使 causal_reachability O(1) 查找。
    let index: HashMap<Digest, &Certificate> = records
        .iter()
        .filter(|(_, certificate)| certificate.round() < commit_round)
        .filter(|(_, cert)| {
            state
                .last_committed
                .get(&cert.origin())
                .map_or(true, |last_r| cert.round() > *last_r)
        })
        .map(|(digest, certificate)| (digest.clone(), certificate))
        .collect();
    let reachable = causal_reachability(&seeds, &index, &state.last_committed);

    // Certification must be exposed by this fixed reachable header history.
    // A QC learned elsewhere may update local state, but cannot retroactively
    // add its target to an earlier anchor's output set.
    let mut certified_targets: HashSet<Digest> = index
        .values()
        .filter(|cert| reachable.contains(&cert.header.id))
        .filter_map(|cert| cert.header.qc.as_ref().map(|qc| qc.target.clone()))
        .collect();
    // Boundary headers are the fixed witnesses used above to anchor r-1.
    // Their embedded QCs certify those frontier blocks in the same decision,
    // so they must not be delayed to the next wave.
    certified_targets.extend(
        records_at_round(records, commit_round)
            .filter_map(|certificate| certificate.header.qc.as_ref())
            .map(|qc| qc.target.clone()),
    );

    // ── 收集并过滤 ──
    let mut blocks: Vec<&Certificate> = records
        .values()
        .filter(|certificate| certificate.round() < commit_round)
        .filter(|cert| {
            state
                .last_committed
                .get(&cert.origin())
                .map_or(true, |last_r| cert.round() > *last_r)
        })
        // A non-empty vote set is the Primary's local marker that this exact
        // target has both a valid QC and a validated payload. Structural-only
        // records, including bubbles, remain in the traversal but are not output.
        .filter(|cert| certified_targets.contains(&cert.header.id) && !cert.votes.is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus_tests::mock_committee;
    use primary::{Header, Vote};

    fn record(author: PublicKey, round: Round, salt: u8, certified: bool) -> Certificate {
        let mut id = [0u8; 32];
        id[..8].copy_from_slice(&round.to_le_bytes());
        id[8..16].copy_from_slice(&author.0[..8]);
        id[31] = salt;
        Certificate {
            header: Header {
                author,
                round,
                id: Digest(id),
                ..Header::default()
            },
            votes: certified.then(Vote::default).into_iter().collect(),
        }
    }

    #[test]
    fn exact_index_retains_equivocations_and_never_downgrades_a_qc() {
        let committee = mock_committee();
        let author = *committee.authorities.keys().next().unwrap();
        let mut state = State::new(Certificate::genesis(&committee));
        let mut records = ShortfinRecords::new();
        let first = record(author, 1, 1, false);
        let second = record(author, 1, 2, false);
        let certified_first = record(author, 1, 1, true);

        insert_record(&mut state, &mut records, first.clone());
        insert_record(&mut state, &mut records, second.clone());
        insert_record(&mut state, &mut records, certified_first.clone());
        insert_record(&mut state, &mut records, first.clone());

        assert_eq!(records.len(), 2);
        assert!(!records.get(&first.digest()).unwrap().votes.is_empty());
        assert!(records.get(&second.digest()).unwrap().votes.is_empty());
        assert_eq!(state.dag.get(&1).unwrap().len(), 1);
    }
}
