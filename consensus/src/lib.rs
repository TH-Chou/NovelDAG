// Copyright(C) Facebook, Inc. and its affiliates.
// Unified consensus router: dispatches based on dag_protocol.
use config::{Committee, ConsensusProtocol, DagProtocol, Stake};
use crypto::Hash as _;
use crypto::{coin, Digest, PublicKey};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

mod bullshark;
mod mahi_mahi;
mod narwhal;
mod shortfin;
mod wahoo;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
pub(crate) struct State {
    /// The last committed round.
    last_committed_round: Round,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    pub(crate) committee: Committee,
    /// The depth of the garbage collector.
    pub(crate) gc_depth: Round,
    /// Which DAG protocol variant is running.
    dag_protocol: DagProtocol,
    /// The consensus leader election mode.
    pub(crate) consensus_protocol: ConsensusProtocol,
    /// The public key of this authority, used by Shortfin-style round-completion detection.
    pub(crate) name: PublicKey,
    /// Sorted authority set and communication-free leader schedules shared by
    /// all consensus protocols.
    coin_committee: coin::CoinCommittee,
    /// Shared threshold-coin backend for protocols that carry coin shares in
    /// their DAG units. The recovered-round cache lives inside this object.
    threshold_coin: Option<coin::ThresholdCoin>,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    pub(crate) rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    pub(crate) tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    pub(crate) tx_output: Sender<Certificate>,

    /// The genesis certificates.
    pub(crate) genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        Self::spawn_with_protocol(
            name,
            committee,
            gc_depth,
            DagProtocol::Shortfin,
            ConsensusProtocol::RoundRobin,
            rx_primary,
            tx_primary,
            tx_output,
        );
    }

    pub fn spawn_with_protocol(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        dag_protocol: DagProtocol,
        consensus_protocol: ConsensusProtocol,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            let authorities: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
            let coin_committee = coin::CoinCommittee::new(&authorities);
            let threshold_coin = if matches!(consensus_protocol, ConsensusProtocol::CommonCoin)
                && !matches!(dag_protocol, DagProtocol::Wahoo)
            {
                Some(coin::ThresholdCoin::from_committee(
                    coin_committee.clone(),
                    coin::threshold(committee.size()),
                ))
            } else {
                None
            };
            Self {
                committee: committee.clone(),
                gc_depth,
                dag_protocol,
                consensus_protocol,
                name,
                coin_committee,
                threshold_coin,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        match self.dag_protocol {
            DagProtocol::Narwhal => narwhal::run(self).await,
            DagProtocol::Bullshark => bullshark::run(self).await,
            DagProtocol::Shortfin => shortfin::run(self).await,
            DagProtocol::MahiMahi | DagProtocol::MahiMahi4 | DagProtocol::MahiMahi5 => {
                mahi_mahi::run(self).await
            }
            DagProtocol::Wahoo => wahoo::run(self).await,
        }
    }

    // ---------------- Shared helpers (used by multiple protocol modules) ----------------

    pub(crate) fn round_robin_coin(&self, round: Round) -> Round {
        #[cfg(test)]
        {
            let _ = round;
            0
        }
        #[cfg(not(test))]
        {
            self.coin_committee.round_robin(round)
        }
    }

    pub(crate) fn pseudo_random_coin(&self, round: Round) -> Round {
        self.coin_committee.pseudo_random(round)
    }

    /// Combine BLS coin shares carried by the selected protocol at `round`.
    /// Every DAG protocol reaches this method after observing a round quorum;
    /// f+1 valid shares are enough to recover a view-independent value.
    pub(crate) fn threshold_coin(&self, round: Round, dag: &Dag) -> Option<Round> {
        let certificates = dag.get(&round)?;
        let weight: Stake = certificates
            .values()
            .map(|(_, c)| self.committee.stake(&c.origin()))
            .sum();
        if weight < self.committee.quorum_threshold() {
            return None;
        }
        let threshold_coin = self.threshold_coin.as_ref()?;
        let shares: Vec<(PublicKey, Vec<u8>)> = certificates
            .values()
            .filter_map(|(_, cert)| {
                if cert.header.coin_share.is_empty() {
                    None
                } else {
                    Some((cert.origin(), cert.header.coin_share.clone()))
                }
            })
            .collect();
        threshold_coin
            .recover(round, &shares)
            .map(|coin| coin as Round)
    }

    pub(crate) fn coin_value(
        &self,
        leader_round: Round,
        coin_round: Round,
        dag: &Dag,
    ) -> Option<Round> {
        match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => Some(self.round_robin_coin(leader_round)),
            ConsensusProtocol::PseudoRandom => Some(self.pseudo_random_coin(coin_round)),
            ConsensusProtocol::CommonCoin => self.threshold_coin(coin_round, dag),
        }
    }

    pub(crate) fn leader_authority(
        &self,
        leader_round: Round,
        coin_round: Round,
        offset: usize,
        dag: &Dag,
    ) -> Option<PublicKey> {
        self.coin_value(leader_round, coin_round, dag)
            .map(|value| self.coin_committee.leader(value, offset))
    }

    /// Returns the certificate (and digest) originated by the leader.
    pub(crate) fn leader<'a>(
        &self,
        round: Round,
        coin_round: Round,
        dag: &'a Dag,
    ) -> Option<&'a (Digest, Certificate)> {
        let by_round = dag.get(&round)?;

        let leader = self.leader_authority(round, coin_round, 0, dag)?;

        by_round.get(&leader)
    }

    pub(crate) fn round_has_quorum(&self, round: Round, dag: &Dag) -> bool {
        let Some(certificates) = dag.get(&round) else {
            return false;
        };
        let weight: Stake = certificates
            .values()
            .map(|(_, certificate)| self.committee.stake(&certificate.origin()))
            .sum();
        weight >= self.committee.quorum_threshold()
    }

    pub(crate) fn certificate_by_author<'a>(
        &self,
        round: Round,
        author: PublicKey,
        dag: &'a Dag,
    ) -> Option<&'a Certificate> {
        dag.get(&round)
            .and_then(|by_authority| by_authority.get(&author))
            .map(|(_, certificate)| certificate)
    }

    pub(crate) fn embedded_qc_links(
        &self,
        child: &Certificate,
        parent: &Certificate,
        commit_round: Round,
    ) -> bool {
        let Some(qc) = child.header.qc.as_ref() else {
            return false;
        };
        let structural_ok = qc.target == parent.header.id
            && qc.round == parent.round()
            && qc.round < commit_round
            && qc.votes.iter().all(|vote| vote.voter_round < commit_round);
        if structural_ok {
            // 防御深度：QC 投票权重应在 Primary 层已验证 ≥ 2f+1。
            // debug_assert! 在 release 构建中被编译器移除，零运行时开销。
            // 空投票的 QC 来自合成 peer 证书（maybe_synthesize_peer_cert）
            // 或测试夹具，此时跳过权重检查。
            debug_assert!(
                qc.votes.is_empty()
                    || qc
                        .votes
                        .iter()
                        .map(|v| self.committee.stake(&v.author))
                        .sum::<Stake>()
                        >= self.committee.quorum_threshold(),
                "embedded QC lacks quorum weight"
            );
        }
        structural_ok
    }
}
