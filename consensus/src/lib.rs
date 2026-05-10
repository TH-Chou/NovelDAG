// Copyright(C) Facebook, Inc. and its affiliates.
// Unified consensus router: dispatches to narwhal, bullshark, or noveldag based on dag_protocol.
use config::{Committee, ConsensusProtocol, DagProtocol, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

mod bullshark;
mod narwhal;
mod noveldag;
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
    /// The public key of this authority, used by NovelDAG for round-completion detection.
    pub(crate) name: PublicKey,

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
            DagProtocol::NovelDAG,
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
            Self {
                committee: committee.clone(),
                gc_depth,
                dag_protocol,
                consensus_protocol,
                name,
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
            DagProtocol::NovelDAG => noveldag::run(self).await,
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
            round
        }
    }

    pub(crate) fn common_coin(&self, round: Round, dag: &Dag) -> Option<Round> {
        let certificates = dag.get(&round)?;
        let weight: Stake = certificates
            .values()
            .map(|(_, certificate)| self.committee.stake(&certificate.origin()))
            .sum();
        if weight < self.committee.quorum_threshold() {
            return None;
        }

        let mut digests: Vec<_> = certificates
            .values()
            .map(|(digest, _)| digest.clone())
            .collect();
        digests.sort();

        let mut seed = round;
        for digest in digests {
            let mut chunk = [0u8; 8];
            chunk.copy_from_slice(&digest.0[..8]);
            seed ^= u64::from_le_bytes(chunk);
            seed = seed.rotate_left(13).wrapping_mul(0x9E37_79B1_85EB_CA87);
        }
        Some(seed)
    }

    /// Returns the certificate (and digest) originated by the leader (NovelDAG version: full committee).
    pub(crate) fn leader<'a>(
        &self,
        round: Round,
        coin_round: Round,
        dag: &'a Dag,
    ) -> Option<&'a (Digest, Certificate)> {
        let by_round = dag.get(&round)?;

        let leader = match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => {
                let coin = self.round_robin_coin(round);
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
            ConsensusProtocol::CommonCoin => {
                let coin = self
                    .common_coin(coin_round, dag)
                    .unwrap_or_else(|| self.round_robin_coin(round));
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
        };

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
        qc.target == parent.header.id
            && qc.round == parent.round()
            && qc.round < commit_round
            && qc.votes.iter().all(|vote| vote.voter_round < commit_round)
    }
}
