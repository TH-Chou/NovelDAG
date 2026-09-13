use crate::messages::Header;
use config::{Committee, DagProtocol, Stake};
use crypto::{Hash as _, PublicKey, SignatureService};
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;

const ATTACK_ENV: &str = "NOVELDAG_BYZANTINE_ATTACK";
const ADDRESSES_ENV: &str = "NOVELDAG_BYZANTINE_PRIMARY_ADDRS";
const VARIANTS_ENV: &str = "NOVELDAG_EQUIVOCATION_VARIANTS";

/// Process-local Byzantine behavior configured by the benchmark launcher.
///
/// Certified protocols keep one certifiable branch: all Byzantine replicas
/// and the minimum honest stake needed for a quorum receive the canonical
/// header. Every remaining honest replica receives one different branch.
/// Mahi-Mahi has no per-block certificate, so every honest replica receives
/// one distinct branch instead.
#[derive(Clone, Debug, Default)]
pub(crate) struct ByzantineConfig {
    equivocation: bool,
    variant_count: usize,
    byzantine_addresses: HashSet<SocketAddr>,
}

impl ByzantineConfig {
    pub(crate) fn from_env() -> Self {
        let equivocation = std::env::var(ATTACK_ENV).as_deref() == Ok("equivocation");
        if !equivocation {
            return Self::default();
        }

        let variant_count = std::env::var(VARIANTS_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(1);
        let byzantine_addresses = std::env::var(ADDRESSES_ENV)
            .unwrap_or_default()
            .split(',')
            .filter_map(|value| value.parse::<SocketAddr>().ok())
            .collect();

        Self {
            equivocation,
            variant_count,
            byzantine_addresses,
        }
    }

    pub(crate) fn is_equivocating(&self) -> bool {
        self.equivocation
    }

    pub(crate) fn allows_multiple_votes(&self) -> bool {
        self.equivocation
    }

    /// Equivocation traffic models syntactically valid blocks whose payload
    /// is invalid or duplicate at execution time. The payload remains on the
    /// wire so the attack still consumes normal data-plane resources.
    pub(crate) fn invalidates_payload(&self) -> bool {
        self.equivocation
    }

    #[cfg(test)]
    pub(crate) fn equivocation_for_test() -> Self {
        Self {
            equivocation: true,
            variant_count: 1,
            byzantine_addresses: HashSet::new(),
        }
    }

    /// Build all signed versions emitted for one proposal.
    ///
    /// Certified protocols retain the canonical header and make each branch
    /// payload-distinct by removing a different digest. This models branches
    /// that cannot obtain data availability without multiplying full worker
    /// traffic. Mahi-Mahi partitions the attacker's independently replicated
    /// batches across one version per honest recipient.
    pub(crate) async fn signed_attack_headers(
        &self,
        canonical: &Header,
        dag_protocol: DagProtocol,
        signature_service: &mut SignatureService,
    ) -> Vec<Header> {
        if !self.equivocation || canonical.round == 0 {
            return vec![canonical.clone()];
        }

        if dag_protocol.is_mahi_mahi() {
            let count = self.variant_count.max(1);
            let mut payloads = vec![BTreeMap::new(); count];
            for (index, (digest, worker_id)) in canonical.payload.iter().enumerate() {
                payloads[index % count].insert(digest.clone(), *worker_id);
            }

            let mut headers = Vec::with_capacity(count);
            for (index, payload) in payloads.into_iter().enumerate() {
                let mut variant = canonical.clone();
                variant.payload = payload;
                variant.equivocation_tag = (index + 1) as u64;
                variant.id = variant.digest();
                variant.signature = signature_service
                    .request_signature(variant.id.clone())
                    .await;
                headers.push(variant);
            }
            return headers;
        }

        let payload_digests = canonical.payload.keys().cloned().collect::<Vec<_>>();
        let mut headers = Vec::with_capacity(self.variant_count + 1);
        headers.push(canonical.clone());
        for tag in 1..=self.variant_count {
            let mut variant = canonical.clone();
            if let Some(digest) = payload_digests.get(tag - 1) {
                variant.payload.remove(digest);
            }
            variant.equivocation_tag = tag as u64;
            variant.id = variant.digest();
            variant.signature = signature_service
                .request_signature(variant.id.clone())
                .await;
            headers.push(variant);
        }
        headers
    }

    fn peers(
        &self,
        committee: &Committee,
        own_name: &PublicKey,
    ) -> (
        Vec<(PublicKey, Stake, SocketAddr)>,
        Vec<(PublicKey, Stake, SocketAddr)>,
    ) {
        let mut honest = Vec::new();
        let mut byzantine = Vec::new();
        for (name, authority) in committee.others_primaries(own_name) {
            let peer = (name, committee.stake(&name), authority.primary_to_primary);
            if self
                .byzantine_addresses
                .contains(&authority.primary_to_primary)
            {
                byzantine.push(peer);
            } else {
                honest.push(peer);
            }
        }
        honest.sort_by_key(|(name, _, _)| *name);
        byzantine.sort_by_key(|(name, _, _)| *name);
        (honest, byzantine)
    }

    fn canonical_honest_count(&self, committee: &Committee, own_name: &PublicKey) -> usize {
        let (honest, byzantine) = self.peers(committee, own_name);
        let mut stake =
            committee.stake(own_name) + byzantine.iter().map(|(_, stake, _)| *stake).sum::<Stake>();
        let mut count = 0;
        for (_, honest_stake, _) in honest {
            if stake >= committee.quorum_threshold() {
                break;
            }
            stake += honest_stake;
            count += 1;
        }
        count
    }

    /// Targets for the one certifiable branch of Narwhal, Shortfin, or Wahoo.
    pub(crate) fn canonical_targets(
        &self,
        committee: &Committee,
        own_name: &PublicKey,
    ) -> Vec<SocketAddr> {
        if !self.equivocation {
            return committee
                .others_primaries(own_name)
                .into_iter()
                .map(|(_, authority)| authority.primary_to_primary)
                .collect();
        }

        let (honest, byzantine) = self.peers(committee, own_name);
        let honest_count = self.canonical_honest_count(committee, own_name);
        byzantine
            .into_iter()
            .map(|(_, _, address)| address)
            .chain(
                honest
                    .into_iter()
                    .take(honest_count)
                    .map(|(_, _, address)| address),
            )
            .collect()
    }

    /// Targets for one conflicting branch.
    ///
    /// Every Byzantine peer receives every branch so it can endorse them.
    /// Exactly one honest peer receives a given branch, and no honest peer is
    /// selected for two directly transmitted branches in the same round.
    pub(crate) fn variant_targets(
        &self,
        committee: &Committee,
        own_name: &PublicKey,
        tag: u64,
        mahi_mahi: bool,
    ) -> Vec<SocketAddr> {
        if !self.equivocation || tag == 0 {
            return Vec::new();
        }

        let (honest, byzantine) = self.peers(committee, own_name);
        let first_honest = if mahi_mahi {
            0
        } else {
            self.canonical_honest_count(committee, own_name)
        };
        let honest_index = first_honest + tag as usize - 1;

        byzantine
            .into_iter()
            .map(|(_, _, address)| address)
            .chain(
                honest
                    .get(honest_index)
                    .map(|(_, _, address)| *address)
                    .into_iter(),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::{Digest, SignatureService};

    #[test]
    fn certified_routes_keep_a_quorum_and_never_double_send_to_honest_peers() {
        let committee = crate::common::committee();
        let names = committee.authorities.keys().cloned().collect::<Vec<_>>();
        let attacker = names[3];
        let config = ByzantineConfig {
            equivocation: true,
            variant_count: 1,
            byzantine_addresses: [committee.primary(&attacker).unwrap().primary_to_primary]
                .iter()
                .copied()
                .collect(),
        };

        let canonical = config.canonical_targets(&committee, &attacker);
        let first = config.variant_targets(&committee, &attacker, 1, false);
        let canonical_honest = canonical.iter().collect::<HashSet<_>>();
        let first_honest = first.iter().collect::<HashSet<_>>();
        assert_eq!(canonical_honest.len(), 2);
        assert_eq!(first_honest.len(), 1);
        assert!(canonical_honest.is_disjoint(&first_honest));
    }

    #[test]
    fn mahi_routes_one_distinct_version_to_each_honest_peer() {
        let committee = crate::common::committee();
        let names = committee.authorities.keys().cloned().collect::<Vec<_>>();
        let attacker = names[3];
        let config = ByzantineConfig {
            equivocation: true,
            variant_count: 3,
            byzantine_addresses: [committee.primary(&attacker).unwrap().primary_to_primary]
                .iter()
                .copied()
                .collect(),
        };

        let mut honest_targets = HashSet::new();
        for tag in 1..=3 {
            let targets = config.variant_targets(&committee, &attacker, tag, true);
            assert_eq!(targets.len(), 1);
            assert!(honest_targets.insert(targets[0]));
        }
        assert_eq!(honest_targets.len(), 3);
    }

    #[tokio::test]
    async fn mahi_headers_partition_payload_while_certified_headers_reuse_one_broadcast() {
        let mut keys = crate::common::keys();
        let (author, secret) = keys.pop().unwrap();
        let mut signature_service = SignatureService::new(secret);
        let payload = (1u8..=3)
            .map(|byte| (Digest([byte; 32]), 0))
            .collect::<BTreeMap<_, _>>();
        let canonical = Header {
            author,
            round: 1,
            payload,
            benchmark_invalid_payload: true,
            ..Header::default()
        };
        let mahi = ByzantineConfig {
            equivocation: true,
            variant_count: 3,
            byzantine_addresses: HashSet::new(),
        }
        .signed_attack_headers(&canonical, DagProtocol::MahiMahi5, &mut signature_service)
        .await;
        assert_eq!(mahi.len(), 3);
        assert!(mahi.iter().all(|header| header.payload.len() == 1));
        assert_eq!(
            mahi.iter()
                .flat_map(|header| header.payload.keys())
                .collect::<HashSet<_>>()
                .len(),
            3
        );

        let certified = ByzantineConfig {
            equivocation: true,
            variant_count: 1,
            byzantine_addresses: HashSet::new(),
        }
        .signed_attack_headers(&canonical, DagProtocol::Narwhal, &mut signature_service)
        .await;
        assert_eq!(certified.len(), 2);
        assert_eq!(certified[0].payload.len(), 3);
        assert_eq!(certified[1].payload.len(), 2);
    }
}
