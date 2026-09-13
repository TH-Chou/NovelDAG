use crate::messages::Header;
use config::Committee;
use crypto::{Hash as _, PublicKey, SignatureService};
use std::collections::HashSet;
use std::net::SocketAddr;

const ATTACK_ENV: &str = "NOVELDAG_BYZANTINE_ATTACK";
const ADDRESSES_ENV: &str = "NOVELDAG_BYZANTINE_PRIMARY_ADDRS";
const VARIANTS_ENV: &str = "NOVELDAG_EQUIVOCATION_VARIANTS";

/// Process-local Byzantine behavior configured by the benchmark launcher.
///
/// Equivocating authorities send one canonical block to every peer so their
/// normal chain can advance. They additionally send one signed conflicting
/// variant to each honest peer and every variant to Byzantine peers. This
/// maximizes distinct validation work without weakening protocol quorums.
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

    pub(crate) async fn signed_variants(
        &self,
        canonical: &Header,
        signature_service: &mut SignatureService,
    ) -> Vec<Header> {
        if !self.equivocation || canonical.round == 0 {
            return Vec::new();
        }

        let mut variants = Vec::with_capacity(self.variant_count);
        for tag in 1..=self.variant_count {
            let mut variant = canonical.clone();
            variant.equivocation_tag = tag as u64;
            variant.id = variant.digest();
            variant.signature = signature_service
                .request_signature(variant.id.clone())
                .await;
            variants.push(variant);
        }
        variants
    }

    /// Return the peers that should receive one conflicting variant.
    ///
    /// Each variant goes to one honest peer (round-robin by tag), while all
    /// other Byzantine authorities receive every variant so they can endorse
    /// it. The canonical block is broadcast separately to all peers first.
    pub(crate) fn variant_targets(
        &self,
        committee: &Committee,
        own_name: &PublicKey,
        tag: u64,
    ) -> Vec<SocketAddr> {
        if !self.equivocation {
            return Vec::new();
        }

        let mut honest = Vec::new();
        let mut byzantine = Vec::new();
        for (_, authority) in committee.others_primaries(own_name) {
            let address = authority.primary_to_primary;
            if self.byzantine_addresses.contains(&address) {
                byzantine.push(address);
            } else {
                honest.push(address);
            }
        }
        honest.sort_unstable();
        byzantine.sort_unstable();

        let mut targets = byzantine;
        if !honest.is_empty() {
            let index = tag.saturating_sub(1) as usize % honest.len();
            targets.push(honest[index]);
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_reach_distinct_honest_peers_and_all_byzantine_peers() {
        let committee = crate::common::committee();
        let mut names: Vec<_> = committee.authorities.keys().cloned().collect();
        names.sort();
        let attacker = names[3];
        let other_byzantine = names[2];
        let other_byzantine_address = committee
            .primary(&other_byzantine)
            .unwrap()
            .primary_to_primary;
        let config = ByzantineConfig {
            equivocation: true,
            variant_count: 2,
            byzantine_addresses: [
                committee.primary(&attacker).unwrap().primary_to_primary,
                other_byzantine_address,
            ]
            .iter()
            .copied()
            .collect(),
        };

        let first = config.variant_targets(&committee, &attacker, 1);
        let second = config.variant_targets(&committee, &attacker, 2);
        assert!(first.contains(&other_byzantine_address));
        assert!(second.contains(&other_byzantine_address));

        let first_honest: Vec<_> = first
            .iter()
            .filter(|address| **address != other_byzantine_address)
            .collect();
        let second_honest: Vec<_> = second
            .iter()
            .filter(|address| **address != other_byzantine_address)
            .collect();
        assert_eq!(first_honest.len(), 1);
        assert_eq!(second_honest.len(), 1);
        assert_ne!(first_honest, second_honest);
    }
}
