// Mahi-Mahi consensus over NovelDAG's uncertified DAG substrate.
use crate::Consensus;
use config::{Committee, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use log::{debug, info, warn};
use primary::{Certificate, Round};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

const LEADER_COUNT: usize = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SlotId {
    round: Round,
    index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LeaderDecision {
    Commit(Digest),
    Skip,
    Undecided,
}

impl LeaderDecision {
    fn is_decided(&self) -> bool {
        !matches!(self, Self::Undecided)
    }
}

#[derive(Clone, Debug)]
struct LeaderSlot {
    id: SlotId,
    authority: PublicKey,
}

#[derive(Clone, Debug)]
struct DecisionRecord {
    slot: LeaderSlot,
    decision: LeaderDecision,
}

/// Mahi-Mahi must retain equivocations instead of overwriting an
/// `(author, round)` entry. The secondary indexes also keep all recursive DAG
/// queries deterministic and avoid repeatedly scanning complete rounds.
struct MahiDag {
    by_round: BTreeMap<Round, BTreeMap<PublicKey, BTreeSet<Digest>>>,
    by_digest: HashMap<Digest, Certificate>,
    support_cache: HashMap<(Digest, PublicKey, Round), Option<Digest>>,
    certificate_cache: HashMap<(Digest, Digest), bool>,
    link_cache: HashMap<(Digest, Digest), bool>,
}

impl MahiDag {
    fn new(genesis: Vec<Certificate>) -> Self {
        let mut dag = Self {
            by_round: BTreeMap::new(),
            by_digest: HashMap::new(),
            support_cache: HashMap::new(),
            certificate_cache: HashMap::new(),
            link_cache: HashMap::new(),
        };
        for certificate in genesis {
            dag.insert(certificate);
        }
        dag
    }

    fn insert(&mut self, certificate: Certificate) -> bool {
        let digest = certificate.digest();
        if self.by_digest.contains_key(&digest) {
            return false;
        }
        self.by_round
            .entry(certificate.round())
            .or_default()
            .entry(certificate.origin())
            .or_default()
            .insert(digest.clone());
        self.by_digest.insert(digest, certificate);
        true
    }

    fn block(&self, digest: &Digest) -> Option<&Certificate> {
        self.by_digest.get(digest)
    }

    fn highest_round(&self) -> Round {
        self.by_round
            .keys()
            .next_back()
            .copied()
            .unwrap_or_default()
    }

    fn round_digests(&self, round: Round) -> Vec<Digest> {
        self.by_round
            .get(&round)
            .into_iter()
            .flat_map(|by_author| by_author.values())
            .flat_map(|digests| digests.iter().cloned())
            .collect()
    }

    fn slot_digests(&self, authority: PublicKey, round: Round) -> Vec<Digest> {
        self.by_round
            .get(&round)
            .and_then(|by_author| by_author.get(&authority))
            .map(|digests| digests.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn round_certificates(&self, round: Round) -> Vec<&Certificate> {
        self.by_round
            .get(&round)
            .into_iter()
            .flat_map(|by_author| by_author.values())
            .flat_map(|digests| digests.iter())
            .filter_map(|digest| self.block(digest))
            .collect()
    }

    /// Return the first block at `(authority, round)` encountered by a
    /// deterministic depth-first traversal. The outer `None` means referenced
    /// history is incomplete; that result is deliberately not cached.
    fn find_support(
        &mut self,
        from: &Digest,
        authority: PublicKey,
        round: Round,
    ) -> Option<Option<Digest>> {
        let mut visiting = HashSet::new();
        self.find_support_inner(from, authority, round, &mut visiting)
    }

    fn find_support_inner(
        &mut self,
        from: &Digest,
        authority: PublicKey,
        round: Round,
        visiting: &mut HashSet<Digest>,
    ) -> Option<Option<Digest>> {
        let key = (from.clone(), authority, round);
        if let Some(cached) = self.support_cache.get(&key) {
            return Some(cached.clone());
        }
        if !visiting.insert(from.clone()) {
            return None;
        }

        let (from_round, parents) = match self.block(from) {
            Some(block) => (block.round(), block.header.parents.clone()),
            None => {
                visiting.remove(from);
                return None;
            }
        };
        if from_round <= round {
            visiting.remove(from);
            self.support_cache.insert(key, None);
            return Some(None);
        }

        for parent in parents {
            let (parent_round, parent_author) = match self.block(&parent) {
                Some(block) => (block.round(), block.origin()),
                None => {
                    visiting.remove(from);
                    return None;
                }
            };
            if parent_round < round {
                continue;
            }
            if parent_round == round && parent_author == authority {
                visiting.remove(from);
                self.support_cache.insert(key, Some(parent.clone()));
                return Some(Some(parent));
            }
            if parent_round > round {
                match self.find_support_inner(&parent, authority, round, visiting) {
                    Some(Some(support)) => {
                        visiting.remove(from);
                        self.support_cache.insert(key, Some(support.clone()));
                        return Some(Some(support));
                    }
                    Some(None) => {}
                    None => {
                        visiting.remove(from);
                        return None;
                    }
                }
            }
        }

        visiting.remove(from);
        self.support_cache.insert(key, None);
        Some(None)
    }

    fn is_vote(&mut self, vote: &Digest, leader: &Digest) -> Option<bool> {
        let (authority, round) = self
            .block(leader)
            .map(|block| (block.origin(), block.round()))?;
        self.find_support(vote, authority, round)
            .map(|support| support.as_ref() == Some(leader))
    }

    /// A decision-round block certifies a leader when its direct strong links
    /// contain a quorum of distinct-author vote-round blocks supporting it.
    fn is_certificate(
        &mut self,
        certificate: &Digest,
        leader: &Digest,
        committee: &Committee,
    ) -> Option<bool> {
        let key = (certificate.clone(), leader.clone());
        if let Some(cached) = self.certificate_cache.get(&key) {
            return Some(*cached);
        }
        let (certificate_round, parents) = self
            .block(certificate)
            .map(|block| (block.round(), block.header.parents.clone()))?;
        let vote_round = certificate_round.saturating_sub(1);
        let mut voters = HashSet::new();
        let mut stake: Stake = 0;
        let mut incomplete = false;

        for parent in parents {
            let (parent_round, parent_author) = match self.block(&parent) {
                Some(block) => (block.round(), block.origin()),
                None => {
                    incomplete = true;
                    continue;
                }
            };
            if parent_round != vote_round || voters.contains(&parent_author) {
                continue;
            }
            match self.is_vote(&parent, leader) {
                Some(true) => {
                    voters.insert(parent_author);
                    stake += committee.stake(&parent_author);
                    if stake >= committee.quorum_threshold() {
                        self.certificate_cache.insert(key, true);
                        return Some(true);
                    }
                }
                Some(false) => {}
                None => incomplete = true,
            }
        }

        if incomplete {
            None
        } else {
            self.certificate_cache.insert(key, false);
            Some(false)
        }
    }

    /// Reachability over both strong and weak links. A false result is cached
    /// only after every relevant referenced block is locally available.
    fn linked(&mut self, from: &Digest, target: &Digest) -> Option<bool> {
        let mut visiting = HashSet::new();
        self.linked_inner(from, target, &mut visiting)
    }

    fn linked_inner(
        &mut self,
        from: &Digest,
        target: &Digest,
        visiting: &mut HashSet<Digest>,
    ) -> Option<bool> {
        if from == target {
            return Some(true);
        }
        let key = (from.clone(), target.clone());
        if let Some(cached) = self.link_cache.get(&key) {
            return Some(*cached);
        }
        if !visiting.insert(from.clone()) {
            return None;
        }
        let target_round = self.block(target)?.round();
        let (from_round, parents) = match self.block(from) {
            Some(block) => (block.round(), block.header.parents.clone()),
            None => {
                visiting.remove(from);
                return None;
            }
        };
        if from_round <= target_round {
            visiting.remove(from);
            self.link_cache.insert(key, false);
            return Some(false);
        }

        let mut incomplete = false;
        for parent in parents {
            let parent_round = match self.block(&parent) {
                Some(block) => block.round(),
                None => {
                    incomplete = true;
                    continue;
                }
            };
            if parent_round < target_round {
                continue;
            }
            match self.linked_inner(&parent, target, visiting) {
                Some(true) => {
                    visiting.remove(from);
                    self.link_cache.insert(key, true);
                    return Some(true);
                }
                Some(false) => {}
                None => incomplete = true,
            }
        }

        visiting.remove(from);
        if incomplete {
            None
        } else {
            self.link_cache.insert(key, false);
            Some(false)
        }
    }

    fn prune_caches(&mut self, min_root_round: Round) {
        let blocks = &self.by_digest;
        self.support_cache.retain(|(root, _, _), _| {
            blocks
                .get(root)
                .map_or(false, |block| block.round() >= min_root_round)
        });
        self.certificate_cache.retain(|(root, _), _| {
            blocks
                .get(root)
                .map_or(false, |block| block.round() >= min_root_round)
        });
        self.link_cache.retain(|(root, _), _| {
            blocks
                .get(root)
                .map_or(false, |block| block.round() >= min_root_round)
        });
    }
}

struct MahiCommitter {
    dag: MahiDag,
    wave_length: Round,
    decisions: BTreeMap<SlotId, LeaderDecision>,
    last_decided: Option<SlotId>,
    emitted: HashSet<Digest>,
}

impl MahiCommitter {
    fn new(genesis: Vec<Certificate>, wave_length: Round) -> Self {
        let emitted = genesis
            .iter()
            .map(|certificate| certificate.digest())
            .collect();
        Self {
            dag: MahiDag::new(genesis),
            wave_length,
            decisions: BTreeMap::new(),
            last_decided: None,
            emitted,
        }
    }

    fn insert(&mut self, certificate: Certificate) -> bool {
        self.dag.insert(certificate)
    }

    fn direct_decision(&mut self, slot: &LeaderSlot, committee: &Committee) -> LeaderDecision {
        let candidates = self.dag.slot_digests(slot.authority, slot.id.round);
        let decision_round = slot.id.round + self.wave_length - 1;
        let decision_blocks = self.dag.round_digests(decision_round);
        let mut supported = Vec::new();

        // Commit evidence is checked before skip evidence. This slot-level
        // ordering avoids candidate-by-candidate equivocation races.
        for candidate in &candidates {
            let mut authors = HashSet::new();
            let mut stake: Stake = 0;
            for decision_block in &decision_blocks {
                if self
                    .dag
                    .is_certificate(decision_block, candidate, committee)
                    == Some(true)
                {
                    let author = self
                        .dag
                        .block(decision_block)
                        .expect("indexed decision block")
                        .origin();
                    if authors.insert(author) {
                        stake += committee.stake(&author);
                    }
                    if stake >= committee.quorum_threshold() {
                        supported.push(candidate.clone());
                        break;
                    }
                }
            }
        }

        assert!(
            supported.len() <= 1,
            "Mahi-Mahi safety violation: multiple directly certified leaders in one slot"
        );
        if let Some(candidate) = supported.pop() {
            return LeaderDecision::Commit(candidate);
        }

        let vote_round = slot.id.round + self.wave_length - 2;
        let vote_blocks = self.dag.round_digests(vote_round);
        let mut non_voters = HashSet::new();
        let mut non_vote_stake: Stake = 0;
        for vote_block in vote_blocks {
            if self
                .dag
                .find_support(&vote_block, slot.authority, slot.id.round)
                == Some(None)
            {
                let author = self
                    .dag
                    .block(&vote_block)
                    .expect("indexed vote block")
                    .origin();
                if non_voters.insert(author) {
                    non_vote_stake += committee.stake(&author);
                    if non_vote_stake >= committee.quorum_threshold() {
                        return LeaderDecision::Skip;
                    }
                }
            }
        }

        LeaderDecision::Undecided
    }

    fn indirect_decision(
        &mut self,
        slot: &LeaderSlot,
        later: &VecDeque<DecisionRecord>,
        committee: &Committee,
    ) -> LeaderDecision {
        let decision_round = slot.id.round + self.wave_length - 1;
        for record in later
            .iter()
            .filter(|record| record.slot.id.round > decision_round)
        {
            match &record.decision {
                LeaderDecision::Skip => continue,
                LeaderDecision::Undecided => return LeaderDecision::Undecided,
                LeaderDecision::Commit(anchor) => {
                    return self.decide_from_anchor(slot, anchor, committee)
                }
            }
        }
        LeaderDecision::Undecided
    }

    fn decide_from_anchor(
        &mut self,
        slot: &LeaderSlot,
        anchor: &Digest,
        committee: &Committee,
    ) -> LeaderDecision {
        let candidates = self.dag.slot_digests(slot.authority, slot.id.round);
        let decision_round = slot.id.round + self.wave_length - 1;
        let potential_certificates = self.dag.round_digests(decision_round);
        let mut certified = Vec::new();
        let mut incomplete = false;

        for candidate in candidates {
            let mut candidate_certified = false;
            for certificate in &potential_certificates {
                match self.dag.linked(anchor, certificate) {
                    Some(true) => match self.dag.is_certificate(certificate, &candidate, committee)
                    {
                        Some(true) => {
                            candidate_certified = true;
                            break;
                        }
                        Some(false) => {}
                        None => incomplete = true,
                    },
                    Some(false) => {}
                    None => incomplete = true,
                }
            }
            if candidate_certified {
                certified.push(candidate);
            }
        }

        assert!(
            certified.len() <= 1,
            "Mahi-Mahi safety violation: anchor links multiple certified leaders in one slot"
        );
        if let Some(candidate) = certified.pop() {
            LeaderDecision::Commit(candidate)
        } else if incomplete {
            LeaderDecision::Undecided
        } else {
            LeaderDecision::Skip
        }
    }

    fn pending_slots(&self, consensus: &Consensus) -> Vec<LeaderSlot> {
        let highest = self.dag.highest_round();
        if highest + 1 < self.wave_length {
            return Vec::new();
        }
        let max_leader_round = highest - (self.wave_length - 1);
        let first = match self.last_decided {
            Some(slot) if slot.index + 1 < LEADER_COUNT => SlotId {
                round: slot.round,
                index: slot.index + 1,
            },
            Some(slot) => SlotId {
                round: slot.round + 1,
                index: 0,
            },
            None => SlotId { round: 1, index: 0 },
        };
        let mut slots = Vec::new();
        for round in first.round..=max_leader_round {
            let coin_round = round + self.wave_length - 1;
            let coin_blocks = self.dag.round_certificates(coin_round);
            let first_index = if round == first.round { first.index } else { 0 };
            for index in first_index..LEADER_COUNT {
                let Some(authority) = consensus.leader_authority_from_certificates(
                    round,
                    coin_round,
                    index,
                    &coin_blocks,
                ) else {
                    return slots;
                };
                slots.push(LeaderSlot {
                    id: SlotId { round, index },
                    authority,
                });
            }
        }
        slots
    }

    fn try_decide(&mut self, consensus: &Consensus) -> Vec<Certificate> {
        let slots = self.pending_slots(consensus);
        let mut records = VecDeque::new();

        for slot in slots.into_iter().rev() {
            let mut decision = self
                .decisions
                .get(&slot.id)
                .cloned()
                .unwrap_or(LeaderDecision::Undecided);
            if !decision.is_decided() {
                decision = self.direct_decision(&slot, &consensus.committee);
            }
            if !decision.is_decided() {
                decision = self.indirect_decision(&slot, &records, &consensus.committee);
            }
            if decision.is_decided() {
                self.decisions.insert(slot.id, decision.clone());
            }
            records.push_front(DecisionRecord { slot, decision });
        }

        let mut sequence = Vec::new();
        for record in records {
            if !record.decision.is_decided() {
                break;
            }
            self.last_decided = Some(record.slot.id);
            if let LeaderDecision::Commit(leader) = record.decision {
                sequence.extend(self.linearize(&leader));
            }
        }

        if let Some(last) = self.last_decided {
            let min_root_round = last.round.saturating_sub(consensus.gc_depth);
            self.dag.prune_caches(min_root_round);
        }
        sequence
    }

    fn linearize(&mut self, leader: &Digest) -> Vec<Certificate> {
        let mut reachable = HashSet::new();
        let mut stack = vec![leader.clone()];
        while let Some(digest) = stack.pop() {
            if self.emitted.contains(&digest) || !reachable.insert(digest.clone()) {
                continue;
            }
            let Some(block) = self.dag.block(&digest) else {
                continue;
            };
            stack.extend(block.header.parents.iter().cloned());
        }

        let mut ordered = reachable.into_iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            let left_block = self.dag.block(left).expect("reachable block is indexed");
            let right_block = self.dag.block(right).expect("reachable block is indexed");
            (left_block.round(), left_block.origin(), left).cmp(&(
                right_block.round(),
                right_block.origin(),
                right,
            ))
        });
        ordered
            .into_iter()
            .filter_map(|digest| {
                let block = self.dag.block(&digest)?.clone();
                self.emitted.insert(digest);
                Some(block)
            })
            .collect()
    }
}

pub(crate) async fn run(consensus: &mut Consensus) {
    let wave_length = consensus.dag_protocol.mahi_mahi_wave_length().unwrap_or(5);
    let mut committer = MahiCommitter::new(consensus.genesis.clone(), wave_length);

    while let Some(certificate) = consensus.rx_primary.recv().await {
        debug!("Processing {:?}", certificate);
        if !committer.insert(certificate) {
            continue;
        }

        for certificate in committer.try_decide(consensus) {
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

            if let Err(error) = consensus.tx_output.send(certificate).await {
                warn!("Failed to output certificate: {}", error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus_tests::mock_committee;
    use primary::Header;

    fn authorities(committee: &Committee) -> Vec<PublicKey> {
        committee.authorities.keys().copied().collect()
    }

    fn block(author: PublicKey, round: Round, salt: u8, parents: &[Digest]) -> Certificate {
        let mut id = [0u8; 32];
        id[..8].copy_from_slice(&round.to_le_bytes());
        id[8..16].copy_from_slice(&author.0[..8]);
        id[31] = salt;
        Certificate {
            header: Header {
                author,
                round,
                parents: parents.iter().cloned().collect(),
                id: Digest(id),
                ..Header::default()
            },
            votes: Vec::new(),
        }
    }

    fn insert_full_round(
        dag: &mut MahiDag,
        authorities: &[PublicKey],
        round: Round,
        parents: &[Digest],
    ) -> Vec<Digest> {
        authorities
            .iter()
            .enumerate()
            .map(|(index, author)| {
                let certificate = block(*author, round, index as u8, parents);
                let digest = certificate.digest();
                dag.insert(certificate);
                digest
            })
            .collect()
    }

    #[test]
    fn is_vote_selects_one_equivocation_deterministically() {
        let committee = mock_committee();
        let authors = authorities(&committee);
        let mut dag = MahiDag::new(Certificate::genesis(&committee));
        let first = block(authors[0], 1, 1, &[]);
        let second = block(authors[0], 1, 2, &[]);
        let first_digest = first.digest();
        let second_digest = second.digest();
        dag.insert(first);
        dag.insert(second);
        let mut parents = vec![first_digest.clone(), second_digest.clone()];
        parents.sort();
        let vote = block(authors[1], 2, 0, &parents);
        let vote_digest = vote.digest();
        dag.insert(vote);

        let selected = parents[0].clone();
        let rejected = parents[1].clone();
        assert_eq!(dag.is_vote(&vote_digest, &selected), Some(true));
        assert_eq!(dag.is_vote(&vote_digest, &rejected), Some(false));
    }

    #[test]
    fn incomplete_support_is_retried_after_dependency_arrives() {
        let committee = mock_committee();
        let authors = authorities(&committee);
        let mut dag = MahiDag::new(Certificate::genesis(&committee));
        let leader = block(authors[0], 1, 0, &[]);
        let leader_digest = leader.digest();
        let vote = block(authors[1], 2, 0, std::slice::from_ref(&leader_digest));
        let vote_digest = vote.digest();
        dag.insert(vote);

        assert_eq!(dag.find_support(&vote_digest, authors[0], 1), None);
        dag.insert(leader);
        assert_eq!(
            dag.find_support(&vote_digest, authors[0], 1),
            Some(Some(leader_digest))
        );
    }

    #[test]
    fn certificate_requires_distinct_vote_authors() {
        let committee = mock_committee();
        let authors = authorities(&committee);
        let mut dag = MahiDag::new(Certificate::genesis(&committee));
        let leader = block(authors[0], 1, 0, &[]);
        let leader_digest = leader.digest();
        dag.insert(leader);

        let duplicate_votes = (0..3)
            .map(|salt| {
                let vote = block(authors[1], 2, salt, std::slice::from_ref(&leader_digest));
                let digest = vote.digest();
                dag.insert(vote);
                digest
            })
            .collect::<Vec<_>>();
        let certificate = block(authors[2], 3, 0, &duplicate_votes);
        let certificate_digest = certificate.digest();
        dag.insert(certificate);
        assert_eq!(
            dag.is_certificate(&certificate_digest, &leader_digest, &committee),
            Some(false)
        );

        let distinct_votes = authors[0..3]
            .iter()
            .enumerate()
            .map(|(index, author)| {
                let vote = block(
                    *author,
                    2,
                    (index + 10) as u8,
                    std::slice::from_ref(&leader_digest),
                );
                let digest = vote.digest();
                dag.insert(vote);
                digest
            })
            .collect::<Vec<_>>();
        let certificate = block(authors[3], 3, 1, &distinct_votes);
        let certificate_digest = certificate.digest();
        dag.insert(certificate);
        assert_eq!(
            dag.is_certificate(&certificate_digest, &leader_digest, &committee),
            Some(true)
        );
    }

    #[test]
    fn direct_commit_and_slot_skip() {
        let committee = mock_committee();
        let authors = authorities(&committee);
        let mut committer = MahiCommitter::new(Certificate::genesis(&committee), 5);
        let leader = block(authors[0], 1, 0, &[]);
        let leader_digest = leader.digest();
        committer.insert(leader);
        let mut parents = vec![leader_digest.clone()];
        for round in 2..=3 {
            parents = insert_full_round(&mut committer.dag, &authors, round, &parents);
        }
        let votes = insert_full_round(&mut committer.dag, &authors, 4, &parents);
        insert_full_round(&mut committer.dag, &authors, 5, &votes);
        let slot = LeaderSlot {
            id: SlotId { round: 1, index: 0 },
            authority: authors[0],
        };
        assert_eq!(
            committer.direct_decision(&slot, &committee),
            LeaderDecision::Commit(leader_digest)
        );

        let absent_slot = LeaderSlot {
            id: SlotId { round: 1, index: 1 },
            authority: authors[1],
        };
        assert_eq!(
            committer.direct_decision(&absent_slot, &committee),
            LeaderDecision::Skip
        );
    }

    fn indirect_fixture(
        anchor_links_certificate: bool,
    ) -> (MahiCommitter, Committee, LeaderSlot, Digest) {
        let committee = mock_committee();
        let authors = authorities(&committee);
        let mut committer = MahiCommitter::new(Certificate::genesis(&committee), 5);
        let leader = block(authors[0], 1, 0, &[]);
        let leader_digest = leader.digest();
        committer.insert(leader);
        let votes = authors[0..3]
            .iter()
            .enumerate()
            .map(|(index, author)| {
                let vote = block(
                    *author,
                    4,
                    index as u8,
                    std::slice::from_ref(&leader_digest),
                );
                let digest = vote.digest();
                committer.insert(vote);
                digest
            })
            .collect::<Vec<_>>();
        let certificate = block(authors[0], 5, 0, &votes);
        let certificate_digest = certificate.digest();
        committer.insert(certificate);
        let anchor_parents = if anchor_links_certificate {
            vec![certificate_digest]
        } else {
            Vec::new()
        };
        let anchor = block(authors[1], 6, 0, &anchor_parents);
        let anchor_digest = anchor.digest();
        committer.insert(anchor);
        (
            committer,
            committee,
            LeaderSlot {
                id: SlotId { round: 1, index: 0 },
                authority: authors[0],
            },
            anchor_digest,
        )
    }

    #[test]
    fn indirect_commit_uses_certificate_in_anchor_history() {
        let (mut committer, committee, slot, anchor) = indirect_fixture(true);
        assert!(matches!(
            committer.decide_from_anchor(&slot, &anchor, &committee),
            LeaderDecision::Commit(_)
        ));
    }

    #[test]
    fn indirect_skip_when_anchor_has_no_certified_link() {
        let (mut committer, committee, slot, anchor) = indirect_fixture(false);
        assert_eq!(
            committer.decide_from_anchor(&slot, &anchor, &committee),
            LeaderDecision::Skip
        );
    }

    #[test]
    fn decided_prefix_stops_at_first_undecided_slot() {
        let records = vec![
            (SlotId { round: 1, index: 0 }, LeaderDecision::Skip),
            (SlotId { round: 1, index: 1 }, LeaderDecision::Undecided),
            (SlotId { round: 2, index: 0 }, LeaderDecision::Skip),
        ];
        let decided = records
            .into_iter()
            .take_while(|(_, decision)| decision.is_decided())
            .map(|(slot, _)| slot)
            .collect::<Vec<_>>();
        assert_eq!(decided, vec![SlotId { round: 1, index: 0 }]);
    }
}
