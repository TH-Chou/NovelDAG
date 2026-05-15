// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote, WahooTag, WahooVotePhase};
use config::{Committee, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use std::collections::HashSet;
 
/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<Vote>,
    used: HashSet<PublicKey>,
}
 
impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }
 
    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push(vote);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

/// Aggregate certificates (by digest) and check if we reach a quorum.
/// Used by Narwhal mode. Weight is reset on each quorum hit.
pub struct CertificatesAggregator {
    weight: Stake,
    certificates: Vec<Digest>,
    used: HashSet<PublicKey>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> DagResult<Option<Vec<Digest>>> {
        let origin = certificate.origin();

        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate.digest());
        self.weight += committee.stake(&origin);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }
}

/// Aggregate certificates (full values) and check if we reach a quorum.
/// Used by Bullshark mode. Weight is NOT reset on quorum hit
/// (original Bullshark behavioral quirk preserved).
pub struct CertificatesVecAggregator {
    weight: Stake,
    certificates: Vec<Certificate>,
    used: HashSet<PublicKey>,
}

impl CertificatesVecAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> DagResult<Option<Vec<Certificate>>> {
        let origin = certificate.origin();

        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate);
        self.weight += committee.stake(&origin);
        if self.weight >= committee.quorum_threshold() {
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }
}

// =============================================================================
// Wahoo dual-track vote aggregation (Phase B).
//
// Wahoo's EPBC runs three concurrent quorums over the SAME block hash:
//   * `Ts1` quorum, 2f+1 → σ_S1, leading to a TS1-tier certificate.
//   * `Ts2` quorum, 2f+1 → σ_S2, upgrading the cert to TS2-tier.
//   * `Tf`  quorum, n     → σ_F,  fast-path TF-tier certificate.
// Whichever fires first dictates the cert tier the local node sees; later
// phases may "upgrade" the tier (TS1 → TS2 → TF).
//
// We model this as a 3-bucket aggregator. PBC rounds reuse `Pbc` which
// behaves like the standard `VotesAggregator` (single 2f+1 quorum).
// =============================================================================

/// Output of `WahooVotesAggregator::append`. At most one variant is emitted
/// for any given (header, phase) tuple; the aggregator latches on emission so
/// subsequent votes for the same phase don't re-emit.
//
// Allow `dead_code` until Phase B step 3 wires this aggregator into `Core`.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum WahooQuorum {
    /// Slow-path tier 1 cert (σ_S1 collected: 2f+1 `Ts1` votes).
    Ts1(Certificate),
    /// Slow-path tier 2 cert (σ_S2 collected: 2f+1 `Ts2` votes).
    Ts2(Certificate),
    /// Fast-path tier   cert (σ_F  collected: n     `Tf`  votes).
    Tf(Certificate),
    /// PBC cert (2f+1 `Pbc` votes), used by the PBC half of every wave.
    Pbc(Certificate),
}

#[allow(dead_code)]
impl WahooQuorum {
    pub fn tag(&self) -> WahooTag {
        match self {
            WahooQuorum::Ts1(_) => WahooTag::EpbcTs1,
            WahooQuorum::Ts2(_) => WahooTag::EpbcTs2,
            WahooQuorum::Tf(_) => WahooTag::EpbcTf,
            WahooQuorum::Pbc(_) => WahooTag::Pbc,
        }
    }

    pub fn into_inner(self) -> Certificate {
        match self {
            WahooQuorum::Ts1(c)
            | WahooQuorum::Ts2(c)
            | WahooQuorum::Tf(c)
            | WahooQuorum::Pbc(c) => c,
        }
    }
}

/// Per-phase vote bucket: dedup by `vote.author`, sum stake, emit on
/// threshold. Each bucket latches `done` after firing so duplicate
/// late-arriving votes are silently ignored.
#[allow(dead_code)]
#[derive(Default)]
struct PhaseBucket {
    weight: Stake,
    votes: Vec<Vote>,
    used: HashSet<PublicKey>,
    done: bool,
}

#[allow(dead_code)]
impl PhaseBucket {
    /// Returns `Some(votes_snapshot)` exactly once, when the bucket first
    /// crosses `threshold`. Subsequent calls return `None`.
    fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        threshold: Stake,
    ) -> DagResult<Option<Vec<Vote>>> {
        if self.done {
            return Ok(None);
        }
        let author = vote.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
        self.votes.push(vote);
        self.weight += committee.stake(&author);
        if self.weight >= threshold {
            self.done = true;
            return Ok(Some(self.votes.clone()));
        }
        Ok(None)
    }
}

#[allow(dead_code)]
pub struct WahooVotesAggregator {
    ts1: PhaseBucket,
    ts2: PhaseBucket,
    tf: PhaseBucket,
    pbc: PhaseBucket,
}

#[allow(dead_code)]
impl WahooVotesAggregator {
    pub fn new() -> Self {
        Self {
            ts1: PhaseBucket::default(),
            ts2: PhaseBucket::default(),
            tf: PhaseBucket::default(),
            pbc: PhaseBucket::default(),
        }
    }

    /// Append a Wahoo vote and return a quorum certificate if its phase just
    /// crossed its threshold.
    ///
    /// `committee.total_stake()` is computed by summing every authority's
    /// stake; for Wahoo's `Tf` (fast path) we treat this as the n-of-n
    /// threshold (no Byzantine fault tolerance — every node must sign).
    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<WahooQuorum>> {
        let phase = vote
            .wahoo_phase
            .ok_or_else(|| DagError::MalformedHeader(vote.id.clone()))?;
        let quorum = committee.quorum_threshold();
        let total: Stake = committee
            .authorities
            .keys()
            .map(|name| committee.stake(name))
            .sum();

        let votes_opt = match phase {
            WahooVotePhase::Ts1 => self.ts1.append(vote, committee, quorum)?,
            WahooVotePhase::Ts2 => self.ts2.append(vote, committee, quorum)?,
            WahooVotePhase::Tf => self.tf.append(vote, committee, total)?,
            WahooVotePhase::Pbc => self.pbc.append(vote, committee, quorum)?,
        };

        Ok(votes_opt.map(|votes| {
            let cert = Certificate {
                header: header.clone(),
                votes,
            };
            match phase {
                WahooVotePhase::Ts1 => WahooQuorum::Ts1(cert),
                WahooVotePhase::Ts2 => WahooQuorum::Ts2(cert),
                WahooVotePhase::Tf => WahooQuorum::Tf(cert),
                WahooVotePhase::Pbc => WahooQuorum::Pbc(cert),
            }
        }))
    }

    /// Returns true if the TF (fast-path n-of-n) quorum has already fired.
    /// The Wahoo core can use this to skip emitting the TS1/TS2 slow-path
    /// certs once a strictly stronger TF cert is in flight.
    pub fn tf_done(&self) -> bool {
        self.tf.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Header;
    use crypto::PublicKey;

    fn dummy_vote(author: PublicKey, phase: WahooVotePhase) -> Vote {
        Vote {
            id: Digest::default(),
            round: 1,
            voter_round: 1,
            origin: PublicKey::default(),
            author,
            wahoo_phase: Some(phase),
            signature: Default::default(),
        }
    }

    // NB: a full multi-authority test fixture lives in `tests/core_tests.rs`
    // and is exercised once Phase B step 2 wires the aggregator into Core.
    // This unit test just verifies the AuthorityReuse guard and the latch.
    #[test]
    fn duplicate_authority_rejected() {
        let header = Header::default();
        let mut agg = WahooVotesAggregator::new();
        let committee = crate::common::committee();
        let author = *committee.authorities.keys().next().unwrap();
        let v1 = dummy_vote(author, WahooVotePhase::Pbc);
        let v2 = dummy_vote(author, WahooVotePhase::Pbc);
        assert!(agg.append(v1, &committee, &header).is_ok());
        assert!(matches!(
            agg.append(v2, &committee, &header),
            Err(DagError::AuthorityReuse(_))
        ));
    }
}
