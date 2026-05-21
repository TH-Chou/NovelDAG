// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::primary::Round;
use config::{Committee, DagProtocol, WorkerId};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::convert::TryInto;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct EmbeddedQc {
    pub target: Digest,
    pub round: Round,
    pub votes: Vec<Vote>,
}

// =============================================================================
// Wahoo-specific extensions (Phase A of the Tier-3 unification plan).
//
// These types and fields are populated *only* when `dag_protocol ==
// DagProtocol::Wahoo`. For all other protocols, the corresponding `Option<...>`
// fields on `Header`/`Vote` are `None` and the `Hash::digest()` impls skip them
// entirely, so the on-the-wire encoding and the digest of a non-Wahoo header
// are byte-for-byte identical to what they were before this commit.
//
// All Wahoo helper structs derive `Default` so that fixture code that uses
// `..Header::default()` continues to compile unchanged.
// =============================================================================

/// EPBC delivery tier (paper Section IV-B):
/// * `EpbcTs1` — 3-step slow path, σ_S1 quorum (2f+1 shares).
/// * `EpbcTs2` — 5-step slow path, σ_S2 quorum (2f+1 shares of σ_S1).
/// * `EpbcTf`  — 3-step fast path, σ_F   (n shares of the raw block).
/// * `Pbc`     — 3-step Provable Broadcast (paper Section IV-A) Tag=1
///   proposal: real block carrying worker-batch digests.
/// * `PbcVoteComplete` — Go reference `Block.Tag=2`: empty
///   "vote-complete" block broadcast after PB collects 2f+1 votes.
///   Mapped onto a separate `WahooTag` variant (rather than to
///   `Certificate`) for parity with the Go simplification; the unified
///   Phase D migration replaces this with a real `Certificate`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WahooTag {
    EpbcTs1 = 1,
    EpbcTs2 = 2,
    EpbcTf = 3,
    Pbc = 4,
    PbcVoteComplete = 5,
}

/// Which phase of EPBC a given `Vote` is collecting shares for. Disambiguates
/// the three concurrent quorums (TS1/TS2/TF) that EPBC runs for the same
/// block.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WahooVotePhase {
    /// First-phase share — collect 2f+1 ⇒ σ_S1, leading to a TS1-cert.
    Ts1 = 1,
    /// Second-phase share over σ_S1 — collect 2f+1 ⇒ σ_S2 (TS2-cert).
    Ts2 = 2,
    /// Full-quorum share over the raw block — collect n ⇒ σ_F (TF-cert).
    Tf = 3,
    /// PBC vote (paper Section IV-A) — single 2f+1 quorum.
    Pbc = 4,
}

/// Wahoo paper Algorithm 2 lines 6-20: the "leader-link" carried by every
/// EPBC block (round 2w-1). Empty link + n-f RECP messages = no-commit proof;
/// non-empty link + (f+1)-threshold signature = exclusive-commit proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderLink {
    /// `ll` — hash of the previous wave's leader block. `None` means
    /// "no-commit proof: I have not seen any TS2/TF cert for the prev leader".
    pub hash: Option<Digest>,
    /// `pf` — proof for the link. Variant determined by whether `hash` is set.
    pub proof: LeaderProof,
}

impl Default for LeaderLink {
    fn default() -> Self {
        Self {
            hash: None,
            proof: LeaderProof::NoCommit(Vec::new()),
        }
    }
}

impl LeaderLink {
    /// Phase B structural validation. Confirms that the proof variant
    /// matches the link kind and that the embedded share count meets the
    /// paper's n-f / f+1 requirements. The full cryptographic check on the
    /// threshold signature lives in Phase C alongside the BLS verifier.
    pub fn verify_structure(&self, committee: &Committee) -> DagResult<()> {
        let n: u32 = committee
            .authorities
            .keys()
            .map(|name| committee.stake(name))
            .sum();
        // `quorum_threshold` = 2f+1, `validity_threshold` = f+1.
        let f_plus_1 = committee.validity_threshold();
        let n_minus_f = n - f_plus_1 + 1; // n - f = (n - (f+1)) + 1

        match (&self.hash, &self.proof) {
            (None, LeaderProof::NoCommit(recps)) => {
                // No-commit proof requires n-f RECP shares from distinct
                // authorities (paper Algorithm 2 line 17-19).
                let mut seen = HashSet::new();
                for r in recps {
                    ensure!(
                        committee.stake(&r.author) > 0,
                        DagError::UnknownAuthority(r.author)
                    );
                    ensure!(seen.insert(r.author), DagError::AuthorityReuse(r.author));
                }
                let weight: u32 = seen.iter().map(|name| committee.stake(name)).sum();
                ensure!(weight >= n_minus_f, DagError::CertificateRequiresQuorum);
                Ok(())
            }
            (Some(_), LeaderProof::ExclusiveCommit(sig)) => {
                // Exclusive-commit proof is a (f+1)-threshold signature. The
                // cryptographic check is in `verify_with_crypto`; here we
                // only sanity-check the bytes are non-empty so structural
                // tests can run without the BLS key material.
                ensure!(!sig.is_empty(), DagError::CertificateRequiresQuorum);
                Ok(())
            }
            // Variant mismatch: hash present with NoCommit, or absent with
            // ExclusiveCommit — neither is legal.
            _ => Err(DagError::CertificateRequiresQuorum),
        }
    }

    /// Phase C Step 4d: full cryptographic verification of the leader
    /// link against the deterministically-derived BLS key set.
    ///
    /// `attested_round` is the previous EPBC wave the link attests to
    /// (i.e. `header.round - 2` for the header that carries this link).
    ///
    /// * `ExclusiveCommit`: validates the 96-byte aggregate BLS signature
    ///   against the master public key with message
    ///   `("wahoo-recp-v1", attested_round, hash)`.
    /// * `NoCommit`: for each carried `RecpMessage`, ensures its `round`
    ///   field equals `attested_round` and that its `share` is a valid
    ///   BLS partial signature from `author` over its `block_hash`.
    pub fn verify_with_crypto(
        &self,
        committee: &Committee,
        attested_round: u64,
    ) -> DagResult<()> {
        // Structural checks first (variant pairing, share counts, stake).
        self.verify_structure(committee)?;

        let mut authorities_sorted: Vec<crypto::PublicKey> =
            committee.authorities.keys().cloned().collect();
        authorities_sorted.sort();
        let threshold = crypto::recp_threshold(authorities_sorted.len());

        match (&self.hash, &self.proof) {
            (Some(hash), LeaderProof::ExclusiveCommit(sig)) => {
                if !crypto::verify_recp_aggregate(
                    &authorities_sorted,
                    threshold,
                    attested_round,
                    hash,
                    sig,
                ) {
                    return Err(DagError::InvalidLeaderLink);
                }
                Ok(())
            }
            (None, LeaderProof::NoCommit(recps)) => {
                for r in recps {
                    ensure!(
                        r.round == attested_round,
                        DagError::InvalidLeaderLink
                    );
                    if !crypto::verify_recp_share(
                        &authorities_sorted,
                        threshold,
                        &r.author,
                        r.round,
                        &r.block_hash,
                        &r.share,
                    ) {
                        return Err(DagError::InvalidLeaderLink);
                    }
                }
                Ok(())
            }
            _ => Err(DagError::CertificateRequiresQuorum),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaderProof {
    /// (f+1)-threshold signature attesting that the leader block was delivered
    /// with tier ≥ TS2 at the previous wave. Carried inline as raw bytes; the
    /// verifier reconstructs the threshold key via `crypto::recover_threshold`.
    ExclusiveCommit(Vec<u8>),
    /// n-f raw RECP messages (each ⟨RECP, h, ρ⟩). The verifier confirms each
    /// ρ is a valid share and that no contradicting `ExclusiveCommit` proof
    /// could exist.
    NoCommit(Vec<RecpMessage>),
}

/// Paper Section IV-B Algorithm 2 line 5: at the start of every EPBC phase,
/// each node broadcasts ⟨RECP, h, ρ⟩ — a (f+1)-threshold-signature share `ρ`
/// over the block hash `h` it just delivered.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecpMessage {
    /// `h` — the block hash being attested to.
    pub block_hash: Digest,
    /// Wave / round of the block.
    pub round: Round,
    /// Issuing authority.
    pub author: PublicKey,
    /// Threshold-signature share over `block_hash`.
    pub share: Vec<u8>,
}

impl Default for RecpMessage {
    fn default() -> Self {
        Self {
            block_hash: Digest::default(),
            round: 0,
            author: PublicKey::default(),
            share: Vec::new(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    pub payload: BTreeMap<Digest, WorkerId>,
    pub parents: BTreeSet<Digest>,
    pub parents_2: BTreeSet<Digest>,
    pub qc: Option<EmbeddedQc>,
    pub coin_share: Vec<u8>,
    /// Wahoo-only: EPBC/PBC tier this header was delivered at. `None` for
    /// Narwhal/Bullshark/NovelDAG headers. See `WahooTag`.
    #[serde(default)]
    pub wahoo_tag: Option<WahooTag>,
    /// Wahoo-only: leader-link carrying the previous wave's exclusive-commit
    /// or no-commit proof (paper Algorithm 2 lines 6-20). `None` for all
    /// other protocols, and for Wahoo PBC (even-round) headers.
    #[serde(default)]
    pub leader_link: Option<LeaderLink>,
    pub id: Digest,
    pub signature: Signature,
}

impl Header {
    pub async fn new(
        author: PublicKey,
        round: Round,
        payload: BTreeMap<Digest, WorkerId>,
        parents: BTreeSet<Digest>,
        parents_2: BTreeSet<Digest>,
        qc: Option<EmbeddedQc>,
        coin_share: Vec<u8>,
        signature_service: &mut SignatureService,
    ) -> Self {
        // Non-Wahoo constructor. Forwards through `new_with_wahoo` with both
        // Wahoo extension fields cleared, so the digest is byte-for-byte
        // identical to what it was before the Tier-3 extension fields were
        // introduced (the `digest()` impl skips `None` Wahoo fields).
        Self::new_with_wahoo(
            author,
            round,
            payload,
            parents,
            parents_2,
            qc,
            coin_share,
            /* wahoo_tag */ None,
            /* leader_link */ None,
            signature_service,
        )
        .await
    }

    /// Wahoo-aware constructor. The first nine positional arguments match
    /// `Header::new`; `wahoo_tag` and `leader_link` are populated by the
    /// Wahoo proposer when running EPBC/PBC rounds. For all other DAG
    /// protocols, pass `None` for both (or just call `Header::new`).
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_wahoo(
        author: PublicKey,
        round: Round,
        payload: BTreeMap<Digest, WorkerId>,
        parents: BTreeSet<Digest>,
        parents_2: BTreeSet<Digest>,
        qc: Option<EmbeddedQc>,
        coin_share: Vec<u8>,
        wahoo_tag: Option<WahooTag>,
        leader_link: Option<LeaderLink>,
        signature_service: &mut SignatureService,
    ) -> Self {
        let header = Self {
            author,
            round,
            payload,
            parents,
            parents_2,
            qc,
            coin_share,
            wahoo_tag,
            leader_link,
            id: Digest::default(),
            signature: Signature::default(),
        };
        let id = header.digest();
        let signature = signature_service.request_signature(id.clone()).await;
        Self {
            id,
            signature,
            ..header
        }
    }

    pub fn verify(&self, committee: &Committee, dag_protocol: DagProtocol) -> DagResult<()> {
        // Ensure the header id is well formed.
        ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        // Structural rules vary by protocol.
        match dag_protocol {
            DagProtocol::NovelDAG => {
                if self.round == 0 {
                    ensure!(
                        self.parents.is_empty() && self.parents_2.is_empty() && self.qc.is_none(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                } else if self.round == 1 {
                    ensure!(
                        self.parents_2.is_empty() && self.qc.is_none(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                } else {
                    ensure!(
                        !self.parents_2.is_empty() && self.qc.is_some(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                }
            }
            DagProtocol::Narwhal | DagProtocol::Bullshark => {
                // Narwhal/Bullshark headers only use the parents field;
                // parents_2 and qc are allowed but not required.
                if self.round == 0 {
                    ensure!(
                        self.parents.is_empty() && self.parents_2.is_empty(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                } else {
                    ensure!(
                        !self.parents.is_empty(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                }
                // Phase A invariant: only Wahoo headers may carry wahoo_tag
                // or leader_link.
                ensure!(
                    self.wahoo_tag.is_none() && self.leader_link.is_none(),
                    DagError::MalformedHeader(self.id.clone())
                );
            }
            DagProtocol::Wahoo => {
                // Wahoo wave structure (paper Section IV):
                //   round 0         : genesis (empty parents, no tag/link)
                //   round odd ≥ 1   : EPBC phase of wave w = (round+1)/2
                //                     wahoo_tag ∈ {EpbcTs1, EpbcTs2, EpbcTf}
                //                     leader_link is optional (only required by
                //                     paper Algorithm 2 for wave ≥ 2; current
                //                     transitional code may leave it as None)
                //   round even ≥ 2  : PBC phase of wave w = round/2
                //                     wahoo_tag = Pbc
                //                     coin_share carries the leader-election
                //                     partial signature (same field NovelDAG
                //                     re-uses for its threshold coin)
                //
                // During the Phase B migration, tags may legitimately be `None`
                // on headers produced by transitional code paths. We therefore
                // accept `None` *and* the correct tag for the round parity,
                // rejecting only outright wrong tags (e.g. EpbcTs1 at an even
                // round). Once Phase D lands, tags become mandatory.
                if self.round == 0 {
                    ensure!(
                        self.parents.is_empty()
                            && self.parents_2.is_empty()
                            && self.wahoo_tag.is_none()
                            && self.leader_link.is_none(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                } else {
                    ensure!(
                        !self.parents.is_empty(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                    let is_epbc_round = self.round % 2 == 1;
                    if let Some(tag) = self.wahoo_tag {
                        let tag_matches_round = match tag {
                            WahooTag::EpbcTs1 | WahooTag::EpbcTs2 | WahooTag::EpbcTf => {
                                is_epbc_round
                            }
                            WahooTag::Pbc | WahooTag::PbcVoteComplete => !is_epbc_round,
                        };
                        ensure!(
                            tag_matches_round,
                            DagError::MalformedHeader(self.id.clone())
                        );
                    }
                    // `leader_link` is only meaningful on EPBC (odd-round)
                    // headers. PBC headers carry the leader choice in
                    // `coin_share`, not in `leader_link`.
                    if !is_epbc_round {
                        ensure!(
                            self.leader_link.is_none(),
                            DagError::MalformedHeader(self.id.clone())
                        );
                    }
                    // Structural validation of the link itself.
                    if let Some(link) = &self.leader_link {
                        link.verify_structure(committee)
                            .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
                    }
                }
            }
        }

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author));

        // Ensure all worker ids are correct.
        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, &worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        // Ensure first-hop and second-hop parent sets don't overlap.
        ensure!(
            self.parents.is_disjoint(&self.parents_2),
            DagError::MalformedHeader(self.id.clone())
        );

        // Validate the coin share: it must not be excessively large.
        ensure!(
            self.coin_share.len() <= 256,
            DagError::MalformedHeader(self.id.clone())
        );

        // If present, validate the embedded QC consistency and signatures.
        if let Some(qc) = &self.qc {
            ensure!(
                qc.round + 1 == self.round,
                DagError::MalformedHeader(self.id.clone())
            );

            let mut weight = 0;
            let mut used = HashSet::new();
            let mut sigs: Vec<(PublicKey, Signature)> =
                Vec::with_capacity(qc.votes.len());
            for vote in qc.votes.iter() {
                ensure!(vote.id == qc.target, DagError::MalformedHeader(self.id.clone()));
                ensure!(vote.round == qc.round, DagError::MalformedHeader(self.id.clone()));
                ensure!(vote.origin == self.author, DagError::MalformedHeader(self.id.clone()));

                ensure!(!used.contains(&vote.author), DagError::AuthorityReuse(vote.author));
                ensure!(
                    committee.stake(&vote.author) > 0,
                    DagError::UnknownAuthority(vote.author)
                );
                sigs.push((vote.author, vote.signature.clone()));
                used.insert(vote.author);
                weight += committee.stake(&vote.author);
            }
            // Batch-verify all QC vote signatures. All votes in a QC sign the
            // same payload (id == target, same round/voter_round/origin).
            if let Some(vote_digest) = qc.votes.first().map(|v| v.digest()) {
                Signature::verify_batch(&vote_digest, &sigs)?;
            }
            ensure!(
                weight >= committee.quorum_threshold(),
                DagError::CertificateRequiresQuorum
            );
        }

        // Check the signature.
        self.signature
            .verify(&self.id, &self.author)
            .map_err(DagError::from)
    }

    /// Async variant that runs CPU-bound batch verification on the blocking
    /// thread pool so the async runtime stays responsive.
    pub async fn verify_async(&self, committee: &Committee, dag_protocol: DagProtocol) -> DagResult<()> {
        let header = self.clone();
        let committee = committee.clone();
        tokio::task::spawn_blocking(move || header.verify(&committee, dag_protocol))
            .await
            .expect("Header::verify panicked")
    }
}

impl Hash for Header {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.author);
        hasher.update(self.round.to_le_bytes());
        for (x, y) in &self.payload {
            hasher.update(x);
            hasher.update(y.to_le_bytes());
        }
        for x in &self.parents {
            hasher.update(x);
        }
        for x in &self.parents_2 {
            hasher.update(x);
        }
        if let Some(qc) = &self.qc {
            hasher.update(&qc.target);
            hasher.update(qc.round.to_le_bytes());
            for vote in &qc.votes {
                hasher.update(&vote.id);
                hasher.update(vote.round.to_le_bytes());
                hasher.update(vote.voter_round.to_le_bytes());
                hasher.update(&vote.origin);
                hasher.update(&vote.author);
                hasher.update(vote.signature.to_bytes());
            }
        }
        hasher.update(&self.coin_share);
        // Wahoo extensions: only mix in when present so that non-Wahoo headers
        // produce the exact same digest as before these fields existed.
        if let Some(tag) = self.wahoo_tag {
            // Domain separator to prevent any collision with `coin_share`
            // bytes that may end with the same value.
            hasher.update(b"WTAG");
            hasher.update([tag as u8]);
        }
        if let Some(ll) = &self.leader_link {
            hasher.update(b"WLLN");
            if let Some(h) = &ll.hash {
                hasher.update(b"WLLH");
                hasher.update(h);
            }
            match &ll.proof {
                LeaderProof::ExclusiveCommit(sig) => {
                    hasher.update(b"WLPE");
                    hasher.update(sig);
                }
                LeaderProof::NoCommit(recps) => {
                    hasher.update(b"WLPN");
                    hasher.update((recps.len() as u32).to_le_bytes());
                    for r in recps {
                        hasher.update(&r.block_hash);
                        hasher.update(r.round.to_le_bytes());
                        hasher.update(&r.author);
                        hasher.update(&r.share);
                    }
                }
            }
        }
        let digest = hasher.finalize();
        Digest(digest[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}({}, {})",
            self.id,
            self.round,
            self.author,
            self.payload.keys().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round, self.author)
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Vote {
    pub id: Digest,
    pub round: Round,
    pub voter_round: Round,
    pub origin: PublicKey,
    pub author: PublicKey,
    /// Wahoo-only: which EPBC/PBC quorum this vote contributes to. `None` for
    /// Narwhal/Bullshark/NovelDAG votes — those have only one quorum per
    /// header, so the field is implicit. See `WahooVotePhase`.
    #[serde(default)]
    pub wahoo_phase: Option<WahooVotePhase>,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        header: &Header,
        voter_round: Round,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        Self::new_with_phase(header, voter_round, author, /* phase */ None, signature_service).await
    }

    /// Wahoo-aware constructor; identical to `Vote::new` except it stamps the
    /// `wahoo_phase` field. Pass `None` for non-Wahoo callers.
    pub async fn new_with_phase(
        header: &Header,
        voter_round: Round,
        author: &PublicKey,
        wahoo_phase: Option<WahooVotePhase>,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id: header.id.clone(),
            round: header.round,
            voter_round,
            origin: header.author,
            author: *author,
            wahoo_phase,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.id);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.voter_round.to_le_bytes());
        hasher.update(&self.origin);
        // Wahoo phase: hashed only when present, with a domain separator,
        // so non-Wahoo vote digests are byte-identical to pre-Tier-3 ones.
        if let Some(phase) = self.wahoo_phase {
            hasher.update(b"WVPH");
            hasher.update([phase as u8]);
        }
        let digest = hasher.finalize();
        Digest(digest[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {}, {})",
            self.digest(),
            self.round,
            self.voter_round,
            self.author,
            self.id
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<Vote>,
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| Self {
                header: Header {
                    author: *name,
                    ..Header::default()
                },
                ..Self::default()
            })
            .collect()
    }

    pub fn verify(&self, committee: &Committee, dag_protocol: DagProtocol) -> DagResult<()> {
        // Genesis certificates are always valid.
        if Self::genesis(committee).contains(self) {
            return Ok(());
        }

        // Check the embedded header.
        self.header.verify(committee, dag_protocol)?;

        // NovelDAG carries peer certificates implicitly through signed headers
        // and embedded QCs. Locally synthesized certificates intentionally have
        // empty vote sets; for NovelDAG the signed header is the object we need
        // to store, sync, and use as a DAG parent.
        if dag_protocol == DagProtocol::NovelDAG && self.votes.is_empty() {
            return Ok(());
        }

        // Ensure the certificate has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        let mut sigs: Vec<(PublicKey, Signature)> =
            Vec::with_capacity(self.votes.len());
        let vote_digest = self.votes.first().map(|v| v.digest());
        for vote in self.votes.iter() {
            ensure!(vote.id == self.header.id, DagError::MalformedHeader(self.header.id.clone()));
            ensure!(vote.round == self.round(), DagError::MalformedHeader(self.header.id.clone()));
            ensure!(
                vote.origin == self.origin(),
                DagError::MalformedHeader(self.header.id.clone())
            );

            ensure!(
                !used.contains(&vote.author),
                DagError::AuthorityReuse(vote.author)
            );
            ensure!(
                committee.stake(&vote.author) > 0,
                DagError::UnknownAuthority(vote.author)
            );
            sigs.push((vote.author, vote.signature.clone()));
            used.insert(vote.author);
            weight += committee.stake(&vote.author);
        }
        // Batch-verify all vote signatures in a single multi-scalar multiplication.
        if let Some(digest) = vote_digest {
            Signature::verify_batch(&digest, &sigs)?;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        Ok(())
    }

    /// Async variant that runs CPU-bound batch verification on the blocking
    /// thread pool so the async runtime stays responsive.
    pub async fn verify_async(&self, committee: &Committee, dag_protocol: DagProtocol) -> DagResult<()> {
        let certificate = self.clone();
        let committee = committee.clone();
        tokio::task::spawn_blocking(move || certificate.verify(&committee, dag_protocol))
            .await
            .expect("Certificate::verify panicked")
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn origin(&self) -> PublicKey {
        self.header.author
    }
}

impl Hash for Certificate {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.header.id);
        hasher.update(self.round().to_le_bytes());
        hasher.update(&self.origin());
        let digest = hasher.finalize();
        Digest(digest[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({}, {})",
            self.digest(),
            self.round(),
            self.origin(),
            self.header.id
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header.id == other.header.id;
        ret &= self.round() == other.round();
        ret &= self.origin() == other.origin();
        ret
    }
}

// =============================================================================
// Phase B Step 2 tests: Wahoo branch of Header::verify and LeaderLink
// structural validation. Uses the existing `tests::common::{keys, committee}`
// fixture (n=4, f=1).
// =============================================================================
#[cfg(test)]
mod novel_verify_tests {
    use super::*;
    use crate::common::{committee, keys};

    #[test]
    fn noveldag_round_1_rejects_embedded_qc() {
        let committee = committee();
        let (author, secret) = keys().pop().unwrap();
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .find(|certificate| certificate.origin() == author)
            .unwrap();

        let votes = keys()
            .into_iter()
            .map(|(voter, voter_secret)| {
                let vote = Vote {
                    id: genesis.header.id.clone(),
                    round: 0,
                    voter_round: 0,
                    origin: author,
                    author: voter,
                    wahoo_phase: None,
                    signature: Signature::default(),
                };
                Vote {
                    signature: Signature::new(&vote.digest(), &voter_secret),
                    ..vote
                }
            })
            .collect();

        let mut header = Header {
            author,
            round: 1,
            parents: Certificate::genesis(&committee)
                .iter()
                .map(|certificate| certificate.digest())
                .collect(),
            qc: Some(EmbeddedQc {
                target: genesis.header.id,
                round: 0,
                votes,
            }),
            ..Header::default()
        };
        header.id = header.digest();
        header.signature = Signature::new(&header.id, &secret);

        assert!(header.verify(&committee, DagProtocol::NovelDAG).is_err());
    }
}

#[cfg(test)]
mod wahoo_verify_tests {
    use super::*;
    use crate::common::{committee, keys};
    use std::collections::BTreeSet;

    /// Build a Wahoo header signed by the first fixture authority. The
    /// signature is computed against the digest *including* the supplied
    /// Wahoo extension fields, so changing them after the fact would
    /// invalidate `id == digest()` — exactly the property we want to test.
    fn make_wahoo_header(
        round: Round,
        wahoo_tag: Option<WahooTag>,
        leader_link: Option<LeaderLink>,
        coin_share: Vec<u8>,
        parents: BTreeSet<Digest>,
    ) -> Header {
        let (author, secret) = keys().pop().unwrap();
        let mut header = Header {
            author,
            round,
            parents,
            coin_share,
            wahoo_tag,
            leader_link,
            ..Header::default()
        };
        header.id = header.digest();
        header.signature = Signature::new(&header.id, &secret);
        header
    }

    fn one_parent() -> BTreeSet<Digest> {
        let mut s = BTreeSet::new();
        // A non-empty digest is enough; we don't validate the referenced
        // block actually exists at this layer.
        s.insert(Digest([1u8; 32]));
        s
    }

    #[test]
    fn round_0_must_have_no_wahoo_fields() {
        let committee = committee();
        let h = make_wahoo_header(0, None, None, Vec::new(), BTreeSet::new());
        assert!(h.verify(&committee, DagProtocol::Wahoo).is_ok());

        let h = make_wahoo_header(0, Some(WahooTag::EpbcTs1), None, Vec::new(), BTreeSet::new());
        assert!(h.verify(&committee, DagProtocol::Wahoo).is_err());
    }

    #[test]
    fn odd_round_accepts_epbc_tags() {
        let committee = committee();
        for tag in [WahooTag::EpbcTs1, WahooTag::EpbcTs2, WahooTag::EpbcTf] {
            let h = make_wahoo_header(1, Some(tag), None, Vec::new(), one_parent());
            assert!(
                h.verify(&committee, DagProtocol::Wahoo).is_ok(),
                "EPBC tag {:?} must be valid at odd round",
                tag
            );
        }
    }

    #[test]
    fn odd_round_rejects_pbc_tag() {
        let committee = committee();
        let h = make_wahoo_header(3, Some(WahooTag::Pbc), None, Vec::new(), one_parent());
        assert!(h.verify(&committee, DagProtocol::Wahoo).is_err());
    }

    #[test]
    fn even_round_accepts_pbc_tag_and_rejects_epbc_tags() {
        let committee = committee();
        let h = make_wahoo_header(2, Some(WahooTag::Pbc), None, Vec::new(), one_parent());
        assert!(h.verify(&committee, DagProtocol::Wahoo).is_ok());

        for tag in [WahooTag::EpbcTs1, WahooTag::EpbcTs2, WahooTag::EpbcTf] {
            let h = make_wahoo_header(2, Some(tag), None, Vec::new(), one_parent());
            assert!(h.verify(&committee, DagProtocol::Wahoo).is_err());
        }
    }

    #[test]
    fn even_round_rejects_leader_link() {
        let committee = committee();
        let h = make_wahoo_header(
            2,
            Some(WahooTag::Pbc),
            Some(LeaderLink::default()),
            Vec::new(),
            one_parent(),
        );
        assert!(h.verify(&committee, DagProtocol::Wahoo).is_err());
    }

    #[test]
    fn no_commit_link_requires_n_minus_f_recps() {
        let committee = committee(); // n=4, f=1, n-f=3
        // Build n-f distinct shares from the fixture authorities.
        let mut recps: Vec<RecpMessage> = keys()
            .into_iter()
            .take(3)
            .map(|(author, _)| RecpMessage {
                block_hash: Digest([7u8; 32]),
                round: 0,
                author,
                share: vec![0u8; 8],
            })
            .collect();
        let link = LeaderLink {
            hash: None,
            proof: LeaderProof::NoCommit(recps.clone()),
        };
        assert!(link.verify_structure(&committee).is_ok());

        // Removing one share drops us below n-f → must fail.
        recps.pop();
        let link = LeaderLink {
            hash: None,
            proof: LeaderProof::NoCommit(recps.clone()),
        };
        assert!(link.verify_structure(&committee).is_err());

        // Duplicate authors → reuse error.
        let dup_author = recps[0].author;
        recps.push(RecpMessage {
            block_hash: Digest([7u8; 32]),
            round: 0,
            author: dup_author,
            share: vec![0u8; 8],
        });
        let link = LeaderLink {
            hash: None,
            proof: LeaderProof::NoCommit(recps),
        };
        assert!(link.verify_structure(&committee).is_err());
    }

    #[test]
    fn exclusive_commit_link_requires_nonempty_sig_and_hash() {
        let committee = committee();
        // hash + ExclusiveCommit(nonempty) is OK.
        let link = LeaderLink {
            hash: Some(Digest([1u8; 32])),
            proof: LeaderProof::ExclusiveCommit(vec![0xab; 48]),
        };
        assert!(link.verify_structure(&committee).is_ok());

        // hash + ExclusiveCommit(empty) is NOT OK.
        let link = LeaderLink {
            hash: Some(Digest([1u8; 32])),
            proof: LeaderProof::ExclusiveCommit(vec![]),
        };
        assert!(link.verify_structure(&committee).is_err());

        // hash present + NoCommit is illegal (variant mismatch).
        let link = LeaderLink {
            hash: Some(Digest([1u8; 32])),
            proof: LeaderProof::NoCommit(Vec::new()),
        };
        assert!(link.verify_structure(&committee).is_err());

        // hash absent + ExclusiveCommit is also illegal.
        let link = LeaderLink {
            hash: None,
            proof: LeaderProof::ExclusiveCommit(vec![0xab; 48]),
        };
        assert!(link.verify_structure(&committee).is_err());
    }

    #[test]
    fn narwhal_rejects_wahoo_fields() {
        let committee = committee();
        // A Narwhal-protocol header must NOT carry wahoo_tag/leader_link
        // even if round-parity rules would otherwise permit them.
        let h = make_wahoo_header(1, Some(WahooTag::EpbcTs1), None, Vec::new(), one_parent());
        assert!(h.verify(&committee, DagProtocol::Narwhal).is_err());

        let h = make_wahoo_header(1, None, Some(LeaderLink::default()), Vec::new(), one_parent());
        assert!(h.verify(&committee, DagProtocol::Narwhal).is_err());
    }
}
