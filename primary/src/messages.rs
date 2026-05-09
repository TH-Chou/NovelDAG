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

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    pub payload: BTreeMap<Digest, WorkerId>,
    pub parents: BTreeSet<Digest>,
    pub parents_2: BTreeSet<Digest>,
    pub qc: Option<EmbeddedQc>,
    pub coin_share: Vec<u8>,
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
        let header = Self {
            author,
            round,
            payload,
            parents,
            parents_2,
            qc,
            coin_share,
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
                        self.parents_2.is_empty(),
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
            // Batch-verify all QC vote signatures in a single multi-scalar multiplication.
            Signature::verify_batch(&qc.target, &sigs)?;
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Digest,
    pub round: Round,
    pub voter_round: Round,
    pub origin: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        header: &Header,
        voter_round: Round,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id: header.id.clone(),
            round: header.round,
            voter_round,
            origin: header.author,
            author: *author,
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
