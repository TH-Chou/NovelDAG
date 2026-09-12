use crate::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
use rand::SeedableRng as _;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use threshold_crypto::{PublicKeySet, SecretKeySet, SignatureShare};

const CACHE_ROUNDS: usize = 256;

/// Shared deterministic leader schedule used by every protocol in modes that
/// do not reconstruct a threshold signature.
#[derive(Clone)]
pub struct CoinCommittee {
    authorities: Arc<Vec<PublicKey>>,
}

impl CoinCommittee {
    pub fn new(authorities: &[PublicKey]) -> Self {
        let mut authorities = authorities.to_vec();
        authorities.sort();
        authorities.dedup();
        assert!(!authorities.is_empty(), "coin committee must not be empty");
        Self {
            authorities: Arc::new(authorities),
        }
    }

    pub fn authorities(&self) -> &[PublicKey] {
        self.authorities.as_slice()
    }

    pub fn round_robin(&self, round: u64) -> u64 {
        round
    }

    /// Deterministic, communication-free pseudorandom value. The committee is
    /// included in the domain so every protocol derives the same value for the
    /// same committee and logical coin round.
    pub fn pseudo_random(&self, round: u64) -> u64 {
        let mut hasher = Sha512::new();
        hasher.update(b"noveldag-pseudo-random-coin-v1");
        for authority in self.authorities.iter() {
            hasher.update(authority);
        }
        hasher.update(&round.to_le_bytes());
        let digest = hasher.finalize();
        u64::from_le_bytes(
            digest[..8]
                .try_into()
                .expect("SHA-512 output is long enough"),
        )
    }

    pub fn leader(&self, value: u64, offset: usize) -> PublicKey {
        self.authorities[(value as usize + offset) % self.authorities.len()]
    }
}

/// Shared threshold-coin backend used by every protocol.
///
/// The benchmark currently derives threshold keys deterministically from the
/// committee. A production deployment should replace this setup with DKG, but
/// share generation, verification, recovery, and caching remain protocol
/// independent.
#[derive(Clone)]
pub struct ThresholdCoin {
    inner: Arc<ThresholdCoinInner>,
}

struct ThresholdCoinInner {
    authority_indices: HashMap<PublicKey, usize>,
    threshold: usize,
    secret_key_set: SecretKeySet,
    public_key_set: PublicKeySet,
    share_cache: Mutex<BTreeMap<(u64, usize), Vec<u8>>>,
    verified_share_cache: Mutex<BTreeMap<(u64, usize), CachedShare>>,
    recovered_cache: Mutex<BTreeMap<u64, u64>>,
}

#[derive(Clone)]
struct CachedShare {
    bytes: Vec<u8>,
    share: Option<SignatureShare>,
}

impl ThresholdCoin {
    pub fn new(authorities: &[PublicKey], threshold: usize) -> Self {
        Self::from_committee(CoinCommittee::new(authorities), threshold)
    }

    pub fn from_committee(committee: CoinCommittee, threshold: usize) -> Self {
        assert!(
            threshold < committee.authorities().len(),
            "coin threshold must be smaller than the committee"
        );

        let authority_indices = committee
            .authorities()
            .iter()
            .enumerate()
            .map(|(index, authority)| (*authority, index))
            .collect();
        let secret_key_set = deterministic_key_set(committee.authorities(), threshold);
        let public_key_set = secret_key_set.public_keys();

        Self {
            inner: Arc::new(ThresholdCoinInner {
                authority_indices,
                threshold,
                secret_key_set,
                public_key_set,
                share_cache: Mutex::new(BTreeMap::new()),
                verified_share_cache: Mutex::new(BTreeMap::new()),
                recovered_cache: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn threshold(&self) -> usize {
        self.inner.threshold
    }

    /// Generate one authority's share, reusing it if this round was requested
    /// before (for example after a protocol-level retransmission).
    pub fn make_share(&self, authority: &PublicKey, round: u64) -> Option<Vec<u8>> {
        let index = *self.inner.authority_indices.get(authority)?;
        let cache_key = (round, index);
        if let Some(share) = self
            .inner
            .share_cache
            .lock()
            .expect("coin share cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            return Some(share);
        }

        let share = self
            .inner
            .secret_key_set
            .secret_key_share(index)
            .sign(coin_message(round));
        let bytes = bincode::serialize(&share).ok()?;
        let mut cache = self
            .inner
            .share_cache
            .lock()
            .expect("coin share cache poisoned");
        cache.insert(cache_key, bytes.clone());
        retain_recent_generated_shares(&mut cache, round);
        drop(cache);

        let mut verified = self
            .inner
            .verified_share_cache
            .lock()
            .expect("verified coin share cache poisoned");
        verified.insert(
            cache_key,
            CachedShare {
                bytes: bytes.clone(),
                share: Some(share),
            },
        );
        retain_recent_verified_shares(&mut verified, round);
        Some(bytes)
    }

    /// Recover a coin from the first threshold+1 distinct valid shares.
    /// Duplicate, malformed, and non-committee shares are ignored without
    /// repeating expensive BLS verification.
    pub fn recover(&self, round: u64, shares: &[(PublicKey, Vec<u8>)]) -> Option<u64> {
        if let Some(coin) = self
            .inner
            .recovered_cache
            .lock()
            .expect("recovered coin cache poisoned")
            .get(&round)
            .copied()
        {
            return Some(coin);
        }

        let message = coin_message(round);
        let mut unique_shares = BTreeMap::new();
        for (authority, bytes) in shares {
            let index = match self.inner.authority_indices.get(authority) {
                Some(index) if !unique_shares.contains_key(index) => *index,
                _ => continue,
            };
            let share = match self.verify_share(round, index, bytes, &message) {
                Some(share) => share,
                None => continue,
            };
            unique_shares.insert(index, share);
            if unique_shares.len() == self.inner.threshold + 1 {
                break;
            }
        }

        if unique_shares.len() < self.inner.threshold + 1 {
            return None;
        }

        let signature = self
            .inner
            .public_key_set
            .combine_signatures(&unique_shares)
            .ok()?;
        if !self
            .inner
            .public_key_set
            .public_key()
            .verify(&signature, &message)
        {
            return None;
        }

        let mut hasher = Sha512::new();
        hasher.update(b"narwhal-common-coin-output-v1");
        hasher.update(&signature.to_bytes());
        let digest = hasher.finalize();
        let coin = u64::from_le_bytes(digest[..8].try_into().ok()?);

        let mut cache = self
            .inner
            .recovered_cache
            .lock()
            .expect("recovered coin cache poisoned");
        cache.insert(round, coin);
        while cache.len() > CACHE_ROUNDS {
            let oldest = *cache.keys().next().expect("non-empty coin cache");
            cache.remove(&oldest);
        }
        Some(coin)
    }

    fn verify_share(
        &self,
        round: u64,
        index: usize,
        bytes: &[u8],
        message: &[u8],
    ) -> Option<SignatureShare> {
        let cache_key = (round, index);
        if let Some(cached) = self
            .inner
            .verified_share_cache
            .lock()
            .expect("verified coin share cache poisoned")
            .get(&cache_key)
            .filter(|cached| cached.bytes == bytes)
            .cloned()
        {
            return cached.share;
        }

        let share: Option<SignatureShare> = bincode::deserialize(bytes).ok().filter(|share| {
            self.inner
                .public_key_set
                .public_key_share(index)
                .verify(share, message)
        });
        let mut cache = self
            .inner
            .verified_share_cache
            .lock()
            .expect("verified coin share cache poisoned");
        cache.insert(
            cache_key,
            CachedShare {
                bytes: bytes.to_vec(),
                share: share.clone(),
            },
        );
        retain_recent_verified_shares(&mut cache, round);
        share
    }
}

pub fn threshold(committee_size: usize) -> usize {
    committee_size.saturating_sub(1) / 3
}

/// Threshold parameter requiring 2f+1 shares to reconstruct. Mahi-Mahi and
/// Wahoo use this gate for their paper-level coin reveal rounds.
pub fn quorum_threshold(committee_size: usize) -> usize {
    2 * threshold(committee_size)
}

fn coin_message(round: u64) -> Vec<u8> {
    let mut message = b"narwhal-common-coin-v1".to_vec();
    message.extend_from_slice(&round.to_le_bytes());
    message
}

fn deterministic_key_set(authorities: &[PublicKey], threshold: usize) -> SecretKeySet {
    let mut hasher = Sha512::new();
    hasher.update(b"narwhal-threshold-coin-seed-v1");
    for authority in authorities {
        hasher.update(authority);
    }
    let digest = hasher.finalize();
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&digest[..32]);
    let mut rng = rand::rngs::StdRng::from_seed(seed);
    SecretKeySet::random(threshold, &mut rng)
}

fn retain_recent_generated_shares(cache: &mut BTreeMap<(u64, usize), Vec<u8>>, newest_round: u64) {
    let oldest_round = newest_round.saturating_sub(CACHE_ROUNDS as u64);
    cache.retain(|(round, _), _| *round >= oldest_round);
}

fn retain_recent_verified_shares(
    cache: &mut BTreeMap<(u64, usize), CachedShare>,
    newest_round: u64,
) {
    let oldest_round = newest_round.saturating_sub(CACHE_ROUNDS as u64);
    cache.retain(|(round, _), _| *round >= oldest_round);
}
