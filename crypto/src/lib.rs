// Copyright(C) Facebook, Inc. and its affiliates.
use ed25519_dalek as dalek;
use ed25519_dalek::Digest as _;
use ed25519_dalek::ed25519;
use ed25519_dalek::Sha512;
use ed25519_dalek::Signer as _;
use rand::rngs::OsRng;
use rand::{CryptoRng, RngCore};
use rand::SeedableRng as _;
use serde::{de, ser, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::array::TryFromSliceError;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use threshold_crypto::{PublicKeySet, SecretKeySet, SignatureShare};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = ed25519::Error;

/// Represents a hash digest (32 bytes).
#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(&self.0))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(&self.0).get(0..16).unwrap())
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for Digest {
    type Error = TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> {
        Ok(Digest(item.try_into()?))
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Default)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..32]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Represents a secret key (in bytes).
pub struct SecretKey([u8; 64]);

impl SecretKey {
    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..64]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.0.iter_mut().for_each(|x| *x = 0);
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    generate_keypair(&mut OsRng)
}

pub fn generate_keypair<R>(csprng: &mut R) -> (PublicKey, SecretKey)
where
    R: CryptoRng + RngCore,
{
    let keypair = dalek::Keypair::generate(csprng);
    let public = PublicKey(keypair.public.to_bytes());
    let secret = SecretKey(keypair.to_bytes());
    (public, secret)
}

/// Represents an ed25519 signature.
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Signature {
    part1: [u8; 32],
    part2: [u8; 32],
}

impl Signature {
    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        let keypair = dalek::Keypair::from_bytes(&secret.0).expect("Unable to load secret key");
        let sig = keypair.sign(&digest.0).to_bytes();
        let part1 = sig[..32].try_into().expect("Unexpected signature length");
        let part2 = sig[32..64].try_into().expect("Unexpected signature length");
        Signature { part1, part2 }
    }

    fn flatten(&self) -> [u8; 64] {
        [self.part1, self.part2]
            .concat()
            .try_into()
            .expect("Unexpected signature length")
    }

    pub fn to_bytes(&self) -> [u8; 64] {
        self.flatten()
    }

    pub fn verify(&self, digest: &Digest, public_key: &PublicKey) -> Result<(), CryptoError> {
        let signature = ed25519::signature::Signature::from_bytes(&self.flatten())?;
        let key = dalek::PublicKey::from_bytes(&public_key.0)?;
        key.verify_strict(&digest.0, &signature)
    }

    pub fn verify_batch<'a, I>(digest: &Digest, votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
    {
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<dalek::Signature> = Vec::new();
        let mut keys: Vec<dalek::PublicKey> = Vec::new();
        for (key, sig) in votes.into_iter() {
            messages.push(&digest.0[..]);
            signatures.push(ed25519::signature::Signature::from_bytes(&sig.flatten())?);
            keys.push(dalek::PublicKey::from_bytes(&key.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &keys[..])
    }

    /// Async wrapper that runs batch verification on the blocking thread pool
    /// to avoid stalling the async runtime with CPU-bound multi-scalar multiplication.
    pub async fn verify_batch_async<'a, I>(digest: Digest, votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (PublicKey, Signature)> + Send + 'a,
    {
        let votes: Vec<(PublicKey, Signature)> = votes.into_iter().cloned().collect();
        tokio::task::spawn_blocking(move || Self::verify_batch(&digest, &votes))
            .await
            .expect("verify_batch panicked")
    }
}

/// This service holds the node's private key. It takes digests as input and returns a signature
/// over the digest (through a oneshot channel).
#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureService {
    pub fn new(secret: SecretKey) -> Self {
        let (tx, mut rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                let signature = Signature::new(&digest, &secret);
                let _ = sender.send(signature);
            }
        });
        Self { channel: tx }
    }

    pub async fn request_signature(&mut self, digest: Digest) -> Signature {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }
}

fn coin_message(round: u64) -> Vec<u8> {
    let mut message = b"narwhal-common-coin-v1".to_vec();
    message.extend_from_slice(&round.to_le_bytes());
    message
}

fn deterministic_coin_key_set(authorities: &[PublicKey], threshold: usize) -> SecretKeySet {
    let mut sorted_authorities = authorities.to_vec();
    sorted_authorities.sort();
    let mut hasher = Sha512::new();
    hasher.update(b"narwhal-threshold-coin-seed-v1");
    for authority in sorted_authorities {
        hasher.update(&authority);
    }
    let digest = hasher.finalize();
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&digest[..32]);
    let mut rng = rand::rngs::StdRng::from_seed(seed);
    SecretKeySet::random(threshold, &mut rng)
}

fn authority_index(authorities: &[PublicKey], authority: &PublicKey) -> Option<usize> {
    let mut sorted_authorities = authorities.to_vec();
    sorted_authorities.sort();
    sorted_authorities.iter().position(|name| name == authority)
}

pub fn coin_threshold(committee_size: usize) -> usize {
    committee_size.saturating_sub(1) / 3
}

pub fn make_coin_share(
    authorities: &[PublicKey],
    threshold: usize,
    authority: &PublicKey,
    round: u64,
) -> Option<Vec<u8>> {
    let index = authority_index(authorities, authority)?;
    let key_set = deterministic_coin_key_set(authorities, threshold);
    let share = key_set.secret_key_share(index).sign(coin_message(round));
    bincode::serialize(&share).ok()
}

pub fn recover_coin(
    authorities: &[PublicKey],
    threshold: usize,
    round: u64,
    shares: &[(PublicKey, Vec<u8>)],
) -> Option<u64> {
    let key_set = deterministic_coin_key_set(authorities, threshold);
    let public_key_set: PublicKeySet = key_set.public_keys();
    let message = coin_message(round);

    let mut unique_shares = BTreeMap::new();
    for (authority, bytes) in shares {
        let index = match authority_index(authorities, authority) {
            Some(index) => index,
            None => continue,
        };
        let share: SignatureShare = match bincode::deserialize(bytes) {
            Ok(share) => share,
            Err(_) => continue,
        };
        if !public_key_set.public_key_share(index).verify(&share, &message) {
            continue;
        }
        unique_shares.entry(index).or_insert(share);
    }

    if unique_shares.len() < threshold + 1 {
        return None;
    }

    let signature = public_key_set.combine_signatures(&unique_shares).ok()?;
    if !public_key_set.public_key().verify(&signature, &message) {
        return None;
    }
    let bytes = signature.to_bytes();
    let mut hasher = Sha512::new();
    hasher.update(b"narwhal-common-coin-output-v1");
    hasher.update(&bytes);
    let digest = hasher.finalize();
    Some(u64::from_le_bytes(digest[..8].try_into().ok()?))
}

// ---------------------------------------------------------------------------
// Wahoo RECP threshold-BLS pipeline (paper Section IV-B, Step 4d).
//
// Mirrors the common-coin pipeline above with two key differences:
//  * Domain-separated seed and message labels so a coin share can never
//    be replayed as a RECP share and vice-versa.
//  * RECP shares sign over (round, block_hash), not just round — so the
//    block being attested to is bound into the signature.
//
// Key material is regenerated deterministically on every node from the
// committee's authority set, identical to the coin path. This is a
// research benchmark concession: in a real deployment, the RECP key
// set would be produced by a DKG once at committee installation.
// ---------------------------------------------------------------------------

fn deterministic_recp_key_set(authorities: &[PublicKey], threshold: usize) -> SecretKeySet {
    let mut sorted_authorities = authorities.to_vec();
    sorted_authorities.sort();
    let mut hasher = Sha512::new();
    hasher.update(b"wahoo-threshold-recp-seed-v1");
    for authority in sorted_authorities {
        hasher.update(&authority);
    }
    let digest = hasher.finalize();
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&digest[..32]);
    let mut rng = rand::rngs::StdRng::from_seed(seed);
    SecretKeySet::random(threshold, &mut rng)
}

fn recp_message(round: u64, block_hash: &Digest) -> Vec<u8> {
    let mut message = b"wahoo-recp-v1".to_vec();
    message.extend_from_slice(&round.to_le_bytes());
    message.extend_from_slice(&block_hash.0);
    message
}

/// Recommended BLS threshold for RECP: t = f = (n-1)/3, so (f+1) shares
/// recover the aggregate signature.
pub fn recp_threshold(committee_size: usize) -> usize {
    committee_size.saturating_sub(1) / 3
}

/// Produce this authority's BLS partial signature on (round, block_hash).
/// Returns `None` if the authority isn't in the committee.
pub fn make_recp_share(
    authorities: &[PublicKey],
    threshold: usize,
    authority: &PublicKey,
    round: u64,
    block_hash: &Digest,
) -> Option<Vec<u8>> {
    let index = authority_index(authorities, authority)?;
    let key_set = deterministic_recp_key_set(authorities, threshold);
    let share = key_set
        .secret_key_share(index)
        .sign(recp_message(round, block_hash));
    bincode::serialize(&share).ok()
}

/// Verify a single BLS partial RECP signature against the authority's
/// public-key share. Used for `LeaderProof::NoCommit` (paper line 17-19),
/// where the verifier checks each share individually.
pub fn verify_recp_share(
    authorities: &[PublicKey],
    threshold: usize,
    authority: &PublicKey,
    round: u64,
    block_hash: &Digest,
    share_bytes: &[u8],
) -> bool {
    let index = match authority_index(authorities, authority) {
        Some(i) => i,
        None => return false,
    };
    let key_set = deterministic_recp_key_set(authorities, threshold);
    let pks: PublicKeySet = key_set.public_keys();
    let share: SignatureShare = match bincode::deserialize(share_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };
    pks.public_key_share(index)
        .verify(&share, recp_message(round, block_hash))
}

/// Combine `(f+1)` partial signatures into a single aggregate BLS
/// signature for paper-Section-IV-B `LeaderProof::ExclusiveCommit`.
/// `shares` is the same `(author, partial_bytes)` shape as the coin
/// path. Returns `None` if fewer than `threshold + 1` verifying shares
/// are supplied or if the aggregate fails self-verification.
pub fn combine_recp_shares(
    authorities: &[PublicKey],
    threshold: usize,
    round: u64,
    block_hash: &Digest,
    shares: &[(PublicKey, Vec<u8>)],
) -> Option<Vec<u8>> {
    let key_set = deterministic_recp_key_set(authorities, threshold);
    let pks: PublicKeySet = key_set.public_keys();
    let message = recp_message(round, block_hash);

    let mut unique_shares = BTreeMap::new();
    for (authority, bytes) in shares {
        let index = match authority_index(authorities, authority) {
            Some(i) => i,
            None => continue,
        };
        let share: SignatureShare = match bincode::deserialize(bytes) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if !pks.public_key_share(index).verify(&share, &message) {
            continue;
        }
        unique_shares.entry(index).or_insert(share);
    }

    if unique_shares.len() < threshold + 1 {
        return None;
    }

    let signature = pks.combine_signatures(&unique_shares).ok()?;
    if !pks.public_key().verify(&signature, &message) {
        return None;
    }
    Some(signature.to_bytes().to_vec())
}

/// Verify an aggregate BLS signature against the master public key.
/// Counterpart to `combine_recp_shares`. Used by `LeaderLink::verify`
/// to validate `LeaderProof::ExclusiveCommit` proofs.
pub fn verify_recp_aggregate(
    authorities: &[PublicKey],
    threshold: usize,
    round: u64,
    block_hash: &Digest,
    aggregate_bytes: &[u8],
) -> bool {
    let key_set = deterministic_recp_key_set(authorities, threshold);
    let pks: PublicKeySet = key_set.public_keys();
    // `threshold_crypto::Signature::from_bytes` requires a fixed 96-byte input.
    let bytes: [u8; 96] = match aggregate_bytes.try_into() {
        Ok(b) => b,
        Err(_) => return false,
    };
    let signature = match threshold_crypto::Signature::from_bytes(bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };
    pks.public_key().verify(&signature, recp_message(round, block_hash))
}
