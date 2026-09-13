// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use ed25519_dalek::Sha512;
use rand::rngs::StdRng;

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        let hash = Sha512::digest(self);
        Digest(hash[..32].try_into().unwrap())
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

#[test]
fn import_export_public_key() {
    let (public_key, _) = keys().pop().unwrap();
    let export = public_key.encode_base64();
    let import = PublicKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let (_, secret_key) = keys().pop().unwrap();
    let export = secret_key.encode_base64();
    let import = SecretKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    let bad_message: &[u8] = b"Bad message!";
    let digest = bad_message.digest();
    assert!(signature.verify(&digest, &public_key).is_err());
}

#[test]
fn verify_valid_batch() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let signatures: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_ok());
}

#[test]
fn verify_invalid_batch() {
    // Make 2 valid signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let mut signatures: Vec<_> = (0..2)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Add an invalid signature.
    let (public_key, _) = keys.pop().unwrap();
    signatures.push((public_key, Signature::default()));

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_err());
}

#[test]
fn verify_valid_batch_with_distinct_digests() {
    let mut keys = keys();
    let messages: [&[u8]; 3] = [b"first", b"second", b"third"];
    let signatures: Vec<_> = messages
        .iter()
        .copied()
        .map(|message| {
            let digest = message.digest();
            let (public_key, secret_key) = keys.pop().unwrap();
            let signature = Signature::new(&digest, &secret_key);
            (digest, public_key, signature)
        })
        .collect();

    assert!(Signature::verify_batch_digests(&signatures).is_ok());
}

#[test]
fn verify_invalid_batch_with_distinct_digests() {
    let mut keys = keys();
    let messages: [&[u8]; 3] = [b"first", b"second", b"third"];
    let mut signatures: Vec<_> = messages
        .iter()
        .copied()
        .map(|message| {
            let digest = message.digest();
            let (public_key, secret_key) = keys.pop().unwrap();
            let signature = Signature::new(&digest, &secret_key);
            (digest, public_key, signature)
        })
        .collect();
    signatures[1].0 = b"tampered".as_slice().digest();

    assert!(Signature::verify_batch_digests(&signatures).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest.clone()).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn shared_threshold_coin_recovers_and_caches() {
    let authorities: Vec<_> = keys().into_iter().map(|(public, _)| public).collect();
    let coin = coin::ThresholdCoin::new(&authorities, coin::threshold(authorities.len()));
    let round = 7;
    let first = coin.make_share(&authorities[0], round).unwrap();
    let second = coin.make_share(&authorities[1], round).unwrap();

    assert!(coin
        .recover(round, &[(authorities[0], first.clone())])
        .is_none());
    let recovered = coin
        .recover(
            round,
            &[
                (authorities[0], vec![0; first.len()]),
                (authorities[0], first),
                (authorities[1], second),
            ],
        )
        .expect("two valid shares recover an f+1 coin for n=4");

    assert_eq!(coin.recover(round, &[]), Some(recovered));
    assert_eq!(
        coin.make_share(&authorities[0], round),
        coin.make_share(&authorities[0], round)
    );
}

#[test]
fn shared_threshold_coin_output_is_threshold_independent() {
    let authorities: Vec<_> = keys().into_iter().map(|(public, _)| public).collect();
    let f_plus_one_coin = coin::ThresholdCoin::new(&authorities, 1);
    let two_f_plus_one_coin = coin::ThresholdCoin::new(&authorities, 2);
    let round = 11;

    let f_plus_one_shares = authorities
        .iter()
        .take(2)
        .map(|authority| {
            (
                *authority,
                f_plus_one_coin.make_share(authority, round).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let two_f_plus_one_shares = authorities
        .iter()
        .take(3)
        .map(|authority| {
            (
                *authority,
                two_f_plus_one_coin.make_share(authority, round).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        f_plus_one_coin.recover(round, &f_plus_one_shares),
        two_f_plus_one_coin.recover(round, &two_f_plus_one_shares)
    );
}

#[test]
fn pseudo_random_coin_is_order_independent() {
    let authorities: Vec<_> = keys().into_iter().map(|(public, _)| public).collect();
    let mut reversed = authorities.clone();
    reversed.reverse();
    let first = coin::CoinCommittee::new(&authorities);
    let second = coin::CoinCommittee::new(&reversed);

    assert_eq!(first.pseudo_random(9), second.pseudo_random(9));
    assert_ne!(first.pseudo_random(9), first.pseudo_random(10));
    assert_eq!(first.round_robin(9), 9);
    assert_eq!(
        first.leader(first.pseudo_random(9), 0),
        second.leader(second.pseudo_random(9), 0)
    );
}

#[test]
fn round_robin_slots_cover_the_committee_for_strided_protocol_rounds() {
    let authorities: Vec<_> = keys().into_iter().map(|(public, _)| public).collect();
    let schedule = coin::CoinCommittee::new(&authorities);

    for (first_round, stride) in [(1, 4), (2, 2), (1, 1)] {
        let leaders = (0..authorities.len())
            .map(|index| {
                let round = first_round + index as u64 * stride;
                let slot = coin::round_robin_slot(round, first_round, stride);
                schedule.leader(schedule.round_robin(slot), 0)
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(leaders.len(), authorities.len());
    }
}
