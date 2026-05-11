// Port of `Wahoo-main/wahoo/tools.go` (lines 1-69).
//
// The Go reference encodes messages with `encoding/json`. NovelDAG uses
// `bincode` everywhere for efficiency; we keep `encode`/`decode` as the
// idiomatic primitives so call sites read the same way as the Go source.
// `getHash` becomes a Hash trait impl on `WahooBlock` further below.

use crate::wahoo::messages::WahooBlock;
use crypto::{Digest, Hash};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use std::time::{SystemTime, UNIX_EPOCH};

/// `tools.go::encode`. Returns the bincode wire form of any `Serialize`.
pub fn encode<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bincode::serialize(v).expect("Wahoo encode failed")
}

/// `tools.go::decode`. Mirrors the Go signature; panics on failure to match
/// the Go behaviour (Go decodes panic via `panic(err)` in callers). Kept
/// as part of the 1:1 port surface even though the Rust state machine
/// uses the typed `WahooMessage` enum directly.
#[allow(dead_code)]
pub fn decode<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::deserialize(bytes).expect("Wahoo decode failed")
}

/// `tools.go::generateTX(s int)` — Go produces `size` random bytes mod 200.
/// In the unified-pipeline port we no longer mint synthetic txs from the
/// primary side (Wahoo blocks now carry worker-batch digests like the
/// other three protocols), but the helper is kept for parity with the
/// Go reference and as a building block for future stress tests.
#[allow(dead_code)]
pub fn generate_tx(size: usize) -> Vec<u8> {
    vec![0x42; size]
}

/// `time.Now().UnixNano()` equivalent.
pub fn unix_nano_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// `tools.go::Block.getHash()` and `getHashAsString()` — folded into the
/// shared `crypto::Hash` trait so callers can use `block.digest()`.
impl Hash for WahooBlock {
    fn digest(&self) -> Digest {
        let bytes = encode(self);
        let mut hasher = Sha512::new();
        hasher.update(&bytes);
        let out = hasher.finalize();
        Digest(out[..32].try_into().expect("sha512 truncation"))
    }
}
