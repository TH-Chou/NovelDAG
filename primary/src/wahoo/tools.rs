// Port of `Wahoo-main/wahoo/tools.go` (lines 1-69).
//
// The Go reference encodes messages with `encoding/json`. NovelDAG uses
// `bincode` everywhere for efficiency; we keep `encode`/`decode` as the
// idiomatic primitives so call sites read the same way as the Go source.
//
// Phase B Step 3b: `WahooBlock` is now a type alias for `messages::Header`,
// so the standalone `impl Hash for WahooBlock` (which hashed the bincode
// encoding of the whole struct) has been removed; callers use
// `Header::digest()` directly, which mixes specific fields into Sha512 in
// a defined order. This changes the on-wire digest values relative to the
// Go reference, but all Wahoo nodes compute digests identically so the
// protocol remains consistent.

use std::time::{SystemTime, UNIX_EPOCH};

/// `tools.go::encode`. Returns the bincode wire form of any `Serialize`.
#[allow(dead_code)]
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

// `tools.go::Block.getHash()` is now provided directly by
// `messages::Header`'s `Hash` impl (Phase B Step 3b made WahooBlock a
// type alias for Header).
