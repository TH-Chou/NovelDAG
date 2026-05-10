// Port of `Wahoo-main/wahoo/msg_send.go`.
//
// In the Go reference each broadcast/send acquires a TCP conn from the
// pool and writes a `(msgType, msgBody, sig)` triple — see
// `wahoo/msg_send.go:77-119`. The signature is computed by
// `sign.SignEd25519(n.privateKey, msgAsBytes)` over the JSON-encoded
// message body, and the recipient verifies it via
// `wahoo/msg_handle.go::HandleMsgLoop` (lines 10-46) using the
// authority's public key looked up via the message's sender field.
//
// We replicate that envelope shape with the `SignedWahoo { msg, sig }`
// struct in `messages.rs`. The Node task is responsible for computing
// the signature (asynchronously via `SignatureService`); these
// functions are pure transport — they bincode-serialise
// `PrimaryMessage::Wahoo(SignedWahoo)` and hand it to the existing
// `network::ReliableSender`.
//
// Encoding deviation from Go: we use bincode (consistent with the rest
// of NovelDAG) instead of JSON. Hashing follows suit (Sha512 truncated
// to 32 bytes via the `Hash` trait, vs. SHA-256 in Go). Both choices
// preserve protocol semantics — the only observable consequence is
// that a Rust Wahoo node cannot interoperate over the wire with a Go
// Wahoo node, which is expected for a port.

use crate::primary::PrimaryMessage;
use crate::wahoo::messages::SignedWahoo;
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::{CancelHandler, ReliableSender};
use std::net::SocketAddr;

/// `msg_send.go::broadcast` — fan-out to every peer in the committee
/// (excluding `self_name`, mirroring `clusterAddrWithPorts` semantics).
pub async fn broadcast(
    sender: &mut ReliableSender,
    committee: &Committee,
    self_name: &PublicKey,
    signed: SignedWahoo,
) -> Vec<CancelHandler> {
    let bytes = bincode::serialize(&PrimaryMessage::Wahoo(signed))
        .expect("Failed to serialize Wahoo message");
    let addresses: Vec<SocketAddr> = committee
        .others_primaries(self_name)
        .into_iter()
        .map(|(_, addrs)| addrs.primary_to_primary)
        .collect();
    sender.broadcast(addresses, Bytes::from(bytes)).await
}

/// `msg_send.go::send` — direct (unicast) delivery to a single peer.
pub async fn send(
    sender: &mut ReliableSender,
    committee: &Committee,
    target: &PublicKey,
    signed: SignedWahoo,
) -> CancelHandler {
    let bytes = bincode::serialize(&PrimaryMessage::Wahoo(signed))
        .expect("Failed to serialize Wahoo message");
    let address = committee
        .primary(target)
        .expect("Wahoo send target not in committee")
        .primary_to_primary;
    sender.send(address, Bytes::from(bytes)).await
}
