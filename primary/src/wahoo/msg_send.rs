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

use crate::messages::{Header, RecpMessage, Vote};
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

/// Phase C Step 4c: broadcast a paper-Section-IV-B RECP share. Unlike
/// the `SignedWahoo`-wrapped traffic above, RECPs are first-class
/// `PrimaryMessage::Recp` envelopes (the share itself authenticates
/// the message via the threshold-signature scheme; an outer ED25519
/// wrapper would be redundant).
pub async fn broadcast_recp(
    sender: &mut ReliableSender,
    committee: &Committee,
    self_name: &PublicKey,
    recp: RecpMessage,
) -> Vec<CancelHandler> {
    let bytes = bincode::serialize(&PrimaryMessage::Recp(recp))
        .expect("Failed to serialize RECP message");
    let addresses: Vec<SocketAddr> = committee
        .others_primaries(self_name)
        .into_iter()
        .map(|(_, addrs)| addrs.primary_to_primary)
        .collect();
    sender.broadcast(addresses, Bytes::from(bytes)).await
}

/// Phase B Step 3d: broadcast a Wahoo PB/EPBC block as a first-class
/// `PrimaryMessage::Header(_)` envelope. The header's inline
/// `signature` field authenticates it (set by the caller via
/// `SignatureService::request_signature(header.id)`) so the receiver
/// never needs the legacy `SignedWahoo` outer wrapper.
pub async fn broadcast_header(
    sender: &mut ReliableSender,
    committee: &Committee,
    self_name: &PublicKey,
    header: Header,
) -> Vec<CancelHandler> {
    let bytes = bincode::serialize(&PrimaryMessage::Header(header))
        .expect("Failed to serialize Wahoo Header");
    let addresses: Vec<SocketAddr> = committee
        .others_primaries(self_name)
        .into_iter()
        .map(|(_, addrs)| addrs.primary_to_primary)
        .collect();
    sender.broadcast(addresses, Bytes::from(bytes)).await
}

/// Phase B Step 3d: unicast a Wahoo PB vote as a first-class
/// `PrimaryMessage::Vote(_)` envelope. Same authentication model as
/// `broadcast_header`: caller has already populated `vote.signature`.
pub async fn send_vote(
    sender: &mut ReliableSender,
    committee: &Committee,
    target: &PublicKey,
    vote: Vote,
) -> CancelHandler {
    let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
        .expect("Failed to serialize Wahoo Vote");
    let address = committee
        .primary(target)
        .expect("Wahoo send target not in committee")
        .primary_to_primary;
    sender.send(address, Bytes::from(bytes)).await
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
