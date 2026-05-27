// Wahoo consensus passthrough.
//
// Wahoo runs as an end-to-end self-contained protocol inside `primary`
// (`primary::wahoo::Node`). The primary already orders blocks (via Wahoo's
// `tryToCommitLeader` + `commitAncestorBlocks`) and forwards each
// committed block — wrapped in a synthetic `Certificate` carrying only
// the round/origin metadata — through `tx_consensus`. Our job here is
// only to forward those committed certificates to `tx_output` so that
// downstream consumers (workers, benchmark scripts) see them in the same
// stream they would for Narwhal/Bullshark/Shortfin-family.

use crate::Consensus;
use log::{info, warn};

pub(crate) async fn run(consensus: &mut Consensus) {
    while let Some(certificate) = consensus.rx_primary.recv().await {
        #[cfg(not(feature = "benchmark"))]
        info!("Wahoo committed {}", certificate.header);

        #[cfg(feature = "benchmark")]
        for digest in certificate.header.payload.keys() {
            info!("Committed {} -> {:?}", certificate.header, digest);
        }

        if let Err(e) = consensus.tx_output.send(certificate).await {
            warn!("Failed to output Wahoo certificate: {}", e);
        }
    }
}
