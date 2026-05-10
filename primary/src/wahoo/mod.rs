// Wahoo protocol port (F2 — full functional/logical fidelity with the Go
// reference at `Wahoo-main/wahoo/`). Gated by `DagProtocol::Wahoo`; never
// participates in Narwhal/Bullshark/NovelDAG paths.
//
// Go → Rust file mapping:
//   wahoo/data_struct.go  → wahoo::messages
//   wahoo/msg_type.go     → wahoo::messages::WahooMessage (variant tags)
//   wahoo/tools.go        → wahoo::tools
//   wahoo/msg_send.go     → wahoo::msg_send
//   wahoo/pb.go           → wahoo::pb
//   wahoo/node.go +
//   wahoo/msg_handle.go   → wahoo::node (merged: tightly coupled state
//                           machine vs. message dispatcher in Go)

pub mod messages;
pub mod msg_send;
pub mod node;
pub mod pb;
pub mod tools;

// Re-export only what crosses module boundaries. Inner types remain
// reachable via their module paths inside the crate.
pub use messages::WahooMessage;
pub use node::{CommittedBlock, Node};
