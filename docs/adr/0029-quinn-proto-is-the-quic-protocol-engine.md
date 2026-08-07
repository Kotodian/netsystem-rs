# Quinn-proto is the QUIC protocol engine

Status: accepted

Hammer uses the vendored `quinn-proto` state machine as its QUIC protocol engine and does not use the `quinn` socket, async runtime, task, or endpoint orchestration layers. Hammer Data Workers and Graph Nodes own UDP ingress and egress, clocks, timer dispatch, packet buffers, worker scheduling, and Session integration around the sans-I/O engine. This keeps Quinn's packet-number-space, ACK/loss, PTO, flow-control, connection-ID, migration, stream, and TLS 1.3 protocol logic while preserving Hammer's synchronous worker-owned data plane; writing another QUIC state machine or adapting the Tokio/socket runtime are rejected.
