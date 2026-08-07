# QUIC Stream payload is Session-owned

Status: accepted

Hammer follows VPP's QUIC stream data boundary: Stream payload is retained only in the Stream Session FIFOs, while the QUIC engine retains stream offsets, flow-control state, acknowledged and lost ranges, and other protocol metadata. The vendored Quinn integration therefore does not use `SendBuffer::unacked_segments` or the receive `Assembler` as private Stream payload stores. TX packet construction reads the Stream TX FIFO by offset and drops only the contiguous acknowledged prefix; decrypted RX STREAM frames write directly to the Stream RX FIFO, using its out-of-order support when needed; Application dequeue advances Quinn receive credit. UDP datagram processing borrows lower Session FIFO records and writes through destination reservations without per-datagram `BytesMut`, `Vec`, private payload, or Data-Plane Buffer copies. Quinn remains the protocol state machine but does not become another payload owner.
