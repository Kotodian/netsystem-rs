# Transport workers own transport state and timers

Session Worker and transport workers are separate worker-local owners. Each transport worker owns its protocol objects, lookup, timer wheel, tick state, expired Timer Tokens, and exact timer dispatch; TCP and QUIC therefore keep independent timer policy and resolution. Session Runtime does not store, advance, interpret, or deliver transport timers.

SessionQueueNode samples one absolute time per dispatch and passes it to each registered transport time subscriber before session control and I/O work. Each transport worker converts that time to its own ticks and applies its own expiry budget; SessionQueueNode does not define a shared timer resolution or compute transport ticks.

The worker graph schedules PreInput nodes before Input/Driver nodes for both polling and interrupt dispatch. SessionQueueNode remains an Input/Driver node and is not reclassified as PreInput.

A session entry stores the protocol dispatch key and is generic over an opaque transport-provided, generation-safe index. The indexed TCP connection, QUIC connection context, or QUIC stream context stores the reverse SessionId. Session Worker owns app/session FIFOs and scheduling, while TcpWorker and future QuicWorker remain separate; there is no TcpQueue wrapper and no transport-specific state in Session Runtime.

Rust transport integration uses a generic `SessionTransport<Index>` trait and a compile-time transport set rather than copying VPP's C function table. The concrete set, such as TCP plus QUIC, is statically dispatched and monomorphized; the protocol id selects a member without `dyn Trait` or protocol-specific enum variants in Session Runtime. This preserves VPP's session worker, protocol dispatch, connection index, and per-transport worker ownership semantics while using Rust's type system for dispatch.

`SessionTransport<Index>` selects an associated typed TX strategy. Session-Packetized TX keeps FIFO selection and buffer preparation in Session Runtime before TCP commits headers and protocol state; Transport-Internal TX lets a QUIC connection engine schedule streams, multiplex, encrypt, and emit packets while payload ownership remains in Session FIFO. The interface does not require transports to implement inapplicable optional TX methods.

Transport methods receive the concrete generic `SessionWorker<Index, Seg>` directly. Session Worker fields remain private and expose only transport-neutral readiness, lifecycle notification, FIFO, ACK cleanup, and RX enqueue operations. This permits a QUIC connection update to affect multiple stream sessions without `dyn Trait`, TLS state, raw-pointer type erasure, or an additional session-access wrapper.

TCP timer identity is a private `TcpTimerKind`, and typed timer sets distinguish wheel-armed timers from expired timers pending exact dispatch. Expiry moves one exact kind from armed to pending; reset clears both, and dispatch ignores a stale expiry if the same kind was rearmed while pending. A timer is active when either set contains its kind, matching VPP's live-handle-or-pending definition without exposing raw timer ids, timer counts, or masks.

A private `TcpTimers` module owns the TCP timer wheel, resolution and clock state, raw expiry scratch, and `TcpTimerToken` queue. Its set, update, and reset operations immediately synchronize the wheel with a connection's typed timer sets. TcpWorker drains exact tokens and invokes connection handlers; TcpTimers does not depend on Session Worker or produce timer-action carriers.

Session ownership is stored as a closed `SessionState<Index>` enum whose variants carry distinct typed state records for Active, App Closed, Transport Closed, and Closed; Transport Deleted carries no transport index. Closed retains the index while TCP TIME_WAIT, QUIC connection draining, or QUIC stream acknowledgement and cleanup still owns the transport object. State-specific methods consume one record and return only legal successor records. TCP connection state, QUIC connection state, and QUIC stream send/receive state remain separate protocol-private state machines.
