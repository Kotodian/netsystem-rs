# Hammer Data Plane

Hammer is a VPP-style packet graph runtime in Rust. This context defines the graph, frame, buffer, session, transport, and memory ownership language used across the data plane.

## Packet Graph

**Packet Graph**:
A worker-local directed graph of packet-processing graph nodes. Packets move through the graph in frames and follow named next arcs selected by the current graph node.
_Avoid_: pipeline, middleware chain, callback stack, async task graph

**Graph Node**:
A packet graph module that consumes a Pending Frame and may enqueue packet indexes to one or more Next Frames. A graph node owns packet classification or transformation for one graph step, not application I/O policy.
_Avoid_: service, handler, processor, callback

**Driver Node**:
A graph node that brings packets or external readiness into the data plane. Driver nodes are runtime roles, not protocol business roles.
_Avoid_: protocol driver, socket task, app poller

**Internal Node**:
A graph node that performs data-plane work while packet ownership stays on the current data worker. Internal nodes transform metadata, split frames, or select next arcs.
_Avoid_: helper node, background worker, side task

**Next Arc**:
A named graph edge selected through a current-node-local slot for a packet or frame. Graph Runtime resolves Next Arc slots through graph registration; protocol code does not route packets with target node ids.
_Avoid_: output callback, destination handler, manually routed node id

**Graph Fanout**:
A worker-local graph operation that groups packet ownership by selected Next Arc and makes the resulting Next Frames visible at the current Graph Node dispatch boundary. Cross-worker ownership transfer remains Handoff, not Graph Fanout.
_Avoid_: cross-thread fanout, handoff enqueue, output router

**Graph Identity**:
The core vocabulary that names packet graph participants and their static edge shape, such as node ids, node handles, node kinds, node states, node registrations, and next-arc labels. Graph identity is data-plane vocabulary, not graph execution policy.
_Avoid_: adapter node identity, runtime-only id, execution trait

**Pending Frame**:
A Hammer-owned frame scheduled for a graph node to process. A Pending Frame is the current graph node's input ownership state.
_Avoid_: input vector, borrowed frame, current frame carrier

**Next Frame**:
A Hammer-owned frame prepared by the current graph node for a selected next arc. It is not a plain frame plus an external node parameter, and it is put with `put_next_frame`.
_Avoid_: submit frame, `submit_frame`, `NextFrame` carrier, node-attached frame

**Frame State**:
The concrete state payload inside a frame. `Next` and `Pending` are state bodies that own frame fields directly, not marker types and not tags for a separate storage enum.
_Avoid_: marker-backed frame state, separate frame storage, separate frame owner

**Node Dispatch Result**:
The outcome of running a graph node over a Pending Frame. Next Frames are acquired and put during node execution; they are not carried back as the node dispatch result.
_Avoid_: `NodeNextFrames`, `NextFrame` carrier, `Current(NodeId)`, forwarding result carrier, node returns next frames

## Runtime

**Data Worker**:
A worker that owns data-plane hot-path state for one execution lane. Data-plane state is worker-local unless a handoff or control-plane barrier explicitly crosses the seam.
_Avoid_: tokio worker, control thread, generic thread

**Main Loop Step**:
One fixed-order pass through worker scheduling, ready graph nodes, local tasks, driver polling, timers, and exit checks. Step order is part of the runtime semantics.
_Avoid_: event loop tick, reactor pass, arbitrary scheduler iteration

**Graph Runtime**:
The runtime-owned execution context for a Data Worker's Packet Graph, including graph node state, next-arc resolution, pending-frame scheduling, readiness, dispatch, and graph runtime statistics.
_Avoid_: adapter graph runtime, scheduler helper, node registry wrapper

**Barrier Synchronization**:
The control-plane mechanism that pauses data workers at a known point so control changes can observe a stable data-plane state. It is not a lock taken around hot-path packet processing.
_Avoid_: global mutex, graph lock, packet-path synchronization

**Handoff**:
A data-plane transfer of packet ownership from one worker to another through worker-owned slots and a handoff graph node. Handoff moves buffer indexes, not protocol payload copies.
_Avoid_: crossbeam channel payload, TCP migration copy, app queue transfer

## Buffer And Memory

**Data-Plane Primitive**:
A packet-path data structure that represents Hammer's shared buffer, frame, packet cursor, frame ownership, or graph identity vocabulary. Data-plane primitives are domain primitives built from generic infrastructure, not generic collections or runtime scheduling policy.
_Avoid_: infra container, runtime scheduler state, helper object

**Data-Plane Buffer**:
A VPP-style packet buffer with header state and inline packet storage. Protocol code may move the current window, prepend headers, append bytes, and link buffers by buffer-header state.
_Avoid_: `Vec<u8>` packet, packet object, protocol-owned payload copy

**Index**:
A copyable data-plane identity containing pool, slot, and generation facts. Buffer and frame pools use the same concrete value, but only pools construct it. Index does not own release policy; buffer ownership belongs to the Frame or other domain owner that contains the identity.
_Avoid_: pool-specific index family, index alias, per-index owner, per-index release context

**Buffer Chain**:
A packet represented by linked data-plane buffers using buffer-header state. Sharing or chaining is represented by buffer metadata, not by feature-specific owner records.
_Avoid_: TCP chain wrapper, single-buffer owner wrapper, payload segment list

**Frame Ownership**:
The worker-local ownership of all buffer references contained in a Pending Frame or Next Frame. Moving indexes between Frames transfers this ownership as a batch, and dropping the owning Frame releases the references that remain in it.
_Avoid_: per-index owner, manual buffer free, borrowed input ownership

**Packet Cursor**:
Packet metadata that records parsed network and transport offsets for a data-plane buffer. It is a parsed-position fact, not a replacement for buffer header state.
_Avoid_: packet view object, parser context, protocol cursor helper

**Segment**:
A memory domain that can back FIFOs and message queues for either local or shared-memory app/session exchange. Segment choice is an adapter detail behind the app/session seam.
_Avoid_: heap-only session storage, mmap business object, transport buffer

**Session FIFO**:
The app/session byte store for RX and TX payload ownership. App-to-session copying happens at this seam so future cross-process sessions preserve the same ownership model.
_Avoid_: AppRing, SQE, CQE, submission queue, completion queue, TCP-owned payload copy

**Session Message Queue**:
The app/session event queue used to signal RX enqueue, TX dequeue, connect, and close facts. It carries session events, not payload bytes.
_Avoid_: payload channel, async stream, per-protocol event bus

## Session And Transport

**App/Session Seam**:
The seam where application-owned bytes become session-owned bytes and session events become application-visible facts. This seam is designed for both local and cross-process app adapters.
_Avoid_: direct TCP app callback, io_uring app ring, socket-like stream hidden inside TCP

**Session Runtime**:
The worker-local module that owns session readiness, app/session FIFO access, and TX packet preparation. It schedules session work; registered transport dispatch coordinates transport worker updates without exposing transport connections, timers, or exact timer dispatch to Session Runtime.
_Avoid_: TCP runtime, congestion-control scheduler, app polling loop

**Transport Timer Policy**:
The transport-owned rules that decide which timer kinds are active, when they should expire, and how exact Timer Token expiry changes transport state. The transport worker owns scheduling and dispatch; Session Runtime does not store, advance, interpret, or deliver transport timers.
_Avoid_: session timer policy, timer-wheel policy, generic keep mask

**Transport Worker State**:
The worker-local owner of protocol-specific transport objects, transport timer scheduling, expired Timer Tokens, and exact timer dispatch. A registered transport dispatch advances the worker without exposing TCP connection, QUIC connection, or QUIC stream state to Session Runtime.
_Avoid_: session connection pool, session timer wheel, protocol state in Session Runtime

**TX Transaction**:
The send-side unit of work owned by Session Runtime for a Session-Packetized TX transport: selecting session-owned TX bytes, preparing data-plane buffers, and making the packet visible to the Packet Graph. Transport logic supplies facts and output intent but does not own payload bytes or session scheduling.
_Avoid_: TX helper, TCP send path, callback chain, payload selection helper

**Session-Packetized TX**:
A typed transport TX strategy in which Session Runtime selects FIFO bytes and prepares the TX Batch before transport commits protocol state and output intent. TCP uses this strategy.
_Avoid_: transport-internal packetization, TCP-owned payload copy, generic custom TX

**Transport-Internal TX**:
A typed transport TX strategy in which Session Runtime exposes session-owned bytes and readiness to a transport engine that schedules, multiplexes, packetizes, and emits transport packets. QUIC stream TX uses this strategy; payload ownership remains in the Session FIFO.
_Avoid_: fake push-header path, per-stream QUIC packet buffer, session-owned QUIC packetization

**TX Batch**:
One TX Transaction may prepare multiple data-plane buffers before a single transport-owned TX action materializes output intent and commits transport state for the batch. A TX Batch is committed and made graph-visible as one unit, preserving VPP-style amortization and ordering across the buffers it contains.
_Avoid_: per-buffer callback loop, one-segment transaction, output carrier list

**Transport TX Action**:
A transport-owned send-side boundary operation for a TX Batch. It materializes protocol output intent and commits transport send state without exposing protocol headers, recovery records, timer masks, or output carriers to Session Runtime.
_Avoid_: session commit callback, TCP helper, prepare/commit/cancel transaction API, output carrier builder

**TX Batch Flush**:
The Session Runtime step that makes a committed TX Batch visible to the Packet Graph by putting its buffers to the selected next arc. It happens after the transport-owned TX action and is not a TCP-owned output carrier.
_Avoid_: TCP output carrier, per-buffer `put_next_frame`, pre-commit flush

**Session Schedule Pending Bit**:
A session-owned scheduling fact that records whether this session has already been staged for worker-local Session Runtime work. It is cleared when the worker consumes the staged session work, mirroring VPP's session flag plus worker handle-vector coalescing shape.
_Avoid_: ready-session token, generic dedup queue, app wake flag, TCP timer flag

**Session Work Batch**:
The worker-local batch of session ids staged for Session Runtime work. It is an append-and-drain batch; duplicate suppression is decided by each session's Schedule Pending Bit, not by a separate hash-backed ready queue object.
_Avoid_: Session Ready Queue, DedupFifo, ReadySession list, protocol-specific scheduler

**Session Handle**:
The VPP-shaped app/runtime routing identity for a session: session index plus worker/thread index. It is not a pool generation token. Control close events carry a Session Handle; IO readiness events do not invent a stronger identity.
_Avoid_: generation-bearing session handle, SessionId-as-app-handle, opaque cookie without worker index

**Session Event**:
An app↔session message-queue event aligned with VPP `session_event_t`. IO events carry session index only; control close/reset events carry a Session Handle. Consume paths drop events whose session slot is free or unmapped. Slot reuse after free may still target a replacement session; that window matches VPP and is not closed by adding generation to the event.
_Avoid_: generation-safe SessionEvt, Index-in-event as ownership proof, one identity field for every event kind

**Session Control Event**:
A worker-local session lifecycle command, such as disconnect, dispatched by Session Runtime separately from TX/RX session work. Control events may invoke transport close handling, but they are not readiness facts and do not enter the Session Work Batch.
_Avoid_: close-ready flag, ready-queue close request, synthetic ready boolean, TX event

**Session Lifecycle**:
The session-owned typed state machine that coordinates independently owned application and transport objects. Its stored states are Active, App Closed, Transport Closed, Closed, and Transport Deleted; Closed retains the Transport Index until asynchronous transport cleanup finishes, while Transport Deleted does not. TCP connection, QUIC connection, and QUIC stream state machines remain protocol-private.
_Avoid_: TCP close state, QUIC stream state, close boolean, immediate cross-owner deletion

**Transport Deleted**:
A Session Lifecycle state in which the app-facing transport object no longer exists while the session remains long enough to complete application-side cleanup. It has no Transport Index.
_Avoid_: stale connection index, TCP closed flag, tombstone connection

**Transport Connection**:
The transport-worker-owned protocol state associated with a session, such as TCP sequence, ACK, recovery, and timer facts. It does not own app/session FIFOs or Session Runtime scheduling.
_Avoid_: app session, runtime session, socket object

**Transport Index**:
An opaque transport-provided index from a Session to its app-facing transport object. The object may be a TCP connection, QUIC connection, or QUIC stream; Session Runtime preserves and passes the index without interpreting the object's kind, pool representation, parent relationship, or state.
_Avoid_: TCP connection in Session Runtime, QUIC stream id in Session Runtime, SessionId used as a transport index

**TX Byte Retention**:
The session-owned retention of transmitted application bytes until transport ACK cleanup releases them. Recovery retransmits from session-owned bytes and transport facts, not from private payload copies.
_Avoid_: recovery payload cache, TCP-owned send buffer, retransmit `Vec`

**TCP Output Intent**:
The transport fact that tells the TCP output graph node what header and sequence metadata to prepend to session-owned payload bytes. It is an output intent, not a recovery record or receive-ordering record.
_Avoid_: sent segment record, output carrier, hand-built TCP header object

**Transport-Neutral TX Fact**:
A send-side fact Session Runtime may use without knowing protocol header semantics, such as TX offset, send space, Send Goal Size, buffer index, next arc, or transport scheduling intent. These facts are the only transport/session TX seam; TCP header fields, recovery records, timer masks, and TCP Output Intent materialization stay inside TCP transport.
_Avoid_: TCP segment fact, header fact, recovery callback, timer mask handoff

**RX Enqueue Locality**:
The receive-side ownership rule that accepted payload bytes are enqueued into the Session FIFO by Session Runtime on the owning Data Worker. Transport supplies only RX delivery facts such as buffer identity, relative offset, and in-order/OOO status, then consumes the returned RX Delivery for sequence, ACK, and SACK decisions. App notification, app-readable readiness, and RX FIFO capacity facts stay session/runtime-owned.
_Avoid_: TCP-owned RX FIFO writes, app wakeups in transport, cross-worker RX copy, protocol-owned app queue events

**RX Delivery**:
The transport-neutral successful result of a Session Runtime RX enqueue, modeled as legal receive outcomes rather than a bag of nullable fields. It distinguishes not-accepted, in-order delivery, and out-of-order delivery; accepted-byte and OOO-span invariants are represented by non-zero domain values, while errors use the existing `CoreResult` boundary.
_Avoid_: RX field bag, zero-length accepted delivery, zero-length OOO fact, RX error enum, TCP enqueue status type, app notification result, ready-queue command

**OOO RX Delivery**:
Receive-side delivery where out-of-order payload is retained by the Session FIFO's OOO storage and returned to transport as OOO facts for SACK and ACK policy, without making app-readable RX work visible until in-order bytes are delivered.
_Avoid_: app-readable OOO event, TCP-owned OOO payload store, immediate app wake for OOO bytes

**Send Goal Size**:
A transport-selected payload sizing fact used by Session Runtime TX packetization. It may equal MSS or a larger GSO-sized goal, but it is still transport-neutral because Session Runtime uses it only to size payload bytes and buffers; offload flags, GSO metadata, TCP option length, and header semantics stay in transport/output code.
_Avoid_: TCP MSS field, GSO flag, header option, output metadata

**Timer Token**:
The exact transport timer kind produced by Transport Worker State when a transport timer expires. The transport worker dispatches this token directly to the owning Transport Connection instead of scanning timer kinds or routing transport timer state through Session Runtime.
_Avoid_: all-timer sweep, timer-kind discovery, guessed expired timer

## Lookup

**TCP Dataplane Lookup**:
An exact-match packet-path lookup that routes a TCP packet tuple or listener endpoint to the existing session, pending open, or listener handling path.
_Avoid_: every TCP hash table, control-plane bookkeeping map, test helper index

**TCP Lookup Key**:
The existing TCP/session domain key used for dataplane exact-match lookup, with `BihashKey` hashing implemented on that key type and equality coming from Rust `Eq` instead of converting call sites to raw words.
_Avoid_: `TcpBihashKey`, `TcpV4RouteKey`, raw `[u64; N]` key plumbing in TCP code

**Bihash Value**:
The opaque integer stored in a dataplane bihash entry, usually a packed pool index or session handle whose target object is owned elsewhere.
_Avoid_: storing business records in bihash, public free-slot marker traits
