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
A named graph edge selected through a current-node-local `u16` slot for a packet or frame. `NodeNext` is only that copyable local-slot decision (`slot() -> u16`); Graph Runtime alone resolves the slot to a target node identity. Static next enums keep count and name metadata inherently; dynamic arc registration returns a checked `u16`.
_Avoid_: output callback, destination handler, manually routed node id, second local-next type, next-count trait requirement

**Forwarding Local Next**:
FIB / DPO / load-balance / adjacency packet-path next values are current-node-local `u16` slots owned by the classifying graph node (lookup or adjacency-rewrite). Control-plane wiring registers those slots; lookup and rewrite enqueue through Graph Fanout, not by storing resolved `NodeId` in forwarding tables.
_Avoid_: FIB-stored NodeId, adjacency NodeId hot path, forwarding-owned frame get/push/put

**Feature Arc**:
An ordered per-interface chain of feature Graph Nodes. Control may retain target node identities while compiling under a barrier; each published transition stores only a predecessor-local `u16` and the next configuration index. Packet traversal advances configuration progress once per feature, preserves the caller default when no feature applies, and never resolves target node identities. Feature config progress is distinct from Handoff continuation state.
_Avoid_: packet-path NodeId feature next, shared handoff/config field, dual NodeId/u16 feature API

**Protocol Dispatch Local Next**:
ICMP type and UDP port registries publish consumer-local `u16` next slots. Control may accept target `NodeId` at registration; the published snapshot and packet path carry only local slots and enqueue through Graph Fanout.
_Avoid_: packet-path NodeId protocol dispatch, ICMP/UDP registry NodeId snapshots on the packet path

**TCP Ingress Local Next**:
TCP input classifies worker-local nexts as current-node-local `u16` slots and enqueues them through Graph Fanout. Cross-worker session ownership leaves the input Frame through Handoff before Fanout; Handoff may retain destination `NodeId` continuation state, and Fanout never enters the cross-worker queue.
_Avoid_: TCP input manual get/push/put, Fanout of handoff-owned indexes, worker-local handoff

**TCP State Fanout**:
TCP established, rcv-process, listen, and syn-sent drain the input Frame, select current-node-local nexts (Output / Drop / Established forward), and flush through Graph Fanout with fixed stack next scratch. Accepted RX payload crosses into Session ownership at the existing app/session seam; generated control buffers are Fanout to tcp-output after TCP/Session state commit. syn-sent retains non-consumed Indexes on the source Frame for RAII free.
_Avoid_: state-node get_next_frame/put_next_frame by target NodeId, one-buffer control put before commit

**Session Queue Fanout**:
Session Queue accumulates generated indexes on the driver Frame with one local next per entry, then performs a single Graph Fanout flush at dispatch end. Existing pending output seeds the shared IO count; normal and custom IO share the remaining allowance up to 128; control processing is not charged; unserved IO remains scheduled. Transport commits before graph visibility.
_Avoid_: per-packet get/push/put from Session Queue, Fanout before transport commit, charging control to the IO budget

**Process Frame Fanout**:
`process_frame!` only runs packet logic, records one current-node-local `NodeNext` decision per Index into fixed stack scratch of production frame capacity (256), and invokes Graph Fanout once. It does not cache target `NodeId`s, scan groups, acquire Next Frames, push Indexes, branch on capacity, put frames, or heap-allocate next scratch.
_Avoid_: NodeId process_frame body, temporary Vec next scratch, macro-owned get/push/put, NodeId: NodeNext

**IP Reassembly Fanout**:
IP reassembly drains the input Frame, retains pending fragments in Fragment Context, and accumulates worker-local Drop/Input outputs on the same Frame with fixed stack next scratch before one Graph Fanout flush (with mid-dispatch flush if output hits frame capacity). Cross-worker fragment ownership remains Handoff.
_Avoid_: reassembly emit_output get/push/put, packet-path NodeId next, Fanout of handoff-owned fragments, direct Lookup bypass of IP Input

**TUN Ingress Fanout**:
TUN input receives into the driver Frame, then enqueues every pending Index through Graph Fanout on the registered local next slot (slot 0). It does not acquire a separate Next Frame or push/put by target `NodeId`.
_Avoid_: TUN input get_next_frame/put_next_frame, NodeId hot-path next field

**Graph Fanout**:
The sole worker-local next-frame enqueue: it groups packet ownership by selected Next Arc and makes the resulting Next Frames visible at the current Graph Node dispatch boundary. Cross-worker ownership transfer remains Handoff, not Graph Fanout.
_Avoid_: cross-thread fanout, handoff enqueue, output router, per-node manual frame get/push/put, recoverable enqueue Result on the packet path

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

**Graph Fanout Layer Contract**:
- `hammer-core` owns Index/Frame/Buffer and graph identity (`NodeId`, `NodeHandle`, `NodeNext` as local `u16` slots). It does not enqueue or resolve next arcs.
- Graph Runtime alone maps local slots to target node identities, owns Graph Fanout (`enqueue_to_next`), appendable Next Frame get/put/rotation, and Handoff queue drain. Direct Frame get/push/put is limited to those internals, the Handoff node drain path, and focused low-level tests.
- Service Graph Nodes run packet logic, choose current-node-local nexts, transfer Session ownership, or invoke Handoff with a local continuation next. They must not resolve target nodes, cache resolved next arrays, or perform worker-local output Frame get/push/put choreography.
- Feature control may retain target `NodeId`s while compiling under a barrier; the packet path carries only local slots and config progress.
- Session Queue accumulates generated indexes with local nexts and flushes once through Graph Fanout.
- Handoff alone owns cross-worker grouping and may retain destination `NodeId` continuation state after Graph Runtime resolves a local next at enqueue.
_Avoid_: `NodeNextStorage`, `runtime_nexts`, production `current_node_next(s)`, protocol target-node routing, compatibility wrappers for removed surfaces

**Barrier Synchronization**:
The control-plane mechanism that pauses data workers at a known point so control changes can observe a stable data-plane state. It is not a lock taken around hot-path packet processing.
_Avoid_: global mutex, graph lock, packet-path synchronization

**Handoff**:
A data-plane transfer of packet ownership from one worker to another through worker-owned slots and a handoff graph node. Handoff moves buffer indexes, not protocol payload copies. Graph Runtime may resolve a current-node-local next into destination `NodeId` continuation state while enqueueing; service nodes pass the local next and must not resolve target node identities themselves.
_Avoid_: crossbeam channel payload, TCP migration copy, app queue transfer, service-side `current_node_next` / resolved-next arrays

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
The worker-local ownership of all buffer references contained in a Pending Frame or Next Frame. Moving indexes between Frames transfers this ownership as a batch, and dropping the owning Frame releases the references that remain in it. Production Frames store indexes in `hammer_infra::vec::Vec` with a fixed logical maximum of 256; Frame-pool size remains the only buffer-frame tuning knob.
_Avoid_: per-index owner, manual buffer free, borrowed input ownership, configurable production frame capacity

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
The VPP-shaped app/session signaling queue: a producer-locked multi-ring message queue (descriptor queue plus IO and CTRL rings) that carries Session Events, not payload bytes. Producers choose the ring via enqueue API (`enqueue_io` / `enqueue_ctrl`); Local and SVM backends share the same logical layout, with backend-specific wake signaling beside the shared header.
_Avoid_: flat SessionEvt ring, payload channel, async stream, per-protocol event bus, matching `evt_type` inside a universal enqueue

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
An app↔session Session Message Queue event aligned with VPP `session_event_t`. IO events (`RxEnq` / `TxDeq`) use the IO ring and carry session index only; control Session Events (`Connect` / `Close`) use the CTRL ring and carry a Session Handle. Consume paths drop events whose session slot is free or unmapped. Slot reuse after free may still target a replacement session; that window matches VPP and is not closed by adding generation to the event. These are not worker-local Session Control Events.
_Avoid_: generation-safe SessionEvt, Index-in-event as ownership proof, one identity field for every event kind, confusing MQ CTRL-ring events with Session Control Events

**Session Control Event**:
A worker-local session lifecycle command, such as disconnect, dispatched by Session Runtime separately from TX/RX session work and separately from Session Message Queue CTRL-ring Session Events. Control events may invoke transport close handling, but they are not readiness facts and do not enter the Session Work Batch.
_Avoid_: close-ready flag, ready-queue close request, synthetic ready boolean, TX event, MQ CTRL-ring Session Event

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

## IP Reassembly

**IP Reassembly**:
The deep module that owns Fragment Context storage, Fragment Owner Bihash interaction, Memory Owner and Sendout decisions, fragment assembly, expiry policy, and reassembly trace emission. The graph node is only the Pending Frame drain and Graph Fanout shell.
_Avoid_: reassembly helper, outcome interpreter node, sticky failed-key table, graph node owning context tables

**Fragment Context**:
The Memory Owner's per-key record of held fragment Indexes, completeness, Sendout Worker, and last-heard time. It lives in the owner Data Worker's Pool and is never mutated by another worker.
_Avoid_: shared HashMap context, first_fragment_worker as a third role, global mutex context table

**Fragment Owner Bihash**:
The shared bihash that maps an IP fragment key to a packed Bihash Value of Fragment Context pool index plus Memory Owner Worker, matching VPP full-reassembly ownership lookup.
_Avoid_: ArcSwap whole-table directory, std HashMap owner map, bihash storing fragment payloads

**Memory Owner Worker**:
The Data Worker that created the Fragment Context for a fragment key and alone may mutate that Pool entry. Non-owner workers Handoff fragments to this worker's reassembly node.
_Avoid_: handoff.worker as a vague owner, directory owner without pool locality

**Sendout Worker**:
The Data Worker that received the fragment with offset zero. When reassembly completes on a different Memory Owner, the completed datagram is Handed off to this worker before re-entering IP Input.
_Avoid_: first_fragment_worker, opaque handoff source as a permanent third role name

**IP Reassembly Expire Walk**:
The per-Data-Worker periodic walk that expires stale Fragment Contexts on the local Pool, frees held fragments, and deletes Fragment Owner Bihash entries, mirroring VPP full-reassembly expire-walk ownership.
_Avoid_: global expire mutex scan, sticky deny after timeout, Fanout of expired fragments to Input

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
