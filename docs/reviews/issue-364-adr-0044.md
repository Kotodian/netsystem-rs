# Issue #364 / ADR-0044 implementation review

- Date: 2026-09-29
- Scope: the uncommitted `feature/364` changes to FIFO, Session queue,
  plugin-session, TCP RX/TX, timers and output. `third_party/iperf/` is
  unrelated untracked user content and is excluded.
- Method: source review against vendored `third_party/vpp/`; no compilation,
  tests, static checks or CI were run, per user instruction.
- Status: second-pass source-review inventory R01-R17 recorded; implementation
  and verification remain incomplete. Findings below are the repair checklist,
  not an assertion that the changed code builds or passes tests.

## Findings and repair order

| ID | Severity | Current implementation and consequence | VPP contract | Repair |
|---|---|---|---|---|
| R01 | Critical | `tcp_session_update_time` records `TcpNodeError` while `session-queue` is current. Its three-entry error table cannot index TCP error codes; the timer path can panic. TCP input/output nodes also record unregistered TCP errors. | `session_node.c:2154-2159`; `tcp_input.c:1407-1413`; `tcp_error.def` | Remove TCP node counters from the Session queue time subscriber; register exact typed error tables at TCP nodes that classify packets. |
| R02 | Critical | `syn_sent.rs` obtains `SessionMain::worker_mut` twice while its first mutable worker borrow is live. This violates the unsafe API's exclusivity contract. | `tcp_input.c:2260-2370`; `session.h:82-170` | Send the control Buffer through the already borrowed Session worker. |
| R03 | High | `TcpWorker::handle_cleanups` stops forever on a due request whose Session still exists, while VPP consumes the request and calls `tcp_connection_cleanup_and_notify`. | `tcp.c:1335-1358`, `session.c` transport delete request | Complete the Session delete-request owner path or retain the exact ADR-0040 dependency; never claim cleanup is complete while a due queue head is stuck. |
| R04 | High | Session originally implemented only TCP peek TX. The ADR also requires reusable stream dequeue, datagram and internal custom TX semantics, including FIFO record offsets, dequeue notifications and event rearming. | `session_node.c:1278-1383,1680-1747` | Source repair now groups equal-length small datagrams and selects the listener transport for Listening dequeue TX. No UDP plugin is required for this issue; this path remains uncompiled and untested. |
| R05 | High | `TcpRecoveryState` already has `high_rxt`, `rescue_rxt`, `next_sack_retransmit`, `rescue_candidate` and `no_sack_first_pending`, but `TcpWorker::retransmit` still uses one lost-sample loop. It never selects the SACK/rescue candidates or gives RFC 6582 no-SACK recovery its own send order. Partial retransmission also waits until the whole sample is sent to advance `high_rxt`; `end.distance_to(sequence)` reverses the remaining-byte calculation. | `tcp_output.c:1672-2065,2123-2186`; `tcp_sack.c:217-287` | Wire the existing recovery state into distinct negotiated SACK/no-SACK sends. Advance high-water only after Buffer publication, retain the one-MSS peer-window reserve for SACK new data, and keep paced/PRR accounting branch-specific. |
| R06 | High | `on_tlp_timer_expiry` always selects a tail sample, and `on_typed_timer_expiry(Tlp)` immediately rearms TLP. The timer subscriber then treats the event as an ordinary retransmit. VPP first tries unsent new data within peer/cwnd limits, otherwise probes the tail, records whether the probe retransmitted, gates a second probe on fresh RTT/ACK state, and always yields to a fresh RTO even when Buffer allocation fails. | `tcp_output.c:1312-1380`; `tcp_tlp.c:15-76,110-156`; `tcp.c:1293-1335` | Keep the Session FIFO probe choice on the owning TCP worker; distinguish new-data and tail-retransmit accounting, clear TLP scheduling state after expiry, and reschedule RTO on every attempted expiry. Do not mark a sample retransmitted before Buffer publication. |
| R07 | High | `TcpTimerKind::TimeWait` handles only TimeWait. Newly parsed CloseWait/FinWait/LastAck/Closing durations have no WAITCLOSE dispatch; `on_session_close` changes state without sending or deferring FIN. The existing `Transport::close(connection_index, worker_index)`/control dispatch has no current runtime and Session worker borrow, so the TCP owner cannot put that FIN into Session pending output there. | `tcp.c:367-469,1199-1275,1713-1722`; `tcp_output.c:842-884` | Use one WAITCLOSE timer with state-specific action/notification, consume configured durations, and pass the existing node-owned runtime/Session borrow through the control dispatch to TCP close so FIN/reset uses the established pending Buffer path. Preserve the FIN-pending and FIN-sent distinction; do not merely rename TimeWait. |
| R08 | High | `SessionWorker::tx_fifo_peek_and_send` fills one segment at a time, not the VPP `n_left >= 4` two-packet pipeline; it also uses `fifo_offset` after send for the event recheck instead of the current transport `tx_offset`. | `session_node.c:1437-1468,1472-1677` | Match VPP burst loop, head/chain accounting, and clear/recheck event order. |
| R09 | Medium | Session queue returns the number of actually flushed pending Buffers, whereas VPP returns the TX budget count (including custom ACK attempts). The node counter uses the latter but runtime scheduling sees the former. | `session_node.c:2033-2159` | Return `n_tx_packets`/the dispatch budget count; pending flush remains a separate side effect. |
| R10 | Medium | The TCP input paths still build immediate control segments, but their RX results lacked VPP's packet-node categories. | `tcp_input.c:977-1210,1361-1412,2260-2370`; `tcp_error.def` | Source repair adds the six FIFO/sequence classifications to established, receive-process and SYN-ACK input; original packet sequence remains available for FIN. The separate immediate-control-output design is not resolved by these counters. |
| R11 | High | FIFO OOO list is updated in place and `Fifo::duplicate` correctly requires `unsafe`, but the public safe `FifoSegment::duplicate_fifo` calls it and leaves both independently mutable handles in the segment pool. Both handles share the header/chunks while their OOO pool and lookup trees diverge. Chunk reclamation and producer/consumer aliasing also need an explicit single-producer/consumer contract. | `svm_fifo.c:171-340,393-407,593-749,833-930`; `svm_fifo.h:70-108` | Make the segment-level duplicate operation unsafe with the same single-owner precondition, or remove it if unused; document the Session's duplicate ownership. Add focused cases after implementation, without running them until testing is permitted. |
| R12 | Medium | ADR-0044 still says Proposed/design-only and its acceptance paragraph says no implementation; that is false once source is changed. | ADR-0044 migration and acceptance sections | Update the ADR only to the honest implemented/partial state, with unresolved ADR-0040 dependency and unrun verification. |
| R13 | Critical | `TcpWorker::transmit_unsent` receives `available_bytes` capped by recovery send space, but its loop is bounded only by peer window and packet count. It can read more retained FIFO bytes than the PRR/CC allowance passed by `retransmit`, then commit and enqueue those extra packets. | `tcp_output.c:1672-1708,1804-2065` | Bound every new-data packet by the remaining recovery byte allowance and debit it after each successful packet; retain the one-MSS lower-bound and peer-window reserve rules from the selected VPP recovery branch. |
| R14 | Medium | The new TCP `send_params` path caches options and updates the receive window, but when flight is empty it omitted VPP's paced bucket reset. The previous `clear_descheduled` edit also reset the bucket without resetting its time base. VPP's `TCP_CC_EVT_START_TX` has a Cubic handler but no BBR handler; Cubic idle-epoch parity is outside this BBR-focused issue. | `tcp_output.c:300-329`; `tcp_cubic.c:297-326`; `transport.c:1053-1058` | Reset both the connection-owned pacer bucket and its worker-clock time base at empty-flight burst start and deschedule clearing; do not invent a BBR start event. |
| R15 | Medium | `SessionTxContext` retains per-event counters and a fixed 32-byte datagram cache, but the TX implementation writes only `tx_buffers` and `transport_pending_buffers`. The cacheline-separated hot context described in ADR-0044 is therefore dead state, and the fixed datagram cache cannot hold VPP's 47-byte record header. | `session.h:22-43`; `session_types.h:495-517`; `session_node.c:1297-1435` | Make the existing context own the values actually used by each TX event, or remove stale fields and correct the ADR; do not leave a false 32-byte datagram layout in service. |
| R16 | Critical | The TCP SACK scoreboard currently constructs holes between surviving outstanding samples and a trailing hole over SACKed bytes. VPP holes are the unsacked outstanding ranges themselves, so loss marking and recovery selection target the wrong sequence intervals. | `tcp_sack.c:217-287,960-1030`; `tcp_output.c:1804-1940` | Rebuild holes from coalesced outstanding sample intervals below `high_sacked`; ACK processing trims those holes but never invents a leading hole where no unsacked sample exists. Preserve `high_rxt` separately. |
| R17 | High | Hammer's initial RACK path used ACK arrival time rather than the delivered transmission timestamp to schedule loss, then sent one timer Buffer while marking an entire sample retransmitted. Its one-record-per-range sample pool also lacks VPP's separate retransmission copies, DSACK reordering-window adjustment and unified RTO/REO/PTO timer choice. | `tcp.c:789-799,1703-1707`; `tcp_rack.c:174-485,506-805`; `tcp_bt.c:1710-1742`; `tcp_output.c:2125-2148` | Hammer's requested default is RACK-on for SACK-negotiated connections, an explicit difference from this VPP checkout's off default. Use send-time order and RACK-only lost ranges; timer expiry must schedule Session custom TX. Complete transmission-copy/DSACK/timer semantics before claiming full parity. |

## Cross-layer decisions

Service owns Session FIFO, event ordering, TX packetization, pending Buffer/next
columns, and dequeue notification. Plugin-session owns IP-family Session type and
datagram metadata. TCP owns sequence, ACK/recovery/WAITCLOSE and headers. Runtime
provides the generic batch node-counter operation; it must not know TCP errors.
The current `Transport` trait remains the only concrete transport contract;
no VFT, `dyn`, second TX mode, or payload staging `Vec` is authorized.

R01/R02/R13 are correctness blockers. R03-R10/R14 are required to claim the
issue's full migration. R11 is an ownership blocker plus focused verification
work. R12 is the final documentation step. Record every repair in this file
before moving to the next item; do not silently remove a finding.

## Review scope and sequence

The initial R01-R14 inventory was written before the next implementation edit.
It covered every changed tracked file in the current diff, the ADR-0044
migration table, and the corresponding vendored VPP Session TX, TCP
output/input/timer, and SVM FIFO paths. An R04-focused second pass found R15
after some repairs had started; the oversight and its repair are recorded
explicitly rather than retroactively claiming the first pass was exhaustive.
The current repair checklist is R01-R17.
The review is source-only: the requested ban on compilation, tests, static
checks and CI means type checking and executable behavior remain unverified.
`third_party/iperf/` is not part of this review or the repair scope.

## Repair log

- R01: source repair applied. The Session time subscriber no longer writes TCP
  node counters into the Session queue's error table. TCP input/listen nodes
  register `TcpNodeError` descriptors, and TCP output nodes register their
  `TcpError` descriptors. Not compiled or tested by request.
- R02: source repair applied. SYN-SENT control output appends through the
  already borrowed owner `SessionWorker`; it no longer creates a second
  mutable borrow of that worker. Not compiled or tested by request.
- R03: not repaired in ADR-0044. VPP's `session_transport_delete_request`
  requires Session-owned app cleanup acknowledgement and a deferred transport
  cleanup callback. That lifecycle API is explicitly assigned to ADR-0040;
  the current TCP queue must remain reported as incomplete until that owner
  path exists. No callback or transport deletion was invented in this change.
- R08: source repair applied to the shared TX fill path. The VPP
  `n_left >= 4` loop now prefetches the next two allocated Buffer headers for
  store before filling the current pair; the residual loop prefetches its
  next header. Chain tail accounting and the current transport offset are
  retained for event recheck. Not compiled or tested.
- R09: source repair applied. Session queue returns its VPP TX budget count,
  while pending Buffer flush remains a separate action. Not compiled or tested.
- R04: source repair applied. The existing Session TX dispatcher has stream
  dequeue and internal custom-TX entries. Generic datagram dequeue now scans
  equal-length small records within VPP's 32 KiB bound, packetizes from FIFO
  directly into final Buffers, updates a partial record's offset, and drops
  completed records. The service selects a Listener transport target only for
  Listening dequeue TX; concrete listener access stays in the owning plugin.
  An empty datagram FIFO no longer retains a runnable TX event. The removed UDP
  plugin is not an acceptance prerequisite. Not compiled or tested.
- R13: source repair applied. Recovery's remaining byte allowance now bounds
  the new-data packet count before `transmit_unsent`, while that method still
  receives the actual FIFO-available byte count. A short final packet remains
  possible when the allowance covers one MSS. Broader branch-specific PRR and
  peer-window reserve behavior remains R05 work. Not compiled or tested.
- R11: the public segment duplicate operation now requires `unsafe` and states
  the producer/consumer ownership transfer precondition, matching the lower
  FIFO duplicate operation. Focused tests now cover duplicate/overlap merging,
  cross-chunk promotion and capacity rejection without tail publication. The
  private lookup reclamation and concurrent producer/consumer publication
  still require executable verification; tests remain unrun by request.
- R12: the ADR now says implementation is in progress and points to this
  review inventory. Its acceptance paragraph no longer claims that only an
  ADR was written. No verification was run.
- R14: source repair applied. Empty-flight burst setup and deschedule clearing
  now reset both pacer bucket and worker-clock time base. VPP's Cubic-specific
  idle epoch hook was deliberately not added to the BBR path. Not compiled or
  tested.
- R15: source repair applied. The existing cacheline-separated TX context now
  owns the selected Session, current send parameters, FIFO/buffer budget and
  reusable Buffer columns; its fill helper consumes the per-Buffer capacities
  and decrements remaining bytes. The unused 32-byte datagram cache was
  removed. Not compiled or tested.
- R10: source repair applied to packet classifications and counter timing.
  TCP input/output now share the registered `TcpError` table for their packet
  errors; TCP state nodes register the additional enqueued, OOO, FIFO-full,
  partial, old and zero-window categories. As in `tcp_input.c:1348-1410`, established
  input accumulates one selected category per packet in fixed frame-local
  counts and a nonzero mask, then commits only set counters after the packet
  loop. SYN-SENT and receive
  processing select the category in each branch and record it only at their
  per-packet exit, matching `tcp_input.c:1700-1921,2009-2365`. TCP output
  uses the same frame-local counts/mask pattern, following
  `tcp_output.c:2301-2393`. The TCP dispatch input also assigns the buffer's
  error index without incrementing its counter, then commits its frame-local
  counts at loop end (`tcp_input.c:2786-2890`). This does not change the separate immediate-control
  output path. Not compiled or tested.
- R05: partial source repair applied. ACK and SACK scoreboard updates now
  preserve the published `high_rxt` during recovery instead of resetting it
  to the cumulative ACK; completing a retransmitted sample also no longer
  moves the high-water mark backward. The worker now selects SACK ranges via
  RACK lost-sample ranges for SACK-negotiated connections. The no-SACK path retransmits sequentially up to the existing
  recovery end, then considers new FIFO bytes; its pacer debit follows VPP's
  segment-count rule. The reversed `distance_to` operands were fixed.
  `high_rxt` now advances after each successful SACK Buffer publication. Remaining questions concern range selection
  across partially retransmitted samples and lifecycle cleanup, not the
  high-water publication point. Not compiled or tested.
- R17: source repair in progress. SACK negotiation selects RACK's lost-sample
  range instead of the scoreboard NextSeg/rescue backend; without negotiated
  SACK the no-SACK path remains. RACK now compares outstanding send timestamps
  with the newest delivered original transmission, arms deadlines from send
  time plus observed RTT/reordering window, and handles already expired loss
  on the ACK. A REO timer schedules Session custom TX instead of committing
  one possibly partial sample. Aggregate loss and active transmission loss
  are distinct sample bits, so a retransmitted range can become eligible again.
  Invalid ordinary SACK blocks do not advance `high_sacked`, and the previous
  valid `high_sacked` survives an ACK without a new SACK block. Hammer enables
  this path at SACK negotiation, as requested,
  whereas vendored VPP requires explicit enable. Separate transmission copies,
  DSACK-based reordering adaptation, RACK's own windowed minimum RTT and VPP's
  one-timer RTO/REO/PTO choice
  remain unimplemented; full RACK parity is not claimed. No validation run.
- Congestion owner: TCP's private algorithm function table and 256-byte erased
  storage were removed. Each connection now holds service's concrete
  `BbrController` and calls the `CongestionController` trait directly; config
  continues to reject non-BBR selection. Recovery stays generic over that
  trait without putting congestion state in service Session scheduling.
- TCP IP metadata: the duplicate `TcpIpVersion` and `TcpIpProtocol` enums were
  removed. TCP input traces and protocol checks now use the already-owned
  `hammer-plugin-ip` `IpVersion` and `IpProtocol` types. This leaves the
  session-table selector `IpSessionFamily` unchanged; it is not part of the
  duplicate TCP metadata path. No compilation or tests were run.
- TCP packet-node time: `tcp4-input`/`tcp6-input` and
  `tcp4-output`/`tcp6-output` now refresh their owner worker's `time_us` and
  `time_tstamp` at their shared node entry points, following
  `tcp_input.c:2789` and `tcp_output.c:2312`. They use the same service clock
  as the Session queue but do not expire the wheel there; the queue's existing
  `Transport::update_time` subscription remains the timer owner.
  VPP also refreshes the clock in `tcp_init_snd_vars` (`tcp.c:734`); Hammer's
  current connection initialization does not derive ISS from this cached
  worker time, so that call site remains a separate source-review difference.
- R06: source repair applied to the existing TLP timer and Session FIFO path.
  PTO no longer selects a tail sample in the timer callback or rearms itself.
  The owning TCP worker first tries unsent FIFO data within peer/cwnd limits,
  falls back to the retained tail, publishes probe state only after pending
  Buffer insertion, and the time subscriber always starts a fresh RTO after
  an attempted probe. Recovery gates another PTO on a fresh RTT and probe
  acknowledgment. This is source review only; not compiled or tested.
- R07: control dispatch now passes the current DataPlaneMain and SessionWorker
  to the approved `Transport::half_close/close/reset` signatures. Session FIFO
  peek TX now follows VPP `session_tx_not_ready` and permits queued data while
  the Session is APP_CLOSED. TCP still needs the FIN-pending/FIN-sent state,
  WAITCLOSE timer branches, and actual FIN/RST pending-Buffer publication;
  the old close/reset behavior is not complete. Not compiled or tested.
- R16: source repair applied. SACK scoreboard holes now coalesce outstanding
  unsacked sent-sample intervals below `high_sacked`; ACK trims them without
  inventing holes over already SACKed bytes. Loss classification starts with
  the trailing SACKed run above the highest hole, matching VPP's initial
  `sacked`/`blks` calculation. This remains uncompiled and untested.
- TCP frame prefetch: `tcp_input.c:2797-2883` and `tcp_output.c:2314-2393`
  use a two-packet body when at least four packets remain, followed by a
  single-packet tail. The Hammer input/output loops now follow those widths
  with explicit packet 0/1 work. Established input retains VPP's single-packet
  next-buffer prefetch (`tcp_input.c:1359-1369`); SYN-SENT, receive-process,
  listen and reset were not converted to two-packet loops. TCP and Session
  Session TX calls runtime for the Buffer header's STORE hint only. TCP input
  calls runtime for header STORE and infra for two fixed `b->data` cache lines
  with LOAD; established calls runtime/infra with LOAD/LOAD, and TCP output
  uses STORE/STORE. This follows the separate `vlib_prefetch_buffer_header`
  and `CLIB_PREFETCH` operations in VPP. Infra expands a byte range into at
  most four cache-line hints and does not derive a Buffer's data address.
- TCP input dispatch: the current connection state and filtered TCP flags now
  select a typed next/error table matching `tcp_input.c:2754-2772,3006-3235`.
  The table entry implements `NodeNext`; the tuple index no longer caches a
  next node. Cross-worker packets re-enter TCP input on the owner before the
  table lookup, and listener records use their published TCP state. The
  existing stateless pending-ACK handshake still enters listen directly;
  unlike VPP's connection-backed SYN-RCVD path, it has no connection state to
  index. This is a documented design difference, not verified parity. No
  compilation, tests, static checks or CI were run.
- TCP nolookup input: `tcp4-input-nolookup` and `tcp6-input-nolookup` are
  registered with the same next arcs and error table as ordinary input, and
  use the same frame loop and dispatch table. They read a preselected TCP
  connection index from TCP secondary opaque, validate its Session/worker
  ownership, and skip the tuple and IP Session lookup, following
  `tcp_inlines.h:282-377` and `tcp_input.c:2896-2960`. No existing production
  graph edge targets these nodes yet; registration alone does not exercise
  them. Source reviewed only, with verification still prohibited.
- TCP TIME_WAIT/CLOSED SYN re-entry remains incomplete: the dispatch table
  routes those packets to listen as in `tcp_input.c:3218-3233`, but Hammer's
  listen node does not yet perform VPP's prior-connection validation and
  cleanup from `tcp_input.c:2511-2561`. It must not be claimed as a completed
  reopen path.
