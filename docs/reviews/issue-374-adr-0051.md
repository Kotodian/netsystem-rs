# Issue #374 / ADR-0051 static review

Scope: packet trace ownership, source capture, intermediate-node records,
cross-worker delivery, CLI, and documentation. This review compares the
working tree with vendored VPP and ADR-0051 before a single correction pass.
No build, tests, or CI were run, as requested.

| ID | Finding | Evidence | Correction |
| --- | --- | --- | --- |
| R01 | A traced packet handed to another worker enters a direct Frame without its trace bit. Nodes gated by `NodeRuntime::trace_enabled` then omit their first record on the receiving worker. | VPP `src/vlib/handoff.c:178-180,389-406` sets the received Frame trace bit; Hammer `crates/hammer-runtime/src/data_plane/handoff.rs:12-30` fills a direct Frame but leaves its flags clear. | Mark the direct Frame traced when its transferred Buffer indices contain a traced Buffer, before `put_frame_to_node`. Keep Buffer handles unchanged until `add_trace` creates the receiving worker's handoff record. |
| R02 | TCP RX/TX records retain only the connection index and state. In particular, a successful listen lookup does not leave a useful snapshot of the listener's local/remote endpoint in the historical record. | VPP `src/vnet/tcp/tcp_input.c:1213-1268,2410-2428` and `src/vnet/tcp/tcp_output.c:50-69,2226-2252` copy connection facts when tracing; Hammer `transport/tcp/src/input.rs:18-28`, `output.rs:23-33` omit endpoints. | Extend the existing TCP receive/output record layouts with compact endpoint and owner facts, filled from the looked-up connection directly into the trace record. No clone of the live `TcpConnection`. |
| R03 | Listener identity is now prefilled on successful input classification; it must remain distinct from the established Session route, including the TIME-WAIT path. The earlier proposed "no connection snapshot" fallback is not acceptable for a valid listener. | VPP `src/vnet/tcp/tcp_input.c:2820-2824,2418-2426,2499-2528,2762-2768`; Hammer `transport/tcp/src/input.rs:784-847`, `listen.rs:130-158,265-294`. | Trace and ordinary LISTEN processing use the prefilled listener connection index. A TIME-WAIT packet retains its old Session connection index for trace and resolves the published listener for processing. Missing or removed listeners remain packet errors, not invented listener ids. |
| R04 | ADR-0051 still calls implementation pending and says TCP input has a `next` trace field, despite the new direct TCP receive/output layouts. ADR-0007 and ADR-0009 still describe trace finalization or the removed global control path as if current. | ADR-0051 status and V10, ADR-0007 buffer-free verification rows, ADR-0009 trace ownership rows. | Update ADR-0051 to an implementation record and mark the conflicting historical trace claims in ADR-0007/0009 as superseded by ADR-0051; retain unrelated historical design. |

The remaining source/ownership checks found no additional correction before
this pass: TUN is the capture source; IP, ICMP, TCP and Drop append to existing
handles; Buffer free no longer finalizes trace; CLI synchronously accesses
worker mains under the barrier. R01-R04 were corrected in the same pass. Static
review does not establish compilation or runtime correctness.
