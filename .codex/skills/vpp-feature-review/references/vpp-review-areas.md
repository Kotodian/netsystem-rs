# VPP Review Reference Map

## When to read

Read this file when reviewing a completed Hammer feature against vendored VPP. Use the paths as starting points, not as exhaustive search boundaries.

## Contents

- [Graph, node, frame](#graph-node-frame)
- [Buffers and memory](#buffers-and-memory)
- [Session and app boundary](#session-and-app-boundary)
- [SVM FIFO and message queue](#svm-fifo-and-message-queue)
- [TCP](#tcp)
- [TLS](#tls)
- [Interface and device](#interface-and-device)
- [Runtime, barrier, and worker](#runtime-barrier-and-worker)
- [Timers and infra primitives](#timers-and-infra-primitives)

## Graph, node, frame

- VPP: `third_party/vpp/src/vlib/node.h`, `node.c`, `main.h`, `buffer.h`
- Hammer: `crates/hammer-core`, `crates/hammer-runtime`
- Review: worker-owned frame state, fixed-schedule step order, node next arcs, barrier publication, graph refork semantics.

## Buffers and memory

- VPP: `third_party/vpp/src/vlib/buffer.h`, `buffer_funcs.h`, `third_party/vpp/src/vnet/buffer.h`, `vnet/buffer.c`
- Hammer: `crates/hammer-infra` Pool, Segment, Main Heap, Buffer Arena; `crates/hammer-core` BufferIndex and chain state
- Review: `attach_clone`/refcount behavior, `current_data`, `current_length`, `NEXT_PRESENT`, `next_buffer`, chain totals; no feature-specific buffer sharing APIs.

## Session and app boundary

- VPP: `third_party/vpp/src/vnet/session/session.c`, `session_node.c`, `session_input.c`, `transport.c`, `application.c`, `application_local.c`, `application_interface.c`, `application_worker.c`, `segment_manager.c`
- Hammer: `crates/hammer-service/src/session`, `crates/hammer-app`, `crates/hammer-runtime/src/app`
- Review: exact session identity in events, Session ownership of policy/ordering/publication, transport opaque listener/session identity, app notification boundary, FIFO plus message-queue semantics.

## SVM FIFO and message queue

- VPP: `third_party/vpp/src/svm/svm_fifo.h`, `svm_fifo.c`, `message_queue.h`, `message_queue.c`, `fifo_segment.h`, `fifo_segment.c`, `queue.h`, `queue.c`
- Hammer: `crates/hammer-infra` FIFO/OOO delivery, segment/shared-memory primitives, `crates/hammer-app` AppSession
- Review: OOO semantics, head/tail and generation state, message ordering, per-app message queue identity, no `AppRing`/SQE/CQE surface.

## TCP

- VPP: `third_party/vpp/src/vnet/tcp/tcp.h`, `tcp_input.c`, `tcp_output.c`, `tcp.c`, `tcp_timer.c`, `tcp_cubic.c`, `tcp_newreno.c`, `tcp_sack.c`
- Hammer: TCP transport/output/recovery in the relevant crates, session-owned TX FIFO
- Review: TCP owns sequence/ACK/loss/timer decisions, Session owns TX byte retention, recovery packetizes from Session FIFO without private payload copies, timer dispatch uses the exact token/kind.

## TLS

- VPP: `third_party/vpp/src/vnet/tls/tls.c`, `tls.h`, `tls_inlines.h`, `tls_record.c`, `tls_record.h`
- Hammer: TLS protocol plugin and FIFO layer tests
- Review: protocol operates on adjacent FIFOs only, transforms source into a destination write reservation, commits destination before consuming source, and leaves both FIFO positions unchanged on error.

## Interface and device

- VPP: `third_party/vpp/src/vnet/interface.c`, `interface.h`, `interface_output.c`, `interface_stats.c`, `third_party/vpp/src/vnet/devices/devices.h`, `devices.c`
- Hammer: `crates/hammer-service` interface/device contracts
- Review: feature-arc contracts, interface/session ownership, stats and lifecycle.

## Runtime, barrier, and worker

- VPP: `third_party/vpp/src/vlib/main.c`, `main.h`, `threads.c`, `threads.h`, `handoff.c`, `handoff.h`, `error.h`
- Hammer: `crates/hammer-runtime` workers, WorkerBarrier, sync primitives
- Review: barrier acknowledges worker stop before publishing worker-visible state; no second lock/atomic pointer publication protocol; no `thread_local!`.

## Timers and infra primitives

- VPP: `third_party/vpp/src/vppinfra/tw_timer_template.h`, `tw_timer_template.c`, `test_tw_timer.c`, `fifo.h`, `fifo.c`, `bihash_template.h`, `bihash_template.c`
- Hammer: `crates/hammer-infra` TimerWheel1t2w2048sl, Pool, Bihash, FIFO, RbTree
- Review: timer expiry dispatches the exact token/kind; infra primitives preserve fixed capacity and worker ownership.
