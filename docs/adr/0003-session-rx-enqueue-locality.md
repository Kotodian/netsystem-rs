# Session RX enqueue locality

Hammer Session Runtime owns receive-side FIFO enqueue locality. TCP transport supplies accepted payload buffer identity, relative offset, and in-order/OOO facts, then receives `CoreResult<RxDelivery>` for sequence, ACK, SACK, and advertised-window decisions. App notification and RX FIFO capacity lookup stay inside Session Runtime instead of being assembled at TCP call sites.

This follows VPP's receive path: TCP calls `session_enqueue_stream_connection` with a transport connection, buffer, relative offset, `queue_event`, and in-order flag; session code finds the worker-local `session_t`, writes `s->rx_fifo`, stages `SESSION_F_RX_EVT` in the session worker, and later flushes enqueue events to app workers. TCP consumes the written/OOO result to advance receive state, but it does not own app notification or ready-queue mechanics. VPP's OOO path passes `queue_event = 0`, so OOO payload may update FIFO OOO state and SACK facts without making app-readable RX work visible until in-order bytes are delivered.

Hammer should mirror that ownership shape instead of inventing a generic deduplicating queue for RX events. RX enqueue event coalescing is a session/app-session pending bit plus a worker-local vector of session ids to flush, analogous to VPP's `SESSION_F_RX_EVT` plus `session_to_enqueue[proto]`. Do not introduce `DedupFifo`, `ReadySession` tokens, `Session<Ready>`, or a second ready queue to model RX app notifications. Those are scheduler abstractions, while VPP's RX enqueue locality is a session flag and a batched handle vector.

Session scheduling coalescing belongs to the session layer, fully isolated from transport connection state. Hammer should model the VPP-style pending fact beside the session runtime entry, not inside TCP or any other transport `St`. Transport may request that its owning session be scheduled through the session context, but it must not own, inspect, or clear the pending bit.

The worker-local batch should align with VPP's vector shape, not preserve the old FIFO/hash-set abstraction. Hammer should append scheduled `SessionId`s to a worker-local `hammer_infra::vec::Vec<SessionId>`, flush the current vector in order, then clear/reset it. Duplicate suppression is only the session-layer pending bit; the batch itself must not own a `FlatHashTable`, `DedupFifo`, or queue-level membership state.

Rust should encode this ownership boundary with a private session-layer entry that wraps transport state, for example `SessionEntry<St> { state: St, schedule_pending: bool }`. The session pool owns `SessionEntry<St>`, while existing `session()` / `session_mut()` style accessors return only `&St` / `&mut St` to transport-facing callers. TCP and other transports therefore cannot represent or mutate session scheduling state by type.

Disconnect/close must also align with VPP in this refactor. VPP handles disconnect, shutdown, reset, and related messages as session control events (`SESSION_CTRL_EVT_*`) dispatched by the session queue node; they are not entries in the TX/RX ready batch. Hammer must therefore remove the old `pending_closes: SessionReadyQueue` coupling and represent app/requested close as a session control event path in the current implementation. `handle_ready_session` must not receive a synthetic `close_requested` boolean from a ready queue drain; close handling is invoked from the control-event dispatch path.

The current Rust control-event shape is intentionally narrow: `SessionControlEvent::Disconnect(SessionId)`. Do not add placeholder `Shutdown` or `Reset` variants until the implementation has those concrete paths. During app/runtime event drain, `SessionEvtType::TxDeq` schedules session work, while `SessionEvtType::Close` becomes a `Disconnect` control event. Control events are drained as control work and never enter the Session Work Batch.

Session queue dispatch order is fixed: drain app/runtime events into their lanes, expire timers, dispatch session control events, dispatch the Session Work Batch, then flush output frames. If a session has both a disconnect control event and scheduled TX/RX work in the same turn, the disconnect is handled first; close/removal clears the session pending bit, and any later batch entry for a removed or closed session is skipped.

The old Hammer `SessionRxEnqueue` shape must not be preserved. `RxDelivery` is a successful transport-neutral result value, not a new error model; errors continue through the existing `CoreResult` boundary. Its Rust shape should model the legal receive outcomes directly:

```rust
pub(crate) enum RxDelivery {
    NotAccepted { rx_available: u32 },
    InOrder {
        accepted: NonZeroU32,
        promoted: u32,
        rx_available: u32,
    },
    OutOfOrder {
        accepted: NonZeroU32,
        newest: OooSpan,
        rx_available: u32,
    },
}
```

`OooSpan` carries `start: u32` and `len: NonZeroU32`, so zero-length OOO facts are unrepresentable. `accepted == 0` is represented by `NotAccepted`, not by a zero field inside accepted variants. In-order delivery cannot carry OOO facts, and OOO delivery cannot pretend to be app-readable in-order delivery. The type must not carry `bool`, `usize`, builder state, compatibility fields, or padding-only wrappers; add a size guard so type-driven representation does not silently grow the hot-path result.
