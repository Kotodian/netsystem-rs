# Session events follow VPP identity rules

Hammer app↔session message-queue events follow VPP `session_event_t` identity rules rather than inventing generation-safe event tokens.

IO events carry session index only. Control close/connect events carry a VPP-shaped Session Handle (`session_index` packed with worker/thread index). Consume paths drop free or unmapped session slots and drop Close events whose worker index does not match the draining worker.

Pool generation is intentionally omitted from the event ABI. After a free slot is reused, a stale index-only IO event may still target the replacement session; that window matches VPP and is accepted here.

## RX dequeue notification

`RxDeq` is the app-to-session-worker fact that the application consumed bytes
from the session-owned RX FIFO. It follows VPP's `SESSION_IO_EVT_RX` path:

1. Session Runtime arms the RX FIFO dequeue notification when RX capacity is
   exhausted.
2. The application dequeues bytes and posts one edge-triggered `RxDeq` IO event
   to the owning worker's Session Message Queue.
3. Session Queue dispatch calls `SessionTransport::app_rx_evt` with the current
   free capacity and configured FIFO capacity.
4. TCP acts only after it has emitted an ACK advertising a zero receive window.
   Below the VPP free-space threshold it asks Session Runtime to re-arm the FIFO;
   at or above the threshold it emits a pure window-update ACK through the
   existing Session Queue Graph Fanout.

`TxDeq` remains the opposite-direction event: app TX data is ready for Session
Runtime to packetize. It must not be reused for RX-space recovery.

## Layer contract

- `hammer-runtime::app` owns shared FIFO dequeue notification and IO-event
  publication. It may report `RxDeq`; it must not inspect transport state.
- `hammer-service::session` owns RX FIFO capacity, notification arming, worker
  event delivery, and Session Queue scheduling. It may pass capacity facts to a
  transport; it must not inspect TCP windows or construct TCP segments.
- A `SessionTransport` owns protocol decisions following `app_rx_evt`. It may
  request another notification or emit protocol output through the supplied
  Session Queue output capability; it must not retain app/FIFO pointers or
  schedule Graph Nodes directly.
- TCP owns whether a zero receive window was actually advertised, the VPP
  `clamp(fifo_size / 8, 4 KiB, 128 KiB)` reopening threshold, receive-window
  state, and the window-update ACK.

The production API additions for this chain are limited to
`SessionEvtType::RxDeq` and `SessionTransport::app_rx_evt`.

## Verification

Focused Rust behavior is verified by the remote GitHub Actions jobs. End-to-end
window recovery is verified by the serial BBR and Cubic `tun_tcp_echo` labs,
which must complete an exact 1,000,000-byte echo after the receive FIFO has
advertised zero window. Agents run these jobs through remote `act`; they do not
create a TUN interface or run the lab locally.
