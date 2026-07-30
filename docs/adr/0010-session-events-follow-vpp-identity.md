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
4. TCP always refreshes its cached receive window from the delivered FIFO
   capacity. It emits a window-update ACK only after an ACK has advertised a
   zero receive window. Below the VPP free-space threshold it asks Session
   Runtime to re-arm the FIFO; at or above the threshold it emits a pure
   window-update ACK through the existing Session Queue Graph Fanout.

`TxEnq` is the opposite-direction event: bytes were enqueued into a TX FIFO and
the exact consumer Session should run. That Session dispatches its selected
Transport or App Session protocol behavior. `TxDeq` reports that TX FIFO
capacity was released and dispatches the Application behavior selected by that
same Session. Neither TX event is reused for RX-space recovery, and Session
Worker does not walk or scan a protocol composition.

`ProtocolOutput` is a Session-internal IO event for protocol state that may
have produced reverse-direction output without an application TX FIFO enqueue, such
as TLS handshake records after record ingress. It targets one exact Session
and invokes the protocol connection selected by that Session.
It is not accepted from an external App and is not a substitute for a real
`TxEnq` after bytes become visible in a TX FIFO.

## Layer contract

- `hammer-runtime::app` owns shared FIFO enqueue/dequeue notification and
  IO-event publication. It may report `RxDeq`, `TxEnq`, and `TxDeq`; it must
  not inspect transport state or protocol ordering.
- `hammer-service::session` owns RX FIFO capacity, notification arming, worker
  event delivery, `ProtocolOutput`, and Session Queue scheduling. It may pass
  capacity facts to a transport; it must not inspect TCP windows or construct
  TCP segments.
- A `SessionTransport` owns protocol decisions following `app_rx_evt`. It may
  request another notification or emit protocol output through the supplied
  Session Queue output capability; it must not retain app/FIFO pointers or
  schedule Graph Nodes directly.
- TCP owns whether a zero receive window was actually advertised, the VPP
  `clamp(fifo_size / 8, 4 KiB, 128 KiB)` reopening threshold, receive-window
  state, and the window-update ACK.

The production interface additions for this dispatch are limited to
`SessionEvtType::RxDeq` and `SessionTransport::app_rx_evt`.

## Verification

Focused Rust behavior is verified by the remote GitHub Actions jobs. End-to-end
window recovery is verified by the serial BBR and Cubic `tun_tcp_echo` labs,
which must complete an exact 1,000,000-byte echo after the receive FIFO has
advertised zero window. Agents run these jobs through remote `act`; they do not
create a TUN interface or run the lab locally.
