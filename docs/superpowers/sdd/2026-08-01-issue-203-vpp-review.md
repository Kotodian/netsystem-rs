# Issue #203 VPP alignment review

## Scope

This review covers the external plaintext/TLS echo fixes and the remote RACK/TLP
classification assertions on `bug/201`. The final two commits modify only the
TCP integration workflow; they do not change TCP recovery implementation.

## Vendored VPP references

- `third_party/vpp/src/vnet/tcp/tcp_timer.c`
- `third_party/vpp/src/vnet/tcp/tcp_input.c`
- `third_party/vpp/src/vnet/tcp/tcp_output.c`
- `third_party/vpp/src/vnet/session/application_interface.h`
- `third_party/vpp/src/vnet/session/session_node.c`
- `third_party/vpp/src/svm/message_queue.c`

## Ownership and event alignment

- Session runtime still owns Session event delivery and node scheduling.
- The TCP plugin still owns connection recovery state, timer actions, ACK/loss
  decisions, and retransmission classification.
- Timer expiry still dispatches the exact typed timer token supplied by the
  runtime. No timer-kind scan, congestion-control scheduler, or sibling
  recovery node was introduced.
- Effective RACK, TLP, and RTO recovery actions remain observable through the
  existing `TcpNodeError` counters. The workflow now uses their actual stable
  codes: `15` for RACK retransmit, `16` for TLP probe, and `17` for RTO
  retransmit.
- Echo backpressure remains event-driven. Partial TX acceptance retains the
  unaccepted bytes and resumes from the exact Session `TxDeq` event, preserving
  VPP message-queue empty-to-non-empty signalling rather than adding polling or
  a second scheduler.

Vendored VPP keeps TCP timers and retransmit handling in worker-owned TCP state.
It does not expose Hammer's separate RACK and TLP timer names. Hammer's typed
RACK/TLP distinction is therefore an intentional extension, while preserving
VPP's transport ownership boundary and timer-driven recovery semantics.

## Recovery assertion review

The Linux loss injection uses an nftables input hook. A packet dropped by that
hook is not guaranteed to be visible to the later libpcap capture point, so
requiring two pcap observations of the target sequence incorrectly rejected a
successful recovery. The corrected test keeps the authoritative facts:

- exact echoed payload;
- the nftables fault rule was hit;
- a valid TCP handshake was captured;
- the owner-local recovery node counter increased for the expected action;
- sibling RACK/TLP/RTO counters did not increase.

This does not weaken recovery classification. It removes an invalid capture
topology assumption while retaining end-to-end byte correctness, proven fault
injection, and exclusive typed recovery accounting.

## API and boundary audit

- No constructor was added; the existing single App Session construction
  surface remains.
- No public type, public API, lock, atomic publication path, trait object,
  intermediate payload allocation, or transport-specific runtime/buffer API
  was added.
- The app/session boundary remains the only payload-copy boundary.
- Existing node-error ownership and classification remain in the TCP plugin.

## Verification evidence

- `cargo test -p hammer-plugin-tcp rack -- --nocapture`: 12 focused tests
  passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- Remote Linux `sender-rack`: exact 10000-byte echo, nft fault counter 1,
  handshake present, `TcpNodeError[15]` increased, and codes 16/17 remained
  zero.
- Remote Linux `sender-tlp`: fault-injected exact 6000-byte echo, packet
  behavior assertion passed, and recovery counter assertion passed. Diagnostic
  artifact digest:
  `d6a39f47c05f9968c6bcdb89cf844e9ec8b151f53ef15ccf0e9c1c27193b3025`.

## Result

The final behavior and tests preserve the VPP ownership model. No blocking
alignment issue remains for #201, #202, or #203.
