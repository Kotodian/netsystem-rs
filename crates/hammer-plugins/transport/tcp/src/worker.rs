use std::time::Instant;

use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::Pool;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_plugin_session::IpSessionMain;
use hammer_runtime::{DataPlaneMain, DataWorkerId, RuntimeResult};
use hammer_service::session::{SessionError, SessionHandle, SessionQueueNext, SessionWorker};

use super::lookup::TcpLookupState;
use super::timers::{
    self, TCP_TIMER_EXPIRY_BUDGET, TCP_TIMER_KIND_COUNT, TcpTimerKind, TcpTimerToken,
};
use super::{TcpCapabilities, TcpConnection, TcpSegment, TcpSegmentFlags, TcpSeq, TcpState};

const DEFAULT_TCP_CONNECTION_CAPACITY: usize = 1024;

#[repr(C)]
pub struct TcpWorker {
    cacheline0: CacheLineAlignMark,
    pub(crate) connections: Pool<TcpConnection>,
    pending_deq_acked: Vec<u32>,
    pending_disconnects: Vec<u32>,
    pending_resets: Vec<u32>,
    pub(crate) time_us: f64,
    pub(crate) time_tstamp: u32,
    pub(crate) max_timers_per_loop: u32,
    pub(super) pending_timers: FifoQueue<TcpTimerToken>,
    cacheline1: CacheLineAlignMark,
    pub(super) cached_opts: [u8; 40],
    pub(super) cached_segment: Option<TcpSegment>,
    tx_buffers: Vec<u32>,
    pending_cleanups: FifoQueue<TcpCleanupRequest>,
    pub(crate) tco_next_node: [SessionQueueNext; 2],
    pub(super) timer_wheel: TimerWheel1t2w2048sl<u32>,
    pub(super) expired_timers: Vec<u32>,
    pub(super) last_timer_update: Instant,
    pub(crate) time_origin: Instant,
    pub(crate) time_origin_seconds: Option<f64>,
    cacheline2: CacheLineAlignMark,
    pub(crate) lookup: TcpLookupState,
    pub(crate) protocol: u8,
}

struct TcpCleanupRequest {
    free_time: Instant,
    connection_index: u32,
    session: SessionHandle,
}

// VPP: tcp.c:1722, tcp_cfg.cleanup_time defaults to 0.1 seconds.
const TCP_CLEANUP_TIME: std::time::Duration = std::time::Duration::from_millis(100);

const _: () = {
    assert!(core::mem::align_of::<TcpWorker>() == CACHE_LINE);
    assert!(core::mem::offset_of!(TcpWorker, cacheline0) == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline1) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline2) % CACHE_LINE == 0);
};

impl TcpWorker {
    /// VPP: tcp.c:345-355, tcp_program_cleanup.
    pub(super) fn program_cleanup(&mut self, connection_index: u32) {
        let session = self
            .connections
            .get(connection_index)
            .expect("scheduled TCP cleanup retains its connection")
            .base
            .session;
        self.pending_cleanups.push_back(TcpCleanupRequest {
            free_time: self.last_timer_update + TCP_CLEANUP_TIME,
            connection_index,
            session,
        });
    }

    /// VPP: tcp.c:1335-1358, tcp_handle_cleanups. A cancelled timer token
    /// remains harmless in the pending timer FIFO after this pool removal.
    pub(super) fn handle_cleanups(
        &mut self,
        runtime: &DataPlaneMain,
        now: Instant,
        sessions: &mut SessionWorker,
    ) -> Result<(), SessionError> {
        let ip_session = IpSessionMain::global()?;
        while !self.pending_cleanups.is_empty() {
            if !self
                .pending_cleanups
                .front()
                .is_some_and(|request| request.free_time <= now)
            {
                break;
            }
            let request = self
                .pending_cleanups
                .front()
                .expect("due TCP cleanup remains queued");
            let connection_index = request.connection_index;
            let Some(connection) = self.connections.get(connection_index) else {
                self.pending_cleanups.pop_front();
                continue;
            };
            if connection.base.session != request.session {
                self.pending_cleanups.pop_front();
                continue;
            }
            let endpoint = connection.base.endpoint;
            let attached = if sessions.session_from_handle(request.session).is_some() {
                ip_session.notify_deleted(
                    runtime,
                    sessions,
                    request.session,
                    connection_index,
                    &endpoint,
                )?
            } else {
                ip_session
                    .lookup_main()
                    .remove_connection_if_current(&endpoint, request.session.into());
                false
            };
            self.pending_cleanups.pop_front();
            if !attached {
                self.release_connection(connection_index);
            }
        }
        Ok(())
    }

    /// VPP: `tcp_connection_cleanup`, tcp.c:250-288. The Session lookup was
    /// removed before this transport-owned connection and its timers.
    pub(crate) fn release_connection(&mut self, connection_index: u32) {
        let Some(connection) = self.connections.get_mut(connection_index) else {
            return;
        };
        for id in 0..TCP_TIMER_KIND_COUNT as u32 {
            let kind =
                TcpTimerKind::from_id(id).expect("TCP timer count covers every registered kind");
            timers::reset(
                &mut self.timer_wheel,
                connection_index,
                connection.timer_state_mut(),
                kind,
            );
        }
        let session_index = connection.base.session.session_index;
        self.lookup.forget_session(session_index);
        self.lookup.forget_pending_open(session_index);
        self.remove_connection(connection_index);
    }

    /// VPP: tcp_output.c:1058-1089. ACKs enter Session custom TX once;
    /// duplicate ACK count remains on the connection until that event runs.
    pub(crate) fn program_ack(
        &mut self,
        runtime: &DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
        duplicate: bool,
    ) {
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("TCP input retains the connection until custom TX");
        if !connection.send_ack_pending {
            let descheduled = connection.base.is_descheduled();
            sessions.add_self_custom_tx_event(runtime, connection.base.session, true, descheduled);
            connection.send_ack_pending = true;
            if descheduled {
                connection.base.flags.descheduled = false;
            }
        }
        if duplicate {
            connection.pending_dupacks = connection.pending_dupacks.saturating_add(1);
        }
    }

    /// VPP: tcp_output.c:1080-1089. Retransmit events use the old list.
    pub(crate) fn program_retransmit(
        &mut self,
        runtime: &DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
    ) {
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("TCP timer retains the connection until custom TX");
        if connection.retransmit_pending {
            return;
        }
        let descheduled = connection.base.is_descheduled();
        sessions.add_self_custom_tx_event(runtime, connection.base.session, false, descheduled);
        connection.retransmit_pending = true;
        if descheduled {
            connection.base.flags.descheduled = false;
        }
    }

    /// VPP: tcp_output.c:1037-1058. Buffer exhaustion drops only this ACK;
    /// the receive window was updated before the allocation attempt.
    fn send_ack(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
    ) {
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("custom TX retains the TCP connection");
        let available = sessions
            .session_from_handle(connection.base.session)
            .and_then(|session| session.rx_fifo())
            .expect("TCP ACK retains its Session RX FIFO")
            .max_enqueue();
        connection.set_rcv_wnd(available);
        let local = connection
            .local()
            .expect("connected TCP has a local endpoint");
        let remote = connection.remote();
        let segment = connection.control_segment(
            local,
            remote,
            TcpSegmentFlags::ACK,
            None,
            TcpCapabilities::default(),
        );
        let next = self.tco_next_node[usize::from(!remote.is_ipv4())];
        crate::enqueue_session_tcp_segment(runtime, sessions, connection_index, next, segment)
            .expect("validated TCP ACK fits a fresh Buffer header");
    }

    /// VPP: tcp_output.c:2070-2122. Count ACK attempts even when Buffer
    /// allocation fails; only allocated buffers enter Session pending TX.
    pub(super) fn send_acks(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
        max_burst: usize,
    ) -> usize {
        if max_burst == 0 {
            return 0;
        }
        let connection = self
            .connections
            .get(connection_index)
            .expect("custom TX retains the TCP connection");
        let pending = usize::from(connection.pending_dupacks);
        if pending == 0 {
            let outstanding =
                TcpSeq::from(connection.snd_una()).distance_to(connection.snd_nxt) as usize;
            let unsent = sessions
                .session_from_handle(connection.base.session)
                .and_then(|session| session.tx_fifo())
                .expect("custom TX retains its Session TX FIFO")
                .max_dequeue()
                .saturating_sub(outstanding);
            if connection.recovery.in_recovery()
                || unsent == 0
                || connection.state() != TcpState::Established
            {
                self.send_ack(runtime, sessions, connection_index);
                return 1;
            }
            return 0;
        }
        let blocks = connection.sack.block_count();
        if blocks == 0 {
            self.send_ack(runtime, sessions, connection_index);
            self.connections
                .get_mut(connection_index)
                .expect("custom TX retains the TCP connection")
                .pending_dupacks = 0;
            return 1;
        }
        self.connections
            .get_mut(connection_index)
            .expect("custom TX retains the TCP connection")
            .sack
            .reset_output_position();
        let attempts = (blocks / 3).min(pending).max(pending.min(3));
        for _ in 0..attempts.min(max_burst) {
            self.send_ack(runtime, sessions, connection_index);
        }
        if attempts < max_burst {
            self.connections
                .get_mut(connection_index)
                .expect("custom TX retains the TCP connection")
                .pending_dupacks = 0;
            self.connections
                .get_mut(connection_index)
                .expect("custom TX retains the TCP connection")
                .sack
                .reset_output_position();
            attempts
        } else {
            self.connections
                .get_mut(connection_index)
                .expect("custom TX retains the TCP connection")
                .pending_dupacks =
                u8::try_from(attempts - max_burst).expect("pending duplicate ACK count fits u8");
            self.program_ack(runtime, sessions, connection_index, true);
            max_burst
        }
    }

    /// VPP: tcp_output.c:1123-1270. The FIFO offset remains relative to
    /// snd_una; all payload bytes go directly into the final Buffer chain.
    pub(super) fn prepare_segment(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
        offset: u32,
        max_bytes: u32,
        retransmit: bool,
    ) -> usize {
        let connection = self
            .connections
            .get(connection_index)
            .expect("custom TX retains the TCP connection");
        let handle = connection.base.session;
        let next = self.tco_next_node[usize::from(!connection.remote().is_ipv4())];
        let fifo = sessions
            .session_from_handle(handle)
            .and_then(|session| session.tx_fifo())
            .expect("custom TX retains its Session TX FIFO");
        let available = fifo.max_dequeue().saturating_sub(offset as usize);
        let requested = available
            .min(max_bytes as usize)
            .min(connection.send_mss as usize);
        if requested == 0 {
            return 0;
        }
        let mut first = [0u32; 1];
        if runtime.buffer_alloc(&mut first) == 0 {
            return 0;
        }
        let first_index = first[0];
        let mut previous = first_index;
        let mut remaining = requested;
        let mut copied = 0usize;
        while remaining != 0 {
            let buffer = runtime.buffer_mut(previous);
            if previous == first_index {
                buffer.make_headroom(140);
            }
            let length = remaining
                .min(buffer.space_left_at_end())
                .min(u16::MAX as usize);
            assert_ne!(length, 0, "TCP Buffer has payload capacity");
            let payload = buffer.put_uninit(length as u16);
            assert_eq!(
                fifo.peek(offset as usize + copied, length, payload),
                length,
                "custom TX reads retained Session FIFO bytes",
            );
            copied += length;
            remaining -= length;
            if remaining == 0 {
                break;
            }
            let mut next_buffer = [0u32; 1];
            if runtime.buffer_alloc(&mut next_buffer) == 0 {
                runtime.buffer_free_one(first_index);
                return 0;
            }
            runtime
                .buffer_mut(previous)
                .set_next_buffer(Some(next_buffer[0]));
            previous = next_buffer[0];
        }
        if previous != first_index {
            let first_length = runtime.buffer(first_index).current_len();
            runtime
                .buffer_mut(first_index)
                .set_total_len_not_including_first(copied - first_length)
                .expect("TCP Buffer chain length fits its header");
        }
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("custom TX retains the TCP connection");
        let app_limited = !retransmit
            && connection.app_limited_for_send(
                u32::try_from(available).expect("TCP available FIFO bytes fit u32"),
            );
        if retransmit {
            connection.tx_intent_sequence =
                Some(TcpSeq::from(connection.snd_una()).advance(offset));
        }
        let segment = connection
            .tx_segment(copied, TcpCapabilities::default())
            .expect("custom TX retains a data-capable TCP connection");
        segment
            .write_to_buffer(runtime.buffer_mut(first_index))
            .expect("TCP Buffer headroom fits the transport header");
        let egress = hammer_core::buffer_opaque!(
            mut runtime.buffer_mut(first_index) => crate::TcpSecondaryOpaque
        )
        .egress_mut();
        egress.connection_index = connection_index;
        egress.worker_index = sessions.worker_index();
        egress.fib_index = connection.base.endpoint.fib_index();
        connection
            .commit_payload_tx(copied, self.last_timer_update, app_limited)
            .expect("custom TX commits its retained FIFO bytes once");
        sessions.add_pending_tx_buffer(runtime, first_index, next);
        copied
    }

    /// VPP: tcp_output.c:1672-1708. Recovery sends new bytes from the same
    /// FIFO only after retransmit/PRR grants remaining packet budget.
    fn transmit_unsent(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
        burst_size: usize,
        available_bytes: u32,
    ) -> usize {
        let connection = self
            .connections
            .get(connection_index)
            .expect("recovery retains the TCP connection");
        let offset = connection.snd_nxt().wrapping_sub(connection.snd_una());
        let max_dequeue = offset
            .checked_add(available_bytes)
            .expect("TCP flight and unsent FIFO bytes fit u32");
        let peer_space = connection.snd_wnd().saturating_sub(offset);
        let send_mss = connection.send_mss;
        let max_burst = burst_size.min((peer_space / send_mss) as usize);
        let mut sent = 0usize;
        let mut fifo_offset = offset;
        for _ in 0..max_burst {
            let written = self.prepare_segment(
                runtime,
                sessions,
                connection_index,
                fifo_offset,
                send_mss,
                false,
            );
            if written == 0 {
                break;
            }
            fifo_offset = fifo_offset
                .checked_add(written as u32)
                .expect("TCP send offset fits Session FIFO");
            sent += 1;
        }
        if sent != 0 {
            self.connections
                .get_mut(connection_index)
                .expect("recovery retains the TCP connection")
                .update_cwnd_limited(max_dequeue);
        }
        sent
    }

    /// VPP: tcp_output.c:1804-2065. The recovery owner selects and accounts
    /// ranges; TCP output copies only retained Session FIFO bytes.
    pub(super) fn retransmit(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
        burst_size: usize,
    ) -> usize {
        let now = (self.time_us * 1_000_000.0) as u64;
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("retransmit retains the TCP connection");
        let send_mss = connection.send_mss;
        let burst_bytes = if connection.base.is_tx_paced() {
            connection.base.pacer.update(now)
        } else {
            (burst_size as u32).saturating_mul(send_mss)
        };
        let max_segments = burst_size.min((burst_bytes / send_mss) as usize);
        if max_segments == 0 {
            self.program_retransmit(runtime, sessions, connection_index);
            return 0;
        }
        let flight = connection.recovery.bytes_in_flight();
        let recovery_space = connection
            .recovery
            .recovery_send_space(flight, send_mss)
            .unwrap_or(send_mss);
        let cc_limited = recovery_space < burst_bytes;
        let mut send_space = recovery_space.min(burst_bytes);
        let mut sent = 0usize;
        let mut sent_bytes = 0u32;
        let sack = connection.negotiated_options().sack;
        let no_sack_first = connection.recovery.no_sack_first_pending();
        let no_sack_end = connection.recovery.recovery_end_sequence();
        let mut no_sack_sequence = connection
            .tx_intent_sequence
            .unwrap_or(TcpSeq::from(connection.snd_una()));
        let handle = connection.base.session;
        let outstanding = connection.snd_nxt().wrapping_sub(connection.snd_una()) as usize;
        let available = sessions
            .session_from_handle(handle)
            .and_then(|session| session.tx_fifo())
            .expect("retransmit retains Session TX FIFO")
            .max_dequeue()
            .saturating_sub(outstanding);
        let mut buffer_exhausted = false;
        while sent < max_segments && send_space != 0 {
            if sack && send_space < send_mss {
                break;
            }
            let connection = self
                .connections
                .get(connection_index)
                .expect("retransmit retains the TCP connection");
            let intent = connection.tx_intent_sequence;
            let sample = if let Some(sequence) = intent {
                connection.recovery.sample_covering(sequence)
            } else if sack {
                connection.recovery.retransmit_candidate()
            } else if no_sack_first && no_sack_sequence < no_sack_end {
                connection.recovery.sample_covering(no_sack_sequence)
            } else {
                None
            };
            let Some(sample) = sample else {
                if intent.is_some() {
                    self.connections
                        .get_mut(connection_index)
                        .expect("retransmit retains the TCP connection")
                        .clear_tx_intent();
                    continue;
                }
                break;
            };
            let sequence = intent.unwrap_or_else(|| {
                if sack {
                    sample.sequence
                } else {
                    no_sack_sequence
                }
            });
            let offset = TcpSeq::from(connection.snd_una()).distance_to(sequence);
            let remaining = sequence.distance_to(sample.end_sequence).min(if sack {
                u32::MAX
            } else {
                sequence.distance_to(no_sack_end)
            });
            let requested = remaining.min(send_mss).min(send_space);
            if requested == 0 {
                break;
            }
            let written =
                self.prepare_segment(runtime, sessions, connection_index, offset, requested, true)
                    as u32;
            if written == 0 {
                self.program_retransmit(runtime, sessions, connection_index);
                buffer_exhausted = true;
                break;
            }
            sent += 1;
            sent_bytes = sent_bytes.saturating_add(written);
            send_space = send_space.saturating_sub(written);
            if !sack {
                no_sack_sequence = sequence.advance(written);
            }
            let connection = self
                .connections
                .get_mut(connection_index)
                .expect("retransmit retains the TCP connection");
            connection.recovery.on_retransmit_sent(written);
            if sack {
                connection
                    .recovery
                    .advance_high_rxt(sequence.advance(written));
            }
            let sample_complete = sequence.advance(written) == sample.end_sequence;
            let recovery_end = !sack && no_sack_sequence >= no_sack_end;
            if sample_complete || recovery_end {
                if sample_complete {
                    connection
                        .recovery
                        .commit_retransmit(sample.sequence, self.last_timer_update);
                }
                connection.tx_intent_sequence = None;
                connection.tx_intent_payload_len = 0;
            } else {
                connection.tx_intent_sequence = Some(sequence.advance(written));
                connection.tx_intent_payload_len = remaining - written;
                if sack || !no_sack_first || no_sack_sequence >= no_sack_end {
                    self.program_retransmit(runtime, sessions, connection_index);
                    break;
                }
            }
        }
        let mut new_data_remaining = false;
        if !buffer_exhausted && sent < max_segments && send_space >= send_mss && available != 0 {
            let connection = self
                .connections
                .get(connection_index)
                .expect("new-data recovery retains TCP connection");
            let available_bytes = u32::try_from(available).expect("Session FIFO length fits u32");
            let peer_space = connection
                .snd_wnd()
                .saturating_sub(u32::try_from(outstanding).expect("TCP flight size fits u32"));
            let permitted = if sack {
                // VPP tcp_output.c:1854-1869 leaves one MSS in the peer
                // window before sending new bytes during SACK recovery.
                send_space
                    .min(peer_space.saturating_sub(send_mss))
                    .min(available_bytes.max(send_mss))
            } else {
                send_space.min(peer_space).min(available_bytes)
            };
            let new_burst = (max_segments - sent)
                .min((permitted / send_mss) as usize)
                .min(if sack { 10 } else { usize::MAX });
            let sequence_before = self
                .connections
                .get(connection_index)
                .expect("retransmit retains TCP connection")
                .snd_nxt();
            let more = self.transmit_unsent(
                runtime,
                sessions,
                connection_index,
                new_burst,
                available_bytes,
            );
            sent += more;
            let sequence_after = self
                .connections
                .get(connection_index)
                .expect("retransmit retains TCP connection")
                .snd_nxt();
            let new_bytes = sequence_after.wrapping_sub(sequence_before);
            sent_bytes = sent_bytes.saturating_add(new_bytes);
            new_data_remaining = more != 0 && available > new_bytes as usize;
        }
        let connection = self
            .connections
            .get(connection_index)
            .expect("retransmit retains TCP connection");
        let has_more = connection.tx_intent_sequence.is_some()
            || new_data_remaining
            || (sack && connection.recovery.retransmit_candidate().is_some());
        if has_more {
            self.program_retransmit(runtime, sessions, connection_index);
        }
        if self
            .connections
            .get(connection_index)
            .expect("retransmit retains TCP connection")
            .base
            .is_tx_paced()
        {
            self.connections
                .get_mut(connection_index)
                .expect("retransmit retains TCP connection")
                .base
                .pacer
                .update_bytes(if cc_limited {
                    burst_bytes
                } else if sack {
                    sent_bytes.min(burst_bytes)
                } else {
                    (sent as u32).saturating_mul(send_mss).min(burst_bytes)
                });
        }
        if sent != 0 {
            self.connections
                .get_mut(connection_index)
                .expect("recovery burst retains TCP connection")
                .recovery
                .on_recovery_burst_sent();
        }
        sent
    }

    /// VPP tcp_output.c:1312-1380. Probe unsent Session FIFO bytes first;
    /// otherwise retransmit the retained tail without advancing snd_nxt.
    pub(super) fn send_tlp_probe(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
        connection_index: u32,
    ) -> usize {
        let connection = self
            .connections
            .get(connection_index)
            .expect("TLP retains its TCP connection");
        let outstanding = connection.snd_nxt().wrapping_sub(connection.snd_una());
        let handle = connection.base.session;
        let max_dequeue = sessions
            .session_from_handle(handle)
            .and_then(|session| session.tx_fifo())
            .expect("TLP retains its Session TX FIFO")
            .max_dequeue();
        let unsent = max_dequeue
            .saturating_sub(outstanding as usize)
            .min(u32::MAX as usize) as u32;
        let new_space = connection.tlp_new_data_space(unsent);
        let mut retransmitted = None;
        let mut written = if new_space != 0 {
            self.prepare_segment(
                runtime,
                sessions,
                connection_index,
                outstanding,
                new_space,
                false,
            )
        } else {
            0
        };
        if written == 0 && outstanding != 0 {
            let send_mss = self
                .connections
                .get(connection_index)
                .expect("TLP retains its TCP connection")
                .send_mss;
            let probe_len = outstanding.min(send_mss);
            written = self.prepare_segment(
                runtime,
                sessions,
                connection_index,
                outstanding - probe_len,
                probe_len,
                true,
            );
            if written != 0 {
                let connection = self
                    .connections
                    .get(connection_index)
                    .expect("tail TLP retains its TCP connection");
                let start = TcpSeq::from(connection.snd_una()).advance(outstanding - probe_len);
                retransmitted = Some((start, start.advance(written as u32)));
            }
        }
        if written != 0 {
            let connection = self
                .connections
                .get_mut(connection_index)
                .expect("published TLP retains its TCP connection");
            let bytes_in_flight = connection.recovery.bytes_in_flight();
            connection
                .congestion
                .on_tail_loss_probe(Instant::now(), bytes_in_flight);
            let probe_end = connection.snd_nxt().into();
            connection
                .recovery
                .record_tlp_probe(probe_end, retransmitted);
            if retransmitted.is_none() {
                connection.update_cwnd_limited(
                    u32::try_from(max_dequeue).expect("Session TX FIFO length fits u32"),
                );
            }
            if connection.base.is_tx_paced() {
                connection.base.pacer.update_bytes(written as u32);
            }
        }
        written
    }

    #[inline]
    pub fn new(worker: DataWorkerId, protocol: u8) -> Self {
        let time_origin = Instant::now();
        Self {
            cacheline0: CacheLineAlignMark,
            connections: Pool::with_capacity(DEFAULT_TCP_CONNECTION_CAPACITY),
            pending_deq_acked: Vec::with_capacity(256),
            pending_disconnects: Vec::with_capacity(256),
            pending_resets: Vec::with_capacity(256),
            time_us: 0.0,
            time_tstamp: 0,
            max_timers_per_loop: 10,
            pending_timers: FifoQueue::new(),
            cacheline1: CacheLineAlignMark,
            cached_opts: [0; 40],
            cached_segment: None,
            tx_buffers: Vec::new(),
            pending_cleanups: FifoQueue::new(),
            tco_next_node: [SessionQueueNext::from_slot(0); 2],
            timer_wheel: TimerWheel1t2w2048sl::with_timer_ids(
                TCP_TIMER_EXPIRY_BUDGET,
                TCP_TIMER_KIND_COUNT,
            ),
            expired_timers: Vec::new(),
            last_timer_update: time_origin,
            time_origin,
            time_origin_seconds: None,
            cacheline2: CacheLineAlignMark,
            lookup: TcpLookupState::new(worker),
            protocol,
        }
    }

    #[inline]
    pub(crate) fn insert_connection(&mut self, connection: TcpConnection) -> u32 {
        let index = self.connections.insert(connection);
        self.connections
            .get_mut(index)
            .expect("inserted TCP connection remains in its pool")
            .base
            .connection_index = index;
        index
    }

    #[inline]
    pub(crate) fn connection(&self, index: u32) -> Option<&TcpConnection> {
        self.connections.get(index)
    }

    #[inline]
    pub(crate) fn connection_mut(&mut self, index: u32) -> Option<&mut TcpConnection> {
        self.connections.get_mut(index)
    }

    #[inline]
    pub(crate) fn remove_connection(&mut self, index: u32) -> TcpConnection {
        self.connections
            .remove(index)
            .expect("TCP connection index remains live during removal")
    }

    /// VPP tcp_input.c:536-545. One connection appears once per input burst.
    pub(crate) fn program_dequeue(&mut self, connection_index: u32, bytes_acked: u32) {
        if bytes_acked == 0 {
            return;
        }
        let connection = self
            .connections
            .get_mut(connection_index)
            .expect("ACK retains its TCP connection");
        if connection.record_acked_bytes(bytes_acked) {
            self.pending_deq_acked.push(connection_index);
        }
    }

    /// VPP tcp_input.c:494-533. Session owns the FIFO drop and app notify.
    pub(crate) fn handle_postponed_dequeues(
        &mut self,
        runtime: &mut DataPlaneMain,
        sessions: &mut SessionWorker,
    ) -> RuntimeResult<()> {
        let mut timer_error = None;
        for &connection_index in &self.pending_deq_acked {
            let connection = self
                .connections
                .get_mut(connection_index)
                .expect("pending ACK retains its TCP connection");
            let bytes_acked = connection.take_burst_acked();
            if bytes_acked == 0 {
                continue;
            }
            let handle = connection.base.session;
            connection.record_flight_drained(self.time_us);
            sessions.tx_fifo_dequeue_drop(runtime, handle, bytes_acked);
            if connection.base.is_descheduled() {
                connection
                    .base
                    .clear_descheduled((self.time_us * 1_000_000.0) as u64);
                let fifo = sessions
                    .session_from_handle(handle)
                    .and_then(|session| session.tx_fifo())
                    .expect("ACK retains its Session TX FIFO");
                if fifo.max_dequeue() != 0 {
                    sessions
                        .enqueue_ready(handle, self.protocol)
                        .expect("ACK reschedules its registered Session");
                }
            }
            if let Err(error) =
                connection.retransmit_timer_after_ack(connection_index, &mut self.timer_wheel)
            {
                if timer_error.is_none() {
                    timer_error = Some(error);
                }
            }
            connection.update_tx_pacer();
            let fifo = sessions
                .session_from_handle(handle)
                .and_then(|session| session.tx_fifo())
                .expect("ACK retains its Session TX FIFO");
            if connection.fin_pending && fifo.max_dequeue() == 0 {
                let local = connection
                    .local()
                    .expect("closing TCP has a local endpoint");
                let remote = connection.remote();
                let segment = connection.control_segment(
                    local,
                    remote,
                    TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
                    None,
                    TcpCapabilities::default(),
                );
                connection.snd_nxt = connection.snd_nxt.advance(1);
                connection.fin_pending = false;
                connection.fin_sent = true;
                if connection.state() == TcpState::CloseWait {
                    connection.state = TcpState::LastAck;
                    if let Err(error) = timers::update(
                        &mut self.timer_wheel,
                        connection_index,
                        connection.timer_state_mut(),
                        TcpTimerKind::WaitClose,
                        crate::active_tcp_policy().last_ack,
                    ) && timer_error.is_none()
                    {
                        timer_error = Some(error);
                    }
                }
                let next = self.tco_next_node[usize::from(!remote.is_ipv4())];
                let sent = crate::enqueue_session_tcp_segment(
                    runtime,
                    sessions,
                    connection_index,
                    next,
                    segment,
                )
                .expect("valid TCP FIN fits a control Buffer");
                let interval = if sent {
                    connection.retransmit_timeout().retransmit_timeout()
                } else {
                    crate::active_tcp_policy().allocation_retry
                };
                if let Err(error) = timers::update(
                    &mut self.timer_wheel,
                    connection_index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Retransmit,
                    interval,
                ) && timer_error.is_none()
                {
                    timer_error = Some(error);
                }
            }
        }
        self.pending_deq_acked.clear();
        match timer_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
