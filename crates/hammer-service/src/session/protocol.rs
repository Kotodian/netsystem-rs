use crate::session::ready::SessionReadyQueue;
use hammer_adapter::DataPlaneBuffers;
use hammer_core::error::CoreResult;
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;

use crate::session::SessionId;
use crate::transport::congestion::CongestionController;
use crate::transport::tcp::TcpNodeError;
use crate::transport::tcp::connection::{TCP_TIMER_COUNT, TcpConnection};

/// Refresh a connection's TCP timer-wheel entries for one session slot.
///
/// For every `timer_id in 0..TCP_TIMER_COUNT`:
///   - if `keep_mask & (1 << timer_id)` is set and
///     `conn.timer_ticks(timer_id, now)` returns `Some(ticks)`:
///     `update_timer(slot, gen, timer_id, ticks)`.
///   - otherwise: `cancel_timer(slot, gen, timer_id)` (drops a stale wheel
///     entry left behind by `TcpConnection::timer_reset`, which only clears
///     the active bit and does not cancel the wheel entry).
///
/// `keep_mask` is the union of (caller allowlist) | (caller-supplied
/// always-keep bits). Callers compute it and pass it in; this helper owns the
/// cancel/update dispatch and the per-(slot, gen) wheel calls. The loop still
/// visits all `TCP_TIMER_COUNT` ids because cancellation is required for ids
/// that were armed last refresh but are no longer kept — iterating only set
/// bits would skip those cancellations.
///
/// `timer_ticks` self-gates on `timer_is_active` (returns `None` for inactive
/// timers), so callers pass the bare allowlist as `keep_mask` rather than
/// `allowlist & active`: an allowlisted-but-inactive timer yields `None` and
/// is cancelled, exactly preserving the prior per-site `!timer_is_active ->
/// cancel` gate.
pub(crate) fn refresh_tcp_timers<C>(
    timers: &mut TimerWheel1t2w2048sl<u32>,
    conn: &TcpConnection<C>,
    session: PoolIndex,
    keep_mask: u16,
    now: std::time::Instant,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let slot = session.slot();
    let generation = session.generation();
    for timer_id in 0..TCP_TIMER_COUNT {
        if (keep_mask & (1u16 << timer_id)) != 0 {
            if let Some(ticks) = conn.timer_ticks(timer_id, now) {
                timers
                    .update_timer(slot, generation, timer_id, ticks)
                    .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
                continue;
            }
        }
        let _ = timers.cancel_timer(slot, generation, timer_id);
    }
    Ok(())
}

pub(crate) struct SessionQueueControlContext {
    timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
    ready: *mut SessionReadyQueue,
    buffers: *const DataPlaneBuffers,
    current_session_id: SessionId,
    has_pending_tx: bool,
}

impl SessionQueueControlContext {
    #[inline]
    pub(crate) fn new(
        timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
        ready: *mut SessionReadyQueue,
        buffers: *const DataPlaneBuffers,
        current_session_id: SessionId,
        has_pending_tx: bool,
    ) -> Self {
        Self {
            timer_wheel,
            ready,
            buffers,
            current_session_id,
            has_pending_tx,
        }
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        unsafe { &*self.buffers }
    }

    #[inline]
    pub(crate) fn timer_wheel(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        unsafe { &mut *self.timer_wheel }
    }

    /// Refresh the current session's TCP timer-wheel entries for `conn`.
    ///
    /// Thin wrapper over the free `refresh_tcp_timers` that supplies this
    /// context's timer wheel and session slot. See that fn for the
    /// cancel/update semantics and the `keep_mask` contract.
    #[inline]
    pub(crate) fn refresh_tcp_timers<C>(
        &mut self,
        conn: &TcpConnection<C>,
        keep_mask: u16,
        now: std::time::Instant,
    ) -> CoreResult<()>
    where
        C: CongestionController + 'static,
    {
        let session = self.current_session_id.pool_index();
        crate::session::protocol::refresh_tcp_timers(
            self.timer_wheel(),
            conn,
            session,
            keep_mask,
            now,
        )
    }

    #[inline]
    pub(crate) fn mark_ready(&mut self) {
        if self.ready.is_null() {
            return;
        }
        unsafe { &mut *self.ready }.mark_ready(self.current_session_id);
    }

    #[inline]
    pub(crate) const fn session_id(&self) -> SessionId {
        self.current_session_id
    }

    #[inline]
    pub(crate) const fn has_pending_tx(&self) -> bool {
        self.has_pending_tx
    }
}
