use std::time::{Duration, Instant};

use hammer_core::error::CoreResult;
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;

use super::TcpNodeError;
use super::connection::TcpConnection;
use hammer_service::transport::congestion::CongestionController;

const TCP_TIMER_MAX_TICKS_PER_UPDATE: u32 = 1_024;
const TCP_TIMER_EXPIRY_BUDGET: usize = 256;
const TCP_TIMER_WHEEL_MAX_INTERVAL_TICKS: u64 = 2048 * 2048 - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum TcpTimerKind {
    Retransmit = 0,
    Rack = 1,
    Tlp = 2,
    DelayedAck = 3,
    Persist = 4,
    KeepAlive = 5,
    TimeWait = 6,
    Pacing = 7,
}

impl TcpTimerKind {
    #[inline]
    const fn id(self) -> u32 {
        self as u32
    }

    #[inline]
    pub(super) const fn from_id(id: u32) -> Option<Self> {
        match id {
            0 => Some(Self::Retransmit),
            1 => Some(Self::Rack),
            2 => Some(Self::Tlp),
            3 => Some(Self::DelayedAck),
            4 => Some(Self::Persist),
            5 => Some(Self::KeepAlive),
            6 => Some(Self::TimeWait),
            7 => Some(Self::Pacing),
            _ => None,
        }
    }

    #[inline]
    const fn flag(self) -> TcpTimerSet {
        TcpTimerSet::from_bits_retain(1 << self.id())
    }
}

const TCP_TIMER_KIND_COUNT: usize = TcpTimerKind::Pacing.id() as usize + 1;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    struct TcpTimerSet: u16 {
        const RETRANSMIT = 1 << 0;
        const RACK = 1 << 1;
        const TLP = 1 << 2;
        const DELAYED_ACK = 1 << 3;
        const PERSIST = 1 << 4;
        const KEEP_ALIVE = 1 << 5;
        const TIME_WAIT = 1 << 6;
        const PACING = 1 << 7;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct TcpTimerState {
    armed: TcpTimerSet,
    pending: TcpTimerSet,
}

impl TcpTimerState {
    #[inline]
    pub(super) fn is_active(&self, kind: TcpTimerKind) -> bool {
        self.armed.contains(kind.flag()) || self.pending.contains(kind.flag())
    }

    #[inline]
    pub(super) fn is_armed(&self, kind: TcpTimerKind) -> bool {
        self.armed.contains(kind.flag())
    }

    #[inline]
    pub(super) fn is_pending(&self, kind: TcpTimerKind) -> bool {
        self.pending.contains(kind.flag())
    }

    #[inline]
    pub(super) fn arm(&mut self, kind: TcpTimerKind) {
        self.armed.insert(kind.flag());
    }

    #[inline]
    pub(super) fn reset(&mut self, kind: TcpTimerKind) {
        self.armed.remove(kind.flag());
        self.pending.remove(kind.flag());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TcpTimerToken {
    pub(super) index: PoolIndex,
    pub(super) kind: TcpTimerKind,
}

pub(super) struct TcpTimers {
    wheel: TimerWheel1t2w2048sl<u32>,
    expired: Vec<u32>,
    pending: FifoQueue<TcpTimerToken>,
    last_update: Instant,
    resolution: Duration,
}

impl TcpTimers {
    pub(super) fn new(last_update: Instant, resolution: Duration) -> Self {
        assert!(
            !resolution.is_zero(),
            "TCP timer resolution must be non-zero"
        );
        Self {
            wheel: TimerWheel1t2w2048sl::with_timer_ids(
                TCP_TIMER_EXPIRY_BUDGET,
                TCP_TIMER_KIND_COUNT,
            ),
            expired: Vec::new(),
            pending: FifoQueue::new(),
            last_update,
            resolution,
        }
    }

    pub(super) fn set(
        &mut self,
        index: PoolIndex,
        state: &mut TcpTimerState,
        kind: TcpTimerKind,
        interval: Duration,
    ) -> CoreResult<()> {
        if state.is_armed(kind) {
            return Ok(());
        }
        self.wheel
            .arm_timer(
                index.slot(),
                index.generation(),
                kind.id(),
                self.duration_ticks(interval),
            )
            .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
        state.arm(kind);
        Ok(())
    }

    pub(super) fn validate_interval(&self, interval: Duration) -> CoreResult<()> {
        let ticks = self.duration_ticks(interval);
        if ticks > TCP_TIMER_WHEEL_MAX_INTERVAL_TICKS
            || self.wheel.current_tick().checked_add(ticks).is_none()
        {
            return Err(TcpNodeError::TimerUpdateFailed.into());
        }
        Ok(())
    }

    pub(super) fn reset(
        &mut self,
        index: PoolIndex,
        state: &mut TcpTimerState,
        kind: TcpTimerKind,
    ) {
        let _ = self
            .wheel
            .cancel_timer(index.slot(), index.generation(), kind.id());
        state.reset(kind);
    }

    pub(super) fn update(
        &mut self,
        index: PoolIndex,
        state: &mut TcpTimerState,
        kind: TcpTimerKind,
        interval: Duration,
    ) -> CoreResult<()> {
        self.wheel
            .update_timer(
                index.slot(),
                index.generation(),
                kind.id(),
                self.duration_ticks(interval),
            )
            .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
        state.arm(kind);
        Ok(())
    }

    pub(super) fn advance<C>(&mut self, now: Instant, connections: &mut Pool<TcpConnection<C>>)
    where
        C: CongestionController,
    {
        let elapsed_ticks = self.elapsed_ticks(now);
        if elapsed_ticks == 0 {
            return;
        }
        if self.wheel.is_empty() {
            self.fast_forward_empty_wheel(elapsed_ticks);
            return;
        }
        let requested_ticks = elapsed_ticks.min(u128::from(TCP_TIMER_MAX_TICKS_PER_UPDATE)) as u32;
        self.expired.clear();
        let tick_before = self.wheel.current_tick();
        self.wheel.expire(requested_ticks, &mut self.expired);
        let consumed_ticks = u32::try_from(self.wheel.current_tick() - tick_before)
            .expect("TCP timer wheel consumes no more than the requested u32 ticks");
        self.last_update += self.resolution * consumed_ticks;
        for payload in self.expired.as_slice() {
            let Some((slot, generation, kind_id)) = self.wheel.take_expired_timer(*payload) else {
                continue;
            };
            let Some(kind) = TcpTimerKind::from_id(kind_id) else {
                continue;
            };
            let index = PoolIndex::new(slot, generation);
            let Some(connection) = connections.get_mut(index) else {
                continue;
            };
            let state = connection.timer_state_mut();
            if !state.is_armed(kind) {
                continue;
            }
            state.armed.remove(kind.flag());
            state.pending.insert(kind.flag());
            self.pending.push_back(TcpTimerToken { index, kind });
        }
    }

    pub(super) fn take_pending<C>(
        &mut self,
        connections: &mut Pool<TcpConnection<C>>,
    ) -> Option<TcpTimerToken>
    where
        C: CongestionController,
    {
        while let Some(token) = self.pending.pop_front() {
            let Some(connection) = connections.get_mut(token.index) else {
                continue;
            };
            let state = connection.timer_state_mut();
            if !state.is_pending(token.kind) {
                continue;
            }
            state.pending.remove(token.kind.flag());
            if state.is_armed(token.kind) {
                continue;
            }
            return Some(token);
        }
        None
    }

    #[inline]
    fn duration_ticks(&self, duration: Duration) -> u64 {
        let resolution = self.resolution.as_nanos();
        duration
            .as_nanos()
            .div_ceil(resolution)
            .max(1)
            .min(u64::MAX as u128) as u64
    }

    fn elapsed_ticks(&self, now: Instant) -> u128 {
        let elapsed = now.saturating_duration_since(self.last_update);
        elapsed.as_nanos() / self.resolution.as_nanos()
    }

    fn fast_forward_empty_wheel(&mut self, elapsed_ticks: u128) {
        let elapsed_nanos = elapsed_ticks * self.resolution.as_nanos();
        let seconds = (elapsed_nanos / 1_000_000_000) as u64;
        let nanos = (elapsed_nanos % 1_000_000_000) as u32;
        self.last_update += Duration::new(seconds, nanos);
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use hammer_infra::pool::{Index, Pool};
    use hammer_runtime::DataWorkerId;

    use super::{
        TCP_TIMER_EXPIRY_BUDGET, TCP_TIMER_MAX_TICKS_PER_UPDATE, TcpTimerKind, TcpTimerState,
        TcpTimers,
    };
    use crate::TcpConnection;
    use hammer_service::transport::congestion::BbrController;

    fn test_connection() -> TcpConnection<BbrController> {
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote address");
        TcpConnection::new(None, DataWorkerId::new(0), 0, None, remote)
    }

    fn test_connections() -> (Pool<TcpConnection<BbrController>>, Index) {
        let mut connections = Pool::with_capacity(1);
        let index = connections
            .insert(test_connection())
            .expect("insert test connection");
        (connections, index)
    }

    fn test_timer_state<C>(connections: &Pool<TcpConnection<C>>, index: Index) -> TcpTimerState
    where
        C: hammer_service::transport::congestion::CongestionController,
    {
        *connections
            .get(index)
            .expect("test connection")
            .timer_state()
    }

    #[test]
    fn tcp_timer_expiry_moves_only_exact_kind_from_armed_to_pending() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm retransmit timer");
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::KeepAlive,
                    Duration::from_millis(30),
                )
                .expect("arm keepalive timer");
        }

        timers.advance(now + Duration::from_millis(10), &mut connections);

        let state = test_timer_state(&connections, index);
        assert_eq!(
            (
                state.is_armed(TcpTimerKind::Retransmit),
                state.is_pending(TcpTimerKind::Retransmit),
                state.is_armed(TcpTimerKind::KeepAlive),
                state.is_pending(TcpTimerKind::KeepAlive),
            ),
            (false, true, true, false),
        );
    }

    #[test]
    fn tcp_timer_reset_while_pending_invalidates_expiry() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers.reset(index, state, TcpTimerKind::Retransmit);
        }

        assert_eq!(timers.take_pending(&mut connections), None);
        assert!(!test_timer_state(&connections, index).is_active(TcpTimerKind::Retransmit));
    }

    #[test]
    fn tcp_timer_rearm_while_pending_makes_old_token_stale() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(30),
                )
                .expect("rearm retransmit timer");
        }
        let before_dispatch = test_timer_state(&connections, index);
        let token = timers.take_pending(&mut connections);
        let after_dispatch = test_timer_state(&connections, index);

        assert_eq!(
            (
                before_dispatch.is_pending(TcpTimerKind::Retransmit),
                before_dispatch.is_armed(TcpTimerKind::Retransmit),
                token,
                after_dispatch.is_pending(TcpTimerKind::Retransmit),
                after_dispatch.is_armed(TcpTimerKind::Retransmit),
            ),
            (true, true, None, false, true),
        );
    }

    #[test]
    fn tcp_timer_token_for_removed_connection_generation_is_ignored() {
        let now = Instant::now();
        let (mut connections, old_index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(old_index)
                .expect("old connection")
                .timer_state_mut();
            timers
                .set(
                    old_index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm old connection timer");
        }
        let _ = connections
            .remove(old_index)
            .expect("remove old connection");
        let new_index = connections
            .insert(test_connection())
            .expect("insert replacement connection");
        timers.advance(now + Duration::from_millis(10), &mut connections);

        assert_eq!(
            (
                new_index.slot(),
                new_index.generation() != old_index.generation(),
                test_timer_state(&connections, new_index).is_active(TcpTimerKind::Retransmit),
                timers.take_pending(&mut connections),
            ),
            (old_index.slot(), true, false, None),
        );
    }

    #[test]
    fn tcp_timer_update_preserves_the_new_deadline() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .update(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(25),
                )
                .expect("update retransmit timer");
        }
        timers.advance(now + Duration::from_millis(20), &mut connections);
        let at_old_deadline = test_timer_state(&connections, index);
        timers.advance(now + Duration::from_millis(39), &mut connections);
        let before_new_deadline = test_timer_state(&connections, index);
        timers.advance(now + Duration::from_millis(40), &mut connections);
        let at_new_deadline = test_timer_state(&connections, index);

        assert_eq!(
            (
                at_old_deadline.is_armed(TcpTimerKind::Retransmit),
                at_old_deadline.is_pending(TcpTimerKind::Retransmit),
                before_new_deadline.is_armed(TcpTimerKind::Retransmit),
                before_new_deadline.is_pending(TcpTimerKind::Retransmit),
                at_new_deadline.is_armed(TcpTimerKind::Retransmit),
                at_new_deadline.is_pending(TcpTimerKind::Retransmit),
            ),
            (true, false, true, false, false, true),
        );
    }

    #[test]
    fn tcp_timer_repeated_set_preserves_original_deadline() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("keep retransmit timer armed");
        }
        timers.advance(now + Duration::from_millis(20), &mut connections);

        let state = test_timer_state(&connections, index);
        assert_eq!(
            (
                state.is_armed(TcpTimerKind::Retransmit),
                state.is_pending(TcpTimerKind::Retransmit),
            ),
            (false, true),
        );
    }

    #[test]
    fn tcp_timer_advance_zero_elapsed_does_not_move_clock() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now, &mut connections);

        assert_eq!(
            (
                timers.wheel.current_tick(),
                timers.last_update,
                test_timer_state(&connections, index).is_armed(TcpTimerKind::Retransmit),
            ),
            (0, now, true),
        );
    }

    #[test]
    fn tcp_timer_advance_above_tick_cap_preserves_carryover() {
        let resolution = Duration::from_millis(10);
        let now = Instant::now();
        let deadline = now + resolution * (TCP_TIMER_MAX_TICKS_PER_UPDATE + 1);
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, resolution);

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    resolution * (TCP_TIMER_MAX_TICKS_PER_UPDATE + 1),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(deadline, &mut connections);
        let after_capped_advance = test_timer_state(&connections, index);
        let consumed_tick = timers.wheel.current_tick();
        let consumed_time = timers.last_update;
        timers.advance(deadline, &mut connections);
        let after_carryover = test_timer_state(&connections, index);

        assert_eq!(
            (
                consumed_tick,
                consumed_time,
                after_capped_advance.is_armed(TcpTimerKind::Retransmit),
                after_capped_advance.is_pending(TcpTimerKind::Retransmit),
                after_carryover.is_armed(TcpTimerKind::Retransmit),
                after_carryover.is_pending(TcpTimerKind::Retransmit),
            ),
            (
                u64::from(TCP_TIMER_MAX_TICKS_PER_UPDATE),
                now + resolution * TCP_TIMER_MAX_TICKS_PER_UPDATE,
                true,
                false,
                false,
                true,
            ),
        );
    }

    #[test]
    fn tcp_timer_empty_wheel_large_jump_fast_forwards_absolute_anchor() {
        let resolution = Duration::from_millis(10);
        let jump_ticks = TCP_TIMER_MAX_TICKS_PER_UPDATE * 100;
        let now = Instant::now();
        let jumped_now = now + resolution * jump_ticks + Duration::from_millis(5);
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, resolution);

        timers.advance(jumped_now, &mut connections);
        let wheel_tick_after_jump = timers.wheel.current_tick();
        let anchor_after_jump = timers.last_update;
        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(index, state, TcpTimerKind::Retransmit, resolution)
                .expect("arm retransmit timer after jump");
        }
        timers.advance(jumped_now, &mut connections);
        let before_completed_tick = test_timer_state(&connections, index);
        timers.advance(jumped_now + Duration::from_millis(5), &mut connections);
        let after_completed_tick = test_timer_state(&connections, index);

        assert_eq!(
            (
                wheel_tick_after_jump,
                anchor_after_jump,
                before_completed_tick.is_armed(TcpTimerKind::Retransmit),
                before_completed_tick.is_pending(TcpTimerKind::Retransmit),
                after_completed_tick.is_armed(TcpTimerKind::Retransmit),
                after_completed_tick.is_pending(TcpTimerKind::Retransmit),
            ),
            (0, now + resolution * jump_ticks, true, false, false, true,),
        );
    }

    #[test]
    fn tcp_timer_expiry_budget_preserves_successive_deadline_carryover() {
        let resolution = Duration::from_millis(10);
        let now = Instant::now();
        let connection_count = TCP_TIMER_EXPIRY_BUDGET + 1;
        let mut connections = Pool::with_capacity(connection_count);
        let mut timers = TcpTimers::new(now, resolution);
        let mut last_index = None;

        for offset in 0..connection_count {
            let index = connections
                .insert(test_connection())
                .expect("insert test connection");
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    resolution * (offset as u32 + 1),
                )
                .expect("arm successive timer");
            last_index = Some(index);
        }
        let last_index = last_index.expect("last connection index");
        let deadline = now + resolution * connection_count as u32;

        timers.advance(deadline, &mut connections);
        let first_tick = timers.wheel.current_tick();
        let first_anchor = timers.last_update;
        let mut first_tokens = 0;
        while timers.take_pending(&mut connections).is_some() {
            first_tokens += 1;
        }
        let last_state_before_carryover = test_timer_state(&connections, last_index);
        timers.advance(deadline, &mut connections);
        let second_token = timers.take_pending(&mut connections);

        assert_eq!(
            (
                first_tick,
                first_anchor,
                first_tokens,
                last_state_before_carryover.is_armed(TcpTimerKind::Retransmit),
                last_state_before_carryover.is_pending(TcpTimerKind::Retransmit),
                timers.wheel.current_tick(),
                timers.last_update,
                second_token.map(|token| token.index),
            ),
            (
                TCP_TIMER_EXPIRY_BUDGET as u64,
                now + resolution * TCP_TIMER_EXPIRY_BUDGET as u32,
                TCP_TIMER_EXPIRY_BUDGET,
                true,
                false,
                connection_count as u64,
                deadline,
                Some(last_index),
            ),
        );
    }

    #[test]
    fn tcp_timer_queued_token_for_removed_connection_generation_is_ignored() {
        let now = Instant::now();
        let (mut connections, old_index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(old_index)
                .expect("old connection")
                .timer_state_mut();
            timers
                .set(
                    old_index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm old connection timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        let _ = connections
            .remove(old_index)
            .expect("remove old connection");
        let new_index = connections
            .insert(test_connection())
            .expect("insert replacement connection");
        {
            let state = connections
                .get_mut(new_index)
                .expect("new connection")
                .timer_state_mut();
            timers
                .set(
                    new_index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(30),
                )
                .expect("arm replacement connection timer");
        }

        let token = timers.take_pending(&mut connections);
        let new_state = test_timer_state(&connections, new_index);
        assert_eq!(
            (
                token,
                new_state.is_armed(TcpTimerKind::Retransmit),
                new_state.is_pending(TcpTimerKind::Retransmit),
            ),
            (None, true, false),
        );
    }

    #[test]
    fn tcp_timer_kind_ids_and_flags_are_unique_roundtrips() {
        let kinds = [
            TcpTimerKind::Retransmit,
            TcpTimerKind::Rack,
            TcpTimerKind::Tlp,
            TcpTimerKind::DelayedAck,
            TcpTimerKind::Persist,
            TcpTimerKind::KeepAlive,
            TcpTimerKind::TimeWait,
            TcpTimerKind::Pacing,
        ];
        let mut ids = 0u16;
        let mut flags = 0u16;

        for kind in kinds {
            let id_bit = 1u16 << kind.id();
            assert_eq!(TcpTimerKind::from_id(kind.id()), Some(kind));
            assert_eq!(ids & id_bit, 0, "duplicate TCP timer id");
            assert_eq!(flags & kind.flag().bits(), 0, "duplicate TCP timer flag");
            ids |= id_bit;
            flags |= kind.flag().bits();
        }

        assert_eq!((ids, flags), (0xff, 0xff));
    }

    #[test]
    fn tcp_timer_duplicate_queued_token_dispatches_once() {
        let now = Instant::now();
        let (mut connections, index) = test_connections();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));

        {
            let state = connections
                .get_mut(index)
                .expect("test connection")
                .timer_state_mut();
            timers
                .set(
                    index,
                    state,
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(10),
                )
                .expect("arm retransmit timer");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        let duplicate = *timers.pending.front().expect("pending timer token");
        timers.pending.push_back(duplicate);

        assert_eq!(
            (
                timers.take_pending(&mut connections),
                timers.take_pending(&mut connections),
            ),
            (Some(duplicate), None),
        );
    }
}
