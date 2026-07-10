use std::time::{Duration, Instant};

use hammer_core::error::CoreResult;
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_infra::vec::Vec;

use super::TcpNodeError;
use super::connection::TcpConnection;
use crate::transport::congestion::CongestionController;

const TCP_TIMER_KIND_COUNT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TcpTimerKind {
    Retransmit,
    Rack,
    Tlp,
    DelayedAck,
    Persist,
    KeepAlive,
    TimeWait,
    Pacing,
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

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub(super) struct TcpTimerSet: u16 {
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
    pub(super) fn active_bits(&self) -> u16 {
        (self.armed | self.pending).bits()
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
            wheel: TimerWheel1t2w2048sl::with_timer_ids(0, TCP_TIMER_KIND_COUNT),
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
        self.update(index, state, kind, interval)
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
        let ticks = self.elapsed_ticks(now);
        if ticks == 0 {
            return;
        }
        self.expired.clear();
        self.wheel.expire(ticks, &mut self.expired);
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

    fn elapsed_ticks(&mut self, now: Instant) -> u32 {
        let elapsed = now.saturating_duration_since(self.last_update);
        let ticks = (elapsed.as_nanos() / self.resolution.as_nanos()).min(u32::MAX as u128) as u32;
        if ticks != 0 {
            self.last_update += self.resolution * ticks;
        }
        ticks
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use hammer_infra::pool::{Index, Pool};
    use hammer_runtime::DataWorkerId;

    use super::{TcpTimerKind, TcpTimerState, TcpTimers};
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::TcpConnection;

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
        C: crate::transport::congestion::CongestionController,
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
}
