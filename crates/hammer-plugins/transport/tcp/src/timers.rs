use std::time::{Duration, Instant};

use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::Pool;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::RuntimeResult;

use super::TcpNodeError;
use super::connection::TcpConnection;

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
    pub(super) index: u32,
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
        index: u32,
        state: &mut TcpTimerState,
        kind: TcpTimerKind,
        interval: Duration,
    ) -> RuntimeResult<()> {
        if state.is_armed(kind) {
            return Ok(());
        }
        self.wheel
            .arm_timer(index, 0, kind.id(), self.duration_ticks(interval))
            .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
        state.arm(kind);
        Ok(())
    }

    pub(super) fn validate_interval(&self, interval: Duration) -> RuntimeResult<()> {
        let ticks = self.duration_ticks(interval);
        if ticks > TCP_TIMER_WHEEL_MAX_INTERVAL_TICKS
            || self.wheel.current_tick().checked_add(ticks).is_none()
        {
            return Err(TcpNodeError::TimerUpdateFailed.into());
        }
        Ok(())
    }

    pub(super) fn reset(&mut self, index: u32, state: &mut TcpTimerState, kind: TcpTimerKind) {
        let _ = self.wheel.cancel_timer(index, 0, kind.id());
        state.reset(kind);
    }

    pub(super) fn update(
        &mut self,
        index: u32,
        state: &mut TcpTimerState,
        kind: TcpTimerKind,
        interval: Duration,
    ) -> RuntimeResult<()> {
        self.wheel
            .update_timer(index, 0, kind.id(), self.duration_ticks(interval))
            .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
        state.arm(kind);
        Ok(())
    }

    pub(super) fn advance(&mut self, now: Instant, connections: &mut Pool<TcpConnection>) {
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
            let Some((index, _, kind_id)) = self.wheel.take_expired_timer(*payload) else {
                continue;
            };
            let Some(kind) = TcpTimerKind::from_id(kind_id) else {
                continue;
            };
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

    pub(super) fn take_pending(
        &mut self,
        connections: &mut Pool<TcpConnection>,
    ) -> Option<TcpTimerToken> {
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
