use std::time::{Duration, Instant};

use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::RuntimeResult;

use super::TcpNodeError;
use super::worker::TcpWorker;

pub(super) const TCP_TIMER_MAX_TICKS_PER_UPDATE: u32 = 1_024;
pub(super) const TCP_TIMER_EXPIRY_BUDGET: usize = 256;
pub(super) const TCP_TIMER_WHEEL_MAX_INTERVAL_TICKS: u64 = 2048 * 2048 - 1;
/// VPP tcp_types.h:71 uses a 100 us TCP timer tick, including RACK REO.
pub(super) const TCP_TIMER_RESOLUTION: Duration = Duration::from_micros(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum TcpTimerKind {
    Retransmit = 0,
    Rack = 1,
    Tlp = 2,
    DelayedAck = 3,
    Persist = 4,
    KeepAlive = 5,
    WaitClose = 6,
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
            6 => Some(Self::WaitClose),
            7 => Some(Self::Pacing),
            _ => None,
        }
    }

    #[inline]
    const fn flag(self) -> TcpTimerSet {
        TcpTimerSet::from_bits_retain(1 << self.id())
    }
}

pub(super) const TCP_TIMER_KIND_COUNT: usize = TcpTimerKind::Pacing.id() as usize + 1;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    struct TcpTimerSet: u16 {
        const RETRANSMIT = 1 << 0;
        const RACK = 1 << 1;
        const TLP = 1 << 2;
        const DELAYED_ACK = 1 << 3;
        const PERSIST = 1 << 4;
        const KEEP_ALIVE = 1 << 5;
        const WAIT_CLOSE = 1 << 6;
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

/// VPP: tcp_timer.h:25-58. Timer handles live in the worker wheel; the
/// connection owns the armed/pending bits used to cancel stale expirations.
#[inline]
pub(super) fn set(
    wheel: &mut TimerWheel1t2w2048sl<u32>,
    index: u32,
    state: &mut TcpTimerState,
    kind: TcpTimerKind,
    interval: Duration,
) -> RuntimeResult<()> {
    if state.is_armed(kind) {
        return Ok(());
    }
    wheel
        .arm_timer(index, 0, kind.id(), duration_ticks(interval))
        .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
    state.arm(kind);
    Ok(())
}

pub(super) fn validate_interval(
    wheel: &TimerWheel1t2w2048sl<u32>,
    interval: Duration,
) -> RuntimeResult<()> {
    let ticks = duration_ticks(interval);
    if ticks > TCP_TIMER_WHEEL_MAX_INTERVAL_TICKS
        || wheel.current_tick().checked_add(ticks).is_none()
    {
        return Err(TcpNodeError::TimerUpdateFailed.into());
    }
    Ok(())
}

#[inline]
pub(super) fn reset(
    wheel: &mut TimerWheel1t2w2048sl<u32>,
    index: u32,
    state: &mut TcpTimerState,
    kind: TcpTimerKind,
) {
    let _ = wheel.cancel_timer(index, 0, kind.id());
    state.reset(kind);
}

#[inline]
pub(super) fn update(
    wheel: &mut TimerWheel1t2w2048sl<u32>,
    index: u32,
    state: &mut TcpTimerState,
    kind: TcpTimerKind,
    interval: Duration,
) -> RuntimeResult<()> {
    wheel
        .update_timer(index, 0, kind.id(), duration_ticks(interval))
        .map_err(|_| TcpNodeError::TimerUpdateFailed)?;
    state.arm(kind);
    Ok(())
}

impl TcpWorker {
    /// VPP: tcp.c:1462-1509, `tcp_expired_timers_dispatch`.
    pub(super) fn advance_timer_wheel(&mut self, now: Instant) {
        let elapsed_ticks = now
            .saturating_duration_since(self.last_timer_update)
            .as_nanos()
            / TCP_TIMER_RESOLUTION.as_nanos();
        if elapsed_ticks == 0 {
            return;
        }
        if self.timer_wheel.is_empty() {
            let elapsed_nanos = elapsed_ticks * TCP_TIMER_RESOLUTION.as_nanos();
            let seconds = (elapsed_nanos / 1_000_000_000) as u64;
            let nanos = (elapsed_nanos % 1_000_000_000) as u32;
            self.last_timer_update += Duration::new(seconds, nanos);
            return;
        }
        let requested_ticks = elapsed_ticks.min(u128::from(TCP_TIMER_MAX_TICKS_PER_UPDATE)) as u32;
        self.expired_timers.clear();
        let tick_before = self.timer_wheel.current_tick();
        self.timer_wheel
            .expire(requested_ticks, &mut self.expired_timers);
        let consumed_ticks = u32::try_from(self.timer_wheel.current_tick() - tick_before)
            .expect("TCP timer wheel consumes no more than the requested u32 ticks");
        self.last_timer_update += TCP_TIMER_RESOLUTION * consumed_ticks;
        for payload in self.expired_timers.as_slice() {
            let Some((index, _, kind_id)) = self.timer_wheel.take_expired_timer(*payload) else {
                continue;
            };
            let Some(kind) = TcpTimerKind::from_id(kind_id) else {
                continue;
            };
            let Some(connection) = self.connections.get_mut(index) else {
                continue;
            };
            let state = connection.timer_state_mut();
            if !state.is_armed(kind) {
                continue;
            }
            state.armed.remove(kind.flag());
            state.pending.insert(kind.flag());
            self.pending_timers.push_back(TcpTimerToken { index, kind });
        }
    }

    /// VPP: tcp.c:1293-1335, `tcp_dispatch_pending_timers`.
    pub(super) fn take_pending_timer(&mut self, budget: &mut usize) -> Option<TcpTimerToken> {
        while *budget != 0 {
            let token = self.pending_timers.pop_front()?;
            *budget -= 1;
            let Some(connection) = self.connections.get_mut(token.index) else {
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
}

#[inline]
fn duration_ticks(duration: Duration) -> u64 {
    duration
        .as_nanos()
        .div_ceil(TCP_TIMER_RESOLUTION.as_nanos())
        .max(1)
        .min(u64::MAX as u128) as u64
}
