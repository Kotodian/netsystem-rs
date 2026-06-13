use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel2t1w2048};

use crate::session::{SessionId, SessionReadyQueue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionTimerToken(u32);

impl SessionTimerToken {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTimerExpiry {
    session_id: SessionId,
    token: SessionTimerToken,
}

impl SessionTimerExpiry {
    #[inline(always)]
    pub const fn new(session_id: SessionId, token: SessionTimerToken) -> Self {
        Self { session_id, token }
    }

    #[inline(always)]
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    #[inline(always)]
    pub const fn token(self) -> SessionTimerToken {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionTimerSlot {
    session_id: SessionId,
    token: SessionTimerToken,
    handle: TimerHandle,
    live: bool,
}

pub struct SessionTimerWheel {
    wheel: TimerWheel2t1w2048,
    slots: hammer_infra::vec::Vec<SessionTimerSlot>,
    expired_slots: hammer_infra::vec::Vec<u32>,
    pending: hammer_infra::vec::Vec<SessionTimerExpiry>,
}

impl SessionTimerWheel {
    #[inline]
    pub fn new() -> Self {
        Self {
            wheel: TimerWheel2t1w2048::new(0),
            slots: hammer_infra::vec::Vec::new(),
            expired_slots: hammer_infra::vec::Vec::new(),
            pending: hammer_infra::vec::Vec::new(),
        }
    }

    pub fn arm_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.cancel(session_id, token);
        let user_handle = u32::try_from(self.slots.len())
            .map_err(|_| CoreError::internal("session timer slot overflow"))?;
        let handle = self
            .wheel
            .start(user_handle, ticks)
            .map_err(timer_start_error)?;
        self.slots.push(SessionTimerSlot {
            session_id,
            token,
            handle,
            live: true,
        });
        Ok(())
    }

    pub fn cancel(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        let Some(slot) = self.live_timer_slot(session_id, token) else {
            return false;
        };
        let timer = self
            .slots
            .get_mut(slot)
            .expect("live session timer slot should be valid");
        timer.live = false;
        self.wheel.stop(timer.handle)
    }

    pub fn expire(&mut self, ticks: u32, ready: &mut SessionReadyQueue) -> CoreResult<usize> {
        self.expired_slots.clear();
        let expired = self.wheel.expire(ticks, &mut self.expired_slots);
        let expired_slots: hammer_infra::vec::Vec<u32> =
            self.expired_slots.iter().copied().collect();
        for slot in expired_slots {
            let Some(timer) = self.slots.get_mut(slot as usize) else {
                return Err(CoreError::internal("session timer slot is invalid"));
            };
            if !timer.live {
                continue;
            }
            timer.live = false;
            let expiry = SessionTimerExpiry::new(timer.session_id, timer.token);
            self.pending.push(expiry);
            ready.mark_ready(timer.session_id);
        }
        Ok(expired)
    }

    pub fn take_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        self.pending.drain(..).collect()
    }

    fn live_timer_slot(&self, session_id: SessionId, token: SessionTimerToken) -> Option<usize> {
        self.slots
            .iter()
            .position(|timer| timer.live && timer.session_id == session_id && timer.token == token)
    }
}

impl Default for SessionTimerWheel {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

fn timer_start_error(error: TimerStartError) -> CoreError {
    CoreError::internal(format!("start session timer: {error:?}"))
}
