use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel2t1w2048};

use crate::session::{AppSessionId, AppSessionReadyQueue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSessionTimerToken(u32);

impl AppSessionTimerToken {
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
pub struct AppSessionTimerExpiry {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
}

impl AppSessionTimerExpiry {
    #[inline(always)]
    pub const fn new(session_id: AppSessionId, token: AppSessionTimerToken) -> Self {
        Self { session_id, token }
    }

    #[inline(always)]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline(always)]
    pub const fn token(self) -> AppSessionTimerToken {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppSessionTimerSlot {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
    handle: TimerHandle,
    live: bool,
}

pub struct AppSessionTimerWheel {
    wheel: TimerWheel2t1w2048,
    slots: hammer_infra::vec::Vec<AppSessionTimerSlot>,
    expired_slots: hammer_infra::vec::Vec<u32>,
    pending: hammer_infra::vec::Vec<AppSessionTimerExpiry>,
}

impl AppSessionTimerWheel {
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
        session_id: AppSessionId,
        token: AppSessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.cancel(session_id, token);
        let user_handle = u32::try_from(self.slots.len())
            .map_err(|_| CoreError::internal("app session timer slot overflow"))?;
        let handle = self
            .wheel
            .start(user_handle, ticks)
            .map_err(timer_start_error)?;
        self.slots.push(AppSessionTimerSlot {
            session_id,
            token,
            handle,
            live: true,
        });
        Ok(())
    }

    pub fn cancel(&mut self, session_id: AppSessionId, token: AppSessionTimerToken) -> bool {
        let Some(slot) = self.live_timer_slot(session_id, token) else {
            return false;
        };
        let timer = self
            .slots
            .get_mut(slot)
            .expect("live app session timer slot should be valid");
        timer.live = false;
        self.wheel.stop(timer.handle)
    }

    pub fn expire(&mut self, ticks: u32, ready: &mut AppSessionReadyQueue) -> CoreResult<usize> {
        self.expired_slots.clear();
        let expired = self.wheel.expire(ticks, &mut self.expired_slots);
        let expired_slots: hammer_infra::vec::Vec<u32> =
            self.expired_slots.iter().copied().collect();
        for slot in expired_slots {
            let Some(timer) = self.slots.get_mut(slot as usize) else {
                return Err(CoreError::internal("app session timer slot is invalid"));
            };
            if !timer.live {
                continue;
            }
            timer.live = false;
            let expiry = AppSessionTimerExpiry::new(timer.session_id, timer.token);
            self.pending.push(expiry);
            ready.mark_ready(timer.session_id);
        }
        Ok(expired)
    }

    pub fn take_expiries(&mut self) -> hammer_infra::vec::Vec<AppSessionTimerExpiry> {
        self.pending.drain(..).collect()
    }

    fn live_timer_slot(
        &self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
    ) -> Option<usize> {
        self.slots
            .iter()
            .position(|timer| timer.live && timer.session_id == session_id && timer.token == token)
    }
}

impl Default for AppSessionTimerWheel {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

fn timer_start_error(error: TimerStartError) -> CoreError {
    CoreError::internal(format!("start app session timer: {error:?}"))
}
