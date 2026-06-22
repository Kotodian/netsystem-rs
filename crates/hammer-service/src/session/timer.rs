use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel2t1w2048};

use crate::session::{SessionId, SessionReadyQueue};

const DEFAULT_SESSION_TIMER_CAPACITY: usize = 1024;

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
    version: u32,
    live: bool,
}

pub struct SessionTimerWheel {
    wheel: TimerWheel2t1w2048,
    slots: hammer_infra::vec::Vec<SessionTimerSlot>,
    expired_slots: hammer_infra::vec::Vec<u32>,
    live_slots: FlatHashTable<u128, u32>,
    pending_versions: FlatHashTable<u128, u32>,
    pending: hammer_infra::vec::Vec<(SessionTimerExpiry, u32)>,
    next_version: u32,
}

impl SessionTimerWheel {
    #[inline]
    pub fn new() -> Self {
        Self {
            wheel: TimerWheel2t1w2048::new(0),
            slots: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_TIMER_CAPACITY),
            expired_slots: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_TIMER_CAPACITY),
            live_slots: FlatHashTable::with_capacity(DEFAULT_SESSION_TIMER_CAPACITY),
            pending_versions: FlatHashTable::with_capacity(DEFAULT_SESSION_TIMER_CAPACITY),
            pending: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_TIMER_CAPACITY),
            next_version: 0,
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
        let version = self.next_version.wrapping_add(1).max(1);
        self.next_version = version;
        let handle = self
            .wheel
            .start(user_handle, ticks)
            .map_err(timer_start_error)?;
        self.slots.push(SessionTimerSlot {
            session_id,
            token,
            handle,
            version,
            live: true,
        });
        self.live_slots
            .insert(timer_key(session_id, token), user_handle);
        Ok(())
    }

    pub fn cancel(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        let key = timer_key(session_id, token);
        self.pending_versions.remove(&key);
        let Some(slot) = self.live_slots.remove(&key) else {
            return false;
        };
        let timer = self
            .slots
            .get_mut(slot as usize)
            .expect("live session timer slot should be valid");
        timer.live = false;
        self.wheel.stop(timer.handle)
    }

    pub fn expire(&mut self, ticks: u32, ready: &mut SessionReadyQueue) -> CoreResult<usize> {
        self.expired_slots.clear();
        self.wheel.expire(ticks, &mut self.expired_slots);
        let mut expired = 0;
        for expired_index in 0..self.expired_slots.len() {
            let slot = self.expired_slots[expired_index];
            let Some(timer) = self.slots.get_mut(slot as usize) else {
                return Err(CoreError::internal("session timer slot is invalid"));
            };
            if !timer.live {
                continue;
            }
            timer.live = false;
            let key = timer_key(timer.session_id, timer.token);
            self.live_slots.remove(&key);
            let expiry = SessionTimerExpiry::new(timer.session_id, timer.token);
            self.pending_versions.insert(key, timer.version);
            self.pending.push((expiry, timer.version));
            ready.mark_ready(timer.session_id);
            expired += 1;
        }
        Ok(expired)
    }

    pub fn take_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        let mut expiries = hammer_infra::vec::Vec::with_capacity(self.pending.len());
        for (expiry, version) in self.pending.drain(..) {
            let key = timer_key(expiry.session_id(), expiry.token());
            if self.pending_versions.lookup(&key) != Some(version) {
                continue;
            }
            self.pending_versions.remove(&key);
            expiries.push(expiry);
        }
        expiries
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

#[inline]
fn timer_key(session_id: SessionId, token: SessionTimerToken) -> u128 {
    (u128::from(session_id.get()) << 32) | u128::from(token.get())
}
