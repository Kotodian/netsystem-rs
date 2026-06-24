use crate::bitmap::Bitmap;
use crate::vec::Vec;

const INVALID_INDEX: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerHandle {
    slot: u32,
    generation: u32,
}

impl TimerHandle {
    #[inline(always)]
    pub const fn slot(self) -> u32 {
        self.slot
    }

    #[inline(always)]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerStartError {
    ZeroInterval,
    IntervalOutOfRange,
}

#[derive(Clone, Copy, Debug)]
struct TimerEntry<T>
where
    T: Copy,
{
    next: u32,
    prev: u32,
    payload: Option<T>,
    generation: u32,
    live: bool,
    fast_ring_offset: u32,
    slow_ring_offset: u32,
    expiration_tick: u64,
}

impl<T> TimerEntry<T>
where
    T: Copy,
{
    #[inline]
    fn list_head(index: u32) -> Self {
        Self {
            next: index,
            prev: index,
            payload: None,
            generation: 0,
            live: false,
            fast_ring_offset: 0,
            slow_ring_offset: 0,
            expiration_tick: 0,
        }
    }

    #[inline]
    fn timer(generation: u32, payload: T, expiration_tick: u64) -> Self {
        Self {
            next: INVALID_INDEX,
            prev: INVALID_INDEX,
            payload: Some(payload),
            generation,
            live: true,
            fast_ring_offset: 0,
            slow_ring_offset: 0,
            expiration_tick,
        }
    }
}

pub struct TimerWheel<
    T,
    const WHEELS: usize,
    const SLOTS: usize,
    const FAST_BITMAP: bool,
    const OVERFLOW: bool,
    const DUPLICATE_STOP: bool,
> where
    T: Copy,
{
    current_tick: u64,
    current_index: [u32; 3],
    slot_heads: Vec<u32>,
    overflow_head: u32,
    entries: Vec<TimerEntry<T>>,
    free: Vec<u32>,
    fast_slot_bitmap: Bitmap,
    slot_generations: Vec<u32>,
    timer_slots: Vec<u32>,
    timer_entry_generations: Vec<u32>,
    timer_bitmap: Bitmap,
    timers_per_slot: usize,
    len: usize,
    max_expirations: usize,
}

pub type TimerWheel1t1w32sl<T> = TimerWheel<T, 1, 32, false, false, true>;
pub type TimerWheel1w32FastHint<T> = TimerWheel<T, 1, 32, true, false, true>;
pub type TimerWheel1t2w32sl<T> = TimerWheel<T, 2, 32, false, false, true>;
pub type TimerWheel2t1w2048sl<T> = TimerWheel<T, 1, 2048, false, false, true>;
pub type TimerWheel1t2w2048sl<T> = TimerWheel<T, 2, 2048, false, false, true>;

impl<
    T,
    const WHEELS: usize,
    const SLOTS: usize,
    const FAST_BITMAP: bool,
    const OVERFLOW: bool,
    const DUPLICATE_STOP: bool,
> TimerWheel<T, WHEELS, SLOTS, FAST_BITMAP, OVERFLOW, DUPLICATE_STOP>
where
    T: Copy,
{
    pub fn new(max_expirations: usize) -> Self {
        Self::validate_geometry();

        let mut entries = Vec::new();
        let mut slot_heads = Vec::with_capacity(WHEELS * SLOTS);
        for _ in 0..(WHEELS * SLOTS) {
            slot_heads.push(push_list_head(&mut entries));
        }

        let overflow_head = if OVERFLOW {
            push_list_head(&mut entries)
        } else {
            INVALID_INDEX
        };

        let fast_slot_bitmap = if FAST_BITMAP {
            Bitmap::with_capacity(SLOTS)
        } else {
            Bitmap::new()
        };

        Self {
            current_tick: 0,
            current_index: [0; 3],
            slot_heads,
            overflow_head,
            entries,
            free: Vec::new(),
            fast_slot_bitmap,
            slot_generations: Vec::new(),
            timer_slots: Vec::new(),
            timer_entry_generations: Vec::new(),
            timer_bitmap: Bitmap::new(),
            timers_per_slot: 0,
            len: 0,
            max_expirations,
        }
    }

    #[inline(always)]
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn start(&mut self, payload: T, interval: u64) -> Result<TimerHandle, TimerStartError> {
        self.validate_interval(interval)?;
        let expiration_tick = self
            .current_tick
            .checked_add(interval)
            .ok_or(TimerStartError::IntervalOutOfRange)?;
        let slot = self.allocate_timer(payload, expiration_tick);
        self.place_timer(slot, interval);
        let generation = self.entries[slot as usize].generation;
        Ok(TimerHandle { slot, generation })
    }

    pub fn stop(&mut self, handle: TimerHandle) -> bool {
        let Some(slot) = self.live_slot(handle) else {
            debug_assert!(DUPLICATE_STOP, "invalid timer stop");
            return false;
        };

        self.unlink(slot as u32);
        self.free_timer(slot as u32);
        true
    }

    pub fn update(&mut self, handle: TimerHandle, interval: u64) -> Result<bool, TimerStartError> {
        self.validate_interval(interval)?;
        let expiration_tick = self
            .current_tick
            .checked_add(interval)
            .ok_or(TimerStartError::IntervalOutOfRange)?;
        let Some(slot) = self.live_slot(handle) else {
            return Ok(false);
        };

        let slot = slot as u32;
        self.unlink(slot);
        self.entries[slot as usize].expiration_tick = expiration_tick;
        self.place_timer(slot, interval);
        Ok(true)
    }

    #[inline]
    pub fn handle_is_live(&self, handle: TimerHandle) -> bool {
        self.live_slot(handle).is_some()
    }

    pub fn expire(&mut self, ticks: u32, expired: &mut Vec<T>) -> usize {
        let initial_len = expired.len();
        let max_expirations = self.expiration_budget();

        for _ in 0..ticks {
            if expired.len() - initial_len >= max_expirations {
                break;
            }
            self.advance_one_tick(expired);
        }

        expired.len() - initial_len
    }

    pub fn first_expires_in_ticks(&self) -> Option<u32> {
        if !FAST_BITMAP {
            return None;
        }

        let current = self.current_index[0] as usize;
        for offset in 0..SLOTS {
            let slot = (current + offset) & (SLOTS - 1);
            if self.fast_slot_bitmap.is_set(slot) {
                return Some(offset as u32);
            }
        }

        Some(SLOTS as u32)
    }

    #[inline]
    fn expiration_budget(&self) -> usize {
        if self.max_expirations == 0 {
            usize::MAX
        } else {
            self.max_expirations
        }
    }

    fn advance_one_tick(&mut self, expired: &mut Vec<T>) {
        self.current_tick = self
            .current_tick
            .checked_add(1)
            .expect("timer wheel current tick overflow");

        let fast_wrapped = self.advance_ring(0);
        let slow_wrapped = if fast_wrapped && WHEELS > 1 {
            self.advance_ring(1)
        } else {
            false
        };
        let glacier_wrapped = if slow_wrapped && WHEELS > 2 {
            self.advance_ring(2)
        } else {
            false
        };

        if glacier_wrapped && OVERFLOW {
            self.process_overflow(expired);
        }
        if slow_wrapped && WHEELS > 2 {
            self.cascade_glacier(expired);
        }
        if fast_wrapped && WHEELS > 1 {
            self.cascade_slow(expired);
        }

        self.expire_fast_slot(expired);
    }

    fn advance_ring(&mut self, ring: usize) -> bool {
        let next = self.current_index[ring] + 1;
        if next == SLOTS as u32 {
            self.current_index[ring] = 0;
            true
        } else {
            self.current_index[ring] = next;
            false
        }
    }

    fn process_overflow(&mut self, expired: &mut Vec<T>) {
        debug_assert!(OVERFLOW);
        let head = self.overflow_head;
        let mut next = self.take_list(head);
        while next != head {
            let slot = next;
            next = self.entries[slot as usize].next;
            self.detach_entry(slot);

            let expiration_tick = self.entries[slot as usize].expiration_tick;
            if expiration_tick <= self.current_tick {
                self.expire_timer(slot, expired);
                continue;
            }

            let interval = expiration_tick - self.current_tick;
            if self.should_overflow(interval) {
                self.add_to_list(self.overflow_head, slot);
            } else {
                self.place_timer_direct(slot, interval);
            }
        }
    }

    fn cascade_glacier(&mut self, expired: &mut Vec<T>) {
        let head = self.slot_head(2, self.current_index[2]);
        let mut next = self.take_list(head);
        while next != head {
            let slot = next;
            next = self.entries[slot as usize].next;
            self.detach_entry(slot);

            let slow_offset = self.entries[slot as usize].slow_ring_offset;
            let fast_offset = self.entries[slot as usize].fast_ring_offset;
            if slow_offset == self.current_index[1] && fast_offset == self.current_index[0] {
                self.expire_timer(slot, expired);
            } else if slow_offset == self.current_index[1] {
                self.add_to_fast_slot(fast_offset, slot);
            } else {
                self.add_to_list(self.slot_head(1, slow_offset), slot);
            }
        }
    }

    fn cascade_slow(&mut self, expired: &mut Vec<T>) {
        let head = self.slot_head(1, self.current_index[1]);
        let mut next = self.take_list(head);
        while next != head {
            let slot = next;
            next = self.entries[slot as usize].next;
            self.detach_entry(slot);

            let fast_offset = self.entries[slot as usize].fast_ring_offset;
            if fast_offset == self.current_index[0] {
                self.expire_timer(slot, expired);
            } else {
                self.add_to_fast_slot(fast_offset, slot);
            }
        }
    }

    fn expire_fast_slot(&mut self, expired: &mut Vec<T>) {
        let fast_slot = self.current_index[0];
        let head = self.slot_head(0, fast_slot);
        let mut next = self.take_list(head);
        while next != head {
            let slot = next;
            next = self.entries[slot as usize].next;
            self.detach_entry(slot);
            self.expire_timer(slot, expired);
        }

        if FAST_BITMAP {
            self.fast_slot_bitmap.clear(fast_slot as usize);
        }
    }

    fn expire_timer(&mut self, slot: u32, expired: &mut Vec<T>) {
        debug_assert!(self.entries[slot as usize].live);
        let payload = self.entries[slot as usize]
            .payload
            .expect("live timer must carry payload");
        expired.push(payload);
        self.free_timer(slot);
    }

    fn allocate_timer(&mut self, payload: T, expiration_tick: u64) -> u32 {
        if let Some(slot) = self.free.pop() {
            let entry = &mut self.entries[slot as usize];
            let generation = entry.generation.wrapping_add(1).max(1);
            *entry = TimerEntry::timer(generation, payload, expiration_tick);
            self.len += 1;
            return slot;
        }

        let slot = self.entries.len();
        assert!(
            u32::try_from(slot).is_ok(),
            "timer wheel entry index overflow"
        );
        self.entries
            .push(TimerEntry::timer(1, payload, expiration_tick));
        self.len += 1;
        slot as u32
    }

    fn free_timer(&mut self, slot: u32) {
        let entry = &mut self.entries[slot as usize];
        debug_assert!(entry.live);
        entry.live = false;
        entry.next = INVALID_INDEX;
        entry.prev = INVALID_INDEX;
        entry.payload = None;
        self.len -= 1;
        self.free.push(slot);
    }

    fn place_timer(&mut self, slot: u32, interval: u64) {
        if self.should_overflow(interval) {
            self.add_to_list(self.overflow_head, slot);
        } else {
            self.place_timer_direct(slot, interval);
        }
    }

    fn place_timer_direct(&mut self, slot: u32, interval: u64) {
        let (fast_offset, slow_offset, glacier_offset) = self.timer_offsets(interval);

        if WHEELS > 2 && glacier_offset != self.current_index[2] {
            self.entries[slot as usize].slow_ring_offset = slow_offset;
            self.entries[slot as usize].fast_ring_offset = fast_offset;
            self.add_to_list(self.slot_head(2, glacier_offset), slot);
        } else if WHEELS > 1 && slow_offset != self.current_index[1] {
            self.entries[slot as usize].fast_ring_offset = fast_offset;
            self.add_to_list(self.slot_head(1, slow_offset), slot);
        } else {
            self.add_to_fast_slot(fast_offset, slot);
        }
    }

    fn timer_offsets(&self, interval: u64) -> (u32, u32, u32) {
        let shift = Self::ring_shift();
        let mask = Self::ring_mask();
        let slots = SLOTS as u64;
        let mut remaining = interval;

        let mut glacier_offset = 0;
        if WHEELS > 2 {
            let glacier_shift = shift * 2;
            glacier_offset = remaining >> glacier_shift;
            remaining -= glacier_offset << glacier_shift;
        }

        let mut slow_offset = 0;
        if WHEELS > 1 {
            slow_offset = remaining >> shift;
            remaining -= slow_offset << shift;
        }

        let mut fast_offset = remaining & mask;
        fast_offset += u64::from(self.current_index[0]);
        let mut carry = fast_offset >= slots;
        fast_offset &= mask;

        if WHEELS > 1 {
            slow_offset += u64::from(self.current_index[1]) + u64::from(carry);
            carry = slow_offset >= slots;
            slow_offset &= mask;
        }

        if WHEELS > 2 {
            glacier_offset += u64::from(self.current_index[2]) + u64::from(carry);
            glacier_offset &= mask;
        }

        (
            fast_offset as u32,
            slow_offset as u32,
            glacier_offset as u32,
        )
    }

    fn should_overflow(&self, interval: u64) -> bool {
        if !(OVERFLOW && WHEELS == 3) {
            return false;
        }

        let horizon = Self::horizon();
        let phase = self.current_tick & (horizon - 1);
        interval.saturating_add(phase) >= horizon
    }

    fn add_to_fast_slot(&mut self, fast_offset: u32, slot: u32) {
        self.add_to_list(self.slot_head(0, fast_offset), slot);
        if FAST_BITMAP {
            self.fast_slot_bitmap.set(fast_offset as usize);
        }
    }

    fn add_to_list(&mut self, head: u32, slot: u32) {
        debug_assert!(self.entries[slot as usize].live);
        let first = self.entries[head as usize].next;
        self.entries[slot as usize].next = first;
        self.entries[slot as usize].prev = head;
        self.entries[first as usize].prev = slot;
        self.entries[head as usize].next = slot;
    }

    fn unlink(&mut self, slot: u32) {
        let prev = self.entries[slot as usize].prev;
        let next = self.entries[slot as usize].next;
        debug_assert_ne!(prev, INVALID_INDEX);
        debug_assert_ne!(next, INVALID_INDEX);
        self.entries[prev as usize].next = next;
        self.entries[next as usize].prev = prev;
        self.detach_entry(slot);
    }

    fn take_list(&mut self, head: u32) -> u32 {
        let next = self.entries[head as usize].next;
        self.entries[head as usize].next = head;
        self.entries[head as usize].prev = head;
        next
    }

    fn detach_entry(&mut self, slot: u32) {
        self.entries[slot as usize].next = INVALID_INDEX;
        self.entries[slot as usize].prev = INVALID_INDEX;
    }

    #[inline]
    fn slot_head(&self, ring: usize, slot: u32) -> u32 {
        debug_assert!(ring < WHEELS);
        debug_assert!((slot as usize) < SLOTS);
        self.slot_heads[ring * SLOTS + slot as usize]
    }

    fn live_slot(&self, handle: TimerHandle) -> Option<usize> {
        let slot = handle.slot as usize;
        if slot >= self.entries.len() {
            return None;
        }

        let entry = self.entries[slot];
        (entry.live && entry.generation == handle.generation).then_some(slot)
    }

    fn validate_interval(&self, interval: u64) -> Result<(), TimerStartError> {
        if interval == 0 {
            return Err(TimerStartError::ZeroInterval);
        }
        if self.current_tick.checked_add(interval).is_none() {
            return Err(TimerStartError::IntervalOutOfRange);
        }
        if !(OVERFLOW && WHEELS == 3) && interval > Self::max_direct_interval() {
            return Err(TimerStartError::IntervalOutOfRange);
        }
        Ok(())
    }

    fn validate_geometry() {
        assert!(
            (1..=3).contains(&WHEELS),
            "timer wheel supports one, two, or three rings"
        );
        assert!(SLOTS > 0, "timer wheel needs at least one slot");
        assert!(
            SLOTS.is_power_of_two(),
            "timer wheel slot count must be a power of two"
        );
        assert!(
            u32::try_from(SLOTS).is_ok(),
            "timer wheel slot count must fit u32"
        );
        let _ = Self::horizon();
    }

    #[inline]
    fn ring_shift() -> u32 {
        SLOTS.trailing_zeros()
    }

    #[inline]
    fn ring_mask() -> u64 {
        SLOTS as u64 - 1
    }

    fn horizon() -> u64 {
        let mut horizon = 1u64;
        for _ in 0..WHEELS {
            horizon = horizon
                .checked_mul(SLOTS as u64)
                .expect("timer wheel horizon overflow");
        }
        horizon
    }

    fn max_direct_interval() -> u64 {
        if WHEELS == 1 {
            SLOTS as u64
        } else {
            Self::horizon() - 1
        }
    }
}

impl<
    const WHEELS: usize,
    const SLOTS: usize,
    const FAST_BITMAP: bool,
    const OVERFLOW: bool,
    const DUPLICATE_STOP: bool,
> TimerWheel<u32, WHEELS, SLOTS, FAST_BITMAP, OVERFLOW, DUPLICATE_STOP>
{
    #[inline]
    pub fn with_timer_ids(max_expirations: usize, timers_per_slot: usize) -> Self {
        let mut wheel = Self::new(max_expirations);
        assert!(timers_per_slot > 0, "timers per slot must be non-zero");
        wheel.timers_per_slot = timers_per_slot;
        wheel
    }

    pub fn arm_timer(
        &mut self,
        slot: u32,
        generation: u32,
        timer_id: u32,
        interval: u64,
    ) -> Result<(), TimerStartError> {
        self.prepare_slot(slot, generation);
        let flat = self.timer_flat_index(slot, timer_id);
        let _ = self.cancel_timer(slot, generation, timer_id);
        let handle = self.start(flat as u32, interval)?;
        self.timer_slots[flat] = handle.slot();
        self.timer_entry_generations[flat] = handle.generation();
        self.timer_bitmap.set(flat);
        Ok(())
    }

    pub fn cancel_timer(&mut self, slot: u32, generation: u32, timer_id: u32) -> bool {
        if self.timers_per_slot == 0 {
            return false;
        }
        let slot = slot as usize;
        if slot >= self.slot_generations.len() || self.slot_generations[slot] != generation {
            return false;
        }
        let flat = self.timer_flat_index(slot as u32, timer_id);
        if !self.timer_bitmap.is_set(flat) {
            return false;
        }
        let handle = TimerHandle {
            slot: self.timer_slots[flat],
            generation: self.timer_entry_generations[flat],
        };
        self.clear_timer_slot(flat);
        self.stop(handle)
    }

    pub fn update_timer(
        &mut self,
        slot: u32,
        generation: u32,
        timer_id: u32,
        interval: u64,
    ) -> Result<(), TimerStartError> {
        self.prepare_slot(slot, generation);
        let flat = self.timer_flat_index(slot, timer_id);
        if self.timer_bitmap.is_set(flat) {
            let handle = TimerHandle {
                slot: self.timer_slots[flat],
                generation: self.timer_entry_generations[flat],
            };
            if self.update(handle, interval)? {
                return Ok(());
            }
            self.clear_timer_slot(flat);
        }
        self.arm_timer(slot, generation, timer_id, interval)
    }

    pub fn take_expired_timer(&mut self, payload: u32) -> Option<(u32, u32, u32)> {
        if self.timers_per_slot == 0 {
            return None;
        }
        let flat = payload as usize;
        if flat >= self.timer_slots.len() || !self.timer_bitmap.is_set(flat) {
            return None;
        }
        let slot = flat / self.timers_per_slot;
        let timer_id = (flat % self.timers_per_slot) as u32;
        let generation = *self.slot_generations.get(slot)?;
        self.clear_timer_slot(flat);
        Some((slot as u32, generation, timer_id))
    }

    fn prepare_slot(&mut self, slot: u32, generation: u32) {
        assert!(
            self.timers_per_slot > 0,
            "timers per slot are not configured"
        );
        let slot = slot as usize;
        if slot >= self.slot_generations.len() {
            let additional = slot + 1 - self.slot_generations.len();
            self.slot_generations.reserve(additional);
            while self.slot_generations.len() <= slot {
                self.slot_generations.push(0);
            }
            let required = (slot + 1) * self.timers_per_slot;
            let additional = required.saturating_sub(self.timer_slots.len());
            self.timer_slots.reserve(additional);
            self.timer_entry_generations.reserve(additional);
            while self.timer_slots.len() < required {
                self.timer_slots.push(INVALID_INDEX);
                self.timer_entry_generations.push(0);
            }
        }
        if self.slot_generations[slot] == generation {
            return;
        }
        self.reset_slot(slot);
        self.slot_generations[slot] = generation;
    }

    fn reset_slot(&mut self, slot: usize) {
        if self.timers_per_slot == 0 {
            return;
        }
        let start = slot * self.timers_per_slot;
        let end = start + self.timers_per_slot;
        if end > self.timer_slots.len() {
            return;
        }
        for flat in start..end {
            if !self.timer_bitmap.is_set(flat) {
                continue;
            }
            let handle = TimerHandle {
                slot: self.timer_slots[flat],
                generation: self.timer_entry_generations[flat],
            };
            self.clear_timer_slot(flat);
            let _ = self.stop(handle);
        }
    }

    #[inline]
    fn timer_flat_index(&self, slot: u32, timer_id: u32) -> usize {
        assert!(
            self.timers_per_slot > 0,
            "timers per slot are not configured"
        );
        let timer_id = timer_id as usize;
        assert!(
            timer_id < self.timers_per_slot,
            "timer id exceeds configured timers per slot"
        );
        slot as usize * self.timers_per_slot + timer_id
    }

    #[inline]
    fn clear_timer_slot(&mut self, flat: usize) {
        self.timer_bitmap.clear(flat);
        self.timer_slots[flat] = INVALID_INDEX;
        self.timer_entry_generations[flat] = 0;
    }
}

fn push_list_head<T>(entries: &mut Vec<TimerEntry<T>>) -> u32
where
    T: Copy,
{
    let slot = entries.len();
    assert!(
        u32::try_from(slot).is_ok(),
        "timer wheel entry index overflow"
    );
    let slot = slot as u32;
    entries.push(TimerEntry::list_head(slot));
    slot
}
