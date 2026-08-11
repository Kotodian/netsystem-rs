use crate::{VclError, VclSession, VclSessionState};

/// Generation-safe client-local Session identity: the local pool slot index
/// plus the slot's generation. A freed slot bumps its generation, so a stale
/// handle (or a handle to a slot reused by another Session) is rejected by
/// generation comparison. The wire `SessionHandle` remains VPP-shaped without
/// a generation; the generation is purely local pool identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct VclSessionHandle(u64);

impl VclSessionHandle {
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self(((generation as u64) << 32) | slot as u64)
    }

    /// Local pool slot index.
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    /// Local slot generation.
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Raw packed identity; also the CONNECT_STREAM control context
    /// (VPP `mp->context = s->session_index` analog).
    pub const fn raw(self) -> u64 {
        self.0
    }

    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

struct Slot {
    generation: u32,
    session: Option<VclSession>,
}

/// Fixed-capacity worker-local Session slot pool (VPP's per-worker Session
/// pool). Slot 0 is allocated first; free slots are reused LIFO with a
/// bumped generation.
pub(crate) struct SessionPool {
    slots: Box<[Slot]>,
    free: Vec<u32>,
}

impl SessionPool {
    pub(crate) fn new(capacity: usize) -> Self {
        let free = (0..capacity as u32).rev().collect();
        let slots = (0..capacity)
            .map(|_| Slot {
                generation: 1,
                session: None,
            })
            .collect();
        Self { slots, free }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Allocates a slot for `session` and returns its generation-safe handle.
    pub(crate) fn alloc(&mut self, session: VclSession) -> Result<VclSessionHandle, VclError> {
        let slot = self.free.pop().ok_or(VclError::PoolFull {
            capacity: self.slots.len(),
        })?;
        let entry = &mut self.slots[slot as usize];
        let handle = VclSessionHandle::new(slot, entry.generation);
        entry.session = Some(session);
        Ok(handle)
    }

    /// Frees the slot if `handle` is current. Returns false for a stale or
    /// already-freed handle (callers treat it as a no-op).
    pub(crate) fn free(&mut self, handle: VclSessionHandle) -> bool {
        let Some(entry) = self.slots.get_mut(handle.slot() as usize) else {
            return false;
        };
        if entry.generation != handle.generation() || entry.session.is_none() {
            return false;
        }
        entry.session = None;
        entry.generation = entry.generation.wrapping_add(1);
        self.free.push(handle.slot());
        true
    }

    pub(crate) fn get(&self, handle: VclSessionHandle) -> Result<&VclSession, VclError> {
        let entry = self
            .slots
            .get(handle.slot() as usize)
            .ok_or(VclError::InvalidHandle { handle })?;
        if entry.generation != handle.generation() {
            return Err(VclError::InvalidHandle { handle });
        }
        entry
            .session
            .as_ref()
            .ok_or(VclError::InvalidHandle { handle })
    }

    pub(crate) fn get_mut(
        &mut self,
        handle: VclSessionHandle,
    ) -> Result<&mut VclSession, VclError> {
        let entry = self
            .slots
            .get_mut(handle.slot() as usize)
            .ok_or(VclError::InvalidHandle { handle })?;
        if entry.generation != handle.generation() {
            return Err(VclError::InvalidHandle { handle });
        }
        entry
            .session
            .as_mut()
            .ok_or(VclError::InvalidHandle { handle })
    }

    pub(crate) fn state(&self, handle: VclSessionHandle) -> Result<VclSessionState, VclError> {
        Ok(self.get(handle)?.state)
    }
}

#[cfg(test)]
mod tests {
    use hammer_runtime::app::TransportProtocol;

    use super::*;

    fn session() -> VclSession {
        VclSession::new(TransportProtocol::Quic, false)
    }

    fn assert_invalid(handle: VclSessionHandle, result: Result<&VclSession, VclError>) {
        assert!(
            matches!(result, Err(VclError::InvalidHandle { handle: actual }) if actual == handle),
            "expected stale handle {handle:?}"
        );
    }

    #[test]
    fn slot_reuse_bumps_generation_and_stales_old_handle() {
        let mut pool = SessionPool::new(4);
        let first = pool.alloc(session()).expect("first allocation");
        assert_eq!(first.slot(), 0);
        assert_eq!(first.generation(), 1);

        assert!(pool.free(first));
        assert_invalid(first, pool.get(first));

        let second = pool.alloc(session()).expect("reused slot");
        assert_eq!(second.slot(), first.slot());
        assert_eq!(second.generation(), 2);
        // The old handle must never resolve to the reused Session.
        assert_invalid(first, pool.get(first));
        assert!(pool.get(second).is_ok());
    }

    #[test]
    fn freeing_twice_is_a_no_op() {
        let mut pool = SessionPool::new(4);
        let handle = pool.alloc(session()).expect("allocation");
        assert!(pool.free(handle));
        assert!(!pool.free(handle), "second free must be a no-op");
        assert!(!pool.free(VclSessionHandle::new(handle.slot() + 4, 1)));
    }

    #[test]
    fn pool_rejects_out_of_range_handle() {
        let mut pool = SessionPool::new(2);
        let handle = VclSessionHandle::new(7, 1);
        assert_invalid(handle, pool.get(handle));
        assert!(!pool.free(handle));
        // Allocation still works after the rejected access.
        assert!(pool.alloc(session()).is_ok());
    }

    #[test]
    fn pool_capacity_is_fixed_and_exhaustion_is_typed() {
        let mut pool = SessionPool::new(2);
        let a = pool.alloc(session()).expect("slot 0");
        let b = pool.alloc(session()).expect("slot 1");
        assert_eq!(pool.capacity(), 2);
        assert!(
            matches!(
                pool.alloc(session()),
                Err(VclError::PoolFull { capacity: 2 })
            ),
            "pool exhaustion must be a typed capacity error"
        );
        assert!(pool.free(a));
        let c = pool.alloc(session()).expect("reused after free");
        assert_eq!(c.slot(), a.slot());
        assert_eq!(c.generation(), 2);
        assert!(pool.get(b).is_ok());
    }
}
