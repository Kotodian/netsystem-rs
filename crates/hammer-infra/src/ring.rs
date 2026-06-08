use std::fmt;

use crate::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubmissionDescriptor<Opcode, UserData, Object, Payload> {
    opcode: Opcode,
    user_data: UserData,
    object: Object,
    payload: Payload,
}

impl<Opcode, UserData, Object, Payload> SubmissionDescriptor<Opcode, UserData, Object, Payload> {
    #[inline]
    pub const fn new(
        opcode: Opcode,
        user_data: UserData,
        object: Object,
        payload: Payload,
    ) -> Self {
        Self {
            opcode,
            user_data,
            object,
            payload,
        }
    }

    #[inline]
    pub fn opcode(&self) -> Opcode
    where
        Opcode: Copy,
    {
        self.opcode
    }

    #[inline]
    pub fn user_data(&self) -> UserData
    where
        UserData: Copy,
    {
        self.user_data
    }

    #[inline]
    pub fn object(&self) -> Object
    where
        Object: Copy,
    {
        self.object
    }

    #[inline]
    pub fn payload(&self) -> Payload
    where
        Payload: Copy,
    {
        self.payload
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionDescriptor<UserData, ResultCode, Flags, Object, Payload> {
    user_data: UserData,
    result: ResultCode,
    flags: Flags,
    object: Object,
    payload: Payload,
}

impl<UserData, ResultCode, Flags, Object, Payload>
    CompletionDescriptor<UserData, ResultCode, Flags, Object, Payload>
{
    #[inline]
    pub const fn new(
        user_data: UserData,
        result: ResultCode,
        flags: Flags,
        object: Object,
        payload: Payload,
    ) -> Self {
        Self {
            user_data,
            result,
            flags,
            object,
            payload,
        }
    }

    #[inline]
    pub fn user_data(&self) -> UserData
    where
        UserData: Copy,
    {
        self.user_data
    }

    #[inline]
    pub fn result(&self) -> ResultCode
    where
        ResultCode: Copy,
    {
        self.result
    }

    #[inline]
    pub fn flags(&self) -> Flags
    where
        Flags: Copy,
    {
        self.flags
    }

    #[inline]
    pub fn object(&self) -> Object
    where
        Object: Copy,
    {
        self.object
    }

    #[inline]
    pub fn payload(&self) -> Payload
    where
        Payload: Copy,
    {
        self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RingEntry<Descriptor, Attachment> {
    descriptor: Descriptor,
    attachment: Option<Attachment>,
}

impl<Descriptor, Attachment> RingEntry<Descriptor, Attachment> {
    #[inline]
    pub const fn new(descriptor: Descriptor) -> Self {
        Self {
            descriptor,
            attachment: None,
        }
    }

    #[inline]
    pub fn with_attachment(descriptor: Descriptor, attachment: Attachment) -> Self {
        Self {
            descriptor,
            attachment: Some(attachment),
        }
    }

    #[inline]
    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    #[inline]
    pub fn attachment(&self) -> Option<&Attachment> {
        self.attachment.as_ref()
    }

    #[inline]
    pub fn into_parts(self) -> (Descriptor, Option<Attachment>) {
        (self.descriptor, self.attachment)
    }
}

pub struct LocalRing<T> {
    slots: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> LocalRing<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            head: 0,
            len: 0,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(None);
        }
        Self {
            slots,
            head: 0,
            len: 0,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    #[inline]
    pub fn front(&self) -> Option<&T> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head].as_ref()
    }

    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head].as_mut()
    }

    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.is_full() {
            return Err(value);
        }
        let slot = if self.capacity() == 0 {
            0
        } else {
            (self.head + self.len) % self.capacity()
        };
        self.slots[slot] = Some(value);
        self.len += 1;
        Ok(())
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        let value = self.slots[self.head].take();
        debug_assert!(
            value.is_some(),
            "occupied slot expected while ring is non-empty"
        );

        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        } else {
            self.head = (self.head + 1) % self.capacity();
        }

        value
    }

    #[inline]
    pub fn clear(&mut self) {
        if self.len == 0 {
            return;
        }

        for offset in 0..self.len {
            let slot = (self.head + offset) % self.capacity();
            let _ = self.slots[slot].take();
        }
        self.head = 0;
        self.len = 0;
    }
}

impl<T> Default for LocalRing<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for LocalRing<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalRing")
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RingSlotId(u32);

impl RingSlotId {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }

    #[inline]
    const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

pub struct IndexedRing<T> {
    ready: LocalRing<RingSlotId>,
    slots: Vec<Option<T>>,
    free: Vec<RingSlotId>,
}

impl<T> IndexedRing<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ready: LocalRing::new(),
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            u32::try_from(capacity).is_ok(),
            "indexed ring capacity exceeds u32 slots"
        );
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for index in 0..capacity {
            slots.push(None);
            let slot = RingSlotId::new((capacity - index - 1) as u32);
            free.push(slot);
        }
        Self {
            ready: LocalRing::with_capacity(capacity),
            slots,
            free,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.free.is_empty()
    }

    #[inline]
    pub fn entry(&self, slot: RingSlotId) -> Option<&T> {
        self.slots.get(slot.as_usize()).and_then(Option::as_ref)
    }

    #[inline]
    pub fn entry_mut(&mut self, slot: RingSlotId) -> Option<&mut T> {
        self.slots.get_mut(slot.as_usize()).and_then(Option::as_mut)
    }

    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<RingSlotId, T> {
        let Some(slot) = self.free.pop() else {
            return Err(value);
        };
        self.slots[slot.as_usize()] = Some(value);
        if self.ready.try_push(slot).is_err() {
            let value = self.slots[slot.as_usize()]
                .take()
                .expect("slot was just populated");
            self.free.push(slot);
            return Err(value);
        }
        Ok(slot)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<(RingSlotId, T)> {
        let slot = self.ready.pop()?;
        let value = self.slots[slot.as_usize()]
            .take()
            .expect("ready slot must have an entry");
        self.free.push(slot);
        Some((slot, value))
    }

    #[inline]
    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T> Default for IndexedRing<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for IndexedRing<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedRing")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}
