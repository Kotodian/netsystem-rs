//! Cross-DSO registrations for Main Thread Binary API methods.
//!
//! The protobuf transport and dispatch loop belong to `hammer-service`. This
//! module contains only the immutable registration carried by each plugin
//! image and the ABI-stable result returned by its generated adapter.

use abi_stable::{
    StableAbi,
    std_types::{RSlice, RVec},
};

pub type BinaryApiMethodFn = for<'a> fn(RSlice<'a, u8>) -> BinaryApiMethodReply;

#[derive(Debug, Clone, Copy, PartialEq, Eq, StableAbi)]
#[repr(u8)]
pub enum BinaryApiMethodStatus {
    Ok = 0,
    InvalidRequest = 1,
    Panicked = 2,
}

#[derive(Debug, Clone, StableAbi)]
#[repr(C)]
pub struct BinaryApiMethodReply {
    status: BinaryApiMethodStatus,
    payload: RVec<u8>,
}

impl BinaryApiMethodReply {
    #[doc(hidden)]
    pub fn ok(payload: Vec<u8>) -> Self {
        Self {
            status: BinaryApiMethodStatus::Ok,
            payload: payload.into(),
        }
    }

    #[doc(hidden)]
    pub fn invalid_request() -> Self {
        Self {
            status: BinaryApiMethodStatus::InvalidRequest,
            payload: RVec::new(),
        }
    }

    #[doc(hidden)]
    pub fn panicked() -> Self {
        Self {
            status: BinaryApiMethodStatus::Panicked,
            payload: RVec::new(),
        }
    }

    #[inline]
    pub const fn status(&self) -> BinaryApiMethodStatus {
        self.status
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

#[derive(Clone, Copy)]
pub struct BinaryApiMethodEntry {
    name: &'static str,
    call: BinaryApiMethodFn,
    /// Multi-process-safe flag after VPP's `vl_msg_api_msg_config_t`
    /// `is_mp_safe` bit (api_common.h:122), copied to the registered message
    /// at registration (api_shared.c:754). A set flag dispatches the method
    /// without the worker barrier.
    is_mp_safe: bool,
}

impl BinaryApiMethodEntry {
    /// Legacy constructor: the method runs under the worker barrier
    /// (`is_mp_safe` defaults to false).
    #[doc(hidden)]
    pub const fn new(name: &'static str, call: BinaryApiMethodFn) -> Self {
        Self {
            name,
            call,
            is_mp_safe: false,
        }
    }

    /// Marks the method multi-process safe: dispatch runs it on the serial
    /// Main Thread with no worker barrier, after VPP's `is_mp_safe`
    /// (`msg_handler_internal` takes the barrier only when `!m->is_mp_safe`,
    /// api_shared.c:545, 564).
    #[doc(hidden)]
    pub const fn mp_safe(self) -> Self {
        Self {
            name: self.name,
            call: self.call,
            is_mp_safe: true,
        }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline]
    pub const fn is_mp_safe(self) -> bool {
        self.is_mp_safe
    }

    #[inline]
    pub fn call(self, request: &[u8]) -> BinaryApiMethodReply {
        (self.call)(RSlice::from_slice(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_reply(_request: RSlice<'_, u8>) -> BinaryApiMethodReply {
        BinaryApiMethodReply::ok(Vec::new())
    }

    #[test]
    fn new_defaults_to_not_mp_safe() {
        let entry = BinaryApiMethodEntry::new("test.method", noop_reply);
        assert_eq!(entry.name(), "test.method");
        assert!(!entry.is_mp_safe(), "legacy entries run under the barrier");
    }

    #[test]
    fn mp_safe_marks_the_entry_mp_safe() {
        let entry = BinaryApiMethodEntry::new("test.readonly", noop_reply).mp_safe();
        assert_eq!(entry.name(), "test.readonly");
        assert!(entry.is_mp_safe());
    }

    #[test]
    fn mp_safe_builder_is_const() {
        const ENTRY: BinaryApiMethodEntry =
            BinaryApiMethodEntry::new("test.readonly", noop_reply).mp_safe();
        assert!(ENTRY.is_mp_safe());
    }
}
