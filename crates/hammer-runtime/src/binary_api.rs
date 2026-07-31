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
}

impl BinaryApiMethodEntry {
    #[doc(hidden)]
    pub const fn new(name: &'static str, call: BinaryApiMethodFn) -> Self {
        Self { name, call }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline]
    pub fn call(self, request: &[u8]) -> BinaryApiMethodReply {
        (self.call)(RSlice::from_slice(request))
    }
}
