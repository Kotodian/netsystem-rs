//! Shared virtual-memory subsystem.
//!
//! This module tree is the Rust counterpart of VPP's `src/svm`: mapping owners,
//! regions and their offset allocator, shared queues, and the byte FIFO with
//! its storage owner. Every shared location is an offset relative to the
//! mapped region; process descriptors, allocator handles, and synchronization
//! objects stay private to the owning process.
//!
//! Layout and ownership are specified in ADR-0011 sections 12.1-12.9.

pub mod fifo;
pub mod fifo_segment;
pub mod msg_queue;
pub mod queue;
pub mod region;
pub mod region_heap;
pub mod segment;
