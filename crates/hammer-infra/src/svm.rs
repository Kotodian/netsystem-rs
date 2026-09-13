//! Shared virtual-memory subsystem.
//!
//! This module tree is the Rust counterpart of VPP's `src/svm`: mapping owners,
//! regions, shared queues, and the byte FIFO with its storage owner. Generic
//! SSVM and FIFO segments use offsets where their ABI requires them; SVM
//! regions use fixed-VA pointers and shared `MemHeap` instances.
//!
//! Layout and ownership are specified in ADR-0011 sections 12.1-12.9.

pub mod fifo;
pub mod fifo_segment;
pub mod msg_queue;
pub mod queue;
pub mod region;
pub mod ssvm;
