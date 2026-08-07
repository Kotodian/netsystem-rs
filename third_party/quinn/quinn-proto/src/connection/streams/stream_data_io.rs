//! Payload I/O callbacks for a Session FIFO-backed QUIC driver.
//!
//! The vendored Quinn state machine normally owns stream payload in
//! `SendBuffer` and the receive `Assembler`. Hammer instead keeps stream
//! payload in Session FIFOs. This optional callback table lets a concrete
//! driver provide the bytes directly at packetization time, release the
//! contiguous acknowledged TX prefix, and deliver decrypted RX data to the
//! owning Session FIFO.

use std::ops::Range;

use thiserror::Error;

use crate::StreamId;

/// Failure reported by a Session FIFO callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum StreamDataError {
    /// The target stream or Session FIFO no longer exists.
    #[error("QUIC stream {stream:?} Session FIFO is missing")]
    StreamMissing {
        /// QUIC stream identity.
        stream: StreamId,
    },
    /// The RX FIFO cannot retain the delivered byte range.
    #[error("QUIC stream {stream:?} Session RX FIFO cannot retain offset {offset} length {len}")]
    RxCapacityExceeded {
        /// QUIC stream identity.
        stream: StreamId,
        /// First stream offset that could not be delivered.
        offset: u64,
        /// Number of bytes that could not be delivered.
        len: u64,
    },
    /// The TX FIFO cannot produce the selected packet range.
    #[error("QUIC stream {stream:?} Session TX FIFO lacks offset {offset} length {len}")]
    TxRangeUnavailable {
        /// QUIC stream identity.
        stream: StreamId,
        /// First requested stream offset.
        offset: u64,
        /// Requested packet length.
        len: u64,
    },
    /// The callback returned an unexpected range or write count.
    #[error(
        "QUIC stream {stream:?} Session FIFO returned invalid range offset {offset} length {len}"
    )]
    IoRangeInvalid {
        /// QUIC stream identity.
        stream: StreamId,
        /// First requested stream offset.
        offset: u64,
        /// Requested packet length.
        len: u64,
    },
    /// QUIC frame validation failed for a STREAM frame.
    #[error(
        "QUIC stream {stream:?} frame at offset {offset} length {len} violates protocol state"
    )]
    ProtocolViolation {
        /// QUIC stream identity.
        stream: StreamId,
        /// Frame offset.
        offset: u64,
        /// Frame payload length.
        len: u64,
    },
    /// The peer exceeded receive flow-control credit.
    #[error(
        "QUIC stream {stream:?} frame at offset {offset} length {len} exceeds flow-control credit"
    )]
    FlowControlViolation {
        /// QUIC stream identity.
        stream: StreamId,
        /// Frame offset.
        offset: u64,
        /// Frame payload length.
        len: u64,
    },
}

/// Concrete function-pointer I/O table used by one QUIC connection.
///
/// `user_data` is an opaque pointer supplied by the owning Data Worker. The
/// callbacks are synchronous and must not outlive the connection or retain
/// any reference to the user data.
#[derive(Debug, Clone, Copy)]
pub struct StreamDataIo {
    /// Opaque owner pointer passed to every callback.
    pub user_data: usize,
    /// Copy stream bytes at `range` into `output`.
    pub transmit:
        unsafe fn(usize, StreamId, Range<u64>, &mut Vec<u8>) -> Result<usize, StreamDataError>,
    /// Release a newly contiguous acknowledged TX prefix ending at `offset`.
    pub ack: unsafe fn(usize, StreamId, u64) -> Result<(), StreamDataError>,
    /// Deliver one decrypted STREAM payload to the Session FIFO.
    pub receive: unsafe fn(usize, StreamId, u64, &[u8]) -> Result<(), StreamDataError>,
}

impl StreamDataIo {
    #[inline]
    pub(crate) unsafe fn transmit(
        self,
        id: StreamId,
        offsets: Range<u64>,
        output: &mut Vec<u8>,
    ) -> Result<usize, StreamDataError> {
        unsafe { (self.transmit)(self.user_data, id, offsets, output) }
    }

    #[inline]
    pub(crate) unsafe fn ack(
        self,
        id: StreamId,
        contiguous_end: u64,
    ) -> Result<(), StreamDataError> {
        unsafe { (self.ack)(self.user_data, id, contiguous_end) }
    }

    #[inline]
    pub(crate) unsafe fn receive(
        self,
        id: StreamId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StreamDataError> {
        unsafe { (self.receive)(self.user_data, id, offset, data) }
    }
}

pub(crate) fn append_io_bytes(
    output: &mut Vec<u8>,
    io: StreamDataIo,
    id: StreamId,
    mut offsets: Range<u64>,
) -> Result<(), StreamDataError> {
    while offsets.start != offsets.end {
        let written = unsafe { io.transmit(id, offsets.clone(), output)? };
        if written == 0 || written as u64 > offsets.end - offsets.start {
            return Err(StreamDataError::IoRangeInvalid {
                stream: id,
                offset: offsets.start,
                len: offsets.end - offsets.start,
            });
        }
        offsets.start += written as u64;
    }
    Ok(())
}
