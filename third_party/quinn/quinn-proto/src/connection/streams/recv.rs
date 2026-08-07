use std::collections::hash_map::Entry;
use std::mem;

use bytes::Bytes;
use thiserror::Error;
use tracing::debug;

use super::state::get_or_insert_recv;
use super::{
    ClosedStream, Retransmits, ShouldTransmit, StreamDataError, StreamDataIo, StreamId,
    StreamsState,
};
use crate::connection::assembler::{Assembler, Chunk, IllegalOrderedRead};
use crate::connection::streams::state::StreamRecv;
use crate::{frame, TransportError, VarInt};

#[derive(Debug, Default)]
pub(super) struct Recv {
    // NB: when adding or removing fields, remember to update `reinit`.
    state: RecvState,
    pub(super) assembler: Option<Assembler>,
    sent_max_stream_data: u64,
    pub(super) end: u64,
    pub(super) stopped: bool,
    bytes_read: u64,
    pub(super) io: Option<StreamDataIo>,
}

impl Recv {
    pub(super) fn new(initial_max_data: u64, io: Option<StreamDataIo>) -> Box<Self> {
        Box::new(Self {
            state: RecvState::default(),
            assembler: io.is_none().then(Assembler::new),
            sent_max_stream_data: initial_max_data,
            end: 0,
            stopped: false,
            bytes_read: 0,
            io,
        })
    }

    /// Reset to the initial state
    pub(super) fn reinit(&mut self, initial_max_data: u64) {
        let io = self.io;
        self.state = RecvState::default();
        if let Some(assembler) = self.assembler.as_mut() {
            assembler.reinit();
        }
        self.sent_max_stream_data = initial_max_data;
        self.end = 0;
        self.stopped = false;
        self.bytes_read = 0;
        self.io = io;
    }

    /// Process a STREAM frame
    ///
    /// Return value is `(number_of_new_bytes_ingested, stream_is_closed)`
    pub(super) fn ingest(
        &mut self,
        frame: frame::Stream<'_>,
        payload_len: usize,
        received: u64,
        max_data: u64,
    ) -> Result<(u64, bool), StreamDataError> {
        let end = frame.offset + frame.data.len() as u64;
        if end >= 2u64.pow(62) {
            return Err(StreamDataError::FlowControlViolation {
                stream: frame.id,
                offset: frame.offset,
                len: frame.data.len() as u64,
            });
        }

        if let Some(final_offset) = self.final_offset() {
            if end > final_offset || (frame.fin && end != final_offset) {
                debug!(end, final_offset, "final size error");
                return Err(StreamDataError::ProtocolViolation {
                    stream: frame.id,
                    offset: frame.offset,
                    len: frame.data.len() as u64,
                });
            }
        }

        let new_bytes = self
            .credit_consumed_by(end, received, max_data)
            .map_err(|_| StreamDataError::FlowControlViolation {
                stream: frame.id,
                offset: frame.offset,
                len: frame.data.len() as u64,
            })?;

        // Deliver directly to the Session FIFO before advancing Quinn receive
        // state. A callback error leaves Quinn ranges and flow-control state
        // unchanged, matching VPP's "drop, fifo full" path.
        if !self.stopped {
            if let Some(io) = self.io {
                unsafe { io.receive(frame.id, frame.offset, frame.data)? };
            } else {
                self.assembler
                    .as_mut()
                    .expect("in-memory receive assembler")
                    .insert(
                        frame.offset,
                        Bytes::copy_from_slice(frame.data),
                        payload_len,
                    );
            }
        }

        // Stopped streams don't need to wait for the actual data, they just need to know
        // how much there was.
        if frame.fin && !self.stopped {
            if let RecvState::Recv { ref mut size } = self.state {
                *size = Some(end);
            }
        }

        self.end = self.end.max(end);
        Ok((new_bytes, frame.fin && self.stopped))
    }

    #[inline]
    pub(super) fn bytes_read(&self) -> u64 {
        if self.io.is_some() {
            self.bytes_read
        } else {
            self.assembler
                .as_ref()
                .expect("in-memory receive assembler")
                .bytes_read()
        }
    }

    #[inline]
    pub(super) fn set_bytes_read(&mut self, bytes_read: u64) {
        assert!(
            bytes_read <= self.end,
            "QUIC stream consumed offset {} exceeds receive end {}",
            bytes_read,
            self.end
        );
        self.bytes_read = self.bytes_read.max(bytes_read);
    }

    pub(super) fn stop(&mut self) -> Result<(u64, ShouldTransmit), ClosedStream> {
        if self.stopped {
            return Err(ClosedStream { _private: () });
        }

        self.stopped = true;
        if let Some(assembler) = self.assembler.as_mut() {
            assembler.clear();
        }
        // Issue flow control credit for unread data
        let read_credits = self.end - self.bytes_read();
        // This may send a spurious STOP_SENDING if we've already received all data, but it's a bit
        // fiddly to distinguish that from the case where we've received a FIN but are missing some
        // data that the peer might still be trying to retransmit, in which case a STOP_SENDING is
        // still useful.
        Ok((read_credits, ShouldTransmit(self.is_receiving())))
    }

    /// Returns the window that should be advertised in a `MAX_STREAM_DATA` frame
    ///
    /// The method returns a tuple which consists of the window that should be
    /// announced, as well as a boolean parameter which indicates if a new
    /// transmission of the value is recommended. If the boolean value is
    /// `false` the new window should only be transmitted if a previous transmission
    /// had failed.
    pub(super) fn max_stream_data(&mut self, stream_receive_window: u64) -> (u64, ShouldTransmit) {
        let max_stream_data = self.bytes_read() + stream_receive_window;

        // Only announce a window update if it's significant enough
        // to make it worthwhile sending a MAX_STREAM_DATA frame.
        // We use here a fraction of the configured stream receive window to make
        // the decision, and accommodate for streams using bigger windows requiring
        // less updates. A fixed size would also work - but it would need to be
        // smaller than `stream_receive_window` in order to make sure the stream
        // does not get stuck.
        let diff = max_stream_data - self.sent_max_stream_data;
        let transmit = self.can_send_flow_control() && diff >= (stream_receive_window / 8);
        (max_stream_data, ShouldTransmit(transmit))
    }

    /// Records that a `MAX_STREAM_DATA` announcing a certain window was sent
    ///
    /// This will suppress enqueuing further `MAX_STREAM_DATA` frames unless
    /// either the previous transmission was not acknowledged or the window
    /// further increased.
    pub(super) fn record_sent_max_stream_data(&mut self, sent_value: u64) {
        if sent_value > self.sent_max_stream_data {
            self.sent_max_stream_data = sent_value;
        }
    }

    /// Whether the total amount of data that the peer will send on this stream is unknown
    ///
    /// True until we've received either a reset or the final frame.
    ///
    /// Implies that the sender might benefit from stream-level flow control updates, and we might
    /// need to issue connection-level flow control updates due to flow control budget use by this
    /// stream in the future, even if it's been stopped.
    pub(super) fn final_offset_unknown(&self) -> bool {
        matches!(self.state, RecvState::Recv { size: None })
    }

    /// Whether stream-level flow control updates should be sent for this stream
    pub(super) fn can_send_flow_control(&self) -> bool {
        // Stream-level flow control is redundant if the sender has already sent the whole stream,
        // and moot if we no longer want data on this stream.
        self.final_offset_unknown() && !self.stopped
    }

    /// Whether data is still being accepted from the peer
    pub(super) fn is_receiving(&self) -> bool {
        matches!(self.state, RecvState::Recv { .. })
    }

    fn final_offset(&self) -> Option<u64> {
        match self.state {
            RecvState::Recv { size } => size,
            RecvState::ResetRecvd { size, .. } => Some(size),
        }
    }

    /// Returns `false` iff the reset was redundant
    pub(super) fn reset(
        &mut self,
        error_code: VarInt,
        final_offset: VarInt,
        received: u64,
        max_data: u64,
    ) -> Result<bool, TransportError> {
        // Validate final_offset
        if let Some(offset) = self.final_offset() {
            if offset != final_offset.into_inner() {
                return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
            }
        } else if self.end > u64::from(final_offset) {
            return Err(TransportError::FINAL_SIZE_ERROR(
                "lower than high water mark",
            ));
        }
        self.credit_consumed_by(final_offset.into(), received, max_data)?;

        if matches!(self.state, RecvState::ResetRecvd { .. }) {
            return Ok(false);
        }
        self.state = RecvState::ResetRecvd {
            size: final_offset.into(),
            error_code,
        };
        // Nuke buffers so that future reads fail immediately, which ensures future reads don't
        // issue flow control credit redundant to that already issued. We could instead special-case
        // reset streams during read, but it's unclear if there's any benefit to retaining data for
        // reset streams.
        if let Some(assembler) = self.assembler.as_mut() {
            assembler.clear();
        }
        Ok(true)
    }

    pub(super) fn reset_code(&self) -> Option<VarInt> {
        match self.state {
            RecvState::ResetRecvd { error_code, .. } => Some(error_code),
            _ => None,
        }
    }

    /// Compute the amount of flow control credit consumed, or return an error if more was consumed
    /// than issued
    fn credit_consumed_by(
        &self,
        offset: u64,
        received: u64,
        max_data: u64,
    ) -> Result<u64, TransportError> {
        let prev_end = self.end;
        let new_bytes = offset.saturating_sub(prev_end);
        if offset > self.sent_max_stream_data || received + new_bytes > max_data {
            debug!(
                received,
                new_bytes,
                max_data,
                offset,
                stream_max_data = self.sent_max_stream_data,
                "flow control error"
            );
            return Err(TransportError::FLOW_CONTROL_ERROR(""));
        }

        Ok(new_bytes)
    }
}

/// Chunks returned from [`RecvStream::read()`][crate::RecvStream::read].
///
/// ### Note: Finalization Needed
/// Bytes read from the stream are not released from the congestion window until
/// either [`Self::finalize()`] is called, or this type is dropped.
///
/// It is recommended that you call [`Self::finalize()`] because it returns a flag
/// telling you whether reading from the stream has resulted in the need to transmit a packet.
///
/// If this type is leaked, the stream will remain blocked on the remote peer until
/// another read from the stream is done.
pub struct Chunks<'a> {
    id: StreamId,
    ordered: bool,
    streams: &'a mut StreamsState,
    pending: &'a mut Retransmits,
    state: ChunksState,
    read: u64,
}

impl<'a> Chunks<'a> {
    pub(super) fn new(
        id: StreamId,
        ordered: bool,
        streams: &'a mut StreamsState,
        pending: &'a mut Retransmits,
    ) -> Result<Self, ReadableError> {
        let mut entry = match streams.recv.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Err(ReadableError::ClosedStream),
        };

        let mut recv = match get_or_insert_recv(
            streams.stream_receive_window,
            streams.stream_data_io,
        )(entry.get_mut())
        .stopped
        {
            true => return Err(ReadableError::ClosedStream),
            false => entry.remove().unwrap().into_inner(), // this can't fail due to the previous get_or_insert_with
        };

        recv.assembler
            .as_mut()
            .ok_or(ReadableError::IllegalOrderedRead)?
            .ensure_ordering(ordered)?;
        Ok(Self {
            id,
            ordered,
            streams,
            pending,
            state: ChunksState::Readable(recv),
            read: 0,
        })
    }

    /// Next
    ///
    /// Should call finalize() when done calling this.
    pub fn next(&mut self, max_length: usize) -> Result<Option<Chunk>, ReadError> {
        let rs = match self.state {
            ChunksState::Readable(ref mut rs) => rs,
            ChunksState::Reset(error_code) => {
                return Err(ReadError::Reset(error_code));
            }
            ChunksState::Finished => {
                return Ok(None);
            }
            ChunksState::Finalized => panic!("must not call next() after finalize()"),
        };

        if let Some(assembler) = rs.assembler.as_mut() {
            if let Some(chunk) = assembler.read(max_length, self.ordered) {
                self.read += chunk.bytes.len() as u64;
                return Ok(Some(chunk));
            }
        }

        match rs.state {
            RecvState::ResetRecvd { error_code, .. } => {
                debug_assert_eq!(self.read, 0, "reset streams have empty buffers");
                let state = mem::replace(&mut self.state, ChunksState::Reset(error_code));
                // At this point if we have `rs` self.state must be `ChunksState::Readable`
                let recv = match state {
                    ChunksState::Readable(recv) => StreamRecv::Open(recv),
                    _ => unreachable!("state must be ChunkState::Readable"),
                };
                self.streams.stream_recv_freed(self.id, recv);
                Err(ReadError::Reset(error_code))
            }
            RecvState::Recv { size } => {
                if size == Some(rs.end) && rs.bytes_read() == rs.end {
                    let state = mem::replace(&mut self.state, ChunksState::Finished);
                    // At this point if we have `rs` self.state must be `ChunksState::Readable`
                    let recv = match state {
                        ChunksState::Readable(recv) => StreamRecv::Open(recv),
                        _ => unreachable!("state must be ChunkState::Readable"),
                    };
                    self.streams.stream_recv_freed(self.id, recv);
                    Ok(None)
                } else {
                    // We don't need a distinct `ChunksState` variant for a blocked stream because
                    // retrying a read harmlessly re-traces our steps back to returning
                    // `Err(Blocked)` again. The buffers can't refill and the stream's own state
                    // can't change so long as this `Chunks` exists.
                    Err(ReadError::Blocked)
                }
            }
        }
    }

    /// Mark the read data as consumed from the stream.
    ///
    /// The number of read bytes will be released from the congestion window,
    /// allowing the remote peer to send more data if it was previously blocked.
    ///
    /// If [`ShouldTransmit::should_transmit()`] returns `true`,
    /// a packet needs to be sent to the peer informing them that the stream is unblocked.
    /// This means that you should call [`Connection::poll_transmit()`][crate::Connection::poll_transmit]
    /// and send the returned packet as soon as is reasonable, to unblock the remote peer.
    pub fn finalize(mut self) -> ShouldTransmit {
        self.finalize_inner()
    }

    fn finalize_inner(&mut self) -> ShouldTransmit {
        let state = mem::replace(&mut self.state, ChunksState::Finalized);
        if let ChunksState::Finalized = state {
            // Noop on repeated calls
            return ShouldTransmit(false);
        }

        // We issue additional stream ID credit after the application is notified that a previously
        // open stream has finished or been reset and we've therefore disposed of its state, as
        // recorded by `stream_freed` calls in `next`.
        let mut should_transmit = self.streams.queue_max_stream_id(self.pending);

        // If the stream hasn't finished, we may need to issue stream-level flow control credit
        if let ChunksState::Readable(mut rs) = state {
            let (_, max_stream_data) = rs.max_stream_data(self.streams.stream_receive_window);
            should_transmit |= max_stream_data.0;
            if max_stream_data.0 {
                self.pending.max_stream_data.insert(self.id);
            }
            // Return the stream to storage for future use
            self.streams
                .recv
                .insert(self.id, Some(StreamRecv::Open(rs)));
        }

        // Issue connection-level flow control credit for any data we read regardless of state
        let max_data = self.streams.add_read_credits(self.read);
        self.pending.max_data |= max_data.0;
        should_transmit |= max_data.0;
        ShouldTransmit(should_transmit)
    }
}

impl Drop for Chunks<'_> {
    fn drop(&mut self) {
        let _ = self.finalize_inner();
    }
}

enum ChunksState {
    Readable(Box<Recv>),
    Reset(VarInt),
    Finished,
    Finalized,
}

/// Errors triggered when reading from a recv stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadError {
    /// No more data is currently available on this stream.
    ///
    /// If more data on this stream is received from the peer, an `Event::StreamReadable` will be
    /// generated for this stream, indicating that retrying the read might succeed.
    #[error("blocked")]
    Blocked,
    /// The peer abandoned transmitting data on this stream.
    ///
    /// Carries an application-defined error code.
    #[error("reset by peer: code {0}")]
    Reset(VarInt),
}

/// Errors triggered when opening a recv stream for reading
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadableError {
    /// The stream has not been opened or was already stopped, finished, or reset
    #[error("closed stream")]
    ClosedStream,
    /// Attempted an ordered read following an unordered read
    ///
    /// Performing an unordered read allows discontinuities to arise in the receive buffer of a
    /// stream which cannot be recovered, making further ordered reads impossible.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
}

impl From<IllegalOrderedRead> for ReadableError {
    fn from(_: IllegalOrderedRead) -> Self {
        Self::IllegalOrderedRead
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RecvState {
    Recv { size: Option<u64> },
    ResetRecvd { size: u64, error_code: VarInt },
}

impl Default for RecvState {
    fn default() -> Self {
        Self::Recv { size: None }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{Dir, Side};

    use super::*;

    unsafe fn unreachable_transmit(
        _: usize,
        _: StreamId,
        _: std::ops::Range<u64>,
        _: &mut BytesMut,
    ) -> Result<usize, StreamDataError> {
        unreachable!("transmit is not used by receive tests")
    }

    unsafe fn unreachable_ack(_: usize, _: StreamId, _: u64) -> Result<(), StreamDataError> {
        unreachable!("ack is not used by receive tests")
    }

    unsafe fn failing_receive(
        _: usize,
        stream: StreamId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StreamDataError> {
        Err(StreamDataError::RxCapacityExceeded {
            stream,
            offset,
            len: data.len() as u64,
        })
    }

    #[test]
    fn reordered_frames_while_stopped() {
        const INITIAL_BYTES: u64 = 3;
        const INITIAL_OFFSET: u64 = 3;
        const RECV_WINDOW: u64 = 8;
        let mut s = Recv::new(RECV_WINDOW, None);
        let mut data_recvd = 0;
        // Receive bytes 3..6
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: INITIAL_OFFSET,
                    fin: false,
                    data: &[0; INITIAL_BYTES as usize],
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, INITIAL_OFFSET + INITIAL_BYTES);
        assert!(!is_closed);

        let (credits, transmit) = s.stop().unwrap();
        assert!(transmit.should_transmit());
        assert_eq!(
            credits,
            INITIAL_OFFSET + INITIAL_BYTES,
            "full connection flow control credit is issued by stop"
        );

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued by stop"
        );

        // Receive byte 7
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: RECV_WINDOW - 1,
                    fin: false,
                    data: &[0; 1],
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, RECV_WINDOW - (INITIAL_OFFSET + INITIAL_BYTES));
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );

        // Receive bytes 0..3
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: 0,
                    fin: false,
                    data: &[0; INITIAL_OFFSET as usize],
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        assert_eq!(
            new_bytes, 0,
            "reordered frames don't issue connection-level flow control for stopped streams"
        );
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );
    }

    #[test]
    fn session_fifo_mode_does_not_own_receive_assembler() {
        const RECV_WINDOW: u64 = 8;
        let io = StreamDataIo {
            user_data: 0,
            transmit: unreachable_transmit,
            ack: unreachable_ack,
            receive: failing_receive,
        };
        let recv = Recv::new(RECV_WINDOW, Some(io));
        assert!(recv.assembler.is_none());
    }

    #[test]
    fn receive_callback_failure_leaves_recv_state_unchanged() {
        const RECV_WINDOW: u64 = 8;
        let io = StreamDataIo {
            user_data: 0,
            transmit: unreachable_transmit,
            ack: unreachable_ack,
            receive: failing_receive,
        };
        let mut recv = Recv::new(RECV_WINDOW, Some(io));
        let stream = StreamId::new(Side::Client, Dir::Uni, 0);
        let result = recv.ingest(
            frame::Stream {
                id: stream,
                offset: 0,
                fin: false,
                data: b"payload",
            },
            "payload".len(),
            0,
            RECV_WINDOW,
        );

        assert!(matches!(
            result,
            Err(StreamDataError::RxCapacityExceeded {
                stream: failed_stream,
                offset: 0,
                len: 7,
            }) if failed_stream == stream
        ));
        assert_eq!(recv.end, 0);
        assert_eq!(recv.bytes_read(), 0);
    }
}
