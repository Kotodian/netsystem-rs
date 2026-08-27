//! Session FIFO payload callbacks used by the vendored QUIC engine.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::BytesMut;
use hammer_infra::fifo::Fifo;
use quinn_proto::{StreamDataError, StreamDataIo, StreamId};

pub(super) struct StreamIoEntry {
    pub(super) context: u32,
    pub(super) session: u32,
    pub(super) rx_fifo: Arc<Fifo>,
    pub(super) tx_fifo: Arc<Fifo>,
    pub(super) bytes_written: u64,
    pub(super) app_rx_data_len: u64,
    pub(super) app_tx_data_len: u64,
    pending_rx: u64,
    pending_tx_deq: u64,
    dirty: bool,
}

#[derive(Debug)]
pub(super) struct StreamIoEvent {
    pub(super) context: u32,
    pub(super) session: u32,
    pub(super) rx: u64,
    pub(super) tx_deq: u64,
    pub(super) bytes_written: u64,
}

pub(super) struct StreamIoTable {
    streams: HashMap<StreamId, StreamIoEntry>,
    dirty: Vec<StreamId>,
}

impl StreamIoTable {
    pub(super) fn new() -> Box<Self> {
        Box::new(Self {
            streams: HashMap::new(),
            dirty: Vec::new(),
        })
    }

    pub(super) fn install_stream(
        &mut self,
        stream: StreamId,
        context: u32,
        session: u32,
        rx_fifo: Arc<Fifo>,
        tx_fifo: Arc<Fifo>,
        bytes_written: u64,
        app_tx_data_len: u64,
    ) {
        if let Some(previous) = self.streams.get(&stream) {
            assert_eq!(
                (previous.context, previous.session),
                (context, session),
                "duplicate STREAM delivery must reuse the exact existing Stream Session"
            );
            return;
        }
        let previous = self.streams.insert(
            stream,
            StreamIoEntry {
                context,
                session,
                rx_fifo,
                tx_fifo,
                bytes_written,
                app_rx_data_len: 0,
                app_tx_data_len,
                pending_rx: 0,
                pending_tx_deq: 0,
                dirty: false,
            },
        );
        assert!(previous.is_none(), "stream Session installed exactly once");
    }

    pub(super) fn stream_session(&self, stream: StreamId) -> Option<u32> {
        self.streams.get(&stream).map(|entry| entry.session)
    }

    pub(super) fn stream_context(&self, stream: StreamId) -> Option<u32> {
        self.streams.get(&stream).map(|entry| entry.context)
    }

    pub(super) fn remove_stream(&mut self, stream: StreamId) -> Option<StreamIoEntry> {
        self.streams.remove(&stream)
    }

    pub(super) fn transmit(
        &mut self,
        stream: StreamId,
        offsets: Range<u64>,
        output: &mut BytesMut,
    ) -> Result<usize, StreamDataError> {
        let entry = self
            .streams
            .get_mut(&stream)
            .ok_or(StreamDataError::StreamMissing { stream })?;
        let len = usize::try_from(offsets.end - offsets.start).map_err(|_| {
            StreamDataError::TxRangeUnavailable {
                stream,
                offset: offsets.start,
                len: offsets.end - offsets.start,
            }
        })?;
        let fifo_offset = usize::try_from(offsets.start.saturating_sub(entry.bytes_written))
            .unwrap_or(usize::MAX);
        let available = entry.tx_fifo.max_dequeue();
        if len == 0 {
            return Ok(0);
        }
        if fifo_offset > available || len > available.saturating_sub(fifo_offset) {
            return Err(StreamDataError::TxRangeUnavailable {
                stream,
                offset: offsets.start,
                len: offsets.end - offsets.start,
            });
        }
        let copied = entry
            .tx_fifo
            .peek_segments(fifo_offset, len, |first, second| {
                output.extend_from_slice(first);
                output.extend_from_slice(second);
                Ok::<usize, StreamDataError>(first.len() + second.len())
            })
            .ok_or(StreamDataError::TxRangeUnavailable {
                stream,
                offset: offsets.start,
                len: offsets.end - offsets.start,
            })??;
        if copied != len {
            return Err(StreamDataError::IoRangeInvalid {
                stream,
                offset: offsets.start,
                len: offsets.end - offsets.start,
            });
        }
        entry.app_tx_data_len = entry.app_tx_data_len.max(offsets.end);
        Ok(copied)
    }

    pub(super) fn ack(
        &mut self,
        stream: StreamId,
        contiguous_end: u64,
    ) -> Result<(), StreamDataError> {
        {
            let entry = self
                .streams
                .get_mut(&stream)
                .ok_or(StreamDataError::StreamMissing { stream })?;
            let delta = contiguous_end.checked_sub(entry.bytes_written).ok_or(
                StreamDataError::IoRangeInvalid {
                    stream,
                    offset: contiguous_end,
                    len: 0,
                },
            )?;
            if delta == 0 {
                return Ok(());
            }
            let dropped = entry.tx_fifo.dequeue_drop(delta as usize);
            if dropped != delta as usize {
                return Err(StreamDataError::IoRangeInvalid {
                    stream,
                    offset: contiguous_end - delta,
                    len: delta,
                });
            }
            entry.bytes_written = contiguous_end;
            entry.pending_tx_deq = entry.pending_tx_deq.saturating_add(dropped as u64);
        }
        self.mark_dirty(stream);
        Ok(())
    }

    pub(super) fn receive(
        &mut self,
        stream: StreamId,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, StreamDataError> {
        if data.is_empty() {
            return Ok(0);
        }
        let delivered = {
            let entry = self
                .streams
                .get_mut(&stream)
                .ok_or(StreamDataError::StreamMissing { stream })?;
            let (offset, data) = if offset < entry.app_rx_data_len {
                let consumed = entry.app_rx_data_len.saturating_sub(offset) as usize;
                if consumed >= data.len() {
                    return Ok(0);
                }
                (entry.app_rx_data_len, &data[consumed..])
            } else {
                (offset, data)
            };
            if offset == entry.app_rx_data_len {
                if data.len() > entry.rx_fifo.max_enqueue() {
                    return Err(StreamDataError::RxCapacityExceeded {
                        stream,
                        offset,
                        len: data.len() as u64,
                    });
                }
                let written = entry.rx_fifo.enqueue(data);
                if written < data.len() {
                    return Err(StreamDataError::RxCapacityExceeded {
                        stream,
                        offset,
                        len: data.len() as u64,
                    });
                }
                entry.app_rx_data_len = entry.app_rx_data_len.saturating_add(written as u64);
                entry.pending_rx = entry.pending_rx.saturating_add(written as u64);
                written as u64
            } else {
                let relative = offset
                    .checked_sub(entry.app_rx_data_len)
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or(StreamDataError::IoRangeInvalid {
                        stream,
                        offset,
                        len: data.len() as u64,
                    })?;
                let result = entry.rx_fifo.enqueue_ooo(relative, data).map_err(|_| {
                    StreamDataError::RxCapacityExceeded {
                        stream,
                        offset,
                        len: data.len() as u64,
                    }
                })?;
                entry.app_rx_data_len = entry
                    .app_rx_data_len
                    .saturating_add(result.delivered as u64);
                entry.pending_rx = entry.pending_rx.saturating_add(result.delivered as u64);
                result.delivered as u64
            }
        };
        if delivered != 0 {
            self.mark_dirty(stream);
        }
        Ok(delivered)
    }

    pub(super) fn take_events(&mut self, events: &mut Vec<StreamIoEvent>) {
        events.clear();
        for stream in self.dirty.drain(..) {
            let Some(entry) = self.streams.get_mut(&stream) else {
                continue;
            };
            entry.dirty = false;
            let rx = entry.pending_rx;
            let tx_deq = entry.pending_tx_deq;
            entry.pending_rx = 0;
            entry.pending_tx_deq = 0;
            if rx == 0 && tx_deq == 0 {
                continue;
            }
            events.push(StreamIoEvent {
                context: entry.context,
                session: entry.session,
                rx,
                tx_deq,
                bytes_written: entry.bytes_written,
            });
        }
    }

    fn mark_dirty(&mut self, stream: StreamId) {
        let Some(entry) = self.streams.get_mut(&stream) else {
            return;
        };
        if !entry.dirty {
            entry.dirty = true;
            self.dirty.push(stream);
        }
    }

    pub(super) fn app_rx_consumed(&self, stream: StreamId) -> Option<u64> {
        let entry = self.streams.get(&stream)?;
        entry
            .app_rx_data_len
            .checked_sub(entry.rx_fifo.max_dequeue() as u64)
    }

    pub(super) fn confirm_app_rx_consumed(&mut self, stream: StreamId) {
        if let Some(entry) = self.streams.get_mut(&stream) {
            entry.app_rx_data_len = entry.rx_fifo.max_dequeue() as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hammer_infra::fifo::Fifo;
    use quinn_proto::{Dir, Side, StreamId};

    use super::*;

    fn test_stream() -> StreamId {
        StreamId::new(Side::Server, Dir::Bi, 0)
    }

    fn test_fifos() -> (Arc<Fifo>, Arc<Fifo>) {
        let mut rx = Fifo::with_capacity(1024).expect("rx FIFO");
        rx.enable_ooo();
        let tx = Fifo::with_capacity(1024).expect("tx FIFO");
        (Arc::new(rx), Arc::new(tx))
    }

    #[test]
    fn receive_transmit_and_ack_flow_through_session_fifos() {
        let (rx, tx) = test_fifos();
        let stream = test_stream();
        let mut table = StreamIoTable::new();
        table.install_stream(stream, 7, 9, Arc::clone(&rx), Arc::clone(&tx), 0, 0);

        assert_eq!(table.receive(stream, 0, b"abc").expect("receive"), 3);
        assert_eq!(rx.max_dequeue(), 3);
        let mut events = Vec::new();
        table.take_events(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session, 9);
        assert_eq!(events[0].rx, 3);

        tx.enqueue(b"abc");
        let mut output = BytesMut::new();
        assert_eq!(
            table.transmit(stream, 0..3, &mut output).expect("transmit"),
            3
        );
        assert_eq!(&output[..], b"abc");

        table.ack(stream, 3).expect("ack");
        assert_eq!(tx.max_dequeue(), 0);
        let mut events = Vec::new();
        table.take_events(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tx_deq, 3);
        assert_eq!(events[0].bytes_written, 3);
    }

    #[test]
    fn out_of_order_receive_publishes_only_promoted_bytes() {
        let (rx, _) = test_fifos();
        let stream = test_stream();
        let mut table = StreamIoTable::new();
        table.install_stream(
            stream,
            8,
            10,
            Arc::clone(&rx),
            Arc::new(Fifo::with_capacity(1024).expect("tx FIFO")),
            0,
            0,
        );

        assert_eq!(table.receive(stream, 5, b"world").expect("ooo receive"), 0);
        assert_eq!(rx.max_dequeue(), 0);
        let mut events = Vec::new();
        table.take_events(&mut events);
        assert!(events.is_empty());

        assert_eq!(table.receive(stream, 0, b"hello").expect("head"), 10);
        assert_eq!(rx.max_dequeue(), 10);
        table.take_events(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rx, 10);
    }

    #[test]
    fn duplicate_stream_delivery_reuses_the_existing_session() {
        let (rx, tx) = test_fifos();
        let stream = test_stream();
        let mut table = StreamIoTable::new();
        table.install_stream(stream, 7, 9, Arc::clone(&rx), Arc::clone(&tx), 0, 0);
        table.install_stream(stream, 7, 9, Arc::clone(&rx), Arc::clone(&tx), 0, 0);

        assert_eq!(table.stream_session(stream), Some(9));
        assert_eq!(table.stream_context(stream), Some(7));
        assert_eq!(table.receive(stream, 0, b"abc").expect("receive"), 3);
        assert_eq!(rx.max_dequeue(), 3);
    }

    #[test]
    fn full_fifo_rejects_whole_contiguous_frame_without_publishing_prefix() {
        let mut rx = Arc::new(Fifo::with_capacity(4).expect("small RX FIFO"));
        Arc::get_mut(&mut rx)
            .expect("small RX FIFO is unshared before install")
            .enable_ooo();
        let tx = Fifo::with_capacity(1024).expect("tx FIFO");
        let stream = test_stream();
        let mut table = StreamIoTable::new();
        table.install_stream(stream, 7, 9, Arc::clone(&rx), Arc::new(tx), 0, 0);

        let error = table
            .receive(stream, 0, b"abcdef")
            .expect_err("reject full frame");
        assert!(matches!(
            error,
            StreamDataError::RxCapacityExceeded {
                stream: failed,
                offset: 0,
                len: 6,
            } if failed == stream
        ));
        assert_eq!(rx.max_dequeue(), 0);
        assert_eq!(table.app_rx_consumed(stream), Some(0));
    }

    #[test]
    fn app_rx_consumed_is_exact_fifo_dequeue_delta_not_free_capacity() {
        let (rx, tx) = test_fifos();
        let stream = test_stream();
        let mut table = StreamIoTable::new();
        table.install_stream(stream, 7, 9, Arc::clone(&rx), Arc::clone(&tx), 0, 0);

        assert_eq!(table.receive(stream, 0, b"abc").expect("receive"), 3);
        assert_eq!(rx.dequeue_drop(1), 1);
        assert_eq!(table.app_rx_consumed(stream), Some(1));
        table.confirm_app_rx_consumed(stream);

        assert_eq!(table.receive(stream, 0, b"abc").expect("overlap"), 1);
        assert_eq!(rx.max_dequeue(), 3);
        assert_eq!(table.app_rx_consumed(stream), Some(0));
    }
}

pub(super) unsafe fn transmit_callback(
    user_data: usize,
    stream: StreamId,
    offsets: Range<u64>,
    output: &mut BytesMut,
) -> Result<usize, StreamDataError> {
    // SAFETY: `user_data` points to the StreamIoTable owned by the enclosing
    // EngineConnection. The table is not borrowed through any other live
    // reference while Quinn invokes these synchronous callbacks.
    unsafe { (&mut *(user_data as *mut StreamIoTable)).transmit(stream, offsets, output) }
}

pub(super) unsafe fn ack_callback(
    user_data: usize,
    stream: StreamId,
    contiguous_end: u64,
) -> Result<(), StreamDataError> {
    // SAFETY: see `transmit_callback`.
    unsafe { (&mut *(user_data as *mut StreamIoTable)).ack(stream, contiguous_end) }
}

pub(super) unsafe fn receive_callback(
    user_data: usize,
    stream: StreamId,
    offset: u64,
    data: &[u8],
) -> Result<(), StreamDataError> {
    // SAFETY: see `transmit_callback`.
    unsafe {
        (&mut *(user_data as *mut StreamIoTable))
            .receive(stream, offset, data)
            .map(|_| ())
    }
}

impl StreamIoTable {
    pub(super) fn io(self: &Box<Self>) -> StreamDataIo {
        let user_data = self.as_ref() as *const Self as usize;
        StreamDataIo {
            user_data,
            transmit: transmit_callback,
            ack: ack_callback,
            receive: receive_callback,
        }
    }
}
