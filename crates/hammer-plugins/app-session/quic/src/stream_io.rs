//! Session FIFO payload callbacks used by the vendored QUIC engine.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_infra::pool::Index;
use hammer_service::session::SessionId;
use quinn_proto::{StreamDataError, StreamDataIo, StreamId};

pub(super) struct StreamIoEntry {
    pub(super) context: Index,
    pub(super) session: SessionId,
    pub(super) rx_fifo: Arc<Fifo>,
    pub(super) tx_fifo: Arc<Fifo>,
    pub(super) bytes_written: u64,
    pub(super) app_rx_data_len: u64,
    pub(super) app_tx_data_len: u64,
    pending_rx: u64,
    pending_tx_deq: u64,
}

#[derive(Debug)]
pub(super) struct PendingRx {
    pub(super) stream: StreamId,
    pub(super) offset: u64,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct StreamIoEvent {
    pub(super) context: Index,
    pub(super) session: SessionId,
    pub(super) rx: u64,
    pub(super) tx_deq: u64,
    pub(super) bytes_written: u64,
}

pub(super) struct StreamIoTable {
    streams: HashMap<StreamId, StreamIoEntry>,
    pending_rx: Vec<PendingRx>,
}

impl StreamIoTable {
    pub(super) fn new() -> Box<Self> {
        Box::new(Self {
            streams: HashMap::new(),
            pending_rx: Vec::new(),
        })
    }

    pub(super) fn install_stream(
        &mut self,
        stream: StreamId,
        context: Index,
        session: SessionId,
        rx_fifo: Arc<Fifo>,
        tx_fifo: Arc<Fifo>,
        bytes_written: u64,
        app_tx_data_len: u64,
    ) {
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
            },
        );
        debug_assert!(previous.is_none(), "stream Session installed exactly once");
    }

    pub(super) fn stream_session(&self, stream: StreamId) -> Option<SessionId> {
        self.streams.get(&stream).map(|entry| entry.session)
    }

    pub(super) fn stream_context(&self, stream: StreamId) -> Option<Index> {
        self.streams.get(&stream).map(|entry| entry.context)
    }

    pub(super) fn remove_stream(&mut self, stream: StreamId) -> Option<StreamIoEntry> {
        self.streams.remove(&stream)
    }

    pub(super) fn transmit(
        &mut self,
        stream: StreamId,
        offsets: Range<u64>,
        output: &mut Vec<u8>,
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
        let Some(entry) = self.streams.get_mut(&stream) else {
            self.pending_rx.push(PendingRx {
                stream,
                offset,
                bytes: data.to_vec(),
            });
            return Ok(0);
        };
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
            let written = entry.rx_fifo.enqueue(data);
            if written == 0 {
                return Err(StreamDataError::RxCapacityExceeded {
                    stream,
                    offset,
                    len: data.len() as u64,
                });
            }
            entry.app_rx_data_len = entry.app_rx_data_len.saturating_add(written as u64);
            entry.pending_rx = entry.pending_rx.saturating_add(written as u64);
            Ok(written as u64)
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
            Ok(result.delivered as u64)
        }
    }

    pub(super) fn drain_pending(
        &mut self,
        stream: StreamId,
        context: Index,
        session: SessionId,
        rx_fifo: Arc<Fifo>,
        tx_fifo: Arc<Fifo>,
        bytes_written: u64,
        app_tx_data_len: u64,
    ) -> Result<usize, StreamDataError> {
        self.install_stream(
            stream,
            context,
            session,
            rx_fifo,
            tx_fifo,
            bytes_written,
            app_tx_data_len,
        );
        let mut delivered = 0u64;
        let mut index = 0;
        while index < self.pending_rx.len() {
            if self.pending_rx[index].stream != stream {
                index += 1;
                continue;
            }
            let pending = self.pending_rx.remove(index);
            delivered =
                delivered.saturating_add(self.receive(stream, pending.offset, &pending.bytes)?);
        }
        Ok(delivered as usize)
    }

    pub(super) fn take_events(&mut self) -> Vec<StreamIoEvent> {
        let mut events = Vec::new();
        for entry in self.streams.values_mut() {
            if entry.pending_rx == 0 && entry.pending_tx_deq == 0 {
                continue;
            }
            events.push(StreamIoEvent {
                context: entry.context,
                session: entry.session,
                rx: entry.pending_rx,
                tx_deq: entry.pending_tx_deq,
                bytes_written: entry.bytes_written,
            });
            entry.pending_rx = 0;
            entry.pending_tx_deq = 0;
        }
        events
    }

    pub(super) fn app_rx_consumed(&self, stream: StreamId, remaining: usize) -> Option<u64> {
        let entry = self.streams.get(&stream)?;
        entry.app_rx_data_len.checked_sub(remaining as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hammer_infra::fifo::Fifo;
    use hammer_infra::pool::Index;
    use hammer_service::session::SessionId;
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
        table.install_stream(
            stream,
            Index::new(7, 1),
            SessionId::from_raw(9),
            Arc::clone(&rx),
            Arc::clone(&tx),
            0,
            0,
        );

        assert_eq!(table.receive(stream, 0, b"abc").expect("receive"), 3);
        assert_eq!(rx.max_dequeue(), 3);
        let events = table.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session, SessionId::from_raw(9));
        assert_eq!(events[0].rx, 3);

        tx.enqueue(b"abc");
        let mut output = Vec::new();
        assert_eq!(
            table.transmit(stream, 0..3, &mut output).expect("transmit"),
            3
        );
        assert_eq!(output, b"abc");

        table.ack(stream, 3).expect("ack");
        assert_eq!(tx.max_dequeue(), 0);
        let events = table.take_events();
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
            Index::new(8, 1),
            SessionId::from_raw(10),
            Arc::clone(&rx),
            Arc::new(Fifo::with_capacity(1024).expect("tx FIFO")),
            0,
            0,
        );

        assert_eq!(table.receive(stream, 5, b"world").expect("ooo receive"), 0);
        assert_eq!(rx.max_dequeue(), 0);
        assert!(table.take_events().is_empty());

        assert_eq!(table.receive(stream, 0, b"hello").expect("head"), 10);
        assert_eq!(rx.max_dequeue(), 10);
        let events = table.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rx, 10);
    }
}

pub(super) unsafe fn transmit_callback(
    user_data: usize,
    stream: StreamId,
    offsets: Range<u64>,
    output: &mut Vec<u8>,
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
