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
