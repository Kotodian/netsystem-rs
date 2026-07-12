use hammer_infra::align::align_up;
use hammer_infra::segment::Segment;

use crate::app::session_msg_queue::SessionMsgQueue;

/// Offsets for the four session queues within a shared segment.
/// These are filled in by the dataplane when it pre-allocates session
/// resources, then sent to the app process via SCM_RIGHTS.
pub struct SessionOffsets {
    pub rx_fifo_off: u64,
    pub tx_fifo_off: u64,
    pub evt_q_off: u64,
    pub tx_evt_q_off: u64,
}

impl SessionOffsets {
    /// Pre-allocate all four session queues in `seg` and return their offsets.
    /// `fifo_capacity` is the chunk count; `evt_q_capacity` is the desired
    /// usable event count (one extra slot is reserved for the ring protocol).
    pub fn allocate<S: Segment>(seg: &S, fifo_chunks: u32, evt_q_capacity: usize) -> Self {
        let chunk_data_size = (fifo_chunks as usize).min(4096);
        let per_chunk = 16 + chunk_data_size;
        let fifo_total = align_up(192 + fifo_chunks as usize * per_chunk, 64);
        let ring_nitems = evt_q_capacity.max(1) as u32;
        let q_nitems = (evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let evt_msgq_total = align_up(
            SessionMsgQueue::<S>::layout_bytes(q_nitems, ring_nitems).expect("session mq layout"),
            64,
        );
        // Shared worker tx_evt_q uses a fixed 64/64 multi-ring capacity (attach path).
        let tx_msgq_total = align_up(
            SessionMsgQueue::<S>::layout_bytes(64, 64).expect("tx session mq layout"),
            64,
        );

        let rx_fifo_off = seg.alloc(fifo_total, 64);
        let tx_fifo_off = seg.alloc(fifo_total, 64);
        let evt_q_off = seg.alloc(evt_msgq_total, 64);
        let tx_evt_q_off = seg.alloc(tx_msgq_total, 64);
        Self {
            rx_fifo_off,
            tx_fifo_off,
            evt_q_off,
            tx_evt_q_off,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::segment::Local;

    #[test]
    fn session_offsets_allocates_all_four_queues() {
        let seg = Local::new(1024 * 1024);
        let offs = SessionOffsets::allocate(&seg, 32, 16);
        assert!(offs.rx_fifo_off < offs.tx_fifo_off);
        assert!(offs.tx_fifo_off < offs.evt_q_off);
        assert!(offs.evt_q_off < offs.tx_evt_q_off);
    }

    #[test]
    fn session_offsets_offsets_are_cachelined() {
        let seg = Local::new(1024 * 1024);
        let offs = SessionOffsets::allocate(&seg, 64, 32);
        assert_eq!(offs.rx_fifo_off % 64, 0);
        assert_eq!(offs.tx_fifo_off % 64, 0);
        assert_eq!(offs.evt_q_off % 64, 0);
        assert_eq!(offs.tx_evt_q_off % 64, 0);
    }
}
