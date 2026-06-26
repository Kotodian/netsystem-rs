use hammer_infra::align::{CACHE_LINE, align_up};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FifoSegmentMemoryKind {
    ProcessLocal,
    SharedMemory,
}

/// Offset layout for a future mmap-backed `FifoSegment` (Stage F). C1 only
/// defines the layout record; nothing allocates from it yet. The record is
/// offset-based and pointer-free so the same struct will describe both the
/// in-process heap variant and the cross-process mmap variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FifoSegmentLayout {
    rx_fifo_offset: usize,
    rx_fifo_bytes: usize,
    tx_fifo_offset: usize,
    tx_fifo_bytes: usize,
    evt_q_offset: usize,
    evt_q_bytes: usize,
    cacheline_size: usize,
    fifo_capacity: usize,
    evt_q_capacity: usize,
}

impl FifoSegmentLayout {
    pub fn new(fifo_capacity: usize, evt_q_capacity: usize) -> Self {
        // Stage F will compute exact byte sizes from SvmFifo/SvmMsgQ `repr(C)`
        // footprints. For C1 we use placeholder rounded sizes so the layout
        // record exists and is usable in tests; Stage F replaces these with
        // `size_of::<SvmFifo>()`/`size_of::<SvmMsgQ>()` once those types are
        // `#[repr(C)]` and mmap-friendly.
        let rx_fifo_bytes = align_up(128, CACHE_LINE);
        let tx_fifo_bytes = align_up(128, CACHE_LINE);
        let evt_q_bytes = align_up(128, CACHE_LINE);
        let rx_fifo_offset = 0;
        let tx_fifo_offset = align_up(rx_fifo_offset + rx_fifo_bytes, CACHE_LINE);
        let evt_q_offset = align_up(tx_fifo_offset + tx_fifo_bytes, CACHE_LINE);
        Self {
            rx_fifo_offset,
            rx_fifo_bytes,
            tx_fifo_offset,
            tx_fifo_bytes,
            evt_q_offset,
            evt_q_bytes,
            cacheline_size: CACHE_LINE,
            fifo_capacity,
            evt_q_capacity,
        }
    }

    #[inline]
    pub const fn rx_fifo_offset(self) -> usize {
        self.rx_fifo_offset
    }

    #[inline]
    pub const fn rx_fifo_bytes(self) -> usize {
        self.rx_fifo_bytes
    }

    #[inline]
    pub const fn tx_fifo_offset(self) -> usize {
        self.tx_fifo_offset
    }

    #[inline]
    pub const fn tx_fifo_bytes(self) -> usize {
        self.tx_fifo_bytes
    }

    #[inline]
    pub const fn evt_q_offset(self) -> usize {
        self.evt_q_offset
    }

    #[inline]
    pub const fn evt_q_bytes(self) -> usize {
        self.evt_q_bytes
    }

    #[inline]
    pub const fn cacheline_size(self) -> usize {
        self.cacheline_size
    }

    #[inline]
    pub const fn fifo_capacity(self) -> usize {
        self.fifo_capacity
    }

    #[inline]
    pub const fn evt_q_capacity(self) -> usize {
        self.evt_q_capacity
    }

    /// Total segment bytes from origin to end of evt_q region.
    #[inline]
    pub const fn total_bytes(self) -> usize {
        self.evt_q_offset + self.evt_q_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_segment_layout_orders_regions_and_aligns_to_cacheline() {
        let layout = FifoSegmentLayout::new(64 * 1024, 16);
        assert_eq!(layout.rx_fifo_offset(), 0);
        assert!(layout.tx_fifo_offset() > layout.rx_fifo_offset());
        assert_eq!(layout.tx_fifo_offset() % CACHE_LINE, 0);
        assert!(layout.evt_q_offset() > layout.tx_fifo_offset());
        assert_eq!(layout.evt_q_offset() % CACHE_LINE, 0);
        assert_eq!(
            layout.total_bytes(),
            layout.evt_q_offset() + layout.evt_q_bytes()
        );
    }
}
