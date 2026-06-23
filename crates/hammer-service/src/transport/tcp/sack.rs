use hammer_core::protocol::tcp::{TcpSackBlock, TcpSegmentFlags, TcpSeq};
use hammer_infra::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSackRange {
    left: TcpSeq,
    right: TcpSeq,
}

impl TcpSackRange {
    #[inline]
    const fn new(left: TcpSeq, right: TcpSeq) -> Self {
        Self { left, right }
    }

    #[inline]
    const fn into_block(self) -> TcpSackBlock {
        TcpSackBlock {
            left_edge: self.left,
            right_edge: self.right,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpSackState {
    blocks: Vec<TcpSackRange>,
    scratch: Vec<TcpSackRange>,
    pending_dsack: Option<TcpSackRange>,
}

impl Default for TcpSackState {
    #[inline]
    fn default() -> Self {
        Self {
            blocks: Vec::with_capacity(8),
            scratch: Vec::with_capacity(8),
            pending_dsack: None,
        }
    }
}

impl TcpSackState {
    #[inline]
    pub(crate) fn update_range(
        &mut self,
        enabled: bool,
        rcv_nxt: TcpSeq,
        left: TcpSeq,
        right: TcpSeq,
    ) {
        if !enabled {
            self.blocks.clear();
            return;
        }
        if right <= left {
            self.rebuild(None, rcv_nxt);
            return;
        }
        self.rebuild(Some(TcpSackRange::new(left, right)), rcv_nxt);
    }

    #[inline]
    pub(crate) fn set_duplicate(
        &mut self,
        enabled: bool,
        left: TcpSeq,
        right: TcpSeq,
    ) {
        if !enabled {
            self.pending_dsack = None;
            return;
        }
        self.pending_dsack = Some(TcpSackRange::new(left, right));
    }

    #[inline]
    pub(crate) fn take_output(
        &mut self,
        enabled: bool,
        flags: TcpSegmentFlags,
    ) -> Option<([TcpSackBlock; 4], usize)> {
        if !enabled || !flags.contains(TcpSegmentFlags::ACK) {
            return None;
        }
        let mut output = [TcpSackBlock {
            left_edge: TcpSeq::from(0),
            right_edge: TcpSeq::from(0),
        }; 4];
        let mut count = 0usize;
        if let Some(dsack) = self.pending_dsack.take() {
            output[count] = dsack.into_block();
            count += 1;
        }
        for block in self.blocks.iter().take(output.len().saturating_sub(count)) {
            output[count] = block.into_block();
            count += 1;
        }
        (count != 0).then_some((output, count))
    }

    fn rebuild(&mut self, newest: Option<TcpSackRange>, rcv_nxt: TcpSeq) {
        self.scratch.clear();
        let mut current = newest.filter(|range| rcv_nxt <= range.left && range.left < range.right);
        for index in 0..self.blocks.len() {
            let block = self.blocks[index];
            if rcv_nxt >= block.left {
                continue;
            }
            if let Some(range) = current.as_mut() {
                if blocks_overlap_or_touch(*range, block) {
                    if block.left < range.left {
                        range.left = block.left;
                    }
                    if range.right < block.right {
                        range.right = block.right;
                    }
                    continue;
                }
            }
            if let Some(range) = current.take() {
                self.scratch.push(range);
            }
            self.scratch.push(block);
        }
        if let Some(range) = current.take() {
            self.scratch.push(range);
        }
        std::mem::swap(&mut self.blocks, &mut self.scratch);
        self.scratch.clear();
    }

    #[cfg(test)]
    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    #[cfg(test)]
    pub(crate) fn block(&self, index: usize) -> TcpSackBlock {
        self.blocks[index].into_block()
    }

    #[cfg(test)]
    pub(crate) fn pending_dsack(&self) -> Option<TcpSackBlock> {
        self.pending_dsack.map(TcpSackRange::into_block)
    }
}

#[inline]
fn blocks_overlap_or_touch(left: TcpSackRange, right: TcpSackRange) -> bool {
    left.right >= right.left && right.right >= left.left
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_update_sack_block_merges_and_drops_delivered_ranges() {
        let mut sack = TcpSackState::default();
        let mut rcv_nxt = TcpSeq::from(1_000);

        sack.update_range(true, rcv_nxt, TcpSeq::from(1_020), TcpSeq::from(1_030));
        sack.update_range(true, rcv_nxt, TcpSeq::from(1_040), TcpSeq::from(1_050));
        sack.update_range(true, rcv_nxt, TcpSeq::from(1_028), TcpSeq::from(1_045));

        assert_eq!(sack.block_count(), 1);
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: TcpSeq::from(1_020),
                right_edge: TcpSeq::from(1_050),
            }
        );

        rcv_nxt = TcpSeq::from(1_050);
        sack.update_range(true, rcv_nxt, rcv_nxt, rcv_nxt);

        assert_eq!(sack.block_count(), 0);
    }

    #[test]
    fn tcp_output_sack_blocks_emits_pending_dsack_first() {
        let mut sack = TcpSackState::default();
        sack.update_range(true, TcpSeq::from(0), TcpSeq::from(8_000), TcpSeq::from(8_100));
        sack.set_duplicate(true, TcpSeq::from(6_500), TcpSeq::from(6_550));

        let (blocks, count) = sack
            .take_output(true, TcpSegmentFlags::ACK)
            .expect("sack blocks");

        assert_eq!(count, 2);
        assert_eq!(
            blocks[0],
            TcpSackBlock {
                left_edge: TcpSeq::from(6_500),
                right_edge: TcpSeq::from(6_550),
            }
        );
        assert_eq!(
            blocks[1],
            TcpSackBlock {
                left_edge: TcpSeq::from(8_000),
                right_edge: TcpSeq::from(8_100),
            }
        );
        assert!(sack.pending_dsack().is_none());
    }

    #[test]
    fn tcp_duplicate_then_overlap_updates_dsack_and_sack_ranges() {
        let mut sack = TcpSackState::default();
        let rcv_nxt = TcpSeq::from(100);

        sack.set_duplicate(true, TcpSeq::from(90), rcv_nxt);
        sack.update_range(true, rcv_nxt, rcv_nxt, TcpSeq::from(110));

        assert_eq!(
            sack.pending_dsack(),
            Some(TcpSackBlock {
                left_edge: TcpSeq::from(90),
                right_edge: TcpSeq::from(100),
            })
        );
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: TcpSeq::from(100),
                right_edge: TcpSeq::from(110),
            }
        );
    }
}
