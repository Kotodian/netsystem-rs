use hammer_core::protocol::tcp::{TcpSackBlock, TcpSegmentFlags, TcpSeq};
use hammer_infra::rbtree::RbTree;

const TCP_MAX_SACK_BLOCKS: usize = 255;
const TCP_OUTPUT_SACK_BLOCKS: usize = 4;
const TCP_OUTPUT_SACK_BLOCKS_WITH_TIMESTAMPS: usize = 3;

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
}

impl From<TcpSackRange> for TcpSackBlock {
    #[inline]
    fn from(range: TcpSackRange) -> Self {
        Self {
            left_edge: range.left,
            right_edge: range.right,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSackBlockState {
    right: TcpSeq,
    prev_recent: Option<TcpSeq>,
    next_recent: Option<TcpSeq>,
}

impl TcpSackBlockState {
    #[inline]
    const fn new(right: TcpSeq) -> Self {
        Self {
            right,
            prev_recent: None,
            next_recent: None,
        }
    }

    #[inline]
    const fn range(self, left: TcpSeq) -> TcpSackRange {
        TcpSackRange::new(left, self.right)
    }
}

#[derive(Debug)]
pub(crate) struct TcpSackState {
    blocks: RbTree<TcpSeq, TcpSackBlockState>,
    recent_head: Option<TcpSeq>,
    recent_tail: Option<TcpSeq>,
    pending_dsack: Option<TcpSackRange>,
    output_pos: usize,
}

impl Clone for TcpSackState {
    fn clone(&self) -> Self {
        let mut blocks = RbTree::with_capacity(self.blocks.len().max(1));
        for (left, state) in self.blocks.iter() {
            let _ = blocks.insert(*left, *state);
        }
        Self {
            blocks,
            recent_head: self.recent_head,
            recent_tail: self.recent_tail,
            pending_dsack: self.pending_dsack,
            output_pos: self.output_pos,
        }
    }
}

impl PartialEq for TcpSackState {
    fn eq(&self, other: &Self) -> bool {
        if self.recent_head != other.recent_head
            || self.recent_tail != other.recent_tail
            || self.pending_dsack != other.pending_dsack
            || self.output_pos != other.output_pos
            || self.blocks.len() != other.blocks.len()
        {
            return false;
        }
        self.blocks
            .iter()
            .zip(other.blocks.iter())
            .all(|((left_a, state_a), (left_b, state_b))| left_a == left_b && state_a == state_b)
    }
}

impl Eq for TcpSackState {}

impl Default for TcpSackState {
    #[inline]
    fn default() -> Self {
        Self {
            blocks: RbTree::with_capacity(8),
            recent_head: None,
            recent_tail: None,
            pending_dsack: None,
            output_pos: 0,
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
            self.clear_blocks();
            self.pending_dsack = None;
            return;
        }
        self.prune_delivered(rcv_nxt);
        if right <= left {
            return;
        }

        let mut merged = TcpSackRange::new(left.max(rcv_nxt), right);
        if merged.right <= merged.left {
            return;
        }

        if let Some(existing) = self.blocks.get(&merged.left).copied()
            && ranges_overlap_or_touch(merged, existing.range(merged.left))
        {
            let removed = self
                .remove_block(merged.left)
                .expect("tcp sack block should exist");
            merged.left = merged.left.min(removed.left);
            merged.right = merged.right.max(removed.right);
        }

        if let Some((block_left, block_state)) = self
            .blocks
            .predecessor(&merged.left)
            .map(|(block_left, block_state)| (*block_left, *block_state))
            && ranges_overlap_or_touch(merged, block_state.range(block_left))
        {
            let removed = self
                .remove_block(block_left)
                .expect("tcp sack predecessor should exist");
            merged.left = merged.left.min(removed.left);
            merged.right = merged.right.max(removed.right);
        }

        loop {
            let Some((block_left, block_state)) = self
                .blocks
                .successor(&merged.left)
                .map(|(block_left, block_state)| (*block_left, *block_state))
            else {
                break;
            };
            if !ranges_overlap_or_touch(merged, block_state.range(block_left)) {
                break;
            }
            let removed = self
                .remove_block(block_left)
                .expect("tcp sack successor should exist");
            merged.left = merged.left.min(removed.left);
            merged.right = merged.right.max(removed.right);
        }

        self.insert_block_as_most_recent(merged);
        self.trim_block_capacity();
    }

    #[inline]
    pub(crate) fn set_duplicate(&mut self, enabled: bool, left: TcpSeq, right: TcpSeq) {
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
        timestamps: bool,
        flags: TcpSegmentFlags,
    ) -> Option<([TcpSackBlock; 4], usize)> {
        if !enabled || !flags.contains(TcpSegmentFlags::ACK) {
            return None;
        }

        let mut output = [zero_sack_block(); 4];
        let mut count = 0usize;

        if let Some(dsack) = self.pending_dsack.take() {
            output[count] = dsack.into();
            count += 1;
        }

        let mut limit = if timestamps {
            TCP_OUTPUT_SACK_BLOCKS_WITH_TIMESTAMPS
        } else {
            TCP_OUTPUT_SACK_BLOCKS
        };
        limit = limit.saturating_sub(count);
        if limit == 0 || self.blocks.is_empty() {
            return (count != 0).then_some((output, count));
        }

        if self.output_pos >= self.blocks.len() {
            self.output_pos = 0;
        }

        let mut cursor = self.recent_head;
        let mut skipped = 0usize;
        while skipped < self.output_pos {
            let Some(left) = cursor else {
                self.output_pos = 0;
                cursor = self.recent_head;
                break;
            };
            let state = *self
                .blocks
                .get(&left)
                .expect("tcp sack recency link should point to existing block");
            cursor = state.next_recent;
            skipped += 1;
        }

        let mut emitted = 0usize;
        while let Some(left) = cursor {
            if emitted == limit || count == output.len() {
                break;
            }
            let state = *self
                .blocks
                .get(&left)
                .expect("tcp sack recency link should point to existing block");
            output[count] = state.range(left).into();
            count += 1;
            emitted += 1;
            cursor = state.next_recent;
        }
        self.output_pos = self.output_pos.saturating_add(emitted);

        (count != 0).then_some((output, count))
    }

    #[inline]
    pub(crate) fn has_pending_output(&self, timestamps: bool) -> bool {
        let output_limit = if timestamps {
            TCP_OUTPUT_SACK_BLOCKS_WITH_TIMESTAMPS
        } else {
            TCP_OUTPUT_SACK_BLOCKS
        };
        self.pending_dsack.is_some()
            || (self.blocks.len() > output_limit)
            || (self.output_pos < self.blocks.len())
    }

    fn clear_blocks(&mut self) {
        self.blocks = RbTree::with_capacity(self.blocks.len().max(1));
        self.recent_head = None;
        self.recent_tail = None;
        self.output_pos = 0;
    }

    fn prune_delivered(&mut self, rcv_nxt: TcpSeq) {
        loop {
            let Some((left, state)) = self.blocks.first().map(|(left, state)| (*left, *state))
            else {
                break;
            };

            if state.right <= rcv_nxt {
                let _ = self.remove_block(left);
                continue;
            }
            if left < rcv_nxt {
                self.rename_block_left(left, rcv_nxt);
            }
            break;
        }
    }

    fn insert_block_as_most_recent(&mut self, block: TcpSackRange) {
        let mut state = TcpSackBlockState::new(block.right);
        state.next_recent = self.recent_head;
        let previous_head = self.recent_head;
        let replaced = self.blocks.insert(block.left, state);
        debug_assert!(
            replaced.is_none(),
            "tcp sack block key should remain unique"
        );

        if let Some(head) = previous_head {
            self.blocks
                .get_mut(&head)
                .expect("tcp sack recent head should exist")
                .prev_recent = Some(block.left);
        } else {
            self.recent_tail = Some(block.left);
        }
        self.recent_head = Some(block.left);
        self.output_pos = 0;
    }

    fn trim_block_capacity(&mut self) {
        while self.blocks.len() > TCP_MAX_SACK_BLOCKS {
            let Some(left) = self.recent_tail else {
                break;
            };
            let _ = self.remove_block(left);
        }
        if self.output_pos > self.blocks.len() {
            self.output_pos = 0;
        }
    }

    fn remove_block(&mut self, left: TcpSeq) -> Option<TcpSackRange> {
        let state = *self.blocks.get(&left)?;
        self.detach_recent(left, state);
        let state = self
            .blocks
            .remove(&left)
            .expect("tcp sack block should exist while removing");
        Some(state.range(left))
    }

    fn rename_block_left(&mut self, old_left: TcpSeq, new_left: TcpSeq) {
        if old_left >= new_left {
            return;
        }
        let Some(state) = self.blocks.remove(&old_left) else {
            return;
        };
        let replaced = self.blocks.insert(new_left, state);
        debug_assert!(
            replaced.is_none(),
            "tcp sack block left edge rename should remain unique"
        );
        if let Some(prev) = state.prev_recent {
            self.blocks
                .get_mut(&prev)
                .expect("tcp sack recent predecessor should exist")
                .next_recent = Some(new_left);
        } else {
            self.recent_head = Some(new_left);
        }
        if let Some(next) = state.next_recent {
            self.blocks
                .get_mut(&next)
                .expect("tcp sack recent successor should exist")
                .prev_recent = Some(new_left);
        } else {
            self.recent_tail = Some(new_left);
        }
    }

    fn detach_recent(&mut self, left: TcpSeq, state: TcpSackBlockState) {
        if let Some(prev) = state.prev_recent {
            self.blocks
                .get_mut(&prev)
                .expect("tcp sack recent predecessor should exist")
                .next_recent = state.next_recent;
        } else {
            self.recent_head = state.next_recent;
        }

        if let Some(next) = state.next_recent {
            self.blocks
                .get_mut(&next)
                .expect("tcp sack recent successor should exist")
                .prev_recent = state.prev_recent;
        } else {
            self.recent_tail = state.prev_recent;
        }

        let _ = left;
        if self.output_pos > self.blocks.len() {
            self.output_pos = 0;
        }
    }

    #[cfg(test)]
    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    #[cfg(test)]
    pub(crate) fn block(&self, index: usize) -> TcpSackBlock {
        self.output_blocks()[index]
    }

    #[cfg(test)]
    pub(crate) fn pending_dsack(&self) -> Option<TcpSackBlock> {
        self.pending_dsack.map(Into::into)
    }

    #[cfg(test)]
    fn output_blocks(&self) -> std::vec::Vec<TcpSackBlock> {
        let mut blocks = std::vec::Vec::new();
        let mut cursor = self.recent_head;
        while let Some(left) = cursor {
            let state = *self
                .blocks
                .get(&left)
                .expect("tcp sack recency link should point to existing block");
            blocks.push(state.range(left).into());
            cursor = state.next_recent;
        }
        blocks
    }
}

#[inline]
fn zero_sack_block() -> TcpSackBlock {
    TcpSackBlock {
        left_edge: 0u32.into(),
        right_edge: 0u32.into(),
    }
}

#[inline]
fn ranges_overlap_or_touch(left: TcpSackRange, right: TcpSackRange) -> bool {
    left.right >= right.left && right.right >= left.left
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_update_sack_block_merges_and_drops_delivered_ranges() {
        let mut sack = TcpSackState::default();
        let mut rcv_nxt: TcpSeq = 1_000u32.into();

        sack.update_range(true, rcv_nxt, 1_020u32.into(), 1_030u32.into());
        sack.update_range(true, rcv_nxt, 1_040u32.into(), 1_050u32.into());
        sack.update_range(true, rcv_nxt, 1_028u32.into(), 1_045u32.into());

        assert_eq!(sack.block_count(), 1);
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: 1_020u32.into(),
                right_edge: 1_050u32.into(),
            }
        );

        rcv_nxt = 1_050u32.into();
        sack.update_range(true, rcv_nxt, rcv_nxt, rcv_nxt);

        assert_eq!(sack.block_count(), 0);
    }

    #[test]
    fn tcp_output_sack_blocks_emits_pending_dsack_first() {
        let mut sack = TcpSackState::default();
        sack.update_range(true, 0u32.into(), 8_000u32.into(), 8_100u32.into());
        sack.set_duplicate(true, 6_500u32.into(), 6_550u32.into());

        let (blocks, count) = sack
            .take_output(true, false, TcpSegmentFlags::ACK)
            .expect("sack blocks");

        assert_eq!(count, 2);
        assert_eq!(
            blocks[0],
            TcpSackBlock {
                left_edge: 6_500u32.into(),
                right_edge: 6_550u32.into(),
            }
        );
        assert_eq!(
            blocks[1],
            TcpSackBlock {
                left_edge: 8_000u32.into(),
                right_edge: 8_100u32.into(),
            }
        );
        assert!(sack.pending_dsack().is_none());
    }

    #[test]
    fn tcp_duplicate_then_overlap_updates_dsack_and_sack_ranges() {
        let mut sack = TcpSackState::default();
        let rcv_nxt: TcpSeq = 100u32.into();

        sack.set_duplicate(true, 90u32.into(), rcv_nxt);
        sack.update_range(true, rcv_nxt, rcv_nxt, 110u32.into());

        assert_eq!(
            sack.pending_dsack(),
            Some(TcpSackBlock {
                left_edge: 90u32.into(),
                right_edge: 100u32.into(),
            })
        );
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: 100u32.into(),
                right_edge: 110u32.into(),
            }
        );
    }

    #[test]
    fn tcp_sack_scoreboard_keeps_newest_block_first_across_disjoint_ooo_ranges() {
        let mut sack = TcpSackState::default();
        let rcv_nxt: TcpSeq = 1_000u32.into();

        sack.update_range(true, rcv_nxt, 1_400u32.into(), 1_450u32.into());
        sack.update_range(true, rcv_nxt, 1_200u32.into(), 1_260u32.into());
        sack.update_range(true, rcv_nxt, 1_300u32.into(), 1_340u32.into());

        assert_eq!(sack.block_count(), 3);
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: 1_300u32.into(),
                right_edge: 1_340u32.into(),
            }
        );
        assert_eq!(
            sack.block(1),
            TcpSackBlock {
                left_edge: 1_200u32.into(),
                right_edge: 1_260u32.into(),
            }
        );
        assert_eq!(
            sack.block(2),
            TcpSackBlock {
                left_edge: 1_400u32.into(),
                right_edge: 1_450u32.into(),
            }
        );
    }

    #[test]
    fn tcp_duplicate_sack_output_limits_regular_blocks_after_dsack() {
        let mut sack = TcpSackState::default();
        let rcv_nxt: TcpSeq = 5_000u32.into();

        sack.update_range(true, rcv_nxt, 5_500u32.into(), 5_550u32.into());
        sack.update_range(true, rcv_nxt, 5_600u32.into(), 5_650u32.into());
        sack.update_range(true, rcv_nxt, 5_700u32.into(), 5_750u32.into());
        sack.update_range(true, rcv_nxt, 5_800u32.into(), 5_850u32.into());
        sack.set_duplicate(true, 4_920u32.into(), 4_980u32.into());

        let (blocks, count) = sack
            .take_output(true, false, TcpSegmentFlags::ACK)
            .expect("sack blocks");

        assert_eq!(count, 3);
        assert_eq!(
            blocks[0],
            TcpSackBlock {
                left_edge: 4_920u32.into(),
                right_edge: 4_980u32.into(),
            }
        );
        assert_eq!(
            blocks[1],
            TcpSackBlock {
                left_edge: 5_800u32.into(),
                right_edge: 5_850u32.into(),
            }
        );
        assert_eq!(
            blocks[2],
            TcpSackBlock {
                left_edge: 5_700u32.into(),
                right_edge: 5_750u32.into(),
            }
        );
    }

    #[test]
    fn tcp_sack_range_bridging_merges_predecessor_and_successor_blocks() {
        let mut sack = TcpSackState::default();
        let rcv_nxt: TcpSeq = 10_000u32.into();

        sack.update_range(true, rcv_nxt, 10_200u32.into(), 10_240u32.into());
        sack.update_range(true, rcv_nxt, 10_300u32.into(), 10_340u32.into());
        sack.update_range(true, rcv_nxt, 10_220u32.into(), 10_320u32.into());

        assert_eq!(sack.block_count(), 1);
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: 10_200u32.into(),
                right_edge: 10_340u32.into(),
            }
        );
    }

    #[test]
    fn tcp_sack_prune_keeps_undelivered_suffix_when_rcv_nxt_moves_into_block() {
        let mut sack = TcpSackState::default();
        let rcv_nxt: TcpSeq = 20_000u32.into();

        sack.update_range(true, rcv_nxt, 20_100u32.into(), 20_180u32.into());
        sack.update_range(true, 20_140u32.into(), 20_140u32.into(), 20_140u32.into());

        assert_eq!(sack.block_count(), 1);
        assert_eq!(
            sack.block(0),
            TcpSackBlock {
                left_edge: 20_140u32.into(),
                right_edge: 20_180u32.into(),
            }
        );
    }
}
