use crate::ds::prefetch::prefetch_read_l1;
use crate::protocol::ip::IpVersion;

use super::dpo::{AdjacencyIndex, DpoId};
use super::ip4_mtrie::Ip4MtrieValue;

pub const LOAD_BALANCE_INLINE_BUCKETS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LoadBalanceIndex(u32);

impl LoadBalanceIndex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }
}

impl Ip4MtrieValue for LoadBalanceIndex {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        self.0
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalanceError {
    BucketProtoMismatch {
        expected: IpVersion,
        actual: IpVersion,
        bucket_index: u16,
    },
    BucketAdjacencyMissing {
        index: AdjacencyIndex,
        bucket_index: u16,
    },
}

#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct LoadBalance<N: Copy> {
    proto: IpVersion,
    bucket_count: u16,
    power_of_two_mask: u16,
    buckets: LoadBalanceBuckets<N>,
}

#[derive(Debug, Clone)]
enum LoadBalanceBuckets<N: Copy> {
    Empty,
    Inline([DpoId<N>; LOAD_BALANCE_INLINE_BUCKETS]),
    Heap(Box<[DpoId<N>]>),
}

impl<N: Copy> LoadBalance<N> {
    #[inline]
    pub fn new(proto: IpVersion, buckets: impl Into<Vec<DpoId<N>>>) -> Self {
        Self::try_new(proto, buckets).expect("load-balance buckets must match proto")
    }

    #[inline]
    pub fn try_new(
        proto: IpVersion,
        buckets: impl Into<Vec<DpoId<N>>>,
    ) -> Result<Self, LoadBalanceError> {
        let buckets = buckets.into();
        validate_bucket_protos(proto, &buckets)?;
        let bucket_count = buckets.len();
        let power_of_two_mask = bucket_count
            .is_power_of_two()
            .then_some(bucket_count.saturating_sub(1) as u16)
            .unwrap_or(0);
        let buckets = match bucket_count {
            0 => LoadBalanceBuckets::Empty,
            1..=LOAD_BALANCE_INLINE_BUCKETS => {
                let first = buckets[0];
                let mut inline = [first; LOAD_BALANCE_INLINE_BUCKETS];
                inline[..bucket_count].copy_from_slice(&buckets);
                LoadBalanceBuckets::Inline(inline)
            }
            _ => LoadBalanceBuckets::Heap(buckets.into_boxed_slice()),
        };
        Ok(Self {
            proto,
            bucket_count: bucket_count as u16,
            power_of_two_mask,
            buckets,
        })
    }

    #[inline(always)]
    pub fn proto(&self) -> IpVersion {
        self.proto
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.bucket_count as usize
    }

    #[inline(always)]
    pub fn buckets(&self) -> &[DpoId<N>] {
        match &self.buckets {
            LoadBalanceBuckets::Empty => &[],
            LoadBalanceBuckets::Inline(buckets) => &buckets[..self.bucket_count()],
            LoadBalanceBuckets::Heap(buckets) => buckets,
        }
    }

    #[inline(always)]
    pub fn select_hash(&self, hash: usize) -> Option<(u16, DpoId<N>)> {
        if self.bucket_count == 0 {
            return None;
        }
        let bucket = self.bucket_for_hash(hash);
        Some((bucket as u16, self.bucket_unchecked(bucket)))
    }

    #[inline(always)]
    pub fn prefetch_bucket(&self, hash: usize) {
        if self.bucket_count == 0 {
            return;
        }
        let bucket = self.bucket_for_hash(hash);
        match &self.buckets {
            LoadBalanceBuckets::Empty => {}
            LoadBalanceBuckets::Inline(buckets) => prefetch_read_l1(&buckets[bucket]),
            LoadBalanceBuckets::Heap(buckets) => prefetch_read_l1(&buckets[bucket]),
        }
    }

    #[inline(always)]
    fn bucket_for_hash(&self, hash: usize) -> usize {
        if self.power_of_two_mask != 0 {
            hash & self.power_of_two_mask as usize
        } else {
            hash % self.bucket_count as usize
        }
    }

    #[inline(always)]
    fn bucket_unchecked(&self, bucket: usize) -> DpoId<N> {
        match &self.buckets {
            LoadBalanceBuckets::Empty => unreachable!(),
            LoadBalanceBuckets::Inline(buckets) => buckets[bucket],
            LoadBalanceBuckets::Heap(buckets) => buckets[bucket],
        }
    }
}

#[inline(always)]
fn validate_bucket_protos<N: Copy>(
    expected: IpVersion,
    buckets: &[DpoId<N>],
) -> Result<(), LoadBalanceError> {
    for (bucket_index, bucket) in buckets.iter().enumerate() {
        let actual = bucket.proto();
        if actual != expected {
            return Err(LoadBalanceError::BucketProtoMismatch {
                expected,
                actual,
                bucket_index: bucket_index as u16,
            });
        }
    }
    Ok(())
}

impl<N: Copy> From<(IpVersion, AdjacencyIndex, N)> for DpoId<N> {
    #[inline(always)]
    fn from((proto, adjacency, next): (IpVersion, AdjacencyIndex, N)) -> Self {
        Self::adjacency(proto, adjacency, next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Next {
        A,
        B,
    }

    #[test]
    fn inline_buckets_handle_small_load_balances_without_heap_slice() {
        let lb = LoadBalance::new(
            IpVersion::V4,
            [
                DpoId::drop(IpVersion::V4, Next::A),
                DpoId::drop(IpVersion::V4, Next::B),
            ],
        );
        assert_eq!(lb.proto(), IpVersion::V4);
        assert_eq!(lb.bucket_count(), 2);
        assert!(matches!(lb.buckets, LoadBalanceBuckets::Inline(_)));
        assert_eq!(lb.select_hash(1).expect("bucket").1.next(), Next::B);
    }

    #[test]
    fn load_balance_rejects_bucket_with_wrong_proto() {
        let err = LoadBalance::try_new(IpVersion::V4, [DpoId::drop(IpVersion::V6, Next::A)])
            .expect_err("wrong-proto bucket should be rejected");

        assert_eq!(
            err,
            LoadBalanceError::BucketProtoMismatch {
                expected: IpVersion::V4,
                actual: IpVersion::V6,
                bucket_index: 0,
            }
        );
    }

    #[test]
    fn load_balance_hot_object_is_cacheline_aligned() {
        assert_eq!(std::mem::align_of::<LoadBalance<Next>>(), 64);
    }
}
