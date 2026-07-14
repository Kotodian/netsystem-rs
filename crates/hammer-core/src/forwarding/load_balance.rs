use crate::ds::prefetch::prefetch_read_l1;
use hammer_infra::boxed::Box;
use hammer_infra::vec::Vec;

use super::dpo::{AdjacencyIndex, DpoId, DpoProto};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalanceError {
    BucketCountZero,
    BucketCountTooLarge {
        bucket_count: usize,
    },
    BucketProtoMismatch {
        expected: DpoProto,
        actual: DpoProto,
        bucket_index: u16,
    },
    BucketAdjacencyMissing {
        index: AdjacencyIndex,
        bucket_index: u16,
    },
    BucketAdjacencyProtoMismatch {
        index: AdjacencyIndex,
        expected: DpoProto,
        actual: DpoProto,
        bucket_index: u16,
    },
    BucketLoadBalanceMissing {
        index: LoadBalanceIndex,
        bucket_index: u16,
    },
    BucketLoadBalanceProtoMismatch {
        index: LoadBalanceIndex,
        expected: DpoProto,
        actual: DpoProto,
        bucket_index: u16,
    },
}

#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct LoadBalance<N: Copy> {
    proto: DpoProto,
    bucket_count: u16,
    power_of_two_mask: u16,
    buckets: LoadBalanceBuckets<N>,
}

#[derive(Debug, Clone)]
enum LoadBalanceBuckets<N: Copy> {
    Inline([DpoId<N>; LOAD_BALANCE_INLINE_BUCKETS]),
    Heap(Box<[DpoId<N>]>),
}

impl<N: Copy> LoadBalance<N> {
    #[inline]
    pub fn new(proto: DpoProto, buckets: impl Into<Vec<DpoId<N>>>) -> Self {
        Self::try_new(proto, buckets)
            .expect("load-balance buckets must be non-empty, fit hot-path index, and match proto")
    }

    #[inline]
    pub fn try_new(
        proto: DpoProto,
        buckets: impl Into<Vec<DpoId<N>>>,
    ) -> Result<Self, LoadBalanceError> {
        let buckets = buckets.into();
        let bucket_count = buckets.len();
        validate_bucket_count(bucket_count)?;
        validate_bucket_protos(proto, &buckets)?;
        let power_of_two_mask = bucket_count
            .is_power_of_two()
            .then_some(bucket_count.saturating_sub(1) as u16)
            .unwrap_or(0);
        let buckets = match bucket_count {
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
    pub fn proto(&self) -> DpoProto {
        self.proto
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.bucket_count as usize
    }

    #[inline(always)]
    pub fn buckets(&self) -> &[DpoId<N>] {
        match &self.buckets {
            LoadBalanceBuckets::Inline(buckets) => &buckets[..self.bucket_count()],
            LoadBalanceBuckets::Heap(buckets) => buckets,
        }
    }

    #[inline(always)]
    pub fn select_hash(&self, hash: usize) -> (u16, DpoId<N>) {
        let bucket = self.bucket_for_hash(hash);
        (bucket as u16, self.bucket_unchecked(bucket))
    }

    #[inline(always)]
    pub fn prefetch_bucket(&self, hash: usize) {
        let bucket = self.bucket_for_hash(hash);
        match &self.buckets {
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
            LoadBalanceBuckets::Inline(buckets) => buckets[bucket],
            LoadBalanceBuckets::Heap(buckets) => buckets[bucket],
        }
    }
}

#[inline(always)]
fn validate_bucket_count(bucket_count: usize) -> Result<(), LoadBalanceError> {
    if bucket_count == 0 {
        return Err(LoadBalanceError::BucketCountZero);
    }
    if bucket_count > u16::MAX as usize {
        return Err(LoadBalanceError::BucketCountTooLarge { bucket_count });
    }
    Ok(())
}

#[inline(always)]
fn validate_bucket_protos<N: Copy>(
    expected: DpoProto,
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

impl<N: Copy> From<(DpoProto, AdjacencyIndex, N)> for DpoId<N> {
    #[inline(always)]
    fn from((proto, adjacency, next): (DpoProto, AdjacencyIndex, N)) -> Self {
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
            DpoProto::IP4,
            [
                DpoId::drop(DpoProto::IP4, Next::A),
                DpoId::drop(DpoProto::IP4, Next::B),
            ],
        );
        assert_eq!(lb.proto(), DpoProto::IP4);
        assert_eq!(lb.bucket_count(), 2);
        assert!(matches!(lb.buckets, LoadBalanceBuckets::Inline(_)));
        let (_bucket, dpo) = lb.select_hash(1);
        assert_eq!(dpo.next(), Next::B);
    }

    #[test]
    fn load_balance_rejects_bucket_with_wrong_proto() {
        let err = LoadBalance::try_new(DpoProto::IP4, [DpoId::drop(DpoProto::IP6, Next::A)])
            .expect_err("wrong-proto bucket should be rejected");

        assert_eq!(
            err,
            LoadBalanceError::BucketProtoMismatch {
                expected: DpoProto::IP4,
                actual: DpoProto::IP6,
                bucket_index: 0,
            }
        );
    }

    #[test]
    fn load_balance_rejects_empty_bucket_set() {
        let err = LoadBalance::try_new(DpoProto::IP4, Vec::<DpoId<Next>>::new())
            .expect_err("empty load-balance should be rejected");

        assert_eq!(err, LoadBalanceError::BucketCountZero);
    }

    #[test]
    fn load_balance_rejects_bucket_count_that_exceeds_hot_path_index() {
        let buckets =
            hammer_infra::vec![DpoId::drop(DpoProto::IP4, Next::A); u16::MAX as usize + 1];
        let err = LoadBalance::try_new(DpoProto::IP4, buckets)
            .expect_err("oversized load-balance should be rejected");

        assert_eq!(
            err,
            LoadBalanceError::BucketCountTooLarge {
                bucket_count: u16::MAX as usize + 1,
            }
        );
    }

    #[test]
    fn load_balance_hot_object_is_cacheline_aligned() {
        assert_eq!(std::mem::align_of::<LoadBalance<Next>>(), 64);
    }
}
