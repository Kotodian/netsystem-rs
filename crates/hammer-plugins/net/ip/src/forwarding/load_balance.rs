use hammer_infra::prefetch::prefetch_read_l1;

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
pub struct LoadBalance {
    proto: DpoProto,
    bucket_count: u16,
    power_of_two_mask: u16,
    buckets: LoadBalanceBuckets,
}

#[derive(Debug, Clone)]
enum LoadBalanceBuckets {
    Inline([DpoId; LOAD_BALANCE_INLINE_BUCKETS]),
    Heap(Box<[DpoId]>),
}

impl LoadBalance {
    #[inline]
    pub fn new(proto: DpoProto, buckets: impl Into<Vec<DpoId>>) -> Self {
        Self::try_new(proto, buckets)
            .expect("load-balance buckets must be non-empty, fit hot-path index, and match proto")
    }

    #[inline]
    pub fn try_new(
        proto: DpoProto,
        buckets: impl Into<Vec<DpoId>>,
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
    pub fn buckets(&self) -> &[DpoId] {
        match &self.buckets {
            LoadBalanceBuckets::Inline(buckets) => &buckets[..self.bucket_count()],
            LoadBalanceBuckets::Heap(buckets) => buckets,
        }
    }

    #[inline(always)]
    pub fn select_hash(&self, hash: usize) -> (u16, DpoId) {
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
    fn bucket_unchecked(&self, bucket: usize) -> DpoId {
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
fn validate_bucket_protos(expected: DpoProto, buckets: &[DpoId]) -> Result<(), LoadBalanceError> {
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
