use std::time::Duration;

use hammer_infra::bihash::BihashKey;
use hammer_infra::bitmap::Bitmap;

/// Approximate duplicate suppression owned by one packet-processing worker.
/// Colliding keys are deliberately suppressed until the bitmap is reset.
pub struct Throttle {
    bitmap: Bitmap,
    seed: u64,
    last_reset: Duration,
    period: Duration,
}

impl Throttle {
    pub fn new(period: Duration) -> Self {
        Self {
            bitmap: Bitmap::with_capacity(512),
            seed: 0,
            last_reset: Duration::ZERO,
            period,
        }
    }

    /// Refreshes once per frame using elapsed monotonic time from one origin.
    /// Equality does not expire the interval, matching VPP's throttle_seed.
    pub fn seed(&mut self, now: Duration) -> u64 {
        if now.saturating_sub(self.last_reset) > self.period {
            self.seed = self
                .seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.bitmap.clear_all();
            self.last_reset = now;
        }
        self.seed
    }

    /// Returns true when this key's bucket was already observed this interval.
    /// `seed` is the value obtained for the current frame, before checking keys.
    #[inline(always)]
    pub fn check(&mut self, key: u64, seed: u64) -> bool {
        // Reuse Hammer's word hash; collision positions are not a wire or
        // persistence contract shared with VPP's clib_xxhash implementation.
        let bucket = ((key ^ seed).hash() & 511) as usize;
        !self.bitmap.set(bucket)
    }
}
