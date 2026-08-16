//! Mapping-relative offsets into the stats segment.

/// A mapping-relative byte offset. Zero is the reserved null offset: never
/// a valid descriptor or value location, since the header occupies the
/// start of the mapping. Offsets are checked against the mapping bounds at
/// the [`crate::mapping::Mapping`] boundary.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Offset(u64);

impl Offset {
    pub(crate) const fn new(value: u64) -> Offset {
        Offset(value)
    }

    pub(crate) fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn is_null(self) -> bool {
        self.0 == 0
    }

    pub(crate) fn checked_add(self, delta: u64) -> Option<Offset> {
        self.0.checked_add(delta).map(Offset)
    }
}
