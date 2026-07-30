use super::AppSessionSemantics;

/// Static protocol semantics declared by one Session Transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTransportRegistration {
    name: &'static str,
    upper: AppSessionSemantics,
}

impl SessionTransportRegistration {
    #[doc(hidden)]
    #[inline]
    pub const fn new(name: &'static str, upper: AppSessionSemantics) -> Self {
        Self { name, upper }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline]
    pub const fn upper(self) -> AppSessionSemantics {
        self.upper
    }
}
