/// Stage-only Lifecycle impl helper. Managers that only need lifecycle log
/// lines use this macro; managers with real per-stage work implement
/// `Lifecycle` by hand.
#[macro_export]
macro_rules! impl_logging_lifecycle {
    ($ty:ty, $name:expr) => {
        impl $crate::adapter::Lifecycle for $ty {
            fn name(&self) -> &str {
                $name
            }

            fn start(
                &self,
                stage: $crate::adapter::StartStage,
            ) -> ::std::result::Result<(), $crate::HammerError> {
                ::tracing::debug!(target: $name, "stage {}", stage.name());
                Ok(())
            }

            fn close(&self) -> ::std::result::Result<(), $crate::HammerError> {
                ::tracing::debug!(target: $name, "close");
                Ok(())
            }
        }
    };
}
