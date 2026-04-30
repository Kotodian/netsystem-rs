/// Stage-only Lifecycle impl helper. Most M2 managers do nothing in the
/// individual stages beyond logging; this macro keeps the duplication out of
/// each manager file. Manager implementations that need real per-stage logic
/// should implement `Lifecycle` by hand.
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
