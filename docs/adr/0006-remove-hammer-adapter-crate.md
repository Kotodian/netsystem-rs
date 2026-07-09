# Remove hammer-adapter crate

Hammer will remove the `hammer-adapter` crate instead of preserving it as a thin compatibility layer. Data-plane primitives and graph identity move to `hammer-core`, executable graph runtime contracts move to `hammer-runtime`, and callers such as `hammer-service`, `hammer-app`, macros, and FFI-facing code use those owners directly. Keeping an empty adapter crate would preserve the old dependency seam and invite new cross-crate contracts to accumulate there again.
