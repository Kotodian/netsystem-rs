# Graph runtime is owned by hammer-runtime

Hammer's Graph Runtime belongs in `hammer-runtime`, including `DataPlaneRuntime`, executable graph node traits, node descriptors, graph registration entries, scheduling queues, readiness, dispatch, named next-arc resolution, and graph runtime statistics. This deliberately replaces the earlier adapter-owned `DataPlaneRuntime` shape so packet graph execution policy lives with the runtime main loop instead of at the adapter seam.

Generated graph code must reference the new owners directly: graph identity and frame primitives through `hammer-core`, executable graph contracts through `hammer-runtime`, and never through `hammer-adapter` re-exports.
