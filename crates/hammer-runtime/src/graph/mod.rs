//! Config-driven packet-graph assembly (VPP `vlib` semantics).
//!
//! Mirrors VPP's node-graph construction at the level the current adapter
//! registration surface supports: a process-global, name-keyed inventory of
//! node builders (`NodeRegistry<D>`, akin to
//! `vlib_global_main_t.node_registrations` populated by `VLIB_REGISTER_NODE`),
//! and per-worker assembly (`PacketGraphAssembler`) that invokes the selected
//! builders in dependency order on each `DataPlaneRuntime`. Each builder
//! resolves its own next edges by name against nodes registered earlier in
//! the same pass.
//!
//! `D` is the service-defined dependency bag carried through assembly. The
//! graph layer is generic over `D` and never names its fields — no trait
//! object, no `dyn Any`.
//!
//! Config selects which *registered* node types participate per graph; it
//! never defines new node types (VPP invariant). Feature arcs remain owned by
//! the existing `FeatureArcControl` abstraction in `hammer-service`.

pub mod assembler;
pub mod registry;

pub use assembler::{AssembledGraph, GraphSpec, PacketGraphAssembler};
pub use registry::{NodeBuilder, NodeCtx, NodeRegistry};
