# Unify data-plane pool identity as Index

Buffer and frame pools share one concrete `Index` value: private `pool_id`, `slot`, and `generation` fields, `#[repr(C)]`, asserted 16-byte layout. Pools construct Index; copying does not take buffer ownership or change reference counts. Frame identity stays inside Frame state and is not exposed through a public accessor.

Pool IDs come from one checked process-wide nonzero namespace. Namespace exhaustion aborts pool construction rather than wrapping. A slot at maximum generation retires instead of becoming valid under an earlier generation.

Validation failures use structured `DataPlaneError` variants that carry the identity facts needed for diagnostics (`ForeignIndex`, `StaleIndex`, `IndexSlotOutOfBounds`, `IndexSlotFree`). Buffer validation no longer reports those failures as string-only internal errors.

`BufferIndex` and `FrameIndex` are removed. Session and transport code that already used `hammer_infra::pool::Index` as a type parameter keeps that pool index distinct from the data-plane `Index`.
