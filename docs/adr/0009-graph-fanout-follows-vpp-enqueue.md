# Graph fanout follows VPP per-group enqueue

Hammer Graph Fanout belongs to Graph Runtime and follows VPP's per-group next-frame flow. For the first unhandled current-node-local next, Graph Runtime gets the arc's current appendable Next Frame, stably moves matching Index values, puts that frame, and rotates within the same group when capacity requires it. It does not build an all-groups transaction or define cross-arc scheduler order.

Frame is the Rust RAII owner of contained buffer references. Each group completes its fallible lookup and capacity work while the input Frame still owns the matching Index values; the subsequent source-to-next move is a non-allocating, non-unwinding internal section. Drop is an ordinary next arc, while an Index already retained by the input Frame or transferred to Session or Handoff does not need an extra Drop hop.

Buffer and frame pools use one concrete `Index` with private pool, slot, and generation fields. Pool IDs never reuse, generation exhaustion retires a slot, and validation errors carry structured identity facts. Frame storage reuses the generic infrastructure vector and defines no collection or iterator implementation of its own.

Handoff remains the only cross-worker ownership path. Feature control state may use target node identities while compiling a chain, but the feature packet path carries only configuration progress and current-node-local next values.
