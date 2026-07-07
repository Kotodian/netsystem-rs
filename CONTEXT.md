# Hammer Data Plane

Hammer is a VPP-style packet graph runtime in Rust. This context defines the graph, frame, buffer, and memory ownership language used across the data plane.

## Language

**Next Frame**:
A Hammer-owned frame prepared by the current graph node for a selected VPP next arc. In code, `Frame<Next>` is this owner; it is not a plain frame plus an external node parameter, and it is put with `put_next_frame`.
_Avoid_: submit frame, `submit_frame`, `NextFrame` carrier, node-attached frame

**Frame State**:
The concrete state payload inside `Frame<S>`. `Next` and `Pending` are state bodies that own frame fields directly, not marker types and not tags for a separate storage enum.
_Avoid_: marker state, `PhantomData`, `FrameStorage`, `FrameOwner`

**Node Dispatch Result**:
The outcome of running a graph node over a Pending Frame. Next Frames are acquired and put during node execution; they are not carried back as the node dispatch result.
_Avoid_: `NodeNextFrames`, `NextFrame` carrier, `Current(NodeId)`, forwarding result carrier, node returns next frames

**TCP Dataplane Lookup**:
An exact-match packet-path lookup that routes a TCP packet tuple or listener endpoint to the existing session, pending open, or listener handling path.
_Avoid_: every TCP hash table, control-plane bookkeeping map, test helper index

**Bihash Value**:
The opaque `u64` stored in a dataplane bihash entry, usually a packed pool index or session handle whose target object is owned elsewhere.
_Avoid_: storing business records in bihash, public free-slot marker traits

**TCP Lookup Key**:
The existing TCP/session domain key used for dataplane exact-match lookup, with `BihashKey` hashing implemented on that key type and equality coming from Rust `Eq` instead of converting call sites to raw words.
_Avoid_: `TcpBihashKey`, `TcpV4RouteKey`, raw `[u64; N]` key plumbing in TCP code
