# Crypto keeps a generic core in the owning plugin

The primitive core uses the closed `Family` markers `Aead`, `Cipher`, `Hash`,
`Mac`, `Kdf`, `Kx`, `Sign`, and `Verify` to parameterize `AlgorithmId<F>`,
`Context<F>`, and `Batch<'_, F>`, while plugins keep the algorithm and
implementation sets open within each family. The exchange core uses
`exchange::Protocol<C>` with protocol-owned parameter, state,
established-result, and error types, represented by `exchange::Exchange<P, C>`
and `exchange::Transition<S, E>`. A plugin monomorphizes its concrete protocol
inside its own link image and retains the concrete exchange state in its own
Main owner. No exchange state, trait object, erased callback table, or state
handle crosses the plugin ABI. Primitive execution performs one indirect call
per batch rather than per operation, and no Adapter wrapper type is introduced.
