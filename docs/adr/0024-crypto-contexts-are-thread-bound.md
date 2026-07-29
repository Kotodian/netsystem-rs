# Crypto Contexts are thread-bound

Status: superseded by ADR 0026

Every Crypto Context is created, executed, and destroyed on one execution thread: Exchange Contexts live on the Main Thread, while traffic-protection Contexts are created separately by each owning Data Worker. Key Handles may be published across threads and referenced by multiple thread-local Contexts, but implementation-private prepared state and Cryptographic Exchange state never migrate or cross threads; key destruction is rejected until all referencing Contexts are released. This preserves worker-local hot paths and supports per-thread software state and hardware sessions without exposing Runtime worker identities through the Crypto Engine interface.
