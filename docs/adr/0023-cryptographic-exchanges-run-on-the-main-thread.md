# Cryptographic Exchanges run on the Main Thread

Status: superseded by ADR 0026

Each protocol plugin owns and advances its own TLS, Noise, IKEv2, or future
cryptographic-exchange state machine on the Main Thread. The plugin defines the
concrete state enum and retains it in its own Main owner; Data Workers never
own or execute that state machine. `hammer-service::crypto` supplies algorithm
selection, Key Handles, and thread-bound Contexts for those transitions, but it
does not register, erase, retain, or dispatch protocol state. Receipt and
delivery of protocol messages remain outside crypto and the plugin does not
couple its state machine to Session, Packet Graph, or transport scheduling.
