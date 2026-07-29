# Cryptographic algorithms stay below Engine policy

Status: superseded by ADR 0026

`hammer-infra` owns protocol-neutral algorithm semantics plus the standard portable and instruction-accelerated functions, while `hammer-service` owns the Crypto Engine, registries, selection policy, Key Handles, Crypto Contexts, and the execution interface used by hardware implementations. Built-in registrations reference `hammer-infra` function tables directly rather than introducing an Adapter wrapper type; plugins submit algorithm, implementation, and Exchange Protocol registrations to `hammer-service` as failure-atomic bundles, and `hammer-runtime` remains unaware of cryptographic types. This preserves the existing dependency direction without forcing software raw-key functions and non-exportable hardware sessions behind a false lower-layer abstraction.
