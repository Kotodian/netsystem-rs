# Cryptographic algorithms use an open canonical registry

Status: superseded by ADR 0026

Hammer reserves unqualified lowercase ASCII kebab-case Algorithm Names for the `hammer-infra` standard catalog, while plugins may register implementations for those algorithms and new semantics under `<plugin>:<algorithm>`. Successful registrations resolve names to compact process-local Algorithm IDs; Crypto Implementation Names and execution capabilities remain separate, protocol adapters privately map wire codepoints, aliases are not engine identities, and any collision or semantic conflict rejects the plugin registration without changing the existing registry. This avoids both a closed protocol-coupled algorithm enum and VPP's proliferation of algorithm names for implementation-specific AAD, tag, chaining, and buffer shapes.
