# Cryptographic Keys and Contexts have separate lifecycles

Status: superseded by ADR 0026

Hammer models a Key Handle as the identity, policy, provenance, and lifetime of key material, while a Crypto Context is implementation-bound prepared state for a permitted operation family using one or more keys. Keeping them separate lets long-lived and non-exportable keys serve multiple short-lived or per-worker contexts without coupling key destruction, rotation, or hardware ownership to one algorithm instance.
