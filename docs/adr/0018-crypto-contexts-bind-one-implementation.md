# Crypto Contexts bind one implementation

Status: superseded by ADR 0026

Each Crypto Context selects and permanently binds one cryptographic implementation when it is created; priority or configuration changes affect only new contexts. Unlike VPP's raw-key-backed context migration, Hammer cannot safely rebuild a context on another implementation when a Key Handle may refer to non-exportable hardware material, so implementation loss is reported as a typed failure rather than triggering silent fallback.
