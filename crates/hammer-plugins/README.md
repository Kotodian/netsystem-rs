# Hammer plugins

Device plugins live under `device/`. The `tuntap` plugin currently owns only
Linux TUN/TAP control-plane creation and process-exit deletion; packet nodes,
FileMain integration, punt/inject, and address synchronization are deferred.

Layout follows domain ownership — **not** a flat list of every name:

```text
hammer-plugins/
  net/ip/           # hammer-plugin-ip
  transport/       # L4 protocols (abstraction stays in hammer-service::transport)
    tcp/           # hammer-plugin-tcp
    udp/           # hammer-plugin-udp
```

Not plugins (shared rlib in `hammer-service`): `device`, `interface`, `transport`, `session`.
