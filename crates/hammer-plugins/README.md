# Hammer plugins

Layout follows domain ownership — **not** a flat list of every name:

```text
hammer-plugins/
  device/          # device-class drivers (abstraction stays in hammer-service::device)
    tun/           # hammer-plugin-tun
  ip/              # hammer-plugin-ip
  transport/       # L4 protocols (abstraction stays in hammer-service::transport)
    tcp/           # hammer-plugin-tcp
    udp/           # hammer-plugin-udp
```

Not plugins (shared rlib in `hammer-service`): `device`, `interface`, `transport`, `session`.
