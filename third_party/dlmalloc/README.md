# dlmalloc mspace backend

This directory contains the public-domain dlmalloc 2.8.6 source used by the
vendored VPP baseline:

- `third_party/vpp` commit `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- `src/vppinfra/dlmalloc.c`
- `src/vppinfra/dlmalloc.h`

The extraction keeps VPP's `ONLY_MSPACES`, `MSPACES`, `USE_LOCKS`, supplied-base
mspace, heap-ownership, disable-expand, and tracing-compatible entry points. It
does not compile `mem_dlmalloc.c` or expose process-global `malloc`/`free`.

Local differences from the VPP copies are limited to build independence:

- Replace the VPP `clib.h`/`cache.h` includes with the standard headers needed by
  dlmalloc.
- Provide the `uword`, `clib_max`, `max_pow2`, and `__clib_nosanitize_addr`
  compatibility definitions used by VPP's dlmalloc additions.
- Replace VPP's `os_panic` abort hook with `abort()`.
- Define hidden no-op `clib_mem_trace_get`/`clib_mem_trace_put` entry points in
  `dlmalloc.c`; Hammer does not enable VPP's heap tracing path.

`LICENSE` is copied from the vendored VPP tree. dlmalloc itself is dedicated to
the public domain as stated in the source headers.
