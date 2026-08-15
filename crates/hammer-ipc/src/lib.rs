//! Shared Protobuf Binary API envelope, blocking client, and client-facing
//! errors. The daemon-side server (`hammer-service`) re-exports these and
//! keeps server ownership and Main Thread dispatch; external client
//! processes such as `hammerctl` use this module directly over a Unix
//! socket, mirroring VPP's separate vat2 client process.

pub mod binary_api;
