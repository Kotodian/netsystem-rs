// Handler trait surface mirrors `adapter/handler.go`. Concrete signatures land
// alongside the data plane in M5/M7; this file exists today so other modules
// can reference `crate::handler::ConnectionHandler` etc. as soon as they
// appear.

pub trait ConnectionHandler: Send + Sync + 'static {}

pub trait PacketConnectionHandler: Send + Sync + 'static {}
