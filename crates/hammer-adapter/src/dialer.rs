use std::fmt;

/// Mirror of Go's `network` enum used by Outbound.Network() / Listen* paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Network {
    #[default]
    Tcp,
    Udp,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Network::Tcp => "tcp",
            Network::Udp => "udp",
        })
    }
}

/// Marker for dial-capable runtime components.
pub trait Dialer: Send + Sync + 'static {}
