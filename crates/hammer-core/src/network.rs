use std::fmt;

/// Mirror of Go's `network` enum used by Outbound.Network() / Listen* paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Network {
    #[default]
    Tcp,
    Udp,
    Icmp,
}

impl Network {
    /// Stable string label suitable for metric tags and diagnostic
    /// formatting; matches the [`Display`] output.
    pub fn as_str(self) -> &'static str {
        match self {
            Network::Tcp => "tcp",
            Network::Udp => "udp",
            Network::Icmp => "icmp",
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
