use hammer_core::error::{HammerError, HammerResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TcpCongestionAlgorithm {
    Hammer,
    Cubic,
    Reno,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionRegistry {
    default_algorithm: TcpCongestionAlgorithm,
}

impl TcpCongestionRegistry {
    #[inline]
    pub fn new(default_algorithm: TcpCongestionAlgorithm) -> Self {
        Self { default_algorithm }
    }

    #[inline]
    pub fn default_algorithm(&self) -> TcpCongestionAlgorithm {
        self.default_algorithm
    }

    #[inline]
    pub fn selected_algorithm(
        &self,
        algorithm: Option<TcpCongestionAlgorithm>,
    ) -> HammerResult<TcpCongestionAlgorithm> {
        let selected = algorithm.unwrap_or(self.default_algorithm);
        if !matches!(selected, TcpCongestionAlgorithm::Hammer) {
            return Err(HammerError::config_validation(format!(
                "tcp congestion algorithm {selected:?} is not implemented in Hammer TCP nodes; only the Hammer-owned congestion controller is currently supported"
            )));
        }
        Ok(selected)
    }
}

impl Default for TcpCongestionRegistry {
    fn default() -> Self {
        Self::new(TcpCongestionAlgorithm::Hammer)
    }
}

#[derive(Debug)]
pub struct TcpConnectionState {
    selected_algorithm: TcpCongestionAlgorithm,
}

impl TcpConnectionState {
    #[inline]
    pub fn new(
        registry: &TcpCongestionRegistry,
        algorithm: Option<TcpCongestionAlgorithm>,
    ) -> HammerResult<Self> {
        Ok(Self {
            selected_algorithm: registry.selected_algorithm(algorithm)?,
        })
    }

    #[inline]
    pub fn selected_congestion_algorithm(&self) -> TcpCongestionAlgorithm {
        self.selected_algorithm
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpInputFlags: u8 {
        const FIN = 0x01;
        const SYN = 0x02;
        const RST = 0x04;
        const ACK = 0x10;
    }
}
