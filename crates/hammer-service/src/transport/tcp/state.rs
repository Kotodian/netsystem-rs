use super::{TcpInputError, TcpInputNext};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionId, TcpConnectionKey, TcpControlPlaneAction, TcpListenerId,
    TcpListenerKey, TcpNegotiatedOptions, TcpState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TcpCongestionAlgorithm {
    Bbr,
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
        if !matches!(selected, TcpCongestionAlgorithm::Bbr) {
            return Err(HammerError::config_validation(format!(
                "tcp congestion algorithm {selected:?} is not implemented in Hammer TCP nodes; only the Hammer-owned congestion controller is currently supported"
            )));
        }
        Ok(selected)
    }
}

impl Default for TcpCongestionRegistry {
    fn default() -> Self {
        Self::new(TcpCongestionAlgorithm::Bbr)
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

    #[inline]
    pub fn install_listener_action(
        &self,
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
    ) -> TcpControlPlaneAction {
        let _ = self;
        TcpControlPlaneAction::InstallListener {
            listener_id,
            listener,
            capabilities: TcpCapabilities::default(),
        }
    }

    #[inline]
    pub fn install_connection_action(
        &self,
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: TcpState,
    ) -> TcpControlPlaneAction {
        let _ = self;
        TcpControlPlaneAction::InstallConnection {
            connection_id,
            key,
            state,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        }
    }

    #[inline]
    pub fn upsert_connection_action(
        &self,
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: TcpState,
    ) -> TcpControlPlaneAction {
        let _ = self;
        TcpControlPlaneAction::UpsertConnectionState {
            connection_id,
            key,
            state,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpListenerConfig {
    congestion_algorithm: Option<TcpCongestionAlgorithm>,
}

impl TcpListenerConfig {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_congestion_algorithm(mut self, algorithm: TcpCongestionAlgorithm) -> Self {
        self.congestion_algorithm = Some(algorithm);
        self
    }

    #[inline]
    pub fn congestion_algorithm(self) -> Option<TcpCongestionAlgorithm> {
        self.congestion_algorithm
    }

    #[inline]
    pub fn install_listener_action(
        self,
        registry: &TcpCongestionRegistry,
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
    ) -> HammerResult<TcpControlPlaneAction> {
        Ok(
            TcpConnectionState::new(registry, self.congestion_algorithm())?
                .install_listener_action(listener_id, listener),
        )
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

impl TcpInputFlags {
    pub const TABLE_LEN: usize = 16;

    #[inline]
    pub fn table_index(self) -> usize {
        let bits = self.bits();
        let fin = usize::from(bits & Self::FIN.bits() != 0);
        let syn = usize::from(bits & Self::SYN.bits() != 0) << 1;
        let rst = usize::from(bits & Self::RST.bits() != 0) << 2;
        let ack = usize::from(bits & Self::ACK.bits() != 0) << 3;
        fin | syn | rst | ack
    }

    #[inline]
    fn from_table_index(index: usize) -> Self {
        let mut flags = Self::empty();
        if index & 0x01 != 0 {
            flags |= Self::FIN;
        }
        if index & 0x02 != 0 {
            flags |= Self::SYN;
        }
        if index & 0x04 != 0 {
            flags |= Self::RST;
        }
        if index & 0x08 != 0 {
            flags |= Self::ACK;
        }
        flags
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpDispatchEntry {
    pub next: TcpInputNext,
    pub error: Option<TcpInputError>,
}

impl TcpDispatchEntry {
    #[inline]
    pub const fn new(next: TcpInputNext, error: Option<TcpInputError>) -> Self {
        Self { next, error }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpDispatchTable {
    entries: [[TcpDispatchEntry; TcpInputFlags::TABLE_LEN]; TcpState::COUNT],
}

impl TcpDispatchTable {
    #[inline]
    pub fn entry(&self, state: TcpState, flags: TcpInputFlags) -> TcpDispatchEntry {
        self.entries[state.index()][flags.table_index()]
    }

    #[inline]
    fn set(&mut self, state: TcpState, flags: TcpInputFlags, entry: TcpDispatchEntry) {
        self.entries[state.index()][flags.table_index()] = entry;
    }

    #[inline]
    fn fill_row(&mut self, state: TcpState, entry: TcpDispatchEntry) {
        for index in 0..TcpInputFlags::TABLE_LEN {
            self.set(state, TcpInputFlags::from_table_index(index), entry);
        }
    }

    #[inline]
    fn set_row_when_contains(
        &mut self,
        state: TcpState,
        mask: TcpInputFlags,
        entry: TcpDispatchEntry,
    ) {
        for index in 0..TcpInputFlags::TABLE_LEN {
            let flags = TcpInputFlags::from_table_index(index);
            if flags.intersects(mask) {
                self.set(state, flags, entry);
            }
        }
    }
}

impl Default for TcpDispatchTable {
    fn default() -> Self {
        let default_entry = TcpDispatchEntry::new(TcpInputNext::Punt, None);
        let mut table = Self {
            entries: [[default_entry; TcpInputFlags::TABLE_LEN]; TcpState::COUNT],
        };

        table.set(
            TcpState::Listen,
            TcpInputFlags::SYN,
            TcpDispatchEntry::new(TcpInputNext::Listen, None),
        );
        table.set(
            TcpState::Listen,
            TcpInputFlags::ACK,
            TcpDispatchEntry::new(TcpInputNext::Reset, Some(TcpInputError::AckInvalid)),
        );
        table.set(
            TcpState::SynSent,
            TcpInputFlags::SYN | TcpInputFlags::ACK,
            TcpDispatchEntry::new(TcpInputNext::SynSent, None),
        );
        table.set(
            TcpState::SynRcvd,
            TcpInputFlags::ACK,
            TcpDispatchEntry::new(TcpInputNext::Listen, None),
        );
        table.set(
            TcpState::Established,
            TcpInputFlags::ACK,
            TcpDispatchEntry::new(TcpInputNext::Established, None),
        );
        table.set(
            TcpState::Established,
            TcpInputFlags::RST,
            TcpDispatchEntry::new(TcpInputNext::Established, None),
        );
        table.set(
            TcpState::Established,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpDispatchEntry::new(TcpInputNext::Established, None),
        );
        let established = TcpDispatchEntry::new(TcpInputNext::Established, None);
        let rcv_process = TcpDispatchEntry::new(TcpInputNext::RcvProcess, None);
        for state in [TcpState::FinWait1, TcpState::FinWait2] {
            table.fill_row(state, rcv_process);
            table.set_row_when_contains(
                state,
                TcpInputFlags::ACK | TcpInputFlags::RST | TcpInputFlags::FIN,
                established,
            );
        }
        table.fill_row(TcpState::CloseWait, rcv_process);
        table.set_row_when_contains(TcpState::CloseWait, TcpInputFlags::RST, established);
        table.fill_row(TcpState::Closing, rcv_process);
        table.set_row_when_contains(
            TcpState::Closing,
            TcpInputFlags::ACK | TcpInputFlags::RST | TcpInputFlags::FIN,
            established,
        );
        table.fill_row(TcpState::LastAck, rcv_process);
        table.set_row_when_contains(
            TcpState::LastAck,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            established,
        );
        table.fill_row(TcpState::TimeWait, rcv_process);
        let closed_reset =
            TcpDispatchEntry::new(TcpInputNext::Reset, Some(TcpInputError::ConnectionClosed));
        let closed_drop =
            TcpDispatchEntry::new(TcpInputNext::Drop, Some(TcpInputError::ConnectionClosed));
        table.fill_row(TcpState::Closed, closed_reset);
        for index in 0..TcpInputFlags::TABLE_LEN {
            let flags = TcpInputFlags::from_table_index(index);
            if flags.contains(TcpInputFlags::RST) {
                table.set(TcpState::Closed, flags, closed_drop);
            }
        }

        table
    }
}
