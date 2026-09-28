use std::io::{self, Read};

use hammer_infra::svm::fifo::Fifo;
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

const COOKIE_SIZE: usize = 37;
const CONTROL_STATE_SIZE: usize = 1;
const PARAMETER_LENGTH_SIZE: usize = 4;
const MAX_PARAMETER_SIZE: usize = 8 * 1024;

#[repr(C)]
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable, IntoBytes)]
struct StateRecord {
    value: u8,
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable, IntoBytes)]
struct ParameterLength {
    bytes: [u8; PARAMETER_LENGTH_SIZE],
}

impl ParameterLength {
    #[inline]
    fn value(self) -> usize {
        u32::from_be_bytes(self.bytes) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ControlState {
    TestStart = 1,
    TestRunning = 2,
    TestEnd = 4,
    ClientTerminate = 12,
    ParameterExchange = 9,
    CreateStreams = 10,
    ExchangeResults = 13,
    DisplayResults = 14,
}

impl TryFrom<u8> for ControlState {
    type Error = Iperf3ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::TestStart),
            2 => Ok(Self::TestRunning),
            4 => Ok(Self::TestEnd),
            12 => Ok(Self::ClientTerminate),
            9 => Ok(Self::ParameterExchange),
            10 => Ok(Self::CreateStreams),
            13 => Ok(Self::ExchangeResults),
            14 => Ok(Self::DisplayResults),
            state => Err(Iperf3ProtocolError::StateUnsupported { state }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPhase {
    Cookie,
    Parameters,
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlAction {
    SendState(ControlState),
    Parameters(ControlParameters),
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct ControlParameters {
    pub tcp: bool,
    #[serde(default)]
    pub udp: bool,
    #[serde(default)]
    pub sctp: bool,
    #[serde(default)]
    pub parallel: Option<u32>,
    #[serde(default, rename = "time")]
    pub duration: Option<u32>,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub bidirectional: bool,
}

#[derive(Debug, Error)]
pub enum Iperf3ProtocolError {
    #[error("iperf3 cookie is not ASCII")]
    CookieInvalid,
    #[error("iperf3 control state {state} is unsupported")]
    StateUnsupported { state: u8 },
    #[error("iperf3 parameter length {length} is invalid")]
    ParameterLengthInvalid { length: usize },
    #[error("iperf3 parameters do not select TCP")]
    TcpRequired,
    #[error("iperf3 parameters are invalid")]
    ParametersInvalid {
        #[source]
        source: serde_json::Error,
    },
}

pub struct ControlParser {
    phase: ControlPhase,
}

impl ControlParser {
    pub const fn new() -> Self {
        Self {
            phase: ControlPhase::Cookie,
        }
    }

    /// Inspect one complete command without changing the FIFO head or parser
    /// phase.  The caller commits both only after its response capacity check.
    // VPP: vperf_protos.c:58-127; the RX path computes availability before
    // app_recv_stream and only then advances the FIFO.
    #[inline(always)]
    pub fn inspect(
        phase: ControlPhase,
        rx: &Fifo,
    ) -> Result<Option<(ControlAction, usize)>, Iperf3ProtocolError> {
        match phase {
            ControlPhase::Cookie => {
                if rx.max_dequeue() < COOKIE_SIZE {
                    return Ok(None);
                }
                let mut cookie = [0; COOKIE_SIZE];
                assert_eq!(
                    rx.peek(0, COOKIE_SIZE, &mut cookie),
                    COOKIE_SIZE,
                    "complete cookie remains readable in the RX FIFO"
                );
                if !cookie.iter().all(u8::is_ascii) {
                    return Err(Iperf3ProtocolError::CookieInvalid);
                }
                Ok(Some((
                    ControlAction::SendState(ControlState::ParameterExchange),
                    COOKIE_SIZE,
                )))
            }
            ControlPhase::Parameters => {
                if rx.max_dequeue() < PARAMETER_LENGTH_SIZE {
                    return Ok(None);
                }
                let mut header = [0; PARAMETER_LENGTH_SIZE];
                assert_eq!(
                    rx.peek(0, PARAMETER_LENGTH_SIZE, &mut header),
                    PARAMETER_LENGTH_SIZE,
                    "available parameter length spans readable FIFO chunks"
                );
                let length = ParameterLength::ref_from_bytes(&header)
                    .expect("fixed parameter length has its declared layout")
                    .value();
                if length == 0 || length > MAX_PARAMETER_SIZE {
                    return Err(Iperf3ProtocolError::ParameterLengthInvalid { length });
                }
                if rx.max_dequeue() < PARAMETER_LENGTH_SIZE + length {
                    return Ok(None);
                }
                let input = ParameterInput {
                    fifo: rx,
                    offset: PARAMETER_LENGTH_SIZE,
                    remaining: length,
                };
                let parameters: ControlParameters = serde_json::from_reader(input)
                    .map_err(|source| Iperf3ProtocolError::ParametersInvalid { source })?;
                if !parameters.tcp || parameters.udp || parameters.sctp {
                    return Err(Iperf3ProtocolError::TcpRequired);
                }
                Ok(Some((
                    ControlAction::Parameters(parameters),
                    PARAMETER_LENGTH_SIZE + length,
                )))
            }
            ControlPhase::Running => {
                if rx.max_dequeue() < CONTROL_STATE_SIZE {
                    return Ok(None);
                }
                let mut state_bytes = [0; CONTROL_STATE_SIZE];
                assert_eq!(
                    rx.peek(0, CONTROL_STATE_SIZE, &mut state_bytes),
                    CONTROL_STATE_SIZE,
                    "complete control state remains readable in the RX FIFO"
                );
                let record = StateRecord::ref_from_bytes(&state_bytes)
                    .expect("fixed control state has its declared layout");
                let state = ControlState::try_from(record.value)?;
                let action = match state {
                    ControlState::TestEnd => {
                        ControlAction::SendState(ControlState::ExchangeResults)
                    }
                    ControlState::ClientTerminate => ControlAction::Close,
                    ControlState::TestStart
                    | ControlState::TestRunning
                    | ControlState::ParameterExchange
                    | ControlState::CreateStreams
                    | ControlState::ExchangeResults
                    | ControlState::DisplayResults => {
                        return Err(Iperf3ProtocolError::StateUnsupported {
                            state: record.value,
                        });
                    }
                };
                Ok(Some((action, CONTROL_STATE_SIZE)))
            }
            ControlPhase::Finished => Ok(None),
        }
    }

    /// Commit the parser phase after the caller has consumed the inspected
    /// bytes.  No FIFO operation belongs here.
    // VPP: vperf_protos.c:102-122; parser state follows successful receive.
    #[inline(always)]
    pub fn commit(&mut self, action: &ControlAction) {
        self.phase = match (self.phase, action) {
            (ControlPhase::Cookie, ControlAction::SendState(ControlState::ParameterExchange)) => {
                ControlPhase::Parameters
            }
            (ControlPhase::Parameters, ControlAction::Parameters(_)) => ControlPhase::Running,
            (ControlPhase::Running, ControlAction::SendState(ControlState::ExchangeResults))
            | (ControlPhase::Running, ControlAction::Close) => ControlPhase::Finished,
            (phase, _) => phase,
        };
    }

    #[inline(always)]
    pub const fn phase(&self) -> ControlPhase {
        self.phase
    }

    #[inline(always)]
    pub fn state_bytes(value: ControlState) -> [u8; CONTROL_STATE_SIZE] {
        StateRecord { value: value as u8 }.to_bytes()
    }
}

// serde_json reads only the declared frame body. FIFO head advances after
// successful parsing, so a malformed or partial frame leaves it unchanged.
struct ParameterInput<'a> {
    fifo: &'a Fifo,
    offset: usize,
    remaining: usize,
}

impl Read for ParameterInput<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = output.len().min(self.remaining);
        if count == 0 {
            return Ok(0);
        }
        let read = self.fifo.peek(self.offset, count, output);
        if read == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        self.offset += read;
        self.remaining -= read;
        Ok(read)
    }
}
