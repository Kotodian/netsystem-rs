use std::io::{self, Read};

use hammer_infra::svm::fifo::Fifo;
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub(crate) const COOKIE_SIZE: usize = 37;
pub(crate) const MAX_STREAMS: u32 = 128;
const CONTROL_STATE_SIZE: usize = 1;
const PARAMETER_LENGTH_SIZE: usize = 4;
const MAX_PARAMETER_SIZE: usize = 8 * 1024;
const MAX_RESULTS_SIZE: usize = 256 * 1024;

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
    Done = 16,
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
            16 => Ok(Self::Done),
            state => Err(Iperf3ProtocolError::StateUnsupported { state }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPhase {
    Cookie,
    Parameters,
    Running,
    Results,
    Done,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlAction {
    SendState(ControlState),
    Parameters(ControlParameters),
    Results(Vec<u32>),
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

#[derive(serde::Deserialize)]
struct ClientResults {
    cpu_util_total: f64,
    cpu_util_user: f64,
    cpu_util_system: f64,
    sender_has_retransmits: i32,
    streams: Vec<ClientStreamResult>,
}

#[derive(serde::Deserialize)]
struct ClientStreamResult {
    id: u32,
    bytes: u64,
    retransmits: i64,
    jitter: f64,
    errors: u64,
    packets: u64,
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
    #[error("iperf3 stream count {count} is outside 1..={MAX_STREAMS}")]
    StreamCountInvalid { count: u32 },
    #[error("iperf3 client results are invalid")]
    ResultsInvalid {
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
                let count = parameters.parallel.unwrap_or(1);
                if !(1..=MAX_STREAMS).contains(&count) {
                    return Err(Iperf3ProtocolError::StreamCountInvalid { count });
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
                    | ControlState::DisplayResults
                    | ControlState::Done => {
                        return Err(Iperf3ProtocolError::StateUnsupported {
                            state: record.value,
                        });
                    }
                };
                Ok(Some((action, CONTROL_STATE_SIZE)))
            }
            ControlPhase::Results => {
                if rx.max_dequeue() < PARAMETER_LENGTH_SIZE {
                    return Ok(None);
                }
                let mut header = [0; PARAMETER_LENGTH_SIZE];
                assert_eq!(rx.peek(0, PARAMETER_LENGTH_SIZE, &mut header), header.len());
                let length = ParameterLength::ref_from_bytes(&header)
                    .expect("fixed result length has its declared layout")
                    .value();
                if length == 0 || length > MAX_RESULTS_SIZE {
                    return Ok(Some((ControlAction::Close, PARAMETER_LENGTH_SIZE)));
                }
                if rx.max_dequeue() < PARAMETER_LENGTH_SIZE + length {
                    return Ok(None);
                }
                // ESnet iperf_api.c:2403-2420,2975-3100: the server reads
                // client results before sending its own framed results.
                let input = ParameterInput {
                    fifo: rx,
                    offset: PARAMETER_LENGTH_SIZE,
                    remaining: length,
                };
                let results: ClientResults = serde_json::from_reader(input)
                    .map_err(|source| Iperf3ProtocolError::ResultsInvalid { source })?;
                if !results.cpu_util_total.is_finite()
                    || !results.cpu_util_user.is_finite()
                    || !results.cpu_util_system.is_finite()
                    || !(-1..=1).contains(&results.sender_has_retransmits)
                    || results.streams.is_empty()
                    || results.streams.len() > MAX_STREAMS as usize
                    || results.streams.iter().any(|stream| {
                        stream.id == 0
                            || stream.retransmits < -1
                            || !stream.jitter.is_finite()
                            || stream.jitter < 0.0
                            || stream.bytes > i64::MAX as u64
                            || stream.errors > i64::MAX as u64
                            || stream.packets > i64::MAX as u64
                    })
                {
                    return Ok(Some((ControlAction::Close, PARAMETER_LENGTH_SIZE + length)));
                }
                let stream_ids = results
                    .streams
                    .into_iter()
                    .map(|stream| stream.id)
                    .collect();
                Ok(Some((
                    ControlAction::Results(stream_ids),
                    PARAMETER_LENGTH_SIZE + length,
                )))
            }
            ControlPhase::Done => {
                if rx.max_dequeue() < CONTROL_STATE_SIZE {
                    return Ok(None);
                }
                let mut state = [0; CONTROL_STATE_SIZE];
                assert_eq!(rx.peek(0, CONTROL_STATE_SIZE, &mut state), state.len());
                match ControlState::try_from(state[0])? {
                    ControlState::Done | ControlState::ClientTerminate => {
                        Ok(Some((ControlAction::Close, CONTROL_STATE_SIZE)))
                    }
                    _ => Err(Iperf3ProtocolError::StateUnsupported { state: state[0] }),
                }
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
            (ControlPhase::Running, ControlAction::SendState(ControlState::ExchangeResults)) => {
                ControlPhase::Results
            }
            (ControlPhase::Results, ControlAction::Results(_)) => ControlPhase::Done,
            (_, ControlAction::Close) => ControlPhase::Finished,
            (phase, _) => phase,
        };
    }

    #[inline(always)]
    pub const fn phase(&self) -> ControlPhase {
        self.phase
    }

    #[inline(always)]
    pub fn state_bytes(value: ControlState) -> [u8; CONTROL_STATE_SIZE] {
        [value as u8]
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
