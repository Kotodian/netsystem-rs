use std::time::{Duration, Instant};

use bitflags::bitflags;

use super::controller::CongestionController;
use super::types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};

pub const DEFAULT_BBR_MAX_DATAGRAM_SIZE: u32 = 1_460;
const BBR_INITIAL_WINDOW_SEGMENTS: u32 = 10;
const BBR_MIN_WINDOW_SEGMENTS: u32 = 4;
const BBR_HIGH_GAIN_MILLI: u32 = 2770;
const BBR_DRAIN_GAIN_MILLI: u32 = 500;
const BBR_CWND_GAIN_MILLI: u32 = 2000;
const BBR_PROBE_UP_CWND_GAIN_MILLI: u32 = 2250;
const BBR_PROBE_RTT_CWND_GAIN_MILLI: u32 = 500;
const BBR_PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
const BBR_MIN_RTT_FILTER: Duration = Duration::from_secs(10);
const BBR_FULL_BANDWIDTH_GAIN_MILLI: u32 = 1250;
const BBR_STARTUP_FULL_LOSS_ROUNDS: u8 = 6;
const BBR_LOSS_THRESHOLD_NUMERATOR: u64 = 2;
const BBR_LOSS_THRESHOLD_DENOMINATOR: u64 = 100;
const BBR_BETA_NUMERATOR: u64 = 7;
const BBR_BETA_DENOMINATOR: u64 = 10;
const BBR_HEADROOM_NUMERATOR: u64 = 15;
const BBR_HEADROOM_DENOMINATOR: u64 = 100;
const BBR_MIN_RTT_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const BBR_PROBE_WAIT_BASE: Duration = Duration::from_secs(2);
const BBR_PROBE_WAIT_RANDOM: Duration = Duration::from_secs(1);
const BBR_SEND_QUANTUM_INTERVAL: Duration = Duration::from_millis(1);
const BBR_SEND_QUANTUM_MAX: u32 = 64 * 1024;
const BBR_OFFLOAD_BUDGET_INTERVAL: Duration = Duration::from_micros(130);
const BBR_OFFLOAD_QUANTA_MIN: u32 = 3;
const BBR_OFFLOAD_QUANTA_MAX: u32 = 16;
const BBR_INFLIGHT_INFINITY: u32 = u32::MAX;
const BBR_BW_INFINITY: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BbrMode {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BbrProbeBwPhase {
    Down,
    Cruise,
    Refill,
    Up,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BbrAckPhase {
    Init,
    Refilling,
    ProbeStarting,
    ProbeFeedback,
    ProbeStopping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BbrUndoState {
    None,
    Startup,
    ProbeUp,
}

bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct BbrFlags: u32 {
        const FULL_BW_REACHED = 1 << 0;
        const ROUND_START = 1 << 1;
        const LOSS_ROUND_START = 1 << 2;
        const LOSS_IN_ROUND = 1 << 3;
        const LOSS_ROUND_HAD_LOSS = 1 << 4;
        const LOSS_EVENT_PENDING = 1 << 5;
        const RECOVERY_IN_ROUND = 1 << 6;
        const IS_BW_PROBE_SAMPLE = 1 << 7;
        const PREV_PROBE_TOO_HIGH = 1 << 8;
        const PREV_PROBE_PRECAUTIONARY = 1 << 9;
        const PROBE_RTT_ROUND_DONE = 1 << 10;
        const IDLE_RESTART = 1 << 11;
        const HAS_SEEN_RTT = 1 << 12;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BbrMinmaxSample {
    round: u32,
    value: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BbrMinmax {
    samples: [BbrMinmaxSample; 3],
}

impl BbrMinmax {
    fn reset(&mut self, round: u32, value: u32) {
        let sample = BbrMinmaxSample { round, value };
        self.samples = [sample; 3];
    }

    fn value(&self) -> u32 {
        self.samples[0].value
    }

    fn running_max(&mut self, window: u32, round: u32, value: u32) -> u32 {
        let sample = BbrMinmaxSample { round, value };
        let current = self.samples[0];
        if value >= current.value || round.wrapping_sub(self.samples[2].round) > window {
            self.reset(round, value);
            return value;
        }
        if value >= self.samples[1].value {
            self.samples[1] = sample;
            self.samples[2] = sample;
        } else if value >= self.samples[2].value {
            self.samples[2] = sample;
        }
        self.subwindow_update(window, sample)
    }

    fn subwindow_update(&mut self, window: u32, sample: BbrMinmaxSample) -> u32 {
        let elapsed = sample.round.wrapping_sub(self.samples[0].round);
        if elapsed > window {
            self.samples[0] = self.samples[1];
            self.samples[1] = self.samples[2];
            self.samples[2] = sample;
            if sample.round.wrapping_sub(self.samples[0].round) > window {
                self.samples[0] = self.samples[1];
                self.samples[1] = self.samples[2];
                self.samples[2] = sample;
            }
        } else if self.samples[1].round == self.samples[0].round && elapsed > window / 4 {
            self.samples[1] = sample;
            self.samples[2] = sample;
        } else if self.samples[2].round == self.samples[1].round && elapsed > window / 2 {
            self.samples[2] = sample;
        }
        self.samples[0].value
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BbrController {
    extra_acked: BbrMinmax,
    bw_hi: [u64; 2],
    bw_lo: u64,
    bw_latest: u64,
    undo_bw_lo: u64,
    min_rtt: Option<Duration>,
    min_rtt_stamp: Option<Instant>,
    probe_rtt_min_delay: Option<Duration>,
    probe_rtt_min_stamp: Option<Instant>,
    probe_rtt_done_stamp: Option<Instant>,
    pacing_rate: Option<u64>,
    ack_epoch_stamp: Option<Instant>,
    cycle_stamp: Option<Instant>,
    bw_probe_wait: Option<Duration>,
    full_bw: u64,
    ack_epoch_acked: u64,
    next_round_delivered: u64,
    loss_round_delivered: u64,
    last_loss_counted: u64,
    inflight_hi: u32,
    inflight_lo: u32,
    inflight_latest: u32,
    undo_inflight_hi: u32,
    undo_inflight_lo: u32,
    prior_cwnd: u32,
    initial_cwnd: u32,
    round_count: u32,
    bw_probe_up_acked: u32,
    probe_up_acked_per_inc: u32,
    random_seed: u32,
    offload_budget: u32,
    flags: BbrFlags,
    offload_mss: u16,
    mode: BbrMode,
    probe_bw_phase: BbrProbeBwPhase,
    ack_phase: BbrAckPhase,
    undo_state: BbrUndoState,
    full_bw_count: u8,
    drain_rounds: u8,
    bw_probe_up_rounds: u8,
    loss_events: u8,
    rounds_since_probe_up: u8,
    max_datagram_size: u32,
    congestion_window: u32,
    delivered: u64,
}

impl BbrController {
    pub fn bbr_mode(&self) -> BbrMode {
        self.mode
    }

    #[inline]
    fn has_flag(&self, flag: BbrFlags) -> bool {
        self.flags.contains(flag)
    }

    #[inline]
    fn set_flag(&mut self, flag: BbrFlags) {
        self.flags.insert(flag);
    }

    #[inline]
    fn clear_flag(&mut self, flag: BbrFlags) {
        self.flags.remove(flag);
    }

    #[inline]
    fn round_start(&self) -> bool {
        self.has_flag(BbrFlags::ROUND_START)
    }

    #[inline]
    fn full_bw_reached(&self) -> bool {
        self.has_flag(BbrFlags::FULL_BW_REACHED)
    }

    #[inline]
    fn max_bw(&self) -> u64 {
        self.bw_hi[0].max(self.bw_hi[1])
    }

    #[inline]
    fn bw(&self) -> u64 {
        self.max_bw().min(self.bw_lo)
    }

    fn ack_sample(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample) -> AckSample {
        AckSample {
            bytes_acked: acked.bytes,
            delivered: acked.delivered,
            prior_delivered: acked.prior_delivered,
            interval: acked.interval,
            tx_in_flight: acked.tx_in_flight,
            tx_lost: acked.tx_lost,
            lost: acked.lost,
            rtt: rtt.latest,
            now,
        }
    }

    fn apply_ack_sample(&mut self, sample: AckSample, bytes_in_flight: u32, app_limited: bool) {
        if sample.bytes_acked == 0 {
            return;
        }

        self.clear_flag(
            BbrFlags::ROUND_START | BbrFlags::LOSS_ROUND_START | BbrFlags::LOSS_ROUND_HAD_LOSS,
        );
        if sample.prior_delivered >= self.next_round_delivered {
            self.set_flag(BbrFlags::ROUND_START);
            self.round_count = self.round_count.wrapping_add(1);
            self.rounds_since_probe_up = self.rounds_since_probe_up.saturating_add(1).min(63);
            if self.mode == BbrMode::Drain {
                self.drain_rounds = self.drain_rounds.saturating_add(1);
            }
        }
        self.delivered = self
            .delivered
            .max(sample.prior_delivered.saturating_add(sample.delivered));
        if self.round_start() {
            self.next_round_delivered = self.delivered;
        }

        if sample.rtt.is_zero() {
            return;
        }

        self.maybe_enter_probe_rtt(sample.now);
        self.update_min_rtt(sample);
        self.update_bandwidth(sample, app_limited);
        self.update_ack_aggregation(sample);
        self.update_loss_round(sample);
        if self.ack_phase == BbrAckPhase::ProbeStarting && self.round_start() {
            self.ack_phase = BbrAckPhase::ProbeFeedback;
        }
        let probe_transitioned = self.adapt_long_term_model(sample, app_limited);

        match self.mode {
            BbrMode::Startup => self.update_startup(sample, app_limited),
            BbrMode::Drain => self.update_drain(bytes_in_flight),
            BbrMode::ProbeBw if !probe_transitioned => self.update_probe_bw(sample),
            BbrMode::ProbeRtt => self.update_probe_rtt(sample, bytes_in_flight),
            BbrMode::ProbeBw => {}
        }

        self.update_pacing_rate();
    }

    fn update_min_rtt(&mut self, sample: AckSample) {
        if sample.rtt.is_zero() {
            return;
        }
        let probe_expired = self.probe_rtt_min_stamp.is_none_or(|stamp| {
            sample.now.saturating_duration_since(stamp) > BBR_MIN_RTT_PROBE_INTERVAL
        });
        if self
            .probe_rtt_min_delay
            .is_none_or(|min_rtt| sample.rtt <= min_rtt)
            || probe_expired
        {
            self.probe_rtt_min_delay = Some(sample.rtt);
            self.probe_rtt_min_stamp = Some(sample.now);
        }
        let filter_expired = self
            .min_rtt_stamp
            .is_none_or(|stamp| sample.now.saturating_duration_since(stamp) > BBR_MIN_RTT_FILTER);
        if self.probe_rtt_min_delay.is_some_and(|delay| {
            self.min_rtt.is_none_or(|min_rtt| delay <= min_rtt) || filter_expired
        }) {
            self.min_rtt = self.probe_rtt_min_delay;
            self.min_rtt_stamp = self.probe_rtt_min_stamp;
        }
    }

    fn update_bandwidth(&mut self, sample: AckSample, app_limited: bool) {
        if sample.delivered == 0 || sample.interval.is_zero() {
            return;
        }
        let micros = sample.interval.as_micros().max(1);
        let sample_rate = ((u128::from(sample.delivered) * 1_000_000u128) / micros)
            .min(u128::from(u64::MAX)) as u64;
        let max_bw = self.max_bw();
        if app_limited && max_bw != 0 && sample_rate <= max_bw {
            return;
        }
        self.bw_hi[1] = self.bw_hi[1].max(sample_rate);
        self.bw_latest = self.bw_latest.max(sample_rate);
        self.inflight_latest = self
            .inflight_latest
            .max(sample.delivered.min(u64::from(u32::MAX)) as u32);
    }

    fn update_ack_aggregation(&mut self, sample: AckSample) {
        if sample.bytes_acked == 0 || self.bw() == 0 {
            return;
        }
        let now = sample.now;
        let epoch = self.ack_epoch_stamp.get_or_insert(now);
        let elapsed = now.saturating_duration_since(*epoch);
        let expected = u128::from(self.bw()).saturating_mul(elapsed.as_nanos()) / 1_000_000_000u128;
        if u128::from(self.ack_epoch_acked) <= expected {
            self.ack_epoch_acked = 0;
            *epoch = now;
        }
        self.ack_epoch_acked = self
            .ack_epoch_acked
            .saturating_add(u64::from(sample.bytes_acked));
        let expected = u64::try_from(expected).unwrap_or(u64::MAX);
        let extra = self
            .ack_epoch_acked
            .saturating_sub(expected)
            .min(u64::from(self.congestion_window));
        let window = if self.full_bw_reached() { 10 } else { 1 };
        self.extra_acked.running_max(
            window,
            self.round_count,
            extra.min(u64::from(u32::MAX)) as u32,
        );
    }

    fn update_loss_round(&mut self, sample: AckSample) {
        let total_loss = sample.tx_lost.saturating_add(sample.lost);
        if sample.lost != 0 && total_loss != self.last_loss_counted {
            if !self.has_flag(BbrFlags::LOSS_IN_ROUND) {
                self.loss_round_delivered = self.delivered;
                self.prior_cwnd = self.prior_cwnd.max(self.congestion_window);
            }
            self.last_loss_counted = total_loss;
            self.set_flag(BbrFlags::LOSS_IN_ROUND | BbrFlags::LOSS_EVENT_PENDING);
            if self.mode == BbrMode::Startup {
                self.loss_events = self.loss_events.saturating_add(1);
                if self.loss_events >= BBR_STARTUP_FULL_LOSS_ROUNDS {
                    self.set_flag(BbrFlags::FULL_BW_REACHED);
                    self.undo_state = BbrUndoState::Startup;
                    self.inflight_hi = self.inflight_latest.max(self.congestion_window);
                    self.mode = BbrMode::Drain;
                }
            }
            if self.has_flag(BbrFlags::IS_BW_PROBE_SAMPLE)
                && sample.tx_in_flight != 0
                && sample.lost.saturating_mul(BBR_LOSS_THRESHOLD_DENOMINATOR)
                    > sample
                        .tx_in_flight
                        .saturating_mul(BBR_LOSS_THRESHOLD_NUMERATOR)
            {
                self.set_flag(BbrFlags::PREV_PROBE_TOO_HIGH);
                self.inflight_hi = self
                    .inflight_hi
                    .max(sample.tx_in_flight.min(u64::from(BBR_INFLIGHT_INFINITY)) as u32);
            }
        }
        if self.has_flag(BbrFlags::LOSS_IN_ROUND)
            && sample.prior_delivered >= self.loss_round_delivered
        {
            self.set_flag(BbrFlags::LOSS_ROUND_START | BbrFlags::LOSS_ROUND_HAD_LOSS);
            self.loss_round_delivered = self.delivered;
            if self.bw_lo == BBR_BW_INFINITY {
                self.bw_lo = self.max_bw();
            }
            if self.inflight_lo == BBR_INFLIGHT_INFINITY {
                self.inflight_lo = self.congestion_window;
            }
            self.bw_lo = self
                .bw_latest
                .max(self.bw_lo.saturating_mul(BBR_BETA_NUMERATOR) / BBR_BETA_DENOMINATOR);
            self.inflight_lo = self.inflight_latest.max(
                self.inflight_lo.saturating_mul(BBR_BETA_NUMERATOR as u32)
                    / BBR_BETA_DENOMINATOR as u32,
            );
            self.bw_latest = 0;
            self.inflight_latest = 0;
            self.clear_flag(BbrFlags::LOSS_IN_ROUND);
        }
    }

    fn start_probe_bw_down(&mut self, now: Instant) {
        self.reset_congestion_signals();
        self.clear_flag(BbrFlags::IS_BW_PROBE_SAMPLE);
        self.probe_up_acked_per_inc = BBR_INFLIGHT_INFINITY;
        self.random_seed = self
            .random_seed
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        let jitter = self.random_seed % 1_000_000_000;
        self.bw_probe_wait = Some(
            BBR_PROBE_WAIT_BASE + BBR_PROBE_WAIT_RANDOM.mul_f64(jitter as f64 / 1_000_000_000.0),
        );
        self.cycle_stamp = Some(now);
        self.ack_phase = BbrAckPhase::ProbeStopping;
        self.mode = BbrMode::ProbeBw;
        self.probe_bw_phase = BbrProbeBwPhase::Down;
    }

    fn start_probe_bw_refill(&mut self) {
        self.reset_short_term_model();
        self.bw_probe_up_rounds = 0;
        self.bw_probe_up_acked = 0;
        self.rounds_since_probe_up = 0;
        self.clear_flag(BbrFlags::PREV_PROBE_PRECAUTIONARY);
        self.ack_phase = BbrAckPhase::Refilling;
        self.mode = BbrMode::ProbeBw;
        self.probe_bw_phase = BbrProbeBwPhase::Refill;
    }

    fn adapt_long_term_model(&mut self, sample: AckSample, app_limited: bool) -> bool {
        let too_high = self.inflight_too_high(sample);
        if self.ack_phase == BbrAckPhase::ProbeStarting && self.round_start() {
            self.ack_phase = BbrAckPhase::ProbeFeedback;
            self.set_flag(BbrFlags::IS_BW_PROBE_SAMPLE);
        }
        if self.ack_phase == BbrAckPhase::ProbeStopping && self.round_start() {
            self.clear_flag(BbrFlags::IS_BW_PROBE_SAMPLE);
            self.ack_phase = BbrAckPhase::Init;
            if self.mode == BbrMode::ProbeBw {
                if !app_limited && self.bw_hi[1] != 0 {
                    self.bw_hi[0] = self.bw_hi[1];
                    self.bw_hi[1] = 0;
                }
                if self.has_flag(BbrFlags::PREV_PROBE_PRECAUTIONARY)
                    && !self.has_flag(BbrFlags::PREV_PROBE_TOO_HIGH)
                {
                    self.start_probe_bw_refill();
                    return true;
                }
            }
        }
        if too_high {
            if self.has_flag(BbrFlags::IS_BW_PROBE_SAMPLE) {
                self.handle_inflight_too_high(sample, app_limited);
                return true;
            }
            return false;
        }
        if self.inflight_hi != BBR_INFLIGHT_INFINITY {
            self.inflight_hi = self
                .inflight_hi
                .max(sample.tx_in_flight.min(u64::from(BBR_INFLIGHT_INFINITY)) as u32);
            if self.mode == BbrMode::ProbeBw && self.probe_bw_phase == BbrProbeBwPhase::Up {
                self.raise_inflight_hi_slope();
            }
        }
        false
    }

    fn inflight_too_high(&self, sample: AckSample) -> bool {
        sample.lost != 0
            && sample.tx_in_flight != 0
            && sample.lost.saturating_mul(BBR_LOSS_THRESHOLD_DENOMINATOR)
                > sample
                    .tx_in_flight
                    .saturating_mul(BBR_LOSS_THRESHOLD_NUMERATOR)
    }

    fn handle_inflight_too_high(&mut self, sample: AckSample, app_limited: bool) {
        self.set_flag(BbrFlags::PREV_PROBE_TOO_HIGH);
        self.clear_flag(BbrFlags::IS_BW_PROBE_SAMPLE);
        if !app_limited {
            let target = self
                .target_congestion_window(BBR_CWND_GAIN_MILLI)
                .saturating_mul(BBR_BETA_NUMERATOR as u32)
                / BBR_BETA_DENOMINATOR as u32;
            self.inflight_hi = sample
                .tx_in_flight
                .min(u64::from(BBR_INFLIGHT_INFINITY))
                .max(u64::from(
                    target.max(min_congestion_window(self.max_datagram_size)),
                )) as u32;
        }
        if self.mode == BbrMode::ProbeBw && self.probe_bw_phase == BbrProbeBwPhase::Up {
            let loss_flags = self.flags
                & (BbrFlags::LOSS_IN_ROUND
                    | BbrFlags::LOSS_EVENT_PENDING
                    | BbrFlags::RECOVERY_IN_ROUND);
            self.undo_state = BbrUndoState::ProbeUp;
            self.start_probe_bw_down(sample.now);
            self.set_flag(loss_flags);
        }
    }

    fn raise_inflight_hi_slope(&mut self) {
        let shift = self.bw_probe_up_rounds.min(30);
        let growth = 1u32 << shift;
        self.bw_probe_up_rounds = self.bw_probe_up_rounds.saturating_add(1).min(30);
        self.probe_up_acked_per_inc = self
            .congestion_window
            .checked_div(growth)
            .unwrap_or(0)
            .max(self.max_datagram_size);
    }

    fn should_probe_bw(&self, now: Instant) -> bool {
        let time_ready = self
            .cycle_stamp
            .zip(self.bw_probe_wait)
            .is_some_and(|(stamp, wait)| now.saturating_duration_since(stamp) >= wait);
        let target = self.target_congestion_window(1000);
        let rounds = target.saturating_add(self.max_datagram_size.saturating_sub(1))
            / self.max_datagram_size.max(1);
        time_ready || self.rounds_since_probe_up >= rounds.min(63) as u8
    }

    fn should_cruise(&self, bytes_in_flight: u32) -> bool {
        let headroom = self
            .inflight_hi
            .saturating_sub(
                (u64::from(self.inflight_hi) * BBR_HEADROOM_NUMERATOR / BBR_HEADROOM_DENOMINATOR)
                    .max(u64::from(self.max_datagram_size)) as u32,
            )
            .max(min_congestion_window(self.max_datagram_size));
        bytes_in_flight <= headroom && bytes_in_flight <= self.target_congestion_window(1000)
    }

    fn should_go_down(&mut self, sample: AckSample) -> bool {
        if self.has_flag(BbrFlags::PREV_PROBE_TOO_HIGH)
            && self.inflight_hi != BBR_INFLIGHT_INFINITY
            && sample.tx_in_flight >= u64::from(self.inflight_hi)
        {
            self.set_flag(BbrFlags::PREV_PROBE_PRECAUTIONARY);
            return true;
        }
        if self.inflight_hi != BBR_INFLIGHT_INFINITY
            && sample.tx_in_flight >= u64::from(self.inflight_hi)
        {
            return true;
        }
        self.full_bw_reached()
    }

    fn reset_short_term_model(&mut self) {
        self.bw_lo = BBR_BW_INFINITY;
        self.inflight_lo = BBR_INFLIGHT_INFINITY;
    }

    fn reset_congestion_signals(&mut self) {
        self.clear_flag(
            BbrFlags::LOSS_IN_ROUND
                | BbrFlags::LOSS_ROUND_HAD_LOSS
                | BbrFlags::LOSS_EVENT_PENDING
                | BbrFlags::RECOVERY_IN_ROUND,
        );
        self.loss_events = 0;
        self.bw_latest = 0;
        self.inflight_latest = 0;
    }

    fn update_offload_budget(&mut self) {
        let Some(rate) = self.pacing_rate else {
            return;
        };
        let quantum = (u128::from(rate).saturating_mul(BBR_SEND_QUANTUM_INTERVAL.as_nanos())
            / 1_000_000_000u128)
            .min(u128::from(BBR_SEND_QUANTUM_MAX)) as u32;
        let quantum = quantum.max(self.max_datagram_size.saturating_mul(2));
        let pipeline = (u128::from(rate).saturating_mul(BBR_OFFLOAD_BUDGET_INTERVAL.as_nanos())
            / 1_000_000_000u128) as u32;
        self.offload_budget = pipeline
            .max(quantum.saturating_mul(BBR_OFFLOAD_QUANTA_MIN))
            .min(quantum.saturating_mul(BBR_OFFLOAD_QUANTA_MAX));
        self.offload_mss = self.max_datagram_size.min(u32::from(u16::MAX)) as u16;
    }

    fn maybe_enter_probe_rtt(&mut self, now: Instant) {
        let Some(stamp) = self.min_rtt_stamp else {
            return;
        };
        if self.mode == BbrMode::ProbeRtt {
            return;
        }
        if now.saturating_duration_since(stamp) <= BBR_MIN_RTT_FILTER {
            return;
        }
        self.mode = BbrMode::ProbeRtt;
        self.ack_phase = BbrAckPhase::ProbeStopping;
        self.probe_rtt_done_stamp = None;
        self.clear_flag(BbrFlags::PROBE_RTT_ROUND_DONE);
        self.prior_cwnd = self.congestion_window;
        self.congestion_window = self.probe_rtt_target();
    }

    fn update_startup(&mut self, sample: AckSample, app_limited: bool) {
        let target = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        if self.full_bw_reached() {
            self.congestion_window = self
                .congestion_window
                .saturating_add(sample.bytes_acked)
                .min(target);
        } else if self.congestion_window < target || self.delivered < u64::from(self.initial_cwnd) {
            self.congestion_window = self.congestion_window.saturating_add(sample.bytes_acked);
        }
        self.congestion_window = self
            .congestion_window
            .max(min_congestion_window(self.max_datagram_size));

        if !self.round_start() || app_limited {
            return;
        }

        if self.full_bw == 0 {
            self.full_bw = self.max_bw();
            self.full_bw_count = 0;
            return;
        }

        let growth_target =
            ((u128::from(self.full_bw) * u128::from(BBR_FULL_BANDWIDTH_GAIN_MILLI) + 999) / 1000)
                .min(u128::from(u64::MAX)) as u64;
        if self.max_bw() >= growth_target {
            self.full_bw = self.max_bw();
            self.full_bw_count = 0;
        } else {
            self.full_bw_count = self.full_bw_count.saturating_add(1);
        }

        if self.full_bw_count >= 3 {
            self.set_flag(BbrFlags::FULL_BW_REACHED);
            self.mode = BbrMode::Drain;
            self.drain_rounds = 0;
        }
    }

    fn update_drain(&mut self, bytes_in_flight: u32) {
        self.congestion_window = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        if bytes_in_flight <= self.target_congestion_window(1000) || self.drain_rounds > 3 {
            self.start_probe_bw_down(Instant::now());
        }
    }

    fn update_probe_bw(&mut self, sample: AckSample) {
        if self.round_start() {
            match self.probe_bw_phase {
                BbrProbeBwPhase::Down if self.should_probe_bw(sample.now) => {
                    self.start_probe_bw_refill();
                }
                BbrProbeBwPhase::Down if self.should_cruise(sample.tx_in_flight as u32) => {
                    self.probe_bw_phase = BbrProbeBwPhase::Cruise;
                }
                BbrProbeBwPhase::Cruise if self.should_probe_bw(sample.now) => {
                    self.start_probe_bw_refill();
                }
                BbrProbeBwPhase::Refill => {
                    self.probe_bw_phase = BbrProbeBwPhase::Up;
                    self.ack_phase = BbrAckPhase::ProbeStarting;
                    self.set_flag(BbrFlags::IS_BW_PROBE_SAMPLE);
                    self.bw_probe_up_rounds = self.bw_probe_up_rounds.saturating_add(1).min(30);
                    self.probe_up_acked_per_inc = self
                        .congestion_window
                        .checked_div(1u32 << self.bw_probe_up_rounds.min(30))
                        .unwrap_or(0)
                        .max(self.max_datagram_size);
                    self.full_bw = self.max_bw();
                    self.full_bw_count = 0;
                    self.clear_flag(BbrFlags::PREV_PROBE_PRECAUTIONARY);
                }
                BbrProbeBwPhase::Up if self.should_go_down(sample) => {
                    self.start_probe_bw_down(sample.now);
                }
                _ => {}
            }
            if self.probe_bw_phase == BbrProbeBwPhase::Up
                && self.probe_up_acked_per_inc != BBR_INFLIGHT_INFINITY
            {
                self.bw_probe_up_acked = self.bw_probe_up_acked.saturating_add(sample.bytes_acked);
                let increments = self.bw_probe_up_acked / self.probe_up_acked_per_inc.max(1);
                self.bw_probe_up_acked %= self.probe_up_acked_per_inc.max(1);
                self.inflight_hi = self
                    .inflight_hi
                    .saturating_add(increments.saturating_mul(self.max_datagram_size));
            }
        }
        let target_gain = if self.probe_bw_phase == BbrProbeBwPhase::Up {
            BBR_PROBE_UP_CWND_GAIN_MILLI
        } else {
            BBR_CWND_GAIN_MILLI
        };
        let target = self.target_congestion_window(target_gain);
        self.congestion_window = self
            .congestion_window
            .saturating_add(sample.bytes_acked)
            .min(target)
            .max(min_congestion_window(self.max_datagram_size));
    }

    fn update_probe_rtt(&mut self, sample: AckSample, bytes_in_flight: u32) {
        let probe_rtt_window = self.probe_rtt_target();
        self.congestion_window = probe_rtt_window;
        if bytes_in_flight <= probe_rtt_window && self.probe_rtt_done_stamp.is_none() {
            self.probe_rtt_done_stamp = Some(
                sample
                    .now
                    .checked_add(BBR_PROBE_RTT_DURATION)
                    .unwrap_or(sample.now),
            );
        }
        if self.round_start() {
            self.set_flag(BbrFlags::PROBE_RTT_ROUND_DONE);
        }
        if self
            .probe_rtt_done_stamp
            .is_some_and(|done| sample.now >= done && self.has_flag(BbrFlags::PROBE_RTT_ROUND_DONE))
        {
            self.probe_rtt_min_stamp = Some(sample.now);
            self.congestion_window = self.congestion_window.max(self.prior_cwnd);
            self.mode = if self.full_bw_reached() {
                self.start_probe_bw_down(sample.now);
                BbrMode::ProbeBw
            } else {
                BbrMode::Startup
            };
            self.ack_phase = BbrAckPhase::Init;
            self.probe_rtt_done_stamp = None;
            self.clear_flag(BbrFlags::PROBE_RTT_ROUND_DONE);
            self.congestion_window = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        }
    }

    fn target_congestion_window(&self, gain_milli: u32) -> u32 {
        let Some(min_rtt) = self.min_rtt else {
            return initial_congestion_window(self.max_datagram_size);
        };
        if self.bw() == 0 {
            return initial_congestion_window(self.max_datagram_size);
        }
        let bytes = u128::from(self.bw())
            .saturating_mul(min_rtt.as_micros())
            .saturating_mul(u128::from(gain_milli))
            / 1_000_000u128
            / 1000u128;
        let mut target = bytes
            .saturating_add(u128::from(self.extra_acked.value()))
            .clamp(
                u128::from(min_congestion_window(self.max_datagram_size)),
                u128::from(u32::MAX),
            ) as u32;
        if self.inflight_lo != BBR_INFLIGHT_INFINITY {
            target = target.min(self.inflight_lo);
        }
        if self.inflight_hi != BBR_INFLIGHT_INFINITY {
            target = target.min(self.inflight_hi);
        }
        target = target.max(self.offload_budget);
        target.max(min_congestion_window(self.max_datagram_size))
    }

    fn probe_rtt_target(&self) -> u32 {
        self.target_congestion_window(BBR_PROBE_RTT_CWND_GAIN_MILLI)
            .max(min_congestion_window(self.max_datagram_size))
    }

    fn update_pacing_rate(&mut self) {
        if self.bw() == 0 {
            return;
        }
        let gain_milli = match self.mode {
            BbrMode::Startup => BBR_HIGH_GAIN_MILLI,
            BbrMode::Drain => BBR_DRAIN_GAIN_MILLI,
            BbrMode::ProbeBw => match self.probe_bw_phase {
                BbrProbeBwPhase::Down => 900,
                BbrProbeBwPhase::Cruise | BbrProbeBwPhase::Refill => 1000,
                BbrProbeBwPhase::Up => 1250,
            },
            BbrMode::ProbeRtt => 1000,
        };
        let pacing_rate = (u128::from(self.bw()) * u128::from(gain_milli) / 1000u128)
            .clamp(1, u128::from(u64::MAX)) as u64;
        self.pacing_rate = Some(pacing_rate);
        self.update_offload_budget();
    }
}

impl CongestionController for BbrController {
    fn new(max_datagram_size: u32) -> Self {
        let max_datagram_size = normalized_max_datagram_size(max_datagram_size);
        Self {
            max_datagram_size,
            mode: BbrMode::Startup,
            probe_bw_phase: BbrProbeBwPhase::Down,
            ack_phase: BbrAckPhase::Init,
            undo_state: BbrUndoState::None,
            flags: BbrFlags::empty(),
            extra_acked: BbrMinmax {
                samples: [
                    BbrMinmaxSample { round: 0, value: 0 },
                    BbrMinmaxSample { round: 0, value: 0 },
                    BbrMinmaxSample { round: 0, value: 0 },
                ],
            },
            congestion_window: initial_congestion_window(max_datagram_size),
            pacing_rate: None,
            bw_hi: [0; 2],
            bw_lo: BBR_BW_INFINITY,
            bw_latest: 0,
            undo_bw_lo: BBR_BW_INFINITY,
            min_rtt: None,
            min_rtt_stamp: None,
            probe_rtt_min_delay: None,
            probe_rtt_min_stamp: None,
            delivered: 0,
            next_round_delivered: 0,
            loss_round_delivered: 0,
            last_loss_counted: 0,
            cycle_stamp: None,
            bw_probe_wait: None,
            full_bw: 0,
            ack_epoch_stamp: None,
            ack_epoch_acked: 0,
            inflight_hi: BBR_INFLIGHT_INFINITY,
            inflight_lo: BBR_INFLIGHT_INFINITY,
            inflight_latest: 0,
            undo_inflight_hi: BBR_INFLIGHT_INFINITY,
            undo_inflight_lo: BBR_INFLIGHT_INFINITY,
            prior_cwnd: initial_congestion_window(max_datagram_size),
            initial_cwnd: initial_congestion_window(max_datagram_size),
            round_count: 0,
            bw_probe_up_acked: 0,
            probe_up_acked_per_inc: BBR_INFLIGHT_INFINITY,
            random_seed: max_datagram_size ^ 0x9e37_79b9,
            offload_budget: 0,
            offload_mss: max_datagram_size.min(u32::from(u16::MAX)) as u16,
            full_bw_count: 0,
            drain_rounds: 0,
            bw_probe_up_rounds: 0,
            loss_events: 0,
            rounds_since_probe_up: 0,
            probe_rtt_done_stamp: None,
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        CongestionMetrics {
            congestion_window: self.congestion_window(),
            pacing_rate_bytes_per_second: self.pacing_rate,
            delivered: self.delivered,
            max_bandwidth_bytes_per_second: self.max_bw(),
            min_rtt: self.min_rtt,
        }
    }

    fn max_datagram_size(&self) -> u32 {
        self.max_datagram_size
    }

    fn congestion_window(&self) -> u32 {
        self.congestion_window
            .max(min_congestion_window(self.max_datagram_size))
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        self.pacing_rate
    }

    fn delivered(&self) -> u64 {
        self.delivered
    }

    fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        self.max_bw()
    }

    fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32) {
        let app_limited = acked.app_limited;
        let sample = self.ack_sample(now, acked, rtt);
        self.apply_ack_sample(sample, bytes_in_flight, app_limited);
        if acked.delivered != 0 {
            self.clear_flag(BbrFlags::IDLE_RESTART);
        }
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        _largest_acked_packet: PacketNumber,
    ) {
        if bytes_in_flight == 0 && app_limited {
            self.set_flag(BbrFlags::IDLE_RESTART);
            self.ack_epoch_stamp = Some(now);
            self.ack_epoch_acked = 0;
            if self.mode == BbrMode::ProbeBw {
                self.pacing_rate = Some(self.bw().max(1));
                self.update_offload_budget();
            }
        }
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, _: bool) {
        if lost.bytes == 0 {
            return;
        }
        if !self.has_flag(BbrFlags::LOSS_IN_ROUND) {
            self.loss_round_delivered = self.delivered;
            self.prior_cwnd = self.prior_cwnd.max(self.congestion_window);
            self.undo_bw_lo = self.bw_lo;
            self.undo_inflight_lo = self.inflight_lo;
            self.undo_inflight_hi = self.inflight_hi;
        }
        self.set_flag(BbrFlags::LOSS_IN_ROUND | BbrFlags::LOSS_EVENT_PENDING);
        if self.mode == BbrMode::ProbeBw && self.probe_bw_phase == BbrProbeBwPhase::Up {
            self.set_flag(BbrFlags::PREV_PROBE_TOO_HIGH);
            self.undo_state = BbrUndoState::ProbeUp;
            let target = self
                .target_congestion_window(BBR_CWND_GAIN_MILLI)
                .saturating_mul(BBR_BETA_NUMERATOR as u32)
                / BBR_BETA_DENOMINATOR as u32;
            self.inflight_hi = self
                .inflight_hi
                .min(target.max(min_congestion_window(self.max_datagram_size)));
            let loss_flags = self.flags
                & (BbrFlags::LOSS_IN_ROUND
                    | BbrFlags::LOSS_EVENT_PENDING
                    | BbrFlags::RECOVERY_IN_ROUND);
            self.start_probe_bw_down(now);
            self.set_flag(loss_flags);
        }
        self.congestion_window = self
            .congestion_window
            .max(min_congestion_window(self.max_datagram_size));
    }

    fn on_recovered(&mut self) {
        self.congestion_window = self.congestion_window.max(self.prior_cwnd);
        self.clear_flag(BbrFlags::LOSS_IN_ROUND | BbrFlags::RECOVERY_IN_ROUND);
        self.full_bw = 0;
        self.full_bw_count = 0;
        self.bw_lo = self.bw_lo.max(self.undo_bw_lo);
        self.inflight_lo = self.inflight_lo.max(self.undo_inflight_lo);
        self.inflight_hi = self.inflight_hi.max(self.undo_inflight_hi);
        match self.undo_state {
            BbrUndoState::Startup if self.mode != BbrMode::Startup => {
                self.clear_flag(BbrFlags::FULL_BW_REACHED);
                if self.mode != BbrMode::ProbeRtt {
                    self.mode = BbrMode::Startup;
                }
            }
            BbrUndoState::ProbeUp
                if self.mode != BbrMode::ProbeRtt
                    && (self.mode != BbrMode::ProbeBw
                        || self.probe_bw_phase != BbrProbeBwPhase::Up) =>
            {
                self.start_probe_bw_refill();
            }
            BbrUndoState::None | BbrUndoState::Startup | BbrUndoState::ProbeUp => {}
        }
        self.undo_state = BbrUndoState::None;
    }

    fn on_tail_loss_probe(&mut self, now: Instant, bytes_in_flight: u32) {
        self.set_flag(BbrFlags::LOSS_IN_ROUND);
        if self.mode == BbrMode::ProbeBw
            && self.probe_bw_phase == BbrProbeBwPhase::Up
            && self.inflight_hi != BBR_INFLIGHT_INFINITY
            && bytes_in_flight >= self.inflight_hi
        {
            self.set_flag(BbrFlags::PREV_PROBE_TOO_HIGH);
            self.undo_state = BbrUndoState::ProbeUp;
            let loss_flags = self.flags
                & (BbrFlags::LOSS_IN_ROUND
                    | BbrFlags::LOSS_EVENT_PENDING
                    | BbrFlags::RECOVERY_IN_ROUND);
            self.start_probe_bw_down(now);
            self.set_flag(loss_flags);
        }
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        let max_datagram_size = normalized_max_datagram_size(max_datagram_size);
        self.max_datagram_size = max_datagram_size;
        self.congestion_window = self
            .congestion_window
            .max(min_congestion_window(max_datagram_size));
        self.offload_mss = max_datagram_size.min(u32::from(u16::MAX)) as u16;
        self.update_pacing_rate();
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        let rate = self.pacing_rate?;
        if rate == 0 || pending_bytes == 0 {
            return None;
        }
        let nanos = div_ceil_u128(
            u128::from(pending_bytes) * 1_000_000_000u128,
            u128::from(rate),
        )
        .clamp(1, u128::from(u64::MAX)) as u64;
        Some(Duration::from_nanos(nanos))
    }
}

impl Default for BbrController {
    fn default() -> Self {
        Self::new(DEFAULT_BBR_MAX_DATAGRAM_SIZE)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AckSample {
    bytes_acked: u32,
    delivered: u64,
    prior_delivered: u64,
    interval: Duration,
    tx_in_flight: u64,
    tx_lost: u64,
    lost: u64,
    rtt: Duration,
    now: Instant,
}

#[inline]
fn normalized_max_datagram_size(max_datagram_size: u32) -> u32 {
    if max_datagram_size == 0 {
        DEFAULT_BBR_MAX_DATAGRAM_SIZE
    } else {
        max_datagram_size
    }
}

#[inline]
fn initial_congestion_window(max_datagram_size: u32) -> u32 {
    normalized_max_datagram_size(max_datagram_size).saturating_mul(BBR_INITIAL_WINDOW_SEGMENTS)
}

#[inline]
fn min_congestion_window(max_datagram_size: u32) -> u32 {
    normalized_max_datagram_size(max_datagram_size).saturating_mul(BBR_MIN_WINDOW_SEGMENTS)
}

#[inline]
fn div_ceil_u128(numerator: u128, denominator: u128) -> u128 {
    if numerator == 0 {
        0
    } else {
        ((numerator - 1) / denominator) + 1
    }
}
