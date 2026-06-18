use std::time::{Duration, Instant};

use super::controller::CongestionController;
use super::types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};

pub const DEFAULT_BBR_MAX_DATAGRAM_SIZE: u32 = 1_460;
const BBR_INITIAL_WINDOW_SEGMENTS: u32 = 10;
const BBR_MIN_WINDOW_SEGMENTS: u32 = 4;
const BBR_HIGH_GAIN_MILLI: u32 = 2885;
const BBR_DRAIN_GAIN_MILLI: u32 = 347;
const BBR_CWND_GAIN_MILLI: u32 = 2000;
const BBR_PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
const BBR_MIN_RTT_FILTER: Duration = Duration::from_secs(10);
const BBR_FULL_BANDWIDTH_GAIN_MILLI: u32 = 1250;

const PROBE_BW_GAIN_CYCLE: [u32; 8] = [1250, 750, 1000, 1000, 1000, 1000, 1000, 1000];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BbrMode {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BbrController {
    max_datagram_size: u32,
    mode: BbrMode,
    congestion_window: u32,
    pacing_rate_bytes_per_second: Option<u64>,
    max_bandwidth_bytes_per_second: u64,
    min_rtt: Option<Duration>,
    min_rtt_stamp: Option<Instant>,
    delivered: u64,
    next_round_delivered: u64,
    round_end_marker_active: bool,
    round_start: bool,
    full_bandwidth_bytes_per_second: u64,
    full_bandwidth_rounds: u8,
    cycle_index: usize,
    cycle_stamp: Option<Instant>,
    probe_rtt_done_stamp: Option<Instant>,
    probe_rtt_round_done: bool,
    prior_mode: BbrMode,
}

impl BbrController {
    pub fn bbr_mode(&self) -> BbrMode {
        self.mode
    }

    fn ack_sample(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample) -> BbrAckSample {
        BbrAckSample {
            bytes_acked: acked.bytes,
            rtt: rtt.latest,
            now,
        }
    }

    fn apply_ack_sample(&mut self, sample: BbrAckSample, bytes_in_flight: u32, app_limited: bool) {
        if sample.bytes_acked == 0 {
            return;
        }

        let prior_delivered = self.delivered;
        self.delivered = self.delivered.saturating_add(u64::from(sample.bytes_acked));
        self.round_start = self.round_end_marker_active
            && prior_delivered < self.next_round_delivered
            && self.delivered >= self.next_round_delivered;
        if self.round_start {
            self.round_end_marker_active = false;
        }

        if sample.rtt.is_zero() {
            return;
        }

        self.maybe_enter_probe_rtt(sample.now);
        self.update_min_rtt(sample);
        self.update_bandwidth(sample, app_limited);

        match self.mode {
            BbrMode::Startup => self.update_startup(sample, app_limited),
            BbrMode::Drain => self.update_drain(bytes_in_flight),
            BbrMode::ProbeBw => self.update_probe_bw(sample),
            BbrMode::ProbeRtt => self.update_probe_rtt(sample, bytes_in_flight),
        }

        self.update_pacing_rate();
    }

    fn is_app_limited_sample(&self, bytes_acked: u32, bytes_in_flight: u32) -> bool {
        bytes_in_flight.saturating_add(bytes_acked) < self.congestion_window()
    }

    fn update_min_rtt(&mut self, sample: BbrAckSample) {
        let expired = self
            .min_rtt_stamp
            .is_some_and(|stamp| sample.now.saturating_duration_since(stamp) > BBR_MIN_RTT_FILTER);
        if self.min_rtt.is_none_or(|min_rtt| sample.rtt <= min_rtt) || expired {
            self.min_rtt = Some(sample.rtt);
            self.min_rtt_stamp = Some(sample.now);
        }
    }

    fn update_bandwidth(&mut self, sample: BbrAckSample, app_limited: bool) {
        let micros = sample.rtt.as_micros().max(1);
        let sample_rate = ((u128::from(sample.bytes_acked) * 1_000_000u128) / micros)
            .min(u128::from(u64::MAX)) as u64;
        if app_limited
            && self.max_bandwidth_bytes_per_second != 0
            && sample_rate <= self.max_bandwidth_bytes_per_second
        {
            return;
        }
        self.max_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second.max(sample_rate);
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
        self.prior_mode = self.mode;
        self.mode = BbrMode::ProbeRtt;
        self.probe_rtt_done_stamp = None;
        self.probe_rtt_round_done = false;
        self.congestion_window = probe_rtt_window(self.max_datagram_size);
    }

    fn update_startup(&mut self, sample: BbrAckSample, app_limited: bool) {
        self.congestion_window = self
            .congestion_window
            .saturating_add(sample.bytes_acked)
            .max(min_congestion_window(self.max_datagram_size));

        if !self.round_start || app_limited {
            return;
        }

        if self.full_bandwidth_bytes_per_second == 0 {
            self.full_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second;
            self.full_bandwidth_rounds = 0;
            return;
        }

        let growth_target = ((u128::from(self.full_bandwidth_bytes_per_second)
            * u128::from(BBR_FULL_BANDWIDTH_GAIN_MILLI)
            + 999)
            / 1000)
            .min(u128::from(u64::MAX)) as u64;
        if self.max_bandwidth_bytes_per_second >= growth_target {
            self.full_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second;
            self.full_bandwidth_rounds = 0;
        } else {
            self.full_bandwidth_rounds = self.full_bandwidth_rounds.saturating_add(1);
        }

        if self.full_bandwidth_rounds >= 3 {
            self.mode = BbrMode::Drain;
        }
    }

    fn update_drain(&mut self, bytes_in_flight: u32) {
        self.congestion_window = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        if bytes_in_flight <= self.target_congestion_window(1000) {
            self.mode = BbrMode::ProbeBw;
            self.cycle_index = 0;
            self.cycle_stamp = Some(Instant::now());
        }
    }

    fn update_probe_bw(&mut self, sample: BbrAckSample) {
        if self.should_advance_probe_bw_cycle(sample.now) {
            self.cycle_index = (self.cycle_index + 1) % PROBE_BW_GAIN_CYCLE.len();
            self.cycle_stamp = Some(sample.now);
        }
        let target = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        self.congestion_window = target
            .max(self.congestion_window)
            .max(min_congestion_window(self.max_datagram_size));
    }

    fn update_probe_rtt(&mut self, sample: BbrAckSample, bytes_in_flight: u32) {
        let probe_rtt_window = probe_rtt_window(self.max_datagram_size);
        self.congestion_window = probe_rtt_window;
        if bytes_in_flight <= probe_rtt_window && self.probe_rtt_done_stamp.is_none() {
            self.probe_rtt_done_stamp = Some(
                sample
                    .now
                    .checked_add(BBR_PROBE_RTT_DURATION)
                    .unwrap_or(sample.now),
            );
            self.probe_rtt_round_done = false;
        }
        if self.round_start {
            self.probe_rtt_round_done = true;
        }
        if self
            .probe_rtt_done_stamp
            .is_some_and(|done| sample.now >= done && self.probe_rtt_round_done)
        {
            self.mode = match self.prior_mode {
                BbrMode::ProbeRtt => BbrMode::ProbeBw,
                prior_mode => prior_mode,
            };
            if self.mode == BbrMode::ProbeBw {
                self.cycle_index = 0;
                self.cycle_stamp = Some(sample.now);
            }
            self.probe_rtt_done_stamp = None;
            self.probe_rtt_round_done = false;
            self.congestion_window = self.target_congestion_window(BBR_CWND_GAIN_MILLI);
        }
    }

    fn should_advance_probe_bw_cycle(&self, now: Instant) -> bool {
        let Some(min_rtt) = self.min_rtt else {
            return false;
        };
        self.cycle_stamp
            .is_none_or(|stamp| now.saturating_duration_since(stamp) >= min_rtt)
    }

    fn target_congestion_window(&self, gain_milli: u32) -> u32 {
        let Some(min_rtt) = self.min_rtt else {
            return initial_congestion_window(self.max_datagram_size);
        };
        if self.max_bandwidth_bytes_per_second == 0 {
            return initial_congestion_window(self.max_datagram_size);
        }
        let bytes = u128::from(self.max_bandwidth_bytes_per_second)
            .saturating_mul(min_rtt.as_micros())
            .saturating_mul(u128::from(gain_milli))
            / 1_000_000u128
            / 1000u128;
        bytes.clamp(
            u128::from(min_congestion_window(self.max_datagram_size)),
            u128::from(u32::MAX),
        ) as u32
    }

    fn update_pacing_rate(&mut self) {
        if self.max_bandwidth_bytes_per_second == 0 {
            return;
        }
        let gain_milli = match self.mode {
            BbrMode::Startup => BBR_HIGH_GAIN_MILLI,
            BbrMode::Drain => BBR_DRAIN_GAIN_MILLI,
            BbrMode::ProbeBw => PROBE_BW_GAIN_CYCLE[self.cycle_index],
            BbrMode::ProbeRtt => 1000,
        };
        let pacing_rate = (u128::from(self.max_bandwidth_bytes_per_second) * u128::from(gain_milli)
            / 1000u128)
            .clamp(1, u128::from(u64::MAX)) as u64;
        self.pacing_rate_bytes_per_second = Some(pacing_rate);
    }
}

impl CongestionController for BbrController {
    fn new(max_datagram_size: u32) -> Self {
        let max_datagram_size = normalized_max_datagram_size(max_datagram_size);
        Self {
            max_datagram_size,
            mode: BbrMode::Startup,
            congestion_window: initial_congestion_window(max_datagram_size),
            pacing_rate_bytes_per_second: None,
            max_bandwidth_bytes_per_second: 0,
            min_rtt: None,
            min_rtt_stamp: None,
            delivered: 0,
            next_round_delivered: 0,
            round_end_marker_active: false,
            round_start: false,
            full_bandwidth_bytes_per_second: 0,
            full_bandwidth_rounds: 0,
            cycle_index: 0,
            cycle_stamp: None,
            probe_rtt_done_stamp: None,
            probe_rtt_round_done: false,
            prior_mode: BbrMode::Startup,
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        CongestionMetrics {
            congestion_window: self.congestion_window(),
            pacing_rate_bytes_per_second: self.pacing_rate_bytes_per_second,
            delivered: self.delivered,
            max_bandwidth_bytes_per_second: self.max_bandwidth_bytes_per_second,
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
        self.pacing_rate_bytes_per_second
    }

    fn delivered(&self) -> u64 {
        self.delivered
    }

    fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        self.max_bandwidth_bytes_per_second
    }

    fn on_packet_sent(
        &mut self,
        _packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        _now: Instant,
    ) {
        if bytes_sent == 0 {
            return;
        }
        if bytes_in_flight == 0 || self.round_start {
            self.next_round_delivered = self
                .delivered
                .saturating_add(u64::from(bytes_in_flight))
                .saturating_add(u64::from(bytes_sent));
            self.round_end_marker_active = true;
            self.round_start = false;
        }
    }

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32) {
        let app_limited =
            acked.app_limited || self.is_app_limited_sample(acked.bytes, bytes_in_flight);
        let sample = self.ack_sample(now, acked, rtt);
        self.apply_ack_sample(sample, bytes_in_flight, app_limited);
    }

    fn on_end_acks(
        &mut self,
        _now: Instant,
        _bytes_in_flight: u32,
        _app_limited: bool,
        _largest_acked_packet: PacketNumber,
    ) {
    }

    fn on_loss(&mut self, _now: Instant, lost: LostPacket, _persistent_congestion: bool) {
        if lost.bytes == 0 {
            return;
        }
        let reduction = lost.bytes.max(self.max_datagram_size);
        let reduced = self.congestion_window.saturating_sub(reduction);
        self.congestion_window = reduced.max(min_congestion_window(self.max_datagram_size));
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        let max_datagram_size = normalized_max_datagram_size(max_datagram_size);
        self.max_datagram_size = max_datagram_size;
        self.congestion_window = self
            .congestion_window
            .max(min_congestion_window(max_datagram_size));
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        let rate = self.pacing_rate_bytes_per_second?;
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
struct BbrAckSample {
    bytes_acked: u32,
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
fn probe_rtt_window(max_datagram_size: u32) -> u32 {
    min_congestion_window(max_datagram_size)
}

#[inline]
fn div_ceil_u128(numerator: u128, denominator: u128) -> u128 {
    if numerator == 0 {
        0
    } else {
        ((numerator - 1) / denominator) + 1
    }
}
