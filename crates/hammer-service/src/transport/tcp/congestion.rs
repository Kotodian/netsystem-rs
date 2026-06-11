use std::time::{Duration, Instant};

use super::output::DEFAULT_TCP_OUTPUT_PAYLOAD_LEN;
use super::state::TcpCongestionAlgorithm;

const DEFAULT_TCP_CONGESTION_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;
const TCP_BBR_INITIAL_WINDOW_SEGMENTS: u32 = 10;
const TCP_BBR_MIN_WINDOW_SEGMENTS: u32 = 4;
const TCP_BBR_HIGH_GAIN_MILLI: u32 = 2885;
const TCP_BBR_DRAIN_GAIN_MILLI: u32 = 347;
const TCP_BBR_CWND_GAIN_MILLI: u32 = 2000;
const TCP_BBR_PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
const TCP_BBR_MIN_RTT_FILTER: Duration = Duration::from_secs(10);

const PROBE_BW_GAIN_CYCLE: [u32; 8] = [1250, 750, 1000, 1000, 1000, 1000, 1000, 1000];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpBbrMode {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpCongestionAckSample {
    pub bytes_acked: u32,
    pub rtt: Duration,
    pub now: Instant,
    pub bytes_in_flight: u32,
}

type TcpBbrAckSample = TcpCongestionAckSample;

pub trait TcpCongestionControl {
    fn algorithm(&self) -> TcpCongestionAlgorithm;
    fn congestion_window(&self) -> u32;
    fn pacing_rate_bytes_per_second(&self) -> Option<u64>;
    fn delivered(&self) -> u64;
    fn min_rtt(&self) -> Option<Duration>;
    fn max_bandwidth_bytes_per_second(&self) -> u64;
    fn on_packet_sent(&mut self, bytes_sent: u32, bytes_in_flight: u32);
    fn on_ack(&mut self, sample: TcpCongestionAckSample);
    fn on_loss(&mut self, bytes_lost: u32);
    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpCongestionState {
    inner: TcpCongestionStateKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpCongestionStateKind {
    Bbr(TcpBbrState),
}

impl Default for TcpCongestionState {
    fn default() -> Self {
        Self::with_default_max_segment_size()
    }
}

impl TcpCongestionState {
    pub fn new(max_segment_size: u32) -> Self {
        Self {
            inner: TcpCongestionStateKind::Bbr(TcpBbrState::new(max_segment_size)),
        }
    }

    pub fn with_default_max_segment_size() -> Self {
        Self::new(DEFAULT_TCP_CONGESTION_MAX_SEGMENT_SIZE)
    }

    pub fn max_segment_size(&self) -> u32 {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.max_segment_size(),
        }
    }

    pub fn algorithm(&self) -> TcpCongestionAlgorithm {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.algorithm(),
        }
    }

    pub fn congestion_window(&self) -> u32 {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.congestion_window(),
        }
    }

    pub fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.pacing_rate_bytes_per_second(),
        }
    }

    pub fn delivered(&self) -> u64 {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.delivered(),
        }
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.min_rtt(),
        }
    }

    pub fn max_bandwidth_bytes_per_second(&self) -> u64 {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.max_bandwidth_bytes_per_second(),
        }
    }

    pub fn on_packet_sent(&mut self, bytes_sent: u32, bytes_in_flight: u32) {
        match &mut self.inner {
            TcpCongestionStateKind::Bbr(state) => state.on_packet_sent(bytes_sent, bytes_in_flight),
        }
    }

    pub fn on_ack(&mut self, sample: TcpCongestionAckSample) {
        match &mut self.inner {
            TcpCongestionStateKind::Bbr(state) => state.on_ack(sample),
        }
    }

    pub fn on_loss(&mut self, bytes_lost: u32) {
        match &mut self.inner {
            TcpCongestionStateKind::Bbr(state) => state.on_loss(bytes_lost),
        }
    }

    pub fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        match &self.inner {
            TcpCongestionStateKind::Bbr(state) => state.next_send_delay(pending_bytes),
        }
    }
}

impl TcpCongestionControl for TcpCongestionState {
    fn algorithm(&self) -> TcpCongestionAlgorithm {
        TcpCongestionState::algorithm(self)
    }

    fn congestion_window(&self) -> u32 {
        TcpCongestionState::congestion_window(self)
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        TcpCongestionState::pacing_rate_bytes_per_second(self)
    }

    fn delivered(&self) -> u64 {
        TcpCongestionState::delivered(self)
    }

    fn min_rtt(&self) -> Option<Duration> {
        TcpCongestionState::min_rtt(self)
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        TcpCongestionState::max_bandwidth_bytes_per_second(self)
    }

    fn on_packet_sent(&mut self, bytes_sent: u32, bytes_in_flight: u32) {
        TcpCongestionState::on_packet_sent(self, bytes_sent, bytes_in_flight);
    }

    fn on_ack(&mut self, sample: TcpCongestionAckSample) {
        TcpCongestionState::on_ack(self, sample);
    }

    fn on_loss(&mut self, bytes_lost: u32) {
        TcpCongestionState::on_loss(self, bytes_lost);
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        TcpCongestionState::next_send_delay(self, pending_bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TcpBbrState {
    algorithm: TcpCongestionAlgorithm,
    max_segment_size: u32,
    mode: TcpBbrMode,
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
    prior_mode: TcpBbrMode,
}

impl Default for TcpBbrState {
    fn default() -> Self {
        Self::new(DEFAULT_TCP_CONGESTION_MAX_SEGMENT_SIZE)
    }
}

impl TcpBbrState {
    fn new(max_segment_size: u32) -> Self {
        let max_segment_size = max_segment_size.max(1);
        Self {
            algorithm: TcpCongestionAlgorithm::Bbr,
            max_segment_size,
            mode: TcpBbrMode::Startup,
            congestion_window: initial_congestion_window(max_segment_size),
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
            prior_mode: TcpBbrMode::Startup,
        }
    }

    fn algorithm(&self) -> TcpCongestionAlgorithm {
        self.algorithm
    }

    fn max_segment_size(&self) -> u32 {
        self.max_segment_size
    }

    #[cfg(test)]
    fn mode(&self) -> TcpBbrMode {
        self.mode
    }

    fn congestion_window(&self) -> u32 {
        self.congestion_window
            .max(min_congestion_window(self.max_segment_size))
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

    fn on_packet_sent(&mut self, bytes_sent: u32, bytes_in_flight: u32) {
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

    fn on_ack(&mut self, sample: TcpBbrAckSample) {
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

        self.maybe_enter_probe_rtt(sample.now);
        self.update_min_rtt(sample);
        self.update_bandwidth(sample);

        match self.mode {
            TcpBbrMode::Startup => self.update_startup(sample),
            TcpBbrMode::Drain => self.update_drain(sample),
            TcpBbrMode::ProbeBw => self.update_probe_bw(sample),
            TcpBbrMode::ProbeRtt => self.update_probe_rtt(sample),
        }

        self.update_pacing_rate();
    }

    fn on_loss(&mut self, bytes_lost: u32) {
        if bytes_lost == 0 {
            return;
        }
        let reduced = self.congestion_window.saturating_sub(bytes_lost / 2);
        self.congestion_window = reduced.max(min_congestion_window(self.max_segment_size));
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        let rate = self.pacing_rate_bytes_per_second?;
        if rate == 0 || pending_bytes == 0 {
            return None;
        }
        let nanos = (u128::from(pending_bytes) * 1_000_000_000u128) / u128::from(rate);
        Some(Duration::from_nanos(nanos.max(1) as u64))
    }

    fn update_min_rtt(&mut self, sample: TcpBbrAckSample) {
        let expired = self
            .min_rtt_stamp
            .is_some_and(|stamp| sample.now.duration_since(stamp) > TCP_BBR_MIN_RTT_FILTER);
        if self.min_rtt.is_none_or(|min_rtt| sample.rtt <= min_rtt) || expired {
            self.min_rtt = Some(sample.rtt);
            self.min_rtt_stamp = Some(sample.now);
        }
    }

    fn update_bandwidth(&mut self, sample: TcpBbrAckSample) {
        let micros = sample.rtt.as_micros().max(1);
        let sample_rate = ((u128::from(sample.bytes_acked) * 1_000_000u128) / micros)
            .min(u128::from(u64::MAX)) as u64;
        self.max_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second.max(sample_rate);
    }

    fn maybe_enter_probe_rtt(&mut self, now: Instant) {
        let Some(stamp) = self.min_rtt_stamp else {
            return;
        };
        if self.mode == TcpBbrMode::ProbeRtt {
            return;
        }
        if now.duration_since(stamp) <= TCP_BBR_MIN_RTT_FILTER {
            return;
        }
        self.prior_mode = self.mode;
        self.mode = TcpBbrMode::ProbeRtt;
        self.probe_rtt_done_stamp = None;
        self.probe_rtt_round_done = false;
        self.congestion_window = probe_rtt_window(self.max_segment_size);
    }

    fn update_startup(&mut self, sample: TcpBbrAckSample) {
        self.congestion_window = self
            .congestion_window
            .saturating_add(sample.bytes_acked)
            .max(min_congestion_window(self.max_segment_size));

        if !self.round_start {
            return;
        }

        if self.full_bandwidth_bytes_per_second == 0 {
            self.full_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second;
            self.full_bandwidth_rounds = 0;
            return;
        }

        let growth_target = self
            .full_bandwidth_bytes_per_second
            .saturating_add((self.full_bandwidth_bytes_per_second / 4).max(1));
        if self.max_bandwidth_bytes_per_second >= growth_target {
            self.full_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second;
            self.full_bandwidth_rounds = 0;
        } else {
            self.full_bandwidth_rounds = self.full_bandwidth_rounds.saturating_add(1);
        }

        if self.full_bandwidth_rounds >= 3 {
            self.mode = TcpBbrMode::Drain;
        }
    }

    fn update_drain(&mut self, sample: TcpBbrAckSample) {
        self.congestion_window = self.target_congestion_window(TCP_BBR_CWND_GAIN_MILLI);
        if sample.bytes_in_flight <= self.target_congestion_window(1000) {
            self.mode = TcpBbrMode::ProbeBw;
            self.cycle_index = 0;
            self.cycle_stamp = Some(sample.now);
        }
    }

    fn update_probe_bw(&mut self, sample: TcpBbrAckSample) {
        if self.should_advance_probe_bw_cycle(sample.now) {
            self.cycle_index = (self.cycle_index + 1) % PROBE_BW_GAIN_CYCLE.len();
            self.cycle_stamp = Some(sample.now);
        }
        let target = self.target_congestion_window(TCP_BBR_CWND_GAIN_MILLI);
        self.congestion_window = target
            .max(self.congestion_window)
            .max(min_congestion_window(self.max_segment_size));
    }

    fn update_probe_rtt(&mut self, sample: TcpBbrAckSample) {
        let probe_rtt_window = probe_rtt_window(self.max_segment_size);
        self.congestion_window = probe_rtt_window;
        if sample.bytes_in_flight <= probe_rtt_window && self.probe_rtt_done_stamp.is_none() {
            self.probe_rtt_done_stamp = Some(sample.now + TCP_BBR_PROBE_RTT_DURATION);
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
                TcpBbrMode::ProbeRtt => TcpBbrMode::ProbeBw,
                prior_mode => prior_mode,
            };
            if self.mode == TcpBbrMode::ProbeBw {
                self.cycle_index = 0;
                self.cycle_stamp = Some(sample.now);
            }
            self.probe_rtt_done_stamp = None;
            self.probe_rtt_round_done = false;
            self.congestion_window = self.target_congestion_window(TCP_BBR_CWND_GAIN_MILLI);
        }
    }

    fn should_advance_probe_bw_cycle(&self, now: Instant) -> bool {
        let Some(min_rtt) = self.min_rtt else {
            return false;
        };
        self.cycle_stamp
            .is_none_or(|stamp| now.duration_since(stamp) >= min_rtt)
    }

    fn target_congestion_window(&self, gain_milli: u32) -> u32 {
        let Some(min_rtt) = self.min_rtt else {
            return initial_congestion_window(self.max_segment_size);
        };
        if self.max_bandwidth_bytes_per_second == 0 {
            return initial_congestion_window(self.max_segment_size);
        }
        let bytes = u128::from(self.max_bandwidth_bytes_per_second)
            .saturating_mul(min_rtt.as_micros())
            .saturating_mul(u128::from(gain_milli))
            / 1_000_000u128
            / 1000u128;
        bytes.clamp(
            u128::from(min_congestion_window(self.max_segment_size)),
            u128::from(u32::MAX),
        ) as u32
    }

    fn update_pacing_rate(&mut self) {
        if self.max_bandwidth_bytes_per_second == 0 {
            return;
        }
        let gain_milli = match self.mode {
            TcpBbrMode::Startup => TCP_BBR_HIGH_GAIN_MILLI,
            TcpBbrMode::Drain => TCP_BBR_DRAIN_GAIN_MILLI,
            TcpBbrMode::ProbeBw => PROBE_BW_GAIN_CYCLE[self.cycle_index],
            TcpBbrMode::ProbeRtt => 1000,
        };
        let pacing_rate = (u128::from(self.max_bandwidth_bytes_per_second) * u128::from(gain_milli)
            / 1000u128)
            .clamp(1, u128::from(u64::MAX)) as u64;
        self.pacing_rate_bytes_per_second = Some(pacing_rate);
    }
}

#[inline]
fn initial_congestion_window(max_segment_size: u32) -> u32 {
    max_segment_size
        .max(1)
        .saturating_mul(TCP_BBR_INITIAL_WINDOW_SEGMENTS)
}

#[inline]
fn min_congestion_window(max_segment_size: u32) -> u32 {
    max_segment_size
        .max(1)
        .saturating_mul(TCP_BBR_MIN_WINDOW_SEGMENTS)
}

#[inline]
fn probe_rtt_window(max_segment_size: u32) -> u32 {
    min_congestion_window(max_segment_size)
}

impl TcpCongestionControl for TcpBbrState {
    fn algorithm(&self) -> TcpCongestionAlgorithm {
        TcpBbrState::algorithm(self)
    }

    fn congestion_window(&self) -> u32 {
        TcpBbrState::congestion_window(self)
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        TcpBbrState::pacing_rate_bytes_per_second(self)
    }

    fn delivered(&self) -> u64 {
        TcpBbrState::delivered(self)
    }

    fn min_rtt(&self) -> Option<Duration> {
        TcpBbrState::min_rtt(self)
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        TcpBbrState::max_bandwidth_bytes_per_second(self)
    }

    fn on_packet_sent(&mut self, bytes_sent: u32, bytes_in_flight: u32) {
        TcpBbrState::on_packet_sent(self, bytes_sent, bytes_in_flight);
    }

    fn on_ack(&mut self, sample: TcpCongestionAckSample) {
        TcpBbrState::on_ack(self, sample);
    }

    fn on_loss(&mut self, bytes_lost: u32) {
        TcpBbrState::on_loss(self, bytes_lost);
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        TcpBbrState::next_send_delay(self, pending_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAX_SEGMENT_SIZE: u32 = 1440;

    fn ack_sample(
        now: Instant,
        bytes_acked: u32,
        rtt: Duration,
        bytes_in_flight: u32,
    ) -> TcpBbrAckSample {
        TcpBbrAckSample {
            bytes_acked,
            rtt,
            now,
            bytes_in_flight,
        }
    }

    fn test_bbr_state() -> TcpBbrState {
        TcpBbrState::new(TEST_MAX_SEGMENT_SIZE)
    }

    fn test_initial_window() -> u32 {
        initial_congestion_window(TEST_MAX_SEGMENT_SIZE)
    }

    fn test_min_window() -> u32 {
        min_congestion_window(TEST_MAX_SEGMENT_SIZE)
    }

    #[test]
    fn bbr_starts_with_initial_window_and_algorithm() {
        let bbr = test_bbr_state();
        let default_bbr = TcpBbrState::default();

        assert_eq!(bbr.algorithm(), TcpCongestionAlgorithm::Bbr);
        assert_eq!(bbr.mode(), TcpBbrMode::Startup);
        assert_eq!(bbr.max_segment_size(), TEST_MAX_SEGMENT_SIZE);
        assert_eq!(bbr.congestion_window(), test_initial_window());
        assert_eq!(bbr.pacing_rate_bytes_per_second(), None);
        assert_eq!(default_bbr.algorithm(), TcpCongestionAlgorithm::Bbr);
        assert_eq!(
            default_bbr.congestion_window(),
            initial_congestion_window(DEFAULT_TCP_CONGESTION_MAX_SEGMENT_SIZE)
        );
    }

    #[test]
    fn bbr_ack_updates_delivery_rate_min_rtt_window_and_pacing() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        bbr.on_ack(ack_sample(
            now,
            TEST_MAX_SEGMENT_SIZE,
            Duration::from_millis(20),
            test_initial_window(),
        ));

        let expected_bandwidth = 72_000;
        assert_eq!(bbr.mode(), TcpBbrMode::Startup);
        assert_eq!(bbr.delivered(), u64::from(TEST_MAX_SEGMENT_SIZE));
        assert_eq!(bbr.min_rtt(), Some(Duration::from_millis(20)));
        assert_eq!(bbr.max_bandwidth_bytes_per_second(), expected_bandwidth);
        assert!(bbr.congestion_window() > test_initial_window());
        assert_eq!(
            bbr.pacing_rate_bytes_per_second(),
            Some(expected_bandwidth * u64::from(TCP_BBR_HIGH_GAIN_MILLI) / 1000)
        );
    }

    #[test]
    fn bbr_state_implements_tcp_congestion_control_trait() {
        fn acknowledge<C: TcpCongestionControl>(control: &mut C, now: Instant) {
            control.on_ack(ack_sample(
                now,
                TEST_MAX_SEGMENT_SIZE,
                Duration::from_millis(20),
                test_initial_window(),
            ));
        }

        let now = Instant::now();
        let mut bbr = test_bbr_state();

        acknowledge(&mut bbr, now);

        assert_eq!(bbr.delivered(), u64::from(TEST_MAX_SEGMENT_SIZE));
        assert!(bbr.pacing_rate_bytes_per_second().is_some());
    }

    #[test]
    fn bbr_loss_never_cuts_below_min_window() {
        let mut bbr = test_bbr_state();

        bbr.on_loss(u32::MAX);

        assert_eq!(bbr.congestion_window(), test_min_window());
    }

    #[test]
    fn bbr_enters_and_leaves_probe_rtt_after_filter_expiry() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        bbr.on_ack(ack_sample(
            now,
            TEST_MAX_SEGMENT_SIZE,
            Duration::from_millis(10),
            test_initial_window(),
        ));
        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        bbr.on_ack(ack_sample(
            now + TCP_BBR_MIN_RTT_FILTER + Duration::from_millis(1),
            TEST_MAX_SEGMENT_SIZE,
            Duration::from_millis(12),
            test_min_window(),
        ));

        assert_eq!(bbr.mode(), TcpBbrMode::ProbeRtt);
        assert_eq!(bbr.congestion_window(), test_min_window());

        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        bbr.on_ack(ack_sample(
            now + TCP_BBR_MIN_RTT_FILTER + TCP_BBR_PROBE_RTT_DURATION + Duration::from_millis(2),
            TEST_MAX_SEGMENT_SIZE,
            Duration::from_millis(12),
            test_min_window(),
        ));

        assert_eq!(bbr.mode(), TcpBbrMode::Startup);
    }

    #[test]
    fn bbr_next_send_delay_uses_pacing_rate() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        assert_eq!(bbr.next_send_delay(TEST_MAX_SEGMENT_SIZE), None);
        assert_eq!(bbr.next_send_delay(0), None);

        bbr.on_ack(ack_sample(
            now,
            TEST_MAX_SEGMENT_SIZE,
            Duration::from_millis(20),
            test_initial_window(),
        ));

        assert_eq!(bbr.next_send_delay(0), None);
        let pacing_rate = bbr
            .pacing_rate_bytes_per_second()
            .expect("positive ACK should establish pacing");
        let expected_delay_nanos =
            (u128::from(TEST_MAX_SEGMENT_SIZE) * 1_000_000_000u128) / u128::from(pacing_rate);
        assert_eq!(
            bbr.next_send_delay(TEST_MAX_SEGMENT_SIZE),
            Some(Duration::from_nanos(expected_delay_nanos as u64))
        );
    }

    #[test]
    fn bbr_startup_rounds_require_send_markers() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        for round in 0..5 {
            bbr.on_ack(ack_sample(
                now + Duration::from_millis(round),
                TEST_MAX_SEGMENT_SIZE,
                Duration::from_millis(10),
                test_initial_window(),
            ));
        }

        assert_eq!(bbr.mode(), TcpBbrMode::Startup);
    }

    #[test]
    fn bbr_extreme_bdp_saturates_congestion_window() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        bbr.on_packet_sent(u32::MAX, 0);
        bbr.on_ack(ack_sample(
            now,
            u32::MAX,
            Duration::from_micros(1),
            test_min_window(),
        ));
        assert!(bbr.max_bandwidth_bytes_per_second() > 0);

        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        bbr.on_ack(ack_sample(
            now + TCP_BBR_MIN_RTT_FILTER + Duration::from_millis(1),
            TEST_MAX_SEGMENT_SIZE,
            Duration::MAX,
            test_min_window(),
        ));
        assert_eq!(bbr.mode(), TcpBbrMode::ProbeRtt);

        bbr.on_packet_sent(TEST_MAX_SEGMENT_SIZE, 0);
        let exit_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bbr.on_ack(ack_sample(
                now + TCP_BBR_MIN_RTT_FILTER
                    + TCP_BBR_PROBE_RTT_DURATION
                    + Duration::from_millis(2),
                TEST_MAX_SEGMENT_SIZE,
                Duration::MAX,
                test_min_window(),
            ));
        }));

        assert!(
            exit_result.is_ok(),
            "extreme BDP sample should saturate instead of panicking"
        );
        assert_eq!(bbr.congestion_window(), u32::MAX);
    }

    #[test]
    fn bbr_tiny_positive_bandwidth_keeps_positive_pacing_rate() {
        let now = Instant::now();
        let mut bbr = test_bbr_state();

        for round in 0..4 {
            bbr.on_packet_sent(1, 0);
            bbr.on_ack(ack_sample(
                now + Duration::from_millis(round),
                1,
                Duration::from_secs(1),
                test_initial_window(),
            ));
        }

        assert_eq!(bbr.mode(), TcpBbrMode::Drain);
        assert_eq!(bbr.max_bandwidth_bytes_per_second(), 1);
        assert_eq!(bbr.pacing_rate_bytes_per_second(), Some(1));
    }
}
