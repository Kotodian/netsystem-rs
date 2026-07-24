use std::fmt;
use std::mem::{MaybeUninit, align_of, size_of};
use std::time::{Duration, Instant};

use hammer_service::transport::congestion::{
    AckedPacket, BbrController, CongestionController, CongestionMetrics, CubicController,
    LostPacket, PacketNumber, RttSample,
};

use crate::config::CongestionAlgorithm;

const PRIVATE_BYTES: usize = 256;
const PRIVATE_ALIGN: usize = 64;

#[repr(C, align(64))]
struct Private([MaybeUninit<u8>; PRIVATE_BYTES]);

impl Private {
    fn uninit() -> Self {
        Self([MaybeUninit::uninit(); PRIVATE_BYTES])
    }

    unsafe fn get<C>(&self) -> &C {
        // SAFETY: callers use the typed trampoline from the `Algorithm` that
        // initialized this storage, which proves type, alignment, and validity.
        unsafe { &*self.0.as_ptr().cast::<C>() }
    }

    unsafe fn get_mut<C>(&mut self) -> &mut C {
        // SAFETY: the same pairing as `get` holds, and callers hold exclusive
        // access to the owning `State`.
        unsafe { &mut *self.0.as_mut_ptr().cast::<C>() }
    }

    unsafe fn write<C>(&mut self, value: C) {
        // SAFETY: `Algorithm::new::<C>` checks size and alignment at compile
        // time, and constructors call this only for uninitialized storage.
        unsafe { self.0.as_mut_ptr().cast::<C>().write(value) };
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Algorithm {
    name: &'static str,
    initialize: fn(&'static Algorithm, u32) -> State,
    clone: fn(&Private, &mut Private),
    drop: fn(&mut Private),
    debug: fn(&Private, &mut fmt::Formatter<'_>) -> fmt::Result,
    metrics: fn(&Private) -> CongestionMetrics,
    max_datagram_size: fn(&Private) -> u32,
    congestion_window: fn(&Private) -> u32,
    pacing_rate_bytes_per_second: fn(&Private) -> Option<u64>,
    delivered: fn(&Private) -> u64,
    min_rtt: fn(&Private) -> Option<Duration>,
    max_bandwidth_bytes_per_second: fn(&Private) -> u64,
    on_packet_sent: fn(&mut Private, PacketNumber, u32, u32, Instant),
    on_ack: fn(&mut Private, Instant, AckedPacket, RttSample, u32),
    on_end_acks: fn(&mut Private, Instant, u32, bool, PacketNumber),
    on_loss: fn(&mut Private, Instant, LostPacket, bool),
    on_mtu_update: fn(&mut Private, u32),
    next_send_delay: fn(&Private, u32) -> Option<Duration>,
}

impl fmt::Debug for Algorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Algorithm")
            .field(&self.name)
            .finish()
    }
}

impl Algorithm {
    const fn new<C>(name: &'static str) -> Self
    where
        C: CongestionController,
    {
        assert!(size_of::<C>() <= PRIVATE_BYTES);
        assert!(align_of::<C>() <= PRIVATE_ALIGN);
        Self {
            name,
            initialize: initialize_controller::<C>,
            clone: clone_controller::<C>,
            drop: drop_controller::<C>,
            debug: debug_controller::<C>,
            metrics: metrics::<C>,
            max_datagram_size: max_datagram_size::<C>,
            congestion_window: congestion_window::<C>,
            pacing_rate_bytes_per_second: pacing_rate_bytes_per_second::<C>,
            delivered: delivered::<C>,
            min_rtt: min_rtt::<C>,
            max_bandwidth_bytes_per_second: max_bandwidth_bytes_per_second::<C>,
            on_packet_sent: on_packet_sent::<C>,
            on_ack: on_ack::<C>,
            on_end_acks: on_end_acks::<C>,
            on_loss: on_loss::<C>,
            on_mtu_update: on_mtu_update::<C>,
            next_send_delay: next_send_delay::<C>,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test<C>(name: &'static str) -> Self
    where
        C: CongestionController,
    {
        Self::new::<C>(name)
    }
}

const BBR: Algorithm = Algorithm::new::<BbrController>("bbr");
const CUBIC: Algorithm = Algorithm::new::<CubicController>("cubic");

pub(crate) const fn resolve(algorithm: CongestionAlgorithm) -> &'static Algorithm {
    match algorithm {
        CongestionAlgorithm::Bbr => &BBR,
        CongestionAlgorithm::Cubic => &CUBIC,
    }
}

pub(crate) struct State {
    algorithm: &'static Algorithm,
    private: Private,
}

impl State {
    pub(crate) fn new(algorithm: &'static Algorithm, max_datagram_size: u32) -> Self {
        (algorithm.initialize)(algorithm, max_datagram_size)
    }

    fn with<C>(algorithm: &'static Algorithm, max_datagram_size: u32) -> Self
    where
        C: CongestionController,
    {
        let mut private = Private::uninit();
        // SAFETY: `Algorithm::new::<C>` proves the storage size and alignment
        // at compile time, and this initializes `private` exactly once.
        unsafe { private.write(C::new(max_datagram_size)) };
        Self { algorithm, private }
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        let mut private = Private::uninit();
        // SAFETY: `self.algorithm` is the table that initialized `self.private`.
        // Its clone trampoline reads that exact concrete type and initializes
        // the equally aligned destination once.
        (self.algorithm.clone)(&self.private, &mut private);
        Self {
            algorithm: self.algorithm,
            private,
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        // SAFETY: every constructor initializes `private` once and stores the
        // matching static algorithm table. `State::drop` runs exactly once.
        (self.algorithm.drop)(&mut self.private);
    }
}

impl fmt::Debug for State {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugState<'state>(&'state State);

        impl fmt::Debug for DebugState<'_> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                // SAFETY: `algorithm` and `private` are created as one pair and
                // neither can be replaced independently.
                (self.0.algorithm.debug)(&self.0.private, formatter)
            }
        }

        formatter
            .debug_struct("CongestionState")
            .field("algorithm", &self.algorithm.name)
            .field("state", &DebugState(self))
            .finish()
    }
}

impl CongestionController for State {
    fn new(max_datagram_size: u32) -> Self {
        Self::new(&BBR, max_datagram_size)
    }

    fn metrics(&self) -> CongestionMetrics {
        (self.algorithm.metrics)(&self.private)
    }

    fn max_datagram_size(&self) -> u32 {
        (self.algorithm.max_datagram_size)(&self.private)
    }

    fn congestion_window(&self) -> u32 {
        (self.algorithm.congestion_window)(&self.private)
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        (self.algorithm.pacing_rate_bytes_per_second)(&self.private)
    }

    fn delivered(&self) -> u64 {
        (self.algorithm.delivered)(&self.private)
    }

    fn min_rtt(&self) -> Option<Duration> {
        (self.algorithm.min_rtt)(&self.private)
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        (self.algorithm.max_bandwidth_bytes_per_second)(&self.private)
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        (self.algorithm.on_packet_sent)(
            &mut self.private,
            packet_number,
            bytes_sent,
            bytes_in_flight,
            now,
        );
    }

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32) {
        (self.algorithm.on_ack)(&mut self.private, now, acked, rtt, bytes_in_flight);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        (self.algorithm.on_end_acks)(
            &mut self.private,
            now,
            bytes_in_flight,
            app_limited,
            largest_acked_packet,
        );
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool) {
        (self.algorithm.on_loss)(&mut self.private, now, lost, persistent_congestion);
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        (self.algorithm.on_mtu_update)(&mut self.private, max_datagram_size);
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        (self.algorithm.next_send_delay)(&self.private, pending_bytes)
    }
}

fn initialize_controller<C>(algorithm: &'static Algorithm, max_datagram_size: u32) -> State
where
    C: CongestionController,
{
    State::with::<C>(algorithm, max_datagram_size)
}

fn clone_controller<C>(source: &Private, destination: &mut Private)
where
    C: CongestionController,
{
    // SAFETY: the owning `Algorithm` was created for `C`, and `State` keeps
    // that table paired with storage initialized as `C` for its full lifetime.
    unsafe { destination.write(source.get::<C>().clone()) };
}

fn drop_controller<C>(private: &mut Private)
where
    C: CongestionController,
{
    // SAFETY: the owning `Algorithm` was created for `C`, and `State::drop`
    // invokes this trampoline exactly once for initialized storage.
    unsafe { std::ptr::drop_in_place(private.get_mut::<C>()) };
}

fn debug_controller<C>(private: &Private, formatter: &mut fmt::Formatter<'_>) -> fmt::Result
where
    C: CongestionController,
{
    // SAFETY: the owning `Algorithm` was created for `C` and remains paired
    // with storage initialized as `C`.
    unsafe { fmt::Debug::fmt(private.get::<C>(), formatter) }
}

fn metrics<C>(private: &Private) -> CongestionMetrics
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().metrics() }
}

fn max_datagram_size<C>(private: &Private) -> u32
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().max_datagram_size() }
}

fn congestion_window<C>(private: &Private) -> u32
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().congestion_window() }
}

fn pacing_rate_bytes_per_second<C>(private: &Private) -> Option<u64>
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().pacing_rate_bytes_per_second() }
}

fn delivered<C>(private: &Private) -> u64
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().delivered() }
}

fn min_rtt<C>(private: &Private) -> Option<Duration>
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().min_rtt() }
}

fn max_bandwidth_bytes_per_second<C>(private: &Private) -> u64
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().max_bandwidth_bytes_per_second() }
}

fn on_packet_sent<C>(
    private: &mut Private,
    packet_number: PacketNumber,
    bytes_sent: u32,
    bytes_in_flight: u32,
    now: Instant,
) where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; mutable access is exclusive through
    // `&mut State` and therefore cannot alias another `C` reference.
    unsafe {
        private
            .get_mut::<C>()
            .on_packet_sent(packet_number, bytes_sent, bytes_in_flight, now)
    };
}

fn on_ack<C>(
    private: &mut Private,
    now: Instant,
    acked: AckedPacket,
    rtt: RttSample,
    bytes_in_flight: u32,
) where
    C: CongestionController,
{
    // SAFETY: see `on_packet_sent`.
    unsafe {
        private
            .get_mut::<C>()
            .on_ack(now, acked, rtt, bytes_in_flight)
    };
}

fn on_end_acks<C>(
    private: &mut Private,
    now: Instant,
    bytes_in_flight: u32,
    app_limited: bool,
    largest_acked_packet: PacketNumber,
) where
    C: CongestionController,
{
    // SAFETY: see `on_packet_sent`.
    unsafe {
        private
            .get_mut::<C>()
            .on_end_acks(now, bytes_in_flight, app_limited, largest_acked_packet)
    };
}

fn on_loss<C>(private: &mut Private, now: Instant, lost: LostPacket, persistent_congestion: bool)
where
    C: CongestionController,
{
    // SAFETY: see `on_packet_sent`.
    unsafe {
        private
            .get_mut::<C>()
            .on_loss(now, lost, persistent_congestion)
    };
}

fn on_mtu_update<C>(private: &mut Private, max_datagram_size: u32)
where
    C: CongestionController,
{
    // SAFETY: see `on_packet_sent`.
    unsafe { private.get_mut::<C>().on_mtu_update(max_datagram_size) };
}

fn next_send_delay<C>(private: &Private, pending_bytes: u32) -> Option<Duration>
where
    C: CongestionController,
{
    // SAFETY: see `debug_controller`; all typed trampolines share this pairing.
    unsafe { private.get::<C>().next_send_delay(pending_bytes) }
}
