use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_queue::SegQueue;
use hammer_core::config::TraceOptions;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::log::Logger;
use hammer_infra::map::FlatHashTable;

use crate::node::NodeId;

pub type TraceFormatter = fn(&[u8]) -> String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceInputPolicy {
    pub node: NodeId,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracePolicy {
    pub enabled: bool,
    pub record_capacity: usize,
    pub packet_capacity: usize,
    pub inputs: Vec<TraceInputPolicy>,
}

impl TracePolicy {
    pub fn disabled(record_capacity: usize, packet_capacity: usize) -> Self {
        Self {
            enabled: false,
            record_capacity,
            packet_capacity,
            inputs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceRecord {
    pub epoch: u64,
    pub input_node: NodeId,
    pub input_node_name: Option<&'static str>,
    pub entries: Vec<TraceEntry>,
}

#[derive(Debug, Clone)]
pub struct TraceEntry {
    pub node: NodeId,
    pub node_name: Option<&'static str>,
    pub payload_bytes: Vec<u8>,
    pub formatter: Option<TraceFormatter>,
}

impl TraceEntry {
    #[inline]
    pub fn format_payload(&self) -> String {
        match self.formatter {
            Some(formatter) => formatter(&self.payload_bytes),
            None => format_raw_payload(&self.payload_bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceMark {
    pub handle: u32,
    pub epoch: u64,
}

#[derive(Debug, Default, Clone)]
pub struct DataPlaneTrace {
    inner: std::rc::Rc<std::cell::RefCell<DataPlaneTraceState>>,
}

#[derive(Debug, Default)]
struct DataPlaneTraceState {
    control: Option<TraceControlHandle>,
    packets: Vec<Option<PacketTraceState>>,
    free: Vec<u32>,
    packet_capacity: usize,
}

#[derive(Debug)]
struct PacketTraceState {
    epoch: u64,
    input_node: NodeId,
    input_node_name: Option<&'static str>,
    entries: Vec<TraceEntry>,
}

pub trait PacketTrace {
    fn encode_trace(&self, out: &mut Vec<u8>);
}

#[inline(always)]
pub fn unlikely(value: bool) -> bool {
    if value {
        core::hint::cold_path();
    }
    value
}

#[derive(Debug, Clone)]
pub struct TraceControlPlane {
    inner: Arc<TraceControlInner>,
}

#[derive(Debug, Clone)]
pub struct TraceControlHandle {
    inner: Arc<TraceControlInner>,
}

#[derive(Debug, Clone)]
pub struct TraceRecordSink {
    inner: Arc<TraceControlInner>,
}

#[derive(Debug)]
struct TraceControlInner {
    state: Mutex<TraceControlState>,
    completed: SegQueue<TraceRecord>,
    completed_len: AtomicUsize,
    completed_capacity: AtomicUsize,
    dropped_completed: AtomicUsize,
    next_epoch: AtomicU64,
}

#[derive(Debug)]
struct TraceControlState {
    epoch: u64,
    policy: TracePolicy,
    quotas: FlatHashTable<u32, u32>,
    ring: VecDeque<TraceRecord>,
}

impl TraceControlPlane {
    pub fn new(record_capacity: usize) -> Self {
        let record_capacity = record_capacity.max(1);
        Self {
            inner: Arc::new(TraceControlInner {
                state: Mutex::new(TraceControlState {
                    epoch: 0,
                    policy: TracePolicy::disabled(record_capacity, 1),
                    quotas: FlatHashTable::new(),
                    ring: VecDeque::with_capacity(record_capacity),
                }),
                completed: SegQueue::new(),
                completed_len: AtomicUsize::new(0),
                completed_capacity: AtomicUsize::new(record_capacity),
                dropped_completed: AtomicUsize::new(0),
                next_epoch: AtomicU64::new(1),
            }),
        }
    }

    pub fn handle(&self) -> TraceControlHandle {
        TraceControlHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn sink(&self) -> TraceRecordSink {
        TraceRecordSink {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn publish(&self, policy: TracePolicy) -> u64 {
        let epoch = self.inner.next_epoch.fetch_add(1, Ordering::AcqRel);
        let mut state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        state.epoch = epoch;
        state.quotas = if policy.enabled {
            FlatHashTable::from_entries(
                policy
                    .inputs
                    .iter()
                    .map(|input| (input.node.slot(), input.count)),
            )
        } else {
            FlatHashTable::new()
        };
        let record_capacity = policy.record_capacity.max(1);
        self.inner
            .completed_capacity
            .store(record_capacity, Ordering::Release);
        trim_completed_queue(&self.inner, record_capacity);
        resize_bounded_queue(&mut state.ring, record_capacity);
        if state.ring.capacity() != record_capacity {
            state.ring = VecDeque::with_capacity(record_capacity);
        }
        state.policy = policy;
        epoch
    }

    pub fn publish_options(
        &self,
        options: &TraceOptions,
        resolve_node: impl Fn(&str) -> Option<NodeId>,
    ) -> CoreResult<u64> {
        let mut inputs = Vec::with_capacity(options.inputs.len());
        if options.enabled {
            for input in &options.inputs {
                let node = resolve_node(&input.node).ok_or_else(|| {
                    CoreError::config_validation(format!(
                        "trace.inputs node is not a declared packet node: {}",
                        input.node
                    ))
                })?;
                inputs.push(TraceInputPolicy {
                    node,
                    count: input.count,
                });
            }
        }
        Ok(self.publish(TracePolicy {
            enabled: options.enabled,
            record_capacity: options.record_capacity.max(1),
            packet_capacity: options.packet_capacity.max(1),
            inputs,
        }))
    }

    pub fn drain_completed(&self) -> usize {
        self.sink().drain_completed()
    }

    pub fn take_records(&self) -> Vec<TraceRecord> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        state.ring.drain(..).collect()
    }

    pub fn dropped_completed(&self) -> usize {
        self.inner.dropped_completed.load(Ordering::Acquire)
    }
}

impl DataPlaneTrace {
    pub fn set_control(&self, control: Option<TraceControlHandle>, packet_capacity: usize) {
        let mut state = self.inner.borrow_mut();
        state.control = control;
        state.packet_capacity = packet_capacity;
        if state.packets.len() > packet_capacity {
            state.packets.truncate(packet_capacity);
            state.free.retain(|slot| (*slot as usize) < packet_capacity);
        }
    }

    pub fn try_mark(&self, node: NodeId, node_name: Option<&'static str>) -> Option<TraceMark> {
        {
            let state = self.inner.borrow();
            state.control.as_ref()?;
            if !state.has_packet_capacity() {
                return None;
            }
        }

        let control = self.inner.borrow().control.clone()?;
        let mark = control.try_mark(node)?;
        let mut state = self.inner.borrow_mut();
        let slot = state.alloc_packet_slot()?;
        state.packets[slot as usize] = Some(PacketTraceState {
            epoch: mark.epoch,
            input_node: node,
            input_node_name: node_name,
            entries: Vec::new(),
        });
        Some(TraceMark {
            handle: slot + 1,
            epoch: mark.epoch,
        })
    }

    pub fn may_mark(&self, node: NodeId) -> bool {
        let state = self.inner.borrow();
        state.has_packet_capacity()
            && state
                .control
                .as_ref()
                .is_some_and(|control| control.may_mark(node))
    }

    pub fn add_entry(
        &self,
        mark: TraceMark,
        node: NodeId,
        node_name: Option<&'static str>,
        formatter: Option<TraceFormatter>,
        payload_bytes: Vec<u8>,
    ) {
        let control = self.inner.borrow().control.clone();
        let Some(control) = control else {
            return;
        };
        if !control.is_epoch_writable(mark.epoch) {
            return;
        }
        let slot = match mark.handle.checked_sub(1) {
            Some(slot) => slot as usize,
            None => return,
        };
        let mut state = self.inner.borrow_mut();
        let Some(Some(packet)) = state.packets.get_mut(slot) else {
            return;
        };
        if packet.epoch != mark.epoch {
            return;
        }
        packet.entries.push(TraceEntry {
            node,
            node_name,
            payload_bytes,
            formatter,
        });
    }

    pub fn finalize(&self, mark: TraceMark) {
        let slot = match mark.handle.checked_sub(1) {
            Some(slot) => slot as usize,
            None => return,
        };
        let control = self.inner.borrow().control.clone();
        let mut state = self.inner.borrow_mut();
        let Some(packet_slot) = state.packets.get_mut(slot) else {
            return;
        };
        let Some(packet) = packet_slot.take() else {
            return;
        };
        state.free.push(slot as u32);
        drop(state);
        if packet.entries.is_empty() {
            return;
        }
        if let Some(control) = control {
            control.push_completed_record(TraceRecord {
                epoch: packet.epoch,
                input_node: packet.input_node,
                input_node_name: packet.input_node_name,
                entries: packet.entries,
            });
        }
    }
}

impl DataPlaneTraceState {
    fn has_packet_capacity(&self) -> bool {
        self.packet_capacity != 0
            && (!self.free.is_empty() || self.packets.len() < self.packet_capacity)
    }

    fn alloc_packet_slot(&mut self) -> Option<u32> {
        if let Some(slot) = self.free.pop() {
            return Some(slot);
        }
        if self.packets.len() >= self.packet_capacity {
            return None;
        }
        let slot = u32::try_from(self.packets.len()).ok()?;
        self.packets.push(None);
        Some(slot)
    }
}

impl TraceControlHandle {
    pub fn may_mark(&self, node: NodeId) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        state.policy.enabled && state.quotas.lookup(&node.slot()).unwrap_or(0) > 0
    }

    pub fn try_mark(&self, node: NodeId) -> Option<TraceMark> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        if !state.policy.enabled {
            return None;
        }
        let epoch = state.epoch;
        let quota = state.quotas.lookup(&node.slot())?;
        if quota == 0 {
            return None;
        }
        state.quotas.insert(node.slot(), quota - 1);
        Some(TraceMark { handle: 0, epoch })
    }

    pub fn is_epoch_writable(&self, epoch: u64) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        state.epoch == epoch
    }

    pub fn push_completed_record(&self, record: TraceRecord) {
        loop {
            let capacity = self.inner.completed_capacity.load(Ordering::Acquire).max(1);
            let current = self.inner.completed_len.load(Ordering::Acquire);
            if current >= capacity {
                self.inner.dropped_completed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if self
                .inner
                .completed_len
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inner.completed.push(record);
                return;
            }
        }
    }
}

impl TraceRecordSink {
    pub fn drain_completed(&self) -> usize {
        self.drain_completed_inner(None)
    }

    pub fn drain_completed_with_logger(&self, logger: &Logger) -> usize {
        self.drain_completed_inner(Some(logger))
    }

    fn drain_completed_inner(&self, logger: Option<&Logger>) -> usize {
        let mut drained = 0usize;
        while let Some(record) = self.inner.completed.pop() {
            self.inner.completed_len.fetch_sub(1, Ordering::AcqRel);
            if let Some(logger) = logger {
                logger.debug(format_trace_record(&record));
            }
            let mut state = self
                .inner
                .state
                .lock()
                .expect("trace control lock poisoned");
            push_ring(&mut state, record);
            drained += 1;
        }
        drained
    }

    pub fn take_records(&self) -> Vec<TraceRecord> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("trace control lock poisoned");
        state.ring.drain(..).collect()
    }

    pub fn dropped_completed(&self) -> usize {
        self.inner.dropped_completed.load(Ordering::Acquire)
    }
}

fn push_ring(state: &mut TraceControlState, record: TraceRecord) {
    let capacity = state.policy.record_capacity.max(1);
    while state.ring.len() >= capacity {
        state.ring.pop_front();
    }
    state.ring.push_back(record);
}

fn resize_bounded_queue(queue: &mut VecDeque<TraceRecord>, capacity: usize) {
    while queue.len() > capacity {
        queue.pop_front();
    }
}

fn trim_completed_queue(inner: &TraceControlInner, capacity: usize) {
    while inner.completed_len.load(Ordering::Acquire) > capacity {
        let Some(_) = inner.completed.pop() else {
            return;
        };
        inner.completed_len.fetch_sub(1, Ordering::AcqRel);
        inner.dropped_completed.fetch_add(1, Ordering::Relaxed);
    }
}

fn format_trace_record(record: &TraceRecord) -> String {
    let mut line = format!(
        "packet trace epoch={} input={}",
        record.epoch,
        node_label(record.input_node, record.input_node_name)
    );
    for entry in &record.entries {
        line.push_str(" | ");
        line.push_str(&node_label(entry.node, entry.node_name));
        line.push_str(": ");
        line.push_str(&entry.format_payload());
    }
    line
}

fn node_label(node: NodeId, name: Option<&'static str>) -> String {
    match name {
        Some(name) => name.to_owned(),
        None => format!("node#{}", node.slot()),
    }
}

fn format_raw_payload(payload: &[u8]) -> String {
    let mut output = String::with_capacity(2 + payload.len() * 2);
    output.push_str("0x");
    for byte in payload {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
