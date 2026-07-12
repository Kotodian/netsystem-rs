//! Graph Fanout verification smoke (#54): delivery order and zero warmed allocations.
//!
//! Uses a counting global allocator so allocation instrumentation—not timing—
//! proves the warmed grouping/transfer section performs zero heap allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, DataPlaneBufferConfig, Frame, Index, Next, NodeId,
    NodeKind, NodeRegistration,
};
use hammer_core::error::CoreResult;
use hammer_runtime::node::{NodeDescriptor, NodeProcessFn, NodeResult, NodeRuntimeData};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

struct CountingAllocator;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
        ALLOC_BYTES.fetch_add(new_size, Ordering::SeqCst);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

static TEST_LOCK: Mutex<()> = Mutex::new(());
static SINK: [Mutex<Vec<Index>>; 4] = [
    Mutex::new(Vec::new()),
    Mutex::new(Vec::new()),
    Mutex::new(Vec::new()),
    Mutex::new(Vec::new()),
];

fn clear_sinks() {
    for sink in &SINK {
        sink.lock().expect("sink").clear();
    }
}

fn collect(slot: usize) -> NodeProcessFn {
    match slot {
        0 => |_, _, frame: &mut BufferFrame| {
            SINK[0]
                .lock()
                .expect("sink")
                .extend_from_slice(frame.indices());
            frame.discard_prefix(frame.len());
            NodeResult::drop()
        },
        1 => |_, _, frame: &mut BufferFrame| {
            SINK[1]
                .lock()
                .expect("sink")
                .extend_from_slice(frame.indices());
            frame.discard_prefix(frame.len());
            NodeResult::drop()
        },
        2 => |_, _, frame: &mut BufferFrame| {
            SINK[2]
                .lock()
                .expect("sink")
                .extend_from_slice(frame.indices());
            frame.discard_prefix(frame.len());
            NodeResult::drop()
        },
        _ => |_, _, frame: &mut BufferFrame| {
            SINK[3]
                .lock()
                .expect("sink")
                .extend_from_slice(frame.indices());
            frame.discard_prefix(frame.len());
            NodeResult::drop()
        },
    }
}

fn register_sink(
    runtime: &DataPlaneRuntime,
    name: &'static str,
    slot: usize,
) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            collect(slot),
            NodeRuntimeData::empty(),
            NodeRegistration::next(name, 0),
            &[],
            None,
        ),
    )
}

fn register_owner(runtime: &DataPlaneRuntime, nexts: &[NodeId]) -> CoreResult<NodeId> {
    fn noop(_: &DataPlaneRuntime, _: NodeRuntimeData, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            noop,
            NodeRuntimeData::empty(),
            NodeRegistration::next("fanout-owner", nexts.len()),
            nexts,
            None,
        ),
    )
}

fn test_runtime(frame_slots: usize, buffer_slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    })
}

fn snapshot_allocs() -> (usize, usize) {
    (
        ALLOC_COUNT.load(Ordering::SeqCst),
        ALLOC_BYTES.load(Ordering::SeqCst),
    )
}

fn fanout_once(runtime: &DataPlaneRuntime, owner: NodeId, frame: &mut Frame<Next>, nexts: &[u16]) {
    runtime.with_current_node(owner, || {
        runtime.enqueue_to_next(frame, nexts);
    });
}

fn build_frame(
    runtime: &DataPlaneRuntime,
    owner: NodeId,
    count: usize,
) -> CoreResult<(Frame<Next>, Vec<Index>)> {
    let mut indices = Vec::with_capacity(count);
    let mut frame = runtime.buffers().get_next_frame(owner)?;
    for offset in 0..count {
        let index = runtime.alloc_index_with_bytes(&[(offset % 256) as u8])?;
        frame.push_index(index)?;
        indices.push(index);
    }
    Ok((frame, indices))
}

#[test]
fn fanout_256_single_next_delivers_stable_order() -> CoreResult<()> {
    let _guard = TEST_LOCK.lock().expect("lock");
    clear_sinks();
    let runtime = test_runtime(64, DEFAULT_BUFFER_FRAME_CAPACITY + 64);
    let a = register_sink(&runtime, "a", 0)?;
    let b = register_sink(&runtime, "b", 1)?;
    let owner = register_owner(&runtime, &[a, b])?;
    let (mut frame, indices) = build_frame(&runtime, owner, DEFAULT_BUFFER_FRAME_CAPACITY)?;
    let nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    fanout_once(&runtime, owner, &mut frame, &nexts);
    drop(frame);
    assert_eq!(runtime.run_ready_nodes()?, 1);
    assert_eq!(SINK[0].lock().expect("a").as_slice(), indices.as_slice());
    assert!(SINK[1].lock().expect("b").is_empty());
    Ok(())
}

#[test]
fn fanout_256_alternating_two_next_keeps_per_next_order() -> CoreResult<()> {
    let _guard = TEST_LOCK.lock().expect("lock");
    clear_sinks();
    let runtime = test_runtime(64, DEFAULT_BUFFER_FRAME_CAPACITY + 64);
    let a = register_sink(&runtime, "a", 0)?;
    let b = register_sink(&runtime, "b", 1)?;
    let owner = register_owner(&runtime, &[a, b])?;
    let (mut frame, indices) = build_frame(&runtime, owner, DEFAULT_BUFFER_FRAME_CAPACITY)?;
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    for (slot, next) in nexts.iter_mut().enumerate() {
        *next = (slot % 2) as u16;
    }
    fanout_once(&runtime, owner, &mut frame, &nexts);
    drop(frame);
    assert_eq!(runtime.run_ready_nodes()?, 2);
    let expected_a: Vec<_> = indices.iter().step_by(2).copied().collect();
    let expected_b: Vec<_> = indices.iter().skip(1).step_by(2).copied().collect();
    assert_eq!(SINK[0].lock().expect("a").as_slice(), expected_a.as_slice());
    assert_eq!(SINK[1].lock().expect("b").as_slice(), expected_b.as_slice());
    Ok(())
}

#[test]
fn fanout_256_multi_next_keeps_per_next_order() -> CoreResult<()> {
    let _guard = TEST_LOCK.lock().expect("lock");
    clear_sinks();
    let runtime = test_runtime(64, DEFAULT_BUFFER_FRAME_CAPACITY + 64);
    let sinks = [
        register_sink(&runtime, "s0", 0)?,
        register_sink(&runtime, "s1", 1)?,
        register_sink(&runtime, "s2", 2)?,
        register_sink(&runtime, "s3", 3)?,
    ];
    let owner = register_owner(&runtime, &sinks)?;
    let (mut frame, indices) = build_frame(&runtime, owner, DEFAULT_BUFFER_FRAME_CAPACITY)?;
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    for (slot, next) in nexts.iter_mut().enumerate() {
        *next = (slot % 4) as u16;
    }
    fanout_once(&runtime, owner, &mut frame, &nexts);
    drop(frame);
    assert_eq!(runtime.run_ready_nodes()?, 4);
    for arc in 0..4 {
        let expected: Vec<_> = indices
            .iter()
            .enumerate()
            .filter_map(|(slot, index)| ((slot % 4) == arc).then_some(*index))
            .collect();
        assert_eq!(
            SINK[arc].lock().expect("sink").as_slice(),
            expected.as_slice(),
            "arc {arc}"
        );
    }
    Ok(())
}

#[test]
fn warmed_fanout_grouping_allocates_zero_heap() -> CoreResult<()> {
    let _guard = TEST_LOCK.lock().expect("lock");
    clear_sinks();
    let runtime = test_runtime(128, DEFAULT_BUFFER_FRAME_CAPACITY * 4);
    let a = register_sink(&runtime, "a", 0)?;
    let b = register_sink(&runtime, "b", 1)?;
    let owner = register_owner(&runtime, &[a, b])?;

    // Warm: fixture construction, graph registration, and first transfer stay
    // outside the measured section.
    let (mut warm_frame, _) = build_frame(&runtime, owner, DEFAULT_BUFFER_FRAME_CAPACITY)?;
    let warm_nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    fanout_once(&runtime, owner, &mut warm_frame, &warm_nexts);
    drop(warm_frame);
    let _ = runtime.run_ready_nodes()?;
    clear_sinks();

    let (mut frame, indices) = build_frame(&runtime, owner, DEFAULT_BUFFER_FRAME_CAPACITY)?;
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    for (slot, next) in nexts.iter_mut().enumerate() {
        *next = (slot % 2) as u16;
    }

    let before = snapshot_allocs();
    fanout_once(&runtime, owner, &mut frame, &nexts);
    let after = snapshot_allocs();
    drop(frame);

    assert_eq!(
        after.0,
        before.0,
        "warmed fanout allocated {} times ({} bytes)",
        after.0 - before.0,
        after.1.saturating_sub(before.1)
    );
    assert_eq!(after.1, before.1);

    assert_eq!(runtime.run_ready_nodes()?, 2);
    let expected_a: Vec<_> = indices.iter().step_by(2).copied().collect();
    let expected_b: Vec<_> = indices.iter().skip(1).step_by(2).copied().collect();
    assert_eq!(SINK[0].lock().expect("a").as_slice(), expected_a.as_slice());
    assert_eq!(SINK[1].lock().expect("b").as_slice(), expected_b.as_slice());
    Ok(())
}
