use hammer_core::data_plane::{BufferFrame, NodeNext, NodeRegistration};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, DriverNode, Node,
    NodeDescriptor, NodeErrorDescriptor, NodeErrorSeverity, NodeProcessFn, NodeResult,
    NodeRuntimeData, TraceControlPlane, TraceFormatter, TraceInputPolicy, TracePolicy,
    add_packet_trace, format_packet_trace, process_frame,
};
use std::sync::atomic::{AtomicU64, Ordering};

static DRIVER_CALLS: AtomicU64 = AtomicU64::new(0);
static INTERNAL_CALLS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, PartialEq, Eq)]
enum TestNext {
    Internal,
}

impl NodeNext for TestNext {
    fn slot(self) -> u16 {
        match self {
            Self::Internal => 0,
        }
    }
}

impl TestNext {
    const COUNT: usize = 1;
}

struct Driver;

impl Node for Driver {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        driver_process(runtime, NodeRuntimeData::empty(), frame)
    }

    fn node_process(&self) -> NodeProcessFn {
        driver_process
    }

    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("owner-driver", TestNext::COUNT)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(u8))
    }
}

impl DriverNode for Driver {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("owner-driver", TestNext::COUNT)
    }
}

fn driver_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    assert_eq!(data, NodeRuntimeData::empty());
    DRIVER_CALLS.fetch_add(1, Ordering::SeqCst);
    process_frame!(runtime, frame, |index| {
        let current = runtime.current_node().expect("driver current node");
        runtime.try_mark_trace(current, index).expect("mark trace");
        add_packet_trace!(runtime, index, 7u8).expect("driver trace");
        TestNext::Internal
    })
}

fn internal_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    assert_eq!(data, NodeRuntimeData::empty());
    INTERNAL_CALLS.fetch_add(1, Ordering::SeqCst);
    for index in frame.pending_indices() {
        add_packet_trace!(runtime, *index, 9u8).expect("internal trace");
    }
    NodeResult::drop()
}

#[test]
fn runtime_owner_registers_dispatches_traces_and_reports_stats() -> RuntimeResult<()> {
    DRIVER_CALLS.store(0, Ordering::SeqCst);
    INTERNAL_CALLS.store(0, Ordering::SeqCst);

    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 128,
            buffer_slots: 8,
            frame_slots: 8,
            ..DataPlaneBufferConfig::default()
        },
    });

    let internal = runtime.nodes().try_register_descriptor(
        hammer_core::data_plane::NodeKind::Internal,
        NodeDescriptor::new(
            internal_process,
            NodeRuntimeData::empty(),
            NodeRegistration::next("owner-internal", 0),
            &[],
            Some(format_packet_trace!(u8)),
        ),
    )?;
    let driver = runtime
        .nodes()
        .try_register_driver_with_next_names(Driver, &["owner-internal"])?;
    runtime.nodes().resolve_named_next_nodes()?;
    assert_eq!(
        runtime.nodes().node_next(driver, TestNext::Internal)?,
        internal
    );
    let driver_errors = [
        NodeErrorDescriptor::new("error-0", NodeErrorSeverity::Error, "test error"),
        NodeErrorDescriptor::new("error-1", NodeErrorSeverity::Error, "test error"),
        NodeErrorDescriptor::new("error-2", NodeErrorSeverity::Error, "test error"),
        NodeErrorDescriptor::new("error-3", NodeErrorSeverity::Error, "test error"),
        NodeErrorDescriptor::new("error-4", NodeErrorSeverity::Error, "test error"),
        NodeErrorDescriptor::new("error-5", NodeErrorSeverity::Error, "test error"),
    ];
    runtime
        .nodes()
        .materialize_node_errors(driver, &driver_errors)?;

    let trace = TraceControlPlane::new(8);
    runtime.set_trace_control(Some(trace.handle()));
    trace.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: Vec::from_iter([TraceInputPolicy {
            node: driver,
            count: 1,
        }]),
    });
    let worker = runtime.for_worker(1, 0)?;
    assert!(worker.may_mark_trace(driver));

    let mut frame = runtime.buffers().get_next_frame(driver)?;
    frame.push_index(runtime.alloc_index_with_bytes(b"pkt")?)?;
    runtime.put_next_frame(frame)?;

    assert_eq!(runtime.run_ready_nodes()?, 2);
    assert_eq!(DRIVER_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(INTERNAL_CALLS.load(Ordering::SeqCst), 1);

    assert_eq!(trace.drain_completed(), 1);
    assert_eq!(trace.records().len(), 1);
    assert_eq!(trace.records().len(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, driver);
    assert_eq!(records[0].entries.len(), 2);
    assert_eq!(records[0].entries[0].format_payload(), "u8 0x07");

    Ok(())
}
