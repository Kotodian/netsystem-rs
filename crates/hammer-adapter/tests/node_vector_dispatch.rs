use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, DataPlaneInstructionSet, DataPlaneRuntime,
    DriverNode, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
    PooledBufferFrame, RouteMetadata,
};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Prefetch(Vec<u8>),
    Route(Vec<u8>),
}

type SharedEvents = Rc<RefCell<Vec<Event>>>;

thread_local! {
    static SINK_PACKETS: RefCell<Vec<Vec<u8>>> = RefCell::new(vec![Vec::new(); 8]);
}

struct SinkNode {
    slot: usize,
}

impl Node for SinkNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "sink test node must run through its function slot",
        ))
    }

    fn node_process(&self) -> NodeProcessFn {
        sink_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        NodeRuntimeData::from_usize(self.slot)
    }
}

impl DriverNode for SinkNode {}

#[test]
fn quad_prefetches_future_chunks_before_later_chunk_processing() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Avx2);
    let mut frame = packet_frame(&runtime, 8);
    let events = Rc::new(RefCell::new(Vec::new()));
    let node = register_sink(&runtime, 0);

    let (result, cached_next) = NodeVectorDispatch::new(None)
        .route_frame(
            &runtime,
            &mut frame,
            log_prefetch(Rc::clone(&events)),
            route_all_to(node, Rc::clone(&events)),
        )
        .expect("route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(node));
    assert!(!frame.has_pending());
    assert_eq!(
        *events.borrow(),
        vec![
            Event::Prefetch(vec![4, 5, 6, 7]),
            Event::Route(vec![0, 1, 2, 3]),
            Event::Route(vec![4, 5, 6, 7]),
        ]
    );

    runtime.free_frame(&mut frame);
}

#[test]
fn pair_prefetches_future_chunks_before_later_chunk_processing() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Scalar);
    let mut frame = packet_frame(&runtime, 5);
    let events = Rc::new(RefCell::new(Vec::new()));
    let node = register_sink(&runtime, 0);

    let (result, cached_next) = NodeVectorDispatch::new(None)
        .route_frame(
            &runtime,
            &mut frame,
            log_prefetch(Rc::clone(&events)),
            route_all_to(node, Rc::clone(&events)),
        )
        .expect("route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(node));
    assert!(!frame.has_pending());
    assert_eq!(
        *events.borrow(),
        vec![
            Event::Prefetch(vec![2, 3]),
            Event::Route(vec![0, 1]),
            Event::Prefetch(vec![4]),
            Event::Route(vec![2, 3]),
            Event::Route(vec![4]),
        ]
    );

    runtime.free_frame(&mut frame);
}

#[test]
fn mixed_next_nodes_split_into_scheduled_frames() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Avx2);
    reset_sink_packets();
    let default = register_sink(&runtime, 0);
    let alternate = register_sink(&runtime, 1);
    let mut frame = packet_frame(&runtime, 8);
    let route = HashMap::from([
        (0, default),
        (1, default),
        (2, alternate),
        (3, alternate),
        (4, default),
        (5, alternate),
        (6, alternate),
        (7, default),
    ]);

    let (result, cached_next) = NodeVectorDispatch::new(Some(default))
        .route_frame(
            &runtime,
            &mut frame,
            |_batch, _indices| {},
            route_by_packet_id(route),
        )
        .expect("route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(default));
    assert!(!frame.has_pending());
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);
    assert_eq!(sink_packets(0), vec![0, 1, 4, 7]);
    assert_eq!(sink_packets(1), vec![2, 3, 5, 6]);

    runtime.free_frame(&mut frame);
}

#[test]
fn none_decisions_expect_callback_to_handle_packet_ownership() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Avx2);
    reset_sink_packets();
    let routed = register_sink(&runtime, 0);
    let mut frame = packet_frame(&runtime, 5);
    let consumed = indices_with_ids(&runtime, &frame, &[1, 3]);

    let (result, cached_next) = NodeVectorDispatch::new(Some(routed))
        .route_frame(
            &runtime,
            &mut frame,
            |_batch, _indices| {},
            |batch, indices, nexts| {
                for (offset, index) in indices.iter().copied().enumerate() {
                    let id = packet_id(batch, index)?;
                    nexts[offset] = if id % 2 == 0 { Some(routed) } else { None };
                }
                Ok(())
            },
        )
        .expect("route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(routed));
    assert!(!frame.has_pending());
    assert_eq!(runtime.in_use_buffers(), 5);
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
    assert_eq!(sink_packets(0), vec![0, 2, 4]);
    assert_eq!(runtime.in_use_buffers(), 2);

    runtime.free_frame(&mut frame);
    for index in consumed {
        runtime.free_index(index);
    }
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn route_frame_prefetch_matches_custom_prefetch_routing_behavior() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Avx2);
    reset_sink_packets();
    let default = register_sink(&runtime, 0);
    let alternate = register_sink(&runtime, 1);
    let route = HashMap::from([
        (0, default),
        (1, alternate),
        (2, default),
        (3, alternate),
        (4, default),
        (5, default),
    ]);

    let mut custom_frame = packet_frame(&runtime, 6);
    let (_, custom_cache) = NodeVectorDispatch::new(Some(default))
        .route_frame(
            &runtime,
            &mut custom_frame,
            |batch, indices| {
                for index in indices {
                    batch.prefetch_read(*index);
                }
            },
            route_by_packet_id(route.clone()),
        )
        .expect("custom route frame");

    assert_eq!(custom_cache, Some(default));
    assert!(!custom_frame.has_pending());
    assert_eq!(
        runtime.run_ready_nodes().expect("run custom ready nodes"),
        2
    );
    assert_eq!(sink_packets(0), vec![0, 2, 4, 5]);
    assert_eq!(sink_packets(1), vec![1, 3]);
    runtime.free_frame(&mut custom_frame);

    reset_sink_packets();
    let mut prefetch_frame = packet_frame(&runtime, 6);
    let (_, prefetch_cache) = NodeVectorDispatch::new(Some(default))
        .route_frame_prefetch(&runtime, &mut prefetch_frame, route_by_packet_id(route))
        .expect("default prefetch route frame");

    assert_eq!(prefetch_cache, Some(default));
    assert!(!prefetch_frame.has_pending());
    assert_eq!(
        runtime.run_ready_nodes().expect("run prefetch ready nodes"),
        2
    );
    assert_eq!(sink_packets(0), vec![0, 2, 4, 5]);
    assert_eq!(sink_packets(1), vec![1, 3]);
    runtime.free_frame(&mut prefetch_frame);
}

#[test]
fn route_frame_map_routes_indices_and_leaves_none_ownership_to_callback() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Scalar);
    reset_sink_packets();
    let default = register_sink(&runtime, 0);
    let alternate = register_sink(&runtime, 1);
    let mut frame = packet_frame(&runtime, 6);
    let consumed = indices_with_ids(&runtime, &frame, &[0, 3]);

    let (result, cached_next) = NodeVectorDispatch::new(Some(default))
        .route_frame_map(&runtime, &mut frame, |batch, index| {
            let id = packet_id(batch, index)?;
            Ok(match id {
                0 | 3 => None,
                1 | 2 => Some(default),
                _ => Some(alternate),
            })
        })
        .expect("map route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(alternate));
    assert!(!frame.has_pending());
    assert_eq!(runtime.in_use_buffers(), 6);
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);
    assert_eq!(sink_packets(0), vec![1, 2]);
    assert_eq!(sink_packets(1), vec![4, 5]);
    assert_eq!(runtime.in_use_buffers(), 2);

    runtime.free_frame(&mut frame);
    for index in consumed {
        runtime.free_index(index);
    }
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn route_frame_index_allows_runtime_owned_consumption_without_batch_borrow() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Scalar);
    reset_sink_packets();
    let routed = register_sink(&runtime, 0);
    let mut frame = packet_frame(&runtime, 4);
    let consumed = Rc::new(RefCell::new(Vec::new()));
    let runtime_for_route = runtime.clone();

    let (result, cached_next) = NodeVectorDispatch::new(Some(routed))
        .route_frame_index(&runtime, &mut frame, {
            let consumed = Rc::clone(&consumed);
            move |index| {
                let id = runtime_for_route.get_buffer(index)?.current()[0];
                if id % 2 == 0 {
                    Ok(Some(routed))
                } else {
                    consumed.borrow_mut().push(index);
                    runtime_for_route.free_index(index);
                    Ok(None)
                }
            }
        })
        .expect("index route frame");

    assert!(result.is_empty());
    assert_eq!(cached_next, Some(routed));
    assert!(!frame.has_pending());
    assert_eq!(runtime.in_use_buffers(), 2);
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
    assert_eq!(sink_packets(0), vec![0, 2]);
    assert_eq!(runtime.in_use_buffers(), 0);

    for index in consumed.borrow().iter().copied() {
        let _ = index;
    }
    runtime.free_frame(&mut frame);
}

#[test]
fn cached_next_updates_to_last_routed_node_or_stays_when_all_consumed() {
    let runtime = runtime_with_instruction_set(DataPlaneInstructionSet::Scalar);
    let first = register_sink(&runtime, 0);
    let second = register_sink(&runtime, 1);
    let original_cache = register_sink(&runtime, 2);
    let mut frame = packet_frame(&runtime, 3);
    let consumed = indices_with_ids(&runtime, &frame, &[1]);

    let (_, cached_next) = NodeVectorDispatch::new(Some(original_cache))
        .route_frame(
            &runtime,
            &mut frame,
            |_batch, _indices| {},
            move |batch, indices, nexts| {
                for (offset, index) in indices.iter().copied().enumerate() {
                    let id = packet_id(batch, index)?;
                    nexts[offset] = match id {
                        0 => Some(first),
                        1 => None,
                        _ => Some(second),
                    };
                }
                Ok(())
            },
        )
        .expect("route frame");

    assert_eq!(cached_next, Some(second));
    assert!(!frame.has_pending());
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);
    runtime.free_frame(&mut frame);
    for index in consumed {
        runtime.free_index(index);
    }

    let mut consumed_frame = packet_frame(&runtime, 2);
    let consumed = indices_with_ids(&runtime, &consumed_frame, &[0, 1]);
    let (_, cached_next) = NodeVectorDispatch::new(Some(original_cache))
        .route_frame(
            &runtime,
            &mut consumed_frame,
            |_batch, _indices| {},
            |batch, indices, nexts| {
                for (offset, index) in indices.iter().copied().enumerate() {
                    let _ = packet_id(batch, index)?;
                    nexts[offset] = None;
                }
                Ok(())
            },
        )
        .expect("route frame");

    assert_eq!(cached_next, Some(original_cache));
    assert!(!consumed_frame.has_pending());
    assert_eq!(runtime.in_use_buffers(), 2);

    runtime.free_frame(&mut consumed_frame);
    for index in consumed {
        runtime.free_index(index);
    }
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn runtime_with_instruction_set(instruction_set: DataPlaneInstructionSet) -> DataPlaneRuntime {
    DataPlaneRuntime::with_capacities_and_instruction_set(64, 32, 32, 16, instruction_set)
}

fn register_sink(runtime: &DataPlaneRuntime, slot: usize) -> NodeId {
    runtime.nodes().register_driver(SinkNode { slot })
}

fn reset_sink_packets() {
    SINK_PACKETS.with(|packets| {
        for packet_list in packets.borrow_mut().iter_mut() {
            packet_list.clear();
        }
    });
}

fn sink_packets(slot: usize) -> Vec<u8> {
    SINK_PACKETS.with(|packets| packets.borrow()[slot].clone())
}

fn sink_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let slot = data.usize_word(0)?;
    let batch = runtime.buffer_batch_mut();
    let mut ids = Vec::new();
    let indices: Vec<_> = frame.drain_pending().collect();
    for index in indices.iter().copied() {
        ids.push(packet_id(&batch, index)?);
    }
    drop(batch);
    for index in indices {
        runtime.free_index(index);
    }
    SINK_PACKETS.with(|packets| packets.borrow_mut()[slot].extend(ids));
    Ok(NodeResult::drop())
}

fn packet_frame(runtime: &DataPlaneRuntime, count: u8) -> PooledBufferFrame {
    let mut frame = runtime.alloc_pooled_frame().expect("alloc frame");
    for id in 0..count {
        let index = runtime
            .alloc_index_with_bytes(RouteMetadata::default(), &[id])
            .expect("alloc packet");
        frame.push_index(index).expect("push packet");
    }
    frame
}

fn log_prefetch(
    events: SharedEvents,
) -> impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]) + 'static {
    move |batch, indices| {
        events.borrow_mut().push(Event::Prefetch(
            packet_ids(batch, indices).expect("read prefetched ids"),
        ));
    }
}

fn route_all_to(
    node: NodeId,
    events: SharedEvents,
) -> impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex], &mut [Option<NodeId>; 4]) -> CoreResult<()>
{
    move |batch, indices, nexts| {
        events
            .borrow_mut()
            .push(Event::Route(packet_ids(batch, indices)?));
        for offset in 0..indices.len() {
            nexts[offset] = Some(node);
        }
        Ok(())
    }
}

fn route_by_packet_id(
    route: HashMap<u8, NodeId>,
) -> impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex], &mut [Option<NodeId>; 4]) -> CoreResult<()>
{
    move |batch, indices, nexts| {
        for (offset, index) in indices.iter().copied().enumerate() {
            let id = packet_id(batch, index)?;
            nexts[offset] = Some(*route.get(&id).expect("packet route"));
        }
        Ok(())
    }
}

fn packet_ids(batch: &BufferBatchMut<'_>, indices: &[BufferIndex]) -> CoreResult<Vec<u8>> {
    indices
        .iter()
        .copied()
        .map(|index| packet_id(batch, index))
        .collect()
}

fn packet_id(batch: &BufferBatchMut<'_>, index: BufferIndex) -> CoreResult<u8> {
    batch.with_buffer(index, |buffer| buffer.current()[0])
}

fn indices_with_ids(
    runtime: &DataPlaneRuntime,
    frame: &BufferFrame,
    ids: &[u8],
) -> Vec<BufferIndex> {
    let batch = runtime.buffer_batch_mut();
    let mut found = Vec::new();
    for index in frame.pending_indices().iter().copied() {
        let id = packet_id(&batch, index).expect("packet id");
        if ids.contains(&id) {
            found.push(index);
        }
    }
    found
}
