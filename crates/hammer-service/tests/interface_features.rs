use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain, GlobalMain, RuntimeRegistry};
use hammer_service::data_plane::{DropNode, PuntNode};
use hammer_service::interface::{InterfaceMain, InterfaceOutputNode};

#[test]
fn feature_chain_terminates_without_an_end_node_self_edge() -> Result<(), Box<dyn std::error::Error>>
{
    hammer_runtime::config::Memory::default().ensure_main_heap()?;
    hammer_core::buffer::BufferMain::new(64, 1024, &[0], 2, hammer_infra::PageSize::Default)
        .unwrap();
    let mut main = GlobalMain::new(
        DataPlaneMain::new(DataPlaneBufferConfig::default()),
        RuntimeRegistry::new(),
    );
    main.install_current();
    let interfaces = InterfaceMain::new();
    let hardware = interfaces.register_hardware_interface(0, 0, 0, 0)?;
    let interface = interfaces
        .hardware_interface(hardware)
        .unwrap()
        .sw_if_index();
    let runtime = main.data_plane_main_mut();
    let output = runtime.nodes().try_register_internal(InterfaceOutputNode)?;
    let punt = runtime.nodes().try_register_internal(PuntNode::new())?;
    let drop_node = runtime.nodes().try_register_internal(DropNode::new())?;
    let arc = interfaces.register_feature_arc("interface-output", &[output], Some("drop"))?;
    interfaces.register_feature("interface-output", "punt", punt, &["drop"], &[])?;
    interfaces.register_feature("interface-output", "drop", drop_node, &[], &[])?;
    interfaces.install_feature_arcs(runtime.nodes())?;
    let punt_feature = interfaces.feature_index(arc, "punt").unwrap();
    let drop_feature = interfaces.feature_index(arc, "drop").unwrap();
    let config = [17, 23];
    interfaces.enable_feature(runtime, arc, punt_feature, interface, &config)?;
    let mut selected_config = [0; 2];

    // config.c::find_config_with_features appends the end next only when the
    // last enabled feature differs from the end node. Both forms terminate at
    // the same node; explicitly enabling the end must not add a self edge.
    for explicit_end in [false, true] {
        if explicit_end {
            interfaces.enable_feature(runtime, arc, drop_feature, interface, &[])?;
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(output, runtime.nodes().frame_args_size(output)?)?;
        let index = runtime.buffers().alloc_index_with_bytes(&[0; 64])?;
        {
            let count = frame.len();
            frame.set_vector_count(count + 1);
            frame.vector_args_mut()[count] = index;
        }
        let buffer = runtime.buffer_mut(index);
        let first = interfaces.start_feature_arc(arc, interface, buffer, u16::MAX);
        let cursor = buffer.current_config_index();
        let (words, next) = interfaces.next_feature_with_config::<2>(buffer);
        selected_config = words;
        assert_eq!(words, config);
        assert_eq!(buffer.current_config_index(), cursor + 3);
        assert_eq!(
            runtime.nodes().node_next_slot(output, usize::from(first))?,
            punt
        );
        assert_eq!(
            runtime.nodes().node_next_slot(punt, usize::from(next))?,
            drop_node
        );
        assert_eq!(
            runtime
                .nodes()
                .node_next_slot_for_target(drop_node, drop_node)?,
            None
        );
    }

    interfaces.disable_feature(runtime, arc, punt_feature, interface, &config)?;
    interfaces.disable_feature(runtime, arc, drop_feature, interface, &[])?;
    interfaces.enable_feature(runtime, arc, punt_feature, interface, &[])?;
    {
        let mut frame = runtime
            .buffers()
            .get_next_frame(output, runtime.nodes().frame_args_size(output)?)?;
        let index = runtime.buffers().alloc_index_with_bytes(&[0; 64])?;
        {
            let count = frame.len();
            frame.set_vector_count(count + 1);
            frame.vector_args_mut()[count] = index;
        }
        let buffer = runtime.buffer_mut(index);
        interfaces.start_feature_arc(arc, interface, buffer, u16::MAX);
        let cursor = buffer.current_config_index();
        let next = interfaces.next_feature(buffer);
        assert_eq!(buffer.current_config_index(), cursor + 1);
        assert_eq!(
            runtime.nodes().node_next_slot(punt, usize::from(next))?,
            drop_node
        );
    }
    assert_eq!(selected_config, config);
    interfaces.disable_feature(runtime, arc, punt_feature, interface, &[])?;
    let mut frame = runtime
        .buffers()
        .get_next_frame(output, runtime.nodes().frame_args_size(output)?)?;
    let index = runtime.buffers().alloc_index_with_bytes(&[0; 64])?;
    {
        let count = frame.len();
        frame.set_vector_count(count + 1);
        frame.vector_args_mut()[count] = index;
    }
    let buffer = runtime.buffer_mut(index);
    let cursor = buffer.current_config_index();
    assert_eq!(interfaces.start_feature_arc(arc, interface, buffer, 7), 7);
    assert_eq!(buffer.current_config_index(), cursor);
    drop(frame);
    assert_eq!(runtime.buffers().in_use_buffers(), 0);
    GlobalMain::uninstall_current();
    Ok(())
}
