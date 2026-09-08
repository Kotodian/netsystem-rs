use std::sync::Arc;

use hammer_core::data_plane::{NodeId, NodeKind};
use hammer_runtime::node::NodeDescriptor;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneMain, GlobalMain, Node, NodeRuntimeData, RuntimeRegistry,
};
use hammer_service::interface::InterfaceMain;
use hammer_service::net::{
    DpoError, DpoId, DpoProto, DpoType, LoadBalanceDpo, LoadBalanceFlags, LoadBalancePath, NetMain,
    ReplicateDpo, ReplicateFlags,
};

#[test]
fn shared_bucket_references_survive_parent_replacement() -> Result<(), DpoError> {
    hammer_runtime::config::Memory::default().ensure_main_heap()?;
    hammer_core::buffer::BufferMain::new(64, 1024, &[0], 2, hammer_infra::PageSize::Default)
        .unwrap();
    let runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
    let mut main = GlobalMain::new(runtime, RuntimeRegistry::new());
    main.install_current();
    let net = NetMain::init(Arc::new(InterfaceMain::new()))?;
    let runtime = main.data_plane_main_mut();
    let terminal = hammer_service::data_plane::register_drop(runtime)?;
    // This is a pool-lifetime test, not a packet-forwarding test. Both owning
    // classes use an installed terminal node; no packet processing is invoked.
    net.register_dpo(
        Some(DpoType::LOAD_BALANCE),
        &[(DpoProto::IP4, &[terminal])],
        Some((LoadBalanceDpo::lock, LoadBalanceDpo::unlock)),
        None,
        Some(LoadBalanceDpo::mtu),
        None,
        None,
        Some(LoadBalanceDpo::format),
        Some(LoadBalanceDpo::memory),
    )?;
    net.register_dpo(
        Some(DpoType::REPLICATE),
        &[(DpoProto::IP4, &[terminal])],
        Some((ReplicateDpo::lock, ReplicateDpo::unlock)),
        None,
        None,
        None,
        None,
        Some(ReplicateDpo::format),
        Some(ReplicateDpo::memory),
    )?;

    let child = net.create_load_balance(
        runtime,
        DpoProto::IP4,
        LoadBalanceDpo::new(DpoProto::IP4, &[], LoadBalanceFlags::empty(), 0x9f)?,
    )?;
    let child_paths = [LoadBalancePath {
        dpo: child,
        path_index: u32::MAX,
        weight: 1,
    }; 8];
    let parent = net.create_load_balance(
        runtime,
        DpoProto::IP4,
        LoadBalanceDpo::new(DpoProto::IP4, &child_paths, LoadBalanceFlags::empty(), 0x9f)?,
    )?;
    let replicate = net.create_replicate(
        runtime,
        DpoProto::IP4,
        ReplicateDpo::new(DpoProto::IP4, &[child; 8], ReplicateFlags::empty())?,
    )?;
    net.unlock_dpo(child);
    assert!(net.load_balance(child.index()).is_some());
    assert_eq!(net.dpo_mtu(parent), u16::MAX);
    assert_eq!(net.dpo_urpf(parent), u32::MAX);
    assert_eq!(net.dpo_mtu(DpoId::INVALID), u16::MAX);
    assert_eq!(net.dpo_urpf(DpoId::INVALID), u32::MAX);
    let interposed = net.interpose_dpo(child, DpoId::drop(DpoProto::IP4))?;
    assert_eq!(interposed, child);
    let memory = net.dpo_memory();
    assert!(memory.iter().any(|&(class, size, used, capacity)| {
        class == DpoType::LOAD_BALANCE && size == 64 && used == 2 && capacity >= used
    }));

    let invalid = DpoId::load_balance(DpoProto::IP4, u32::MAX);
    assert_eq!(
        net.update_load_balance(runtime, invalid, &child_paths[..1])?,
        None
    );
    let object = net
        .load_balance(parent.index())
        .expect("parent remains published");
    assert_eq!(object.bucket_count(), 8);
    for bucket in 0..8 {
        assert_eq!(object.select_bucket(bucket), Some(child));
    }
    drop(object);

    let output = runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            hammer_service::data_plane::DropNode.node_process(),
            NodeRuntimeData::empty(),
            None,
            &[],
            None,
        ),
    )?;
    let output_class = net.register_dpo(
        None,
        &[(DpoProto::IP4, &[output])],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    let missing_class = net.register_dpo(
        None,
        &[(DpoProto::IP4, &[NodeId::new(u32::MAX)])],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    let output_dpo = net.dpo_main().identity(output_class, DpoProto::IP4, 0)?;
    let missing_dpo = net.dpo_main().identity(missing_class, DpoProto::IP4, 0)?;
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot_for_target(terminal, output)?,
        None
    );
    assert!(matches!(
        net.update_load_balance(
            runtime,
            parent,
            &[
                LoadBalancePath {
                    dpo: output_dpo,
                    path_index: u32::MAX,
                    weight: 1
                },
                LoadBalancePath {
                    dpo: missing_dpo,
                    path_index: u32::MAX,
                    weight: 1
                },
            ]
        ),
        Err(DpoError::GraphEdgeAdd { .. })
    ));
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot_for_target(terminal, output)?,
        None
    );
    assert_eq!(
        net.dpo_main().next_node(
            DpoType::LOAD_BALANCE,
            DpoProto::IP4,
            output_class,
            DpoProto::IP4,
        ),
        None
    );
    let object = net.load_balance(parent.index()).unwrap();
    assert_eq!(object.bucket_count(), 8);
    for bucket in 0..8 {
        assert_eq!(object.select_bucket(bucket), Some(child));
    }
    drop(object);

    let interface_tx_class = net.register_dpo(
        None,
        &[],
        None,
        Some(|dpo| vec![NodeId::new(dpo.index())]),
        None,
        None,
        None,
        None,
        None,
    )?;
    let interface_tx = net
        .dpo_main()
        .identity(interface_tx_class, DpoProto::IP4, output.slot())?;
    let stacked = net
        .dpo_main_mut()
        .stack_from_node(runtime, terminal, interface_tx)?;
    assert_eq!(
        Some(stacked.next()),
        runtime
            .nodes()
            .node_next_slot_for_target(terminal, output)?
    );
    let cached =
        net.dpo_main_mut()
            .stack(runtime, DpoType::LOAD_BALANCE, DpoProto::IP4, interface_tx)?;
    assert_eq!(cached.next(), stacked.next());
    let local_tx = net
        .dpo_main()
        .identity(interface_tx_class, DpoProto::IP4, terminal.slot())?;
    let local_stack = net
        .dpo_main_mut()
        .stack_from_node(runtime, terminal, local_tx)?;
    assert_eq!(
        Some(local_stack.next()),
        runtime
            .nodes()
            .node_next_slot_for_target(terminal, terminal)?
    );
    assert_ne!(local_stack.next(), stacked.next());

    net.update_load_balance(runtime, parent, &[])?;
    assert!(net.load_balance(child.index()).is_some());
    assert_eq!(
        net.replicate(replicate.index()).unwrap().bucket(7),
        Some(child)
    );
    net.unlock_dpo(parent);
    assert!(net.load_balance(parent.index()).is_none());
    assert!(net.load_balance(child.index()).is_some());

    net.update_replicate(runtime, replicate, &[DpoId::drop(DpoProto::IP4)])?;
    assert!(net.load_balance(child.index()).is_some());
    net.unlock_dpo(interposed);
    assert!(net.load_balance(child.index()).is_none());
    net.unlock_dpo(replicate);
    assert!(net.replicate(replicate.index()).is_none());

    main.close()?;
    GlobalMain::uninstall_current();
    Ok(())
}
