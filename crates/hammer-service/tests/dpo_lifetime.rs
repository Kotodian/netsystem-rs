use std::sync::Arc;

use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain, GlobalMain, RuntimeRegistry};
use hammer_service::interface::InterfaceMain;
use hammer_service::net::{
    DpoError, DpoId, DpoProto, DpoType, LoadBalanceDpo, LoadBalanceFlags, NetMain, ReplicateDpo,
    ReplicateFlags,
};

#[test]
fn shared_bucket_references_survive_parent_replacement() -> Result<(), DpoError> {
    hammer_runtime::config::Memory::default().ensure_main_heap()?;
    let runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
    let mut main = GlobalMain::new(runtime, RuntimeRegistry::new());
    main.install_current();
    let net = NetMain::init(Arc::new(InterfaceMain::new()))?;
    let runtime = main.data_plane_main_mut();
    let terminal = hammer_service::data_plane::register_drop(runtime)?;
    // This is a pool-lifetime test, not a packet-forwarding test. Both owning
    // classes use an installed terminal node; no packet processing is invoked.
    net.register_builtin_dpo(DpoType::LOAD_BALANCE, &[(DpoProto::IP4, &[terminal])])?;
    net.register_builtin_dpo(DpoType::REPLICATE, &[(DpoProto::IP4, &[terminal])])?;

    let child = net.create_load_balance(
        runtime,
        DpoProto::IP4,
        LoadBalanceDpo::new(
            DpoProto::IP4,
            &[DpoId::drop(DpoProto::IP4)],
            LoadBalanceFlags::empty(),
            0x9f,
        )?,
    )?;
    let parent = net.create_load_balance(
        runtime,
        DpoProto::IP4,
        LoadBalanceDpo::new(DpoProto::IP4, &[child; 8], LoadBalanceFlags::empty(), 0x9f)?,
    )?;
    let replicate = net.create_replicate(
        runtime,
        DpoProto::IP4,
        ReplicateDpo::new(DpoProto::IP4, &[child; 8], ReplicateFlags::empty())?,
    )?;
    net.retire_load_balance_root(runtime, child)?;
    assert!(net.load_balance(child.index()).is_some());

    let invalid = DpoId::load_balance(DpoProto::IP4, u32::MAX);
    assert!(matches!(
        net.update_load_balance(runtime, parent, &[invalid]),
        Err(DpoError::ObjectMissing {
            index: u32::MAX,
            ..
        })
    ));
    let object = net
        .load_balance(parent.index())
        .expect("parent remains published");
    assert_eq!(object.bucket_count(), 8);
    for bucket in 0..8 {
        assert_eq!(object.select_bucket(bucket), Some(child));
    }

    net.update_load_balance(runtime, parent, &[DpoId::drop(DpoProto::IP4)])?;
    assert!(net.load_balance(child.index()).is_some());
    assert_eq!(
        net.replicate(replicate.index()).unwrap().bucket(7),
        Some(child)
    );
    net.retire_load_balance_root(runtime, parent)?;
    assert!(net.load_balance(parent.index()).is_none());
    assert!(net.load_balance(child.index()).is_some());

    net.update_replicate(runtime, replicate, &[DpoId::drop(DpoProto::IP4)])?;
    assert!(net.load_balance(child.index()).is_none());
    net.retire_replicate_root(runtime, replicate)?;
    assert!(net.replicate(replicate.index()).is_none());

    main.close()?;
    GlobalMain::uninstall_current();
    Ok(())
}
