//! Control ping through the SHM client, real message queues and main-thread Process.
#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use hammer_infra::mem::MainHeapConfig;
use hammer_infra::svm::region::SvmRegion;
use hammer_ipc::binary_api::control::{ControlPing, ControlPingReply};
use hammer_ipc::binary_api::memory_client::{Client, Error, Message, MessageId};
use hammer_ipc::binary_api::{Api, ApiMain, control, memclnt};
use hammer_runtime::{DataPlaneMain, GlobalMain, PluginMain, RuntimeResult, ThreadMain};

#[hammer_component_macros::process_node(name = "control-ping-api")]
fn api_process(
    main: &mut DataPlaneMain,
) -> impl Future<Output = RuntimeResult<()>> + Send + 'static {
    let api = ApiMain::current();
    control::setup_message_id_table(api);
    hammer_runtime::init::run_api_init(main).unwrap();
    assert!(api.get_msg_data(23).unwrap().is_mp_safe);
    assert!(api.get_msg_data(23).unwrap().dispatch.is_some());
    assert!(api.get_msg_data(24).unwrap().is_mp_safe);
    async {
        loop {
            while memclnt::receive().unwrap() {}
            tokio::time::sleep(Duration::from_micros(400)).await;
        }
    }
}

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    num_workers_change_functions = [];
    api_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [__PROCESS_NODE_CONTROL_PING_API];
    binary_api_methods = [];
);

#[derive(Default)]
struct PingResults {
    responses: Vec<ControlPingReply>,
    missing: Vec<u32>,
}

fn ping_reply(
    replies: &mut PingResults,
    context: u32,
    is_last: bool,
    message: Result<Option<Message>, Error>,
) {
    assert!(is_last);
    match message {
        Ok(Some(Message::ControlPingReply(reply))) => {
            assert_eq!(reply.context, context);
            replies.responses.push(reply);
        }
        Err(Error::NoResponse { context: missing }) => {
            assert_eq!(missing, context);
            replies.missing.push(context);
        }
        _ => panic!("ping requires its concrete reply or NoResponse"),
    }
}

#[test]
fn control_ping_shmem_process_round_trip() {
    // Main Heap interception is process-wide. Keep libtest's earlier System
    // allocations in the parent, as the infra SVM process tests do.
    if std::env::var_os("HAMMER_CONTROL_PING_PROCESS").is_some() {
        let status = if std::panic::catch_unwind(control_ping_shmem_process).is_ok() {
            0
        } else {
            1
        };
        unsafe { libc::_exit(status) }
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .env("HAMMER_CONTROL_PING_PROCESS", "1")
        .args([
            "--exact",
            "control_ping_shmem_process_round_trip",
            "--nocapture",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "control ping child failed: {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn control_ping_shmem_process() {
    MainHeapConfig {
        size: "256 MiB".parse().unwrap(),
        ..MainHeapConfig::default()
    }
    .initialize()
    .unwrap();
    let directory =
        std::env::temp_dir().join(format!("hammer-control-ping-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let stats_path = directory.join("stats.sock");
    let api_path = directory.join("api");
    let root_path = directory.join("root");
    let config = format!(
        "[stats]\nsocket_path = '{}'\n[worker]\ncount = 1\n[worker.buffer]\nslots_per_numa = 64\n",
        stats_path.display()
    );
    let mut global = GlobalMain::new(
        "control-ping".to_owned(),
        String::new(),
        Vec::new(),
        config.clone(),
    );
    let mut plugins = PluginMain::default();
    plugins.register_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_global_declarations(&mut global);
    let mut threads = ThreadMain::new().unwrap();
    hammer_runtime::init::run_config_functions(&global, None, true, &config).unwrap();
    threads.configure().unwrap();
    let mut api = ApiMain::new(29);
    api.set_input_queue_length(32);
    api.install();
    let api = ApiMain::current();
    let root_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&root_path)
        .unwrap();
    let mut root = SvmRegion::create(
        api.global_base_va(),
        &api.root_region_config(),
        root_file.into(),
    )
    .unwrap();
    unsafe { api.map_shared_region(&mut root, &api_path, true) }.unwrap();
    let mut main = DataPlaneMain::new_main(&threads).unwrap();
    hammer_runtime::main_loop::run(global, threads, plugins, &mut main, async {
        let mut client = Client::<PingResults>::new();
        let mut replies = PingResults::default();
        client
            .connect(
                "control-ping",
                NonZeroU32::new(16).unwrap(),
                NonZeroUsize::new(4).unwrap(),
                true,
            )
            .await
            .unwrap();
        assert!(client.is_connected());
        let client_index = client.client_index().unwrap();
        assert_eq!(client.message_id(MessageId::ControlPing), Some(23));
        assert_eq!(client.message_id(MessageId::ControlPingReply), Some(24));
        assert_eq!(api.get_msg_index(ControlPing::NAME_CRC).unwrap(), 23);
        assert_eq!(api.get_msg_index(ControlPingReply::NAME_CRC).unwrap(), 24);
        for expected_context in 0x8000_0001..=0x8000_0003 {
            assert_eq!(client.control_ping(ping_reply).unwrap(), expected_context);
            client
                .dispatch(&mut replies, Duration::from_secs(1))
                .await
                .unwrap();
            let reply = replies.responses.pop().unwrap();
            assert_eq!(reply.id, 24);
            assert_eq!(reply.context, expected_context);
            assert_eq!(reply.retval, 0);
            assert_eq!(reply.client_index, client_index);
            assert_eq!(reply.vpe_pid, std::process::id());
            assert_eq!(client.request_count(), 0);
        }
        client.disconnect(&mut replies).await.unwrap();
        assert!(!client.is_connected());
        assert_eq!(client.client_index(), None);
        assert_eq!(client.message_id(MessageId::ControlPing), None);
        assert_eq!(client.request_count(), 0);
        Ok(())
    })
    .unwrap();
    // Process tasks have been joined and the client queue has been released.
    unsafe { api.unmap_shared_regions() }.unwrap();
    root.unmap().unwrap();
    std::fs::remove_file(stats_path).unwrap();
    std::fs::remove_file(api_path).unwrap();
    std::fs::remove_file(root_path).unwrap();
    std::fs::remove_dir(directory).unwrap();
}
