//! Control ping through the SHM client, real message queues and main-thread Process.
#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use hammer_infra::mem::MainHeapConfig;
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::svm::region::SvmRegion;
use hammer_ipc::binary_api::codec;
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
    events: Vec<u32>,
    subscription_events: Vec<u32>,
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

fn ping_event(replies: &mut PingResults, message: Message) {
    let Message::ControlPingReply(reply) = message else {
        panic!("ping event")
    };
    replies.events.push(reply.context);
}

fn ping_subscription(replies: &mut PingResults, message: Message) {
    let Message::ControlPingReply(reply) = message else {
        panic!("subscribed ping event")
    };
    replies.subscription_events.push(reply.context);
}

fn send_reply<T: Api>(api: &ApiMain, client_index: u32, reply: &T) {
    let registration = api.registration(client_index).unwrap();
    let queue = unsafe { registration.as_ref().input_queue.as_ref() };
    let mut message = unsafe { api.alloc(codec::serialized_len(reply).unwrap()) };
    unsafe { message.encode(reply) }.unwrap();
    queue
        .add(
            &usize::from(&message).to_ne_bytes(),
            SvmQueueConditionalWait::Nowait,
        )
        .unwrap();
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
            "binary_api::memory_client::tests::control_ping_shmem_process_round_trip",
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
        // Cancel only the wait, retaining the published create and queue.
        {
            let mut connect = Box::pin(client.connect("control-ping", NonZeroU32::new(16).unwrap(),
                NonZeroUsize::new(4).unwrap(), true));
            let mut task = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(connect.as_mut().poll(&mut task).is_pending());
        }
        client.connect("control-ping", NonZeroU32::new(16).unwrap(),
            NonZeroUsize::new(4).unwrap(), true).await.unwrap();
        assert!(client.is_connected());
        let client_index = client.client_index().unwrap();
        assert_eq!(client.message_id(MessageId::ControlPing), Some(23));
        assert_eq!(client.message_id(MessageId::ControlPingReply), Some(24));
        assert_eq!(api.get_msg_index(ControlPing::NAME_CRC).unwrap(), 23);
        assert_eq!(api.get_msg_index(ControlPingReply::NAME_CRC).unwrap(), 24);
        for expected_context in 0x8000_0001..=0x8000_0003 {
            assert_eq!(client.control_ping(ping_reply).unwrap(), expected_context);
            client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
            let reply = replies.responses.pop().unwrap();
            assert_eq!(reply.id, 24);
            assert_eq!(reply.context, expected_context);
            assert_eq!(reply.retval, 0);
            assert_eq!(reply.client_index, client_index);
            assert_eq!(reply.vpe_pid, std::process::id());
            assert_eq!(client.request_count(), 0);
        }
        // Real liveness probes share the receive path with ping responses.
        let scan_time = std::time::Instant::now() + Duration::from_secs(11);
        api.dead_client_scan(scan_time);
        api.dead_client_scan(scan_time + Duration::from_secs(11));
        assert_eq!(unsafe { api.registration(client_index).unwrap().as_ref() }.unanswered_keepalives, 1);
        client.control_ping(ping_reply).unwrap();
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        // Another ping orders observation after the automatic keepalive reply.
        client.control_ping(ping_reply).unwrap();
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        assert_eq!(unsafe { api.registration(client_index).unwrap().as_ref() }.unanswered_keepalives, 0);
        replies.responses.clear();

        // All four slots are usable; rejected submission leaves both the
        // pending count and the server queue unchanged.
        for _ in 0..4 { client.control_ping(ping_reply).unwrap(); }
        assert!(matches!(client.control_ping(ping_reply), Err(Error::RequestsFull { capacity: 4 })));
        assert_eq!(client.request_count(), 4);
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        assert_eq!(replies.responses.len(), 4);
        replies.responses.clear();

        // A timeout only ends this wait. It does not lose the committed request.
        let context = client.control_ping(ping_reply).unwrap();
        assert!(matches!(client.dispatch(&mut replies, Duration::ZERO).await,
            Err(Error::ResponseTimeout { context: received }) if received == context));
        assert_eq!(client.request_count(), 1);
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        assert_eq!(replies.responses.pop().unwrap().context, context);

        // Exercise the decoder and ordered ring with actual queued ping messages.
        // A wrong known message ID must leave its pending request untouched.
        let context = client.control_ping(ping_reply).unwrap();
        send_reply(api, client_index, &ControlPing { id: 23, client_index, context });
        assert!(matches!(client.dispatch_one(&mut replies, Duration::ZERO).await,
            Err(Error::UnexpectedResponse { context: received, expected: MessageId::ControlPingReply,
                received: MessageId::ControlPing }) if received == context));
        assert_eq!(client.request_count(), 1);
        send_reply(api, client_index, &ControlPingReply {
            id: u16::MAX, client_index, context, retval: 0, vpe_pid: std::process::id(),
        });
        assert!(matches!(client.dispatch_one(&mut replies, Duration::ZERO).await,
            Err(Error::UnknownMessageId { id: u16::MAX })));
        assert_eq!(client.request_count(), 1);
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        assert_eq!(replies.responses.pop().unwrap().context, context);

        let missing_context = client.control_ping(ping_reply).unwrap();
        let context = client.control_ping(ping_reply).unwrap();
        // An unknown high-bit context is not an event and cannot retire requests.
        send_reply(api, client_index, &ControlPingReply {
            id: 24, client_index, context: 0xffff_ffff, retval: 0, vpe_pid: std::process::id(),
        });
        client.dispatch_one(&mut replies, Duration::ZERO).await.unwrap();
        assert_eq!(client.request_count(), 2);
        send_reply(api, client_index, &ControlPingReply {
            id: 24, client_index, context, retval: 0, vpe_pid: std::process::id(),
        });
        client.dispatch_one(&mut replies, Duration::ZERO).await.unwrap();
        assert_eq!(replies.missing, [missing_context]);
        assert_eq!(client.request_count(), 0);
        replies.responses.clear();
        // Normal delayed replies to the retired contexts must be ignored.
        let context = client.control_ping(ping_reply).unwrap();
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        assert_eq!(replies.responses.len(), 1);
        assert_eq!(replies.responses.pop().unwrap().context, context);

        client.set_generic_callback(Some(ping_event));
        send_reply(api, client_index, &ControlPingReply {
            id: 24, client_index, context: 7, retval: 0, vpe_pid: std::process::id(),
        });
        client.dispatch_one(&mut replies, Duration::ZERO).await.unwrap();
        assert_eq!(replies.events, [7]);
        client.set_event_callback(MessageId::ControlPingReply, Some(ping_subscription));
        send_reply(api, client_index, &ControlPingReply {
            id: 24, client_index, context: 8, retval: 0, vpe_pid: std::process::id(),
        });
        client.dispatch_one(&mut replies, Duration::ZERO).await.unwrap();
        assert_eq!(replies.events, [7]);
        assert_eq!(replies.subscription_events, [8]);

        // An invalid client index produces no reply; subsequent valid ping works.
        let request = ControlPing { id: 23, client_index: u32::MAX, context: 7 };
        let mut allocation = unsafe { api.alloc_as_client(codec::serialized_len(&request).unwrap()) };
        unsafe { allocation.encode(&request) }.unwrap();
        unsafe { api.shmem_header().input_queue() }
            .add(&usize::from(&allocation).to_ne_bytes(), SvmQueueConditionalWait::Nowait).unwrap();
        assert!(!client.dispatch_one(&mut replies, Duration::from_millis(10)).await.unwrap());
        client.control_ping(ping_reply).unwrap();
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();

        // Disconnect drains responses and retires all outstanding callbacks.
        let context = client.control_ping(ping_reply).unwrap();
        {
            let mut disconnect = Box::pin(client.disconnect(&mut replies));
            let mut task = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(disconnect.as_mut().poll(&mut task).is_pending());
        }
        client.disconnect(&mut replies).await.unwrap();
        assert_eq!(replies.missing.last(), Some(&context));
        assert!(!client.is_connected());
        assert_eq!(client.client_index(), None);
        assert_eq!(client.message_id(MessageId::ControlPing), None);
        assert_eq!(client.request_count(), 0);
        // Queue cleanup leaves the client reusable with fresh ID bindings.
        client.connect("control-ping-reconnect", NonZeroU32::new(16).unwrap(),
            NonZeroUsize::new(4).unwrap(), true).await.unwrap();
        client.control_ping(ping_reply).unwrap();
        client.dispatch(&mut replies, Duration::from_secs(1)).await.unwrap();
        client.disconnect(&mut replies).await.unwrap();
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
