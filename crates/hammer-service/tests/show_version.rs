#![cfg(target_os = "linux")]

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use hammer_infra::mem::MainHeapConfig;
use hammer_ipc::binary_api::memory_client::{Client, Error, Message};
use hammer_runtime::{DataPlaneMain, GlobalMain, PluginMain, ThreadMain};

#[derive(Default)]
struct Results {
    replies: Vec<hammer_ipc::binary_api::vpe::ShowVersionReply>,
}

fn show_version_reply(
    results: &mut Results,
    context: u32,
    is_last: bool,
    message: Result<Option<Message>, Error>,
) {
    assert!(is_last);
    let Ok(Some(Message::ShowVersionReply(reply))) = message else {
        panic!("show_version must return its typed reply");
    };
    assert_eq!(reply.context, context);
    results.replies.push(reply);
}

#[test]
fn show_version_shmem_round_trip() {
    if std::env::var_os("HAMMER_SHOW_VERSION_PROCESS").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env("HAMMER_SHOW_VERSION_PROCESS", "1")
            .args(["--exact", "show_version_shmem_round_trip", "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "show version child failed: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let status = if std::panic::catch_unwind(show_version_shmem_process).is_ok() {
        0
    } else {
        1
    };
    unsafe { libc::_exit(status) }
}

fn show_version_shmem_process() {
    MainHeapConfig {
        size: "256 MiB".parse().unwrap(),
        ..MainHeapConfig::default()
    }
    .initialize()
    .unwrap();
    let directory =
        std::env::temp_dir().join(format!("hammer-show-version-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let api_prefix = directory.join("show-version");
    let api_path = directory.join("show-version-vpe-api");
    let root_path = directory.join("show-version-global_vm");
    let stats_path = directory.join("stats.sock");
    let config = format!(
        "[api-segment]\nprefix = '{}'\n[network]\n[stats]\nsocket_path = '{}'\n[worker]\ncount = 1\n[worker.buffer]\nslots_per_numa = 64\n",
        api_prefix.display(),
        stats_path.display()
    );
    let mut global = GlobalMain::new(
        "show-version".to_owned(),
        String::new(),
        Vec::new(),
        config.clone(),
    );
    let mut plugins = PluginMain::default();
    plugins.register_image(hammer_service::registration_image());
    plugins.register_global_declarations(&mut global);
    let mut threads = ThreadMain::new().unwrap();
    hammer_runtime::init::run_config_functions(&global, None, true, &config).unwrap();
    threads.configure().unwrap();
    let mut main = DataPlaneMain::new_main(&threads).unwrap();
    hammer_runtime::main_loop::run(global, threads, plugins, &mut main, async {
        let mut client = Client::<Results>::new();
        let mut results = Results::default();
        client
            .connect(
                "show-version",
                NonZeroU32::new(16).unwrap(),
                NonZeroUsize::new(4).unwrap(),
                true,
            )
            .await
            .unwrap();
        let context = client.show_version(show_version_reply).unwrap();
        client
            .dispatch(&mut results, Duration::from_secs(1))
            .await
            .unwrap();
        let reply = results.replies.pop().unwrap();
        assert_eq!(reply.context, context);
        assert_eq!(reply.retval, 0);
        assert_eq!(reply.program, "vpe");
        assert_eq!(reply.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            reply.build_date,
            option_env!("HAMMER_BUILD_DATE").unwrap_or("unknown")
        );
        assert_eq!(reply.build_directory, env!("CARGO_MANIFEST_DIR"));
        let mut payload = [0xff; 362];
        assert_eq!(
            hammer_ipc::binary_api::codec::serialize(&reply, &mut payload).unwrap(),
            362
        );
        assert_eq!(&payload[2..6], &context.to_be_bytes());
        assert_eq!(&payload[10..14], b"vpe\0");
        for end in [41, 73, 105, 361] {
            assert_eq!(payload[end], 0);
        }
        client.disconnect(&mut results).await.unwrap();
        Ok(())
    })
    .unwrap();
    std::fs::remove_file(api_path).unwrap();
    std::fs::remove_file(root_path).unwrap();
    std::fs::remove_file(stats_path).unwrap();
    std::fs::remove_dir(directory).unwrap();
}
