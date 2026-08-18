//! Service-backed coverage of the `hammerctl stats` command family: the real
//! binary talks to a live engine over the Binary API socket, and `stats
//! list` / `stats dump` print the three `/sys` entries with exact stable
//! columns and typed values.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hammer_runtime::config::Worker;
use hammer_runtime::{Engine, EnginePool, RuntimeRegistry};
use hammer_service::binary_api::DEFAULT_MAX_FRAME_BYTES;

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hammerctl-stats-{}-{sequence}.sock",
        std::process::id()
    ))
}

/// Builds an engine with the service registration image: the Binary API
/// socket is bound at `path` and the stats collector runs on a short cadence
/// so the heartbeat advances while the commands execute.
fn engine_with_service(path: &Path) -> Engine {
    let mut engine = Engine::new_configured(RuntimeRegistry::new(), Worker::default())
        .expect("configure stats command engine");
    engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    let config = format!(
        "[binary_api]\nsocket_path = \"{}\"\nmax_frame_bytes = {}\n\
         [stats]\nupdate_interval = \"100ms\"\n",
        path.display(),
        DEFAULT_MAX_FRAME_BYTES
    );
    EnginePool::main_loop_enter(&mut engine, &[], &config).expect("enter main loop");
    engine
}

/// Runs the hammerctl binary on a blocking thread while the test runtime
/// keeps driving the Process Nodes, and bridges the result back.
async fn run_hammerctl(args: Vec<String>) -> Output {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(
            Command::new(env!("CARGO_BIN_EXE_hammerctl"))
                .args(&args)
                .output()
                .expect("spawn hammerctl binary"),
        );
    });
    rx.await.expect("hammerctl process must exit")
}

#[test]
fn stats_commands_list_and_dump_system_metrics() {
    let path = socket_path();
    let mut engine = engine_with_service(&path);
    engine.install_current();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build main runtime");

    engine.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(20), async {
            let socket = path.display().to_string();

            // stats list: exact stable record stream for the three /sys
            // entries, published before the collector node runs.
            let list = run_hammerctl(vec![
                "--socket".to_owned(),
                socket.clone(),
                "stats".to_owned(),
                "list".to_owned(),
                "^/sys/(heartbeat|boottime|last_stats_clear)$".to_owned(),
            ])
            .await;
            assert!(
                list.status.success(),
                "stats list failed: {}",
                String::from_utf8_lossy(&list.stderr)
            );
            let expected_list = "\
0:1\t/sys/heartbeat\tscalar\tcounter\thammer_sys_heartbeat_total\tcollector passes since the engine started\t
1:1\t/sys/boottime\tscalar\tgauge\thammer_sys_boottime_seconds\tUnix epoch seconds when the stats collector started\t
2:1\t/sys/last_stats_clear\tscalar\tgauge\thammer_sys_last_stats_clear_seconds\tUnix epoch seconds of the last stats clear; zero until a clear exists\t
";
            assert_eq!(
                String::from_utf8(list.stdout).expect("utf8 list output"),
                expected_list
            );

            // stats dump with --socket after the nested subcommand: the live
            // heartbeat advances past the initial pass within the deadline.
            let mut heartbeat = 0_u64;
            for _ in 0..40 {
                let dump = run_hammerctl(vec![
                    "stats".to_owned(),
                    "dump".to_owned(),
                    "--socket".to_owned(),
                    socket.clone(),
                    "^/sys/(heartbeat|boottime|last_stats_clear)$".to_owned(),
                ])
                .await;
                assert!(
                    dump.status.success(),
                    "stats dump failed: {}",
                    String::from_utf8_lossy(&dump.stderr)
                );
                let stdout = String::from_utf8(dump.stdout).expect("utf8 dump output");
                let lines: Vec<&str> = stdout.lines().collect();
                assert_eq!(
                    lines.len(),
                    3,
                    "dump must cover the three system entries: {stdout}"
                );
                assert!(
                    lines[0].starts_with("0:1\t/sys/heartbeat\tscalar\tcounter\tcounter:"),
                    "unexpected heartbeat line: {}",
                    lines[0]
                );
                heartbeat = lines[0]
                    .rsplit("counter:")
                    .next()
                    .expect("heartbeat value")
                    .parse()
                    .expect("heartbeat counter");
                if heartbeat >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                heartbeat >= 1,
                "heartbeat must advance past the initial collector pass"
            );

            // A settled dump carries typed values: boottime published, the
            // last-stats-clear scalar still zero.
            let dump = run_hammerctl(vec![
                "stats".to_owned(),
                "dump".to_owned(),
                "--socket".to_owned(),
                socket.clone(),
                "^/sys/(heartbeat|boottime|last_stats_clear)$".to_owned(),
            ])
            .await;
            assert!(
                dump.status.success(),
                "stats dump failed: {}",
                String::from_utf8_lossy(&dump.stderr)
            );
            let stdout = String::from_utf8(dump.stdout).expect("utf8 dump output");
            let lines: Vec<&str> = stdout.lines().collect();
            assert_eq!(lines.len(), 3, "unexpected dump: {stdout}");
            assert!(
                lines[1].starts_with("1:1\t/sys/boottime\tscalar\tgauge\tgauge:"),
                "unexpected boottime line: {}",
                lines[1]
            );
            let boottime: f64 = lines[1]
                .rsplit("gauge:")
                .next()
                .expect("boottime value")
                .parse()
                .expect("boottime float");
            assert!(boottime > 0.0, "boottime must be published");
            assert_eq!(
                lines[2], "2:1\t/sys/last_stats_clear\tscalar\tgauge\tgauge:0",
                "last-stats-clear stays zero"
            );
        })
        .await
        .expect("stats commands completed within the deadline");
    });

    engine
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(engine);
    Engine::uninstall_current();
}

#[test]
fn stats_commands_dead_socket_fails_with_concise_stderr() {
    let missing = std::env::temp_dir().join(format!(
        "hammerctl-missing-{}-{}.sock",
        std::process::id(),
        SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&missing);
    let socket = missing.display().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_hammerctl"))
        .args(["--socket", &socket, "stats", "list"])
        .output()
        .expect("spawn hammerctl");

    assert!(!output.status.success(), "dead socket must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("stats command failed:"),
        "unexpected stderr: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no partial output on failure");
}
