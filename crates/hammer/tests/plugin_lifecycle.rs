use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hammer_ipc::handler::{IpcRequest, IpcResponse, PluginCommandReply};
use hammer_ipc::{read_frame, write_frame};
use hammer_runtime::PluginMain;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PROTOCOL_REGISTRATION_CHILD: &str = "HAMMER_PROTOCOL_REGISTRATION_CHILD";

struct Daemon {
    child: Child,
    address: SocketAddr,
    config_path: PathBuf,
}

impl Daemon {
    fn start(config: &str) -> Self {
        Self::start_with_plugin_directory(config, None)
    }

    fn start_with_plugin_directory(config: &str, plugin_directory: Option<&Path>) -> Self {
        let config_path = write_config(config);
        let address = unused_address();
        let mut command = Command::new(daemon_binary());
        command
            .arg(&config_path)
            .env("HAMMER_IPC_ADDR", address.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(plugin_directory) = plugin_directory {
            command.env("HAMMER_PLUGIN_DIR", plugin_directory);
        }
        let child = command.spawn().expect("start hammer daemon");

        let mut daemon = Self {
            child,
            address,
            config_path,
        };
        daemon.wait_until_ready();
        daemon
    }

    fn plugin_names(&self) -> Vec<String> {
        let payload = request(self.address, "plugin_list", Vec::new());
        match bincode::deserialize::<PluginCommandReply<'_>>(&payload)
            .expect("deserialize plugin list reply")
        {
            PluginCommandReply::Loaded(names) => names.into_iter().map(str::to_owned).collect(),
            PluginCommandReply::Error(error) => panic!("plugin list failed: {error:?}"),
        }
    }

    fn load_plugins(&self, roots: &[&str]) {
        let roots = roots
            .iter()
            .map(|root| (*root).to_owned())
            .collect::<Vec<_>>();
        let request_payload =
            Vec::from(bincode::serialize(&roots).expect("serialize plugin roots"));
        let payload = request(self.address, "plugin_load", request_payload);
        match bincode::deserialize::<PluginCommandReply<'_>>(&payload)
            .expect("deserialize plugin load reply")
        {
            PluginCommandReply::Loaded(_) => {}
            PluginCommandReply::Error(error) => panic!("plugin load failed: {error:?}"),
        }
    }

    fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if TcpStream::connect(self.address).is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("inspect hammer daemon") {
                panic!(
                    "hammer daemon exited during startup with {status}: {}",
                    child_stderr(&mut self.child)
                );
            }
            assert!(Instant::now() < deadline, "hammer daemon did not start");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn shutdown(mut self) {
        let _ = request(self.address, "shutdown", Vec::new());
        self.wait_for_exit();
    }

    fn wait_for_exit(&mut self) {
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            if self
                .child
                .try_wait()
                .expect("inspect hammer daemon")
                .is_some()
            {
                return;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                panic!("hammer daemon did not exit after shutdown");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_file(&self.config_path);
    }
}

#[test]
fn daemon_loads_startup_and_additive_plugins() {
    let daemon = Daemon::start(
        r#"
plugins = []

[memory]
main_heap_size = "256 MiB"

[plugin.tcp]
mss = 1200
"#,
    );

    assert_eq!(daemon.plugin_names(), Vec::<String>::new());
    daemon.load_plugins(&["tcp"]);
    assert_eq!(daemon.plugin_names(), ["ip", "tcp"]);
    daemon.load_plugins(&["udp", "tun"]);
    assert_eq!(daemon.plugin_names(), ["ip", "tcp", "udp", "tun"]);
    daemon.shutdown();
}

#[test]
fn daemon_discovers_tls_plugin_through_dynamic_lifecycle() {
    let daemon = Daemon::start(
        r#"
plugins = []

[memory]
main_heap_size = "256 MiB"
"#,
    );

    daemon.load_plugins(&["tls"]);
    assert_eq!(daemon.plugin_names(), ["tls"]);
    daemon.shutdown();
}

#[test]
fn tls_dso_registers_session_app() {
    if std::env::var_os(PROTOCOL_REGISTRATION_CHILD).is_some() {
        let mut plugins = PluginMain::default();
        plugins
            .load(env!("CARGO_PKG_VERSION"), &["tls".to_owned()])
            .expect("load TLS DSO");
        assert_eq!(
            plugins
                .session_app("tls")
                .expect("resolve TLS Session App registration")
                .name(),
            "tls"
        );
        return;
    }

    let status = Command::new(std::env::current_exe().expect("resolve test executable"))
        .arg("--exact")
        .arg("tls_dso_registers_session_app")
        .arg("--nocapture")
        .env(PROTOCOL_REGISTRATION_CHILD, "1")
        .env("HAMMER_PLUGIN_DIR", daemon_binary_directory())
        .status()
        .expect("run isolated TLS DSO registration test");
    assert!(status.success(), "TLS DSO registration child failed");
}

#[test]
fn daemon_exits_after_shutdown_response_when_client_keeps_connection_open() {
    let mut daemon = Daemon::start(
        r#"
plugins = []

[memory]
main_heap_size = "256 MiB"
"#,
    );

    let (stream, _) = request_while_connection_remains_open(daemon.address, "shutdown", Vec::new());
    daemon.wait_for_exit();
    drop(stream);
}

#[test]
fn daemon_rejects_malformed_plugin_owned_config() {
    let config_path = write_config(
        r#"
plugins = ["tcp"]

[memory]
main_heap_size = "256 MiB"

[plugin.tcp]
mss = "not-a-number"
"#,
    );
    let address = unused_address();
    let output = Command::new(daemon_binary())
        .arg(&config_path)
        .env("HAMMER_IPC_ADDR", address.to_string())
        .output()
        .expect("run hammer daemon with malformed config");
    fs::remove_file(config_path).expect("remove malformed startup configuration");

    assert!(
        !output.status.success(),
        "malformed plugin config must fail startup"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("config function `tcp_config` section `plugin.tcp`"),
        "unexpected daemon error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn daemon_rejects_mismatched_plugin_file_without_poisoning_next_startup() {
    let directory = unique_temp_directory("hammer-mismatched-plugin");
    fs::create_dir_all(&directory).expect("create staged plugin directory");
    fs::copy(
        daemon_binary_directory().join(libloading::library_filename("hammer_plugin_udp")),
        directory.join(libloading::library_filename("hammer_plugin_mismatch")),
    )
    .expect("stage UDP plugin under a mismatched name");

    let config_path = write_config(
        r#"
plugins = ["mismatch"]

[memory]
main_heap_size = "256 MiB"
"#,
    );
    let output = Command::new(daemon_binary())
        .arg(&config_path)
        .env("HAMMER_IPC_ADDR", unused_address().to_string())
        .env("HAMMER_PLUGIN_DIR", &directory)
        .output()
        .expect("run hammer daemon with mismatched plugin");
    fs::remove_file(config_path).expect("remove mismatch startup configuration");
    fs::remove_dir_all(directory).expect("remove staged plugin directory");

    assert!(
        !output.status.success(),
        "mismatched plugin must fail startup"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("exported a mismatched module name"),
        "unexpected daemon error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let daemon = Daemon::start(
        r#"
plugins = ["ip"]

[memory]
main_heap_size = "256 MiB"
"#,
    );
    assert_eq!(daemon.plugin_names(), ["ip"]);
    daemon.shutdown();
}

fn daemon_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_hammer"))
}

fn daemon_binary_directory() -> PathBuf {
    daemon_binary()
        .parent()
        .expect("hammer binary has a parent directory")
        .to_path_buf()
}

fn request(address: SocketAddr, name: &str, payload: Vec<u8>) -> Vec<u8> {
    let (_, payload) = request_while_connection_remains_open(address, name, payload);
    payload
}

fn request_while_connection_remains_open(
    address: SocketAddr,
    name: &str,
    payload: Vec<u8>,
) -> (TcpStream, Vec<u8>) {
    let mut stream = TcpStream::connect(address).expect("connect to hammer IPC");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set hammer IPC read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set hammer IPC write timeout");
    let request = IpcRequest {
        name: name.to_owned(),
        payload,
    };
    write_frame(
        &mut stream,
        &bincode::serialize(&request).expect("serialize hammer IPC request"),
    )
    .expect("write hammer IPC request");
    let response = read_frame(&mut stream).expect("read hammer IPC response");
    let payload = bincode::deserialize::<IpcResponse>(&response)
        .expect("deserialize hammer IPC response")
        .payload;
    (stream, payload)
}

fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve IPC address");
    listener.local_addr().expect("read reserved IPC address")
}

fn write_config(config: &str) -> PathBuf {
    let path = unique_temp_directory("hammer-plugin-config").with_extension("toml");
    fs::write(&path, config).expect("write hammer startup configuration");
    path
}

fn unique_temp_directory(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{nonce}-{sequence}",
        std::process::id()
    ))
}

fn child_stderr(child: &mut Child) -> String {
    let mut output = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        stderr
            .read_to_string(&mut output)
            .expect("read hammer stderr");
    }
    output
}
