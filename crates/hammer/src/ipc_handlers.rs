use std::fmt::Write as _;

use hammer_component_macros::ipc_handler;
use hammer_ipc::{PluginCommandError, PluginCommandReply};
use hammer_runtime::engine::{Engine, EnginePool, WorkerRuntimeStats};
use hammer_runtime::{RuntimeError, TraceControlPlane};

#[ipc_handler(name = "ping")]
fn handle_ping(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::from(b"pong".as_slice())
}

#[ipc_handler(name = "status")]
fn handle_status(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    if !request.is_empty() {
        return Vec::from(b"status_error: request payload must be empty".as_slice());
    }

    let mut output = String::new();
    let plugins = engine.loaded_plugins();
    let _ = writeln!(output, "plugins: {}", plugins.join(", "));
    let _ = writeln!(
        output,
        "workers: configured={}",
        engine.configured_worker_count()
    );
    let graph = engine.runtime.nodes().node_runtime_stats_snapshot();
    let _ = writeln!(output, "graph_nodes: {}", graph.len());
    for node in graph {
        let _ = writeln!(
            output,
            "  {} {}",
            node.node_id.slot(),
            node.node_name.unwrap_or("<unnamed>")
        );
    }

    match engine.worker_runtime_stats_snapshot() {
        Ok(workers) => {
            let _ = writeln!(output, "running_workers: {}", workers.len());
            for worker in workers {
                format_worker_runtime_stats(&mut output, &worker);
            }
        }
        Err(error) => {
            let _ = writeln!(output, "worker_stats_error: {error}");
        }
    }

    match engine.registry.get::<TraceControlPlane>() {
        Some(trace) => {
            trace.drain_completed();
            let records = trace.records();
            let _ = writeln!(
                output,
                "packet_trace: records={} dropped={}",
                records.len(),
                trace.dropped_completed()
            );
            for record in records {
                let _ = writeln!(output, "  {record}");
            }
        }
        None => {
            let _ = writeln!(output, "packet_trace: disabled");
        }
    }
    output.into_bytes()
}

fn format_worker_runtime_stats(output: &mut String, worker: &WorkerRuntimeStats) {
    let _ = writeln!(
        output,
        "worker {}: numa={} loops={}",
        worker.thread_index, worker.numa_node, worker.main_loop_count
    );
    for node in &worker.nodes {
        let errors = node
            .error_counters
            .iter()
            .map(|(code, count)| format!("{code}={count}"))
            .collect::<Vec<_>>()
            .join(",");
        if node.calls == 0 && node.vectors == 0 && errors.is_empty() {
            continue;
        }
        let _ = writeln!(
            output,
            "  node {}: calls={} vectors={} errors=[{}] total_ns={} max_ns={}",
            node.node_name.unwrap_or("<unnamed>"),
            node.calls,
            node.vectors,
            errors,
            node.total_elapsed_ns,
            node.max_elapsed_ns
        );
    }
    if worker.files.is_empty() {
        let _ = writeln!(output, "  files: none");
    } else {
        for file in &worker.files {
            let _ = writeln!(
                output,
                "  file {}.{} fd={} read_interest={} write_interest={} read={} write={} errors={} description={}",
                file.index.slot(),
                file.index.generation(),
                file.fd,
                file.read_enabled,
                file.write_enabled,
                file.read_events,
                file.write_events,
                file.error_events,
                file.description
            );
        }
    }
}

#[ipc_handler(name = "pause")]
fn handle_pause(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "wake")]
fn handle_wake(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "shutdown")]
fn handle_shutdown(engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    EnginePool::main_loop_exit(engine);
    Vec::new()
}

#[ipc_handler(name = "reset_network")]
fn handle_reset_network(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "metrics")]
fn handle_metrics(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "list_listeners")]
fn handle_list_listeners(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "config_reload")]
fn handle_config_reload(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "plugin_list")]
fn handle_plugin_list(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    if !request.is_empty() {
        return encode_plugin_reply(PluginCommandReply::Error(
            PluginCommandError::InvalidRequest,
        ));
    }
    let names = engine.loaded_plugins();
    let names = names.iter().map(String::as_str).collect();
    encode_plugin_reply(PluginCommandReply::Loaded(names))
}

#[ipc_handler(name = "plugin_load")]
fn handle_plugin_load(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    let roots: Vec<String> = match bincode::deserialize(request) {
        Ok(roots) => roots,
        Err(_) => {
            return encode_plugin_reply(PluginCommandReply::Error(
                PluginCommandError::InvalidRequest,
            ));
        }
    };
    let config = match super::load_current_config() {
        Ok(config) => config,
        Err(_) => {
            return encode_plugin_reply(PluginCommandReply::Error(
                PluginCommandError::Configuration,
            ));
        }
    };
    match engine.load_plugins(&roots, &config) {
        Ok(()) => {
            let names = engine.loaded_plugins();
            let names = names.iter().map(String::as_str).collect();
            encode_plugin_reply(PluginCommandReply::Loaded(names))
        }
        Err(error) => encode_plugin_reply(PluginCommandReply::Error(plugin_command_error(error))),
    }
}

fn encode_plugin_reply(reply: PluginCommandReply<'_>) -> Vec<u8> {
    match bincode::serialize(&reply) {
        Ok(encoded) => Vec::from(encoded),
        Err(_) => Vec::new(),
    }
}

fn plugin_command_error(error: RuntimeError) -> PluginCommandError {
    match error {
        RuntimeError::MemoryNotInitialized => PluginCommandError::MemoryNotInitialized,
        RuntimeError::WorkerCountOverflow { .. } => PluginCommandError::WorkerCountOverflow,
        RuntimeError::WorkerGraphUpdateAlreadyPending => {
            PluginCommandError::WorkerGraphUpdatePending
        }
        RuntimeError::WorkerGraphUpdateMissing
        | RuntimeError::WorkerGraphUpdateStatePoisoned
        | RuntimeError::WorkerGraphUpdateNotAdditive => PluginCommandError::WorkerGraphUpdate,
        RuntimeError::ConfigParse { .. } | RuntimeError::ConfigValidation { .. } => {
            PluginCommandError::Configuration
        }
        RuntimeError::DataPlane(_) | RuntimeError::GraphNodeInitialization { .. } => {
            PluginCommandError::GraphMaterialization
        }
        _ => PluginCommandError::Lifecycle,
    }
}
