//! Main-thread packet trace CLI. Every command is non-MP-safe, so the
//! synchronous handler completes while the existing WorkerBarrier is held.

use std::fmt::Write;
use std::str::FromStr;

use hammer_core::data_plane::NodeId;
use zerocopy::IntoBytes;

use crate::cli::CliError;
use crate::{DataPlaneMain, ThreadMain};

use super::{TraceHeader, TraceTimestampFormat};

struct TraceAddArgs {
    node: String,
    count: u32,
    verbose: bool,
}

impl FromStr for TraceAddArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        let node = words
            .next()
            .ok_or_else(|| CliError::InvalidArgument {
                argument: input.to_owned(),
            })?
            .to_owned();
        let count = words
            .next()
            .ok_or_else(|| CliError::InvalidArgument {
                argument: input.to_owned(),
            })?
            .parse()
            .map_err(|_| CliError::InvalidArgument {
                argument: input.to_owned(),
            })?;
        let verbose = match words.next() {
            None => false,
            Some("verbose") => true,
            Some(_) => {
                return Err(CliError::InvalidArgument {
                    argument: input.to_owned(),
                });
            }
        };
        if words.next().is_some() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self {
            node,
            count,
            verbose,
        })
    }
}

struct ShowTraceArgs {
    max: u32,
}

impl FromStr for ShowTraceArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        let Some(word) = words.next() else {
            return Ok(Self { max: 50 });
        };
        if word != "max" {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        let max = words
            .next()
            .ok_or_else(|| CliError::InvalidArgument {
                argument: input.to_owned(),
            })?
            .parse()
            .map_err(|_| CliError::InvalidArgument {
                argument: input.to_owned(),
            })?;
        if words.next().is_some() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self { max })
    }
}

struct ClearTraceArgs;

impl FromStr for ClearTraceArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self)
    }
}

struct TraceTimestampArgs {
    format: TraceTimestampFormat,
}

impl FromStr for TraceTimestampArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let format = match input.trim() {
            "relative" => TraceTimestampFormat::Relative,
            "unix" => TraceTimestampFormat::Unix,
            "datetime" => TraceTimestampFormat::Datetime,
            _ => {
                return Err(CliError::InvalidArgument {
                    argument: input.to_owned(),
                });
            }
        };
        Ok(Self { format })
    }
}

struct ShowTraceTimestampArgs;

impl FromStr for ShowTraceTimestampArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self)
    }
}

/// VPP `cli_add_trace_buffer` and `trace_update_capture_options`.
#[hammer_component_macros::cli_command(path = "trace add", args = TraceAddArgs, mp_safe = false)]
fn trace_add(main: &mut DataPlaneMain, args: TraceAddArgs) -> Result<(), CliError> {
    let node = main
        .node_by_name(&args.node)
        .ok_or_else(|| CliError::TraceNodeMissing {
            name: args.node.clone(),
        })?;
    if !main
        .nodes
        .node_trace_supported(node)
        .expect("named Node is registered")
    {
        return Err(CliError::TraceUnsupported { node: args.node });
    }

    let threads = ThreadMain::global();
    for owner in std::iter::once(&*main).chain(threads.data_workers().map(|worker| {
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier, and the Worker
        // released its own main borrow before acknowledging the barrier.
        unsafe { threads.worker_main_at_barrier(worker) as &DataPlaneMain }
    })) {
        let limit = owner
            .trace_main
            .nodes
            .get(node.slot() as usize)
            .map_or(0, |trace_node| trace_node.limit);
        if args.count != 0 && limit.checked_add(args.count).is_none() {
            return Err(CliError::TraceLimitOverflow { node: args.node });
        }
    }

    main.trace_main.add_count(node, args.count, args.verbose);
    for worker in threads.data_workers() {
        // SAFETY: the same barrier remains held through the complete update.
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main
            .add_count(node, args.count, args.verbose);
    }
    Ok(())
}

/// VPP `format_vlib_trace`; the vector stores aligned headers and payloads.
fn format_trace_buffer(
    owner: &DataPlaneMain,
    trace: &[TraceHeader],
    timestamp_format: TraceTimestampFormat,
    output: &mut String,
) {
    let mut offset = 0;
    let mut previous_node = None;
    while offset < trace.len() {
        let header = trace[offset];
        let end = offset
            .checked_add(1)
            .and_then(|start| start.checked_add(header.n_data as usize))
            .expect("trace header length fits its vector");
        assert!(end <= trace.len(), "trace header stays within its vector");
        let node = NodeId::new(header.node_index);
        let name = owner
            .nodes
            .node_name(node)
            .expect("trace Node remains registered")
            .expect("trace Node has a name");
        if previous_node != Some(node) {
            match timestamp_format {
                TraceTimestampFormat::Relative => {
                    let seconds = header
                        .time
                        .checked_sub(owner.main_loop_start_ticks)
                        .expect("trace follows main-loop start")
                        as f64
                        * owner.seconds_per_cpu_tick;
                    let whole = seconds.trunc() as u64;
                    let microseconds = (seconds.fract() * 1_000_000.0).trunc() as u32;
                    writeln!(
                        output,
                        "\n{:02}:{:02}:{:02}:{:06}: {}",
                        whole / 3_600,
                        (whole / 60) % 60,
                        whole % 60,
                        microseconds,
                        name
                    )
                    .expect("write to String");
                }
                TraceTimestampFormat::Unix | TraceTimestampFormat::Datetime => {
                    let seconds = owner.unix_reference_seconds
                        + header
                            .time
                            .checked_sub(owner.cpu_reference_ticks)
                            .expect("trace follows clock reference")
                            as f64
                            * owner.seconds_per_cpu_tick;
                    if matches!(timestamp_format, TraceTimestampFormat::Unix) {
                        writeln!(output, "\n{seconds:.6}: {name}").expect("write to String");
                    } else {
                        let seconds_whole = seconds.trunc() as libc::time_t;
                        let microseconds = (seconds.fract() * 1_000_000.0).trunc() as u32;
                        let mut calendar = std::mem::MaybeUninit::<libc::tm>::uninit();
                        // SAFETY: localtime_r initializes calendar on success.
                        let calendar = unsafe {
                            assert!(
                                !libc::localtime_r(&seconds_whole, calendar.as_mut_ptr()).is_null(),
                                "trace timestamp is representable"
                            );
                            calendar.assume_init()
                        };
                        writeln!(
                            output,
                            "\n{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}: {}",
                            calendar.tm_year + 1900,
                            calendar.tm_mon + 1,
                            calendar.tm_mday,
                            calendar.tm_hour,
                            calendar.tm_min,
                            calendar.tm_sec,
                            microseconds,
                            name
                        )
                        .expect("write to String");
                    }
                }
            }
        }
        previous_node = Some(node);

        let payload = trace[offset + 1..end].as_bytes();
        if let Some(formatter) = owner
            .nodes
            .node_trace_formatter(node)
            .expect("trace Node remains registered")
        {
            writeln!(output, "  {}", formatter(payload)).expect("write to String");
        } else {
            output.push_str("  ");
            for byte in payload {
                write!(output, "{byte:02x}").expect("write to String");
            }
            output.push('\n');
        }
        offset = end;
    }
}

/// VPP `cli_show_trace_buffer`: per-main sorting and a per-main display cap.
#[hammer_component_macros::cli_command(path = "show trace", args = ShowTraceArgs, mp_safe = false)]
fn show_trace(main: &mut DataPlaneMain, args: ShowTraceArgs) -> Result<String, CliError> {
    let mut output = String::new();
    let timestamp_format = main.trace_main.timestamp_format;
    let threads = ThreadMain::global();
    let mains = std::iter::once((0, "main", &*main)).chain(threads.data_workers().map(|worker| {
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier until formatting ends.
        (worker.thread_index(), worker.name(), unsafe {
            threads.worker_main_at_barrier(worker) as &DataPlaneMain
        })
    }));
    for (thread_index, name, owner) in mains {
        writeln!(
            output,
            "------------------- Start of thread {thread_index} {name} -------------------"
        )
        .expect("write to String");

        // Only record slices are sorted; payload bytes stay in the owner pool.
        let mut traces: Vec<&[TraceHeader]> = owner
            .trace_main
            .trace_buffer_pool
            .iter()
            .filter_map(|(_, record)| (!record.is_empty()).then_some(record.as_slice()))
            .collect();
        if traces.is_empty() {
            output.push_str("No packets in trace buffer\n");
            continue;
        }
        traces.sort_unstable_by_key(|record| record[0].time);
        for (index, record) in traces.iter().take(args.max as usize).enumerate() {
            writeln!(output, "Packet {}", index + 1).expect("write to String");
            format_trace_buffer(owner, record, timestamp_format, &mut output);
            output.push_str("\n\n");
        }
        if traces.len() > args.max as usize {
            writeln!(
                output,
                "Limiting display to {} packets. To display more specify max.",
                args.max
            )
            .expect("write to String");
        }
    }
    Ok(output)
}

/// VPP `clear_trace_buffer`: disable every main before clearing any pool.
#[hammer_component_macros::cli_command(path = "clear trace", args = ClearTraceArgs, mp_safe = false)]
fn clear_trace(main: &mut DataPlaneMain, _: ClearTraceArgs) -> Result<(), CliError> {
    let threads = ThreadMain::global();
    main.trace_main.trace_enable = false;
    for worker in threads.data_workers() {
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier for both passes.
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main
            .trace_enable = false;
    }
    main.trace_main.clear();
    for worker in threads.data_workers() {
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main
            .clear();
    }
    Ok(())
}

#[hammer_component_macros::cli_command(
    path = "set trace timestamp-format",
    args = TraceTimestampArgs,
    mp_safe = false,
)]
fn set_trace_timestamp_format(
    main: &mut DataPlaneMain,
    args: TraceTimestampArgs,
) -> Result<(), CliError> {
    main.trace_main.timestamp_format = args.format;
    Ok(())
}

#[hammer_component_macros::cli_command(
    path = "show trace timestamp-format",
    args = ShowTraceTimestampArgs,
    mp_safe = false,
)]
fn show_trace_timestamp_format(
    main: &mut DataPlaneMain,
    _: ShowTraceTimestampArgs,
) -> Result<TraceTimestampFormat, CliError> {
    Ok(main.trace_main.timestamp_format)
}
