//! Main-thread packet trace CLI. Every command is non-MP-safe, so the
//! synchronous handler completes while the existing WorkerBarrier is held.

use std::fmt::Write;
use std::str::FromStr;

use hammer_runtime::cli::CliError;
use hammer_runtime::trace::{TraceHeader, TraceTimestampFormat};
use hammer_runtime::{DataPlaneMain, ThreadMain};

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
        .nodes()
        .node_trace_supported(node)
        .expect("named Node is registered")
    {
        return Err(CliError::TraceUnsupported { node: args.node });
    }

    let threads = ThreadMain::global();
    for owner in std::iter::once(&*main).chain((1..=threads.worker_count()).map(|thread_index| {
        let worker = threads
            .thread_by_index(thread_index)
            .expect("Data Worker has a thread descriptor");
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier, and the Worker
        // released its own main borrow before acknowledging the barrier.
        unsafe { threads.worker_main_at_barrier(worker) as &DataPlaneMain }
    })) {
        let limit = owner
            .trace_main()
            .nodes
            .get(node.slot() as usize)
            .map_or(0, |trace_node| trace_node.limit);
        if args.count != 0 && limit.checked_add(args.count).is_none() {
            return Err(CliError::TraceLimitOverflow { node: args.node });
        }
    }

    main.trace_main_mut()
        .add_count(node, args.count, args.verbose);
    for thread_index in 1..=threads.worker_count() {
        let worker = threads
            .thread_by_index(thread_index)
            .expect("Data Worker has a thread descriptor");
        // SAFETY: the same barrier remains held through the complete update.
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main_mut()
            .add_count(node, args.count, args.verbose);
    }
    Ok(())
}

/// VPP `cli_show_trace_buffer`: per-main sorting and a per-main display cap.
#[hammer_component_macros::cli_command(path = "show trace", args = ShowTraceArgs, mp_safe = false)]
fn show_trace(main: &mut DataPlaneMain, args: ShowTraceArgs) -> Result<String, CliError> {
    let mut output = String::new();
    let timestamp_format = main.trace_main().timestamp_format;
    let threads = ThreadMain::global();
    let mains = std::iter::once((0, "main", &*main)).chain((1..=threads.worker_count()).map(
        |thread_index| {
            let worker = threads
                .thread_by_index(thread_index)
                .expect("Data Worker has a thread descriptor");
            // SAFETY: the non-MP-safe CLI holds WorkerBarrier until formatting ends.
            (worker.thread_index(), worker.name(), unsafe {
                threads.worker_main_at_barrier(worker) as &DataPlaneMain
            })
        },
    ));
    for (thread_index, name, owner) in mains {
        writeln!(
            output,
            "------------------- Start of thread {thread_index} {name} -------------------"
        )
        .expect("write to String");

        // Only record slices are sorted; payload bytes stay in the owner pool.
        let mut traces: Vec<&[TraceHeader]> = owner
            .trace_main()
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
            owner.format_trace_buffer(record, timestamp_format, &mut output);
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
    main.trace_main_mut().trace_enable = false;
    for thread_index in 1..=threads.worker_count() {
        let worker = threads
            .thread_by_index(thread_index)
            .expect("Data Worker has a thread descriptor");
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier for both passes.
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main_mut()
            .trace_enable = false;
    }
    main.trace_main_mut().clear();
    for thread_index in 1..=threads.worker_count() {
        let worker = threads
            .thread_by_index(thread_index)
            .expect("Data Worker has a thread descriptor");
        unsafe { threads.worker_main_at_barrier(worker) }
            .trace_main_mut()
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
    main.trace_main_mut().timestamp_format = args.format;
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
    Ok(main.trace_main().timestamp_format)
}
