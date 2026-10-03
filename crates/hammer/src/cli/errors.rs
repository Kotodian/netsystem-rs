use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::data_plane::{NodeErrorIndex, NodeId};
use hammer_runtime::cli::CliError;
use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{DataPlaneMain, ThreadMain};

struct ErrorsArgs {
    verbose: u32,
}

struct ClearErrorsArgs;

impl FromStr for ClearErrorsArgs {
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

impl FromStr for ErrorsArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        match words.next() {
            None => Ok(Self { verbose: 0 }),
            Some("verbose") => {
                let verbose = match words.next() {
                    Some(level) => level.parse().map_err(|_| CliError::InvalidArgument {
                        argument: input.to_owned(),
                    })?,
                    None => 1,
                };
                if words.next().is_some() {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
                Ok(Self { verbose })
            }
            Some(_) => Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        }
    }
}

struct ErrorsReport {
    verbose: u32,
    threads: Vec<(u32, &'static str)>,
    rows: Vec<(
        u32,
        NodeId,
        &'static str,
        NodeErrorDescriptor,
        NodeErrorIndex,
        u64,
    )>,
    totals: Vec<(&'static str, NodeErrorDescriptor, NodeErrorIndex, u64)>,
}

impl Display for ErrorsReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.verbose == 0 {
            writeln!(
                f,
                "{:>10} {:<35} {:<35} {:<10}",
                "Count", "Node", "Reason", "Severity"
            )?;
        } else {
            writeln!(
                f,
                "{:>10} {:<35} {:<35} {:<10} {:>6}",
                "Count", "Node", "Reason", "Severity", "Index"
            )?;
        }
        for (thread, thread_name) in &self.threads {
            if self.verbose != 0 {
                writeln!(f, "Thread {thread} ({thread_name}):")?;
            }
            for (_, _, name, descriptor, index, count) in
                self.rows.iter().filter(|row| row.0 == *thread)
            {
                let severity = match descriptor.severity {
                    NodeErrorSeverity::Error => "error",
                    NodeErrorSeverity::Warn => "warn",
                    NodeErrorSeverity::Info => "info",
                };
                if self.verbose == 0 {
                    writeln!(
                        f,
                        "{count:>10} {name:<35} {:<35} {severity:<10}",
                        descriptor.description
                    )?;
                } else {
                    writeln!(
                        f,
                        "{count:>10} {name:<35} {:<35} {severity:<10} {:>6}",
                        descriptor.description,
                        index.get()
                    )?;
                }
            }
        }
        if self.verbose != 0 {
            writeln!(f, "Total:")?;
            for (name, descriptor, index, count) in &self.totals {
                writeln!(
                    f,
                    "{count:>10} {name:<40} {:<20} {:>10}",
                    descriptor.description,
                    index.get()
                )?;
            }
        }
        Ok(())
    }
}

/// VPP `show_errors`: read published per-thread error columns while the CLI
/// directory holds the Worker Barrier, then format the copied values.
#[hammer_component_macros::cli_command(
    path = "errors",
    args = ErrorsArgs,
    short_help = "errors [verbose [level]]",
    mp_safe = false,
)]
fn errors(main: &mut DataPlaneMain, args: ErrorsArgs) -> Result<ErrorsReport, CliError> {
    let nodes = main.nodes();
    let threads = ThreadMain::global();
    let mut report = ErrorsReport {
        verbose: args.verbose,
        threads: (0..=threads.worker_count())
            .map(|thread| {
                let descriptor = threads
                    .thread_by_index(thread)
                    .expect("each graph thread has a descriptor");
                (thread, descriptor.name())
            })
            .collect(),
        rows: Vec::new(),
        totals: Vec::new(),
    };
    let mut declared = false;
    for slot in 0..nodes.node_count() {
        if !nodes
            .node_error_descriptors(NodeId::new(slot as u32))
            .expect("registered Node has an error declaration")
            .is_empty()
        {
            declared = true;
            break;
        }
    }
    if !declared {
        return Ok(report);
    }
    for slot in 0..nodes.node_count() {
        let node = NodeId::new(slot as u32);
        let descriptors = nodes
            .node_error_descriptors(node)
            .expect("registered Node has an error declaration");
        if descriptors.is_empty() {
            continue;
        }
        let name = nodes
            .node_name(node)
            .expect("registered Node has a name entry")
            .expect("Node declaring errors is named");
        for (code, descriptor) in descriptors.iter().copied().enumerate() {
            let index = nodes
                .node_error_index(node, code as u16)
                .expect("declared error owns a column");
            let mut total = 0u64;
            for thread in 0..=threads.worker_count() {
                let count = if thread == 0 {
                    main.node_error_count_since_clear(index)
                } else {
                    let worker = threads
                        .thread_by_index(thread)
                        .expect("Data Worker has a thread descriptor");
                    // SAFETY: this non-MP-safe CLI holds WorkerBarrier until
                    // the handler returns; the worker is not borrowing its main.
                    unsafe { threads.worker_main_at_barrier(worker) }
                        .node_error_count_since_clear(index)
                };
                total = total.wrapping_add(count);
                if count != 0 || args.verbose >= 2 {
                    report
                        .rows
                        .push((thread, node, name, descriptor, index, count));
                }
            }
            if total != 0 && args.verbose != 0 {
                report.totals.push((name, descriptor, index, total));
            }
        }
    }
    report
        .rows
        .sort_unstable_by_key(|(thread, node, _, _, index, _)| (*thread, node.slot(), index.get()));
    Ok(report)
}

#[hammer_component_macros::cli_command(
    path = "clear errors",
    args = ClearErrorsArgs,
    short_help = "clear errors",
    mp_safe = false,
)]
fn clear_errors(main: &mut DataPlaneMain, _: ClearErrorsArgs) -> Result<(), CliError> {
    let threads = ThreadMain::global();
    main.clear_node_error_counters();
    for thread_index in 1..=threads.worker_count() {
        let worker = threads
            .thread_by_index(thread_index)
            .expect("Data Worker has a thread descriptor");
        // SAFETY: this non-MP-safe CLI holds WorkerBarrier for the full pass.
        unsafe { threads.worker_main_at_barrier(worker) }.clear_node_error_counters();
    }
    Ok(())
}
