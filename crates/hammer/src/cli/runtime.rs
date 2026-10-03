use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::data_plane::{NodeId, NodeKind};
use hammer_runtime::cli::CliError;
use hammer_runtime::node::NodeFlags;
use hammer_runtime::{DataPlaneMain, DirectoryType, StatsMain, ThreadMain};

struct RuntimeArgs {
    node: Option<String>,
    verbose: bool,
    time: bool,
    max: bool,
    summary: bool,
}

struct ClearRuntimeArgs;

impl FromStr for ClearRuntimeArgs {
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

impl FromStr for RuntimeArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut args = Self {
            node: None,
            verbose: false,
            time: false,
            max: false,
            summary: false,
        };
        let mut brief = false;
        for word in input.split_whitespace() {
            match word {
                "brief" | "b" if !brief && !args.verbose && args.node.is_none() => brief = true,
                "verbose" | "v" if !brief && !args.verbose && args.node.is_none() => {
                    args.verbose = true
                }
                "time" | "t" if !args.time && args.node.is_none() => args.time = true,
                "max" | "m" if !args.max && args.node.is_none() => args.max = true,
                "summary" | "sum" | "su" if !args.summary && args.node.is_none() => {
                    args.summary = true;
                }
                name if args.node.is_none()
                    && !brief
                    && !args.verbose
                    && !args.time
                    && !args.max
                    && !args.summary =>
                {
                    args.node = Some(name.to_owned());
                }
                _ => {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
            }
        }
        Ok(args)
    }
}

struct RuntimeReport {
    args: RuntimeArgs,
    threads: Vec<(u32, &'static str, Option<u32>, f64, f64, f64)>,
    rows: Vec<(
        u32,
        &'static str,
        NodeKind,
        &'static str,
        NodeFlags,
        u64,
        u64,
        u64,
        u32,
        u32,
    )>,
    process_rows: Vec<(u32, &'static str, u64, u64, u64)>,
    seconds_per_clock: f64,
    update_interval: f64,
}

fn sync_thread_node_stats(main: &mut DataPlaneMain, report: &mut RuntimeReport) {
    let thread_index = main.thread_index();
    let descriptor = ThreadMain::global()
        .thread_by_index(thread_index)
        .expect("each DataPlaneMain has a WorkerThread descriptor");
    report.threads.push((
        thread_index,
        descriptor.name(),
        descriptor.cpu_index(),
        main.internal_node_vector_rate(),
        main.loops_per_second(),
        main.runtime_stats_elapsed_seconds(),
    ));

    for slot in 0..main.nodes().node_count() {
        let node = NodeId::new(slot as u32);
        main.sync_node_stats(node).expect("registered Node syncs");
        let nodes = main.nodes();
        let Some(name) = nodes.node_name(node).expect("registered Node slot") else {
            continue;
        };
        let kind = nodes.node_kind(node).expect("registered Node kind");
        if kind == NodeKind::Process {
            if thread_index != 0
                || !StatsMain::global()
                    .expect("stats is initialized")
                    .segment
                    .node_counters_enabled()
            {
                continue;
            }
            let (completions, _, clocks, pending, _, _) = nodes
                .node_stats(node)
                .expect("started Process has a validated stats column");
            report
                .process_rows
                .push((thread_index, name, completions, pending, clocks));
            continue;
        }
        let (calls, vectors, clocks, _, max_clock, max_clock_n) =
            nodes.node_stats(node).expect("registered Node has stats");
        report.rows.push((
            thread_index,
            name,
            kind,
            nodes
                .node_display_state(node)
                .expect("registered Node state"),
            nodes.node_flags(node).expect("registered Node flags"),
            calls,
            vectors,
            clocks,
            max_clock,
            max_clock_n,
        ));
    }
}

impl Display for RuntimeReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (thread_index, name, cpu, internal_rate, loops, elapsed) in &self.threads {
            if self.args.node.is_none() {
                if let Some(cpu) = cpu {
                    writeln!(f, "Thread {thread_index} {name} (lcore {cpu})")?;
                } else {
                    writeln!(f, "Thread {thread_index} {name}")?;
                }
                let mut traffic = (0_u64, 0_u64, 0_u64, 0_u64);
                for row in self.rows.iter().filter(|row| row.0 == *thread_index) {
                    let (_, _, kind, _, flags, _, vectors, ..) = row;
                    if *kind == NodeKind::Driver || flags.contains(NodeFlags::IS_HANDOFF) {
                        traffic.0 = traffic.0.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_OUTPUT) {
                        traffic.1 = traffic.1.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_DROP) {
                        traffic.2 = traffic.2.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_PUNT) {
                        traffic.3 = traffic.3.wrapping_add(*vectors);
                    }
                }
                let rate = |count| {
                    if *elapsed > 0.0 {
                        count as f64 / *elapsed
                    } else {
                        0.0
                    }
                };
                writeln!(
                    f,
                    "Time {elapsed:.1}, {:.6} sec internal node vector rate {internal_rate:.2} loops/sec {loops:.2}",
                    self.update_interval
                )?;
                writeln!(
                    f,
                    "  vector rates in {:.4e}, out {:.4e}, drop {:.4e}, punt {:.4e}",
                    rate(traffic.0),
                    rate(traffic.1),
                    rate(traffic.2),
                    rate(traffic.3)
                )?;
            }
            if self.args.summary {
                continue;
            }
            if self.args.max {
                writeln!(
                    f,
                    "{:<30}{:>17}{:>16}{:>16}{:>16}{:>16}",
                    "Name",
                    "Max Node Clocks",
                    "Vectors at Max",
                    "Max Clocks",
                    if self.args.time {
                        "Avg Time (ns)"
                    } else {
                        "Avg Clocks"
                    },
                    "Avg Vectors/Call"
                )?;
            } else {
                writeln!(
                    f,
                    "{:<30}{:>12}{:>16}{:>16}{:>16}{:>16}{:>16}",
                    "Name",
                    "State",
                    "Calls",
                    "Vectors",
                    "Suspends",
                    if self.args.time {
                        "Packet-Time"
                    } else {
                        "Packet-Clocks"
                    },
                    "Vectors/Call"
                )?;
            }
            for row in self.rows.iter().filter(|row| row.0 == *thread_index) {
                let (_, node_name, _, state, _, calls, vectors, clocks, max_clock, max_clock_n) =
                    row;
                if !self.args.verbose && *calls == 0 && self.args.node.is_none() {
                    continue;
                }
                let denominator = if *vectors != 0 { *vectors } else { *calls };
                let mut average = if denominator == 0 {
                    0.0
                } else {
                    *clocks as f64 / denominator as f64
                };
                if self.args.time {
                    average *= 1e9 * self.seconds_per_clock;
                }
                let vectors_per_call = if *calls == 0 {
                    0.0
                } else {
                    *vectors as f64 / *calls as f64
                };
                if self.args.max {
                    let per_vector = if *max_clock_n == 0 {
                        0.0
                    } else {
                        *max_clock as f64 / *max_clock_n as f64
                    };
                    writeln!(
                        f,
                        "{node_name:<30}{max_clock:>17}{max_clock_n:>16}{per_vector:>16.2}{average:>16.2}{vectors_per_call:>16.2}"
                    )?;
                } else {
                    writeln!(
                        f,
                        "{node_name:<30}{state:>12}{calls:>16}{vectors:>16}{:>16}{average:>16.2}{vectors_per_call:>16.2}",
                        0_u64
                    )?;
                }
            }
            if self.process_rows.iter().any(|row| row.0 == *thread_index) {
                writeln!(
                    f,
                    "{:<30}{:>16}{:>16}{:>18}",
                    "Process",
                    "Completions",
                    "Pending polls",
                    if self.args.time {
                        "Poll time (ns)"
                    } else {
                        "Poll clocks"
                    }
                )?;
                for (_, name, completions, pending, clocks) in self
                    .process_rows
                    .iter()
                    .filter(|row| row.0 == *thread_index)
                {
                    if !self.args.verbose
                        && *completions == 0
                        && *pending == 0
                        && self.args.node.is_none()
                    {
                        continue;
                    }
                    let measured = if self.args.time {
                        *clocks as f64 * self.seconds_per_clock * 1e9
                    } else {
                        *clocks as f64
                    };
                    writeln!(
                        f,
                        "{name:<30}{completions:>16}{pending:>16}{measured:>18.2}"
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[hammer_component_macros::cli_command(
    path = "runtime",
    args = RuntimeArgs,
    short_help = "runtime [node|time|brief|verbose|max|summary]",
    mp_safe = true,
)]
fn runtime(main: &mut DataPlaneMain, args: RuntimeArgs) -> Result<RuntimeReport, CliError> {
    let mut report = RuntimeReport {
        args,
        threads: Vec::new(),
        rows: Vec::new(),
        process_rows: Vec::new(),
        seconds_per_clock: main.seconds_per_cpu_tick(),
        update_interval: StatsMain::global()
            .expect("stats is initialized")
            .segment
            .update_interval()
            .as_secs_f64(),
    };
    if let Some(name) = report.args.node.as_deref() {
        let node = main
            .nodes()
            .node_by_name(name)
            .ok_or_else(|| CliError::InvalidArgument {
                argument: name.to_owned(),
            })?;
        main.sync_node_stats(node).expect("named Node syncs");
        let descriptor = ThreadMain::global()
            .thread_by_index(0)
            .expect("thread-zero descriptor exists");
        report.threads.push((
            0,
            descriptor.name(),
            descriptor.cpu_index(),
            main.internal_node_vector_rate(),
            main.loops_per_second(),
            main.runtime_stats_elapsed_seconds(),
        ));
        let nodes = main.nodes();
        let node_name = nodes
            .node_name(node)
            .expect("named Node slot")
            .expect("named Node has a name");
        if nodes.node_kind(node).expect("named Node kind") == NodeKind::Process {
            if StatsMain::global()
                .expect("stats is initialized")
                .segment
                .node_counters_enabled()
            {
                let (completions, _, clocks, pending, _, _) = nodes
                    .node_stats(node)
                    .expect("started Process has a validated stats column");
                report
                    .process_rows
                    .push((0, node_name, completions, pending, clocks));
            }
        } else {
            let (calls, vectors, clocks, _, max_clock, max_clock_n) =
                nodes.node_stats(node).expect("named Node has stats");
            report.rows.push((
                0,
                node_name,
                nodes.node_kind(node).expect("named Node kind"),
                nodes.node_display_state(node).expect("named Node state"),
                nodes.node_flags(node).expect("named Node flags"),
                calls,
                vectors,
                clocks,
                max_clock,
                max_clock_n,
            ));
        }
        report.args.time = false;
        report.args.max = false;
        report.args.summary = false;
        return Ok(report);
    }
    hammer_runtime::worker_thread_barrier_sync!(main, {
        sync_thread_node_stats(main, &mut report);
        for thread_index in 1..=ThreadMain::global().worker_count() {
            let worker = ThreadMain::global()
                .thread_by_index(thread_index)
                .expect("Data Worker has a thread descriptor");
            // SAFETY: the barrier has stopped this worker; the mutable borrow
            // ends before release and cannot overlap the worker's own borrow.
            let owner = unsafe { ThreadMain::global().worker_main_at_barrier(worker) };
            sync_thread_node_stats(owner, &mut report);
        }
    });
    report
        .rows
        .sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    report
        .process_rows
        .sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    Ok(report)
}

#[hammer_component_macros::cli_command(
    path = "clear runtime",
    args = ClearRuntimeArgs,
    short_help = "clear runtime",
    mp_safe = false,
)]
fn clear_runtime(main: &mut DataPlaneMain, _: ClearRuntimeArgs) -> Result<(), CliError> {
    let threads = ThreadMain::global();
    let timestamp = main.clear_runtime_stats();
    for worker in threads.data_workers() {
        // SAFETY: the CLI directory holds WorkerBarrier until this handler
        // returns, so the worker cannot borrow its DataPlaneMain here.
        unsafe { threads.worker_main_at_barrier(worker) }.clear_runtime_stats();
    }
    let segment = &StatsMain::global()
        .expect("stats is initialized before CLI dispatch")
        .segment;
    let entry = segment
        .find("/sys/last_stats_clear", DirectoryType::ScalarIndex)
        .expect("the system stats declaration installs last_stats_clear");
    segment.set_timestamp(entry, timestamp);
    Ok(())
}
