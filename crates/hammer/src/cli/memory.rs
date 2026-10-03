use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_infra::mem::{HeapUsage, MemMain};
use hammer_ipc::binary_api::ApiMain;
use hammer_runtime::cli::CliError;
use hammer_runtime::{DataPlaneMain, StatsMain};

struct MemoryArgs {
    api_segment: bool,
    stats_segment: bool,
    main_heap: bool,
    map: bool,
    verbose: bool,
}

impl FromStr for MemoryArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut args = Self {
            api_segment: false,
            stats_segment: false,
            main_heap: false,
            map: false,
            verbose: false,
        };
        for word in input.split_whitespace() {
            let selected = match word {
                "api-segment" => &mut args.api_segment,
                "stats-segment" => &mut args.stats_segment,
                "main-heap" => &mut args.main_heap,
                "map" => &mut args.map,
                "verbose" => &mut args.verbose,
                _ => {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
            };
            if *selected {
                return Err(CliError::InvalidArgument {
                    argument: input.to_owned(),
                });
            }
            *selected = true;
        }
        Ok(args)
    }
}

struct MemoryReport {
    verbose: bool,
    show_map: bool,
    heaps: Vec<(String, usize, usize, HeapUsage)>,
    mappings: Vec<(
        usize,
        usize,
        i32,
        u8,
        usize,
        Vec<(u32, u64)>,
        u64,
        u64,
        String,
    )>,
}

impl Display for MemoryReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (name, base, size, usage) in &self.heaps {
            writeln!(f, "base {base:#x}, size {size}, name '{name}'")?;
            writeln!(
                f,
                "  total: {}, used: {}, free: {}, trimmable: {}",
                usage.total_bytes, usage.used_bytes, usage.free_bytes, usage.releasable_bytes
            )?;
            if self.verbose {
                writeln!(
                    f,
                    "  free chunks {}, max allocated {}",
                    usage.free_chunk_count, usage.max_allocated_bytes
                )?;
            }
            writeln!(f)?;
        }
        if self.show_map {
            writeln!(
                f,
                "StartAddr        size   FD  PageSz  Pages  NotPop  Unknown Name"
            )?;
            for (base, size, fd, page_log2, pages, per_numa, not_populated, unknown, name) in
                &self.mappings
            {
                write!(
                    f,
                    "{base:016x} {size:>7} {fd:>4} {:>7} {pages:>6} {not_populated:>7} {unknown:>8} {name}",
                    1usize << *page_log2
                )?;
                for (node, count) in per_numa {
                    write!(f, " Numa{node}:{count}")?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

/// VPP `show_memory_usage`: select the current owner heap or VM map and copy
/// its reading before the CLI command returns a formatter result.
#[hammer_component_macros::cli_command(
    path = "memory",
    args = MemoryArgs,
    short_help = "memory [api-segment] [stats-segment] [main-heap] [map] [verbose]",
    mp_safe = false,
)]
fn memory(_: &mut DataPlaneMain, args: MemoryArgs) -> Result<MemoryReport, CliError> {
    let mut report = MemoryReport {
        verbose: args.verbose,
        show_map: args.map,
        heaps: Vec::new(),
        mappings: Vec::new(),
    };
    if !args.api_segment && !args.stats_segment && !args.main_heap && !args.map {
        report.heaps = MemMain::heap_usages();
        report.verbose = true;
        return Ok(report);
    }
    if args.api_segment {
        report
            .heaps
            .push(ApiMain::current().api_segment_heap_usage());
    }
    if args.stats_segment {
        let heap = StatsMain::global()
            .expect("CLI starts after stats initialization")
            .segment
            .heap();
        report.heaps.push((
            heap.name().to_owned(),
            heap.base().as_ptr() as usize,
            heap.size(),
            heap.usage(),
        ));
    }
    if args.main_heap {
        let heap = MemMain::main_heap();
        report.heaps.push((
            heap.name().to_owned(),
            heap.base().as_ptr() as usize,
            heap.size(),
            heap.usage(),
        ));
    }
    if args.map {
        report.mappings =
            MemMain::mappings().map_err(|source| CliError::MemoryMapping { source })?;
    }
    Ok(report)
}
