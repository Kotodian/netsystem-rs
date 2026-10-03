use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::buffer::{BufferMain, BufferPoolUsage};
use hammer_runtime::DataPlaneMain;
use hammer_runtime::ThreadMain;
use hammer_runtime::cli::CliError;

struct BufferArgs {
    detail: bool,
}

impl FromStr for BufferArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim() {
            "" => Ok(Self { detail: false }),
            "detail" => Ok(Self { detail: true }),
            _ => Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        }
    }
}

struct BufferReport {
    detail: bool,
    rows: Vec<(u8, String, u32, usize, usize, BufferPoolUsage, Vec<u32>)>,
}

impl Display for BufferReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<20}{:>6}{:>6}{:>6}{:>11}{:>6}{:>8}{:>8}{:>8}",
            "Pool Name", "Index", "NUMA", "Size", "Data Size", "Total", "Avail", "Cached", "Used"
        )?;
        for (index, name, numa, size, data_size, usage, per_thread) in &self.rows {
            writeln!(
                f,
                "{name:<20}{index:>6}{numa:>6}{size:>6}{data_size:>11}{:>6}{:>8}{:>8}{:>8}",
                usage.buffer_count,
                usage.available,
                usage.cached,
                usage.used(),
            )?;
            if self.detail {
                for (thread, cached) in per_thread.iter().enumerate() {
                    writeln!(f, "{:>20}{thread:>6}{:>37}{cached:>8}", "thread", "")?;
                }
            }
        }
        Ok(())
    }
}

#[hammer_component_macros::cli_command(
    path = "buffer",
    args = BufferArgs,
    short_help = "buffer [detail]",
    mp_safe = true,
)]
fn buffer(_: &mut DataPlaneMain, args: BufferArgs) -> Result<BufferReport, CliError> {
    let main = BufferMain::global();
    let thread_count = ThreadMain::global().worker_count() + 1;
    let mut report = BufferReport {
        detail: args.detail,
        rows: Vec::with_capacity(main.pool_count()),
    };
    for pool_index in 0..main.pool_count() {
        let index = u8::try_from(pool_index).expect("Pool indices fit u8");
        let (numa, size, data_size) = main.pool_properties(index);
        let usage = main.pool_usage(index);
        let per_thread = if args.detail {
            (0..thread_count)
                .map(|thread| main.pool_cached_count(index, thread))
                .collect()
        } else {
            Vec::new()
        };
        report.rows.push((
            index,
            main.pool_name(index).to_owned(),
            numa,
            size,
            data_size,
            usage,
            per_thread,
        ));
    }
    Ok(report)
}
