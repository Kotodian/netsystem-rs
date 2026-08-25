#[hammer_component_macros::file]
pub mod stats_segment_socket {
    use hammer_core::file::File;

    use crate::{StatsError, StatsMain};

    fn read<Context, Error>(
        _context: &Context,
        file: &mut File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<StatsError>,
    {
        StatsMain::global()?.accept(file.fd())?;
        Ok(())
    }

    fn error<Context, Error>(
        context: &Context,
        file: &mut File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<StatsError>,
    {
        read(context, file)
    }
}
