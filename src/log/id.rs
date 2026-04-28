use std::time::Instant;

use tokio::task_local;

#[derive(Debug, Clone, Copy)]
pub struct ConnId {
    pub id: u64,
    pub created_at: Instant,
}

impl ConnId {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            created_at: Instant::now(),
        }
    }

    /// Truncated 16-bit value used by the format layer to render `c#XXXX`.
    /// Matches Go: `xh(uint16(id.ID), 4)`.
    pub fn short(self) -> u16 {
        self.id as u16
    }
}

task_local! {
    static CURRENT: ConnId;
}

pub fn current() -> Option<ConnId> {
    CURRENT.try_with(|c| *c).ok()
}

pub async fn with_conn_id<F, T>(id: ConnId, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT.scope(id, fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_is_low_16_bits() {
        let c = ConnId::new(0xdead_0042);
        assert_eq!(c.short(), 0x0042);
    }
}
