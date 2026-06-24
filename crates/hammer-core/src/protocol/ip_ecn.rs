#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IpEcnCodepoint {
    NotEct = 0,
    Ect1 = 1,
    Ect0 = 2,
    Ce = 3,
}

impl From<IpEcnCodepoint> for u8 {
    #[inline]
    fn from(value: IpEcnCodepoint) -> Self {
        value as u8
    }
}

impl From<u8> for IpEcnCodepoint {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Ect1,
            2 => Self::Ect0,
            3 => Self::Ce,
            _ => Self::NotEct,
        }
    }
}
