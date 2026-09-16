//! Static Binary API definitions. These describe the protocol, not Rust memory
//! layout. The CRC follows vppapigen's original block and dependency folding;
//! message IDs, service declarations, flags and options are not payload CRCs.

pub use hammer_component_macros::{Api, Typedef};
#[doc(hidden)]
pub use serde;

/// A concrete owned message. `#[derive(Api)]` generates its protocol codec from
/// the same field declarations used for identity and CRC generation.
pub trait Api: serde::Serialize + for<'de> serde::Deserialize<'de> {
    const NAME: &'static str;
    const BLOCK: Block;
    const CRC: u32;
    const NAME_CRC: &'static str;
    const SERVICE: Option<Service> = None;
    const OPTIONS: &'static [(&'static str, Option<&'static str>)] = &[];
    const FLAGS: &'static [&'static str] = &[];
}

/// A non-message protocol type. Rust type aliases retain the target's identity.
pub trait Typedef {
    const NAME: &'static str;
    const BLOCK: Block;
}

/// Service facts have the same meaning as vppapigen's Service. In a traditional
/// stream, `reply` names details and `stream_message` is absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Service {
    pub caller: &'static str,
    pub reply: Option<&'static str>,
    pub stream: bool,
    pub stream_message: Option<&'static str>,
    pub events: &'static [&'static str],
}

/// The original protocol declaration block. Aliases deliberately contribute
/// `[]`, matching Using's compatibility rule, rather than their target's block.
#[derive(Clone, Copy, Debug)]
pub enum Block {
    Fields(&'static [Field]),
    Enum(&'static [(&'static str, i64)]),
    Alias,
}

/// One declared field. A referenced block makes `field_type` a nominal protocol
/// name (rendered as `vl_api_<name>_t`); without one it is a primitive name.
#[derive(Clone, Copy, Debug)]
pub struct Field {
    pub name: &'static str,
    pub field_type: &'static str,
    pub block: Option<Block>,
    pub length: Option<usize>,
    pub length_field: Option<&'static str>,
}

impl Block {
    /// Whether the declaration ends in variable-length storage.
    pub const fn is_vla(self) -> bool {
        match self {
            Self::Fields(fields) => {
                let mut index = 0;
                while index < fields.len() {
                    let field = fields[index];
                    if field.length_field.is_some()
                        || matches!(field.length, Some(0))
                        || matches!(field.block, Some(block) if block.is_vla())
                    {
                        return true;
                    }
                    index += 1;
                }
                false
            }
            Self::Enum(_) | Self::Alias => false,
        }
    }

    /// Compile-time declaration constraint, including nested VLA types.
    pub const fn validate(self) {
        if let Self::Fields(fields) = self {
            let mut index = 0;
            while index < fields.len() {
                let field = fields[index];
                if let Some(block) = field.block {
                    block.validate();
                }
                if field.length_field.is_some()
                    || matches!(field.length, Some(0))
                    || matches!(field.block, Some(block) if block.is_vla())
                {
                    assert!(index + 1 == fields.len(), "VLA must be the last API field");
                }
                index += 1;
            }
        }
    }

    /// CRC of the original block, followed by each referenced type in field
    /// order, recursively. Feed the block bytes, not a referenced final CRC.
    pub const fn crc(self) -> u32 {
        self.validate();
        !fold(self, block_crc(!0, self))
    }
}

const fn byte_crc(mut crc: u32, byte: u8) -> u32 {
    crc ^= byte as u32;
    let mut bit = 0;
    while bit < 8 {
        crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        bit += 1;
    }
    crc
}

const fn text_crc(mut crc: u32, text: &str) -> u32 {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        crc = byte_crc(crc, bytes[index]);
        index += 1;
    }
    crc
}

const fn quoted_crc(crc: u32, text: &str) -> u32 {
    // Macro-validated protocol identifiers cannot contain quotes or escapes.
    byte_crc(text_crc(byte_crc(crc, b'\''), text), b'\'')
}

const fn integer_crc(mut crc: u32, value: u64) -> u32 {
    let mut divisor = 1;
    while value / divisor >= 10 {
        divisor *= 10;
    }
    loop {
        crc = byte_crc(crc, b'0' + ((value / divisor) % 10) as u8);
        if divisor == 1 {
            return crc;
        }
        divisor /= 10;
    }
}

const fn block_crc(mut crc: u32, block: Block) -> u32 {
    crc = byte_crc(crc, b'[');
    match block {
        Block::Fields(fields) => {
            let mut index = 0;
            while index < fields.len() {
                if index != 0 {
                    crc = text_crc(crc, ", ");
                }
                let field = fields[index];
                crc = text_crc(crc, "['");
                if field.block.is_some() {
                    crc = text_crc(crc, "vl_api_");
                }
                crc = text_crc(crc, field.field_type);
                if field.block.is_some() {
                    crc = text_crc(crc, "_t");
                }
                crc = text_crc(crc, "', ");
                crc = quoted_crc(crc, field.name);
                if let Some(length) = field.length {
                    crc = integer_crc(text_crc(crc, ", "), length as u64);
                    crc = text_crc(crc, ", ");
                    crc = match field.length_field {
                        Some(name) => quoted_crc(crc, name),
                        None => text_crc(crc, "None"),
                    };
                }
                crc = byte_crc(crc, b']');
                index += 1;
            }
        }
        Block::Enum(values) => {
            let mut index = 0;
            while index < values.len() {
                if index != 0 {
                    crc = text_crc(crc, ", ");
                }
                let (name, value) = values[index];
                crc = quoted_crc(byte_crc(crc, b'['), name);
                crc = text_crc(crc, ", ");
                if value < 0 {
                    crc = byte_crc(crc, b'-');
                }
                crc = integer_crc(crc, value.unsigned_abs());
                crc = byte_crc(crc, b']');
                index += 1;
            }
        }
        Block::Alias => {}
    }
    byte_crc(crc, b']')
}

const fn fold(block: Block, mut crc: u32) -> u32 {
    // Only Fields contain dependencies. Alias and enum blocks have none.
    if let Block::Fields(fields) = block {
        let mut index = 0;
        while index < fields.len() {
            if let Some(dependency) = fields[index].block {
                crc = fold(dependency, block_crc(crc, dependency));
            }
            index += 1;
        }
    }
    crc
}

/// Macro support for the compile-time `name_crc` key; no allocation or payload.
#[doc(hidden)]
pub const fn name_crc<const N: usize>(name: &str, crc: u32) -> [u8; N] {
    assert!(
        N == name.len() + 9,
        "API name_crc length must match its name"
    );
    let mut output = [0; N];
    let mut index = 0;
    while index < name.len() {
        output[index] = name.as_bytes()[index];
        index += 1;
    }
    output[index] = b'_';
    let digits = b"0123456789abcdef";
    let mut digit = 0;
    while digit < 8 {
        output[index + 1 + digit] = digits[((crc >> ((7 - digit) * 4)) & 15) as usize];
        digit += 1;
    }
    output
}

/// Cross-type identity comparison used by generated constant assertions.
#[doc(hidden)]
pub const fn same_name(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Module CRC folds original declarations in order, before autoreply expansion.
/// The module owner supplies its explicit type/message inventory.
pub const fn file_crc(blocks: &[Block]) -> u32 {
    let mut crc = !0;
    let mut index = 0;
    while index < blocks.len() {
        crc = block_crc(crc, blocks[index]);
        index += 1;
    }
    !crc
}

/// Validate the explicit module service set without name-suffix inference.
pub const fn validate_services(services: &[Option<Service>]) {
    let mut index = 0;
    while index < services.len() {
        if let Some(service) = services[index] {
            let mut other = 0;
            while other < services.len() {
                if let Some(peer) = services[other]
                    && let Some(reply) = peer.reply
                {
                    assert!(
                        !same_name(service.caller, reply),
                        "API caller is also a reply"
                    );
                }
                other += 1;
            }
        }
        index += 1;
    }
}
