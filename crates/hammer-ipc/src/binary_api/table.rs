//! Socket table records and the distinct shared-memory message table encoding.
use super::{ApiMain, Array, Typedef, codec::Error};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Typedef)]
pub struct MessageTableEntry {
    pub index: u16,
    #[api(string)]
    pub name: [u8; 64],
}
impl Serialize for MessageTableEntry {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_struct("message_table_entry", 2)?;
        fields.serialize_field("index", &self.index)?;
        fields.serialize_field("name", self.name.as_slice())?;
        fields.end()
    }
}
impl<'de> Deserialize<'de> for MessageTableEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MessageTable;
        impl<'de> Visitor<'de> for MessageTable {
            type Value = MessageTableEntry;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("API message table entry")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
                let index = fields
                    .next_element()?
                    .ok_or_else(|| A::Error::missing_field("index"))?;
                let name = fields
                    .next_element_seed(Array::<[u8; 64]>::new())?
                    .ok_or_else(|| A::Error::missing_field("name"))?;
                Ok(MessageTableEntry { index, name })
            }
        }
        deserializer.deserialize_struct("message_table_entry", &["index", "name"], MessageTable)
    }
}

impl ApiMain {
    pub fn socket_message_table(&self) -> impl Iterator<Item = MessageTableEntry> + '_ {
        self.message_table().map(|(key, index)| {
            let mut name = [0; 64];
            // socket_api.c exports at most 63 bytes and a terminator. Do not
            // invent an error retval for this truncation policy.
            let length = key.len().min(63);
            name[..length].copy_from_slice(&key.as_bytes()[..length]);
            MessageTableEntry { index, name }
        })
    }

    pub fn serialize_message_table(&self, output: &mut [u8]) -> Result<usize, Error> {
        let mut remaining = output;
        put(
            &mut remaining,
            &(self.message_table().count() as u32).to_be_bytes(),
        )?;
        let capacity = remaining.len() + 4;
        for (name, id) in self.message_table() {
            integer(&mut remaining, u64::from(id))?;
            // serialize_cstring uses compact strlen followed by bytes, WITHOUT
            // a wire NUL; unserialize_cstring adds the local terminator.
            integer(&mut remaining, name.len() as u64)?;
            put(&mut remaining, name.as_bytes())?;
        }
        Ok(capacity - remaining.len())
    }
}

pub fn deserialize_message_table(input: &[u8]) -> Result<Vec<(std::string::String, u16)>, Error> {
    let mut remaining = input;
    let mut count = [0; 4];
    count.copy_from_slice(take(&mut remaining, 4)?);
    let mut entries = Vec::new();
    for _ in 0..u32::from_be_bytes(count) {
        let id = u16::try_from(read_integer(&mut remaining)?).map_err(Error::custom)?;
        let length = usize::try_from(read_integer(&mut remaining)?).map_err(Error::custom)?;
        let name = std::str::from_utf8(take(&mut remaining, length)?)
            .map_err(Error::custom)?
            .to_owned();
        entries.push((name, id));
    }
    Ok(entries)
}

fn put(output: &mut &mut [u8], bytes: &[u8]) -> Result<(), Error> {
    if output.len() < bytes.len() {
        return Err(Error::custom("API message table output is too short"));
    }
    let (prefix, remaining) = std::mem::take(output).split_at_mut(bytes.len());
    prefix.copy_from_slice(bytes);
    *output = remaining;
    Ok(())
}
fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8], Error> {
    if input.len() < length {
        return Err(Error::custom("truncated API message table"));
    }
    let (prefix, remaining) = input.split_at(length);
    *input = remaining;
    Ok(prefix)
}
fn integer(output: &mut &mut [u8], value: u64) -> Result<(), Error> {
    if value < 128 {
        put(output, &[(1 + 2 * value) as u8])
    } else if value - 128 < 16384 {
        put(output, &((4 * (value - 128) + 2) as u16).to_le_bytes())
    } else if value - 128 - 16384 < (1 << 29) {
        put(
            output,
            &((8 * (value - 128 - 16384) + 4) as u32).to_le_bytes(),
        )
    } else {
        put(output, &[0])?;
        put(output, &value.to_le_bytes())
    }
}
fn read_integer(input: &mut &[u8]) -> Result<u64, Error> {
    let byte = take(input, 1)?[0];
    if byte & 1 != 0 {
        return Ok(u64::from(byte / 2));
    }
    if byte & 2 != 0 {
        return Ok(128 + u64::from(u16::from_le_bytes([byte, take(input, 1)?[0]]) / 4));
    }
    if byte & 4 != 0 {
        let rest = take(input, 3)?;
        return Ok(128
            + 16384
            + u64::from(u32::from_le_bytes([byte, rest[0], rest[1], rest[2]]) / 8));
    }
    let mut bytes = [0; 8];
    bytes.copy_from_slice(take(input, 8)?);
    Ok(u64::from_le_bytes(bytes))
}
