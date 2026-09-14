//! The API wire format over borrowed message storage. Diagnostics use Serde's
//! existing value error; they are local codec diagnostics, never API retval.
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use serde::de::Error as _;
pub use serde::de::value::Error;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Deserialize, Serialize};

pub struct Serializer<'a> {
    output: &'a mut [MaybeUninit<u8>],
    offset: usize,
    opaque: bool,
}

pub struct Deserializer<'de> {
    input: &'de [u8],
    offset: usize,
    remaining: usize,
    fields: &'static [&'static str],
    opaque: bool,
}

pub fn serialize<T: Serialize + ?Sized>(value: &T, output: &mut [u8]) -> Result<usize, Error> {
    let mut serializer = Serializer::new(output);
    value.serialize(&mut serializer)?;
    Ok(serializer.finish())
}

pub fn serialize_uninit<T: Serialize + ?Sized>(
    value: &T,
    output: &mut [MaybeUninit<u8>],
) -> Result<usize, Error> {
    let mut serializer = Serializer {
        output,
        offset: 0,
        opaque: false,
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.finish())
}

pub fn deserialize<'de, T: Deserialize<'de>>(input: &'de [u8]) -> Result<T, Error> {
    T::deserialize(&mut Deserializer::new(input))
}

impl<'a> Serializer<'a> {
    pub fn new(output: &'a mut [u8]) -> Self {
        let output =
            unsafe { std::slice::from_raw_parts_mut(output.as_mut_ptr().cast(), output.len()) };
        Self {
            output,
            offset: 0,
            opaque: false,
        }
    }

    pub fn finish(self) -> usize {
        self.offset
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .filter(|end| *end <= self.output.len())
            .ok_or_else(|| Error::custom("API output slice is too short"))?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.output.as_mut_ptr().add(self.offset).cast(),
                bytes.len(),
            );
        }
        self.offset = end;
        Ok(())
    }
}

impl<'de> Deserializer<'de> {
    pub fn new(input: &'de [u8]) -> Self {
        Self {
            input,
            offset: 0,
            remaining: 0,
            fields: &[],
            opaque: false,
        }
    }

    /// Consumed bytes. VPP dispatch permits a larger containing message.
    pub fn finish(self) -> usize {
        self.offset
    }

    pub fn remaining_bytes(&self) -> usize {
        self.input.len() - self.offset
    }

    /// Decode a legacy tail array after the containing owner has selected its
    /// fixed element wire size. No other message fields may follow this call.
    pub fn deserialize_legacy<T: Deserialize<'de>>(
        &mut self,
        element_size: usize,
    ) -> Result<Vec<T>, Error> {
        if element_size == 0 || !self.remaining_bytes().is_multiple_of(element_size) {
            return Err(Error::custom("legacy API array length mismatch"));
        }
        Array::<Vec<T>>::new(self.remaining_bytes() / element_size).deserialize(self)
    }

    fn read(&mut self, length: usize) -> Result<&'de [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or_else(|| Error::custom("truncated API message"))?;
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let mut bytes = [0; N];
        bytes.copy_from_slice(self.read(N)?);
        Ok(bytes)
    }

    fn sequence<V: Visitor<'de>>(
        &mut self,
        length: usize,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        // API array elements must consume at least one wire byte. Do not let
        // an untrusted count drive a huge loop over zero-size Serde values.
        if fields.is_empty() && length > self.remaining_bytes() {
            return Err(Error::custom("truncated API array"));
        }
        let parent_remaining = self.remaining;
        let parent_fields = self.fields;
        self.remaining = length;
        self.fields = fields;
        let value = visitor.visit_seq(&mut *self);
        let remaining = self.remaining;
        self.remaining = parent_remaining;
        self.fields = parent_fields;
        let value = value?;
        if remaining != 0 {
            return Err(Error::custom("API array element count mismatch"));
        }
        Ok(value)
    }
}

macro_rules! serialize_integer {
    ($($method:ident: $ty:ty),* $(,)?) => {$ (
        fn $method(self, value: $ty) -> Result<(), Error> {
            let bytes = if self.opaque { value.to_ne_bytes() } else { value.to_be_bytes() };
            self.write(&bytes)
        }
    )*};
}

impl<'a> serde::Serializer for &mut Serializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeStruct = Self;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;

    serialize_integer!(serialize_u8: u8, serialize_u16: u16, serialize_u32: u32, serialize_u64: u64,
        serialize_i8: i8, serialize_i16: i16, serialize_i32: i32, serialize_i64: i64);
    fn serialize_bool(self, value: bool) -> Result<(), Error> {
        self.write(&[u8::from(value)])
    }
    fn serialize_f64(self, value: f64) -> Result<(), Error> {
        self.write(&value.to_ne_bytes())
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<(), Error> {
        let length = u32::try_from(value.len()).map_err(Error::custom)?;
        if self.output.len() - self.offset < 4 || value.len() > self.output.len() - self.offset - 4
        {
            return Err(Error::custom("API output slice is too short"));
        }
        self.write(&length.to_be_bytes())?;
        self.write(value)
    }
    fn serialize_str(self, value: &str) -> Result<(), Error> {
        self.serialize_bytes(value.as_bytes())
    }
    fn serialize_seq(self, length: Option<usize>) -> Result<Self, Error> {
        if length.is_none() {
            return Err(Error::custom("API array requires a length"));
        }
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        Ok(())
    }
    fn is_human_readable(&self) -> bool {
        false
    }
    fn serialize_i128(self, _: i128) -> Result<(), Error> {
        Err(Error::custom("i128 is not an API primitive"))
    }
    fn serialize_u128(self, _: u128) -> Result<(), Error> {
        Err(Error::custom("u128 is not an API primitive"))
    }
    fn serialize_f32(self, _: f32) -> Result<(), Error> {
        Err(Error::custom("f32 is not an API primitive"))
    }
    fn serialize_char(self, _: char) -> Result<(), Error> {
        Err(Error::custom("char is not an API primitive"))
    }
    fn serialize_none(self) -> Result<(), Error> {
        Err(Error::custom("API fields are not optional"))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<(), Error> {
        Err(Error::custom("API fields are not optional"))
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(Error::custom("API enums require numeric serialization"))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), Error> {
        Err(Error::custom(
            "API unions require their declared wire layout",
        ))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::custom(
            "API unions require their declared wire layout",
        ))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::custom(
            "API unions require their declared wire layout",
        ))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(Error::custom("maps have no API wire format"))
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, _: &T) -> Result<(), Error> {
        Err(Error::custom("Display is not an API field definition"))
    }
}

impl SerializeStruct for &mut Serializer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        // vppapigen_c.NO_ENDIAN_CONVERSION: client_index is an opaque token.
        let opaque = self.opaque;
        self.opaque = name == "client_index";
        let result = value.serialize(&mut **self);
        self.opaque = opaque;
        result
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}
macro_rules! serialize_elements {
    ($trait:ident, $method:ident) => {
        impl $trait for &mut Serializer<'_> {
            type Ok = ();
            type Error = Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
                value.serialize(&mut **self)
            }
            fn end(self) -> Result<(), Error> {
                Ok(())
            }
        }
    };
}
serialize_elements!(SerializeSeq, serialize_element);
serialize_elements!(SerializeTuple, serialize_element);
serialize_elements!(SerializeTupleStruct, serialize_field);

macro_rules! deserialize_integer {
    ($($method:ident, $visit:ident, $ty:ty),* $(,)?) => {$ (
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            let bytes = self.read_array()?;
            visitor.$visit(if self.opaque { <$ty>::from_ne_bytes(bytes) } else { <$ty>::from_be_bytes(bytes) })
        }
    )*};
}
macro_rules! unsupported_deserialize {
    ($($method:ident),* $(,)?) => {$ (
        fn $method<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Error> {
            Err(Error::custom(concat!(stringify!($method), " is not an API wire operation")))
        }
    )*};
}
impl<'de> serde::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;
    deserialize_integer!(
        deserialize_u8,
        visit_u8,
        u8,
        deserialize_u16,
        visit_u16,
        u16,
        deserialize_u32,
        visit_u32,
        u32,
        deserialize_u64,
        visit_u64,
        u64,
        deserialize_i8,
        visit_i8,
        i8,
        deserialize_i16,
        visit_i16,
        i16,
        deserialize_i32,
        visit_i32,
        i32,
        deserialize_i64,
        visit_i64,
        i64
    );
    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_bool(self.read_array::<1>()?[0] != 0)
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_f64(f64::from_ne_bytes(self.read_array()?))
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let length = u32::from_be_bytes(self.read_array()?) as usize;
        visitor.visit_borrowed_bytes(self.read(length)?)
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let length = u32::from_be_bytes(self.read_array()?) as usize;
        visitor.visit_byte_buf(self.read(length)?.to_vec())
    }
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let length = u32::from_be_bytes(self.read_array()?) as usize;
        visitor.visit_borrowed_str(std::str::from_utf8(self.read(length)?).map_err(Error::custom)?)
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let length = u32::from_be_bytes(self.read_array()?) as usize;
        visitor.visit_string(
            std::str::from_utf8(self.read(length)?)
                .map_err(Error::custom)?
                .to_owned(),
        )
    }
    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.sequence(length, &[], visitor)
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.sequence(length, &[], visitor)
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.sequence(fields.len(), fields, visitor)
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Error> {
        Err(Error::custom("API enums require numeric deserialization"))
    }
    fn is_human_readable(&self) -> bool {
        false
    }
    unsupported_deserialize!(
        deserialize_any,
        deserialize_seq,
        deserialize_i128,
        deserialize_u128,
        deserialize_f32,
        deserialize_char,
        deserialize_option,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any
    );
}
impl<'de> SeqAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let opaque = self.opaque;
        self.opaque = self
            .fields
            .get(self.fields.len().saturating_sub(self.remaining))
            == Some(&"client_index");
        self.remaining -= 1;
        let array_element = self.fields.is_empty();
        let offset = self.offset;
        let result = seed.deserialize(&mut **self).map(Some);
        self.opaque = opaque;
        if result.is_ok() && array_element && self.offset == offset {
            return Err(Error::custom("API array elements must consume wire bytes"));
        }
        result
    }
}

/// Array element count supplied by the containing protocol definition.
pub struct Array<T> {
    length: usize,
    value: PhantomData<fn() -> T>,
}
impl<T> Array<Vec<T>> {
    pub fn new(length: usize) -> Self {
        Self {
            length,
            value: PhantomData,
        }
    }
}
impl<T, const N: usize> Array<[T; N]> {
    pub fn new() -> Self {
        Self {
            length: N,
            value: PhantomData,
        }
    }
}
impl<T, const N: usize> Default for Array<[T; N]> {
    fn default() -> Self {
        Self::new()
    }
}
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for Array<Vec<T>> {
    type Value = Vec<T>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_tuple(self.length, self)
    }
}
impl<'de, T: Deserialize<'de>> Visitor<'de> for Array<Vec<T>> {
    type Value = Vec<T>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} API array elements", self.length)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        for index in 0..self.length {
            values.push(
                sequence
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(index, &self))?,
            );
        }
        Ok(values)
    }
}
impl<'de, T: Deserialize<'de>, const N: usize> DeserializeSeed<'de> for Array<[T; N]> {
    type Value = [T; N];
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_tuple(N, self)
    }
}
impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for Array<[T; N]> {
    type Value = [T; N];
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{N} API array elements")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values: [Option<T>; N] = [const { None }; N];
        for (index, slot) in values.iter_mut().enumerate() {
            *slot = Some(
                sequence
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(index, &self))?,
            );
        }
        Ok(values.map(|value| value.expect("all API array elements initialized")))
    }
}

/// `vl_api_string_t`: length-prefixed arbitrary bytes, without a terminator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct String {
    buf: Vec<u8>,
}
impl String {
    pub fn new(buf: Vec<u8>) -> Result<Self, Error> {
        u32::try_from(buf.len()).map_err(Error::custom)?;
        Ok(Self { buf })
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}
impl Serialize for String {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.buf)
    }
}
impl<'de> Deserialize<'de> for String {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ApiString;
        impl<'de> Visitor<'de> for ApiString {
            type Value = String;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("API string bytes")
            }
            fn visit_byte_buf<E: serde::de::Error>(self, buf: Vec<u8>) -> Result<String, E> {
                Ok(String { buf })
            }
        }
        deserializer.deserialize_byte_buf(ApiString)
    }
}
