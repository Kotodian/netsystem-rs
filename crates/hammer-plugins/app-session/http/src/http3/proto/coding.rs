//! Encode/decode helper traits on `bytes::Buf`/`BufMut`.
//!
//! Adapted from `third_party/h3/h3/src/proto/coding.rs`. Unlike h3, `write_var`
//! takes an already-validated `VarInt` so encoding never panics.

use bytes::{Buf, BufMut};

use super::varint::{UnexpectedEnd, VarInt};

/// Trait for encoding helpers on basic types; returns `UnexpectedEnd` instead
/// of panicking as the `Buf` impls do when there are not enough bytes.
pub trait Encode {
    fn encode<B: BufMut>(&self, buf: &mut B);
}

pub trait Decode: Sized {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, UnexpectedEnd>;
}

impl Encode for u8 {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(*self);
    }
}

impl Decode for u8 {
    fn decode<B: Buf>(buf: &mut B) -> Result<u8, UnexpectedEnd> {
        if buf.remaining() < 1 {
            return Err(UnexpectedEnd(1));
        }
        Ok(buf.get_u8())
    }
}

pub trait BufExt {
    fn get<T: Decode>(&mut self) -> Result<T, UnexpectedEnd>;
    fn get_var(&mut self) -> Result<u64, UnexpectedEnd>;
}

impl<T: Buf> BufExt for T {
    fn get<U: Decode>(&mut self) -> Result<U, UnexpectedEnd> {
        U::decode(self)
    }

    fn get_var(&mut self) -> Result<u64, UnexpectedEnd> {
        Ok(VarInt::decode(self)?.into_inner())
    }
}

pub trait BufMutExt {
    fn write<T: Encode>(&mut self, x: T);
    fn write_var(&mut self, x: VarInt);
}

impl<T: BufMut> BufMutExt for T {
    fn write<U: Encode>(&mut self, x: U) {
        x.encode(self);
    }

    fn write_var(&mut self, x: VarInt) {
        x.encode(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_u8_needs_a_byte() {
        let mut empty: &[u8] = &[];
        assert_eq!(BufExt::get::<u8>(&mut empty), Err(UnexpectedEnd(1)));
        let mut one: &[u8] = &[0x2a];
        assert_eq!(BufExt::get::<u8>(&mut one), Ok(0x2a));
        assert!(!one.has_remaining());
    }

    #[test]
    fn encode_u8() {
        let mut out = Vec::new();
        out.write(0x2au8);
        assert_eq!(out, vec![0x2a]);
    }

    #[test]
    fn get_var_and_write_var() {
        let mut out = Vec::new();
        out.write_var(VarInt::from_u32(64));
        assert_eq!(out, vec![0x40, 0x40]);

        let mut read: &[u8] = &out;
        assert_eq!(read.get_var(), Ok(64));
    }
}
