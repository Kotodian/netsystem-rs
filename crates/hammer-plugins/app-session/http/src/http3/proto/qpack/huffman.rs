//! QPACK Huffman coding (RFC 9204 Section 4.2; RFC 7541 Appendix B table).
//!
//! Ported verbatim from `third_party/h3/h3/src/qpack/prefix_string/{bitwin,encode,decode}.rs`:
//! the encode bit-writer and `HPACK_STRING` table, the decode trie
//! (`bits_decode!`), and `BitWindow` are the vendored h3 implementation and
//! data. Hammer differences: visibility is `pub(crate)`, the two h3 `Error`
//! types are renamed `HuffmanDecodingError`/`HuffmanEncodingError`, and no
//! code paths panic.
//!
//! Decode failure sites: `MissingBits` covers input ending mid-code, with
//! trailing bits that are not all-ones (EOS-prefix) padding, or with an
//! all-ones run longer than 7 bits (a truncated or complete EOS symbol);
//! `Unhandled` fires on the all-ones (EOS) path whose remaining bits
//! resolve to no symbol.

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) struct BitWindow {
    pub byte: u32,
    pub bit: u32,
    pub count: u32,
}

impl BitWindow {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn forwards(&mut self, step: u32) {
        self.bit += self.count;

        self.byte += self.bit / 8;
        self.bit %= 8;

        self.count = step;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HuffmanEncodingError {
    buffer_pos: BitWindow,
    len: usize,
    capacity: usize,
    text: String,
}

#[derive(Clone, Debug)]
struct EncodeValue {
    buffer: &'static [u8],
    bit_count: u32,
}

#[derive(Clone, Debug)]
struct HuffmanEncoder {
    buffer_pos: BitWindow,
    buffer: Vec<u8>,
}

impl HuffmanEncoder {
    fn new() -> HuffmanEncoder {
        HuffmanEncoder {
            buffer_pos: BitWindow::new(),
            buffer: Vec::new(),
        }
    }

    fn ensure_free_space(&mut self, bit_count: u32) {
        let mut end_range = self.buffer_pos.clone();
        end_range.forwards(bit_count);
        end_range.forwards(0);

        // buffer still has enough space to work on
        if self.buffer.len() > end_range.byte as usize {
            return;
        }

        // optimisation to grow capacity before pushing data
        if self.buffer.capacity() <= end_range.byte as usize {
            self.buffer.reserve(((7 * end_range.byte) / 4) as usize);
        }

        let forward =
            end_range.byte as usize - self.buffer.len() + if end_range.bit > 0 { 1 } else { 0 };
        for _ in 0..forward {
            // push filler value that will end huffman decoding if not
            // modified
            self.buffer.push(255);
        }
    }

    fn put(&mut self, code: u8) -> Result<(), HuffmanEncodingError> {
        let encode_value = &HPACK_STRING[code as usize];

        self.ensure_free_space(encode_value.bit_count);

        let mut rest = encode_value.bit_count;
        for i in 0..encode_value.buffer.len() {
            let part = encode_value.buffer[i];

            self.buffer_pos.forwards(if rest < 8 { rest } else { 8 });
            rest -= self.buffer_pos.count;

            write_bits(&mut self.buffer, &self.buffer_pos, part)
        }

        Ok(())
    }

    fn ends(self) -> Result<Vec<u8>, HuffmanEncodingError> {
        Ok(self.buffer)
    }
}

/// Write bits from `value` to the `out` slice
///
/// Write the least significant `pos.count` bits from `value` to the position specified by
/// `(pos.byte, pos.bit)`. Writes may span multiple bytes. `out` is expected to be long enough
/// to write these bits; this is ensured by `HuffmanEncoder::ensure_free_space()`, which is
/// always called prior to calling this function.
///
/// The bits to be written to are expected to be set to 1 when calling this function. Similarly,
/// this function maintains the invariant that unused bits in the output bytes are set to 1.
fn write_bits(out: &mut [u8], pos: &BitWindow, value: u8) {
    debug_assert!(pos.bit < 8);
    debug_assert!(pos.count <= 8);
    debug_assert!(pos.count > 0);

    if (pos.bit + pos.count) <= 8 {
        // Bits to be written to fit in a single byte
        debug_assert_eq!(out[pos.byte as usize] | PAD_LEFT[pos.bit as usize], 255);
        let pad_left = out[pos.byte as usize] | PAD_RIGHT[(8 - pos.bit) as usize];
        let shifted = value << (8 - pos.bit - pos.count) | PAD_LEFT[pos.bit as usize];
        let pad_right = PAD_RIGHT[(8 - pos.count - pos.bit) as usize];
        out[pos.byte as usize] = (pad_left & shifted) | pad_right;
    } else {
        // Bits to be written to span two bytes
        debug_assert_eq!(out[pos.byte as usize] | PAD_LEFT[pos.bit as usize], 255);
        let split = 8 - pos.bit;
        let pad_left = out[pos.byte as usize] | PAD_RIGHT[split as usize];
        let shifted = (value >> (pos.count - split)) | PAD_LEFT[pos.bit as usize];
        out[pos.byte as usize] = pad_left & shifted;

        let rem = 8 - (pos.count - split);
        out[(pos.byte + 1) as usize] = (value << rem) | PAD_RIGHT[rem as usize];
    }
}

const PAD_RIGHT: [u8; 9] = [0, 1, 3, 7, 15, 31, 63, 127, 255];
const PAD_LEFT: [u8; 9] = [0, 128, 192, 224, 240, 248, 252, 254, 255];

macro_rules! bits_encode {
    [ $( ( $len:expr => [ $( $byte:expr ),* ] ), )* ] => {
        [ $(
            EncodeValue{
                buffer: &[ $( $byte as u8 ),* ],
                bit_count: $len
            } ,
        )* ]
    }
}

const HPACK_STRING: [EncodeValue; 256] = bits_encode![
    ( 13 => [0b1111_1111, 0b0001_1000]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1000]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0010]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0011]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0100]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0101]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0110]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0111]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1000]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1010]),
    ( 30 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0011_1100]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1001]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1010]),
    ( 30 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0011_1101]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1011]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1100]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1101]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1110]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_1111]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0000]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0001]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0010]),
    ( 30 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0011_1110]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0011]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0100]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0101]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0110]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_0111]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1000]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1001]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1010]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1011]),
    (  6 => [0b0001_0100]),
    ( 10 => [0b1111_1110, 0b0000_0000]), // '!'
    ( 10 => [0b1111_1110, 0b0000_0001]), // ';'
    ( 12 => [0b1111_1111, 0b0000_1010]), // '#'
    ( 13 => [0b1111_1111, 0b0001_1001]), // '$'
    (  6 => [0b0001_0101]), // '%'
    (  8 => [0b1111_1000]), // '&'
    ( 11 => [0b1111_1111, 0b0000_0010]), // '''
    ( 10 => [0b1111_1110, 0b0000_0010]), // '('
    ( 10 => [0b1111_1110, 0b0000_0011]), // ')'
    (  8 => [0b1111_1001]), // '*'
    ( 11 => [0b1111_1111, 0b0000_0011]), // '+'
    (  8 => [0b1111_1010]), // ','
    (  6 => [0b0001_0110]), // '-'
    (  6 => [0b0001_0111]), // '.'
    (  6 => [0b0001_1000]), // '/'
    (  5 => [0b0000_0000]), // '0'
    (  5 => [0b0000_0001]), // '1'
    (  5 => [0b0000_0010]), // '2'
    (  6 => [0b0001_1001]), // '3'
    (  6 => [0b0001_1010]), // '4'
    (  6 => [0b0001_1011]), // '5'
    (  6 => [0b0001_1100]), // '6'
    (  6 => [0b0001_1101]), // '7'
    (  6 => [0b0001_1110]), // '8'
    (  6 => [0b0001_1111]), // '9'
    (  7 => [0b0101_1100]), // ':'
    (  8 => [0b1111_1011]),
    ( 15 => [0b1111_1111, 0b0111_1100]), // '<'
    (  6 => [0b0010_0000]), // '='
    ( 12 => [0b1111_1111, 0b0000_1011]), // '>'
    ( 10 => [0b1111_1111, 0b0000_0000]), // '?'
    ( 13 => [0b1111_1111, 0b0001_1010]), // '@'
    (  6 => [0b0010_0001]), // 'A'
    (  7 => [0b0101_1101]), // 'B'
    (  7 => [0b0101_1110]), // 'C'
    (  7 => [0b0101_1111]), // 'D'
    (  7 => [0b0110_0000]), // 'E'
    (  7 => [0b0110_0001]), // 'F'
    (  7 => [0b0110_0010]), // 'G'
    (  7 => [0b0110_0011]), // 'H'
    (  7 => [0b0110_0100]), // 'I'
    (  7 => [0b0110_0101]), // 'J'
    (  7 => [0b0110_0110]), // 'K'
    (  7 => [0b0110_0111]), // 'L'
    (  7 => [0b0110_1000]), // 'M'
    (  7 => [0b0110_1001]), // 'N'
    (  7 => [0b0110_1010]), // 'O'
    (  7 => [0b0110_1011]), // 'P'
    (  7 => [0b0110_1100]), // 'Q'
    (  7 => [0b0110_1101]), // 'R'
    (  7 => [0b0110_1110]), // 'S'
    (  7 => [0b0110_1111]), // 'T'
    (  7 => [0b0111_0000]), // 'U'
    (  7 => [0b0111_0001]), // 'V'
    (  7 => [0b0111_0010]), // 'W'
    (  8 => [0b1111_1100]), // 'X'
    (  7 => [0b0111_0011]), // 'Y'
    (  8 => [0b1111_1101]), // 'Z'
    ( 13 => [0b1111_1111, 0b0001_1011]), // '['
    ( 19 => [0b1111_1111, 0b1111_1110, 0b0000_0000]), // '\'
    ( 13 => [0b1111_1111, 0b0001_1100]), // ']'
    ( 14 => [0b1111_1111, 0b0011_1100]), // '^'
    (  6 => [0b0010_0010]), // '_'
    ( 15 => [0b1111_1111, 0b0111_1101]), // '`'
    (  5 => [0b0000_0011]), // 'a'
    (  6 => [0b0010_0011]), // 'b'
    (  5 => [0b0000_0100]), // 'c'
    (  6 => [0b0010_0100]), // 'd'
    (  5 => [0b0000_0101]), // 'e'
    (  6 => [0b0010_0101]), // 'f'
    (  6 => [0b0010_0110]), // 'g'
    (  6 => [0b0010_0111]), // 'h'
    (  5 => [0b0000_0110]), // 'i'
    (  7 => [0b0111_0100]), // 'j'
    (  7 => [0b0111_0101]), // 'k'
    (  6 => [0b0010_1000]), // 'l'
    (  6 => [0b0010_1001]), // 'm'
    (  6 => [0b0010_1010]), // 'n'
    (  5 => [0b0000_0111]), // 'o'
    (  6 => [0b0010_1011]), // 'p'
    (  7 => [0b0111_0110]), // 'q'
    (  6 => [0b0010_1100]), // 'r'
    (  5 => [0b0000_1000]), // 's'
    (  5 => [0b0000_1001]), // 't'
    (  6 => [0b0010_1101]), // 'u'
    (  7 => [0b0111_0111]), // 'v'
    (  7 => [0b0111_1000]), // 'w'
    (  7 => [0b0111_1001]), // 'x'
    (  7 => [0b0111_1010]), // 'y'
    (  7 => [0b0111_1011]), // 'z'
    ( 15 => [0b1111_1111, 0b0111_1110]), // '{'
    ( 11 => [0b1111_1111, 0b0000_0100]), // '|'
    ( 14 => [0b1111_1111, 0b0011_1101]), // '}'
    ( 13 => [0b1111_1111, 0b0001_1101]), // '~'
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1100]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_0110]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0010]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_0111]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1000]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0011]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0100]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0101]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1001]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1010]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1011]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1100]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1101]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1110]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1011]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0101_1111]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1100]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1101]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_0111]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0000]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0001]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0010]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0011]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0100]),
    ( 21 => [0b1111_1111, 0b1111_1110, 0b0001_1100]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1000]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0101]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1001]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_0111]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1110_1111]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1010]),
    ( 21 => [0b1111_1111, 0b1111_1110, 0b0001_1101]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1001]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1011]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1100]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1000]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1001]),
    ( 21 => [0b1111_1111, 0b1111_1110, 0b0001_1110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1010]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1101]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1110]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0000]),
    ( 21 => [0b1111_1111, 0b1111_1110, 0b0001_1111]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0001_1111]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1011]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1100]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0000]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0001]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0000]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0010]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1101]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0001]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0110_1111]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1010]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0010]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0011]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0100]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0111_0000]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0101]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0110]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0111_0001]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1000, 0b0000_0000]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1000, 0b0000_0001]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1011]),
    ( 19 => [0b1111_1111, 0b1111_1110, 0b0000_0001]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_0111]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0111_0010]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_1000]),
    ( 25 => [0b1111_1111, 0b1111_1111, 0b1111_0110, 0b0000_0000]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1000, 0b0000_0010]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1000, 0b0000_0011]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1001, 0b0000_0000]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1011, 0b0000_0110]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1011, 0b0000_0111]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1001, 0b0000_0001]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0001]),
    ( 25 => [0b1111_1111, 0b1111_1111, 0b1111_0110, 0b0000_0001]),
    ( 19 => [0b1111_1111, 0b1111_1110, 0b0000_0010]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0011]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1001, 0b0000_0010]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0000]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0001]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1001, 0b0000_0011]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0010]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0010]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0100]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0101]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1010, 0b0000_0000]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1010, 0b0000_0001]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1101]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0011]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0100]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0101]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1100]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0011]),
    ( 20 => [0b1111_1111, 0b1111_1110, 0b0000_1101]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0110]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_1001]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_0111]),
    ( 21 => [0b1111_1111, 0b1111_1111, 0b0000_1000]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0111_0011]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_1010]),
    ( 22 => [0b1111_1111, 0b1111_1111, 0b0010_1011]),
    ( 25 => [0b1111_1111, 0b1111_1111, 0b1111_0111, 0b0000_0000]),
    ( 25 => [0b1111_1111, 0b1111_1111, 0b1111_0111, 0b0000_0001]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0100]),
    ( 24 => [0b1111_1111, 0b1111_1111, 0b1111_0101]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1010, 0b0000_0010]),
    ( 23 => [0b1111_1111, 0b1111_1111, 0b0111_0100]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1010, 0b0000_0011]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0110]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1011, 0b0000_0000]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1011, 0b0000_0001]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1100, 0b0000_0111]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0000]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0001]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0010]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0011]),
    ( 28 => [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b0000_1110]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0100]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0101]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0110]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1101, 0b0000_0111]),
    ( 27 => [0b1111_1111, 0b1111_1111, 0b1111_1110, 0b0000_0000]),
    ( 26 => [0b1111_1111, 0b1111_1111, 0b1111_1011, 0b0000_0010]),
];

pub(crate) trait HpackStringEncode {
    fn hpack_encode(&self) -> Result<Vec<u8>, HuffmanEncodingError>;
}

impl HpackStringEncode for Vec<u8> {
    fn hpack_encode(&self) -> Result<Vec<u8>, HuffmanEncodingError> {
        let mut encoder = HuffmanEncoder::new();
        for code in self {
            encoder.put(*code)?;
        }
        encoder.ends()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HuffmanDecodingError {
    MissingBits(BitWindow),
    Unhandled(BitWindow, usize),
}

#[derive(Clone, Debug)]
enum DecodeValue {
    Partial(&'static HuffmanDecoder),
    Sym(u8),
}

#[derive(Clone, Debug)]
struct HuffmanDecoder {
    lookup: u32,
    table: &'static [DecodeValue],
}

impl HuffmanDecoder {
    fn check_eof(
        &self,
        bit_pos: &mut BitWindow,
        input: &[u8],
        last_symbol_end: u32,
    ) -> Result<Option<u32>, HuffmanDecodingError> {
        // EOS-prefix padding is at most 7 one-bits (RFC 7541 Section 5.2,
        // RFC 9204 Section 4.2). `last_symbol_end` is the bit position right
        // after the last decoded symbol; every bit from there to the end of
        // the input must be all-ones padding. Validate the whole trailing
        // range: a run longer than 7 bits is a truncated or complete EOS
        // symbol, and any non-one bit is malformed padding.
        let padding_len = (input.len() as u32) * 8 - last_symbol_end;
        if padding_len > 7 {
            return Err(HuffmanDecodingError::MissingBits(bit_pos.clone()));
        }
        if padding_len == 0 {
            // Input ends exactly at a symbol boundary.
            return Ok(None);
        }
        let rest = match read_bits(input, last_symbol_end / 8, last_symbol_end % 8, padding_len) {
            Ok(x) => x,
            Err(()) => return Err(HuffmanDecodingError::MissingBits(bit_pos.clone())),
        };
        let eof_filler = ((2u16 << (padding_len - 1)) - 1) as u8;
        if rest & eof_filler == eof_filler {
            return Ok(None);
        }
        Err(HuffmanDecodingError::MissingBits(bit_pos.clone()))
    }

    fn fetch_value(
        &self,
        bit_pos: &mut BitWindow,
        input: &[u8],
        last_symbol_end: u32,
    ) -> Result<Option<u32>, HuffmanDecodingError> {
        match read_bits(input, bit_pos.byte, bit_pos.bit, bit_pos.count) {
            Ok(value) => Ok(Some(value as u32)),
            Err(()) => self.check_eof(bit_pos, input, last_symbol_end),
        }
    }

    fn decode_next(
        &self,
        bit_pos: &mut BitWindow,
        input: &[u8],
        last_symbol_end: u32,
    ) -> Result<Option<u8>, HuffmanDecodingError> {
        bit_pos.forwards(self.lookup);

        let value = match self.fetch_value(bit_pos, input, last_symbol_end) {
            Ok(Some(value)) => value as usize,
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let at_value = match (self.table).get(value) {
            Some(x) => x,
            None => return Err(HuffmanDecodingError::Unhandled(bit_pos.clone(), value)),
        };

        match at_value {
            DecodeValue::Sym(x) => Ok(Some(*x)),
            DecodeValue::Partial(d) => d.decode_next(bit_pos, input, last_symbol_end),
        }
    }
}

/// Read `len` bits from the `src` slice at the specified position
///
/// Never read more than 8 bits at a time. `bit_offset` may be larger than 8.
fn read_bits(src: &[u8], mut byte_offset: u32, mut bit_offset: u32, len: u32) -> Result<u8, ()> {
    if len == 0 || len > 8 || src.len() as u32 * 8 < (byte_offset * 8) + bit_offset + len {
        return Err(());
    }

    // Deal with `bit_offset` > 8
    byte_offset += bit_offset / 8;
    bit_offset -= (bit_offset / 8) * 8;

    Ok(if bit_offset + len <= 8 {
        // Read all the bits from a single byte
        (src[byte_offset as usize] << bit_offset) >> (8 - len)
    } else {
        // The range of bits spans over 2 bytes
        let mut result = (src[byte_offset as usize] as u16) << 8;
        result |= src[byte_offset as usize + 1] as u16;
        ((result << bit_offset) >> (16 - len)) as u8
    })
}

macro_rules! bits_decode {
    // general way
    (
        lookup: $count:expr, [
        $($sym:expr,)*
        $(=> $sub:ident,)* ]
    ) => {
        HuffmanDecoder {
            lookup: $count,
            table: &[
                $( DecodeValue::Sym($sym as u8), )*
                $( DecodeValue::Partial(&$sub), )*
            ]
        }
    };
    // 2-final
    ( $first:expr, $second:expr ) => {
        HuffmanDecoder {
            lookup: 1,
            table: &[
                DecodeValue::Sym($first as u8),
                DecodeValue::Sym($second as u8),
            ]
        }
    };
    // 4-final
    ( $first:expr, $second:expr, $third:expr, $fourth:expr ) => {
        HuffmanDecoder {
            lookup: 2,
            table: &[
                DecodeValue::Sym($first as u8),
                DecodeValue::Sym($second as u8),
                DecodeValue::Sym($third as u8),
                DecodeValue::Sym($fourth as u8),
            ]
        }
    };
    // 2-final-partial
    ( $first:expr, => $second:ident ) => {
        HuffmanDecoder {
            lookup: 1,
            table: &[
                DecodeValue::Sym($first as u8),
                DecodeValue::Partial(&$second),
            ]
        }
    };
    // 2-partial
    ( => $first:ident, => $second:ident ) => {
        HuffmanDecoder {
            lookup: 1,
            table: &[
                DecodeValue::Partial(&$first),
                DecodeValue::Partial(&$second),
            ]
        }
    };
    // 4-partial
    ( => $first:ident, => $second:ident,
      => $third:ident, => $fourth:ident ) => {
        HuffmanDecoder {
            lookup: 2,
            table: &[
                DecodeValue::Partial(&$first),
                DecodeValue::Partial(&$second),
                DecodeValue::Partial(&$third),
                DecodeValue::Partial(&$fourth),
            ]
        }
    };
    [ $( $name:ident => ( $($value:tt)* ), )* ] => {
        $( const $name: HuffmanDecoder = bits_decode!( $( $value )* ); )*
    };
}

#[rustfmt::skip]
bits_decode![
    HPACK_DECODE => (
        lookup: 5, [ '0', '1', '2', 'a', 'c', 'e', 'i', 'o', 's', 't',
        => END0_01010, => END0_01011, => END0_01100, => END0_01101,
        => END0_01110, => END0_01111, => END0_10000, => END0_10001,
        => END0_10010, => END0_10011, => END0_10100, => END0_10101,
        => END0_10110, => END0_10111, => END0_11000, => END0_11001,
        => END0_11010, => END0_11011, => END0_11100, => END0_11101,
        => END0_11110, => END0_11111,
        ]),
    END0_01010 => ( 32, '%'),
    END0_01011 => ('-', '.'),
    END0_01100 => ('/', '3'),
    END0_01101 => ('4', '5'),
    END0_01110 => ('6', '7'),
    END0_01111 => ('8', '9'),
    END0_10000 => ('=', 'A'),
    END0_10001 => ('_', 'b'),
    END0_10010 => ('d', 'f'),
    END0_10011 => ('g', 'h'),
    END0_10100 => ('l', 'm'),
    END0_10101 => ('n', 'p'),
    END0_10110 => ('r', 'u'),
    END0_10111 => (':', 'B', 'C', 'D'),
    END0_11000 => ('E', 'F', 'G', 'H'),
    END0_11001 => ('I', 'J', 'K', 'L'),
    END0_11010 => ('M', 'N', 'O', 'P'),
    END0_11011 => ('Q', 'R', 'S', 'T'),
    END0_11100 => ('U', 'V', 'W', 'Y'),
    END0_11101 => ('j', 'k', 'q', 'v'),
    END0_11110 => ('w', 'x', 'y', 'z'),
    END0_11111 => (=> END5_00, => END5_01, => END5_10, => END5_11),
    END5_00 => ('&', '*'),
    END5_01 => (',', 59),
    END5_10 => ('X', 'Z'),
    END5_11 => (=> END7_0, => END7_1),
    END7_0 => ('!', '"', '(', ')'),
    END7_1 => (=> END8_0, => END8_1),
    END8_0 => ('?', => END9A_1),
    END9A_1 => ('\'', '+'),
    END8_1 => (lookup: 2, ['|', => END9B_01, => END9B_10, => END9B_11,]),
    END9B_01 => ('#', '>'),
    END9B_10 => (0, '$', '@', '['),
    END9B_11 => (lookup: 2, [']', '~', => END13_10, => END13_11,]),
    END13_10 => ('^', '}'),
    END13_11 => (=> END14_0, => END14_1),
    END14_0 => ('<', '`'),
    END14_1 => ('{', => END15_1),
    END15_1 =>
    (lookup: 4, [ '\\', 195, 208, => END19_0011,
     => END19_0100, => END19_0101, => END19_0110, => END19_0111,
     => END19_1000, => END19_1001, => END19_1010, => END19_1011,
     => END19_1100, => END19_1101, => END19_1110, => END19_1111,
    ]),
    END19_0011 => (128, 130),
    END19_0100 => (131, 162),
    END19_0101 => (184, 194),
    END19_0110 => (224, 226),
    END19_0111 => (153, 161, 167, 172),
    END19_1000 => (176, 177, 179, 209),
    END19_1001 => (216, 217, 227, 229),
    END19_1010 => (lookup: 2, [230, => END19_1010_01, => END19_1010_10,
                   => END19_1010_11,]),
    END19_1010_01 => (129, 132),
    END19_1010_10 => (133, 134),
    END19_1010_11 => (136, 146),
    END19_1011 => (lookup: 3, [154, 156, 160, 163, 164, 169, 170, 173,]),
    END19_1100 => (lookup: 3, [178, 181, 185, 186, 187, 189, 190, 196,]),
    END19_1101 => (lookup: 3, [198, 228, 232, 233,
                   => END23A_100, => END23A_101,
                   => END23A_110, => END23A_111,]),
    END23A_100 => (  1, 135),
    END23A_101 => (137, 138),
    END23A_110 => (139, 140),
    END23A_111 => (141, 143),
    END19_1110 => (lookup: 4, [147, 149, 150, 151, 152, 155, 157, 158,
                   165, 166, 168, 174, 175, 180, 182, 183,]),
    END19_1111 => (lookup: 4, [188, 191, 197, 231, 239,
                   => END23B_0101, => END23B_0110, => END23B_0111,
                   => END23B_1000, => END23B_1001, => END23B_1010,
                   => END23B_1011, => END23B_1100, => END23B_1101,
                   => END23B_1110, => END23B_1111,]),
    END23B_0101 => (  9, 142),
    END23B_0110 => (144, 145),
    END23B_0111 => (148, 159),
    END23B_1000 => (171, 206),
    END23B_1001 => (215, 225),
    END23B_1010 => (236, 237),
    END23B_1011 => (199, 207, 234, 235),
    END23B_1100 => (lookup: 3, [192, 193, 200, 201, 202, 205, 210, 213,]),
    END23B_1101 => (lookup: 3, [218, 219, 238, 240, 242, 243, 255,
                    => END27A_111,]),
    END27A_111 => (203, 204),
    END23B_1110 => (lookup: 4, [211, 212, 214, 221, 222, 223, 241, 244,
                    245, 246, 247, 248, 250, 251, 252, 253,]),
    END23B_1111 => (lookup: 4, [ 254, => END27B_0001, => END27B_0010,
                    => END27B_0011, => END27B_0100, => END27B_0101,
                    => END27B_0110, => END27B_0111, => END27B_1000,
                    => END27B_1001, => END27B_1010, => END27B_1011,
                    => END27B_1100, => END27B_1101, => END27B_1110,
                    => END27B_1111,]),
    END27B_0001 => (2, 3),
    END27B_0010 => (4, 5),
    END27B_0011 => (6, 7),
    END27B_0100 => (8, 11),
    END27B_0101 => (12, 14),
    END27B_0110 => (15, 16),
    END27B_0111 => (17, 18),
    END27B_1000 => (19, 20),
    END27B_1001 => (21, 23),
    END27B_1010 => (24, 25),
    END27B_1011 => (26, 27),
    END27B_1100 => (28, 29),
    END27B_1101 => (30, 31),
    END27B_1110 => (127, 220),
    END27B_1111 => (lookup: 1, [249, => END31_1,]),
    END31_1 => (lookup: 2, [10, 13, 22, => EOF,]),
    EOF => (lookup: 8, []),
    ];

pub(crate) struct DecodeIter<'a> {
    bit_pos: BitWindow,
    /// Bit position right after the last decoded symbol; EOS-prefix padding
    /// is measured from here to the end of the input.
    last_symbol_end: u32,
    content: &'a Vec<u8>,
}

impl<'a> Iterator for DecodeIter<'a> {
    type Item = Result<u8, HuffmanDecodingError>;

    fn next(&mut self) -> Option<Self::Item> {
        match HPACK_DECODE.decode_next(&mut self.bit_pos, self.content, self.last_symbol_end) {
            Ok(Some(x)) => {
                // `bit_pos` points at the start of the window that held the
                // symbol; its `count` is that window's size, so the sum is
                // the bit position right after the decoded symbol.
                self.last_symbol_end =
                    self.bit_pos.byte * 8 + self.bit_pos.bit + self.bit_pos.count;
                Some(Ok(x))
            }
            Err(err) => Some(Err(err)),
            Ok(None) => None,
        }
    }
}

pub(crate) trait HpackStringDecode {
    fn hpack_decode(&self) -> DecodeIter<'_>;
}

impl HpackStringDecode for Vec<u8> {
    fn hpack_decode(&self) -> DecodeIter<'_> {
        DecodeIter {
            bit_pos: BitWindow::new(),
            last_symbol_end: 0,
            content: self,
        }
    }
}
