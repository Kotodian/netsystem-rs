use thiserror::Error;

const TLS13_NONCE_LEN: usize = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordSequence {
    value: u64,
    exhausted: bool,
}

impl RecordSequence {
    pub(crate) const fn new() -> Self {
        Self {
            value: 0,
            exhausted: false,
        }
    }

    pub(crate) fn nonce(self, iv: [u8; TLS13_NONCE_LEN]) -> Result<[u8; 12], SequenceError> {
        if self.exhausted {
            return Err(SequenceError::Exhausted);
        }
        let mut nonce = iv;
        for (nonce_byte, sequence_byte) in nonce[4..].iter_mut().zip(self.value.to_be_bytes()) {
            *nonce_byte ^= sequence_byte;
        }
        Ok(nonce)
    }

    pub(crate) fn advance(&mut self) -> Result<(), SequenceError> {
        if self.exhausted {
            return Err(SequenceError::Exhausted);
        }
        match self.value.checked_add(1) {
            Some(next) => self.value = next,
            None => self.exhausted = true,
        }
        Ok(())
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::new();
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SequenceError {
    #[error("TLS record sequence is exhausted for the active traffic key")]
    Exhausted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_xors_big_endian_sequence_into_iv_tail() {
        let mut sequence = RecordSequence::new();
        let iv = [0xa5; 12];

        assert_eq!(sequence.nonce(iv), Ok(iv));
        sequence.advance().expect("advance sequence");
        assert_eq!(
            sequence.nonce(iv),
            Ok([
                0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa4
            ])
        );
    }

    #[test]
    fn final_sequence_is_usable_once_then_exhausted() {
        let mut sequence = RecordSequence {
            value: u64::MAX,
            exhausted: false,
        };

        assert!(sequence.nonce([0; 12]).is_ok());
        assert_eq!(sequence.advance(), Ok(()));
        assert_eq!(sequence.nonce([0; 12]), Err(SequenceError::Exhausted));
        assert_eq!(sequence.advance(), Err(SequenceError::Exhausted));

        sequence.reset();
        assert_eq!(sequence.nonce([0; 12]), Ok([0; 12]));
    }
}
