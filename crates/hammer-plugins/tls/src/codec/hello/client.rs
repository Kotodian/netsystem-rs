use core::mem::{size_of, transmute};

use super::{ExtensionsLength, HelloError, HelloPrefix, LEGACY_HELLO_VERSION};
use crate::codec::extension::{self, Extension};

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct CipherSuitesLength {
    length: [u8; 2],
}

const _: () = assert!(size_of::<CipherSuitesLength>() == 2);

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct CompressionMethodsLength {
    length: u8,
}

const _: () = assert!(size_of::<CompressionMethodsLength>() == 1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClientHello<'a> {
    pub(crate) random: [u8; 32],
    pub(crate) session_id: &'a [u8],
    pub(crate) cipher_suites: &'a [u8],
    pub(crate) compression_methods: &'a [u8],
    extensions: &'a [u8],
}

impl<'a> ClientHello<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<Self, HelloError> {
        let prefix_bytes =
            input
                .get(..size_of::<HelloPrefix>())
                .ok_or(HelloError::ClientPrefixTruncated {
                    available: input.len(),
                })?;
        // SAFETY: `prefix_bytes` contains a complete `HelloPrefix`. The
        // pointer may be unaligned and is therefore only read unaligned.
        let prefix =
            unsafe { transmute::<_, *const HelloPrefix>(prefix_bytes.as_ptr()).read_unaligned() };
        if prefix.legacy_version != LEGACY_HELLO_VERSION {
            return Err(HelloError::LegacyVersion {
                version: prefix.legacy_version,
            });
        }

        let session_id_start = size_of::<HelloPrefix>();
        let session_id_end = session_id_start
            .checked_add(usize::from(prefix.session_id_length))
            .ok_or(HelloError::ClientSessionIdTruncated {
                declared: usize::from(prefix.session_id_length),
                available: input.len().saturating_sub(session_id_start),
            })?;
        let session_id = input.get(session_id_start..session_id_end).ok_or(
            HelloError::ClientSessionIdTruncated {
                declared: usize::from(prefix.session_id_length),
                available: input.len().saturating_sub(session_id_start),
            },
        )?;

        let cipher_length_end = session_id_end
            .checked_add(size_of::<CipherSuitesLength>())
            .ok_or(HelloError::CipherSuitesLengthTruncated)?;
        let cipher_length_bytes = input
            .get(session_id_end..cipher_length_end)
            .ok_or(HelloError::CipherSuitesLengthTruncated)?;
        // SAFETY: `cipher_length_bytes` contains a complete packed length and
        // is only read unaligned.
        let cipher_length = unsafe {
            transmute::<_, *const CipherSuitesLength>(cipher_length_bytes.as_ptr()).read_unaligned()
        };
        let cipher_suites_length = usize::from(u16::from_be_bytes(cipher_length.length));
        if cipher_suites_length == 0 || cipher_suites_length % 2 != 0 {
            return Err(HelloError::CipherSuitesLength {
                length: cipher_suites_length,
            });
        }
        let cipher_suites_end = cipher_length_end.checked_add(cipher_suites_length).ok_or(
            HelloError::CipherSuitesTruncated {
                declared: cipher_suites_length,
                available: input.len().saturating_sub(cipher_length_end),
            },
        )?;
        let cipher_suites = input.get(cipher_length_end..cipher_suites_end).ok_or(
            HelloError::CipherSuitesTruncated {
                declared: cipher_suites_length,
                available: input.len().saturating_sub(cipher_length_end),
            },
        )?;

        let compression_length_end = cipher_suites_end
            .checked_add(size_of::<CompressionMethodsLength>())
            .ok_or(HelloError::CompressionMethodsLengthTruncated)?;
        let compression_length_bytes = input
            .get(cipher_suites_end..compression_length_end)
            .ok_or(HelloError::CompressionMethodsLengthTruncated)?;
        // SAFETY: `compression_length_bytes` contains a complete packed length
        // and is only read unaligned.
        let compression_length = unsafe {
            transmute::<_, *const CompressionMethodsLength>(compression_length_bytes.as_ptr())
                .read_unaligned()
        };
        let compression_methods_length = usize::from(compression_length.length);
        if compression_methods_length == 0 {
            return Err(HelloError::CompressionMethodsEmpty);
        }
        let compression_methods_end = compression_length_end
            .checked_add(compression_methods_length)
            .ok_or(HelloError::CompressionMethodsTruncated {
                declared: compression_methods_length,
                available: input.len().saturating_sub(compression_length_end),
            })?;
        let compression_methods = input
            .get(compression_length_end..compression_methods_end)
            .ok_or(HelloError::CompressionMethodsTruncated {
                declared: compression_methods_length,
                available: input.len().saturating_sub(compression_length_end),
            })?;
        if compression_methods != [0] {
            return Err(HelloError::CompressionMethods);
        }

        let extensions_length_end = compression_methods_end
            .checked_add(size_of::<ExtensionsLength>())
            .ok_or(HelloError::ExtensionsLengthTruncated)?;
        let extensions_length_bytes = input
            .get(compression_methods_end..extensions_length_end)
            .ok_or(HelloError::ExtensionsLengthTruncated)?;
        // SAFETY: `extensions_length_bytes` contains a complete packed length
        // and is only read unaligned.
        let extensions_length = unsafe {
            transmute::<_, *const ExtensionsLength>(extensions_length_bytes.as_ptr())
                .read_unaligned()
        };
        let extensions_length = usize::from(u16::from_be_bytes(extensions_length.length));
        let extensions_end = extensions_length_end.checked_add(extensions_length).ok_or(
            HelloError::ExtensionsTruncated {
                declared: extensions_length,
                available: input.len().saturating_sub(extensions_length_end),
            },
        )?;
        let extensions = input.get(extensions_length_end..extensions_end).ok_or(
            HelloError::ExtensionsTruncated {
                declared: extensions_length,
                available: input.len().saturating_sub(extensions_length_end),
            },
        )?;
        if extensions_end != input.len() {
            return Err(HelloError::TrailingData {
                trailing: input.len() - extensions_end,
            });
        }
        extension::validate(extensions).map_err(|source| HelloError::Extension { source })?;

        Ok(Self {
            random: prefix.random,
            session_id,
            cipher_suites,
            compression_methods,
            extensions,
        })
    }

    fn encoded_len(self) -> usize {
        size_of::<HelloPrefix>()
            + self.session_id.len()
            + size_of::<CipherSuitesLength>()
            + self.cipher_suites.len()
            + size_of::<CompressionMethodsLength>()
            + self.compression_methods.len()
            + size_of::<ExtensionsLength>()
            + self.extensions.len()
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<usize, HelloError> {
        let encoded_len = self.encoded_len();
        if output.len() < encoded_len {
            return Err(HelloError::OutputTooSmall {
                required: encoded_len,
                available: output.len(),
            });
        }
        let session_id_length =
            u8::try_from(self.session_id.len()).map_err(|_| HelloError::SessionIdLength {
                length: self.session_id.len(),
            })?;
        let cipher_suites_length = u16::try_from(self.cipher_suites.len()).map_err(|_| {
            HelloError::CipherSuitesLength {
                length: self.cipher_suites.len(),
            }
        })?;
        let compression_methods_length =
            u8::try_from(self.compression_methods.len()).map_err(|_| {
                HelloError::CompressionMethodsLength {
                    length: self.compression_methods.len(),
                }
            })?;
        let extensions_length =
            u16::try_from(self.extensions.len()).map_err(|_| HelloError::ExtensionsLength {
                length: self.extensions.len(),
            })?;

        let prefix = HelloPrefix {
            legacy_version: LEGACY_HELLO_VERSION,
            random: self.random,
            session_id_length,
        };
        // SAFETY: the output length check covers the full packed prefix; the
        // pointer is only written unaligned.
        unsafe { transmute::<_, *mut HelloPrefix>(output.as_mut_ptr()).write_unaligned(prefix) };
        let mut offset = size_of::<HelloPrefix>();
        output[offset..offset + self.session_id.len()].copy_from_slice(self.session_id);
        offset += self.session_id.len();

        let cipher_length = CipherSuitesLength {
            length: cipher_suites_length.to_be_bytes(),
        };
        // SAFETY: `encoded_len` covers the complete packed length at `offset`.
        unsafe {
            transmute::<_, *mut CipherSuitesLength>(output.as_mut_ptr().add(offset))
                .write_unaligned(cipher_length)
        };
        offset += size_of::<CipherSuitesLength>();
        output[offset..offset + self.cipher_suites.len()].copy_from_slice(self.cipher_suites);
        offset += self.cipher_suites.len();

        let compression_length = CompressionMethodsLength {
            length: compression_methods_length,
        };
        // SAFETY: `encoded_len` covers the complete packed length at `offset`.
        unsafe {
            transmute::<_, *mut CompressionMethodsLength>(output.as_mut_ptr().add(offset))
                .write_unaligned(compression_length)
        };
        offset += size_of::<CompressionMethodsLength>();
        output[offset..offset + self.compression_methods.len()]
            .copy_from_slice(self.compression_methods);
        offset += self.compression_methods.len();

        let extension_length = ExtensionsLength {
            length: extensions_length.to_be_bytes(),
        };
        // SAFETY: `encoded_len` covers the complete packed length at `offset`.
        unsafe {
            transmute::<_, *mut ExtensionsLength>(output.as_mut_ptr().add(offset))
                .write_unaligned(extension_length)
        };
        offset += size_of::<ExtensionsLength>();
        output[offset..offset + self.extensions.len()].copy_from_slice(self.extensions);
        Ok(encoded_len)
    }

    pub(crate) fn extension<T>(self) -> Result<Option<T>, T::Error>
    where
        T: Extension<'a>,
    {
        extension::find(self.extensions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::RFC8448_CLIENT_HELLO;

    #[test]
    fn rfc8448_client_hello_decodes_without_copying_variable_fields() {
        let hello = ClientHello::decode(RFC8448_CLIENT_HELLO).expect("RFC 8448 ClientHello");

        assert!(hello.session_id.is_empty());
        assert_eq!(hello.cipher_suites, &[0x13, 0x01, 0x13, 0x03, 0x13, 0x02]);
        assert_eq!(
            hello.cipher_suites.as_ptr(),
            RFC8448_CLIENT_HELLO[37..].as_ptr()
        );
        assert_eq!(hello.extensions.len(), 145);
        assert_eq!(hello.random[0..4], [0xcb, 0x34, 0xec, 0xb1]);

        let mut encoded = [0u8; RFC8448_CLIENT_HELLO.len()];
        assert_eq!(hello.encode(&mut encoded), Ok(encoded.len()));
        assert_eq!(encoded, RFC8448_CLIENT_HELLO);
    }
}
