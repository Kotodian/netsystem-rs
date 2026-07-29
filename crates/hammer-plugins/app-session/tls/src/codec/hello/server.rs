use core::mem::{size_of, transmute};

use super::{HelloError, HelloPrefix, LEGACY_HELLO_VERSION};
use crate::codec::extension;

const TLS_AES_128_GCM_SHA256: [u8; 2] = [0x13, 0x01];
const TLS_AES_256_GCM_SHA384: [u8; 2] = [0x13, 0x02];
const TLS_CHACHA20_POLY1305_SHA256: [u8; 2] = [0x13, 0x03];

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ServerHelloSuffix {
    cipher_suite: [u8; 2],
    compression_method: u8,
    extensions_length: [u8; 2],
}

const _: () = assert!(size_of::<ServerHelloSuffix>() == 5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ServerHello<'a> {
    pub(crate) random: [u8; 32],
    pub(crate) session_id: &'a [u8],
    pub(crate) cipher_suite: [u8; 2],
    pub(crate) extensions: &'a [u8],
}

impl<'a> ServerHello<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<Self, HelloError> {
        let prefix_bytes =
            input
                .get(..size_of::<HelloPrefix>())
                .ok_or(HelloError::ServerPrefixTruncated {
                    available: input.len(),
                })?;
        // SAFETY: `prefix_bytes` contains a complete packed `HelloPrefix` and
        // is only read unaligned.
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
            .ok_or(HelloError::ServerSessionIdTruncated {
                declared: usize::from(prefix.session_id_length),
                available: input.len().saturating_sub(session_id_start),
            })?;
        let session_id = input.get(session_id_start..session_id_end).ok_or(
            HelloError::ServerSessionIdTruncated {
                declared: usize::from(prefix.session_id_length),
                available: input.len().saturating_sub(session_id_start),
            },
        )?;

        let suffix_end = session_id_end
            .checked_add(size_of::<ServerHelloSuffix>())
            .ok_or(HelloError::ServerSuffixTruncated)?;
        let suffix_bytes = input
            .get(session_id_end..suffix_end)
            .ok_or(HelloError::ServerSuffixTruncated)?;
        // SAFETY: `suffix_bytes` contains a complete packed
        // `ServerHelloSuffix` and is only read unaligned.
        let suffix = unsafe {
            transmute::<_, *const ServerHelloSuffix>(suffix_bytes.as_ptr()).read_unaligned()
        };
        if !matches!(
            suffix.cipher_suite,
            TLS_AES_128_GCM_SHA256 | TLS_AES_256_GCM_SHA384 | TLS_CHACHA20_POLY1305_SHA256
        ) {
            return Err(HelloError::CipherSuite {
                cipher_suite: suffix.cipher_suite,
            });
        }
        if suffix.compression_method != 0 {
            return Err(HelloError::CompressionMethod {
                compression_method: suffix.compression_method,
            });
        }

        let extensions_length = usize::from(u16::from_be_bytes(suffix.extensions_length));
        let extensions_end =
            suffix_end
                .checked_add(extensions_length)
                .ok_or(HelloError::ExtensionsTruncated {
                    declared: extensions_length,
                    available: input.len().saturating_sub(suffix_end),
                })?;
        let extensions =
            input
                .get(suffix_end..extensions_end)
                .ok_or(HelloError::ExtensionsTruncated {
                    declared: extensions_length,
                    available: input.len().saturating_sub(suffix_end),
                })?;
        if extensions_end != input.len() {
            return Err(HelloError::TrailingData {
                trailing: input.len() - extensions_end,
            });
        }
        extension::validate(extensions).map_err(|source| HelloError::Extension { source })?;

        Ok(Self {
            random: prefix.random,
            session_id,
            cipher_suite: suffix.cipher_suite,
            extensions,
        })
    }

    fn encoded_len(self) -> usize {
        size_of::<HelloPrefix>()
            + self.session_id.len()
            + size_of::<ServerHelloSuffix>()
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
        let extensions_length =
            u16::try_from(self.extensions.len()).map_err(|_| HelloError::ExtensionsLength {
                length: self.extensions.len(),
            })?;
        let prefix = HelloPrefix {
            legacy_version: LEGACY_HELLO_VERSION,
            random: self.random,
            session_id_length,
        };
        // SAFETY: the output length check covers the complete packed prefix;
        // the pointer is only written unaligned.
        unsafe { transmute::<_, *mut HelloPrefix>(output.as_mut_ptr()).write_unaligned(prefix) };

        let mut offset = size_of::<HelloPrefix>();
        output[offset..offset + self.session_id.len()].copy_from_slice(self.session_id);
        offset += self.session_id.len();

        let suffix = ServerHelloSuffix {
            cipher_suite: self.cipher_suite,
            compression_method: 0,
            extensions_length: extensions_length.to_be_bytes(),
        };
        // SAFETY: `encoded_len` covers the complete packed suffix at `offset`;
        // the pointer is only written unaligned.
        unsafe {
            transmute::<_, *mut ServerHelloSuffix>(output.as_mut_ptr().add(offset))
                .write_unaligned(suffix)
        };
        offset += size_of::<ServerHelloSuffix>();
        output[offset..offset + self.extensions.len()].copy_from_slice(self.extensions);
        Ok(encoded_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::RFC8448_SERVER_HELLO;

    #[test]
    fn rfc8448_server_hello_decodes_without_copying_variable_fields() {
        let hello = ServerHello::decode(RFC8448_SERVER_HELLO).expect("RFC 8448 ServerHello");

        assert!(hello.session_id.is_empty());
        assert_eq!(u16::from_be_bytes(hello.cipher_suite), 0x1301);
        assert_eq!(hello.extensions.len(), 46);
        assert_eq!(hello.random[0..4], [0xa6, 0xaf, 0x06, 0xa4]);

        let mut encoded = [0u8; RFC8448_SERVER_HELLO.len()];
        assert_eq!(hello.encode(&mut encoded), Ok(encoded.len()));
        assert_eq!(encoded, RFC8448_SERVER_HELLO);
    }

    #[test]
    fn extension_body_truncation_is_typed() {
        let mut truncated = RFC8448_SERVER_HELLO.to_vec();
        truncated.pop();

        assert_eq!(
            ServerHello::decode(&truncated),
            Err(HelloError::ExtensionsTruncated {
                declared: 46,
                available: 45,
            })
        );
    }
}
