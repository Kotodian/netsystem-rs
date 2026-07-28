//! `supported_versions` bodies for ClientHello and ServerHello.

use thiserror::Error;

use super::Extension;

pub(crate) const TLS_1_3: [u8; 2] = [0x03, 0x04];
const SUPPORTED_VERSIONS: u16 = 43;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SupportedVersions<'a> {
    versions: &'a [u8],
}

impl SupportedVersions<'_> {
    pub(crate) fn contains(self, version: [u8; 2]) -> bool {
        self.versions
            .chunks_exact(2)
            .any(|candidate| candidate == version)
    }
}

impl<'a> Extension<'a> for SupportedVersions<'a> {
    type Error = SupportedVersionsError;

    const TYPE: u16 = SUPPORTED_VERSIONS;

    fn decode(body: &'a [u8]) -> Result<Self, Self::Error> {
        let (&declared, versions) = body
            .split_first()
            .ok_or(SupportedVersionsError::LengthTruncated)?;
        let declared = usize::from(declared);
        if declared == 0 || declared % 2 != 0 {
            return Err(SupportedVersionsError::Length { length: declared });
        }
        if declared != versions.len() {
            return Err(SupportedVersionsError::BodyLength {
                declared,
                available: versions.len(),
            });
        }
        Ok(Self { versions })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedVersion {
    version: [u8; 2],
}

impl SelectedVersion {
    pub(crate) fn is(self, version: [u8; 2]) -> bool {
        self.version == version
    }
}

impl<'a> Extension<'a> for SelectedVersion {
    type Error = SupportedVersionsError;

    const TYPE: u16 = SUPPORTED_VERSIONS;

    fn decode(body: &'a [u8]) -> Result<Self, Self::Error> {
        let version = <[u8; 2]>::try_from(body)
            .map_err(|_| SupportedVersionsError::SelectedLength { length: body.len() })?;
        Ok(Self { version })
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SupportedVersionsError {
    #[error("TLS supported_versions list length is truncated")]
    LengthTruncated,
    #[error("TLS supported_versions list length {length} must be nonzero and even")]
    Length { length: usize },
    #[error("TLS supported_versions list is truncated: declared {declared}, received {available}")]
    BodyLength { declared: usize, available: usize },
    #[error("TLS selected supported_versions body must be 2 bytes, received {length}")]
    SelectedLength { length: usize },
}

#[cfg(test)]
mod tests {
    use super::super::find;
    use super::*;

    #[test]
    fn client_versions_borrow_the_version_list() {
        let input = [0x00, 0x2b, 0x00, 0x05, 0x04, 0x03, 0x03, 0x03, 0x04];
        let versions = find::<SupportedVersions<'_>>(&input)
            .expect("supported_versions body")
            .expect("supported_versions extension");

        assert!(versions.contains(TLS_1_3));
        assert_eq!(versions.versions.as_ptr(), input[5..].as_ptr());
    }

    #[test]
    fn server_version_requires_one_protocol_version() {
        assert_eq!(
            SelectedVersion::decode(&[0x03]),
            Err(SupportedVersionsError::SelectedLength { length: 1 })
        );
    }
}
