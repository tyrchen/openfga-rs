//! Deterministic, length-delimited SHA-256 fingerprints for semantic values.

use std::fmt;

use sha2::{Digest, Sha256};

/// A stable 256-bit semantic fingerprint.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    /// Creates a fingerprint from its complete binary representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the complete binary representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Fingerprint([REDACTED])")
    }
}

/// A domain-separated canonical fingerprint encoder.
///
/// Every byte/string is length-delimited, and callers explicitly tag variants
/// and fields. This prevents concatenation ambiguity and makes fingerprints
/// independent of map iteration order when callers feed sorted entries.
#[non_exhaustive]
pub struct FingerprintBuilder {
    hasher: Sha256,
}

impl FingerprintBuilder {
    /// Starts a new fingerprint in the supplied stable semantic domain.
    #[must_use]
    pub fn new(domain: &str) -> Self {
        let mut builder = Self {
            hasher: Sha256::new(),
        };
        builder.write_bytes(domain.as_bytes());
        builder
    }

    /// Writes a one-byte variant or field tag.
    pub fn write_tag(&mut self, tag: u8) {
        self.hasher.update([tag]);
    }

    /// Writes an unsigned 32-bit value in network byte order.
    pub fn write_u32(&mut self, value: u32) {
        self.hasher.update(value.to_be_bytes());
    }

    /// Writes an unsigned 64-bit value in network byte order.
    pub fn write_u64(&mut self, value: u64) {
        self.hasher.update(value.to_be_bytes());
    }

    /// Writes a signed 64-bit value in network byte order.
    pub fn write_i64(&mut self, value: i64) {
        self.hasher.update(value.to_be_bytes());
    }

    /// Writes a length-delimited byte string.
    pub fn write_bytes(&mut self, value: &[u8]) {
        self.write_u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
        self.hasher.update(value);
    }

    /// Writes a length-delimited UTF-8 string.
    pub fn write_str(&mut self, value: &str) {
        self.write_bytes(value.as_bytes());
    }

    /// Finishes the canonical digest.
    #[must_use]
    pub fn finish(self) -> Fingerprint {
        Fingerprint(self.hasher.finalize().into())
    }
}

impl fmt::Debug for FingerprintBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FingerprintBuilder([REDACTED STATE])")
    }
}

#[cfg(test)]
mod tests {
    use super::FingerprintBuilder;

    #[test]
    fn test_should_domain_separate_and_length_delimit_fingerprints() {
        let mut first = FingerprintBuilder::new("openfga.test.v1");
        first.write_str("ab");
        first.write_str("c");

        let mut second = FingerprintBuilder::new("openfga.test.v1");
        second.write_str("a");
        second.write_str("bc");

        let mut other_domain = FingerprintBuilder::new("openfga.other.v1");
        other_domain.write_str("ab");
        other_domain.write_str("c");

        assert_ne!(first.finish(), second.finish());
        assert_ne!(
            FingerprintBuilder::new("openfga.test.v1").finish(),
            other_domain.finish()
        );
    }
}
