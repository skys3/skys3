//! Incremental digests for every checksum algorithm.
//!
//! SHA-1 and SHA-256 come from `aws-lc-rs`, the workspace's crypto
//! library; it has no MD5, which comes from RustCrypto's `md-5`. The three
//! CRCs come from `crc-fast`, which `s3s` and the AWS SDK already use, and
//! which also combines CRCs of consecutive byte ranges.

use std::collections::BTreeMap;
use std::fmt;

use aws_lc_rs::digest;
use crc_fast::CrcAlgorithm;
use md5::{Digest as _, Md5};
use skys3_types::checksum::ChecksumAlgorithm;

/// Digests by algorithm. A CRC is its big-endian value.
pub type Digests = BTreeMap<ChecksumAlgorithm, Vec<u8>>;

/// Computes one algorithm's digest over bytes fed in any number of pieces.
pub struct Hasher {
    algorithm: ChecksumAlgorithm,
    state: State,
}

enum State {
    /// Boxed: its parameter tables make it several times larger than
    /// the others.
    Crc(Box<crc_fast::Digest>),
    Sha(digest::Context),
    Md5(Md5),
}

fn crc(algorithm: CrcAlgorithm) -> State {
    State::Crc(Box::new(crc_fast::Digest::new(algorithm)))
}

impl Hasher {
    /// A hasher for `algorithm`, with nothing fed yet.
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        let state = match algorithm {
            ChecksumAlgorithm::Crc32 => crc(CrcAlgorithm::Crc32IsoHdlc),
            ChecksumAlgorithm::Crc32c => crc(CrcAlgorithm::Crc32Iscsi),
            ChecksumAlgorithm::Crc64Nvme => crc(CrcAlgorithm::Crc64Nvme),
            ChecksumAlgorithm::Sha1 => {
                State::Sha(digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY))
            }
            ChecksumAlgorithm::Sha256 => State::Sha(digest::Context::new(&digest::SHA256)),
            ChecksumAlgorithm::Md5 => State::Md5(Md5::new()),
        };
        Self { algorithm, state }
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// Feeds the next bytes.
    pub fn update(&mut self, data: &[u8]) {
        match &mut self.state {
            State::Crc(crc) => crc.update(data),
            State::Sha(context) => context.update(data),
            State::Md5(md5) => md5.update(data),
        }
    }

    /// The digest of everything fed: [`ChecksumAlgorithm::digest_len`]
    /// bytes.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        match self.state {
            State::Crc(crc) => crc_bytes(self.algorithm, crc.finalize()),
            State::Sha(context) => context.finish().as_ref().to_vec(),
            State::Md5(md5) => md5.finalize().to_vec(),
        }
    }
}

impl fmt::Debug for Hasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Hasher").field(&self.algorithm).finish()
    }
}

/// The digest of `data` in one call.
#[must_use]
pub fn digest(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new(algorithm);
    hasher.update(data);
    hasher.finish()
}

/// Several hashers fed the same bytes.
#[derive(Debug)]
pub struct Hashers(Vec<Hasher>);

impl Hashers {
    /// One hasher per algorithm in `algorithms`, without duplicates.
    pub fn new(algorithms: impl IntoIterator<Item = ChecksumAlgorithm>) -> Self {
        let mut hashers: Vec<Hasher> = Vec::new();
        for algorithm in algorithms {
            if hashers.iter().all(|h| h.algorithm != algorithm) {
                hashers.push(Hasher::new(algorithm));
            }
        }
        Self(hashers)
    }

    /// Feeds the next bytes to every hasher.
    pub fn update(&mut self, data: &[u8]) {
        for hasher in &mut self.0 {
            hasher.update(data);
        }
    }

    /// Every hasher's digest.
    #[must_use]
    pub fn finish(self) -> Digests {
        self.0
            .into_iter()
            .map(|hasher| (hasher.algorithm, hasher.finish()))
            .collect()
    }
}

/// The CRC algorithm and width of `algorithm`, if it is a CRC.
pub(crate) fn crc_algorithm(algorithm: ChecksumAlgorithm) -> Option<CrcAlgorithm> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => Some(CrcAlgorithm::Crc32IsoHdlc),
        ChecksumAlgorithm::Crc32c => Some(CrcAlgorithm::Crc32Iscsi),
        ChecksumAlgorithm::Crc64Nvme => Some(CrcAlgorithm::Crc64Nvme),
        _ => None,
    }
}

/// A CRC value as digest bytes: big-endian, as wide as the CRC.
pub(crate) fn crc_bytes(algorithm: ChecksumAlgorithm, value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    bytes[bytes.len() - algorithm.digest_len()..].to_vec()
}

/// A CRC's digest bytes as its value.
pub(crate) fn crc_value(digest: &[u8]) -> u64 {
    digest
        .iter()
        .fold(0, |value, &byte| value << 8 | u64::from(byte))
}
