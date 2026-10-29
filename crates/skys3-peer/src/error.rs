//! Why a message could not be encoded or decoded.

use crate::frame::{MAX_HEADER_LEN, MAX_PAYLOAD_LEN};

/// Why a message could not be encoded or decoded. Encoding and decoding
/// check the same rules, so whatever one end can send, the other accepts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MessageError {
    /// The header is longer than [`MAX_HEADER_LEN`].
    #[error("message header of {0} bytes exceeds the limit of {MAX_HEADER_LEN}")]
    HeaderTooLong(u64),
    /// The payload is longer than [`MAX_PAYLOAD_LEN`].
    #[error("message payload of {0} bytes exceeds the limit of {MAX_PAYLOAD_LEN}")]
    PayloadTooLong(u64),
    /// The header is not a valid protobuf message.
    #[error("malformed message header: {0}")]
    Malformed(#[from] prost::DecodeError),
    /// The header holds no message this version knows.
    #[error("the header holds no known message")]
    UnknownMessage,
    /// The payload does not match the checksum the header gives for it.
    #[error("payload checksum {actual:#010x} does not match {expected:#010x}")]
    ChecksumMismatch {
        /// The CRC32C the header gives.
        expected: u32,
        /// The CRC32C of the payload received.
        actual: u32,
    },
    /// A field breaks a rule of the protocol.
    #[error("invalid {field}: {problem}")]
    Invalid {
        /// The field, such as `commit.put.parts`.
        field: &'static str,
        /// What is wrong with it.
        problem: String,
    },
}

impl MessageError {
    /// An [`MessageError::Invalid`] for `field`.
    pub(crate) fn invalid(field: &'static str, problem: impl ToString) -> Self {
        Self::Invalid {
            field,
            problem: problem.to_string(),
        }
    }
}

/// Checks a rule of `field`.
pub(crate) fn ensure(
    ok: bool,
    field: &'static str,
    problem: impl FnOnce() -> String,
) -> Result<(), MessageError> {
    if ok {
        Ok(())
    } else {
        Err(MessageError::invalid(field, problem()))
    }
}
