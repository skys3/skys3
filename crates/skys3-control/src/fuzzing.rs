//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API. It
//! exposes the etcd backend's parsers of what etcd sends without making
//! them part of the crate's public surface.

use http::{HeaderMap, HeaderValue};
use prost::Message;

use crate::etcd::grpc::{FrameDecoder, MAX_MESSAGE_LEN, PREFIX_LEN, status_of};
use crate::etcd::proto::{RangeResponse, TxnResponse, WatchResponse};

/// What [`etcd_frames`] decoded.
#[derive(Debug, Default)]
pub struct Decoded {
    /// The complete messages, in order.
    pub messages: Vec<Vec<u8>>,
    /// Whether the decoder refused the stream.
    pub refused: bool,
}

/// Splits `data` into gRPC messages as the etcd backend reads a response
/// body, fed in chunks of `chunk` bytes (at least 1), and decodes each
/// message as every response type the backend reads.
///
/// # Panics
///
/// If a message is over [`ETCD_MAX_MESSAGE_LEN`], which the decoder must
/// refuse before buffering it.
#[must_use]
pub fn etcd_frames(data: &[u8], chunk: usize) -> Decoded {
    let mut decoder = FrameDecoder::default();
    let mut decoded = Decoded::default();
    for piece in data.chunks(chunk.max(1)) {
        if decoder.push(piece).is_err() {
            decoded.refused = true;
            return decoded;
        }
        loop {
            match decoder.next_message() {
                Ok(Some(message)) => {
                    assert!(message.len() <= MAX_MESSAGE_LEN);
                    let _ = RangeResponse::decode(message.clone());
                    let _ = TxnResponse::decode(message.clone());
                    let _ = WatchResponse::decode(message.clone());
                    decoded.messages.push(message.to_vec());
                }
                Ok(None) => break,
                Err(_) => {
                    decoded.refused = true;
                    return decoded;
                }
            }
        }
    }
    decoded
}

/// The length of a gRPC message prefix.
pub const ETCD_PREFIX_LEN: usize = PREFIX_LEN;

/// The longest gRPC message the etcd backend accepts.
pub const ETCD_MAX_MESSAGE_LEN: usize = MAX_MESSAGE_LEN;

/// Reads a gRPC status from `grpc-status` and `grpc-message` header
/// values, as the backend reads trailers. Returns `None` if a value is not
/// a valid header value, which the HTTP/2 decoder would have rejected.
#[must_use]
pub fn etcd_status(code: &[u8], message: &[u8]) -> Option<(u32, String)> {
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", HeaderValue::from_bytes(code).ok()?);
    headers.insert("grpc-message", HeaderValue::from_bytes(message).ok()?);
    let status = status_of(&headers)?;
    Some((status.code.0, status.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_decode_whatever_the_chunk_size() {
        let mut stream = vec![0, 0, 0, 0, 2, 0x20, 3];
        stream.extend([0, 0, 0, 0, 0]);
        for chunk in [0, 1, 3, 100] {
            let decoded = etcd_frames(&stream, chunk);
            assert_eq!(decoded.messages, [vec![0x20, 3], vec![]]);
            assert!(!decoded.refused);
        }
        stream.extend([1, 0, 0, 0, 0]);
        for chunk in [1, 100] {
            let decoded = etcd_frames(&stream, chunk);
            assert_eq!(decoded.messages.len(), 2);
            assert!(decoded.refused);
        }
        assert!(etcd_frames(&[0, 0xff, 0xff, 0xff, 0xff], 5).refused);
        const { assert!(ETCD_MAX_MESSAGE_LEN > ETCD_PREFIX_LEN) };
    }

    #[test]
    fn statuses_decode_from_header_values() {
        assert_eq!(
            etcd_status(b"14", b"no%20leader"),
            Some((14, "no leader".to_owned()))
        );
        assert_eq!(etcd_status(b"14", b"bad\nvalue"), None);
    }
}
