//! The frame format of a message on a peer stream: a length-prefixed
//! `prost` header and a raw payload, as on intra-cluster connections
//! (`skys3-net`).
//!
//! ```text
//! frame   = header_len payload_len header payload
//! header_len, payload_len: u32, big-endian
//! header  = header_len bytes: a protobuf envelope holding one message
//! payload = payload_len bytes: DATA's bytes, or BATCH's inline bodies;
//!           empty for every other message
//! ```
//!
//! Both lengths are checked against [`MAX_HEADER_LEN`] and
//! [`MAX_PAYLOAD_LEN`] as soon as the prefix is complete, and the header
//! as soon as it is, so an invalid frame is refused before the rest of its
//! bytes arrive. The format and `HELLO` stay the same in every protocol
//! version, so two ends can always read each other's `HELLO`; the version
//! `HELLO` negotiates governs every later message.

use bytes::Bytes;
use prost::Message as _;
use skys3_types::limits::MAX_RECORD_PAYLOAD_LEN;

use crate::error::MessageError;
use crate::message::Message;
use crate::wire::Envelope;

/// The length of the prefix that holds the header and payload lengths.
pub const PREFIX_LEN: usize = 8;

/// The largest encoded header, in bytes (1 MiB). It holds the largest
/// `COMMIT`, that of a 10,000-part object with the most metadata and tags,
/// about 0.5 MiB; a source splits batches to fit.
pub const MAX_HEADER_LEN: u32 = 1024 * 1024;

/// The largest payload, in bytes (16 MiB): the largest log record payload,
/// since a destination stages each `DATA` frame as one record. It bounds
/// `peer_frame_bytes`, and the inline bytes of a `BATCH`.
pub const MAX_PAYLOAD_LEN: u32 = MAX_RECORD_PAYLOAD_LEN;

/// Checks a frame's prefix and returns its header and payload lengths.
///
/// # Errors
///
/// [`MessageError::HeaderTooLong`] or [`MessageError::PayloadTooLong`].
pub fn parse_prefix(prefix: &[u8; PREFIX_LEN]) -> Result<(usize, usize), MessageError> {
    let [h0, h1, h2, h3, p0, p1, p2, p3] = *prefix;
    let header_len = u32::from_be_bytes([h0, h1, h2, h3]);
    let payload_len = u32::from_be_bytes([p0, p1, p2, p3]);
    if header_len > MAX_HEADER_LEN {
        return Err(MessageError::HeaderTooLong(header_len.into()));
    }
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(MessageError::PayloadTooLong(payload_len.into()));
    }
    // Both limits are far below `usize::MAX` on every supported target.
    Ok((header_len as usize, payload_len as usize))
}

impl Message {
    /// Encodes the message as one frame.
    ///
    /// ```
    /// use skys3_peer::{Abort, AbortReason, Message};
    ///
    /// let abort = Message::Abort(Abort {
    ///     identity: "prod-us/b-7f3a/5/42.1001".parse()?,
    ///     reason: AbortReason::Cancelled,
    ///     detail: String::new(),
    /// });
    /// let bytes = abort.encode()?;
    /// assert_eq!(Message::decode(&bytes)?, Some((abort, bytes.len())));
    /// assert_eq!(Message::decode(&bytes[..bytes.len() - 1])?, None);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// The errors of [`Message::validate`], or
    /// [`MessageError::HeaderTooLong`] for a header past
    /// [`MAX_HEADER_LEN`].
    pub fn encode(&self) -> Result<Vec<u8>, MessageError> {
        self.validate()?;
        let (envelope, payload) = Envelope::encode(self);
        let header_len = envelope.encoded_len();
        let header_len = u32::try_from(header_len)
            .ok()
            .filter(|&len| len <= MAX_HEADER_LEN)
            .ok_or(MessageError::HeaderTooLong(header_len as u64))?;
        let payload_len: usize = payload.iter().map(Bytes::len).sum();
        // `validate` bounds every payload.
        let payload_len = u32::try_from(payload_len).expect("a validated payload fits a u32");
        let mut out = Vec::with_capacity(PREFIX_LEN + header_len as usize + payload_len as usize);
        out.extend_from_slice(&header_len.to_be_bytes());
        out.extend_from_slice(&payload_len.to_be_bytes());
        envelope
            .encode(&mut out)
            .expect("a Vec grows to fit the encoded header");
        for chunk in &payload {
            out.extend_from_slice(chunk);
        }
        Ok(out)
    }

    /// Decodes the frame at the start of `buf`, returning its message and
    /// the bytes it took, or `None` if `buf` holds only part of a frame.
    ///
    /// # Errors
    ///
    /// [`MessageError`] for a length over its limit, a malformed or
    /// unknown header, a payload that fails its checksum, or a message
    /// that breaks a rule of the protocol.
    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, MessageError> {
        let Some(prefix) = buf.first_chunk::<PREFIX_LEN>() else {
            return Ok(None);
        };
        let (header_len, payload_len) = parse_prefix(prefix)?;
        let header_end = PREFIX_LEN + header_len;
        let Some(header) = buf.get(PREFIX_LEN..header_end) else {
            return Ok(None);
        };
        let envelope = Envelope::decode_header(header)?;
        let total = header_end + payload_len;
        let Some(payload) = buf.get(header_end..total) else {
            return Ok(None);
        };
        let message = envelope.into_message(Bytes::copy_from_slice(payload))?;
        Ok(Some((message, total)))
    }

    /// Decodes a message from a frame's header and payload, for a reader
    /// that has taken the lengths from the prefix with [`parse_prefix`].
    ///
    /// # Errors
    ///
    /// As [`Message::decode`].
    pub fn decode_parts(header: &[u8], payload: Bytes) -> Result<Self, MessageError> {
        Envelope::decode_header(header)?.into_message(payload)
    }
}
