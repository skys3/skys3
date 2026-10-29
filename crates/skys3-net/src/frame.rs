//! The frame format: a length-prefixed `prost` header and a raw payload.
//!
//! ```text
//! frame   = header_len payload_len header payload
//! header_len, payload_len: u32, big-endian
//! header  = header_len bytes: a protobuf `Header` (kind, request ID, body)
//! payload = payload_len bytes, opaque to the transport
//! ```
//!
//! Bulk data, such as an encoded log record or a backfill chunk, travels in
//! the payload, so it is never copied through protobuf. Both lengths are
//! checked against [`MAX_HEADER_LEN`] and [`MAX_PAYLOAD_LEN`] before
//! anything is allocated for them, and a payload's buffer grows only as its
//! bytes arrive, so a peer cannot make a node allocate memory it has not
//! sent. The wire format's version is negotiated once per connection, by
//! ALPN ([`ALPN_PROTOCOL`](crate::ALPN_PROTOCOL)), not stored per frame.

use std::io;

use bytes::Bytes;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::message::MessageKind;

/// The length of the prefix that holds the header and payload lengths.
pub const PREFIX_LEN: usize = 8;

/// The largest encoded header, in bytes (64 KiB). Headers hold small
/// protocol fields; anything large belongs in the payload.
pub const MAX_HEADER_LEN: u32 = 64 * 1024;

/// The largest payload, in bytes (32 MiB). It covers the largest log record
/// (a 2 MiB record header and a 16 MiB payload, §10.1) with room to spare.
pub const MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024;

/// The most a payload buffer grows by before the bytes to fill it arrive.
const PAYLOAD_GROWTH_STEP: usize = 64 * 1024;

/// A frame's header: what the frame is and the kind-specific fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// What the frame carries. Never [`MessageKind::Unspecified`] in a
    /// frame that was read or written.
    pub kind: MessageKind,
    /// Correlates a reply with its request: a request that expects a
    /// reply carries a sender-chosen nonzero ID, and the reply carries the
    /// same ID. Zero for one-way messages.
    pub request_id: u64,
    /// The kind-specific fields, a `prost` message defined by the layer
    /// that owns the kind.
    pub body: Bytes,
}

impl Header {
    /// A header of `kind` with no request ID and an empty body.
    #[must_use]
    pub fn new(kind: MessageKind) -> Self {
        Self {
            kind,
            request_id: 0,
            body: Bytes::new(),
        }
    }

    /// Sets the request ID.
    #[must_use]
    pub fn with_request_id(mut self, request_id: u64) -> Self {
        self.request_id = request_id;
        self
    }

    /// Sets the body.
    #[must_use]
    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }
}

/// One frame: a header and a raw payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The header.
    pub header: Header,
    /// The payload, possibly empty.
    pub payload: Bytes,
}

/// The header as it is encoded on the wire.
#[derive(Clone, PartialEq, Message)]
struct WireHeader {
    #[prost(enumeration = "MessageKind", tag = "1")]
    kind: i32,
    #[prost(uint64, tag = "2")]
    request_id: u64,
    #[prost(bytes = "bytes", tag = "3")]
    body: Bytes,
}

/// Why a frame could not be read or written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The header is longer than [`MAX_HEADER_LEN`].
    #[error("frame header of {0} bytes exceeds the limit of {MAX_HEADER_LEN}")]
    HeaderTooLong(u64),
    /// The payload is longer than [`MAX_PAYLOAD_LEN`].
    #[error("frame payload of {0} bytes exceeds the limit of {MAX_PAYLOAD_LEN}")]
    PayloadTooLong(u64),
    /// The header is not a valid protobuf message.
    #[error("malformed frame header: {0}")]
    Malformed(#[from] prost::DecodeError),
    /// The header's message kind is unknown or unspecified.
    #[error("frame header has an unknown message kind {0}")]
    UnknownKind(i32),
    /// The stream ended inside a frame.
    #[error("the connection closed in the middle of a frame")]
    Truncated,
    /// Reading or writing the stream failed.
    #[error(transparent)]
    Io(io::Error),
}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(error)
        }
    }
}

impl Frame {
    /// A frame of `header` and `payload`.
    #[must_use]
    pub fn new(header: Header, payload: impl Into<Bytes>) -> Self {
        Self {
            header,
            payload: payload.into(),
        }
    }

    /// Encodes the prefix and header, everything but the payload, which is
    /// written after it as is.
    ///
    /// # Errors
    ///
    /// [`FrameError::HeaderTooLong`], [`FrameError::PayloadTooLong`], or
    /// [`FrameError::UnknownKind`] for [`MessageKind::Unspecified`].
    pub fn encode_head(&self) -> Result<Vec<u8>, FrameError> {
        let header = &self.header;
        if header.kind == MessageKind::Unspecified {
            return Err(FrameError::UnknownKind(header.kind as i32));
        }
        let wire = WireHeader {
            kind: header.kind as i32,
            request_id: header.request_id,
            body: header.body.clone(),
        };
        let header_len = wire.encoded_len();
        let header_len = u32::try_from(header_len)
            .ok()
            .filter(|len| *len <= MAX_HEADER_LEN)
            .ok_or(FrameError::HeaderTooLong(header_len as u64))?;
        let payload_len = u32::try_from(self.payload.len())
            .ok()
            .filter(|len| *len <= MAX_PAYLOAD_LEN)
            .ok_or(FrameError::PayloadTooLong(self.payload.len() as u64))?;
        let mut head = Vec::with_capacity(PREFIX_LEN + header_len as usize);
        head.extend_from_slice(&header_len.to_be_bytes());
        head.extend_from_slice(&payload_len.to_be_bytes());
        wire.encode(&mut head)
            .expect("a Vec grows to fit the encoded header");
        Ok(head)
    }

    /// Encodes the whole frame into one buffer.
    ///
    /// # Errors
    ///
    /// As [`Frame::encode_head`].
    pub fn encode(&self) -> Result<Vec<u8>, FrameError> {
        let mut out = self.encode_head()?;
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decodes the frame at the start of `buf`, returning it and the bytes
    /// it took, or `None` if `buf` holds only part of a frame. The lengths
    /// are checked as soon as the prefix is complete, and the header as soon
    /// as it is, so an invalid frame is refused without waiting for the rest
    /// of its bytes, as [`read_frame`] refuses it.
    ///
    /// ```
    /// use skys3_net::{Frame, Header, MessageKind};
    ///
    /// let frame = Frame::new(Header::new(MessageKind::Beacon).with_request_id(7), "data");
    /// let bytes = frame.encode()?;
    /// assert_eq!(Frame::decode(&bytes)?, Some((frame, bytes.len())));
    /// assert_eq!(Frame::decode(&bytes[..bytes.len() - 1])?, None);
    /// # Ok::<(), skys3_net::FrameError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`FrameError`] for a length over its limit, a malformed header, or an
    /// unknown message kind.
    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, FrameError> {
        let Some(prefix) = buf.first_chunk::<PREFIX_LEN>() else {
            return Ok(None);
        };
        let (header_len, payload_len) = parse_prefix(prefix)?;
        let header_end = PREFIX_LEN + header_len;
        let Some(header) = buf.get(PREFIX_LEN..header_end) else {
            return Ok(None);
        };
        let header = decode_header(Bytes::copy_from_slice(header))?;
        let total = header_end + payload_len;
        let Some(payload) = buf.get(header_end..total) else {
            return Ok(None);
        };
        let payload = Bytes::copy_from_slice(payload);
        Ok(Some((Self { header, payload }, total)))
    }
}

/// Checks a prefix and returns the header and payload lengths.
fn parse_prefix(prefix: &[u8; PREFIX_LEN]) -> Result<(usize, usize), FrameError> {
    let [h0, h1, h2, h3, p0, p1, p2, p3] = *prefix;
    let header_len = u32::from_be_bytes([h0, h1, h2, h3]);
    let payload_len = u32::from_be_bytes([p0, p1, p2, p3]);
    if header_len > MAX_HEADER_LEN {
        return Err(FrameError::HeaderTooLong(header_len.into()));
    }
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLong(payload_len.into()));
    }
    // Both limits are far below `usize::MAX` on every supported target.
    Ok((header_len as usize, payload_len as usize))
}

fn decode_header(bytes: Bytes) -> Result<Header, FrameError> {
    let wire = WireHeader::decode(bytes)?;
    match MessageKind::try_from(wire.kind) {
        Ok(kind) if kind != MessageKind::Unspecified => Ok(Header {
            kind,
            request_id: wire.request_id,
            body: wire.body,
        }),
        _ => Err(FrameError::UnknownKind(wire.kind)),
    }
}

/// Reads one frame, or `None` if the stream ends cleanly before it.
///
/// # Errors
///
/// [`FrameError`] for an invalid frame, a stream that ends inside a frame,
/// or a failed read.
pub async fn read_frame<R>(reader: &mut R) -> Result<Option<Frame>, FrameError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut prefix = [0; PREFIX_LEN];
    let mut filled = 0;
    while filled < PREFIX_LEN {
        let read = reader.read(&mut prefix[filled..]).await?;
        if read == 0 {
            return if filled == 0 {
                Ok(None)
            } else {
                Err(FrameError::Truncated)
            };
        }
        filled += read;
    }
    let (header_len, payload_len) = parse_prefix(&prefix)?;

    let mut header = vec![0; header_len];
    reader.read_exact(&mut header).await?;
    let header = decode_header(Bytes::from(header))?;

    let mut payload = Vec::new();
    while payload.len() < payload_len {
        if payload.len() == payload.capacity() {
            // Grow geometrically, so filling the buffer copies each byte
            // O(1) times, but never past the declared length nor more than
            // 1 MiB ahead of the bytes received.
            let target = (payload.capacity() * 2)
                .clamp(
                    PAYLOAD_GROWTH_STEP,
                    payload.len() + PAYLOAD_GROWTH_STEP * 16,
                )
                .min(payload_len);
            payload.reserve_exact(target - payload.len());
        }
        let room = payload.capacity() - payload.len();
        let read = (&mut *reader)
            .take(room as u64)
            .read_buf(&mut payload)
            .await?;
        if read == 0 {
            return Err(FrameError::Truncated);
        }
    }
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

/// Writes one frame and flushes the stream.
///
/// # Errors
///
/// The errors of [`Frame::encode_head`], or [`FrameError::Io`] for a failed
/// write.
pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let head = frame.encode_head()?;
    writer.write_all(&head).await?;
    writer.write_all(&frame.payload).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn kinds() -> impl Strategy<Value = MessageKind> {
        prop::sample::select(vec![
            MessageKind::Append,
            MessageKind::AppendAck,
            MessageKind::Beacon,
            MessageKind::Forward,
            MessageKind::Handoff,
        ])
    }

    fn frames() -> impl Strategy<Value = Frame> {
        (
            kinds(),
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 0..64),
            prop::collection::vec(any::<u8>(), 0..300_000),
        )
            .prop_map(|(kind, request_id, body, payload)| {
                Frame::new(
                    Header::new(kind)
                        .with_request_id(request_id)
                        .with_body(body),
                    payload,
                )
            })
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn prefix(header_len: u32, payload_len: u32) -> Vec<u8> {
        let mut out = header_len.to_be_bytes().to_vec();
        out.extend_from_slice(&payload_len.to_be_bytes());
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn frames_round_trip(frame in frames()) {
            let bytes = frame.encode().unwrap();
            prop_assert_eq!(Frame::decode(&bytes).unwrap(), Some((frame.clone(), bytes.len())));
            let mut reader = bytes.as_slice();
            let read = block_on(read_frame(&mut reader)).unwrap();
            prop_assert_eq!(read, Some(frame));
            prop_assert!(reader.is_empty());
        }

        #[test]
        fn every_prefix_of_a_frame_is_incomplete(frame in frames(), cut in any::<prop::sample::Index>()) {
            let bytes = frame.encode().unwrap();
            let cut = cut.index(bytes.len());
            prop_assert_eq!(Frame::decode(&bytes[..cut]).unwrap(), None);
            let mut reader = &bytes[..cut];
            let result = block_on(read_frame(&mut reader));
            if cut == 0 {
                prop_assert!(matches!(result, Ok(None)));
            } else {
                prop_assert!(matches!(result, Err(FrameError::Truncated)), "{result:?}");
            }
        }

        #[test]
        fn arbitrary_bytes_parse_alike(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let decoded = Frame::decode(&bytes);
            let mut reader = bytes.as_slice();
            let read = block_on(read_frame(&mut reader));
            // Both parsers agree: on complete frames, on where the bytes
            // run out, and on refusing invalid frames.
            match decoded {
                Ok(Some((frame, _))) => prop_assert_eq!(read.unwrap(), Some(frame)),
                Ok(None) if bytes.is_empty() => prop_assert!(matches!(read, Ok(None))),
                Ok(None) => prop_assert!(matches!(read, Err(FrameError::Truncated)), "{read:?}"),
                Err(_) => prop_assert!(read.is_err()),
            }
        }
    }

    #[test]
    fn lengths_over_the_limits_are_refused_from_the_prefix() {
        let mut too_long = prefix(MAX_HEADER_LEN + 1, 0);
        assert!(matches!(
            Frame::decode(&too_long),
            Err(FrameError::HeaderTooLong(len)) if len == u64::from(MAX_HEADER_LEN) + 1
        ));
        too_long = prefix(2, MAX_PAYLOAD_LEN + 1);
        assert!(matches!(
            Frame::decode(&too_long),
            Err(FrameError::PayloadTooLong(_))
        ));
        let mut reader = &prefix(u32::MAX, u32::MAX)[..];
        assert!(matches!(
            block_on(read_frame(&mut reader)),
            Err(FrameError::HeaderTooLong(_))
        ));
    }

    #[test]
    fn a_payload_buffer_grows_only_with_the_bytes_received() {
        // A frame that declares the largest payload but sends 10 bytes of it
        // must not allocate the declared length.
        let mut bytes = Frame::new(Header::new(MessageKind::Append), Bytes::new())
            .encode()
            .unwrap();
        bytes[4..8].copy_from_slice(&MAX_PAYLOAD_LEN.to_be_bytes());
        bytes.extend_from_slice(&[0; 10]);
        let mut reader = bytes.as_slice();
        assert!(matches!(
            block_on(read_frame(&mut reader)),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn a_maximal_frame_round_trips() {
        let frame = Frame::new(
            Header::new(MessageKind::Backfill),
            vec![7; MAX_PAYLOAD_LEN as usize],
        );
        let bytes = frame.encode().unwrap();
        let mut reader = bytes.as_slice();
        assert_eq!(block_on(read_frame(&mut reader)).unwrap(), Some(frame));
    }

    #[test]
    fn oversized_or_unspecified_frames_are_not_encoded() {
        let big_body = Header::new(MessageKind::Append).with_body(vec![0; MAX_HEADER_LEN as usize]);
        assert!(matches!(
            Frame::new(big_body, Bytes::new()).encode(),
            Err(FrameError::HeaderTooLong(_))
        ));
        let big_payload = Frame::new(
            Header::new(MessageKind::Append),
            vec![0; MAX_PAYLOAD_LEN as usize + 1],
        );
        assert!(matches!(
            big_payload.encode(),
            Err(FrameError::PayloadTooLong(_))
        ));
        let unspecified = Frame::new(Header::new(MessageKind::Unspecified), Bytes::new());
        assert!(matches!(
            unspecified.encode(),
            Err(FrameError::UnknownKind(0))
        ));
    }

    #[test]
    fn headers_with_unknown_kinds_or_bad_protobuf_are_refused() {
        for kind in [0, 7, -1, 1000] {
            let wire = WireHeader {
                kind,
                request_id: 1,
                body: Bytes::new(),
            };
            let header = wire.encode_to_vec();
            let mut bytes = prefix(header.len() as u32, 0);
            bytes.extend_from_slice(&header);
            assert!(
                matches!(Frame::decode(&bytes), Err(FrameError::UnknownKind(k)) if k == kind),
                "{kind}"
            );
        }
        // A field with a truncated length-delimited value.
        let mut bytes = prefix(2, 0);
        bytes.extend_from_slice(&[0x1a, 0x05]);
        let error = Frame::decode(&bytes).unwrap_err();
        assert!(matches!(error, FrameError::Malformed(_)), "{error}");
        assert!(error.to_string().starts_with("malformed frame header"));
    }

    #[test]
    fn write_frame_writes_what_encode_returns() {
        let frame = Frame::new(
            Header::new(MessageKind::StepDown).with_body(&b"body"[..]),
            &b"payload"[..],
        );
        let mut out = Vec::new();
        block_on(write_frame(&mut out, &frame)).unwrap();
        assert_eq!(out, frame.encode().unwrap());
        let error = FrameError::from(io::Error::other("boom"));
        assert_eq!(error.to_string(), "boom");
    }
}
