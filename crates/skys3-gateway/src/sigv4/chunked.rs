//! `aws-chunked` request bodies (S3's streaming SigV4 uploads).
//!
//! The body is a sequence of chunks, each `hex-size[;chunk-signature=sig]`
//! CRLF, then the data and CRLF, ending with a zero-size chunk, optional
//! trailer lines (`name:value` CRLF, with `x-amz-trailer-signature` when
//! signed), and an empty line. Three forms are accepted:
//!
//! | `x-amz-content-sha256` | Chunk signatures | Trailers |
//! |---|---|---|
//! | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | yes | none |
//! | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` | yes | signed |
//! | `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | no | unsigned |
//!
//! Each chunk signature chains from the previous one, starting from the
//! request's own signature. The [`Decoder`] is a state machine over input
//! slices, so its memory is bounded by the limits below and never by a
//! length the client declares: chunk data passes through without being
//! buffered. A chunk's signature is checked when its last byte arrives,
//! before that byte is yielded, so the decoded stream as a whole is
//! authenticated only once it ends without error. Consumers commit nothing
//! before then, which holds for checksums too: the trailer that carries
//! one comes last.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use aws_lc_rs::{digest, hmac};
use bytes::{Buf, Bytes};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http_body::{Frame, SizeHint};
use s3s::{Body, StdError};

use super::body::BodyError;
use super::canonical::{EMPTY_SHA256, hex, sha256, sign};
use super::params::decode_signature;

/// The longest chunk header line, CRLF included. A signed chunk's line is
/// at most 16 hexadecimal digits, `;chunk-signature=`, and 64 more digits.
pub const MAX_CHUNK_LINE_BYTES: usize = 128;

/// The most bytes of trailer lines, the trailer signature and the final
/// empty line included.
pub const MAX_TRAILER_BYTES: usize = 4096;

/// The most trailers `x-amz-trailer` may declare. S3 sends one checksum.
pub const MAX_TRAILERS: usize = 8;

/// The name of the trailer that signs the others.
const TRAILER_SIGNATURE: &str = "x-amz-trailer-signature";

/// The trailers of an `aws-chunked` body, such as a trailing checksum
/// (`x-amz-checksum-crc32c`).
///
/// The authenticator puts one in the request's extensions when the body
/// declares trailers. It is empty until the body has been read to its end
/// without error, and then holds the trailers, verified against the
/// trailer signature when the body is signed. Clones share the trailers.
#[derive(Debug, Clone, Default)]
pub struct Trailers(Arc<OnceLock<HeaderMap>>);

impl Trailers {
    /// The trailers, once the body has been read to its end, with
    /// lowercase names.
    #[must_use]
    pub fn get(&self) -> Option<&HeaderMap> {
        self.0.get()
    }

    fn set(&self, trailers: HeaderMap) {
        // Only the decoder sets them, once.
        let _ = self.0.set(trailers);
    }
}

/// Checks the signatures of a body's chunks and trailers, which chain from
/// the request signature.
pub(crate) struct ChunkSigner {
    key: hmac::Key,
    /// `timestamp\nscope\n`, the same in every string to sign.
    context: String,
    previous: [u8; 32],
}

impl ChunkSigner {
    /// A signer whose first chunk follows `seed`, the request signature.
    pub(crate) fn new(key: hmac::Key, timestamp: &str, scope: &str, seed: [u8; 32]) -> Self {
        Self {
            key,
            context: format!("{timestamp}\n{scope}\n"),
            previous: seed,
        }
    }

    fn string_to_sign(&self, algorithm: &str, payload: &str) -> String {
        format!(
            "{algorithm}\n{}{}\n{payload}",
            self.context,
            hex(&self.previous)
        )
    }

    fn chunk_string(&self, data_hash: &[u8; 32]) -> String {
        let payload = format!("{EMPTY_SHA256}\n{}", hex(data_hash));
        self.string_to_sign("AWS4-HMAC-SHA256-PAYLOAD", &payload)
    }

    fn trailer_string(&self, canonical_trailers: &[u8]) -> String {
        let payload = hex(&sha256(canonical_trailers));
        self.string_to_sign("AWS4-HMAC-SHA256-TRAILER", &payload)
    }

    /// Checks `signature` against `string_to_sign`, and on success chains
    /// the next signature from it.
    fn check(&mut self, string_to_sign: &str, signature: &[u8; 32]) -> Result<(), BodyError> {
        if super::canonical::verify(&self.key, string_to_sign.as_bytes(), signature) {
            self.previous = *signature;
            Ok(())
        } else {
            Err(BodyError::signature())
        }
    }

    /// Signs the next chunk, for [`encode`].
    #[cfg(any(test, feature = "test-util"))]
    fn sign_chunk(&mut self, data: &[u8]) -> [u8; 32] {
        let signature = sign(&self.key, self.chunk_string(&sha256(data)).as_bytes());
        self.previous = signature;
        signature
    }

    /// Signs canonical trailers, for [`encode`].
    #[cfg(any(test, feature = "test-util"))]
    fn sign_trailers(&mut self, canonical: &[u8]) -> [u8; 32] {
        let signature = sign(&self.key, self.trailer_string(canonical).as_bytes());
        self.previous = signature;
        signature
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Reading a chunk header line.
    Size,
    /// Passing on chunk data, `left` bytes to go.
    Data { left: u64 },
    /// Expecting the CRLF after chunk data; `seen` bytes of it so far.
    DataEnd { seen: usize },
    /// Reading trailer lines, after the zero-size chunk.
    Trailer,
    /// The body is complete; nothing may follow.
    Done,
}

/// The `aws-chunked` state machine. See the [module documentation](self).
pub(crate) struct Decoder {
    signer: Option<ChunkSigner>,
    /// Whether the body ends with trailers.
    trailer: bool,
    /// The trailers `x-amz-trailer` declares.
    expected: Vec<HeaderName>,
    /// Declared decoded bytes not yet claimed by a chunk header.
    unclaimed: u64,
    state: State,
    /// The partial line being read.
    line: Vec<u8>,
    /// Bytes of the trailer section read so far.
    trailer_bytes: usize,
    /// The hash of the current chunk's data, for signed chunks.
    hasher: Option<digest::Context>,
    /// The current chunk's signature.
    chunk_signature: [u8; 32],
    trailers: HeaderMap,
    trailer_signature: Option<[u8; 32]>,
}

impl Decoder {
    /// A decoder for a body of `decoded_length` bytes. `signer` is present
    /// for signed chunks; `trailer` says whether the body ends with the
    /// `expected` trailers.
    pub(crate) fn new(
        signer: Option<ChunkSigner>,
        trailer: bool,
        expected: Vec<HeaderName>,
        decoded_length: u64,
    ) -> Self {
        Self {
            signer,
            trailer,
            expected,
            unclaimed: decoded_length,
            state: State::Size,
            line: Vec::new(),
            trailer_bytes: 0,
            hasher: None,
            chunk_signature: [0; 32],
            trailers: HeaderMap::new(),
            trailer_signature: None,
        }
    }

    /// Consumes `input` and returns the next piece of decoded data, or
    /// `None` once `input` is used up (or the body is complete; see
    /// [`Decoder::is_done`]).
    ///
    /// # Errors
    ///
    /// A [`BodyError`] for malformed framing, a chunk or trailer signature
    /// that does not match, or more data than declared. The decoder must
    /// not be used after an error.
    pub(crate) fn decode(&mut self, input: &mut Bytes) -> Result<Option<Bytes>, BodyError> {
        loop {
            match self.state {
                State::Size => {
                    let Some(line) = self.take_line(input, MAX_CHUNK_LINE_BYTES)? else {
                        return Ok(None);
                    };
                    self.start_chunk(&line)?;
                }
                State::Data { left } => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let n = usize::try_from(left).map_or(input.len(), |left| left.min(input.len()));
                    let data = input.split_to(n);
                    if let Some(hasher) = &mut self.hasher {
                        hasher.update(&data);
                    }
                    let left = left - n as u64;
                    self.state = State::Data { left };
                    if left == 0 {
                        self.end_chunk()?;
                        self.state = State::DataEnd { seen: 0 };
                    }
                    return Ok(Some(data));
                }
                State::DataEnd { mut seen } => {
                    while seen < 2 {
                        let Some(&byte) = input.first() else {
                            self.state = State::DataEnd { seen };
                            return Ok(None);
                        };
                        if byte != b"\r\n"[seen] {
                            return Err(BodyError::malformed("chunk data is not followed by CRLF"));
                        }
                        input.advance(1);
                        seen += 1;
                    }
                    self.state = State::Size;
                }
                State::Trailer => {
                    let budget = MAX_TRAILER_BYTES - self.trailer_bytes;
                    let Some(line) = self.take_line(input, budget)? else {
                        return Ok(None);
                    };
                    self.trailer_bytes += line.len() + 2;
                    if line.is_empty() {
                        self.end_trailers()?;
                        self.state = State::Done;
                    } else {
                        self.add_trailer(&line)?;
                    }
                }
                State::Done => {
                    return if input.is_empty() {
                        Ok(None)
                    } else {
                        Err(BodyError::malformed("data follows the final chunk"))
                    };
                }
            }
        }
    }

    /// Whether the body is complete.
    pub(crate) fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Ends decoding at the end of the input and returns the trailers.
    ///
    /// # Errors
    ///
    /// `IncompleteBody` if the body stopped before its final chunk.
    pub(crate) fn finish(&mut self) -> Result<HeaderMap, BodyError> {
        if self.is_done() {
            Ok(std::mem::take(&mut self.trailers))
        } else {
            Err(BodyError::incomplete(
                "the aws-chunked body ended before its final chunk",
            ))
        }
    }

    /// Takes the next CRLF-terminated line, of at most `limit` bytes with
    /// its CRLF, without the CRLF, or `None` if `input` ends first.
    fn take_line(&mut self, input: &mut Bytes, limit: usize) -> Result<Option<Vec<u8>>, BodyError> {
        let newline = input.iter().position(|&b| b == b'\n');
        let take = newline.map_or(input.len(), |at| at + 1);
        if self.line.len() + take > limit {
            return Err(BodyError::malformed(format!(
                "a chunk header or trailer section is longer than {limit} bytes"
            )));
        }
        self.line.extend_from_slice(&input[..take]);
        input.advance(take);
        if newline.is_none() {
            return Ok(None);
        }
        let mut line = std::mem::take(&mut self.line);
        if !line.ends_with(b"\r\n") {
            return Err(BodyError::malformed("a line does not end with CRLF"));
        }
        line.truncate(line.len() - 2);
        Ok(Some(line))
    }

    /// Parses a chunk header line and starts the chunk.
    fn start_chunk(&mut self, line: &[u8]) -> Result<(), BodyError> {
        let (size, extension) = match line.iter().position(|&b| b == b';') {
            Some(at) => (&line[..at], Some(&line[at + 1..])),
            None => (line, None),
        };
        if size.is_empty() || size.len() > 16 || !size.iter().all(u8::is_ascii_hexdigit) {
            return Err(BodyError::malformed("a chunk size is not hexadecimal"));
        }
        let size = size.iter().fold(0_u64, |n, &digit| {
            n << 4 | u64::from(char::from(digit).to_digit(16).unwrap_or(0))
        });
        match (&self.signer, extension) {
            (Some(_), Some(extension)) => {
                self.chunk_signature = extension
                    .strip_prefix(b"chunk-signature=")
                    .and_then(decode_signature)
                    .ok_or_else(|| BodyError::malformed("a chunk signature is malformed"))?;
            }
            (Some(_), None) => return Err(BodyError::malformed("a signed chunk has no signature")),
            (None, Some(_)) => {
                return Err(BodyError::malformed("an unsigned chunk has an extension"));
            }
            (None, None) => {}
        }
        if size > self.unclaimed {
            return Err(BodyError::malformed(
                "the chunks hold more data than x-amz-decoded-content-length",
            ));
        }
        self.unclaimed -= size;
        if size == 0 {
            if self.unclaimed != 0 {
                return Err(BodyError::incomplete(
                    "the chunks hold less data than x-amz-decoded-content-length",
                ));
            }
            self.end_chunk()?;
            self.state = State::Trailer;
        } else {
            self.hasher = self
                .signer
                .is_some()
                .then(|| digest::Context::new(&digest::SHA256));
            self.state = State::Data { left: size };
        }
        Ok(())
    }

    /// Checks the signature of the chunk whose data just ended.
    fn end_chunk(&mut self) -> Result<(), BodyError> {
        let Some(signer) = &mut self.signer else {
            return Ok(());
        };
        let hash = match self.hasher.take() {
            Some(hasher) => {
                let mut hash = [0; 32];
                hash.copy_from_slice(hasher.finish().as_ref());
                hash
            }
            None => sha256(b""),
        };
        let string_to_sign = signer.chunk_string(&hash);
        signer.check(&string_to_sign, &self.chunk_signature)
    }

    fn add_trailer(&mut self, line: &[u8]) -> Result<(), BodyError> {
        let (name, value) = line
            .iter()
            .position(|&b| b == b':')
            .map(|at| (&line[..at], &line[at + 1..]))
            .ok_or_else(|| BodyError::trailer("a trailer line has no colon"))?;
        let name = HeaderName::from_bytes(&name.to_ascii_lowercase())
            .map_err(|_| BodyError::trailer("a trailer name is not valid"))?;
        let value = HeaderValue::from_bytes(value.trim_ascii())
            .map_err(|_| BodyError::trailer("a trailer value is not valid"))?;
        if name == TRAILER_SIGNATURE {
            if self.signer.is_none() || self.trailer_signature.is_some() {
                return Err(BodyError::trailer("unexpected trailer signature"));
            }
            self.trailer_signature = Some(
                decode_signature(value.as_bytes())
                    .ok_or_else(|| BodyError::trailer("the trailer signature is malformed"))?,
            );
            return Ok(());
        }
        if !self.expected.contains(&name) {
            return Err(BodyError::trailer(format!(
                "the trailer {name} is not declared in x-amz-trailer"
            )));
        }
        if self.trailers.insert(name, value).is_some() {
            return Err(BodyError::trailer("a trailer appears more than once"));
        }
        Ok(())
    }

    /// Checks the trailer section once its empty line arrives.
    fn end_trailers(&mut self) -> Result<(), BodyError> {
        if self.trailers.len() != self.expected.len() {
            return Err(BodyError::trailer(
                "the body lacks a trailer that x-amz-trailer declares",
            ));
        }
        let Some(signer) = self.signer.as_mut().filter(|_| self.trailer) else {
            return match self.trailer_signature {
                Some(_) => Err(BodyError::trailer("unexpected trailer signature")),
                None => Ok(()),
            };
        };
        if self.trailers.is_empty() && self.trailer_signature.is_none() {
            return Ok(());
        }
        let signature = self
            .trailer_signature
            .ok_or_else(|| BodyError::trailer("the trailers are not signed"))?;
        let string_to_sign = signer.trailer_string(&canonical_trailers(&self.trailers));
        signer.check(&string_to_sign, &signature)
    }
}

/// The canonical form of trailers that their signature covers:
/// `name:value\n` for each, sorted by name.
pub(crate) fn canonical_trailers(trailers: &HeaderMap) -> Vec<u8> {
    let mut sorted: Vec<_> = trailers.iter().collect();
    sorted.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let mut out = Vec::new();
    for (name, value) in sorted {
        out.extend_from_slice(name.as_str().as_bytes());
        out.push(b':');
        out.extend_from_slice(value.as_bytes());
        out.push(b'\n');
    }
    out
}

/// A request body that decodes `aws-chunked` framing as it is read.
pub(crate) struct AwsChunkedBody {
    inner: Body,
    pending: Bytes,
    decoder: Decoder,
    trailers: Trailers,
    /// Decoded bytes not yet yielded.
    left: u64,
    inner_ended: bool,
    finished: bool,
}

impl AwsChunkedBody {
    pub(crate) fn new(
        inner: Body,
        decoder: Decoder,
        decoded_length: u64,
        trailers: Trailers,
    ) -> Self {
        Self {
            inner,
            pending: Bytes::new(),
            decoder,
            trailers,
            left: decoded_length,
            inner_ended: false,
            finished: false,
        }
    }

    fn fail(&mut self, error: impl Into<StdError>) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
        self.finished = true;
        Poll::Ready(Some(Err(error.into())))
    }
}

impl http_body::Body for AwsChunkedBody {
    type Data = Bytes;
    type Error = StdError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        loop {
            match this.decoder.decode(&mut this.pending) {
                Ok(Some(data)) => {
                    this.left -= data.len() as u64;
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Ok(None) => {}
                Err(error) => return this.fail(error),
            }
            if this.inner_ended {
                return match this.decoder.finish() {
                    Ok(trailers) => {
                        this.trailers.set(trailers);
                        this.finished = true;
                        Poll::Ready(None)
                    }
                    Err(error) => this.fail(error),
                };
            }
            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        this.pending = data;
                    }
                }
                Some(Err(error)) => return this.fail(error),
                None => this.inner_ended = true,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.left)
    }
}

/// Encodes `data` in chunks of the given sizes, signed when `signer`
/// is given, with `trailers` at the end: the encoder of tests and fuzz
/// targets.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn encode(
    data: &[u8],
    sizes: &[usize],
    mut signer: Option<ChunkSigner>,
    trailers: &HeaderMap,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = data;
    let mut sizes = sizes.iter().copied().filter(|&s| s > 0).cycle();
    loop {
        let size = sizes.next().unwrap_or(rest.len()).min(rest.len());
        let (chunk, tail) = rest.split_at(size);
        rest = tail;
        out.extend_from_slice(format!("{size:x}").as_bytes());
        if let Some(signer) = &mut signer {
            let signature = hex(&signer.sign_chunk(chunk));
            out.extend_from_slice(format!(";chunk-signature={signature}").as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        if size == 0 {
            break;
        }
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    for (name, value) in trailers {
        out.extend_from_slice(format!("{name}:").as_bytes());
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if let Some(signer) = &mut signer
        && !trailers.is_empty()
    {
        let signature = signer.sign_trailers(&canonical_trailers(trailers));
        let line = format!("{TRAILER_SIGNATURE}:{}\r\n", hex(&signature));
        out.extend_from_slice(line.as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use proptest::prelude::*;

    use super::*;
    use crate::sigv4::canonical::signing_key;

    const SECRET: &[u8] = b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const TIMESTAMP: &str = "20130524T000000Z";
    const SCOPE: &str = "20130524/us-east-1/s3/aws4_request";

    fn signer(seed: &str) -> ChunkSigner {
        let key = signing_key(SECRET, "20130524", "us-east-1", "s3");
        ChunkSigner::new(
            key,
            TIMESTAMP,
            SCOPE,
            decode_signature(seed.as_bytes()).unwrap(),
        )
    }

    fn crc32c() -> Vec<HeaderName> {
        vec![HeaderName::from_static("x-amz-checksum-crc32c")]
    }

    /// Decodes `body` delivered in pieces of `split` bytes.
    fn decode_split(
        mut decoder: Decoder,
        body: &[u8],
        split: usize,
    ) -> Result<(Vec<u8>, HeaderMap), BodyError> {
        let mut out = Vec::new();
        for piece in body.chunks(split.max(1)) {
            let mut input = Bytes::copy_from_slice(piece);
            while let Some(data) = decoder.decode(&mut input)? {
                out.extend_from_slice(&data);
            }
            assert!(input.is_empty());
        }
        let trailers = decoder.finish()?;
        Ok((out, trailers))
    }

    /// The streaming example of
    /// <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html>:
    /// 65,536 and 1,024 bytes of `a`.
    fn documented_body(signatures: [&str; 3], trailer: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("10000;chunk-signature={}\r\n", signatures[0]).as_bytes());
        body.extend_from_slice(&[b'a'; 65_536]);
        body.extend_from_slice(format!("\r\n400;chunk-signature={}\r\n", signatures[1]).as_bytes());
        body.extend_from_slice(&[b'a'; 1024]);
        body.extend_from_slice(format!("\r\n0;chunk-signature={}\r\n", signatures[2]).as_bytes());
        body.extend_from_slice(trailer);
        body.extend_from_slice(b"\r\n");
        body
    }

    const SEED: &str = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9";
    const SIGNATURES: [&str; 3] = [
        "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
        "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
        "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
    ];
    const TRAILER_SEED: &str = "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e";
    const TRAILER_SIGNATURES: [&str; 3] = [
        "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2",
        "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7",
        "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992",
    ];
    const TRAILER: &[u8] = b"x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:\
        d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n";

    #[test]
    fn the_documented_signed_body_decodes() {
        let body = documented_body(SIGNATURES, b"");
        for split in [1, 7, 100, 65_536, body.len()] {
            let decoder = Decoder::new(Some(signer(SEED)), false, Vec::new(), 66_560);
            let (data, trailers) = decode_split(decoder, &body, split).unwrap();
            assert_eq!(data, vec![b'a'; 66_560]);
            assert!(trailers.is_empty());
        }
        // A wrong seed breaks the first chunk.
        let decoder = Decoder::new(Some(signer(TRAILER_SEED)), false, Vec::new(), 66_560);
        let error = decode_split(decoder, &body, 4096).unwrap_err();
        assert_eq!(
            *error.to_s3_error().code(),
            s3s::S3ErrorCode::SignatureDoesNotMatch
        );
    }

    #[test]
    fn the_documented_trailer_body_decodes() {
        let body = documented_body(TRAILER_SIGNATURES, TRAILER);
        let decoder = Decoder::new(Some(signer(TRAILER_SEED)), true, crc32c(), 66_560);
        let (data, trailers) = decode_split(decoder, &body, 999).unwrap();
        assert_eq!(data.len(), 66_560);
        assert_eq!(trailers.len(), 1);
        assert_eq!(trailers["x-amz-checksum-crc32c"], "sOO8/Q==");
        // Changing the checksum breaks the trailer signature.
        let forged = documented_body(
            TRAILER_SIGNATURES,
            &TRAILER
                .iter()
                .map(|&b| if b == b'Q' { b'R' } else { b })
                .collect::<Vec<_>>(),
        );
        let decoder = Decoder::new(Some(signer(TRAILER_SEED)), true, crc32c(), 66_560);
        let error = decode_split(decoder, &forged, 999).unwrap_err();
        assert_eq!(
            *error.to_s3_error().code(),
            s3s::S3ErrorCode::SignatureDoesNotMatch
        );
    }

    #[test]
    fn unsigned_trailer_bodies_decode() {
        // As the AWS SDK for Rust writes them.
        let body = b"A\r\n1234567890\r\n5\r\n12345\r\n0\r\nx-amz-checksum-crc32:78DeVw==\r\n\r\n";
        let crc32 = vec![HeaderName::from_static("x-amz-checksum-crc32")];
        let decoder = Decoder::new(None, true, crc32, 15);
        let (data, trailers) = decode_split(decoder, body, 3).unwrap();
        assert_eq!(data, b"123456789012345");
        assert_eq!(trailers["x-amz-checksum-crc32"], "78DeVw==");
    }

    fn error(signed: bool, expected: Vec<HeaderName>, length: u64, body: &[u8]) -> String {
        let signer = signed.then(|| signer(SEED));
        let trailer = !expected.is_empty();
        let decoder = Decoder::new(signer, trailer, expected, length);
        decode_split(decoder, body, 5).unwrap_err().to_string()
    }

    #[test]
    fn malformed_bodies_are_refused() {
        let crc32 = || vec![HeaderName::from_static("x-amz-checksum-crc32")];
        for (body, length, why) in [
            (&b"5\r\nabcde\r\n0\r\n\r\n"[..], 4, "more data"),
            (b"5\r\nabcde\r\n0\r\n\r\n", 6, "less data"),
            (b"5\r\nabcde\r\n", 5, "ended before"),
            (b"5\r\nabcdeXY0\r\n\r\n", 5, "not followed by CRLF"),
            (b"5\nabcde\r\n0\r\n\r\n", 5, "CRLF"),
            (b"g\r\n", 5, "not hexadecimal"),
            (b"\r\n", 5, "not hexadecimal"),
            (
                b"00000000000000005\r\nabcde\r\n0\r\n\r\n",
                5,
                "not hexadecimal",
            ),
            (b"5;x=y\r\nabcde\r\n0\r\n\r\n", 5, "extension"),
            (b"0\r\n\r\nextra", 0, "follows the final chunk"),
            (
                b"0\r\nx-amz-checksum-crc64nvme:AAAA\r\n\r\n",
                0,
                "not declared",
            ),
        ] {
            let message = error(false, Vec::new(), length, body);
            assert!(message.contains(why), "{body:?}: {message}");
        }
        let long = format!("{}\r\n", "0".repeat(MAX_CHUNK_LINE_BYTES));
        assert!(error(false, Vec::new(), 5, long.as_bytes()).contains("longer than"));
        for (body, why) in [
            (
                &b"0\r\nx-amz-checksum-crc32:AAAA\r\nx-amz-checksum-crc32:AAAA\r\n\r\n"[..],
                "more than once",
            ),
            (b"0\r\n\r\n", "lacks a trailer"),
            (b"0\r\nx-amz-checksum-crc32\r\n\r\n", "no colon"),
            (b"0\r\nx amz:1\r\n\r\n", "name is not valid"),
            (
                b"0\r\nx-amz-checksum-crc32:\x01\r\n\r\n",
                "value is not valid",
            ),
            (
                b"0\r\nx-amz-checksum-crc32:AAAA\r\nx-amz-trailer-signature:00\r\n\r\n",
                "unexpected trailer signature",
            ),
        ] {
            let message = error(false, crc32(), 0, body);
            assert!(message.contains(why), "{body:?}: {message}");
        }
        let huge = format!(
            "0\r\nx-amz-checksum-crc32:{}\r\n\r\n",
            "A".repeat(MAX_TRAILER_BYTES)
        );
        assert!(error(false, crc32(), 0, huge.as_bytes()).contains("longer than"));
        for (body, why) in [
            (&b"5\r\nabcde\r\n"[..], "has no signature"),
            (
                b"5;chunk-signature=00\r\nabcde\r\n",
                "signature is malformed",
            ),
            (b"5;chunk-sig=00\r\nabcde\r\n", "signature is malformed"),
        ] {
            let message = error(true, Vec::new(), 5, body);
            assert!(message.contains(why), "{body:?}: {message}");
        }
    }

    #[test]
    fn signed_trailers_need_their_signature() {
        let mut signer = signer(SEED);
        let final_chunk = hex(&signer.sign_chunk(b""));
        let body = format!("0;chunk-signature={final_chunk}\r\nx-amz-checksum-crc32c:AAAA\r\n\r\n");
        let decoder = Decoder::new(Some(self::signer(SEED)), true, crc32c(), 0);
        let error = decode_split(decoder, body.as_bytes(), 4).unwrap_err();
        assert!(error.to_string().contains("not signed"), "{error}");
        // Signed chunks without trailers take no trailer signature.
        let body = format!(
            "0;chunk-signature={final_chunk}\r\nx-amz-trailer-signature:{final_chunk}\r\n\r\n"
        );
        let decoder = Decoder::new(Some(self::signer(SEED)), false, Vec::new(), 0);
        let error = decode_split(decoder, body.as_bytes(), 4).unwrap_err();
        assert!(error.to_string().contains("unexpected"), "{error}");
        // A trailer mode with nothing declared needs no trailer signature.
        let body = format!("0;chunk-signature={final_chunk}\r\n\r\n");
        let decoder = Decoder::new(Some(self::signer(SEED)), true, Vec::new(), 0);
        decode_split(decoder, body.as_bytes(), 4).unwrap();
    }

    #[tokio::test]
    async fn bodies_decode_as_they_stream() {
        let body = documented_body(TRAILER_SIGNATURES, TRAILER);
        let stream = futures_body(&body, 1000);
        let trailers = Trailers::default();
        let decoder = Decoder::new(Some(signer(TRAILER_SEED)), true, crc32c(), 66_560);
        let mut decoded = AwsChunkedBody::new(stream, decoder, 66_560, trailers.clone());
        assert_eq!(http_body::Body::size_hint(&decoded).exact(), Some(66_560));
        let mut data = Vec::new();
        while let Some(frame) = decoded.frame().await {
            assert!(trailers.get().is_none());
            data.extend_from_slice(&frame.unwrap().into_data().unwrap());
        }
        assert!(http_body::Body::is_end_stream(&decoded));
        assert_eq!(data.len(), 66_560);
        assert_eq!(trailers.get().unwrap()["x-amz-checksum-crc32c"], "sOO8/Q==");
        // A truncated body fails, once.
        let stream = futures_body(&body[..body.len() - 2], 1000);
        let decoder = Decoder::new(Some(signer(TRAILER_SEED)), true, crc32c(), 66_560);
        let mut decoded = AwsChunkedBody::new(stream, decoder, 66_560, Trailers::default());
        let error = loop {
            match decoded.frame().await.unwrap() {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        let error = error.downcast::<BodyError>().unwrap();
        assert_eq!(
            *error.to_s3_error().code(),
            s3s::S3ErrorCode::IncompleteBody
        );
        assert!(decoded.frame().await.is_none());
    }

    /// A body that delivers `data` in frames of `size` bytes.
    fn futures_body(data: &[u8], size: usize) -> Body {
        Body::http_body(Frames(
            data.chunks(size).map(Bytes::copy_from_slice).collect(),
        ))
    }

    struct Frames(std::collections::VecDeque<Bytes>);

    impl http_body::Body for Frames {
        type Data = Bytes;
        type Error = StdError;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
            Poll::Ready(self.0.pop_front().map(|data| Ok(Frame::data(data))))
        }
    }

    proptest! {
        #[test]
        fn encoded_bodies_round_trip(
            data in proptest::collection::vec(any::<u8>(), 0..2000),
            sizes in proptest::collection::vec(1_usize..700, 1..5),
            split in 1_usize..300,
            signed in any::<bool>(),
            with_trailer in any::<bool>(),
        ) {
            let mut trailers = HeaderMap::new();
            if with_trailer {
                trailers.insert("x-amz-checksum-crc32c", HeaderValue::from_static("sOO8/Q=="));
            }
            let expected: Vec<HeaderName> = trailers.keys().cloned().collect();
            let body = encode(&data, &sizes, signed.then(|| signer(SEED)), &trailers);
            let decoder = Decoder::new(signed.then(|| signer(SEED)), with_trailer, expected, data.len() as u64);
            let (decoded, got) = decode_split(decoder, &body, split).unwrap();
            prop_assert_eq!(decoded, data);
            prop_assert_eq!(got, trailers);
        }

        #[test]
        fn signed_bodies_never_decode_to_other_data(
            data in proptest::collection::vec(any::<u8>(), 1..600),
            sizes in proptest::collection::vec(1_usize..200, 1..4),
            at in any::<proptest::sample::Index>(),
            flip in 1_u8..=255,
        ) {
            let mut trailers = HeaderMap::new();
            trailers.insert("x-amz-checksum-crc32c", HeaderValue::from_static("sOO8/Q=="));
            let mut body = encode(&data, &sizes, Some(signer(TRAILER_SEED)), &trailers);
            let at = at.index(body.len());
            body[at] ^= flip;
            let decoder = Decoder::new(Some(signer(TRAILER_SEED)), true, crc32c(), data.len() as u64);
            if let Ok((decoded, got)) = decode_split(decoder, &body, 64) {
                // Only a change of case in a hexadecimal size or a trailer
                // name can survive, and it changes nothing.
                prop_assert_eq!(decoded, data);
                prop_assert_eq!(got, trailers);
            }
        }

        #[test]
        fn arbitrary_bodies_never_panic(
            body in proptest::collection::vec(any::<u8>(), 0..400),
            length in 0_u64..400,
            signed in any::<bool>(),
            split in 1_usize..50,
        ) {
            let signer = signed.then(|| signer(SEED));
            let decoder = Decoder::new(signer, true, crc32c(), length);
            if let Ok((decoded, _)) = decode_split(decoder, &body, split) {
                prop_assert_eq!(decoded.len() as u64, length);
            }
        }
    }
}
