//! The part of gRPC the etcd backend needs: unary calls and one
//! server-streaming watch over HTTP/2, on `hyper`'s HTTP/2 client.
//!
//! A gRPC message on the wire is a 5-byte prefix, a compression flag and
//! a big-endian length, followed by the Protobuf encoding. The call's
//! status arrives in the `grpc-status` and `grpc-message` trailers, or in
//! the headers of a response without a body. The backend never asks for
//! compression, so a compressed message is a protocol error, and the
//! length is checked against [`MAX_MESSAGE_LEN`] before anything is
//! buffered for it.

use std::convert::Infallible;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::header::{CONTENT_TYPE, HeaderMap, TE};
use http::{Method, Request, StatusCode, Uri};
use http_body::{Body, Frame};
use hyper::body::Incoming;
use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use prost::Message;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio_rustls::TlsConnector;

/// The longest message the backend accepts, in bytes. Register values are
/// small, and listings are paged, so etcd's answers stay far below it.
pub(crate) const MAX_MESSAGE_LEN: usize = 4 << 20;

/// The length of a message's prefix: the compression flag and the length.
pub(crate) const PREFIX_LEN: usize = 5;

/// How often an HTTP/2 connection with a call in progress, such as a watch,
/// is pinged, and how long the answer may take before the connection is
/// dropped. etcd refuses pings more often than every 5 seconds.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// A gRPC status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Code(pub(crate) u32);

impl Code {
    pub(crate) const OK: Self = Self(0);
    pub(crate) const CANCELLED: Self = Self(1);
    pub(crate) const UNKNOWN: Self = Self(2);
    pub(crate) const DEADLINE_EXCEEDED: Self = Self(4);
    pub(crate) const RESOURCE_EXHAUSTED: Self = Self(8);
    pub(crate) const ABORTED: Self = Self(10);
    pub(crate) const OUT_OF_RANGE: Self = Self(11);
    pub(crate) const INTERNAL: Self = Self(13);
    pub(crate) const UNAVAILABLE: Self = Self(14);

    /// Whether a request answered with this code may have been applied,
    /// or may still be: etcd answers `UNAVAILABLE` to a proposal that timed
    /// out, which can still commit.
    pub(crate) fn may_have_applied(self) -> bool {
        matches!(
            self,
            Self::CANCELLED
                | Self::UNKNOWN
                | Self::DEADLINE_EXCEEDED
                | Self::ABORTED
                | Self::INTERNAL
                | Self::UNAVAILABLE
        )
    }

    /// Whether the same request may succeed if sent again, perhaps to
    /// another member.
    pub(crate) fn is_transient(self) -> bool {
        self.may_have_applied() || self == Self::RESOURCE_EXHAUSTED
    }

    /// The code's name in the gRPC specification.
    fn name(self) -> &'static str {
        const NAMES: [&str; 17] = [
            "OK",
            "CANCELLED",
            "UNKNOWN",
            "INVALID_ARGUMENT",
            "DEADLINE_EXCEEDED",
            "NOT_FOUND",
            "ALREADY_EXISTS",
            "PERMISSION_DENIED",
            "RESOURCE_EXHAUSTED",
            "FAILED_PRECONDITION",
            "ABORTED",
            "OUT_OF_RANGE",
            "UNIMPLEMENTED",
            "INTERNAL",
            "UNAVAILABLE",
            "DATA_LOSS",
            "UNAUTHENTICATED",
        ];
        usize::try_from(self.0)
            .ok()
            .and_then(|code| NAMES.get(code))
            .copied()
            .unwrap_or("an unknown code")
    }
}

/// The status a call ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Status {
    pub(crate) code: Code,
    pub(crate) message: String,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.code.name(), self.code.0)?;
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        Ok(())
    }
}

/// Reads the status from headers or trailers, or `None` if they carry
/// none. The message is percent-decoded, as the gRPC specification
/// requires; a code that is not a number is [`Code::UNKNOWN`].
pub(crate) fn status_of(headers: &HeaderMap) -> Option<Status> {
    let code = headers.get("grpc-status")?;
    let code = code
        .to_str()
        .ok()
        .and_then(|code| code.parse().ok())
        .map_or(Code::UNKNOWN, Code);
    let message = headers
        .get("grpc-message")
        .map(|message| percent_decode(message.as_bytes()))
        .unwrap_or_default();
    Some(Status { code, message })
}

fn percent_decode(text: &[u8]) -> String {
    let mut decoded = Vec::with_capacity(text.len());
    let mut rest = text;
    while let Some((&byte, tail)) = rest.split_first() {
        let escaped = match tail {
            [high, low, ..] if byte == b'%' => {
                let hex = |b: u8| char::from(b).to_digit(16);
                hex(*high).zip(hex(*low)).map(|(h, l)| h * 16 + l)
            }
            _ => None,
        };
        match escaped.and_then(|value| u8::try_from(value).ok()) {
            Some(value) => {
                decoded.push(value);
                rest = &tail[2..];
            }
            None => {
                decoded.push(byte);
                rest = tail;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// A call that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallError {
    /// The request was never sent: no endpoint could be reached.
    NotSent(String),
    /// The request may have reached etcd, but no complete answer came
    /// back: a timeout, a broken connection, or a malformed answer.
    NoAnswer(String),
    /// etcd answered with an error status.
    Status(Status),
}

impl CallError {
    /// Whether the call should move to another endpoint: it got no answer,
    /// or a transient status, which a member cut off from its quorum gives
    /// to every linearizable request ("no leader", "request timed out").
    fn leaves_endpoint(&self) -> bool {
        match self {
            Self::NoAnswer(_) => true,
            Self::Status(status) => status.code.is_transient(),
            Self::NotSent(_) => false,
        }
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSent(reason) => write!(f, "cannot reach etcd: {reason}"),
            Self::NoAnswer(reason) => write!(f, "no answer from etcd: {reason}"),
            Self::Status(status) => write!(f, "etcd answered {status}"),
        }
    }
}

/// A malformed message stream.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The compression flag was set, though no compression was asked for.
    #[error("a gRPC message is compressed (flag {0})")]
    Compressed(u8),
    /// A message is longer than [`MAX_MESSAGE_LEN`].
    #[error("a gRPC message of {0} bytes is over the limit")]
    TooLong(usize),
    /// The stream ended inside a message.
    #[error("the stream ended inside a gRPC message")]
    Truncated,
}

/// Splits a byte stream into gRPC messages.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buffer: BytesMut,
}

impl FrameDecoder {
    /// Adds bytes received.
    ///
    /// # Errors
    ///
    /// A [`FrameError`] if the next message's prefix is invalid, before its
    /// body is buffered.
    pub fn push(&mut self, data: &[u8]) -> Result<(), FrameError> {
        self.buffer.extend_from_slice(data);
        self.check_prefix().map(|_| ())
    }

    /// The length of the next message, once its prefix is complete.
    fn check_prefix(&self) -> Result<Option<usize>, FrameError> {
        let Some(prefix) = self.buffer.get(..PREFIX_LEN) else {
            return Ok(None);
        };
        if prefix[0] != 0 {
            return Err(FrameError::Compressed(prefix[0]));
        }
        let len = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]);
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if len > MAX_MESSAGE_LEN {
            return Err(FrameError::TooLong(len));
        }
        Ok(Some(len))
    }

    /// The next complete message, if one is buffered.
    ///
    /// # Errors
    ///
    /// As [`FrameDecoder::push`].
    pub fn next_message(&mut self) -> Result<Option<Bytes>, FrameError> {
        let Some(len) = self.check_prefix()? else {
            return Ok(None);
        };
        if self.buffer.len() < PREFIX_LEN + len {
            return Ok(None);
        }
        self.buffer.advance(PREFIX_LEN);
        Ok(Some(self.buffer.split_to(len).freeze()))
    }

    /// Checks that the stream did not end inside a message.
    ///
    /// # Errors
    ///
    /// [`FrameError::Truncated`] if bytes are left over.
    pub fn finish(&self) -> Result<(), FrameError> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(FrameError::Truncated)
        }
    }
}

/// Encodes `message` with its gRPC prefix.
pub(crate) fn encode_frame(message: &impl Message) -> Bytes {
    let len = message.encoded_len();
    let mut frame = BytesMut::with_capacity(PREFIX_LEN + len);
    frame.put_u8(0);
    // Requests are register values and keys, far below 4 GiB.
    frame.put_u32(u32::try_from(len).unwrap_or(u32::MAX));
    message
        .encode(&mut frame)
        .expect("the buffer has room for the message");
    frame.freeze()
}

/// A request body: one message, then either the end of the stream or,
/// for a stream that stays open, nothing until the sender of `open` is
/// dropped.
pub(crate) struct RequestBody {
    message: Option<Bytes>,
    open: Option<oneshot::Receiver<()>>,
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(message) = self.message.take() {
            return Poll::Ready(Some(Ok(Frame::data(message))));
        }
        if let Some(open) = &mut self.open {
            if Pin::new(open).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.open = None;
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.message.is_none() && self.open.is_none()
    }
}

/// An etcd client URL: `http://` or `https://`, a host, and a port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    tls: bool,
    host: String,
    port: u16,
}

impl Endpoint {
    /// etcd's client port, for a URL without one.
    const DEFAULT_PORT: u16 = 2379;

    /// Parses a client URL without a path.
    pub(crate) fn parse(url: &str) -> Result<Self, String> {
        let uri: Uri = url
            .parse()
            .map_err(|error| format!("{url:?} is not a URL: {error}"))?;
        let tls = match uri.scheme_str() {
            Some("http") => false,
            Some("https") => true,
            _ => return Err(format!("{url:?} must start with http:// or https://")),
        };
        if !matches!(uri.path(), "" | "/") || uri.query().is_some() {
            return Err(format!("{url:?} must not have a path"));
        }
        let host = uri.host().unwrap_or_default();
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        if host.is_empty() {
            return Err(format!("{url:?} has no host"));
        }
        Ok(Self {
            tls,
            host: host.to_owned(),
            port: uri.port_u16().unwrap_or(Self::DEFAULT_PORT),
        })
    }

    /// Whether the endpoint speaks TLS.
    pub(crate) fn tls(&self) -> bool {
        self.tls
    }

    fn uri(&self, path: &str) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        if self.host.contains(':') {
            format!("{scheme}://[{}]:{}{path}", self.host, self.port)
        } else {
            format!("{scheme}://{}:{}{path}", self.host, self.port)
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri(""))
    }
}

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

type Sender = http2::SendRequest<RequestBody>;

/// Connections to an etcd cluster: one HTTP/2 connection at a time, to
/// the first endpoint that accepts one, multiplexing every call.
///
/// A call without an answer, or with a transient status, drops the
/// connection, and the next call connects again, starting from the next
/// endpoint, so a member that stops answering, or is cut off from its
/// quorum and answers `UNAVAILABLE`, is left behind.
pub(crate) struct Channel {
    endpoints: Vec<Endpoint>,
    tls: Option<TlsConnector>,
    timeout: Duration,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The connection and the index of its endpoint.
    connection: Option<(usize, Sender)>,
    /// The endpoint to try first.
    next: usize,
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("endpoints", &self.endpoints)
            .field("tls", &self.tls.is_some())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Channel {
    /// A channel to `endpoints`, with `tls` for the `https://` ones. Each
    /// connection attempt and each call is bounded by `timeout`.
    pub(crate) fn new(
        endpoints: Vec<Endpoint>,
        tls: Option<Arc<rustls::ClientConfig>>,
        timeout: Duration,
    ) -> Self {
        let tls = tls.map(|config| {
            let mut config = Arc::unwrap_or_clone(config);
            config.alpn_protocols = vec![b"h2".to_vec()];
            TlsConnector::from(Arc::new(config))
        });
        Self {
            endpoints,
            tls,
            timeout,
            state: Mutex::default(),
        }
    }

    /// The connection, made if there is none.
    async fn connection(&self) -> Result<(usize, Sender), CallError> {
        let mut state = self.state.lock().await;
        if let Some((index, sender)) = &state.connection
            && !sender.is_closed()
        {
            return Ok((*index, sender.clone()));
        }
        state.connection = None;
        let mut failures = Vec::new();
        for attempt in 0..self.endpoints.len() {
            let index = (state.next + attempt) % self.endpoints.len();
            let endpoint = &self.endpoints[index];
            match tokio::time::timeout(self.timeout, self.connect(endpoint)).await {
                Ok(Ok(sender)) => {
                    state.connection = Some((index, sender.clone()));
                    state.next = index;
                    return Ok((index, sender));
                }
                Ok(Err(error)) => failures.push(format!("{endpoint}: {error}")),
                Err(_) => failures.push(format!(
                    "{endpoint}: no connection within {:?}",
                    self.timeout
                )),
            }
        }
        Err(CallError::NotSent(failures.join("; ")))
    }

    async fn connect(&self, endpoint: &Endpoint) -> Result<Sender, String> {
        let tcp = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(|error| error.to_string())?;
        tcp.set_nodelay(true).map_err(|error| error.to_string())?;
        let io: Box<dyn Io> = if endpoint.tls {
            let tls = self
                .tls
                .as_ref()
                .ok_or("an https endpoint needs a TLS configuration")?;
            let name = ServerName::try_from(endpoint.host.clone())
                .map_err(|error| format!("invalid server name: {error}"))?;
            Box::new(
                tls.connect(name, tcp)
                    .await
                    .map_err(|error| format!("TLS handshake failed: {error}"))?,
            )
        } else {
            Box::new(tcp)
        };
        let (sender, connection) = http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .handshake(TokioIo::new(io))
            .await
            .map_err(|error| format!("HTTP/2 handshake failed: {error}"))?;
        // The connection ends when every sender is dropped or it fails;
        // its error reaches the calls in progress.
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(sender)
    }

    /// Drops the connection to endpoint `index` after a call through it
    /// went unanswered or failed transiently, so the next call, such as
    /// the retry or the re-read of the lost-response rule, tries the next
    /// endpoint.
    async fn abandon(&self, index: usize) {
        let mut state = self.state.lock().await;
        if matches!(&state.connection, Some((current, _)) if *current == index) {
            state.connection = None;
            state.next = (index + 1) % self.endpoints.len();
        }
    }

    fn request(&self, endpoint: &Endpoint, path: &str, body: RequestBody) -> Request<RequestBody> {
        let mut request = Request::new(body);
        *request.method_mut() = Method::POST;
        *request.uri_mut() = endpoint
            .uri(path)
            .parse()
            .expect("an endpoint and a method path form a URI");
        let headers = request.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        headers.insert(TE, http::HeaderValue::from_static("trailers"));
        request
    }

    /// Calls a unary method.
    pub(crate) async fn unary<R: Message + Default>(
        &self,
        path: &str,
        request: &impl Message,
    ) -> Result<R, CallError> {
        let (index, mut sender) = self.connection().await?;
        let body = RequestBody {
            message: Some(encode_frame(request)),
            open: None,
        };
        let mut request = self.request(&self.endpoints[index], path, body);
        let millis = self.timeout.as_millis().clamp(1, 99_999_999);
        request.headers_mut().insert(
            "grpc-timeout",
            http::HeaderValue::from_str(&format!("{millis}m")).expect("digits are a header value"),
        );
        let call = async {
            let response = sender
                .send_request(request)
                .await
                .map_err(|error| CallError::NoAnswer(error.to_string()))?;
            let mut stream = Streaming::new(response, None)?;
            let message = stream.message().await?;
            stream.end().await?;
            Ok(message)
        };
        let result = match tokio::time::timeout(self.timeout, call).await {
            Ok(result) => result,
            Err(_) => Err(CallError::NoAnswer(format!(
                "no answer within {:?}",
                self.timeout
            ))),
        };
        if result.as_ref().is_err_and(CallError::leaves_endpoint) {
            self.abandon(index).await;
        }
        result
    }

    /// Opens a stream with one request, which stays open until the
    /// [`Streaming`] is dropped.
    pub(crate) async fn open(
        &self,
        path: &str,
        request: &impl Message,
    ) -> Result<Streaming, CallError> {
        let (index, mut sender) = self.connection().await?;
        let (open, closed) = oneshot::channel();
        let body = RequestBody {
            message: Some(encode_frame(request)),
            open: Some(closed),
        };
        let request = self.request(&self.endpoints[index], path, body);
        let result = match tokio::time::timeout(self.timeout, sender.send_request(request)).await {
            Ok(Ok(response)) => Streaming::new(response, Some(open)),
            Ok(Err(error)) => Err(CallError::NoAnswer(error.to_string())),
            Err(_) => Err(CallError::NoAnswer(format!(
                "no answer within {:?}",
                self.timeout
            ))),
        };
        if result.as_ref().is_err_and(CallError::leaves_endpoint) {
            self.abandon(index).await;
        }
        result
    }
}

/// The messages of a response.
pub(crate) struct Streaming {
    body: Incoming,
    frames: FrameDecoder,
    /// The status from the trailers, once they arrived.
    ended: Option<Status>,
    /// Keeps the request stream open while the response is read.
    _open: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for Streaming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Streaming")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl Streaming {
    /// Checks a response's head: HTTP 200, and no error status in a
    /// response without a body.
    fn new(
        response: http::Response<Incoming>,
        open: Option<oneshot::Sender<()>>,
    ) -> Result<Self, CallError> {
        let (parts, body) = response.into_parts();
        if parts.status != StatusCode::OK {
            return Err(CallError::NoAnswer(format!("HTTP status {}", parts.status)));
        }
        let ended = status_of(&parts.headers);
        if let Some(status) = &ended
            && status.code != Code::OK
        {
            return Err(CallError::Status(status.clone()));
        }
        Ok(Self {
            body,
            frames: FrameDecoder::default(),
            ended,
            _open: open,
        })
    }

    /// The next message.
    ///
    /// # Errors
    ///
    /// The status of a call that ended with an error, or
    /// [`CallError::NoAnswer`] for a stream that ended without one, or
    /// broke.
    pub(crate) async fn message<R: Message + Default>(&mut self) -> Result<R, CallError> {
        loop {
            if let Some(message) = self.frames.next_message().map_err(malformed)? {
                return R::decode(message).map_err(|error| CallError::NoAnswer(error.to_string()));
            }
            if let Some(status) = &self.ended {
                return Err(match status.code {
                    Code::OK => CallError::NoAnswer("the stream ended".to_owned()),
                    _ => CallError::Status(status.clone()),
                });
            }
            self.receive().await?;
        }
    }

    /// Reads the rest of the stream, which must hold no further message,
    /// and checks that the call succeeded.
    async fn end(&mut self) -> Result<(), CallError> {
        while self.ended.is_none() {
            self.receive().await?;
        }
        if self.frames.next_message().map_err(malformed)?.is_some() {
            return Err(CallError::NoAnswer(
                "a unary call answered with several messages".to_owned(),
            ));
        }
        self.frames.finish().map_err(malformed)?;
        match self.ended.take() {
            Some(status) if status.code != Code::OK => Err(CallError::Status(status)),
            _ => Ok(()),
        }
    }

    /// Receives the next HTTP/2 frame.
    async fn receive(&mut self) -> Result<(), CallError> {
        let frame = poll_fn(|cx| Pin::new(&mut self.body).poll_frame(cx)).await;
        match frame {
            None => Err(CallError::NoAnswer(
                "the stream ended without a status".to_owned(),
            )),
            Some(Err(error)) => Err(CallError::NoAnswer(error.to_string())),
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => self.frames.push(&data).map_err(malformed),
                Err(frame) => {
                    let trailers = frame.into_trailers().unwrap_or_default();
                    self.ended = Some(status_of(&trailers).unwrap_or(Status {
                        code: Code::UNKNOWN,
                        message: "the trailers carry no status".to_owned(),
                    }));
                    Ok(())
                }
            },
        }
    }
}

fn malformed(error: FrameError) -> CallError {
    CallError::NoAnswer(error.to_string())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::etcd::proto::RangeResponse;

    fn frame(flag: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = vec![flag];
        frame.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    #[test]
    fn frames_are_split_from_a_byte_stream() {
        let mut decoder = FrameDecoder::default();
        let mut stream = frame(0, b"first");
        stream.extend(frame(0, b""));
        stream.extend(frame(0, b"third"));
        decoder.push(&stream[..3]).unwrap();
        assert_eq!(decoder.next_message().unwrap(), None);
        decoder.push(&stream[3..17]).unwrap();
        assert_eq!(decoder.next_message().unwrap().unwrap(), "first");
        assert_eq!(decoder.next_message().unwrap().unwrap(), "");
        assert_eq!(decoder.next_message().unwrap(), None);
        assert_eq!(decoder.finish(), Err(FrameError::Truncated));
        decoder.push(&stream[17..]).unwrap();
        assert_eq!(decoder.next_message().unwrap().unwrap(), "third");
        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn oversized_and_compressed_frames_are_refused_before_buffering() {
        let mut decoder = FrameDecoder::default();
        let len = u32::try_from(MAX_MESSAGE_LEN + 1).unwrap().to_be_bytes();
        let error = decoder
            .push(&[0, len[0], len[1], len[2], len[3]])
            .unwrap_err();
        assert_eq!(error, FrameError::TooLong(MAX_MESSAGE_LEN + 1));
        let mut decoder = FrameDecoder::default();
        assert_eq!(
            decoder.push(&frame(1, b"x")),
            Err(FrameError::Compressed(1))
        );
        assert_eq!(decoder.next_message(), Err(FrameError::Compressed(1)));
        let mut decoder = FrameDecoder::default();
        let at_limit = u32::try_from(MAX_MESSAGE_LEN).unwrap().to_be_bytes();
        decoder
            .push(&[0, at_limit[0], at_limit[1], at_limit[2], at_limit[3]])
            .unwrap();
        assert_eq!(decoder.next_message(), Ok(None));
    }

    #[test]
    fn encoded_frames_carry_the_prefix() {
        let response = RangeResponse {
            count: 3,
            ..RangeResponse::default()
        };
        let encoded = encode_frame(&response);
        assert_eq!(encoded.as_ref(), [0, 0, 0, 0, 2, 0x20, 3]);
        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded).unwrap();
        let message = decoder.next_message().unwrap().unwrap();
        assert_eq!(RangeResponse::decode(message).unwrap(), response);
    }

    #[test]
    fn statuses_are_read_and_percent_decoded() {
        let mut headers = HeaderMap::new();
        assert_eq!(status_of(&headers), None);
        headers.insert("grpc-status", "14".parse().unwrap());
        headers.insert(
            "grpc-message",
            "etcdserver:%20request%20timed%20out%2".parse().unwrap(),
        );
        let status = status_of(&headers).unwrap();
        assert_eq!(status.code, Code::UNAVAILABLE);
        assert_eq!(status.message, "etcdserver: request timed out%2");
        assert_eq!(
            status.to_string(),
            "UNAVAILABLE (14): etcdserver: request timed out%2"
        );
        headers.insert("grpc-status", "seven".parse().unwrap());
        headers.remove("grpc-message");
        let status = status_of(&headers).unwrap();
        assert_eq!(status.code, Code::UNKNOWN);
        assert_eq!(status.to_string(), "UNKNOWN (2)");
        assert_eq!(Code(99).name(), "an unknown code");
        assert_eq!(percent_decode(b"%e2%9c%93 %zz%"), "\u{2713} %zz%");
    }

    #[test]
    fn endpoints_parse_client_urls() {
        let endpoint = Endpoint::parse("https://etcd-1.example:2380").unwrap();
        assert!(endpoint.tls());
        assert_eq!(endpoint.to_string(), "https://etcd-1.example:2380");
        let endpoint = Endpoint::parse("http://10.0.0.1/").unwrap();
        assert!(!endpoint.tls());
        assert_eq!(endpoint.to_string(), "http://10.0.0.1:2379");
        let endpoint = Endpoint::parse("http://[::1]:23790").unwrap();
        assert_eq!(endpoint.host, "::1");
        assert_eq!(endpoint.uri("/x"), "http://[::1]:23790/x");
        for bad in [
            "etcd-1:2379",
            "ftp://etcd-1",
            "http://etcd-1:2379/v3",
            "http://etcd-1?x=1",
            "not a url",
            "http://:2379",
        ] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn call_errors_describe_themselves() {
        let status = Status {
            code: Code::OUT_OF_RANGE,
            message: "mvcc: required revision has been compacted".to_owned(),
        };
        assert_eq!(
            CallError::Status(status).to_string(),
            "etcd answered OUT_OF_RANGE (11): mvcc: required revision has been compacted"
        );
        assert_eq!(
            CallError::NotSent("refused".to_owned()).to_string(),
            "cannot reach etcd: refused"
        );
        assert_eq!(
            CallError::NoAnswer("timeout".to_owned()).to_string(),
            "no answer from etcd: timeout"
        );
    }

    proptest! {
        /// Messages framed and split at arbitrary points come back whole
        /// and in order.
        #[test]
        fn framed_messages_survive_any_split(
            messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..8),
            cuts in prop::collection::vec(any::<usize>(), 0..8),
        ) {
            let stream: Vec<u8> = messages.iter().flat_map(|m| frame(0, m)).collect();
            let mut cuts: Vec<usize> = cuts.into_iter().map(|c| c % (stream.len() + 1)).collect();
            cuts.push(stream.len());
            cuts.sort_unstable();
            let mut decoder = FrameDecoder::default();
            let mut decoded = Vec::new();
            let mut start = 0;
            for cut in cuts {
                decoder.push(&stream[start..cut]).unwrap();
                start = cut;
                while let Some(message) = decoder.next_message().unwrap() {
                    decoded.push(message.to_vec());
                }
            }
            prop_assert_eq!(decoded, messages);
            prop_assert_eq!(decoder.finish(), Ok(()));
        }
    }
}
