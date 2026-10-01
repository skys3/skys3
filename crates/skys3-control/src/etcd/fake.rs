//! An in-process stand-in for etcd's gRPC API, for tests that need no
//! etcd: the backend's error mapping, timeouts, endpoint failover, TLS,
//! and watches that etcd cancels or ends.
//!
//! It keeps one version of each key, like an etcd that compacts after
//! every write, and implements the requests the backend sends: `Range`
//! (of a key or a range, with a limit), `Txn` with one comparison, and
//! `Watch` of one key. Faults are queued for the next requests.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body::{Body, Frame};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::grpc::{FrameDecoder, encode_frame};
use super::proto::{
    self, CompareTarget, Event, EventType, KeyValue, RangeRequest, RangeResponse, Request,
    ResponseHeader, TargetUnion, TxnRequest, TxnResponse, WatchRequest, WatchRequestUnion,
    WatchResponse,
};

/// What the fake does with the next unary request.
#[derive(Debug, Clone)]
pub(super) enum Fault {
    /// Answers normally.
    Pass,
    /// Answers with this gRPC status in the headers, applying nothing.
    Status(u32),
    /// Applies the request, then answers with this status in the trailers.
    AppliedThenStatus(u32),
    /// Never answers, applying nothing.
    Hang,
    /// Applies the request and never answers.
    AppliedThenHang,
    /// Answers with a message prefix over the size limit.
    Oversized,
    /// Answers with a compressed message.
    Compressed,
    /// Answers with this HTTP status.
    Http(u16),
}

/// What the fake does with the next watch.
#[derive(Debug, Clone)]
pub(super) enum WatchFault {
    /// Confirms it, then cancels it as compacted.
    CompactedAfterCreate,
    /// Confirms it, then cancels it.
    CanceledAfterCreate,
    /// Confirms it, then ends the stream.
    EndedAfterCreate,
    /// Answers with a cancellation instead of a confirmation.
    Refused,
}

type Sender = mpsc::UnboundedSender<Frame<Bytes>>;

#[derive(Default)]
struct State {
    kvs: BTreeMap<Bytes, KeyValue>,
    revision: i64,
    faults: VecDeque<Fault>,
    watch_faults: VecDeque<WatchFault>,
    watchers: Vec<(Bytes, Sender)>,
    /// Streams that never end.
    held: Vec<Sender>,
    watches: usize,
    requests: usize,
}

/// A fake etcd member listening on a local port.
pub(super) struct Fake {
    state: Arc<Mutex<State>>,
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A response body fed through a channel.
struct ChannelBody(mpsc::UnboundedReceiver<Frame<Bytes>>);

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

fn trailers(code: u32) -> Frame<Bytes> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from(code));
    trailers.insert("grpc-message", HeaderValue::from_static("fake%20status"));
    Frame::trailers(trailers)
}

impl Fake {
    /// Starts a fake on a free local port, with TLS if `tls` is given.
    pub(super) async fn start(tls: Option<Arc<rustls::ServerConfig>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let acceptor = tls.map(TlsAcceptor::from);
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let (state, acceptor) = (Arc::clone(&shared), acceptor.clone());
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = Arc::clone(&state);
                        async move { Ok::<_, Infallible>(handle(&state, request).await) }
                    });
                    let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                    match acceptor {
                        Some(acceptor) => {
                            if let Ok(tls) = acceptor.accept(tcp).await {
                                let _ = builder.serve_connection(TokioIo::new(tls), service).await;
                            }
                        }
                        None => {
                            let _ = builder.serve_connection(TokioIo::new(tcp), service).await;
                        }
                    }
                });
            }
        });
        Self { state, addr, task }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// The fake's client URL.
    pub(super) fn url(&self, scheme: &str) -> String {
        format!("{scheme}://localhost:{}", self.addr.port())
    }

    /// Queues faults for the next unary requests.
    pub(super) fn fail(&self, faults: impl IntoIterator<Item = Fault>) {
        self.state().faults.extend(faults);
    }

    /// Queues a fault for the next watch.
    pub(super) fn fail_watch(&self, fault: WatchFault) {
        self.state().watch_faults.push_back(fault);
    }

    /// The watches opened so far.
    pub(super) fn watches(&self) -> usize {
        self.state().watches
    }

    /// The unary requests received so far.
    pub(super) fn requests(&self) -> usize {
        self.state().requests
    }

    /// Writes a key directly, as another client would.
    pub(super) fn put(&self, key: &str, value: &[u8]) {
        let mut state = self.state();
        state.revision += 1;
        let revision = state.revision;
        put(
            &mut state,
            Bytes::copy_from_slice(key.as_bytes()),
            Bytes::copy_from_slice(value),
            revision,
        );
    }
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn header(state: &State) -> Option<ResponseHeader> {
    Some(ResponseHeader {
        cluster_id: 1,
        member_id: 2,
        revision: state.revision,
        raft_term: 3,
    })
}

fn put(state: &mut State, key: Bytes, value: Bytes, revision: i64) {
    let create_revision = state
        .kvs
        .get(&key)
        .map_or(revision, |kv| kv.create_revision);
    let kv = KeyValue {
        key: key.clone(),
        create_revision,
        mod_revision: revision,
        version: 1,
        value,
        lease: 0,
    };
    state.kvs.insert(key.clone(), kv.clone());
    notify(
        state,
        &key,
        Event {
            r#type: EventType::Put as i32,
            kv: Some(kv),
        },
    );
}

fn notify(state: &mut State, key: &Bytes, event: Event) {
    let response = WatchResponse {
        header: header(state),
        events: vec![event],
        ..WatchResponse::default()
    };
    let frame = encode_frame(&response);
    state.watchers.retain(|(watched, sender)| {
        watched != key || sender.send(Frame::data(frame.clone())).is_ok()
    });
}

/// Reads the first message of a request body.
async fn first_message(body: &mut Incoming) -> Option<Bytes> {
    let mut decoder = FrameDecoder::default();
    loop {
        if let Some(message) = decoder.next_message().ok()? {
            return Some(message);
        }
        let frame = poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx))
            .await?
            .ok()?;
        if let Ok(data) = frame.into_data() {
            decoder.push(&data).ok()?;
        }
    }
}

async fn handle(
    state: &Arc<Mutex<State>>,
    request: http::Request<Incoming>,
) -> Response<ChannelBody> {
    let path = request.uri().path().to_owned();
    let mut body = request.into_body();
    let (sender, receiver) = mpsc::unbounded_channel();
    let mut response = Response::new(ChannelBody(receiver));
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/grpc"));
    let Some(message) = first_message(&mut body).await else {
        *response.status_mut() = StatusCode::BAD_REQUEST;
        return response;
    };
    if path == proto::WATCH {
        watch(state, message, sender);
        // Keep reading the request stream until the client closes it.
        tokio::spawn(async move {
            while let Some(Ok(_)) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {}
        });
        return response;
    }
    let mut state = lock(state);
    state.requests += 1;
    let fault = state.faults.pop_front().unwrap_or(Fault::Pass);
    let apply = matches!(
        fault,
        Fault::Pass | Fault::AppliedThenStatus(_) | Fault::AppliedThenHang
    );
    let answer = if apply {
        match path.as_str() {
            proto::RANGE => range(&state, &message),
            proto::TXN => txn(&mut state, &message),
            _ => Err(12),
        }
    } else {
        Ok(Bytes::new())
    };
    match (fault, answer) {
        (_, Err(code)) | (Fault::Status(code), _) => {
            response
                .headers_mut()
                .insert("grpc-status", HeaderValue::from(code));
            return response;
        }
        (Fault::Pass, Ok(message)) => {
            let _ = sender.send(Frame::data(message));
            let _ = sender.send(trailers(0));
        }
        (Fault::AppliedThenStatus(code), Ok(_)) => {
            let _ = sender.send(trailers(code));
        }
        (Fault::Hang | Fault::AppliedThenHang, _) => state.held.push(sender),
        (Fault::Oversized, _) => {
            let _ = sender.send(Frame::data(Bytes::from_static(&[
                0, 0xff, 0xff, 0xff, 0xff,
            ])));
            state.held.push(sender);
        }
        (Fault::Compressed, _) => {
            let _ = sender.send(Frame::data(Bytes::from_static(&[1, 0, 0, 0, 0])));
            let _ = sender.send(trailers(0));
        }
        (Fault::Http(status), _) => {
            *response.status_mut() = StatusCode::from_u16(status).unwrap();
        }
    }
    response
}

fn range(state: &State, message: &[u8]) -> Result<Bytes, u32> {
    let request = RangeRequest::decode(message).map_err(|_| 3_u32)?;
    let mut kvs: Vec<KeyValue> = if request.range_end.is_empty() {
        state.kvs.get(&request.key).cloned().into_iter().collect()
    } else {
        state
            .kvs
            .range(request.key.clone()..request.range_end.clone())
            .map(|(_, kv)| kv.clone())
            .collect()
    };
    let count = i64::try_from(kvs.len()).unwrap();
    let limit = usize::try_from(request.limit).unwrap();
    let more = limit > 0 && kvs.len() > limit;
    if more {
        kvs.truncate(limit);
    }
    if request.keys_only {
        for kv in &mut kvs {
            kv.value = Bytes::new();
        }
    }
    Ok(encode_frame(&RangeResponse {
        header: header(state),
        kvs,
        more,
        count,
    }))
}

fn txn(state: &mut State, message: &[u8]) -> Result<Bytes, u32> {
    let request = TxnRequest::decode(message).map_err(|_| 3_u32)?;
    let [compare] = request.compare.as_slice() else {
        return Err(12);
    };
    let current = state.kvs.get(&compare.key);
    let holds = match (compare.target, &compare.target_union) {
        (t, Some(TargetUnion::CreateRevision(r))) if t == CompareTarget::Create as i32 => {
            current.map_or(0, |kv| kv.create_revision) == *r
        }
        (t, Some(TargetUnion::ModRevision(r))) if t == CompareTarget::Mod as i32 => {
            current.map_or(0, |kv| kv.mod_revision) == *r
        }
        _ => return Err(12),
    };
    if holds {
        state.revision += 1;
        let revision = state.revision;
        for op in request.success {
            match op.request {
                Some(Request::Put(request)) => put(state, request.key, request.value, revision),
                Some(Request::DeleteRange(request)) => {
                    if let Some(mut kv) = state.kvs.remove(&request.key) {
                        kv.mod_revision = revision;
                        kv.value = Bytes::new();
                        notify(
                            state,
                            &request.key,
                            Event {
                                r#type: EventType::Delete as i32,
                                kv: Some(kv),
                            },
                        );
                    }
                }
                None => return Err(12),
            }
        }
    }
    Ok(encode_frame(&TxnResponse {
        header: header(state),
        succeeded: holds,
    }))
}

fn watch(state: &Arc<Mutex<State>>, message: Bytes, sender: Sender) {
    let mut state = lock(state);
    state.watches += 1;
    let Ok(WatchRequest {
        request_union: Some(WatchRequestUnion::Create(create)),
    }) = WatchRequest::decode(message)
    else {
        let _ = sender.send(trailers(3));
        return;
    };
    let created = WatchResponse {
        header: header(&state),
        created: true,
        ..WatchResponse::default()
    };
    let canceled = WatchResponse {
        header: header(&state),
        canceled: true,
        cancel_reason: "fake cancellation".to_owned(),
        ..WatchResponse::default()
    };
    match state.watch_faults.pop_front() {
        None => {
            let _ = sender.send(Frame::data(encode_frame(&created)));
            state.watchers.push((create.key, sender));
        }
        Some(WatchFault::Refused) => {
            let _ = sender.send(Frame::data(encode_frame(&canceled)));
            state.held.push(sender);
        }
        Some(WatchFault::CanceledAfterCreate) => {
            let _ = sender.send(Frame::data(encode_frame(&created)));
            let _ = sender.send(Frame::data(encode_frame(&canceled)));
            state.held.push(sender);
        }
        Some(WatchFault::CompactedAfterCreate) => {
            let _ = sender.send(Frame::data(encode_frame(&created)));
            let compacted = WatchResponse {
                compact_revision: state.revision,
                ..canceled
            };
            let _ = sender.send(Frame::data(encode_frame(&compacted)));
            state.held.push(sender);
        }
        Some(WatchFault::EndedAfterCreate) => {
            let _ = sender.send(Frame::data(encode_frame(&created)));
            let _ = sender.send(trailers(0));
        }
    }
}
