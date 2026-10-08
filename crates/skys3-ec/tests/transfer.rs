//! Fragment writes between nodes (§8.4, step 2) over real loopback TCP:
//! a fragment arrives whole and durable or not at all, and a node refuses
//! what breaks the protocol.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_ec::{
    CHUNK_LEN, FragmentClient, FragmentServer, FragmentStore, FragmentStoreConfig, FragmentWrite,
    FragmentWriter, FragmentWritten, TransferError,
};
use skys3_io::{SimDisk, SimMount};
use skys3_net::{
    CertificateDer, Credentials, Frame, Header, MessageKind, PeerIdentity, PrivateKeyDer,
    TokioNetwork, Transport,
};
use skys3_types::{ClusterId, NodeAddress, NodeId};
use support::{header, sample};

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

struct Pki {
    cluster: ClusterId,
    issuer: Issuer<'static, KeyPair>,
    ca: CertificateDer<'static>,
}

impl Pki {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = params.self_signed(&key).unwrap().der().clone();
        Self {
            cluster: ClusterId::new("test").unwrap(),
            issuer: Issuer::new(params, key),
            ca,
        }
    }

    fn transport(&self, node: &NodeId) -> Transport<TokioNetwork> {
        let identity = PeerIdentity::Node(node.clone()).spiffe_id(&self.cluster);
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![SanType::URI(identity.as_str().try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        let key = PrivateKeyDer::try_from(key.serialize_der()).unwrap();
        let credentials = Credentials::new(
            self.cluster.clone(),
            vec![cert],
            key,
            std::slice::from_ref(&self.ca),
        )
        .unwrap();
        Transport::new(TokioNetwork, &credentials)
    }
}

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

fn config(disk: u8) -> FragmentStoreConfig {
    FragmentStoreConfig {
        disk,
        segment_bytes: 64 << 20,
        group_commit_max_bytes: 16 << 20,
        max_fragment_bytes: 32 << 20,
    }
}

/// A fragment server over `disks` fresh stores.
async fn server(disks: u8) -> FragmentServer<SimMount> {
    let mut stores = Vec::new();
    for disk in 0..disks {
        let mount = SimDisk::new(u64::from(disk) + 1).mount();
        let (store, _) = FragmentStore::open(mount, config(disk)).await.unwrap();
        stores.push(store);
    }
    FragmentServer::new(stores)
}

/// Node 2 serving fragment writes on a loopback port.
async fn listening(pki: &Pki, disks: u8) -> (SocketAddr, FragmentServer<SimMount>) {
    let server = server(disks).await;
    let listener = pki
        .transport(&node(2))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = server.clone();
    tokio::spawn(async move { serving.serve_listener(listener).await });
    (addr, server)
}

/// Node 1's client, knowing node 2 at `addr`.
fn client(pki: &Pki, addr: SocketAddr) -> FragmentClient<TokioNetwork, SimMount> {
    FragmentClient::new(
        pki.transport(&node(1)),
        BTreeMap::from([(node(2), address(addr))]),
    )
}

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.expect("in time")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragments_arrive_durable_on_the_emptiest_store() {
    let pki = Pki::new();
    let (addr, server) = listening(&pki, 2).await;
    let client = client(&pki, addr);

    // More than a frame's worth, so it travels in two.
    let len = CHUNK_LEN as u64 + 4096;
    let data = Bytes::from(sample(len as usize, 1));
    let id = timed(client.write(&node(2), &header("k", len, 0), data.clone()))
        .await
        .unwrap();
    let first = server.stores().iter().position(|s| s.len(id).is_some());
    let read = server.stores()[first.unwrap()]
        .read(id, 0..len)
        .await
        .unwrap();
    assert_eq!(read.header, header("k", len, 0));
    assert_eq!(read.data, data);

    // The next goes to the other, emptier store.
    let small = Bytes::from(sample(64, 2));
    let id = timed(client.write(&node(2), &header("k", 64, 1), small))
        .await
        .unwrap();
    let second = server.stores().iter().position(|s| s.len(id).is_some());
    assert_ne!(first, second);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_writes_its_own_fragments_directly() {
    let pki = Pki::new();
    let local = server(1).await;
    // No address for node 1 itself: the write never touches the network.
    let client = FragmentClient::new(pki.transport(&node(1)), BTreeMap::new())
        .with_local(node(1), local.clone());
    let data = Bytes::from(sample(128, 3));
    let id = timed(client.write(&node(1), &header("k", 128, 2), data.clone()))
        .await
        .unwrap();
    assert_eq!(local.stores()[0].read(id, 0..128).await.unwrap().data, data);

    // A fragment that does not fit its header is refused, as remotely.
    let error = timed(client.write(&node(1), &header("k", 192, 2), data))
        .await
        .unwrap_err();
    assert!(matches!(error, TransferError::Refused { .. }), "{error}");
    let error = timed(client.write(&node(3), &header("k", 128, 2), Bytes::new()))
        .await
        .unwrap_err();
    assert!(matches!(error, TransferError::UnknownNode(_)), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_refuses_writes_it_cannot_make_durable() {
    let pki = Pki::new();
    let (addr, server) = listening(&pki, 1).await;
    let client = client(&pki, addr);

    // The bytes do not fit the header's stripe.
    let error = timed(client.write(&node(2), &header("k", 128, 0), sample(64, 4).into()))
        .await
        .unwrap_err();
    assert!(matches!(error, TransferError::Refused { .. }), "{error}");
    // Too long for any fragment store.
    let len = (32 << 20) + 64;
    let error = timed(client.write(
        &node(2),
        &header("k", len, 0),
        sample(len as usize, 5).into(),
    ))
    .await
    .unwrap_err();
    assert!(matches!(error, TransferError::Refused { .. }), "{error}");
    assert!(server.stores()[0].segments().iter().all(|s| s.len == 0));

    // A node nothing listens on fails as a transport error.
    let closed = {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let error =
        timed(client_of(&pki, closed).write(&node(2), &header("k", 64, 0), sample(64, 6).into()))
            .await
            .unwrap_err();
    assert!(matches!(error, TransferError::Transport { .. }), "{error}");
}

fn client_of(pki: &Pki, addr: SocketAddr) -> FragmentClient<TokioNetwork, SimMount> {
    client(pki, addr)
}

/// Sends `frames` on a fresh connection to node 2 and returns its answer.
async fn exchange(pki: &Pki, addr: SocketAddr, frames: Vec<Frame>) -> FragmentWritten {
    let connection = pki
        .transport(&node(1))
        .connect(&node(2), &address(addr))
        .await
        .unwrap();
    let (mut receiver, mut sender) = connection.into_split();
    let sending = async {
        for frame in &frames {
            if sender.send(frame).await.is_err() {
                break;
            }
        }
        // Nothing more comes: a node waiting for more bytes stops.
        let _ = sender.close().await;
    };
    let (_, answer) = timed(async { tokio::join!(sending, receiver.recv()) }).await;
    let frame = answer.unwrap().expect("an answer");
    assert_eq!(frame.header.kind, MessageKind::FragmentWritten);
    FragmentWritten::decode(frame.header.body.as_ref()).unwrap()
}

/// A `FragmentWrite` frame with `body`, if any, carrying `payload`.
fn write(body: Option<FragmentWrite>, payload: &[u8]) -> Frame {
    let mut head = Header::new(MessageKind::FragmentWrite);
    if let Some(body) = body {
        head = head.with_body(body.encode_to_vec());
    }
    Frame::new(head, Bytes::copy_from_slice(payload))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_refuses_writes_that_break_the_protocol() {
    let pki = Pki::new();
    let (addr, server) = listening(&pki, 1).await;
    let header = header("k", 64, 0).to_bytes().unwrap();
    let data = sample(64, 7);
    let body = |header_len: usize, len: usize, crc32c: u32| FragmentWrite {
        header_len: header_len as u32,
        len: len as u64,
        crc32c,
    };
    let crc = crc32c::crc32c(&data);
    let stream = [header.as_slice(), &data].concat();

    // A well-formed write, split over three frames, is accepted.
    let (one, rest) = stream.split_at(10);
    let (two, three) = rest.split_at(rest.len() - 5);
    let good = exchange(
        &pki,
        addr,
        vec![
            write(Some(body(header.len(), 64, crc)), one),
            write(None, two),
            write(None, three),
        ],
    )
    .await;
    assert!(good.error.is_empty(), "{}", good.error);
    assert_eq!(good.id.len(), 16);

    let refused = |answer: FragmentWritten, why: &str| {
        assert!(answer.id.is_empty());
        assert!(answer.error.contains(why), "{} lacks {why}", answer.error);
    };
    let cases: Vec<(Vec<Frame>, &str)> = vec![
        (
            vec![Frame::new(Header::new(MessageKind::Append), Bytes::new())],
            "expected a fragment write",
        ),
        (
            vec![Frame::new(
                Header::new(MessageKind::FragmentWrite).with_body(vec![0xff]),
                Bytes::new(),
            )],
            "malformed",
        ),
        (vec![write(Some(body(0, 64, crc)), &stream)], "header of 0"),
        (
            vec![write(Some(body(header.len(), 0, crc)), &stream)],
            "fragment of 0",
        ),
        (
            vec![write(Some(body(header.len(), 32, crc)), &stream)],
            "more bytes",
        ),
        (
            vec![write(Some(body(header.len(), 64, crc)), &stream[..20])],
            "broke off",
        ),
        (
            vec![write(Some(body(header.len(), 64, crc ^ 1)), &stream)],
            "CRC32C",
        ),
        (
            vec![write(Some(body(header.len() - 1, 65, crc)), &stream)],
            "header",
        ),
    ];
    for (frames, why) in cases {
        refused(exchange(&pki, addr, frames).await, why);
    }
    // Only the good write reached the store.
    assert_eq!(server.stores()[0].ids().len(), 1);
}
