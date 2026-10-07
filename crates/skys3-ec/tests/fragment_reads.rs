//! Fragment reads between nodes (§8.5) over real loopback TCP: a node
//! serves a fragment's bytes only for the fragment a read names, and
//! answers every other read with why it does not serve it.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use prost::Message;
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::read::{FragmentData, FragmentRead, MAX_READ_LEN};
use skys3_ec::{
    FragmentClient, FragmentId, FragmentIdentity, FragmentReadClient, FragmentReadError,
    FragmentRequest, FragmentServer, FragmentSource, FragmentStore, FragmentStoreConfig,
    FragmentWriter,
};
use skys3_io::{SimDisk, SimMount};
use skys3_net::{Frame, Header, MessageKind, TokioNetwork};
use skys3_types::{NodeAddress, NodeId};
use support::pki::Pki;
use support::{header, sample};

const WAIT: Duration = Duration::from_secs(30);

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

async fn server() -> FragmentServer<SimMount> {
    let config = FragmentStoreConfig {
        disk: 0,
        segment_bytes: 1 << 20,
        group_commit_max_bytes: 1 << 20,
        max_fragment_bytes: 1 << 20,
    };
    let (store, _) = FragmentStore::open(SimDisk::new(1).mount(), config)
        .await
        .unwrap();
    FragmentServer::new(vec![store])
}

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.expect("in time")
}

fn identity(header: &FragmentHeader) -> FragmentIdentity {
    FragmentIdentity {
        shard: header.shard.clone(),
        key: header.key.clone(),
        version: header.version,
        stripe: header.stripe,
        index: header.index,
    }
}

fn request(
    node: NodeId,
    fragment: FragmentId,
    header: &FragmentHeader,
    range: std::ops::Range<u64>,
) -> FragmentRequest {
    FragmentRequest {
        node,
        fragment,
        identity: identity(header),
        range,
    }
}

/// Node 2 serving fragments on a loopback port, holding fragment 3 of
/// object `k`, 256 bytes long; node 1's read client.
async fn cluster(
    pki: &Pki,
) -> (
    SocketAddr,
    FragmentId,
    Bytes,
    FragmentReadClient<TokioNetwork, SimMount>,
) {
    let served = server().await;
    let listener = pki
        .transport(&node(2))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = served.clone();
    tokio::spawn(async move { serving.serve_listener(listener).await });
    let peers = BTreeMap::from([(node(2), address(addr))]);
    let writer = FragmentClient::<_, SimMount>::new(pki.transport(&node(1)), peers.clone());
    let data = Bytes::from(sample(256, 9));
    let id = timed(writer.write(&node(2), &header("k", 256, 3), data.clone()))
        .await
        .unwrap();
    let reader =
        FragmentReadClient::new(node(1), pki.transport(&node(1)), peers).with_local(server().await);
    (addr, id, data, reader)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_serves_the_fragment_a_read_names() {
    let pki = Pki::new();
    let (_, id, data, reader) = cluster(&pki).await;
    let header = header("k", 256, 3);
    // Several reads, which share one connection.
    for range in [0..256, 10..20, 255..256] {
        let read = timed(reader.read(request(node(2), id, &header, range.clone())))
            .await
            .unwrap();
        assert_eq!(
            read.data,
            data.slice(range.start as usize..range.end as usize)
        );
        assert_eq!(read.crc32c, crc32c::crc32c(&read.data));
    }

    let not_held = |result: Result<_, FragmentReadError>, why: &str| match result {
        Err(FragmentReadError::NotHeld { node: n, reason }) => {
            assert_eq!(n, node(2));
            assert!(reason.contains(why), "{reason} lacks {why}");
        }
        other => panic!("expected not held, got {other:?}"),
    };
    // Another index, another key, another ID, bytes past the fragment.
    let other = request(node(2), id, &support::header("k", 256, 4), 0..8);
    not_held(timed(reader.read(other)).await, "fragment 3 of stripe 0");
    let other = request(node(2), id, &support::header("j", 256, 3), 0..8);
    not_held(timed(reader.read(other)).await, "of k at");
    let unknown = request(node(2), FragmentId::new(77), &header, 0..8);
    not_held(timed(reader.read(unknown)).await, "no fragment");
    let past = request(node(2), id, &header, 200..300);
    not_held(timed(reader.read(past)).await, "is outside fragment");

    // A node with no address cannot be asked; this node's own store is
    // read without one, and holds nothing here.
    let error = timed(reader.read(request(node(5), id, &header, 0..8)))
        .await
        .unwrap_err();
    assert!(
        matches!(error, FragmentReadError::Unreachable { .. }),
        "{error}"
    );
    let local = timed(reader.read(request(node(1), id, &header, 0..8))).await;
    assert!(
        matches!(local, Err(FragmentReadError::NotHeld { .. })),
        "{local:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_answers_malformed_reads_and_stops_at_other_frames() {
    let pki = Pki::new();
    let (addr, id, _, _) = cluster(&pki).await;
    let mut connection = pki
        .transport(&node(1))
        .connect(&node(2), &address(addr))
        .await
        .unwrap();
    let ask = |body: Vec<u8>, request_id| {
        Frame::new(
            Header::new(MessageKind::FragmentRead)
                .with_request_id(request_id)
                .with_body(body),
            Bytes::new(),
        )
    };
    let mut good = FragmentRead::new(&request(node(2), id, &header("k", 256, 3), 0..4));
    for (n, body) in [vec![0xff], {
        good.end = good.start;
        good.encode_to_vec()
    }]
    .into_iter()
    .enumerate()
    {
        timed(connection.send(&ask(body, n as u64 + 7)))
            .await
            .unwrap();
        let answer = timed(connection.recv()).await.unwrap().unwrap();
        assert_eq!(answer.header.kind, MessageKind::FragmentData);
        assert_eq!(answer.header.request_id, n as u64 + 7);
        let data = FragmentData::decode(answer.header.body.as_ref()).unwrap();
        let error = data.parse(&node(2), answer.payload).unwrap_err();
        assert!(error.to_string().contains("malformed"), "{error}");
    }
    // Any other frame ends the connection's reads.
    timed(connection.send(&Frame::new(Header::new(MessageKind::Append), Bytes::new())))
        .await
        .unwrap();
    assert!(matches!(timed(connection.recv()).await, Ok(None) | Err(_)));
}

#[test]
fn answers_parse_their_failures() {
    let n = node(2);
    let answer = |failure| FragmentData {
        crc32c: 5,
        failure,
        reason: "why".to_owned(),
    };
    let served = answer(0).parse(&n, Bytes::from_static(b"ab")).unwrap();
    assert_eq!((served.data.as_ref(), served.crc32c), (&b"ab"[..], 5));
    assert!(matches!(
        answer(1).parse(&n, Bytes::new()),
        Err(FragmentReadError::NotHeld { .. })
    ));
    for failure in [2, 3, 99] {
        assert!(matches!(
            answer(failure).parse(&n, Bytes::new()),
            Err(FragmentReadError::Damaged { .. })
        ));
    }
}

proptest! {
    /// A read's body names what it was built from, and only well-formed
    /// bodies parse.
    #[test]
    fn read_bodies_round_trip(
        index in 0u8..6,
        start in 0u64..1 << 30,
        len in 1u64..=MAX_READ_LEN,
        id in any::<u128>(),
    ) {
        let header = header("a/key", 1 << 20, index);
        let request = request(node(2), FragmentId::new(id), &header, start..start + len);
        let body = FragmentRead::new(&request);
        let decoded = FragmentRead::decode(body.encode_to_vec().as_slice()).unwrap();
        let (fragment, identity, range) = decoded.parse().unwrap();
        prop_assert_eq!(fragment, request.fragment);
        prop_assert_eq!(identity, request.identity);
        prop_assert_eq!(range, request.range);

        let broken: [fn(&mut FragmentRead); 9] = [
            |b| b.id.pop().map(drop).unwrap_or(()),
            |b| b.shard = 256,
            |b| b.key.clear(),
            |b| b.data_fragments = 0,
            |b| b.index = 6,
            |b| b.stripe = b.stripes,
            |b| b.data_len = 0,
            |b| b.end = b.start,
            |b| b.end = b.start + MAX_READ_LEN + 1,
        ];
        for breaks in broken {
            let mut bad = body.clone();
            breaks(&mut bad);
            prop_assert!(bad.parse().is_err(), "{:?}", bad);
        }
    }
}
