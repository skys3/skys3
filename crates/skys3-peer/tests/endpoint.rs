//! The QUIC endpoint over loopback: mutual TLS against each peer's trust
//! bundle, the `HELLO` exchange, authorization of bucket pairs, and
//! refused 0-RTT.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use skys3_peer::{
    Abort, AbortReason, Applied, ApplyError, Batch, Begin, Capabilities, Commit, ConnectError,
    Data, Hello, Message, Outcome, PROTOCOL_VERSION, PeerConnection, PeerTrust, Precondition, Put,
    PutData, StagedRanges, StreamError, Unauthorized, VersionRange, WINDOW_INTERVAL, Write,
};
use skys3_types::{BucketName, ETag, WriteIdentity};

const US: &str = "prod-us";
const EU: &str = "prod-eu";
const AP: &str = "prod-ap";

/// The pair the source cluster may write in most tests.
const SOURCE_BUCKET: &str = "b-src";
const DESTINATION_BUCKET: &str = "archive";

fn identity(cluster: &str, bucket: &str, seq: u64) -> WriteIdentity {
    format!("{cluster}/{bucket}/5/42.{seq}").parse().unwrap()
}

fn bucket(name: &str) -> BucketName {
    BucketName::new(name).unwrap()
}

fn begin(identity: WriteIdentity, destination: &str) -> Message {
    Message::Begin(Begin {
        identity,
        bucket: bucket(destination),
        key: "photos/cat.jpg".to_owned(),
    })
}

fn commit(identity: WriteIdentity, destination: &str, data: PutData, size: u64) -> Commit {
    Commit {
        identity,
        bucket: bucket(destination),
        key: format!("key-{size}-{}", destination.len()),
        precondition: Precondition::Absent,
        write: Write::Put(Put {
            size,
            etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
            last_modified_ms: 1_700_000_000_000,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            data,
        }),
        apply_by_ms: Some(1_800_000_000_000),
    }
}

fn staged_commit(identity: WriteIdentity, destination: &str) -> Message {
    Message::Commit(commit(
        identity,
        destination,
        PutData::Staged { piece: 1 },
        5,
    ))
}

fn inline(identity: WriteIdentity, destination: &str, key: &str) -> Commit {
    Commit {
        key: key.to_owned(),
        ..commit(
            identity,
            destination,
            PutData::Inline(Bytes::from_static(b"hello")),
            5,
        )
    }
}

/// Two clusters: `prod-us` sends, `prod-eu` receives and lets `prod-us`
/// write `pairs`.
struct Peers {
    us: Cluster,
    eu: Cluster,
    source: skys3_peer::PeerEndpoint,
    destination: skys3_peer::PeerEndpoint,
}

fn peers() -> Peers {
    let us = Cluster::new(US);
    let eu = Cluster::new(EU);
    let mut destination_trust = PeerTrust::new();
    us.trusted(
        &mut destination_trust,
        &[pair(SOURCE_BUCKET, DESTINATION_BUCKET)],
    );
    let mut source_trust = PeerTrust::new();
    eu.trusted(&mut source_trust, &[]);
    Peers {
        source: us.endpoint("us-1", source_trust),
        destination: eu.endpoint("eu-1", destination_trust),
        us,
        eu,
    }
}

/// Connects the source to the destination, returning both ends.
async fn connected(peers: &Peers) -> (PeerConnection, PeerConnection) {
    let to = destination(EU, &peers.destination);
    let (source, destination) =
        async { tokio::join!(peers.source.connect(&to), accept(&peers.destination)) }
            .bounded()
            .await;
    (source.unwrap(), destination.unwrap())
}

#[tokio::test]
async fn trusted_peers_negotiate_and_exchange_messages() {
    let peers = peers();
    let (source, destination) = connected(&peers).await;
    assert_eq!(source.peer().as_str(), EU);
    assert_eq!(source.peer_node().as_str(), "eu-1");
    assert_eq!(source.side(), skys3_peer::Side::Source);
    assert_eq!(destination.peer().as_str(), US);
    assert_eq!(destination.peer_node().as_str(), "us-1");
    assert_eq!(destination.side(), skys3_peer::Side::Destination);
    for end in [&source, &destination] {
        assert_eq!(end.session().version, PROTOCOL_VERSION);
        assert_eq!(end.session().capabilities, Capabilities::BATCH);
        assert!(end.close_reason().is_none());
    }
    assert_eq!(
        destination.remote_address(),
        peers.source.local_addr().unwrap()
    );
    assert_eq!(peers.source.settings(), &settings());

    let id = identity(US, SOURCE_BUCKET, 1);
    let mut out = source.open_stream().bounded().await.unwrap();
    assert_eq!(source.open_streams(), 1);
    out.send(&begin(id.clone(), DESTINATION_BUCKET))
        .bounded()
        .await
        .unwrap();
    let data = Message::Data(Data {
        piece: 1,
        offset: 0,
        bytes: Bytes::from(vec![7; 3 << 20]),
    });
    let mut inbound = destination.accept_stream().bounded().await.unwrap();
    assert_eq!(inbound.peer().as_str(), US);
    assert!(matches!(
        inbound.recv().bounded().await.unwrap(),
        Some(Message::Begin(_))
    ));
    let resume = Message::Resume(StagedRanges {
        identity: id.clone(),
        pieces: BTreeMap::new(),
    });
    inbound.send(&resume).bounded().await.unwrap();
    // A 3 MiB frame crosses while both ends read and write.
    let (sent, received) = async { tokio::join!(out.send(&data), inbound.recv()) }
        .bounded()
        .await;
    sent.unwrap();
    assert_eq!(received.unwrap(), Some(data));
    assert_eq!(out.recv().bounded().await.unwrap(), Some(resume));

    // Messages of the other side are refused before they are sent.
    let error = out
        .send(&Message::Applied(Applied {
            identity: id.clone(),
            outcome: Outcome::Committed { etag: None },
        }))
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Protocol(_)), "{error}");

    out.finish().unwrap();
    let (mut sender, mut receiver) = out.split();
    assert!(sender.finish().is_err(), "already finished");
    inbound.finish().await.unwrap();
    assert_eq!(receiver.recv().bounded().await.unwrap(), None);
    drop((sender, receiver));
    assert_eq!(source.open_streams(), 0);

    // Windows are sized while the connection lives.
    tokio::time::sleep(WINDOW_INTERVAL * 2 + Duration::from_millis(200)).await;
    assert!(source.rtt() > Duration::ZERO);
    assert!(source.stats().udp_tx.bytes > 3 << 20);

    source.close();
    let reason = destination.closed().bounded().await;
    assert!(
        matches!(reason, quinn::ConnectionError::ApplicationClosed(_)),
        "{reason}"
    );
    peers.source.close();
    peers.destination.close();
    peers.source.wait_idle().bounded().await;
    assert!(peers.destination.accept().bounded().await.is_none());
}

/// Connects `source` to the destination endpoint and returns both
/// verdicts.
async fn attempt(
    source: &skys3_peer::PeerEndpoint,
    to: &skys3_peer::Destination,
    destination: &skys3_peer::PeerEndpoint,
) -> (
    Result<PeerConnection, ConnectError>,
    Result<PeerConnection, ConnectError>,
) {
    async { tokio::join!(source.connect(to), accept(destination)) }
        .bounded()
        .await
}

#[track_caller]
fn refused_handshake(result: Result<PeerConnection, ConnectError>) {
    match result {
        Err(ConnectError::Connection(_)) => {}
        other => panic!("expected a failed handshake, got {other:?}"),
    }
}

#[tokio::test]
async fn untrusted_peers_are_rejected() {
    let peers = peers();
    let to = destination(EU, &peers.destination);
    let eu_trust = || {
        let mut trust = PeerTrust::new();
        peers.eu.trusted(&mut trust, &[]);
        trust
    };

    // A certificate naming prod-us from a CA prod-eu does not trust for it.
    let rogue = Cluster {
        id: US,
        ca: Ca::new("rogue"),
    };
    let (client, server) =
        attempt(&rogue.endpoint("us-1", eu_trust()), &to, &peers.destination).await;
    refused_handshake(server);
    assert!(client.is_err());

    // A cluster prod-eu does not trust at all.
    let stranger = Cluster::new("prod-xx");
    let (client, server) = attempt(
        &stranger.endpoint("xx-1", eu_trust()),
        &to,
        &peers.destination,
    )
    .await;
    refused_handshake(server);
    assert!(client.is_err());

    // prod-us's CA vouching for a node of prod-ap, whose CA prod-eu also
    // trusts: each cluster's nodes must lead to that cluster's own CA.
    let ap = Cluster::new(AP);
    let mut trust = PeerTrust::new();
    peers.us.trusted(&mut trust, &[]);
    ap.trusted(&mut trust, &[]);
    let destination_ap = peers.eu.endpoint("eu-2", trust);
    let to_ap = destination(EU, &destination_ap);
    let forged = peers.us.ca.credentials(AP, "ap-1");
    let forged = skys3_peer::PeerTls::new(&forged, std::sync::Arc::new(eu_trust())).unwrap();
    let forged = skys3_peer::PeerEndpoint::bind(loopback(), &forged, settings()).unwrap();
    let (client, server) = attempt(&forged, &to_ap, &destination_ap).await;
    refused_handshake(server);
    assert!(client.is_err());

    // An operator tool's certificate is not a node's.
    let admin = RawClient::new(peers.us.ca.issue(&format!("spiffe://{US}/admin/ops")));
    let (client, server) = async {
        tokio::join!(
            async {
                let connection = admin.connect(to.address, EU).await?;
                round_trip(&connection, &hello(US)).await
            },
            accept(&peers.destination)
        )
    }
    .bounded()
    .await;
    refused_handshake(server);
    assert!(client.is_err());

    // The source refuses a destination it does not trust: here its trust
    // bundle for prod-eu is another CA.
    let mut wrong_ca = PeerTrust::new();
    Cluster::new(EU).trusted(&mut wrong_ca, &[]);
    let (client, server) = attempt(
        &peers.us.endpoint("us-2", wrong_ca),
        &to,
        &peers.destination,
    )
    .await;
    refused_handshake(client);
    assert!(server.is_err());

    // And a trusted server of another cluster than the one it meant.
    let mut both = eu_trust();
    ap.trusted(&mut both, &[]);
    let mut ap_trust = PeerTrust::new();
    peers.us.trusted(&mut ap_trust, &[]);
    let ap_endpoint = ap.endpoint("ap-1", ap_trust);
    let pretend = skys3_peer::Destination {
        cluster: id(EU),
        address: ap_endpoint.local_addr().unwrap(),
    };
    let (client, server) = attempt(&peers.us.endpoint("us-3", both), &pretend, &ap_endpoint).await;
    refused_handshake(client);
    assert!(server.is_err());
}

#[tokio::test]
async fn hellos_must_match_the_certificate_and_share_a_version() {
    let peers = peers();
    let to = destination(EU, &peers.destination);
    let raw = RawClient::new(peers.us.ca.node(US, "us-1"));
    let attempt = |message: Option<Message>| {
        let raw = &raw;
        let peers = &peers;
        async move {
            let (_, server) = async {
                tokio::join!(
                    async {
                        let connection = raw.connect(to.address, EU).await.unwrap();
                        match message {
                            Some(message) => round_trip(&connection, &message).await.ok(),
                            None => {
                                let (mut send, _recv) = connection.open_bi().await.unwrap();
                                send.write_all(&[0; 3]).await.unwrap();
                                send.finish().unwrap();
                                connection.closed().await;
                                None
                            }
                        }
                    },
                    accept(&peers.destination)
                )
            }
            .bounded()
            .await;
            server.unwrap_err()
        }
    };

    let error = attempt(Some(hello("prod-xx"))).await;
    assert!(
        matches!(error, ConnectError::WrongCluster { .. }),
        "{error}"
    );
    // A peer that speaks only newer versions, or only version 1, whose
    // commits carry no apply-by time.
    let error = attempt(Some(Message::Hello(Hello {
        versions: VersionRange::new(3, 4).unwrap(),
        ..match hello(US) {
            Message::Hello(hello) => hello,
            _ => unreachable!(),
        }
    })))
    .await;
    assert!(matches!(error, ConnectError::Negotiation(_)), "{error}");
    let error = attempt(Some(Message::Hello(Hello {
        versions: VersionRange::new(1, 1).unwrap(),
        ..match hello(US) {
            Message::Hello(hello) => hello,
            _ => unreachable!(),
        }
    })))
    .await;
    assert!(matches!(error, ConnectError::Negotiation(_)), "{error}");
    let error = attempt(Some(Message::Abort(Abort {
        identity: identity(US, SOURCE_BUCKET, 1),
        reason: AbortReason::Cancelled,
        detail: String::new(),
    })))
    .await;
    assert!(matches!(error, ConnectError::NotHello("ABORT")), "{error}");
    let error = attempt(None).await;
    assert!(
        matches!(error, ConnectError::Hello(StreamError::Truncated)),
        "{error}"
    );
}

/// Sends `messages` on a new stream, and returns what the destination
/// makes of the first one and what the source hears back.
async fn exchange(
    source: &PeerConnection,
    destination: &PeerConnection,
    messages: &[Message],
) -> (Result<Option<Message>, StreamError>, Vec<Message>) {
    let mut out = source.open_stream().bounded().await.unwrap();
    for message in messages {
        out.send(message).bounded().await.unwrap();
    }
    out.finish().unwrap();
    let mut inbound = destination.accept_stream().bounded().await.unwrap();
    let verdict = inbound.recv().bounded().await;
    // The destination has nothing more to say.
    let _ = inbound.finish().await;
    let mut replies = Vec::new();
    while let Ok(Some(reply)) = out.recv().bounded().await {
        replies.push(reply);
    }
    (verdict, replies)
}

#[track_caller]
fn refused_with(verdict: Result<Option<Message>, StreamError>) -> Unauthorized {
    match verdict {
        Err(StreamError::Unauthorized(unauthorized)) => unauthorized,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn unauthorized_bucket_pairs_are_rejected() {
    let peers = peers();
    let (source, destination) = connected(&peers).await;

    // A BEGIN into a bucket the pair does not name: ABORT, and the stream
    // is stopped.
    let id = identity(US, SOURCE_BUCKET, 1);
    let (verdict, replies) = exchange(&source, &destination, &[begin(id.clone(), "other")]).await;
    assert!(matches!(
        refused_with(verdict),
        Unauthorized::BucketPair { .. }
    ));
    let [Message::Abort(abort)] = replies.as_slice() else {
        panic!("expected one ABORT, got {replies:?}");
    };
    assert_eq!(abort.identity, id);
    assert_eq!(abort.reason, AbortReason::Refused);
    assert!(abort.detail.contains("other"), "{}", abort.detail);

    // From a bucket the pair does not name.
    let (verdict, _) = exchange(
        &source,
        &destination,
        &[begin(identity(US, "b-other", 2), DESTINATION_BUCKET)],
    )
    .await;
    assert!(matches!(
        refused_with(verdict),
        Unauthorized::BucketPair { .. }
    ));

    // In another cluster's name.
    let (verdict, _) = exchange(
        &source,
        &destination,
        &[begin(identity(AP, SOURCE_BUCKET, 3), DESTINATION_BUCKET)],
    )
    .await;
    assert!(matches!(
        refused_with(verdict),
        Unauthorized::ForeignIdentity { .. }
    ));

    // A COMMIT: APPLIED says refused.
    let id = identity(US, SOURCE_BUCKET, 4);
    let (verdict, replies) =
        exchange(&source, &destination, &[staged_commit(id.clone(), "other")]).await;
    refused_with(verdict);
    let [Message::Applied(applied)] = replies.as_slice() else {
        panic!("expected one APPLIED, got {replies:?}");
    };
    assert_eq!(applied.identity, id);
    assert!(matches!(
        applied.outcome,
        Outcome::Failed {
            error: ApplyError::Refused,
            ..
        }
    ));

    // An ABORT names no destination, so its source bucket must be in a
    // pair.
    let (verdict, replies) = exchange(
        &source,
        &destination,
        &[Message::Abort(Abort {
            identity: identity(US, "b-other", 5),
            reason: AbortReason::Cancelled,
            detail: String::new(),
        })],
    )
    .await;
    assert!(matches!(
        refused_with(verdict),
        Unauthorized::SourceBucket { .. }
    ));
    assert!(matches!(
        replies.as_slice(),
        [Message::Abort(Abort {
            reason: AbortReason::Refused,
            ..
        })]
    ));
    let cancel = Message::Abort(Abort {
        identity: identity(US, SOURCE_BUCKET, 6),
        reason: AbortReason::Cancelled,
        detail: String::new(),
    });
    let (verdict, replies) = exchange(&source, &destination, std::slice::from_ref(&cancel)).await;
    assert_eq!(verdict.unwrap(), Some(cancel));
    assert!(replies.is_empty());

    // A batch loses its refused items, each answered on its own.
    let allowed = inline(identity(US, SOURCE_BUCKET, 7), DESTINATION_BUCKET, "a");
    let refused = inline(identity(US, SOURCE_BUCKET, 8), "other", "b");
    let batch = Message::Batch(Batch {
        items: vec![allowed.clone(), refused.clone()],
    });
    let (verdict, replies) = exchange(&source, &destination, &[batch]).await;
    assert_eq!(
        verdict.unwrap(),
        Some(Message::Batch(Batch {
            items: vec![allowed]
        }))
    );
    let [Message::Applied(applied)] = replies.as_slice() else {
        panic!("expected one APPLIED, got {replies:?}");
    };
    assert_eq!(applied.identity, refused.identity);
    let only_refused = Message::Batch(Batch {
        items: vec![refused],
    });
    let (verdict, replies) = exchange(&source, &destination, &[only_refused]).await;
    refused_with(verdict);
    assert_eq!(replies.len(), 1);

    // DATA without an accepted BEGIN on its stream.
    let data = Message::Data(Data {
        piece: 1,
        offset: 0,
        bytes: Bytes::from_static(b"x"),
    });
    let (verdict, _) = exchange(&source, &destination, &[data]).await;
    assert!(matches!(verdict, Err(StreamError::DataWithoutBegin)));

    // The authorized pair still works on the same connection.
    let id = identity(US, SOURCE_BUCKET, 9);
    let (verdict, _) = exchange(
        &source,
        &destination,
        &[staged_commit(id.clone(), DESTINATION_BUCKET)],
    )
    .await;
    assert!(matches!(verdict, Ok(Some(Message::Commit(_)))));
}

#[tokio::test]
async fn zero_rtt_attempts_are_rejected() {
    let peers = peers();
    let to = destination(EU, &peers.destination);

    // A server that issues tickets and accepts early data, under the
    // destination's name, hands a client a ticket for prod-eu.
    let ticketing = ticket_server(peers.eu.ca.node(EU, "eu-9"));
    let ticketing_address = ticketing.local_addr().unwrap();
    let serve = tokio::spawn(async move {
        while let Some(incoming) = ticketing.accept().await {
            let Ok(connection) = incoming.await else {
                continue;
            };
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let bytes = recv.read_to_end(1 << 20).await.unwrap_or_default();
                    let _ = send.write_all(&bytes).await;
                    let _ = send.finish();
                }
            });
        }
    });
    let client = RawClient::new(peers.us.ca.node(US, "us-1"));
    let connection = client
        .connect(ticketing_address, EU)
        .bounded()
        .await
        .unwrap();
    round_trip(&connection, &hello(US)).bounded().await.unwrap();
    // The client can now send 0-RTT data to that name.
    let (early, accepted) = client
        .connect(ticketing_address, EU)
        .into_0rtt()
        .expect("the ticket allows 0-RTT");
    assert!(accepted.bounded().await, "the ticketing server takes 0-RTT");
    early.close(0u32.into(), b"");

    // Against the real destination the early data is refused: the client
    // learns 0-RTT was rejected, and the HELLO it sent early never
    // arrives, so the connection is not established.
    let (early, accepted) = client
        .connect(to.address, EU)
        .into_0rtt()
        .expect("the client attempts 0-RTT");
    let (mut send, _recv) = early.open_bi().await.unwrap();
    let _ = send.write_all(&hello(US).encode().unwrap()).await;
    let (accepted, server) = async {
        tokio::join!(
            async {
                let accepted = accepted.await;
                early.close(0u32.into(), b"0-RTT refused");
                accepted
            },
            accept(&peers.destination)
        )
    }
    .bounded()
    .await;
    assert!(!accepted, "the destination accepted 0-RTT data");
    assert!(server.is_err(), "a connection was established from 0-RTT");

    // The destination issues no tickets: after a full handshake and HELLOs,
    // a fresh client still cannot resume or send early data.
    let fresh = RawClient::new(peers.us.ca.node(US, "us-1"));
    let (reply, server) = async {
        tokio::join!(
            async {
                let connection = fresh.connect(to.address, EU).await.unwrap();
                round_trip(&connection, &hello(US)).await.unwrap()
            },
            accept(&peers.destination)
        )
    }
    .bounded()
    .await;
    assert!(matches!(reply, Some(Message::Hello(_))));
    let server = server.unwrap();
    assert!(
        fresh.connect(to.address, EU).into_0rtt().is_err(),
        "the destination issued a session ticket"
    );
    drop(server);
    serve.abort();
}

/// An endpoint over a socket of the caller's, seeded, serves peers as a
/// bound one does, and loses a connection once it heard nothing for its
/// idle timeout.
#[tokio::test]
async fn a_seeded_endpoint_over_any_socket_serves_peers() {
    let us = Cluster::new(US);
    let eu = Cluster::new(EU);
    let mut destination_trust = PeerTrust::new();
    us.trusted(
        &mut destination_trust,
        &[pair(SOURCE_BUCKET, DESTINATION_BUCKET)],
    );
    let mut source_trust = PeerTrust::new();
    eu.trusted(&mut source_trust, &[]);
    let idle = Duration::from_millis(600);
    let short = skys3_peer::EndpointSettings {
        idle_timeout: idle,
        ..settings()
    };
    let socket = |address| {
        let socket = std::net::UdpSocket::bind(address).unwrap();
        quinn::default_runtime()
            .unwrap()
            .wrap_udp_socket(socket)
            .unwrap()
    };
    let destination = skys3_peer::PeerEndpoint::with_socket(
        socket(loopback()),
        &eu.tls("eu-1", destination_trust),
        short.clone(),
        Some(7),
    )
    .unwrap();
    let source = skys3_peer::PeerEndpoint::with_socket(
        socket(loopback()),
        &us.tls("us-1", source_trust),
        short,
        Some(8),
    )
    .unwrap();
    let to = skys3_peer::Destination {
        cluster: id(EU),
        address: destination.local_addr().unwrap(),
    };
    for _ in 0..2 {
        let (client, server) = async { tokio::join!(source.connect(&to), accept(&destination)) }
            .bounded()
            .await;
        let (client, server) = (client.unwrap(), server.unwrap());
        assert_eq!(client.peer().as_str(), EU);
        assert_eq!(server.peer().as_str(), US);
        let (sent, received) = async {
            tokio::join!(
                async {
                    let mut stream = client.open_stream().await.unwrap();
                    stream
                        .send(&begin(identity(US, SOURCE_BUCKET, 1), DESTINATION_BUCKET))
                        .await
                },
                async { server.accept_stream().await.unwrap().recv().await }
            )
        }
        .bounded()
        .await;
        sent.unwrap();
        assert!(matches!(received, Ok(Some(Message::Begin(_)))));
        // Keep-alives hold the connection open past its idle timeout...
        tokio::time::sleep(idle * 2).await;
        assert!(client.close_reason().is_none());
        // ...until the peer goes silent.
        server.close();
        let reason = client.closed().bounded().await;
        assert!(
            matches!(reason, quinn::ConnectionError::ApplicationClosed(_)),
            "{reason:?}"
        );
    }
}
