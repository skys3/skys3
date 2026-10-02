//! The connection pool over loopback: connections per destination within
//! the adaptive limit, streams spread across them, and the ceiling that
//! follows the attached shards.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::*;
use skys3_peer::{
    Begin, ConnectError, ConnectionPool, Destination, EndpointSettings, Message, PeerEndpoint,
    PeerTrust, PoolError, PoolStats, StagedRanges,
};
use skys3_types::{BucketName, WriteIdentity};

const US: &str = "prod-us";
const EU: &str = "prod-eu";

fn begin(seq: u64) -> Message {
    let identity: WriteIdentity = format!("{US}/b-src/5/42.{seq}").parse().unwrap();
    Message::Begin(Begin {
        identity,
        bucket: BucketName::new("archive").unwrap(),
        key: "k".to_owned(),
    })
}

/// A destination that answers every `BEGIN` with an empty `RESUME`.
fn serve(endpoint: PeerEndpoint) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            tokio::spawn(async move {
                let Ok(connection) = incoming.establish().await else {
                    return;
                };
                while let Ok(mut stream) = connection.accept_stream().await {
                    tokio::spawn(async move {
                        while let Ok(Some(message)) = stream.recv().await {
                            if let Message::Begin(begin) = message {
                                let resume = Message::Resume(StagedRanges {
                                    identity: begin.identity,
                                    pieces: BTreeMap::new(),
                                });
                                if stream.send(&resume).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                }
            });
        }
    })
}

fn stats(shards: u32, limit: u32, max: u32, connections: usize) -> Option<PoolStats> {
    Some(PoolStats {
        shards,
        limit,
        max,
        connections,
    })
}

#[tokio::test]
async fn pools_connections_per_destination_within_the_limit() {
    let us = Cluster::new(US);
    let eu = Cluster::new(EU);
    let mut destination_trust = PeerTrust::new();
    us.trusted(&mut destination_trust, &[pair("b-src", "archive")]);
    let mut source_trust = PeerTrust::new();
    eu.trusted(&mut source_trust, &[]);
    let destination_endpoint = eu.endpoint("eu-1", destination_trust);
    let to = destination(EU, &destination_endpoint);
    let server = serve(destination_endpoint);

    let pool = ConnectionPool::new(us.endpoint("us-1", source_trust), 2);
    assert_eq!(pool.stats(&to), None);
    let lease = pool.attach(to.clone());
    assert_eq!(lease.destination(), &to);
    assert_eq!(pool.stats(&to), stats(1, 1, 2, 0));

    // Two streams at once share the one connection the limit allows.
    let (a, b) = async { tokio::join!(lease.open_stream(), lease.open_stream()) }
        .bounded()
        .await;
    let (mut a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(pool.stats(&to), stats(1, 1, 2, 1));
    a.send(&begin(1)).bounded().await.unwrap();
    assert!(matches!(
        a.recv().bounded().await.unwrap(),
        Some(Message::Resume(_))
    ));

    // Saturated and moving bytes: the limit grows, and the next stream
    // gets a connection of its own.
    pool.adapt();
    assert_eq!(pool.stats(&to), stats(1, 2, 2, 1));
    let mut c = lease.open_stream().bounded().await.unwrap();
    assert_eq!(pool.stats(&to), stats(1, 2, 2, 2));
    c.send(&begin(2)).bounded().await.unwrap();
    assert!(c.recv().bounded().await.unwrap().is_some());
    // At the ceiling, a stream joins the least busy connection.
    let d = lease.open_stream().bounded().await.unwrap();
    assert_eq!(pool.stats(&to).unwrap().connections, 2);

    // A second shard raises the ceiling.
    let second = pool.attach(to.clone());
    assert_eq!(pool.stats(&to), stats(2, 2, 4, 2));

    // An overload halves the limit, and idle connections past it close.
    drop((a, b, c, d));
    second.report_overload();
    pool.adapt();
    assert_eq!(pool.stats(&to), stats(2, 1, 4, 1));
    // An idle connection takes the next stream.
    let mut e = second.open_stream().bounded().await.unwrap();
    assert_eq!(pool.stats(&to).unwrap().connections, 1);

    // The last lease releases the destination; open streams keep working.
    drop(lease);
    assert_eq!(pool.stats(&to), stats(1, 1, 2, 1));
    drop(second);
    assert_eq!(pool.stats(&to), None);
    e.send(&begin(3)).bounded().await.unwrap();
    assert!(e.recv().bounded().await.unwrap().is_some());
    pool.adapt();
    server.abort();
}

#[tokio::test]
async fn a_destination_that_does_not_answer_fails_the_stream() {
    let us = Cluster::new(US);
    let eu = Cluster::new(EU);
    let mut trust = PeerTrust::new();
    eu.trusted(&mut trust, &[]);
    let settings = EndpointSettings {
        connect_timeout: Duration::from_millis(300),
        ..settings()
    };
    let endpoint = PeerEndpoint::bind(loopback(), &us.tls("us-1", trust), settings).unwrap();
    // A port with nobody behind it.
    let silent = std::net::UdpSocket::bind(loopback()).unwrap();
    let to = Destination {
        cluster: id(EU),
        address: silent.local_addr().unwrap(),
    };
    let pool = ConnectionPool::new(endpoint, 4);
    let lease = pool.attach(to.clone());
    let error = lease.open_stream().bounded().await.unwrap_err();
    assert!(
        matches!(error, PoolError::Connect(ConnectError::Timeout(_))),
        "{error}"
    );
    assert_eq!(pool.stats(&to).unwrap().connections, 0);
    // The slot is free again: the next attempt connects anew.
    assert!(lease.open_stream().bounded().await.is_err());
}
