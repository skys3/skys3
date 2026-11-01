//! QUIC over the simulated network: Quinn's [`AsyncUdpSocket`] on a
//! turmoil UDP socket, so that the peer transport runs its real endpoint,
//! handshake, streams, and connection pool between simulated hosts
//! (plan M6-08).
//!
//! Quinn needs three things of its socket: to send a datagram without
//! waiting, to be woken when one arrives, and its address. Turmoil's
//! socket sends at once, since the simulated network has no backpressure,
//! and its `readable` waits for the next datagram. Quinn's clock is
//! Tokio's, which turmoil runs paused and advances step by step, so its
//! timers, pacing, and loss detection run on simulated time.
//!
//! The socket offers neither segmentation offload nor MTU discovery
//! ([`AsyncUdpSocket::may_fragment`]): every datagram is at most Quinn's
//! initial 1,200 bytes, whatever the run, so the network carries as many
//! datagrams for the same messages in every run of a seed.

use std::fmt;
use std::future::Future;
use std::io::{self, IoSliceMut};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use turmoil::net::UdpSocket;

/// A pending wait for the socket to become readable.
type Readable = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// A turmoil UDP socket that Quinn can drive.
pub(crate) struct SimUdp {
    socket: Arc<UdpSocket>,
    /// The wait for the next datagram, kept across polls: turmoil's
    /// `readable` holds the socket's receive queue until a datagram
    /// arrives, so only one may run at a time.
    readable: Mutex<Option<Readable>>,
}

impl fmt::Debug for SimUdp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimUdp")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

impl SimUdp {
    /// Binds a socket on `port` of every address of the current host.
    pub(crate) async fn bind(port: u16) -> io::Result<Arc<Self>> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
        Ok(Arc::new(Self {
            socket: Arc::new(socket),
            readable: Mutex::new(None),
        }))
    }
}

impl AsyncUdpSocket for SimUdp {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Writable)
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        // One datagram per transmit: the socket offers no segmentation
        // offload (`max_transmit_segments` is 1).
        self.socket
            .try_send_to(transmit.contents, transmit.destination)
            .map(drop)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let (Some(buf), Some(slot)) = (bufs.first_mut(), meta.first_mut()) else {
            return Poll::Ready(Ok(0));
        };
        let mut readable = self.readable.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if readable.is_none() {
                match self.socket.try_recv_from(buf) {
                    Ok((len, addr)) => {
                        *slot = RecvMeta {
                            addr,
                            len,
                            stride: len,
                            ecn: None,
                            dst_ip: None,
                        };
                        return Poll::Ready(Ok(1));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Poll::Ready(Err(error)),
                }
                let socket = Arc::clone(&self.socket);
                *readable = Some(Box::pin(async move { socket.readable().await }));
            }
            let wait = readable.as_mut().expect("a wait was just set");
            match wait.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    *readable = None;
                    result?;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    fn may_fragment(&self) -> bool {
        // Keeps Quinn from probing for a larger MTU.
        true
    }
}

/// The simulated network has no backpressure: a socket is always writable.
#[derive(Debug)]
struct Writable;

impl UdpPoller for Writable {
    fn poll_writable(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use skys3_config::BucketPair;
    use skys3_peer::{Destination, EndpointSettings, PeerEndpoint, PeerTls, PeerTrust};
    use skys3_types::{BucketId, BucketName, ClusterId, NodeId};

    use super::*;
    use crate::pki::Pki;

    /// What a source saw of one connection to a destination.
    #[derive(Debug, PartialEq, Eq)]
    struct Seen {
        connected_at: Duration,
        datagrams: (u64, u64),
        bytes: (u64, u64),
    }

    /// Two clusters, each one node, the source connecting to the
    /// destination over the simulated network and exchanging a stream.
    fn run(seed: u64) -> Seen {
        let source = ClusterId::new("src").unwrap();
        let destination = ClusterId::new("dst").unwrap();
        let (source_pki, destination_pki) = (Pki::ed25519().unwrap(), Pki::ed25519().unwrap());
        let pair = BucketPair {
            source: BucketId::new("b-1").unwrap(),
            destination: BucketName::new("sink").unwrap(),
        };
        let tls = |pki: &Pki, cluster: &ClusterId, other: (&ClusterId, &Pki)| {
            let mut trust = PeerTrust::new();
            trust
                .add(
                    other.0.clone(),
                    std::slice::from_ref(other.1.ca()),
                    [pair.clone()],
                )
                .unwrap();
            let credentials = pki.node(cluster, &NodeId::new("n-1").unwrap()).unwrap();
            PeerTls::new(&credentials, Arc::new(trust)).unwrap()
        };
        let source_tls = tls(&source_pki, &source, (&destination, &destination_pki));
        let destination_tls = tls(&destination_pki, &destination, (&source, &source_pki));
        let settings = EndpointSettings {
            idle_timeout: Duration::from_secs(4),
            ..EndpointSettings::from_config(&skys3_config::PeeringConfig::default())
        };
        let mut sim = turmoil::Builder::new()
            .rng_seed(seed)
            .min_message_latency(Duration::from_millis(5))
            .max_message_latency(Duration::from_millis(15))
            .build();
        let server_settings = settings.clone();
        sim.host("dst", move || {
            let (tls, settings) = (destination_tls.clone(), server_settings.clone());
            async move {
                let socket = SimUdp::bind(7600).await?;
                let endpoint = PeerEndpoint::with_socket(socket, &tls, settings, Some(seed))?;
                while let Some(incoming) = endpoint.accept().await {
                    let connection = incoming.establish().await?;
                    tokio::spawn(async move {
                        while let Ok(mut stream) = connection.accept_stream().await {
                            while let Ok(Some(_)) = stream.recv().await {}
                        }
                    });
                }
                Ok(())
            }
        });
        let seen = Arc::new(Mutex::new(None));
        let out = Arc::clone(&seen);
        sim.client("src", async move {
            let socket = SimUdp::bind(7601).await?;
            let endpoint = PeerEndpoint::with_socket(socket, &source_tls, settings, Some(!seed))?;
            let address = turmoil::lookup("dst");
            let connection = endpoint
                .connect(&Destination {
                    cluster: destination.clone(),
                    address: SocketAddr::new(address, 7600),
                })
                .await?;
            let connected_at = turmoil::sim_elapsed().unwrap_or_default();
            assert_eq!(connection.peer(), &destination);
            let stream = connection.open_stream().await?;
            let (mut send, _) = stream.split();
            let abort = skys3_peer::Message::Abort(skys3_peer::Abort {
                identity: "src/b-1/0/1.2".parse().unwrap(),
                reason: skys3_peer::AbortReason::Cancelled,
                detail: "x".repeat(200),
            });
            for _ in 0..50 {
                send.send(&abort).await?;
            }
            send.finish()?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let stats = connection.stats();
            *out.lock().unwrap() = Some(Seen {
                connected_at,
                datagrams: (stats.udp_tx.datagrams, stats.udp_rx.datagrams),
                bytes: (stats.udp_tx.bytes, stats.udp_rx.bytes),
            });
            Ok(())
        });
        sim.run().unwrap();
        seen.lock().unwrap().take().unwrap()
    }

    #[test]
    fn quic_runs_over_the_simulated_network_and_replays_exactly() {
        for seed in 0..4 {
            let first = run(seed);
            assert!(first.datagrams.0 > 3, "{first:?}");
            assert_eq!(first, run(seed), "seed {seed}");
        }
    }
}
