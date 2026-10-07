//! Reads of coded objects (§8.5): every range reads back the object's
//! bytes while up to `m` fragments of each stripe are missing or corrupt,
//! a healthy read fetches only data fragments, and a stripe that lost more
//! than `m` fails the read rather than returning other bytes.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use skys3_ec::fragment::StripeInfo;
use skys3_ec::read::PIECE_LEN;
use skys3_ec::read::seeded::{ReadBug, seed_read_bug};
use skys3_ec::{
    CodecId, CodedRead, CodedReadError, CodedStripe, FragmentBytes, FragmentId, FragmentIdentity,
    FragmentLocation, FragmentReadError, FragmentRequest, FragmentSource, Geometry, current_codec,
    read_coded,
};
use skys3_types::NodeId;
use support::{position, runtime, sample, shard};

/// How a fragment fails when it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// Its node holds no such fragment.
    NotHeld,
    /// Its node cannot read it intact.
    Damaged,
    /// Its node does not answer.
    Unreachable,
    /// Its bytes arrive changed, with the CRC32C of the right ones.
    Corrupt,
}

const LOSSES: [Loss; 4] = [Loss::NotHeld, Loss::Damaged, Loss::Unreachable, Loss::Corrupt];

/// Fragments held in memory by node and ID, failing as planned, with
/// every request recorded.
#[derive(Debug, Default)]
struct Nodes {
    fragments: BTreeMap<(NodeId, FragmentId), (FragmentIdentity, Bytes)>,
    losses: BTreeMap<(NodeId, FragmentId), Loss>,
    requests: Mutex<Vec<FragmentRequest>>,
}

impl Nodes {
    fn requests(&self) -> Vec<FragmentRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl FragmentSource for Nodes {
    fn read(&self, request: FragmentRequest) -> skys3_ec::read::ReadFuture<'_> {
        Box::pin(async move {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(request.clone());
            let key = (request.node.clone(), request.fragment);
            let node = request.node.clone();
            let failure = |loss| match loss {
                Loss::NotHeld => FragmentReadError::NotHeld {
                    node: node.clone(),
                    reason: "gone".to_owned(),
                },
                Loss::Damaged => FragmentReadError::Damaged {
                    node: node.clone(),
                    reason: "a block fails its checksum".to_owned(),
                },
                _ => FragmentReadError::Unreachable {
                    node: node.clone(),
                    reason: "down".to_owned(),
                },
            };
            let Some((identity, bytes)) = self.fragments.get(&key) else {
                return Err(failure(Loss::NotHeld));
            };
            // A node serves only the fragment the request names.
            assert_eq!(*identity, request.identity, "the request names its fragment");
            let range = request.range.start as usize..request.range.end as usize;
            let data = bytes.slice(range);
            let crc32c = crc32c::crc32c(&data);
            match self.losses.get(&key) {
                None => Ok(FragmentBytes { data, crc32c }),
                Some(Loss::Corrupt) => {
                    let mut changed = data.to_vec();
                    changed[0] ^= 0x5a;
                    Ok(FragmentBytes {
                        data: changed.into(),
                        crc32c,
                    })
                }
                Some(loss) => Err(failure(*loss)),
            }
        })
    }
}

/// An object of `data` coded in stripes of `stripe_len` bytes as
/// `geometry`, each fragment of a stripe on its own node: with `shared`,
/// the stripes rotate over `k + m + 1` nodes, and otherwise no two
/// stripes share a node.
fn coded_on(
    geometry: Geometry,
    data: &[u8],
    stripe_len: usize,
    shared: bool,
) -> (Vec<CodedStripe>, Nodes) {
    let codec = current_codec();
    let width = geometry.total_fragments();
    let chunks: Vec<&[u8]> = data.chunks(stripe_len).collect();
    let mut nodes = Nodes::default();
    let mut stripes = Vec::new();
    let mut next_id = 1u128;
    for (number, chunk) in chunks.iter().enumerate() {
        let offset = (number * stripe_len) as u64;
        let fragments = codec.encode(geometry, chunk).unwrap();
        let info = StripeInfo {
            number: number as u32,
            count: chunks.len() as u32,
            offset,
            data_len: chunk.len() as u64,
            geometry,
            codec: CodecId::CURRENT,
        };
        let mut locations = Vec::new();
        for (index, fragment) in fragments.into_iter().enumerate() {
            let node: NodeId = if shared {
                format!("n{}", (number + index) % (width + 1))
            } else {
                format!("s{number}-n{index}")
            }
            .parse()
            .unwrap();
            let id = FragmentId::new(next_id);
            next_id += 1;
            let identity = FragmentIdentity {
                shard: shard(),
                key: "k".to_owned(),
                version: position(4, 18),
                stripe: info,
                index: index as u8,
            };
            nodes
                .fragments
                .insert((node.clone(), id), (identity, fragment.into()));
            locations.push(FragmentLocation { node, fragment: id });
        }
        let stripe = CodedStripe::new(
            number as u32,
            offset,
            chunk.len() as u64,
            geometry,
            CodecId::CURRENT,
            locations,
        )
        .unwrap();
        stripes.push(stripe);
    }
    (stripes, nodes)
}

/// As [`coded_on`], with no node shared between stripes.
fn coded(geometry: Geometry, data: &[u8], stripe_len: usize) -> (Vec<CodedStripe>, Nodes) {
    coded_on(geometry, data, stripe_len, false)
}

/// Loses fragment `index` of every stripe as `loss`.
fn lose(stripes: &[CodedStripe], nodes: &mut Nodes, index: usize, loss: Loss) {
    for stripe in stripes {
        let location = &stripe.fragments()[index];
        nodes
            .losses
            .insert((location.node.clone(), location.fragment), loss);
    }
}

fn request(stripes: Vec<CodedStripe>, size: usize, range: Range<u64>) -> CodedRead {
    CodedRead {
        shard: shard(),
        key: "k".to_owned(),
        version: position(4, 18),
        size: size as u64,
        stripes,
        range,
    }
}

/// Reads `range` of the object to its end: the bytes, or the error that
/// stopped the read, before or during the stream.
async fn read(
    nodes: Arc<Nodes>,
    stripes: Vec<CodedStripe>,
    size: usize,
    range: Range<u64>,
) -> Result<Vec<u8>, String> {
    let mut body = read_coded(nodes, request(stripes, size, range))
        .await
        .map_err(|error| error.to_string())?;
    let mut data = Vec::new();
    while let Some(piece) = body.recv().await {
        data.extend_from_slice(&piece.map_err(|error| error.to_string())?);
    }
    Ok(data)
}

fn geometry(k: usize, m: usize) -> Geometry {
    Geometry::new(k, m).unwrap()
}

/// Every loss pattern of up to `m` fragments of each stripe, each fragment
/// missing or corrupt, for every geometry of the design: the whole object
/// and a range cutting through fragments read back exactly.
#[test]
fn every_loss_pattern_up_to_m_reads_back() {
    let runtime = runtime();
    for (n, geometry) in Geometry::DESIGN_TABLE.into_iter().enumerate() {
        let (k, width) = (geometry.data_fragments(), geometry.total_fragments());
        // Two stripes and a short third; fragments of 192 bytes.
        let stripe_len = 64 * 3 * k - 7;
        let size = stripe_len * 2 + 100;
        let data = sample(size, n as u64 + 1);
        let ranges = [0..size as u64, 70..(stripe_len + 300) as u64];
        let mut patterns = 0;
        for lost in 0u32..1 << width {
            if lost.count_ones() as usize > geometry.parity_fragments() {
                continue;
            }
            let indices: Vec<usize> = (0..width).filter(|i| lost & (1 << i) != 0).collect();
            for kinds in 0u32..1 << indices.len() {
                let (stripes, mut nodes) = coded(geometry, &data, stripe_len);
                for (j, index) in indices.iter().enumerate() {
                    let loss = if kinds & (1 << j) == 0 {
                        Loss::NotHeld
                    } else {
                        Loss::Corrupt
                    };
                    lose(&stripes, &mut nodes, *index, loss);
                }
                let nodes = Arc::new(nodes);
                for range in &ranges {
                    let read = runtime
                        .block_on(read(
                            Arc::clone(&nodes),
                            stripes.clone(),
                            size,
                            range.clone(),
                        ))
                        .unwrap_or_else(|e| panic!("{geometry}, lost {indices:?}: {e}"));
                    let expected = &data[range.start as usize..range.end as usize];
                    assert!(read == expected, "{geometry}, lost {indices:?} as {kinds:b}");
                }
                patterns += 1;
            }
        }
        // 1 + 2·C(w, 1) + 4·C(w, 2) patterns for two parity fragments.
        assert_eq!(patterns, 1 + 2 * width + 2 * width * (width - 1));
    }
}

/// A healthy read asks only the data fragments covering its range, once
/// per piece, and never a parity fragment.
#[test]
fn a_healthy_read_fetches_only_the_data_it_needs() {
    let runtime = runtime();
    let g = Geometry::RS_4_2;
    // One stripe of 4 MiB: four data fragments of 1 MiB, one piece each.
    let size = 4 << 20;
    let data = sample(size, 3);
    let (stripes, nodes) = coded(g, &data, size);
    let nodes = Arc::new(nodes);
    // From inside fragment 1 to inside fragment 2.
    let range = (1 << 20) + 5..(2 << 20) + 9;
    let read = runtime
        .block_on(read(
            Arc::clone(&nodes),
            stripes.clone(),
            size,
            range.clone(),
        ))
        .unwrap();
    assert!(read == data[range.start as usize..range.end as usize]);
    let asked: Vec<(u8, Range<u64>)> = nodes
        .requests()
        .iter()
        .map(|r| (r.identity.index, r.range.clone()))
        .collect();
    assert_eq!(asked, [(1, 5..PIECE_LEN), (2, 0..9)]);
}

/// A fragment longer than a piece is read in pieces, and a lost one is
/// decoded piece by piece, without asking the lost fragment again.
#[test]
fn long_fragments_are_read_and_decoded_in_pieces() {
    let runtime = runtime();
    let g = geometry(2, 1);
    let size = (5 << 20) + 3;
    let data = sample(size, 4);
    let (stripes, mut nodes) = coded(g, &data, size);
    lose(&stripes, &mut nodes, 0, Loss::Damaged);
    let nodes = Arc::new(nodes);
    let read = runtime
        .block_on(read(Arc::clone(&nodes), stripes, size, 0..size as u64))
        .unwrap();
    assert!(read == data);
    let requests = nodes.requests();
    let of = |index: u8| requests.iter().filter(|r| r.identity.index == index).count();
    // Fragment 0, 2.5 MiB, is asked once; its three pieces are decoded
    // from fragments 1 and 2, and fragment 1's own three pieces are read.
    assert_eq!((of(0), of(1), of(2)), (1, 6, 3));
    assert!(requests.iter().all(|r| r.range.end - r.range.start <= PIECE_LEN));
}

/// A node that does not answer is not asked again during the read, for
/// any stripe.
#[test]
fn an_unreachable_node_is_asked_once() {
    let runtime = runtime();
    let g = Geometry::RS_3_2;
    let stripe_len = 3 * 64;
    let size = stripe_len * 6;
    let data = sample(size, 5);
    let (stripes, mut nodes) = coded_on(g, &data, stripe_len, true);
    // Every stripe has a fragment on n2.
    let down: NodeId = "n2".parse().unwrap();
    for stripe in &stripes {
        for location in stripe.fragments() {
            if location.node == down {
                nodes
                    .losses
                    .insert((down.clone(), location.fragment), Loss::Unreachable);
            }
        }
    }
    let nodes = Arc::new(nodes);
    let read = runtime
        .block_on(read(Arc::clone(&nodes), stripes, size, 0..size as u64))
        .unwrap();
    assert!(read == data);
    let asked = nodes.requests().iter().filter(|r| r.node == down).count();
    assert_eq!(asked, 1);
}

/// A stripe with more than `m` fragments lost fails the read: before it
/// begins if it is the first piece's, mid-stream otherwise.
#[test]
fn more_than_m_losses_fail_the_read() {
    let runtime = runtime();
    let g = Geometry::RS_4_2;
    let stripe_len = 4 * 64;
    let size = stripe_len * 2;
    let data = sample(size, 6);
    let (stripes, mut nodes) = coded(g, &data, stripe_len);
    // Three fragments of the second stripe only.
    for index in [0, 4, 5] {
        let location = &stripes[1].fragments()[index];
        nodes
            .losses
            .insert((location.node.clone(), location.fragment), Loss::NotHeld);
    }
    let nodes = Arc::new(nodes);
    runtime.block_on(async {
        let mut body = read_coded(
            Arc::clone(&nodes) as Arc<dyn FragmentSource>,
            request(stripes.clone(), size, 0..size as u64),
        )
        .await
        .unwrap();
        let mut pieces = Vec::new();
        while let Some(piece) = body.recv().await {
            pieces.push(piece);
        }
        let error = pieces.last().unwrap().as_ref().unwrap_err();
        assert!(error.to_string().contains("3 of the 4"), "{error}");
        assert!(pieces[..pieces.len() - 1].iter().all(Result::is_ok));

        let error = read_coded(nodes, request(stripes, size, stripe_len as u64..size as u64))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            CodedReadError::Unreadable {
                stripe: 1,
                readable: 3,
                needed: 4
            }
        );
    });
}

#[test]
fn layouts_must_cover_the_object_and_the_range() {
    let runtime = runtime();
    let g = Geometry::RS_3_2;
    let data = sample(1000, 7);
    let (stripes, nodes) = coded(g, &data, 400);
    let nodes: Arc<dyn FragmentSource> = Arc::new(nodes);
    runtime.block_on(async {
        let layout = |read| async {
            matches!(
                read_coded(Arc::clone(&nodes), read).await,
                Err(CodedReadError::Layout(_))
            )
        };
        // A range past the object.
        assert!(layout(request(stripes.clone(), 1000, 900..1001)).await);
        // A missing stripe.
        let gap = vec![stripes[0].clone(), stripes[2].clone()];
        assert!(layout(request(gap, 1000, 0..10)).await);
        // Stripes that end before the object does.
        assert!(layout(request(stripes[..2].to_vec(), 1000, 0..10)).await);
        // An empty range reads nothing.
        let mut body = read_coded(Arc::clone(&nodes), request(stripes, 1000, 5..5))
            .await
            .unwrap();
        assert!(body.recv().await.is_none());
    });
}

/// The seeded bugs return other bytes than the object's, which is what
/// the cluster simulation must catch.
#[test]
fn seeded_bugs_read_wrong_bytes() {
    let runtime = runtime();
    let g = Geometry::RS_3_2;
    let size = 3 * 64 * 4;
    let data = sample(size, 8);
    for (bug, loss) in [
        (ReadBug::TrustCrc, Loss::Corrupt),
        (ReadBug::WrongIndices, Loss::NotHeld),
    ] {
        let (stripes, mut nodes) = coded(g, &data, size);
        lose(&stripes, &mut nodes, 1, loss);
        let nodes = Arc::new(nodes);
        seed_read_bug(Some(bug));
        let read = runtime.block_on(read(nodes, stripes, size, 0..size as u64));
        seed_read_bug(None);
        assert_ne!(read.unwrap(), data, "{bug:?}");
    }
}

/// A read of `geometry` with `losses` per stripe, drawn from `seed`.
fn losses_of(geometry: Geometry, stripes: usize, seed: u64) -> Vec<Vec<(usize, Loss)>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..stripes)
        .map(|_| {
            let mut indices: Vec<usize> = (0..geometry.total_fragments()).collect();
            indices.shuffle(&mut rng);
            let count = rng.random_range(0..=geometry.parity_fragments());
            indices[..count]
                .iter()
                .map(|index| (*index, LOSSES[rng.random_range(0..LOSSES.len())]))
                .collect()
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Any geometry, object, stripe size, and range, with up to `m`
    /// fragments of each stripe lost in any way, reads back exactly.
    #[test]
    fn degraded_reads_return_the_objects_bytes(
        (k, m) in prop_oneof![
            Just((1, 1)), Just((2, 1)), Just((3, 2)), Just((4, 2)), Just((5, 3)),
            Just((6, 2)), Just((8, 2)), Just((10, 4)),
        ],
        size in 1usize..12_000,
        stripe_len in 1usize..5_000,
        bounds in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        seed in any::<u64>(),
    ) {
        let geometry = geometry(k, m);
        let data = sample(size, seed);
        let (stripes, mut nodes) = coded(geometry, &data, stripe_len);
        let mut lost = BTreeSet::new();
        for (stripe, losses) in losses_of(geometry, stripes.len(), seed).iter().enumerate() {
            for (index, loss) in losses {
                let location = &stripes[stripe].fragments()[*index];
                nodes.losses.insert((location.node.clone(), location.fragment), *loss);
                lost.insert((stripe, *index));
            }
        }
        let (a, b) = (bounds.0.index(size + 1), bounds.1.index(size + 1));
        let range = a.min(b) as u64..a.max(b) as u64;
        let read = runtime()
            .block_on(read(Arc::new(nodes), stripes, size, range.clone()))
            .map_err(TestCaseError::fail)?;
        prop_assert!(read == data[range.start as usize..range.end as usize], "lost {:?}", lost);
    }
}
