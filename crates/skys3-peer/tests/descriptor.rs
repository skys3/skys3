//! Peer descriptors (design §7.8): signed by a destination node's peer
//! identity, verified against the source's trust bundle, and refused when
//! forged, tampered with, expired, or meant for another bucket or source.

mod common;

use std::time::Duration;

use common::*;
use proptest::prelude::*;
use skys3_peer::{
    DESCRIPTOR_CLOCK_SKEW, DESCRIPTOR_LIFETIME, DescriptorError, DescriptorVerifier, Expected,
    MAX_DESCRIPTOR_LIFETIME, PeerDescriptor, PeerTls, PeerTrust, VersionRange,
};
use skys3_types::{BucketName, ClusterId};

const US: &str = "prod-us";
const EU: &str = "prod-eu";
const AP: &str = "prod-ap";

/// A time within every test certificate's validity.
const NOW: Duration = Duration::from_secs(1_800_000_000);

fn bucket(name: &str) -> BucketName {
    BucketName::new(name).unwrap()
}

/// The descriptor of the EU cluster's `archive`, which receives from US.
fn descriptor(now: Duration) -> PeerDescriptor {
    PeerDescriptor::new(
        id(EU),
        bucket("archive"),
        id(US),
        vec![
            "quic.eu.example.internal:7443".to_owned(),
            "198.51.100.7:7443".to_owned(),
        ],
        now,
    )
}

struct World {
    eu: Cluster,
    /// What US trusts: EU only.
    verifier: DescriptorVerifier,
    /// EU's destination node.
    signer: PeerTls,
}

fn world() -> World {
    let (us, eu) = (Cluster::new(US), Cluster::new(EU));
    let mut trust = PeerTrust::new();
    eu.trusted(&mut trust, &[]);
    let verifier = us.tls("us-1", trust).descriptor_verifier();
    let mut eu_trust = PeerTrust::new();
    us.trusted(&mut eu_trust, &[]);
    let signer = eu.tls("eu-1", eu_trust);
    World {
        eu,
        verifier,
        signer,
    }
}

fn expected<'a>(bucket: &'a BucketName, source: &'a ClusterId) -> Expected<'a> {
    Expected { bucket, source }
}

#[test]
fn a_signed_descriptor_verifies_and_reads_back() {
    let world = world();
    let signed = world
        .signer
        .descriptor_signer()
        .sign(&descriptor(NOW))
        .unwrap();
    let (archive, us) = (bucket("archive"), id(US));
    let verified = world
        .verifier
        .verify(&signed, NOW, expected(&archive, &us))
        .unwrap();
    assert_eq!(verified, descriptor(NOW));
    assert_eq!(
        PeerDescriptor::decode_unverified(&signed).unwrap(),
        verified
    );
    // A verifier built from the trust alone checks the same.
    let mut trust = PeerTrust::new();
    world.eu.trusted(&mut trust, &[]);
    let alone = DescriptorVerifier::new(std::sync::Arc::new(trust));
    assert_eq!(
        alone.verify(&signed, NOW, expected(&archive, &us)).unwrap(),
        verified
    );
    assert!(format!("{:?}", world.signer.descriptor_signer()).contains("DescriptorSigner"));
}

#[test]
fn forged_descriptors_are_refused() {
    let world = world();
    let (archive, us) = (bucket("archive"), id(US));
    let check = |signed: &[u8]| {
        world
            .verifier
            .verify(signed, NOW, expected(&archive, &us))
            .unwrap_err()
    };

    // Signed by a node of a cluster US does not trust.
    let ap = Cluster::new(AP);
    let stranger = ap.tls("ap-1", PeerTrust::new()).descriptor_signer();
    let mut claims_ap = descriptor(NOW);
    claims_ap.cluster = id(AP);
    assert!(matches!(
        check(&stranger.sign(&claims_ap).unwrap()),
        DescriptorError::Untrusted(_)
    ));
    // ... or claiming to be EU: the chain does not name EU.
    assert!(matches!(
        check(&stranger.sign(&descriptor(NOW)).unwrap()),
        DescriptorError::Untrusted(_)
    ));
    // A certificate of EU's own CA that names another cluster.
    let impostor = PeerTls::new(
        &world.eu.ca.credentials(AP, "ap-1"),
        std::sync::Arc::new(PeerTrust::new()),
    )
    .unwrap()
    .descriptor_signer();
    assert!(matches!(
        check(&impostor.sign(&descriptor(NOW)).unwrap()),
        DescriptorError::Untrusted(_)
    ));

    // A valid descriptor whose body changed after signing: the chain and
    // signature are EU's, the addresses an attacker's.
    let signed = world
        .signer
        .descriptor_signer()
        .sign(&descriptor(NOW))
        .unwrap();
    let mut tampered = signed.to_vec();
    let at = tampered
        .windows(3)
        .position(|w| w == b"198")
        .expect("the address is in the body");
    tampered[at] = b'2';
    assert_eq!(check(&tampered), DescriptorError::Signature);
    // A signature cut short.
    let mut cut = signed.to_vec();
    let last = cut.len() - 1;
    cut[last] ^= 0x01;
    assert!(
        world
            .verifier
            .verify(&cut, NOW, expected(&archive, &us))
            .is_err()
    );
}

#[test]
fn descriptors_are_valid_only_for_their_time_bucket_and_source() {
    let world = world();
    let signer = world.signer.descriptor_signer();
    let (archive, us) = (bucket("archive"), id(US));
    let verify = |descriptor: &PeerDescriptor, now: Duration| {
        world.verifier.verify(
            &signer.sign(descriptor).unwrap(),
            now,
            expected(&archive, &us),
        )
    };
    // Expired, or issued too far ahead of the source's clock.
    assert_eq!(
        verify(&descriptor(NOW), NOW + DESCRIPTOR_LIFETIME),
        Err(DescriptorError::Expired)
    );
    assert!(
        verify(
            &descriptor(NOW),
            NOW + DESCRIPTOR_LIFETIME - Duration::from_secs(1)
        )
        .is_ok()
    );
    let ahead = NOW + DESCRIPTOR_CLOCK_SKEW;
    assert!(verify(&descriptor(ahead), NOW).is_ok());
    assert_eq!(
        verify(&descriptor(ahead + Duration::from_secs(1)), NOW),
        Err(DescriptorError::NotYetValid)
    );
    let mut long = descriptor(NOW);
    long.expires = NOW + MAX_DESCRIPTOR_LIFETIME + Duration::from_secs(1);
    assert_eq!(verify(&long, NOW), Err(DescriptorError::Lifetime));

    // For another bucket, or a bucket that receives from someone else.
    let other = bucket("other");
    assert!(matches!(
        world.verifier.verify(
            &signer.sign(&descriptor(NOW)).unwrap(),
            NOW,
            expected(&other, &us)
        ),
        Err(DescriptorError::Bucket { .. })
    ));
    let ap = id(AP);
    assert!(matches!(
        world.verifier.verify(
            &signer.sign(&descriptor(NOW)).unwrap(),
            NOW,
            expected(&archive, &ap)
        ),
        Err(DescriptorError::Source { .. })
    ));
    // No common protocol version.
    let mut newer = descriptor(NOW);
    newer.versions = VersionRange::new(2, 9).unwrap();
    assert_eq!(verify(&newer, NOW), Err(DescriptorError::Versions));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// Any descriptor round-trips through signing and verification, and
    /// no single changed byte of the signed form verifies to anything else.
    #[test]
    fn signing_round_trips_and_resists_tampering(
        addresses in prop::collection::vec("[a-z]{1,12}(\\.[a-z]{1,8}){0,2}:[1-9][0-9]{0,3}", 1..=16),
        issued_offset in 0u64..3_600,
        lifetime in 1u64..=86_400,
        flip in any::<prop::sample::Index>(),
        bit in 0u8..8,
    ) {
        let world = world();
        let issued = NOW - Duration::from_secs(issued_offset);
        let mut sent = descriptor(issued);
        sent.addresses = addresses;
        sent.expires = issued + Duration::from_secs(lifetime);
        let signed = world.signer.descriptor_signer().sign(&sent).unwrap();
        let (archive, us) = (bucket("archive"), id(US));
        let result = world.verifier.verify(&signed, NOW, expected(&archive, &us));
        if NOW < sent.expires {
            prop_assert_eq!(result.unwrap(), sent.clone());
        } else {
            prop_assert_eq!(result, Err(DescriptorError::Expired));
        }
        let mut tampered = signed.to_vec();
        let at = flip.index(tampered.len());
        tampered[at] ^= 1 << bit;
        if let Ok(read) = world.verifier.verify(&tampered, NOW, expected(&archive, &us)) {
            // Only bytes the signature does not cover may change, and
            // then nothing the descriptor says does.
            prop_assert_eq!(read, sent);
        }
    }
}
