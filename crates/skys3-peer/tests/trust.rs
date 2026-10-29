//! Peer trust bundles from configuration, bucket-pair authorization, and
//! the TLS configurations built on them.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use skys3_config::{PeerConfig, PeeringConfig};
use skys3_peer::{ALPN, PeerTls, PeerTrust, TrustError, Unauthorized};
use skys3_types::{BucketName, WriteIdentity};

fn pem(der: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = "-----BEGIN CERTIFICATE-----\n".to_owned();
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap());
        out.push('\n');
    }
    out + "-----END CERTIFICATE-----\n"
}

fn config(peers: &[(&str, std::path::PathBuf)]) -> PeeringConfig {
    PeeringConfig {
        peers: peers
            .iter()
            .map(|(cluster, ca_file)| {
                (
                    id(cluster),
                    PeerConfig {
                        ca_file: ca_file.clone(),
                        buckets: vec![pair("b-src", "archive")],
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
        ..PeeringConfig::default()
    }
}

#[test]
fn trust_bundles_load_from_the_peering_section() {
    let dir = tempfile::tempdir().unwrap();
    let us = Ca::new("prod-us");
    let eu = Ca::new("prod-eu");
    let bundle = dir.path().join("us.crt");
    std::fs::write(&bundle, pem(&us.cert()) + &pem(&eu.cert())).unwrap();

    let trust = PeerTrust::load(&config(&[("prod-us", bundle.clone())])).unwrap();
    assert!(trust.trusts(&id("prod-us")));
    assert!(!trust.trusts(&id("prod-eu")));
    let identity: WriteIdentity = "prod-us/b-src/5/42.1".parse().unwrap();
    trust
        .authorize(
            &id("prod-us"),
            &identity,
            Some(&BucketName::new("archive").unwrap()),
        )
        .unwrap();
    assert!(format!("{trust:?}").contains("prod-us"));

    let missing = dir.path().join("missing.crt");
    let error = PeerTrust::load(&config(&[("prod-us", missing)])).unwrap_err();
    assert!(matches!(error, TrustError::Read { .. }), "{error}");
    assert!(error.to_string().contains("missing.crt"), "{error}");

    let empty = dir.path().join("empty.crt");
    std::fs::write(&empty, "").unwrap();
    let error = PeerTrust::load(&config(&[("prod-us", empty)])).unwrap_err();
    assert!(matches!(error, TrustError::NoCa(_)), "{error}");

    let mut trust = PeerTrust::new();
    let error = trust
        .add_pem(
            id("prod-us"),
            b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
            [],
        )
        .unwrap_err();
    assert!(matches!(error, TrustError::Pem { .. }), "{error}");
    let error = trust
        .add_pem(id("prod-us"), pem(b"not a certificate").as_bytes(), [])
        .unwrap_err();
    assert!(matches!(error, TrustError::Certificate { .. }), "{error}");
    assert!(error.to_string().contains("prod-us"), "{error}");
}

#[test]
fn peers_write_only_their_authorized_pairs() {
    let mut trust = PeerTrust::new();
    trust
        .add(
            id("prod-us"),
            &[Ca::new("prod-us").cert()],
            [pair("b-src", "archive"), pair("b-src", "mirror")],
        )
        .unwrap();
    let us = id("prod-us");
    let ours: WriteIdentity = "prod-us/b-src/5/42.1".parse().unwrap();
    let other_bucket: WriteIdentity = "prod-us/b-other/5/42.1".parse().unwrap();
    let foreign: WriteIdentity = "prod-ap/b-src/5/42.1".parse().unwrap();
    let bucket = |name: &str| BucketName::new(name).unwrap();

    for destination in ["archive", "mirror"] {
        trust
            .authorize(&us, &ours, Some(&bucket(destination)))
            .unwrap();
    }
    trust.authorize(&us, &ours, None).unwrap();
    assert!(matches!(
        trust.authorize(&us, &ours, Some(&bucket("logs"))),
        Err(Unauthorized::BucketPair { .. })
    ));
    assert!(matches!(
        trust.authorize(&us, &other_bucket, Some(&bucket("archive"))),
        Err(Unauthorized::BucketPair { .. })
    ));
    assert!(matches!(
        trust.authorize(&us, &other_bucket, None),
        Err(Unauthorized::SourceBucket { .. })
    ));
    let error = trust
        .authorize(&us, &foreign, Some(&bucket("archive")))
        .unwrap_err();
    assert!(
        matches!(error, Unauthorized::ForeignIdentity { .. }),
        "{error}"
    );
    assert!(error.to_string().contains("prod-ap"), "{error}");
    // A cluster that is not a peer may write nothing, even in its own
    // name.
    let ap = id("prod-ap");
    assert!(trust.authorize(&ap, &foreign, None).is_err());
    assert!(
        trust
            .authorize(&ap, &foreign, Some(&bucket("archive")))
            .is_err()
    );
}

#[test]
fn tls_refuses_resumption_and_early_data() {
    let us = Cluster::new("prod-us");
    let tls = us.tls("us-1", PeerTrust::new());
    assert_eq!(tls.cluster().as_str(), "prod-us");
    assert!(!tls.trust().trusts(&id("prod-eu")));
    assert!(format!("{tls:?}").contains("prod-us"));

    let server = tls.server_config();
    assert_eq!(server.max_early_data_size, 0);
    assert_eq!(server.send_tls13_tickets, 0);
    assert_eq!(server.alpn_protocols, [ALPN.to_vec()]);
    let client = tls.client_config();
    assert!(!client.enable_early_data);
    assert!(!client.enable_sni);
    assert_eq!(client.alpn_protocols, [ALPN.to_vec()]);

    // Only nodes peer.
    let admin = us.ca.issue("spiffe://prod-us/admin/ops");
    let admin =
        skys3_net::Credentials::new(id("prod-us"), vec![admin.cert], admin.key, &[us.ca.cert()])
            .unwrap();
    assert!(PeerTls::new(&admin, Arc::new(PeerTrust::new())).is_none());
}
