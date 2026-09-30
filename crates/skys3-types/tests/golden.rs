//! Golden vectors for the shard hash and the write identity.
//!
//! Both are persistent formats: a key's shard can never change for an
//! existing bucket, and write identities are stored in remote objects. If a
//! test here fails, the change broke compatibility; fix the code, never the
//! vectors.
//!
//! The hash vectors were computed independently of this crate, with
//! `hashlib.sha256` and `sha256sum` over the byte string the `shard` module
//! specifies, for example:
//!
//! ```text
//! $ printf 'skys3-shard-v1\0b-7f3a\0photos/cat.jpg' | sha256sum | cut -c1-16
//! b07d3a17b8ce3057
//! ```

use skys3_types::{
    BucketId, ClusterId, Epoch, EpochSeq, KeyHash, Seq, ShardCount, ShardId, WriteIdentity,
    shard_for_key,
};

struct HashVector {
    bucket: &'static str,
    key: &'static [u8],
    hash: u64,
    /// The shard for 1, 7, 8, and 256 shards.
    shards: [u8; 4],
}

const COUNTS: [u32; 4] = [1, 7, 8, 256];

const HASH_VECTORS: &[HashVector] = &[
    HashVector {
        bucket: "b-7f3a",
        key: b"photos/cat.jpg",
        hash: 0xb07d_3a17_b8ce_3057,
        shards: [0, 1, 7, 87],
    },
    HashVector {
        bucket: "b-7f3a",
        key: b"photos/dog.jpg",
        hash: 0x8347_0c5e_0dbc_76ba,
        shards: [0, 5, 2, 186],
    },
    HashVector {
        bucket: "b-0001",
        key: b"photos/cat.jpg",
        hash: 0x5d55_3eb9_c316_271f,
        shards: [0, 4, 7, 31],
    },
    HashVector {
        bucket: "b-7f3a",
        key: b"",
        hash: 0x31b2_5a6c_9d81_3bec,
        shards: [0, 4, 4, 236],
    },
    HashVector {
        bucket: "a",
        key: b"a",
        hash: 0x9103_1d49_fae7_dd0c,
        shards: [0, 0, 4, 12],
    },
    HashVector {
        bucket: "b-7f3a",
        key: "日本語/ファイル.txt".as_bytes(),
        hash: 0x18e7_5337_1e6c_e21f,
        shards: [0, 4, 7, 31],
    },
    HashVector {
        bucket: "b-7f3a",
        key: b"a\0b",
        hash: 0xc12a_97b3_c921_b048,
        shards: [0, 5, 0, 72],
    },
    HashVector {
        bucket: "b-7f3a",
        key: &[b'x'; 1024],
        hash: 0xb5ab_728a_a5fc_c01b,
        shards: [0, 3, 3, 27],
    },
    HashVector {
        bucket: "b-0123456789abcdefghijklm",
        key: b"key",
        hash: 0x497b_46cc_be41_a911,
        shards: [0, 4, 1, 17],
    },
    HashVector {
        bucket: "b-7f3a",
        key: b"a/b/c/d/e/f/g/h",
        hash: 0x3816_be24_2b88_81ed,
        shards: [0, 4, 5, 237],
    },
];

#[test]
fn shard_hash_matches_golden_vectors() {
    for vector in HASH_VECTORS {
        let bucket = BucketId::new(vector.bucket).unwrap();
        let hash = KeyHash::of(&bucket, vector.key);
        assert_eq!(
            hash.get(),
            vector.hash,
            "hash of {:?} in {}",
            vector.key,
            vector.bucket
        );
        for (count, expected) in COUNTS.into_iter().zip(vector.shards) {
            let count = ShardCount::new(count).unwrap();
            assert_eq!(hash.shard(count), ShardId::new(expected));
            assert_eq!(
                shard_for_key(&bucket, vector.key, count),
                ShardId::new(expected)
            );
        }
    }
}

#[test]
fn shard_hash_domain_tag_is_frozen() {
    assert_eq!(KeyHash::DOMAIN_TAG, b"skys3-shard-v1");
}

#[test]
fn shard_hash_spreads_keys_evenly() {
    const KEYS: u32 = 16_000;
    const SHARDS: u32 = 16;
    let bucket = BucketId::new("b-7f3a").unwrap();
    let count = ShardCount::new(SHARDS).unwrap();
    let mut per_shard = [0_u32; SHARDS as usize];
    for i in 0..KEYS {
        let key = format!("logs/2026/09/30/object-{i:06}.json");
        per_shard[usize::from(shard_for_key(&bucket, key.as_bytes(), count).get())] += 1;
    }
    // Expected 1,000 per shard, standard deviation about 31; allow 6 sigma.
    for (shard, n) in per_shard.iter().enumerate() {
        assert!((810..=1190).contains(n), "shard {shard} got {n} keys");
    }
}

struct IdentityVector {
    cluster: &'static str,
    bucket: &'static str,
    shard: u8,
    epoch: u64,
    seq: u64,
    text: &'static str,
}

const IDENTITY_VECTORS: &[IdentityVector] = &[
    IdentityVector {
        cluster: "skys3-prod-a",
        bucket: "b-7f3a",
        shard: 5,
        epoch: 42,
        seq: 1001,
        text: "skys3-prod-a/b-7f3a/5/42.1001",
    },
    IdentityVector {
        cluster: "a",
        bucket: "b",
        shard: 0,
        epoch: 0,
        seq: 0,
        text: "a/b/0/0.0",
    },
    IdentityVector {
        cluster: "c1",
        bucket: "b-0001",
        shard: 10,
        epoch: 1,
        seq: 18_446_744_073_709_551_615,
        text: "c1/b-0001/10/1.18446744073709551615",
    },
    // The longest identity: every part at its limit, exactly 96 bytes.
    IdentityVector {
        cluster: "cccccccccccccccccccccccc",
        bucket: "bbbbbbbbbbbbbbbbbbbbbbbbb",
        shard: 255,
        epoch: u64::MAX,
        seq: u64::MAX,
        text: "cccccccccccccccccccccccc/bbbbbbbbbbbbbbbbbbbbbbbbb/255/\
               18446744073709551615.18446744073709551615",
    },
];

#[test]
fn write_identity_matches_golden_vectors() {
    for vector in IDENTITY_VECTORS {
        let wid = WriteIdentity::new(
            ClusterId::new(vector.cluster).unwrap(),
            BucketId::new(vector.bucket).unwrap(),
            ShardId::new(vector.shard),
            EpochSeq::new(Epoch::new(vector.epoch), Seq::new(vector.seq)),
        );
        assert_eq!(wid.to_string(), vector.text);
        assert!(wid.matches(vector.text));
        assert_eq!(vector.text.parse::<WriteIdentity>().unwrap(), wid);
        assert!(vector.text.len() <= WriteIdentity::MAX_LEN);
    }
    let longest = IDENTITY_VECTORS.last().unwrap().text;
    assert_eq!(longest.len(), WriteIdentity::MAX_LEN);
}

#[test]
fn write_identity_limits_are_frozen() {
    assert_eq!(WriteIdentity::MAX_LEN, 96);
    assert_eq!(ClusterId::MAX_LEN, 24);
    assert_eq!(BucketId::MAX_LEN, 25);
    assert_eq!(WriteIdentity::METADATA_KEY, "skys3-wid");
}
