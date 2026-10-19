//! Golden vectors for every registered codec.
//!
//! A codec ID is a persistent format: stripes written with it must decode
//! the same way forever, and new stripes must encode the same way, or a
//! fragment rebuilt by repair would not match its stripe. If a test here
//! fails, the change (in this crate, or an upgrade of `reed-solomon-simd`)
//! broke compatibility. Fix the code or add a new codec ID, never the
//! vectors.
//!
//! Each vector in `golden/<codec>.txt` is a geometry, a data length, and a
//! seed for [`sample`], followed by the `k + m` fragments in hex. The
//! vectors cover every geometry of the design's table (§8.3), with data
//! lengths that leave padding, fill the fragments exactly, and span several
//! 64-byte blocks. The vectors of codec 1 were produced with
//! `reed-solomon-simd` 3.1.0 and cross-checked against `reed-solomon-16`
//! 0.1.0, the independent crate it was forked from, whose high-rate code is
//! the same.
//!
//! To add vectors for a new codec, register it, then run
//! `cargo test -p skys3-ec --test golden -- --ignored --nocapture` and save
//! its output as the new codec's file.

use skys3_ec::{CodecId, Geometry, codec};

/// The data of a vector: a xorshift64 stream, one byte per step.
fn sample(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

struct Vector {
    geometry: Geometry,
    data_len: usize,
    seed: u64,
    fragments: Vec<Vec<u8>>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd-length hex");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex digit"))
        .collect()
}

/// Parses a vector file: `vector <k>+<m> <data_len> <seed>` lines, each
/// followed by its fragments, one hex line each. `#` starts a comment.
fn parse(text: &str) -> Vec<Vector> {
    let mut lines = text
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'));
    let mut vectors = Vec::new();
    while let Some(header) = lines.next() {
        let fields: Vec<&str> = header.split(' ').collect();
        let ["vector", geometry, data_len, seed] = fields[..] else {
            panic!("bad vector header {header:?}");
        };
        let (k, m) = geometry.split_once('+').expect("k+m");
        let geometry = Geometry::new(k.parse().unwrap(), m.parse().unwrap()).unwrap();
        let fragments = (0..geometry.total_fragments())
            .map(|_| unhex(lines.next().expect("fragment line")))
            .collect();
        vectors.push(Vector {
            geometry,
            data_len: data_len.parse().unwrap(),
            seed: seed.parse().unwrap(),
            fragments,
        });
    }
    vectors
}

/// The vectors each codec's file holds: every design geometry with four
/// data lengths.
fn cases() -> Vec<(Geometry, usize, u64)> {
    let mut cases = Vec::new();
    for geometry in Geometry::DESIGN_TABLE {
        let k = geometry.data_fragments();
        for data_len in [1, 100, 64 * k, 2500] {
            let seed = 0x5eed_0000 + (k * 10_000 + data_len) as u64;
            cases.push((geometry, data_len, seed));
        }
    }
    cases
}

fn vectors(id: CodecId) -> Vec<Vector> {
    let text = match id {
        CodecId::REED_SOLOMON_V1 => include_str!("golden/reed_solomon_v1.txt"),
        _ => panic!("no golden vectors for codec {id}"),
    };
    parse(text)
}

/// Every registered codec has vectors, and they cover the expected cases.
#[test]
fn every_codec_has_its_vectors() {
    for raw in 1..=u16::MAX {
        let id = CodecId::new(raw);
        if codec(id).is_err() {
            continue;
        }
        let covered: Vec<_> = vectors(id)
            .iter()
            .map(|v| (v.geometry, v.data_len, v.seed))
            .collect();
        assert_eq!(covered, cases(), "codec {id}");
    }
}

#[test]
fn reed_solomon_v1_encodes_its_golden_fragments() {
    let codec = codec(CodecId::REED_SOLOMON_V1).unwrap();
    for vector in vectors(CodecId::REED_SOLOMON_V1) {
        let data = sample(vector.data_len, vector.seed);
        let fragments = codec.encode(vector.geometry, &data).unwrap();
        assert!(
            fragments == vector.fragments,
            "{} with {} bytes encodes differently",
            vector.geometry,
            vector.data_len
        );
    }
}

#[test]
fn reed_solomon_v1_decodes_its_golden_fragments() {
    let codec = codec(CodecId::REED_SOLOMON_V1).unwrap();
    for vector in vectors(CodecId::REED_SOLOMON_V1) {
        let data = sample(vector.data_len, vector.seed);
        let data_len = vector.data_len as u64;
        let total = vector.geometry.total_fragments();
        let m = vector.geometry.parity_fragments();
        // Lose the first `m` fragments, then the last `m`: decoding then
        // needs every parity fragment, then none.
        for lost in [0..m, total - m..total] {
            let slots: Vec<Option<&[u8]>> = vector
                .fragments
                .iter()
                .enumerate()
                .map(|(i, f)| (!lost.contains(&i)).then_some(f.as_slice()))
                .collect();
            let context = format!("{} with {} bytes, lost {lost:?}", vector.geometry, data_len);
            assert!(
                codec.decode(vector.geometry, data_len, &slots).unwrap() == data,
                "{context}"
            );
            let rebuilt = codec
                .reconstruct(vector.geometry, data_len, &slots)
                .unwrap();
            assert!(rebuilt == vector.fragments, "{context}");
        }
    }
}

/// Prints the vector file of the current codec. See the module docs.
#[test]
#[ignore = "generates vectors for a new codec; never regenerate an existing codec's file"]
fn print_vectors() {
    let codec = skys3_ec::current_codec();
    println!(
        "# Golden vectors for erasure codec {}. Never edit; see tests/golden.rs.",
        codec.id()
    );
    for (geometry, data_len, seed) in cases() {
        println!("vector {geometry} {data_len} {seed}");
        for fragment in codec.encode(geometry, &sample(data_len, seed)).unwrap() {
            println!("{}", hex(&fragment));
        }
    }
}
