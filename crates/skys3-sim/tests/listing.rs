//! Property tests for the simulated store's listings: paging through
//! `ListObjectsV2` and `ListParts` with any page size returns exactly what a
//! direct reading of S3's rules selects, each entry once and in order.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use bytes::Bytes;
use proptest::prelude::*;
use skys3_remote::{
    CreateMultipartUpload, DeleteObject, ListObjectsV2, ListParts, ObjectStore, PutObject,
    UploadPart,
};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;

/// Runs a future that never sleeps: the store has no faults.
fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

/// The entries S3 lists: every current key under `prefix`, rolled up to
/// its common prefix at the first `delimiter` after the prefix, each
/// distinct entry once, in order, and only those after `start_after`. The
/// flag marks common prefixes.
fn reference(
    keys: &BTreeSet<String>,
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
) -> Vec<(String, bool)> {
    let mut entries = BTreeMap::new();
    for key in keys.iter().filter(|key| key.starts_with(prefix)) {
        let rest = &key[prefix.len()..];
        let entry = match delimiter.filter(|d| !d.is_empty()) {
            Some(d) => match rest.find(d) {
                Some(at) => (key[..prefix.len() + at + d.len()].to_owned(), true),
                None => (key.clone(), false),
            },
            None => (key.clone(), false),
        };
        if start_after.is_none_or(|after| entry.0.as_str() > after) {
            entries.insert(entry.0, entry.1);
        }
    }
    entries.into_iter().collect()
}

fn key() -> impl Strategy<Value = String> {
    "[ab/]{1,4}"
}

/// Any position, often one inside a common prefix: the case where S3 leaves
/// out a prefix whose keys come after the position.
fn start_after() -> impl Strategy<Value = Option<String>> {
    prop::option::of(prop_oneof![
        "[ab/]{0,4}",
        "[ab]?/[ab/]{1,2}",
        "[ab]?b[ab/]{1,2}"
    ])
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1024,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn list_objects_v2_pages_match_the_reference(
        keys in prop::collection::btree_map(key(), any::<bool>(), 0..16),
        versioning in any::<bool>(),
        prefix in "[ab/]{0,2}",
        delimiter in prop::option::of(prop_oneof!["/", "b", "ab", ""]),
        start_after in start_after(),
        max_keys in 0_u32..5,
    ) {
        let store = SimS3::new(0, SimS3Config { versioning, ..SimS3Config::default() });
        let current: BTreeSet<String> = block_on(async {
            for (key, deleted) in &keys {
                store.put_object(PutObject::new(key.clone(), "v")).await.unwrap();
                if *deleted {
                    store.delete_object(DeleteObject::new(key.clone())).await.unwrap();
                }
            }
            keys.iter().filter(|(_, deleted)| !**deleted).map(|(k, _)| k.clone()).collect()
        });
        prop_assert_eq!(store.keys(), current.iter().cloned().collect::<Vec<_>>());
        let expected = reference(&current, &prefix, delimiter.as_deref(), start_after.as_deref());

        let mut request = ListObjectsV2::new(prefix.clone()).with_max_keys(max_keys);
        request.delimiter = delimiter.clone();
        request.start_after = start_after.clone();
        let mut listed = Vec::new();
        let mut pages = 0;
        loop {
            let page = block_on(store.list_objects_v2(request.clone())).unwrap();
            pages += 1;
            prop_assert!(pages <= expected.len() + 1, "the listing does not end");
            let mut entries: Vec<(String, bool)> = page
                .objects
                .iter()
                .map(|o| (o.key.clone(), false))
                .chain(page.common_prefixes.iter().map(|p| (p.clone(), true)))
                .collect();
            entries.sort();
            prop_assert!(entries.len() <= max_keys as usize);
            prop_assert_eq!(page.is_truncated, page.next_continuation_token.is_some());
            listed.extend(entries);
            match page.next_continuation_token {
                Some(token) => request.continuation_token = Some(token),
                None => break,
            }
        }
        if max_keys == 0 {
            // An empty page that is not truncated.
            prop_assert!(listed.is_empty());
            prop_assert_eq!(pages, 1);
        } else {
            prop_assert_eq!(&listed, &expected);
            prop_assert_eq!(pages, expected.len().div_ceil(max_keys as usize).max(1));
        }
    }

    #[test]
    fn list_parts_pages_match_the_reference(
        parts in prop::collection::btree_set(1_u32..40, 0..12),
        marker in prop::option::of(0_u32..45),
        max_parts in 0_u32..5,
    ) {
        let store = SimS3::new(0, SimS3Config::default());
        let upload_id = block_on(async {
            let upload_id = store
                .create_multipart_upload(CreateMultipartUpload::new("k"))
                .await
                .unwrap();
            for &part_number in &parts {
                let body = Bytes::from(vec![0; part_number as usize]);
                let request = UploadPart {
                    key: "k".into(),
                    upload_id: upload_id.clone(),
                    part_number,
                    body,
                };
                store.upload_part(request).await.unwrap();
            }
            upload_id
        });
        let expected: Vec<u32> =
            parts.iter().copied().filter(|&n| n > marker.unwrap_or(0)).collect();

        let mut request = ListParts {
            part_number_marker: marker,
            max_parts,
            ..ListParts::new("k", upload_id)
        };
        let mut listed = Vec::new();
        let mut pages = 0;
        loop {
            let page = block_on(store.list_parts(request.clone())).unwrap();
            pages += 1;
            prop_assert!(pages <= expected.len() + 1, "the listing does not end");
            prop_assert!(page.parts.len() <= max_parts as usize);
            prop_assert_eq!(page.is_truncated, page.next_part_number_marker.is_some());
            for part in &page.parts {
                prop_assert_eq!(part.size, u64::from(part.part_number));
            }
            listed.extend(page.parts.iter().map(|p| p.part_number));
            match page.next_part_number_marker {
                Some(next) => request.part_number_marker = Some(next),
                None => break,
            }
        }
        if max_parts == 0 {
            prop_assert!(listed.is_empty());
            prop_assert_eq!(pages, 1);
        } else {
            prop_assert_eq!(&listed, &expected);
            prop_assert_eq!(pages, expected.len().div_ceil(max_parts as usize).max(1));
        }
    }
}
