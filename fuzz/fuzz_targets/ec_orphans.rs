//! Fuzzes what a shard primary decodes from a fragment node's orphan
//! query, and what the node decodes from the primary's verdicts (design
//! §8.4): the `OrphanQuery` and `OrphanVerdicts` bodies and their checks.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use skys3_ec::orphans::{OrphanQuery, OrphanVerdicts};

fuzz_target!(|data: &[u8]| {
    // Decoding and checking must never panic, and a query that checks out
    // is built again from what it names.
    if let Ok(query) = OrphanQuery::decode(data)
        && let Ok((shard, suspects)) = query.parse()
    {
        let again = OrphanQuery::new(&shard, &suspects).parse();
        assert_eq!(again, Ok((shard, suspects)));
    }
    if let Ok(answer) = OrphanVerdicts::decode(data)
        && let Ok(verdicts) = answer.parse(answer.verdicts.len())
    {
        assert_eq!(verdicts.len(), answer.verdicts.len());
        assert!(answer.error.is_empty());
    }
});
