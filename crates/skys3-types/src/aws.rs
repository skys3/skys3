//! AWS S3 endpoints: which host names SkyS3 treats as AWS, and the
//! partition and region an S3 endpoint names.
//!
//! AWS bucket names are unique within a partition, not across partitions:
//! the commercial partition (`aws`), China (`aws-cn`), and GovCloud
//! (`aws-us-gov`) each have their own bucket namespace. Configuration
//! checks that compare buckets of two endpoints (§6.1) therefore key them
//! by partition, and the remote client's addressing (§15) recognizes AWS
//! endpoints by the same domains.
//!
//! ```
//! use skys3_types::aws::{AwsPartition, s3_endpoint};
//!
//! let endpoint = s3_endpoint("s3.cn-north-1.amazonaws.com.cn").unwrap();
//! assert_eq!(endpoint.partition, AwsPartition::China);
//! assert_eq!(endpoint.region, "cn-north-1");
//! let gov = s3_endpoint("s3-fips.us-gov-west-1.amazonaws.com").unwrap();
//! assert_eq!(gov.partition, AwsPartition::GovCloud);
//! assert_eq!(s3_endpoint("minio.example"), None);
//! ```

use std::fmt;

/// An AWS partition: a set of regions with its own bucket namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AwsPartition {
    /// The commercial partition, `aws`.
    Standard,
    /// China, `aws-cn`, under `amazonaws.com.cn`.
    China,
    /// GovCloud (US), `aws-us-gov`: `amazonaws.com` host names whose region
    /// starts with `us-gov-`.
    GovCloud,
}

impl AwsPartition {
    /// Every partition.
    pub const ALL: [AwsPartition; 3] = [
        AwsPartition::Standard,
        AwsPartition::China,
        AwsPartition::GovCloud,
    ];

    /// The partition's name, as ARNs spell it: `aws`, `aws-cn`, or
    /// `aws-us-gov`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            AwsPartition::Standard => "aws",
            AwsPartition::China => "aws-cn",
            AwsPartition::GovCloud => "aws-us-gov",
        }
    }

    /// The partition named `name`, if any.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|partition| partition.name() == name)
    }
}

impl fmt::Display for AwsPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The DNS suffixes of AWS host names, each with the partition its host
/// names belong to before GovCloud regions are told apart. Longest first,
/// so `amazonaws.com.cn` is not read as a host under `com`.
const DOMAINS: [(&str, AwsPartition); 2] = [
    (".amazonaws.com.cn", AwsPartition::China),
    (".amazonaws.com", AwsPartition::Standard),
];

/// The prefix of GovCloud region names.
const GOV_REGION_PREFIX: &str = "us-gov-";

/// Whether `host`, a lowercase DNS name, is an AWS host name.
#[must_use]
pub fn is_aws_host(host: &str) -> bool {
    DOMAINS.iter().any(|(suffix, _)| host.ends_with(suffix))
}

/// What an AWS S3 endpoint's host name says about where it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct S3Endpoint<'a> {
    /// The partition, whose bucket namespace the endpoint reaches.
    pub partition: AwsPartition,
    /// The region the host name names, or the region of the global
    /// endpoint (`s3.amazonaws.com`, `us-east-1`).
    pub region: &'a str,
}

/// The partition and region of `host`, a lowercase DNS name, if it is an
/// AWS host name: `s3.<region>`, `<bucket>.s3.<region>`,
/// `s3.dualstack.<region>`, `s3-fips.<region>`, the legacy
/// `s3-<region>`, or the global `s3` of the commercial partition.
#[must_use]
pub fn s3_endpoint(host: &str) -> Option<S3Endpoint<'_>> {
    let (service, partition) = DOMAINS
        .iter()
        .find_map(|(suffix, partition)| Some((host.strip_suffix(suffix)?, *partition)))?;
    let last = service.rsplit('.').next().unwrap_or(service);
    let region = match (last, partition) {
        ("s3", AwsPartition::Standard) => "us-east-1",
        _ => last.strip_prefix("s3-").unwrap_or(last),
    };
    let partition = if partition == AwsPartition::Standard && region.starts_with(GOV_REGION_PREFIX)
    {
        AwsPartition::GovCloud
    } else {
        partition
    };
    Some(S3Endpoint { partition, region })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(host: &str) -> (AwsPartition, &str) {
        let endpoint = s3_endpoint(host).unwrap();
        (endpoint.partition, endpoint.region)
    }

    #[test]
    fn endpoints_name_their_partition_and_region() {
        use AwsPartition::{China, GovCloud, Standard};
        for (host, expected) in [
            ("s3.us-west-2.amazonaws.com", (Standard, "us-west-2")),
            ("b.s3.us-west-2.amazonaws.com", (Standard, "us-west-2")),
            (
                "s3.dualstack.eu-west-1.amazonaws.com",
                (Standard, "eu-west-1"),
            ),
            ("s3-eu-west-1.amazonaws.com", (Standard, "eu-west-1")),
            ("s3.amazonaws.com", (Standard, "us-east-1")),
            ("s3.cn-north-1.amazonaws.com.cn", (China, "cn-north-1")),
            (
                "b.s3.cn-northwest-1.amazonaws.com.cn",
                (China, "cn-northwest-1"),
            ),
            (
                "s3.us-gov-west-1.amazonaws.com",
                (GovCloud, "us-gov-west-1"),
            ),
            (
                "s3-fips.us-gov-east-1.amazonaws.com",
                (GovCloud, "us-gov-east-1"),
            ),
            (
                "s3-us-gov-west-1.amazonaws.com",
                (GovCloud, "us-gov-west-1"),
            ),
        ] {
            assert_eq!(endpoint(host), expected, "{host}");
            assert!(is_aws_host(host), "{host}");
        }
        for host in [
            "minio.example",
            "amazonaws.com",
            "s3.amazonaws.com.evil",
            "x.com.cn",
        ] {
            assert_eq!(s3_endpoint(host), None, "{host}");
            assert!(!is_aws_host(host), "{host}");
        }
    }

    #[test]
    fn partitions_round_trip_their_names() {
        for partition in AwsPartition::ALL {
            assert_eq!(AwsPartition::from_name(partition.name()), Some(partition));
            assert_eq!(partition.to_string(), partition.name());
        }
        assert_eq!(AwsPartition::from_name("dns"), None);
    }
}
