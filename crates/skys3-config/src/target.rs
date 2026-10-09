//! Endpoint and target URLs.
//!
//! A target is written as one path-style URL, `https://host[:port]/bucket`
//! or `https://host[:port]/bucket/prefix`, as `backup_target` and
//! `snapshot_target` are in design §14. Endpoints (`etcd_endpoints`, the S3
//! control store's `endpoint`) are URLs without a path.
//!
//! The authority is a host and an optional port, parsed with the rules of
//! [`NodeAddress`]: a DNS name, an IPv4 address, or a bracketed IPv6
//! address, and a canonical decimal port from 1 to 65535. Host names are
//! case-insensitive, so the authority is lowercased first, and parsed
//! endpoints are written in canonical form (lowercase names, RFC 5952 IPv6
//! literals). User information (`user:password@`) is rejected: credentials
//! never appear in the configuration.

use std::net::IpAddr;

use skys3_types::aws::{self, AwsPartition};
use skys3_types::{AddressError, Host, NodeAddress, RemoteTarget};

/// A URL's scheme, host, and the port it names explicitly, if any.
struct Origin {
    scheme: &'static str,
    host: Host,
    port: Option<u16>,
}

impl Origin {
    /// The canonical `scheme://host[:port]` form.
    fn to_endpoint(&self) -> String {
        match self.port {
            Some(port) => format!("{}://{}:{port}", self.scheme, self.host),
            None => format!("{}://{}", self.scheme, self.host),
        }
    }
}

/// Parses `url`'s origin and returns it with the rest of the URL, which is
/// empty or starts with `/`.
fn split_origin(url: &str) -> Result<(Origin, &str), String> {
    let (scheme, default_port, after_scheme) = if let Some(rest) = url.strip_prefix("https://") {
        ("https", 443, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        ("http", 80, rest)
    } else {
        return Err(format!("{url:?} must start with https:// or http://"));
    };
    if let Some(bad) = url.chars().find(|c| !c.is_ascii_graphic()) {
        return Err(format!(
            "{url:?} contains {bad:?}; only visible ASCII is allowed"
        ));
    }
    if url.contains(['?', '#']) {
        return Err(format!("{url:?} must not have a query or fragment"));
    }
    let (authority, rest) =
        after_scheme.split_at(after_scheme.find('/').unwrap_or(after_scheme.len()));
    if authority.is_empty() {
        return Err(format!("{url:?} has no host"));
    }
    if authority.contains('@') {
        return Err(format!(
            "{url:?} must not embed credentials; SkyS3 takes them from credential providers"
        ));
    }
    let (host, port) = parse_authority(authority, default_port)
        .map_err(|reason| format!("{url:?} has {reason}"))?;
    Ok((Origin { scheme, host, port }, rest))
}

/// Parses `host` or `host:port` with [`NodeAddress`]'s rules, supplying
/// the scheme's default port when none is written.
fn parse_authority(authority: &str, default_port: u16) -> Result<(Host, Option<u16>), String> {
    let authority = authority.to_ascii_lowercase();
    let has_port = if authority.starts_with('[') {
        !authority.ends_with(']')
    } else {
        authority.contains(':')
    };
    let text = if has_port {
        authority
    } else {
        format!("{authority}:{default_port}")
    };
    let address: NodeAddress = text.parse().map_err(|error| match error {
        AddressError::InvalidPort(port) => {
            format!("an invalid port {port:?}; it must be from 1 to 65535, without leading zeros")
        }
        AddressError::InvalidHost { host, reason } => {
            format!("an invalid host {host:?}: {reason}")
        }
        AddressError::MissingPort(_) => {
            "a malformed authority; expected host, host:port, [ipv6], or [ipv6]:port".to_owned()
        }
        other => format!("an invalid authority: {other}"),
    })?;
    Ok((address.host().clone(), has_port.then(|| address.port())))
}

/// Checks an endpoint URL, a scheme and an authority with no path, and
/// returns its canonical form.
pub(crate) fn parse_endpoint(url: &str) -> Result<String, String> {
    let (origin, path) = split_origin(url)?;
    if !path.is_empty() && path != "/" {
        return Err(format!("{url:?} must not have a path"));
    }
    Ok(origin.to_endpoint())
}

/// Parses a path-style target URL, `https://host[:port]/bucket` or
/// `https://host[:port]/bucket/prefix`, into a canonical endpoint, a
/// bucket, and an optional key prefix, with the rules of `backup_target`.
///
/// Bucket creation reads a `write_back` bucket's target in the same form
/// (design §11).
///
/// # Errors
///
/// A message that quotes the URL and names the rule it breaks.
///
/// ```
/// let target = skys3_config::parse_target("https://S3.Example.com/data/team-a/")?;
/// assert_eq!(target.endpoint, "https://s3.example.com");
/// assert_eq!(target.bucket, "data");
/// assert_eq!(target.prefix.as_deref(), Some("team-a/"));
/// # Ok::<(), String>(())
/// ```
pub fn parse_target(url: &str) -> Result<RemoteTarget, String> {
    let (origin, path) = split_origin(url)?;
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(format!(
            "{url:?} names no bucket; write it as https://host/bucket or https://host/bucket/prefix"
        ));
    }
    Ok(RemoteTarget {
        endpoint: origin.to_endpoint(),
        bucket: bucket.to_owned(),
        prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
    })
}

/// The failure scope an endpoint can be identified with: the AWS partition
/// and region for an AWS S3 endpoint, the IP address for an IP literal, otherwise the host
/// name. Ports are ignored.
///
/// IP addresses are compared as addresses, so every spelling of one IPv6
/// address, and an IPv4 address and its IPv4-mapped IPv6 form, share a
/// scope. Two endpoints in the same scope share an outage. The check is
/// conservative: endpoints in different scopes may still be correlated in
/// ways the configuration cannot show (§6.1).
///
/// Returns `None` for a URL that [`parse_endpoint`] rejects.
pub(crate) fn failure_scope(endpoint: &str) -> Option<String> {
    let (origin, _) = split_origin(endpoint).ok()?;
    let ip = match origin.host {
        Host::Ipv4(ip) => IpAddr::V4(ip),
        Host::Ipv6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        Host::Dns(name) => return Some(dns_scope(name.as_str())),
    };
    Some(format!("ip:{ip}"))
}

/// The namespace of the bucket names behind endpoints of failure `scope`.
/// AWS bucket names are unique per partition, so every AWS S3 endpoint of
/// one partition shares one, named after the partition; any other endpoint
/// has its own, the scope itself.
pub(crate) fn bucket_namespace(scope: &str) -> &str {
    match scope.split_once(':') {
        Some((partition, _)) if AwsPartition::from_name(partition).is_some() => partition,
        _ => scope,
    }
}

/// The scope of a lowercase DNS name: `<partition>:<region>` for an AWS S3
/// endpoint, such as `aws:us-west-2` or `aws-cn:cn-north-1`.
fn dns_scope(host: &str) -> String {
    match aws::s3_endpoint(host) {
        Some(endpoint) => format!("{}:{}", endpoint.partition, endpoint.region),
        None => format!("dns:{host}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_split_into_endpoint_bucket_and_prefix() {
        let target = parse_target("https://s3.us-west-2.amazonaws.com/snaps/archive/").unwrap();
        assert_eq!(target.endpoint, "https://s3.us-west-2.amazonaws.com");
        assert_eq!(target.bucket, "snaps");
        assert_eq!(target.prefix.as_deref(), Some("archive/"));

        let target = parse_target("http://10.0.0.5:9000/archive").unwrap();
        assert_eq!(target.endpoint, "http://10.0.0.5:9000");
        assert_eq!(target.bucket, "archive");
        assert_eq!(target.prefix, None);

        assert_eq!(parse_target("https://h/b/").unwrap().prefix, None);
    }

    #[test]
    fn endpoints_are_canonical() {
        for (url, canonical) in [
            ("https://etcd-1:2379/", "https://etcd-1:2379"),
            ("https://S3.Example.COM", "https://s3.example.com"),
            ("http://[FD00:0:0:0:0:0:0:1]:9000", "http://[fd00::1]:9000"),
            ("http://[::1]", "http://[::1]"),
            ("https://10.0.0.1:443", "https://10.0.0.1:443"),
        ] {
            assert_eq!(parse_endpoint(url).unwrap(), canonical, "{url}");
        }
    }

    #[test]
    fn malformed_targets_are_rejected() {
        for url in [
            "s3://bucket",
            "https://",
            "https:///bucket",
            "https://host",
            "https://host/",
            "https://host//prefix",
            "https://user:secret@host/bucket",
            "https://host/bucket?x=1",
            "https://host/bucket#frag",
            "https://host/my bucket",
            "https://host/bücket",
        ] {
            assert!(parse_target(url).is_err(), "{url} was accepted");
        }
    }

    #[test]
    fn malformed_authorities_are_rejected() {
        for (url, reason) in [
            ("https://:9000/bucket", "invalid host"),
            ("https://host:bad/bucket", "invalid port"),
            ("https://host:/bucket", "invalid port"),
            ("https://host:0/bucket", "invalid port"),
            ("https://host:65536/bucket", "invalid port"),
            ("https://host:0443/bucket", "invalid port"),
            ("https://[fd00::1/bucket", "invalid host"),
            ("https://[fd00::1]x/bucket", "malformed authority"),
            ("https://[not-ip]/bucket", "invalid host"),
            ("https://fd00::1/bucket", "must be in brackets"),
            ("https://host_name/bucket", "invalid host"),
            ("https://-host/bucket", "invalid host"),
            ("https://host..example/bucket", "invalid host"),
            ("https://10.0.0.256/bucket", "invalid host"),
        ] {
            let error = parse_target(url).unwrap_err();
            assert!(error.contains(reason), "{url}: {error}");
        }
        assert!(parse_endpoint("https://host:bad").is_err());
    }

    #[test]
    fn endpoints_have_no_path() {
        assert!(parse_endpoint("https://etcd-1:2379/v3").is_err());
        assert!(parse_endpoint("etcd-1:2379").is_err());
    }

    #[test]
    fn failure_scopes_group_aws_regions_and_hosts() {
        let scope = |url| failure_scope(url).unwrap();
        assert_eq!(scope("https://s3.us-west-2.amazonaws.com"), "aws:us-west-2");
        assert_eq!(
            scope("https://b.s3.us-west-2.amazonaws.com"),
            "aws:us-west-2"
        );
        assert_eq!(
            scope("https://s3.dualstack.eu-west-1.amazonaws.com"),
            "aws:eu-west-1"
        );
        assert_eq!(scope("https://s3-eu-west-1.amazonaws.com"), "aws:eu-west-1");
        assert_eq!(scope("https://s3.amazonaws.com"), "aws:us-east-1");
        assert_eq!(
            scope("https://s3.cn-north-1.amazonaws.com.cn"),
            "aws-cn:cn-north-1"
        );
        assert_eq!(
            scope("https://s3.us-gov-west-1.amazonaws.com"),
            "aws-us-gov:us-gov-west-1"
        );
        assert_eq!(scope("https://MinIO.example:9000"), "dns:minio.example");
        assert_eq!(failure_scope("ftp://x"), None);
    }

    #[test]
    fn aws_endpoints_share_a_bucket_namespace_per_partition() {
        let namespace = |url| {
            let scope = failure_scope(url).unwrap();
            bucket_namespace(&scope).to_owned()
        };
        assert_eq!(namespace("https://s3.us-west-2.amazonaws.com"), "aws");
        assert_eq!(namespace("https://s3.amazonaws.com"), "aws");
        assert_eq!(
            namespace("https://s3.cn-northwest-1.amazonaws.com.cn"),
            "aws-cn"
        );
        assert_eq!(
            namespace("https://s3.us-gov-east-1.amazonaws.com"),
            "aws-us-gov"
        );
        assert_eq!(namespace("https://minio.example"), "dns:minio.example");
        assert_eq!(namespace("http://10.0.0.5"), "ip:10.0.0.5");
    }

    #[test]
    fn failure_scopes_compare_ip_addresses() {
        let scope = |url| failure_scope(url).unwrap();
        assert_eq!(scope("http://[fd00::1]:9000"), "ip:fd00::1");
        assert_eq!(
            scope("http://[fd00::1]:9000"),
            scope("https://[FD00:0:0:0:0:0:0:1]")
        );
        assert_eq!(
            scope("http://10.0.0.5"),
            scope("http://[::ffff:10.0.0.5]:9000")
        );
        assert_eq!(scope("http://10.0.0.5"), "ip:10.0.0.5");
        assert_ne!(scope("http://10.0.0.5"), scope("http://10.0.0.6"));
    }
}
