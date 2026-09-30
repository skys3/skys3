//! Endpoint and target URLs.
//!
//! A target is written as one path-style URL, `https://host[:port]/bucket`
//! or `https://host[:port]/bucket/prefix`, as `backup_target` and
//! `snapshot_target` are in design §14. Endpoints (`etcd_endpoints`, the S3
//! control store's `endpoint`) are URLs without a path.

use skys3_types::RemoteTarget;

/// Splits `url` into its scheme and authority (`https://host:port`) and the
/// rest, which is empty or starts with `/`.
fn split_origin(url: &str) -> Result<(&str, &str), String> {
    let Some(after_scheme) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
    else {
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
    let authority_len = after_scheme.find('/').unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_len];
    if authority.is_empty() {
        return Err(format!("{url:?} has no host"));
    }
    if authority.contains('@') {
        return Err(format!(
            "{url:?} must not embed credentials; SkyS3 takes them from credential providers"
        ));
    }
    let origin_len = url.len() - after_scheme.len() + authority_len;
    Ok(url.split_at(origin_len))
}

/// Checks an endpoint URL: a scheme and host, with no path.
pub(crate) fn parse_endpoint(url: &str) -> Result<String, String> {
    let (origin, path) = split_origin(url)?;
    if !path.is_empty() && path != "/" {
        return Err(format!("{url:?} must not have a path"));
    }
    Ok(origin.to_owned())
}

/// Parses a path-style target URL into an endpoint, a bucket, and an
/// optional key prefix.
pub(crate) fn parse_target(url: &str) -> Result<RemoteTarget, String> {
    let (origin, path) = split_origin(url)?;
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(format!(
            "{url:?} names no bucket; write it as https://host/bucket or https://host/bucket/prefix"
        ));
    }
    Ok(RemoteTarget {
        endpoint: origin.to_owned(),
        bucket: bucket.to_owned(),
        prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
    })
}

/// The host of an endpoint URL that [`parse_endpoint`] or [`parse_target`]
/// accepted, lowercased and without its port.
fn host(endpoint: &str) -> String {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, authority)| authority);
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or(bracketed)
    } else {
        authority.split(':').next().unwrap_or(authority)
    };
    host.to_ascii_lowercase()
}

/// The failure scope an endpoint can be identified with: the AWS region for
/// an AWS S3 endpoint, otherwise the host.
///
/// Two endpoints in the same scope share an outage. The check is
/// conservative: endpoints in different scopes may still be correlated in
/// ways the configuration cannot show (§6.1).
pub(crate) fn failure_scope(endpoint: &str) -> String {
    let host = host(endpoint);
    if let Some(service) = host.strip_suffix(".amazonaws.com") {
        // s3.us-west-2, bucket.s3.us-west-2, s3.dualstack.us-west-2,
        // s3-us-west-2, or the global s3 endpoint (us-east-1).
        let last = service.rsplit('.').next().unwrap_or(service);
        let region = match last {
            "s3" => "us-east-1",
            _ => last.strip_prefix("s3-").unwrap_or(last),
        };
        return format!("aws:{region}");
    }
    host
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
    fn endpoints_have_no_path() {
        assert_eq!(
            parse_endpoint("https://etcd-1:2379/").unwrap(),
            "https://etcd-1:2379"
        );
        assert!(parse_endpoint("https://etcd-1:2379/v3").is_err());
        assert!(parse_endpoint("etcd-1:2379").is_err());
    }

    #[test]
    fn failure_scopes_group_aws_regions_and_hosts() {
        let scope = |url| failure_scope(&parse_endpoint(url).unwrap());
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
        assert_eq!(scope("https://MinIO.example:9000"), "minio.example");
        assert_eq!(scope("http://[fd00::1]:9000"), "fd00::1");
    }
}
