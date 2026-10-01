//! Parsing of `AssumeRoleWithWebIdentity` query requests.
//!
//! The STS query API sends an action's parameters form-encoded, in the
//! body of a `POST` (as the SDKs do) and optionally in the query string.
//! Both are decoded as forms and taken together. Parsing is strict: a
//! parameter given twice, an unknown parameter, and a parameter SkyS3 does
//! not support (`PolicyArns.member.N.arn`, `ProviderId`) are refused rather
//! than ignored, since ignoring a narrowing parameter would widen the
//! session.

use std::collections::HashSet;

use zeroize::Zeroizing;

use super::StsError;

/// The action this endpoint serves.
pub const ACTION: &str = "AssumeRoleWithWebIdentity";

/// The STS API version every request must name.
pub const API_VERSION: &str = "2011-06-15";

/// The largest form accepted, in bytes: room for a 20,000-byte token and a
/// 2,048-byte policy, percent-encoded.
pub const MAX_FORM_BYTES: usize = 64 * 1024;

/// The longest session policy accepted, in bytes (the AWS STS limit).
pub const MAX_SESSION_POLICY_BYTES: usize = 2_048;

/// The shortest and longest `WebIdentityToken` (the AWS STS limits).
const TOKEN_LENGTHS: std::ops::RangeInclusive<usize> = 4..=crate::jwt::MAX_TOKEN_BYTES;

/// The shortest and longest `RoleSessionName` (the AWS STS limits).
const SESSION_NAME_LENGTHS: std::ops::RangeInclusive<usize> = 2..=64;

/// The shortest and longest `RoleArn` (the AWS STS limits).
const ROLE_ARN_LENGTHS: std::ops::RangeInclusive<usize> = 20..=2_048;

/// The prefix of a role ARN, before the account.
const ROLE_ARN_PREFIX: &str = "arn:aws:iam::";

/// An `AssumeRoleWithWebIdentity` request.
#[derive(Clone, PartialEq, Eq)]
pub struct AssumeRoleRequest {
    /// `RoleArn`, as given.
    pub role_arn: String,
    /// The account digits of `RoleArn`, which the assumed-role ARN repeats.
    pub account: String,
    /// The role's name: the last segment of `RoleArn`.
    pub role: String,
    /// `RoleSessionName`.
    pub session_name: String,
    /// `WebIdentityToken`.
    pub token: Zeroizing<String>,
    /// `DurationSeconds`, if given.
    pub duration_seconds: Option<u64>,
    /// `Policy`, the session policy, if given.
    pub policy: Option<String>,
}

impl std::fmt::Debug for AssumeRoleRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssumeRoleRequest")
            .field("role_arn", &self.role_arn)
            .field("session_name", &self.session_name)
            .field("duration_seconds", &self.duration_seconds)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// Parses the form-encoded `query` and `body` of a request.
///
/// # Errors
///
/// The STS error to answer with: `InvalidAction` for another action or
/// version, `InvalidParameterValue` for an unsupported, unknown, or
/// repeated parameter, and `ValidationError` for a missing or invalid one.
pub fn parse(query: &[u8], body: &[u8]) -> Result<AssumeRoleRequest, StsError> {
    let mut params = Params::default();
    for form in [query, body] {
        let pairs: Vec<(String, String)> = serde_urlencoded::from_bytes(form)
            .map_err(|_| StsError::validation("The request parameters are not a valid form."))?;
        for (name, value) in pairs {
            params.add(name, value)?;
        }
    }
    match params.action.as_deref() {
        Some(ACTION) => {}
        Some(other) => {
            return Err(StsError::invalid_action(format!(
                "Could not find operation {other} for version {}",
                params.version.as_deref().unwrap_or("NO_VERSION_SPECIFIED")
            )));
        }
        None => return Err(StsError::invalid_action("The request names no Action.")),
    }
    if params.version.as_deref() != Some(API_VERSION) {
        return Err(StsError::invalid_action(format!(
            "Could not find operation {ACTION} for version {}",
            params.version.as_deref().unwrap_or("NO_VERSION_SPECIFIED")
        )));
    }
    let role_arn = required(params.role_arn, "roleArn")?;
    let (account, role) = parse_role_arn(&role_arn)?;
    let session_name = required(params.session_name, "roleSessionName")?;
    let name_ok = SESSION_NAME_LENGTHS.contains(&session_name.len())
        && session_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_+=,.@-".contains(&b));
    if !name_ok {
        return Err(StsError::validation(
            "RoleSessionName must be 2 to 64 characters: letters, digits, and _+=,.@-",
        ));
    }
    let token = Zeroizing::new(required(params.token, "webIdentityToken")?);
    if !TOKEN_LENGTHS.contains(&token.len()) {
        return Err(StsError::validation(format!(
            "WebIdentityToken must be {} to {} characters",
            TOKEN_LENGTHS.start(),
            TOKEN_LENGTHS.end()
        )));
    }
    let duration_seconds = params
        .duration
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| StsError::validation("DurationSeconds must be a whole number"))
        })
        .transpose()?;
    if let Some(policy) = &params.policy
        && !(1..=MAX_SESSION_POLICY_BYTES).contains(&policy.len())
    {
        return Err(StsError::validation(format!(
            "Policy must be 1 to {MAX_SESSION_POLICY_BYTES} characters"
        )));
    }
    Ok(AssumeRoleRequest {
        role_arn: role_arn.clone(),
        account: account.to_owned(),
        role: role.to_owned(),
        session_name,
        token,
        duration_seconds,
        policy: params.policy,
    })
}

/// The parameters of a request, each at most once.
#[derive(Default)]
struct Params {
    seen: HashSet<String>,
    action: Option<String>,
    version: Option<String>,
    role_arn: Option<String>,
    session_name: Option<String>,
    token: Option<String>,
    duration: Option<String>,
    policy: Option<String>,
}

impl Params {
    fn add(&mut self, name: String, value: String) -> Result<(), StsError> {
        let slot = match name.as_str() {
            "Action" => &mut self.action,
            "Version" => &mut self.version,
            "RoleArn" => &mut self.role_arn,
            "RoleSessionName" => &mut self.session_name,
            "WebIdentityToken" => &mut self.token,
            "DurationSeconds" => &mut self.duration,
            "Policy" => &mut self.policy,
            "ProviderId" => {
                return Err(StsError::invalid_parameter(
                    "ProviderId is not supported: SkyS3 accepts OIDC ID tokens only",
                ));
            }
            name if name.starts_with("PolicyArns.") => {
                return Err(StsError::invalid_parameter(
                    "PolicyArns is not supported: SkyS3 has no managed policies; pass Policy",
                ));
            }
            _ => {
                return Err(StsError::invalid_parameter(format!(
                    "The parameter {name} is not recognized"
                )));
            }
        };
        if !self.seen.insert(name) {
            return Err(StsError::invalid_parameter(
                "A parameter appears more than once",
            ));
        }
        *slot = Some(value);
        Ok(())
    }
}

fn required(value: Option<String>, member: &str) -> Result<String, StsError> {
    value.ok_or_else(|| {
        StsError::validation(format!(
            "1 validation error detected: Value null at '{member}' failed to satisfy \
             constraint: Member must not be null"
        ))
    })
}

/// Splits `arn:aws:iam::<account>:role/<path/><name>` into its account and
/// role name. The account is digits, possibly none: SkyS3 has no accounts
/// and accepts whatever the client was configured with.
fn parse_role_arn(arn: &str) -> Result<(&str, &str), StsError> {
    let invalid = || {
        StsError::validation(format!(
            "RoleArn must be {} to {} characters of the form \
             arn:aws:iam::<account>:role/<name>, with a role name of letters, digits, and _-.",
            ROLE_ARN_LENGTHS.start(),
            ROLE_ARN_LENGTHS.end()
        ))
    };
    if !ROLE_ARN_LENGTHS.contains(&arn.len()) {
        return Err(invalid());
    }
    let rest = arn.strip_prefix(ROLE_ARN_PREFIX).ok_or_else(invalid)?;
    let (account, path) = rest.split_once(":role/").ok_or_else(invalid)?;
    if !account.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let (prefix, name) = path.rsplit_once('/').unwrap_or(("", path));
    let prefix_ok = prefix.bytes().all(|b| b.is_ascii_graphic());
    if !prefix_ok || !skys3_types::RoleDocument::valid_name(name) {
        return Err(invalid());
    }
    Ok((account, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.e30.c2ln";

    fn form(extra: &[(&str, &str)]) -> String {
        let mut pairs = vec![
            ("Action", ACTION),
            ("Version", API_VERSION),
            ("RoleArn", "arn:aws:iam::123456789012:role/deployer"),
            ("RoleSessionName", "ci-run.1@example"),
            ("WebIdentityToken", TOKEN),
        ];
        pairs.retain(|(name, _)| !extra.iter().any(|(other, _)| other == name));
        pairs.extend(extra.iter().filter(|(_, value)| !value.is_empty()));
        serde_urlencoded::to_string(pairs).unwrap()
    }

    #[test]
    fn parses_what_the_sdks_send() {
        let request = parse(b"", form(&[]).as_bytes()).unwrap();
        assert_eq!(request.role, "deployer");
        assert_eq!(request.account, "123456789012");
        assert_eq!(request.session_name, "ci-run.1@example");
        assert_eq!(request.token.as_str(), TOKEN);
        assert_eq!(request.duration_seconds, None);
        assert_eq!(request.policy, None);
        assert!(!format!("{request:?}").contains(TOKEN));

        let policy = r#"{"Version":"2012-10-17"}"#;
        let body = form(&[("DurationSeconds", "900"), ("Policy", policy)]);
        let request = parse(b"", body.as_bytes()).unwrap();
        assert_eq!(request.duration_seconds, Some(900));
        assert_eq!(request.policy.as_deref(), Some(policy));

        // Parameters may come in the query string too.
        let request = parse(
            b"Action=AssumeRoleWithWebIdentity",
            form(&[("Action", "")]).as_bytes(),
        );
        assert_eq!(request.unwrap().role, "deployer");

        // A role path is allowed; the name is its last segment.
        let arn = "arn:aws:iam::1:role/team/ci/deployer";
        let request = parse(b"", form(&[("RoleArn", arn)]).as_bytes()).unwrap();
        assert_eq!(
            (request.account.as_str(), request.role.as_str()),
            ("1", "deployer")
        );
    }

    #[test]
    fn refuses_bad_requests() {
        let cases: &[(&[(&str, &str)], &str)] = &[
            (&[("Action", "AssumeRole")], "InvalidAction"),
            (&[("Action", "")], "InvalidAction"),
            (&[("Version", "2010-01-01")], "InvalidAction"),
            (&[("Version", "")], "InvalidAction"),
            (&[("RoleArn", "")], "ValidationError"),
            (
                &[("RoleArn", "arn:aws:iam::1:user/deployer")],
                "ValidationError",
            ),
            (
                &[("RoleArn", "arn:aws:iam::x1:role/deployer")],
                "ValidationError",
            ),
            (
                &[("RoleArn", "arn:aws:sts::1:role/deployer1234")],
                "ValidationError",
            ),
            (
                &[("RoleArn", "arn:aws:iam::1:role/bad+name")],
                "ValidationError",
            ),
            (
                &[("RoleArn", "arn:aws:iam::1:role/a b/name")],
                "ValidationError",
            ),
            (&[("RoleSessionName", "")], "ValidationError"),
            (&[("RoleSessionName", "x")], "ValidationError"),
            (&[("RoleSessionName", "a b")], "ValidationError"),
            (&[("WebIdentityToken", "")], "ValidationError"),
            (&[("WebIdentityToken", "abc")], "ValidationError"),
            (&[("DurationSeconds", "1h")], "ValidationError"),
            (&[("DurationSeconds", "-1")], "ValidationError"),
            (
                &[("PolicyArns.member.1.arn", "arn:aws:iam::aws:policy/x")],
                "InvalidParameterValue",
            ),
            (&[("ProviderId", "www.amazon.com")], "InvalidParameterValue"),
            (&[("Tags.member.1.Key", "k")], "InvalidParameterValue"),
        ];
        for (extra, code) in cases {
            let error = parse(b"", form(extra).as_bytes()).unwrap_err();
            assert_eq!(error.code, *code, "{extra:?}: {error:?}");
        }
        let long_token = "a".repeat(TOKEN_LENGTHS.end() + 1);
        let long_policy = "p".repeat(MAX_SESSION_POLICY_BYTES + 1);
        let long_arn = format!("arn:aws:iam::1:role/{}/r", "p".repeat(2_048));
        for extra in [
            [("WebIdentityToken", long_token.as_str())],
            [("Policy", long_policy.as_str())],
            [("RoleArn", long_arn.as_str())],
        ] {
            let error = parse(b"", form(&extra).as_bytes()).unwrap_err();
            assert_eq!(error.code, "ValidationError");
        }
        // Malformed escapes decode leniently, as everywhere else, into a
        // name that is not a parameter.
        let error = parse(b"", b"%zz=\xff").unwrap_err();
        assert_eq!(error.code, "InvalidParameterValue");
        let twice = format!("{}&RoleSessionName=again", form(&[]));
        assert_eq!(
            parse(b"", twice.as_bytes()).unwrap_err().code,
            "InvalidParameterValue"
        );
        let split = parse(b"Version=2011-06-15", form(&[]).as_bytes()).unwrap_err();
        assert_eq!(split.code, "InvalidParameterValue");
    }
}
