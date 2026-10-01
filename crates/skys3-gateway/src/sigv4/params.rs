//! Parsing of a request's SigV4 parameters: the `Authorization` header or
//! the presigned-URL query parameters, the credential scope, and the
//! request time.

use std::time::Duration;

use http::HeaderMap;
use http::request::Parts;
use s3s::{S3Error, S3ErrorCode, s3_error};

use crate::limits::query_pairs;

/// The only signing algorithm SkyS3 accepts.
pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The last element of every credential scope.
const TERMINATOR: &str = "aws4_request";

/// The longest access key ID. AWS key IDs are 16 to 128 characters.
const MAX_ACCESS_KEY_ID_BYTES: usize = 128;

/// The longest signing region. SkyS3 has no regions and accepts any.
const MAX_REGION_BYTES: usize = 64;

/// The query parameters of a presigned URL that carry its signature. They
/// are removed before `s3s` sees the request.
pub(crate) const PRESIGNED_PARAMS: [&str; 7] = [
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
];

/// Where a request carries its signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// The `Authorization` header.
    Header,
    /// The query parameters of a presigned URL.
    Presigned,
}

impl AuthMethod {
    /// The S3 error for malformed signing parameters of this kind.
    pub(crate) fn malformed(self, message: impl Into<String>) -> S3Error {
        let code = match self {
            AuthMethod::Header => S3ErrorCode::AuthorizationHeaderMalformed,
            AuthMethod::Presigned => S3ErrorCode::AuthorizationQueryParametersError,
        };
        S3Error::with_message(code, message.into())
    }
}

/// A request's signing parameters, before the signature is checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Signed {
    pub(crate) method: AuthMethod,
    pub(crate) access_key_id: String,
    /// The scope's date, `YYYYMMDD`.
    pub(crate) date: String,
    pub(crate) region: String,
    pub(crate) service: String,
    /// The signed header names as sent: lowercase, sorted, `;`-separated.
    pub(crate) signed_headers: String,
    pub(crate) signature: [u8; 32],
    /// The request time as sent, `YYYYMMDDTHHMMSSZ`.
    pub(crate) timestamp: String,
    /// The request time since the Unix epoch.
    pub(crate) time: Duration,
    /// How long a presigned URL stays valid.
    pub(crate) expires: Option<Duration>,
    pub(crate) session_token: Option<String>,
}

impl Signed {
    /// The credential scope, `date/region/service/aws4_request`.
    pub(crate) fn scope(&self) -> String {
        format!(
            "{}/{}/{}/{TERMINATOR}",
            self.date, self.region, self.service
        )
    }

    /// Whether `name`, lowercase, is among the signed headers.
    pub(crate) fn signs(&self, name: &str) -> bool {
        self.signed_headers.split(';').any(|signed| signed == name)
    }
}

/// The answer to a signature in a form SkyS3 does not support: SigV2 or
/// SigV4a.
pub(crate) fn unsupported() -> S3Error {
    s3_error!(
        InvalidRequest,
        "The authorization mechanism you have provided is not supported. Please use {ALGORITHM}."
    )
}

/// Reads the signing parameters of a request, or `None` for an unsigned
/// request.
///
/// # Errors
///
/// `InvalidArgument` for a request signed both ways, `InvalidRequest` for
/// an unsupported algorithm, and `AuthorizationHeaderMalformed` or
/// `AuthorizationQueryParametersError` for malformed parameters.
pub(crate) fn parse(parts: &Parts, service: &str) -> Result<Option<Signed>, S3Error> {
    let query = parts.uri.query().unwrap_or("");
    let presigned = query_pairs(query).any(|(name, _)| {
        matches!(
            name,
            "X-Amz-Algorithm" | "X-Amz-Credential" | "X-Amz-Signature"
        )
    });
    let v2_query =
        query_pairs(query).any(|(name, _)| matches!(name, "AWSAccessKeyId" | "Signature"));
    let mut authorization = parts.headers.get_all(http::header::AUTHORIZATION).iter();
    let header = authorization.next();
    if header.is_some() && authorization.next().is_some() {
        return Err(
            AuthMethod::Header.malformed("The request has more than one Authorization header.")
        );
    }
    match (header, presigned) {
        (Some(_), true) => Err(s3_error!(
            InvalidArgument,
            "Only one auth mechanism allowed; only the X-Amz-Algorithm query parameter, Signature \
             query string parameter or the Authorization header should be specified"
        )),
        (Some(value), false) => parse_header(value.as_bytes(), &parts.headers, service).map(Some),
        (None, true) => parse_query(query, service).map(Some),
        (None, false) if v2_query => Err(unsupported()),
        (None, false) => Ok(None),
    }
}

/// Parses `AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...`.
fn parse_header(value: &[u8], headers: &HeaderMap, service: &str) -> Result<Signed, S3Error> {
    let method = AuthMethod::Header;
    let value = std::str::from_utf8(value)
        .map_err(|_| method.malformed("The Authorization header is not ASCII."))?;
    let (algorithm, fields) = value.split_once(' ').unwrap_or((value, ""));
    if algorithm != ALGORITHM {
        return Err(unsupported());
    }
    let (mut credential, mut signed_headers, mut signature) = (None, None, None);
    for field in fields.split(',') {
        let field = field.trim_matches(' ');
        let (name, value) = field.split_once('=').ok_or_else(|| {
            method.malformed("The Authorization header has a field without a value.")
        })?;
        let slot = match name {
            "Credential" => &mut credential,
            "SignedHeaders" => &mut signed_headers,
            "Signature" => &mut signature,
            _ => {
                return Err(method.malformed(format!(
                    "The Authorization header has an unknown field {name:?}."
                )));
            }
        };
        if slot.replace(value).is_some() {
            return Err(method.malformed(format!(
                "The Authorization header has more than one {name}."
            )));
        }
    }
    let missing = |name: &str| method.malformed(format!("The Authorization header has no {name}."));
    let credential = credential.ok_or_else(|| missing("Credential"))?;
    let signed_headers = signed_headers.ok_or_else(|| missing("SignedHeaders"))?;
    let signature = signature.ok_or_else(|| missing("Signature"))?;
    let mut dates = headers.get_all("x-amz-date").iter();
    let timestamp = match (dates.next(), dates.next()) {
        (Some(date), None) => date.to_str().ok(),
        _ => None,
    }
    .ok_or_else(|| {
        s3_error!(
            AccessDenied,
            "AWS authentication requires a valid Date or x-amz-date header"
        )
    })?;
    let mut tokens = headers.get_all("x-amz-security-token").iter();
    let session_token = match (tokens.next(), tokens.next()) {
        (None, _) => None,
        (Some(token), None) => Some(
            token
                .to_str()
                .map_err(|_| {
                    s3_error!(
                        InvalidToken,
                        "The provided token is malformed or otherwise invalid."
                    )
                })?
                .to_owned(),
        ),
        (Some(_), Some(_)) => {
            return Err(s3_error!(
                InvalidToken,
                "The request has more than one x-amz-security-token header."
            ));
        }
    };
    build(
        method,
        credential,
        signed_headers,
        signature,
        timestamp,
        None,
        session_token,
        service,
    )
}

/// Parses the `X-Amz-*` parameters of a presigned URL.
fn parse_query(query: &str, service: &str) -> Result<Signed, S3Error> {
    let method = AuthMethod::Presigned;
    let mut values: [Option<String>; PRESIGNED_PARAMS.len()] = Default::default();
    for (name, value) in query_pairs(query) {
        let Some(index) = PRESIGNED_PARAMS.iter().position(|param| *param == name) else {
            continue;
        };
        let decoded = percent_decode(value)
            .ok_or_else(|| method.malformed(format!("The {name} query parameter is not valid.")))?;
        if values[index].replace(decoded).is_some() {
            return Err(method.malformed(format!(
                "The {name} query parameter appears more than once."
            )));
        }
    }
    let [
        algorithm,
        credential,
        timestamp,
        expires,
        signed_headers,
        signature,
        token,
    ] = values;
    let required = |value: Option<String>, name: &str| {
        value.ok_or_else(|| {
            method.malformed(format!(
                "Query-string authentication version 4 requires the X-Amz-Algorithm, \
                 X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and \
                 X-Amz-Expires parameters; {name} is missing."
            ))
        })
    };
    let algorithm = required(algorithm, "X-Amz-Algorithm")?;
    if algorithm != ALGORITHM {
        return Err(unsupported());
    }
    let credential = required(credential, "X-Amz-Credential")?;
    let timestamp = required(timestamp, "X-Amz-Date")?;
    let expires = required(expires, "X-Amz-Expires")?;
    let signed_headers = required(signed_headers, "X-Amz-SignedHeaders")?;
    let signature = required(signature, "X-Amz-Signature")?;
    let expires = parse_expires(&expires)?;
    build(
        method,
        &credential,
        &signed_headers,
        &signature,
        &timestamp,
        Some(expires),
        token,
        service,
    )
}

/// The longest a presigned URL can be valid, in seconds.
const MAX_EXPIRES_SECONDS: u64 = super::MAX_PRESIGNED_EXPIRY.as_secs();

fn parse_expires(value: &str) -> Result<Duration, S3Error> {
    let seconds = (value.len() <= 10 && value.bytes().all(|b| b.is_ascii_digit()))
        .then(|| value.parse::<u64>().ok())
        .flatten()
        .ok_or_else(|| AuthMethod::Presigned.malformed("X-Amz-Expires should be a number"))?;
    if seconds == 0 {
        return Err(AuthMethod::Presigned.malformed("X-Amz-Expires must be non-negative"));
    }
    if seconds > MAX_EXPIRES_SECONDS {
        return Err(AuthMethod::Presigned.malformed(format!(
            "X-Amz-Expires must be less than a week (in seconds) that is {MAX_EXPIRES_SECONDS}"
        )));
    }
    Ok(Duration::from_secs(seconds))
}

#[allow(clippy::too_many_arguments)]
fn build(
    method: AuthMethod,
    credential: &str,
    signed_headers: &str,
    signature: &str,
    timestamp: &str,
    expires: Option<Duration>,
    session_token: Option<String>,
    service: &str,
) -> Result<Signed, S3Error> {
    let mut scope = credential.split('/');
    let (Some(key), Some(date), Some(region), Some(scope_service), Some(terminator), None) = (
        scope.next(),
        scope.next(),
        scope.next(),
        scope.next(),
        scope.next(),
        scope.next(),
    ) else {
        return Err(method.malformed(
            "The credential is malformed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
        ));
    };
    if key.is_empty()
        || key.len() > MAX_ACCESS_KEY_ID_BYTES
        || !key.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err(method.malformed("The access key ID in the credential is not valid."));
    }
    let time = parse_timestamp(timestamp).ok_or_else(|| match method {
        AuthMethod::Header => s3_error!(
            AccessDenied,
            "AWS authentication requires a valid Date or x-amz-date header"
        ),
        AuthMethod::Presigned => method
            .malformed("X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\""),
    })?;
    if date != &timestamp[..8] {
        return Err(method.malformed(format!(
            "The credential date {date:?} does not match the request date {:?}.",
            &timestamp[..8]
        )));
    }
    if region.is_empty()
        || region.len() > MAX_REGION_BYTES
        || !region
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(method.malformed("The region in the credential is not valid."));
    }
    if scope_service != service {
        return Err(method.malformed(format!(
            "The credential should be scoped to a valid service: {service:?}."
        )));
    }
    if terminator != TERMINATOR {
        return Err(method.malformed(format!(
            "The credential should be scoped with a valid terminator: {TERMINATOR:?}."
        )));
    }
    check_signed_headers(method, signed_headers)?;
    let signature = decode_signature(signature.as_bytes())
        .ok_or_else(|| method.malformed("The signature is not 64 lowercase hexadecimal digits."))?;
    Ok(Signed {
        method,
        access_key_id: key.to_owned(),
        date: date.to_owned(),
        region: region.to_owned(),
        service: scope_service.to_owned(),
        signed_headers: signed_headers.to_owned(),
        signature,
        timestamp: timestamp.to_owned(),
        time,
        expires,
        session_token,
    })
}

/// Checks that the signed header list is lowercase header names, sorted,
/// with no duplicates, as every signer produces it. The list goes into the
/// canonical request as sent, so a list in any other form could never
/// match.
fn check_signed_headers(method: AuthMethod, list: &str) -> Result<(), S3Error> {
    let mut previous: Option<&str> = None;
    for name in list.split(';') {
        let valid = !name.is_empty()
            && name.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b)
            });
        if !valid || previous.is_some_and(|previous| previous >= name) {
            return Err(method.malformed(
                "SignedHeaders must be lowercase header names, sorted, separated by semicolons.",
            ));
        }
        previous = Some(name);
    }
    Ok(())
}

/// Decodes a signature: 64 lowercase hexadecimal digits.
pub(crate) fn decode_signature(hex: &[u8]) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0_u8; 32];
    let (pairs, _) = hex.as_chunks::<2>();
    for (byte, &[high, low]) in out.iter_mut().zip(pairs) {
        *byte = lower_hex_value(high)? << 4 | lower_hex_value(low)?;
    }
    Some(out)
}

fn lower_hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

/// Decodes `%XX` escapes. A `+` stays a `+`.
fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let value = u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
            out.push(value);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Parses a SigV4 timestamp, `YYYYMMDDTHHMMSSZ` in UTC, into the time
/// since the Unix epoch. Years before 1970 are refused.
pub(crate) fn parse_timestamp(text: &str) -> Option<Duration> {
    let bytes = text.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<u32> {
        let digits = &bytes[range];
        digits
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| digits.iter().fold(0, |n, d| n * 10 + u32::from(d - b'0')))
    };
    let (year, month, day) = (number(0..4)?, number(4..6)?, number(6..8)?);
    let (hour, minute, second) = (number(9..11)?, number(11..13)?, number(13..15)?);
    if year < 1970 || !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day == 0 || day > month_days[month as usize - 1] {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + u64::from(hour * 3600 + minute * 60 + second);
    Some(Duration::from_secs(seconds))
}

/// Days from 1970-01-01 to a date of the proleptic Gregorian calendar, for
/// years from 1970 (Howard Hinnant's `days_from_civil`).
fn days_from_civil(year: u32, month: u32, day: u32) -> u64 {
    let year = u64::from(if month <= 2 { year - 1 } else { year });
    let era = year / 400;
    let year_of_era = year - era * 400;
    let month_from_march = u64::from((month + 9) % 12);
    let day_of_year = (153 * month_from_march + 2) / 5 + u64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use http::Request;
    use proptest::prelude::*;

    use super::*;

    const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
        SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
        Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

    fn parts(uri: &str, headers: &[(&str, &str)]) -> Parts {
        let mut request = Request::get(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    fn header_auth(authorization: &str) -> Result<Option<Signed>, S3Error> {
        parse(
            &parts(
                "/b/k",
                &[
                    ("authorization", authorization),
                    ("x-amz-date", "20130524T000000Z"),
                ],
            ),
            "s3",
        )
    }

    fn code(result: Result<Option<Signed>, S3Error>) -> S3ErrorCode {
        result.unwrap_err().code().clone()
    }

    #[test]
    fn authorization_headers_are_parsed() {
        let signed = header_auth(AUTH).unwrap().unwrap();
        assert_eq!(signed.method, AuthMethod::Header);
        assert_eq!(signed.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(signed.scope(), "20130524/us-east-1/s3/aws4_request");
        assert_eq!(
            signed.signed_headers,
            "host;range;x-amz-content-sha256;x-amz-date"
        );
        assert!(signed.signs("range") && !signed.signs("rang"));
        assert_eq!(signed.time, Duration::from_secs(1_369_353_600));
        assert_eq!(signed.expires, None);
        // Fields in any order, with or without spaces.
        let reordered = "AWS4-HMAC-SHA256 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41,SignedHeaders=host,Credential=AKID/20130524/auto/s3/aws4_request";
        assert_eq!(header_auth(reordered).unwrap().unwrap().region, "auto");
        assert_eq!(parse(&parts("/b/k", &[]), "s3").unwrap(), None);
    }

    #[test]
    fn malformed_authorization_headers_are_refused() {
        let malformed = S3ErrorCode::AuthorizationHeaderMalformed;
        for (bad, expected) in [
            (
                AUTH.replace("AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256"),
                S3ErrorCode::InvalidRequest,
            ),
            (
                "AWS AKID:c2lnbmF0dXJl".to_owned(),
                S3ErrorCode::InvalidRequest,
            ),
            (AUTH.replace("Signature=", "Signature"), malformed.clone()),
            (AUTH.replace("Signature=", "Sig="), malformed.clone()),
            (format!("{AUTH}, Signature=00"), malformed.clone()),
            (
                AUTH.replace(
                    ", Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
                    "",
                ),
                malformed.clone(),
            ),
            (
                AUTH.replace("SignedHeaders=host;range;", "Foo=1, X="),
                malformed.clone(),
            ),
            (AUTH.replace("f0e8", "F0E8"), malformed.clone()),
            (AUTH.replace("bdb41", "bdb4"), malformed.clone()),
            (AUTH.replace("/s3/", "/sts/"), malformed.clone()),
            (
                AUTH.replace("aws4_request", "aws5_request"),
                malformed.clone(),
            ),
            (AUTH.replace("20130524/", "20130525/"), malformed.clone()),
            (AUTH.replace("us-east-1", "us east"), malformed.clone()),
            (
                AUTH.replace("AKIAIOSFODNN7EXAMPLE", "AKIA+"),
                malformed.clone(),
            ),
            (AUTH.replace("AKIAIOSFODNN7EXAMPLE/", ""), malformed.clone()),
            (AUTH.replace("host;range", "range;host"), malformed.clone()),
            (AUTH.replace("host;range", "host;host"), malformed.clone()),
            (AUTH.replace("host;range", "Host;range"), malformed.clone()),
            (AUTH.replace("host;range", "host;;range"), malformed.clone()),
        ] {
            assert_eq!(code(header_auth(&bad)), expected, "{bad}");
        }
        let undated = parts("/b/k", &[("authorization", AUTH)]);
        assert_eq!(code(parse(&undated, "s3")), S3ErrorCode::AccessDenied);
        let bad_date = parts(
            "/b/k",
            &[("authorization", AUTH), ("x-amz-date", "20130524")],
        );
        assert_eq!(code(parse(&bad_date, "s3")), S3ErrorCode::AccessDenied);
        let twice = parts(
            "/b/k",
            &[
                ("authorization", AUTH),
                ("authorization", AUTH),
                ("x-amz-date", "20130524T000000Z"),
            ],
        );
        assert_eq!(code(parse(&twice, "s3")), malformed);
        let both = parts(
            "/b/k?X-Amz-Signature=x",
            &[("authorization", AUTH), ("x-amz-date", "20130524T000000Z")],
        );
        assert_eq!(code(parse(&both, "s3")), S3ErrorCode::InvalidArgument);
        let v2 = parts("/b/k?AWSAccessKeyId=AKID&Signature=x&Expires=1", &[]);
        assert_eq!(code(parse(&v2, "s3")), S3ErrorCode::InvalidRequest);
    }

    const PRESIGNED: &str = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
        &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
        &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
        &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

    #[test]
    fn presigned_urls_are_parsed() {
        let signed = parse(&parts(PRESIGNED, &[]), "s3").unwrap().unwrap();
        assert_eq!(signed.method, AuthMethod::Presigned);
        assert_eq!(signed.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(signed.expires, Some(Duration::from_secs(86_400)));
        assert_eq!(signed.session_token, None);
        let token = format!("{PRESIGNED}&X-Amz-Security-Token=a%2Bb%2F%3D+c");
        let signed = parse(&parts(&token, &[]), "s3").unwrap().unwrap();
        assert_eq!(signed.session_token.as_deref(), Some("a+b/=+c"));
    }

    #[test]
    fn malformed_presigned_urls_are_refused() {
        let malformed = S3ErrorCode::AuthorizationQueryParametersError;
        for (bad, expected) in [
            (
                PRESIGNED.replace("X-Amz-Expires=86400", "X-Amz-Expires=604801"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("X-Amz-Expires=86400", "X-Amz-Expires=0"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("X-Amz-Expires=86400", "X-Amz-Expires=-1"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("X-Amz-Expires=86400", "X-Amz-Expires=99999999999"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("&X-Amz-Expires=86400", ""),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("&X-Amz-Date=20130524T000000Z", ""),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("X-Amz-Date=20130524T000000Z", "X-Amz-Date=2013-05-24"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("&X-Amz-SignedHeaders=host", ""),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("X-Amz-Algorithm=AWS4-HMAC-SHA256&", ""),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("%2F20130524", "%2G20130524"),
                malformed.clone(),
            ),
            (PRESIGNED.replace("%2Fs3%2F", "%2Fs4%2F"), malformed.clone()),
            (
                format!("{PRESIGNED}&X-Amz-Date=20130524T000000Z"),
                malformed.clone(),
            ),
            (
                PRESIGNED.replace("AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256"),
                S3ErrorCode::InvalidRequest,
            ),
        ] {
            assert_eq!(code(parse(&parts(&bad, &[]), "s3")), expected, "{bad}");
        }
    }

    #[test]
    fn timestamps_are_parsed_strictly() {
        assert_eq!(parse_timestamp("19700101T000000Z"), Some(Duration::ZERO));
        assert_eq!(
            parse_timestamp("20150830T123600Z"),
            Some(Duration::from_secs(1_440_938_160))
        );
        assert_eq!(
            parse_timestamp("20240229T235959Z"),
            Some(Duration::from_secs(1_709_251_199))
        );
        assert_eq!(
            parse_timestamp("99991231T235959Z"),
            Some(Duration::from_secs(253_402_300_799))
        );
        for bad in [
            "20230229T000000Z",
            "19000229T000000Z",
            "19691231T235959Z",
            "20130524T240000Z",
            "20130524T236000Z",
            "20130524T235960Z",
            "20131301T000000Z",
            "20130001T000000Z",
            "20130500T000000Z",
            "20130524 000000Z",
            "20130524T000000",
            "2013052AT000000Z",
            "+0130524T000000Z",
            "20130524T000000ZZ",
        ] {
            assert_eq!(parse_timestamp(bad), None, "{bad}");
        }
    }

    proptest! {
        #[test]
        fn timestamps_match_a_calendar(days in 0_u64..2_932_896, second in 0_u64..86_400) {
            // Walk the calendar from 1970 to find the date `days` after the epoch.
            let (mut year, mut left) = (1970_u32, days);
            loop {
                let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
                let length = if leap { 366 } else { 365 };
                if left < length { break; }
                left -= length;
                year += 1;
            }
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            let lengths = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
            let mut month = 0;
            while left >= lengths[month] { left -= lengths[month]; month += 1; }
            let text = format!(
                "{year:04}{:02}{:02}T{:02}{:02}{:02}Z",
                month + 1, left + 1, second / 3600, second / 60 % 60, second % 60
            );
            prop_assert_eq!(parse_timestamp(&text), Some(Duration::from_secs(days * 86_400 + second)));
        }

        #[test]
        fn arbitrary_headers_never_panic(value in proptest::collection::vec(any::<u8>(), 0..200)) {
            if let Ok(value) = http::HeaderValue::from_bytes(&value) {
                let mut request = Request::get("/b/k").body(()).unwrap();
                request.headers_mut().insert("authorization", value);
                let _ = parse(&request.into_parts().0, "s3");
            }
        }

        #[test]
        fn signatures_round_trip(bytes in any::<[u8; 32]>()) {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            prop_assert_eq!(decode_signature(hex.as_bytes()), Some(bytes));
            prop_assert_eq!(decode_signature(hex.to_uppercase().as_bytes()).is_some(), hex == hex.to_uppercase());
        }
    }
}
