//! STS query API responses, as AWS STS writes them.

use std::fmt::Write;
use std::time::Duration;

/// The XML namespace of STS responses.
const NAMESPACE: &str = "https://sts.amazonaws.com/doc/2011-06-15/";

/// What a successful `AssumeRoleWithWebIdentity` answers.
pub(crate) struct AssumeRoleResult<'a> {
    pub(crate) subject: &'a str,
    pub(crate) audience: &'a str,
    pub(crate) provider: &'a str,
    pub(crate) assumed_role_arn: &'a str,
    pub(crate) assumed_role_id: &'a str,
    pub(crate) access_key_id: &'a str,
    pub(crate) secret_access_key: &'a str,
    pub(crate) session_token: &'a str,
    pub(crate) expiration: Duration,
    pub(crate) request_id: &'a str,
}

impl AssumeRoleResult<'_> {
    /// The response document.
    pub(crate) fn to_xml(&self) -> String {
        let mut xml = String::with_capacity(1024);
        let _ = write!(
            xml,
            "<AssumeRoleWithWebIdentityResponse xmlns=\"{NAMESPACE}\">\
             <AssumeRoleWithWebIdentityResult>\
             <SubjectFromWebIdentityToken>{}</SubjectFromWebIdentityToken>\
             <Audience>{}</Audience>\
             <AssumedRoleUser><Arn>{}</Arn><AssumedRoleId>{}</AssumedRoleId></AssumedRoleUser>\
             <Credentials>\
             <AccessKeyId>{}</AccessKeyId>\
             <SecretAccessKey>{}</SecretAccessKey>\
             <SessionToken>{}</SessionToken>\
             <Expiration>{}</Expiration>\
             </Credentials>\
             <Provider>{}</Provider>\
             </AssumeRoleWithWebIdentityResult>\
             <ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>\
             </AssumeRoleWithWebIdentityResponse>",
            Escaped(self.subject),
            Escaped(self.audience),
            Escaped(self.assumed_role_arn),
            Escaped(self.assumed_role_id),
            Escaped(self.access_key_id),
            Escaped(self.secret_access_key),
            Escaped(self.session_token),
            iso8601(self.expiration),
            Escaped(self.provider),
            Escaped(self.request_id),
        );
        xml
    }
}

/// An STS error document.
pub(crate) fn error_xml(sender: bool, code: &str, message: &str, request_id: &str) -> String {
    let kind = if sender { "Sender" } else { "Receiver" };
    format!(
        "<ErrorResponse xmlns=\"{NAMESPACE}\">\
         <Error><Type>{kind}</Type><Code>{}</Code><Message>{}</Message></Error>\
         <RequestId>{}</RequestId>\
         </ErrorResponse>",
        Escaped(code),
        Escaped(message),
        Escaped(request_id),
    )
}

/// Text escaped for XML character data. Characters XML 1.0 cannot hold
/// at all, such as most control characters, are replaced with U+FFFD.
struct Escaped<'a>(&'a str);

impl std::fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for c in self.0.chars() {
            match c {
                '&' => f.write_str("&amp;")?,
                '<' => f.write_str("&lt;")?,
                '>' => f.write_str("&gt;")?,
                '"' => f.write_str("&quot;")?,
                '\'' => f.write_str("&apos;")?,
                '\t' | '\n' | '\r' => f.write_char(c)?,
                c if c.is_control() || matches!(c, '\u{fffe}' | '\u{ffff}') => {
                    f.write_char('\u{fffd}')?;
                }
                c => f.write_char(c)?,
            }
        }
        Ok(())
    }
}

/// Formats a time since the Unix epoch as ISO 8601 in UTC, to the second:
/// `2014-10-24T23:00:23Z`.
pub(crate) fn iso8601(time: Duration) -> String {
    let secs = time.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// The proleptic Gregorian date `days` after 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_times() {
        assert_eq!(iso8601(Duration::ZERO), "1970-01-01T00:00:00Z");
        assert_eq!(
            iso8601(Duration::from_secs(1_414_191_623)),
            "2014-10-24T23:00:23Z"
        );
        assert_eq!(
            iso8601(Duration::from_secs(951_782_400)),
            "2000-02-29T00:00:00Z"
        );
        assert_eq!(
            iso8601(Duration::from_millis(4_102_444_799_999)),
            "2099-12-31T23:59:59Z"
        );
    }

    #[test]
    fn escapes_text() {
        assert_eq!(
            Escaped("a<b>&\"c'\u{1}\n").to_string(),
            "a&lt;b&gt;&amp;&quot;c&apos;\u{fffd}\n"
        );
        let xml = error_xml(true, "AccessDenied", "no <access>", "id");
        assert!(xml.contains("<Type>Sender</Type><Code>AccessDenied</Code>"));
        assert!(xml.contains("<Message>no &lt;access&gt;</Message>"));
        assert!(error_xml(false, "X", "", "id").contains("<Type>Receiver</Type>"));
    }
}
