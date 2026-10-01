//! Trust policies: who may assume a role with a web identity token
//! (design §11).
//!
//! A role's trust policy is a JSON document in the IAM grammar:
//!
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": {
//!     "Effect": "Allow",
//!     "Principal": {"Federated": "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"},
//!     "Action": "sts:AssumeRoleWithWebIdentity",
//!     "Condition": {
//!       "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
//!       "StringLike": {"token.actions.githubusercontent.com:sub": "repo:example/app:*"}
//!     }
//!   }
//! }
//! ```
//!
//! The subset, which design §11 records:
//!
//! - `Version` (`2012-10-17`), `Id`, and `Statement` as in identity
//!   policies ([`crate::policy`]). A statement has an `Effect`, a
//!   `Principal`, an `Action`, an optional `Condition`, and an optional
//!   `Sid`.
//! - `Principal` is an object whose only key is `Federated`: one or more
//!   OIDC issuers, each written as its issuer URL, as its *issuer key*, or
//!   as `arn:aws:iam::<account>:oidc-provider/<issuer key>` with any
//!   account digits. The issuer key is the issuer URL without its
//!   `https://` scheme, as AWS writes it (an `http://` issuer, used only in
//!   tests, keeps its URL). Wildcards are refused.
//! - `Action` is one or more action patterns, as in identity policies; a
//!   statement applies to `sts:AssumeRoleWithWebIdentity` if one matches.
//! - `Condition` maps an operator to keys and values. The operators are
//!   `StringEquals` (exact) and `StringLike` (`*` and `?` wildcards), both
//!   case-sensitive. A key is `<issuer key>:aud`, `<issuer key>:sub`, or
//!   `<issuer key>:azp`; its issuer must be a principal of the statement. A
//!   key holds when the token's claim matches any of its values, and a
//!   statement's conditions hold when every key of every operator holds. A
//!   key the token does not have (another issuer's, or `azp` on a token
//!   without one) does not hold.
//! - `NotPrincipal`, `NotAction`, `Resource`, `NotResource`, other
//!   operators (including `...IfExists` and the negated ones), other keys,
//!   policy variables (`${...}`), unknown members, and duplicate members are
//!   refused, so an accepted document never means less than it says.
//! - The limits of identity policies apply: at most
//!   [`MAX_POLICY_BYTES`](super::MAX_POLICY_BYTES) of text (operators and
//!   condition keys count as text), patterns and values of at most
//!   [`MAX_PATTERN_BYTES`](super::MAX_PATTERN_BYTES), and arrays and
//!   objects of at most [`MAX_ARRAY_LEN`] entries. [`TrustPolicy::parse`]
//!   and `Deserialize` apply the same rules.
//!
//! [`TrustPolicy::evaluate`] decides as identity policies do: an explicit
//! deny wins, then an allow, else an implicit deny.
//!
//! ```
//! use skys3_types::policy::Decision;
//! use skys3_types::policy::trust::{TrustPolicy, WebIdentity};
//!
//! let policy: TrustPolicy = r#"{
//!     "Version": "2012-10-17",
//!     "Statement": {
//!         "Effect": "Allow",
//!         "Principal": {"Federated": "https://idp.example"},
//!         "Action": "sts:AssumeRoleWithWebIdentity",
//!         "Condition": {"StringLike": {"idp.example:sub": "system:serviceaccount:ci:*"}}
//!     }
//! }"#
//! .parse()?;
//! let mut identity = WebIdentity {
//!     issuer: "https://idp.example",
//!     audience: "sts.amazonaws.com",
//!     subject: "system:serviceaccount:ci:builder",
//!     authorized_party: None,
//! };
//! assert_eq!(policy.evaluate(&identity), Decision::Allow);
//! identity.subject = "system:serviceaccount:prod:builder";
//! assert_eq!(policy.evaluate(&identity), Decision::ImplicitDeny);
//! # Ok::<(), skys3_types::policy::PolicyError>(())
//! ```

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::Deserialize;
use serde::de::{self, Deserializer, IgnoredAny, MapAccess, Visitor};

use super::{
    Decision, Effect, MAX_ARRAY_LEN, OneOrMany, PolicyError, PolicyLanguage, VERSION,
    action_pattern, bounded, check_text, check_version, wildcard_match,
};

/// The action a trust policy authorizes.
pub const ASSUME_ROLE_WITH_WEB_IDENTITY: &str = "sts:AssumeRoleWithWebIdentity";

/// The prefix of an OIDC provider ARN, before the account.
const PROVIDER_ARN_PREFIX: &str = "arn:aws:iam::";

/// What follows the account in an OIDC provider ARN.
const PROVIDER_ARN_RESOURCE: &str = ":oidc-provider/";

/// The caller of `AssumeRoleWithWebIdentity`, from its verified token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebIdentity<'a> {
    /// The token's issuer (`iss`), an allowlisted issuer URL.
    pub issuer: &'a str,
    /// The token's audience that the issuer's provider record accepted.
    pub audience: &'a str,
    /// The token's subject (`sub`).
    pub subject: &'a str,
    /// The token's authorized party (`azp`), if it has one.
    pub authorized_party: Option<&'a str>,
}

/// Returns an issuer's key: its URL without the `https://` scheme, as
/// trust-policy principals and condition keys name it.
#[must_use]
pub fn issuer_key(issuer: &str) -> &str {
    issuer.strip_prefix("https://").unwrap_or(issuer)
}

/// A parsed trust policy. See the [module documentation](self).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RawTrustPolicy")]
pub struct TrustPolicy {
    statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Statement {
    effect: Effect,
    /// Issuer keys.
    principals: Vec<String>,
    actions: Vec<Box<[char]>>,
    conditions: Vec<Condition>,
}

/// One key of one condition operator.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Condition {
    operator: Operator,
    issuer: String,
    claim: Claim,
    values: Vec<Box<[char]>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    StringEquals,
    StringLike,
}

/// A token claim a condition key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Claim {
    Aud,
    Sub,
    Azp,
}

impl TrustPolicy {
    /// Parses a JSON trust policy.
    ///
    /// # Errors
    ///
    /// If the document is not JSON, or is outside the subset or the limits
    /// the [module documentation](self) describes.
    pub fn parse(json: &str) -> Result<Self, PolicyError> {
        let raw: RawTrustPolicy =
            serde_json::from_str(json).map_err(|error| PolicyError::new(error.to_string()))?;
        Self::try_from(raw)
    }

    /// Decides whether `identity` may assume the role: an explicit deny
    /// wins, then an allow, else an implicit deny.
    #[must_use]
    pub fn evaluate(&self, identity: &WebIdentity<'_>) -> Decision {
        let action: Vec<char> = ASSUME_ROLE_WITH_WEB_IDENTITY.chars().collect();
        let issuer = issuer_key(identity.issuer);
        let mut allowed = false;
        for statement in &self.statements {
            let applies = statement.principals.iter().any(|key| key == issuer)
                && statement
                    .actions
                    .iter()
                    .any(|pattern| wildcard_match(pattern, &action, true))
                && statement
                    .conditions
                    .iter()
                    .all(|condition| condition.holds(identity));
            if applies {
                match statement.effect {
                    Effect::Deny => return Decision::ExplicitDeny,
                    Effect::Allow => allowed = true,
                }
            }
        }
        if allowed {
            Decision::Allow
        } else {
            Decision::ImplicitDeny
        }
    }
}

impl Condition {
    fn holds(&self, identity: &WebIdentity<'_>) -> bool {
        if self.issuer != issuer_key(identity.issuer) {
            return false;
        }
        let value = match self.claim {
            Claim::Aud => Some(identity.audience),
            Claim::Sub => Some(identity.subject),
            Claim::Azp => identity.authorized_party,
        };
        let Some(value) = value else {
            return false;
        };
        let value: Vec<char> = value.chars().collect();
        self.values.iter().any(|pattern| match self.operator {
            Operator::StringEquals => **pattern == *value,
            Operator::StringLike => wildcard_match(pattern, &value, false),
        })
    }
}

impl FromStr for TrustPolicy {
    type Err = PolicyError;

    fn from_str(json: &str) -> Result<Self, Self::Err> {
        Self::parse(json)
    }
}

impl PolicyLanguage for TrustPolicy {
    fn parse_document(json: &str) -> Result<Self, PolicyError> {
        Self::parse(json)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTrustPolicy {
    #[serde(rename = "Version")]
    version: Option<String>,
    #[serde(rename = "Id")]
    id: Option<String>,
    #[serde(rename = "Statement")]
    statement: Option<OneOrMany<RawStatement>>,
}

type RawConditions = UniqueMap<UniqueMap<OneOrMany<String>>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStatement {
    #[serde(rename = "Sid")]
    sid: Option<String>,
    #[serde(rename = "Effect")]
    effect: Option<String>,
    #[serde(rename = "Principal")]
    principal: Option<RawPrincipal>,
    #[serde(rename = "Action")]
    action: Option<OneOrMany<String>>,
    #[serde(rename = "Condition")]
    condition: Option<RawConditions>,
    // Named so that they are refused with a clear message.
    #[serde(rename = "NotPrincipal")]
    not_principal: Option<IgnoredAny>,
    #[serde(rename = "NotAction")]
    not_action: Option<IgnoredAny>,
    #[serde(rename = "Resource")]
    resource: Option<IgnoredAny>,
    #[serde(rename = "NotResource")]
    not_resource: Option<IgnoredAny>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPrincipal {
    #[serde(rename = "Federated")]
    federated: OneOrMany<String>,
}

impl TryFrom<RawTrustPolicy> for TrustPolicy {
    type Error = PolicyError;

    fn try_from(raw: RawTrustPolicy) -> Result<Self, PolicyError> {
        check_version(raw.version.as_deref())?;
        let statements = raw
            .statement
            .ok_or_else(|| PolicyError::new("Statement is required"))?
            .non_empty("Statement")?;
        let text = VERSION.len()
            + raw.id.as_ref().map_or(0, String::len)
            + statements.iter().map(RawStatement::text_len).sum::<usize>();
        check_text(text)?;
        let statements = statements
            .into_iter()
            .enumerate()
            .map(|(index, statement)| {
                statement
                    .validate()
                    .map_err(|error| PolicyError::new(format!("Statement {index}: {}", error.0)))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { statements })
    }
}

impl RawStatement {
    /// The bytes of text in the statement's strings, condition operators
    /// and keys included.
    fn text_len(&self) -> usize {
        let conditions = self.condition.as_ref().map_or(0, |operators| {
            operators
                .0
                .iter()
                .map(|(operator, keys)| {
                    operator.len()
                        + keys
                            .0
                            .iter()
                            .map(|(key, values)| key.len() + values.text_len())
                            .sum::<usize>()
                })
                .sum()
        });
        self.sid.as_ref().map_or(0, String::len)
            + self.effect.as_ref().map_or(0, String::len)
            + self
                .principal
                .as_ref()
                .map_or(0, |principal| principal.federated.text_len())
            + self.action.as_ref().map_or(0, OneOrMany::text_len)
            + conditions
    }

    fn validate(self) -> Result<Statement, PolicyError> {
        for (name, present) in [
            ("NotPrincipal", self.not_principal.is_some()),
            ("NotAction", self.not_action.is_some()),
            ("Resource", self.resource.is_some()),
            ("NotResource", self.not_resource.is_some()),
        ] {
            if present {
                return Err(PolicyError::new(format!(
                    "{name} is not supported in a trust policy"
                )));
            }
        }
        let effect = Effect::parse(self.effect.as_deref())?;
        let principals = self
            .principal
            .ok_or_else(|| PolicyError::new("Principal is required"))?
            .federated
            .non_empty("Federated")?
            .iter()
            .map(|principal| federated_principal(principal))
            .collect::<Result<Vec<_>, _>>()?;
        let actions = self
            .action
            .ok_or_else(|| PolicyError::new("Action is required"))?
            .non_empty("Action")?
            .iter()
            .map(|action| {
                action_pattern(action)
                    .map_err(|why| PolicyError::new(format!("Action {action:?} {why}")))
            })
            .collect::<Result<_, _>>()?;
        let mut conditions = Vec::new();
        for (operator, keys) in self.condition.map(|map| map.0).unwrap_or_default() {
            let operator = match operator.as_str() {
                "StringEquals" => Operator::StringEquals,
                "StringLike" => Operator::StringLike,
                other => {
                    return Err(PolicyError::new(format!(
                        "the condition operator {other:?} is not supported; use StringEquals \
                         or StringLike"
                    )));
                }
            };
            for (key, values) in keys.0 {
                conditions.push(condition(operator, &key, values, &principals)?);
            }
        }
        Ok(Statement {
            effect,
            principals,
            actions,
            conditions,
        })
    }
}

/// The issuer key a `Federated` principal names.
fn federated_principal(principal: &str) -> Result<String, PolicyError> {
    let refuse = |why: &str| PolicyError::new(format!("Federated {principal:?} {why}"));
    bounded(principal).map_err(|why| refuse(&why))?;
    let key = match principal.strip_prefix(PROVIDER_ARN_PREFIX) {
        Some(rest) => {
            let (account, key) = rest
                .split_once(PROVIDER_ARN_RESOURCE)
                .ok_or_else(|| refuse("is not an OIDC provider ARN"))?;
            if !account.bytes().all(|b| b.is_ascii_digit()) {
                return Err(refuse("has an account that is not digits"));
            }
            key
        }
        None => issuer_key(principal),
    };
    if key.is_empty() || key.contains(['*', '?']) || key.contains("${") {
        return Err(refuse("must name one issuer, without wildcards"));
    }
    if key.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(refuse("contains whitespace or control characters"));
    }
    Ok(key.to_owned())
}

/// One condition key and its values.
fn condition(
    operator: Operator,
    key: &str,
    values: OneOrMany<String>,
    principals: &[String],
) -> Result<Condition, PolicyError> {
    let refuse = |why: &str| PolicyError::new(format!("Condition key {key:?} {why}"));
    let (issuer, claim) = key
        .rsplit_once(':')
        .ok_or_else(|| refuse("must be <issuer>:aud, <issuer>:sub, or <issuer>:azp"))?;
    let claim = match claim {
        "aud" => Claim::Aud,
        "sub" => Claim::Sub,
        "azp" => Claim::Azp,
        _ => {
            return Err(refuse(
                "must be <issuer>:aud, <issuer>:sub, or <issuer>:azp",
            ));
        }
    };
    if !principals.iter().any(|principal| principal == issuer) {
        return Err(refuse(
            "names an issuer that is not a principal of the statement",
        ));
    }
    let values = values
        .non_empty(key)?
        .iter()
        .map(|value| {
            bounded(value).map_err(|why| refuse(&why))?;
            if value.contains("${") {
                return Err(refuse(
                    "has a value with a policy variable, which is not supported",
                ));
            }
            Ok(value.chars().collect())
        })
        .collect::<Result<_, _>>()?;
    Ok(Condition {
        operator,
        issuer: issuer.to_owned(),
        claim,
        values,
    })
}

/// A JSON object read in document order, refusing duplicate keys, so a
/// document cannot mean different things to different parsers, and more
/// than [`MAX_ARRAY_LEN`] members.
struct UniqueMap<V>(Vec<(String, V)>);

impl<'de, V: Deserialize<'de>> Deserialize<'de> for UniqueMap<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueMapVisitor<V>(PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for UniqueMapVisitor<V> {
            type Value = UniqueMap<V>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut seen = HashSet::new();
                let mut entries = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, V>()? {
                    if entries.len() == MAX_ARRAY_LEN {
                        return Err(de::Error::custom(format!(
                            "an object may hold at most {MAX_ARRAY_LEN} members"
                        )));
                    }
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom(format!("duplicate key {key:?}")));
                    }
                    entries.push((key, value));
                }
                Ok(UniqueMap(entries))
            }
        }

        deserializer.deserialize_map(UniqueMapVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISSUER: &str = "https://token.actions.githubusercontent.com";

    fn identity<'a>(subject: &'a str, audience: &'a str) -> WebIdentity<'a> {
        WebIdentity {
            issuer: ISSUER,
            audience,
            subject,
            authorized_party: None,
        }
    }

    fn statement(effect: &str, principal: &str, condition: &str) -> String {
        format!(
            r#"{{"Effect":"{effect}","Principal":{{"Federated":"{principal}"}},"Action":"sts:AssumeRoleWithWebIdentity"{condition}}}"#
        )
    }

    fn policy(statements: &[String]) -> TrustPolicy {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{}]}}"#,
            statements.join(",")
        )
        .parse()
        .unwrap()
    }

    #[test]
    fn principals_name_issuers_in_three_forms() {
        for principal in [
            ISSUER,
            "token.actions.githubusercontent.com",
            "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com",
        ] {
            let policy = policy(&[statement("Allow", principal, "")]);
            assert_eq!(
                policy.evaluate(&identity("repo:a/b", "sts")),
                Decision::Allow,
                "{principal}"
            );
            let other = WebIdentity {
                issuer: "https://other.example",
                ..identity("repo:a/b", "sts")
            };
            assert_eq!(policy.evaluate(&other), Decision::ImplicitDeny);
        }
        // An http issuer, as tests use, keeps its scheme in its key.
        let local = "http://127.0.0.1:8080";
        let policy = policy(&[statement(
            "Allow",
            local,
            r#","Condition":{"StringEquals":{"http://127.0.0.1:8080:sub":"me"}}"#,
        )]);
        let caller = WebIdentity {
            issuer: local,
            ..identity("me", "sts")
        };
        assert_eq!(policy.evaluate(&caller), Decision::Allow);
    }

    #[test]
    fn conditions_must_all_hold() {
        let key = "token.actions.githubusercontent.com";
        let condition = format!(
            r#","Condition":{{"StringEquals":{{"{key}:aud":["sts","other"]}},"StringLike":{{"{key}:sub":"repo:org/*:ref:refs/heads/main"}}}}"#
        );
        let policy = policy(&[statement("Allow", ISSUER, &condition)]);
        let allowed = identity("repo:org/app:ref:refs/heads/main", "other");
        assert_eq!(policy.evaluate(&allowed), Decision::Allow);
        for denied in [
            identity("repo:org/app:ref:refs/heads/dev", "sts"),
            identity("repo:org/app:ref:refs/heads/main", "STS"),
            identity("repo:other/app:ref:refs/heads/main", "sts"),
        ] {
            assert_eq!(
                policy.evaluate(&denied),
                Decision::ImplicitDeny,
                "{denied:?}"
            );
        }
    }

    #[test]
    fn missing_claims_do_not_hold() {
        let policy = policy(&[statement(
            "Allow",
            ISSUER,
            r#","Condition":{"StringLike":{"token.actions.githubusercontent.com:azp":"*"}}"#,
        )]);
        let without = identity("s", "a");
        assert_eq!(policy.evaluate(&without), Decision::ImplicitDeny);
        let with = WebIdentity {
            authorized_party: Some("client"),
            ..without
        };
        assert_eq!(policy.evaluate(&with), Decision::Allow);
    }

    #[test]
    fn explicit_deny_wins() {
        let deny = statement(
            "Deny",
            ISSUER,
            r#","Condition":{"StringEquals":{"token.actions.githubusercontent.com:sub":"mallory"}}"#,
        );
        let allow = statement("Allow", ISSUER, "");
        for order in [[&allow, &deny], [&deny, &allow]] {
            let policy = policy(&[order[0].clone(), order[1].clone()]);
            assert_eq!(
                policy.evaluate(&identity("mallory", "a")),
                Decision::ExplicitDeny
            );
            assert_eq!(policy.evaluate(&identity("alice", "a")), Decision::Allow);
        }
    }

    #[test]
    fn actions_must_match_the_operation() {
        let other = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow",
            "Principal":{"Federated":"idp.example"},"Action":["sts:AssumeRole","sts:TagSession"]}}"#;
        let policy: TrustPolicy = other.parse().unwrap();
        let caller = WebIdentity {
            issuer: "https://idp.example",
            ..identity("s", "a")
        };
        assert_eq!(policy.evaluate(&caller), Decision::ImplicitDeny);
        let wildcard: TrustPolicy = other
            .replace("sts:AssumeRole\"", "sts:assume*\"")
            .parse()
            .unwrap();
        assert_eq!(wildcard.evaluate(&caller), Decision::Allow);
    }

    #[test]
    fn documents_outside_the_subset_are_refused() {
        let wrap =
            |statement: &str| format!(r#"{{"Version":"2012-10-17","Statement":{statement}}}"#);
        let base = r#""Effect":"Allow","Action":"sts:AssumeRoleWithWebIdentity""#;
        let principal = r#""Principal":{"Federated":"idp.example"}"#;
        let with = |extra: &str| wrap(&format!("{{{base},{principal},{extra}}}"));
        let cases = [
            ("{}".to_owned(), "Version"),
            (
                r#"{"Version":"2012-10-17"}"#.to_owned(),
                "Statement is required",
            ),
            (wrap("[]"), "must not be empty"),
            (wrap(&format!("{{{base}}}")), "Principal is required"),
            (
                wrap(&format!(r#"{{"Effect":"Allow",{principal}}}"#)),
                "Action is required",
            ),
            (with(r#""Resource":"*""#), "Resource is not supported"),
            (with(r#""NotResource":"*""#), "NotResource is not supported"),
            (with(r#""NotAction":"*""#), "NotAction is not supported"),
            (
                with(r#""NotPrincipal":{}"#),
                "NotPrincipal is not supported",
            ),
            (with(r#""Other":1"#), "unknown field"),
            (
                wrap(&format!(r#"{{{base},"Principal":"*"}}"#)),
                "invalid type",
            ),
            (
                wrap(&format!(r#"{{{base},"Principal":{{"AWS":"*"}}}}"#)),
                "unknown field",
            ),
            (
                wrap(&format!(r#"{{{base},"Principal":{{"Federated":[]}}}}"#)),
                "Federated must not be empty",
            ),
            (
                wrap(&format!(r#"{{{base},"Principal":{{"Federated":"*"}}}}"#)),
                "without wildcards",
            ),
            (
                wrap(&format!(
                    r#"{{{base},"Principal":{{"Federated":"https://"}}}}"#
                )),
                "without wildcards",
            ),
            (
                wrap(&format!(r#"{{{base},"Principal":{{"Federated":"a b"}}}}"#)),
                "whitespace",
            ),
            (
                wrap(&format!(
                    r#"{{{base},"Principal":{{"Federated":"arn:aws:iam::x:oidc-provider/idp"}}}}"#
                )),
                "not digits",
            ),
            (
                wrap(&format!(
                    r#"{{{base},"Principal":{{"Federated":"arn:aws:iam::1:role/r"}}}}"#
                )),
                "not an OIDC provider ARN",
            ),
            (
                with(r#""Condition":{"StringNotEquals":{"idp.example:sub":"x"}}"#),
                "not supported",
            ),
            (
                with(r#""Condition":{"StringEqualsIfExists":{"idp.example:sub":"x"}}"#),
                "not supported",
            ),
            (
                with(r#""Condition":{"StringEquals":{"idp.example:email":"x"}}"#),
                "must be <issuer>:aud",
            ),
            (
                with(r#""Condition":{"StringEquals":{"sub":"x"}}"#),
                "must be <issuer>:aud",
            ),
            (
                with(r#""Condition":{"StringEquals":{"other.example:sub":"x"}}"#),
                "not a principal",
            ),
            (
                with(r#""Condition":{"StringEquals":{"idp.example:sub":[]}}"#),
                "must not be empty",
            ),
            (
                with(r#""Condition":{"StringEquals":{"idp.example:sub":"${aws:username}"}}"#),
                "policy variable",
            ),
            (
                with(
                    r#""Condition":{"StringEquals":{"idp.example:sub":"a","idp.example:sub":"b"}}"#,
                ),
                "duplicate key",
            ),
            (
                with(r#""Condition":{"StringEquals":{"idp.example:sub":"a"},"StringEquals":{}}"#),
                "duplicate key",
            ),
            (with(r#""Condition":[]"#), "an object"),
        ];
        for (json, expected) in cases {
            let error = TrustPolicy::parse(&json).unwrap_err().to_string();
            assert!(error.contains(expected), "{json}: {error}");
        }
        let sid = "s".repeat(super::super::MAX_POLICY_BYTES);
        let huge = with(&format!(r#""Sid":"{sid}""#));
        let error = TrustPolicy::parse(&huge).unwrap_err().to_string();
        assert!(error.contains("bytes of text"), "{error}");
        let spaced = format!(
            "{}{}",
            with(r#""Sid":"x""#),
            " ".repeat(2 * super::super::MAX_POLICY_BYTES)
        );
        assert!(
            TrustPolicy::parse(&spaced).is_ok(),
            "whitespace is not text"
        );
        let keys: Vec<String> = (0..=MAX_ARRAY_LEN)
            .map(|n| format!(r#""idp.example:sub{n}":"v""#))
            .collect();
        let wide = with(&format!(
            r#""Condition":{{"StringEquals":{{{}}}}}"#,
            keys.join(",")
        ));
        let error = TrustPolicy::parse(&wide).unwrap_err().to_string();
        assert!(error.contains("at most"), "{error}");
        let values = vec![r#""v""#; MAX_ARRAY_LEN + 1].join(",");
        let long = with(&format!(
            r#""Condition":{{"StringEquals":{{"idp.example:sub":[{values}]}}}}"#
        ));
        let error = TrustPolicy::parse(&long).unwrap_err().to_string();
        assert!(error.contains("at most"), "{error}");
    }
}
