//! Access policies: the subset of the IAM policy language that SkyS3
//! evaluates for static credentials, roles, and session policies (§11).
//!
//! A policy is a JSON document in the IAM grammar:
//!
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": [
//!     { "Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::photos/*" },
//!     { "Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*" }
//!   ]
//! }
//! ```
//!
//! The subset, which design §11 records:
//!
//! - `Version` is required and must be `2012-10-17`; `Id` is optional.
//!   `Statement` is one statement or a non-empty array of them.
//! - A statement has an `Effect` (`Allow` or `Deny`), exactly one of
//!   `Action` and `NotAction`, exactly one of `Resource` and `NotResource`,
//!   and an optional `Sid`. Each of those four is a string or a non-empty
//!   array of strings.
//! - An action is `*` or `service:name`, such as `s3:GetObject`. The name
//!   may use the wildcards `*` (any run of characters) and `?` (one
//!   character). Actions match without regard to case.
//! - A resource is `*` or an S3 ARN, `arn:aws:s3:::` followed by a bucket
//!   and optionally `/` and a key, where `*` and `?` may appear after the
//!   prefix. A `*` also matches `/`, as in IAM. Resources match with case.
//!   Policy variables (`${...}`) are refused, not matched literally.
//! - `Principal`, `NotPrincipal`, and `Condition` are refused, as is any
//!   other key. Refusing an element is safe: ignoring a `Condition` would
//!   widen an `Allow`, and a later version can accept more without
//!   changing what an accepted policy means.
//! - A document is at most [`MAX_POLICY_BYTES`] and each pattern at most
//!   [`MAX_PATTERN_BYTES`], which also bounds the cost of matching.
//!
//! [`Policy::evaluate`] gives the [`Decision`] for one action on one
//! resource: an explicit deny if any applicable statement denies, else an
//! allow if any applicable statement allows, else an implicit deny.
//!
//! ```
//! use skys3_types::policy::{Decision, Policy, RequestContext};
//!
//! let policy: Policy = r#"{
//!     "Version": "2012-10-17",
//!     "Statement": [
//!         {"Effect": "Allow", "Action": "s3:Get*", "Resource": "arn:aws:s3:::photos/*"},
//!         {"Effect": "Deny", "Action": "*", "Resource": "arn:aws:s3:::photos/private/*"}
//!     ]
//! }"#
//! .parse()?;
//! let get = |key: &str| RequestContext::new("s3:GetObject", &format!("arn:aws:s3:::photos/{key}"));
//! assert_eq!(policy.evaluate(&get("cat.jpg")), Decision::Allow);
//! assert_eq!(policy.evaluate(&get("private/cat.jpg")), Decision::ExplicitDeny);
//! let put = RequestContext::new("s3:PutObject", "arn:aws:s3:::photos/cat.jpg");
//! assert_eq!(policy.evaluate(&put), Decision::ImplicitDeny);
//! # Ok::<(), skys3_types::policy::PolicyError>(())
//! ```

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

/// The only policy language version SkyS3 accepts.
pub const VERSION: &str = "2012-10-17";

/// The largest policy document [`Policy::parse`] accepts, in bytes: the
/// IAM limit for a role's inline policies.
pub const MAX_POLICY_BYTES: usize = 10_240;

/// The longest action or resource pattern, in bytes: room for the S3 ARN
/// prefix, a 63-byte bucket name, and a 1,024-byte key.
pub const MAX_PATTERN_BYTES: usize = 1_280;

/// The prefix of every S3 resource ARN.
pub const S3_ARN_PREFIX: &str = "arn:aws:s3:::";

/// Why a policy document was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid policy: {0}")]
pub struct PolicyError(String);

impl PolicyError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// A parsed policy. See the [module documentation](self).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RawPolicy")]
pub struct Policy {
    statements: Vec<Statement>,
}

/// Whether a statement grants or denies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Statement {
    effect: Effect,
    actions: Selector,
    resources: Selector,
}

/// `Action` or `NotAction`, `Resource` or `NotResource`: patterns, and
/// whether the statement applies to what they match or to everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Selector {
    negated: bool,
    patterns: Vec<Box<[char]>>,
}

impl Selector {
    fn matches(&self, value: &[char], fold_case: bool) -> bool {
        let matched = self
            .patterns
            .iter()
            .any(|pattern| wildcard_match(pattern, value, fold_case));
        matched != self.negated
    }
}

/// The outcome of evaluating policies for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    /// A statement allows the request and none denies it.
    Allow,
    /// No statement applies to the request.
    ImplicitDeny,
    /// A statement denies the request; no allow can override it.
    ExplicitDeny,
}

impl Decision {
    /// Whether the request may proceed.
    #[must_use]
    pub fn is_allowed(self) -> bool {
        self == Decision::Allow
    }
}

/// The action and resource of a request, prepared for matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    action: Box<[char]>,
    resource: Box<[char]>,
}

impl RequestContext {
    /// A request for `action`, such as `s3:GetObject`, on `resource`, such
    /// as `arn:aws:s3:::photos/cat.jpg`.
    #[must_use]
    pub fn new(action: &str, resource: &str) -> Self {
        Self {
            action: action.chars().collect(),
            resource: resource.chars().collect(),
        }
    }

    /// The same resource with another action, for operations that need
    /// several.
    #[must_use]
    pub fn with_action(&self, action: &str) -> Self {
        Self {
            action: action.chars().collect(),
            resource: self.resource.clone(),
        }
    }
}

impl Policy {
    /// Parses a JSON policy document of at most [`MAX_POLICY_BYTES`].
    ///
    /// # Errors
    ///
    /// If the document is too large, is not JSON, or is outside the subset
    /// the [module documentation](self) describes.
    pub fn parse(json: &str) -> Result<Self, PolicyError> {
        if json.len() > MAX_POLICY_BYTES {
            return Err(PolicyError::new(format!(
                "the document is {} bytes; at most {MAX_POLICY_BYTES} are allowed",
                json.len()
            )));
        }
        let raw: RawPolicy =
            serde_json::from_str(json).map_err(|error| PolicyError::new(error.to_string()))?;
        Self::try_from(raw)
    }

    /// A policy that allows every action on every resource.
    #[must_use]
    pub fn allow_all() -> Self {
        let any = || Selector {
            negated: false,
            patterns: vec![Box::new(['*'])],
        };
        Self {
            statements: vec![Statement {
                effect: Effect::Allow,
                actions: any(),
                resources: any(),
            }],
        }
    }

    /// Evaluates the policy for one request: explicit deny wins, then
    /// allow, else implicit deny.
    #[must_use]
    pub fn evaluate(&self, request: &RequestContext) -> Decision {
        let mut allowed = false;
        for statement in &self.statements {
            let applies = statement.actions.matches(&request.action, true)
                && statement.resources.matches(&request.resource, false);
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

impl FromStr for Policy {
    type Err = PolicyError;

    fn from_str(json: &str) -> Result<Self, Self::Err> {
        Self::parse(json)
    }
}

/// Whether `text` matches `pattern`, where `*` matches any run of
/// characters and `?` exactly one.
///
/// Greedy matching that backtracks only to the last `*`: O(pattern ×
/// text) at worst, never exponential.
fn wildcard_match(pattern: &[char], text: &[char], fold_case: bool) -> bool {
    let same = |a: char, b: char| {
        if fold_case {
            a.eq_ignore_ascii_case(&b)
        } else {
            a == b
        }
    };
    let (mut p, mut t) = (0, 0);
    // The last `*` seen, and the text position its match ends at.
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, t));
                p += 1;
            }
            Some(&c) if c == '?' || same(c, text[t]) => {
                p += 1;
                t += 1;
            }
            _ => match star {
                Some((star_p, star_t)) => {
                    // Let the last `*` take one more character.
                    star = Some((star_p, star_t + 1));
                    p = star_p + 1;
                    t = star_t + 1;
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

// The document as JSON has it. Duplicate keys are refused by the derived
// implementations, so a document cannot mean different things to
// different parsers.

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    #[serde(rename = "Version")]
    version: Option<String>,
    #[serde(rename = "Id")]
    _id: Option<String>,
    #[serde(rename = "Statement")]
    statement: Option<OneOrMany<RawStatement>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStatement {
    #[serde(rename = "Sid")]
    _sid: Option<String>,
    #[serde(rename = "Effect")]
    effect: Option<String>,
    #[serde(rename = "Action")]
    action: Option<OneOrMany<String>>,
    #[serde(rename = "NotAction")]
    not_action: Option<OneOrMany<String>>,
    #[serde(rename = "Resource")]
    resource: Option<OneOrMany<String>>,
    #[serde(rename = "NotResource")]
    not_resource: Option<OneOrMany<String>>,
    // Named so that they are refused with a clear message rather than as
    // unknown keys.
    #[serde(rename = "Principal")]
    principal: Option<IgnoredAny>,
    #[serde(rename = "NotPrincipal")]
    not_principal: Option<IgnoredAny>,
    #[serde(rename = "Condition")]
    condition: Option<IgnoredAny>,
}

impl TryFrom<RawPolicy> for Policy {
    type Error = PolicyError;

    fn try_from(raw: RawPolicy) -> Result<Self, PolicyError> {
        match raw.version.as_deref() {
            Some(VERSION) => {}
            Some(other) => {
                return Err(PolicyError::new(format!(
                    "Version is {other:?}; only {VERSION:?} is supported"
                )));
            }
            None => return Err(PolicyError::new(format!("Version {VERSION:?} is required"))),
        }
        let statements = raw
            .statement
            .ok_or_else(|| PolicyError::new("Statement is required"))?
            .non_empty("Statement")?;
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
    fn validate(self) -> Result<Statement, PolicyError> {
        for (name, present) in [
            ("Principal", self.principal.is_some()),
            ("NotPrincipal", self.not_principal.is_some()),
            ("Condition", self.condition.is_some()),
        ] {
            if present {
                return Err(PolicyError::new(format!("{name} is not supported")));
            }
        }
        let effect = match self.effect.as_deref() {
            Some("Allow") => Effect::Allow,
            Some("Deny") => Effect::Deny,
            Some(other) => {
                return Err(PolicyError::new(format!(
                    "Effect is {other:?}; it must be \"Allow\" or \"Deny\""
                )));
            }
            None => return Err(PolicyError::new("Effect is required")),
        };
        let actions = selector(
            ("Action", self.action),
            ("NotAction", self.not_action),
            action_pattern,
        )?;
        let resources = selector(
            ("Resource", self.resource),
            ("NotResource", self.not_resource),
            resource_pattern,
        )?;
        Ok(Statement {
            effect,
            actions,
            resources,
        })
    }
}

/// The selector from a key and its negated form, exactly one of which must
/// be present.
fn selector(
    (name, positive): (&str, Option<OneOrMany<String>>),
    (not_name, negative): (&str, Option<OneOrMany<String>>),
    check: fn(&str) -> Result<Box<[char]>, String>,
) -> Result<Selector, PolicyError> {
    let (negated, key, values) = match (positive, negative) {
        (Some(values), None) => (false, name, values),
        (None, Some(values)) => (true, not_name, values),
        (Some(_), Some(_)) => {
            return Err(PolicyError::new(format!(
                "{name} and {not_name} cannot both be present"
            )));
        }
        (None, None) => {
            return Err(PolicyError::new(format!(
                "{name} or {not_name} is required"
            )));
        }
    };
    let patterns = values
        .non_empty(key)?
        .iter()
        .map(|value| check(value).map_err(|why| PolicyError::new(format!("{key} {value:?} {why}"))))
        .collect::<Result<_, _>>()?;
    Ok(Selector { negated, patterns })
}

fn bounded(pattern: &str) -> Result<(), String> {
    if pattern.len() > MAX_PATTERN_BYTES {
        Err(format!("is longer than {MAX_PATTERN_BYTES} bytes"))
    } else {
        Ok(())
    }
}

/// Checks an action pattern: `*`, or `service:name` with an alphanumeric
/// service (and `-`) and a name of alphanumerics and wildcards.
fn action_pattern(pattern: &str) -> Result<Box<[char]>, String> {
    bounded(pattern)?;
    if pattern != "*" {
        let (service, name) = pattern
            .split_once(':')
            .ok_or("must be \"*\" or service:action")?;
        let service_ok = !service.is_empty()
            && service
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-');
        let name_ok = !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'*' || b == b'?');
        if !service_ok || !name_ok {
            return Err(
                "must be \"*\" or service:action, with only letters, digits, and the \
                        wildcards * and ? in the action"
                    .to_owned(),
            );
        }
    }
    Ok(pattern.chars().collect())
}

/// Checks a resource pattern: `*`, or an S3 ARN without policy variables.
fn resource_pattern(pattern: &str) -> Result<Box<[char]>, String> {
    bounded(pattern)?;
    if pattern != "*" {
        let rest = pattern
            .strip_prefix(S3_ARN_PREFIX)
            .ok_or_else(|| format!("must be \"*\" or an ARN starting with {S3_ARN_PREFIX:?}"))?;
        if rest.is_empty() {
            return Err("names no bucket".to_owned());
        }
        if rest.contains("${") {
            return Err("uses a policy variable, which is not supported".to_owned());
        }
    }
    Ok(pattern.chars().collect())
}

/// A JSON value that is one item or an array of them.
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn non_empty(self, key: &str) -> Result<Vec<T>, PolicyError> {
        match self {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) if items.is_empty() => {
                Err(PolicyError::new(format!("{key} must not be empty")))
            }
            OneOrMany::Many(items) => Ok(items),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for OneOrMany<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OneOrManyVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for OneOrManyVisitor<T> {
            type Value = OneOrMany<T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a value or an array of values")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                T::deserialize(de::value::StrDeserializer::new(value)).map(OneOrMany::One)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(OneOrMany::One)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(OneOrMany::Many(items))
            }
        }

        deserializer.deserialize_any(OneOrManyVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(text: &str) -> Vec<char> {
        text.chars().collect()
    }

    fn matches(pattern: &str, text: &str) -> bool {
        wildcard_match(&chars(pattern), &chars(text), false)
    }

    #[test]
    fn wildcards() {
        assert!(matches("*", ""));
        assert!(matches("*", "a/b/c"));
        assert!(matches("a*c", "abc"));
        assert!(matches("a*c", "ac"));
        assert!(matches("a*b*c", "a/x/b/y/c"));
        assert!(matches("a?c", "abc"));
        assert!(
            matches("a?c", "aéc"),
            "? matches one character, not one byte"
        );
        assert!(!matches("a?c", "ac"));
        assert!(!matches("a*c", "abd"));
        assert!(!matches("abc", "ab"));
        assert!(!matches("ab", "abc"));
        assert!(matches("*a*a*a", "aaaa"));
        assert!(!matches("*a*a*b", "aaaa"));
        assert!(wildcard_match(
            &chars("s3:get*"),
            &chars("s3:GetObject"),
            true
        ));
        assert!(!matches("s3:get*", "s3:GetObject"));
    }

    fn statement(effect: &str, action: &str, resource: &str) -> String {
        format!(r#"{{"Effect":"{effect}","Action":"{action}","Resource":"{resource}"}}"#)
    }

    fn policy(statements: &[String]) -> Policy {
        let json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{}]}}"#,
            statements.join(",")
        );
        json.parse().unwrap()
    }

    #[test]
    fn explicit_deny_wins_regardless_of_order() {
        let allow = statement("Allow", "s3:*", "*");
        let deny = statement("Deny", "s3:DeleteObject", "arn:aws:s3:::b/*");
        let delete = RequestContext::new("s3:DeleteObject", "arn:aws:s3:::b/k");
        for order in [[&allow, &deny], [&deny, &allow]] {
            let policy = policy(&[order[0].clone(), order[1].clone()]);
            assert_eq!(policy.evaluate(&delete), Decision::ExplicitDeny);
            assert_eq!(
                policy.evaluate(&delete.with_action("s3:GetObject")),
                Decision::Allow
            );
        }
    }

    #[test]
    fn negated_selectors_apply_to_everything_else() {
        let policy: Policy = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","NotAction":["s3:Delete*","s3:Put*"],"Resource":"*"},
            {"Effect":"Deny","Action":"*","NotResource":"arn:aws:s3:::public*"}
        ]}"#
        .parse()
        .unwrap();
        let public = RequestContext::new("s3:GetObject", "arn:aws:s3:::public/k");
        assert_eq!(policy.evaluate(&public), Decision::Allow);
        assert_eq!(
            policy.evaluate(&public.with_action("s3:PutObject")),
            Decision::ImplicitDeny
        );
        let private = RequestContext::new("s3:GetObject", "arn:aws:s3:::private/k");
        assert_eq!(policy.evaluate(&private), Decision::ExplicitDeny);
        assert!(!Decision::ExplicitDeny.is_allowed());
    }

    #[test]
    fn single_values_and_arrays_are_equivalent() {
        let one: Policy = r#"{"Version":"2012-10-17","Statement":
            {"Sid":"one","Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}"#
            .parse()
            .unwrap();
        let many: Policy = r#"{"Version":"2012-10-17","Id":"p","Statement":
            [{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["*"]}]}"#
            .parse()
            .unwrap();
        assert_eq!(one, many);
    }

    #[test]
    fn allow_all_allows_everything() {
        let request = RequestContext::new("s3:CreateBucket", "arn:aws:s3:::b");
        assert_eq!(Policy::allow_all().evaluate(&request), Decision::Allow);
        assert_eq!(Policy::allow_all(), policy(&[statement("Allow", "*", "*")]));
    }

    #[test]
    fn documents_outside_the_subset_are_refused() {
        let wrap =
            |statement: &str| format!(r#"{{"Version":"2012-10-17","Statement":{statement}}}"#);
        let cases = [
            (String::from("not json"), "expected"),
            (String::from(r#"{"Statement":[]}"#), "Version"),
            (
                String::from(r#"{"Version":"2008-10-17","Statement":[]}"#),
                "only",
            ),
            (
                String::from(r#"{"Version":"2012-10-17"}"#),
                "Statement is required",
            ),
            (wrap("[]"), "Statement must not be empty"),
            (wrap("7"), "a value or an array"),
            (
                String::from(r#"{"Version":"2012-10-17","Statement":[],"Extra":1}"#),
                "unknown field",
            ),
            (
                String::from(r#"{"Version":"2012-10-17","Version":"2012-10-17","Statement":[]}"#),
                "duplicate field",
            ),
            (
                wrap(r#"{"Action":"*","Resource":"*"}"#),
                "Effect is required",
            ),
            (
                wrap(r#"{"Effect":"allow","Action":"*","Resource":"*"}"#),
                "\"Allow\" or \"Deny\"",
            ),
            (
                wrap(r#"{"Effect":"Allow","Effect":"Deny","Action":"*","Resource":"*"}"#),
                "duplicate field",
            ),
            (
                wrap(r#"{"Effect":"Allow","Resource":"*"}"#),
                "Action or NotAction",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","NotAction":"*","Resource":"*"}"#),
                "cannot both",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*"}"#),
                "Resource or NotResource",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":[],"Resource":"*"}"#),
                "Action must not be empty",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"GetObject","Resource":"*"}"#),
                "service:action",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"s3:Get Object","Resource":"*"}"#),
                "service:action",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":":x","Resource":"*"}"#),
                "service:action",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","Resource":"arn:aws:iam::1:role/r"}"#),
                "arn:aws:s3:::",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","Resource":"arn:aws:s3:::"}"#),
                "names no bucket",
            ),
            (
                wrap(
                    r#"{"Effect":"Allow","Action":"*","Resource":"arn:aws:s3:::b/${aws:username}/*"}"#,
                ),
                "policy variable",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","Resource":"*","Principal":"*"}"#),
                "Principal is not supported",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","Resource":"*","NotPrincipal":{}}"#),
                "NotPrincipal is not supported",
            ),
            (
                wrap(
                    r#"{"Effect":"Allow","Action":"*","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":"true"}}}"#,
                ),
                "Condition is not supported",
            ),
            (
                wrap(r#"{"Effect":"Allow","Action":"*","Resource":"*","Other":1}"#),
                "unknown field",
            ),
        ];
        for (json, expected) in cases {
            let error = Policy::parse(&json).unwrap_err().to_string();
            assert!(error.contains(expected), "{json}: {error}");
        }
    }

    #[test]
    fn sizes_are_bounded() {
        let long = format!("arn:aws:s3:::b/{}", "k".repeat(MAX_PATTERN_BYTES));
        let json = format!(
            r#"{{"Version":"2012-10-17","Statement":{{"Effect":"Allow","Action":"*","Resource":"{long}"}}}}"#
        );
        assert!(
            Policy::parse(&json)
                .unwrap_err()
                .to_string()
                .contains("longer")
        );
        let huge = format!("{json}{}", " ".repeat(MAX_POLICY_BYTES));
        assert!(
            Policy::parse(&huge)
                .unwrap_err()
                .to_string()
                .contains("bytes")
        );
    }

    #[test]
    fn policies_embed_in_other_documents() {
        #[derive(Deserialize)]
        struct Role {
            policy: Policy,
        }
        let role: Role = serde_json::from_str(
            r#"{"policy":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"*","Resource":"*"}}}"#,
        )
        .unwrap();
        assert_eq!(role.policy, Policy::allow_all());
    }
}
