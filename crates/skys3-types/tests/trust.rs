//! Property tests of trust-policy evaluation against a reference model.

use proptest::prelude::*;
use serde_json::{Map, Value, json};
use skys3_types::policy::Decision;
use skys3_types::policy::trust::{TrustPolicy, WebIdentity};

/// Wildcard matching by its definition.
fn reference_match(pattern: &[char], text: &[char], fold_case: bool) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some(('*', rest)) => {
            (0..=text.len()).any(|skip| reference_match(rest, &text[skip..], fold_case))
        }
        Some((&c, rest)) => text.split_first().is_some_and(|(&t, text)| {
            let same = c == '?' || c == t || (fold_case && c.eq_ignore_ascii_case(&t));
            same && reference_match(rest, text, fold_case)
        }),
    }
}

fn matches(pattern: &str, text: &str, fold_case: bool) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    reference_match(&pattern, &text, fold_case)
}

/// The issuer keys principals can name, and how they may be written.
const PRINCIPALS: &[(&str, &str)] = &[
    ("a.example", "https://a.example"),
    ("a.example", "a.example"),
    (
        "a.example",
        "arn:aws:iam::123456789012:oidc-provider/a.example",
    ),
    ("b.example", "https://b.example"),
];

const ACTIONS: &[&str] = &[
    "sts:AssumeRoleWithWebIdentity",
    "sts:assumerolewith*",
    "sts:*",
    "*",
    "sts:AssumeRole",
    "sts:TagSession",
];

#[derive(Debug, Clone)]
struct ModelStatement {
    deny: bool,
    principal: usize,
    action: usize,
    /// Values of StringEquals and StringLike on `aud` and `sub`.
    conditions: [Option<Vec<String>>; 4],
}

const OPERATORS: [(&str, &str); 4] = [
    ("StringEquals", "aud"),
    ("StringEquals", "sub"),
    ("StringLike", "aud"),
    ("StringLike", "sub"),
];

impl ModelStatement {
    fn key(&self) -> &'static str {
        PRINCIPALS[self.principal].0
    }

    fn to_json(&self) -> Value {
        let mut condition = Map::new();
        for ((operator, claim), values) in OPERATORS.iter().zip(&self.conditions) {
            if let Some(values) = values {
                let keys = condition
                    .entry(*operator)
                    .or_insert_with(|| Value::Object(Map::new()));
                keys[format!("{}:{claim}", self.key())] = json!(values);
            }
        }
        let mut statement = json!({
            "Effect": if self.deny { "Deny" } else { "Allow" },
            "Principal": {"Federated": PRINCIPALS[self.principal].1},
            "Action": ACTIONS[self.action],
        });
        if !condition.is_empty() {
            statement["Condition"] = Value::Object(condition);
        }
        statement
    }

    fn applies(&self, issuer_key: &str, audience: &str, subject: &str) -> bool {
        let conditions =
            OPERATORS
                .iter()
                .zip(&self.conditions)
                .all(|((operator, claim), values)| {
                    let Some(values) = values else { return true };
                    let value = if *claim == "aud" { audience } else { subject };
                    values.iter().any(|pattern| {
                        if *operator == "StringEquals" {
                            pattern == value
                        } else {
                            matches(pattern, value, false)
                        }
                    })
                });
        self.key() == issuer_key
            && matches(ACTIONS[self.action], "sts:AssumeRoleWithWebIdentity", true)
            && conditions
    }
}

fn values() -> impl Strategy<Value = Option<Vec<String>>> {
    proptest::option::of(proptest::collection::vec("[ab*?]{0,4}", 1..3))
}

fn statement() -> impl Strategy<Value = ModelStatement> {
    (
        any::<bool>(),
        0..PRINCIPALS.len(),
        0..ACTIONS.len(),
        [values(), values(), values(), values()],
    )
        .prop_map(|(deny, principal, action, conditions)| ModelStatement {
            deny,
            principal,
            action,
            conditions,
        })
}

proptest! {
    #[test]
    fn evaluation_matches_the_model(
        statements in proptest::collection::vec(statement(), 1..5),
        issuer in prop_oneof![Just("https://a.example"), Just("https://b.example")],
        audience in "[ab]{0,4}",
        subject in "[ab]{0,4}",
    ) {
        let document = json!({
            "Version": "2012-10-17",
            "Statement": statements.iter().map(ModelStatement::to_json).collect::<Vec<_>>(),
        })
        .to_string();
        let policy = TrustPolicy::parse(&document).unwrap();
        let embedded: TrustPolicy = serde_json::from_str(&document).unwrap();
        prop_assert_eq!(&embedded, &policy);

        let key = issuer.strip_prefix("https://").unwrap();
        let applying: Vec<_> = statements
            .iter()
            .filter(|statement| statement.applies(key, &audience, &subject))
            .collect();
        let expected = if applying.iter().any(|statement| statement.deny) {
            Decision::ExplicitDeny
        } else if applying.is_empty() {
            Decision::ImplicitDeny
        } else {
            Decision::Allow
        };
        let identity = WebIdentity {
            issuer,
            audience: &audience,
            subject: &subject,
            authorized_party: None,
        };
        prop_assert_eq!(policy.evaluate(&identity), expected);
    }

    #[test]
    fn arbitrary_documents_are_handled(document in "\\PC{0,200}") {
        let _ = TrustPolicy::parse(&document);
    }
}
