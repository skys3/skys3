//! Property tests of the policy parser and evaluator against a reference
//! model.

use proptest::prelude::*;
use serde_json::{Value, json};
use skys3_types::policy::{Decision, Policy, RequestContext, S3_ARN_PREFIX};

/// Wildcard matching by its definition: exponential, but the inputs here
/// are short.
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

#[derive(Debug, Clone)]
struct ModelStatement {
    deny: bool,
    not_action: bool,
    actions: Vec<String>,
    not_resource: bool,
    resources: Vec<String>,
}

impl ModelStatement {
    fn to_json(&self) -> Value {
        let mut statement = serde_json::Map::new();
        statement.insert(
            "Effect".into(),
            json!(if self.deny { "Deny" } else { "Allow" }),
        );
        let action_key = if self.not_action {
            "NotAction"
        } else {
            "Action"
        };
        let resource_key = if self.not_resource {
            "NotResource"
        } else {
            "Resource"
        };
        statement.insert(action_key.into(), json!(self.actions));
        statement.insert(resource_key.into(), json!(self.resources));
        Value::Object(statement)
    }

    fn applies(&self, action: &str, resource: &str) -> bool {
        let any = |patterns: &[String], value: &str, fold| {
            let value: Vec<char> = value.chars().collect();
            patterns.iter().any(|pattern| {
                let pattern: Vec<char> = pattern.chars().collect();
                reference_match(&pattern, &value, fold)
            })
        };
        any(&self.actions, action, true) != self.not_action
            && any(&self.resources, resource, false) != self.not_resource
    }
}

fn model_decision(statements: &[ModelStatement], action: &str, resource: &str) -> Decision {
    let applicable: Vec<_> = statements
        .iter()
        .filter(|statement| statement.applies(action, resource))
        .collect();
    if applicable.iter().any(|statement| statement.deny) {
        Decision::ExplicitDeny
    } else if applicable.is_empty() {
        Decision::ImplicitDeny
    } else {
        Decision::Allow
    }
}

fn document(statements: &[ModelStatement]) -> String {
    let statements: Vec<Value> = statements.iter().map(ModelStatement::to_json).collect();
    json!({"Version": "2012-10-17", "Statement": statements}).to_string()
}

fn action_pattern() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("*".to_owned()),
        "(s3|S3|sts):[gG]?[eE]?[tpPdD*?]{0,2}[a-zA-Z*?]{0,4}"
            .prop_filter("an action name is not empty", |p| !p.ends_with(':')),
    ]
}

fn action() -> impl Strategy<Value = String> {
    "(s3|sts):(Get|Put|Delete)(Object|Bucket|Acl)?"
}

fn resource_pattern() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("*".to_owned()),
        "[ab*?][ab/*?é]{0,6}".prop_map(|rest| format!("{S3_ARN_PREFIX}{rest}")),
    ]
}

fn resource() -> impl Strategy<Value = String> {
    "[ab]{1,2}(/[ab/é]{0,5})?".prop_map(|rest| format!("{S3_ARN_PREFIX}{rest}"))
}

fn statement() -> impl Strategy<Value = ModelStatement> {
    (
        any::<bool>(),
        any::<bool>(),
        prop::collection::vec(action_pattern(), 1..3),
        any::<bool>(),
        prop::collection::vec(resource_pattern(), 1..3),
    )
        .prop_map(
            |(deny, not_action, actions, not_resource, resources)| ModelStatement {
                deny,
                not_action,
                actions,
                not_resource,
                resources,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn evaluation_matches_the_model(
        statements in prop::collection::vec(statement(), 1..5),
        requests in prop::collection::vec((action(), resource()), 1..8),
    ) {
        let policy = Policy::parse(&document(&statements)).unwrap();
        for (action, resource) in &requests {
            let request = RequestContext::new(action, resource);
            prop_assert_eq!(
                policy.evaluate(&request),
                model_decision(&statements, action, resource),
                "{} on {}", action, resource
            );
        }
    }

    #[test]
    fn an_explicit_deny_always_wins(
        mut statements in prop::collection::vec(statement(), 0..5),
        position in any::<prop::sample::Index>(),
        (action, resource) in (action(), resource()),
    ) {
        let deny = ModelStatement {
            deny: true,
            not_action: false,
            actions: vec![action.clone()],
            not_resource: false,
            resources: vec![resource.clone()],
        };
        statements.insert(position.index(statements.len() + 1), deny);
        let policy = Policy::parse(&document(&statements)).unwrap();
        prop_assert_eq!(
            policy.evaluate(&RequestContext::new(&action, &resource)),
            Decision::ExplicitDeny
        );
    }

    #[test]
    fn arbitrary_text_never_panics(text in any::<String>()) {
        let _ = Policy::parse(&text);
    }

    #[test]
    fn mutated_documents_never_panic(
        statements in prop::collection::vec(statement(), 1..3),
        cut in any::<prop::sample::Index>(),
        insert in "[\\[\\]{}\",:*a-zA-Z0-9 ]{0,4}",
    ) {
        let mut text = document(&statements);
        let at = cut.index(text.len() + 1);
        if text.is_char_boundary(at) {
            text.insert_str(at, &insert);
            if let Ok(policy) = Policy::parse(&text) {
                let _ = policy.evaluate(&RequestContext::new("s3:GetObject", "arn:aws:s3:::a/b"));
            }
        }
    }
}
