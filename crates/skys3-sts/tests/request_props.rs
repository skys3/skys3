//! Property tests of the `AssumeRoleWithWebIdentity` request parser.

use proptest::prelude::*;
use skys3_sts::endpoint::request::{ACTION, API_VERSION, parse};

/// A valid request's parameters, as (name, value) pairs.
fn valid_params() -> impl Strategy<Value = Vec<(String, String)>> {
    (
        "[0-9]{0,12}",
        "([a-z0-9_-]{1,8}/){0,2}",
        "[A-Za-z0-9_-][A-Za-z0-9_.-]{0,20}",
        "[A-Za-z0-9_+=,.@-]{2,64}",
        "[A-Za-z0-9._~+/= -]{4,200}",
        proptest::option::of(900u64..50_000),
        proptest::option::of("[ -~]{1,300}"),
    )
        .prop_map(|(account, path, role, session, token, duration, policy)| {
            let mut params = vec![
                ("Action".to_owned(), ACTION.to_owned()),
                ("Version".to_owned(), API_VERSION.to_owned()),
                (
                    "RoleArn".to_owned(),
                    format!("arn:aws:iam::{account}:role/{path}{role}-role-name"),
                ),
                ("RoleSessionName".to_owned(), session),
                ("WebIdentityToken".to_owned(), token),
            ];
            if let Some(duration) = duration {
                params.push(("DurationSeconds".to_owned(), duration.to_string()));
            }
            if let Some(policy) = policy {
                params.push(("Policy".to_owned(), policy));
            }
            params
        })
}

proptest! {
    /// Any valid request parses to its values, in any order, with its
    /// parameters split in any way between the query and the body.
    #[test]
    fn valid_requests_parse(
        params in valid_params(),
        order in any::<proptest::sample::Index>(),
        split in any::<proptest::sample::Index>(),
    ) {
        let mut shuffled = params.clone();
        shuffled.rotate_left(order.index(params.len()));
        let at = split.index(shuffled.len() + 1);
        let query = serde_urlencoded::to_string(&shuffled[..at]).unwrap();
        let body = serde_urlencoded::to_string(&shuffled[at..]).unwrap();
        let request = parse(query.as_bytes(), body.as_bytes()).unwrap();
        let value = |name: &str| params.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone());
        prop_assert_eq!(Some(request.role_arn.clone()), value("RoleArn"));
        let suffix = format!("/{}", request.role);
        prop_assert!(request.role_arn.ends_with(&suffix));
        prop_assert_eq!(Some(request.session_name), value("RoleSessionName"));
        prop_assert_eq!(Some(request.token.to_string()), value("WebIdentityToken"));
        prop_assert_eq!(request.duration_seconds.map(|d| d.to_string()), value("DurationSeconds"));
        prop_assert_eq!(request.policy, value("Policy"));
    }

    /// Repeating any parameter, in the query or the body, is refused.
    #[test]
    fn repeated_parameters_are_refused(
        params in valid_params(),
        which in any::<proptest::sample::Index>(),
        in_query in any::<bool>(),
    ) {
        let repeated = params[which.index(params.len())].clone();
        let body = serde_urlencoded::to_string(&params).unwrap();
        let again = serde_urlencoded::to_string([repeated]).unwrap();
        let result = if in_query {
            parse(again.as_bytes(), body.as_bytes())
        } else {
            parse(b"", format!("{body}&{again}").as_bytes())
        };
        prop_assert_eq!(result.unwrap_err().code, "InvalidParameterValue");
    }

    /// Arbitrary input never panics.
    #[test]
    fn arbitrary_input_is_handled(query in any::<Vec<u8>>(), body in any::<Vec<u8>>()) {
        let _ = parse(&query, &body);
    }
}
