//! `AssumeRoleWithWebIdentity` through the gateway: trust-policy
//! evaluation, token errors, duration bounds, session policies, the
//! identity copy's staleness, and session expiry.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{AUDIENCE, ISSUER, ISSUER_KEY, Node, role_policy, trust_policy};
use serde_json::json;
use skys3_gateway::{CredentialLookup, LookupError, StaticCredentials};
use skys3_io::WallClock;
use skys3_sts::{MemorySessionStore, NodeCredentials, OidcProvider, SessionStore};

/// 2027-01-15T08:00:00Z.
const START: Duration = Duration::from_secs(1_800_000_000);

const CI: &str = "system:serviceaccount:ci:builder";

impl Node {
    /// A credential lookup over the node's sessions, as its gateway has.
    fn lookup(&self) -> NodeCredentials<MemorySessionStore> {
        NodeCredentials::new(
            StaticCredentials::new(),
            self.sessions.clone(),
            Arc::clone(self.sts.identity()),
            Arc::new(self.clock.clone()),
        )
    }
}

#[tokio::test]
async fn issues_session_credentials() {
    let node = Node::start(START).await;
    let answer = node.assume(&node.token(CI, json!({})), &[]).await;
    answer.assert(200, None);
    assert!(
        answer
            .body
            .starts_with("<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"),
        "{answer:?}"
    );
    assert_eq!(answer.headers["content-type"], "text/xml");
    assert_eq!(
        answer.headers["x-amzn-requestid"].to_str().unwrap(),
        answer.element("RequestId").unwrap()
    );
    assert_eq!(answer.element("SubjectFromWebIdentityToken"), Some(CI));
    assert_eq!(answer.element("Audience"), Some(AUDIENCE));
    assert_eq!(answer.element("Provider"), Some(ISSUER));
    assert_eq!(
        answer.element("Arn"),
        Some("arn:aws:sts::123456789012:assumed-role/deployer/ci-run")
    );
    let role_id = answer.element("AssumedRoleId").unwrap();
    assert!(
        role_id.starts_with("AROA") && role_id.ends_with(":ci-run"),
        "{role_id}"
    );
    // The default lifetime, session_default_seconds.
    assert_eq!(answer.element("Expiration"), Some("2027-01-15T08:30:00Z"));

    let key = answer.element("AccessKeyId").unwrap();
    let secret = answer.element("SecretAccessKey").unwrap();
    let token = answer.element("SessionToken").unwrap();
    assert!(key.starts_with("ASIA"));
    let session = node.sessions.get(key).await.unwrap().unwrap();
    assert_eq!(session.role, "deployer");
    assert_eq!(session.subject, CI);
    assert_eq!(session.issuer, ISSUER);
    assert_eq!(session.policy, None);
    let record = serde_json::to_string(&session).unwrap();
    assert!(!record.contains(secret) && !record.contains(token));

    // The gateway's lookup finds the session with its token only.
    let found = node.lookup().lookup(key, Some(token)).await.unwrap();
    assert_eq!(found.principal.name(), "assumed-role/deployer/ci-run");
    for (token, error) in [
        (None, LookupError::InvalidToken),
        (Some("not-the-token"), LookupError::InvalidToken),
    ] {
        assert_eq!(node.lookup().lookup(key, token).await.unwrap_err(), error);
    }
    assert_eq!(
        node.lookup()
            .lookup("ASIAAAAAAAAAAAAAAAAA", Some(token))
            .await
            .unwrap_err(),
        LookupError::UnknownAccessKey
    );
}

#[tokio::test]
async fn trust_policies_decide_who_may_assume_a_role() {
    let node = Node::start(START).await;
    // The issuer also accepts another audience, which the trust policy
    // does not.
    node.put_provider(
        "idp",
        OidcProvider::new(ISSUER, [AUDIENCE, "other-audience"]),
    )
    .await;
    node.sync().await.unwrap();

    let ok = node.token(CI, json!({}));
    node.assume(&ok, &[]).await.assert(200, None);
    for denied in [
        node.token("system:serviceaccount:prod:builder", json!({})),
        node.token(CI, json!({"aud": "other-audience"})),
    ] {
        node.assume(&denied, &[])
            .await
            .assert(403, Some("AccessDenied"));
    }
    // A role that does not exist is denied the same way.
    node.assume(&ok, &[("RoleArn", "arn:aws:iam::1:role/nobody")])
        .await
        .assert(403, Some("AccessDenied"));

    // An explicit deny wins over the allow.
    let deny = json!({
        "Version": "2012-10-17",
        "Statement": [
            serde_json::from_str::<serde_json::Value>(&trust_policy()).unwrap()["Statement"].clone(),
            {
                "Effect": "Deny",
                "Principal": {"Federated": ISSUER},
                "Action": "sts:*",
                "Condition": {"StringEquals": {format!("{ISSUER_KEY}:sub"): CI}},
            },
        ]
    });
    node.put_role("deployer", &deny.to_string(), &[&role_policy()])
        .await;
    node.sync().await.unwrap();
    node.assume(&ok, &[])
        .await
        .assert(403, Some("AccessDenied"));
    let other = node.token("system:serviceaccount:ci:other", json!({}));
    node.assume(&other, &[]).await.assert(200, None);
}

#[tokio::test]
async fn invalid_registers_are_left_out() {
    let node = Node::start(START).await;
    let token = node.token(CI, json!({}));
    node.assume(&token, &[]).await.assert(200, None);
    // A trust policy this build cannot read removes the role rather than
    // keeping its previous, looser version.
    let unsupported = trust_policy().replace("StringLike", "StringNotLike");
    let role = format!(
        r#"{{"trust_policy":{},"proposal_id":"01J8Z6K3V2Q4"}}"#,
        serde_json::to_string(&unsupported).unwrap()
    );
    node.put_raw("identity/roles/deployer.json", &role).await;
    node.put_raw("identity/roles/broken.json", "{").await;
    let long_name = format!("identity/roles/{}.json", "r".repeat(65));
    node.put_raw(&long_name, &role).await;
    node.put_raw("identity/other/thing.json", "{}").await;
    node.sync().await.unwrap();
    node.assume(&token, &[])
        .await
        .assert(403, Some("AccessDenied"));
    assert!(node.sts.identity().snapshot().role("broken").is_none());

    // Two providers for one issuer are both left out.
    node.put_role("deployer", &trust_policy(), &[&role_policy()])
        .await;
    node.put_provider("twin", OidcProvider::new(ISSUER, ["x"]))
        .await;
    node.sync().await.unwrap();
    assert!(node.sts.identity().snapshot().providers().is_empty());
    node.assume(&token, &[])
        .await
        .assert(400, Some("InvalidIdentityToken"));
}

#[tokio::test]
async fn token_problems_have_sts_error_codes() {
    let node = Node::start(START).await;
    let now = node.clock.now().as_secs();
    let expired = node.token(CI, json!({"iat": now - 7200, "exp": now - 3600}));
    node.assume(&expired, &[])
        .await
        .assert(400, Some("ExpiredTokenException"));
    let unknown_issuer = node.token(CI, json!({"iss": "https://elsewhere.example"}));
    node.assume(&unknown_issuer, &[])
        .await
        .assert(400, Some("InvalidIdentityToken"));
    let mut forged = node.token(CI, json!({}));
    forged.replace_range(forged.len() - 4.., "AAAA");
    node.assume(&forged, &[])
        .await
        .assert(400, Some("InvalidIdentityToken"));

    // An allowlisted issuer whose keys cannot be fetched.
    let offline = "https://offline.example";
    node.put_provider("offline", OidcProvider::new(offline, [AUDIENCE]))
        .await;
    node.sync().await.unwrap();
    let token = node.token(CI, json!({"iss": offline}));
    node.assume(&token, &[])
        .await
        .assert(400, Some("IDPCommunicationError"));
}

#[tokio::test]
async fn durations_are_bounded_by_the_configuration() {
    let node = Node::start(START).await;
    let token = node.token(CI, json!({}));
    for (duration, expiration) in [
        ("900", "2027-01-15T08:15:00Z"),
        ("7200", "2027-01-15T10:00:00Z"),
    ] {
        let answer = node.assume(&token, &[("DurationSeconds", duration)]).await;
        answer.assert(200, None);
        assert_eq!(answer.element("Expiration"), Some(expiration));
    }
    for duration in ["899", "7201", "0", "43200"] {
        let answer = node.assume(&token, &[("DurationSeconds", duration)]).await;
        answer.assert(400, Some("ValidationError"));
        assert!(
            answer.element("Message").unwrap().contains("900 to 7200"),
            "{answer:?}"
        );
    }
}

#[tokio::test]
async fn session_policies_narrow_the_session() {
    let node = Node::start(START).await;
    let token = node.token(CI, json!({}));
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"},
    })
    .to_string();
    let answer = node.assume(&token, &[("Policy", &policy)]).await;
    answer.assert(200, None);
    let key = answer.element("AccessKeyId").unwrap();
    let session = node.sessions.get(key).await.unwrap().unwrap();
    assert_eq!(session.policy.unwrap().text(), policy);

    let found = node
        .lookup()
        .lookup(key, answer.element("SessionToken"))
        .await
        .unwrap();
    let permissions = found.principal.permissions();
    let request = |action: &str| {
        skys3_types::policy::RequestContext::new(action, "arn:aws:s3:::deploy-app/key")
    };
    assert!(permissions.evaluate(&request("s3:GetObject")).is_allowed());
    assert!(!permissions.evaluate(&request("s3:PutObject")).is_allowed());

    for bad in [r#"{"Version":"2012-10-17"}"#, "not json"] {
        node.assume(&token, &[("Policy", bad)])
            .await
            .assert(400, Some("MalformedPolicyDocument"));
    }
}

#[tokio::test]
async fn a_stale_identity_copy_stops_new_sessions_only() {
    let node = Node::start(START).await;
    let token = node.token(CI, json!({}));
    let answer = node.assume(&token, &[("DurationSeconds", "7200")]).await;
    answer.assert(200, None);
    let (key, session_token) = (
        answer.element("AccessKeyId").unwrap(),
        answer.element("SessionToken").unwrap(),
    );

    // The control store goes away; the copy ages past
    // identity_max_staleness (an hour here).
    node.set_outage(true);
    node.clock.advance(Duration::from_secs(1800));
    assert!(node.sync().await.is_err());
    let fresh_token = node.token(CI, json!({}));
    node.assume(&fresh_token, &[]).await.assert(200, None);
    node.clock.advance(Duration::from_secs(1801));
    assert!(node.sync().await.is_err());
    let fresh_token = node.token(CI, json!({}));
    let refused = node.assume(&fresh_token, &[]).await;
    refused.assert(503, Some("ServiceUnavailable"));
    assert!(
        refused.body.contains("<Type>Receiver</Type>"),
        "{refused:?}"
    );

    // The session issued before still works.
    node.lookup()
        .lookup(key, Some(session_token))
        .await
        .unwrap();

    // Once a sync succeeds, sessions are issued again.
    node.set_outage(false);
    node.sync().await.unwrap();
    node.assume(&fresh_token, &[]).await.assert(200, None);
}

#[tokio::test]
async fn a_node_that_never_synced_issues_nothing() {
    let node = Node::start(START).await;
    let copy =
        skys3_sts::IdentityCopy::new(Arc::new(node.clock.clone()), Duration::from_secs(3600));
    assert_eq!(copy.fresh().unwrap_err(), skys3_sts::StaleIdentity);
    assert_eq!(copy.snapshot().synced_at(), None);
    // A clock that steps back before the last sync counts as stale.
    let snapshot = node.sts.identity().fresh().unwrap();
    node.clock
        .set(snapshot.synced_at().unwrap() - Duration::from_secs(1));
    assert!(node.sts.identity().fresh().is_err());
}

#[tokio::test]
async fn sessions_expire() {
    let node = Node::start(START).await;
    let answer = node
        .assume(&node.token(CI, json!({})), &[("DurationSeconds", "900")])
        .await;
    answer.assert(200, None);
    let key = answer.element("AccessKeyId").unwrap();
    let token = answer.element("SessionToken");
    node.clock.advance(Duration::from_secs(899));
    node.lookup().lookup(key, token).await.unwrap();
    node.clock.advance(Duration::from_secs(1));
    assert_eq!(
        node.lookup().lookup(key, token).await.unwrap_err(),
        LookupError::ExpiredToken
    );
    // A wrong token still reads as invalid, not expired.
    assert_eq!(
        node.lookup().lookup(key, Some("x")).await.unwrap_err(),
        LookupError::InvalidToken
    );
    assert_eq!(
        node.sessions
            .remove_expired(node.clock.now())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        node.lookup().lookup(key, token).await.unwrap_err(),
        LookupError::UnknownAccessKey
    );
}

#[tokio::test]
async fn a_removed_role_ends_its_sessions() {
    let node = Node::start(START).await;
    let answer = node.assume(&node.token(CI, json!({})), &[]).await;
    let key = answer.element("AccessKeyId").unwrap();
    let token = answer.element("SessionToken");
    node.lookup().lookup(key, token).await.unwrap();
    node.put_raw("identity/roles/deployer.json", "{}").await;
    node.sync().await.unwrap();
    assert_eq!(
        node.lookup().lookup(key, token).await.unwrap_err(),
        LookupError::InvalidToken
    );
}

#[tokio::test]
async fn malformed_requests_are_refused() {
    let node = Node::start(START).await;
    let token = node.token(CI, json!({}));
    node.post("application/json", "{}")
        .await
        .assert(400, Some("ValidationError"));
    node.post("application/x-www-form-urlencoded", vec![b'a'; 70_000])
        .await
        .assert(400, Some("ValidationError"));
    node.assume(&token, &[("Action", "GetCallerIdentity")])
        .await
        .assert(400, Some("InvalidAction"));
    node.assume(
        &token,
        &[("PolicyArns.member.1.arn", "arn:aws:iam::aws:policy/x")],
    )
    .await
    .assert(400, Some("InvalidParameterValue"));
    node.assume(&token, &[("RoleSessionName", "")])
        .await
        .assert(400, Some("ValidationError"));

    // Other requests to `/` are S3 requests, which need a signature.
    let request = http::Request::get("/").body(s3s::Body::empty()).unwrap();
    let response = node.gateway.handle(request).await;
    assert_eq!(response.status(), 403);
}
