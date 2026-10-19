//! An OpenID Connect issuer over HTTPS for web-identity tests: it serves
//! its discovery document and key set with the test server certificate,
//! and mints tokens signed with a test key.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use skys3_sts::Algorithm;
use skys3_sts::testkit::{TestKey, jwks};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::tls_file;

/// The longest request head the issuer reads.
const MAX_HEAD: usize = 16 * 1024;
/// How long the issuer waits for a request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// A running issuer. Dropping it stops the server.
pub struct Issuer {
    /// The issuer URL, `https://127.0.0.1:<port>`, which tokens carry as
    /// `iss`.
    pub url: String,
    key: TestKey,
    server: JoinHandle<()>,
}

impl Issuer {
    /// Starts an issuer on a free loopback port. A node trusts it when its
    /// fetcher trusts the test CA, as `SSL_CERT_FILE` makes the binary do.
    pub async fn start() -> Self {
        let tls = skys3::tls::server_config(&tls_file("server.pem"), &tls_file("server.key"))
            .expect("the test certificate loads");
        let acceptor = TlsAcceptor::from(tls);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "https://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        );
        let key = TestKey::rsa("sdk-matrix");
        let documents = Arc::new(Documents {
            discovery: json!({
                "issuer": url,
                "jwks_uri": format!("{url}/keys"),
                "response_types_supported": ["id_token"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["RS256"],
            })
            .to_string(),
            keys: jwks(&[&key]),
        });
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (acceptor, documents) = (acceptor.clone(), Arc::clone(&documents));
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(REQUEST_TIMEOUT, async move {
                        let mut stream = acceptor.accept(stream).await?;
                        let path = read_path(&mut stream).await?;
                        stream.write_all(&documents.answer(&path)).await?;
                        stream.shutdown().await
                    })
                    .await;
                });
            }
        });
        Issuer { url, key, server }
    }

    /// The issuer key a trust policy names: the URL without `https://`.
    pub fn key(&self) -> &str {
        self.url.trim_start_matches("https://")
    }

    /// A token for `subject` and `audience`, issued now and valid for
    /// `lifetime`.
    pub fn token(&self, subject: &str, audience: &str, lifetime: Duration) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims: Value = json!({
            "iss": self.url,
            "sub": subject,
            "aud": audience,
            "iat": now,
            "exp": now + lifetime.as_secs(),
        });
        self.key.sign(Algorithm::Rs256, &claims)
    }
}

impl Drop for Issuer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// What the issuer serves.
struct Documents {
    discovery: String,
    keys: String,
}

impl Documents {
    /// The whole HTTP response for a `GET` of `path`.
    fn answer(&self, path: &str) -> Vec<u8> {
        let (status, body) = match path {
            "/.well-known/openid-configuration" => ("200 OK", self.discovery.as_str()),
            "/keys" => ("200 OK", self.keys.as_str()),
            _ => ("404 Not Found", "{}"),
        };
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }
}

/// Reads a request head and returns its target.
async fn read_path<S: AsyncReadExt + Unpin>(stream: &mut S) -> std::io::Result<String> {
    let mut head = Vec::new();
    let mut buffer = [0; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buffer).await?;
        if n == 0 || head.len() + n > MAX_HEAD {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        head.extend_from_slice(&buffer[..n]);
    }
    let line = String::from_utf8_lossy(&head);
    Ok(line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned())
}
