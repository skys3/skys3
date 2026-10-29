//! The production fetcher against a local server that sends canned HTTP
//! responses.

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use skys3_sts::{
    DocumentFetcher, FetchError, HttpsFetcher, HttpsFetcherError, HttpsFetcherOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Serves `response` to every connection, after reading the request head.
/// With `None`, reads the request and never answers.
async fn serve(response: Option<Vec<u8>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let response = response.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buffer[..n]),
                    }
                }
                match response {
                    Some(response) => {
                        let _ = stream.write_all(&response).await;
                        let _ = stream.shutdown().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            });
        }
    });
    format!("127.0.0.1:{}", address.port())
}

fn ok(body: &str) -> Option<Vec<u8>> {
    Some(
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes(),
    )
}

fn fetcher(allow_http: bool) -> HttpsFetcher {
    HttpsFetcher::new(HttpsFetcherOptions {
        timeout: Duration::from_millis(500),
        allow_http,
        ..HttpsFetcherOptions::default()
    })
    .unwrap()
}

#[tokio::test]
async fn fetches_a_document() {
    let host = serve(ok(r#"{"keys":[]}"#)).await;
    let document = fetcher(true)
        .fetch(&format!("http://{host}/jwks"), 1024)
        .await
        .unwrap();
    assert_eq!(document, r#"{"keys":[]}"#);
}

#[tokio::test]
async fn refuses_plain_http_unless_allowed() {
    let host = serve(ok("{}")).await;
    let url = format!("http://{host}/jwks");
    assert_eq!(
        fetcher(false).fetch(&url, 1024).await,
        Err(FetchError::UrlRejected {
            url,
            reason: "plain http is not allowed"
        })
    );
}

#[tokio::test]
async fn refuses_other_urls() {
    let fetcher = fetcher(true);
    for (url, reason) in [
        ("not a url", "not a URL"),
        ("ftp://idp.example/jwks", "not an https URL"),
        ("/jwks", "not an https URL"),
        (
            "https://user:pass@idp.example/jwks",
            "URL carries credentials",
        ),
    ] {
        assert_eq!(
            fetcher.fetch(url, 1024).await,
            Err(FetchError::UrlRejected {
                url: url.to_owned(),
                reason
            }),
            "{url}"
        );
    }
}

#[tokio::test]
async fn rejects_oversized_bodies() {
    let fetcher = fetcher(true);

    // Declared too large.
    let host = serve(ok(&"x".repeat(2048))).await;
    assert_eq!(
        fetcher.fetch(&format!("http://{host}/"), 1024).await,
        Err(FetchError::TooLarge { limit: 1024 })
    );

    // Chunked, with no declared length.
    let chunk = "y".repeat(600);
    let chunked = format!(
        "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n{len:x}\r\n{chunk}\r\n{len:x}\r\n{chunk}\r\n0\r\n\r\n",
        len = chunk.len()
    );
    let host = serve(Some(chunked.into_bytes())).await;
    assert_eq!(
        fetcher.fetch(&format!("http://{host}/"), 1024).await,
        Err(FetchError::TooLarge { limit: 1024 })
    );

    // Declared small, sent large: the declared length wins and the rest is
    // never read.
    let lying = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\n{}",
        "z".repeat(4096)
    );
    let host = serve(Some(lying.into_bytes())).await;
    assert_eq!(
        fetcher
            .fetch(&format!("http://{host}/"), 1024)
            .await
            .unwrap(),
        "zzzz"
    );
}

#[tokio::test]
async fn needs_status_200_and_follows_no_redirects() {
    let fetcher = fetcher(true);
    for (status, code) in [
        ("302 Found", 302),
        ("404 Not Found", 404),
        ("500 Oops", 500),
    ] {
        let response = format!(
            "HTTP/1.1 {status}\r\nlocation: http://elsewhere/\r\ncontent-length: 0\r\n\r\n"
        );
        let host = serve(Some(response.into_bytes())).await;
        assert_eq!(
            fetcher.fetch(&format!("http://{host}/"), 1024).await,
            Err(FetchError::Status(code))
        );
    }
}

#[tokio::test]
async fn times_out() {
    let host = serve(None).await;
    assert_eq!(
        fetcher(true).fetch(&format!("http://{host}/"), 1024).await,
        Err(FetchError::Timeout)
    );
}

#[tokio::test]
async fn reports_transport_failures() {
    let fetcher = fetcher(true);

    // Nothing listens on the port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let result = fetcher
        .fetch(&format!("http://127.0.0.1:{port}/"), 1024)
        .await;
    assert!(
        matches!(result, Err(FetchError::Transport(_))),
        "{result:?}"
    );

    // A TLS handshake with a server that speaks plain HTTP.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = stream.write_all(&ok("{}").unwrap()).await;
    });
    let result = fetcher.fetch(&format!("https://{host}/"), 1024).await;
    assert!(
        matches!(result, Err(FetchError::Transport(_))),
        "{result:?}"
    );

    // A malformed response.
    let host = serve(Some(b"SSH-2.0-OpenSSH\r\n\r\n".to_vec())).await;
    let result = fetcher.fetch(&format!("http://{host}/"), 1024).await;
    assert!(
        matches!(result, Err(FetchError::Transport(_))),
        "{result:?}"
    );

    // A body cut short.
    let host = serve(Some(
        b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nabc".to_vec(),
    ))
    .await;
    let result = fetcher.fetch(&format!("http://{host}/"), 1024).await;
    assert!(
        matches!(result, Err(FetchError::Transport(_))),
        "{result:?}"
    );
}

const CA: &str = include_str!("data/ca.pem");
const SERVER_CERT: &str = include_str!("data/server.pem");
const SERVER_KEY: &str = include_str!("data/server.key");

/// Serves `body` over TLS with a certificate for 127.0.0.1 issued by the
/// test CA.
async fn serve_tls(body: &'static str) -> String {
    let certs = CertificateDer::pem_slice_iter(SERVER_CERT.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(SERVER_KEY.as_bytes()).unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut stream) = acceptor.accept(stream).await else {
                    return;
                };
                let mut buffer = [0; 4096];
                let _ = stream.read(&mut buffer).await;
                let _ = stream.write_all(&ok(body).unwrap()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    format!("https://127.0.0.1:{}/jwks", address.port())
}

#[tokio::test]
async fn fetches_over_tls_from_a_private_root() {
    let url = serve_tls(r#"{"keys":[]}"#).await;
    let ca = CertificateDer::from_pem_slice(CA.as_bytes()).unwrap();
    let fetcher = HttpsFetcher::new(HttpsFetcherOptions {
        native_roots: false,
        extra_roots: vec![ca],
        ..HttpsFetcherOptions::default()
    })
    .unwrap();
    assert_eq!(fetcher.fetch(&url, 1024).await.unwrap(), r#"{"keys":[]}"#);
}

#[tokio::test]
async fn rejects_an_untrusted_certificate() {
    let url = serve_tls("{}").await;
    let result = HttpsFetcher::new(HttpsFetcherOptions::default())
        .unwrap()
        .fetch(&url, 1024)
        .await;
    let Err(FetchError::Transport(detail)) = result else {
        panic!("{result:?}");
    };
    assert!(detail.contains("certificate"), "{detail}");
}

#[test]
fn needs_trusted_roots() {
    let error = HttpsFetcher::new(HttpsFetcherOptions {
        native_roots: false,
        ..HttpsFetcherOptions::default()
    })
    .unwrap_err();
    assert!(matches!(error, HttpsFetcherError::NoRoots));
    assert!(error.to_string().contains("no trusted root certificates"));

    let error = HttpsFetcher::new(HttpsFetcherOptions {
        native_roots: false,
        extra_roots: vec![b"not a certificate".to_vec().into()],
        ..HttpsFetcherOptions::default()
    })
    .unwrap_err();
    assert!(matches!(error, HttpsFetcherError::Tls(_)), "{error}");
}
