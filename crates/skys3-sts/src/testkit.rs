//! Test keys and token minting, for tests of token validation and STS.
//!
//! This module is hidden from the documentation and is not a stable API.

use std::sync::{Arc, OnceLock};

use aws_lc_rs::hmac;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::KeySize;
use aws_lc_rs::signature::{
    self, ECDSA_P256_SHA256_FIXED_SIGNING, ECDSA_P384_SHA384_FIXED_SIGNING, EcdsaKeyPair,
    Ed25519KeyPair, KeyPair, RsaEncoding, RsaKeyPair,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

use crate::jwt::Algorithm;

pub fn b64(bytes: impl AsRef<[u8]>) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// RSA key generation is slow, so each test binary makes two keys and
/// shares them.
fn rsa_key_pair(index: usize) -> Arc<RsaKeyPair> {
    static KEYS: OnceLock<[Arc<RsaKeyPair>; 2]> = OnceLock::new();
    let keys = KEYS
        .get_or_init(|| [0, 1].map(|_| Arc::new(RsaKeyPair::generate(KeySize::Rsa2048).unwrap())));
    Arc::clone(&keys[index])
}

#[derive(Clone)]
enum Signer {
    Rsa(Arc<RsaKeyPair>),
    Ec(Arc<EcdsaKeyPair>),
    Ed(Arc<Ed25519KeyPair>),
}

/// A signing key with its public JWK.
#[derive(Clone)]
pub struct TestKey {
    signer: Signer,
    kid: Option<String>,
}

impl TestKey {
    /// The first shared RSA key.
    pub fn rsa(kid: &str) -> Self {
        Self::rsa_n(0, kid)
    }

    /// The second shared RSA key, a different key pair from [`TestKey::rsa`].
    pub fn other_rsa(kid: &str) -> Self {
        Self::rsa_n(1, kid)
    }

    fn rsa_n(index: usize, kid: &str) -> Self {
        TestKey {
            signer: Signer::Rsa(rsa_key_pair(index)),
            kid: Some(kid.to_owned()),
        }
    }

    pub fn ec(alg: Algorithm, kid: &str) -> Self {
        let curve = match alg {
            Algorithm::Es256 => &ECDSA_P256_SHA256_FIXED_SIGNING,
            Algorithm::Es384 => &ECDSA_P384_SHA384_FIXED_SIGNING,
            _ => panic!("{alg} is not an ECDSA algorithm"),
        };
        TestKey {
            signer: Signer::Ec(Arc::new(EcdsaKeyPair::generate(curve).unwrap())),
            kid: Some(kid.to_owned()),
        }
    }

    pub fn ed25519(kid: &str) -> Self {
        TestKey {
            signer: Signer::Ed(Arc::new(Ed25519KeyPair::generate().unwrap())),
            kid: Some(kid.to_owned()),
        }
    }

    /// Returns a key for `alg`: the shared RSA key for RSA algorithms.
    pub fn for_algorithm(alg: Algorithm, kid: &str) -> Self {
        match alg {
            Algorithm::Es256 | Algorithm::Es384 => Self::ec(alg, kid),
            Algorithm::EdDsa => Self::ed25519(kid),
            _ => Self::rsa(kid),
        }
    }

    /// Returns the same key without a key ID, in its JWK and its tokens.
    pub fn without_kid(mut self) -> Self {
        self.kid = None;
        self
    }

    /// The public JWK, without `alg`.
    pub fn jwk(&self) -> Value {
        let mut jwk = match &self.signer {
            Signer::Rsa(pair) => {
                let (n, e) = rsa_components(pair.public_key().as_ref());
                json!({"kty": "RSA", "n": b64(n), "e": b64(e)})
            }
            Signer::Ec(pair) => {
                let point = pair.public_key().as_ref();
                let size = (point.len() - 1) / 2;
                let crv = if size == 32 { "P-256" } else { "P-384" };
                json!({
                    "kty": "EC",
                    "crv": crv,
                    "x": b64(&point[1..=size]),
                    "y": b64(&point[1 + size..]),
                })
            }
            Signer::Ed(pair) => {
                json!({"kty": "OKP", "crv": "Ed25519", "x": b64(pair.public_key().as_ref())})
            }
        };
        if let Some(kid) = &self.kid {
            jwk["kid"] = json!(kid);
        }
        jwk
    }

    /// Mints a token signed with `alg`, with this key's `kid`.
    pub fn sign(&self, alg: Algorithm, claims: &Value) -> String {
        let mut header = json!({"alg": alg.name(), "typ": "JWT"});
        if let Some(kid) = &self.kid {
            header["kid"] = json!(kid);
        }
        self.sign_raw(alg, &header, claims)
    }

    /// Mints a token with an arbitrary header, signed as `alg`.
    pub fn sign_raw(&self, alg: Algorithm, header: &Value, claims: &Value) -> String {
        let input = format!("{}.{}", b64(header.to_string()), b64(claims.to_string()));
        format!("{input}.{}", b64(self.signature(alg, input.as_bytes())))
    }

    fn signature(&self, alg: Algorithm, message: &[u8]) -> Vec<u8> {
        match &self.signer {
            Signer::Rsa(pair) => {
                let encoding: &'static dyn RsaEncoding = match alg {
                    Algorithm::Rs256 => &signature::RSA_PKCS1_SHA256,
                    Algorithm::Rs384 => &signature::RSA_PKCS1_SHA384,
                    Algorithm::Rs512 => &signature::RSA_PKCS1_SHA512,
                    Algorithm::Ps256 => &signature::RSA_PSS_SHA256,
                    Algorithm::Ps384 => &signature::RSA_PSS_SHA384,
                    Algorithm::Ps512 => &signature::RSA_PSS_SHA512,
                    _ => panic!("{alg} is not an RSA algorithm"),
                };
                let mut signature = vec![0; pair.public_modulus_len()];
                pair.sign(encoding, &SystemRandom::new(), message, &mut signature)
                    .unwrap();
                signature
            }
            Signer::Ec(pair) => pair
                .sign(&SystemRandom::new(), message)
                .unwrap()
                .as_ref()
                .to_vec(),
            Signer::Ed(pair) => pair.sign(message).as_ref().to_vec(),
        }
    }

    /// Mints an `HS256` token whose HMAC key is this key's public JWK
    /// modulus or point: the classic algorithm-confusion forgery.
    pub fn hs256_confusion(&self, claims: &Value) -> String {
        let secret = self.jwk().to_string();
        let mut header = json!({"alg": "HS256", "typ": "JWT"});
        if let Some(kid) = &self.kid {
            header["kid"] = json!(kid);
        }
        let input = format!("{}.{}", b64(header.to_string()), b64(claims.to_string()));
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let tag = hmac::sign(&key, input.as_bytes());
        format!("{input}.{}", b64(tag.as_ref()))
    }
}

/// A JWKS document holding `keys`.
pub fn jwks(keys: &[&TestKey]) -> String {
    json!({"keys": keys.iter().map(|key| key.jwk()).collect::<Vec<_>>()}).to_string()
}

/// Splits a PKCS#1 `RSAPublicKey` (a DER SEQUENCE of two INTEGERs) into its
/// modulus and exponent, without leading zero bytes.
fn rsa_components(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    fn read(der: &[u8], pos: &mut usize, tag: u8) -> Vec<u8> {
        assert_eq!(der[*pos], tag);
        *pos += 1;
        let mut len = usize::from(der[*pos]);
        *pos += 1;
        if len & 0x80 != 0 {
            let octets = len & 0x7f;
            len = der[*pos..*pos + octets]
                .iter()
                .fold(0, |acc, byte| acc << 8 | usize::from(*byte));
            *pos += octets;
        }
        let value = der[*pos..*pos + len].to_vec();
        *pos += len;
        value
    }
    let mut pos = 0;
    let sequence = read(der, &mut pos, 0x30);
    let mut inner = 0;
    let strip = |mut int: Vec<u8>| {
        let zeros = int.iter().take_while(|byte| **byte == 0).count();
        int.drain(..zeros);
        int
    };
    let n = strip(read(&sequence, &mut inner, 0x02));
    let e = strip(read(&sequence, &mut inner, 0x02));
    (n, e)
}
