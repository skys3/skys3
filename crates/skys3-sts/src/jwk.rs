//! JSON Web Key Sets (RFC 7517) holding an issuer's signature keys.
//!
//! Only public signature keys are kept: RSA keys of 2048 to 8192 bits,
//! P-256 and P-384 keys, and Ed25519 keys. A key that is marked for
//! encryption, names an algorithm SkyS3 does not verify, is symmetric
//! (`oct`), or is malformed is skipped, so one odd key does not make the
//! issuer's other keys unusable. Keys are parsed and checked when the set is
//! loaded, not when a token is verified.

use aws_lc_rs::signature::{
    self, ECDSA_P256_SHA256_FIXED, ECDSA_P384_SHA384_FIXED, ED25519, ParsedPublicKey,
    RsaParameters, RsaPublicKeyComponents,
};
use base64::Engine;
use base64::alphabet::URL_SAFE;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::jwt::Algorithm;

/// The most keys a set may hold. Issuers publish a handful; a set with more
/// is treated as hostile.
pub const MAX_KEYS: usize = 100;

/// RSA moduli below this many bits are rejected.
const MIN_RSA_BITS: usize = 2048;
/// RSA moduli above this many bits are rejected, as `aws-lc-rs` does.
const MAX_RSA_BITS: usize = 8192;

/// Base64url for key material. RFC 7518 omits padding, but tolerating it
/// costs nothing here: keys are not compared as text.
const KEY_BASE64: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Why a key set could not be loaded.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum JwksError {
    /// The document is not a JSON object with a `keys` array.
    #[error("the JWKS is not a JSON object with a `keys` array")]
    Malformed,
    /// The set holds more than [`MAX_KEYS`] keys.
    #[error("the JWKS holds more than {MAX_KEYS} keys")]
    TooManyKeys,
}

/// An issuer's usable signature keys.
#[derive(Debug, Default)]
pub struct KeySet {
    keys: Vec<Jwk>,
}

impl KeySet {
    /// Parses a JWKS document, skipping keys that cannot verify a supported
    /// algorithm.
    ///
    /// # Errors
    ///
    /// Returns a [`JwksError`] if the document is not a key set or holds too
    /// many keys.
    pub fn parse(document: &[u8]) -> Result<KeySet, JwksError> {
        let Ok(Value::Object(mut set)) = serde_json::from_slice::<Value>(document) else {
            return Err(JwksError::Malformed);
        };
        let Some(Value::Array(entries)) = set.remove("keys") else {
            return Err(JwksError::Malformed);
        };
        if entries.len() > MAX_KEYS {
            return Err(JwksError::TooManyKeys);
        }
        let mut keys = Vec::with_capacity(entries.len());
        for (index, entry) in entries.iter().enumerate() {
            match Jwk::parse(entry) {
                Ok(key) => keys.push(key),
                Err(reason) => tracing::debug!(index, reason, "skipping a JWKS key"),
            }
        }
        Ok(KeySet { keys })
    }

    /// Returns the number of usable keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns whether the set has no usable keys.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the keys that may verify a token signed with `alg` whose
    /// header names `kid`. A token without a `kid` may be signed by any key
    /// of the right type.
    pub fn candidates<'a>(
        &'a self,
        kid: Option<&'a str>,
        alg: Algorithm,
    ) -> impl Iterator<Item = &'a Jwk> + 'a {
        self.keys.iter().filter(move |key| {
            key.accepts(alg) && kid.is_none_or(|kid| key.kid.as_deref() == Some(kid))
        })
    }
}

/// One usable public signature key.
#[derive(Debug)]
pub struct Jwk {
    kid: Option<String>,
    /// The key's `alg`, if the issuer restricts it to one algorithm.
    alg: Option<Algorithm>,
    key: PublicKey,
}

#[derive(Debug)]
enum PublicKey {
    /// An RSA key, usable with every RSA algorithm unless `alg` narrows it.
    Rsa { n: Vec<u8>, e: Vec<u8> },
    /// A P-256, P-384, or Ed25519 key, bound to its only algorithm.
    Bound {
        alg: Algorithm,
        key: ParsedPublicKey,
    },
}

impl Jwk {
    /// Returns the key's ID.
    pub fn kid(&self) -> Option<&str> {
        self.kid.as_deref()
    }

    /// Returns whether this key may verify signatures made with `alg`.
    fn accepts(&self, alg: Algorithm) -> bool {
        let fits = match &self.key {
            PublicKey::Rsa { .. } => alg.is_rsa(),
            PublicKey::Bound { alg: bound, .. } => *bound == alg,
        };
        fits && self.alg.is_none_or(|restricted| restricted == alg)
    }

    /// Returns whether `signature` is a valid `alg` signature of `message`
    /// by this key.
    pub fn verify(&self, alg: Algorithm, message: &[u8], signature: &[u8]) -> bool {
        if !self.accepts(alg) {
            return false;
        }
        match &self.key {
            PublicKey::Rsa { n, e } => rsa_parameters(alg).is_some_and(|parameters| {
                RsaPublicKeyComponents { n, e }
                    .verify(parameters, message, signature)
                    .is_ok()
            }),
            PublicKey::Bound { key, .. } => key.verify_sig(message, signature).is_ok(),
        }
    }

    /// Parses one JWKS entry, or says why it is skipped.
    fn parse(entry: &Value) -> Result<Jwk, &'static str> {
        let Value::Object(fields) = entry else {
            return Err("not a JSON object");
        };
        if let Some(usage) = fields.get("use")
            && usage != "sig"
        {
            return Err("not a signature key");
        }
        if let Some(ops) = fields.get("key_ops")
            && !ops
                .as_array()
                .is_some_and(|ops| ops.iter().any(|op| op == "verify"))
        {
            return Err("key_ops excludes verify");
        }
        let alg = match fields.get("alg") {
            None => None,
            Some(Value::String(name)) => Some(
                name.parse::<Algorithm>()
                    .map_err(|_| "unsupported algorithm")?,
            ),
            Some(_) => return Err("alg is not a string"),
        };
        let kid = match fields.get("kid") {
            None => None,
            Some(Value::String(kid)) => Some(kid.clone()),
            Some(_) => return Err("kid is not a string"),
        };
        let key = match fields.get("kty").and_then(Value::as_str) {
            Some("RSA") => rsa_key(fields)?,
            Some("EC") => ec_key(fields)?,
            Some("OKP") => okp_key(fields)?,
            _ => return Err("unsupported key type"),
        };
        let jwk = Jwk { kid, alg, key };
        if let Some(alg) = alg
            && !jwk.accepts(alg)
        {
            return Err("alg does not match the key type");
        }
        Ok(jwk)
    }
}

fn component(fields: &Map<String, Value>, name: &str) -> Result<Vec<u8>, &'static str> {
    fields
        .get(name)
        .and_then(Value::as_str)
        .and_then(|text| KEY_BASE64.decode(text).ok())
        .ok_or("a key component is missing or not base64url")
}

fn rsa_key(fields: &Map<String, Value>) -> Result<PublicKey, &'static str> {
    let mut n = component(fields, "n")?;
    let e = component(fields, "e")?;
    let leading_zeros = n.iter().take_while(|byte| **byte == 0).count();
    n.drain(..leading_zeros);
    let bits = n
        .first()
        .map_or(0, |top| n.len() * 8 - top.leading_zeros() as usize);
    if !(MIN_RSA_BITS..=MAX_RSA_BITS).contains(&bits) {
        return Err("RSA modulus size is out of range");
    }
    RsaPublicKeyComponents { n: &n, e: &e }
        .to_parsed_public_key(&signature::RSA_PKCS1_2048_8192_SHA256)
        .map_err(|_| "invalid RSA key")?;
    Ok(PublicKey::Rsa { n, e })
}

fn ec_key(fields: &Map<String, Value>) -> Result<PublicKey, &'static str> {
    let (alg, verification, size): (_, &'static dyn signature::VerificationAlgorithm, _) =
        match fields.get("crv").and_then(Value::as_str) {
            Some("P-256") => (Algorithm::Es256, &ECDSA_P256_SHA256_FIXED, 32),
            Some("P-384") => (Algorithm::Es384, &ECDSA_P384_SHA384_FIXED, 48),
            _ => return Err("unsupported curve"),
        };
    let x = component(fields, "x")?;
    let y = component(fields, "y")?;
    if x.len() != size || y.len() != size {
        return Err("EC coordinates have the wrong length");
    }
    let point = [&[0x04][..], &x, &y].concat();
    let key = ParsedPublicKey::new(verification, point).map_err(|_| "invalid EC key")?;
    Ok(PublicKey::Bound { alg, key })
}

fn okp_key(fields: &Map<String, Value>) -> Result<PublicKey, &'static str> {
    if fields.get("crv").and_then(Value::as_str) != Some("Ed25519") {
        return Err("unsupported curve");
    }
    let x = component(fields, "x")?;
    if x.len() != 32 {
        return Err("Ed25519 key has the wrong length");
    }
    let key = ParsedPublicKey::new(&ED25519, x).map_err(|_| "invalid Ed25519 key")?;
    Ok(PublicKey::Bound {
        alg: Algorithm::EdDsa,
        key,
    })
}

fn rsa_parameters(alg: Algorithm) -> Option<&'static RsaParameters> {
    Some(match alg {
        Algorithm::Rs256 => &signature::RSA_PKCS1_2048_8192_SHA256,
        Algorithm::Rs384 => &signature::RSA_PKCS1_2048_8192_SHA384,
        Algorithm::Rs512 => &signature::RSA_PKCS1_2048_8192_SHA512,
        Algorithm::Ps256 => &signature::RSA_PSS_2048_8192_SHA256,
        Algorithm::Ps384 => &signature::RSA_PSS_2048_8192_SHA384,
        Algorithm::Ps512 => &signature::RSA_PSS_2048_8192_SHA512,
        Algorithm::Es256 | Algorithm::Es384 | Algorithm::EdDsa => return None,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::testkit::{TestKey, b64};

    fn parse(keys: Value) -> KeySet {
        KeySet::parse(json!({ "keys": keys }).to_string().as_bytes()).unwrap()
    }

    fn with(key: &TestKey, changes: Value) -> Value {
        let mut jwk = key.jwk();
        for (name, value) in changes.as_object().unwrap() {
            if value.is_null() {
                jwk.as_object_mut().unwrap().remove(name);
            } else {
                jwk[name] = value.clone();
            }
        }
        jwk
    }

    #[test]
    fn loads_every_supported_key_type() {
        let keys = [
            TestKey::rsa("rsa"),
            TestKey::ec(Algorithm::Es256, "p256"),
            TestKey::ec(Algorithm::Es384, "p384"),
            TestKey::ed25519("ed"),
        ];
        let set = parse(keys.iter().map(TestKey::jwk).collect());
        assert_eq!(set.len(), 4);
        assert!(!set.is_empty());
        let kids = |alg| {
            set.candidates(None, alg)
                .map(|key| key.kid().unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(kids(Algorithm::Rs256), ["rsa"]);
        assert_eq!(kids(Algorithm::Ps512), ["rsa"]);
        assert_eq!(kids(Algorithm::Es256), ["p256"]);
        assert_eq!(kids(Algorithm::Es384), ["p384"]);
        assert_eq!(kids(Algorithm::EdDsa), ["ed"]);
        assert_eq!(set.candidates(Some("p256"), Algorithm::Es256).count(), 1);
        assert_eq!(set.candidates(Some("p256"), Algorithm::Rs256).count(), 0);
        assert_eq!(set.candidates(Some("nope"), Algorithm::Rs256).count(), 0);
    }

    #[test]
    fn keys_verify_only_their_own_algorithms() {
        let rsa = TestKey::rsa("k");
        let set = parse(json!([with(&rsa, json!({"alg": "PS256"}))]));
        let key = set.candidates(None, Algorithm::Ps256).next().unwrap();
        let token = rsa.sign(Algorithm::Ps256, &json!({}));
        let (input, signature) = token.rsplit_once('.').unwrap();
        let signature = KEY_BASE64.decode(signature).unwrap();
        assert!(key.verify(Algorithm::Ps256, input.as_bytes(), &signature));
        assert!(!key.verify(Algorithm::Rs256, input.as_bytes(), &signature));
        assert!(!key.verify(Algorithm::Es256, input.as_bytes(), &signature));
        assert!(!key.verify(Algorithm::Ps256, b"other", &signature));
        assert!(rsa_parameters(Algorithm::EdDsa).is_none());
    }

    #[test]
    fn skips_keys_it_cannot_use() {
        let rsa = TestKey::rsa("rsa");
        let ec = TestKey::ec(Algorithm::Es256, "ec");
        let ed = TestKey::ed25519("ed");
        let small_modulus = b64([&[0xc0][..], &[1; 127]].concat());
        let huge_modulus = b64([&[0xc0][..], &[1; 1024]].concat());
        for (jwk, why) in [
            (json!("not an object"), "not an object"),
            (with(&rsa, json!({"use": "enc"})), "encryption key"),
            (with(&rsa, json!({"key_ops": ["encrypt"]})), "no verify op"),
            (
                with(&rsa, json!({"key_ops": "verify"})),
                "key_ops not an array",
            ),
            (
                with(&rsa, json!({"alg": "RSA-OAEP"})),
                "encryption algorithm",
            ),
            (with(&rsa, json!({"alg": "HS256"})), "HMAC algorithm"),
            (with(&rsa, json!({"alg": 1})), "alg not a string"),
            (
                with(&rsa, json!({"alg": "ES256"})),
                "alg of another key type",
            ),
            (with(&rsa, json!({"kid": 1})), "kid not a string"),
            (
                with(&rsa, json!({"kty": "oct", "k": "c2VjcmV0"})),
                "symmetric key",
            ),
            (with(&rsa, json!({"kty": null})), "no key type"),
            (with(&rsa, json!({"n": small_modulus})), "1024-bit modulus"),
            (with(&rsa, json!({"n": huge_modulus})), "8200-bit modulus"),
            (with(&rsa, json!({"n": ""})), "empty modulus"),
            (with(&rsa, json!({"e": null})), "no exponent"),
            (with(&rsa, json!({"e": "AA"})), "zero exponent"),
            (with(&rsa, json!({"n": "*"})), "modulus not base64"),
            (with(&ec, json!({"crv": "P-521"})), "unsupported curve"),
            (with(&ec, json!({"crv": null})), "no curve"),
            (with(&ec, json!({"x": b64([1; 31])})), "short coordinate"),
            (
                with(&ec, json!({"x": b64([1; 32]), "y": b64([1; 32])})),
                "point off the curve",
            ),
            (with(&ec, json!({"alg": "ES384"})), "alg of another curve"),
            (with(&ed, json!({"crv": "X25519"})), "key-agreement curve"),
            (with(&ed, json!({"x": b64([1; 31])})), "short Ed25519 key"),
            (
                with(&ed, json!({"alg": "RS256"})),
                "alg of another key type",
            ),
        ] {
            assert!(parse(json!([jwk])).is_empty(), "{why}: {jwk}");
        }
    }

    #[test]
    fn tolerates_harmless_variations() {
        let rsa = TestKey::rsa("rsa");
        let jwk = rsa.jwk();
        let n = KEY_BASE64.decode(jwk["n"].as_str().unwrap()).unwrap();
        let zero_prefixed_and_padded =
            base64::engine::general_purpose::URL_SAFE.encode([&[0][..], &n].concat());
        for variant in [
            with(&rsa, json!({"n": zero_prefixed_and_padded})),
            with(&rsa, json!({"use": "sig", "key_ops": ["sign", "verify"]})),
            with(&rsa, json!({"x5c": ["MIIB"], "x5t": "abc", "ext": true})),
            with(&rsa, json!({"alg": "RS512"})),
            with(&rsa, json!({"kid": null})),
        ] {
            assert_eq!(parse(json!([variant])).len(), 1, "{variant}");
        }
    }

    #[test]
    fn rejects_malformed_or_oversized_sets() {
        for document in ["", "[]", "{}", r#"{"keys":{}}"#, r#"{"keys":null}"#, "{"] {
            assert_eq!(
                KeySet::parse(document.as_bytes()).unwrap_err(),
                JwksError::Malformed,
                "{document}"
            );
        }
        let entries = vec![json!({"kty": "oct"}); MAX_KEYS + 1];
        let document = json!({ "keys": entries }).to_string();
        assert_eq!(
            KeySet::parse(document.as_bytes()).unwrap_err(),
            JwksError::TooManyKeys
        );
        assert_eq!(
            JwksError::TooManyKeys.to_string(),
            "the JWKS holds more than 100 keys"
        );
        let entries = vec![json!({"kty": "oct"}); MAX_KEYS];
        let document = json!({ "keys": entries }).to_string();
        assert!(KeySet::parse(document.as_bytes()).unwrap().is_empty());
    }

    proptest! {
        #[test]
        fn arbitrary_documents_never_panic(document in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = KeySet::parse(&document);
        }

        #[test]
        fn arbitrary_keys_never_panic(
            kty in "(RSA|EC|OKP|oct|x)",
            crv in "(P-256|P-384|Ed25519|x)",
            n in prop::collection::vec(any::<u8>(), 0..300),
            e in prop::collection::vec(any::<u8>(), 0..8),
            x in prop::collection::vec(any::<u8>(), 0..50),
            y in prop::collection::vec(any::<u8>(), 0..50),
        ) {
            let jwk = json!({"kty": kty, "crv": crv, "n": b64(n), "e": b64(e), "x": b64(x), "y": b64(y)});
            prop_assert!(parse(json!([jwk])).len() <= 1);
        }
    }
}
