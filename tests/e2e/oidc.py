#!/usr/bin/env python3
"""A static OpenID Connect issuer for the end-to-end suite (plan M8-01).

    oidc.py serve --port <port> --cert <pem> --key <pem> --signing-key <pem>
    oidc.py token --issuer <url> --signing-key <pem> --subject <sub>
                  [--audience <aud>] [--lifetime <seconds>]

`serve` answers OIDC Discovery (`/.well-known/openid-configuration`) and
the key set (`/keys`) over HTTPS at https://127.0.0.1:<port>, the issuer
URL, until it is killed. `token` prints an RS256 token from that issuer.
Both use only the standard library and the openssl command, so the suite
needs no Python packages: the key set comes from the key's modulus, and
tokens are signed with `openssl dgst -sha256 -sign`.
"""

import argparse
import base64
import http.server
import json
import ssl
import subprocess
import sys
import time

KEY_ID = "skys3-e2e"


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def modulus(signing_key: str) -> bytes:
    out = subprocess.run(
        ["openssl", "rsa", "-in", signing_key, "-noout", "-modulus"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    # "Modulus=<hex>"
    return bytes.fromhex(out.split("=", 1)[1])


def jwks(signing_key: str) -> dict:
    return {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": KEY_ID,
                "n": b64url(modulus(signing_key)),
                "e": b64url((65537).to_bytes(3, "big")),
            }
        ]
    }


def sign(signing_key: str, payload: bytes) -> bytes:
    return subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", signing_key, "-binary"],
        input=payload,
        check=True,
        capture_output=True,
    ).stdout


def token(args: argparse.Namespace) -> None:
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT", "kid": KEY_ID}
    claims = {
        "iss": args.issuer,
        "sub": args.subject,
        "aud": args.audience,
        "iat": now,
        "exp": now + args.lifetime,
    }
    signing_input = ".".join(
        b64url(json.dumps(part, separators=(",", ":")).encode()) for part in (header, claims)
    )
    signature = sign(args.signing_key, signing_input.encode())
    print(f"{signing_input}.{b64url(signature)}")


def serve(args: argparse.Namespace) -> None:
    issuer = f"https://127.0.0.1:{args.port}"
    documents = {
        "/.well-known/openid-configuration": {
            "issuer": issuer,
            "jwks_uri": f"{issuer}/keys",
            "response_types_supported": ["id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        },
        "/keys": jwks(args.signing_key),
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (the http.server API)
            document = documents.get(self.path)
            body = json.dumps(document if document is not None else {}).encode()
            self.send_response(200 if document is not None else 404)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            sys.stderr.write("oidc: " + (format % args) + "\n")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    print(f"oidc: serving {issuer}", file=sys.stderr, flush=True)
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    s = commands.add_parser("serve")
    s.add_argument("--port", type=int, required=True)
    s.add_argument("--cert", required=True)
    s.add_argument("--key", required=True)
    s.add_argument("--signing-key", required=True)
    t = commands.add_parser("token")
    t.add_argument("--issuer", required=True)
    t.add_argument("--signing-key", required=True)
    t.add_argument("--subject", required=True)
    t.add_argument("--audience", default="sts.amazonaws.com")
    t.add_argument("--lifetime", type=int, default=600)
    args = parser.parse_args()
    {"serve": serve, "token": token}[args.command](args)


if __name__ == "__main__":
    main()
