"""A small S3 client: SigV4 signing, PUT from a generated body, GET into
nothing, and CreateBucket with SkyS3's bucket headers.

Bodies are sent with `UNSIGNED-PAYLOAD`, as the chaos suite's client does,
so a 1 GiB PUT is streamed rather than hashed first. Each client keeps one
connection alive, which is what an SDK's pool does per thread.
"""

from __future__ import annotations

import datetime
import hashlib
import hmac
import html
import http.client
import random
import re
import ssl
import time
import urllib.parse
from dataclasses import dataclass

UNSIGNED = "UNSIGNED-PAYLOAD"
CHUNK = 1 << 20


@dataclass(frozen=True)
class Credentials:
    access_key: str
    secret_key: str
    region: str = "us-east-1"


def sign(method: str, host: str, path: str, query: str, headers: dict[str, str],
         credentials: Credentials, payload_hash: str = UNSIGNED, service: str = "s3",
         now: datetime.datetime | None = None) -> dict[str, str]:
    """`headers` plus the SigV4 headers (host, x-amz-date,
    x-amz-content-sha256, authorization) of a request. `path` is the
    request path as it is sent, already percent-encoded; `query` is the
    canonical query string."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    signed = {name.lower(): " ".join(value.split()) for name, value in headers.items()}
    signed.update({"host": host, "x-amz-date": amz_date, "x-amz-content-sha256": payload_hash})
    names = sorted(signed)
    canonical = "\n".join([
        method, path, query,
        "".join(f"{name}:{signed[name]}\n" for name in names),
        ";".join(names), payload_hash,
    ])
    scope = f"{date}/{credentials.region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()])
    key = ("AWS4" + credentials.secret_key).encode()
    for part in (date, credentials.region, service, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    signed["authorization"] = (f"AWS4-HMAC-SHA256 Credential={credentials.access_key}/{scope}, "
                               f"SignedHeaders={';'.join(names)}, Signature={signature}")
    return signed


def object_path(bucket: str, key: str = "") -> str:
    """The percent-encoded path of a bucket or an object."""
    path = "/" + urllib.parse.quote(bucket, safe="")
    if key:
        path += "/" + urllib.parse.quote(key, safe="/-_.~")
    return path


class Payload:
    """`size` bytes cut from a random block, so that bodies cost no CPU to
    make and do not compress. The block is the same for every body of a
    seed."""

    _blocks: dict[int, bytes] = {}

    def __init__(self, size: int, seed: int = 0) -> None:
        self.size = size
        if seed not in Payload._blocks:
            Payload._blocks[seed] = random.Random(seed).randbytes(CHUNK)
        self.block = Payload._blocks[seed]

    def __iter__(self):
        left = self.size
        while left > 0:
            piece = self.block[: min(left, CHUNK)]
            left -= len(piece)
            yield piece


class Client:
    """One connection to an S3 endpoint (`http://host:port` or https)."""

    def __init__(self, endpoint: str, credentials: Credentials, ca_file: str | None = None,
                 timeout: float = 30.0) -> None:
        url = urllib.parse.urlsplit(endpoint)
        self.secure = url.scheme == "https"
        self.host = url.hostname or "127.0.0.1"
        self.port = url.port or (443 if self.secure else 80)
        self.authority = url.netloc
        self.credentials = credentials
        self.timeout = timeout
        self.context = ssl.create_default_context(cafile=ca_file) if self.secure else None
        self.connection: http.client.HTTPConnection | None = None

    def _connect(self) -> http.client.HTTPConnection:
        if self.connection is None:
            if self.secure:
                self.connection = http.client.HTTPSConnection(
                    self.host, self.port, timeout=self.timeout, context=self.context)
            else:
                self.connection = http.client.HTTPConnection(self.host, self.port,
                                                             timeout=self.timeout)
        return self.connection

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def request(self, method: str, path: str, body=None, length: int = 0,
                headers: dict[str, str] | None = None) -> tuple[int, bytes, int]:
        """One attempt: the status (0 when no answer came), the body of an
        answer that is not 2xx, and the bytes of a 2xx body, which are read
        and dropped."""
        headers = dict(headers or {})
        if method == "PUT":
            headers["content-length"] = str(length)
        signed = sign(method, self.authority, path, "", headers, self.credentials)
        connection = self._connect()
        try:
            connection.request(method, path, body=body, headers=signed)
            response = connection.getresponse()
            received = 0
            kept = b""
            while True:
                piece = response.read(CHUNK)
                if not piece:
                    break
                received += len(piece)
                if response.status >= 300 and len(kept) < 4096:
                    kept += piece
            if response.getheader("connection", "").lower() == "close":
                self.close()
            return response.status, kept, received
        except (OSError, http.client.HTTPException):
            self.close()
            return 0, b"", 0

    def put(self, bucket: str, key: str, payload: Payload) -> int:
        status, _, _ = self.request("PUT", object_path(bucket, key), payload, payload.size)
        return status

    def get(self, bucket: str, key: str) -> tuple[int, int]:
        status, _, received = self.request("GET", object_path(bucket, key))
        return status, received

    def count_keys(self, bucket: str) -> int:
        """How many keys ListObjectsV2 lists in the bucket, page by page."""
        count, token = 0, None
        while True:
            params = {"list-type": "2", "max-keys": "1000"}
            if token:
                params["continuation-token"] = token
            query = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}="
                             f"{urllib.parse.quote(v, safe='-_.~')}"
                             for k, v in sorted(params.items()))
            path = object_path(bucket)
            signed = sign("GET", self.authority, path, query, {}, self.credentials)
            connection = self._connect()
            try:
                connection.request("GET", f"{path}?{query}", headers=signed)
                response = connection.getresponse()
                body = response.read().decode()
            except (OSError, http.client.HTTPException):
                self.close()
                raise
            if response.status != 200:
                raise RuntimeError(f"ListObjectsV2 {bucket}: {response.status} {body[:300]}")
            count += len(re.findall(r"<Key>", body))
            match = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", body)
            if not match:
                return count
            token = html.unescape(match.group(1))

    def create_bucket(self, bucket: str, target: str | None = None) -> None:
        """Creates the bucket, a write_back bucket flushing to `target` if
        one is given (design section 4.1); a bucket that exists is kept."""
        headers = {}
        if target:
            headers = {"x-skys3-bucket-mode": "write_back", "x-skys3-bucket-target": target}
        for attempt in range(5):
            status, body, _ = self.request("PUT", object_path(bucket), b"", 0, headers)
            if status == 200 or b"BucketAlreadyOwnedByYou" in body:
                return
            if status and status < 500:
                break
            time.sleep(1 + attempt)
        raise RuntimeError(f"CreateBucket {bucket} at {self.authority}: {status} {body[:300]!r}")
