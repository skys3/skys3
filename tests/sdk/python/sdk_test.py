"""The SDK matrix's Python client: boto3 against SkyS3 (plan M1-25).

Runs against a matrix started by `cargo test -p skys3 --test sdk`, which
sets the environment tests/sdk/run.sh describes. Exits non-zero on the
first failed check.
"""

import base64
import io
import os
import sys
import threading
import time
import zlib

import boto3
import requests
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ["SKYS3_ENDPOINT"]
CA = os.environ["SKYS3_CA_FILE"]
BUCKET = os.environ["SKYS3_BUCKET"]
LOAD_SECONDS = int(os.environ["SKYS3_LOAD_SECONDS"])
PART = 5 << 20
ALGORITHMS = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"]
# Path-style requests (design section 11); presigned URLs need SigV4, which
# botocore does not use for them unless told to.
CONFIG = Config(
    signature_version="s3v4",
    s3={"addressing_style": "path"},
    retries={"mode": "standard", "max_attempts": 2},
)


def body(seed, length):
    return bytes((seed + i % 251) % 256 for i in range(length))


def log(message):
    print(f"python: {message}", flush=True)


def static_client():
    """A client with the static credential and an explicit endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        verify=CA,
        aws_access_key_id=os.environ["SKYS3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["SKYS3_SECRET_ACCESS_KEY"],
        config=CONFIG,
    )


def checksums(answer):
    return {
        name[len("Checksum") :]: value
        for name, value in answer.items()
        if name.startswith("Checksum") and name != "ChecksumType"
    }


def features():
    s3 = static_client()
    forms = []
    s3.meta.events.register(
        "before-send.s3.*",
        lambda request, **_: forms.append(request.headers.get("x-amz-content-sha256")),
    )
    s3.create_bucket(Bucket=BUCKET)

    # The default checksum, CRC32, sent in an aws-chunked trailer over
    # HTTPS, and returned on GET.
    data = body(1, 70_000)
    put = s3.put_object(Bucket=BUCKET, Key="default", Body=data)
    expected = base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()
    assert put.get("ChecksumCRC32") == expected, put
    got = s3.get_object(Bucket=BUCKET, Key="default", ChecksumMode="ENABLED")
    assert got["ChecksumCRC32"] == expected, got
    assert got["Body"].read() == data
    assert b"STREAMING-UNSIGNED-PAYLOAD-TRAILER" in forms, forms
    log("default checksum and aws-chunked upload")

    for i, algorithm in enumerate(ALGORITHMS):
        key = f"checksum/{algorithm}"
        data = body(10 + i, 3_000 + i * 1_000)
        put = s3.put_object(Bucket=BUCKET, Key=key, Body=data, ChecksumAlgorithm=algorithm)
        assert list(checksums(put)) == [algorithm], put
        got = s3.get_object(Bucket=BUCKET, Key=key, ChecksumMode="ENABLED")
        assert checksums(got) == checksums(put), (algorithm, got)
        assert got["ChecksumType"] == "FULL_OBJECT"
        assert got["Body"].read() == data
    log(f"checksums {', '.join(ALGORITHMS)}")

    # A non-seekable stream of known length.
    data = body(30, 300_000)
    s3.put_object(Bucket=BUCKET, Key="streamed", Body=Stream(data), ContentLength=len(data))
    assert s3.get_object(Bucket=BUCKET, Key="streamed")["Body"].read() == data
    log("streamed upload")

    # The transfer manager splits a large upload into parts.
    data = body(40, 2 * PART + 1_000_000)
    transfer = TransferConfig(multipart_threshold=PART, multipart_chunksize=PART, max_concurrency=3)
    s3.upload_fileobj(io.BytesIO(data), BUCKET, "multipart", Config=transfer)
    head = s3.head_object(Bucket=BUCKET, Key="multipart", ChecksumMode="ENABLED")
    assert head["ETag"].endswith('-3"'), head
    assert head["ContentLength"] == len(data)
    out = io.BytesIO()
    s3.download_fileobj(BUCKET, "multipart", out, Config=transfer)
    assert out.getvalue() == data
    part = s3.get_object(Bucket=BUCKET, Key="multipart", PartNumber=2)
    assert part["PartsCount"] == 3 and part["Body"].read() == data[PART : 2 * PART]
    log(f"multipart upload ({head['ETag']}, checksum type {head.get('ChecksumType')})")

    # The transfer manager copies a large object part by part with
    # UploadPartCopy, each part a range of the source under If-Match with
    # the source's ETag. The parts have the source's boundaries, so their
    # MD5s, and the copy's multipart ETag, are the source's, as in S3.
    copies = []
    s3.meta.events.register(
        "before-call.s3.UploadPartCopy",
        lambda params, **_: copies.append(params["headers"]),
    )
    s3.copy({"Bucket": BUCKET, "Key": "multipart"}, BUCKET, "multipart-copy", Config=transfer)
    assert len(copies) == 3, copies
    for headers in copies:
        assert "x-amz-copy-source-range" in headers, headers
        assert headers.get("x-amz-copy-source-if-match") == head["ETag"], headers
    copied = s3.head_object(Bucket=BUCKET, Key="multipart-copy")
    assert copied["ETag"] == head["ETag"], (copied, head)
    out = io.BytesIO()
    s3.download_fileobj(BUCKET, "multipart-copy", out, Config=transfer)
    assert out.getvalue() == data
    # A failed source condition answers 412.
    upload = s3.create_multipart_upload(Bucket=BUCKET, Key="refused-copy")
    try:
        s3.upload_part_copy(
            Bucket=BUCKET,
            Key="refused-copy",
            UploadId=upload["UploadId"],
            PartNumber=1,
            CopySource={"Bucket": BUCKET, "Key": "multipart"},
            CopySourceIfMatch='"0123"',
        )
        raise AssertionError("a failed copy-source condition was copied")
    except ClientError as error:
        assert error.response["Error"]["Code"] == "PreconditionFailed", error
    s3.abort_multipart_upload(Bucket=BUCKET, Key="refused-copy", UploadId=upload["UploadId"])
    log(f"multipart copy by UploadPartCopy ({copied['ETag']})")

    # Presigned URLs, used without the SDK.
    url = s3.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": "default"}, ExpiresIn=300)
    answer = requests.get(url, verify=CA, timeout=30)
    assert answer.status_code == 200 and answer.content == body(1, 70_000), answer.text
    url = s3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": "presigned"}, ExpiresIn=300)
    data = body(50, 20_000)
    answer = requests.put(url, data=data, verify=CA, timeout=30)
    assert answer.status_code == 200, answer.text
    assert s3.get_object(Bucket=BUCKET, Key="presigned")["Body"].read() == data
    log("presigned GET and PUT")

    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix="checksum/", Delimiter="/")
    assert len(listed["Contents"]) == len(ALGORITHMS), listed
    s3.copy_object(Bucket=BUCKET, Key="copy", CopySource={"Bucket": BUCKET, "Key": "default"})
    s3.put_object_tagging(Bucket=BUCKET, Key="copy", Tagging={"TagSet": [{"Key": "sdk", "Value": "python"}]})
    assert s3.get_object_tagging(Bucket=BUCKET, Key="copy")["TagSet"] == [{"Key": "sdk", "Value": "python"}]
    ranged = s3.get_object(Bucket=BUCKET, Key="copy", Range="bytes=10-19")
    assert ranged["Body"].read() == body(1, 70_000)[10:20]
    keys = [{"Key": o["Key"]} for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    deleted = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
    assert not deleted.get("Errors") and len(deleted["Deleted"]) == len(keys), deleted
    s3.delete_bucket(Bucket=BUCKET)
    log("listing, copy, tagging, ranges, and batch delete")


class Stream(io.RawIOBase):
    """A body that cannot seek, as from a pipe."""

    def __init__(self, data):
        self._data = io.BytesIO(data)

    def readable(self):
        return True

    def readinto(self, buffer):
        chunk = self._data.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)


def web_identity():
    """The default chain, configured by the environment alone.

    botocore refreshes a session within 15 minutes of its expiry, so with
    900-second sessions every request finds its session due and refreshes
    it, one refresh at a time while other requests go on.
    """
    session = boto3.Session()
    s3 = session.client("s3", config=CONFIG)
    bucket = f"{BUCKET}-web-identity"
    s3.create_bucket(Bucket=bucket)
    try:
        s3.create_bucket(Bucket=f"other-{BUCKET}")
        raise AssertionError("the role may not create other buckets")
    except ClientError as error:
        assert error.response["Error"]["Code"] == "AccessDenied", error
    credentials = session.get_credentials()
    first = credentials.get_frozen_credentials().access_key
    assert first.startswith("ASIA"), first

    keys = {first}
    lock = threading.Lock()
    failures = []
    count = [0]
    deadline = time.monotonic() + LOAD_SECONDS

    def worker(n):
        try:
            i = 0
            while time.monotonic() < deadline:
                key = f"load/{n}/{i % 4}"
                data = body(n, 1_000 + i % 7 * 100)
                s3.put_object(Bucket=bucket, Key=key, Body=data)
                assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == data
                access_key = credentials.get_frozen_credentials().access_key
                with lock:
                    keys.add(access_key)
                    count[0] += 2
                i += 1
        except Exception as error:  # reported below
            failures.append(repr(error))

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not failures, failures
    assert len(keys) >= 2, f"the session was never refreshed: {keys}"
    log(f"web identity: {count[0]} requests under load with {len(keys)} sessions")


def main():
    log(f"boto3 {boto3.__version__}")
    features()
    web_identity()
    log("passed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)
