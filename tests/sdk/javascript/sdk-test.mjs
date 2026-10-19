// The SDK matrix's JavaScript client: the AWS SDK for JavaScript v3
// against SkyS3 (plan M1-25).
//
// It runs against a matrix started by `cargo test -p skys3 --test sdk`,
// which sets the environment tests/sdk/run.sh describes (including
// NODE_EXTRA_CA_CERTS, so Node trusts the node's certificate), and exits
// non-zero on the first failed check.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import https from "node:https";
import { Readable } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";

import {
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectTaggingCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  PutObjectTaggingCommand,
  S3Client,
  UploadPartCopyCommand,
} from "@aws-sdk/client-s3";
import { fromTokenFile } from "@aws-sdk/credential-providers";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { memoize } from "@smithy/property-provider";

const endpoint = process.env.SKYS3_ENDPOINT;
const bucket = process.env.SKYS3_BUCKET;
const PART = 5 << 20;
const ALGORITHMS = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"];

const body = (seed, length) => {
  const data = Buffer.alloc(length);
  for (let i = 0; i < length; i++) data[i] = (seed + (i % 251)) % 256;
  return data;
};
const log = (message) => console.log(`javascript: ${message}`);
const bytes = async (answer) => Buffer.from(await answer.Body.transformToByteArray());
const checksums = (answer) =>
  Object.fromEntries(
    Object.entries(answer).filter(
      ([name, value]) => name.startsWith("Checksum") && name !== "ChecksumType" && value !== undefined,
    ),
  );

/** Sends a request to a presigned URL, without the SDK. */
function send(method, url, headers, data) {
  return new Promise((resolve, reject) => {
    const request = https.request(url, { method, headers, timeout: 30_000 }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => resolve({ status: response.statusCode, body: Buffer.concat(chunks) }));
    });
    request.on("error", reject);
    request.on("timeout", () => request.destroy(new Error(`${method} ${url} timed out`)));
    request.end(data);
  });
}

async function features() {
  const s3 = new S3Client({
    endpoint,
    region: "us-east-1",
    forcePathStyle: true,
    credentials: {
      accessKeyId: process.env.SKYS3_ACCESS_KEY_ID,
      secretAccessKey: process.env.SKYS3_SECRET_ACCESS_KEY,
    },
  });
  // The payload hash of every signed request names the aws-chunked form
  // when it is one.
  const forms = [];
  s3.middlewareStack.add(
    (next) => async (args) => {
      forms.push(args.request.headers["x-amz-content-sha256"]);
      return next(args);
    },
    { step: "deserialize", name: "recordPayloadForm" },
  );
  await s3.send(new CreateBucketCommand({ Bucket: bucket }));

  // The default checksum, CRC32, comes back on PUT and GET.
  let data = body(1, 70_000);
  const put = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: "default", Body: data }));
  assert.ok(put.ChecksumCRC32, "no default checksum");
  let got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "default", ChecksumMode: "ENABLED" }));
  assert.equal(got.ChecksumCRC32, put.ChecksumCRC32);
  assert.deepEqual(await bytes(got), data);
  log("default checksum");

  for (const [i, algorithm] of ALGORITHMS.entries()) {
    const key = `checksum/${algorithm}`;
    const data = body(10 + i, 3_000 + i * 1_000);
    const put = await s3.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: data, ChecksumAlgorithm: algorithm }),
    );
    const sent = checksums(put);
    assert.deepEqual(Object.keys(sent), [`Checksum${algorithm}`], algorithm);
    const got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key, ChecksumMode: "ENABLED" }));
    assert.deepEqual(checksums(got), sent, algorithm);
    assert.equal(got.ChecksumType, "FULL_OBJECT");
    assert.deepEqual(await bytes(got), data, algorithm);
  }
  log(`checksums ${ALGORITHMS.join(", ")}`);

  // A stream of known length goes as aws-chunked with a trailing checksum.
  data = body(30, 300_000);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: "streamed",
      Body: Readable.from([data.subarray(0, 100_000), data.subarray(100_000)]),
      ContentLength: data.length,
    }),
  );
  got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "streamed" }));
  assert.deepEqual(await bytes(got), data);
  assert.ok(forms.includes("STREAMING-UNSIGNED-PAYLOAD-TRAILER"), `no aws-chunked upload: ${forms}`);
  log("aws-chunked upload");

  // lib-storage splits a large upload into parts.
  data = body(40, 2 * PART + 1_000_000);
  await new Upload({
    client: s3,
    params: { Bucket: bucket, Key: "multipart", Body: data },
    partSize: PART,
    queueSize: 3,
  }).done();
  const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: "multipart", ChecksumMode: "ENABLED" }));
  assert.ok(head.ETag.endsWith('-3"'), head.ETag);
  got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "multipart" }));
  assert.deepEqual(await bytes(got), data);
  got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "multipart", PartNumber: 2 }));
  assert.equal(got.PartsCount, 3);
  assert.deepEqual(await bytes(got), data.subarray(PART, 2 * PART));
  log(`multipart upload (${head.ETag}, checksum type ${head.ChecksumType})`);
  await multipartCopy(s3, head.ETag, data);

  // Presigned URLs, used without the SDK.
  const getUrl = await getSignedUrl(s3, new GetObjectCommand({ Bucket: bucket, Key: "default" }), {
    expiresIn: 300,
  });
  let answer = await send("GET", getUrl, {});
  assert.equal(answer.status, 200, answer.body.toString());
  assert.deepEqual(answer.body, body(1, 70_000));
  data = body(50, 20_000);
  const putUrl = await getSignedUrl(s3, new PutObjectCommand({ Bucket: bucket, Key: "presigned" }), {
    expiresIn: 300,
  });
  answer = await send("PUT", putUrl, { "content-length": data.length }, data);
  assert.equal(answer.status, 200, answer.body.toString());
  got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "presigned" }));
  assert.deepEqual(await bytes(got), data);
  log("presigned GET and PUT");

  const listed = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: "checksum/", Delimiter: "/" }));
  assert.equal(listed.Contents.length, ALGORITHMS.length);
  await s3.send(new CopyObjectCommand({ Bucket: bucket, Key: "copy", CopySource: `${bucket}/default` }));
  await s3.send(
    new PutObjectTaggingCommand({
      Bucket: bucket,
      Key: "copy",
      Tagging: { TagSet: [{ Key: "sdk", Value: "javascript" }] },
    }),
  );
  const tags = await s3.send(new GetObjectTaggingCommand({ Bucket: bucket, Key: "copy" }));
  assert.deepEqual(tags.TagSet, [{ Key: "sdk", Value: "javascript" }]);
  got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "copy", Range: "bytes=10-19" }));
  assert.deepEqual(await bytes(got), body(1, 70_000).subarray(10, 20));
  const all = await s3.send(new ListObjectsV2Command({ Bucket: bucket }));
  const objects = all.Contents.map(({ Key }) => ({ Key }));
  const deleted = await s3.send(new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: objects } }));
  assert.equal(deleted.Errors, undefined);
  assert.equal(deleted.Deleted.length, objects.length);
  await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
  log("listing, copy, tagging, ranges, and batch delete");
}

/**
 * Copies the object "multipart", which holds `data` and has the ETag
 * `etag`, part by part with UploadPartCopy, as the AWS documentation's
 * multipart copy does: lib-storage has no copy helper. Each part is a range
 * of the source under If-Match, at the source's own part boundaries, so the
 * copy's multipart ETag is the source's, as in S3.
 */
async function multipartCopy(s3, etag, data) {
  const target = { Bucket: bucket, Key: "multipart-copy" };
  const CopySource = `${bucket}/multipart`;
  const { UploadId } = await s3.send(new CreateMultipartUploadCommand(target));
  await assert.rejects(
    s3.send(
      new UploadPartCopyCommand({ ...target, UploadId, PartNumber: 1, CopySource, CopySourceIfNoneMatch: etag }),
    ),
    { name: "PreconditionFailed" },
  );
  const Parts = [];
  for (let first = 0; first < data.length; first += PART) {
    const last = Math.min(first + PART, data.length) - 1;
    const PartNumber = first / PART + 1;
    const { CopyPartResult } = await s3.send(
      new UploadPartCopyCommand({
        ...target,
        UploadId,
        PartNumber,
        CopySource,
        CopySourceRange: `bytes=${first}-${last}`,
        CopySourceIfMatch: etag,
      }),
    );
    const md5 = createHash("md5")
      .update(data.subarray(first, last + 1))
      .digest("hex");
    assert.equal(CopyPartResult.ETag, `"${md5}"`, `part ${PartNumber}`);
    Parts.push({ PartNumber, ETag: CopyPartResult.ETag });
  }
  const done = await s3.send(new CompleteMultipartUploadCommand({ ...target, UploadId, MultipartUpload: { Parts } }));
  assert.equal(done.ETag, etag);
  const got = await s3.send(new GetObjectCommand(target));
  assert.deepEqual(await bytes(got), data);
  log(`multipart copy by UploadPartCopy (${etag})`);
}

/**
 * The default chain, configured by the environment alone, then the
 * chain's web-identity provider under a cache that refreshes a session
 * SKYS3_REFRESH_SECONDS after its issue, under load. The default chain's
 * own cache refreshes five minutes before expiry and cannot be told
 * otherwise. Every refresh must reread the token file, whose tokens expire
 * within seconds.
 */
async function webIdentity() {
  const wiBucket = `${bucket}-web-identity`;
  const chain = new S3Client({ forcePathStyle: true });
  await chain.send(new CreateBucketCommand({ Bucket: wiBucket }));
  await assert.rejects(chain.send(new CreateBucketCommand({ Bucket: `other-${bucket}` })), {
    name: "AccessDenied",
  });
  const first = await chain.config.credentials();
  assert.ok(first.accessKeyId.startsWith("ASIA"), first.accessKeyId);

  const window = (Number(process.env.SKYS3_SESSION_SECONDS) - Number(process.env.SKYS3_REFRESH_SECONDS)) * 1000;
  const credentials = memoize(
    fromTokenFile(),
    (identity) => identity.expiration.getTime() - Date.now() < window,
    (identity) => identity.expiration !== undefined,
  );
  const s3 = new S3Client({ forcePathStyle: true, credentials });
  // The keys this provider gives: a refresh is a second one from it.
  const keys = new Set([(await credentials()).accessKeyId]);
  let count = 0;
  const deadline = Date.now() + Number(process.env.SKYS3_LOAD_SECONDS) * 1000;
  const worker = async (n) => {
    for (let i = 0; Date.now() < deadline; i++) {
      const key = `load/${n}/${i % 4}`;
      const data = body(n, 1_000 + (i % 7) * 100);
      await s3.send(new PutObjectCommand({ Bucket: wiBucket, Key: key, Body: data }));
      const got = await s3.send(new GetObjectCommand({ Bucket: wiBucket, Key: key }));
      assert.deepEqual(await bytes(got), data, key);
      keys.add((await credentials()).accessKeyId);
      count += 2;
    }
  };
  await Promise.all(Array.from({ length: 8 }, (_, n) => worker(n)));
  assert.ok(keys.size >= 2, `the session was never refreshed: ${[...keys]}`);
  log(`web identity: ${count} requests under load with ${keys.size} sessions`);
}

try {
  await features();
  await webIdentity();
  log("passed");
} catch (error) {
  console.error("javascript: FAILED:", error);
  await sleep(0);
  process.exit(1);
}
