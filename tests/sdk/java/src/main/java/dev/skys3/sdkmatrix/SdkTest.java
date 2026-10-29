package dev.skys3.sdkmatrix;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;

/**
 * The SDK matrix's Java client: the AWS SDK for Java 2.x against SkyS3 (plan M1-25).
 *
 * <p>It runs against a matrix started by {@code cargo test -p skys3 --test sdk}, which sets the
 * environment tests/sdk/run.sh describes, and exits non-zero on the first failed check.
 */
public final class SdkTest {
  private static final String ENDPOINT = System.getenv("SKYS3_ENDPOINT");
  private static final String BUCKET = System.getenv("SKYS3_BUCKET");
  private static final int PART = 5 << 20;
  private static final List<ChecksumAlgorithm> ALGORITHMS =
      List.of(
          ChecksumAlgorithm.CRC32,
          ChecksumAlgorithm.CRC32_C,
          ChecksumAlgorithm.CRC64_NVME,
          ChecksumAlgorithm.SHA1,
          ChecksumAlgorithm.SHA256);

  private SdkTest() {}

  public static void main(String[] args) throws Exception {
    try {
      trustTestCa();
      features();
      webIdentity();
      log("passed");
      System.exit(0);
    } catch (Throwable error) {
      System.err.println("java: FAILED");
      error.printStackTrace();
      System.exit(1);
    }
  }

  private static void log(String message) {
    System.out.println("java: " + message);
  }

  private static void check(boolean ok, String message) {
    if (!ok) {
      throw new AssertionError(message);
    }
  }

  private static byte[] body(int seed, int length) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (seed + i % 251);
    }
    return data;
  }

  private static long seconds(String name) {
    return Long.parseLong(System.getenv(name));
  }

  /**
   * Makes the JVM's default trust store the test CA, before any TLS starts, so every client the
   * SDK builds, the STS client of the web-identity provider included, trusts the node.
   */
  private static void trustTestCa() throws Exception {
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    try (InputStream pem = new FileInputStream(System.getenv("SKYS3_CA_FILE"))) {
      store.setCertificateEntry(
          "skys3-test-ca", CertificateFactory.getInstance("X.509").generateCertificate(pem));
    }
    Path path = Path.of(System.getenv("SKYS3_WORK_DIR"), "truststore.p12");
    try (FileOutputStream out = new FileOutputStream(path.toFile())) {
      store.store(out, "changeit".toCharArray());
    }
    System.setProperty("javax.net.ssl.trustStore", path.toString());
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
  }

  /** Records the payload hash of every signed request, which names the aws-chunked form. */
  private static final class PayloadForms implements ExecutionInterceptor {
    final List<String> forms = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attrs) {
      context.httpRequest().firstMatchingHeader("x-amz-content-sha256").ifPresent(forms::add);
    }
  }

  private static Map<ChecksumAlgorithm, String> checksums(
      String crc32, String crc32c, String crc64, String sha1, String sha256) {
    Map<ChecksumAlgorithm, String> all = new LinkedHashMap<>();
    String[] values = {crc32, crc32c, crc64, sha1, sha256};
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        all.put(ALGORITHMS.get(i), values[i]);
      }
    }
    return all;
  }

  private static Map<ChecksumAlgorithm, String> checksums(PutObjectResponse r) {
    return checksums(
        r.checksumCRC32(),
        r.checksumCRC32C(),
        r.checksumCRC64NVME(),
        r.checksumSHA1(),
        r.checksumSHA256());
  }

  private static Map<ChecksumAlgorithm, String> checksums(GetObjectResponse r) {
    return checksums(
        r.checksumCRC32(),
        r.checksumCRC32C(),
        r.checksumCRC64NVME(),
        r.checksumSHA1(),
        r.checksumSHA256());
  }

  private static void features() throws Exception {
    AwsCredentialsProvider credentials =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                System.getenv("SKYS3_ACCESS_KEY_ID"), System.getenv("SKYS3_SECRET_ACCESS_KEY")));
    PayloadForms forms = new PayloadForms();
    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(credentials)
            .overrideConfiguration(o -> o.addExecutionInterceptor(forms))
            .build();
    s3.createBucket(r -> r.bucket(BUCKET));

    // The default checksum, CRC32, comes back on PUT and GET.
    byte[] data = body(1, 70_000);
    PutObjectResponse put = s3.putObject(r -> r.bucket(BUCKET).key("default"), RequestBody.fromBytes(data));
    check(put.checksumCRC32() != null, "no default checksum: " + put);
    ResponseBytes<GetObjectResponse> got =
        s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("default").checksumMode(ChecksumMode.ENABLED));
    check(put.checksumCRC32().equals(got.response().checksumCRC32()), "GET checksum " + got.response());
    check(Arrays.equals(got.asByteArray(), data), "GET returned other bytes");
    log("default checksum");

    for (int i = 0; i < ALGORITHMS.size(); i++) {
      ChecksumAlgorithm algorithm = ALGORITHMS.get(i);
      String key = "checksum/" + algorithm;
      byte[] bytes = body(10 + i, 3_000 + i * 1_000);
      PutObjectResponse sent =
          s3.putObject(
              r -> r.bucket(BUCKET).key(key).checksumAlgorithm(algorithm), RequestBody.fromBytes(bytes));
      check(checksums(sent).keySet().equals(Set.of(algorithm)), algorithm + ": sent " + sent);
      ResponseBytes<GetObjectResponse> stored =
          s3.getObjectAsBytes(r -> r.bucket(BUCKET).key(key).checksumMode(ChecksumMode.ENABLED));
      check(
          checksums(stored.response()).equals(checksums(sent)),
          algorithm + ": stored " + stored.response());
      check(stored.response().checksumType() == ChecksumType.FULL_OBJECT, algorithm + ": type");
      check(Arrays.equals(stored.asByteArray(), bytes), algorithm + ": GET returned other bytes");
    }
    log("checksums " + ALGORITHMS);

    // A stream of known length goes as aws-chunked with a trailing checksum.
    byte[] streamed = body(30, 300_000);
    s3.putObject(
        r -> r.bucket(BUCKET).key("streamed"),
        RequestBody.fromInputStream(new ByteArrayInputStream(streamed), streamed.length));
    check(
        Arrays.equals(s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("streamed")).asByteArray(), streamed),
        "streamed: GET returned other bytes");
    check(
        forms.forms.contains("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        "no aws-chunked upload: " + forms.forms);
    log("aws-chunked upload");

    // The asynchronous client's multipart support splits a large upload.
    byte[] large = body(40, 2 * PART + 1_000_000);
    try (S3AsyncClient async =
        S3AsyncClient.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(credentials)
            .multipartEnabled(true)
            .multipartConfiguration(c -> c.thresholdInBytes((long) PART).minimumPartSizeInBytes((long) PART))
            .build()) {
      async.putObject(r -> r.bucket(BUCKET).key("multipart"), AsyncRequestBody.fromBytes(large)).join();
    }
    HeadObjectResponse head =
        s3.headObject(r -> r.bucket(BUCKET).key("multipart").checksumMode(ChecksumMode.ENABLED));
    check(head.eTag().endsWith("-3\""), "ETag " + head.eTag());
    check(
        Arrays.equals(s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("multipart")).asByteArray(), large),
        "multipart: GET returned other bytes");
    ResponseBytes<GetObjectResponse> second =
        s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("multipart").partNumber(2));
    check(second.response().partsCount() == 3, "parts " + second.response().partsCount());
    check(
        Arrays.equals(second.asByteArray(), Arrays.copyOfRange(large, PART, 2 * PART)),
        "part 2: other bytes");
    log("multipart upload (" + head.eTag() + ", checksum type " + head.checksumType() + ")");

    // Presigned URLs, used without the SDK.
    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    try (S3Presigner presigner =
        S3Presigner.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(credentials)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build()) {
      var get =
          presigner.presignGetObject(
              r -> r.signatureDuration(Duration.ofMinutes(5)).getObjectRequest(g -> g.bucket(BUCKET).key("default")));
      HttpResponse<byte[]> answer =
          http.send(
              HttpRequest.newBuilder(get.url().toURI()).GET().build(),
              HttpResponse.BodyHandlers.ofByteArray());
      check(answer.statusCode() == 200, "presigned GET: " + answer.statusCode());
      check(Arrays.equals(answer.body(), body(1, 70_000)), "presigned GET: other bytes");
      byte[] presigned = body(50, 20_000);
      var putUrl =
          presigner.presignPutObject(
              r -> r.signatureDuration(Duration.ofMinutes(5)).putObjectRequest(p -> p.bucket(BUCKET).key("presigned")));
      HttpRequest.Builder request =
          HttpRequest.newBuilder(putUrl.url().toURI()).PUT(HttpRequest.BodyPublishers.ofByteArray(presigned));
      putUrl.signedHeaders().forEach(
          (name, values) -> {
            if (!name.equalsIgnoreCase("host")) {
              values.forEach(value -> request.header(name, value));
            }
          });
      answer = http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      check(answer.statusCode() == 200, "presigned PUT: " + answer.statusCode() + " " + new String(answer.body()));
      check(
          Arrays.equals(s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("presigned")).asByteArray(), presigned),
          "presigned PUT stored other bytes");
    }
    log("presigned GET and PUT");

    var listed = s3.listObjectsV2(r -> r.bucket(BUCKET).prefix("checksum/").delimiter("/"));
    check(listed.contents().size() == ALGORITHMS.size(), "listed " + listed.contents().size());
    s3.copyObject(r -> r.sourceBucket(BUCKET).sourceKey("default").destinationBucket(BUCKET).destinationKey("copy"));
    s3.putObjectTagging(
        r -> r.bucket(BUCKET).key("copy").tagging(t -> t.tagSet(Tag.builder().key("sdk").value("java").build())));
    var tags = s3.getObjectTagging(r -> r.bucket(BUCKET).key("copy")).tagSet();
    check(tags.size() == 1 && tags.get(0).value().equals("java"), "tags " + tags);
    byte[] ranged = s3.getObjectAsBytes(r -> r.bucket(BUCKET).key("copy").range("bytes=10-19")).asByteArray();
    check(Arrays.equals(ranged, Arrays.copyOfRange(body(1, 70_000), 10, 20)), "range: other bytes");
    List<ObjectIdentifier> keys = new ArrayList<>();
    s3.listObjectsV2(r -> r.bucket(BUCKET)).contents().forEach(o -> keys.add(ObjectIdentifier.builder().key(o.key()).build()));
    var deleted = s3.deleteObjects(r -> r.bucket(BUCKET).delete(d -> d.objects(keys)));
    check(deleted.errors().isEmpty() && deleted.deleted().size() == keys.size(), "deleted " + deleted);
    s3.deleteBucket(r -> r.bucket(BUCKET));
    log("listing, copy, tagging, ranges, and batch delete");
  }

  /**
   * The default chain, configured by the environment alone; then the web-identity provider with a
   * cache that refreshes a session SKYS3_REFRESH_SECONDS after its issue, under load.
   */
  private static void webIdentity() throws Exception {
    String wiBucket = BUCKET + "-web-identity";
    try (S3Client chain = S3Client.builder().forcePathStyle(true).build()) {
      chain.createBucket(r -> r.bucket(wiBucket));
      try {
        chain.createBucket(r -> r.bucket("other-" + BUCKET));
        throw new AssertionError("the role may not create other buckets");
      } catch (S3Exception error) {
        check("AccessDenied".equals(error.awsErrorDetails().errorCode()), "other bucket: " + error);
      }
    }

    long session = seconds("SKYS3_SESSION_SECONDS");
    long refresh = seconds("SKYS3_REFRESH_SECONDS");
    try (StsWebIdentityTokenFileCredentialsProvider provider =
            StsWebIdentityTokenFileCredentialsProvider.builder()
                .stsClient(software.amazon.awssdk.services.sts.StsClient.create())
                .asyncCredentialUpdateEnabled(true)
                .prefetchTime(Duration.ofSeconds(session - refresh))
                .staleTime(Duration.ofSeconds(session - 2 * refresh))
                .build();
        S3Client s3 = S3Client.builder().forcePathStyle(true).credentialsProvider(provider).build()) {
      String first = provider.resolveCredentials().accessKeyId();
      check(first.startsWith("ASIA"), "access key " + first);
      Set<String> keys = Collections.synchronizedSet(new HashSet<>(Set.of(first)));
      AtomicInteger count = new AtomicInteger();
      long deadline = System.nanoTime() + seconds("SKYS3_LOAD_SECONDS") * 1_000_000_000L;
      ExecutorService pool = Executors.newFixedThreadPool(8);
      List<Future<?>> workers = new ArrayList<>();
      for (int n = 0; n < 8; n++) {
        int worker = n;
        workers.add(
            pool.submit(
                () -> {
                  for (int i = 0; System.nanoTime() < deadline; i++) {
                    String key = "load/" + worker + "/" + (i % 4);
                    byte[] data = body(worker, 1_000 + i % 7 * 100);
                    s3.putObject(r -> r.bucket(wiBucket).key(key), RequestBody.fromBytes(data));
                    byte[] read = s3.getObjectAsBytes(r -> r.bucket(wiBucket).key(key)).asByteArray();
                    check(Arrays.equals(read, data), key + ": GET returned other bytes");
                    keys.add(provider.resolveCredentials().accessKeyId());
                    count.addAndGet(2);
                  }
                  return null;
                }));
      }
      for (Future<?> worker : workers) {
        worker.get();
      }
      pool.shutdown();
      check(keys.size() >= 2, "the session was never refreshed: " + keys);
      log("web identity: " + count.get() + " requests under load with " + keys.size() + " sessions");
    }
  }
}
