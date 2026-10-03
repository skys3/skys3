// The SDK matrix's Go client: the AWS SDK for Go v2 against SkyS3 (plan
// M1-25).
//
// It runs against a matrix started by `cargo test -p skys3 --test sdk`,
// which sets the environment tests/sdk/run.sh describes, and exits
// non-zero on the first failed check.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const part = 5 << 20

var (
	endpoint = os.Getenv("SKYS3_ENDPOINT")
	bucket   = os.Getenv("SKYS3_BUCKET")
	algos    = []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32,
		types.ChecksumAlgorithmCrc32c,
		types.ChecksumAlgorithmCrc64nvme,
		types.ChecksumAlgorithmSha1,
		types.ChecksumAlgorithmSha256,
	}
)

func body(seed, length int) []byte {
	data := make([]byte, length)
	for i := range data {
		data[i] = byte(seed + i%251)
	}
	return data
}

func logf(format string, args ...any) {
	fmt.Printf("go: "+format+"\n", args...)
}

func check(err error, what string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %v", what, err))
	}
}

func require(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}

func envSeconds(name string) time.Duration {
	n, err := strconv.Atoi(os.Getenv(name))
	check(err, name)
	return time.Duration(n) * time.Second
}

// payloadForms records the x-amz-content-sha256 of every signed request,
// which names the aws-chunked form when it is one.
type payloadForms struct {
	sync.Mutex
	forms []string
}

func (p *payloadForms) record(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("RecordPayloadForm",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				p.Lock()
				p.forms = append(p.forms, req.Header.Get("X-Amz-Content-Sha256"))
				p.Unlock()
			}
			return next.HandleFinalize(ctx, in)
		}), middleware.After)
}

func (p *payloadForms) has(form string) bool {
	p.Lock()
	defer p.Unlock()
	for _, f := range p.forms {
		if f == form {
			return true
		}
	}
	return false
}

// readAll reads and closes an object body.
func readAll(r io.ReadCloser) []byte {
	defer r.Close()
	data, err := io.ReadAll(r)
	check(err, "reading a body")
	return data
}

func checksums(crc32, crc32c, crc64, sha1, sha256 *string) map[types.ChecksumAlgorithm]string {
	all := map[types.ChecksumAlgorithm]*string{
		types.ChecksumAlgorithmCrc32:     crc32,
		types.ChecksumAlgorithmCrc32c:    crc32c,
		types.ChecksumAlgorithmCrc64nvme: crc64,
		types.ChecksumAlgorithmSha1:      sha1,
		types.ChecksumAlgorithmSha256:    sha256,
	}
	found := map[types.ChecksumAlgorithm]string{}
	for algorithm, value := range all {
		if value != nil {
			found[algorithm] = *value
		}
	}
	return found
}

// onlyReader hides every method but Read, so the SDK cannot seek the body.
type onlyReader struct{ io.Reader }

func features(ctx context.Context, httpClient *http.Client) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("SKYS3_ACCESS_KEY_ID"), os.Getenv("SKYS3_SECRET_ACCESS_KEY"), "")))
	check(err, "loading the configuration")
	forms := &payloadForms{}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.APIOptions = append(o.APIOptions, forms.record)
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
	check(err, "CreateBucket")

	// The default checksum, CRC32, comes back on PUT and GET.
	data := body(1, 70_000)
	put, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: aws.String("default"), Body: bytes.NewReader(data)})
	check(err, "PutObject")
	require(put.ChecksumCRC32 != nil, "no default checksum: %+v", put)
	got, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket, Key: aws.String("default"), ChecksumMode: types.ChecksumModeEnabled})
	check(err, "GetObject")
	require(aws.ToString(got.ChecksumCRC32) == *put.ChecksumCRC32, "GET checksum %v", got.ChecksumCRC32)
	require(bytes.Equal(readAll(got.Body), data), "GET returned other bytes")
	logf("default checksum")

	for i, algorithm := range algos {
		key := "checksum/" + string(algorithm)
		data := body(10+i, 3_000+i*1_000)
		put, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key,
			Body: bytes.NewReader(data), ChecksumAlgorithm: algorithm})
		check(err, "PutObject with "+string(algorithm))
		sent := checksums(put.ChecksumCRC32, put.ChecksumCRC32C, put.ChecksumCRC64NVME, put.ChecksumSHA1, put.ChecksumSHA256)
		_, ok := sent[algorithm]
		require(len(sent) == 1 && ok, "%s: sent %v", algorithm, sent)
		got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key,
			ChecksumMode: types.ChecksumModeEnabled})
		check(err, "GetObject")
		stored := checksums(got.ChecksumCRC32, got.ChecksumCRC32C, got.ChecksumCRC64NVME, got.ChecksumSHA1, got.ChecksumSHA256)
		require(fmt.Sprint(stored) == fmt.Sprint(sent), "%s: stored %v, sent %v", algorithm, stored, sent)
		require(got.ChecksumType == types.ChecksumTypeFullObject, "%s: type %s", algorithm, got.ChecksumType)
		require(bytes.Equal(readAll(got.Body), data), "%s: GET returned other bytes", algorithm)
	}
	logf("checksums %v", algos)

	// A body that cannot seek goes as aws-chunked with a trailing checksum.
	data = body(30, 300_000)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: aws.String("streamed"),
		Body: onlyReader{bytes.NewReader(data)}, ContentLength: aws.Int64(int64(len(data)))})
	check(err, "PutObject of a stream")
	got, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String("streamed")})
	check(err, "GetObject")
	require(bytes.Equal(readAll(got.Body), data), "streamed: GET returned other bytes")
	require(forms.has("STREAMING-UNSIGNED-PAYLOAD-TRAILER"), "no aws-chunked upload: %v", forms.forms)
	logf("aws-chunked upload")

	// The upload manager splits a large upload into parts.
	data = body(40, 2*part+1_000_000)
	uploader := manager.NewUploader(client, func(u *manager.Uploader) { u.PartSize = part })
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: aws.String("multipart"),
		Body: bytes.NewReader(data)})
	check(err, "multipart upload")
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: aws.String("multipart"),
		ChecksumMode: types.ChecksumModeEnabled})
	check(err, "HeadObject")
	require(strings.HasSuffix(aws.ToString(head.ETag), `-3"`), "ETag %s", aws.ToString(head.ETag))
	buffer := manager.NewWriteAtBuffer(nil)
	_, err = manager.NewDownloader(client, func(d *manager.Downloader) { d.PartSize = part }).
		Download(ctx, buffer, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String("multipart")})
	check(err, "multipart download")
	require(bytes.Equal(buffer.Bytes(), data), "multipart: GET returned other bytes")
	logf("multipart upload (%s, checksum type %s)", aws.ToString(head.ETag), head.ChecksumType)

	// Presigned URLs, used without the SDK.
	presign := s3.NewPresignClient(client)
	get, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String("default")},
		s3.WithPresignExpires(5*time.Minute))
	check(err, "presigning a GET")
	status, answer := send(httpClient, get.Method, get.URL, get.SignedHeader, nil)
	require(status == 200 && bytes.Equal(answer, body(1, 70_000)), "presigned GET: %d %s", status, answer)
	data = body(50, 20_000)
	putURL, err := presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: aws.String("presigned")},
		s3.WithPresignExpires(5*time.Minute))
	check(err, "presigning a PUT")
	status, answer = send(httpClient, putURL.Method, putURL.URL, putURL.SignedHeader, data)
	require(status == 200, "presigned PUT: %d %s", status, answer)
	got, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String("presigned")})
	check(err, "GetObject")
	require(bytes.Equal(readAll(got.Body), data), "presigned PUT stored other bytes")
	logf("presigned GET and PUT")

	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket,
		Prefix: aws.String("checksum/"), Delimiter: aws.String("/")})
	check(err, "ListObjectsV2")
	require(len(listed.Contents) == len(algos), "listed %d objects", len(listed.Contents))
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &bucket, Key: aws.String("copy"),
		CopySource: aws.String(bucket + "/default")})
	check(err, "CopyObject")
	_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: &bucket, Key: aws.String("copy"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("sdk"), Value: aws.String("go")}}}})
	check(err, "PutObjectTagging")
	tags, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: &bucket, Key: aws.String("copy")})
	check(err, "GetObjectTagging")
	require(len(tags.TagSet) == 1 && aws.ToString(tags.TagSet[0].Value) == "go", "tags %v", tags.TagSet)
	ranged, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String("copy"),
		Range: aws.String("bytes=10-19")})
	check(err, "ranged GetObject")
	require(bytes.Equal(readAll(ranged.Body), body(1, 70_000)[10:20]), "range returned other bytes")
	all, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	check(err, "ListObjectsV2")
	var objects []types.ObjectIdentifier
	for _, object := range all.Contents {
		objects = append(objects, types.ObjectIdentifier{Key: object.Key})
	}
	deleted, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &bucket,
		Delete: &types.Delete{Objects: objects}})
	check(err, "DeleteObjects")
	require(len(deleted.Errors) == 0 && len(deleted.Deleted) == len(objects), "deleted %+v", deleted)
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket})
	check(err, "DeleteBucket")
	logf("listing, copy, tagging, ranges, and batch delete")
}

// send sends a request to a presigned URL with its signed headers.
func send(client *http.Client, method, url string, headers http.Header, data []byte) (int, []byte) {
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	check(err, "building a request")
	for name, values := range headers {
		if !strings.EqualFold(name, "host") {
			req.Header[name] = values
		}
	}
	req.ContentLength = int64(len(data))
	resp, err := client.Do(req)
	check(err, method+" "+url)
	return resp.StatusCode, readAll(resp.Body)
}

// webIdentity uses the default chain, configured by the environment
// alone, with a cache that refreshes a session SKYS3_REFRESH_SECONDS after
// its issue, under load.
func webIdentity(ctx context.Context) {
	window := envSeconds("SKYS3_SESSION_SECONDS") - envSeconds("SKYS3_REFRESH_SECONDS")
	cfg, err := config.LoadDefaultConfig(ctx, config.WithCredentialsCacheOptions(
		func(o *aws.CredentialsCacheOptions) { o.ExpiryWindow = window }))
	check(err, "loading the default configuration")
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	wiBucket := bucket + "-web-identity"
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &wiBucket})
	check(err, "CreateBucket with the web identity")
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("other-" + bucket)})
	var apiErr smithy.APIError
	require(errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied", "other bucket: %v", err)
	first, err := cfg.Credentials.Retrieve(ctx)
	check(err, "retrieving credentials")
	require(strings.HasPrefix(first.AccessKeyID, "ASIA"), "access key %s", first.AccessKeyID)

	var (
		mu       sync.Mutex
		keys     = map[string]bool{first.AccessKeyID: true}
		count    int
		failures []string
		wg       sync.WaitGroup
	)
	deadline := time.Now().Add(envSeconds("SKYS3_LOAD_SECONDS"))
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprint(r))
					mu.Unlock()
				}
			}()
			for i := 0; time.Now().Before(deadline); i++ {
				key := fmt.Sprintf("load/%d/%d", n, i%4)
				data := body(n, 1_000+i%7*100)
				_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &wiBucket, Key: &key,
					Body: bytes.NewReader(data)})
				check(err, "PutObject "+key)
				got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &wiBucket, Key: &key})
				check(err, "GetObject "+key)
				require(bytes.Equal(readAll(got.Body), data), "%s: GET returned other bytes", key)
				credentials, err := cfg.Credentials.Retrieve(ctx)
				check(err, "retrieving credentials")
				mu.Lock()
				keys[credentials.AccessKeyID] = true
				count += 2
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()
	require(len(failures) == 0, "failures under load: %v", failures)
	require(len(keys) >= 2, "the session was never refreshed: %v", keys)
	logf("web identity: %d requests under load with %d sessions", count, len(keys))
}

func main() {
	ctx := context.Background()
	pem, err := os.ReadFile(os.Getenv("SKYS3_CA_FILE"))
	check(err, "reading the CA")
	roots := x509.NewCertPool()
	require(roots.AppendCertsFromPEM(pem), "no certificate in the CA file")
	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}},
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "go: FAILED: %v\n", r)
			os.Exit(1)
		}
	}()
	features(ctx, httpClient)
	webIdentity(ctx)
	logf("passed")
}
