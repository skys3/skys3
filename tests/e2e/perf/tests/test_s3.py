import datetime
import unittest

from fakes import FakeNode

from skys3perf import s3
from skys3perf.s3 import Client, Credentials, Payload

EXAMPLE = Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class Signing(unittest.TestCase):
    def test_the_aws_documentation_example(self):
        # "Example: GET Object" of the S3 SigV4 documentation (header-based
        # authentication).
        signed = s3.sign("GET", "examplebucket.s3.amazonaws.com", "/test.txt", "",
                         {"Range": "bytes=0-9"}, EXAMPLE, payload_hash=EMPTY,
                         now=datetime.datetime(2013, 5, 24, tzinfo=datetime.timezone.utc))
        self.assertEqual(
            signed["authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/"
            "aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
        self.assertEqual(signed["x-amz-date"], "20130524T000000Z")

    def test_unsigned_payload_by_default(self):
        signed = s3.sign("PUT", "h", "/b/k", "", {}, EXAMPLE)
        self.assertEqual(signed["x-amz-content-sha256"], s3.UNSIGNED)

    def test_paths(self):
        self.assertEqual(s3.object_path("b"), "/b")
        self.assertEqual(s3.object_path("b", "a b/c+d"), "/b/a%20b/c%2Bd")


class Payloads(unittest.TestCase):
    def test_sizes_and_reuse(self):
        self.assertEqual(sum(len(p) for p in Payload(3 * s3.CHUNK + 5)), 3 * s3.CHUNK + 5)
        self.assertEqual(list(Payload(0)), [])
        self.assertIs(Payload(1, seed=3).block, Payload(2, seed=3).block)
        self.assertNotEqual(Payload(8, seed=1).block[:8], Payload(8, seed=2).block[:8])


class Requests(unittest.TestCase):
    def test_put_get_and_bucket_creation(self):
        with FakeNode() as node:
            client = Client(node.url, EXAMPLE)
            self.assertEqual(client.put("b", "k", Payload(2 * s3.CHUNK + 1)), 200)
            self.assertEqual(node.put_bytes, 2 * s3.CHUNK + 1)
            self.assertEqual(client.get("b", "k"), (200, 5))
            client.create_bucket("b", "http://remote/x")
            node.status = 409
            node.body = b"<Code>BucketAlreadyOwnedByYou</Code>"
            self.assertEqual(client.get("b", "k")[0], 409)
            node.status = 403
            with self.assertRaises(RuntimeError):
                client.create_bucket("b")
            client.close()

    def test_no_answer_is_status_zero(self):
        client = Client("http://127.0.0.1:9", EXAMPLE, timeout=2)
        self.assertEqual(client.put("b", "k", Payload(1)), 0)
        self.assertEqual(client.get("b", "k"), (0, 0))


if __name__ == "__main__":
    unittest.main()
