"""A pytest plugin that adapts ceph/s3-tests to SkyS3 (plan M1-25).

s3-tests empties its buckets before and after every test by listing
object versions, which SkyS3 rejects with 501 NotImplemented (design
section 11: no local versioning APIs). Without versions, ListObjectsV2
lists the same objects, so the cleanup lists them that way. Nothing else
about the tests changes.
"""

import s3tests.functional as functional


def _list_objects(client, bucket, batch_size):
    """Yields the bucket's keys in batches, as `list_versions` does."""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": batch_size})
    for page in pages:
        objects = page.get("Contents", [])
        if objects:
            yield [{"Key": o["Key"]} for o in objects]


functional.list_versions = _list_objects
