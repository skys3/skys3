# The SDK matrix's ceph/s3-tests subset (tests/sdk/run.sh), from a pinned
# commit of ceph/s3-tests.
FROM python:3.13.16-slim-bookworm@sha256:5024f48ba9441d4b13a95d3945abc6365538e3a31109833367a1923523c6efed
ARG S3TESTS_COMMIT=5522d1c351f75bc00ae0f64f742f3f095f5939d9
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && git init -q /s3-tests \
    && git -C /s3-tests fetch -q --depth 1 https://github.com/ceph/s3-tests "$S3TESTS_COMMIT" \
    && git -C /s3-tests checkout -q FETCH_HEAD \
    && rm -rf /s3-tests/.git
WORKDIR /client
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
COPY run-subset.sh skys3_s3tests.py subset.txt ./
ENTRYPOINT ["sh", "/client/run-subset.sh", "/s3-tests"]
