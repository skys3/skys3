# The SDK matrix's Python client (tests/sdk/run.sh).
FROM python:3.13.16-slim-bookworm@sha256:5024f48ba9441d4b13a95d3945abc6365538e3a31109833367a1923523c6efed
WORKDIR /client
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
COPY sdk_test.py .
ENTRYPOINT ["python", "/client/sdk_test.py"]
