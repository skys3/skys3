# The SDK matrix's Go client (tests/sdk/run.sh). The module versions are
# pinned by go.mod and go.sum.
FROM golang:1.26.8-bookworm@sha256:a688600ca24f8a4d3ca77f95b0dd40704a9fc787c826660eb7ba0b641b8b175d
WORKDIR /client
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/sdk-test .
ENTRYPOINT ["/usr/local/bin/sdk-test"]
