# SkyS3 security review (M7-08)

This review checks the code against design section 12 (Security) and the
plan entry of M7-08: input bounds, mutual TLS on every internal path,
control-store credential scope and remote-side versioning, disabled
0-RTT, secret handling, and a dependency audit.

- **Base.** The stack at `334662c` (M6-08), plus the fixes of this PR.
- **Method.** Code reading of every listener, connector, parser, and
  secret named below. `cargo deny check` (licenses, bans, sources, and
  the RustSec advisory database). A scan of every crate root for
  `#![forbid(unsafe_code)]`, of the tree for `build.rs`, and of the
  dependency graph for build scripts and proc macros. `cargo audit` is
  not installed; `cargo deny check advisories` reads the same database.
- **Verdicts.** *Holds*: the code does what the claim says, and a test
  shows it. *Gap*: it does not, or not fully; the gap is fixed here or
  tracked below. *Not applicable yet*: the path exists as library code,
  exercised by tests and the simulation, but the node binary does not run
  it yet.

File references are to the PR's head. Test names are functions in the
file named, or in the crate's `tests/` directory.

## 1. Summary

Fixed in this PR, each with a test:

1. The admin bearer token compared with `==` in variable time (a derived
   `PartialEq`), and its own comparison was a hand-written fold. Both now
   use `aws-lc-rs`'s constant-time comparison, and the token is zeroed on
   drop.
2. The check that no target overlaps the control prefix ran only when
   both endpoints had the same failure scope, so the control bucket named
   through another AWS region's endpoint passed. AWS bucket names are now
   one namespace for that check.
3. A ranged `GetObject` buffered whatever body the store sent, so the
   peer descriptor read (64 KiB by §7.8) could buffer an object of any
   size from a store that ignores `Range`. A longer answer is now refused
   before its body is read.
4. Intra-cluster TLS had no test that a node issues no session tickets
   (so no resumption and no 0-RTT). One is added, and early data is now
   refused explicitly on both sides.

Tracked for later PRs (section 9): 8 new findings (3 medium, 5 low) and
the 4 items earlier tasks reported and did not fix, each verified against
the current code. No finding is high: the node binary runs a single node
with the file control store, and the paths of the medium findings are
either configuration an operator chooses or not wired into the binary.

## 2. Internal paths

"Mutual" means both ends prove an identity before any message is acted
on. Every intra-cluster path runs over `skys3-net`'s `Transport`
(TCP + TLS 1.3, mutual, SPIFFE identities), which the node binary does
not bind yet: replication, routing, the coordinator, and fragment
transfer run in the simulation and the libraries' tests only.

| Path | In the binary | Mutually authenticated | How identities are checked | Verdict |
|---|---|---|---|---|
| Node to node: replication, leases, backfill (`skys3-shard` `replication`) | No | Yes: mTLS, both chains against `[transport] tls_ca_file` (`skys3-net/src/pki.rs:277` `PeerVerifier::verify`) | SPIFFE ID `spiffe://<cluster>/node/<id>`, exactly one URI SAN, this cluster's trust domain (`identity.rs:158`); a client checks the node it meant (`transport.rs:154`); class per role on every frame (`transport.rs:374`, `message.rs:144`); a member follows only the primary its configuration names (`replication/member.rs:197`), a learner only its primary (`replication/backfill.rs:407`) | Not applicable yet; the library holds (tests below) |
| Gateway routing wire (`Forward` to a shard primary, `skys3-gateway/src/routing`) | No | Yes, same transport | `Request` class: nodes only, admin certificates refused; the client checks the primary's node ID | Not applicable yet; holds |
| Coordinator and admin messages (`skys3-coord/src/admin.rs`) | No | Yes, same transport | Heartbeats take the node ID from the certificate (`admin.rs:145`); change hints and handoff commands come from any node or `admin` certificate | Not applicable yet; holds, with finding T6 (handoffs are not checked against the coordinator) |
| Fragment reads, transfers, orphan checks (`skys3-ec` `read/wire.rs`, `orphans/wire.rs`, `transfer`) | No | Yes, same transport | `Replication` class (`FragmentData`): nodes only; the reader's node ID from the certificate (`read/wire.rs:494`, `orphans/wire.rs:382`) | Not applicable yet; holds |
| Peer QUIC transport (`skys3-peer`) | Yes, when `[peering.peers]` is set (`skys3/src/peering.rs`) | Yes: mTLS, the peer's chain against that peer's own `ca_file` (`skys3-peer/src/tls.rs:210`) | Leaf's trust domain must be a configured peer and lead to its bundle; holder must be a `node`; a source checks the destination cluster from the server name (`tls.rs:296`); `HELLO`'s cluster must equal the certificate's (`endpoint.rs:466`); every `BEGIN`, `COMMIT`, `BATCH` item, and `ABORT` authorized against the peer's bucket pairs (`trust.rs:198`) | Holds |
| Peer S3 REST fallback (`s3_access_key_ids`, `x-skys3-apply-by`; `skys3-gateway/src/peer_s3.rs`) | Yes | No: the source signs with SigV4 (a shared secret); the destination is authenticated only by the target endpoint's TLS (WebPKI), or not at all over `http://` | The access key maps to one peer (`peer_s3.rs:118`, config refuses a key listed twice); a write identity must name that peer and one of its pairs (`peer_s3.rs:129`); `x-skys3-*` headers must be signed (`sigv4/mod.rs:356`); the apply-by time is read only from the peer's keys and required where `COMMIT`s land (`peer_s3.rs:199`); the descriptor is verified against the peer's bundle before any address is used (`skys3-peer/src/descriptor.rs:409`) | Holds as designed; finding T2 (no destination authentication over `http://`) |
| Control store, file (`skys3-control/src/file.rs`) | Yes | Not a network path: a local directory under an exclusive lock | Directory permissions | Holds; findings T4 (no value bound), K1 (no reopen after a failed write) |
| Control store, S3 (`skys3-control/src/s3.rs`) | No (the binary refuses it, `skys3/src/node.rs:811`) | One-way: the store by its endpoint's TLS; SkyS3 by its SigV4 credential | Keys only under the prefix (`s3.rs:160`); targets may not overlap it (`skys3-config/src/cluster.rs:137`, fixed here for AWS regions) | Not applicable yet; findings T4, T8 |
| Control store, etcd (`skys3-control/src/etcd`) | No | Optional: `https://` with a `rustls` client configuration that may carry a client certificate (`etcd/mod.rs:53`); `http://` endpoints are accepted | None beyond TLS; no configuration keys load a client certificate yet | Not applicable yet; finding T3 |
| Admin HTTP listener: metrics, health, admin API (`skys3-obs/src/admin.rs`) | Yes | No: plain HTTP, bearer token, loopback by default | Token of at least 32 bytes in constant time (`admin.rs:168`, fixed here); a non-loopback `listen` without a token is refused (`admin.rs:111`); `/healthz` and `/readyz` are open by design | Holds as designed; finding T1 (no TLS, no `admin` certificates) |
| Gateway (S3 and STS clients) | Yes | No, by design: clients sign with SigV4; the gateway serves HTTPS when configured (`skys3/src/tls.rs:35`) and warns on plain HTTP off loopback (`node.rs:724`) | SigV4 signatures in constant time (`sigv4/canonical.rs:260`) | Holds |

## 3. Claims of section 12

One row per claim of §12, and per item of the plan entry.

| # | Claim | Where it is enforced | Test that shows it | Verdict |
|---|---|---|---|---|
| 1 | Header fields at most 100, header bytes at most 16 KiB, request target at most 16 KiB, checked before authentication | `skys3-gateway/src/limits.rs:136` `check_head`, `:215` `check_headers`; hyper's limits from the same values (`listener.rs:93`) | `limits.rs` `header_sections_are_bounded`, `uris_and_keys_are_bounded` | Holds |
| 2 | Keys at most 1,024 bytes; part numbers 1 to 10,000; `Range` at most 64 bytes, one range below 2⁶³ | `limits.rs:156`, `:312` `valid_part_number`, `:181` | `limits.rs` `uris_and_keys_are_bounded`, `part_numbers_and_ranges_are_bounded` | Holds |
| 3 | XML bodies read up to 4 MiB, at most 32 elements deep, no DTD, before `s3s` parses them | `limits.rs:248` `check_xml`; `s3s_config` sets `xml_max_body_size` | `limits.rs` `declared_xml_bodies_are_bounded`, `xml_depth_and_doctypes_are_checked`, proptests; fuzz `gateway_request` | Holds |
| 4 | `aws-chunked`: chunk line at most 128 bytes, trailers at most 4 KiB and 8 declared, data streamed, total equal to `x-amz-decoded-content-length` at most 5 GiB | `sigv4/chunked.rs:40`–`47`, `sigv4/body.rs:158` `decoded_length` | `chunked.rs` `malformed_bodies_are_refused`, `body.rs` `chunked_heads_are_checked`; fuzz `gateway_aws_chunked` | Holds |
| 5 | Canonical request bytes kept intact for SigV4; `x-amz-*` and `x-skys3-*` headers must be signed | `sigv4/mod.rs:356` `check_signed_headers`; unsigned requests carrying `x-skys3-*` refused (`mod.rs:222`) | `tests/sigv4.rs`, `tests/sigv4_http.rs`; fuzz `gateway_sigv4_canonical` | Holds |
| 6 | Internal traffic uses mutual TLS with node identities from the operator's PKI | `skys3-net/src/pki.rs:216`, `:237`, `:277` | `skys3-net/tests/transport.rs` `a_client_without_a_certificate_is_refused`, `a_client_from_another_ca_is_refused` | Not applicable yet in the binary; holds in the library |
| 7 | TLS 1.3 only, `aws-lc-rs`, no session resumption, so every connection verifies a full chain | `pki.rs:216`–`250`: TLS 1.3 only, no session storage, no tickets, no early data (explicit since this PR) | `transport.rs` `a_node_issues_no_session_tickets` (new; fails with the ticket settings removed) | Holds (test added) |
| 8 | Certificates, key, and CA read from `[transport]` files at startup; rotation needs a restart; no revocation lists | `pki.rs` `Credentials::load` (key buffer zeroed); `skys3/src/peering.rs:83` | `transport.rs` `credentials_load_from_pem_files` | Holds |
| 9 | Exactly one SPIFFE URI SAN, `spiffe://<cluster_id>/<role>/<name>`; trust domain is the cluster ID | `skys3-net/src/identity.rs:158` `from_uri_names` | `transport.rs` `identities_of_other_clusters_or_without_a_spiffe_id_are_refused`; fuzz `net_spiffe_id` | Holds |
| 10 | Chain, validity, and `clientAuth`/`serverAuth` usage checked on both ends | `pki.rs:277` (`verify_for_usage`) | `transport.rs` `expired_and_not_yet_valid_certificates_are_refused`, `certificates_without_the_needed_key_usage_are_refused` | Holds |
| 11 | A client checks that the server is a node, and the node it meant | `pki.rs:338` (role), `transport.rs:154` (node ID) | `transport.rs` `a_client_checks_which_node_it_reached` | Holds |
| 12 | Message classes per role; an `admin` certificate sends only admin messages; a forbidden frame ends the connection | `message.rs:144`, `transport.rs:374` and `:411` | `transport.rs` `admins_may_send_only_admin_messages`, `a_node_refuses_replication_and_lease_messages_from_an_admin`; `message.rs` unit tests | Holds |
| 13 | Shard-level roles are checked against the peer's authenticated node ID | `replication/member.rs:197`, `replication/backfill.rs:407`, `coord/admin.rs:145` | `skys3-shard` replication tests; simulation | Holds for replication and heartbeats; handoffs are not checked (T6) |
| 14 | Frames: 64 KiB header, 32 MiB payload, both checked before allocation, buffer grows as bytes arrive; ALPN `skys3-cluster/1`; handshake within 10 s | `skys3-net/src/frame.rs:31`, `:234`, `:262`; `transport.rs:23`, `:27` | `frame.rs` `lengths_over_the_limits_are_refused_from_the_prefix`, `a_payload_buffer_grows_only_with_the_bytes_received`; `transport.rs` `peers_must_speak_the_cluster_protocol`; fuzz `net_frame` | Holds |
| 15 | Admin listener on loopback by default; non-loopback needs `token_file`; token applies on loopback too; `/healthz` and `/readyz` open | `skys3-obs/src/admin.rs:111` `validate`, `:432` `authorized` | `tests/admin_listener.rs` `token_protects_metrics_but_not_health_checks`, `refuses_a_non_loopback_address_without_a_token`; fuzz `obs_bearer` | Holds |
| 16 | The token is at least 32 bytes and compared in constant time | `admin.rs:68`, `:168` `matches`, `:173` `PartialEq` | `admin.rs` `token_rules`, `tokens_compare_whole_and_alike_by_eq_and_by_header` (new) | Gap, fixed: `==` was variable-time |
| 17 | TLS for the admin listener and `admin` certificates as an alternative to the token are not built | (absent) | none | Gap as documented; tracked as T1 |
| 18 | Gateway HTTPS with `rustls` on `aws-lc-rs`, TLS 1.2 and 1.3, HTTP/1.1; warns on plain HTTP off loopback | `skys3/src/tls.rs:35`; `node.rs:724` | `tls.rs` `loads_a_certificate_and_its_key`; `skys3/tests/sdk.rs` (HTTPS) | Holds |
| 19 | The control store gets a dedicated bucket or prefix | S3 backend: keys only under the prefix (`skys3-control/src/s3.rs:160`, `RegisterKey` grammar); config refuses overlapping targets (`skys3-config/src/cluster.rs:137`) | `tests/validation.rs` `a_target_must_not_overlap_the_control_prefix_even_when_correlation_is_allowed`, `attached_targets_must_not_share_the_control_stores_scope` (extended) | Gap, fixed: the overlap check skipped AWS endpoints of other regions |
| 20 | Least-privilege control-store credentials | Operator's (§6.1); SkyS3 cannot verify scope | none | Holds as designed; §12 now says what SkyS3 checks |
| 21 | Remote-side versioning for audit | Operator's; no code reads the bucket's versioning | none | Gap in the design's wording, fixed: §12 now says SkyS3 does not check it; T8 proposes a check |
| 22 | E2EE ciphertext never decrypted; keys, sizes, and access patterns visible | No decryption code exists | none | Holds |
| 23 | Peer clusters authenticate each other with mTLS against a configured trust bundle | `skys3-peer/src/tls.rs:210` (per-peer roots from `PeerTrust`) | `skys3-peer/tests/endpoint.rs` `untrusted_peers_are_rejected`; `tests/trust.rs` `trust_bundles_load_from_the_peering_section` | Holds |
| 24 | A CA shared by two peers cannot vouch for one as the other; holder must be a node; a source checks the destination cluster | `tls.rs:210` (roots chosen by the leaf's trust domain), `:296` | `endpoint.rs` `untrusted_peers_are_rejected`, `hellos_must_match_the_certificate_and_share_a_version` | Holds |
| 25 | Each peer is authorized for specific bucket pairs | `skys3-peer/src/trust.rs:198` `authorize`, applied to `BEGIN`, `COMMIT`, `BATCH` items, `ABORT` | `endpoint.rs` `unauthorized_bucket_pairs_are_rejected`; `trust.rs` `peers_write_only_their_authorized_pairs` | Holds |
| 26 | QUIC 0-RTT disabled, so a replayed peer message never applies a mutation | Section 4 | `endpoint.rs` `zero_rtt_attempts_are_rejected`; `trust.rs` `tls_refuses_resumption_and_early_data` | Holds |
| 27 | `s3_access_key_ids`: a key belongs to at most one peer; its requests may write the buckets receiving from that peer and carry that peer's identities | `skys3-config/src/peering.rs:167`; `peer_s3.rs:118`, `:129` | `tests/peer_s3.rs` `a_peer_flushes_over_s3_with_its_write_identity`, `only_the_peer_writes_and_only_its_own_identities` | Holds |
| 28 | `x-skys3-apply-by` checked where the write is sequenced; required where `COMMIT`s land | `peer_s3.rs:199`; `Precondition::ApplyBy` | `tests/peer_s3.rs` `where_commits_land_a_peer_write_must_be_sequenced_by_its_apply_by_time`, `where_no_commit_lands_the_apply_by_time_is_optional` | Holds |
| 29 | Peer descriptors signed with the `[transport]` key and trusted only as far as the chain leads to the named cluster's bundle | `descriptor.rs:409` `verify`; size checked first (`:298`) | `tests/descriptor.rs` `forged_descriptors_are_refused`, `descriptors_are_valid_only_for_their_time_bucket_and_source`; fuzz `peer_descriptor` | Holds |
| 30 | The source reads at most 64 KiB of the descriptor (§7.8) | `skys3-flush/src/peer/discovery.rs:378` asks for the range; `skys3-remote/src/aws/store.rs:195`–`222` refuses a longer answer | `skys3-remote/tests/aws.rs` `a_ranged_read_answered_with_more_than_the_range_is_refused` (new) | Gap, fixed |
| 31 | Nodes are non-malicious; no Byzantine tolerance | (assumption) | none | Not applicable |
| 32 | Plan: dependency audit | Section 7 | `cargo deny check` | Holds |

## 4. 0-RTT

| Endpoint | Server side | Client side | Test |
|---|---|---|---|
| Peer QUIC (`skys3-peer`, node binary and simulation alike) | `max_early_data_size = 0`, no session storage, no tickets (`tls.rs:127`); a stream opened in 0-RTT is stopped (`endpoint.rs:667`) | Resumption disabled, `enable_early_data = false` (`tls.rs:145`); no code calls `into_0rtt` | `endpoint.rs` `zero_rtt_attempts_are_rejected` sends a `HELLO` in 0-RTT with a ticket from another server and is refused; `trust.rs` `tls_refuses_resumption_and_early_data` |
| Intra-cluster TCP (`skys3-net`) | No session storage, no tickets, `max_early_data_size = 0` (`pki.rs:216`) | Resumption disabled, `enable_early_data = false` (`pki.rs:237`) | `transport.rs` `a_node_issues_no_session_tickets` (new) |
| Gateway HTTPS | `rustls` defaults: `max_early_data_size = 0`; stateful resumption allowed, which §12 does not forbid for clients | n/a | none needed |

The simulation's QUIC endpoints use the node binary's `PeerEndpoint` and
`PeerTls`, so they refuse early data the same way.

## 5. Secrets

| Secret | Loaded | Held | Printed or logged | Compared | Verdict |
|---|---|---|---|---|---|
| Static secret access keys (`[identity.static_credentials]`) | From `secret_access_key_file` at startup (`skys3-gateway/src/credentials.rs:79`); read buffer zeroed | `SecretAccessKey`, `secrecy` (`sigv4/mod.rs:81`) | `Debug` redacted (`SecretAccessKey`, `StaticCredentials`); errors name the file and length only (`credentials.rs:166`) | Signatures in constant time (`canonical.rs:260`) | Holds |
| STS session secrets and tokens | Issued in memory | Sealed with a key derived from the token; only the token's SHA-256 stored (`skys3-sts/src/session.rs`); `IssuedSession` fields `Zeroizing` | `Debug` of `Session`, `IssuedSession`, `AssumeRoleRequest` omit them; logs carry issuer, subject, role, access key ID | Token hash in constant time (`session.rs:195`) | Holds |
| Web identity tokens (OIDC) | Request parameter, `Zeroizing` (`endpoint/request.rs:54`) | Dropped after validation | Validation errors name the failing part, never the token | Signatures by `aws-lc-rs` | Holds |
| Admin bearer token | `[admin] token_file` at startup (`skys3/src/node.rs:1096`) | `Zeroizing<String>` (since this PR) | `Debug` redacted | Constant time since this PR | Gap, fixed |
| Node private key (`[transport]`) | PEM file at startup, buffer zeroed (`pki.rs` `Credentials::load`) | `rustls` `CertifiedKey` | `Debug` of `Credentials`, `PeerTls`, `DescriptorSigner` omit it | n/a | Holds |
| Continuation-token HMAC keys | Generated, or shared by configuration | `aws-lc-rs` `hmac::Key` | `Debug` of `ListTokenKeys` shows counts only | `hmac::verify` (constant time, `listing/token.rs:150`) | Holds |
| Remote target and control-store credentials | `aws-config` providers, including web identity (`skys3-remote/src/aws`) | The SDK's providers | The SDK redacts `Debug`; SkyS3 logs no request headers or URIs | n/a | Holds |
| etcd client certificate | `EtcdStoreConfig::tls`, a prebuilt `rustls` configuration | `rustls` | `Debug` shows only whether TLS is set (`etcd/mod.rs:83`) | n/a | Holds; no configuration keys yet (T3) |

The configuration holds paths, never secret values, so printing a
`Config` prints no secret. No `tracing` call in the gateway, STS, admin
listener, or node binary records request URIs, query strings, or headers,
so presigned URL signatures and `Authorization` headers stay out of logs.

## 6. Input bounds

| Input | Source | Bound | Where | Test or fuzz target | Verdict |
|---|---|---|---|---|---|
| Gateway request head | Clients | §12 limits | `limits.rs:136` | `limits.rs` tests; `gateway_request` | Holds |
| XML bodies | Clients | 4 MiB, depth 32, no DTD | `limits.rs:248` | `gateway_request` | Holds |
| `aws-chunked` framing | Clients | 128 B lines, 4 KiB and 8 trailers, 5 GiB | `sigv4/chunked.rs` | `gateway_aws_chunked` | Holds |
| SigV4 canonical form, presigned parameters | Clients | Head limits | `sigv4/params.rs`, `canonical.rs` | `gateway_sigv4_canonical` | Holds |
| Continuation tokens | Clients | HMAC first | `listing/token.rs` | `gateway_list_token` | Holds |
| Copy source ranges, tagging, checksums | Clients | Typed parsers | `objects/`, `checksum/` | `gateway_copy_range`, `gateway_tagging`, `gateway_checksums` | Holds |
| STS requests, JWTs, JWKS | Clients, issuers | 20,000-byte JWT (`jwt.rs:21`); fetched documents through `Limited` (`http.rs:162`) | `skys3-sts` | `sts_assume_role`, `sts_jwt`, `sts_jwks` | Holds |
| Intra-cluster frames | Nodes, tools | 64 KiB header, 32 MiB payload | `skys3-net/src/frame.rs` | `net_frame` | Holds |
| Routing wire | Nodes | Frame limits, then key and extent counts (`routing/wire.rs:732`) | `skys3-gateway` | `gateway_forward` | Holds |
| Replication, coordinator, EC wire bodies | Nodes | Frame limits, typed decoders | `skys3-shard`, `skys3-coord`, `skys3-ec` | `shard_replication`, `coord_heartbeat`, `coord_handoff`, `coord_control_changed`, `ec_read`, `ec_transfer`, `ec_orphans` | Holds |
| Peer frames | Peer clusters | 1 MiB header, 16 MiB payload, prefix checked first (`skys3-peer/src/frame.rs:46`); batch, range, and piece counts (`message.rs:26`–`44`); 256 streams, no unidirectional streams (`endpoint.rs:106`) | `skys3-peer` | `peer_messages` | Holds |
| Peer descriptor | A target's S3 endpoint | 64 KiB, checked before decoding (`descriptor.rs:298`) and, since this PR, before the body is buffered (`aws/store.rs:215`) | `skys3-peer`, `skys3-remote` | `peer_descriptor`; `aws.rs` `a_ranged_read_answered_with_more_than_the_range_is_refused` | Gap, fixed |
| Control-store documents, etcd | etcd | 4 MiB gRPC message, prefix checked first (`etcd/grpc.rs:36`, `:248`) | `skys3-control` | `control_etcd_frame` | Holds |
| Control-store documents, S3 and file | Control bucket, local directory | None: a register is read whole (`s3.rs` `get`, `file.rs:329`) | `skys3-control` | none | Gap, tracked as T4 (low: the store is a trust anchor) |
| Register JSON | Control store | `serde_json` recursion limit, `deny_unknown_fields` | `skys3-types/src/register.rs` | `tests/registers.rs` | Holds |
| Control rebuild exports | Operator files | Typed decoder | `skys3-control/src/rebuild.rs` | `control_rebuild` | Holds |
| Log records on recovery | Local disk | 16 MiB payload (`skys3-types/src/limits.rs:18`), counts bounded by bytes present | `skys3-log/src/record` | `log_record`; `tests/recovery_props.rs` | Holds |
| Index rows | Local disk (redb) | Typed codec | `skys3-index/src/codec.rs` | `index_codec` | Holds |
| Index snapshots | Snapshot target | Counts bounded by bytes present (`snapshot/format.rs:264`); the object itself read whole | `skys3-flush/src/snapshot` | `flush_snapshot` | Holds for the decoder; buffering tracked as T5 |
| Fragment headers | Local disk | Typed decoder | `skys3-ec` | `ec_fragment_header` | Holds |
| Write identities, ETags, node addresses, policies | Clients, peers, registers | Typed parsers | `skys3-types` | `types_write_identity`, `types_etag`, `types_node_address`, `types_policy`, `types_trust_policy` | Holds |
| `x-skys3-apply-by` | Peer keys only | `u64` parse, signed header | `peer_s3.rs:199` | `tests/peer_s3.rs` | Holds |
| Remote responses (listings, heads, errors) | Remote targets | Parsed by the AWS SDK | `skys3-remote/src/aws` | `tests/aws.rs` | Holds |
| Configuration and certificate files | Operator | Read whole at startup | `skys3-config`, `skys3-net`, `skys3-peer/src/trust.rs` | `tests/validation.rs` | Not applicable: operator input |

## 7. Dependencies and unsafe code

- `cargo deny check`: advisories, bans, licenses, and sources all pass.
  `advisories.ignore` is empty, unmaintained crates are denied, and the
  license allowlist is permissive only. The `skip` entries of `deny.toml`
  are duplicate versions, each with a reason. The second versions
  compiled on Linux are `syn` 2 (build time only), `rand` and `rand_core`
  0.10 (quinn-proto), `digest` 0.10 and `crypto-common` 0.1 (crc-fast),
  and `cpufeatures` 0.2 (reed-solomon-simd); the others serve other
  platforms only.
- No `ring`, OpenSSL, or `native-tls` is compiled for the host: one TLS
  stack, `rustls` on `aws-lc-rs` (`cargo tree -i ring` prints nothing).
- Every crate root and fuzz target has `#![forbid(unsafe_code)]`, and the
  workspace lint `unsafe_code = "forbid"` covers every target, integration
  tests included. No workspace crate has a `build.rs`.
- Third-party build scripts (51) are the usual ones: `aws-lc-sys` (C and
  assembly through `cc`/`cmake`), `libc`, `rustix`, `quinn-udp`, `redb`,
  `reed-solomon-simd`, `serde`, `proc-macro2`, and the like.
  `readme-rustdocifier` is a build dependency of `reed-solomon-simd`, and
  `defmt` appears in metadata only, for embedded targets. Crates with
  unsafe code that SkyS3 relies on: `aws-lc-sys`/`aws-lc-rs` (crypto),
  `quinn-udp` (socket options), `redb` (memory-mapped I/O), and
  `reed-solomon-simd` (SIMD).

## 8. Fixes in this PR

| Fix | Files | Test |
|---|---|---|
| Admin token compared in constant time by `PartialEq` and `matches`; token and file buffer zeroed | `crates/skys3-obs/src/admin.rs` | `tokens_compare_whole_and_alike_by_eq_and_by_header` |
| Overlap check treats AWS endpoints of every region as one bucket namespace | `crates/skys3-config/src/cluster.rs`, `target.rs` | `attached_targets_must_not_share_the_control_stores_scope` (fails without the fix) |
| A ranged `GetObject` refuses a response longer than the range before reading it | `crates/skys3-remote/src/aws/store.rs`, `model.rs` (`ByteRange::max_len`) | `a_ranged_read_answered_with_more_than_the_range_is_refused` |
| Intra-cluster TLS refuses early data explicitly; a test that a node issues no tickets | `crates/skys3-net/src/pki.rs` | `a_node_issues_no_session_tickets` (fails with the ticket settings removed) |

## 9. Tracked findings

New findings first, then the items earlier tasks reported and did not
fix. Each is written for an issue of its own.

### T1. The admin listener serves plain HTTP

- **Severity:** medium.
- **Path:** admin HTTP listener (metrics, health, admin API).
- **Scenario:** an operator binds `[admin] listen` to a management
  address so Prometheus can scrape it. Anyone who can observe that network
  reads the bearer token from a scrape and can then call
  `POST /v1/buckets/<name>/conflicts/overwrite/<key>`, overwriting the
  remote's newer version, or `discard_local`, dropping a local write.
- **Proposed fix:** `[admin] tls_cert_file` and `tls_key_file`, served
  with the gateway's `rustls` configuration, and `admin` certificates of
  the node PKI accepted in place of the token (design §12, plan §14).
  Until then, refuse a non-loopback `listen` without TLS unless an
  explicit `allow_plaintext_admin = true`.

### T2. Remote targets and the peer S3 fallback accept `http://`

- **Severity:** medium.
- **Path:** remote targets (`write_back`, backup, snapshot), and the peer
  S3 REST fallback.
- **Scenario:** a target is configured as `http://`. A machine in the
  path answers `200` with an ETag to the flusher's `PUT`s. The source
  records `FLUSHED`, may evict the clean copy, and acknowledges
  write-through clients, while the remote never received the object: a
  silent loss of the durable copy. For a peer, the same MITM can also
  withhold the descriptor and keep the target on S3 REST. SigV4 keeps the
  secret and the request's integrity, but authenticates only the client.
- **Proposed fix:** refuse `http://` endpoints for targets, origins, and
  the control store at configuration loading, unless the host is
  loopback or `allow_plaintext_endpoint = true` is set on that target;
  warn at startup when it is.

### T3. The etcd control store accepts `http://` and has no certificate keys

- **Severity:** medium (not reachable from the binary yet).
- **Path:** control store, etcd backend.
- **Scenario:** once the binary runs etcd (plan M2), an operator lists
  `http://etcd-1:2379` because no configuration key loads a client
  certificate. Anyone on that network can write `shards/<bucket>/<n>.json`
  and reassign primaries, which §12 calls a trust anchor.
- **Proposed fix:** `[control_store] etcd_tls_ca_file`,
  `etcd_tls_cert_file`, and `etcd_tls_key_file`, building the existing
  `EtcdStoreConfig::tls`; refuse `http://` endpoints off loopback.

### T4. Control-store register values have no size bound on the S3 and file backends

- **Severity:** low.
- **Path:** control store, S3 and file backends.
- **Scenario:** a provider fault or a writer of the control bucket leaves
  a multi-gigabyte object at `cluster.json`. Every node polls it and
  buffers it whole on each change, running out of memory. (etcd's
  4 MiB message limit already bounds its backend.)
- **Proposed fix:** a `MAX_REGISTER_BYTES` (1 MiB is ample) in
  `skys3-control`, refused by `put_if` and checked on reads: the S3
  backend from `Content-Length` before buffering (as `AwsS3::get_object`
  now does for ranges), the file backend from the file's length.

### T5. Unranged remote reads are buffered whole

- **Severity:** low.
- **Path:** remote targets: snapshot restore and drills
  (`skys3-flush/src/snapshot/restore.rs`), the S3 control store.
- **Scenario:** a snapshot target returns an object far larger than any
  snapshot, or a misbehaving store streams without end; the node buffers
  it until memory runs out. Fills and imports read in bounded ranges and
  are not affected.
- **Proposed fix:** give `GetObject` an optional `max_bytes` that
  `AwsS3` checks against `Content-Length` and enforces while collecting,
  and pass a bound sized from the shard's index for snapshots.

### T6. Handoff commands are not checked against the coordinator

- **Severity:** low (nodes are assumed non-malicious).
- **Path:** coordinator and admin messages (`skys3-coord/src/admin.rs`).
- **Scenario:** a node with a stale view of who coordinates, or a stray
  operator tool, sends `Handoff` for a shard; the receiving primary
  starts a planned handoff the coordinator did not decide, racing a
  concurrent placement round.
- **Proposed fix:** accept `Handoff` only from the node the coordinator
  lease names, or from an `admin` certificate, and log the sender.

### T7. Pre-authentication handshakes are not capped

- **Severity:** low.
- **Path:** intra-cluster TCP listeners (`skys3-net` users), the peer
  QUIC endpoint, and the gateway.
- **Scenario:** an unauthenticated host floods a listener with
  connections. Each gets a task and TLS state for up to the handshake
  timeout (10 s, or `peer_connect_timeout`), so memory grows with the
  flood rate. The admin listener already caps connections at 256.
- **Proposed fix:** a semaphore of in-flight handshakes per listener, as
  the admin listener has, sized by configuration.

### T8. SkyS3 does not check the control bucket's versioning

- **Severity:** low.
- **Path:** control store, S3 backend.
- **Scenario:** an operator forgets to enable versioning; a compromised
  credential overwrites registers, and no history of the change exists
  for an audit or for choosing a copy to rebuild from.
- **Proposed fix:** at startup, if the credential allows
  `s3:GetBucketVersioning`, read the status and warn when it is not
  `Enabled`; export it as a gauge. §12 now states that SkyS3 does not
  check it.

### K1. The file control store is not reopened after a failed write

- **Severity:** medium (availability).
- **Path:** control store, file backend; `skys3/src/node.rs` control loop.
- **Verified:** still present. `FileControlStore` sets `failed` on a
  failed write and refuses every later request (`file.rs:172`), and the
  node's loop reopens only when it holds no store (`node.rs:380`).
- **Scenario:** the control directory's disk fills or goes away briefly;
  one CreateBucket fails, and every later bucket change on that node fails
  until a restart, although §6.2 has it use the store once it answers.
- **Proposed fix:** when the attached store reports `failed`, detach it
  and let the loop's `reopen` open it again.

### K2. Bucket changes answer `500` instead of `503` when the store fails

- **Severity:** low.
- **Path:** gateway bucket operations (`skys3-gateway/src/buckets.rs:917`).
- **Verified:** still present: `ControlError::Io` maps to
  `500 InternalError`.
- **Scenario:** as in K1; SDKs do not retry the `500`, and operators read
  it as a bug rather than an outage.
- **Proposed fix:** map a failed store (`Io`) to `503 ServiceUnavailable`,
  as a node running from its copy answers.

### K3. Rollback detection relies on the generation number alone

- **Severity:** medium.
- **Path:** control-state copies (`skys3/src/control.rs:92`
  `ControlCopy::fetch_since`).
- **Verified:** still present: only `generation < previous.generation` is
  refused.
- **Scenario:** a control store is restored from a backup, or rolled back
  by someone with write access, to a state at the copy's generation that
  lacks later writes. Nodes follow it, dropping newer buckets and roles
  or bringing deleted ones back; a membership change written over an older
  shard register can reuse an epoch a replica already used.
- **Proposed fix:** compare register versions where they are ordered
  (etcd's `mod_revision`), and on S3 keep the listed versions of the
  copy's generation and refuse a listing at the same generation that
  differs from it (runbooks section 8).

### K4. The gateway exports no request metrics

- **Severity:** low.
- **Path:** gateway.
- **Verified:** still present: `skys3-gateway` registers no metric.
- **Scenario:** a client brute-forces access keys or replays expired
  presigned URLs; the `403`s are counted nowhere, and neither are the
  `503`s of a failover.
- **Proposed fix:** `skys3_s3_requests_total{operation, code}` and an
  authentication-failure counter by error code, added to the metrics
  reference with an alert on a high `403` rate.
