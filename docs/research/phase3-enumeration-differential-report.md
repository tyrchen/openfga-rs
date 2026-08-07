# Phase 3 enumeration differential report

Status: Passing · Verified: 2026-08-05

## Reproducibility pins

- Go baseline: `vendors/openfga` commit `4e4f79ed841513dfd61746a75ef473f6198299f7`
- Go toolchain: 1.26.5, bootstrapped and checksum-verified by the Makefile
- Rust toolchain: the repository-pinned stable toolchain plus `nightly-2026-07-29` for formatting
- Generated semantic seed: `0x5eed_f6a3_0003_0005`
- Generated semantic cases: 64, each containing four finite document states

The differential uses only loopback URLs, rejects redirects, applies connect and request timeouts, and caps every response at 1 MiB. Its fixture contains synthetic identities and no credentials or caller-provided context.

## Semantic evidence

`crates/openfga-list/tests/candidates.rs` generates combinations of direct viewer, computed owner, typed wildcard, and concrete exclusion tuples. For every finite document universe it independently evaluates `Check(document, allowed, user:alice)` and proves:

- the complete `ListObjects` result set equals the Check-filtered universe;
- every emitted object is Check-allowed;
- concrete `ListUsers` membership agrees with Check;
- wildcard membership follows the explicit symbolic exception rule when a concrete user is subtracted.

Failures include the case number, fixed seed, and complete generated flag vector. The same suite covers userset recursion, TTU, conditions, cycles, intersection, difference, wildcard algebra, typed limits, cancellation, and Expand shapes in deterministic fixtures.

Internal tuple-read ceilings are independent of public response caps: deterministic regressions prove that three direct matches with a two-result public cap return a successful truncated result, while raw storage reads still reject true internal overflows. Residual ListObjects Checks share atomic request-level dispatch, datastore-query, and tuple-item meters; concurrent roots are rejected at the evaluator charge point before over-budget work is scheduled or processed.

## Vendored live differential

`make enumeration-differential` starts the pinned Go binary and the complete Rust HTTP/gRPC server with isolated in-memory stores, writes the same model and tuple fixture to both, and compares normalized HTTP observations. Normalization sorts protocol result sets and treats omitted protobuf default repeated fields as their empty-array value; it does not rewrite authorization results or tree variants.

The passing corpus contains 12 cases:

- unary ListObjects for union/computed/TTU/userset/cycle/wildcard and difference;
- StreamedListObjects for the same viewer corpus, including clean terminal completion;
- ListUsers direct, wildcard, wildcard plus explicit user, recursive userset, intersection, and difference;
- Expand direct/computed/TTU union and difference trees.

The run completed with `mismatches: []` against the pinned baseline.

## Backpressure and cleanup evidence

The slow-consumer engine test configures residual concurrency and stream capacity to one, then deliberately leaves the result stream unpolled. Two Checks complete, the full channel prevents the third Check from starting, and dropping the stream releases the producer and its evaluator reference within a bounded timeout.

The runtime disconnect test exercises Check and StreamedListObjects through real HTTP and gRPC listeners. Check blocks during model storage; StreamedListObjects resolves a static model immediately and blocks specifically in reverse tuple storage during candidate discovery. After disconnecting or aborting the client, the test proves the request cancellation token is cancelled, active work returns to zero, listener tasks drain, and the memory actor can be uniquely owned and stopped. This test found and fixed the pre-stream ownership gap: candidate discovery remains guarded until cancellation responsibility transfers to the returned stream.

## PostgreSQL query-plan evidence

`crates/openfga-storage-sql/tests/postgres.rs::verify_hot_query_plans` loads 10,000 tuples and changes, runs `ANALYZE`, and checks the concrete `EXPLAIN (COSTS OFF)` output. Forward reads must use `tuples_pkey` or `tuples_forward_idx`; reverse enumeration must use `tuples_reverse_idx`; both explicitly reject `Seq Scan`. The schema inventory also requires `tuples_userset_idx`, and changelog reads require `tuple_changes_object_type_idx`.

The PostgreSQL plan gate passed against an isolated temporary PostgreSQL 17.10 cluster. The cluster used local trust authentication, was stopped after the test, and its data directory was moved to Trash. The reusable command for an existing isolated database is:

```console
make postgres-storage POSTGRES_TEST_URL='postgres://.../isolated_test_database'
```

## Verification commands

```console
cargo test -p openfga-list --test candidates
cargo test -p openfga-server cancel_http_and_grpc_storage_work_on_client_disconnect
make enumeration-differential
cargo build --workspace --all-targets
cargo test --workspace --all-targets
cargo +nightly-2026-07-29 fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic
```

All commands and the full workspace gates passed on the final Phase 3 diff, including the previously ignored PostgreSQL contract, fault, migration, cancellation, and query-plan suite.
