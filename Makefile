CARGO ?= cargo
RUSTFMT_TOOLCHAIN := nightly-2026-07-29
ACTIONLINT_VERSION := 1.7.12
ACTIONLINT_TOOL := .tools/actionlint-$(ACTIONLINT_VERSION)/actionlint
GO_VERSION := 1.26.5
GO_TOOL := .tools/go/bin/go
GO_BASELINE_COMMIT := 4e4f79ed841513dfd61746a75ef473f6198299f7
GO_BASELINE := .tools/openfga-go-$(GO_BASELINE_COMMIT)
GO_HTTP_ADDR ?= 127.0.0.1:18080
GO_GRPC_ADDR ?= 127.0.0.1:18082
RUST_PROBE_ADDR ?= 127.0.0.1:18081
RUST_HTTP_ADDR ?= 127.0.0.1:18083
RUST_GRPC_ADDR ?= 127.0.0.1:18084
RUST_WRITER_HTTP_ADDR ?= 127.0.0.1:18085
RUST_WRITER_GRPC_ADDR ?= 127.0.0.1:18086
RUST_AUTHZEN_DISABLED_HTTP_ADDR ?= 127.0.0.1:18087
RUST_AUTHZEN_DISABLED_GRPC_ADDR ?= 127.0.0.1:18088
SERVER_READINESS_ATTEMPTS ?= 600
FUZZ_TIME ?= 15
POSTGRES_TEST_URL ?=
MYSQL_TEST_URL ?=
COMPAT_STORAGE_BACKEND ?= postgres
COMPAT_DATABASE_URL ?= $(POSTGRES_TEST_URL)
CONFIG ?= config/openfga-development.yaml
PHASE4_BENCH_REQUESTS ?= 25
PHASE4_BENCH_MUTATION_REQUESTS ?= 5
PHASE4_CONSISTENCY_ITERATIONS ?= 32
PHASE4_SOAK_CLIENTS ?= 100
PHASE4_SOAK_SECONDS ?= 1800
PHASE4_RSS_GROWTH_KIB ?= 65536
PHASE4_MAXIMUM_CONCURRENCY ?= 256
PHASE4_ARTIFACT_DIR ?= target/phase4
PHASE4_STORAGE_BACKEND ?= memory
PHASE4_POSTGRES_MIGRATE ?= false
PHASE4_POSTGRES_PORT ?= 55432
PHASE4_SOAK_CONSISTENCY_ARG ?=
GITLEAKS_VERSION := 8.30.1
GITLEAKS_TOOL := .tools/gitleaks-$(GITLEAKS_VERSION)/gitleaks
SYFT_VERSION := 1.50.0
SYFT_TOOL := .tools/syft-$(SYFT_VERSION)/syft
PHASE5_ARTIFACT_DIR ?= target/phase5
PHASE5_PLATFORM := $(shell uname -s | tr '[:upper:]' '[:lower:]')-$(shell uname -m)
WORKSPACE_TARGET_DIR := $(shell $(CARGO) metadata --no-deps --format-version 1 | \
	sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
UPSTREAM_SQLITE_URL_ENV ?= OPENFGA_UPSTREAM_SQLITE_URL
DESTINATION_SQLITE_URL_ENV ?= OPENFGA_SQLITE_URL
PROTO_OUTPUT := crates/openfga-proto/src/generated

build:
	@$(CARGO) build --workspace --all-targets

build-release:
	@$(CARGO) build --release -p openfga-server

phase4-criterion:
	@$(CARGO) bench -p openfga-cache --bench model_load
	@$(CARGO) bench -p openfga-check --bench check_latency
	@$(CARGO) bench -p openfga-model --bench model_compile
	@$(CARGO) bench -p openfga-model --bench model_compile_memory
	@$(CARGO) bench -p openfga-service --bench identifier_allocation

test:
	@$(CARGO) test --workspace --all-targets

fmt:
	@$(CARGO) +$(RUSTFMT_TOOLCHAIN) fmt --all -- --check
	@$(CARGO) +$(RUSTFMT_TOOLCHAIN) fmt --manifest-path fuzz/Cargo.toml -- --check

clippy:
	@$(CARGO) clippy --workspace --all-targets -- -D warnings

clippy-strict:
	@$(CARGO) clippy --workspace --all-targets -- \
		-D warnings -W clippy::pedantic -W clippy::unwrap_used \
		-W clippy::expect_used -W clippy::indexing_slicing -W clippy::panic
	@$(CARGO) clippy --manifest-path fuzz/Cargo.toml --all-targets -- \
		-D warnings -W clippy::pedantic -W clippy::unwrap_used \
		-W clippy::expect_used -W clippy::indexing_slicing -W clippy::panic

doc:
	@RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps

check: check-proto check-docs check-actions check-fuzz build test fmt clippy doc

check-fuzz:
	@$(CARGO) check --manifest-path fuzz/Cargo.toml --all-targets

validate-config:
	@$(CARGO) run --quiet -p openfga-server -- validate-config --config "$(CONFIG)"

print-effective-config:
	@$(CARGO) run --quiet -p openfga-server -- print-effective-config --config "$(CONFIG)"

migrate-up:
	@$(CARGO) run --quiet -p openfga-server -- migrate --config "$(CONFIG)" up

migrate-status:
	@$(CARGO) run --quiet -p openfga-server -- migrate --config "$(CONFIG)" status

check-docs:
	@$(CARGO) run --quiet -p openfga-doc-check

$(ACTIONLINT_TOOL):
	@actionlint_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$actionlint_tmp"' EXIT; \
	case "$$(uname -s)-$$(uname -m)" in \
		Darwin-arm64) platform=darwin_arm64; sha=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f ;; \
		Darwin-x86_64) platform=darwin_amd64; sha=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644 ;; \
		Linux-aarch64) platform=linux_arm64; sha=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6 ;; \
		Linux-x86_64) platform=linux_amd64; sha=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8 ;; \
		*) echo "unsupported actionlint platform: $$(uname -s)-$$(uname -m)" >&2; exit 1 ;; \
	esac; \
	archive="actionlint_$(ACTIONLINT_VERSION)_$$platform.tar.gz"; \
	curl --fail --location --silent --show-error \
		"https://github.com/rhysd/actionlint/releases/download/v$(ACTIONLINT_VERSION)/$$archive" \
		--output "$$actionlint_tmp/$$archive"; \
	if command -v shasum >/dev/null 2>&1; then \
		actual_sha=$$(shasum -a 256 "$$actionlint_tmp/$$archive" | awk '{print $$1}'); \
	else \
		actual_sha=$$(sha256sum "$$actionlint_tmp/$$archive" | awk '{print $$1}'); \
	fi; \
	test "$$actual_sha" = "$$sha"; \
	mkdir -p "$(@D)"; \
	tar -xzf "$$actionlint_tmp/$$archive" -C "$(@D)" actionlint

check-actions: $(ACTIONLINT_TOOL)
	@$(ACTIONLINT_TOOL) -color

proto:
	@$(CARGO) run -p openfga-proto-codegen -- --output $(PROTO_OUTPUT)

check-proto:
	@phase0_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase0_tmp"' EXIT; \
	$(CARGO) run --quiet -p openfga-proto-codegen -- --output "$$phase0_tmp"; \
	diff -ru $(PROTO_OUTPUT) "$$phase0_tmp"

$(GO_TOOL):
	@phase0_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase0_tmp"' EXIT; \
	case "$$(uname -s)-$$(uname -m)" in \
		Darwin-arm64) archive="go$(GO_VERSION).darwin-arm64.tar.gz"; sha="efb87ff28af9a188d0536ef5d42e63dd52ba8263cd7344a993cc48dd11dedb6a" ;; \
		Darwin-x86_64) archive="go$(GO_VERSION).darwin-amd64.tar.gz"; sha="6231d8d3b8f5552ec6cbf6d685bdd5482e1e703214b120e89b3bf0d7bf1ef725" ;; \
		Linux-aarch64) archive="go$(GO_VERSION).linux-arm64.tar.gz"; sha="fe4789e92b1f33358680864bbe8704289e7bb5fc207d80623c308935bd696d49" ;; \
		Linux-x86_64) archive="go$(GO_VERSION).linux-amd64.tar.gz"; sha="5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053" ;; \
		*) echo "unsupported Go bootstrap platform: $$(uname -s)-$$(uname -m)" >&2; exit 1 ;; \
	esac; \
	curl --fail --location --silent --show-error \
		"https://go.dev/dl/$$archive" --output "$$phase0_tmp/$$archive"; \
	if command -v shasum >/dev/null 2>&1; then \
		actual_sha=$$(shasum -a 256 "$$phase0_tmp/$$archive" | awk '{print $$1}'); \
	else \
		actual_sha=$$(sha256sum "$$phase0_tmp/$$archive" | awk '{print $$1}'); \
	fi; \
	test "$$actual_sha" = "$$sha"; \
	mkdir -p .tools; \
	tar -xzf "$$phase0_tmp/$$archive" -C .tools

verify-go-tool: $(GO_TOOL)
	@$(GO_TOOL) version | grep -F "go$(GO_VERSION)" >/dev/null

verify-go-pin:
	@test "$$(git -C vendors/openfga rev-parse HEAD)" = "$(GO_BASELINE_COMMIT)"
	@test -z "$$(git -C vendors/openfga status --porcelain)"

$(GO_BASELINE): $(GO_TOOL) vendors/openfga/go.mod vendors/openfga/go.sum | verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) build \
		-trimpath -o ../../$(GO_BASELINE) ./cmd/openfga

go-baseline: $(GO_BASELINE)

differential-smoke: $(GO_BASELINE) build
	@phase0_tmp=$$(mktemp -d); \
	go_pid=""; rust_pid=""; \
	cleanup() { \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		rm -rf "$$phase0_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false >"$$phase0_tmp/go.log" 2>&1 & go_pid=$$!; \
	$(CARGO) run --quiet -p openfga-server -- probe-server \
		--address $(RUST_PROBE_ADDR) >"$$phase0_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "http://$(RUST_PROBE_ADDR)/healthz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "a compatibility server exited before readiness" >&2; \
				tail -n 100 "$$phase0_tmp/go.log" "$$phase0_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase0_tmp/go.log" "$$phase0_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	$(CARGO) run --quiet -p openfga-server -- differential-smoke \
		--go-url "http://$(GO_HTTP_ADDR)/" --rust-url "http://$(RUST_PROBE_ADDR)/"; \
	npm ci --prefix tests/sdk-smoke-js --ignore-scripts --no-audit --no-fund; \
	FGA_API_URL="http://$(GO_HTTP_ADDR)" node tests/sdk-smoke-js/smoke.mjs; \
	npm audit --prefix tests/sdk-smoke-js --audit-level=moderate

phase2-compatibility: $(GO_BASELINE) build
	@test "$(COMPAT_STORAGE_BACKEND)" = sqlite || test -n "$(COMPAT_DATABASE_URL)" || { \
		echo "COMPAT_DATABASE_URL is required for $(COMPAT_STORAGE_BACKEND)" >&2; exit 1; \
	}
	@set -eu; \
	phase2_tmp=$$(mktemp -d); \
	compat_database_url="$(COMPAT_DATABASE_URL)"; \
	if test "$(COMPAT_STORAGE_BACKEND)" = sqlite; then \
		compat_database_url="sqlite://$$phase2_tmp/openfga.db?mode=rwc"; \
	fi; \
	go_pid=""; rust_pid=""; \
	cleanup() { \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		rm -rf "$$phase2_tmp"; \
	}; \
	fingerprint() { \
		openssl s_client -connect "$$1" -servername localhost </dev/null 2>/dev/null | \
			openssl x509 -noout -fingerprint -sha256; \
	}; \
	trap cleanup EXIT INT TERM; \
	printf '%s\n' \
		'basicConstraints=critical,CA:FALSE' \
		'keyUsage=critical,digitalSignature,keyEncipherment' \
		'extendedKeyUsage=serverAuth' \
		'subjectAltName=DNS:localhost,IP:127.0.0.1' >"$$phase2_tmp/tls.ext"; \
	openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
		-subj /CN=openfga-ci-ca \
		-addext basicConstraints=critical,CA:TRUE \
		-addext keyUsage=critical,keyCertSign,cRLSign \
		-keyout "$$phase2_tmp/ca.key" -out "$$phase2_tmp/ca.crt" >/dev/null 2>&1; \
	openssl req -new -newkey rsa:2048 -nodes -sha256 -subj /CN=localhost \
		-keyout "$$phase2_tmp/tls.key" -out "$$phase2_tmp/tls.csr" >/dev/null 2>&1; \
	openssl x509 -req -sha256 -days 1 -in "$$phase2_tmp/tls.csr" \
		-CA "$$phase2_tmp/ca.crt" -CAkey "$$phase2_tmp/ca.key" -set_serial 1 \
		-extfile "$$phase2_tmp/tls.ext" -out "$$phase2_tmp/tls.crt" >/dev/null 2>&1; \
	chmod 600 "$$phase2_tmp/ca.key"; \
	chmod 600 "$$phase2_tmp/tls.key"; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false >"$$phase2_tmp/go.log" 2>&1 & go_pid=$$!; \
	OPENFGA__PROFILE=production \
	OPENFGA__LISTENERS__HTTP=$(RUST_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_GRPC_ADDR) \
	OPENFGA__TLS__ENABLED=true \
	OPENFGA__TLS__CERTIFICATE_PATH="$$phase2_tmp/tls.crt" \
	OPENFGA__TLS__PRIVATE_KEY_PATH="$$phase2_tmp/tls.key" \
	OPENFGA__TLS__RELOAD_INTERVAL_SECONDS=1 \
	OPENFGA__AUTHZEN__BASE_URL="https://$(RUST_HTTP_ADDR)" \
	OPENFGA__STORAGE__BACKEND=$(COMPAT_STORAGE_BACKEND) \
	OPENFGA__STORAGE__POSTGRES__MIGRATE_ON_START=true \
	OPENFGA__STORAGE__MYSQL__URL_ENV=OPENFGA_MYSQL_URL \
	OPENFGA__STORAGE__MYSQL__MIGRATE_ON_START=true \
	OPENFGA__STORAGE__SQLITE__URL_ENV=OPENFGA_SQLITE_URL \
	OPENFGA__STORAGE__SQLITE__MAX_CONNECTIONS=1 \
	OPENFGA__STORAGE__SQLITE__MIGRATE_ON_START=true \
	OPENFGA_DATABASE_URL="$$compat_database_url" \
	OPENFGA_MYSQL_URL="$$compat_database_url" \
	OPENFGA_SQLITE_URL="$$compat_database_url" \
	OPENFGA_PRESHARED_KEY=phase2-compatibility-preshared-key-material \
	OPENFGA_TOKEN_KEY=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
	$(CARGO) run --quiet -p openfga-server -- run \
		--config config/openfga-preshared-development.yaml \
		>"$$phase2_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "https://$(RUST_HTTP_ADDR)/readyz"; do \
		attempt=0; \
		until curl --insecure --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "a Phase 2 compatibility server exited before readiness" >&2; \
				tail -n 100 "$$phase2_tmp/go.log" "$$phase2_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "Phase 2 server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase2_tmp/go.log" "$$phase2_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	before_http=$$(fingerprint "$(RUST_HTTP_ADDR)"); \
	before_grpc=$$(fingerprint "$(RUST_GRPC_ADDR)"); \
	test -n "$$before_http"; \
	test "$$before_http" = "$$before_grpc"; \
	mv "$$phase2_tmp/tls.key" "$$phase2_tmp/tls.old.key"; \
	: >"$$phase2_tmp/tls.key"; \
	attempt=0; \
	until grep -F "TLS reload rejected" "$$phase2_tmp/rust.log" >/dev/null; do \
		attempt=$$((attempt + 1)); \
		test "$$attempt" -lt 50 || { echo "invalid TLS reload was not observed" >&2; exit 1; }; \
		sleep 0.1; \
	done; \
	test "$$(fingerprint "$(RUST_HTTP_ADDR)")" = "$$before_http"; \
	openssl req -new -newkey rsa:2048 -nodes -sha256 -subj /CN=localhost \
		-keyout "$$phase2_tmp/tls.next.key" -out "$$phase2_tmp/tls.next.csr" >/dev/null 2>&1; \
	openssl x509 -req -sha256 -days 1 -in "$$phase2_tmp/tls.next.csr" \
		-CA "$$phase2_tmp/ca.crt" -CAkey "$$phase2_tmp/ca.key" -set_serial 2 \
		-extfile "$$phase2_tmp/tls.ext" \
		-out "$$phase2_tmp/tls.next.crt" >/dev/null 2>&1; \
	chmod 600 "$$phase2_tmp/tls.next.key"; \
	mv "$$phase2_tmp/tls.next.key" "$$phase2_tmp/tls.key"; \
	mv "$$phase2_tmp/tls.next.crt" "$$phase2_tmp/tls.crt"; \
	attempt=0; after_http=""; \
	while test -z "$$after_http" || test "$$after_http" = "$$before_http"; do \
		after_http=$$(fingerprint "$(RUST_HTTP_ADDR)" || true); \
		attempt=$$((attempt + 1)); \
		test "$$attempt" -lt 50 || { echo "valid TLS identity was not reloaded" >&2; exit 1; }; \
		sleep 0.1; \
	done; \
	after_grpc=$$(fingerprint "$(RUST_GRPC_ADDR)"); \
	test "$$after_http" = "$$after_grpc"; \
	npm ci --prefix tests/sdk-smoke-js --ignore-scripts --no-audit --no-fund; \
	FGA_API_URL="http://$(GO_HTTP_ADDR)" node tests/sdk-smoke-js/smoke.mjs \
		>"$$phase2_tmp/go-sdk.json"; \
	NODE_EXTRA_CA_CERTS="$$phase2_tmp/ca.crt" \
	FGA_API_URL="https://$(RUST_HTTP_ADDR)" \
	FGA_API_TOKEN=phase2-compatibility-preshared-key-material \
		node tests/sdk-smoke-js/smoke.mjs >"$$phase2_tmp/rust-sdk.json"; \
	diff -u "$$phase2_tmp/go-sdk.json" "$$phase2_tmp/rust-sdk.json"; \
	$(CARGO) run --quiet -p phase2-grpc-smoke -- \
		--go-url "http://$(GO_GRPC_ADDR)" \
		--rust-url "https://$(RUST_GRPC_ADDR)" \
		--rust-ca "$$phase2_tmp/ca.crt" \
		--rust-token phase2-compatibility-preshared-key-material; \
	test "$$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
		"https://$(RUST_HTTP_ADDR)/stores")" = 401; \
	$(CARGO) test -p openfga-transport --all-targets; \
	$(CARGO) test -p openfga-server 'runtime::tests::test_should_'; \
	kill -TERM "$$rust_pid"; \
	wait "$$rust_pid"; \
	rust_pid=""; \
	npm audit --prefix tests/sdk-smoke-js --audit-level=moderate

cel-baseline: verify-go-tool verify-go-pin
	@cd tests/cel-baseline-go && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test ./... && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) run . \
			../cel-conformance/cases.json

cel-spike: cel-baseline
	@$(CARGO) test -p openfga-condition --test conformance

model-baseline: verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test ./pkg/typesystem

model-spike: model-baseline
	@$(CARGO) test -p openfga-model --test compiler

storage-contract:
	@$(CARGO) test -p openfga-storage-memory --test contracts

postgres-storage:
	@test -n "$(POSTGRES_TEST_URL)" || { echo "POSTGRES_TEST_URL is required" >&2; exit 1; }
	@OPENFGA_POSTGRES_TEST_URL="$(POSTGRES_TEST_URL)" \
		$(CARGO) test -p openfga-storage-sql --test postgres -- --ignored

mysql-storage:
	@test -n "$(MYSQL_TEST_URL)" || { echo "MYSQL_TEST_URL is required" >&2; exit 1; }
	@OPENFGA_MYSQL_TEST_URL="$(MYSQL_TEST_URL)" \
		$(CARGO) test -p openfga-storage-sql --test portable \
			test_should_satisfy_mysql_contract_and_plan_gates -- --ignored

sqlite-storage:
	@$(CARGO) test -p openfga-storage-sql --test portable

upstream-migration:
	@$(CARGO) run --quiet -p openfga-upstream-migrate -- \
		--source-url-env "$(UPSTREAM_SQLITE_URL_ENV)" \
		--destination-url-env "$(DESTINATION_SQLITE_URL_ENV)"

upstream-migration-drill:
	@$(CARGO) test -p openfga-upstream-migrate

phase5-sqlite-compatibility:
	@$(MAKE) phase2-compatibility COMPAT_STORAGE_BACKEND=sqlite COMPAT_DATABASE_URL=

phase5-mysql-compatibility:
	@test -n "$(MYSQL_TEST_URL)" || { echo "MYSQL_TEST_URL is required" >&2; exit 1; }
	@$(MAKE) phase2-compatibility COMPAT_STORAGE_BACKEND=mysql \
		COMPAT_DATABASE_URL="$(MYSQL_TEST_URL)"

sqlx-prepare-check:
	@test -n "$(POSTGRES_TEST_URL)" || { echo "POSTGRES_TEST_URL is required" >&2; exit 1; }
	@DATABASE_URL="$(POSTGRES_TEST_URL)" \
		$(CARGO) sqlx prepare --check --workspace -- --all-targets

check-baseline: verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test ./tests/check \
		-run '^(TestCheckMemory|TestMatrixMemory|TestContextualTuplesMemory)$$' -count=1

check-oracle:
	@$(CARGO) test -p openfga-check --all-targets

check-differential: $(GO_BASELINE) build
	@phase1_tmp=$$(mktemp -d); \
	go_pid=""; rust_pid=""; \
	cleanup() { \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		rm -rf "$$phase1_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false >"$$phase1_tmp/go.log" 2>&1 & go_pid=$$!; \
	$(CARGO) run --quiet -p openfga-server -- check-probe-server \
		--address $(RUST_PROBE_ADDR) >"$$phase1_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "http://$(RUST_PROBE_ADDR)/healthz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "a Check compatibility server exited before readiness" >&2; \
				tail -n 100 "$$phase1_tmp/go.log" "$$phase1_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "Check server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase1_tmp/go.log" "$$phase1_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	$(CARGO) run --quiet -p openfga-server -- differential-check \
		--go-url "http://$(GO_HTTP_ADDR)/" --rust-url "http://$(RUST_PROBE_ADDR)/"

enumeration-differential: $(GO_BASELINE) build
	@set -eu; \
	phase3_tmp=$$(mktemp -d); \
	go_pid=""; rust_pid=""; \
	cleanup() { \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		rm -rf "$$phase3_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false >"$$phase3_tmp/go.log" 2>&1 & go_pid=$$!; \
	token_key=$$(openssl rand -base64 32); \
	OPENFGA__LISTENERS__HTTP=$(RUST_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_GRPC_ADDR) \
	OPENFGA_TOKEN_KEY="$$token_key" \
	$(CARGO) run --quiet -p openfga-server -- run \
		--config config/openfga-development.yaml \
		>"$$phase3_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "http://$(RUST_HTTP_ADDR)/readyz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "an enumeration differential server exited before readiness" >&2; \
				tail -n 100 "$$phase3_tmp/go.log" "$$phase3_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "enumeration server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase3_tmp/go.log" "$$phase3_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	$(CARGO) run --quiet -p openfga-server -- differential-enumeration \
		--go-url "http://$(GO_HTTP_ADDR)/" --rust-url "http://$(RUST_HTTP_ADDR)/"

authzen-baseline: verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test ./tests/authzen -count=1

authzen-differential: $(GO_BASELINE) build
	@set -eu; \
	phase6_tmp=$$(mktemp -d); \
	go_pid=""; rust_pid=""; \
	cleanup() { \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		rm -rf "$$phase6_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false --experimentals authzen \
		--authzen-base-url "http://$(GO_HTTP_ADDR)" \
		>"$$phase6_tmp/go.log" 2>&1 & go_pid=$$!; \
	token_key=$$(openssl rand -base64 32); \
	OPENFGA__LISTENERS__HTTP=$(RUST_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_GRPC_ADDR) \
	OPENFGA__AUTHZEN__ENABLED=true \
	OPENFGA__AUTHZEN__BASE_URL="http://$(RUST_HTTP_ADDR)" \
	OPENFGA_TOKEN_KEY="$$token_key" \
	$(CARGO) run --quiet -p openfga-server -- run \
		--config config/openfga-development.yaml \
		>"$$phase6_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "http://$(RUST_HTTP_ADDR)/readyz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "an AuthZEN differential server exited before readiness" >&2; \
				tail -n 100 "$$phase6_tmp/go.log" "$$phase6_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "AuthZEN server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase6_tmp/go.log" "$$phase6_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	$(CARGO) run --quiet -p openfga-server -- differential-authzen \
		--go-url "http://$(GO_HTTP_ADDR)/" --rust-url "http://$(RUST_HTTP_ADDR)/"

authzen-conformance: authzen-baseline build
	@set -eu; \
	phase6_tmp=$$(mktemp -d); \
	repository_root=$$(pwd); \
	corpus_dir=$$(mktemp -d "$$repository_root/vendors/openfga/.authzen-external.XXXXXX"); \
	rust_pid=""; \
	disabled_rust_pid=""; \
	cleanup() { \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$disabled_rust_pid" || kill "$$disabled_rust_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		test -z "$$disabled_rust_pid" || wait "$$disabled_rust_pid" 2>/dev/null || true; \
		case "$$corpus_dir" in "$$repository_root"/vendors/openfga/.authzen-external.*) rm -rf "$$corpus_dir" ;; *) exit 1 ;; esac; \
		rm -rf "$$phase6_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	cp vendors/openfga/tests/authzen/*.go "$$corpus_dir/"; \
	patch -d "$$corpus_dir" -p1 < tests/conformance/authzen-external-server.patch; \
	token_key=$$(openssl rand -base64 32); \
	OPENFGA__LISTENERS__HTTP=$(RUST_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_GRPC_ADDR) \
	OPENFGA__AUTHZEN__ENABLED=true \
	OPENFGA__AUTHZEN__BASE_URL="http://openfga.example" \
	OPENFGA_TOKEN_KEY="$$token_key" \
	$(CARGO) run --quiet -p openfga-server -- run \
		--config config/openfga-development.yaml \
		>"$$phase6_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	OPENFGA__LISTENERS__HTTP=$(RUST_AUTHZEN_DISABLED_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_AUTHZEN_DISABLED_GRPC_ADDR) \
	OPENFGA__AUTHZEN__ENABLED=false \
	OPENFGA_TOKEN_KEY="$$token_key" \
	$(CARGO) run --quiet -p openfga-server -- run \
		--config config/openfga-development.yaml \
		>"$$phase6_tmp/rust-disabled.log" 2>&1 & disabled_rust_pid=$$!; \
	for endpoint in "http://$(RUST_HTTP_ADDR)/readyz" "http://$(RUST_AUTHZEN_DISABLED_HTTP_ADDR)/readyz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$rust_pid" 2>/dev/null || ! kill -0 "$$disabled_rust_pid" 2>/dev/null; then \
				echo "a Rust AuthZEN conformance server exited before readiness" >&2; \
				tail -n 100 "$$phase6_tmp/rust.log" "$$phase6_tmp/rust-disabled.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge "$(SERVER_READINESS_ATTEMPTS)"; then \
				echo "Rust AuthZEN conformance server did not become ready: $$endpoint" >&2; \
				tail -n 100 "$$phase6_tmp/rust.log" "$$phase6_tmp/rust-disabled.log" >&2; \
				exit 1; \
			fi; \
			sleep 0.1; \
		done; \
	done; \
	corpus_package="./$$(basename "$$corpus_dir")"; \
	cd vendors/openfga && \
	OPENFGA_AUTHZEN_TEST_GRPC_ADDR=$(RUST_GRPC_ADDR) \
	OPENFGA_AUTHZEN_TEST_HTTP_ADDR=$(RUST_HTTP_ADDR) \
	OPENFGA_AUTHZEN_DISABLED_TEST_GRPC_ADDR=$(RUST_AUTHZEN_DISABLED_GRPC_ADDR) \
	OPENFGA_AUTHZEN_DISABLED_TEST_HTTP_ADDR=$(RUST_AUTHZEN_DISABLED_HTTP_ADDR) \
	GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test "$$corpus_package" -count=1

phase4-scale: $(GO_BASELINE) build-release
	@set -eu; \
	phase4_tmp=$$(mktemp -d); \
	go_pid=""; rust_pid=""; writer_pid=""; sampler_pid=""; \
	cleanup() { \
		phase4_status=$$?; \
		test -z "$$sampler_pid" || kill "$$sampler_pid" 2>/dev/null || true; \
		test -z "$$sampler_pid" || wait "$$sampler_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || kill "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || kill "$$rust_pid" 2>/dev/null || true; \
		test -z "$$writer_pid" || kill "$$writer_pid" 2>/dev/null || true; \
		test -z "$$go_pid" || wait "$$go_pid" 2>/dev/null || true; \
		test -z "$$rust_pid" || wait "$$rust_pid" 2>/dev/null || true; \
		test -z "$$writer_pid" || wait "$$writer_pid" 2>/dev/null || true; \
		if test "$$phase4_status" -ne 0; then \
			tail -n 100 "$$phase4_tmp/go.log" "$$phase4_tmp/rust.log" "$$phase4_tmp/writer.log" 2>/dev/null || true; \
		fi; \
		rm -rf "$$phase4_tmp"; \
		return "$$phase4_status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	target_dir=$$($(CARGO) metadata --no-deps --format-version 1 | \
		sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'); \
	test -n "$$target_dir"; \
	server_binary="$$target_dir/release/openfga-server"; \
	test -x "$$server_binary"; \
	$(GO_BASELINE) run --http-addr $(GO_HTTP_ADDR) --grpc-addr $(GO_GRPC_ADDR) \
		--playground-enabled=false >"$$phase4_tmp/go.log" 2>&1 & go_pid=$$!; \
	OPENFGA__LISTENERS__HTTP=$(RUST_HTTP_ADDR) \
	OPENFGA__LISTENERS__GRPC=$(RUST_GRPC_ADDR) \
	OPENFGA__STORAGE__BACKEND=$(PHASE4_STORAGE_BACKEND) \
	OPENFGA__STORAGE__POSTGRES__MIGRATE_ON_START=$(PHASE4_POSTGRES_MIGRATE) \
	OPENFGA__TRANSPORT__ADMISSION__AUTHENTICATION_ATTEMPTS=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__GLOBAL_AUTHENTICATION_ATTEMPTS=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__ADMINISTRATION=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__READS=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__WRITES=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__CHECKS=1000000 \
	OPENFGA__TRANSPORT__ADMISSION__ENUMERATION=1000000 \
	OPENFGA__TRANSPORT__MAXIMUM_CONCURRENCY=$(PHASE4_MAXIMUM_CONCURRENCY) \
	OPENFGA_DATABASE_URL="$(POSTGRES_TEST_URL)" \
	OPENFGA_TOKEN_KEY=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
	"$$server_binary" run --config config/openfga-development.yaml \
		>"$$phase4_tmp/rust.log" 2>&1 & rust_pid=$$!; \
	for endpoint in "http://$(GO_HTTP_ADDR)/healthz" "http://$(RUST_HTTP_ADDR)/readyz"; do \
		attempt=0; \
		until curl --fail --silent "$$endpoint" >/dev/null; do \
			if ! kill -0 "$$go_pid" 2>/dev/null || ! kill -0 "$$rust_pid" 2>/dev/null; then \
				echo "a Phase 4 scale server exited before readiness" >&2; \
				tail -n 100 "$$phase4_tmp/go.log" "$$phase4_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			test "$$attempt" -lt "$(SERVER_READINESS_ATTEMPTS)" || { echo "Phase 4 server did not become ready: $$endpoint" >&2; exit 1; }; \
			sleep 0.1; \
		done; \
	done; \
	writer_arg=""; \
	if test "$(PHASE4_STORAGE_BACKEND)" = postgres; then \
		OPENFGA__LISTENERS__HTTP=$(RUST_WRITER_HTTP_ADDR) \
		OPENFGA__LISTENERS__GRPC=$(RUST_WRITER_GRPC_ADDR) \
		OPENFGA__STORAGE__BACKEND=postgres \
		OPENFGA__STORAGE__POSTGRES__MIGRATE_ON_START=false \
		OPENFGA__TRANSPORT__ADMISSION__AUTHENTICATION_ATTEMPTS=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__GLOBAL_AUTHENTICATION_ATTEMPTS=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__ADMINISTRATION=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__READS=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__WRITES=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__CHECKS=1000000 \
		OPENFGA__TRANSPORT__ADMISSION__ENUMERATION=1000000 \
		OPENFGA__TRANSPORT__MAXIMUM_CONCURRENCY=$(PHASE4_MAXIMUM_CONCURRENCY) \
		OPENFGA_DATABASE_URL="$(POSTGRES_TEST_URL)" \
		OPENFGA_TOKEN_KEY=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
		"$$server_binary" run --config config/openfga-development.yaml \
			>"$$phase4_tmp/writer.log" 2>&1 & writer_pid=$$!; \
		attempt=0; \
		until curl --fail --silent "http://$(RUST_WRITER_HTTP_ADDR)/readyz" >/dev/null; do \
			if ! kill -0 "$$writer_pid" 2>/dev/null; then \
				echo "the Phase 4 writer server exited before readiness" >&2; \
				tail -n 100 "$$phase4_tmp/writer.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			test "$$attempt" -lt "$(SERVER_READINESS_ATTEMPTS)" || { echo "Phase 4 writer did not become ready" >&2; exit 1; }; \
			sleep 0.1; \
		done; \
		writer_arg="--writer-url http://$(RUST_WRITER_HTTP_ADDR)/"; \
	fi; \
	mkdir -p "$(PHASE4_ARTIFACT_DIR)"; \
	"$$server_binary" phase4-consistency-faults \
		--rust-url "http://$(RUST_HTTP_ADDR)/" \
		--iterations "$(PHASE4_CONSISTENCY_ITERATIONS)" $$writer_arg \
		>"$(PHASE4_ARTIFACT_DIR)/consistency.json"; \
	"$$server_binary" phase4-reference-benchmark \
		--go-url "http://$(GO_HTTP_ADDR)/" \
		--rust-url "http://$(RUST_HTTP_ADDR)/" \
		--go-pid "$$go_pid" \
		--rust-pid "$$rust_pid" \
		--requests-per-client "$(PHASE4_BENCH_REQUESTS)" \
		--mutation-requests-per-client "$(PHASE4_BENCH_MUTATION_REQUESTS)" \
		>"$(PHASE4_ARTIFACT_DIR)/reference-benchmark.json"; \
	resource_samples="$$phase4_tmp/process-resources.tsv"; \
	process_threads() { \
		if test "$$(uname -s)" = Darwin; then \
			ps -M -p "$$rust_pid" | awk 'NR > 1 { count += 1 } END { print count }'; \
		else \
			ps -o nlwp= -p "$$rust_pid" | tr -d ' '; \
		fi; \
	}; \
	sample_once() { \
		rss=$$(ps -o rss= -p "$$rust_pid" | tr -d ' '); \
		threads=$$(process_threads); \
		test -n "$$rss" && test -n "$$threads"; \
		printf '%s %s\n' "$$rss" "$$threads"; \
	}; \
	sample_process() { \
		while kill -0 "$$rust_pid" 2>/dev/null; do \
			sample_once >>"$$resource_samples"; \
			sleep 1; \
			done; \
	}; \
	sample_once >"$$resource_samples"; \
	sample_process & sampler_pid=$$!; \
	read rss_before threads_before <"$$resource_samples"; \
	"$$server_binary" phase4-soak \
		--rust-url "http://$(RUST_HTTP_ADDR)/" \
		--seconds "$(PHASE4_SOAK_SECONDS)" \
		--clients "$(PHASE4_SOAK_CLIENTS)" $(PHASE4_SOAK_CONSISTENCY_ARG) \
		>"$(PHASE4_ARTIFACT_DIR)/soak.json"; \
	sample_once >>"$$resource_samples"; \
	kill "$$sampler_pid" 2>/dev/null || true; \
	wait "$$sampler_pid" 2>/dev/null || true; \
	sampler_pid=""; \
	rss_peak=$$(awk 'BEGIN { peak = 0 } { if ($$1 > peak) peak = $$1 } END { print peak }' "$$resource_samples"); \
	threads_peak=$$(awk 'BEGIN { peak = 0 } { if ($$2 > peak) peak = $$2 } END { print peak }' "$$resource_samples"); \
	set -- $$(tail -1 "$$resource_samples"); \
	rss_post_drain=$$1; threads_post_drain=$$2; \
	rss_peak_growth=$$((rss_peak > rss_before ? rss_peak - rss_before : 0)); \
	rss_post_drain_growth=$$((rss_post_drain > rss_before ? rss_post_drain - rss_before : 0)); \
	test "$$rss_peak_growth" -le "$(PHASE4_RSS_GROWTH_KIB)" || { \
		echo "Rust peak RSS grew by $$rss_peak_growth KiB, above $(PHASE4_RSS_GROWTH_KIB) KiB" >&2; \
		exit 1; \
	}; \
	test "$$rss_post_drain_growth" -le "$(PHASE4_RSS_GROWTH_KIB)" || { \
		echo "Rust post-drain RSS grew by $$rss_post_drain_growth KiB, above $(PHASE4_RSS_GROWTH_KIB) KiB" >&2; \
		exit 1; \
	}; \
	printf '{"sampleIntervalSeconds":1,"rssBaselineKiB":%s,"rssPeakKiB":%s,"rssPostDrainKiB":%s,"rssPeakGrowthKiB":%s,"rssPostDrainGrowthKiB":%s,"maximumGrowthKiB":%s,"threadsBaseline":%s,"threadsPeak":%s,"threadsPostDrain":%s}\n' \
		"$$rss_before" "$$rss_peak" "$$rss_post_drain" "$$rss_peak_growth" \
		"$$rss_post_drain_growth" "$(PHASE4_RSS_GROWTH_KIB)" "$$threads_before" \
		"$$threads_peak" "$$threads_post_drain" \
		>"$(PHASE4_ARTIFACT_DIR)/memory.json"; \
	kill -TERM "$$rust_pid"; \
	wait "$$rust_pid"; \
	rust_pid=""; \
	$(CARGO) test -p openfga-cache --all-targets; \
	$(CARGO) test -p openfga-server \
		'runtime::tests::test_should_bound_shutdown_with_an_in_flight_client'; \
	$(MAKE) phase4-criterion; \
	echo "Phase 4 artifacts: $(PHASE4_ARTIFACT_DIR)"

phase4-scale-smoke:
	@$(MAKE) phase4-scale \
		PHASE4_ARTIFACT_DIR=target/phase4-smoke \
		PHASE4_BENCH_REQUESTS=5 \
		PHASE4_CONSISTENCY_ITERATIONS=8 \
		PHASE4_SOAK_CLIENTS=16 \
		PHASE4_SOAK_SECONDS=5

phase4-postgres-scale-smoke:
	@test -n "$(POSTGRES_TEST_URL)" || { echo "POSTGRES_TEST_URL is required" >&2; exit 1; }
	@$(MAKE) phase4-scale \
		PHASE4_ARTIFACT_DIR=target/phase4-postgres \
		PHASE4_BENCH_REQUESTS=5 \
		PHASE4_CONSISTENCY_ITERATIONS=8 \
		PHASE4_SOAK_CLIENTS=16 \
		PHASE4_SOAK_SECONDS=30 \
		PHASE4_MAXIMUM_CONCURRENCY=64 \
		PHASE4_STORAGE_BACKEND=postgres \
		PHASE4_POSTGRES_MIGRATE=true \
		PHASE4_SOAK_CONSISTENCY_ARG=--higher-consistency

phase4-local-postgres-scale-smoke:
	@set -eu; \
	phase4_pg_tmp=$$(mktemp -d); \
	postgres_started=false; \
	cleanup() { \
		if test "$$postgres_started" = true; then \
			pg_ctl -D "$$phase4_pg_tmp/data" -m fast -w stop >/dev/null; \
		fi; \
		rm -rf "$$phase4_pg_tmp"; \
	}; \
	trap cleanup EXIT INT TERM; \
	initdb -D "$$phase4_pg_tmp/data" --auth=trust --no-instructions >/dev/null; \
	pg_ctl -D "$$phase4_pg_tmp/data" \
		-o "-h 127.0.0.1 -p $(PHASE4_POSTGRES_PORT)" -w start >/dev/null; \
	postgres_started=true; \
	postgres_url="postgresql://$$(id -un)@127.0.0.1:$(PHASE4_POSTGRES_PORT)/postgres?sslmode=disable"; \
	$(MAKE) postgres-storage POSTGRES_TEST_URL="$$postgres_url"; \
	$(MAKE) sqlx-prepare-check POSTGRES_TEST_URL="$$postgres_url"; \
	$(MAKE) phase2-compatibility \
		COMPAT_STORAGE_BACKEND=postgres \
		COMPAT_DATABASE_URL="$$postgres_url"; \
	$(MAKE) phase4-postgres-scale-smoke \
		POSTGRES_TEST_URL="$$postgres_url"

check-corpus-differential: verify-go-tool verify-go-pin
	@phase1_tmp=$$(mktemp -d); \
	repo_root=$$(pwd -P); \
	cleanup() { rm -rf "$$phase1_tmp"; }; \
	trap cleanup EXIT INT TERM; \
	case "$$repo_root" in *\\*|*\"*) echo "repository path cannot be encoded in the Go overlay" >&2; exit 1 ;; esac; \
	printf '{"Replace":{"%s":"%s"}}\n' \
		"$$repo_root/vendors/openfga/tests/check/export_test.go" \
		"$$repo_root/tests/check-corpus-overlay/export_test.go" \
		>"$$phase1_tmp/overlay.json"; \
	cd vendors/openfga && \
		OPENFGA_CHECK_CORPUS_OUTPUT="$$phase1_tmp/corpus.json" \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test \
		-overlay="$$phase1_tmp/overlay.json" ./tests/check \
		-run '^TestExportCheckCorpus$$' -count=1 && \
	cd "$$repo_root" && \
		$(CARGO) run --quiet -p openfga-server -- differential-check-corpus \
		--corpus "$$phase1_tmp/corpus.json"

check-spike: check-baseline check-oracle check-differential check-corpus-differential

listobjects-spike: verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test \
		./pkg/server/commands -run '^$$' -bench '^BenchmarkListObjects$$' \
		-benchtime=1x -count=1

conformance: cel-spike check-spike listobjects-spike model-spike storage-contract

fuzz-domain:
	@phase1_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase1_tmp"' EXIT; \
	cp -R fuzz/corpus/domain_inputs "$$phase1_tmp/corpus"; \
	$(CARGO) +$(RUSTFMT_TOOLCHAIN) fuzz run domain_inputs "$$phase1_tmp/corpus" -- \
		-max_total_time=$(FUZZ_TIME) -max_len=8192

fuzz-condition:
	@phase1_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase1_tmp"' EXIT; \
	cp -R fuzz/corpus/condition_inputs "$$phase1_tmp/corpus"; \
	$(CARGO) +$(RUSTFMT_TOOLCHAIN) fuzz run condition_inputs "$$phase1_tmp/corpus" -- \
		-max_total_time=$(FUZZ_TIME) -max_len=8192

fuzz-model:
	@phase1_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase1_tmp"' EXIT; \
	cp -R fuzz/corpus/model_inputs "$$phase1_tmp/corpus"; \
	$(CARGO) +$(RUSTFMT_TOOLCHAIN) fuzz run model_inputs "$$phase1_tmp/corpus" -- \
		-max_total_time=$(FUZZ_TIME) -max_len=8192

audit:
	@$(CARGO) audit
	@$(CARGO) audit --file fuzz/Cargo.lock

deny:
	@$(CARGO) deny check
	@$(CARGO) deny --manifest-path fuzz/Cargo.toml check

$(GITLEAKS_TOOL):
	@phase5_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase5_tmp"' EXIT; \
	case "$$(uname -s)-$$(uname -m)" in \
		Darwin-arm64) platform=darwin_arm64; sha=b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5 ;; \
		Darwin-x86_64) platform=darwin_x64; sha=dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709 ;; \
		Linux-aarch64) platform=linux_arm64; sha=e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080 ;; \
		Linux-x86_64) platform=linux_x64; sha=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb ;; \
		*) echo "unsupported gitleaks platform: $$(uname -s)-$$(uname -m)" >&2; exit 1 ;; \
	esac; \
	archive="gitleaks_$(GITLEAKS_VERSION)_$$platform.tar.gz"; \
	curl --fail --location --silent --show-error \
		"https://github.com/gitleaks/gitleaks/releases/download/v$(GITLEAKS_VERSION)/$$archive" \
		--output "$$phase5_tmp/$$archive"; \
	if command -v shasum >/dev/null 2>&1; then \
		actual_sha=$$(shasum -a 256 "$$phase5_tmp/$$archive" | awk '{print $$1}'); \
	else \
		actual_sha=$$(sha256sum "$$phase5_tmp/$$archive" | awk '{print $$1}'); \
	fi; \
	test "$$actual_sha" = "$$sha"; \
	mkdir -p "$(@D)"; \
	tar -xzf "$$phase5_tmp/$$archive" -C "$(@D)" gitleaks

$(SYFT_TOOL):
	@phase5_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase5_tmp"' EXIT; \
	case "$$(uname -s)-$$(uname -m)" in \
		Darwin-arm64) platform=darwin_arm64; sha=e32fdb9d47823fa633748a1efca2528fd77c37469ea93c9e40ab835da44e4cce ;; \
		Darwin-x86_64) platform=darwin_amd64; sha=d11a8c7bc27114853bd7c1e1b2f3be3ddda3a1de17aee585329f04c369341c75 ;; \
		Linux-aarch64) platform=linux_arm64; sha=887c57cbcc2d0e8c5c110a4571a3fc7150058b24d74f993ee4663516e5c8ce86 ;; \
		Linux-x86_64) platform=linux_amd64; sha=bf7b29ff57f06da30918266a0e1c2885a8f99784798d1bdb1628886aa015d788 ;; \
		*) echo "unsupported syft platform: $$(uname -s)-$$(uname -m)" >&2; exit 1 ;; \
	esac; \
	archive="syft_$(SYFT_VERSION)_$$platform.tar.gz"; \
	curl --fail --location --silent --show-error \
		"https://github.com/anchore/syft/releases/download/v$(SYFT_VERSION)/$$archive" \
		--output "$$phase5_tmp/$$archive"; \
	if command -v shasum >/dev/null 2>&1; then \
		actual_sha=$$(shasum -a 256 "$$phase5_tmp/$$archive" | awk '{print $$1}'); \
	else \
		actual_sha=$$(sha256sum "$$phase5_tmp/$$archive" | awk '{print $$1}'); \
	fi; \
	test "$$actual_sha" = "$$sha"; \
	mkdir -p "$(@D)"; \
	tar -xzf "$$phase5_tmp/$$archive" -C "$(@D)" syft

secret-scan: $(GITLEAKS_TOOL)
	@$(GITLEAKS_TOOL) git --no-banner --redact --verbose .
	@$(GITLEAKS_TOOL) dir --no-banner --redact --verbose .

sbom: build-release $(SYFT_TOOL)
	@set -eu; \
	phase5_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase5_tmp"' EXIT; \
	mkdir -p "$(PHASE5_ARTIFACT_DIR)"; \
	test -x "$(WORKSPACE_TARGET_DIR)/release/openfga-server"; \
	cp "$(WORKSPACE_TARGET_DIR)/release/openfga-server" Cargo.lock Cargo.toml LICENSE NOTICE \
		"$$phase5_tmp/"; \
	$(SYFT_TOOL) scan "dir:$$phase5_tmp" \
		--source-name openfga-rs --source-version "$$(git rev-parse HEAD)" \
		-o cyclonedx-json="$(PHASE5_ARTIFACT_DIR)/openfga-rs-$(PHASE5_PLATFORM).cdx.json" \
		-o spdx-json="$(PHASE5_ARTIFACT_DIR)/openfga-rs-$(PHASE5_PLATFORM).spdx.json"

release-artifacts: build-release sbom
	@set -eu; \
	artifact="openfga-server-$(PHASE5_PLATFORM).tar.gz"; \
	test -n "$(WORKSPACE_TARGET_DIR)"; \
	tar -czf "$(PHASE5_ARTIFACT_DIR)/$$artifact" \
		-C "$(WORKSPACE_TARGET_DIR)/release" openfga-server \
		-C "$(CURDIR)" LICENSE NOTICE README.md \
		config/openfga-development.yaml config/openfga-preshared-development.yaml; \
	for required in openfga-server LICENSE NOTICE README.md \
		config/openfga-development.yaml config/openfga-preshared-development.yaml; do \
		tar -tzf "$(PHASE5_ARTIFACT_DIR)/$$artifact" | grep -Fx "$$required" >/dev/null; \
	done; \
	cd "$(PHASE5_ARTIFACT_DIR)"; \
	if command -v shasum >/dev/null 2>&1; then \
		shasum -a 256 "$$artifact" openfga-rs-$(PHASE5_PLATFORM).cdx.json \
			openfga-rs-$(PHASE5_PLATFORM).spdx.json > SHA256SUMS-$(PHASE5_PLATFORM); \
	else \
		sha256sum "$$artifact" openfga-rs-$(PHASE5_PLATFORM).cdx.json \
			openfga-rs-$(PHASE5_PLATFORM).spdx.json > SHA256SUMS-$(PHASE5_PLATFORM); \
	fi

security: audit deny secret-scan sbom

phase5-ga-evidence: differential-smoke check-spike enumeration-differential security \
	release-artifacts phase4-scale-smoke

phase5-release-gate: check clippy-strict sqlite-storage upstream-migration-drill \
	phase5-sqlite-compatibility phase5-ga-evidence
	@test -n "$(POSTGRES_TEST_URL)" || { echo "POSTGRES_TEST_URL is required" >&2; exit 1; }
	@test -n "$(MYSQL_TEST_URL)" || { echo "MYSQL_TEST_URL is required" >&2; exit 1; }
	@$(MAKE) postgres-storage phase2-compatibility \
		POSTGRES_TEST_URL="$(POSTGRES_TEST_URL)"
	@$(MAKE) mysql-storage phase5-mysql-compatibility \
		MYSQL_TEST_URL="$(MYSQL_TEST_URL)"

check-agent-sync:
	@cmp -s CLAUDE.md AGENTS.md || { \
		echo "AGENTS.md must stay in sync with CLAUDE.md"; \
		echo "Update both files with the same shared project instructions."; \
		exit 1; \
	}
	@phase0_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase0_tmp"' EXIT; \
	cp -R .claude/skills "$$phase0_tmp/expected-skills"; \
	find "$$phase0_tmp/expected-skills" -name SKILL.md -exec perl -0pi -e \
		's/CLAUDE\.md/AGENTS.md/g; s/Claude/Codex/g; s/claude/codex/g' {} +; \
	diff -ru --exclude agents "$$phase0_tmp/expected-skills" .agents/skills || { \
		echo "Codex skills must stay in sync with Claude skills after Claude-to-Codex renaming."; \
		echo "Update .claude/skills first, then mirror the shared content into .agents/skills."; \
		exit 1; \
	}

release:
	@cargo release tag --execute
	@git cliff -o CHANGELOG.md
	@git commit -a -n -m "Update CHANGELOG.md" || true
	@git push origin master
	@cargo release push --execute

update-submodule:
	@git submodule update --init --recursive --remote

.PHONY: audit build cel-baseline cel-spike check check-actions check-agent-sync check-baseline check-corpus-differential check-differential check-docs check-fuzz \
	check-oracle check-proto check-spike \
	clippy clippy-strict \
	conformance deny differential-smoke doc enumeration-differential fmt fuzz-condition fuzz-domain fuzz-model go-baseline listobjects-spike model-baseline \
	model-spike mysql-storage phase2-compatibility phase4-criterion phase5-mysql-compatibility \
	phase5-ga-evidence phase5-release-gate phase5-sqlite-compatibility postgres-storage proto release release-artifacts \
	sbom secret-scan security sqlite-storage sqlx-prepare-check storage-contract test \
	update-submodule upstream-migration upstream-migration-drill verify-go-pin verify-go-tool
