CARGO ?= cargo
RUSTFMT_TOOLCHAIN := nightly-2026-07-29
GO_VERSION := 1.26.5
GO_TOOL := .tools/go/bin/go
GO_BASELINE_COMMIT := 4e4f79ed841513dfd61746a75ef473f6198299f7
GO_BASELINE := .tools/openfga-go-$(GO_BASELINE_COMMIT)
GO_HTTP_ADDR ?= 127.0.0.1:18080
GO_GRPC_ADDR ?= 127.0.0.1:18082
RUST_PROBE_ADDR ?= 127.0.0.1:18081
FUZZ_TIME ?= 15
PROTO_OUTPUT := crates/openfga-proto/src/generated

build:
	@$(CARGO) build --workspace --all-targets

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

doc:
	@RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps

check: check-proto check-docs build test fmt clippy doc

check-docs:
	@$(CARGO) run --quiet -p openfga-doc-check

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
				tail -100 "$$phase0_tmp/go.log" "$$phase0_tmp/rust.log" >&2; \
				exit 1; \
			fi; \
			attempt=$$((attempt + 1)); \
			if test "$$attempt" -ge 100; then \
				echo "server did not become ready: $$endpoint" >&2; \
				tail -100 "$$phase0_tmp/go.log" "$$phase0_tmp/rust.log" >&2; \
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

cel-baseline: verify-go-tool verify-go-pin
	@cd tests/cel-baseline-go && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test ./... && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) run . \
			../cel-conformance/cases.json

cel-spike: cel-baseline
	@$(CARGO) test -p openfga-condition --test phase0_candidate

listobjects-spike: verify-go-tool verify-go-pin
	@cd vendors/openfga && \
		GOTOOLCHAIN=local GOFLAGS=-mod=readonly ../../$(GO_TOOL) test \
		./pkg/server/commands -run '^$$' -bench '^BenchmarkListObjects$$' \
		-benchtime=1x -count=1

conformance: cel-spike listobjects-spike

fuzz-domain:
	@phase1_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$phase1_tmp"' EXIT; \
	cp -R fuzz/corpus/domain_inputs "$$phase1_tmp/corpus"; \
	$(CARGO) +$(RUSTFMT_TOOLCHAIN) fuzz run domain_inputs "$$phase1_tmp/corpus" -- \
		-max_total_time=$(FUZZ_TIME) -max_len=8192

audit:
	@$(CARGO) audit

deny:
	@$(CARGO) deny check

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

.PHONY: audit build cel-baseline cel-spike check check-agent-sync check-docs check-proto \
	clippy clippy-strict \
	conformance deny differential-smoke doc fmt fuzz-domain go-baseline listobjects-spike proto release test \
	update-submodule verify-go-pin verify-go-tool
