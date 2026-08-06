# Workspace and crate boundaries

Status: Proposed · Depends on: component designs 10–21

## Target workspace

```text
crates/
  openfga-domain          validated values, commands, limits, shared errors
  openfga-proto           pinned generated protobuf/gRPC only
  openfga-model           model source validation, compiler, graph/IR
  openfga-condition       project CEL interfaces and selected adapter
  openfga-storage         narrow traits, filters, pagination, contract suite
  openfga-storage-memory  actor-owned in-memory backend
  openfga-storage-sql     SQLx backends and migrations
  openfga-check           oracle and optimized Check strategies
  openfga-list            ListObjects/ListUsers/Expand engines
  openfga-cache           cache contracts, keys, controller actors
  openfga-auth            principals, OIDC/preshared authentication, policy
  openfga-service         use cases, orchestration, transport-neutral mapping
  openfga-transport       Tonic/Axum adapters and middleware
apps/
  openfga-server          config, composition, CLI, lifecycle
```

The existing placeholder `openfga-rs-core` is removed as responsibilities migrate; no compatibility/deprecation layer is needed before a published API exists.

## Dependency direction

`domain` depends only on foundational parsing/serialization/error crates. `proto` is independent generated wire code. `model` depends on domain and condition interfaces. `storage` depends on domain; backend crates depend on storage. `check` and `list` depend on domain/model/condition/storage traits, never concrete backends. `cache` depends on domain/model/storage changelog traits. `auth` depends on domain principals but not service/transport. `service` composes engine/storage/cache/auth capabilities. `transport` converts proto ↔ domain/service. Only the application chooses concrete implementations.

Cycles are forbidden and verified with workspace metadata. Lower crates cannot depend on Axum, Tonic, SQLx, Moka, or application configuration unless that framework is their explicit responsibility.

## API rules

- Public cross-crate types live at the owning semantic layer; no catch-all `common` crate.
- Domain commands never contain generated proto, SQL rows, CEL values, or HTTP types.
- Storage traits return domain/storage-owned values and streams, not SQLx types.
- Engine traits return semantic outcomes, not transport statuses.
- Constructors validate invariants and take specific dependencies; structures with more than five fields use typed builders.
- Traits are introduced at genuine substitution/test boundaries. Internal helpers remain concrete/generic.
- All library public structs are non-exhaustive where external construction/evolution requires it and implement safe `Debug`.

## Feature policy

Backend, transport, and telemetry selection occurs through application dependencies, not a combinatorial feature matrix in semantic crates. Features are additive and explicitly tested. TLS uses rustls/aws-lc only. Default workspace build includes the supported production server; minimal domain/model crates remain lightweight.

## Acceptance criteria

- Dependency graph follows the direction above with no cycles or framework leakage.
- Each crate has focused module docs, owner responsibilities, public error types, and relevant unit/contract tests.
- `cargo doc` has no public-item/lint failures and cross-crate examples compile.
- Removing a backend or transport crate does not change semantic engine compilation.
