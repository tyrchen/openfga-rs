# Authorization-model compiler design

Status: Proposed · Depends on: [`10-domain-model-design.md`](10-domain-model-design.md), [`11-condition-engine-design.md`](11-condition-engine-design.md)

## Contract

`ModelCompiler::compile(source) -> Result<Arc<CompiledModel>, ModelErrors>` is deterministic, side-effect free, and bounded by `ModelLimits`. Publication persists a model only after successful compilation. Loading legacy stored models compiles before caching or serving; invalid stored data becomes a typed integrity error.

Errors contain stable codes, declaration paths, and safe messages, sorted by source position then code. Validation may collect independent errors up to a configured cap, then returns `TooManyModelErrors`.

## Compilation passes

1. Validate schema version, model/type/relation/condition counts, byte limits, uniqueness, and reserved names.
2. Intern type, relation, condition, and rewrite-node names into dense IDs using deterministic source order.
3. Resolve every reference; reject undefined relations/conditions/types and illegal self references.
4. Validate rewrites: direct restrictions, nonempty operators, assignability, public access, and entrypoints.
5. Validate TTU: tupleset is directly assignable, target types are known, and computed relation exists on every permitted target type.
6. Compile/type-check all CEL conditions and require Boolean output.
7. Detect forbidden model cycles and annotate legal recursive/cyclic query paths without recursing on the Rust stack.
8. Normalize the rewrite DAG, deduplicate structurally equal immutable nodes where safe, and compute relation metadata.
9. Build forward/reverse relationship graphs, reachable subject types, wildcard reachability, TTU targets, recursion groups, and conservative enumeration edges.
10. Compute a canonical model fingerprint covering every semantic field.

All graph passes use iterative worklists with explicit depth/node budgets. Hash iteration order MUST NOT influence errors, fingerprints, serialized diagnostics, or evaluation plans.

## Compiled representation

`CompiledModel` owns:

- explicit `StoreId`, `AuthorizationModelId`, schema version, and fingerprint;
- intern tables and lookup maps;
- compact immutable rewrite nodes and per-relation roots;
- validated relation type restrictions;
- `Arc<dyn CompiledCondition>` handles;
- reachability bitsets/sets, wildcard paths, recursion groups, and reverse-enumeration metadata;
- a compiler format version used to invalidate incompatible cached artifacts.

Public access is through query methods; internal maps are not exposed. The type is `Send + Sync + Debug`, with condition content redacted, and contains no datastore/cache/service handles.

## Publication and resolution

Model IDs are immutable. The write flow validates the store, allocates a monotonic ULID through an injected clock/ID source, compiles, persists the source and fingerprint, then caches the identical compiled `Arc`. A duplicate ID with different content is an integrity failure.

Latest-model resolution is an independently cached `(store -> model_id)` lookup with a short bounded TTL. Explicit historical reads never consult that alias. Publication invalidates/updates only the latest alias; immutable `(store, model)` cache entries need no invalidation.

## Acceptance criteria

- Vendored valid/invalid model fixtures produce compatible accept/reject outcomes and error categories.
- Compilation is deterministic across process runs and randomized map seeds.
- Adversarial wide/deep graphs terminate under configured budgets without stack overflow.
- Reachability never rejects a decision that the oracle evaluator would allow.
- Model publication cannot expose a partially persisted or uncompiled model.

## Engineering norms

Repository `AGENTS.md` sections **Error Handling**, **Type Design & API**, **Safety & Security**, **Serialization & Data**, **Testing**, **Logging & Observability**, **Performance**, **Documentation**, and **Code Style** bind compiler APIs and implementation. **Async & Concurrency** applies only to publication/resolution orchestration; compilation itself is synchronous, side-effect free, non-panicking, and moved to bounded blocking work if profiling shows it can stall the runtime.

## Cross-references

- ← Depends on: [`10-domain-model-design.md`](10-domain-model-design.md), [`11-condition-engine-design.md`](11-condition-engine-design.md)
- → Consumed by: [`14-check-engine-design.md`](14-check-engine-design.md), [`15-list-queries-design.md`](15-list-queries-design.md), [`16-cache-consistency-design.md`](16-cache-consistency-design.md)
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md)
- ↔ Prior art: deterministic validation in `vendors/openfga/pkg/typesystem/typesystem.go:1113` and weighted graph metadata in `vendors/openfga/internal/modelgraph/model.go:19`
