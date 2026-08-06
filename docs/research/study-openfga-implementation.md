# Study: OpenFGA implementation architecture and authorization algorithms

Status: Done · Owner: openfga-rs · Date: 2026-08-05 · Vendor pin: `vendors/openfga` @ `4e4f79ed841513dfd61746a75ef473f6198299f7` (`v1.18.3-2-g4e4f79ed`)

## Why this study

This study answers: **which contracts, algorithms, and failure semantics must a Rust implementation preserve to be an OpenFGA-equivalent server, and which upstream implementation techniques should be adapted rather than copied?** It is the evidence base for the specs under `../../specs/`.

The pin is intentionally the vendored `main` revision requested for this project, not merely the latest release tag. At this revision OpenFGA contains a mature recursive Check engine and a feature-gated weighted-graph successor. That overlap is valuable: it exposes both the compatibility oracle and the direction of current optimization work.

## Architecture map

```text
 Clients / SDKs
   │ gRPC OpenFGAService + AuthZEN     │ HTTP/JSON gateway
   └──────────────────┬────────────────┘
                      ▼
 ┌───────────────────────────────────────────────────────────────────────┐
 │ Process boundary                                                      │
 │ recovery → request id → tracing → validation → authn → authz → timeout│
 │                                                                       │
 │ ┌─────────────────── Control plane ────────────────────────────────┐   │
 │ │ stores · immutable authorization models · assertions · changes  │   │
 │ │ model resolver → validated TypeSystem + compiled model graph    │   │
 │ └──────────────────────────┬───────────────────────────────────────┘   │
 │                            │                                           │
 │ ┌─────────────────── Query/data plane ─────────────────────────────┐   │
 │ │ Check / BatchCheck   ListObjects   ListUsers   Expand            │   │
 │ │   │ recursive/       │ reverse +   │ forward   │ tree            │   │
 │ │   │ weighted graph   │ candidate   │ expansion │ expansion       │   │
 │ │   └──────────────────┴──────┬──────┴───────────┘                 │   │
 │ │ contextual tuples + CEL condition evaluation                     │   │
 │ └─────────────────────────────┬─────────────────────────────────────┘   │
 │                               │ bounded readers / iterator cache       │
 │ ┌─────────────────────────────▼─────────────────────────────────────┐   │
 │ │ caches: model · typesystem/graph · query result · tuple iterator │   │
 │ │ cache controller reads changelog and writes invalidation markers │   │
 │ └─────────────────────────────┬─────────────────────────────────────┘   │
 └───────────────────────────────┼─────────────────────────────────────────┘
                                 ▼
               ┌──────────────────────────────────────┐
               │ OpenFGADatastore                     │
               │ memory | PostgreSQL | MySQL | SQLite │
               │ tuples · models · stores · assertions│
               │ changelog (same transaction as tuple)│
               └──────────────────────────────────────┘
```

The public server owns both OpenFGA and AuthZEN services and delegates storage through one aggregate interface (`vendors/openfga/pkg/server/server.go:165`, `vendors/openfga/pkg/storage/storage.go:407`). Clients may use gRPC or the HTTP gateway, while PostgreSQL, MySQL, and SQLite are the persistent backends (`vendors/openfga/docs/architecture/architecture.md:3`, `vendors/openfga/docs/architecture/architecture.md:7`, `vendors/openfga/docs/architecture/architecture.md:11`).

The logical split is stronger than the directory tree suggests. Store/model/assertion operations form a control plane. Check, BatchCheck, ListObjects, ListUsers, and Expand form a query plane over an immutable compiled model and mutable relationship tuples. Models are immutable by ID, which permits seven-day model/typesystem caches without invalidation (`vendors/openfga/docs/caching.md:18`, `vendors/openfga/docs/caching.md:24`).

## Hot path walkthrough: Check

```text
 Client              Server/API           Model/cache             Evaluator              Storage
   │                     │                     │                       │                      │
   │ 1. Check request ──▶│                     │                       │                      │
   │                     │ 2. validate/authz   │                       │                      │
   │                     │ 3. resolve model ──▶│ immutable graph       │                      │
   │                     │◀────────────────────│                       │                      │
   │                     │ 4. build request + contextual indexes ─────▶│                      │
   │                     │                     │                       │                      │
   │                     │                     │ 5. reachable? cache? │                      │
   │                     │                     │                       │ 6. tuple reads ─────▶│
   │                     │                     │                       │◀── streamed tuples ──│
   │                     │                     │                       │ 7. merge contextual   │
   │                     │                     │                       │    tuples; CEL filter │
   │                     │                     │                       │ 8. dispatch branches  │
   │                     │                     │                       │    bounded + cancel    │
   │                     │                     │                       │ 9. reduce allow/deny   │
   │                     │◀──────────────────── CheckResult ───────────│                      │
   │◀── allowed/error ───│ emit query/dispatch/item/duration metrics  │                      │
```

1. The handler validates the generated protobuf request, attaches telemetry, performs per-store authorization, resolves the requested or latest model, and creates a resolver chain (`vendors/openfga/pkg/server/check.go:36`, `vendors/openfga/pkg/server/check.go:51`, `vendors/openfga/pkg/server/check.go:62`, `vendors/openfga/pkg/server/check.go:165`, `vendors/openfga/pkg/server/check.go:171`). The weighted engine is feature-gated and falls back to v1 on non-terminal errors to preserve compatibility (`vendors/openfga/pkg/server/check.go:77`, `vendors/openfga/pkg/server/check.go:80`, `vendors/openfga/pkg/server/check.go:150`).

2. Request construction validates the target tuple and every contextual tuple against the graph, derives object/user types, computes a context-sensitive invariant cache hash, and builds two sorted contextual-tuple indexes—by user and by object (`vendors/openfga/internal/check/request.go:54`, `vendors/openfga/internal/check/request.go:72`, `vendors/openfga/internal/check/request.go:106`, `vendors/openfga/internal/check/request.go:154`, `vendors/openfga/internal/check/request.go:176`). This is allocation work paid once per root request and shared by child requests.

3. The weighted resolver rejects unreachable user types without storage I/O, rejects unreachable typed wildcards, then resolves the root as a union (`vendors/openfga/internal/check/check.go:114`, `vendors/openfga/internal/check/check.go:124`, `vendors/openfga/internal/check/check.go:135`, `vendors/openfga/internal/check/check.go:144`). It flattens computed/union edges to reduce intermediate dispatches (`vendors/openfga/internal/check/check.go:345`, `vendors/openfga/internal/modelgraph/model.go:77`).

4. Union edges run under a configured concurrency limit. An allow short-circuits and cancels siblings; errors matter only when no branch allows. The resolver always waits for spawned work during teardown, avoiding task leaks (`vendors/openfga/internal/check/check.go:202`, `vendors/openfga/internal/check/check.go:216`, `vendors/openfga/internal/check/check.go:220`, `vendors/openfga/internal/check/check.go:223`, `vendors/openfga/internal/check/check.go:297`, `vendors/openfga/internal/check/check.go:309`). The mature engine follows the same essential pattern for userset/TTU dispatch: producer and bounded consumer, cancellation on a decisive result, and a final wait (`vendors/openfga/internal/graph/default_resolver.go:63`, `vendors/openfga/internal/graph/default_resolver.go:71`, `vendors/openfga/internal/graph/default_resolver.go:190`, `vendors/openfga/internal/graph/default_resolver.go:224`).

5. Direct edges use exact tuple reads and userset iterators. Tuple-to-userset (TTU) first reads the tupleset relation, then checks the computed relation on each referenced object (`vendors/openfga/docs/check/README.md:106`, `vendors/openfga/docs/check/README.md:134`, `vendors/openfga/docs/check/README.md:207`). Contextual tuples are concatenated after storage lookup and before condition filtering, so they affect decisions without polluting the datastore iterator cache (`vendors/openfga/internal/check/check.go:1120`, `vendors/openfga/internal/check/check.go:1127`, `vendors/openfga/internal/check/check.go:1138`).

6. Union is existential, intersection is universal, and difference is `base && !subtract`. Intersection and difference evaluate operands concurrently and cancel on a decisive result (`vendors/openfga/docs/check/README.md:400`, `vendors/openfga/docs/check/README.md:405`, `vendors/openfga/docs/check/README.md:459`, `vendors/openfga/docs/check/README.md:462`, `vendors/openfga/docs/check/README.md:464`). Correct error precedence is therefore part of the algorithm, not transport decoration.

7. Cycles are branch-path properties. The mature resolver clones the visited map when cloning a request (`vendors/openfga/internal/graph/resolve_check_request.go:119`, `vendors/openfga/internal/graph/resolve_check_request.go:143`); the weighted resolver initializes visited state only for recursive or tuple-cycle nodes (`vendors/openfga/internal/check/check.go:332`). A cycle contributes a false path rather than crashing or globally poisoning other branches (`vendors/openfga/docs/check/README.md:537`). Depth, breadth, datastore-read, and dispatch limits provide independent resource bounds; the server documents depth and breadth as separate controls (`vendors/openfga/pkg/server/server.go:326`, `vendors/openfga/pkg/server/server.go:400`, `vendors/openfga/pkg/server/config/config.go:25`).

### Allocations, locks, atomics, and async boundaries

- Root request construction allocates contextual-tuple maps and sorted slices; child requests share those maps and recompute only tuple-derived fields (`vendors/openfga/internal/check/request.go:182`, `vendors/openfga/internal/check/request.go:362`).
- Edge fan-out creates a bounded output channel and an `errgroup`; branch results and cache entries allocate response wrappers (`vendors/openfga/internal/check/check.go:218`, `vendors/openfga/internal/check/check.go:220`, `vendors/openfga/internal/check/check.go:271`).
- Dispatch/query/item counters are atomics shared by request clones (`vendors/openfga/internal/graph/resolve_check_request.go:119`, `vendors/openfga/internal/graph/resolve_check_request.go:123`).
- The adaptive planner uses concurrent maps and atomics, but its distribution state is stored through raw pointers and compare-and-swap (`vendors/openfga/internal/planner/plan.go:10`, `vendors/openfga/internal/planner/thompson.go:13`, `vendors/openfga/internal/planner/thompson.go:105`). That technique conflicts with this project's `#![forbid(unsafe_code)]` rule and must not be copied.
- Storage iterators own database rows and must be stopped on every exit (`vendors/openfga/pkg/storage/storage.go:158`). Cancellation without draining is a connection-pool leak risk; upstream tests explicitly exercise that behavior (`vendors/openfga/pkg/server/server_test.go:935`).

## Hot path walkthrough: ListObjects and ListUsers

### ListObjects

The stable ListObjects algorithm builds a reverse graph from the requested subject toward candidate objects. `GetPrunedRelationshipEdges` deliberately follows one operand through intersection/difference, marks candidates requiring further evaluation, and leaves final truth to Check (`vendors/openfga/internal/graph/graph.go:118`, `vendors/openfga/internal/graph/graph.go:131`, `vendors/openfga/internal/graph/graph.go:295`, `vendors/openfga/internal/graph/graph.go:327`). Reverse expansion uses the datastore's `ReadStartingWithUser` primitive (`vendors/openfga/pkg/storage/storage.go:193`) and emits candidates; ambiguous candidates are checked concurrently before output (`vendors/openfga/pkg/server/commands/list_objects.go:302`, `vendors/openfga/pkg/server/commands/list_objects.go:371`, `vendors/openfga/pkg/server/commands/list_objects.go:444`). Results stop at a configured deadline or maximum (`vendors/openfga/pkg/server/commands/list_objects.go:505`, `vendors/openfga/pkg/server/commands/list_objects.go:514`).

The newer pipeline compiles the weighted graph into a network of workers connected by bounded media. Terminal, union/basic, intersection, and difference workers have distinct reduction behavior (`vendors/openfga/internal/listobjects/pipeline/pipeline.go:120`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:152`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:188`). Construction prunes graph edges that cannot reach the subject, wires cycle groups, sizes a message pool, and starts one worker per reachable node (`vendors/openfga/internal/listobjects/pipeline/pipeline.go:297`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:313`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:333`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:364`, `vendors/openfga/internal/listobjects/pipeline/pipeline.go:395`). `Close` cancels, drains, joins all workers, and preserves the first meaningful error (`vendors/openfga/internal/listobjects/pipeline/pipeline.go:479`).

### ListUsers

ListUsers is a forward expansion from a concrete object/relation. Direct tuples are condition-filtered and either emitted or recursively dispatched through a userset (`vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:453`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:467`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:500`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:531`). Intersection counts a subject once per operand while treating a typed wildcard as satisfying its operand; exclusion tracks explicit negation and wildcard cases (`vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:564`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:600`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:647`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:752`). Depth and branch-local visited usersets bound recursion (`vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:339`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:359`, `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:968`).

These APIs are not interchangeable implementations of Check. They enumerate sets, must deduplicate, and have typed-wildcard semantics. A Rust port should share the compiled model and tuple primitives but give enumeration its own algorithms and conformance suite.

## Key data structures

### Validated tuple and reference grammar

The data model is a relationship tuple `(object, relation, user)` where the user may be an object, typed wildcard, or userset `object#relation`; tuples may carry a condition reference and context. The current Go implementation repeatedly parses strings through tuple utilities. In Rust these are natural validated newtypes and enums. The API boundary must preserve wire strings, but no engine or storage code should receive an unparsed string.

### TypeSystem and compiled graphs

`TypeSystem` indexes type definitions, relations, conditions, TTUs, a relationship graph, and a weighted graph (`vendors/openfga/pkg/typesystem/typesystem.go:168`). Construction is separate from validation. Validation checks schema version, duplicates, names, rewrites, type restrictions, entrypoints, cycles, and conditions in deterministic sorted order (`vendors/openfga/pkg/typesystem/typesystem.go:1113`, `vendors/openfga/pkg/typesystem/typesystem.go:1127`, `vendors/openfga/pkg/typesystem/typesystem.go:1156`, `vendors/openfga/pkg/typesystem/typesystem.go:1189`). A TTU's tupleset must be a direct relation and its computed relation must exist on a permitted target type (`vendors/openfga/pkg/typesystem/typesystem.go:1292`, `vendors/openfga/pkg/typesystem/typesystem.go:1300`, `vendors/openfga/pkg/typesystem/typesystem.go:1308`).

The weighted graph adds reachability weights, wildcard paths, recursive-relation annotations, and edge condition sets. It enables O(1)-ish reachability rejection and safe strategy selection, but the model compiler is correspondingly load-bearing (`vendors/openfga/internal/modelgraph/model.go:19`, `vendors/openfga/internal/modelgraph/model.go:26`).

### Evaluation request and subproblem key

The root request contains immutable store/model/contextual state plus derived indexes, while each child carries a new `(object, relation, user)` and branch path. Cache identity includes store, model, contextual tuples, and request condition context (`vendors/openfga/internal/check/request.go:22`, `vendors/openfga/internal/check/request.go:30`, `vendors/openfga/internal/check/request.go:154`). Omitting any one of these fields creates cross-tenant or cross-context authorization corruption.

### Datastore traits and iterators

The datastore separates tuple reads/writes, models, stores, assertions, changelog, readiness, and close behind narrow interfaces composed into `OpenFGADatastore` (`vendors/openfga/pkg/storage/storage.go:144`, `vendors/openfga/pkg/storage/storage.go:332`, `vendors/openfga/pkg/storage/storage.go:363`, `vendors/openfga/pkg/storage/storage.go:380`, `vendors/openfga/pkg/storage/storage.go:395`, `vendors/openfga/pkg/storage/storage.go:407`). Dominant query shapes are exact tuple lookup, userset lookup by object/relation, and reverse lookup by user/object type (`vendors/openfga/pkg/storage/storage.go:168`, `vendors/openfga/pkg/storage/storage.go:177`, `vendors/openfga/pkg/storage/storage.go:193`). Iteration is streaming and explicitly owned.

### Cache entries and invalidation markers

OpenFGA has independent model, typesystem/graph, subproblem decision, and iterator caches (`vendors/openfga/docs/caching.md:5`). Higher consistency bypasses decision and iterator caches (`vendors/openfga/docs/caching.md:13`). Decision keys include all request invariants; iterator keys include only fields that change the database result (`vendors/openfga/docs/caching.md:30`, `vendors/openfga/docs/caching.md:63`, `vendors/openfga/docs/caching.md:80`).

Invalidation is timestamp-based. The controller reads the changelog, marks all earlier decision results stale, and writes coarse or targeted iterator invalidation markers (`vendors/openfga/docs/caching.md:129`, `vendors/openfga/docs/caching.md:144`, `vendors/openfga/docs/caching.md:146`, `vendors/openfga/docs/caching.md:147`). It admits a bounded stale window because refresh is asynchronous (`vendors/openfga/internal/cachecontroller/cache_controller.go:143`, `vendors/openfga/internal/cachecontroller/cache_controller.go:170`).

## Key algorithms and correctness arguments

### Check rewrite evaluation

Let `Eval(o, r, u, path)` return allow/deny or error. `this` queries direct and userset tuples; a computed userset rewrites `r`; TTU reads `(o, tupleset, x)` and evaluates `(x, computed, u)`; union returns allow if any child allows; intersection returns deny if any child denies; difference returns allow exactly when base allows and subtract denies. Each recursive call adds its subproblem key to a branch-local path. Repeating a key denies only that cyclic branch. With maximum depth `D`, bounded fan-out `B`, and cancellation, evaluation terminates. Worst-case logical work is exponential in `D`, which is why independent dispatch and I/O budgets are also required.

### Recursive userset/TTU fast path

For a recursive relation, upstream builds two frontiers: usersets reachable from the request user and usersets reachable from the target object. It streams both and returns allow at the first intersection; otherwise it expands breadth-first (`vendors/openfga/internal/graph/recursive_resolver.go:82`, `vendors/openfga/internal/graph/recursive_resolver.go:85`, `vendors/openfga/internal/graph/recursive_resolver.go:123`, `vendors/openfga/internal/graph/recursive_resolver.go:140`). This turns repeated recursive dispatch into set reachability. It is valid only for graph shapes proven by the model compiler, so it belongs behind a strategy predicate and differential oracle.

### Weight-two fast path and adaptive strategy planning

The weighted engine can use a specialized two-hop join when the graph weight is exactly two (`vendors/openfga/internal/check/check.go:1017`, `vendors/openfga/internal/check/check.go:1093`). Where multiple correct strategies exist, a per-plan-key Thompson sampler chooses an expected fast strategy and updates a Normal-gamma latency model (`vendors/openfga/internal/planner/plan.go:46`, `vendors/openfga/internal/planner/plan.go:70`, `vendors/openfga/internal/planner/thompson.go:13`). This is an optimization policy, never an authorization semantic. Shadow evaluation and fallback are essential rollout controls.

### ListObjects reverse expansion with residual Check

Reverse traversal uses storage's reverse index to enumerate only plausible objects. For union-only paths candidates are final; intersection/difference paths are conservatively pruned and marked for Check. If reverse traversal is complete and Check is sound, the output equals `{o | Check(o, r, u)}`. Deduplication, deadlines, result caps, and cancellation are required for finite resource use.

### Transactional tuple mutation and changelog

PostgreSQL routes higher-consistency reads to the primary and latency-preferring reads to a replica when present (`vendors/openfga/pkg/storage/postgres/postgres.go:320`). Writes begin a read-committed transaction, deterministically lock all affected tuple keys, validate duplicate/missing policy against existing rows, apply deletes then inserts, append changelog rows, and commit (`vendors/openfga/pkg/storage/postgres/postgres.go:584`, `vendors/openfga/pkg/storage/postgres/postgres.go:592`, `vendors/openfga/pkg/storage/postgres/postgres.go:600`, `vendors/openfga/pkg/storage/postgres/postgres.go:627`, `vendors/openfga/pkg/storage/postgres/postgres.go:637`, `vendors/openfga/pkg/storage/postgres/postgres.go:643`). Tuple and changelog atomicity is the cache-consistency foundation.

The PostgreSQL schema's tuple primary key is `(store, object_type, object_id, relation, user)`, with forward and reverse indexes plus ULID ordering; models, assertions, stores, and changelog are separate tables (`vendors/openfga/assets/migrations/postgres/001_initialize_schema.sql:2`, `vendors/openfga/assets/migrations/postgres/001_initialize_schema.sql:11`, `vendors/openfga/assets/migrations/postgres/001_initialize_schema.sql:14`, `vendors/openfga/assets/migrations/postgres/003_add_reverse_lookup_index.sql:2`, `vendors/openfga/assets/migrations/postgres/001_initialize_schema.sql:18`, `vendors/openfga/assets/migrations/postgres/001_initialize_schema.sql:41`). Identifiers are case-sensitive; later migrations add binary/collated indexes (`vendors/openfga/assets/migrations/postgres/006_add_collate_index.sql:3`).

### CEL condition evaluation

Conditions are lazily compiled once into a typed CEL program; compilation declares model parameters, requires a Boolean output, and enables partial evaluation (`vendors/openfga/internal/condition/condition.go:51`, `vendors/openfga/internal/condition/condition.go:67`, `vendors/openfga/internal/condition/condition.go:104`, `vendors/openfga/internal/condition/condition.go:127`, `vendors/openfga/internal/condition/condition.go:137`). Tuple context overrides request context, evaluation is cancellable, cost is tracked and capped, and missing parameters become an evaluation error (`vendors/openfga/internal/condition/eval/eval.go:49`, `vendors/openfga/internal/condition/eval/eval.go:57`, `vendors/openfga/internal/condition/eval/eval.go:62`, `vendors/openfga/internal/condition/eval/eval.go:68`; `vendors/openfga/internal/condition/condition.go:299`). OpenFGA supports custom duration, timestamp, IP address, list, map, and scalar conversions, so syntax compatibility alone is insufficient.

## What we will adopt

1. **Protocol compatibility as a hard boundary.** Generate Rust gRPC types from the same OpenFGA protobuf contract and expose the HTTP/JSON mapping. Preserve error codes, pagination tokens, consistency preferences, and streaming behavior.
2. **Validated domain types.** Parse tuple grammar once into private-field newtypes/enums. Model compilation produces an immutable `Arc<CompiledModel>` containing deterministic indexes, rewrite IR, reachability metadata, and compiled conditions.
3. **A correctness-first evaluator.** Implement direct, computed, TTU, union, intersection, difference, wildcard, contextual tuple, condition, depth, and cycle semantics before fast paths. Use it as the differential oracle forever.
4. **Structured, bounded concurrency.** Every evaluator owns a cancellation token and joined task set; union/intersection/difference short-circuit according to Boolean and error precedence. Independent semaphores bound request fan-out, datastore I/O, and server-wide in-flight work.
5. **Narrow storage capabilities.** Mirror the useful separation of tuple reads, tuple writes, models, stores, assertions, changelog, and health rather than requiring every unit test to mock one giant datastore.
6. **Purpose-built forward and reverse indexes.** Preserve exact, object/relation, and user/object-type query shapes. Make tuple mutation and changelog append one transaction.
7. **Immutable model caches and conservative decision caches.** Cache models by `(store_id, model_id)`. Include every semantic request invariant in decision keys. Bypass mutable-data caches for higher consistency.
8. **Shadow/fallback optimization rollout.** Recursive, weight-two, pipeline, and adaptive strategies remain internal. They graduate only after conformance, differential, race/cancellation, and benchmark gates.
9. **Observability at semantic boundaries.** Record datastore query/item count, dispatch count, chosen strategy, cache outcome, throttling, cycle detection, and decision latency without logging tuple contexts or credentials.
10. **Graceful lifecycle.** Supervise cache invalidation, JWKS refresh, and optional planner actors; stop admission, cancel, drain, join, flush telemetry, and close pools in order.

## What we will avoid

1. **Raw strings inside the engine.** They permit malformed states, repeated parsing, ambiguous cache keys, and accidental cross-type comparisons.
2. **Context-as-service-locator.** Upstream passes type systems and readers through Go contexts (`vendors/openfga/pkg/storage/storage.go:34`). Rust dependencies will be explicit fields; context carries cancellation/deadline/trace only.
3. **One giant storage trait in consumers.** The aggregate is convenient for server assembly but violates interface segregation for algorithms and tests.
4. **Unbounded task-per-tuple fan-out.** Upstream defaults for some per-request database read caps are effectively unbounded (`vendors/openfga/pkg/server/config/config.go:29`). Rust defaults will be finite and derived from pool capacity.
5. **Unsafe lock-free planner state.** The upstream planner's raw-pointer atomics are unnecessary for initial correctness and forbidden by project policy (`vendors/openfga/internal/planner/thompson.go:8`, `vendors/openfga/internal/planner/thompson.go:16`). A later safe implementation may use immutable `ArcSwap` snapshots or actor-owned state after profiling.
6. **Shared iterator-cache policy with first-writer-wins configuration.** Upstream documents nondeterministic effective TTL/max-results when Check and ListObjects share entries (`vendors/openfga/docs/caching.md:111`). Rust will use one canonical policy per cache namespace or separate namespaces.
7. **Detached invalidation work tied ambiguously to request context.** The Rust invalidation actor will be supervised, capacity-bounded, timeout-bound, and explicitly stopped.
8. **Experimental semantics on the authoritative path.** The vendored weighted engine still rejects some userset/wildcard shapes and relies on fallback (`vendors/openfga/internal/check/check.go:35`, `vendors/openfga/pkg/server/check.go:80`). Compatibility must not depend on a fallback becoming unreachable by accident.
9. **FFI CEL as the default.** Project policy forbids unsafe application code and prefers pure Rust. A pure-Rust CEL adapter may be selected only after OpenFGA-specific conformance; otherwise condition support remains behind a clearly isolated, audited boundary rather than silently drifting.

## Edge cases and failure semantics

- A branch error cannot override a decisive allow in union or a decisive deny in intersection. When no decisive Boolean exists, propagate a typed error.
- Higher consistency bypasses mutable tuple/decision caches and reads from primary storage.
- Contextual tuples are validated like writes, merged only for the request, and included in decision-cache identity.
- Tuple condition context overrides request context; missing parameters are errors, not false.
- Cycles deny only the cyclic path. Depth exhaustion is a resource error, not an authorization deny.
- Cancellation closes/drains every iterator and joins every child task before returning.
- Duplicate insert and missing delete behavior is explicit (`error` or `ignore`) and the combined mutation is atomic.
- Continuation tokens are opaque, authenticated or integrity-protected, scoped to endpoint/filter/store, and bounded in decoded size.
- A typed wildcard is not a literal individual and has special set behavior in Check/ListUsers.
- Model IDs are immutable ULIDs; selecting the latest model and caching it must not make explicit historical model reads ambiguous.

## Open questions

All design-blocking questions are converted into Phase 0 verification artifacts in `../../specs/91-implementation-impl-plan.md`; none is left as an implicit assumption:

- `spike-cel-openfga-conformance.md`: demonstrate that the selected pure-Rust CEL adapter matches OpenFGA types, unknowns, precedence, functions, and cost/cancellation rules, or define the minimal in-house compatibility layer.
- `spike-openfga-proto-generation.md`: pin the OpenFGA API commit and prove deterministic tonic/prost generation plus HTTP transcoding metadata.
- `spike-listobjects-algorithm.md`: compare conservative reverse-expand-plus-Check with the weighted worker pipeline on upstream matrices and retain the simpler correct baseline.

These spike names are exit-gate artifacts, not deferred implementation placeholders; Phase 0 must resolve them before production code depends on their outcomes.
