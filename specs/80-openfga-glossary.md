# OpenFGA glossary

Status: Proposed · Scope: normative project terminology

| Term | Meaning |
| --- | --- |
| Authorization model | Immutable schema and relation rewrites identified by a model ULID within a store. |
| Compiled model | Validated, immutable engine representation containing relation IR, type restrictions, reachability, and compiled conditions. |
| Store | Tenant-like namespace for models, tuples, assertions, and changelog. Store isolation is a security boundary. |
| Relationship tuple | `(object, relation, user)` plus optional condition name/context. |
| Object | Typed identifier such as `document:roadmap`. |
| Subject | Individual object, typed wildcard, or userset whose membership is tested. “User” in the OpenFGA wire API means this broader concept. |
| Userset | Subject reference such as `group:eng#member`. |
| Typed wildcard | All objects of one type, represented as `type:*`; never a literal object ID. |
| Rewrite | Relation definition composed from direct, computed, TTU, union, intersection, or difference nodes. |
| TTU | Tuple-to-userset: follow a tupleset relation to related objects and evaluate a computed relation on each. |
| Contextual tuple | Valid tuple supplied only for one query; it is never persisted or placed in tuple iterator caches. |
| Condition context | Typed values used by CEL. Tuple context overrides request context for identical keys. |
| Check | Determine whether a subject has a relation to an object under one model. |
| BatchCheck | Evaluate bounded independent Check items while preserving item correlation and errors. |
| ListObjects | Enumerate objects for which Check would allow a given subject/relation, subject to result/deadline limits. |
| ListUsers | Enumerate subjects of a requested type that satisfy an object/relation. |
| Expand | Return the userset expansion tree for diagnostics; not an authorization decision API. |
| Higher consistency | Request preference requiring primary/authoritative tuple state and bypass of mutable-data decision/iterator caches. For DynamoDB it means `ConsistentRead=true` on base-table operations, never a GSI. |
| Minimize latency | Default request preference permitting configured replicas and safe mutable-data caches. |
| Decision cache | Cache of final Check results keyed by every semantic input. |
| Iterator cache | Cache of bounded materialized tuple-read results, never live database iterators. |
| Decisive result | Allow for union, deny for intersection, or an operand result that determines difference; it may cancel siblings. |
| Compatibility oracle | Simple correctness-first evaluator retained to validate optimized evaluators. |
| Branch-local cycle | Revisit of the same subproblem along one recursion path; it denies only that branch. |
| Resource error | Typed failure such as deadline, depth, dispatch, read, or condition-cost exhaustion; it is not a deny. |
| Changelog | Ordered record of tuple writes, committed atomically with the tuple mutation and used for ReadChanges/cache invalidation. |
| Packed change batch | DynamoDB physical item containing several individually identified ordered `TupleChange` values. It is invisible above the backend and is not one logical change. |
| Change HEAD | DynamoDB per-store conditional record that serializes mutation commit order and allocates strictly increasing individual change IDs; it is not a cache watermark. |
| Baseline | Exact vendored OpenFGA commit whose externally observable behavior a release targets. |
