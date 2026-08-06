# Domain model design

Status: Proposed · Depends on: [`00-openfga-prd.md`](00-openfga-prd.md)

## Boundary rule

Generated protobuf values and HTTP JSON are untrusted wire types. Adapters MUST validate lengths, grammar, ranges, collection counts, and cross-field rules, then convert to domain commands. Project-owned wire structs derive `validator::Validate`; generated protobuf adapters invoke equivalent explicit validation before conversion. Semantic guarantees remain in fallible private-field constructors. No evaluator, compiler, cache, or storage trait accepts a raw tuple string or generated request.

## Primitive types

Private-field newtypes provide `TryFrom`, `FromStr`, borrowed accessors, canonical `Display`, redacted or safe `Debug`, and serde only where persistence needs it:

- `StoreId`, `AuthorizationModelId`, and `ChangeId`: canonical ULIDs; reject noncanonical/oversized input.
- `TypeName`, `RelationName`, `ConditionName`, and `ParameterName`: OpenFGA-compatible ASCII grammar and byte caps.
- `ObjectId`: non-empty allowed OpenFGA bytes with an explicit cap.
- `ObjectRef { object_type, object_id }`.
- `UsersetRef { object, relation }`.
- `SubjectRef`: `Object(ObjectRef)`, `Userset(UsersetRef)`, or `TypedWildcard(TypeName)`.
- `TupleKey { object, relation, subject }` and `RelationshipTuple { key, condition }`.
- `ConditionBinding { name, context }`; context is a bounded typed tree, not unrestricted `serde_json::Value` inside the engine.

Winnow parsers MUST consume the complete input, reject NUL/control characters and ambiguous separators, avoid unchecked slicing, and report typed offset/kind errors without echoing hostile full values. Canonical rendering MUST round-trip for all generated valid values; property tests enforce this.

## Limits

One validated `InputLimits` policy supplies byte and count limits for identifiers, condition contexts, tuples per write/request, batch items, type definitions, relations, operands, assertions, and token bytes. Transport rejects oversize bodies first; constructors enforce the same invariants for non-HTTP callers. Limits are positive bounded newtypes and configuration cannot exceed compiled safety ceilings.

## Model source and rewrite IR

`AuthorizationModelSource` mirrors schema 1.1 after wire conversion: ordered type definitions, relation metadata/type restrictions, rewrite AST, and condition definitions. The source retains stable declaration positions for useful errors.

Compilation produces normalized nodes referenced by dense IDs:

```text
RewriteNode = Direct(restrictions)
            | Computed(RelationId)
            | TupleToUserset { tupleset: RelationId, computed: RelationName }
            | Union(NonEmpty<NodeId>)
            | Intersection(NonEmpty<NodeId>)
            | Difference { base: NodeId, subtract: NodeId }
```

Empty unions/intersections are unrepresentable. Relation IDs are local to a compiled model and never serialized. Conditions are referenced through `ConditionId`; absent condition is represented by an enum variant, not an empty string.

## Query commands

Domain commands share `QueryContext` containing store/model selection, consistency, bounded contextual tuples, bounded condition context, deadline, and caller principal. `CheckCommand` adds object/relation/subject. `BatchCheckCommand` holds bounded keyed items. List commands include typed subject/user filters, maximum results, and continuation state. Limits are `NonZeroU32` or constrained duration types.

`ConsistencyPreference` is `MinimizeLatency` or `HigherConsistency`. It is mandatory in domain commands even when the wire default selects the former. Model selection is `Explicit(ModelId)` or `Latest`; resolution converts it to an explicit model before evaluation and cache keying.

## Errors

Libraries use non-exhaustive `thiserror` enums with structured variants: `Parse`, `Validation`, `Model`, `Condition`, `Storage`, `ResourceExhausted`, `Cancelled`, and `Internal`. External values are summarized by field and reason, never copied wholesale. Transport alone maps domain errors to gRPC/HTTP errors.

No library API uses `Option` to hide malformed input or operational failure. Absence is used only where semantically optional. No production path uses `unwrap`, `expect`, indexing on untrusted offsets, `panic`, or unreachable assertions.

## Acceptance criteria

- Invalid wire values cannot construct engine-visible domain values.
- Parser round-trip and non-panicking properties pass over arbitrary byte strings.
- Domain `Debug` never reveals condition values or credentials.
- Equivalent wire spellings have one canonical cache/storage representation.
- Every externally derived string, collection, integer, and nested value has a tested bound.

## Engineering norms

Repository `AGENTS.md` sections **Error Handling**, **Type Design & API**, **Safety & Security**, **Serialization & Data**, **Testing**, **Performance**, **Documentation**, and **Code Style** bind this crate. **Async & Concurrency** and **Logging & Observability** are N/A for pure domain values: the crate performs no I/O, spawns no tasks, and emits no telemetry.

## Cross-references

- ← Depends on: [`00-openfga-prd.md`](00-openfga-prd.md)
- → Consumed by: [`11-condition-engine-design.md`](11-condition-engine-design.md), [`12-model-compiler-design.md`](12-model-compiler-design.md), [`13-storage-design.md`](13-storage-design.md), and all query/service designs
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md), [`../docs/research/survey-rust-ecosystem.md`](../docs/research/survey-rust-ecosystem.md)
- ↔ Prior art: tuple/storage grammar contracts in `vendors/openfga/pkg/storage/storage.go:144` and request invariants in `vendors/openfga/internal/check/request.go:22`
