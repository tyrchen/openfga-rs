//! Transport-independent, fully validated query commands.

use std::{
    collections::BTreeSet,
    fmt,
    num::NonZeroU32,
    time::{Duration, Instant},
};

use typed_builder::TypedBuilder;

use crate::{
    context::ConditionContext,
    error::{ValidationError, ValidationReason},
    identifier::{
        AuthorizationModelId, CorrelationId, PrincipalId, RelationName, StoreId, TypeName,
    },
    limits::InputLimits,
    reference::{ContextualTuples, ObjectRef, SubjectRef, TupleKey},
    token::ContinuationCursor,
};

const MAXIMUM_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

/// The authorization-model resolution policy for a query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ModelSelection {
    /// Evaluate against one immutable model.
    Explicit(AuthorizationModelId),
    /// Resolve the latest model once before evaluation.
    Latest,
}

/// The caller-selected datastore consistency preference.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ConsistencyPreference {
    /// Prefer a lower-latency snapshot when the backend supports it.
    #[default]
    MinimizeLatency,
    /// Prefer the highest consistency offered by the backend.
    HigherConsistency,
}

/// The trusted authentication mechanism that established a caller identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum PrincipalKind {
    /// An `OpenID` Connect identity.
    OpenIdConnect,
    /// A preshared-key identity represented by a non-secret stable key label.
    PresharedKey,
    /// An explicitly enabled loopback-only development identity.
    Development,
    /// A server-internal actor.
    Internal,
}

/// A validated authenticated caller whose identity is redacted from diagnostics.
#[derive(Clone, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct Principal {
    kind: PrincipalKind,
    id: PrincipalId,
}

impl Principal {
    /// Creates an authenticated principal from a trusted mechanism and validated ID.
    #[must_use]
    pub const fn new(kind: PrincipalKind, id: PrincipalId) -> Self {
        Self { kind, id }
    }

    /// Returns the authentication mechanism.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }

    /// Returns the validated identity for explicit authorization checks.
    #[must_use]
    pub const fn id(&self) -> &PrincipalId {
        &self.id
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("kind", &self.kind)
            .field("id", &"[REDACTED]")
            .finish()
    }
}

/// A positive request timeout bounded by the compiled five-minute safety ceiling.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct RequestTimeout(Duration);

impl RequestTimeout {
    /// Creates a positive timeout at or below the safety ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for zero or values over five minutes.
    pub fn new(duration: Duration) -> Result<Self, ValidationError> {
        if duration.is_zero() || duration > MAXIMUM_REQUEST_TIMEOUT {
            return Err(ValidationError::new(
                "request_timeout",
                ValidationReason::OutOfRange,
            ));
        }
        Ok(Self(duration))
    }

    /// Returns the bounded duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }
}

/// An absolute monotonic request deadline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct Deadline(Instant);

impl Deadline {
    /// Computes a deadline without wall-clock arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if the monotonic clock cannot represent the sum.
    pub fn from_timeout(now: Instant, timeout: RequestTimeout) -> Result<Self, ValidationError> {
        now.checked_add(timeout.duration())
            .map(Self)
            .ok_or_else(|| ValidationError::new("request_deadline", ValidationReason::OutOfRange))
    }

    /// Returns the absolute monotonic instant.
    #[must_use]
    pub const fn instant(self) -> Instant {
        self.0
    }

    /// Returns whether the deadline has elapsed at `now`.
    #[must_use]
    pub fn is_elapsed(self, now: Instant) -> bool {
        now >= self.0
    }
}

/// Shared immutable inputs and budgets for one authorization query.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct QueryContext {
    store_id: StoreId,
    model_selection: ModelSelection,
    consistency: ConsistencyPreference,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    deadline: Deadline,
    principal: Principal,
}

impl QueryContext {
    /// Returns the store identity.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the authorization-model selection policy.
    #[must_use]
    pub const fn model_selection(&self) -> ModelSelection {
        self.model_selection
    }

    /// Returns the consistency preference.
    #[must_use]
    pub const fn consistency(&self) -> ConsistencyPreference {
        self.consistency
    }

    /// Returns request-only relationship tuples.
    #[must_use]
    pub const fn contextual_tuples(&self) -> &ContextualTuples {
        &self.contextual_tuples
    }

    /// Returns the redacted, bounded condition context.
    #[must_use]
    pub const fn condition_context(&self) -> &ConditionContext {
        &self.condition_context
    }

    /// Returns the monotonic deadline.
    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Returns the authenticated caller.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }
}

/// A validated single authorization check.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CheckCommand {
    query: QueryContext,
    tuple: TupleKey,
}

impl CheckCommand {
    /// Creates a check command from validated inputs.
    #[must_use]
    pub const fn new(query: QueryContext, tuple: TupleKey) -> Self {
        Self { query, tuple }
    }

    /// Returns shared query context.
    #[must_use]
    pub const fn query(&self) -> &QueryContext {
        &self.query
    }

    /// Returns the object/relation/subject question.
    #[must_use]
    pub const fn tuple(&self) -> &TupleKey {
        &self.tuple
    }
}

/// One independently contextualized `BatchCheck` item.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchCheckItem {
    correlation_id: CorrelationId,
    tuple: TupleKey,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
}

impl BatchCheckItem {
    /// Creates a keyed `BatchCheck` item from validated inputs.
    #[must_use]
    pub const fn new(
        correlation_id: CorrelationId,
        tuple: TupleKey,
        contextual_tuples: ContextualTuples,
        condition_context: ConditionContext,
    ) -> Self {
        Self {
            correlation_id,
            tuple,
            contextual_tuples,
            condition_context,
        }
    }

    /// Returns the stable response correlation ID.
    #[must_use]
    pub const fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }

    /// Returns the item question.
    #[must_use]
    pub const fn tuple(&self) -> &TupleKey {
        &self.tuple
    }

    /// Returns this item's request-only tuples.
    #[must_use]
    pub const fn contextual_tuples(&self) -> &ContextualTuples {
        &self.contextual_tuples
    }

    /// Returns this item's condition context.
    #[must_use]
    pub const fn condition_context(&self) -> &ConditionContext {
        &self.condition_context
    }
}

/// A non-empty, bounded `BatchCheck` item set with unique correlation IDs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchCheckItems(Vec<BatchCheckItem>);

impl BatchCheckItems {
    /// Validates item count and correlation-ID uniqueness.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for an empty, oversized, or duplicate-keyed batch.
    pub fn new(items: Vec<BatchCheckItem>, limits: &InputLimits) -> Result<Self, ValidationError> {
        if items.is_empty() {
            return Err(ValidationError::new(
                "batch_checks",
                ValidationReason::Missing,
            ));
        }
        if items.len() > limits.batch_items() {
            return Err(ValidationError::new(
                "batch_checks",
                ValidationReason::TooManyItems,
            ));
        }
        let mut ids = BTreeSet::new();
        if items
            .iter()
            .any(|item| !ids.insert(item.correlation_id.clone()))
        {
            return Err(ValidationError::new(
                "batch_checks",
                ValidationReason::Duplicate,
            ));
        }
        Ok(Self(items))
    }

    /// Returns items in request order.
    #[must_use]
    pub fn as_slice(&self) -> &[BatchCheckItem] {
        &self.0
    }
}

/// A validated batch of independent authorization checks.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchCheckCommand {
    query: QueryContext,
    items: BatchCheckItems,
}

impl BatchCheckCommand {
    /// Creates a `BatchCheck` command.
    #[must_use]
    pub const fn new(query: QueryContext, items: BatchCheckItems) -> Self {
        Self { query, items }
    }

    /// Returns shared store/model/consistency/deadline/principal context.
    #[must_use]
    pub const fn query(&self) -> &QueryContext {
        &self.query
    }

    /// Returns bounded correlated items.
    #[must_use]
    pub const fn items(&self) -> &BatchCheckItems {
        &self.items
    }
}

/// Bounded result and verified continuation state for an internal list query.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ListControl {
    maximum_results: NonZeroU32,
    continuation: Option<ContinuationCursor>,
}

impl ListControl {
    /// Creates list controls under the configured result cap.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the requested maximum exceeds policy.
    pub fn new(
        maximum_results: NonZeroU32,
        continuation: Option<ContinuationCursor>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if maximum_results.get() > limits.results() {
            return Err(ValidationError::new(
                "maximum_results",
                ValidationReason::OutOfRange,
            ));
        }
        Ok(Self {
            maximum_results,
            continuation,
        })
    }

    /// Returns the hard result cap.
    #[must_use]
    pub const fn maximum_results(&self) -> NonZeroU32 {
        self.maximum_results
    }

    /// Returns verified continuation state when resuming a query.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ContinuationCursor> {
        self.continuation.as_ref()
    }
}

/// A typed `ListObjects` query.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ListObjectsCommand {
    query: QueryContext,
    object_type: TypeName,
    relation: RelationName,
    subject: SubjectRef,
    control: ListControl,
}

impl ListObjectsCommand {
    /// Creates a `ListObjects` command from validated inputs.
    #[must_use]
    pub const fn new(
        query: QueryContext,
        object_type: TypeName,
        relation: RelationName,
        subject: SubjectRef,
        control: ListControl,
    ) -> Self {
        Self {
            query,
            object_type,
            relation,
            subject,
            control,
        }
    }

    /// Returns shared query context.
    #[must_use]
    pub const fn query(&self) -> &QueryContext {
        &self.query
    }

    /// Returns the requested result object type.
    #[must_use]
    pub const fn object_type(&self) -> &TypeName {
        &self.object_type
    }

    /// Returns the relation evaluated on candidate objects.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns the subject from which objects are listed.
    #[must_use]
    pub const fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    /// Returns list result/continuation controls.
    #[must_use]
    pub const fn control(&self) -> &ListControl {
        &self.control
    }
}

/// One `ListUsers` type/relation restriction.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct UserTypeFilter {
    user_type: TypeName,
    relation: Option<RelationName>,
}

impl UserTypeFilter {
    /// Creates a user-type filter; relation absence means concrete users of the type.
    #[must_use]
    pub const fn new(user_type: TypeName, relation: Option<RelationName>) -> Self {
        Self {
            user_type,
            relation,
        }
    }

    /// Returns the user type.
    #[must_use]
    pub const fn user_type(&self) -> &TypeName {
        &self.user_type
    }

    /// Returns the userset relation restriction, when present.
    #[must_use]
    pub const fn relation(&self) -> Option<&RelationName> {
        self.relation.as_ref()
    }
}

/// A non-empty, bounded, duplicate-free `ListUsers` filter set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UserTypeFilters(Vec<UserTypeFilter>);

impl UserTypeFilters {
    /// Validates filter count and uniqueness.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for empty, oversized, or duplicate filters.
    pub fn new(
        filters: Vec<UserTypeFilter>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if filters.is_empty() {
            return Err(ValidationError::new(
                "user_filters",
                ValidationReason::Missing,
            ));
        }
        if filters.len() > limits.user_filters() {
            return Err(ValidationError::new(
                "user_filters",
                ValidationReason::TooManyItems,
            ));
        }
        let unique = filters.iter().collect::<BTreeSet<_>>();
        if unique.len() != filters.len() {
            return Err(ValidationError::new(
                "user_filters",
                ValidationReason::Duplicate,
            ));
        }
        Ok(Self(filters))
    }

    /// Returns filters in request order.
    #[must_use]
    pub fn as_slice(&self) -> &[UserTypeFilter] {
        &self.0
    }
}

/// A typed `ListUsers` query.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ListUsersCommand {
    query: QueryContext,
    object: ObjectRef,
    relation: RelationName,
    filters: UserTypeFilters,
    control: ListControl,
}

impl ListUsersCommand {
    /// Creates a `ListUsers` command from validated inputs.
    #[must_use]
    pub const fn new(
        query: QueryContext,
        object: ObjectRef,
        relation: RelationName,
        filters: UserTypeFilters,
        control: ListControl,
    ) -> Self {
        Self {
            query,
            object,
            relation,
            filters,
            control,
        }
    }

    /// Returns shared query context.
    #[must_use]
    pub const fn query(&self) -> &QueryContext {
        &self.query
    }

    /// Returns the target object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns requested user-type filters.
    #[must_use]
    pub const fn filters(&self) -> &UserTypeFilters {
        &self.filters
    }

    /// Returns list result/continuation controls.
    #[must_use]
    pub const fn control(&self) -> &ListControl {
        &self.control
    }
}

/// A typed Expand query.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ExpandCommand {
    query: QueryContext,
    object: ObjectRef,
    relation: RelationName,
}

impl ExpandCommand {
    /// Creates an Expand command from validated inputs.
    #[must_use]
    pub const fn new(query: QueryContext, object: ObjectRef, relation: RelationName) -> Self {
        Self {
            query,
            object,
            relation,
        }
    }

    /// Returns shared query context.
    #[must_use]
    pub const fn query(&self) -> &QueryContext {
        &self.query
    }

    /// Returns the target object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Instant};

    use super::{
        BatchCheckItem, BatchCheckItems, ConsistencyPreference, Deadline, ListControl,
        ModelSelection, Principal, PrincipalKind, QueryContext, RequestTimeout, UserTypeFilter,
        UserTypeFilters,
    };
    use crate::{
        ConditionContext, ContextualTuples, InputLimits, PrincipalId, StoreId, TupleKey, TypeName,
    };

    fn item(correlation_id: &str) -> Result<BatchCheckItem, crate::DomainError> {
        Ok(BatchCheckItem::new(
            correlation_id.parse()?,
            "document:roadmap#viewer@user:anne".parse::<TupleKey>()?,
            ContextualTuples::empty(),
            ConditionContext::empty(),
        ))
    }

    #[test]
    fn test_should_reject_duplicate_batch_correlation_ids() -> Result<(), crate::DomainError> {
        let limits = InputLimits::default();
        assert!(BatchCheckItems::new(vec![item("one")?, item("one")?], &limits).is_err());
        Ok(())
    }

    #[test]
    fn test_should_bound_deadlines_results_and_user_filters() -> Result<(), crate::DomainError> {
        let limits = InputLimits::default();
        assert!(RequestTimeout::new(std::time::Duration::ZERO).is_err());
        let timeout = RequestTimeout::new(std::time::Duration::from_secs(2))?;
        let now = Instant::now();
        let deadline = Deadline::from_timeout(now, timeout)?;
        assert!(!deadline.is_elapsed(now));
        assert!(ListControl::new(NonZeroU32::MIN, None, &limits).is_ok());

        let filter = UserTypeFilter::new("user".parse::<TypeName>()?, None);
        assert!(UserTypeFilters::new(vec![filter.clone(), filter], &limits).is_err());
        Ok(())
    }

    #[test]
    fn test_should_redact_principal_through_query_debug() -> Result<(), crate::DomainError> {
        let now = Instant::now();
        let query = QueryContext::builder()
            .store_id("01G5JAVJ41T49E9TT3SKVS7X1J".parse::<StoreId>()?)
            .model_selection(ModelSelection::Latest)
            .consistency(ConsistencyPreference::MinimizeLatency)
            .contextual_tuples(ContextualTuples::empty())
            .condition_context(ConditionContext::empty())
            .deadline(Deadline::from_timeout(
                now,
                RequestTimeout::new(std::time::Duration::from_secs(1))?,
            )?)
            .principal(Principal::new(
                PrincipalKind::OpenIdConnect,
                "issuer|very-secret-subject".parse::<PrincipalId>()?,
            ))
            .build();

        assert!(!format!("{query:?}").contains("very-secret-subject"));
        Ok(())
    }
}
