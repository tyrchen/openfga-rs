//! Fail-closed action and store authorization policy.

use std::{collections::BTreeSet, fmt};

use openfga_domain::{Principal, PrincipalId, StoreId};
use serde::{Deserialize, Serialize};

/// An operation protected by service authorization.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum Action {
    /// Create a store.
    CreateStore,
    /// List stores.
    ListStores,
    /// Read store metadata.
    GetStore,
    /// Update store metadata.
    UpdateStore,
    /// Delete a store.
    DeleteStore,
    /// Read authorization models.
    ReadAuthorizationModels,
    /// Publish an authorization model.
    WriteAuthorizationModel,
    /// Read assertions.
    ReadAssertions,
    /// Write assertions.
    WriteAssertions,
    /// Read relationship tuples.
    Read,
    /// Write relationship tuples.
    Write,
    /// Read the tuple changelog.
    ReadChanges,
    /// Evaluate one authorization check.
    Check,
    /// Evaluate a batch of authorization checks.
    BatchCheck,
    /// Expand a userset tree.
    Expand,
    /// List authorized objects.
    ListObjects,
    /// Stream authorized objects.
    StreamedListObjects,
    /// List authorized users.
    ListUsers,
}

impl Action {
    /// Every action known to the pinned API surface.
    pub const ALL: [Self; 18] = [
        Self::CreateStore,
        Self::ListStores,
        Self::GetStore,
        Self::UpdateStore,
        Self::DeleteStore,
        Self::ReadAuthorizationModels,
        Self::WriteAuthorizationModel,
        Self::ReadAssertions,
        Self::WriteAssertions,
        Self::Read,
        Self::Write,
        Self::ReadChanges,
        Self::Check,
        Self::BatchCheck,
        Self::Expand,
        Self::ListObjects,
        Self::StreamedListObjects,
        Self::ListUsers,
    ];

    /// Returns whether this action targets server-wide state rather than one store.
    #[must_use]
    pub const fn is_system(self) -> bool {
        match self {
            Self::CreateStore | Self::ListStores => true,
            Self::GetStore
            | Self::UpdateStore
            | Self::DeleteStore
            | Self::ReadAuthorizationModels
            | Self::WriteAuthorizationModel
            | Self::ReadAssertions
            | Self::WriteAssertions
            | Self::Read
            | Self::Write
            | Self::ReadChanges
            | Self::Check
            | Self::BatchCheck
            | Self::Expand
            | Self::ListObjects
            | Self::StreamedListObjects
            | Self::ListUsers => false,
        }
    }
}

/// The stores covered by one policy binding.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum StoreScope {
    /// Every store plus server-wide actions.
    Any,
    /// An explicit finite set of stores; server-wide actions remain denied.
    Stores(BTreeSet<StoreId>),
}

impl fmt::Debug for StoreScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => formatter.write_str("StoreScope::Any"),
            Self::Stores(stores) => formatter
                .debug_struct("StoreScope::Stores")
                .field("store_count", &stores.len())
                .finish_non_exhaustive(),
        }
    }
}

impl StoreScope {
    fn permits(&self, action: Action, store_id: Option<StoreId>) -> bool {
        match (self, action.is_system(), store_id) {
            (Self::Any, true, None) | (Self::Any, false, Some(_)) => true,
            (Self::Stores(stores), false, Some(store_id)) => stores.contains(&store_id),
            _ => false,
        }
    }
}

/// A finite grant for one authenticated principal.
#[derive(Clone)]
#[non_exhaustive]
pub struct PolicyBinding {
    principal_id: PrincipalId,
    actions: BTreeSet<Action>,
    stores: StoreScope,
}

impl fmt::Debug for PolicyBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolicyBinding")
            .field("principal_id", &"[REDACTED]")
            .field("action_count", &self.actions.len())
            .field("stores", &self.stores)
            .finish_non_exhaustive()
    }
}

impl PolicyBinding {
    /// Creates a policy binding.
    #[must_use]
    pub fn new(principal_id: PrincipalId, actions: BTreeSet<Action>, stores: StoreScope) -> Self {
        Self {
            principal_id,
            actions,
            stores,
        }
    }

    fn permits(&self, principal: &Principal, action: Action, store_id: Option<StoreId>) -> bool {
        &self.principal_id == principal.id()
            && self.actions.contains(&action)
            && self.stores.permits(action, store_id)
    }
}

/// Immutable, default-deny service authorization policy.
#[derive(Clone)]
pub struct AuthorizationPolicy {
    bindings: Vec<PolicyBinding>,
}

impl fmt::Debug for AuthorizationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPolicy")
            .field("binding_count", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

impl AuthorizationPolicy {
    /// Creates a default-deny policy from explicit bindings.
    #[must_use]
    pub fn new(bindings: Vec<PolicyBinding>) -> Self {
        Self { bindings }
    }

    /// Creates the development-only policy that grants one principal every action.
    #[must_use]
    pub fn development(principal_id: PrincipalId) -> Self {
        Self::new(vec![PolicyBinding::new(
            principal_id,
            Action::ALL.into_iter().collect(),
            StoreScope::Any,
        )])
    }

    /// Authorizes one operation without consulting resource existence.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError`] when no explicit binding grants the action and resource.
    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        store_id: Option<StoreId>,
    ) -> Result<(), AuthorizationError> {
        if action.is_system() != store_id.is_none() {
            return Err(AuthorizationError::InvalidResource);
        }
        if self
            .bindings
            .iter()
            .any(|binding| binding.permits(principal, action, store_id))
        {
            return Ok(());
        }
        Err(AuthorizationError::Forbidden)
    }
}

/// A redacted service-authorization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthorizationError {
    /// The authenticated principal has no matching grant.
    #[error("the principal is not authorized for this operation")]
    Forbidden,
    /// The caller supplied a system/store resource shape that does not match the action.
    #[error("the authorization resource is invalid")]
    InvalidResource,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use openfga_domain::{Principal, PrincipalKind};

    use super::{Action, AuthorizationError, AuthorizationPolicy, PolicyBinding, StoreScope};

    const ALLOWED_STORE: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const FORBIDDEN_STORE: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

    #[test]
    fn test_should_fail_closed_across_stores_without_existence_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let principal = Principal::new(PrincipalKind::PresharedKey, "reader".parse()?);
        let policy = AuthorizationPolicy::new(vec![PolicyBinding::new(
            "reader".parse()?,
            BTreeSet::from([Action::Read]),
            StoreScope::Stores(BTreeSet::from([ALLOWED_STORE.parse()?])),
        )]);

        assert!(
            policy
                .authorize(&principal, Action::Read, Some(ALLOWED_STORE.parse()?))
                .is_ok()
        );
        assert_eq!(
            policy.authorize(&principal, Action::Read, Some(FORBIDDEN_STORE.parse()?)),
            Err(AuthorizationError::Forbidden)
        );
        assert_eq!(
            policy.authorize(&principal, Action::GetStore, Some(FORBIDDEN_STORE.parse()?)),
            Err(AuthorizationError::Forbidden)
        );
        let debug = format!("{policy:?}");
        assert!(!debug.contains("reader"));
        assert!(!debug.contains(ALLOWED_STORE));
        assert!(!debug.contains(FORBIDDEN_STORE));
        Ok(())
    }

    #[test]
    fn test_should_require_wildcard_scope_for_system_actions()
    -> Result<(), Box<dyn std::error::Error>> {
        let principal = Principal::new(PrincipalKind::OpenIdConnect, "operator".parse()?);
        let scoped = AuthorizationPolicy::new(vec![PolicyBinding::new(
            "operator".parse()?,
            BTreeSet::from([Action::ListStores]),
            StoreScope::Stores(BTreeSet::new()),
        )]);
        let global = AuthorizationPolicy::new(vec![PolicyBinding::new(
            "operator".parse()?,
            BTreeSet::from([Action::ListStores]),
            StoreScope::Any,
        )]);

        assert_eq!(
            scoped.authorize(&principal, Action::ListStores, None),
            Err(AuthorizationError::Forbidden)
        );
        assert!(
            global
                .authorize(&principal, Action::ListStores, None)
                .is_ok()
        );
        Ok(())
    }
}
