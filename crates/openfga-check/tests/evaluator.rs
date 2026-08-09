//! End-to-end evaluator semantics over the actor-owned memory backend.

use std::{
    collections::BTreeMap,
    error::Error,
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use openfga_cache::{DecisionCache, DecisionCacheConfig, DecisionKeyHasher, InvalidationWatermark};
use openfga_check::{
    CachedCheckEvaluator, CheckBudget, CheckCoalescingConfig, CheckCoalescingMode, CheckError,
    CheckErrorKind, CheckEvaluator, CheckResolution, CoalescingCheckEvaluator,
    DirectCheckEvaluator,
};
use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand,
    ConditionBinding, ConditionContext, ConditionReference, ConsistencyPreference,
    ContextualTuples, Deadline, InputLimits, Limit, ModelSelection, Principal, PrincipalKind,
    QueryContext, RelationshipTuple, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, ModelCompiler,
    RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    ObjectRelationFilter, OperationContext, Page, PageOptions, ReadOptions, ReverseTupleFilter,
    StorageCancellationToken, StorageError, StorageErrorKind, StoreName, StoreWriter, StoredTuple,
    TupleReadFilter, TupleReader, TupleStream, TupleWriteOptions, TupleWriter, UsersetTupleFilter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use serde_json::json;
use tokio::sync::{Barrier, Notify, Semaphore};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[tokio::test]
async fn test_should_coalesce_simultaneous_identical_checks() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![tuple("document:coalesced#viewer@user:alice")?],
    )
    .await?;
    let oracle = Arc::new(BlockingCountingEvaluator::default());
    let evaluator = Arc::new(CoalescingCheckEvaluator::new(
        oracle.clone(),
        CheckCoalescingConfig::new(
            CheckCoalescingMode::Enabled,
            std::num::NonZeroU64::new(32).ok_or("invalid coalescing test capacity")?,
        )?,
        DecisionKeyHasher::random()?,
    ));
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:coalesced#viewer@user:alice".parse()?,
    ));
    let callers = 16_usize;
    let barrier = Arc::new(Barrier::new(callers + 1));
    let mut tasks = Vec::with_capacity(callers);
    for _ in 0..callers {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let storage: Arc<dyn TupleReader> = storage.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            evaluator
                .check(
                    &command,
                    model,
                    storage,
                    CheckBudget::default(),
                    None,
                    StorageCancellationToken::new(),
                )
                .await
        }));
    }
    barrier.wait().await;
    oracle.started.notified().await;
    tokio::task::yield_now().await;
    oracle.release.add_permits(1);
    wait_for_calls(&oracle.calls, 2).await?;
    oracle.release.add_permits(1);
    for task in tasks {
        let outcome = task.await??;
        assert!(outcome.allowed());
    }
    assert_eq!(oracle.calls.load(Ordering::Acquire), 2);

    drop(evaluator);
    drop(oracle);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_isolate_coalesced_follower_from_leader_cancellation()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![tuple("document:isolated#viewer@user:alice")?],
    )
    .await?;
    let oracle = Arc::new(BlockingCountingEvaluator::default());
    let evaluator = Arc::new(CoalescingCheckEvaluator::new(
        oracle.clone(),
        CheckCoalescingConfig::new(
            CheckCoalescingMode::Enabled,
            std::num::NonZeroU64::new(32).ok_or("invalid coalescing test capacity")?,
        )?,
        DecisionKeyHasher::random()?,
    ));
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:isolated#viewer@user:alice".parse()?,
    ));
    let leader_cancellation = StorageCancellationToken::new();
    let leader = {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let tuples: Arc<dyn TupleReader> = storage.clone();
        let cancellation = leader_cancellation.clone();
        tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    tuples,
                    CheckBudget::default(),
                    None,
                    cancellation,
                )
                .await
        })
    };
    oracle.started.notified().await;
    let follower = {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let tuples: Arc<dyn TupleReader> = storage.clone();
        tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    tuples,
                    CheckBudget::default(),
                    None,
                    StorageCancellationToken::new(),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    leader_cancellation.cancel();
    oracle.release.add_permits(1);
    wait_for_calls(&oracle.calls, 2).await?;
    oracle.release.add_permits(1);
    wait_for_calls(&oracle.calls, 3).await?;
    oracle.release.add_permits(1);

    let leader_error = leader.await?.err().ok_or("leader was not cancelled")?;
    assert_eq!(leader_error.kind(), CheckErrorKind::Cancelled);
    assert!(follower.await??.allowed());
    assert!(matches!(oracle.calls.load(Ordering::Acquire), 3 | 4));

    drop(evaluator);
    drop(oracle);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_return_cancelled_follower_without_waiting_for_leader()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let oracle = Arc::new(BlockingCountingEvaluator::default());
    let evaluator = Arc::new(CoalescingCheckEvaluator::new(
        oracle.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
    ));
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:cancelled-follower#viewer@user:alice".parse()?,
    ));
    let leader = {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let tuples: Arc<dyn TupleReader> = storage.clone();
        tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    tuples,
                    CheckBudget::default(),
                    None,
                    StorageCancellationToken::new(),
                )
                .await
        })
    };
    oracle.started.notified().await;
    let follower_cancellation = StorageCancellationToken::new();
    let follower = {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let tuples: Arc<dyn TupleReader> = storage.clone();
        let cancellation = follower_cancellation.clone();
        tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    tuples,
                    CheckBudget::default(),
                    None,
                    cancellation,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    follower_cancellation.cancel();
    let follower_error = tokio::time::timeout(Duration::from_secs(1), follower)
        .await??
        .err()
        .ok_or("cancelled follower unexpectedly succeeded")?;
    assert_eq!(follower_error.kind(), CheckErrorKind::Cancelled);
    assert_eq!(follower_error.code(), "check_cancelled");
    assert_eq!(oracle.calls.load(Ordering::Acquire), 3);

    oracle.release.add_permits(1);
    wait_for_calls(&oracle.calls, 2).await?;
    oracle.release.add_permits(1);
    assert!(!leader.await??.allowed());
    drop(evaluator);
    drop(oracle);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_preserve_production_cancellation_code_while_coalescing()
-> Result<(), Box<dyn Error>> {
    assert_production_coalescing_control(ControlExit::Cancellation).await
}

#[tokio::test]
async fn test_should_preserve_production_deadline_code_while_coalescing()
-> Result<(), Box<dyn Error>> {
    assert_production_coalescing_control(ControlExit::Deadline).await
}

#[tokio::test]
async fn test_should_not_join_in_flight_checks_across_tuple_mutations() -> Result<(), Box<dyn Error>>
{
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let key: TupleKey = "document:mutation#viewer@user:alice".parse()?;
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        key.clone(),
    ));

    let write_watermark = InvalidationWatermark::new();
    let write_oracle = Arc::new(BlockThirdCompletedEvaluation::default());
    let write_evaluator = Arc::new(CoalescingCheckEvaluator::with_invalidation(
        write_oracle.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
        write_watermark.clone(),
    ));
    let warm_write = evaluate_coalesced(&write_evaluator, &command, &model, &storage).await?;
    assert!(!warm_write.allowed());
    assert_eq!(write_oracle.calls.load(Ordering::Acquire), 2);
    let pre_write = spawn_check(
        Arc::clone(&write_evaluator),
        Arc::clone(&command),
        Arc::clone(&model),
        storage.clone(),
    );
    write_oracle.completed.notified().await;
    write_tuples(storage.as_ref(), vec![tuple(key.to_string().as_str())?]).await?;
    let _generation = write_watermark.advance();
    let post_write = evaluate_coalesced(&write_evaluator, &command, &model, &storage).await?;
    assert!(post_write.allowed());
    write_oracle.release.notify_one();
    assert!(!pre_write.await??.allowed());

    let delete_watermark = InvalidationWatermark::new();
    let delete_oracle = Arc::new(BlockThirdCompletedEvaluation::default());
    let delete_evaluator = Arc::new(CoalescingCheckEvaluator::with_invalidation(
        delete_oracle.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
        delete_watermark.clone(),
    ));
    let warm_delete = evaluate_coalesced(&delete_evaluator, &command, &model, &storage).await?;
    assert!(warm_delete.allowed());
    assert_eq!(delete_oracle.calls.load(Ordering::Acquire), 2);
    let pre_delete = spawn_check(
        Arc::clone(&delete_evaluator),
        Arc::clone(&command),
        Arc::clone(&model),
        storage.clone(),
    );
    delete_oracle.completed.notified().await;
    storage
        .write_tuples(
            &operation_context()?,
            STORE_ID.parse()?,
            vec![key],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    let _generation = delete_watermark.advance();
    let post_delete = evaluate_coalesced(&delete_evaluator, &command, &model, &storage).await?;
    assert!(!post_delete.allowed());
    delete_oracle.release.notify_one();
    assert!(pre_delete.await??.allowed());

    drop(write_evaluator);
    drop(write_oracle);
    drop(delete_evaluator);
    drop(delete_oracle);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_not_share_faults_or_cross_request_budgets() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![tuple("document:fault#viewer@user:alice")?],
    )
    .await?;
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:fault#viewer@user:alice".parse()?,
    ));

    let faulting = Arc::new(FailThirdBlockingEvaluator::default());
    let fault_isolating = Arc::new(CoalescingCheckEvaluator::new(
        faulting.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
    ));
    let warm_fault = fault_isolating
        .check(
            &command,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(warm_fault.allowed());
    assert_eq!(faulting.calls.load(Ordering::Acquire), 2);
    let leader = spawn_check(
        Arc::clone(&fault_isolating),
        Arc::clone(&command),
        Arc::clone(&model),
        storage.clone(),
    );
    faulting.fault_started.notified().await;
    let follower = spawn_check(
        Arc::clone(&fault_isolating),
        Arc::clone(&command),
        Arc::clone(&model),
        storage.clone(),
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(faulting.calls.load(Ordering::Acquire), 3);
    faulting.release_fault.notify_one();
    let leader_error = leader
        .await?
        .err()
        .ok_or("coalescing leader did not preserve the injected fault")?;
    assert_eq!(leader_error.kind(), CheckErrorKind::StorageUnavailable);
    assert_eq!(leader_error.code(), "tuple_storage_failed");
    assert!(follower.await??.allowed());
    assert_eq!(faulting.calls.load(Ordering::Acquire), 4);

    let oracle = Arc::new(BlockingCountingEvaluator::default());
    let evaluator = Arc::new(CoalescingCheckEvaluator::new(
        oracle.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
    ));
    let narrow_budget = CheckBudget::builder()
        .concurrent_reads(Limit::<1_024>::new(1)?)
        .build();
    let mut tasks = Vec::with_capacity(2);
    for budget in [narrow_budget, CheckBudget::default()] {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let tuples: Arc<dyn TupleReader> = storage.clone();
        tasks.push(tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    tuples,
                    budget,
                    None,
                    StorageCancellationToken::new(),
                )
                .await
        }));
    }
    wait_for_calls(&oracle.calls, 2).await?;
    oracle.release.add_permits(2);
    wait_for_calls(&oracle.calls, 3).await?;
    oracle.release.add_permits(1);
    for task in tasks {
        assert!(task.await??.allowed());
    }
    assert_eq!(oracle.calls.load(Ordering::Acquire), 3);

    drop(evaluator);
    drop(oracle);
    drop(fault_isolating);
    drop(faulting);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_shadow_oracle_across_rewrite_matrix() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:direct#viewer@user:alice")?,
            tuple("document:computed#owner@user:alice")?,
            tuple("document:both#owner@user:alice")?,
            tuple("document:both#editor@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("folder:roadmap#viewer@user:alice")?,
        ],
    )
    .await?;
    let strategy = CoalescingCheckEvaluator::new(
        Arc::new(DirectCheckEvaluator::default()),
        CheckCoalescingConfig::new(
            CheckCoalescingMode::Shadow,
            std::num::NonZeroU64::new(32).ok_or("invalid coalescing test capacity")?,
        )?,
        DecisionKeyHasher::random()?,
    );
    for (query, expected) in [
        ("document:direct#viewer@user:alice", true),
        ("document:missing#viewer@user:alice", false),
        ("document:computed#viewer@user:alice", true),
        ("document:both#both@user:alice", true),
        ("document:excluded#allowed@user:alice", false),
        ("document:ttu#viewer@user:alice", true),
        ("document:direct#viewer@user:bob", false),
    ] {
        let command = CheckCommand::new(
            query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
            query.parse()?,
        );
        let outcome = strategy
            .check(
                &command,
                Arc::clone(&model),
                storage.clone(),
                CheckBudget::default(),
                None,
                StorageCancellationToken::new(),
            )
            .await?;
        assert_eq!(
            outcome.allowed(),
            expected,
            "unexpected decision for {query}"
        );
        assert!(!strategy.is_killed(), "shadow mismatch for {query}");
    }

    drop(strategy);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_kill_enabled_coalescing_on_live_mismatch() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![tuple("document:mismatch#viewer@user:alice")?],
    )
    .await?;
    let oracle = Arc::new(FailSecondEvaluator::default());
    let strategy = CoalescingCheckEvaluator::new(
        oracle.clone(),
        coalescing_test_config()?,
        DecisionKeyHasher::random()?,
    );
    let command = CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:mismatch#viewer@user:alice".parse()?,
    );
    let outcome = strategy
        .check(
            &command,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(outcome.allowed());
    assert!(strategy.is_killed());
    assert_eq!(oracle.calls.load(Ordering::Acquire), 2);

    drop(strategy);
    drop(oracle);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_cache_complete_decisions_and_bypass_for_higher_consistency()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![tuple("document:cached#viewer@user:alice")?],
    )
    .await?;
    let decisions = DecisionCache::new(
        DecisionCacheConfig::new(
            std::num::NonZeroU64::new(100).ok_or("invalid test cache capacity")?,
            Duration::from_mins(1),
        )?,
        InvalidationWatermark::new(),
    );
    let evaluator = CachedCheckEvaluator::new(
        Arc::new(DirectCheckEvaluator::default()),
        decisions,
        DecisionKeyHasher::random()?,
        InputLimits::default(),
    );
    let minimize = CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::MinimizeLatency)?,
        "document:cached#viewer@user:alice".parse()?,
    );
    let first = evaluator
        .check(
            &minimize,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await?;
    let second = evaluator
        .check(
            &minimize,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(first.allowed());
    assert_eq!(second.resolution(), CheckResolution::Cached);

    storage
        .write_tuples(
            &operation_context()?,
            STORE_ID.parse()?,
            vec!["document:cached#viewer@user:alice".parse()?],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    let higher = CheckCommand::new(
        query_context_with_consistency(ConsistencyPreference::HigherConsistency)?,
        "document:cached#viewer@user:alice".parse()?,
    );
    let fresh = evaluator
        .check(
            &higher,
            model,
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(!fresh.allowed());
    assert_ne!(fresh.resolution(), CheckResolution::Cached);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_resolve_all_rewrites_usersets_wildcards_and_legal_recursion()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:direct#viewer@user:alice")?,
            tuple("document:wild#viewer@user:*")?,
            tuple("document:computed#owner@user:alice")?,
            tuple("document:userset#viewer@group:eng#member")?,
            tuple("group:eng#member@user:alice")?,
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("folder:roadmap#viewer@user:alice")?,
            tuple("document:both#owner@user:alice")?,
            tuple("document:both#editor@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
            tuple("document:included#viewer@user:alice")?,
            tuple("folder:cycle#parent@folder:cycle")?,
        ],
    )
    .await?;
    for query in [
        "document:direct#viewer@user:alice",
        "document:wild#viewer@user:bob",
        "document:wild#viewer@user:*",
        "document:computed#viewer@user:alice",
        "document:userset#viewer@user:alice",
        "document:userset#viewer@group:eng#member",
        "document:ttu#viewer@user:alice",
        "document:both#both@user:alice",
        "document:included#allowed@user:alice",
    ] {
        let outcome = evaluate(
            query,
            ContextualTuples::empty(),
            ConditionContext::empty(),
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
        assert!(outcome.allowed(), "expected allow for {query}");
    }

    for query in [
        "document:direct#viewer@user:bob",
        "document:both#both@user:bob",
        "document:excluded#allowed@user:alice",
        "folder:cycle#viewer@user:alice",
    ] {
        let outcome = evaluate(
            query,
            ContextualTuples::empty(),
            ConditionContext::empty(),
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
        assert!(!outcome.allowed(), "expected deny for {query}");
        if query.starts_with("folder:cycle#") {
            assert!(outcome.metadata().cycles() > 0);
        }
    }

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_overlay_conditions_and_suppress_losing_union_errors()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let empty = ConditionContext::empty();
    let tuple_override =
        ConditionContext::try_from_json(json!({"x": 50}), &InputLimits::default())?;
    let request_false =
        ConditionContext::try_from_json(json!({"x": 200}), &InputLimits::default())?;
    write_tuples(
        storage.as_ref(),
        vec![
            conditional_tuple("document:override#conditional@user:alice", tuple_override)?,
            conditional_tuple("document:missing#conditional@user:alice", empty.clone())?,
            conditional_tuple("document:guarded#conditional@user:alice", empty)?,
            tuple("document:guarded#owner@user:alice")?,
        ],
    )
    .await?;
    let override_outcome = evaluate(
        "document:override#conditional@user:alice",
        ContextualTuples::empty(),
        request_false,
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(override_outcome.allowed());
    assert!(override_outcome.metadata().condition_cost() > 0);

    let condition_budget = CheckBudget::builder()
        .condition_cost(Limit::<1_000_000>::new(1)?)
        .build();
    let budget_error = evaluate(
        "document:override#conditional@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        condition_budget,
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("condition cost exhaustion unexpectedly completed")?;
    assert_eq!(budget_error.kind(), CheckErrorKind::ConditionCostExceeded);

    let missing = evaluate(
        "document:missing#conditional@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing condition parameters unexpectedly denied")?;
    assert_eq!(missing.kind(), CheckErrorKind::Condition);

    let guarded = evaluate(
        "document:guarded#guarded@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(guarded.allowed());

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_merge_contextual_tuples_and_reject_invalid_contextual_shapes()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let contextual = ContextualTuples::new(
        vec![tuple("document:context#viewer@user:alice")?],
        &InputLimits::default(),
    )?;
    let outcome = evaluate(
        "document:context#viewer@user:alice",
        contextual,
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(outcome.allowed());
    assert_eq!(outcome.metadata().tuple_items(), 1);

    let invalid = ContextualTuples::new(
        vec![tuple("document:context#parent@user:alice")?],
        &InputLimits::default(),
    )?;
    let error = evaluate(
        "document:context#viewer@user:alice",
        invalid,
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid contextual tuple unexpectedly accepted")?;
    assert_eq!(error.kind(), CheckErrorKind::InvalidTuple);

    let error = evaluate(
        "document:context#undeclared@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("undeclared query relation unexpectedly accepted")?;
    assert_eq!(error.kind(), CheckErrorKind::InvalidModel);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_independent_budgets_and_skip_unreachable_reads()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:many#owner@user:alice")?,
            tuple("document:many#owner@user:bob")?,
            tuple("document:deep#viewer@group:g0#member")?,
            tuple("group:g0#member@group:g1#member")?,
            tuple("group:g1#member@user:alice")?,
        ],
    )
    .await?;
    let unreachable = evaluate(
        "document:none#viewer@service:worker",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(!unreachable.allowed());
    assert_eq!(unreachable.metadata().datastore_queries(), 0);

    let dispatch_budget = CheckBudget::builder()
        .dispatches(Limit::<1_000_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:none#owner@user:alice",
        Arc::clone(&model),
        storage.clone(),
        dispatch_budget,
        CheckErrorKind::DispatchExceeded,
    )
    .await?;

    let datastore_budget = CheckBudget::builder()
        .datastore_queries(Limit::<100_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:none#viewer@user:alice",
        Arc::clone(&model),
        storage.clone(),
        datastore_budget,
        CheckErrorKind::DatastoreQueryExceeded,
    )
    .await?;

    let tuple_budget = CheckBudget::builder()
        .tuple_items(Limit::<1_000_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:many#owner@user:nobody",
        Arc::clone(&model),
        storage.clone(),
        tuple_budget,
        CheckErrorKind::TupleItemExceeded,
    )
    .await?;

    let depth_budget = CheckBudget::builder()
        .depth(Limit::<1_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:deep#viewer@user:alice",
        model,
        storage.clone(),
        depth_budget,
        CheckErrorKind::DepthExceeded,
    )
    .await?;

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_root_cancellation_and_deadline() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let evaluator = DirectCheckEvaluator::default();
    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let error = evaluate(
        "document:none#owner@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        cancellation,
    )
    .await
    .err()
    .ok_or("cancelled check unexpectedly completed")?;
    assert_eq!(error.kind(), CheckErrorKind::Cancelled);

    let expired_deadline = Deadline::from_timeout(
        Instant::now(),
        openfga_domain::RequestTimeout::new(Duration::from_millis(1))?,
    )?;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let expired = CheckCommand::new(
        query_context_at(
            ContextualTuples::empty(),
            ConditionContext::empty(),
            expired_deadline,
        )?,
        "document:none#owner@user:alice".parse()?,
    );
    let error = evaluator
        .check(
            &expired,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("expired check unexpectedly completed")?;
    assert_eq!(error.kind(), CheckErrorKind::Timeout);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_bound_batch_concurrency_order_results_and_isolate_item_errors()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let evaluator = DirectCheckEvaluator::default();

    let first_context = ContextualTuples::new(
        vec![tuple("document:first#owner@user:alice")?],
        &InputLimits::default(),
    )?;
    let items = BatchCheckItems::new(
        vec![
            BatchCheckItem::new(
                "first".parse()?,
                "document:first#owner@user:alice".parse()?,
                first_context,
                ConditionContext::empty(),
            ),
            BatchCheckItem::new(
                "second".parse()?,
                "document:second#owner@user:alice".parse()?,
                ContextualTuples::empty(),
                ConditionContext::empty(),
            ),
        ],
        &InputLimits::default(),
    )?;
    let command = BatchCheckCommand::new(
        query_context(ContextualTuples::empty(), ConditionContext::empty())?,
        items,
    );
    let batch = evaluator
        .batch_check(
            &command,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::builder()
                .batch_concurrency(Limit::<1_000>::new(1)?)
                .build(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(batch.results().len(), 2);
    let first = batch
        .results()
        .first()
        .ok_or("first batch result missing")?;
    assert_eq!(first.correlation_id().as_str(), "first");
    assert!(matches!(first.outcome(), Ok(outcome) if outcome.allowed()));
    assert!(!format!("{first:?}").contains("first"));
    let second = batch
        .results()
        .get(1)
        .ok_or("second batch result missing")?;
    assert_eq!(second.correlation_id().as_str(), "second");
    assert!(matches!(second.outcome(), Ok(outcome) if !outcome.allowed()));

    let low_limits = InputLimits::builder()
        .context_bytes(Limit::<32_768>::new(12)?)
        .build();
    let base_context = ConditionContext::try_from_json(json!({"a": 1}), &low_limits)?;
    let item_context = ConditionContext::try_from_json(json!({"b": 2}), &low_limits)?;
    let item = BatchCheckItem::new(
        "overlay".parse()?,
        "document:none#owner@user:alice".parse()?,
        ContextualTuples::empty(),
        item_context,
    );
    let command = BatchCheckCommand::new(
        query_context(ContextualTuples::empty(), base_context)?,
        BatchCheckItems::new(vec![item], &low_limits)?,
    );
    let overlay = DirectCheckEvaluator::new(low_limits)
        .batch_check(
            &command,
            model,
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let item = overlay
        .results()
        .first()
        .ok_or("overlay batch result missing")?;
    assert!(matches!(item.outcome(), Err(error) if error.kind() == CheckErrorKind::InvalidTuple));

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_abort_and_join_short_circuited_datastore_reads() -> Result<(), Box<dyn Error>>
{
    let model = ModelCompiler::default().compile(&short_circuit_model()?)?;
    let reader = Arc::new(ShortCircuitReader::new(tuple(
        "document:one#viewer@user:alice",
    )?));
    let outcome = evaluate(
        "document:one#viewer@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        reader.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(outcome.allowed());
    assert_eq!(reader.calls.load(Ordering::Acquire), 2);
    assert_eq!(reader.active.load(Ordering::Acquire), 0);
    Ok(())
}

async fn assert_budget_error(
    query: &str,
    model: Arc<openfga_model::CompiledModel>,
    storage: Arc<MemoryStorage>,
    budget: CheckBudget,
    expected: CheckErrorKind,
) -> Result<(), Box<dyn Error>> {
    let error = evaluate(
        query,
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage,
        budget,
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("budgeted check unexpectedly completed")?;
    assert_eq!(error.kind(), expected);
    Ok(())
}

async fn evaluate(
    query: &str,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    model: Arc<openfga_model::CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    cancellation: StorageCancellationToken,
) -> Result<openfga_check::CheckOutcome, openfga_check::CheckError> {
    let command = CheckCommand::new(
        query_context(contextual_tuples, condition_context)
            .map_err(|_| openfga_check::CheckErrorKind::Internal)
            .map_err(|_| StorageError::new(StorageErrorKind::Internal, "test_query_context"))
            .map_err(openfga_check::CheckError::from)?,
        query
            .parse()
            .map_err(|_| StorageError::new(StorageErrorKind::Internal, "test_tuple"))
            .map_err(openfga_check::CheckError::from)?,
    );
    DirectCheckEvaluator::default()
        .check(&command, model, tuples, budget, None, cancellation)
        .await
}

fn query_context(
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
) -> Result<QueryContext, Box<dyn Error>> {
    query_context_at(
        contextual_tuples,
        condition_context,
        Deadline::from_timeout(
            Instant::now(),
            openfga_domain::RequestTimeout::new(Duration::from_secs(5))?,
        )?,
    )
}

fn query_context_at(
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    deadline: Deadline,
) -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(ConsistencyPreference::HigherConsistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(condition_context)
        .deadline(deadline)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "phase1-tests".parse()?,
        ))
        .build())
}

fn query_context_with_consistency(
    consistency: ConsistencyPreference,
) -> Result<QueryContext, Box<dyn Error>> {
    query_context_with_consistency_at(
        consistency,
        Deadline::from_timeout(
            Instant::now(),
            openfga_domain::RequestTimeout::new(Duration::from_secs(5))?,
        )?,
    )
}

fn query_context_with_consistency_at(
    consistency: ConsistencyPreference,
    deadline: Deadline,
) -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(consistency)
        .contextual_tuples(ContextualTuples::empty())
        .condition_context(ConditionContext::empty())
        .deadline(deadline)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "phase4-cache-tests".parse()?,
        ))
        .build())
}

async fn memory_storage() -> Result<Arc<MemoryStorage>, Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let context = operation_context()?;
    storage
        .create_store(
            &context,
            STORE_ID.parse()?,
            StoreName::new("check-tests".to_owned())?,
        )
        .await?;
    Ok(storage)
}

async fn shutdown(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error>> {
    let mut owner = Arc::try_unwrap(storage).map_err(|_| "memory storage still shared")?;
    owner.stop().await?;
    Ok(())
}

async fn write_tuples(
    storage: &MemoryStorage,
    tuples: Vec<RelationshipTuple>,
) -> Result<(), Box<dyn Error>> {
    let context = operation_context()?;
    storage
        .write_tuples(
            &context,
            STORE_ID.parse()?,
            Vec::new(),
            tuples,
            TupleWriteOptions::default(),
        )
        .await?;
    Ok(())
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(
            Instant::now(),
            openfga_domain::RequestTimeout::new(Duration::from_secs(5))?,
        )?,
        StorageCancellationToken::new(),
    ))
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse()?))
}

fn conditional_tuple(
    value: &str,
    context: ConditionContext,
) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::new(
        value.parse()?,
        ConditionReference::Conditional(ConditionBinding::new("under_limit".parse()?, context)),
    ))
}

fn complete_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let user = type_source("user", Vec::new())?;
    let service = type_source("service", Vec::new())?;
    let group = type_source(
        "group",
        vec![relation(
            "member",
            RewriteSource::Direct,
            vec![object("user", None)?, userset("group", "member", None)?],
        )?],
    )?;
    let folder = type_source(
        "folder",
        vec![
            relation(
                "parent",
                RewriteSource::Direct,
                vec![object("folder", None)?],
            )?,
            relation(
                "viewer",
                RewriteSource::Union(vec![RewriteSource::Direct, ttu("parent", "viewer")?]),
                vec![object("user", None)?, wildcard("user")?],
            )?,
        ],
    )?;
    let document = type_source(
        "document",
        vec![
            relation("owner", RewriteSource::Direct, vec![object("user", None)?])?,
            relation("editor", RewriteSource::Direct, vec![object("user", None)?])?,
            relation("banned", RewriteSource::Direct, vec![object("user", None)?])?,
            relation(
                "conditional",
                RewriteSource::Direct,
                vec![object("user", Some("under_limit"))?],
            )?,
            relation(
                "parent",
                RewriteSource::Direct,
                vec![object("folder", None)?],
            )?,
            relation(
                "viewer",
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    computed("owner")?,
                    ttu("parent", "viewer")?,
                ]),
                vec![
                    object("user", None)?,
                    wildcard("user")?,
                    userset("group", "member", None)?,
                ],
            )?,
            relation(
                "guarded",
                RewriteSource::Union(vec![computed("conditional")?, computed("owner")?]),
                Vec::new(),
            )?,
            relation(
                "both",
                RewriteSource::Intersection(vec![computed("owner")?, computed("editor")?]),
                Vec::new(),
            )?,
            relation(
                "allowed",
                RewriteSource::Difference {
                    base: Box::new(computed("viewer")?),
                    subtract: Box::new(computed("banned")?),
                },
                Vec::new(),
            )?,
        ],
    )?;
    let parameters = BTreeMap::from([("x".parse()?, ParameterType::int())]);
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![user, service, group, folder, document],
        vec![ConditionSource::new(
            "under_limit".parse()?,
            ConditionDefinition::new("under_limit".parse()?, "x < 100".to_owned(), parameters),
        )],
    ))
}

fn short_circuit_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![
            type_source("user", Vec::new())?,
            type_source(
                "document",
                vec![relation(
                    "viewer",
                    RewriteSource::Union(vec![RewriteSource::Direct, RewriteSource::Direct]),
                    vec![object("user", None)?],
                )?],
            )?,
        ],
        Vec::new(),
    ))
}

fn type_source(
    name: &str,
    relations: Vec<RelationSource>,
) -> Result<TypeDefinitionSource, Box<dyn Error>> {
    Ok(TypeDefinitionSource::new(name.parse()?, relations))
}

fn relation(
    name: &str,
    rewrite: RewriteSource,
    restrictions: Vec<DirectRestrictionSource>,
) -> Result<RelationSource, Box<dyn Error>> {
    Ok(RelationSource::new(name.parse()?, rewrite, restrictions))
}

fn object(
    subject_type: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Object,
        condition.map(str::parse).transpose()?,
    ))
}

fn wildcard(subject_type: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Wildcard,
        None,
    ))
}

fn userset(
    subject_type: &str,
    relation: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Userset(relation.parse()?),
        condition.map(str::parse).transpose()?,
    ))
}

fn computed(relation: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::Computed(relation.parse()?))
}

fn ttu(tupleset: &str, computed: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::TupleToUserset {
        tupleset: tupleset.parse()?,
        computed: computed.parse()?,
    })
}

fn coalescing_test_config() -> Result<CheckCoalescingConfig, Box<dyn Error>> {
    Ok(CheckCoalescingConfig::new(
        CheckCoalescingMode::Enabled,
        std::num::NonZeroU64::new(32).ok_or("invalid coalescing test capacity")?,
    )?)
}

#[derive(Clone, Copy, Debug)]
enum ControlExit {
    Cancellation,
    Deadline,
}

async fn assert_production_coalescing_control(control: ControlExit) -> Result<(), Box<dyn Error>> {
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let reader = Arc::new(PendingTupleReader::default());
    let invalidation = InvalidationWatermark::new();
    let cached: Arc<dyn CheckEvaluator> = Arc::new(CachedCheckEvaluator::new(
        Arc::new(DirectCheckEvaluator::default()),
        DecisionCache::new(
            DecisionCacheConfig::new(
                std::num::NonZeroU64::new(100).ok_or("invalid test cache capacity")?,
                Duration::from_mins(1),
            )?,
            invalidation.clone(),
        ),
        DecisionKeyHasher::random()?,
        InputLimits::default(),
    ));
    let evaluator = Arc::new(CoalescingCheckEvaluator::with_invalidation(
        cached,
        CheckCoalescingConfig::new(
            CheckCoalescingMode::Shadow,
            std::num::NonZeroU64::new(32).ok_or("invalid coalescing test capacity")?,
        )?,
        DecisionKeyHasher::random()?,
        invalidation,
    ));
    let timeout = match control {
        ControlExit::Cancellation => Duration::from_secs(5),
        ControlExit::Deadline => Duration::from_millis(25),
    };
    let command = Arc::new(CheckCommand::new(
        query_context_with_consistency_at(
            ConsistencyPreference::MinimizeLatency,
            Deadline::from_timeout(
                Instant::now(),
                openfga_domain::RequestTimeout::new(timeout)?,
            )?,
        )?,
        "document:control#owner@user:alice".parse()?,
    ));
    let cancellation = StorageCancellationToken::new();
    let evaluation = {
        let evaluator = Arc::clone(&evaluator);
        let command = Arc::clone(&command);
        let model = Arc::clone(&model);
        let reader: Arc<dyn TupleReader> = reader.clone();
        let request_cancellation = cancellation.clone();
        tokio::spawn(async move {
            evaluator
                .check(
                    &command,
                    model,
                    reader,
                    CheckBudget::default(),
                    None,
                    request_cancellation,
                )
                .await
        })
    };
    reader.started.notified().await;
    if matches!(control, ControlExit::Cancellation) {
        cancellation.cancel();
    }
    let error = tokio::time::timeout(Duration::from_secs(1), evaluation)
        .await??
        .err()
        .ok_or("controlled evaluation unexpectedly succeeded")?;
    let (kind, code) = match control {
        ControlExit::Cancellation => (CheckErrorKind::Cancelled, "check_cancelled"),
        ControlExit::Deadline => (CheckErrorKind::Timeout, "check_deadline_elapsed"),
    };
    assert_eq!(error.kind(), kind);
    assert_eq!(error.code(), code);
    assert!(!evaluator.is_killed());
    Ok(())
}

#[derive(Debug, Default)]
struct FailThirdBlockingEvaluator {
    delegate: DirectCheckEvaluator,
    calls: AtomicUsize,
    fault_started: Notify,
    release_fault: Notify,
}

#[derive(Debug, Default)]
struct FailSecondEvaluator {
    delegate: DirectCheckEvaluator,
    calls: AtomicUsize,
}

#[async_trait]
impl CheckEvaluator for FailSecondEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<openfga_check::CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::CheckOutcome, CheckError> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 1 {
            return Err(StorageError::new(StorageErrorKind::Unavailable, "injected").into());
        }
        self.delegate
            .check(command, model, tuples, budget, work_meter, cancellation)
            .await
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

#[async_trait]
impl CheckEvaluator for FailThirdBlockingEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<openfga_check::CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::CheckOutcome, CheckError> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 2 {
            self.fault_started.notify_one();
            self.release_fault.notified().await;
            return Err(StorageError::new(StorageErrorKind::Unavailable, "injected").into());
        }
        self.delegate
            .check(command, model, tuples, budget, work_meter, cancellation)
            .await
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

#[derive(Debug)]
struct BlockingCountingEvaluator {
    delegate: DirectCheckEvaluator,
    calls: AtomicUsize,
    started: Notify,
    release: Semaphore,
}

impl Default for BlockingCountingEvaluator {
    fn default() -> Self {
        Self {
            delegate: DirectCheckEvaluator::default(),
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

#[derive(Debug, Default)]
struct BlockThirdCompletedEvaluation {
    delegate: DirectCheckEvaluator,
    calls: AtomicUsize,
    completed: Notify,
    release: Notify,
}

#[derive(Debug, Default)]
struct PendingTupleReader {
    started: Notify,
}

#[async_trait]
impl TupleReader for PendingTupleReader {
    async fn read_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &TupleReadFilter,
        _options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        Err(unsupported())
    }

    async fn read_exact_tuple(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        Err(unsupported())
    }

    async fn read_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.started.notify_one();
        pending().await
    }

    async fn read_userset_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &UsersetTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn read_reverse_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ReverseTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn tuple_exists(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<bool, StorageError> {
        Err(unsupported())
    }

    async fn count_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        Err(unsupported())
    }
}

#[async_trait]
impl CheckEvaluator for BlockThirdCompletedEvaluation {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<openfga_check::CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::CheckOutcome, CheckError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        let outcome = self
            .delegate
            .check(command, model, tuples, budget, work_meter, cancellation)
            .await;
        if call == 2 {
            self.completed.notify_one();
            self.release.notified().await;
        }
        outcome
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

fn spawn_check(
    evaluator: Arc<CoalescingCheckEvaluator>,
    command: Arc<CheckCommand>,
    model: Arc<openfga_model::CompiledModel>,
    storage: Arc<MemoryStorage>,
) -> tokio::task::JoinHandle<Result<openfga_check::CheckOutcome, CheckError>> {
    tokio::spawn(async move {
        evaluator
            .check(
                &command,
                model,
                storage,
                CheckBudget::default(),
                None,
                StorageCancellationToken::new(),
            )
            .await
    })
}

async fn evaluate_coalesced(
    evaluator: &CoalescingCheckEvaluator,
    command: &CheckCommand,
    model: &Arc<openfga_model::CompiledModel>,
    storage: &Arc<MemoryStorage>,
) -> Result<openfga_check::CheckOutcome, CheckError> {
    evaluator
        .check(
            command,
            Arc::clone(model),
            storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        )
        .await
}

#[async_trait]
impl CheckEvaluator for BlockingCountingEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<openfga_check::CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::CheckOutcome, CheckError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.started.notify_one();
        if !cancellation.is_cancelled() {
            tokio::select! {
                permit = self.release.acquire() => {
                    let permit = permit.map_err(|_| {
                        StorageError::new(StorageErrorKind::Unavailable, "test_closed")
                    })?;
                    permit.forget();
                }
                () = cancellation.cancelled() => {}
            }
        }
        self.delegate
            .check(command, model, tuples, budget, work_meter, cancellation)
            .await
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<openfga_model::CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<openfga_check::BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

async fn wait_for_calls(calls: &AtomicUsize, expected: usize) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::Acquire) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[derive(Debug)]
struct ShortCircuitReader {
    allowed: RelationshipTuple,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    barrier: Arc<Barrier>,
}

impl ShortCircuitReader {
    fn new(allowed: RelationshipTuple) -> Self {
        Self {
            allowed,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(Barrier::new(2)),
        }
    }
}

struct ActiveRead(Arc<AtomicUsize>);

impl ActiveRead {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}

impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[async_trait]
impl TupleReader for ShortCircuitReader {
    async fn read_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &TupleReadFilter,
        _options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        Err(unsupported())
    }

    async fn read_exact_tuple(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        Err(unsupported())
    }

    async fn read_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveRead::new(Arc::clone(&self.active));
        self.barrier.wait().await;
        if call == 0 {
            Ok(TupleStream::from_tuples(vec![self.allowed.clone()]))
        } else {
            pending::<Result<TupleStream, StorageError>>().await
        }
    }

    async fn read_userset_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &UsersetTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn read_reverse_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ReverseTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn tuple_exists(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<bool, StorageError> {
        Err(unsupported())
    }

    async fn count_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        Err(unsupported())
    }
}

const fn unsupported() -> StorageError {
    StorageError::new(StorageErrorKind::Internal, "unsupported_test_read")
}
