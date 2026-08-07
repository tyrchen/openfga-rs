//! Reverse-candidate traversal over the actor-owned storage contract.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use openfga_check::{
    BatchCheckOutcome, CheckBudget, CheckError, CheckEvaluator, CheckOutcome, DirectCheckEvaluator,
};
use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, BatchCheckCommand, CheckCommand, ConditionBinding, ConditionContext,
    ConditionReference, ConsistencyPreference, ContextValue, ContextualTuples, Deadline,
    ExpandCommand, InputLimits, Limit, ListControl, ListObjectsCommand, ListUsersCommand,
    ModelSelection, Principal, PrincipalKind, QueryContext, RelationshipTuple, RequestTimeout,
    StoreId, TupleKey, UserTypeFilter, UserTypeFilters,
};
use openfga_list::{
    Candidate, CandidateBudget, DirectExpandEngine, DirectListObjectsEngine, DirectListUsersEngine,
    ExpandBudget, ExpandEngine, ExpandNodeValue, ListErrorKind, ListObjectsBudget,
    ListObjectsEngine, ListUsersBudget, ListUsersEngine, ReverseCandidateTraversal,
};
use openfga_model::{
    AuthorizationModelSource, CompiledModel, ConditionSource, DirectRestrictionSource,
    ModelCompiler, RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    OperationContext, StorageCancellationToken, StoreName, StoreWriter, TupleReader,
    TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use proptest::{
    prelude::*,
    strategy::{Strategy, ValueTree},
    test_runner::{Config as ProptestConfig, RngSeed, TestRunner},
};
use tokio_stream::StreamExt;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[tokio::test]
async fn test_should_find_direct_computed_userset_ttu_wildcard_and_recursive_candidates()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:direct#viewer@user:alice")?,
            tuple("document:wild#viewer@user:*")?,
            tuple("document:computed#owner@user:alice")?,
            tuple("document:userset#viewer@group:eng#member")?,
            tuple("document:nested#viewer@group:leads#member")?,
            tuple("group:eng#member@user:alice")?,
            tuple("group:leads#member@group:eng#member")?,
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("folder:roadmap#viewer@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let contextual = ContextualTuples::new(
        vec![tuple("document:contextual#viewer@user:alice")?],
        &InputLimits::default(),
    )?;
    let command = command("viewer", contextual)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let candidates = ReverseCandidateTraversal::default()
        .traverse(
            &command,
            model,
            tuple_reader,
            CandidateBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let actual = candidates
        .candidates()
        .iter()
        .map(|candidate| candidate.object().to_string())
        .collect::<BTreeSet<_>>();
    let expected = [
        "document:computed",
        "document:contextual",
        "document:direct",
        "document:nested",
        "document:ttu",
        "document:userset",
        "document:wild",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    assert!(candidates.metadata().datastore_queries() > 0);
    assert!(candidates.metadata().maximum_depth() > 0);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_mark_ambiguous_candidates_and_fail_closed_on_limits_and_cancellation()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:both#owner@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let intersection = ReverseCandidateTraversal::default()
        .traverse(
            &command("both", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            CandidateBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(intersection.candidates().len(), 1);
    assert!(intersection.candidates()[0].requires_check());

    let difference = ReverseCandidateTraversal::default()
        .traverse(
            &command("allowed", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            CandidateBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(difference.candidates().len(), 2);
    assert!(
        difference
            .candidates()
            .iter()
            .all(Candidate::requires_check)
    );
    assert!(
        difference
            .candidates()
            .iter()
            .any(|candidate| { candidate.object().to_string() == "document:excluded" })
    );

    let budget = CandidateBudget::builder()
        .candidates(Limit::<100_000>::new(1)?)
        .build();
    let error = ReverseCandidateTraversal::default()
        .traverse(
            &command("viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            budget,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("candidate ceiling unexpectedly returned a partial result")?;
    assert_eq!(error.kind(), ListErrorKind::CandidateExceeded);

    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let error = ReverseCandidateTraversal::default()
        .traverse(
            &command("viewer", ContextualTuples::empty())?,
            model,
            tuple_reader,
            CandidateBudget::default(),
            cancellation,
        )
        .await
        .err()
        .ok_or("cancelled traversal unexpectedly completed")?;
    assert_eq!(error.kind(), ListErrorKind::Cancelled);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_residual_check_ambiguous_candidates_for_unary_and_streaming_results()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:included#owner@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let engine = DirectListObjectsEngine::default();
    let query = command("allowed", ContextualTuples::empty())?;
    let outcome = engine
        .list_objects(
            &query,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListObjectsBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(
        outcome
            .objects()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["document:included"],
    );
    assert_eq!(outcome.metadata().residual_checks(), 2);

    let mut stream = engine
        .streamed_list_objects(
            &query,
            model,
            tuple_reader,
            ListObjectsBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let mut streamed = Vec::new();
    while let Some(item) = stream.next().await {
        streamed.push(item?.to_string());
    }
    assert_eq!(streamed, vec!["document:included"]);
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_cancel_and_join_residual_checks_when_stream_is_dropped()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:included#owner@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let evaluator = Arc::new(BlockingCheckEvaluator::default());
    let engine = DirectListObjectsEngine::new(InputLimits::default(), evaluator.clone());
    let stream = engine
        .streamed_list_objects(
            &command("allowed", ContextualTuples::empty())?,
            model,
            tuple_reader,
            ListObjectsBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;

    tokio::time::timeout(Duration::from_secs(1), async {
        while evaluator.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), async {
        while evaluator.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_backpressure_a_slow_stream_consumer_and_release_the_producer()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:first#viewer@user:alice")?,
            tuple("document:second#viewer@user:alice")?,
            tuple("document:third#viewer@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let evaluator = Arc::new(CountingCheckEvaluator::default());
    let engine = DirectListObjectsEngine::new(InputLimits::default(), evaluator.clone());
    let budget = ListObjectsBudget::builder()
        .residual_concurrency(Limit::<1_024>::new(1)?)
        .stream_buffer(Limit::<1_024>::new(1)?)
        .build();
    let stream = engine
        .streamed_list_objects(
            &command("allowed", ContextualTuples::empty())?,
            model,
            tuple_reader,
            budget,
            StorageCancellationToken::new(),
        )
        .await?;

    tokio::time::timeout(Duration::from_secs(1), async {
        while evaluator.completed.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        evaluator.completed.load(Ordering::SeqCst),
        2,
        "a full stream buffer must stop the producer before the third residual Check",
    );

    drop(engine);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&evaluator) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_match_check_over_seeded_generated_enumeration_sets()
-> Result<(), Box<dyn Error>> {
    const SEED: u64 = 0x5eed_f6a3_0003_0005;
    const CASES: u32 = 64;
    let strategy = prop::collection::vec(any::<(bool, bool, bool, bool)>(), 4);
    let mut runner = TestRunner::new(ProptestConfig {
        cases: CASES,
        rng_seed: RngSeed::Fixed(SEED),
        ..ProptestConfig::default()
    });

    for case_index in 0..CASES {
        let flags = strategy
            .new_tree(&mut runner)
            .map_err(|error| format!("generated case {case_index} seed {SEED:#x}: {error}"))?
            .current();
        verify_generated_enumeration_case(case_index, SEED, &flags).await?;
    }
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the rewrite compatibility matrix shares one immutable model and tuple fixture"
)]
async fn test_should_list_filtered_users_across_rewrites_conditions_wildcards_and_cycles()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:direct#viewer@user:alice")?,
            tuple("document:wild#viewer@user:*")?,
            tuple("document:computed#owner@user:alice")?,
            tuple("document:userset#viewer@group:eng#member")?,
            tuple("document:nested#viewer@group:leads#member")?,
            tuple("group:eng#member@user:alice")?,
            tuple("group:leads#member@group:eng#member")?,
            tuple("group:cycle#member@group:cycle#member")?,
            tuple("document:cycle#viewer@group:cycle#member")?,
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("folder:roadmap#viewer@user:alice")?,
            tuple("document:intersection#owner@user:*")?,
            tuple("document:intersection#editor@user:alice")?,
            tuple("document:included#owner@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
            tuple("document:public#viewer@user:*")?,
            tuple("document:public#banned@user:alice")?,
            conditional_tuple("document:conditional#conditional@user:alice", 10)?,
            conditional_tuple("document:blocked#conditional@user:alice", 100)?,
            tuple("document:bounded#viewer@user:alice")?,
            tuple("document:bounded#viewer@user:bob")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let engine = DirectListUsersEngine::default();
    let user_filter = UserTypeFilter::new("user".parse()?, None);
    for (object, relation, expected) in [
        ("document:direct", "viewer", vec!["user:alice"]),
        ("document:wild", "viewer", vec!["user:*"]),
        ("document:computed", "viewer", vec!["user:alice"]),
        ("document:userset", "viewer", vec!["user:alice"]),
        ("document:nested", "viewer", vec!["user:alice"]),
        ("document:cycle", "viewer", Vec::new()),
        ("document:ttu", "viewer", vec!["user:alice"]),
        ("document:intersection", "both", vec!["user:alice"]),
        ("document:included", "allowed", vec!["user:alice"]),
        ("document:excluded", "allowed", Vec::new()),
        ("document:public", "allowed", vec!["user:*"]),
        ("document:conditional", "conditional", vec!["user:alice"]),
        ("document:blocked", "conditional", Vec::new()),
    ] {
        let outcome = engine
            .list_users(
                &users_command(
                    object,
                    relation,
                    vec![user_filter.clone()],
                    ContextualTuples::empty(),
                    ConditionContext::empty(),
                )?,
                Arc::clone(&model),
                Arc::clone(&tuple_reader),
                ListUsersBudget::default(),
                StorageCancellationToken::new(),
            )
            .await?;
        assert_eq!(
            outcome
                .users()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            expected,
            "unexpected users for {object}#{relation}",
        );
    }

    let userset_filter = UserTypeFilter::new("group".parse()?, Some("member".parse()?));
    let outcome = engine
        .list_users(
            &users_command(
                "document:userset",
                "viewer",
                vec![userset_filter],
                ContextualTuples::empty(),
                ConditionContext::empty(),
            )?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListUsersBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(
        outcome
            .users()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["group:eng#member"],
    );

    let contextual = ContextualTuples::new(
        vec![tuple("document:contextual-users#viewer@user:alice")?],
        &InputLimits::default(),
    )?;
    let outcome = engine
        .list_users(
            &users_command(
                "document:contextual-users",
                "viewer",
                vec![user_filter.clone()],
                contextual,
                ConditionContext::empty(),
            )?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListUsersBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(
        outcome
            .users()
            .first()
            .ok_or("contextual ListUsers result was empty")?
            .to_string(),
        "user:alice",
    );

    let budget = ListUsersBudget::builder()
        .subjects(Limit::<100_000>::new(1)?)
        .build();
    let error = engine
        .list_users(
            &users_command(
                "document:bounded",
                "viewer",
                vec![user_filter],
                ContextualTuples::empty(),
                ConditionContext::empty(),
            )?,
            model,
            tuple_reader,
            budget,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("ListUsers subject ceiling unexpectedly returned a partial result")?;
    assert_eq!(error.kind(), ListErrorKind::SubjectExceeded);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_list_users_controls_and_combine_multiple_filters()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:controls#viewer@user:alice")?,
            tuple("document:controls#viewer@user:bob")?,
            tuple("document:controls#viewer@group:eng#member")?,
            tuple("group:eng#member@user:alice")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let engine = DirectListUsersEngine::default();
    let user_filter = UserTypeFilter::new("user".parse()?, None);
    let userset_filter = UserTypeFilter::new("group".parse()?, Some("member".parse()?));

    let outcome = engine
        .list_users(
            &users_command(
                "document:controls",
                "viewer",
                vec![user_filter.clone(), userset_filter],
                ContextualTuples::empty(),
                ConditionContext::empty(),
            )?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListUsersBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(
        outcome
            .users()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["user:alice", "user:bob", "group:eng#member"],
    );

    let outcome = engine
        .list_users(
            &users_command_with_limit(
                "document:controls",
                "viewer",
                vec![user_filter.clone()],
                ContextualTuples::empty(),
                ConditionContext::empty(),
                1,
            )?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListUsersBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(outcome.users().len(), 1);
    assert_eq!(outcome.metadata().results(), 1);
    assert!(outcome.metadata().truncated());

    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let error = engine
        .list_users(
            &users_command(
                "document:controls",
                "viewer",
                vec![user_filter],
                ContextualTuples::empty(),
                ConditionContext::empty(),
            )?,
            model,
            tuple_reader,
            ListUsersBudget::default(),
            cancellation,
        )
        .await
        .err()
        .ok_or("cancelled ListUsers unexpectedly completed")?;
    assert_eq!(error.kind(), ListErrorKind::Cancelled);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the baseline tree-shape matrix shares one immutable model and tuple fixture"
)]
async fn test_should_build_baseline_compatible_expand_tree_shapes() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("document:bounded#viewer@user:bob")?,
            tuple("document:bounded#viewer@user:alice")?,
            tuple("document:ordering#viewer@user:zeta")?,
            tuple("document:ordering#viewer@user:*")?,
            tuple("document:ordering#viewer@group:eng#member")?,
            conditional_tuple("document:conditional#conditional@user:alice", 100)?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let engine = DirectExpandEngine::default();

    let outcome = engine
        .expand(
            &expand_command("document:ttu", "viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ExpandBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let ExpandNodeValue::Union(children) = outcome.root().value() else {
        return Err("viewer expansion was not a union".into());
    };
    let ttu = children.get(2).ok_or("viewer expansion omitted TTU")?;
    let ExpandNodeValue::TupleToUserset { tupleset, computed } = ttu.value() else {
        return Err("viewer TTU child had the wrong shape".into());
    };
    assert_eq!(tupleset.to_string(), "document:ttu#parent");
    assert_eq!(
        computed.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["folder:roadmap#viewer"],
    );

    let outcome = engine
        .expand(
            &expand_command("document:bounded", "viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ExpandBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let ExpandNodeValue::Union(children) = outcome.root().value() else {
        return Err("bounded viewer expansion was not a union".into());
    };
    let direct = children
        .first()
        .ok_or("viewer expansion omitted direct users")?;
    let ExpandNodeValue::Users(users) = direct.value() else {
        return Err("viewer direct child had the wrong shape".into());
    };
    assert_eq!(
        users.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["user:alice", "user:bob"],
    );

    let outcome = engine
        .expand(
            &expand_command("document:ordering", "viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ExpandBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let ExpandNodeValue::Union(children) = outcome.root().value() else {
        return Err("ordering viewer expansion was not a union".into());
    };
    let direct = children
        .first()
        .ok_or("ordering expansion omitted direct users")?;
    let ExpandNodeValue::Users(users) = direct.value() else {
        return Err("ordering direct child had the wrong shape".into());
    };
    assert_eq!(
        users.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["group:eng#member", "user:*", "user:zeta"],
    );

    for (relation, expected) in [("both", "intersection"), ("allowed", "difference")] {
        let outcome = engine
            .expand(
                &expand_command("document:bounded", relation, ContextualTuples::empty())?,
                Arc::clone(&model),
                Arc::clone(&tuple_reader),
                ExpandBudget::default(),
                StorageCancellationToken::new(),
            )
            .await?;
        let actual = match outcome.root().value() {
            ExpandNodeValue::Intersection(_) => "intersection",
            ExpandNodeValue::Difference { .. } => "difference",
            _ => "other",
        };
        assert_eq!(actual, expected);
    }

    let contextual = ContextualTuples::new(
        vec![tuple("document:contextual-users#viewer@user:alice")?],
        &InputLimits::default(),
    )?;
    let outcome = engine
        .expand(
            &expand_command("document:contextual-users", "viewer", contextual)?,
            model,
            tuple_reader,
            ExpandBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(outcome.metadata().nodes() > 0);
    assert!(outcome.metadata().estimated_response_bytes() > 0);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_fail_expand_closed_on_resource_limits_and_cancellation()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    write_tuples(
        storage.as_ref(),
        vec![
            conditional_tuple("document:conditional#conditional@user:alice", 100)?,
            tuple("document:conditional#conditional@folder:roadmap")?,
        ],
    )
    .await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let engine = DirectExpandEngine::default();
    let command = expand_command(
        "document:conditional",
        "conditional",
        ContextualTuples::empty(),
    )?;

    let outcome = engine
        .expand(
            &command,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ExpandBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let ExpandNodeValue::Users(users) = outcome.root().value() else {
        return Err("conditional direct expansion had the wrong shape".into());
    };
    assert_eq!(
        users.len(),
        1,
        "Expand must retain conditioned tuples and ignore invalid persisted tuples",
    );

    let node_budget = ExpandBudget::builder()
        .nodes(Limit::<100_000>::new(1)?)
        .build();
    let error = engine
        .expand(
            &expand_command("document:conditional", "viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            node_budget,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("Expand node limit unexpectedly returned a partial tree")?;
    assert_eq!(error.kind(), ListErrorKind::NodeExceeded);

    let depth_budget = ExpandBudget::builder()
        .depth(Limit::<1_000>::new(1)?)
        .build();
    let error = engine
        .expand(
            &expand_command("document:conditional", "viewer", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            depth_budget,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("Expand depth limit unexpectedly returned a partial tree")?;
    assert_eq!(error.kind(), ListErrorKind::DepthExceeded);

    let response_budget = ExpandBudget::builder()
        .response_bytes(Limit::<16_777_216>::new(1)?)
        .build();
    let error = engine
        .expand(
            &command,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            response_budget,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("Expand response limit unexpectedly returned a partial tree")?;
    assert_eq!(error.kind(), ListErrorKind::ResponseSizeExceeded);

    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let error = engine
        .expand(
            &command,
            model,
            tuple_reader,
            ExpandBudget::default(),
            cancellation,
        )
        .await
        .err()
        .ok_or("cancelled Expand unexpectedly completed")?;
    assert_eq!(error.kind(), ListErrorKind::Cancelled);

    shutdown(storage).await?;
    Ok(())
}

#[derive(Debug, Default)]
struct BlockingCheckEvaluator {
    active: AtomicUsize,
    delegate: DirectCheckEvaluator,
}

#[derive(Debug, Default)]
struct CountingCheckEvaluator {
    completed: AtomicUsize,
    delegate: DirectCheckEvaluator,
}

#[async_trait]
impl CheckEvaluator for CountingCheckEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        let outcome = self
            .delegate
            .check(command, model, tuples, budget, cancellation)
            .await?;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(outcome)
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

#[async_trait]
impl CheckEvaluator for BlockingCheckEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCheck(&self.active);
        cancellation.cancelled().await;
        self.delegate
            .check(command, model, tuples, budget, cancellation)
            .await
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError> {
        self.delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

#[derive(Debug)]
struct ActiveCheck<'a>(&'a AtomicUsize);

impl Drop for ActiveCheck<'_> {
    fn drop(&mut self) {
        let _previous = self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn verify_generated_enumeration_case(
    case_index: u32,
    seed: u64,
    flags: &[(bool, bool, bool, bool)],
) -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let mut tuples = Vec::with_capacity(flags.len().saturating_mul(4));
    for (document_index, &(viewer, owner, wildcard, banned)) in flags.iter().enumerate() {
        let object = format!("document:generated-{document_index}");
        if viewer {
            tuples.push(tuple(&format!("{object}#viewer@user:alice"))?);
        }
        if owner {
            tuples.push(tuple(&format!("{object}#owner@user:alice"))?);
        }
        if wildcard {
            tuples.push(tuple(&format!("{object}#viewer@user:*"))?);
        }
        if banned {
            tuples.push(tuple(&format!("{object}#banned@user:alice"))?);
        }
    }
    write_tuples(storage.as_ref(), tuples).await?;
    let model = ModelCompiler::default().compile(&model()?)?;
    let tuple_reader: Arc<dyn TupleReader> = storage.clone();
    let check = DirectCheckEvaluator::default();
    let mut expected_objects = BTreeSet::new();
    for document_index in 0..flags.len() {
        let object = format!("document:generated-{document_index}");
        let outcome = check
            .check(
                &CheckCommand::new(
                    query_context(ContextualTuples::empty(), ConditionContext::empty())?,
                    format!("{object}#allowed@user:alice").parse()?,
                ),
                Arc::clone(&model),
                Arc::clone(&tuple_reader),
                CheckBudget::default(),
                StorageCancellationToken::new(),
            )
            .await?;
        if outcome.allowed() {
            expected_objects.insert(object);
        }
    }

    let listed_objects = DirectListObjectsEngine::default()
        .list_objects(
            &command("allowed", ContextualTuples::empty())?,
            Arc::clone(&model),
            Arc::clone(&tuple_reader),
            ListObjectsBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?
        .objects()
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        listed_objects, expected_objects,
        "ListObjects/Check mismatch in generated case {case_index} seed {seed:#x}; flags={flags:?}",
    );

    let user_filter = UserTypeFilter::new("user".parse()?, None);
    for (document_index, &(_, _, wildcard, banned)) in flags.iter().enumerate() {
        let object = format!("document:generated-{document_index}");
        let users = DirectListUsersEngine::default()
            .list_users(
                &users_command(
                    &object,
                    "allowed",
                    vec![user_filter.clone()],
                    ContextualTuples::empty(),
                    ConditionContext::empty(),
                )?,
                Arc::clone(&model),
                Arc::clone(&tuple_reader),
                ListUsersBudget::default(),
                StorageCancellationToken::new(),
            )
            .await?
            .users()
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let covers_alice =
            users.contains("user:alice") || (users.contains("user:*") && !(wildcard && banned));
        assert_eq!(
            covers_alice,
            expected_objects.contains(&object),
            "ListUsers/Check mismatch for {object} in generated case {case_index} seed {seed:#x}; \
             flags={flags:?}; users={users:?}",
        );
    }

    drop(tuple_reader);
    drop(model);
    shutdown(storage).await?;
    Ok(())
}

fn command(
    relation: &str,
    contextual_tuples: ContextualTuples,
) -> Result<ListObjectsCommand, Box<dyn Error>> {
    let query = query_context(contextual_tuples, ConditionContext::empty())?;
    Ok(ListObjectsCommand::new(
        query,
        "document".parse()?,
        relation.parse()?,
        "user:alice".parse()?,
        ListControl::new(
            NonZeroU32::new(100).ok_or("result limit was zero")?,
            None,
            &InputLimits::default(),
        )?,
    ))
}

fn users_command(
    object: &str,
    relation: &str,
    filters: Vec<UserTypeFilter>,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
) -> Result<ListUsersCommand, Box<dyn Error>> {
    users_command_with_limit(
        object,
        relation,
        filters,
        contextual_tuples,
        condition_context,
        100,
    )
}

fn expand_command(
    object: &str,
    relation: &str,
    contextual_tuples: ContextualTuples,
) -> Result<ExpandCommand, Box<dyn Error>> {
    Ok(ExpandCommand::new(
        query_context(contextual_tuples, ConditionContext::empty())?,
        object.parse()?,
        relation.parse()?,
    ))
}

fn users_command_with_limit(
    object: &str,
    relation: &str,
    filters: Vec<UserTypeFilter>,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    maximum_results: u32,
) -> Result<ListUsersCommand, Box<dyn Error>> {
    Ok(ListUsersCommand::new(
        query_context(contextual_tuples, condition_context)?,
        object.parse()?,
        relation.parse()?,
        UserTypeFilters::new(filters, &InputLimits::default())?,
        ListControl::new(
            NonZeroU32::new(maximum_results).ok_or("result limit was zero")?,
            None,
            &InputLimits::default(),
        )?,
    ))
}

fn query_context(
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
) -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(ConsistencyPreference::HigherConsistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(condition_context)
        .deadline(Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_secs(5))?,
        )?)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "phase3-tests".parse()?,
        ))
        .build())
}

async fn memory_storage() -> Result<Arc<MemoryStorage>, Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    storage
        .create_store(
            &operation_context()?,
            STORE_ID.parse()?,
            StoreName::new("list-tests".to_owned())?,
        )
        .await?;
    Ok(storage)
}

async fn write_tuples(
    storage: &MemoryStorage,
    tuples: Vec<RelationshipTuple>,
) -> Result<(), Box<dyn Error>> {
    storage
        .write_tuples(
            &operation_context()?,
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
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_secs(5))?)?,
        StorageCancellationToken::new(),
    ))
}

async fn shutdown(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error>> {
    let mut owner = Arc::try_unwrap(storage).map_err(|_| "memory storage still shared")?;
    owner.stop().await?;
    Ok(())
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse::<TupleKey>()?))
}

fn conditional_tuple(value: &str, x: i64) -> Result<RelationshipTuple, Box<dyn Error>> {
    let context = ConditionContext::new(
        BTreeMap::from([("x".parse()?, ContextValue::Int(x))]),
        &InputLimits::default(),
    )?;
    Ok(RelationshipTuple::new(
        value.parse()?,
        ConditionReference::Conditional(ConditionBinding::new("under_limit".parse()?, context)),
    ))
}

fn model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let user = type_source("user", Vec::new())?;
    let group = type_source(
        "group",
        vec![relation(
            "member",
            RewriteSource::Direct,
            vec![object("user")?, userset("group", "member")?],
        )?],
    )?;
    let folder = type_source(
        "folder",
        vec![
            relation("parent", RewriteSource::Direct, vec![object("folder")?])?,
            relation(
                "viewer",
                RewriteSource::Union(vec![RewriteSource::Direct, ttu("parent", "viewer")?]),
                vec![object("user")?, wildcard("user")?],
            )?,
        ],
    )?;
    let document = type_source(
        "document",
        vec![
            relation(
                "owner",
                RewriteSource::Direct,
                vec![object("user")?, wildcard("user")?],
            )?,
            relation("editor", RewriteSource::Direct, vec![object("user")?])?,
            relation("banned", RewriteSource::Direct, vec![object("user")?])?,
            relation("parent", RewriteSource::Direct, vec![object("folder")?])?,
            relation(
                "viewer",
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    computed("owner")?,
                    ttu("parent", "viewer")?,
                ]),
                vec![
                    object("user")?,
                    wildcard("user")?,
                    userset("group", "member")?,
                ],
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
            relation(
                "conditional",
                RewriteSource::Direct,
                vec![conditional_object("user", "under_limit")?],
            )?,
        ],
    )?;
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![user, group, folder, document],
        vec![ConditionSource::new(
            "under_limit".parse()?,
            ConditionDefinition::new(
                "under_limit".parse()?,
                "x < 50".to_owned(),
                BTreeMap::from([("x".parse()?, ParameterType::int())]),
            ),
        )],
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

fn object(subject_type: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Object,
        None,
    ))
}

fn wildcard(subject_type: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Wildcard,
        None,
    ))
}

fn conditional_object(
    subject_type: &str,
    condition: &str,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Object,
        Some(condition.parse()?),
    ))
}

fn userset(subject_type: &str, relation: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Userset(relation.parse()?),
        None,
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
