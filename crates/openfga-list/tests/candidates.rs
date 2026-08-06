//! Reverse-candidate traversal over the actor-owned storage contract.

use std::{
    collections::BTreeSet,
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
use openfga_domain::{
    AuthorizationModelId, BatchCheckCommand, CheckCommand, ConditionContext, ConsistencyPreference,
    ContextualTuples, Deadline, InputLimits, Limit, ListControl, ListObjectsCommand,
    ModelSelection, Principal, PrincipalKind, QueryContext, RelationshipTuple, RequestTimeout,
    StoreId, TupleKey,
};
use openfga_list::{
    Candidate, CandidateBudget, DirectListObjectsEngine, ListErrorKind, ListObjectsBudget,
    ListObjectsEngine, ReverseCandidateTraversal,
};
use openfga_model::{
    AuthorizationModelSource, CompiledModel, DirectRestrictionSource, ModelCompiler,
    RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    OperationContext, StorageCancellationToken, StoreName, StoreWriter, TupleReader,
    TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
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

#[derive(Debug, Default)]
struct BlockingCheckEvaluator {
    active: AtomicUsize,
    delegate: DirectCheckEvaluator,
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

fn command(
    relation: &str,
    contextual_tuples: ContextualTuples,
) -> Result<ListObjectsCommand, Box<dyn Error>> {
    let query = QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(ConsistencyPreference::HigherConsistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(ConditionContext::empty())
        .deadline(Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_secs(5))?,
        )?)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "phase3-tests".parse()?,
        ))
        .build();
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
            relation("owner", RewriteSource::Direct, vec![object("user")?])?,
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
        ],
    )?;
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![user, group, folder, document],
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
