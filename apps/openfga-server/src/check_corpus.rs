//! Replay of the complete pinned upstream Check fixture corpus against Rust.

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, bail};
use openfga_check::CheckBudget;
use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, CheckCommand, ConditionBinding, ConditionContext, ConditionReference,
    ConsistencyPreference, ContextualTuples, Deadline, InputLimits, Limit, ModelSelection,
    ObjectRef, Principal, PrincipalKind, QueryContext, RelationName, RelationshipTuple,
    RequestTimeout, StoreId, SubjectRef, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, ModelCompiler, ModelLimits,
    RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_service::{CheckService, ServiceErrorKind};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, StorageCancellationToken, StoreName, StoreWriter,
    StoredAuthorizationModel, TupleReader, TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const MAXIMUM_CORPUS_BYTES: u64 = 16 * 1_024 * 1_024;
const MAXIMUM_CORPUS_EVENTS: usize = 10_000;
const MAXIMUM_REPORTED_MISMATCHES: usize = 100;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorpusEvent {
    kind: EventKind,
    #[serde(default)]
    store_id: String,
    #[serde(default)]
    model_id: String,
    #[serde(default)]
    request: Option<Value>,
    #[serde(default)]
    allowed: Option<bool>,
    #[serde(default)]
    error_code: i32,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EventKind {
    CreateStore,
    WriteModel,
    WriteTuples,
    Check,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutcomeClass {
    Allowed,
    Denied,
    Validation,
    ResourceExhausted,
    Timeout,
    Storage,
    Internal,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CorpusMismatch {
    event_index: usize,
    go: OutcomeClass,
    rust: OutcomeClass,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CorpusReport {
    baseline_commit: &'static str,
    source: &'static str,
    events: usize,
    checks: usize,
    mismatches: Vec<CorpusMismatch>,
    mismatches_truncated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWriteModel {
    store_id: String,
    type_definitions: Vec<WireTypeDefinition>,
    schema_version: String,
    #[serde(default)]
    conditions: BTreeMap<String, WireConditionDefinition>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTypeDefinition {
    #[serde(rename = "type")]
    object_type: String,
    #[serde(default)]
    relations: BTreeMap<String, WireUserset>,
    #[serde(default)]
    metadata: WireTypeMetadata,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTypeMetadata {
    #[serde(default)]
    relations: BTreeMap<String, WireRelationMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRelationMetadata {
    #[serde(default)]
    directly_related_user_types: Vec<WireRelationReference>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRelationReference {
    #[serde(rename = "type")]
    subject_type: String,
    #[serde(default)]
    relation: String,
    #[serde(default)]
    wildcard: Option<Value>,
    #[serde(default)]
    condition: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireUserset {
    #[serde(default)]
    this: Option<Value>,
    #[serde(default)]
    computed_userset: Option<WireObjectRelation>,
    #[serde(default)]
    tuple_to_userset: Option<WireTupleToUserset>,
    #[serde(default)]
    union: Option<WireUsersets>,
    #[serde(default)]
    intersection: Option<WireUsersets>,
    #[serde(default)]
    difference: Option<WireDifference>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireObjectRelation {
    relation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTupleToUserset {
    tupleset: WireObjectRelation,
    computed_userset: WireObjectRelation,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireUsersets {
    child: Vec<WireUserset>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDifference {
    base: Box<WireUserset>,
    subtract: Box<WireUserset>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireConditionDefinition {
    name: String,
    expression: String,
    #[serde(default)]
    parameters: BTreeMap<String, WireParameterType>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireParameterType {
    type_name: String,
    #[serde(default)]
    generic_types: Vec<WireParameterType>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWriteRequest {
    store_id: String,
    #[serde(default)]
    writes: Option<WireTupleCollection>,
    #[serde(default)]
    deletes: Option<WireTupleCollection>,
    #[serde(default)]
    authorization_model_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTupleCollection {
    #[serde(default)]
    tuple_keys: Vec<WireRelationshipTuple>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRelationshipTuple {
    user: String,
    relation: String,
    object: String,
    #[serde(default)]
    condition: Option<WireConditionBinding>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireConditionBinding {
    name: String,
    #[serde(default)]
    context: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCheckRequest {
    store_id: String,
    #[serde(default)]
    tuple_key: Option<WireCheckTuple>,
    #[serde(default)]
    contextual_tuples: Option<WireTupleCollection>,
    #[serde(default)]
    authorization_model_id: String,
    #[serde(default)]
    consistency: String,
    #[serde(default)]
    trace: bool,
    #[serde(default)]
    context: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCheckTuple {
    user: String,
    relation: String,
    object: String,
}

/// Replays every recorded vendored Check fixture against the Rust service.
pub(crate) async fn run(path: PathBuf) -> Result<()> {
    let events = read_corpus(path).await?;
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let models: Arc<dyn ModelReader> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage.clone();
    let service = CheckService::direct(models, tuples, CheckBudget::default());
    let replay = replay(&events, storage.as_ref(), &service).await;
    drop(service);
    let mut owner = Arc::try_unwrap(storage)
        .map_err(|_| anyhow::anyhow!("corpus replay retained a storage capability"))?;
    owner.stop().await?;
    let (report, mismatch_count) = replay?;
    write_report(&report)?;
    if mismatch_count != 0 {
        bail!("vendored Check corpus differential found {mismatch_count} mismatches");
    }
    Ok(())
}

async fn replay(
    events: &[CorpusEvent],
    storage: &MemoryStorage,
    service: &CheckService,
) -> Result<(CorpusReport, usize)> {
    let mut mismatches = Vec::new();
    let mut checks = 0_usize;
    let mut mismatch_count = 0_usize;
    for (event_index, event) in events.iter().enumerate() {
        match event.kind {
            EventKind::CreateStore => create_store(storage, event)
                .await
                .with_context(|| format!("create-store corpus event {event_index} failed"))?,
            EventKind::WriteModel => write_model(storage, event)
                .await
                .with_context(|| format!("write-model corpus event {event_index} failed"))?,
            EventKind::WriteTuples => write_tuples(storage, event)
                .await
                .with_context(|| format!("write-tuples corpus event {event_index} failed"))?,
            EventKind::Check => {
                checks = checks
                    .checked_add(1)
                    .context("Check corpus count overflowed")?;
                let go = expected_outcome(event)?;
                let rust = observe_rust(service, event).await;
                if go != rust {
                    mismatch_count = mismatch_count
                        .checked_add(1)
                        .context("Check mismatch count overflowed")?;
                    if mismatches.len() < MAXIMUM_REPORTED_MISMATCHES {
                        mismatches.push(CorpusMismatch {
                            event_index,
                            go,
                            rust,
                        });
                    }
                }
            }
        }
    }
    let report = CorpusReport {
        baseline_commit: super::check_probe::GO_BASELINE_COMMIT,
        source: "vendors/openfga/tests/check and assets/tests",
        events: events.len(),
        checks,
        mismatches,
        mismatches_truncated: mismatch_count > MAXIMUM_REPORTED_MISMATCHES,
    };
    Ok((report, mismatch_count))
}

fn write_report(report: &CorpusReport) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &report)?;
    writeln!(output)?;
    Ok(())
}

async fn read_corpus(path: PathBuf) -> Result<Vec<CorpusEvent>> {
    let display = display_path(&path);
    #[allow(
        clippy::disallowed_types,
        reason = "bounded standard-file I/O is isolated inside spawn_blocking"
    )]
    let data = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)
            .with_context(|| format!("failed to open Check corpus at {display}"))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("failed to inspect Check corpus at {display}"))?;
        if !metadata.is_file() || metadata.len() > MAXIMUM_CORPUS_BYTES {
            bail!(
                "Check corpus must be a regular file no larger than {MAXIMUM_CORPUS_BYTES} bytes"
            );
        }
        let mut data = Vec::with_capacity(metadata.len().try_into()?);
        file.take(MAXIMUM_CORPUS_BYTES.saturating_add(1))
            .read_to_end(&mut data)
            .context("failed to read Check corpus")?;
        if u64::try_from(data.len())? > MAXIMUM_CORPUS_BYTES {
            bail!("Check corpus exceeds the byte-size limit");
        }
        Ok(data)
    })
    .await
    .context("Check corpus read task failed")??;
    let events: Vec<CorpusEvent> =
        serde_json::from_slice(&data).context("invalid Check corpus JSON")?;
    if events.len() > MAXIMUM_CORPUS_EVENTS {
        bail!("Check corpus exceeds the event-count limit");
    }
    Ok(events)
}

fn display_path(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("[REDACTED]")
        .to_owned()
}

async fn create_store(storage: &MemoryStorage, event: &CorpusEvent) -> Result<()> {
    let store_id = event.store_id.parse::<StoreId>()?;
    storage
        .create_store(
            &operation_context()?,
            store_id,
            StoreName::new("check-corpus".to_owned())?,
        )
        .await?;
    Ok(())
}

async fn write_model(storage: &MemoryStorage, event: &CorpusEvent) -> Result<()> {
    let request: WireWriteModel = request(event)?;
    if request.store_id != event.store_id {
        bail!("model event store identity mismatch");
    }
    let source = Arc::new(convert_model(request, &event.model_id)?);
    let model_compiler = ModelCompiler::new(
        ModelLimits::builder()
            .input(
                InputLimits::builder()
                    .relations(Limit::<2_000>::new(200)?)
                    .build(),
            )
            .build(),
    );
    let compiled_model = model_compiler.compile(&source).map_err(|errors| {
        anyhow::anyhow!("authorization model diagnostics: {:?}", errors.errors())
    })?;
    storage
        .write_model(
            &operation_context()?,
            Arc::new(StoredAuthorizationModel::new(
                source,
                compiled_model,
                SystemTime::now(),
            )?),
        )
        .await?;
    Ok(())
}

async fn write_tuples(storage: &MemoryStorage, event: &CorpusEvent) -> Result<()> {
    let request: WireWriteRequest = request(event)?;
    if request.store_id != event.store_id {
        bail!("tuple event store identity mismatch");
    }
    if !request.authorization_model_id.is_empty() {
        let _model_id = request
            .authorization_model_id
            .parse::<AuthorizationModelId>()?;
    }
    let limits = InputLimits::default();
    let writes = convert_tuple_collection(request.writes, &limits)?;
    let deletes = convert_tuple_collection(request.deletes, &limits)?
        .into_iter()
        .map(|tuple| tuple.key().clone())
        .collect();
    storage
        .write_tuples(
            &operation_context()?,
            event.store_id.parse()?,
            deletes,
            writes,
            TupleWriteOptions::default(),
        )
        .await?;
    Ok(())
}

async fn observe_rust(service: &CheckService, event: &CorpusEvent) -> OutcomeClass {
    let command = convert_check_event(event);
    let Ok(command) = command else {
        return OutcomeClass::Validation;
    };
    match service
        .check(&command, StorageCancellationToken::new())
        .await
    {
        Ok(outcome) if outcome.allowed() => OutcomeClass::Allowed,
        Ok(_) => OutcomeClass::Denied,
        Err(error) => match error.kind() {
            ServiceErrorKind::InvalidRequest | ServiceErrorKind::Condition => {
                OutcomeClass::Validation
            }
            ServiceErrorKind::ResourceExhausted => OutcomeClass::ResourceExhausted,
            ServiceErrorKind::Timeout | ServiceErrorKind::Cancelled => OutcomeClass::Timeout,
            ServiceErrorKind::Storage => OutcomeClass::Storage,
            _ => OutcomeClass::Internal,
        },
    }
}

fn expected_outcome(event: &CorpusEvent) -> Result<OutcomeClass> {
    match (event.error_code, event.allowed) {
        (0, Some(true)) => Ok(OutcomeClass::Allowed),
        (0, Some(false)) => Ok(OutcomeClass::Denied),
        (2000 | 2027, None) => Ok(OutcomeClass::Validation),
        (2002, None) => Ok(OutcomeClass::ResourceExhausted),
        _ => bail!("Check corpus contains an unsupported Go outcome category"),
    }
}

fn convert_check_event(event: &CorpusEvent) -> Result<CheckCommand> {
    let request: WireCheckRequest = request(event)?;
    if request.store_id != event.store_id
        || request.authorization_model_id != event.model_id
        || request.trace && request.tuple_key.is_none()
    {
        bail!("Check event identity or tuple mismatch");
    }
    let limits = InputLimits::default();
    let tuple = request
        .tuple_key
        .as_ref()
        .context("Check tuple is missing")?;
    let tuple = convert_check_tuple(tuple, &limits)?;
    let contextual_tuples = ContextualTuples::new(
        convert_tuple_collection(request.contextual_tuples, &limits)?,
        &limits,
    )?;
    let condition_context = ConditionContext::try_from_json(
        request.context.unwrap_or_else(|| Value::Object(Map::new())),
        &limits,
    )?;
    let consistency = match request.consistency.as_str() {
        "" | "CONSISTENCY_PREFERENCE_UNSPECIFIED" | "MINIMIZE_LATENCY" => {
            ConsistencyPreference::MinimizeLatency
        }
        "HIGHER_CONSISTENCY" => ConsistencyPreference::HigherConsistency,
        _ => bail!("Check corpus contains an unknown consistency preference"),
    };
    let query = QueryContext::builder()
        .store_id(event.store_id.parse()?)
        .model_selection(ModelSelection::Explicit(event.model_id.parse()?))
        .consistency(consistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(condition_context)
        .deadline(Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(REQUEST_TIMEOUT)?,
        )?)
        .principal(Principal::new(
            PrincipalKind::Development,
            "vendored-check-corpus".parse()?,
        ))
        .build();
    Ok(CheckCommand::new(query, tuple))
}

fn convert_model(request: WireWriteModel, model_id: &str) -> Result<AuthorizationModelSource> {
    let mut types = Vec::with_capacity(request.type_definitions.len());
    for type_definition in request.type_definitions {
        let mut relations = Vec::with_capacity(type_definition.relations.len());
        for (name, rewrite) in type_definition.relations {
            let restrictions = type_definition
                .metadata
                .relations
                .get(&name)
                .map_or(&[][..], |metadata| {
                    metadata.directly_related_user_types.as_slice()
                })
                .iter()
                .map(convert_restriction)
                .collect::<Result<Vec<_>>>()?;
            relations.push(RelationSource::new(
                name.parse()?,
                convert_rewrite(rewrite)?,
                restrictions,
            ));
        }
        types.push(TypeDefinitionSource::new(
            type_definition.object_type.parse()?,
            relations,
        ));
    }
    let conditions = request
        .conditions
        .into_iter()
        .map(|(key, definition)| {
            let parameters = definition
                .parameters
                .into_iter()
                .map(|(name, parameter_type)| {
                    Ok((name.parse()?, convert_parameter_type(parameter_type)?))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok(ConditionSource::new(
                key.parse()?,
                ConditionDefinition::new(
                    definition.name.parse()?,
                    definition.expression,
                    parameters,
                ),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(AuthorizationModelSource::new(
        request.store_id.parse()?,
        model_id.parse()?,
        request.schema_version,
        types,
        conditions,
    ))
}

fn convert_restriction(reference: &WireRelationReference) -> Result<DirectRestrictionSource> {
    let kind = match (reference.wildcard.is_some(), reference.relation.is_empty()) {
        (true, true) => RestrictionKindSource::Wildcard,
        (false, false) => RestrictionKindSource::Userset(reference.relation.parse()?),
        (false, true) => RestrictionKindSource::Object,
        (true, false) => bail!("relation restriction combines wildcard and userset"),
    };
    Ok(DirectRestrictionSource::new(
        reference.subject_type.parse()?,
        kind,
        (!reference.condition.is_empty())
            .then(|| reference.condition.parse())
            .transpose()?,
    ))
}

fn convert_rewrite(rewrite: WireUserset) -> Result<RewriteSource> {
    let WireUserset {
        this,
        computed_userset,
        tuple_to_userset,
        union,
        intersection,
        difference,
    } = rewrite;
    let variants = usize::from(this.is_some())
        + usize::from(computed_userset.is_some())
        + usize::from(tuple_to_userset.is_some())
        + usize::from(union.is_some())
        + usize::from(intersection.is_some())
        + usize::from(difference.is_some());
    if variants != 1 {
        bail!("rewrite must contain exactly one userset variant");
    }
    if this.is_some() {
        return Ok(RewriteSource::Direct);
    }
    if let Some(computed) = computed_userset {
        return Ok(RewriteSource::Computed(computed.relation.parse()?));
    }
    if let Some(ttu) = tuple_to_userset {
        return Ok(RewriteSource::TupleToUserset {
            tupleset: ttu.tupleset.relation.parse()?,
            computed: ttu.computed_userset.relation.parse()?,
        });
    }
    if let Some(union) = union {
        return Ok(RewriteSource::Union(convert_children(union.child)?));
    }
    if let Some(intersection) = intersection {
        return Ok(RewriteSource::Intersection(convert_children(
            intersection.child,
        )?));
    }
    let difference = difference.context("difference rewrite is missing")?;
    Ok(RewriteSource::Difference {
        base: Box::new(convert_rewrite(*difference.base)?),
        subtract: Box::new(convert_rewrite(*difference.subtract)?),
    })
}

fn convert_children(children: Vec<WireUserset>) -> Result<Vec<RewriteSource>> {
    children.into_iter().map(convert_rewrite).collect()
}

fn convert_parameter_type(parameter_type: WireParameterType) -> Result<ParameterType> {
    let mut generics = parameter_type.generic_types.into_iter();
    let converted = match parameter_type.type_name.as_str() {
        "TYPE_NAME_ANY" => ParameterType::any(),
        "TYPE_NAME_BOOL" => ParameterType::bool(),
        "TYPE_NAME_STRING" => ParameterType::string(),
        "TYPE_NAME_INT" => ParameterType::int(),
        "TYPE_NAME_UINT" => ParameterType::uint(),
        "TYPE_NAME_DOUBLE" => ParameterType::double(),
        "TYPE_NAME_BYTES" => ParameterType::bytes(),
        "TYPE_NAME_DURATION" => ParameterType::duration(),
        "TYPE_NAME_TIMESTAMP" => ParameterType::timestamp(),
        "TYPE_NAME_IPADDRESS" => ParameterType::ip_address(),
        "TYPE_NAME_LIST" => ParameterType::list(convert_parameter_type(
            generics
                .next()
                .context("list parameter type is missing its element")?,
        )?)?,
        "TYPE_NAME_MAP" => ParameterType::map(convert_parameter_type(
            generics
                .next()
                .context("map parameter type is missing its value")?,
        )?)?,
        _ => bail!("condition parameter has an unsupported type"),
    };
    if generics.next().is_some() {
        bail!("condition parameter has unexpected generic arguments");
    }
    Ok(converted)
}

fn convert_tuple_collection(
    collection: Option<WireTupleCollection>,
    limits: &InputLimits,
) -> Result<Vec<RelationshipTuple>> {
    collection
        .map_or_else(Vec::new, |collection| collection.tuple_keys)
        .into_iter()
        .map(|tuple| convert_relationship_tuple(tuple, limits))
        .collect()
}

fn convert_relationship_tuple(
    tuple: WireRelationshipTuple,
    limits: &InputLimits,
) -> Result<RelationshipTuple> {
    let key = convert_tuple_parts(&tuple.object, &tuple.relation, &tuple.user, limits)?;
    let condition = match tuple.condition {
        Some(condition) => ConditionReference::Conditional(ConditionBinding::new(
            condition.name.parse()?,
            ConditionContext::try_from_json(
                condition
                    .context
                    .unwrap_or_else(|| Value::Object(Map::new())),
                limits,
            )?,
        )),
        None => ConditionReference::Unconditional,
    };
    Ok(RelationshipTuple::new(key, condition))
}

fn convert_check_tuple(tuple: &WireCheckTuple, limits: &InputLimits) -> Result<TupleKey> {
    convert_tuple_parts(&tuple.object, &tuple.relation, &tuple.user, limits)
}

fn convert_tuple_parts(
    object: &str,
    relation: &str,
    user: &str,
    limits: &InputLimits,
) -> Result<TupleKey> {
    Ok(TupleKey::new(
        ObjectRef::parse_with_limits(object, limits)?,
        RelationName::parse_with_limits(relation, limits)?,
        SubjectRef::parse_with_limits(user, limits)?,
    ))
}

fn operation_context() -> Result<OperationContext> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(REQUEST_TIMEOUT)?)?,
        StorageCancellationToken::new(),
    ))
}

fn request<T>(event: &CorpusEvent) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(
        event
            .request
            .clone()
            .context("corpus event request is missing")?,
    )
    .context("corpus event request has an invalid shape")
}
