//! AWS SDK construction and operation admission.

#[cfg(test)]
use std::{collections::VecDeque, sync::Mutex};
use std::{fmt, future::Future, sync::Arc, time::Duration};

use aws_config::{BehaviorVersion, retry::RetryConfig, timeout::TimeoutConfig};
use aws_sdk_dynamodb::{
    Client,
    config::Region,
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        batch_get_item::BatchGetItemOutput,
        create_table::CreateTableOutput,
        delete_item::DeleteItemOutput,
        describe_table::DescribeTableOutput,
        get_item::GetItemOutput,
        put_item::PutItemOutput,
        query::QueryOutput,
        transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput},
        update_continuous_backups::UpdateContinuousBackupsOutput,
        update_item::UpdateItemOutput,
    },
    types::{CancellationReason, ReturnConsumedCapacity, TransactWriteItem},
};
use aws_smithy_http_client::{
    Builder as HttpClientBuilder,
    tls::{self, rustls_provider::CryptoMode},
};
use openfga_storage::{OperationContext, StorageError, StorageErrorKind};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep_until},
};

use crate::DynamoDbStorageConfig;

pub(crate) trait DynamoOutputMetrics {
    fn consumed_capacity_units(&self) -> f64;
}

macro_rules! single_capacity_output {
    ($($output:ty),+ $(,)?) => {
        $(
            impl DynamoOutputMetrics for $output {
                fn consumed_capacity_units(&self) -> f64 {
                    self.consumed_capacity()
                        .and_then(|capacity| capacity.capacity_units())
                        .unwrap_or_default()
                }
            }
        )+
    };
}

single_capacity_output!(
    DeleteItemOutput,
    GetItemOutput,
    PutItemOutput,
    QueryOutput,
    UpdateItemOutput,
);

macro_rules! multiple_capacity_output {
    ($($output:ty),+ $(,)?) => {
        $(
            impl DynamoOutputMetrics for $output {
                fn consumed_capacity_units(&self) -> f64 {
                    self.consumed_capacity()
                        .iter()
                        .filter_map(|capacity| capacity.capacity_units())
                        .sum()
                }
            }
        )+
    };
}

multiple_capacity_output!(BatchGetItemOutput, TransactWriteItemsOutput);

macro_rules! no_capacity_output {
    ($($output:ty),+ $(,)?) => {
        $(
            impl DynamoOutputMetrics for $output {
                fn consumed_capacity_units(&self) -> f64 {
                    0.0
                }
            }
        )+
    };
}

no_capacity_output!(
    (),
    CreateTableOutput,
    DescribeTableOutput,
    UpdateContinuousBackupsOutput,
);

#[derive(Clone)]
pub(crate) struct DynamoClient {
    client: Client,
    table_name: String,
    permits: Arc<Semaphore>,
    background_timeout: Duration,
    maximum_attempts: u32,
    metrics: DynamoMetrics,
    #[cfg(test)]
    faults: FaultInjector,
}

#[derive(Clone)]
struct DynamoMetrics {
    admission_wait: Histogram<f64>,
    operation_duration: Histogram<f64>,
    operations: Counter<u64>,
    failures: Counter<u64>,
    evaluated_items: Counter<u64>,
    emitted_items: Counter<u64>,
    shard_fan_out: Counter<u64>,
    encoded_bytes: Counter<u64>,
    blob_chunks: Counter<u64>,
    unprocessed_keys: Counter<u64>,
    garbage_collection_work: Counter<u64>,
    garbage_collection_failures: Counter<u64>,
    garbage_collection_lag: Histogram<f64>,
    head_retries: Counter<u64>,
    readiness: Counter<u64>,
    throttles: Counter<u64>,
    transaction_cancellations: Counter<u64>,
    consumed_capacity: Counter<f64>,
}

impl DynamoMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("openfga-storage-dynamodb");
        Self {
            admission_wait: meter
                .f64_histogram("openfga.storage.work.wait.duration")
                .with_description("Time waiting for bounded DynamoDB request admission")
                .with_unit("s")
                .build(),
            operation_duration: meter
                .f64_histogram("openfga.storage.dynamodb.operation.duration")
                .with_description("DynamoDB SDK operation duration")
                .with_unit("s")
                .build(),
            operations: meter
                .u64_counter("openfga.storage.dynamodb.operations")
                .with_description("DynamoDB SDK operations by bounded result class")
                .build(),
            failures: meter
                .u64_counter("openfga.storage.dynamodb.failures")
                .with_description("DynamoDB SDK failures by bounded error class")
                .build(),
            evaluated_items: meter
                .u64_counter("openfga.storage.dynamodb.evaluated.items")
                .with_description("Items evaluated by bounded DynamoDB planners")
                .build(),
            emitted_items: meter
                .u64_counter("openfga.storage.dynamodb.emitted.items")
                .with_description("Logical items emitted by DynamoDB planners")
                .build(),
            shard_fan_out: meter
                .u64_counter("openfga.storage.dynamodb.shard.fanout")
                .with_description("Physical shards queried by logical operations")
                .build(),
            encoded_bytes: meter
                .u64_counter("openfga.storage.dynamodb.encoded.bytes")
                .with_description("Encoded bytes written or read by blob operations")
                .with_unit("By")
                .build(),
            blob_chunks: meter
                .u64_counter("openfga.storage.dynamodb.blob.chunks")
                .with_description("Blob chunks written or read")
                .build(),
            unprocessed_keys: meter
                .u64_counter("openfga.storage.dynamodb.unprocessed.keys")
                .with_description("BatchGet keys returned as unprocessed")
                .build(),
            garbage_collection_work: meter
                .u64_counter("openfga.storage.dynamodb.gc.work")
                .with_description("Durable garbage-collection work items processed")
                .build(),
            garbage_collection_failures: meter
                .u64_counter("openfga.storage.dynamodb.gc.failures")
                .with_description("Failed durable garbage-collection passes")
                .build(),
            garbage_collection_lag: meter
                .f64_histogram("openfga.storage.dynamodb.gc.lag")
                .with_description("Age of overdue durable garbage-collection work")
                .with_unit("s")
                .build(),
            head_retries: meter
                .u64_counter("openfga.storage.dynamodb.head.retries")
                .with_description("Optimistic changelog or assertion HEAD retries")
                .build(),
            readiness: meter
                .u64_counter("openfga.storage.dynamodb.readiness")
                .with_description("DynamoDB readiness checks by bounded state")
                .build(),
            throttles: meter
                .u64_counter("openfga.storage.dynamodb.throttles")
                .with_description("DynamoDB throttling and capacity failures")
                .build(),
            transaction_cancellations: meter
                .u64_counter("openfga.storage.dynamodb.transaction.cancellations")
                .with_description("DynamoDB transaction cancellations by bounded class")
                .build(),
            consumed_capacity: meter
                .f64_counter("openfga.storage.dynamodb.consumed.capacity")
                .with_description("DynamoDB capacity units returned by the service")
                .with_unit("{unit}")
                .build(),
        }
    }

    fn record_result<T>(&self, operation: &'static str, result: &Result<T, StorageError>) {
        let result_class = result
            .as_ref()
            .map_or_else(|error| error_kind_label(error.kind()), |_| "ok");
        let attributes = [
            KeyValue::new("backend", "dynamodb"),
            KeyValue::new("operation", operation),
            KeyValue::new("result", result_class),
        ];
        self.operations.add(1, &attributes);
        if result.is_err() {
            self.failures.add(1, &attributes);
        }
    }

    fn record_sdk_error(&self, code: Option<&str>) {
        if matches!(
            code,
            Some(
                "ThrottlingException"
                    | "ProvisionedThroughputExceededException"
                    | "RequestLimitExceeded"
            )
        ) {
            self.throttles
                .add(1, &[KeyValue::new("backend", "dynamodb")]);
        }
    }

    fn record_transaction_cancellation(&self, kind: StorageErrorKind) {
        self.transaction_cancellations.add(
            1,
            &[
                KeyValue::new("backend", "dynamodb"),
                KeyValue::new("result", error_kind_label(kind)),
            ],
        );
        if kind == StorageErrorKind::Unavailable {
            self.throttles
                .add(1, &[KeyValue::new("backend", "dynamodb")]);
        }
    }

    fn record_consumed_capacity<T: DynamoOutputMetrics>(
        &self,
        operation: &'static str,
        output: &T,
    ) {
        let units = output.consumed_capacity_units();
        if units > 0.0 {
            self.consumed_capacity.add(
                units,
                &[
                    KeyValue::new("backend", "dynamodb"),
                    KeyValue::new("operation", operation),
                ],
            );
        }
    }
}

impl DynamoClient {
    pub(crate) async fn create(config: &DynamoDbStorageConfig) -> Result<Self, StorageError> {
        config.validate().map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "dynamodb_config_invalid",
                error,
            )
        })?;
        let http_client = HttpClientBuilder::new()
            .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
            .build_https();
        let timeout = TimeoutConfig::builder()
            .operation_attempt_timeout(config.attempt_timeout)
            .operation_timeout(config.operation_timeout)
            .build();
        let retry = RetryConfig::standard().with_max_attempts(config.maximum_attempts.get());
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.as_str().to_owned()))
            .http_client(http_client)
            .timeout_config(timeout)
            .retry_config(retry)
            .load()
            .await;
        let mut service = aws_sdk_dynamodb::config::Builder::from(&shared);
        if let Some(endpoint) = &config.endpoint {
            service = service.endpoint_url(endpoint.as_str());
        }
        Ok(Self {
            client: Client::from_conf(service.build()),
            table_name: config.table_name.as_str().to_owned(),
            permits: Arc::new(Semaphore::new(config.maximum_in_flight.get() as usize)),
            background_timeout: config.operation_timeout,
            maximum_attempts: config.maximum_attempts.get(),
            metrics: DynamoMetrics::new(),
            #[cfg(test)]
            faults: FaultInjector::default(),
        })
    }

    pub(crate) const fn sdk(&self) -> &Client {
        &self.client
    }

    pub(crate) fn table(&self) -> &str {
        &self.table_name
    }

    pub(crate) async fn execute<T, E, F>(
        &self,
        context: &OperationContext,
        code: &'static str,
        future: F,
    ) -> Result<T, StorageError>
    where
        T: DynamoOutputMetrics,
        E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
    {
        self.execute_mapped(context, code, future, |error| {
            self.metrics.record_sdk_error(error.code());
            map_sdk(code, error)
        })
        .await
    }

    pub(crate) async fn execute_transaction<F>(
        &self,
        context: &OperationContext,
        code: &'static str,
        future: F,
    ) -> Result<TransactWriteItemsOutput, StorageError>
    where
        F: Future<Output = Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError>>>,
    {
        self.execute_mapped(context, code, future, |error| {
            if let Some(kind) = transaction_cancellation_kind(&error) {
                self.metrics.record_transaction_cancellation(kind);
            }
            map_transaction_sdk(code, error)
        })
        .await
    }

    async fn execute_mapped<T, E, F, M>(
        &self,
        context: &OperationContext,
        code: &'static str,
        future: F,
        mapper: M,
    ) -> Result<T, StorageError>
    where
        T: DynamoOutputMetrics,
        E: std::error::Error + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
        M: FnOnce(E) -> StorageError,
    {
        context.check()?;
        #[cfg(test)]
        if let Some(error) = self.faults.take(code, FaultTiming::BeforeDispatch)? {
            return Err(error);
        }
        let deadline = Instant::from_std(context.deadline().instant());
        let wait_started = Instant::now();
        let permit = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(cancelled()),
            () = sleep_until(deadline) => return Err(timed_out()),
            result = Arc::clone(&self.permits).acquire_owned() => result.map_err(|error| {
                StorageError::with_source(StorageErrorKind::Unavailable, "dynamodb_admission_closed", error)
            })?,
        };
        self.metrics.admission_wait.record(
            wait_started.elapsed().as_secs_f64(),
            &[KeyValue::new("backend", "dynamodb")],
        );
        let operation_started = Instant::now();
        let result = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(cancelled()),
            () = sleep_until(deadline) => Err(timed_out()),
            result = future => result.map_err(mapper),
        };
        #[cfg(test)]
        let result = if result.is_ok()
            && let Some(error) = self.faults.take(code, FaultTiming::AfterDispatch)?
        {
            Err(error)
        } else {
            result
        };
        self.metrics.operation_duration.record(
            operation_started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("backend", "dynamodb"),
                KeyValue::new("operation", code),
            ],
        );
        self.metrics.record_result(code, &result);
        if let Ok(output) = &result {
            self.metrics.record_consumed_capacity(code, output);
        }
        drop(permit);
        result
    }

    pub(crate) async fn execute_background<T, E, F>(
        &self,
        code: &'static str,
        future: F,
    ) -> Result<T, StorageError>
    where
        T: DynamoOutputMetrics,
        E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
    {
        self.execute_background_mapped(code, future, |error| {
            self.metrics.record_sdk_error(error.code());
            map_sdk(code, error)
        })
        .await
    }

    pub(crate) async fn execute_transaction_background<F>(
        &self,
        code: &'static str,
        future: F,
    ) -> Result<TransactWriteItemsOutput, StorageError>
    where
        F: Future<Output = Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError>>>,
    {
        self.execute_background_mapped(code, future, |error| {
            if let Some(kind) = transaction_cancellation_kind(&error) {
                self.metrics.record_transaction_cancellation(kind);
            }
            map_transaction_sdk(code, error)
        })
        .await
    }

    pub(crate) async fn transact_write_background(
        &self,
        code: &'static str,
        actions: Vec<TransactWriteItem>,
        token: String,
    ) -> Result<TransactWriteItemsOutput, StorageError> {
        for attempt in 0..self.maximum_attempts {
            match self
                .execute_transaction_background(
                    code,
                    self.client
                        .transact_write_items()
                        .return_consumed_capacity(ReturnConsumedCapacity::Total)
                        .set_transact_items(Some(actions.clone()))
                        .client_request_token(token.clone())
                        .send(),
                )
                .await
            {
                Err(error)
                    if error.kind() == StorageErrorKind::Timeout
                        && attempt.saturating_add(1) < self.maximum_attempts =>
                {
                    tokio::time::sleep(Duration::from_millis(u64::from(
                        1_u32.checked_shl(attempt.min(6)).unwrap_or(64),
                    )))
                    .await;
                }
                result => return result,
            }
        }
        Err(StorageError::new(
            StorageErrorKind::Timeout,
            "dynamodb_background_idempotent_retry_exhausted",
        ))
    }

    async fn execute_background_mapped<T, E, F, M>(
        &self,
        code: &'static str,
        future: F,
        mapper: M,
    ) -> Result<T, StorageError>
    where
        T: DynamoOutputMetrics,
        E: std::error::Error + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
        M: FnOnce(E) -> StorageError,
    {
        #[cfg(test)]
        if let Some(error) = self.faults.take(code, FaultTiming::BeforeDispatch)? {
            return Err(error);
        }
        let wait_started = Instant::now();
        let permit = tokio::time::timeout(
            self.background_timeout,
            Arc::clone(&self.permits).acquire_owned(),
        )
        .await
        .map_err(|_| timed_out())?
        .map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Unavailable,
                "dynamodb_admission_closed",
                error,
            )
        })?;
        self.metrics.admission_wait.record(
            wait_started.elapsed().as_secs_f64(),
            &[KeyValue::new("backend", "dynamodb")],
        );
        let operation_started = Instant::now();
        let result = tokio::time::timeout(self.background_timeout, future)
            .await
            .map_err(|_| timed_out())?
            .map_err(mapper);
        #[cfg(test)]
        let result = if result.is_ok()
            && let Some(error) = self.faults.take(code, FaultTiming::AfterDispatch)?
        {
            Err(error)
        } else {
            result
        };
        self.metrics.operation_duration.record(
            operation_started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("backend", "dynamodb"),
                KeyValue::new("operation", code),
            ],
        );
        self.metrics.record_result(code, &result);
        if let Ok(output) = &result {
            self.metrics.record_consumed_capacity(code, output);
        }
        drop(permit);
        result
    }

    pub(crate) fn record_query_work(
        &self,
        operation: &'static str,
        evaluated: usize,
        emitted: usize,
        shards: usize,
    ) {
        let attributes = [
            KeyValue::new("backend", "dynamodb"),
            KeyValue::new("operation", operation),
        ];
        self.metrics
            .evaluated_items
            .add(u64::try_from(evaluated).unwrap_or(u64::MAX), &attributes);
        self.metrics
            .emitted_items
            .add(u64::try_from(emitted).unwrap_or(u64::MAX), &attributes);
        self.metrics
            .shard_fan_out
            .add(u64::try_from(shards).unwrap_or(u64::MAX), &attributes);
    }

    pub(crate) fn record_blob_work(&self, operation: &'static str, bytes: usize, chunks: usize) {
        let attributes = [
            KeyValue::new("backend", "dynamodb"),
            KeyValue::new("operation", operation),
        ];
        self.metrics
            .encoded_bytes
            .add(u64::try_from(bytes).unwrap_or(u64::MAX), &attributes);
        self.metrics
            .blob_chunks
            .add(u64::try_from(chunks).unwrap_or(u64::MAX), &attributes);
    }

    pub(crate) fn record_unprocessed_keys(&self, count: usize) {
        self.metrics.unprocessed_keys.add(
            u64::try_from(count).unwrap_or(u64::MAX),
            &[KeyValue::new("backend", "dynamodb")],
        );
    }

    pub(crate) fn record_garbage_collection_work(&self) {
        self.metrics
            .garbage_collection_work
            .add(1, &[KeyValue::new("backend", "dynamodb")]);
    }

    pub(crate) fn record_garbage_collection_failure(&self) {
        self.metrics
            .garbage_collection_failures
            .add(1, &[KeyValue::new("backend", "dynamodb")]);
    }

    pub(crate) fn record_garbage_collection_lag(&self, lag_millis: u64) {
        self.metrics.garbage_collection_lag.record(
            Duration::from_millis(lag_millis).as_secs_f64(),
            &[KeyValue::new("backend", "dynamodb")],
        );
    }

    pub(crate) fn record_head_retry(&self, operation: &'static str) {
        self.metrics.head_retries.add(
            1,
            &[
                KeyValue::new("backend", "dynamodb"),
                KeyValue::new("operation", operation),
            ],
        );
    }

    pub(crate) fn record_readiness(&self, state: &'static str) {
        self.metrics.readiness.add(
            1,
            &[
                KeyValue::new("backend", "dynamodb"),
                KeyValue::new("state", state),
            ],
        );
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultTiming {
    BeforeDispatch,
    AfterDispatch,
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct InjectedFault {
    operation: &'static str,
    timing: FaultTiming,
    kind: StorageErrorKind,
    code: &'static str,
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
struct FaultInjector(Arc<Mutex<VecDeque<InjectedFault>>>);

#[cfg(test)]
impl FaultInjector {
    fn push(&self, fault: InjectedFault) -> Result<(), StorageError> {
        self.0
            .lock()
            .map_err(|_| {
                StorageError::new(StorageErrorKind::Internal, "dynamodb_fault_lock_poisoned")
            })?
            .push_back(fault);
        Ok(())
    }

    fn take(
        &self,
        operation: &'static str,
        timing: FaultTiming,
    ) -> Result<Option<StorageError>, StorageError> {
        let mut faults = self.0.lock().map_err(|_| {
            StorageError::new(StorageErrorKind::Internal, "dynamodb_fault_lock_poisoned")
        })?;
        let Some(position) = faults
            .iter()
            .position(|fault| fault.operation == operation && fault.timing == timing)
        else {
            return Ok(None);
        };
        Ok(faults
            .remove(position)
            .map(|fault| StorageError::new(fault.kind, fault.code)))
    }
}

impl fmt::Debug for DynamoClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamoClient")
            .field("table_name", &self.table_name)
            .field("available_permits", &self.permits.available_permits())
            .field("background_timeout", &self.background_timeout)
            .field("maximum_attempts", &self.maximum_attempts)
            .finish_non_exhaustive()
    }
}

pub(crate) fn map_sdk(
    code: &'static str,
    error: impl std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
) -> StorageError {
    let kind = match error.code() {
        Some("ConditionalCheckFailedException") => StorageErrorKind::Conflict,
        Some("ResourceNotFoundException") if code == "dynamodb_describe_table_failed" => {
            StorageErrorKind::NotFound
        }
        Some(
            "ResourceNotFoundException"
            | "AccessDeniedException"
            | "UnrecognizedClientException"
            | "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "RequestLimitExceeded",
        ) => StorageErrorKind::Unavailable,
        _ if format!("{error:?}")
            .to_ascii_lowercase()
            .contains("timeout") =>
        {
            StorageErrorKind::Timeout
        }
        _ => StorageErrorKind::Internal,
    };
    StorageError::with_source(kind, code, error)
}

fn map_transaction_sdk(
    code: &'static str,
    error: SdkError<TransactWriteItemsError>,
) -> StorageError {
    let kind = match error.as_service_error() {
        Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) => {
            classify_cancellation_reasons(cancelled.cancellation_reasons())
        }
        _ => return map_sdk(code, error),
    };
    StorageError::with_source(kind, code, error)
}

fn transaction_cancellation_kind(
    error: &SdkError<TransactWriteItemsError>,
) -> Option<StorageErrorKind> {
    match error.as_service_error() {
        Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) => Some(
            classify_cancellation_reasons(cancelled.cancellation_reasons()),
        ),
        _ => None,
    }
}

fn classify_cancellation_reasons(reasons: &[CancellationReason]) -> StorageErrorKind {
    let mut conflict = false;
    for reason in reasons {
        match reason.code() {
            None | Some("None") => {}
            Some("ConditionalCheckFailed" | "TransactionConflict") => conflict = true,
            Some("ProvisionedThroughputExceeded" | "ThrottlingError") => {
                return StorageErrorKind::Unavailable;
            }
            Some("ItemCollectionSizeLimitExceeded" | "ValidationError") => {
                return StorageErrorKind::Integrity;
            }
            Some(_) => return StorageErrorKind::Internal,
        }
    }
    if conflict {
        StorageErrorKind::Conflict
    } else {
        StorageErrorKind::Internal
    }
}

const fn cancelled() -> StorageError {
    StorageError::new(StorageErrorKind::Cancelled, "dynamodb_operation_cancelled")
}

const fn timed_out() -> StorageError {
    StorageError::new(StorageErrorKind::Timeout, "dynamodb_operation_timed_out")
}

const fn error_kind_label(kind: StorageErrorKind) -> &'static str {
    match kind {
        StorageErrorKind::Cancelled => "cancelled",
        StorageErrorKind::Conflict => "conflict",
        StorageErrorKind::Integrity => "integrity",
        StorageErrorKind::Internal => "internal",
        StorageErrorKind::NotFound => "not_found",
        StorageErrorKind::ResourceExhausted => "resource_exhausted",
        StorageErrorKind::Timeout => "timeout",
        StorageErrorKind::Unavailable => "unavailable",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use openfga_domain::{ConsistencyPreference, Deadline, RequestTimeout};
    use openfga_storage::StorageCancellationToken;

    use super::*;

    #[test]
    fn test_should_classify_transaction_capacity_before_conflict() {
        let reasons = [
            CancellationReason::builder()
                .code("ConditionalCheckFailed")
                .build(),
            CancellationReason::builder()
                .code("ProvisionedThroughputExceeded")
                .build(),
        ];

        assert_eq!(
            classify_cancellation_reasons(&reasons),
            StorageErrorKind::Unavailable
        );
    }

    #[test]
    fn test_should_treat_unknown_transaction_cancellation_as_internal() {
        let reasons = [CancellationReason::builder().code("FutureReason").build()];

        assert_eq!(
            classify_cancellation_reasons(&reasons),
            StorageErrorKind::Internal
        );
    }

    #[tokio::test]
    async fn test_should_inject_deterministic_faults_before_and_after_dispatch()
    -> Result<(), StorageError> {
        let client = test_client();
        let context = test_context()?;
        let polled = Arc::new(AtomicBool::new(false));
        client.faults.push(InjectedFault {
            operation: "test_operation",
            timing: FaultTiming::BeforeDispatch,
            kind: StorageErrorKind::Unavailable,
            code: "injected_before",
        })?;
        let marker = Arc::clone(&polled);
        let Err(before) = client
            .execute_mapped(
                &context,
                "test_operation",
                async move {
                    marker.store(true, Ordering::Release);
                    Ok::<(), std::io::Error>(())
                },
                |error| StorageError::with_source(StorageErrorKind::Internal, "unexpected", error),
            )
            .await
        else {
            return Err(StorageError::new(
                StorageErrorKind::Internal,
                "injected_pre_dispatch_fault_missing",
            ));
        };
        assert_eq!(before.code(), "injected_before");
        assert!(!polled.load(Ordering::Acquire));

        client.faults.push(InjectedFault {
            operation: "test_operation",
            timing: FaultTiming::AfterDispatch,
            kind: StorageErrorKind::Timeout,
            code: "injected_unknown_commit",
        })?;
        let Err(after) = client
            .execute_mapped(
                &context,
                "test_operation",
                async { Ok::<(), std::io::Error>(()) },
                |error| StorageError::with_source(StorageErrorKind::Internal, "unexpected", error),
            )
            .await
        else {
            return Err(StorageError::new(
                StorageErrorKind::Internal,
                "injected_post_dispatch_fault_missing",
            ));
        };
        assert_eq!(after.kind(), StorageErrorKind::Timeout);
        assert_eq!(after.code(), "injected_unknown_commit");
        Ok(())
    }

    fn test_client() -> DynamoClient {
        let sdk = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-west-2"))
            .build();
        DynamoClient {
            client: Client::from_conf(sdk),
            table_name: "test-table".to_owned(),
            permits: Arc::new(Semaphore::new(1)),
            background_timeout: Duration::from_secs(1),
            maximum_attempts: 1,
            metrics: DynamoMetrics::new(),
            faults: FaultInjector::default(),
        }
    }

    fn test_context() -> Result<OperationContext, StorageError> {
        let timeout = RequestTimeout::new(Duration::from_secs(1)).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "test_timeout", error)
        })?;
        let deadline =
            Deadline::from_timeout(std::time::Instant::now(), timeout).map_err(|error| {
                StorageError::with_source(StorageErrorKind::Internal, "test_deadline", error)
            })?;
        Ok(OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            deadline,
            StorageCancellationToken::new(),
        ))
    }
}
