# Observability, dashboard, and alert runbook

The server exports structured traces and metrics through the configured OTLP gRPC endpoint. The same
finite export timeout applies at startup and shutdown. Export failure never changes authorization
semantics; local structured logs and health endpoints remain the containment path.

Import [`openfga-dashboard.json`](../../deploy/observability/openfga-dashboard.json) into Grafana and
load [`openfga-alerts.json`](../../deploy/observability/openfga-alerts.json) as Prometheus-compatible
rule YAML (JSON is valid YAML). Bind `${DS_PROMETHEUS}` to the deployment datasource. Thresholds in
the checked-in alerts match the default configuration and must be reviewed when the request SLO,
controller maximum lag, or pool size changes.

## Metric contract

| Raw OTLP instrument | Prometheus-normalized family | Labels | Meaning |
| --- | --- | --- | --- |
| `openfga.transport.requests` | `openfga_transport_requests_total` | `endpoint_class`, `result` | admitted or concurrency-overloaded requests |
| `openfga.transport.in_flight` | `openfga_transport_in_flight` | `endpoint_class` | requests holding endpoint permits, including live streams |
| `openfga.transport.request.duration` | `openfga_transport_request_duration_seconds` | `endpoint_class`, `result` | admitted lifetime histogram split into bounded success, client/server error, timeout, cancellation, overload, stream-error, and unimplemented classes |
| `openfga.cache.requests` | `openfga_cache_requests_total` | `cache`, `result` | hit, miss, invalidated, or explicit bypass |
| `openfga.cache.controller.running` | `openfga_cache_controller_running` | none | controller task lifecycle |
| `openfga.cache.controller.ready` | `openfga_cache_controller_ready` | none | every tracked store caught up within lag policy |
| `openfga.cache.controller.tracked_stores` | `openfga_cache_controller_tracked_stores` | none | bounded active-store cardinality |
| `openfga.cache.controller.poll_freshness_age` | `openfga_cache_controller_poll_freshness_age_milliseconds` | none | maximum elapsed time since a tracked store's successful changelog poll |
| `openfga.cache.controller.polls.successful` / `polls.failed` | `openfga_cache_controller_polls_successful_total` / `polls_failed_total` | none | cumulative changelog poll outcomes |
| `openfga.cache.controller.flushes` / `overflows` / `restarts` | matching `openfga_cache_controller_*_total` families | none | cumulative conservative invalidation events |
| `openfga.storage.work.wait.duration` | `openfga_storage_work_wait_duration_seconds` | `result` | bounded PostgreSQL work-admission wait |
| `openfga.storage.pool.connections` | `openfga_storage_pool_connections` | `pool_role`, `state` | primary/replica open and idle connections |
| `openfga.storage.work.available` | `openfga_storage_work_available` | none | immediately available storage work permits |

Collector-specific Prometheus translation may preserve a unit without appending the conventional
suffix. Confirm the normalized names once during collector rollout and adjust only the dashboard
queries, never the stable raw OTLP instrument names.

Labels are deliberately bounded. They never contain store, object, subject, tuple, context, token,
principal, DSN, or SQL parameter values.

## Alert response

| Alert | Immediate interpretation and action |
| --- | --- |
| `OpenFgaCacheControllerDown` | Mutable caches are disabled; remove the instance from admission, inspect the supervised task, and restart only after preserving the error |
| `OpenFgaCacheControllerPollStale` | Cache eligibility is at risk; check changelog storage latency/backlog and do not raise `maximumLagMs` during diagnosis |
| `OpenFgaCacheControllerFailures` / `Overflow` | Conservative flushes are protecting correctness; check channel/store cardinality and storage errors |
| `OpenFgaTransportOverload` | The endpoint permit is shedding; compare database wait and CPU before scaling or changing limits |
| `OpenFgaPostgresWorkSaturated` | All work permits are occupied; reduce traffic/fan-out and inspect pool/database latency |
| `OpenFgaPostgresWaitHigh` | Queueing precedes the pool; inspect slow queries, locks, network RTT, and connection reserve |
| `OpenFgaCacheHitRatioLow` | Cache effectiveness regressed; correlate invalidations, TTL, model/store churn, and memory eviction before increasing weights |

After recovery, verify readiness, zero/declining overload, controller poll-freshness age below policy, returned work
permits, stable pool idle capacity, and memory/task return to baseline. Preserve only bounded metric
screenshots and redacted logs in incident evidence.
