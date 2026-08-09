//! Pinned live differential for the `AuthZEN` Authorization API 1.0 HTTP surface.

use std::{io::Write, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, Response, Url, redirect::Policy};
use serde::Serialize;
use serde_json::{Value, json};

use super::{check_probe::configure_differential_server, validated_loopback_url};

const GO_BASELINE_COMMIT: &str = "4e4f79ed841513dfd61746a75ef473f6198299f7";
const AUTHZEN_API_COMMIT: &str = "f153694bfc20f7be303e33cabe72b668596c5a06";
const MODEL_HEADER: &str = "Openfga-Authorization-Model-Id";
const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DifferentialAuthzenReport {
    baseline_commit: &'static str,
    authzen_api_commit: &'static str,
    cases: Vec<CaseReport>,
    mismatches: Vec<Mismatch>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CaseReport {
    name: &'static str,
    endpoint: &'static str,
    go: Value,
    rust: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Mismatch {
    case: &'static str,
    go: Value,
    rust: Value,
}

#[derive(Clone, Copy, Debug)]
enum Normalization {
    Json,
    ErrorContexts,
    Search,
    Status,
    Discovery,
}

#[derive(Clone, Copy, Debug)]
struct DifferentialCase {
    name: &'static str,
    endpoint: &'static str,
    body: Option<fn() -> Value>,
    normalization: Normalization,
    pin_model: bool,
}

/// Runs a bounded normalized `AuthZEN` corpus against both loopback servers.
pub(crate) async fn run(go_url: &str, rust_url: &str) -> Result<()> {
    let go_url = validated_loopback_url(go_url).context("invalid Go AuthZEN URL")?;
    let rust_url = validated_loopback_url(rust_url).context("invalid Rust AuthZEN URL")?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(Policy::none())
        .build()
        .context("failed to build the AuthZEN differential client")?;
    let (go_store, go_model) = configure_differential_server(&client, &go_url)
        .await
        .context("failed to configure the Go AuthZEN server")?;
    let (rust_store, rust_model) = configure_differential_server(&client, &rust_url)
        .await
        .context("failed to configure the Rust AuthZEN server")?;

    let mut reports = Vec::with_capacity(cases().len());
    let mut mismatches = Vec::new();
    for case in cases() {
        let go = observe(&client, &go_url, &go_store, &go_model, case)
            .await
            .with_context(|| format!("Go {} AuthZEN case failed", case.name))?;
        let rust = observe(&client, &rust_url, &rust_store, &rust_model, case)
            .await
            .with_context(|| format!("Rust {} AuthZEN case failed", case.name))?;
        if go != rust {
            mismatches.push(Mismatch {
                case: case.name,
                go: go.clone(),
                rust: rust.clone(),
            });
        }
        reports.push(CaseReport {
            name: case.name,
            endpoint: case.endpoint,
            go,
            rust,
        });
    }

    let report = DifferentialAuthzenReport {
        baseline_commit: GO_BASELINE_COMMIT,
        authzen_api_commit: AUTHZEN_API_COMMIT,
        cases: reports,
        mismatches,
    };
    serde_json::to_writer_pretty(std::io::stdout().lock(), &report)
        .context("failed to serialize the AuthZEN differential report")?;
    std::io::stdout()
        .lock()
        .write_all(b"\n")
        .context("failed to terminate the AuthZEN differential report")?;
    if !report.mismatches.is_empty() {
        bail!("AuthZEN differential found normalized mismatches");
    }
    Ok(())
}

fn cases() -> [DifferentialCase; 19] {
    [
        post("evaluation_direct_allow", "evaluation", evaluation_direct),
        post("evaluation_direct_deny", "evaluation", evaluation_deny),
        post(
            "evaluation_computed_allow",
            "evaluation",
            evaluation_computed,
        ),
        post("evaluation_ttu_allow", "evaluation", evaluation_ttu),
        post(
            "evaluation_difference_deny",
            "evaluation",
            evaluation_difference,
        ),
        post("evaluations_execute_all", "evaluations", evaluations_all),
        DifferentialCase {
            name: "evaluations_item_error",
            endpoint: "evaluations",
            body: Some(evaluations_item_error),
            normalization: Normalization::ErrorContexts,
            pin_model: true,
        },
        post(
            "evaluations_deny_on_first_deny",
            "evaluations",
            evaluations_deny_first,
        ),
        post(
            "evaluations_permit_on_first_permit",
            "evaluations",
            evaluations_permit_first,
        ),
        search("subject_search", "search/subject", subject_search),
        search("resource_search", "search/resource", resource_search),
        search("action_search", "search/action", action_search),
        DifferentialCase {
            name: "invalid_evaluation",
            endpoint: "evaluation",
            body: Some(empty_request),
            normalization: Normalization::Status,
            pin_model: false,
        },
        DifferentialCase {
            name: "invalid_evaluations",
            endpoint: "evaluations",
            body: Some(empty_request),
            normalization: Normalization::Status,
            pin_model: false,
        },
        DifferentialCase {
            name: "invalid_subject_search",
            endpoint: "search/subject",
            body: Some(empty_request),
            normalization: Normalization::Status,
            pin_model: false,
        },
        DifferentialCase {
            name: "invalid_resource_search",
            endpoint: "search/resource",
            body: Some(empty_request),
            normalization: Normalization::Status,
            pin_model: false,
        },
        DifferentialCase {
            name: "invalid_action_search",
            endpoint: "search/action",
            body: Some(empty_request),
            normalization: Normalization::Status,
            pin_model: false,
        },
        DifferentialCase {
            name: "latest_model_fallback",
            endpoint: "evaluation",
            body: Some(evaluation_direct),
            normalization: Normalization::Json,
            pin_model: false,
        },
        DifferentialCase {
            name: "discovery",
            endpoint: "discovery",
            body: None,
            normalization: Normalization::Discovery,
            pin_model: false,
        },
    ]
}

const fn post(name: &'static str, endpoint: &'static str, body: fn() -> Value) -> DifferentialCase {
    DifferentialCase {
        name,
        endpoint,
        body: Some(body),
        normalization: Normalization::Json,
        pin_model: true,
    }
}

const fn search(
    name: &'static str,
    endpoint: &'static str,
    body: fn() -> Value,
) -> DifferentialCase {
    DifferentialCase {
        name,
        endpoint,
        body: Some(body),
        normalization: Normalization::Search,
        pin_model: true,
    }
}

async fn observe(
    client: &Client,
    base_url: &Url,
    store_id: &str,
    model_id: &str,
    case: DifferentialCase,
) -> Result<Value> {
    let relative = if case.body.is_some() {
        format!("stores/{store_id}/access/v1/{}", case.endpoint)
    } else {
        format!(".well-known/authzen-configuration/{store_id}")
    };
    let url = base_url
        .join(&relative)
        .context("failed to build AuthZEN URL")?;
    let mut request = match case.body {
        Some(body) => client.post(url).json(&body()),
        None => client.get(url),
    };
    if case.pin_model {
        request = request.header(MODEL_HEADER, model_id);
    }
    let response = request
        .send()
        .await
        .context("AuthZEN differential request failed")?;
    normalize_response(response, case.normalization, store_id).await
}

async fn normalize_response(
    response: Response,
    normalization: Normalization,
    store_id: &str,
) -> Result<Value> {
    let status = response.status().as_u16();
    let body = read_bounded(response).await?;
    if matches!(normalization, Normalization::Status) {
        return Ok(json!({"status": status}));
    }
    let mut value =
        serde_json::from_slice::<Value>(&body).context("AuthZEN response was not valid JSON")?;
    match normalization {
        Normalization::Json => normalize_json(&mut value),
        Normalization::ErrorContexts => {
            normalize_json(&mut value);
            remove_error_messages(&mut value);
        }
        Normalization::Search => {
            normalize_json(&mut value);
            sort_results(&mut value)?;
        }
        Normalization::Discovery => normalize_discovery(&mut value, store_id)?,
        Normalization::Status => {}
    }
    Ok(json!({"status": status, "body": value}))
}

async fn read_bounded(mut response: Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read AuthZEN response")?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .context("AuthZEN response length overflow")?;
        if next > MAXIMUM_RESPONSE_BYTES {
            bail!("AuthZEN response exceeded the byte ceiling");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn normalize_json(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                normalize_json(item);
            }
        }
        Value::Object(fields) => {
            fields.retain(|_, value| !value.is_null());
            for child in fields.values_mut() {
                normalize_json(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn remove_error_messages(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                remove_error_messages(item);
            }
        }
        Value::Object(fields) => {
            fields.remove("message");
            if fields.get("status").and_then(Value::as_f64) == Some(400.0) {
                fields.insert("status".to_owned(), json!(400));
            } else if fields.get("status").and_then(Value::as_f64) == Some(500.0) {
                fields.insert("status".to_owned(), json!(500));
            }
            for child in fields.values_mut() {
                remove_error_messages(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn sort_results(value: &mut Value) -> Result<()> {
    let Some(results) = value.get_mut("results").and_then(Value::as_array_mut) else {
        bail!("AuthZEN search response omitted results");
    };
    results.sort_by_key(Value::to_string);
    Ok(())
}

fn normalize_discovery(value: &mut Value, store_id: &str) -> Result<()> {
    let Some(fields) = value.as_object_mut() else {
        bail!("AuthZEN discovery response was not an object");
    };
    for name in [
        "policy_decision_point",
        "access_evaluation_endpoint",
        "access_evaluations_endpoint",
        "search_subject_endpoint",
        "search_resource_endpoint",
        "search_action_endpoint",
    ] {
        if let Some(url) = fields.get_mut(name) {
            let absolute = url
                .as_str()
                .context("AuthZEN discovery URL was not a string")?
                .parse::<Url>()
                .context("AuthZEN discovery URL was not absolute")?;
            *url = Value::String(
                absolute
                    .path()
                    .replace(&format!("/stores/{store_id}"), "/stores/{store}"),
            );
        }
    }
    fields
        .entry("capabilities".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    normalize_json(value);
    Ok(())
}

fn subject(id: &str) -> Value {
    json!({"type": "user", "id": id})
}

fn resource(id: &str) -> Value {
    json!({"type": "document", "id": id})
}

fn action(name: &str) -> Value {
    json!({"name": name})
}

fn evaluation(resource_id: &str, action_name: &str) -> Value {
    json!({
        "subject": subject("anne"),
        "resource": resource(resource_id),
        "action": action(action_name),
    })
}

fn evaluation_direct() -> Value {
    evaluation("direct", "viewer")
}

fn evaluation_deny() -> Value {
    evaluation("missing", "viewer")
}

fn evaluation_computed() -> Value {
    evaluation("computed", "viewer")
}

fn evaluation_ttu() -> Value {
    evaluation("ttu", "viewer")
}

fn evaluation_difference() -> Value {
    evaluation("excluded", "allowed")
}

fn evaluations_all() -> Value {
    json!({
        "subject": subject("anne"),
        "action": action("viewer"),
        "evaluations": [
            {"resource": resource("direct")},
            {"resource": resource("missing")},
            {"resource": resource("computed")}
        ],
        "options": {"evaluations_semantic": "execute_all"}
    })
}

fn evaluations_deny_first() -> Value {
    json!({
        "subject": subject("anne"),
        "action": action("viewer"),
        "evaluations": [
            {"resource": resource("direct")},
            {"resource": resource("missing")},
            {"resource": resource("computed")}
        ],
        "options": {"evaluations_semantic": "deny_on_first_deny"}
    })
}

fn evaluations_permit_first() -> Value {
    json!({
        "subject": subject("anne"),
        "action": action("viewer"),
        "evaluations": [
            {"resource": resource("missing")},
            {"resource": resource("direct")},
            {"resource": resource("computed")}
        ],
        "options": {"evaluations_semantic": "permit_on_first_permit"}
    })
}

fn evaluations_item_error() -> Value {
    json!({
        "subject": subject("anne"),
        "action": action("viewer"),
        "evaluations": [
            {"resource": resource("direct")},
            {"resource": {"type": "unknown", "id": "missing"}}
        ],
        "options": {"evaluations_semantic": "execute_all"}
    })
}

fn subject_search() -> Value {
    json!({
        "resource": resource("direct"),
        "action": action("viewer"),
        "subject": {"type": "user", "id": "ignored"},
        "page": {"limit": 1, "token": "ignored"}
    })
}

fn resource_search() -> Value {
    json!({
        "subject": subject("anne"),
        "action": action("viewer"),
        "resource": {"type": "document", "id": "ignored"}
    })
}

fn action_search() -> Value {
    json!({
        "subject": subject("anne"),
        "resource": resource("both")
    })
}

fn empty_request() -> Value {
    json!({})
}
