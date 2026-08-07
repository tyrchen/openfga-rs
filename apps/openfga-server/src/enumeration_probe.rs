//! Pinned live differential for Phase 3 enumeration and expansion endpoints.

use std::{io::Write, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, Response, Url, redirect::Policy};
use serde::Serialize;
use serde_json::{Value, json};

use super::{check_probe::configure_differential_server, validated_loopback_url};

const GO_BASELINE_COMMIT: &str = "4e4f79ed841513dfd61746a75ef473f6198299f7";
const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DifferentialEnumerationReport {
    baseline_commit: &'static str,
    corpus_source: &'static str,
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
enum ResponseKind {
    Json,
    StreamedObjects,
}

#[derive(Clone, Copy, Debug)]
struct DifferentialCase {
    name: &'static str,
    endpoint: &'static str,
    response_kind: ResponseKind,
    request: fn(&str) -> Value,
}

/// Runs the normalized enumeration/Expand corpus against both loopback servers.
pub(crate) async fn run(go_url: &str, rust_url: &str) -> Result<()> {
    let go_url = validated_loopback_url(go_url).context("invalid Go differential URL")?;
    let rust_url = validated_loopback_url(rust_url).context("invalid Rust differential URL")?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(Policy::none())
        .build()
        .context("failed to build the enumeration differential client")?;
    let (go_store, go_model) = configure_differential_server(&client, &go_url)
        .await
        .context("failed to configure the Go enumeration server")?;
    let (rust_store, rust_model) = configure_differential_server(&client, &rust_url)
        .await
        .context("failed to configure the Rust enumeration server")?;

    let mut reports = Vec::with_capacity(cases().len());
    let mut mismatches = Vec::new();
    for case in cases() {
        let go = observe(&client, &go_url, &go_store, &go_model, case)
            .await
            .with_context(|| format!("Go {} case failed", case.name))?;
        let rust = observe(&client, &rust_url, &rust_store, &rust_model, case)
            .await
            .with_context(|| format!("Rust {} case failed", case.name))?;
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

    let report = DifferentialEnumerationReport {
        baseline_commit: GO_BASELINE_COMMIT,
        corpus_source: "vendors/openfga Phase 3 HTTP surface",
        cases: reports,
        mismatches,
    };
    serde_json::to_writer_pretty(std::io::stdout().lock(), &report)
        .context("failed to serialize the enumeration differential report")?;
    std::io::stdout()
        .lock()
        .write_all(b"\n")
        .context("failed to terminate the enumeration differential report")?;
    if !report.mismatches.is_empty() {
        bail!("enumeration differential found normalized mismatches");
    }
    Ok(())
}

fn cases() -> [DifferentialCase; 12] {
    [
        DifferentialCase {
            name: "list_objects_viewer",
            endpoint: "list-objects",
            response_kind: ResponseKind::Json,
            request: list_objects_viewer,
        },
        DifferentialCase {
            name: "streamed_list_objects_viewer",
            endpoint: "streamed-list-objects",
            response_kind: ResponseKind::StreamedObjects,
            request: list_objects_viewer,
        },
        DifferentialCase {
            name: "list_objects_difference",
            endpoint: "list-objects",
            response_kind: ResponseKind::Json,
            request: list_objects_allowed,
        },
        DifferentialCase {
            name: "list_users_direct",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_direct,
        },
        DifferentialCase {
            name: "list_users_wildcard",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_wildcard,
        },
        DifferentialCase {
            name: "list_users_wildcard_with_explicit",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_wildcard_with_explicit,
        },
        DifferentialCase {
            name: "list_users_userset",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_userset,
        },
        DifferentialCase {
            name: "list_users_intersection",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_intersection,
        },
        DifferentialCase {
            name: "list_users_difference",
            endpoint: "list-users",
            response_kind: ResponseKind::Json,
            request: list_users_difference,
        },
        DifferentialCase {
            name: "expand_direct",
            endpoint: "expand",
            response_kind: ResponseKind::Json,
            request: expand_direct,
        },
        DifferentialCase {
            name: "expand_ttu",
            endpoint: "expand",
            response_kind: ResponseKind::Json,
            request: expand_ttu,
        },
        DifferentialCase {
            name: "expand_difference",
            endpoint: "expand",
            response_kind: ResponseKind::Json,
            request: expand_difference,
        },
    ]
}

async fn observe(
    client: &Client,
    base_url: &Url,
    store_id: &str,
    model_id: &str,
    case: DifferentialCase,
) -> Result<Value> {
    let url = base_url
        .join(&format!("stores/{store_id}/{}", case.endpoint))
        .context("failed to build enumeration URL")?;
    let response = client
        .post(url)
        .json(&(case.request)(model_id))
        .send()
        .await
        .context("enumeration request failed")?;
    let status = response.status();
    let body = read_bounded(response).await?;
    if !status.is_success() {
        bail!(
            "enumeration request returned HTTP {} with a {}-byte body",
            status.as_u16(),
            body.len(),
        );
    }
    match case.response_kind {
        ResponseKind::Json => {
            let mut value = serde_json::from_slice::<Value>(&body)
                .context("enumeration response was not valid JSON")?;
            if case.endpoint == "list-users"
                && let Value::Object(fields) = &mut value
                && !fields.contains_key("users")
            {
                fields.insert("users".to_owned(), Value::Array(Vec::new()));
            }
            normalize(&mut value);
            Ok(value)
        }
        ResponseKind::StreamedObjects => normalize_streamed_objects(&body),
    }
}

async fn read_bounded(mut response: Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read enumeration response")?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .context("enumeration response length overflow")?;
        if next > MAXIMUM_RESPONSE_BYTES {
            bail!("enumeration response exceeded the byte ceiling");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn normalize_streamed_objects(body: &[u8]) -> Result<Value> {
    let mut objects = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let item = serde_json::from_slice::<Value>(line)
                .context("streamed ListObjects item was not valid JSON")?;
            item.get("object")
                .or_else(|| item.get("result").and_then(|result| result.get("object")))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("streamed ListObjects item omitted object")
        })
        .collect::<Result<Vec<_>>>()?;
    objects.sort_unstable();
    Ok(json!({"objects": objects}))
}

fn normalize(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                normalize(item);
            }
        }
        Value::Object(fields) => {
            for child in fields.values_mut() {
                normalize(child);
            }
            if let Some(Value::Object(users)) = fields.get_mut("users")
                && !users.contains_key("users")
            {
                users.insert("users".to_owned(), Value::Array(Vec::new()));
            }
            if let Some(Value::Object(ttu)) = fields.get_mut("tupleToUserset")
                && !ttu.contains_key("computed")
            {
                ttu.insert("computed".to_owned(), Value::Array(Vec::new()));
            }
            for field in ["objects", "users"] {
                if let Some(Value::Array(items)) = fields.get_mut(field) {
                    items.sort_by_key(Value::to_string);
                }
            }
        }
        _ => {}
    }
}

fn list_objects_viewer(model_id: &str) -> Value {
    list_objects_request(model_id, "viewer")
}

fn list_objects_allowed(model_id: &str) -> Value {
    list_objects_request(model_id, "allowed")
}

fn list_objects_request(model_id: &str, relation: &str) -> Value {
    json!({
        "authorization_model_id": model_id,
        "type": "document",
        "relation": relation,
        "user": "user:anne",
        "consistency": "HIGHER_CONSISTENCY"
    })
}

fn list_users_direct(model_id: &str) -> Value {
    list_users_request(model_id, "direct", "viewer")
}

fn list_users_wildcard(model_id: &str) -> Value {
    list_users_request(model_id, "wild", "viewer")
}

fn list_users_wildcard_with_explicit(model_id: &str) -> Value {
    list_users_request(model_id, "wild-plus", "viewer")
}

fn list_users_userset(model_id: &str) -> Value {
    list_users_request(model_id, "userset", "viewer")
}

fn list_users_intersection(model_id: &str) -> Value {
    list_users_request(model_id, "both", "both")
}

fn list_users_difference(model_id: &str) -> Value {
    list_users_request(model_id, "excluded", "allowed")
}

fn list_users_request(model_id: &str, object_id: &str, relation: &str) -> Value {
    json!({
        "authorization_model_id": model_id,
        "object": {"type": "document", "id": object_id},
        "relation": relation,
        "user_filters": [{"type": "user"}],
        "consistency": "HIGHER_CONSISTENCY"
    })
}

fn expand_direct(model_id: &str) -> Value {
    expand_request(model_id, "direct", "viewer")
}

fn expand_ttu(model_id: &str) -> Value {
    expand_request(model_id, "ttu", "viewer")
}

fn expand_difference(model_id: &str) -> Value {
    expand_request(model_id, "excluded", "allowed")
}

fn expand_request(model_id: &str, object_id: &str, relation: &str) -> Value {
    json!({
        "authorization_model_id": model_id,
        "tuple_key": {"object": format!("document:{object_id}"), "relation": relation},
        "consistency": "HIGHER_CONSISTENCY"
    })
}

#[cfg(test)]
mod tests {
    use super::normalize_streamed_objects;

    #[test]
    fn test_should_normalize_streamed_objects_independent_of_order() -> anyhow::Result<()> {
        let normalized = normalize_streamed_objects(
            b"{\"object\":\"document:z\"}\n{\"object\":\"document:a\"}\n",
        )?;
        assert_eq!(
            normalized,
            serde_json::json!({"objects": ["document:a", "document:z"]}),
        );
        Ok(())
    }
}
