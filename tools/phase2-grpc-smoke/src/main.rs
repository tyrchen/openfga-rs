//! Differential gRPC scenario for the complete Phase 2 endpoint surface.

#![forbid(unsafe_code)]

use std::{collections::HashMap, io::Write, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use openfga_proto::openfga::v1::{self as pb, open_fga_service_client::OpenFgaServiceClient};
use serde::Serialize;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

#[derive(Debug, Parser)]
#[command(about = "Compare the delivered OpenFGA gRPC API against the pinned Go server")]
struct Arguments {
    #[arg(long)]
    go_url: String,
    #[arg(long)]
    rust_url: String,
    #[arg(long)]
    rust_ca: PathBuf,
    #[arg(long)]
    rust_token: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ScenarioReport {
    store_round_trip: Evidence,
    model_count: usize,
    tuple_count: usize,
    check_decision: Decision,
    batch_count: usize,
    assertion_count: usize,
    change_count: usize,
    delete_only: Evidence,
    update_unimplemented: WireError,
    invalid_page: WireError,
    invalid_store: WireError,
    missing_tuple: WireError,
    object_too_long: WireError,
    relation_too_long: WireError,
    invalid_user: WireError,
    empty_write: WireError,
    missing_type: WireError,
    missing_relation: WireError,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum Evidence {
    Pass,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum Decision {
    Allowed,
    Denied,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireError {
    code: String,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let go = connect(&arguments.go_url, None).await?;
    let ca = tokio::fs::read(&arguments.rust_ca)
        .await
        .context("failed to read the Rust server CA certificate")?;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .domain_name("localhost");
    let rust = connect(&arguments.rust_url, Some(tls)).await?;
    let rust_token = format!("Bearer {}", arguments.rust_token)
        .parse::<MetadataValue<Ascii>>()
        .context("Rust API token is not valid gRPC metadata")?;

    let go_report = scenario(go, None).await?;
    let rust_report = scenario(rust, Some(&rust_token)).await?;
    if go_report != rust_report {
        bail!(
            "Phase 2 gRPC mismatch\nGo: {}\nRust: {}",
            serde_json::to_string(&go_report)?,
            serde_json::to_string(&rust_report)?,
        );
    }
    let output = format!("{}\n", serde_json::to_string(&rust_report)?);
    std::io::stdout()
        .write_all(output.as_bytes())
        .context("failed to write the gRPC differential report")?;
    Ok(())
}

async fn connect(url: &str, tls: Option<ClientTlsConfig>) -> Result<OpenFgaServiceClient<Channel>> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())
        .context("gRPC URL is invalid")?
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(5));
    if let Some(tls) = tls {
        endpoint = endpoint
            .tls_config(tls)
            .context("gRPC client TLS configuration is invalid")?;
    }
    let channel = endpoint.connect().await.context("gRPC connection failed")?;
    Ok(OpenFgaServiceClient::new(channel))
}

async fn scenario(
    mut client: OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
) -> Result<ScenarioReport> {
    let store_id = create_store_round_trip(&mut client, token).await?;
    let model = client
        .write_authorization_model(request(model_request(&store_id), token))
        .await?
        .into_inner();
    let model_id = model.authorization_model_id;
    let model_count = model_round_trip(&mut client, token, &store_id, &model_id).await?;
    let tuple = relationship_tuple();
    client
        .write(request(
            pb::WriteRequest {
                store_id: store_id.clone(),
                writes: Some(pb::WriteRequestWrites {
                    tuple_keys: vec![tuple.clone()],
                    on_duplicate: String::new(),
                }),
                deletes: None,
                authorization_model_id: model_id.clone(),
            },
            token,
        ))
        .await?;
    let queries = query_round_trip(&mut client, token, &store_id, &model_id).await?;
    let assertion_count = assertion_round_trip(&mut client, token, &store_id, &model_id).await?;
    let change_count = client
        .read_changes(request(
            pb::ReadChangesRequest {
                store_id: store_id.clone(),
                r#type: String::new(),
                page_size: Some(pbjson_types::Int32Value { value: 100 }),
                continuation_token: String::new(),
                start_time: None,
            },
            token,
        ))
        .await?
        .into_inner()
        .changes
        .len();
    let delete_only = delete_only(&mut client, token, &store_id, &model_id, tuple).await?;
    let errors = error_parity(&mut client, token, &store_id, &model_id).await?;
    client
        .delete_store(request(
            pb::DeleteStoreRequest {
                store_id: store_id.clone(),
            },
            token,
        ))
        .await?;

    Ok(ScenarioReport {
        store_round_trip: Evidence::Pass,
        model_count,
        tuple_count: queries.0,
        check_decision: if queries.1 {
            Decision::Allowed
        } else {
            Decision::Denied
        },
        batch_count: queries.2,
        assertion_count,
        change_count,
        delete_only: if delete_only {
            Evidence::Pass
        } else {
            bail!("delete-only Write did not complete")
        },
        update_unimplemented: errors.update_unimplemented,
        invalid_page: errors.invalid_page,
        invalid_store: errors.invalid_store,
        missing_tuple: errors.missing_tuple,
        object_too_long: errors.object_too_long,
        relation_too_long: errors.relation_too_long,
        invalid_user: errors.invalid_user,
        empty_write: errors.empty_write,
        missing_type: errors.missing_type,
        missing_relation: errors.missing_relation,
    })
}

async fn create_store_round_trip(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
) -> Result<String> {
    let store = client
        .create_store(request(
            pb::CreateStoreRequest {
                name: "phase2-grpc-smoke".to_owned(),
            },
            token,
        ))
        .await?
        .into_inner();
    let fetched = client
        .get_store(request(
            pb::GetStoreRequest {
                store_id: store.id.clone(),
            },
            token,
        ))
        .await?
        .into_inner();
    if fetched.id != store.id || fetched.name != "phase2-grpc-smoke" {
        bail!("GetStore did not preserve the created store");
    }
    Ok(store.id)
}

async fn model_round_trip(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
) -> Result<usize> {
    let model = client
        .read_authorization_model(request(
            pb::ReadAuthorizationModelRequest {
                store_id: store_id.to_owned(),
                id: model_id.to_owned(),
            },
            token,
        ))
        .await?
        .into_inner();
    if model.authorization_model.is_none() {
        bail!("ReadAuthorizationModel omitted the model");
    }
    Ok(client
        .read_authorization_models(request(
            pb::ReadAuthorizationModelsRequest {
                store_id: store_id.to_owned(),
                page_size: Some(pbjson_types::Int32Value { value: 100 }),
                continuation_token: String::new(),
            },
            token,
        ))
        .await?
        .into_inner()
        .authorization_models
        .len())
}

async fn query_round_trip(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
) -> Result<(usize, bool, usize)> {
    let tuples = client
        .read(request(
            pb::ReadRequest {
                store_id: store_id.to_owned(),
                tuple_key: None,
                page_size: Some(pbjson_types::Int32Value { value: 100 }),
                continuation_token: String::new(),
                consistency: 0,
            },
            token,
        ))
        .await?
        .into_inner();
    let check_request = check_request(store_id, model_id);
    let check = client
        .check(request(check_request.clone(), token))
        .await?
        .into_inner();
    let batch = client
        .batch_check(request(
            pb::BatchCheckRequest {
                store_id: store_id.to_owned(),
                checks: vec![pb::BatchCheckItem {
                    tuple_key: check_request.tuple_key,
                    contextual_tuples: None,
                    context: None,
                    correlation_id: "item-1".to_owned(),
                }],
                authorization_model_id: model_id.to_owned(),
                consistency: 0,
            },
            token,
        ))
        .await?
        .into_inner();
    Ok((tuples.tuples.len(), check.allowed, batch.result.len()))
}

async fn assertion_round_trip(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
) -> Result<usize> {
    client
        .write_assertions(request(
            pb::WriteAssertionsRequest {
                store_id: store_id.to_owned(),
                authorization_model_id: model_id.to_owned(),
                assertions: vec![pb::Assertion {
                    tuple_key: Some(pb::AssertionTupleKey {
                        object: "document:roadmap".to_owned(),
                        relation: "viewer".to_owned(),
                        user: "user:anne".to_owned(),
                    }),
                    expectation: true,
                    contextual_tuples: Vec::new(),
                    context: None,
                }],
            },
            token,
        ))
        .await?;
    Ok(client
        .read_assertions(request(
            pb::ReadAssertionsRequest {
                store_id: store_id.to_owned(),
                authorization_model_id: model_id.to_owned(),
            },
            token,
        ))
        .await?
        .into_inner()
        .assertions
        .len())
}

async fn delete_only(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
    tuple: pb::TupleKey,
) -> Result<bool> {
    client
        .write(request(
            pb::WriteRequest {
                store_id: store_id.to_owned(),
                writes: None,
                deletes: Some(pb::WriteRequestDeletes {
                    tuple_keys: vec![pb::TupleKeyWithoutCondition {
                        user: tuple.user,
                        relation: tuple.relation,
                        object: tuple.object,
                    }],
                    on_missing: String::new(),
                }),
                authorization_model_id: model_id.to_owned(),
            },
            token,
        ))
        .await?;
    Ok(true)
}

#[allow(
    clippy::too_many_lines,
    reason = "the ordered exact-wire matrix is kept together so Go and Rust exercise identical \
              state"
)]
async fn error_parity(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
) -> Result<ErrorEvidence> {
    let update = client
        .update_store(request(
            pb::UpdateStoreRequest {
                store_id: store_id.to_owned(),
                name: "ignored".to_owned(),
            },
            token,
        ))
        .await
        .err()
        .context("UpdateStore unexpectedly succeeded")?;
    let page = client
        .list_stores(request(
            pb::ListStoresRequest {
                page_size: Some(pbjson_types::Int32Value { value: 101 }),
                continuation_token: "!".to_owned(),
                name: "x".to_owned(),
            },
            token,
        ))
        .await
        .err()
        .context("invalid page size unexpectedly succeeded")?;
    let store = client
        .get_store(request(
            pb::GetStoreRequest {
                store_id: "short".to_owned(),
            },
            token,
        ))
        .await
        .err()
        .context("invalid store ID unexpectedly succeeded")?;
    let missing_tuple = invalid_check(client, token, store_id, model_id, None)
        .await
        .context("missing Check tuple_key unexpectedly succeeded")?;
    let object_too_long = invalid_check(
        client,
        token,
        store_id,
        model_id,
        Some(check_tuple(
            &format!("document:{}", "x".repeat(513)),
            &"r".repeat(51),
            "x",
        )),
    )
    .await
    .context("oversized Check object unexpectedly succeeded")?;
    let relation_too_long = invalid_check(
        client,
        token,
        store_id,
        model_id,
        Some(check_tuple(
            "document:roadmap",
            &"r".repeat(51),
            "user:anne",
        )),
    )
    .await
    .context("oversized Check relation unexpectedly succeeded")?;
    let invalid_user = invalid_check(
        client,
        token,
        store_id,
        model_id,
        Some(check_tuple("document:roadmap", "viewer", "invalid-user")),
    )
    .await
    .context("malformed Check user unexpectedly succeeded")?;
    let empty_write = client
        .write(request(
            pb::WriteRequest {
                store_id: store_id.to_owned(),
                writes: None,
                deletes: None,
                authorization_model_id: model_id.to_owned(),
            },
            token,
        ))
        .await
        .err()
        .context("empty Write unexpectedly succeeded")?;
    let missing_type = invalid_check(
        client,
        token,
        store_id,
        model_id,
        Some(check_tuple("unknown:roadmap", "viewer", "user:anne")),
    )
    .await
    .context("Check with a missing type unexpectedly succeeded")?;
    let missing_relation = invalid_check(
        client,
        token,
        store_id,
        model_id,
        Some(check_tuple("document:roadmap", "editor", "user:anne")),
    )
    .await
    .context("Check with a missing relation unexpectedly succeeded")?;
    Ok(ErrorEvidence {
        update_unimplemented: wire_error(&update),
        invalid_page: wire_error(&page),
        invalid_store: wire_error(&store),
        missing_tuple: wire_error(&missing_tuple),
        object_too_long: wire_error(&object_too_long),
        relation_too_long: wire_error(&relation_too_long),
        invalid_user: wire_error(&invalid_user),
        empty_write: wire_error(&empty_write),
        missing_type: wire_error(&missing_type),
        missing_relation: wire_error(&missing_relation),
    })
}

#[derive(Debug)]
struct ErrorEvidence {
    update_unimplemented: WireError,
    invalid_page: WireError,
    invalid_store: WireError,
    missing_tuple: WireError,
    object_too_long: WireError,
    relation_too_long: WireError,
    invalid_user: WireError,
    empty_write: WireError,
    missing_type: WireError,
    missing_relation: WireError,
}

async fn invalid_check(
    client: &mut OpenFgaServiceClient<Channel>,
    token: Option<&MetadataValue<Ascii>>,
    store_id: &str,
    model_id: &str,
    tuple_key: Option<pb::CheckRequestTupleKey>,
) -> Option<tonic::Status> {
    client
        .check(request(
            pb::CheckRequest {
                store_id: store_id.to_owned(),
                tuple_key,
                contextual_tuples: None,
                authorization_model_id: model_id.to_owned(),
                trace: false,
                context: None,
                consistency: 0,
            },
            token,
        ))
        .await
        .err()
}

fn check_tuple(object: &str, relation: &str, user: &str) -> pb::CheckRequestTupleKey {
    pb::CheckRequestTupleKey {
        user: user.to_owned(),
        relation: relation.to_owned(),
        object: object.to_owned(),
    }
}

fn wire_error(status: &tonic::Status) -> WireError {
    WireError {
        code: format!("{:?}", status.code()),
        message: status.message().to_owned(),
    }
}

fn request<T>(value: T, token: Option<&MetadataValue<Ascii>>) -> Request<T> {
    let mut request = Request::new(value);
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", token.clone());
    }
    request
}

fn relationship_tuple() -> pb::TupleKey {
    pb::TupleKey {
        user: "user:anne".to_owned(),
        relation: "viewer".to_owned(),
        object: "document:roadmap".to_owned(),
        condition: None,
    }
}

fn check_request(store_id: &str, model_id: &str) -> pb::CheckRequest {
    pb::CheckRequest {
        store_id: store_id.to_owned(),
        tuple_key: Some(pb::CheckRequestTupleKey {
            user: "user:anne".to_owned(),
            relation: "viewer".to_owned(),
            object: "document:roadmap".to_owned(),
        }),
        contextual_tuples: None,
        authorization_model_id: model_id.to_owned(),
        trace: false,
        context: None,
        consistency: 0,
    }
}

fn model_request(store_id: &str) -> pb::WriteAuthorizationModelRequest {
    let direct = pb::Userset {
        userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
    };
    pb::WriteAuthorizationModelRequest {
        store_id: store_id.to_owned(),
        schema_version: "1.1".to_owned(),
        type_definitions: vec![
            pb::TypeDefinition {
                r#type: "user".to_owned(),
                relations: HashMap::new(),
                metadata: Some(pb::Metadata {
                    relations: HashMap::new(),
                    module: String::new(),
                    source_info: None,
                }),
            },
            pb::TypeDefinition {
                r#type: "document".to_owned(),
                relations: HashMap::from([("viewer".to_owned(), direct)]),
                metadata: Some(pb::Metadata {
                    relations: HashMap::from([(
                        "viewer".to_owned(),
                        pb::RelationMetadata {
                            directly_related_user_types: vec![pb::RelationReference {
                                r#type: "user".to_owned(),
                                condition: String::new(),
                                relation_or_wildcard: None,
                            }],
                            module: String::new(),
                            source_info: None,
                        },
                    )]),
                    module: String::new(),
                    source_info: None,
                }),
            },
        ],
        conditions: HashMap::new(),
    }
}
