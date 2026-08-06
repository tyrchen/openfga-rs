//! Reproducible `OpenFGA` v1 Tonic/Prost and route-metadata generator.

#![forbid(unsafe_code)]

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tonic_prost_build::Config;

const EXPECTED_API_COMMIT: &str = "f153694bfc20f7be303e33cabe72b668596c5a06";
const EXPECTED_VENDORED_PROTO_SHA256: &str =
    "8afaacc68fab3005a988d18edaf44cf38f309fc77de6601d04f0825a520d7d28";
const GENERATED_FILES: &[&str] = &[
    "openfga.v1.rs",
    "openfga_descriptor.bin",
    "route_metadata.rs",
];
const OPENFGA_PROTO_FILES: &[&str] = &[
    "openfga/v1/authzmodel.proto",
    "openfga/v1/errors_ignore.proto",
    "openfga/v1/openapi.proto",
    "openfga/v1/openfga.proto",
    "openfga/v1/openfga_service.proto",
    "openfga/v1/openfga_service_consistency.proto",
];
const API_INPUTS: &[(&str, &str)] = &[
    (
        "openfga/v1/authzmodel.proto",
        "c4a2ee0f5bbb3d49659a742c353bef0e9271bf1e88edf1554ec98adaa61e7489",
    ),
    (
        "openfga/v1/errors_ignore.proto",
        "fb356436851b49898c57420f7efac8145c078871c9cfa11f10188c3615dfff9d",
    ),
    (
        "openfga/v1/openapi.proto",
        "e18fb74d6d9d4bfea0c0d4909ceaf319838cfecae57c01dacb922c5ad5632cd3",
    ),
    (
        "openfga/v1/openfga.proto",
        "f06f35b29f619a44f2e936b7d7a46434e4b2e5617426f699a0a56be83eea4f97",
    ),
    (
        "openfga/v1/openfga_service.proto",
        "d05816c630c6f99f66ebda17a1c389e5612935316943c0e4abb0f70cd14f4695",
    ),
    (
        "openfga/v1/openfga_service_consistency.proto",
        "5fe6b029af164de557c79750ea6517943c9ad35ee15683bd88b9a3b0abefc79f",
    ),
    (
        "docs/openapiv2/apidocs.swagger.json",
        "861e32f12c0b63ded8108ec6fc03bbc1fc3ae8d422214072a9a2f1e0981b4855",
    ),
];
const PROTOC_BINARY_SHA256: &[&str] = &[
    "5ea6b5ee26c6169925d6c99c8141b855db02c4d382c60b48f535971791b6e8b8",
    "4e33404682b4c09e8126ade198284f0de2fecee586b5f6569832f998a25e1eed",
    "6867b2e740c858564873d14896c29be2f6ff19bfde4b58b166296caeb8918b0b",
    "4ead2930a0d1c57ef4cf573b30d804186c6f76480c4bef3eb6f129a95f7b4a1f",
    "caaf8517e57c57d34a7d6f0544172d9051abf58556aa35c70d3fb0d824b8cfbb",
    "a54e72a5c3b06f8eecd235a0ea22ca8494cec738a655cabbd0b9a9864f82f4f1",
    "773c020a052293bafe187e7c3cd0a0116e2d25798373c6fa1f47648e2a4d5303",
    "cbd1ca1fd6afd1bb6ddd1c09c118ecd4c50f928980857f16fe6fc23704ea17e2",
];

#[derive(Debug, Parser)]
#[command(about = "Generate pinned OpenFGA v1 Rust protocol artifacts")]
struct Arguments {
    /// Directory receiving generated Rust and descriptor files.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Route {
    path: String,
    method: &'static str,
    operation_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let workspace_root = workspace_root()?;
    let api_root = workspace_root.join("vendors/openfga-api");
    let vendor_root = workspace_root.join("crates/openfga-proto/proto/vendor");

    verify_api_pin(&api_root).await?;
    verify_vendored_proto_inputs(&vendor_root).await?;
    tokio::fs::create_dir_all(&arguments.output)
        .await
        .with_context(|| format!("failed to create {}", arguments.output.display()))?;

    let protoc_binary = protoc_bin_vendored::protoc_bin_path()
        .context("failed to locate the vendored protoc binary")?;
    let protoc_include = protoc_bin_vendored::include_path()
        .context("failed to locate the vendored protoc includes")?;
    verify_protoc(&protoc_binary).await?;

    let proto_inputs = OPENFGA_PROTO_FILES
        .iter()
        .map(|path| api_root.join(path))
        .collect::<Vec<_>>();
    let includes = vec![api_root.clone(), vendor_root, protoc_include];
    let descriptor_path = arguments.output.join("openfga_descriptor.bin");

    let mut prost_config = Config::new();
    prost_config.protoc_executable(&protoc_binary);
    tonic_prost_build::configure()
        .out_dir(&arguments.output)
        .file_descriptor_set_path(&descriptor_path)
        .extern_path(".google.api", "::prost_types")
        .extern_path(".grpc.gateway", "::prost_types")
        .extern_path(".validate", "::prost_types")
        .compile_with_config(prost_config, &proto_inputs, &includes)
        .context("failed to generate Tonic/Prost protocol artifacts")?;

    generate_route_metadata(
        &api_root.join("docs/openapiv2/apidocs.swagger.json"),
        &arguments.output.join("route_metadata.rs"),
    )
    .await?;
    verify_generated_artifacts(
        &arguments.output,
        &workspace_root.join("crates/openfga-proto/proto.lock.json"),
    )
    .await?;

    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("code generator is not located under the workspace tools directory"))
}

async fn verify_api_pin(api_root: &Path) -> Result<()> {
    let git_output = Command::new("git")
        .arg("-C")
        .arg(api_root)
        .args(["rev-parse", "HEAD"])
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to inspect the OpenFGA API submodule")?;
    if !git_output.status.success() {
        bail!("failed to resolve the OpenFGA API submodule commit");
    }
    let commit =
        String::from_utf8(git_output.stdout).context("OpenFGA API commit was not valid UTF-8")?;
    if commit.trim() != EXPECTED_API_COMMIT {
        bail!(
            "OpenFGA API source pin mismatch: expected {EXPECTED_API_COMMIT}, found {}",
            commit.trim()
        );
    }

    let lock_path = api_root.join("buf.lock");
    let lock = tokio::fs::read(&lock_path)
        .await
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let lock_digest = sha256_hex(&lock)?;
    if lock_digest != "183317100d2a9ef772e49777dc2c99b9ea9c500f748bdb607c2df6a18bdd9961" {
        bail!("OpenFGA API buf.lock checksum does not match the reviewed source pin");
    }
    for (path, expected_digest) in API_INPUTS {
        let input_path = api_root.join(path);
        let input = tokio::fs::read(&input_path)
            .await
            .with_context(|| format!("failed to read {}", input_path.display()))?;
        let actual_digest = sha256_hex(&input)?;
        if &actual_digest != expected_digest {
            bail!("OpenFGA API input checksum mismatch for {path}");
        }
    }
    Ok(())
}

async fn verify_protoc(protoc: &Path) -> Result<()> {
    let output = Command::new(protoc)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to execute the vendored protoc binary")?;
    if !output.status.success() {
        bail!("vendored protoc failed its version check");
    }
    let version = String::from_utf8(output.stdout).context("protoc version was not valid UTF-8")?;
    if version.trim() != "libprotoc 31.1" {
        bail!("unexpected protoc version: {}", version.trim());
    }
    let binary = tokio::fs::read(protoc)
        .await
        .context("failed to checksum the vendored protoc binary")?;
    let digest = sha256_hex(&binary)?;
    if !PROTOC_BINARY_SHA256.contains(&digest.as_str()) {
        bail!("vendored protoc binary checksum is not in the reviewed platform set");
    }
    Ok(())
}

async fn verify_vendored_proto_inputs(vendor_root: &Path) -> Result<()> {
    let mut directories = vec![vendor_root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&directory)
            .await
            .with_context(|| format!("failed to read {}", directory.display()))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .with_context(|| format!("failed to inspect {}", directory.display()))?
        {
            let file_type = entry
                .file_type()
                .await
                .with_context(|| format!("failed to inspect {}", entry.path().display()))?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "proto")
            {
                files.push(entry.path());
            }
        }
    }
    files.sort();

    let mut aggregate = Sha256::new();
    for path in files {
        let relative = path
            .strip_prefix(vendor_root)
            .context("vendored protocol input escaped its root")?;
        let portable_name = relative
            .to_str()
            .context("vendored protocol input path is not valid UTF-8")?
            .replace('\\', "/");
        let contents = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        aggregate.update(portable_name.as_bytes());
        aggregate.update([0]);
        aggregate.update(contents);
    }
    let digest = hex_encode(&aggregate.finalize())?;
    if digest != EXPECTED_VENDORED_PROTO_SHA256 {
        bail!("vendored protocol import checksum does not match the reviewed source set");
    }
    Ok(())
}

fn sha256_hex(input: &[u8]) -> Result<String> {
    hex_encode(&Sha256::digest(input))
}

fn hex_encode(input: &[u8]) -> Result<String> {
    let mut encoded = String::with_capacity(input.len().saturating_mul(2));
    for byte in input {
        write!(&mut encoded, "{byte:02x}").context("failed to encode a SHA-256 digest")?;
    }
    Ok(encoded)
}

async fn verify_generated_artifacts(output: &Path, lock_path: &Path) -> Result<()> {
    let lock = tokio::fs::read(lock_path)
        .await
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let document: Value = serde_json::from_slice(&lock)
        .with_context(|| format!("failed to parse {}", lock_path.display()))?;
    let expected_digest = document
        .pointer("/generated/aggregateSha256")
        .and_then(Value::as_str)
        .context("protocol lock has no generated aggregate checksum")?;

    let mut aggregate = Sha256::new();
    for name in GENERATED_FILES {
        let path = output.join(name);
        let contents = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read generated artifact {}", path.display()))?;
        aggregate.update(name.as_bytes());
        aggregate.update([0]);
        aggregate.update(contents);
    }
    let actual_digest = hex_encode(&aggregate.finalize())?;
    if actual_digest != expected_digest {
        bail!(
            "generated protocol checksum mismatch: expected {expected_digest}, found \
             {actual_digest}"
        );
    }
    Ok(())
}

async fn generate_route_metadata(swagger_path: &Path, output_path: &Path) -> Result<()> {
    let source = tokio::fs::read(swagger_path)
        .await
        .with_context(|| format!("failed to read {}", swagger_path.display()))?;
    let document: Value = serde_json::from_slice(&source)
        .with_context(|| format!("failed to parse {}", swagger_path.display()))?;
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("OpenAPI document does not contain a paths object"))?;

    let mut routes = Vec::new();
    for (path, operations) in paths {
        if path.contains("/access/v1/") || path.contains("authzen") {
            continue;
        }
        let operations = operations
            .as_object()
            .ok_or_else(|| anyhow!("OpenAPI route {path} is not an object"))?;
        for (method, operation) in operations {
            let method_variant = match method.as_str() {
                "delete" => "Delete",
                "get" => "Get",
                "post" => "Post",
                "put" => "Put",
                unsupported => bail!("unsupported HTTP method {unsupported} for route {path}"),
            };
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("OpenAPI route {path} has no operationId"))?;
            routes.push(Route {
                path: path.clone(),
                method: method_variant,
                operation_id: operation_id.to_owned(),
            });
        }
    }
    routes.sort();

    let mut generated = String::from(
        "// @generated by openfga-proto-codegen. Do not edit.\n\n/// `OpenFGA` v1 HTTP routes \
         from the pinned API source.\npub const OPENFGA_HTTP_ROUTES: &[HttpRoute] = &[\n",
    );
    for route in routes {
        write!(
            &mut generated,
            "    HttpRoute {{\n        method: HttpMethod::{},\n        path: {:?},\n        \
             operation_id: {:?},\n    }},\n",
            route.method, route.path, route.operation_id
        )
        .context("failed to render HTTP route metadata")?;
    }
    generated.push_str("];\n");
    tokio::fs::write(output_path, generated)
        .await
        .with_context(|| format!("failed to write {}", output_path.display()))?;
    Ok(())
}
