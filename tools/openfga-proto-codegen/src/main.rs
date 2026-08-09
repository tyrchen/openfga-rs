//! Reproducible `OpenFGA` v1 Tonic/Prost and route-metadata generator.

#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::{Component, Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tonic_prost_build::Config;

const EXPECTED_API_REPOSITORY: &str = "https://github.com/openfga/api.git";
const EXPECTED_PROTOC_DISTRIBUTION: &str = "protoc-bin-vendored 3.2.0";
const EXPECTED_PROTOC_VERSION: &str = "31.1";
const EXPECTED_PROST_VERSION: &str = "0.14.4";
const EXPECTED_PROST_REFLECT_VERSION: &str = "0.16.5";
const EXPECTED_PROST_VALIDATE_TYPES_VERSION: &str = "0.2.9";
const EXPECTED_TONIC_VERSION: &str = "0.14.6";
const EXPECTED_PBJSON_VERSION: &str = "0.9.0";
const EXPECTED_IMPORT_PROVENANCE: &[(&str, &str, &str, &str)] = &[
    (
        "buf.build/envoyproxy/protoc-gen-validate",
        "https://github.com/envoyproxy/protoc-gen-validate.git",
        "414042a5ff2e98dc47f8161937316a25b1da5bba",
        "licenses/envoyproxy-protoc-gen-validate.LICENSE",
    ),
    (
        "buf.build/googleapis/googleapis",
        "https://github.com/googleapis/googleapis.git",
        "23141773936b44fb83e26edaf39b64e50a691cb1",
        "licenses/googleapis.LICENSE",
    ),
    (
        "buf.build/grpc-ecosystem/grpc-gateway",
        "https://github.com/grpc-ecosystem/grpc-gateway.git",
        "cb724e4d41e20a47bb6067845908d3f19e79b984",
        "licenses/grpc-gateway.LICENSE",
    ),
];
const EXPECTED_PROTOC_BINARY_SHA256: &[(&str, &str)] = &[
    (
        "linux-aarch64",
        "5ea6b5ee26c6169925d6c99c8141b855db02c4d382c60b48f535971791b6e8b8",
    ),
    (
        "linux-ppc64le",
        "4e33404682b4c09e8126ade198284f0de2fecee586b5f6569832f998a25e1eed",
    ),
    (
        "linux-s390x",
        "6867b2e740c858564873d14896c29be2f6ff19bfde4b58b166296caeb8918b0b",
    ),
    (
        "linux-x86",
        "4ead2930a0d1c57ef4cf573b30d804186c6f76480c4bef3eb6f129a95f7b4a1f",
    ),
    (
        "linux-x86_64",
        "caaf8517e57c57d34a7d6f0544172d9051abf58556aa35c70d3fb0d824b8cfbb",
    ),
    (
        "macos-aarch64",
        "a54e72a5c3b06f8eecd235a0ea22ca8494cec738a655cabbd0b9a9864f82f4f1",
    ),
    (
        "macos-x86_64",
        "773c020a052293bafe187e7c3cd0a0116e2d25798373c6fa1f47648e2a4d5303",
    ),
    (
        "windows-x86_64",
        "cbd1ca1fd6afd1bb6ddd1c09c118ecd4c50f928980857f16fe6fc23704ea17e2",
    ),
];
const GENERATED_FILES: &[&str] = &[
    "authzen.v1.rs",
    "authzen.v1.serde.rs",
    "openfga.v1.rs",
    "openfga.v1.serde.rs",
    "openfga_descriptor.bin",
    "route_metadata.rs",
];
const OPENFGA_PROTO_FILES: &[&str] = &[
    "authzen/v1/authzen_service.proto",
    "openfga/v1/authzmodel.proto",
    "openfga/v1/errors_ignore.proto",
    "openfga/v1/openapi.proto",
    "openfga/v1/openfga.proto",
    "openfga/v1/openfga_service.proto",
    "openfga/v1/openfga_service_consistency.proto",
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProtocolLock {
    api: ApiLock,
    imports: Vec<ImportLock>,
    protoc: ProtocLock,
    generated: GeneratedLock,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApiLock {
    repository: String,
    commit: String,
    git_archive_sha256: String,
    buf_lock_sha256: String,
    vendored_imports_aggregate_sha256: String,
    inputs_sha256: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImportLock {
    module: String,
    commit: String,
    digest: String,
    source_repository: String,
    license_source_commit: String,
    license_file: String,
    license_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProtocLock {
    version: String,
    distribution: String,
    binary_sha256: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GeneratedLock {
    tonic: String,
    prost: String,
    prost_reflect: String,
    prost_validate_types: String,
    pbjson: String,
    aggregate_sha256: String,
}

#[derive(Debug, Eq, PartialEq)]
struct BufDependency {
    commit: String,
    digest: String,
}

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
    let protocol_lock =
        load_protocol_lock(&workspace_root.join("crates/openfga-proto/proto.lock.json")).await?;
    verify_lock_metadata(&protocol_lock)?;
    verify_workspace_dependency_versions(&workspace_root, &protocol_lock).await?;

    verify_api_pin(&api_root, &protocol_lock.api, &protocol_lock.imports).await?;
    verify_vendored_proto_inputs(&vendor_root, &protocol_lock.api).await?;
    verify_import_licenses(&vendor_root, &protocol_lock.imports).await?;
    tokio::fs::create_dir_all(&arguments.output)
        .await
        .with_context(|| format!("failed to create {}", arguments.output.display()))?;

    let protoc_binary = protoc_bin_vendored::protoc_bin_path()
        .context("failed to locate the vendored protoc binary")?;
    let protoc_include = protoc_bin_vendored::include_path()
        .context("failed to locate the vendored protoc includes")?;
    verify_protoc(&protoc_binary, &protocol_lock.protoc).await?;

    let proto_inputs = OPENFGA_PROTO_FILES
        .iter()
        .map(|path| api_root.join(path))
        .collect::<Vec<_>>();
    let includes = vec![api_root.clone(), vendor_root, protoc_include];
    let descriptor_path = arguments.output.join("openfga_descriptor.bin");
    let reflection_bootstrap = arguments.output.join(".reflection-bootstrap");
    tokio::fs::create_dir(&reflection_bootstrap)
        .await
        .context("failed to create reflection bootstrap directory")?;

    let mut prost_config = Config::new();
    prost_config.protoc_executable(&protoc_binary);
    prost_config.out_dir(&reflection_bootstrap);
    prost_config.compile_well_known_types();
    prost_config.extern_path(".google.protobuf", "::pbjson_types");
    prost_reflect_build::Builder::new()
        .file_descriptor_set_path(&descriptor_path)
        .file_descriptor_set_bytes("crate::FILE_DESCRIPTOR_SET")
        .configure(&mut prost_config, &proto_inputs, &includes)
        .context("failed to configure pinned protobuf reflection")?;
    tokio::fs::remove_dir_all(&reflection_bootstrap)
        .await
        .context("failed to remove reflection bootstrap directory")?;
    tonic_prost_build::configure()
        .out_dir(&arguments.output)
        .file_descriptor_set_path(&descriptor_path)
        .extern_path(".google.api", "::prost_types")
        .extern_path(".grpc.gateway", "::prost_types")
        .extern_path(".validate", "::prost_types")
        .compile_with_config(prost_config, &proto_inputs, &includes)
        .context("failed to generate Tonic/Prost protocol artifacts")?;
    let descriptors = tokio::fs::read(&descriptor_path)
        .await
        .context("failed to read generated protocol descriptors")?;
    let mut json_builder = pbjson_build::Builder::new();
    json_builder
        .ignore_unknown_fields()
        .ignore_unknown_enum_variants()
        .out_dir(&arguments.output)
        .register_descriptors(&descriptors)
        .context("failed to register protocol descriptors for protobuf JSON")?
        .build(&[".authzen.v1", ".openfga.v1"])
        .context("failed to generate protobuf JSON implementations")?;
    reject_duplicate_generated_map_keys(&arguments.output.join("openfga.v1.serde.rs")).await?;
    reject_unknown_generated_numeric_enums(
        &arguments.output.join("authzen.v1.serde.rs"),
        &["EvaluationsSemantic"],
    )
    .await?;
    emit_authzen_required_defaults(&arguments.output.join("authzen.v1.serde.rs")).await?;
    reject_unknown_generated_numeric_enums(
        &arguments.output.join("openfga.v1.serde.rs"),
        &[
            "AuthErrorCode",
            "ConsistencyPreference",
            "ErrorCode",
            "InternalErrorCode",
            "NotFoundErrorCode",
            "TupleOperation",
            "UnprocessableContentErrorCode",
            "condition_param_type_ref::TypeName",
        ],
    )
    .await?;

    generate_route_metadata(
        &api_root.join("docs/openapiv2/apidocs.swagger.json"),
        &arguments.output.join("route_metadata.rs"),
    )
    .await?;
    verify_generated_artifacts(&arguments.output, &protocol_lock.generated).await?;

    Ok(())
}

async fn reject_unknown_generated_numeric_enums(path: &Path, enums: &[&str]) -> Result<()> {
    let mut generated = tokio::fs::read_to_string(path).await.with_context(|| {
        format!(
            "failed to read generated protobuf JSON at {}",
            path.display()
        )
    })?;
    for name in enums {
        let permissive = format!("x.try_into().ok().or_else(|| Some({name}::default()))");
        let replacements = generated.matches(&permissive).count();
        if replacements == 0 {
            bail!("pbjson output contains no recognized numeric deserializer for {name}");
        }
        generated = generated.replace(&permissive, "x.try_into().ok()");
    }
    tokio::fs::write(path, generated)
        .await
        .with_context(|| format!("failed to harden protobuf JSON at {}", path.display()))?;
    Ok(())
}

async fn emit_authzen_required_defaults(path: &Path) -> Result<()> {
    const GENERATED_LENGTH: &str =
        "let mut len = 0;\n        if self.decision {\n            len += 1;\n        }";
    const REQUIRED_LENGTH: &str = "let mut len = 1;";
    const GENERATED_FIELD: &str =
        "if self.decision {\n            struct_ser.serialize_field(\"decision\", \
         &self.decision)?;\n        }";
    const REQUIRED_FIELD: &str = "struct_ser.serialize_field(\"decision\", &self.decision)?;";

    let mut generated = tokio::fs::read_to_string(path).await.with_context(|| {
        format!(
            "failed to read generated protobuf JSON at {}",
            path.display()
        )
    })?;
    if generated.matches(GENERATED_LENGTH).count() != 1
        || generated.matches(GENERATED_FIELD).count() != 1
    {
        bail!("pbjson output does not contain the required AuthZEN decision serializer shape");
    }
    generated = generated
        .replace(GENERATED_LENGTH, REQUIRED_LENGTH)
        .replace(GENERATED_FIELD, REQUIRED_FIELD);
    tokio::fs::write(path, generated)
        .await
        .with_context(|| format!("failed to harden protobuf JSON at {}", path.display()))?;
    Ok(())
}

async fn reject_duplicate_generated_map_keys(path: &Path) -> Result<()> {
    const GENERATED: &str = "map_.next_value::<std::collections::HashMap<_, _>>()?";
    const PROJECT_OWNED: &str =
        "map_.next_value::<crate::DuplicateRejectingMap<_, _>>()?.into_inner()";

    let generated = tokio::fs::read_to_string(path).await.with_context(|| {
        format!(
            "failed to read generated protobuf JSON at {}",
            path.display()
        )
    })?;
    let replacements = generated.matches(GENERATED).count();
    if replacements == 0 {
        bail!("pbjson output contains no recognized protobuf map deserializers");
    }
    let hardened = generated.replace(GENERATED, PROJECT_OWNED);
    tokio::fs::write(path, hardened)
        .await
        .with_context(|| format!("failed to harden protobuf JSON at {}", path.display()))?;
    Ok(())
}

async fn load_protocol_lock(path: &Path) -> Result<ProtocolLock> {
    let contents = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&contents).with_context(|| format!("failed to parse {}", path.display()))
}

fn verify_lock_metadata(protocol_lock: &ProtocolLock) -> Result<()> {
    if protocol_lock.api.repository != EXPECTED_API_REPOSITORY {
        bail!("protocol lock names an unexpected API repository");
    }
    if protocol_lock.protoc.version != EXPECTED_PROTOC_VERSION
        || protocol_lock.protoc.distribution != EXPECTED_PROTOC_DISTRIBUTION
    {
        bail!("protocol lock does not match the reviewed protoc dependency");
    }
    if protocol_lock.generated.tonic != EXPECTED_TONIC_VERSION
        || protocol_lock.generated.prost != EXPECTED_PROST_VERSION
        || protocol_lock.generated.prost_reflect != EXPECTED_PROST_REFLECT_VERSION
        || protocol_lock.generated.prost_validate_types != EXPECTED_PROST_VALIDATE_TYPES_VERSION
        || protocol_lock.generated.pbjson != EXPECTED_PBJSON_VERSION
    {
        bail!("protocol lock does not match the reviewed Tonic/Prost/pbjson dependencies");
    }
    if !is_lower_hex(&protocol_lock.api.commit, 40)
        || !is_lower_hex(&protocol_lock.api.git_archive_sha256, 64)
        || !is_lower_hex(&protocol_lock.api.buf_lock_sha256, 64)
        || !is_lower_hex(&protocol_lock.api.vendored_imports_aggregate_sha256, 64)
        || !is_lower_hex(&protocol_lock.generated.aggregate_sha256, 64)
    {
        bail!("protocol lock contains a malformed source or artifact digest");
    }
    if protocol_lock.imports.len() != EXPECTED_IMPORT_PROVENANCE.len()
        || !protoc_platform_map_matches(&protocol_lock.protoc.binary_sha256)
        || EXPECTED_IMPORT_PROVENANCE.iter().any(
            |(module, source_repository, license_source_commit, license_file)| {
                !protocol_lock.imports.iter().any(|import| {
                    import.module == *module
                        && import.source_repository == *source_repository
                        && import.license_source_commit == *license_source_commit
                        && import.license_file == *license_file
                })
            },
        )
    {
        bail!("protocol lock does not contain the complete reviewed platform/import set");
    }
    Ok(())
}

fn protoc_platform_map_matches(binary_sha256: &BTreeMap<String, String>) -> bool {
    binary_sha256.len() == EXPECTED_PROTOC_BINARY_SHA256.len()
        && EXPECTED_PROTOC_BINARY_SHA256
            .iter()
            .all(|(platform, digest)| {
                binary_sha256
                    .get(*platform)
                    .is_some_and(|actual| actual == digest)
            })
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn verify_workspace_dependency_versions(
    workspace_root: &Path,
    protocol_lock: &ProtocolLock,
) -> Result<()> {
    let path = workspace_root.join("Cargo.lock");
    let contents = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    let locked_versions = cargo_locked_versions(&contents)?;
    let expected = [
        ("prost", protocol_lock.generated.prost.as_str()),
        (
            "prost-reflect",
            protocol_lock.generated.prost_reflect.as_str(),
        ),
        (
            "prost-validate-types",
            protocol_lock.generated.prost_validate_types.as_str(),
        ),
        ("protoc-bin-vendored", "3.2.0"),
        ("tonic", protocol_lock.generated.tonic.as_str()),
    ];
    for (package, expected_version) in expected {
        if locked_versions.get(package).map(String::as_str) != Some(expected_version) {
            bail!("protocol lock version for {package} does not match Cargo.lock");
        }
    }
    Ok(())
}

fn cargo_locked_versions(contents: &str) -> Result<BTreeMap<String, String>> {
    let mut versions = BTreeMap::new();
    let mut package_name = None;
    for line in contents.lines() {
        if line == "[[package]]" {
            package_name = None;
        } else if let Some(name) = quoted_toml_value(line, "name") {
            package_name = Some(name);
        } else if let (Some(name), Some(version)) =
            (package_name.take(), quoted_toml_value(line, "version"))
            && [
                "prost",
                "prost-reflect",
                "prost-validate-types",
                "protoc-bin-vendored",
                "tonic",
            ]
            .contains(&name.as_str())
            && versions.insert(name.clone(), version).is_some()
        {
            bail!("Cargo.lock contains duplicate reviewed protocol package {name}");
        }
    }
    Ok(versions)
}

fn quoted_toml_value(line: &str, key: &str) -> Option<String> {
    line.strip_prefix(&format!("{key} = \""))
        .and_then(|value| value.strip_suffix('"'))
        .map(str::to_owned)
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("code generator is not located under the workspace tools directory"))
}

async fn verify_api_pin(api_root: &Path, api: &ApiLock, imports: &[ImportLock]) -> Result<()> {
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
    if commit.trim() != api.commit {
        bail!(
            "OpenFGA API source pin mismatch: expected {}, found {}",
            api.commit,
            commit.trim()
        );
    }
    let remote = Command::new("git")
        .arg("-C")
        .arg(api_root)
        .args(["remote", "get-url", "origin"])
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to inspect the OpenFGA API source remote")?;
    let remote_url =
        String::from_utf8(remote.stdout).context("OpenFGA API remote was not valid UTF-8")?;
    if !remote.status.success() || remote_url.trim() != api.repository {
        bail!("OpenFGA API source remote does not match the protocol lock");
    }

    let archive = Command::new("git")
        .arg("-C")
        .arg(api_root)
        .args(["archive", "--format=tar", "HEAD"])
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to archive the OpenFGA API source")?;
    if !archive.status.success() || sha256_hex(&archive.stdout)? != api.git_archive_sha256 {
        bail!("OpenFGA API archive checksum does not match the protocol lock");
    }

    let lock_path = api_root.join("buf.lock");
    let buf_lock = tokio::fs::read(&lock_path)
        .await
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    if sha256_hex(&buf_lock)? != api.buf_lock_sha256 {
        bail!("OpenFGA API buf.lock checksum does not match the reviewed source pin");
    }
    let buf_lock_text = String::from_utf8(buf_lock).context("OpenFGA buf.lock is not UTF-8")?;
    let buf_dependencies = parse_buf_dependencies(&buf_lock_text)?;
    if buf_dependencies.len() != imports.len() {
        bail!("OpenFGA buf.lock dependency set does not match the protocol lock");
    }
    for import in imports {
        verify_import_metadata(import, &buf_dependencies)?;
    }

    let expected_input_count = OPENFGA_PROTO_FILES.len().saturating_add(1);
    if api.inputs_sha256.len() != expected_input_count
        || !api
            .inputs_sha256
            .contains_key("docs/openapiv2/apidocs.swagger.json")
        || OPENFGA_PROTO_FILES
            .iter()
            .any(|path| !api.inputs_sha256.contains_key(*path))
    {
        bail!("protocol lock does not contain the exact OpenFGA API input set");
    }
    for (path, expected_digest) in &api.inputs_sha256 {
        if !is_lower_hex(expected_digest, 64) {
            bail!("protocol lock has a malformed OpenFGA API input checksum");
        }
        let input_path = api_root.join(path);
        let input = tokio::fs::read(&input_path)
            .await
            .with_context(|| format!("failed to read {}", input_path.display()))?;
        let actual_digest = sha256_hex(&input)?;
        if actual_digest != *expected_digest {
            bail!("OpenFGA API input checksum mismatch for {path}");
        }
    }
    Ok(())
}

fn parse_buf_dependencies(buf_lock: &str) -> Result<BTreeMap<String, BufDependency>> {
    let mut blocks = buf_lock.split("\n  - remote: ");
    let header = blocks
        .next()
        .context("OpenFGA buf.lock has no document header")?;
    if !header.ends_with("version: v1\ndeps:") {
        bail!("OpenFGA buf.lock has an unsupported document structure");
    }

    let mut dependencies = BTreeMap::new();
    for block in blocks {
        let mut lines = block.lines();
        if lines.next() != Some("buf.build") {
            bail!("OpenFGA buf.lock contains a non-BSR dependency");
        }
        let mut fields = BTreeMap::new();
        for line in lines {
            let (key, value) = line
                .trim()
                .split_once(": ")
                .context("OpenFGA buf.lock contains a malformed dependency field")?;
            if fields.insert(key, value).is_some() {
                bail!("OpenFGA buf.lock contains a duplicate dependency field");
            }
        }
        if fields.len() != 4 {
            bail!("OpenFGA buf.lock dependency does not contain the exact reviewed fields");
        }
        let owner = fields
            .remove("owner")
            .context("OpenFGA buf.lock dependency has no owner")?;
        let repository = fields
            .remove("repository")
            .context("OpenFGA buf.lock dependency has no repository")?;
        let commit = fields
            .remove("commit")
            .context("OpenFGA buf.lock dependency has no commit")?;
        let digest = fields
            .remove("digest")
            .context("OpenFGA buf.lock dependency has no digest")?;
        let module = format!("buf.build/{owner}/{repository}");
        if dependencies
            .insert(
                module,
                BufDependency {
                    commit: commit.to_owned(),
                    digest: digest.to_owned(),
                },
            )
            .is_some()
        {
            bail!("OpenFGA buf.lock contains a duplicate module");
        }
    }
    Ok(dependencies)
}

fn verify_import_metadata(
    import: &ImportLock,
    dependencies: &BTreeMap<String, BufDependency>,
) -> Result<()> {
    let module = import
        .module
        .strip_prefix("buf.build/")
        .context("protocol import is not a public BSR module")?;
    let (owner, repository) = module
        .split_once('/')
        .context("protocol import has an invalid BSR module name")?;
    let dependency = dependencies
        .get(&import.module)
        .context("protocol import is absent from the pinned API buf.lock")?;
    if owner.is_empty()
        || repository.is_empty()
        || !is_lower_hex(&import.commit, 32)
        || !import
            .digest
            .strip_prefix("shake256:")
            .is_some_and(|digest| is_lower_hex(digest, 128))
        || dependency.commit != import.commit
        || dependency.digest != import.digest
    {
        bail!("protocol import metadata does not match the pinned API buf.lock");
    }
    if !import.source_repository.starts_with("https://github.com/")
        || !Path::new(&import.source_repository)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("git"))
        || !is_lower_hex(&import.license_source_commit, 40)
        || !is_safe_license_path(&import.license_file)
        || !is_lower_hex(&import.license_sha256, 64)
    {
        bail!("protocol import license provenance is malformed");
    }
    Ok(())
}

fn is_safe_license_path(path: &str) -> bool {
    let path = Path::new(path);
    path.starts_with("licenses")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

async fn verify_protoc(protoc: &Path, lock: &ProtocLock) -> Result<()> {
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
    if version.trim() != format!("libprotoc {}", lock.version) {
        bail!("unexpected protoc version: {}", version.trim());
    }
    let binary = tokio::fs::read(protoc)
        .await
        .context("failed to checksum the vendored protoc binary")?;
    let digest = sha256_hex(&binary)?;
    let platform = current_protoc_platform()?;
    if lock.binary_sha256.get(platform) != Some(&digest) {
        bail!("vendored protoc binary checksum does not match platform {platform}");
    }
    Ok(())
}

fn current_protoc_platform() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "aarch64") => Ok("linux-aarch64"),
        ("linux", "powerpc64") if cfg!(target_endian = "little") => Ok("linux-ppc64le"),
        ("linux", "s390x") => Ok("linux-s390x"),
        ("linux", "x86") => Ok("linux-x86"),
        ("linux", "x86_64") => Ok("linux-x86_64"),
        ("macos", "aarch64") => Ok("macos-aarch64"),
        ("macos", "x86_64") => Ok("macos-x86_64"),
        ("windows", "x86_64") => Ok("windows-x86_64"),
        (operating_system, architecture) => {
            bail!("unsupported protoc platform {operating_system}-{architecture}")
        }
    }
}

async fn verify_vendored_proto_inputs(vendor_root: &Path, api: &ApiLock) -> Result<()> {
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
    if digest != api.vendored_imports_aggregate_sha256 {
        bail!("vendored protocol import checksum does not match the reviewed source set");
    }
    Ok(())
}

async fn verify_import_licenses(vendor_root: &Path, imports: &[ImportLock]) -> Result<()> {
    for import in imports {
        let path = vendor_root.join(&import.license_file);
        let contents = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read import license {}", path.display()))?;
        if sha256_hex(&contents)? != import.license_sha256 {
            bail!(
                "protocol import license checksum mismatch for {}",
                import.module
            );
        }
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

async fn verify_generated_artifacts(output: &Path, lock: &GeneratedLock) -> Result<()> {
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
    if actual_digest != lock.aggregate_sha256 {
        bail!(
            "generated protocol checksum mismatch: expected {}, found {actual_digest}",
            lock.aggregate_sha256
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
        "// @generated by openfga-proto-codegen. Do not edit.\n\n/// `OpenFGA` and `AuthZEN` v1 \
         HTTP routes from the pinned API source.\npub const OPENFGA_HTTP_ROUTES: &[HttpRoute] = \
         &[\n",
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

#[cfg(test)]
mod tests {
    use super::{
        ProtocolLock, parse_buf_dependencies, verify_import_metadata, verify_lock_metadata,
    };

    #[test]
    fn test_should_reject_protocol_lock_dependency_drift() {
        let protocol_lock = serde_json::from_str::<ProtocolLock>(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../crates/openfga-proto/proto.lock.json"
        )));
        assert!(protocol_lock.is_ok());
        let Some(mut protocol_lock) = protocol_lock.ok() else {
            return;
        };
        assert!(verify_lock_metadata(&protocol_lock).is_ok());
        protocol_lock.generated.tonic = "0.0.0-drift".to_owned();
        assert!(verify_lock_metadata(&protocol_lock).is_err());
    }

    #[test]
    fn test_should_reject_protocol_import_tuple_drift() {
        let protocol_lock = serde_json::from_str::<ProtocolLock>(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../crates/openfga-proto/proto.lock.json"
        )));
        let dependencies = parse_buf_dependencies(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../vendors/openfga-api/buf.lock"
        )));
        assert!(protocol_lock.is_ok() && dependencies.is_ok());
        let (Some(mut protocol_lock), Some(dependencies)) = (protocol_lock.ok(), dependencies.ok())
        else {
            return;
        };
        assert!(
            protocol_lock
                .imports
                .iter()
                .all(|import| verify_import_metadata(import, &dependencies).is_ok())
        );
        let replacement_commit = protocol_lock
            .imports
            .iter()
            .find(|import| import.module == "buf.build/googleapis/googleapis")
            .map(|import| import.commit.clone());
        assert!(replacement_commit.is_some());
        let Some(replacement_commit) = replacement_commit else {
            return;
        };
        let target = protocol_lock
            .imports
            .iter_mut()
            .find(|import| import.module == "buf.build/envoyproxy/protoc-gen-validate");
        assert!(target.is_some());
        let Some(target) = target else {
            return;
        };
        target.commit = replacement_commit;
        assert!(verify_import_metadata(target, &dependencies).is_err());
    }

    #[test]
    fn test_should_reject_protocol_platform_digest_drift() {
        let protocol_lock = serde_json::from_str::<ProtocolLock>(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../crates/openfga-proto/proto.lock.json"
        )));
        assert!(protocol_lock.is_ok());
        let Some(mut protocol_lock) = protocol_lock.ok() else {
            return;
        };
        let linux_digest = protocol_lock
            .protoc
            .binary_sha256
            .get("linux-x86_64")
            .cloned();
        let macos_digest = protocol_lock
            .protoc
            .binary_sha256
            .get("macos-x86_64")
            .cloned();
        assert!(linux_digest.is_some() && macos_digest.is_some());
        let (Some(linux_digest), Some(macos_digest)) = (linux_digest, macos_digest) else {
            return;
        };
        protocol_lock
            .protoc
            .binary_sha256
            .insert("linux-x86_64".to_owned(), macos_digest);
        protocol_lock
            .protoc
            .binary_sha256
            .insert("macos-x86_64".to_owned(), linux_digest);
        assert!(verify_lock_metadata(&protocol_lock).is_err());
    }
}
