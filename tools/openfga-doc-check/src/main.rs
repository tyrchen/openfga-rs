//! Repository-local documentation and observability artifact validation.

#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use config::{Config, File, FileFormat};

const ROOT_MARKDOWN_FILES: &[&str] = &[
    "README.md",
    "CHANGELOG.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
];

fn main() -> Result<()> {
    let workspace = workspace_root()?;
    let mut markdown_files = ROOT_MARKDOWN_FILES
        .iter()
        .map(|file| workspace.join(file))
        .collect::<Vec<_>>();
    collect_files(&workspace.join("docs"), "md", &mut markdown_files)?;
    collect_files(&workspace.join("specs"), "md", &mut markdown_files)?;

    let mut invalid = Vec::new();
    for file in markdown_files {
        check_links(&workspace, &file, &mut invalid)?;
    }
    if !invalid.is_empty() {
        bail!("invalid local Markdown links:\n{}", invalid.join("\n"));
    }

    let mut json_files = Vec::new();
    collect_files(
        &workspace.join("deploy/observability"),
        "json",
        &mut json_files,
    )?;
    for file in json_files {
        check_json(&file)?;
    }
    let mut yaml_files = Vec::new();
    collect_files(&workspace.join(".github"), "yml", &mut yaml_files)?;
    collect_files(&workspace.join(".github"), "yaml", &mut yaml_files)?;
    for file in yaml_files {
        check_yaml(&file)?;
        if file.starts_with(workspace.join(".github/workflows")) {
            check_workflow_pins(&file)?;
        }
    }
    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .context("documentation checker is not under the workspace tools directory")
}

// This finite command-line scan has no async runtime in which synchronous filesystem work blocks.
#[allow(clippy::disallowed_methods)]
fn collect_files(directory: &Path, extension: &str, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("failed to inspect {}", directory.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", entry.path().display()))?;
        if file_type.is_dir() {
            collect_files(&entry.path(), extension, files)?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|candidate| candidate == extension)
        {
            files.push(entry.path());
        }
    }
    Ok(())
}

// This finite command-line scan has no async runtime in which synchronous filesystem work blocks.
#[allow(clippy::disallowed_methods)]
fn check_json(file: &Path) -> Result<()> {
    let contents =
        fs::read_to_string(file).with_context(|| format!("failed to read {}", file.display()))?;
    parse_json(&contents).with_context(|| format!("invalid JSON in {}", file.display()))?;
    Ok(())
}

fn parse_json(contents: &str) -> std::result::Result<serde_json::Value, serde_json::Error> {
    serde_json::from_str(contents)
}

fn check_yaml(file: &Path) -> Result<()> {
    Config::builder()
        .add_source(File::from(file).format(FileFormat::Yaml).required(true))
        .build()
        .with_context(|| format!("invalid YAML in {}", file.display()))?;
    Ok(())
}

// This finite command-line scan has no async runtime in which synchronous filesystem work blocks.
#[allow(clippy::disallowed_methods)]
fn check_workflow_pins(file: &Path) -> Result<()> {
    let contents =
        fs::read_to_string(file).with_context(|| format!("failed to read {}", file.display()))?;
    for (index, line) in contents.lines().enumerate() {
        let value = line.trim_start().trim_start_matches("- ");
        if let Some(action) = value.strip_prefix("uses:").map(str::trim) {
            let action = action
                .split_once('#')
                .map_or(action, |(value, _)| value)
                .trim();
            if action.starts_with("./") {
                continue;
            }
            let Some((_, revision)) = action.rsplit_once('@') else {
                bail!(
                    "GitHub Action is not versioned at {}:{}",
                    file.display(),
                    index.saturating_add(1),
                );
            };
            if !is_lower_hex(revision, 40) && !is_exact_semver_tag(revision) {
                bail!(
                    "GitHub Action is not pinned to a lowercase commit SHA or exact SemVer tag at \
                     {}:{}",
                    file.display(),
                    index.saturating_add(1),
                );
            }
        }
        if let Some(image) = value.strip_prefix("image:").map(str::trim) {
            let Some((_, digest)) = image.rsplit_once("@sha256:") else {
                bail!(
                    "workflow service image is not digest-pinned at {}:{}",
                    file.display(),
                    index.saturating_add(1),
                );
            };
            if !is_lower_hex(digest, 64) {
                bail!(
                    "workflow service image has an invalid SHA-256 digest at {}:{}",
                    file.display(),
                    index.saturating_add(1),
                );
            }
        }
    }
    Ok(())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_exact_semver_tag(value: &str) -> bool {
    let Some(version) = value.strip_prefix('v') else {
        return false;
    };
    let mut components = version.split('.');
    matches!(
        (
            components.next(),
            components.next(),
            components.next(),
            components.next(),
        ),
        (Some(major), Some(minor), Some(patch), None)
            if is_decimal_component(major)
                && is_decimal_component(minor)
                && is_decimal_component(patch)
    )
}

fn is_decimal_component(value: &str) -> bool {
    value == "0"
        || value
            .strip_prefix(|character: char| ('1'..='9').contains(&character))
            .is_some_and(|remaining| remaining.bytes().all(|byte| byte.is_ascii_digit()))
}

// This finite command-line scan has no async runtime in which synchronous filesystem work blocks.
#[allow(clippy::disallowed_methods)]
fn check_links(workspace: &Path, file: &Path, invalid: &mut Vec<String>) -> Result<()> {
    let contents =
        fs::read_to_string(file).with_context(|| format!("failed to read {}", file.display()))?;
    for target in inline_link_targets(&contents) {
        if is_external_or_anchor(target) {
            continue;
        }
        let path_part = target
            .split('#')
            .next()
            .unwrap_or_default()
            .trim_matches(['<', '>']);
        if path_part.is_empty() {
            continue;
        }
        let resolved = if let Some(repository_path) = path_part.strip_prefix('/') {
            workspace.join(repository_path)
        } else {
            file.parent()
                .context("Markdown file has no parent directory")?
                .join(path_part)
        };
        if !resolved.exists() {
            invalid.push(format!("{} -> {target}", file.display()));
        }
    }
    Ok(())
}

fn inline_link_targets(contents: &str) -> Vec<&str> {
    let mut targets = Vec::new();
    let mut remaining = contents;
    while let Some(marker) = remaining.find("](") {
        let target_start = marker.saturating_add(2);
        let Some(after_marker) = remaining.get(target_start..) else {
            break;
        };
        let Some(target_end) = after_marker.find(')') else {
            break;
        };
        if let Some(target) = after_marker.get(..target_end) {
            targets.push(target);
        }
        let next_start = target_end.saturating_add(1);
        let Some(next) = after_marker.get(next_start..) else {
            break;
        };
        remaining = next;
    }
    targets
}

fn is_external_or_anchor(target: &str) -> bool {
    target.starts_with('#')
        || target.starts_with("http://")
        || target.starts_with("https://")
        || target.starts_with("mailto:")
}

#[cfg(test)]
mod tests {
    use super::{inline_link_targets, is_exact_semver_tag, is_lower_hex, parse_json};

    #[test]
    fn test_should_extract_multiple_inline_markdown_links() {
        let targets = inline_link_targets("[one](a.md) and [two](../b.md#section)");
        assert_eq!(targets, ["a.md", "../b.md#section"]);
    }

    #[test]
    fn test_should_reject_invalid_observability_json() {
        assert!(matches!(
            parse_json(r#"{"groups": [}"#),
            Err(error) if error.is_syntax() || error.is_eof()
        ));
    }

    #[test]
    fn test_should_accept_only_exact_lowercase_hex_pins() {
        assert!(is_lower_hex("0123456789abcdef", 16));
        assert!(!is_lower_hex("0123456789ABCDEF", 16));
        assert!(!is_lower_hex("0123456789abcde", 16));
        assert!(!is_lower_hex("0123456789abcdeg", 16));
    }

    #[test]
    fn test_should_accept_only_exact_stable_semver_action_tags() {
        assert!(is_exact_semver_tag("v3.0.0"));
        assert!(is_exact_semver_tag("v0.12.34"));
        assert!(!is_exact_semver_tag("v3"));
        assert!(!is_exact_semver_tag("v3.0"));
        assert!(!is_exact_semver_tag("3.0.0"));
        assert!(!is_exact_semver_tag("v03.0.0"));
        assert!(!is_exact_semver_tag("v3.0.0-beta.1"));
    }
}
