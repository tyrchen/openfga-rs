//! Repository-local documentation and observability artifact validation.

#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

fn main() -> Result<()> {
    let workspace = workspace_root()?;
    let mut markdown_files = vec![workspace.join("README.md")];
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
    use super::{inline_link_targets, parse_json};

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
}
