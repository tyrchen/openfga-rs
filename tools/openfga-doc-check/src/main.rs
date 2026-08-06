//! Repository-local Markdown link validation.

#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

fn main() -> Result<()> {
    let workspace = workspace_root()?;
    let mut files = vec![workspace.join("README.md")];
    collect_markdown(&workspace.join("docs"), &mut files)?;
    collect_markdown(&workspace.join("specs"), &mut files)?;

    let mut invalid = Vec::new();
    for file in files {
        check_links(&workspace, &file, &mut invalid)?;
    }
    if !invalid.is_empty() {
        bail!("invalid local Markdown links:\n{}", invalid.join("\n"));
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
fn collect_markdown(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("failed to inspect {}", directory.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", entry.path().display()))?;
        if file_type.is_dir() {
            collect_markdown(&entry.path(), files)?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "md")
        {
            files.push(entry.path());
        }
    }
    Ok(())
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
    use super::inline_link_targets;

    #[test]
    fn test_should_extract_multiple_inline_markdown_links() {
        let targets = inline_link_targets("[one](a.md) and [two](../b.md#section)");
        assert_eq!(targets, ["a.md", "../b.md#section"]);
    }
}
