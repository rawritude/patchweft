//! Git and path rules both sides apply: the scheduler when it imports and validates, the runner
//! when it checks a job's source out and captures its outputs.
use anyhow::{Context, Result, bail, ensure};
use std::{
    path::{Component, Path, PathBuf},
    process::{Command, Output},
};

pub fn git_command(repo: &Path) -> Command {
    let mut command = Command::new("/usr/bin/git");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C.UTF-8")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        // /etc/gitattributes would change checkout bytes outside every declared input (pw#11 gate P3).
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            // Checkouts write the tree's bytes as declared inputs hash them: line-ending conversion
            // comes only from tracked .gitattributes, which are inputs (pw#11 gate P2-2).
            "-c",
            "core.autocrlf=false",
            "-C",
        ])
        .arg(repo);
    command
}

pub fn checked(output: Output, operation: &str) -> Result<String> {
    if !output.status.success() {
        bail!(
            "{operation}: {}",
            String::from_utf8_lossy(&output.stderr)
                .chars()
                .take(4000)
                .collect::<String>()
        );
    }
    Ok(String::from_utf8(output.stdout)?
        .trim_end_matches('\n')
        .to_owned())
}

pub fn git(repo: &Path, args: &[&str]) -> Result<String> {
    checked(git_command(repo).args(args).output()?, args[0])
}

/// Checks `commit` out of whichever repository holds it: the store's, or an executor's mirror.
pub fn checkout_from(repository: &Path, commit: &str, destination: &Path) -> Result<()> {
    ensure!(!destination.exists(), "checkout destination exists");
    // git runs in the destination's parent, where a relative path would mean something else.
    ensure!(
        repository.is_absolute() && destination.is_absolute(),
        "checkout paths must be absolute"
    );
    let parent = destination
        .parent()
        .context("checkout destination parent")?;
    let args = [
        "-c",
        "protocol.file.allow=always",
        "clone",
        "--quiet",
        "--shared",
        "--no-checkout",
    ];
    checked(
        git_command(parent)
            .args(args)
            .arg(repository)
            .arg(destination)
            .output()?,
        "clone",
    )?;
    git(destination, &["checkout", "--quiet", "--detach", commit])?;
    Ok(())
}

pub fn protected(path: &Path) -> bool {
    path.components().any(|c| {
        let name = c.as_os_str().to_string_lossy();
        name.starts_with(".env")
            || name == ".secrets"
            || name == ".ssh"
            || name == ".aws"
            || name == ".git"
    })
}

pub fn relative_path(path: &str) -> Result<PathBuf> {
    let value = PathBuf::from(path);
    ensure!(
        !path.is_empty()
            && value
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "paths must be relative without traversal: {path}"
    );
    ensure!(!protected(&value), "protected path refused: {path}");
    Ok(value)
}
