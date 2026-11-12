use crate::{
    identity,
    model::{Revision, Workspace},
    now,
    store::{Store, event, insert},
    unique,
};
// Shared with the runner, which checks a job's source out and captures its outputs.
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
pub use patchweft_control::git::{
    checked, checkout_from, git, git_command, protected, relative_path,
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
};

pub fn repository(store: &Store, project: &str) -> PathBuf {
    store.root.join("repos").join(project)
}

pub fn projects(store: &Store) -> Result<serde_json::Value> {
    let mut statement = store.db.prepare(
        "SELECT p.id,p.name,p.origin,f.candidate FROM projects p
         LEFT JOIN frontiers f ON f.project=p.id ORDER BY p.name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(json!({
            "id":row.get::<_,String>(0)?,
            "name":row.get::<_,String>(1)?,
            "origin":row.get::<_,String>(2)?,
            "validated_candidate":row.get::<_,Option<String>>(3)?
        }))
    })?;
    Ok(json!(rows.collect::<rusqlite::Result<Vec<_>>>()?))
}

fn locked(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

pub fn record_revision(
    store: &mut Store,
    project: &str,
    commit: &str,
    parents: Vec<String>,
    owner: &str,
    message: &str,
) -> Result<Revision> {
    let repo = repository(store, project);
    let id = identity(&json!({"project":project, "commit":commit}))?;
    if let Ok(existing) = store.revision(&id) {
        return Ok(existing);
    }
    let revision = Revision {
        id: id.clone(),
        project: project.into(),
        commit: commit.into(),
        tree: git(&repo, &["rev-parse", &format!("{commit}^{{tree}}")])?,
        parents,
        owner: owner.into(),
        message: message.into(),
        created: now(),
    };
    publish_revision(store, revision, None)
}

fn publish_revision(
    store: &mut Store,
    revision: Revision,
    workspace: Option<Workspace>,
) -> Result<Revision> {
    let repo = repository(store, &revision.project);
    git(
        &repo,
        &[
            "update-ref",
            &format!("refs/patchweft/revisions/{}", revision.id),
            &revision.commit,
        ],
    )?;
    let transaction = store.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO revisions VALUES (?,?,?)",
        params![
            revision.id,
            revision.project,
            serde_json::to_string(&revision)?
        ],
    )?;
    if let Some(mut workspace) = workspace {
        workspace.tip = revision.id.clone();
        transaction.execute(
            "UPDATE workspaces SET body=? WHERE id=?",
            params![serde_json::to_string(&workspace)?, workspace.id],
        )?;
        event(
            &transaction,
            "workspace.checkpoint",
            &workspace.id,
            &json!({"revision":revision.id}),
        )?;
    }
    event(&transaction, "revision.available", &revision.id, &revision)?;
    transaction.commit()?;
    Ok(revision)
}

pub fn import(
    store: &mut Store,
    name: &str,
    origin: &Path,
    revision: &str,
    owner: &str,
) -> Result<Revision> {
    ensure!(
        !name.is_empty() && !owner.is_empty(),
        "name and owner must be nonempty"
    );
    let origin = origin.canonicalize()?;
    ensure!(origin.is_dir(), "import requires a local repository");
    let commit = git(
        &origin,
        &["rev-parse", "--verify", &format!("{revision}^{{commit}}")],
    )?;
    ensure!(
        (commit.len() == 40 || commit.len() == 64) && commit.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid commit"
    );
    let project = identity(&json!({"name":name}))?;
    let _lock = locked(&store.root.join("repos").join(format!("{project}.lock")))?;
    let old: Option<String> = store
        .db
        .query_row("SELECT origin FROM projects WHERE id=?", [&project], |r| {
            r.get(0)
        })
        .optional()?;
    let origin_string = origin.to_str().context("non-UTF8 source path")?;
    if let Some(old) = old {
        ensure!(old == origin_string, "project names a different source");
    }
    let target = repository(store, &project);
    if !target.exists() {
        fs::create_dir(&target)?;
        git(&target, &["init", "--bare", "--quiet"])?;
    }
    git(
        &target,
        &[
            "-c",
            "protocol.file.allow=always",
            "fetch",
            "--quiet",
            "--no-tags",
            origin_string,
            &format!("{commit}:refs/patchweft/imports/{commit}"),
        ],
    )?;
    store.db.execute(
        "INSERT OR IGNORE INTO projects VALUES (?,?,?)",
        params![project, name, origin_string],
    )?;
    record_revision(
        store,
        &project,
        &commit,
        vec![],
        owner,
        &format!("Import {name} {commit}"),
    )
}

pub fn checkout(store: &Store, revision: &str, destination: &Path) -> Result<()> {
    let row = store.revision(revision)?;
    checkout_from(&repository(store, &row.project), &row.commit, destination)
}

pub fn workspace(store: &mut Store, revision: &str, owner: &str) -> Result<Workspace> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    let row = store.revision(revision)?;
    let id = unique();
    let path = store.root.join("workspaces").join(&id);
    checkout(store, revision, &path)?;
    let view = Workspace {
        id,
        project: row.project,
        base: revision.into(),
        tip: revision.into(),
        owner: owner.into(),
        path,
    };
    let transaction = store.transaction()?;
    insert(&transaction, "workspaces", &view.id, &view)?;
    event(&transaction, "workspace.created", &view.id, &view)?;
    transaction.commit()?;
    Ok(view)
}

pub fn commit_tree(
    repo: &Path,
    tree: &str,
    parents: &[&str],
    owner: &str,
    message: &str,
) -> Result<String> {
    ensure!(
        !owner.contains(['\n', '\r', '<', '>']),
        "invalid author name"
    );
    let mut command = git_command(repo);
    command
        .args(["commit-tree", tree])
        .env("GIT_AUTHOR_NAME", owner)
        .env("GIT_AUTHOR_EMAIL", "agent@local.invalid")
        .env("GIT_COMMITTER_NAME", "Patchweft")
        .env("GIT_COMMITTER_EMAIL", "patchweft@local.invalid");
    for parent in parents {
        command.args(["-p", parent]);
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .context("commit stdin")?
        .write_all(format!("{message}\n").as_bytes())?;
    checked(child.wait_with_output()?, "commit-tree")
}

pub fn checkpoint(store: &mut Store, workspace_id: &str, message: &str) -> Result<Revision> {
    let view = store.workspace(workspace_id)?;
    let _lock = locked(
        &store
            .root
            .join("workspaces")
            .join(format!("{}.lock", view.id)),
    )?;
    let view = store.workspace(workspace_id)?;
    let base = store.revision(&view.base)?;
    let previous = store.revision(&view.tip)?;
    ensure!(
        git(&view.path, &["rev-parse", "HEAD"])? == base.commit,
        "workspace HEAD changed externally; explicitly import it"
    );
    let changed = git(&view.path, &["diff", "--name-only", "-z", "HEAD"])?;
    let untracked = git(
        &view.path,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?;
    for name in changed
        .split('\0')
        .chain(untracked.split('\0'))
        .filter(|n| !n.is_empty())
    {
        ensure!(
            !protected(Path::new(name)),
            "checkpoint includes protected local configuration"
        );
    }
    // Independent index: no staging or working-file changes in the author's directory.
    let temp = tempfile::tempdir_in(&store.root)?;
    let index = temp.path().join("index");
    let run = |args: &[&str]| -> Result<String> {
        checked(
            git_command(&view.path)
                .env("GIT_INDEX_FILE", &index)
                .args(args)
                .output()?,
            args[0],
        )
    };
    run(&["read-tree", &previous.commit])?;
    run(&["add", "-A", "--", "."])?;
    let tree = run(&["write-tree"])?;
    let stable = git_command(&view.path)
        .env("GIT_INDEX_FILE", &index)
        .args(["diff-files", "--quiet"])
        .status()?;
    ensure!(
        stable.success(),
        "files changed during checkpoint; finish the edit and retry"
    );
    if tree == previous.tree {
        return Ok(previous);
    }
    let commit = commit_tree(&view.path, &tree, &[&previous.commit], &view.owner, message)?;
    let target = repository(store, &view.project);
    checked(
        git_command(&target)
            .args(["-c", "protocol.file.allow=always", "fetch", "--quiet"])
            .arg(&view.path)
            .arg(&commit)
            .output()?,
        "import checkpoint",
    )?;
    let revision = Revision {
        id: identity(&json!({"project":view.project,"commit":commit}))?,
        project: view.project.clone(),
        commit,
        tree,
        parents: vec![view.tip.clone()],
        owner: view.owner.clone(),
        message: message.into(),
        created: now(),
    };
    publish_revision(store, revision, Some(view))
}

pub fn compose(
    store: &mut Store,
    base: &str,
    changes: &[String],
    owner: &str,
    message: &str,
) -> Result<Revision> {
    ensure!(!changes.is_empty(), "composition needs at least one change");
    let base_row = store.revision(base)?;
    let repo = repository(store, &base_row.project);
    let mut current = base_row.commit.clone();
    let mut parents = vec![base.to_owned()];
    for change in changes {
        let row = store.revision(change)?;
        ensure!(
            row.project == base_row.project,
            "composition requires a single project"
        );
        let output = git_command(&repo)
            .args(["merge-tree", "--write-tree", &current, &row.commit])
            .output()?;
        if !output.status.success() {
            let repair = unique();
            let detail = json!({"base":base, "changes":changes,
                "conflicts":String::from_utf8_lossy(&output.stdout).chars().take(16000).collect::<String>()});
            store.db.execute(
                "INSERT INTO repairs VALUES (?,?,NULL,?,'open',?,NULL,?)",
                params![repair, base, "source-conflict", detail.to_string(), now()],
            )?;
            event(&store.db, "composition.conflict", &repair, &detail)?;
            return Err(CompositionConflict { repair }.into());
        }
        let output = String::from_utf8(output.stdout)?;
        let tree = output.lines().next().context("merge produced no tree")?;
        current = commit_tree(&repo, tree, &[&current, &row.commit], owner, message)?;
        parents.push(change.clone());
    }
    record_revision(store, &base_row.project, &current, parents, owner, message)
}

#[derive(Debug)]
pub struct CompositionConflict {
    pub repair: String,
}
impl std::fmt::Display for CompositionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "composition conflicts; repair {}; author workspaces remain intact",
            self.repair
        )
    }
}
impl std::error::Error for CompositionConflict {}

/// Export to an explicitly named local bare repository. Remote publication stays outside this core.
pub fn export(
    store: &Store,
    revision: &str,
    destination: &Path,
    reference: &str,
    expected: Option<&str>,
) -> Result<serde_json::Value> {
    ensure!(
        reference.starts_with("refs/heads/"),
        "export needs a refs/heads/ destination"
    );
    let destination = destination.canonicalize()?;
    ensure!(
        git(&destination, &["rev-parse", "--is-bare-repository"])? == "true",
        "local export destination must be bare"
    );
    let row = store.revision(revision)?;
    let repo = repository(store, &row.project);
    checked(
        git_command(&destination)
            .args(["-c", "protocol.file.allow=always", "fetch", "--quiet"])
            .arg(repo)
            .arg(&row.commit)
            .output()?,
        "export objects",
    )?;
    let zero = "0".repeat(row.commit.len());
    git(
        &destination,
        &[
            "update-ref",
            reference,
            &row.commit,
            expected.unwrap_or(&zero),
        ],
    )?;
    let receipt = json!({"revision":revision, "commit":row.commit, "destination":destination,
                         "reference":reference, "expected":expected, "time":now()});
    event(&store.db, "revision.exported", revision, &receipt)?;
    Ok(receipt)
}
