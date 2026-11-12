//! Versioned, authenticated application boundary shared by HTTP and MCP.
//! Local administrative CLI commands retain their existing Unix-account authority.
use crate::{
    authority::{self, AuthorityError, LOCAL, Outputs, Scope},
    engine, hash, identity,
    model::*,
    now, source,
    store::{Store, event},
    unique,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS api_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS api_principals (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS api_credentials (digest TEXT PRIMARY KEY, principal TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS api_receipts (
 id TEXT PRIMARY KEY, principal TEXT NOT NULL, request_key TEXT NOT NULL,
 request_hash TEXT NOT NULL, command TEXT NOT NULL, status INTEGER NOT NULL,
 response TEXT NOT NULL, created REAL NOT NULL, UNIQUE(principal,request_key));
";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub id: String,
    pub projects: BTreeSet<String>,
    pub profiles: BTreeSet<String>,
    pub roles: BTreeSet<String>,
    #[serde(default = "default_pending")]
    pub max_pending: usize,
}
fn default_pending() -> usize {
    1024
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Request {
        revision: String,
        profile: String,
        job: String,
    },
    Submit {
        revision: String,
        profile: String,
        #[serde(default)]
        requires: Vec<String>,
        /// Retain passed obligations of a same-revision candidate whose job identity is
        /// unchanged. Retained passes are earlier observations, not fresh tests.
        // Skipped when absent: receipts hash the re-serialized command.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retain_from: Option<String>,
    },
    Cancel {
        action: String,
    },
    Retry {
        candidate: String,
        #[serde(default)]
        jobs: Vec<String>,
    },
    Review {
        revision: String,
    },
    Resolve {
        repair: String,
        candidate: String,
    },
    Promote {
        candidate: String,
        #[serde(default)]
        expected: Option<String>,
    },
    Compose {
        base: String,
        changes: Vec<String>,
        message: String,
    },
    Publish {
        project: String,
        sha256: String,
        commit: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Projects {
        #[serde(default)]
        after: i64,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    Profiles,
    Revisions {
        project: String,
        #[serde(default)]
        after: i64,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    Actions {
        project: String,
        #[serde(default)]
        after: i64,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    Action {
        action: String,
    },
    Candidate {
        candidate: String,
    },
    Repairs {
        project: String,
        #[serde(default)]
        after: i64,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    Repair {
        repair: String,
    },
    Logs {
        attempt: String,
        #[serde(default)]
        offset: u64,
        #[serde(default = "default_log_limit")]
        limit: usize,
    },
    Artifacts {
        action: String,
    },
    Events {
        #[serde(default)]
        after: i64,
        #[serde(default)]
        project: Option<String>,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    Operation {
        id: String,
    },
    Metrics,
    Frontier {
        project: String,
    },
}
fn default_limit() -> usize {
    100
}
fn default_log_limit() -> usize {
    65536
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    #[serde(skip)]
    pub status: u16,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for ApiError {}
impl ApiError {
    pub fn new(status: u16, code: &str, message: &str) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            detail: None,
        }
    }
    pub(crate) fn internal(error: impl std::fmt::Display) -> Self {
        eprintln!("Patchweft API internal error: {error}");
        Self::new(500, "internal", "The operation could not be completed.")
    }
    fn forbidden() -> Self {
        Self::new(
            403,
            "forbidden",
            "Permission does not cover this operation.",
        )
    }
    pub fn body(&self) -> Value {
        json!({"api_version":"1","error":self})
    }
}
type ApiResult<T> = std::result::Result<T, ApiError>;

impl From<AuthorityError> for ApiError {
    fn from(error: AuthorityError) -> Self {
        match error {
            AuthorityError::Invalid(message) => Self::new(400, "invalid", &message),
            AuthorityError::Forbidden => Self::forbidden(),
            AuthorityError::Unregistered => Self::new(
                403,
                "executor_unregistered",
                "No executor is registered for this principal.",
            ),
            AuthorityError::Fenced => Self::new(
                409,
                "attempt_fenced",
                "This executor no longer holds the attempt under this lease.",
            ),
            AuthorityError::CompletionConflict => Self::new(
                409,
                "completion_conflict",
                "The attempt was already completed by a different report.",
            ),
            AuthorityError::MissingBlobs(missing) => Self {
                detail: Some(json!({"missing":missing})),
                ..Self::new(
                    422,
                    "missing_blob",
                    "Upload the missing blobs, then report again.",
                )
            },
            AuthorityError::InvalidEvidence(message) => {
                Self::new(422, "invalid_evidence", &message)
            }
            AuthorityError::Internal(error) => Self::internal(format!("{error:#}")),
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    pub commands: AtomicU64,
    pub replays: AtomicU64,
    pub queries: AtomicU64,
    pub failures: AtomicU64,
    pub gate_wait_us: AtomicU64,
    pub transaction_wait_us: AtomicU64,
    pub execution_us: AtomicU64,
    pub commit_us: AtomicU64,
}
impl Metrics {
    fn value(&self) -> Value {
        let read = |v: &AtomicU64| v.load(Ordering::Relaxed);
        json!({"commands":read(&self.commands),"replays":read(&self.replays),
            "queries":read(&self.queries),"failures":read(&self.failures),
            "gate_wait_us":read(&self.gate_wait_us),
            "transaction_wait_us":read(&self.transaction_wait_us),
            "execution_us":read(&self.execution_us),"commit_us":read(&self.commit_us)})
    }
}

pub struct Api {
    pub root: PathBuf,
    writer: Mutex<Store>,
    pub metrics: Metrics,
    /// When this process opened the store: every lease gets one full term after it.
    pub started: f64,
}

pub fn prepare(store: &Store) -> Result<()> {
    store.db.execute_batch(SCHEMA)?;
    crate::authority::prepare(&store.db)?;
    crate::retention::prepare(&store.db)?;
    store.db.execute(
        "INSERT OR IGNORE INTO api_settings VALUES ('version','1')",
        [],
    )?;
    let version: String = store.db.query_row(
        "SELECT value FROM api_settings WHERE key='version'",
        [],
        |r| r.get(0),
    )?;
    ensure!(version == "1", "unsupported API schema version");
    fs::create_dir_all(store.root.join("uploads"))?;
    Ok(())
}

fn validate_policy(store: &Store, principal: &Principal) -> Result<()> {
    ensure!(
        !principal.id.is_empty() && principal.id.len() <= 128,
        "invalid principal name"
    );
    ensure!(
        !principal.projects.is_empty()
            && principal.max_pending > 0
            && principal.max_pending <= 100_000,
        "invalid project scope or queue limit"
    );
    ensure!(
        !principal.roles.is_empty()
            && principal.roles.iter().all(|r| {
                ["read", "write", "review", "promote", "operator", "execute"].contains(&r.as_str())
            }),
        "invalid roles"
    );
    if principal.roles.contains("execute") {
        // An executor fetches the sources and artifacts its claims name (A6).
        ensure!(
            principal.roles.contains("read"),
            "the execute role requires the read role"
        );
        ensure!(
            principal.id != LOCAL,
            "`local` is the store's own executor; choose another principal name"
        );
    }
    for project in &principal.projects {
        ensure!(
            store
                .db
                .query_row("SELECT COUNT(*) FROM projects WHERE id=?", [project], |r| r
                    .get::<_, i64>(0))?
                == 1,
            "unknown project"
        );
    }
    for profile in &principal.profiles {
        store.profile(profile)?;
    }
    Ok(())
}

pub fn grant(store: &Store, principal: Principal, token_file: &Path) -> Result<Value> {
    prepare(store)?;
    validate_policy(store, &principal)?;
    let token = format!("patchweft_{}{}", unique(), unique());
    // Never print credential bytes. The operator gives this private file to a client.
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(token_file)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    store.db.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<()> {
        // A re-grant replaces a policy: the audit event keeps the one it replaced.
        let before: Option<Value> = store
            .db
            .query_row(
                "SELECT body FROM api_principals WHERE id=?",
                [&principal.id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(|body| serde_json::from_str(&body))
            .transpose()?;
        store.db.execute("INSERT INTO api_principals VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            params![principal.id, serde_json::to_string(&principal)?])?;
        // Re-granting a principal rotates credentials and replaces its scope.
        store.db.execute(
            "DELETE FROM api_credentials WHERE principal=?",
            [&principal.id],
        )?;
        store.db.execute(
            "INSERT INTO api_credentials VALUES (?,?)",
            params![hash(token.as_bytes()), principal.id],
        )?;
        event(
            &store.db,
            "principal.granted",
            &principal.id,
            &json!({"before":before,"after":principal,"rotated":true}),
        )?;
        store.db.execute_batch("COMMIT")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = store.db.execute_batch("ROLLBACK");
        let _ = fs::remove_file(token_file);
    }
    result?;
    Ok(json!({"principal":principal.id,"credential_file":token_file,"rotated":true}))
}

/// Replace an existing principal's scope; its issued credentials keep working. A policy without
/// `max_pending` keeps the current limit. Read, validation, write and the `{before, after}` audit
/// event are one immediate transaction, so every overwrite of a concurrent edit is visible via
/// before/after.
pub fn update_policy(store: &Store, policy: Value) -> Result<Value> {
    prepare(store)?;
    store.db.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<Value> {
        let id = policy["id"].as_str().context("policy lacks an id")?;
        let before: Option<String> = store
            .db
            .query_row("SELECT body FROM api_principals WHERE id=?", [id], |r| {
                r.get(0)
            })
            .optional()?;
        let before: Value = serde_json::from_str(
            &before.context("unknown principal; grant issues its first credential")?,
        )?;
        let mut policy = policy.clone();
        if policy.get("max_pending").is_none() {
            policy["max_pending"] = before["max_pending"].clone();
        }
        let principal: Principal = serde_json::from_value(policy)?;
        validate_policy(store, &principal)?;
        let after = serde_json::to_value(&principal)?;
        store.db.execute(
            "UPDATE api_principals SET body=? WHERE id=?",
            params![after.to_string(), principal.id],
        )?;
        event(
            &store.db,
            "principal.policy",
            &principal.id,
            &json!({"before":before,"after":after}),
        )?;
        store.db.execute_batch("COMMIT")?;
        Ok(json!({"principal":principal.id,"rotated":false}))
    })();
    if result.is_err() {
        let _ = store.db.execute_batch("ROLLBACK");
    }
    result
}

pub fn revoke(store: &Store, principal: &str) -> Result<Value> {
    prepare(store)?;
    store.db.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<usize> {
        let count = store
            .db
            .execute("DELETE FROM api_credentials WHERE principal=?", [principal])?;
        event(
            &store.db,
            "principal.revoked",
            principal,
            &json!({"credentials_revoked":count}),
        )?;
        store.db.execute_batch("COMMIT")?;
        Ok(count)
    })();
    if result.is_err() {
        let _ = store.db.execute_batch("ROLLBACK");
    }
    Ok(json!({"principal":principal,"credentials_revoked":result?}))
}

fn auth(store: &Store, token: &str) -> ApiResult<Principal> {
    if token.len() < 16 || token.len() > 256 {
        return Err(ApiError::new(
            401,
            "unauthorized",
            "A valid scoped credential is required.",
        ));
    }
    let body: Option<String> = store.db.query_row(
        "SELECT p.body FROM api_credentials c JOIN api_principals p ON p.id=c.principal WHERE c.digest=?",
        [hash(token.as_bytes())], |r| r.get(0)).optional().map_err(ApiError::internal)?;
    body.map(|b| serde_json::from_str(&b).map_err(ApiError::internal))
        .unwrap_or_else(|| {
            Err(ApiError::new(
                401,
                "unauthorized",
                "A valid scoped credential is required.",
            ))
        })
}
fn role(principal: &Principal, name: &str) -> ApiResult<()> {
    if principal.roles.contains(name) {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
}
/// Authors upload publication bundles; executors upload their attempts' logs and artifacts.
fn uploader(principal: &Principal) -> ApiResult<()> {
    role(principal, "write").or_else(|_| role(principal, "execute"))
}
/// An executor principal is the executor of the same id, limited to its current grant (A6).
fn executor(store: &Store, token: &str) -> ApiResult<(Principal, Scope)> {
    let principal = auth(store, token)?;
    role(&principal, "execute")?;
    if principal.id == LOCAL {
        return Err(ApiError::forbidden());
    }
    if authority::load(&store.db, &principal.id)
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(AuthorityError::Unregistered.into());
    }
    let scope = Scope {
        projects: Some(principal.projects.clone()),
        profiles: Some(principal.profiles.clone()),
    };
    Ok((principal, scope))
}
fn project(principal: &Principal, id: &str) -> ApiResult<()> {
    if principal.projects.contains(id) {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
}
fn revision_project(store: &Store, principal: &Principal, revision: &str) -> ApiResult<String> {
    let row = store
        .revision(revision)
        .map_err(|_| ApiError::forbidden())?;
    project(principal, &row.project)?;
    Ok(row.project)
}
fn profile(principal: &Principal, id: &str) -> ApiResult<()> {
    if principal.profiles.contains(id) {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
}
fn action_project(store: &Store, principal: &Principal, action: &str) -> ApiResult<String> {
    let row = store.action(action).map_err(|_| ApiError::forbidden())?;
    revision_project(store, principal, &row.revision)
}
fn candidate_projects(
    store: &Store,
    principal: &Principal,
    id: &str,
    visited: &mut BTreeSet<String>,
) -> ApiResult<String> {
    let row = store.candidate(id).map_err(|_| ApiError::forbidden())?;
    let result = revision_project(store, principal, &row.revision)?;
    if visited.insert(id.into()) {
        for dependency in &row.prerequisites {
            candidate_projects(store, principal, dependency, visited)?;
        }
    }
    Ok(result)
}
fn authorize(store: &Store, principal: &Principal, command: &Command) -> ApiResult<String> {
    match command {
        Command::Review { revision } => {
            role(principal, "review")?;
            revision_project(store, principal, revision)
        }
        Command::Resolve { repair, candidate } => {
            role(principal, "review")?;
            let revision: String = store
                .db
                .query_row("SELECT revision FROM repairs WHERE id=?", [repair], |r| {
                    r.get(0)
                })
                .map_err(|_| ApiError::forbidden())?;
            let p = revision_project(store, principal, &revision)?;
            if candidate_projects(store, principal, candidate, &mut BTreeSet::new())? != p {
                return Err(ApiError::forbidden());
            }
            Ok(p)
        }
        Command::Promote {
            candidate,
            expected,
        } => {
            role(principal, "promote")?;
            let p = candidate_projects(store, principal, candidate, &mut BTreeSet::new())?;
            if let Some(id) = expected {
                candidate_projects(store, principal, id, &mut BTreeSet::new())?;
            }
            Ok(p)
        }
        other => {
            role(principal, "write")?;
            match other {
                Command::Request {
                    revision,
                    profile: id,
                    ..
                }
                | Command::Submit {
                    revision,
                    profile: id,
                    ..
                } => {
                    profile(principal, id)?;
                    let p = revision_project(store, principal, revision)?;
                    if let Command::Submit {
                        requires,
                        retain_from,
                        ..
                    } = other
                    {
                        if requires.len() > 64 {
                            return Err(ApiError::new(400, "invalid", "Too many prerequisites."));
                        }
                        for id in requires {
                            candidate_projects(store, principal, id, &mut BTreeSet::new())?;
                        }
                        if let Some(source) = retain_from {
                            if candidate_projects(store, principal, source, &mut BTreeSet::new())?
                                != p
                            {
                                return Err(ApiError::forbidden());
                            }
                            // Retaining consumes evidence produced under the source's profile.
                            let row = store.candidate(source).map_err(ApiError::internal)?;
                            profile(principal, &row.profile)?;
                        }
                    }
                    Ok(p)
                }
                Command::Cancel { action } => action_project(store, principal, action),
                Command::Retry { candidate, .. } => {
                    let p = candidate_projects(store, principal, candidate, &mut BTreeSet::new())?;
                    let c = store.candidate(candidate).map_err(ApiError::internal)?;
                    profile(principal, &c.profile)?;
                    let associated: bool = store
                        .db
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM interests WHERE context=? AND owner=?)",
                            params![candidate, owner(principal)],
                            |r| r.get(0),
                        )
                        .map_err(ApiError::internal)?;
                    if !associated {
                        return Err(ApiError::forbidden());
                    }
                    Ok(p)
                }
                Command::Compose {
                    base,
                    changes,
                    message,
                } => {
                    if changes.is_empty() || changes.len() > 64 || message.len() > 4096 {
                        return Err(ApiError::new(400, "invalid", "Invalid composition size."));
                    }
                    let p = revision_project(store, principal, base)?;
                    for id in changes {
                        if revision_project(store, principal, id)? != p {
                            return Err(ApiError::forbidden());
                        }
                    }
                    Ok(p)
                }
                Command::Publish {
                    project: p,
                    sha256,
                    commit,
                    message,
                } => {
                    project(principal, p)?;
                    if !digest(sha256, 64)
                        || !(digest(commit, 40) || digest(commit, 64))
                        || message.len() > 4096
                    {
                        return Err(ApiError::new(400, "invalid", "Invalid source identity."));
                    }
                    Ok(p.clone())
                }
                _ => unreachable!(),
            }
        }
    }
}
fn owner(principal: &Principal) -> String {
    format!("api:{}", principal.id)
}
/// Idempotency and claim keys.
pub fn visible_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 128 && key.bytes().all(|c| c.is_ascii_graphic())
}
pub fn digest(text: &str, length: usize) -> bool {
    text.len() == length
        && text
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

impl Api {
    pub fn open(root: &Path) -> Result<Self> {
        let store = Store::open(root)?;
        prepare(&store)?;
        Ok(Self {
            root: store.root.clone(),
            writer: Mutex::new(store),
            metrics: Metrics::default(),
            started: now(),
        })
    }
    pub fn authenticate(&self, token: &str) -> ApiResult<Principal> {
        auth(&Store::open(&self.root).map_err(ApiError::internal)?, token)
    }

    pub fn command(&self, token: &str, key: &str, command: &Command) -> ApiResult<Value> {
        if !visible_key(key) {
            return Err(ApiError::new(
                400,
                "invalid_key",
                "A 1–128 byte visible-ASCII idempotency key is required.",
            ));
        }
        self.metrics.commands.fetch_add(1, Ordering::Relaxed);
        let wait = Instant::now();
        let mut store = self
            .writer
            .lock()
            .map_err(|_| ApiError::internal("poisoned writer"))?;
        self.metrics
            .gate_wait_us
            .fetch_add(wait.elapsed().as_micros() as u64, Ordering::Relaxed);
        let wait = Instant::now();
        store
            .db
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(ApiError::internal)?;
        self.metrics
            .transaction_wait_us
            .fetch_add(wait.elapsed().as_micros() as u64, Ordering::Relaxed);
        let result = self.command_transaction(&mut store, token, key, command);
        match result {
            Ok(value) => {
                let started = Instant::now();
                if let Err(error) = store.db.execute_batch("COMMIT") {
                    let _ = store.db.execute_batch("ROLLBACK");
                    return Err(ApiError::internal(error));
                }
                self.metrics
                    .commit_us
                    .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                Ok(value)
            }
            Err(error) => {
                let _ = store.db.execute_batch("ROLLBACK");
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    fn command_transaction(
        &self,
        store: &mut Store,
        token: &str,
        key: &str,
        command: &Command,
    ) -> ApiResult<Value> {
        let principal = auth(store, token)?;
        let p = authorize(store, &principal, command)?;
        let serialized = serde_json::to_string(command).map_err(ApiError::internal)?;
        let request_hash = hash(serialized.as_bytes());
        let previous: Option<(String,String)> = store.db.query_row(
            "SELECT request_hash,response FROM api_receipts WHERE principal=? AND request_key=?",
            params![principal.id,key], |r| Ok((r.get(0)?,r.get(1)?))).optional().map_err(ApiError::internal)?;
        if let Some((previous_hash, response)) = previous {
            if previous_hash != request_hash {
                return Err(ApiError::new(
                    409,
                    "idempotency_conflict",
                    "This key is already bound to a different command.",
                ));
            }
            let mut value: Value = serde_json::from_str(&response).map_err(ApiError::internal)?;
            value["replayed"] = json!(true);
            self.metrics.replays.fetch_add(1, Ordering::Relaxed);
            return Ok(value);
        }
        let operation =
            identity(&json!({"principal":principal.id,"key":key})).map_err(ApiError::internal)?;
        store
            .db
            .execute_batch("SAVEPOINT api_effect")
            .map_err(ApiError::internal)?;
        let started = Instant::now();
        let effect = apply(store, &principal, command);
        self.metrics
            .execution_us
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        let (status, payload) = match effect {
            Ok(value) => {
                if matches!(
                    command,
                    Command::Request { .. } | Command::Submit { .. } | Command::Retry { .. }
                ) {
                    // Count actual pending work after coalescing, inside the rollback boundary.
                    let queued:usize=store.db.query_row(
                        "SELECT COUNT(DISTINCT a.id) FROM actions a JOIN interests i ON i.action=a.id
                         WHERE i.owner=? AND i.active=1 AND a.state IN ('queued','running')",
                        [owner(&principal)],|r|r.get(0)).map_err(ApiError::internal)?;
                    let project_queued:usize=store.db.query_row(
                        "SELECT COUNT(*) FROM actions a JOIN revisions r ON r.id=json_extract(a.body,'$.revision')
                         WHERE r.project=? AND a.state IN ('queued','running')",[&p],|r|r.get(0)).map_err(ApiError::internal)?;
                    if queued > principal.max_pending || project_queued > 10_000 {
                        return Err(ApiError::new(
                            429,
                            "queue_limit",
                            "Pending-work budget is full; retry after work completes.",
                        ));
                    }
                }
                store
                    .db
                    .execute_batch("RELEASE api_effect")
                    .map_err(ApiError::internal)?;
                (200, json!({"result":value}))
            }
            Err(error) => {
                store
                    .db
                    .execute_batch("ROLLBACK TO api_effect; RELEASE api_effect")
                    .map_err(ApiError::internal)?;
                // Domain errors are durable outcomes, but do not expose host paths or stderr.
                eprintln!("Patchweft command {operation} refused: {error:#}");
                (
                    409,
                    json!({"error":{"code":"command_rejected","message":"The command's preconditions were not met. Inspect its source, profile and current state."}}),
                )
            }
        };
        let mut response =
            json!({"api_version":"1","operation_id":operation,"replayed":false,"status":status});
        response
            .as_object_mut()
            .expect("object")
            .extend(payload.as_object().expect("object").clone());
        store
            .db
            .execute(
                "INSERT INTO api_receipts VALUES (?,?,?,?,?,?,?,?)",
                params![
                    operation,
                    principal.id,
                    key,
                    request_hash,
                    serialized,
                    status,
                    response.to_string(),
                    now()
                ],
            )
            .map_err(ApiError::internal)?;
        Ok(response)
    }

    pub fn query(&self, token: &str, query: &Query) -> ApiResult<Value> {
        self.metrics.queries.fetch_add(1, Ordering::Relaxed);
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let principal = auth(&store, token)?;
        role(&principal, "read")?;
        query_inner(&store, &principal, query, &self.metrics)
    }
}

fn apply(store: &mut Store, principal: &Principal, command: &Command) -> Result<Value> {
    let owner = owner(principal);
    Ok(match command {
        Command::Request {
            revision,
            profile,
            job,
        } => serde_json::to_value(engine::request(store, revision, profile, job, &owner)?)?,
        Command::Submit {
            revision,
            profile,
            requires,
            retain_from,
        } => serde_json::to_value(engine::submit_retaining(
            store,
            revision,
            profile,
            &owner,
            requires.clone(),
            retain_from.as_deref(),
        )?)?,
        Command::Cancel { action } => engine::cancel(store, action, &owner)?,
        Command::Retry { candidate, jobs } => {
            engine::retry_owned(store, candidate, jobs, Some(&owner))?
        }
        Command::Resolve { repair, candidate } => engine::resolve_repair(store, repair, candidate)?,
        Command::Review { revision } => engine::review(store, revision, &owner)?,
        Command::Promote {
            candidate,
            expected,
        } => engine::promote(store, candidate, expected.as_deref())?,
        Command::Compose {
            base,
            changes,
            message,
        } => match source::compose(store, base, changes, &owner, message) {
            Ok(revision) => serde_json::to_value(revision)?,
            Err(error) => {
                if let Some(conflict) = error.downcast_ref::<source::CompositionConflict>() {
                    json!({"state":"conflict","repair":conflict.repair,"base":base})
                } else {
                    return Err(error);
                }
            }
        },
        Command::Publish {
            project,
            sha256,
            commit,
            message,
        } => {
            let bundle = upload_path(store, &principal.id, sha256);
            ensure!(
                crate::file_hash(&bundle)? == *sha256,
                "uploaded bundle digest changed"
            );
            let repo = source::repository(store, project);
            source::checked(
                source::git_command(&repo)
                    .args(["bundle", "verify"])
                    .arg(&bundle)
                    .output()?,
                "verify bundle",
            )?;
            let heads = source::checked(
                source::git_command(&repo)
                    .args(["bundle", "list-heads"])
                    .arg(&bundle)
                    .output()?,
                "bundle heads",
            )?;
            ensure!(
                heads
                    .lines()
                    .any(|line| line.split_whitespace().next() == Some(commit)),
                "requested commit is not a bundle head"
            );
            source::checked(
                source::git_command(&repo)
                    .args([
                        "-c",
                        "protocol.file.allow=always",
                        "-c",
                        "fetch.fsckObjects=true",
                        "fetch",
                        "--quiet",
                        "--no-tags",
                    ])
                    .arg(&bundle)
                    .arg(format!("{commit}:refs/patchweft/imports/{commit}"))
                    .output()?,
                "fetch bundle",
            )?;
            serde_json::to_value(source::record_revision(
                store,
                project,
                commit,
                vec![],
                &owner,
                message,
            )?)?
        }
    })
}

fn pagination(after: i64, limit: usize) -> ApiResult<()> {
    if after < 0 || !(1..=1000).contains(&limit) {
        Err(ApiError::new(
            400,
            "invalid_page",
            "Cursor must be nonnegative and limit between 1 and 1000.",
        ))
    } else {
        Ok(())
    }
}

fn query_inner(
    store: &Store,
    principal: &Principal,
    query: &Query,
    metrics: &Metrics,
) -> ApiResult<Value> {
    let result = match query {
        Query::Projects { after, limit } => {
            pagination(*after, *limit)?;
            let mut stmt = store
                .db
                .prepare(
                    "SELECT p.rowid,p.id,p.name,f.candidate FROM projects p
                LEFT JOIN frontiers f ON f.project=p.id WHERE p.rowid>? ORDER BY p.rowid LIMIT ?",
                )
                .map_err(ApiError::internal)?;
            let rows = stmt
                .query_map(params![after, limit], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                })
                .map_err(ApiError::internal)?;
            let mut items = vec![];
            let mut cursor = *after;
            let mut count = 0;
            for row in rows {
                let (seq, id, name, frontier) = row.map_err(ApiError::internal)?;
                cursor = seq;
                count += 1;
                if principal.projects.contains(&id) {
                    items.push(json!({"id":id,"name":name,"validated_candidate":frontier}));
                }
            }
            json!({"items":items,"next_after":cursor,"more":count==*limit})
        }
        Query::Profiles => {
            let mut values = vec![];
            for id in &principal.profiles {
                let p = store.profile(id).map_err(ApiError::internal)?;
                let recipes = engine::recipes(&p).map_err(ApiError::internal)?;
                values.push(json!({"id":id,"name":p.definition.name,"certifying":p.definition.certifying,
                    "jobs":p.definition.jobs.iter().map(|j| json!({"name":j.name,"dependencies":j.dependencies,
                        "slots":j.slots,"memory_mb":j.memory_mb,"reuse":j.reuse,
                        "timeout_seconds":j.timeout_seconds,"export_slots":j.export_slots,
                        "recipe":recipes[&j.name]})).collect::<Vec<_>>()}));
            }
            json!({"items":values})
        }
        Query::Revisions {
            project: p,
            after,
            limit,
        }
        | Query::Actions {
            project: p,
            after,
            limit,
        } => {
            project(principal, p)?;
            pagination(*after, *limit)?;
            let sql = if matches!(query, Query::Revisions { .. }) {
                "SELECT rowid,body FROM revisions WHERE project=? AND rowid>? ORDER BY rowid LIMIT ?"
            } else {
                "SELECT a.rowid,json_object('id',a.id,'state',a.state,'attempt',a.attempt,
                    'revision',r.id,'job',json_extract(a.body,'$.job'),'created',a.created)
                 FROM actions a JOIN revisions r ON r.id=json_extract(a.body,'$.revision')
                 WHERE r.project=? AND a.rowid>? ORDER BY a.rowid LIMIT ?"
            };
            let mut stmt = store.db.prepare(sql).map_err(ApiError::internal)?;
            let rows = stmt
                .query_map(params![p, after, limit], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(ApiError::internal)?;
            let mut items = vec![];
            let mut cursor = *after;
            for row in rows {
                let (seq, body) = row.map_err(ApiError::internal)?;
                cursor = seq;
                items.push(serde_json::from_str::<Value>(&body).map_err(ApiError::internal)?);
            }
            json!({"more":items.len()==*limit,"items":items,"next_after":cursor})
        }
        Query::Action { action } => {
            action_project(store, principal, action)?;
            let row = store.action(action).map_err(ApiError::internal)?;
            let (state, attempt): (String, Option<String>) = store
                .db
                .query_row(
                    "SELECT state,attempt FROM actions WHERE id=?",
                    [action],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(ApiError::internal)?;
            // Why the current attempt was stopped, or null: a lane reads `reclaimed` (lent capacity
            // taken back for prioritized work) apart from a lost executor (lumenloop-ng#4204).
            let stop: Option<String> = match &attempt {
                Some(attempt) => store
                    .db
                    .query_row(
                        "SELECT json_extract(body,'$.stop') FROM attempts WHERE id=?",
                        [attempt],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(ApiError::internal)?
                    .flatten(),
                None => None,
            };
            json!({"id":action,"revision":row.revision,"profile":row.profile,"job":row.job,
                "dependencies":row.dependencies,"state":state,"attempt":attempt,"stop":stop,
                "key":row.key,"identity":row.identity,"reservation":row.reservation,
                "origin":row.origin})
        }
        Query::Candidate { candidate } => {
            candidate_projects(store, principal, candidate, &mut BTreeSet::new())?;
            engine::candidate_status(store, candidate).map_err(ApiError::internal)?
        }
        Query::Repair { repair } => {
            let value = repair_value(store, repair).map_err(|_| ApiError::forbidden())?;
            revision_project(
                store,
                principal,
                value["revision"].as_str().expect("revision"),
            )?;
            value
        }
        Query::Repairs {
            project: p,
            after,
            limit,
        } => {
            project(principal, p)?;
            pagination(*after, *limit)?;
            let mut statement = store
                .db
                .prepare(
                    "SELECT x.rowid,x.id FROM repairs x JOIN revisions r ON r.id=x.revision
                WHERE r.project=? AND x.rowid>? ORDER BY x.rowid LIMIT ?",
                )
                .map_err(ApiError::internal)?;
            let rows = statement
                .query_map(params![p, after, limit], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(ApiError::internal)?;
            let mut items = vec![];
            let mut cursor = *after;
            for row in rows {
                let (seq, id) = row.map_err(ApiError::internal)?;
                cursor = seq;
                items.push(repair_value(store, &id).map_err(ApiError::internal)?);
            }
            json!({"more":items.len()==*limit,"items":items,"next_after":cursor})
        }
        Query::Logs {
            attempt,
            offset,
            limit,
        } => {
            let row = store.attempt(attempt).map_err(|_| ApiError::forbidden())?;
            action_project(store, principal, &row.action)?;
            if !(1..=65536).contains(limit) {
                return Err(ApiError::new(
                    400,
                    "invalid_page",
                    "Log limit must be 1–65536 bytes.",
                ));
            }
            if let Some(note) =
                crate::retention::deleted_note(&store.db, &row.id).map_err(ApiError::internal)?
            {
                return Err(ApiError::new(
                    410,
                    "log_deleted",
                    &format!("The log was {note}."),
                ));
            }
            let path = store.root.join("attempts").join(&row.id).join("output.log");
            let mut file = File::open(path).map_err(|_| {
                ApiError::new(404, "log_unavailable", "The log is not available yet.")
            })?;
            let total = file.metadata().map_err(ApiError::internal)?.len();
            if *offset > total {
                return Err(ApiError::new(
                    416,
                    "invalid_offset",
                    "Offset exceeds log length.",
                ));
            }
            file.seek(SeekFrom::Start(*offset))
                .map_err(ApiError::internal)?;
            let mut bytes = vec![0; *limit];
            let count = file.read(&mut bytes).map_err(ApiError::internal)?;
            bytes.truncate(count);
            use base64::Engine;
            json!({"attempt":attempt,"encoding":"base64","data":base64::engine::general_purpose::STANDARD.encode(bytes),
                "next_offset":offset+count as u64,"observed_bytes":total})
        }
        Query::Artifacts { action } => {
            action_project(store, principal, action)?;
            artifact_manifest(store, action)?
        }
        Query::Events {
            after,
            project: p,
            limit,
        } => events(store, principal, *after, p.as_deref(), *limit)?,
        Query::Operation { id } => {
            let row: Option<(String, String)> = store
                .db
                .query_row(
                    "SELECT command,response FROM api_receipts WHERE id=? AND principal=?",
                    params![id, principal.id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(ApiError::internal)?;
            let (command, response) = row.ok_or_else(ApiError::forbidden)?;
            let command = serde_json::from_str(&command).map_err(ApiError::internal)?;
            authorize(store, principal, &command)?;
            serde_json::from_str(&response).map_err(ApiError::internal)?
        }
        Query::Metrics => {
            role(principal, "operator")?;
            metrics.value()
        }
        Query::Frontier { project: p } => {
            project(principal, p)?;
            engine::frontier(store, p)
                .map_err(ApiError::internal)?
                .unwrap_or_else(|| json!({"candidate":null,"certificate":null}))
        }
    };
    Ok(result)
}

fn repair_value(store: &Store, id: &str) -> Result<Value> {
    let (revision, action, kind, state, detail, resolved): (
        String,
        Option<String>,
        String,
        String,
        String,
        Option<String>,
    ) = store.db.query_row(
        "SELECT revision,action,kind,state,detail,resolved_by FROM repairs WHERE id=?",
        [id],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        },
    )?;
    Ok(
        json!({"id":id,"revision":revision,"action":action,"kind":kind,"state":state,
        "detail":serde_json::from_str::<Value>(&detail)?,"resolved_by":resolved}),
    )
}

fn event_project(store: &Store, row: &Value) -> Result<Option<String>> {
    let kind = row["kind"].as_str().context("event kind")?;
    let subject = row["subject"].as_str().context("event subject")?;
    let revision = if kind.starts_with("revision.") {
        Some(subject.to_owned())
    } else if kind == "composition.conflict" || kind.starts_with("repair.") {
        Some(
            store
                .db
                .query_row("SELECT revision FROM repairs WHERE id=?", [subject], |r| {
                    r.get(0)
                })?,
        )
    } else if kind.starts_with("workspace.") {
        Some(store.workspace(subject)?.base)
    } else if kind.starts_with("action.") || kind == "interest.detached" {
        Some(store.action(subject)?.revision)
    } else if kind.starts_with("attempt.") {
        Some(store.action(&store.attempt(subject)?.action)?.revision)
    } else if kind.starts_with("candidate.") || kind == "frontier.advanced" {
        Some(store.candidate(subject)?.revision)
    } else {
        None
    };
    revision
        .map(|id| store.revision(&id).map(|r| r.project))
        .transpose()
}
fn events(
    store: &Store,
    principal: &Principal,
    after: i64,
    selected: Option<&str>,
    limit: usize,
) -> ApiResult<Value> {
    pagination(after, limit)?;
    if let Some(p) = selected {
        project(principal, p)?;
    }
    let highest: i64 = store
        .db
        .query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |r| r.get(0))
        .map_err(ApiError::internal)?;
    if after > highest {
        return Err(ApiError::new(
            409,
            "future_cursor",
            "Event cursor is ahead of this store.",
        ));
    }
    let mut page = store.events(after).map_err(ApiError::internal)?;
    page.truncate(limit);
    let next = page.last().and_then(|e| e["seq"].as_i64()).unwrap_or(after);
    let mut visible = vec![];
    for row in &page {
        if let Some(p) = event_project(store, row).map_err(ApiError::internal)?
            && principal.projects.contains(&p)
            && selected.is_none_or(|id| id == p)
        {
            visible.push(row.clone());
        }
    }
    Ok(json!({"events":visible,"next_after":next,"more":next<highest}))
}

fn upload_path(store: &Store, principal: &str, sha256: &str) -> PathBuf {
    store
        .root
        .join("uploads")
        .join(hash(principal.as_bytes()))
        .join(sha256)
}
pub const MAX_BUNDLE: usize = 1024 * 1024 * 1024;
pub const MAX_RETAINED_UPLOADS: u64 = 4 * MAX_BUNDLE as u64;
/// Per artifact or log file, so a result recorded on one host is deliverable from every host.
pub const MAX_ARTIFACT_BYTES: u64 = MAX_BUNDLE as u64;
/// Per attempt: artifacts and logs together.
pub const MAX_ATTEMPT_OUTPUT_BYTES: u64 = MAX_RETAINED_UPLOADS;
/// A result's artifact manifest, serialized; every host enforces it at capture, so a result that
/// passes locally is deliverable from a remote executor too (review P2-3).
pub const MAX_MANIFEST_BYTES: u64 = 12 * 1024 * 1024;
/// The one limit on JSON either side reads (gate P2-1): `executor/complete` requests, which carry
/// a manifest twice, and responses such as claims, which inline their dependencies' manifests.
pub const MAX_RESPONSE_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_COMPLETE_BODY: usize = MAX_RESPONSE_BYTES as usize;
/// Dependency manifests a claim may inline: the response budget less one manifest of headroom for
/// the profile, action and revision it also carries.
pub const MAX_CLAIM_MANIFESTS: u64 = MAX_RESPONSE_BYTES - MAX_MANIFEST_BYTES;
pub const TRANSFER_TIMEOUT_SECONDS: u64 = 600;
impl Api {
    pub fn begin_upload(&self, token: &str, sha256: &str) -> ApiResult<tempfile::NamedTempFile> {
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let principal = auth(&store, token)?;
        uploader(&principal)?;
        if !digest(sha256, 64) {
            return Err(ApiError::new(422, "digest_mismatch", "Invalid SHA-256."));
        }
        tempfile::Builder::new()
            .prefix(".upload-")
            .tempfile_in(&self.root)
            .map_err(ApiError::internal)
    }

    pub fn upload(&self, token: &str, sha256: &str, bytes: &[u8]) -> ApiResult<Value> {
        let mut temp = self.begin_upload(token, sha256)?;
        if bytes.len() > MAX_BUNDLE {
            return Err(ApiError::new(413, "too_large", "Bundle exceeds 1 GiB."));
        }
        temp.write_all(bytes).map_err(ApiError::internal)?;
        self.finish_upload(token, sha256, temp)
    }

    pub fn finish_upload(
        &self,
        token: &str,
        sha256: &str,
        temp: tempfile::NamedTempFile,
    ) -> ApiResult<Value> {
        let bytes = temp.as_file().metadata().map_err(ApiError::internal)?.len();
        if bytes > MAX_BUNDLE as u64 {
            return Err(ApiError::new(413, "too_large", "Bundle exceeds 1 GiB."));
        }
        if !digest(sha256, 64)
            || crate::file_hash(temp.path()).map_err(ApiError::internal)? != sha256
        {
            return Err(ApiError::new(
                422,
                "digest_mismatch",
                "Uploaded content does not match its SHA-256.",
            ));
        }
        // Recheck authorization after receiving and hashing the complete stream.
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let principal = auth(&store, token)?;
        uploader(&principal)?;
        let path = upload_path(&store, &principal.id, sha256);
        fs::create_dir_all(path.parent().expect("upload parent")).map_err(ApiError::internal)?;
        // Serialize quota accounting with publication and other uploads.
        let _writer = self
            .writer
            .lock()
            .map_err(|_| ApiError::internal("poisoned writer"))?;
        if !path.exists() {
            let used = fs::read_dir(path.parent().expect("parent"))
                .map_err(ApiError::internal)?
                .try_fold(0u64, |n, e| -> std::io::Result<u64> {
                    Ok(n + e?.metadata()?.len())
                })
                .map_err(ApiError::internal)?;
            if used + bytes > MAX_RETAINED_UPLOADS {
                return Err(ApiError::new(
                    429,
                    "upload_quota",
                    "This principal's retained upload quota is full.",
                ));
            }
            temp.as_file().sync_all().map_err(ApiError::internal)?;
            temp.persist_noclobber(&path).map_err(ApiError::internal)?;
        }
        Ok(json!({"sha256":sha256,"bytes":bytes}))
    }

    /// The revision's history, without what `haves` (commits the caller holds) already reach.
    pub fn source_bundle(
        &self,
        token: &str,
        revision: &str,
        haves: &[String],
    ) -> ApiResult<(tempfile::TempDir, PathBuf)> {
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let principal = auth(&store, token)?;
        role(&principal, "read")?;
        if haves.len() > MAX_HAVES || !haves.iter().all(|h| digest(h, 40) || digest(h, 64)) {
            return Err(ApiError::new(
                400,
                "invalid",
                "At most 32 haves, each a full lowercase commit ID.",
            ));
        }
        let p = revision_project(&store, &principal, revision)?;
        let repo = source::repository(&store, &p);
        let haves = commits(&repo, haves).map_err(ApiError::internal)?;
        let temp = tempfile::tempdir_in(&self.root).map_err(ApiError::internal)?;
        let path = temp.path().join("source.bundle");
        let create = |haves: &[String]| {
            let mut git = source::git_command(&repo);
            git.args(["bundle", "create"])
                .arg(&path)
                .arg(format!("refs/patchweft/revisions/{revision}"));
            if !haves.is_empty() {
                git.arg("--not").args(haves);
            }
            git.output()
        };
        let mut output = create(&haves).map_err(ApiError::internal)?;
        if !output.status.success() && !haves.is_empty() {
            // git refuses an empty bundle: the haves already reach the revision.
            let _ = fs::remove_file(&path);
            output = create(&[]).map_err(ApiError::internal)?;
        }
        source::checked(output, "create bundle").map_err(ApiError::internal)?;
        if path.metadata().map_err(ApiError::internal)?.len() > MAX_BUNDLE as u64 {
            return Err(ApiError::new(
                413,
                "too_large",
                "Source bundle exceeds this service's 1 GiB transfer limit.",
            ));
        }
        Ok((temp, path))
    }

    pub fn executor_hello(&self, token: &str, request: &HelloRequest) -> ApiResult<Hello> {
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let (principal, scope) = executor(&store, token)?;
        Ok(authority::hello(
            &store.db,
            &principal.id,
            &scope,
            &request.ids,
        )?)
    }

    pub fn executor_claim(&self, token: &str, request: &ClaimRequest) -> ApiResult<ClaimResponse> {
        if !visible_key(&request.claim_key) {
            return Err(ApiError::new(
                400,
                "invalid_key",
                "A 1–128 byte visible-ASCII claim key is required.",
            ));
        }
        let mut store = Store::open(&self.root).map_err(ApiError::internal)?;
        let (principal, scope) = executor(&store, token)?;
        let mut response = authority::claim_for(&mut store, &principal.id, &scope, request)?;
        // Never fails a claim: the list is sent again on a later one.
        match crate::retention::retire_list(&mut store, &principal.id) {
            Ok(retire) => response.retire = retire,
            Err(error) => eprintln!(
                "{}",
                json!({"event":"retention_retire_list_failed","executor":principal.id,
                       "error":format!("{error:#}")})
            ),
        }
        Ok(response)
    }

    pub fn executor_renew(&self, token: &str, request: &RenewRequest) -> ApiResult<RenewResponse> {
        let mut store = Store::open(&self.root).map_err(ApiError::internal)?;
        let (principal, scope) = executor(&store, token)?;
        Ok(authority::renew(
            &mut store,
            &principal.id,
            &scope,
            request,
        )?)
    }

    /// Stages the report's uploads, then lets the authority record it. A report of another
    /// holder's attempt, or a replay of the recorded one, never touches the uploads.
    pub fn executor_complete(
        &self,
        token: &str,
        request: &CompleteRequest,
    ) -> ApiResult<CompleteResponse> {
        let mut store = Store::open(&self.root).map_err(ApiError::internal)?;
        let (principal, scope) = executor(&store, token)?;
        let body: Option<String> = store
            .db
            .query_row(
                "SELECT body FROM attempts WHERE id=?",
                [&request.attempt],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let attempt = body
            .map(|body| serde_json::from_str::<Attempt>(&body))
            .transpose()
            .map_err(ApiError::internal)?
            .filter(|attempt| {
                attempt.executor.as_deref().unwrap_or(LOCAL) == principal.id
                    && attempt.lease.as_deref() == Some(request.lease.as_str())
            });
        let Some(attempt) = attempt else {
            return Err(AuthorityError::Fenced.into());
        };
        // A recorded completion never changes, so the authority answers it from the store.
        let (outputs, used) = if attempt.completion.is_some() {
            (Outputs::InPlace, vec![])
        } else {
            let (staging, used) = stage(&store, &principal.id, request)?;
            (Outputs::Staged(staging), used)
        };
        let result = authority::complete(&mut store, &principal.id, &scope, request, outputs);
        // Staged reports consume their uploads whatever the authority decided: the quota frees.
        for sha256 in used {
            let _ = fs::remove_file(upload_path(&store, &principal.id, &sha256));
        }
        Ok(result?)
    }

    pub fn artifact(&self, token: &str, action: &str, sha256: &str) -> ApiResult<File> {
        let store = Store::open(&self.root).map_err(ApiError::internal)?;
        let principal = auth(&store, token)?;
        role(&principal, "read")?;
        action_project(&store, &principal, action)?;
        if !digest(sha256, 64) {
            return Err(ApiError::new(400, "invalid", "Invalid SHA-256."));
        }
        let manifest = artifact_manifest(&store, action)?;
        let files = manifest["files"]
            .as_object()
            .ok_or_else(|| ApiError::internal("invalid manifest"))?;
        let name = files
            .iter()
            .find(|(_, v)| v["sha256"] == sha256)
            .map(|(k, _)| k)
            .ok_or_else(ApiError::forbidden)?;
        let path = store
            .root
            .join("attempts")
            .join(manifest["attempt"].as_str().expect("attempt"))
            .join("artifacts")
            .join(source::relative_path(name).map_err(ApiError::internal)?);
        let mut file = File::open(path).map_err(ApiError::internal)?;
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer).map_err(ApiError::internal)?;
            if n == 0 {
                break;
            }
            digest.update(&buffer[..n]);
        }
        if format!("{:x}", digest.finalize()) != sha256 {
            return Err(ApiError::new(
                409,
                "artifact_corrupt",
                "Retained artifact failed digest verification.",
            ));
        }
        file.seek(SeekFrom::Start(0)).map_err(ApiError::internal)?;
        Ok(file)
    }
}

pub const MAX_HAVES: usize = 32;

/// The haves that name commits of the repository; anything else is ignored.
fn commits(repo: &Path, haves: &[String]) -> Result<Vec<String>> {
    if haves.is_empty() {
        return Ok(vec![]);
    }
    let mut child = source::git_command(repo)
        .args(["cat-file", "--batch-check=%(objecttype)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    // At most 32 short lines: they fit the pipe before git answers.
    child
        .stdin
        .take()
        .context("git stdin")?
        .write_all(format!("{}\n", haves.join("\n")).as_bytes())?;
    let kinds = source::checked(child.wait_with_output()?, "check haves")?;
    Ok(haves
        .iter()
        .zip(kinds.lines())
        .filter(|(_, kind)| *kind == "commit")
        .map(|(have, _)| have.clone())
        .collect())
}

fn invalid_evidence(message: String) -> ApiError {
    AuthorityError::InvalidEvidence(message).into()
}

/// Moves a report's verified uploads out of the principal's upload area into
/// `.incoming-<unique>/{output.log,cleanup.log,artifacts/<path>,artifacts.json}`. Nothing moves
/// unless every blob is present with its declared length and digest.
fn stage(
    store: &Store,
    principal: &str,
    request: &CompleteRequest,
) -> ApiResult<(PathBuf, Vec<String>)> {
    let blobs: Vec<BlobRef> = request
        .artifacts
        .values()
        .map(|entry| BlobRef {
            sha256: entry.sha256.clone(),
            bytes: entry.bytes,
        })
        .collect();
    // Each staged file and the blob it is made from.
    let mut targets: Vec<(PathBuf, &BlobRef, Option<u32>)> = vec![];
    let logs = [
        ("output.log", &request.log),
        ("cleanup.log", &request.cleanup_log),
    ];
    for (name, blob) in logs {
        if let Some(blob) = blob {
            targets.push((PathBuf::from(name), blob, None));
        }
    }
    for ((path, entry), blob) in request.artifacts.iter().zip(&blobs) {
        let relative =
            source::relative_path(path).map_err(|e| invalid_evidence(format!("{e:#}")))?;
        if entry.mode > 0o777 {
            return Err(invalid_evidence(format!(
                "Artifact {path} has mode {:o}.",
                entry.mode
            )));
        }
        // A file cannot also be the directory of another artifact.
        if relative.ancestors().skip(1).any(|parent| {
            parent
                .to_str()
                .is_some_and(|p| request.artifacts.contains_key(p))
        }) {
            return Err(invalid_evidence(format!(
                "Artifact {path} lies inside another artifact."
            )));
        }
        targets.push((
            Path::new("artifacts").join(relative),
            blob,
            Some(entry.mode),
        ));
    }
    let mut lengths = BTreeMap::new();
    for (_, blob, _) in &targets {
        if !digest(&blob.sha256, 64) {
            return Err(ApiError::new(400, "invalid", "Invalid SHA-256."));
        }
        if *lengths.entry(blob.sha256.as_str()).or_insert(blob.bytes) != blob.bytes {
            return Err(invalid_evidence(format!(
                "Blob {} is reported with two lengths.",
                blob.sha256
            )));
        }
    }
    let mut missing = vec![];
    for (sha256, bytes) in &lengths {
        let upload = upload_path(store, principal, sha256);
        let actual = match crate::file_hash(&upload) {
            Ok(actual) => actual,
            Err(_) if !upload.exists() => {
                missing.push(sha256.to_string());
                continue;
            }
            Err(error) => return Err(ApiError::internal(error)),
        };
        if actual != *sha256 {
            // Damaged at rest: a fresh upload under the same name replaces it.
            let _ = fs::remove_file(&upload);
            missing.push(sha256.to_string());
        } else if fs::metadata(&upload).map_err(ApiError::internal)?.len() != *bytes {
            return Err(invalid_evidence(format!(
                "Blob {sha256} does not have {bytes} bytes."
            )));
        }
    }
    if !missing.is_empty() {
        return Err(AuthorityError::MissingBlobs(missing).into());
    }
    let staging = store.root.join(format!(".incoming-{}", unique()));
    fs::create_dir(&staging).map_err(ApiError::internal)?;
    let moved = (|| -> ApiResult<()> {
        let mut placed: BTreeMap<&str, PathBuf> = BTreeMap::new();
        for (relative, blob, mode) in &targets {
            let target = staging.join(relative);
            fs::create_dir_all(target.parent().expect("staged parent"))
                .map_err(ApiError::internal)?;
            if let Some(first) = placed.get(blob.sha256.as_str()) {
                // One digest named by several paths: each gets its own copy.
                fs::copy(first, &target).map_err(ApiError::internal)?;
            } else {
                // A link, not a move: a concurrent report of the same digest still finds the
                // upload; the names are consumed once this report is recorded (gate P3-4).
                match fs::hard_link(upload_path(store, principal, &blob.sha256), &target) {
                    Ok(()) => {}
                    // Consumed by a concurrent report since it was checked.
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        return Err(AuthorityError::MissingBlobs(vec![blob.sha256.clone()]).into());
                    }
                    Err(error) => return Err(ApiError::internal(error)),
                }
                placed.insert(&blob.sha256, target.clone());
            }
            if let Some(mode) = mode {
                fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o777 & !0o222))
                    .map_err(ApiError::internal)?;
            }
        }
        fs::write(
            staging.join("artifacts.json"),
            serde_json::to_vec_pretty(&request.artifacts).map_err(ApiError::internal)?,
        )
        .map_err(ApiError::internal)
    })();
    if let Err(error) = moved {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    Ok((
        staging,
        lengths.keys().map(|sha256| sha256.to_string()).collect(),
    ))
}

const UPLOAD_RETENTION: Duration = Duration::from_secs(24 * 3600);
const STAGING_RETENTION: Duration = Duration::from_secs(3600);

/// Removes uploads nobody consumed within a day and staging directories older than an hour
/// (MUST-FIX 3). `serve` runs this every ten minutes. Returns how many entries were removed.
pub fn purge(root: &Path, now: SystemTime) -> Result<usize> {
    let older = |path: &Path, age: Duration| {
        fs::symlink_metadata(path)
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|modified| now.duration_since(modified).is_ok_and(|d| d > age))
    };
    let mut removed = 0;
    let areas = match fs::read_dir(root.join("uploads")) {
        Ok(areas) => areas.collect::<std::io::Result<Vec<_>>>()?,
        Err(error) if error.kind() == ErrorKind::NotFound => vec![],
        Err(error) => return Err(error.into()),
    };
    for area in areas.into_iter().filter(|a| a.path().is_dir()) {
        for entry in fs::read_dir(area.path())? {
            let path = entry?.path();
            if older(&path, UPLOAD_RETENTION) && fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".incoming-")
            && older(&entry.path(), STAGING_RETENTION)
            && fs::remove_dir_all(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
    Ok(removed)
}

fn artifact_manifest(store: &Store, action: &str) -> ApiResult<Value> {
    if engine::action_state(&store.db, action).map_err(ApiError::internal)? != "passed" {
        return Err(ApiError::new(
            409,
            "artifacts_unavailable",
            "Artifacts are available only for a passing action. Inspect its status and logs.",
        ));
    }
    let attempt: String = store
        .db
        .query_row("SELECT attempt FROM actions WHERE id=?", [action], |r| {
            r.get(0)
        })
        .map_err(ApiError::internal)?;
    if let Some(note) =
        crate::retention::deleted_note(&store.db, &attempt).map_err(ApiError::internal)?
    {
        return Err(ApiError::new(
            410,
            "artifacts_deleted",
            &format!("The action's artifacts were {note}."),
        ));
    }
    let row = store.attempt(&attempt).map_err(ApiError::internal)?;
    let files = row
        .result
        .ok_or_else(|| ApiError::internal("missing result"))?["artifacts"]
        .clone();
    if !files.is_object() {
        return Err(ApiError::internal("missing artifact manifest"));
    }
    Ok(
        json!({"action":action,"revision":store.action(action).map_err(ApiError::internal)?.revision,"attempt":attempt,"files":files}),
    )
}
