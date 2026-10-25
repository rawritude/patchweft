use crate::{
    api, engine, file_hash,
    model::*,
    source,
    store::{Store, event},
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::params;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{ErrorKind, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop_signal(_: i32) {
    STOP.store(true, Ordering::SeqCst);
}

pub fn process_start(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, fields) = text.rsplit_once(')')?;
    let values: Vec<_> = fields.split_whitespace().collect();
    if values.first() == Some(&"Z") {
        return None;
    }
    values.get(19).map(|s| s.to_string())
}

pub fn alive(pid: u32, start: &str) -> bool {
    process_start(pid).as_deref() == Some(start)
}

fn descendants(parent: u32) -> Vec<u32> {
    descendants_of(&[parent])
}

/// Every live process descending from any of `roots`, the roots themselves excluded.
pub(crate) fn descendants_of(roots: &[u32]) -> Vec<u32> {
    let mut relationships = Vec::new();
    if let Ok(entries) = fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, fields)) = stat.rsplit_once(')') else {
                continue;
            };
            let Some(ppid) = fields
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u32>().ok())
            else {
                continue;
            };
            relationships.push((pid, ppid));
        }
    }
    let mut found = roots.to_vec();
    loop {
        let old = found.len();
        for (pid, ppid) in &relationships {
            if found.contains(ppid) && !found.contains(pid) {
                found.push(*pid);
            }
        }
        if found.len() == old {
            break;
        }
    }
    found
        .into_iter()
        .filter(|pid| !roots.contains(pid))
        .collect()
}

fn signal_tree(signal: i32) {
    // Restricted to descendants of this dedicated supervisor, including adopted setsid children.
    for pid in descendants(std::process::id()).into_iter().rev() {
        unsafe {
            libc::kill(pid as i32, signal);
        }
    }
}

fn reap_adopted(main: u32) {
    for pid in descendants(std::process::id()) {
        if pid != main {
            unsafe {
                libc::waitpid(pid as i32, std::ptr::null_mut(), libc::WNOHANG);
            }
        }
    }
}

/// Polls the job's command until it has exited. On the exit, records whether the command ended on
/// its own: before this supervisor signalled anything (`signaled`), and before a SIGTERM reached
/// the supervisor. That flag is read just after the reap, so a group-wide SIGTERM that also ended
/// the command is already set; one that came later, as an executor deploy sends, leaves the
/// command's own exit standing.
fn reap_command(
    child: &mut std::process::Child,
    status: &mut Option<std::process::ExitStatus>,
    on_its_own: &mut bool,
    signaled: bool,
) -> Result<()> {
    if status.is_none() {
        *status = child.try_wait()?;
        if status.is_some() {
            *on_its_own = !signaled && !STOP.load(Ordering::SeqCst);
        }
    }
    Ok(())
}

/// Operator scheduling priority for an owner; 0 (the default) removes it. Priority only orders
/// admission: it never preempts running work or changes action identity.
pub fn set_priority(store: &Store, owner: &str, priority: i64) -> Result<Value> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    store.db.execute_batch(
        "CREATE TABLE IF NOT EXISTS owner_priorities (owner TEXT PRIMARY KEY, priority INTEGER NOT NULL)",
    )?;
    if priority == 0 {
        store
            .db
            .execute("DELETE FROM owner_priorities WHERE owner=?", [owner])?;
    } else {
        store.db.execute(
            "INSERT INTO owner_priorities VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET priority=excluded.priority",
            params![owner, priority],
        )?;
    }
    event(
        &store.db,
        "owner.priority",
        owner,
        &json!({"priority":priority}),
    )?;
    Ok(json!({"owner":owner,"priority":priority}))
}

/// Source and dependencies for the job, from the repository and files the claim names.
fn prepare(claim: &ClaimFile, job: &Job, directory: &Path) -> Result<(PathBuf, Value)> {
    let source_started = Instant::now();
    engine::verify_runtime(&claim.profile).map_err(RuntimeMismatch)?;
    let view = directory.join("work");
    let (repo, commit) = (&claim.repository, &claim.revision.commit);
    // Infrastructure (this host's I/O, its checkout) is marked `setup`; content checks the revision
    // or its producers fail stay failures with a repair (gate P3-B: classified by cause).
    if job.reuse == Reuse::Files {
        setup(fs::create_dir(&view))?;
        for input in &job.inputs {
            let path = source::relative_path(input)?;
            let output = setup(
                source::git_command(repo)
                    .args(["show", &format!("{commit}:{input}")])
                    .output(),
            )?;
            ensure!(output.status.success(), "declared source input unavailable");
            let target = view.join(path);
            setup(fs::create_dir_all(target.parent().context("input parent")?))?;
            setup(fs::write(&target, output.stdout))?;
            let entry = setup(source::git(repo, &["ls-tree", commit, "--", input]))?;
            if entry.starts_with("100755 ") {
                setup(fs::set_permissions(
                    target,
                    fs::Permissions::from_mode(0o755),
                ))?;
            }
        }
    } else {
        setup(source::checkout_from(repo, commit, &view))?;
    }
    let source_seconds = source_started.elapsed().as_secs_f64();
    let dependencies_started = Instant::now();
    let mut copied_bytes = 0_u64;
    let mut copy_seconds = 0.0;
    let mut restored = BTreeMap::new();
    for dependency in &claim.dependencies {
        let name = &dependency.name;
        let relative = source::relative_path(name)?;
        let metadata = setup(fs::symlink_metadata(&dependency.path))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "dependency artifact is a symlink"
        );
        let hash = setup(file_hash(&dependency.path))?;
        ensure!(
            dependency.sha256 == hash,
            "dependency artifact is corrupt: {name}"
        );
        if let Some(old) = restored.get(name) {
            ensure!(old == &hash, "dependency artifacts conflict: {name}");
            continue;
        }
        let target = view.join(&relative);
        ensure!(
            matches!(fs::symlink_metadata(&target),Err(error) if error.kind()==ErrorKind::NotFound),
            "dependency artifact would overwrite source: {name}"
        );
        contained_parent(&view, &target)?;
        let copy_started = Instant::now();
        copied_bytes += setup(fs::copy(&dependency.path, &target))?;
        setup(fs::set_permissions(
            &target,
            fs::Permissions::from_mode(dependency.mode),
        ))?;
        copy_seconds += copy_started.elapsed().as_secs_f64();
        restored.insert(name.clone(), hash);
    }
    Ok((
        view,
        json!({
            "source_seconds": source_seconds,
            "dependency_seconds": dependencies_started.elapsed().as_secs_f64(),
            "dependency_copy_seconds": copy_seconds,
            "dependency_copy_bytes": copied_bytes,
            "dependency_files": restored.len(),
            "transfer_seconds": claim.transfer["seconds"].as_f64().unwrap_or(0.0),
            "transfer_bytes": claim.transfer["bytes"].as_u64().unwrap_or(0)
        }),
    ))
}

/// A supervisor hears of stops decided elsewhere and reports the stops it decides itself
/// through its attempt's control files; its coordinator relays both.
struct FileControl<'a> {
    control: &'a Path,
}

impl FileControl<'_> {
    fn command_started(&self) -> Result<()> {
        write_atomically(&self.control.join("command-started"), b"")
    }
    /// A stop decided outside the supervisor, once its coordinator wrote one.
    fn requested(&self) -> Result<Option<String>> {
        stop_reason(self.control)
    }
    /// Reports a stop the supervisor decided itself: `supervisor-stop` or `timeout`.
    fn stopping(&self, reason: &str) -> Result<()> {
        write_atomically(
            &self.control.join("stopping"),
            json!({"reason":reason}).to_string().as_bytes(),
        )
    }
}

/// Readers of a control file never see it half written.
pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().context("control directory")?)?;
    temp.write_all(bytes)?;
    temp.persist(path)?;
    Ok(())
}

/// The reason in `control/stop`, once the coordinator wrote one.
pub(crate) fn stop_reason(control: &Path) -> Result<Option<String>> {
    control_reason(&control.join("stop"))
}

/// The reason in a `stop` or `stopping` control file, once one was written.
pub(crate) fn control_reason(path: &Path) -> Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let stop: Value = serde_json::from_slice(&bytes)?;
    Ok(Some(
        stop["reason"]
            .as_str()
            .context("control file lacks a reason")?
            .to_owned(),
    ))
}

/// Tool drift on this host: nothing of the job ran and nothing about the revision failed.
#[derive(Debug)]
struct RuntimeMismatch(anyhow::Error);

impl std::fmt::Display for RuntimeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}
impl std::error::Error for RuntimeMismatch {}

/// A stop that arrived after registration but before the command ran (review P3-4): nothing of
/// the job ran, so the attempt ends not started.
#[derive(Debug)]
struct StoppedBeforeCommand(String);

impl std::fmt::Display for StoppedBeforeCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stopped before the command ran: {}", self.0)
    }
}
impl std::error::Error for StoppedBeforeCommand {}

/// The host could not set the job up (checkout, dependency copies, a full disk): nothing of the
/// job ran and nothing about the revision failed (gate P3-2).
#[derive(Debug)]
struct PreCommand(anyhow::Error);

impl std::fmt::Display for PreCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}
impl std::error::Error for PreCommand {}

fn setup<T>(result: Result<T, impl Into<anyhow::Error>>) -> Result<T> {
    result.map_err(|error| {
        let error = error.into();
        // Tool drift keeps its own, more specific outcome.
        if error.is::<RuntimeMismatch>() {
            error
        } else {
            PreCommand(error).into()
        }
    })
}

/// Output a result may carry, per file (artifacts and logs) and per attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Limits {
    file: u64,
    attempt: u64,
    manifest: u64,
}

/// Every host enforces the same limits, so a result recorded on one is deliverable from all.
const LIMITS: Limits = Limits {
    file: api::MAX_ARTIFACT_BYTES,
    attempt: api::MAX_ATTEMPT_OUTPUT_BYTES,
    manifest: api::MAX_MANIFEST_BYTES,
};

/// The attempt's logs so far, each within the per-file limit; returns their total size.
fn logs_within(directory: &Path, limits: Limits) -> Result<u64> {
    let mut total = 0;
    for log in ["output.log", "cleanup.log"] {
        let bytes = match fs::metadata(directory.join(log)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        ensure!(bytes <= limits.file, "{log} exceeds {} bytes", limits.file);
        total += bytes;
    }
    Ok(total)
}

/// Rechecked after cleanup, whose log counts toward the attempt's output.
fn outputs_within(directory: &Path, evidence: &Value, limits: Limits) -> Result<()> {
    let artifacts: u64 = evidence["artifacts"]
        .as_object()
        .into_iter()
        .flat_map(|artifacts| artifacts.values())
        .filter_map(|artifact| artifact["bytes"].as_u64())
        .sum();
    ensure!(
        logs_within(directory, limits)? + artifacts <= limits.attempt,
        "attempt outputs exceed {} bytes",
        limits.attempt
    );
    Ok(())
}

fn execute_command(
    claim: &ClaimFile,
    job: &Job,
    directory: &Path,
    control: &FileControl,
) -> Result<(i32, Value)> {
    let (action, profile) = (&claim.action, &claim.profile);
    let started = Instant::now();
    if let Some(identity) = &action.identity {
        let current = engine::recipes_for(profile, &identity.version)
            .ok()
            .and_then(|r| r.get(&job.name).cloned());
        ensure!(
            current.as_ref() == Some(&identity.recipe),
            "job identity changed since request"
        );
    }
    let timeout_seconds = claim.attempt.timeout_seconds;
    let (view, preparation) = prepare(claim, job, directory)?;
    let scratch = directory.join("scratch");
    let home = directory.join("home");
    setup(fs::create_dir(&scratch))?;
    setup(fs::create_dir(&home))?;
    if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
        let report = view.join(source::relative_path(&job.report.path)?);
        contained_parent(&view, &report)?;
        if fs::symlink_metadata(&report).is_ok() {
            fs::remove_file(&report)?;
        }
    }
    let log = setup(File::create(directory.join("output.log")))?;
    let paths: Vec<PathBuf> = profile
        .definition
        .tools
        .values()
        .filter_map(|p| p.parent().map(Path::to_path_buf))
        .chain([PathBuf::from("/usr/bin"), PathBuf::from("/bin")])
        .collect();
    let path = std::env::join_paths(paths)?;
    let mut command = Command::new(&profile.definition.tools[&job.command[0]]);
    command
        .args(&job.command[1..])
        .current_dir(&view)
        .env_clear()
        .env("PATH", path)
        .env("HOME", &home)
        .env("TMPDIR", &scratch)
        .env("LANG", "C.UTF-8")
        .env("LC_ALL", "C.UTF-8")
        .env("TZ", "UTC")
        .env("CI", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .envs(&profile.definition.environment)
        .envs(&job.environment)
        .env("PATCHWEFT_STATE", &claim.root)
        .env("PATCHWEFT_ATTEMPT", &claim.attempt.id)
        .env("PATCHWEFT_REVISION", &action.revision)
        .env("PATCHWEFT_REPORT", &job.report.path)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    if job.export_slots {
        command.env("PATCHWEFT_SLOTS", job.slots.to_string());
    }
    // Derived from the declared inputs the action is keyed on, so not part of the recipe.
    if let Some(inputs) = &action.declared_inputs {
        command.env("PATCHWEFT_INPUTS", inputs);
    }
    // The base the key read `base_paths` at, so a job diffs against the base it was keyed on.
    if let Some(base) = &action.declared_base {
        command.env("PATCHWEFT_BASE", base);
    }
    // Preparing can take seconds; a stop recorded meanwhile wins before anything of the job runs.
    let pending = if STOP.load(Ordering::SeqCst) {
        Some("supervisor-stop".to_owned())
    } else {
        control.requested()?
    };
    if let Some(reason) = pending {
        return Err(StoppedBeforeCommand(reason).into());
    }
    let command_started = Instant::now();
    let timer_seconds;
    control.command_started()?;
    let mut child = command.spawn()?;
    let mut status = None;
    let mut terminating: Option<(Instant, String)> = None;
    // Whether the command ended on its own, before any stop could have ended it (`reap_command`).
    let mut exited_first = false;
    loop {
        if terminating.is_none() {
            // A stop recorded elsewhere wins over this supervisor's own timeout.
            let reason = if STOP.load(Ordering::SeqCst) {
                control.stopping("supervisor-stop")?;
                Some("supervisor-stop".to_owned())
            } else if let Some(reason) = control.requested()? {
                Some(reason)
            } else if started.elapsed().as_secs() >= timeout_seconds {
                control.stopping("timeout")?;
                Some("timeout".to_owned())
            } else {
                None
            };
            if reason.is_some() {
                // An exit since the last poll still came before the tree is signalled below.
                reap_command(&mut child, &mut status, &mut exited_first, false)?;
            }
            terminating = reason.map(|reason| (Instant::now(), reason));
        }
        if let Some((time, _)) = &terminating {
            signal_tree(if time.elapsed().as_secs() < 3 {
                libc::SIGTERM
            } else {
                libc::SIGKILL
            });
        }
        reap_command(
            &mut child,
            &mut status,
            &mut exited_first,
            terminating.is_some(),
        )?;
        reap_adopted(child.id());
        if status.is_some() && descendants(std::process::id()).is_empty() {
            // Descendants keep the timer running, so this bounds every timeout check above.
            timer_seconds = started.elapsed().as_secs_f64();
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let command_seconds = command_started.elapsed().as_secs_f64();
    let mut code = status
        .context("child result missing")?
        .code()
        .unwrap_or(128);
    let mut reason = terminating.map(|(_, reason)| reason);
    // A command that failed on its own keeps that verdict: the stop only ended what it left
    // running. An exit 0 is not one, since a job is not done while its descendants run.
    let mut stopped_after_exit = None;
    if exited_first && code != 0 && reason.is_some() {
        stopped_after_exit = reason.take();
    } else if reason.is_some() {
        code = 124;
    }
    logs_within(directory, LIMITS)?;
    let report = if code == 0 {
        validate_report(job, &view, &directory.join("output.log"))?
    } else {
        json!({"execution_failed":true})
    };
    let capture_started = Instant::now();
    let artifacts = if code == 0 {
        capture_artifacts(job, &view, directory, LIMITS)?
    } else {
        BTreeMap::new()
    };
    let mut evidence = json!({"exit_code":code,"reason":reason,
        "elapsed_seconds":started.elapsed().as_secs_f64(),
        "command_seconds":command_seconds,"preparation":preparation,
        "artifact_capture_seconds":capture_started.elapsed().as_secs_f64(),
        "report":report,"log_sha256":file_hash(&directory.join("output.log"))?,
        "artifacts":artifacts,"revision":action.revision,"profile":profile.id,"job":job.name,
        "identity":evidence_identity(action),"timer_seconds":timer_seconds,"host":host(),
        "executor":claim.executor,
        "budget":{"slots":claim.attempt.slots,"memory_mb":claim.attempt.memory_mb,
                  "timeout_seconds":timeout_seconds}});
    if let Some(stop) = stopped_after_exit {
        evidence["stopped_after_exit"] = json!(stop);
    }
    Ok((code, evidence))
}

pub(crate) fn evidence_identity(action: &Action) -> Value {
    action.identity.as_ref().map_or(
        Value::Null,
        |i| json!({"version":i.version,"recipe":i.recipe,"class":i.class,"key":action.key}),
    )
}

pub(crate) fn host() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|name| name.trim().to_owned())
}

fn contained_parent(view: &Path, target: &Path) -> Result<()> {
    let parent = target.parent().context("output parent")?;
    let mut existing = parent;
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing.parent().context("output ancestor")?
            }
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(
        existing.canonicalize()?.starts_with(view.canonicalize()?),
        "output path escapes workspace"
    );
    fs::create_dir_all(parent)?;
    ensure!(
        parent.canonicalize()?.starts_with(view.canonicalize()?),
        "output path escapes workspace"
    );
    Ok(())
}

/// Makes this process the dedicated supervisor of one job: it adopts the job's descendants,
/// turns SIGTERM and SIGINT into a stop and yields the CPU to coordinators.
fn dedicate() -> Result<()> {
    STOP.store(false, Ordering::SeqCst);
    unsafe {
        // The dedicated process owns all adopted descendants; shared coordinators are never reapers.
        ensure!(
            cfg!(target_os = "linux"),
            "supervised execution requires Linux child-subreaper support"
        );
        #[cfg(target_os = "linux")]
        ensure!(
            libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0,
            "subreaper unavailable"
        );
        libc::signal(
            libc::SIGTERM,
            stop_signal as *const () as libc::sighandler_t,
        );
        libc::signal(libc::SIGINT, stop_signal as *const () as libc::sighandler_t);
        libc::nice(10);
    }
    Ok(())
}

/// Evidence of an attempt whose command produced no result.
fn failure(claim: &ClaimFile, job: &Job, error: Option<String>) -> Value {
    let action = &claim.action;
    json!({"error":error,"revision":action.revision,"profile":claim.profile.id,"job":job.name,
           "identity":evidence_identity(action),"key":action.key,"executor":claim.executor})
}

fn not_started(
    claim: &ClaimFile,
    job: &Job,
    reason: &str,
    error: Option<String>,
) -> SupervisorResult {
    SupervisorResult {
        version: CONTROL_VERSION,
        outcome: Outcome::NotStarted,
        reason: Some(reason.into()),
        exit_code: 125,
        evidence: failure(claim, job, error),
        drained: true,
    }
}

/// Ends a registered attempt. Even after an error nothing is reported before the job's
/// processes are gone and its external cleanup ran.
fn conclude(
    claim: &ClaimFile,
    job: &Job,
    directory: &Path,
    result: Result<(i32, Value)>,
) -> SupervisorResult {
    let deadline = Instant::now();
    while !descendants(std::process::id()).is_empty() {
        signal_tree(if deadline.elapsed().as_secs() < 3 {
            libc::SIGTERM
        } else {
            libc::SIGKILL
        });
        reap_adopted(0);
        thread::sleep(Duration::from_millis(50));
    }
    let (outcome, reason, mut code, mut evidence) = match result {
        Ok((code, evidence)) => {
            let reason = evidence["reason"].as_str().map(String::from);
            (Outcome::Completed, reason, code, evidence)
        }
        Err(error) if error.is::<RuntimeMismatch>() => (
            Outcome::NotStarted,
            Some("runtime-mismatch".into()),
            125,
            failure(claim, job, Some(format!("{error:#}"))),
        ),
        Err(error) if error.is::<PreCommand>() => (
            Outcome::NotStarted,
            Some("setup-failed".into()),
            125,
            failure(claim, job, Some(format!("{error:#}"))),
        ),
        Err(error) if error.is::<StoppedBeforeCommand>() => (
            Outcome::NotStarted,
            error
                .downcast_ref::<StoppedBeforeCommand>()
                .map(|stop| stop.0.clone()),
            125,
            failure(claim, job, None),
        ),
        Err(error) => (
            Outcome::Completed,
            None,
            125,
            failure(claim, job, Some(format!("{error:#}"))),
        ),
    };
    let cleanup = cleanup_external(claim, job, directory);
    let drained = cleanup.is_ok();
    if let Err(error) = cleanup {
        code = 125;
        evidence["cleanup_error"] = json!(format!("{error:#}"));
    }
    if let Err(error) = outputs_within(directory, &evidence, LIMITS) {
        code = 125;
        if evidence["error"].is_null() {
            evidence["error"] = json!(format!("{error:#}"));
        }
    }
    SupervisorResult {
        version: CONTROL_VERSION,
        outcome,
        reason,
        exit_code: code,
        evidence,
        drained,
    }
}

/// The coordinator gave up on a supervisor it never registered.
const REGISTRATION_SECONDS: u64 = 3600;

/// `_supervise ROOT ATTEMPT`: runs one claimed attempt from its control files alone. It never
/// opens the store and holds no credentials; its coordinator relays registration and stops.
pub fn supervise(root: &Path, attempt: &str) -> Result<Value> {
    dedicate()?;
    let root = root.canonicalize()?;
    let directory = root.join("attempts").join(attempt);
    let control = directory.join("control");
    let mut claim: ClaimFile = serde_json::from_slice(&fs::read(control.join("claim.json"))?)?;
    ensure!(
        claim.version == CONTROL_VERSION,
        "unsupported claim version {}",
        claim.version
    );
    ensure!(
        claim.attempt.id == attempt && claim.root.canonicalize()? == root,
        "claim belongs to another attempt"
    );
    // Jobs find their attempt directory as `$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT`.
    claim.root = root.clone();
    let job = engine::job(&claim.profile, &claim.action.job)?;
    let result = match registration(&claim, job, &control)? {
        Some(result) => result,
        None => {
            let result =
                execute_command(&claim, job, &directory, &FileControl { control: &control });
            conclude(&claim, job, &directory, result)
        }
    };
    write_atomically(&control.join("result.json"), &serde_json::to_vec(&result)?)?;
    Ok(json!({"attempt":attempt,"outcome":result.outcome,"exit_code":result.exit_code}))
}

/// Waits until the coordinator reports the attempt registered: nothing of the job runs before
/// the store recorded it running. A stop instead yields a not-started result.
fn registration(claim: &ClaimFile, job: &Job, control: &Path) -> Result<Option<SupervisorResult>> {
    let started = Instant::now();
    loop {
        if let Some(reason) = stop_reason(control)? {
            return Ok(Some(not_started(claim, job, &reason, None)));
        }
        if STOP.load(Ordering::SeqCst) {
            return Ok(Some(not_started(claim, job, "supervisor-stop", None)));
        }
        if control.join("registered").exists() {
            return Ok(None);
        }
        if started.elapsed().as_secs() >= REGISTRATION_SECONDS {
            let error = format!("not registered within {REGISTRATION_SECONDS} s");
            return Ok(Some(not_started(
                claim,
                job,
                "registration-timeout",
                Some(error),
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn cleanup_external(claim: &ClaimFile, job: &Job, directory: &Path) -> Result<()> {
    let profile = &claim.profile;
    if job.cleanup.is_empty() {
        return Ok(());
    }
    let log = File::create(directory.join("cleanup.log"))?;
    let paths: Vec<PathBuf> = profile
        .definition
        .tools
        .values()
        .filter_map(|p| p.parent().map(Path::to_path_buf))
        .chain([PathBuf::from("/usr/bin"), PathBuf::from("/bin")])
        .collect();
    let work = directory.join("work");
    let mut child = Command::new(&profile.definition.tools[&job.cleanup[0]])
        .args(&job.cleanup[1..])
        .env_clear()
        .env("PATH", std::env::join_paths(paths)?)
        .envs(&profile.definition.environment)
        .envs(&job.environment)
        .env("PATCHWEFT_STATE", &claim.root)
        .env("PATCHWEFT_ATTEMPT", &claim.attempt.id)
        .env("PATCHWEFT_WORKSPACE", &work)
        .env("TMPDIR", directory.join("scratch"))
        .env("HOME", directory.join("home"))
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()?;
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if start.elapsed().as_secs() >= 60 {
            signal_tree(libc::SIGKILL);
            child.wait()?;
            bail!("external cleanup timed out; capacity remains held");
        }
        thread::sleep(Duration::from_millis(100));
    };
    while !descendants(std::process::id()).is_empty() {
        signal_tree(libc::SIGKILL);
        reap_adopted(child.id());
        thread::sleep(Duration::from_millis(50));
    }
    ensure!(
        status.success(),
        "external cleanup failed; capacity remains held"
    );
    Ok(())
}

pub fn validate_report(job: &Job, view: &Path, log: &Path) -> Result<Value> {
    if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
        let report = view.join(source::relative_path(&job.report.path)?);
        ensure!(
            fs::symlink_metadata(&report)?.is_file()
                && report.canonicalize()?.starts_with(view.canonicalize()?),
            "report must be a fresh regular file inside the workspace"
        );
    }
    let (passed, failed, skipped) = match job.report.kind.as_str() {
        "command" => return Ok(json!({"contract":"command-exit-zero"})),
        "node-tap" => {
            let text = fs::read_to_string(log)?;
            let count = |name: &str| -> Result<u64> {
                text.lines()
                    .rev()
                    .find_map(|line| line.strip_prefix(&format!("# {name} ")))
                    .context(format!("TAP missing {name}"))?
                    .parse()
                    .context("invalid TAP count")
            };
            let passed = count("pass")?;
            let failed = count("fail")? + count("cancelled")?;
            let skipped = count("skipped")? + count("todo")?;
            ensure!(
                count("tests")? == passed + failed + skipped,
                "TAP counts do not reconcile"
            );
            (passed, failed, skipped)
        }
        "vitest" => {
            let report: Value = serde_json::from_slice(&fs::read(view.join(&job.report.path))?)?;
            let cases: Vec<&Value> = report["testResults"]
                .as_array()
                .context("missing Vitest results")?
                .iter()
                .flat_map(|v| v["assertionResults"].as_array().into_iter().flatten())
                .collect();
            let passed = cases.iter().filter(|v| v["status"] == "passed").count() as u64;
            let skipped = cases
                .iter()
                .filter(|v| {
                    ["pending", "todo", "skipped"]
                        .iter()
                        .any(|s| v["status"] == *s)
                })
                .count() as u64;
            let failed = cases.len() as u64 - passed - skipped;
            ensure!(
                report["success"] == true
                    && report["numFailedTestSuites"].as_u64() == Some(0)
                    && report["numFailedTests"].as_u64() == Some(0),
                "Vitest reported failure"
            );
            ensure!(
                report["numTotalTests"].as_u64() == Some(cases.len() as u64),
                "Vitest case counts do not reconcile"
            );
            (passed, failed, skipped)
        }
        "junit" => {
            use quick_xml::{Reader, events::Event};
            let data = fs::read(view.join(&job.report.path))?;
            let mut reader = Reader::from_reader(data.as_slice());
            let (mut total, mut failed, mut skipped) = (0, 0, 0);
            loop {
                match reader.read_event()? {
                    Event::Start(e) | Event::Empty(e) => match e.name().as_ref() {
                        b"testcase" => total += 1,
                        b"failure" | b"error" => failed += 1,
                        b"skipped" => skipped += 1,
                        _ => {}
                    },
                    Event::Eof => break,
                    _ => {}
                }
            }
            ensure!(total >= failed + skipped, "invalid JUnit case counts");
            (total - failed - skipped, failed, skipped)
        }
        _ => bail!("unknown report contract"),
    };
    ensure!(
        failed == 0 && passed >= job.report.minimum && (job.report.allow_skips || skipped == 0),
        "execution contract not met: passed={passed}, failed={failed}, skipped={skipped}, minimum={}",
        job.report.minimum
    );
    Ok(json!({"passed":passed,"failed":failed,"skipped":skipped,"minimum":job.report.minimum}))
}

fn copy_files(
    source: &Path,
    destination: &Path,
    relative: &Path,
    manifest: &mut BTreeMap<String, Value>,
    limits: Limits,
    total: &mut u64,
) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "artifact symlinks are not accepted"
    );
    if metadata.is_dir() {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let path = relative.join(entry.file_name());
            ensure!(!source::protected(&path), "protected artifact path");
            copy_files(
                &entry.path(),
                &destination.join(entry.file_name()),
                &path,
                manifest,
                limits,
                total,
            )?;
        }
    } else {
        ensure!(metadata.is_file(), "artifact is not a regular file");
        ensure!(
            metadata.len() <= limits.file,
            "artifact {} exceeds {} bytes",
            relative.display(),
            limits.file
        );
        *total += metadata.len();
        ensure!(
            *total <= limits.attempt,
            "attempt outputs exceed {} bytes",
            limits.attempt
        );
        fs::create_dir_all(destination.parent().context("artifact parent")?)?;
        fs::copy(source, destination)?;
        let mode = metadata.permissions().mode() & 0o777;
        fs::set_permissions(destination, fs::Permissions::from_mode(mode & !0o222))?;
        manifest.insert(
            relative.to_str().context("non-UTF8 artifact name")?.into(),
            json!({"sha256":file_hash(destination)?,"mode":mode,"bytes":metadata.len()}),
        );
    }
    Ok(())
}

/// Artifacts count toward the attempt's output together with its log.
fn capture_artifacts(
    job: &Job,
    view: &Path,
    directory: &Path,
    limits: Limits,
) -> Result<BTreeMap<String, Value>> {
    let mut total = logs_within(directory, limits)?;
    let output = directory.join("artifacts");
    fs::create_dir(&output)?;
    let mut manifest = BTreeMap::new();
    for path in &job.artifacts {
        let relative = source::relative_path(path)?;
        let target = view.join(&relative);
        ensure!(
            target.canonicalize()?.starts_with(view.canonicalize()?),
            "artifact escapes job workspace"
        );
        copy_files(
            &target,
            &output.join(&relative),
            &relative,
            &mut manifest,
            limits,
            &mut total,
        )?;
    }
    let size = serde_json::to_vec(&manifest)?.len() as u64;
    ensure!(
        size <= limits.manifest,
        "artifact manifest of {} files exceeds {} bytes",
        manifest.len(),
        limits.manifest
    );
    fs::write(
        directory.join("artifacts.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

pub fn deliver(store: &Store, action_id: &str, destination: &Path) -> Result<Value> {
    ensure!(
        engine::action_state(&store.db, action_id)? == "passed",
        "action has no passing result"
    );
    ensure!(
        !destination.exists(),
        "delivery requires a new owned directory; author files are never overwritten"
    );
    let attempt_id: String =
        store
            .db
            .query_row("SELECT attempt FROM actions WHERE id=?", [action_id], |r| {
                r.get(0)
            })?;
    let attempt = store.attempt(&attempt_id)?;
    let evidence = attempt.result.context("missing execution evidence")?;
    let artifacts = evidence["artifacts"]
        .as_object()
        .context("missing artifact manifest")?;
    let parent = destination
        .parent()
        .context("delivery needs a parent directory")?;
    fs::create_dir_all(parent)?;
    let temp = tempfile::tempdir_in(parent)?;
    for (name, expected) in artifacts {
        let relative = source::relative_path(name)?;
        let source = store
            .root
            .join("attempts")
            .join(&attempt_id)
            .join("artifacts")
            .join(&relative);
        ensure!(
            file_hash(&source)?
                == expected["sha256"]
                    .as_str()
                    .context("missing artifact hash")?,
            "retained artifact is corrupt: {name}"
        );
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().context("delivery parent")?)?;
        fs::copy(&source, &target)?;
        fs::set_permissions(
            &target,
            fs::Permissions::from_mode(expected["mode"].as_u64().context("artifact mode")? as u32),
        )?;
    }
    fs::write(
        temp.path().join(".patchweft-receipt.json"),
        serde_json::to_vec_pretty(
            &json!({"action":action_id,"attempt":attempt_id,"evidence":evidence}),
        )?,
    )?;
    publish_directory(temp.path(), destination)?;
    event(
        &store.db,
        "artifact.delivered",
        action_id,
        &json!({"attempt":attempt_id,"destination":destination}),
    )?;
    Ok(json!({"action":action_id,"attempt":attempt_id,"destination":destination}))
}

pub fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes())?;
    let destination = CString::new(destination.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    // Remote clients deliver on macOS; RENAME_EXCL has the same no-overwrite semantics.
    #[cfg(target_os = "macos")]
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(value: Value) -> Job {
        serde_json::from_value(value).unwrap()
    }

    /// An attempt directory whose job wrote `output.log` (4 bytes), `dist/a` (8) and `dist/b` (12).
    fn finished() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let view = temp.path().join("work");
        fs::create_dir_all(view.join("dist")).unwrap();
        fs::write(temp.path().join("output.log"), "log\n").unwrap();
        fs::write(view.join("dist/a"), "12345678").unwrap();
        fs::write(view.join("dist/b"), "123456789012").unwrap();
        temp
    }

    #[test]
    fn capture_enforces_artifact_and_attempt_limits() {
        let build = job(json!({"name":"build","command":["sh"],"artifacts":["dist"]}));
        let capture_with = |file, attempt, manifest| {
            let temp = finished();
            capture_artifacts(
                &build,
                &temp.path().join("work"),
                temp.path(),
                Limits {
                    file,
                    attempt,
                    manifest,
                },
            )
            .map_err(|error| format!("{error:#}"))
        };
        let capture = |file, attempt| capture_with(file, attempt, u64::MAX);
        // The manifest is bounded too, on every host (review P2-3).
        let error = capture_with(12, 24, 10).unwrap_err();
        assert!(
            error.contains("artifact manifest of 2 files exceeds 10 bytes"),
            "{error}"
        );
        let error = capture(11, 100).unwrap_err();
        assert!(error.contains("dist/b exceeds 11 bytes"), "{error}");
        let error = capture(3, 100).unwrap_err();
        assert!(error.contains("output.log exceeds 3 bytes"), "{error}");
        // Artifacts and the log together: 24 bytes.
        let error = capture(12, 23).unwrap_err();
        assert!(error.contains("outputs exceed 23 bytes"), "{error}");
        let manifest = capture(12, 24).unwrap();
        assert_eq!(
            (&manifest["dist/a"]["bytes"], &manifest["dist/b"]["bytes"]),
            (&json!(8), &json!(12))
        );

        // The cleanup log is counted once cleanup ran.
        let temp = finished();
        let limits = Limits {
            file: 12,
            attempt: 24,
            manifest: u64::MAX,
        };
        let artifacts =
            capture_artifacts(&build, &temp.path().join("work"), temp.path(), limits).unwrap();
        let evidence = json!({"artifacts":artifacts});
        outputs_within(temp.path(), &evidence, limits).unwrap();
        fs::write(temp.path().join("cleanup.log"), "x").unwrap();
        let error = format!(
            "{:#}",
            outputs_within(temp.path(), &evidence, limits).unwrap_err()
        );
        assert!(error.contains("outputs exceed 24 bytes"), "{error}");
        fs::write(temp.path().join("cleanup.log"), "1234567890123").unwrap();
        let error = format!(
            "{:#}",
            outputs_within(temp.path(), &evidence, limits).unwrap_err()
        );
        assert!(error.contains("cleanup.log exceeds 12 bytes"), "{error}");

        // Every host passes the same constants, so a result is deliverable from every host.
        assert_eq!(
            LIMITS,
            Limits {
                file: api::MAX_ARTIFACT_BYTES,
                attempt: api::MAX_ATTEMPT_OUTPUT_BYTES,
                manifest: api::MAX_MANIFEST_BYTES,
            }
        );
    }

    #[test]
    fn a_stop_recorded_during_setup_wins_before_the_command() {
        // Gate P3-4: setup can take seconds after registration; a stop recorded meanwhile must
        // end the attempt not started, never spawn the command and then kill it.
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("mirror");
        fs::create_dir(&repository).unwrap();
        let git = |args: &[&str]| source::git(&repository, args).unwrap();
        git(&["init", "--quiet"]);
        fs::write(repository.join("input.txt"), "x\n").unwrap();
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@local.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ]);
        let commit = git(&["rev-parse", "HEAD"]);
        let root = temp.path().join("root");
        let definition: Profile = serde_json::from_value(json!({
            "version":1,"name":"fixture","tools":{"sh":"/bin/sh"},"jobs":[
                {"name":"full","command":["sh","-c","touch \"$PATCHWEFT_STATE/ran\""]}]}))
        .unwrap();
        let profile = RegisteredProfile {
            id: "f-1".into(),
            definition,
            tools: BTreeMap::from([("sh".to_owned(), file_hash(Path::new("/bin/sh")).unwrap())]),
            engine: file_hash(&std::env::current_exe().unwrap()).unwrap(),
            platform: engine::platform(),
        };
        let claim = ClaimFile {
            version: CONTROL_VERSION,
            executor: "mindy".into(),
            root: root.clone(),
            attempt: Grant {
                id: "a1".into(),
                lease: "lease".into(),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds: 60,
            },
            action: Action {
                id: "act".into(),
                key: "key".into(),
                revision: "rev".into(),
                profile: "f-1".into(),
                job: "full".into(),
                dependencies: vec![],
                created: 0.0,
                identity: None,
                reservation: None,
                origin: None,
                declared_inputs: None,
                declared_base: None,
            },
            profile: profile.clone(),
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit,
            },
            repository,
            dependencies: vec![],
            transfer: json!({}),
        };
        let directory = root.join("attempts").join("a1");
        let control = directory.join("control");
        fs::create_dir_all(&control).unwrap();
        fs::write(
            control.join("stop"),
            json!({"reason":"no-requesters"}).to_string(),
        )
        .unwrap();
        let job = engine::job(&profile, "full").unwrap();
        let result = execute_command(&claim, job, &directory, &FileControl { control: &control });
        let error = result.unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<StoppedBeforeCommand>()
                .map(|stop| stop.0.as_str()),
            Some("no-requesters"),
            "{error:#}"
        );
        assert!(!root.join("ran").exists());
        assert!(!control.join("command-started").exists());
    }

    #[test]
    fn files_only_job_reads_the_repository_named_in_the_claim() {
        let temp = tempfile::tempdir().unwrap();
        // The claim's repository lies outside the root, which has no repos/ at all.
        let repository = temp.path().join("mirror");
        fs::create_dir(&repository).unwrap();
        let git = |args: &[&str]| source::git(&repository, args).unwrap();
        git(&["init", "--quiet"]);
        fs::write(repository.join("input.txt"), "from the claim\n").unwrap();
        fs::write(repository.join("tool.sh"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(
            repository.join("tool.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(repository.join("unlisted.txt"), "not an input\n").unwrap();
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@local.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ]);
        let commit = git(&["rev-parse", "HEAD"]);
        let root = temp.path().join("root");
        let definition: Profile = serde_json::from_value(json!({
            "version":1,"name":"fixture","tools":{},"jobs":[
                {"name":"lint","command":["sh"],"reuse":"files","inputs":["input.txt","tool.sh"]},
                {"name":"full","command":["sh"]}]}))
        .unwrap();
        let profile = RegisteredProfile {
            id: "f-1".into(),
            definition,
            tools: BTreeMap::new(),
            engine: file_hash(&std::env::current_exe().unwrap()).unwrap(),
            platform: engine::platform(),
        };
        let claim = ClaimFile {
            version: CONTROL_VERSION,
            executor: "mindy".into(),
            root: root.clone(),
            attempt: Grant {
                id: "a1".into(),
                lease: "lease".into(),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds: 60,
            },
            action: Action {
                id: "act".into(),
                key: "key".into(),
                revision: "rev".into(),
                profile: "f-1".into(),
                job: "lint".into(),
                dependencies: vec![],
                created: 0.0,
                identity: None,
                reservation: None,
                origin: None,
                declared_inputs: None,
                declared_base: None,
            },
            profile: profile.clone(),
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit: commit.clone(),
            },
            repository: repository.clone(),
            dependencies: vec![],
            transfer: json!({"seconds":1.5,"bytes":42}),
        };
        let directory = root.join("attempts").join("a1");
        fs::create_dir_all(&directory).unwrap();
        let (view, preparation) =
            prepare(&claim, engine::job(&profile, "lint").unwrap(), &directory).unwrap();
        assert_eq!(
            fs::read_to_string(view.join("input.txt")).unwrap(),
            "from the claim\n"
        );
        let mode = fs::metadata(view.join("tool.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755);
        assert!(!view.join("unlisted.txt").exists());
        assert!(!view.join(".git").exists());
        assert!(!root.join("repos").exists());
        assert_eq!(
            (
                &preparation["transfer_seconds"],
                &preparation["transfer_bytes"]
            ),
            (&json!(1.5), &json!(42))
        );

        // Every other job checks the claimed commit out of the same repository.
        let directory = root.join("attempts").join("a2");
        fs::create_dir_all(&directory).unwrap();
        let (view, _) =
            prepare(&claim, engine::job(&profile, "full").unwrap(), &directory).unwrap();
        assert!(view.join("unlisted.txt").exists());
        assert_eq!(source::git(&view, &["rev-parse", "HEAD"]).unwrap(), commit);
    }
}
