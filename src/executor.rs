//! The coordinator of one executor. `worker` runs it for the local executor; a remote executor
//! runs the same code over another transport. It claims work, keeps the leases of its attempts,
//! launches `_supervise`, relays stops in both directions and publishes results. Its supervisors
//! reach it only through files in their attempt's `control/` directory.
use crate::{
    api::{self, MAX_HAVES, MAX_RETAINED_UPLOADS, TRANSFER_TIMEOUT_SECONDS},
    authority::{self, AuthorityError, LOCAL, MAX_LEASES, Outputs, REFUSE_ALL, Scope},
    client::{Refused, Remote},
    engine, file_hash,
    model::*,
    source,
    store::Store,
    unique,
    worker::{self, write_atomically},
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    net::{IpAddr, Ipv6Addr, SocketAddr},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Component, Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Refused profiles are judged again after this long: their tools may have been repaired.
const REVERIFY: Duration = Duration::from_secs(600);
/// Transient failures while fetching an attempt's inputs are retried this long, as a serve restart
/// or full transfer permits are; giving up only requeues the action (review P2-1).
const MATERIALIZE_PATIENCE: Duration = Duration::from_secs(300);
const PUBLISH_RETRY: Duration = Duration::from_secs(1);
const PUBLISH_RETRY_MAX: Duration = Duration::from_secs(60);
/// Uploads again after `missing_blob` within one delivery before the publisher's backoff.
const MISSING_BLOB_UPLOADS: u32 = 3;
/// An upload quota still full after this long makes a report undeliverable (MUST-FIX 3).
const QUOTA_PATIENCE: Duration = Duration::from_secs(30 * 60);
/// Retained attempt directories past their retention are looked for this often.
const MAINTENANCE: Duration = Duration::from_secs(3600);
const FLOOR_LOG: Duration = Duration::from_secs(60);
const MIB: u64 = 1024 * 1024;

/// How a coordinator reaches the authority and the inputs of its attempts.
pub trait Transport: Send + Sync {
    /// `PATCHWEFT_STATE` for jobs: attempt `ID` lives in `root/attempts/ID`.
    fn root(&self) -> &Path;
    /// The coordinator's own files: `lock`, `pending-claim` and `ledger/`.
    fn control(&self) -> &Path;
    fn hello(&self, ids: &[String]) -> Result<Hello, TransportError>;
    fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError>;
    fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError>;
    /// A remote transport uploads the attempt's blobs first.
    fn complete(
        &self,
        request: &CompleteRequest,
        attempt: &Path,
    ) -> Result<CompleteResponse, TransportError>;
    /// A repository holding the revision's commit.
    fn source(&self, revision: &RevisionRef) -> Result<PathBuf, TransportError>;
    /// A local file holding one dependency artifact, for the supervisor to copy.
    fn dependency(
        &self,
        dependency: &DependencyEvidence,
        name: &str,
        entry: &ArtifactEntry,
    ) -> Result<PathBuf, TransportError>;
    /// Disposes of an attempt directory once its report was acknowledged.
    fn retire(&self, attempt: &Path, response: &CompleteResponse) -> Result<()>;
    /// Hourly: deletes what retire could not. Returns how many attempts still could not be
    /// cleaned (foreign-owned files need an operator), or None when another sweep is running.
    fn sweep(&self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// Bytes the calling thread downloaded through `source` and `dependency` since it last asked.
    fn transferred(&self) -> u64 {
        0
    }
}

#[derive(Debug)]
pub enum TransportError {
    /// Worth retrying: the authority is unreachable or busy.
    Transient(anyhow::Error),
    /// The attempt is no longer this executor's to report.
    Fenced,
    /// The executor's credential or registration is gone.
    Unauthorized(anyhow::Error),
    /// The same request can never succeed.
    Permanent(anyhow::Error),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(error) | Self::Unauthorized(error) | Self::Permanent(error) => {
                write!(f, "{error:#}")
            }
            Self::Fenced => write!(f, "attempt is fenced"),
        }
    }
}
impl std::error::Error for TransportError {}

impl From<AuthorityError> for TransportError {
    fn from(error: AuthorityError) -> Self {
        match error {
            // Another completion was recorded: there is nothing left to report.
            AuthorityError::Fenced | AuthorityError::CompletionConflict => Self::Fenced,
            AuthorityError::Forbidden | AuthorityError::Unregistered => {
                Self::Unauthorized(anyhow::Error::new(error))
            }
            AuthorityError::Internal(error) => Self::Transient(error),
            error => Self::Permanent(anyhow::Error::new(error)),
        }
    }
}

/// The local executor: the authority in-process, the store's own repositories and attempt
/// directories. It copies nothing.
pub struct LocalTransport {
    root: PathBuf,
    control: PathBuf,
}

impl LocalTransport {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.into(),
            control: root.join("executors").join(LOCAL),
        }
    }
    fn store(&self) -> Result<Store, TransportError> {
        Store::open(&self.root).map_err(TransportError::Transient)
    }
}

impl Transport for LocalTransport {
    fn root(&self) -> &Path {
        &self.root
    }
    fn control(&self) -> &Path {
        &self.control
    }
    fn hello(&self, ids: &[String]) -> Result<Hello, TransportError> {
        Ok(authority::hello(
            &self.store()?.db,
            LOCAL,
            &Scope::all(),
            ids,
        )?)
    }
    fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError> {
        Ok(authority::claim_for(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
        )?)
    }
    fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError> {
        Ok(authority::renew(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
        )?)
    }
    fn complete(
        &self,
        request: &CompleteRequest,
        _: &Path,
    ) -> Result<CompleteResponse, TransportError> {
        Ok(authority::complete(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
            Outputs::InPlace,
        )?)
    }
    fn source(&self, revision: &RevisionRef) -> Result<PathBuf, TransportError> {
        Ok(self.root.join("repos").join(&revision.project))
    }
    fn dependency(
        &self,
        dependency: &DependencyEvidence,
        name: &str,
        _: &ArtifactEntry,
    ) -> Result<PathBuf, TransportError> {
        let relative = source::relative_path(name).map_err(TransportError::Permanent)?;
        Ok(self
            .root
            .join("attempts")
            .join(&dependency.attempt)
            .join("artifacts")
            .join(relative))
    }
    /// The attempt directory holds the store's evidence; once the report is recorded and the
    /// attempt drained, only the job's own trees go.
    fn retire(&self, attempt: &Path, response: &CompleteResponse) -> Result<()> {
        if response.status == "recorded" && response.drained {
            // Renamed aside at once, deleted off the serial publisher thread (gate P3-2).
            if let Some(discard) = set_aside_job_trees(attempt)? {
                let attempt = response.attempt.clone();
                thread::spawn(move || {
                    if let Err(error) = remove_tree(&discard) {
                        log(
                            json!({"event":"retire_failed","executor":LOCAL,"attempt":attempt,
                                   "error":format!("{error:#}")}),
                        );
                    }
                });
            }
        }
        Ok(())
    }
    fn sweep(&self) -> Result<Option<usize>> {
        let Some(swept) = sweep_job_trees(&self.store().map_err(|error| anyhow!(error))?)? else {
            return Ok(None);
        };
        if swept["attempts"] != 0 || swept["failed"] != json!([]) {
            log(json!({"event":"job_trees_swept","executor":LOCAL,"result":swept}));
        }
        Ok(Some(swept["failed"].as_array().map_or(0, Vec::len)))
    }
}

/// What a local job used besides its evidence (`output.log`, `cleanup.log`, `supervisor.log`,
/// `artifacts/`, `artifacts.json`, which stay in the store): its checkout, `TMPDIR` and `HOME`.
const JOB_TREES: [&str; 3] = ["work", "scratch", "home"];
/// Where retire renames job trees before deleting them, so a crash mid-delete leaves a name the
/// sweep knows.
const DISCARD: &str = ".discard";
/// One sweep at a time: the start sweep may still run when the first hourly one begins.
static SWEEPING: Mutex<()> = Mutex::new(());

/// Renames the attempt's job trees into `.discard` (atomic, so evidence and job trees never mix);
/// returns it when there was anything to set aside.
fn set_aside_job_trees(attempt: &Path) -> Result<Option<PathBuf>> {
    let discard = attempt.join(DISCARD);
    // A job could plant `.discard` as a link: its trees must never be renamed out of the store.
    if let Ok(metadata) = fs::symlink_metadata(&discard) {
        ensure!(
            metadata.is_dir(),
            "{} is not a directory",
            discard.display()
        );
    }
    let mut moved = false;
    for tree in JOB_TREES {
        let source = attempt.join(tree);
        if fs::symlink_metadata(&source).is_err() {
            continue;
        }
        fs::create_dir_all(&discard)?;
        fs::rename(&source, discard.join(format!("{tree}-{}", unique())))
            .with_context(|| format!("{}", source.display()))?;
        moved = true;
    }
    Ok((moved || discard.exists()).then_some(discard))
}

fn discard_job_trees(attempt: &Path) -> Result<()> {
    for tree in JOB_TREES.into_iter().chain([DISCARD]) {
        remove_tree(&attempt.join(tree))
            .with_context(|| format!("{}", attempt.join(tree).display()))?;
    }
    Ok(())
}

/// Deletes the job trees of every drained local attempt that still has them: attempts that finished
/// before attempts were retired, or whose retire failed. `worker` runs it once at start.
/// None when another sweep holds the lock: the hourly round then skips instead of stalling the
/// claim loop behind a long start sweep (gate P3-1 on pw#7).
pub fn sweep_job_trees(store: &Store) -> Result<Option<Value>> {
    let _sweeping = match SWEEPING.try_lock() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
    };
    let ids: Vec<String> = {
        let mut stmt = store.db.prepare(
            "SELECT id FROM attempts WHERE drained=1 AND state NOT IN ('claimed','running')",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    let (mut swept, mut failed) = (0, Vec::new());
    for id in ids {
        let directory = store.root.join("attempts").join(&id);
        if !JOB_TREES
            .iter()
            .chain(&[DISCARD])
            .any(|tree| directory.join(tree).exists())
        {
            continue;
        }
        match discard_job_trees(&directory) {
            Ok(()) => swept += 1,
            Err(error) => failed.push(json!({"attempt":id,"error":format!("{error:#}")})),
        }
    }
    Ok(Some(json!({"attempts":swept,"failed":failed})))
}

pub struct Options {
    /// Exit once nothing is claimable and every attempt was reported.
    pub drain: bool,
    pub poll: Duration,
    pub heartbeat: Duration,
}

/// Free bytes on the filesystem holding a path.
pub type FreeSpace = Arc<dyn Fn(&Path) -> io::Result<u64> + Send + Sync>;

/// What a coordinator checks about its host before each claim. `worker` checks nothing unless
/// asked to.
pub struct Guards {
    /// Claims wait while less than this many bytes are free under the root.
    pub min_free: u64,
    /// Loopback ports job containers may reach (MUST-FIX 8); None skips the check.
    pub loopback: Option<BTreeSet<u16>>,
    /// A remote work directory's cache and retained attempts, reclaimed below the floor.
    pub disk: Option<Arc<Disk>>,
    pub free_space: FreeSpace,
    /// The live supervisors last walked and the socket inodes their trees held (option B).
    pub owned_listeners: Arc<Mutex<(Vec<SupervisorIdentity>, BTreeSet<u64>)>>,
}

impl Default for Guards {
    fn default() -> Self {
        Self {
            min_free: 0,
            loopback: None,
            disk: None,
            free_space: Arc::new(|path| fs2::available_space(path)),
            owned_listeners: Arc::default(),
        }
    }
}

impl Guards {
    /// `supervisors`: the live supervisors of this executor's attempts. A listener their trees hold
    /// is the job's own, not a host service (option B), and so are the rootless daemon's ports.
    fn loopback_refusal(
        &self,
        supervisors: &dyn Fn() -> Vec<SupervisorIdentity>,
    ) -> Option<String> {
        let allow = self.loopback.as_ref()?;
        loopback_refusal(&|path| fs::read(path), allow, &|unlisted| {
            let mut exempt = rootless_sockets(rootless_state().as_deref());
            exempt.extend(owned_cached(
                &self.owned_listeners,
                unlisted,
                &supervisors(),
                &sockets_held_by,
            ));
            exempt
        })
    }

    /// How this coordinator checks loopback, reported on its executor record (gate P3-1 on pw#8).
    fn loopback_mode(&self) -> String {
        match &self.loopback {
            None => "off".into(),
            Some(allow) => format!(
                "allow [{}]",
                allow
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Inputs are fetched and the supervisor is not yet spawned.
    Materializing,
    Launched,
    /// A report waits for the publisher.
    Publishing,
}

#[derive(Debug, Clone, PartialEq)]
enum Report {
    /// The supervisor's `result.json`.
    Result,
    /// The supervisor is gone without a result.
    Lost,
    /// Nothing of the job ran.
    NotStarted {
        reason: String,
        error: Option<String>,
    },
    /// The authority refused the report for good; capacity is released only if it drained.
    Undeliverable { error: String, drained: bool },
}

/// One ledger entry: an attempt this coordinator claimed and has not yet seen acknowledged.
struct Entry {
    claim: Claim,
    phase: Phase,
    supervisor: Option<SupervisorIdentity>,
    registered: bool,
    /// The reason for `control/stop`, once the coordinator decided or relayed one.
    stop: Option<String>,
    stop_written: bool,
    fenced: bool,
    /// The supervisor's own stop from `control/stopping`, sent with every renewal.
    stopping: Option<String>,
    stopping_relayed: bool,
    report: Option<Report>,
}

impl Entry {
    fn new(claim: Claim, phase: Phase) -> Self {
        Self {
            claim,
            phase,
            supervisor: None,
            registered: false,
            stop: None,
            stop_written: false,
            fenced: false,
            stopping: None,
            stopping_relayed: false,
            report: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Claimed {
    Attempt(String),
    /// Nothing claimable, and every profile asked about was judged.
    Idle,
    /// The exchange did not settle; try again at the next poll.
    Retry,
}

struct State {
    ledger: BTreeMap<String, Entry>,
    /// Every attempt the ledger held when this coordinator took it up.
    taken_up: BTreeSet<String>,
    verified: BTreeSet<String>,
    refused: BTreeMap<String, (String, Instant)>,
    lease_seconds: u64,
    /// When the last renewal that covered every held lease was sent.
    last_renewal: Instant,
    partitioned: bool,
    revoked: Option<String>,
    /// Why claims are declined, as last logged.
    declined: Option<String>,
    floor_logged: Option<Instant>,
    maintained: Instant,
    /// Local attempts whose job trees the last sweep could not delete.
    trees_stuck: usize,
    children: Vec<Child>,
    /// Attempts waiting for the publisher, in order.
    queue: VecDeque<String>,
    wake: bool,
    shutdown: bool,
    launched: u64,
}

pub struct Coordinator<T> {
    transport: T,
    executor: String,
    options: Options,
    /// sha256 of this binary, the runner every profile run here must have been registered with.
    engine: String,
    host: Option<String>,
    guards: Guards,
    state: Mutex<State>,
    heartbeat: Condvar,
    publisher: Condvar,
}

fn log(value: Value) {
    eprintln!("{value}");
}

/// The control-file view of the attempt the authority acknowledged as fenced.
fn unreported(attempt: &str) -> CompleteResponse {
    CompleteResponse {
        attempt: attempt.into(),
        status: "fenced".into(),
        state: None,
        drained: false,
        replayed: false,
        drain_recorded: false,
    }
}

fn blob(path: &Path) -> Result<Option<BlobRef>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(BlobRef {
            sha256: file_hash(path)?,
            bytes: metadata.len(),
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

impl<T: Transport> Coordinator<T> {
    fn new(
        transport: T,
        executor: String,
        lease_seconds: u64,
        options: Options,
        engine: String,
    ) -> Self {
        Self {
            transport,
            executor,
            options,
            engine,
            host: worker::host(),
            guards: Guards::default(),
            state: Mutex::new(State {
                ledger: BTreeMap::new(),
                taken_up: BTreeSet::new(),
                verified: BTreeSet::new(),
                refused: BTreeMap::new(),
                lease_seconds,
                last_renewal: Instant::now(),
                partitioned: false,
                revoked: None,
                declined: None,
                floor_logged: None,
                maintained: Instant::now(),
                trees_stuck: 0,
                children: Vec::new(),
                queue: VecDeque::new(),
                wake: false,
                shutdown: false,
                launched: 0,
            }),
            heartbeat: Condvar::new(),
            publisher: Condvar::new(),
        }
    }

    fn guarded(mut self, guards: Guards) -> Self {
        self.guards = guards;
        self
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        locked(&self.state)
    }

    fn control(&self, attempt: &str) -> PathBuf {
        self.transport
            .root()
            .join("attempts")
            .join(attempt)
            .join("control")
    }

    fn ledger(&self, attempt: &str) -> PathBuf {
        self.transport
            .control()
            .join("ledger")
            .join(format!("{attempt}.json"))
    }

    /// Judges profiles against this host's tools, runner and platform.
    fn verify(&self, profiles: &[RegisteredProfile]) {
        for profile in profiles {
            let verdict = engine::verify_runtime_for(profile, &self.engine);
            let mut state = self.lock();
            match verdict {
                Ok(()) => {
                    state.refused.remove(&profile.id);
                    state.verified.insert(profile.id.clone());
                }
                Err(error) => self.refuse(&mut state, &profile.id, format!("{error:#}")),
            }
        }
    }

    fn refuse(&self, state: &mut State, profile: &str, error: String) {
        log(json!({"event":"profile_refused","executor":self.executor,
                   "profile":profile,"error":error}));
        state.verified.remove(profile);
        state
            .refused
            .insert(profile.into(), (error, Instant::now()));
    }

    /// The executor may no longer act: every job stops, nothing more is claimed, and reports
    /// keep being tried.
    fn revoke(&self, error: &anyhow::Error) {
        let attempts: Vec<String> = {
            let mut state = self.lock();
            if state.revoked.is_none() {
                log(json!({"event":"revoked","executor":self.executor,
                           "error":format!("{error:#}")}));
                state.revoked = Some(format!("{error:#}"));
            }
            state.ledger.keys().cloned().collect()
        };
        for attempt in attempts {
            self.stop(&attempt, "revoked");
        }
    }

    /// Records a stop and hands it to the supervisor once `claim.json` exists; before that,
    /// launch sees it and reports the attempt not started. The first reason wins.
    fn stop(&self, attempt: &str, reason: &str) {
        let mut state = self.lock();
        let Some(entry) = state.ledger.get_mut(attempt) else {
            return;
        };
        if entry.phase == Phase::Publishing {
            return;
        }
        let reason = entry.stop.get_or_insert_with(|| reason.into()).clone();
        let control = self.control(attempt);
        if entry.stop_written || !control.join("claim.json").exists() {
            return;
        }
        match write_atomically(
            &control.join("stop"),
            json!({"reason":reason}).to_string().as_bytes(),
        ) {
            Ok(()) => entry.stop_written = true,
            Err(error) => log(json!({"event":"stop_failed","executor":self.executor,
                                     "attempt":attempt,"error":format!("{error:#}")})),
        }
    }

    /// Hands an attempt to the publisher.
    fn report(&self, attempt: &str, report: Report) {
        let mut state = self.lock();
        let Some(entry) = state.ledger.get_mut(attempt) else {
            return;
        };
        if entry.phase == Phase::Publishing {
            return;
        }
        entry.phase = Phase::Publishing;
        entry.report = Some(report);
        state.queue.push_back(attempt.into());
        self.publisher.notify_all();
    }

    fn wake(&self) {
        self.lock().wake = true;
        self.heartbeat.notify_all();
    }

    /// Takes up this coordinator's attempts from its ledger: results are published, live
    /// supervisors resumed and dead ones reported. Returns the attempts still to materialize.
    /// `recovering` launches nothing and leaves live supervisors to the next coordinator.
    fn reconcile(&self, recovering: bool) -> Result<Vec<String>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(self.transport.control().join("ledger"))?
            .map(|entry| Ok(entry?.path()))
            .collect::<Result<_>>()?;
        paths.sort();
        let mut materialize = Vec::new();
        for path in paths {
            // Partly written entries are temporary files without the extension.
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let claim: Claim = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("unreadable ledger entry {}", path.display()))?;
            let attempt = claim.attempt.id.clone();
            self.lock().taken_up.insert(attempt.clone());
            let control = self.control(&attempt);
            let supervisor: Option<SupervisorIdentity> =
                match fs::read(control.join("supervisor.json")) {
                    Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
                    Err(error) if error.kind() == ErrorKind::NotFound => None,
                    Err(error) => return Err(error.into()),
                };
            let alive = supervisor
                .as_ref()
                .is_some_and(|s| worker::alive(s.pid, &s.start));
            if recovering && alive {
                continue;
            }
            let launched = control.join("claim.json").exists();
            let finished = || control.join("result.json").exists();
            let mut entry = Entry::new(claim, Phase::Launched);
            entry.registered = control.join("registered").exists();
            entry.stop = worker::stop_reason(&control)?;
            entry.stop_written = entry.stop.is_some();
            entry.stopping = worker::control_reason(&control.join("stopping"))?;
            let spawned = supervisor.is_some();
            entry.supervisor = supervisor;
            if !launched && !recovering {
                entry.phase = Phase::Materializing;
                materialize.push(attempt.clone());
            }
            self.lock().ledger.insert(attempt.clone(), entry);
            if finished() {
                self.report(&attempt, Report::Result);
            } else if !launched {
                if recovering {
                    let reason = "coordinator-lost".into();
                    self.report(
                        &attempt,
                        Report::NotStarted {
                            reason,
                            error: None,
                        },
                    );
                }
            } else if !alive {
                // A supervisor spawned just before the crash must not start its job now.
                if !spawned {
                    self.stop(&attempt, "coordinator-lost");
                }
                let report = if finished() {
                    Report::Result
                } else {
                    Report::Lost
                };
                self.report(&attempt, report);
            }
        }
        Ok(materialize)
    }

    /// One claim exchange. Profiles the authority asks about are verified and the claim is sent
    /// again, so admission never depends on verification lag. While this host must take no work,
    /// the exchange only tells the authority why: `*` refuses every profile.
    fn claim(&self) -> Result<Claimed> {
        let free = self.room();
        let declined = self.declining(free);
        let pending = self.transport.control().join("pending-claim");
        // A key whose response was lost is sent again first: the authority replays its claim.
        let key = match fs::read(&pending) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)?["key"]
                .as_str()
                .context("pending claim lacks a key")?
                .to_owned(),
            // Nothing new is claimed under a refusal, so its key need not survive a crash.
            Err(error) if error.kind() == ErrorKind::NotFound && declined.is_some() => unique(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let key = unique();
                write_atomically(&pending, json!({"key":key}).to_string().as_bytes())?;
                key
            }
            Err(error) => return Err(error.into()),
        };
        let settled = || match fs::remove_file(&pending) {
            Err(error) if error.kind() != ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        };
        loop {
            let mut request = self.ask(&key, free);
            if let Some(reason) = &declined {
                request.refused.insert(REFUSE_ALL.into(), reason.clone());
            }
            let sent = Instant::now();
            let response = match self.transport.claim(&request) {
                Ok(response) => response,
                Err(error) => return self.unclaimed(error),
            };
            if let Some(attempt) = response.fenced {
                log(json!({"event":"claim_fenced","executor":self.executor,"attempt":attempt}));
                // Unless the ledger held it, the key's attempt ended before this coordinator
                // recorded it, so nothing of it ran here. Saying so once lets the authority drain
                // it and queue its action again; its lapsed lease would otherwise hold this host's
                // capacity until an operator confirms the drain. A held one is the ledger's to report.
                if !self.recorded(&attempt) {
                    let report = ClaimRequest {
                        unrecorded: Some(attempt.clone()),
                        ..request
                    };
                    match self.transport.claim(&report) {
                        Ok(_) => log(json!({"event":"claim_unrecorded","executor":self.executor,
                                            "attempt":attempt})),
                        // An authority that predates the field refuses it: only the drain is lost.
                        Err(TransportError::Permanent(error)) => {
                            log(
                                json!({"event":"claim_unrecorded_refused","executor":self.executor,
                                       "attempt":attempt,"error":format!("{error:#}")}),
                            );
                        }
                        // The key stays pending: the next exchange is fenced and says so again.
                        Err(error) => return self.unclaimed(error),
                    }
                }
                settled()?;
                return Ok(Claimed::Retry);
            }
            if let Some(claim) = response.claim {
                let attempt = claim.attempt.id.clone();
                write_atomically(&self.ledger(&attempt), &serde_json::to_vec(&claim)?)?;
                settled()?;
                let mut state = self.lock();
                // The new lease starts now; held ones keep the time of their last renewal.
                if state.ledger.is_empty() {
                    state.last_renewal = sent;
                }
                let mut entry = Entry::new(claim, Phase::Materializing);
                // A claim replayed while declining is reported, never run.
                entry.stop = declined;
                state.ledger.insert(attempt.clone(), entry);
                return Ok(Claimed::Attempt(attempt));
            }
            if declined.is_some() || response.unverified.is_empty() {
                return Ok(Claimed::Idle);
            }
            let hello = match self.transport.hello(&response.unverified) {
                Ok(hello) => hello,
                Err(error) => return self.unclaimed(error),
            };
            self.verify(&hello.profiles);
            let mut state = self.lock();
            for profile in &response.unverified {
                if !state.verified.contains(profile) && !state.refused.contains_key(profile) {
                    let error = "profile is not available to this executor".into();
                    self.refuse(&mut state, profile, error);
                }
            }
        }
    }

    /// Whether this coordinator held the attempt in its ledger: now, or when it took the ledger up
    /// at start, since a crash before a claim's key was settled leaves that key pending while its
    /// attempt runs and is reported. A ledger it cannot read counts as holding it.
    fn recorded(&self, attempt: &str) -> bool {
        let state = self.lock();
        state.ledger.contains_key(attempt)
            || state.taken_up.contains(attempt)
            || self.ledger(attempt).try_exists().unwrap_or(true)
    }

    /// A claim request with this coordinator's verdicts and what it observes of its host.
    fn ask(&self, key: &str, free: Option<u64>) -> ClaimRequest {
        let memory_limit_mb = memory_limit();
        let mut state = self.lock();
        let now = Instant::now();
        state
            .refused
            .retain(|_, (_, at)| now.duration_since(*at) < REVERIFY);
        ClaimRequest {
            claim_key: key.into(),
            verified: state.verified.iter().cloned().collect(),
            refused: state
                .refused
                .iter()
                .map(|(profile, (error, _))| (profile.clone(), error.clone()))
                .collect(),
            memory_limit_mb,
            disk_free_mb: free.map(|bytes| bytes / MIB),
            host: self.host.clone(),
            engine: Some(self.engine.clone()),
            loopback: Some(self.guards.loopback_mode()),
            unrecorded: None,
        }
    }

    /// Free bytes under the root, after reclaiming what the disk bounds allow when below the
    /// floor (MUST-FIX 4).
    fn room(&self) -> Option<u64> {
        let root = self.transport.root();
        let free = || (self.guards.free_space)(root).ok();
        let floor = self.guards.min_free;
        let available = free();
        if floor == 0 || available.is_some_and(|bytes| bytes >= floor) {
            return available;
        }
        if let Some(disk) = &self.guards.disk
            && let Err(error) = disk.reclaim(floor, &|| free().unwrap_or(0), SystemTime::now())
        {
            log(json!({"event":"reclaim_failed","executor":self.executor,
                       "error":format!("{error:#}")}));
        }
        free()
    }

    /// Why this host must take no work now, if it must not: a listener jobs could reach and nobody
    /// allowlisted (MUST-FIX 8), or too little free disk (MUST-FIX 4).
    fn declining(&self, free: Option<u64>) -> Option<String> {
        let floor = self.guards.min_free;
        let low = floor > 0 && free.is_none_or(|bytes| bytes < floor);
        let stuck = self.guards.disk.as_ref().map_or(0, |disk| disk.stuck());
        let trees_stuck = self.lock().trees_stuck;
        let supervisors = || live_supervisors(self.transport.root(), self.transport.control());
        let reason = self.guards.loopback_refusal(&supervisors).or_else(|| {
            low.then(|| {
                let mut reason = format!("free space below the {} MiB floor", floor / MIB);
                if stuck > 0 {
                    // Undeletable without an operator: say so where mauve can see it (gate P3-5).
                    reason.push_str(&format!(
                        "; {stuck} retained attempt directories could not be deleted (foreign-owned \
                         files) and need an operator"
                    ));
                }
                if trees_stuck > 0 {
                    reason.push_str(&format!(
                        "; {trees_stuck} attempts' job trees could not be deleted (foreign-owned \
                         files) and need an operator"
                    ));
                }
                reason
            })
        });
        let mut state = self.lock();
        if low
            && state
                .floor_logged
                .is_none_or(|at| at.elapsed() >= FLOOR_LOG)
        {
            state.floor_logged = Some(Instant::now());
            log(json!({"event":"disk_floor","executor":self.executor,
                       "free_mb":free.map(|bytes| bytes / MIB)}));
        }
        if state.declined != reason {
            log(json!({"event":"claims_declined","executor":self.executor,"reason":reason}));
            state.declined = reason.clone();
        }
        reason
    }

    /// Hourly: retained attempt directories past their retention go, and the local job trees a
    /// retire left behind are swept.
    fn maintain(&self) {
        {
            let mut state = self.lock();
            if state.maintained.elapsed() < MAINTENANCE {
                return;
            }
            state.maintained = Instant::now();
        }
        if let Some(disk) = &self.guards.disk
            && let Err(error) = disk.purge_retained(SystemTime::now())
        {
            log(json!({"event":"purge_failed","executor":self.executor,
                       "error":format!("{error:#}")}));
        }
        // Retire failures, and attempts settled later by recover or a drain report (gate P3-1).
        match self.transport.sweep() {
            Ok(Some(stuck)) => self.lock().trees_stuck = stuck,
            Ok(None) => {}
            Err(error) => log(json!({"event":"sweep_failed","executor":self.executor,
                                     "error":format!("{error:#}")})),
        }
    }

    fn unclaimed(&self, error: TransportError) -> Result<Claimed> {
        match error {
            TransportError::Unauthorized(error) => self.revoke(&error),
            TransportError::Transient(error) => {
                log(json!({"event":"claim_failed","executor":self.executor,
                           "error":format!("{error:#}")}));
            }
            error => return Err(anyhow!(error)),
        }
        Ok(Claimed::Retry)
    }

    /// Fetches the attempt's inputs, writes `claim.json` and spawns its supervisor.
    fn materialize(&self, attempt: &str) {
        if let Err(report) = self.launch(attempt) {
            self.report(attempt, report);
        }
    }

    fn launch(&self, attempt: &str) -> Result<(), Report> {
        let not_started = |reason: &str, error: String| Report::NotStarted {
            reason: reason.into(),
            error: Some(error),
        };
        let Some((claim, stop)) = self
            .lock()
            .ledger
            .get(attempt)
            .map(|e| (e.claim.clone(), e.stop.clone()))
        else {
            return Ok(());
        };
        // A stop that came first ends the attempt before anything is fetched.
        if let Some(reason) = stop {
            return Err(Report::NotStarted {
                reason,
                error: None,
            });
        }
        // Tools may have drifted since the profile was verified.
        if let Err(error) = engine::verify_runtime_for(&claim.profile, &self.engine) {
            let error = format!("{error:#}");
            self.refuse(&mut self.lock(), &claim.profile.id, error.clone());
            return Err(not_started("runtime-mismatch", error));
        }
        let fetching = Instant::now();
        // Giving up requeues the action; refusing the profile here until it is judged again keeps
        // this host from reclaiming it in a loop while other executors can take it.
        let give_up = |reason: &str, error: String| {
            self.refuse(
                &mut self.lock(),
                &claim.profile.id,
                format!("{reason}: {error}"),
            );
            not_started(reason, error)
        };
        let repository = self
            .retrying(|| self.transport.source(&claim.revision))
            .map_err(|error| give_up("materialize-failed", error))?;
        let mut dependencies = Vec::new();
        for dependency in &claim.dependencies {
            for (name, entry) in &dependency.artifacts {
                let path = self
                    .retrying(|| self.transport.dependency(dependency, name, entry))
                    .map_err(|error| give_up("materialize-failed", error))?;
                dependencies.push(DependencyFile {
                    action: dependency.action.clone(),
                    attempt: dependency.attempt.clone(),
                    name: name.clone(),
                    sha256: entry.sha256.clone(),
                    mode: entry.mode,
                    bytes: entry.bytes,
                    path,
                });
            }
        }
        let file = ClaimFile {
            version: CONTROL_VERSION,
            executor: self.executor.clone(),
            root: self.transport.root().into(),
            attempt: claim.attempt.clone(),
            action: claim.action.clone(),
            profile: claim.profile.clone(),
            revision: claim.revision.clone(),
            repository,
            dependencies,
            transfer: json!({"seconds":fetching.elapsed().as_secs_f64(),
                             "bytes":self.transport.transferred()}),
        };
        let control = self.control(attempt);
        fs::create_dir_all(&control)
            .map_err(anyhow::Error::from)
            .and_then(|()| {
                write_atomically(&control.join("claim.json"), &serde_json::to_vec(&file)?)
            })
            .map_err(|error| not_started("launch-failed", format!("{error:#}")))?;
        // A stop that came meanwhile ends the attempt before anything of it runs.
        if let Some(reason) = self.lock().ledger.get(attempt).and_then(|e| e.stop.clone()) {
            return Err(Report::NotStarted {
                reason,
                error: None,
            });
        }
        let child = self
            .spawn(attempt)
            .map_err(|error| give_up("launch-failed", format!("{error:#}")))?;
        let pid = child.id();
        // A supervisor that already exited is reported lost by the next poll.
        let supervisor = worker::process_start(pid).map(|start| SupervisorIdentity { pid, start });
        if let Some(supervisor) = &supervisor
            && let Err(error) = serde_json::to_vec(supervisor)
                .map_err(anyhow::Error::from)
                .and_then(|bytes| write_atomically(&control.join("supervisor.json"), &bytes))
        {
            log(
                json!({"event":"supervisor_unrecorded","executor":self.executor,
                       "attempt":attempt,"error":format!("{error:#}")}),
            );
        }
        let mut state = self.lock();
        state.children.push(child);
        state.launched += 1;
        if let Some(entry) = state.ledger.get_mut(attempt) {
            entry.phase = Phase::Launched;
            entry.supervisor = supervisor;
        }
        // Registration is due at once.
        state.wake = true;
        self.heartbeat.notify_all();
        Ok(())
    }

    fn spawn(&self, attempt: &str) -> Result<Child> {
        let directory = self.transport.root().join("attempts").join(attempt);
        let log = File::create(directory.join("supervisor.log"))?;
        Ok(Command::new(std::env::current_exe()?)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .arg("_supervise")
            .arg(self.transport.root())
            .arg(attempt)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?)
    }

    /// Transient failures are retried with backoff; any other ends the attempt.
    fn retrying<V>(
        &self,
        mut operation: impl FnMut() -> Result<V, TransportError>,
    ) -> Result<V, String> {
        let mut delay = PUBLISH_RETRY;
        let started = Instant::now();
        loop {
            match operation() {
                Ok(value) => return Ok(value),
                Err(TransportError::Transient(error))
                    if started.elapsed() < MATERIALIZE_PATIENCE =>
                {
                    log(json!({"event":"materialize_retry","executor":self.executor,
                               "error":format!("{error:#}")}));
                    thread::sleep(delay);
                    delay = (delay * 2).min(PUBLISH_RETRY_MAX);
                }
                Err(error) => return Err(format!("{error:#}")),
            }
        }
    }

    /// One renewal of every lease this coordinator holds.
    fn heartbeat_once(&self) -> Result<(), TransportError> {
        let reports: Vec<LeaseReport> = {
            let mut state = self.lock();
            if state.ledger.is_empty() {
                state.last_renewal = Instant::now();
                return Ok(());
            }
            state
                .ledger
                .iter()
                .map(|(attempt, entry)| LeaseReport {
                    attempt: attempt.clone(),
                    lease: entry.claim.attempt.lease.clone(),
                    started: (entry.phase == Phase::Launched && !entry.registered)
                        .then(|| entry.supervisor.clone())
                        .flatten(),
                    stopping: entry.stopping.clone(),
                })
                .collect()
        };
        let sent = Instant::now();
        let mut statuses = Vec::new();
        let mut lease_seconds = None;
        for leases in reports.chunks(MAX_LEASES) {
            let request = RenewRequest {
                leases: leases.to_vec(),
            };
            let response = match self.transport.renew(&request) {
                Ok(response) => response,
                Err(TransportError::Unauthorized(error)) => {
                    self.revoke(&error);
                    return Err(TransportError::Unauthorized(error));
                }
                Err(error) => return Err(error),
            };
            lease_seconds = Some(response.lease_seconds);
            statuses.extend(response.leases);
        }
        {
            let mut state = self.lock();
            state.last_renewal = sent;
            state.partitioned = false;
            if let Some(lease_seconds) = lease_seconds {
                // The server's lease is the one it expires by, so it is adopted even when shorter
                // than three heartbeats; renewals then come every third of it (gate P3-1).
                if self.options.heartbeat * 3 > Duration::from_secs(lease_seconds)
                    && state.lease_seconds != lease_seconds
                {
                    log(json!({"event":"heartbeat_adapted","executor":self.executor,
                               "lease_seconds":lease_seconds,
                               "heartbeat_seconds":self.interval(lease_seconds).as_secs_f64()}));
                }
                state.lease_seconds = lease_seconds;
            }
        }
        let sent: BTreeMap<String, Option<String>> = reports
            .into_iter()
            .map(|report| (report.attempt, report.stopping))
            .collect();
        for status in statuses {
            let stopping = sent.get(&status.attempt).cloned().flatten();
            self.apply(status, stopping.as_deref());
        }
        Ok(())
    }

    fn apply(&self, status: LeaseStatus, sent: Option<&str>) {
        let attempt = status.attempt.as_str();
        if status.status != "ok" {
            if let Some(entry) = self.lock().ledger.get_mut(attempt) {
                entry.fenced = true;
            }
            // A live supervisor stops and then reports its drain; a dead one is reported lost.
            self.stop(attempt, "fenced");
            return;
        }
        // A stop is written before registration, so a supervisor never sees itself registered for
        // an action already recorded stopping (review P3-4).
        if let Some(reason) = &status.stop {
            self.stop(attempt, reason);
        }
        {
            let mut state = self.lock();
            let Some(entry) = state.ledger.get_mut(attempt) else {
                return;
            };
            if sent.is_some() && entry.stopping.as_deref() == sent {
                entry.stopping_relayed = true;
            }
            let control = self.control(attempt);
            if status.registered
                && status.stop.is_none()
                && !entry.registered
                && control.join("claim.json").exists()
            {
                match write_atomically(&control.join("registered"), b"") {
                    Ok(()) => entry.registered = true,
                    Err(error) => log(json!({"event":"registration_unrecorded",
                        "executor":self.executor,"attempt":attempt,"error":format!("{error:#}")})),
                }
            }
        }
    }

    /// The renewal interval for a lease: the configured heartbeat, or a third of the lease when
    /// the server shortened it, so a healthy host never runs out of margin (gate P3-1).
    fn interval(&self, lease_seconds: u64) -> Duration {
        self.options
            .heartbeat
            .min(Duration::from_secs(lease_seconds) / 3)
    }

    /// The heartbeat thread: renews when due or woken, backing off while renewals fail.
    fn beat(&self) {
        let mut delay = self.interval(self.lock().lease_seconds);
        loop {
            {
                let state = self.lock();
                let (mut state, _) = self
                    .heartbeat
                    .wait_timeout_while(state, delay, |s| !s.wake && !s.shutdown)
                    .unwrap_or_else(PoisonError::into_inner);
                if state.shutdown {
                    return;
                }
                state.wake = false;
            }
            let renewed = self.heartbeat_once();
            let lease_seconds = self.lock().lease_seconds;
            let interval = self.interval(lease_seconds);
            delay = match renewed {
                Ok(()) => interval,
                Err(error) => {
                    log(json!({"event":"renew_failed","executor":self.executor,
                               "error":format!("{error:#}")}));
                    let cap = Duration::from_secs(lease_seconds) / 4;
                    (delay * 2).min(cap).max(interval)
                }
            };
        }
    }

    /// Watches launched attempts: relays their supervisors' own stops, hands results to the
    /// publisher, reports lost supervisors, and stops every job before an unreachable authority
    /// could let its lease expire.
    fn poll(&self, now: Instant) {
        let launched: Vec<(String, Option<SupervisorIdentity>, bool)> = {
            let mut state = self.lock();
            state
                .children
                .retain_mut(|child| matches!(child.try_wait(), Ok(None)));
            state
                .ledger
                .iter()
                .filter(|(_, entry)| entry.phase == Phase::Launched)
                .map(|(attempt, entry)| {
                    let stopping = entry.stopping.is_some();
                    (attempt.clone(), entry.supervisor.clone(), stopping)
                })
                .collect()
        };
        for (attempt, supervisor, stopping) in launched {
            let control = self.control(&attempt);
            if !stopping && let Ok(Some(reason)) = worker::control_reason(&control.join("stopping"))
            {
                if let Some(entry) = self.lock().ledger.get_mut(&attempt) {
                    entry.stopping = Some(reason);
                }
                self.wake();
            }
            let result = control.join("result.json");
            if result.exists() {
                self.report(&attempt, Report::Result);
            } else if !supervisor.is_some_and(|s| worker::alive(s.pid, &s.start)) {
                // It may have written its result just before exiting.
                let report = if result.exists() {
                    Report::Result
                } else {
                    Report::Lost
                };
                self.report(&attempt, report);
            }
        }
        self.partition(now);
    }

    fn partition(&self, now: Instant) {
        let attempts: Vec<String> = {
            let mut state = self.lock();
            let lease = Duration::from_secs(state.lease_seconds);
            // At most half the lease, so a lease shortened below three heartbeats while running
            // still leaves renewals time to land instead of stopping every job at once.
            let heartbeat = self.options.heartbeat.min(lease / 3);
            let margin = (heartbeat * 2).max(lease / 5).min(lease / 2);
            if now <= state.last_renewal + lease.saturating_sub(margin) {
                return;
            }
            let attempts: Vec<String> = state
                .ledger
                .iter()
                .filter(|(_, entry)| entry.phase == Phase::Launched && entry.stop.is_none())
                .map(|(attempt, _)| attempt.clone())
                .collect();
            if !attempts.is_empty() && !state.partitioned {
                state.partitioned = true;
                log(json!({"event":"partition_stop","executor":self.executor,
                           "attempts":attempts}));
            }
            attempts
        };
        for attempt in attempts {
            self.stop(&attempt, "partition");
        }
    }

    /// Reports one attempt and retires it once the authority acknowledged it. None when the
    /// attempt is no longer in the ledger.
    fn publish(&self, attempt: &str) -> Result<Option<CompleteResponse>, TransportError> {
        let directory = self.transport.root().join("attempts").join(attempt);
        loop {
            let (claim, report, relay) = {
                let state = self.lock();
                let Some(entry) = state.ledger.get(attempt) else {
                    return Ok(None);
                };
                let relay = entry
                    .stopping
                    .clone()
                    .filter(|_| !entry.stopping_relayed && !entry.fenced);
                let report = entry.report.clone().unwrap_or(Report::Lost);
                (entry.claim.clone(), report, relay)
            };
            // The store records a stop the supervisor began before it records the result.
            if let Some(reason) = relay {
                self.relay(attempt, &claim, &reason)?;
            }
            let request = match self.request(attempt, &claim, &report) {
                Ok(request) => request,
                Err(error) => {
                    let drained = !self.control(attempt).join("command-started").exists();
                    self.undeliverable(attempt, format!("{error:#}"), drained);
                    continue;
                }
            };
            let response = match self.transport.complete(&request, &directory) {
                Ok(response) => response,
                Err(TransportError::Fenced) => unreported(attempt),
                Err(TransportError::Permanent(error)) => {
                    if matches!(report, Report::Undeliverable { .. }) {
                        log(json!({"event":"report_abandoned","executor":self.executor,
                                   "attempt":attempt,"error":format!("{error:#}")}));
                        unreported(attempt)
                    } else {
                        self.undeliverable(attempt, format!("{error:#}"), request.drained);
                        continue;
                    }
                }
                Err(TransportError::Unauthorized(error)) => {
                    self.revoke(&error);
                    return Err(TransportError::Unauthorized(error));
                }
                Err(error) => return Err(error),
            };
            // The supervisor found this host's tools drifted: claim no more of the profile.
            if request.outcome == Outcome::NotStarted
                && request.reason.as_deref() == Some("runtime-mismatch")
                && report == Report::Result
            {
                let error = request.evidence["error"]
                    .as_str()
                    .unwrap_or("runtime mismatch")
                    .to_owned();
                self.refuse(&mut self.lock(), &claim.profile.id, error);
            }
            if let Err(error) = self.transport.retire(&directory, &response) {
                log(json!({"event":"retire_failed","executor":self.executor,
                           "attempt":attempt,"error":format!("{error:#}")}));
            }
            match fs::remove_file(self.ledger(attempt)) {
                Err(error) if error.kind() != ErrorKind::NotFound => {
                    return Err(TransportError::Transient(error.into()));
                }
                _ => {}
            }
            self.lock().ledger.remove(attempt);
            return Ok(Some(response));
        }
    }

    fn undeliverable(&self, attempt: &str, error: String, drained: bool) {
        log(json!({"event":"undeliverable","executor":self.executor,
                   "attempt":attempt,"error":error}));
        if let Some(entry) = self.lock().ledger.get_mut(attempt) {
            entry.report = Some(Report::Undeliverable { error, drained });
        }
    }

    fn relay(&self, attempt: &str, claim: &Claim, reason: &str) -> Result<(), TransportError> {
        let request = RenewRequest {
            leases: vec![LeaseReport {
                attempt: attempt.into(),
                lease: claim.attempt.lease.clone(),
                started: None,
                stopping: Some(reason.into()),
            }],
        };
        let response = match self.transport.renew(&request) {
            Ok(response) => response,
            Err(TransportError::Unauthorized(error)) => {
                self.revoke(&error);
                return Err(TransportError::Unauthorized(error));
            }
            Err(error) => return Err(error),
        };
        for status in response.leases {
            self.apply(status, Some(reason));
        }
        Ok(())
    }

    /// The completion for a report, rebuilt identically after a restart so a lost response is
    /// answered as a replay.
    fn request(&self, attempt: &str, claim: &Claim, report: &Report) -> Result<CompleteRequest> {
        let directory = self.transport.root().join("attempts").join(attempt);
        let control = directory.join("control");
        let base = CompleteRequest {
            attempt: attempt.into(),
            lease: claim.attempt.lease.clone(),
            outcome: Outcome::Lost,
            reason: None,
            exit_code: 125,
            evidence: Value::Null,
            drained: true,
            log: None,
            cleanup_log: None,
            artifacts: BTreeMap::new(),
        };
        Ok(match report {
            Report::Result => {
                let result: SupervisorResult =
                    serde_json::from_slice(&fs::read(control.join("result.json"))?)?;
                let artifacts = match result.evidence.get("artifacts") {
                    None | Some(Value::Null) => BTreeMap::new(),
                    Some(artifacts) => serde_json::from_value(artifacts.clone())?,
                };
                CompleteRequest {
                    outcome: result.outcome,
                    reason: result.reason,
                    exit_code: result.exit_code,
                    evidence: result.evidence,
                    drained: result.drained,
                    log: blob(&directory.join("output.log"))?,
                    cleanup_log: blob(&directory.join("cleanup.log"))?,
                    artifacts,
                    ..base
                }
            }
            Report::Lost => {
                let supervisor = fs::read(control.join("supervisor.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .unwrap_or(Value::Null);
                CompleteRequest {
                    evidence: json!({"error":"supervisor exited without a result",
                                     "supervisor":supervisor}),
                    // Only a command that never started is known to have left nothing behind.
                    drained: !control.join("command-started").exists(),
                    ..base
                }
            }
            Report::NotStarted { reason, error } => CompleteRequest {
                outcome: Outcome::NotStarted,
                reason: Some(reason.clone()),
                evidence: json!({"error":error}),
                ..base
            },
            Report::Undeliverable { error, drained } => CompleteRequest {
                outcome: Outcome::Undeliverable,
                evidence: json!({"error":error}),
                drained: *drained,
                ..base
            },
        })
    }

    /// Publishes every queued report now, in place of the publisher thread.
    fn publish_queued(&self) -> Result<Vec<CompleteResponse>, TransportError> {
        let mut responses = Vec::new();
        loop {
            let next = self.lock().queue.front().cloned();
            let Some(attempt) = next else {
                return Ok(responses);
            };
            responses.extend(self.publish(&attempt)?);
            self.lock().queue.pop_front();
        }
    }

    /// The publisher thread: one report at a time, retried with backoff while heartbeats go on.
    fn publisher(&self) {
        let mut delay = PUBLISH_RETRY;
        loop {
            let attempt = {
                let mut state = self.lock();
                loop {
                    if state.shutdown {
                        return;
                    }
                    if let Some(attempt) = state.queue.front() {
                        break attempt.clone();
                    }
                    state = self
                        .publisher
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };
            match self.publish(&attempt) {
                Ok(_) => {
                    self.lock().queue.pop_front();
                    delay = PUBLISH_RETRY;
                }
                Err(error) => {
                    log(json!({"event":"publish_retry","executor":self.executor,
                               "attempt":attempt,"error":format!("{error:#}"),
                               "retry_seconds":delay.as_secs()}));
                    let state = self.lock();
                    let _ = self
                        .publisher
                        .wait_timeout_while(state, delay, |s| !s.shutdown)
                        .unwrap_or_else(PoisonError::into_inner);
                    delay = (delay * 2).min(PUBLISH_RETRY_MAX);
                }
            }
        }
    }

    fn shutdown(&self) {
        self.lock().shutdown = true;
        self.heartbeat.notify_all();
        self.publisher.notify_all();
    }
}

impl<T: Transport + 'static> Coordinator<T> {
    fn materialize_later(self: &Arc<Self>, attempt: String) {
        let coordinator = Arc::clone(self);
        thread::spawn(move || coordinator.materialize(&attempt));
    }

    /// The main loop: watch, then claim until nothing is claimable.
    fn coordinate(self: &Arc<Self>) -> Result<Value> {
        loop {
            self.poll(Instant::now());
            self.maintain();
            let (revoked, idle) = {
                let state = self.lock();
                (state.revoked.clone(), state.ledger.is_empty())
            };
            if let Some(error) = &revoked
                && idle
            {
                anyhow::bail!("executor {} may no longer run work: {error}", self.executor);
            }
            let claimed = if revoked.is_some() {
                Claimed::Idle
            } else {
                self.claim()?
            };
            match claimed {
                Claimed::Attempt(attempt) => {
                    self.materialize_later(attempt);
                    continue;
                }
                // Checked before the claim: a report published since may have unblocked work.
                Claimed::Idle if self.options.drain && idle => break,
                _ => {}
            }
            thread::sleep(self.options.poll);
        }
        let launched = self.lock().launched;
        Ok(json!({"launched":launched,"executor":self.executor}))
    }
}

/// The file whose lock makes one coordinator the executor's only active one.
fn lock_file(transport: &impl Transport) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(transport.control().join("lock"))?)
}

/// Takes the executor's lock. A second coordinator for the same executor waits as a standby
/// until the first one exits.
fn exclusive(transport: &impl Transport, executor: &str) -> Result<(File, bool)> {
    let file = lock_file(transport)?;
    if file.try_lock_exclusive().is_ok() {
        return Ok((file, false));
    }
    log(json!({"waiting_for":"executor lock","executor":executor}));
    file.lock_exclusive()?;
    Ok((file, true))
}

/// Runs a coordinator over `transport` until it drains, or forever.
pub fn run<T: Transport + 'static>(
    transport: T,
    options: Options,
    guards: Guards,
) -> Result<Value> {
    fs::create_dir_all(transport.control().join("ledger"))?;
    let engine = file_hash(&std::env::current_exe()?)?;
    // MUST-FIX 8: nothing registers while job containers could reach an unlisted listener.
    if let Some(reason) =
        guards.loopback_refusal(&|| live_supervisors(transport.root(), transport.control()))
    {
        let request = ClaimRequest {
            claim_key: unique(),
            refused: BTreeMap::from([(REFUSE_ALL.to_owned(), reason.clone())]),
            memory_limit_mb: memory_limit(),
            host: worker::host(),
            engine: Some(engine),
            loopback: Some(guards.loopback_mode()),
            ..ClaimRequest::default()
        };
        // Operators see the refusal on the executor record.
        if let Err(error) = transport.claim(&request) {
            log(json!({"event":"refusal_unreported","error":format!("{error:#}")}));
        }
        bail!("refusing to register: {reason}");
    }
    let hello = transport.hello(&[])?;
    let (_lock, waited) = exclusive(&transport, &hello.executor)?;
    // A standby may have waited for hours.
    let hello = if waited { transport.hello(&[])? } else { hello };
    ensure!(
        options.heartbeat * 3 <= Duration::from_secs(hello.lease_seconds),
        "heartbeat ({} s) must be at most a third of the lease ({} s)",
        options.heartbeat.as_secs_f64(),
        hello.lease_seconds
    );
    if let Some(disk) = &guards.disk {
        disk.start(SystemTime::now())?;
    }
    let coordinator = Arc::new(
        Coordinator::new(
            transport,
            hello.executor.clone(),
            hello.lease_seconds,
            options,
            engine,
        )
        .guarded(guards),
    );
    coordinator.verify(&hello.profiles);
    for attempt in coordinator.reconcile(false)? {
        coordinator.materialize_later(attempt);
    }
    let heartbeat = {
        let coordinator = Arc::clone(&coordinator);
        thread::spawn(move || coordinator.beat())
    };
    let publisher = {
        let coordinator = Arc::clone(&coordinator);
        thread::spawn(move || coordinator.publisher())
    };
    let result = coordinator.coordinate();
    coordinator.shutdown();
    let _ = heartbeat.join();
    let _ = publisher.join();
    result
}

/// `patchweft worker`: the local executor's coordinator.
pub fn local(store: &Store, options: Options, guards: Guards) -> Result<Value> {
    authority::prepare(&store.db)?;
    let legacy: Vec<String> = {
        let mut stmt = store.db.prepare(
            "SELECT id FROM attempts WHERE state IN ('claimed','running')
             AND json_extract(body,'$.lease') IS NULL ORDER BY id",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    ensure!(
        legacy.is_empty(),
        "drain attempts started by the previous binary before upgrading: {}",
        legacy.join(", ")
    );
    // Off the claim path: a first sweep after an upgrade can delete hundreds of gigabytes.
    let root = store.root.clone();
    thread::spawn(move || {
        let swept = Store::open(&root).and_then(|store| sweep_job_trees(&store));
        match swept {
            Ok(swept) => log(json!({"event":"job_trees_swept","executor":LOCAL,"result":swept})),
            Err(error) => log(json!({"event":"job_trees_sweep_failed","executor":LOCAL,
                                     "error":format!("{error:#}")})),
        }
    });
    run(LocalTransport::new(&store.root), options, guards)
}

/// `patchweft recover`: when no coordinator runs, reports the local executor's attempts it can
/// settle (published results, lost supervisors, attempts never launched); live supervisors are
/// left to the next coordinator. Then an operator may confirm one attempt drained.
pub fn recover(store: &mut Store, confirmed: Option<&str>) -> Result<Value> {
    authority::prepare(&store.db)?;
    let transport = LocalTransport::new(&store.root);
    fs::create_dir_all(transport.control().join("ledger"))?;
    let mut drained = Vec::new();
    // A running coordinator takes care of its own attempts.
    let lock = lock_file(&transport)?;
    if lock.try_lock_exclusive().is_ok() {
        let lease_seconds = authority::load(&store.db, LOCAL)?
            .context("local executor record missing")?
            .lease_seconds;
        let options = Options {
            drain: true,
            poll: Duration::from_millis(100),
            heartbeat: Duration::from_secs(1),
        };
        let coordinator = Coordinator::new(
            transport,
            LOCAL.into(),
            lease_seconds,
            options,
            String::new(),
        );
        coordinator.reconcile(true)?;
        for response in coordinator.publish_queued()? {
            if response.drained && response.state.as_deref() == Some("interrupted") {
                drained.push(response.attempt);
            }
        }
    }
    if let Some(attempt) = confirmed {
        authority::confirm_drained(store, attempt)?;
        drained.push(attempt.into());
    }
    let uncertain: Vec<String> = {
        let mut stmt = store.db.prepare(
            "SELECT id FROM attempts WHERE drained=0 AND state NOT IN ('claimed','running')
             ORDER BY rowid",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    Ok(json!({"interrupted_and_drained":drained,"needs_drain_confirmation":uncertain}))
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where a remote executor's work directory stops growing (MUST-FIX 4).
pub struct Bounds {
    /// Claims wait while less is free; the cache and retained attempts are reclaimed first.
    pub min_free: u64,
    pub cas_max: u64,
    /// Fenced or undrained attempt directories are kept this long for inspection.
    pub retain: Duration,
}

/// `patchweft executor`: a remote executor's coordinator. `connect` opens one API client; the
/// heartbeat gets its own.
pub fn remote(
    connect: impl Fn() -> Result<Remote>,
    work: &Path,
    options: Options,
    bounds: Bounds,
    loopback: Option<BTreeSet<u16>>,
) -> Result<Value> {
    fs::create_dir_all(work)?;
    // Supervisors check out from the mirrors by absolute path.
    let work = work.canonicalize()?;
    for directory in ["attempts", "repos", "cas", "retained", "tmp", "ledger"] {
        fs::create_dir_all(work.join(directory))?;
    }
    let disk = Arc::new(Disk::new(&work, bounds.cas_max, bounds.retain));
    let transport = HttpTransport {
        work,
        remote: connect()?,
        heartbeat: connect()?,
        disk: Arc::clone(&disk),
        projects: Mutex::default(),
        uploaded: Mutex::default(),
        throttled: Mutex::default(),
    };
    let guards = Guards {
        min_free: bounds.min_free,
        loopback,
        disk: Some(disk),
        ..Guards::default()
    };
    run(transport, options, guards)
}

/// A remote executor: the authority over the API, sources from per-project mirrors fetched
/// incrementally, dependencies from a content-addressed cache. `R` and `C` are both the work
/// directory.
pub struct HttpTransport {
    work: PathBuf,
    remote: Remote,
    /// Renewals use their own connections, so a long transfer never delays a lease.
    heartbeat: Remote,
    disk: Arc<Disk>,
    /// One fetch per project at a time; one coordinator per work directory makes this enough.
    projects: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    /// Digests each unacknowledged attempt already uploaded.
    uploaded: Mutex<BTreeMap<String, BTreeSet<String>>>,
    /// When each attempt's uploads were first refused for quota.
    throttled: Mutex<BTreeMap<String, Instant>>,
}

thread_local! {
    /// What this materializer thread downloaded, for its attempt's `preparation.transfer_bytes`.
    static TRANSFERRED: Cell<u64> = const { Cell::new(0) };
}

fn fetched(bytes: u64) {
    TRANSFERRED.with(|transferred| transferred.set(transferred.get() + bytes));
}

/// Only an answer from the service is final; an unreachable or failing exchange is retried.
fn classify(error: anyhow::Error) -> TransportError {
    let Some(refused) = error.downcast_ref::<Refused>() else {
        return TransportError::Transient(error);
    };
    match (refused.status, refused.code()) {
        (401 | 403, _) => TransportError::Unauthorized(error),
        (409, Some("attempt_fenced" | "completion_conflict")) => TransportError::Fenced,
        (429 | 500.., _) => TransportError::Transient(error),
        _ => TransportError::Permanent(error),
    }
}

/// Digests a `422 missing_blob` asks for again.
fn missing_blobs(error: &anyhow::Error) -> Option<BTreeSet<String>> {
    let refused = error.downcast_ref::<Refused>()?;
    (refused.code() == Some("missing_blob")).then(|| {
        refused.error["detail"]["missing"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|digest| digest.as_str().map(str::to_owned))
            .collect()
    })
}

/// Each distinct blob a completion names, with its file in the attempt directory.
fn blobs(request: &CompleteRequest, attempt: &Path) -> Result<BTreeMap<String, (PathBuf, u64)>> {
    let mut blobs = BTreeMap::new();
    for (name, blob) in [
        ("output.log", &request.log),
        ("cleanup.log", &request.cleanup_log),
    ] {
        if let Some(blob) = blob {
            blobs.insert(blob.sha256.clone(), (attempt.join(name), blob.bytes));
        }
    }
    for (name, entry) in &request.artifacts {
        let path = attempt.join("artifacts").join(source::relative_path(name)?);
        blobs
            .entry(entry.sha256.clone())
            .or_insert((path, entry.bytes));
    }
    Ok(blobs)
}

impl HttpTransport {
    /// The project's mirror, holding the revision's commit. A fetch sends the mirror's newest
    /// revisions, so the bundle carries only the history the mirror lacks (MUST-FIX 5).
    fn mirror(&self, revision: &RevisionRef) -> Result<PathBuf> {
        ensure!(api::digest(&revision.project, 64), "invalid project ID");
        let lock = Arc::clone(
            locked(&self.projects)
                .entry(revision.project.clone())
                .or_default(),
        );
        let _fetching = locked(&lock);
        let repo = self.work.join("repos").join(&revision.project);
        let holds = || {
            source::git_command(&repo)
                .args(["cat-file", "-e", &format!("{}^{{commit}}", revision.commit)])
                .output()
                .is_ok_and(|output| output.status.success())
        };
        if repo.join("HEAD").exists() && holds() {
            return Ok(repo);
        }
        if !repo.join("HEAD").exists() {
            fs::create_dir_all(&repo)?;
            source::git(&repo, &["init", "--bare", "--quiet"])?;
        }
        let tips = source::git(
            &repo,
            &[
                "for-each-ref",
                "--sort=-committerdate",
                &format!("--count={MAX_HAVES}"),
                "--format=%(objectname)",
                "refs/patchweft/revisions/",
            ],
        )?;
        let haves: Vec<String> = tips.lines().map(str::to_owned).collect();
        let bundle = self.work.join("tmp").join(format!("{}.bundle", unique()));
        let fetch = || -> Result<u64> {
            self.remote.bundle_since(&revision.id, &haves, &bundle)?;
            let reference = format!("refs/patchweft/revisions/{}", revision.id);
            source::git(
                &repo,
                &[
                    "-c",
                    "protocol.file.allow=always",
                    "-c",
                    "fetch.fsckObjects=true",
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    bundle.to_str().context("non-UTF-8 work directory")?,
                    &format!("{reference}:{reference}"),
                ],
            )?;
            Ok(fs::metadata(&bundle)?.len())
        };
        let result = fetch();
        let _ = fs::remove_file(&bundle);
        let bytes = result?;
        ensure!(
            holds(),
            "the revision's bundle lacks commit {}",
            revision.commit
        );
        fetched(bytes);
        log(json!({"event":"source.fetched","project":revision.project,
                   "revision":revision.id,"bytes":bytes,"haves":haves.len()}));
        Ok(repo)
    }

    /// The artifact's file in the cache, downloaded at most once per digest.
    fn cached(&self, dependency: &DependencyEvidence, entry: &ArtifactEntry) -> Result<PathBuf> {
        let sha256 = &entry.sha256;
        ensure!(api::digest(sha256, 64), "invalid artifact digest");
        let path = self.disk.cas().join(sha256);
        {
            let lock = self.disk.lock(sha256);
            let _held = locked(&lock);
            if fs::metadata(&path).is_ok_and(|metadata| metadata.len() == entry.bytes) {
                // A corrupt cache entry is evicted and fetched again rather than failing every
                // consumer on this host with a repair (review P3-8).
                if file_hash(&path).is_ok_and(|hash| &hash == sha256) {
                    self.disk.touch(sha256);
                    return Ok(path);
                }
                log(json!({"event":"cache_corrupt","sha256":sha256}));
                let _ = fs::remove_file(&path);
            }
            let temp = self.work.join("tmp").join(unique());
            let download = || -> Result<()> {
                let route = format!("/v1/artifacts/{}/{sha256}", dependency.action);
                self.remote
                    .download(&route, &temp, Some(sha256), entry.bytes)?;
                fs::set_permissions(&temp, fs::Permissions::from_mode(0o444))?;
                Ok(fs::rename(&temp, &path)?)
            };
            if let Err(error) = download() {
                let _ = fs::remove_file(&temp);
                return Err(error);
            }
        }
        fetched(entry.bytes);
        log(json!({"event":"dependency.fetched","sha256":sha256,"bytes":entry.bytes}));
        self.disk.bound();
        Ok(path)
    }

    /// Uploads the blobs this attempt has not uploaded yet, or exactly `again`. A quota still
    /// full after half an hour makes the report undeliverable.
    fn upload(
        &self,
        attempt: &str,
        blobs: &BTreeMap<String, (PathBuf, u64)>,
        again: Option<&BTreeSet<String>>,
    ) -> Result<(), TransportError> {
        for (sha256, (path, _)) in blobs {
            let done = match again {
                Some(again) => !again.contains(sha256),
                None => locked(&self.uploaded)
                    .get(attempt)
                    .is_some_and(|done| done.contains(sha256)),
            };
            if done {
                continue;
            }
            if let Err(error) = self.remote.upload_blob(path, sha256) {
                if error
                    .downcast_ref::<Refused>()
                    .is_some_and(|refused| refused.status == 429)
                {
                    let first = *locked(&self.throttled)
                        .entry(attempt.into())
                        .or_insert_with(Instant::now);
                    if first.elapsed() >= QUOTA_PATIENCE {
                        return Err(TransportError::Permanent(error));
                    }
                }
                return Err(classify(error));
            }
            locked(&self.uploaded)
                .entry(attempt.into())
                .or_default()
                .insert(sha256.clone());
        }
        locked(&self.throttled).remove(attempt);
        Ok(())
    }
}

impl Transport for HttpTransport {
    fn root(&self) -> &Path {
        &self.work
    }
    fn control(&self) -> &Path {
        &self.work
    }
    fn hello(&self, ids: &[String]) -> Result<Hello, TransportError> {
        let request = HelloRequest { ids: ids.to_vec() };
        self.remote
            .executor("profiles", &request, None)
            .map_err(classify)
    }
    fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError> {
        self.remote
            .executor("claim", request, None)
            .map_err(classify)
    }
    fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError> {
        self.heartbeat
            .executor("renew", request, None)
            .map_err(classify)
    }
    /// A produced artifact enters the cache before the report, so a consumer claimed here never
    /// downloads it (MUST-FIX 4).
    fn complete(
        &self,
        request: &CompleteRequest,
        attempt: &Path,
    ) -> Result<CompleteResponse, TransportError> {
        let blobs = blobs(request, attempt).map_err(TransportError::Permanent)?;
        let total: u64 = blobs.values().map(|(_, bytes)| bytes).sum();
        if total > MAX_RETAINED_UPLOADS {
            return Err(TransportError::Permanent(anyhow!(
                "{total} bytes of logs and artifacts exceed the {MAX_RETAINED_UPLOADS}-byte upload quota"
            )));
        }
        for (name, entry) in &request.artifacts {
            let adopted = source::relative_path(name).and_then(|relative| {
                self.disk
                    .adopt(&attempt.join("artifacts").join(relative), &entry.sha256)
            });
            if let Err(error) = adopted {
                log(json!({"event":"cache_failed","attempt":request.attempt,
                           "sha256":entry.sha256,"error":format!("{error:#}")}));
            }
        }
        if !request.artifacts.is_empty() {
            self.disk.bound();
        }
        self.upload(&request.attempt, &blobs, None)?;
        let timeout = Some(Duration::from_secs(TRANSFER_TIMEOUT_SECONDS));
        let mut uploaded_again = 0;
        loop {
            match self.remote.executor("complete", request, timeout) {
                Ok(response) => {
                    locked(&self.uploaded).remove(&request.attempt);
                    return Ok(response);
                }
                Err(error) => match missing_blobs(&error) {
                    // Purged or taken by a concurrent report since it was uploaded: upload again.
                    Some(missing) if uploaded_again < MISSING_BLOB_UPLOADS => {
                        self.upload(&request.attempt, &blobs, Some(&missing))?;
                        uploaded_again += 1;
                    }
                    // Always recoverable by uploading, so never a reason to discard the result:
                    // the publisher's backoff tries again (gate P3-4).
                    Some(_) => return Err(TransportError::Transient(error)),
                    None => return Err(classify(error)),
                },
            }
        }
    }
    fn source(&self, revision: &RevisionRef) -> Result<PathBuf, TransportError> {
        self.mirror(revision).map_err(classify)
    }
    fn dependency(
        &self,
        dependency: &DependencyEvidence,
        _: &str,
        entry: &ArtifactEntry,
    ) -> Result<PathBuf, TransportError> {
        self.cached(dependency, entry).map_err(classify)
    }
    /// Recorded and drained, nothing of the attempt is needed here (MUST-FIX 4); otherwise its
    /// directory is kept for inspection until the retention passes.
    fn retire(&self, attempt: &Path, response: &CompleteResponse) -> Result<()> {
        if response.status == "recorded" && response.drained {
            match remove_tree(attempt) {
                Ok(()) => return Ok(()),
                // Undeletable now (subordinate-uid files): retain it, so the time-based purge
                // retries it instead of leaving an orphan nothing sweeps (review P2-2).
                Err(error) => log(json!({"event":"retire_failed","attempt":response.attempt,
                                         "error":format!("{error:#}")})),
            }
        }
        let target = self
            .work
            .join("retained")
            .join(attempt.file_name().context("attempt directory")?);
        match fs::rename(attempt, &target) {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            result => result?,
        }
        // Retention counts from now, not from the job's last write.
        let _ = File::open(&target).and_then(|directory| directory.set_modified(SystemTime::now()));
        log(
            json!({"event":"attempt_retained","attempt":response.attempt,
                   "status":response.status,"drained":response.drained}),
        );
        Ok(())
    }
    fn transferred(&self) -> u64 {
        TRANSFERRED.with(|transferred| transferred.replace(0))
    }
}

/// A remote work directory's cache (`cas/<sha256>`, read-only files) and retained attempts
/// (`retained/<attempt>`), kept within bounds (MUST-FIX 4).
pub struct Disk {
    work: PathBuf,
    cas_max: u64,
    retain: Duration,
    /// Per digest: held while it is fetched, adopted or evicted.
    locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    /// Last use of entries whose file time could not be set.
    used: Mutex<BTreeMap<String, SystemTime>>,
    /// Retained entries that could not be deleted (foreign-owned files): retried, logged once,
    /// and named when the disk floor declines work, since only an operator can remove them.
    stuck: Mutex<BTreeSet<String>>,
    /// Deletes a retained entry; tests stand in a failing one for foreign-owned files.
    remove: fn(&Path) -> Result<()>,
}

impl Disk {
    pub fn new(work: &Path, cas_max: u64, retain: Duration) -> Self {
        Self {
            work: work.into(),
            cas_max,
            retain,
            locks: Mutex::default(),
            used: Mutex::default(),
            stuck: Mutex::default(),
            remove: remove_tree,
        }
    }

    /// Retained entries an operator must remove.
    pub fn stuck(&self) -> usize {
        locked(&self.stuck).len()
    }

    fn cas(&self) -> PathBuf {
        self.work.join("cas")
    }

    fn lock(&self, sha256: &str) -> Arc<Mutex<()>> {
        Arc::clone(locked(&self.locks).entry(sha256.into()).or_default())
    }

    /// Marks a cached digest used now. Call with its lock held.
    fn touch(&self, sha256: &str) {
        let now = SystemTime::now();
        let path = self.cas().join(sha256);
        if File::open(path)
            .and_then(|file| file.set_modified(now))
            .is_err()
        {
            locked(&self.used).insert(sha256.into(), now);
        }
    }

    fn last_use(&self, sha256: &str) -> io::Result<SystemTime> {
        let modified = fs::symlink_metadata(self.cas().join(sha256))?.modified()?;
        let used = locked(&self.used).get(sha256).copied();
        Ok(modified.max(used.unwrap_or(UNIX_EPOCH)))
    }

    /// Adds an artifact this executor produced to the cache, unless the digest is there.
    fn adopt(&self, path: &Path, sha256: &str) -> Result<()> {
        ensure!(api::digest(sha256, 64), "invalid artifact digest");
        ensure!(
            fs::symlink_metadata(path)?.is_file(),
            "artifact is not a regular file"
        );
        let lock = self.lock(sha256);
        let _held = locked(&lock);
        let target = self.cas().join(sha256);
        if !target.exists() {
            // Only content that is what its name says enters the cache (review P3-8).
            ensure!(
                file_hash(path)? == sha256,
                "artifact content does not match its digest"
            );
            fs::hard_link(path, &target)?;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o444))?;
        }
        self.touch(sha256);
        Ok(())
    }

    /// Inputs of attempts whose command has not started: they may still be copied.
    fn pinned(&self) -> Result<BTreeSet<String>> {
        let mut pinned = BTreeSet::new();
        for entry in fs::read_dir(self.work.join("ledger"))? {
            let path = entry?.path();
            // Partly written entries are temporary files without the extension.
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let claim: Claim = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("unreadable ledger entry {}", path.display()))?;
            let control = self
                .work
                .join("attempts")
                .join(&claim.attempt.id)
                .join("control");
            if control.join("command-started").exists() {
                continue;
            }
            for dependency in &claim.dependencies {
                pinned.extend(dependency.artifacts.values().map(|e| e.sha256.clone()));
            }
        }
        Ok(pinned)
    }

    /// Deletes the least recently used entries nothing pins, while the cache exceeds its cap or,
    /// given a floor, while less than the floor is free.
    pub fn evict(&self, floor: Option<(u64, &dyn Fn() -> u64)>) -> Result<()> {
        let pinned = self.pinned()?;
        let mut entries = Vec::new();
        let mut total = 0;
        for entry in fs::read_dir(self.cas())? {
            let entry = entry?;
            let Ok(sha256) = entry.file_name().into_string() else {
                continue;
            };
            if !api::digest(&sha256, 64) {
                continue;
            }
            let bytes = entry.metadata()?.len();
            total += bytes;
            entries.push((self.last_use(&sha256)?, sha256, bytes));
        }
        entries.sort();
        for (used, sha256, bytes) in entries {
            let short = floor.is_some_and(|(floor, free)| free() < floor);
            if total <= self.cas_max && !short {
                break;
            }
            if pinned.contains(&sha256) {
                continue;
            }
            let lock = self.lock(&sha256);
            // Being fetched or adopted, or used since it was listed: keep it.
            let Ok(_held) = lock.try_lock() else {
                continue;
            };
            if self.last_use(&sha256).ok() != Some(used) {
                continue;
            }
            fs::remove_file(self.cas().join(&sha256))?;
            total -= bytes;
            log(json!({"event":"cache_evicted","sha256":sha256,"bytes":bytes}));
        }
        Ok(())
    }

    /// Keeps the cache under its cap after an insertion.
    fn bound(&self) {
        if let Err(error) = self.evict(None) {
            log(json!({"event":"evict_failed","error":format!("{error:#}")}));
        }
    }

    /// Deletes retained attempt directories older than the retention; returns their names.
    pub fn purge_retained(&self, now: SystemTime) -> Result<Vec<String>> {
        let entries = match fs::read_dir(self.work.join("retained")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut purged = Vec::new();
        // One undeletable entry (a job wrote files as a subordinate uid) must not stop the rest,
        // the executor's start or the cache eviction behind it (review P2-2): log it and go on.
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    log(json!({"event":"retained_unreadable","error":error.to_string()}));
                    continue;
                }
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let expired = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|modified| now.duration_since(modified).unwrap_or_default() > self.retain);
            match expired.map_err(anyhow::Error::from) {
                Ok(false) => continue,
                Ok(true) => match (self.remove)(&entry.path()) {
                    Ok(()) => {
                        log(json!({"event":"retained_purged","attempt":name}));
                        locked(&self.stuck).remove(&name);
                        purged.push(name);
                    }
                    Err(error) => {
                        if locked(&self.stuck).insert(name.clone()) {
                            log(json!({"event":"retained_purge_failed","attempt":name,
                                       "error":format!("{error:#}")}));
                        }
                    }
                },
                Err(error) => log(json!({"event":"retained_purge_failed","attempt":name,
                                         "error":format!("{error:#}")})),
            }
        }
        purged.sort();
        Ok(purged)
    }

    /// At startup, under the executor lock: half-finished downloads of a previous coordinator go,
    /// and so do retained attempts past their retention.
    fn start(&self, now: SystemTime) -> Result<()> {
        for entry in fs::read_dir(self.work.join("tmp"))? {
            let path = entry?.path();
            let removed = fs::symlink_metadata(&path)
                .map_err(anyhow::Error::from)
                .and_then(|m| {
                    if m.is_dir() {
                        remove_tree(&path)
                    } else {
                        Ok(fs::remove_file(&path)?)
                    }
                });
            if let Err(error) = removed {
                log(json!({"event":"tmp_cleanup_failed","path":path,"error":format!("{error:#}")}));
            }
        }
        if let Err(error) = self.purge_retained(now) {
            log(json!({"event":"retained_purge_failed","error":format!("{error:#}")}));
        }
        Ok(())
    }

    /// Reclaims space for the floor: expired retained attempts, then cache entries.
    fn reclaim(&self, floor: u64, free: &dyn Fn() -> u64, now: SystemTime) -> Result<()> {
        // Eviction runs even when purging could not finish (review P2-2).
        if let Err(error) = self.purge_retained(now) {
            log(json!({"event":"retained_purge_failed","error":format!("{error:#}")}));
        }
        self.evict(Some((floor, free)))
    }
}

/// Removes a directory tree, including directories a job left read-only.
fn remove_tree(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(_) => {}
    }
    fn writable(path: &Path) -> io::Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            let mode = metadata.permissions().mode() | 0o700;
            fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
            for entry in fs::read_dir(path)? {
                writable(&entry?.path())?;
            }
        }
        Ok(())
    }
    writable(path)?;
    Ok(fs::remove_dir_all(path)?)
}

/// The smallest `memory.high` or `memory.max` on this process's cgroup v2 path, in MiB. The
/// authority admits work against the smaller of this and the configured memory.
pub fn cgroup_memory_limit_mb(proc_self_cgroup: &str, sys_root: &Path) -> Option<u32> {
    let path = proc_self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    let mut directory = sys_root.to_path_buf();
    let mut directories = vec![directory.clone()];
    for component in Path::new(path).components() {
        if let Component::Normal(name) = component {
            directory.push(name);
            directories.push(directory.clone());
        }
    }
    directories
        .iter()
        .flat_map(|directory| ["memory.high", "memory.max"].map(|file| directory.join(file)))
        .filter_map(|file| fs::read_to_string(file).ok()?.trim().parse::<u64>().ok())
        .min()
        .map(|bytes| u32::try_from(bytes / MIB).unwrap_or(u32::MAX))
}

/// This process's cgroup memory limit, reported with every claim.
fn memory_limit() -> Option<u32> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_memory_limit_mb(&cgroup, Path::new("/sys/fs/cgroup"))
}

/// MUST-FIX 8 (follow-up 1): jobs run natively on executor hosts, so they reach every listener
/// bound to loopback, and every wildcard (0.0.0.0, ::) listener at 127.0.0.1 with a loopback
/// source, whatever a Docker daemon exposes to containers (gate P2-1 on pw#8). Each must be
/// allowlisted unless `exempt`, given the unlisted listeners' inodes, returns it: the rootless
/// daemon's own processes hold the ports it publishes for job containers. `read` reads a file.
/// Returns why no work may be claimed, if none may.
pub fn loopback_refusal(
    read: &dyn Fn(&Path) -> io::Result<Vec<u8>>,
    allow: &BTreeSet<u16>,
    exempt: &dyn Fn(&BTreeSet<u64>) -> BTreeSet<u64>,
) -> Option<String> {
    if wsl(read) {
        return Some(
            "WSL: Windows listeners can answer at 127.0.0.1 without being listed; run executors in a Linux VM"
                .into(),
        );
    }
    let unlisted: Vec<(SocketAddr, u64)> = match reachable_listeners(read) {
        Ok(listeners) => listeners
            .into_iter()
            .filter(|(listener, _)| !allow.contains(&listener.port()))
            .collect(),
        Err(error) => return Some(format!("cannot list loopback listeners: {error:#}")),
    };
    if unlisted.is_empty() {
        return None;
    }
    let exempt = exempt(&unlisted.iter().map(|(_, inode)| *inode).collect());
    unlisted
        .into_iter()
        .find(|(_, inode)| !exempt.contains(inode))
        .map(|(listener, _)| {
            let kind = if wildcard(listener.ip()) {
                "wildcard"
            } else {
                "loopback"
            };
            format!("{kind} listener {listener} not allowlisted")
        })
}

/// Whether this is a WSL kernel (re-gate P2 on pw#8): under mirrored networking the Windows host's
/// listeners answer at 127.0.0.1 without appearing in /proc/net/tcp, so no listing is complete. The
/// mode can't be ruled out from inside (an interface can be renamed, /sys masked, WSL1 has no
/// `loopback0`), so any WSL kernel refuses: `microsoft` in the release, or the interop entry.
fn wsl(read: &dyn Fn(&Path) -> io::Result<Vec<u8>>) -> bool {
    read(Path::new("/proc/sys/kernel/osrelease")).is_ok_and(|release| {
        String::from_utf8_lossy(&release)
            .to_lowercase()
            .contains("microsoft")
    }) || read(Path::new("/proc/sys/fs/binfmt_misc/WSLInterop")).is_ok()
}

/// An address listening on every interface: 0.0.0.0, ::, or ::ffff:0.0.0.0.
fn wildcard(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_unspecified(),
        IpAddr::V6(ip) => {
            ip.is_unspecified() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_unspecified())
        }
    }
}

/// Where the rootless Docker daemon jobs use keeps its state: beside the socket `DOCKER_HOST`
/// names, or in the user's runtime directory. None when `DOCKER_HOST` names no Unix socket.
fn rootless_state() -> Option<PathBuf> {
    let socket = match std::env::var_os("DOCKER_HOST") {
        Some(host) => PathBuf::from(host.to_str()?.strip_prefix("unix://")?),
        None => {
            let runtime = match std::env::var_os("XDG_RUNTIME_DIR") {
                Some(runtime) => PathBuf::from(runtime),
                None => format!("/run/user/{}", fs::metadata("/proc/self").ok()?.uid()).into(),
            };
            runtime.join("docker.sock")
        }
    };
    Some(socket.parent()?.join("dockerd-rootless"))
}

/// Socket inodes held by the rootless daemon's `rootlesskit` (the parent of the `child_pid` its
/// state records) and its descendants: the ports it publishes for job containers. An
/// unidentified daemon holds none, so its listeners refuse.
fn rootless_sockets(state: Option<&Path>) -> BTreeSet<u64> {
    let rootlesskit = state.and_then(|state| {
        let child: u32 = fs::read_to_string(state.join("child_pid"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        let stat = fs::read_to_string(format!("/proc/{child}/stat")).ok()?;
        // The command name may hold spaces and parentheses; the fields after it cannot.
        let parent: u32 = stat
            .rsplit_once(')')?
            .1
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        let cmdline = fs::read(format!("/proc/{parent}/cmdline")).ok()?;
        let program =
            String::from_utf8_lossy(cmdline.split(|byte| *byte == 0).next()?).into_owned();
        (Path::new(&program).file_name()? == "rootlesskit").then_some(parent)
    });
    match rootlesskit {
        Some(root) => sockets_held_by(&[root]),
        None => BTreeSet::new(),
    }
}

/// The live supervisors this executor's ledger names (`control/ledger/<attempt>.json`, then
/// `root/attempts/<attempt>/control/supervisor.json`: pid and start time, so a reused pid is none).
fn live_supervisors(root: &Path, control: &Path) -> Vec<SupervisorIdentity> {
    let mut live = Vec::new();
    for entry in fs::read_dir(control.join("ledger"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        let Some(attempt) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let identity = root
            .join("attempts")
            .join(attempt)
            .join("control/supervisor.json");
        if let Some(supervisor) = fs::read(identity)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<SupervisorIdentity>(&bytes).ok())
            .filter(|s| worker::alive(s.pid, &s.start))
        {
            live.push(supervisor);
        }
    }
    live.sort_by(|a, b| (a.pid, &a.start).cmp(&(b.pid, &b.start)));
    live
}

/// Socket inodes the trees of `supervisors` hold, walked again whenever the live supervisors
/// change (a socket is no longer its attempt's once its supervisor is gone, though an orphan keeps
/// it open: gate P2-1 on pw#9) or a listener not found owned before appears. Every job process
/// descends from its supervisor, a subreaper.
fn owned_cached(
    cache: &Mutex<(Vec<SupervisorIdentity>, BTreeSet<u64>)>,
    unlisted: &BTreeSet<u64>,
    supervisors: &[SupervisorIdentity],
    held: &dyn Fn(&[u32]) -> BTreeSet<u64>,
) -> BTreeSet<u64> {
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cache.0 != supervisors || !unlisted.is_subset(&cache.1) {
        let pids: Vec<u32> = supervisors.iter().map(|s| s.pid).collect();
        let owned = if pids.is_empty() {
            BTreeSet::new()
        } else {
            held(&pids)
        };
        *cache = (supervisors.to_vec(), owned);
    }
    cache.1.clone()
}

/// Socket inodes open in `roots` or any of their descendants.
fn sockets_held_by(roots: &[u32]) -> BTreeSet<u64> {
    let mut held = BTreeSet::new();
    for pid in roots.iter().copied().chain(worker::descendants_of(roots)) {
        for fd in fs::read_dir(format!("/proc/{pid}/fd"))
            .into_iter()
            .flatten()
            .flatten()
        {
            if let Some(inode) = fs::read_link(fd.path()).ok().and_then(|target| {
                target
                    .to_str()?
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .parse()
                    .ok()
            }) {
                held.insert(inode);
            }
        }
    }
    held
}

/// TCP sockets listening on 127.0.0.0/8, ::1 or a wildcard address (IPv4-mapped included), with
/// their inodes: everything a native job can reach at 127.0.0.1.
fn reachable_listeners(
    read: &dyn Fn(&Path) -> io::Result<Vec<u8>>,
) -> Result<Vec<(SocketAddr, u64)>> {
    let mut listeners = Vec::new();
    for (table, required) in [("/proc/net/tcp", true), ("/proc/net/tcp6", false)] {
        let bytes = match read(Path::new(table)) {
            Ok(bytes) => bytes,
            Err(error) if !required && error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(anyhow::Error::new(error).context(table)),
        };
        for line in String::from_utf8_lossy(&bytes).lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // State 0A is LISTEN.
            if fields.get(3) != Some(&"0A") {
                continue;
            }
            let address = fields
                .get(1)
                .and_then(|field| socket_address(field))
                .with_context(|| format!("{table}: unreadable line {line:?}"))?;
            let reachable = match address.ip() {
                IpAddr::V4(ip) => ip.is_loopback() || ip.is_unspecified(),
                IpAddr::V6(ip) => {
                    ip.is_loopback()
                        || ip.is_unspecified()
                        || ip
                            .to_ipv4_mapped()
                            .is_some_and(|ip| ip.is_loopback() || ip.is_unspecified())
                }
            };
            if reachable {
                let inode = fields
                    .get(9)
                    .and_then(|field| field.parse().ok())
                    .with_context(|| format!("{table}: unreadable inode in {line:?}"))?;
                listeners.push((address, inode));
            }
        }
    }
    Ok(listeners)
}

/// A `/proc/net/tcp*` address: the IP as 32-bit words in host byte order, then the port.
fn socket_address(text: &str) -> Option<SocketAddr> {
    let (ip, port) = text.split_once(':')?;
    if !ip.is_ascii() || ![8, 32].contains(&ip.len()) {
        return None;
    }
    let mut bytes = Vec::with_capacity(16);
    for word in 0..ip.len() / 8 {
        let word = u32::from_str_radix(&ip[word * 8..word * 8 + 8], 16).ok()?;
        bytes.extend(word.to_ne_bytes());
    }
    let ip = match <[u8; 4]>::try_from(bytes.as_slice()) {
        Ok(v4) => IpAddr::from(v4),
        Err(_) => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(bytes).ok()?)),
    };
    Some(SocketAddr::new(ip, u16::from_str_radix(port, 16).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    enum Call {
        Hello(Vec<String>),
        Claim(ClaimRequest),
        Renew(RenewRequest),
        Complete(CompleteRequest),
        Retire(String),
    }

    /// Records every call. Claims answer from a script, then with no work; renewals and
    /// completions follow the configured verdict.
    struct Fake {
        root: PathBuf,
        control: PathBuf,
        profiles: Vec<RegisteredProfile>,
        claims: Mutex<VecDeque<ClaimResponse>>,
        /// "ok", "fenced" or "unreachable".
        renewal: &'static str,
        /// "recorded" or "fenced".
        completion: &'static str,
        /// How a claim saying `unrecorded` fails: "transient", "permanent" or not at all.
        unrecorded_error: Mutex<Option<&'static str>>,
        calls: Mutex<Vec<Call>>,
    }

    impl Fake {
        fn record(&self, call: Call) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl Transport for Fake {
        fn root(&self) -> &Path {
            &self.root
        }
        fn control(&self) -> &Path {
            &self.control
        }
        fn hello(&self, ids: &[String]) -> Result<Hello, TransportError> {
            self.record(Call::Hello(ids.to_vec()));
            Ok(Hello {
                executor: "mindy".into(),
                lease_seconds: 10,
                profiles: self
                    .profiles
                    .iter()
                    .filter(|p| ids.contains(&p.id))
                    .cloned()
                    .collect(),
            })
        }
        fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError> {
            self.record(Call::Claim(request.clone()));
            if request.unrecorded.is_some() {
                match *self.unrecorded_error.lock().unwrap() {
                    Some("transient") => {
                        return Err(TransportError::Transient(anyhow!("connection refused")));
                    }
                    Some(_) => return Err(TransportError::Permanent(anyhow!("HTTP 400"))),
                    None => {}
                }
            }
            Ok(self
                .claims
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ClaimResponse {
                    claim: None,
                    fenced: None,
                    unverified: vec![],
                }))
        }
        fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError> {
            self.record(Call::Renew(request.clone()));
            if self.renewal == "unreachable" {
                return Err(TransportError::Transient(anyhow!("connection refused")));
            }
            let leases = request
                .leases
                .iter()
                .map(|lease| LeaseStatus {
                    attempt: lease.attempt.clone(),
                    status: self.renewal.into(),
                    stop: None,
                    registered: self.renewal == "ok",
                    lease_expires: Some(0.0),
                })
                .collect();
            Ok(RenewResponse {
                lease_seconds: 10,
                leases,
            })
        }
        fn complete(
            &self,
            request: &CompleteRequest,
            _: &Path,
        ) -> Result<CompleteResponse, TransportError> {
            self.record(Call::Complete(request.clone()));
            let fenced = self.completion == "fenced";
            Ok(CompleteResponse {
                attempt: request.attempt.clone(),
                status: self.completion.into(),
                state: Some("interrupted".into()),
                drained: request.drained,
                replayed: false,
                drain_recorded: fenced && request.drained,
            })
        }
        fn source(&self, _: &RevisionRef) -> Result<PathBuf, TransportError> {
            Ok(self.root.join("repos/p"))
        }
        fn dependency(
            &self,
            _: &DependencyEvidence,
            name: &str,
            _: &ArtifactEntry,
        ) -> Result<PathBuf, TransportError> {
            Ok(self.root.join("cas").join(name))
        }
        fn retire(&self, attempt: &Path, _: &CompleteResponse) -> Result<()> {
            let name = attempt.file_name().unwrap().to_string_lossy().into_owned();
            self.record(Call::Retire(name));
            Ok(())
        }
        /// Two attempts whose job trees an operator must delete.
        fn sweep(&self) -> Result<Option<usize>> {
            Ok(Some(2))
        }
    }

    fn profile(id: &str, engine: &str) -> RegisteredProfile {
        let definition: Profile = serde_json::from_value(json!({
            "version":1,"name":id,"tools":{},"jobs":[{"name":"job","command":["sh"]}]}))
        .unwrap();
        RegisteredProfile {
            id: id.into(),
            definition,
            tools: BTreeMap::new(),
            engine: engine.into(),
            platform: engine::platform(),
        }
    }

    fn claim(attempt: &str) -> Claim {
        Claim {
            attempt: Grant {
                id: attempt.into(),
                lease: format!("lease-{attempt}"),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds: 60,
            },
            action: Action {
                id: format!("action-{attempt}"),
                key: "key".into(),
                revision: "rev".into(),
                profile: "p-ok".into(),
                job: "job".into(),
                dependencies: vec![],
                created: 0.0,
                identity: None,
                reservation: None,
                origin: None,
                declared_inputs: None,
                declared_base: None,
            },
            profile: profile("p-ok", "e"),
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit: "c".repeat(40),
            },
            dependencies: vec![],
        }
    }

    fn fake(root: &Path) -> Fake {
        Fake {
            root: root.join("work"),
            control: root.join("work"),
            profiles: vec![profile("p-ok", "e"), profile("p-bad", "other")],
            claims: Mutex::new(VecDeque::new()),
            renewal: "ok",
            completion: "recorded",
            unrecorded_error: Mutex::new(None),
            calls: Mutex::new(vec![]),
        }
    }

    /// A coordinator for executor mindy: lease 10 s, heartbeat 1 s, engine "e".
    fn coordinator(fake: Fake) -> Coordinator<Fake> {
        fs::create_dir_all(fake.control.join("ledger")).unwrap();
        let options = Options {
            drain: true,
            poll: Duration::from_millis(10),
            heartbeat: Duration::from_secs(1),
        };
        Coordinator::new(fake, "mindy".into(), 10, options, "e".into())
    }

    /// A claimed attempt whose supervisor was spawned: ledger entry, `claim.json` and, given an
    /// identity, `supervisor.json`.
    fn launched(
        coordinator: &Coordinator<Fake>,
        attempt: &str,
        supervisor: Option<&SupervisorIdentity>,
    ) -> PathBuf {
        fs::write(
            coordinator.ledger(attempt),
            serde_json::to_vec(&claim(attempt)).unwrap(),
        )
        .unwrap();
        let control = coordinator.control(attempt);
        fs::create_dir_all(&control).unwrap();
        fs::write(control.join("claim.json"), "{}").unwrap();
        if let Some(supervisor) = supervisor {
            fs::write(
                control.join("supervisor.json"),
                serde_json::to_vec(supervisor).unwrap(),
            )
            .unwrap();
        }
        control
    }

    /// The identity of a process that no longer exists.
    fn dead() -> SupervisorIdentity {
        SupervisorIdentity {
            pid: std::process::id(),
            start: "0".into(),
        }
    }

    /// A live stand-in for a supervisor, killed when dropped.
    struct Live(Child);

    impl Live {
        fn new() -> Self {
            Self(Command::new("sleep").arg("30").spawn().unwrap())
        }
        fn identity(&self) -> SupervisorIdentity {
            let pid = self.0.id();
            SupervisorIdentity {
                pid,
                start: worker::process_start(pid).unwrap(),
            }
        }
    }

    impl Drop for Live {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn calls(coordinator: &Coordinator<Fake>) -> Vec<Call> {
        coordinator.transport.calls.lock().unwrap().clone()
    }

    fn completions(coordinator: &Coordinator<Fake>) -> Vec<CompleteRequest> {
        calls(coordinator)
            .into_iter()
            .filter_map(|call| match call {
                Call::Complete(request) => Some(request),
                _ => None,
            })
            .collect()
    }

    fn result(control: &Path, result: SupervisorResult) {
        fs::write(
            control.join("result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn reconcile_publishes_a_finished_attempt() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        let control = launched(&coordinator, "a1", Some(&dead()));
        let directory = control.parent().unwrap();
        fs::write(directory.join("output.log"), "log\n").unwrap();
        let artifacts = json!({"dist/out":{"sha256":"s","mode":0o444,"bytes":3}});
        result(
            &control,
            SupervisorResult {
                version: CONTROL_VERSION,
                outcome: Outcome::Completed,
                reason: Some("timeout".into()),
                exit_code: 124,
                evidence: json!({"exit_code":124,"reason":"timeout","artifacts":artifacts}),
                drained: true,
            },
        );
        // The supervisor timed out on its own; the store must hear of it before the result.
        fs::write(control.join("stopping"), r#"{"reason":"timeout"}"#).unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let calls = calls(&coordinator);
        assert_eq!(calls.len(), 3, "{calls:?}");
        let Call::Renew(renewal) = &calls[0] else {
            panic!("{calls:?}")
        };
        assert_eq!(renewal.leases.len(), 1);
        assert_eq!(renewal.leases[0].lease, "lease-a1");
        assert_eq!(renewal.leases[0].stopping.as_deref(), Some("timeout"));
        let Call::Complete(request) = &calls[1] else {
            panic!("{calls:?}")
        };
        assert_eq!(
            (request.attempt.as_str(), request.lease.as_str()),
            ("a1", "lease-a1")
        );
        assert_eq!(
            (
                request.outcome,
                request.reason.as_deref(),
                request.exit_code
            ),
            (Outcome::Completed, Some("timeout"), 124)
        );
        assert!(request.drained);
        assert_eq!(
            request.log,
            Some(BlobRef {
                sha256: crate::hash(b"log\n"),
                bytes: 4
            })
        );
        assert_eq!(request.cleanup_log, None);
        assert_eq!(json!(request.artifacts), artifacts);
        assert!(matches!(&calls[2], Call::Retire(attempt) if attempt == "a1"));
        assert!(!coordinator.ledger("a1").exists());
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn reconcile_reports_never_started_dead_supervisor_as_drained() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        launched(&coordinator, "a1", Some(&dead()));
        // Spawned but never recorded: the crash came between the spawn and supervisor.json.
        let unrecorded = launched(&coordinator, "a2", None);
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(request.outcome, Outcome::Lost);
            // Its command never started, so nothing of it can be left behind.
            assert!(request.drained, "{}", request.attempt);
            assert_eq!(
                request.evidence["error"],
                "supervisor exited without a result"
            );
        }
        assert_eq!(requests[0].evidence["supervisor"], json!(dead()));
        assert_eq!(requests[1].evidence["supervisor"], Value::Null);
        // A supervisor nobody recorded may still be waiting for registration: it must not start.
        assert_eq!(
            fs::read_to_string(unrecorded.join("stop")).unwrap(),
            r#"{"reason":"coordinator-lost"}"#
        );
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn reconcile_reports_started_dead_supervisor_as_undrained() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        let control = launched(&coordinator, "a1", Some(&dead()));
        fs::write(control.join("registered"), "").unwrap();
        fs::write(control.join("command-started"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].outcome, Outcome::Lost);
        // Its job may have left processes or external resources: capacity stays held.
        assert!(!requests[0].drained);
        assert!(!control.join("stop").exists());
    }

    #[test]
    fn fenced_renew_stops_live_supervisor_then_reports_drain() {
        let temp = tempfile::tempdir().unwrap();
        let mut fake = fake(temp.path());
        fake.renewal = "fenced";
        fake.completion = "fenced";
        let coordinator = coordinator(fake);
        let supervisor = Live::new();
        let control = launched(&coordinator, "a1", Some(&supervisor.identity()));
        fs::write(control.join("registered"), "").unwrap();
        fs::write(control.join("command-started"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.poll(Instant::now());
        assert!(coordinator.lock().queue.is_empty(), "a live supervisor");
        coordinator.heartbeat_once().unwrap();
        assert_eq!(
            fs::read_to_string(control.join("stop")).unwrap(),
            r#"{"reason":"fenced"}"#
        );
        assert!(completions(&coordinator).is_empty());
        // The supervisor stops its job, cleans up and reports the attempt drained.
        result(
            &control,
            SupervisorResult {
                version: CONTROL_VERSION,
                outcome: Outcome::Completed,
                reason: Some("fenced".into()),
                exit_code: 124,
                evidence: json!({"exit_code":124,"reason":"fenced"}),
                drained: true,
            },
        );
        drop(supervisor);
        coordinator.poll(Instant::now());
        let responses = coordinator.publish_queued().unwrap();
        assert_eq!(responses.len(), 1);
        assert!(responses[0].drain_recorded);
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].reason.as_deref(), Some("fenced"));
        assert!(requests[0].drained);
        assert!(
            calls(&coordinator)
                .iter()
                .any(|call| matches!(call, Call::Retire(attempt) if attempt == "a1"))
        );
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn partition_deadline_stops_jobs_before_lease_expiry() {
        let temp = tempfile::tempdir().unwrap();
        let mut fake = fake(temp.path());
        fake.renewal = "unreachable";
        let coordinator = coordinator(fake);
        let supervisor = Live::new();
        let control = launched(&coordinator, "a1", Some(&supervisor.identity()));
        fs::write(control.join("registered"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        let last = coordinator.lock().last_renewal;
        assert!(matches!(
            coordinator.heartbeat_once(),
            Err(TransportError::Transient(_))
        ));
        assert_eq!(coordinator.lock().last_renewal, last);
        // Lease 10 s, heartbeat 1 s: the margin is max(2 × 1 s, 10 s / 5) = 2 s.
        coordinator.poll(last + Duration::from_millis(7_900));
        assert!(!control.join("stop").exists());
        coordinator.poll(last + Duration::from_millis(8_100));
        assert_eq!(
            fs::read_to_string(control.join("stop")).unwrap(),
            r#"{"reason":"partition"}"#
        );
        // The job keeps its lease until it reports; nothing is completed yet.
        assert!(completions(&coordinator).is_empty());
        assert!(coordinator.lock().partitioned);
    }

    #[test]
    fn a_shortened_lease_adapts_the_heartbeat_and_keeps_a_margin() {
        let temp = tempfile::tempdir().unwrap();
        let mut fake = fake(temp.path());
        fake.renewal = "unreachable";
        let coordinator = coordinator(fake);
        // Heartbeat 1 s: a 2 s lease is renewed every third of it, not every second (gate P3-1).
        assert_eq!(coordinator.interval(2), Duration::from_secs(2) / 3);
        assert_eq!(coordinator.interval(10), Duration::from_secs(1));
        let supervisor = Live::new();
        let control = launched(&coordinator, "a1", Some(&supervisor.identity()));
        fs::write(control.join("registered"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.lock().lease_seconds = 3;
        let last = coordinator.lock().last_renewal;
        // Lease 3 s: the margin max(2 × 1 s, 0.6 s) is capped at half the lease, 1.5 s.
        coordinator.poll(last + Duration::from_millis(1_400));
        assert!(!control.join("stop").exists());
        coordinator.poll(last + Duration::from_millis(1_600));
        assert!(control.join("stop").exists());
    }

    #[test]
    fn pending_claim_is_replayed_first() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let pending = fake.control.join("pending-claim");
        fake.claims.lock().unwrap().extend([
            ClaimResponse {
                claim: None,
                fenced: None,
                unverified: vec![],
            },
            ClaimResponse {
                claim: Some(claim("a1")),
                fenced: None,
                unverified: vec![],
            },
        ]);
        let coordinator = coordinator(fake);
        // Written before a crash lost the response to it.
        fs::write(&pending, r#"{"key":"replayed"}"#).unwrap();
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        // A poll without work keeps the key.
        assert!(pending.exists());
        assert_eq!(coordinator.claim().unwrap(), Claimed::Attempt("a1".into()));
        assert!(!pending.exists());
        let ledger: Claim =
            serde_json::from_slice(&fs::read(coordinator.ledger("a1")).unwrap()).unwrap();
        assert_eq!(ledger.attempt, claim("a1").attempt);
        assert_eq!(coordinator.lock().ledger["a1"].phase, Phase::Materializing);
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        let keys: Vec<String> = calls(&coordinator)
            .into_iter()
            .filter_map(|call| match call {
                Call::Claim(request) => Some(request.claim_key),
                _ => None,
            })
            .collect();
        assert_eq!(keys[..2], ["replayed", "replayed"]);
        assert_ne!(keys[2], "replayed");
        let next: Value = serde_json::from_slice(&fs::read(&pending).unwrap()).unwrap();
        assert_eq!(next["key"], keys[2]);
    }

    fn fenced(attempt: &str) -> ClaimResponse {
        ClaimResponse {
            claim: None,
            fenced: Some(attempt.into()),
            unverified: vec![],
        }
    }

    /// The key and `unrecorded` of every claim request sent.
    fn claim_keys(coordinator: &Coordinator<Fake>) -> Vec<(String, Option<String>)> {
        claims_sent(coordinator)
            .into_iter()
            .map(|request| (request.claim_key, request.unrecorded))
            .collect()
    }

    fn sent(key: &str, unrecorded: Option<&str>) -> (String, Option<String>) {
        (key.into(), unrecorded.map(String::from))
    }

    #[test]
    fn a_fenced_key_the_ledger_never_held_is_reported_unrecorded() {
        // The authority recorded the claim, its response was lost, and its lease lapsed before the
        // replayed key got through.
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let pending = fake.control.join("pending-claim");
        fake.claims
            .lock()
            .unwrap()
            .extend([fenced("a9"), fenced("a9")]);
        let coordinator = coordinator(fake);
        fs::write(&pending, r#"{"key":"lost"}"#).unwrap();
        assert_eq!(coordinator.claim().unwrap(), Claimed::Retry);
        assert_eq!(
            claim_keys(&coordinator),
            [sent("lost", None), sent("lost", Some("a9"))]
        );
        // Otherwise the same exchange; only this report carries the field.
        let requests = claims_sent(&coordinator);
        let mut report = serde_json::to_value(&requests[1]).unwrap();
        assert_eq!(report["unrecorded"], "a9");
        report.as_object_mut().unwrap().remove("unrecorded");
        assert_eq!(report, serde_json::to_value(&requests[0]).unwrap());
        assert!(!pending.exists());
        // The next exchange claims under a new key and says nothing more.
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        let requests = claims_sent(&coordinator);
        assert_eq!(requests.len(), 3);
        assert_ne!(requests[2].claim_key, "lost");
        let next = serde_json::to_value(&requests[2]).unwrap();
        assert!(next.get("unrecorded").is_none(), "{next}");
    }

    #[test]
    fn an_unrecorded_report_is_sent_again_until_it_is_delivered_or_refused() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let pending = fake.control.join("pending-claim");
        *fake.unrecorded_error.lock().unwrap() = Some("transient");
        fake.claims.lock().unwrap().extend([
            fenced("a9"),
            fenced("a9"),
            fenced("a9"),
            fenced("b9"),
            fenced("b9"),
        ]);
        let coordinator = coordinator(fake);
        fs::write(&pending, r#"{"key":"lost"}"#).unwrap();
        // Unreachable: the key stays pending, and the next exchange is fenced and reports again.
        assert_eq!(coordinator.claim().unwrap(), Claimed::Retry);
        assert!(pending.exists());
        *coordinator.transport.unrecorded_error.lock().unwrap() = None;
        assert_eq!(coordinator.claim().unwrap(), Claimed::Retry);
        assert!(!pending.exists());
        assert_eq!(
            claim_keys(&coordinator),
            [
                sent("lost", None),
                sent("lost", Some("a9")),
                sent("lost", None),
                sent("lost", Some("a9")),
            ]
        );
        // An authority that predates the field refuses it: the key is settled as before.
        *coordinator.transport.unrecorded_error.lock().unwrap() = Some("permanent");
        fs::write(&pending, r#"{"key":"older"}"#).unwrap();
        assert_eq!(coordinator.claim().unwrap(), Claimed::Retry);
        assert!(!pending.exists());
        assert_eq!(
            claim_keys(&coordinator)[4..],
            [sent("older", None), sent("older", Some("b9"))]
        );
    }

    #[test]
    fn a_fenced_key_whose_attempt_the_ledger_held_reports_nothing_new() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let pending = fake.control.join("pending-claim");
        let coordinator = coordinator(fake);
        let replay = |attempt: &str| {
            let before = claims_sent(&coordinator).len();
            fs::write(&pending, r#"{"key":"pending"}"#).unwrap();
            coordinator
                .transport
                .claims
                .lock()
                .unwrap()
                .push_back(fenced(attempt));
            assert_eq!(coordinator.claim().unwrap(), Claimed::Retry);
            assert!(!pending.exists());
            claim_keys(&coordinator)[before..].to_vec()
        };
        // A crash after the ledger entry was written but before the key was settled: taken up at
        // start, the attempt ran and its report was published before the replay got through.
        launched(&coordinator, "a1", Some(&dead()));
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        assert!(!coordinator.ledger("a1").exists());
        assert!(coordinator.lock().ledger.is_empty());
        assert_eq!(replay("a1"), [sent("pending", None)]);
        // The ledger's file alone, or its entry alone.
        launched(&coordinator, "a2", None);
        assert_eq!(replay("a2"), [sent("pending", None)]);
        coordinator
            .lock()
            .ledger
            .insert("a3".into(), Entry::new(claim("a3"), Phase::Launched));
        assert_eq!(replay("a3"), [sent("pending", None)]);
        // An attempt none of them holds is reported.
        assert_eq!(
            replay("a4"),
            [sent("pending", None), sent("pending", Some("a4"))]
        );
    }

    #[test]
    fn drain_waits_for_unverified_profiles() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        fake.claims.lock().unwrap().push_back(ClaimResponse {
            claim: None,
            fenced: None,
            unverified: vec!["p-ok".into(), "p-bad".into(), "p-gone".into()],
        });
        let coordinator = Arc::new(coordinator(fake));
        assert_eq!(
            coordinator.coordinate().unwrap(),
            json!({"launched":0,"executor":"mindy"})
        );
        let calls = calls(&coordinator);
        assert_eq!(calls.len(), 3, "{calls:?}");
        let Call::Claim(first) = &calls[0] else {
            panic!("{calls:?}")
        };
        assert!(first.verified.is_empty() && first.refused.is_empty());
        assert!(matches!(&calls[1], Call::Hello(ids) if ids == &["p-ok", "p-bad", "p-gone"]));
        // Asked again at once, with every profile judged; only then is the queue empty.
        let Call::Claim(second) = &calls[2] else {
            panic!("{calls:?}")
        };
        assert_eq!(second.claim_key, first.claim_key);
        assert_eq!(second.verified, ["p-ok"]);
        assert_eq!(
            second.refused.keys().collect::<Vec<_>>(),
            ["p-bad", "p-gone"]
        );
        assert!(second.refused["p-bad"].contains("runner binary changed"));
        assert_eq!(second.engine.as_deref(), Some("e"));
    }

    /// Ten-byte cache entries named for digests, used one minute apart in the order given.
    fn cached(work: &Path, names: &[&str]) -> Vec<String> {
        let cas = work.join("cas");
        fs::create_dir_all(&cas).unwrap();
        let start = SystemTime::now() - Duration::from_secs(3600);
        names
            .iter()
            .zip(0..)
            .map(|(name, minute)| {
                let sha256 = crate::hash(name.as_bytes());
                let path = cas.join(&sha256);
                fs::write(&path, b"0123456789").unwrap();
                File::open(&path)
                    .unwrap()
                    .set_modified(start + Duration::from_secs(60 * minute))
                    .unwrap();
                sha256
            })
            .collect()
    }

    /// A ledger entry under `work` for an attempt consuming `digests`.
    fn consuming(work: &Path, attempt: &str, digests: &[&String]) {
        let mut claim = claim(attempt);
        claim.dependencies = vec![DependencyEvidence {
            action: "producer".into(),
            attempt: "produced".into(),
            artifacts: digests
                .iter()
                .zip(0..)
                .map(|(sha256, n)| {
                    let entry = ArtifactEntry {
                        sha256: (*sha256).clone(),
                        mode: 0o444,
                        bytes: 10,
                    };
                    (format!("dist/{n}"), entry)
                })
                .collect(),
        }];
        fs::create_dir_all(work.join("ledger")).unwrap();
        fs::write(
            work.join("ledger").join(format!("{attempt}.json")),
            serde_json::to_vec(&claim).unwrap(),
        )
        .unwrap();
    }

    fn cache(work: &Path) -> BTreeSet<String> {
        fs::read_dir(work.join("cas"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    #[test]
    fn cas_lru_evicts_oldest_unpinned_first() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path();
        let names = ["locked", "used", "started", "waiting", "newer", "newest"];
        let [locked, used, started, waiting, newer, newest] =
            <[String; 6]>::try_from(cached(work, &names)).unwrap();
        // `waiting` feeds an attempt whose command has not started; `started` one whose has.
        consuming(work, "a1", &[&waiting]);
        consuming(work, "a2", &[&started]);
        fs::create_dir_all(work.join("attempts/a2/control")).unwrap();
        fs::write(work.join("attempts/a2/control/command-started"), "").unwrap();
        let disk = Disk::new(work, 35, Duration::from_secs(3600));
        disk.touch(&used);
        // A digest being fetched or adopted is never evicted meanwhile.
        let lock = disk.lock(&locked);
        let held = lock.lock().unwrap();
        disk.evict(None).unwrap();
        assert_eq!(
            cache(work),
            BTreeSet::from([locked.clone(), used.clone(), waiting.clone()])
        );
        assert!(!cache(work).contains(&newer) && !cache(work).contains(&newest));
        drop(held);
        let disk = Disk::new(work, 20, Duration::from_secs(3600));
        disk.evict(None).unwrap();
        // Recency now comes from the file times alone: `used` was marked on disk.
        assert_eq!(cache(work), BTreeSet::from([used, waiting]));
    }

    #[test]
    fn free_disk_floor_blocks_claims_after_eviction() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let work = fake.root.clone();
        let coordinator = coordinator(fake);
        let [old, pinned, new] =
            <[String; 3]>::try_from(cached(&work, &["old", "pinned", "new"])).unwrap();
        consuming(&work, "a0", &[&pinned]);
        // A 1000-byte disk: other files use `other` bytes, the cache the rest of what is used.
        let other = Arc::new(std::sync::atomic::AtomicU64::new(970));
        let free_space: FreeSpace = {
            let (other, cas) = (other.clone(), work.join("cas"));
            Arc::new(move |_: &Path| {
                let cached: u64 = fs::read_dir(&cas)?
                    .map(|entry| Ok(entry?.metadata()?.len()))
                    .sum::<std::io::Result<u64>>()?;
                Ok(1000 - other.load(std::sync::atomic::Ordering::SeqCst) - cached)
            })
        };
        let disk = Arc::new(Disk::new(&work, u64::MAX, Duration::from_secs(3600)));
        let coordinator = coordinator.guarded(Guards {
            min_free: 25,
            loopback: None,
            disk: Some(disk),
            free_space,
            ..Guards::default()
        });
        // Eviction frees 20 bytes of the 25 needed: claims wait.
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        assert_eq!(cache(&work), BTreeSet::from([pinned.clone()]));
        assert!(!cache(&work).contains(&old) && !cache(&work).contains(&new));
        let declined = claims_sent(&coordinator);
        assert_eq!(declined.len(), 1);
        let reason = &declined[0].refused[authority::REFUSE_ALL];
        assert!(reason.contains("floor"), "{reason}");
        // The report claims nothing: a fresh key, never the pending one.
        assert!(
            !coordinator
                .transport
                .control()
                .join("pending-claim")
                .exists()
        );
        coordinator
            .transport
            .claims
            .lock()
            .unwrap()
            .push_back(ClaimResponse {
                claim: Some(claim("a1")),
                fenced: None,
                unverified: vec![],
            });
        other.store(900, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(coordinator.claim().unwrap(), Claimed::Attempt("a1".into()));
        let requests = claims_sent(&coordinator);
        assert_ne!(requests[1].claim_key, requests[0].claim_key);
        assert!(requests[1].refused.is_empty());
        assert_eq!(cache(&work), BTreeSet::from([pinned]));
    }

    #[test]
    fn a_claim_replayed_while_declining_is_reported_unrun() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        fs::create_dir_all(&fake.control).unwrap();
        // Written before a crash lost the response to it.
        fs::write(fake.control.join("pending-claim"), r#"{"key":"lost"}"#).unwrap();
        fake.claims.lock().unwrap().push_back(ClaimResponse {
            claim: Some(claim("a1")),
            fenced: None,
            unverified: vec![],
        });
        let coordinator = coordinator(fake).guarded(Guards {
            min_free: 1,
            free_space: Arc::new(|_: &Path| Ok(0)),
            ..Guards::default()
        });
        assert_eq!(coordinator.claim().unwrap(), Claimed::Attempt("a1".into()));
        let requests = claims_sent(&coordinator);
        assert_eq!(requests[0].claim_key, "lost");
        assert!(requests[0].refused[authority::REFUSE_ALL].contains("floor"));
        assert!(
            !coordinator
                .transport
                .control()
                .join("pending-claim")
                .exists()
        );
        // Nothing is fetched or launched; the attempt is reported drained and unrun.
        coordinator.materialize("a1");
        assert!(!coordinator.control("a1").join("claim.json").exists());
        coordinator.publish_queued().unwrap();
        let reports = completions(&coordinator);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, Outcome::NotStarted);
        assert!(reports[0].reason.as_deref().unwrap().contains("floor"));
        assert!(reports[0].drained);
        assert!(coordinator.lock().ledger.is_empty());
    }

    fn claims_sent(coordinator: &Coordinator<Fake>) -> Vec<ClaimRequest> {
        calls(coordinator)
            .into_iter()
            .filter_map(|call| match call {
                Call::Claim(request) => Some(request),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_undeletable_retained_entry_does_not_stop_the_purge() {
        // Review P2-2: a foreign-owned file inside one entry must not keep the others.
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path();
        let now = SystemTime::now();
        for name in ["a-foreign", "b-own"] {
            let directory = work.join("retained").join(name);
            fs::create_dir_all(&directory).unwrap();
            File::open(&directory)
                .unwrap()
                .set_modified(now - Duration::from_secs(100 * 3600))
                .unwrap();
        }
        let mut disk = Disk::new(work, u64::MAX, Duration::from_secs(72 * 3600));
        disk.remove = |path| {
            if path.ends_with("a-foreign") {
                bail!("Operation not permitted (os error 1)")
            }
            remove_tree(path)
        };
        assert_eq!(disk.purge_retained(now).unwrap(), ["b-own"]);
        assert_eq!(disk.stuck(), 1);
        assert!(work.join("retained/a-foreign").exists());
        // Retried later without erroring, and still counted for the operator.
        assert!(disk.purge_retained(now).unwrap().is_empty());
        assert_eq!(disk.stuck(), 1);
    }

    #[test]
    fn only_content_matching_its_digest_enters_the_cache() {
        // Review P3-8: a mismatched file is refused, so a cache hit can be trusted.
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path();
        fs::create_dir_all(work.join("cas")).unwrap();
        let disk = Disk::new(work, u64::MAX, Duration::from_secs(3600));
        let file = work.join("artifact");
        fs::write(&file, "content").unwrap();
        let wrong = "0".repeat(64);
        assert!(disk.adopt(&file, &wrong).is_err());
        assert!(!work.join("cas").join(&wrong).exists());
        let right = file_hash(&file).unwrap();
        disk.adopt(&file, &right).unwrap();
        assert!(work.join("cas").join(&right).exists());
    }

    #[test]
    fn transient_materialize_failures_are_retried() {
        // Review P2-1: a serve restart or full transfer permits are ridden out, not a red.
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        let mut left = 2;
        let fetched = coordinator.retrying(|| {
            if left > 0 {
                left -= 1;
                Err(TransportError::Transient(anyhow::anyhow!("busy")))
            } else {
                Ok(7)
            }
        });
        assert_eq!(fetched, Ok(7));
        let permanent: Result<u8, String> =
            coordinator.retrying(|| Err(TransportError::Permanent(anyhow::anyhow!("gone"))));
        assert!(permanent.is_err());
    }

    #[test]
    fn retained_dirs_are_purged_after_retention() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path();
        let now = SystemTime::now();
        for (name, hours) in [("old", 73), ("recent", 71)] {
            let directory = work.join("retained").join(name);
            fs::create_dir_all(directory.join("work/locked")).unwrap();
            fs::write(directory.join("work/locked/file"), "x").unwrap();
            // Jobs may leave read-only directories behind.
            fs::set_permissions(
                directory.join("work/locked"),
                fs::Permissions::from_mode(0o555),
            )
            .unwrap();
            File::open(&directory)
                .unwrap()
                .set_modified(now - Duration::from_secs(hours * 3600))
                .unwrap();
        }
        let disk = Disk::new(work, u64::MAX, Duration::from_secs(72 * 3600));
        assert_eq!(disk.purge_retained(now).unwrap(), ["old"]);
        assert!(!work.join("retained/old").exists());
        assert!(work.join("retained/recent/work/locked/file").exists());
        fs::set_permissions(
            work.join("retained/recent/work/locked"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }

    /// A local attempt as a finished job leaves it: job trees (one read-only) and the evidence.
    fn finished_attempt(root: &Path, id: &str) -> PathBuf {
        let directory = root.join("attempts").join(id);
        for tree in ["work/node_modules/locked", "scratch", "home"] {
            fs::create_dir_all(directory.join(tree)).unwrap();
        }
        fs::write(directory.join("work/node_modules/locked/file"), "x").unwrap();
        fs::set_permissions(
            directory.join("work/node_modules/locked"),
            fs::Permissions::from_mode(0o555),
        )
        .unwrap();
        fs::create_dir_all(directory.join("artifacts/dist")).unwrap();
        for evidence in [
            "output.log",
            "supervisor.log",
            "artifacts.json",
            "artifacts/dist/a",
        ] {
            fs::write(directory.join(evidence), "evidence").unwrap();
        }
        directory
    }

    fn evidence_only(directory: &Path) -> bool {
        ["work", "scratch", "home"]
            .iter()
            .all(|tree| !directory.join(tree).exists())
            && [
                "output.log",
                "supervisor.log",
                "artifacts.json",
                "artifacts/dist/a",
            ]
            .iter()
            .all(|evidence| directory.join(evidence).is_file())
    }

    #[test]
    fn a_local_attempt_keeps_only_its_evidence_once_recorded_and_drained() {
        // Storage (chief, #4204): every local attempt kept its whole checkout (813 GB on mauve).
        let temp = tempfile::tempdir().unwrap();
        let transport = LocalTransport::new(temp.path());
        let acknowledged = |id: &str, status: &str, drained: bool| CompleteResponse {
            attempt: id.into(),
            status: status.into(),
            state: Some("passed".into()),
            drained,
            replayed: false,
            drain_recorded: drained,
        };
        let done = finished_attempt(temp.path(), "done");
        transport
            .retire(&done, &acknowledged("done", "recorded", true))
            .unwrap();
        // Set aside at once (gate P3-2: the serial publisher never waits on a delete), gone soon.
        assert!(evidence_only(&done));
        let start = Instant::now();
        while done.join(DISCARD).exists() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "set-aside trees not deleted"
            );
            thread::sleep(Duration::from_millis(20));
        }
        // Cleanup may still need the checkout, and a fenced report was not recorded.
        for (id, status, drained) in [("draining", "recorded", false), ("fenced", "fenced", true)] {
            let directory = finished_attempt(temp.path(), id);
            transport
                .retire(&directory, &acknowledged(id, status, drained))
                .unwrap();
            assert!(
                directory.join("work/node_modules/locked/file").exists(),
                "{id}"
            );
            fs::set_permissions(
                directory.join("work/node_modules/locked"),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
    }

    #[test]
    fn the_hourly_maintenance_sweeps_and_reports_what_it_could_not_delete() {
        // Gate P3-2 on pw#7: maintain() runs the transport's sweep, and attempts it could not
        // clean show in the disk-floor refusal, as the remote path's retained directories do.
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path())).guarded(Guards {
            min_free: 1,
            free_space: Arc::new(|_: &Path| Ok(0)),
            ..Guards::default()
        });
        coordinator.maintain();
        assert!(
            !coordinator
                .declining(Some(0))
                .unwrap()
                .contains("job trees"),
            "within the hour"
        );
        coordinator.lock().maintained = Instant::now() - MAINTENANCE - Duration::from_secs(1);
        coordinator.maintain();
        let reason = coordinator.declining(Some(0)).unwrap();
        assert!(
            reason.contains("2 attempts' job trees could not be deleted"),
            "{reason}"
        );
    }

    #[test]
    fn the_worker_sweeps_job_trees_of_every_drained_attempt() {
        // Attempts finished before this version, or whose retire failed, are swept at start.
        let temp = tempfile::tempdir().unwrap();
        let store = Store::init(temp.path(), 4, 4096).unwrap();
        // Gate P2 (pw#7): the predicate guarding the first start's ≈785 GB delete is pinned row by
        // row. Lease expiry writes interrupted with drained=0 while its supervisor may still run.
        let kept = [
            ("live", "running", 0),
            ("claimed", "claimed", 0),
            ("expired", "interrupted", 0),
            ("red-undrained", "failed", 0),
            ("flagged", "running", 1),
        ];
        for (id, state, drained) in [("old", "passed", 1), ("red", "failed", 1)]
            .into_iter()
            .chain(kept)
        {
            store
                .db
                .execute(
                    "INSERT INTO attempts(id,action,state,drained,body) VALUES (?,?,?,?,'{}')",
                    rusqlite::params![id, "a", state, drained],
                )
                .unwrap();
            finished_attempt(temp.path(), id);
        }
        finished_attempt(temp.path(), "unknown");
        // A retire that set the trees aside but died before deleting them left `.discard`.
        store
            .db
            .execute(
                "INSERT INTO attempts(id,action,state,drained,body) VALUES ('aside','a','passed',1,'{}')",
                [],
            )
            .unwrap();
        let aside = finished_attempt(temp.path(), "aside");
        set_aside_job_trees(&aside).unwrap();
        let swept = sweep_job_trees(&store).unwrap();
        assert_eq!(swept, Some(json!({"attempts":3,"failed":[]})));
        // A sweep already running makes another skip rather than wait (gate P3-1 on pw#7).
        let held = SWEEPING.lock().unwrap();
        assert_eq!(sweep_job_trees(&store).unwrap(), None);
        drop(held);
        for id in ["old", "red", "aside"] {
            assert!(
                evidence_only(&temp.path().join("attempts").join(id)),
                "{id}"
            );
        }
        assert!(!aside.join(DISCARD).exists());
        // Unfinished or undrained attempts and a directory the store does not know keep everything.
        for id in kept.iter().map(|(id, ..)| *id).chain(["unknown"]) {
            let directory = temp.path().join("attempts").join(id);
            assert!(
                directory.join("work/node_modules/locked/file").exists(),
                "{id}"
            );
            fs::set_permissions(
                directory.join("work/node_modules/locked"),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
    }

    #[test]
    fn cgroup_limit_is_min_of_high_and_max_along_path() {
        let temp = tempfile::tempdir().unwrap();
        let sys = temp.path();
        let cgroup = "0::/user.slice/user-1000.slice/app.slice/patchweft.service\n";
        let limit = |path: &str, file: &str, value: &str| {
            let directory = sys.join(path);
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join(file), value).unwrap();
        };
        limit("user.slice", "memory.max", "max\n");
        limit("user.slice/user-1000.slice", "memory.max", "12884901888\n");
        limit(
            "user.slice/user-1000.slice/app.slice",
            "memory.high",
            "max\n",
        );
        let leaf = "user.slice/user-1000.slice/app.slice/patchweft.service";
        limit(leaf, "memory.max", "8589934592\n");
        limit(leaf, "memory.high", "max\n");
        assert_eq!(cgroup_memory_limit_mb(cgroup, sys), Some(8192));
        // memory.high counts like memory.max, and an ancestor's limit binds its descendants.
        limit(leaf, "memory.high", "7516192768\n");
        assert_eq!(cgroup_memory_limit_mb(cgroup, sys), Some(7168));
        limit("user.slice/user-1000.slice", "memory.high", "4294967296\n");
        assert_eq!(cgroup_memory_limit_mb(cgroup, sys), Some(4096));
        // Unlimited, cgroup v1 only, or no cgroup at all: nothing to report.
        assert_eq!(cgroup_memory_limit_mb("0::/user.slice\n", sys), None);
        assert_eq!(cgroup_memory_limit_mb("0::/\n", sys), None);
        assert_eq!(cgroup_memory_limit_mb("4:memory:/user.slice\n", sys), None);
        assert_eq!(cgroup_memory_limit_mb("", sys), None);
    }

    const ROOTLESS: &str = "/run/user/1000/dockerd-rootless";
    const TCP: &str = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:0947 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 31001 1 0000000000000000 100 0 0 10 0
   1: 3500007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   101        0 31002 1 0000000000000000 100 0 0 10 0
   2: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31003 1 0000000000000000 100 0 0 10 0
   3: 0100007F:B5F2 0100007F:0947 01 00000000:00000000 00:00000000 00000000  1000        0 31004 1 0000000000000000 20 4 30 10 -1
";
    const TCP6: &str = "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000001000000:0277 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31005 1 0000000000000000 100 0 0 10 0
   1: 00000000000000000000000000000000:0050 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31006 1 0000000000000000 100 0 0 10 0
";

    /// A host holding exactly `files`: listeners on 127.0.0.1:2375, 127.0.0.53:53 and [::1]:631,
    /// plus, with `flags`, a rootless daemon whose `rootlesskit` parent runs with them.
    fn host(flags: Option<&str>) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::from([
            ("/proc/net/tcp".into(), TCP.into()),
            ("/proc/net/tcp6".into(), TCP6.into()),
        ]);
        if let Some(flags) = flags {
            files.insert(Path::new(ROOTLESS).join("child_pid"), b"4242\n".to_vec());
            files.insert(
                "/proc/4242/stat".into(),
                b"4242 (exe (child)) S 4200 4242 4200 0 -1 4194560".to_vec(),
            );
            let cmdline = format!("/usr/bin/rootlesskit\0--net=slirp4netns\0{flags}\0dockerd\0");
            files.insert("/proc/4200/cmdline".into(), cmdline.into_bytes());
        }
        files
    }

    fn refusal(files: &BTreeMap<PathBuf, Vec<u8>>, ports: &[u16]) -> Option<String> {
        held_refusal(files, ports, &[])
    }

    /// As `refusal`, with the rootless daemon's own processes holding the sockets `held`.
    fn held_refusal(
        files: &BTreeMap<PathBuf, Vec<u8>>,
        ports: &[u16],
        held: &[u64],
    ) -> Option<String> {
        let read = |path: &Path| {
            files
                .get(path)
                .cloned()
                .ok_or_else(|| std::io::Error::from(ErrorKind::NotFound))
        };
        loopback_refusal(&read, &ports.iter().copied().collect(), &|_| {
            held.iter().copied().collect()
        })
    }

    #[test]
    fn wildcard_listeners_are_reachable_from_jobs_and_refuse_too() {
        // Gate P2-1 on pw#8: a native job reaches 0.0.0.0:22 at 127.0.0.1 with a loopback source,
        // so a service trusting loopback behind a wildcard bind (mindy's 8080/8443/9100) counts.
        let files = host(None);
        assert_eq!(
            refusal(&files, &[53, 631, 2375]).as_deref(),
            Some("wildcard listener 0.0.0.0:22 not allowlisted")
        );
        assert_eq!(
            refusal(&files, &[22, 53, 631, 2375]).as_deref(),
            Some("wildcard listener [::]:80 not allowlisted")
        );
        assert_eq!(refusal(&files, &[22, 53, 80, 631, 2375]), None);
        // Ports the rootless daemon publishes for job containers are held by its rootlesskit.
        assert_eq!(
            held_refusal(&files, &[53, 631, 2375], &[31003, 31006]),
            None
        );
    }

    /// A process listening on 127.0.0.1 (python3), and the inode of its socket.
    fn listening_child(parent_script: &str) -> (std::process::Child, u64) {
        let listener = "import socket,time\ns=socket.socket()\ns.bind(('127.0.0.1',0))\ns.listen()\nprint('ready', flush=True)\ntime.sleep(60)";
        let mut child = Command::new("/bin/bash")
            .arg("-c")
            .arg(parent_script)
            .arg("bash")
            .arg(listener)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            // Inherited stdio can be a socket; only the listener may be one.
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(child.stdout.as_mut().unwrap()),
            &mut ready,
        )
        .unwrap();
        assert_eq!(ready.trim(), "ready");
        let socket = std::iter::once(child.id())
            .chain(worker::descendants_of(&[child.id()]))
            .flat_map(|pid| {
                fs::read_dir(format!("/proc/{pid}/fd"))
                    .into_iter()
                    .flatten()
                    .flatten()
            })
            .find_map(|fd| {
                fs::read_link(fd.path())
                    .ok()?
                    .to_str()?
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .parse()
                    .ok()
            })
            .unwrap();
        (child, socket)
    }

    fn stop(mut child: std::process::Child) {
        for pid in worker::descendants_of(&[child.id()]) {
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Records `pid` as the live supervisor of `attempt` (ledger entry plus supervisor.json).
    fn supervising(root: &Path, control: &Path, attempt: &str, pid: u32) {
        fs::create_dir_all(control.join("ledger")).unwrap();
        fs::write(control.join("ledger").join(format!("{attempt}.json")), "{}").unwrap();
        let directory = root.join("attempts").join(attempt).join("control");
        fs::create_dir_all(&directory).unwrap();
        let start = worker::process_start(pid).unwrap();
        fs::write(
            directory.join("supervisor.json"),
            json!({"pid":pid,"start":start}).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn only_descendants_of_live_supervisors_own_their_listeners() {
        // Option B, gate P2-2 on pw#9: a real tree. The supervisor's child holds a listener (owned);
        // a listener outside the tree, in the supervisor's parent, is not.
        let temp = tempfile::tempdir().unwrap();
        let (root, control) = (temp.path().join("root"), temp.path().join("control"));
        let (supervisor, inside) = listening_child(r#"python3 -c "$1" & wait"#);
        supervising(&root, &control, "a1", supervisor.id());
        let live = live_supervisors(&root, &control);
        assert_eq!(
            live.iter().map(|s| s.pid).collect::<Vec<_>>(),
            [supervisor.id()]
        );
        assert!(sockets_held_by(&[supervisor.id()]).contains(&inside));
        let (outsider, outside) = listening_child(r#"exec python3 -c "$1""#);
        assert!(!sockets_held_by(&[supervisor.id()]).contains(&outside));
        // A reused pid (another start time) supervises nothing.
        fs::write(
            root.join("attempts/a1/control/supervisor.json"),
            json!({"pid":supervisor.id(),"start":"0"}).to_string(),
        )
        .unwrap();
        assert!(live_supervisors(&root, &control).is_empty());
        stop(supervisor);
        stop(outsider);
    }

    #[test]
    fn the_claim_loop_exempts_the_listeners_of_its_live_supervisors() {
        // pw#9's PASS P3-1: declining() must hand the check this executor's live supervisors. Given
        // none, mauve would refuse every claim while a Vitest shard's server listens. Real /proc,
        // allowing every listener on this host except the probe's own.
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let (root, control) = (fake.root.clone(), fake.control.clone());
        let coordinator = coordinator(fake);
        let (supervisor, socket) = listening_child(r#"python3 -c "$1" & wait"#);
        let others = || -> BTreeSet<u16> {
            reachable_listeners(&|path| fs::read(path))
                .unwrap()
                .into_iter()
                .filter(|(_, inode)| *inode != socket)
                .map(|(listener, _)| listener.port())
                .collect()
        };
        let coordinator = coordinator.guarded(Guards {
            loopback: Some(others()),
            ..Guards::default()
        });
        let refused = coordinator.declining(None);
        assert!(
            refused
                .as_deref()
                .is_some_and(|reason| reason.ends_with(" not allowlisted")),
            "an unrecorded process's listener is a host service: {refused:?}"
        );
        supervising(&root, &control, "a1", supervisor.id());
        // Tests running alongside open listeners of their own between computing the allow list and
        // the check; recompute it a few times. The probe's own listener is never allowed, so an
        // unwired `declining()` still refuses every time.
        let (mut coordinator, mut declined) = (coordinator, Some(String::new()));
        for _ in 0..10 {
            coordinator = coordinator.guarded(Guards {
                loopback: Some(others()),
                ..Guards::default()
            });
            declined = coordinator.declining(None);
            if declined.is_none() {
                break;
            }
        }
        assert_eq!(declined, None);
        stop(supervisor);
    }

    #[test]
    fn owned_listeners_are_walked_again_when_the_supervisors_change() {
        // Gate P2-1 on pw#9: a socket is not its attempt's once its supervisor is gone (an orphan
        // keeps it open), so the cache is keyed on the live supervisors, and a new listener is
        // looked up too.
        let cache = Mutex::new((Vec::new(), BTreeSet::new()));
        let walks = std::cell::Cell::new(0);
        let one = vec![SupervisorIdentity {
            pid: 10,
            start: "a".into(),
        }];
        let held = |roots: &[u32]| {
            walks.set(walks.get() + 1);
            if roots.is_empty() {
                BTreeSet::new()
            } else {
                BTreeSet::from([7])
            }
        };
        assert_eq!(
            owned_cached(&cache, &BTreeSet::from([7]), &one, &held),
            BTreeSet::from([7])
        );
        assert_eq!(
            owned_cached(&cache, &BTreeSet::from([7]), &one, &held),
            BTreeSet::from([7])
        );
        assert_eq!(walks.get(), 1);
        // The supervisor died: its orphan's socket is owned no longer (no supervisor, no walk).
        assert_eq!(
            owned_cached(&cache, &BTreeSet::from([7]), &[], &held),
            BTreeSet::new()
        );
        assert_eq!(walks.get(), 1);
        // The same supervisors again are walked again, and an unknown listener beside them too.
        assert_eq!(
            owned_cached(&cache, &BTreeSet::from([7]), &one, &held),
            BTreeSet::from([7])
        );
        assert_eq!(walks.get(), 2);
        assert_eq!(
            owned_cached(&cache, &BTreeSet::from([7, 9]), &one, &held),
            BTreeSet::from([7])
        );
        assert_eq!(walks.get(), 3);
    }

    #[test]
    fn any_wsl_kernel_refuses_because_windows_listeners_cannot_be_listed() {
        // Re-gate P2 on pw#8 (robin): under mirrored networking Windows' SMB, RDP and others answer at
        // 127.0.0.1 but are not in /proc/net/tcp. pw#8's PASS P3-1: requiring `loopback0` failed open
        // on a renamed interface, a masked /sys or WSL1, so any WSL kernel refuses.
        const WSL: &str = "WSL: Windows listeners can answer at 127.0.0.1 without being listed; run executors in a Linux VM";
        let mut files = host(None);
        let release = b"6.18.54.1-microsoft-standard-WSL2\n".to_vec();
        files.insert("/proc/sys/kernel/osrelease".into(), release);
        assert_eq!(
            refusal(&files, &[22, 53, 80, 631, 2375]).as_deref(),
            Some(WSL),
            "WSL with NAT networking and no loopback0"
        );
        // A custom WSL kernel without `microsoft` in its release still exposes the interop entry.
        files.insert(
            "/proc/sys/kernel/osrelease".into(),
            b"6.6.87-custom\n".to_vec(),
        );
        files.insert(
            "/proc/sys/fs/binfmt_misc/WSLInterop".into(),
            b"enabled\n".to_vec(),
        );
        assert_eq!(
            refusal(&files, &[22, 53, 80, 631, 2375]).as_deref(),
            Some(WSL)
        );
        // loopback0 alone, outside WSL, is not WSL.
        files.remove(Path::new("/proc/sys/fs/binfmt_misc/WSLInterop"));
        files.insert(
            "/proc/sys/kernel/osrelease".into(),
            b"6.8.0-136-generic\n".to_vec(),
        );
        files.insert("/sys/class/net/loopback0/type".into(), b"772\n".to_vec());
        assert_eq!(refusal(&files, &[22, 53, 80, 631, 2375]), None);
    }

    #[test]
    fn a_v4_mapped_wildcard_counts_as_a_wildcard() {
        // Re-gate P3-2 on pw#8: [::ffff:0.0.0.0]:8080 listens on every IPv4 address.
        let mut files = host(None);
        files.insert(
            "/proc/net/tcp6".into(),
            format!("{}   0: 0000000000000000FFFF000000000000:1F90 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31007 1 0000000000000000 100 0 0 10 0\n",
                    TCP6.lines().next().unwrap().to_owned() + "\n")
            .into_bytes(),
        );
        assert_eq!(
            refusal(&files, &[22, 53, 2375]).as_deref(),
            Some("wildcard listener [::ffff:0.0.0.0]:8080 not allowlisted")
        );
    }

    #[test]
    fn only_a_rootlesskit_parent_of_child_pid_exempts_its_tree() {
        // Re-gate P3-1 on pw#8: a reused pid or a garbage record must never exempt an unrelated
        // tree (systemd --user, say).
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("dockerd-rootless");
        fs::create_dir_all(&state).unwrap();
        // The impostor's tree holds a real listener, so a wrong exemption would show.
        let listener = "import socket,time\ns=socket.socket()\ns.bind(('0.0.0.0',0))\ns.listen()\nprint('ready', flush=True)\ntime.sleep(60)";
        let mut impostor = Command::new("/bin/bash")
            .arg("-c")
            .arg(r#"exec -a notrootlesskit /bin/bash -c 'sleep 60 & echo $! > "$0/child_pid"; python3 -c "$1" & wait' "$1" "$2""#)
            .arg("bash")
            .arg(&state)
            .arg(listener)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(impostor.stdout.as_mut().unwrap()),
            &mut ready,
        )
        .unwrap();
        assert_eq!(ready.trim(), "ready");
        assert!(
            !sockets_held_by(&[impostor.id()]).is_empty(),
            "the impostor holds a listener"
        );
        assert!(
            rootless_sockets(Some(&state)).is_empty(),
            "a parent not named rootlesskit"
        );
        for garbage in ["abc", "", "4294967295", "1"] {
            fs::write(state.join("child_pid"), garbage).unwrap();
            assert!(
                rootless_sockets(Some(&state)).is_empty(),
                "child_pid {garbage:?}"
            );
        }
        for pid in worker::descendants_of(&[impostor.id()]) {
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
        impostor.kill().unwrap();
        let _ = impostor.wait();
    }

    #[test]
    fn sockets_of_the_rootless_daemon_are_found_by_holder() {
        // A stand-in rootlesskit (its child named by child_pid) holding a published port.
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("dockerd-rootless");
        fs::create_dir_all(&state).unwrap();
        let listener = "import socket,time\ns=socket.socket()\ns.bind(('0.0.0.0',0))\ns.listen()\nprint('ready', flush=True)\ntime.sleep(60)";
        let mut daemon = Command::new("/bin/bash")
            .arg("-c")
            .arg(r#"exec -a rootlesskit /bin/bash -c 'sleep 60 & echo $! > "$0/child_pid"; python3 -c "$1" & wait' "$1" "$2""#)
            .arg("bash")
            .arg(&state)
            .arg(listener)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            // Inherited stdio can be a socket; only the listener may be one.
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(daemon.stdout.as_mut().unwrap()),
            &mut ready,
        )
        .unwrap();
        assert_eq!(ready.trim(), "ready");
        // The listener is the stand-in's python3 child, a descendant of "rootlesskit".
        let python = worker::descendants_of(&[daemon.id()])
            .into_iter()
            .find(|pid| {
                fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|c| c.starts_with(b"python3"))
            })
            .unwrap();
        let socket: u64 = fs::read_dir(format!("/proc/{python}/fd"))
            .unwrap()
            .filter_map(|fd| {
                fs::read_link(fd.ok()?.path())
                    .ok()?
                    .to_str()?
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .parse()
                    .ok()
            })
            .next()
            .unwrap();
        assert!(rootless_sockets(Some(&state)).contains(&socket));
        // An unidentified daemon exempts nothing.
        assert!(rootless_sockets(None).is_empty());
        assert!(rootless_sockets(Some(&temp.path().join("absent"))).is_empty());
        for pid in worker::descendants_of(&[daemon.id()]) {
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
        daemon.kill().unwrap();
        let _ = daemon.wait();
    }

    #[test]
    fn loopback_check_refuses_every_unlisted_loopback_listener() {
        let files = host(None);
        assert_eq!(
            refusal(&files, &[53]).as_deref(),
            Some("loopback listener 127.0.0.1:2375 not allowlisted")
        );
        assert_eq!(
            refusal(&files, &[22, 53, 2375]).as_deref(),
            Some("loopback listener [::1]:631 not allowlisted")
        );
        // Established connections are not listeners; wildcard ones count (see the next test).
        assert_eq!(refusal(&files, &[22, 53, 80, 631, 2375]), None);
    }

    #[test]
    fn loopback_check_holds_even_when_docker_hides_host_loopback() {
        // Follow-up 1 (#4204): jobs run natively, so a daemon started with --disable-host-loopback
        // (mindy) leaves 127.0.0.1:2375 just as reachable; it returned early before.
        for flags in [
            "--disable-host-loopback",
            "--disable-host-loopback=true",
            "--port-driver=builtin",
        ] {
            assert_eq!(
                refusal(&host(Some(flags)), &[53]).as_deref(),
                Some("loopback listener 127.0.0.1:2375 not allowlisted"),
                "{flags}"
            );
        }
    }

    #[test]
    fn loopback_check_fails_closed_when_listeners_cannot_be_listed() {
        let mut files = host(None);
        files.remove(Path::new("/proc/net/tcp"));
        assert!(
            refusal(&files, &[53, 631, 2375])
                .unwrap()
                .contains("/proc/net/tcp")
        );
        // An IPv6-less host has no tcp6 table, which is no refusal.
        let mut files = host(None);
        files.remove(Path::new("/proc/net/tcp6"));
        assert_eq!(refusal(&files, &[22, 53, 2375]), None);
    }
}
