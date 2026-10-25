use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub id: String,
    pub project: String,
    pub commit: String,
    pub tree: String,
    pub parents: Vec<String>,
    pub owner: String,
    pub message: String,
    pub created: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workspace {
    pub id: String,
    pub project: String,
    pub base: String,
    pub tip: String,
    pub owner: String,
    pub path: PathBuf,
}

fn default_slots() -> u32 {
    1
}
fn default_memory() -> u32 {
    1024
}
fn default_timeout() -> u64 {
    1800
}
fn yes() -> bool {
    true
}
fn is_true(value: &bool) -> bool {
    *value
}

/// Version tag of the frozen job recipe builder. A builder change needs a new tag.
pub const JOB_RECIPE: &str = "patchweft.job/1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Reuse {
    /// Share outstanding requests; a completed observation is not a new observation.
    #[default]
    Inflight,
    /// Explicitly audited hermetic action, with the complete commit/history as input.
    Exact,
    /// Explicitly audited files-only action; every input must be listed.
    Files,
    /// Keyed on the files the project declares the job reads (its `inputs_file`); an earlier
    /// pass with the same declared inputs is reused. Undeclared jobs behave as `inflight`.
    Declared,
    /// Each request is physically independent.
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Report {
    /// "command", "vitest", "junit", or "node-tap".
    pub kind: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub minimum: u64,
    #[serde(default)]
    pub allow_skips: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub name: String,
    /// Exact argv. Shell expansion only happens when the profile explicitly invokes a shell.
    pub command: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default = "default_slots")]
    pub slots: u32,
    #[serde(default = "default_memory")]
    pub memory_mb: u32,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub reuse: Reuse,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub audit: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    /// Optional bounded finalizer for resources outside the process tree, such as owned containers.
    #[serde(default)]
    pub cleanup: Vec<String>,
    #[serde(default)]
    pub report: Report,
    /// False makes `slots` a pure admission reservation: no `PATCHWEFT_SLOTS`, not in identity.
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub export_slots: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub certifying: bool,
    /// Absolute local executable paths, hashed when the profile is registered and before each run.
    pub tools: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub jobs: Vec<Job>,
    /// The revision's declarations file (`patchweft.inputs/1`) for `declared` jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredProfile {
    pub id: String,
    pub definition: Profile,
    pub tools: BTreeMap<String, String>,
    pub engine: String,
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Queued,
    Running,
    Passed,
    Failed,
    Interrupted,
    Canceled,
}

impl State {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reservation {
    pub slots: u32,
    pub memory_mb: u32,
}

impl Reservation {
    pub fn max(self, other: Self) -> Self {
        Self {
            slots: self.slots.max(other.slots),
            memory_mb: self.memory_mb.max(other.memory_mb),
        }
    }
}

impl From<&Job> for Reservation {
    fn from(job: &Job) -> Self {
        Self {
            slots: job.slots,
            memory_mb: job.memory_mb,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionIdentity {
    pub version: String,
    pub recipe: String,
    pub class: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Origin {
    pub owner: String,
    pub context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub id: String,
    pub key: String,
    pub revision: String,
    pub profile: String,
    pub job: String,
    pub dependencies: Vec<String>,
    pub created: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ActionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<Reservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    /// A declared job's passed key, exported as `PATCHWEFT_INPUTS`. Absent when it has none (the
    /// fail-safe path, or a dependency without one) and for every other policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_inputs: Option<String>,
    /// The base commit a declared job's `base_paths` were keyed at, exported as `PATCHWEFT_BASE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_base: Option<String>,
}

/// A declared job's earlier pass that a candidate requires instead of a new action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reused {
    pub action: String,
    /// The candidate the action was created for; empty for a direct request.
    pub from_candidate: String,
    pub from_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub id: String,
    pub revision: String,
    pub profile: String,
    pub required: BTreeMap<String, String>,
    pub prerequisites: Vec<String>,
    pub owner: String,
    pub created: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_from: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub not_retained: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub reused: BTreeMap<String, Reused>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub id: String,
    pub action: String,
    pub worker_pid: u32,
    pub worker_start: String,
    pub supervisor_pid: Option<u32>,
    pub supervisor_start: Option<String>,
    pub slots: u32,
    pub memory_mb: u32,
    pub created: f64,
    pub started: Option<f64>,
    pub ended: Option<f64>,
    pub exit_code: Option<i32>,
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// `local` or a remote executor id. Absent on attempts claimed before executors existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    /// Fencing token: only its holder may renew or complete the attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<String>,
    /// API-host wall clock, the same clock as `now()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_key: Option<String>,
    /// sha256 of the accepted completion, so a replayed completion is recognized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutorRecord {
    pub id: String,
    /// None for `local`, whose capacity is the store's settings.
    #[serde(default)]
    pub slots: Option<u32>,
    #[serde(default)]
    pub memory_mb: Option<u32>,
    pub lease_seconds: u64,
    #[serde(default)]
    pub placement: Placement,
    pub created: f64,
    #[serde(default)]
    pub observed: Observed,
    /// The train-first hold this executor is keeping, if any: shown so a hold is never mistaken
    /// for draining for the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holding: Option<Holding>,
}

/// Capacity an executor keeps for prioritized jobs waiting on a running dependency.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Holding {
    /// The highest priority among the jobs held for.
    pub priority: i64,
    pub slots: u32,
    pub memory_mb: u32,
}

/// Operator configuration read only when claiming; never part of action identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// Job names; None admits every job.
    #[serde(default)]
    pub allow: Option<BTreeSet<String>>,
    #[serde(default)]
    pub deny: BTreeSet<String>,
}

impl Placement {
    pub fn permits(&self, job: &str) -> bool {
        !self.deny.contains(job) && self.allow.as_ref().is_none_or(|allow| allow.contains(job))
    }
}

/// What the executor's coordinator last reported, for operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct Observed {
    pub last_seen: Option<f64>,
    pub verified: BTreeSet<String>,
    /// Profile id -> runtime verification error.
    pub refused: BTreeMap<String, String>,
    pub memory_limit_mb: Option<u32>,
    pub disk_free_mb: Option<u64>,
    pub host: Option<String>,
    /// sha256 of the coordinator's binary.
    pub engine: Option<String>,
    /// How the coordinator checks loopback: "off" or "allow [ports]" (gate P3-1 on pw#8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loopback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactEntry {
    pub sha256: String,
    pub mode: u32,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobRef {
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionRef {
    pub id: String,
    pub project: String,
    pub commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Grant {
    pub id: String,
    pub lease: String,
    pub lease_expires: f64,
    pub slots: u32,
    pub memory_mb: u32,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DependencyEvidence {
    pub action: String,
    pub attempt: String,
    pub artifacts: BTreeMap<String, ArtifactEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claim {
    pub attempt: Grant,
    pub action: Action,
    pub profile: RegisteredProfile,
    pub revision: RevisionRef,
    pub dependencies: Vec<DependencyEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HelloRequest {
    #[serde(default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub executor: String,
    pub lease_seconds: u64,
    pub profiles: Vec<RegisteredProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    pub claim_key: String,
    /// Profiles whose runtime the coordinator verified; the only ones it may be handed.
    #[serde(default)]
    pub verified: Vec<String>,
    #[serde(default)]
    pub refused: BTreeMap<String, String>,
    #[serde(default)]
    pub memory_limit_mb: Option<u32>,
    #[serde(default)]
    pub disk_free_mb: Option<u64>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub loopback: Option<String>,
    /// The attempt this key was just reported fenced for, which the coordinator never recorded:
    /// nothing of it ran on that host. Sent only then, so other claims serialize as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrecorded: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub claim: Option<Claim>,
    pub fenced: Option<String>,
    pub unverified: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SupervisorIdentity {
    pub pid: u32,
    pub start: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseReport {
    pub attempt: String,
    pub lease: String,
    #[serde(default)]
    pub started: Option<SupervisorIdentity>,
    #[serde(default)]
    pub stopping: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewRequest {
    pub leases: Vec<LeaseReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LeaseStatus {
    pub attempt: String,
    /// "ok" or "fenced".
    pub status: String,
    pub stop: Option<String>,
    pub registered: bool,
    pub lease_expires: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenewResponse {
    pub lease_seconds: u64,
    pub leases: Vec<LeaseStatus>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    NotStarted,
    Lost,
    Undeliverable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteRequest {
    pub attempt: String,
    pub lease: String,
    pub outcome: Outcome,
    #[serde(default)]
    pub reason: Option<String>,
    pub exit_code: i32,
    pub evidence: serde_json::Value,
    pub drained: bool,
    #[serde(default)]
    pub log: Option<BlobRef>,
    #[serde(default)]
    pub cleanup_log: Option<BlobRef>,
    #[serde(default)]
    pub artifacts: BTreeMap<String, ArtifactEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompleteResponse {
    pub attempt: String,
    /// "recorded" or "fenced".
    pub status: String,
    pub state: Option<String>,
    pub drained: bool,
    pub replayed: bool,
    pub drain_recorded: bool,
}

/// Version of the control files a coordinator and its supervisor exchange.
pub const CONTROL_VERSION: u32 = 1;

/// `control/claim.json`: everything a supervisor needs, written by its coordinator. The
/// supervisor reads nothing else from the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimFile {
    pub version: u32,
    pub executor: String,
    /// `PATCHWEFT_STATE` for the job: the store root locally, the work directory remotely.
    pub root: PathBuf,
    pub attempt: Grant,
    pub action: Action,
    pub profile: RegisteredProfile,
    pub revision: RevisionRef,
    /// A repository holding `revision.commit`.
    pub repository: PathBuf,
    pub dependencies: Vec<DependencyFile>,
    /// `{"seconds":f64,"bytes":u64}` the coordinator spent fetching inputs before launch.
    pub transfer: serde_json::Value,
}

/// One dependency artifact and the local file the supervisor copies it from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DependencyFile {
    pub action: String,
    pub attempt: String,
    pub name: String,
    pub sha256: String,
    pub mode: u32,
    pub bytes: u64,
    pub path: PathBuf,
}

/// `control/result.json`: the supervisor's final word, published by its coordinator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SupervisorResult {
    pub version: u32,
    /// `Completed` or `NotStarted`.
    pub outcome: Outcome,
    pub reason: Option<String>,
    pub exit_code: i32,
    pub evidence: serde_json::Value,
    pub drained: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_attempt_body_round_trips_byte_identically() {
        // Bodies written before executors existed: a finished attempt and an older unregistered one.
        let finished = r#"{"id":"a1","action":"b1","worker_pid":4242,"worker_start":"987654","supervisor_pid":4243,"supervisor_start":"987660","slots":2,"memory_mb":1536,"created":1792511574.9006627,"started":1792511575.1,"ended":1792511590.25,"exit_code":0,"result":{"artifacts":{},"exit_code":0,"reason":null},"timeout_seconds":60}"#;
        let claimed = r#"{"id":"a2","action":"b2","worker_pid":4242,"worker_start":"987654","supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1024,"created":1.5,"started":null,"ended":null,"exit_code":null,"result":null}"#;
        for body in [finished, claimed] {
            let attempt: Attempt = serde_json::from_str(body).unwrap();
            assert_eq!(
                (attempt.executor.as_deref(), attempt.lease.as_deref()),
                (None, None)
            );
            assert_eq!(attempt.lease_expires, None);
            assert_eq!(
                (attempt.claim_key, attempt.completion, attempt.stop),
                (None, None, None)
            );
            let attempt: Attempt = serde_json::from_str(body).unwrap();
            assert_eq!(serde_json::to_string(&attempt).unwrap(), body);
        }
        let mut attempt: Attempt = serde_json::from_str(claimed).unwrap();
        attempt.executor = Some("mindy".into());
        attempt.lease = Some("l".into());
        attempt.lease_expires = Some(2.5);
        attempt.claim_key = Some("k".into());
        let text = serde_json::to_string(&attempt).unwrap();
        assert!(
            text.ends_with(
                r#""result":null,"executor":"mindy","lease":"l","lease_expires":2.5,"claim_key":"k"}"#
            ),
            "{text}"
        );
        let reread: Attempt = serde_json::from_str(&text).unwrap();
        assert_eq!(serde_json::to_string(&reread).unwrap(), text);
    }
}
