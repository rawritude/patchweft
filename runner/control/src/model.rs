//! The types a coordinator and its runner exchange through an attempt's control files, and the
//! job definitions they carry. The runner (`patchweft-runner`) and the scheduler both build on
//! them; any change here changes the runner's bytes, so it is a runner release.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    NotStarted,
    Lost,
    Undeliverable,
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
