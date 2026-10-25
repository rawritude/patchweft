//! `_supervise` runs from its attempt's control files alone: a temp root with a git fixture
//! and a hand-written `claim.json`, never a `state.sqlite`. The supervisor runs as its own
//! process, as in production: it adopts and signals every descendant of its process, which
//! inside the test harness would include other tests' children.
use patchweft::{
    engine, file_hash,
    model::{
        Action, ActionIdentity, CONTROL_VERSION, ClaimFile, DependencyFile, Grant, JOB_RECIPE, Job,
        Outcome, Profile, RegisteredProfile, Reservation, RevisionRef, SupervisorResult,
    },
    unique,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

const AW: &str = env!("CARGO_BIN_EXE_patchweft");

fn engine_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| file_hash(Path::new(AW)).unwrap())
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(repo)
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@local.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

struct Lab {
    _temp: tempfile::TempDir,
    /// `PATCHWEFT_STATE`; holds attempts but no store.
    root: PathBuf,
    repo: PathBuf,
    commit: String,
}

/// Kills a supervisor a failed assertion left behind.
struct Supervisor(Child);

impl Drop for Supervisor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Lab {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let repo = base.join("source");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("README"), "fixture\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        let root = base.join("root");
        fs::create_dir_all(root.join("attempts")).unwrap();
        Self {
            _temp: temp,
            root,
            repo,
            commit,
        }
    }

    /// A claim for one `sh -c SCRIPT` job; `cleanup` adds an external cleanup script.
    fn claim(&self, script: &str, cleanup: Option<&str>, timeout_seconds: u64) -> ClaimFile {
        let mut job = json!({"name":"job","command":["sh","-c",script],
                             "report":{"kind":"command"},"timeout_seconds":timeout_seconds});
        if let Some(cleanup) = cleanup {
            job["cleanup"] = json!(["sh", "-c", cleanup]);
        }
        let job: Job = serde_json::from_value(job).unwrap();
        let definition = Profile {
            version: 1,
            name: "fixture".into(),
            certifying: false,
            tools: BTreeMap::from([("sh".into(), PathBuf::from("/usr/bin/sh"))]),
            environment: BTreeMap::new(),
            jobs: vec![job],
            inputs_file: None,
        };
        let profile = RegisteredProfile {
            id: "f-1".into(),
            tools: engine::tool_identity(&definition).unwrap(),
            definition,
            engine: engine_hash().into(),
            platform: engine::platform(),
        };
        self.claim_for(profile, timeout_seconds)
    }

    fn claim_for(&self, profile: RegisteredProfile, timeout_seconds: u64) -> ClaimFile {
        let recipe = engine::recipes_for(&profile, JOB_RECIPE).unwrap()["job"].clone();
        let id = unique();
        ClaimFile {
            version: CONTROL_VERSION,
            executor: "mindy".into(),
            root: self.root.clone(),
            attempt: Grant {
                id: id.clone(),
                lease: "lease".into(),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds,
            },
            action: Action {
                id: "act".into(),
                key: "key".into(),
                revision: "rev".into(),
                profile: profile.id.clone(),
                job: "job".into(),
                dependencies: vec![],
                created: 0.0,
                identity: Some(ActionIdentity {
                    version: JOB_RECIPE.into(),
                    recipe,
                    class: "exploratory".into(),
                    timeout_seconds,
                }),
                reservation: Some(Reservation {
                    slots: 1,
                    memory_mb: 512,
                }),
                origin: None,
                declared_inputs: None,
                declared_base: None,
            },
            profile,
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit: self.commit.clone(),
            },
            repository: self.repo.clone(),
            dependencies: vec![],
            transfer: json!({"seconds":0.0,"bytes":0}),
        }
    }

    fn directory(&self, claim: &ClaimFile) -> PathBuf {
        self.root.join("attempts").join(&claim.attempt.id)
    }

    fn control(&self, claim: &ClaimFile) -> PathBuf {
        self.directory(claim).join("control")
    }

    /// Writes `claim.json` the way a coordinator does before it spawns the supervisor.
    fn write(&self, claim: &ClaimFile) {
        fs::create_dir_all(self.control(claim)).unwrap();
        fs::write(
            self.control(claim).join("claim.json"),
            serde_json::to_vec(claim).unwrap(),
        )
        .unwrap();
    }

    fn register(&self, claim: &ClaimFile) {
        fs::write(self.control(claim).join("registered"), "").unwrap();
    }

    fn stop(&self, claim: &ClaimFile, reason: &str) {
        fs::write(
            self.control(claim).join("stop"),
            json!({"reason":reason}).to_string(),
        )
        .unwrap();
    }

    /// Spawns `_supervise` as the coordinator does: cleared environment, own process group.
    /// `--state` names the root, which has no store: a supervisor must never need one.
    fn spawn(&self, claim: &ClaimFile) -> Supervisor {
        let log = fs::File::create(self.directory(claim).join("supervisor.log")).unwrap();
        Supervisor(
            Command::new(AW)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .arg("--state")
                .arg(&self.root)
                .arg("_supervise")
                .arg(&self.root)
                .arg(&claim.attempt.id)
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    }

    /// Waits for `result.json`, then for the supervisor to exit successfully.
    fn result(&self, claim: &ClaimFile, mut supervisor: Supervisor) -> SupervisorResult {
        let path = self.control(claim).join("result.json");
        let started = Instant::now();
        while !path.exists() {
            if let Some(status) = supervisor.0.try_wait().unwrap() {
                panic!(
                    "supervisor exited without a result ({status}): {}",
                    fs::read_to_string(self.directory(claim).join("supervisor.log"))
                        .unwrap_or_default()
                );
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "no result.json"
            );
            thread::sleep(Duration::from_millis(50));
        }
        assert!(supervisor.0.wait().unwrap().success());
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }
}

#[test]
fn supervisor_runs_nothing_before_registration() {
    let lab = Lab::new();
    let claim = lab.claim("touch \"$PATCHWEFT_STATE/ran\"", None, 60);
    lab.write(&claim);
    let mut supervisor = lab.spawn(&claim);
    thread::sleep(Duration::from_millis(700));
    assert!(
        supervisor.0.try_wait().unwrap().is_none(),
        "the supervisor waits for registration"
    );
    // Not the command, not its marker, not even the checkout.
    assert!(!lab.root.join("ran").exists());
    assert!(!lab.control(&claim).join("command-started").exists());
    assert!(!lab.directory(&claim).join("work").exists());
    assert!(!lab.control(&claim).join("result.json").exists());
    lab.register(&claim);
    let result = lab.result(&claim, supervisor);
    assert_eq!(
        (result.outcome, result.exit_code, result.drained),
        (Outcome::Completed, 0, true)
    );
    assert_eq!(result.reason, None);
    assert!(lab.root.join("ran").exists());
    assert_eq!(result.evidence["exit_code"], 0);
    assert_eq!(result.evidence["executor"], "mindy");
    assert_eq!(result.evidence["profile"], "f-1");
    assert_eq!(result.evidence["budget"]["timeout_seconds"], 60);
}

#[test]
fn supervisor_stop_before_registration_is_not_started_and_drained() {
    let lab = Lab::new();
    let claim = lab.claim(
        "touch \"$PATCHWEFT_STATE/ran\"",
        Some("touch \"$PATCHWEFT_STATE/cleaned\""),
        60,
    );
    lab.write(&claim);
    let supervisor = lab.spawn(&claim);
    thread::sleep(Duration::from_millis(300));
    lab.stop(&claim, "no-requesters");
    let result = lab.result(&claim, supervisor);
    assert_eq!(
        result,
        SupervisorResult {
            version: 1,
            outcome: Outcome::NotStarted,
            reason: Some("no-requesters".into()),
            exit_code: 125,
            evidence: result.evidence.clone(),
            drained: true,
        }
    );
    assert_eq!(result.evidence["executor"], "mindy");
    // Nothing of the job ran, so there is nothing to clean up.
    assert!(!lab.root.join("ran").exists());
    assert!(!lab.root.join("cleaned").exists());
    assert!(!lab.control(&claim).join("command-started").exists());
    assert!(!lab.directory(&claim).join("work").exists());

    // A stop already present at start wins over a registration written with it.
    let claim = lab.claim("touch \"$PATCHWEFT_STATE/ran\"", None, 60);
    lab.write(&claim);
    lab.stop(&claim, "fenced");
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.reason.as_deref(), result.drained),
        (Outcome::NotStarted, Some("fenced"), true)
    );
    assert!(!lab.root.join("ran").exists());
}

#[test]
fn supervisor_never_opens_a_store() {
    let lab = Lab::new();
    let claim = lab.claim(
        "test ! -e \"$PATCHWEFT_STATE/state.sqlite\" && pwd > \"$PATCHWEFT_STATE/cwd\"",
        Some("pwd > \"$PATCHWEFT_STATE/cleanup-cwd\""),
        60,
    );
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.exit_code, result.drained),
        (Outcome::Completed, 0, true),
        "{}",
        result.evidence
    );
    let directory = lab.directory(&claim);
    let read = |name: &str| fs::read_to_string(lab.root.join(name)).unwrap();
    assert_eq!(read("cwd").trim(), directory.join("work").to_str().unwrap());
    assert_eq!(read("cleanup-cwd").trim(), directory.to_str().unwrap());
    let names: Vec<String> = fs::read_dir(&lab.root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().all(|name| !name.starts_with("state.sqlite")),
        "{names:?}"
    );
}

#[test]
fn supervisor_records_its_own_timeout_as_stopping() {
    let lab = Lab::new();
    let claim = lab.claim("sleep 30", None, 1);
    lab.write(&claim);
    lab.register(&claim);
    let started = Instant::now();
    let result = lab.result(&claim, lab.spawn(&claim));
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(
        (result.outcome, result.reason.as_deref(), result.exit_code),
        (Outcome::Completed, Some("timeout"), 124)
    );
    assert!(result.drained);
    let stopping: Value =
        serde_json::from_slice(&fs::read(lab.control(&claim).join("stopping")).unwrap()).unwrap();
    assert_eq!(stopping, json!({"reason":"timeout"}));
    // A supervisor reports its own stops; `stop` is only ever its coordinator's.
    assert!(!lab.control(&claim).join("stop").exists());
}

#[test]
fn a_command_that_failed_before_a_stop_keeps_its_own_exit() {
    // Each command leaves a descendant the supervisor waits for, then a stop arrives. A command
    // that had already failed on its own (or been killed, as by the OOM killer) keeps that
    // verdict: the stop ended only what it left running. That holds for a SIGTERM to the
    // supervisor alone too, as an executor deploy sends, and for a stop that arrives within the
    // same poll as the exit. One that exited 0 is not done while its descendants run, one still
    // running is stopped, and one a group-wide SIGTERM ended is stopped by it.
    enum Stop {
        File,
        Supervisor,
        Group,
        /// The command's own descendant writes the stop just after the command exits.
        Itself,
    }
    let lab = Lab::new();
    let writer = "(sleep 0.001; c=\"$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT/control\"; \
                  printf '{\"reason\":\"partition\"}' > \"$c/stop.new\"; mv \"$c/stop.new\" \"$c/stop\") & ";
    for (name, script, stop, exit_code, reason, after) in [
        ("failed", "exit 3", Stop::File, 3, None, Some("partition")),
        (
            "killed",
            "kill -KILL $$",
            Stop::File,
            128,
            None,
            Some("partition"),
        ),
        ("passed", "exit 0", Stop::File, 124, Some("partition"), None),
        (
            "running",
            "sleep 30",
            Stop::File,
            124,
            Some("partition"),
            None,
        ),
        (
            "deploy",
            "exit 3",
            Stop::Supervisor,
            3,
            None,
            Some("supervisor-stop"),
        ),
        (
            "group",
            "sleep 30",
            Stop::Group,
            124,
            Some("supervisor-stop"),
            None,
        ),
        (
            "same-poll",
            "exit 3",
            Stop::Itself,
            3,
            None,
            Some("partition"),
        ),
    ] {
        let marker = lab.root.join(name);
        let writes = if matches!(stop, Stop::Itself) {
            writer
        } else {
            ""
        };
        let claim = lab.claim(
            &format!("sleep 30 & touch \"$PATCHWEFT_STATE/{name}\"; {writes}{script}"),
            None,
            60,
        );
        lab.write(&claim);
        lab.register(&claim);
        let supervisor = lab.spawn(&claim);
        let started = Instant::now();
        while !marker.exists() {
            assert!(started.elapsed() < Duration::from_secs(30), "{name}");
            thread::sleep(Duration::from_millis(20));
        }
        // Several of the supervisor's polls: it has seen a command that exited.
        thread::sleep(Duration::from_millis(400));
        let pid = supervisor.0.id() as i32;
        match stop {
            Stop::File => lab.stop(&claim, "partition"),
            Stop::Supervisor => unsafe {
                assert_eq!(libc::kill(pid, libc::SIGTERM), 0);
            },
            Stop::Group => unsafe {
                assert_eq!(libc::kill(-pid, libc::SIGTERM), 0);
            },
            Stop::Itself => {}
        }
        let result = lab.result(&claim, supervisor);
        assert_eq!(
            (result.outcome, result.exit_code, result.reason.as_deref()),
            (Outcome::Completed, exit_code, reason),
            "{name}"
        );
        assert_eq!(result.evidence["exit_code"], exit_code, "{name}");
        assert_eq!(result.evidence["reason"].as_str(), reason, "{name}");
        assert_eq!(
            result.evidence["stopped_after_exit"].as_str(),
            after,
            "{name}"
        );
        assert!(result.drained, "{name}");
    }
}

/// A dependency of `claim` at `path` named `name`, whose digest is that of `content`.
fn dependency(lab: &Lab, name: &str, path: PathBuf, content: &str) -> DependencyFile {
    let sample = lab.root.join(format!("sample-{}", unique()));
    fs::write(&sample, content).unwrap();
    DependencyFile {
        action: "producer".into(),
        attempt: "p-1".into(),
        name: name.into(),
        sha256: file_hash(&sample).unwrap(),
        mode: 0o644,
        bytes: content.len() as u64,
        path,
    }
}

#[test]
fn setup_failures_are_classified_by_cause() {
    // Gate P3-B. This host failing to set the job up says nothing about the revision: not started,
    // requeued elsewhere.
    let lab = Lab::new();
    let mut claim = lab.claim("touch \"$PATCHWEFT_STATE/ran\"", None, 60);
    claim
        .dependencies
        .push(dependency(&lab, "dist/x", lab.root.join("vanished"), "x"));
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.reason.as_deref()),
        (Outcome::NotStarted, Some("setup-failed")),
        "{}",
        result.evidence
    );
    // A producer's artifact that would overwrite the revision's own file is the content's fault:
    // it fails, with a repair, as before.
    let present = lab.root.join("present");
    fs::write(&present, "y").unwrap();
    let mut claim = lab.claim("touch \"$PATCHWEFT_STATE/ran\"", None, 60);
    claim
        .dependencies
        .push(dependency(&lab, "README", present, "y"));
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.exit_code),
        (Outcome::Completed, 125),
        "{}",
        result.evidence
    );
    assert!(
        result.evidence["error"]
            .as_str()
            .unwrap()
            .contains("would overwrite source"),
        "{}",
        result.evidence
    );
    assert!(!lab.root.join("ran").exists());
}

#[test]
fn declared_jobs_receive_their_inputs_and_keyed_base_from_the_claim() {
    // pw#11: PATCHWEFT_INPUTS and PATCHWEFT_BASE come from the claimed action, so a local and a remote
    // supervisor export the same values; a job with neither recorded sees neither.
    let lab = Lab::new();
    let (inputs, base) = ("a".repeat(64), "b".repeat(40));
    let mut claim = lab.claim(
        &format!("test \"$PATCHWEFT_INPUTS\" = {inputs} && test \"$PATCHWEFT_BASE\" = {base}"),
        None,
        60,
    );
    claim.action.declared_inputs = Some(inputs);
    claim.action.declared_base = Some(base);
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.exit_code),
        (Outcome::Completed, 0),
        "{}",
        result.evidence
    );
    let plain = lab.claim(
        "test -z \"${PATCHWEFT_INPUTS+x}${PATCHWEFT_BASE+x}\"",
        None,
        60,
    );
    lab.write(&plain);
    lab.register(&plain);
    let result = lab.result(&plain, lab.spawn(&plain));
    assert_eq!(
        (result.outcome, result.exit_code),
        (Outcome::Completed, 0),
        "{}",
        result.evidence
    );
}

#[test]
fn command_started_marker_precedes_spawn() {
    let lab = Lab::new();
    let claim = lab.claim(
        "test -f \"$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT/control/command-started\"",
        None,
        60,
    );
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.exit_code),
        (Outcome::Completed, 0),
        "{}",
        result.evidence
    );
}

#[test]
fn runtime_mismatch_is_not_started() {
    let lab = Lab::new();
    let mut profile = lab
        .claim(
            "touch \"$PATCHWEFT_STATE/ran\"",
            Some("touch \"$PATCHWEFT_STATE/cleaned\""),
            60,
        )
        .profile;
    // Registered by a runner binary other than the one on this host.
    profile.engine = "0".repeat(64);
    let claim = lab.claim_for(profile, 60);
    lab.write(&claim);
    lab.register(&claim);
    let result = lab.result(&claim, lab.spawn(&claim));
    assert_eq!(
        (result.outcome, result.reason.as_deref(), result.exit_code),
        (Outcome::NotStarted, Some("runtime-mismatch"), 125)
    );
    assert!(
        result.evidence["error"]
            .as_str()
            .unwrap()
            .contains("runner binary changed"),
        "{}",
        result.evidence
    );
    assert!(!lab.root.join("ran").exists());
    assert!(!lab.control(&claim).join("command-started").exists());
    // Past registration, external cleanup still runs before the result.
    assert!(lab.root.join("cleaned").exists());
    assert!(result.drained);
}
