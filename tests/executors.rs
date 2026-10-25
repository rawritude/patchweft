//! Remote executors end to end: the API process, a remote coordinator (`mindy`) with its own work
//! directory, and the local worker, all against one store on this host.
use patchweft::{
    client::{Refused, Remote},
    model::{ClaimRequest, ClaimResponse, CompleteRequest, CompleteResponse, Outcome},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const AW: &str = env!("CARGO_BIN_EXE_patchweft");
const BUILD: &str = "mkdir dist; echo built > dist/out";

fn parsed(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON response")
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

fn job(name: &str, script: &str, extra: Value) -> Value {
    let mut job = json!({"name":name,"command":["sh","-c",script],"report":{"kind":"command"}});
    job.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    job
}

/// Marks `$PATCHWEFT_STATE/NAME-started`, then waits for `$PATCHWEFT_STATE/release`.
fn gated(name: &str) -> Value {
    job(
        name,
        &format!(
            "touch \"$PATCHWEFT_STATE/{name}-started\"; \
             while ! test -f \"$PATCHWEFT_STATE/release\"; do sleep 0.05; done"
        ),
        json!({}),
    )
}

/// A certifying profile with `sh` and `tools` as its tools.
fn definition(name: &str, tools: Value, jobs: Value) -> Value {
    let mut all = json!({"sh":"/usr/bin/sh"});
    all.as_object_mut()
        .unwrap()
        .extend(tools.as_object().unwrap().clone());
    json!({"version":1,"name":name,"certifying":true,"tools":all,"jobs":jobs})
}

fn started(work: &Path, name: &str) -> bool {
    work.join(format!("{name}-started")).exists()
}

fn names(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = match value {
        Value::Array(items) => items.iter().map(|v| v.as_str().unwrap().into()).collect(),
        Value::Object(map) => map.keys().cloned().collect(),
        _ => vec![],
    };
    names.sort();
    names
}

/// Polls until `done` holds, for at most a minute.
fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(
            start.elapsed().as_secs() < 60,
            "timed out waiting for {what}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn signal(pid: i32, signal: i32) {
    unsafe {
        assert_eq!(libc::kill(pid, signal), 0);
    }
}

/// Stands in for a rootless Docker daemon as `dockerd-rootless/child_pid` describes one: a
/// `rootlesskit` process run with `flag`, and its child.
struct Daemon(Child);

impl Daemon {
    fn start(runtime: &Path, flag: &str) -> Self {
        let state = runtime.join("dockerd-rootless");
        fs::create_dir_all(&state).unwrap();
        let child = Command::new("/bin/bash")
            .arg("-c")
            .arg(
                r#"exec -a rootlesskit /bin/bash -c 'sleep 600 & echo $! > "$0/child_pid.new"; mv "$0/child_pid.new" "$0/child_pid"; wait' "$1" "$2""#,
            )
            .arg("bash")
            .arg(&state)
            .arg(flag)
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        wait_until("the stand-in daemon", || state.join("child_pid").exists());
        Self(child)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Lab {
    temp: tempfile::TempDir,
    state: PathBuf,
    base: String,
    profiles: Vec<String>,
    /// mindy's work directory, `PATCHWEFT_STATE` for its jobs.
    work: PathBuf,
    credential: PathBuf,
    server: Option<Server>,
    address: String,
    starts: usize,
}

impl Lab {
    fn new(jobs: Value, allow: &[&str]) -> Self {
        Self::with(&[definition("fixture", json!({}), jobs)], allow)
    }

    /// One project and `profiles`, granted to executor `mindy` (one slot, 2048 MiB, six-second
    /// leases), which may run the jobs in `allow`. The API is listening.
    fn with(profiles: &[Value], allow: &[&str]) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("source");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("README"), "initial\n").unwrap();
        // Incompressible history, so an incremental bundle is visibly smaller than a full one.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let noise: Vec<u8> = (0..65536)
            .map(|_| {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (seed >> 33) as u8
            })
            .collect();
        fs::write(repo.join("noise.bin"), noise).unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let state = temp.path().join("state");
        let mut lab = Self {
            work: temp.path().join("mindy"),
            credential: temp.path().join("mindy.credential"),
            state,
            base: String::new(),
            profiles: vec![],
            server: None,
            address: String::new(),
            starts: 0,
            temp,
        };
        lab.call(&["init", "--slots", "2", "--memory-mb", "4096"]);
        let revision = lab.call(&[
            "import",
            "--name",
            "fixture",
            "--repo",
            repo.to_str().unwrap(),
            "--owner",
            "root",
        ]);
        lab.base = revision["id"].as_str().unwrap().into();
        lab.profiles = profiles.iter().map(|p| lab.register(p.clone())).collect();
        let policy = lab.temp.path().join("mindy.policy.json");
        fs::write(
            &policy,
            json!({"id":"mindy","projects":[revision["project"]],"profiles":lab.profiles,
                   "roles":["read","execute"],"max_pending":1})
            .to_string(),
        )
        .unwrap();
        let credential = lab.credential.to_str().unwrap().to_owned();
        lab.call(&[
            "grant",
            "--policy",
            policy.to_str().unwrap(),
            "--credential-file",
            &credential,
        ]);
        let mut set = vec![
            "executor-set",
            "mindy",
            "--slots",
            "1",
            "--memory-mb",
            "2048",
            "--lease-seconds",
            "6",
        ];
        for job in allow {
            set.extend(["--allow-job", job]);
        }
        lab.call(&set);
        lab.serve("127.0.0.1:0");
        lab
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(AW);
        command.arg("--state").arg(&self.state).args(args);
        command
    }

    fn call(&self, args: &[&str]) -> Value {
        parsed(self.command(args).output().unwrap())
    }

    fn register(&self, definition: Value) -> String {
        let path = self.temp.path().join(format!("profile-{}.json", unique()));
        fs::write(&path, definition.to_string()).unwrap();
        self.call(&["register", path.to_str().unwrap()])["id"]
            .as_str()
            .unwrap()
            .into()
    }

    fn submit_on(&self, revision: &str, profile: &str, owner: &str) -> Value {
        self.call(&[
            "submit",
            "--revision",
            revision,
            "--profile",
            profile,
            "--owner",
            owner,
        ])
    }

    fn submit(&self, owner: &str) -> Value {
        self.submit_on(&self.base, &self.profiles[0], owner)
    }

    /// A child revision of the base with `path` rewritten.
    fn edit(&self, path: &str, text: &str) -> String {
        let view = self.call(&["workspace", "--revision", &self.base, "--owner", "author"]);
        fs::write(Path::new(view["path"].as_str().unwrap()).join(path), text).unwrap();
        self.call(&[
            "checkpoint",
            view["id"].as_str().unwrap(),
            "--message",
            "edit",
        ])["id"]
            .as_str()
            .unwrap()
            .into()
    }

    fn state_of(&self, candidate: &Value) -> String {
        self.call(&["status", candidate["id"].as_str().unwrap()])["state"]
            .as_str()
            .unwrap()
            .into()
    }

    fn events_of(&self, kind: &str) -> Vec<Value> {
        self.call(&["events"])
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == kind)
            .cloned()
            .collect()
    }

    fn count(&self, kind: &str) -> usize {
        self.events_of(kind).len()
    }

    fn database(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.state.join("state.sqlite")).unwrap()
    }

    fn body(&self, table: &str, id: &str) -> Value {
        let body: String = self
            .database()
            .query_row(&format!("SELECT body FROM {table} WHERE id=?"), [id], |r| {
                r.get(0)
            })
            .unwrap();
        serde_json::from_str(&body).unwrap()
    }

    fn row(&self, table: &str, id: &str) -> (String, bool) {
        let column = if table == "attempts" { "drained" } else { "0" };
        self.database()
            .query_row(
                &format!("SELECT state,{column} FROM {table} WHERE id=?"),
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
    }

    /// The body of the action's current attempt.
    fn attempt_of(&self, action: &str) -> Value {
        let attempt: String = self
            .database()
            .query_row("SELECT attempt FROM actions WHERE id=?", [action], |r| {
                r.get(0)
            })
            .unwrap();
        self.body("attempts", &attempt)
    }

    /// mindy as `executors` lists it.
    fn mindy(&self) -> Value {
        self.call(&["executors"])
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == "mindy")
            .unwrap()
            .clone()
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn serve(&mut self, listen: &str) {
        self.starts += 1;
        let ready = self.temp.path().join(format!("ready-{}", self.starts));
        let child = self
            .command(&["serve", "--listen", listen, "--ready-file"])
            .arg(&ready)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.server = Some(Server(child));
        let mut address = None;
        wait_until("the API", || {
            address = fs::read_to_string(&ready)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .map(|ready| ready["address"].as_str().unwrap().to_owned());
            address.is_some()
        });
        self.address = address.unwrap();
    }

    fn stop_server(&mut self) {
        self.server = None;
    }

    /// The remote client CLI with mindy's credential.
    fn remote(&self, args: &[&str]) -> Value {
        parsed(
            Command::new(AW)
                .args([
                    "remote",
                    "--endpoint",
                    &self.endpoint(),
                    "--credential-file",
                ])
                .arg(&self.credential)
                .args(args)
                .output()
                .unwrap(),
        )
    }

    /// mindy's coordinator in `work`: fast polls, one-second heartbeats, no disk floor, and no
    /// loopback check (this host's own listeners are not the subject; `checked_executor` is).
    fn executor(&self, work: &Path, args: &[&str]) -> Command {
        let mut command = self.checked_executor(work, args);
        command.arg("--no-loopback-check");
        command
    }

    /// mindy's coordinator as `executor` runs it on a host, loopback check included.
    fn checked_executor(&self, work: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(AW);
        command
            .args([
                "executor",
                "--endpoint",
                &self.endpoint(),
                "--credential-file",
            ])
            .arg(&self.credential)
            .arg("--work")
            .arg(work)
            .args(["--poll-ms", "100", "--heartbeat-seconds", "1"])
            .args(["--min-free-gb", "0"])
            .args(args);
        command
    }

    /// Runs mindy until nothing it may run is left.
    fn drain_remote(&self) -> Value {
        parsed(self.executor(&self.work, &["--drain"]).output().unwrap())
    }

    /// Starts `executor --drain` in the background with its log lines in `log`.
    fn spawn_remote(&self, log: &Path) -> Child {
        self.executor(&self.work, &["--drain"])
            .stdout(Stdio::piped())
            .stderr(File::create(log).unwrap())
            .spawn()
            .unwrap()
    }

    fn log(&self, name: &str) -> PathBuf {
        self.temp.path().join(format!("{name}.log"))
    }
}

fn unique() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    NEXT.fetch_add(1, Ordering::SeqCst).to_string()
}

/// JSON log lines of one kind from a coordinator's stderr.
fn logged(log: &Path, event: &str) -> Vec<Value> {
    fs::read_to_string(log)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["event"] == event)
        .collect()
}

/// A producer and its consumer, both run by mindy alone.
fn producer_consumer() -> (Lab, Value) {
    let lab = Lab::new(
        json!([
            job("build", BUILD, json!({"artifacts":["dist"]})),
            job(
                "use",
                "grep -q built dist/out",
                json!({"dependencies":["build"]})
            ),
        ]),
        &["build", "use"],
    );
    let candidate = lab.submit("agent");
    assert_eq!(lab.drain_remote(), json!({"executor":"mindy","launched":2}));
    assert_eq!(lab.state_of(&candidate), "passed");
    (lab, candidate)
}

#[test]
fn remote_executor_runs_producer_and_consumer_end_to_end() {
    let (lab, candidate) = producer_consumer();
    for name in ["build", "use"] {
        let attempt = lab.attempt_of(candidate["required"][name].as_str().unwrap());
        assert_eq!(attempt["executor"], "mindy", "{name}");
        assert_eq!(attempt["result"]["executor"], "mindy", "{name}");
    }
    // The consumer's input came from the executor's read-only cache.
    let cached = lab.work.join("cas").join(patchweft::hash(b"built\n"));
    assert_eq!(
        fs::metadata(&cached).unwrap().permissions().mode() & 0o777,
        0o444
    );
    // The result lives in the store and is delivered from the API host.
    let destination = lab.temp.path().join("delivered");
    lab.remote(&[
        "deliver",
        candidate["required"]["build"].as_str().unwrap(),
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(destination.join("dist/out")).unwrap(),
        "built\n"
    );
    assert_eq!(fs::read_dir(lab.work.join("attempts")).unwrap().count(), 0);
}

#[test]
fn remote_capacity_is_per_executor_and_local_budget_unaffected() {
    let lab = Lab::new(
        json!([
            gated("r1"),
            gated("r2"),
            job("wide", "true", json!({"slots":2}))
        ]),
        &["r1", "r2"],
    );
    lab.call(&[
        "executor-set",
        "local",
        "--deny-job",
        "r1",
        "--deny-job",
        "r2",
    ]);
    let candidate = lab.submit("agent");
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("a remote job", || {
        started(&lab.work, "r1") || started(&lab.work, "r2")
    });
    // The local worker admits a job needing both local slots while mindy holds one of its own.
    assert_eq!(
        lab.call(&["worker", "--drain", "--no-loopback-check"]),
        json!({"executor":"local","launched":1})
    );
    let wide = candidate["required"]["wide"].as_str().unwrap();
    assert_eq!(lab.row("actions", wide).0, "passed");
    thread::sleep(Duration::from_millis(500));
    let held = lab.mindy()["held"].clone();
    assert_eq!(held["slots"], 1);
    assert_eq!(held["attempts"].as_array().unwrap().len(), 1);
    assert_eq!(lab.count("attempt.claimed"), 2);
    fs::write(lab.work.join("release"), "").unwrap();
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":2})
    );
    assert_eq!(lab.state_of(&candidate), "passed");
    // One slot: mindy claimed its second job only after reporting the first.
    let events = lab.call(&["events"]);
    let mindy: Vec<(&str, i64)> = events
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| {
            e["detail"]["executor"] == "mindy" || e["detail"]["evidence"]["executor"] == "mindy"
        })
        .filter(|e| ["attempt.claimed", "attempt.completed"].contains(&e["kind"].as_str().unwrap()))
        .map(|e| (e["kind"].as_str().unwrap(), e["seq"].as_i64().unwrap()))
        .collect();
    let kinds: Vec<&str> = mindy.iter().map(|(kind, _)| *kind).collect();
    assert_eq!(
        kinds,
        [
            "attempt.claimed",
            "attempt.completed",
            "attempt.claimed",
            "attempt.completed"
        ]
    );
}

#[test]
fn local_coordinator_never_touches_remote_attempts() {
    let lab = Lab::new(json!([gated("remote")]), &["remote"]);
    lab.call(&["executor-set", "local", "--deny-job", "remote"]);
    let candidate = lab.submit("agent");
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("the remote job", || started(&lab.work, "remote"));
    // The local coordinator and `recover` settle only the local executor's own attempts.
    assert_eq!(
        lab.call(&["worker", "--drain", "--no-loopback-check"]),
        json!({"executor":"local","launched":0})
    );
    let recovered = lab.call(&["recover"]);
    assert_eq!(recovered["interrupted_and_drained"], json!([]));
    assert_eq!(recovered["needs_drain_confirmation"], json!([]));
    let action = candidate["required"]["remote"].as_str().unwrap();
    assert_eq!(lab.row("actions", action).0, "running");
    fs::write(lab.work.join("release"), "").unwrap();
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":1})
    );
    assert_eq!(lab.state_of(&candidate), "passed");
    assert_eq!(lab.count("attempt.interrupted"), 0);
}

#[test]
fn lease_expiry_interrupts_undrained_and_holds_capacity() {
    let lab = Lab::new(
        json!([gated("hold"), job("next", "true", json!({}))]),
        &["hold"],
    );
    let candidate = lab.submit("agent");
    let mut remote = lab
        .executor(&lab.work, &[])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("the held job", || started(&lab.work, "hold"));
    lab.call(&[
        "executor-set",
        "mindy",
        "--allow-job",
        "hold",
        "--allow-job",
        "next",
    ]);
    let launched = lab.events_of("attempt.started")[0].clone();
    let attempt = launched["subject"].as_str().unwrap().to_owned();
    // The host loses its coordinator, the supervisor and the job at once.
    remote.kill().unwrap();
    remote.wait().unwrap();
    signal(
        -(launched["detail"]["pid"].as_i64().unwrap() as i32),
        libc::SIGKILL,
    );
    wait_until("lease expiry", || lab.count("attempt.lease_expired") == 1);
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), false));
    // Nobody knows what the job left behind, so mindy's only slot stays held.
    let fresh = lab.temp.path().join("fresh");
    let drain = || parsed(lab.executor(&fresh, &["--drain"]).output().unwrap());
    assert_eq!(drain(), json!({"executor":"mindy","launched":0}));
    let next = candidate["required"]["next"].as_str().unwrap();
    assert_eq!(lab.row("actions", next).0, "queued");
    assert_eq!(lab.mindy()["held"]["attempts"][0]["id"], attempt.as_str());
    lab.call(&["recover", "--confirm-drained", &attempt]);
    assert_eq!(drain(), json!({"executor":"mindy","launched":1}));
    assert_eq!(lab.row("actions", next).0, "passed");
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    fs::write(fresh.join("release"), "").unwrap();
    assert_eq!(drain(), json!({"executor":"mindy","launched":1}));
    assert_eq!(lab.state_of(&candidate), "passed");
}

#[test]
fn late_completion_is_fenced_and_drain_report_releases() {
    let lab = Lab::new(json!([gated("hold")]), &["hold"]);
    let candidate = lab.submit("agent");
    let action = candidate["required"]["hold"].as_str().unwrap().to_owned();
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("the job", || started(&lab.work, "hold"));
    let attempt = lab.events_of("attempt.started")[0]["subject"]
        .as_str()
        .unwrap()
        .to_owned();
    signal(remote.id() as i32, libc::SIGSTOP);
    wait_until("lease expiry", || lab.count("attempt.lease_expired") == 1);
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), false));
    // Only the lease holder may report the drain.
    let client = Remote::new(&lab.endpoint(), &lab.credential, None).unwrap();
    let forged = CompleteRequest {
        attempt: attempt.clone(),
        lease: "not-the-lease".into(),
        outcome: Outcome::Lost,
        reason: None,
        exit_code: 125,
        evidence: json!({}),
        drained: true,
        log: None,
        cleanup_log: None,
        artifacts: BTreeMap::new(),
    };
    let error = client
        .executor::<CompleteResponse>("complete", &forged, None)
        .unwrap_err();
    let refused = error.downcast_ref::<Refused>().unwrap();
    assert_eq!(
        (refused.status, refused.code()),
        (409, Some("attempt_fenced"))
    );
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), false));
    // The coordinator wakes up, stops its job, cleans up and reports the drain under its lease.
    fs::remove_file(lab.work.join("hold-started")).unwrap();
    signal(remote.id() as i32, libc::SIGCONT);
    // The lapse gave no verdict, so the action is queued again and mindy runs it once more.
    wait_until("the job again", || started(&lab.work, "hold"));
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), true));
    let reports = lab.events_of("attempt.drain_reported");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["subject"], attempt.as_str());
    // Stopped as fenced, an interruption rather than a verdict.
    assert_eq!(
        reports[0]["detail"],
        json!({"executor":"mindy","outcome":"completed","exit_code":124,"requeued":true})
    );
    assert!(
        lab.events_of("attempt.completed")
            .iter()
            .all(|e| e["subject"] != attempt.as_str())
    );
    assert!(!lab.work.join("release").exists());
    // A fenced attempt's directory stays on the executor for inspection.
    wait_until("the retained attempt", || {
        lab.work.join("retained").join(&attempt).is_dir()
    });
    fs::write(lab.work.join("release"), "").unwrap();
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":2})
    );
    assert_eq!(lab.state_of(&candidate), "passed");
    assert_ne!(lab.attempt_of(&action)["id"], attempt.as_str());
}

#[test]
fn a_lost_claim_response_is_drained_and_run_again_without_an_operator() {
    // The authority records a claim whose response never reaches mindy. Its replayed key gets
    // through only after the lease lapsed: fenced, and mindy's ledger never held the attempt.
    let lab = Lab::new(
        json!([
            job("kept", "true", json!({})),
            job("lost", "true", json!({}))
        ]),
        &["kept"],
    );
    let candidate = lab.submit("agent");
    let kept = candidate["required"]["kept"].as_str().unwrap();
    let lost = candidate["required"]["lost"].as_str().unwrap();
    assert_eq!(lab.drain_remote(), json!({"executor":"mindy","launched":1}));
    assert_eq!(lab.row("actions", kept).0, "passed");
    lab.call(&[
        "executor-set",
        "mindy",
        "--allow-job",
        "kept",
        "--allow-job",
        "lost",
    ]);
    let client = Remote::new(&lab.endpoint(), &lab.credential, None).unwrap();
    let request = ClaimRequest {
        claim_key: "lost-response".into(),
        verified: lab.profiles.clone(),
        ..ClaimRequest::default()
    };
    let response: ClaimResponse = client.executor("claim", &request, None).unwrap();
    let claim = response.claim.unwrap();
    assert_eq!(claim.action.id, lost);
    let attempt = claim.attempt.id;
    fs::write(
        lab.work.join("pending-claim"),
        json!({"key":"lost-response"}).to_string(),
    )
    .unwrap();
    wait_until("lease expiry", || lab.count("attempt.lease_expired") == 1);
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), false));
    // Until something confirms the drain, the candidate says who holds the job and since when.
    let status = lab.call(&["status", candidate["id"].as_str().unwrap()]);
    assert_eq!(status["state"], "failed");
    assert_eq!(
        status["jobs"]["lost"]["awaiting_drain"],
        json!({"attempt":attempt,"executor":"mindy",
               "lease_expires":lab.body("attempts", &attempt)["lease_expires"],"may_self_heal":true})
    );
    assert!(status["jobs"]["kept"].get("awaiting_drain").is_none());
    let log = lab.log("mindy");
    assert_eq!(
        parsed(lab.spawn_remote(&log).wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":1})
    );
    assert_eq!(logged(&log, "claim_fenced")[0]["attempt"], attempt.as_str());
    assert_eq!(
        logged(&log, "claim_unrecorded")[0]["attempt"],
        attempt.as_str()
    );
    assert_eq!(lab.row("attempts", &attempt), ("interrupted".into(), true));
    let reports = lab.events_of("attempt.drain_reported");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["subject"], attempt.as_str());
    assert_eq!(
        reports[0]["detail"],
        json!({"executor":"mindy","reason":"claim-unrecorded","requeued":true})
    );
    // Run again under the shared requeue budget; the candidate's pass was kept, not rerun.
    let requeues: i64 = lab
        .database()
        .query_row("SELECT count FROM requeues WHERE action=?", [lost], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(requeues, 1);
    assert_ne!(lab.attempt_of(lost)["id"], attempt.as_str());
    let status = lab.call(&["status", candidate["id"].as_str().unwrap()]);
    assert_eq!(status["state"], "passed");
    assert!(status["jobs"]["lost"].get("awaiting_drain").is_none());
    let claimed = lab.events_of("attempt.claimed");
    assert_eq!(
        claimed
            .iter()
            .filter(|e| e["detail"]["action"] == kept)
            .count(),
        1
    );
    assert_eq!(lab.count("attempt.interrupted"), 0);
}

#[test]
fn cancel_stops_remote_job_as_canceled() {
    let lab = Lab::new(json!([gated("slow")]), &["slow"]);
    let action = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &lab.profiles[0],
        "--job",
        "slow",
        "--owner",
        "first",
    ])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("the job", || started(&lab.work, "slow"));
    lab.call(&["cancel", &action, "--owner", "first"]);
    // The store records the stop at once; mindy hears of it at its next renewal.
    let stopping = lab.events_of("attempt.stopping");
    assert_eq!(stopping.len(), 1);
    assert_eq!(
        stopping[0]["detail"],
        json!({"reason":"no-requesters","executor":"mindy"})
    );
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":1})
    );
    let attempt = stopping[0]["subject"].as_str().unwrap();
    assert_eq!(lab.row("attempts", attempt), ("canceled".into(), true));
    assert_eq!(lab.row("actions", &action).0, "canceled");
}

#[test]
fn runtime_mismatch_is_refused_without_attempts() {
    let tools = tempfile::tempdir().unwrap();
    let drift = tools.path().join("drift");
    fs::write(&drift, "#!/bin/sh\n").unwrap();
    fs::set_permissions(&drift, fs::Permissions::from_mode(0o755)).unwrap();
    let lab = Lab::with(
        &[
            definition(
                "mismatch",
                json!({"drift":drift}),
                json!([job("checked", "true", json!({}))]),
            ),
            definition(
                "control",
                json!({}),
                json!([job("plain", "true", json!({}))]),
            ),
        ],
        &["checked", "plain"],
    );
    let (mismatch, control) = (&lab.profiles[0], &lab.profiles[1]);
    let refused = lab.submit_on(&lab.base, mismatch, "agent");
    let passing = lab.submit_on(&lab.base, control, "agent");
    fs::write(&drift, "#!/bin/sh\n# changed\n").unwrap();
    assert_eq!(lab.drain_remote(), json!({"executor":"mindy","launched":1}));
    assert_eq!(lab.state_of(&passing), "passed");
    let checked = refused["required"]["checked"].as_str().unwrap();
    assert_eq!(lab.row("actions", checked).0, "queued");
    assert!(
        lab.events_of("attempt.claimed")
            .iter()
            .all(|e| e["detail"]["action"] != checked)
    );
    assert!(lab.call(&["repairs"]).as_array().unwrap().is_empty());
    let observed = lab.mindy()["observed"].clone();
    let error = observed["refused"][mismatch.as_str()].as_str().unwrap();
    assert!(error.contains("declared tool bytes changed"), "{error}");
    assert!(
        observed["verified"]
            .as_array()
            .unwrap()
            .contains(&json!(control))
    );
    assert!(observed["last_seen"].as_f64().is_some());
}

#[test]
fn placement_never_changes_action_identity() {
    let jobs = |test_a_memory: u32| {
        json!([
            job(
                "build",
                BUILD,
                json!({"artifacts":["dist"],"reuse":"exact",
                       "audit":"writes a constant; reads no inputs"})
            ),
            job(
                "test_a",
                "test -s dist/out",
                json!({"dependencies":["build"],"memory_mb":test_a_memory})
            ),
            job(
                "test_b",
                "grep -q built dist/out",
                json!({"dependencies":["build"]})
            ),
        ])
    };
    let lab = Lab::new(jobs(1024), &["test_a", "test_b"]);
    lab.call(&["executor-set", "mindy", "--deny-job", "build"]);
    lab.call(&["executor-set", "local", "--allow-job", "build"]);
    let first = lab.submit("agent");
    assert_eq!(
        lab.call(&["worker", "--drain", "--no-loopback-check"]),
        json!({"executor":"local","launched":1})
    );
    assert_eq!(lab.drain_remote(), json!({"executor":"mindy","launched":2}));
    assert_eq!(lab.state_of(&first), "passed");
    let executor = |candidate: &Value, name: &str| {
        lab.attempt_of(candidate["required"][name].as_str().unwrap())["executor"].clone()
    };
    assert_eq!(executor(&first, "build"), "local");
    assert_eq!(executor(&first, "test_a"), "mindy");
    assert_eq!(executor(&first, "test_b"), "mindy");
    // The same jobs requested afresh and run on the local executor carry the same action keys.
    lab.call(&["executor-set", "local", "--clear-placement"]);
    let request = |name: &str| {
        lab.call(&[
            "request",
            "--revision",
            &lab.base,
            "--profile",
            &lab.profiles[0],
            "--job",
            name,
            "--owner",
            "other",
        ])["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let local: BTreeMap<&str, String> = ["test_a", "test_b"]
        .into_iter()
        .map(|name| (name, request(name)))
        .collect();
    assert_eq!(
        lab.call(&["worker", "--drain", "--no-loopback-check"]),
        json!({"executor":"local","launched":2})
    );
    for (name, local) in &local {
        let remote = first["required"][name].as_str().unwrap();
        assert_ne!(remote, local);
        assert_eq!(lab.row("actions", local).0, "passed");
        assert_eq!(lab.attempt_of(local)["executor"], "local");
        assert_eq!(
            lab.body("actions", remote)["key"],
            lab.body("actions", local)["key"],
            "{name}"
        );
    }
    // A pass from mindy is retained and promoted like any other.
    let changed = lab.register(definition("fixture", json!({}), jobs(2048)));
    let third = lab.call(&[
        "submit",
        "--revision",
        &lab.base,
        "--profile",
        &changed,
        "--owner",
        "agent",
        "--retain-from",
        first["id"].as_str().unwrap(),
    ]);
    assert_eq!(names(&third["retained"]), ["build", "test_a", "test_b"]);
    lab.call(&["review", &lab.base, "--reviewer", "reviewer"]);
    let certificate = lab.call(&["promote", third["id"].as_str().unwrap()]);
    for name in ["test_a", "test_b"] {
        let receipt = &certificate["receipts"][name];
        assert_eq!(receipt["route"], "retained", "{name}");
        assert_eq!(receipt["evidence"]["executor"], "mindy", "{name}");
    }
}

#[test]
fn concurrent_claims_fetch_each_commit_once() {
    let lab = Lab::new(
        json!([job("one", "true", json!({})), job("two", "true", json!({}))]),
        &["one", "two"],
    );
    lab.call(&["executor-set", "mindy", "--slots", "2"]);
    let first = lab.submit("agent");
    let log = lab.log("first");
    assert_eq!(
        parsed(lab.spawn_remote(&log).wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":2})
    );
    assert_eq!(lab.state_of(&first), "passed");
    let initial = logged(&log, "source.fetched");
    assert_eq!(initial.len(), 1, "{initial:?}");
    assert_eq!(initial[0]["revision"], lab.base.as_str());
    assert_eq!(initial[0]["haves"], 0);
    // A child revision needs only the history the mirror lacks.
    let revision = lab.edit("README", "changed\n");
    let second = lab.submit_on(&revision, &lab.profiles[0], "agent");
    let log = lab.log("second");
    assert_eq!(
        parsed(lab.spawn_remote(&log).wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":2})
    );
    assert_eq!(lab.state_of(&second), "passed");
    let later = logged(&log, "source.fetched");
    assert_eq!(later.len(), 1, "{later:?}");
    assert_eq!(later[0]["revision"], revision.as_str());
    assert_eq!(later[0]["haves"], 1);
    let (full, incremental) = (
        initial[0]["bytes"].as_u64().unwrap(),
        later[0]["bytes"].as_u64().unwrap(),
    );
    assert!(incremental * 10 < full, "{incremental} of {full} bytes");
}

#[test]
fn api_restart_within_lease_keeps_remote_work() {
    let mut lab = Lab::new(
        json!([job(
            "five",
            "touch \"$PATCHWEFT_STATE/five-started\"; sleep 5",
            json!({})
        )]),
        &["five"],
    );
    // A two-second outage stays well inside a ten-second lease's self-stop margin, whatever
    // the renewal backoff.
    lab.call(&["executor-set", "mindy", "--lease-seconds", "10"]);
    let candidate = lab.submit("agent");
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("the job", || started(&lab.work, "five"));
    let address = lab.address.clone();
    lab.stop_server();
    thread::sleep(Duration::from_secs(2));
    lab.serve(&address);
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":1})
    );
    assert_eq!(lab.state_of(&candidate), "passed");
    assert_eq!(lab.count("attempt.lease_expired"), 0);
    assert_eq!(lab.count("attempt.stopping"), 0);
}

#[test]
fn unreachable_api_self_stops_job_before_lease_expiry() {
    let mut lab = Lab::new(
        json!([job(
            "trapped",
            "trap 'touch \"$PATCHWEFT_STATE/termed\"; exit 143' TERM; \
             touch \"$PATCHWEFT_STATE/trapped-started\"; \
             while ! test -f \"$PATCHWEFT_STATE/release\"; do sleep 0.05; done",
            json!({})
        )]),
        &["trapped"],
    );
    let candidate = lab.submit("agent");
    let remote = lab.spawn_remote(&lab.log("mindy"));
    wait_until("the job", || started(&lab.work, "trapped"));
    let action = candidate["required"]["trapped"].as_str().unwrap();
    let first = lab.attempt_of(action)["id"].as_str().unwrap().to_owned();
    let address = lab.address.clone();
    lab.stop_server();
    let stopped = Instant::now();
    wait_until("the self-stop", || lab.work.join("termed").exists());
    // Lease 6 s, heartbeat 1 s: the job stops lease − margin (4 s) after the last renewal,
    // before the authority could let the lease lapse.
    let elapsed = stopped.elapsed();
    assert!(elapsed > Duration::from_secs(2), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(6), "{elapsed:?}");
    fs::remove_file(lab.work.join("trapped-started")).unwrap();
    lab.serve(&address);
    // Reported under the held lease, the interruption is no verdict: the job runs again, as it
    // would after a longer partition that let the lease lapse (gate P2-2).
    wait_until("the job again", || started(&lab.work, "trapped"));
    let attempt = lab.body("attempts", &first);
    assert_eq!(attempt["result"]["reason"], "partition");
    assert_eq!(lab.row("attempts", &first), ("interrupted".into(), true));
    let completed = lab.events_of("attempt.completed");
    assert_eq!(completed[0]["subject"], first.as_str());
    assert_eq!(completed[0]["detail"]["requeued"], true);
    assert!(lab.call(&["repairs"]).as_array().unwrap().is_empty());
    fs::write(lab.work.join("release"), "").unwrap();
    assert_eq!(
        parsed(remote.wait_with_output().unwrap()),
        json!({"executor":"mindy","launched":2})
    );
    assert_eq!(lab.count("attempt.lease_expired"), 0);
    assert_eq!(lab.state_of(&candidate), "passed");
}

#[test]
fn executor_disk_is_bounded() {
    let (lab, candidate) = producer_consumer();
    let preparation = |name: &str| {
        lab.attempt_of(candidate["required"][name].as_str().unwrap())["result"]["preparation"]
            .clone()
    };
    // The producer fetched the revision; its consumer found source and artifact on the host.
    assert!(preparation("build")["transfer_bytes"].as_u64().unwrap() > 0);
    assert_eq!(preparation("use")["transfer_bytes"], 0);
    assert!(
        lab.work
            .join("cas")
            .join(patchweft::hash(b"built\n"))
            .is_file()
    );
    for directory in ["attempts", "retained", "tmp"] {
        assert_eq!(
            fs::read_dir(lab.work.join(directory)).unwrap().count(),
            0,
            "{directory}"
        );
    }
}

#[test]
fn executor_upload_quota_frees_after_complete() {
    let (lab, candidate) = producer_consumer();
    let uploads = lab.state.join("uploads").join(patchweft::hash(b"mindy"));
    assert_eq!(fs::read_dir(&uploads).unwrap().count(), 0);
    // The uploads became the store's attempt outputs.
    let build = lab.attempt_of(candidate["required"]["build"].as_str().unwrap());
    let directory = lab
        .state
        .join("attempts")
        .join(build["id"].as_str().unwrap());
    assert_eq!(
        fs::read_to_string(directory.join("artifacts/dist/out")).unwrap(),
        "built\n"
    );
    assert_eq!(
        patchweft::file_hash(&directory.join("output.log")).unwrap(),
        build["result"]["log_sha256"].as_str().unwrap()
    );
}

#[test]
fn local_worker_checks_loopback_by_default() {
    // The gate's probe: mauve's worker ran a job beside an unlisted listener (P2-2). The lab API
    // itself listens on 127.0.0.1, which every job on this host can reach.
    let lab = Lab::new(json!([job("work", "true", json!({}))]), &["work"]);
    let candidate = lab.submit("agent");
    let output = lab.command(&["worker", "--drain"]).output().unwrap();
    assert_eq!(
        lab.count("attempt.claimed"),
        0,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_ne!(lab.state_of(&candidate), "passed");
    // Allowlisting nothing is not an opt-out; only --no-loopback-check is.
    lab.command(&["worker", "--drain", "--loopback-allow", "1"])
        .output()
        .unwrap();
    assert_eq!(lab.count("attempt.claimed"), 0);
    lab.call(&["worker", "--drain", "--no-loopback-check"]);
    assert_eq!(lab.state_of(&candidate), "passed");
}

#[test]
fn executor_refuses_to_register_with_an_unlisted_loopback_listener() {
    let lab = Lab::new(json!([job("work", "true", json!({}))]), &["work"]);
    let candidate = lab.submit("agent");
    // Follow-up 1 (#4204): a daemon that keeps containers off host loopback (mindy's) changes
    // nothing, since jobs run natively and reach the API's own listener directly.
    let runtime = lab.temp.path().join("runtime");
    let _daemon = Daemon::start(&runtime, "--disable-host-loopback");
    let output = lab
        .checked_executor(&lab.work, &["--drain"])
        .env("XDG_RUNTIME_DIR", &runtime)
        .env_remove("DOCKER_HOST")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("refusing to register"), "{stderr}");
    let reason = lab.mindy()["observed"]["refused"]["*"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        (reason.starts_with("loopback listener ") || reason.starts_with("wildcard listener "))
            && reason.ends_with(" not allowlisted"),
        "{reason}"
    );
    assert_eq!(lab.mindy()["observed"]["loopback"], "allow []");
    assert_eq!(lab.count("attempt.claimed"), 0);
    // Only the explicit opt-out lets the same executor work.
    assert_eq!(lab.drain_remote(), json!({"executor":"mindy","launched":1}));
    assert_eq!(lab.state_of(&candidate), "passed");
    assert!(lab.mindy()["observed"]["refused"].get("*").is_none());
}
