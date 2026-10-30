use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const AW: &str = env!("CARGO_BIN_EXE_patchweft");
/// Deadline for event-driven waits. Generous because a loaded host stalls workers for seconds;
/// waits poll for the event, so a long deadline costs nothing when the host is idle.
const PATIENCE: Duration = Duration::from_secs(60);

struct Lab {
    _temp: tempfile::TempDir,
    state: PathBuf,
    repo: PathBuf,
    base: String,
}

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
        .args(["-C"])
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

impl Lab {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("source");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("provider.mjs"), "export const offset = 0;\n").unwrap();
        fs::write(repo.join("consumer.mjs"), "export const factor = 1;\n").unwrap();
        fs::write(
            repo.join("contract.test.mjs"),
            "import { test } from 'node:test';\nimport assert from 'node:assert/strict';\n\
             import { offset } from './provider.mjs';\nimport { factor } from './consumer.mjs';\n\
             test('provider and consumer compose', () => assert.ok(offset * factor < 2));\n",
        )
        .unwrap();
        fs::write(repo.join("README"), "initial\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let state = temp.path().join("state");
        parsed(
            Command::new(AW)
                .arg("--state")
                .arg(&state)
                .args(["init", "--slots", "2", "--memory-mb", "4096"])
                .output()
                .unwrap(),
        );
        let base = parsed(
            Command::new(AW)
                .arg("--state")
                .arg(&state)
                .args(["import", "--name", "fixture", "--repo"])
                .arg(&repo)
                .args(["--owner", "root"])
                .output()
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .into();
        Self {
            _temp: temp,
            state,
            repo,
            base,
        }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(AW);
        command.arg("--state").arg(&self.state).args(args);
        // These jobs run no containers and this lab has no rootless daemon to inspect.
        if args.first() == Some(&"worker") {
            command.arg("--no-loopback-check");
        }
        command
    }
    fn call(&self, args: &[&str]) -> Value {
        parsed(self.command(args).output().unwrap())
    }
    fn refuses(&self, args: &[&str], message: &str) {
        let output = self.command(args).output().unwrap();
        assert!(!output.status.success(), "expected refusal");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fn profile(&self, jobs: Value) -> String {
        let path = self.state.join("profile.json");
        let node = std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
            .map(|directory| directory.join("node"))
            .find(|path| path.is_file())
            .expect("Node must be on PATH to run the real node:test contract fixture");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version":1,"name":"fixture","certifying":true,
                "tools":{"node":node,"sh":"/usr/bin/sh"},"jobs":jobs
            }))
            .unwrap(),
        )
        .unwrap();
        self.call(&["register", path.to_str().unwrap()])["id"]
            .as_str()
            .unwrap()
            .into()
    }
    fn contract_profile(&self) -> String {
        self.profile(
            json!([{"name":"contract","command":["node","--test","contract.test.mjs"],
                            "report":{"kind":"node-tap","minimum":1}}]),
        )
    }
    fn edit(&self, base: &str, owner: &str, path: &str, text: &str) -> (String, Value) {
        let view = self.call(&["workspace", "--revision", base, "--owner", owner]);
        fs::write(Path::new(view["path"].as_str().unwrap()).join(path), text).unwrap();
        let revision = self.call(&[
            "checkpoint",
            view["id"].as_str().unwrap(),
            "--message",
            owner,
        ]);
        (revision["id"].as_str().unwrap().into(), view)
    }
    fn submit(&self, revision: &str, profile: &str, owner: &str) -> Value {
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
    fn state(&self, candidate: &Value) -> String {
        self.call(&["status", candidate["id"].as_str().unwrap()])["state"]
            .as_str()
            .unwrap()
            .into()
    }
    fn drain(&self) {
        self.call(&["worker", "--drain"]);
    }
    fn register(&self, name: &str, definition: Value) -> String {
        let path = self.state.join(format!("{name}.json"));
        fs::write(&path, serde_json::to_vec(&definition).unwrap()).unwrap();
        self.call(&["register", path.to_str().unwrap()])["id"]
            .as_str()
            .unwrap()
            .into()
    }
    fn retain(&self, revision: &str, profile: &str, owner: &str, from: &Value) -> Value {
        self.call(&[
            "submit",
            "--revision",
            revision,
            "--profile",
            profile,
            "--owner",
            owner,
            "--retain-from",
            from["id"].as_str().unwrap(),
        ])
    }
    fn count(&self, kind: &str) -> usize {
        self.call(&["events"])
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == kind)
            .count()
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
}

fn job(name: &str, script: &str, extra: Value) -> Value {
    let mut job = json!({"name":name,"command":["sh","-c",script],"report":{"kind":"command"}});
    job.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    job
}

/// `build` feeds `test_a` and `test_b`; `lint` is independent and opts out of slot export.
fn chain(build_script: &str, test_b_script: &str, tuning: &[(&str, Value)]) -> Value {
    let tuned = |name: &str| {
        tuning
            .iter()
            .find(|(n, _)| *n == name)
            .map_or(json!({}), |(_, v)| v.clone())
    };
    let with = |name: &str, base: Value| {
        let mut value = base;
        value
            .as_object_mut()
            .unwrap()
            .extend(tuned(name).as_object().unwrap().clone());
        value
    };
    json!([
        job(
            "build",
            build_script,
            with("build", json!({"artifacts":["dist"]}))
        ),
        job(
            "test_a",
            "test -s dist/out",
            with("test_a", json!({"dependencies":["build"]}))
        ),
        job(
            "test_b",
            test_b_script,
            with("test_b", json!({"dependencies":["build"]}))
        ),
        job("lint", "true", with("lint", json!({"export_slots":false}))),
    ])
}

const BUILD: &str = "mkdir dist; echo built > dist/out";
const TEST_B: &str = "grep -q built dist/out";

fn names(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = match value {
        Value::Array(items) => items.iter().map(|v| v.as_str().unwrap().into()).collect(),
        Value::Object(map) => map.keys().cloned().collect(),
        _ => vec![],
    };
    names.sort();
    names
}

#[test]
fn incompatible_changes_fail_without_stalling_independent_work() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (a, a_view) = lab.edit(
        &lab.base,
        "provider",
        "provider.mjs",
        "export const offset = 1;\n",
    );
    let (b, _) = lab.edit(
        &lab.base,
        "consumer",
        "consumer.mjs",
        "export const factor = 2;\n",
    );
    let ca = lab.submit(&a, &profile, "provider");
    let cb = lab.submit(&b, &profile, "consumer");
    lab.drain();
    assert_eq!(lab.state(&ca), "passed");
    assert_eq!(lab.state(&cb), "passed");
    let combined = lab.call(&[
        "compose",
        "--base",
        &lab.base,
        "--change",
        &a,
        "--change",
        &b,
        "--owner",
        "integrator",
    ]);
    let cc = lab.submit(combined["id"].as_str().unwrap(), &profile, "integrator");
    let (independent, _) = lab.edit(
        &lab.base,
        "independent",
        "README",
        "useful independent change\n",
    );
    let ci = lab.submit(&independent, &profile, "independent");
    lab.drain();
    assert_eq!(lab.state(&cc), "failed");
    assert_eq!(lab.state(&ci), "passed");
    assert_eq!(lab.call(&["repairs"]).as_array().unwrap().len(), 1);
    lab.refuses(
        &["promote", cc["id"].as_str().unwrap()],
        "complete passing evidence",
    );
    // Checkpoint did not rewrite the author's base, index or working files.
    let path = Path::new(a_view["path"].as_str().unwrap());
    assert_eq!(
        git(path, &["rev-parse", "HEAD"]),
        git(&lab.repo, &["rev-parse", "HEAD"])
    );
    assert_eq!(
        fs::read_to_string(path.join("provider.mjs")).unwrap(),
        "export const offset = 1;\n"
    );
    lab.call(&["review", &independent, "--reviewer", "reviewer"]);
    lab.call(&["promote", ci["id"].as_str().unwrap()]);
    lab.refuses(&["promote", ci["id"].as_str().unwrap()], "frontier changed");
}

#[test]
fn concurrent_requests_share_one_execution_and_detachment_is_per_owner() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let mut children = Vec::new();
    for n in 0..8 {
        children.push(
            lab.command(&[
                "request",
                "--revision",
                &lab.base,
                "--profile",
                &profile,
                "--job",
                "contract",
                "--owner",
                &format!("agent-{n}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        );
    }
    let actions: Vec<_> = children
        .into_iter()
        .map(|p| parsed(p.wait_with_output().unwrap()))
        .collect();
    let first = actions[0]["id"].as_str().unwrap();
    assert!(actions.iter().all(|a| a["id"] == first));
    assert_eq!(
        lab.call(&["cancel", first, "--owner", "agent-0"])["active_interests"],
        7
    );
    lab.drain();
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        1
    );
    assert_eq!(lab.call(&["status"])["actions"][0]["state"], "passed");
}

#[test]
fn operator_priority_admits_a_train_before_earlier_lane_work() {
    let lab = Lab::new();
    // Each job takes the whole two-slot budget, so admission order is observable.
    let profile = lab.profile(json!([
        {"name":"lane","command":["sh","-c","true"],"slots":2,"reuse":"never","report":{"kind":"command"}},
        {"name":"train","command":["sh","-c","true"],"slots":2,"reuse":"never","report":{"kind":"command"}}
    ]));
    let request = |job: &str, owner: &str| {
        lab.call(&[
            "request",
            "--revision",
            &lab.base,
            "--profile",
            &profile,
            "--job",
            job,
            "--owner",
            owner,
        ])["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let lane = request("lane", "api:claude-c");
    let train = request("train", "api:maintainer");
    assert_eq!(
        lab.call(&["priority", "api:maintainer", "1"])["priority"],
        1
    );
    lab.drain();
    let claimed: Vec<String> = lab
        .call(&["events"])
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "attempt.claimed")
        .map(|e| e["detail"]["action"].as_str().unwrap().to_owned())
        .collect();
    // Without the priority, fair share and age would admit the earlier lane request first.
    assert_eq!(claimed, vec![train, lane]);
    assert_eq!(
        lab.call(&["priority", "api:maintainer", "0"])["priority"],
        0
    );
}

#[test]
fn operator_reserve_keeps_local_capacity_for_prioritized_owners() {
    let lab = Lab::new();
    // The job takes the whole two-slot budget, so it never fits beside a one-slot reserve.
    let profile = lab.profile(json!([
        {"name":"prepare","command":["sh","-c","true"],"slots":2,"reuse":"never","report":{"kind":"command"}}
    ]));
    let reserve = |slots: &str, memory_mb: &str| {
        lab.call(&[
            "reserve",
            "local",
            "--slots",
            slots,
            "--memory-mb",
            memory_mb,
        ])
    };
    let listed = || {
        lab.call(&["executors"])
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == "local")
            .unwrap()["reserve"]
            .clone()
    };
    lab.refuses(
        &["reserve", "ghost", "--slots", "1", "--memory-mb", "1024"],
        "not registered",
    );
    lab.refuses(
        &["reserve", "local", "--slots", "2", "--memory-mb", "1024"],
        "below",
    );
    lab.refuses(
        &["reserve", "local", "--slots=-1", "--memory-mb", "1024"],
        "invalid value",
    );
    assert_eq!(listed(), Value::Null);
    assert_eq!(
        reserve("1", "1024"),
        json!({"executor":"local","slots":1,"memory_mb":1024})
    );
    assert_eq!(listed(), json!({"slots":1,"memory_mb":1024}));
    lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "prepare",
        "--owner",
        "api:claude-c",
    ]);
    // The worker skips what a lane can never claim beside the reserve instead of waiting for it.
    lab.drain();
    assert_eq!(lab.call(&["status"])["actions"][0]["state"], "queued");
    lab.call(&["priority", "api:claude-c", "1"]);
    lab.drain();
    assert_eq!(lab.call(&["status"])["actions"][0]["state"], "passed");
    assert_eq!(
        reserve("0", "0"),
        json!({"executor":"local","slots":0,"memory_mb":0})
    );
    assert_eq!(listed(), Value::Null);
}

#[test]
fn successful_jobs_survive_retry_of_a_failed_obligation() {
    let lab = Lab::new();
    let profile = lab.profile(json!([
        {"name":"contract","command":["node","--test","contract.test.mjs"],
         "report":{"kind":"node-tap","minimum":1}},
        {"name":"transient","command":["sh","-c","test -f \"$PATCHWEFT_STATE/ready\""],
         "report":{"kind":"command"}}
    ]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    fs::write(lab.state.join("ready"), "operator repaired prerequisite").unwrap();
    let retry = lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    assert_eq!(retry["retried"], json!(["transient"]));
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        3
    );
}

#[test]
fn skipped_success_is_not_passing_evidence() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"dishonest-exit","command":["sh","-c",
            "printf '# tests 1\\n# pass 0\\n# fail 0\\n# cancelled 0\\n# skipped 1\\n# todo 0\\n'"],
        "report":{"kind":"node-tap","minimum":1}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    assert!(
        lab.call(&["repairs"])[0]["detail"]["error"]
            .as_str()
            .unwrap()
            .contains("execution contract not met")
    );
}

#[test]
fn failed_prerequisite_blocks_only_its_dependent_candidate() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (bad, _) = lab.edit(
        &lab.base,
        "bad",
        "provider.mjs",
        "export const offset = 3;\n",
    );
    let failed = lab.submit(&bad, &profile, "bad");
    lab.drain();
    let (dependent, _) = lab.edit(&bad, "dependent", "README", "dependent\n");
    let blocked = lab.call(&[
        "submit",
        "--revision",
        &dependent,
        "--profile",
        &profile,
        "--owner",
        "dependent",
        "--requires",
        failed["id"].as_str().unwrap(),
    ]);
    let independent = lab.submit(&lab.base, &profile, "independent");
    lab.drain();
    assert_eq!(lab.state(&blocked), "blocked");
    assert_eq!(lab.state(&independent), "passed");
    let status = lab.call(&["status", blocked["id"].as_str().unwrap()]);
    assert_eq!(status["jobs"]["contract"]["state"], "queued");
}

#[test]
fn late_artifacts_go_to_an_owned_view_and_corruption_is_refused() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"build","command":["sh","-c","mkdir dist; cp provider.mjs dist/provider.mjs"],
        "artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
    let candidate = lab.submit(&lab.base, &profile, "author");
    fs::write(
        Path::new(view["path"].as_str().unwrap()).join("provider.mjs"),
        "newer edit\n",
    )
    .unwrap();
    lab.drain();
    let action = candidate["required"]["build"].as_str().unwrap();
    lab.refuses(
        &[
            "deliver",
            action,
            "--destination",
            view["path"].as_str().unwrap(),
        ],
        "never overwritten",
    );
    let destination = lab.state.join("delivered");
    let receipt = lab.call(&[
        "deliver",
        action,
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(destination.join("dist/provider.mjs")).unwrap(),
        "export const offset = 0;\n"
    );
    assert_eq!(
        fs::read_to_string(Path::new(view["path"].as_str().unwrap()).join("provider.mjs")).unwrap(),
        "newer edit\n"
    );
    let retained = lab
        .state
        .join("attempts")
        .join(receipt["attempt"].as_str().unwrap())
        .join("artifacts/dist/provider.mjs");
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&retained, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(retained, "corrupt\n").unwrap();
    lab.refuses(
        &[
            "deliver",
            action,
            "--destination",
            lab.state.join("bad-delivery").to_str().unwrap(),
        ],
        "corrupt",
    );
}

#[test]
fn coordinator_loss_retains_running_execution() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"survivor","command":["sh","-c","touch \"$PATCHWEFT_STATE/started\"; sleep 1; mkdir dist; echo result > dist/value"],
        "artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let mut coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("started").exists() {
        assert!(start.elapsed() < PATIENCE, "worker did not start");
        thread::sleep(Duration::from_millis(10));
    }
    coordinator.kill().unwrap();
    coordinator.wait().unwrap();
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        1
    );
}

#[test]
fn detached_descendants_hold_capacity_and_complete_artifacts() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"descendant","command":["sh","-c",
            "mkdir dist; setsid sh -c 'sleep 0.3; echo finished > dist/value' >/dev/null 2>&1 & exit 0"],
        "slots":2,"artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let output = lab.state.join("output");
    lab.call(&[
        "deliver",
        candidate["required"]["descendant"].as_str().unwrap(),
        "--destination",
        output.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(output.join("dist/value")).unwrap(),
        "finished\n"
    );
}

#[test]
fn audited_projection_reuses_unchanged_inputs_but_invalidates_changed_ones() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"contract","command":["node","--test","contract.test.mjs"],
        "reuse":"files","inputs":["provider.mjs","consumer.mjs","contract.test.mjs"],
        "audit":"Fixture imports exactly these three files and Node builtins. It has no external data inputs.",
        "report":{"kind":"node-tap","minimum":1}
    }]));
    let first = lab.submit(&lab.base, &profile, "first");
    lab.drain();
    let (docs, _) = lab.edit(&lab.base, "docs", "README", "new unrelated documentation\n");
    let second = lab.submit(&docs, &profile, "second");
    assert_eq!(
        first["required"]["contract"],
        second["required"]["contract"]
    );
    assert_eq!(lab.state(&second), "passed");
    let (bad, _) = lab.edit(
        &lab.base,
        "bad",
        "provider.mjs",
        "export const offset = 9;\n",
    );
    let third = lab.submit(&bad, &profile, "third");
    assert_ne!(third["required"]["contract"], first["required"]["contract"]);
    lab.drain();
    assert_eq!(lab.state(&third), "failed");
}

#[test]
fn textual_conflicts_and_export_races_are_explicit() {
    let lab = Lab::new();
    let (a, _) = lab.edit(&lab.base, "a", "README", "a\n");
    let (b, _) = lab.edit(&lab.base, "b", "README", "b\n");
    lab.refuses(
        &[
            "compose",
            "--base",
            &lab.base,
            "--change",
            &a,
            "--change",
            &b,
            "--owner",
            "integrator",
        ],
        "conflicts",
    );
    assert_eq!(lab.call(&["repairs"])[0]["kind"], "source-conflict");
    let destination = lab.state.join("published.git");
    fs::create_dir(&destination).unwrap();
    git(&destination, &["init", "--bare", "--quiet"]);
    let receipt = lab.call(&["export", &a, "--destination", destination.to_str().unwrap()]);
    lab.refuses(
        &["export", &b, "--destination", destination.to_str().unwrap()],
        "update-ref",
    );
    lab.call(&[
        "export",
        &b,
        "--destination",
        destination.to_str().unwrap(),
        "--expected",
        receipt["commit"].as_str().unwrap(),
    ]);
    // The original fixture branch was never changed.
    assert_eq!(
        fs::read_to_string(lab.repo.join("README")).unwrap(),
        "initial\n"
    );
}

#[test]
fn promotion_preserves_the_previously_validated_work() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (a, _) = lab.edit(&lab.base, "a", "provider.mjs", "export const offset = 1;\n");
    let (b, _) = lab.edit(&lab.base, "b", "README", "independent work\n");
    let ca = lab.submit(&a, &profile, "a");
    let cb = lab.submit(&b, &profile, "b");
    lab.drain();
    lab.call(&["review", &a, "--reviewer", "reviewer"]);
    lab.call(&["review", &b, "--reviewer", "reviewer"]);
    lab.call(&["promote", ca["id"].as_str().unwrap()]);
    lab.refuses(
        &[
            "promote",
            cb["id"].as_str().unwrap(),
            "--expected",
            ca["id"].as_str().unwrap(),
        ],
        "does not contain",
    );
    let composed = lab.call(&[
        "compose",
        "--base",
        &a,
        "--change",
        &b,
        "--owner",
        "integrator",
    ]);
    let successor = lab.submit(composed["id"].as_str().unwrap(), &profile, "integrator");
    lab.drain();
    lab.call(&[
        "review",
        composed["id"].as_str().unwrap(),
        "--reviewer",
        "reviewer",
    ]);
    lab.call(&[
        "promote",
        successor["id"].as_str().unwrap(),
        "--expected",
        ca["id"].as_str().unwrap(),
    ]);
}

#[test]
fn failed_external_cleanup_holds_capacity_until_confirmed() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource","command":["sh","-c","true"],"slots":2,
        "cleanup":["sh","-c","test -f \"$PATCHWEFT_STATE/externals-drained\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    lab.refuses(
        &["retry", candidate["id"].as_str().unwrap()],
        "still owns capacity",
    );
    let status = lab.call(&["status"]);
    let attempt = status["actions"][0]["attempt"].as_str().unwrap();
    fs::write(
        lab.state.join("externals-drained"),
        "confirmed by test operator",
    )
    .unwrap();
    lab.call(&["recover", "--confirm-drained", attempt]);
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
}

#[test]
fn timeout_retains_output_and_cleanup_evidence_across_retry() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource", "slots":2, "timeout_seconds":15,
        "command":["sh","-c",
            "printf 'stdout before completion\n'; printf 'stderr before completion\n' >&2; \
             printf '%s\n' \"$PATCHWEFT_ATTEMPT\" > \"$PATCHWEFT_STATE/external-resource\"; \
             if test -f \"$PATCHWEFT_STATE/allow-completion\"; then printf 'retry completed\n'; exit 0; fi; \
             exec sleep 60"],
        "cleanup":["sh","-c",
            "test \"$(cat \"$PATCHWEFT_STATE/external-resource\")\" = \"$PATCHWEFT_ATTEMPT\" && \
             rm \"$PATCHWEFT_STATE/external-resource\" && test ! -e \"$PATCHWEFT_STATE/external-resource\" && \
             printf '%s\n' \"$PATCHWEFT_ATTEMPT\" >> \"$PATCHWEFT_STATE/cleanup-records\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");

    let database = rusqlite::Connection::open(lab.state.join("state.sqlite")).unwrap();
    let (first_id, first_state, first_drained, first_body): (String, String, bool, String) =
        database
            .query_row("SELECT id,state,drained,body FROM attempts", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
    assert_eq!(first_state, "failed");
    assert!(first_drained);
    let first: Value = serde_json::from_str(&first_body).unwrap();
    assert_eq!(first["exit_code"], 124);
    assert_eq!(first["result"]["reason"], "timeout");
    assert_eq!(first["result"]["report"]["execution_failed"], true);
    let log_path = lab
        .state
        .join("attempts")
        .join(&first_id)
        .join("output.log");
    let log = fs::read(&log_path).unwrap();
    let output = String::from_utf8(log.clone()).unwrap();
    assert!(output.contains("stdout before completion\n"));
    assert!(output.contains("stderr before completion\n"));
    assert!(!output.contains("retry completed"));
    assert_eq!(
        first["result"]["log_sha256"],
        format!("{:x}", Sha256::digest(&log))
    );
    assert!(!lab.state.join("external-resource").exists());
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-records")).unwrap(),
        format!("{first_id}\n")
    );

    // The retry consumes all available slots, so completion also checks release
    // of the failed attempt's reservation after its cleanup succeeds.
    fs::write(lab.state.join("allow-completion"), "ready").unwrap();
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    lab.drain();
    assert_eq!(
        lab.state(&candidate),
        "passed",
        "{}",
        database
            .query_row(
                "SELECT body FROM attempts WHERE id != ?",
                [&first_id],
                |row| row.get::<_, String>(0)
            )
            .unwrap()
    );
    let (second_id, second_state, second_drained): (String, String, bool) = database
        .query_row(
            "SELECT id,state,drained FROM attempts WHERE id != ?",
            [&first_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(second_state, "passed");
    assert!(second_drained);
    assert!(!lab.state.join("external-resource").exists());
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-records")).unwrap(),
        format!("{first_id}\n{second_id}\n")
    );
    assert_eq!(fs::read(&log_path).unwrap(), log);
    let retained_body: String = database
        .query_row(
            "SELECT body FROM attempts WHERE id = ?",
            [&first_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_body, first_body);
    assert_eq!(
        database
            .query_row("SELECT COUNT(*) FROM attempts", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn external_cleanup_starts_outside_the_job_workspace() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource","command":["sh","-c","chmod 000 ."],
        "cleanup":["sh","-c",
            "test \"$PWD\" = \"$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT\" && chmod 700 \"$PATCHWEFT_WORKSPACE\" && printf drained > \"$PATCHWEFT_STATE/cleanup-cwd\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-cwd")).unwrap(),
        "drained"
    );
}

#[test]
fn a_tracked_old_report_cannot_supply_fresh_passing_evidence() {
    let lab = Lab::new();
    let stale = json!({"success":true,"numTotalTests":1,"numFailedTests":0,"numFailedTestSuites":0,
                     "testResults":[{"assertionResults":[{"status":"passed"}]}]});
    let (revision, _) = lab.edit(&lab.base, "author", "old-report.json", &stale.to_string());
    let profile = lab.profile(json!([{
        "name":"missing-execution","command":["sh","-c","true"],
        "report":{"kind":"vitest","path":"old-report.json","minimum":1}
    }]));
    let candidate = lab.submit(&revision, &profile, "author");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
}

#[test]
fn a_shared_build_feeds_two_private_downstream_workspaces() {
    let lab = Lab::new();
    let profile=lab.profile(json!([
        {"name":"build","command":["sh","-c","mkdir dist; cp provider.mjs dist/provider.mjs"],
         "artifacts":["dist"],"report":{"kind":"command"}},
        {"name":"consumer-one","dependencies":["build"],
         "command":["sh","-c","test \"$(cat dist/provider.mjs)\" = 'export const offset = 0;'; echo private > dist/provider.mjs"],
         "report":{"kind":"command"}},
        {"name":"consumer-two","dependencies":["build"],
         "command":["sh","-c","sleep 0.2; test \"$(cat dist/provider.mjs)\" = 'export const offset = 0;'"],
         "report":{"kind":"command"}}
    ]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        3
    );
    let database = rusqlite::Connection::open(lab.state.join("state.sqlite")).unwrap();
    let mut statement = database.prepare("SELECT body FROM attempts").unwrap();
    let attempts: Vec<Value> = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    assert_eq!(attempts.len(), 3);
    for attempt in attempts {
        let result = &attempt["result"];
        let preparation = &result["preparation"];
        let expected_bytes = if result["job"] == "build" {
            0
        } else {
            b"export const offset = 0;\n".len() as u64
        };
        assert_eq!(
            preparation["dependency_copy_bytes"].as_u64(),
            Some(expected_bytes)
        );
        assert_eq!(
            preparation["dependency_files"].as_u64(),
            Some(u64::from(expected_bytes > 0))
        );
        for field in [
            "source_seconds",
            "dependency_seconds",
            "dependency_copy_seconds",
        ] {
            assert!(preparation[field].as_f64().unwrap() >= 0.0);
        }
        assert!(
            preparation["dependency_copy_seconds"].as_f64().unwrap()
                <= preparation["dependency_seconds"].as_f64().unwrap()
        );
        assert!(result["command_seconds"].as_f64().unwrap() >= 0.0);
        assert!(result["artifact_capture_seconds"].as_f64().unwrap() >= 0.0);
    }
}

#[test]
fn a_new_request_does_not_join_an_attempt_already_being_canceled() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"slow","command":["node","-e",
            "const fs=require('fs'); const p=process.env.PATCHWEFT_STATE+'/started'; if(fs.existsSync(p)){process.exit(0)}; fs.writeFileSync(p,'first'); process.on('SIGTERM',()=>{}); setTimeout(()=>{},20000)"],
        "report":{"kind":"command"}
    }]));
    let first = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "slow",
        "--owner",
        "first",
    ]);
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("started").exists() {
        assert!(start.elapsed() < PATIENCE);
        thread::sleep(Duration::from_millis(10));
    }
    lab.call(&["cancel", first["id"].as_str().unwrap(), "--owner", "first"]);
    let start = Instant::now();
    loop {
        if lab
            .call(&["events"])
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "attempt.stopping")
        {
            break;
        }
        assert!(start.elapsed() < PATIENCE);
        thread::sleep(Duration::from_millis(10));
    }
    let second = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "slow",
        "--owner",
        "second",
    ]);
    assert_ne!(first["id"], second["id"]);
    parsed(coordinator.wait_with_output().unwrap());
    lab.drain();
    let status = lab.call(&["status"]);
    assert!(
        status["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["id"] == second["id"] && a["state"] == "passed")
    );
}

#[test]
fn supervisor_loss_is_reported_once_and_keeps_capacity_reserved() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"live","slots":2,"command":["node","-e",
            "require('fs').writeFileSync(process.env.PATCHWEFT_STATE+'/child-pid',String(process.pid)); setTimeout(()=>{},20000)"],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("child-pid").exists() {
        assert!(start.elapsed() < PATIENCE);
        thread::sleep(Duration::from_millis(10));
    }
    let events = lab.call(&["events"]);
    let started = events
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "attempt.started")
        .unwrap();
    let supervisor = started["detail"]["pid"].as_i64().unwrap() as i32;
    let child = fs::read_to_string(lab.state.join("child-pid"))
        .unwrap()
        .parse::<i32>()
        .unwrap();
    unsafe {
        assert_eq!(libc::kill(supervisor, libc::SIGKILL), 0);
    }
    parsed(coordinator.wait_with_output().unwrap());
    lab.refuses(
        &["retry", candidate["id"].as_str().unwrap()],
        "still owns capacity",
    );
    lab.call(&["recover"]);
    lab.call(&["recover"]);
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.interrupted")
            .count(),
        1
    );
    unsafe {
        assert_eq!(libc::kill(child, libc::SIGKILL), 0);
    }
    lab.call(&[
        "recover",
        "--confirm-drained",
        started["subject"].as_str().unwrap(),
    ]);
}

#[test]
fn successive_checkpoints_and_reverts_preserve_author_history_without_rewriting_files() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (first, view) = lab.edit(&lab.base, "author", "README", "first checkpoint\n");
    let first_candidate = lab.submit(&first, &profile, "author");
    lab.drain();
    lab.call(&["review", &first, "--reviewer", "reviewer"]);
    lab.call(&["promote", first_candidate["id"].as_str().unwrap()]);
    let workspace = view["id"].as_str().unwrap();
    let path = Path::new(view["path"].as_str().unwrap());
    assert_eq!(
        lab.call(&["checkpoint", workspace, "--message", "no edits"])["id"],
        first
    );
    fs::write(path.join("README"), "second checkpoint\n").unwrap();
    let second = lab.call(&["checkpoint", workspace, "--message", "second"]);
    assert_eq!(second["parents"], json!([first]));
    let second_candidate = lab.submit(second["id"].as_str().unwrap(), &profile, "author");
    lab.drain();
    lab.call(&[
        "review",
        second["id"].as_str().unwrap(),
        "--reviewer",
        "reviewer",
    ]);
    lab.call(&[
        "promote",
        second_candidate["id"].as_str().unwrap(),
        "--expected",
        first_candidate["id"].as_str().unwrap(),
    ]);
    fs::write(path.join("README"), "initial\n").unwrap();
    let reverted = lab.call(&["checkpoint", workspace, "--message", "revert"]);
    assert_ne!(reverted["id"], lab.base);
    assert_eq!(reverted["parents"], json!([second["id"]]));
    assert_eq!(
        git(path, &["rev-parse", "HEAD"]),
        git(&lab.repo, &["rev-parse", "HEAD"])
    );
    assert_eq!(
        fs::read_to_string(path.join("README")).unwrap(),
        "initial\n"
    );
}

#[test]
fn dependency_artifacts_cannot_follow_source_symlinks_outside_the_workspace() {
    use std::os::unix::fs::symlink;
    for dangling in [false, true] {
        let lab = Lab::new();
        let outside = lab.state.join("outside");
        fs::create_dir(&outside).unwrap();
        let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
        let root = Path::new(view["path"].as_str().unwrap());
        // Both a parent symlink and a dangling final symlink must be refused before writing.
        if dangling {
            fs::create_dir(root.join("dist")).unwrap();
            symlink(outside.join("payload"), root.join("dist/payload")).unwrap();
        } else {
            symlink(&outside, root.join("dist")).unwrap();
        }
        let revision = lab.call(&[
            "checkpoint",
            view["id"].as_str().unwrap(),
            "--message",
            "source link",
        ]);
        let profile=lab.profile(json!([
            {"name":"build","command":["sh","-c","rm -rf dist; mkdir dist; echo payload > dist/payload"],
             "artifacts":["dist"],"report":{"kind":"command"}},
            {"name":"consume","dependencies":["build"],"command":["sh","-c","true"],
             "report":{"kind":"command"}}
        ]));
        let candidate = lab.submit(revision["id"].as_str().unwrap(), &profile, "author");
        lab.drain();
        assert_eq!(lab.state(&candidate), "failed");
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    }
}

#[test]
fn concurrent_checkpoints_of_one_workspace_publish_one_tip() {
    let lab = Lab::new();
    let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
    fs::write(
        Path::new(view["path"].as_str().unwrap()).join("README"),
        "edited\n",
    )
    .unwrap();
    let mut children = Vec::new();
    for index in 0..4 {
        children.push(
            lab.command(&[
                "checkpoint",
                view["id"].as_str().unwrap(),
                "--message",
                &format!("concurrent requester {index}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        );
    }
    let revisions: Vec<Value> = children
        .into_iter()
        .map(|child| parsed(child.wait_with_output().unwrap()))
        .collect();
    assert!(
        revisions
            .iter()
            .all(|revision| revision["id"] == revisions[0]["id"])
    );
    assert_eq!(revisions[0]["parents"], json!([lab.base]));
    let workspaces = lab.call(&["list", "workspaces"]);
    assert_eq!(workspaces[0]["tip"], revisions[0]["id"]);
}

#[test]
fn multiple_repositories_keep_frontiers_separate_and_share_explicit_prerequisites() {
    let lab = Lab::new();
    let python_repo = lab.state.join("python-source");
    fs::create_dir(&python_repo).unwrap();
    git(&python_repo, &["init", "--quiet"]);
    fs::write(
        python_repo.join("contract.py"),
        "def total(values):\n    return sum(values)\n\nassert total([2, 3]) == 5\n",
    )
    .unwrap();
    git(&python_repo, &["add", "."]);
    git(&python_repo, &["commit", "--quiet", "-m", "Python tool"]);
    let python_revision = lab.call(&[
        "import",
        "--name",
        "python-tool",
        "--repo",
        python_repo.to_str().unwrap(),
        "--owner",
        "python-agent",
    ]);
    let profile_path = lab.state.join("python-profile.json");
    fs::write(
        &profile_path,
        serde_json::to_vec(&json!({
            "version":1,"name":"python-tool","certifying":true,
            "tools":{"python":"/usr/bin/python3"},
            "jobs":[{"name":"contract","command":["python","contract.py"],
                     "report":{"kind":"command"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    let python_profile = lab.call(&["register", profile_path.to_str().unwrap()]);
    let javascript_profile = lab.contract_profile();
    let (broken, _) = lab.edit(
        &lab.base,
        "javascript-agent",
        "provider.mjs",
        "export const offset = 3;\n",
    );
    let javascript = lab.submit(&broken, &javascript_profile, "javascript-agent");
    let independent = lab.submit(
        python_revision["id"].as_str().unwrap(),
        python_profile["id"].as_str().unwrap(),
        "python-agent",
    );
    let dependent = lab.call(&[
        "submit",
        "--revision",
        python_revision["id"].as_str().unwrap(),
        "--profile",
        python_profile["id"].as_str().unwrap(),
        "--owner",
        "release-agent",
        "--requires",
        javascript["id"].as_str().unwrap(),
    ]);
    lab.drain();
    assert_eq!(lab.state(&javascript), "failed");
    assert_eq!(lab.state(&independent), "passed");
    assert_eq!(lab.state(&dependent), "blocked");
    assert_ne!(
        javascript["required"]["contract"],
        independent["required"]["contract"]
    );
    lab.call(&[
        "review",
        python_revision["id"].as_str().unwrap(),
        "--reviewer",
        "python-reviewer",
    ]);
    lab.call(&["promote", independent["id"].as_str().unwrap()]);
    let projects = lab.call(&["list", "projects"]);
    assert_eq!(projects.as_array().unwrap().len(), 2);
    let python = projects
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "python-tool")
        .unwrap();
    let javascript_project = projects
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "fixture")
        .unwrap();
    assert_eq!(python["validated_candidate"], independent["id"]);
    assert!(javascript_project["validated_candidate"].is_null());
    lab.refuses(
        &[
            "compose",
            "--base",
            &lab.base,
            "--change",
            python_revision["id"].as_str().unwrap(),
            "--owner",
            "integrator",
        ],
        "single project",
    );
}

#[test]
fn executable_symlinks_preserve_multicall_dispatch_names() {
    let lab = Lab::new();
    let tool = lab.state.join("cargo-fixture");
    std::os::unix::fs::symlink("/usr/bin/sh", &tool).unwrap();
    let path = lab.state.join("multicall-profile.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "version":1,"name":"multicall","tools":{"cargo":tool},
            "jobs":[{"name":"dispatch","command":["cargo","-c",
                "case \"$0\" in */cargo-fixture) exit 0 ;; *) exit 3 ;; esac"],
                "report":{"kind":"command"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    let profile = lab.call(&["register", path.to_str().unwrap()]);
    assert_eq!(
        profile["definition"]["tools"]["cargo"],
        tool.to_str().unwrap()
    );
    let candidate = lab.submit(&lab.base, profile["id"].as_str().unwrap(), "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
}

#[test]
fn reservation_only_change_retains_every_passed_obligation() {
    let lab = Lab::new();
    let p1 = lab.profile(chain(BUILD, TEST_B, &[]));
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    assert_eq!(lab.state(&first), "passed");
    let p2 = lab.profile(chain(
        BUILD,
        TEST_B,
        &[
            ("test_a", json!({"memory_mb":2048})),
            ("build", json!({"timeout_seconds":3600})),
            ("lint", json!({"slots":2})),
        ],
    ));
    assert_ne!(p1, p2);
    let second = lab.retain(&lab.base, &p2, "agent", &first);
    assert_eq!(second["required"], first["required"]);
    assert_eq!(
        names(&second["retained"]),
        ["build", "lint", "test_a", "test_b"]
    );
    assert!(second.get("not_retained").is_none(), "{second}");
    assert_eq!(second["retained_from"], first["id"]);
    assert_eq!(lab.state(&second), "passed");
    lab.drain();
    assert_eq!(lab.count("attempt.started"), 4);
    assert_eq!(lab.count("action.retained"), 4);
}

#[test]
fn plain_resubmit_after_profile_change_is_a_fresh_observation() {
    let lab = Lab::new();
    let p1 = lab.profile(chain(BUILD, TEST_B, &[]));
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    let p2 = lab.profile(chain(
        BUILD,
        TEST_B,
        &[("test_a", json!({"memory_mb":2048}))],
    ));
    let second = lab.submit(&lab.base, &p2, "agent");
    for (name, id) in second["required"].as_object().unwrap() {
        assert_ne!(&first["required"][name], id, "{name}");
    }
    assert!(second.get("retained").is_none());
    lab.drain();
    assert_eq!(lab.state(&second), "passed");
    assert_eq!(lab.count("attempt.started"), 8);
}

#[test]
fn changed_command_reruns_only_that_job_and_its_consumers() {
    let lab = Lab::new();
    let p1 = lab.profile(chain(BUILD, TEST_B, &[]));
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    let p2 = lab.profile(chain(BUILD, "grep -q '^built$' dist/out", &[]));
    let second = lab.retain(&lab.base, &p2, "agent", &first);
    assert_eq!(names(&second["retained"]), ["build", "lint", "test_a"]);
    assert_eq!(
        second["not_retained"],
        json!({"test_b":"job identity changed"})
    );
    assert_ne!(second["required"]["test_b"], first["required"]["test_b"]);

    let p3 = lab.profile(chain(
        "mkdir dist; echo built again > dist/out",
        TEST_B,
        &[],
    ));
    let third = lab.retain(&lab.base, &p3, "agent", &first);
    assert_eq!(names(&third["retained"]), ["lint"]);
    assert_eq!(names(&third["not_retained"]), ["build", "test_a", "test_b"]);
    let build = third["required"]["build"].as_str().unwrap();
    assert_ne!(build, first["required"]["build"]);
    for consumer in ["test_a", "test_b"] {
        let action = lab.body("actions", third["required"][consumer].as_str().unwrap());
        assert_eq!(action["dependencies"], json!([build]));
    }
    lab.drain();
    assert_eq!(lab.state(&second), "passed");
    assert_eq!(lab.state(&third), "passed");
    assert_eq!(lab.count("attempt.started"), 4 + 1 + 3);
}

#[test]
fn failed_upstream_then_reservation_fix_wires_consumers_to_new_upstream() {
    let lab = Lab::new();
    let gated = "test -f \"$PATCHWEFT_STATE/ready\" && mkdir dist && echo built > dist/out";
    let p1 = lab.profile(chain(gated, TEST_B, &[]));
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    assert_eq!(lab.state(&first), "failed");
    let status = lab.call(&["status", first["id"].as_str().unwrap()]);
    assert_eq!(status["jobs"]["test_a"]["state"], "queued");
    assert_eq!(status["jobs"]["lint"]["state"], "passed");

    fs::write(lab.state.join("ready"), "fixed").unwrap();
    let p2 = lab.profile(chain(
        gated,
        TEST_B,
        &[("build", json!({"memory_mb":2048}))],
    ));
    let second = lab.retain(&lab.base, &p2, "agent", &first);
    assert_eq!(names(&second["retained"]), ["lint"]);
    let build = second["required"]["build"].as_str().unwrap();
    assert_ne!(build, first["required"]["build"]);
    assert_eq!(lab.body("actions", build)["reservation"]["memory_mb"], 2048);
    for consumer in ["test_a", "test_b"] {
        assert_ne!(second["required"][consumer], first["required"][consumer]);
        let action = lab.body("actions", second["required"][consumer].as_str().unwrap());
        assert_eq!(action["dependencies"], json!([build]));
    }
    lab.drain();
    assert_eq!(lab.state(&second), "passed");
}

#[test]
fn any_declared_tool_byte_change_reruns_every_job() {
    let lab = Lab::new();
    let tool = lab.state.join("unused-adapter");
    fs::write(&tool, "#!/bin/sh\nexit 0\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    let definition = json!({"version":1,"name":"tools","certifying":true,
        "tools":{"sh":"/usr/bin/sh","adapter":tool},"jobs":chain(BUILD, TEST_B, &[])});
    let p1 = lab.register("tools", definition.clone());
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    assert_eq!(lab.state(&first), "passed");
    fs::write(&tool, "#!/bin/sh\nexit 1\n").unwrap();
    let p2 = lab.register("tools", definition);
    assert_ne!(p1, p2);
    let second = lab.retain(&lab.base, &p2, "agent", &first);
    assert!(second.get("retained").is_none(), "{second}");
    assert_eq!(
        names(&second["not_retained"]),
        ["build", "lint", "test_a", "test_b"]
    );
    for reason in second["not_retained"].as_object().unwrap().values() {
        assert_eq!(reason, "job identity changed");
    }
}

#[test]
fn export_slots_false_hides_variable_and_leaves_identity() {
    let lab = Lab::new();
    let record = "mkdir out; env > out/env";
    for export in [false, true] {
        let name = format!("env-{export}");
        let jobs = |slots: u32| {
            json!([job(
                &name,
                record,
                json!({"artifacts":["out"],"slots":slots,"export_slots":export})
            )])
        };
        let p1 = lab.profile(jobs(1));
        let first = lab.submit(&lab.base, &p1, "agent");
        lab.drain();
        assert_eq!(lab.state(&first), "passed");
        let destination = lab.state.join(format!("delivered-{export}"));
        lab.call(&[
            "deliver",
            first["required"][&name].as_str().unwrap(),
            "--destination",
            destination.to_str().unwrap(),
        ]);
        let environment = fs::read_to_string(destination.join("out/env")).unwrap();
        assert_eq!(environment.contains("PATCHWEFT_SLOTS=1\n"), export);
        assert!(environment.contains("PATCHWEFT_REVISION="));
        let p2 = lab.profile(jobs(2));
        let second = lab.retain(&lab.base, &p2, "agent", &first);
        if export {
            assert_eq!(second["not_retained"][&name], "job identity changed");
        } else {
            assert_eq!(second["retained"], json!([name]));
        }
    }
}

#[test]
fn a_pass_that_creates_patchweft_vacuous_is_marked_vacuous() {
    let lab = Lab::new();
    // Every job first checks the path is absent at launch, in its attempt directory and outside
    // its checkout view (its working directory).
    let fresh = "test ! -e \"$PATCHWEFT_VACUOUS\" && \
                 test \"$PATCHWEFT_VACUOUS\" = \"$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT/vacuous\" && \
                 case \"$PATCHWEFT_VACUOUS\" in \"$PWD\"/*) exit 3;; esac";
    let profile = lab.profile(json!([
        job(
            "skipped",
            &format!("{fresh} && touch \"$PATCHWEFT_VACUOUS\""),
            json!({})
        ),
        job("worked", fresh, json!({})),
        job(
            "refused",
            &format!("{fresh} && touch \"$PATCHWEFT_VACUOUS\" && exit 1"),
            json!({})
        ),
    ]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    for (job, state, created, vacuous) in [
        ("skipped", "passed", true, Some(&json!(true))),
        ("worked", "passed", false, None),
        ("refused", "failed", true, None),
    ] {
        let action = candidate["required"][job].as_str().unwrap();
        let (attempt, recorded): (String, String) = lab
            .database()
            .query_row(
                "SELECT attempt, state FROM actions WHERE id=?",
                [action],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(recorded, state, "{job}");
        assert_eq!(
            lab.state
                .join("attempts")
                .join(&attempt)
                .join("vacuous")
                .exists(),
            created,
            "{job}"
        );
        let evidence = &lab.body("attempts", &attempt)["result"];
        assert_eq!(evidence.get("vacuous"), vacuous, "{job}: {evidence}");
        // Only the pass that ran its work measures how long its job takes.
        let samples: i64 = lab
            .database()
            .query_row(
                "SELECT COUNT(*) FROM job_estimates WHERE job=?",
                [job],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(samples, i64::from(job == "worked"), "{job}");
    }
}

#[test]
fn timeout_is_a_key_and_acceptance_budget_not_identity() {
    let lab = Lab::new();
    let jobs = |name: &str, script: &str, timeout: u64| {
        json!([job(name, script, json!({"timeout_seconds":timeout}))])
    };
    // Case A: a pass that took about 1.5 s cannot certify a 1 s budget.
    // Loose budgets stay far above source preparation (~5 s for a debug binary) under load.
    let p10 = lab.profile(jobs("sleepy", "sleep 1.5", 120));
    let first = lab.submit(&lab.base, &p10, "agent");
    lab.drain();
    assert_eq!(lab.state(&first), "passed");
    let p1 = lab.profile(jobs("sleepy", "sleep 1.5", 1));
    let tight = lab.retain(&lab.base, &p1, "agent", &first);
    assert_eq!(
        tight["not_retained"]["sleepy"],
        "observation exceeded the timeout budget"
    );
    assert_ne!(tight["required"]["sleepy"], first["required"]["sleepy"]);
    lab.drain();
    assert_eq!(lab.state(&tight), "failed");
    let failed = lab.body("actions", tight["required"]["sleepy"].as_str().unwrap());
    assert_eq!(failed["identity"]["timeout_seconds"], 1);
    let attempt: String = lab
        .database()
        .query_row(
            "SELECT attempt FROM actions WHERE id=?",
            [tight["required"]["sleepy"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(lab.body("attempts", &attempt)["exit_code"], 124);
    let p20 = lab.profile(jobs("sleepy", "sleep 1.5", 240));
    let loose = lab.retain(&lab.base, &p20, "agent", &first);
    assert_eq!(loose["retained"], json!(["sleepy"]));
    assert_eq!(loose["required"], first["required"]);

    // Case B: the main process exits at once, but a detached descendant holds the timer.
    let detached = "setsid sh -c 'sleep 3' >/dev/null 2>&1 & exit 0";
    let p10 = lab.profile(jobs("detached", detached, 30));
    let first = lab.submit(&lab.base, &p10, "agent");
    lab.drain();
    assert_eq!(lab.state(&first), "passed");
    let attempt: String = lab
        .database()
        .query_row(
            "SELECT attempt FROM actions WHERE id=?",
            [first["required"]["detached"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    let evidence = &lab.body("attempts", &attempt)["result"];
    // The budget includes source preparation, which varies with host load. This budget
    // exceeds preparation plus the main process by a second but not the descendant's 3 s.
    let preparation = evidence["preparation"]["source_seconds"].as_f64().unwrap()
        + evidence["preparation"]["dependency_seconds"]
            .as_f64()
            .unwrap();
    assert!(
        evidence["timer_seconds"].as_f64().unwrap() >= preparation + 3.0,
        "{evidence}"
    );
    let budget = (preparation + 1.0).ceil() as u64;
    let tight = lab.retain(
        &lab.base,
        &lab.profile(jobs("detached", detached, budget)),
        "agent",
        &first,
    );
    assert_eq!(
        tight["not_retained"]["detached"],
        "observation exceeded the timeout budget"
    );

    // Case C: equal recipes with different timeouts never share outstanding work.
    let p10 = lab.profile(jobs("queued", "true", 10));
    let p20 = lab.profile(jobs("queued", "true", 20));
    let a = lab.submit(&lab.base, &p10, "agent");
    let b = lab.submit(&lab.base, &p20, "agent");
    assert_ne!(a["required"]["queued"], b["required"]["queued"]);
}

#[test]
fn queued_share_raises_reservation_running_join_requires_enough() {
    let lab = Lab::new();
    let jobs = |memory: u32| json!([job("work", "true", json!({"memory_mb":memory}))]);
    let p1 = lab.profile(jobs(512));
    let p2 = lab.profile(jobs(1024));
    let p3 = lab.profile(jobs(256));
    let first = lab.submit(&lab.base, &p1, "one");
    let second = lab.submit(&lab.base, &p2, "two");
    let third = lab.submit(&lab.base, &p3, "three");
    let action = first["required"]["work"].as_str().unwrap();
    assert_eq!(second["required"]["work"], action);
    assert_eq!(third["required"]["work"], action);
    let raised: Vec<Value> = lab
        .call(&["events"])
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "action.reservation_raised")
        .cloned()
        .collect();
    assert_eq!(raised.len(), 1);
    assert_eq!(raised[0]["detail"]["from"]["memory_mb"], 512);
    assert_eq!(raised[0]["detail"]["to"]["memory_mb"], 1024);
    assert_eq!(raised[0]["detail"]["requesting_profile"], p2);
    assert_eq!(
        lab.body("actions", action)["reservation"]["memory_mb"],
        1024
    );
    lab.drain();
    let claimed = lab
        .call(&["events"])
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "attempt.claimed")
        .cloned()
        .unwrap();
    assert_eq!(claimed["detail"]["memory_mb"], 1024);
    let attempt = lab.body("attempts", claimed["subject"].as_str().unwrap());
    assert_eq!(attempt["memory_mb"], 1024);
    assert_eq!(attempt["result"]["budget"]["memory_mb"], 1024);

    // A running attempt's reservation is fixed: only requesters it already covers join.
    let gate = "touch \"$PATCHWEFT_STATE/gate-started\"; \
                while [ ! -f \"$PATCHWEFT_STATE/gate-release\" ]; do sleep 0.05; done";
    let gated = |memory: u32| json!([job("gate", gate, json!({"memory_mb":memory}))]);
    let p1 = lab.profile(gated(512));
    let running = lab.submit(&lab.base, &p1, "one");
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("gate-started").exists() {
        assert!(start.elapsed() < PATIENCE, "worker did not start");
        thread::sleep(Duration::from_millis(10));
    }
    let larger = lab.submit(&lab.base, &lab.profile(gated(1024)), "two");
    let smaller = lab.submit(&lab.base, &lab.profile(gated(256)), "three");
    assert_ne!(larger["required"]["gate"], running["required"]["gate"]);
    assert_eq!(smaller["required"]["gate"], running["required"]["gate"]);
    fs::write(lab.state.join("gate-release"), "go").unwrap();
    parsed(coordinator.wait_with_output().unwrap());
    lab.drain();
    assert_eq!(lab.state(&running), "passed");
    assert_eq!(lab.state(&larger), "passed");
}

#[test]
fn retry_raises_reservation_to_retrying_profile() {
    let lab = Lab::new();
    let jobs = |memory: u32| {
        json!([job(
            "flaky",
            "test -f \"$PATCHWEFT_STATE/ready\"",
            json!({"memory_mb":memory})
        )])
    };
    let p1 = lab.profile(jobs(512));
    let first = lab.submit(&lab.base, &p1, "one");
    let action = first["required"]["flaky"].as_str().unwrap();
    // Every public join already raises or guards the reservation, so seed a P2 candidate that
    // holds the action without having raised it.
    let p2 = lab.profile(jobs(1024));
    let second = lab.submit(&lab.base, &p2, "two");
    assert_eq!(second["required"]["flaky"], action);
    lab.database()
        .execute(
            "UPDATE actions SET body=json_set(body,'$.reservation.memory_mb',512) WHERE id=?",
            [action],
        )
        .unwrap();
    lab.drain();
    assert_eq!(lab.state(&second), "failed");
    fs::write(lab.state.join("ready"), "repaired").unwrap();
    let retry = lab.call(&["retry", second["id"].as_str().unwrap()]);
    assert_eq!(retry["retried"], json!(["flaky"]));
    assert_eq!(
        lab.body("actions", action)["reservation"]["memory_mb"],
        1024
    );
    lab.drain();
    assert_eq!(lab.state(&second), "passed");
    let attempt: String = lab
        .database()
        .query_row("SELECT attempt FROM actions WHERE id=?", [action], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(lab.body("attempts", &attempt)["memory_mb"], 1024);
}

#[test]
fn certificate_v2_attributes_receipts_and_is_queryable() {
    use patchweft::api::{self, Api, Principal, Query};
    let lab = Lab::new();
    let p1 = lab.profile(chain(BUILD, TEST_B, &[]));
    let first = lab.submit(&lab.base, &p1, "agent");
    lab.drain();
    let p2 = lab.profile(chain(
        BUILD,
        TEST_B,
        &[("test_a", json!({"memory_mb":2048}))],
    ));
    let second = lab.retain(&lab.base, &p2, "agent", &first);
    lab.call(&["review", &lab.base, "--reviewer", "reviewer"]);
    let certificate = lab.call(&["promote", second["id"].as_str().unwrap()]);
    assert_eq!(certificate["certificate"], 2);
    assert_eq!(certificate["profile"], p2);
    assert_eq!(certificate["retained_from"], first["id"]);

    let project = lab.call(&["list", "projects"])[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let store = patchweft::store::Store::open(&lab.state).unwrap();
    let credential = lab.state.join("reader.credential");
    api::grant(
        &store,
        Principal {
            id: "reader".into(),
            projects: [project.clone()].into(),
            profiles: [p2.clone()].into(),
            roles: ["read".to_owned()].into(),
            max_pending: 1,
        },
        &credential,
    )
    .unwrap();
    let token = fs::read_to_string(&credential).unwrap();
    let api = Api::open(&lab.state).unwrap();
    let profiles = api.query(&token, &Query::Profiles).unwrap();
    let jobs = profiles["items"][0]["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 4);
    for job in jobs {
        let name = job["name"].as_str().unwrap();
        let receipt = &certificate["receipts"][name];
        assert_eq!(receipt["route"], "retained", "{name}");
        assert_eq!(receipt["executed_under"], p1);
        assert_eq!(receipt["evidence"]["profile"], p1);
        assert_eq!(receipt["recipe"], job["recipe"]);
        assert_eq!(receipt["evidence"]["identity"]["recipe"], job["recipe"]);
        assert!(receipt["observed"]["started"].as_f64().is_some());
        assert_eq!(receipt["attempts"]["total"], 1);
        assert_eq!(receipt["attempts"]["failed"], 0);
    }
    let frontier = api
        .query(
            &token,
            &Query::Frontier {
                project: project.clone(),
            },
        )
        .unwrap();
    assert_eq!(frontier["candidate"], second["id"]);
    assert_eq!(frontier["certificate"], certificate);
    assert_eq!(
        lab.call(&["frontier", &project])["certificate"],
        certificate
    );
}

#[test]
fn exploratory_and_certifying_never_share_or_retain() {
    let lab = Lab::new();
    let definition = |certifying: bool| {
        json!({"version":1,"name":"classes","certifying":certifying,
               "tools":{"sh":"/usr/bin/sh"},"jobs":[job("work", "true", json!({}))]})
    };
    let exploratory = lab.register("exploratory", definition(false));
    let certifying = lab.register("certifying", definition(true));
    let first = lab.submit(&lab.base, &exploratory, "explorer");
    let second = lab.submit(&lab.base, &certifying, "certifier");
    assert_ne!(first["required"]["work"], second["required"]["work"]);
    lab.drain();
    assert_eq!(lab.state(&first), "passed");
    let third = lab.retain(&lab.base, &certifying, "certifier", &first);
    assert_eq!(
        third["not_retained"],
        json!({"work":"evidence class differs"})
    );
    assert_ne!(third["required"]["work"], first["required"]["work"]);
}

#[test]
fn never_jobs_are_not_retained() {
    let lab = Lab::new();
    let jobs = |memory: u32| {
        json!([
            job("fresh", "true", json!({"reuse":"never","memory_mb":memory})),
            job("shared", "true", json!({"memory_mb":memory}))
        ])
    };
    let first = lab.submit(&lab.base, &lab.profile(jobs(512)), "agent");
    lab.drain();
    let second = lab.retain(&lab.base, &lab.profile(jobs(1024)), "agent", &first);
    assert_eq!(second["retained"], json!(["shared"]));
    assert_eq!(
        second["not_retained"],
        json!({"fresh":"reuse policy is never"})
    );
}

#[test]
fn retention_refused_across_revisions_and_plain_ids_unchanged() {
    let lab = Lab::new();
    let profile = lab.profile(chain(BUILD, TEST_B, &[]));
    let first = lab.submit(&lab.base, &profile, "agent");
    assert_eq!(
        first["id"],
        patchweft::identity(&json!({"revision":lab.base,"profile":profile,"prerequisites":[]}))
            .unwrap()
    );
    let (edited, _) = lab.edit(&lab.base, "author", "README", "edited\n");
    lab.refuses(
        &[
            "submit",
            "--revision",
            &edited,
            "--profile",
            &profile,
            "--owner",
            "agent",
            "--retain-from",
            first["id"].as_str().unwrap(),
        ],
        "same revision",
    );
}

#[test]
fn legacy_rows_still_claim_status_promote_deliver() {
    let lab = Lab::new();
    let profile = lab.profile(chain(BUILD, TEST_B, &[]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let project = lab.call(&["list", "projects"])[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let commit = lab.call(&["list", "revisions"])[0]["commit"]
        .as_str()
        .unwrap()
        .to_owned();
    let database = lab.database();
    for name in ["build", "test_a", "test_b", "lint"] {
        let id = candidate["required"][name].as_str().unwrap();
        let dependencies = lab.body("actions", id)["dependencies"].clone();
        let key = patchweft::identity(&json!({"project":project,"profile":profile,"job":name,
            "inputs":{"commit":commit},"dependencies":dependencies}))
        .unwrap();
        database
            .execute(
                "UPDATE actions SET key=?1,body=json_set(json_remove(body,'$.identity',
                 '$.reservation','$.origin'),'$.key',?1) WHERE id=?2",
                rusqlite::params![key, id],
            )
            .unwrap();
        assert!(lab.body("actions", id).get("identity").is_none());
    }
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    lab.call(&["review", &lab.base, "--reviewer", "reviewer"]);
    let certificate = lab.call(&["promote", candidate["id"].as_str().unwrap()]);
    assert_eq!(
        certificate["receipts"]["build"]["action"],
        candidate["required"]["build"]
    );
    let destination = lab.state.join("legacy-delivery");
    lab.call(&[
        "deliver",
        candidate["required"]["build"].as_str().unwrap(),
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(destination.join("dist/out")).unwrap(),
        "built\n"
    );
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

impl Lab {
    fn events_of(&self, kind: &str) -> Vec<Value> {
        self.call(&["events"])
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == kind)
            .cloned()
            .collect()
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
}

#[test]
fn second_coordinator_waits_as_standby() {
    let lab = Lab::new();
    let profile = lab.profile(json!([job(
        "gate",
        "touch \"$PATCHWEFT_STATE/started\"; \
         while ! test -f \"$PATCHWEFT_STATE/release\"; do sleep 0.05; done",
        json!({})
    )]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let mut first = lab
        .command(&["worker"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("the first coordinator's job", || {
        lab.state.join("started").exists()
    });
    let mut standby = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    fs::write(lab.state.join("release"), "go").unwrap();
    wait_until("the job to pass", || lab.state(&candidate) == "passed");
    thread::sleep(Duration::from_millis(500));
    // Nothing is left to drain, yet the standby waits while the first coordinator holds the lock.
    assert!(standby.try_wait().unwrap().is_none());
    first.kill().unwrap();
    first.wait().unwrap();
    let output = standby.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains(r#"{"executor":"local","waiting_for":"executor lock"}"#),
        "{stderr}"
    );
    let result = parsed(output);
    assert_eq!(result, json!({"executor":"local","launched":0}));
    assert_eq!(lab.count("attempt.started"), 1);
}

#[test]
fn worker_refuses_legacy_held_attempts() {
    let lab = Lab::new();
    let profile = lab.profile(json!([job("work", "true", json!({}))]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let action = candidate["required"]["work"].as_str().unwrap();
    // Held by the previous binary's worker: no executor and no lease.
    let body = json!({"id":"legacy","action":action,"worker_pid":1,"worker_start":"1",
        "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1024,"created":0.0,
        "started":0.5,"ended":null,"exit_code":null,"result":null});
    let database = lab.database();
    database
        .execute(
            "INSERT INTO attempts VALUES ('legacy',?,'running',0,?)",
            rusqlite::params![action, body.to_string()],
        )
        .unwrap();
    database
        .execute(
            "UPDATE actions SET state='running',attempt='legacy' WHERE id=?",
            [action],
        )
        .unwrap();
    lab.refuses(
        &["worker", "--drain"],
        "drain attempts started by the previous binary before upgrading: legacy",
    );
    assert_eq!(lab.row("attempts", "legacy"), ("running".into(), false));
    // Its supervisor is gone; once an operator confirms the drain, work resumes.
    lab.call(&["recover", "--confirm-drained", "legacy"]);
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
}

#[test]
fn cancel_records_stopping_before_the_executor_acts() {
    let lab = Lab::new();
    let profile = lab.profile(json!([job(
        "slow",
        "touch \"$PATCHWEFT_STATE/started\"; exec sleep 30",
        json!({})
    )]));
    let action = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "slow",
        "--owner",
        "first",
    ])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("the job", || lab.state.join("started").exists());
    let started = &lab.events_of("attempt.started")[0];
    let attempt = started["subject"].as_str().unwrap();
    let supervisor = started["detail"]["pid"].as_i64().unwrap() as i32;
    // Neither the coordinator nor the supervisor can act on the cancellation.
    coordinator.kill().unwrap();
    coordinator.wait().unwrap();
    unsafe {
        assert_eq!(libc::kill(supervisor, libc::SIGSTOP), 0);
    }
    lab.call(&["cancel", &action, "--owner", "first"]);
    assert_eq!(lab.row("actions", &action).0, "stopping");
    let stopping = lab.events_of("attempt.stopping");
    assert_eq!(stopping.len(), 1);
    assert_eq!(stopping[0]["subject"], attempt);
    assert_eq!(
        stopping[0]["detail"],
        json!({"reason":"no-requesters","executor":"local"})
    );
    let control = lab.state.join("attempts").join(attempt).join("control");
    assert!(!control.join("stop").exists());
    unsafe {
        assert_eq!(libc::kill(supervisor, libc::SIGCONT), 0);
    }
    // The next coordinator takes the attempt up and relays the recorded stop.
    lab.drain();
    assert_eq!(lab.row("attempts", attempt), ("canceled".into(), true));
    assert_eq!(lab.row("actions", &action).0, "canceled");
    assert_eq!(
        fs::read_to_string(control.join("stop")).unwrap(),
        r#"{"reason":"no-requesters"}"#
    );
    assert_eq!(lab.count("attempt.stopping"), 1);
}

#[test]
fn timeout_stopping_is_recorded_through_renew() {
    let lab = Lab::new();
    let profile = lab.profile(json!([job(
        "slow",
        "exec sleep 30",
        json!({"timeout_seconds":1})
    )]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    let stopping = lab.events_of("attempt.stopping");
    assert_eq!(stopping.len(), 1);
    assert_eq!(
        stopping[0]["detail"],
        json!({"reason":"timeout","executor":"local"})
    );
    let attempt = lab.body("attempts", stopping[0]["subject"].as_str().unwrap());
    assert_eq!(attempt["stop"], "timeout");
    assert_eq!(attempt["exit_code"], 124);
    assert_eq!(attempt["result"]["reason"], "timeout");
    assert_eq!(lab.call(&["repairs"]).as_array().unwrap().len(), 1);
}

/// A profile declaring `drift`, a tool file the test rewrites.
fn drifting(lab: &Lab, jobs: Value) -> (String, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let tool = lab.state.join("drift");
    fs::write(&tool, "#!/bin/sh\n").unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    let profile = lab.register(
        "drifting",
        json!({"version":1,"name":"drifting","certifying":true,
               "tools":{"sh":"/usr/bin/sh","drift":tool},"jobs":jobs}),
    );
    (profile, tool)
}

fn refused(lab: &Lab) -> Value {
    lab.body("executors", "local")["observed"]["refused"].clone()
}

#[test]
fn tool_drift_refuses_profile_without_attempts_or_repairs() {
    let lab = Lab::new();
    let (profile, tool) = drifting(&lab, json!([job("work", "true", json!({}))]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    fs::write(&tool, "#!/bin/sh\n# changed\n").unwrap();
    lab.drain();
    let action = candidate["required"]["work"].as_str().unwrap();
    assert_eq!(lab.row("actions", action).0, "queued");
    assert_eq!(lab.count("attempt.claimed"), 0);
    assert!(lab.call(&["repairs"]).as_array().unwrap().is_empty());
    let error = refused(&lab)[&profile].as_str().unwrap().to_owned();
    assert!(error.contains("declared tool bytes changed"), "{error}");
    let observed = lab.events_of("executor.observed");
    assert_eq!(
        observed.last().unwrap()["detail"]["refused"][&profile],
        error
    );
}

#[test]
fn tool_drift_after_verification_requeues_without_repair() {
    let lab = Lab::new();
    let (profile, tool) = drifting(
        &lab,
        json!([
            job(
                "first",
                "touch \"$PATCHWEFT_STATE/first-started\"; \
                 while ! test -f \"$PATCHWEFT_STATE/release\"; do sleep 0.05; done",
                json!({})
            ),
            job("second", "true", json!({"dependencies":["first"]}))
        ]),
    );
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The coordinator verified the profile before claiming `first`; the tool drifts afterwards.
    wait_until("the first job", || lab.state.join("first-started").exists());
    fs::write(&tool, "#!/bin/sh\n# changed\n").unwrap();
    fs::write(lab.state.join("release"), "go").unwrap();
    parsed(coordinator.wait_with_output().unwrap());
    let second = candidate["required"]["second"].as_str().unwrap();
    assert_eq!(
        lab.row("actions", candidate["required"]["first"].as_str().unwrap())
            .0,
        "passed"
    );
    assert_eq!(lab.row("actions", second).0, "queued");
    let launch = lab.events_of("attempt.launch_failed");
    assert_eq!(launch.len(), 1);
    assert_eq!(launch[0]["detail"]["reason"], "runtime-mismatch");
    assert_eq!(launch[0]["detail"]["requeued"], true);
    let attempt = launch[0]["subject"].as_str().unwrap();
    assert_eq!(lab.body("attempts", attempt)["action"], second);
    assert_eq!(lab.row("attempts", attempt), ("interrupted".into(), true));
    assert_eq!(lab.count("attempt.started"), 1);
    assert!(lab.call(&["repairs"]).as_array().unwrap().is_empty());
    assert!(
        refused(&lab)[&profile]
            .as_str()
            .unwrap()
            .contains("declared tool bytes changed")
    );
}

#[test]
fn supervisor_killed_before_command_drains_without_operator() {
    let lab = Lab::new();
    let profile = lab.profile(json!([
        job(
            "build",
            "mkdir dist; echo built > dist/out",
            json!({"artifacts":["dist"],"reuse":"exact",
                   "audit":"writes a constant; reads no inputs"})
        ),
        job(
            "use",
            "touch \"$PATCHWEFT_STATE/used\"",
            json!({"dependencies":["build"]})
        )
    ]));
    lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "build",
        "--owner",
        "agent",
    ]);
    lab.drain();
    let build = lab.events_of("attempt.started")[0]["subject"]
        .as_str()
        .unwrap()
        .to_owned();
    // A FIFO in place of the artifact holds the next supervisor after registration, before its
    // command: it blocks reading the dependency.
    let artifact = lab
        .state
        .join("attempts")
        .join(&build)
        .join("artifacts/dist/out");
    fs::remove_file(&artifact).unwrap();
    let fifo = std::ffi::CString::new(artifact.to_str().unwrap()).unwrap();
    unsafe {
        assert_eq!(libc::mkfifo(fifo.as_ptr(), 0o644), 0);
    }
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_until("registration", || lab.count("attempt.started") == 2);
    let started = lab.events_of("attempt.started")[1].clone();
    let attempt = started["subject"].as_str().unwrap();
    let control = lab.state.join("attempts").join(attempt).join("control");
    thread::sleep(Duration::from_millis(300));
    assert!(!control.join("command-started").exists());
    // The held supervisor keeps the FIFO it opened; the next claim reads the real artifact.
    fs::remove_file(&artifact).unwrap();
    fs::write(&artifact, "built\n").unwrap();
    unsafe {
        assert_eq!(
            libc::kill(
                started["detail"]["pid"].as_i64().unwrap() as i32,
                libc::SIGKILL
            ),
            0
        );
    }
    parsed(coordinator.wait_with_output().unwrap());
    // Its command never started, so the coordinator reports it drained: no operator needed,
    // and the action requeues instead of reddening the candidate (gate P3-2).
    assert_eq!(lab.row("attempts", attempt), ("interrupted".into(), true));
    let lost = lab.events_of("attempt.interrupted");
    assert_eq!(lost.len(), 1);
    assert_eq!(
        lost[0]["detail"],
        json!({"drained":true,"operator_confirmed":false,"reason":"supervisor-lost",
               "executor":"local","requeued":true})
    );
    assert_eq!(lab.state(&candidate), "passed");
    assert!(lab.state.join("used").exists());
}

const INPUTS_FILE: &str = "patchweft-inputs.json";
const DECLARED_AUDIT: &str = "patchweft-inputs.json names every file the job reads";
/// `check` reads provider.mjs (and the common consumer.mjs); `gate` also reads README, which
/// it does not declare, so an outside change can flip it.
const DECLARATIONS: &str = r#"{"schema":"patchweft.inputs/1","common":["consumer.mjs"],
 "jobs":{"prep":{"paths":["*.mjs"],"why":"prep builds dist from the modules"},
         "check":{"paths":["provider.mjs"],"why":"check reads provider.mjs and consumer.mjs"},
         "gate":{"paths":["*.mjs"],"why":"gate's declared reads are the modules"},
         "census":{"whole_tree":"lists the whole tree"}}}"#;
const RECORD: &str = "mkdir -p out; env > out/env";

fn declared_profile(lab: &Lab) -> String {
    let declared = |name: &str, script: &str, extra: Value| {
        let mut job = job(name, script, extra);
        job["reuse"] = json!("declared");
        job["audit"] = json!(DECLARED_AUDIT);
        job
    };
    lab.register(
        "declared",
        json!({"version":1,"name":"declared","certifying":true,"tools":{"sh":"/usr/bin/sh"},
        "inputs_file":INPUTS_FILE,"jobs":[
            declared("prep", BUILD, json!({"artifacts":["dist"]})),
            declared("check", &format!("test -s dist/out && {RECORD}"),
                     json!({"dependencies":["prep"],"artifacts":["out"]})),
            declared("gate", "! grep -q fail README", json!({})),
            declared("census", RECORD, json!({"artifacts":["out"]})),
            job("plain", RECORD, json!({"artifacts":["out"]})),
        ]}),
    )
}

fn delivered_env(lab: &Lab, action: &Value, name: &str) -> String {
    let destination = lab.state.join(format!("env-{name}"));
    lab.call(&[
        "deliver",
        action.as_str().unwrap(),
        "--destination",
        destination.to_str().unwrap(),
    ]);
    fs::read_to_string(destination.join("out/env")).unwrap()
}

#[test]
fn a_declared_pass_is_reused_until_a_declared_input_changes() {
    let lab = Lab::new();
    let profile = declared_profile(&lab);
    let (declared, _) = lab.edit(&lab.base, "author", INPUTS_FILE, DECLARATIONS);
    let (first, _) = lab.edit(&declared, "author", "README", "fail\n");
    let a = lab.submit(&first, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&a), "failed");
    let status = lab.call(&["status", a["id"].as_str().unwrap()]);
    assert_eq!(status["jobs"]["check"]["state"], "passed");
    assert_eq!(status["jobs"]["gate"]["state"], "failed");

    // Only README changes: outside `check`'s inputs, and outside `gate`'s declared ones.
    let (second, _) = lab.edit(&first, "author", "README", "pass\n");
    let b = lab.submit(&second, &profile, "agent");
    assert_eq!(b["required"]["check"], a["required"]["check"]);
    // prep is declared too, so check's passed key (which folds in prep's) holds.
    assert_eq!(names(&b["reused"]), ["check", "prep"]);
    assert_eq!(b["reused"]["check"]["from_candidate"], a["id"]);
    assert_eq!(b["reused"]["check"]["from_revision"], json!(first));
    let checks: i64 = lab
        .database()
        .query_row(
            "SELECT COUNT(*) FROM actions WHERE json_extract(body,'$.job')='check'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(checks, 1, "no new action for a reused job");
    let reused = lab.events_of("action.reused");
    assert_eq!(reused.len(), 2);
    let reused: Vec<_> = reused
        .iter()
        .filter(|e| e["subject"] == a["required"]["check"])
        .collect();
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0]["detail"]["candidate"], b["id"]);
    // A failed declared action is never reused, even with the same declared inputs.
    assert_ne!(b["required"]["gate"], a["required"]["gate"]);
    for job in ["census", "plain"] {
        assert_ne!(b["required"][job], a["required"][job], "{job}");
    }
    let status = lab.call(&["status", b["id"].as_str().unwrap()]);
    assert_eq!(status["reused"], json!(["check", "prep"]));
    assert_eq!(status["jobs"]["check"]["state"], "passed");
    assert_eq!(status["jobs"]["check"]["reused"], b["reused"]["check"]);
    lab.drain();
    assert_eq!(lab.state(&b), "passed");
    // b reuses prep and check, so it starts gate, census and plain only.
    assert_eq!(lab.count("attempt.started"), 5 + 3);

    // The reused pass consumed A's `prep`; promote accepts it by its passed key.
    lab.call(&["review", &second, "--reviewer", "reviewer"]);
    let certificate = lab.call(&["promote", b["id"].as_str().unwrap()]);
    assert_eq!(certificate["receipts"]["check"]["route"], "reused");
    assert_eq!(
        certificate["receipts"]["check"]["reused"],
        b["reused"]["check"]
    );
    assert_eq!(certificate["receipts"]["gate"]["route"], "own");

    let (third, _) = lab.edit(
        &second,
        "author",
        "provider.mjs",
        "export const offset = 1;\n",
    );
    let c = lab.submit(&third, &profile, "agent");
    assert!(c.get("reused").is_none(), "{c}");
    assert_ne!(c["required"]["check"], a["required"]["check"]);
    lab.drain();
    assert_eq!(lab.state(&c), "passed");

    // PATCHWEFT_INPUTS reaches declared jobs off the fail-safe path only.
    let check = &a["required"]["check"];
    let inputs = lab.body("actions", check.as_str().unwrap())["declared_inputs"].clone();
    assert_eq!(inputs.as_str().unwrap().len(), 64);
    let environment = delivered_env(&lab, check, "check");
    assert!(
        environment.contains(&format!("PATCHWEFT_INPUTS={}\n", inputs.as_str().unwrap())),
        "{environment}"
    );
    assert_ne!(
        lab.body("actions", c["required"]["check"].as_str().unwrap())["declared_inputs"],
        inputs
    );
    for job in ["census", "plain"] {
        let environment = delivered_env(&lab, &a["required"][job], job);
        assert!(environment.contains("PATCHWEFT_REVISION="), "{job}");
        assert!(!environment.contains("PATCHWEFT_INPUTS"), "{job}");
    }
}

#[test]
fn absent_or_invalid_declarations_behave_like_inflight() {
    let lab = Lab::new();
    let profile = declared_profile(&lab);
    let project = lab.call(&["list", "projects"])[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let invalid = DECLARATIONS.replace("\"why\"", "\"reason\"");
    let mut base = lab.base.clone();
    for (round, text) in [None, Some(invalid.as_str())].into_iter().enumerate() {
        if let Some(text) = text {
            base = lab.edit(&base, "author", INPUTS_FILE, text).0;
        }
        let first = lab.submit(&base, &profile, "agent");
        let event = lab.events_of("inputs.invalid").pop().unwrap();
        assert_eq!(event["detail"]["candidate"], first["id"]);
        let commit = lab.body("revisions", &base)["commit"].clone();
        for job in ["check", "gate"] {
            let action = lab.body("actions", first["required"][job].as_str().unwrap());
            let today = patchweft::engine::action_key(
                &project,
                action["identity"]["recipe"].as_str().unwrap(),
                "certifying",
                1800,
                &json!({"commit":commit}),
                &serde_json::from_value::<Vec<String>>(action["dependencies"].clone()).unwrap(),
            )
            .unwrap();
            assert_eq!(action["key"], today, "{job}");
            assert!(action.get("declared_inputs").is_none(), "{job}");
        }
        lab.drain();
        assert_eq!(
            lab.call(&["status", first["id"].as_str().unwrap()])["jobs"]["check"]["state"],
            "passed"
        );
        let (next, _) = lab.edit(&base, "author", "README", &format!("round {round}\n"));
        let second = lab.submit(&next, &profile, "agent");
        assert!(second.get("reused").is_none(), "{second}");
        assert_ne!(second["required"]["check"], first["required"]["check"]);
        let environment = {
            lab.drain();
            delivered_env(
                &lab,
                &second["required"]["check"],
                &format!("round-{round}"),
            )
        };
        assert!(!environment.contains("PATCHWEFT_INPUTS"));
    }
    assert_eq!(lab.count("action.reused"), 0);
}
