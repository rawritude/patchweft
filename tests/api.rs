use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use patchweft::{
    api::{self, Api, Command, Principal, Query},
    engine, hash, http, mcp, source,
    store::{self, Store},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command as Process,
    sync::Arc,
};
use tower::ServiceExt;

// Public synthetic test strings; no deployment credential is loaded by these tests.
const ALICE: &str = "fixture-alice-credential-not-for-deployment";
const BOB: &str = "fixture-bob-credential-not-for-deployment";
struct Lab {
    _temp: tempfile::TempDir,
    root: PathBuf,
    repo: PathBuf,
    a: String,
    b: String,
    pa: String,
    pb: String,
    profile: String,
    api: Arc<Api>,
}
fn git(path: &Path, args: &[&str]) -> String {
    let output = Process::new("git")
        .arg("-C")
        .arg(path)
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
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("README"), "initial\n").unwrap();
        git(&repo, &["add", "README"]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let root = temp.path().join("state");
        let mut store = Store::init(&root, 2, 2048).unwrap();
        let a = source::import(&mut store, "a", &repo, "HEAD", "fixture").unwrap();
        let b = source::import(&mut store, "b", &repo, "HEAD", "fixture").unwrap();
        let profile_path = temp.path().join("profile.json");
        fs::write(&profile_path,json!({"version":1,"name":"fixture","certifying":true,
            "tools":{"sh":"/usr/bin/sh"},"jobs":[
            {"name":"dep","command":["sh","-c","true"],"reuse":"never","report":{"kind":"command"}},
            {"name":"root","dependencies":["dep"],"command":["sh","-c","true"],"reuse":"never","report":{"kind":"command"}}
        ]}).to_string()).unwrap();
        let profile = engine::register(&mut store, &profile_path).unwrap().id;
        let api = Arc::new(Api::open(&root).unwrap());
        for (id, token, p) in [("alice", ALICE, &a.project), ("bob", BOB, &b.project)] {
            let principal = Principal {
                id: id.into(),
                projects: BTreeSet::from([p.clone()]),
                profiles: BTreeSet::from([profile.clone()]),
                roles: BTreeSet::from(["read".into(), "write".into()]),
                max_pending: 1024,
            };
            store
                .db
                .execute(
                    "INSERT INTO api_principals VALUES (?,?)",
                    rusqlite::params![id, serde_json::to_string(&principal).unwrap()],
                )
                .unwrap();
            store
                .db
                .execute(
                    "INSERT INTO api_credentials VALUES (?,?)",
                    rusqlite::params![hash(token.as_bytes()), id],
                )
                .unwrap();
        }
        Self {
            _temp: temp,
            root,
            repo,
            a: a.id,
            b: b.id,
            pa: a.project,
            pb: b.project,
            profile,
            api,
        }
    }
    fn command(&self) -> Command {
        Command::Request {
            revision: self.a.clone(),
            profile: self.profile.clone(),
            job: "root".into(),
        }
    }
    fn rows(&self, table: &str) -> i64 {
        Store::open(&self.root)
            .unwrap()
            .db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    fn router(&self) -> axum::Router {
        http::router(http::Service::new(self.api.clone(), BTreeSet::new()))
    }
}

#[test]
fn receipt_and_dependency_effects_are_atomic_and_replay_after_restart() {
    let lab = Lab::new();
    let before = lab.rows("events");
    let store = Store::open(&lab.root).unwrap();
    store
        .db
        .execute_batch(
            "CREATE TRIGGER fail_receipt BEFORE INSERT ON api_receipts BEGIN
        SELECT RAISE(ABORT,'fixture interruption before receipt'); END;",
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "one", &lab.command())
            .unwrap_err()
            .status,
        500
    );
    assert_eq!(lab.rows("actions"), 0);
    assert_eq!(lab.rows("interests"), 0);
    assert_eq!(lab.rows("api_receipts"), 0);
    assert_eq!(lab.rows("events"), before);
    store.db.execute_batch("DROP TRIGGER fail_receipt").unwrap();
    let first = lab.api.command(ALICE, "one", &lab.command()).unwrap();
    assert_eq!(first["status"], 200);
    assert_eq!(lab.rows("actions"), 2);
    let restarted = Api::open(&lab.root).unwrap();
    let replay = restarted.command(ALICE, "one", &lab.command()).unwrap();
    assert_eq!(replay["operation_id"], first["operation_id"]);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(replay["replayed"], true);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(
        restarted
            .command(
                ALICE,
                "one",
                &Command::Cancel {
                    action: first["result"]["id"].as_str().unwrap().into()
                }
            )
            .unwrap_err()
            .status,
        409
    );
}

#[test]
fn concurrent_retry_of_one_key_creates_one_dependency_graph() {
    let lab = Lab::new();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let mut threads = vec![];
    for _ in 0..16 {
        let api = lab.api.clone();
        let command = lab.command();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            api.command(ALICE, "concurrent", &command).unwrap()
        }));
    }
    let values: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(values.iter().all(|v| v["result"] == values[0]["result"]));
    assert_eq!(values.iter().filter(|v| v["replayed"] == false).count(), 1);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(lab.rows("api_receipts"), 1);
}

#[test]
fn local_and_api_candidate_submission_share_one_transaction_order() {
    let lab = Lab::new();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let root = lab.root.clone();
    let revision = lab.a.clone();
    let profile = lab.profile.clone();
    let ready = barrier.clone();
    let local = std::thread::spawn(move || {
        ready.wait();
        engine::submit(
            &mut Store::open(&root).unwrap(),
            &revision,
            &profile,
            "local",
            vec![],
        )
        .unwrap()
    });
    barrier.wait();
    let remote = lab
        .api
        .command(
            ALICE,
            "submit",
            &Command::Submit {
                revision: lab.a.clone(),
                profile: lab.profile.clone(),
                requires: vec![],
                retain_from: None,
            },
        )
        .unwrap();
    assert_eq!(remote["result"]["id"], local.join().unwrap().id);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(lab.rows("candidates"), 1);
}

#[test]
fn principal_scope_covers_replays_dependencies_roles_and_cancellation() {
    let lab = Lab::new();
    let first = lab.api.command(ALICE, "first", &lab.command()).unwrap();
    let action = first["result"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        lab.api
            .query(
                BOB,
                &Query::Action {
                    action: action.clone()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .command(BOB, "cancel", &Command::Cancel { action })
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .query(
                BOB,
                &Query::Operation {
                    id: first["operation_id"].as_str().unwrap().into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "review",
                &Command::Review {
                    revision: lab.a.clone()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let prerequisite = engine::submit(
        &mut Store::open(&lab.root).unwrap(),
        &lab.b,
        &lab.profile,
        "b",
        vec![],
    )
    .unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "cross",
                &Command::Submit {
                    revision: lab.a.clone(),
                    profile: lab.profile.clone(),
                    requires: vec![prerequisite.id],
                    retain_from: None,
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let store = Store::open(&lab.root).unwrap();
    let mut principal: Principal = serde_json::from_str(
        &store
            .db
            .query_row(
                "SELECT body FROM api_principals WHERE id='alice'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    principal.roles.remove("write");
    store
        .db
        .execute(
            "UPDATE api_principals SET body=? WHERE id='alice'",
            [serde_json::to_string(&principal).unwrap()],
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "first", &lab.command())
            .unwrap_err()
            .status,
        403,
        "replay must recheck current scope"
    );
    api::revoke(&store, "alice").unwrap();
    assert_eq!(
        lab.api.query(ALICE, &Query::Profiles).unwrap_err().status,
        401
    );
}

#[test]
fn policy_update_changes_scope_without_rotating_the_credential() {
    let lab = Lab::new();
    let review = Command::Review {
        revision: lab.a.clone(),
    };
    assert_eq!(
        lab.api
            .command(ALICE, "review-1", &review)
            .unwrap_err()
            .status,
        403
    );
    let store = Store::open(&lab.root).unwrap();
    let widened = Principal {
        id: "alice".into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::from([lab.profile.clone()]),
        roles: BTreeSet::from(["read".into(), "review".into()]),
        max_pending: 1024,
    };
    assert_eq!(
        api::update_policy(&store, serde_json::to_value(&widened).unwrap()).unwrap()["rotated"],
        false
    );
    // The same credential now carries the new scope: review is allowed, write is gone.
    lab.api.command(ALICE, "review-2", &review).unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "request", &lab.command())
            .unwrap_err()
            .status,
        403
    );
    let unknown = Principal {
        id: "carol".into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::new(),
        roles: BTreeSet::from(["read".into()]),
        max_pending: 1,
    };
    assert!(api::update_policy(&store, serde_json::to_value(&unknown).unwrap()).is_err());
    assert_eq!(lab.rows("api_credentials"), 2);
    // Every policy change is audited with the scope before and after it (gate P2-1).
    let audits: Vec<Value> = store
        .events(0)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "principal.policy")
        .collect();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0]["subject"], "alice");
    assert_eq!(
        audits[0]["detail"]["after"]["roles"],
        json!(["read", "review"])
    );
    assert!(audits[0]["detail"]["before"]["roles"].is_array());
    assert_ne!(audits[0]["detail"]["before"], audits[0]["detail"]["after"]);
}

#[test]
fn policy_update_refuses_an_invalid_policy_and_changes_nothing() {
    // Gate P2-2: the validate_policy call is what stands between a typo and a scope.
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    let body = |store: &Store| -> String {
        store
            .db
            .query_row(
                "SELECT body FROM api_principals WHERE id='alice'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    let before = body(&store);
    for invalid in [
        json!({"id":"alice","projects":[lab.pa],"profiles":["no-such-profile"],"roles":["read"]}),
        json!({"id":"alice","projects":[lab.pa],"profiles":[],"roles":["admin-everything"]}),
        json!({"id":"alice","projects":[lab.pa],"profiles":[],"roles":["read"],"max_pending":0}),
    ] {
        assert!(
            api::update_policy(&store, invalid.clone()).is_err(),
            "{invalid}"
        );
        assert_eq!(body(&store), before, "{invalid}");
    }
    assert!(
        !store
            .events(0)
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "principal.policy")
    );
}

#[test]
fn policy_update_keeps_max_pending_when_omitted_and_grant_and_revoke_are_audited() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    let limit = |store: &Store| -> Value {
        let body: String = store
            .db
            .query_row(
                "SELECT body FROM api_principals WHERE id='alice'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        serde_json::from_str::<Value>(&body).unwrap()["max_pending"].clone()
    };
    // A non-default limit first, so a reset to the default would show (gate N1).
    api::update_policy(
        &store,
        json!({"id":"alice","projects":[lab.pa],"profiles":[lab.profile],"roles":["read"],
               "max_pending":77}),
    )
    .unwrap();
    assert_eq!(limit(&store), 77);
    api::update_policy(
        &store,
        json!({"id":"alice","projects":[lab.pa],"profiles":[lab.profile],"roles":["read"]}),
    )
    .unwrap();
    // A policy without max_pending keeps the limit instead of resetting it.
    assert_eq!(limit(&store), 77);
    let dave = Principal {
        id: "dave".into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::new(),
        roles: BTreeSet::from(["read".into()]),
        max_pending: 8,
    };
    api::grant(&store, dave, &lab.root.join("dave.credential")).unwrap();
    let granted: Vec<Value> = store
        .events(0)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "principal.granted")
        .collect();
    assert_eq!(granted.len(), 1);
    assert_eq!(
        (
            &granted[0]["subject"],
            &granted[0]["detail"]["after"]["max_pending"]
        ),
        (&json!("dave"), &json!(8))
    );
    assert!(granted[0]["detail"]["before"].is_null());
    // A re-grant records the policy it replaces (gate N3).
    let wider = Principal {
        id: "dave".into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::new(),
        roles: BTreeSet::from(["read".into()]),
        max_pending: 9,
    };
    std::fs::remove_file(lab.root.join("dave.credential")).unwrap();
    api::grant(&store, wider, &lab.root.join("dave.credential")).unwrap();
    let regranted: Vec<Value> = store
        .events(0)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "principal.granted")
        .collect();
    assert_eq!(regranted[1]["detail"]["before"]["max_pending"], 8);
    assert_eq!(regranted[1]["detail"]["after"]["max_pending"], 9);
    api::revoke(&store, "alice").unwrap();
    let revoked: Vec<Value> = store
        .events(0)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "principal.revoked")
        .collect();
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0]["detail"]["credentials_revoked"], 1);
}

#[test]
fn durable_domain_rejection_replays_without_creating_partial_work() {
    let lab = Lab::new();
    let command = Command::Request {
        revision: lab.a.clone(),
        profile: lab.profile.clone(),
        job: "absent".into(),
    };
    let first = lab.api.command(ALICE, "bad-job", &command).unwrap();
    assert_eq!(first["status"], 409);
    assert_eq!(lab.rows("actions"), 0);
    let second = lab.api.command(ALICE, "bad-job", &command).unwrap();
    assert_eq!(second["operation_id"], first["operation_id"]);
    assert_eq!(second["replayed"], true);
}

#[test]
fn paginated_events_cross_hidden_projects_without_losing_cursor_progress() {
    let lab = Lab::new();
    let mut store = Store::open(&lab.root).unwrap();
    let transaction = store.transaction().unwrap();
    for n in 0..1100 {
        store::event(
            &transaction,
            "revision.reviewed",
            if n % 2 == 0 { &lab.a } else { &lab.b },
            &json!({"reviewer":format!("fixture-{n}")}),
        )
        .unwrap();
    }
    transaction.commit().unwrap();
    let mut cursor = 0;
    let mut selected = vec![];
    loop {
        let page = lab
            .api
            .query(
                ALICE,
                &Query::Events {
                    after: cursor,
                    project: None,
                    limit: 1000,
                },
            )
            .unwrap();
        for row in page["events"].as_array().unwrap() {
            assert_ne!(row["subject"], lab.b);
            selected.push(row["seq"].as_i64().unwrap());
        }
        cursor = page["next_after"].as_i64().unwrap();
        if page["more"] == false {
            break;
        }
    }
    assert_eq!(
        selected.len(),
        551,
        "initial revision plus 550 allowed fixture events"
    );
    assert_eq!(
        selected.iter().collect::<BTreeSet<_>>().len(),
        selected.len()
    );
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Events {
                    after: cursor + 1,
                    project: None,
                    limit: 10
                }
            )
            .unwrap_err()
            .code,
        "future_cursor"
    );
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Events {
                    after: 0,
                    project: Some(lab.pb.clone()),
                    limit: 10
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[test]
fn bundle_upload_hash_and_project_boundaries_precede_publication() {
    let lab = Lab::new();
    fs::write(lab.repo.join("README"), "new revision\n").unwrap();
    git(&lab.repo, &["add", "README"]);
    git(&lab.repo, &["commit", "--quiet", "-m", "second"]);
    let commit = git(&lab.repo, &["rev-parse", "HEAD"]);
    let bundle = lab._temp.path().join("source.bundle");
    git(
        &lab.repo,
        &["bundle", "create", bundle.to_str().unwrap(), "HEAD"],
    );
    let bytes = fs::read(&bundle).unwrap();
    let sha = hash(&bytes);
    assert_eq!(
        lab.api
            .upload(ALICE, &"0".repeat(64), &bytes)
            .unwrap_err()
            .status,
        422
    );
    lab.api.upload(ALICE, &sha, &bytes).unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "wrong-project",
                &Command::Publish {
                    project: lab.pb.clone(),
                    sha256: sha.clone(),
                    commit: commit.clone(),
                    message: "publish".into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let publish = Command::Publish {
        project: lab.pa.clone(),
        sha256: sha,
        commit: commit.clone(),
        message: "publish".into(),
    };
    let response = lab.api.command(ALICE, "publish", &publish).unwrap();
    assert_eq!(response["status"], 200, "{response}");
    assert_eq!(response["result"]["commit"], commit);
    assert_eq!(
        lab.api.command(ALICE, "publish", &publish).unwrap()["result"],
        response["result"]
    );
}

async fn send(
    router: axum::Router,
    uri: &str,
    body: Value,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::post(uri).header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    for (key, value) in extra {
        request = request.header(*key, *value);
    }
    let response = router
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

#[tokio::test]
async fn unavailable_artifacts_are_a_scoped_state_error_and_failed_logs_remain_readable() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    let profile_path = lab.root.join("artifact-profile.json");
    fs::write(
        &profile_path,
        json!({"version":1,"name":"artifact-states","tools":{"sh":"/usr/bin/sh"},"jobs":[
            {"name":"fail","command":["sh","-c","printf 'failed fixture\\n'; printf 'not retained\\n' > report.txt; exit 1"],
             "artifacts":["report.txt"],"report":{"kind":"command"}},
            {"name":"pass","command":["sh","-c","printf 'artifact\\n' > report.txt"],
             "artifacts":["report.txt"],"report":{"kind":"command"}}
        ]}).to_string(),
    ).unwrap();
    let registered = Process::new(env!("CARGO_BIN_EXE_patchweft"))
        .arg("--state")
        .arg(&lab.root)
        .arg("register")
        .arg(&profile_path)
        .output()
        .unwrap();
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    let profile: Value = serde_json::from_slice(&registered.stdout).unwrap();
    let profile_id = profile["id"].as_str().unwrap();
    let request = |job: &str| {
        let output = Process::new(env!("CARGO_BIN_EXE_patchweft"))
            .arg("--state")
            .arg(&lab.root)
            .args([
                "request",
                "--revision",
                &lab.a,
                "--profile",
                profile_id,
                "--job",
                job,
                "--owner",
                "fixture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<patchweft::model::Action>(&output.stdout).unwrap()
    };
    let failed = request("fail");
    let passed = request("pass");
    let digest = hash(b"artifact\n");
    for after_execution in [false, true] {
        if after_execution {
            let output = Process::new(env!("CARGO_BIN_EXE_patchweft"))
                .arg("--state")
                .arg(&lab.root)
                .args(["worker", "--drain", "--no-loopback-check"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                engine::action_state(&store.db, &failed.id).unwrap(),
                "failed"
            );
        }
        let query = json!({"op":"artifacts","action":failed.id});
        let (status, body) = send(lab.router(), "/v1/query", query.clone(), Some(ALICE), &[]).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "artifacts_unavailable");
        assert_eq!(
            send(lab.router(), "/v1/query", query, Some(BOB), &[])
                .await
                .0,
            StatusCode::FORBIDDEN
        );
        let response = lab
            .router()
            .oneshot(
                Request::get(format!("/v1/artifacts/{}/{digest}", failed.id))
                    .header("authorization", format!("Bearer {ALICE}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(error["error"]["code"], "artifacts_unavailable");
        assert_eq!(
            lab.api
                .artifact(BOB, &failed.id, &digest)
                .unwrap_err()
                .status,
            403
        );
    }
    let action = lab
        .api
        .query(
            ALICE,
            &Query::Action {
                action: failed.id.clone(),
            },
        )
        .unwrap();
    let log = lab
        .api
        .query(
            ALICE,
            &Query::Logs {
                attempt: action["attempt"].as_str().unwrap().into(),
                offset: 0,
                limit: 1024,
            },
        )
        .unwrap();
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(log["data"].as_str().unwrap())
        .unwrap();
    assert!(String::from_utf8(bytes).unwrap().contains("failed fixture"));
    let manifest = lab
        .api
        .query(
            ALICE,
            &Query::Artifacts {
                action: passed.id.clone(),
            },
        )
        .unwrap();
    assert_eq!(manifest["files"]["report.txt"]["sha256"], digest);
    use std::io::Read;
    let mut contents = String::new();
    lab.api
        .artifact(ALICE, &passed.id, &digest)
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();
    assert_eq!(contents, "artifact\n");
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Artifacts {
                    action: "unknown-action".into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[tokio::test]
async fn http_rejects_spoofed_identity_origin_and_missing_credentials() {
    let lab = Lab::new();
    let command = serde_json::to_value(lab.command()).unwrap();
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command.clone(),
            None,
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        401
    );
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command.clone(),
            Some(ALICE),
            &[
                ("idempotency-key", "x"),
                ("origin", "https://unapproved.invalid")
            ]
        )
        .await
        .0,
        403
    );
    let mut spoof = command.clone();
    spoof["owner"] = json!("bob");
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            spoof,
            Some(ALICE),
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        422
    );
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command,
            Some(ALICE),
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        200
    );
    assert_eq!(lab.rows("actions"), 2);
}

fn modern(id: i64, method: &str, params: Value) -> Value {
    let mut params = params;
    params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":mcp::MODERN,
        "io.modelcontextprotocol/clientCapabilities":{}});
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}
#[tokio::test]
async fn mcp_eras_share_effects_but_keep_request_envelopes_and_headers_distinct() {
    let lab = Lab::new();
    let headers = [
        ("accept", "application/json, text/event-stream"),
        ("mcp-protocol-version", mcp::MODERN),
        ("mcp-method", "tools/call"),
        ("mcp-name", "patchweft_command"),
    ];
    let params = json!({"name":"patchweft_command","arguments":{"idempotency_key":"mcp-same","command":lab.command()}});
    let (status, first) = send(
        lab.router(),
        "/mcp",
        modern(7, "tools/call", params.clone()),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(first["result"]["resultType"], "complete");
    let (_, second) = send(
        lab.router(),
        "/mcp",
        modern(8, "tools/call", params.clone()),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(second["id"], 8);
    assert_eq!(second["result"]["structuredContent"]["replayed"], true);
    assert_eq!(lab.rows("actions"), 2);
    let (_, legacy) = send(
        lab.router(),
        "/mcp",
        json!({"jsonrpc":"2.0","id":9,"method":"tools/call","params":params}),
        Some(ALICE),
        &[
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", mcp::LEGACY),
        ],
    )
    .await;
    assert_eq!(legacy["id"], 9);
    assert!(legacy["result"].get("resultType").is_none());
    assert_eq!(
        legacy["result"]["structuredContent"]["operation_id"],
        first["result"]["structuredContent"]["operation_id"]
    );
    let (status, bad) = send(
        lab.router(),
        "/mcp",
        modern(10, "tools/list", json!({})),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(bad["error"]["code"], -32020);
    let (status,notification)=send(lab.router(),"/mcp",json!({"jsonrpc":"2.0","method":"tools/call",
        "params":{"name":"patchweft_command","arguments":{"idempotency_key":"never-execute","command":lab.command()}}}),
        Some(ALICE),&headers).await;
    assert_eq!(status, 202);
    assert!(notification.is_null());
    assert_eq!(lab.rows("actions"), 2);
}

#[tokio::test]
async fn sse_disconnect_releases_stream_capacity_and_reconnect_replays() {
    let lab = Lab::new();
    let router = lab.router();
    let request = || {
        Request::get("/v1/events")
            .header("authorization", format!("Bearer {ALICE}"))
            .header("accept", "text/event-stream")
            .body(Body::empty())
            .unwrap()
    };
    let mut streams = vec![];
    for _ in 0..32 {
        let response = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), 200);
        streams.push(response);
    }
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        503
    );
    streams.clear();
    let mut response = router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), 200);
    let frame = response
        .body_mut()
        .frame()
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&frame).contains("event: patchweft"));
    let first_seq = lab
        .api
        .query(
            ALICE,
            &Query::Events {
                after: 0,
                project: None,
                limit: 1000,
            },
        )
        .unwrap()["events"][0]["seq"]
        .as_i64()
        .unwrap();
    drop(response);
    let response = router
        .oneshot(
            Request::get("/v1/events")
                .header("authorization", format!("Bearer {ALICE}"))
                .header("last-event-id", first_seq.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        value["result"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["seq"].as_i64().unwrap() > first_seq)
    );
    assert_eq!(lab.rows("actions"), 0);
}

#[test]
fn shared_candidate_retry_uses_association_without_reviving_other_owners() {
    let lab = Lab::new();
    let mut store = Store::open(&lab.root).unwrap();
    let candidate =
        engine::submit(&mut store, &lab.a, &lab.profile, "local-first", vec![]).unwrap();
    let response = lab
        .api
        .command(
            ALICE,
            "attach",
            &Command::Submit {
                revision: lab.a.clone(),
                profile: lab.profile.clone(),
                requires: vec![],
                retain_from: None,
            },
        )
        .unwrap();
    assert_eq!(response["result"]["id"], candidate.id);
    for action in candidate.required.values() {
        engine::cancel(&mut store, action, "local-first").unwrap();
        engine::cancel(&mut store, action, "api:alice").unwrap();
    }
    let response = lab
        .api
        .command(
            ALICE,
            "retry",
            &Command::Retry {
                candidate: candidate.id.clone(),
                jobs: vec![],
            },
        )
        .unwrap();
    assert_eq!(response["status"], 200);
    assert_eq!(response["result"]["retried"].as_array().unwrap().len(), 2);
    let active: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM interests WHERE owner='local-first' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 0);
    let active: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM interests WHERE owner='api:alice' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 2);
}

#[test]
fn admission_counts_actual_leaf_work_and_rolls_back_excess_dependencies() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    store
        .db
        .execute(
            "UPDATE api_principals SET body=json_set(body,'$.max_pending',1) WHERE id='alice'",
            [],
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "too-many", &lab.command())
            .unwrap_err()
            .status,
        429
    );
    assert_eq!(lab.rows("actions"), 0);
    assert_eq!(lab.rows("api_receipts"), 0);
    let leaf = Command::Request {
        revision: lab.a.clone(),
        profile: lab.profile.clone(),
        job: "dep".into(),
    };
    assert_eq!(
        lab.api.command(ALICE, "leaf", &leaf).unwrap()["status"],
        200
    );
    assert_eq!(lab.rows("actions"), 1);
}

#[test]
fn composition_conflicts_are_durable_inspectable_and_scoped() {
    let lab = Lab::new();
    let base = git(&lab.repo, &["rev-parse", "HEAD"]);
    let mut store = Store::open(&lab.root).unwrap();
    let mut changes = vec![];
    for text in ["first change\n", "second change\n"] {
        git(&lab.repo, &["checkout", "--quiet", "--detach", &base]);
        fs::write(lab.repo.join("README"), text).unwrap();
        git(&lab.repo, &["add", "README"]);
        git(
            &lab.repo,
            &["commit", "--quiet", "-m", "conflicting fixture"],
        );
        changes.push(
            source::import(&mut store, "a", &lab.repo, "HEAD", "fixture")
                .unwrap()
                .id,
        );
    }
    let command = Command::Compose {
        base: lab.a.clone(),
        changes,
        message: "conflict fixture".into(),
    };
    let response = lab.api.command(ALICE, "conflict", &command).unwrap();
    assert_eq!(response["status"], 200, "{response}");
    assert_eq!(response["result"]["state"], "conflict");
    let repair = response["result"]["repair"].as_str().unwrap().to_owned();
    assert_eq!(
        lab.api.command(ALICE, "conflict", &command).unwrap()["result"],
        response["result"]
    );
    assert_eq!(lab.rows("repairs"), 1);
    let query = Query::Repair {
        repair: repair.clone(),
    };
    let detail = lab.api.query(ALICE, &query).unwrap();
    assert!(
        detail["detail"]["conflicts"]
            .as_str()
            .unwrap()
            .contains("README")
    );
    assert_eq!(lab.api.query(BOB, &query).unwrap_err().status, 403);
    let page = lab
        .api
        .query(
            ALICE,
            &Query::Repairs {
                project: lab.pa.clone(),
                after: 0,
                limit: 10,
            },
        )
        .unwrap();
    assert_eq!(page["items"][0]["id"], repair);
    let events = lab
        .api
        .query(
            ALICE,
            &Query::Events {
                after: 0,
                project: None,
                limit: 1000,
            },
        )
        .unwrap();
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "composition.conflict" && e["subject"] == repair)
    );
    let cross = engine::submit(&mut store, &lab.b, &lab.profile, "b", vec![]).unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "bad-resolve",
                &Command::Resolve {
                    repair,
                    candidate: cross.id
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[tokio::test]
async fn extractor_errors_and_mcp_schema_keep_the_wire_contract() {
    let lab = Lab::new();
    let request = Request::get("/v1/events?after=oops")
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::empty())
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 400);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["error"]["code"], "invalid_query");
    assert_eq!(value["api_version"], "1");
    let request = Request::put(format!("/v1/blobs/{}", "0".repeat(64)))
        .header("authorization", format!("Bearer {ALICE}"))
        .header("content-length", (api::MAX_BUNDLE + 1).to_string())
        .body(Body::empty())
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 413);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["error"]["code"], "invalid_body");
    assert_eq!(value["api_version"], "1");
    let (status, value) = send(
        lab.router(),
        "/mcp",
        modern(1, "tools/list", json!({})),
        Some(ALICE),
        &[
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", mcp::MODERN),
            ("mcp-method", "tools/list"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    for tool in value["result"]["tools"].as_array().unwrap() {
        assert_eq!(tool["inputSchema"]["type"], "object");
    }
}

fn pending_uploads(lab: &Lab) -> usize {
    fs::read_dir(&lab.root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".upload-")
        })
        .count()
}

async fn await_upload_cleanup(lab: &Lab) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pending_uploads(lab) != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn streamed_upload_exceeds_old_limit_without_length_and_retains_quota_guards() {
    use sha2::{Digest, Sha256};
    let lab = Lab::new();
    let chunk = axum::body::Bytes::from(vec![42u8; 65536]);
    let count = 65 * 1024 * 1024 / chunk.len();
    let mut digest = Sha256::new();
    for _ in 0..count {
        digest.update(&chunk);
    }
    let sha = format!("{:x}", digest.finalize());
    let stream = async_stream::stream! {
        for _ in 0..count {
            yield Ok::<_, std::io::Error>(chunk.clone());
        }
    };
    let request = Request::put(format!("/v1/blobs/{sha}"))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["result"]["bytes"], 65 * 1024 * 1024);
    let retained = lab.root.join("uploads").join(hash(b"alice"));
    assert_eq!(patchweft::file_hash(&retained.join(&sha)).unwrap(), sha);
    assert_eq!(pending_uploads(&lab), 0);

    // Sparse accounting fixture: quota includes retained content, never a bundle-sized allocation.
    let used = fs::metadata(retained.join(&sha)).unwrap().len();
    fs::File::create(retained.join("quota-fixture"))
        .unwrap()
        .set_len(api::MAX_RETAINED_UPLOADS - used)
        .unwrap();
    let error = lab.api.upload(ALICE, &hash(b"new"), b"new").unwrap_err();
    assert_eq!(error.status, 429);
    assert_eq!(error.code, "upload_quota");
    assert_eq!(pending_uploads(&lab), 0);
}

#[tokio::test]
async fn failed_streams_and_revoked_uploads_leave_no_staging_files() {
    let lab = Lab::new();
    let (started, ready) = tokio::sync::oneshot::channel();
    let stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"partial"));
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    };
    let request = Request::put(format!("/v1/blobs/{}", hash(b"partial")))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let pending = tokio::spawn(lab.router().oneshot(request));
    ready.await.unwrap();
    assert_eq!(pending_uploads(&lab), 1);
    pending.abort();
    let _ = pending.await;
    await_upload_cleanup(&lab).await;
    assert_eq!(pending_uploads(&lab), 0);

    let stream = async_stream::stream! {
        yield Ok(axum::body::Bytes::from_static(b"partial"));
        yield Err(std::io::Error::other("interrupted fixture"));
    };
    let request = Request::put(format!("/v1/blobs/{}", hash(b"partial")))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 400);
    await_upload_cleanup(&lab).await;
    assert_eq!(pending_uploads(&lab), 0);
    assert_eq!(
        lab.api
            .upload(ALICE, &hash(b"other"), b"partial")
            .unwrap_err()
            .status,
        422
    );
    assert_eq!(pending_uploads(&lab), 0);

    let oversized = lab.api.begin_upload(ALICE, &hash(b"large")).unwrap();
    oversized
        .as_file()
        .set_len(api::MAX_BUNDLE as u64 + 1)
        .unwrap();
    assert_eq!(
        lab.api
            .finish_upload(ALICE, &hash(b"large"), oversized)
            .unwrap_err()
            .status,
        413
    );
    assert_eq!(pending_uploads(&lab), 0);

    let mut pending = lab.api.begin_upload(ALICE, &hash(b"valid")).unwrap();
    std::io::Write::write_all(&mut pending, b"valid").unwrap();
    api::revoke(&Store::open(&lab.root).unwrap(), "alice").unwrap();
    assert_eq!(
        lab.api
            .finish_upload(ALICE, &hash(b"valid"), pending)
            .unwrap_err()
            .status,
        401
    );
    assert_eq!(pending_uploads(&lab), 0);
}

#[test]
fn submit_without_retain_serializes_unchanged() {
    // Pre-upgrade receipts hash this exact serialization; a changed form would turn a replay
    // into an idempotency conflict.
    let submit = Command::Submit {
        revision: "r".into(),
        profile: "p".into(),
        requires: vec!["c".into()],
        retain_from: None,
    };
    assert_eq!(
        serde_json::to_string(&submit).unwrap(),
        r#"{"op":"submit","revision":"r","profile":"p","requires":["c"]}"#
    );
}

fn register(lab: &Lab, name: &str, script: &str) -> String {
    let path = lab.root.join(format!("{name}.json"));
    fs::write(
        &path,
        json!({"version":1,"name":name,"certifying":true,"tools":{"sh":"/usr/bin/sh"},
            "jobs":[{"name":"build","command":["sh","-c",script],"report":{"kind":"command"}}]})
        .to_string(),
    )
    .unwrap();
    engine::register(&mut Store::open(&lab.root).unwrap(), &path)
        .unwrap()
        .id
}

fn pass(store: &Store, action: &str) {
    let attempt = patchweft::unique();
    let body = json!({"id":attempt,"action":action,"worker_pid":0,"worker_start":"0",
        "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,"created":0.0,
        "started":0.5,"ended":1.0,"exit_code":0,"result":{"timer_seconds":0.5}});
    store
        .db
        .execute(
            "INSERT INTO attempts VALUES (?,?,'passed',1,?)",
            rusqlite::params![attempt, action, body.to_string()],
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE actions SET state='passed',attempt=? WHERE id=?",
            rusqlite::params![attempt, action],
        )
        .unwrap();
}

#[tokio::test]
async fn submit_retain_from_is_project_scoped() {
    let lab = Lab::new();
    let granted = register(&lab, "granted", "true");
    let ungranted = register(&lab, "ungranted", "exit 0");
    let mut store = Store::open(&lab.root).unwrap();
    store
        .db
        .execute(
            "UPDATE api_principals SET body=json_set(body,'$.profiles',json(?)) WHERE id='alice'",
            [json!([lab.profile, granted]).to_string()],
        )
        .unwrap();
    let submit = |source: &str| Command::Submit {
        revision: lab.a.clone(),
        profile: granted.clone(),
        requires: vec![],
        retain_from: Some(source.into()),
    };
    let elsewhere = engine::submit(&mut store, &lab.b, &granted, "bob", vec![]).unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "other-project", &submit(&elsewhere.id))
            .unwrap_err()
            .status,
        403
    );
    // Retention consumes evidence produced under the source's profile, so it needs that grant.
    let foreign = engine::submit(&mut store, &lab.a, &ungranted, "fixture", vec![]).unwrap();
    pass(&store, &foreign.required["build"]);
    assert_eq!(
        lab.api
            .command(ALICE, "other-profile", &submit(&foreign.id))
            .unwrap_err()
            .status,
        403
    );
    let source = engine::submit(&mut store, &lab.a, &granted, "fixture", vec![]).unwrap();
    pass(&store, &source.required["build"]);
    let (status, body) = send(
        lab.router(),
        "/v1/commands",
        json!({"op":"submit","revision":lab.a,"profile":granted,"retain_from":source.id}),
        Some(ALICE),
        &[("idempotency-key", "retain")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], 200, "{body}");
    assert_eq!(body["result"]["required"], json!(source.required));
    assert_eq!(body["result"]["retained"], json!(["build"]));
    assert_eq!(body["result"]["retained_from"], source.id);
    assert_ne!(body["result"]["id"], source.id);
}

// Executors reach the authority through these routes (multi-host step 6).
const MINDY: &str = "fixture-mindy-executor-credential-not-for-deployment";

/// Grants `id` the lab profile on project `a` with `roles`, under `token`.
fn executor_principal(lab: &Lab, id: &str, token: &str, roles: &[&str]) {
    let store = Store::open(&lab.root).unwrap();
    let principal = Principal {
        id: id.into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::from([lab.profile.clone()]),
        roles: roles.iter().map(|role| role.to_string()).collect(),
        max_pending: 1024,
    };
    store
        .db
        .execute(
            "INSERT INTO api_principals VALUES (?,?)",
            rusqlite::params![id, serde_json::to_string(&principal).unwrap()],
        )
        .unwrap();
    store
        .db
        .execute(
            "INSERT INTO api_credentials VALUES (?,?)",
            rusqlite::params![hash(token.as_bytes()), id],
        )
        .unwrap();
}

fn configure(lab: &Lab, id: &str, request: patchweft::authority::ConfigureRequest) -> Value {
    patchweft::authority::configure(&mut Store::open(&lab.root).unwrap(), id, &request).unwrap()
}

/// mindy: an executor of project `a` with two slots, placed to run `dep`.
fn register_mindy(lab: &Lab) {
    executor_principal(lab, "mindy", MINDY, &["read", "execute"]);
    configure(
        lab,
        "mindy",
        patchweft::authority::ConfigureRequest {
            slots: Some(2),
            memory_mb: Some(2048),
            allow: vec!["dep".into()],
            ..Default::default()
        },
    );
}

fn queue_job(lab: &Lab, revision: &str, job: &str) -> String {
    engine::request(
        &mut Store::open(&lab.root).unwrap(),
        revision,
        &lab.profile,
        job,
        "lane",
    )
    .unwrap()
    .id
}

async fn executor_post(lab: &Lab, route: &str, token: &str, body: Value) -> (StatusCode, Value) {
    send(
        lab.router(),
        &format!("/v1/executor/{route}"),
        body,
        Some(token),
        &[],
    )
    .await
}

/// Claims as mindy: (attempt, lease, action).
async fn claim_as_mindy(lab: &Lab, key: &str) -> (String, String, String) {
    let (status, body) = executor_post(
        lab,
        "claim",
        MINDY,
        json!({"claim_key":key,"verified":[lab.profile]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let claim = &body["result"]["claim"];
    let text = |value: &Value| {
        value
            .as_str()
            .unwrap_or_else(|| panic!("{body}"))
            .to_owned()
    };
    (
        text(&claim["attempt"]["id"]),
        text(&claim["attempt"]["lease"]),
        text(&claim["action"]["id"]),
    )
}

fn upload_names(lab: &Lab, principal: &str) -> BTreeSet<String> {
    fs::read_dir(lab.root.join("uploads").join(hash(principal.as_bytes())))
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect()
        })
        .unwrap_or_default()
}

fn staging_dirs(lab: &Lab) -> usize {
    fs::read_dir(&lab.root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".incoming-")
        })
        .count()
}

fn attempt_row(lab: &Lab, attempt: &str) -> (String, bool, Value) {
    let store = Store::open(&lab.root).unwrap();
    let (state, drained, body): (String, bool, String) = store
        .db
        .query_row(
            "SELECT state,drained,body FROM attempts WHERE id=?",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    (state, drained, serde_json::from_str(&body).unwrap())
}

#[tokio::test]
async fn executor_routes_require_execute_role() {
    let lab = Lab::new();
    register_mindy(&lab);
    let complete = json!({"attempt":"absent","lease":"l","outcome":"lost","exit_code":125,
        "evidence":{},"drained":false});
    let routes = [
        ("profiles", json!({})),
        ("claim", json!({"claim_key":"k"})),
        ("renew", json!({"leases":[]})),
        ("complete", complete),
    ];
    for (route, body) in &routes {
        let (status, error) = executor_post(&lab, route, ALICE, body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{route}: {error}");
        assert_eq!(error["error"]["code"], "forbidden", "{route}");
    }
    // `local` is the store's own executor; a principal of that name never acts as one.
    let local = "fixture-local-named-credential-not-for-deployment";
    executor_principal(&lab, "local", local, &["read", "execute"]);
    for (route, body) in &routes {
        let (status, error) = executor_post(&lab, route, local, body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{route}: {error}");
    }
    let (status, body) = executor_post(&lab, "profiles", MINDY, json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["api_version"], "1");
    assert_eq!(body["result"]["executor"], "mindy");
    assert_eq!(body["result"]["lease_seconds"], 300);
    // Executors upload their results; a reader may not upload at all.
    lab.api.upload(MINDY, &hash(b"log\n"), b"log\n").unwrap();
    let reader = "fixture-reader-credential-not-for-deployment";
    executor_principal(&lab, "reader", reader, &["read"]);
    assert_eq!(
        lab.api
            .upload(reader, &hash(b"log\n"), b"log\n")
            .unwrap_err()
            .status,
        403
    );
}

#[tokio::test]
async fn unregistered_executor_is_refused() {
    let lab = Lab::new();
    let ghost = "fixture-ghost-executor-credential-not-for-deployment";
    executor_principal(&lab, "ghost", ghost, &["read", "execute"]);
    queue_job(&lab, &lab.a, "dep");
    let complete = json!({"attempt":"absent","lease":"l","outcome":"lost","exit_code":125,
        "evidence":{},"drained":false});
    for (route, body) in [
        ("profiles", json!({})),
        ("claim", json!({"claim_key":"k","verified":[lab.profile]})),
        ("renew", json!({"leases":[]})),
        ("complete", complete),
    ] {
        let (status, error) = executor_post(&lab, route, ghost, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{route}: {error}");
        assert_eq!(error["error"]["code"], "executor_unregistered", "{route}");
    }
    assert_eq!(lab.rows("attempts"), 0);
    configure(
        &lab,
        "ghost",
        patchweft::authority::ConfigureRequest {
            slots: Some(1),
            memory_mb: Some(1024),
            ..Default::default()
        },
    );
    let (status, body) = executor_post(&lab, "profiles", ghost, json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["profiles"][0]["id"], lab.profile);
}

#[tokio::test]
async fn verified_outside_profile_allowlist_is_ignored() {
    // Gate P3-3: a narrowed grant narrows what is claimed; it does not refuse the executor,
    // which would take a 403 for revocation and exit.
    let lab = Lab::new();
    register_mindy(&lab);
    let action = queue_job(&lab, &lab.a, "dep");
    let other = register(&lab, "other", "true");
    let (status, body) = executor_post(
        &lab,
        "claim",
        MINDY,
        json!({"claim_key":"k1","verified":[lab.profile, other]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["claim"]["action"]["id"],
        json!(action),
        "{body}"
    );
    // Profiles are described only within the executor's allowlist.
    let (status, body) =
        executor_post(&lab, "profiles", MINDY, json!({"ids":[lab.profile, other]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let described: Vec<&str> = body["result"]["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(described, [lab.profile.as_str()]);
}

#[tokio::test]
async fn claims_never_cross_project_scope() {
    let lab = Lab::new();
    register_mindy(&lab);
    let elsewhere = queue_job(&lab, &lab.b, "dep");
    let here = queue_job(&lab, &lab.a, "dep");
    let (attempt, lease, claimed) = claim_as_mindy(&lab, "k1").await;
    assert_eq!(claimed, here);
    // A free slot remains, but the earlier action belongs to a project mindy may not read.
    let (status, body) = executor_post(
        &lab,
        "claim",
        MINDY,
        json!({"claim_key":"k2","verified":[lab.profile]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["claim"], Value::Null);
    let store = Store::open(&lab.root).unwrap();
    assert_eq!(
        engine::action_state(&store.db, &elsewhere).unwrap(),
        "queued"
    );
    // Scope is rechecked on every call: narrowing the grant fences work already held.
    store
        .db
        .execute(
            "UPDATE api_principals SET body=json_set(body,'$.projects',json(?)) WHERE id='mindy'",
            [json!([lab.pb]).to_string()],
        )
        .unwrap();
    let (status, body) = executor_post(
        &lab,
        "renew",
        MINDY,
        json!({"leases":[{"attempt":attempt,"lease":lease}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["leases"][0]["status"], "fenced");
}

#[tokio::test]
async fn a_candidate_names_the_undrained_attempt_its_interrupted_job_waits_on() {
    let lab = Lab::new();
    register_mindy(&lab);
    let mut store = Store::open(&lab.root).unwrap();
    let candidate = engine::submit(&mut store, &lab.a, &lab.profile, "agent", vec![]).unwrap();
    let query = Query::Candidate {
        candidate: candidate.id.clone(),
    };
    let dep = |lab: &Lab| lab.api.query(ALICE, &query).unwrap()["jobs"]["dep"].clone();
    let verdict = |lab: &Lab| lab.api.query(ALICE, &query).unwrap()["state"].clone();
    // mindy's claim is recorded, but the response never reaches it.
    let (attempt, _, action) = claim_as_mindy(&lab, "lost").await;
    assert_eq!(action, candidate.required["dep"]);
    assert!(dep(&lab).get("awaiting_drain").is_none());
    let expired = patchweft::authority::expire(&mut store, patchweft::now() + 1e6, 0.0).unwrap();
    assert_eq!(expired, std::slice::from_ref(&attempt));
    let lease_expires = attempt_row(&lab, &attempt).2["lease_expires"].clone();
    assert!(lease_expires.is_f64());
    // Nothing was recorded of the attempt, so mindy can still report its drain.
    assert_eq!(
        dep(&lab),
        json!({"action":action,"state":"interrupted",
               "awaiting_drain":{"attempt":attempt,"executor":"mindy","lease_expires":lease_expires,
                                 "may_self_heal":true}})
    );
    // Not a verdict yet: the candidate is pending, as while the job is queued or running.
    assert_eq!(verdict(&lab), "pending");
    // The replayed key says mindy never recorded the attempt: drained, and the job queued again.
    let (status, body) = executor_post(
        &lab,
        "claim",
        MINDY,
        json!({"claim_key":"lost","verified":[lab.profile],"unrecorded":attempt}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["fenced"], json!(attempt));
    assert_eq!(body["result"]["claim"], Value::Null);
    assert_eq!(dep(&lab), json!({"action":action,"state":"queued"}));
    // A late undrained report is refused but recorded: that coordinator never reports again.
    let (refused, lease, _) = claim_as_mindy(&lab, "refused").await;
    patchweft::authority::expire(&mut store, patchweft::now() + 1e6, 0.0).unwrap();
    assert_eq!(dep(&lab)["awaiting_drain"]["may_self_heal"], true);
    let (status, error) = executor_post(
        &lab,
        "complete",
        MINDY,
        json!({"attempt":refused,"lease":lease,"outcome":"lost","exit_code":125,
               "evidence":{},"drained":false}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "attempt_fenced");
    assert_eq!(attempt_row(&lab, &refused).2["refused"]["outcome"], "lost");
    let job = dep(&lab);
    assert_eq!(job["awaiting_drain"]["attempt"], json!(refused));
    assert_eq!(job["awaiting_drain"]["may_self_heal"], false);
    assert_eq!(verdict(&lab), "failed");
    patchweft::authority::confirm_drained(&mut store, &refused).unwrap();
    engine::retry(&mut store, &candidate.id, &[]).unwrap();
    // Nothing reported within two of mindy's leases after the lapse: the verdict is failed.
    let (silent, _, _) = claim_as_mindy(&lab, "silent").await;
    patchweft::authority::expire(&mut store, patchweft::now() + 1e6, 0.0).unwrap();
    assert_eq!(verdict(&lab), "pending");
    store
        .db
        .execute(
            "UPDATE attempts SET body=json_set(body,'$.lapsed',?) WHERE id=?",
            rusqlite::params![patchweft::now() - 601.0, silent],
        )
        .unwrap();
    assert_eq!(dep(&lab)["awaiting_drain"]["may_self_heal"], false);
    assert_eq!(verdict(&lab), "failed");
    patchweft::authority::confirm_drained(&mut store, &silent).unwrap();
    engine::retry(&mut store, &candidate.id, &[]).unwrap();
    // A report recorded undrained ends the attempt: only an operator's confirmation clears it,
    // whether the job was interrupted, failed or canceled, since `retry` refuses each.
    for (key, report, state) in [
        (
            "lost-undrained",
            json!({"outcome":"lost","exit_code":125,"evidence":{}}),
            "interrupted",
        ),
        (
            "failed-undrained",
            json!({"outcome":"completed","exit_code":1,"evidence":{"exit_code":1}}),
            "failed",
        ),
        (
            "canceled-undrained",
            json!({"outcome":"completed","reason":"no-requesters","exit_code":143,
                   "evidence":{"exit_code":143}}),
            "canceled",
        ),
    ] {
        let (attempt, lease, _) = claim_as_mindy(&lab, key).await;
        let mut body = report;
        body["attempt"] = json!(attempt);
        body["lease"] = json!(lease);
        body["drained"] = json!(false);
        let (status, response) = executor_post(&lab, "complete", MINDY, body).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        let job = dep(&lab);
        assert_eq!(job["state"], state);
        assert_eq!(job["awaiting_drain"]["attempt"], json!(attempt), "{state}");
        assert_eq!(job["awaiting_drain"]["may_self_heal"], false, "{state}");
        // Confirmed drained: nothing is awaited.
        patchweft::authority::confirm_drained(&mut store, &attempt).unwrap();
        assert_eq!(dep(&lab), json!({"action":action,"state":state}));
        engine::retry(&mut store, &candidate.id, &[]).unwrap();
    }
}

#[test]
fn grant_execute_requires_read_and_reserves_local() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    let policy = |id: &str, roles: &[&str]| Principal {
        id: id.into(),
        projects: BTreeSet::from([lab.pa.clone()]),
        profiles: BTreeSet::from([lab.profile.clone()]),
        roles: roles.iter().map(|role| role.to_string()).collect(),
        max_pending: 1,
    };
    let file = |name: &str| lab._temp.path().join(name);
    let error = api::grant(&store, policy("mindy", &["execute"]), &file("execute-only"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("read"), "{error}");
    assert!(!file("execute-only").exists());
    let error = api::grant(
        &store,
        policy("local", &["read", "execute"]),
        &file("local-executor"),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("local"), "{error}");
    assert!(!file("local-executor").exists());
    let granted = api::grant(
        &store,
        policy("mindy", &["read", "execute"]),
        &file("mindy"),
    )
    .unwrap();
    assert_eq!(granted["principal"], "mindy");
    // Only executors are kept away from the name.
    api::grant(&store, policy("local", &["read"]), &file("local-reader")).unwrap();
    assert!(api::grant(&store, policy("x", &["exec"]), &file("typo")).is_err());
}

/// An attempt claimed by mindy and registered as running: (attempt, lease, action).
async fn running_on_mindy(lab: &Lab, key: &str) -> (String, String, String) {
    queue_job(lab, &lab.a, "dep");
    let (attempt, lease, action) = claim_as_mindy(lab, key).await;
    let (status, body) = executor_post(
        lab,
        "renew",
        MINDY,
        json!({"leases":[{"attempt":attempt,"lease":lease,"started":{"pid":4242,"start":"1"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["leases"][0]["registered"], true, "{body}");
    (attempt, lease, action)
}

/// A passing report naming a log and `dist/report.txt`.
fn passing(attempt: &str, lease: &str, log: &[u8], artifact: &[u8]) -> Value {
    let entry = json!({"sha256":hash(artifact),"mode":0o644,"bytes":artifact.len()});
    json!({"attempt":attempt,"lease":lease,"outcome":"completed","exit_code":0,
        "evidence":{"exit_code":0,"log_sha256":hash(log),"artifacts":{"dist/report.txt":entry},
                    "host":"mindy","executor":"spoofed"},
        "drained":true,"log":{"sha256":hash(log),"bytes":log.len()},
        "artifacts":{"dist/report.txt":entry}})
}

#[tokio::test]
async fn remote_complete_moves_uploads_and_checks_hashes() {
    use std::os::unix::fs::PermissionsExt;
    let lab = Lab::new();
    register_mindy(&lab);
    let (attempt, lease, action) = running_on_mindy(&lab, "k1").await;
    let (log, artifact) = (b"remote log\n".as_slice(), b"artifact\n".as_slice());
    let report = passing(&attempt, &lease, log, artifact);
    lab.api.upload(MINDY, &hash(log), log).unwrap();
    // An upload whose bytes no longer hash to its name is discarded and asked for again.
    let area = lab.root.join("uploads").join(hash(b"mindy"));
    fs::write(area.join(hash(artifact)), b"corrupt\n").unwrap();
    let (status, error) = executor_post(&lab, "complete", MINDY, report.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["error"]["code"], "missing_blob");
    assert_eq!(error["error"]["detail"]["missing"], json!([hash(artifact)]));
    assert_eq!(upload_names(&lab, "mindy"), BTreeSet::from([hash(log)]));
    lab.api.upload(MINDY, &hash(artifact), artifact).unwrap();
    // A declared length that differs from the upload is refused before anything moves.
    let mut wrong = report.clone();
    wrong["log"]["bytes"] = json!(log.len() + 1);
    let (status, error) = executor_post(&lab, "complete", MINDY, wrong).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["error"]["code"], "invalid_evidence");
    assert_eq!(upload_names(&lab, "mindy").len(), 2);
    assert_eq!(attempt_row(&lab, &attempt).0, "running");

    let (status, body) = executor_post(&lab, "complete", MINDY, report.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"],
        json!({"attempt":attempt,"status":"recorded","state":"passed","drained":true,
               "replayed":false,"drain_recorded":false})
    );
    // The uploads moved into the attempt directory: the executor's quota is free again.
    assert!(upload_names(&lab, "mindy").is_empty());
    assert_eq!(staging_dirs(&lab), 0);
    let directory = lab.root.join("attempts").join(&attempt);
    assert_eq!(fs::read(directory.join("output.log")).unwrap(), log);
    let copy = directory.join("artifacts/dist/report.txt");
    assert_eq!(fs::read(&copy).unwrap(), artifact);
    assert_eq!(
        fs::metadata(&copy).unwrap().permissions().mode() & 0o777,
        0o444
    );
    let manifest: Value =
        serde_json::from_slice(&fs::read(directory.join("artifacts.json")).unwrap()).unwrap();
    assert_eq!(manifest, report["artifacts"]);
    let (state, drained, body) = attempt_row(&lab, &attempt);
    assert_eq!((state.as_str(), drained), ("passed", true));
    assert_eq!(body["result"]["executor"], "mindy");
    assert_eq!(body["result"]["host"], "mindy");
    // The remote pass is delivered like a local one.
    use std::io::Read;
    let mut contents = Vec::new();
    lab.api
        .artifact(MINDY, &action, &hash(artifact))
        .unwrap()
        .read_to_end(&mut contents)
        .unwrap();
    assert_eq!(contents, artifact);
    let log_page = lab
        .api
        .query(
            MINDY,
            &Query::Logs {
                attempt: attempt.clone(),
                offset: 0,
                limit: 1024,
            },
        )
        .unwrap();
    assert_eq!(log_page["observed_bytes"], log.len());
    // A lost response is answered again without its uploads; a different report is refused.
    let (status, body) = executor_post(&lab, "complete", MINDY, report.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["replayed"], true);
    let mut different = report;
    different["drained"] = json!(false);
    let (status, error) = executor_post(&lab, "complete", MINDY, different).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "completion_conflict");
}

#[tokio::test]
async fn missing_blob_is_422_with_detail() {
    let lab = Lab::new();
    register_mindy(&lab);
    let (attempt, lease, _) = running_on_mindy(&lab, "k1").await;
    let log = b"uploaded log\n".as_slice();
    lab.api.upload(MINDY, &hash(log), log).unwrap();
    let mut report = passing(&attempt, &lease, log, b"never uploaded\n");
    let other = json!({"sha256":hash(b"also absent\n"),"mode":0o644,"bytes":12});
    report["artifacts"]["other.txt"] = other.clone();
    report["evidence"]["artifacts"]["other.txt"] = other;
    let (status, error) = executor_post(&lab, "complete", MINDY, report.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["error"]["code"], "missing_blob");
    let mut missing = vec![hash(b"never uploaded\n"), hash(b"also absent\n")];
    missing.sort();
    assert_eq!(error["error"]["detail"], json!({"missing":missing}));
    // Nothing moved and nothing was recorded.
    assert_eq!(upload_names(&lab, "mindy"), BTreeSet::from([hash(log)]));
    assert_eq!(staging_dirs(&lab), 0);
    assert_eq!(attempt_row(&lab, &attempt).0, "running");
    assert!(!lab.root.join("attempts").join(&attempt).exists());
    // Digests become file names, so a malformed one is refused outright.
    report["log"]["sha256"] = json!("../../state.sqlite");
    let (status, error) = executor_post(&lab, "complete", MINDY, report).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["error"]["code"], "invalid");
}

#[tokio::test]
async fn a_complete_with_a_large_manifest_is_not_refused_for_its_size() {
    // Review P2-3: the manifest travels twice, so the route takes more than the 2 MiB default.
    let lab = Lab::new();
    register_mindy(&lab);
    let (attempt, lease, _) = running_on_mindy(&lab, "k1").await;
    let artifacts: serde_json::Map<String, Value> = (0..30_000)
        .map(|n| {
            (
                format!("dist/file-{n:05}.js"),
                json!({"sha256":hash(format!("{n}").as_bytes()),"mode":0o644,"bytes":1}),
            )
        })
        .collect();
    let body = json!({"attempt":attempt,"lease":lease,"outcome":"completed","exit_code":0,
        "evidence":{"exit_code":0,"reason":null,"artifacts":artifacts},"drained":true,
        "artifacts":artifacts});
    assert!(body.to_string().len() > 4 * 1024 * 1024);
    let (status, error) = executor_post(&lab, "complete", MINDY, body).await;
    // Refused for its missing uploads, not for its size.
    assert_ne!(status, StatusCode::PAYLOAD_TOO_LARGE, "{error}");
    assert_eq!(error["error"]["code"], "missing_blob", "{error}");
}

#[tokio::test]
async fn fenced_complete_discards_staged_uploads() {
    let lab = Lab::new();
    register_mindy(&lab);
    let (attempt, lease, action) = running_on_mindy(&lab, "k1").await;
    let log = b"partial log\n".as_slice();
    let lost = |lease: &str, drained: bool| {
        json!({"attempt":attempt,"lease":lease,"outcome":"lost","exit_code":125,
            "evidence":{"error":"supervisor exited without a result"},"drained":drained,
            "log":{"sha256":hash(log),"bytes":log.len()}})
    };
    lab.api.upload(MINDY, &hash(log), log).unwrap();
    // Another lease's report is refused without touching anything.
    let (status, error) = executor_post(&lab, "complete", MINDY, lost("stale", true)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "attempt_fenced");
    assert_eq!(upload_names(&lab, "mindy"), BTreeSet::from([hash(log)]));
    // The lease lapses: the attempt is interrupted and stays undrained.
    let mut store = Store::open(&lab.root).unwrap();
    let expired = patchweft::authority::expire(&mut store, patchweft::now() + 1e6, 0.0).unwrap();
    assert_eq!(expired, std::slice::from_ref(&attempt));
    // A late result is never recorded, and its uploads are consumed rather than kept.
    let (status, error) = executor_post(&lab, "complete", MINDY, lost(&lease, false)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "attempt_fenced");
    assert!(upload_names(&lab, "mindy").is_empty());
    assert_eq!(staging_dirs(&lab), 0);
    assert!(!lab.root.join("attempts").join(&attempt).exists());
    assert!(!attempt_row(&lab, &attempt).1);
    // A drain report is still accepted (A2), and its uploads are discarded too.
    lab.api.upload(MINDY, &hash(log), log).unwrap();
    let (status, body) = executor_post(&lab, "complete", MINDY, lost(&lease, true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["status"], "fenced");
    assert_eq!(body["result"]["drain_recorded"], true);
    assert!(upload_names(&lab, "mindy").is_empty());
    assert_eq!(staging_dirs(&lab), 0);
    assert!(!lab.root.join("attempts").join(&attempt).exists());
    let (state, drained, body) = attempt_row(&lab, &attempt);
    assert_eq!((state.as_str(), drained), ("interrupted", true));
    assert_eq!(body["result"], Value::Null);
    // The undrained report before it was recorded as refused, which settled the verdict for an
    // operator: this drain releases the capacity and queues nothing.
    assert_eq!(body["refused"]["outcome"], "lost");
    assert_eq!(
        engine::action_state(&store.db, &action).unwrap(),
        "interrupted"
    );
}

#[test]
fn stale_uploads_and_staging_are_purged() {
    use std::time::{Duration, SystemTime};
    let lab = Lab::new();
    let now = SystemTime::now();
    let aged = |path: &Path, age: Duration| {
        fs::File::open(path)
            .unwrap()
            .set_modified(now - age)
            .unwrap();
    };
    let hour = Duration::from_secs(3600);
    let mut kept = vec![];
    let mut purged = vec![];
    for principal in ["mindy", "alice"] {
        let area = lab.root.join("uploads").join(hash(principal.as_bytes()));
        fs::create_dir_all(&area).unwrap();
        for (name, age) in [("old", 25 * hour), ("fresh", 23 * hour)] {
            let path = area.join(hash(format!("{principal}{name}").as_bytes()));
            fs::write(&path, name).unwrap();
            aged(&path, age);
            if name == "old" {
                purged.push(path)
            } else {
                kept.push(path)
            }
        }
    }
    for (name, age) in [
        (".incoming-old", 2 * hour),
        (".incoming-stale-old", 2 * hour),
        (".incoming-fresh", hour / 2),
    ] {
        let path = lab.root.join(name);
        fs::create_dir_all(path.join("artifacts")).unwrap();
        fs::write(path.join("output.log"), "log").unwrap();
        aged(&path, age);
        if name.ends_with("old") {
            purged.push(path)
        } else {
            kept.push(path)
        }
    }
    assert_eq!(api::purge(&lab.root, now).unwrap(), purged.len());
    for path in &purged {
        assert!(!path.exists(), "{}", path.display());
    }
    for path in &kept {
        assert!(path.exists(), "{}", path.display());
    }
    // Nothing else in the store is touched.
    assert!(lab.root.join("state.sqlite").exists());
    assert_eq!(api::purge(&lab.root, now).unwrap(), 0);
}

fn cli(lab: &Lab, args: &[&str]) -> std::process::Output {
    Process::new(env!("CARGO_BIN_EXE_patchweft"))
        .arg("--state")
        .arg(&lab.root)
        .args(args)
        .output()
        .unwrap()
}

fn cli_value(lab: &Lab, args: &[&str]) -> Value {
    let output = cli(lab, args);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn executor_set_and_list_report_held_and_observed() {
    let lab = Lab::new();
    executor_principal(&lab, "mindy", MINDY, &["read", "execute"]);
    let configured = cli_value(
        &lab,
        &[
            "executor-set",
            "mindy",
            "--slots",
            "2",
            "--memory-mb",
            "2048",
            "--lease-seconds",
            "30",
            "--allow-job",
            "dep",
            "--deny-job",
            "root",
        ],
    );
    assert_eq!(configured["id"], "mindy");
    assert_eq!(
        configured["configured"],
        json!({"slots":2,"memory_mb":2048})
    );
    assert_eq!(configured["lease_seconds"], 30);
    assert_eq!(
        configured["placement"],
        json!({"allow":["dep"],"deny":["root"]})
    );
    assert_eq!(
        configured["held"],
        json!({"slots":0,"memory_mb":0,"attempts":[]})
    );
    for (args, message) in [
        (&["executor-set", "fresh"][..], "slots"),
        (&["executor-set", "mindy", "--slots", "0"][..], "slots"),
        (&["executor-set", "mindy", "--slots", "65"][..], "slots"),
        (
            &["executor-set", "mindy", "--memory-mb", "64"][..],
            "memory",
        ),
        (
            &["executor-set", "mindy", "--lease-seconds", "2"][..],
            "lease",
        ),
        (
            &["executor-set", "mindy", "--lease-seconds", "3601"][..],
            "lease",
        ),
    ] {
        let output = cli(&lab, args);
        assert!(!output.status.success(), "{args:?}");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(message), "{args:?}: {error}");
    }
    // The local executor's budget is the store's own settings.
    let local = cli_value(
        &lab,
        &[
            "executor-set",
            "local",
            "--slots",
            "3",
            "--memory-mb",
            "3072",
        ],
    );
    assert_eq!(local["configured"], json!({"slots":3,"memory_mb":3072}));
    assert_eq!(
        Store::open(&lab.root).unwrap().capacity().unwrap(),
        (3, 3072)
    );
    queue_job(&lab, &lab.a, "dep");
    let (status, body) = executor_post(
        &lab,
        "claim",
        MINDY,
        json!({"claim_key":"k1","verified":[lab.profile],"memory_limit_mb":1536,
               "disk_free_mb":70000,"host":"mindy-host","engine":"e".repeat(64)}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let attempt = body["result"]["claim"]["attempt"]["id"].clone();
    let listed = cli_value(&lab, &["executors"]);
    let ids: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["local", "mindy"]);
    let mindy = &listed[1];
    assert_eq!(mindy["configured"], json!({"slots":2,"memory_mb":2048}));
    // The reported cgroup limit caps what the executor is given.
    assert_eq!(mindy["effective"], json!({"slots":2,"memory_mb":1536}));
    assert_eq!(mindy["held"]["slots"], 1);
    assert_eq!(mindy["held"]["memory_mb"], 1024);
    assert_eq!(mindy["held"]["attempts"][0]["id"], attempt);
    assert_eq!(mindy["held"]["attempts"][0]["state"], "claimed");
    let observed = &mindy["observed"];
    assert!(observed["last_seen"].as_f64().is_some(), "{observed}");
    assert_eq!(observed["verified"], json!([lab.profile]));
    assert_eq!(observed["memory_limit_mb"], 1536);
    assert_eq!(observed["disk_free_mb"], 70000);
    assert_eq!(observed["host"], "mindy-host");
    // Remote work never counts against the local budget.
    assert_eq!(listed[0]["held"]["slots"], 0);
    assert_eq!(listed[0]["effective"], json!({"slots":3,"memory_mb":3072}));
    let events = Store::open(&lab.root).unwrap().events(0).unwrap();
    let configured: Vec<&str> = events
        .iter()
        .filter(|e| e["kind"] == "executor.configured")
        .map(|e| e["subject"].as_str().unwrap())
        .collect();
    assert_eq!(configured, ["mindy", "local"]);
}

// Incremental source bundles (multi-host step 7).

/// Commits `contents` as `name` and imports it into project `a`: (revision, commit).
fn commit_revision(lab: &Lab, name: &str, contents: &[u8]) -> (String, String) {
    fs::write(lab.repo.join(name), contents).unwrap();
    git(&lab.repo, &["add", name]);
    git(&lab.repo, &["commit", "--quiet", "-m", name]);
    let mut store = Store::open(&lab.root).unwrap();
    let revision = source::import(&mut store, "a", &lab.repo, "HEAD", "fixture").unwrap();
    (revision.id, revision.commit)
}

/// Hex text that compresses poorly, so bundle sizes track history.
fn noise(bytes: usize) -> Vec<u8> {
    (0..bytes / 64)
        .flat_map(|i| hash(i.to_string().as_bytes()).into_bytes())
        .collect()
}

fn verifies(repo: &Path, bundle: &Path) -> bool {
    Process::new("git")
        .arg("-C")
        .arg(repo)
        .args(["bundle", "verify"])
        .arg(bundle)
        .output()
        .unwrap()
        .status
        .success()
}

fn bare(lab: &Lab, name: &str) -> PathBuf {
    let path = lab._temp.path().join(name);
    git(
        lab._temp.path(),
        &["init", "--bare", "--quiet", path.to_str().unwrap()],
    );
    path
}

async fn get(lab: &Lab, uri: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let response = lab
        .router()
        .oneshot(
            Request::get(uri)
                .header("authorization", format!("Bearer {ALICE}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes.to_vec())
}

#[tokio::test]
async fn incremental_bundle_needs_only_missing_history() {
    let lab = Lab::new();
    let (_, parent) = commit_revision(&lab, "large", &noise(256 * 1024));
    // A mirror holding the parent, as an executor's would.
    let holder = lab._temp.path().join("holder");
    git(
        lab._temp.path(),
        &[
            "clone",
            "--bare",
            "--quiet",
            lab.repo.to_str().unwrap(),
            holder.to_str().unwrap(),
        ],
    );
    let (revision, commit) = commit_revision(&lab, "small", b"small change\n");
    let path = format!("/v1/revisions/{revision}/bundle");
    let (status, _, full) = get(&lab, &path).await;
    assert_eq!(status, StatusCode::OK);
    let (status, headers, incremental) = get(&lab, &format!("{path}?have={parent}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-content-sha256"], hash(&incremental).as_str());
    assert!(
        incremental.len() * 8 < full.len(),
        "{} vs {}",
        incremental.len(),
        full.len()
    );
    let (full_path, incremental_path) = (
        lab._temp.path().join("full.bundle"),
        lab._temp.path().join("incremental.bundle"),
    );
    fs::write(&full_path, &full).unwrap();
    fs::write(&incremental_path, &incremental).unwrap();
    let empty = bare(&lab, "empty");
    assert!(verifies(&empty, &full_path));
    assert!(!verifies(&empty, &incremental_path));
    assert!(verifies(&holder, &incremental_path));
    git(
        &holder,
        &[
            "fetch",
            "--quiet",
            incremental_path.to_str().unwrap(),
            &format!("refs/patchweft/revisions/{revision}:refs/patchweft/revisions/{revision}"),
        ],
    );
    git(
        &holder,
        &["cat-file", "-e", &format!("{commit}^{{commit}}")],
    );
}

#[test]
fn unknown_haves_are_ignored() {
    let lab = Lab::new();
    let (_, parent) = commit_revision(&lab, "large", &noise(64 * 1024));
    let (revision, _) = commit_revision(&lab, "small", b"small change\n");
    let tree = git(&lab.repo, &["rev-parse", "HEAD^{tree}"]);
    let empty = bare(&lab, "empty");
    let (_full_dir, full) = lab.api.source_bundle(ALICE, &revision, &[]).unwrap();
    let full_bytes = fs::metadata(&full).unwrap().len();
    // Absent objects, a tree and a SHA-256-length name: none is a commit, so the bundle is whole.
    let unknown = vec![
        hash(b"never committed")[..40].to_owned(),
        tree,
        "f".repeat(64),
    ];
    let (_dir, ignored) = lab.api.source_bundle(ALICE, &revision, &unknown).unwrap();
    assert!(verifies(&empty, &ignored));
    // Known haves still count beside unknown ones.
    let mixed = vec![unknown[0].clone(), parent];
    let (_dir, partial) = lab.api.source_bundle(ALICE, &revision, &mixed).unwrap();
    assert!(!verifies(&empty, &partial));
    assert!(fs::metadata(&partial).unwrap().len() < full_bytes);
}

#[test]
fn have_already_containing_revision_falls_back_to_full_bundle() {
    let lab = Lab::new();
    let (revision, commit) = commit_revision(&lab, "first", b"first\n");
    let (_, later) = commit_revision(&lab, "second", b"second\n");
    let empty = bare(&lab, "empty");
    for have in [commit.clone(), later] {
        let (_dir, bundle) = lab.api.source_bundle(ALICE, &revision, &[have]).unwrap();
        assert!(verifies(&empty, &bundle));
        let heads = git(&empty, &["bundle", "list-heads", bundle.to_str().unwrap()]);
        assert!(heads.starts_with(&commit), "{heads}");
    }
}

#[tokio::test]
async fn invalid_have_is_400() {
    let lab = Lab::new();
    let path = format!("/v1/revisions/{}/bundle", lab.a);
    let commit = git(&lab.repo, &["rev-parse", "HEAD"]);
    let many = vec![commit.clone(); 33].join(",");
    for query in [
        "have=xyz".to_owned(),
        format!("have={}", commit.to_uppercase()),
        format!("have={}", &commit[..39]),
        format!("have={commit},"),
        "have=".to_owned(),
        format!("have={many}"),
    ] {
        let (status, _, body) = get(&lab, &format!("{path}?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        let error: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["error"]["code"], "invalid", "{query}");
    }
    let (status, _, _) = get(&lab, &format!("{path}?have={}", vec![commit; 32].join(","))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        lab.api
            .source_bundle(ALICE, &lab.a, &["../HEAD".into()])
            .unwrap_err()
            .status,
        400
    );
}
