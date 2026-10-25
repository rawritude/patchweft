use crate::{
    authority,
    declared::{self, Declared},
    file_hash, identity,
    model::*,
    now, source,
    store::{Store, event, passed_key_of, prepare_passed_keys, record},
    unique,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

pub fn tool_identity(profile: &Profile) -> Result<BTreeMap<String, String>> {
    profile
        .tools
        .iter()
        .map(|(name, path)| Ok((name.clone(), file_hash(path)?)))
        .collect()
}

pub fn validate_profile(profile: &Profile, capacity: (u32, u32)) -> Result<()> {
    ensure!(
        profile.version == 1 && !profile.name.is_empty() && !profile.jobs.is_empty(),
        "profile requires version 1, a name and nonempty obligations"
    );
    for (name, path) in &profile.tools {
        ensure!(
            !name.is_empty() && !name.contains('/') && path.is_absolute() && path.is_file(),
            "tools must name absolute local executables"
        );
    }
    let mut names = BTreeSet::new();
    for job in &profile.jobs {
        ensure!(
            !job.name.is_empty() && names.insert(job.name.clone()),
            "duplicate or empty job name"
        );
        ensure!(
            !job.command.is_empty() && profile.tools.contains_key(&job.command[0]),
            "command executable must name a declared tool: {}",
            job.name
        );
        ensure!(
            job.cleanup.is_empty() || profile.tools.contains_key(&job.cleanup[0]),
            "cleanup executable must name a declared tool"
        );
        ensure!(
            job.slots > 0
                && job.slots <= capacity.0
                && job.memory_mb > 0
                && job.memory_mb <= capacity.1
                && job.timeout_seconds > 0,
            "job exceeds configured capacity: {}",
            job.name
        );
        ensure!(
            ["command", "node-tap", "vitest", "junit"].contains(&job.report.kind.as_str()),
            "job needs an explicit execution/report contract: {}",
            job.name
        );
        if job.report.kind != "command" {
            ensure!(
                job.report.minimum > 0,
                "test report must require executed cases"
            );
        }
        if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
            source::relative_path(&job.report.path)?;
        }
        if [Reuse::Exact, Reuse::Files, Reuse::Declared].contains(&job.reuse) {
            ensure!(
                job.audit.trim().chars().count() >= 20,
                "completed-result reuse requires a recorded input audit"
            );
        }
        if job.reuse == Reuse::Files {
            ensure!(
                !job.inputs.is_empty() && job.dependencies.is_empty(),
                "files-only actions require a nonempty complete input list and no ambient dependencies"
            );
        } else {
            ensure!(
                job.inputs.is_empty(),
                "input projection only applies to files reuse"
            );
        }
        for path in job.inputs.iter().chain(job.artifacts.iter()) {
            source::relative_path(path)?;
        }
        for (key, _) in profile.environment.iter().chain(job.environment.iter()) {
            let name = key.to_ascii_uppercase();
            ensure!(
                ![
                    "HOME",
                    "PATH",
                    "TMPDIR",
                    "GIT_DIR",
                    "GIT_WORK_TREE",
                    "LD_PRELOAD"
                ]
                .contains(&name.as_str())
                    && !name.contains("TOKEN")
                    && !name.contains("PASSWORD")
                    && !name.contains("SECRET"),
                "profile cannot override protected process environment: {key}"
            );
        }
    }
    if let Some(file) = &profile.inputs_file {
        source::relative_path(file)?;
    }
    ensure!(
        profile.inputs_file.is_some()
            || profile.jobs.iter().all(|job| job.reuse != Reuse::Declared),
        "declared jobs require the profile's inputs_file"
    );
    let mut ordered = BTreeSet::new();
    loop {
        let old = ordered.len();
        for job in &profile.jobs {
            ensure!(
                job.dependencies.iter().all(|d| names.contains(d)),
                "unknown dependency"
            );
            if job.dependencies.iter().all(|d| ordered.contains(d)) {
                ordered.insert(job.name.clone());
            }
        }
        if ordered.len() == names.len() {
            break;
        }
        ensure!(old != ordered.len(), "profile contains a dependency cycle");
    }
    Ok(())
}

pub fn register(store: &mut Store, path: &Path) -> Result<RegisteredProfile> {
    let definition: Profile = serde_json::from_slice(&fs::read(path)?)?;
    for path in definition.tools.values() {
        // Resolve for validation/hash reads, but preserve argv[0]. Multicall tools such as
        // rustup dispatch cargo/rustc through their symlink invocation name.
        path.canonicalize()?;
    }
    validate_profile(&definition, store.capacity()?)?;
    let tools = tool_identity(&definition)?;
    let engine = file_hash(&std::env::current_exe()?)?;
    let platform = platform();
    let id = profile_id(&definition, &tools, &engine, &platform)?;
    let profile = RegisteredProfile {
        id,
        definition,
        tools,
        engine,
        platform,
    };
    let transaction = store.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO profiles VALUES (?,?)",
        params![profile.id, serde_json::to_string(&profile)?],
    )?;
    event(
        &transaction,
        "profile.registered",
        &profile.id,
        &json!({"name":profile.definition.name}),
    )?;
    transaction.commit()?;
    Ok(profile)
}

pub fn platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

pub fn profile_id(
    definition: &Profile,
    tools: &BTreeMap<String, String>,
    engine: &str,
    platform: &str,
) -> Result<String> {
    identity(&json!({"definition":definition,"tools":tools,"engine":engine,"platform":platform}))
}

pub fn verify_runtime(profile: &RegisteredProfile) -> Result<()> {
    verify_runtime_for(profile, &file_hash(&std::env::current_exe()?)?)
}

/// `engine` is the sha256 of the runner binary that will execute the profile's jobs.
pub fn verify_runtime_for(profile: &RegisteredProfile, engine: &str) -> Result<()> {
    ensure!(
        tool_identity(&profile.definition)? == profile.tools,
        "declared tool bytes changed; register a new profile"
    );
    ensure!(
        engine == profile.engine,
        "runner binary changed; register a new profile"
    );
    ensure!(
        profile.platform == platform(),
        "platform changed; register a new profile"
    );
    Ok(())
}

pub fn class(profile: &RegisteredProfile) -> &'static str {
    if profile.definition.certifying {
        "certifying"
    } else {
        "exploratory"
    }
}

pub fn recipes(profile: &RegisteredProfile) -> Result<BTreeMap<String, String>> {
    recipes_for(profile, JOB_RECIPE)
}

/// Promote and the worker recompute recorded recipes, so the builder for a tag never changes.
pub fn recipes_for(profile: &RegisteredProfile, version: &str) -> Result<BTreeMap<String, String>> {
    ensure!(
        version == JOB_RECIPE,
        "unknown job recipe version: {version}"
    );
    // Exhaustive destructuring: a new field fails to compile here until it is classified as
    // identity or as metadata that never reaches a job's execution.
    let RegisteredProfile {
        id: _,
        definition,
        tools: tool_hashes,
        engine,
        platform,
    } = profile;
    let Profile {
        version: _,
        name: _,
        certifying: _,
        tools: tool_paths,
        environment: profile_environment,
        jobs,
        inputs_file,
    } = definition;
    // Every tool directory is on every job's PATH, so every tool is part of every recipe.
    let tools = tool_paths
        .iter()
        .map(|(name, path)| {
            let sha256 = tool_hashes.get(name).context("tool identity missing")?;
            Ok((name.clone(), json!({"path":path,"sha256":sha256})))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut memo: BTreeMap<String, String> = BTreeMap::new();
    while memo.len() < jobs.len() {
        let before = memo.len();
        for job in jobs {
            let Job {
                name,
                command,
                dependencies,
                slots,
                // Reservations: scheduling only, never seen by the job.
                memory_mb: _,
                // In the action key, not the recipe (decision D6).
                timeout_seconds: _,
                reuse,
                inputs,
                // Why reuse is sound; wording is not identity.
                audit: _,
                environment: job_environment,
                artifacts,
                cleanup,
                report,
                export_slots,
            } = job;
            let Report {
                kind,
                path,
                minimum,
                allow_skips,
            } = report;
            if memo.contains_key(name) || !dependencies.iter().all(|d| memo.contains_key(d)) {
                continue;
            }
            let mut environment = profile_environment.clone();
            environment.extend(job_environment.clone());
            let dependencies: BTreeMap<_, _> = dependencies
                .iter()
                .map(|d| (d.clone(), memo[d].clone()))
                .collect();
            let mut recipe = json!({
                "recipe": JOB_RECIPE,
                "job": name,
                "command": command,
                "cleanup": cleanup,
                "tools": tools,
                "environment": environment,
                "report": {"kind":kind,"path":path,"minimum":minimum,"allow_skips":allow_skips},
                "artifacts": artifacts,
                "reuse": reuse,
                "inputs": inputs,
                "slots": if *export_slots { Some(*slots) } else { None },
                "dependencies": dependencies,
                "engine": engine,
                "platform": platform,
            });
            // Only when set, so every recipe recorded under this tag before it existed still holds.
            if let Some(file) = inputs_file {
                recipe["inputs_file"] = json!(file);
            }
            let recipe = identity(&recipe)?;
            memo.insert(name.clone(), recipe);
        }
        ensure!(memo.len() != before, "profile contains a dependency cycle");
    }
    Ok(memo)
}

pub fn action_key(
    project: &str,
    recipe: &str,
    class: &str,
    timeout_seconds: u64,
    inputs: &Value,
    dependencies: &[String],
) -> Result<String> {
    identity(
        &json!({"key":"patchweft.action/2","project":project,"recipe":recipe,"class":class,
                     "timeout_seconds":timeout_seconds,"inputs":inputs,"dependencies":dependencies}),
    )
}

/// The key under which a declared job's pass is reused: its declared inputs and recipe, plus its
/// dependencies' passed keys (not their actions: a dependency rerun on equal inputs keeps them).
pub fn passed_key(
    project: &str,
    recipe: &str,
    class: &str,
    timeout_seconds: u64,
    inputs: &Value,
    dependencies: &[String],
) -> Result<String> {
    identity(
        &json!({"key":"patchweft.declared/1","project":project,"recipe":recipe,"class":class,
                     "timeout_seconds":timeout_seconds,"inputs":inputs,"dependencies":dependencies}),
    )
}

/// Each job's passed key, dependencies first. A job has one only if it is declared off the fail-safe
/// path and every dependency has one: a dependency's output is an input of its dependents, so a
/// commit-keyed or fail-safe dependency leaves them nothing to reuse under (pw#11 adversary F1).
pub fn passed_keys(
    profile: &RegisteredProfile,
    recipes: &BTreeMap<String, String>,
    declared: &Declared,
    project: &str,
    class: &str,
) -> Result<BTreeMap<String, Option<String>>> {
    let jobs = &profile.definition.jobs;
    let mut keys: BTreeMap<String, Option<String>> = BTreeMap::new();
    while keys.len() < jobs.len() {
        let before = keys.len();
        for job in jobs {
            if keys.contains_key(&job.name)
                || !job.dependencies.iter().all(|d| keys.contains_key(d))
            {
                continue;
            }
            let dependencies: Option<Vec<String>> =
                job.dependencies.iter().map(|d| keys[d].clone()).collect();
            let key = match (declared.identity(&job.name), dependencies) {
                (Some(inputs), Some(dependencies)) => Some(passed_key(
                    project,
                    recipes.get(&job.name).context("job recipe missing")?,
                    class,
                    job.timeout_seconds,
                    inputs,
                    &dependencies,
                )?),
                _ => None,
            };
            keys.insert(job.name.clone(), key);
        }
        ensure!(keys.len() != before, "profile contains a dependency cycle");
    }
    Ok(keys)
}

/// What a job's action must answer to, and what it recorded: a pass reused under its passed key
/// answers to that key (it excludes the dependency actions it consumed), any other action to its
/// action key. A reused job that is no longer declared has no passed key, so it answers to nothing.
/// The one owner of this choice for retention and promote (pw#11 gate P3-5).
struct Answer<'a> {
    project: &'a str,
    recipe: &'a str,
    class: &'a str,
    timeout_seconds: u64,
    inputs: &'a Value,
    passed: Option<&'a str>,
    dependencies: &'a [String],
}

impl Answer<'_> {
    fn matches(&self, db: &Connection, action: &Action, reused: bool) -> Result<bool> {
        let (key, recorded) = if reused {
            (self.passed.map(Into::into), passed_key_of(db, &action.id)?)
        } else {
            let key = action_key(
                self.project,
                self.recipe,
                self.class,
                self.timeout_seconds,
                self.inputs,
                self.dependencies,
            )?;
            (Some(key), Some(action.key.clone()))
        };
        Ok(key.is_some() && key == recorded)
    }
}

/// The pre-recipe key. Only promote reads it, to check actions recorded before the upgrade.
fn legacy_key(
    project: &str,
    profile: &str,
    job: &str,
    inputs: &Value,
    dependencies: &[String],
) -> Result<String> {
    identity(&json!({"project":project,"profile":profile,"job":job,
                          "inputs":inputs, "dependencies":dependencies}))
}

pub fn job<'a>(profile: &'a RegisteredProfile, name: &str) -> Result<&'a Job> {
    profile
        .definition
        .jobs
        .iter()
        .find(|j| j.name == name)
        .context("unknown job")
}

/// The profile's declarations at the revision, read only when one of its jobs is declared.
pub fn declarations(store: &Store, revision: &Revision, profile: &Profile) -> Declared {
    if profile.jobs.iter().any(|job| job.reuse == Reuse::Declared) {
        declared::resolve(
            &source::repository(store, &revision.project),
            &revision.commit,
            profile,
        )
    } else {
        Declared::default()
    }
}

/// The inputs of a job's action key, the key outstanding work is shared under: the whole commit
/// unless the job is projected to files. A declared job's declared inputs key only its passed key
/// (`passed_key`), so queued and running work never joins across revisions (pw#11 gate P2-1).
pub fn input_identity(store: &Store, revision: &Revision, job: &Job) -> Result<Value> {
    if job.reuse != Reuse::Files {
        return Ok(json!({"commit":revision.commit}));
    }
    let repo = source::repository(store, &revision.project);
    let mut files = BTreeMap::new();
    for path in &job.inputs {
        let result = source::git(&repo, &["ls-tree", &revision.commit, "--", path])?;
        ensure!(
            result.lines().count() == 1,
            "declared input missing: {path}"
        );
        let (metadata, actual) = result.split_once('\t').context("invalid git entry")?;
        let fields: Vec<_> = metadata.split_whitespace().collect();
        ensure!(
            actual == path
                && fields.len() == 3
                && fields[1] == "blob"
                && ["100644", "100755"].contains(&fields[0]),
            "input must be an exact regular file: {path}"
        );
        files.insert(path.clone(), metadata.to_owned());
    }
    Ok(json!({"files":files}))
}

struct Retention {
    source: Candidate,
    retained: Vec<String>,
    not_retained: BTreeMap<String, String>,
}

struct Requester<'a> {
    revision: &'a Revision,
    profile: &'a RegisteredProfile,
    recipes: BTreeMap<String, String>,
    owner: &'a str,
    context: &'a str,
    requested: BTreeMap<String, String>,
    retention: Option<Retention>,
    declared: Declared,
    passed: BTreeMap<String, Option<String>>,
    reused: BTreeMap<String, Reused>,
}

/// Records why declared jobs keep the commit key and which keys name no job. Never fails a
/// submit: an invalid file only puts every declared job on the fail-safe path.
fn report_declarations(db: &Connection, candidate: &str, declared: &Declared) -> Result<()> {
    if let Some(reason) = &declared.invalid {
        event(
            db,
            "inputs.invalid",
            candidate,
            &json!({"candidate":candidate,"reason":reason}),
        )?;
    }
    for key in &declared.unmatched {
        event(
            db,
            "inputs.unmatched",
            candidate,
            &json!({"candidate":candidate,"key":key}),
        )?;
    }
    Ok(())
}

/// The earliest passed action with this passed key. Nothing else is reused: not a failed,
/// interrupted, canceled or stopping action.
fn reusable(db: &Connection, key: &str) -> Result<Option<Action>> {
    let body: Option<String> = db
        .query_row(
            "SELECT a.body FROM passed_keys p JOIN actions a ON a.id=p.action
             WHERE p.key=? AND a.state='passed' ORDER BY a.created LIMIT 1",
            [key],
            |r| r.get(0),
        )
        .optional()?;
    Ok(body.map(|body| serde_json::from_str(&body)).transpose()?)
}

/// The predecessor's action when every retention condition holds, else the first failed one.
fn retainable(
    store: &Store,
    request: &Requester,
    definition: &Job,
    inputs: &Value,
    dependencies: &[String],
) -> Result<std::result::Result<Action, String>> {
    let refuse = |reason: &str| Ok(Err(reason.to_owned()));
    let Some(retention) = &request.retention else {
        return refuse("no predecessor");
    };
    if definition.reuse == Reuse::Never {
        return refuse("reuse policy is never");
    }
    let Some(id) = retention.source.required.get(&definition.name) else {
        return refuse("predecessor has no such obligation");
    };
    let action = store.action(id)?;
    let Some(identity) = action.identity.clone() else {
        return refuse("predecessor action predates job identity");
    };
    let attempt: Option<String> = store
        .db
        .query_row(
            "SELECT attempt FROM actions WHERE id=? AND state='passed'",
            [id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(attempt) = attempt else {
        return refuse("predecessor obligation has not passed");
    };
    let drained: bool =
        store
            .db
            .query_row("SELECT drained FROM attempts WHERE id=?", [&attempt], |r| {
                r.get(0)
            })?;
    let Some(evidence) = store.attempt(&attempt)?.result.filter(|_| drained) else {
        return refuse("predecessor obligation has not passed");
    };
    if identity.class != class(request.profile) {
        return refuse("evidence class differs");
    }
    let recipe = &request.recipes[&definition.name];
    let executed = recipes_for(&store.profile(&action.profile)?, &identity.version)
        .ok()
        .and_then(|r| r.get(&definition.name).cloned());
    if executed.as_ref() != Some(recipe) || identity.recipe != *recipe {
        return refuse("job identity changed");
    }
    // The predecessor reused this pass from another candidate, whose dependency actions it
    // consumed; its passed key deliberately excludes them, so that key is checked instead.
    let reused = retention
        .source
        .reused
        .get(&definition.name)
        .is_some_and(|reused| reused.action == *id);
    if !reused && action.dependencies != dependencies {
        return refuse("dependency actions differ");
    }
    let answer = Answer {
        project: &request.revision.project,
        recipe,
        class: &identity.class,
        timeout_seconds: identity.timeout_seconds,
        inputs,
        passed: request
            .passed
            .get(&definition.name)
            .and_then(|key| key.as_deref()),
        dependencies,
    };
    if !answer.matches(&store.db, &action, reused)? {
        return refuse("action key differs");
    }
    if !evidence["timer_seconds"]
        .as_f64()
        .is_some_and(|t| t < definition.timeout_seconds as f64)
    {
        return refuse("observation exceeded the timeout budget");
    }
    Ok(Ok(action))
}

fn request_job(store: &mut Store, request: &mut Requester, name: &str) -> Result<String> {
    if let Some(id) = request.requested.get(name) {
        return Ok(id.clone());
    }
    let definition = job(request.profile, name)?.clone();
    let mut dependencies = Vec::new();
    for dependency in &definition.dependencies {
        dependencies.push(request_job(store, request, dependency)?);
    }
    let inputs = input_identity(store, request.revision, &definition)?;
    let recipe = request
        .recipes
        .get(name)
        .context("job recipe missing")?
        .clone();
    let class = class(request.profile);
    let key = action_key(
        &request.revision.project,
        &recipe,
        class,
        definition.timeout_seconds,
        &inputs,
        &dependencies,
    )?;
    // Only a declared job off the fail-safe path, with every dependency keyed too, has a passed key.
    let passed = request.passed.get(name).cloned().flatten();
    let (owner, context, profile) = (request.owner, request.context, request.profile);
    // Before the generic lookup, so an unrelated queued row cannot capture the job and break the chain.
    let outcome = match request.retention {
        Some(_) => Some(retainable(
            store,
            request,
            &definition,
            &inputs,
            &dependencies,
        )?),
        None => None,
    };
    if let (Some(outcome), Some(retention)) = (outcome, request.retention.as_mut()) {
        match outcome {
            Ok(action) => {
                let transaction = store.transaction()?;
                transaction.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                                    params![action.id, owner, context])?;
                event(
                    &transaction,
                    "action.retained",
                    &action.id,
                    &json!({"candidate":context,"from":retention.source.id,"owner":owner,
                            "requesting_profile":profile.id,"executed_under":action.profile,
                            "recipe":recipe}),
                )?;
                transaction.commit()?;
                retention.retained.push(name.into());
                if let Some(reused) = retention.source.reused.get(name)
                    && reused.action == action.id
                {
                    request.reused.insert(name.into(), reused.clone());
                }
                request.requested.insert(name.into(), action.id.clone());
                return Ok(action.id);
            }
            Err(reason) => {
                retention.not_retained.insert(name.into(), reason);
            }
        }
    }
    if let Some(passed) = &passed {
        prepare_passed_keys(&store.db)?;
        if let Some(action) = reusable(&store.db, passed)? {
            let reused = Reused {
                action: action.id.clone(),
                from_candidate: action
                    .origin
                    .as_ref()
                    .map(|origin| origin.context.clone())
                    .unwrap_or_default(),
                from_revision: action.revision.clone(),
            };
            let transaction = store.transaction()?;
            transaction.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                                params![action.id, owner, context])?;
            event(
                &transaction,
                "action.reused",
                &action.id,
                &json!({"candidate":context,"owner":owner,"from":reused.from_candidate,
                        "recipe":recipe,"key":passed}),
            )?;
            transaction.commit()?;
            request.reused.insert(name.into(), reused);
            request.requested.insert(name.into(), action.id.clone());
            return Ok(action.id);
        }
    }
    let requirement = Reservation::from(&definition);
    let transaction = store.transaction()?;
    let mut action_id = None;
    if definition.reuse != Reuse::Never {
        let states = if [Reuse::Exact, Reuse::Files].contains(&definition.reuse) {
            "'queued','running','passed'"
        } else {
            "'queued','running'"
        };
        let rows: Vec<(String, String, Option<String>, String)> = {
            let mut statement = transaction.prepare(&format!(
                "SELECT id,state,attempt,body FROM actions WHERE key=? AND state IN ({states}) ORDER BY created"
            ))?;
            statement
                .query_map([&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        for (id, state, attempt, body) in rows {
            match state.as_str() {
                "queued" => {
                    let mut action: Action = serde_json::from_str(&body)?;
                    if let Some(current) = action.reservation {
                        let raised = current.max(requirement);
                        if raised != current {
                            action.reservation = Some(raised);
                            transaction.execute(
                                "UPDATE actions SET body=? WHERE id=? AND state='queued'",
                                params![serde_json::to_string(&action)?, id],
                            )?;
                            event(
                                &transaction,
                                "action.reservation_raised",
                                &id,
                                &json!({"from":current,"to":raised,"requesting_profile":profile.id}),
                            )?;
                        }
                    }
                    action_id = Some(id);
                    break;
                }
                "running" => {
                    // A running attempt's reservation is fixed; join only if it already covers this one.
                    let attempt: Attempt = record(
                        &transaction,
                        "attempts",
                        attempt.as_deref().context("running action lacks attempt")?,
                    )?;
                    if attempt.slots >= requirement.slots
                        && attempt.memory_mb >= requirement.memory_mb
                    {
                        action_id = Some(id);
                        break;
                    }
                }
                _ => {
                    action_id = Some(id);
                    break;
                }
            }
        }
    }
    let shared = action_id.is_some();
    let id = action_id.unwrap_or_else(unique);
    if !shared {
        let action = Action {
            id: id.clone(),
            key: key.clone(),
            revision: request.revision.id.clone(),
            profile: profile.id.clone(),
            job: name.into(),
            dependencies,
            created: now(),
            identity: Some(ActionIdentity {
                version: JOB_RECIPE.into(),
                recipe: recipe.clone(),
                class: class.into(),
                timeout_seconds: definition.timeout_seconds,
            }),
            reservation: Some(requirement),
            origin: Some(Origin {
                owner: owner.into(),
                context: context.into(),
            }),
            // The passed key, so runs whose dependencies differ never share a value (pw#11 gate P2).
            declared_inputs: passed.clone(),
            declared_base: request.declared.base(name).map(Into::into),
        };
        transaction.execute(
            "INSERT INTO actions VALUES (?,?,'queued',NULL,?,?)",
            params![id, key, serde_json::to_string(&action)?, action.created],
        )?;
        if let Some(passed) = &passed {
            transaction.execute("INSERT INTO passed_keys VALUES (?,?)", params![id, passed])?;
        }
        event(
            &transaction,
            "action.queued",
            &id,
            &json!({"revision":request.revision.id,"job":name,"profile":profile.id,"recipe":recipe}),
        )?;
    }
    transaction.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                        params![id, owner, context])?;
    event(
        &transaction,
        if shared {
            "action.shared"
        } else {
            "action.requested"
        },
        &id,
        &json!({"owner":owner,"context":context,"revision":request.revision.id,
                "profile":profile.id,"recipe":recipe}),
    )?;
    transaction.commit()?;
    request.requested.insert(name.into(), id.clone());
    Ok(id)
}

pub fn request(
    store: &mut Store,
    revision: &str,
    profile: &str,
    name: &str,
    owner: &str,
) -> Result<Action> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    let revision = store.revision(revision)?;
    let profile = store.profile(profile)?;
    verify_runtime(&profile)?;
    let declared = declarations(store, &revision, &profile.definition);
    report_declarations(&store.db, "", &declared)?;
    let recipes = recipes(&profile)?;
    let passed = passed_keys(
        &profile,
        &recipes,
        &declared,
        &revision.project,
        class(&profile),
    )?;
    let mut request = Requester {
        revision: &revision,
        recipes,
        profile: &profile,
        owner,
        context: "",
        requested: BTreeMap::new(),
        retention: None,
        declared,
        passed,
        reused: BTreeMap::new(),
    };
    let id = request_job(store, &mut request, name)?;
    store.action(&id)
}

pub fn submit(
    store: &mut Store,
    revision: &str,
    profile: &str,
    owner: &str,
    prerequisites: Vec<String>,
) -> Result<Candidate> {
    submit_retaining(store, revision, profile, owner, prerequisites, None)
}

/// `retain_from` carries forward passed obligations of a same-revision candidate whose job
/// identity, evidence class, dependency wiring and timeout budget still hold.
pub fn submit_retaining(
    store: &mut Store,
    revision: &str,
    profile: &str,
    owner: &str,
    prerequisites: Vec<String>,
    retain_from: Option<&str>,
) -> Result<Candidate> {
    store.atomic(|store| submit_inner(store, revision, profile, owner, prerequisites, retain_from))
}

fn submit_inner(
    store: &mut Store,
    revision: &str,
    profile: &str,
    owner: &str,
    mut prerequisites: Vec<String>,
    retain_from: Option<&str>,
) -> Result<Candidate> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    let revision_row = store.revision(revision)?;
    let profile_row = store.profile(profile)?;
    verify_runtime(&profile_row)?;
    for prerequisite in &prerequisites {
        store.candidate(prerequisite)?;
    }
    let source = retain_from.map(|id| store.candidate(id)).transpose()?;
    if let Some(source) = &source {
        ensure!(
            source.revision == revision,
            "retain_from must name a candidate of the same revision"
        );
    }
    prerequisites.sort();
    prerequisites.dedup();
    let mut fields = json!({"revision":revision, "profile":profile, "prerequisites":prerequisites});
    // Only when given, so candidates submitted without retention keep their ids.
    if let Some(source) = retain_from {
        fields["retained_from"] = json!(source);
    }
    let id = identity(&fields)?;
    if let Ok(existing) = store.candidate(&id)
        && existing.required.len() == profile_row.definition.jobs.len()
    {
        for action in existing.required.values() {
            store.db.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                                 params![action,owner,id])?;
        }
        return Ok(existing);
    }
    // Candidate and obligations become visible together when the outer transaction commits.
    let mut candidate = Candidate {
        id: id.clone(),
        revision: revision.into(),
        profile: profile.into(),
        required: BTreeMap::new(),
        prerequisites,
        owner: owner.into(),
        created: now(),
        retained_from: retain_from.map(Into::into),
        retained: vec![],
        not_retained: BTreeMap::new(),
        reused: BTreeMap::new(),
    };
    let transaction = store.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO candidates VALUES (?,?)",
        params![id, serde_json::to_string(&candidate)?],
    )?;
    event(&transaction, "candidate.submitted", &id, &candidate)?;
    transaction.commit()?;
    let declared = declarations(store, &revision_row, &profile_row.definition);
    report_declarations(&store.db, &id, &declared)?;
    let recipes = recipes(&profile_row)?;
    let passed = passed_keys(
        &profile_row,
        &recipes,
        &declared,
        &revision_row.project,
        class(&profile_row),
    )?;
    let mut request = Requester {
        revision: &revision_row,
        recipes,
        profile: &profile_row,
        owner,
        context: &id,
        requested: BTreeMap::new(),
        retention: source.map(|source| Retention {
            source,
            retained: vec![],
            not_retained: BTreeMap::new(),
        }),
        declared,
        passed,
        reused: BTreeMap::new(),
    };
    for job in &profile_row.definition.jobs {
        request_job(store, &mut request, &job.name)?;
    }
    candidate.required = request.requested;
    candidate.reused = request.reused;
    if let Some(mut retention) = request.retention {
        retention.retained.sort();
        candidate.retained = retention.retained;
        candidate.not_retained = retention.not_retained;
    }
    store.db.execute(
        "UPDATE candidates SET body=? WHERE id=?",
        params![serde_json::to_string(&candidate)?, id],
    )?;
    Ok(candidate)
}

pub fn action_state(db: &Connection, id: &str) -> Result<String> {
    Ok(db.query_row("SELECT state FROM actions WHERE id=?", [id], |r| r.get(0))?)
}

pub fn candidate_status(store: &Store, id: &str) -> Result<Value> {
    candidate_status_db(&store.db, id, &mut BTreeSet::new(), &mut BTreeMap::new())
}

fn candidate_status_db(
    db: &Connection,
    id: &str,
    visited: &mut BTreeSet<String>,
    memo: &mut BTreeMap<String, Value>,
) -> Result<Value> {
    if let Some(value) = memo.get(id) {
        return Ok(value.clone());
    }
    ensure!(visited.insert(id.into()), "candidate dependency cycle");
    let candidate: Candidate = record(db, "candidates", id)?;
    let profile: RegisteredProfile = record(db, "profiles", &candidate.profile)?;
    let mut prerequisites = BTreeMap::new();
    for prerequisite in &candidate.prerequisites {
        let result = candidate_status_db(db, prerequisite, visited, memo)?;
        prerequisites.insert(prerequisite, result["state"].clone());
    }
    visited.remove(id);
    let mut jobs = BTreeMap::new();
    for (name, action) in &candidate.required {
        let state = action_state(db, action)?;
        let mut job = json!({"action":action,"state":state});
        if let Some(reused) = candidate.reused.get(name) {
            job["reused"] = json!(reused);
        }
        if state == "interrupted"
            && let Some(awaiting) = awaiting_drain(db, action)?
        {
            job["awaiting_drain"] = awaiting;
        }
        jobs.insert(name, job);
    }
    let state = if candidate.required.len() != profile.definition.jobs.len() {
        "incomplete"
    } else if prerequisites.values().any(|v| v != "passed") {
        "blocked"
    } else if jobs.values().all(|v| v["state"] == "passed") {
        "passed"
    } else if jobs
        .values()
        .any(|v| v["state"] == "failed" || v["state"] == "interrupted")
    {
        "failed"
    } else if jobs.values().all(|v| v["state"] == "canceled") {
        "canceled"
    } else {
        "pending"
    };
    let mut value = json!({"id":id,"revision":candidate.revision,"profile":candidate.profile,"state":state,
                    "jobs":jobs,"prerequisites":prerequisites});
    if !candidate.reused.is_empty() {
        value["reused"] = json!(candidate.reused.keys().collect::<Vec<_>>());
    }
    memo.insert(id.into(), value.clone());
    Ok(value)
}

/// The undrained attempt an interrupted action still points at: until its executor reports the
/// drain or an operator confirms it, its capacity stays held and `retry` refuses the job.
fn awaiting_drain(db: &Connection, action: &str) -> Result<Option<Value>> {
    let body: Option<String> = db
        .query_row(
            "SELECT t.body FROM actions a JOIN attempts t ON t.id=a.attempt
             WHERE a.id=? AND t.drained=0",
            [action],
            |r| r.get(0),
        )
        .optional()?;
    let Some(body) = body else {
        return Ok(None);
    };
    let attempt: Attempt = serde_json::from_str(&body)?;
    Ok(Some(json!({"attempt":attempt.id,
        "executor":attempt.executor.as_deref().unwrap_or(authority::LOCAL),
        "lease_expires":attempt.lease_expires})))
}

pub fn eligible(db: &Connection, action: &Action) -> Result<bool> {
    for dependency in &action.dependencies {
        if action_state(db, dependency)? != "passed" {
            return Ok(false);
        }
    }
    requested(db, action)
}

/// Eligible as soon as its running dependencies pass: every dependency passed or running, at
/// least one running, and wanted by a candidate whose prerequisites passed. A train's shard waits
/// like this on its prepare.
pub fn eligible_soon(db: &Connection, action: &Action) -> Result<bool> {
    let mut running = false;
    for dependency in &action.dependencies {
        match action_state(db, dependency)?.as_str() {
            "passed" => {}
            "running" => running = true,
            _ => return Ok(false),
        }
    }
    Ok(running && requested(db, action)?)
}

/// Whether an interest still wants the action: a direct request, or a candidate whose
/// prerequisites passed.
fn requested(db: &Connection, action: &Action) -> Result<bool> {
    let mut stmt = db.prepare("SELECT context FROM interests WHERE action=? AND active=1")?;
    let contexts: Vec<String> = stmt
        .query_map([&action.id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut memo = BTreeMap::new();
    for context in contexts {
        if context.is_empty() {
            return Ok(true);
        }
        let candidate: Candidate = record(db, "candidates", &context)?;
        if candidate.required.is_empty() {
            continue;
        }
        let mut ready = true;
        for prerequisite in &candidate.prerequisites {
            if candidate_status_db(db, prerequisite, &mut BTreeSet::new(), &mut memo)?["state"]
                != "passed"
            {
                ready = false;
                break;
            }
        }
        if ready {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn status(store: &Store) -> Result<Value> {
    let mut stmt = store
        .db
        .prepare("SELECT id FROM candidates ORDER BY rowid")?;
    let ids: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut memo = BTreeMap::new();
    let candidates: Vec<Value> = ids
        .iter()
        .map(|id| candidate_status_db(&store.db, id, &mut BTreeSet::new(), &mut memo))
        .collect::<Result<_>>()?;
    let mut stmt = store
        .db
        .prepare("SELECT id,state,attempt,body FROM actions ORDER BY created")?;
    let actions: Vec<Value> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .map(|r| {
            let (id, state, attempt, body) = r?;
            Ok(json!({"id":id,"state":state,"attempt":attempt,
                      "definition":serde_json::from_str::<Value>(&body)?}))
        })
        .collect::<Result<_>>()?;
    Ok(json!({"capacity":store.capacity()?,"candidates":candidates,"actions":actions}))
}

pub fn retry(store: &mut Store, candidate_id: &str, selected: &[String]) -> Result<Value> {
    retry_owned(store, candidate_id, selected, None)
}

pub fn retry_owned(
    store: &mut Store,
    candidate_id: &str,
    selected: &[String],
    owner: Option<&str>,
) -> Result<Value> {
    let candidate = store.candidate(candidate_id)?;
    ensure!(
        selected
            .iter()
            .all(|name| candidate.required.contains_key(name)),
        "unknown retry job"
    );
    let profile = store.profile(&candidate.profile)?;
    let mut retried = Vec::new();
    let transaction = store.transaction()?;
    for (name, id) in &candidate.required {
        if !selected.is_empty() && !selected.contains(name) {
            continue;
        }
        let state = action_state(&transaction, id)?;
        if !["failed", "interrupted", "canceled"].contains(&state.as_str()) {
            continue;
        }
        let held: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM attempts WHERE action=? AND drained=0",
            [id],
            |r| r.get(0),
        )?;
        ensure!(
            held == 0,
            "attempt still owns capacity; confirmed drain is required"
        );
        let mut action: Action = record(&transaction, "actions", id)?;
        if action.identity.is_some() {
            // The retrying candidate's reservation applies, e.g. an OOM fix on a shared action.
            let requirement = Reservation::from(job(&profile, name)?);
            action.reservation = Some(
                action
                    .reservation
                    .map_or(requirement, |current| current.max(requirement)),
            );
        }
        transaction.execute(
            "UPDATE actions SET state='queued',attempt=NULL,body=? WHERE id=?",
            params![serde_json::to_string(&action)?, id],
        )?;
        // An operator's retry restarts the not-started requeue budget (gate P3-D).
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS requeues (action TEXT PRIMARY KEY, count INTEGER NOT NULL)",
        )?;
        transaction.execute("DELETE FROM requeues WHERE action=?", [id])?;
        transaction.execute(
            "UPDATE interests SET active=1 WHERE action=? AND context=? AND (? IS NULL OR owner=?)",
            params![id, candidate_id, owner, owner],
        )?;
        event(
            &transaction,
            "action.retried",
            id,
            &json!({"candidate":candidate_id}),
        )?;
        retried.push(name);
    }
    transaction.commit()?;
    Ok(json!({"retried":retried,"retained_successes":true}))
}

pub fn cancel(store: &mut Store, action: &str, owner: &str) -> Result<Value> {
    store.action(action)?;
    let transaction = store.transaction()?;
    let changed = transaction.execute(
        "UPDATE interests SET active=0 WHERE action=? AND owner=?",
        params![action, owner],
    )?;
    ensure!(changed > 0, "owner has no interest in this action");
    let active: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
        [action],
        |r| r.get(0),
    )?;
    if active == 0 {
        transaction.execute(
            "UPDATE actions SET state='canceled' WHERE id=? AND state='queued'",
            [action],
        )?;
    }
    event(
        &transaction,
        "interest.detached",
        action,
        &json!({"owner":owner,"remaining":active}),
    )?;
    let running: Option<String> = transaction
        .query_row(
            "SELECT attempt FROM actions WHERE id=? AND state='running'",
            [action],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    // Recorded with the detach, so no new requester joins work that is being stopped.
    if active == 0
        && let Some(attempt) = running
    {
        authority::stopping(&transaction, action, &attempt, None)?;
    }
    transaction.commit()?;
    Ok(json!({"action":action,"active_interests":active}))
}

pub fn review(store: &mut Store, revision: &str, reviewer: &str) -> Result<Value> {
    store.revision(revision)?;
    ensure!(!reviewer.is_empty(), "reviewer must be identified");
    store.db.execute(
        "INSERT OR REPLACE INTO reviews VALUES (?,?,?)",
        params![revision, reviewer, now()],
    )?;
    event(
        &store.db,
        "revision.reviewed",
        revision,
        &json!({"reviewer":reviewer}),
    )?;
    Ok(json!({"revision":revision,"reviewer":reviewer}))
}

pub fn promote(store: &mut Store, candidate_id: &str, expected: Option<&str>) -> Result<Value> {
    let candidate = store.candidate(candidate_id)?;
    let revision = store.revision(&candidate.revision)?;
    let profile = store.profile(&candidate.profile)?;
    let repository_root = source::repository(store, &revision.project);
    ensure!(
        profile.definition.certifying,
        "profile is exploratory, not a certification policy"
    );
    let recipes = recipes(&profile)?;
    let declared = declarations(store, &revision, &profile.definition);
    let passed = passed_keys(
        &profile,
        &recipes,
        &declared,
        &revision.project,
        "certifying",
    )?;
    // Once actions cross profiles, construction no longer guarantees identity; recompute it.
    let mut obligations = BTreeMap::new();
    for name in candidate.required.keys() {
        let definition = job(&profile, name)?;
        // A missing obligation is reported by the status check below, not as miswiring.
        let dependencies: Vec<String> = definition
            .dependencies
            .iter()
            .map(|d| candidate.required.get(d).cloned().unwrap_or_default())
            .collect();
        let inputs = input_identity(store, &revision, definition)?;
        obligations.insert(name.clone(), (definition.clone(), dependencies, inputs));
    }
    let transaction = store.transaction()?;
    ensure!(
        candidate_status_db(
            &transaction,
            candidate_id,
            &mut BTreeSet::new(),
            &mut BTreeMap::new()
        )?["state"]
            == "passed",
        "candidate does not have complete passing evidence"
    );
    let reviews: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM reviews WHERE revision=?",
        [&candidate.revision],
        |r| r.get(0),
    )?;
    ensure!(reviews > 0, "exact composition has no review");
    let previous: Option<String> = transaction
        .query_row(
            "SELECT candidate FROM frontiers WHERE project=?",
            [&revision.project],
            |r| r.get(0),
        )
        .optional()?;
    ensure!(
        previous.as_deref() == expected,
        "validated frontier changed; inspect and compose before promotion"
    );
    if let Some(previous) = &previous {
        let previous_candidate: Candidate = record(&transaction, "candidates", previous)?;
        let previous_revision: Revision =
            record(&transaction, "revisions", &previous_candidate.revision)?;
        ensure!(
            source::git_command(&repository_root)
                .args([
                    "merge-base",
                    "--is-ancestor",
                    &previous_revision.commit,
                    &revision.commit
                ])
                .status()?
                .success(),
            "candidate does not contain the current validated frontier; compose it first"
        );
    }
    let mut receipts = BTreeMap::new();
    for (name, action) in &candidate.required {
        let attempt: String =
            transaction.query_row("SELECT attempt FROM actions WHERE id=?", [action], |r| {
                r.get(0)
            })?;
        let evidence: Attempt = record(&transaction, "attempts", &attempt)?;
        let drained: bool =
            transaction.query_row("SELECT drained FROM attempts WHERE id=?", [&attempt], |r| {
                r.get(0)
            })?;
        ensure!(
            drained && evidence.result.is_some(),
            "missing completed execution evidence"
        );
        let result = evidence.result.clone().unwrap_or_default();
        let row: Action = record(&transaction, "actions", action)?;
        let (definition, dependencies, inputs) = &obligations[name];
        let mismatch = format!(
            "obligation {name} does not match the certifying profile's job identity or wiring"
        );
        // A reused pass consumed another candidate's dependency actions; its passed key
        // deliberately excludes them, so that key is checked instead of the wiring.
        let reused = candidate
            .reused
            .get(name)
            .filter(|reused| reused.action == *action);
        ensure!(
            row.job == *name && (reused.is_some() || row.dependencies == *dependencies),
            "{mismatch}"
        );
        if let Some(identity) = &row.identity {
            let answer = Answer {
                project: &revision.project,
                recipe: &recipes[name],
                class: "certifying",
                timeout_seconds: identity.timeout_seconds,
                inputs,
                passed: passed.get(name).and_then(|key| key.as_deref()),
                dependencies,
            };
            ensure!(
                identity.version == JOB_RECIPE
                    && identity.class == "certifying"
                    && answer.matches(&transaction, &row, reused.is_some())?,
                "{mismatch}"
            );
            if identity.timeout_seconds != definition.timeout_seconds {
                ensure!(
                    result["timer_seconds"]
                        .as_f64()
                        .is_some_and(|t| t < definition.timeout_seconds as f64),
                    "obligation {name} exceeded the certifying timeout budget"
                );
            }
        } else {
            let key = legacy_key(&revision.project, &profile.id, name, inputs, dependencies)?;
            ensure!(
                reused.is_none() && row.profile == profile.id && key == row.key,
                "{mismatch}"
            );
        }
        let (total, failed): (i64, i64) = transaction.query_row(
            "SELECT COUNT(*),COALESCE(SUM(state<>'passed'),0) FROM attempts WHERE action=?",
            [action],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let route = if candidate.retained.contains(name) {
            "retained"
        } else if reused.is_some() {
            "reused"
        } else if row.identity.is_none() {
            "legacy"
        } else if row
            .origin
            .as_ref()
            .is_some_and(|o| o.context == candidate_id)
        {
            "own"
        } else {
            "joined"
        };
        let mut receipt = json!({"action":action,"attempt":attempt,"evidence":evidence.result,
            "executed_under":row.profile,"recipe":row.identity.as_ref().map(|i| &i.recipe),
            "key":row.key,"route":route,"reuse":definition.reuse,
            "observed":{"started":evidence.started,"ended":evidence.ended},
            "attempts":{"total":total,"failed":failed}});
        if [Reuse::Exact, Reuse::Files, Reuse::Declared].contains(&definition.reuse) {
            receipt["audit"] = json!(definition.audit);
        }
        if let Some(reused) = reused {
            receipt["reused"] = json!(reused);
        }
        receipts.insert(name, receipt);
    }
    // `profile` is the certifying policy that accepted these job identities; each receipt
    // names the profile that executed it.
    let certificate = json!({"certificate":2,"candidate":candidate_id,"revision":revision.id,
                            "commit":revision.commit,"tree":revision.tree,"profile":profile.id,
                            "retained_from":candidate.retained_from,"receipts":receipts,"time":now()});
    transaction.execute("INSERT INTO frontiers VALUES (?,?,?) ON CONFLICT(project) DO UPDATE SET candidate=excluded.candidate,certificate=excluded.certificate",
                        params![revision.project, candidate_id, certificate.to_string()])?;
    event(
        &transaction,
        "frontier.advanced",
        candidate_id,
        &certificate,
    )?;
    transaction.commit()?;
    Ok(certificate)
}

pub fn frontier(store: &Store, project: &str) -> Result<Option<Value>> {
    let row: Option<(String, String)> = store
        .db
        .query_row(
            "SELECT candidate,certificate FROM frontiers WHERE project=?",
            [project],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    row.map(|(candidate, certificate)| {
        Ok(json!({"candidate":candidate,
                  "certificate":serde_json::from_str::<Value>(&certificate)?}))
    })
    .transpose()
}

pub fn repairs(store: &Store) -> Result<Vec<Value>> {
    let mut stmt = store.db.prepare(
        "SELECT id,revision,action,kind,state,detail,resolved_by FROM repairs ORDER BY created",
    )?;
    let values = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })?;
    values
        .map(|v| {
            let (id, revision, action, kind, state, detail, resolved_by) = v?;
            Ok(
                json!({"id":id,"revision":revision,"action":action,"kind":kind,"state":state,
                  "detail":serde_json::from_str::<Value>(&detail)?,"resolved_by":resolved_by}),
            )
        })
        .collect()
}

pub fn resolve_repair(store: &mut Store, repair: &str, candidate: &str) -> Result<Value> {
    ensure!(
        candidate_status(store, candidate)?["state"] == "passed",
        "repair candidate has not passed"
    );
    let changed = store.db.execute(
        "UPDATE repairs SET state='resolved',resolved_by=? WHERE id=? AND state='open'",
        params![candidate, repair],
    )?;
    ensure!(changed == 1, "repair is absent or already resolved");
    event(
        &store.db,
        "repair.resolved",
        repair,
        &json!({"candidate":candidate}),
    )?;
    Ok(json!({"repair":repair,"candidate":candidate}))
}

#[cfg(test)]
mod graph_tests {
    use super::*;

    #[test]
    fn shared_dependency_graph_is_evaluated_once_per_node_and_still_rejects_cycles() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE profiles(id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE candidates(id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE actions(id TEXT PRIMARY KEY,state TEXT);
            INSERT INTO actions VALUES('passed-action','passed');",
        )
        .unwrap();
        let profile = json!({"id":"profile","engine":"fixture","platform":"fixture","tools":{},
            "definition":{"version":1,"name":"graph-fixture","tools":{},
                "jobs":[{"name":"check","command":["unused"]}]}});
        db.execute(
            "INSERT INTO profiles VALUES('profile',?)",
            [profile.to_string()],
        )
        .unwrap();
        let mut previous: Vec<String> = vec![];
        let mut final_id = String::new();
        for level in 0..20 {
            let mut next = vec![];
            for branch in 0..2 {
                let id = format!("node-{level}-{branch}");
                let value = json!({"id":id,"revision":"revision","profile":"profile",
                    "required":{"check":"passed-action"},"prerequisites":previous,"owner":"fixture","created":0});
                db.execute(
                    "INSERT INTO candidates VALUES (?,?)",
                    params![id, value.to_string()],
                )
                .unwrap();
                next.push(id.clone());
                final_id = id;
            }
            previous = next;
        }
        let mut memo = BTreeMap::new();
        let result = candidate_status_db(&db, &final_id, &mut BTreeSet::new(), &mut memo).unwrap();
        assert_eq!(result["state"], "passed");
        // One root plus both branches of each preceding level; millions of paths share 39 nodes.
        assert_eq!(memo.len(), 39);
        assert_eq!(result["prerequisites"].as_object().unwrap().len(), 2);
        db.execute("UPDATE candidates SET body=json_set(body,'$.prerequisites',json(?)) WHERE id='node-0-0'",
            [json!([final_id]).to_string()]).unwrap();
        let error = candidate_status_db(&db, &final_id, &mut BTreeSet::new(), &mut BTreeMap::new())
            .unwrap_err();
        assert!(error.to_string().contains("cycle"));
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    fn fixture() -> RegisteredProfile {
        let definition: Profile = serde_json::from_value(json!({
        "version":1,"name":"golden","certifying":true,
        "tools":{"sh":"/usr/bin/sh","node":"/opt/node/bin/node"},
        "environment":{"SHARED":"1"},
        "jobs":[
            {"name":"build","command":["sh","-c","make"],"artifacts":["dist"],"slots":2,
             "memory_mb":512,"timeout_seconds":60,"environment":{"JOB":"x"},
             "report":{"kind":"command"}},
            {"name":"test","dependencies":["build"],"command":["node","--test"],
             "cleanup":["sh","-c","true"],
             "report":{"kind":"node-tap","minimum":3,"allow_skips":true}}
        ]}))
        .unwrap();
        RegisteredProfile {
            id: "unused".into(),
            definition,
            tools: BTreeMap::from([("sh".into(), "a".into()), ("node".into(), "b".into())]),
            engine: "engine-fixture".into(),
            platform: "linux-x86_64".into(),
        }
    }

    #[test]
    fn unchanged_profile_json_keeps_profile_id() {
        // Captured with the registration formula before `export_slots` existed.
        let p = fixture();
        assert_eq!(
            profile_id(&p.definition, &p.tools, &p.engine, &p.platform).unwrap(),
            "8ac9adaf2bf0e293aa5caa8175f75a143073d333b80134e7a7c5e09f824db9be"
        );
    }

    type Edit<'a> = (&'a str, &'a dyn Fn(&mut RegisteredProfile));

    #[test]
    fn recipe_golden_and_field_coverage() {
        let base = fixture();
        let golden = recipes(&base).unwrap();
        assert_eq!(
            golden["build"],
            "eb166cdf1b8593bd3081e75c2ea0c8493411ef0e151642b06155a321c720396c"
        );
        assert_eq!(
            golden["test"],
            "c977b827b3b5f9618fb6a86f4fe683207c699a79e92505d5c84dd6d19293c9ea"
        );
        let changed = |edit: &dyn Fn(&mut RegisteredProfile), job: &str| {
            let mut p = base.clone();
            edit(&mut p);
            recipes(&p).unwrap()[job] != golden[job]
        };
        fn build(p: &mut RegisteredProfile) -> &mut Job {
            &mut p.definition.jobs[0]
        }
        let unchanged: [Edit; 6] = [
            ("memory_mb", &|p| build(p).memory_mb = 4096),
            ("timeout_seconds", &|p| build(p).timeout_seconds = 7),
            ("profile name", &|p| p.definition.name = "renamed".into()),
            ("certifying", &|p| p.definition.certifying = false),
            ("audit", &|p| {
                build(p).audit = "a different justification".into()
            }),
            ("profile id", &|p| p.id = "other".into()),
        ];
        for (field, edit) in unchanged {
            for job in ["build", "test"] {
                assert!(!changed(edit, job), "{field} changed {job}");
            }
        }
        let mut quiet = base.clone();
        build(&mut quiet).export_slots = false;
        let quiet_recipes = recipes(&quiet).unwrap();
        build(&mut quiet).slots = 1;
        assert_eq!(recipes(&quiet).unwrap(), quiet_recipes, "unexported slots");
        let moved = |p: &mut RegisteredProfile| {
            build(p).environment.insert("SHARED".into(), "1".into());
        };
        assert!(!changed(&moved, "build"), "same-value move between levels");
        let changes: [Edit; 19] = [
            ("command", &|p| build(p).command.push("-j2".into())),
            ("cleanup", &|p| build(p).cleanup = vec!["sh".into()]),
            ("unused tool hash", &|p| {
                p.tools.insert("node".into(), "c".into());
            }),
            ("tool path", &|p| {
                p.definition
                    .tools
                    .insert("node".into(), "/usr/bin/node".into());
            }),
            ("profile environment", &|p| {
                p.definition.environment.insert("SHARED".into(), "2".into());
            }),
            ("job environment", &|p| {
                build(p).environment.insert("JOB".into(), "y".into());
            }),
            ("report kind", &|p| build(p).report.kind = "junit".into()),
            ("report path", &|p| {
                build(p).report.path = "report.xml".into()
            }),
            ("report minimum", &|p| build(p).report.minimum = 1),
            ("report skips", &|p| build(p).report.allow_skips = true),
            ("artifacts", &|p| build(p).artifacts.push("out".into())),
            ("reuse", &|p| build(p).reuse = Reuse::Never),
            ("inputs", &|p| build(p).inputs.push("README".into())),
            ("exported slots", &|p| build(p).slots = 1),
            ("export flag", &|p| build(p).export_slots = false),
            ("engine", &|p| p.engine = "other-engine".into()),
            ("platform", &|p| p.platform = "linux-aarch64".into()),
            ("inputs file", &|p| {
                p.definition.inputs_file = Some("ci/inputs.json".into())
            }),
            ("dependency command", &|p| {
                build(p).command.push("-j2".into())
            }),
        ];
        for (field, edit) in changes {
            let job = if field == "dependency command" {
                "test"
            } else {
                "build"
            };
            assert!(changed(edit, job), "{field} did not change {job}");
        }
        // A profile-level field, so every job's identity.
        let mut declared = base.clone();
        declared.definition.inputs_file = Some("ci/inputs.json".into());
        let declared = recipes(&declared).unwrap();
        assert!(golden.iter().all(|(job, recipe)| declared[job] != *recipe));
        // Renaming a job renames its identity; the consumer's dependency map follows it.
        let mut renamed = base.clone();
        build(&mut renamed).name = "compile".into();
        renamed.definition.jobs[1].dependencies = vec!["compile".into()];
        let renamed = recipes(&renamed).unwrap();
        assert_ne!(renamed["compile"], golden["build"]);
        assert_ne!(renamed["test"], golden["test"]);
        assert!(!changed(&|p| p.definition.jobs[1].memory_mb = 1, "build"));
    }

    #[test]
    fn platform_mismatch_refuses_at_prepare() {
        let mut profile = RegisteredProfile {
            id: "fixture".into(),
            definition: serde_json::from_value(json!({"version":1,"name":"p",
                "tools":{},"jobs":[{"name":"j","command":["x"]}]}))
            .unwrap(),
            tools: BTreeMap::new(),
            engine: file_hash(&std::env::current_exe().unwrap()).unwrap(),
            platform: platform(),
        };
        verify_runtime(&profile).unwrap();
        profile.platform = "plan9-mips".into();
        assert!(
            verify_runtime(&profile)
                .unwrap_err()
                .to_string()
                .contains("platform changed")
        );
    }

    struct Lab {
        _temp: tempfile::TempDir,
        store: Store,
        revision: Revision,
        root: std::path::PathBuf,
    }

    impl Lab {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let repo = temp.path().join("repo");
            fs::create_dir(&repo).unwrap();
            source::git(&repo, &["init", "--quiet"]).unwrap();
            fs::write(repo.join("README"), "fixture\n").unwrap();
            source::git(&repo, &["add", "README"]).unwrap();
            source::git(
                &repo,
                &[
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@local.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "fixture",
                ],
            )
            .unwrap();
            let mut store = Store::init(&temp.path().join("state"), 4, 4096).unwrap();
            let revision = source::import(&mut store, "fixture", &repo, "HEAD", "fixture").unwrap();
            Self {
                root: temp.path().into(),
                _temp: temp,
                store,
                revision,
            }
        }
        fn register(&mut self, name: &str, build: &str) -> RegisteredProfile {
            let path = self.root.join(format!("{name}.json"));
            fs::write(
                &path,
                json!({"version":1,"name":name,"certifying":true,"tools":{"sh":"/usr/bin/sh"},
                    "jobs":[{"name":"build","command":["sh","-c",build],"report":{"kind":"command"}},
                            {"name":"test","dependencies":["build"],"command":["sh","-c","true"],
                             "report":{"kind":"command"}}]})
                .to_string(),
            )
            .unwrap();
            register(&mut self.store, &path).unwrap()
        }
        fn pass(&self, action: &str) {
            let attempt = unique();
            let body = json!({"id":attempt,"action":action,"worker_pid":0,"worker_start":"0",
                "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,
                "created":0.0,"started":0.5,"ended":1.0,"exit_code":0,
                "result":{"timer_seconds":0.5}});
            self.store
                .db
                .execute(
                    "INSERT INTO attempts VALUES (?,?,'passed',1,?)",
                    params![attempt, action, body.to_string()],
                )
                .unwrap();
            self.store
                .db
                .execute(
                    "UPDATE actions SET state='passed',attempt=? WHERE id=?",
                    params![attempt, action],
                )
                .unwrap();
        }
        fn point(&self, candidate: &str, job: &str, action: &str) {
            self.store
                .db
                .execute(
                    "UPDATE candidates SET body=json_set(body,?,?) WHERE id=?",
                    params![format!("$.required.{job}"), action, candidate],
                )
                .unwrap();
        }
        fn refused(&mut self, candidate: &str, obligation: &str) {
            let error = promote(&mut self.store, candidate, None)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("obligation {obligation} does not match")),
                "{error}"
            );
        }
    }

    #[test]
    fn promote_refuses_mismatched_or_miswired_receipts() {
        let mut lab = Lab::new();
        let p = lab.register("p", "true");
        let q = lab.register("q", "exit 0");
        let candidate = submit(&mut lab.store, &lab.revision.id, &p.id, "agent", vec![]).unwrap();
        let (build, test) = (&candidate.required["build"], &candidate.required["test"]);
        lab.pass(build);
        lab.pass(test);
        review(&mut lab.store, &lab.revision.id, "reviewer").unwrap();

        // An obligation pointing at an action with a different recipe.
        let other = request(&mut lab.store, &lab.revision.id, &q.id, "build", "q").unwrap();
        lab.pass(&other.id);
        lab.point(&candidate.id, "build", &other.id);
        lab.refused(&candidate.id, "build");

        // A same-identity build that the passed consumer did not consume.
        let rerun = request(&mut lab.store, &lab.revision.id, &p.id, "build", "p").unwrap();
        assert_eq!(rerun.key, lab.store.action(build).unwrap().key);
        assert_ne!(&rerun.id, build);
        lab.pass(&rerun.id);
        lab.point(&candidate.id, "build", &rerun.id);
        lab.refused(&candidate.id, "test");
        lab.point(&candidate.id, "build", build);

        // A legacy action executed under another profile.
        let inputs = json!({"commit":lab.revision.commit});
        let project = lab.revision.project.clone();
        let legacy = |profile: &str, job: &str, deps: &[String]| {
            legacy_key(&project, profile, job, &inputs, deps).unwrap()
        };
        let strip = "UPDATE actions SET key=?1,body=json_set(json_remove(body,'$.identity',
            '$.reservation','$.origin'),'$.key',?1,'$.profile',?2) WHERE id=?3";
        let wrong = legacy(&q.id, "build", &[]);
        lab.store
            .db
            .execute(strip, params![wrong, q.id, build])
            .unwrap();
        lab.refused(&candidate.id, "build");

        // Legacy actions recorded under the candidate's own profile still certify.
        for (job, id, deps) in [
            ("build", build, vec![]),
            ("test", test, vec![build.clone()]),
        ] {
            let key = legacy(&p.id, job, &deps);
            lab.store.db.execute(strip, params![key, p.id, id]).unwrap();
        }
        let certificate = promote(&mut lab.store, &candidate.id, None).unwrap();
        assert_eq!(certificate["certificate"], 2);
        assert_eq!(certificate["receipts"]["test"]["route"], "legacy");
        assert_eq!(certificate["receipts"]["build"]["executed_under"], p.id);
    }
}

#[cfg(test)]
mod declared_tests {
    use super::*;
    use crate::declared::SCHEMA;

    const FILE: &str = "ci/inputs.json";
    const AUDIT: &str = "the declarations file names every read";
    const DECLARATIONS: &str = r#"{"schema":"patchweft.inputs/1","common":["package.json"],
        "jobs":{"prep":{"paths":["package.json"],"why":"prep installs from package.json"},
                "check":{"paths":["src/**"],"why":"check reads src and package.json"},
                "solo":{"paths":["src/**"],"exclude":["src/*.md"],"why":"solo reads src and package.json"},
                "census":{"whole_tree":"lists the whole tree"},
                "ghost":{"paths":["src/**"],"why":"a job this profile does not have"}}}"#;

    struct Lab {
        _temp: tempfile::TempDir,
        store: Store,
        repo: std::path::PathBuf,
        root: std::path::PathBuf,
    }

    fn job(name: &str, reuse: &str, dependencies: &[&str]) -> Value {
        let mut job = json!({"name":name,"command":["sh","-c","true"],"report":{"kind":"command"},
            "dependencies":dependencies});
        if reuse != "inflight" {
            job["reuse"] = json!(reuse);
            job["audit"] = json!(AUDIT);
        }
        job
    }

    impl Lab {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let repo = temp.path().join("repo");
            fs::create_dir(&repo).unwrap();
            source::git(&repo, &["init", "--quiet"]).unwrap();
            let store = Store::init(&temp.path().join("state"), 4, 4096).unwrap();
            let mut lab = Self {
                root: temp.path().into(),
                _temp: temp,
                store,
                repo,
            };
            lab.commit(&[
                ("package.json", Some("{}")),
                ("src/a.ts", Some("a")),
                ("src/notes.md", Some("notes")),
                ("README", Some("readme")),
            ]);
            lab
        }
        /// Writes (Some) or deletes (None) files, commits and imports the commit.
        fn commit(&mut self, files: &[(&str, Option<&str>)]) -> Revision {
            self.commit_with(files, "fixture")
        }
        fn commit_with(&mut self, files: &[(&str, Option<&str>)], message: &str) -> Revision {
            for (name, text) in files {
                let target = self.repo.join(name);
                match text {
                    Some(text) => {
                        fs::create_dir_all(target.parent().unwrap()).unwrap();
                        fs::write(target, text).unwrap();
                    }
                    None => fs::remove_file(target).unwrap(),
                }
            }
            source::git(&self.repo, &["add", "-A"]).unwrap();
            source::git(
                &self.repo,
                &[
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@local.invalid",
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    message,
                ],
            )
            .unwrap();
            source::import(&mut self.store, "fixture", &self.repo, "HEAD", "fixture").unwrap()
        }
        fn register(&mut self, name: &str, jobs: Value) -> RegisteredProfile {
            let path = self.root.join(format!("{name}.json"));
            fs::write(
                &path,
                json!({"version":1,"name":name,"certifying":true,"tools":{"sh":"/usr/bin/sh"},
                    "inputs_file":FILE,"jobs":jobs})
                .to_string(),
            )
            .unwrap();
            register(&mut self.store, &path).unwrap()
        }
        /// `prep` (inflight) feeds `check` (declared); `solo` is declared, `census` whole-tree.
        fn profile(&mut self) -> RegisteredProfile {
            self.register(
                "declared",
                json!([
                    job("prep", "declared", &[]),
                    job("check", "declared", &["prep"]),
                    job("solo", "declared", &[]),
                    job("census", "declared", &[])
                ]),
            )
        }
        fn submit(&mut self, revision: &Revision, profile: &RegisteredProfile) -> Candidate {
            submit(&mut self.store, &revision.id, &profile.id, "agent", vec![]).unwrap()
        }
        fn pass(&self, action: &str) {
            let attempt = unique();
            let body = json!({"id":attempt,"action":action,"worker_pid":0,"worker_start":"0",
                "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,
                "created":0.0,"started":0.5,"ended":1.0,"exit_code":0,
                "result":{"timer_seconds":0.5}});
            self.store
                .db
                .execute(
                    "INSERT INTO attempts VALUES (?,?,'passed',1,?)",
                    params![attempt, action, body.to_string()],
                )
                .unwrap();
            self.set(action, "passed");
            self.store
                .db
                .execute(
                    "UPDATE actions SET attempt=? WHERE id=?",
                    params![attempt, action],
                )
                .unwrap();
        }
        fn pass_all(&self, candidate: &Candidate) {
            for action in candidate.required.values() {
                if action_state(&self.store.db, action).unwrap() != "passed" {
                    self.pass(action);
                }
            }
        }
        fn set(&self, action: &str, state: &str) {
            self.store
                .db
                .execute(
                    "UPDATE actions SET state=? WHERE id=?",
                    params![state, action],
                )
                .unwrap();
        }
        fn events(&self, kind: &str) -> Vec<Value> {
            self.store
                .events(0)
                .unwrap()
                .into_iter()
                .filter(|e| e["kind"] == kind)
                .collect()
        }
        fn actions(&self, job: &str) -> i64 {
            self.store
                .db
                .query_row(
                    "SELECT COUNT(*) FROM actions WHERE json_extract(body,'$.job')=?",
                    [job],
                    |r| r.get(0),
                )
                .unwrap()
        }
    }

    #[test]
    fn declared_jobs_need_an_audit_and_the_profile_an_inputs_file() {
        let profile = |jobs: Value, file: Value| -> Profile {
            serde_json::from_value(json!({"version":1,"name":"p","tools":{"sh":"/usr/bin/sh"},
                "inputs_file":file,"jobs":jobs}))
            .unwrap()
        };
        let ok = profile(
            json!([job("a", "inflight", &[]), job("b", "declared", &["a"])]),
            json!(FILE),
        );
        validate_profile(&ok, (4, 4096)).unwrap();
        let refused = |p: Profile, message: &str| {
            let error = validate_profile(&p, (4, 4096)).unwrap_err().to_string();
            assert!(error.contains(message), "{error}");
        };
        let mut short = job("b", "declared", &[]);
        short["audit"] = json!("too short");
        refused(profile(json!([short]), json!(FILE)), "audit");
        let mut projected = job("b", "declared", &[]);
        projected["inputs"] = json!(["src/a.ts"]);
        refused(profile(json!([projected]), json!(FILE)), "input projection");
        refused(
            profile(json!([job("b", "declared", &[])]), Value::Null),
            "inputs_file",
        );
        refused(
            profile(json!([job("b", "declared", &[])]), json!("../inputs.json")),
            "relative",
        );
        refused(
            profile(json!([job("b", "inflight", &[])]), json!("/inputs.json")),
            "relative",
        );
        validate_profile(
            &profile(json!([job("b", "inflight", &[])]), Value::Null),
            (4, 4096),
        )
        .unwrap();
    }

    #[test]
    fn a_declared_pass_is_reused_across_commits_that_differ_only_outside() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS))]);
        let a = lab.submit(&first, &profile);
        assert!(a.reused.is_empty());
        lab.pass_all(&a);
        let outside = lab.commit(&[("README", Some("changed"))]);
        let b = lab.submit(&outside, &profile);
        // prep is declared, so it is reused too, and check's passed key folds in prep's.
        assert_eq!(b.reused["prep"].action, a.required["prep"]);
        assert_ne!(
            b.required["census"], a.required["census"],
            "census keeps the commit key"
        );
        for job in ["check", "solo"] {
            assert_eq!(b.required[job], a.required[job], "{job}");
            assert_eq!(lab.actions(job), 1, "{job} has no new action");
            assert_eq!(
                b.reused[job],
                Reused {
                    action: a.required[job].clone(),
                    from_candidate: a.id.clone(),
                    from_revision: first.id.clone(),
                }
            );
        }
        assert_eq!(b.reused.len(), 3);
        let events = lab.events("action.reused");
        assert_eq!(events.len(), 3);
        let check = events
            .iter()
            .find(|e| e["subject"] == a.required["check"])
            .unwrap();
        let recipe = recipes(&profile).unwrap()["check"].clone();
        let key = passed_key_of(&lab.store.db, &a.required["check"])
            .unwrap()
            .unwrap();
        assert_eq!(
            check["detail"],
            json!({"candidate":b.id,"owner":"agent","from":a.id,"recipe":recipe,"key":key})
        );
        let status = candidate_status(&lab.store, &b.id).unwrap();
        assert_eq!(status["reused"], json!(["check", "prep", "solo"]));
        assert_eq!(status["jobs"]["check"]["state"], "passed");
        assert_eq!(status["jobs"]["check"]["reused"], json!(b.reused["check"]));
        assert_eq!(status["jobs"]["prep"]["reused"], json!(b.reused["prep"]));
        assert!(
            candidate_status(&lab.store, &a.id)
                .unwrap()
                .get("reused")
                .is_none()
        );

        // Inputs are per job: `solo` excludes the notes `check` reads.
        let notes = lab.commit(&[("src/notes.md", None)]);
        let n = lab.submit(&notes, &profile);
        assert_eq!(n.reused.keys().collect::<Vec<_>>(), ["prep", "solo"]);
        assert_eq!(n.required["solo"], a.required["solo"]);
        assert_ne!(n.required["check"], a.required["check"]);
        let inside = lab.commit(&[("src/a.ts", Some("a2"))]);
        let c = lab.submit(&inside, &profile);
        assert_eq!(
            c.reused.keys().collect::<Vec<_>>(),
            ["prep"],
            "prep reads no source"
        );
        for job in ["check", "solo"] {
            assert_ne!(c.required[job], a.required[job], "{job}");
            assert_ne!(c.required[job], n.required[job], "{job}");
        }
        assert_eq!((lab.actions("check"), lab.actions("solo")), (3, 2));
    }

    #[test]
    fn only_a_passed_action_is_reused() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let mut readme = String::from("readme");
        let mut revision = lab.commit(&[(FILE, Some(DECLARATIONS))]);
        let mut previous = lab.submit(&revision, &profile);
        for state in ["failed", "interrupted", "canceled", "stopping"] {
            lab.set(&previous.required["solo"], state);
            readme.push('!');
            revision = lab.commit(&[("README", Some(&readme))]);
            let next = lab.submit(&revision, &profile);
            assert_ne!(next.required["solo"], previous.required["solo"], "{state}");
            assert!(next.reused.is_empty(), "{state}");
            previous = next;
        }
        assert!(lab.events("action.reused").is_empty());
        lab.pass(&previous.required["solo"]);
        readme.push('!');
        let revision = lab.commit(&[("README", Some(&readme))]);
        let last = lab.submit(&revision, &profile);
        assert_eq!(last.required["solo"], previous.required["solo"]);
    }

    #[test]
    fn absent_or_invalid_declarations_keep_the_commit_key() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let invalid = DECLARATIONS.replacen("\"common\"", "\"extra\":1,\"common\"", 1);
        let mut readme = String::from("readme");
        for (text, reason) in [
            (None, "missing"),
            (Some(invalid.as_str()), "unknown field `extra`"),
        ] {
            let revision = match text {
                Some(text) => lab.commit(&[(FILE, Some(text))]),
                None => lab.commit(&[]),
            };
            let first = lab.submit(&revision, &profile);
            let events = lab.events("inputs.invalid");
            let event = events.last().unwrap();
            assert_eq!(event["detail"]["candidate"], first.id);
            assert!(
                event["detail"]["reason"].as_str().unwrap().contains(reason),
                "{event}"
            );
            let recipes = recipes(&profile).unwrap();
            for job in ["check", "solo"] {
                let action = lab.store.action(&first.required[job]).unwrap();
                let dependencies: Vec<String> = match job {
                    "check" => vec![first.required["prep"].clone()],
                    _ => vec![],
                };
                let today = action_key(
                    &revision.project,
                    &recipes[job],
                    "certifying",
                    1800,
                    &json!({"commit":revision.commit}),
                    &dependencies,
                )
                .unwrap();
                assert_eq!(action.key, today, "{job}");
                assert_eq!(action.declared_inputs, None, "{job}");
                assert_eq!(passed_key_of(&lab.store.db, &action.id).unwrap(), None);
            }
            lab.pass_all(&first);
            readme.push('!');
            let next = lab.commit(&[("README", Some(&readme))]);
            let second = lab.submit(&next, &profile);
            assert!(second.reused.is_empty());
            for job in ["check", "solo"] {
                assert_ne!(second.required[job], first.required[job], "{job}");
            }
        }
        assert!(lab.events("action.reused").is_empty());
    }

    #[test]
    fn unmatched_keys_are_reported_and_declared_inputs_recorded() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let revision = lab.commit(&[(FILE, Some(DECLARATIONS))]);
        let candidate = lab.submit(&revision, &profile);
        let unmatched = lab.events("inputs.unmatched");
        assert_eq!(unmatched.len(), 1);
        assert_eq!(
            unmatched[0]["detail"],
            json!({"candidate":candidate.id,"key":"ghost"})
        );
        assert!(lab.events("inputs.invalid").is_empty());
        let declared = declarations(&lab.store, &revision, &profile.definition);
        let recipes = recipes(&profile).unwrap();
        let passed = passed_keys(
            &profile,
            &recipes,
            &declared,
            &revision.project,
            "certifying",
        )
        .unwrap();
        for job in ["prep", "check", "solo"] {
            let action = lab.store.action(&candidate.required[job]).unwrap();
            let inputs = declared.identity(job).unwrap();
            assert_eq!(inputs["declared"]["schema"], SCHEMA);
            assert_eq!(
                action.declared_inputs, passed[job],
                "{job}: PATCHWEFT_INPUTS is the passed key"
            );
            assert_eq!(
                passed_key_of(&lab.store.db, &action.id).unwrap(),
                passed[job],
                "{job}"
            );
        }
        // check's passed key folds in prep's (pw#11 adversary F1).
        let check = declared.identity("check").unwrap();
        let prep = passed["prep"].clone().unwrap();
        assert_eq!(
            passed["check"],
            Some(
                passed_key(
                    &revision.project,
                    &recipes["check"],
                    "certifying",
                    1800,
                    check,
                    &[prep]
                )
                .unwrap()
            )
        );
        let census = lab.store.action(&candidate.required["census"]).unwrap();
        assert_eq!(census.declared_inputs, None);
        assert_eq!(passed_key_of(&lab.store.db, &census.id).unwrap(), None);
    }

    #[test]
    fn promote_accepts_a_reused_receipt_by_its_passed_key() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS))]);
        let a = lab.submit(&first, &profile);
        lab.pass_all(&a);
        let outside = lab.commit(&[("README", Some("changed"))]);
        let b = lab.submit(&outside, &profile);
        let check = lab.store.action(&b.required["check"]).unwrap();
        // The reused pass consumed the earlier candidate's `prep`, itself reused under its passed key.
        assert_eq!(check.dependencies, [a.required["prep"].clone()]);
        assert_eq!(b.reused["prep"].action, a.required["prep"]);
        lab.pass_all(&b);
        review(&mut lab.store, &outside.id, "reviewer").unwrap();
        let refused = |lab: &mut Lab| {
            let error = promote(&mut lab.store, &b.id, None)
                .unwrap_err()
                .to_string();
            assert!(error.contains("obligation check does not match"), "{error}");
        };
        let key = passed_key_of(&lab.store.db, &check.id).unwrap().unwrap();
        lab.store
            .db
            .execute(
                "UPDATE passed_keys SET key='other' WHERE action=?",
                [&check.id],
            )
            .unwrap();
        refused(&mut lab);
        lab.store
            .db
            .execute(
                "UPDATE passed_keys SET key=? WHERE action=?",
                [&key, &check.id],
            )
            .unwrap();
        let strip = "UPDATE candidates SET body=json_remove(body,'$.reused.check') WHERE id=?";
        lab.store.db.execute(strip, [&b.id]).unwrap();
        refused(&mut lab);
        lab.store
            .db
            .execute(
                "UPDATE candidates SET body=? WHERE id=?",
                params![serde_json::to_string(&b).unwrap(), b.id],
            )
            .unwrap();
        let certificate = promote(&mut lab.store, &b.id, None).unwrap();
        let receipt = &certificate["receipts"]["check"];
        assert_eq!(receipt["route"], "reused");
        assert_eq!(receipt["reused"], json!(b.reused["check"]));
        assert_eq!(receipt["audit"], AUDIT);
        assert_eq!(certificate["receipts"]["solo"]["route"], "reused");
        assert_eq!(certificate["receipts"]["prep"]["route"], "reused");
    }

    #[test]
    fn retention_keeps_a_reused_pass_whose_dependency_actions_differ() {
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS))]);
        let a = lab.submit(&first, &profile);
        lab.pass_all(&a);
        let outside = lab.commit(&[("README", Some("changed"))]);
        let b = lab.submit(&outside, &profile);
        lab.pass_all(&b);
        let retained = submit_retaining(
            &mut lab.store,
            &outside.id,
            &profile.id,
            "agent",
            vec![],
            Some(&b.id),
        )
        .unwrap();
        assert_eq!(retained.retained, ["census", "check", "prep", "solo"]);
        assert!(
            retained.not_retained.is_empty(),
            "{:?}",
            retained.not_retained
        );
        assert_eq!(retained.reused, b.reused);
        review(&mut lab.store, &outside.id, "reviewer").unwrap();
        let certificate = promote(&mut lab.store, &retained.id, None).unwrap();
        assert_eq!(certificate["receipts"]["check"]["route"], "retained");
        assert_eq!(
            certificate["receipts"]["check"]["reused"],
            json!(b.reused["check"])
        );
    }

    #[test]
    fn queued_work_never_joins_across_revisions() {
        // pw#11 gate P2-1: the sharing key keeps {commit}; only the passed key is declared.
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS)), ("src/a.ts", Some("a\n"))]);
        let a = lab.submit(&first, &profile);
        let outside = lab.commit(&[("README", Some("changed"))]);
        let b = lab.submit(&outside, &profile);
        assert_ne!(
            b.required["solo"], a.required["solo"],
            "a queued action joined across revisions"
        );
        assert!(b.reused.is_empty());
        lab.pass_all(&a);
        let later = lab.commit(&[("README", Some("changed again"))]);
        let c = lab.submit(&later, &profile);
        assert_eq!(
            c.reused["solo"].action, a.required["solo"],
            "a pass is reused by its passed key"
        );
        assert_eq!(lab.events("action.reused").len(), c.reused.len());
    }

    #[test]
    fn gitattributes_over_a_matched_path_are_inputs_and_checkouts_ignore_autocrlf() {
        // pw#11 gate P2-2: attributes change the bytes a checkout writes.
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS)), ("src/a.ts", Some("a\n"))]);
        let a = lab.submit(&first, &profile);
        lab.pass_all(&a);
        let elsewhere = lab.commit(&[("docs/.gitattributes", Some("*.md text eol=crlf\n"))]);
        let b = lab.submit(&elsewhere, &profile);
        assert_eq!(
            b.reused["solo"].action, a.required["solo"],
            "attributes on no matched path are not inputs"
        );
        for attributes in [".gitattributes", "src/.gitattributes"] {
            let crlf = lab.commit(&[(attributes, Some("*.ts text eol=crlf\n"))]);
            let c = lab.submit(&crlf, &profile);
            assert!(
                !c.reused.contains_key("solo"),
                "{attributes} changes the checkout's bytes"
            );
            lab.pass_all(&c);
            lab.commit(&[(attributes, None)]);
        }
        // checkout_from clones into a fresh repository, so a source repository's autocrlf never
        // applies; git_command also pins core.autocrlf=false for every checkout path.
        source::git(&lab.repo, &["config", "core.autocrlf", "true"]).unwrap();
        let dest = lab.root.join("checkout");
        source::checkout_from(&lab.repo, &first.commit, &dest).unwrap();
        assert_eq!(
            fs::read(dest.join("src/a.ts")).unwrap(),
            b"a\n",
            "a repository's autocrlf never rewrites a checkout"
        );
    }

    #[test]
    fn a_base_is_only_the_trailer_and_the_job_learns_it() {
        // pw#11 gate P2-3: with no trailer the base would be a guess, so the job keeps the commit key.
        let mut lab = Lab::new();
        let based = r#"{"schema":"patchweft.inputs/1","common":["package.json"],
            "jobs":{"solo":{"paths":["src/**"],"base_paths":["src/**"],"why":"solo diffs src against the base"}}}"#;
        let profile = lab.register("based", json!([job("solo", "declared", &[])]));
        let base = lab.commit(&[
            (FILE, Some(based)),
            ("package.json", Some("{}")),
            ("src/a.ts", Some("a\n")),
        ]);
        let plain = lab.commit(&[("README", Some("x"))]);
        let a = lab.submit(&plain, &profile);
        let action: Action = record(&lab.store.db, "actions", &a.required["solo"]).unwrap();
        assert_eq!(
            (action.declared_inputs, action.declared_base),
            (None, None),
            "no trailer, no base"
        );
        let message = format!("train\n\nPatchweft-Base: {}\n", base.commit);
        let trailed = lab.commit_with(&[("README", Some("y"))], &message);
        let b = lab.submit(&trailed, &profile);
        let action: Action = record(&lab.store.db, "actions", &b.required["solo"]).unwrap();
        assert!(action.declared_inputs.is_some());
        assert_eq!(
            action.declared_base.as_deref(),
            Some(base.commit.as_str()),
            "PATCHWEFT_BASE is the keyed base"
        );
    }

    #[test]
    fn editing_another_jobs_entry_keeps_this_jobs_identity() {
        // pw#11 gate P3-4 (schema rule 7): a job keys on common and its own entry, not the file's bytes.
        let mut lab = Lab::new();
        let profile = lab.profile();
        let first = lab.commit(&[(FILE, Some(DECLARATIONS)), ("src/a.ts", Some("a\n"))]);
        let a = lab.submit(&first, &profile);
        lab.pass_all(&a);
        let others = DECLARATIONS
            .replace(
                "check reads src and package.json",
                "check reads src and package.json, reworded",
            )
            .replace(
                "a job this profile does not have",
                "still a job this profile lacks",
            );
        let revision = lab.commit(&[(FILE, Some(&others))]);
        let b = lab.submit(&revision, &profile);
        assert_eq!(
            b.reused["solo"].action, a.required["solo"],
            "another entry's edit reruns nothing here"
        );
        let own = others.replace(r#""exclude":["src/*.md"]"#, r#""exclude":["src/*.txt"]"#);
        let revision = lab.commit(&[(FILE, Some(&own))]);
        let c = lab.submit(&revision, &profile);
        assert!(
            !c.reused.contains_key("solo"),
            "its own entry's edit reruns it"
        );
    }

    #[test]
    fn audit_floors_count_characters_not_whitespace() {
        // pw#11 gate P3-7: twenty spaces are no audit.
        let mut lab = Lab::new();
        let mut blank = job("solo", "declared", &[]);
        blank["audit"] = json!(" ".repeat(24));
        let path = lab.root.join("blank.json");
        fs::write(
            &path,
            json!({"version":1,"name":"blank","certifying":true,"tools":{"sh":"/usr/bin/sh"},
                "inputs_file":FILE,"jobs":[blank]})
            .to_string(),
        )
        .unwrap();
        assert!(register(&mut lab.store, &path).is_err());
    }

    #[test]
    fn a_dependencys_input_change_reruns_its_declared_dependents() {
        // pw#11 adversary F1: a dependency's output is an input of its dependents, so a dependency's
        // passed key folds into theirs, and a commit-keyed dependency gives them none.
        let mut lab = Lab::new();
        let declarations = r#"{"schema":"patchweft.inputs/1","common":["package.json"],
            "jobs":{"prep":{"paths":["gen.txt"],"why":"prep copies gen.txt into its artifact"},
                    "check":{"paths":["src/**"],"why":"check reads src and prep's artifact"}}}"#;
        let profile = lab.register(
            "deps",
            json!([
                job("prep", "declared", &[]),
                job("check", "declared", &["prep"])
            ]),
        );
        let first = lab.commit(&[
            (FILE, Some(declarations)),
            ("package.json", Some("{}")),
            ("gen.txt", Some("ok")),
            ("src/a.ts", Some("a\n")),
        ]);
        let a = lab.submit(&first, &profile);
        lab.pass_all(&a);
        let outside = lab.commit(&[("README", Some("x"))]);
        let b = lab.submit(&outside, &profile);
        assert_eq!(
            b.reused["check"].action, a.required["check"],
            "nothing check depends on changed"
        );
        let generated = lab.commit(&[("gen.txt", Some("fail"))]);
        let c = lab.submit(&generated, &profile);
        assert!(!c.reused.contains_key("prep"));
        // check's own inputs are unchanged but prep's are not: its PATCHWEFT_INPUTS differs too (pw#11 gate P2).
        let inputs = |id: &str| {
            record::<Action>(&lab.store.db, "actions", id)
                .unwrap()
                .declared_inputs
        };
        assert_ne!(inputs(&c.required["check"]), inputs(&a.required["check"]));
        assert!(inputs(&c.required["check"]).is_some());
        assert!(
            !c.reused.contains_key("check"),
            "prep's inputs changed, so check's passed key changed"
        );
        let inflight = lab.register(
            "inflight-prep",
            json!([
                job("prep", "inflight", &[]),
                job("check", "declared", &["prep"])
            ]),
        );
        let d = lab.submit(&first, &inflight);
        lab.pass_all(&d);
        let later = lab.commit(&[("README", Some("y"))]);
        let e = lab.submit(&later, &inflight);
        assert!(
            !e.reused.contains_key("check"),
            "a commit-keyed dependency gives its dependents no passed key"
        );
        let check: Action = record(&lab.store.db, "actions", &e.required["check"]).unwrap();
        assert_eq!(
            check.declared_inputs, None,
            "no passed key, no PATCHWEFT_INPUTS"
        );
    }
}
