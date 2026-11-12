//! A job's identity: its recipe, and the runtime a profile was registered with. The scheduler keys
//! actions on recipes; the runner recomputes the recipe before it runs a job and checks its own
//! runtime against the profile.
use crate::{file_hash, identity, model::*};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::collections::BTreeMap;

pub fn tool_identity(profile: &Profile) -> Result<BTreeMap<String, String>> {
    profile
        .tools
        .iter()
        .map(|(name, path)| Ok((name.clone(), file_hash(path)?)))
        .collect()
}

pub fn platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
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
                // Evidence metadata (`containers_unmeasured`), never seen by the job.
                uses_containers: _,
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

pub fn job<'a>(profile: &'a RegisteredProfile, name: &str) -> Result<&'a Job> {
    profile
        .definition
        .jobs
        .iter()
        .find(|j| j.name == name)
        .context("unknown job")
}
