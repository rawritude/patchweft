//! `patchweft-runner`: the only program that runs Patchweft jobs. A coordinator launches it as
//! `patchweft-runner _supervise ROOT ATTEMPT (--cgroup UNIT | --cgroup-unavailable WHY)` for each
//! attempt it claimed; it reads the attempt's control files, runs the job (in its own cgroup
//! under UNIT when it has one) and writes `control/result.json`. It never opens the store and
//! holds no credentials.
//!
//! A profile binds this binary's sha256 (its `engine`), so it carries no version string, commit
//! or build path: a rebuild of unchanged sources gives the same bytes (`scripts/build-runner.sh`).
mod cgroup;
mod supervise;

use patchweft_control::cgroup::{CGROUP_ARG, Setup, UNAVAILABLE_ARG};
use serde_json::json;
use std::{ffi::OsString, path::Path};

fn usage() -> anyhow::Error {
    anyhow::anyhow!(
        "usage: patchweft-runner _supervise ROOT ATTEMPT \
         [{CGROUP_ARG} UNIT | {UNAVAILABLE_ARG} WHY]"
    )
}

/// The coordinator's cgroup arguments: the delegated unit cgroup the attempt's own goes under, or
/// why it has none. A coordinator that predates them passes neither.
fn setup(arguments: &[OsString]) -> anyhow::Result<Setup> {
    match arguments {
        [] => Ok(Setup::Unavailable("not passed by the coordinator".into())),
        [flag, unit] if flag == CGROUP_ARG => Ok(Setup::Available(unit.into())),
        [flag, why] if flag == UNAVAILABLE_ARG => match why.to_str() {
            Some(why) => Ok(Setup::Unavailable(why.into())),
            None => Err(anyhow::anyhow!("{UNAVAILABLE_ARG} is UTF-8")),
        },
        _ => Err(usage()),
    }
}

fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let result = match args.as_slice() {
        [command, root, attempt, cgroup @ ..] if command == "_supervise" => {
            match (attempt.to_str(), setup(cgroup)) {
                (Some(attempt), Ok(setup)) => {
                    supervise::supervise(Path::new(root), attempt, &setup)
                }
                (None, _) => Err(anyhow::anyhow!("attempt ids are UTF-8")),
                (_, Err(error)) => Err(error),
            }
        }
        _ => Err(usage()),
    };
    match result {
        Ok(result) => println!(
            "{}",
            serde_json::to_string_pretty(&result).expect("JSON output")
        ),
        Err(error) => {
            eprintln!("{}", json!({"error":format!("{error:#}")}));
            std::process::exit(1);
        }
    }
}
