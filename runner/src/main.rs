//! `patchweft-runner`: the only program that runs Patchweft jobs. A coordinator launches it as
//! `patchweft-runner _supervise ROOT ATTEMPT` for each attempt it claimed; it reads the attempt's
//! control files, runs the job and writes `control/result.json`. It never opens the store and
//! holds no credentials.
//!
//! A profile binds this binary's sha256 (its `engine`), so it carries no version string, commit
//! or build path: a rebuild of unchanged sources gives the same bytes (`scripts/build-runner.sh`).
mod supervise;

use serde_json::json;
use std::path::Path;

/// The scheduler's names for what the moved code uses, so it reads as it did in `worker.rs`.
mod engine {
    pub use patchweft_control::recipe::*;
    use patchweft_control::{file_hash, model::RegisteredProfile};

    /// Checks the tools, platform and this process's own bytes against the profile. The bytes
    /// are read through `/proc/self/exe`, the file this process executes, whatever its path now
    /// names.
    pub fn verify_runtime(profile: &RegisteredProfile) -> anyhow::Result<()> {
        verify_runtime_for(profile, &file_hash(std::path::Path::new("/proc/self/exe"))?)
    }
}
mod source {
    pub use patchweft_control::git::*;
}
mod api {
    pub use patchweft_control::{MAX_ARTIFACT_BYTES, MAX_ATTEMPT_OUTPUT_BYTES, MAX_MANIFEST_BYTES};
}

fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let result = match args.as_slice() {
        [command, root, attempt] if command == "_supervise" => match attempt.to_str() {
            Some(attempt) => supervise::supervise(Path::new(root), attempt),
            None => Err(anyhow::anyhow!("attempt ids are UTF-8")),
        },
        _ => Err(anyhow::anyhow!(
            "usage: patchweft-runner _supervise ROOT ATTEMPT"
        )),
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
