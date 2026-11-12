//! What both sides of an attempt cgroup share (v30 part 3): the coordinator (`patchweft`'s
//! `cgroup` module) finds and prepares the executor unit's delegated cgroup, and hands each
//! runner it launches a [`Setup`] on its argv; the runner (`patchweft-runner`'s `cgroup` module)
//! enters `<unit>/attempt-<id>` and measures it.
//!
//! Under the executor unit's delegated cgroup `<unit>`:
//! - `<unit>/coordinator` holds the coordinator: cgroup v2 allows no process in a node whose
//!   `cgroup.subtree_control` enables a controller;
//! - `<unit>/attempt-<id>` holds one runner and everything its job starts, from just before the
//!   job's preparation until the runner's result is written.
//!
//! Measurement only: nothing sets `memory.max` or any other limit, and no failure may fail or
//! refuse a job: every path that cannot measure ends in `{"unavailable": why}`.

use serde_json::{Value, json};
use std::{ffi::OsString, path::PathBuf};

/// The coordinator's leaf under the unit cgroup. With `DelegateSubgroup=coordinator` systemd starts
/// the coordinator there itself, which a restart beside running attempts needs: once the unit's
/// subtree enables a controller, no process can join the unit's own cgroup, so systemd could not
/// start a new main process there.
pub const COORDINATOR: &str = "coordinator";

/// The runner's argument naming the delegated unit cgroup.
pub const CGROUP_ARG: &str = "--cgroup";
/// The runner's argument saying why the attempt has no cgroup.
pub const UNAVAILABLE_ARG: &str = "--cgroup-unavailable";

/// What a coordinator found, and what it hands each runner it launches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Setup {
    /// The delegated unit cgroup, controllers enabled in its subtree.
    Available(PathBuf),
    Unavailable(String),
}

impl Setup {
    /// The runner's argv for it: `--cgroup UNIT` or `--cgroup-unavailable WHY`.
    pub fn arguments(&self) -> [OsString; 2] {
        match self {
            Self::Available(unit) => [CGROUP_ARG.into(), unit.into()],
            Self::Unavailable(why) => [UNAVAILABLE_ARG.into(), why.into()],
        }
    }
}

/// The evidence of an attempt that was not measured.
pub fn unavailable(reason: &str) -> Value {
    json!({ "unavailable": reason })
}

/// The cgroup v2 path in `/proc/<pid>/cgroup` text: its `0::` line.
pub fn parse_self_cgroup(text: &str) -> Option<&str> {
    text.lines().find_map(|line| line.strip_prefix("0::"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_v2_line_of_proc_self_cgroup() {
        let text = "1:name=systemd:/legacy\n0::/user.slice/user-1001.slice/user@1001.service/\
            app.slice/patchweft-executor.service\n";
        assert_eq!(
            parse_self_cgroup(text),
            Some(
                "/user.slice/user-1001.slice/user@1001.service/app.slice/patchweft-executor.service"
            )
        );
        assert_eq!(parse_self_cgroup("1:cpu:/x\n"), None);
    }

    #[test]
    fn a_setup_is_two_runner_arguments() {
        assert_eq!(
            Setup::Available("/sys/fs/cgroup/pw.service".into()).arguments(),
            [
                OsString::from("--cgroup"),
                "/sys/fs/cgroup/pw.service".into()
            ]
        );
        assert_eq!(
            Setup::Unavailable("off".into()).arguments(),
            [OsString::from("--cgroup-unavailable"), "off".into()]
        );
    }
}
