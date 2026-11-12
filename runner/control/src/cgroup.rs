//! What both sides of an attempt cgroup share: the coordinator (`patchweft`'s
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
//!
//! Also the few file and process helpers both sides use on a cgroup.

use serde_json::{Value, json};
use std::{
    ffi::{CString, OsString},
    fs, io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

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

/// `statfs(2)`'s `f_type` for a cgroup2 filesystem.
pub const CGROUP2_SUPER_MAGIC: i64 = 0x6367_7270;

/// Whether `path` is on a cgroup2 filesystem.
pub fn is_cgroup2(path: &Path) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: a valid C string and a zeroed statfs to write into.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // `f_type`'s width differs between architectures.
    #[allow(clippy::unnecessary_cast)]
    unsafe {
        libc::statfs(path.as_ptr(), &mut stat) == 0 && stat.f_type as i64 == CGROUP2_SUPER_MAGIC
    }
}

/// One write of `text` to a cgroup file: the kernel applies each write whole, or refuses it.
pub fn write(path: &Path, text: &str) -> io::Result<()> {
    fs::write(path, text)
}

/// The pids a `cgroup.procs` file lists; none when it is unreadable.
pub fn pids(procs: &Path) -> Vec<u32> {
    fs::read_to_string(procs)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

/// SIGKILL to each of `pids`: where `cgroup.kill` is missing (kernels before 5.14) or would reach
/// the caller. The caller never passes its own pid.
pub fn kill_each(pids: &[u32]) {
    for pid in pids {
        // SAFETY: kill(2) takes any pid; one that is gone is ESRCH.
        unsafe {
            libc::kill(*pid as i32, libc::SIGKILL);
        }
    }
}

/// Removes a cgroup and any child cgroups a job made in it, deepest first: a cgroup with children
/// cannot be removed. Each `rmdir` is retried while the kernel says EBUSY (killed processes still
/// leaving) for up to `wait`. A tree that is not on cgroup2 (a test's fake, of regular files) is
/// deleted as a directory.
pub fn remove(directory: &Path, wait: Duration) -> io::Result<()> {
    if !is_cgroup2(directory) {
        return fs::remove_dir_all(directory);
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            remove(&entry.path(), wait)?;
        }
    }
    let started = Instant::now();
    loop {
        match fs::remove_dir(directory) {
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) && started.elapsed() < wait => {
                std::thread::sleep(Duration::from_millis(20));
            }
            other => return other,
        }
    }
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
    fn helpers_on_a_fake_tree() {
        let temp = tempfile::tempdir().unwrap();
        let unit = temp.path().join("unit");
        fs::create_dir_all(unit.join("attempt-a/child")).unwrap();
        write(&unit.join("attempt-a/cgroup.procs"), "12\n 34\nx\n").unwrap();
        assert_eq!(pids(&unit.join("attempt-a/cgroup.procs")), [12, 34]);
        assert!(pids(&unit.join("missing")).is_empty());
        assert!(!is_cgroup2(&unit));
        remove(&unit.join("attempt-a"), Duration::ZERO).unwrap();
        assert!(!unit.join("attempt-a").exists());
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
