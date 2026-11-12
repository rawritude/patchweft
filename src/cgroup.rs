//! A cgroup v2 leaf per attempt (v30 part 3), coordinator side: find the executor unit's
//! delegated cgroup, move into its `coordinator` leaf and enable the controllers in its subtree.
//! The result, a [`Setup`], goes to each runner on its argv (`Setup::arguments`); the runner
//! enters `<unit>/attempt-<id>` and measures it (`runner/src/cgroup.rs`). The layout and the
//! shared types are in `patchweft_control::cgroup`.
//!
//! Measurement only: nothing here sets `memory.max` or any other limit. Nothing here may fail or
//! refuse a job: anything short of a prepared unit is `Unavailable`, and jobs run without one.

pub use patchweft_control::cgroup::{COORDINATOR, Setup, parse_self_cgroup, unavailable};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    ffi::CString,
    fs, io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Enabled in the unit's `cgroup.subtree_control`; only `memory` is required. Never `cpu`: it would
/// share CPU between attempts by `cpu.weight`, changing their scheduling, where the promise is to
/// measure only. `cpu.stat` (`usage_usec`) and `cpu.pressure` exist on every cgroup without it.
pub const CONTROLLERS: [&str; 2] = ["memory", "pids"];

/// `--attempt-cgroups`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Auto,
    Off,
}

/// Whether `path` carries systemd's delegation mark, `user.delegate` or `trusted.delegate` set to
/// `1`. Not every manager sets it: systemd 255's user manager sets none on its delegated units, so
/// there the proof is `DelegateSubgroup=` (see `Host::prepare_coordinator`). Write access proves
/// nothing, since a user manager's units are owned by its user whether delegated or not, and
/// without delegation systemd may rewrite the unit's `subtree_control` behind the coordinator.
pub fn delegated_xattr(path: &Path) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    ["user.delegate", "trusted.delegate"].iter().any(|name| {
        let name = CString::new(*name).expect("static name");
        let mut value = [0u8; 8];
        let length = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        length > 0 && value[..length as usize].starts_with(b"1")
    })
}

/// Where a coordinator looks. `Host::system()` is the real host; tests point it at a directory.
pub struct Host {
    /// The cgroup2 mount, `/sys/fs/cgroup`.
    pub root: PathBuf,
    /// `/proc/self/cgroup`.
    pub self_cgroup: PathBuf,
    pub pid: u32,
    pub delegated: fn(&Path) -> bool,
}

impl Host {
    pub fn system() -> Self {
        Self {
            root: PathBuf::from("/sys/fs/cgroup"),
            self_cgroup: PathBuf::from("/proc/self/cgroup"),
            pid: std::process::id(),
            delegated: delegated_xattr,
        }
    }

    /// Executor side, once at start: if this process sits in its unit's delegated cgroup, move it
    /// into the unit's `coordinator` leaf, then enable the controllers in the unit's subtree.
    /// Anything short of that is `Unavailable`, never an error.
    ///
    /// Delegated means one of: this process started in `<unit>/coordinator`, where systemd puts
    /// it only for a unit with `Delegate=` and `DelegateSubgroup=coordinator` (the production
    /// drop-in); or the unit's cgroup carries the delegation xattr (`delegated`).
    pub fn prepare_coordinator(&self, mode: Mode) -> Setup {
        if mode == Mode::Off {
            return Setup::Unavailable("off".into());
        }
        match self.prepare() {
            Ok(unit) => Setup::Available(unit),
            Err(reason) => Setup::Unavailable(reason),
        }
    }

    fn prepare(&self) -> Result<PathBuf, String> {
        let text = fs::read_to_string(&self.self_cgroup)
            .map_err(|e| format!("read {}: {e}", self.self_cgroup.display()))?;
        let own = parse_self_cgroup(&text).ok_or("no cgroup v2 entry in /proc/self/cgroup")?;
        let own = self.root.join(own.trim_start_matches('/'));
        let in_leaf = own.file_name().is_some_and(|name| name == COORDINATOR);
        let unit = if in_leaf {
            own.parent()
                .ok_or("coordinator leaf without a parent")?
                .to_path_buf()
        } else {
            own.clone()
        };
        if !in_leaf && !(self.delegated)(&unit) {
            return Err(format!(
                "{} is not delegated: the unit needs Delegate=memory pids and \
                 DelegateSubgroup={COORDINATOR}",
                unit.display()
            ));
        }
        let leaf = unit.join(COORDINATOR);
        if !in_leaf {
            match fs::create_dir(&leaf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(format!("create {}: {e}", leaf.display())),
            }
            write(&leaf.join("cgroup.procs"), &self.pid.to_string())
                .map_err(|e| format!("move into {}: {e}", leaf.display()))?;
        }
        let enabled = self.enable(&unit);
        if let Err(reason) = enabled {
            // Back where systemd put it, so the unit is as it was.
            if !in_leaf {
                let _ = write(&own.join("cgroup.procs"), &self.pid.to_string());
            }
            return Err(reason);
        }
        Ok(unit)
    }

    fn enable(&self, unit: &Path) -> Result<(), String> {
        let control = unit.join("cgroup.subtree_control");
        let all: Vec<String> = CONTROLLERS.iter().map(|c| format!("+{c}")).collect();
        let mut errors = Vec::new();
        // The kernel applies all or none of one write; one at a time finds which are missing.
        if write(&control, &all.join(" ")).is_err() {
            for controller in &all {
                if let Err(e) = write(&control, controller) {
                    errors.push(format!("{controller}: {e}"));
                }
            }
        }
        let now = fs::read_to_string(&control).unwrap_or_default();
        let memory = now
            .split_whitespace()
            .any(|c| c.trim_start_matches('+') == "memory");
        if memory {
            Ok(())
        } else {
            Err(format!(
                "memory not enabled in {}: {}",
                control.display(),
                errors.join("; ")
            ))
        }
    }
}

/// `Host::system().prepare_coordinator(mode)`.
pub fn prepare_coordinator(mode: Mode) -> Setup {
    Host::system().prepare_coordinator(mode)
}

fn write(path: &Path, text: &str) -> io::Result<()> {
    fs::write(path, text)
}

const CGROUP2_SUPER_MAGIC: i64 = 0x6367_7270;

/// Whether `path` is on a cgroup2 filesystem.
fn is_cgroup2(path: &Path) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // `f_type`'s width differs between architectures.
    #[allow(clippy::unnecessary_cast)]
    unsafe {
        libc::statfs(path.as_ptr(), &mut stat) == 0 && stat.f_type as i64 == CGROUP2_SUPER_MAGIC
    }
}

fn pids(path: &Path) -> Vec<u32> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

/// A cgroup and any child cgroups a job made in it, deepest first, waiting up to `wait` for
/// killed processes to leave. A test's fake tree holds regular files and goes at once.
fn remove_cgroup(directory: &Path, wait: Duration) -> io::Result<()> {
    if !is_cgroup2(directory) {
        return fs::remove_dir_all(directory);
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            remove_cgroup(&entry.path(), wait)?;
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

/// Coordinator side, at start, once its ledger is taken up and before it launches anything:
/// each `<unit>/attempt-<id>` whose attempt is not in `live` (the attempts whose supervisors are
/// alive) is left from a supervisor that died outright. Its processes, orphans of that job, are
/// killed with `cgroup.kill` (this process is never in it: one holding `pid` is skipped) and the
/// cgroup is removed. One JSON object per cgroup swept, for an `attempt_cgroup_swept` event.
pub fn sweep_stale(unit: &Path, live: &BTreeSet<String>, pid: u32, wait: Duration) -> Vec<Value> {
    let Ok(entries) = fs::read_dir(unit) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    names.sort();
    let mut swept = Vec::new();
    for name in names {
        let Some(attempt) = name.strip_prefix("attempt-") else {
            continue;
        };
        if live.contains(attempt) {
            continue;
        }
        let directory = unit.join(&name);
        let held = pids(&directory.join("cgroup.procs"));
        let mut errors = Vec::new();
        if held.contains(&pid) {
            errors.push(format!("{} holds this coordinator", directory.display()));
        } else {
            if !held.is_empty()
                && let Err(e) = write(&directory.join("cgroup.kill"), "1")
            {
                // Kernels before 5.14 have no cgroup.kill: the pids just read, one by one.
                errors.push(format!("cgroup.kill: {e}; killed by pid"));
                for pid in &held {
                    unsafe {
                        libc::kill(*pid as i32, libc::SIGKILL);
                    }
                }
            }
            if let Err(e) = remove_cgroup(&directory, wait) {
                errors.push(format!("remove {}: {e}", directory.display()));
            }
        }
        swept.push(
            json!({"attempt":attempt,"cgroup":directory,"pids":held.len(),
                          "errors":errors}),
        );
    }
    swept
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn host(temp: &tempfile::TempDir, own: &str, delegated: fn(&Path) -> bool) -> Host {
        let self_cgroup = temp.path().join("self-cgroup");
        fs::write(&self_cgroup, format!("0::{own}\n")).unwrap();
        Host {
            root: temp.path().into(),
            self_cgroup,
            pid: 4242,
            delegated,
        }
    }

    #[test]
    fn coordinator_moves_to_its_leaf_and_enables_the_controllers() {
        let temp = tempfile::tempdir().unwrap();
        let own = "/user.slice/user@1001.service/app.slice/pw.service";
        let unit = temp.path().join(own.trim_start_matches('/'));
        fs::create_dir_all(&unit).unwrap();
        let host = host(&temp, own, |_| true);
        assert_eq!(
            host.prepare_coordinator(Mode::Auto),
            Setup::Available(unit.clone())
        );
        assert_eq!(
            fs::read_to_string(unit.join("coordinator/cgroup.procs")).unwrap(),
            "4242"
        );
        assert_eq!(
            fs::read_to_string(unit.join("cgroup.subtree_control")).unwrap(),
            "+memory +pids"
        );
        // Started by systemd in the leaf (DelegateSubgroup=coordinator), which only a delegated
        // unit gets: no delegation mark needed, as systemd 255's user manager sets none.
        fs::remove_file(unit.join("coordinator/cgroup.procs")).unwrap();
        let host = Host {
            self_cgroup: {
                let path = temp.path().join("leaf-cgroup");
                fs::write(&path, format!("0::{own}/coordinator\n")).unwrap();
                path
            },
            delegated: |_| false,
            ..host
        };
        assert_eq!(
            host.prepare_coordinator(Mode::Auto),
            Setup::Available(unit.clone())
        );
        assert!(
            !unit.join("coordinator/cgroup.procs").exists(),
            "not moved again"
        );
    }

    #[test]
    fn coordinator_unavailable_paths_name_why() {
        let temp = tempfile::tempdir().unwrap();
        let own = "/user.slice/pw.service";
        fs::create_dir_all(temp.path().join("user.slice/pw.service")).unwrap();
        assert_eq!(
            host(&temp, own, |_| true).prepare_coordinator(Mode::Off),
            Setup::Unavailable("off".into())
        );
        let Setup::Unavailable(why) = host(&temp, own, |_| false).prepare_coordinator(Mode::Auto)
        else {
            panic!("an undelegated unit is unavailable");
        };
        assert!(why.contains("not delegated"), "{why}");
        assert!(
            !temp
                .path()
                .join("user.slice/pw.service/coordinator")
                .exists()
        );
        // The unit vanished (or its subtree_control refused memory): moved back, unavailable.
        let host = host(&temp, "/gone.service", |_| true);
        assert!(matches!(
            host.prepare_coordinator(Mode::Auto),
            Setup::Unavailable(_)
        ));
        let host = Host {
            self_cgroup: temp.path().join("missing"),
            ..host
        };
        assert!(matches!(
            host.prepare_coordinator(Mode::Auto),
            Setup::Unavailable(_)
        ));
    }

    #[test]
    fn stale_attempt_cgroups_are_swept_and_live_ones_never_touched() {
        let temp = tempfile::tempdir().unwrap();
        let unit = temp.path().join("pw.service");
        let cgroup = |name: &str, procs: &str| {
            let directory = unit.join(name);
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("cgroup.procs"), procs).unwrap();
            directory
        };
        cgroup(COORDINATOR, "4242\n");
        let live = cgroup("attempt-live", "111\n");
        // A supervisor killed outright: its job's orphans are still there.
        let stale = cgroup("attempt-stale", "999999001\n999999002\n");
        let empty = cgroup("attempt-empty", "");
        let mine = cgroup("attempt-mine", "4242\n");
        let other = cgroup("other", "");
        let swept = sweep_stale(
            &unit,
            &BTreeSet::from(["live".to_string()]),
            4242,
            Duration::ZERO,
        );
        let by: BTreeMap<String, Value> = swept
            .into_iter()
            .map(|s| (s["attempt"].as_str().unwrap().to_owned(), s))
            .collect();
        assert_eq!(
            by.keys().map(String::as_str).collect::<Vec<_>>(),
            ["empty", "mine", "stale"]
        );
        assert_eq!(by["stale"]["pids"], 2);
        assert_eq!(by["stale"]["errors"], json!([]));
        assert_eq!(by["empty"]["pids"], 0);
        assert!(!stale.exists() && !empty.exists());
        // Never a live attempt's cgroup, never one holding this coordinator.
        assert!(live.exists() && !live.join("cgroup.kill").exists());
        assert!(mine.exists() && !mine.join("cgroup.kill").exists());
        assert!(
            by["mine"]["errors"][0]
                .as_str()
                .unwrap()
                .contains("holds this coordinator")
        );
        assert!(other.exists() && unit.join(COORDINATOR).exists());
        // No unit, nothing to sweep.
        assert!(
            sweep_stale(
                &temp.path().join("gone"),
                &BTreeSet::new(),
                1,
                Duration::ZERO
            )
            .is_empty()
        );
    }

    #[test]
    fn coordinator_without_memory_moves_back_and_is_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let own = "/pw.service";
        let unit = temp.path().join("pw.service");
        fs::create_dir_all(&unit).unwrap();
        // A subtree_control that cannot be written: a directory in its place.
        fs::create_dir(unit.join("cgroup.subtree_control")).unwrap();
        let Setup::Unavailable(why) = host(&temp, own, |_| true).prepare_coordinator(Mode::Auto)
        else {
            panic!("no memory controller is unavailable");
        };
        assert!(why.contains("memory not enabled"), "{why}");
        assert_eq!(
            fs::read_to_string(unit.join("cgroup.procs")).unwrap(),
            "4242"
        );
    }

    /// On a real delegated cgroup, as the executor's unit runs (the user unit's drop-in), and
    /// alone in it. Changes no unit: a transient user service that ends with the test. The
    /// attempt side has its own, in the runner (`runner/src/cgroup.rs`).
    ///
    /// ```text
    /// systemd-run --user --wait --pipe --quiet -p 'Delegate=memory pids' \
    ///   -p DelegateSubgroup=coordinator -- target/debug/deps/patchweft-<hash> \
    ///   --ignored --exact cgroup::tests::on_a_real_delegated_cgroup
    /// ```
    #[test]
    #[ignore = "needs a delegated cgroup: run inside a transient user service (see the doc)"]
    fn on_a_real_delegated_cgroup() {
        let own = || {
            parse_self_cgroup(&fs::read_to_string("/proc/self/cgroup").unwrap())
                .unwrap()
                .to_owned()
        };
        let Setup::Available(unit) = prepare_coordinator(Mode::Auto) else {
            panic!("not delegated: {}", own());
        };
        assert!(own().ends_with("/coordinator"), "{}", own());
        assert!(
            fs::read_to_string(unit.join("cgroup.subtree_control"))
                .unwrap()
                .contains("memory")
        );
        let controllers = fs::read_to_string(unit.join("cgroup.subtree_control")).unwrap();
        assert!(
            !controllers.split_whitespace().any(|c| c == "cpu"),
            "{controllers}"
        );
        // Why the coordinator needs its leaf: no process may join the unit's own cgroup now.
        let refused = fs::write(unit.join("cgroup.procs"), std::process::id().to_string());
        assert_eq!(refused.unwrap_err().raw_os_error(), Some(libc::EBUSY));
        // A supervisor killed outright left its attempt's cgroup with an orphan in it.
        let stale = unit.join("attempt-stale");
        fs::create_dir(&stale).unwrap();
        let mut orphan = std::process::Command::new("/usr/bin/sleep")
            .arg("300")
            .spawn()
            .unwrap();
        fs::write(stale.join("cgroup.procs"), orphan.id().to_string()).unwrap();
        let live = unit.join("attempt-live");
        fs::create_dir(&live).unwrap();
        let swept = sweep_stale(
            &unit,
            &BTreeSet::from(["live".to_string()]),
            std::process::id(),
            Duration::from_secs(2),
        );
        assert_eq!(
            swept,
            [json!({"attempt":"stale","cgroup":stale,"pids":1,"errors":[]})]
        );
        assert!(!orphan.wait().unwrap().success(), "killed");
        assert!(!stale.exists() && live.exists());
        fs::remove_dir(&live).unwrap();
    }
}
