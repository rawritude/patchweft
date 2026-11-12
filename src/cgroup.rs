//! A cgroup v2 leaf per attempt (v30 part 3), coordinator side: find the executor unit's
//! delegated cgroup, move into its `coordinator` leaf and enable the controllers in its subtree.
//! The result, a [`Setup`], goes to each runner on its argv (`Setup::arguments`); the runner
//! enters `<unit>/attempt-<id>` and measures it (`runner/src/cgroup.rs`). The layout and the
//! shared types are in `patchweft_control::cgroup`.
//!
//! Measurement only: nothing here sets `memory.max` or any other limit. Nothing here may fail or
//! refuse a job: anything short of a prepared unit is `Unavailable`, and jobs run without one.

pub use patchweft_control::cgroup::{COORDINATOR, Setup, parse_self_cgroup, unavailable};
use std::{
    ffi::CString,
    fs, io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

/// Enabled in the unit's `cgroup.subtree_control`; only `memory` is required.
pub const CONTROLLERS: [&str; 3] = ["memory", "pids", "cpu"];

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
                "{} is not delegated: the unit needs Delegate=memory pids cpu and \
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

#[cfg(test)]
mod tests {
    use super::*;

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
            "+memory +pids +cpu"
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
    /// systemd-run --user --wait --pipe --quiet -p 'Delegate=memory pids cpu' \
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
        // Why the coordinator needs its leaf: no process may join the unit's own cgroup now.
        let refused = fs::write(unit.join("cgroup.procs"), std::process::id().to_string());
        assert_eq!(refused.unwrap_err().raw_os_error(), Some(libc::EBUSY));
    }
}
