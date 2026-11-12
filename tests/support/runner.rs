//! The runner the integration tests launch: `runner/` built once per test binary, as its own
//! workspace with its own lockfile, the way scripts/build-runner.sh builds it (debug here).
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

pub fn runner() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("runner");
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        // Cargo serializes concurrent builds into one target directory itself.
        let output = Command::new(cargo)
            .args([
                "build",
                "--offline",
                "--locked",
                "--quiet",
                "--manifest-path",
            ])
            .arg(root.join("runner/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target)
            .output()
            .expect("cargo runs");
        assert!(
            output.status.success(),
            "building the runner failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        target.join("debug/patchweft-runner")
    })
}

/// `patchweft --runner RUNNER`: the binary under test, launching the runner under test.
pub fn patchweft() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_patchweft"));
    command.arg("--runner").arg(runner());
    command
}
