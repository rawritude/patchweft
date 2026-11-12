use crate::{
    engine, file_hash, source,
    store::{Store, event},
};
// Shared with the runner, which supervises the jobs.
use anyhow::{Context, Result, ensure};
pub use patchweft_control::{
    control_reason, descendants_of, evidence_identity, host, stop_reason, write_atomically,
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

pub fn process_start(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, fields) = text.rsplit_once(')')?;
    let values: Vec<_> = fields.split_whitespace().collect();
    if values.first() == Some(&"Z") {
        return None;
    }
    values.get(19).map(|s| s.to_string())
}

pub fn alive(pid: u32, start: &str) -> bool {
    process_start(pid).as_deref() == Some(start)
}

/// Operator scheduling priority for an owner; 0 (the default) removes it. Priority only orders
/// admission: it never preempts running work or changes action identity.
pub fn set_priority(store: &Store, owner: &str, priority: i64) -> Result<Value> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    store.db.execute_batch(
        "CREATE TABLE IF NOT EXISTS owner_priorities (owner TEXT PRIMARY KEY, priority INTEGER NOT NULL)",
    )?;
    if priority == 0 {
        store
            .db
            .execute("DELETE FROM owner_priorities WHERE owner=?", [owner])?;
    } else {
        store.db.execute(
            "INSERT INTO owner_priorities VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET priority=excluded.priority",
            params![owner, priority],
        )?;
    }
    event(
        &store.db,
        "owner.priority",
        owner,
        &json!({"priority":priority}),
    )?;
    Ok(json!({"owner":owner,"priority":priority}))
}

pub fn deliver(store: &Store, action_id: &str, destination: &Path) -> Result<Value> {
    ensure!(
        engine::action_state(&store.db, action_id)? == "passed",
        "action has no passing result"
    );
    ensure!(
        !destination.exists(),
        "delivery requires a new owned directory; author files are never overwritten"
    );
    let attempt_id: String =
        store
            .db
            .query_row("SELECT attempt FROM actions WHERE id=?", [action_id], |r| {
                r.get(0)
            })?;
    let attempt = store.attempt(&attempt_id)?;
    let evidence = attempt.result.context("missing execution evidence")?;
    let artifacts = evidence["artifacts"]
        .as_object()
        .context("missing artifact manifest")?;
    let parent = destination
        .parent()
        .context("delivery needs a parent directory")?;
    fs::create_dir_all(parent)?;
    let temp = tempfile::tempdir_in(parent)?;
    for (name, expected) in artifacts {
        let relative = source::relative_path(name)?;
        let source = store
            .root
            .join("attempts")
            .join(&attempt_id)
            .join("artifacts")
            .join(&relative);
        ensure!(
            file_hash(&source)?
                == expected["sha256"]
                    .as_str()
                    .context("missing artifact hash")?,
            "retained artifact is corrupt: {name}"
        );
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().context("delivery parent")?)?;
        fs::copy(&source, &target)?;
        fs::set_permissions(
            &target,
            fs::Permissions::from_mode(expected["mode"].as_u64().context("artifact mode")? as u32),
        )?;
    }
    fs::write(
        temp.path().join(".patchweft-receipt.json"),
        serde_json::to_vec_pretty(
            &json!({"action":action_id,"attempt":attempt_id,"evidence":evidence}),
        )?,
    )?;
    publish_directory(temp.path(), destination)?;
    event(
        &store.db,
        "artifact.delivered",
        action_id,
        &json!({"attempt":attempt_id,"destination":destination}),
    )?;
    Ok(json!({"action":action_id,"attempt":attempt_id,"destination":destination}))
}

pub fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes())?;
    let destination = CString::new(destination.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    // Remote clients deliver on macOS; RENAME_EXCL has the same no-overwrite semantics.
    #[cfg(target_os = "macos")]
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
