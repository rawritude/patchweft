//! What the scheduler (`patchweft`) and the runner (`patchweft-runner`) share: the control-file
//! types, job identity, and the few file and process helpers both sides use.
//!
//! Everything here is compiled into the runner, so a change to this crate is a runner release:
//! it changes the runner's bytes, and with them every profile id and recipe. It holds no version
//! string and depends on nothing of the scheduler.
pub mod git;
pub mod model;
pub mod recipe;

use anyhow::{Context, Result};
use model::Action;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{ErrorKind, Write},
    path::Path,
};

/// Per artifact or log file, so a result recorded on one host is deliverable from every host.
pub const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
/// Per attempt: artifacts and logs together.
pub const MAX_ATTEMPT_OUTPUT_BYTES: u64 = 4 * MAX_ARTIFACT_BYTES;
/// A result's artifact manifest, serialized; every host enforces it at capture, so a result that
/// passes locally is deliverable from a remote executor too (review P2-3).
pub const MAX_MANIFEST_BYTES: u64 = 12 * 1024 * 1024;

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn identity(value: &impl Serialize) -> Result<String> {
    Ok(hash(&serde_json::to_vec(value)?))
}

pub fn file_hash(path: &Path) -> Result<String> {
    reader_hash(fs::File::open(path)?)
}

/// The sha256 of everything `reader` yields, such as a file already opened.
pub fn reader_hash(mut file: impl std::io::Read) -> Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Every live process descending from any of `roots`, the roots themselves excluded.
pub fn descendants_of(roots: &[u32]) -> Vec<u32> {
    let mut relationships = Vec::new();
    if let Ok(entries) = fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, fields)) = stat.rsplit_once(')') else {
                continue;
            };
            let Some(ppid) = fields
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u32>().ok())
            else {
                continue;
            };
            relationships.push((pid, ppid));
        }
    }
    let mut found = roots.to_vec();
    loop {
        let old = found.len();
        for (pid, ppid) in &relationships {
            if found.contains(ppid) && !found.contains(pid) {
                found.push(*pid);
            }
        }
        if found.len() == old {
            break;
        }
    }
    found
        .into_iter()
        .filter(|pid| !roots.contains(pid))
        .collect()
}

/// Readers of a control file never see it half written.
pub fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().context("control directory")?)?;
    temp.write_all(bytes)?;
    temp.persist(path)?;
    Ok(())
}

/// The reason in `control/stop`, once the coordinator wrote one.
pub fn stop_reason(control: &Path) -> Result<Option<String>> {
    control_reason(&control.join("stop"))
}

/// The reason in a `stop` or `stopping` control file, once one was written.
pub fn control_reason(path: &Path) -> Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let stop: Value = serde_json::from_slice(&bytes)?;
    Ok(Some(
        stop["reason"]
            .as_str()
            .context("control file lacks a reason")?
            .to_owned(),
    ))
}

pub fn evidence_identity(action: &Action) -> Value {
    action.identity.as_ref().map_or(
        Value::Null,
        |i| json!({"version":i.version,"recipe":i.recipe,"class":i.class,"key":action.key}),
    )
}

pub fn host() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|name| name.trim().to_owned())
}
