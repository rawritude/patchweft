//! Attempt-store retention: an operator setting, off by default, under which the authority
//! deletes the logs and retained artifacts of old attempts nothing can use any more. The attempt
//! row stays, with its evidence, so history, durations and certificates keep working.
//!
//! An attempt's outputs go only when every keep-rule (`consider`) lets them: it drained, ended
//! more than N days ago, and is not
//! - the passing attempt behind a passed key used within N days (an older key retires with it);
//! - an artifact source of a queued, running or stopping action;
//! - an attempt of an action a candidate still open requires;
//! - an attempt of an action the validated frontier requires, or one its certificate names;
//! - the passing attempt of an `exact` or `files` job, which a request shares by its action key.

use crate::{
    authority::LOCAL,
    model::{Candidate, RegisteredProfile, Reuse},
    store::{Store, event, prepare_passed_keys, record},
    unique,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::ErrorKind,
    path::Path,
};

/// The shortest retention an operator may set: job logs are read up to three weeks back.
pub const MIN_DAYS: u32 = 30;
const DAY: f64 = 86400.0;
/// The real deletion runs at most this often.
const INTERVAL: f64 = 3600.0;
/// At most this many attempt ids ride on one claim response.
const RETIRE_BATCH: i64 = 256;
/// Each id is sent this many times, then dropped: there is no acknowledgement, and an executor
/// that predates the field never applies it.
const RETIRE_SENDS: i64 = 3;
/// Where the authority renames an attempt's directory before deleting it.
const ASIDE: &str = ".retention-";

/// Additive and idempotent. Keys recorded before uses were (or by a v29 binary since) count as
/// last used when they were created, or at their latest recorded reuse when that is later.
pub fn prepare(db: &Connection) -> Result<()> {
    prepare_passed_keys(db)?;
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS retention_deleted (attempt TEXT PRIMARY KEY,
          executor TEXT NOT NULL, deleted REAL NOT NULL, days INTEGER NOT NULL, removed REAL);
         CREATE INDEX IF NOT EXISTS retention_unremoved ON retention_deleted(attempt)
          WHERE removed IS NULL;
         CREATE TABLE IF NOT EXISTS retention_retire (attempt TEXT PRIMARY KEY,
          executor TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0);
         CREATE INDEX IF NOT EXISTS retention_retire_executor ON retention_retire(executor, sent);",
    )?;
    let missing: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM passed_keys p
          WHERE NOT EXISTS (SELECT 1 FROM passed_key_uses u WHERE u.action=p.action))",
        [],
        |r| r.get(0),
    )?;
    if missing {
        db.execute(
            "INSERT OR IGNORE INTO passed_key_uses
             SELECT p.action, MAX(COALESCE(a.created,0), COALESCE(e.time,0))
             FROM passed_keys p LEFT JOIN actions a ON a.id=p.action
             LEFT JOIN (SELECT subject, MAX(time) AS time FROM events
                        WHERE kind IN ('action.reused','action.retained') GROUP BY subject) e
              ON e.subject=p.action
             WHERE NOT EXISTS (SELECT 1 FROM passed_key_uses u WHERE u.action=p.action)",
            [],
        )?;
    }
    Ok(())
}

fn table_exists(db: &Connection, name: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT COUNT(*)>0 FROM sqlite_master WHERE type='table' AND name=?",
        [name],
        |r| r.get(0),
    )?)
}

/// The retention in days, None when it is off (the default).
pub fn days(db: &Connection) -> Result<Option<u32>> {
    let value: Option<String> = db
        .query_row(
            "SELECT value FROM settings WHERE key='retention_days'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(value.map(|v| v.parse()).transpose()?)
}

/// `patchweft retention --days N`: 0 clears it. Recorded as store state with an
/// `executor.retention` event, as `reserve` and `share` are.
pub fn set_retention(store: &mut Store, days: u32) -> Result<Value> {
    set_with_minimum(store, days, MIN_DAYS)
}

fn set_with_minimum(store: &mut Store, wanted: u32, minimum: u32) -> Result<Value> {
    ensure!(
        wanted == 0 || wanted >= minimum,
        "retention must keep at least {minimum} days (0 clears it)"
    );
    prepare(&store.db)?;
    let transaction = store.transaction()?;
    let previous = days(&transaction)?;
    let detail = json!({"days":wanted,"previous":previous});
    if previous == (wanted > 0).then_some(wanted) {
        // Nothing changes: no row or event.
        return Ok(detail);
    }
    if wanted == 0 {
        transaction.execute("DELETE FROM settings WHERE key='retention_days'", [])?;
    } else {
        transaction.execute(
            "INSERT INTO settings VALUES ('retention_days',?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [wanted.to_string()],
        )?;
    }
    event(&transaction, "executor.retention", "retention", &detail)?;
    transaction.commit()?;
    Ok(detail)
}

/// When retention deleted the attempt's outputs, and under how many days.
pub fn deleted(db: &Connection, attempt: &str) -> Result<Option<(f64, u32)>> {
    if !table_exists(db, "retention_deleted")? {
        return Ok(None);
    }
    Ok(db
        .query_row(
            "SELECT deleted, days FROM retention_deleted WHERE attempt=?",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?)
}

/// What `logs` answers for an attempt whose log retention deleted.
pub fn deleted_note(db: &Connection, attempt: &str) -> Result<Option<String>> {
    Ok(deleted(db, attempt)?.map(|(_, days)| format!("deleted by retention after {days} days")))
}

/// An attempt id that names one directory entry: never a path.
pub fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id != "."
        && id != ".."
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

/// Which side of the passed-key rule an old attempt is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// The passing attempt behind a passed key used within N days: kept.
    InUse,
    /// Behind a passed key not used within N days: it retires with the attempt.
    Unused,
    /// Behind no passed key.
    Keyless,
}

impl Class {
    fn name(self) -> &'static str {
        match self {
            Self::InUse => "reused_key",
            Self::Unused => "unused_key",
            Self::Keyless => "no_key",
        }
    }
}

/// A drained attempt that ended before the cutoff, and why it is kept, if it is.
#[derive(Debug, Clone)]
struct Considered {
    attempt: String,
    action: String,
    job: String,
    executor: String,
    ended: f64,
    class: Class,
    kept: Option<&'static str>,
}

/// The keep-rules, over the whole store at once. Read inside the deleting transaction, so what it
/// lets go is what that transaction deletes.
fn consider(db: &Connection, now: f64, days: u32) -> Result<Vec<Considered>> {
    let cutoff = now - f64::from(days) * DAY;
    let deleted: BTreeSet<String> = strings(db, "SELECT attempt FROM retention_deleted", ())?;
    // Each passed action's current attempt: the one its dependents are handed.
    let mut passing: BTreeMap<String, String> = BTreeMap::new();
    let mut shareable: BTreeSet<String> = BTreeSet::new();
    let mut reuse_of: BTreeMap<String, BTreeMap<String, Reuse>> = BTreeMap::new();
    {
        let mut statement = db.prepare(
            "SELECT id, attempt, json_extract(body,'$.profile'), json_extract(body,'$.job')
             FROM actions WHERE state='passed' AND attempt IS NOT NULL",
        )?;
        let rows = statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (action, attempt, profile, job) = row?;
            if let (Some(profile), Some(job)) = (profile, job) {
                if !reuse_of.contains_key(&profile) {
                    let jobs = record::<RegisteredProfile>(db, "profiles", &profile)
                        .map(|p| {
                            p.definition
                                .jobs
                                .into_iter()
                                .map(|j| (j.name, j.reuse))
                                .collect()
                        })
                        .unwrap_or_default();
                    reuse_of.insert(profile.clone(), jobs);
                }
                // A request shares a passed action of these by its action key (`request_job`).
                if matches!(
                    reuse_of[&profile].get(&job),
                    Some(Reuse::Exact | Reuse::Files)
                ) {
                    shareable.insert(attempt.clone());
                }
            }
            passing.insert(attempt, action);
        }
    }
    // Each passed key's last use; a key without one counts as used now.
    let mut last_used: BTreeMap<String, Option<f64>> = BTreeMap::new();
    {
        let mut statement = db.prepare(
            "SELECT p.action, u.last_used FROM passed_keys p
             LEFT JOIN passed_key_uses u ON u.action=p.action",
        )?;
        let rows = statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        for row in rows {
            let (action, used) = row?;
            last_used.insert(action, used);
        }
    }
    // Live work, and the attempts its dependencies were (or will be) handed.
    let mut live: BTreeSet<String> = BTreeSet::new();
    let mut sources: BTreeSet<String> = BTreeSet::new();
    {
        let mut statement = db.prepare(
            "SELECT id, json_extract(body,'$.dependencies') FROM actions
             WHERE state IN ('queued','running','stopping')",
        )?;
        let rows = statement.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        let mut dependencies = BTreeSet::new();
        for row in rows {
            let (action, depends) = row?;
            live.insert(action);
            if let Some(depends) = depends {
                dependencies.extend(serde_json::from_str::<Vec<String>>(&depends)?);
            }
        }
        for dependency in dependencies {
            let producer: Option<Option<String>> = db
                .query_row(
                    "SELECT attempt FROM actions WHERE id=?",
                    [&dependency],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(Some(producer)) = producer {
                sources.insert(producer);
            }
        }
    }
    // Candidates still open: submitted within N days, or still waiting on live work.
    let mut open: BTreeSet<String> = BTreeSet::new();
    {
        let mut statement = db.prepare("SELECT body FROM candidates")?;
        let rows = statement.query_map([], |r| r.get::<_, String>(0))?;
        for row in rows {
            let candidate: Candidate = serde_json::from_str(&row?)?;
            if candidate.created >= cutoff || candidate.required.values().any(|a| live.contains(a))
            {
                open.extend(candidate.required.into_values());
            }
        }
    }
    // The validated frontier: its candidate's actions and the attempts its certificate names.
    let mut frontier_actions: BTreeSet<String> = BTreeSet::new();
    let mut frontier_attempts: BTreeSet<String> = BTreeSet::new();
    {
        let mut statement = db.prepare("SELECT candidate, certificate FROM frontiers")?;
        let rows =
            statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for row in rows {
            let (candidate, certificate) = row?;
            if let Ok(candidate) = record::<Candidate>(db, "candidates", &candidate) {
                frontier_actions.extend(candidate.required.into_values());
            }
            let certificate: Value = serde_json::from_str(&certificate)?;
            if let Some(receipts) = certificate["receipts"].as_object() {
                for receipt in receipts.values() {
                    if let Some(action) = receipt["action"].as_str() {
                        frontier_actions.insert(action.into());
                    }
                    if let Some(attempt) = receipt["attempt"].as_str() {
                        frontier_attempts.insert(attempt.into());
                    }
                }
            }
        }
    }
    let mut considered = Vec::new();
    let mut statement = db.prepare(
        "SELECT t.id, t.action, json_extract(t.body,'$.executor'),
          COALESCE(json_extract(t.body,'$.ended'), json_extract(t.body,'$.lapsed')),
          COALESCE(json_extract(a.body,'$.job'),'')
         FROM attempts t LEFT JOIN actions a ON a.id=t.action
         WHERE t.drained=1 AND t.state NOT IN ('claimed','running','stopping')
         ORDER BY t.rowid",
    )?;
    let rows = statement.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<f64>>(3)?,
            r.get::<_, String>(4)?,
        ))
    })?;
    for row in rows {
        let (attempt, action, executor, ended, job) = row?;
        // Rule: ended more than N days ago. An attempt with no end time is kept.
        let Some(ended) = ended.filter(|ended| *ended < cutoff) else {
            continue;
        };
        if deleted.contains(&attempt) {
            continue;
        }
        // Rule: not the passing attempt behind a passed key used within N days.
        let class = match passing
            .get(&attempt)
            .filter(|passed| **passed == action)
            .and_then(|_| last_used.get(&action))
        {
            None => Class::Keyless,
            Some(used) if used.is_none_or(|used| used >= cutoff) => Class::InUse,
            Some(_) => Class::Unused,
        };
        let kept = if class == Class::InUse {
            Some("reused_key")
        } else if sources.contains(&attempt) {
            Some("live_dependency")
        } else if open.contains(&action) {
            Some("open_candidate")
        } else if frontier_actions.contains(&action) || frontier_attempts.contains(&attempt) {
            Some("frontier")
        } else if shareable.contains(&attempt) {
            Some("action_key")
        } else {
            None
        };
        considered.push(Considered {
            attempt,
            action,
            job,
            executor: executor.unwrap_or_else(|| LOCAL.into()),
            ended,
            class,
            kept,
        });
    }
    Ok(considered)
}

fn strings<P: rusqlite::Params>(db: &Connection, sql: &str, params: P) -> Result<BTreeSet<String>> {
    let mut statement = db.prepare(sql)?;
    let rows = statement.query_map(params, |r| r.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Bytes under a directory, without following links; 0 when it is gone.
fn size(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if !metadata.is_dir() {
        return metadata.len();
    }
    fs::read_dir(path)
        .map(|entries| entries.flatten().map(|entry| size(&entry.path())).sum())
        .unwrap_or(0)
}

#[derive(Default)]
struct Tally {
    attempts: u64,
    bytes: u64,
}

impl Tally {
    fn add(&mut self, bytes: u64) {
        self.attempts += 1;
        self.bytes += bytes;
    }
    fn value(&self) -> Value {
        json!({"attempts":self.attempts,"bytes":self.bytes})
    }
}

fn tallies(map: &BTreeMap<String, Tally>) -> Value {
    map.iter().map(|(k, v)| (k.clone(), v.value())).collect()
}

/// `patchweft retention --days N --dry`: what N days would delete now, and its bytes, split by
/// the passed-key rule, per executor and per job. Deletes and records nothing.
pub fn dry_run(store: &Store, days: u32, now: f64) -> Result<Value> {
    ensure!(days > 0, "a dry run needs --days of 1 or more");
    prepare(&store.db)?;
    let considered = consider(&store.db, now, days)?;
    let mut split: BTreeMap<&str, (Tally, Tally, BTreeMap<String, Tally>)> = BTreeMap::new();
    let (mut total, mut keys) = (Tally::default(), 0);
    let (mut executors, mut jobs) = (BTreeMap::new(), BTreeMap::new());
    let mut listed = Vec::new();
    for item in &considered {
        let bytes = size(&store.root.join("attempts").join(&item.attempt));
        let (all, deleted, kept) = split.entry(item.class.name()).or_default();
        all.add(bytes);
        match item.kept {
            Some(reason) => kept.entry(reason.into()).or_default().add(bytes),
            None => {
                deleted.add(bytes);
                total.add(bytes);
                keys += u64::from(item.class == Class::Unused);
                executors
                    .entry(item.executor.clone())
                    .or_insert_with(Tally::default)
                    .add(bytes);
                jobs.entry(item.job.clone())
                    .or_insert_with(Tally::default)
                    .add(bytes);
                listed.push(
                    json!({"attempt":item.attempt,"action":item.action,"job":item.job,
                    "executor":item.executor,"ended":item.ended,"class":item.class.name(),
                    "bytes":bytes}),
                );
            }
        }
    }
    let mut classes = serde_json::Map::new();
    for class in [Class::InUse, Class::Unused, Class::Keyless] {
        let (all, deleted, kept) = split.remove(class.name()).unwrap_or_default();
        classes.insert(
            class.name().into(),
            json!({"attempts":all.attempts,"bytes":all.bytes,"deleted":deleted.value(),
                   "kept":tallies(&kept)}),
        );
    }
    Ok(
        json!({"dry":true,"days":days,"setting":self::days(&store.db)?,
        "cutoff":now - f64::from(days) * DAY,"split":classes,
        "delete":{"attempts":total.attempts,"bytes":total.bytes,"keys_retired":keys},
        "by_executor":tallies(&executors),"by_job":tallies(&jobs),"attempts":listed}),
    )
}

/// The real deletion, behind its own gate: only with the setting on, at most once an hour. The
/// attempts it lets go are marked and their unused keys retired in one transaction; their
/// directories go after it commits, and a removal that fails is retried next round. Logs a
/// `retention.deleted` summary. None when the gate is shut.
pub fn round(store: &mut Store, now: f64) -> Result<Option<Value>> {
    prepare(&store.db)?;
    let Some(days) = days(&store.db)? else {
        return Ok(None);
    };
    let transaction = store.transaction()?;
    let last: Option<f64> = transaction
        .query_row(
            "SELECT value FROM settings WHERE key='retention_last_run'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .and_then(|v| v.parse().ok());
    if last.is_some_and(|last| now - last < INTERVAL) {
        return Ok(None);
    }
    transaction.execute(
        "INSERT INTO settings VALUES ('retention_last_run',?)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [now.to_string()],
    )?;
    let considered = consider(&transaction, now, days)?;
    let mut marked = Vec::new();
    let mut keys = 0;
    for item in considered.iter().filter(|item| item.kept.is_none()) {
        transaction.execute(
            "INSERT INTO retention_deleted VALUES (?,?,?,?,NULL)",
            params![item.attempt, item.executor, now, days],
        )?;
        transaction.execute(
            "UPDATE attempts SET body=json_set(body,'$.deleted_by_retention',?,
              '$.retention_days',?) WHERE id=?",
            params![now, days, item.attempt],
        )?;
        if item.executor != LOCAL {
            transaction.execute(
                "INSERT OR IGNORE INTO retention_retire VALUES (?,?,0)",
                params![item.attempt, item.executor],
            )?;
        }
        if item.class == Class::Unused {
            // Retired: never reused again, so an identical request runs cold. The key stays
            // readable for the receipts that reused it (`passed_key_of`).
            transaction.execute(
                "INSERT OR REPLACE INTO retired_keys SELECT action,key,? FROM passed_keys
                 WHERE action=?",
                params![now, item.action],
            )?;
            transaction.execute("DELETE FROM passed_keys WHERE action=?", [&item.action])?;
            transaction.execute("DELETE FROM passed_key_uses WHERE action=?", [&item.action])?;
            keys += 1;
        }
        marked.push(item);
    }
    transaction.commit()?;
    let removal = remove_pending(store, now)?;
    let mut split: BTreeMap<String, Tally> = BTreeMap::new();
    let (mut executors, mut jobs) = (BTreeMap::new(), BTreeMap::new());
    let mut total = Tally::default();
    for item in &marked {
        let bytes = removal.bytes.get(&item.attempt).copied().unwrap_or(0);
        total.add(bytes);
        split
            .entry(item.class.name().into())
            .or_default()
            .add(bytes);
        executors
            .entry(item.executor.clone())
            .or_insert_with(Tally::default)
            .add(bytes);
        jobs.entry(item.job.clone())
            .or_insert_with(Tally::default)
            .add(bytes);
    }
    let mut kept: BTreeMap<String, u64> = BTreeMap::new();
    for item in &considered {
        if let Some(reason) = item.kept {
            *kept.entry(reason.into()).or_default() += 1;
        }
    }
    let now_marked: BTreeSet<&str> = marked.iter().map(|item| item.attempt.as_str()).collect();
    let earlier = removal
        .bytes
        .keys()
        .filter(|attempt| !now_marked.contains(attempt.as_str()))
        .count();
    let summary = json!({"days":days,"attempts":total.attempts,"bytes":total.bytes,
        "keys_retired":keys,"split":tallies(&split),"kept":kept,
        "by_executor":tallies(&executors),"by_job":tallies(&jobs),
        "removed_from_earlier_rounds":earlier,"failed":removal.failed});
    event(&store.db, "retention.deleted", "retention", &summary)?;
    Ok(Some(summary))
}

struct Removal {
    /// Bytes each removed attempt directory held.
    bytes: BTreeMap<String, u64>,
    failed: Vec<Value>,
}

/// Deletes the directories of attempts marked deleted whose removal has not finished: renamed
/// aside first, so a crash mid-delete leaves a name the next round knows.
fn remove_pending(store: &Store, now: f64) -> Result<Removal> {
    let root = &store.root;
    for entry in fs::read_dir(root)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with(ASIDE) {
            let _ = crate::executor::remove_tree(&entry.path());
        }
    }
    let pending = strings(
        &store.db,
        "SELECT attempt FROM retention_deleted WHERE removed IS NULL ORDER BY attempt",
        (),
    )?;
    let mut removal = Removal {
        bytes: BTreeMap::new(),
        failed: Vec::new(),
    };
    for attempt in pending {
        let removed = (|| -> Result<u64> {
            if !safe_id(&attempt) {
                bail!("not an attempt id");
            }
            let directory = root.join("attempts").join(&attempt);
            let bytes = size(&directory);
            let aside = root.join(format!("{ASIDE}{}", unique()));
            match fs::rename(&directory, &aside) {
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
                result => result.with_context(|| format!("{}", directory.display()))?,
            }
            crate::executor::remove_tree(&aside)?;
            Ok(bytes)
        })();
        match removed {
            Ok(bytes) => {
                store.db.execute(
                    "UPDATE retention_deleted SET removed=? WHERE attempt=?",
                    params![now, attempt],
                )?;
                removal.bytes.insert(attempt, bytes);
            }
            Err(error) => removal
                .failed
                .push(json!({"attempt":attempt,"error":format!("{error:#}")})),
        }
    }
    Ok(removal)
}

/// The attempts this executor ran whose outputs retention deleted, for its claim response: each
/// id at most `RETIRE_SENDS` times, the least sent first. Empty for `local`, whose directories
/// are the store's own.
pub fn retire_list(store: &mut Store, executor: &str) -> Result<Vec<String>> {
    if executor == LOCAL || !table_exists(&store.db, "retention_retire")? {
        return Ok(Vec::new());
    }
    let select = "SELECT attempt FROM retention_retire WHERE executor=? ORDER BY sent, attempt
                  LIMIT ?";
    let waiting: bool = store.db.query_row(
        "SELECT EXISTS(SELECT 1 FROM retention_retire WHERE executor=?)",
        [executor],
        |r| r.get(0),
    )?;
    if !waiting {
        return Ok(Vec::new());
    }
    let transaction = store.transaction()?;
    let ids: Vec<String> = {
        let mut statement = transaction.prepare(select)?;
        let rows = statement.query_map(params![executor, RETIRE_BATCH], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    for id in &ids {
        transaction.execute(
            "UPDATE retention_retire SET sent=sent+1 WHERE attempt=?",
            [id],
        )?;
    }
    transaction.execute(
        "DELETE FROM retention_retire WHERE executor=? AND sent>=?",
        params![executor, RETIRE_SENDS],
    )?;
    transaction.commit()?;
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{authority, now};

    /// Long ago: every attempt ending here is past a 30-day retention.
    const OLD: f64 = 1000.0;

    struct Lab {
        _temp: tempfile::TempDir,
        store: Store,
    }

    impl Lab {
        /// One project, revision `r` and profile `f`: `build` and `test` share outstanding
        /// work only, `check` is declared, `pinned` is `exact`.
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let store = Store::init(temp.path(), 4, 4096).unwrap();
            authority::prepare(&store.db).unwrap();
            prepare(&store.db).unwrap();
            store
                .db
                .execute_batch("INSERT INTO projects VALUES ('p','alpha','/dev/null')")
                .unwrap();
            let revision = json!({"id":"r","project":"p","commit":"c".repeat(40),"tree":"t",
                "parents":[],"owner":"author","message":"m","created":0.0});
            store
                .db
                .execute(
                    "INSERT INTO revisions VALUES ('r','p',?)",
                    [revision.to_string()],
                )
                .unwrap();
            let profile = json!({"id":"f","engine":"e","platform":"linux-x86_64","tools":{},
                "definition":{"version":1,"name":"f","tools":{},"jobs":[
                    {"name":"build","command":["sh"]},{"name":"test","command":["sh"]},
                    {"name":"check","command":["sh"],"reuse":"declared"},
                    {"name":"pinned","command":["sh"],"reuse":"exact"}]}});
            crate::store::insert(&store.db, "profiles", "f", &profile).unwrap();
            Self { _temp: temp, store }
        }

        fn db(&self) -> &Connection {
            &self.store.db
        }

        fn action(&self, id: &str, job: &str, state: &str, dependencies: &[&str]) {
            let action = json!({"id":id,"key":id,"revision":"r","profile":"f","job":job,
                "dependencies":dependencies,"created":OLD});
            self.db()
                .execute(
                    "INSERT INTO actions VALUES (?,?,?,NULL,?,?)",
                    params![id, id, state, action.to_string(), OLD],
                )
                .unwrap();
        }

        /// A drained attempt of `action` that ended at `ended`, with a log and an artifact on
        /// disk, made the action's current attempt.
        fn attempt(&self, id: &str, action: &str, state: &str, ended: f64, executor: &str) {
            let mut body = json!({"id":id,"action":action,"worker_pid":0,"worker_start":"0",
                "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,
                "created":ended - 10.0,"started":ended - 5.0,"ended":ended,"exit_code":0,
                "result":{"timer_seconds":5.0,"artifacts":{}}});
            if executor != LOCAL {
                body["executor"] = json!(executor);
            }
            self.db()
                .execute(
                    "INSERT INTO attempts VALUES (?,?,?,1,?)",
                    params![id, action, state, body.to_string()],
                )
                .unwrap();
            self.db()
                .execute(
                    "UPDATE actions SET attempt=? WHERE id=?",
                    params![id, action],
                )
                .unwrap();
            let directory = self.store.root.join("attempts").join(id);
            fs::create_dir_all(directory.join("artifacts")).unwrap();
            fs::write(directory.join("output.log"), "0123456789").unwrap();
            fs::write(directory.join("artifacts").join("a.tgz"), "abcde").unwrap();
        }

        /// A passed action with its passing attempt `<id>-t`.
        fn passed(&self, id: &str, job: &str, ended: f64) -> String {
            self.action(id, job, "passed", &[]);
            let attempt = format!("{id}-t");
            self.attempt(&attempt, id, "passed", ended, LOCAL);
            attempt
        }

        fn key(&self, action: &str, last_used: f64) {
            self.db()
                .execute(
                    "INSERT INTO passed_keys VALUES (?,?)",
                    params![action, format!("key-{action}")],
                )
                .unwrap();
            self.db()
                .execute(
                    "INSERT INTO passed_key_uses VALUES (?,?)",
                    params![action, last_used],
                )
                .unwrap();
        }

        fn candidate(&self, id: &str, created: f64, required: &[(&str, &str)]) {
            let required: BTreeMap<&str, &str> = required.iter().copied().collect();
            let candidate = json!({"id":id,"revision":"r","profile":"f","required":required,
                "prerequisites":[],"owner":"agent","created":created});
            crate::store::insert(self.db(), "candidates", id, &candidate).unwrap();
        }

        fn on(&mut self) {
            set_with_minimum(&mut self.store, 30, MIN_DAYS).unwrap();
        }

        fn round(&mut self, at: f64) -> Option<Value> {
            round(&mut self.store, at).unwrap()
        }

        fn present(&self, attempt: &str) -> bool {
            self.store.root.join("attempts").join(attempt).exists()
        }

        fn deleted(&self, attempt: &str) -> bool {
            deleted(self.db(), attempt).unwrap().is_some()
        }

        fn events(&self, kind: &str) -> Vec<Value> {
            self.store
                .events(0)
                .unwrap()
                .into_iter()
                .filter(|e| e["kind"] == kind)
                .collect()
        }

        fn keys(&self) -> BTreeSet<String> {
            strings(self.db(), "SELECT action FROM passed_keys", ()).unwrap()
        }
    }

    #[test]
    fn off_by_default_nothing_is_ever_deleted() {
        let mut lab = Lab::new();
        let attempt = lab.passed("a", "build", OLD);
        lab.action("f", "test", "failed", &[]);
        lab.attempt("f-t", "f", "failed", OLD, "mindy");
        assert_eq!(days(lab.db()).unwrap(), None);
        for at in [now(), now() + 7200.0, now() + 86400.0 * 400.0] {
            assert_eq!(lab.round(at), None);
        }
        assert!(lab.present(&attempt) && lab.present("f-t"));
        assert!(!lab.deleted(&attempt) && !lab.deleted("f-t"));
        assert!(lab.events("retention.deleted").is_empty());
        assert_eq!(
            strings(lab.db(), "SELECT attempt FROM retention_retire", ()).unwrap(),
            BTreeSet::new()
        );
        // Set, then cleared: off again.
        lab.on();
        set_with_minimum(&mut lab.store, 0, MIN_DAYS).unwrap();
        assert_eq!(lab.round(now()), None);
        assert!(lab.present(&attempt) && !lab.deleted(&attempt));
    }

    #[test]
    fn the_setting_refuses_under_thirty_days_and_records_an_event() {
        let mut lab = Lab::new();
        for days in [1, 7, 29] {
            let error = set_retention(&mut lab.store, days).unwrap_err().to_string();
            assert!(error.contains("at least 30 days"), "{error}");
        }
        assert_eq!(days(lab.db()).unwrap(), None);
        // Clearing what is not set changes nothing.
        assert_eq!(
            set_retention(&mut lab.store, 0).unwrap(),
            json!({"days":0,"previous":null})
        );
        assert!(lab.events("executor.retention").is_empty());
        assert_eq!(
            set_retention(&mut lab.store, 45).unwrap(),
            json!({"days":45,"previous":null})
        );
        assert_eq!(days(lab.db()).unwrap(), Some(45));
        set_retention(&mut lab.store, 45).unwrap();
        set_retention(&mut lab.store, 30).unwrap();
        assert_eq!(
            set_retention(&mut lab.store, 0).unwrap(),
            json!({"days":0,"previous":30})
        );
        assert_eq!(days(lab.db()).unwrap(), None);
        let events = lab.events("executor.retention");
        let details: Vec<_> = events.iter().map(|e| e["detail"].clone()).collect();
        assert_eq!(
            details,
            [
                json!({"days":45,"previous":null}),
                json!({"days":30,"previous":45}),
                json!({"days":0,"previous":30})
            ]
        );
        assert!(events.iter().all(|e| e["subject"] == "retention"));
        // The test-only floor.
        set_with_minimum(&mut lab.store, 1, 1).unwrap();
        assert_eq!(days(lab.db()).unwrap(), Some(1));
    }

    /// In use (a), unused (b), keyless (c), each also once kept by another rule, plus one
    /// that ended too recently.
    fn split_lab() -> Lab {
        let lab = Lab::new();
        let recent = now() - 86400.0;
        lab.passed("used", "check", OLD);
        lab.key("used", recent);
        lab.passed("stale", "check", OLD);
        lab.key("stale", OLD);
        lab.passed("stale-dep", "check", OLD);
        lab.key("stale-dep", OLD);
        lab.action("waiting", "test", "queued", &["stale-dep"]);
        lab.passed("plain", "build", OLD);
        lab.action("lost", "test", "failed", &[]);
        lab.attempt("lost-t", "lost", "failed", OLD, "mindy");
        lab.passed("wanted", "build", OLD);
        lab.candidate("open", recent, &[("build", "wanted")]);
        lab.passed("young", "build", recent);
        lab
    }

    #[test]
    fn the_dry_run_splits_three_ways_and_deletes_nothing() {
        let lab = split_lab();
        let dry = dry_run(&lab.store, 30, now()).unwrap();
        let split = &dry["split"];
        // 15 bytes per attempt: a 10-byte log and a 5-byte artifact.
        assert_eq!(
            split["reused_key"],
            json!({"attempts":1,"bytes":15,"deleted":{"attempts":0,"bytes":0},
                   "kept":{"reused_key":{"attempts":1,"bytes":15}}})
        );
        assert_eq!(
            split["unused_key"],
            json!({"attempts":2,"bytes":30,"deleted":{"attempts":1,"bytes":15},
                   "kept":{"live_dependency":{"attempts":1,"bytes":15}}})
        );
        assert_eq!(
            split["no_key"],
            json!({"attempts":3,"bytes":45,"deleted":{"attempts":2,"bytes":30},
                   "kept":{"open_candidate":{"attempts":1,"bytes":15}}})
        );
        assert_eq!(
            dry["delete"],
            json!({"attempts":3,"bytes":45,"keys_retired":1})
        );
        assert_eq!(
            dry["by_executor"],
            json!({"local":{"attempts":2,"bytes":30},"mindy":{"attempts":1,"bytes":15}})
        );
        assert_eq!(
            dry["by_job"],
            json!({"build":{"attempts":1,"bytes":15},"check":{"attempts":1,"bytes":15},
                   "test":{"attempts":1,"bytes":15}})
        );
        let listed: BTreeSet<&str> = dry["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["attempt"].as_str().unwrap())
            .collect();
        assert_eq!(listed, BTreeSet::from(["stale-t", "plain-t", "lost-t"]));
        assert_eq!(
            (dry["dry"].clone(), dry["setting"].clone()),
            (json!(true), Value::Null)
        );
        // Nothing went: no directory, mark, key, queue row or event.
        for attempt in [
            "used-t", "stale-t", "plain-t", "lost-t", "wanted-t", "young-t",
        ] {
            assert!(lab.present(attempt) && !lab.deleted(attempt), "{attempt}");
        }
        assert_eq!(lab.keys().len(), 3);
        assert!(lab.events("retention.deleted").is_empty());
        assert_eq!(
            strings(lab.db(), "SELECT attempt FROM retention_retire", ()).unwrap(),
            BTreeSet::new()
        );
        // A dry run needs no setting and may look at any horizon; it changes nothing either.
        assert_eq!(
            dry_run(&lab.store, 400_000, now()).unwrap()["delete"]["attempts"],
            0
        );
        assert!(dry_run(&lab.store, 0, now()).is_err());
    }

    #[test]
    fn the_round_deletes_what_the_dry_run_listed_and_logs_a_summary() {
        let mut lab = split_lab();
        let listed = dry_run(&lab.store, 30, now()).unwrap();
        lab.on();
        let summary = lab.round(now()).unwrap();
        assert_eq!(
            (summary["attempts"].clone(), summary["bytes"].clone()),
            (
                listed["delete"]["attempts"].clone(),
                listed["delete"]["bytes"].clone()
            )
        );
        assert_eq!(summary["keys_retired"], 1);
        assert_eq!(summary["by_executor"], listed["by_executor"]);
        assert_eq!(summary["by_job"], listed["by_job"]);
        assert_eq!(
            summary["split"],
            json!({"no_key":{"attempts":2,"bytes":30},"unused_key":{"attempts":1,"bytes":15}})
        );
        assert_eq!(
            summary["kept"],
            json!({"live_dependency":1,"open_candidate":1,"reused_key":1})
        );
        let events = lab.events("retention.deleted");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["detail"], summary);
        for attempt in ["stale-t", "plain-t", "lost-t"] {
            assert!(!lab.present(attempt) && lab.deleted(attempt), "{attempt}");
            // The row stays, with its evidence, marked.
            let row: Value = serde_json::from_str(
                &lab.db()
                    .query_row("SELECT body FROM attempts WHERE id=?", [attempt], |r| {
                        r.get::<_, String>(0)
                    })
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(row["result"]["timer_seconds"], 5.0, "{attempt}");
            assert!(row["deleted_by_retention"].as_f64().is_some(), "{attempt}");
            assert_eq!(row["retention_days"], 30, "{attempt}");
        }
        for attempt in ["used-t", "stale-dep-t", "wanted-t", "young-t"] {
            assert!(lab.present(attempt) && !lab.deleted(attempt), "{attempt}");
        }
        // Only the remote attempt is queued for its executor.
        assert_eq!(
            strings(lab.db(), "SELECT attempt FROM retention_retire", ()).unwrap(),
            BTreeSet::from(["lost-t".to_string()])
        );
    }

    #[test]
    fn a_passed_key_used_within_n_days_keeps_its_attempt() {
        let mut lab = Lab::new();
        let attempt = lab.passed("a", "check", OLD);
        lab.key("a", now() - 29.0 * DAY);
        lab.on();
        assert_eq!(lab.round(now()).unwrap()["attempts"], 0);
        assert!(lab.present(&attempt) && !lab.deleted(&attempt));
        assert_eq!(lab.keys(), BTreeSet::from(["a".to_string()]));
        // Unused for longer than N days, it goes with its key.
        assert_eq!(lab.round(now() + 2.0 * DAY).unwrap()["keys_retired"], 1);
        assert!(!lab.present(&attempt) && lab.deleted(&attempt));
    }

    #[test]
    fn an_unused_key_retires_with_its_attempt_and_stays_readable_for_receipts() {
        let mut lab = Lab::new();
        let attempt = lab.passed("a", "check", OLD);
        lab.key("a", OLD);
        // An earlier failed attempt of the same action is behind no key.
        lab.attempt("a-0", "a", "failed", OLD - 100.0, LOCAL);
        lab.db()
            .execute("UPDATE actions SET attempt=? WHERE id='a'", [&attempt])
            .unwrap();
        lab.on();
        let summary = lab.round(now()).unwrap();
        assert_eq!(
            summary["split"],
            json!({"no_key":{"attempts":1,"bytes":15},"unused_key":{"attempts":1,"bytes":15}})
        );
        assert!(lab.keys().is_empty());
        assert_eq!(
            strings(lab.db(), "SELECT action FROM passed_key_uses", ()).unwrap(),
            BTreeSet::new()
        );
        assert_eq!(
            crate::store::passed_key_of(lab.db(), "a")
                .unwrap()
                .as_deref(),
            Some("key-a")
        );
        // The row is gone from what reuse reads, so an identical request runs cold.
        let reusable: i64 = lab
            .db()
            .query_row(
                "SELECT COUNT(*) FROM passed_keys WHERE key='key-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(reusable, 0);
    }

    #[test]
    fn a_live_dependency_is_kept_until_its_dependent_is_done() {
        let mut lab = Lab::new();
        let attempt = lab.passed("dep", "build", OLD);
        lab.action("next", "test", "queued", &["dep"]);
        lab.on();
        let mut at = now();
        for state in ["queued", "running", "stopping"] {
            lab.db()
                .execute("UPDATE actions SET state=? WHERE id='next'", [state])
                .unwrap();
            assert_eq!(lab.round(at).unwrap()["kept"], json!({"live_dependency":1}));
            assert!(lab.present(&attempt) && !lab.deleted(&attempt), "{state}");
            at += INTERVAL;
        }
        lab.db()
            .execute("UPDATE actions SET state='passed' WHERE id='next'", [])
            .unwrap();
        lab.round(at).unwrap();
        assert!(!lab.present(&attempt) && lab.deleted(&attempt));
    }

    #[test]
    fn a_reference_from_an_open_candidate_or_the_frontier_is_kept() {
        let mut lab = Lab::new();
        let recent = now() - DAY;
        // Open: submitted within N days, or old but still waiting on live work.
        let young = lab.passed("young", "build", OLD);
        lab.candidate("c-young", recent, &[("build", "young")]);
        let waiting = lab.passed("waiting", "build", OLD);
        lab.action("blocked", "test", "queued", &[]);
        lab.candidate("c-old", OLD, &[("build", "waiting"), ("test", "blocked")]);
        // Closed: old, nothing live.
        let closed = lab.passed("closed", "build", OLD);
        lab.candidate("c-closed", OLD, &[("build", "closed")]);
        // The frontier's candidate, and an attempt its certificate names.
        let promoted = lab.passed("promoted", "build", OLD);
        lab.candidate("c-front", OLD, &[("build", "promoted")]);
        let certified = lab.passed("certified", "test", OLD);
        let certificate = json!({"certificate":2,"candidate":"c-front",
            "receipts":{"test":{"action":"other","attempt":certified}}});
        lab.db()
            .execute(
                "INSERT INTO frontiers VALUES ('p','c-front',?)",
                [certificate.to_string()],
            )
            .unwrap();
        // An exact job's pass is shared by its action key.
        let pinned = lab.passed("pinned", "pinned", OLD);
        lab.on();
        let summary = lab.round(now()).unwrap();
        assert_eq!(
            summary["kept"],
            json!({"open_candidate":2,"frontier":2,"action_key":1})
        );
        for attempt in [&young, &waiting, &promoted, &certified, &pinned] {
            assert!(lab.present(attempt) && !lab.deleted(attempt), "{attempt}");
        }
        assert!(!lab.present(&closed) && lab.deleted(&closed));
    }

    #[test]
    fn only_drained_attempts_that_ended_before_the_cutoff_qualify() {
        let mut lab = Lab::new();
        lab.action("a", "build", "failed", &[]);
        lab.attempt("undrained", "a", "interrupted", OLD, LOCAL);
        lab.db()
            .execute("UPDATE attempts SET drained=0 WHERE id='undrained'", [])
            .unwrap();
        lab.action("b", "build", "failed", &[]);
        lab.attempt("endless", "b", "interrupted", OLD, LOCAL);
        lab.db()
            .execute(
                "UPDATE attempts SET body=json_remove(body,'$.ended') WHERE id='endless'",
                [],
            )
            .unwrap();
        lab.action("c", "build", "failed", &[]);
        lab.attempt("lapsed", "c", "interrupted", OLD, LOCAL);
        lab.db()
            .execute(
                "UPDATE attempts SET body=json_set(json_remove(body,'$.ended'),'$.lapsed',?)
                 WHERE id='lapsed'",
                [OLD],
            )
            .unwrap();
        lab.action("d", "build", "failed", &[]);
        lab.attempt("edge", "d", "failed", now() - 30.0 * DAY + 60.0, LOCAL);
        lab.on();
        lab.round(now()).unwrap();
        for attempt in ["undrained", "endless", "edge"] {
            assert!(lab.present(attempt) && !lab.deleted(attempt), "{attempt}");
        }
        assert!(!lab.present("lapsed") && lab.deleted("lapsed"));
    }

    #[test]
    fn the_real_deletion_runs_at_most_once_an_hour() {
        let mut lab = Lab::new();
        lab.on();
        let start = now();
        assert!(lab.round(start).is_some());
        lab.passed("a", "build", OLD);
        assert_eq!(lab.round(start + INTERVAL - 1.0), None);
        assert!(lab.present("a-t"));
        assert_eq!(lab.round(start + INTERVAL).unwrap()["attempts"], 1);
        assert!(!lab.present("a-t"));
        assert_eq!(lab.events("retention.deleted").len(), 2);
    }

    #[test]
    fn a_removal_that_failed_is_finished_by_a_later_round() {
        let mut lab = Lab::new();
        lab.passed("a", "build", OLD);
        lab.on();
        // The commit happened, the files did not go: what a crash between them leaves.
        lab.db()
            .execute(
                "INSERT INTO retention_deleted VALUES ('a-t','local',1.0,30,NULL)",
                [],
            )
            .unwrap();
        let aside = lab.store.root.join(format!("{ASIDE}leftover"));
        fs::create_dir(&aside).unwrap();
        let summary = lab.round(now()).unwrap();
        assert_eq!(summary["attempts"], 0);
        assert_eq!(summary["removed_from_earlier_rounds"], 1);
        assert!(!lab.present("a-t") && !aside.exists());
    }

    #[test]
    fn executors_are_sent_their_deleted_attempts_a_bounded_number_of_times() {
        let mut lab = Lab::new();
        for (index, executor) in ["mindy", "mindy", "robin", LOCAL].iter().enumerate() {
            let action = format!("x{index}");
            lab.action(&action, "build", "failed", &[]);
            lab.attempt(&format!("{action}-t"), &action, "failed", OLD, executor);
        }
        lab.on();
        lab.round(now()).unwrap();
        assert!(retire_list(&mut lab.store, LOCAL).unwrap().is_empty());
        for _ in 0..RETIRE_SENDS {
            assert_eq!(
                retire_list(&mut lab.store, "mindy").unwrap(),
                ["x0-t", "x1-t"]
            );
        }
        assert!(retire_list(&mut lab.store, "mindy").unwrap().is_empty());
        assert_eq!(retire_list(&mut lab.store, "robin").unwrap(), ["x2-t"]);
        // A store that never had retention sends nothing.
        let fresh = tempfile::tempdir().unwrap();
        let mut store = Store::init(fresh.path(), 1, 1024).unwrap();
        assert!(retire_list(&mut store, "mindy").unwrap().is_empty());
    }

    #[test]
    fn existing_passed_keys_are_migrated_to_their_creation_or_last_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::init(temp.path(), 1, 1024).unwrap();
        // A v29 store: passed keys, no uses table.
        store
            .db
            .execute_batch(
                "CREATE TABLE passed_keys (action TEXT PRIMARY KEY, key TEXT NOT NULL);
                 INSERT INTO passed_keys VALUES ('a','k1'),('b','k2'),('orphan','k3');",
            )
            .unwrap();
        for (id, created) in [("a", 100.0), ("b", 200.0)] {
            store
                .db
                .execute(
                    "INSERT INTO actions VALUES (?,?,'passed',NULL,'{}',?)",
                    params![id, id, created],
                )
                .unwrap();
        }
        for (subject, time, kind) in [
            ("b", 500.0, "action.reused"),
            ("b", 400.0, "action.reused"),
            ("a", 900.0, "action.queued"),
        ] {
            store
                .db
                .execute(
                    "INSERT INTO events(time,kind,subject,detail) VALUES (?,?,?,'{}')",
                    params![time, kind, subject],
                )
                .unwrap();
        }
        prepare(&store.db).unwrap();
        let uses = |db: &Connection| -> Vec<(String, f64)> {
            let mut statement = db
                .prepare("SELECT action,last_used FROM passed_key_uses ORDER BY action")
                .unwrap();
            statement
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let expected = vec![
            ("a".to_string(), 100.0),
            ("b".to_string(), 500.0),
            ("orphan".to_string(), 0.0),
        ];
        assert_eq!(uses(&store.db), expected);
        // Idempotent, and a key a v29 binary recorded since is seeded on the next prepare.
        store
            .db
            .execute(
                "UPDATE passed_key_uses SET last_used=7.0 WHERE action='a'",
                [],
            )
            .unwrap();
        store
            .db
            .execute("INSERT INTO passed_keys VALUES ('c','k4')", [])
            .unwrap();
        prepare(&store.db).unwrap();
        let mut expected = expected;
        expected[0].1 = 7.0;
        expected.insert(2, ("c".to_string(), 0.0));
        assert_eq!(uses(&store.db), expected);
        // The v29 shape of passed_keys itself is unchanged: its two-column insert still works.
        store
            .db
            .execute("INSERT INTO passed_keys VALUES ('d','k5')", [])
            .unwrap();
    }

    #[test]
    fn safe_ids_name_one_entry() {
        for id in ["a1", "0123abcd", "x-1_t.2"] {
            assert!(safe_id(id), "{id}");
        }
        for id in ["", ".", "..", "a/b", "../x", "a\0b", &"x".repeat(129)] {
            assert!(!safe_id(id), "{id:?}");
        }
    }
}
