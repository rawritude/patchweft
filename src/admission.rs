//! Caller-owned scheduling preferences, separate from job identity and operator capacity policy.
use crate::{
    model::Action,
    store::{Store, event},
};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Preference {
    /// A caller-owned check identifier, independent of a candidate or action's identity.
    pub check: String,
    #[serde(default)]
    pub priority: i64,
    /// Job names whose admission should precede other jobs of this check. Higher first.
    #[serde(default)]
    pub jobs: BTreeMap<String, i64>,
}

fn prepare(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS admission_checks (
           owner TEXT NOT NULL, name TEXT NOT NULL, project TEXT NOT NULL, priority INTEGER, version INTEGER NOT NULL,
           PRIMARY KEY(owner,name));
         CREATE TABLE IF NOT EXISTS admission_roots (
           owner TEXT NOT NULL, name TEXT NOT NULL, action TEXT NOT NULL, priority INTEGER NOT NULL,
           PRIMARY KEY(owner,name,action));
         CREATE INDEX IF NOT EXISTS admission_action ON admission_roots(action);"
    )?;
    Ok(())
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && name.bytes().all(|b| b.is_ascii_graphic())
}

pub fn validate(store: &Store, preference: &Preference, profile: &str) -> Result<()> {
    ensure!(
        valid_name(&preference.check),
        "invalid admission check name"
    );
    ensure!(
        (-1000..=1000).contains(&preference.priority),
        "invalid check priority"
    );
    let profile = store.profile(profile)?;
    for (name, priority) in &preference.jobs {
        ensure!((-1000..=1000).contains(priority), "invalid job priority");
        ensure!(
            profile.definition.jobs.iter().any(|job| job.name == *name),
            "unknown admission job"
        );
    }
    Ok(())
}

/// Called inside the API's request/submit transaction, before any job becomes claimable.
pub fn attach(
    store: &Store,
    owner: &str,
    project: &str,
    preference: &Preference,
    roots: &[Action],
) -> Result<()> {
    prepare(&store.db)?;
    // Passing roots never retry. Keep failed, canceled and interrupted roots: the same
    // action may be queued again by an explicit retry or by automatic drain recovery.
    store.db.execute(
        "DELETE FROM admission_roots WHERE owner=?1 AND EXISTS (
           SELECT 1 FROM actions a WHERE a.id=admission_roots.action AND a.state='passed')",
        [owner],
    )?;
    let existing: Option<(String, Option<i64>, i64)> = store
        .db
        .query_row(
            "SELECT project,priority,version FROM admission_checks WHERE owner=? AND name=?",
            params![owner, preference.check],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    ensure!(
        existing.as_ref().is_none_or(|(p, _, _)| p == project),
        "check belongs to another project"
    );
    ensure!(
        existing.as_ref().is_none_or(|(_, p, _)| p.is_some()),
        "admission check was released"
    );
    store.db.execute(
        "INSERT INTO admission_checks VALUES (?,?,?,?,0) ON CONFLICT(owner,name)
         DO UPDATE SET priority=CASE WHEN admission_checks.version=0 THEN excluded.priority ELSE admission_checks.priority END",
        params![owner, preference.check, project, preference.priority],
    )?;
    for action in roots {
        store.db.execute(
            "INSERT INTO admission_roots VALUES (?,?,?,?) ON CONFLICT(owner,name,action)
             DO UPDATE SET priority=excluded.priority",
            params![
                owner,
                preference.check,
                action.id,
                preference.jobs.get(&action.job).copied().unwrap_or(0)
            ],
        )?;
    }
    event(
        &store.db,
        "admission.attached",
        &preference.check,
        &json!({"owner":owner,"project":project,"priority":preference.priority,
            "actions":roots.iter().map(|a| &a.id).collect::<Vec<_>>(),"jobs":preference.jobs}),
    )?;
    Ok(())
}

/// None releases only this check's preferences, including marks on shared actions.
pub fn set(
    store: &Store,
    owner: &str,
    project: &str,
    check: &str,
    priority: Option<i64>,
    version: u64,
) -> Result<Value> {
    ensure!(valid_name(check), "invalid admission check name");
    ensure!(
        priority.is_none_or(|p| (-1000..=1000).contains(&p)),
        "invalid check priority"
    );
    prepare(&store.db)?;
    let existing: Option<(String, Option<i64>, i64)> = store
        .db
        .query_row(
            "SELECT project,priority,version FROM admission_checks WHERE owner=? AND name=?",
            params![owner, check],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    ensure!(
        existing.as_ref().is_none_or(|(p, _, _)| p == project),
        "check belongs to another project"
    );
    ensure!(
        version > 0 && version <= i64::MAX as u64,
        "invalid admission version"
    );
    ensure!(
        existing
            .as_ref()
            .is_none_or(|(_, _, v)| version as i64 > *v),
        "stale admission update"
    );
    if priority.is_some() {
        ensure!(
            existing.as_ref().is_some_and(|(_, p, _)| p.is_some()),
            "unknown or released admission check"
        );
    } else {
        store.db.execute(
            "DELETE FROM admission_roots WHERE owner=? AND name=?",
            params![owner, check],
        )?;
    }
    store.db.execute(
        "INSERT INTO admission_checks VALUES (?,?,?,?,?) ON CONFLICT(owner,name)
        DO UPDATE SET priority=excluded.priority, version=excluded.version",
        params![owner, check, project, priority, version],
    )?;
    let result = json!({"owner":owner,"project":project,"check":check,"priority":priority,"version":version});
    event(&store.db, "admission.updated", check, &result)?;
    Ok(result)
}

pub type Ranks = BTreeMap<(String, String), (i64, i64)>;

/// Every still-wanted pending root carries its preference through queued dependencies. Shared
/// actions take the highest check/job pair for each owner, never another owner's preference.
pub fn ranks(db: &Connection, queued: &[Action]) -> Result<Ranks> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='admission_roots')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(BTreeMap::new());
    }
    let queued: BTreeMap<_, _> = queued.iter().map(|a| (a.id.as_str(), a)).collect();
    let mut statement = db.prepare(
        "SELECT r.owner,c.priority,r.priority,a.body FROM admission_roots r
         JOIN admission_checks c ON c.owner=r.owner AND c.name=r.name
         JOIN actions a ON a.id=r.action WHERE c.priority IS NOT NULL AND a.state IN ('queued','running')
         AND EXISTS(SELECT 1 FROM interests i WHERE i.action=a.id AND i.owner=r.owner AND i.active=1)"
    )?;
    let rows = statement.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    let mut ranks: Ranks = BTreeMap::new();
    for row in rows {
        let (owner, check, job, body) = row?;
        let root: Action = serde_json::from_str(&body)?;
        let mut pending = vec![root.id.clone()];
        let mut seen = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            if let Some(action) = queued.get(id.as_str()) {
                let rank = ranks.entry((id, owner.clone())).or_insert((check, job));
                *rank = (*rank).max((check, job));
                pending.extend(action.dependencies.iter().cloned());
            } else if id == root.id {
                pending.extend(root.dependencies.iter().cloned());
            }
        }
    }
    Ok(ranks)
}
