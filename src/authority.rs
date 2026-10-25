//! The only code that changes attempt and lease state. The local worker calls it in-process;
//! remote executors reach the same functions through the authenticated API.
use crate::{
    api, engine,
    model::*,
    now, source,
    store::{self, Store, event, record},
    unique,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fs,
    path::PathBuf,
};

pub const LOCAL: &str = "local";
pub const DEFAULT_LEASE_SECONDS: u64 = 300;
/// The local executor's supervisors outlive a stopped worker, so its lease covers a worker
/// restart or maintenance window; a remote one keeps the default (review P3-6).
pub const LOCAL_LEASE_SECONDS: u64 = 3600;
/// A `refused` key with which an executor declines every profile, saying why (MUST-FIX 8).
pub const REFUSE_ALL: &str = "*";
const MAX_UNVERIFIED: usize = 32;
/// The executor of an attempt row. Attempts claimed before executors existed are local.
const EXECUTOR: &str = "COALESCE(json_extract(body,'$.executor'),'local')";

/// Projects and profiles an executor may run; `None` is unrestricted.
#[derive(Debug, Clone, Default)]
pub struct Scope {
    pub projects: Option<BTreeSet<String>>,
    pub profiles: Option<BTreeSet<String>>,
}

impl Scope {
    pub fn all() -> Self {
        Self::default()
    }
    pub fn allows_profile(&self, profile: &str) -> bool {
        self.profiles.as_ref().is_none_or(|p| p.contains(profile))
    }
    pub fn allows_project(&self, project: &str) -> bool {
        self.projects.as_ref().is_none_or(|p| p.contains(project))
    }
}

#[derive(Debug)]
pub enum AuthorityError {
    Invalid(String),
    Forbidden,
    Unregistered,
    Fenced,
    CompletionConflict,
    MissingBlobs(Vec<String>),
    InvalidEvidence(String),
    Internal(anyhow::Error),
}

impl std::fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::InvalidEvidence(message) => write!(f, "{message}"),
            Self::Forbidden => write!(f, "permission does not cover this operation"),
            Self::Unregistered => write!(f, "executor is not registered"),
            Self::Fenced => write!(f, "attempt is fenced"),
            Self::CompletionConflict => write!(f, "attempt was completed by a different request"),
            Self::MissingBlobs(missing) => write!(f, "uploads missing: {}", missing.join(",")),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}
impl std::error::Error for AuthorityError {}
impl From<anyhow::Error> for AuthorityError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}
impl From<rusqlite::Error> for AuthorityError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Internal(error.into())
    }
}
impl From<serde_json::Error> for AuthorityError {
    fn from(error: serde_json::Error) -> Self {
        Self::Internal(error.into())
    }
}

/// Additive and idempotent; the store schema itself is unchanged.
pub fn prepare(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS executors (id TEXT PRIMARY KEY, body TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS manifest_sizes (attempt TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS requeues (action TEXT PRIMARY KEY, count INTEGER NOT NULL);
         CREATE INDEX IF NOT EXISTS attempts_held ON attempts(drained, state);
         CREATE INDEX IF NOT EXISTS attempts_claim_key ON attempts(json_extract(body,'$.claim_key'));",
    )?;
    db.execute(
        "INSERT OR IGNORE INTO executors VALUES (?,?)",
        params![LOCAL, serde_json::to_string(&default_record(LOCAL))?],
    )?;
    Ok(())
}

pub fn default_record(id: &str) -> ExecutorRecord {
    ExecutorRecord {
        id: id.into(),
        slots: None,
        memory_mb: None,
        lease_seconds: if id == LOCAL {
            LOCAL_LEASE_SECONDS
        } else {
            DEFAULT_LEASE_SECONDS
        },
        placement: Placement::default(),
        created: now(),
        observed: Observed::default(),
        holding: None,
    }
}

fn table_exists(db: &Connection, name: &str) -> Result<bool> {
    let count: i64 = db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        [name],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

/// A store not yet prepared knows only the local executor.
pub fn load(db: &Connection, id: &str) -> Result<Option<ExecutorRecord>> {
    if !table_exists(db, "executors")? {
        return Ok((id == LOCAL).then(|| default_record(LOCAL)));
    }
    let body: Option<String> = db
        .query_row("SELECT body FROM executors WHERE id=?", [id], |r| r.get(0))
        .optional()?;
    Ok(body.map(|body| serde_json::from_str(&body)).transpose()?)
}

pub fn save(db: &Connection, record: &ExecutorRecord) -> Result<()> {
    db.execute(
        "INSERT INTO executors VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
        params![record.id, serde_json::to_string(record)?],
    )?;
    Ok(())
}

/// The executor's total budget. Memory never exceeds the cgroup limit its coordinator reported.
pub fn capacity(db: &Connection, record: &ExecutorRecord) -> Result<Reservation> {
    let (slots, memory_mb) = if record.id == LOCAL {
        store::capacity(db)?
    } else {
        (
            record.slots.context("executor has no slots")?,
            record.memory_mb.context("executor has no memory")?,
        )
    };
    let memory_mb = record
        .observed
        .memory_limit_mb
        .map_or(memory_mb, |limit| limit.min(memory_mb));
    Ok(Reservation { slots, memory_mb })
}

/// Reservations of this executor's undrained attempts. Other executors' holds never count.
pub fn held(db: &Connection, executor: &str) -> Result<Reservation> {
    Ok(db.query_row(
        &format!(
            "SELECT COALESCE(SUM(json_extract(body,'$.slots')),0),
             COALESCE(SUM(json_extract(body,'$.memory_mb')),0)
             FROM attempts WHERE drained=0 AND {EXECUTOR}=?"
        ),
        [executor],
        |r| {
            Ok(Reservation {
                slots: r.get(0)?,
                memory_mb: r.get(1)?,
            })
        },
    )?)
}

pub fn fits(held: Reservation, wanted: Reservation, capacity: Reservation) -> bool {
    held.slots + wanted.slots <= capacity.slots
        && held.memory_mb + wanted.memory_mb <= capacity.memory_mb
}

pub struct Choice {
    pub owner: String,
    /// The owner's scheduling priority.
    pub priority: i64,
    pub action: Action,
    pub reservation: Reservation,
    pub timeout_seconds: u64,
}

pub struct Selection {
    pub choice: Option<Choice>,
    /// Profiles of admissible work the executor has not verified (at most 32).
    pub unverified: Vec<String>,
    /// Capacity held for prioritized work this executor could run once a running dependency
    /// passes (the train-first hold): what it needs, per priority.
    pub hold: BTreeMap<i64, Reservation>,
    /// What the running dependencies being waited on hold here, with the highest priority of the
    /// held jobs waiting on each: it frees before they start, so it is not counted beside the hold
    /// as well (gate P3-3), and only for heads below that priority (re-gate P3-1).
    pub freed: Vec<(i64, Reservation)>,
}

/// The head of one executor's queue: work it may run and could ever fit, ordered by owner
/// priority then fair share. An unverified head admits nothing until it is verified, so
/// verification lag never reorders admission.
pub fn select(
    db: &Connection,
    record: &ExecutorRecord,
    scope: &Scope,
    verified: &BTreeSet<String>,
    refused: &BTreeSet<String>,
    capacity: Reservation,
) -> Result<Selection> {
    let queued: Vec<Action> = {
        let mut stmt =
            db.prepare("SELECT body FROM actions WHERE state='queued' ORDER BY created,id")?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .map(|v| Ok(serde_json::from_str(&v?)?))
            .collect::<Result<_>>()?
    };
    let priorities = owner_priorities(db)?;
    let mut profiles = BTreeMap::new();
    let mut unverified = Vec::new();
    let mut choices = Vec::new();
    let mut hold: BTreeMap<i64, Reservation> = BTreeMap::new();
    let mut waited_on = BTreeMap::new();
    for action in queued {
        if !scope.allows_profile(&action.profile)
            || refused.contains(&action.profile)
            || refused.contains(REFUSE_ALL)
            || !record.placement.permits(&action.job)
        {
            continue;
        }
        let eligible = engine::eligible(db, &action)?;
        if !eligible && !engine::eligible_soon(db, &action)? {
            continue;
        }
        if scope.projects.is_some() && !scope.allows_project(&project(db, &action.revision)?) {
            continue;
        }
        let (reservation, timeout_seconds) = budget(db, &mut profiles, &action)?;
        // Work this executor can never fit must not idle it behind the head.
        if reservation.slots > capacity.slots || reservation.memory_mb > capacity.memory_mb {
            continue;
        }
        if !eligible {
            // The train-first hold (chief, #4204): a prioritized owner's job waiting only on a
            // running dependency keeps the capacity it will need from lower-priority owners.
            let mut stmt =
                db.prepare("SELECT DISTINCT owner FROM interests WHERE action=? AND active=1")?;
            let priority = stmt
                .query_map([&action.id], |r| r.get::<_, String>(0))?
                .map(|owner| Ok(priorities.get(&owner?).copied().unwrap_or(0)))
                .collect::<Result<Vec<i64>>>()?
                .into_iter()
                .max()
                .unwrap_or(0);
            if priority > 0 {
                let demand = hold.entry(priority).or_insert(Reservation {
                    slots: 0,
                    memory_mb: 0,
                });
                demand.slots = demand.slots.saturating_add(reservation.slots);
                demand.memory_mb = demand.memory_mb.saturating_add(reservation.memory_mb);
                for dependency in &action.dependencies {
                    let attempt: Option<String> = db
                        .query_row(
                            "SELECT attempt FROM actions WHERE id=? AND state='running'",
                            [dependency],
                            |r| r.get(0),
                        )
                        .optional()?
                        .flatten();
                    let running: Option<(String, String)> = match attempt {
                        Some(attempt) => db
                            .query_row(
                                &format!(
                                    "SELECT id, body FROM attempts WHERE id=? AND drained=0 AND {EXECUTOR}=?"
                                ),
                                [&attempt, &record.id],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .optional()?,
                        None => None,
                    };
                    if let Some((attempt, body)) = running {
                        let held: Attempt = serde_json::from_str(&body)?;
                        let entry = waited_on.entry(attempt).or_insert((
                            priority,
                            held.slots,
                            held.memory_mb,
                        ));
                        entry.0 = entry.0.max(priority);
                    }
                }
            }
            continue;
        }
        // A claim inlines its dependencies' manifests: one a remote executor could not read is
        // never served remotely, so it cannot loop holding a slot; `local` reads none (P2-1).
        if record.id != LOCAL && dependency_manifests(db, &action)? > api::MAX_CLAIM_MANIFESTS {
            continue;
        }
        let trusted = verified.contains(&action.profile);
        if !trusted && unverified.len() < MAX_UNVERIFIED && !unverified.contains(&action.profile) {
            unverified.push(action.profile.clone());
        }
        let mut stmt =
            db.prepare("SELECT DISTINCT owner FROM interests WHERE action=? AND active=1")?;
        let owners: Vec<String> = stmt
            .query_map([&action.id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for owner in owners {
            let served = db
                .query_row("SELECT last FROM dispatches WHERE owner=?", [&owner], |r| {
                    r.get::<_, f64>(0)
                })
                .optional()?
                .unwrap_or(0.0);
            let priority = priorities.get(&owner).copied().unwrap_or(0);
            let choice = Choice {
                owner,
                priority,
                action: action.clone(),
                reservation,
                timeout_seconds,
            };
            choices.push((priority, served, trusted, choice));
        }
    }
    // Higher operator priority first (a landing-ready train), then fair share between owners.
    choices.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.total_cmp(&b.1))
            .then(a.3.action.created.total_cmp(&b.3.action.created))
            .then(a.3.owner.cmp(&b.3.owner))
    });
    let choice = choices
        .into_iter()
        .next()
        .filter(|(_, _, trusted, _)| *trusted)
        .map(|(.., choice)| choice);
    let freed = waited_on
        .into_values()
        .map(|(priority, slots, memory_mb)| (priority, Reservation { slots, memory_mb }))
        .collect();
    Ok(Selection {
        choice,
        unverified,
        hold,
        freed,
    })
}

/// The serialized size of an attempt's artifact manifest, as a claim inlines it. Recorded once
/// with the result, so the claim path reads a number instead of parsing manifests (gate P2-A).
fn record_manifest_size(db: &Connection, attempt: &str, evidence: &Value) -> Result<u64> {
    let bytes = serde_json::to_vec(&evidence["artifacts"])?.len() as u64;
    db.execute(
        "INSERT OR REPLACE INTO manifest_sizes VALUES (?,?)",
        params![attempt, bytes as i64],
    )?;
    Ok(bytes)
}

/// Bytes of the dependency manifests a claim of `action` would inline.
fn dependency_manifests(db: &Connection, action: &Action) -> Result<u64> {
    let mut total = 0;
    for dependency in &action.dependencies {
        let row: Option<(Option<String>, Option<i64>)> = db
            .query_row(
                "SELECT a.attempt, m.bytes FROM actions a
                 LEFT JOIN manifest_sizes m ON m.attempt = a.attempt WHERE a.id=?",
                [dependency],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        total += match row {
            Some((_, Some(bytes))) => bytes as u64,
            // Recorded before sizes were stored: measured once, then read like the rest.
            Some((Some(producer), None)) => {
                match record::<Attempt>(db, "attempts", &producer)?.result {
                    Some(evidence) => record_manifest_size(db, &producer, &evidence)?,
                    None => 0,
                }
            }
            _ => 0,
        };
    }
    Ok(total)
}

/// Legacy actions carry neither field; their creating profile's job values apply.
fn budget(
    db: &Connection,
    profiles: &mut BTreeMap<String, RegisteredProfile>,
    action: &Action,
) -> Result<(Reservation, u64)> {
    if let (Some(reservation), Some(identity)) = (action.reservation, &action.identity) {
        return Ok((reservation, identity.timeout_seconds));
    }
    let profile = match profiles.entry(action.profile.clone()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(record(db, "profiles", &action.profile)?),
    };
    let job = engine::job(profile, &action.job)?;
    Ok((
        action.reservation.unwrap_or_else(|| Reservation::from(job)),
        action
            .identity
            .as_ref()
            .map_or(job.timeout_seconds, |i| i.timeout_seconds),
    ))
}

fn owner_priorities(db: &Connection) -> Result<BTreeMap<String, i64>> {
    if !table_exists(db, "owner_priorities")? {
        return Ok(BTreeMap::new());
    }
    let mut stmt = db.prepare("SELECT owner, priority FROM owner_priorities")?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

const MAX_PROFILE_IDS: usize = 32;

/// The executor's lease and the profiles it should verify: the ones named, or else those with
/// queued work. Profiles outside its scope are never handed out.
pub fn hello(
    db: &Connection,
    executor: &str,
    scope: &Scope,
    ids: &[String],
) -> Result<Hello, AuthorityError> {
    if ids.len() > MAX_PROFILE_IDS {
        return Err(AuthorityError::Invalid(format!(
            "At most {MAX_PROFILE_IDS} profiles are described at once."
        )));
    }
    let record = load(db, executor)?.ok_or(AuthorityError::Unregistered)?;
    let ids: Vec<String> = if ids.is_empty() {
        let mut stmt = db.prepare(
            "SELECT DISTINCT json_extract(body,'$.profile') FROM actions WHERE state='queued' ORDER BY 1",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    } else {
        ids.to_vec()
    };
    let mut profiles = Vec::new();
    for id in ids.iter().filter(|id| scope.allows_profile(id)) {
        let body: Option<String> = db
            .query_row("SELECT body FROM profiles WHERE id=?", [id], |r| r.get(0))
            .optional()?;
        if let Some(body) = body {
            profiles.push(serde_json::from_str(&body)?);
        }
    }
    Ok(Hello {
        executor: executor.into(),
        lease_seconds: record.lease_seconds,
        profiles,
    })
}

/// Admit the head of this executor's queue. The same claim key returns the attempt it already
/// claimed, so a lost response never claims twice.
pub fn claim_for(
    store: &mut Store,
    executor: &str,
    scope: &Scope,
    request: &ClaimRequest,
) -> Result<ClaimResponse, AuthorityError> {
    let key = &request.claim_key;
    if !api::visible_key(key) {
        return Err(AuthorityError::Invalid(
            "A 1–128 byte visible-ASCII claim key is required.".into(),
        ));
    }
    // A narrowed grant only narrows what can be claimed: verified profiles outside the scope are
    // ignored rather than refused, so the executor keeps working instead of exiting (gate P3-3).
    let verified: BTreeSet<String> = request
        .verified
        .iter()
        .filter(|profile| scope.allows_profile(profile))
        .cloned()
        .collect();
    let transaction = store.transaction()?;
    let mut record = load(&transaction, executor)?.ok_or(AuthorityError::Unregistered)?;
    observe(&transaction, &mut record, request, verified.clone())?;
    if let Some(response) = replay(&transaction, executor, key, request.unrecorded.as_deref())? {
        transaction.commit()?;
        return Ok(response);
    }
    // The coordinator settles the key whatever this answers, so a claim made now would be lost.
    if request.unrecorded.is_some() {
        transaction.commit()?;
        return Ok(ClaimResponse {
            claim: None,
            fenced: None,
            unverified: Vec::new(),
        });
    }
    let capacity = capacity(&transaction, &record)?;
    let refused = request.refused.keys().cloned().collect();
    let Selection {
        choice,
        unverified,
        hold,
        freed,
    } = select(&transaction, &record, scope, &verified, &refused, capacity)?;
    let held = held(&transaction, executor)?;
    // A head is admitted only beside the demand of the higher priorities waiting (gate P3-1): a
    // train never holds back its own jobs, and a lower train waits only for the higher ones.
    let holding = |priority: i64| {
        let demand = hold.range(priority + 1..).fold(
            Reservation {
                slots: 0,
                memory_mb: 0,
            },
            |sum, (_, need)| Reservation {
                slots: sum.slots.saturating_add(need.slots),
                memory_mb: sum.memory_mb.saturating_add(need.memory_mb),
            },
        );
        (demand.slots > 0 || demand.memory_mb > 0).then_some(demand)
    };
    let beside_hold = |choice: &Choice| {
        holding(choice.priority).is_none_or(|demand| {
            // Only dependencies higher priorities wait on free room for this head.
            let freed = freed
                .iter()
                .filter(|(priority, _)| *priority > choice.priority)
                .fold(
                    Reservation {
                        slots: 0,
                        memory_mb: 0,
                    },
                    |sum, (_, need)| Reservation {
                        slots: sum.slots.saturating_add(need.slots),
                        memory_mb: sum.memory_mb.saturating_add(need.memory_mb),
                    },
                );
            fits(
                Reservation {
                    slots: held
                        .slots
                        .saturating_sub(freed.slots)
                        .saturating_add(demand.slots),
                    memory_mb: held
                        .memory_mb
                        .saturating_sub(freed.memory_mb)
                        .saturating_add(demand.memory_mb),
                },
                choice.reservation,
                capacity,
            )
        })
    };
    let blocked = choice.as_ref().filter(|choice| !beside_hold(choice));
    let now_holding = blocked.and_then(|choice| {
        let demand = holding(choice.priority)?;
        Some(Holding {
            priority: hold.keys().next_back().copied().unwrap_or(choice.priority),
            slots: demand.slots,
            memory_mb: demand.memory_mb,
        })
    });
    if record.holding != now_holding {
        let kind = if now_holding.is_some() {
            "executor.held"
        } else {
            "executor.hold_released"
        };
        event(
            &transaction,
            kind,
            executor,
            &json!({"executor":executor,"holding":now_holding.or(record.holding)}),
        )?;
        record.holding = now_holding;
        save(&transaction, &record)?;
    }
    let choice = match choice {
        Some(choice) if fits(held, choice.reservation, capacity) && beside_hold(&choice) => choice,
        // Drain for the head: later small actions cannot starve its reservation.
        _ => {
            transaction.commit()?;
            return Ok(ClaimResponse {
                claim: None,
                fenced: None,
                unverified,
            });
        }
    };
    let created = now();
    let attempt = Attempt {
        id: unique(),
        action: choice.action.id.clone(),
        worker_pid: 0,
        worker_start: String::new(),
        supervisor_pid: None,
        supervisor_start: None,
        slots: choice.reservation.slots,
        memory_mb: choice.reservation.memory_mb,
        created,
        started: None,
        ended: None,
        exit_code: None,
        result: None,
        timeout_seconds: Some(choice.timeout_seconds),
        executor: Some(executor.into()),
        lease: Some(unique()),
        lease_expires: Some(created + record.lease_seconds as f64),
        claim_key: Some(key.clone()),
        completion: None,
        stop: None,
    };
    transaction.execute(
        "INSERT INTO attempts VALUES (?,?,'claimed',0,?)",
        params![attempt.id, attempt.action, serde_json::to_string(&attempt)?],
    )?;
    transaction.execute(
        "UPDATE actions SET state='running',attempt=? WHERE id=? AND state='queued'",
        params![attempt.id, attempt.action],
    )?;
    // Fair share is global: every executor's claims serve the same owner history.
    transaction.execute(
        "INSERT INTO dispatches VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET last=excluded.last",
        params![choice.owner, now()],
    )?;
    event(
        &transaction,
        "attempt.claimed",
        &attempt.id,
        &json!({"action":attempt.action,"owner":choice.owner,"executor":executor,
                "slots":attempt.slots,"memory_mb":attempt.memory_mb,
                "timeout_seconds":choice.timeout_seconds}),
    )?;
    let claim = claim_of(&transaction, &attempt, choice.action)?;
    transaction.commit()?;
    Ok(ClaimResponse {
        claim: Some(claim),
        fenced: None,
        unverified,
    })
}

/// Steady polling rewrites the record at most every 30 s; changed verification is an event.
fn observe(
    db: &Connection,
    record: &mut ExecutorRecord,
    request: &ClaimRequest,
    verified: BTreeSet<String>,
) -> Result<()> {
    let seen = now();
    let observed = Observed {
        last_seen: Some(seen),
        verified,
        refused: request.refused.clone(),
        memory_limit_mb: request.memory_limit_mb,
        disk_free_mb: request.disk_free_mb,
        host: request.host.clone(),
        engine: request.engine.clone(),
        loopback: request.loopback.clone(),
    };
    let previous = &record.observed;
    let profiles_changed =
        previous.verified != observed.verified || previous.refused != observed.refused;
    if !profiles_changed
        && previous.memory_limit_mb == observed.memory_limit_mb
        && previous.host == observed.host
        && previous.engine == observed.engine
        && previous.loopback == observed.loopback
        && previous.last_seen.is_some_and(|last| seen - last <= 30.0)
    {
        return Ok(());
    }
    db.execute(
        "UPDATE executors SET body=json_set(body,'$.observed',json(?)) WHERE id=?",
        params![serde_json::to_string(&observed)?, record.id],
    )?;
    if profiles_changed {
        event(
            db,
            "executor.observed",
            &record.id,
            &json!({"verified":observed.verified,"refused":observed.refused}),
        )?;
    }
    record.observed = observed;
    Ok(())
}

/// A key already used by this executor: a still-held attempt is the same claim again, any other
/// is reported fenced. Only expiry judges lease time, so no clock is compared here. A coordinator
/// that never recorded the fenced attempt says so as `unrecorded`, which may drain it.
fn replay(
    db: &Connection,
    executor: &str,
    key: &str,
    unrecorded: Option<&str>,
) -> Result<Option<ClaimResponse>> {
    let row: Option<(String, String, bool, String)> = db
        .query_row(
            &format!(
                "SELECT id,state,drained,body FROM attempts
                 WHERE json_extract(body,'$.claim_key')=?1 AND {EXECUTOR}=?2"
            ),
            params![key, executor],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((id, state, drained, body)) = row else {
        return Ok(None);
    };
    let claim = if !drained && ["claimed", "running"].contains(&state.as_str()) {
        let attempt: Attempt = serde_json::from_str(&body)?;
        let action = record(db, "actions", &attempt.action)?;
        Some(claim_of(db, &attempt, action)?)
    } else {
        None
    };
    if claim.is_none() && unrecorded == Some(id.as_str()) {
        let attempt: Attempt = serde_json::from_str(&body)?;
        // The lookup bound the attempt to this executor and key. Only a lapsed lease the store
        // never saw start or report is drained: past either, processes or resources may remain
        // that only the executor's own drain report can vouch for. Anything else is ignored, not
        // refused: the coordinator settles the key either way, a drain recorded since is a benign
        // race, and an error would end its claim loop.
        if state == "interrupted"
            && !drained
            && attempt.started.is_none()
            && attempt.supervisor_pid.is_none()
            && attempt.completion.is_none()
        {
            let detail = json!({"executor":executor,"reason":"claim-unrecorded"});
            drain_lapsed(db, &attempt, true, detail)?;
        }
    }
    let fenced = claim.is_none().then_some(id);
    Ok(Some(ClaimResponse {
        claim,
        fenced,
        unverified: Vec::new(),
    }))
}

/// Everything a coordinator needs to run the attempt; rebuilt from the store on replay.
fn claim_of(db: &Connection, attempt: &Attempt, action: Action) -> Result<Claim> {
    let profile: RegisteredProfile = record(db, "profiles", &action.profile)?;
    let revision: Revision = record(db, "revisions", &action.revision)?;
    let mut dependencies = Vec::new();
    for dependency in &action.dependencies {
        let (state, producer): (String, Option<String>) = db.query_row(
            "SELECT state,attempt FROM actions WHERE id=?",
            [dependency],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(state == "passed", "dependency no longer passes");
        let producer = producer.context("passed dependency lacks an attempt")?;
        let evidence: Value = record::<Attempt>(db, "attempts", &producer)?
            .result
            .context("dependency lacks evidence")?;
        let artifacts = serde_json::from_value(evidence["artifacts"].clone())
            .context("dependency lacks artifacts")?;
        dependencies.push(DependencyEvidence {
            action: dependency.clone(),
            attempt: producer,
            artifacts,
        });
    }
    Ok(Claim {
        attempt: Grant {
            id: attempt.id.clone(),
            lease: attempt.lease.clone().context("attempt has no lease")?,
            lease_expires: attempt.lease_expires.context("attempt has no lease")?,
            slots: attempt.slots,
            memory_mb: attempt.memory_mb,
            timeout_seconds: attempt.timeout_seconds.context("attempt has no timeout")?,
        },
        action,
        profile,
        revision: RevisionRef {
            id: revision.id,
            project: revision.project,
            commit: revision.commit,
        },
        dependencies,
    })
}

fn project(db: &Connection, revision: &str) -> Result<String> {
    Ok(db.query_row(
        "SELECT project FROM revisions WHERE id=?",
        [revision],
        |r| r.get(0),
    )?)
}

/// The one writer of the stop transition. A reason the executor reports wins; otherwise the
/// store stops a running action nobody requests any more. Returns the recorded reason.
pub fn stopping(
    db: &Connection,
    action: &str,
    attempt: &str,
    reported: Option<&str>,
) -> Result<Option<String>> {
    let (state, current): (String, Option<String>) = db.query_row(
        "SELECT state,attempt FROM actions WHERE id=?",
        [action],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if current.as_deref() != Some(attempt) {
        return Ok(None);
    }
    if state == "stopping" {
        return Ok(db.query_row(
            "SELECT json_extract(body,'$.stop') FROM attempts WHERE id=?",
            [attempt],
            |r| r.get(0),
        )?);
    }
    if state != "running" {
        return Ok(None);
    }
    let reason = match reported {
        Some(reason) => reason.to_owned(),
        None => {
            let active: i64 = db.query_row(
                "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
                [action],
                |r| r.get(0),
            )?;
            if active > 0 {
                return Ok(None);
            }
            "no-requesters".to_owned()
        }
    };
    db.execute(
        "UPDATE actions SET state='stopping' WHERE id=? AND attempt=? AND state='running'",
        params![action, attempt],
    )?;
    db.execute(
        "UPDATE attempts SET body=json_set(body,'$.stop',?) WHERE id=?",
        params![reason, attempt],
    )?;
    let executor: String = db.query_row(
        &format!("SELECT {EXECUTOR} FROM attempts WHERE id=?"),
        [attempt],
        |r| r.get(0),
    )?;
    event(
        db,
        "attempt.stopping",
        attempt,
        &json!({"reason":reason,"executor":executor}),
    )?;
    Ok(Some(reason))
}

/// This executor's attempt under this lease, with its state and drained flag.
fn owned(
    db: &Connection,
    executor: &str,
    attempt: &str,
    lease: &str,
) -> Result<Option<(String, bool, Attempt)>> {
    let row: Option<(String, bool, String)> = db
        .query_row(
            "SELECT state,drained,body FROM attempts WHERE id=?",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((state, drained, body)) = row else {
        return Ok(None);
    };
    let attempt: Attempt = serde_json::from_str(&body)?;
    let mine = attempt.executor.as_deref().unwrap_or(LOCAL) == executor
        && attempt.lease.as_deref() == Some(lease);
    Ok(mine.then_some((state, drained, attempt)))
}

/// The action of an attempt that is still held (A10), within the executor's current scope (A6),
/// with the action's state. None means the attempt is fenced.
fn holding(
    db: &Connection,
    scope: &Scope,
    attempt: &Attempt,
    state: &str,
) -> Result<Option<(Action, String)>> {
    if !["claimed", "running"].contains(&state) {
        return Ok(None);
    }
    let row: Option<(String, Option<String>, String)> = db
        .query_row(
            "SELECT state,attempt,body FROM actions WHERE id=?",
            [&attempt.action],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((action_state, current, body)) = row else {
        return Ok(None);
    };
    if current.as_deref() != Some(attempt.id.as_str())
        || !["running", "stopping"].contains(&action_state.as_str())
    {
        return Ok(None);
    }
    let action: Action = serde_json::from_str(&body)?;
    if !scope.allows_profile(&action.profile)
        || (scope.projects.is_some() && !scope.allows_project(&project(db, &action.revision)?))
    {
        return Ok(None);
    }
    Ok(Some((action, action_state)))
}

pub const MAX_LEASES: usize = 64;
/// Stops a supervisor decides itself. `no-requesters` is decided by the store alone.
const SUPERVISOR_STOPS: [&str; 2] = ["timeout", "supervisor-stop"];

/// Extends the leases an executor still holds, registers its supervisors and relays stops in
/// both directions. Entries it no longer holds are fenced and left untouched.
pub fn renew(
    store: &mut Store,
    executor: &str,
    scope: &Scope,
    request: &RenewRequest,
) -> Result<RenewResponse, AuthorityError> {
    if request.leases.len() > MAX_LEASES {
        return Err(AuthorityError::Invalid(format!(
            "At most {MAX_LEASES} leases are renewed at once."
        )));
    }
    let transaction = store.transaction()?;
    let mut record = load(&transaction, executor)?.ok_or(AuthorityError::Unregistered)?;
    seen(&transaction, &mut record)?;
    let mut leases = Vec::new();
    for entry in &request.leases {
        // A job can write its own control files: an unknown stop fences that lease alone, and
        // the rest of the host's batch renews (review P3-7).
        if entry
            .stopping
            .as_deref()
            .is_some_and(|reason| !SUPERVISOR_STOPS.contains(&reason))
        {
            leases.push(LeaseStatus {
                attempt: entry.attempt.clone(),
                status: "fenced".into(),
                stop: None,
                registered: false,
                lease_expires: None,
            });
            continue;
        }
        leases.push(renew_lease(&transaction, &record, scope, entry)?);
    }
    transaction.commit()?;
    Ok(RenewResponse {
        lease_seconds: record.lease_seconds,
        leases,
    })
}

fn renew_lease(
    db: &Connection,
    record: &ExecutorRecord,
    scope: &Scope,
    entry: &LeaseReport,
) -> Result<LeaseStatus> {
    let held = match owned(db, &record.id, &entry.attempt, &entry.lease)? {
        Some((state, _, attempt)) => holding(db, scope, &attempt, &state)?
            .map(|(action, action_state)| (state, attempt, action, action_state)),
        None => None,
    };
    let Some((mut state, mut attempt, action, action_state)) = held else {
        return Ok(LeaseStatus {
            attempt: entry.attempt.clone(),
            status: "fenced".into(),
            stop: None,
            registered: false,
            lease_expires: None,
        });
    };
    let expires = now() + record.lease_seconds as f64;
    attempt.lease_expires = Some(expires);
    // Nothing runs before the store records the attempt running, and nothing starts once the
    // action is stopping.
    if let Some(started) = &entry.started
        && state == "claimed"
        && action_state == "running"
    {
        state = "running".into();
        attempt.started = Some(now());
        attempt.supervisor_pid = Some(started.pid);
        attempt.supervisor_start = Some(started.start.clone());
        event(
            db,
            "attempt.started",
            &attempt.id,
            &json!({"pid":started.pid,"executor":record.id}),
        )?;
    }
    db.execute(
        "UPDATE attempts SET state=?,body=? WHERE id=?",
        params![state, serde_json::to_string(&attempt)?, attempt.id],
    )?;
    let stop = stopping(db, &action.id, &attempt.id, entry.stopping.as_deref())?;
    Ok(LeaseStatus {
        attempt: attempt.id,
        status: "ok".into(),
        stop,
        registered: state == "running",
        lease_expires: Some(expires),
    })
}

/// A heartbeat is a sign of life; like claims it rewrites the record at most every 30 s.
fn seen(db: &Connection, record: &mut ExecutorRecord) -> Result<()> {
    let seen = now();
    if record
        .observed
        .last_seen
        .is_some_and(|last| seen - last <= 30.0)
    {
        return Ok(());
    }
    record.observed.last_seen = Some(seen);
    db.execute(
        "UPDATE executors SET body=json_set(body,'$.observed',json(?)) WHERE id=?",
        params![serde_json::to_string(&record.observed)?, record.id],
    )?;
    Ok(())
}

pub enum Outputs {
    /// The attempt's files are already in `state/attempts/ID` (the local executor).
    InPlace,
    /// Verified uploads, moved into `state/attempts/ID` only when the result is recorded.
    Staged(PathBuf),
}

/// Records an executor's report of its attempt. A replay of the recorded report is answered
/// again; a fenced attempt's result is never recorded, though its drain may be.
pub fn complete(
    store: &mut Store,
    executor: &str,
    scope: &Scope,
    request: &CompleteRequest,
    outputs: Outputs,
) -> Result<CompleteResponse, AuthorityError> {
    let result = record_completion(store, executor, scope, request, &outputs);
    // Uploads of an unrecorded completion are consumed, never kept. serve's purge of stale
    // staging covers a removal that fails here.
    if let Outputs::Staged(directory) = &outputs
        && directory.exists()
    {
        let _ = fs::remove_dir_all(directory);
    }
    result
}

fn record_completion(
    store: &mut Store,
    executor: &str,
    scope: &Scope,
    request: &CompleteRequest,
    outputs: &Outputs,
) -> Result<CompleteResponse, AuthorityError> {
    let hash = crate::identity(request)?;
    let root = store.root.clone();
    let transaction = store.transaction()?;
    let Some((state, drained, attempt)) =
        owned(&transaction, executor, &request.attempt, &request.lease)?
    else {
        return Err(AuthorityError::Fenced);
    };
    // Before fencing: the answer to a lost response must not depend on what happened since.
    if let Some(completion) = &attempt.completion {
        if *completion != hash {
            return Err(AuthorityError::CompletionConflict);
        }
        return Ok(CompleteResponse {
            attempt: attempt.id,
            status: "recorded".into(),
            state: Some(state),
            drained,
            replayed: true,
            drain_recorded: false,
        });
    }
    // Still live but no longer in this credential's scope: its evidence is not trusted, so it is
    // recorded through the one result writer as an outcome without evidence, which releases the
    // capacity and honours a recorded stop (gate P3-C).
    if holding(&transaction, scope, &attempt, &state)?.is_none()
        && holding(&transaction, &Scope::all(), &attempt, &state)?.is_some()
    {
        let unrecorded = CompleteRequest {
            outcome: Outcome::Lost,
            reason: Some("out-of-scope".into()),
            exit_code: 125,
            evidence: json!({"error":"the executor's grant no longer covers this action"}),
            log: None,
            cleanup_log: None,
            artifacts: BTreeMap::new(),
            ..request.clone()
        };
        let state = record_result(&transaction, &unrecorded, &hash)?;
        transaction.commit()?;
        return Ok(CompleteResponse {
            attempt: attempt.id,
            status: "fenced".into(),
            state: Some(state.into()),
            drained: request.drained,
            replayed: false,
            drain_recorded: request.drained,
        });
    }
    let Some((action, _)) = holding(&transaction, scope, &attempt, &state)? else {
        // A2: the holder of an expired lease may still report that its attempt drained. A late
        // verdict is never recorded, so it leaves the action for an operator; without one (nothing
        // ran, the supervisor was lost, the job was interrupted) the action is queued again. Work
        // nobody wants is canceled either way.
        if state == "interrupted" && !drained && request.drained {
            let detail = json!({"executor":executor,"outcome":request.outcome,
                                "exit_code":request.exit_code});
            drain_lapsed(&transaction, &attempt, verdictless(request), detail)?;
            transaction.commit()?;
            return Ok(CompleteResponse {
                attempt: attempt.id,
                status: "fenced".into(),
                state: Some(state),
                drained: true,
                replayed: false,
                drain_recorded: true,
            });
        }
        return Err(AuthorityError::Fenced);
    };
    let evidence = verified_evidence(request, &attempt, &action)?;
    if let Outputs::Staged(directory) = outputs {
        let target = root.join("attempts").join(&attempt.id);
        if fs::symlink_metadata(&target).is_ok() {
            // Left by a commit that failed after the move; this completion replaces it.
            let stale = root.join(format!(".incoming-stale-{}", unique()));
            fs::rename(&target, &stale).context("set aside stale attempt outputs")?;
            fs::remove_dir_all(&stale).context("remove stale attempt outputs")?;
        }
        fs::rename(directory, &target).context("move staged attempt outputs")?;
    }
    let recorded = CompleteRequest {
        evidence,
        ..request.clone()
    };
    let state = record_result(&transaction, &recorded, &hash)?;
    transaction.commit()?;
    Ok(CompleteResponse {
        attempt: attempt.id,
        status: "recorded".into(),
        state: Some(state.into()),
        drained: request.drained,
        replayed: false,
        drain_recorded: false,
    })
}

/// A11: identity fields come from the store, and the blobs a completion names must be the ones
/// its evidence names. Reports are accepted: executors are trusted like the local worker.
fn verified_evidence(
    request: &CompleteRequest,
    attempt: &Attempt,
    action: &Action,
) -> Result<Value, AuthorityError> {
    let invalid = |message: String| Err(AuthorityError::InvalidEvidence(message));
    let mut evidence = request.evidence.clone();
    let Some(fields) = evidence.as_object_mut() else {
        return invalid("Evidence must be a JSON object.".into());
    };
    for (field, value) in [
        ("revision", json!(action.revision)),
        ("profile", json!(action.profile)),
        ("job", json!(action.job)),
        ("identity", crate::worker::evidence_identity(action)),
        (
            "executor",
            json!(attempt.executor.as_deref().unwrap_or(LOCAL)),
        ),
    ] {
        fields.insert(field.into(), value);
    }
    let mut total = 0;
    for blob in [&request.log, &request.cleanup_log].into_iter().flatten() {
        if blob.bytes > api::MAX_ARTIFACT_BYTES {
            return invalid(format!("A log exceeds {} bytes.", api::MAX_ARTIFACT_BYTES));
        }
        total += blob.bytes;
    }
    for (path, entry) in &request.artifacts {
        if let Err(error) = source::relative_path(path) {
            return invalid(format!("{error:#}"));
        }
        if entry.mode > 0o777 {
            return invalid(format!("Artifact {path} has mode {:o}.", entry.mode));
        }
        if entry.bytes > api::MAX_ARTIFACT_BYTES {
            return invalid(format!(
                "Artifact {path} exceeds {} bytes.",
                api::MAX_ARTIFACT_BYTES
            ));
        }
        total += entry.bytes;
    }
    if total > api::MAX_ATTEMPT_OUTPUT_BYTES {
        return invalid(format!(
            "Attempt outputs exceed {} bytes.",
            api::MAX_ATTEMPT_OUTPUT_BYTES
        ));
    }
    if request.outcome == Outcome::Completed {
        // A failure after the command keeps the command's own code beside the error.
        let failed_after = ["error", "cleanup_error"]
            .iter()
            .any(|field| fields.get(*field).is_some_and(|v| !v.is_null()));
        let implied = if failed_after {
            Some(125)
        } else {
            fields.get("exit_code").and_then(Value::as_i64)
        };
        if implied != Some(i64::from(request.exit_code)) {
            return invalid("Evidence exit_code does not match the completion.".into());
        }
        if let Some(log) = fields.get("log_sha256").filter(|v| !v.is_null())
            && request.log.as_ref().map(|blob| blob.sha256.as_str()) != log.as_str()
        {
            return invalid("Evidence log_sha256 does not match the completion's log.".into());
        }
        let artifacts: BTreeMap<String, ArtifactEntry> = match fields.get("artifacts") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(value) => match serde_json::from_value(value.clone()) {
                Ok(artifacts) => artifacts,
                Err(_) => return invalid("Evidence artifacts are malformed.".into()),
            },
        };
        if artifacts != request.artifacts {
            return invalid("Evidence artifacts do not match the completion.".into());
        }
    }
    Ok(evidence)
}

/// Attempts without a verdict (not started, lost or lapsed) an action may requeue before it waits
/// for an operator (gate P3-2).
const REQUEUE_BUDGET: i64 = 3;

/// The action state after an attempt that gave no verdict: canceled once drained if nobody wants
/// the work (P3-C), queued again if drained within the budget, else interrupted for an operator.
fn unsettled(db: &Connection, attempt: &Attempt, drained: bool) -> Result<&'static str> {
    // Requeueing needs drained resources and a budget left: a failure that repeats on every host
    // ends interrupted for an operator instead of looping. The budget counts requeues since the
    // action's last retry only (gate P3-D); `retry` resets it.
    let requeued: i64 = db
        .query_row(
            "SELECT count FROM requeues WHERE action=?",
            [&attempt.action],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    // A recorded no-requesters stop, or no requester left: a cancel of an action that is no
    // longer running records no stop, and queued work nobody requests is never claimed.
    let requested: i64 = db.query_row(
        "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
        [&attempt.action],
        |r| r.get(0),
    )?;
    let unwanted = attempt.stop.as_deref() == Some("no-requesters") || requested == 0;
    Ok(if drained && unwanted {
        "canceled"
    } else if drained && requeued < REQUEUE_BUDGET {
        "queued"
    } else {
        "interrupted"
    })
}

/// Moves the action of its current attempt to `state`; `queued` frees it for another claim and
/// spends one requeue.
fn settle_action(db: &Connection, action: &str, attempt: &str, state: &str) -> Result<()> {
    if state == "queued" {
        db.execute(
            "UPDATE actions SET state='queued',attempt=NULL WHERE id=? AND attempt=?",
            params![action, attempt],
        )?;
        db.execute(
            "INSERT INTO requeues VALUES (?,1) ON CONFLICT(action) DO UPDATE SET count=count+1",
            [action],
        )?;
    } else {
        db.execute(
            "UPDATE actions SET state=? WHERE id=? AND attempt=?",
            params![state, action, attempt],
        )?;
    }
    Ok(())
}

/// Drains an attempt whose lease lapsed (A2, or a claim its executor never recorded) and records
/// `detail` with whether it was requeued. An action still waiting on it that nobody wants is
/// canceled; otherwise, without a verdict, it goes where a lost supervisor's would, and with one
/// it stays interrupted.
fn drain_lapsed(
    db: &Connection,
    attempt: &Attempt,
    verdictless: bool,
    mut detail: Value,
) -> Result<()> {
    db.execute("UPDATE attempts SET drained=1 WHERE id=?", [&attempt.id])?;
    let waiting: i64 = db.query_row(
        "SELECT COUNT(*) FROM actions WHERE id=? AND attempt=? AND state='interrupted'",
        params![attempt.action, attempt.id],
        |r| r.get(0),
    )?;
    let action_state = if waiting > 0 {
        let state = unsettled(db, attempt, true)?;
        // Nobody wanting the work decides before any verdict does.
        (state == "canceled" || verdictless).then_some(state)
    } else {
        None
    };
    if let Some(state) = action_state {
        settle_action(db, &attempt.action, &attempt.id, state)?;
    }
    detail["requeued"] = json!(action_state == Some("queued"));
    event(db, "attempt.drain_reported", &attempt.id, &detail)?;
    Ok(())
}

/// Stops after which the action may simply be retried: nothing about the revision failed.
const INTERRUPTIONS: [&str; 4] = ["supervisor-stop", "partition", "fenced", "revoked"];

/// Whether a report says nothing about the revision, so its drained action may be queued again:
/// nothing ran, the supervisor was lost, or an interruption stopped the command (one that failed
/// on its own first keeps its exit, gate P3-2). A result that could not be delivered may hide a
/// pass or a failure. The one judge for reports under a held lease and late ones (gate P2-2).
fn verdictless(request: &CompleteRequest) -> bool {
    match request.outcome {
        Outcome::Completed => {
            request.exit_code != 0
                && request
                    .reason
                    .as_deref()
                    .is_some_and(|reason| INTERRUPTIONS.contains(&reason))
        }
        Outcome::Undeliverable => false,
        Outcome::NotStarted | Outcome::Lost => true,
    }
}

/// The one writer of a result: every finished attempt, local or remote, ends here.
/// `request.evidence` is recorded as given; `hash` identifies the completion for replays.
pub fn record_result(
    db: &Connection,
    request: &CompleteRequest,
    hash: &str,
) -> Result<&'static str> {
    let mut attempt: Attempt = record(db, "attempts", &request.attempt)?;
    let action: Action = record(db, "actions", &attempt.action)?;
    let current: Option<String> = db.query_row(
        "SELECT attempt FROM actions WHERE id=?",
        [&action.id],
        |r| r.get(0),
    )?;
    ensure!(
        current.as_deref() == Some(attempt.id.as_str()),
        "attempt result was fenced"
    );
    let evidence = &request.evidence;
    let reason = request.reason.as_deref();
    let executor = attempt.executor.as_deref().unwrap_or(LOCAL);
    let unsettled = unsettled(db, &attempt, request.drained)?;
    let requeue = unsettled == "queued";
    let (state, action_state, kind, detail) = match request.outcome {
        Outcome::Completed => {
            let (state, action_state) = if request.exit_code == 0 {
                ("passed", "passed")
            } else if reason == Some("no-requesters") {
                ("canceled", "canceled")
            } else if verdictless(request) {
                // An interruption ends like a lost supervisor, whether or not the lease lapsed.
                if unsettled == "canceled" {
                    ("canceled", "canceled")
                } else {
                    ("interrupted", unsettled)
                }
            } else {
                ("failed", "failed")
            };
            let detail = json!({"state":state,"evidence":evidence,
                                "requeued":action_state == "queued"});
            (state, action_state, "attempt.completed", detail)
        }
        Outcome::NotStarted => {
            let (state, action_state) = match reason {
                Some("no-requesters") => ("canceled", "canceled"),
                // Drained work nobody wants, whatever stopped it, as for a lost supervisor.
                _ if unsettled == "canceled" => ("canceled", "canceled"),
                // Nothing of the attempt ran, so tool drift or an infrastructure failure (setup,
                // materialize, launch, registration) says nothing about the revision: the action
                // waits for another claim. Undrained resources keep it held until confirmed, and
                // a bounded budget stops a deterministic failure from requeueing forever.
                _ if requeue => ("interrupted", "queued"),
                _ => ("interrupted", "interrupted"),
            };
            let detail = json!({"reason":reason,"error":evidence["error"],
                                "requeued":action_state == "queued"});
            (state, action_state, "attempt.launch_failed", detail)
        }
        // A lost supervisor reported drained never started its command (gate P3-2); an attempt
        // whose result is not recorded (out of scope, P3-C) reports this outcome with its reason.
        Outcome::Lost => {
            let state = if unsettled == "canceled" {
                "canceled"
            } else {
                "interrupted"
            };
            let action_state = unsettled;
            let detail = json!({"drained":request.drained,"operator_confirmed":false,
                                "reason":reason.unwrap_or("supervisor-lost"),"executor":executor,
                                "requeued":action_state == "queued"});
            (state, action_state, "attempt.interrupted", detail)
        }
        Outcome::Undeliverable => (
            "interrupted",
            "interrupted",
            "attempt.interrupted",
            json!({"drained":request.drained,"reason":"undeliverable",
                   "error":evidence["error"],"executor":executor}),
        ),
    };
    attempt.ended = Some(now());
    attempt.exit_code = Some(request.exit_code);
    attempt.result = Some(evidence.clone());
    record_manifest_size(db, &attempt.id, evidence)?;
    attempt.completion = Some(hash.into());
    db.execute(
        "UPDATE attempts SET state=?,drained=?,body=? WHERE id=?",
        params![
            state,
            request.drained,
            serde_json::to_string(&attempt)?,
            attempt.id
        ],
    )?;
    settle_action(db, &action.id, &attempt.id, action_state)?;
    if state == "failed" {
        db.execute(
            "INSERT INTO repairs VALUES (?,?,?,'execution','open',?,NULL,?)",
            params![
                unique(),
                action.revision,
                action.id,
                evidence.to_string(),
                now()
            ],
        )?;
    }
    event(db, kind, &attempt.id, &detail)?;
    Ok(state)
}

/// A held attempt whose lease ran out, with the expiry applied after the API's startup grace.
struct Lapsed {
    attempt: String,
    action: String,
    executor: String,
    lease_expires: f64,
    effective: f64,
}

/// Interrupts attempts whose executor stopped renewing. Only the API process expires leases.
pub fn expire(store: &mut Store, now: f64, api_start: f64) -> Result<Vec<String>> {
    let mut expired = Vec::new();
    for lease in lapsed(&store.db, now, api_start)? {
        if expire_lease(store, &lease)? {
            expired.push(lease.attempt);
        }
    }
    Ok(expired)
}

/// A restarted API first gives every executor one full lease to renew, so the API's own
/// downtime never expires healthy work. Attempts without a lease predate executors.
fn lapsed(db: &Connection, now: f64, api_start: f64) -> Result<Vec<Lapsed>> {
    let rows: Vec<(String, String, String, f64)> = {
        let mut stmt = db.prepare(&format!(
            "SELECT id,action,{EXECUTOR},json_extract(body,'$.lease_expires') FROM attempts
             WHERE drained=0 AND state IN ('claimed','running')
             AND json_extract(body,'$.lease_expires') IS NOT NULL"
        ))?;
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
            .collect::<rusqlite::Result<_>>()?
    };
    let mut leases = BTreeMap::new();
    let mut due = Vec::new();
    for (attempt, action, executor, lease_expires) in rows {
        let lease_seconds = match leases.entry(executor.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => *entry
                .insert(load(db, &executor)?.map_or(DEFAULT_LEASE_SECONDS, |r| r.lease_seconds)),
        };
        let effective = lease_expires.max(api_start + lease_seconds as f64);
        if now > effective {
            due.push(Lapsed {
                attempt,
                action,
                executor,
                lease_expires,
                effective,
            });
        }
    }
    Ok(due)
}

/// Expiry never drains: the store cannot know whether anything ran. A renewal that moved the
/// lease after it was read wins.
fn expire_lease(store: &mut Store, lapsed: &Lapsed) -> Result<bool> {
    let transaction = store.transaction()?;
    let expired = transaction.execute(
        "UPDATE attempts SET state='interrupted',drained=0 WHERE id=?
         AND state IN ('claimed','running') AND json_extract(body,'$.lease_expires')=?",
        params![lapsed.attempt, lapsed.lease_expires],
    )? == 1;
    if expired {
        transaction.execute(
            "UPDATE actions SET state='interrupted' WHERE id=? AND attempt=?",
            params![lapsed.action, lapsed.attempt],
        )?;
        event(
            &transaction,
            "attempt.lease_expired",
            &lapsed.attempt,
            &json!({"executor":lapsed.executor,"lease_expires":lapsed.lease_expires,
                    "effective":lapsed.effective}),
        )?;
    }
    transaction.commit()?;
    Ok(expired)
}

/// An operator's assertion that an attempt's processes and external resources are gone (A3).
/// It releases the attempt's capacity and keeps its state, result and action state.
pub fn confirm_drained(store: &mut Store, id: &str) -> Result<()> {
    let transaction = store.transaction()?;
    let (state, drained, body): (String, bool, String) = transaction
        .query_row(
            "SELECT state,drained,body FROM attempts WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
        .with_context(|| format!("unknown attempt: {id}"))?;
    ensure!(!drained, "attempt is already drained");
    let attempt: Attempt = serde_json::from_str(&body)?;
    let executor = attempt.executor.as_deref().unwrap_or(LOCAL);
    if executor == LOCAL
        && let (Some(pid), Some(start)) = (attempt.supervisor_pid, &attempt.supervisor_start)
    {
        ensure!(
            !crate::worker::alive(pid, start),
            "supervisor is still alive; cannot confirm drain"
        );
    }
    if ["claimed", "running"].contains(&state.as_str()) {
        ensure!(
            attempt.lease.is_none(),
            "attempt is held by executor {executor} until its coordinator reports it or its lease expires"
        );
        // A dead supervisor of the previous binary never reports.
        transaction.execute(
            "UPDATE attempts SET state='interrupted',drained=1 WHERE id=?",
            [id],
        )?;
        transaction.execute(
            "UPDATE actions SET state='interrupted' WHERE id=? AND attempt=?",
            params![attempt.action, id],
        )?;
    } else {
        ensure!(
            ["interrupted", "failed", "canceled", "passed"].contains(&state.as_str()),
            "attempt in state {state} cannot be confirmed drained"
        );
        transaction.execute("UPDATE attempts SET drained=1 WHERE id=?", [id])?;
    }
    if ["failed", "canceled", "passed"].contains(&state.as_str()) {
        event(
            &transaction,
            "attempt.drain_confirmed",
            id,
            &json!({"state":state,"operator_confirmed":true}),
        )?;
    } else {
        event(
            &transaction,
            "attempt.interrupted",
            id,
            &json!({"drained":true,"operator_confirmed":true}),
        )?;
    }
    transaction.commit()?;
    Ok(())
}

/// `executor-set`: unset fields keep their value; a placement list replaces the current one.
#[derive(Debug, Clone, Default)]
pub struct ConfigureRequest {
    pub slots: Option<u32>,
    pub memory_mb: Option<u32>,
    pub lease_seconds: Option<u64>,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
    /// Back to the default placement before `allow` and `deny` apply.
    pub clear_placement: bool,
}

/// MUST-FIX 8: a remote executor runs only the jobs an operator placed on it by name. The local
/// executor admits every job unless it is given an allowlist.
fn default_placement(id: &str) -> Placement {
    Placement {
        allow: (id != LOCAL).then(BTreeSet::new),
        deny: BTreeSet::new(),
    }
}

/// Creates or updates an executor. The local executor's slots and memory are the store's
/// settings, the budget `init`, `status` and profile registration already read.
pub fn configure(store: &mut Store, id: &str, request: &ConfigureRequest) -> Result<Value> {
    ensure!(!id.is_empty() && id.len() <= 128, "invalid executor name");
    if let Some(slots) = request.slots {
        ensure!((1..=64).contains(&slots), "slots must be 1–64");
    }
    if let Some(memory_mb) = request.memory_mb {
        ensure!(memory_mb >= 128, "memory must be at least 128 MiB");
    }
    if let Some(lease_seconds) = request.lease_seconds {
        ensure!(
            (3..=3600).contains(&lease_seconds),
            "lease must be 3–3600 seconds"
        );
    }
    ensure!(
        request
            .allow
            .iter()
            .chain(&request.deny)
            .all(|job| !job.is_empty()),
        "job names must be nonempty"
    );
    prepare(&store.db)?;
    let transaction = store.transaction()?;
    let existing = load(&transaction, id)?;
    let created = existing.is_none();
    let mut record = match existing {
        Some(record) => record,
        None => {
            ensure!(
                request.slots.is_some() && request.memory_mb.is_some(),
                "a new executor needs --slots and --memory-mb"
            );
            ExecutorRecord {
                placement: default_placement(id),
                ..default_record(id)
            }
        }
    };
    if id == LOCAL {
        for (key, value) in [("slots", request.slots), ("memory_mb", request.memory_mb)] {
            if let Some(value) = value {
                transaction.execute(
                    "UPDATE settings SET value=? WHERE key=?",
                    params![value.to_string(), key],
                )?;
            }
        }
    } else {
        record.slots = request.slots.or(record.slots);
        record.memory_mb = request.memory_mb.or(record.memory_mb);
    }
    record.lease_seconds = request.lease_seconds.unwrap_or(record.lease_seconds);
    if request.clear_placement {
        record.placement = default_placement(id);
    }
    if !request.allow.is_empty() {
        record.placement.allow = Some(request.allow.iter().cloned().collect());
    }
    if !request.deny.is_empty() {
        record.placement.deny = request.deny.iter().cloned().collect();
    }
    save(&transaction, &record)?;
    let view = view(&transaction, &record)?;
    let configured = &view["configured"];
    event(
        &transaction,
        "executor.configured",
        id,
        &json!({"created":created,"slots":configured["slots"],
                "memory_mb":configured["memory_mb"],"lease_seconds":record.lease_seconds,
                "placement":record.placement}),
    )?;
    transaction.commit()?;
    Ok(view)
}

/// Every executor with its budget, what it holds and what its coordinator last reported.
pub fn list(store: &Store) -> Result<Value> {
    prepare(&store.db)?;
    let records: Vec<ExecutorRecord> = {
        let mut stmt = store.db.prepare("SELECT body FROM executors ORDER BY id")?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .map(|body| Ok(serde_json::from_str(&body?)?))
            .collect::<Result<_>>()?
    };
    Ok(Value::Array(
        records
            .iter()
            .map(|record| view(&store.db, record))
            .collect::<Result<_>>()?,
    ))
}

fn view(db: &Connection, record: &ExecutorRecord) -> Result<Value> {
    let (slots, memory_mb) = if record.id == LOCAL {
        let (slots, memory_mb) = store::capacity(db)?;
        (Some(slots), Some(memory_mb))
    } else {
        (record.slots, record.memory_mb)
    };
    // A record written without capacity is still listed; it can claim nothing.
    let effective = capacity(db, record).ok();
    let held = held(db, &record.id)?;
    let attempts: Vec<Value> = {
        let mut stmt = db.prepare(&format!(
            "SELECT id,state,action FROM attempts WHERE drained=0 AND {EXECUTOR}=? ORDER BY rowid"
        ))?;
        stmt.query_map([&record.id], |r| {
            Ok(
                json!({"id":r.get::<_, String>(0)?,"state":r.get::<_, String>(1)?,
                      "action":r.get::<_, String>(2)?}),
            )
        })?
        .collect::<rusqlite::Result<_>>()?
    };
    Ok(
        json!({"id":record.id,"configured":{"slots":slots,"memory_mb":memory_mb},
        "effective":effective,
        "held":{"slots":held.slots,"memory_mb":held.memory_mb,"attempts":attempts},
        "lease_seconds":record.lease_seconds,"placement":record.placement,
        "observed":record.observed}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> (tempfile::TempDir, Store) {
        let root = tempfile::tempdir().unwrap();
        let store = Store::init(root.path(), 4, 4096).unwrap();
        (root, store)
    }

    fn rows(db: &Connection) -> Vec<(String, String)> {
        let mut stmt = db
            .prepare("SELECT id,body FROM executors ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    #[test]
    fn prepare_is_idempotent_and_seeds_local() {
        let (_root, store) = empty();
        // A store created before executors existed reads as the local executor only.
        assert_eq!(load(&store.db, LOCAL).unwrap().unwrap().id, LOCAL);
        assert!(load(&store.db, "mindy").unwrap().is_none());
        prepare(&store.db).unwrap();
        let seeded = rows(&store.db);
        assert_eq!(seeded.len(), 1);
        let local: ExecutorRecord = serde_json::from_str(&seeded[0].1).unwrap();
        assert_eq!(
            (local.id.as_str(), local.slots, local.memory_mb),
            (LOCAL, None, None)
        );
        assert_eq!(local.lease_seconds, LOCAL_LEASE_SECONDS);
        assert_eq!(local.placement, Placement::default());
        assert_eq!(local.observed, Observed::default());
        let mindy = ExecutorRecord {
            slots: Some(2),
            memory_mb: Some(2048),
            ..default_record("mindy")
        };
        save(&store.db, &mindy).unwrap();
        let configured = rows(&store.db);
        prepare(&store.db).unwrap();
        prepare(&store.db).unwrap();
        assert_eq!(rows(&store.db), configured);
        assert_eq!(configured[0].1, seeded[0].1);
        assert_eq!(load(&store.db, "mindy").unwrap(), Some(mindy));
        let indexes: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index'
                 AND name IN ('attempts_held','attempts_claim_key')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 2);
        // The claim replay lookup must hit the expression index, not scan every attempt.
        let plan: String = store
            .db
            .query_row(
                &format!(
                    "EXPLAIN QUERY PLAN SELECT id FROM attempts
                     WHERE json_extract(body,'$.claim_key')=?1 AND {EXECUTOR}=?2"
                ),
                params!["key", LOCAL],
                |r| r.get(3),
            )
            .unwrap();
        assert!(plan.contains("attempts_claim_key"), "{plan}");
        // The API boundary prepares the same table.
        let (_other, fresh) = empty();
        crate::api::Api::open(&fresh.root).unwrap();
        assert_eq!(rows(&fresh.db).len(), 1);
    }

    #[test]
    fn executor_record_defaults_round_trip() {
        // Only identity, lease and creation are required; capacity, placement and observations default.
        let minimal: ExecutorRecord =
            serde_json::from_value(json!({"id":"mindy","lease_seconds":300,"created":1.5}))
                .unwrap();
        assert_eq!((minimal.slots, minimal.memory_mb), (None, None));
        assert_eq!(minimal.placement, Placement::default());
        assert_eq!(minimal.observed, Observed::default());
        let text = serde_json::to_string(&minimal).unwrap();
        assert_eq!(
            serde_json::from_str::<ExecutorRecord>(&text).unwrap(),
            minimal
        );
        let local = default_record(LOCAL);
        assert_eq!(
            serde_json::from_str::<ExecutorRecord>(&serde_json::to_string(&local).unwrap())
                .unwrap(),
            local
        );
        let full = ExecutorRecord {
            id: "mindy".into(),
            slots: Some(3),
            memory_mb: Some(12288),
            lease_seconds: 60,
            placement: Placement {
                allow: Some(["test".to_owned()].into()),
                deny: ["prepare".to_owned(), "images".to_owned()].into(),
            },
            created: 1_792_511_574.900_662_7,
            observed: Observed {
                last_seen: Some(1_792_511_600.25),
                verified: ["f-1".to_owned()].into(),
                refused: [("f-2".to_owned(), "tool drift".to_owned())].into(),
                memory_limit_mb: Some(10240),
                disk_free_mb: Some(70_000),
                host: Some("mindy".into()),
                engine: Some("e".repeat(64)),
                loopback: None,
            },
            holding: None,
        };
        let text = serde_json::to_string(&full).unwrap();
        assert_eq!(serde_json::from_str::<ExecutorRecord>(&text).unwrap(), full);
        // Configuration typos fail loudly instead of silently widening placement or capacity.
        for typo in [
            json!({"id":"x","lease_seconds":300,"created":0.0,"slot":2}),
            json!({"id":"x","lease_seconds":300,"created":0.0,"placement":{"alow":["test"]}}),
        ] {
            assert!(serde_json::from_value::<ExecutorRecord>(typo).is_err());
        }
    }

    /// Two projects, one revision each, profiles `f-1`..`f-3` (jobs `build`, `test`) and
    /// `f-wide` whose `wide` job reserves two slots. Local capacity is 4 slots, 4096 MiB.
    fn lab() -> (tempfile::TempDir, Store) {
        let (root, store) = empty();
        prepare(&store.db).unwrap();
        store
            .db
            .execute_batch(
                "INSERT INTO projects VALUES ('p-a','alpha','/dev/null'),('p-b','beta','/dev/null')",
            )
            .unwrap();
        for (id, project) in [("r-a", "p-a"), ("r-b", "p-b")] {
            let revision = json!({"id":id,"project":project,"commit":"c".repeat(40),"tree":"t",
                "parents":[],"owner":"author","message":"m","created":0.0});
            store
                .db
                .execute(
                    "INSERT INTO revisions VALUES (?,?,?)",
                    params![id, project, revision.to_string()],
                )
                .unwrap();
        }
        for id in ["f-1", "f-2", "f-3", "f-wide"] {
            let profile = json!({"id":id,"engine":"e","platform":"linux-x86_64","tools":{},
                "definition":{"version":1,"name":id,"tools":{},"jobs":[
                    {"name":"build","command":["sh"]},{"name":"test","command":["sh"]},
                    {"name":"wide","command":["sh"],"slots":2,"memory_mb":512,"timeout_seconds":90}]}});
            crate::store::insert(&store.db, "profiles", id, &profile).unwrap();
        }
        (root, store)
    }

    struct Queue<'a> {
        id: &'a str,
        revision: &'a str,
        profile: &'a str,
        job: &'a str,
        owner: &'a str,
        created: f64,
        /// None writes a legacy action whose reservation and timeout come from its profile.
        reservation: Option<(u32, u32)>,
        dependencies: &'a [&'a str],
    }

    impl Default for Queue<'_> {
        fn default() -> Self {
            Self {
                id: "",
                revision: "r-a",
                profile: "f-1",
                job: "test",
                owner: "lane",
                created: 1.0,
                reservation: Some((1, 512)),
                dependencies: &[],
            }
        }
    }

    fn queue(db: &Connection, q: Queue) {
        let action = Action {
            id: q.id.into(),
            key: q.id.into(),
            revision: q.revision.into(),
            profile: q.profile.into(),
            job: q.job.into(),
            dependencies: q.dependencies.iter().map(|d| d.to_string()).collect(),
            created: q.created,
            identity: q.reservation.map(|_| ActionIdentity {
                version: JOB_RECIPE.into(),
                recipe: "recipe".into(),
                class: "class".into(),
                timeout_seconds: 60,
            }),
            reservation: q
                .reservation
                .map(|(slots, memory_mb)| Reservation { slots, memory_mb }),
            origin: None,
            declared_inputs: None,
            declared_base: None,
        };
        db.execute(
            "INSERT INTO actions VALUES (?,?,'queued',NULL,?,?)",
            params![
                q.id,
                q.id,
                serde_json::to_string(&action).unwrap(),
                q.created
            ],
        )
        .unwrap();
        db.execute(
            "INSERT INTO interests VALUES (?,?,'',1)",
            params![q.id, q.owner],
        )
        .unwrap();
    }

    /// A held attempt: `executor` None writes a body from before executors existed.
    fn hold(db: &Connection, id: &str, executor: Option<&str>, slots: u32, memory_mb: u32) {
        let mut body = json!({"id":id,"action":"elsewhere","worker_pid":1,"worker_start":"1",
            "supervisor_pid":null,"supervisor_start":null,"slots":slots,"memory_mb":memory_mb,
            "created":0.0,"started":null,"ended":null,"exit_code":null,"result":null});
        if let Some(executor) = executor {
            body["executor"] = json!(executor);
        }
        db.execute(
            "INSERT INTO attempts VALUES (?,'elsewhere','running',0,?)",
            params![id, body.to_string()],
        )
        .unwrap();
    }

    fn register(db: &Connection, id: &str, slots: u32, memory_mb: u32) -> ExecutorRecord {
        let record = ExecutorRecord {
            slots: Some(slots),
            memory_mb: Some(memory_mb),
            ..default_record(id)
        };
        save(db, &record).unwrap();
        record
    }

    fn ask(key: &str, verified: &[&str]) -> ClaimRequest {
        ClaimRequest {
            claim_key: key.into(),
            verified: verified.iter().map(|v| v.to_string()).collect(),
            ..ClaimRequest::default()
        }
    }

    /// A claim by the local executor through the production path (gate P3-6): every registered
    /// profile verified, as the worker verifies them at start.
    fn local_claim(store: &mut Store) -> Option<Attempt> {
        let verified: Vec<String> = {
            let mut stmt = store.db.prepare("SELECT id FROM profiles").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let request = ClaimRequest {
            claim_key: unique(),
            verified,
            ..ClaimRequest::default()
        };
        let response = claim_for(store, LOCAL, &Scope::all(), &request).unwrap();
        response
            .claim
            .map(|claim| store.attempt(&claim.attempt.id).unwrap())
    }

    fn claimed(response: &ClaimResponse) -> Option<&str> {
        response.claim.as_ref().map(|c| c.action.id.as_str())
    }

    fn state(db: &Connection, action: &str) -> String {
        crate::engine::action_state(db, action).unwrap()
    }

    fn events(db: &Connection, kind: &str) -> Vec<(String, Value)> {
        let mut stmt = db
            .prepare("SELECT subject,detail FROM events WHERE kind=? ORDER BY seq")
            .unwrap();
        stmt.query_map([kind], |r| {
            Ok((
                r.get(0)?,
                serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    #[test]
    fn claim_for_counts_only_its_own_executor() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 1, 1024);
        // The local budget is fully held by an attempt from before executors existed.
        hold(&store.db, "legacy", None, 4, 4096);
        queue(
            &store.db,
            Queue {
                id: "first",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "second",
                created: 2.0,
                ..Queue::default()
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        let claim = response.claim.unwrap();
        assert_eq!(claim.action.id, "first");
        assert_eq!(
            (claim.revision.id.as_str(), claim.revision.project.as_str()),
            ("r-a", "p-a")
        );
        assert_eq!(claim.profile.id, "f-1");
        assert!(claim.dependencies.is_empty());
        let grant = &claim.attempt;
        assert_eq!(
            (grant.slots, grant.memory_mb, grant.timeout_seconds),
            (1, 512, 60)
        );
        let attempt = store.attempt(&grant.id).unwrap();
        assert_eq!(attempt.executor.as_deref(), Some("mindy"));
        assert_eq!(attempt.lease.as_deref(), Some(grant.lease.as_str()));
        assert_eq!(attempt.lease_expires, Some(grant.lease_expires));
        assert!((grant.lease_expires - now() - DEFAULT_LEASE_SECONDS as f64).abs() < 5.0);
        assert_eq!(attempt.claim_key.as_deref(), Some("k1"));
        assert_eq!((attempt.worker_pid, attempt.worker_start.as_str()), (0, ""));
        assert_eq!(attempt.timeout_seconds, Some(60));
        let row: (String, String) = store
            .db
            .query_row(
                "SELECT state,attempt FROM actions WHERE id='first'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("running".into(), grant.id.clone()));
        assert_eq!(
            store
                .db
                .query_row("SELECT state FROM attempts WHERE id=?", [&grant.id], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
            "claimed"
        );
        let claims = events(&store.db, "attempt.claimed");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].0, grant.id);
        assert_eq!(
            claims[0].1,
            json!({"action":"first","owner":"lane","executor":"mindy","slots":1,
                   "memory_mb":512,"timeout_seconds":60})
        );
        let held_by = |store: &Store, executor: &str| held(&store.db, executor).unwrap();
        assert_eq!(
            held_by(&store, "mindy"),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 4,
                memory_mb: 4096
            }
        );
        // mindy is full; its hold never reaches the local budget and vice versa.
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert!(response.claim.is_none() && response.fenced.is_none());
        assert_eq!(state(&store.db, "second"), "queued");
        assert!(local_claim(&mut store).is_none());
        store
            .db
            .execute("UPDATE attempts SET drained=1 WHERE id='legacy'", [])
            .unwrap();
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 0,
                memory_mb: 0
            }
        );
        let local = local_claim(&mut store).unwrap();
        assert_eq!(local.action, "second");
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
        assert_eq!(
            held_by(&store, "mindy"),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
    }

    #[test]
    fn claim_skips_actions_that_never_fit_the_executor() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 1, 1024);
        // Older than everything else: without the fit rule each would idle mindy forever.
        queue(
            &store.db,
            Queue {
                id: "heavy",
                created: 0.5,
                reservation: Some((1, 2048)),
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "wide",
                profile: "f-wide",
                job: "wide",
                reservation: None,
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "small",
                created: 2.0,
                ..Queue::default()
            },
        );
        let verified = ["f-1", "f-wide"];
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k", &verified)).unwrap();
        assert_eq!(claimed(&response), Some("small"));
        assert_eq!(state(&store.db, "heavy"), "queued");
        assert_eq!(state(&store.db, "wide"), "queued");
        // The legacy job's two slots fit the local executor, which still admits it in order.
        let local = default_record(LOCAL);
        let selection = select(
            &store.db,
            &local,
            &Scope::all(),
            &verified.map(String::from).into(),
            &BTreeSet::new(),
            capacity(&store.db, &local).unwrap(),
        )
        .unwrap();
        let head = selection.choice.unwrap();
        assert_eq!(head.action.id, "heavy");
        let heavy = local_claim(&mut store).unwrap();
        assert_eq!(heavy.action, "heavy");
        let wide = local_claim(&mut store).unwrap();
        assert_eq!(wide.action, "wide");
        assert_eq!(
            (wide.slots, wide.memory_mb, wide.timeout_seconds),
            (2, 512, Some(90))
        );
    }

    #[test]
    fn claim_still_drains_for_a_fitting_head_action() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 2, 2048);
        hold(&store.db, "busy", Some("mindy"), 1, 512);
        queue(
            &store.db,
            Queue {
                id: "wide",
                reservation: Some((2, 512)),
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "small",
                created: 2.0,
                ..Queue::default()
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        // The head fits mindy's total but not its free capacity: nothing smaller jumps ahead.
        assert!(response.claim.is_none() && response.unverified.is_empty());
        assert_eq!(state(&store.db, "small"), "queued");
        store
            .db
            .execute("UPDATE attempts SET drained=1 WHERE id='busy'", [])
            .unwrap();
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("wide"));
    }

    #[test]
    fn refusing_every_profile_claims_nothing() {
        let (_root, mut store) = lab();
        queue(
            &store.db,
            Queue {
                id: "work",
                ..Queue::default()
            },
        );
        let reason = "loopback listener 127.0.0.1:2375 not allowlisted";
        let mut request = ask("k1", &["f-1"]);
        request.refused.insert(REFUSE_ALL.into(), reason.into());
        let response = claim_for(&mut store, LOCAL, &Scope::all(), &request).unwrap();
        assert_eq!(claimed(&response), None);
        assert!(response.unverified.is_empty());
        assert_eq!(state(&store.db, "work"), "queued");
        let local = load(&store.db, LOCAL).unwrap().unwrap();
        assert_eq!(local.observed.refused[REFUSE_ALL], reason);
        assert_eq!(local.observed.verified, BTreeSet::from(["f-1".to_owned()]));
        // Once the executor stops declining, the same work is claimed.
        let response = claim_for(&mut store, LOCAL, &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("work"));
    }

    #[test]
    fn claim_filters_scope_verified_and_placement() {
        let (_root, mut store) = lab();
        let mut mindy = register(&store.db, "mindy", 4, 4096);
        mindy.placement.deny.insert("build".into());
        save(&store.db, &mindy).unwrap();
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "other-project",
                revision: "r-b",
                created: 1.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "other-profile",
                profile: "f-2",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "denied-job",
                job: "build",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "refused",
                profile: "f-3",
                created: 4.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "allowed",
                created: 5.0,
                ..base
            },
        );
        let scope = Scope {
            projects: Some(["p-a".to_owned()].into()),
            profiles: Some(["f-1".to_owned(), "f-3".to_owned()].into()),
        };
        // A verified profile the grant no longer covers is ignored, not refused: a narrowed grant
        // must not stop the executor (gate P3-3). Nothing outside the scope is claimed.
        let narrowed = claim_for(&mut store, "mindy", &scope, &ask("k0", &["f-1", "f-2"])).unwrap();
        // f-3 is in scope but not verified: it heads the queue and waits, as before.
        assert_eq!(claimed(&narrowed), None);
        assert_eq!(narrowed.unverified, ["f-3"]);
        let mut request = ask("k1", &["f-1"]);
        request.refused.insert("f-3".into(), "tool drift".into());
        let response = claim_for(&mut store, "mindy", &scope, &request).unwrap();
        assert_eq!(claimed(&response), Some("allowed"));
        assert!(response.unverified.is_empty());
        for id in ["other-project", "other-profile", "denied-job", "refused"] {
            assert_eq!(state(&store.db, id), "queued", "{id}");
        }
        // Placement: deny wins over allow, and an allow list admits only the jobs it names.
        let placement = Placement {
            allow: Some(["build".to_owned(), "test".to_owned()].into()),
            deny: ["build".to_owned()].into(),
        };
        assert!(placement.permits("test") && !placement.permits("build"));
        assert!(!placement.permits("wide") && Placement::default().permits("wide"));
        // The executor's report is recorded for operators.
        let observed = load(&store.db, "mindy").unwrap().unwrap().observed;
        assert_eq!(observed.verified, BTreeSet::from(["f-1".to_owned()]));
        assert_eq!(observed.refused["f-3"], "tool drift");
        assert!(observed.last_seen.is_some());
        // The narrowed claim reported too; only verified profiles in scope were recorded.
        let reports = events(&store.db, "executor.observed");
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].1["verified"], json!(["f-1"]));
        assert_eq!(
            reports[1].1,
            json!({"verified":["f-1"],"refused":{"f-3":"tool drift"}})
        );
    }

    #[test]
    fn claim_reports_unverified_profiles() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        let base = Queue::default();
        queue(&store.db, Queue { id: "head", ..base });
        queue(
            &store.db,
            Queue {
                id: "later",
                profile: "f-2",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "again",
                profile: "f-2",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "third",
                profile: "f-3",
                created: 4.0,
                ..base
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("head"));
        assert_eq!(response.unverified, ["f-2", "f-3"]);
        // Reports stay bounded however many profiles wait.
        let profiles: Vec<String> = (0..40).map(|n| format!("u-{n:02}")).collect();
        for (n, profile) in profiles.iter().enumerate() {
            let id = format!("many-{n}");
            let created = 10.0 + n as f64;
            queue(
                &store.db,
                Queue {
                    id: &id,
                    profile: profile.as_str(),
                    created,
                    ..base
                },
            );
        }
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert!(response.claim.is_none());
        assert_eq!(response.unverified.len(), 32);
        assert_eq!(response.unverified[..3], ["f-2", "f-3", "u-00"]);
    }

    #[test]
    fn unverified_head_blocks_admission_until_verified() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "head",
                profile: "f-2",
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "later",
                created: 2.0,
                ..base
            },
        );
        // Verification lag must not reorder admission: nothing runs until the head is judged.
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert!(response.claim.is_none());
        assert_eq!(response.unverified, ["f-2"]);
        assert_eq!(state(&store.db, "later"), "queued");
        // A refused profile is skipped rather than waited for.
        let mut refused = ask("k2", &["f-1"]);
        refused.refused.insert("f-2".into(), "tool drift".into());
        let response = claim_for(&mut store, "mindy", &Scope::all(), &refused).unwrap();
        assert_eq!(claimed(&response), Some("later"));
        assert!(response.unverified.is_empty());
        queue(
            &store.db,
            Queue {
                id: "after",
                created: 3.0,
                ..base
            },
        );
        let response = claim_for(
            &mut store,
            "mindy",
            &Scope::all(),
            &ask("k3", &["f-1", "f-2"]),
        )
        .unwrap();
        assert_eq!(claimed(&response), Some("head"));
    }

    #[test]
    fn claim_key_replays_held_attempt_or_reports_fenced() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queue(
            &store.db,
            Queue {
                id: "first",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "second",
                created: 2.0,
                ..Queue::default()
            },
        );
        for key in [
            String::new(),
            "k".repeat(129),
            "has space".into(),
            "naïve".into(),
        ] {
            let result = claim_for(&mut store, "mindy", &Scope::all(), &ask(&key, &["f-1"]));
            assert!(matches!(result, Err(AuthorityError::Invalid(_))), "{key:?}");
        }
        let ghost = claim_for(&mut store, "ghost", &Scope::all(), &ask("k1", &["f-1"]));
        assert!(matches!(ghost, Err(AuthorityError::Unregistered)));
        let first = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"]))
            .unwrap()
            .claim
            .unwrap();
        // A lost response: the same key returns the same grant instead of a second attempt.
        let replay = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"]))
            .unwrap()
            .claim
            .unwrap();
        assert_eq!(replay.attempt, first.attempt);
        assert_eq!(replay.action.id, "first");
        assert_eq!(state(&store.db, "second"), "queued");
        assert_eq!(events(&store.db, "attempt.claimed").len(), 1);
        store
            .db
            .execute(
                "UPDATE attempts SET state='running' WHERE id=?",
                [&first.attempt.id],
            )
            .unwrap();
        let replay = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(replay.claim.unwrap().attempt, first.attempt);
        // Keys belong to one executor: another executor's identical key claims fresh work.
        let local = claim_for(&mut store, LOCAL, &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&local), Some("second"));
        store
            .db
            .execute(
                "UPDATE attempts SET state='interrupted' WHERE id=?",
                [&first.attempt.id],
            )
            .unwrap();
        let fenced = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert!(fenced.claim.is_none());
        assert_eq!(fenced.fenced.as_deref(), Some(first.attempt.id.as_str()));
        assert_eq!(events(&store.db, "attempt.claimed").len(), 2);
    }

    #[test]
    fn claim_order_is_priority_then_fair_share() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 8, 8192);
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "lane",
                owner: "lane",
                created: 1.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "alice-1",
                owner: "alice",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "alice-2",
                owner: "alice",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "bob-1",
                owner: "bob",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "bob-2",
                owner: "bob",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "carol",
                owner: "carol",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "train",
                owner: "train",
                created: 4.0,
                ..base
            },
        );
        store
            .db
            .execute_batch("INSERT INTO dispatches VALUES ('lane',100.0),('alice',50.0)")
            .unwrap();
        crate::worker::set_priority(&store, "train", 10).unwrap();
        let remote = |store: &mut Store, key: &str| {
            let response = claim_for(store, "mindy", &Scope::all(), &ask(key, &["f-1"])).unwrap();
            claimed(&response).unwrap().to_owned()
        };
        let mut order = Vec::new();
        for key in ["k1", "k2", "k3"] {
            order.push(remote(&mut store, key));
        }
        // Fair share is global: local and remote claims are served from one owner history.
        order.push(local_claim(&mut store).unwrap().action);
        for key in ["k4", "k5", "k6"] {
            order.push(remote(&mut store, key));
        }
        assert_eq!(
            order,
            [
                "train", "bob-1", "carol", "alice-1", "lane", "bob-2", "alice-2"
            ]
        );
    }

    /// A train (priority 1) whose shard waits on its prepare, beside a lane's eligible job, on
    /// mindy (8 slots, 8192 MiB). `prepare` is the prepare action's state.
    fn train_beside_lane(
        prepare: &str,
        shard: (u32, u32),
        lane: (u32, u32),
    ) -> (tempfile::TempDir, Store) {
        let (root, store) = lab();
        register(&store.db, "mindy", 8, 8192);
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "prepare",
                owner: "train",
                job: "prepare",
                ..base
            },
        );
        store
            .db
            .execute("UPDATE actions SET state=? WHERE id='prepare'", [prepare])
            .unwrap();
        queue(
            &store.db,
            Queue {
                id: "shard",
                owner: "train",
                job: "vitest",
                created: 2.0,
                reservation: Some(shard),
                dependencies: &["prepare"],
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "lane",
                owner: "lane",
                created: 3.0,
                reservation: Some(lane),
                ..base
            },
        );
        crate::worker::set_priority(&store, "train", 1).unwrap();
        (root, store)
    }

    fn mindy_claims(store: &mut Store, key: &str) -> Option<String> {
        let response = claim_for(store, "mindy", &Scope::all(), &ask(key, &["f-1"])).unwrap();
        claimed(&response).map(str::to_owned)
    }

    #[test]
    fn a_train_waiting_on_its_running_prepare_holds_the_capacity_its_shard_needs() {
        // Train 10b (#4204): a lane's shards took mindy three minutes before the train's prepare
        // finished, and the train's shards waited 34 minutes behind them.
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        assert_eq!(mindy_claims(&mut store, "k1"), None);
        // Once prepare passes, the shard is the head and nothing is held.
        store
            .db
            .execute("UPDATE actions SET state='passed' WHERE id='prepare'", [])
            .unwrap();
        let record = load(&store.db, "mindy").unwrap().unwrap();
        let capacity = capacity(&store.db, &record).unwrap();
        let verified = BTreeSet::from(["f-1".to_owned()]);
        let selection = select(
            &store.db,
            &record,
            &Scope::all(),
            &verified,
            &BTreeSet::new(),
            capacity,
        )
        .unwrap();
        assert_eq!(selection.choice.unwrap().action.id, "shard");
        assert!(selection.hold.is_empty());
    }

    #[test]
    fn the_hold_leaves_room_the_train_will_not_need() {
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (2, 1024));
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lane"));
    }

    #[test]
    fn no_hold_for_a_dependency_not_yet_running_or_one_that_failed() {
        // Holding for a whole chain would idle the executor for longer than one dependency runs.
        for prepare in ["queued", "failed"] {
            let (_root, mut store) = train_beside_lane(prepare, (6, 6144), (4, 4096));
            // mindy may not run the train's prepare itself, which would otherwise be its head.
            let mut record = load(&store.db, "mindy").unwrap().unwrap();
            record.placement.deny = BTreeSet::from(["prepare".to_owned()]);
            save(&store.db, &record).unwrap();
            assert_eq!(
                mindy_claims(&mut store, "k1").as_deref(),
                Some("lane"),
                "{prepare}"
            );
        }
    }

    #[test]
    fn no_hold_for_work_this_executor_may_not_run_or_for_an_owner_without_priority() {
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        let mut record = load(&store.db, "mindy").unwrap().unwrap();
        record.placement.deny = BTreeSet::from(["vitest".to_owned()]);
        save(&store.db, &record).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lane"));
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        crate::worker::set_priority(&store, "train", 0).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lane"));
    }

    /// Gate FIX on pw#10: the hold's size and its release, pinned. mindy is 8 slots, 8192 MiB.
    fn hold_lab() -> (tempfile::TempDir, Store) {
        let (root, store) = lab();
        register(&store.db, "mindy", 8, 8192);
        (root, store)
    }

    /// An action of `owner` in `state`, waiting on `dependencies`, reserving `need`.
    fn job_of(
        store: &Store,
        id: &str,
        owner: &str,
        state: &str,
        need: (u32, u32),
        dependencies: &[&str],
    ) {
        queue(
            &store.db,
            Queue {
                id,
                owner,
                job: id,
                reservation: Some(need),
                dependencies,
                created: 2.0,
                ..Queue::default()
            },
        );
        store
            .db
            .execute("UPDATE actions SET state=? WHERE id=?", [state, id])
            .unwrap();
    }

    #[test]
    fn the_hold_is_sized_by_memory_and_by_slots_and_counts_every_waiting_job() {
        // B/C: memory was 10b's binding constraint; A: two shards wait, as in the incident.
        for (shards, lane) in [
            (vec![(1, 6144)], (1, 4096)),
            (vec![(6, 512)], (4, 512)),
            (vec![(3, 1024), (3, 1024)], (3, 1024)),
        ] {
            let (_root, mut store) = hold_lab();
            job_of(&store, "prepare", "train", "running", (1, 1), &[]);
            for (index, shard) in shards.iter().enumerate() {
                job_of(
                    &store,
                    &format!("shard-{index}"),
                    "train",
                    "queued",
                    *shard,
                    &["prepare"],
                );
            }
            job_of(&store, "lane", "lane", "queued", lane, &[]);
            crate::worker::set_priority(&store, "train", 1).unwrap();
            assert_eq!(
                mindy_claims(&mut store, "k1"),
                None,
                "{shards:?} beside {lane:?}"
            );
        }
    }

    #[test]
    fn the_hold_counts_running_work_but_not_the_dependency_it_waits_on() {
        // Each case binds one dimension alone, so slots and memory are each pinned (mindy 8/8192).
        // D: work already running on the host leaves less room beside the hold.
        for (shard, other, lane) in [
            ((4, 512), (3, 512), (2, 512)),
            ((1, 4096), (1, 3072), (1, 2048)),
        ] {
            let (_root, mut store) = hold_lab();
            job_of(&store, "prepare", "train", "running", (1, 1), &[]);
            job_of(&store, "shard", "train", "queued", shard, &["prepare"]);
            job_of(&store, "lane", "lane", "queued", lane, &[]);
            hold(&store.db, "other", Some("mindy"), other.0, other.1);
            crate::worker::set_priority(&store, "train", 1).unwrap();
            assert_eq!(
                mindy_claims(&mut store, "k1"),
                None,
                "{shard:?} {other:?} {lane:?}"
            );
        }
        // P3-3: the running prepare's own reservation frees before its shard starts, so it is not
        // counted beside the hold as well.
        for (prepare, shard, lane) in [
            ((4, 1024), (4, 1024), (2, 1024)),
            ((1, 4096), (1, 4096), (1, 2048)),
        ] {
            let (_root, mut store) = hold_lab();
            job_of(&store, "prepare", "train", "running", prepare, &[]);
            job_of(&store, "shard", "train", "queued", shard, &["prepare"]);
            job_of(&store, "lane", "lane", "queued", lane, &[]);
            store
                .db
                .execute(
                    "INSERT INTO attempts VALUES ('prep-a','prepare','running',0,?)",
                    [
                        json!({"id":"prep-a","action":"prepare","worker_pid":1,"worker_start":"1",
                            "supervisor_pid":null,"supervisor_start":null,"slots":prepare.0,
                            "memory_mb":prepare.1,"created":0.0,"started":null,"ended":null,
                            "exit_code":null,"result":null,"executor":"mindy"})
                        .to_string(),
                    ],
                )
                .unwrap();
            store
                .db
                .execute("UPDATE actions SET attempt='prep-a' WHERE id='prepare'", [])
                .unwrap();
            crate::worker::set_priority(&store, "train", 1).unwrap();
            assert_eq!(
                mindy_claims(&mut store, "k1").as_deref(),
                Some("lane"),
                "{prepare:?}"
            );
        }
    }

    #[test]
    fn the_hold_ends_when_its_dependency_stops_or_nobody_wants_the_job() {
        // F: lease expiry interrupts, a stop cancels; G: an unwanted job holds nothing.
        for state in ["interrupted", "stopping", "canceled"] {
            let (_root, mut store) = train_beside_lane(state, (6, 6144), (4, 4096));
            let mut record = load(&store.db, "mindy").unwrap().unwrap();
            record.placement.deny = BTreeSet::from(["prepare".to_owned()]);
            save(&store.db, &record).unwrap();
            assert_eq!(
                mindy_claims(&mut store, "k1").as_deref(),
                Some("lane"),
                "{state}"
            );
        }
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        crate::engine::cancel(&mut store, "shard", "train").unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lane"));
    }

    #[test]
    fn no_hold_for_a_job_whose_candidate_still_waits_on_a_prerequisite() {
        // G: the shard is queued and its prepare runs, but its only requester is a candidate whose
        // prerequisite has not passed, so it would not run once prepare passes either.
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        let candidate = |id: &str, required: Value, prerequisites: Value| {
            json!({"id":id,"revision":"r-a","profile":"f-1","required":required,
                   "prerequisites":prerequisites,"owner":"train","created":1.0})
            .to_string()
        };
        store
            .db
            .execute(
                "INSERT INTO candidates VALUES ('pre',?),('wants',?)",
                [
                    candidate("pre", json!({"prepare":"prepare"}), json!([])),
                    candidate("wants", json!({"vitest":"shard"}), json!(["pre"])),
                ],
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE interests SET context='wants' WHERE action='shard'",
                [],
            )
            .unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lane"));
    }

    #[test]
    fn a_train_is_never_held_back_by_its_own_hold_and_priorities_hold_only_above_themselves() {
        // E: the train's own eligible lint is admitted beside its waiting shard.
        let (_root, mut store) = hold_lab();
        job_of(&store, "prepare", "train", "running", (1, 1), &[]);
        job_of(&store, "shard", "train", "queued", (6, 1024), &["prepare"]);
        job_of(&store, "lint", "train", "queued", (4, 1024), &[]);
        crate::worker::set_priority(&store, "train", 1).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("lint"));
        // H, P3-1: train A (2) and train B (1) both wait; B's lint fits beside A's demand alone, the
        // lane fits beside neither.
        let (_root, mut store) = hold_lab();
        job_of(&store, "prepare", "a", "running", (1, 1), &[]);
        job_of(&store, "a-shard", "a", "queued", (4, 4096), &["prepare"]);
        job_of(&store, "b-shard", "b", "queued", (4, 4096), &["prepare"]);
        job_of(&store, "b-lint", "b", "queued", (1, 512), &[]);
        job_of(&store, "lane", "lane", "queued", (1, 512), &[]);
        crate::worker::set_priority(&store, "a", 2).unwrap();
        crate::worker::set_priority(&store, "b", 1).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1").as_deref(), Some("b-lint"));
        assert_eq!(mindy_claims(&mut store, "k2"), None);
    }

    /// A running attempt of `action` on mindy, so it counts in `held` and as a dependency waited on.
    fn running_on_mindy(store: &Store, action: &str, need: (u32, u32)) {
        let id = format!("{action}-a");
        store
            .db
            .execute(
                "INSERT INTO attempts VALUES (?,?,'running',0,?)",
                rusqlite::params![
                    id,
                    action,
                    json!({"id":id,"action":action,"worker_pid":1,"worker_start":"1","supervisor_pid":null,
                           "supervisor_start":null,"slots":need.0,"memory_mb":need.1,"created":0.0,
                           "started":null,"ended":null,"exit_code":null,"result":null,"executor":"mindy"})
                    .to_string()
                ],
            )
            .unwrap();
        store
            .db
            .execute("UPDATE actions SET attempt=? WHERE id=?", [&id, action])
            .unwrap();
    }

    #[test]
    fn a_lower_train_never_counts_its_own_dependency_as_freed_for_a_higher_train() {
        // Re-gate P3-1 on pw#10: train A (2) waits on a prepare elsewhere; train B (1) runs its
        // prepare on mindy. B's lint must not take A's room because B's prepare will free.
        let (_root, mut store) = hold_lab();
        job_of(&store, "prepare-a", "a", "running", (1, 1), &[]);
        job_of(&store, "a-shard", "a", "queued", (4, 1024), &["prepare-a"]);
        job_of(&store, "prepare-b", "b", "running", (4, 1024), &[]);
        running_on_mindy(&store, "prepare-b", (4, 1024));
        job_of(&store, "b-shard", "b", "queued", (4, 1024), &["prepare-b"]);
        job_of(&store, "b-lint", "b", "queued", (4, 1024), &[]);
        crate::worker::set_priority(&store, "a", 2).unwrap();
        crate::worker::set_priority(&store, "b", 1).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1"), None);
    }

    #[test]
    fn a_dependency_two_held_jobs_wait_on_frees_its_capacity_once() {
        // Re-gate P3-2 on pw#10: held 7 (prepare 4 + other 3) - freed 4 + demand 5 + lane 1 = 9 > 8.
        let (_root, mut store) = hold_lab();
        job_of(&store, "prepare", "train", "running", (4, 256), &[]);
        running_on_mindy(&store, "prepare", (4, 256));
        hold(&store.db, "other", Some("mindy"), 3, 256);
        job_of(&store, "shard-1", "train", "queued", (3, 256), &["prepare"]);
        job_of(&store, "shard-2", "train", "queued", (2, 256), &["prepare"]);
        job_of(&store, "lane", "lane", "queued", (1, 256), &[]);
        crate::worker::set_priority(&store, "train", 1).unwrap();
        assert_eq!(mindy_claims(&mut store, "k1"), None);
    }

    #[test]
    fn a_hold_is_visible_on_the_executor_record_and_in_events() {
        // P3-4: a hold no longer looks like an executor draining for its head.
        let (_root, mut store) = train_beside_lane("running", (6, 6144), (4, 4096));
        assert_eq!(mindy_claims(&mut store, "k1"), None);
        let record = load(&store.db, "mindy").unwrap().unwrap();
        assert_eq!(
            record.holding,
            Some(Holding {
                priority: 1,
                slots: 6,
                memory_mb: 6144
            })
        );
        assert_eq!(events(&store.db, "executor.held").len(), 1);
        assert_eq!(mindy_claims(&mut store, "k2"), None);
        assert_eq!(events(&store.db, "executor.held").len(), 1, "deduplicated");
        store
            .db
            .execute("UPDATE actions SET state='failed' WHERE id='prepare'", [])
            .unwrap();
        assert_eq!(mindy_claims(&mut store, "k3").as_deref(), Some("lane"));
        assert_eq!(load(&store.db, "mindy").unwrap().unwrap().holding, None);
        assert_eq!(events(&store.db, "executor.hold_released").len(), 1);
    }

    fn queued(db: &Connection, id: &str, created: f64) {
        queue(
            db,
            Queue {
                id,
                created,
                ..Queue::default()
            },
        );
    }

    /// mindy claims the head of its queue.
    fn grant(store: &mut Store, key: &str) -> Claim {
        claim_for(store, "mindy", &Scope::all(), &ask(key, &["f-1"]))
            .unwrap()
            .claim
            .unwrap()
    }

    fn lease(claim: &Claim) -> LeaseReport {
        LeaseReport {
            attempt: claim.attempt.id.clone(),
            lease: claim.attempt.lease.clone(),
            started: None,
            stopping: None,
        }
    }

    fn supervisor(pid: u32) -> Option<SupervisorIdentity> {
        Some(SupervisorIdentity {
            pid,
            start: format!("{pid}0"),
        })
    }

    fn heartbeat(
        store: &mut Store,
        executor: &str,
        scope: &Scope,
        leases: Vec<LeaseReport>,
    ) -> Vec<LeaseStatus> {
        renew(store, executor, scope, &RenewRequest { leases })
            .unwrap()
            .leases
    }

    fn register_supervisor(store: &mut Store, claim: &Claim) {
        let started = LeaseReport {
            started: supervisor(4242),
            ..lease(claim)
        };
        assert!(heartbeat(store, "mindy", &Scope::all(), vec![started])[0].registered);
    }

    fn finish(
        claim: &Claim,
        outcome: Outcome,
        reason: Option<&str>,
        exit_code: i32,
        evidence: Value,
    ) -> CompleteRequest {
        CompleteRequest {
            attempt: claim.attempt.id.clone(),
            lease: claim.attempt.lease.clone(),
            outcome,
            reason: reason.map(String::from),
            exit_code,
            evidence,
            drained: true,
            log: None,
            cleanup_log: None,
            artifacts: BTreeMap::new(),
        }
    }

    fn passing(claim: &Claim) -> CompleteRequest {
        finish(
            claim,
            Outcome::Completed,
            None,
            0,
            json!({"exit_code":0,"reason":null,"artifacts":{}}),
        )
    }

    fn row(db: &Connection, attempt: &str) -> (String, bool) {
        db.query_row(
            "SELECT state,drained FROM attempts WHERE id=?",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    fn body(db: &Connection, attempt: &str) -> String {
        db.query_row("SELECT body FROM attempts WHERE id=?", [attempt], |r| {
            r.get(0)
        })
        .unwrap()
    }

    fn repairs(db: &Connection, action: &str) -> i64 {
        db.query_row(
            "SELECT COUNT(*) FROM repairs WHERE action=? AND kind='execution' AND state='open'",
            [action],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Verified uploads moved out of the upload area, as the API stages them.
    fn stage(store: &Store, log: &str) -> std::path::PathBuf {
        let directory = store.root.join(format!(".incoming-{}", unique()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("output.log"), log).unwrap();
        directory
    }

    fn idle() -> Reservation {
        Reservation {
            slots: 0,
            memory_mb: 0,
        }
    }

    fn one() -> Reservation {
        Reservation {
            slots: 1,
            memory_mb: 512,
        }
    }

    #[test]
    fn stopping_is_recorded_once_by_one_function() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let attempt = claim.attempt.id.clone();
        let before = store.attempt(&attempt).unwrap();
        // A requested action has no reason to stop, and another attempt never stops this one.
        assert_eq!(stopping(&store.db, "first", &attempt, None).unwrap(), None);
        assert_eq!(
            stopping(&store.db, "first", "other", Some("timeout")).unwrap(),
            None
        );
        assert_eq!(state(&store.db, "first"), "running");
        assert_eq!(
            stopping(&store.db, "first", &attempt, Some("timeout"))
                .unwrap()
                .as_deref(),
            Some("timeout")
        );
        assert_eq!(state(&store.db, "first"), "stopping");
        // Recording the reason leaves every other byte of the attempt body as it was.
        let expected = Attempt {
            stop: Some("timeout".into()),
            ..before
        };
        assert_eq!(
            body(&store.db, &attempt),
            serde_json::to_string(&expected).unwrap()
        );
        // Later reports, relayed or not, and the store's own decision return the recorded reason.
        store
            .db
            .execute("UPDATE interests SET active=0 WHERE action='first'", [])
            .unwrap();
        for reported in [None, Some("supervisor-stop"), Some("timeout")] {
            assert_eq!(
                stopping(&store.db, "first", &attempt, reported)
                    .unwrap()
                    .as_deref(),
                Some("timeout")
            );
        }
        let relayed = LeaseReport {
            stopping: Some("supervisor-stop".into()),
            ..lease(&claim)
        };
        let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![relayed]);
        assert_eq!(status[0].stop.as_deref(), Some("timeout"));
        assert_eq!(
            events(&store.db, "attempt.stopping"),
            [(
                attempt.clone(),
                json!({"reason":"timeout","executor":"mindy"})
            )]
        );
        // Without requesters the store alone decides; executors cannot report that reason.
        queued(&store.db, "second", 2.0);
        let second = grant(&mut store, "k2");
        store
            .db
            .execute("UPDATE interests SET active=0 WHERE action='second'", [])
            .unwrap();
        let forged = LeaseReport {
            stopping: Some("no-requesters".into()),
            ..lease(&second)
        };
        let refused = renew(
            &mut store,
            "mindy",
            &Scope::all(),
            &RenewRequest {
                leases: vec![forged],
            },
        );
        // An unknown supervisor stop fences that lease alone and records nothing (review P3-7).
        let refused = refused.unwrap();
        assert_eq!(
            (
                refused.leases[0].status.as_str(),
                refused.leases[0].stop.as_deref()
            ),
            ("fenced", None)
        );
        assert_eq!(state(&store.db, "second"), "running");
        let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![lease(&second)]);
        assert_eq!(status[0].stop.as_deref(), Some("no-requesters"));
        assert_eq!(state(&store.db, "second"), "stopping");
        let stops = events(&store.db, "attempt.stopping");
        assert_eq!(stops.len(), 2);
        assert_eq!(
            stops[1],
            (
                second.attempt.id.clone(),
                json!({"reason":"no-requesters","executor":"mindy"})
            )
        );
        // A finished action is never stopped.
        store
            .db
            .execute("UPDATE actions SET state='passed' WHERE id='second'", [])
            .unwrap();
        assert_eq!(
            stopping(&store.db, "second", &second.attempt.id, Some("timeout")).unwrap(),
            None
        );
        assert_eq!(events(&store.db, "attempt.stopping").len(), 2);
    }

    #[test]
    fn renew_fences_wrong_lease_executor_state_and_scope() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 8, 8192);
        register(&store.db, "other", 8, 8192);
        let base = Queue::default();
        queue(&store.db, Queue { id: "held", ..base });
        for (n, id) in ["expired", "replaced", "canceled"].into_iter().enumerate() {
            queue(
                &store.db,
                Queue {
                    id,
                    created: 2.0 + n as f64,
                    ..base
                },
            );
        }
        queue(
            &store.db,
            Queue {
                id: "profile",
                profile: "f-2",
                created: 6.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "project",
                revision: "r-b",
                created: 7.0,
                ..base
            },
        );
        let mut claims = BTreeMap::new();
        for n in 0..6 {
            let request = ask(&format!("k{n}"), &["f-1", "f-2"]);
            let claim = claim_for(&mut store, "mindy", &Scope::all(), &request)
                .unwrap()
                .claim
                .unwrap();
            claims.insert(claim.action.id.clone(), claim);
        }
        // The attempt lost its lease; the action moved to a newer attempt; the action ended.
        store
            .db
            .execute(
                "UPDATE attempts SET state='interrupted' WHERE id=?",
                [&claims["expired"].attempt.id],
            )
            .unwrap();
        store
            .db
            .execute("UPDATE actions SET attempt='newer' WHERE id='replaced'", [])
            .unwrap();
        store
            .db
            .execute(
                "UPDATE actions SET state='canceled' WHERE id='canceled'",
                [],
            )
            .unwrap();
        let bodies = |db: &Connection| -> Vec<String> {
            let mut stmt = db.prepare("SELECT body FROM attempts ORDER BY id").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let before = bodies(&store.db);
        let scope = Scope {
            projects: Some(["p-a".to_owned()].into()),
            profiles: Some(["f-1".to_owned()].into()),
        };
        let other = heartbeat(&mut store, "other", &scope, vec![lease(&claims["held"])]);
        let wrong = LeaseReport {
            lease: "stolen".into(),
            ..lease(&claims["held"])
        };
        let ghost = LeaseReport {
            attempt: "ghost".into(),
            ..lease(&claims["held"])
        };
        let mut reports = vec![wrong, ghost];
        for id in ["expired", "replaced", "canceled", "profile", "project"] {
            reports.push(lease(&claims[id]));
        }
        for report in &mut reports {
            report.started = supervisor(4242);
        }
        let statuses = heartbeat(&mut store, "mindy", &scope, reports);
        assert_eq!(statuses.len(), 7);
        for status in other.iter().chain(&statuses) {
            assert_eq!(
                (
                    status.status.as_str(),
                    status.stop.as_deref(),
                    status.registered,
                    status.lease_expires
                ),
                ("fenced", None, false, None),
                "{}",
                status.attempt
            );
        }
        // Fenced entries are never mutated.
        assert_eq!(bodies(&store.db), before);
        assert!(events(&store.db, "attempt.started").is_empty());
        let held = &claims["held"].attempt;
        let renewed = heartbeat(&mut store, "mindy", &scope, vec![lease(&claims["held"])]);
        assert_eq!(renewed[0].status, "ok");
        assert!(renewed[0].lease_expires.unwrap() > held.lease_expires);
        let too_many = RenewRequest {
            leases: vec![lease(&claims["held"]); 65],
        };
        assert!(matches!(
            renew(&mut store, "mindy", &scope, &too_many),
            Err(AuthorityError::Invalid(_))
        ));
        let nobody = RenewRequest { leases: vec![] };
        assert!(matches!(
            renew(&mut store, "ghost", &scope, &nobody),
            Err(AuthorityError::Unregistered)
        ));
        // An attempt from before executors has no lease to renew.
        hold(&store.db, "legacy", None, 1, 512);
        let legacy = LeaseReport {
            attempt: "legacy".into(),
            lease: String::new(),
            started: None,
            stopping: None,
        };
        let status = heartbeat(&mut store, LOCAL, &Scope::all(), vec![legacy]);
        assert_eq!(status[0].status, "fenced");
    }

    #[test]
    fn remote_claims_skip_actions_whose_manifests_exceed_the_response_budget() {
        // One size owner (gate P2-1): mindy must never be granted a claim it cannot read.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 16, 16384);
        queue(
            &store.db,
            Queue {
                id: "producer",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "consumer",
                created: 2.0,
                ..Queue::default()
            },
        );
        let big = "x".repeat(api::MAX_CLAIM_MANIFESTS as usize + 1);
        let attempt = json!({"id":"p-1","action":"producer","worker_pid":0,"worker_start":"",
            "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,"created":0.0,
            "started":null,"ended":null,"exit_code":0,
            "result":{"artifacts":{"dist/a":{"sha256":big,"mode":420,"bytes":1}}}});
        let db = &store.db;
        db.execute(
            "INSERT INTO attempts VALUES ('p-1','producer','passed',1,?)",
            [attempt.to_string()],
        )
        .unwrap();
        db.execute(
            "UPDATE actions SET state='passed',attempt='p-1' WHERE id='producer'",
            [],
        )
        .unwrap();
        db.execute(
            "UPDATE actions SET body=json_set(body,'$.dependencies',json('[\"producer\"]'))
             WHERE id='consumer'",
            [],
        )
        .unwrap();
        let remote = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&remote), None);
        // The producer recorded before sizes were stored was measured once, then kept.
        let stored: i64 = store
            .db
            .query_row(
                "SELECT bytes FROM manifest_sizes WHERE attempt='p-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(stored as u64 > api::MAX_CLAIM_MANIFESTS);
        // The local executor reads no claim response, so it runs the action.
        let local = claim_for(&mut store, LOCAL, &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert_eq!(claimed(&local), Some("consumer"));
    }

    #[test]
    fn the_claim_path_reads_the_recorded_manifest_size() {
        // Gate P2-A: select compares the size recorded with the result; it never parses the
        // producer's manifest inside the write transaction. A tiny manifest recorded as huge proves
        // the stored number is what counts.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 16, 16384);
        queue(
            &store.db,
            Queue {
                id: "producer",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "consumer",
                created: 2.0,
                ..Queue::default()
            },
        );
        let attempt = json!({"id":"p-1","action":"producer","worker_pid":0,"worker_start":"",
            "supervisor_pid":null,"supervisor_start":null,"slots":1,"memory_mb":1,"created":0.0,
            "started":null,"ended":null,"exit_code":0,"result":{"artifacts":{}}});
        store
            .db
            .execute(
                "INSERT INTO attempts VALUES ('p-1','producer','passed',1,?)",
                [attempt.to_string()],
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE actions SET state='passed',attempt='p-1' WHERE id='producer'",
                [],
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE actions SET body=json_set(body,'$.dependencies',json('[\"producer\"]'))
             WHERE id='consumer'",
                [],
            )
            .unwrap();
        store
            .db
            .execute(
                "INSERT INTO manifest_sizes VALUES ('p-1',?)",
                [api::MAX_CLAIM_MANIFESTS as i64 + 1],
            )
            .unwrap();
        let remote = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&remote), None);
        // And record_result stores the size with every result it writes.
        store.db.execute("DELETE FROM manifest_sizes", []).unwrap();
        store
            .db
            .execute("DELETE FROM actions WHERE id='consumer'", [])
            .unwrap();
        queued(&store.db, "built", 3.0);
        let claim = grant(&mut store, "k2");
        register_supervisor(&mut store, &claim);
        let evidence = json!({"exit_code":0,"reason":null,
                              "artifacts":{"dist/a":{"sha256":"0".repeat(64),"mode":420,"bytes":1}}});
        let request = CompleteRequest {
            artifacts: serde_json::from_value(evidence["artifacts"].clone()).unwrap(),
            ..finish(&claim, Outcome::Completed, None, 0, evidence.clone())
        };
        complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        let bytes: i64 = store
            .db
            .query_row(
                "SELECT bytes FROM manifest_sizes WHERE attempt=?",
                [&claim.attempt.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            bytes as usize,
            serde_json::to_vec(&evidence["artifacts"]).unwrap().len()
        );
    }

    #[test]
    fn out_of_scope_results_go_through_the_one_result_writer() {
        // Gate P3-C: a report the narrowed grant no longer covers is recorded without its evidence,
        // releases its capacity, and honours a recorded stop like any other result.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "wanted", 1.0);
        queued(&store.db, "unwanted", 2.0);
        let narrowed = Scope {
            projects: None,
            profiles: Some(["f-other".to_owned()].into()),
        };
        let wanted = grant(&mut store, "k1");
        register_supervisor(&mut store, &wanted);
        let request = finish(&wanted, Outcome::Completed, None, 0, json!({"exit_code":0}));
        let response =
            complete(&mut store, "mindy", &narrowed, &request, Outputs::InPlace).unwrap();
        assert_eq!(
            (response.status.as_str(), response.state.as_deref()),
            ("fenced", Some("interrupted"))
        );
        assert_eq!(state(&store.db, "wanted"), "queued");
        let recorded = store.attempt(&wanted.attempt.id).unwrap();
        assert_eq!(
            recorded.result.unwrap()["error"],
            "the executor's grant no longer covers this action"
        );
        // Out of the way of the next claim.
        store
            .db
            .execute("UPDATE actions SET state='canceled' WHERE id='wanted'", [])
            .unwrap();
        // A recorded no-requesters stop wins: nobody wants it, so it is canceled, not requeued.
        let unwanted = grant(&mut store, "k2");
        register_supervisor(&mut store, &unwanted);
        let recorded = stopping(
            &store.db,
            "unwanted",
            &unwanted.attempt.id,
            Some("no-requesters"),
        )
        .unwrap();
        assert_eq!(recorded.as_deref(), Some("no-requesters"));
        assert_eq!(
            store.attempt(&unwanted.attempt.id).unwrap().stop.as_deref(),
            Some("no-requesters")
        );
        let request = finish(
            &unwanted,
            Outcome::Completed,
            None,
            0,
            json!({"exit_code":0}),
        );
        let response =
            complete(&mut store, "mindy", &narrowed, &request, Outputs::InPlace).unwrap();
        assert_eq!(response.state.as_deref(), Some("canceled"));
        assert_eq!(state(&store.db, "unwanted"), "canceled");
    }

    #[test]
    fn the_requeue_budget_counts_not_started_requeues_since_the_last_retry() {
        // Gate P3-D: three not-started requeues, then the action waits for an operator; a retry
        // starts the count again, and earlier interruptions of other kinds never spend it.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "flaky", 1.0);
        let not_started = |store: &mut Store, key: &str| {
            let claim = grant(store, key);
            let request = finish(
                &claim,
                Outcome::NotStarted,
                Some("launch-failed"),
                125,
                json!({}),
            );
            complete(store, "mindy", &Scope::all(), &request, Outputs::InPlace).unwrap();
        };
        for key in ["k1", "k2", "k3"] {
            not_started(&mut store, key);
            assert_eq!(state(&store.db, "flaky"), "queued", "{key}");
        }
        not_started(&mut store, "k4");
        assert_eq!(state(&store.db, "flaky"), "interrupted");
        // An operator's retry resets the budget (engine::retry deletes the row).
        store
            .db
            .execute("DELETE FROM requeues WHERE action='flaky'", [])
            .unwrap();
        store
            .db
            .execute(
                "UPDATE actions SET state='queued',attempt=NULL WHERE id='flaky'",
                [],
            )
            .unwrap();
        not_started(&mut store, "k5");
        assert_eq!(state(&store.db, "flaky"), "queued");
    }

    #[test]
    fn unknown_stop_fences_only_its_own_lease() {
        // A job can write its own control files; one bad value must not stall the host (P3-7).
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        queued(&store.db, "second", 2.0);
        let (first, second) = (grant(&mut store, "k1"), grant(&mut store, "k2"));
        let bogus = LeaseReport {
            stopping: Some("bogus".into()),
            ..lease(&first)
        };
        let status = heartbeat(
            &mut store,
            "mindy",
            &Scope::all(),
            vec![bogus, lease(&second)],
        );
        let by_attempt: BTreeMap<_, _> = status
            .iter()
            .map(|s| (s.attempt.clone(), s.status.clone()))
            .collect();
        assert_eq!(by_attempt[&first.attempt.id], "fenced");
        assert_eq!(by_attempt[&second.attempt.id], "ok");
    }

    #[test]
    fn renew_registers_supervisor_once_with_pid() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        // Heartbeats while the coordinator prepares keep the claim without registering it.
        let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![lease(&claim)]);
        assert_eq!(
            (
                status[0].status.as_str(),
                status[0].registered,
                status[0].stop.as_deref()
            ),
            ("ok", false, None)
        );
        assert_eq!(row(&store.db, &id).0, "claimed");
        register_supervisor(&mut store, &claim);
        assert_eq!(row(&store.db, &id).0, "running");
        let attempt = store.attempt(&id).unwrap();
        assert_eq!(
            (attempt.supervisor_pid, attempt.supervisor_start.as_deref()),
            (Some(4242), Some("42420"))
        );
        assert!(attempt.started.is_some());
        assert_eq!(
            events(&store.db, "attempt.started"),
            [(id.clone(), json!({"pid":4242,"executor":"mindy"}))]
        );
        // Repeated reports, even naming another process, change nothing.
        for started in [supervisor(4242), supervisor(5151), None] {
            let report = LeaseReport {
                started,
                ..lease(&claim)
            };
            let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![report]);
            assert!(status[0].registered);
        }
        let again = store.attempt(&id).unwrap();
        assert_eq!(
            (again.supervisor_pid, again.started),
            (Some(4242), attempt.started)
        );
        assert_eq!(events(&store.db, "attempt.started").len(), 1);
        // Leases follow the executor's current lease length; a heartbeat is a sign of life.
        let mut mindy = load(&store.db, "mindy").unwrap().unwrap();
        mindy.lease_seconds = 60;
        mindy.observed.last_seen = Some(1.0);
        save(&store.db, &mindy).unwrap();
        let response = renew(
            &mut store,
            "mindy",
            &Scope::all(),
            &RenewRequest {
                leases: vec![lease(&claim)],
            },
        )
        .unwrap();
        assert_eq!(response.lease_seconds, 60);
        let expires = response.leases[0].lease_expires.unwrap();
        assert!((expires - now() - 60.0).abs() < 5.0);
        assert_eq!(store.attempt(&id).unwrap().lease_expires, Some(expires));
        let seen = load(&store.db, "mindy")
            .unwrap()
            .unwrap()
            .observed
            .last_seen;
        assert!(seen.unwrap() > now() - 5.0);
    }

    #[test]
    fn renew_refuses_registration_when_stopping() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        // Canceled while the coordinator was still preparing the attempt.
        store
            .db
            .execute("UPDATE interests SET active=0 WHERE action='first'", [])
            .unwrap();
        assert_eq!(
            stopping(&store.db, "first", &id, None).unwrap().as_deref(),
            Some("no-requesters")
        );
        let started = LeaseReport {
            started: supervisor(4242),
            ..lease(&claim)
        };
        let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![started]);
        assert_eq!(
            (
                status[0].status.as_str(),
                status[0].registered,
                status[0].stop.as_deref()
            ),
            ("ok", false, Some("no-requesters"))
        );
        assert_eq!(row(&store.db, &id).0, "claimed");
        let attempt = store.attempt(&id).unwrap();
        assert_eq!((attempt.supervisor_pid, attempt.started), (None, None));
        assert!(events(&store.db, "attempt.started").is_empty());
        // The supervisor exits without running anything and the attempt drains as canceled.
        let request = finish(
            &claim,
            Outcome::NotStarted,
            Some("no-requesters"),
            125,
            json!({}),
        );
        let response = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        assert_eq!(response.state.as_deref(), Some("canceled"));
        assert_eq!(row(&store.db, &id), ("canceled".into(), true));
        assert_eq!(state(&store.db, "first"), "canceled");
        assert_eq!(held(&store.db, "mindy").unwrap(), idle());
    }

    #[test]
    fn complete_replays_identical_request_before_fencing() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        register(&store.db, "other", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        // A directory left by a commit that failed after the move is replaced.
        let directory = store.root.join("attempts").join(&id);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("stale"), "").unwrap();
        let staged = stage(&store, "log\n");
        let mut request = passing(&claim);
        let log = crate::hash(b"log\n");
        request.log = Some(BlobRef {
            sha256: log.clone(),
            bytes: 4,
        });
        request.evidence["log_sha256"] = json!(log);
        let first = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::Staged(staged.clone()),
        )
        .unwrap();
        assert_eq!(
            first,
            CompleteResponse {
                attempt: id.clone(),
                status: "recorded".into(),
                state: Some("passed".into()),
                drained: true,
                replayed: false,
                drain_recorded: false,
            }
        );
        assert!(!staged.exists());
        assert_eq!(
            std::fs::read_to_string(directory.join("output.log")).unwrap(),
            "log\n"
        );
        assert!(!directory.join("stale").exists());
        let recorded = body(&store.db, &id);
        assert_eq!(
            store.attempt(&id).unwrap().completion,
            Some(crate::identity(&request).unwrap())
        );
        // The response was lost: the same report is answered again, although the attempt is no
        // longer held and the executor's scope has since narrowed.
        let none = Scope {
            projects: Some(BTreeSet::new()),
            profiles: Some(BTreeSet::new()),
        };
        let again = stage(&store, "log\n");
        let replay = complete(
            &mut store,
            "mindy",
            &none,
            &request,
            Outputs::Staged(again.clone()),
        )
        .unwrap();
        assert_eq!(
            replay,
            CompleteResponse {
                replayed: true,
                ..first
            }
        );
        assert!(!again.exists());
        assert_eq!(
            std::fs::read_to_string(directory.join("output.log")).unwrap(),
            "log\n"
        );
        assert_eq!(body(&store.db, &id), recorded);
        assert_eq!(events(&store.db, "attempt.completed").len(), 1);
        // Only the lease holder's executor is answered.
        let stolen = CompleteRequest {
            lease: "stolen".into(),
            ..request.clone()
        };
        for (executor, request) in [("mindy", &stolen), ("other", &request)] {
            let result = complete(
                &mut store,
                executor,
                &Scope::all(),
                request,
                Outputs::InPlace,
            );
            assert!(matches!(result, Err(AuthorityError::Fenced)), "{executor}");
        }
    }

    #[test]
    fn complete_conflicting_request_is_rejected() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        let request = passing(&claim);
        complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        let recorded = body(&store.db, &id);
        let mut failed = finish(
            &claim,
            Outcome::Completed,
            None,
            1,
            json!({"exit_code":1,"artifacts":{}}),
        );
        failed.drained = request.drained;
        let undrained = CompleteRequest {
            drained: false,
            ..request.clone()
        };
        for different in [failed, undrained] {
            let staged = stage(&store, "other\n");
            let result = complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &different,
                Outputs::Staged(staged.clone()),
            );
            assert!(matches!(result, Err(AuthorityError::CompletionConflict)));
            assert!(!staged.exists());
        }
        assert_eq!(body(&store.db, &id), recorded);
        assert_eq!(row(&store.db, &id), ("passed".into(), true));
        assert_eq!(state(&store.db, "first"), "passed");
        assert_eq!(repairs(&store.db, "first"), 0);
        assert_eq!(events(&store.db, "attempt.completed").len(), 1);
    }

    #[test]
    fn drain_report_requires_executor_lease_and_undrained() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        register(&store.db, "other", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        // The coordinator was partitioned past its lease: interrupted, capacity still held.
        let lapsed = expire(&mut store, now() + 3600.0, 0.0).unwrap();
        assert_eq!(lapsed, [id.as_str()]);
        assert_eq!(row(&store.db, &id), ("interrupted".into(), false));
        let report = passing(&claim);
        let stolen = CompleteRequest {
            lease: "stolen".into(),
            ..report.clone()
        };
        let undrained = CompleteRequest {
            drained: false,
            ..report.clone()
        };
        for (executor, request) in [
            ("mindy", &stolen),
            ("other", &report),
            ("mindy", &undrained),
        ] {
            let staged = stage(&store, "late\n");
            let result = complete(
                &mut store,
                executor,
                &Scope::all(),
                request,
                Outputs::Staged(staged.clone()),
            );
            assert!(matches!(result, Err(AuthorityError::Fenced)), "{executor}");
            assert!(!staged.exists());
            assert_eq!(row(&store.db, &id), ("interrupted".into(), false));
        }
        assert_eq!(held(&store.db, "mindy").unwrap(), one());
        let staged = stage(&store, "late\n");
        let response = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &report,
            Outputs::Staged(staged.clone()),
        )
        .unwrap();
        assert_eq!(
            response,
            CompleteResponse {
                attempt: id.clone(),
                status: "fenced".into(),
                state: Some("interrupted".into()),
                drained: true,
                replayed: false,
                drain_recorded: true,
            }
        );
        // The late result is never recorded; only the drain is.
        assert!(!staged.exists());
        assert!(!store.root.join("attempts").join(&id).exists());
        assert_eq!(row(&store.db, &id), ("interrupted".into(), true));
        // A verdict too late to record leaves the action for an operator.
        assert_eq!(state(&store.db, "first"), "interrupted");
        assert_eq!(requeues(&store.db, "first"), 0);
        let attempt = store.attempt(&id).unwrap();
        assert_eq!(
            (attempt.result, attempt.exit_code, attempt.completion),
            (None, None, None)
        );
        assert!(events(&store.db, "attempt.completed").is_empty());
        assert_eq!(
            events(&store.db, "attempt.drain_reported"),
            [(
                id.clone(),
                json!({"executor":"mindy","outcome":"completed","exit_code":0,"requeued":false})
            )]
        );
        assert_eq!(held(&store.db, "mindy").unwrap(), idle());
        let result = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &report,
            Outputs::InPlace,
        );
        assert!(matches!(result, Err(AuthorityError::Fenced)));
        assert_eq!(events(&store.db, "attempt.drain_reported").len(), 1);
    }

    fn requeues(db: &Connection, action: &str) -> i64 {
        db.query_row(
            "SELECT COALESCE((SELECT count FROM requeues WHERE action=?),0)",
            [action],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// mindy claims the head of its queue under `key`, and every held lease then lapses.
    fn lapsed_claim(store: &mut Store, key: &str) -> Claim {
        let claim = grant(store, key);
        let expired = expire(store, now() + 3600.0, 0.0).unwrap();
        assert!(expired.contains(&claim.attempt.id));
        claim
    }

    /// The holder's late, drained report of a lapsed attempt (A2), and the detail it records.
    fn late_report(
        store: &mut Store,
        claim: &Claim,
        outcome: Outcome,
        reason: Option<&str>,
        exit_code: i32,
    ) -> Value {
        let evidence = json!({"exit_code":exit_code,"reason":reason,"artifacts":{}});
        let request = finish(claim, outcome, reason, exit_code, evidence);
        let response = complete(store, "mindy", &Scope::all(), &request, Outputs::InPlace).unwrap();
        assert_eq!(
            (response.status.as_str(), response.drain_recorded),
            ("fenced", true)
        );
        assert_eq!(
            row(&store.db, &claim.attempt.id),
            ("interrupted".into(), true)
        );
        assert!(store.attempt(&claim.attempt.id).unwrap().result.is_none());
        let reported = events(&store.db, "attempt.drain_reported");
        let (subject, detail) = reported.last().unwrap().clone();
        assert_eq!(subject, claim.attempt.id);
        detail
    }

    #[test]
    fn a_late_report_requeues_only_without_a_verdict_and_within_the_budget() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 16, 16384);
        let names = [
            "failed",
            "passed",
            "passed-while-stopped",
            "timed-out",
            "not-started",
            "lost",
            "interrupted",
            "undeliverable",
            "spent",
            "unwanted",
            "unwanted-verdict",
            "moved",
        ];
        let mut claims = BTreeMap::new();
        for (name, n) in names.iter().zip(1..) {
            queued(&store.db, name, f64::from(n));
            claims.insert(*name, grant(&mut store, &format!("k{n}")));
        }
        store
            .db
            .execute("INSERT INTO requeues VALUES ('spent',3)", [])
            .unwrap();
        for name in ["unwanted", "unwanted-verdict"] {
            stopping(
                &store.db,
                name,
                &claims[name].attempt.id,
                Some("no-requesters"),
            )
            .unwrap();
        }
        assert_eq!(expire(&mut store, now() + 3600.0, 0.0).unwrap().len(), 12);
        // A verdict too late to record is not acted on: the action waits for an operator, and a
        // failure that might pass on a rerun keeps its human checkpoint. A command that exited 0
        // as it was stopped passed, and a timeout is a failure, as `record_result` judges them.
        // An undeliverable report may hide either, and `record_result` never requeues it.
        for (name, outcome, reason, exit_code) in [
            ("failed", Outcome::Completed, None, 1),
            ("passed", Outcome::Completed, None, 0),
            (
                "passed-while-stopped",
                Outcome::Completed,
                Some("fenced"),
                0,
            ),
            ("timed-out", Outcome::Completed, Some("timeout"), 124),
            ("undeliverable", Outcome::Undeliverable, None, 125),
        ] {
            let detail = late_report(&mut store, &claims[name], outcome, reason, exit_code);
            assert_eq!(
                detail,
                json!({"executor":"mindy","outcome":outcome,"exit_code":exit_code,
                       "requeued":false}),
                "{name}"
            );
            assert_eq!(state(&store.db, name), "interrupted", "{name}");
            assert_eq!(requeues(&store.db, name), 0, "{name}");
            assert_eq!(repairs(&store.db, name), 0, "{name}");
        }
        // Without a verdict the action is queued again, like a lost supervisor's.
        for (name, outcome, reason, exit_code) in [
            (
                "not-started",
                Outcome::NotStarted,
                Some("launch-failed"),
                125,
            ),
            ("lost", Outcome::Lost, None, 125),
            ("interrupted", Outcome::Completed, Some("fenced"), 143),
        ] {
            let detail = late_report(&mut store, &claims[name], outcome, reason, exit_code);
            assert_eq!(
                detail,
                json!({"executor":"mindy","outcome":outcome,"exit_code":exit_code,
                       "requeued":true}),
                "{name}"
            );
            assert_eq!(state(&store.db, name), "queued", "{name}");
            assert_eq!(requeues(&store.db, name), 1, "{name}");
        }
        // Spent by earlier requeues: the action waits for an operator, its capacity released.
        let detail = late_report(&mut store, &claims["spent"], Outcome::Lost, None, 125);
        assert_eq!(detail["requeued"], false);
        assert_eq!(state(&store.db, "spent"), "interrupted");
        assert_eq!(requeues(&store.db, "spent"), 3);
        // Nobody wants it any more: canceled, never queued (P3-C), whether or not the report
        // carries a verdict.
        for (name, outcome, reason, exit_code) in [
            ("unwanted", Outcome::Lost, None, 125),
            (
                "unwanted-verdict",
                Outcome::Completed,
                Some("no-requesters"),
                143,
            ),
        ] {
            assert_eq!(state(&store.db, name), "interrupted");
            let detail = late_report(&mut store, &claims[name], outcome, reason, exit_code);
            assert_eq!(detail["requeued"], false, "{name}");
            assert_eq!(state(&store.db, name), "canceled", "{name}");
            assert_eq!(requeues(&store.db, name), 0, "{name}");
        }
        // An action no longer waiting on the attempt is left as it is.
        store
            .db
            .execute("UPDATE actions SET state='failed' WHERE id='moved'", [])
            .unwrap();
        let detail = late_report(&mut store, &claims["moved"], Outcome::Lost, None, 125);
        assert_eq!(detail["requeued"], false);
        assert_eq!(state(&store.db, "moved"), "failed");
        assert_eq!(requeues(&store.db, "moved"), 0);
    }

    #[test]
    fn an_interruption_requeues_alike_in_lease_and_after_its_lease_lapsed() {
        // The same drained partition report, landing while the lease is held (a short blip) or
        // after it lapsed (a long one), means the same: no verdict, so the action is queued again
        // within the budget, or canceled if nobody wants it.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 16, 16384);
        let cases = ["fresh", "spent", "unwanted"];
        let mut claims = BTreeMap::new();
        for (n, (case, when)) in cases
            .iter()
            .flat_map(|case| [(*case, "held"), (*case, "lapsed")])
            .enumerate()
        {
            let name = format!("{when}-{case}");
            queued(&store.db, &name, n as f64);
            let claim = grant(&mut store, &format!("k{n}"));
            assert_eq!(claim.action.id, name);
            register_supervisor(&mut store, &claim);
            if case == "spent" {
                store
                    .db
                    .execute("INSERT INTO requeues VALUES (?,3)", [&name])
                    .unwrap();
            }
            if case == "unwanted" {
                stopping(&store.db, &name, &claim.attempt.id, Some("no-requesters")).unwrap();
            }
            claims.insert((case, when), claim);
        }
        let report = |claim: &Claim| {
            let evidence = json!({"exit_code":124,"reason":"partition","artifacts":{}});
            finish(claim, Outcome::Completed, Some("partition"), 124, evidence)
        };
        for case in cases {
            let request = report(&claims[&(case, "held")]);
            let response = complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::InPlace,
            )
            .unwrap();
            assert_eq!(response.status, "recorded", "{case}");
            // Recorded like a lost supervisor's: canceled when nobody wants it.
            let expected = if case == "unwanted" {
                "canceled"
            } else {
                "interrupted"
            };
            assert_eq!(response.state.as_deref(), Some(expected), "{case}");
        }
        assert_eq!(expire(&mut store, now() + 3600.0, 0.0).unwrap().len(), 3);
        for case in cases {
            let request = report(&claims[&(case, "lapsed")]);
            let response = complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::InPlace,
            )
            .unwrap();
            assert!(response.drain_recorded, "{case}");
        }
        for (case, expected) in [
            ("fresh", ("queued", 1)),
            ("spent", ("interrupted", 3)),
            ("unwanted", ("canceled", 0)),
        ] {
            for when in ["held", "lapsed"] {
                let name = format!("{when}-{case}");
                let outcome = (state(&store.db, &name), requeues(&store.db, &name));
                assert_eq!((outcome.0.as_str(), outcome.1), expected, "{name}");
            }
        }
    }

    #[test]
    fn a_lapsed_attempt_nobody_requests_any_more_is_canceled_not_requeued() {
        // A cancel after the lease lapsed leaves the interrupted action as it is and records no
        // stop; the drain that follows must not queue work nobody wants.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "unrecorded", 1.0);
        queued(&store.db, "reported", 2.0);
        queued(&store.db, "verdict", 2.5);
        let first = grant(&mut store, "k1");
        let second = grant(&mut store, "k2");
        let fourth = grant(&mut store, "k4");
        expire(&mut store, now() + 3600.0, 0.0).unwrap();
        for name in ["unrecorded", "reported", "verdict"] {
            crate::engine::cancel(&mut store, name, "lane").unwrap();
            assert_eq!(state(&store.db, name), "interrupted");
        }
        let id = first.attempt.id.clone();
        claim_for(&mut store, "mindy", &Scope::all(), &unrecorded("k1", &id)).unwrap();
        assert_eq!(row(&store.db, &id), ("interrupted".into(), true));
        assert_eq!(state(&store.db, "unrecorded"), "canceled");
        assert_eq!(requeues(&store.db, "unrecorded"), 0);
        assert_eq!(
            events(&store.db, "attempt.drain_reported")[0].1,
            json!({"executor":"mindy","reason":"claim-unrecorded","requeued":false})
        );
        let detail = late_report(&mut store, &second, Outcome::Lost, None, 125);
        assert_eq!(detail["requeued"], false);
        assert_eq!(state(&store.db, "reported"), "canceled");
        assert_eq!(requeues(&store.db, "reported"), 0);
        // Nobody wanting it decides before a late verdict does.
        let detail = late_report(&mut store, &fourth, Outcome::Completed, None, 1);
        assert_eq!(detail["requeued"], false);
        assert_eq!(state(&store.db, "verdict"), "canceled");
        // A requester again: a later lapse of the same work is queued as before.
        queued(&store.db, "wanted", 3.0);
        let third = lapsed_claim(&mut store, "k3");
        let id = third.attempt.id.clone();
        claim_for(&mut store, "mindy", &Scope::all(), &unrecorded("k3", &id)).unwrap();
        assert_eq!(state(&store.db, "wanted"), "queued");
    }

    #[test]
    fn a_drained_report_nobody_requests_any_more_is_canceled_whatever_its_outcome() {
        // The action is already stopping for its own reason when its last requester cancels, so
        // no no-requesters stop is recorded: not started or lost, nobody wants it requeued.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "not-started", 1.0);
        queued(&store.db, "lost", 2.0);
        let mut claims = BTreeMap::new();
        for (name, key) in [("not-started", "k1"), ("lost", "k2")] {
            let claim = grant(&mut store, key);
            let reason = stopping(&store.db, name, &claim.attempt.id, Some("timeout")).unwrap();
            assert_eq!(reason.as_deref(), Some("timeout"));
            crate::engine::cancel(&mut store, name, "lane").unwrap();
            assert_eq!(state(&store.db, name), "stopping");
            assert_eq!(
                store.attempt(&claim.attempt.id).unwrap().stop.as_deref(),
                Some("timeout")
            );
            claims.insert(name, claim);
        }
        for (name, outcome) in [
            ("not-started", Outcome::NotStarted),
            ("lost", Outcome::Lost),
        ] {
            let claim = &claims[name];
            let request = finish(claim, outcome, Some("timeout"), 125, json!({}));
            complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::InPlace,
            )
            .unwrap();
            assert_eq!(row(&store.db, &claim.attempt.id), ("canceled".into(), true));
            assert_eq!(state(&store.db, name), "canceled", "{name}");
            assert_eq!(requeues(&store.db, name), 0, "{name}");
        }
        // A reported no-requesters reason is canceled as before.
        queued(&store.db, "reported", 3.0);
        let claim = grant(&mut store, "k3");
        let request = finish(
            &claim,
            Outcome::NotStarted,
            Some("no-requesters"),
            125,
            json!({}),
        );
        complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        assert_eq!(state(&store.db, "reported"), "canceled");
    }

    fn unrecorded(key: &str, attempt: &str) -> ClaimRequest {
        ClaimRequest {
            unrecorded: Some(attempt.into()),
            ..ask(key, &["f-1"])
        }
    }

    #[test]
    fn an_unrecorded_lapsed_claim_is_drained_and_requeued_within_the_budget() {
        // A claim the authority recorded but whose response never reached the executor: its lease
        // lapses, the replayed key is fenced, and the executor says it never recorded the attempt.
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let mut last = String::new();
        for round in 1..=4 {
            let key = format!("k{round}");
            let claim = lapsed_claim(&mut store, &key);
            assert_eq!(claim.action.id, "first");
            let id = claim.attempt.id.clone();
            // The replay alone is fenced and changes nothing.
            let fenced =
                claim_for(&mut store, "mindy", &Scope::all(), &ask(&key, &["f-1"])).unwrap();
            assert_eq!(fenced.fenced.as_deref(), Some(id.as_str()));
            assert_eq!(row(&store.db, &id), ("interrupted".into(), false));
            assert_eq!(held(&store.db, "mindy").unwrap(), one());
            let response =
                claim_for(&mut store, "mindy", &Scope::all(), &unrecorded(&key, &id)).unwrap();
            assert!(response.claim.is_none());
            assert_eq!(response.fenced.as_deref(), Some(id.as_str()));
            assert_eq!(row(&store.db, &id), ("interrupted".into(), true));
            assert_eq!(held(&store.db, "mindy").unwrap(), idle());
            // Three requeues, then the action waits for an operator's retry.
            let requeued = round <= 3;
            let expected = if requeued { "queued" } else { "interrupted" };
            assert_eq!(state(&store.db, "first"), expected, "round {round}");
            assert_eq!(
                events(&store.db, "attempt.drain_reported").last().unwrap(),
                &(
                    id.clone(),
                    json!({"executor":"mindy","reason":"claim-unrecorded","requeued":requeued})
                )
            );
            last = id;
        }
        assert_eq!(requeues(&store.db, "first"), 3);
        // Said again, it changes nothing more.
        let again =
            claim_for(&mut store, "mindy", &Scope::all(), &unrecorded("k4", &last)).unwrap();
        assert_eq!(again.fenced.as_deref(), Some(last.as_str()));
        assert_eq!(events(&store.db, "attempt.drain_reported").len(), 4);
        assert_eq!(state(&store.db, "first"), "interrupted");
        // Nobody wants it any more: canceled, never queued (P3-C).
        queued(&store.db, "unwanted", 2.0);
        let claim = grant(&mut store, "k5");
        let id = claim.attempt.id.clone();
        stopping(&store.db, "unwanted", &id, Some("no-requesters")).unwrap();
        expire(&mut store, now() + 3600.0, 0.0).unwrap();
        claim_for(&mut store, "mindy", &Scope::all(), &unrecorded("k5", &id)).unwrap();
        assert_eq!(row(&store.db, &id), ("interrupted".into(), true));
        assert_eq!(state(&store.db, "unwanted"), "canceled");
        assert_eq!(
            events(&store.db, "attempt.drain_reported")
                .last()
                .unwrap()
                .1,
            json!({"executor":"mindy","reason":"claim-unrecorded","requeued":false})
        );
    }

    #[test]
    fn unrecorded_drains_only_the_keys_own_lapsed_attempt_that_never_started() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 8, 8192);
        register(&store.db, "other", 8, 8192);
        let names = [
            "mine",
            "keyed",
            "started",
            "began",
            "pid",
            "reported",
            "confirmed",
            "ended",
        ];
        let mut claims = BTreeMap::new();
        for (name, n) in names.iter().zip(1..) {
            queued(&store.db, name, f64::from(n));
            claims.insert(*name, grant(&mut store, &format!("k{n}")));
        }
        let id = |name: &str| claims[name].attempt.id.clone();
        register_supervisor(&mut store, &claims["started"]);
        // Bodies no current writer leaves: a start without a supervisor, a supervisor without a
        // start, and an end other than an interruption without a report.
        for (name, edit) in [
            (
                "began",
                "UPDATE attempts SET body=json_set(body,'$.started',1.0) WHERE id=?",
            ),
            (
                "pid",
                "UPDATE attempts SET body=json_set(body,'$.supervisor_pid',4242) WHERE id=?",
            ),
        ] {
            store.db.execute(edit, [id(name)]).unwrap();
        }
        // Reported lost and undrained: interrupted, but under a recorded report.
        let lost = CompleteRequest {
            drained: false,
            ..finish(&claims["reported"], Outcome::Lost, None, 125, json!({}))
        };
        complete(&mut store, "mindy", &Scope::all(), &lost, Outputs::InPlace).unwrap();
        assert_eq!(
            row(&store.db, &id("reported")),
            ("interrupted".into(), false)
        );
        expire(&mut store, now() + 3600.0, 0.0).unwrap();
        confirm_drained(&mut store, &id("confirmed")).unwrap();
        store
            .db
            .execute(
                "UPDATE attempts SET state='canceled' WHERE id=?",
                [id("ended")],
            )
            .unwrap();
        // Still held: its key is the same claim again.
        queued(&store.db, "held", 9.0);
        let held_claim = grant(&mut store, "k9");
        // Queued work a fresh key would claim.
        queued(&store.db, "spare", 10.0);
        let snapshot = |store: &Store| {
            let actions: Vec<String> = names
                .iter()
                .chain(&["held", "spare"])
                .map(|name| state(&store.db, name))
                .collect();
            let mut stmt = store
                .db
                .prepare("SELECT id,state,drained FROM attempts ORDER BY rowid")
                .unwrap();
            let attempts: Vec<(String, String, bool)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            (actions, attempts)
        };
        let before = snapshot(&store);
        let mut send = |executor: &str, key: &str, attempt: &str| {
            let response = claim_for(
                &mut store,
                executor,
                &Scope::all(),
                &unrecorded(key, attempt),
            )
            .unwrap();
            (
                response.claim.map(|claim| claim.attempt.id),
                response.fenced,
            )
        };
        // Another executor's key of the same name is not mindy's.
        assert_eq!(send("other", "k1", &id("mine")), (None, None));
        // The key's own attempt is another one.
        assert_eq!(send("mindy", "k2", &id("mine")), (None, Some(id("keyed"))));
        // A key without an attempt claims nothing, though work is queued.
        assert_eq!(send("mindy", "k-new", &id("mine")), (None, None));
        // Started, a supervisor recorded, reported, drained already, or not interrupted.
        for (name, n) in names.iter().zip(1..).skip(2) {
            let key = format!("k{n}");
            assert_eq!(
                send("mindy", &key, &id(name)),
                (None, Some(id(name))),
                "{name}"
            );
        }
        // Still held, so not fenced at all.
        let held_id = held_claim.attempt.id.clone();
        assert_eq!(send("mindy", "k9", &held_id), (Some(held_id.clone()), None));
        assert_eq!(snapshot(&store), before);
        assert!(events(&store.db, "attempt.drain_reported").is_empty());
        // The executor's own key and its unstarted lapsed attempt.
        let response = claim_for(
            &mut store,
            "mindy",
            &Scope::all(),
            &unrecorded("k1", &id("mine")),
        )
        .unwrap();
        assert_eq!(response.fenced, Some(id("mine")));
        assert_eq!(row(&store.db, &id("mine")), ("interrupted".into(), true));
        assert_eq!(state(&store.db, "mine"), "queued");
        assert_eq!(events(&store.db, "attempt.drain_reported").len(), 1);
    }

    #[test]
    fn expire_honours_startup_grace_and_never_drains() {
        let (_root, mut store) = lab();
        let mut mindy = register(&store.db, "mindy", 4, 4096);
        mindy.lease_seconds = 10;
        save(&store.db, &mindy).unwrap();
        hold(&store.db, "legacy", None, 1, 512);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        queued(&store.db, "local", 2.0);
        let local = local_claim(&mut store).unwrap();
        let expires = claim.attempt.lease_expires;
        // An API restarted 5 s before the lease ran out gives the executor a full lease to renew.
        let restarted = expires - 5.0;
        for early in [1.0, 4.9] {
            assert!(
                expire(&mut store, expires + early, restarted)
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(state(&store.db, "first"), "running");
        assert_eq!(
            expire(&mut store, expires + 5.1, restarted).unwrap(),
            [id.as_str()]
        );
        // The local executor's hour-long lease outlives a remote one (review P3-6).
        assert_eq!(row(&store.db, &local.id), ("claimed".into(), false));
        // The store cannot know whether anything ran, so capacity stays held.
        assert_eq!(row(&store.db, &id), ("interrupted".into(), false));
        assert_eq!(state(&store.db, "first"), "interrupted");
        assert_eq!(held(&store.db, "mindy").unwrap(), one());
        let lapsed = events(&store.db, "attempt.lease_expired");
        assert_eq!(lapsed.len(), 1);
        assert_eq!(
            (lapsed[0].0.as_str(), &lapsed[0].1["executor"]),
            (id.as_str(), &json!("mindy"))
        );
        assert!((lapsed[0].1["lease_expires"].as_f64().unwrap() - expires).abs() < 1e-3);
        assert!((lapsed[0].1["effective"].as_f64().unwrap() - expires - 5.0).abs() < 1e-3);
        // Expiry happens once, and attempts without a lease are never expired; the local
        // attempt's own hour runs out in its turn.
        assert_eq!(
            expire(&mut store, expires + 3600.0, 0.0).unwrap(),
            [local.id.as_str()]
        );
        assert_eq!(row(&store.db, "legacy"), ("running".into(), false));
        assert_eq!(row(&store.db, &local.id), ("interrupted".into(), false));
        assert_eq!(events(&store.db, "attempt.lease_expired").len(), 2);
        // An executor whose record is gone gets the default lease as grace.
        queued(&store.db, "second", 3.0);
        let orphan = grant(&mut store, "k2");
        store
            .db
            .execute("DELETE FROM executors WHERE id='mindy'", [])
            .unwrap();
        let expires = orphan.attempt.lease_expires;
        let restarted = expires - 250.0;
        assert!(
            expire(&mut store, expires + 49.0, restarted)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            expire(&mut store, expires + 51.0, restarted).unwrap(),
            [orphan.attempt.id]
        );
    }

    #[test]
    fn expire_loses_to_concurrent_renew() {
        let (_root, mut store) = lab();
        let mut mindy = register(&store.db, "mindy", 4, 4096);
        mindy.lease_seconds = 10;
        save(&store.db, &mindy).unwrap();
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        // Expiry reads a lapsed lease; the executor renews before expiry writes.
        let due = lapsed(&store.db, now() + 3600.0, 0.0).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].attempt, id);
        std::thread::sleep(std::time::Duration::from_millis(5));
        heartbeat(&mut store, "mindy", &Scope::all(), vec![lease(&claim)]);
        assert!(!expire_lease(&mut store, &due[0]).unwrap());
        assert_eq!(row(&store.db, &id), ("running".into(), false));
        assert_eq!(state(&store.db, "first"), "running");
        assert!(events(&store.db, "attempt.lease_expired").is_empty());
        // The renewed lease is judged on its own expiry.
        let renewed = store.attempt(&id).unwrap().lease_expires.unwrap();
        assert!(expire(&mut store, renewed - 1.0, 0.0).unwrap().is_empty());
        assert_eq!(expire(&mut store, renewed + 1.0, 0.0).unwrap(), [id]);
    }

    #[test]
    fn outcomes_map_to_states_events_and_repairs() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 16, 16384);
        let exited = |code: i32| json!({"exit_code":code,"reason":null,"artifacts":{}});
        let stopped = |reason: &str| json!({"exit_code":124,"reason":reason,"artifacts":{}});
        let error = |message: &str| json!({"error":message});
        use Outcome::*;
        let stop = |reason: &'static str, state: &'static str| {
            (Completed, Some(reason), 124, stopped(reason), true, state)
        };
        // (outcome, reason, exit code, evidence, drained, attempt and action state)
        let cases = [
            (Completed, None, 0, exited(0), true, "passed"),
            (Completed, None, 2, exited(2), true, "failed"),
            stop("timeout", "failed"),
            (
                Completed,
                None,
                125,
                error("input unavailable"),
                false,
                "failed",
            ),
            stop("no-requesters", "canceled"),
            stop("supervisor-stop", "interrupted"),
            stop("partition", "interrupted"),
            stop("fenced", "interrupted"),
            stop("revoked", "interrupted"),
            (
                NotStarted,
                Some("no-requesters"),
                125,
                json!({}),
                true,
                "canceled",
            ),
            (
                NotStarted,
                Some("launch-failed"),
                125,
                error("spawn"),
                true,
                "interrupted",
            ),
            (
                NotStarted,
                Some("materialize-failed"),
                125,
                error("mirror"),
                false,
                "interrupted",
            ),
            (
                Lost,
                None,
                125,
                error("supervisor exited"),
                false,
                "interrupted",
            ),
            (
                Undeliverable,
                None,
                125,
                error("too_large"),
                true,
                "interrupted",
            ),
        ];
        for (n, (outcome, reason, exit_code, evidence, drained, attempt_state)) in
            cases.into_iter().enumerate()
        {
            let id = format!("action-{n:02}");
            queued(&store.db, &id, n as f64);
            let claim = grant(&mut store, &format!("k{n}"));
            assert_eq!(claim.action.id, id);
            register_supervisor(&mut store, &claim);
            let request = CompleteRequest {
                drained,
                ..finish(&claim, outcome, reason, exit_code, evidence.clone())
            };
            let response = complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::InPlace,
            )
            .unwrap();
            assert_eq!(response.state.as_deref(), Some(attempt_state), "{id}");
            assert_eq!(response.drained, drained, "{id}");
            let attempt = &claim.attempt.id;
            assert_eq!(
                row(&store.db, attempt),
                (attempt_state.into(), drained),
                "{id}"
            );
            // A drained attempt that never started requeues its action (review P2-1), and so does
            // one an interruption stopped (gate P2-2).
            let interruption = outcome == Completed
                && matches!(
                    reason,
                    Some("supervisor-stop" | "partition" | "fenced" | "revoked")
                );
            let requeued = drained
                && (interruption || outcome == NotStarted && reason != Some("no-requesters"));
            let action_state = if requeued { "queued" } else { attempt_state };
            assert_eq!(state(&store.db, &id), action_state, "{id}");
            if requeued {
                // Out of the way of the next case's claim.
                store
                    .db
                    .execute("UPDATE actions SET state='canceled' WHERE id=?", [&id])
                    .unwrap();
            }
            // Only a failure says something about the revision.
            let repaired = i64::from(attempt_state == "failed");
            assert_eq!(repairs(&store.db, &id), repaired, "{id}");
            // Identity fields come from the store, whatever the executor reported.
            let mut recorded = evidence.clone();
            for (field, value) in [
                ("revision", json!("r-a")),
                ("profile", json!("f-1")),
                ("job", json!("test")),
                (
                    "identity",
                    json!({"version":JOB_RECIPE,"recipe":"recipe","class":"class","key":id}),
                ),
                ("executor", json!("mindy")),
            ] {
                recorded[field] = value;
            }
            let stored = store.attempt(attempt).unwrap();
            assert_eq!(stored.result.as_ref(), Some(&recorded), "{id}");
            assert_eq!(stored.exit_code, Some(exit_code), "{id}");
            assert!(stored.ended.is_some(), "{id}");
            assert_eq!(stored.completion, Some(crate::identity(&request).unwrap()));
            let (kind, detail) = match outcome {
                Completed => (
                    "attempt.completed",
                    json!({"state":attempt_state,"evidence":recorded,"requeued":requeued}),
                ),
                NotStarted => (
                    "attempt.launch_failed",
                    json!({"reason":reason,"error":evidence["error"],"requeued":requeued}),
                ),
                Lost => (
                    "attempt.interrupted",
                    json!({"drained":drained,"operator_confirmed":false,
                           "reason":"supervisor-lost","executor":"mindy",
                           "requeued":false}),
                ),
                Undeliverable => (
                    "attempt.interrupted",
                    json!({"drained":drained,"reason":"undeliverable",
                           "error":evidence["error"],"executor":"mindy"}),
                ),
            };
            let recorded_events: Vec<Value> = events(&store.db, kind)
                .into_iter()
                .filter(|(subject, _)| subject == attempt)
                .map(|(_, detail)| detail)
                .collect();
            assert_eq!(recorded_events, [detail], "{id}");
        }
    }

    #[test]
    fn runtime_mismatch_requeues_without_repair() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        // Tool drift on this host says nothing about the revision.
        let error = "declared tool bytes changed; register a new profile";
        let request = finish(
            &claim,
            Outcome::NotStarted,
            Some("runtime-mismatch"),
            125,
            json!({"error":error}),
        );
        let response = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        assert_eq!(response.state.as_deref(), Some("interrupted"));
        assert_eq!(row(&store.db, &id), ("interrupted".into(), true));
        let action: (String, Option<String>) = store
            .db
            .query_row(
                "SELECT state,attempt FROM actions WHERE id='first'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(action, ("queued".into(), None));
        assert_eq!(repairs(&store.db, "first"), 0);
        assert_eq!(
            events(&store.db, "attempt.launch_failed"),
            [(
                id.clone(),
                json!({"reason":"runtime-mismatch","error":error,"requeued":true})
            )]
        );
        assert_eq!(held(&store.db, "mindy").unwrap(), idle());
        // The executor that refuses the profile is not handed it again; one that verifies it is.
        let mut refusing = ask("k2", &[]);
        refusing.refused.insert("f-1".into(), error.into());
        let response = claim_for(&mut store, "mindy", &Scope::all(), &refusing).unwrap();
        assert!(response.claim.is_none());
        let again = grant(&mut store, "k3");
        assert_eq!(again.action.id, "first");
        assert_ne!(again.attempt.id, id);
        // The requeued attempt can no longer renew.
        let status = heartbeat(&mut store, "mindy", &Scope::all(), vec![lease(&claim)]);
        assert_eq!(status[0].status, "fenced");
    }

    #[test]
    fn confirm_drained_preserves_terminal_state() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 8, 8192);
        // Remote cleanup failed: the result stands and the capacity stays held.
        for (n, (id, exit_code, evidence, terminal)) in [
            (
                "failed",
                125,
                json!({"exit_code":0,"cleanup_error":"external cleanup failed","artifacts":{}}),
                "failed",
            ),
            ("passed", 0, json!({"exit_code":0,"artifacts":{}}), "passed"),
            (
                "canceled",
                124,
                json!({"exit_code":124,"reason":"no-requesters","artifacts":{}}),
                "canceled",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            queued(&store.db, id, n as f64);
            let claim = grant(&mut store, &format!("k{n}"));
            register_supervisor(&mut store, &claim);
            let reason = evidence["reason"].as_str().map(String::from);
            let request = CompleteRequest {
                reason,
                drained: false,
                ..finish(&claim, Outcome::Completed, None, exit_code, evidence)
            };
            complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::InPlace,
            )
            .unwrap();
            let attempt = claim.attempt.id;
            let before = body(&store.db, &attempt);
            assert_eq!(held(&store.db, "mindy").unwrap(), one());
            confirm_drained(&mut store, &attempt).unwrap();
            assert_eq!(row(&store.db, &attempt), (terminal.into(), true));
            assert_eq!(state(&store.db, id), terminal);
            assert_eq!(body(&store.db, &attempt), before);
            assert_eq!(held(&store.db, "mindy").unwrap(), idle());
            let confirmed = events(&store.db, "attempt.drain_confirmed");
            assert_eq!(
                confirmed.last().unwrap(),
                &(
                    attempt.clone(),
                    json!({"state":terminal,"operator_confirmed":true})
                )
            );
            let again = confirm_drained(&mut store, &attempt).unwrap_err();
            assert!(
                format!("{again:#}").contains("already drained"),
                "{again:#}"
            );
        }
        // A held attempt belongs to its coordinator until it reports or its lease expires.
        queued(&store.db, "held", 3.0);
        let held_claim = grant(&mut store, "k3");
        let attempt = held_claim.attempt.id.clone();
        let refused = confirm_drained(&mut store, &attempt).unwrap_err();
        assert!(
            format!("{refused:#}").contains("held by executor mindy"),
            "{refused:#}"
        );
        assert_eq!(row(&store.db, &attempt), ("claimed".into(), false));
        assert_eq!(
            expire(&mut store, now() + 3600.0, 0.0).unwrap(),
            [attempt.as_str()]
        );
        confirm_drained(&mut store, &attempt).unwrap();
        assert_eq!(row(&store.db, &attempt), ("interrupted".into(), true));
        assert_eq!(
            events(&store.db, "attempt.interrupted"),
            [(
                attempt.clone(),
                json!({"drained":true,"operator_confirmed":true})
            )]
        );
        // A local supervisor that is still alive is never confirmed drained.
        let me = std::process::id();
        let alive = json!({"id":"alive","action":"elsewhere","worker_pid":me,"worker_start":"0",
            "supervisor_pid":me,"supervisor_start":crate::worker::process_start(me).unwrap(),
            "slots":1,"memory_mb":512,"created":0.0,"started":0.0,"ended":1.0,"exit_code":125,
            "result":null});
        store
            .db
            .execute(
                "INSERT INTO attempts VALUES ('alive','elsewhere','failed',0,?)",
                [alive.to_string()],
            )
            .unwrap();
        let refused = confirm_drained(&mut store, "alive").unwrap_err();
        assert!(
            format!("{refused:#}").contains("supervisor is still alive"),
            "{refused:#}"
        );
        assert_eq!(row(&store.db, "alive"), ("failed".into(), false));
        // A held attempt of the previous binary never reports; once its supervisor is gone the
        // operator interrupts and drains it.
        queued(&store.db, "upgrade", 4.0);
        hold(&store.db, "legacy", None, 1, 512);
        store
            .db
            .execute_batch(
                "UPDATE attempts SET action='upgrade',body=json_set(body,'$.action','upgrade')
                 WHERE id='legacy';
                 UPDATE actions SET state='running',attempt='legacy' WHERE id='upgrade';",
            )
            .unwrap();
        confirm_drained(&mut store, "legacy").unwrap();
        assert_eq!(row(&store.db, "legacy"), ("interrupted".into(), true));
        assert_eq!(state(&store.db, "upgrade"), "interrupted");
        assert_eq!(events(&store.db, "attempt.interrupted").len(), 2);
        assert!(confirm_drained(&mut store, "ghost").is_err());
    }

    #[test]
    fn complete_rederives_identity_and_checks_evidence() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queued(&store.db, "first", 1.0);
        let claim = grant(&mut store, "k1");
        let id = claim.attempt.id.clone();
        register_supervisor(&mut store, &claim);
        let log = BlobRef {
            sha256: "a".repeat(64),
            bytes: 10,
        };
        let artifact = ArtifactEntry {
            sha256: "b".repeat(64),
            mode: 0o644,
            bytes: 3,
        };
        let with = |artifacts: BTreeMap<String, ArtifactEntry>| {
            let mut request = finish(
                &claim,
                Outcome::Completed,
                None,
                0,
                json!({"exit_code":0,"reason":null,"log_sha256":log.sha256,"artifacts":artifacts}),
            );
            request.log = Some(log.clone());
            request.artifacts = artifacts;
            request
        };
        let good = || with([("dist/value".to_owned(), artifact.clone())].into());
        let entry = |mode: u32, bytes: u64| ArtifactEntry {
            mode,
            bytes,
            ..artifact.clone()
        };
        let limit = crate::api::MAX_ARTIFACT_BYTES;
        let huge: BTreeMap<String, ArtifactEntry> = (0..5)
            .map(|n| (format!("part-{n}"), entry(0o644, limit)))
            .collect();
        let invalid = [
            (
                "exit code",
                CompleteRequest {
                    exit_code: 1,
                    ..good()
                },
            ),
            (
                "log digest",
                CompleteRequest {
                    log: Some(BlobRef {
                        sha256: "c".repeat(64),
                        bytes: 10,
                    }),
                    ..good()
                },
            ),
            (
                "missing log",
                CompleteRequest {
                    log: None,
                    ..good()
                },
            ),
            (
                "artifacts",
                CompleteRequest {
                    artifacts: BTreeMap::new(),
                    ..good()
                },
            ),
            (
                "traversal",
                with([("../escape".to_owned(), artifact.clone())].into()),
            ),
            ("mode", with([("tool".to_owned(), entry(0o4755, 3))].into())),
            (
                "file size",
                with([("big".to_owned(), entry(0o644, limit + 1))].into()),
            ),
            ("attempt size", with(huge)),
            ("cleanup failure", {
                let mut request = good();
                request.evidence["cleanup_error"] = json!("external cleanup failed");
                request
            }),
            (
                "not an object",
                CompleteRequest {
                    evidence: json!("passed"),
                    ..good()
                },
            ),
        ];
        for (case, request) in invalid {
            let staged = stage(&store, "log\n");
            let result = complete(
                &mut store,
                "mindy",
                &Scope::all(),
                &request,
                Outputs::Staged(staged.clone()),
            );
            assert!(
                matches!(result, Err(AuthorityError::InvalidEvidence(_))),
                "{case}: {result:?}"
            );
            assert!(!staged.exists(), "{case}");
            assert_eq!(row(&store.db, &id), ("running".into(), false), "{case}");
        }
        assert!(store.attempt(&id).unwrap().completion.is_none());
        // A failed cleanup keeps the command's own code beside the error; the attempt fails 125.
        let mut request = good();
        request.exit_code = 125;
        request.evidence["cleanup_error"] = json!("external cleanup failed");
        for field in ["revision", "profile", "job", "identity", "executor"] {
            request.evidence[field] = json!("forged");
        }
        let response = complete(
            &mut store,
            "mindy",
            &Scope::all(),
            &request,
            Outputs::InPlace,
        )
        .unwrap();
        assert_eq!(response.state.as_deref(), Some("failed"));
        let evidence = store.attempt(&id).unwrap().result.unwrap();
        for (field, value) in [
            ("revision", "r-a"),
            ("profile", "f-1"),
            ("job", "test"),
            ("executor", "mindy"),
        ] {
            assert_eq!(evidence[field], value, "{field}");
        }
        assert_eq!(
            evidence["identity"],
            json!({"version":JOB_RECIPE,"recipe":"recipe","class":"class","key":"first"})
        );
        // The report itself is accepted as the executor gave it.
        assert_eq!(evidence["exit_code"], 0);
        assert_eq!(evidence["log_sha256"], json!(log.sha256));
    }

    #[test]
    fn executor_without_placement_allowlist_claims_nothing() {
        let (_root, mut store) = lab();
        queue(
            &store.db,
            Queue {
                id: "work",
                ..Queue::default()
            },
        );
        let fresh = ConfigureRequest {
            slots: Some(2),
            memory_mb: Some(2048),
            ..ConfigureRequest::default()
        };
        let configured = configure(&mut store, "mindy", &fresh).unwrap();
        assert_eq!(configured["placement"], json!({"allow":[],"deny":[]}));
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), None);
        assert!(response.unverified.is_empty());
        assert_eq!(state(&store.db, "work"), "queued");
        // The operator places jobs on a remote executor by name.
        let allow = ConfigureRequest {
            allow: vec!["test".into()],
            ..ConfigureRequest::default()
        };
        configure(&mut store, "mindy", &allow).unwrap();
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("work"));
        // Clearing placement closes a remote executor again; the local one admits every job.
        let clear = ConfigureRequest {
            clear_placement: true,
            ..ConfigureRequest::default()
        };
        configure(&mut store, "mindy", &clear).unwrap();
        let mindy = load(&store.db, "mindy").unwrap().unwrap();
        assert_eq!(mindy.placement.allow, Some(BTreeSet::new()));
        assert!(!mindy.placement.permits("test"));
        configure(&mut store, LOCAL, &allow).unwrap();
        configure(&mut store, LOCAL, &clear).unwrap();
        let local = load(&store.db, LOCAL).unwrap().unwrap();
        assert_eq!(local.placement, Placement::default());
        queue(
            &store.db,
            Queue {
                id: "local-work",
                job: "build",
                created: 2.0,
                ..Queue::default()
            },
        );
        let response = claim_for(&mut store, LOCAL, &Scope::all(), &ask("k3", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("local-work"));
        // Configuration never loses the executor's capacity or its observations.
        assert_eq!(
            (mindy.slots, mindy.memory_mb, mindy.lease_seconds),
            (Some(2), Some(2048), DEFAULT_LEASE_SECONDS)
        );
        assert!(mindy.observed.last_seen.is_some());
    }

    #[test]
    fn configure_validates_and_local_capacity_is_the_settings() {
        let (_root, mut store) = empty();
        let request = |slots, memory_mb, lease_seconds| ConfigureRequest {
            slots,
            memory_mb,
            lease_seconds,
            ..ConfigureRequest::default()
        };
        for (id, invalid) in [
            ("mindy", request(Some(1), None, None)),
            ("mindy", request(None, Some(1024), None)),
            ("mindy", request(Some(0), Some(1024), None)),
            ("mindy", request(Some(65), Some(1024), None)),
            ("mindy", request(Some(1), Some(127), None)),
            ("mindy", request(Some(1), Some(1024), Some(2))),
            ("mindy", request(Some(1), Some(1024), Some(3601))),
            ("", request(Some(1), Some(1024), None)),
        ] {
            assert!(
                configure(&mut store, id, &invalid).is_err(),
                "{id} {invalid:?}"
            );
        }
        let mut blank = request(Some(1), Some(1024), None);
        blank.deny = vec![String::new()];
        assert!(configure(&mut store, "mindy", &blank).is_err());
        assert!(load(&store.db, "mindy").unwrap().is_none());
        configure(&mut store, "mindy", &request(Some(64), Some(128), Some(3))).unwrap();
        configure(&mut store, LOCAL, &request(Some(2), Some(1536), Some(3600))).unwrap();
        assert_eq!(store.capacity().unwrap(), (2, 1536));
        let local = load(&store.db, LOCAL).unwrap().unwrap();
        assert_eq!((local.slots, local.memory_mb), (None, None));
        assert_eq!(local.lease_seconds, 3600);
        let listed = list(&store).unwrap();
        assert_eq!(listed[0]["id"], LOCAL);
        assert_eq!(listed[0]["configured"], json!({"slots":2,"memory_mb":1536}));
        assert_eq!(listed[1]["id"], "mindy");
        assert_eq!(listed[1]["configured"], json!({"slots":64,"memory_mb":128}));
        assert_eq!(listed[1]["lease_seconds"], 3);
        let events = events(&store.db, "executor.configured");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, "mindy");
        assert_eq!(events[0].1["created"], true);
        assert_eq!(events[1].1["slots"], 2);
    }
}
